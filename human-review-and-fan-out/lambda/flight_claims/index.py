"""Flight delay/cancellation compensation claim processing (durable workflow).

Demonstrates FOUR AWS Lambda durable functions concepts on a SINGLE unit of
work, the same kind of unit that ``large_scale_processing`` fans out across
many items:

  1. Error handling   - automatic per-step retries (StepConfig.retry_strategy)
                        plus explicit try/except recovery around a parallel
                        assessment.
  2. Parallel checks   - context.parallel() runs independent checks on ONE
                        claim concurrently instead of serially: three
                        downstream lookups that don't depend on each other
                        finish in the time of the slowest one, not the sum.
  3. Branching        - ordinary Python if/elif over checkpointed results,
                        the idiomatic (replay-deterministic) durable way.
  4. Callback pattern - context.create_callback() + callback.result() suspends
                        the execution (no compute charges) until a human
                        adjudicator signals back via the
                        SendDurableExecutionCallback* APIs.

Offloading large step results to S3 (keep the checkpoint small):
``fetch_flight_history`` calls a (mock) flight-history API whose response can
easily exceed the ~256 KB durable-state limit. The step writes the full
response to S3 itself and returns only a small pointer (``FlightHistoryRef``:
bucket, key, counts). That pointer is the step result that gets checkpointed;
on replay the step returns the pointer without re-calling the API, and the
consumer (``check_duplicate``) reads the body back from S3 by key. Because the
API is slow and only the duplicate check needs its result, the fetch runs
INSIDE the duplicate-check branch of context.parallel() (fetch step, then
check step) rather than before it, so the other checks don't wait on it.

Flow:

                         +---------------------------+
    event.claim_id ----> | extract_claim              |
                         | retry: transient intake    |
                         +-------------+-------------+
                                       |
                                       v
                         +----------------------------------------------+
                         | context.parallel(): THREE independent checks |
                         |   verify_delay | check_eligibility | fetch_  |
                         |                                   flight_    |
                         |                                   history -> |
                         |                                   (S3 key)   |
                         |                                   check_     |
                         |                                   duplicate  |
                         +---------+----------------------+-------------+
                                   |                       |
                      assessment   |                       | any branch
                        failed     |                       | failed
                                   v                       v
                         +----------------+      +----------------------+
                         | confidence     |      | explicit recovery:   |
                         | branch         |      | reject + persist,    |
                         | (is_duplicate, |      | return 500           |
                         |  claim_score,  |      +----------------------+
                         |  eligible)     |
                         +--+------+----+-+
                            |      |      |
                  duplicate |      | 0.6- | >= 0.9 & eligible
                            v      | 0.9  v
                      +----------+ |  +----------+
                      | reject   | |  | approve  |
                      +----+-----+ |  +-----+----+
                           |       v        |
                           |  +-------------------+
                           |  | create callback   |
                           |  | -> SQS review      |
                           |  +--------+----------+
                           |           |
                           |  +-------------------+
                           |  | reviewer response |
                           |  | (execution waits) |
                           |  +----+----------+---+
                           |       |          |
                           |   success     timeout/
                           |       |         failure
                           |       v          v
                           |  human_approved  escalate/reject
                           |       |          |
                           v       v          v
                      +--------------------------------------+
                      | persist final outcome                  |
                      +--------------------------------------+
"""

import json
import os
import random
import time
from datetime import UTC, datetime
from decimal import Decimal
from typing import TypedDict, cast

import boto3

from aws_durable_execution_sdk_python.concurrency.models import BatchCompletionError
from aws_durable_execution_sdk_python.config import (
    CallbackConfig,
    Duration,
    StepConfig,
)
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_parallel_branch,
    durable_step,
)
from aws_durable_execution_sdk_python.exceptions import (
    CallbackExternalError,
    CallbackTimeoutError,
    ChildContextError,
)
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.retries import (
    RetryStrategyConfig,
    create_retry_strategy,
)
from aws_durable_execution_sdk_python.serdes import JsonSerDes

# Confidence/eligibility thresholds that drive the branching decision.
AUTO_APPROVE_THRESHOLD = 0.9
HUMAN_REVIEW_THRESHOLD = 0.6

# SQS queue (provisioned by the CDK stack) where human-review requests are
# published. Adjudicators consume from it and resolve the execution with the
# callback id via SendDurableExecutionCallbackSuccess/Failure.
CLAIM_REVIEW_QUEUE_URL = os.environ.get("CLAIM_REVIEW_QUEUE_URL")

# DynamoDB table (provisioned by the CDK stack) holding each claim's status,
# keyed by its claim id.
CLAIM_TABLE_NAME = os.environ.get("CLAIM_TABLE_NAME")

# S3 bucket (the stack's data bucket) where step results too large to
# checkpoint are offloaded. Only the object's key goes into durable state.
DATA_BUCKET = os.environ.get("DATA_BUCKET")
FLIGHT_HISTORY_PREFIX = "flight-history/"

# Demo knob: how many past flights the mock flight-history API returns. At
# ~200 bytes per record, the default 2,000 is ~400 KB — well over the ~256 KB
# checkpoint limit if the step returned it inline.
_FLIGHT_HISTORY_LENGTH = int(os.environ.get("FLIGHT_HISTORY_LENGTH", "2000"))

# Module-scope clients so warm invocations reuse the connection pool.
_sqs = boto3.client("sqs")
_dynamodb = boto3.resource("dynamodb")
_s3 = boto3.client("s3")

# Demo knob: simulated latency (ms) for each of the three parallel checks,
# scaled per check below so the slowest one sets the parallel wall-clock time.
_BASE_CHECK_LATENCY_MS = int(os.environ.get("CHECK_LATENCY_MS", "300"))


def _simulate_latency(multiplier: float) -> None:
    """Sleep to stand in for a downstream call (demo knob: CHECK_LATENCY_MS)."""
    time.sleep(_BASE_CHECK_LATENCY_MS * multiplier / 1000.0)


# A Claim is enriched in stages, each stage its own TypedDict (all plain
# dicts at runtime — this is purely a static-typing/IntelliSense layer, so the
# existing **merge/.get()/[...] access patterns and JSON checkpointing are
# unaffected):
#
#   Claim            <- extract_claim (intake)
#   FlightHistoryRef <- fetch_flight_history (S3 pointer, NOT the history)
#   + DelayAssessment, EligibilityAssessment, DuplicateAssessment
#                     <- the three context.parallel() branches, concurrently
#   = AssessedClaim   <- merged by the handler once all three complete
class Claim(TypedDict):
    claim_id: str
    flight_number: str
    claim_score: float
    eligible_override: bool | None
    duplicate_override: bool | None


class DelayAssessment(TypedDict):
    verified_delay_minutes: int
    compensation_eur: int


class EligibilityAssessment(TypedDict):
    eligible: bool
    fare_class: str


class DuplicateAssessment(TypedDict):
    is_duplicate: bool
    prior_claims: int


class FlightHistoryRef(TypedDict):
    """Pointer to a flight history offloaded to S3 — what actually gets checkpointed."""

    bucket: str
    key: str
    flight_count: int
    size_bytes: int


class AssessedClaim(
    Claim, DelayAssessment, EligibilityAssessment, DuplicateAssessment
):
    assessment_ms: int


def _record_status(claim_id: str, status: str, **attributes) -> None:
    """Upsert a claim's current status (plus any extra attributes).

    Uses SET semantics, so repeating a write is harmless. That matters because
    steps run at-least-once: a step that fails after this write and is retried
    simply writes the same status again.
    """
    if not CLAIM_TABLE_NAME:
        msg = "CLAIM_TABLE_NAME is not configured"
        raise RuntimeError(msg)

    values = {
        "status": status,
        "updated_at": datetime.now(UTC).isoformat(),
        **attributes,
    }
    # DynamoDB rejects Python floats; store them as Decimals.
    values = {
        name: Decimal(str(value)) if isinstance(value, float) else value
        for name, value in values.items()
    }
    # Name placeholders because "status" is a DynamoDB reserved word.
    _dynamodb.Table(CLAIM_TABLE_NAME).update_item(
        Key={"claim_id": claim_id},
        UpdateExpression="SET " + ", ".join(f"#{n} = :{n}" for n in values),
        ExpressionAttributeNames={f"#{n}": n for n in values},
        ExpressionAttributeValues={f":{n}": v for n, v in values.items()},
    )

# Retry only transient intake failures (e.g. OCR'ing an uploaded boarding
# pass/ticket), with exponential backoff and jitter. Non-matching errors fail
# the step immediately.
intake_retry = create_retry_strategy(
    RetryStrategyConfig(
        max_attempts=5,
        initial_delay=Duration.from_seconds(2),
        max_delay=Duration.from_minutes(1),
        retryable_errors=[
            "ThrottlingException",
            "ProvisionedThroughputExceededException",
            "ServiceUnavailable",
        ],
    )
)

# Retry transient failures in the three parallel checks (flight-ops,
# loyalty, and duplicate-claim lookups) inside each branch, so a brief
# downstream blip never surfaces as a failed branch. Shorter and tighter than
# intake_retry: a check only buys time for the slowest branch, so give up fast
# and let the handler's explicit recovery take over.
check_retry = create_retry_strategy(
    RetryStrategyConfig(
        max_attempts=3,
        initial_delay=Duration.from_seconds(1),
        max_delay=Duration.from_seconds(10),
        retryable_errors=[
            "ThrottlingException",
            "ProvisionedThroughputExceededException",
            "ServiceUnavailable",
        ],
    )
)


def _extract_claim(
    claim_id: str,
    flight_number: str,
    claim_score_override: float | None = None,
    eligible_override: bool | None = None,
    duplicate_override: bool | None = None,
) -> Claim:
    """Parse an incoming claim (e.g. OCR'd from an uploaded boarding pass/ticket).

    The claim score is variable so every downstream branch can be exercised. An
    explicit override drives a chosen demo path; otherwise a random score in
    [0.0, 1.0) is returned. ``eligible_override``/``duplicate_override`` thread
    through to the two parallel checks below so a demo can force every branch
    deterministically.

    This function runs inside ``extract_claim`` so its result is checkpointed
    and therefore replay-safe. Do not call it directly from the handler.
    """
    # Here is where you would call Textract/OCR on the uploaded boarding pass
    # and ticket, or an airline ops API for the flight record.
    claim_score = (
        float(claim_score_override)
        if claim_score_override is not None
        else round(random.random(), 4)  # nosec B311 - simulated data, not security-sensitive
    )
    _record_status(claim_id, "extracted", flight_number=flight_number, claim_score=claim_score)
    return {
        "claim_id": claim_id,
        "flight_number": flight_number,
        "claim_score": claim_score,
        "eligible_override": eligible_override,
        "duplicate_override": duplicate_override,
    }


def _call_flight_history_api(customer_id: str) -> list[dict]:
    """Mock flight-history API: every past flight for a customer (can be huge).

    Stands in for a downstream loyalty/booking system. The response size is
    driven by FLIGHT_HISTORY_LENGTH; it's seeded by customer id so the same
    customer always gets the same history.
    """
    _simulate_latency(1.0)
    rng = random.Random(customer_id)  # nosec B311 - simulated data, not security-sensitive
    airports = ["LHR", "JFK", "CDG", "FRA", "AMS", "MAD", "DXB", "SIN", "SFO", "ORD"]
    flights = []
    for i in range(_FLIGHT_HISTORY_LENGTH):
        origin, destination = rng.sample(airports, 2)
        delay = rng.choice([0, 0, 0, 5, 15, 45, 90, 200, 300])
        flights.append(
            {
                "booking_ref": f"{customer_id}-{i:06d}",
                "flight_number": f"{rng.choice(['BA', 'LH', 'AF', 'KL', 'IB'])}{rng.randint(100, 9999)}",
                "date": f"{rng.randint(2015, 2026)}-{rng.randint(1, 12):02d}-{rng.randint(1, 28):02d}",
                "origin": origin,
                "destination": destination,
                "fare_class": rng.choice(["basic", "standard", "flex"]),
                "arrival_delay_minutes": delay,
                "cancelled": rng.random() < 0.02,
                "claim_filed": delay >= 180 and rng.random() < 0.3,
            }
        )
    return flights


def _fetch_flight_history(claim_id: str, customer_id: str) -> FlightHistoryRef:
    """Fetch the customer's flight history, offload it to S3, return a pointer.

    The full API response never leaves this function: it goes straight to S3
    and only the small ``FlightHistoryRef`` is returned (and so checkpointed).
    The key is deterministic (derived from the claim id, never a timestamp or
    random value), so a retried step overwrites the same object instead of
    leaving orphans.
    """
    if not DATA_BUCKET:
        msg = "DATA_BUCKET is not configured"
        raise RuntimeError(msg)

    history = _call_flight_history_api(customer_id)
    body = json.dumps(history).encode("utf-8")
    key = f"{FLIGHT_HISTORY_PREFIX}{claim_id}.json"
    _s3.put_object(
        Bucket=DATA_BUCKET, Key=key, Body=body, ContentType="application/json"
    )
    return {
        "bucket": DATA_BUCKET,
        "key": key,
        "flight_count": len(history),
        "size_bytes": len(body),
    }


def _load_flight_history(ref: FlightHistoryRef) -> list[dict]:
    """Read an offloaded flight history back from S3 by its pointer."""
    obj = _s3.get_object(Bucket=ref["bucket"], Key=ref["key"])
    return json.loads(obj["Body"].read())


def _verify_delay(claim: Claim) -> DelayAssessment:
    """Confirm the delay against flight-ops data and compute the compensation tier.

    EU261-style bands: under 3h -> no compensation, 3-4h -> EUR 300, 4h+ ->
    EUR 600. Flavor only (not used for branching) — simulates the slowest of
    the three downstream lookups.
    """
    _simulate_latency(1.3)
    delay_minutes = random.randint(0, 420)  # nosec B311 - simulated data, not security-sensitive
    compensation_eur = 600 if delay_minutes >= 240 else 300 if delay_minutes >= 180 else 0
    return {"verified_delay_minutes": delay_minutes, "compensation_eur": compensation_eur}


def _check_eligibility(claim: Claim) -> EligibilityAssessment:
    """Check the passenger's fare class/loyalty tier for compensation eligibility.

    ``eligible`` gates the auto-approve branch, so it's deterministically
    overridable via ``claim["eligible_override"]``; otherwise a realistic
    random default (85% eligible) keeps un-forced demo runs varied. The
    fastest of the three checks.
    """
    _simulate_latency(0.5)
    override = claim.get("eligible_override")
    eligible = bool(override) if override is not None else random.random() < 0.85  # nosec B311 - simulated data
    fare_class = random.choice(["basic", "standard", "flex"])  # nosec B311 - simulated data
    return {"eligible": eligible, "fare_class": fare_class}


def _check_duplicate(claim: Claim, history_ref: FlightHistoryRef) -> DuplicateAssessment:
    """Check whether this flight/passenger has already filed a claim.

    Reads the customer's flight history back from S3 via the pointer that
    ``fetch_flight_history`` checkpointed, and counts the claims already filed
    (``prior_claims``). ``is_duplicate`` gates auto-rejection ahead of every
    other signal, so it's deterministically overridable via
    ``claim["duplicate_override"]``; otherwise a realistic random default (5%
    duplicate) keeps un-forced demo runs varied.
    """
    _simulate_latency(0.9)
    history = _load_flight_history(history_ref)
    prior_claims = sum(1 for flight in history if flight.get("claim_filed"))
    override = claim.get("duplicate_override")
    is_duplicate = bool(override) if override is not None else random.random() < 0.05  # nosec B311 - simulated data
    return {"is_duplicate": is_duplicate, "prior_claims": prior_claims}


def _auto_approve(claim: AssessedClaim) -> dict:
    """Return the approval outcome, with the compensation payout amount."""
    # TODO: trigger the actual payout.
    compensation_eur = claim["compensation_eur"]
    _record_status(claim["claim_id"], "approved", compensation_eur=compensation_eur)
    return {"claim_id": claim["claim_id"], "status": "approved", "compensation_eur": compensation_eur}


def _reject(claim: Claim, reason: str) -> dict:
    """Return the rejection outcome with a machine-readable reason."""
    # TODO: notify the claimant.
    _record_status(claim["claim_id"], "rejected", reason=reason)
    return {"claim_id": claim["claim_id"], "status": "rejected", "reason": reason}


def _notify_reviewer(callback_id: str, claim: AssessedClaim) -> None:
    """Publish a human-review request containing the callback id and claim."""
    if not CLAIM_REVIEW_QUEUE_URL:
        msg = "CLAIM_REVIEW_QUEUE_URL is not configured"
        raise RuntimeError(msg)
    _sqs.send_message(
        QueueUrl=CLAIM_REVIEW_QUEUE_URL,
        MessageBody=json.dumps({"callback_id": callback_id, "claim": claim}),
    )
    # Storing the callback id lets an adjudicator tool look it up by claim id.
    _record_status(claim["claim_id"], "pending_review", callback_id=callback_id)


def _persist(claim_id: str, outcome: dict) -> dict:
    """Record the final decision as the claim's terminal status."""
    decision = outcome["decision"]
    status = "approved" if decision.endswith("approved") else "rejected"
    _record_status(claim_id, status, decision=decision)
    return outcome


@durable_step
def extract_claim(
    step_context: StepContext,
    claim_id: str,
    flight_number: str,
    claim_score_override: float | None = None,
    eligible_override: bool | None = None,
    duplicate_override: bool | None = None,
) -> Claim:
    """Durable adapter for claim intake."""
    extracted = _extract_claim(
        claim_id, flight_number, claim_score_override, eligible_override, duplicate_override
    )
    step_context.logger.info(
        "flight_claims: extract_claim for %s (claim_score=%.4f)",
        claim_id,
        extracted["claim_score"],
    )
    return extracted


@durable_step
def fetch_flight_history(
    step_context: StepContext, claim_id: str, customer_id: str
) -> FlightHistoryRef:
    """Durable adapter for the flight-history lookup (result offloaded to S3)."""
    ref = _fetch_flight_history(claim_id, customer_id)
    step_context.logger.info(
        "flight_claims: fetch_flight_history %s -> s3://%s/%s (%d flights, %d bytes)",
        claim_id, ref["bucket"], ref["key"], ref["flight_count"], ref["size_bytes"],
    )
    return ref


@durable_step
def verify_delay(step_context: StepContext, claim: Claim) -> DelayAssessment:
    """Durable adapter for the flight-ops delay/compensation lookup."""
    step_context.logger.info("flight_claims: verify_delay %s", claim["claim_id"])
    return _verify_delay(claim)


@durable_step
def check_eligibility(step_context: StepContext, claim: Claim) -> EligibilityAssessment:
    """Durable adapter for the fare-class/loyalty eligibility lookup."""
    step_context.logger.info("flight_claims: check_eligibility %s", claim["claim_id"])
    return _check_eligibility(claim)


@durable_step
def check_duplicate(
    step_context: StepContext, claim: Claim, history_ref: FlightHistoryRef
) -> DuplicateAssessment:
    """Durable adapter for the duplicate-claim lookup."""
    step_context.logger.info("flight_claims: check_duplicate %s", claim["claim_id"])
    return _check_duplicate(claim, history_ref)


@durable_step
def auto_approve(step_context: StepContext, claim: AssessedClaim) -> dict:
    """Durable adapter for automatic approval."""
    step_context.logger.info("flight_claims: auto_approve %s", claim["claim_id"])
    return _auto_approve(claim)


@durable_step
def reject(step_context: StepContext, claim: Claim, reason: str) -> dict:
    """Durable adapter for claim rejection."""
    step_context.logger.info(
        "flight_claims: reject %s (%s)", claim.get("claim_id"), reason
    )
    return _reject(claim, reason)


@durable_step
def notify_reviewer(
    step_context: StepContext, callback_id: str, claim: AssessedClaim
) -> None:
    """Durable adapter for publishing a human-review request."""
    step_context.logger.info(
        "flight_claims: notify_reviewer callback_id=%s claim=%s",
        callback_id,
        claim["claim_id"],
    )
    _notify_reviewer(callback_id, claim)


@durable_step
def persist(step_context: StepContext, claim_id: str, outcome: dict) -> dict:
    """Durable adapter for final-outcome persistence."""
    step_context.logger.info("flight_claims: persist %s", outcome.get("decision"))
    return _persist(claim_id, outcome)


# Parallel branches — each wraps one of the three checks in a ctx.step(...) for
# per-branch checkpoint + retry, exactly like a context.map branch does (see
# large_scale_processing's enrich_branch). The difference is cardinality, not
# mechanism: map runs one branch per item in a dynamic list; parallel runs a
# small, fixed set of named branches on ONE item.
@durable_parallel_branch(name="verify-delay")
def verify_delay_branch(ctx: DurableContext, claim: Claim) -> DelayAssessment:
    return ctx.step(
        verify_delay(claim),
        name="verify-delay-step",
        config=StepConfig(retry_strategy=check_retry),
    )


@durable_parallel_branch(name="check-eligibility")
def check_eligibility_branch(ctx: DurableContext, claim: Claim) -> EligibilityAssessment:
    return ctx.step(
        check_eligibility(claim),
        name="check-eligibility-step",
        config=StepConfig(retry_strategy=check_retry),
    )


@durable_parallel_branch(name="check-duplicate")
def check_duplicate_branch(
    ctx: DurableContext, claim: Claim, customer_id: str
) -> DuplicateAssessment:
    # Two steps in sequence inside ONE branch. The slow flight-history fetch
    # lives here (not before context.parallel()) because only the duplicate
    # check needs it — the other two checks run alongside it instead of
    # waiting. The fetch offloads the (large) history to S3 and checkpoints
    # just the pointer; the check step reads the body back by key.
    history_ref: FlightHistoryRef = ctx.step(
        fetch_flight_history(claim["claim_id"], customer_id),
        name="fetch-flight-history",
        config=StepConfig(retry_strategy=check_retry),
    )
    return ctx.step(
        check_duplicate(claim, history_ref),
        name="check-duplicate-step",
        config=StepConfig(retry_strategy=check_retry),
    )


@durable_execution
def lambda_handler(event, context: DurableContext) -> dict:
    claim_id: str = event["claim_id"]
    flight_number: str = event.get("flight_number", "UNKNOWN")
    customer_id: str = event.get("customer_id", f"CUST-{claim_id}")
    # Optional demo levers: force every branch deterministically.
    #   claim_score  >= 0.9 auto-approve, 0.6..0.9 human review, < 0.6 auto-reject
    #   eligible     overrides the eligibility check
    #   duplicate    overrides the duplicate-claim check (wins over everything)
    claim_score_override = event.get("claim_score")
    eligible_override = event.get("eligible")
    duplicate_override = event.get("duplicate")

    # --- ERROR HANDLING: automatic per-step retries with backoff. ---
    claim: Claim = context.step(
        extract_claim(
            claim_id, flight_number, claim_score_override, eligible_override, duplicate_override
        ),
        config=StepConfig(retry_strategy=intake_retry),
    )

    # --- EFFICIENCY: three independent checks, run concurrently instead of
    # serially. (The duplicate branch also fetches the customer's flight
    # history and offloads it to S3 — see check_duplicate_branch.) Each is
    # bounded by max_concurrency=None (no cap — all three run at once); the
    # whole assessment takes as long as the SLOWEST check, not the sum of all
    # three. ---
    assess_started = time.monotonic()
    try:
        assessed = context.parallel(
            functions=[
                verify_delay_branch(claim),
                check_eligibility_branch(claim),
                check_duplicate_branch(claim, customer_id),
            ],
            name="assess-claim",
        )
        assessed.throw_if_error()
    except (ChildContextError, BatchCompletionError):
        context.logger.exception(
            "flight_claims: assessment failed; dead-lettering"
        )
        outcome = {
            "decision": "assessment_error",
            "result": context.step(reject(claim, "assessment_error")),
        }
        context.step(persist(claim_id, outcome))
        return {"statusCode": 500, "body": outcome}

    # Real wall-clock time on first execution. If this point is ever replayed
    # after the callback below suspends, it reads near-zero instead — proof
    # that context.parallel() replays its checkpointed result rather than
    # re-running the branches.
    assess_elapsed_ms = int((time.monotonic() - assess_started) * 1000)

    # Map each branch's result back to the claim by index (the same technique
    # BatchResult teaches for context.map): functions[0]=delay, [1]=eligibility,
    # [2]=duplicate. The casts assert that contract — context.parallel()'s
    # BatchResult is generic over the whole heterogeneous branch list, so a
    # type checker can't verify per-index types on its own.
    by_index = {item.index: item.result for item in assessed.succeeded()}
    delay_result = cast(DelayAssessment, by_index[0])
    eligibility_result = cast(EligibilityAssessment, by_index[1])
    duplicate_result = cast(DuplicateAssessment, by_index[2])
    assessment: AssessedClaim = {
        **claim,
        **delay_result,
        **eligibility_result,
        **duplicate_result,
        "assessment_ms": assess_elapsed_ms,
    }

    claim_score: float = assessment["claim_score"]

    # --- BRANCHING on checkpointed, parallel-assessed results
    # (replay-deterministic). ---
    if assessment["is_duplicate"]:
        outcome = {
            "decision": "auto_rejected",
            "result": context.step(reject(assessment, "duplicate_claim")),
        }

    elif claim_score >= AUTO_APPROVE_THRESHOLD and assessment["eligible"]:
        outcome = {
            "decision": "auto_approved",
            "result": context.step(auto_approve(assessment)),
        }

    elif claim_score >= HUMAN_REVIEW_THRESHOLD:
        # --- CALLBACK PATTERN: suspend until a human adjudicator responds. ---
        callback = context.create_callback(
            name="human_review",
            config=CallbackConfig(
                timeout=Duration.from_hours(24),
                heartbeat_timeout=Duration.from_hours(2),
                # Decode the adjudicator's JSON payload into a dict. Without a
                # serdes, callback.result() returns the raw payload STRING.
                serdes=JsonSerDes(),
            ),
        )

        context.step(notify_reviewer(callback.callback_id, assessment))

        try:
            # Suspends the execution (no compute charges) until the adjudicator
            # calls SendDurableExecutionCallbackSuccess/Failure.
            review = callback.result() or {}
            outcome = {
                "decision": "human_" + review.get("decision", "approved"),
                "result": review,
            }
        except CallbackTimeoutError:
            context.logger.warning(
                "flight_claims: human review timed out; escalating"
            )
            outcome = {
                "decision": "escalated",
                "result": context.step(reject(assessment, "review_timeout")),
            }
        except CallbackExternalError:
            outcome = {
                "decision": "human_rejected",
                "result": context.step(reject(assessment, "reviewer_rejected")),
            }

    else:
        outcome = {
            "decision": "auto_rejected",
            "result": context.step(reject(assessment, "low_claim_score")),
        }

    context.step(persist(claim_id, outcome))
    return {"statusCode": 200, "body": outcome}
