"""Durable, high-throughput flight enrichment with the Lambda Durable Functions MAP operation.

This sample's large-scale example. A single durable execution enriches a
large JSONL file of flights against the mock Airport API, using the fan-out /
fan-in ``context.map`` pattern:

    plan_chunks (step)                  split the S3 input into chunk objects in S3
        |
        v
    context.map(chunks, process_chunk)  one branch per chunk, run in parallel
        |                               - max_concurrency protects the downstream API
        |                               - all_completed isolates failed chunks
        |                               - each branch's enrich step retries transient errors
        v
    (optional) re-map over failed chunks only   retry failures without redoing successes
        |
        v
    write_manifest (step)               summarize outputs to S3; return a small result

Design notes that keep this within the durable-execution limits (3,000 ops and
256 KB of state per execution):

* The input and every enriched chunk live in S3. Durable state holds only small
  pointers (chunk keys and per-chunk counts), so a job far too big for one
  Lambda invocation survives suspends, retries, and restarts.
* Enrichment fans out per *chunk*, not per *record*: 100k flights at chunk_size
  1,000 is ~100 branches, comfortably under the operation limit.

Event input (all optional except the input location):

    {
      "bucket":          "<data bucket>",          # defaults to $DATA_BUCKET
      "input_key":       "input/flights.jsonl",
      "output_prefix":   "enriched-runs/<id>/",     # default derived from execution ARN
      "chunk_size":      1000,
      "max_concurrency": 8,
      "retry_failed":    true                        # one extra map pass over failed chunks
    }
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import boto3
import botocore.session
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest

from aws_durable_execution_sdk_python.config import (
    CompletionConfig,
    MapConfig,
    NestingType,
    StepConfig,
)
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
)
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.retries import RetryDecision, RetryPresets

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Reused across invocations within an execution environment.
_S3 = boto3.client("s3")
_BOTO_SESSION = botocore.session.Session()

_DATA_BUCKET = os.environ.get("DATA_BUCKET", "")
_AIRPORT_API_URL = os.environ.get("AIRPORT_API_URL", "").rstrip("/") + "/"
_REGION = os.environ.get("AWS_REGION", "us-east-1")

# On-time threshold (arrival within N minutes of schedule), DOT-style.
_ON_TIME_THRESHOLD_MIN = 15
# Cap codes per batch call so the signed URL stays a sane length.
_CODES_PER_CALL = 40


# --------------------------------------------------------------------------- #
# Airport API client (SigV4-signed calls to the Function URL)
# --------------------------------------------------------------------------- #
def _signed_get(url: str, params: dict[str, str] | None = None) -> dict:
    """GET a Function URL with SigV4 auth. Raises on any non-2xx (to trigger retry).

    Query parameters MUST be passed via ``params`` (not baked into ``url``) so
    botocore builds the canonical query string it signs. We then send the fully
    prepared, signed URL on the wire, so the string that was signed is exactly
    the string urllib requests — otherwise the canonical query diverges (e.g.
    comma encoding) and the Function URL rejects the signature with a 403.
    """
    creds = _BOTO_SESSION.get_credentials()
    aws_req = AWSRequest(method="GET", url=url, params=params)
    # Function URLs authenticate as the "lambda" service.
    SigV4Auth(creds, "lambda", _REGION).add_auth(aws_req)
    prepared = aws_req.prepare()
    http_req = urllib.request.Request(
        prepared.url, headers=dict(prepared.headers.items()), method="GET"
    )
    # Only HTTPS is allowed (checked here), so urlopen never sees file: or
    # other schemes.
    if not prepared.url.startswith("https://"):
        raise ValueError("AIRPORT_API_URL must be an https:// URL")
    with urllib.request.urlopen(http_req, timeout=10) as resp:  # nosec B310 # nosemgrep: python.lang.security.audit.dynamic-urllib-use-detected.dynamic-urllib-use-detected
        return json.loads(resp.read().decode("utf-8"))


def _lookup_airports(codes: set[str], region: str = "INTL") -> dict[str, dict]:
    """Resolve a set of IATA codes to airport records via the batch API endpoint.

    ``region`` scopes the lookup: ``US`` resolves only US airports and makes the
    API fail fast (503 RegionNotCovered) if any code is non-US; ``INTL`` (the
    default) resolves everything. Unknown codes are simply absent from the
    returned mapping. HTTP errors (throttling 429, injected 500s, the 503
    region error) propagate so the enclosing step's retry strategy decides.
    """
    resolved: dict[str, dict] = {}
    ordered = sorted(codes)
    for start in range(0, len(ordered), _CODES_PER_CALL):
        batch = ordered[start:start + _CODES_PER_CALL]
        payload = _signed_get(
            f"{_AIRPORT_API_URL}airports",
            params={"codes": ",".join(batch), "region": region},
        )
        resolved.update(payload.get("results", {}))
    return resolved


def _is_region_not_covered(exc: Exception) -> bool:
    """True when an exception is the API's fail-fast RegionNotCovered (503).

    The region error is a *deliberate* "wrong scope" signal, not a transient
    blip — so the step should NOT retry it in-branch. It surfaces as a urllib
    HTTPError with status 503 and a body naming the error.
    """
    if isinstance(exc, urllib.error.HTTPError) and exc.code == 503:
        return True
    return "RegionNotCovered" in str(exc)


def _enrichment_retry(exc: Exception, attempt: int) -> "RetryDecision":
    """Step retry policy: fail fast on RegionNotCovered, else retry transiently.

    A RegionNotCovered 503 means "this scope can't serve these codes" — retrying
    in-branch would just hit the same wall, so we return no_retry() and let the
    chunk fail straight through to the map-level retry pass (which widens the
    scope to INTL). Everything else (e.g. 429 throttling) uses the normal
    transient backoff.
    """
    if _is_region_not_covered(exc):
        return RetryDecision.no_retry()
    return RetryPresets.transient()(exc, attempt)


# --------------------------------------------------------------------------- #
# Enrichment helpers
# --------------------------------------------------------------------------- #
def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0088
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = (
        math.sin(dphi / 2) ** 2
        + math.cos(p1) * math.cos(p2) * math.sin(dlambda / 2) ** 2
    )
    return 2 * r * math.asin(math.sqrt(a))


def _parse(iso: str | None) -> datetime | None:
    if not iso:
        return None
    return datetime.fromisoformat(iso.replace("Z", "+00:00"))


def _delay_min(scheduled: str | None, actual: str | None) -> int | None:
    ds, da = _parse(scheduled), _parse(actual)
    if ds is None or da is None:
        return None
    return int((da - ds).total_seconds() // 60)


def _local(iso: str | None, tz: str) -> str | None:
    moment = _parse(iso)
    if moment is None:
        return None
    return moment.astimezone(ZoneInfo(tz)).isoformat()


def _airport_summary(record: dict) -> dict:
    return {
        "code": record.get("code"),
        "name": record.get("name"),
        "city": record.get("city"),
        "country": record.get("country"),
        "lat": record.get("lat"),
        "lon": record.get("lon"),
        "tz": record.get("tz"),
    }


def _enrich_record(flight: dict, airports: dict[str, dict]) -> dict:
    """Return a new enriched flight record. Unknown codes are a record-level error."""
    origin_code = flight.get("origin")
    dest_code = flight.get("destination")
    origin = airports.get(origin_code)
    dest = airports.get(dest_code)

    enriched = dict(flight)

    if origin is None or dest is None:
        missing = [c for c, a in ((origin_code, origin), (dest_code, dest)) if a is None]
        enriched["enrichment_status"] = "error"
        enriched["enrichment_error"] = f"unknown airport code(s): {', '.join(missing)}"
        return enriched

    dep_delay = _delay_min(flight.get("scheduled_departure"), flight.get("actual_departure"))
    arr_delay = _delay_min(flight.get("scheduled_arrival"), flight.get("actual_arrival"))

    if flight.get("actual_departure") is None and flight.get("actual_arrival") is None:
        status = "cancelled"
    elif arr_delay is not None and arr_delay > _ON_TIME_THRESHOLD_MIN:
        status = "delayed"
    else:
        status = "on_time"

    enriched.update(
        {
            "origin_airport": _airport_summary(origin),
            "destination_airport": _airport_summary(dest),
            "distance_km": round(
                _haversine_km(origin["lat"], origin["lon"], dest["lat"], dest["lon"]), 1
            ),
            "departure_delay_min": dep_delay,
            "arrival_delay_min": arr_delay,
            "flight_status": status,
            "scheduled_departure_local": _local(
                flight.get("scheduled_departure"), origin["tz"]
            ),
            "scheduled_arrival_local": _local(
                flight.get("scheduled_arrival"), dest["tz"]
            ),
            "actual_departure_local": _local(flight.get("actual_departure"), origin["tz"]),
            "actual_arrival_local": _local(flight.get("actual_arrival"), dest["tz"]),
            "enrichment_status": "ok",
        }
    )
    return enriched


# --------------------------------------------------------------------------- #
# Stage work — plain functions that do the real S3 work for each stage. The
# durable steps below are thin wrappers over these, so the checkpointed unit
# and the work it performs are cleanly separated.
# --------------------------------------------------------------------------- #
def _plan_chunks(
    bucket: str, input_key: str, output_prefix: str, chunk_size: int
) -> list[dict]:
    """Split the S3 input JSONL into chunk objects; return small descriptors."""
    logger.info(
        "Planning chunks from s3://%s/%s (chunk_size=%d)", bucket, input_key, chunk_size
    )
    body = _S3.get_object(Bucket=bucket, Key=input_key)["Body"]

    descriptors: list[dict] = []
    buffer: list[str] = []

    def flush(index: int) -> None:
        chunk_key = f"{output_prefix}chunks/chunk-{index:05d}.jsonl"
        _S3.put_object(
            Bucket=bucket,
            Key=chunk_key,
            Body=("\n".join(buffer) + "\n").encode("utf-8"),
        )
        descriptors.append(
            {"index": index, "chunk_key": chunk_key, "line_count": len(buffer)}
        )

    index = 0
    for raw in body.iter_lines():
        line = raw.decode("utf-8").strip() if isinstance(raw, bytes) else raw.strip()
        if not line:
            continue
        buffer.append(line)
        if len(buffer) >= chunk_size:
            flush(index)
            index += 1
            buffer = []
    if buffer:
        flush(index)

    logger.info("Planned %d chunk(s)", len(descriptors))
    return descriptors


def _enrich_chunk(
    chunk: dict, bucket: str, output_prefix: str, region: str = "INTL"
) -> dict:
    """Enrich every flight in one chunk and write the result back to S3.

    ``region`` is the Airport API scope used for the lookups (see
    ``_lookup_airports``). Idempotent: re-running rewrites the same
    deterministic output key, so step retries and replays are safe.
    """
    chunk_key = chunk["chunk_key"]
    index = chunk["index"]
    raw = _S3.get_object(Bucket=bucket, Key=chunk_key)["Body"].read().decode("utf-8")
    flights = [json.loads(line) for line in raw.splitlines() if line.strip()]

    # One batched lookup for every airport referenced in the chunk keeps the
    # downstream call count tiny regardless of chunk size.
    codes = {
        code
        for flight in flights
        for code in (flight.get("origin"), flight.get("destination"))
        if code
    }
    airports = _lookup_airports(codes, region)

    enriched = [_enrich_record(flight, airports) for flight in flights]
    record_errors = sum(1 for r in enriched if r.get("enrichment_status") == "error")

    output_key = f"{output_prefix}enriched/chunk-{index:05d}.jsonl"
    _S3.put_object(
        Bucket=bucket,
        Key=output_key,
        Body=("\n".join(json.dumps(r) for r in enriched) + "\n").encode("utf-8"),
    )
    logger.info(
        "Chunk %d: enriched %d flights (%d record error(s)) -> s3://%s/%s",
        index, len(enriched), record_errors, bucket, output_key,
    )
    return {
        "index": index,
        "output_key": output_key,
        "records": len(enriched),
        "record_errors": record_errors,
    }


def _write_manifest(bucket: str, output_prefix: str, summary: dict) -> str:
    """Persist a run manifest to S3 and return its key."""
    manifest_key = f"{output_prefix}manifest.json"
    _S3.put_object(
        Bucket=bucket, Key=manifest_key, Body=json.dumps(summary, indent=2).encode("utf-8")
    )
    logger.info("Wrote manifest s3://%s/%s", bucket, manifest_key)
    return manifest_key


def _build_run_summary(
    *,
    bucket: str,
    input_key: str,
    output_prefix: str,
    chunks: list[dict],
    chunk_summaries: list[dict],
    failed_indexes: list[int],
    retried_indexes: list[int],
    max_concurrency: int,
) -> dict:
    """Aggregate per-chunk summaries into the run manifest payload."""
    return {
        "input": f"s3://{bucket}/{input_key}",
        "output_prefix": f"s3://{bucket}/{output_prefix}",
        "total_chunks": len(chunks),
        "succeeded_chunks": len(chunk_summaries),
        "failed_chunks": sorted(failed_indexes),
        "retried_chunks": sorted(retried_indexes),
        "records_in": sum(c["line_count"] for c in chunks),
        "records_enriched": sum(s["records"] for s in chunk_summaries),
        "records_with_errors": sum(s["record_errors"] for s in chunk_summaries),
        "max_concurrency": max_concurrency,
        "chunk_outputs": sorted(s["output_key"] for s in chunk_summaries),
    }


# --------------------------------------------------------------------------- #
# Durable steps — thin @durable_step wrappers over the underscore-prefixed
# functions above (same name, minus the leading underscore). The wrapper is
# what makes each a replay-safe, retryable checkpoint; the work itself lives
# above.
# --------------------------------------------------------------------------- #
@durable_step
def plan_chunks(
    step_context: StepContext,
    bucket: str,
    input_key: str,
    output_prefix: str,
    chunk_size: int,
) -> list[dict]:
    return _plan_chunks(bucket, input_key, output_prefix, chunk_size)


@durable_step
def enrich_chunk(
    step_context: StepContext,
    chunk: dict,
    bucket: str,
    output_prefix: str,
    region: str = "INTL",
) -> dict:
    return _enrich_chunk(chunk, bucket, output_prefix, region)


@durable_step
def write_manifest(
    step_context: StepContext, bucket: str, output_prefix: str, summary: dict
) -> str:
    return _write_manifest(bucket, output_prefix, summary)


# --------------------------------------------------------------------------- #
# Run configuration — event parsing + a replay-stable output prefix. Not a
# durable concept, just reading the event and hashing the execution ARN so
# replays land in the same S3 prefix.
# --------------------------------------------------------------------------- #
@dataclass
class RunConfig:
    bucket: str
    input_key: str
    output_prefix: str
    chunk_size: int
    max_concurrency: int
    retry_failed: bool


def _resolve_run(event: dict, context: DurableContext) -> RunConfig:
    """Parse the event and derive a replay-stable output prefix."""
    bucket = event.get("bucket") or _DATA_BUCKET
    if not bucket:
        raise ValueError("No S3 bucket provided (event.bucket or $DATA_BUCKET)")

    # Short, stable id for the S3 output prefix, derived from the execution ARN.
    run_id = hashlib.sha256(
        context.execution_context.durable_execution_arn.encode("utf-8")
    ).hexdigest()[:12]

    return RunConfig(
        bucket=bucket,
        input_key=event.get("input_key", "input/flights.jsonl"),
        output_prefix=event.get("output_prefix") or f"enriched-runs/{run_id}/",
        chunk_size=int(event.get("chunk_size", 1000)),
        max_concurrency=int(event.get("max_concurrency", 8)),
        retry_failed=bool(event.get("retry_failed", True)),
    )


def enrich_branch(run: RunConfig, region: str = "US"):
    """Build the per-chunk map branch: enrich one chunk as a retrying durable step.

    ``region`` is the Airport API scope the branch enriches against. The first
    map pass uses the default ``US`` (so chunks with a non-US airport fail fast
    with RegionNotCovered); the retry pass passes ``INTL`` to widen the scope so
    those same chunks succeed. Returns the function ``context.map`` invokes for
    each chunk. The durable idea it carries is ``ctx.step(...)`` with a retry
    strategy, run once per chunk by ``context.map``.
    """
    def process_chunk(ctx: DurableContext, chunk: dict, _i: int, _all) -> dict:
        return ctx.step(
            enrich_chunk(chunk, run.bucket, run.output_prefix, region),
            name=f"enrich-chunk-{chunk['index']:05d}",
            # Fail fast on RegionNotCovered (so the chunk drops to the map-level
            # retry below); retry genuine transient errors in-branch.
            config=StepConfig(retry_strategy=_enrichment_retry),
        )

    return process_chunk


def _retry_failures(
    context: DurableContext,
    run: RunConfig,
    summaries: list[dict],
    failed_chunks: list[dict],
    config: MapConfig,
) -> tuple[list[dict], list[int], list[int]]:
    """Second map pass over ONLY the chunks that failed; successes are never redone.

    The handler has already consumed the BatchResult (``result.get_results()``
    and ``result.failed()``) and passes in the success ``summaries`` plus the
    ``failed_chunks`` descriptors. This helper just re-maps the failures and
    tracks the index bookkeeping the manifest needs.
    """
    chunk_summaries = list(summaries)
    failed_indexes = [c["index"] for c in failed_chunks]
    retried_indexes: list[int] = []

    if run.retry_failed and failed_chunks:
        context.logger.info(
            "Retrying %d failed chunk(s): %s", len(failed_chunks), failed_indexes
        )
        # Widen the scope to INTL on the retry so chunks that failed with
        # RegionNotCovered under the first pass's US scope now resolve.
        retry_result = context.map(
            failed_chunks, enrich_branch(run, "INTL"),
            name="retry-failed-chunks", config=config,
        )
        chunk_summaries.extend(retry_result.get_results())
        retried_indexes = failed_indexes
        failed_indexes = [failed_chunks[item.index]["index"] for item in retry_result.failed()]

    return chunk_summaries, failed_indexes, retried_indexes


def _finalize(
    context: DurableContext,
    run: RunConfig,
    chunks: list[dict],
    chunk_summaries: list[dict],
    failed_indexes: list[int],
    retried_indexes: list[int],
) -> dict:
    """Aggregate the run, write the manifest (a durable step), return a small dict."""
    summary = _build_run_summary(
        bucket=run.bucket,
        input_key=run.input_key,
        output_prefix=run.output_prefix,
        chunks=chunks,
        chunk_summaries=chunk_summaries,
        failed_indexes=failed_indexes,
        retried_indexes=retried_indexes,
        max_concurrency=run.max_concurrency,
    )
    manifest_key = context.step(
        write_manifest(run.bucket, run.output_prefix, summary), name="write-manifest"
    )
    return {
        "statusCode": 200,
        "manifest": f"s3://{run.bucket}/{manifest_key}",
        **{k: summary[k] for k in (
            "total_chunks", "succeeded_chunks", "failed_chunks",
            "records_in", "records_enriched", "records_with_errors",
        )},
    }


# --------------------------------------------------------------------------- #
# Durable entry point — the whole durable flow, top to bottom:
#   plan STEP  ->  MAP fan-out (concurrency cap + failure isolation)
#              ->  retry re-map over only the failures  ->  finalize.
# Everything it calls is defined above; each line here is a durable construct.
# --------------------------------------------------------------------------- #
@durable_execution
def lambda_handler(event: dict, context: DurableContext) -> dict:
    run = _resolve_run(event, context)

    # A durable STEP: split the input into S3 chunk objects.
    chunks: list[dict] = context.step(
        plan_chunks(
            bucket=run.bucket,
            input_key=run.input_key,
            output_prefix=run.output_prefix,
            chunk_size=run.chunk_size,
        ),
        name="plan-chunks",
    )
    if not chunks:
        raise ValueError(f"No flight records found in s3://{run.bucket}/{run.input_key}")

    # At this point, we have the chunks, each a descriptor of a slice of the work to do.
    # An item describes work, but does not contain work due to the ~256 KB durable-state
    # (checkpoint) limit. (Fan-out is per-chunk, not per-record, for the ~3,000-operation limit.)
    # chunks - [ { "index": 0, "chunk_key": ".../chunk-00000.jsonl", "line_count": 1000 }, ...
    #            { "index": n, "chunk_key": ".../chunk-00nnn.jsonl", "line_count": 1000 } ]

    # The MAP: one branch per chunk, capped concurrency, failures isolated.
    # completion_config: default()=fail-fast | all_completed() | all_successful() | first_successful()
    #            thresholds: min_successful, tolerated_failure_count, tolerated_failure_percentage
    #            custom:     should_complete=<predicate>
    # nesting_type: NESTED or FLAT
    # item_namer: label for each branch in the execution history (observability)

    config = MapConfig(
        max_concurrency=run.max_concurrency,                 # protect the downstream API
        completion_config=CompletionConfig.all_completed(),  # try all branches
        nesting_type=NestingType.NESTED,
        item_namer=lambda item, _i: f"chunk-{item['index']:05d}",
    )
    result = context.map(
        inputs=chunks,
        func=enrich_branch(run),
        name="enrich-chunks",
        config=config,
    )

    # The result is a BatchResult, which has:
    #   result.get_results()      # list of the result values of SUCCEEDED branches (includes None if a branch returned None)
    #   result.succeeded()        # list[BatchItem] that succeeded
    #   result.failed()           # list[BatchItem] that failed  (each has .index and .error)
    #   result.started()          # list[BatchItem] still STARTED (never reached terminal)
    #   result.get_errors()       # list[ErrorObject] from the failed branches
    #   result.success_count      # int
    #   result.failure_count      # int
    #   result.has_failure        # bool — any branch failed?
    #   result.status             # overall BatchItemStatus (SUCCEEDED / FAILED)
    #   result.completion_reason  # why the batch ended (see below)
    #   result.throw_if_error()   # raise if the batch failed; no-op if it succeeded

    # Consume the BatchResult: the successes, and the chunks whose branch failed.
    # A failed BatchItem's .index is its position in `chunks`, so chunks[item.index]
    # is the descriptor to retry.
    summaries = result.get_results()
    failed_chunks = [chunks[item.index] for item in result.failed()]

    # Retry only the failed branches, then aggregate + write the manifest.
    summaries, failed, retried = _retry_failures(
        context=context,
        run=run,
        summaries=summaries,
        failed_chunks=failed_chunks,
        config=config,
    )
    return _finalize(
        context=context,
        run=run,
        chunks=chunks,
        chunk_summaries=summaries,
        failed_indexes=failed,
        retried_indexes=retried,
    )
