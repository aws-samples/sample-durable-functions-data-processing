# Human review and fan-out with AWS Lambda durable functions

An AWS CDK (Python) project with two data-processing examples built on AWS
Lambda durable functions:

- **`flight_claims`** processes one flight delay/cancellation compensation
  claim. It shows automatic step retries, independent checks run in parallel
  instead of serially, replay-deterministic branching, offloading a large step
  result to Amazon S3, and a human-review callback that suspends the execution
  with no compute charge.
- **`large_scale_processing`** applies the same kind of unit of work across a
  large JSONL file of flights with the durable functions `map` operation
  (fan-out / fan-in). It shows failure isolation, per-branch retries,
  concurrency control, a retry pass over only the failed items, and keeping
  durable state small by storing data in S3.

[`docs/HANDLER_EXPLAINED.md`](docs/HANDLER_EXPLAINED.md) is a conceptual guide
to the `map` operation: item design, every `completion_config` option, and the
`BatchResult` structure.

## Architecture

The stack `Distributed-Functions-for-Data` (see
`distributed_functions_for_data/main_stack.py`) provisions the resources below.
Function, queue, and table names are prefixed with the stack name; the names
below omit the `<stack>-` prefix.

- **`FlightDataBucket`** (S3) holds the input JSONL, per-chunk objects,
  enriched output, run manifests, and offloaded flight histories.
- **`airport-reference-api`** (Lambda + Function URL, `AWS_IAM` auth) is a mock
  downstream airport lookup. Its `reserved_concurrent_executions=5` gives the
  map's concurrency limit something real to protect.
- **`flight-map-enrichment`** (durable Lambda) runs the `map` pipeline and
  SigV4-signs its calls to the Airport API.
- **`flight-claims`** (durable Lambda) runs the claim workflow.
- **`flight-claims-human-review`** (Amazon SQS) carries human-review requests:
  the claim and its durable-execution callback id.
- **`flight-claims-status`** (Amazon DynamoDB) holds per-claim status keyed by
  `claim_id`.

All functions run on **Python 3.14 / ARM64 (Graviton)**. Lambda assets bundle
**locally via pip, with no Docker required**. Network access is needed on the
first bundle to fetch wheels.

## The `flight_claims` example

One durable execution processes a single claim:

```
extract_claim (step, intake retry)       parse the claim; retries transient intake errors
      |
      v
context.parallel(): three independent checks, concurrently
      |                verify_delay | check_eligibility | fetch_flight_history -> check_duplicate
      |                                                   (history -> S3; checkpoint = S3 key)
      |                bounded by the slowest check, not the sum of all three
      v  (any check failed: explicit try/except recovery -> reject, persist, 500)
assessment branch                  if/elif/else on the checkpointed results
      |  is_duplicate         -> auto-reject                   (reject step)
      |  >= 0.9 and eligible  -> auto-approve                  (auto_approve step)
      |  0.6-0.9              -> human review via callback     (suspend for an adjudicator)
      |  < 0.6                -> auto-reject                   (reject step)
      v
persist (step)                     record the final outcome; return a small result
```

| Concept | Code |
| --- | --- |
| Automatic per-step retry | `context.step(extract_claim(...), config=StepConfig(retry_strategy=intake_retry))` |
| Parallel checks on one item | `context.parallel(functions=[verify_delay_branch(claim), check_eligibility_branch(claim), check_duplicate_branch(claim, customer_id)])` |
| Explicit recovery | `try/except (ChildContextError, BatchCompletionError)` around `context.parallel(...)`, then `reject` + `persist` |
| Branching on checkpointed results | `if assessment["is_duplicate"]` / `elif claim_score >= AUTO_APPROVE_THRESHOLD and eligible` / `elif claim_score >= HUMAN_REVIEW_THRESHOLD` / `else` |
| Suspend for a human | `callback = context.create_callback(...)` then `review = callback.result()` |

**Error handling.** `extract_claim` runs as a step with
`StepConfig(retry_strategy=intake_retry)`, so transient intake errors
(`ThrottlingException`, `ProvisionedThroughputExceededException`,
`ServiceUnavailable`) back off and retry automatically. Each parallel check
retries with the shorter `check_retry` strategy.

**Parallel checks.** `context.parallel()` runs three independent lookups
(delay verification, fare-class eligibility, duplicate-claim check)
concurrently. The assessment takes as long as the slowest check. Each branch
nests a `ctx.step(...)` for per-branch checkpoint and retry, the same mechanism
a `context.map` branch uses with a fixed set of named branches instead of a
dynamic list.

**Offloading a large step result to S3.** `fetch_flight_history` calls a mock
flight-history API whose response (~430 KB at the default
`FLIGHT_HISTORY_LENGTH=2000`) exceeds the ~256 KB checkpoint limit. The step
writes the response to `s3://<data bucket>/flight-history/<claim_id>.json` and
returns only a `FlightHistoryRef` pointer (bucket, key, counts), which is what
gets checkpointed. On replay the pointer comes from the checkpoint without
calling the API again, and `check_duplicate` reads the body from S3 by key. The
fetch runs inside the duplicate-check branch, so the other two checks do not
wait on it.

**Branching.** Plain Python `if/elif/else` on the checkpointed assessment
(`is_duplicate`, `claim_score` against `AUTO_APPROVE_THRESHOLD` (0.9) and
`HUMAN_REVIEW_THRESHOLD` (0.6), and `eligible`). Every value comes from a
checkpoint, so the branch is replay-deterministic.

**Callback pattern.** `context.create_callback()` plus `callback.result()`
suspends the execution with no compute charge until an adjudicator calls
`SendDurableExecutionCallbackSuccess` or `SendDurableExecutionCallbackFailure`.
The step before it enqueues the claim and callback id onto the review queue.

## The `large_scale_processing` example

One durable execution processes the whole dataset:

```
plan_chunks (step)                 split the S3 input JSONL into chunk objects in S3
      |
      v
context.map(chunks, enrich_branch(run))   one durable branch per chunk, in parallel
      |                            - MapConfig(max_concurrency=...)  protect the downstream API
      |                            - CompletionConfig.all_completed() isolate failed chunks
      |                            - first pass enriches with API scope region=US
      v
re-map over only the failed chunks retry with region=INTL so they succeed
      |
      v
write_manifest (step)              summarize outputs to S3; return a small result
```

| Concept | Code |
| --- | --- |
| Fan-out / fan-in | `context.map(chunks, enrich_branch(run), config=MapConfig(...))` |
| Isolate failed branches | `CompletionConfig.all_completed()` |
| Retry only the failures | a second `context.map` over the failed chunks, with the API scope widened from `region=US` to `region=INTL` |
| Protect a downstream | `MapConfig(max_concurrency=N)` against the Airport API's `reserved_concurrent_executions=5` |
| Keep durable state small | input, chunk objects, and enriched output live in S3; durable state holds only pointers and counts |

The fan-out is over chunks of one input file, not individual records. At
`chunk_size` 1,000, 100k flights is about 100 branches, well under the
durable execution operation limit.

## Project layout

```
app.py                                     CDK app entry point
cdk.json                                   CDK toolkit config
distributed_functions_for_data/
  main_stack.py                            the stack: bucket, API, durable functions, queue, table
lambda/
  requirements.txt                         durable execution SDK, bundled into every durable function
  airport_api/                             mock Airport reference API (+ airports.json)
  flight_claims/                           claim workflow
  large_scale_processing/                  map enrichment pipeline
scripts/
  generate_flights.py                      flight-data generator (stdlib only, optional S3 upload)
  demo_region.py                           seed international flights for the fail-then-retry run
tests/unit/                                CDK assertion tests
docs/HANDLER_EXPLAINED.md                  conceptual guide to the map operation
```

## Prerequisites

- **Python 3.14** (`python3 --version`).
- **AWS CDK CLI** new enough for the `aws-cdk-lib` version in
  `requirements.txt`. A *cloud assembly schema version mismatch* error means
  the CLI needs an upgrade (`npm install -g aws-cdk@latest`, or prefix commands
  with `npx aws-cdk@latest`).
- **AWS credentials** for a non-production account
  (`aws sts get-caller-identity`).

## Deploy

All commands run from this sample's folder. Clone the repository, create the
virtual environment, install dependencies, bootstrap the account (first time
only), deploy the stack, and upload a dataset:

```bash
git clone https://github.com/aws-samples/sample-durable-functions-data-processing.git
cd sample-durable-functions-data-processing/human-review-and-fan-out

python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate.bat
python -m pip install --upgrade pip
python -m pip install -r requirements.txt -r requirements-dev.txt

cdk bootstrap
cdk deploy

# 100k flights, deterministic, uploaded to the stack's data bucket.
python scripts/generate_flights.py --count 100000 --seed 42
```

`requirements.txt` holds the CDK app dependencies and `requirements-dev.txt`
holds `pytest` and `boto3` for the scripts. The Lambda dependencies (durable
execution SDK, `boto3`, `tzdata`) are installed at bundle time by CDK, not into
`.venv`.

The stack name is set in `app.py`. Stack names are limited to 42 characters so
the derived Lambda function names fit the 64-character limit.

**Lambda console test events.** The stack saves a shareable console test event
for the enrichment function. The console stores these in an EventBridge schema
registry named `lambda-testevent-schemas`, which the console creates the first
time anyone saves a shareable test event in that account and Region. On the
first deploy into an account and Region without that registry, pass the opt-in
parameter:

```bash
cdk deploy --parameters CreateTestEventRegistry=true
```

The parameter defaults to `false` because CloudFormation conditions are
evaluated from stack inputs, not account state, so the stack cannot detect
whether the registry exists, and creating one that already exists fails. With
`false`, `cdk destroy` also never deletes a registry that holds other
functions' test events. CloudFormation reuses the previous parameter value on
later updates, so it is passed once per stack.

`cdk deploy --hotswap` updates function code without a full CloudFormation
deployment, for development use only.

### Generator options

`scripts/generate_flights.py` resolves the data bucket from the stack's
`FlightDataBucketName` output. Airport codes come from
`lambda/airport_api/airports.json`, the same reference data the Airport API
serves.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--count` | `100000` | number of flight records to generate |
| `--seed` | none | RNG seed for reproducible output |
| `--output` | `data/flights.jsonl` | local output path (always written) |
| `--no-upload` | off | write the local file only; skip the S3 upload |
| `--bucket` | stack's `FlightDataBucketName` | target S3 bucket (overrides stack resolution) |
| `--key` | `input/flights.jsonl` | S3 key to upload to |
| `--stack` | `Distributed-Functions-for-Data` | stack to resolve the bucket from when `--bucket` is omitted |
| `--region` | profile/env | AWS Region for stack lookup and upload |
| `--days` | `1` | spread scheduled departures over this many days |
| `--start-date` | today (UTC) | window start, `YYYY-MM-DD` |
| `--corrupt-rate` | `0.0` | fraction `[0,1]` of records given an unknown airport code (record-level errors; the chunk still succeeds) |
| `--intl-count` | none | concentrate N international flights into the leading records so only the first chunks fail the US-scoped pass |
| `--intl-rate` | `0.02` | fraction `[0,1]` of international flights spread uniformly (even a small rate puts one in every chunk) |

## Run the examples

The commands below use this helper to read stack outputs:

```bash
STACK=Distributed-Functions-for-Data
get() { aws cloudformation describe-stacks --stack-name "$STACK" \
        --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" --output text; }
```

Durable functions are invoked through a qualified ARN, so each command targets
the function's `prod` alias. Invocations are asynchronous
(`--invocation-type Event`) and return `StatusCode 202` right away. Progress
appears in the Lambda console's durable execution view and in CloudWatch Logs.

### Flight claims

```bash
aws lambda invoke \
  --function-name "$(get FlightClaimsAliasArn)" \
  --payload '{"claim_id":"claim-001","flight_number":"FL1234","claim_score":0.95}' \
  --invocation-type Event \
  --cli-binary-format raw-in-base64-out \
  response.json
```

| Field | Default | Meaning |
| --- | --- | --- |
| `claim_id` | *required* | identifies the claim |
| `flight_number` | `"UNKNOWN"` | flight number (informational) |
| `customer_id` | `CUST-<claim_id>` | seeds the mock flight history |
| `claim_score` | random `[0.0, 1.0)` | `>= 0.9` (and eligible) auto-approves, `0.6-0.9` suspends for human review, `< 0.6` auto-rejects |
| `eligible` | random (85% true) | forces the eligibility check; `false` with a high score falls through to human review |
| `duplicate` | random (5% true) | forces the duplicate-claim check; `true` auto-rejects regardless of other fields |

Each claim's progress is recorded in the status table (`ClaimStatusTableName`
output), keyed by `claim_id`: `extracted`, then `pending_review`, `approved`,
or `rejected`.

**Resolving a human review.** A claim scored 0.6-0.9 suspends on the callback.
The review queue (`ClaimReviewQueueUrl` output) holds a message with the
`callback_id` and the claim. Approve or reject it:

```bash
MSG=$(aws sqs receive-message --queue-url "$(get ClaimReviewQueueUrl)" \
  --query 'Messages[0].Body' --output text)
CALLBACK_ID=$(echo "$MSG" | python3 -c 'import json,sys; print(json.load(sys.stdin)["callback_id"])')

# Approve
aws lambda send-durable-execution-callback-success \
  --callback-id "$CALLBACK_ID" \
  --result '{"decision":"approved"}' \
  --cli-binary-format raw-in-base64-out

# Or reject
aws lambda send-durable-execution-callback-failure --callback-id "$CALLBACK_ID"
```

**Parallel speedup.** Each check sleeps for a simulated latency
(`CHECK_LATENCY_MS`, default 300 ms, scaled per check; delay verification is
the slowest). The response body's `assessment_ms` is the wall-clock time for
the three checks. At `CHECK_LATENCY_MS=500` it is about 650 ms, against about
1,350 ms if the checks ran serially. `--environment` replaces the whole
variable map, so the command carries the existing variables. The configuration
change applies to `$LATEST`, so a new version is published and the `prod` alias
is pointed at it:

```bash
FN=$(get FlightClaimsFunctionName)
aws lambda update-function-configuration --function-name "$FN" \
  --environment "Variables={CLAIM_REVIEW_QUEUE_URL=$(get ClaimReviewQueueUrl),CLAIM_TABLE_NAME=$(get ClaimStatusTableName),DATA_BUCKET=$(get FlightDataBucketName),CHECK_LATENCY_MS=500}"
aws lambda wait function-updated --function-name "$FN"
VERSION=$(aws lambda publish-version --function-name "$FN" --query Version --output text)
aws lambda update-alias --function-name "$FN" --name prod --function-version "$VERSION"
```

These commands change the function outside CloudFormation; the next
`cdk deploy` restores the stack's configuration. When an execution resumes
after a human-review callback, `assessment_ms` reads near zero because
`context.parallel()` replays its checkpointed result instead of re-running the
branches.

### Large-scale enrichment

```bash
aws lambda invoke \
  --function-name "$(get LargeScaleProcessingAliasArn)" \
  --payload '{"input_key":"input/flights.jsonl","chunk_size":1000,"max_concurrency":8}' \
  --invocation-type Event \
  --cli-binary-format raw-in-base64-out \
  response.json
```

| Field | Default | Meaning |
| --- | --- | --- |
| `input_key` | `input/flights.jsonl` | S3 key of the JSONL input |
| `chunk_size` | `1000` | flights per chunk (per map branch) |
| `max_concurrency` | `8` | maximum chunks processed in parallel |
| `retry_failed` | `true` | one extra map pass over failed chunks |
| `bucket` | `$DATA_BUCKET` | override the data bucket |
| `output_prefix` | derived from the execution ARN | where chunks, enriched output, and the manifest land |

The run manifest lands at `s3://<bucket>/enriched-runs/<run id>/manifest.json`:

```bash
BUCKET=$(get FlightDataBucketName)
aws s3 ls "s3://$BUCKET/enriched-runs/"
aws s3 cp "s3://$BUCKET/enriched-runs/<run id>/manifest.json" -
```

**Failure isolation and retry.** `scripts/demo_region.py` uploads data whose
leading records are international flights, so the first chunks reference
non-US airports:

```bash
python scripts/demo_region.py            # ~1,500 international flights in the leading records
```

On the first map pass the enrichment calls the Airport API with `region=US`,
which fails fast (HTTP 503 `RegionNotCovered`) for non-US codes. Those chunks
fail and `all_completed()` isolates them. The retry pass re-runs only the
failed chunks with `region=INTL`, which serves every airport, and they succeed.
In the manifest, `retried_chunks` lists the chunks that failed the first pass
and recovered, and `failed_chunks` is empty. The script prints the invoke
command to run. Options: `--intl-count`, `--chunk-size`, `--total`, `--seed`,
`--bucket`, `--stack`.

```bash
python scripts/demo_region.py --reset    # clean US-only data; nothing fails
```

**Concurrency against a downstream.** The Airport API has
`reserved_concurrent_executions=5`. With a high `max_concurrency` (for example
`32`) the Function URL throttles with HTTP 429, and the enrichment step's
transient retry strategy absorbs it. At `max_concurrency` `4` the throttling
stops.

**Slow downstream.** `DEMO_LATENCY_MS` on the Airport API adds a delay to every
response, which makes latency and concurrency effects visible:

```bash
aws lambda update-function-configuration \
  --function-name "$(get AirportApiFunctionName)" \
  --environment "Variables={DEMO_LATENCY_MS=250}"
```

**Record-level errors.** Data generated with `--corrupt-rate 0.01` gives about
1% of records an unknown airport code. Those records carry
`"enrichment_status": "error"` while their chunk still succeeds.

## Tests

```bash
python -m pytest
```

The unit tests synthesize the stack without bundling assets and assert on the
CloudFormation template.

## Cleanup

```bash
cdk destroy
```

The data bucket uses `RemovalPolicy.DESTROY` with `auto_delete_objects`, and
the status table uses `RemovalPolicy.DESTROY`, so both are removed with the
stack.

The functions' CloudWatch log groups remain after `cdk destroy`. Lambda creates
them implicitly, so CloudFormation does not own them, and they do not block the
next deploy. They never expire by default. This includes the log group of the
helper function CDK uses to empty the bucket. To delete every log group for the
stack:

```bash
STACK=Distributed-Functions-for-Data
for lg in $(aws logs describe-log-groups --log-group-name-prefix "/aws/lambda/$STACK-" \
              --query 'logGroups[].logGroupName' --output text); do
  aws logs delete-log-group --log-group-name "$lg"
done
```

## Troubleshooting

- **`Cloud assembly schema version mismatch`**: the `cdk` CLI is older than
  `aws-cdk-lib`. Upgrade with `npm install -g aws-cdk@latest`, or prefix
  commands with `npx aws-cdk@latest`.
- **`AWS::Logs::LogGroup` ... `/aws/lambda/<function>` already exists**: a
  previous deploy's log group still exists. `cdk.json` sets
  `"@aws-cdk/aws-lambda:useCdkManagedLogGroup": false` so CDK emits no
  log-group resources and Lambda reuses existing groups. This error means that
  flag was changed to `true`.
- **Stack in `ROLLBACK_COMPLETE`**: CloudFormation cannot update a stack in
  that state. Delete it with `cdk destroy`, then deploy again.
- **`Schema ... 'lambda-testevent-schemas' was not found`**: the account and
  Region have no console test-event registry yet. Deploy once with
  `--parameters CreateTestEventRegistry=true`.
- **`No S3 bucket provided`**: the enrichment function reads `$DATA_BUCKET`
  (set by the stack) or `event.bucket`. Pass `bucket` in the payload to use a
  different bucket.
- **403 calling the Airport API**: the Function URL uses `AWS_IAM` auth. Only
  the enrichment function's role is granted `lambda:InvokeFunctionUrl`, and it
  SigV4-signs its requests.

## Security

See [CONTRIBUTING](../CONTRIBUTING.md#security-issue-notifications) for more
information.

### Design choices in this sample

This sample is intended for learning and is not production-ready. The stack
applies least-privilege IAM (explicit actions on scoped resources), S3
encryption with SSL-only access, an SSL-only SQS queue, DynamoDB
point-in-time recovery, and `AWS_IAM` auth on the Airport API Function URL.
The following [cdk-nag](https://github.com/cdklabs/cdk-nag) AwsSolutions
findings remain by design:

| Rule | Resource | Reason |
| --- | --- | --- |
| AwsSolutions-IAM4 | Lambda execution roles | The durable functions use the AWS managed `AWSLambdaBasicDurableExecutionRolePolicy`, which grants the checkpoint permissions durable execution requires. The Airport API uses `AWSLambdaBasicExecutionRole` for CloudWatch Logs. |
| AwsSolutions-IAM5 | `FlightDataBucket` object ARNs | Object keys are generated per run (`enriched-runs/<run id>/...`, `flight-history/<claim id>.json`), so the policies grant `s3:GetObject` and `s3:PutObject` on a key prefix. The claims function is limited to `flight-history/*`. |
| AwsSolutions-L1 | All three functions | The functions use `python3.14`, the latest generally available Python runtime. `python3.15` is in public preview. |
| AwsSolutions-S1 | `FlightDataBucket` | Server access logging is not enabled, to keep the sample to one bucket. Production workloads should enable it. |
| AwsSolutions-SQS3 | `ClaimReviewQueue` | The review queue is read by a human adjudicator, not an automated consumer, so there is no retry loop for a dead-letter queue to catch. Production workloads with an automated consumer should add one. |

## License

This library is licensed under the MIT-0 License. See the [LICENSE](../LICENSE)
file.
