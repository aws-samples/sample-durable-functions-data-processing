# Durable Functions MAP — a conceptual guide

This explains the durable-functions **MAP** pattern as concepts you can reuse,
not as a tour of one file. It uses the flight-enrichment demo only for concrete
examples. Three things to take away:

1. the shape of the data flowing through a map (the **items**, the **branch**,
   the **result**),
2. every **`completion_config`** option and when to reach for it,
3. the structure of the **`BatchResult`** and how to consume it.

The handler that ties it together is short on purpose:

```python
@durable_execution
def lambda_handler(event, context):
    run = _resolve_run(event, context)                      # parse input (plumbing)

    chunks = context.step(plan_chunks(...), name="plan-chunks")   # produce the items

    config = MapConfig(
        max_concurrency=run.max_concurrency,
        completion_config=CompletionConfig.all_completed(),
        nesting_type=NestingType.NESTED,
        item_namer=lambda item, _i: f"chunk-{item['index']:05d}",
    )
    result = context.map(chunks, enrich_branch(run), name="enrich-chunks", config=config)  # fan out

    summaries = result.get_results()
    failed_chunks = [chunks[item.index] for item in result.failed()]
    summaries, failed, retried = _retry_failures(context, run, summaries, failed_chunks, config)
    return _finalize(context, run, chunks, summaries, failed, retried)
```

`context.map(items, branch, config)` is the whole idea: run **one function**
across **a collection**, in parallel, under a **completion policy**, and get back
a **`BatchResult`**. The rest of this doc unpacks those four nouns.

---

## 1. The items — what you map over

`context.map` takes a plain Python sequence. Each element is one unit of work and
becomes one branch. The SDK does not care what the elements are, with one
practical constraint: **they get checkpointed, so keep them small and
JSON-serializable.**

In the demo the items are *chunk descriptors* produced by the plan step. One
looks like this:

```json
{ "index": 0, "chunk_key": "enriched-runs/ab12cd/chunks/chunk-00000.jsonl", "line_count": 1000 }
```

The list handed to `context.map` is just a hundred of those:

```python
chunks = [
    {"index": 0, "chunk_key": ".../chunk-00000.jsonl", "line_count": 1000},
    {"index": 1, "chunk_key": ".../chunk-00001.jsonl", "line_count": 1000},
    ...
    {"index": 99, "chunk_key": ".../chunk-00099.jsonl", "line_count": 1000},
]
```

Notice what the descriptor is **not**: it is not the 1,000 flight records. The
rows live in an S3 object; the descriptor carries only a *pointer* (`chunk_key`)
and a little metadata. This is the key scaling move — see "Why items are small"
below.

### Why items are small (the checkpoint-offload idea)

A durable execution has limits: roughly **3,000 operations** and **256 KB of
durable state** per execution. Two consequences shape how you design the items:

- **Fan out over coarse units, not fine ones.** One branch per *chunk* (1,000
  rows) gives ~100 branches for 100k rows. One branch per *row* would be 100,000
  branches and blow the operation ceiling. The grain of your items is a design
  decision bounded by the op limit.
- **Put bulk data in a store, pass references.** The item holds `chunk_key`, not
  the rows; the branch result holds `output_key` and counts, not the enriched
  rows. Durable state stays tiny no matter how big the dataset is. If your items
  or results were the actual payloads, a large job would exceed the 256 KB state
  limit.

A good rule: **an item should describe work, not contain it.**

---

## 2. The branch — the function run per item

The second argument to `context.map` is the function invoked once per item. Its
signature is:

```python
def branch(ctx: DurableContext, item, index: int, all_items) -> R:
    ...
```

- `ctx` — a child `DurableContext` for *this branch*. Call `ctx.step(...)` inside
  it and that step is checkpointed and retryable independently of sibling
  branches.
- `item` — the element from the collection (one chunk descriptor here).
- `index` — its position in the input list.
- `all_items` — the whole collection, if a branch needs sibling context (rarely).

In the demo the branch runs the enrichment as a nested, retrying step:

```python
def process_chunk(ctx, chunk, _i, _all):
    return ctx.step(
        enrich_chunk(chunk, bucket, output_prefix, region),
        name=f"enrich-chunk-{chunk['index']:05d}",
        config=StepConfig(retry_strategy=_enrichment_retry),
    )
```

Two ideas worth separating:

- **The branch runs in parallel with its siblings** (bounded by
  `max_concurrency`). That is the fan-out.
- **A step *inside* the branch** gives per-branch checkpointing and per-branch
  retry. The step's retry strategy decides *which* errors retry in-branch: a
  genuine transient error (e.g. a 429) retries right there, but a deliberate
  "wrong scope" error fails fast so the chunk drops to the **map-level** retry
  (the second `context.map` over only the failures).

Note the `region` the branch enriches with: the first map pass uses `US` (so a
chunk with a non-US airport fails fast), and the retry pass passes `INTL` to
widen the scope — same chunk, different scope, now it succeeds. That is how a
failed batch is made to recover deterministically in one run.

Whatever the branch **returns** becomes that branch's result in the
`BatchResult`. Keep it small for the same reason items are small — in the demo it
returns `{"index", "output_key", "records", "record_errors"}`, i.e. a pointer and
counts, not data.

---

## 3. `completion_config` — when is the batch "done"?

`completion_config` answers one question: as branches succeed and fail, **when
should the map stop, and does the whole batch count as success or failure?** It
does *not* change what runs — every branch still runs unless a policy decides to
stop scheduling. It governs the batch-level outcome.

All options come from `CompletionConfig`. Here is each one.

### Default — `CompletionConfig()` — fail fast

```python
context.map(items, branch)                       # no config, or MapConfig() with defaults
```

The first failed branch exceeds tolerance and the batch is marked failed. Sibling
branches already running are allowed to finish, but no new ones are scheduled
once a failure makes the outcome unwinnable. Use when **any failure means the
whole job is invalid** and there's no point continuing.

### `CompletionConfig.all_completed()` — run everything, isolate failures

```python
completion_config=CompletionConfig.all_completed()
```

Tolerates 100% failures, so **no failure ever stops the batch** — every branch
runs to a terminal state. The batch "succeeds" in the sense that it completed;
you then inspect the result to see which branches failed and decide what to do.
This is the isolate-and-handle-later policy, and it's what the demo uses: the
few chunks containing an international flight fail the US-scoped first pass alone
while every other chunk succeeds — then the retry pass heals them.

### `CompletionConfig.all_successful()` — every branch must pass

```python
completion_config=CompletionConfig.all_successful()
```

Tolerates zero failures. Lets running siblings finish, but any single failure
makes the **batch** fail. The difference from the fail-fast default is intent and
readability: `all_successful()` says "I expect all of these to pass" explicitly.
Use for a batch that is only meaningful if every piece succeeds (e.g. all shards
of a transaction).

### `CompletionConfig.first_successful()` — race / quorum of one

```python
completion_config=CompletionConfig.first_successful()
```

Completes as soon as **one** branch succeeds (`min_successful=1`). Use for
redundant work where you only need one winner — query three mirrors, take the
first that answers; try three payment providers, stop at the first success.

### Threshold: `min_successful=N`

```python
completion_config=CompletionConfig(min_successful=3)
```

Complete successfully once **N** branches have succeeded; remaining branches stop
being scheduled. "I need any 3 of these 10 to work." `first_successful()` is just
`min_successful=1`.

### Threshold: `tolerated_failure_count=N`

```python
completion_config=CompletionConfig(tolerated_failure_count=2)
```

Keep going until **more than N** branches fail, then fail the batch. "I can live
with up to 2 bad shards; the 3rd means abort."

### Threshold: `tolerated_failure_percentage=P`

```python
completion_config=CompletionConfig(tolerated_failure_percentage=10)
```

Fail the batch once **more than P%** of branches fail. The percentage version of
the above, for when batch size varies. "Abort if more than 10% of chunks fail."

Thresholds combine — e.g. "succeed if at least 3 pass, but abort if more than 2
fail":

```python
completion_config=CompletionConfig(min_successful=3, tolerated_failure_count=2)
```

### Custom: `should_complete=<predicate>`

```python
def quorum(status):                              # status: CompletionStatus
    if status.success_count >= 2:
        return complete_batch()                  # CompletionOutcome.SUCCEEDED (default)
    if status.failure_count > 3:
        return complete_batch(CompletionOutcome.FAILED)
    return continue_batch()

completion_config=CompletionConfig(should_complete=quorum)
```

Full control. The predicate receives a `CompletionStatus` snapshot
(`success_count`, `failure_count`, `completed_count`, `total_count`, and
per-branch `items`) and returns `continue_batch()` or
`complete_batch(outcome)`. A `FAILED` outcome can fail the batch **even when no
individual branch failed** — e.g. a required quorum couldn't be met.

Three rules the SDK enforces, because the predicate runs during live execution
*and* on replay:

- **Deterministic & side-effect-free** — decide only from the `status` argument,
  never from external state or the clock.
- **Monotonic** — once a given level of progress would complete the batch, more
  progress must not flip it back to "continue." (On resume, completed branches
  replay in arbitrary order, so a non-monotonic predicate could decide
  differently.)
- **Cannot be combined** with the threshold fields — it's one or the other.

### Choosing — a cheat sheet

| You want… | Use |
| --- | --- |
| Abort on the first failure | default `CompletionConfig()` |
| Run them all, sort out failures afterward | `all_completed()` |
| All-or-nothing (every branch must pass) | `all_successful()` |
| First winner wins | `first_successful()` |
| "At least N must pass" | `min_successful=N` |
| "At most N (or P%) may fail" | `tolerated_failure_count` / `_percentage` |
| Anything bespoke / quorum / cross-branch rule | `should_complete=` |

---

## 4. `BatchResult` — the structure of `result` and how to use it

`context.map(...)` returns a `BatchResult` once the batch completes. Internally
it holds a list of **`BatchItem`** — one per branch — plus the overall completion
reason.

### A `BatchItem`

```python
BatchItem(
    index=0,                       # position in the input list (stable across replay)
    status=BatchItemStatus.SUCCEEDED,   # SUCCEEDED | FAILED | STARTED
    result={"index": 0, "output_key": ".../enriched/chunk-00000.jsonl",
            "records": 1000, "record_errors": 0},   # the branch's return value (None if failed)
    error=None,                    # an ErrorObject when status is FAILED (None otherwise)
)
```

- `index` ties the item back to `your_items[index]` — this is how you recover
  *which* input a result or failure belongs to.
- `status` is one of three values. `STARTED` can appear for branches that were
  scheduled but not completed (e.g. when a policy stopped the batch early, so
  some branches never reached a terminal state).
- `result` is populated only on success; `error` only on failure.

### The methods you'll actually call

```python
result.get_results()      # list of the result values of SUCCEEDED branches (includes None if a branch returned None)
result.succeeded()        # list[BatchItem] that succeeded
result.failed()           # list[BatchItem] that failed  (each has .index and .error)
result.started()          # list[BatchItem] still STARTED (never reached terminal)
result.get_errors()       # list[ErrorObject] from the failed branches

result.success_count      # int
result.failure_count      # int
result.has_failure        # bool — any branch failed?
result.status             # overall BatchItemStatus (SUCCEEDED / FAILED)
result.completion_reason  # why the batch ended (see below)

result.throw_if_error()   # raise if the batch failed; no-op if it succeeded
```

### Common consumption patterns

**Collect the successes** (the ordinary happy path):

```python
summaries = result.get_results()        # [{...}, {...}, ...] for every SUCCEEDED branch
total_rows = sum(s["records"] for s in summaries)
```

**Find out which inputs failed** — map failed items back to the originals via
`index`:

```python
failed_items  = result.failed()                       # list[BatchItem]
failed_inputs = [your_items[it.index] for it in failed_items]
for it in failed_items:
    log.warning("item %d failed: %s", it.index, it.error)
```

This `index`-based mapping is exactly how the demo's retry pass rebuilds the list
of failed chunks to re-map over — only the failures get a second attempt;
successes are never recomputed.

**Just blow up if anything failed** (when you chose an all-or-nothing policy):

```python
result.throw_if_error()                 # raises on the first failure, or BatchCompletionError
summaries = result.get_results()        # safe: everything succeeded
```

**Branch on how the batch ended** using `completion_reason`
(a `CompletionReason`):

| Value | Meaning |
| --- | --- |
| `ALL_COMPLETED` | every branch reached a terminal state |
| `MIN_SUCCESSFUL_REACHED` | stopped early because `min_successful` was hit |
| `FAILURE_TOLERANCE_EXCEEDED` | stopped/failed because failures passed the tolerance |
| `CUSTOM_COMPLETION_SUCCEEDED` | a `should_complete` predicate completed it as success |
| `CUSTOM_COMPLETION_FAILED` | a `should_complete` predicate completed it as failure |

```python
if result.completion_reason is CompletionReason.FAILURE_TOLERANCE_EXCEEDED:
    # too many branches failed — surface a batch-level error
    ...
```

Note `throw_if_error()` and `completion_reason` interact with
`completion_config`: a batch run under `all_completed()` won't "throw" just
because some branches failed (completing was the whole point) — you inspect
`failed()` yourself. Under `all_successful()` or a `FAILED` custom outcome,
`throw_if_error()` raises.

---

## Putting it together — the mental model

```
items            a small, JSON-serializable list; each element describes one unit of work
   │             (keep bulk data in S3/DB; pass references)
   ▼
context.map(items, branch, config)
   │  branch      runs once per item, in parallel up to max_concurrency;
   │              nest ctx.step(...) inside for per-branch checkpoint + retry
   │  config      completion_config decides the batch outcome; max_concurrency caps
   │              parallelism; nesting_type trades observability vs cost/scale
   ▼
BatchResult      one BatchItem per branch (index, status, result|error) + completion_reason
   │             get_results() / succeeded() / failed() / started() / get_errors()
   ▼             success_count / failure_count / has_failure / status / throw_if_error()
your code        collect successes, map failures back by index, retry or report
```

The pattern generalizes well beyond flights: any "split a big input into units,
process them in parallel with controlled concurrency and failure handling, then
aggregate" job is a `context.map` with the right `completion_config` and a branch
that reads/writes a store and returns a small summary.

---

## A companion example — `flight_claims`

This sample ships a second example, `flight_claims`, that complements the MAP
pattern this doc covers. Instead
of fanning out over a collection, it processes a single compensation claim and
leans on three other durable-functions building blocks, including
`context.parallel()`, the single-item sibling of `context.map()` this doc
describes above. The code is in `lambda/flight_claims/index.py`:

- **Error handling** — a per-step retry strategy
  (`StepConfig(retry_strategy=...)`) absorbs transient failures, plus explicit
  `try/except (ChildContextError, BatchCompletionError)` recovery around a
  parallel assessment.
- **Parallel checks (efficiency at the single-item level)** —
  `context.parallel()` runs a small, fixed set of named branches on ONE item
  concurrently, each nesting a `ctx.step(...)` for per-branch checkpoint +
  retry — the same mechanism `context.map()` uses, at a different cardinality.
  It returns the same `BatchResult` type map does, so `.succeeded()`,
  `.throw_if_error()`, and index-based result mapping all carry over directly.
- **Branching** — ordinary Python `if/elif/else` on checkpointed results,
  which is replay-deterministic because every value came from a checkpoint.
- **Callback pattern** — `context.create_callback()` + `callback.result()`
  suspend the execution (no compute charge) until a human responds via
  `SendDurableExecutionCallbackSuccess`/`SendDurableExecutionCallbackFailure`.
