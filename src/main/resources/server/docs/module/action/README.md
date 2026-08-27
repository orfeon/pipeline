# Action Modules

Action modules execute an operation against an external service from inside the pipeline — run a BigQuery job, launch a Vertex AI batch prediction job, call an HTTP endpoint, write a result-history file. They give a pipeline lightweight workflow steps (run a job after files are written, scale an instance before reading, notify results) without an external orchestrator.

Actions are the fourth module kind next to sources / transforms / sinks and are declared in their own config section, `actions`. The `module` field names the service:

```yaml
actions:
  - name: load_to_bq
    module: bigquery          # the action service
    operation: jobs.load      # which operation of the service (declared per service)
    trigger: once             # optional (default): once | perElement | collect
    waits: [store]
    parameters:               # parameters of that operation
      sourceUris: [gs://my-bucket/export/*.avro]
      destinationTable: myproject.mydataset.loaded
```

Available services: [bigquery](bigquery.md) · [vertexai_gemini](vertexai_gemini.md) · [storage](storage.md) · [tasks](tasks.md) · [http](http.md)

## The two planes

A pipeline config has two kinds of streams:

- **Data plane** — the records your pipeline processes. Data is born in `sources`, reshaped by `transforms`, and terminates at `sinks`.
- **Control plane** — records *about* execution: what a sink wrote (e.g. the storage sink's written-file records), what job an action launched (its result envelope), plus pure completion signals (`waits`).

The rules that keep a config readable:

| reference | may point at |
|---|---|
| `inputs` of a data transform/sink | data outputs (sources, transforms) |
| `inputs` of an **action** module | anything — data records (to parameterize jobs) or control records (to chain on results) |
| `waits` of any module | anything (pure completion signal) |

A data transform/sink consuming control records via `inputs` produces an assembly-time **warning** (not an error): it usually means the step should be expressed differently, but deliberate crossings — e.g. aggregating a written-file list before acting on it — remain possible.

## Where an action sits in the flow

An action's position in the flow is expressed entirely by its references, not by where it is written — every action lives in the `actions` section and the assembly order is dependency-driven:

- **pipeline start** — no `inputs` and no `waits` (e.g. scale up an instance); other steps gate on it via `waits`.
- **mid-flow** — gated by `inputs` and/or `waits`; its result envelope may be consumed by later steps (actions via `inputs`, anything via `waits`).
- **terminal** — nothing references it.

`inputs` is optional for actions: an action gated by `waits` alone, or by nothing at all, is valid with `trigger: once`.

## Operation

An action step is "which service" (`module`), "which operation" (`operation`) and "when" (`trigger`). `operation` selects one of the operations the service declares, and the set of required `parameters` depends on it (each service page lists its operations and their parameters):

| service | operations |
|---|---|
| bigquery | `jobs.query`, `jobs.load`, `jobs.extract`, `jobs.copy`, `jobs.wait`, `tables.get`, `tables.insert`, `tables.patch`, `tables.delete`, `datasets.get`, `datasets.insert`, `datasets.delete` |
| tasks | `queues.create`, `queues.update`, `queues.delete`, `queues.pause`, `queues.resume`, `queues.purge`, `queues.get`, `queues.waitForEmpty`, `tasks.run`, `tasks.delete` |
| vertexai_gemini | `batchPredictionJobs.create` |
| storage, http | none — single-operation services; `operation` must be omitted |

Naming convention for values: single-operation services have none; services wrapping an API with several resources use the API's own `resource.method` names (Cloud Tasks `queues.pause`, BigQuery `jobs.load`), so the value can be looked up in the service's API reference; other multi-operation services use plain verbs. Transport details (HTTP method, URL) stay in `parameters`. An unknown value, a missing value for a multi-operation service, or a value on a single-operation service is an assembly-time error.

## Trigger

The firing semantics, set via the module-level `trigger` field (next to `name` / `module`, not inside `parameters`):

| trigger | fires | elements delivered | typical use |
|---|---|---|---|
| `once` (default) | once, after **all** `inputs` and `waits` complete | none (inputs are pure signals) | run a job after steps finish, scale, gate |
| `perElement` | once per input element | that element | one job/notification per record, with `${field}` templates |
| `collect` | once, with all input elements gathered | the full list | summary over results: one load job for all files, one history file, one message |

`collect` materializes all elements on a single worker — meant for control records (file lists, job results), not large data. In streaming it fires per window; with zero input elements it does not fire unless `fireOnEmpty: true` (then it fires once with an empty list, `size` = 0). Templates in `collect` mode see `elements` (list of field maps) and `size`; FreeMarker list directives work, e.g. `<#list elements as e>${e.path} </#list>`.

## Output envelope

Every execution emits exactly one record with the same schema regardless of service, so downstream steps can `waits` on an action or consume its result:

| field      | type      | description                                                        |
|------------|-----------|--------------------------------------------------------------------|
| service    | STRING    | The action service (`bigquery`, `vertexai_gemini`, `storage`, `tasks`, `http`). |
| operation  | STRING    | The operation executed: the config's `operation` for multi-operation services (e.g. `jobs.load`, `queues.pause`), a service-defined value otherwise (`write` for storage, the HTTP method for http). |
| jobId      | STRING (nullable) | Id / resource name of the launched job or written object.  |
| state      | STRING (nullable) | Final (or last observed) state, e.g. `DONE`.               |
| startedAt  | TIMESTAMP | When the execution started.                                        |
| finishedAt | TIMESTAMP | When the execution finished (after waiting, if enabled).           |
| payload    | STRING (nullable) | Service-specific result JSON (e.g. BigQuery job statistics). |

## Failure handling and execution guarantees

- If the action throws (e.g. a transient job failure), the firing is first retried on the same worker per `retry` (exponential backoff `initialBackoff × 2^n`, capped at `maxBackoff`; a single attempt when `retry` is absent), then the trigger element is routed to failure handling as a `BadRecord`, honoring `failFast` / `failureSinks`. Use `retry` for transient API errors (429/503, network). Failures that re-execution cannot fix are not retried: a service's `NonRetryableException` (e.g. the http action on a 4xx response or a `poll.failWhen` match, the bigquery action on a rejected job, a permanent job error such as `invalidQuery` / `notFound` / `accessDenied`, or a wait timeout), a module-level `failWhen` match, configuration/template errors, and interruption.
- Execution is **at-least-once**: Beam may retry a bundle and re-invoke the action, and `retry` re-invokes it as well. The bigquery service submits jobs idempotently (deterministic job ids, resubmitting under a `-r<n>` suffix only when the earlier attempt failed transiently); services without a client-supplied id (vertexai_gemini) may duplicate on retry.

## Result conditions: failWhen / skipWhen

Two optional module-level conditions are evaluated **after** a successful execution, against the result envelope — so a step can act as a guard (stop the flow when a result is not what it should be) or a no-op (skip when there is nothing to do) without a separate check step:

| field | when it matches |
|---|---|
| `failWhen` | The firing fails with a non-retryable error (no `retry` re-execution; routed to failure handling per `failFast` / `failureSinks`). The error message carries the condition and the result. |
| `skipWhen` | The envelope is emitted with `state: SKIPPED` (`jobId` and `payload` kept); steps that `waits` on it proceed. Evaluated only when `failWhen` did not match. |

The condition is a [filter](../common/filter.md) — usually the SQL-like text form — over these values:

| key | value |
|---|---|
| `service`, `operation`, `jobId`, `state` | The envelope fields. |
| `payload.<path>` | A dotted path into the service's result payload (the `payload` JSON of the envelope). Each service page documents its payload structure; bigquery returns the Jobs API `Job` resource with numeric fields as numbers, e.g. `payload.statistics.query.numDmlAffectedRows`. |

Conditions are post-execution only: a pre-check (e.g. "does the table have rows") is a separate action step gated by `waits`. A missing key compares as null (`payload.x > 0` is false when `x` is absent), and a dotted path into a payload that is not a JSON object (e.g. the storage service's text payload) never matches. Names that are SQL reserved words (`count`, `table`, `order`, …) must be back-quoted: ``payload.`count` = 0``. Dotted paths work in the SQL-like and `key:` forms only, not inside a JSON `expression:`.

`state` is whatever the service reported, so a guard like `state <> 'DONE'` also fires on legitimate non-job outcomes: the http service reports `SUCCEEDED`, a service may report `SKIPPED` (nothing to do, e.g. an empty templated query — the same value `skipWhen` uses), and the bigquery table/dataset operations report `CREATED` / `EXISTS` / `DELETED` / `NOT_FOUND`; with `wait: false` a job is reported as `PENDING` / `RUNNING`. Prefer conditions on the specific outcome you want to catch.

Evaluation is fail-open: a condition that cannot be evaluated against the result (a path into a non-object payload, a literal that does not compare with the value's type, an `expression:` variable that is not a top-level key) is logged at WARN and counts as not matched — for `failWhen` this means the flow continues, so keep guards to keys the service documents.

```yaml
actions:
  - name: merge
    module: bigquery
    operation: jobs.query
    waits: [load]
    failWhen: payload.statistics.query.numDmlAffectedRows = 0    # nothing merged: stop before the swap
    parameters:
      query: MERGE `myproject.mydataset.target` t USING `myproject.mydataset.staging` s ON t.id = s.id WHEN NOT MATCHED THEN INSERT ROW
  - name: swap_view
    module: bigquery
    operation: jobs.query
    waits: [merge]
    parameters:
      query: CREATE OR REPLACE VIEW `myproject.mydataset.current` AS SELECT * FROM `myproject.mydataset.target`
```
- There is **no try/finally**: if the pipeline fails mid-way, cleanup actions gated on later steps never run (e.g. a scale-down action after a failed read). Plan recovery accordingly (e.g. `system.failure.alterConfig`).

## Common fields

| field        | optional | type            | description |
|--------------|----------|-----------------|-------------|
| name         | required | String          | Step name (referenced by other steps' `inputs` / `waits`). |
| module       | required | String          | The action service: `bigquery`, `vertexai_gemini`, `storage`, `tasks`, `http`. |
| operation    | conditionally required | String | Which operation of the service to execute — see [Operation](#operation). Required for services that declare operations (bigquery, tasks, vertexai_gemini); must be omitted for single-operation services (storage, http). |
| trigger      | optional | Enum            | `once` (default), `perElement`, `collect` — see [Trigger](#trigger). |
| inputs       | optional | Array<String\>  | Upstream step names. Pure completion signals for `once`; the elements for `perElement` / `collect`. |
| waits        | optional | Array<String\>  | Steps that must complete before this action fires. |
| strategy     | optional | [Strategy](../common/strategy.md) | Windowing strategy applied when flattening the inputs (`perElement` / `collect`). |
| retry        | optional | Retry           | Re-execute a failed firing with exponential backoff before routing it to failure handling. `maxAttempts` (default `3` when the block is present; `1` = no retry without it), `initialBackoff` (default `1s`), `maxBackoff` (default `30s`); durations as `500ms` / `10s` / `PT1M`. See [Failure handling](#failure-handling-and-execution-guarantees). |
| failWhen     | optional | [Filter](../common/filter.md) | Post-execution condition on the result envelope; when it matches, the firing fails (non-retryable). See [Result conditions](#result-conditions-failwhen--skipwhen). |
| skipWhen     | optional | [Filter](../common/filter.md) | Post-execution condition; when it matches, the envelope is emitted with `state: SKIPPED`. See [Result conditions](#result-conditions-failwhen--skipwhen). |
| fireOnEmpty  | optional | Boolean         | `collect` only: fire once with an empty element list when no input element arrives (e.g. report "0 files written", still create a marker). Requires the default strategy (global window; declaring `strategy` with it is an assembly-time error) and a bounded input — on an unbounded input the global window never closes, so the empty firing never happens. Default: `false`. |
| parameters   | required | Object          | The service's own parameters (see each service page). |
| failFast, failureSinks, tags, logs, ignore, description, args | optional | | The standard module fields. |

`parameters.trigger` and `parameters.op` / `parameters.operation` are rejected — `trigger` and `operation` are module-level fields.

## A worked example

Write files, load them all into BigQuery in one job, keep a history file, all readable top-to-bottom:

```yaml
sources:
  - name: input
    module: bigquery
    parameters:
      query: "SELECT * FROM `myproject.mydataset.mytable`"

sinks:
  - name: store                      # writes the dataset; emits written-file records
    module: storage
    inputs: [input]
    parameters:
      output: gs://my-bucket/export/data
      format: avro
actions:
  - name: load                       # one load job over every written file
    module: bigquery
    operation: jobs.load
    trigger: collect
    inputs: [store]
    parameters:
      sourceUrisField: path
      sourceFormat: AVRO
      destinationTable: myproject.mydataset.loaded
  - name: history                    # keep the written-file list as a JSONL history object
    module: storage
    trigger: collect
    inputs: [store]
    parameters:
      output: gs://my-bucket/history/latest.jsonl
```
