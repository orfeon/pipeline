# Action Modules

Action modules (`action.<service>`) execute an operation against an external service from inside the pipeline — run a BigQuery job, launch a Vertex AI batch prediction job, write a result-history file. They give a pipeline lightweight workflow steps (run a job after files are written, scale an instance before reading, notify results) without an external orchestrator.

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

## Placement

Action modules are one module kind usable in **all three config sections**. Placement never changes behavior; it tells the reader where the step sits in the flow:

- **`sources:`** — nothing upstream. A pipeline-start action (e.g. scale up an instance; other steps gate on it via `waits`). No `inputs` (only `trigger: once` applies).
- **`transforms:`** — upstream and downstream. A mid-flow action whose result envelope is consumed by later steps. Unlike data transforms, `inputs` is optional here — an action gated by `waits` alone is valid.
- **`sinks:`** — nothing downstream. A terminal action; other steps may still `waits` on it.

Rule of thumb: no upstream → sources, no downstream → sinks, both → transforms.

## Trigger

The firing semantics, set via the `trigger` parameter:

| trigger | fires | elements delivered | typical use |
|---|---|---|---|
| `once` (default) | once, after **all** `inputs` and `waits` complete | none (inputs are pure signals) | run a job after steps finish, scale, gate |
| `perElement` | once per input element | that element | one job/notification per record, with `${field}` templates |
| `collect` | once, with all input elements gathered | the full list | summary over results: one load job for all files, one history file, one message |

`collect` materializes all elements on a single worker — meant for control records (file lists, job results), not large data. In streaming it fires per window; with zero input elements it does not fire. Templates in `collect` mode see `elements` (list of field maps) and `size`; FreeMarker list directives work, e.g. `<#list elements as e>${e.path} </#list>`.

## Output envelope

Every execution emits exactly one record with the same schema regardless of service, so downstream steps can `waits` on an action or consume its result:

| field      | type      | description                                                        |
|------------|-----------|--------------------------------------------------------------------|
| service    | STRING    | The action service (`bigquery`, `vertexai_gemini`, `storage`, `tasks`, `http`). |
| op         | STRING    | The operation executed (e.g. `query`, `load`, `write`).            |
| jobId      | STRING (nullable) | Id / resource name of the launched job or written object.  |
| state      | STRING (nullable) | Final (or last observed) state, e.g. `DONE`.               |
| startedAt  | TIMESTAMP | When the execution started.                                        |
| finishedAt | TIMESTAMP | When the execution finished (after waiting, if enabled).           |
| payload    | STRING (nullable) | Service-specific result JSON (e.g. BigQuery job statistics). |

## Failure handling and execution guarantees

- If the action throws (including job failure and wait timeout), the trigger element is routed to failure handling as a `BadRecord`, honoring `failFast` / `failureSinks`.
- Execution is **at-least-once**: Beam may retry a bundle and re-invoke the action. The bigquery service submits jobs idempotently (deterministic job ids); services without a client-supplied id (vertexai_gemini) may duplicate on retry.
- There is **no try/finally**: if the pipeline fails mid-way, cleanup actions gated on later steps never run (e.g. a scale-down action after a failed read). Plan recovery accordingly (e.g. `system.failure.alterConfig`).

## Common parameters

Action modules take the standard module fields (`name`, `module`, `inputs`, `waits`, `failFast`, `failureSinks`, …). The `parameters` object is flat: `trigger` plus the selected service's own parameters, side by side.

```yaml
sinks:
  - name: load_to_bq
    module: action.bigquery
    waits:
      - store
    parameters:
      trigger: once      # optional (default)
      op: load
      sourceUris:
        - gs://my-bucket/export/*.avro
      destinationTable: myproject.mydataset.loaded
```

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
  - name: load                       # one load job over every written file
    module: action.bigquery
    inputs: [store]
    parameters:
      trigger: collect
      op: load
      sourceUrisField: path
      sourceFormat: AVRO
      destinationTable: myproject.mydataset.loaded
  - name: history                    # keep the written-file list as a JSONL history object
    module: action.storage
    inputs: [store]
    parameters:
      trigger: collect
      output: gs://my-bucket/history/latest.jsonl
```
