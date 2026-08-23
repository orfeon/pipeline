---
type: Action Module
title: BigQuery Action Module
description: Runs a BigQuery job (query or load) via the Jobs API from inside the pipeline and waits for its completion. Submission is idempotent via deterministic job ids (a retried bundle adopts the running job instead of duplicating it). Supports trigger once (run after steps complete), perElement (one job per record with ${field} templates) and collect (one job over all records, e.g. sourceUrisField gathering every written file into a single load job).
tags: [action, bigquery, job, query, load, trigger, batch, workflow]
timestamp: 2026-08-19T00:00:00Z
---

# BigQuery Action Module

Action module (`action.bigquery`) that runs a BigQuery job — a query (SELECT/DML/DDL) or a load job — via the BigQuery Jobs API, and by default waits for its completion. Placeable in sources/transforms/sinks; see [action modules](README.md) for placement, trigger semantics and the output envelope.

Typical uses: run a load job over files a storage sink wrote, run a summary/merge query after other steps complete, run one parameterized job per input record.

## Idempotent job submission

The job id is derived deterministically from the pipeline job name, the step name and the effective parameters (after template expansion) — or set explicitly via `jobId`. A retried Beam bundle therefore gets HTTP 409 ALREADY_EXISTS and adopts the already-running job instead of starting a duplicate. Note this also means two `perElement` firings with identical effective parameters collapse into one job.

## Templates

- `trigger: perElement` — `${field}` expressions in `query`, `sourceUris`, `destinationTable` and `jobId` are expanded with the element's values (primitive representation, e.g. timestamps as epoch micros).
- `trigger: collect` — the same parameters can use `elements` (list of field maps) and `size`, including FreeMarker list directives; `sourceUrisField` gathers one field's value from every element into `sourceUris`.

## Parameters

| parameter         | optional | type           | description                                                                                                             |
|-------------------|----------|----------------|-------------------------------------------------------------------------------------------------------------------------|
| trigger           | optional | Enum           | `once` (default), `perElement`, `collect`. See [action modules](README.md#trigger).                                     |
| op                | required | Enum           | Job type: `query` or `load`.                                                                                             |
| projectId         | optional | String         | GCP project ID to run the job in. Defaults to the pipeline's project.                                                    |
| query             | conditionally required | String | SQL to execute (SELECT/DML/DDL). Required when `op` is `query`. A template that resolves to an empty string (e.g. a cdc `SCHEMA` record without a generated `statement`) submits no job: the firing is reported with `state: SKIPPED` and a null `jobId`. |
| useLegacySql      | optional | Boolean        | Whether the query uses legacy SQL. Default: `false` (standard SQL). (`op: query` only)                                   |
| priority          | optional | Enum           | Query priority: `INTERACTIVE` or `BATCH`. Default: `INTERACTIVE`. (`op: query` only)                                     |
| sourceUris        | conditionally required | Array<String\> | GCS URIs of files to load. Required when `op` is `load` unless `sourceUrisField` is set.                  |
| sourceUrisField   | optional | String         | With `trigger: collect` and `op: load`: gathers this field's value from every collected element into `sourceUris` (e.g. `path` from storage sink results — one load job for all written files). |
| sourceFormat      | optional | String         | Source format for `op: load`: `AVRO`, `PARQUET`, `CSV`, `NEWLINE_DELIMITED_JSON`, `ORC`, … Default: BigQuery's default (`CSV`). |
| destinationTable  | conditionally required | String | Destination table (e.g. `project.dataset.table`). Required when `op` is `load`; optional for `op: query` (writes query results to the table). |
| writeDisposition  | optional | Enum           | `WRITE_TRUNCATE`, `WRITE_APPEND`, `WRITE_EMPTY`.                                                                         |
| createDisposition | optional | Enum           | `CREATE_IF_NEEDED`, `CREATE_NEVER`.                                                                                      |
| location          | optional | String         | Job location (e.g. `US`, `asia-northeast1`). Usually inferred by BigQuery.                                               |
| jobId             | optional | String         | Explicit job id (template-able). Default: deterministic generated id.                                                    |
| jobIdPrefix       | optional | String         | Prefix of the generated job id. Default: `mp-action`.                                                                    |
| wait              | optional | Boolean        | Whether to wait until the job reaches `DONE` (polled with exponential backoff). A failed job raises an error. Default: `true`. |
| timeoutSeconds    | optional | Integer        | Max seconds to wait for job completion; exceeding it raises an error. Default: `86400` (24h).                            |
| quotaUser         | optional | String         | Arbitrary string used as the quota user on the Jobs API request.                                                         |
| labels            | optional | Map<String,String\> | Labels attached to the launched job.                                                                                |

## Output

One envelope record per execution (see [action modules](README.md#output-envelope)); `payload` carries the job statistics JSON.

## Examples

### Example 1: One load job over every file a storage sink wrote

```yaml
sinks:
  - name: store
    module: storage
    inputs: [input]
    parameters:
      output: gs://my-bucket/export/data
      format: avro
  - name: load
    module: action.bigquery
    inputs: [store]                # storage sink emits {sink, path, timestamp} records
    parameters:
      trigger: collect
      op: load
      sourceUrisField: path
      sourceFormat: AVRO
      destinationTable: myproject.mydataset.loaded
      writeDisposition: WRITE_TRUNCATE
```

### Example 2: Summary query after another step

```yaml
sinks:
  - name: summarize
    module: action.bigquery
    waits: [some_previous_sink]
    parameters:
      op: query
      query: "SELECT category, COUNT(*) AS cnt FROM `myproject.mydataset.events` GROUP BY category"
      destinationTable: myproject.mydataset.event_summary
      writeDisposition: WRITE_TRUNCATE
```

### Example 3: One parameterized load per input record

```yaml
sinks:
  - name: load_partitions
    module: action.bigquery
    inputs: [partitions]           # records with field: date_str
    parameters:
      trigger: perElement
      op: load
      sourceUris:
        - gs://my-bucket/export/${date_str}/*.avro
      sourceFormat: AVRO
      destinationTable: myproject.mydataset.events$${date_str}
      writeDisposition: WRITE_TRUNCATE
```
