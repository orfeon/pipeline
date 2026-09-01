---
type: Action Module
title: BigQuery Action Module
description: Runs a BigQuery job (query, load, extract or copy/snapshot/clone) via the Jobs API from inside the pipeline and waits for its completion; also waits for jobs launched elsewhere (jobs.wait) and manages tables and datasets (tables.get as a guard, tables.insert / tables.patch for tables and views, tables.delete, datasets.get / insert / delete). Submission is idempotent via deterministic job ids (a retried bundle adopts the running job instead of duplicating it; on retry a transiently failed job is resubmitted, a permanent error such as invalidQuery is not retried). Common job settings (jobTimeoutMs, reservation, dryRun, labels, cancelOnTimeout) plus a raw configuration escape hatch for any JobConfiguration field. The result payload is the Job resource with typed statistics, usable in module-level failWhen / skipWhen conditions. Supports trigger once (run after steps complete), perElement (one job per record with ${field} templates) and collect (one job over all records, e.g. sourceUrisField gathering every written file into a single load job).
tags: [action, bigquery, job, query, load, extract, copy, snapshot, wait, table, dataset, view, guard, trigger, batch, workflow]
timestamp: 2026-08-27T00:00:00Z
---

# BigQuery Action Module

Action module (`actions` section, `module: bigquery`) that runs a BigQuery job — a query (SELECT/DML/DDL), a load, an extract (table/model → GCS) or a copy (copy / snapshot / clone / restore) job — via the BigQuery Jobs API, and by default waits for its completion. See [action modules](README.md) for the `actions` section, trigger semantics and the output envelope.

Beyond jobs it can wait for jobs submitted by something else (`jobs.wait`), read a table's metadata to guard the flow (`tables.get` + `failWhen` / `skipWhen`), create / update / delete tables and views (`tables.insert`, `tables.patch`, `tables.delete`) and datasets (`datasets.get`, `datasets.insert`, `datasets.delete`).

Typical uses: run a load job over files a storage sink wrote, run a summary/merge query after other steps complete, snapshot a table before a destructive load, export a table to GCS, run one parameterized job per input record, rotate generation tables.

## Idempotent job submission and failure handling

The job id is derived deterministically from the pipeline job name, the step name and the effective parameters (after template expansion) — or set explicitly via `jobId`. A retried Beam bundle or a module-level `retry` attempt therefore gets HTTP 409 ALREADY_EXISTS and adopts the already-running (or already succeeded) job instead of starting a duplicate. Note this also means two `perElement` firings with identical effective parameters collapse into one job.

Failures are classified by the BigQuery error reason so that `retry` is spent only where it can help:

| situation | behaviour |
|---|---|
| Job finished with a transient reason (`rateLimitExceeded`, `quotaExceeded`, `backendError`, `internalError`, `jobBackendError`, `jobInternalError`, `jobRateLimitExceeded`) | Retryable. A retry finds the failed job under the deterministic id and resubmits it as `<jobId>-r1`, `-r2`, … (at most 10). |
| Job finished with any other reason (`invalidQuery`, `invalid`, `notFound`, `accessDenied`, `duplicate`, `resourcesExceeded`, `stopped`, …) | Non-retryable: routed to failure handling immediately. |
| Request rejected with HTTP 4xx (other than 408/429) | Non-retryable — unless the error body carries a transient reason (`rateLimitExceeded` / `quotaExceeded` arrive as HTTP 403), which stays retryable. |
| Submission failed with HTTP 5xx / 429 / network error | Retryable. |
| Job not `DONE` within `timeoutSeconds` | Non-retryable; with `cancelOnTimeout: true` (default) the job is cancelled first. Prefer `jobTimeoutMs` to have BigQuery itself stop a runaway job. (Earlier releases retried the timeout and re-adopted the still-running job; set `cancelOnTimeout: false` and a `retry` block to get close to that behaviour.) |

The existing-job check runs on every submission, also with `wait: false`: a `wait: false` firing whose deterministic id already belongs to a permanently failed job fails immediately, and one whose id belongs to a transiently failed job resubmits it. The deterministic id contains the pipeline job name, so two runs that reuse the same `jobName` (an explicit `--jobName`, `options.jobName` in a fixed serve-mode config) with identical effective parameters adopt each other's job instead of running again — let the runner generate the job name, or make a parameter (e.g. a `labels` value or `jobIdPrefix`) run-specific.

## Result payload and conditions

`payload` carries the [Jobs API `Job` resource](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job) as returned after completion (`jobReference`, `configuration`, `status`, `statistics`) — with the API's declared types, i.e. int64 fields such as `statistics.totalBytesProcessed` or `statistics.query.numDmlAffectedRows` as numbers, not strings. `statistics.query.queryPlan` and `statistics.query.timeline` (per-stage execution details, potentially hundreds of KB) are omitted; note that `configuration` echoes the submitted job, including the SQL text and every source URI. The module-level `failWhen` / `skipWhen` conditions (see [action modules](README.md#result-conditions-failwhen--skipwhen)) reference it as `payload.<path>`, e.g.:

```yaml
failWhen: payload.statistics.query.numDmlAffectedRows = 0
skipWhen: payload.statistics.load.outputRows = 0
failWhen: payload.statistics.totalBytesProcessed > 1073741824   # with dryRun: true — cost guard
```

With `wait: false` the payload is the job as submitted (`status.state` `PENDING` / `RUNNING`, no completion statistics); with `dryRun: true` no job is allocated, so `jobId` is null. A job whose `status.errors` lists non-fatal problems (e.g. rows skipped within `maxBadRecords`) counts as succeeded — only `status.errorResult` fails it.

Note for users of the earlier release: the payload used to be the job *statistics* object only (`totalBytesProcessed`, `query.numDmlAffectedRows` as strings at its top level); those paths now live under `statistics.*` with numeric types.

## Templates

- `trigger: perElement` — `${field}` expressions in the parameters marked "template-able" below (`query`, `queryParameters` values, `sourceUris`, `destinationTable`, `defaultDataset`, `jobId`, `reservation`, label values, `sourceTable` / `sourceModel` / `destinationUris`, `sourceTables` / `destinationExpirationTime`, `table` / `dataset` / `view` / `description` / `expirationTime`, `hivePartitioningOptions.sourceUriPrefix`) are expanded with the element's values (primitive representation, e.g. timestamps as epoch micros). The raw `configuration` / `resource` JSON is not templated.
- `trigger: collect` — the same parameters can use `elements` (list of field maps) and `size`, including FreeMarker list directives; `sourceUrisField` gathers one field's value from every element into `sourceUris`.

## Operations

| operation    | effect |
|--------------|--------|
| `jobs.query` | Run a query job (SELECT / DML / DDL / script) — `query` required; the [query parameters](#jobsquery-parameters) and, when `destinationTable` is set, the [destination options](#destination-options) apply. |
| `jobs.load`  | Run a load job — `sourceUris` or `sourceUrisField`, and `destinationTable` required; the [load parameters](#jobsload-parameters) and [destination options](#destination-options) apply. |
| `jobs.extract` | Export a table (or a BigQuery ML model) to GCS — `sourceTable` or `sourceModel`, and `destinationUris` required; see [extract parameters](#jobsextract-parameters). |
| `jobs.copy`  | Copy tables, or take a SNAPSHOT / CLONE / RESTORE — `sourceTables` or `sourceTablesField`, and `destinationTable` required; see [copy parameters](#jobscopy-parameters). |
| `jobs.wait`  | Wait for a job submitted elsewhere (another action with `wait: false`, an external scheduler, the `bq` CLI) — `jobId` or `jobIdField` required; see [wait parameters](#jobswait-parameters). |
| `tables.get` | Read a table's metadata (`numRows`, `lastModifiedTime`, `schema`, …) into the payload — `table` required. Combined with `failWhen` / `skipWhen` it guards the flow. See [table parameters](#tablesget--tablesdelete-parameters). |
| `tables.insert` | Create a table or a view — `table` and one of `schema` / `view` / `resource` required. Idempotent with `ifNotExists` (default `true`: an existing table is reported as `state: EXISTS`). |
| `tables.patch` | Update table metadata (`schema`, `description`, `labels`, `expirationTime`, `view`, `requirePartitionFilter`, `resource`) — `table` required. Only the given fields change. |
| `tables.delete` | Delete a table (idempotent with `ignoreNotFound`, default `true`) — `table` required. |
| `datasets.get` | Read a dataset's metadata into the payload — `dataset` required. |
| `datasets.insert` | Create a dataset (`location`, `description`, `labels`, `defaultTableExpirationMs`, `resource`) — `dataset` required; `ifNotExists` (default `true`). |
| `datasets.delete` | Delete a dataset (`deleteContents`, `ignoreNotFound` default `true`) — `dataset` required. |

The `jobs.<type>` operations correspond to `jobs.insert` with the matching `configuration.<jobType>` of the Jobs API (the same as `bq query` / `bq load` / `bq extract` / `bq cp`); `jobs.wait` is `jobs.get` polling; `tables.*` / `datasets.*` are the Tables / Datasets API methods.

## Parameters

| parameter         | optional | type           | description                                                                                                             |
|-------------------|----------|----------------|-------------------------------------------------------------------------------------------------------------------------|
| projectId         | optional | String         | GCP project ID to run the job in. Defaults to the pipeline's project.                                                    |
| destinationTable  | conditionally required | String | Destination table (e.g. `project.dataset.table`, partition decorators like `table$20260827` allowed). Required for `jobs.load`; optional for `jobs.query` (writes query results to the table). |
| writeDisposition  | optional | Enum           | `WRITE_TRUNCATE`, `WRITE_APPEND`, `WRITE_EMPTY`.                                                                         |
| createDisposition | optional | Enum           | `CREATE_IF_NEEDED`, `CREATE_NEVER`.                                                                                      |
| location          | optional | String         | Job location (e.g. `US`, `asia-northeast1`). Usually inferred by BigQuery from the referenced dataset, and completion polling follows the location the job was actually placed in. Set it explicitly for `jobs.wait`, and when a retried firing must adopt an already-submitted job in a regional location (the 409-adoption lookup cannot infer it). |
| jobId             | optional | String         | Explicit job id (template-able). Must be unique per run — an existing job under that id is adopted (a succeeded job from an earlier run is returned as the result without running again), so include a run-specific part (e.g. a date or `${args.version}`). Default: a deterministic generated id that includes the pipeline job name. |
| jobIdPrefix       | optional | String         | Prefix of the generated job id. Default: `mp-action`.                                                                    |
| wait              | optional | Boolean        | Whether to wait until the job reaches `DONE` (polled with exponential backoff). A failed job raises an error. Default: `true`. |
| timeoutSeconds    | optional | Integer        | Max seconds to wait for job completion; exceeding it raises a non-retryable error. Default: `86400` (24h).               |
| cancelOnTimeout   | optional | Boolean        | Cancel the job when `timeoutSeconds` is exceeded, so it does not keep running (and billing) after the action failed. Default: `true` for jobs this action submits, `false` for `jobs.wait` (a job launched elsewhere is not cancelled unless asked). |
| jobTimeoutMs      | optional | Integer        | `JobConfiguration.jobTimeoutMs`: BigQuery itself aborts the job after this many milliseconds — effective even with `wait: false` or after the worker died. |
| reservation       | optional | String         | `JobConfiguration.reservation`: run the job in this reservation (`projects/{p}/locations/{l}/reservations/{r}`). Template-able. |
| dryRun            | optional | Boolean        | `JobConfiguration.dryRun`: validate the job and estimate `statistics.totalBytesProcessed` without running it; no waiting. Default: `false`. |
| quotaUser         | optional | String         | Arbitrary string used as the quota user on the Jobs API request.                                                         |
| labels            | optional | Map<String,String\> | Labels attached to the launched job (values are template-able).                                                    |
| configuration     | optional | Object         | Raw [`JobConfiguration`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfiguration) JSON — an escape hatch for any API field this page does not list (e.g. `query.maximumBytesBilled`, `query.queryParameters`, `load.timePartitioning`, `load.csvOptions`). It is the base; the explicit parameters above are merged over it (they win on conflict). |

### jobs.query parameters

| parameter         | optional | type           | description |
|-------------------|----------|----------------|-------------|
| query             | required | String         | SQL to execute (SELECT/DML/DDL/script). A template that resolves to an empty string (e.g. a cdc `SCHEMA` record without a generated `statement`) submits no job: the firing is reported with `state: SKIPPED` and a null `jobId`. |
| queryParameters   | optional | Object or Array | Query parameters — the recommended way to inject per-element values (no quoting / injection issues, unlike `${field}` inside `query`). Object form is `name: value` for `@name` references: a primitive infers its type (`true` → BOOL, `1` → INT64, `0.5` → FLOAT64, text → STRING), an array of primitives is an ARRAY of the inferred element type, and `{type, value}` sets an explicit type (`{type: DATE, value: "2026-08-27"}`, `{type: TIMESTAMP, value: "…"}`, `{type: STRING, value: [a, b]}` for a typed array). Array form takes raw API [`QueryParameter`](https://cloud.google.com/bigquery/docs/reference/rest/v2/QueryParameter) objects (named, or positional for `?` placeholders). String values are template-able — a templated value is a string, so a non-STRING per-element parameter needs the explicit form: `minId: {type: INT64, value: "${id}"}`. |
| useLegacySql      | optional | Boolean        | Whether the query uses legacy SQL. Default: `false` (standard SQL). |
| priority          | optional | Enum           | Query priority: `INTERACTIVE` or `BATCH`. Default: `INTERACTIVE`. |
| defaultDataset    | optional | String         | Dataset (`dataset` or `project.dataset`) that resolves unqualified table names in the query. Template-able. |
| maximumBytesBilled | optional | Integer       | Cost guard: the job fails (`bytesBilledLimitExceeded`, non-retryable) when it would bill more than this many bytes. |
| useQueryCache     | optional | Boolean        | Whether to look for the result in the query cache. Default: BigQuery's default (`true`). |
| connectionProperties | optional | Map<String,String\> | [Connection properties](https://cloud.google.com/bigquery/docs/reference/rest/v2/ConnectionProperty) such as `time_zone`, `session_id`, `query_label`. |
| resultRows        | optional | Integer        | After completion, fetch up to this many result rows (`jobs.getQueryResults`) into the payload: `resultRows` (list of column→value maps), `firstRow` (the first row) and `totalRows`. Meant for small control results — a count, a max timestamp — to drive `failWhen` / `skipWhen` (`payload.firstRow.cnt = 0`), not for moving data. (The keys avoid the SQL reserved words `row` / `rows`.) Requires `wait: true` and no `dryRun`; at most 1000. Values: INTEGER/FLOAT/BOOLEAN as numbers/booleans, NUMERIC/BIGNUMERIC as decimals, DATE as epoch days, TIME as micros of day, TIMESTAMP as epoch micros, DATETIME/GEOGRAPHY/INTERVAL/RANGE/JSON as text, RECORD as a nested map. |

### jobs.load parameters

| parameter         | optional | type           | description |
|-------------------|----------|----------------|-------------|
| sourceUris        | conditionally required | Array<String\> | GCS URIs of files to load (`*` wildcard allowed). Required unless `sourceUrisField` is set. |
| sourceUrisField   | optional | String         | With `trigger: collect`: gathers this field's value from every collected element into `sourceUris` (e.g. `path` from storage sink results — one load job for all written files). |
| sourceFormat      | optional | String         | `AVRO`, `PARQUET`, `CSV`, `NEWLINE_DELIMITED_JSON`, `ORC`, `DATASTORE_BACKUP`, … Default: BigQuery's default (`CSV`). |
| schema            | optional | [Schema](../common/schema.md) | Destination table schema in the common `fields` notation (converted to a BigQuery table schema). Exclusive with `autodetect`. When neither is set the table must already exist (or the format must carry a schema, as AVRO / PARQUET / ORC do). |
| autodetect        | optional | Boolean        | Infer the schema from CSV / JSON files. Exclusive with `schema`. |
| ignoreUnknownValues | optional | Boolean      | Accept rows with extra columns not in the schema. |
| maxBadRecords     | optional | Integer        | Number of bad records tolerated before the job fails. Default: `0`. |
| csvOptions        | optional | Object         | CSV settings: `skipLeadingRows`, `fieldDelimiter`, `quote`, `allowQuotedNewlines`, `allowJaggedRows`, `encoding` (`UTF-8` / `ISO-8859-1`), `nullMarker`, `preserveAsciiControlCharacters`. |
| parquetOptions    | optional | Object         | `enumAsString`, `enableListInference`. |
| useAvroLogicalTypes | optional | Boolean      | Interpret Avro logical types (e.g. `timestamp-micros`) as their BigQuery types instead of raw values. |
| jsonExtension     | optional | String         | `GEOJSON` for newline-delimited GeoJSON. |
| hivePartitioningOptions | optional | Object   | `mode` (`AUTO` / `STRINGS` / `CUSTOM`), `sourceUriPrefix` (template-able), `requirePartitionFilter` — load `gs://…/dt=2026-08-27/…` layouts as partition columns. |
| decimalTargetTypes | optional | Array<String\> | Preference order for decimal columns: `NUMERIC`, `BIGNUMERIC`, `STRING`. |

### jobs.extract parameters

| parameter         | optional | type           | description |
|-------------------|----------|----------------|-------------|
| sourceTable       | conditionally required | String | Table to export (`project.dataset.table`). Exactly one of `sourceTable` / `sourceModel`. Template-able. |
| sourceModel       | conditionally required | String | BigQuery ML model to export (`project.dataset.model`). Template-able. |
| destinationUris   | required | Array<String\> | GCS URIs to write to; use a `*` wildcard (`gs://bucket/export/part-*.avro`) for exports larger than 1 GB. Template-able. |
| destinationFormat | optional | String         | `CSV` (default), `NEWLINE_DELIMITED_JSON`, `AVRO`, `PARQUET`; for models `ML_TF_SAVED_MODEL`, `ML_XGBOOST_BOOSTER`. |
| compression       | optional | String         | `GZIP`, `DEFLATE`, `SNAPPY`, `ZSTD` (availability depends on the format). |
| fieldDelimiter    | optional | String         | CSV delimiter. |
| printHeader       | optional | Boolean        | CSV header row. Default: BigQuery's default (`true`). |
| useAvroLogicalTypes | optional | Boolean      | Write Avro logical types (e.g. `timestamp-micros`) instead of raw values. |

The payload's `statistics.extract.destinationUriFileCounts` lists the number of files written per destination URI.

### jobs.copy parameters

| parameter         | optional | type           | description |
|-------------------|----------|----------------|-------------|
| sourceTables      | conditionally required | Array<String\> | Source tables (`project.dataset.table`). Several sources are appended into the destination (`COPY` only). Template-able. |
| sourceTablesField | optional | String         | With `trigger: collect`: gathers this field's value from every collected element into `sourceTables`. |
| operationType     | optional | Enum           | `COPY` (default), `SNAPSHOT` (create a table snapshot), `CLONE` (create a writable clone), `RESTORE` (restore a snapshot into a table). |
| destinationExpirationTime | optional | String | Expiration of the destination — for snapshots typically — as a timestamp (`2030-01-01T00:00:00Z`, `2030-01-01 00:00:00`, `2030-01-01`) or a duration relative to the execution time (`7d`, `PT168H`). Template-able. |
| writeDisposition / createDisposition | optional | Enum | As for load. `WRITE_TRUNCATE` replaces the destination atomically. |

### jobs.wait parameters

| parameter         | optional | type           | description |
|-------------------|----------|----------------|-------------|
| jobId             | conditionally required | String | The job to wait for (template-able, e.g. `${jobId!}` from an upstream envelope with `trigger: perElement` — the `!` default makes a `SKIPPED` upstream envelope with a null `jobId` render empty, which this step reports as `SKIPPED` instead of failing on the missing variable). |
| jobIdField        | optional | String         | With `trigger: collect`: gathers this field from every collected element and waits for all of them — e.g. fan-out many `jobs.query` steps with `wait: false, priority: BATCH`, then fan-in with one wait. With two or more ids the envelope's `jobId` is the comma-joined list and `payload.jobs` the list of `Job` resources (so per-job paths such as `payload.statistics.load.outputRows` do not apply to the collected result); a single id yields the plain `Job` payload. |
| location, timeoutSeconds, cancelOnTimeout | optional | | As for job submission, except that `cancelOnTimeout` defaults to `false` here. |

All gathered jobs are polled in one loop with a shared backoff and a single `timeoutSeconds` window (on timeout the still-pending jobs are cancelled when `cancelOnTimeout` is set); a transient error while polling one job is retried within that window, an unknown job id (404) fails the firing. A job that finished with an error fails the firing as non-retryable regardless of the reason (this action cannot resubmit a job it did not create). An empty / missing id (`state: SKIPPED`) is not an error.

### tables.* parameters

| parameter         | optional | type           | description |
|-------------------|----------|----------------|-------------|
| table             | required | String         | `project.dataset.table` (or `dataset.table` in the default project). Template-able. |
| ignoreNotFound    | optional | Boolean        | `tables.get` / `tables.delete`: when the table does not exist, emit `state: NOT_FOUND` (null payload) instead of failing. Default: `false` for `tables.get`, `true` for `tables.delete` (so a retried firing after a successful delete is a no-op). |
| ifNotExists       | optional | Boolean        | `tables.insert`: when the table already exists, emit `state: EXISTS` with the existing table as payload instead of failing. Default: `true`. |
| schema            | optional | [Schema](../common/schema.md) | `tables.insert` / `tables.patch`: the table schema in the common `fields` notation. |
| view              | optional | String         | `tables.insert` / `tables.patch`: create the table as a view with this SQL (standard SQL unless `useLegacySql: true`). Patching `view` on an existing view swaps its definition atomically — the `CREATE OR REPLACE VIEW` equivalent without SQL string assembly. Template-able. |
| description       | optional | String         | Table description. Template-able. |
| labels            | optional | Map<String,String\> | Table labels. |
| expirationTime    | optional | String         | Table expiration as a timestamp (`2030-01-01T00:00:00Z`, `2030-01-01`) or a duration relative to the execution time (`1d`, `PT36H`). Template-able. |
| requirePartitionFilter | optional | Boolean   | Require a partition filter in queries. |
| timePartitioning / rangePartitioning / clustering | optional | | `tables.insert`: the [destination options](#destination-options) (table definition). |
| resource          | optional | Object         | Raw [`Table` resource](https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#Table) JSON for fields not listed (`friendlyName`, `materializedView`, `externalDataConfiguration`, `encryptionConfiguration`, …). Base of the merge; explicit parameters win. |

`tables.get` returns the [`Table` resource](https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#Table) as payload (`numRows`, `numBytes`, `lastModifiedTime` as epoch milliseconds, `creationTime`, `expirationTime`, `type`, `schema`, `timePartitioning`, …) with `jobId` = the full table id and `state: DONE`; `tables.insert` / `tables.patch` return the created / updated resource (`state: CREATED` / `EXISTS` / `DONE`); `tables.delete` emits `state: DELETED` or `NOT_FOUND`.

### datasets.* parameters

| parameter         | optional | type           | description |
|-------------------|----------|----------------|-------------|
| dataset           | required | String         | `project.dataset` (or `dataset` in the default project). Template-able. |
| ignoreNotFound    | optional | Boolean        | `datasets.get` / `datasets.delete`: `state: NOT_FOUND` instead of failing. Default: `false` for get, `true` for delete. |
| ifNotExists       | optional | Boolean        | `datasets.insert`: `state: EXISTS` instead of failing. Default: `true`. |
| location          | optional | String         | `datasets.insert`: dataset location (`US`, `asia-northeast1`, …). |
| description, labels | optional | | `datasets.insert`: dataset description / labels. |
| defaultTableExpirationMs | optional | Integer | `datasets.insert`: default expiration of tables created in the dataset. |
| deleteContents    | optional | Boolean        | `datasets.delete`: also delete the tables in it. Default: `false` (a non-empty dataset fails). |
| resource          | optional | Object         | Raw [`Dataset` resource](https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#Dataset) JSON for fields not listed (`defaultPartitionExpirationMs`, `access`, `defaultEncryptionConfiguration`, …). |

`datasets.get` / `datasets.insert` return the `Dataset` resource as payload with `jobId` = `project.dataset`.

### Destination options

Apply to `jobs.load` and to `jobs.query` with `destinationTable`; they define the table when it is created by the job (an existing table keeps its definition):

| parameter         | optional | type           | description |
|-------------------|----------|----------------|-------------|
| timePartitioning  | optional | Object         | `type` (`DAY` / `HOUR` / `MONTH` / `YEAR`, required), `field` (partition column; ingestion time when absent), `expirationMs`, `requirePartitionFilter`. Exclusive with `rangePartitioning`. |
| rangePartitioning | optional | Object         | `field` (INT64 column), `start`, `end`, `interval`. |
| clustering        | optional | Array<String\> | Clustering columns (up to 4). |
| schemaUpdateOptions | optional | Array<String\> | `ALLOW_FIELD_ADDITION`, `ALLOW_FIELD_RELAXATION` — schema changes allowed on `WRITE_APPEND` / partition overwrite. |

Anything else of the Jobs API (`query.scriptOptions`, `load.referenceFileSchemaUri`, …) goes through `configuration`.

## Output

One envelope record per execution (see [action modules](README.md#output-envelope)); `payload` carries the `Job` resource JSON (see [Result payload](#result-payload-and-conditions)).

## Examples

### Example 0: Blue-green load with a view swap by API

```yaml
actions:
  - name: load_new                # load into a new generation table
    module: bigquery
    operation: jobs.load
    parameters:
      sourceUris: [gs://my-bucket/export/*.avro]
      sourceFormat: AVRO
      destinationTable: myproject.mydataset.events_${args.version}
      writeDisposition: WRITE_TRUNCATE
  - name: swap_view               # repoint the stable view; only after the load succeeded
    module: bigquery
    operation: tables.patch
    waits: [load_new]
    parameters:
      table: myproject.mydataset.events
      view: SELECT * FROM `myproject.mydataset.events_${args.version}`
```

### Example 1: One load job over every file a storage sink wrote

```yaml
sinks:
  - name: store
    module: storage
    inputs: [input]
    parameters:
      output: gs://my-bucket/export/data
      format: avro
actions:
  - name: load
    module: bigquery
    operation: jobs.load
    trigger: collect
    inputs: [store]                # storage sink emits {sink, path, timestamp} records
    parameters:
      sourceUrisField: path
      sourceFormat: AVRO
      destinationTable: myproject.mydataset.loaded
      writeDisposition: WRITE_TRUNCATE
```

### Example 2: Summary query after another step

```yaml
actions:
  - name: summarize
    module: bigquery
    operation: jobs.query
    waits: [some_previous_sink]
    parameters:
      query: "SELECT category, COUNT(*) AS cnt FROM `myproject.mydataset.events` GROUP BY category"
      destinationTable: myproject.mydataset.event_summary
      writeDisposition: WRITE_TRUNCATE
```

### Example 3: One parameterized load per input record

```yaml
actions:
  - name: load_partitions
    module: bigquery
    operation: jobs.load
    trigger: perElement
    inputs: [partitions]           # records with field: date_str
    parameters:
      sourceUris:
        - gs://my-bucket/export/${date_str}/*.avro
      sourceFormat: AVRO
      destinationTable: myproject.mydataset.events$${date_str}
      writeDisposition: WRITE_TRUNCATE
```

### Example 4: Guarded merge with a cost estimate and the raw configuration escape hatch

```yaml
actions:
  - name: estimate
    module: bigquery
    operation: jobs.query
    failWhen: payload.statistics.totalBytesProcessed > 10737418240   # refuse to run a > 10 GiB query
    parameters:
      dryRun: true
      query: "SELECT * FROM `myproject.mydataset.events` WHERE dt = @dt"
      configuration:
        query:
          queryParameters:
            - name: dt
              parameterType: { type: DATE }
              parameterValue: { value: "2026-08-27" }
  - name: merge
    module: bigquery
    operation: jobs.query
    waits: [estimate]
    retry: { maxAttempts: 3 }
    skipWhen: payload.statistics.query.numDmlAffectedRows = 0
    parameters:
      jobTimeoutMs: 1800000
      query: "MERGE `myproject.mydataset.target` t USING `myproject.mydataset.staging` s ON t.id = s.id WHEN NOT MATCHED THEN INSERT ROW"
```

### Example 5: Partitioned CSV load with an explicit schema, then a parameterized DML per record

```yaml
actions:
  - name: load_csv
    module: bigquery
    operation: jobs.load
    parameters:
      sourceUris: [gs://my-bucket/events/dt=2026-08-27/*.csv]
      sourceFormat: CSV
      destinationTable: myproject.mydataset.events
      writeDisposition: WRITE_APPEND
      schema:
        fields:
          - { name: id, type: int64, mode: required }
          - { name: dt, type: date }
          - { name: payload, type: json }
      csvOptions: { skipLeadingRows: 1, allowQuotedNewlines: true }
      maxBadRecords: 10
      timePartitioning: { type: DAY, field: dt }
      clustering: [id]
      hivePartitioningOptions: { mode: AUTO, sourceUriPrefix: gs://my-bucket/events/ }
  - name: expire_partitions
    module: bigquery
    operation: jobs.query
    trigger: perElement
    inputs: [expired_dates]          # records with field: date_str
    waits: [load_csv]
    parameters:
      query: DELETE FROM `myproject.mydataset.events` WHERE dt < @before
      queryParameters:
        before: { type: DATE, value: "${date_str}" }
```

### Example 6: Snapshot before a destructive load, then export the result

```yaml
actions:
  - name: snapshot
    module: bigquery
    operation: jobs.copy
    parameters:
      sourceTables: [myproject.mydataset.events]
      destinationTable: myproject.backup.events_${args.version}
      operationType: SNAPSHOT
      destinationExpirationTime: 7d
  - name: reload
    module: bigquery
    operation: jobs.load
    waits: [snapshot]                # only overwrite once the snapshot exists
    parameters:
      sourceUris: [gs://my-bucket/import/*.parquet]
      sourceFormat: PARQUET
      destinationTable: myproject.mydataset.events
      writeDisposition: WRITE_TRUNCATE
  - name: export
    module: bigquery
    operation: jobs.extract
    waits: [reload]
    parameters:
      sourceTable: myproject.mydataset.events
      destinationUris: ["gs://my-bucket/export/${args.version}/events-*.avro"]
      destinationFormat: AVRO
      compression: SNAPPY
```

### Example 7: Guard on freshness, fan-out / fan-in of batch queries, rotate old generations

```yaml
sources:
  - name: partitions            # records with field: dt
    module: bigquery
    parameters:
      query: SELECT DISTINCT dt FROM `myproject.mydataset.staging`
  - name: old_generations       # records with field: table_name
    module: bigquery
    parameters:
      query: |
        SELECT table_name FROM `myproject.mydataset.INFORMATION_SCHEMA.TABLES`
        WHERE table_name LIKE 'events_%' AND creation_time < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)

actions:
  - name: check_staging          # stop everything unless staging was refreshed today and is non-empty
    module: bigquery
    operation: tables.get
    failWhen: payload.numRows = 0 OR payload.lastModifiedTime < ${args.todayEpochMs}
    parameters:
      table: myproject.mydataset.staging

  - name: submit_per_partition   # fan-out: submit one batch job per partition, do not block
    module: bigquery
    operation: jobs.query
    trigger: perElement
    inputs: [partitions]
    waits: [check_staging]
    parameters:
      wait: false
      priority: BATCH
      query: INSERT INTO `myproject.mydataset.events` SELECT * FROM `myproject.mydataset.staging` WHERE dt = @dt
      queryParameters:
        dt: { type: DATE, value: "${dt}" }

  - name: wait_all               # fan-in: one firing waits for every submitted job
    module: bigquery
    operation: jobs.wait
    trigger: collect
    inputs: [submit_per_partition]   # envelopes carry the job ids
    parameters:
      jobIdField: jobId

  - name: count_check            # data-quality gate on a small result
    module: bigquery
    operation: jobs.query
    waits: [wait_all]
    failWhen: payload.firstRow.missing > 0
    parameters:
      query: SELECT COUNTIF(id IS NULL) AS missing FROM `myproject.mydataset.events`
      resultRows: 1

  - name: drop_old               # cleanup, idempotent
    module: bigquery
    operation: tables.delete
    trigger: perElement
    inputs: [old_generations]
    waits: [count_check]
    parameters:
      table: myproject.mydataset.${table_name}
```
