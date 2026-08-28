---
type: Sink Module
title: BigQuery Sink Module
description: Writes input data to Google BigQuery tables. Supports multiple write methods (FILE_LOADS, STREAMING_INSERTS, STORAGE_WRITE_API, STORAGE_API_AT_LEAST_ONCE), dynamic destination tables via FreeMarker templates, time partitioning, clustering, primary key, schema update options, auto-sharding, KMS encryption, and failure handling.
tags: [sink, bigquery, batch, streaming, gcp, sql]
timestamp: 2026-06-23T00:00:00Z
---

# BigQuery Sink Module

Sink Module for writing input data to [Google BigQuery](https://cloud.google.com/bigquery/docs) tables. Each input record is converted and written to the specified BigQuery table using one of several write methods.

Supports four write methods:

- **FILE_LOADS** - Uses BigQuery load jobs. Best for batch mode with large volumes of data.
- **STREAMING_INSERTS** - Uses the legacy [streaming insert API](https://cloud.google.com/bigquery/docs/streaming-data-into-bigquery). Available in streaming mode.
- **STORAGE_WRITE_API** - Uses the [Storage Write API](https://cloud.google.com/bigquery/docs/write-api) with exactly-once semantics.
- **STORAGE_API_AT_LEAST_ONCE** - Uses the Storage Write API with at-least-once semantics for higher throughput.

If not specified, the write method is automatically determined based on the pipeline execution mode (batch/streaming).

The destination table can be specified statically or dynamically using FreeMarker template expressions with input field values.

## Sink module common parameters

| parameter  | optional | type                | description                                                           |
|------------|----------|---------------------|-----------------------------------------------------------------------|
| name       | required | String              | Step name. specified to be unique in config file.                     |
| module     | required | String              | Specified `bigquery`                                                  |
| inputs     | required | Array<String\>      | Specify the names of the step to be used as input.                    |
| waits      | optional | Array<String\>      | Specify the names of the steps to wait for before processing.        |
| strategy   | optional | [Strategy](../common/strategy.md) | Windowing strategy for streaming execution.               |
| parameters | required | Map<String,Object\> | Specify the following individual parameters                          |

## BigQuery sink module parameters

### Destination parameters

| parameter  | optional | type   | description                                                                                                                                                                                                                                                                    |
|------------|----------|--------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| table      | required | String | BigQuery table to write to. Accepts formats: `project.dataset.table`, `project:dataset.table`, or `dataset.table` (uses default project). Supports FreeMarker template expressions (e.g. `myproject.mydataset.events_${region}`) for [dynamic destination](#dynamic-destination). |
| projectId  | optional | String | GCP Project ID. Used when `table` is not in fully-qualified format. If not specified, the pipeline execution environment's project is used.                                                                                                                                    |
| datasetId  | optional | String | Dataset ID. Can be used together with `tableId` instead of the `table` parameter.                                                                                                                                                                                              |

### Write behavior parameters

| parameter         | optional | type | description                                                                                                                                                                                         |
|-------------------|----------|------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| writeDisposition  | optional | Enum | Write disposition. Values: `WRITE_EMPTY` (fail if table is not empty), `WRITE_APPEND` (append to table), `WRITE_TRUNCATE` (overwrite table). Default: `WRITE_EMPTY`.                               |
| createDisposition | optional | Enum | Create disposition. Values: `CREATE_NEVER` (fail if table does not exist), `CREATE_IF_NEEDED` (create table if it does not exist). Default: `CREATE_NEVER`.                                         |
| method            | optional | Enum | Write method. Values: `FILE_LOADS`, `STREAMING_INSERTS`, `STORAGE_WRITE_API`, `STORAGE_API_AT_LEAST_ONCE`, `DEFAULT`. If not specified, automatically determined. See [Write methods](#write-methods). |
| writeFormat       | optional | Enum | Internal data format for writing. Values: `json`, `avro`, `row`, `avrofile`. Auto-determined based on method and mode. Specify only when needed for performance or schema compatibility.             |
| outputResult      | optional | Boolean | If `true`, output successful write results. Default: `true` for batch mode with FILE_LOADS/STREAMING_INSERTS/DEFAULT, `false` otherwise.                                                         |
| cdc               | optional | Boolean | CDC apply mode: consume unified change records (the [`cdc` transform](../transform/cdc.md) output) and upsert/delete rows on the destination table. See [CDC apply mode](#cdc-apply-mode). Default: `false`. |
| onTruncate        | optional | Enum | (cdc mode) Reaction to a `TRUNCATE` control record: `skip` (log and drop) or `fail` (fail the pipeline). Default: `skip`. |

### Table creation parameters

These parameters are effective only when `createDisposition` is `CREATE_IF_NEEDED` and the table is first auto-generated.

| parameter         | optional | type           | description                                                                                                                                              |
|-------------------|----------|----------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|
| partitioning      | optional | Enum           | Time partitioning type. Values: `DAY`, `HOUR`, `MONTH`, `YEAR`. If not specified, no time partitioning is applied.                                       |
| partitioningField | optional | String         | Field name to use for time partitioning. The field must be a TIMESTAMP or DATE type. If not specified, the ingestion time is used.                        |
| clusteringFields  | optional | Array<String\> | Field names to use for [clustering](https://cloud.google.com/bigquery/docs/clustered-tables). Up to 4 fields.                                            |
| primaryKeyFields  | optional | Array<String\> | Field names to set as [primary key](https://cloud.google.com/bigquery/docs/information-schema-table-constraints) on the table.                           |

The `description` of each input schema field (declared in a `schema.fields` entry or read from a
source such as a `bigquery` table or a `jdbc` table with column comments — see
[schema](../common/schema.md#field-descriptions)) is part of the table schema this sink submits to
BigQuery. It therefore becomes the BigQuery field description whenever BigQuery applies that schema:
when the table is auto-created (`CREATE_IF_NEEDED`), when a file load with `WRITE_TRUNCATE` replaces
the schema of an existing table, and for fields added through `schemaUpdateOptions`. Appending to an
existing table without schema updates leaves its descriptions untouched.

### Streaming mode parameters

These parameters are applicable only in streaming mode.

| parameter                | optional | type    | description                                                                                                                                                                                                          |
|--------------------------|----------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| skipInvalidRows          | optional | Boolean | If `true`, inserts all valid rows even if some rows are invalid. Default: `false`.                                                                                                                                   |
| ignoreInsertIds          | optional | Boolean | If `true`, disables [insertId-based deduplication](https://cloud.google.com/bigquery/streaming-data-into-bigquery#disabling_best_effort_de-duplication). Improves throughput but may allow duplicates. Default: `false`. |
| withExtendedErrorInfo    | optional | Boolean | If `true`, enables extended error information for failed inserts (includes error message, reason, location). Only for `STREAMING_INSERTS`. Default: `false`.                                                         |
| failedInsertRetryPolicy  | optional | Enum    | Retry policy for failed inserts. Values: `always`, `never`, `retryTransientErrors`. Only for `STREAMING_INSERTS`. Default: `always`.                                                                                 |
| triggeringFrequencySecond| optional | Long    | Frequency in seconds for triggering writes. Only for `FILE_LOADS`, `STORAGE_WRITE_API`, or `STORAGE_API_AT_LEAST_ONCE` in streaming mode. Default: `10`.                                                            |
| numStorageWriteApiStreams| optional | Integer | Number of Storage Write API streams. Only for `STORAGE_WRITE_API` or `STORAGE_API_AT_LEAST_ONCE`. If not specified, `autoSharding` is enabled automatically.                                                        |
| autoSharding             | optional | Boolean | If `true`, uses a dynamically determined number of shards. Applicable to `FILE_LOADS` and `STREAMING_INSERTS` in streaming mode. Default: `false`.                                                                   |

### Other parameters

| parameter            | optional | type           | description                                                                                                                                                                                                   |
|----------------------|----------|----------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| kmsKey               | optional | String         | [Cloud KMS key](https://cloud.google.com/bigquery/docs/customer-managed-encryption) for encrypting data written to BigQuery.                                                                                  |
| ignoreUnknownValues  | optional | Boolean        | If `true`, values that do not match the destination table schema (e.g. columns not present on the table) are dropped instead of failing the row. Applies to all write methods, in both batch and streaming mode. Cannot be combined with `schemaUpdateOptions`. Default: `false`. |
| schemaUpdateOptions  | optional | Array<Enum\>   | Allows schema updates during writes. Values: `ALLOW_FIELD_ADDITION`, `ALLOW_FIELD_RELAXATION`. Only applicable with `FILE_LOADS` method. Cannot be combined with `ignoreUnknownValues` or `autoSchemaUpdate`. |
| autoSchemaUpdate     | optional | Boolean        | If `true`, enables [automatic schema update](https://cloud.google.com/bigquery/docs/write-api#update_the_schema): a streaming pipeline picks up destination table schema changes (e.g. `ALTER TABLE ... ADD COLUMN`) without a restart. Only applicable with `STORAGE_WRITE_API` or `STORAGE_API_AT_LEAST_ONCE` method, and requires `ignoreUnknownValues: true` (unknown values are then held back and merged once the write stream reports the updated schema, instead of being dropped). |
| optimizedWrites      | optional | Boolean        | If `true`, enables optimized write codepaths that use fewer resources. Default: `false`.                                                                                                                      |
| withoutValidation    | optional | Boolean        | If `true`, skips validation of the destination table. Default: `false`.                                                                                                                                       |
| customGcsTempLocation| optional | String         | Custom GCS path for temporary files during load jobs. If not specified, uses the pipeline's `tempLocation` setting.                                                                                           |

## Write methods

| method                    | description                                                                                      | recommended use                                |
|---------------------------|--------------------------------------------------------------------------------------------------|------------------------------------------------|
| FILE_LOADS                | Uses BigQuery load jobs. Batches data into files and loads them.                                 | Batch mode with large data volumes.            |
| STREAMING_INSERTS         | Uses the legacy streaming insert API. Near real-time but at-least-once.                          | Streaming mode with low latency needs.         |
| STORAGE_WRITE_API         | Uses the Storage Write API with exactly-once semantics.                                          | Streaming mode when exactly-once is required.  |
| STORAGE_API_AT_LEAST_ONCE | Uses the Storage Write API without exactly-once. Higher throughput than STORAGE_WRITE_API.        | Streaming mode when duplicates are acceptable. |
| DEFAULT                   | Auto-determined: FILE_LOADS in batch mode, STREAMING_INSERTS in streaming mode.                  | When you don't need explicit method control.   |

### Automatic write format selection

The internal data format used for writing is automatically determined:

| method             | batch mode | streaming mode |
|--------------------|------------|----------------|
| FILE_LOADS         | avrofile   | avrofile       |
| STREAMING_INSERTS  | -          | json           |
| STORAGE_WRITE_API  | row        | row            |
| STORAGE_API_AT_LEAST_ONCE | row        | row            |
| DEFAULT            | avrofile   | json           |

You can override this with the `writeFormat` parameter if needed.

## Dynamic destination

When the `table` parameter contains FreeMarker template expressions (e.g. `${field_name}`), each record is routed to a different destination table based on its field values.

```
myproject.mydataset.events_${region}
```

All input field names can be used as template variables. The template is evaluated for each record to determine the destination table.

When using dynamic destination, `partitioning`, `partitioningField`, and `clusteringFields` are also applied to each dynamically created table. The schema for all destination tables is derived from the input schema — except in [CDC apply mode](#cdc-apply-mode), where each destination table must already exist and its schema is fetched from BigQuery.

## CDC apply mode

With `cdc: true`, the sink consumes **unified change records** — the output of the
[`cdc` transform](../transform/cdc.md) — and applies them to the destination table as
upserts/deletes through the Storage Write API
[`_CHANGE_TYPE` / `_CHANGE_SEQUENCE_NUMBER`](https://cloud.google.com/bigquery/docs/change-data-capture)
pseudocolumns. Works in both streaming (live change stream) and batch (replay of archived change
records) pipelines.

- The row written is `keys ∪ after` for `INSERT`/`UPDATE`/`SNAPSHOT` (mapped to `UPSERT`), and the
  key values only for `DELETE`.
- The envelope `sequence` field becomes `_CHANGE_SEQUENCE_NUMBER`, so out-of-order delivery resolves
  to the latest change per key.
- `method` must be `STORAGE_API_AT_LEAST_ONCE` (default in this mode) or `STORAGE_WRITE_API`.
- The destination table must already exist (`CREATE_NEVER`) with a
  [primary key](https://cloud.google.com/bigquery/docs/information-schema-table-constraints) and
  `max_staleness` configured as needed; its schema cannot be derived from change records.
- `after` is applied as a whole-row `UPSERT`: every column missing from `keys ∪ after` is written
  as NULL. The change records must therefore carry the **full row** — with Spanner change streams
  use `valueCaptureType: NEW_ROW` (or `NEW_ROW_AND_OLD_VALUES`); the default `OLD_AND_NEW_VALUES`
  delivers only the modified columns and would null out the others.
- Control records (`TRUNCATE`, `SCHEMA`, ...) carry no row mutation and are dropped (counted in
  the `bigquery_sink_cdc_control_records` metric). `onTruncate: fail` stops the pipeline on a
  `TRUNCATE` instead, since the destination would otherwise keep rows the source no longer has. To
  apply truncations and schema changes, route the control records to an action (e.g.
  `partition` on `op` → a `bigquery` action running `TRUNCATE TABLE` / `ALTER TABLE`) — the
  [`cdc` transform](../transform/cdc.md#control-records) describes the records.
- A template `table` (e.g. `myproject.mydataset.${table}`) routes each change record to its own
  destination table — one sink applies a whole change stream to many tables. The schema of each
  destination table is fetched from BigQuery at write time. **Every table the template resolves to
  must already exist**: a change record referencing an unknown table is a request-level error that
  fails the pipeline (not a row failure). If the stream contains tables you do not want to apply,
  filter them out upstream on the envelope `table` field (e.g. with the `select` transform).

```yaml
sinks:
  - name: bq
    module: bigquery
    inputs: [normalized_changes]
    parameters:
      table: myproject.mydataset.${table}   # or a fixed table name for a single-table sink
      cdc: true
```

### Schema evolution

Source schema changes never break the pipeline — the change records carry row data as JSON, and
the [`cdc` transform](../transform/cdc.md#control-records) reports each change as a `SCHEMA`
control record. Keeping the destination table in sync has two independent parts: **adding the
column** on BigQuery and **not losing the values** written before the column existed.

**Recommended setup (additive changes, no restart):**

```yaml
transforms:
  - name: normalize
    module: cdc
    inputs: [change_stream]
    parameters:
      format: spanner
      schemaChanges:                  # SCHEMA records carry BigQuery DDL in `statement`
        dialect: bigquery
        table: myproject.mydataset.${table}
  - name: route
    module: partition
    inputs: [normalize]
    parameters:
      partitions:
        - name: ddl
          filter: { key: op, op: "=", value: SCHEMA }
        - name: rows
          filter: { key: op, op: in, value: [INSERT, UPDATE, DELETE, SNAPSHOT] }

sinks:
  - name: bq
    module: bigquery
    inputs: [route.rows]
    parameters:
      table: myproject.mydataset.${table}
      cdc: true
      method: STORAGE_WRITE_API
      ignoreUnknownValues: true       # required by autoSchemaUpdate
      autoSchemaUpdate: true          # pick up the new column without a restart
  - name: archive                     # envelope archive: the backfill source (see below)
    module: storage
    inputs: [normalize]
    parameters:
      output: gs://mybucket/cdc/envelope/
      format: avro
actions:
  - name: apply_ddl                   # ALTER TABLE ... ADD COLUMN IF NOT EXISTS (idempotent)
    module: bigquery
    operation: jobs.query
    trigger: perElement
    inputs: [route.ddl]
    parameters:
      query: ${statement}
```

How it behaves:

1. The `cdc` transform detects the new column from the change records (Spanner: requires
   `valueCaptureType: NEW_ROW` / `NEW_ROW_AND_OLD_VALUES`), emits a `SCHEMA` record with the
   `ALTER TABLE` statement, and — with the default `schemaChanges.baseline: destination` — also
   reports a column that was added while the pipeline was down.
2. The `bigquery` action runs the statement. Duplicate reports (one per worker) are harmless: the DDL
   is `IF NOT EXISTS` and the action's job id is derived from the statement.
3. With `autoSchemaUpdate: true` the Storage Write API writer does **not** drop unknown columns:
   it keeps them aside and merges them once the write stream reports the updated table schema
   (Beam requires `ignoreUnknownValues: true` for this mode — the sink rejects the combination
   without it). Rows written after the writer refreshed its schema carry the new column.
4. **Gap**: rows written between the `ALTER` and the writer's schema refresh (typically the first
   append batch per worker after the change) are stored without the new column. Backfill them
   from the envelope archive: replay the window around the `SCHEMA` record's `commitTimestamp`
   with `format: envelope` + `accumulate: true` (example below). Replaying more than the window is
   harmless — `_CHANGE_SEQUENCE_NUMBER` ignores changes the destination already has.

Replay (batch) of an archived window:

```yaml
sources:
  - name: archived
    module: storage
    parameters:
      input: gs://mybucket/cdc/envelope/2024-08-09/*.avro
      format: avro
transforms:
  - name: latest
    module: cdc
    inputs: [archived]
    parameters:
      format: envelope
      accumulate: true
sinks:
  - name: bq
    module: bigquery
    inputs: [latest]
    parameters:
      table: myproject.mydataset.${table}
      cdc: true
      method: STORAGE_WRITE_API
```

**Strict alternative (no silent gap, restart required):** `autoSchemaUpdate: false` +
`ignoreUnknownValues: false`. Rows with unknown columns fail row-by-row and are routed to the
failure output as **replayable envelopes** (see [Failure output](#failure-output)) while the other
rows keep flowing; after the `ALTER` (manual, or by the action above) restart/update the pipeline
so the writer fetches the new schema, then replay the failure records with `format: envelope`
(`field: record.json`). Use a failure path distinct from the replay pipeline's own failure sink.

**Manual operation:** replace `apply_ddl` by a `pubsub` sink (or any notification) and run the
`ALTER TABLE` from `statement` by hand — everything else stays the same. This is the setup for
destinations whose schema is governed (policy tags, column descriptions) or where the pipeline's
service account must not hold `bigquery.tables.update`.

**Not automated** (reported by the `SCHEMA` record, handled by hand): type changes
(`schemaChanges.onTypeChange`), dropped and renamed columns (the destination column is kept; a
rename appears as drop + add), key changes, `NOT NULL` constraints, policy tags, and existing-row
backfills of `ADD COLUMN ... DEFAULT` (not part of the change stream).

### Performance and quotas

**Destination schema fetch.** In cdc mode the destination table schema is fetched from BigQuery
lazily on the workers — once per destination table per worker process, then cached process-wide.
There is no per-record or per-bundle schema RPC, and no BigQuery access at pipeline construction.
The cache is only refreshed periodically when `autoSchemaUpdate: true` (that refresh is what picks
up an `ALTER TABLE` without a restart). This behavior is the same for a fixed and a template
`table`.

**Method characteristics with many destination tables.**

- `STORAGE_API_AT_LEAST_ONCE` (the default): no shuffle; records append to each table's *default*
  stream, which consumes no stream-creation quota. Each worker keeps an open append stream and
  buffer per destination table it has seen, so per-worker memory and gRPC connections grow with the
  number of tables — negligible for tens of tables. For hundreds of tables, consider the Storage
  Write API connection-pool pipeline options (`useStorageApiConnectionPool`,
  `minConnectionPoolConnections` / `maxConnectionPoolConnections`) to multiplex connections.
- `STORAGE_WRITE_API` (exactly-once): records are shuffled keyed by destination × shard. In a
  **streaming** pipeline with a template `table`, new write streams are created per destination
  table every triggering period, so
  [`CreateWriteStream` quota](https://cloud.google.com/bigquery/quotas#write-api-limits) usage
  scales with *(number of tables × triggering frequency)*. Raise `triggeringFrequencySecond` when
  applying many tables, or prefer `STORAGE_API_AT_LEAST_ONCE` — the envelope `sequence`
  (`_CHANGE_SEQUENCE_NUMBER`) already resolves out-of-order and duplicate applies per key, which is
  why at-least-once is the default for this mode. In batch (e.g. archive replay), stream creation
  happens once per table and this concern does not apply.

**Template evaluation.** With a template `table`, the destination is evaluated per record
(the compiled template is cached); the overhead is a string build per record and is normally
negligible next to write I/O.

Sharding is tuned the same way as normal storage-API writes: `numStorageWriteApiStreams`, or
`autoSharding` in streaming mode.

## Failure output

Failed insert records are captured and available as error output. The failure output follows the standard MFailure schema:

In [CDC apply mode](#cdc-apply-mode) the `record.json` of a failed write is the **change record
envelope itself** (`table`, `op`, `keys`, `after`, `sequence`, ...), not the merged destination
row, so failure records can be replayed with the `cdc` transform (`format: envelope`,
`field: record.json`).

| field     | type      | description                                      |
|-----------|-----------|--------------------------------------------------|
| job       | STRING    | Pipeline job name (nullable).                    |
| module    | STRING    | Sink module name.                                |
| input     | STRING    | The failed record data as string (nullable).     |
| error     | STRING    | Error message or details (nullable).             |
| timestamp | TIMESTAMP | Timestamp when the failure occurred.             |
| eventtime | TIMESTAMP | Original event time of the failed record.        |

When `withExtendedErrorInfo` is `true` (STREAMING_INSERTS only), the `error` field contains a JSON object with detailed error information including `message`, `reason`, `location`, and `debugInfo` for each error.

## Examples

### Example 1: Basic write to BigQuery

Write data to a BigQuery table using default settings (FILE_LOADS in batch).

```yaml
sources:
  - name: spanner_source
    module: spanner
    parameters:
      projectId: myproject
      instanceId: myinstance
      databaseId: mydatabase
      query: "SELECT user_id, name, email FROM Users"

sinks:
  - name: bigquery_sink
    module: bigquery
    inputs:
      - spanner_source
    parameters:
      table: "myproject.mydataset.users"
      writeDisposition: WRITE_TRUNCATE
      createDisposition: CREATE_IF_NEEDED
```

### Example 2: Write with partitioning and clustering

Create a partitioned and clustered table.

```yaml
sinks:
  - name: bigquery_sink
    module: bigquery
    inputs:
      - source
    parameters:
      table: "myproject.mydataset.events"
      writeDisposition: WRITE_TRUNCATE
      createDisposition: CREATE_IF_NEEDED
      partitioning: DAY
      partitioningField: event_time
      clusteringFields:
        - user_id
        - event_type
```

### Example 3: Append to existing table

Append data to an existing BigQuery table.

```yaml
sinks:
  - name: bigquery_sink
    module: bigquery
    inputs:
      - source
    parameters:
      table: "myproject.mydataset.logs"
      writeDisposition: WRITE_APPEND
      createDisposition: CREATE_NEVER
```

### Example 4: Streaming write with Storage Write API

Write to BigQuery in streaming mode with exactly-once semantics.

```yaml
sinks:
  - name: bigquery_sink
    module: bigquery
    inputs:
      - pubsub_source
    parameters:
      table: "myproject.mydataset.events"
      method: STORAGE_WRITE_API
      writeDisposition: WRITE_APPEND
      createDisposition: CREATE_NEVER
      triggeringFrequencySecond: 30
      numStorageWriteApiStreams: 4
```

### Example 5: Streaming write with at-least-once semantics

Higher throughput streaming write without exactly-once guarantees.

```yaml
sinks:
  - name: bigquery_sink
    module: bigquery
    inputs:
      - pubsub_source
    parameters:
      table: "myproject.mydataset.metrics"
      method: STORAGE_API_AT_LEAST_ONCE
      writeDisposition: WRITE_APPEND
      createDisposition: CREATE_NEVER
      autoSharding: true
```

### Example 6: Streaming insert with error handling

Use streaming inserts with extended error information.

```yaml
sinks:
  - name: bigquery_sink
    module: bigquery
    inputs:
      - pubsub_source
    parameters:
      table: "myproject.mydataset.events"
      method: STREAMING_INSERTS
      writeDisposition: WRITE_APPEND
      createDisposition: CREATE_NEVER
      skipInvalidRows: true
      withExtendedErrorInfo: true
      failedInsertRetryPolicy: retryTransientErrors
```

### Example 7: Dynamic destination tables

Route records to different tables based on field values using FreeMarker template.

```yaml
sinks:
  - name: bigquery_sink
    module: bigquery
    inputs:
      - source
    parameters:
      table: "myproject.mydataset.events_${region}"
      writeDisposition: WRITE_APPEND
      createDisposition: CREATE_IF_NEEDED
      partitioning: DAY
      partitioningField: event_time
```

This routes records to tables like `events_JP`, `events_US`, etc. based on each record's `region` field value.

### Example 8: Write with schema update options

Allow adding new fields to the destination table during writes.

```yaml
sinks:
  - name: bigquery_sink
    module: bigquery
    inputs:
      - source
    parameters:
      table: "myproject.mydataset.evolving_table"
      method: FILE_LOADS
      writeDisposition: WRITE_APPEND
      createDisposition: CREATE_NEVER
      schemaUpdateOptions:
        - ALLOW_FIELD_ADDITION
        - ALLOW_FIELD_RELAXATION
```

### Example 9: Write with KMS encryption

Encrypt BigQuery data with a customer-managed encryption key.

```yaml
sinks:
  - name: bigquery_sink
    module: bigquery
    inputs:
      - source
    parameters:
      table: "myproject.mydataset.sensitive_data"
      writeDisposition: WRITE_TRUNCATE
      createDisposition: CREATE_IF_NEEDED
      kmsKey: "projects/myproject/locations/global/keyRings/myring/cryptoKeys/mykey"
```

### Example 10: Write with primary key

Create a table with primary key constraints.

```yaml
sinks:
  - name: bigquery_sink
    module: bigquery
    inputs:
      - source
    parameters:
      table: "myproject.mydataset.master_data"
      writeDisposition: WRITE_TRUNCATE
      createDisposition: CREATE_IF_NEEDED
      primaryKeyFields:
        - id
```

### Example 11: Spanner to BigQuery pipeline

Export Spanner query results to BigQuery.

```yaml
sources:
  - name: spanner_source
    module: spanner
    parameters:
      projectId: myproject
      instanceId: myinstance
      databaseId: mydatabase
      query: >
        SELECT user_id, name, email, status, created_at
        FROM Users
        WHERE status = 'active'

sinks:
  - name: bigquery_sink
    module: bigquery
    inputs:
      - spanner_source
    parameters:
      table: "myproject.mydataset.active_users"
      writeDisposition: WRITE_TRUNCATE
      createDisposition: CREATE_IF_NEEDED
      partitioning: DAY
      partitioningField: created_at
      clusteringFields:
        - status
```

### Example 12: Streaming FILE_LOADS with auto-sharding

Use file loads in streaming mode with automatic sharding.

```yaml
sinks:
  - name: bigquery_sink
    module: bigquery
    inputs:
      - pubsub_source
    parameters:
      table: "myproject.mydataset.stream_data"
      method: FILE_LOADS
      writeDisposition: WRITE_APPEND
      createDisposition: CREATE_NEVER
      triggeringFrequencySecond: 60
      autoSharding: true
```
