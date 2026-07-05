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

### Table creation parameters

These parameters are effective only when `createDisposition` is `CREATE_IF_NEEDED` and the table is first auto-generated.

| parameter         | optional | type           | description                                                                                                                                              |
|-------------------|----------|----------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|
| partitioning      | optional | Enum           | Time partitioning type. Values: `DAY`, `HOUR`, `MONTH`, `YEAR`. If not specified, no time partitioning is applied.                                       |
| partitioningField | optional | String         | Field name to use for time partitioning. The field must be a TIMESTAMP or DATE type. If not specified, the ingestion time is used.                        |
| clusteringFields  | optional | Array<String\> | Field names to use for [clustering](https://cloud.google.com/bigquery/docs/clustered-tables). Up to 4 fields.                                            |
| primaryKeyFields  | optional | Array<String\> | Field names to set as [primary key](https://cloud.google.com/bigquery/docs/information-schema-table-constraints) on the table.                           |

### Streaming mode parameters

These parameters are applicable only in streaming mode.

| parameter                | optional | type    | description                                                                                                                                                                                                          |
|--------------------------|----------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| skipInvalidRows          | optional | Boolean | If `true`, inserts all valid rows even if some rows are invalid. Default: `false`.                                                                                                                                   |
| ignoreUnknownValues      | optional | Boolean | If `true`, accepts rows with values that do not match the schema. Default: `false`.                                                                                                                                  |
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
| schemaUpdateOptions  | optional | Array<Enum\>   | Allows schema updates during writes. Values: `ALLOW_FIELD_ADDITION`, `ALLOW_FIELD_RELAXATION`. Only applicable with `FILE_LOADS` method.                                                                     |
| autoSchemaUpdate     | optional | Boolean        | If `true`, enables [automatic schema update](https://cloud.google.com/bigquery/docs/write-api#update_the_schema). Only applicable with `STORAGE_WRITE_API` method.                                           |
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

When using dynamic destination, `partitioning`, `partitioningField`, and `clusteringFields` are also applied to each dynamically created table. The schema for all destination tables is derived from the input schema.

## Failure output

Failed insert records are captured and available as error output. The failure output follows the standard MFailure schema:

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
