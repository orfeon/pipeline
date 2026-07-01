---
type: Source Module
title: BigQuery Source Module
description: Reads data from Google BigQuery via SQL query or table direct read. Supports Standard SQL with query result caching, Storage Read API for direct table access with field/row filtering, view mode for periodic polling, and microbatch mode for near real-time streaming.
tags: [source, bigquery, batch, gcp, sql]
timestamp: 2026-06-23T00:00:00Z
---

# BigQuery Source Module

Source Module for reading data from [Google BigQuery](https://cloud.google.com/bigquery/docs). Supports two primary read methods:

- **Query read**: Execute a Standard SQL query and read the results.
- **Table direct read**: Read directly from a table using the [BigQuery Storage Read API](https://cloud.google.com/bigquery/docs/reference/storage), with optional field selection and row filtering.

Additionally, two special execution modes are available for streaming pipelines:

- **View mode**: Periodically polls a BigQuery query and outputs the results as a map keyed by a specified field.
- **Microbatch mode**: Executes time-ranged queries at regular intervals for near real-time data ingestion.

Schema is automatically inferred from the query result or table definition; no `schema` parameter is needed.

## Source module common parameters

| parameter          | optional | type                | description                                                                                                                  |
|--------------------|----------|---------------------|------------------------------------------------------------------------------------------------------------------------------|
| name               | required | String              | Step name. specified to be unique in config file.                                                                            |
| module             | required | String              | Specified `bigquery`                                                                                                         |
| schema             | -        | -                   | Not required. Schema is automatically inferred from the query or table.                                                      |
| timestampAttribute | optional | String              | If you want to use the value of a field as the event time, specify the name of the field. (The field must be Timestamp or Date type) |
| parameters         | required | Map<String,Object\> | Specify the following individual parameters                                                                                  |

## BigQuery source module parameters

### Query read parameters

| parameter        | optional           | type   | description                                                                                                                                                                                                                                                           |
|------------------|--------------------|--------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| query            | selective required | String | Standard SQL query to read data from BigQuery. Either `query` or `table` must be specified. Supports inline SQL, GCS path (`gs://...`), or [Parameter Manager](https://cloud.google.com/secret-manager/parameter-manager/docs) resource path. Supports FreeMarker template variables via `system.args`. |
| queryTempDataset | optional           | String | Temporary dataset to store query results. Format: `dataset_id` or `project_id.dataset_id`. If not specified, a temporary dataset is created automatically (requires dataset create/delete permissions).                                                                |
| queryLocation    | optional           | String | Query execution location (e.g. `US`, `asia-northeast1`). If not specified, it is automatically estimated from the datasets referenced in the query (requires `bigquery.datasets.get` permission).                                                                      |
| queryPriority    | optional           | Enum   | Query execution priority. Values: `INTERACTIVE`, `BATCH`. Default: `INTERACTIVE`.                                                                                                                                                                                      |
| queryRunProjectId | optional          | String | Project ID to use for running the query. If not specified, the pipeline execution environment's project is used.                                                                                                                                                        |

### Table direct read parameters

| parameter      | optional           | type           | description                                                                                                                                                                                                                                               |
|----------------|--------------------|----------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| table          | selective required | String         | BigQuery table reference. Accepts formats: `project.dataset.table`, `dataset.table` (uses default project), or `project:dataset.table`. Either `query` or `table` must be specified.                                                                       |
| projectId      | optional           | String         | Project ID for the table. Used when `table` is specified as just a table name with `datasetId`. If not specified, the pipeline execution environment's project is used.                                                                                     |
| datasetId      | optional           | String         | Dataset ID for the table. Used in combination with `table` when the table reference is just the table name.                                                                                                                                                |
| fields         | optional           | Array<String\> | Field names to read from the table (column projection). Only the specified fields are read, reducing data transfer. Only available for table direct read.                                                                                                   |
| rowRestriction | optional           | String         | SQL predicate to filter rows at the storage level before reading. Uses the same syntax as a `WHERE` clause (e.g. `age > 18 AND status = 'active'`). Only available for table direct read.                                                                  |
| format         | optional           | Enum           | Data format for Storage Read API. Values: `AVRO`, `ARROW`. Only available for table direct read.                                                                                                                                                           |

### Common parameters

| parameter | optional | type   | description                                                                                                                           |
|-----------|----------|--------|---------------------------------------------------------------------------------------------------------------------------------------|
| mode      | optional | Enum   | Execution mode. Values: `batch` (default), `microBatch`, `view`. See [Execution modes](#execution-modes).                             |
| method    | optional | Enum   | Read method. Values: `EXPORT` (via extract job), `DIRECT_READ` (via Storage Read API). Default: auto-detected based on query/table.   |
| kmsKey    | optional | String | Cloud KMS key for encrypting temporary data.                                                                                           |

### Query loading

The `query` parameter supports multiple sources:

| format                     | example                                                    | description                                                               |
|----------------------------|------------------------------------------------------------|---------------------------------------------------------------------------|
| Inline SQL                 | `"SELECT * FROM \`project.dataset.table\`"`                | SQL text specified directly in config.                                    |
| GCS path                   | `"gs://my-bucket/queries/source.sql"`                      | Reads SQL from a Cloud Storage file. Supports FreeMarker template variables. |
| Parameter Manager resource | `"projects/p/locations/l/parameters/n/versions/v"`         | Reads SQL from Google Cloud Parameter Manager.                            |

## Execution modes

### Batch mode (default)

Standard batch read. The query or table is read once and all results are output.

- For query read: uses `BigQueryIO.read().fromQuery()` with Standard SQL
- For table direct read: uses `BigQueryIO.read().from()` with Storage Read API (default) or export method

### View mode

Periodically polls a BigQuery query and outputs the entire result set as a single map element, keyed by a specified field. Useful for loading slowly-changing lookup data in streaming pipelines.

| parameter             | optional | type    | description                                                                   |
|-----------------------|---------|---------|-------------------------------------------------------------------------------|
| view.keyField         | required | String  | Field name to use as the map key for the query result.                        |
| view.intervalMinute   | optional | Integer | Polling interval in minutes. Default: `60`.                                   |

In batch mode, the query runs once. In streaming mode, the query runs repeatedly at the specified interval.

### Microbatch mode

Executes time-ranged queries at regular intervals for near real-time data ingestion. The pipeline must run in streaming mode.

| parameter                           | optional | type    | description                                                                                                                                                         |
|-------------------------------------|----------|---------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| microBatch.intervalSecond           | optional | Integer | Interval between query executions in seconds. Default: `60`.                                                                                                        |
| microBatch.gapSecond                | optional | Integer | Buffer time (in seconds) between the end of the query time range and the current time. Default: `30`.                                                               |
| microBatch.maxDurationMinute        | optional | Integer | Maximum time range for a single query in minutes. Used when catching up with a large backlog. Default: `60`.                                                        |
| microBatch.catchupIntervalSecond    | optional | Integer | Query interval (in seconds) when catching up with a backlog. Default: same as `intervalSecond`.                                                                      |
| microBatch.startDatetime            | optional | String  | Start time of the first query. If not set, the value from `outputCheckpoint` is used.                                                                                |
| microBatch.outputCheckpoint         | optional | String  | GCS path to persist the latest query checkpoint timestamp.                                                                                                           |
| microBatch.useCheckpointAsStartDatetime | optional | Boolean | If `true`, uses the checkpoint as the start datetime. Default: `false`.                                                                                          |

## Examples

### Example 1: Read from BigQuery using a query

```yaml
sources:
  - name: bigquery_source
    module: bigquery
    parameters:
      query: >
        SELECT user_id, name, email, created_at
        FROM `myproject.mydataset.users`
        WHERE status = 'active'
      queryLocation: US
      queryTempDataset: temp_dataset
```

### Example 2: Read directly from a BigQuery table

Use Storage Read API for efficient table reads with field selection and row filtering.

```yaml
sources:
  - name: table_source
    module: bigquery
    parameters:
      table: "myproject.mydataset.users"
      fields:
        - user_id
        - name
        - email
      rowRestriction: "status = 'active' AND age >= 18"
```

### Example 3: Read from a table with dataset/project split

```yaml
sources:
  - name: table_source
    module: bigquery
    parameters:
      projectId: myproject
      datasetId: mydataset
      table: users
```

### Example 4: Load query from GCS

Store SQL separately in Cloud Storage for easier management.

```yaml
sources:
  - name: bigquery_source
    module: bigquery
    parameters:
      query: "gs://my-bucket/queries/daily_report.sql"
      queryLocation: US
      queryTempDataset: temp_dataset
```

### Example 5: Query with template variables

Use FreeMarker template variables defined in `system.args`.

```yaml
system:
  args:
    target_date: "2024-01-15"
    region: "JP"

sources:
  - name: bigquery_source
    module: bigquery
    parameters:
      query: >
        SELECT *
        FROM `myproject.mydataset.orders`
        WHERE DATE(created_at) = '${args.target_date}'
          AND region = '${args.region}'
      queryLocation: US
      queryTempDataset: temp_dataset
```

### Example 6: View mode for lookup data

Periodically refresh lookup data from BigQuery in a streaming pipeline.

```yaml
sources:
  - name: lookup_table
    module: bigquery
    parameters:
      mode: view
      query: >
        SELECT category_id, category_name, discount_rate
        FROM `myproject.mydataset.categories`
      view:
        keyField: category_id
        intervalMinute: 30
```

### Example 7: Microbatch mode for near real-time ingestion

Poll BigQuery at regular intervals using time-ranged queries.

```yaml
sources:
  - name: recent_events
    module: bigquery
    parameters:
      mode: microBatch
      query: >
        SELECT event_id, user_id, event_type, event_time
        FROM `myproject.mydataset.events`
        WHERE event_time >= TIMESTAMP('${startDatetime}')
          AND event_time < TIMESTAMP('${endDatetime}')
      queryLocation: US
      queryTempDataset: temp_dataset
      microBatch:
        intervalSecond: 60
        gapSecond: 30
        maxDurationMinute: 120
        startDatetime: "2024-01-01T00:00:00Z"
        outputCheckpoint: "gs://my-bucket/checkpoints/events"
```

### Example 8: BigQuery to Spanner pipeline

```yaml
sources:
  - name: bigquery_source
    module: bigquery
    parameters:
      query: >
        SELECT user_id, name, email
        FROM `myproject.mydataset.users`
      queryLocation: US
      queryTempDataset: temp_dataset

sinks:
  - name: spanner_output
    module: spanner
    inputs:
      - bigquery_source
    parameters:
      projectId: myproject
      instanceId: myinstance
      databaseId: mydatabase
      table: users
```

### Example 9: Read with BATCH query priority

Use BATCH priority for cost-efficient, non-urgent queries.

```yaml
sources:
  - name: batch_source
    module: bigquery
    parameters:
      query: >
        SELECT *
        FROM `myproject.mydataset.large_table`
      queryPriority: BATCH
      queryLocation: US
      queryTempDataset: temp_dataset
```

### Example 10: Read with KMS encryption

```yaml
sources:
  - name: encrypted_source
    module: bigquery
    parameters:
      query: >
        SELECT * FROM `myproject.mydataset.sensitive_data`
      queryLocation: US
      queryTempDataset: temp_dataset
      kmsKey: "projects/myproject/locations/global/keyRings/myring/cryptoKeys/mykey"
```
