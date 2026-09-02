---
type: Source Module
title: Spanner Source Module
description: Reads data from Google Cloud Spanner via SQL query or table scan. Supports batch partitioned queries, table key range filtering, change streams for CDC, view mode for periodic polling, microbatch mode for near real-time streaming, Data Boost, and timestamp-bound reads.
tags: [source, spanner, batch, streaming, gcp, sql, cdc]
timestamp: 2026-06-23T00:00:00Z
---

# Spanner Source Module

Source Module for reading data from [Google Cloud Spanner](https://cloud.google.com/spanner/docs). Supports multiple read methods:

- **Query read**: Execute a SQL query. The query is automatically partitioned for parallel execution across workers.
- **Table read**: Read directly from a table with optional field selection and key range filtering using `SpannerIO.read()`.
- **All-tables read**: Read every base table matching the `tables` patterns, producing one named output per table (`<moduleName>.<tableName>`).

Additionally, the following special execution modes are available:

- **Change stream mode**: Read change data capture (CDC) events from a Spanner [change stream](https://cloud.google.com/spanner/docs/change-streams).
- **View mode**: Periodically poll a Spanner query and output results as a map keyed by a specified field.
- **Microbatch mode**: Execute time-ranged queries at regular intervals for near real-time data ingestion.

Schema is automatically inferred from the query result or table definition; no `schema` parameter is needed.

Supported column types (GoogleSQL): `BOOL`, `INT64`, `FLOAT32`, `FLOAT64`, `NUMERIC`, `STRING`, `BYTES`, `DATE`, `TIMESTAMP`, `JSON`, `UUID`, `ARRAY` of these, and `STRUCT` (query results). Not supported: `INTERVAL`, `PROTO`, `ENUM`, `TOKENLIST` — reading a table or query containing them fails at launch.

## Source module common parameters

| parameter          | optional | type                | description                                                                                                                  |
|--------------------|----------|---------------------|------------------------------------------------------------------------------------------------------------------------------|
| name               | required | String              | Step name. specified to be unique in config file.                                                                            |
| module             | required | String              | Specified `spanner`                                                                                                          |
| schema             | -        | -                   | Not required. Schema is automatically inferred from the query or table.                                                      |
| timestampAttribute | optional | String              | If you want to use the value of a field as the event time, specify the name of the field. (The field must be Timestamp or Date type) |
| parameters         | required | Map<String,Object\> | Specify the following individual parameters                                                                                  |

## Spanner source module parameters

### Common parameters

| parameter       | optional | type    | description                                                                                                                                                                  |
|-----------------|----------|---------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| projectId       | required | String  | GCP Project ID of the Spanner instance.                                                                                                                                      |
| instanceId      | required | String  | Spanner instance ID.                                                                                                                                                          |
| databaseId      | required | String  | Spanner database ID.                                                                                                                                                          |
| mode            | optional | Enum    | Execution mode. Values: `batch` (default), `microBatch`, `changeDataCapture`, `view`. See [Execution modes](#execution-modes).                                               |
| priority        | optional | Enum    | Query [RPC priority](https://cloud.google.com/spanner/docs/cpu-utilization). Values: `HIGH`, `MEDIUM`, `LOW`. Default: `MEDIUM`.                                             |
| enableDataBoost | optional | Boolean | Enable [Data Boost](https://cloud.google.com/spanner/docs/databoost/databoost-overview) for independent compute resources. Default: `false`.                                 |
| requestTag      | optional | String  | [Request tag](https://cloud.google.com/spanner/docs/introspection/troubleshooting-with-tags#request_tags) for query tracing and diagnostics.                                  |
| timestampBound  | optional | String  | Read data as of a specific timestamp (stale read). Format: ISO-8601 (e.g. `2024-01-15T10:30:00Z`). If not specified, a strong read is used.                                  |
| emulator        | optional | Boolean | If `true`, connects to the Spanner emulator instead of production. Default: `false`.                                                                                          |

### Query read parameters

| parameter | optional           | type   | description                                                                                                                                                                                     |
|-----------|--------------------|--------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| query     | selective required | String | SQL query to read data from Spanner. Either `query` or `table` must be specified. Supports inline SQL or GCS path (`gs://...`). Supports FreeMarker template variables via `system.args`.       |

The query is executed as a partitioned query using `BatchReadOnlyTransaction`. If partitioning fails (e.g. for queries that cannot be partitioned), it falls back to a single-query execution automatically.

Multiple SQL statements can be concatenated with the separator `--SPLITTER--` to execute them in sequence within the same transaction.

### Table read parameters

| parameter | optional           | type                                  | description                                                                                                                                                                      |
|-----------|--------------------|---------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| table     | selective required | String                                | Spanner table name to read from. Either `query` or `table` must be specified.                                                                                                    |
| fields    | optional           | Array<String\>                        | Field names to read (column projection). If not specified, all columns are read.                                                                                                  |
| keyRange  | optional           | Array<[KeyRange](#keyrange-parameters)\> | Key ranges to filter rows by primary key. If not specified, all rows are read (`KeySet.all()`).                                                                                |

#### KeyRange parameters

| parameter | optional | type           | description                                                                                                                                |
|-----------|----------|----------------|--------------------------------------------------------------------------------------------------------------------------------------------|
| startType | optional | Enum           | Start boundary type. Values: `closed` (inclusive, default), `open` (exclusive).                                                            |
| endType   | optional | Enum           | End boundary type. Values: `closed` (inclusive, default), `open` (exclusive).                                                              |
| startKeys | optional | JSON           | Start key values. A single value for single-column primary key, or a JSON array for composite keys. Supports STRING, INT64, FLOAT64, BOOL, DATE, TIMESTAMP types. |
| endKeys   | optional | JSON           | End key values. Same format as `startKeys`.                                                                                                |

### All-tables read parameters

Read all base tables matching the given patterns in a single module. Exactly one of `query`, `table` or `tables` must be specified (`fields` and `keyRange` are not supported with `tables`).

| parameter        | optional           | type            | description                                                                                                                                                     |
|------------------|--------------------|-----------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| tables           | selective required | Object or Array | Table name patterns. Either an object with `includes`/`excludes`/`query`, or an array shorthand for `includes`. `*` in a pattern matches any character sequence. |
| tables.includes  | optional           | Array<String\>  | Patterns of tables to read. Default: `["*"]` (all base tables).                                                                                                 |
| tables.excludes  | optional           | Array<String\>  | Patterns of tables to exclude. Default: `[]`.                                                                                                                    |
| tables.query     | optional           | String          | Common query template applied to every matched table; **must reference `${table}`**. Inline SQL or a GCS path (`gs://...`). Default: `SELECT * FROM <table>`. Module-level `args` are also available as template variables. `--SPLITTER--` is not supported here. |

Behavior:

- The table list and each table's schema are resolved **at pipeline launch** from `INFORMATION_SCHEMA` (views and system tables are excluded). Matching no table is a launch-time error.
- The module outputs one collection per table, named `<moduleName>.<tableName>` (tables in a named schema are qualified as `<schema>.<table>`). Downstream modules can consume a single table (`inputs: [mySpanner.Users]`) or all of them with a wildcard (`inputs: [mySpanner.*]`).
- Each output carries assembly-time attributes (`table`, `projectId`, `instanceId`, `databaseId`) usable in downstream sinks via the `${input.*}` template (see the config README).
- Every table is read as a **partitioned query** (same machinery as the `query` parameter: `partitionQuery` + per-partition parallel reads via the batch endpoint), all attached to **one shared `BatchReadOnlyTransaction`** — so the per-table outputs form a mutually consistent snapshot at that transaction's read timestamp (`timestampBound` applies to it).
- With a custom `tables.query`, the output schema is resolved from the query (`analyzeQuery`), one round trip **per table** at launch — expect slower launches on databases with many tables. Without it, schemas come from the single bulk `INFORMATION_SCHEMA` query.
- Keep a custom `tables.query` [root-partitionable](https://cloud.google.com/spanner/docs/reads#read_data_in_parallel) (simple scan + projection + filter; no aggregation/JOIN/ORDER BY). A non-partitionable query does not fail — it falls back to a single serial read for that table (a WARN is logged), losing the parallelism.
- A query referencing specific columns (e.g. `WHERE updated_at > ...`) fails at launch for tables lacking that column; narrow the target with `includes` in that case.
- GoogleSQL dialect databases only (PostgreSQL dialect databases are not yet supported).

### Change stream parameters

Read change data capture (CDC) events from a Spanner change stream. The pipeline must run in streaming mode with Runner V2 (`additional-experiments=use_runner_v2`).

| parameter                    | optional | type   | description                                                                                                                                |
|------------------------------|----------|--------|--------------------------------------------------------------------------------------------------------------------------------------------|
| changeStream.changeStreamName | required | String | Name of the Spanner change stream to read from.                                                                                           |
| changeStream.metadataInstance | optional | String | Spanner instance for the change stream connector metadata table. Default: same as `instanceId`.                                            |
| changeStream.metadataDatabase | optional | String | Spanner database for the change stream connector metadata table. Default: same as `databaseId`.                                            |
| changeStream.metadataTable   | optional | String | Metadata table name for the change stream connector. If not provided, automatically created.                                               |
| changeStream.inclusiveStartAt | optional | String | Timestamp to start reading change events from (inclusive). Default: current time.                                                          |
| changeStream.inclusiveEndAt  | optional | String | Timestamp to stop reading change events at (inclusive). Default: `Timestamp.MAX_VALUE` (never stop).                                       |

#### Change stream output schema

The module outputs the change stream records with full fidelity — one element per
[DataChangeRecord](https://cloud.google.com/spanner/docs/change-streams/details#data-change-records),
without interpreting the changed values. To turn them into per-row change records (e.g. for BigQuery
CDC upserts via the `bigquery` sink `cdc` mode), pass them through the [`cdc` transform](../transform/cdc.md)
(`format: spanner`); to archive them, write them as-is with a file sink and replay later through the
same `cdc` transform.

| field                               | type            | description                                                             |
|-------------------------------------|-----------------|-------------------------------------------------------------------------|
| partitionToken                      | String          | Change stream partition token.                                          |
| commitTimestamp                     | Timestamp       | Commit timestamp of the transaction.                                    |
| serverTransactionId                 | String          | Transaction id.                                                         |
| isLastRecordInTransactionInPartition | Boolean        | Whether this is the transaction's last record in the partition.         |
| recordSequence                      | String          | Order of the record within its transaction.                             |
| tableName                           | String          | Changed table.                                                          |
| rowType                             | Array<Record\>  | Column catalog: `name`, `code`, `isPrimaryKey`, `ordinalPosition`.      |
| mods                                | Array<Record\>  | Changed rows: `keysJson`, `oldValuesJson`, `newValuesJson` (JSON text). |
| modType                             | String          | `INSERT`, `UPDATE` or `DELETE`.                                         |
| valueCaptureType                    | String          | The change stream's [value capture type](https://cloud.google.com/spanner/docs/change-streams#value-capture-type). |
| transactionTag                      | String          | Transaction tag (nullable).                                             |
| isSystemTransaction                 | Boolean         | Whether the change was made by a system transaction.                    |
| numberOfRecordsInTransaction        | Long            | Records in the transaction.                                             |
| numberOfPartitionsInTransaction     | Long            | Partitions in the transaction.                                          |

### View mode parameters

Periodically poll a Spanner query and output the entire result set as a single map element, keyed by a specified field. Useful for loading slowly-changing lookup data in streaming pipelines.

| parameter            | optional | type    | description                                                     |
|----------------------|---------|---------|-----------------------------------------------------------------|
| view.keyField        | required | String  | Field name to use as the map key for the query result.          |
| view.intervalMinute  | optional | Integer | Polling interval in minutes. Default: `60`.                     |

In batch mode, the query runs once. In streaming mode, the query runs repeatedly at the specified interval.

### Microbatch mode parameters

Execute time-ranged queries at regular intervals for near real-time data ingestion. The pipeline must run in streaming mode.

| parameter                                   | optional | type    | description                                                                                                                                                          |
|---------------------------------------------|----------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| microBatch.intervalSecond                   | optional | Integer | Interval between query executions in seconds. Default: `60`.                                                                                                         |
| microBatch.gapSecond                        | optional | Integer | Buffer time (in seconds) between the end of the query time range and the current time. Default: `30`.                                                                |
| microBatch.maxDurationMinute                | optional | Integer | Maximum time range for a single query in minutes. Used when catching up with a large backlog. Default: `60`.                                                         |
| microBatch.catchupIntervalSecond            | optional | Integer | Query interval (in seconds) when catching up with a backlog. Default: same as `intervalSecond`.                                                                      |
| microBatch.startDatetime                    | optional | String  | Start time of the first query. If not set, the value from `outputCheckpoint` is used.                                                                                |
| microBatch.outputCheckpoint                 | optional | String  | GCS path to persist the latest query checkpoint timestamp.                                                                                                           |
| microBatch.useCheckpointAsStartDatetime     | optional | Boolean | If `true`, uses the checkpoint as the start datetime. Default: `false`.                                                                                              |

## Examples

### Example 1: Read from Spanner using a query

```yaml
sources:
  - name: spanner_source
    module: spanner
    parameters:
      projectId: myproject
      instanceId: myinstance
      databaseId: mydatabase
      query: >
        SELECT user_id, name, email, created_at
        FROM Users
        WHERE status = 'active'
```

### Example 2: Read directly from a table

```yaml
sources:
  - name: spanner_table
    module: spanner
    parameters:
      projectId: myproject
      instanceId: myinstance
      databaseId: mydatabase
      table: Users
```

### Example 3: Read from a table with field selection

Read only specific columns.

```yaml
sources:
  - name: spanner_partial
    module: spanner
    parameters:
      projectId: myproject
      instanceId: myinstance
      databaseId: mydatabase
      table: Users
      fields:
        - user_id
        - name
        - email
```

### Example 4: Export all tables to per-table parquet files

Read every table except backup tables, and let the storage sink fan out per table using the assembly-time `${input.table}` template. The wildcard input `db.*` binds all per-table outputs of the source.

```yaml
sources:
  - name: db
    module: spanner
    parameters:
      projectId: myproject
      instanceId: myinstance
      databaseId: mydatabase
      tables:
        excludes: ["backup_*"]

sinks:
  - name: export
    module: storage
    inputs: [db.*]
    parameters:
      format: parquet
      output: gs://mybucket/export/${input.table}/data
```

### Example 5: All-tables export with a common per-table query

Add a fixed snapshot column to every table while exporting. `${table}` is replaced with each matched table name at launch.

```yaml
sources:
  - name: db
    module: spanner
    parameters:
      projectId: myproject
      instanceId: myinstance
      databaseId: mydatabase
      tables:
        excludes: ["backup_*"]
        query: "SELECT *, CURRENT_TIMESTAMP() AS exported_at FROM ${table}"

sinks:
  - name: export
    module: storage
    inputs: [db.*]
    parameters:
      format: parquet
      output: gs://mybucket/export/${input.table}/data
```

### Example 6: Read with key range filtering

Read a subset of rows by primary key range.

```yaml
sources:
  - name: spanner_range
    module: spanner
    parameters:
      projectId: myproject
      instanceId: myinstance
      databaseId: mydatabase
      table: Users
      keyRange:
        - startType: closed
          endType: open
          startKeys: "A"
          endKeys: "M"
```

### Example 7: Read with composite key range

Filter by a composite primary key using JSON arrays.

```yaml
sources:
  - name: spanner_composite
    module: spanner
    parameters:
      projectId: myproject
      instanceId: myinstance
      databaseId: mydatabase
      table: OrderItems
      keyRange:
        - startKeys: ["order_001", 1]
          endKeys: ["order_001", 100]
```

### Example 8: Load query from GCS with template variables

```yaml
system:
  args:
    target_date: "2024-01-15"

sources:
  - name: spanner_source
    module: spanner
    parameters:
      projectId: myproject
      instanceId: myinstance
      databaseId: mydatabase
      query: "gs://my-bucket/queries/daily_report.sql"
```

### Example 9: Read with Data Boost and priority

Use Data Boost for independent compute and HIGH priority.

```yaml
sources:
  - name: spanner_boosted
    module: spanner
    parameters:
      projectId: myproject
      instanceId: myinstance
      databaseId: mydatabase
      query: "SELECT * FROM LargeTable"
      enableDataBoost: true
      priority: HIGH
      requestTag: "daily-export-job"
```

### Example 10: Stale read with timestamp bound

Read data as of a specific timestamp.

```yaml
sources:
  - name: spanner_stale
    module: spanner
    parameters:
      projectId: myproject
      instanceId: myinstance
      databaseId: mydatabase
      query: "SELECT * FROM Users"
      timestampBound: "2024-01-15T10:00:00Z"
```

### Example 11: Change stream for CDC

Read change data capture events from a Spanner change stream.

```yaml
sources:
  - name: change_stream
    module: spanner
    parameters:
      projectId: myproject
      instanceId: myinstance
      databaseId: mydatabase
      mode: changeDataCapture
      changeStream:
        changeStreamName: MyChangeStream
        inclusiveStartAt: "2024-01-01T00:00:00Z"
```

### Example 12: View mode for lookup data

Periodically refresh lookup data from Spanner.

```yaml
sources:
  - name: lookup_table
    module: spanner
    parameters:
      projectId: myproject
      instanceId: myinstance
      databaseId: mydatabase
      mode: view
      query: "SELECT category_id, category_name, discount_rate FROM Categories"
      view:
        keyField: category_id
        intervalMinute: 30
```

### Example 13: Spanner to BigQuery pipeline

```yaml
sources:
  - name: spanner_source
    module: spanner
    parameters:
      projectId: myproject
      instanceId: myinstance
      databaseId: mydatabase
      query: >
        SELECT user_id, name, email, status
        FROM Users

sinks:
  - name: bigquery_output
    module: bigquery
    inputs:
      - spanner_source
    parameters:
      table: "myproject.mydataset.users"
```
