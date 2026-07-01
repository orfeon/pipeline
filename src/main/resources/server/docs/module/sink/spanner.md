---
type: Sink Module
title: Spanner Sink Module
description: Writes input data to Google Cloud Spanner tables using mutations. Supports INSERT, UPDATE, INSERT_OR_UPDATE, REPLACE, and DELETE operations. Includes commit timestamp injection, batch tuning parameters, RPC priority control, max commit delay, failure handling with dead-letter output, and emulator support.
tags: [sink, spanner, batch, streaming, gcp, sql]
timestamp: 2026-06-23T00:00:00Z
---

# Spanner Sink Module

Sink Module for writing input data to [Google Cloud Spanner](https://cloud.google.com/spanner/docs) tables. Each input record is converted to a Spanner [Mutation](https://cloud.google.com/spanner/docs/modify-mutation-api) and written to the specified table.

Supports the following mutation operations:

- **INSERT_OR_UPDATE** (default) - Inserts a new row or updates an existing row if the primary key already exists.
- **INSERT** - Inserts a new row. Fails if the primary key already exists.
- **UPDATE** - Updates an existing row. Fails if the primary key does not exist.
- **REPLACE** - Inserts a new row or replaces the entire existing row if the primary key already exists.
- **DELETE** - Deletes a row by primary key. Requires `keyFields` to be specified.

## Sink module common parameters

| parameter  | optional | type                | description                                                           |
|------------|----------|---------------------|-----------------------------------------------------------------------|
| name       | required | String              | Step name. specified to be unique in config file.                     |
| module     | required | String              | Specified `spanner`                                                   |
| inputs     | required | Array<String\>      | Specify the names of the step to be used as input.                    |
| wait       | optional | Array<String\>      | Specify the names of the steps to wait for before processing.        |
| strategy   | optional | [Strategy](../common/strategy.md) | Windowing strategy for streaming execution.               |
| failFast   | optional | Boolean             | If `true`, write failures cause the pipeline to fail immediately. If `false`, failed mutations are reported to the failure output. Default: `true`. |
| parameters | required | Map<String,Object\> | Specify the following individual parameters                          |

## Spanner sink module parameters

### Required parameters

| parameter  | optional | type   | description                                            |
|------------|----------|--------|--------------------------------------------------------|
| projectId  | required | String | GCP Project ID of the Spanner instance.                |
| instanceId | required | String | Spanner instance ID.                                   |
| databaseId | required | String | Spanner database ID.                                   |
| table      | required | String | Spanner table name to write to.                        |

### Write behavior parameters

| parameter             | optional | type           | description                                                                                                                                                                                                                  |
|-----------------------|----------|----------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| mutationOp            | optional | Enum           | Mutation operation type. Values: `INSERT`, `UPDATE`, `INSERT_OR_UPDATE`, `REPLACE`, `DELETE`. Default: `INSERT_OR_UPDATE`.                                                                                                   |
| keyFields             | optional | Array<String\> | Field names that form the primary key. Required when `mutationOp` is `DELETE`. Also used for key construction in write operations.                                                                                            |
| commitTimestampFields | optional | Array<String\> | Field names for which `Value.COMMIT_TIMESTAMP` should be set. Use this for columns with `allow_commit_timestamp = true` option. If the field exists in the input schema, its value is replaced with COMMIT_TIMESTAMP. If the field does not exist in the schema, it is added with COMMIT_TIMESTAMP. |

### Performance tuning parameters

| parameter      | optional | type    | description                                                                                                                                                                                    |
|----------------|----------|---------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| priority       | optional | Enum    | RPC [priority](https://cloud.google.com/spanner/docs/cpu-utilization) for write operations. Values: `HIGH`, `MEDIUM`, `LOW`. Default: `MEDIUM`. See [RPC priority](#rpc-priority).             |
| maxNumRows     | optional | Long    | Maximum number of rows per batch commit. Default: `500`. See [Batch size tuning](#batch-size-tuning).                                                                                          |
| maxNumMutations| optional | Long    | Maximum number of mutated cells per batch commit. Default: `5000`. See [Batch size tuning](#batch-size-tuning).                                                                                |
| batchSizeBytes | optional | Long    | Maximum batch size in bytes. Default: `1048576` (1 MB). See [Batch size tuning](#batch-size-tuning).                                                                                           |
| groupingFactor | optional | Integer | Number of batches to group for sorting by key before writing. Default: `1000`. See [Grouping factor](#grouping-factor).                                                                        |
| maxCommitDelay | optional | Integer | Maximum [commit delay](https://cloud.google.com/spanner/docs/throughput-optimized-writes) in milliseconds for throughput-optimized writes. Must be between `0` and `500`. Default: not set. See [Throughput-optimized writes (maxCommitDelay)](#throughput-optimized-writes-maxcommitdelay). |

### Other parameters

| parameter       | optional | type    | description                                                                                                                                     |
|-----------------|----------|---------|-------------------------------------------------------------------------------------------------------------------------------------------------|
| emulator        | optional | Boolean | If `true`, connects to the Spanner emulator instead of production. Must run with DirectRunner. Default: `false`.                                |
| flattenFailures | optional | Boolean | If `true`, failed mutations are output as flat individual records. If `false`, they are grouped with their attached mutations. Default: `true`.  |

## Mutation operations

### INSERT_OR_UPDATE (default)

Inserts a new row if the primary key does not exist, or updates all specified columns if the primary key already exists. This is the safest default for idempotent writes.

### INSERT

Inserts a new row. The write fails if a row with the same primary key already exists. Use when you need strict insert-only semantics.

### UPDATE

Updates an existing row. The write fails if the primary key does not exist. Use when you need to modify existing rows only.

### REPLACE

Similar to INSERT_OR_UPDATE, but replaces the entire row. Columns not specified in the mutation are set to their default values (or NULL). Use when you want to overwrite the complete row.

### DELETE

Deletes the row matching the primary key. Requires `keyFields` to be specified to identify which input fields form the primary key for deletion.

## Failure handling

When `failFast` is `false`, mutations that fail to write are captured instead of causing the pipeline to fail. The failure output can be accessed by referencing `{sinkName}.failures` as an input to another step.

### Failure output schema (flattenFailures: true)

When `flattenFailures` is `true` (default), each failed mutation is output as a flat record:

| field     | type      | description                                                |
|-----------|-----------|------------------------------------------------------------|
| id        | STRING    | Unique ID generated by UUID.                               |
| timestamp | TIMESTAMP | Timestamp when the failure occurred.                       |
| project   | STRING    | GCP Project ID of the target Spanner instance.             |
| instance  | STRING    | Spanner instance ID.                                       |
| database  | STRING    | Spanner database ID.                                       |
| table     | STRING    | Target table name (nullable).                              |
| op        | STRING    | Mutation operation name (nullable).                        |
| mutation  | JSON      | The failed mutation data converted to JSON.                |

### Failure output schema (flattenFailures: false)

When `flattenFailures` is `false`, failed mutations are grouped:

| field     | type                  | description                                            |
|-----------|-----------------------|--------------------------------------------------------|
| timestamp | TIMESTAMP             | Timestamp when the failure occurred.                   |
| project   | STRING                | GCP Project ID of the target Spanner instance.         |
| instance  | STRING                | Spanner instance ID.                                   |
| database  | STRING                | Spanner database ID.                                   |
| mutations | Array<MutationRecord\>| List of failed mutation records.                       |

Each MutationRecord contains:

| field    | type   | description                                    |
|----------|--------|------------------------------------------------|
| table    | STRING | Target table name (nullable).                  |
| op       | STRING | Mutation operation name (nullable).            |
| mutation | JSON   | The failed mutation data converted to JSON.    |

## Performance tuning guide

### RPC priority

The `priority` parameter controls the [RPC request priority](https://cloud.google.com/blog/topics/developers-practitioners/introducing-request-priorities-cloud-spanner-apis) for write operations. Spanner's scheduler uses priority to allocate resources when there is CPU contention.

| priority | behavior                                                                                                                                           |
|----------|----------------------------------------------------------------------------------------------------------------------------------------------------|
| HIGH     | Write operations are treated as high priority. Best for latency-sensitive, user-facing write paths.                                                |
| MEDIUM   | Default priority. Suitable for standard workloads.                                                                                                 |
| LOW      | Write operations yield CPU to higher-priority tasks. Best for batch loading, analytics, or maintenance workloads that do not have strict latency SLOs. |

**When to use each priority:**

- **HIGH**: Use for streaming pipelines where writes are part of user-facing transaction flows and latency matters.
- **MEDIUM** (default): Use for general batch pipelines where you want reasonable performance without affecting other workloads.
- **LOW**: Use for large-scale bulk loading or backfill jobs. LOW priority writes will proceed at full speed when no higher-priority workloads are contending for resources, but will yield CPU when contention arises.

When resources are not constrained, all priorities perform equally. The difference only becomes visible under CPU contention. Spanner recommends keeping CPU utilization below 65% for regional instances and 45% for multi-region instances.

### Batch size tuning

Mutations are grouped into batches before being committed to Spanner. A batch is flushed (committed) when **any** of the three limits is reached: `maxNumRows`, `maxNumMutations`, or `batchSizeBytes`.

#### maxNumRows

Maximum number of rows per batch commit. Each input record corresponds to one row.

- Default: `500`
- Recommended range: `500` to `2000` for most workloads.
- For small rows (few columns, short values), increasing to `1000`–`2000` improves throughput by reducing the number of commit RPCs.
- For large rows (many columns, large string/bytes values), keep at `500` or lower to stay within the `batchSizeBytes` limit.

#### maxNumMutations

Maximum number of mutated **cells** (not rows) per batch commit. A single row mutation counts as one mutation per affected column, plus primary key columns are always included. For example, inserting a row with 5 columns counts as 5 mutations.

- Default: `5000`
- Spanner enforces a hard limit of [80,000 mutations per commit](https://cloud.google.com/spanner/quotas). Setting this parameter above 80,000 will cause commit failures.
- For tables with many columns, this limit may be reached before `maxNumRows`. For example, a table with 50 columns reaches 5,000 mutations at only 100 rows.
- For tables with few columns (3–5), you can increase this to `10000`–`20000` to allow larger batches.

#### batchSizeBytes

Maximum total byte size per batch commit.

- Default: `1048576` (1 MB)
- For rows with large string/bytes columns, this limit may be reached before `maxNumRows` or `maxNumMutations`.
- Increasing to `2097152` (2 MB) or `5242880` (5 MB) can improve throughput for large-row workloads, as Spanner supports up to 100 MB per transaction.
- Each worker needs enough memory to hold `groupingFactor × batchSizeBytes` bytes of mutations. Increasing `batchSizeBytes` significantly may require reducing `groupingFactor` to avoid out-of-memory errors.

#### Tuning guidelines

| scenario                              | maxNumRows | maxNumMutations | batchSizeBytes | notes                                                    |
|---------------------------------------|------------|-----------------|----------------|----------------------------------------------------------|
| Small rows, few columns (3–5)         | 1000–2000  | 10000–20000     | 1048576 (1 MB) | Increase row/mutation limits to reduce commit RPCs.      |
| Wide tables, many columns (20+)       | 200–500    | 5000            | 1048576 (1 MB) | Keep defaults; mutation count is the binding limit.      |
| Large rows with big string/bytes      | 100–500    | 5000            | 2097152–5242880 (2–5 MB) | Increase byte limit; row size is the binding limit. |
| Maximum throughput bulk loading        | 1000       | 10000           | 2097152 (2 MB) | Combine with `maxCommitDelay: 100` and `priority: LOW`.  |

### Grouping factor

The `groupingFactor` controls how many batches of mutations are collected and sorted by primary key before being written. Sorting by key improves write locality and reduces contention on Spanner splits.

- Default: `1000`
- Higher values improve write ordering and reduce split contention, but increase local memory usage on each worker. Each worker needs memory for `groupingFactor × batchSizeBytes` bytes.
- Lower values reduce memory usage but may result in less optimal write ordering and more cross-split writes.

**Memory impact:** With the default values (`groupingFactor=1000`, `batchSizeBytes=1 MB`), each worker needs up to ~1 GB of memory for mutation buffering. If you increase `batchSizeBytes` to 5 MB, consider reducing `groupingFactor` to `200`–`500` to keep memory usage manageable.

**Streaming pipelines:** Grouping and batching increases latency between receiving a mutation and writing it to the database, because enough mutations need to accumulate to fill the grouped batches. In streaming pipelines, consider reducing `groupingFactor` (e.g. to `100`–`500`) to reduce write latency.

### Throughput-optimized writes (maxCommitDelay)

The `maxCommitDelay` parameter enables Spanner's [throughput-optimized writes](https://cloud.google.com/spanner/docs/throughput-optimized-writes). Spanner introduces a small artificial delay and collects a group of commits that need to be sent to the same voting participants, then executes them together. This amortizes the replication overhead across multiple commits.

- Must be between `0` and `500` milliseconds.
- If not set, Spanner may still apply a small delay internally if it determines it would improve throughput.

**Recommended values:**

| use case                        | maxCommitDelay | notes                                                           |
|---------------------------------|----------------|-----------------------------------------------------------------|
| Latency-sensitive writes        | not set        | Do not set. Let Spanner decide.                                 |
| Moderate throughput improvement | 20–50          | Good starting point for most batch workloads.                   |
| Bulk loading / backfill         | 100            | Recommended by Google for peak write throughput.                |
| Maximum throughput tolerance    | 200–500        | Use only when latency is not a concern at all.                  |

**Important considerations:**
- `maxCommitDelay` adds latency to each individual commit. The throughput benefit comes from reduced per-commit replication overhead.
- Peak write performance using throughput-optimized writes is typically achieved with a delay of **100 ms**.
- This parameter is most effective when combined with large batch sizes and low priority.

### Recommended configurations by use case

#### Batch bulk loading (maximum throughput)

```yaml
parameters:
  priority: LOW
  maxNumRows: 1000
  maxNumMutations: 10000
  batchSizeBytes: 2097152
  groupingFactor: 2000
  maxCommitDelay: 100
```

#### Streaming real-time writes (low latency)

```yaml
parameters:
  priority: HIGH
  maxNumRows: 500
  maxNumMutations: 5000
  batchSizeBytes: 1048576
  groupingFactor: 100
```

#### Batch loading without impacting production traffic

```yaml
parameters:
  priority: LOW
  maxNumRows: 500
  maxNumMutations: 5000
  batchSizeBytes: 1048576
  groupingFactor: 1000
  maxCommitDelay: 100
```

#### Wide tables (many columns)

```yaml
parameters:
  maxNumRows: 200
  maxNumMutations: 5000
  batchSizeBytes: 1048576
  groupingFactor: 1000
```

## Examples

### Example 1: Basic write to Spanner

Write BigQuery data to a Spanner table using default INSERT_OR_UPDATE.

```yaml
sources:
  - name: bigquery_source
    module: bigquery
    parameters:
      query: "SELECT user_id, name, email FROM `myproject.mydataset.users`"

sinks:
  - name: spanner_sink
    module: spanner
    inputs:
      - bigquery_source
    parameters:
      projectId: myproject
      instanceId: myinstance
      databaseId: mydatabase
      table: Users
```

### Example 2: Delete rows from Spanner

Delete rows matching the primary key from a Spanner table.

```yaml
sources:
  - name: keys_to_delete
    module: spanner
    parameters:
      projectId: myproject
      instanceId: myinstance
      databaseId: mydatabase
      query: "SELECT user_id FROM Users WHERE status = 'inactive'"

sinks:
  - name: spanner_delete
    module: spanner
    inputs:
      - keys_to_delete
    parameters:
      projectId: myproject
      instanceId: myinstance
      databaseId: mydatabase
      table: Users
      mutationOp: DELETE
      keyFields:
        - user_id
```

### Example 3: Write with commit timestamp

Automatically set commit timestamp on specified fields.

```yaml
sinks:
  - name: spanner_sink
    module: spanner
    inputs:
      - source
    parameters:
      projectId: myproject
      instanceId: myinstance
      databaseId: mydatabase
      table: Events
      commitTimestampFields:
        - updated_at
```

### Example 4: Write with failure handling

Capture failed mutations instead of failing the pipeline.

```yaml
sinks:
  - name: spanner_sink
    module: spanner
    inputs:
      - source
    failFast: false
    parameters:
      projectId: myproject
      instanceId: myinstance
      databaseId: mydatabase
      table: Users
      mutationOp: INSERT

  - name: failure_output
    module: storage
    inputs:
      - spanner_sink.failures
    parameters:
      output: "gs://my-bucket/failures/"
      format: json
```

### Example 5: High-throughput write with tuning

Configure batch parameters for large-scale writes.

```yaml
sinks:
  - name: spanner_sink
    module: spanner
    inputs:
      - source
    parameters:
      projectId: myproject
      instanceId: myinstance
      databaseId: mydatabase
      table: LargeTable
      priority: LOW
      maxNumRows: 1000
      maxNumMutations: 10000
      batchSizeBytes: 2097152
      groupingFactor: 2000
      maxCommitDelay: 100
```

### Example 6: Write with explicit INSERT operation

Use strict INSERT to ensure no existing rows are overwritten.

```yaml
sinks:
  - name: spanner_sink
    module: spanner
    inputs:
      - source
    parameters:
      projectId: myproject
      instanceId: myinstance
      databaseId: mydatabase
      table: AuditLogs
      mutationOp: INSERT
      keyFields:
        - log_id
```

### Example 7: Ordered writes with wait dependency

Write to parent table first, then child table.

```yaml
sinks:
  - name: parent_sink
    module: spanner
    inputs:
      - parent_source
    parameters:
      projectId: myproject
      instanceId: myinstance
      databaseId: mydatabase
      table: Orders

  - name: child_sink
    module: spanner
    inputs:
      - child_source
    wait:
      - parent_sink
    parameters:
      projectId: myproject
      instanceId: myinstance
      databaseId: mydatabase
      table: OrderItems
```

### Example 8: Avro file to Spanner pipeline

Load Avro files from Cloud Storage into Spanner.

```yaml
sources:
  - name: avro_source
    module: storage
    parameters:
      input: "gs://my-bucket/export/*.avro"
      format: avro

sinks:
  - name: spanner_sink
    module: spanner
    inputs:
      - avro_source
    parameters:
      projectId: myproject
      instanceId: myinstance
      databaseId: mydatabase
      table: Users
```

### Example 9: CSV to Spanner pipeline

Load CSV files into Spanner.

```yaml
sources:
  - name: csv_source
    module: storage
    schema:
      fields:
        - name: ID
          type: string
          mode: required
        - name: NumberField
          type: double
          mode: nullable
        - name: DateField
          type: date
          mode: nullable
    parameters:
      input: "gs://my-bucket/data/*.csv"
      format: csv
      filterPrefix: "\"ID\""

sinks:
  - name: spanner_sink
    module: spanner
    inputs:
      - csv_source
    parameters:
      projectId: myproject
      instanceId: myinstance
      databaseId: mydatabase
      table: MyTable
      keyFields:
        - ID
```

### Example 10: Replace and delete for table synchronization

Synchronize a Spanner table: write new/updated rows and delete removed rows.

```yaml
sources:
  - name: bigquery_source
    module: bigquery
    parameters:
      query: "SELECT * FROM `myproject.mydataset.master_data`"

  - name: spanner_source
    module: spanner
    parameters:
      projectId: myproject
      instanceId: myinstance
      databaseId: mydatabase
      query: "SELECT key_field FROM MasterData"

transforms:
  - name: diff
    module: setoperation
    inputs:
      - spanner_source
      - bigquery_source
    parameters:
      type: except
      keyFields:
        - key_field

sinks:
  - name: spanner_write
    module: spanner
    inputs:
      - bigquery_source
    parameters:
      projectId: myproject
      instanceId: myinstance
      databaseId: mydatabase
      table: MasterData

  - name: spanner_delete
    module: spanner
    inputs:
      - diff
    wait:
      - spanner_write
    parameters:
      projectId: myproject
      instanceId: myinstance
      databaseId: mydatabase
      table: MasterData
      mutationOp: DELETE
      keyFields:
        - key_field
```
