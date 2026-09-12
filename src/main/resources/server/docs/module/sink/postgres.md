---
type: Sink Module
title: Postgres Sink Module
description: Writes records to PostgreSQL (or compatible) databases with COPY ... FROM STDIN (FORMAT BINARY) for high throughput. Supports plain INSERT (direct COPY), upsert (INSERT_OR_UPDATE / INSERT_OR_DONOTHING via a temporary staging table and INSERT ... ON CONFLICT), DELETE, row-level ops from an input field (MERGE), table creation / truncation before writing, session settings, and a CDC apply mode that applies unified change records (cdc transform output) with a sequence guard.
tags: [sink, postgres, batch, streaming, database, sql, copy, upsert, merge, cdc]
timestamp: 2026-09-13T00:00:00Z
---

# Postgres Sink Module

Sink module for writing records to PostgreSQL (or PostgreSQL compatible: Cloud SQL, AlloyDB, RDS, …) tables.

Unlike the [`jdbc` sink](jdbc.md), which binds every value of every row into prepared statements, this module transfers rows with `COPY ... FROM STDIN (FORMAT BINARY)` — the PostgreSQL bulk-load protocol — so a batch of rows costs one round trip and no bind parameters (the `jdbc` sink is capped by the driver's 65535 bind-parameter limit per statement). Every value is encoded in the column's binary send format, so all the types the [`postgres` source](../source/postgres.md) reads can be written back, including enums, arrays, `jsonb`, `uuid`, `inet` and `xml`.

Rows are encoded into a worker-memory COPY buffer as they arrive and flushed in **one transaction per batch**:

- `INSERT` copies straight into the destination table.
- `INSERT_OR_UPDATE`, `INSERT_OR_DONOTHING`, `DELETE` and the row-level op modes copy into a session-scoped **temporary staging table** (the staged columns with the destination's types, `ON COMMIT DELETE ROWS`; NOT NULL constraints are not inherited, so a delete can stage keys only) and apply it with a single set-based statement (`INSERT ... ON CONFLICT`, `DELETE ... USING`, or `MERGE`). The COPY and the apply statement commit together; a failure rolls both back.

Use the `jdbc` sink for other databases (MySQL, SQL Server, H2). For PostgreSQL, prefer this module.

## Sink module common parameters

| parameter  | optional | type                | description                                                           |
|------------|----------|---------------------|-----------------------------------------------------------------------|
| name       | required | String              | Step name. specified to be unique in config file.                     |
| module     | required | String              | Specified `postgres`                                                  |
| inputs     | required | Array<String\>      | Specify the names of the step to be used as input.                    |
| waits      | optional | Array<String\>      | Specify the names of the steps to wait for before processing.        |
| strategy   | optional | [Strategy](../common/strategy.md) | Windowing strategy for streaming execution. A transaction never spans windows. |
| failFast   | optional | Boolean             | Whether an encoding failure stops the pipeline (see [Failure handling](#failure-handling)). |
| parameters | required | Map<String,Object\> | Specify the following individual parameters                          |

## Postgres sink module parameters

### Connection parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| url | required | String | JDBC connection url such as `jdbc:postgresql://{host}:{port}/{database}`. See the [postgres source](../source/postgres.md) for Cloud SQL / AlloyDB url examples. |
| user | conditional required | String | Database user. Accepts a secret reference (GCP Secret Manager `projects/{proj}/secrets/{name}/versions/latest`, AWS Secrets Manager, Vault). If omitted, the worker's service account is used as the [Cloud SQL IAM database user](https://cloud.google.com/sql/docs/postgres/iam-logins) (`enableIamAuth=true` is appended to the url). |
| password | conditional required | String | Database password. Accepts a secret reference. Not needed with IAM authentication. |
| settings | optional | Map<String,String\> | Session settings applied with `SET LOCAL` to every write transaction, e.g. `synchronous_commit: "off"` (skips the WAL flush wait per commit) or `work_mem`. Keys must be setting names. |

### Write parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| table | required | String | Destination table, `table` or `schema.table` (a bare name is in the `public` schema; double-quote mixed-case names). Accepts the assembly-time `${input.*}` template with wildcard inputs; in `cdc` mode it may also be an element template on the envelope table (`${table}`). |
| op | optional | Enum | `INSERT` (default), `INSERT_OR_UPDATE`, `INSERT_OR_DONOTHING` or `DELETE`. See [Write operations](#write-operations). Not applicable with `opField` / `cdc`. |
| keyFields | optional | Array<String\> | Key columns of the upsert / delete. **Defaults to the table's primary key.** `INSERT_OR_UPDATE` / `INSERT_OR_DONOTHING` require a unique index whose columns are exactly the key columns (used by `ON CONFLICT`); a missing index is a launch-time error. |
| updateFields | optional | Array<String\> | Columns overwritten on conflict (`INSERT_OR_UPDATE`, `opField`, `cdc`). Default: every written column except the keys. |
| updateCondition | optional | String | `INSERT_OR_UPDATE` only: SQL condition appended as `DO UPDATE ... WHERE`. Refer to the existing row as `target` and the incoming row as `excluded`, e.g. `target.updated_at < excluded.updated_at` to keep the newest version regardless of arrival order. |
| opField | optional | String | Input field holding the per-row operation: `INSERT` / `UPDATE` (upsert) or `DELETE` (`I` / `U` / `D` and lowercase are accepted). The field itself is not written. The batch is applied with `MERGE` (PostgreSQL 15+). See [Row-level operations](#row-level-operations-opfield). |
| batchSize | optional | Integer | Rows per transaction (one COPY + one apply statement). Default: `100000`. `0` writes each bundle in a single transaction. |
| maxBatchBytes | optional | Long | Encoded bytes buffered per worker thread before a flush, whichever of `batchSize` / `maxBatchBytes` comes first. Default: 64 MiB. |
| createTable | optional | Boolean | Run `CREATE TABLE IF NOT EXISTS` before writing, derived from the input schema (`keyFields` become the primary key; records and maps become `jsonb`, timestamps `timestamptz`, decimals `numeric`). Default: `false`. |
| emptyTable | optional | Boolean or String | Empty the table before writing: `true` runs `TRUNCATE` (fast, takes an exclusive lock and is not MVCC-safe for concurrent readers), `"delete"` runs `DELETE FROM` (slower, transactional). Default: `false`. |
| ignoreUnknownFields | optional | Boolean | Skip input fields that have no column in the destination instead of failing at launch. Default: `false`. |
| applyStatement | optional | String | Replaces the generated apply statement for staged operations. FreeMarker template with `${target}`, `${staging}`, `${columns}`, `${keyColumns}` and `${updateColumns}` (quoted identifier lists). Use it for `ON CONFLICT` on a partial index, `MERGE` variants, or custom transformations. With `op: INSERT` the rows are staged instead of copied directly. |

### CDC apply mode parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| cdc | optional | Boolean | Apply unified change records (the [`cdc` transform](../transform/cdc.md) output) to the destination. Default: `false`. See [CDC apply mode](#cdc-apply-mode). |
| sequenceField | optional | String | A `text` / `varchar` column of the destination that stores the change record `sequence`. When set, a change is applied only if it is newer than the stored one, so replays and out-of-order batches converge. An update always refreshes this column, even when `updateFields` omits it. |
| onTruncate | optional | Enum | Reaction to a `TRUNCATE` control record: `skip` (default, logged), `fail` (stop the pipeline) or `apply` (run `TRUNCATE` on the destination after flushing the rows received before it). |

## Write operations

Columns are matched by input field name (exact, then case-insensitive). Column types come from the destination table: the input value is encoded in the column's binary format (a `string` field can feed a `uuid`, `inet`, `jsonb`, enum or `numeric` column, a nested record or map feeds a `json` / `jsonb` column). Generated columns are skipped automatically; columns absent from the input receive their default.

| op | statement | duplicate keys within a batch |
| --- | --- | --- |
| `INSERT` (default) | `COPY` into the destination. Fails on a constraint violation. | both rows inserted (or the unique constraint fails) |
| `INSERT_OR_UPDATE` | `INSERT INTO t SELECT DISTINCT ON (keys) ... ON CONFLICT (keys) DO UPDATE SET ...` | the **last** row wins |
| `INSERT_OR_DONOTHING` | `INSERT INTO t SELECT ... ON CONFLICT (keys) DO NOTHING` | the **first** row wins |
| `DELETE` | `DELETE FROM t USING (SELECT DISTINCT keys FROM staging)`. Only the key columns are staged. | — |
| `opField` / `cdc` | `MERGE INTO t USING (SELECT DISTINCT ON (keys) ...) WHEN MATCHED AND op = 'DELETE' THEN DELETE WHEN MATCHED THEN UPDATE ... WHEN NOT MATCHED THEN INSERT ...` | the last row (or the highest `sequence`) wins |

Notes:

- The upsert statements order the staged rows by key, so concurrent batches from different workers lock rows in the same order and do not deadlock; `ON CONFLICT` is safe under concurrent inserts of the same key.
- `MERGE` requires **PostgreSQL 15 or later** (checked at launch).
- `INSERT` is at-least-once: a retried bundle re-copies rows that were already committed. Use `INSERT_OR_DONOTHING` with a unique key for effectively-once inserts; the upsert, delete and cdc operations are idempotent.
- Batches (of different workers and bundles) commit in no particular order, so across batches the last committed one wins; use `updateCondition` (upsert) or `accumulate` in the `cdc` transform plus `sequenceField` (cdc) when arrival order is not reliable.

### Row-level operations (opField)

With `opField` each input row carries its own operation, for example the output of the `cdc` transform's `accumulate` mode reshaped with `select`, or a diff computed by `compare`. The batch is applied with one `MERGE`: `DELETE` rows delete the matching destination row (a missing row is ignored), any other value inserts or updates it.

```yaml
sinks:
  - name: pg
    module: postgres
    inputs: [diff]
    parameters:
      url: jdbc:postgresql://10.0.0.10:5432/mydb
      user: projects/myproject/secrets/db-user/versions/latest
      password: projects/myproject/secrets/db-password/versions/latest
      table: public.users
      opField: op          # INSERT / UPDATE / DELETE per row
```

## CDC apply mode

With `cdc: true` the sink consumes **unified change records** — the output of the [`cdc` transform](../transform/cdc.md) — and applies them with `MERGE`: `INSERT` / `UPDATE` / `SNAPSHOT` upsert the row from `keys ∪ after`, `DELETE` deletes it by `keys`. `table` may be a template on the envelope table (`${table}`) so one sink replicates a whole change stream; every resolved table must exist and its columns and primary key are read once per table per worker. The changes of every destination table buffered in a batch commit in one transaction.

- **The whole row must be present in `after`.** The apply is set-based, so columns missing from a change record are written as `NULL` (there is no per-row partial update). Spanner sources need `valueCaptureType: NEW_ROW`; PostgreSQL sources should set `REPLICA IDENTITY FULL` on tables with wide TOAST columns (pgoutput omits unchanged TOAST values). Columns of the change record that do not exist in the destination are ignored (logged once).
- **Batches are applied in no particular order** (they come from different workers and bundles), so within a window a `DELETE` may reach the destination before the `INSERT` of the same key. Feed the sink from the `cdc` transform with **`accumulate: true`**: it collapses the changes of each key within the window to the latest one, so every batch holds at most one change per key and the order of batches no longer matters.
- **Convergence across windows and replays**: add a `text` column to the destination and name it in `sequenceField`. The sink stores the change record `sequence` there (zero-padded so text order equals sequence order) and skips changes whose sequence is not newer than the stored one — replays of archived envelopes, retried bundles and out-of-order batches all converge to the latest state. Without it, the last committed batch wins.
- Control records (`TRUNCATE`, `SCHEMA`, …) are counted in the metric `postgres_sink_cdc_control_records`. `TRUNCATE` follows `onTruncate`; with `apply` the sink flushes the rows received before it and then truncates the destination table. `SCHEMA` records are skipped — apply destination DDL out of band (see the cdc transform's `schemaChanges`).
- In streaming, set a windowing `strategy`; a batch never spans windows. Transactional apply (one source transaction = one commit) is not provided; see the [spanner sink](spanner.md#transactional-apply) for that mode.

```yaml
sources:
  - name: pgcdc
    module: postgres
    mode: changeDataCapture
    parameters:
      url: jdbc:postgresql://source:5432/mydb
      user: myuser
      password: mypassword
      cdc: { slot: pipeline_slot, publication: pipeline_pub }

transforms:
  - name: normalize
    module: cdc
    inputs: [pgcdc]
    strategy:
      type: fixed
      unit: second
      size: 10
    parameters:
      format: postgres
      accumulate: true

sinks:
  - name: replica
    module: postgres
    inputs: [normalize]
    strategy:
      type: fixed
      unit: second
      size: 10
    parameters:
      url: jdbc:postgresql://replica:5432/mydb
      user: myuser
      password: mypassword
      table: ${table}
      cdc: true
      sequenceField: _seq
```

## Output

The sink outputs one control record per committed transaction and destination table, usable by `waits` and as `inputs` of actions (e.g. `collect` trigger to sum the rows):

| field | type | description |
| --- | --- | --- |
| table | String | Destination table (`schema.table`). |
| op | String | Applied operation (`INSERT`, `INSERT_OR_UPDATE`, `INSERT_OR_DONOTHING`, `DELETE` or `MERGE`). |
| rows | Long | Rows copied in the transaction. |
| affectedRows | Long | Rows reported by the apply statement (equals `rows` for `INSERT`). |
| startedAt | Timestamp | Transaction start. |
| finishedAt | Timestamp | Commit time. |

Metrics: `postgres_sink_transactions`, `postgres_sink_cdc_control_records`.

## Failure handling

- **Encoding failures** (a value that does not fit the column: an out-of-range integer, an unparsable uuid or timestamp, a null in the key of a delete) are detected before the row reaches the server. The element is routed to `failureSinks` (or the pipeline stops with `failFast: true`); the other rows of the batch are written.
- **Server-side rejections** (constraint violations, type mismatches, lost connections) roll back the whole batch and fail the bundle; the runner retries it. They cannot be attributed to a single element, so `failFast: false` does not route them.

## Performance notes

- Each worker thread holds at most one connection during a flush; the number of concurrent connections is about `workers × threads`. Keep it within the server's `max_connections` (Cloud SQL: `numberOfWorkerHarnessThreads`, `maxNumWorkers`).
- Larger `batchSize` means fewer transactions but longer ones (locks are held until commit). The 100000-row default suits most loads; lower it for wide rows or tables with heavy concurrent access.
- Bulk-load checklist for very large loads: `settings: { synchronous_commit: "off" }`; drop secondary indexes before the load and recreate them afterwards (e.g. an `action` step); load into an `UNLOGGED` table and `SET LOGGED` at the end; raise `maintenance_work_mem` for the index builds.
- Partitioned tables: rows copied into the parent are routed to the partitions by the server.

## Supported column types

`boolean`, `smallint`, `integer`, `bigint`, `real`, `double precision`, `numeric`, `text`, `varchar`, `char`, `bytea`, `date`, `time`, `timetz`, `timestamp`, `timestamptz`, `uuid`, `json`, `jsonb`, `xml`, `inet`, `cidr`, `macaddr`, `macaddr8`, user-defined `enum` types (their text labels), and one-dimensional arrays of these types (`NULL` elements are written). Domain types resolve to their base type. Other column types (PostGIS, range, hstore, composite types, multidimensional arrays) are a launch-time error; cast them in an `applyStatement` from a `text` staging column or use the `jdbc` sink.

Input value conventions: timestamps are written in UTC (`timestamp` without time zone receives the UTC wall time); `timetz` values are written with a zero offset; `numeric` takes the decimal's scale; a nested record / map / array of records is written as JSON to `json` / `jsonb` (or text) columns.

## Examples

### Example 1: Load query results (full refresh)

```yaml
sources:
  - name: ranking
    module: bigquery
    parameters:
      query: "SELECT item_id, score, computed_at FROM `myproject.mydataset.daily_ranking`"

sinks:
  - name: pg
    module: postgres
    inputs: [ranking]
    parameters:
      url: jdbc:postgresql://10.0.0.10:5432/serving
      user: projects/myproject/secrets/db-user/versions/latest
      password: projects/myproject/secrets/db-password/versions/latest
      table: public.daily_ranking
      emptyTable: true
      settings:
        synchronous_commit: "off"
```

### Example 2: Upsert by primary key

```yaml
sinks:
  - name: pg
    module: postgres
    inputs: [users]
    parameters:
      url: jdbc:postgresql://10.0.0.10:5432/mydb
      user: myuser
      password: mypassword
      table: public.users        # keyFields default to the table's primary key
      op: INSERT_OR_UPDATE
      updateCondition: "target.updated_at < excluded.updated_at"
```

### Example 3: Copy every table of a database

The [postgres source `tables` mode](../source/postgres.md#all-tables-parameters) emits one output per table; with a wildcard input the sink is assembled once per table and `${input.*}` names the destination.

```yaml
sources:
  - name: src
    module: postgres
    parameters:
      url: jdbc:postgresql://source:5432/mydb
      user: myuser
      password: mypassword
      tables: ["*"]

sinks:
  - name: dst
    module: postgres
    inputs: [src.*]
    parameters:
      url: jdbc:postgresql://target:5432/mydb
      user: myuser
      password: mypassword
      table: "${input.schema}.${input.name}"
      createTable: true
      emptyTable: true
```

### Example 4: Delete rows by key

```yaml
sinks:
  - name: pg
    module: postgres
    inputs: [expired]
    parameters:
      url: jdbc:postgresql://10.0.0.10:5432/mydb
      user: myuser
      password: mypassword
      table: public.sessions
      op: DELETE
      keyFields: [session_id]
      ignoreUnknownFields: true   # the input carries more than the key
```
