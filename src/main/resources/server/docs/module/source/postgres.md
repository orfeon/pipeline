---
type: Source Module
title: Postgres Source Module
description: Reads records from PostgreSQL (or compatible) databases in parallel using COPY BINARY format. Each table is split into physical block (ctid) ranges read concurrently by distributed workers via TID range scans, giving higher throughput than the generic jdbc source. Reads a single table, or every table matching include/exclude patterns with one tagged output per table. Also supports change data capture streaming via native logical replication (pgoutput).
tags: [source, postgres, batch, streaming, database, sql, copy, cdc, changedatacapture, replication]
timestamp: 2026-08-17T00:00:00Z
---

# Postgres Source Module

Source module for loading records from PostgreSQL (or PostgreSQL compatible) databases.

Unlike the `jdbc` source module, this module transfers data in `COPY (SELECT ...) TO STDOUT (FORMAT BINARY)` format using PostgreSQL CopyManager API for higher throughput.
The table is automatically split into physical block (`ctid`) ranges, and the ranges are read in parallel by distributed workers.
A single source can also read multiple tables at once with the `tables` parameter, producing one tagged output per table (see [All-tables parameters](#all-tables-parameters)).
With `mode: changeDataCapture` the module instead streams row changes from a logical replication slot using the built-in `pgoutput` plugin (see [Change data capture](#change-data-capture-mode-changedatacapture)).
The number of blocks is obtained from `pg_relation_size` (the physical size of the table) and the block range is split mechanically, so no full scan or `OFFSET` is needed to plan the split.
Each range is read with an efficient TID range scan (`WHERE ctid >= '(start,0)' AND ctid < '(end,0)'`) so that a single query does not become huge.

> Note: TID range scans are supported in PostgreSQL 14 and later. On older versions each range may fall back to a sequential scan.
> Because `ctid` is the physical row location, rows that are inserted, updated (moved to another page), or vacuumed *while the read is running* may be read more than once or missed. Use against a table that is not being modified concurrently (or accept the snapshot skew typical of batch reads).

## Source module common parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| name | required | String | Step name. specified to be unique in config file. |
| module | required | String | Specified `postgres` |
| parameters | required | Map<String,Object\> | Specify the following individual parameters |

## Postgres source module parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| url | required | String | JDBC connection url such as `jdbc:postgresql://{host}:{port}/{database}`. |
| user | conditional required | String | User name to access the database. Accepts a secret reference: GCP Secret Manager (`projects/{myproj}/secrets/{mysecret}/versions/latest`), AWS Secrets Manager (a secret ARN or `aws-sm://{name}`), or Vault (`vault://v1/{kv-path}#{field}`). If this parameter is not specified, the worker's service account will be used as the [database user](https://cloud.google.com/sql/docs/postgres/iam-logins). In that case, specify `enableIamAuth=true` as a parameter in the `url`. |
| password | conditional required | String | User password to access the database. Accepts a secret reference: GCP Secret Manager (`projects/{myproj}/secrets/{mysecret}/versions/latest`), AWS Secrets Manager (a secret ARN or `aws-sm://{name}`), or Vault (`vault://v1/{kv-path}#{field}`). No need to specify if the service account will be used as `user`. |
| table | selective required | String | Table name for reading data. Either `table` or `tables` must be specified. |
| tables | selective required | Object or Array | Read every base table matching name patterns instead of a single `table`, with one tagged output per table. See [All-tables parameters](#all-tables-parameters). |
| select | optional | String | The text to be inserted into the SELECT clause to specify the columns to be retrieved. The default is `*`. (`table` mode only; use `tables.select` in `tables` mode) |
| where | optional | String | The condition text to be inserted into the WHERE clause to filter records. (`table` mode only; use `tables.where` in `tables` mode) |
| splitSize | optional | Integer | The approximate number of records in one `ctid` range. The block count per range is derived from this and the estimated row density (`pg_class.reltuples` / block count). The default is 1000000. Run `ANALYZE` on the table beforehand for a more accurate split; when statistics are unavailable a conservative default density is used. |

### All-tables parameters

The `tables` parameter reads multiple tables with a single source module. Either an object with the following fields, or an array shorthand for `includes` (`tables: ["users", "item_*"]`). `*` in a pattern matches any character sequence.

| parameter | optional | type | description |
| --- | --- | --- | --- |
| tables.includes | optional | Array<String\> | Patterns of tables to read. A pattern without a dot matches table names in the `public` schema only; a pattern with a dot (e.g. `myschema.*`) matches the schema-qualified `schema.table` name. Default: `["*"]` (all `public` tables). |
| tables.excludes | optional | Array<String\> | Patterns of tables to exclude (same matching rule). Default: `[]`. |
| tables.select | optional | String | Common SELECT clause template applied to every matched table. `${table}` (the output tag), `${schema}` and `${name}` are available as template variables, in addition to module-level `args`. Default: `*`. |
| tables.where | optional | String | Common WHERE clause template applied to every matched table (same template variables). Default: none. |

Behavior:

- The table list, each table's schema, and each table's `ctid` split are resolved **at pipeline launch** (from `pg_class`; regular tables and partitioned-table parents in user schemas — leaf partitions are read via their parent). Matching no table is a launch-time error.
- The module outputs one collection per table, named `<moduleName>.<tableName>` (tables outside the `public` schema are qualified as `<moduleName>.<schema>.<table>`). Downstream modules can consume a single table (`inputs: [myPostgres.users]`) or all of them with a wildcard (`inputs: [myPostgres.*]`).
- Each output carries assembly-time attributes (`table`, `schema`, `name`) usable in downstream sinks via the `${input.*}` template (see the config README).
- Every table is read in parallel over its own `ctid` ranges, exactly like the single-`table` mode. A partitioned-table parent has no physical storage of its own, so it falls back to a single unsplit `COPY` covering all partitions.
- **No cross-table (or even cross-range) snapshot consistency**: unlike the spanner source's `tables` mode there is no shared read transaction — each `ctid` range is read on its own connection. Run against a quiescent database (or accept the skew typical of batch reads).

### Change data capture (mode: changeDataCapture)

With module-level `mode: changeDataCapture` the source streams row changes (INSERT / UPDATE /
DELETE / TRUNCATE) from a **logical replication slot** using the PostgreSQL built-in `pgoutput`
plugin — no Debezium or server extension required. Tuple values are transferred in the pgoutput
**binary** mode (the same wire representation as COPY BINARY), decoded directly by the module.

The output is the provider-native change record schema below; connect it to the
[`cdc` transform](../transform/cdc.md) with `format: postgres` to normalize into the unified change
record envelope (for the `bigquery` sink CDC apply mode, archiving, etc.).

| parameter | optional | type | description |
| --- | --- | --- | --- |
| cdc.slot | required | String | Logical replication slot name to read. The slot's confirmed position advances as the pipeline checkpoints, letting the server recycle WAL. |
| cdc.publication | required | String | Publication that selects the captured tables (`CREATE PUBLICATION mypub FOR TABLE ...` / `FOR ALL TABLES`). A comma-separated list is passed through to the server. |
| cdc.createSlot | optional | Boolean | Create the slot (plugin `pgoutput`) at pipeline launch when it does not exist. Default: `false` (a missing slot is a launch-time error). |
| cdc.statusIntervalSeconds | optional | Integer | Interval of standby status updates (keepalive) sent to the server. Default: `10`. |
| cdc.maxNumRecords | optional | Integer | Stop after this many change records (turns the read into a bounded drain; mainly for tests and one-shot catch-up runs). |
| cdc.maxReadTimeSeconds | optional | Integer | Stop after this read duration (with `maxNumRecords`, whichever comes first). |

`table`, `tables`, `select` and `where` are not applicable in this mode.

#### Output schema (provider-native change record)

| field | type | description |
| --- | --- | --- |
| lsn | Long | WAL position (LSN) of this change message. |
| commitLsn | Long | Commit LSN of the enclosing transaction. |
| commitTimestamp | Timestamp | Commit timestamp of the enclosing transaction. |
| transactionId | Long | Transaction id (xid). |
| sequence | Long | Change index within the transaction. |
| database | String | Database name. |
| schema | String | Schema name of the changed table. |
| table | String | Name of the changed table. |
| op | String | `INSERT`, `UPDATE`, `DELETE` or `TRUNCATE`. |
| keysJson | JSON | Primary key column values as a JSON object (`{}` for TRUNCATE). The key columns are resolved from the catalog at pipeline launch, so they stay the primary key even under `REPLICA IDENTITY FULL`; tables unknown at launch (e.g. created later under a `FOR ALL TABLES` publication) fall back to the replica-identity columns. |
| oldValuesJson | JSON | Before-image values (see replica identity below); null when not captured. |
| newValuesJson | JSON | After-image values; null for DELETE / TRUNCATE. |

#### Server requirements

Verified at pipeline launch with clear errors:

- PostgreSQL **14 or later** (the pgoutput `binary` option).
- `wal_level = logical` — Cloud SQL: flag `cloudsql.logical_decoding=on`; AlloyDB: `alloydb.logical_decoding=on`; RDS: parameter `rds.logical_replication=1`.
- The user needs the `REPLICATION` privilege (on Cloud SQL, role `cloudsqlreplica` / `REPLICATION` attribute).
- The publication must exist; the slot must exist or `cdc.createSlot: true`.

#### Behavior and caveats

- A logical replication slot is a **single-consumer** stream: the read itself is one connection (the module redistributes decoded records across workers right after). For horizontal scale, split tables across multiple publications/slots and run one source module per slot.
- Delivery is **at-least-once**: the slot position is confirmed on pipeline checkpoints; after a crash/restart the server resends every transaction committing after the confirmed position. Downstream apply consumes the envelope `sequence` (commit LSN + in-transaction index), so replays converge (e.g. BigQuery CDC apply mode `_CHANGE_SEQUENCE_NUMBER` semantics).
- `oldValuesJson` carries the full before-image only for tables with `ALTER TABLE ... REPLICA IDENTITY FULL`; with the default replica identity, UPDATE carries no before-image (old key values only when the key changed) and DELETE carries key values only.
- Large TOASTed values that did not change in an UPDATE are **absent** from `newValuesJson` (not null) — pgoutput does not resend them. Apply sinks that overwrite whole rows will null such columns; use `REPLICA IDENTITY FULL` or exclude wide TOAST columns when this matters.
- While the pipeline is stopped, the server retains WAL from the confirmed slot position: set `max_slot_wal_keep_size` as a safety bound and drop the slot when decommissioning the pipeline.
- Schema changes (`ADD COLUMN`, ...) are picked up automatically from the stream's relation metadata; new columns appear inside the JSON values (see the cdc transform's schema evolution notes).

* url examples
    * PostgreSQL for Cloud SQL
        * `jdbc:postgresql://google/mydatabase?cloudSqlInstance=myproject:us-central1:myinstance&socketFactory=com.google.cloud.sql.postgres.SocketFactory`
    * PostgreSQL for AlloyDB
        * `jdbc:postgresql:///mydatabase?alloydbInstanceName=projects/myproject/locations/us-central1/clusters/mycluster/instances/myinstance-primary&socketFactory=com.google.cloud.alloydb.SocketFactory`

## Supported column types

`boolean`, `smallint`, `integer`, `bigint`, `real`, `double precision`, `numeric`, `text`, `varchar`, `char`, `bytea`, `date`, `time`, `timetz`, `timestamp`, `timestamptz`, `uuid`, `json`, `jsonb`, `xml`, `inet`, `cidr`, `macaddr`, `macaddr8`, user-defined `enum` types, and one-dimensional arrays of these types.

* `enum` values are read as their text labels (string).
* Domain types are resolved to their base type.
* `timetz` values are normalized to UTC time-of-day.
* `inet`/`cidr` values always include the netmask suffix (e.g. `192.168.0.1/32`), matching the PostgreSQL text output.
* Array columns are read as Avro arrays. Multidimensional arrays are not supported, and `NULL` elements inside an array are skipped.

## Example config file

```yaml
sources:
  - name: postgresInput
    module: postgres
    parameters:
      url: jdbc:postgresql://localhost:5432/mydatabase
      user: myuser
      password: projects/myproject/secrets/mysecret/versions/latest
      table: public.mytable
      select: "id,name,created_at"
      where: "created_at >= '2024-01-01'"
      splitSize: 1000000
```

## Example config file (all-tables mode)

Reads every `public` table except `tmp_*`, filtered by a common WHERE clause template, and fans out to per-table GCS paths via a wildcard input and the `${input.*}` template:

```yaml
sources:
  - name: postgresAll
    module: postgres
    parameters:
      url: jdbc:postgresql://localhost:5432/mydatabase
      user: myuser
      password: projects/myproject/secrets/mysecret/versions/latest
      tables:
        includes: ["*"]
        excludes: ["tmp_*"]
        where: "updated_at >= '2024-01-01'"

sinks:
  - name: storage
    module: storage
    inputs: [postgresAll.*]
    parameters:
      output: gs://mybucket/export/${input.table}/data
      format: parquet
```

## Example config file (change data capture)

Streams changes from a logical replication slot and applies them to BigQuery via the unified
envelope (`cdc` transform + `bigquery` sink CDC apply mode). Run in streaming mode.

```yaml
sources:
  - name: postgresCdc
    module: postgres
    mode: changeDataCapture
    parameters:
      url: jdbc:postgresql://localhost:5432/mydatabase
      user: myuser
      password: projects/myproject/secrets/mysecret/versions/latest
      cdc:
        slot: pipeline_slot
        publication: pipeline_pub
        createSlot: true

transforms:
  - name: normalize
    module: cdc
    inputs: [postgresCdc]
    parameters:
      format: postgres

sinks:
  - name: bq
    module: bigquery
    inputs: [normalize]
    parameters:
      table: myproject.mydataset.${table}
      cdc: true
```
