---
type: Source Module
title: Postgres Source Module
description: Reads records from PostgreSQL (or compatible) databases in parallel using COPY BINARY format. Each table is split into physical block (ctid) ranges read concurrently by distributed workers via TID range scans, giving higher throughput than the generic jdbc source. Reads a single table, or every table matching include/exclude patterns with one tagged output per table.
tags: [source, postgres, batch, database, sql, copy]
timestamp: 2026-08-16T00:00:00Z
---

# Postgres Source Module

Source module for loading records from PostgreSQL (or PostgreSQL compatible) databases.

Unlike the `jdbc` source module, this module transfers data in `COPY (SELECT ...) TO STDOUT (FORMAT BINARY)` format using PostgreSQL CopyManager API for higher throughput.
The table is automatically split into physical block (`ctid`) ranges, and the ranges are read in parallel by distributed workers.
A single source can also read multiple tables at once with the `tables` parameter, producing one tagged output per table (see [All-tables parameters](#all-tables-parameters)).
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
