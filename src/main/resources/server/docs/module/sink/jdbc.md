---
type: Sink Module
title: JDBC Sink Module
description: Writes input data to relational databases via JDBC. Supports MySQL, PostgreSQL, SQL Server, and H2 with INSERT, INSERT_OR_UPDATE (upsert), and INSERT_OR_DONOTHING operations, multi-row bulk inserts (bulkInsertSize), batched commits, optional table creation from the input schema, table truncation before writing, and Secret Manager integration for credentials.
tags: [sink, jdbc, mysql, postgresql, sqlserver, h2, batch, sql]
timestamp: 2026-09-12T00:00:00Z
---

# JDBC Sink Module

Sink Module for writing input data to relational databases via [JDBC](https://docs.oracle.com/javase/tutorial/jdbc/basics/index.html). Each input record is converted to a prepared statement and written to the specified table in batched, committed transactions.

The target database type is detected from the `driver` class name. Supported databases:

| database   | driver class name contains | example driver                                 |
|------------|-----------------------------|------------------------------------------------|
| MySQL      | `mysql`                     | `com.mysql.cj.jdbc.Driver`                     |
| PostgreSQL | `postgresql`                | `org.postgresql.Driver`                        |
| SQL Server | `sqlserver`                 | `com.microsoft.sqlserver.jdbc.SQLServerDriver` |
| H2         | `h2`                        | `org.h2.Driver`                                |

Any other driver class name causes the module to fail with a "Not supported JDBC driver" error.

## Sink module common parameters

| parameter  | optional | type                | description                                                           |
|------------|----------|---------------------|-----------------------------------------------------------------------|
| name       | required | String              | Step name. specified to be unique in config file.                     |
| module     | required | String              | Specified `jdbc`                                                      |
| inputs     | required | Array<String\>      | Specify the names of the step to be used as input.                    |
| waits      | optional | Array<String\>      | Specify the names of the steps to wait for before processing.        |
| strategy   | optional | [Strategy](../common/strategy.md) | Windowing strategy for streaming execution.               |
| parameters | required | Map<String,Object\> | Specify the following individual parameters                          |

## JDBC sink module parameters

### Connection parameters

| parameter | optional | type   | description                                                                                                                                                  |
|-----------|----------|--------|----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url       | required | String | JDBC connection URL, e.g. `jdbc:postgresql://<host>:<port>/<database>` or `jdbc:mysql://<host>:<port>/<database>`.                                          |
| driver    | required | String | JDBC driver class name, e.g. `org.postgresql.Driver` or `com.mysql.cj.jdbc.Driver`. Determines the target database type. See [Supported databases](#jdbc-sink-module). |
| user      | required | String | Database user name. Accepts a [secret reference](#credentials-from-a-secret-store).                                                                          |
| password  | required | String | Database password. Accepts a [secret reference](#credentials-from-a-secret-store).                                                                           |

### Write parameters

| parameter   | optional | type           | description                                                                                                                                                                    |
|-------------|----------|----------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| table       | required | String         | Name of the table to write to.                                                                                                                                                 |
| op             | optional | Enum           | Write operation. Values: `INSERT`, `INSERT_OR_UPDATE`, `INSERT_OR_DONOTHING`. Default: `INSERT`. See [Write operations](#write-operations).                                    |
| keyFields      | optional | Array<String\> | Field names that form the primary key. Used as the merge key for `INSERT_OR_UPDATE` / `INSERT_OR_DONOTHING`, and as the `PRIMARY KEY` when `createTable` is `true`.           |
| batchSize      | optional | Integer        | Number of bulk operations (statements) added to the JDBC batch before it is executed and committed. The batch is also flushed at the end of each bundle. Must be >= 1. Default: `1000`. |
| bulkInsertSize | optional | Integer        | Maximum number of records written by a single multi-row statement (`VALUES (...), (...), ...`). Must be >= 1, and `bulkInsertSize * <number of fields>` must not exceed the driver's bind-parameter limit (SQL Server: 2100, MySQL / PostgreSQL: 65535); SQL Server additionally allows at most `1000` rows per statement. Default: `1`. See [Bulk inserts](#bulk-inserts). |
| createTable    | optional | Boolean        | If `true`, executes `CREATE TABLE IF NOT EXISTS` before writing, deriving column definitions from the input schema and using `keyFields` as the primary key. Default: `false`. |
| emptyTable     | optional | Boolean        | If `true`, executes `DELETE FROM <table>` before writing to empty the table. Default: `false`.                                                                                 |

## Write operations

| op                  | behavior                                                                                                          |
|---------------------|--------------------------------------------------------------------------------------------------------------------|
| INSERT (default)    | Plain `INSERT`. Fails if a row with the same primary key already exists.                                          |
| INSERT_OR_UPDATE    | Upsert: inserts a new row, or updates the non-key columns when the key already exists (`ON DUPLICATE KEY UPDATE` for MySQL, `MERGE` for PostgreSQL/H2). Not supported for SQL Server. |
| INSERT_OR_DONOTHING | Inserts a new row, or leaves the existing row unchanged when the key already exists (`ON DUPLICATE KEY UPDATE` on the key columns only for MySQL, `MERGE` for PostgreSQL). Not supported for SQL Server or H2. |

Notes:

- `INSERT_OR_UPDATE` and `INSERT_OR_DONOTHING` require `keyFields` to identify the merge key; an empty `keyFields` fails at pipeline construction.
- SQL Server supports only `INSERT`; H2 supports `INSERT` and `INSERT_OR_UPDATE`. Unsupported combinations fail at pipeline construction (the statement is validated before the pipeline is launched).
- `DELETE` is not supported by this module.

## Bulk inserts

`bulkInsertSize` controls how many records go into one multi-row statement, while `batchSize` controls how many of those statements are added to the JDBC batch before it is executed and committed. For example, with `bulkInsertSize: 100` and `batchSize: 10`, up to 100 records are written per statement and up to 1,000 records are processed before each commit. The records left over at the end of a bundle are written with a shorter statement sized to the remaining count.

Every record binds one placeholder per field, so a multi-row statement carries `bulkInsertSize * <number of fields>` bind parameters. Drivers cap that count (SQL Server: 2100 per request, MySQL and PostgreSQL: 65535 per prepared statement); a `bulkInsertSize` that would exceed the cap for the input schema fails at pipeline construction. For example, a 20-column table on SQL Server allows `bulkInsertSize` up to `105`.

For `INSERT_OR_UPDATE` and `INSERT_OR_DONOTHING`, records that share the same `keyFields` values are consolidated within each multi-row statement so that one statement does not contain two records with the same key: `INSERT_OR_DONOTHING` keeps the first record and `INSERT_OR_UPDATE` keeps the last record. Records whose key contains `null` are never consolidated. `bulkInsertSize` counts records after this consolidation. Plain `INSERT` does not consolidate duplicates.

Consolidation compares the exact field values. On PostgreSQL (`MERGE`), records that the database still treats as the same row — keys that differ only under a case-insensitive collation such as `citext`, `CHAR(n)` trailing-space padding, or a second `UNIQUE` index whose columns are not in `keyFields` — make the whole statement fail with `MERGE command cannot affect row a second time` or a unique-constraint violation. Set `bulkInsertSize: 1` for such tables. MySQL and H2 process the rows of one statement in order and are not affected.

## Table preparation

When `createTable` or `emptyTable` is `true`, the module executes the corresponding DDL/DML once before any records are written, and all writes wait for the preparation to complete:

- `createTable: true` — builds a `CREATE TABLE IF NOT EXISTS` statement from the input schema. Nullable fields become nullable columns, other fields get `NOT NULL`, and `keyFields` become the `PRIMARY KEY`.
- `emptyTable: true` — executes `DELETE FROM <table>` to remove all existing rows.

## Credentials from a secret store

If `user` or `password` is given as a secret reference, the value is resolved at pipeline
construction time. Use this to avoid writing credentials into the config file. Supported forms:

- GCP [Secret Manager](https://cloud.google.com/secret-manager/docs): `projects/<project>/secrets/<secret>/versions/<version>`
- AWS [Secrets Manager](https://docs.aws.amazon.com/secretsmanager/): a full secret ARN (`arn:aws:secretsmanager:...`) or `aws-sm://<name>` (region from `options.aws.region`)
- HashiCorp [Vault](https://developer.hashicorp.com/vault) KV: `vault://v1/<kv-path>#<field>` (connection from the `VAULT_ADDR` / `VAULT_NAMESPACE` environment variables; auth via `VAULT_AUTH` = `token` (`VAULT_TOKEN`), `gcp` (`VAULT_AUTH_SERVICE_ACCOUNT` + `VAULT_ROLE`), or `aws-iam` (`VAULT_ROLE`))

## Examples

### Example 1: Write BigQuery query results to PostgreSQL

```yaml
sources:
  - name: bigquery_source
    module: bigquery
    parameters:
      query: "SELECT user_id, name, email FROM `myproject.mydataset.users`"

sinks:
  - name: jdbc_sink
    module: jdbc
    inputs:
      - bigquery_source
    parameters:
      url: "jdbc:postgresql://10.0.0.10:5432/mydatabase"
      driver: org.postgresql.Driver
      user: "projects/myproject/secrets/db-user/versions/latest"
      password: "projects/myproject/secrets/db-password/versions/latest"
      table: users
```

### Example 2: Upsert into MySQL with table creation

Create the table from the input schema if it does not exist, and upsert rows by primary key.

```yaml
sinks:
  - name: jdbc_sink
    module: jdbc
    inputs:
      - source
    parameters:
      url: "jdbc:mysql://10.0.0.20:3306/mydatabase"
      driver: com.mysql.cj.jdbc.Driver
      user: myuser
      password: mypassword
      table: user_profiles
      op: INSERT_OR_UPDATE
      keyFields:
        - user_id
      createTable: true
      batchSize: 500
```

### Example 3: Multi-row inserts into SQL Server

Write up to 100 records per statement and commit every 100 statements (10,000 records). With SQL Server's 2100-parameter limit, 100 rows per statement leaves room for tables of up to 21 columns.

```yaml
sinks:
  - name: jdbc_sink
    module: jdbc
    inputs:
      - source
    parameters:
      url: "jdbc:sqlserver://10.0.0.30:1433;databaseName=mydatabase"
      driver: com.microsoft.sqlserver.jdbc.SQLServerDriver
      user: "projects/myproject/secrets/db-user/versions/latest"
      password: "projects/myproject/secrets/db-password/versions/latest"
      table: events
      bulkInsertSize: 100
      batchSize: 100
```

### Example 4: Full refresh (empty the table before writing)

```yaml
sinks:
  - name: jdbc_sink
    module: jdbc
    inputs:
      - source
    parameters:
      url: "jdbc:postgresql://10.0.0.10:5432/mydatabase"
      driver: org.postgresql.Driver
      user: "projects/myproject/secrets/db-user/versions/latest"
      password: "projects/myproject/secrets/db-password/versions/latest"
      table: master_data
      emptyTable: true
```
