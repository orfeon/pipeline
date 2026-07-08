---
type: Source Module
title: JDBC Source Module
description: Reads records from relational databases via JDBC. Supports SQL query execution (with prepare calls and chained parameter queries) or direct table reads with seek-based splitting for parallel reading by distributed workers.
tags: [source, jdbc, batch, mysql, postgresql, sql]
timestamp: 2026-07-05T00:00:00Z
---

# JDBC Source Module

Source module for reading records from relational databases (MySQL, PostgreSQL, etc.) via JDBC. Supports two read methods:

- **Query read**: Execute an arbitrary SQL query and output the result rows. Optionally run preparation statements (`prepareCalls`) and chained parameterized queries (`prepareParameterQueries`) first.
- **Table read**: Read a whole table. The table is read with seek-based (keyset) pagination over the `seekFields` (primary key by default), and can be split into key ranges that are read in parallel by distributed workers.

Exactly one of `query` or `table` must be specified.

The output schema is automatically inferred from the query or table metadata; no `schema` parameter is needed (for query read you may still supply a `schema` explicitly to override inference).

## Source module common parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| name | required | String | Step name. specified to be unique in config file. |
| module | required | String | Specified `jdbc` |
| schema | optional | - | Usually not required. Schema is automatically inferred from the query or table. |
| timestampAttribute | optional | String | If you want to use the value of a field as the event time, specify the name of the field. |
| parameters | required | Map<String,Object\> | Specify the following individual parameters |

## JDBC source module parameters

### Connection parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| url | required | String | JDBC connection URL, such as `jdbc:mysql://{host}:{port}/{database}` or `jdbc:postgresql://{host}:{port}/{database}`. |
| driver | required | String | JDBC driver class name, such as `com.mysql.cj.jdbc.Driver` or `org.postgresql.Driver`. |
| user | conditional required | String | User name to access the database. Accepts a secret reference: GCP Secret Manager (`projects/{myproj}/secrets/{mysecret}/versions/latest`), AWS Secrets Manager (a secret ARN or `aws-sm://{name}`), or Vault (`vault://v1/{kv-path}#{field}`). If this parameter is not specified, the worker's service account will be used as the database user (IAM database authentication); in that case `enableIamAuth=true` is appended to the `url` automatically if missing. |
| password | conditional required | String | User password to access the database. Required when `user` is specified. Accepts a secret reference: GCP Secret Manager (`projects/{myproj}/secrets/{mysecret}/versions/latest`), AWS Secrets Manager (a secret ARN or `aws-sm://{name}`), or Vault (`vault://v1/{kv-path}#{field}`). No need to specify if the service account is used as `user`. |
| properties | optional | Map<String,String\> | Additional JDBC data source properties passed to the connection pool. |
| transactionIsolation | optional | Integer | Transaction isolation level for the read connection, as a [java.sql.Connection](https://docs.oracle.com/en/java/javase/21/docs/api/java.sql/java/sql/Connection.html) constant value: `0` (NONE), `1` (READ_UNCOMMITTED), `2` (READ_COMMITTED), `4` (REPEATABLE_READ), `8` (SERIALIZABLE). The default is `2` (READ_COMMITTED). Applied to query read. |

### Query read parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| query | selective required | String | SQL query to execute. Either `query` or `table` must be specified (not both). Supports inline SQL or a GCS path (`gs://...`). Supports FreeMarker template variables (e.g. `${args.myVar}` from `system.args` / module `args`). |
| prepareCalls | optional | Array<String\> | SQL statements executed via `CallableStatement` on the same connection before the main query (e.g. session settings or stored procedure calls). The default is empty. |
| prepareParameterQueries | optional | Array<PrepareParameterQuery\> | Queries executed before the main query whose result rows are used as positional prepared-statement parameters (`?` placeholders) of the following query. Queries are chained in order: each stage's output rows become the parameter sets of the next stage, and the last stage feeds the main `query`. This also fans the main query out in parallel, one execution per parameter row. |
| excludeFields | optional | Array<String\> | Field names to exclude from the inferred output schema. Applied only when the schema is inferred from the query. The default is empty. |

#### PrepareParameterQuery parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| query | required | String | SQL query to execute. Its result rows are passed as prepared-statement parameters to the next query. Supports FreeMarker template variables. |
| prepareCalls | optional | Array<String\> | SQL statements executed via `CallableStatement` before this query. The default is empty. |

### Table read parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| table | selective required | String | Table name to read. Either `query` or `table` must be specified (not both). |
| select | optional | String | The text to be inserted into the SELECT clause to specify the columns to be retrieved. The default is `*`. |
| seekFields | optional | Array<String\> | Fields used for seek-based (keyset) pagination and range splitting. If not specified, the table's primary key columns are used. |
| fetchSize | optional | Integer | Number of records fetched per page while seeking through the table. The default is `50000`. |
| enableSplit | optional | Boolean | If `true`, the read range is split by the seek key so that multiple workers read the table in parallel. The default is `false`. |
| enableInitialSplit | optional | Boolean | If `true`, the key range is split into `initialSplitSize` parts before reading starts. The default is `false`. |
| initialSplitSize | optional | Integer | Number of initial splits when `enableInitialSplit` is `true`. The default is `10`. |
| collations | optional | Map<String,String\> | Overrides the character collation assumed for string seek fields when computing split boundaries. Key is the field name (`*` applies to all fields), value is the collation name. By default the collation is detected from the table metadata. |

In table read mode the module emits an additional tagged output named `restriction` containing the processed key-range records (useful for monitoring split progress), and read failures can be routed with the common `failFast` / `outputFailure` / `failureSinks` settings.

## Example config files

### Example 1: Read from MySQL using a query

```yaml
sources:
  - name: jdbcInput
    module: jdbc
    parameters:
      url: jdbc:mysql://localhost:3306/mydatabase
      driver: com.mysql.cj.jdbc.Driver
      user: myuser
      password: projects/myproject/secrets/mysecret/versions/latest
      query: |
        SELECT id, name, price, created_at
        FROM products
        WHERE created_at >= '2024-01-01'

sinks:
  - name: output
    module: storage
    inputs:
      - jdbcInput
    parameters:
      output: gs://mybucket/products/
      format: avro
```

### Example 2: Parallel table read from PostgreSQL

```yaml
sources:
  - name: jdbcTableInput
    module: jdbc
    parameters:
      url: jdbc:postgresql://localhost:5432/mydatabase
      driver: org.postgresql.Driver
      user: myuser
      password: mypassword
      table: public.orders
      select: "id, customer_id, amount, ordered_at"
      seekFields:
        - id
      fetchSize: 50000
      enableSplit: true
      enableInitialSplit: true
      initialSplitSize: 10
```

### Example 3: Query with template variables and prepare calls

```yaml
system:
  args:
    targetDate: "2024-01-15"

sources:
  - name: jdbcInput
    module: jdbc
    parameters:
      url: jdbc:mysql://localhost:3306/mydatabase
      driver: com.mysql.cj.jdbc.Driver
      user: myuser
      password: projects/myproject/secrets/mysecret/versions/latest
      prepareCalls:
        - "SET SESSION net_read_timeout = 600"
      query: |
        SELECT * FROM events WHERE event_date = '${args.targetDate}'
```
