---
type: Transform Module
title: BeamSQL Transform Module
description: Processes and combines input data using SQL queries based on Apache Beam SQL (Calcite). Supports JOIN across multiple inputs, aggregation with windowing, and built-in UDFs/UDAFs.
tags: [transform, beamsql, batch, streaming, sql, join, aggregation]
timestamp: 2026-06-21T14:30:00Z
---

# BeamSQL Transform Module

Transform Module for processing and combining input data with SQL queries. Based on [Apache Beam SQL](https://beam.apache.org/documentation/dsls/sql/overview/) with the Calcite query planner.

This module accepts one or more inputs referenced by their step name in SQL, and outputs the query result. It supports JOIN, aggregation, windowing functions (`TUMBLE`, `HOP`, `SESSION`), and pattern matching (`MATCH_RECOGNIZE`).

A variety of built-in [UDFs and UDAFs](#built-in-udfs) are also available.

## Limitations

- `INT32` (int) and `FLOAT32` (float) data types are **not supported** as input. Use `INT64` (long) and `FLOAT64` (double) instead.
- `ENUM` typed fields are automatically converted to `STRING` before SQL processing.

## Transform module common parameters

| parameter | optional | type                | description                                                                                                 |
|-----------|----------|---------------------|-------------------------------------------------------------------------------------------------------------|
| name      | required | String              | Step name. specified to be unique in config file.                                                           |
| module    | required | String              | Specified `beamsql`                                                                                         |
| inputs    | required | Array<String\>      | Specify the names of the step from which you want to process the data, including the name of the transform. |
| strategy  | optional | [Strategy](../common/strategy.md) | Windowing strategy for streaming execution. Specify when you need to set a trigger.                |
| parameters | required | Map<String,Object\> | Specify the following individual parameters.                                                               |

## BeamSQL transform module parameters

| parameter            | optional | type                | description                                                                                                                                                                                                                                      |
|----------------------|----------|---------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| sql                  | required | String              | SQL query text. Input steps can be referenced by their step name using backtick-quoted identifiers (e.g. `` `myInput` ``). Also accepts a GCS path (`gs://...`), a local file path, a Parameter Manager resource path, or a base64-encoded string (`data:...`). |
| ddl                  | optional | String              | DDL statement(s) to register additional tables or views. Accepts the same loading options as `sql` (GCS, file, Parameter Manager, base64).                                                                                                       |
| namedParameters      | optional | Map<String,String\> | Named parameters for the SQL query. Cannot be used together with `positionalParameters`.                                                                                                                                                         |
| positionalParameters | optional | Array<String\>      | Positional parameters for the SQL query. Cannot be used together with `namedParameters`.                                                                                                                                                         |
| autoLoading          | optional | Boolean             | Enable auto-loading of Beam SQL table providers.                                                                                                                                                                                                 |
| sources              | optional | Array<Source\>      | External lookup sources (jdbc / spanner / bigtable / rest). Each source's tables are referenced as `sourceName.tableName` and joined as inline lookups — see below. Same per-type parameters as the [query](query.md) module.                     |

### SQL loading

The `sql` (and `ddl`) parameter supports multiple sources:

| format                         | example                                           | description                                |
|--------------------------------|---------------------------------------------------|--------------------------------------------|
| Inline SQL                     | `"SELECT * FROM \`input\`"`                       | SQL text specified directly in config.     |
| GCS path                       | `"gs://my-bucket/queries/transform.sql"`          | Reads SQL from a Cloud Storage file. Supports FreeMarker template variables. |
| Local file path                | `"/path/to/query.sql"`                            | Reads SQL from a local file. Supports FreeMarker template variables. |
| Parameter Manager resource     | `"projects/p/locations/l/parameters/n/versions/v"` | Reads SQL from Google Cloud Parameter Manager. |
| Base64 encoded                 | `"data:U0VMRUNUICogRlJPTSBpbnB1dA=="`            | Decodes base64-encoded SQL text.           |

### Referencing inputs in SQL

Each input step is available as a table in the SQL query, referenced by its step name. Use backtick-quoted identifiers when the name contains special characters or is a reserved word.

```sql
SELECT a.field1, b.field2
FROM `inputA` AS a
JOIN `inputB` AS b ON a.key = b.key
```

## External lookup sources

With `sources`, tables in external systems are registered through Beam SQL's native
seekable-table mechanism: a join against one is planned as an **inline lookup** — for
each input row the matching rows are fetched from the external system (never a scan
of the external table; a standalone `FROM sourceName.tableName` fails). The
configuration is identical to the [query](query.md) module's `sources` (jdbc /
spanner incl. parameterized query tables / bigtable / rest).

Constraints (differences from the `query` module's lookup-join):

- **Point equi-joins only** — the join columns must together form one of the table's
  keys (primary key / unique index / row key / request parameters); key-prefix and
  bounded-range fetches are not available here.
- Write each equality with the **main-input column on the left**:
  `ON c.itemId = m.ITEM_ID` (the reverse order fails inside Beam's join planning).
- One backend request per input row — there is no batching or key deduplication.
  The source-level `cache` block (see the [query](query.md) module's "Lookup cache")
  applies here too and absorbs repeated keys; for high-volume enrichment still
  prefer the `query` module (512-row batched fetches).
- `DATE` / `TIME` columns of a lookup table surface as ISO-8601 **strings** (a Beam
  limitation on logical types in the seekable-join output); `CAST` them in SQL when
  the temporal type is needed. `TIMESTAMP` columns pass through natively.
- Lookups must be read-only and idempotent; the launcher needs connectivity to the
  sources at pipeline construction (table metadata is derived once and shipped to
  workers).

```yaml
transforms:
  - name: enrich
    module: beamsql
    inputs: [carts]
    parameters:
      sql: |
        SELECT c.itemId, m.TITLE AS title, c.qty * m.PRICE AS total
        FROM carts AS c
        JOIN db.ITEMS AS m ON c.itemId = m.ITEM_ID
      sources:
        - name: db
          type: jdbc
          driver: org.postgresql.Driver
          url: jdbc:postgresql://localhost:5432/shop
          user: app
          password: secret
          tables:
            - name: ITEMS
```

## Built-in UDFs

The following User-Defined Functions are registered automatically and can be used in SQL queries.

### Scalar functions (UDFs)

| function                                    | description                                                              |
|---------------------------------------------|--------------------------------------------------------------------------|
| `MDT_GREATEST_INT64(value1, value2)`        | Returns the greater of two INT64 values. NULL-safe (returns non-null).   |
| `MDT_GREATEST_FLOAT64(value1, value2)`      | Returns the greater of two FLOAT64 values. NULL-safe (returns non-null). |
| `MDT_LEAST_INT64(value1, value2)`           | Returns the lesser of two INT64 values. NULL-safe (returns non-null).    |
| `MDT_LEAST_FLOAT64(value1, value2)`         | Returns the lesser of two FLOAT64 values. NULL-safe (returns non-null).  |
| `MDT_GENERATE_UUID()`                       | Generates a random UUID string.                                          |
| `MDT_CONTAINS_ALL_INT64(array1, array2)`    | Returns true if `array1` contains all elements of `array2`.              |
| `MDT_CONTAINS_ALL_STRING(array1, array2)`   | Returns true if `array1` contains all elements of `array2`.              |

### Aggregate functions (UDAFs)

| function                                    | description                                                              |
|---------------------------------------------|--------------------------------------------------------------------------|
| `MDT_ARRAY_AGG_INT64(value)`                | Aggregates INT64 values into an array. Ignores NULL.                     |
| `MDT_ARRAY_AGG_STRING(value)`               | Aggregates STRING values into an array. Ignores NULL.                    |
| `MDT_ARRAY_AGG_DISTINCT_STRING(value)`      | Aggregates distinct STRING values into an array. Ignores NULL.           |
| `MDT_ARRAY_AGG_DISTINCT_FLOAT64(value)`     | Aggregates distinct FLOAT64 values into an array. Ignores NULL.          |
| `MDT_ARRAY_AGG_DISTINCT_INT64(value)`       | Aggregates distinct INT64 values into an array. Ignores NULL.            |
| `MDT_COUNT_DISTINCT_STRING(value)`          | Counts distinct STRING values. Ignores NULL.                             |
| `MDT_COUNT_DISTINCT_FLOAT64(value)`         | Counts distinct FLOAT64 values. Ignores NULL.                            |
| `MDT_COUNT_DISTINCT_INT64(value)`           | Counts distinct INT64 values. Ignores NULL.                              |

## Examples

### Example 1: Simple query with filter

```yaml
sources:
  - name: orders
    module: create
    parameters:
      type: element
      elements:
        - user_id: "u001"
          amount: 1500
          status: "completed"
        - user_id: "u002"
          amount: 200
          status: "cancelled"
        - user_id: "u003"
          amount: 3000
          status: "completed"
    schema:
      fields:
        - name: user_id
          type: string
        - name: amount
          type: int64
        - name: status
          type: string

transforms:
  - name: high_value_orders
    module: beamsql
    inputs:
      - orders
    parameters:
      sql: >
        SELECT user_id, amount
        FROM `orders`
        WHERE status = 'completed' AND amount > 1000
```

### Example 2: JOIN across multiple inputs

Combine data from BigQuery and Spanner using SQL JOIN.

```yaml
sources:
  - name: BigQueryInput
    module: bigquery
    parameters:
      query: "SELECT user_id, name FROM `myproject.mydataset.users`"
  - name: SpannerInput
    module: spanner
    parameters:
      projectId: myproject
      instanceId: myinstance
      databaseId: mydatabase
      table: orders
      fields:
        - user_id
        - total_amount

transforms:
  - name: joined
    module: beamsql
    inputs:
      - BigQueryInput
      - SpannerInput
    parameters:
      sql: >
        SELECT
          b.user_id,
          b.name,
          COALESCE(s.total_amount, 0) AS total_amount
        FROM `BigQueryInput` AS b
        LEFT JOIN `SpannerInput` AS s
          ON b.user_id = s.user_id

sinks:
  - name: output
    module: spanner
    inputs:
      - joined
    parameters:
      projectId: myproject
      instanceId: myinstance
      databaseId: mydatabase
      table: user_summary
```

### Example 3: Streaming aggregation with windowed GROUP BY

Aggregate PubSub messages within 10-second tumbling windows.

```yaml
sources:
  - name: events
    module: pubsub
    parameters:
      subscription: projects/myproject/subscriptions/mysubscription
      format: json
    schema:
      fields:
        - name: user_id
          type: string
        - name: count
          type: int64
        - name: event_time
          type: timestamp
    timestampAttribute: event_time

transforms:
  - name: windowed_counts
    module: beamsql
    inputs:
      - events
    parameters:
      sql: >
        SELECT
          user_id,
          COUNT(*) AS event_count,
          SUM(count) AS total_count,
          MIN(event_time) AS window_start,
          MAX(event_time) AS window_end
        FROM `events`
        GROUP BY
          user_id,
          TUMBLE(event_time, INTERVAL '10' SECOND)
        HAVING SUM(count) > 1
```

### Example 4: Using built-in UDFs

```yaml
transforms:
  - name: with_udfs
    module: beamsql
    inputs:
      - input
    parameters:
      sql: >
        SELECT
          MDT_GENERATE_UUID() AS id,
          name,
          MDT_GREATEST_INT64(score_a, score_b) AS max_score,
          MDT_LEAST_FLOAT64(rate_a, rate_b) AS min_rate
        FROM `input`
```

### Example 5: Using aggregate UDAFs with GROUP BY

```yaml
transforms:
  - name: aggregated
    module: beamsql
    inputs:
      - input
    parameters:
      sql: >
        SELECT
          group_key,
          MDT_ARRAY_AGG_STRING(name) AS names,
          MDT_ARRAY_AGG_DISTINCT_STRING(category) AS unique_categories,
          MDT_COUNT_DISTINCT_INT64(item_id) AS unique_item_count
        FROM `input`
        GROUP BY group_key
```

### Example 6: Loading SQL from GCS

```yaml
transforms:
  - name: processed
    module: beamsql
    inputs:
      - input
    parameters:
      sql: "gs://my-bucket/queries/transform.sql"
```

### Example 7: MATCH_RECOGNIZE for pattern matching

Detect sequential patterns in ordered data.

```yaml
sources:
  - name: events
    module: create
    parameters:
      type: element
      elements:
        - user_id: "a"
          amount: 100
          category: "A"
          timestamp: "2025-01-01T00:00:01Z"
        - user_id: "a"
          amount: 200
          category: "B"
          timestamp: "2025-01-01T00:00:02Z"
        - user_id: "a"
          amount: 300
          category: "C"
          timestamp: "2025-01-01T00:00:03Z"
    schema:
      fields:
        - name: user_id
          type: string
        - name: amount
          type: int64
        - name: category
          type: string
        - name: timestamp
          type: timestamp
    timestampAttribute: timestamp

transforms:
  - name: patterns
    module: beamsql
    inputs:
      - events
    parameters:
      sql: >
        SELECT *
        FROM `events`
        MATCH_RECOGNIZE(
          PARTITION BY user_id
          ORDER BY `timestamp`
          MEASURES
            A.category AS first_category,
            B.category AS second_category,
            A.amount AS first_amount,
            B.amount AS second_amount
          PATTERN (A B)
          DEFINE
            B AS B.category = 'B'
        ) AS M
```
