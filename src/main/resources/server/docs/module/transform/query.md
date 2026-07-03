---
type: Transform Module
title: Query Transform Module
description: Runs a Calcite SQL query over each input element inside a DoFn, without any shuffle. Array-of-struct fields can be expanded with UNNEST/LATERAL so aggregation, ORDER BY/LIMIT and set operations run over the per-element collection. One element yields zero or more output rows.
tags: [transform, query, batch, streaming, sql, calcite, unnest, lateral]
timestamp: 2026-07-03T00:00:00Z
---

# Query Transform Module

Transform Module for processing each input element with a SQL query, evaluated in-memory inside the worker (a `DoFn`).

Unlike the [beamsql](beamsql.md) module — which plans SQL over whole PCollections and may introduce shuffles — this module registers **each single element as a one-row table** and evaluates the query per element as a bounded in-memory computation:

- **No shuffle, ever.** The query runs entirely inside the DoFn. Windowing, triggering, and element timestamps of the surrounding pipeline are preserved, so batch and streaming behave identically.
- **Collections live inside the element.** Array-of-struct fields can be expanded with `UNNEST` (optionally with `LATERAL` subqueries), so aggregation (`COUNT`/`SUM`/`AVG`/…, `GROUP BY`, `HAVING`), ordering with `ORDER BY` / `LIMIT`, and set operations (`UNION` / `INTERSECT` / `EXCEPT`) all operate over the per-element collection.
- **Fan-out or fold.** One input element yields zero or more output rows: a query without aggregation fans out (one row per unnested item), an aggregate query folds the element into one row, and a `WHERE` that matches nothing drops the element.

This is a good fit for post-processing enrichment results — for example an upstream `lookup` / `bigtable` / `http` step that attaches an array of fetched records to each element, followed by this module to aggregate or select over that array with SQL.

## Limitations

- The SQL sees exactly **one element per evaluation**. Aggregation across elements is out of scope — use the [aggregation](aggregation.md) or [beamsql](beamsql.md) module for cross-element grouping.
- Every expression in the select list must have an **explicit alias** (`AS name`); auto-generated column names such as `EXPR$0` are rejected at pipeline construction.
- Reserved words used as field names (e.g. `value`) must be quoted with backticks.

## Transform module common parameters

| parameter | optional | type                | description                                                                                                 |
|-----------|----------|---------------------|-------------------------------------------------------------------------------------------------------------|
| name      | required | String              | Step name. specified to be unique in config file.                                                           |
| module    | required | String              | Specified `query`                                                                                           |
| inputs    | required | Array<String\>      | Specify the names of the step from which you want to process the data. Multiple inputs are unioned into a single stream. |
| parameters | required | Map<String,Object\> | Specify the following individual parameters.                                                               |

## Query transform module parameters

| parameter | optional | type   | description                                                                                                                                                                                              |
|-----------|----------|--------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| sql       | required | String | SQL query text. Also accepts a GCS path (`gs://...`), a local file path, a Parameter Manager resource path, or a base64-encoded string (`data:...`), same as the `beamsql` module.                        |
| table     | optional | String | Table name under which the input element is visible in the SQL. Defaults to `INPUT`.                                                                                                                     |

## Examples

### Aggregate over an array field per element

Each element carries an `events` array; the query folds it into one row per element.
Elements whose `events` array is empty produce no output row.

```yaml
transforms:
  - name: eventStats
    module: query
    inputs: [users]
    parameters:
      sql: |
        SELECT
          userId,
          COUNT(*) AS cnt,
          SUM(e.amount) AS total
        FROM
          INPUT, UNNEST(events) AS e
        GROUP BY
          userId
```

### Keep the element even when the array is empty (LEFT JOIN LATERAL)

Postgres-style `LEFT JOIN LATERAL ... ON TRUE` keeps the input row and fills the folded
columns with defaults when the per-element collection is empty.

```yaml
transforms:
  - name: eventStats
    module: query
    inputs: [users]
    parameters:
      sql: |
        SELECT
          t.userId,
          COALESCE(s.total, 0) AS total
        FROM
          INPUT AS t
        LEFT JOIN LATERAL (
          SELECT SUM(e.amount) AS total FROM UNNEST(t.events) AS e
        ) AS s ON TRUE
```

### Fan-out with per-element top-N

`ORDER BY` / `LIMIT` apply within each element's collection — top-N per element without any shuffle.

```yaml
transforms:
  - name: topEvents
    module: query
    inputs: [users]
    parameters:
      table: users
      sql: |
        SELECT
          userId,
          e.category AS category,
          e.amount AS amount
        FROM
          users, UNNEST(events) AS e
        ORDER BY e.amount DESC
        LIMIT 3
```

### Scalar per-element computation and filtering

Without arrays, the module works as a per-element SQL `SELECT`/`WHERE`.

```yaml
transforms:
  - name: scale
    module: query
    inputs: [numbers]
    parameters:
      sql: "SELECT `value` AS id, `value` * 10 AS scaled FROM INPUT WHERE `value` > 1"
```
