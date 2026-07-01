---
type: Source Module
title: Firestore Source Module
description: Reads documents from Google Cloud Firestore. Supports collection queries with field filters, field projection, ordering, parallel partition queries, and subcollection traversal via allDescendants.
tags: [source, firestore, batch, gcp, nosql, document]
timestamp: 2026-06-22T00:00:00Z
---

# Firestore Source Module

Source Module for reading documents from [Google Cloud Firestore](https://cloud.google.com/firestore/docs). Supports two read modes:

- **List mode** (no `filter`): Lists all documents in a collection using `ListDocuments` API.
- **Query mode** (with `filter`): Runs a structured query with field filters, ordering, and optional parallel partition execution.

A `schema` must be defined at the source level to describe the expected document fields.

## Source module common parameters

| parameter          | optional | type                | description                                                                                                                  |
|--------------------|----------|---------------------|------------------------------------------------------------------------------------------------------------------------------|
| name               | required | String              | Step name. specified to be unique in config file.                                                                            |
| module             | required | String              | Specified `firestore`                                                                                                        |
| schema             | required | [Schema](SCHEMA.md) | Schema of the data to be read.                                                                                               |
| timestampAttribute | optional | String              | If you want to use the value of a field as the event time, specify the name of the field. (The field must be Timestamp type) |
| parameters         | required | Map<String,Object\> | Specify the following individual parameters                                                                                  |

## Firestore source module parameters

| parameter      | optional | type           | description                                                                                                                                                                                                                                          |
|----------------|----------|----------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| projectId      | optional | String         | GCP Project ID. If not specified, the pipeline execution environment's project ID is used.                                                                                                                                                            |
| databaseId     | optional | String         | Firestore database ID. Default: `(default)`.                                                                                                                                                                                                          |
| collection     | required | String         | Collection name to read documents from.                                                                                                                                                                                                               |
| filter         | optional | String         | Filter condition string. When specified, the module uses query mode (`RunQuery`). When not specified, it uses list mode (`ListDocuments`). See [Filter syntax](#filter-syntax).                                                                        |
| fields         | optional | Array<String\> | Field names to retrieve (field projection). Only the specified fields are returned. If not specified, all fields are returned.                                                                                                                         |
| orderField     | optional | String         | Field name to order results by. Only available in query mode (when `filter` is specified).                                                                                                                                                             |
| orderDirection | optional | Enum           | Sort direction for `orderField`. Values: `ASCENDING`, `DESCENDING`. Default: `ASCENDING`.                                                                                                                                                             |
| parent         | optional | String         | Parent document path for subcollection queries (e.g. `users/user123`). Prepended with `/` if not already present. Default: `""` (root).                                                                                                                |
| allDescendants | optional | Boolean        | If `true`, queries all descendant collections matching `collection` name, not just immediate children. Required to be `true` when `parallel` is `true`. Default: `false`.                                                                              |
| parallel       | optional | Boolean        | If `true`, uses `PartitionQuery` to split the query into multiple partitions for parallel execution. Requires `allDescendants` to be `true`. Requires [collection group index exemption](https://cloud.google.com/firestore/docs/query-data/indexing) for filtered fields. Default: `false`. |
| partitionCount | optional | Long           | Number of partitions for parallel query. Only used when `parallel` is `true`. Default: `maxNumWorkers - 1` (or `1` if maxNumWorkers <= 1).                                                                                                            |
| pageSize       | optional | Integer        | Page size for list requests. Only used in list mode (when `filter` is not specified).                                                                                                                                                                  |

### Filter syntax

The `filter` parameter accepts a simple expression string with comparison operators and `AND` logical operator.

**Supported comparison operators:** `=`, `>`, `<`, `>=`, `<=`, `!=`

**Supported logical operator:** `AND` (only)

**Format:** `field operator value [AND field operator value ...]`

String values should be quoted with single or double quotes. The field type is inferred from the `schema` definition.

**Examples:**

```
status = 'active'
```

```
created_at >= '2024-01-01T00:00:00Z' AND created_at < '2024-02-01T00:00:00Z'
```

```
age >= 18 AND status = 'active'
```

Note: Appropriate [composite indexes](https://cloud.google.com/firestore/docs/query-data/indexing) must be created for filter conditions that use multiple fields or inequality operators.

### Reserved schema fields

Special fields can be defined in the `schema` to retrieve document metadata:

| name             | type      | description                                      |
|------------------|-----------|--------------------------------------------------|
| `__name__`       | STRING    | Full resource path / unique key name of the document.  |
| `__createtime__` | TIMESTAMP | Timestamp when the document was created.         |
| `__updatetime__` | TIMESTAMP | Timestamp when the document was last updated.    |

## Execution modes

### List mode (no filter)

When `filter` is not specified, the module uses the Firestore `ListDocuments` API to retrieve all documents in the collection. This mode is simpler but does not support ordering or complex filtering.

### Query mode (with filter)

When `filter` is specified, the module builds a `StructuredQuery` and uses the `RunQuery` API. This mode supports:
- Field filtering with comparison operators
- Result ordering via `orderField` / `orderDirection`
- Field projection via `fields`
- Parallel partition execution via `parallel`

### Parallel query mode

When `parallel` is `true`, the module uses `PartitionQuery` to split the query into multiple partitions that can be executed in parallel across workers. This improves throughput for large collections.

Requirements:
- `allDescendants` must be `true`
- Collection group index exemption must be created for the filtered fields

## Examples

### Example 1: Read all documents from a collection

```yaml
sources:
  - name: users
    module: firestore
    schema:
      fields:
        - name: user_id
          type: string
        - name: name
          type: string
        - name: email
          type: string
        - name: created_at
          type: timestamp
    parameters:
      projectId: myproject
      collection: users
```

### Example 2: Read with filter condition

Read only active users.

```yaml
sources:
  - name: active_users
    module: firestore
    schema:
      fields:
        - name: user_id
          type: string
        - name: name
          type: string
        - name: status
          type: string
        - name: last_login
          type: timestamp
    parameters:
      projectId: myproject
      collection: users
      filter: "status = 'active'"
    timestampAttribute: last_login
```

### Example 3: Read with date range filter

Read documents within a specific date range.

```yaml
sources:
  - name: recent_orders
    module: firestore
    schema:
      fields:
        - name: order_id
          type: string
        - name: amount
          type: float64
        - name: created_at
          type: timestamp
    parameters:
      projectId: myproject
      collection: orders
      filter: "created_at >= '2024-01-01T00:00:00Z' AND created_at < '2024-02-01T00:00:00Z'"
      orderField: created_at
      orderDirection: DESCENDING
```

### Example 4: Read with field projection

Only retrieve specific fields to reduce data transfer.

```yaml
sources:
  - name: user_emails
    module: firestore
    schema:
      fields:
        - name: user_id
          type: string
        - name: email
          type: string
    parameters:
      projectId: myproject
      collection: users
      fields:
        - user_id
        - email
```

### Example 5: Read subcollection documents

Read documents from a subcollection under a specific parent document.

```yaml
sources:
  - name: user_orders
    module: firestore
    schema:
      fields:
        - name: order_id
          type: string
        - name: item
          type: string
        - name: amount
          type: float64
    parameters:
      projectId: myproject
      collection: orders
      parent: "users/user123"
```

### Example 6: Parallel query for large collections

Use partition query for parallel execution on large datasets.

```yaml
sources:
  - name: all_events
    module: firestore
    schema:
      fields:
        - name: event_id
          type: string
        - name: event_type
          type: string
        - name: timestamp
          type: timestamp
    parameters:
      projectId: myproject
      collection: events
      filter: "event_type = 'click'"
      allDescendants: true
      parallel: true
      partitionCount: 50
```

### Example 7: Read with document metadata

Include document metadata fields in the output.

```yaml
sources:
  - name: documents
    module: firestore
    schema:
      fields:
        - name: __name__
          type: string
        - name: __createtime__
          type: timestamp
        - name: __updatetime__
          type: timestamp
        - name: title
          type: string
        - name: content
          type: string
    parameters:
      projectId: myproject
      collection: articles
```

### Example 8: Read from a non-default database

```yaml
sources:
  - name: items
    module: firestore
    schema:
      fields:
        - name: item_id
          type: string
        - name: name
          type: string
        - name: price
          type: float64
    parameters:
      projectId: myproject
      databaseId: my-database
      collection: items
```

### Example 9: Firestore to BigQuery export

```yaml
sources:
  - name: firestore_source
    module: firestore
    schema:
      fields:
        - name: user_id
          type: string
        - name: name
          type: string
        - name: email
          type: string
        - name: created_at
          type: timestamp
    parameters:
      projectId: myproject
      collection: users

sinks:
  - name: bigquery_output
    module: bigquery
    inputs:
      - firestore_source
    parameters:
      table: "myproject.mydataset.users"
```
