---
type: Source Module
title: Datastore Source Module
description: Reads entities from Google Cloud Datastore using GQL queries. Supports namespace filtering, query splitting for parallel reads, and optional entity key extraction.
tags: [source, datastore, batch, gcp, nosql, entity, gql]
timestamp: 2026-06-22T00:00:00Z
---

# Datastore Source Module

Source Module for reading entities from [Google Cloud Datastore](https://cloud.google.com/datastore/docs) using [GQL](https://cloud.google.com/datastore/docs/reference/gql_reference) queries. Each entity is converted to a record matching the specified schema.

A `schema` must be defined at the source level to describe the expected entity properties (unless `kind` is specified for schema auto-detection).

## Source module common parameters

| parameter          | optional | type                | description                                                                                                                                          |
|--------------------|----------|---------------------|------------------------------------------------------------------------------------------------------------------------------------------------------|
| name               | required | String              | Step name. specified to be unique in config file.                                                                                                    |
| module             | required | String              | Specified `datastore`                                                                                                                                |
| schema             | required | [Schema](../common/schema.md) | Schema of the data to be read. Required unless `kind` is specified (in which case schema is estimated from Datastore Stats on a best-effort basis). |
| timestampAttribute | optional | String              | If you want to use the value of a field as the event time, specify the name of the field. (The field must be Timestamp type)                         |
| parameters         | required | Map<String,Object\> | Specify the following individual parameters                                                                                                          |

## Datastore source module parameters

| parameter      | optional | type    | description                                                                                                                                                                                                                    |
|----------------|----------|---------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| projectId      | optional | String  | GCP Project ID of the Datastore to read from. If not specified, the pipeline execution environment's project ID is used.                                                                                                       |
| gql            | required | String  | Query in [GQL format](https://cloud.google.com/datastore/docs/reference/gql_reference) to read data from Datastore.                                                                                                           |
| namespace      | optional | String  | Datastore namespace to query within. If not specified, the default namespace is used.                                                                                                                                           |
| kind           | optional | String  | Kind name. When specified without `schema`, the module estimates the kind's schema on a best-effort basis using [Datastore Stats](https://cloud.google.com/datastore/docs/concepts/stats).                                     |
| numQuerySplits | optional | Integer | Number of splits for parallel query execution. Controls the degree of parallelism for reading. If not specified, the Beam SDK determines the split count automatically.                                                        |
| withKey        | optional | Boolean | If `true`, includes the entity's Key as a `__key__` field in the output. See [Key schema](#key-schema). Default: `false`.                                                                                                      |

### Key schema

When `withKey` is `true`, a `__key__` field is appended to the output schema with the following structure:

| field     | type   | description                                            |
|-----------|--------|--------------------------------------------------------|
| namespace | STRING | The namespace of the entity.                           |
| app       | STRING | The application/project ID.                            |
| path      | STRING | The full key path string.                              |
| kind      | STRING | The kind name of the entity.                           |
| name      | STRING | The string name of the key (null if numeric ID is used). |
| id        | INT64  | The numeric ID of the key (null if string name is used). |

Note: Because `__key__` is a nested (element) field, some sink modules that do not support nested types may not be able to process it directly.

## Examples

### Example 1: Read all entities of a kind

Read all entities from a Datastore kind with a defined schema.

```yaml
sources:
  - name: users
    module: datastore
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
      gql: "SELECT * FROM User"
```

### Example 2: Read with GQL filter

Query with conditions to read a subset of entities.

```yaml
sources:
  - name: active_users
    module: datastore
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
      gql: "SELECT * FROM User WHERE status = 'active'"
    timestampAttribute: last_login
```

### Example 3: Read with namespace

Query entities in a specific namespace.

```yaml
sources:
  - name: tenant_data
    module: datastore
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
      namespace: tenant-a
      gql: "SELECT * FROM Product"
```

### Example 4: Read with entity key

Read entities including the `__key__` field for downstream processing (e.g. deletion).

```yaml
sources:
  - name: entities_with_key
    module: datastore
    schema:
      fields:
        - name: field1
          type: string
        - name: field2
          type: int64
    parameters:
      projectId: myproject
      gql: "SELECT __key__ FROM MyKind"
      withKey: true
```

### Example 5: Read and delete entities

Read entity keys from Datastore and delete them using the Datastore sink.

```yaml
sources:
  - name: read
    module: datastore
    schema:
      fields:
        - name: field1
          type: string
    parameters:
      projectId: myproject
      gql: "SELECT __key__ FROM MyKind"
      withKey: true

sinks:
  - name: delete
    module: datastore
    inputs:
      - read
    parameters:
      projectId: myproject
      kind: MyKind
      delete: true
```

### Example 6: Read with parallel query splitting

Increase parallelism for large datasets by specifying the number of query splits.

```yaml
sources:
  - name: large_dataset
    module: datastore
    schema:
      fields:
        - name: id
          type: string
        - name: data
          type: string
        - name: updated_at
          type: timestamp
    parameters:
      projectId: myproject
      gql: "SELECT * FROM LargeKind"
      numQuerySplits: 100
```

### Example 7: Read with nested schema

Read entities with embedded (nested) fields.

```yaml
sources:
  - name: orders
    module: datastore
    schema:
      fields:
        - name: order_id
          type: string
        - name: items
          type: string
          mode: repeated
        - name: created_at
          type: timestamp
        - name: address
          type: record
          fields:
            - name: street
              type: string
            - name: city
              type: string
            - name: zip
              type: string
    parameters:
      projectId: myproject
      gql: "SELECT * FROM Order"
```

### Example 8: Export Datastore to Avro files

```yaml
sources:
  - name: datastore_input
    module: datastore
    schema:
      fields:
        - name: field1
          type: string
          mode: required
        - name: field2
          type: string
          mode: repeated
        - name: created_at
          type: timestamp

sinks:
  - name: avro_output
    module: storage
    inputs:
      - datastore_input
    parameters:
      output: "gs://my-bucket/exports/datastore"
      format: avro
```
