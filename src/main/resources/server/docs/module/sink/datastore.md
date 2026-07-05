---
type: Sink Module
title: Datastore Sink Module
description: Writes or deletes entities in Google Cloud Datastore. Supports flexible key generation from fields, templates, or auto-generated UUIDs, and can exclude fields from indexing.
tags: [sink, datastore, batch, streaming, gcp, nosql, entity]
timestamp: 2026-06-22T00:00:00Z
---

# Datastore Sink Module

Sink Module for writing input data as entities to [Google Cloud Datastore](https://cloud.google.com/datastore/docs), or deleting existing entities by key. Each input record is converted to a Datastore Entity and upserted (or deleted) in the specified kind.

The entity key can be generated from one or more input fields, a FreeMarker template expression, or automatically assigned as a UUID.

## Sink module common parameters

| parameter | optional | type                | description                                                           |
|-----------|----------|---------------------|-----------------------------------------------------------------------|
| name      | required | String              | Step name. specified to be unique in config file.                     |
| module    | required | String              | Specified `datastore`                                                 |
| inputs    | required | Array<String\>      | Specify the names of the step to be used as input.                    |
| waits     | optional | Array<String\>      | Specify the names of the steps to wait for before processing.        |
| strategy  | optional | [Strategy](../common/strategy.md) | Windowing strategy for streaming execution.               |
| parameters | required | Map<String,Object\> | Specify the following individual parameters                          |

## Datastore sink module parameters

| parameter              | optional | type           | description                                                                                                                                                                                                                       |
|------------------------|----------|----------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| projectId              | required | String         | GCP Project ID containing the Datastore to write to.                                                                                                                                                                              |
| kind                   | required | String         | Datastore Kind name to save entities under.                                                                                                                                                                                       |
| keyFields              | optional | Array<String\> | Field names to use as the entity key. If a single INT64 field is specified, it is used as a numeric ID key. If multiple fields or string fields are specified, their values are concatenated with `separator` to form a string name key. Cannot be used together with `keyTemplate`. |
| keyTemplate            | optional | String         | FreeMarker template expression to generate the entity key string. All input field values are available as template variables. Cannot be used together with `keyFields`. See [Key template](#key-template).                          |
| separator              | optional | String         | Separator string for joining multiple `keyFields` values. Default: `#`.                                                                                                                                                           |
| delete                 | optional | Boolean        | If `true`, deletes entities instead of writing them. The entity key must be determinable via `keyFields`, `keyTemplate`, or a `__key__` field in the input schema. Default: `false`.                                               |
| excludeFromIndexFields | optional | Array<String\> | Field names to exclude from Datastore indexing. Use this for fields that do not need to be queried and would otherwise consume index storage.                                                                                      |
| enableRampupThrottling | optional | Boolean        | If `true`, enables [ramp-up throttling](https://cloud.google.com/datastore/docs/best-practices#ramping_up_traffic) to gradually increase write throughput. Default: `false`.                                                       |

### Key generation behavior

The entity key is determined by the following priority:

1. **`keyFields` with a single INT64 field** - The field value is used as a numeric ID key.
2. **`keyFields` with one or more fields** - Field values are concatenated with `separator` to form a string name key (e.g. `value1#value2`).
3. **`keyTemplate`** - The template expression is evaluated to produce a string name key.
4. **Input `__key__` field** - If the input data already contains a Datastore key (e.g. from a Datastore source), it is reused with the specified `kind`.
5. **None of the above** - A random UUID is generated as the string name key.

### Key template

The `keyTemplate` parameter supports FreeMarker template expressions. All input field values are available as template variables.

Built-in utility functions are also available:

```
// Format a date field
${_DateTimeUtil.formatDate(dateField, 'yyyyMMdd')}

// Format a timestamp field with time zone
${_DateTimeUtil.formatTimestamp(timestampField, 'yyyyMMddHHmmss', 'Asia/Tokyo')}

// Use the implicit event timestamp
${_DateTimeUtil.formatTimestamp(_EVENTTIME, 'yyyyMMddHHmmss')}
```

## Output schema (write mode)

When `delete` is `false`, the module outputs a write summary after completion:

| field     | type  | description                              |
|-----------|-------|------------------------------------------|
| numWrites | INT32 | Number of entities written.              |
| numBytes  | INT64 | Total bytes written.                     |

When `delete` is `true`, no output is produced (returns `PDone`).

## Examples

### Example 1: Write with a single key field

Write BigQuery data to Datastore using a single field as the entity key.

```yaml
sources:
  - name: bigquery_source
    module: bigquery
    parameters:
      query: "SELECT user_id, name, email FROM `myproject.mydataset.users`"

sinks:
  - name: datastore_sink
    module: datastore
    inputs:
      - bigquery_source
    parameters:
      projectId: myproject
      kind: User
      keyFields:
        - user_id
```

### Example 2: Composite key from multiple fields

Use multiple fields joined by `#` separator as the entity key.

```yaml
sinks:
  - name: datastore_sink
    module: datastore
    inputs:
      - source
    parameters:
      projectId: myproject
      kind: OrderItem
      keyFields:
        - order_id
        - item_id
      separator: "#"
```

This produces keys like `order123#item456`.

### Example 3: Key generation with template

Use a FreeMarker template to construct a custom key format.

```yaml
sinks:
  - name: datastore_sink
    module: datastore
    inputs:
      - source
    parameters:
      projectId: myproject
      kind: DailyReport
      keyTemplate: "${region}_${_DateTimeUtil.formatDate(report_date, 'yyyyMMdd')}"
```

### Example 4: Auto-generated UUID keys

Omit `keyFields` and `keyTemplate` to automatically generate UUID keys.

```yaml
sinks:
  - name: datastore_sink
    module: datastore
    inputs:
      - source
    parameters:
      projectId: myproject
      kind: Event
```

### Example 5: Exclude fields from indexing

Exclude large or unqueried fields from Datastore indexes to save storage.

```yaml
sinks:
  - name: datastore_sink
    module: datastore
    inputs:
      - source
    parameters:
      projectId: myproject
      kind: Article
      keyFields:
        - article_id
      excludeFromIndexFields:
        - body
        - metadata
```

### Example 6: Delete entities

Read entities from Datastore and delete them.

```yaml
sources:
  - name: read
    module: datastore
    parameters:
      projectId: myproject
      kind: MyKind
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

### Example 7: Write with ramp-up throttling

Enable gradual ramp-up for large batch writes to avoid overloading Datastore.

```yaml
sinks:
  - name: datastore_sink
    module: datastore
    inputs:
      - source
    parameters:
      projectId: myproject
      kind: LargeDataset
      keyFields:
        - id
      enableRampupThrottling: true
```

### Example 8: Write Avro files to Datastore

```yaml
sources:
  - name: avro_input
    module: storage
    parameters:
      input: "gs://my-bucket/data/prefix*"
      format: avro

sinks:
  - name: datastore_output
    module: datastore
    inputs:
      - avro_input
    parameters:
      projectId: myproject
      kind: MyKind
      keyFields:
        - KeyField
```
