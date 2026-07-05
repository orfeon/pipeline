---
type: Sink Module
title: Firestore Sink Module
description: Writes or deletes documents in Google Cloud Firestore. Supports flexible document name generation from fields, templates, or auto-generated UUIDs. Includes RPC QoS tuning for write throughput control.
tags: [sink, firestore, batch, streaming, gcp, nosql, document]
timestamp: 2026-06-22T00:00:00Z
---

# Firestore Sink Module

Sink Module for writing input data as documents to [Google Cloud Firestore](https://cloud.google.com/firestore/docs), or deleting existing documents by name. Each input record is converted to a Firestore Document and written (upserted) or deleted in the specified collection.

The document name (ID) can be generated from one or more input fields, a FreeMarker template expression, the built-in `__name__` field, or automatically assigned as a UUID.

## Sink module common parameters

| parameter | optional | type                | description                                                           |
|-----------|----------|---------------------|-----------------------------------------------------------------------|
| name      | required | String              | Step name. specified to be unique in config file.                     |
| module    | required | String              | Specified `firestore`                                                 |
| inputs    | required | Array<String\>      | Specify the names of the step to be used as input.                    |
| waits     | optional | Array<String\>      | Specify the names of the steps to wait for before processing.        |
| strategy  | optional | [Strategy](../common/strategy.md) | Windowing strategy for streaming execution.               |
| parameters | required | Map<String,Object\> | Specify the following individual parameters                          |

## Firestore sink module parameters

| parameter    | optional | type                          | description                                                                                                                                                                                                                                                      |
|--------------|----------|-------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| projectId    | optional | String                        | GCP Project ID. If not specified, the pipeline execution environment's project ID is used.                                                                                                                                                                        |
| databaseId   | optional | String                        | Firestore database ID. Default: `(default)`.                                                                                                                                                                                                                      |
| collection   | optional | String                        | Collection name to store documents in. Can be omitted when using `nameTemplate` that includes the full collection/document path.                                                                                                                                  |
| nameFields   | optional | Array<String\>                | Field names to use as the document name (ID). Multiple fields are concatenated with `separator`. Cannot be used together with `nameTemplate`. If both `nameFields` and `nameTemplate` are not specified, falls back to `__name__` field or auto-generated UUID.    |
| nameTemplate | optional | String                        | FreeMarker template expression to generate the document name (or full path). All input field values are available as template variables. See [Name template](#name-template). Cannot be used together with `nameFields`.                                           |
| separator    | optional | String                        | Separator string for joining multiple `nameFields` values. Default: `#`.                                                                                                                                                                                          |
| delete       | optional | Boolean                       | If `true`, deletes documents instead of writing them. The document name must be determinable via `nameFields`, `nameTemplate`, or a `__name__` field. Default: `false`.                                                                                            |
| failFast     | optional | Boolean                       | If `true`, write failures cause the pipeline to fail immediately. If `false`, failed writes are sent to a dead-letter queue for error handling. Default: `true`.                                                                                                   |
| rpcQos       | optional | [RpcQos](#rpcqos-parameters)  | Quality of Service options for Firestore RPCs. Used to tune write throughput, batching, and retry behavior.                                                                                                                                                        |

### Document name (ID) generation

The document name is determined by the following priority:

1. **`nameFields`** - Field values are concatenated with `separator` to form the document ID.
2. **`nameTemplate`** - The template expression is evaluated to produce the document name or full path.
3. **`__name__` field in input** - If the input data contains a `__name__` field, its value is used. If the value starts with `projects/`, it is used as the full resource path; otherwise, it is used as the document ID within the specified `collection`.
4. **None of the above** - A random UUID is generated as the document ID.

The final document resource path is constructed as:
```
projects/{projectId}/databases/{databaseId}/documents/{collection}/{documentId}
```

When `nameTemplate` is used without `collection`, the template must produce the full `{collection}/{documentId}` portion of the path (e.g. `users/${user_id}` or `regions/${region}/orders/${order_id}` for subcollections).

### Name template

The `nameTemplate` parameter supports FreeMarker template expressions. All input field values are available as template variables.

Built-in utility functions are also available:

```
// Format a date field
${_DateTimeUtil.formatDate(dateField, 'yyyyMMdd')}

// Format a timestamp field with time zone
${_DateTimeUtil.formatTimestamp(timestampField, 'yyyyMMddHHmmss', 'Asia/Tokyo')}

// Use the implicit event timestamp
${_DateTimeUtil.formatTimestamp(_EVENTTIME, 'yyyyMMddHHmmss')}
```

### RpcQos parameters

Quality of Service options to control Firestore write throughput and retry behavior.

| parameter              | optional | type    | description                                                                                                   |
|------------------------|----------|---------|---------------------------------------------------------------------------------------------------------------|
| batchInitialCount      | optional | Integer | Initial batch size; used before the QoS system has enough data to determine an optimal batch size.            |
| batchMaxCount          | optional | Integer | Maximum number of writes to include in a single batch.                                                        |
| batchTargetLatency     | optional | Integer | Target latency for batch requests in seconds.                                                                 |
| initialBackoff         | optional | Integer | Initial backoff duration in seconds before retrying a failed request.                                         |
| maxAttempts            | optional | Integer | Maximum number of retry attempts for a request.                                                               |
| overloadRatio          | optional | Integer | Target ratio between requests sent and successful requests.                                                   |
| samplePeriod           | optional | Integer | Duration in seconds for which sampled request data is retained.                                               |
| samplePeriodBucketSize | optional | Integer | Bucket size within the sample period in seconds.                                                              |
| throttleDuration       | optional | Integer | Duration in seconds to throttle requests when needed based on previous success rate.                          |
| hintMaxNumWorkers      | optional | Integer | Hint for the intended maximum number of workers. Defaults to the pipeline's `maxNumWorkers` setting or `10`.  |

## Output schema (write mode)

When `failFast` is `true`, the module outputs a write summary after completion:

| field     | type  | description                              |
|-----------|-------|------------------------------------------|
| numWrites | INT32 | Number of documents written.             |
| numBytes  | INT64 | Total bytes written.                     |

When `failFast` is `false`, failed writes are sent to the error handler and no summary output is produced.

When `delete` is `true`, the write summary is also output.

## Examples

### Example 1: Write with a single name field

Write BigQuery data to Firestore using a field as the document ID.

```yaml
sources:
  - name: bigquery_source
    module: bigquery
    parameters:
      query: "SELECT user_id, name, email FROM `myproject.mydataset.users`"

sinks:
  - name: firestore_sink
    module: firestore
    inputs:
      - bigquery_source
    parameters:
      projectId: myproject
      collection: users
      nameFields:
        - user_id
```

### Example 2: Composite document name

Use multiple fields joined by separator as the document ID.

```yaml
sinks:
  - name: firestore_sink
    module: firestore
    inputs:
      - source
    parameters:
      projectId: myproject
      collection: order_items
      nameFields:
        - order_id
        - item_id
      separator: "_"
```

This produces document IDs like `order123_item456`.

### Example 3: Document name with template

Use a FreeMarker template to construct a custom document name.

```yaml
sinks:
  - name: firestore_sink
    module: firestore
    inputs:
      - source
    parameters:
      projectId: myproject
      collection: daily_reports
      nameTemplate: "${region}_${_DateTimeUtil.formatDate(report_date, 'yyyyMMdd')}"
```

### Example 4: Subcollection path with template

Use `nameTemplate` to write to a subcollection without specifying `collection`.

```yaml
sinks:
  - name: firestore_sink
    module: firestore
    inputs:
      - source
    parameters:
      projectId: myproject
      nameTemplate: "users/${user_id}/orders/${order_id}"
```

This writes to the path `projects/{projectId}/databases/(default)/documents/users/{user_id}/orders/{order_id}`.

### Example 5: Auto-generated UUID document names

Omit `nameFields` and `nameTemplate` to automatically generate UUID document IDs.

```yaml
sinks:
  - name: firestore_sink
    module: firestore
    inputs:
      - source
    parameters:
      projectId: myproject
      collection: events
```

### Example 6: Delete documents

Delete documents by specifying their IDs.

```yaml
sinks:
  - name: firestore_delete
    module: firestore
    inputs:
      - documents_to_delete
    parameters:
      projectId: myproject
      collection: users
      nameFields:
        - user_id
      delete: true
```

### Example 7: Write to a non-default database

```yaml
sinks:
  - name: firestore_sink
    module: firestore
    inputs:
      - source
    parameters:
      projectId: myproject
      databaseId: my-database
      collection: items
      nameFields:
        - item_id
```

### Example 8: Tuning RPC QoS for high-throughput writes

Configure batch size and retry behavior for large-scale writes.

```yaml
sinks:
  - name: firestore_sink
    module: firestore
    inputs:
      - source
    parameters:
      projectId: myproject
      collection: large_dataset
      nameFields:
        - id
      failFast: false
      rpcQos:
        batchMaxCount: 500
        maxAttempts: 5
        initialBackoff: 2
        hintMaxNumWorkers: 50
```
