---
type: Sink Module
title: Bigtable Sink Module
description: Writes, updates, or deletes cells and rows in Google Cloud Bigtable. Supports flexible row key generation with FreeMarker templates, multiple column families and qualifiers, per-record dynamic mutation operations, cell timestamp control, flow control, and batching.
tags: [sink, bigtable, batch, streaming, gcp, nosql, wide-column]
timestamp: 2026-06-22T00:00:00Z
---

# Bigtable Sink Module

Sink Module for writing input data as mutations to [Google Cloud Bigtable](https://cloud.google.com/bigtable/docs). Each input record is converted into one or more Bigtable mutations (cell writes, column deletes, family deletes, or row deletes) and applied to the specified table.

The row key is generated from a FreeMarker template expression. Column families and qualifiers define which cells to write to. The mutation operation type (`mutationOp`) can be a fixed value or a FreeMarker template expression evaluated per record, enabling dynamic mutation routing based on record content.

## Sink module common parameters

| parameter  | optional | type                | description                                                           |
|------------|----------|---------------------|-----------------------------------------------------------------------|
| name       | required | String              | Step name. specified to be unique in config file.                     |
| module     | required | String              | Specified `bigtable`                                                  |
| inputs     | required | Array<String\>      | Specify the names of the step to be used as input.                    |
| wait       | optional | Array<String\>      | Specify the names of the steps to wait for before processing.        |
| strategy   | optional | [Strategy](../common/strategy.md) | Windowing strategy for streaming execution.               |
| logging    | optional | Array<[Logging](../common/logging.md)\> | Logging settings. Supports `input` and `output` targets. |
| parameters | required | Map<String,Object\> | Specify the following individual parameters                          |

## Bigtable sink module parameters

| parameter              | optional | type           | description                                                                                                                                                                                                                                     |
|------------------------|----------|----------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| projectId              | required | String         | GCP Project ID of the Cloud Bigtable instance.                                                                                                                                                                                                  |
| instanceId             | required | String         | Cloud Bigtable instance ID.                                                                                                                                                                                                                     |
| tableId                | required | String         | Cloud Bigtable table name.                                                                                                                                                                                                                      |
| rowKey                 | required | String         | FreeMarker template expression for generating the row key. All input field values are available as template variables. See [Row key template](#row-key-template).                                                                                |
| columns                | optional | Array<[Column](#column-parameters)\> | Column family and qualifier settings for cell mutations. Required unless `mutationOp` is `DELETE_FROM_ROW`.                                                                                                                  |
| format                 | optional | Enum           | Default cell value serialization format. One of `bytes`, `avro`, `text`. Applied as the default for all columns/qualifiers that do not specify their own. Default: `bytes`.                                                                      |
| mutationOp             | optional | String         | Default mutation operation. A [MutationOp](#mutationop) enum name (e.g. `SET_CELL`) or a FreeMarker template expression (e.g. `${op_type}`) that evaluates to a MutationOp name per record. Applied as the default for all columns/qualifiers that do not specify their own. Default: `SET_CELL`. See [Dynamic mutationOp](#dynamic-mutationop). |
| timestampType          | optional | Enum           | Default cell timestamp type. One of `server`, `event`, `current`, `field`, `fixed`, `zero`. Applied as the default for all columns/qualifiers that do not specify their own. Default: `server`. See [TimestampType](#timestamptype).              |
| timestampField         | optional | String         | Field name to use as the cell timestamp when `timestampType` is `field`. Applied as the default for all columns/qualifiers.                                                                                                                     |
| timestampValue         | optional | String         | Fixed timestamp value string when `timestampType` is `fixed`. Applied as the default for all columns/qualifiers.                                                                                                                                |
| withWriteResults       | optional | Boolean        | If `true`, the module outputs write result records instead of `PDone`. Default: `false`. See [Output schema](#output-schema).                                                                                                                   |
| appProfileId           | optional | String         | Bigtable app profile ID.                                                                                                                                                                                                                        |
| flowControl            | optional | Boolean        | If `true`, enables client-side flow control. Default: `false`.                                                                                                                                                                                  |
| maxBytesPerBatch       | optional | Long           | Maximum bytes per write batch. Must be greater than zero.                                                                                                                                                                                       |
| maxElementsPerBatch    | optional | Long           | Maximum elements per write batch. Must be greater than zero.                                                                                                                                                                                    |
| maxOutstandingBytes    | optional | Long           | Maximum outstanding bytes before enforcing flow control. Must be greater than zero.                                                                                                                                                             |
| maxOutstandingElements | optional | Long           | Maximum outstanding elements before enforcing flow control. Must be greater than zero.                                                                                                                                                          |
| batching               | optional | Boolean        | If `true`, aggregates mutations by row key before writing to reduce Bigtable I/O load. Useful when mutations contain many columns per row. Default: `false`.                                                                                    |
| maxMutationPerBatchElement | optional | Long       | When `batching` is `true`, the maximum number of mutations per batch element. Default: `1000`.                                                                                                                                                  |
| operationTimeoutSecond | optional | Integer        | Operation timeout in seconds. Must be greater than zero.                                                                                                                                                                                        |
| attemptTimeoutSecond   | optional | Integer        | Per-attempt timeout in seconds. Must be greater than zero.                                                                                                                                                                                      |
| emulatorHost           | optional | String         | Bigtable emulator host address (e.g. `localhost:8086`). When specified, the write connects to the emulator instead of Cloud Bigtable.                                                                                                           |

### Parameter cascading

The parameters `format`, `mutationOp`, `timestampType`, `timestampField`, and `timestampValue` follow a three-level cascade:

```
Top-level (parameters)  -->  Column (column family)  -->  Qualifier (column qualifier)
```

Each level inherits from its parent unless explicitly overridden. This allows setting a default at the top level and overriding per column family or per qualifier as needed.

## Column parameters

Settings for each column family. If `format`, `mutationOp`, or `timestampType` are not specified, the parent-level setting is applied.

| parameter     | optional | type                                     | description                                                                                                              |
|---------------|----------|------------------------------------------|--------------------------------------------------------------------------------------------------------------------------|
| family        | required | String                                   | Column family name. Supports FreeMarker template expressions for dynamic family names.                                   |
| qualifiers    | optional | Array<[Qualifier](#qualifier-parameters)\> | Column qualifier settings. If omitted (or empty) and the mutation operation is `SET_CELL` or `DELETE_FROM_COLUMN`, qualifiers are automatically derived from all input schema fields (qualifier name = field name). Not needed when `mutationOp` resolves to `DELETE_FROM_FAMILY`. |
| format        | optional | Enum                                     | Cell value serialization format. One of `bytes`, `avro`, `text`. Default: inherited from parent.                         |
| mutationOp    | optional | String                                   | Mutation operation. A [MutationOp](#mutationop) name or FreeMarker template expression. Default: inherited from parent.  |
| timestampType | optional | Enum                                     | Cell timestamp type. Default: inherited from parent.                                                                     |
| timestampField | optional | String                                  | Field name for `field` timestamp type. Default: inherited from parent.                                                   |
| timestampValue | optional | String                                  | Fixed timestamp value for `fixed` timestamp type. Default: inherited from parent.                                        |

## Qualifier parameters

Settings for each column qualifier. If `format`, `mutationOp`, or `timestampType` are not specified, the parent-level setting is applied.

| parameter      | optional | type   | description                                                                                                                          |
|----------------|----------|--------|--------------------------------------------------------------------------------------------------------------------------------------|
| name           | required | String | Column qualifier name. Supports FreeMarker template expressions. If `field` is not specified, the qualifier name is also used as the field name. |
| field          | optional | String | Input field name whose value is written to this cell. Default: same as `name`.                                                       |
| format         | optional | Enum   | Cell value serialization format. One of `bytes`, `avro`, `text`. Default: inherited from parent.                                     |
| type           | optional | String | Serialization type override. Specifies a fixed type name (e.g. `STRING`, `INT64`, `FLOAT64`) or a FreeMarker template expression that evaluates to a type name per record (e.g. `${type_field}`). When set, the cell value is serialized according to the resolved type instead of the default field type. See [Dynamic type](#dynamic-type). |
| mutationOp     | optional | String | Mutation operation. A [MutationOp](#mutationop) name or FreeMarker template expression. Default: inherited from parent.              |
| timestampType  | optional | Enum   | Cell timestamp type. Default: inherited from parent.                                                                                 |
| timestampField | optional | String | Field name for `field` timestamp type. Default: inherited from parent.                                                               |
| timestampValue | optional | String | Fixed timestamp value for `fixed` timestamp type. Default: inherited from parent.                                                    |

## MutationOp

Specifies the type of change to apply to the target cell, column family, or row. The `mutationOp` field accepts either a fixed enum name or a FreeMarker template expression that evaluates to one of these names.

| value              | applicable level         | description                                                          |
|--------------------|--------------------------|----------------------------------------------------------------------|
| SET_CELL           | qualifier                | Write a value to the cell.                                           |
| ADD_TO_CELL        | qualifier                | Add a value to an existing aggregation cell.                         |
| MERGE_TO_CELL      | qualifier                | Merge a value into an existing aggregation cell.                     |
| DELETE_FROM_COLUMN | qualifier                | Delete all cells from the specified column qualifier.                |
| DELETE_FROM_FAMILY | column (family)          | Delete all cells from the specified column family.                   |
| DELETE_FROM_ROW    | top-level (parameters)   | Delete all cells from the row. No `columns` required.                |

### Dynamic mutationOp

The `mutationOp` parameter accepts a FreeMarker template expression (e.g. `${op_type}`) that is evaluated per input record. The evaluated string must resolve to a valid MutationOp enum name. This enables routing different records to different mutation operations within a single pipeline step.

For fixed values, the value is validated at pipeline construction time. For template expressions, invalid values will produce an error at record processing time.

### Dynamic type

The `type` parameter on a qualifier overrides the serialization type of the cell value. It accepts either:

1. **A fixed type name** (e.g. `STRING`, `INT64`) - The cell value is always serialized as the specified type.
2. **A FreeMarker template expression** (e.g. `${type_field}`) - The template is evaluated per record, and the resulting string is used as the type name. This allows dynamic type resolution based on record content.

Supported type names include: `STRING`, `INT32`, `INT64`, `FLOAT32`, `FLOAT64`, `BOOLEAN`, `BYTES`, `DATE`, `TIME`, `TIMESTAMP`, and others defined in `Schema.Type`.

When `type` is not specified, the cell value is serialized using its natural field type from the input schema.

## TimestampType

Determines the timestamp value set on each cell.

| value   | description                                                                             |
|---------|-----------------------------------------------------------------------------------------|
| server  | Bigtable server assigns the timestamp at write time. (default)                          |
| event   | Uses the Apache Beam event timestamp of the record.                                     |
| current | Uses the current wall-clock time at processing time.                                    |
| field   | Uses the value of the field specified in `timestampField`. The field must be a timestamp or INT64 (epoch microseconds). |
| fixed   | Uses the fixed timestamp specified in `timestampValue`.                                 |
| zero    | Sets the timestamp to epoch microseconds = 0 (unspecified).                             |

## Row key template

The `rowKey` parameter is a FreeMarker template expression. All input field values are available as template variables. Additionally, the built-in variable `__timestamp` is available, representing the element's event timestamp as a `java.time.Instant`.

Built-in utility functions are also available:

```
// Reverse timestamp for descending time-order scans
${utils.bigtable.reverseTimestampMicros(timestampField)}

// Format a date field
${utils.datetime.formatDate(dateField, 'yyyyMMdd')}

// Format a timestamp field with time zone
${utils.datetime.formatTimestamp(timestampField, 'yyyyMMddHHmmss', 'Asia/Tokyo')}

// Use the implicit event timestamp
${utils.datetime.formatTimestamp(context.timestamp, 'yyyyMMddHHmmss', 'Asia/Tokyo')}
```

## Output schema

When `withWriteResults` is `true`, the module outputs write result records:

| field       | type      | description                            |
|-------------|-----------|----------------------------------------|
| rowsWritten | INT64     | Number of rows written in the batch.   |
| timestamp   | TIMESTAMP | Current time when the result was emitted. |
| eventtime   | TIMESTAMP | Event timestamp of the write result.   |

When `withWriteResults` is `false` (default), no output is produced (returns `PDone`).

## Examples

### Example 1: Basic cell write

Write BigQuery data to Bigtable with a composite row key and two column families.

```yaml
sources:
  - name: bigquery_source
    module: bigquery
    parameters:
      query: "SELECT user_id, event, description, created_at FROM `myproject.mydataset.activities`"

sinks:
  - name: bigtable_sink
    module: bigtable
    inputs:
      - bigquery_source
    parameters:
      projectId: myproject
      instanceId: myinstance
      tableId: mytable
      rowKey: "${user_id}#${utils.bigtable.reverseTimestampMicros(created_at)}"
      timestampType: event
      columns:
        - family: u
          qualifiers:
            - name: id
              field: user_id
            - name: e
              field: event
        - family: d
          qualifiers:
            - name: desc
              field: description
```

### Example 2: Delete entire rows

Read row keys from Bigtable and delete the corresponding rows.

```yaml
sources:
  - name: bigtable_read
    module: bigtable
    parameters:
      projectId: myproject
      instanceId: myinstance
      tableId: mytable
      withRowKey: true
      rowKeyField: row_key

sinks:
  - name: bigtable_delete
    module: bigtable
    inputs:
      - bigtable_read
    parameters:
      projectId: myproject
      instanceId: myinstance
      tableId: mytable
      rowKey: "${row_key}"
      mutationOp: DELETE_FROM_ROW
```

### Example 3: Delete then insert (atomic per-row)

Bigtable applies mutations atomically per row in the order defined. This pattern first deletes existing cell values and then inserts new ones, ensuring exactly one value per cell.

```yaml
sinks:
  - name: bigtable_upsert
    module: bigtable
    inputs:
      - source
    parameters:
      projectId: myproject
      instanceId: myinstance
      tableId: mytable
      rowKey: "${user_id}#${event_name}#${utils.bigtable.reverseTimestampMicros(created_at)}"
      timestampType: server
      columns:
        - family: a
          mutationOp: DELETE_FROM_COLUMN
          qualifiers:
            - name: iid
              field: item_id
            - name: at
              field: action_type
        - family: a
          mutationOp: SET_CELL
          qualifiers:
            - name: iid
              field: item_id
            - name: at
              field: action_type
```

### Example 4: Delete an entire column family

Delete all cells in a column family for each row.

```yaml
sinks:
  - name: bigtable_delete_family
    module: bigtable
    inputs:
      - source
    parameters:
      projectId: myproject
      instanceId: myinstance
      tableId: mytable
      rowKey: "${row_key}"
      columns:
        - family: old_data
          mutationOp: DELETE_FROM_FAMILY
```

### Example 5: Dynamic mutationOp per record

Use a FreeMarker template expression to determine the mutation type from a field in each record. For example, a CDC (Change Data Capture) stream where each record has an `op` field indicating the operation type.

```yaml
sources:
  - name: cdc_stream
    module: pubsub
    parameters:
      subscription: projects/myproject/subscriptions/cdc-sub
      format: json
    schema:
      fields:
        - { name: key, type: string }
        - { name: op, type: string }
        - { name: value, type: string }

sinks:
  - name: bigtable_cdc
    module: bigtable
    inputs:
      - cdc_stream
    parameters:
      projectId: myproject
      instanceId: myinstance
      tableId: mytable
      rowKey: "${key}"
      mutationOp: "${op}"
      columns:
        - family: d
          qualifiers:
            - name: val
              field: value
```

When a record has `op: "SET_CELL"`, the value is written. When `op: "DELETE_FROM_ROW"`, the entire row is deleted. This enables handling mixed insert/update/delete operations in a single pipeline.

### Example 6: Field-based cell timestamps

Use a field value as the cell timestamp.

```yaml
sinks:
  - name: bigtable_sink
    module: bigtable
    inputs:
      - source
    parameters:
      projectId: myproject
      instanceId: myinstance
      tableId: mytable
      rowKey: "${sensor_id}"
      timestampType: field
      timestampField: recorded_at
      columns:
        - family: m
          qualifiers:
            - name: temp
              field: temperature
            - name: hum
              field: humidity
```

### Example 7: Flow control and batching for high-throughput writes

Configure flow control and batching for large-scale writes.

```yaml
sinks:
  - name: bigtable_bulk
    module: bigtable
    inputs:
      - source
    parameters:
      projectId: myproject
      instanceId: myinstance
      tableId: mytable
      rowKey: "${id}"
      flowControl: true
      maxOutstandingElements: 10000
      maxOutstandingBytes: 104857600
      batching: true
      maxMutationPerBatchElement: 500
      maxBytesPerBatch: 5242880
      maxElementsPerBatch: 1000
      operationTimeoutSecond: 600
      attemptTimeoutSecond: 150
      columns:
        - family: d
          qualifiers:
            - name: data
              field: payload

```

### Example 8: Write with results output

Enable write result output for monitoring write throughput.

```yaml
sinks:
  - name: bigtable_sink
    module: bigtable
    inputs:
      - source
    parameters:
      projectId: myproject
      instanceId: myinstance
      tableId: mytable
      rowKey: "${id}"
      withWriteResults: true
      columns:
        - family: d
          qualifiers:
            - name: val
              field: value

  - name: monitor
    module: bigquery
    inputs:
      - bigtable_sink
    parameters:
      table: myproject.monitoring.bigtable_write_results
```

### Example 9: Per-qualifier timestamp override

Set different timestamp types for different qualifiers within the same column family.

```yaml
sinks:
  - name: bigtable_sink
    module: bigtable
    inputs:
      - source
    parameters:
      projectId: myproject
      instanceId: myinstance
      tableId: mytable
      rowKey: "${user_id}"
      timestampType: server
      columns:
        - family: profile
          qualifiers:
            - name: name
              field: user_name
            - name: updated
              field: updated_at
              timestampType: field
              timestampField: updated_at
```

### Example 10: Fixed type override

Use the `type` parameter to explicitly specify the serialization type for a qualifier, overriding the field's natural type.

```yaml
sinks:
  - name: bigtable_sink
    module: bigtable
    inputs:
      - source
    parameters:
      projectId: myproject
      instanceId: myinstance
      tableId: mytable
      rowKey: "${id}"
      columns:
        - family: d
          qualifiers:
            - name: count
              field: count_value
              type: INT64
            - name: label
              field: label_value
              type: STRING
```

For dynamic type resolution based on record content, use a FreeMarker template expression:

```yaml
            - name: value
              field: attr_value
              type: "${attr_type}"
```

In this case, each record's `attr_type` field (e.g. `"STRING"`, `"INT64"`, `"FLOAT64"`) determines how `attr_value` is serialized to bytes.

### Example 11: Testing with Bigtable emulator

Connect to a local Bigtable emulator for development and testing.

```yaml
sinks:
  - name: bigtable_test
    module: bigtable
    inputs:
      - source
    parameters:
      projectId: test-project
      instanceId: test-instance
      tableId: test-table
      rowKey: "${id}"
      emulatorHost: "localhost:8086"
      columns:
        - family: cf
          qualifiers:
            - name: data
              field: value
```
