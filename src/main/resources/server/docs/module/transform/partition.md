---
type: Transform Module
title: Partition Transform Module
description: Splits input data into multiple named outputs based on filter conditions. Each partition can have its own filter and optional select processing. Supports exclusive routing (first match only) or non-exclusive routing (multiple matches). Unmatched records are routed to an excluded output. Optionally merges all partitions into a single union output.
tags: [transform, partition, filter, routing, batch, streaming]
timestamp: 2026-06-23T00:00:00Z
---

# Partition Transform Module

Transform Module for splitting input data into multiple named output collections based on filter conditions. Each record is evaluated against the partition filters and routed to the matching partition output(s).

Supports:

- **Multiple named outputs** - Each partition produces a separate named output that can be referenced by downstream modules as `{partitionName}.{outputName}`.
- **Exclusive routing** - By default, each record is routed to only the first matching partition (`exclusive: true`).
- **Non-exclusive routing** - When `exclusive: false`, a record can match and be output to multiple partitions.
- **Per-partition select** - Each partition can optionally apply [Select](../common/select.md) field transformations, producing a partition-specific output schema.
- **Excluded output** - Records that do not match any partition filter are automatically routed to an `excluded` output.
- **Union mode** - When `union: true`, all matching partition outputs are merged into a single default output instead of separate named outputs.

## Transform module common parameters

| parameter  | optional | type                              | description                                                           |
|------------|----------|-----------------------------------|-----------------------------------------------------------------------|
| name       | required | String                            | Step name. specified to be unique in config file.                     |
| module     | required | String                            | Specified `partition`                                                 |
| inputs     | required | Array<String\>                    | Specify the names of the steps to be used as input.                   |
| wait       | optional | Array<String\>                    | Specify the names of the steps to wait for before processing.        |
| strategy   | optional | [Strategy](../common/strategy.md) | Windowing strategy for streaming execution.                           |
| parameters | required | Map<String,Object\>               | Specify the following individual parameters                          |

## Partition transform module parameters

| parameter  | optional | type                                              | description                                                                                                                                                                                       |
|------------|----------|---------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| partitions | required | Array<[Partition](#partition-parameters)\>         | List of partition definitions. Each partition specifies a name, filter condition, and optional select processing. At least one partition must be defined.                                          |
| exclusive  | optional | Boolean                                           | If `true`, each record is routed to only the first matching partition. If `false`, a record can be output to multiple partitions. Default: `true`.                                                |
| union      | optional | Boolean                                           | If `true`, all matching partition outputs are merged into a single default output instead of separate named outputs. Default: `false`.                                                            |

## Partition parameters

Each element in the `partitions` array defines one partition.

| parameter | optional | type                                     | description                                                                                                                                          |
|-----------|----------|------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------|
| name      | required | String                                   | Output name for this partition. Referenced by downstream modules as `{transformName}.{name}`.                                                        |
| filter    | required | [Filter](../common/filter.md)            | Filter condition for routing records to this partition. Records matching the condition are included in this partition's output.                       |
| select    | optional | Array<[Select](../common/select.md)\>    | Field selection and transformation for this partition. When specified, only the selected fields are included in this partition's output schema.        |

## Output naming

The partition transform produces multiple named outputs:

| output name                  | description                                                                                         |
|------------------------------|-----------------------------------------------------------------------------------------------------|
| `{transformName}`            | Default output. Contains all matched records when `union: true`. Empty when `union: false`.         |
| `{transformName}.{name}`     | Per-partition output. Contains records matching the partition's filter. Only when `union: false`.    |
| `{transformName}.excluded`   | Excluded output. Contains records that did not match any partition filter.                           |

Downstream modules reference partition outputs using the `{transformName}.{name}` format in their `inputs` parameter.

## Processing flow

For each input record:

1. Evaluate the record against each partition's filter in order.
2. If the filter matches:
   - Apply the partition's `select` transformations (if specified).
   - Output the record to the partition's named output (or the default output if `union: true`).
   - If `exclusive: true`, stop evaluating further partitions for this record.
   - If `exclusive: false`, continue evaluating the remaining partitions.
3. If no partition filter matches, output the record to the `excluded` output.

## Examples

### Example 1: Basic partitioning by field value

Split records into separate outputs based on event type.

```yaml
sources:
  - name: events
    module: bigquery
    parameters:
      query: "SELECT user_id, event_type, amount, created_at FROM `myproject.mydataset.events`"

transforms:
  - name: by_type
    module: partition
    inputs:
      - events
    parameters:
      partitions:
        - name: purchases
          filter:
            key: event_type
            op: "="
            value: "purchase"
        - name: logins
          filter:
            key: event_type
            op: "="
            value: "login"
        - name: signups
          filter:
            key: event_type
            op: "="
            value: "signup"

sinks:
  - name: purchase_sink
    module: bigquery
    inputs:
      - by_type.purchases
    parameters:
      table: "myproject.mydataset.purchases"
      writeDisposition: WRITE_APPEND
      createDisposition: CREATE_IF_NEEDED

  - name: login_sink
    module: bigquery
    inputs:
      - by_type.logins
    parameters:
      table: "myproject.mydataset.logins"
      writeDisposition: WRITE_APPEND
      createDisposition: CREATE_IF_NEEDED

  - name: other_sink
    module: bigquery
    inputs:
      - by_type.excluded
    parameters:
      table: "myproject.mydataset.other_events"
      writeDisposition: WRITE_APPEND
      createDisposition: CREATE_IF_NEEDED
```

### Example 2: Partitioning with per-partition select

Apply different field projections to each partition.

```yaml
transforms:
  - name: split
    module: partition
    inputs:
      - source
    parameters:
      partitions:
        - name: high_value
          filter:
            key: amount
            op: ">="
            value: 10000
          select:
            - name: user_id
            - name: amount
            - name: priority
              type: string
              value: "high"
        - name: low_value
          filter:
            key: amount
            op: "<"
            value: 10000
          select:
            - name: user_id
            - name: amount
```

### Example 3: Non-exclusive partitioning

Allow records to match multiple partitions.

```yaml
transforms:
  - name: tags
    module: partition
    inputs:
      - orders
    parameters:
      exclusive: false
      partitions:
        - name: large_orders
          filter:
            key: amount
            op: ">="
            value: 5000
        - name: recent_orders
          filter:
            key: order_date
            op: ">="
            value: "2024-01-01"
        - name: premium_customers
          filter:
            key: customer_tier
            op: "="
            value: "premium"
```

A single order can appear in multiple outputs (e.g., a large recent order from a premium customer would appear in all three).

### Example 4: Partitioning with compound filter conditions

Use AND/OR compound conditions.

```yaml
transforms:
  - name: segments
    module: partition
    inputs:
      - users
    parameters:
      partitions:
        - name: active_premium
          filter:
            and:
              - key: status
                op: "="
                value: "active"
              - key: tier
                op: "="
                value: "premium"
        - name: active_standard
          filter:
            and:
              - key: status
                op: "="
                value: "active"
              - key: tier
                op: "!="
                value: "premium"
        - name: inactive
          filter:
            key: status
            op: "="
            value: "inactive"
```

### Example 5: Union mode

Merge all matched partition outputs into a single output with select transformations.

```yaml
transforms:
  - name: normalized
    module: partition
    inputs:
      - raw_data
    parameters:
      union: true
      exclusive: false
      partitions:
        - name: type_a
          filter:
            key: record_type
            op: "="
            value: "A"
          select:
            - name: id
              field: a_id
            - name: value
              field: a_value
              type: float64
        - name: type_b
          filter:
            key: record_type
            op: "="
            value: "B"
          select:
            - name: id
              field: b_id
            - name: value
              field: b_value
              type: float64

sinks:
  - name: unified_sink
    module: bigquery
    inputs:
      - normalized
    parameters:
      table: "myproject.mydataset.normalized_data"
      writeDisposition: WRITE_APPEND
      createDisposition: CREATE_IF_NEEDED
```

### Example 6: Streaming partition with different sinks

Partition streaming data to different destinations.

```yaml
sources:
  - name: events
    module: pubsub
    schema:
      fields:
        - name: user_id
          type: string
        - name: event_type
          type: string
        - name: payload
          type: string
        - name: region
          type: string
    parameters:
      subscription: "projects/myproject/subscriptions/events-sub"
      format: json

transforms:
  - name: route
    module: partition
    inputs:
      - events
    parameters:
      partitions:
        - name: japan
          filter:
            key: region
            op: "="
            value: "JP"
        - name: us
          filter:
            key: region
            op: "="
            value: "US"

sinks:
  - name: japan_sink
    module: bigquery
    inputs:
      - route.japan
    parameters:
      table: "myproject.mydataset.events_jp"
      method: STORAGE_WRITE_API
      writeDisposition: WRITE_APPEND
      createDisposition: CREATE_NEVER
      triggeringFrequencySecond: 30

  - name: us_sink
    module: bigquery
    inputs:
      - route.us
    parameters:
      table: "myproject.mydataset.events_us"
      method: STORAGE_WRITE_API
      writeDisposition: WRITE_APPEND
      createDisposition: CREATE_NEVER
      triggeringFrequencySecond: 30

  - name: other_sink
    module: bigquery
    inputs:
      - route.excluded
    parameters:
      table: "myproject.mydataset.events_other"
      method: STORAGE_WRITE_API
      writeDisposition: WRITE_APPEND
      createDisposition: CREATE_NEVER
      triggeringFrequencySecond: 30
```

### Example 7: Partition for data quality routing

Route valid and invalid records to different outputs.

```yaml
transforms:
  - name: validate
    module: partition
    inputs:
      - source
    parameters:
      partitions:
        - name: valid
          filter:
            and:
              - key: email
                op: "!="
                value: ""
              - key: user_id
                op: "!="
                value: null
          select:
            - name: user_id
            - name: email
            - name: name
            - name: validated_at
              func: current_timestamp

sinks:
  - name: valid_sink
    module: spanner
    inputs:
      - validate.valid
    parameters:
      projectId: myproject
      instanceId: myinstance
      databaseId: mydatabase
      table: users

  - name: invalid_sink
    module: storage
    inputs:
      - validate.excluded
    parameters:
      output: "gs://my-bucket/invalid_records/"
      format: json
```
