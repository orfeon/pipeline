---
type: Source Module
title: PubSub Source Module
description: Reads messages from Google Cloud Pub/Sub topics or subscriptions. Supports JSON, Avro, Protocol Buffers, and raw message formats. Includes attribute-based filtering, partition routing with inline filter/select/flatten, additional metadata field injection, seek-based replay, and multi-partition output.
tags: [source, pubsub, streaming, gcp, messaging]
timestamp: 2026-06-23T00:00:00Z
---

# PubSub Source Module

Source Module for reading messages from [Google Cloud Pub/Sub](https://cloud.google.com/pubsub/docs) topics or subscriptions. Each received message is deserialized from the specified format and output as a structured record.

**This module only supports streaming mode.** The pipeline must be configured for streaming execution.

Supports four message deserialization formats:

- **json** - Deserializes the message payload as a JSON object.
- **avro** - Deserializes the message payload in [Apache Avro](https://avro.apache.org/) binary format.
- **protobuf** - Deserializes the message payload as a [Protocol Buffers](https://protobuf.dev/) message.
- **message** - Outputs the raw PubsubMessage without deserialization, including payload bytes, attributes, message ID, and ordering key.

## Source module common parameters

| parameter          | optional | type                | description                                                                                                                                         |
|--------------------|----------|---------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| name               | required | String              | Step name. specified to be unique in config file.                                                                                                   |
| module             | required | String              | Specified `pubsub`                                                                                                                                  |
| schema             | conditional | [Schema](../common/schema.md) | Schema of the data to be read. Required for `json`, `avro`, and `protobuf` formats. Not required for `message` format. For `avro`, the schema can be auto-inferred from the Pub/Sub topic/subscription schema if not specified. |
| timestampAttribute | optional | String              | Attribute name containing the event timestamp. If specified, the attribute value is used as the event time instead of the Pub/Sub publish time.      |
| parameters         | required | Map<String,Object\> | Specify the following individual parameters                                                                                                         |

## PubSub source module parameters

### Required parameters

| parameter    | optional           | type   | description                                                                                                                                                                                            |
|--------------|--------------------|--------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| topic        | selective required | String | Pub/Sub topic to read from. Format: `projects/{project}/topics/{topic}`. Either `topic` or `subscription` must be specified, but not both. When using `topic`, a temporary subscription is auto-created. |
| subscription | selective required | String | Pub/Sub subscription to read from. Format: `projects/{project}/subscriptions/{subscription}`. Either `topic` or `subscription` must be specified, but not both.                                         |
| format       | optional           | Enum   | Deserialization format. Values: `json`, `avro`, `protobuf`, `message`. When omitted, derived from `schema.encoding.format` if declared; otherwise defaults to `message`.                                                                                                             |

### Message ID parameter

| parameter   | optional | type   | description                                                                                                                                                                          |
|-------------|----------|--------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| idAttribute | optional | String | Attribute name for message [deduplication](https://cloud.google.com/dataflow/docs/concepts/streaming-with-cloud-pubsub#efficient_deduplication). The attribute value is used as the message ID for dedup. |

### Additional fields parameters

Inject Pub/Sub message metadata (message ID, topic, ordering key, attributes) as additional fields in the output schema. Each parameter specifies the output field name for the corresponding metadata.

| parameter                     | optional | type                | description                                                                                                                                                          |
|-------------------------------|----------|---------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| additionalFields.id           | optional | String              | Output field name for the Pub/Sub message ID. The field is added as STRING type.                                                                                     |
| additionalFields.topic        | optional | String              | Output field name for the source topic. The field is added as nullable STRING type.                                                                                  |
| additionalFields.timestamp    | optional | String              | Output field name for the event timestamp. The field is added as TIMESTAMP type.                                                                                     |
| additionalFields.orderingKey  | optional | String              | Output field name for the [ordering key](https://cloud.google.com/pubsub/docs/ordering). The field is added as nullable STRING type.                                 |
| additionalFields.attributes   | optional | Map<String,String\> | Map of attribute names to output field names. Each specified Pub/Sub attribute is extracted and added as a nullable STRING field. Keys are attribute names, values are output field names. |

### Attribute filter parameter

| parameter       | optional | type   | description                                                                                                                                                   |
|-----------------|----------|--------|---------------------------------------------------------------------------------------------------------------------------------------------------------------|
| attributeFilter | optional | Filter | Filter condition applied to message attributes before deserialization. Messages that do not match the filter are discarded. Uses the same [Filter](../common/filter.md) syntax. |

### Seek parameters

Seek the subscription to a specific point before reading. Allows replaying previously acknowledged messages. Requires `subscription` to be specified.

| parameter     | optional | type   | description                                                                                                                                                                          |
|---------------|----------|--------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| seek.time     | selective required | String | Seek the subscription to a specific timestamp. Messages published after this time are replayed. Supports ISO-8601 format or `current_timestamp` for the current time. Either `time` or `snapshot` must be specified. |
| seek.snapshot | selective required | String | Seek the subscription to a [snapshot](https://cloud.google.com/pubsub/docs/replay-overview#snapshots). Format: `projects/{project}/snapshots/{snapshot}`. Either `time` or `snapshot` must be specified.              |

### Single partition parameters

These parameters apply simple inline processing when no `partitions` array is defined.

| parameter    | optional | type   | description                                                                                                                              |
|--------------|----------|--------|------------------------------------------------------------------------------------------------------------------------------------------|
| filter       | optional | Filter | Filter condition applied to deserialized records. Records that do not match are discarded (or routed to excluded output if `outputExcluded` is `true`). Uses [Filter](../common/filter.md) syntax. |
| select       | optional | Array  | Field selection and transformation. Uses [Select](../common/select.md) syntax to project, rename, or compute fields.                    |
| flattenField | optional | String | Field name of an array field to flatten (unnest). Each array element produces a separate output record.                                  |

### Multi-partition parameters

Route messages to multiple named outputs based on filter conditions. Each partition can have its own filter, select, and flatten processing.

| parameter  | optional | type    | description                                                                                                                                                                                  |
|------------|----------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| partitions | optional | Array   | Array of partition definitions. Each partition has `name`, `filter`, `select`, `flattenField`, and/or `sql`. See [Partitions](#partitions).                                                  |
| exclusive  | optional | Boolean | If `true`, each message is routed to only the first matching partition. If `false`, a message can be output to multiple partitions. Default: `true`.                                          |

### Output control parameters

| parameter            | optional | type    | description                                                                                                                                                                                                      |
|----------------------|----------|---------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| outputOriginal       | optional | Boolean | If `true`, outputs the original message as a separate `original` output. Default: `false`.                                                                                                                       |
| deserializeOriginal  | optional | Boolean | If `true` and `outputOriginal` is `true`, the original output is deserialized into the input schema. If `false`, the original is output as a raw PubsubMessage. Default: `false`.                                |
| outputExcluded       | optional | Boolean | If `true`, messages that do not match any partition filter are output as a separate `excluded` output. Default: `false`.                                                                                          |
| charset              | optional | String  | Character encoding for the message payload. Default: `UTF-8`.                                                                                                                                                    |

## Schema requirements by format

| format   | schema requirement                                                                                                             |
|----------|--------------------------------------------------------------------------------------------------------------------------------|
| json     | Required. Define fields via `schema.fields`.                                                                                   |
| avro     | Optional. Declare `schema.encoding: {format: avro}` with `schema.reference` (or `schema.fields`), or let it be auto-inferred from the Pub/Sub topic/subscription schema. |
| protobuf | Required. Declare `schema.encoding: {format: protobuf, messageName: ...}` with `schema.reference.uri` pointing at the descriptor file (legacy spelling: `schema.protobuf.descriptorFile` + `messageName`). |

Example (protobuf, new-format schema declaration — `format` is derived from the encoding):

```yaml
sources:
  - name: input
    module: pubsub
    parameters:
      subscription: projects/myproject/subscriptions/mysubscription
      schema:
        encoding:
          format: protobuf
          messageName: com.example.Event
        reference:
          uri: gs://my-bucket/schemas/event.pb
```
| message  | Not required. Output uses a fixed schema (see [Message format output schema](#message-format-output-schema)).                  |

## Message format output schema

When `format` is `message`, the output schema is fixed:

| field       | type                  | description                                         |
|-------------|-----------------------|-----------------------------------------------------|
| topic       | STRING                | The source Pub/Sub topic.                           |
| messageId   | STRING                | The Pub/Sub message ID.                             |
| orderingKey | STRING (nullable)     | The ordering key of the message.                    |
| attributes  | Map<String,String\>   | The message attributes map (nullable).              |
| payload     | BYTES (nullable)      | The raw message payload bytes.                      |
| timestamp   | TIMESTAMP             | The publish timestamp.                              |
| eventTime   | TIMESTAMP             | The event time.                                     |

## Partitions

The `partitions` parameter defines multiple named outputs, each with its own filter and processing logic. When a message matches a partition's filter, it is processed through that partition's select/flatten/sql and output with the partition's name.

Each partition object supports:

| field        | optional | type   | description                                                          |
|--------------|----------|--------|----------------------------------------------------------------------|
| name         | required | String | Output name for this partition. Referenced as `{sourceName}.{name}`. |
| filter       | optional | Filter | Filter condition for routing messages to this partition.             |
| select       | optional | Array  | Field selection and transformation for this partition.               |
| flattenField | optional | String | Array field to flatten for this partition.                            |
| sql          | optional | String | SQL query to process messages in this partition (alternative to select). |

When `exclusive` is `true` (default), each message is routed to the first matching partition only. When `false`, a message can match and be output to multiple partitions.

## Examples

### Example 1: Read JSON messages from subscription

```yaml
sources:
  - name: pubsub_source
    module: pubsub
    schema:
      fields:
        - name: user_id
          type: string
        - name: event_type
          type: string
        - name: amount
          type: double
        - name: created_at
          type: timestamp
    parameters:
      subscription: "projects/myproject/subscriptions/events-sub"
      format: json
```

### Example 2: Read Avro messages from topic

```yaml
sources:
  - name: pubsub_source
    module: pubsub
    schema:
      avro:
        file: "gs://my-bucket/schemas/user_event.avsc"
    parameters:
      topic: "projects/myproject/topics/avro-events"
      format: avro
```

### Example 3: Read Protocol Buffers messages

```yaml
sources:
  - name: pubsub_source
    module: pubsub
    schema:
      protobufDescriptor: "gs://my-bucket/descriptors/messages.desc"
      protobufMessageName: "com.example.entity.UserEvent"
    parameters:
      subscription: "projects/myproject/subscriptions/proto-sub"
      format: protobuf
```

### Example 4: Read raw messages

Read messages without deserialization.

```yaml
sources:
  - name: pubsub_source
    module: pubsub
    parameters:
      subscription: "projects/myproject/subscriptions/raw-sub"
      format: message
```

### Example 5: Read with message ID deduplication

Use an attribute for deduplication.

```yaml
sources:
  - name: pubsub_source
    module: pubsub
    schema:
      fields:
        - name: order_id
          type: string
        - name: amount
          type: double
    parameters:
      subscription: "projects/myproject/subscriptions/orders-sub"
      format: json
      idAttribute: order_id
```

### Example 6: Read with additional metadata fields

Inject Pub/Sub message metadata into the output schema.

```yaml
sources:
  - name: pubsub_source
    module: pubsub
    schema:
      fields:
        - name: user_id
          type: string
        - name: event_type
          type: string
    parameters:
      subscription: "projects/myproject/subscriptions/events-sub"
      format: json
      additionalFields:
        id: message_id
        topic: source_topic
        timestamp: publish_time
        orderingKey: ordering_key
        attributes:
          trace_id: trace_id
          region: source_region
```

This adds `message_id`, `source_topic`, `publish_time`, `ordering_key`, `trace_id`, and `source_region` fields to the output schema.

### Example 7: Read with attribute-based filtering

Discard messages based on attribute values.

```yaml
sources:
  - name: pubsub_source
    module: pubsub
    schema:
      fields:
        - name: user_id
          type: string
        - name: event_type
          type: string
    parameters:
      subscription: "projects/myproject/subscriptions/events-sub"
      format: json
      attributeFilter:
        key: event_type
        op: "="
        value: "purchase"
```

### Example 8: Read with seek replay

Replay messages from a specific timestamp.

```yaml
sources:
  - name: pubsub_source
    module: pubsub
    schema:
      fields:
        - name: user_id
          type: string
        - name: event_type
          type: string
    parameters:
      subscription: "projects/myproject/subscriptions/events-sub"
      format: json
      seek:
        time: "2024-01-15T00:00:00Z"
```

### Example 9: Multi-partition routing

Route messages to different outputs based on filter conditions.

```yaml
sources:
  - name: pubsub_source
    module: pubsub
    schema:
      fields:
        - name: user_id
          type: string
        - name: event_type
          type: string
        - name: amount
          type: double
    parameters:
      subscription: "projects/myproject/subscriptions/events-sub"
      format: json
      exclusive: true
      outputExcluded: true
      partitions:
        - name: purchases
          filter:
            key: event_type
            op: "="
            value: "purchase"
          select:
            - name: user_id
              field: user_id
            - name: amount
              field: amount
        - name: logins
          filter:
            key: event_type
            op: "="
            value: "login"
          select:
            - name: user_id
              field: user_id

sinks:
  - name: purchase_sink
    module: bigquery
    inputs:
      - pubsub_source.purchases
    parameters:
      table: "myproject.mydataset.purchases"
      writeDisposition: WRITE_APPEND
      createDisposition: CREATE_NEVER

  - name: login_sink
    module: bigquery
    inputs:
      - pubsub_source.logins
    parameters:
      table: "myproject.mydataset.logins"
      writeDisposition: WRITE_APPEND
      createDisposition: CREATE_NEVER
```

### Example 10: PubSub to BigQuery streaming pipeline

```yaml
sources:
  - name: pubsub_source
    module: pubsub
    schema:
      fields:
        - name: user_id
          type: string
        - name: event_type
          type: string
        - name: amount
          type: double
        - name: event_time
          type: timestamp
    parameters:
      subscription: "projects/myproject/subscriptions/events-sub"
      format: json
    timestampAttribute: event_time

sinks:
  - name: bigquery_sink
    module: bigquery
    inputs:
      - pubsub_source
    parameters:
      table: "myproject.mydataset.events"
      method: STORAGE_WRITE_API
      writeDisposition: WRITE_APPEND
      createDisposition: CREATE_NEVER
      triggeringFrequencySecond: 30
      numStorageWriteApiStreams: 4
```

### Example 11: PubSub to Spanner streaming pipeline

```yaml
sources:
  - name: pubsub_source
    module: pubsub
    schema:
      fields:
        - name: field_string
          type: string
          mode: required
        - name: field_double
          type: double
          mode: nullable
        - name: field_long
          type: long
          mode: nullable
        - name: field_timestamp
          type: timestamp
          mode: nullable
    parameters:
      subscription: "projects/myproject/subscriptions/my-sub"
      format: avro

sinks:
  - name: spanner_sink
    module: spanner
    inputs:
      - pubsub_source
    parameters:
      projectId: myproject
      instanceId: myinstance
      databaseId: mydatabase
      table: mytable
```
