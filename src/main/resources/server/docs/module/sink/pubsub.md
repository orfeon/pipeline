---
type: Sink Module
title: PubSub Sink Module
description: Publishes input data as messages to Google Cloud Pub/Sub topics. Supports JSON, Avro, and Protocol Buffers serialization formats. Includes dynamic topic routing via FreeMarker templates, custom message attributes with template expressions, message ID and ordering key control, batch size tuning, and passthrough of PubsubMessage inputs.
tags: [sink, pubsub, batch, streaming, gcp, messaging]
timestamp: 2026-06-23T00:00:00Z
---

# PubSub Sink Module

Sink Module for publishing input data as messages to [Google Cloud Pub/Sub](https://cloud.google.com/pubsub/docs) topics. Each input record is serialized into the specified format and published as a PubsubMessage.

Supports four message serialization formats:

- **json** - Serializes each record as a JSON object (UTF-8 encoded).
- **avro** - Serializes each record in [Apache Avro](https://avro.apache.org/) binary format. Requires an Avro schema definition.
- **protobuf** - Serializes each record as a [Protocol Buffers](https://protobuf.dev/) message. Requires a protobuf descriptor file and message name.
- **message** - Passes through raw bytes from a specified payload field. Use when the input already contains pre-serialized message content.

The destination topic can be specified statically or dynamically using FreeMarker template expressions based on input field values.

## Sink module common parameters

| parameter  | optional | type                | description                                                           |
|------------|----------|---------------------|-----------------------------------------------------------------------|
| name       | required | String              | Step name. specified to be unique in config file.                     |
| module     | required | String              | Specified `pubsub`                                                    |
| inputs     | required | Array<String\>      | Specify the names of the step to be used as input.                    |
| waits      | optional | Array<String\>      | Specify the names of the steps to wait for before processing.        |
| strategy   | optional | [Strategy](../common/strategy.md) | Windowing strategy for streaming execution.               |
| parameters | required | Map<String,Object\> | Specify the following individual parameters                          |

## PubSub sink module parameters

### Required parameters

| parameter | optional | type   | description                                                                                                                                                                                                                                                                  |
|-----------|----------|--------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| topic     | required | String | Pub/Sub topic to publish to. Format: `projects/{project}/topics/{topic}`. Supports FreeMarker template expressions for [dynamic topic routing](#dynamic-topic-routing) (e.g. `projects/myproject/topics/events_${region}`).                                                   |
| format    | optional | Enum   | Serialization format for the message payload. Values: `json`, `avro`, `protobuf`, `message`. Required unless `schema.encoding.format` is declared (the format is then derived from it).                                                                                                                                                                                 |

### Message attribute parameters

| parameter          | optional | type                | description                                                                                                                                                                                                                                                                                              |
|--------------------|----------|---------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| attributes         | optional | Map<String,String\> | Custom [message attributes](https://cloud.google.com/pubsub/docs/publisher#using-attributes) to attach to each message. Keys are attribute names, values are FreeMarker template expressions evaluated per record. Input field names and built-in variables are available as template variables. See [Attribute templates](#attribute-templates). |
| idAttribute        | optional | String              | Attribute name for the message [deduplication ID](https://cloud.google.com/dataflow/docs/concepts/streaming-with-cloud-pubsub#efficient_deduplication). The message ID value is set as this attribute. Used together with `idAttributeFields`.                                                            |
| idAttributeFields  | optional | Array<String\>      | Field names whose values form the message ID. Multiple field values are concatenated with `#`. If not specified, the original message ID is preserved (for PubsubMessage inputs).                                                                                                                         |
| timestampAttribute | optional | String              | Attribute name to store the event timestamp of the record.                                                                                                                                                                                                                                                |

### Ordering parameters

| parameter        | optional | type           | description                                                                                                                                                                                                                                                                                       |
|------------------|----------|----------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| orderingKeyFields| optional | Array<String\> | Field names whose values form the [ordering key](https://cloud.google.com/pubsub/docs/ordering). Multiple field values are concatenated with `#`. When specified, `maxBatchSize` is automatically set to `1` due to a [Beam limitation](https://issues.apache.org/jira/browse/BEAM-13148). |

### Payload parameters

| parameter    | optional | type   | description                                                                                                             |
|--------------|----------|--------|-------------------------------------------------------------------------------------------------------------------------|
| payloadField | optional | String | Field name containing raw bytes to use as the message payload. Only used when `format` is `message`.                    |

### Batch tuning parameters

| parameter         | optional | type    | description                                                                                 |
|-------------------|----------|---------|---------------------------------------------------------------------------------------------|
| maxBatchSize      | optional | Integer | Maximum number of messages per publish batch.                                               |
| maxBatchBytesSize | optional | Integer | Maximum total bytes per publish batch.                                                      |

### Schema parameters

For Avro and Protobuf formats, a schema definition is required, declared via `parameters.schema`
(see [Schema](../common/schema.md)). To serialize using the schema attached to the destination
topic, declare `schema.reference: {destination: true}`.

| format   | schema requirement                                                                                                            |
|----------|-------------------------------------------------------------------------------------------------------------------------------|
| json     | No schema required. Uses the input schema for field serialization.                                                            |
| avro     | Requires an Avro definition: `schema.encoding: {format: avro}` with `schema.reference` (uri or inline), or `schema.fields` (legacy spelling: `schema.avro`). |
| protobuf | Requires `schema.encoding: {format: protobuf, messageName: ...}` with `schema.reference.uri` pointing at the descriptor file (legacy spelling: `schema.protobuf.descriptorFile` + `messageName`). |
| message  | No schema required. Reads raw bytes from `payloadField`.                                                                      |

### Other parameters

| parameter             | optional | type    | description                                                                                                                                                    |
|-----------------------|----------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| useDestinationSchema  | optional | Boolean | Deprecated — declare `schema.reference: {destination: true}` instead. If `true`, retrieves the schema from the destination Pub/Sub topic and uses it for serialization. Default: `false`. |

## Attribute templates

The `attributes` parameter accepts a map where each value is a FreeMarker template expression. The following variables are available:

| variable        | description                                              |
|-----------------|----------------------------------------------------------|
| `${fieldName}`  | Any input field value, referenced by field name.         |
| `${__id}`       | The original message ID (for PubsubMessage inputs).      |
| `${__source}`   | The name of the input step that produced this record.    |
| `${__timestamp}`| The event time of the record (epoch milliseconds).       |

When the input is a PubsubMessage, the original message's attributes are preserved and merged with the newly defined attributes.

## Dynamic topic routing

When the `topic` parameter contains FreeMarker template expressions (e.g. `${field_name}`), each record is published to a different topic determined by its field values. The topic name is evaluated per record using the template expression.

```
projects/myproject/topics/events_${region}
```

This routes records to topics like `events_JP`, `events_US`, etc. based on each record's `region` field value.

## PubsubMessage passthrough

When the input data type is PubsubMessage (e.g. from a PubSub source), the original message is passed through directly without re-serialization. If `attributes` are specified, they are merged with the original message's attributes. The original payload, message ID, and ordering key are preserved.

## Examples

### Example 1: Publish as JSON

Publish BigQuery data as JSON messages to a Pub/Sub topic.

```yaml
sources:
  - name: bigquery_source
    module: bigquery
    parameters:
      query: "SELECT user_id, name, email FROM `myproject.mydataset.users`"

sinks:
  - name: pubsub_sink
    module: pubsub
    inputs:
      - bigquery_source
    parameters:
      topic: "projects/myproject/topics/user-events"
      format: json
```

### Example 2: Publish with message ID and timestamp attribute

Set a deduplication ID and timestamp attribute on each message.

```yaml
sinks:
  - name: pubsub_sink
    module: pubsub
    inputs:
      - source
    parameters:
      topic: "projects/myproject/topics/transactions"
      format: json
      idAttribute: TransactionID
      idAttributeFields:
        - transaction_id
      timestampAttribute: CreatedAt
```

### Example 3: Publish with custom attributes

Attach custom attributes to each message using FreeMarker templates.

```yaml
sinks:
  - name: pubsub_sink
    module: pubsub
    inputs:
      - source
    parameters:
      topic: "projects/myproject/topics/events"
      format: json
      attributes:
        user_id: "${user_id}"
        event_type: "${event_type}"
        source_module: "${__source}"
```

### Example 4: Publish with ordering key

Publish messages with ordering key for ordered delivery.

```yaml
sinks:
  - name: pubsub_sink
    module: pubsub
    inputs:
      - source
    parameters:
      topic: "projects/myproject/topics/ordered-events"
      format: json
      orderingKeyFields:
        - user_id
```

Note: `maxBatchSize` is automatically set to `1` when `orderingKeyFields` is specified.

### Example 5: Publish as Avro binary

Serialize messages in Avro binary format.

```yaml
sinks:
  - name: pubsub_sink
    module: pubsub
    inputs:
      - source
    schema:
      avro:
        file: "gs://my-bucket/schemas/user_event.avsc"
    parameters:
      topic: "projects/myproject/topics/avro-events"
      format: avro
```

### Example 6: Publish as Protocol Buffers

Serialize messages using Protocol Buffers.

```yaml
sinks:
  - name: pubsub_sink
    module: pubsub
    inputs:
      - source
    schema:
      protobufDescriptor: "gs://my-bucket/descriptors/messages.desc"
      protobufMessageName: "com.example.entity.UserEvent"
    parameters:
      topic: "projects/myproject/topics/proto-events"
      format: protobuf
```

### Example 7: Dynamic topic routing

Route messages to different topics based on field values.

```yaml
sinks:
  - name: pubsub_sink
    module: pubsub
    inputs:
      - source
    parameters:
      topic: "projects/myproject/topics/events_${region}"
      format: json
```

### Example 8: Batch size tuning

Configure batch parameters for high-throughput publishing.

```yaml
sinks:
  - name: pubsub_sink
    module: pubsub
    inputs:
      - source
    parameters:
      topic: "projects/myproject/topics/high-volume"
      format: json
      maxBatchSize: 1000
      maxBatchBytesSize: 10485760
```

### Example 9: Raw bytes passthrough

Publish pre-serialized bytes from a specific field.

```yaml
sinks:
  - name: pubsub_sink
    module: pubsub
    inputs:
      - source
    parameters:
      topic: "projects/myproject/topics/raw-messages"
      format: message
      payloadField: content
```

### Example 10: PubSub to PubSub with transformation

Read from one topic, transform, and publish to another.

```yaml
sources:
  - name: pubsub_source
    module: pubsub
    parameters:
      subscription: "projects/myproject/subscriptions/input-sub"
      format: json
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

transforms:
  - name: filtered
    module: beamsql
    inputs:
      - pubsub_source
    parameters:
      sql: >
        SELECT user_id, event_type, amount, created_at
        FROM pubsub_source
        WHERE amount > 100

sinks:
  - name: pubsub_sink
    module: pubsub
    inputs:
      - filtered
    parameters:
      topic: "projects/myproject/topics/high-value-events"
      format: json
      attributes:
        user_id: "${user_id}"
        event_type: "${event_type}"
```
