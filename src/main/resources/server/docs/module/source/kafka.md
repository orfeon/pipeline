---
type: Source Module
title: Kafka Source Module
description: Reads messages from Apache Kafka topics in streaming mode. Connects to brokers via bootstrapServers and subscribes to a single topic, a list of topics, or a regex topic pattern. Message values can be deserialized as JSON, Avro, or Protocol Buffers according to the declared schema. Supports bounded reads via maxNumRecords/maxReadTime and Kafka read options such as read-committed isolation and log-append-time timestamps.
tags: [source, kafka, streaming, messaging]
timestamp: 2026-08-06T00:00:00Z
---

# Kafka Source Module

Source Module for reading messages from [Apache Kafka](https://kafka.apache.org/) topics. Each received record's value is deserialized from the specified format and output as a structured record.

**This module only supports streaming mode.** The pipeline must be configured for streaming execution (an error is raised if the pipeline runs in batch mode).

Supported value deserialization formats:

- **json** - Deserializes the record value as a JSON object.
- **avro** - Deserializes the record value in [Apache Avro](https://avro.apache.org/) binary format. Requires `schema.avro`.
- **protobuf** - Deserializes the record value as a [Protocol Buffers](https://protobuf.dev/) message. Requires `schema.protobuf` (`messageName` and `descriptorFile`).
- **message** - Passes the raw record value through without format-specific deserialization.

## Source module common parameters

| parameter  | optional    | type                          | description                                                                                           |
|------------|-------------|-------------------------------|-------------------------------------------------------------------------------------------------------|
| name       | required    | String                        | Step name. specified to be unique in config file.                                                     |
| module     | required    | String                        | Specified `kafka`                                                                                     |
| schema     | conditional | [Schema](../common/schema.md) | Schema of the data to be read. Required for `avro` and `protobuf` formats; defines the output fields for `json`. |
| parameters | required    | Map<String,Object\>           | Specify the following individual parameters                                                           |

## Kafka source module parameters

### Connection and topic parameters

One of `topic`, `topics`, or `topicPattern` must be specified.

| parameter        | optional           | type           | description                                                                                             |
|------------------|--------------------|----------------|---------------------------------------------------------------------------------------------------------|
| bootstrapServers | required           | String         | Comma-separated list of Kafka bootstrap servers to connect to, in `host:port` form (e.g. `broker1:9092,broker2:9092`). |
| topic            | selective required | String         | Name of a single Kafka topic to read from.                                                              |
| topics           | selective required | Array<String\> | List of Kafka topic names to read from. Used when `topic` is not specified.                             |
| topicPattern     | selective required | String         | Regular expression pattern of topic names to subscribe to. Used when neither `topic` nor `topics` is specified. |

### Format parameters

| parameter | optional | type   | description                                                                                                                      |
|-----------|----------|--------|----------------------------------------------------------------------------------------------------------------------------------|
| format    | optional | Enum   | Deserialization format for the record value. Values: `json`, `avro`, `protobuf`, `message`.                                      |
| charset   | optional | String | Character encoding for the record value payload (used for text formats such as `json`). Default: `UTF-8`.                        |

### Read control parameters

| parameter          | optional | type     | description                                                                                                                          |
|--------------------|----------|----------|--------------------------------------------------------------------------------------------------------------------------------------|
| maxNumRecords      | optional | Integer  | Maximum number of records to read. Setting this turns the unbounded Kafka read into a bounded read (mainly useful for testing).       |
| maxReadTime        | optional | Duration | Maximum duration to read from the topic(s). Like `maxNumRecords`, bounds the otherwise unbounded read.                                |
| withProcessingTime | optional | Boolean  | When set, uses the processing time as the record's event timestamp.                                                                   |
| withLogAppendTime  | optional | Boolean  | When set, uses the Kafka log append time as the record's event timestamp.                                                             |
| withReadCommitted  | optional | Boolean  | When set, reads with `read_committed` isolation level, so only committed transactional messages are consumed.                         |
| withoutMetadata    | optional | Boolean  | When set, drops Kafka record metadata (partition, offset, headers) and reads only the key/value pair.                                 |

### Schema requirements by format

| format   | schema requirement                                                                                                       |
|----------|--------------------------------------------------------------------------------------------------------------------------|
| json     | Define output fields via `schema.fields`.                                                                                |
| avro     | Required. Declare the Avro schema via `schema.avro`.                                                                     |
| protobuf | Required. Declare `schema.protobuf.messageName` and `schema.protobuf.descriptorFile` (path to the compiled descriptor).  |
| message  | No schema required.                                                                                                      |

### Currently inactive parameters

The following parameters are accepted by the configuration but are not yet functional in the current implementation. Do not rely on them: `idAttribute`, `filter`, `select`, `flattenField`, `outputOriginal`.

## Examples

### Example 1: Read JSON messages from a single topic

Read JSON events from a Kafka topic and write them to BigQuery in streaming mode.

```yaml
sources:
  - name: events
    module: kafka
    schema:
      fields:
        - name: user_id
          type: string
        - name: event_type
          type: string
        - name: amount
          type: int64
    parameters:
      bootstrapServers: "broker1:9092,broker2:9092"
      topic: user-events
      format: json

sinks:
  - name: events_sink
    module: bigquery
    inputs:
      - events
    parameters:
      table: "myproject.mydataset.user_events"
      method: STORAGE_WRITE_API
      writeDisposition: WRITE_APPEND
      createDisposition: CREATE_IF_NEEDED
      triggeringFrequencySecond: 30
```

### Example 2: Read from multiple topics with read-committed isolation

Subscribe to several topics at once, consume only committed transactional messages, and use the broker's log append time as the event timestamp.

```yaml
sources:
  - name: orders
    module: kafka
    schema:
      fields:
        - name: order_id
          type: string
        - name: status
          type: string
    parameters:
      bootstrapServers: "broker1:9092"
      topics:
        - orders-jp
        - orders-us
      format: json
      withReadCommitted: true
      withLogAppendTime: true

sinks:
  - name: debug_out
    module: debug
    inputs:
      - orders
    parameters:
      logLevel: info
```
