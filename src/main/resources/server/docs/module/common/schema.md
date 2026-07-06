---
type: Common
title: Schema
description: The schema block shared by source/transform/sink modules — logical fields, wire-format encoding, and the schema definition reference.
tags: [common, schema, avro, protobuf]
timestamp: 2026-07-06T00:00:00Z
---

# Schema

Several modules accept a `schema` block that declares the shape of the data and, when the module
reads or writes encoded bytes (Pub/Sub messages, files, …), how to decode/encode them.

A schema block separates three concerns. All three keys are optional — use only the ones the
module needs:

| key         | question it answers                            |
|-------------|------------------------------------------------|
| `fields`    | What is the logical shape of the data?         |
| `encoding`  | How do bytes map to/from records?              |
| `reference` | Where does the schema definition come from?    |

## fields

The logical field list. When `fields` is the only key, it fully defines the schema.

```yaml
schema:
  fields:
    - { name: user_id, type: string }
    - { name: amount,  type: int64, mode: required }
    - { name: tags,    type: string, mode: repeated }
    - { name: details, type: element, fields:
        [ { name: key, type: string } ] }
```

| parameter | optional | type   | description                                                                   |
|-----------|----------|--------|-------------------------------------------------------------------------------|
| name      | required | String | Field name.                                                                   |
| type      | required | Enum   | `bool`,`string`,`json`,`bytes`,`int32` (`int`),`int64` (`long`),`float32` (`float`),`float64` (`double`),`decimal`,`date`,`time`,`timestamp`,`enum`,`map`,`element`, … |
| mode      | optional | Enum   | `nullable` (default), `required`, `repeated`.                                 |
| fields    | selective | Array | Nested fields (for `element` type).                                           |
| symbols   | selective | Array<String\> | Enum symbols (for `enum` type).                                       |

## encoding

The wire format used to decode/encode payload bytes. Only meaningful for modules that
(de)serialize (e.g. `pubsub`, `kafka`).

| parameter   | optional  | type   | description                                            |
|-------------|-----------|--------|--------------------------------------------------------|
| format      | required  | Enum   | `avro`, `protobuf`.                                    |
| messageName | selective | String | Protobuf message full name. Required for `protobuf`.   |

## reference

Where the schema definition document lives. Exactly one of `uri` / `inline` / `destination`.

| parameter   | optional  | type    | description                                                              |
|-------------|-----------|---------|--------------------------------------------------------------------------|
| uri         | selective | String  | Definition file location (`gs://…`): an `.avsc` file for avro, a descriptor file for protobuf. |
| inline      | selective | String  | The definition document itself (e.g. Avro schema JSON).                  |
| destination | selective | Boolean | If `true`, use the schema of the write destination (sink modules).       |

## Examples

Protobuf-encoded Pub/Sub messages:

```yaml
sources:
  - name: input
    module: pubsub
    schema:
      encoding:
        format: protobuf
        messageName: com.example.Event
      reference:
        uri: gs://my-bucket/schemas/event.pb
    parameters:
      subscription: projects/myproject/subscriptions/mysubscription
      format: protobuf
```

Avro with an inline definition:

```yaml
schema:
  encoding: { format: avro }
  reference:
    inline: '{"type":"record","name":"root","fields":[{"name":"id","type":"long"}]}'
```

Use the destination table's schema (sinks):

```yaml
schema:
  reference: { destination: true }
```

## Legacy format

The following keys are the older spelling and remain supported, but must not be mixed with
`encoding`/`reference` in the same schema block:

| legacy key                          | new form                                                   |
|-------------------------------------|------------------------------------------------------------|
| `avro: { json: <document> }`        | `encoding: {format: avro}` + `reference: {inline: …}`      |
| `avro: { file: <uri> }`             | `encoding: {format: avro}` + `reference: {uri: …}`         |
| `protobuf: { descriptorFile, messageName }` | `encoding: {format: protobuf, messageName}` + `reference: {uri: …}` |
| `useDestinationSchema: true`        | `reference: {destination: true}`                           |
| `avroSchema` / `protobufDescriptor` | deprecated aliases of `avro.file` / `protobuf.descriptorFile` |
