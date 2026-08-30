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

## Location

Write the schema block inside the module's `parameters`:

```yaml
sources:
  - name: input
    module: pubsub
    parameters:
      subscription: projects/myproject/subscriptions/mysubscription
      format: json
      schema:
        fields:
          - { name: user_id, type: string }
```

The older location — `schema` at the module top level, next to `parameters` — keeps working but is
deprecated (a warning is logged). Declaring both locations is an error, and `parameters.schema` on
a module that does not consume a schema is an error.

Modules that accept a schema: sources `pubsub`, `kafka`, `storage`, `bigtable`, `datastore`,
`firestore`, `iceberg`, `jdbc`, `create`; sinks `pubsub`, `storage`. Other modules infer their
schema from the service or from their input and do not take a schema declaration.

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
    - { name: weights, type: matrix, shape: [2, 3] }
```

| parameter | optional | type   | description                                                                   |
|-----------|----------|--------|-------------------------------------------------------------------------------|
| name      | required | String | Field name.                                                                   |
| type      | required | Enum   | `bool`,`string`,`json`,`bytes`,`int32` (`int`),`int64` (`long`),`float32` (`float`),`float64` (`double`),`decimal`,`date`,`time`,`timestamp`,`enum`,`map`,`element`,`matrix`, … |
| mode      | optional | Enum   | `nullable` (default), `required`, `repeated`.                                 |
| fields    | selective | Array | Nested fields (for `element` type).                                           |
| symbols   | selective | Array<String\> | Enum symbols (for `enum` type).                                       |
| defaultValue | optional | Primitive | Value substituted when the field is missing or null. For `repeated` mode it replaces null elements inside the array. |
| shape     | selective | Array<Integer\> | Dimensions (for `matrix` type). Required; positive integers.         |
| valueType | selective | Enum   | Element type (for `matrix` type): a numeric type. Default: `float64`.         |
| description | optional | String | Human-readable description of the field. Shown in the dry-run output schema (`spec.modules[].schema`) and written to destinations that support it (see below). |
| options   | optional | Map<String,String\> | Free-form key/value metadata attached to the field. Shown in the dry-run output schema. |

### Field descriptions

The `schema` block itself also accepts a `description` (the table / view / record description).
Sources fill it from the destination metadata the same way as field descriptions (BigQuery table
description, jdbc table comment, Avro record `doc`); it appears as `description` of the module's
output schema in the dry-run output. It is not carried into schemas derived by other modules, so a
description always refers to the module that read it.

Field descriptions are metadata: they never affect how data is read, converted or written, but
they travel with the schema so that the dry-run output (Pipeline Builder, `run-pipeline` with `dryRun` /
`run-pipeline` MCP tools) can show what each column means. They come from:

- the `description` key of a declared `schema.fields` entry (any module);
- the source table's metadata, for sources whose destination stores it:
  `bigquery` (table or view read: BigQuery field descriptions; not available for `query` reads),
  `jdbc` / `postgres` / `tidb` (`table` read: column comments, i.e. JDBC `REMARKS`),
  and Avro-based inputs (`storage` / `files` avro or parquet, `pubsub` with an Avro schema: the field `doc`).

Descriptions are kept by modules that pass fields through unchanged (`union`, `partition`,
`reshuffle`, aggregation keys, `onnx`) and dropped by modules that rebuild their output schema
(`select`, `beamsql`, `query`). They are written to destinations that store them: the `bigquery`
sink (field descriptions of a table auto-created with `createDisposition: CREATE_IF_NEEDED`) and
Avro output (`storage` / `files` avro, and parquet written via parquet-avro, as the field `doc`).

A `matrix` field holds its values **flat in row-major order**; the shape lives in the schema
(the same representation as ONNX tensor outputs and the select module's `reshape` function).
JSON input accepts both nested rows (`[[2, 1], [1, 3]]`) and the flat form (`[2, 1, 1, 3]`);
the element count must match the shape. The select module's matrix functions
(`matrix_solve` etc. via `matrixField`) read the 2D shape from the schema automatically; in the
query module the field surfaces as a flat `ARRAY<DOUBLE>` — pass the column count explicitly
(`MATRIX_SOLVE(mat, vec, 2)`).

The shape survives Avro round trips: matrix fields are written as flat arrays whose Avro schema
carries `logicalType: matrix` and `shape` props, and the `storage` source restores the matrix
type automatically when reading such files (avro, or parquet with the declared/embedded Avro
schema). Files written by other systems without these props read as plain flat arrays.

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
| destination | selective | Boolean | If `true`, use the schema of the write destination. Sink modules only — declaring it on a source module is an assembly-time error. |

## Examples

Protobuf-encoded Pub/Sub messages:

```yaml
sources:
  - name: input
    module: pubsub
    parameters:
      subscription: projects/myproject/subscriptions/mysubscription
      format: protobuf
      schema:
        encoding:
          format: protobuf
          messageName: com.example.Event
        reference:
          uri: gs://my-bucket/schemas/event.pb
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
`encoding`/`reference` in the same schema block. See the
[Schema Migration Guide](schema-migration.md) — the MCP `upgrade-config` tool rewrites configs
automatically:

| legacy key                          | new form                                                   |
|-------------------------------------|------------------------------------------------------------|
| `avro: { json: <document> }`        | `encoding: {format: avro}` + `reference: {inline: …}`      |
| `avro: { file: <uri> }`             | `encoding: {format: avro}` + `reference: {uri: …}`         |
| `protobuf: { descriptorFile, messageName }` | `encoding: {format: protobuf, messageName}` + `reference: {uri: …}` |
| `useDestinationSchema: true`        | `reference: {destination: true}`                           |
| `avroSchema` / `protobufDescriptor` | deprecated aliases of `avro.file` / `protobuf.descriptorFile` |
