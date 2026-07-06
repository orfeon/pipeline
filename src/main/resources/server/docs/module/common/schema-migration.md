---
type: Common
title: Schema Migration Guide
description: How to migrate pipeline configs from the legacy schema declarations (top-level schema, avro/protobuf/useDestinationSchema keys) to the current form (parameters.schema with fields/encoding/reference).
tags: [common, schema, migration, deprecated]
timestamp: 2026-07-07T00:00:00Z
---

# Schema Migration Guide

The schema block was restructured (see [Schema](schema.md)). Legacy configs keep working, but the
old spellings are deprecated: a warning is logged at assembly, and validation responses from the
Pipeline API include the deprecations in a `warnings` attribute. The old spellings will be removed
in the next major version.

## Automatic upgrade

The bundled MCP server provides an **`upgrade-config`** tool: give it a config (YAML or JSON) and
it returns the upgraded config plus the list of changes applied. Declarations it cannot rewrite
safely (old and new keys mixed, both `avro` and `protobuf` declared, schema present in both
locations) are left unchanged with a note — resolve those manually.

## What changes

### 1. Location: `schema` moves into `parameters`

```yaml
# before                              # after
- name: input                         - name: input
  module: pubsub                        module: pubsub
  schema:                               parameters:
    fields: [...]                         subscription: ...
  parameters:                             schema:
    subscription: ...                       fields: [...]
```

### 2. Definition keys: `encoding` + `reference`

| legacy | current |
|---|---|
| `avro: { json: <document> }` | `encoding: {format: avro}` + `reference: {inline: <document>}` |
| `avro: { file: gs://... }` | `encoding: {format: avro}` + `reference: {uri: gs://...}` |
| `avroSchema: gs://...` (deprecated alias) | same as `avro.file` |
| `protobuf: { descriptorFile, messageName }` | `encoding: {format: protobuf, messageName: ...}` + `reference: {uri: ...}` |
| `protobufDescriptor: gs://...` (deprecated alias) | same as `protobuf.descriptorFile` |
| `useDestinationSchema: true` | `reference: {destination: true}` (sinks only) |
| pubsub sink `parameters.useDestinationSchema: true` | `parameters.schema.reference.destination: true` |

`fields` is unchanged — it remains the logical shape of the output, and (Phase 4 onward) a subset
of the input schema acts as a projection.

### 3. Format derivation (pubsub)

With `schema.encoding.format` declared, `parameters.format` may be omitted — it is derived from
the encoding. Declaring both with different values currently warns (`parameters.format` wins) and
will become an error.

## Rules to keep in mind

- Old and new keys must not be mixed in one schema block (assembly-time error).
- `schema` must not be declared both at the module top level and in `parameters.schema`
  (assembly-time error).
- `parameters.schema` on a module that does not consume a schema is an assembly-time error;
  the same declaration at the deprecated top level only warns (until the next major version).
- `reference.destination` is for sink modules only; on a source it is an assembly-time error.
