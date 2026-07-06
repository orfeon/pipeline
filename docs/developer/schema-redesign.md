# Schema Redesign (Design Document)

Status: **Accepted — Phase 0 done (characterization tests; storage sampling IT pending),
Phase 1 done (`Encoding`/`Reference` normalization + staged setup pipeline in `Schema`),
Phase 2 done (`encoding:`/`reference:` accepted by `Schema.parse`; old+new mixing rejected;
user docs at `server/docs/module/common/schema.md`),
Phase 3 done (`parameters.schema` canonical; top-level fallback warns; support declared via
`@…Module(schema=true)`; `parameters.schema` on unsupported modules errors — top-level on
unsupported modules stays a warning until Phase 5 for backward compatibility),
Phase 4 in progress (storage done: fields projection unified across avro/parquet with
assembly-time validation — see the storage row in §3; pubsub done: `parameters.format` derived
from `schema.encoding.format`, `reference.destination` works on the sink — destination-only
schema declarations parse into a placeholder resolved from the topic,
`parameters.useDestinationSchema` deprecated, sink protobuf validation NPE fixed; Confluent
runtime registry remains a separate PR; bigtable done: `CellEncoding` component — the shared
encoding/reference vocabulary at cell granularity with the same cascade, legacy `format` keys
aliased, `encoding.reference` supplies the avsc for avro cells)**
Scope: the `schema` configuration block, its position in module config, and the internal `Schema` model
(`com.mercari.solution.module.Schema`).

This document is the single source of truth for the schema refactoring. Each implementation PR should
reference the relevant section rather than restating the design.

---

## 1. Problems in the current design

1. **`schema` sits in the wrong layer.** It is defined on `SourceConfig` / `TransformConfig` / `SinkConfig`
   (outside `parameters`), alongside DAG-wiring and framework-policy fields — yet its meaning differs per
   module (Avro decode instruction for `pubsub`, generation definition for `create`, cell mapping for
   `bigtable`, type override for `jdbc`). Modules that do not consume it silently ignore it.
2. **The schema block mixes three concerns.** `fields` (logical shape), `avro`/`protobuf` (wire format
   definitions, each embedding its own inline-vs-file source choice), and `useDestinationSchema`
   (a provenance flag) live side by side with implicit precedence.
3. **Implicit fallback chain.** `Schema.setup()` derives missing representations through a
   fields→avro→protobuf→row fallback web. What happens when both `fields` and `avro` are present is
   defined only by code, not by documented rules. Concrete pinned example
   (`SchemaParseTest.testParseDeprecatedAvroSchemaAliasWithFieldsIsShadowed`): `fields` + a file-based
   avro definition silently discards the file reference — `getAvro()` re-derives from `fields` whenever
   `avro.json` is null.
4. **Duplicate shape descriptions.** `StorageSource` accepts both a config-level `schema` (used as the
   reader schema, replacing the sampled writer schema entirely) and `parameters.fields` (name-list
   projection) — two places describing the output shape. Pinned behavior: the `fields` projection is
   applied **only for parquet** (`createParquetRead`, building block pinned in
   `AvroSchemaUtilTest.testToBuilderProjection`); for avro format `parameters.fields` is silently
   ignored. Non-selected parquet columns are kept but forced nullable (`createNullableSchema`).
5. **No room for multiplicity or registries.** A single top-level `schema` cannot express Kafka key/value
   schemas, per-cell Bigtable encodings, or schema-registry references (build-time or runtime).

## 2. Design principles

### P1 — Interpretation locus decides the layer

> A field belongs to the common (top-level) module config **only if the framework interprets it with the
> same meaning for every module**. Anything whose meaning the module defines goes in `parameters`,
> no matter how common it is.

Under this criterion the common layer holds exactly two groups:

| Group | Fields |
|---|---|
| DAG wiring | `name`, `module`, `inputs`, `waits`, `sideInputs`, `tags`, `ignore`, `description`, `args` |
| Execution policy (applied by the framework around the module) | `failFast`, `outputFailure`, `failureSinks`, `logs`, `outputType`, `strategy`, `union` |

`schema` in its current role (decode/definition instruction) fails the criterion → it moves into
`parameters`, owned by each module. The **representation** stays core-owned: the core provides the
`Schema` vocabulary (JSON shape + parser + components) that modules embed wherever their physical model
requires (`parameters.schema`, `parameters.keySchema`/`valueSchema`, per-qualifier `encoding`, …).

A future optional common-level `outputSchema` (a pure output **contract**, verified uniformly by the
framework for any module) does pass the criterion and may be added later; it is out of scope for the
initial phases.

### P2 — Three orthogonal axes inside `schema`

The schema block separates into three keys, each answering one question:

| Key | Question | Consumed by |
|---|---|---|
| `fields` | What is the logical shape of this module's output? | everyone |
| `encoding` | How do bytes map to/from elements? (format + format options) | modules that (de)serialize bytes |
| `reference` | Where does the schema document come from? (inline / file / registry / destination) | resolved at assembly time |

```yaml
schema:
  fields:
    - { name: user_id, type: string }
    - { name: amount,  type: int64 }
  encoding:
    format: protobuf            # avro | protobuf | confluent-avro | json | none | (module-specific: hbase, text, …)
    messageName: com.example.Event
  reference:
    uri: gs://bucket/schemas/event.pb
    # uri: registry://sr.example.com/subjects/events-value/versions/12
    # inline: "<the definition document itself>"   (implemented in Phase 2)
    # destination: true       (use the sink target's schema; replaces useDestinationSchema)
```

Rules:

- The schema **definition** is supplied by exactly one of `fields` or `reference`. When both are present,
  `reference` supplies the definition and `fields` declares the output shape against it (see P3).
- `encoding` carries only the conversion spec, never the definition document.
- All three keys are optional; modules use only the axes their physical model needs.
- Mixing old keys (`avro`, `protobuf`, `useDestinationSchema`, …) with new keys in one schema block is an
  assembly-time error.

**Build-time vs runtime references.** A `reference` is resolved once at assembly time; `…/versions/latest`
is pinned to the concrete resolved version and logged (reproducibility). Wire formats that require
runtime registry lookups (Confluent wire format: magic byte + schema id per message) are an `encoding`
concern, not a `reference`: `encoding: { format: confluent-avro, registry: { endpoint, auth } }`.

### P3 — `fields` has one meaning: the output logical shape

> If `fields` is present, it **is** the module's output schema. When a definition source
> (`reference`, self-describing data, or an inferred schema) also exists: a subset acts as a projection,
> an incompatibility (missing field, incompatible type) is an assembly-time error where detectable,
> a runtime error / failure-routing case otherwise.

Consequences:

- "Assertion" and "projection" are not two modes — both follow from the single rule. No mode flag exists.
- For Avro this maps natively onto **reader/writer schema resolution**: a narrower `fields` compiles into
  a reader schema, so unwanted columns are skipped during decode. For columnar sources
  (BigQuery / Iceberg / Parquet) `fields` pushes down to column pruning. For Protobuf the full message is
  always materialized and `fields` drops columns post-decode (same semantics, different cost — document it).
- `fields` is limited to **names and types**. Renaming, computed columns, and conditionals remain the job
  of `select` — `fields` must not become a second select.
- Strictness knobs live outside `fields`: `schema.validation: { unknownFields: allow | error }` (later phase).

### P4 — `encoding`/`reference` are reusable components, not top-level-only keys

The granularity at which encoding applies is dictated by the module's physical model:

| Module | Granularity | Placement |
|---|---|---|
| `pubsub` | whole payload = one document | `parameters.schema.encoding` |
| `kafka` | key and value independently | `parameters.keySchema` / `parameters.valueSchema` |
| `bigtable` | per cell, with cascading defaults | `encoding` object per qualifier inside `parameters.columns`; top-level default cascades (top → family → qualifier), preserving the current `format` cascade design |

For primitive cells the decode is determined by the pair *(field `type` — logical) × (`encoding.format` —
byte convention: `hbase`, `text`, …)*, matching the current `format` + `type` split. An `encoding` object
may carry its own `reference` (e.g. a cell containing an Avro-serialized blob).

### P5 — Per-source semantics

- **Schemaless services (Datastore / Firestore / `create`)**: no `encoding`, no `reference`; `fields` is
  the sole definition and the module maps service values into it. Mismatches are runtime events — the
  behavior (null / `failureSinks` / fail) must be specified per module and uses the existing dead-letter
  vocabulary.
- **Self-describing data (Avro files via `storage`)**: with no definition supplied, the writer schema
  sampled from the file header is used as-is. A supplied `reference` acts as the **reader schema**
  (active resolution: reordering, defaults, promotions — not mere validation). `fields` then declares the
  final output shape, folded into the reader schema where possible. Application order:
  **writer (data) → reader resolution (`reference`) → `fields`**. Docs should recommend explicit
  `reference`/`fields` as the schema-evolution-safe mode (auto mode depends on whichever file was sampled).
- **Modules that do not support schema**: declaring one is an assembly-time **error**, never silently
  ignored. Support is declared on the module annotation (e.g. `@Source.Module(name = "pubsub", schema = true)`
  — exact attribute shape decided in Phase 3).

## 3. Backward-compatibility mapping

Old configs must keep working until Phase 5. `Schema.parse` normalizes old keys into the new internal
model:

| Old | New |
|---|---|
| `schema.fields` | `schema.fields` (meaning per P3) |
| `schema.avro.json` | `encoding: {format: avro}` + `reference: {uri: data:…}` (inline) |
| `schema.avro.file` | `encoding: {format: avro}` + `reference: {uri: <file>}` |
| `schema.protobuf.descriptorFile` + `messageName` | `encoding: {format: protobuf, messageName}` + `reference: {uri: <file>}` |
| `schema.useDestinationSchema` | `reference: {destination: true}` |
| `schema.avroSchema` (deprecated) | as `avro.file` |
| `schema.protobufDescriptor` (deprecated) | as `protobuf.descriptorFile` |
| top-level `schema` on module config | `parameters.schema` (framework injects + deprecation warning; Phase 3) |
| `storage` `parameters.fields` | kept as a name-only projection list (a full alias would need type-less `schema.fields`, which `Field.parse` rejects); Phase 4 unified its semantics: it now projects for avro too (was silently ignored), unknown names are assembly-time errors, and a declared `schema.fields` subset projects via the Avro reader schema. Parquet keeps its legacy output shape (all columns, non-selected forced nullable) — aligning it to subset-only would change existing output schemas, deferred to Phase 5 |
| `bigtable` `format`/`type` per qualifier | `encoding.format` + field `type` (alias; Phase 4) |

## 4. Migration plan

Invariant for every phase: **all existing configs keep working unchanged.** Compatibility is only removed
in Phase 5 (major version).

| Phase | Content | Config format |
|---|---|---|
| 0 | This document + characterization tests fixing current behavior of every schema-consuming module (config-driven e2e style: `Config.load` → `MPipeline.apply` → `PAssert`) | unchanged |
| 1 | Internal 3-way split of `Schema` (`fields` / `Encoding` / `Reference`); replace the `setup()` fallback web with a one-way pipeline: reference resolution → encoding validation → fields derivation/assertion. `Schema.parse` normalizes old JSON into the new internals | unchanged |
| 2 | `Schema.parse` additionally accepts `encoding:` / `reference:`; old+new mixing is an error; docs page + examples for the new form | additive |
| 3 | Location move: module base classes read `parameters.schema` first, top-level `schema` falls back with a deprecation warning; modules declare schema support via annotation; unsupported schema declarations become errors; kafka key/value split becomes possible | additive |
| 4 | Per-module semantics migration, one module per PR: `storage` (merge the dual shape paths; reader-schema projection), `pubsub`/`kafka` (encoding/reference split; Confluent runtime registry — new feature, kept in its own PR), `bigtable` (per-qualifier `encoding` component), `datastore`/`firestore`/`create`/`jdbc` (verification mostly). Each PR updates `src/main/resources/server/docs/module/…` + `index.yaml` | additive |
| 5 | Promote deprecation warnings into validation-API responses; publish a migration guide; add a config-upgrade endpoint/MCP tool on the Server (reuses the Phase 1–2 normalizer); remove old paths in the next major version | breaking (major) |

Ordering rationale: the riskiest work is untangling the conversion chain, so it happens first (Phase 1)
while the config format — and therefore the Phase 0 characterization tests — stay unchanged. Every later
phase is a thin mapping layer over the already-clean internals.

## 5. Open questions (not blocking Phases 0–2)

- Common-level `outputSchema` contract (P1): shape and enforcement point.
- `reference.registry` expanded form (`{type, endpoint, auth}`) vs `registry://` URI only; auth wiring.
- `schema.validation` knob surface (`unknownFields`, runtime mismatch policy defaults per module).
- Exact annotation attribute for schema support declaration (Phase 3).
