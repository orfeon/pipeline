# Adding a Source Module

A source reads data from an external system and starts the pipeline from `PBegin`.

## Contract

```java
package com.mercari.solution.module.source;

import com.mercari.solution.module.*;

@Source.Module(name="myservice")
public class MyServiceSource extends Source {

    private static class Parameters implements Serializable { /* ... */ }

    @Override
    public MCollectionTuple expand(final PBegin begin, final MErrorHandler errorHandler) {
        final Parameters parameters = getParameters(Parameters.class).validate().setDefaults();
        // build PCollection<MElement> from begin
        return MCollectionTuple.of(output, outputSchema);
    }
}
```

## Source-specific base-class accessors

- `getSchema()` — the user-supplied `schema` from config (may be null). Sources that cannot infer a schema
  from the external system require it (validate accordingly); sources that can infer (Avro/Parquet, BQ
  tables) use it as an override. Call `schema.setup()` is already done by the base class.
- `getMode()` — `Source.Mode`: `batch` / `streaming` / `microBatch` / `changeDataCapture` / `view`.
  Defaults to `streaming` when the pipeline runs streaming, else `batch`. Branch on this if the source
  supports multiple modes.
- `getTimestampAttribute()` / `getTimestampDefault()` — field/attribute to use as event time. If set,
  assign each element's event time from that field.

## Output

- Determine the **output `Schema`** first — from `getSchema()`, from the external system's metadata, or
  from parameters — and return it with the collection: `MCollectionTuple.of(collection, schema)`.
- Emit `MElement` with event time set. For batch reads without a natural timestamp, use the read time or
  `timestampDefault`.
- Use `ElementCoder.of(schema)` when applying `Create`/custom transforms that need an explicit coder.
- Multiple named outputs: `MCollectionTuple.of(main, mainSchema).and("tag", other, otherSchema)` — they
  become addressable as `<name>.tag` in config.

## Failure handling

Seed-based or request-based sources (see `HttpSource`) collect per-request failures into a
`TupleTag<BadRecord>` and call `errorHandler.addError(outputs.get(failureTag))`. IO-connector-based sources
usually rely on the connector's own error semantics.

## Reference implementations

- `HttpSource.java` (~80 lines) — minimal: Parameters validate/setDefaults, seed `Create`, util transform,
  failure tag. **Best starting template.**
- `CreateSource.java` — schema construction, `select` post-processing, both batch and streaming.
- `PubSubSource.java` — streaming source: formats (json/avro/protobuf), attributes, timestamp handling.
- `JdbcSource.java` / `PostgresSource.java` — DB-backed batch source with query splitting.
- `StorageSource.java` — file formats with schema inference.

## Config docs example skeleton (`src/main/resources/server/docs/module/source/myservice.md`)

````markdown
---
title: MyService source module
---

# MyService source module

Reads records from MyService ...

## Parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| endpoint | required | String | ... |

## Example

```yaml
sources:
  - name: myservice
    module: myservice
    parameters:
      endpoint: https://...
```
````
