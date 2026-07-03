# Adding a Sink Module

A sink writes input collections to an external system. Sinks still return an `MCollectionTuple` — the
returned collection (e.g. write results) is registered under the sink's name, so other modules can `waits:`
on it.

## Contract

```java
package com.mercari.solution.module.sink;

import com.mercari.solution.module.*;

@Sink.Module(name="myservice")
public class MyServiceSink extends Sink {

    private static class Parameters implements Serializable { /* ... */ }

    @Override
    public MCollectionTuple expand(final MCollectionTuple inputs, final MErrorHandler errorHandler) {
        final Parameters parameters = getParameters(Parameters.class).validate().setDefaults();

        final PCollection<MElement> input = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));
        final Schema inputSchema = Union.createUnionSchema(inputs);

        // ... write to the external system ...
        return MCollectionTuple.of(writeResults, resultSchema);
    }
}
```

## Sink-specific base-class accessors

- `getSchema()` — optional user-supplied `schema` from config (e.g. destination table schema override).
- `getStrategy()` — windowing strategy; pass to `Union.flatten()`.
- `getUnion()` — `Union.Parameters` for input-merging behavior (defaults provided).
- `getInputNames()` — configured input names.
- `getOutputType()` / `getFailFast()` / `getLoggings()` — as in all modules.

## Writing patterns

- **Beam IO connector** (BigQuery, Spanner, PubSub, Jdbc…): translate `MElement` to the connector's type
  using `util/schema/converter/`, apply the IO, and surface the connector's failure/dead-letter output into
  `errorHandler` where available (see `SpannerSink`, `BigQuerySink` for failure wiring).
- **Custom DoFn writer** (HTTP APIs, per-record files): same DoFn `outputTag`/`failureTag` pattern as
  transforms — catch per-element errors, `processError(...)`, `errorHandler.addError(...)`
  (see `FilesSink`, `AuxiaSink`).
- Destination values (table names, paths, row keys) commonly support **FreeMarker templates** evaluated
  per record — follow existing sinks (`BigtableSink`, `FilesSink`) via `TemplateUtil` if the new sink
  needs dynamic destinations.

## Output

Return something meaningful when possible (write results, counts); downstream `waits:` semantics depend on
it. If there is nothing natural to return, follow `DebugSink`'s pattern for constructing the returned
tuple.

## Reference implementations

- `DebugSink.java` (~240 lines) — simplest full sink; also shows DirectRunner-only local output.
- `ActionSink.java` / `TasksSink.java` — small, action-style sinks.
- `FilesSink.java` — per-record file writing with templated paths.
- `SpannerSink.java` — mutations, failure handling, emulator support.
- `PubSubSink.java` — serialization formats + dynamic topics.

## Config docs

Create `src/main/resources/server/docs/module/sink/<name>.md` (front-matter `title:`, parameters table,
YAML example with `inputs:`) and register in `module/index.yaml` under `sinks:`.
