# Adding a Transform Module

A transform consumes one or more named input collections (`MCollectionTuple`) and produces output
collections.

## Contract

```java
package com.mercari.solution.module.transform;

import com.mercari.solution.module.*;
import com.mercari.solution.module.Transform.Module;

@Module(name="mytransform")   // or spell out @Transform.Module(name="mytransform")
public class MyTransform extends Transform {

    private static class Parameters implements Serializable { /* ... */ }

    @Override
    public MCollectionTuple expand(final MCollectionTuple inputs, final MErrorHandler errorHandler) {
        final Parameters parameters = getParameters(Parameters.class).validate().setDefaults();

        // Merge multiple inputs into one collection with a unified schema:
        final PCollection<MElement> input = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));
        final Schema inputSchema = Union.createUnionSchema(inputs);

        // ... process ...
        return MCollectionTuple.of(output, outputSchema);
    }
}
```

**Start from `ExampleTransform.java`** (`module/transform/ExampleTransform.java`) — it is a maintained,
commented template covering the full canonical pattern: Union of inputs, output schema definition,
DoFn with `outputTag`/`failureTag`, `processError(...)`, `Logging.log(...)`, and
`errorHandler.addError(...)`. Copy it and adapt.

## Transform-specific base-class accessors

- `getStrategy()` — windowing/triggering strategy from config (`Strategy`); pass to `Union.flatten()`.
- `getInputs()` — the configured input names (order preserved). Use when inputs must be treated
  separately (join, compare) instead of flattened — access per-name collections via the `inputs` tuple.
- `getSideInputs()` — `List<MCollection>` configured as `sideInputs`; convert to Beam side inputs for
  lookup-style enrichment (see `LookupTransform`, `HttpTransform`).

## Handling inputs

Two idioms:
1. **Flatten** (most transforms): `Union.flatten().withWaits(getWaits()).withStrategy(getStrategy())` +
   `Union.createUnionSchema(inputs)` — treats all inputs as one stream with a merged schema.
2. **Per-input** (joins/compares): iterate `inputs.getAll()` / access by tag to keep inputs distinct
   (see `BeamSQLTransform`, `CompareTransform`).

## Output

- Define the output `Schema` explicitly (`Schema.builder()...build()`), or derive it from `inputSchema`
  when passing fields through.
- Multiple named outputs (e.g. per-partition): `MCollectionTuple.of(...).and("tag", collection, schema)` —
  downstream modules address them as `<name>.tag` (see `PartitionTransform`).

## Failure handling (canonical DoFn pattern)

```java
final TupleTag<MElement> outputTag = new TupleTag<>() {};
final TupleTag<BadRecord> failureTag = new TupleTag<>() {};
final PCollectionTuple outputs = input.apply("Process", ParDo
        .of(new MyDoFn(getLoggings(), getFailFast(), failureTag))
        .withOutputTags(outputTag, TupleTagList.of(failureTag)));
errorHandler.addError(outputs.get(failureTag));
```

Inside the DoFn, wrap per-element work in try/catch and on error:
`c.output(failureTag, processError("Failed to ...", input, e, failFast));`

## Reference implementations

- `ExampleTransform.java` — the template (see above).
- `FilterTransform.java` (~100 lines) — minimal real transform.
- `PartitionTransform.java` — multiple tagged outputs.
- `LookupTransform.java` — side inputs.
- `BeamSQLTransform.java` — per-input handling (join).

## Config docs

Create `src/main/resources/server/docs/module/transform/<name>.md` (front-matter `title:`, parameters
table, YAML example with `inputs:`) and register in `module/index.yaml` under `transforms:`.
