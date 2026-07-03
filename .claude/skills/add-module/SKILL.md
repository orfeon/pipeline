---
name: add-module
description: Add a new pipeline module (source, transform, or sink) to Mercari Pipeline. Use when asked to create/add a new module, connector, or integration (e.g. "add a redis source", "create a slack sink", "add a dedup transform"). Guides implementation, tests, and agent-readable docs.
---

# Add a New Pipeline Module

Add a source, transform, or sink module to Mercari Pipeline. Modules are auto-discovered by package
scanning — a new module is: **one annotated class + one test + docs**. No manual registration.

## Step 0 — Determine type and name

- **Type**: `source` (reads external data, starts from `PBegin`), `transform` (processes `MCollectionTuple`
  inputs), or `sink` (writes to external system). If ambiguous from the request, ask the user.
- **Registered name**: lowercase, short (e.g. `bigquery`, `select`, `pubsub`). This is what users write in
  config `module:` fields, and it must match the doc filename (the server agent lowercases the name when
  looking up docs).
- **Class name**: `<PascalName>Source` / `<PascalName>Transform` / `<PascalName>Sink`.
- Check the name is not already taken:
  `grep -rhoE '@(Source|Transform|Sink)\.Module\([^)]*\)' src/main/java | sort -u`

## Step 1 — Read the type-specific guide

Read the matching reference file in this skill directory **before writing code**:

- Source → [source.md](source.md)
- Transform → [transform.md](transform.md)
- Sink → [sink.md](sink.md)

Also read 1–2 existing modules similar to the new one (each guide lists good references), and
`src/main/java/com/mercari/solution/module/transform/ExampleTransform.java` — a commented template showing
the canonical DoFn / failure-handling / logging pattern that applies to all three types.

## Step 2 — Implement

All module types share this skeleton (details per type in the guides):

```java
package com.mercari.solution.module.<type>;          // MUST be in this package (scanned at startup)

@<Base>.Module(name="<registeredname>")               // nested annotation of the base class
public class <PascalName><Base> extends <Base> {

    private static class Parameters implements Serializable {
        // fields deserialized from config `parameters` (Gson)
        private Parameters validate() { /* collect errorMessages, throw IllegalArgumentException */ }
        private Parameters setDefaults() { /* fill nulls */ }
    }

    @Override
    public MCollectionTuple expand(final <Input> input, final MErrorHandler errorHandler) {
        final Parameters parameters = getParameters(Parameters.class).validate().setDefaults();
        // ... build Beam transforms producing PCollection<MElement> ...
        // route failures: errorHandler.addError(failures)
        return MCollectionTuple.of(output, outputSchema);
    }
}
```

Shared conventions (all types):

- **Parameters**: inner `Parameters implements Serializable` + `getParameters(Parameters.class)`; validate
  with accumulated error messages (`parameters.xxx must not be empty` style).
- **Elements**: emit `MElement` (see `MElement.builder()`); **event time is required**
  (`.withEventTime(...)`); map-based construction takes timestamps in **microseconds** (`millis * 1000L`).
- **Output schema**: every output collection is paired with a `Schema`
  (`MCollectionTuple.of(collection, schema)`); build with `Schema.builder().withField(...)`.
- **Failures**: in DoFns, catch per-element errors → `processError(message, input, e, getFailFast())` →
  output `BadRecord` to a `failureTag`; then `errorHandler.addError(outputs.get(failureTag))`.
- **Logging**: accept `getLoggings()` and use `Logging.log(LOG, logs, "input"/"output", element)`.
- **Base-class accessors** available inside `expand`: `getName()`, `getWaits()`, `getFailFast()`,
  `getLoggings()`, `getOutputType()`, `getRunner()`, plus type-specific ones (see guides).

## Step 3 — Test

Add `src/test/java/com/mercari/solution/module/<type>/<PascalName><Base>Test.java`. Tests are JUnit 5
(`org.junit.jupiter.api.Test` / `Assertions` — no JUnit4). The established pattern (see
`FilterTransformTest`, `CreateSourceTest`) is a **config-driven end-to-end test**:

1. `private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);`
2. Write a config JSON text block using the `create` source to generate input elements, wiring your module
   in `transforms:`/`sinks:` (or directly in `sources:`).
3. `Config config = Config.load(configJson);` then `Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);`
4. `PAssert.that(outputs.get("<name>").getCollection()).satisfies(...)`, then `pipeline.run();`

Run: `mvn test -Dtest=<PascalName><Base>Test`

## Step 4 — Documentation (required)

User-facing docs are read by the bundled AI agent from the classpath — this is the canonical location:

1. Create `src/main/resources/server/docs/module/<type>/<registeredname>.md`:
   - YAML front-matter with `title:` (the agent's `listModules` displays it).
   - Self-contained: what it does, all `parameters` (name/type/required/description table), at least one
     complete YAML config example. Model it on an existing file in the same directory.
2. Register in `src/main/resources/server/docs/module/index.yaml`: add `title` / `description` / `tags`
   under the matching `sources:` / `transforms:` / `sinks:` key. Write a real description (1–3 sentences,
   capabilities + supported options), not a placeholder.

## Step 5 — Verify

- `mvn test -Dtest=<PascalName><Base>Test` passes.
- Discovery works: the e2e test itself proves it (module resolved by `module:` name). If the annotation is
  missing, package scanning throws at startup ("must have Module annotation").
- Doc file name exactly matches the registered name (lowercase).

## Pitfalls

- `expand()` must apply at least one transform that consumes its inputs. Returning an empty tuple
  (`MCollectionTuple.done(...)`) without applying anything leaves an empty composite node, which
  **never completes on DirectRunner and hangs the pipeline** (see DebugSink's workDir fallback).

- The annotation is the **nested** one (`@Source.Module` / `@Transform.Module` / `@Sink.Module`) — there is
  no shared top-level `@Module`.
- The class must live directly under `com.mercari.solution.module.<type>` (subpackages are scanned too:
  `getTopLevelClassesRecursive`), and every `Source`/`Transform`/`Sink` subclass in those packages **must**
  be annotated or startup fails.
- Forgetting `.withEventTime(...)` on built `MElement`s causes runtime failures.
- `failFast` defaults to `true` in batch and `false` in streaming — don't assume one mode.
- Don't add docs under `docs/config/module/` — that tree is legacy; `src/main/resources/server/docs/` is
  canonical.
