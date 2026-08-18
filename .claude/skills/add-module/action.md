# Adding an Action Service

An action executes an operation against an external service (run a job, write a result artifact,
send a notification) as a lightweight workflow step. Actions are a module **kind orthogonal to
placement**: one implementation is registered as `action.<service>` in all three module registries
and is placeable in `sources` / `transforms` / `sinks` — placement never changes behavior, it only
tells the config reader where the step sits in the flow.

**You only write the service class** — the three position adapters (`ActionSource` /
`ActionTransform` / `ActionSink`), trigger topologies, envelope output and failure routing already
exist in `module/action/Actions.java`. No adapter or registry work.

## Contract

```java
package com.mercari.solution.module.action;           // MUST be in this package (scanned at startup)

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.mercari.solution.module.*;

@Action.Service(name = "myservice")                    // registered as module `action.myservice`
public class MyServiceAction implements Action {       // implements Action — does NOT extend Module

    public static class Parameters implements Serializable { /* flat config parameters */ }

    private Trigger trigger;
    private Parameters parameters;

    @Override
    public void configure(String name, JsonObject parametersJson, PipelineOptions options) {
        // assembly time: deserialize, validate (throw IllegalModuleException with accumulated
        // messages), apply defaults. The instance is serialized into the DoFn afterwards,
        // so all remaining state must be Serializable (or transient).
        this.trigger = Trigger.of(parametersJson);     // if the service is trigger-aware
        this.parameters = new Gson().fromJson(parametersJson, Parameters.class);
        // validate + setDefaults ...
    }

    @Override
    public void setup() { /* worker-side init (clients), fields transient */ }

    @Override
    public ActionResult execute(List<MElement> elements) throws Exception {
        // elements by trigger: once -> empty (pure signal), perElement -> one, collect -> all
        // do the work, then return the envelope contents (never null):
        return ActionResult.of("<op>", jobIdOrPath, state, payloadJson);
    }
}
```

Key differences from data modules:

- **Lifecycle**: `configure` (assembly, validate/defaults) → `setup` (worker) → `execute` (per firing).
- **Parameters are flat**: `trigger` and the service's own parameters sit side by side in the config
  `parameters` object (no nested per-service block). Don't name a parameter `trigger`.
- **Output**: return an `ActionResult` — the framework wraps it into the common envelope
  `(service, op, jobId, state, startedAt, finishedAt, payload)`. Never return null.
- **Failures**: just throw from `execute` — the framework routes the firing to `BadRecord` /
  `failureSinks` honoring `failFast`. Throw on job failure and wait timeout too.

## Trigger handling in the service

The framework decides *when* `execute` fires; the service decides *what the elements mean*:

- `once` — `elements` is empty; run purely from configured parameters.
- `perElement` — expand `${field}` FreeMarker templates in templatable string parameters with
  `elements.getFirst().asPrimitiveMap()` (see `BigQueryAction.templateParameters`).
- `collect` — template context is `Action.createCollectTemplateData(elements)` = `elements`
  (list of field maps) + `size`; FreeMarker list directives work. Service-specific aggregation
  parameters are welcome where they beat templating (e.g. `BigQueryAction.sourceUrisField` gathers
  one field from every element into `sourceUris`).

Use `TemplateUtil.isTemplateText` / `executeStrictTemplate`. If templating makes no sense for the
service (e.g. `GeminiAction`), ignore `elements` and say so in the docs.

## Execution guarantees (design these in)

- **At-least-once**: Beam may retry a bundle and re-invoke `execute`. Make submission idempotent
  where the API allows a client-supplied id — see `BigQueryAction`: deterministic id from
  (pipeline jobName, step name, effective parameters hash), 409 ALREADY_EXISTS → adopt the existing
  job. If the API has no such id, state clearly in docs that retries may duplicate.
- **Waiting**: poll with `ExponentialBackOff` + a `timeoutSeconds` parameter (default 86400);
  timeout and failed terminal states throw. Default `wait: true`.
- **No try/finally** in pipelines: cleanup-style actions never run if the pipeline fails earlier —
  note it in docs if the service mutates infrastructure.

## Two-plane rule (affects docs you write)

Sink outputs and action envelopes are **control records**: only action `inputs` and anyone's
`waits` should consume them (data transforms/sinks doing so get an assembly-time warning).
Actions are the bridge — their `inputs` accept both data records (perElement parameterization)
and control records (chaining on results). See `docs/module/action/README.md` for the model.

## Reference implementations

- `BigQueryAction.java` — the full pattern: trigger-aware templating, idempotent submission,
  polling with backoff/timeout, collect aggregation parameter.
- `StorageAction.java` (~130 lines) — simplest complete service (file writing, template content).
- `GeminiAction.java` — REST submission + state polling, non-idempotent (documented).
- `src/test/java/com/mercari/solution/module/action/MockAction.java` — minimal test service.

## Test

Config-driven e2e in `src/test/java/com/mercari/solution/module/` — see `ActionModuleTest`:
wire `module: action.<service>` in a config text block (any section), run `MPipeline.apply`,
`PAssert` on the envelope fields (`service`/`op`/`jobId`/`state`/`payload`). Cover the triggers
your service treats specially, plus validation errors. External services: Testcontainers IT
(`ActionBigQueryIT` with the BigQuery emulator is the model).

## Config docs

1. Create `src/main/resources/server/docs/module/action/<service>.md` — front-matter `title:`,
   idempotency/templating notes, parameters table (include `trigger`), YAML examples. Link the
   concept page `README.md` in the same directory for placement/trigger/envelope semantics.
2. Register in `module/index.yaml` under the `actions:` key with `title: action.<service>` —
   the server appends these entries to the sources/transforms/sinks catalogs for the Builder UI.
