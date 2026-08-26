# Adding an Action Service

An action executes an operation against an external service (run a job, write a result artifact,
send a notification, wait for a queue to drain) as a lightweight workflow step. Actions are the
fourth module kind next to sources / transforms / sinks and have their own config section:

```yaml
actions:
  - name: load
    module: bigquery          # the action service (what you add)
    operation: jobs.load      # which operation, from the service's declared list — module-level, not a parameter
    trigger: collect          # once (default) | perElement | collect — module-level, not a parameter
    inputs: [store]           # optional: signals (once) or the elements (perElement/collect)
    parameters: { sourceUrisField: path, destinationTable: p.d.t }
```

**You only write the service class** — the module itself (`module/Action.java`: trigger topologies,
envelope output, failure routing, `ActionConfig`) already exists. No registry work.

## Contract

```java
package com.mercari.solution.module.action;           // MUST be in this package (scanned at startup)

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Action;
import com.mercari.solution.module.Action.Trigger;
import com.mercari.solution.module.*;

@Action.Service(name = "myservice",                    // config: actions[].module: myservice
        operations = {"things.create", "things.delete"}) // omit for a single-operation service
public class MyServiceAction implements ActionService { // implements the SPI — does NOT extend Module

    public static class Parameters implements Serializable { /* service parameters */ }

    private Trigger trigger;
    private Parameters parameters;

    @Override
    public void configure(String name, Trigger trigger, String operation, JsonObject parametersJson,
                          PipelineOptions options, Schema inputSchema) {
        // assembly time: deserialize, validate (throw IllegalModuleException with accumulated
        // messages), apply defaults. The instance is serialized into the DoFn afterwards,
        // so all remaining state must be Serializable (or transient).
        this.trigger = trigger;                        // if the service is trigger-aware
        // operation: already validated against `operations`; null for single-operation services.
        // Branch validation/execution on it (see BigQueryAction.Op.of / TasksAction.Op.of).
        this.parameters = new Gson().fromJson(parametersJson, Parameters.class);
        // inputSchema: union schema of the step's inputs (null without inputs) — for services
        // that compile element templates at assembly time (see HttpAction)
        // validate + setDefaults ...
    }

    @Override
    public void setup() { /* worker-side init (clients), fields transient */ }

    @Override
    public ActionResult execute(List<MElement> elements) throws Exception {
        // elements by trigger: once -> empty (pure signal), perElement -> one, collect -> all
        // do the work, then return the envelope contents (never null):
        return ActionResult.of(operation, jobIdOrPath, state, payloadJson);
    }
}
```

Key differences from data modules:

- **Lifecycle**: `configure` (assembly, validate/defaults) → `setup` (worker) → `execute` (per firing).
- **`trigger` and `operation` are module-level fields** (`ActionConfig`), handed to `configure`;
  `parameters.trigger` / `parameters.op` / `parameters.operation` are rejected by the framework. Don't
  name a service parameter `trigger` or `op`. Declare operations in `@Action.Service(operations=…)`:
  none for a single-operation service, the backing API's `resource.method` names when it wraps an API
  with several resources (`queues.pause`, `jobs.load`), plain verbs otherwise. Pass the `operation`
  string through to `ActionResult.of(operation, …)` so the envelope reports it.
- **Output**: return an `ActionResult` — the framework wraps it into the common envelope
  `(service, operation, jobId, state, startedAt, finishedAt, payload)`. Never return null.
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
- `HttpAction.java` — generic HTTP request per firing on the shared `outbound` core (`RequestRenderer`,
  `SyncCaller`, `ResponsePolicy`), schema-aware `configure` (uses `inputSchema`), `poll` loop.
- `TasksAction.java` — custom client boundary (`QueueClient` interface, `endpoint: memory://…` for
  unit tests, emulator IT).
- `src/test/java/com/mercari/solution/module/action/MockAction.java` — minimal test service.

## Test

Config-driven e2e in `src/test/java/com/mercari/solution/module/` — see `ActionModuleTest`:
declare the step under `actions:` in a config text block, run `MPipeline.apply`,
`PAssert` on the envelope fields (`service`/`operation`/`jobId`/`state`/`payload`). Cover the triggers
your service treats specially, plus validation errors. External services: Testcontainers IT
(`ActionBigQueryIT` with the BigQuery emulator is the model).

## Config docs

1. Create `src/main/resources/server/docs/module/action/<service>.md` — front-matter `title:`,
   idempotency/templating notes, an "Operations" table when the service declares operations,
   parameters table (service parameters only — `trigger` / `operation` are documented in the concept
   page), YAML examples under `actions:`. Link the concept page
   `README.md` in the same directory for section layout / trigger / envelope semantics.
2. Register in `module/index.yaml` under the `actions:` key with `title: <service>` — the Builder
   UI lists these entries in its "Actions" group.
