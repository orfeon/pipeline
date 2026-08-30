# Architecture (Internals)

Deep-dive companion to the root [`CLAUDE.md`](../../CLAUDE.md). This covers how a config file becomes a
running Apache Beam pipeline, the unified data model, and the cross-cutting concerns (schema conversion,
error handling, module lifecycle). Package root is `com.mercari.solution`.

## 1. Execution flow

`MPipeline.main` (`src/main/java/com/mercari/solution/MPipeline.java`):

1. Parse pipeline options and resolve the **runner** (`MPipeline.Runner`: `direct`, `dataflow`, `prism`,
   `portable`, `flink`, `spark`) via `OptionUtil.getRunner`.
2. `Config.load(config, context, format, args)` — read and template the config (see §2).
3. `Options.setOptions(...)` — apply the config's `options` onto the Beam `PipelineOptions`.
4. `Pipeline.create(...)`, then `apply(pipeline, config)` builds the graph.
5. `pipeline.run()`.

### The assembly loop (`MPipeline.apply` → `setResult`)

Modules are **not** built in file order. `apply` repeatedly walks sources, transforms, sinks and actions, building
any module whose dependencies are already available in the `outputs` map (`name → MCollection`):

- A module is buildable once all of its `inputs`, `waits`, and `sideInputs` names exist in `outputs`.
- Built module outputs are registered under `<name>` (and extra tagged outputs as additional keys), and the
  name is added to `executedModuleNames`.
- The loop continues until every module is built. If a full pass adds nothing new, it throws
  `IllegalModuleException("No input for modules: …")` — indicating a cycle, a typo in an input name, or a
  missing upstream module.
- Modules with `ignore: true` are skipped entirely; `null` entries (trailing commas) are tolerated.

This makes config authoring order-independent and lets transforms/sinks reference any upstream module by name.

Two assembly-time extensions build on this loop (`resolveInputNames` / `applyFanOutSinks` in `MPipeline`):

- **Wildcard inputs** — `inputs: ["module.*"]` waits until `module` is in `executedModuleNames`, then expands
  to every registered `module.<tag>` output (sorted; `.failures` excluded). Matching nothing throws.
- **`${input.*}` fan-out (sinks only)** — if a sink both declares a wildcard input and references the reserved
  `${input.*}` namespace in its parameters, it is instantiated once per matched input (`<sinkName>.<tag>`),
  with the expressions resolved against that input's `MCollection.getAttributes()` plus `name`/`tag` before
  `Sink.create`. Only `${input.…}` expressions are consumed (`TemplateUtil.executeInputTemplate`); all other
  `${...}` text survives for runtime templating. `MCollection`/`MCollectionTuple` carry the per-tag
  `attributes` map (e.g. `table` from the spanner source's all-tables mode) through `withSource`/merges.

### Failure fallback

`apply` wraps the build in a try/catch. If assembly throws and `system.failure.alterConfig` is set, that
alternate config is loaded and applied instead — a way to degrade gracefully when the primary config fails
to assemble.

## 2. Configuration system (`config/`)

`Config.load` accepts the config parameter in several forms (`Config.java`):

| Prefix / form                          | Source                          |
|----------------------------------------|---------------------------------|
| `gs://…`                               | Google Cloud Storage object     |
| Parameter Manager resource             | Google Cloud Parameter Manager  |
| `ar://…`                               | Artifact Registry               |
| `data:…` (base64)                      | Inline base64-encoded config    |
| plain text                             | Literal YAML/JSON body          |

Format is YAML or JSON (`Config.Format`, auto-detected when `unknown`).

**Templating.** Config text is processed with FreeMarker (`TemplateUtil`). `${args.varName}` placeholders are
resolved from `system.args` merged with runtime `--arg` values; arg values can themselves be templates.

**Imports.** `system.imports` (`base` + `files`) compose a config from multiple files.

**Module config shape.** `ModuleConfig` is the base of `SourceConfig` / `TransformConfig` / `SinkConfig` /
`ActionConfig` (config sections `sources` / `transforms` / `sinks` / `actions`). Common fields:

- `name` — unique id, used as the graph node key.
- `module` — registered module name (see §3).
- `parameters` — module-specific JSON object.
- `inputs` — upstream module names (transforms/sinks; optional for actions).
- `trigger` — actions only: `once` / `perElement` / `collect` firing semantics.
- `waits` — names that must complete before this module starts (ordering without data flow).
- `sideInputs` — names provided as Beam side inputs.
- `tags` — additional named outputs.
- `ignore` — skip this module.
- `failFast`, `outputFailure`, `failureSinks` — error routing (see §5).
- `outputType` — force the output `DataType`.
- `logs`, `args`, `description` — logging, per-module args, docs.

## 3. Module system (`module/`)

Four module kinds — `Source`, `Transform`, `Sink`, `Action` — all extend `Module<InputT>`. Each defines its
**own** nested runtime annotation used for discovery:

```java
@Source.Module(name="bigquery")
@Transform.Module(name="select")
@Sink.Module(name="spanner")
@Action.Service(name="bigquery")   // on an ActionService implementation
```

At class-load time each base class scans its package with Guava `ClassPath`
(`findSourcesInPackage("com.mercari.solution.module.source")`, and the transform/sink/action equivalents) to
build a `name → Class` registry. `Source.create` / `Transform.create` / `Sink.create` / `Action.create`
instantiate the right class for a config's `module` value and call its `expand()`.

`Action` differs in shape: it is a single concrete module (trigger topologies `once` / `perElement` /
`collect`, the common envelope output, failure routing) whose behavior is supplied by a pluggable
`ActionService` (`module/action/`, `configure` → `setup` → `execute`); the config's `module` value is the
service name. The service instance is serialized into the DoFn, so it must be `Serializable`.

To find the authoritative module list, grep the annotations:

```bash
grep -rhoE '@(Source|Transform|Sink)\.Module\([^)]*\)|@Action\.Service\([^)]*\)' src/main/java | sort -u
```

See the root `CLAUDE.md` for the current enumerated list and for the "Adding a New Module" steps.

## 4. Unified data model

The framework passes a single element type through Beam so every module interoperates regardless of the
underlying storage format.

- **`MElement`** — universal element wrapping a value plus its `DataType`.
- **`DataType`** — the backing representations: `ROW` (Beam Row), `AVRO` (Avro `GenericRecord`),
  `STRUCT` (Spanner), `DOCUMENT` (Firestore), `ENTITY` (Datastore), `MESSAGE` (Pub/Sub), `JSON`, and others.
- **`Schema`** — a unified schema description independent of `DataType`.
- **`MCollection`** — `PCollection<MElement>` bundled with its `Schema` and metadata (source name, `DataType`).
- **`MCollectionTuple`** — a container of named `MCollection`s (a module can emit several tagged outputs).

Conversions between representations live in `util/schema/converter/` (Avro ↔ Row ↔ Entity ↔ Struct ↔
Document ↔ Proto ↔ JSON). When adding a module, prefer emitting `MElement` with a proper `Schema` rather than
a raw backing type so downstream modules and schema inference keep working.

Known constraint: Struct-backed `MElement`s (Spanner reads) rely on `SerializableCoder`, and reading a
Spanner `Struct` mutates its lazily-decoded internal state, so re-encoding the same element produces
different bytes. DirectRunner's `enforceImmutability` check false-positives on such elements (tests disable
it via `DirectOptions.setEnforceImmutability(false)`); replacing this with a dedicated Struct coder is the
long-term fix.

## 5. Error handling

- `MErrorHandler` — created per pipeline (`createPipelineErrorHandler`) and passed into every module; collects
  failures raised during processing.
- `MFailure` — the failure element representation; `FailureSink` / `FailureConfig` route failures to a
  configured sink (`failureSinks`) or to a `<name>.failures` output collection (`outputFailure`).
- `failFast` controls whether a module error aborts the pipeline vs. is diverted as a failure record.
- `MPipeline` skips `*.failures` collections when logging final outputs.

## 6. Server (`server/`)

Built with the `server` Maven profile (WAR, Jetty EE11 12). Surfaces:

- **REST API** — `PipelineApiServer` + `api/` services: `PipelineService`, `SchemaService`, `SpecService`,
  `LaunchService`, `ProbeService`, `AgentService` (validate config, infer schema, launch jobs).
- **MCP** — `PipelineMcpStreamableServer` (Streamable HTTP at `/mcp`; `PipelineMcpSseServer` exists but is not
  mapped) with `mcp/tool` (14 tools, one class per tool annotated `@Tool.Module`, discovered by package scan;
  docs / source / validation / launch / job observation — user doc: `server/docs/deploy/mcp.md`),
  `mcp/resource` (`docs://` documents) and `mcp/prompt` (`design-pipeline`). `Tool.Registry` holds one
  instance per tool, shared with the agent. Runner-agnostic job observation lives in `server/job/`
  (`JobReader` over `dataflow/DataflowJobReader`, Cloud Run and Cloud Logging; `JobProgress` = workers /
  stage timeline / plan mapping); launch targets in `server/launch/` (`Launcher` SPI).
- **Webhook / Agent** — `PipelineWebhookServer`, `agent/PipelineAgent` (langchain4j) whose tools
  (`agent/tool/*`: `DocsReader`, `CodeReader`, `JobTools`, `PipelineExecutor`, `FeatureValidator`,
  `PipelineLauncher`) are thin wrappers over the MCP tools through `McpToolBridge` — one implementation per
  capability, agent names = camelCase of the MCP names.

`src/main/resources/server/docs/` is the **canonical location for user-facing docs**: the MCP
`read-docs` tool (and the agent's `readDocs` wrapper) reads `module/<type>/<name>.md` from the classpath,
`module/index.yaml` is the module catalog behind `list-modules` and the Builder UI, and MCP `DocsResources`
exposes the files as `docs://` resources (read from the same classpath tree). All user-facing docs (config reference,
`options/`, `deploy/`, `exec/`) live in this tree; `docs/` in the repo root keeps only developer docs.

## 7. Where to look

| Task                                   | Start here                                             |
|----------------------------------------|--------------------------------------------------------|
| How a config becomes a pipeline        | `MPipeline.java`                                       |
| Add/inspect a source/transform/sink    | `module/<type>/`, grep `@…Module`                     |
| Config parsing / templating / imports  | `config/Config.java`, `util/TemplateUtil`             |
| Data type conversions                  | `util/schema/converter/`                               |
| Field selection / expressions          | `util/pipeline/select/`                                |
| Aggregations                           | `util/pipeline/aggregation/`                           |
| SQL (BeamSQL / Calcite)                | `util/domain/sql/`                                     |
| Server / MCP / API                     | `server/`                                              |
| Runnable examples                      | `examples/` (`examples/README.md`)                    |
| Per-module config reference            | `src/main/resources/server/docs/module/`               |
