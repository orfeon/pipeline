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

Modules are **not** built in file order. `apply` repeatedly walks sources, transforms, and sinks, building
any module whose dependencies are already available in the `outputs` map (`name → MCollection`):

- A module is buildable once all of its `inputs`, `waits`, and `sideInputs` names exist in `outputs`.
- Built module outputs are registered under `<name>` (and extra tagged outputs as additional keys), and the
  name is added to `executedModuleNames`.
- The loop continues until every module is built. If a full pass adds nothing new, it throws
  `IllegalModuleException("No input for modules: …")` — indicating a cycle, a typo in an input name, or a
  missing upstream module.
- Modules with `ignore: true` are skipped entirely; `null` entries (trailing commas) are tolerated.

This makes config authoring order-independent and lets transforms/sinks reference any upstream module by name.

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

**Module config shape.** `ModuleConfig` is the base of `SourceConfig` / `TransformConfig` / `SinkConfig`.
Common fields:

- `name` — unique id, used as the graph node key.
- `module` — registered module name (see §3).
- `parameters` — module-specific JSON object.
- `inputs` — upstream module names (transforms/sinks).
- `waits` — names that must complete before this module starts (ordering without data flow).
- `sideInputs` — names provided as Beam side inputs.
- `tags` — additional named outputs.
- `ignore` — skip this module.
- `failFast`, `outputFailure`, `failureSinks` — error routing (see §5).
- `outputType` — force the output `DataType`.
- `logs`, `args`, `description` — logging, per-module args, docs.

## 3. Module system (`module/`)

Three base classes — `Source`, `Transform`, `Sink` — all extend `Module<InputT>`. Each defines its **own**
nested runtime annotation used for discovery:

```java
@Source.Module(name="bigquery")
@Transform.Module(name="select")
@Sink.Module(name="spanner")
```

At class-load time each base class scans its package with Guava `ClassPath`
(`findSourcesInPackage("com.mercari.solution.module.source")`, and the transform/sink equivalents) to build a
`name → Class` registry. `Source.create` / `Transform.create` / `Sink.create` instantiate the right class for
a config's `module` value and call its `expand()`.

To find the authoritative module list, grep the annotations:

```bash
grep -rhoE '@(Source|Transform|Sink)\.Module\([^)]*\)' src/main/java | sort -u
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
- **MCP** — `PipelineMcpStreamableServer` / `PipelineMcpSseServer` with `mcp/tool` (list/describe/validate/run),
  `mcp/resource` (docs), and `mcp/prompt`.
- **Webhook / Agent** — `PipelineWebhookServer`, `agent/PipelineAgent` with `DocsReader` + `PipelineExecutor`.

`src/main/resources/server/docs/` is the **canonical location for user-facing docs**: the agent's
`DocsReader` tool reads `module/<type>/<name>.md` from the classpath (front-matter `title:` is used for
listings), `module/index.yaml` is the module catalog, and MCP `DocsResources` exposes the files as
`docs://` resources. Legacy user docs under `docs/config/` are being migrated here.

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
| Per-module config reference            | `docs/config/module/`                                  |
