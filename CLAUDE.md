# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Mercari Pipeline is a configuration-driven data pipeline framework built on Apache Beam. Users describe a
pipeline as a YAML/JSON config file (sources → transforms → sinks) and run it — without writing code — on
Cloud Dataflow, Direct/Prism, Apache Flink, or Apache Spark. It also ships a Server (REST API + MCP + web UI)
as an auxiliary tool to create, validate, debug, and deploy pipelines.

Package root: `com.mercari.solution` (`src/main/java/com/mercari/solution/`).

## Build & Test Commands

```bash
# Build FlexTemplate container (default profile: Dataflow runner)
mvn clean package -DskipTests -Dimage={region}-docker.pkg.dev/{project}/{repo}/dataflow:latest

# Build for local execution (DirectRunner)
mvn clean package -DskipTests -Pdirect -Dimage="{region}-docker.pkg.dev/{project}/{repo}/direct"

# Build API server (WAR)
mvn clean package -DskipTests -Pserver -Dimage="{region}-docker.pkg.dev/{project}/{repo}/server"

# Tests
mvn test                                   # all tests
mvn test -Dtest=ConfigTest                 # single class
mvn test -Dtest=ConfigTest#testMethodName  # single method
```

### Maven Profiles (runners)

`dataflow` (default) · `direct` · `prism` · `portable` · `flink` · `spark` · `server` (WAR) · `dataflow-gpu`.
The active runner is also resolved at runtime from pipeline options (see `MPipeline.Runner`).

## Architecture

For deep internals (assembly loop, data model, schema conversions, error handling, module lifecycle) see
**[docs/developer/architecture.md](docs/developer/architecture.md)**. Summary:

### Entry Point — `MPipeline.java`
Loads the `Config`, sets pipeline `Options`, then `apply()` assembles the Beam pipeline.
When the `PORT` env var is set (Cloud Run Service) or `--serve=true` is passed, `main` instead
starts `MPipelineHttpServer` (JDK built-in HTTP server, no Jetty): `POST /run` assembles and runs
one pipeline per request (fixed config via `--config`/`MPIPELINE_CONFIG`; body = data for the
`request` source, or the config itself; `?args.*` = template args). Assembly is a
**dependency-resolution loop**: modules whose `inputs`/`waits`/`sideInputs` are all satisfied get built and
their outputs registered; the loop repeats until every module is built (or it detects an unsatisfiable module).
Order in the config file does not matter. If assembly throws, `system.failure.alterConfig` can supply a
fallback config.

### Configuration System (`config/`)
- `Config.java` loads config from local text, `gs://…` (GCS), Google Parameter Manager resource, `ar://…`,
  or `data:` base64 (see `Config.load`).
- FreeMarker templating: `${args.varName}` placeholders are substituted from `system.args` and runtime args.
- `system.imports` can compose config from other files.
- `ModuleConfig` (base of `SourceConfig`/`TransformConfig`/`SinkConfig`/`ActionConfig`) common fields: `name`, `module`,
  `parameters`, `inputs`, `tags`, `waits`, `sideInputs`, `logs`, `ignore`, `failFast`, `outputFailure`,
  `failureSinks`, `outputType`, `description`, `args`.

### Module System (`module/`)
Four module kinds are auto-discovered by scanning their packages (Guava `ClassPath`) for annotations —
**not** a single `@Module`. Each base class defines its own nested annotation:
`@Source.Module(name="…")`, `@Transform.Module(name="…")`, `@Sink.Module(name="…")`,
`@Action.Service(name="…")` (action services, see below).

**Sources** (`module/source/`): `bigquery` `spanner` `bigtable` `datastore` `firestore` `iceberg`
`jdbc` `postgres` `tidb` `storage` `files` `drive` `http` `pubsub` `kafka` `create` `request`.

**Transforms** (`module/transform/`): `select` `aggregation` `beamsql` `query` `partition`
`compare` `reshuffle` `onnx` `onnx_gen` `pdfextract` `feature`.

**Sinks** (`module/sink/`): `bigquery` `spanner` `bigtable` `datastore` `firestore` `iceberg` `jdbc`
`pubsub` `storage` `files` `debug` `auxia` `tasks` `http` `grpc` `localH2`.

**Actions** (`module/action/`, `@Action.Service(name=…)`): `bigquery` `vertexai` `storage` `tasks` `http` `dataflow` `build`.
The fourth module kind, declared in the `actions` config section (`ActionConfig`: `module` = service
name, `operation` (service-declared `resource.method` values, e.g. `jobs.load`, `queues.pause`), `trigger`, optional `inputs`, `waits`, `strategy`, `retry`, `fireOnEmpty`). `module/Action.java` is the single concrete
module class (trigger topologies, envelope output, failure routing); services implement the
`ActionService` SPI (`configure` / `setup` / `execute`) and are discovered by scanning `module/action`.
Triggers: `once` (fire after all inputs/waits complete; inputs are pure signals) / `perElement` /
`collect` (gather all elements into one firing). Every firing emits a common envelope record
(`service, operation, jobId, state, startedAt, finishedAt, payload`). Two-plane rule: sink outputs and action
envelopes are control records — consumable by action `inputs` and anyone's `waits`; a data
transform/sink consuming them via `inputs` gets an assembly-time warning (see
`docs → module/action/README.md`).

> The registered `@…Module(name=…)` / `@Action.Service(name=…)` value is authoritative. If this list
> drifts, regenerate it by grepping `@Source.Module` / `@Transform.Module` / `@Sink.Module` /
> `@Action.Service` in `src/main/java`.

### Core Module Classes (`module/`)
- `Module.java` — base for all modules; `Source`/`Transform`/`Sink`/`Action` extend it and hold the discovery registries.
- `MElement.java` — universal data element that wraps any backing type (`DataType`: `ROW`, `AVRO`, `STRUCT`
  (Spanner), `DOCUMENT` (Firestore), `ENTITY` (Datastore), `MESSAGE` (Pub/Sub), `JSON`, …).
- `Schema.java` — unified schema representation used across all data types.
- `MCollection.java` — `PCollection<MElement>` + schema/metadata; `MCollectionTuple.java` — named-collection container.
- `MErrorHandler` / `MFailure` / `FailureSink` — dead-letter / failure routing (`failureSinks`, `outputFailure`).

### Utilities (`util/`)
- `schema/` — schema + `converter/` between Avro / Row / Entity / Struct / Document / Proto / JSON.
- `pipeline/` — pipeline building blocks (`select/`, `aggregation/`, `mutation/`, filters, queries).
  - `pipeline/Query2.java` + `pipeline/lookup/` + `pipeline/udf/` — the per-element SQL engine behind the
    `query` transform: Calcite SQL inside a DoFn (plan once per worker, no shuffle) with key-driven
    lookup-joins to external sources (jdbc / spanner incl. parameterized GoogleSQL/GQL query tables /
    bigtable / datastore / firestore / rest / grpc / sideinput — other MCollections via Beam side
    inputs), correlated LATERAL
    blocks evaluated per key set, and UDF/UDAF registration.
    Built on the Beam-vendored Calcite 1.40 — never add a regular `org.apache.calcite` dependency.
    Maintain via the **`query-lookup-sources` skill** (`.claude/skills/query-lookup-sources/`).
- `cloud/` — cloud service clients (`google/`, `amazon/`, `hashicorp/`, `crm/`).
- `domain/` — domain logic: `sql/` (BeamSQL + Calcite), `ml/onnx/`, `text/` (tokenizer/analyzer/template), `db/`, `math/`.
  - `domain/sql/calcite/` is **deprecated** (pending deletion): only the old `util/pipeline/Query.java`
    still depends on it. New per-element SQL work uses `Query2`; do not add new dependencies on it.
- `pipeline/feature/` — the `feature` transform: `FeaturePlanCompiler` (pure compile layer: sources
  contract, availability-time algebra, DAG expansion, leak checks, `describe()` = validate --expand) and
  `FeatureStages` (Beam wiring: row ParDo / context GBK / keyed time-ordered replay for sequence &
  population; stages run wave by wave — the independent stages of a wave branch in parallel and are merged
  back by row id, `engine.parallelWaves: false` = linear chain). Spec in repo-root `work-feature.md`, engine design in `work-feature-engine-beam.md`
  (uncommitted working docs). Keep examples/tests domain-neutral.
- `pipeline/outbound/` — shared core for modules that call external HTTP/gRPC endpoints (`http` source/sink,
  http action, `tasks` sink, `grpc` sink, rest/grpc lookup, select http): `AuthProvider` (basic/bearer/apiKey/oauth2/
  gcpOidc/gcpOauth with worker-scoped token cache), `HttpTransport` (JDK HttpClient, async), `ResponsePolicy`
  (declarative success/retry/partial-failure classification, Retry-After backoff), `RequestSpec`/`RequestRenderer`
  (target/body config + template rendering), `SyncCaller` (blocking send-with-retry), `GrpcSupport` (descriptor-set linking, dynamic
  method descriptors, metadata/auth interceptor — shared by the grpc sink and the grpc lookup source). Design notes in
  repo-root `work_http.md` (uncommitted).
- `coder/` — Beam coders.

### Server (`server/`)
- `PipelineApiServer.java` — REST API for validating/launching pipelines (`api/`: Pipeline/Schema/Spec/Launch/Probe/Agent).
- `PipelineMcpStreamableServer.java` / `PipelineMcpSseServer.java` — MCP servers (`mcp/tool`, `mcp/resource`, `mcp/prompt`) for AI integration.
- `PipelineWebhookServer.java`, `agent/` (PipelineAgent + tools). Docs served from `src/main/resources/server/docs/`.
- `launch/` — `/api/launch` targets: `Launcher` SPI keyed `runner/environment` (`dataflow/flexTemplate`,
  `direct/cloudRunJob`, `direct/cloudRunWorkerPool`, `spark/dataprocServerless`), `LaunchDefaults` (the only
  reader of `MERCARI_PIPELINE_LAUNCH[_<RUNNER>]_<KEY>` env vars + metadata fallbacks), `LaunchSchema` (turns
  `x-launch-default` keys in `server/api/spec/launch.json` into `x-default-hint` placeholders for the UI). Cloud Run calls go through
  `util/cloud/google/CloudRunUtil` (REST v2, shared with a future `run` action). Env/IAM reference:
  `server/docs/deploy/server.md`.

## Skills

- **`add-module`** (`.claude/skills/add-module/`) — adding a source/transform/sink module (implementation,
  tests, agent-readable docs).
- **`query-lookup-sources`** (`.claude/skills/query-lookup-sources/`) — the `query` transform's SQL engine:
  adding/changing external lookup sources, the key-prefix join contract, correlated LATERAL internals,
  UDF/UDAF registration, Calcite-internal value conventions, and the emulator IT patterns. Consult it before
  touching `util/pipeline/Query2.java` or `util/pipeline/lookup/`.

## Adding a New Module

Use the **`add-module` skill** (`.claude/skills/add-module/`) — it walks through implementation, tests,
and agent-readable docs, with type-specific guides for source/transform/sink/action. Summary:

1. Create a class in `module/source/`, `module/transform/`, `module/sink/`, or `module/action/`.
2. Extend `Source`, `Transform`, or `Sink` — or, for an action service, implement `ActionService`
   (the `Action` module itself already exists).
3. Annotate with the matching nested annotation, e.g. `@Transform.Module(name="mymodule")` or
   `@Action.Service(name="myservice", operations={…})`.
4. Implement `expand()` returning an `MCollectionTuple` (source/transform) or handling the sink;
   an action service implements `configure` / `setup` / `execute` instead.
5. It is auto-discovered via package scanning — no manual registration.
6. Write user-facing config docs at `src/main/resources/server/docs/module/<type>/<name>.md`
   (YAML front-matter with `title:` — the agent's `listModules` uses it) and add an entry
   (`title` / `description` / `tags`) to `src/main/resources/server/docs/module/index.yaml`.

## Configuration File Structure

```yaml
system:
  args:
    myVar: "value"
  imports:
    - base: "gs://bucket/"
      files: ["common.yaml"]

sources:
  - name: input1
    module: bigquery
    parameters:
      query: "SELECT * FROM `proj.dataset.table`"

transforms:
  - name: process1
    module: select
    inputs: [input1]
    parameters:
      fields: [...]

sinks:
  - name: output1
    module: spanner
    inputs: [process1]
    parameters:
      projectId: myproject
      instanceId: myinstance
      databaseId: mydatabase
      table: mytable
```

## Documentation Map

### User-facing docs — `src/main/resources/server/docs/` (canonical, single tree)

All user-facing documentation lives here — read on GitHub by humans AND at runtime (bundled on the
classpath) by the AI agent, MCP server, and Pipeline Builder UI:

- `README.md` — config file structure reference (entry point).
- `module/<type>/<name>.md` — per-module config reference, read by the agent's `DocsReader` tool
  (`listModules` / `getModule`). Each file needs YAML front-matter with `title:`.
- `module/README.md` — human-browsable module list; `module/common/` — shared parameter docs
  (schema, filter, select, strategy, union, …); `module/failure/` — failure (dead-letter) modules.
- `module/index.yaml` — module catalog (`title` / `description` / `tags` per module), used for discovery.
  It is also the source of the Pipeline Builder UI's module list (`/api/spec`) — a module missing here
  does not appear in the GUI editor.
- `system.md` — `system` block reference; `options/` — pipeline options (per-runner pages).
- `deploy/`, `exec/` — deploy/execute guides (human-oriented; harmless to the agent).
- MCP `DocsResources` exposes every `.md` in this tree as `docs://<path>` resources (same classpath
  source as `DocsReader`).

When adding or updating user-facing/module documentation, **write it here**. Keep files
self-contained (parameters, examples) — the agent reads one file per module.

### Developer docs — `docs/`

- `docs/developer/` — developer docs: [architecture.md](docs/developer/architecture.md) (internals),
  `server/frontend.md`.
- `docs/images/` — images referenced by the root README.
- `examples/` — runnable example configs (`examples/README.md` indexes them by use case).

## Testing Conventions

- **JUnit 5 (Jupiter)** — `org.junit.jupiter.api.Test` / `Assertions`. JUnit4 stays on the test classpath
  only because Beam's `TestPipeline` implements a JUnit4 `TestRule`; do not write new JUnit4 tests.
- `TestPipeline` is used standalone (no `@Rule`):
  `private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);`
- Module tests are config-driven e2e: `Config.load(json)` → `MPipeline.apply(pipeline, config)` →
  `PAssert` → `pipeline.run()` (see `SelectTransformTest`). File-writing sinks assert by reading the
  output files back from a directory under `target/` instead of `PAssert` (see `StorageSinkTest`).
- kafka-clients is NOT on the test classpath (Beam's KafkaIO marks it provided): constructing any
  `KafkaIO` transform in a unit test fails with `NoClassDefFoundError`; Kafka modules need ITs.
- Tests run in parallel (4 threads) via JUnit Platform config in the surefire plugin.
- Coverage: JaCoCo runs with `mvn test`; report at `target/site/jacoco/index.html` (CSV/XML alongside).
- Integration tests (`*IT.java`, Testcontainers emulators, requires Docker) are skipped by default:
  `mvn verify -DskipITs=false -Djib.skip=true` (single class: add `-Dit.test=SpannerIT`).
  Do NOT add a Maven profile for them — activating any profile deactivates the default dataflow profile.
- CI: `.github/workflows/test.yml` runs `mvn test` on push/PR (JDK 21) and publishes the coverage summary/report.
- Parameters that accept "text or local file path" must guard `Paths.get(text)` with try/catch —
  Windows throws `InvalidPathException` for strings with `\n`/`:` (see `Config.load`,
  `BeamSQLTransform.loadQuery`).
- Known constraint: Struct-backed `MElement`s (Spanner reads) are encoded with `SerializableCoder`,
  and reading a `Struct` mutates its lazily-decoded internal state, so re-encoding differs.
  DirectRunner's `enforceImmutability` check false-positives on them — integration tests reading
  from Spanner disable it (`DirectOptions.setEnforceImmutability(false)`); a proper Struct coder
  is the long-term fix.

## Key Dependencies

- Java 21
- Apache Beam 2.74.0
- Google Cloud Platform SDKs (BigQuery, Spanner, Datastore, Firestore, Bigtable, Pub/Sub, …)
- Jetty EE11 12 (Server)
