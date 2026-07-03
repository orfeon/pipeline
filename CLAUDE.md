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
Loads the `Config`, sets pipeline `Options`, then `apply()` assembles the Beam pipeline. Assembly is a
**dependency-resolution loop**: modules whose `inputs`/`waits`/`sideInputs` are all satisfied get built and
their outputs registered; the loop repeats until every module is built (or it detects an unsatisfiable module).
Order in the config file does not matter. If assembly throws, `system.failure.alterConfig` can supply a
fallback config.

### Configuration System (`config/`)
- `Config.java` loads config from local text, `gs://…` (GCS), Google Parameter Manager resource, `ar://…`,
  or `data:` base64 (see `Config.load`).
- FreeMarker templating: `${args.varName}` placeholders are substituted from `system.args` and runtime args.
- `system.imports` can compose config from other files.
- `ModuleConfig` (base of `SourceConfig`/`TransformConfig`/`SinkConfig`) common fields: `name`, `module`,
  `parameters`, `inputs`, `tags`, `waits`, `sideInputs`, `logs`, `ignore`, `failFast`, `outputFailure`,
  `failureSinks`, `outputType`, `description`, `args`.

### Module System (`module/`)
Three module types are auto-discovered by scanning their packages (Guava `ClassPath`) for annotations —
**not** a single `@Module`. Each base class defines its own nested annotation:
`@Source.Module(name="…")`, `@Transform.Module(name="…")`, `@Sink.Module(name="…")`.

**Sources** (`module/source/`): `bigquery` `spanner` `bigtable` `datastore` `firestore` `iceberg`
`jdbc` `postgres` `tidb` `storage` `files` `drive` `http` `pubsub` `kafka` `create`.

**Transforms** (`module/transform/`): `select` `filter` `aggregation` `beamsql` `partition` `lookup`
`compare` `deserialize` `reshuffle` `http` `bigtable` `onnx` `onnx_gen` `pdfextract` `vertexai.gemini`.

**Sinks** (`module/sink/`): `bigquery` `spanner` `bigtable` `datastore` `firestore` `iceberg` `jdbc`
`pubsub` `storage` `files` `debug` `action` `auxia` `tasks` `localH2`.

> The registered `@…Module(name=…)` value is authoritative. If this list drifts, regenerate it by grepping
> `@Source.Module` / `@Transform.Module` / `@Sink.Module` in `src/main/java`.

### Core Module Classes (`module/`)
- `Module.java` — base for all modules; `Source`/`Transform`/`Sink` extend it and hold the discovery registries.
- `MElement.java` — universal data element that wraps any backing type (`DataType`: `ROW`, `AVRO`, `STRUCT`
  (Spanner), `DOCUMENT` (Firestore), `ENTITY` (Datastore), `MESSAGE` (Pub/Sub), `JSON`, …).
- `Schema.java` — unified schema representation used across all data types.
- `MCollection.java` — `PCollection<MElement>` + schema/metadata; `MCollectionTuple.java` — named-collection container.
- `MErrorHandler` / `MFailure` / `FailureSink` — dead-letter / failure routing (`failureSinks`, `outputFailure`).

### Utilities (`util/`)
- `schema/` — schema + `converter/` between Avro / Row / Entity / Struct / Document / Proto / JSON.
- `pipeline/` — pipeline building blocks (`select/`, `aggregation/`, `mutation/`, `action/`, filters, queries).
- `cloud/` — cloud service clients (`google/`, `amazon/`, `hashicorp/`, `crm/`).
- `domain/` — domain logic: `sql/` (BeamSQL + Calcite), `ml/onnx/`, `text/` (tokenizer/analyzer/template), `db/`, `math/`, `web/`.
- `coder/` — Beam coders.

### Server (`server/`)
- `PipelineApiServer.java` — REST API for validating/launching pipelines (`api/`: Pipeline/Schema/Spec/Launch/Probe/Agent).
- `PipelineMcpStreamableServer.java` / `PipelineMcpSseServer.java` — MCP servers (`mcp/tool`, `mcp/resource`, `mcp/prompt`) for AI integration.
- `PipelineWebhookServer.java`, `agent/` (PipelineAgent + tools). Docs served from `src/main/resources/server/docs/`.

## Adding a New Module

Use the **`add-module` skill** (`.claude/skills/add-module/`) — it walks through implementation, tests,
and agent-readable docs, with type-specific guides for source/transform/sink. Summary:

1. Create a class in `module/source/`, `module/transform/`, or `module/sink/`.
2. Extend `Source`, `Transform`, or `Sink`.
3. Annotate with the matching nested annotation, e.g. `@Transform.Module(name="mymodule")`.
4. Implement `expand()` returning an `MCollectionTuple` (source/transform) or handling the sink.
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

There are two doc trees with distinct audiences:

### User-facing docs — `src/main/resources/server/docs/` (canonical, agent-readable)

User-facing documentation is being **migrated here from `docs/`** because the bundled AI agent and MCP
server read docs from this location (bundled on the classpath):

- `module/<type>/<name>.md` — per-module config reference, read by the agent's `DocsReader` tool
  (`listModules` / `getModule`). Each file needs YAML front-matter with `title:`.
- `module/index.yaml` — module catalog (`title` / `description` / `tags` per module), used for discovery.
- `system.md` — `system` block reference.
- MCP `DocsResources` also exposes these files as `docs://` resources.

When adding or updating user-facing/module documentation, **write it here first**. Keep files
self-contained (parameters, examples) — the agent reads one file per module.

### Developer docs — `docs/` (and legacy user docs pending migration)

- `docs/developer/` — developer docs: [architecture.md](docs/developer/architecture.md) (internals),
  `server/frontend.md`.
- `docs/config/` — legacy user-facing config reference (`module/`, `options/`, `system.md`); being migrated
  to `src/main/resources/server/docs/`. When touching a page here, prefer migrating it rather than
  duplicating edits in both trees.
- `docs/deploy/`, `docs/exec/`, `docs/README.md` — deploy/execute guides.
- `examples/` — runnable example configs (`examples/README.md` indexes them by use case).

## Testing Conventions

- **JUnit 5 (Jupiter)** — `org.junit.jupiter.api.Test` / `Assertions`. JUnit4 stays on the test classpath
  only because Beam's `TestPipeline` implements a JUnit4 `TestRule`; do not write new JUnit4 tests.
- `TestPipeline` is used standalone (no `@Rule`):
  `private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);`
- Module tests are config-driven e2e: `Config.load(json)` → `MPipeline.apply(pipeline, config)` →
  `PAssert` → `pipeline.run()` (see `FilterTransformTest`).
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
