# Develop Mercari Pipeline

Guides for working on the Mercari Pipeline codebase itself (extending modules, understanding internals).

## Contents

* [Architecture (Internals)](architecture.md) — how a config becomes a running Beam pipeline: the assembly
  loop, the unified data model (`MElement` / `DataType` / `Schema`), config loading & templating, module
  discovery, error handling, and the Server.
* [Server Frontend](server/frontend.md) — Pipeline Server web UI.
* [Schema Redesign (Design Document)](schema-redesign.md) — accepted design for restructuring the `schema`
  block (fields / encoding / reference), moving it into `parameters`, and the phased migration plan.
* [Cross-Cloud Authentication (Design Document)](cloud-auth.md) — accepted design for running pipelines on
  GCP or AWS with transparent access to the other cloud's resources: bidirectional workload identity
  federation, `SecretProvider`, unified file loading, and the phased plan.
* [Feature Transform DSL (Design Document)](feature-dsl.md) — the declarative feature-engineering DSL
  of the `feature` transform: sources contract (`availableAt` / `ingestionLag` / `snapshotOf`), the four
  scopes, the unified encoding with structured keys and shrinkage, the availability algebra behind the
  leak check, naming / lineage / `validate --expand`, and the implementation phases.
* [Feature Transform Engine (Design Document)](feature-engine.md) — how that DSL runs on Beam: the pure
  compile layer, stage scheduling, per-scope evaluators, static fits and artifacts, the spill sorter, the
  parallel wave DAG, runner findings, and the implementation status / deferred items.

## Quick reference

* **Entry point:** `src/main/java/com/mercari/solution/MPipeline.java`
* **Add a module:** create a class under `module/{source,transform,sink}/`, extend the base class, annotate
  with `@Source.Module` / `@Transform.Module` / `@Sink.Module`, implement `expand()`. Discovery is automatic.
  See the root [`CLAUDE.md`](../../CLAUDE.md) for the full checklist, or use the Claude Code `add-module`
  skill (`.claude/skills/add-module/`) which includes per-type implementation guides.
* **Build & test:** see [`CLAUDE.md`](../../CLAUDE.md) (Maven profiles per runner; `mvn test -Dtest=…`).
* **List registered modules:**
  `grep -rhoE '@(Source|Transform|Sink)\.Module\([^)]*\)' src/main/java | sort -u`
