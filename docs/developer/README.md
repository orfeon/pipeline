# Develop Mercari Pipeline

Guides for working on the Mercari Pipeline codebase itself (extending modules, understanding internals).

## Contents

* [Architecture (Internals)](architecture.md) — how a config becomes a running Beam pipeline: the assembly
  loop, the unified data model (`MElement` / `DataType` / `Schema`), config loading & templating, module
  discovery, error handling, and the Server.
* [Server Frontend](server/frontend.md) — Pipeline Server web UI.

## Quick reference

* **Entry point:** `src/main/java/com/mercari/solution/MPipeline.java`
* **Add a module:** create a class under `module/{source,transform,sink}/`, extend the base class, annotate
  with `@Source.Module` / `@Transform.Module` / `@Sink.Module`, implement `expand()`. Discovery is automatic.
  See the root [`CLAUDE.md`](../../CLAUDE.md) for the full checklist, or use the Claude Code `add-module`
  skill (`.claude/skills/add-module/`) which includes per-type implementation guides.
* **Build & test:** see [`CLAUDE.md`](../../CLAUDE.md) (Maven profiles per runner; `mvn test -Dtest=…`).
* **List registered modules:**
  `grep -rhoE '@(Source|Transform|Sink)\.Module\([^)]*\)' src/main/java | sort -u`
