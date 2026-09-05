# Design Documents

Design documents for Mercari Pipeline: why a subsystem is shaped the way it is, what was decided,
what is deferred. Each document opens with a **Status** line (`Proposal` / `Accepted` /
`Implemented`, with the phase or the deferred items) that is kept current as the code moves; the
code cites these documents by section number (`<doc>.md §x.y` in javadoc), so section numbers are
stable once a document is accepted.

Contributor guides (how the code works, how to extend it) live in [`../developer/`](../developer/README.md);
user-facing documentation lives in `src/main/resources/server/docs/` and is bundled at run time.

## Documents

* [Schema Redesign](schema-redesign.md) — restructuring the `schema` block (fields / encoding /
  reference), moving it into `parameters`, and the phased migration plan.
* [Cross-Cloud Authentication](cloud-auth.md) — running pipelines on GCP or AWS with transparent
  access to the other cloud's resources: bidirectional workload identity federation, `SecretProvider`,
  unified file loading, and the phased plan.
* [Feature Transform DSL](feature-dsl.md) — the declarative feature-engineering DSL of the `feature`
  transform: sources contract (`availableAt` / `ingestionLag` / `snapshotOf`), the four scopes, the
  unified encoding with structured keys and shrinkage, the availability algebra behind the leak check,
  naming / lineage / `validate --expand`, and the implementation phases.
* [Screen Transform DSL](screen-dsl.md) — the contract of the `screen` transform: baseline-conditioned
  feature screening before training, the Rao score test per family, placebo calibration, transform variants,
  periods / time window / leak flags, the partial test against an existing feature set, the outputs and the
  pass list that closes the loop with the feature transform, limits and extension positions.
* [Screen Transform Engine](screen-engine.md) — how that contract runs on Beam: the pure / wiring layout,
  one bounded Combine per statistic, the unrolled Newton passes and their controller, determinism, the
  DirectRunner findings behind the test setup, tests, status and deferred items.
* [Feature Transform Engine](feature-engine.md) — how that DSL runs on Beam: the pure compile layer,
  stage scheduling, per-scope evaluators, static fits and artifacts, the spill sorter, the parallel wave
  DAG, runner findings, and the implementation status / deferred items.

## Writing a new one

* One topic per file, `<topic>.md`, title `# <Topic> (Design Document)`, then `Status: **...**`.
* Keep examples domain-neutral (the tests' online-auction dataset for the feature transform) and keep
  decisions that belong to a consuming project out of the repository.
* Number the sections and do not renumber after acceptance; add subsections instead.
* When the design is implemented, update the Status line and turn implementation logs into a
  current-state description rather than a diary.
