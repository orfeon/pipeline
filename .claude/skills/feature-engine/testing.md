# Testing and measuring feature-engine changes

## Test map

| Test | Layer | What it pins down |
|---|---|---|
| `util/pipeline/feature/FeaturePlanCompilerTest` | compile (no Beam) | expansion / naming / coordinates, availability algebra, violation vs intermediate, lattice expansion, scheduling by key affinity, `dependsOn` / waves / DAG estimate, hot-key audit SQL, hash stability, every diagnostic code |
| `SequenceIncrementalTest` | evaluators | randomized equivalence of the incremental and scan paths, trimmed vs untrimmed history, bounded tails, per-field trimming |
| `ContextEvaluatorTest`, `OrderStatisticsTest`, `DiscretizationTest`, `FactorizationTest`, `KeyedSpillSorterTest` | pure units | operator maths, artifact round-trips, spill / merge / cleanup |
| `FeatureStagesMergeTest` | engine unit | `coalesce` semantics (partials == branches, duplicate row ids, orphans, null-token keys) |
| `module/transform/FeatureTransformTest` | e2e (DirectRunner) | values for all four scopes on the auction rows, shrinkage / share / variance components, static + fold fits and artifacts (write, reuse, fixed windows), factorization, discretize, quantile, grouped output, leak rejection at assembly, the five `testParallelWaves*` A/B cases |
| `module/transform/FeatureSpillTest` | e2e | one hot key of 20k rows with a 1 MB budget: spills, replays in order, deletes chunks |
| `MPipelineDryRunTest` | CLI | `--dryRun` assembles and prints the plan without running |
| `server/.../ValidateFeatureToolTest` (`-Pserver`) | server | MCP `validate-feature` |

```bash
mvn test -Dtest=FeaturePlanCompilerTest                       # compile layer only (seconds)
mvn test -Dtest='Feature*Test,SequenceIncrementalTest'        # everything feature-related
mvn test -Dtest=FeatureTransformTest#testParallelWavesRowIdMerge
mvn test -Pserver -Dtest=ValidateFeatureToolTest              # server-side exposure
```

## Conventions

- JUnit 5, `TestPipeline.create().enableAbandonedNodeEnforcement(false)` as a field, config-driven
  e2e: `Config.load(yamlOrJson)` → `MPipeline.apply(pipeline, config)` → `PAssert` → `pipeline.run()`.
- **Domain-neutral data**: the online-auction dataset (`session_id`, `seller_id`, `category`,
  `start_price`, `condition_grade`, `current_bid_t10` (market), `sold` / `final_price` (outcomes,
  `settlementLag: PT30M`, `ingestionLag: P6D`)). Extend it; do not introduce another domain.
- Configs are **text blocks composed by `String.replace` on exact lines**. The runtime indentation
  of a text block is the source indentation minus the common prefix (6 / 8 / 10 spaces in
  `FeatureTransformTest`; `withEncoding` in the compiler test strips 4 more). A replace that
  matches nothing silently tests the old config — assert `assertNotEquals(BASE, modified)` or
  check the new column exists in the schema.
- Large generated configs are JSON (SnakeYAML ~3 MB cap); artifact / spill directories are
  relative `target/...` paths (Windows drive letters read as URI schemes).
- Assert diagnostics by **code** (`hasCode(plan, "discretize.bins")`), not by message text;
  pass `plan::describe` as the assertion message so a failure prints the whole report.
- Expected values are hand-computed from the six auction rows and written in a comment next to
  the assertion (e.g. `// s1: 100 (A) -> 200 (B) -> 80 (C) -> 120 (D)`).
- Wave / merge changes: `assertParallelMatchesLinear(config, rows, expectedTransforms,
  forbiddenTransforms)` runs the config twice on one pipeline (`features` parallel, `linear` with
  `engine.parallelWaves: false`), compares canonicalised rows, and fingerprints the merge path by
  transform names (`RowId_Pin`, `Wave1_FanIn`, `Wave1_Merge`, `_context_Vc`). A shared row
  expression consumed by two keyed blocks (`won`) is in `PARALLEL_CONFIG` on purpose — it is the
  only shape that catches "row column not recomputed on the branch".
- Keyed-path changes: add the column to `SequenceIncrementalTest.SPEC`; the test replays one key
  exactly like `KeyedHistoryDoFn` (pending rows join when the timestamp advances) with
  `forceScan` on and off and asserts row-by-row equality.
- Log wording is part of the interface for the verifier's greps (`budget`, `keyed spill sorter`,
  `keeps`): when you change a line, say so in the PR (S6 changed `keeps` and greps went silent).

## Measuring a change on real data

The engine is throughput-bound and shuffle-bound in ways a unit test cannot show. The loop that
closed the 52 → 9 min arc, runnable entirely over MCP (`server/mcp/tool`) or the agent tools:

1. **Build and push** the image (`mvn clean package -DskipTests -Dimage=...`); Cloud Run Jobs pin the
   image digest, so `gcloud run jobs update` after every push for that path.
2. **Dry run** — `run-pipeline` with `dryRun: true` (or CLI `--dryRun=true --config=...`). Read
   `featurePlans` / the printed report: `stages=n shuffles=n waves=d (dag shuffles~n)`, the
   `-- stages` list (`kind key=[...] blocks=[...] deps=[...] wave=w`), hints
   (`encoding.globalKey`, `sequence.window.unbounded`), and `-- audit`. Run the audit SQL on the
   warehouse: the top `row_count` per key set is the largest key one thread replays (spill size,
   memory for unbounded columns).
3. **Launch** — `launch-pipeline` on Dataflow with `options.dataflow.autoscalingAlgorithm: NONE`,
   `numWorkers = maxNumWorkers` sized for wave 1 (at least the number of heavy branches; 12 ×
   n1-highmem-8 for the 916k-row reference), `diskSizeGb` for the keys spilling concurrently
   (100 GB was ample after the sorter rewrite). Use a distinct output table per run.
4. **Observe** — `get-job-progress` (workers / autoscaling events, fused-stage timeline: a Dataflow
   fused stage `Stage{n};Stage{n+1}_<kind>` = the GBK read of one stage plus the next DoFn; wave
   branches show as parallel fused stages), `get-job-logs` / `list-job-errors` for the spill lines,
   the budget line, OOM / GC warnings, and the plan the launcher logged.
5. **Compare outputs** against the previous run: row count and a per-column diff of every numeric
   column (the reference config has 107); a wave / scheduling change must be **identical**, a
   `fit.mode` change is expected to differ (modeling change). For a suspected engine regression,
   rerun with `engine.parallelWaves: false` as the A/B control.
6. **Record** stage times per fused stage, the longest branch, memory peak, and the decision —
   in the PR description or the engine doc, in neutral terms (no customer key names).

Runner rules: Dataflow for full runs; the `prism` image on Cloud Run for subsets (minutes, output
identical to Dataflow; in-memory, so size the container for the input); **never** the `direct`
image for keyed stages over coarse keys (§9.5 — a local reproduction is
`src/test/java/.../FeatureDirectGlobalKeyBench.java`, untracked). Prism completion marker in
the logs: `Pipeline finished with state: DONE`; the `ManagedChannel allocation site` stack at
shutdown is a harmless gRPC leak-detector warning.
