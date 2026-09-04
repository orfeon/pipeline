# Reading the plan, sizing the job, choosing the runner, launching and monitoring

## The plan report, line by line

```
feature plan 2b57bfceb0e0d469
predictAt=event_time - PT10M time.field=session_time columns=127/183 stages=21 shuffles=20 waves=3 (dag shuffles~5)
-- stages (linear chain; deps = stages whose keyed/fit columns this one needs, wave = depth in that DAG)
  #0 context key=[session_id] blocks=[relative, composition] columns=12 deps=[] wave=1
  #1 population key=[seller_id] blocks=[recent, enc] columns=34 deps=[] wave=1
  #9 population blocks=[enc] columns=6 deps=[] wave=1                 ← no key: the global level
  #11 fit blocks=[encFold] columns=7 deps=[] wave=1
  #19 context key=[session_id] blocks=[...] columns=84 deps=[1, 2, ...] wave=2
  #20 groupBy key=[session_id] blocks=[output] columns=0 deps=[...] wave=3
-- columns
  f_recent_n5_start_price_mean : float64 [sequence/aggregate] availableAt=event_time - PT10M status=staticSafe derivedFrom=[attribute] <- [start_price, seller_id]
  _sold_flag : float64 [row/expr] availableAt=event_time + PT144H30M status=violation derivedFrom=[outcome] (intermediate) <- [sold]
-- audit (hot keys; {input} = the transform input relation)
  seller_id [stages 1, 4]: SELECT seller_id, COUNT(1) AS row_count FROM {input} WHERE seller_id IS NOT NULL GROUP BY seller_id ORDER BY row_count DESC LIMIT 20
  <global> [stage 9]: SELECT COUNT(1) AS row_count FROM {input}
-- diagnostics
  hint[encoding.globalKey] features.enc: stage #9 evaluates every row under one key ...
```

- **`columns=a/b`**: a emitted, b including intermediates and hidden lattice levels.
- **`stages` / `shuffles`**: one shuffle per keyed stage in the linear chain. **`waves`** is the
  depth of the stage dependency DAG: the stages of one wave run **in parallel** as branches and are
  merged by row id; **`dag shuffles`** is the barrier count of that execution. A job takes roughly
  one barrier per wave plus the slowest branch of each wave.
- **`deps=[...]`**: which earlier stages' keyed columns a stage reads. A row column that only the
  output reads is evaluated in the last keyed stage and can pull that stage into a later wave; put
  such columns in the block that consumes them when it matters.
- A `population` / `sequence` stage **without `key=`** is a global level (a lattice's `[]` entry or a
  `share` denominator): every row under one key, one worker thread. The `encoding.globalKey` hint names
  it. After parallelisation it is usually the critical path.
- **Column lines**: `status` (see reference.md), `derivedFrom` (origin kinds), `(intermediate)` for
  `_` columns, `<- [inputs]`. Sequence / population columns also show `(shift=PT...)` when their window
  near edge moved.
- **Audit**: one SQL per distinct key set of the keyed stages; keys that are derived columns are
  flagged in a `note` (evaluate on the relation as it stands before that stage).

## Sizing from the audit queries

Run every audit query on the warehouse with `{input}` replaced by the relation that feeds the
transform. For each keyed stage the top `row_count` is the number of rows **one worker thread**
gathers for the hottest key:

- The rows of a key are sorted in memory up to the spill budget (`engine.spill.memoryMB`, default a
  quarter of the worker heap divided by the cores, clamped to 16–256 MB) and beyond it as sorted chunks
  on the worker's local disk, merged on read and deleted when the key is done. Disk holds at most the
  keys being processed concurrently (≈ cores per worker), each up to its encoded size — size
  `diskSizeGb` for that, plus the branches of a wave spilling at the same time.
- What stays in memory per key during the replay is the running statistics plus the **projected
  history** behind the longest window: only the fields the windows read, only as far back as the
  longest `maxAge` / bounded tail. Columns flagged `sequence.window.unbounded` keep every past row
  (their own fields only, ~40 bytes per row skeleton plus the fields) — give them a `maxAge` before a
  big backfill.
- Context stages and the `output.groupBy` finalize hold a whole group in memory; groups are meant to
  be small (one event's rows).
- Static fits (`factorization`, `discretize`, static / fold encoding levels) gather their training
  data on one worker: factorization = the whole example set (must fit in memory), discretize = 8 bytes
  per row, encoding levels = one entry per key.
- `engine.spill.compress: true` halves the disk footprint at the cost of 2–3× slower spill stages;
  keep it off unless disk is the blocker.

## Dataflow settings for a batch feature job

```yaml
options:
  dataflow:
    autoscalingAlgorithm: NONE       # the default autoscaler shrinks the pool right at the wave fan-out
    numWorkers: 12
    maxNumWorkers: 12
    workerMachineType: n1-highmem-8  # memory-heavy: keyed replays and context groups live on the heap
    diskSizeGb: 100                  # concurrent spills of a wave
```

- **Fix the pool.** Dataflow's autoscaler judges a batch job by the currently running fused stage;
  at the fan-out of a wave it sees one stage with no visible backlog and scales *down* (6 → 1 was
  observed). With the pool fixed, wave 1 finishes in the time of its slowest branch instead of the sum
  of all branches divided by one worker.
- Size `numWorkers` for wave 1's combined work: at least the number of heavy branches (stages of
  wave 1 with large `columns=` or coarse keys), typically 6–12 for a plan with ~20 stages.
- Machine type: high-memory. Heap per worker is shared by the concurrent keyed replays (one per core)
  and their spill budgets.
- Disk: a full run of ≈ 1 M rows × 180 columns completed on the default 30 GB with the spill
  sorter (chunks are deleted per key); 100 GB is the comfortable setting for parallel waves, where
  several branches spill at once. The older 500 GB estimate predates the sorter and is obsolete.
- The same values can be passed as launch parameters of `launch-pipeline` (`numWorkers`,
  `maxNumWorkers`, `workerMachineType`, `diskSizeGb`, `workerZone`, `serviceAccount`,
  `templateLocation`, `jobName`, `project`, `region`); `autoscalingAlgorithm` goes in the config's
  `options.dataflow` block.

## Choosing the runner

| runner / image | use for | do not use for |
|---|---|---|
| **Dataflow** (`dataflow` image, Flex Template) | full backfills, any measurement | — |
| **prism** (`prism` image, Cloud Run Jobs or local) | subsets, reproduction of a Dataflow result (outputs identical), local iteration | full-size inputs: it is an in-memory runner without spill, and container memory grows roughly linearly with the input — measured 11 GB for 44 k rows and 28 GB for 130 k rows of a ~180-column plan (≈ 0.2 GB per thousand rows), so size the container from the input row count. **OOM signature**: the prism sub-process is killed silently — no prism log, no OOM message from Cloud Run — and the JVM only reports `UNAVAILABLE: Network closed for unknown reason` / `connection refused` on every gRPC channel at once. That pattern means "out of memory": shrink the input or move to Dataflow |
| **direct** (`direct` image) | tiny smoke tests, row / context-only specs | anything with a coarse or global key: the DirectRunner copies a key's buffered rows for every bundle that touches it, so a global level takes hours where Dataflow takes seconds |

Completion markers in the logs: Dataflow job state `JOB_STATE_DONE`; prism / direct print `Pipeline
finished with state: DONE`. On prism the `ManagedChannel allocation site` stack at shutdown is a
harmless gRPC warning.

## Launch and monitor over MCP

1. **`run-pipeline`** `{config, dryRun: true, args}` → check `status`, `spec` (every step's schema) and
   `featurePlans[].ok` / `describe` / `engineErrors`.
2. **`launch-pipeline`** `{config, runner: dataflow | prism | direct | spark, environment?, parameters:
   {project, region, jobName, numWorkers, maxNumWorkers, workerMachineType, diskSizeGb, workerZone,
   serviceAccount, templateLocation, ...}, args}` → `job` (id / name). All of the Dataflow worker
   parameters listed here, `diskSizeGb` included, are forwarded to the Flex Template launch; the same
   values may instead sit in the config's `options.dataflow` block (`autoscalingAlgorithm` only there).
   `project` / `region` (and the template location / Cloud Run job name) are resolved in this order:
   launch `parameters` → the config's `options` (runner block, then `options.gcp`) → the server's
   `MERCARI_PIPELINE_LAUNCH_<RUNNER>_<KEY>` / `MERCARI_PIPELINE_LAUNCH_<KEY>` environment → the
   server's own project / metadata; a value found nowhere rejects the launch before anything is
   submitted. **`args`**: only `${args.<name>}` placeholders are substituted (a bare `${name}` stays
   literal); the config's `args:` block gives the defaults and the launch `args` override them. Use a
   distinct output table per run (a template arg) so runs can be compared.
3. **`get-job`** `{job, runner?, project?, region?}` for the state; **`get-job-progress`** `{job}` for
   workers / autoscaling events and the per-stage timeline (Dataflow fuses each stage's GroupByKey read
   with the next DoFn, so a fused stage reads `Stage{n};Stage{n+1}_<kind>`; the branches of a wave
   appear as parallel fused stages); **`get-job-logs`** `{job, contains: "feature plan" | "keyed spill
   sorter" | "budget" | <stage name>, minSeverity, limit}` for the plan the launcher logged, the spill
   lines (`keyed spill sorter Stage<N>_<kind> key=<key>: <chunks> chunk(s) / <MB> MB on disk ... live
   spill on this worker <MB> MB (peak <MB> MB)` — the peak is the disk a worker needed) and the
   unbounded-columns line; **`list-job-errors`** `{job}` for failures.
4. **Verify the output**: row count equals the input (or the context count under `output.groupBy`);
   column count equals the plan's emitted count; when the change must not alter values (a rename,
   `engine.*`, worker settings, runner), diff every numeric column against the previous run; when it
   may (`fit.mode`, shrinkage, windows), compare model metrics instead.

The Pipeline Builder agent exposes the same steps as `validateFeature`, `run` (dry run), `launchPipeline`,
`getJob`, `getJobProgress`, `getJobLogs`, `listJobErrors`; the CLI equivalent of the dry run is
`--dryRun=true --config=<path>` on any runner build.

## When the job is slow

| symptom | cause | lever |
|---|---|---|
| the pool shrinks to one worker early in the job | default autoscaler at the fan-out | `autoscalingAlgorithm: NONE` + fixed `numWorkers` |
| one branch of wave 1 runs alone for minutes after the others finish | the global-level stage (single thread) | `fit.mode: static` / `fold` for that encoding block (modeling change), or accept |
| `No space left on device` / keys failing with sort errors | concurrent spills exceed the disk | raise `diskSizeGb`, add `maxAge` to unbounded columns, fewer concurrent branches (`engine.parallelWaves: false` as a stop-gap) |
| OOM on the workers | large context groups, factorization example set, or too many unbounded columns | high-memory machines, `maxAge`, split static fits into their own step |
| many stages, each short | the linear chain's barrier per keyed stage (`engine.parallelWaves: false`, or streaming) | keep `parallelWaves: true` in batch; fuse blocks on the same key into the same stage by sharing `entity` / keySet keys |
| a stage of wave 2+ that only carries row columns | a row column consumed only by the output pulled into a late stage | move it into the block that consumes it |
| `waves=` grew after adding a `discretize` / static block whose column keys an encoding | the fit stage must finish before the keyed stage that reads it, and that stage before the context stage reading the encoding: each such chain adds waves (observed 3 → 5 waves, 9 → 14 min with two discretize blocks) | key the encoding on a hand-written row `bin` instead, or accept the depth; check `waves=` in the dry run before launching |
