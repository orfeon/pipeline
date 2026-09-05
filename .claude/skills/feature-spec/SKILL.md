---
name: feature-spec
description: Authoring, validating and running Mercari Pipeline `module: feature` configs from a consumer project — the sources contract, scope choice (row / context / sequence / population), encoding design (keySets, lattices, shrinkage, fit.mode expanding | static | fold), reading the validate --expand / dry-run plan report, fixing diagnostics (availability.*, sources.*, lineage.*, reference.*, encoding.*, sequence.*, fit.*, engine and fan-out merge errors), and sizing / launching / monitoring over MCP (validate-feature, run-pipeline dryRun, launch-pipeline, get-job-progress, get-job-logs) or the CLI (--dryRun). Use whenever a feature config is written, reviewed, fails validation, leaks, is slow, or runs out of disk / memory.
---

# Feature transform: authoring and operating a spec

This skill is **self-contained** and meant to be copied into a project that *uses* the pipeline
(`.claude/skills/feature-spec/` in that repository). It needs no access to the pipeline's source
code. The full parameter reference is the module's own documentation, readable at run time through
the pipeline server: MCP tool `read-docs` with `path: module/transform/feature.md` (also
`module/common/filter.md` for the predicate grammar, `options/dataflow.md` for Dataflow options) or
the MCP resource `docs://module/transform/feature.md`; on GitHub it is
`src/main/resources/server/docs/module/transform/feature.md` of the pipeline repository.

Companion files: [reference.md](reference.md) (the config cheat sheet — every key, its values and
the generated column names), [diagnostics.md](diagnostics.md) (every diagnostic code → meaning →
fix), [sizing.md](sizing.md) (reading the plan report, hot keys, spill, worker pool, runner choice,
launch and monitoring).

## The mental model in five lines

1. A **sources contract** states facts about the input: for every field, when its value is known in
   the world (`availableAt`), how much later it reaches this system (`ingestionLag`), whether rows get
   corrected later (`mutability`), and what kind of information it is (`kind`: attribute / market /
   outcome).
2. A **feature spec** states intent: features in four scopes — `row` (the row itself), `context` (rows
   co-occurring in one event), `sequence` (the entity's strictly-past rows), `population` (learned:
   encodings, factorization, discretize, quantileTransform, svd).
3. The compiler derives **when every output column is available** and rejects, at assembly, any
   emitted column that would need information after `predictAt`. History windows are shifted back by
   the outcome's settlement + ingestion lag automatically. Leak safety is a type check, not a review.
4. `validate --expand` (MCP `validate-feature`, `run-pipeline` with `dryRun: true`, CLI `--dryRun=true`)
   compiles the spec **without running** and returns the plan: every expanded column with its
   availability status, the stages / shuffles / waves, hot-key audit SQL, and structured diagnostics
   with codes. Iterate on that until it is clean.
5. `fit.mode` is a **modeling decision**: `expanding` (default) is the leak-safe backfill for training;
   `static` freezes whole-input statistics (serving / offline); `fold` is out-of-fold cross-fitting;
   `forward` reads complete earlier time blocks only (leak-free, stepwise expanding, parallel — the
   replacement for an expanding global-level stage).
   Switching modes changes the values, so validate a switch with model metrics, never with output diffs.

## Workflow

### Step 0 — collect the facts before writing YAML

For the input relation (one relation; joins happen upstream in the pipeline), write down:

- the event-time field and a tie-break field for rows sharing a timestamp (an event id);
- for every field: is it known before the event (attribute), a market snapshot taken at a known time
  before the event, or an outcome known after it? When exactly? How long after that does it show up in
  the table this pipeline reads (batch reflection delay)? Is the row corrected later (withdrawals,
  disputes)? Is there a column with the actual observation time?
- the prediction time relative to the event (`event_time - PT10M`), and whether some features are
  computed earlier than others (`computeAt`, v1);
- the entities (subjects of histories) and contexts (co-occurrence groups) the features need;
- the consumer's expected output shape: one row per input row, or one record per context with a child
  array (`output.groupBy`).

### Step 1 — write the sources contract

```yaml
sources:
  - name: listings
    eventTime: session_time
    availability: atEventTime          # table default: pre-event
    mutability: corrections            # rows change later → say how training values are obtained
    snapshotOf: {source: listings_snapshot, at: "event_date T08:00"}   # optional: archived point-in-time snapshot
    keys: [session_id, seller_id]
    fields:
      - {name: start_price, type: float64, kind: attribute}
      - {name: category,    type: string,  kind: attribute}
  - name: price_snapshots
    eventTime: session_time
    ingestionLag: PT1M
    fields:
      - {name: current_bid_t10, type: float64, availableAt: "event_time - PT10M",
         observedAtField: snapshot_time, kind: market, validFor: PT15M}
  - name: auction_results
    eventTime: session_time
    settlementLag: PT30M               # after(event) = event_time + 30 min (the world knows)
    ingestionLag: P6D                  # ... and this system sees it up to 6 days later (upper bound!)
    mutability: corrections
    fields:
      - {name: sold,        type: int32,   availableAt: after(event), kind: outcome}
      - {name: final_price, type: float64, availableAt: after(event), kind: outcome}
```

Rules that decide correctness:

- **`ingestionLag` is an upper bound and is relative to `availableAt`**, not to event time. Declare the
  worst case of a batch schedule (weekly reflection → `P7D`). Without it the leak check admits values
  that were usable in training but had not arrived at serving time.
- **Pre-event relative claims need evidence**: `availableAt: "event_time - δ"` or `atRowCreation`
  requires `observedAtField` (the real observation-time column). No such column → `evidence: declared`
  explicitly. `kind: market` + `declared` is an error unless `allowDeclared: true` with a written
  `justification` (per field, never globally). Reason: a "morning snapshot" that is really the closing
  value passes every check and leaks.
- `atEventTime` means "known once the event exists" (pre-event); `after(event)` means `event_time +
  settlementLag`. Do not write `event_time + PT0S` for pre-event data.
- `mutability: corrections` without `snapshotOf` means training uses the *final* row set and values;
  the validator warns (`context.rowSetDrift`) because serving sees the pre-correction set.
- `kind` is free text but `market` and `outcome` have semantics (market → evidence rules and lineage
  selectors; outcome → the encoding-rewrite hint and fold requirements). Tag every field.
- `validFor` only on freshness-sensitive fields (market snapshots); an expired value becomes null at
  `predictAt` (`output.nullPolicy: indicator` adds a flag).

### Step 2 — write the spec skeleton

```yaml
parameters:
  sources: gs://bucket/feature/sources.yaml   # or inline
  lineage:                                    # every field a feature uses, mapped to its source
    - {fields: [session_id, seller_id, category, start_price], from: listings}
    - {fields: [current_bid_t10], from: price_snapshots}
    - {fields: [sold, final_price], from: auction_results}
  time: {field: session_time, orderTieBreak: [session_id]}
  predictAt: "event_time - PT10M"
  entities:
    - {name: seller, keys: [seller_id]}
    - {name: pair, keys: [seller_id, category]}
  contexts:
    - {name: session, keys: [session_id]}
  baselines:
    - {name: market, context: session, expr: "share(1 / current_bid_t10)"}
  features: [...]
  fit: {mode: expanding}
  output: {prefix: f_, passThrough: keys}
```

- `time.field` must equal every source's `eventTime` (`time.mismatch` otherwise). Always declare
  `orderTieBreak` when sequence / encoding features exist.
- Input fields without a lineage entry cannot be used (`lineage.undeclared`); a feature must not reuse
  an input field's name (`column.shadowsInput`); names contain no `.` and do not start with `_`.
- `output.passThrough: keys` makes the output table safe for `SELECT *` (input fields are not
  availability-checked, so a full pass-through can carry post-event columns into a model).
- When the table feeds a training / screening / evaluation step, declare its contract:
  `output.roles: {group, time, entity, label, baseline, weight}` (role columns are never features and
  always pass through), `output.manifest: <uri>` (written at assembly, also by a dry run: roles, every
  emitted column with lineage and availability, `planHash` / `outputHash`; a batch run adds
  `manifest.run.json` with the row count and the observedAt audit), and `output.include: <uri or list>`
  to project the columns a screening step passed (replaces `exclude`; outside the plan hash).
- Serving a group-softmax model: `onnx` → `feature` with a context `softmax` op (`field` = the model
  score, `offset` = the market baseline, `temperature` or `temperatureFrom: <calibration uri>`) → sink.
  `baselines[].emit` outputs the baseline itself so training and serving read the same probability.
- Placebos for threshold calibration / permutation importance: `type: noise` (row, `seed`) and the
  context op `shuffle` (`fields`, `seed`) — deterministic per row identity / group, no information.
- Make pre-event claims auditable: pass the `observedAtField` column through from upstream. The engine
  then counts rows observed after the declared availability / after predictAt (metrics
  `feature/observedAt_<field>_*`, run manifest deciles of `predictAt − observedAt`);
  `audit.observedAt: fail` turns it into a guard.
- **Template arguments** (`sources`, `features` files and the config itself): only the exact form
  `${args.<name>}` is substituted — a bare `${name}` stays literal. The config's own `args:` block
  supplies the defaults; the `args` of `run-pipeline` / `launch-pipeline` (or `--args` on the CLI)
  override them per run. Use one for the output table so runs never overwrite each other.

### Step 3 — choose the scope of each feature

| you want | scope / type | notes |
|---|---|---|
| arithmetic on the row, calendar parts, fixed bins, a categorical cross, 0/1 flags | `row` (`expr`, `datetime`, `bin`, `cross`, `indicator`, `equals`) | `expr` is numeric only (doubles); no `$self` |
| difference to a named baseline (market) | `row`, `type: residual`, `baseline: market`, `on: identity \| logit \| log` | the baseline is declared once in `baselines` |
| rank / z-score / share / composition inside the event | `context` | `excludeSelf: true` for "vs. the others"; `values: [...]` for per-value columns |
| the entity's past: lag, delta, trend, EWMA, run length, time since, counts, deterministic aggregates | `sequence` | strictly past rows; windows `maxEvents` / `maxAge` / `filter` with `$self` |
| a **target mean / rate per key**, shrunk toward coarser keys, optionally windowed | `population`, `type: encoding` | never `sequence.aggregate mean` over an outcome (no shrinkage; the validator hints `sequence.aggregate.encoding`) |
| per-key counts / shares (frequency encoding), std, quantiles of a value | `population`, `type: encoding` with `stats: [count, share]` / `std` / `quantile`, `q25` | quantiles / distribution are expanding-only |
| low-rank interaction scores for sparse crosses | `population`, `type: factorization` | always static; whole training set on one worker |
| learned bin edges (to key an encoding) | `population`, `type: discretize` (`method: quantile`) | always static; bins `-1` missing, `0` below, `1..B`, `B+1` above |
| rank normalisation of a skewed value (uniform or normal score) | `population`, `type: quantileTransform` (`bins`, `distribution`) | always static; out of range clamps to 0 / 1 |
| decorrelated low-rank summary of several numeric features (e.g. a lag window) | `population`, `type: svd` (`inputs`, `rank`) | always static; fitted from sufficient statistics only |

Difference to the previous row of the entity: `lag` the past value, then subtract in a row `expr` —
sequence ops never see the current row.

### Step 4 — design encodings deliberately

```yaml
- name: enc
  scope: population
  type: encoding
  keySets:
    - keys: [seller_id]                                   # flat: seller → global
    - keys: [category]
      windows: [{maxAge: P365D}]                          # windowed conditional statistics
    - keys: [condition_grade]
    - keys: [category, condition_grade]
      structure: cross                                    # cell → additive(main effects) → global
    - keys: [seller_id]
      hierarchy: [[seller_segment], []]                   # explicit lattice: seller → segment → global
  targets:
    - {stats: [count, share]}
    - {field: sold, stats: [mean]}
    - {expr: "final_price > start_price", stats: [mean], as: gain}
  shrinkage: {weights: varianceComponents, priorWeight: 20, scale: logit, output: [composed]}
  maxFeatures: 100
```

- Expansion is the product keySet × window × target × stat: **always read the expanded column list**
  in the plan and keep `maxFeatures` tight.
- A lattice (`hierarchy` / `structure`) shrinks sparse keys toward coarser contexts. `additive` /
  `structure: cross` need the single-key keySets in the same block and an explicit `shrinkage.scale`.
- The **global level** (`[]`, or any `share` denominator) is one key holding every row — a single
  worker thread. The plan flags it (`encoding.globalKey`). If it dominates the run time, consider
  `fit.mode: forward` (leak-free, `blocks: {size: P90D}` default) / `static` / `fold` for that block — a
  modeling change, see the model note in step 0.
- `weights: varianceComponents` estimates the pseudo-counts from the whole batch (a hyper-parameter,
  not time-expanding; the validator says so as info). `fixed` + `priorWeight` is the fully expanding
  alternative.
- `fit.mode: fold` with keys derived from past outcomes requires `fit.groupBy: <entity>`.
- **A static fit whose output keys another block deepens the DAG.** A `discretize` (or any static
  block) must finish before the keyed stage that reads its column, so "fit wave → encoding keyed on the
  bins → the context stage that reads the encoding" adds waves: on a reference plan two discretize
  blocks feeding encodings took the job from 3 to 5 waves and from 9 to 14 minutes. Check `waves=` in
  the plan before and after; if the extra depth is not worth it, key the encoding on a hand-written
  row `bin` (no fit, no extra wave) or accept the cost consciously.

### Step 5 — validate, read the report, fix, repeat

Three equivalent entry points (all run the same compiler, none executes the pipeline):

- **MCP `validate-feature`**: `parameters` (the feature step's `parameters`) or `config` (a whole
  pipeline config; the first `module: feature` step, or the one named by `name`), optional
  `inputSchema: {fields: [{name, type}, ...]}`, `args`, `streaming`, `format: text` for the report.
  Without `inputSchema` the field types come from the sources contract.
- **MCP `run-pipeline` with `dryRun: true`** (and `args`): assembles the *whole* config on the server —
  every step's validation and resolved schema — and returns `featurePlans` (`{name, ok, describe,
  engineErrors}`) compiled against the **real** upstream schema. Prefer this before a launch.
- **CLI** `--dryRun=true --config=...` on any runner build prints the same report; **REST** `POST
  /api/feature` returns it as JSON (`ok`, `plan`, `engineErrors`, `describe`).

Read the text report top-down ([sizing.md](sizing.md) explains every line):

1. **Header** `columns=<emitted>/<all> stages=n shuffles=n waves=d (dag shuffles~n)`.
2. **`-- stages`**: `#i <kind> key=[...] blocks=[...] columns=n deps=[...] wave=w` — a key-less
   `population` / `sequence` stage is the global level.
3. **`-- columns`**: `name : type [scope/op] availableAt=... status=staticSafe|windowShift|runtimeFilter|violation
   derivedFrom=[kinds] <- [inputs]`. `(intermediate)` columns are not emitted (`_` prefix).
4. **`-- audit`**: SQL per key set — run it on the warehouse before a big backfill.
5. **`-- diagnostics`**: `level[code] location: message`. Fix **errors** first (the run will not
   assemble), then warnings, then read hints and infos (they explain shifts, filters, fits).
   [diagnostics.md](diagnostics.md) maps every code to its fix.

Then check that the expanded names are what the model expects (naming rules in
[reference.md](reference.md)), that no emitted column is `derivedFrom=[outcome]` unless intended, and
that every `windowShift` is the shift you expect (outcome settlement + ingestion lag + the predictAt
offset).

### Step 6 — size and run

See [sizing.md](sizing.md). In short: run the audit SQL, size the keyed stages by the top row counts,
fix the Dataflow worker pool (`options.dataflow.autoscalingAlgorithm: NONE`, `numWorkers` =
`maxNumWorkers`), give the workers disk for concurrent spills, launch with `launch-pipeline`, follow
with `get-job-progress` / `get-job-logs` / `list-job-errors`, and verify the output (row count, the
plan's `emitted` column count, a per-column comparison against the previous run when the change should
not alter values).

### Step 7 — iterate safely

- The **plan hash** covers the sources contract and the spec except `fit.artifact`, `engine`,
  `output.include` and `output.manifest`. Changing a feature changes the hash and the artifact directory
  (`<uri>/<planHash>/`); `artifact.id` pins a version for a serving config with `fit.mode: static`.
  The **output hash** (manifest) adds the projection, roles and include content: compare it, not the
  plan hash, to tell whether two runs produced the same table.
- `engine.*` (`parallelWaves`, `rowId`, `spill.*`) never changes values — use `engine.parallelWaves:
  false` as an A/B control when a result looks wrong.
- Any change that should not alter values (renames, `engine`, worker settings, runner) is verified by a
  per-column diff against the previous output; a `fit.mode` or shrinkage change is verified by model
  metrics.

## Pitfalls seen in production

- A `sequence` op such as `sinceEvent` / `countMatch` / `runLength` / `ewma` or a filtered window
  **without `maxAge`** keeps the entity's whole history on the worker (`sequence.window.unbounded`).
  Give it a `maxAge`.
- Two ops of the same type in one block (two `countMatch` predicates) collide on the column name: name
  them with `as:`. `as:` also replaces the anonymous `__e{n}` segment of an inline `expr`.
- `countByValue` / `ratioByValue` produce a `map` column; warehouse sinks and models want `values:
  [...]` (one numeric column per value).
- A column name that is a keyword of the condition grammar (`rank`, `order`) inside a `filter` /
  `predicate` is quoted automatically (`filter.quoted` info); a condition that does not parse is a
  compile error, not a worker crash.
- `output.groupBy` puts group-constant context columns on the **parent** record; look for them next to
  the group keys. `output.childName` must not collide with an input field.
- `engine.rowId` must be **unique** per input row; duplicates fail fast (`Fan-out merge: engine.rowId is
  not unique`) — this catches duplicated input rows, which is usually the real problem.
- **`quantile` / `distribution` stats have no serving path through artifacts**: they are
  expanding-only (a static / fold artifact keeps only n / Σy / Σy² per key), so a model that uses them
  must be served from the backfill path — the same history-based run as sequence features — not from a
  `fit.mode: static` config. Decide that before adopting them.
- Very large generated configs must be JSON: the YAML loader keeps SnakeYAML's default code-point
  limit of about 3 MB per document.
- Artifact / spill paths: `gs://...` or relative local paths (a Windows drive letter is read as a URI
  scheme).
- The `direct` image is **not** a runner for coarse-key encodings (a global level can take an hour per
  stage where Dataflow takes seconds); use Dataflow, or the `prism` image for subsets. A prism run
  that dies of memory leaves no OOM message anywhere — only `UNAVAILABLE: Network closed` /
  `connection refused` from the JVM (see the runner table in sizing.md).
