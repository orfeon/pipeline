# Evaluation Transform Engine (Design Document)

Status: **Implemented (stages 1–3)** — the Beam execution of the contract in [evaluation-dsl.md](evaluation-dsl.md):
one Combine for the metrics, the bootstrap, the slices and the slice discovery cells, one sketch pass plus one
Combine for the calibration tables, one grid pass per temperature fit and unrolled Newton passes per blend fit
ahead of the aggregation, one sketch pass for the numeric discovery dimensions. Code: `util/pipeline/evaluation/` and `module/transform/EvaluationTransform.java`; the tests are
`EvaluationScorerTest` / `EvaluationSpecTest` (pure) and `EvaluationTransformTest` (e2e). The shared parts
live in `util/pipeline/glm/` and `util/pipeline/feature/FeatureLineage` (screen engine doc §1).

## 1. Layout: pure computation and Beam wiring

| class | role | Beam |
|---|---|---|
| `EvaluationSpec` | parse (every error collected: predictions, splits and their ordering rule, tables, slices, bootstrap) and resolve (feature role defaults, schema checks, the column layout of `EvaluationRow.x`: prediction columns, calibration fields, utility — each column once); `parametersHash` | no |
| `EvaluationRow` | the prepared sample (split, group, identity, time, bootstrap key, label, baseline, weight, slice values, numeric columns) with a compact coder | coder only |
| `AlignedRow` | one row of a scored unit as the calibration tables read it (split, ỹ, p, every q, the table fields, the utility) | coder only |
| `EvaluationScorer` | per-unit: `prepare` (sort by (time, identity), the baseline and every prediction set as means per row via `Baselines.means` / `GlmFit.softmax`, the labels normalised, the skip reasons), `fitInputs` / `derive` (the fit inputs f, o of a set; the derived sets' means from the fitted parameters), `temperatureLogLikelihoods` / `blendEvaluate` (the fit passes' contributions: the grid log scores; the Newton evaluation via `GlmFit` at the uniform share), `score` (log score, hit@1, Brier per set, the baseline at index 0, derived sets last), `accumulate` (the metrics keys for the overall record and every slice value, the split bookkeeping, every key's contribution under the unit's bootstrap key), `dimensionValues` / `accumulateDiscovery` (the unit's discovery dimension values — a numeric one binned by the sketch edges — and its `[n, Σd, Σd²]` into every candidate cell of up to `maxDepth` dimensions, discovery and confirmation splits only), `aligned`, `unitRecords`, `standardErrors` | no |
| `FitResults` | the fits' outcome as a singleton side input: derived set name → parameters, and the fit records of the summary / `output.calibration` | Serializable |
| `MetricAccumulator` | 9 total slots (units, rows, Σw, Σwỹ, Σw·logScore, Σw·logScoreBaseline, Σw·hit, Σw·brier, Σw·utility) plus 7 × samples replicate slots, expanded lazily from pending contributions (the 7 replicate slots + the bootstrap key) once they exceed `Fn.pendingLimit(samples)` (samples / 2, at most `PENDING_MAX` = 32) or at extraction, the Poisson weights from `poissonWeights` = `seededRandom(seed, bootKey)`; the same shape carries the run bookkeeping under ``-prefixed keys; coder + `Fn` | coder + CombineFn |
| `SketchAccumulator` | a KLL doubles sketch (k = 400) of one table's value stream; bytes coder + `Fn` | coder + CombineFn |
| `EvaluationReport` | `metric` (a weighted mean, the excess as a difference of means, the binomial prior reference from Σwỹ / Σw), `replicate` / `interval` (the 2.5 / 97.5 percentiles), `build` (records + pair records + slice discovery + summary), `discoveryZ` / `discoveryThreshold` / `discovery` (the random-subset z, the max-of-K threshold, the candidate records with their confirmation), `calibration` (bins with bounds, Wilson), the table value / bin functions, the output schemas, `describe` | no |
| `EvaluationStages` | the graph (§2–§3) and its DoFns | yes |
| `EvaluationTransform` | thin: streaming rejected, parse → lineage → resolve → `engineConstraints`, `describe` to the log, four outputs | module |

## 2. The metrics graph

```
input ─ Prepare ─┬─ rows KV<split|unit, EvaluationRow> ─ Group (GBK) or Units (one row each) ─ Align ─┬─ scored KV<key, MetricAccumulator> ─┐
                 └─ bookkeeping KV<rows, MetricAccumulator> (one per bundle) ────────────────────┼─ units MElement                     ├─ Flatten
                                                                                                      ├─ rows MElement (rows output)         │
                                                                                                      └─ AlignedRow (calibration, §3)        │
                     ─ Combine.perKey(MetricAccumulator.Fn) ─ Gather (Combine.globally, list) ─ Finalize ─┬─ metrics ◄────────────────────────┘
                                                                                                          └─ summary
```

- **Prepare** reads the element: the time (from `time.field` with its schema type; else the element timestamp),
  the split (the time-range lookup, or the split column's value — an unknown value is unassigned), the label
  (field or expression), the group, the weight, the baseline, the numeric columns (NaN = missing), the slice
  values (the field's text, or the period bucket of a time field), the bootstrap key and the identity (a
  128-bit murmur3 hash of the `rowId` fields, else of every field value). Unassigned and invalid rows are
  counted per bundle on the `rows` key; a null time with time-range splits is a failure. The unit key is
  `split  group` (or the identity), so a group whose rows fall in two splits is two units.
- **Align** calls `EvaluationScorer.prepare` (which also counts the unit's duplicate identities — adjacent
  after the sort — and whether each slice / dimension varies over its rows; `accumulate` writes them into the
  split bookkeeping and the `sliceVaries` / `dimensionVaries` counters) / `score` / `accumulate` per unit into a bundle-local
  `Map<String, MetricAccumulator>` flushed at `@FinishBundle` (a partial combine: the shuffle carries keys ×
  bundles elements; a unit enters as a **contribution** — its slots and bootstrap key — and the replicate sums
  are expanded only when a key's pending contributions exceed `Fn.pendingLimit`, in the bundle map, in the
  Combine's merges, or at extraction, so a runner that gives the align step one unit per bundle ships a few
  dozen bytes per key per unit, not 7 × samples doubles), emits the unit records straight away (no coder for a unit result type), when tables
  are declared the aligned rows, and when a `rows` output is declared the row records of the selected splits
  (`EvaluationScorer.rowRecords`: every compared set's mean per row; the `rowId` values ride `EvaluationRow`
  only then). A skipped unit is counted on its split's bookkeeping key.
- **Keys** are `split  prediction index  slice index  slice value` (slice −1 = overall), the
  baseline being prediction 0. A unit adds (1 + k) × (1 + its non-null slice values) accumulators, each with
  the unit's replicate weights.
- **Combine.perKey** merges the totals and the replicate vectors; **Gather** collects the few hundred
  accumulators into one list; **Finalize** runs `EvaluationReport.build` once. In the global window the
  default empty list still fires, so the summary is emitted on an empty input. The gather is the engine's
  memory boundary: the key count is splits × (1 + k) × (1 + Σ slice cardinalities) and nothing bounds a
  slice's cardinality, so a slice on a high-cardinality field (an id) gathers gigabytes onto one worker;
  the docs restrict `slices` to low-cardinality dimensions. `build` only ever needs the accumulators of one
  cell together (the summary reads the bookkeeping keys alone), so a per-cell finalize is the future shape.

### 2.1 Why the pair interval is free

The replicate weights w_{u,b} are drawn per resampling unit, so for two prediction sets on the same units
Σ_u w_{u,b} w_u (m_A,u − m_B,u) = Σ_u w_{u,b} w_u m_A,u − Σ_u w_{u,b} w_u m_B,u: the replicate series of a
pair difference is the difference of the two series the accumulators already hold. The same holds for Δ
itself (log score minus baseline log score). `bootstrap.unit` only changes which key seeds the draw: two
units sharing a key share their weight vector, which is the cluster bootstrap.

### 2.2 Binomial prior mode

Without a baseline the binomial reference is the entropy of the split's label mean, a function of Σwỹ and Σw,
which the totals and every replicate carry; the per-row `logScoreBaseline` is NaN in the accumulators and the
report derives the reference (also per replicate, so the excess interval is right). The grouped prior
(uniform 1 / n) is known per unit and accumulates like a baseline.

## 2.3 The fit graph

```
units ─ Temperature<i> (grid log scores per bundle, selection split only) ─ Combine.globally ─ singleton view ─┐
Create(base) ─ Blend<i>_<base>_Init (θ = (1, 1|0[, 0]): the set as declared) ─ view = state₀                 │
for it in 1..maxIter: units ─ Blend<i>_<base>_Fit<it> [side: state_{it-1}] ─ Combine.globally ─ Advance ─ view │
Create(0) ─ Fits_Collect [side: every fit view] ─ FitResults singleton ─► Align (derive), Finalize (summary, JSON)
```

Every fit pass reads the GroupByKey output and keeps the units of `fitOn` only (the unit key carries the
split); a temperature fit is one pass whatever the grid, a blend fit is the screen transform's unrolled
Newton chain (`FitState` cloned and advanced per pass, converged passes read nothing). `Fits_Collect` picks
the grid argmax (flagging a boundary optimum) and the blend's best point with its standard errors from the
inverse of the Fisher information, and `Align` derives the sets before scoring — so the derived sets are
ordinary sets for everything downstream. Without fits the collect step still runs (an empty result).

## 2.4 Slice discovery

```
units ─ Dimensions (KLL per numeric dimension, discovery split only) ─ Combine.perKey ─ View.asMap ─► Align
Align ─ discovery cells KV<\u0001disc\u0001 split|set|dims|values, MetricAccumulator[n, Σd, Σd²]> ─► the metrics Combine ─► Finalize ─ slices
```

`Align` reads the dimension edges once per bundle from the sketch view, enumerates the unit's candidate
cells (every combination of its non-null dimension values up to `maxDepth`, plus the split's overall cell)
and adds `[n, d, d²]` per set into a bundle-local map, flushed as `MetricAccumulator`s whose first three
total slots carry the sums — the same `Combine.perKey` and `Gather` as the metrics, so the discovery costs
no pass of its own. `Finalize` (`EvaluationReport.discovery`) applies the support floor, the candidate cap,
the random-subset z against the discovery split's mean and variance, the max-of-K threshold, and re-reads
every passed cell on the confirmation split; the records go to the `slices` output and the per-set counts
to the summary.

## 3. The calibration graph

```
AlignedRow ─ Sketch (per bundle, quantile tables only) ─ Combine.perKey(SketchAccumulator.Fn) ─ View.asMap ──┐
AlignedRow ─ Bins [side: sketches] ─ Combine.perKey(VectorAccumulator.Fn) ─ Gather ─ Bins_Finalize [side] ─ calibration
```

- **Sketch**: for every quantile-binned table and prediction set, the table's value (the prediction, or the
  divergence logit q − logit p) enters a bundle-local `SketchAccumulator` keyed by (split, prediction, table).
- **Bins**: the edges of a quantile table are read once per key from the sketch view (cached per bundle);
  `edges` tables use their declared boundaries; an `edge` table adds the row to every threshold it exceeds.
  The bin vector is `[n, Σy, Σq, Σp, Σ utility·y, Σỹ]` (`EvaluationReport.addBin`: the outcome y as declared
  for positives / rate / utility, the share ỹ in its own slot), bundle-local, then `Combine.perKey`.
- **Bins_Finalize** reads the sketch view again for the bounds (the outer bins carry the sketch minimum /
  maximum) and runs `EvaluationReport.calibration` once. Without tables the output is an empty collection.

Total: two passes over the aligned rows (one when no table is quantile-binned) on top of the one metrics
pass, plus one pass per temperature fit and `maxIter` passes per blend fit × base set; the side-input views
are why the transform needs the global window.

## 4. Determinism and cost

- Every random draw is `seededRandom(seed, bootKey + "bootstrap")` → `samples` Poisson(1) draws by Knuth's
  method: a pure function of the seed and the key, so bundle boundaries, worker counts, runners and the point
  at which a pending contribution is expanded do not
  change an interval. Unit rows are sorted by (time, identity) before any per-row computation.
- Accumulator width: (2 + 5) × samples doubles per key (56 KB at 1000 samples); keys = splits × (1 + k) ×
  (1 + slice values). A bundle-local map of a few hundred keys is a few tens of MB.
- Poisson draws: samples × keys per unit scored (the draws are redone per key at expansion: (1 + k) × (1 +
  slice values) × samples per unit, 80 M draws for 10 k units at 1000 samples and 8 keys), seconds.
- DirectRunner (the consumer's local runs, feedback item 12): the runner hands every grouped unit to the
  align step as its own bundle and, in its lifted combine, wraps every element in an accumulator of its own,
  so before the lazy expansion every unit cost (1 + k) × (1 + slices) encodings of 7 × samples doubles.
  Measured on 1000 sessions × 8 rows, bootstrap 1000, one blend + one temperature fit, one quantile table
  (a local bench class kept out of the repository): 104 s before, 40 s after — the same 39 s as without
  bootstrap, so the replicate sums no longer cost anything a local run can feel; 4000 sessions (32 k rows)
  take 141 s, linear in the units. The fit passes now read a
  per-split filtered copy of the units (`FitUnits_<split>`), so an unrolled blend pass past convergence
  decodes the selection split only.

## 5. Tests

- Pure, hand-computed: `EvaluationScorerTest` (a grouped unit's log score / hit / Brier, tied labels and tied
  maxima, the score set reproducing the baseline, the skip reasons, deterministic Poisson weights with mean 1,
  a four-unit report with slices and the pair record of two identical sets — the pair interval collapses to
  [0, 0] and a single-unit slice to a degenerate interval — binomial prior mode deriving the reference, the bin
  function, Wilson, the calibration records from bins filled through `addBin`, a dead heat counted whole in
  the tables while the log score uses ỹ, the integrity notes: a duplicate row, a slice / dimension varying
  within a unit, a split without units), `EvaluationSpecTest` (the layout with
  shared columns, every split / prediction / table / slice / bootstrap rule, the role defaults from the
  feature lineage, the hash ignoring `manifest`).
- e2e (`EvaluationTransformTest`, DirectRunner): sessions of listings with the exact conditional baseline,
  the true probabilities (Δ > 0 with an interval above 0), a copy of the baseline (Δ = 0 exactly, a degenerate
  interval), a score set on top of the baseline, the pair record equal to the excess, the tracks partitioning
  the units, the quarter buckets, four calibration tables (the field table's declared bounds, the edge groups
  where the realised rate follows the model), the units and summary counts (unassigned rows); a binomial run
  with column splits, prior mode, an expression label and a cluster bootstrap unit.

## 6. Deferred (design §11)

The gaussian / ranking families, `contributions`, `compareWith`, the HTML report; a baseline-drawn parametric
null for the slice discovery.
