# Diagnostics: code → meaning → fix

The report lists `level[code] location: message`. **error** fails assembly (the pipeline does not
start); **warning** and **hint** are advice; **info** explains a decision the compiler made. Codes are
stable; messages may change. Fix errors top-down: many later errors are consequences of an earlier
one (the compiler says `caused by: ...` and reports `reference.unresolved` as info for blocks it could
not expand because another block failed).

## Sources contract

| code | level | meaning / fix |
|---|---|---|
| `sources.missing` / `sources.invalid` | error | the document is empty or not a list (or an object with a `sources` list) |
| `sources.name` / `sources.eventTime` / `sources.fields` | error | required keys of a source |
| `sources.duplicate` | error | two sources share a name |
| `sources.mutability` | error | must be `appendOnly` or `corrections` |
| `sources.availability` | error | the table-level `availability` expression does not parse (see the expression table in reference.md) |
| `sources.snapshotOf` | error | `snapshotOf` needs `source` and `at`, or `at` does not parse |
| `sources.snapshotOf.appendOnly` | warning | `snapshotOf` on an `appendOnly` source does nothing (first value = final value) — remove it or change `mutability` |
| `sources.fields.name` / `sources.fields.type` | error | required; unsupported type names are rejected |
| `sources.fields.duplicate` | error | duplicate field in one source |
| `sources.fields.availableAt` | error | the field's `availableAt` does not parse |
| `sources.fields.observedAtField` | error | a pre-event relative claim (`event_time - δ`, `atRowCreation`) has no `observedAtField`: add the observation-time column, or declare `evidence: declared` explicitly |
| `sources.fields.evidence` | error | must be `measured` or `declared` |
| `sources.fields.declaredMarket` | error / warning | `kind: market` with `evidence: declared` is an error; `allowDeclared: true` + `justification` turns it into this warning. Prefer adding `observedAtField` |
| `sources.fields.allowDeclared` | error | `allowDeclared: true` without a `justification` string |
| `sources.observedAt.missingInput` | warning | the declared `observedAtField` is not in the input relation, so the observedAt audit of that field cannot run: pass the observation-time column through from upstream |
| `duration.invalid` | error | an ISO-8601 duration does not parse (`PT30M`, `P6D`, `P1Y`; no `1d`) |

## Spec structure

| code | level | meaning / fix |
|---|---|---|
| `lineage.missing` / `lineage.invalid` | error | `lineage` is required; each entry needs `fields` and `from` |
| `lineage.source` | error | `from` names an unknown source |
| `lineage.field` | error | a lineage field is not declared in that source's `fields` |
| `lineage.duplicate` | error | one field mapped by two entries |
| `lineage.missingInput` | error | a lineage field is not in the real input schema (typo, or the upstream step does not emit it) |
| `lineage.undeclared` | warning | an input field has no lineage entry; features cannot read it. Add it if needed |
| `time.field` / `time.field.type` | error | required; must be timestamp / datetime / date |
| `time.mismatch` | error | a source's `eventTime` differs from `time.field`; mixed event times need `lineage[].eventTime` |
| `time.orderTieBreak` | hint | sequence / encoding features without a tie-break: declare one (`[event_id]`) for deterministic results |
| `predictAt.missing` / `predictAt.invalid` | error | required; `event_time ± duration` or `event_time` |
| `entities.invalid` / `entities.duplicate` | error | each entity needs `name` and `keys`; unique names |
| `contexts.invalid` / `contexts.duplicate` | error | same for contexts |
| `baselines.invalid` | error | each baseline needs `name` and `expr` |
| `baselines.context` | error | the baseline's `context` is unknown |
| `baselines.declaredMarket` | error | a baseline reads a `market` field with `evidence: declared`; baselines must be time-consistent (`measured`, or `allowDeclared`) |
| `fit.mode` | error | `expanding`, `static` or `fold` |
| `fit.orderBy` | error | must equal `time.field` |
| `fit.groupBy` | error | must name an entity |
| `fit.folds` | error | at least 2 |
| `fit.minHistory` | warning | accepted, not implemented |
| `engine.rowId` | error | every `rowId` field must be an input field |
| `engine.spill.memoryMB` | error | integer ≥ 1 |
| `input.reserved` | error | an input field is named `__rowId` or `__partial`; rename it upstream |
| `output.groupBy` | error | must name a context |
| `output.nullPolicy` / `output.passThrough` | error | `keep \| fillZero \| indicator` / `all \| keys \| none` |
| `output.childName` | error | collides with an input field |
| `output.roles` / `output.roles.unknown` / `output.roles.value` | error | `roles` must be an object of `group \| time \| entity \| label \| baseline \| weight` → one name |
| `output.roles.unresolved` | error | a role names nothing: an input field (any role), a context (`group`), an entity (`entity`), a baseline or output column (`baseline`) |
| `output.roles.baseline.notEmitted` | warning | the `baseline` role names a baseline, which is an intermediate column: derive it as a feature (`shareOfTotal`) and name that column |
| `output.roles.time` | warning | the `time` role differs from `time.field` |
| `output.include.unresolved` | error | `include` must be a list (a URI is resolved before compile; a bare string reached the compiler) |
| `output.include.unknown` | warning | listed names match no column of this plan (the list may come from another plan version) |
| `output.include.exclude` | info | both declared: `include` is the projection, `exclude` is ignored |
| `audit.observedAt` | error | `count \| fail \| off` |

## Feature blocks (all scopes)

| code | level | meaning / fix |
|---|---|---|
| `features.missing` / `features.name` / `features.duplicate` | error | non-empty list; every block named; unique names; no `.`, no leading `_` |
| `features.scope` | error | `row \| context \| sequence \| population` |
| `features.combine` | error | `product` or `zip` |
| `column.duplicate` | error | two blocks expand to the same column (or output name); rename with `as:` or the block name |
| `column.shadowsInput` | error | a feature has an input field's name; in-place overwrite is not supported — choose another name |
| `reference.unknown` | error | a field / feature / `task.target` name does not exist (typo, missing lineage, or a column of a block that failed) |
| `reference.unresolved` | error / info | references that never resolved (unknown name or a cycle); as **info** it only means "not expanded because of an earlier error" |
| `reference.cycle` | error | blocks reference each other in a cycle |
| `computeAt.invalid` / `computeAt.afterPredictAt` | error | `event_time ± duration`, not after `predictAt` |
| `ops.invalid` / `ops.type` | error | ops are strings or objects with `type` |
| `window.invalid` / `window.both` | error | `window` is an object with `maxEvents` / `maxAge` / `filter`; do not use `window` and `windows` together |
| `window.nearEdge` | error | a window key that would set the near edge; the near edge comes from `ingestionLag` |
| `filter.parse` / `predicate.parse` | error | the condition text does not parse (Filter grammar) |
| `filter.quoted` / `predicate.quoted` | info | a column named like a reserved word was quoted automatically |
| `validFor.alwaysExpired` | warning | the column's `validFor` expires before `predictAt` for every row — the value is always null |

## Availability (leak check)

| code | level | meaning / fix |
|---|---|---|
| `availability.violation` | error | an **emitted** column needs information available after `computeAt` / `predictAt`. Either it is a true leak (remove it, or move the outcome into a sequence / encoding where past rows are fine), or the source declaration is wrong (an attribute declared `after(event)`), or the column is meant as an intermediate only (exclude it, or reference it from another block so it becomes `_`-prefixed) |
| `availability.intermediate` | info | the column is post-event and consumed by another block: kept as a `_` intermediate, not emitted |
| `availability.deferred` | info | the verdict waits for other blocks that failed to expand |
| `availability.windowShift` | info | the history near edge moved back by settlement + ingestion lag: expected for outcome inputs. Check the amount |
| `availability.runtimeFilter` | info (then an engine error) | not statically decidable (`atRowCreation`, `event_date THH:MM`): the engine rejects it today — declare a constant `availableAt` / `ingestionLag` |
| `evidence.declared` | warning | the column derives from a `declared` (unauditable) pre-event claim; add `observedAtField` upstream when possible |
| `context.rowSetDrift` | warning | the context is keyed on a `corrections` source without `snapshotOf`: the training row set is the final one, serving sees the pre-event one. Declare `snapshotOf` or accept the drift consciously |
| `encoding.target.preEvent` | hint | the target is pre-event; expanding is not essential for leak safety here (fine to keep) |

## Row

| code | level | meaning / fix |
|---|---|---|
| `row.type` | error | `expr` or `type: datetime \| bin \| cross \| indicator \| equals \| residual` |
| `row.input` | error | the type needs exactly one `input` |
| `row.expr.type` | error | an operand is not numeric / bool — expressions are evaluated as doubles; use `cross` / `indicator` / `equals` for strings |
| `row.self` | error | `$self` is only valid inside `window.filter` |
| `row.datetime.input` / `row.datetime.derive` | error | input must be a time field; derivation must be one of year / month / day / dayOfWeek / dayOfYear / weekOfYear / hour / minute (hour / minute not on a date) |
| `row.bin.edges` | error | `bin` needs `edges` |
| `row.cross.inputs` / `row.equals.inputs` | error | `cross` ≥ 2 inputs; `equals` exactly 2 |
| `row.indicator.values` | error | `indicator` needs `values` |
| `row.residual.baseline` / `row.residual.on` | error | `baseline` must name a baseline; `on` is identity / logit / log |

## Context

| code | level | meaning / fix |
|---|---|---|
| `context.unknown` | error | `context` must name a declared context |
| `context.ops` / `context.fields` | error | `ops` required; a field-taking op needs `fields` or block `inputs` |
| `context.op` / `context.op.type` | error | unknown op, or wrong input type (numeric op on a categorical field) |

## Sequence

| code | level | meaning / fix |
|---|---|---|
| `sequence.entity` / `sequence.ops` | error | `entity` must name an entity; `ops` required |
| `sequence.op` / `sequence.op.type` | error | unknown op / wrong input type |
| `sequence.fields` | error | the op needs `field` / `fields` / `expr` (only `aggregate` with `funcs: [count]` may omit them) |
| `sequence.predicate` | error | `sinceEvent` / `countMatch` need `predicate` |
| `sequence.self` | error | `$self` inside an op `expr` / `predicate`; use `window.filter`, or `lag` + a row `expr` |
| `sequence.aggregate.func` | error | unknown aggregate function |
| `sequence.ewma.halflife` / `sequence.ewma.decayBy` | error | `halflife` required; `decayBy` is `events` or `time` |
| `sequence.runLength.value` | error | `runLength` needs `value` |
| `sequence.filter.reduced` | info | a same-field `$self` equality filter became an extra partition key (good: hot entities split) |
| `sequence.aggregate.encoding` | hint | `mean` / `rate` over an outcome field has no shrinkage: use a population encoding with a windowed keySet |
| `sequence.window.unbounded` | hint | the column keeps every past row of its key (no `maxAge` on a scan-path op / filtered window): add `maxAge` |

## Population

| code | level | meaning / fix |
|---|---|---|
| `population.type` / `population.unsupported` | error | `type` required; only `encoding`, `factorization`, `discretize` are implemented |
| `encoding.keySets` / `encoding.targets` / `encoding.keySet.keys` | error | required parts |
| `encoding.keySet.structure` | error | `flat \| hierarchy \| cross` (`sequence` not implemented) |
| `encoding.keySet.parentRef` / `encoding.keySet.cross` | error | `hierarchy` needs `parentRef`; `cross` needs ≥ 2 keys |
| `encoding.hierarchy.entry` / `encoding.hierarchy.key` | error | entries are key lists, `additive` or `[]`; keys must exist |
| `encoding.hierarchy.additive` | error | `additive` once, last before `[]`, and the single-key keySets (same windows) must exist in the block |
| `encoding.hierarchy.scale` | error | a lattice with `additive` needs `shrinkage.scale` |
| `encoding.stat` | error | unknown stat (available: count, share, mean, rate, std, distribution, quantile, quantile<NN>, q<NN>) |
| `encoding.stat.target` | error | the stat needs a target `field` / `expr` |
| `encoding.stat.static` | error | `quantile` / `distribution` are expanding-only; use `fit.mode: expanding` or another stat |
| `encoding.nested` | error | nested targets (`field.ref`) not implemented |
| `encoding.offset` / `encoding.offset.computeAt` / `encoding.offset.scale` | error | offset must name a baseline; offset blocks compute at `predictAt`; offset with logit / log scale not implemented |
| `encoding.shrinkage.estimator` | error | `backoff` on an overlapping lattice (additive / cross) is invalid; `joint` not implemented (use `sequential`) |
| `encoding.shrinkage.weights` | error / info | `fixed \| varianceComponents` (`heldOut` not implemented); as info: variance components are estimated from the whole batch |
| `encoding.shrinkage.priorWeight` / `.scale` / `.output` | error | numeric ≥ 0 / identity-logit-log / composed-deviations-effectiveN |
| `encoding.shrinkage.family` / `.parentStatistic` | warning | not implemented, ignored |
| `encoding.smoothing.type` / `.priorWeight` | error | legacy block: `type: bayesian`, numeric `priorWeight` |
| `encoding.emitConfidence` | warning | not implemented, ignored |
| `encoding.zip` | warning | `combine: zip` with unequal list lengths drops the extras |
| `encoding.empty` | error | the block expands to nothing |
| `encoding.maxFeatures` / `factorization.maxFeatures` | error | the expansion exceeds `maxFeatures`: raise it deliberately or cut keySets / targets |
| `encoding.globalKey` | hint | a key-less stage (global level / `share` denominator) runs on one thread and is the critical path: consider `fit.mode: static` / `fold` for that block (values change — a modeling decision) |
| `fit.mode.static` / `fit.mode.fold` | info | how the block is fitted, where the artifact goes, and the caveat (static: rows see their own outcome; fold: other folds contain later rows) |
| `fit.mode.static.windows` | warning | keySet windows are ignored in static / fold |
| `fit.fold.identity` | warning | fold by `time.field` alone (no `groupBy`, no tie-break): rows sharing a timestamp share a fold — declare `orderTieBreak` |
| `fit.groupBy.required` | error | a key derives from a past target and `fit.mode: fold` needs entity-level folds: set `fit.groupBy` |
| `factorization.fields` / `.latentDim` / `.task` / `.outputs` / `.offset` / `.variant` / `.als` | error | ≥ 2 categorical fields; latentDim ≥ 1; `task.target` or `task.expr`; outputs as `pair` / `embedding` / `sum` naming the block's fields; offset names a baseline; variant `fm \| fwfm`; numeric ALS settings |
| `factorization.fit.mode` / `discretize.fit.mode` | error | these types are always `static` |
| `factorization.fit.*` / `discretize.fit.*` | warning | `cadence` / `window` / `warmStart` not implemented |
| `discretize.input` / `.bins` / `.minSamplesPerBin` / `.method` / `.target` | error / warning | numeric input; bins ≥ 2; minSamplesPerBin ≥ 1; only `quantile`; `target` is ignored by `quantile` |

## Engine errors at assembly (after a clean compile)

Reported as `engineErrors` in the dry run / `IllegalModuleException` at launch:

| message | fix |
|---|---|
| `sequence / population features are supported in batch only` | the pipeline is streaming; keep row / context features only, or run in batch |
| `fit.mode fold is supported in batch only` | use `static` with an artifact for streaming |
| `<column>: per-row availability filtering (atRowCreation / event_date time) is not implemented` | declare a constant `availableAt` / `ingestionLag` for that field |
| `<column>: stat '...' is not implemented yet` | the stat is not served by the engine; see `encoding.stat` |
| `fit.mode static in streaming requires an existing artifact for plan <hash>` | fit with a batch run first (same config, `artifact.uri`), then run streaming |
| `fit.mode static targets/offsets/inputs [...] are computed in the same fit stage and would read null; split them into a separate feature step` | the fit reads a column produced by the same static block; move the producer into an earlier `feature` step |

## Failures at run time

Per-row failures (`failFast: true` fails the job; otherwise they go to the failure sink) carry one of
`Failed to prepare feature input` (null `time.field`), `Failed to evaluate row / context / keyed
features`, `Failed to apply fitted features`, `Failed to finalize (grouped) features`, or the merge
messages `Fan-out merge: engine.rowId is not unique (row id ...)` (duplicate input rows or a non-unique
`engine.rowId`), `Fan-out merge: n of m branches produced row id ...` (a branch failed for that row —
look for the branch's own failure record), `Fan-out merge: a partial row has no base row`. A keyed
stage whose sort / spill fails routes every row of that key (`Failed to sort keyed rows` / `Failed to
read the spilled rows of a key`): usually the worker disk is full — see sizing.md.
