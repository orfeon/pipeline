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
| `sources.fields.name` / `sources.fields.type` | error | required; a scalar type name (`float64`, `int64`, `string`, `timestamp`, `date`, ...) or `array<element type>` (e.g. `array<float64>`, the `svd` vector input; the element must be a scalar type, so nested arrays are rejected); other type names are rejected |
| `lineage.type.mismatch` | error | a field is declared `array<...>` in the sources contract but the input schema field is a scalar (or the reverse): the engine would read null for every row — fix the contract type or the upstream schema (`mode: repeated`) |
| `sources.fields.duplicate` | error | duplicate field in one source |
| `sources.fields.availableAt` | error | the field's `availableAt` does not parse |
| `sources.fields.observedAtField` | error | a pre-event relative claim (`event_time - δ`, `atRowCreation`) has no `observedAtField`: add the observation-time column, or declare `evidence: declared` explicitly |
| `sources.fields.evidence` | error | must be `measured` or `declared` |
| `sources.fields.declaredMarket` | error / warning | `kind: market` with `evidence: declared` is an error; `allowDeclared: true` + `justification` turns it into this warning. Prefer adding `observedAtField` |
| `sources.fields.allowDeclared` | error | `allowDeclared: true` without a `justification` string |
| `entity.minInterval` | info | the entity's declared `minInterval` lets windows over it read an outcome without a shift (`staticSafe`); it is trusted, not checked — the plan's audit query counts the input's events that follow their predecessor sooner, and the run counts them as `feature/minInterval_<entity>_below`. A count above zero: declare the interval the data has |
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
| `baselines.expr.op` | error | the baseline expression calls a context op that cannot be called that way (`softmax`, `ratingProb`, `residualize`, `harville`, `shuffle` take parameters of their own — declare them as ops of a context block), or calls one without naming the `context` it is computed over |
| `baselines.declaredMarket` | error | a baseline reads a `market` field with `evidence: declared`; baselines must be time-consistent (`measured`, or `allowDeclared`) |
| `fit.mode` | error | `expanding`, `static`, `fold` or `forward` |
| `fit.blocks` / `fit.blocks.bucket` / `fit.blocks.size` / `fit.blocks.field` / `fit.minBlocks` | error | forward blocks: `{bucket: year \| quarter \| month \| week \| day}` or `{size: <positive duration>}` (not both); `field` must be `time.field`; `minBlocks` ≥ 1 |
| `fit.orderBy` | error | must equal `time.field` |
| `fit.groupBy` | error | must name an entity |
| `fit.folds` | error | at least 2 |
| `fit.fold` / `fit.fold.by` | error | `fold` is `{by: row \| time, purge, embargo, until}` |
| `fit.fold.negative` | error | `fold.purge` / `fold.embargo` must be non-negative durations |
| `fit.fold.until` | error | `fold.until` must be an ISO-8601 instant, date-time or date, UTC (`2025-06-30T00:00:00Z` / `2025-06-30T00:00:00` / `2025-06-30`) |
| `fit.fold.ignored` | warning | `fold` settings outside `mode: fold`, or `purge` / `embargo` / `until` without `by: time` — ignored |
| `fit.fold.until.crossFit` | info | under `until` the training rows get cross-fit values and the later rows forward ones: within a key a cross-fit value moves against the row's own outcome (leave-one-out leak, strongest for keys with many rows and a stable rate), so a model trained on the training rows and evaluated on the later ones sees two distributions — use `fit.mode: forward` when the column must behave the same on every row |
| `fit.fold.purge` (info) | info | the time fold's purge defaults to the horizon of the label the target reads; declare `fold.purge` to override |
| `fit.fold.time.joint` | error | `estimator: joint` solves hash folds only: use `by: row`, or backoff / sequential (one error per block) |
| `fit.minRows` | error / info | error: must be ≥ 0 (0 = no floor). info (top level): it is the fewest rows a `smooth` / `svd` / `quantileTransform` / `spectralEmbedding` fit is solved from; encodings shrink a thin level instead and ignore it |
| `fit.align` | error / info | error: must be `procrustes`, `sign` or `none`. info (top level): how the forward fits of an `svd` / `spectralEmbedding` block are brought into the coordinates of the fit before them; the other fits ignore it |
| `<type>.fit.align` | warning | `fit.align` on a static `svd` / `spectralEmbedding` (solved once: nothing to align to) or on a type without that freedom (`smooth`, `quantileTransform`, `discretize`, `factorization`, `encoding`); it is ignored |
| `<type>.fit.minRows` | warning | `encoding` / `discretize` / `factorization` do not take `fit.minRows`; it is ignored (an encoding shrinks a thin level instead) |
| `fit.minHistory` | info | the minimum history of a `fit.mode: forward` block, rounded up to whole blocks (`minBlocks` wins); ignored by the other modes |
| `engine.rowId` | error | every `rowId` field must be an input field |
| `engine.spill.memoryMB` | error | integer ≥ 1 |
| `engine.rowWidth` | hint | a keyed stage groups rows that still carry a map column (a `distribution` map read by a later stage or emitted): a map over many categories makes a wide row, and a hot key's spill grows with row width × row count — the audit's `row_count` bounds the count only. The hint reads the engine that runs: with parallel waves a map that only its readouts read is dropped as soon as the levels are merged (emit the readouts — `ownValueProb` / `entropy` / `expected` — rather than `distribution`, or keep the map-reading block away from the widest key); in the linear chain (`engine.parallelWaves: false`, or no wave with two stages) the readouts run in the last stage, so a consumed map rides every keyed stage before it — switch the waves on. The plan's `-- carry` section lists per keyed stage what its rows carry in either engine |
| `input.reserved` | error | an input field is named `__rowId` or `__partial`; rename it upstream |
| `output.groupBy` | error | must name a context |
| `output.nullPolicy` / `output.passThrough` | error | `keep \| fillZero \| indicator` / `all \| keys \| none` |
| `output.childName` | error | collides with an input field |
| `output.roles` / `output.roles.unknown` / `output.roles.value` | error | `roles` must be an object of `group \| time \| entity \| label \| baseline \| weight` → one name |
| `output.roles.unresolved` | error | a role names nothing: an input field (any role), a context (`group`), an entity (`entity`), a baseline or output column (`baseline`) |
| `output.roles.baseline.notEmitted` | warning | the `baseline` role names a baseline, which is an intermediate column: derive it as a feature (`shareOfTotal`) and name that column |
| `output.roles.time` | warning | the `time` role differs from `time.field` |
| `output.include.unresolved` | error | `include` must be a list (a URI or `{from: [...], mode}` is resolved before compile; an unresolved form reached the compiler) |
| `output.include.empty` | error | `include` is an empty list (a screening step that passed nothing, or a broken file): remove `include` to emit every column, or list the columns to keep |
| `output.include.unknown` | warning | listed names match no column of this plan (the list may come from another plan version) |
| `output.include.exclude` | info | both declared: `include` is the projection, `exclude` is ignored |
| `output.include.role` | info | columns that `roles` name (a baseline's `emit` copy, a label derived as a column) are emitted although the list does not name them: a pass list never contains role columns, and the consumer's manifest needs them (a column kept only as a role gets no `_isnull` indicator) |
| `output.exclude.role` | info | the same for `exclude`: a role column matching an exclude pattern (`derivedFrom:market` on a market baseline's `emit` copy) stays emitted — roles are the data contract, not features |
| `output.exclude.unmatched` | warning | an `exclude` pattern selects no emitted column. Patterns are `<block>.*`, an exact canonical column name, a block name, or a `derivedFrom:` / `evidence:` / `scope:` / `block:` selector — never a glob or regex (`dm.*__distribution` matches nothing; write `dm.*` or the full column name) |
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
| `window.as` | error | a window's name for the generated columns: letters, digits and `_` starting with a letter — and two windows of a block named the same must select the same rows (`maxAge` / `maxEvents` / `clock` / `filter`), since the statistics behind the name are shared |
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
| `row.type` | error | `expr` or `type: datetime \| bin \| cross \| indicator \| equals \| residual \| noise \| vector` |
| `row.input` | error | the type needs exactly one `input` |
| `row.expr.type` | error | an operand is not numeric / bool — expressions are evaluated as doubles; use `cross` / `indicator` / `equals` for strings |
| `row.self` | error | `$self` is only valid inside `window.filter` |
| `row.datetime.input` / `row.datetime.derive` | error | input must be a time field; derivation must be one of year / month / day / dayOfWeek / dayOfYear / weekOfYear / hour / minute (hour / minute not on a date) |
| `row.bin.edges` | error | `bin` needs `edges` |
| `row.cross.inputs` / `row.equals.inputs` | error | `cross` ≥ 2 inputs; `equals` exactly 2 |
| `row.indicator.values` | error | `indicator` needs `values` |
| `row.residual.baseline` / `row.residual.on` | error | `baseline` must name a baseline; `on` is identity / logit / log |
| `row.noise.seed` / `row.noise.distribution` | error | `noise` needs an integer `seed`; `distribution` is normal / uniform |
| `row.noise.identity` | warning | no `time.orderTieBreak`: rows sharing a timestamp get the same draw; declare a tie-break |
| `row.vector.input` | error | `vector` needs one `input` that is an array of numbers — declare the field as `array<float64>` in the sources contract |
| `row.vector.funcs` | error | `funcs` is missing, lists an unknown readout (the message lists the available ones) or lists one twice |
| `row.vector.slice` / `row.vector.diff` / `row.vector.normalize` / `row.vector.position` | error | `slice` is an object `{from, to}`; `diff` ≥ 0; `normalize` is sum / mean / l2 / zscore; `position` is index / unit |
| `row.vector.degree` | error / warning | polyfit `degree` must be 1..5; a warning when `degree` is set but `funcs` has no `polyfit` |
| `row.vector.coefficients` | error / warning | `coefficients` lists distinct integer indices in `0..degree` (the polyfit coefficients to emit); a warning when it is set but `funcs` has no `polyfit` |
| `row.vector.resample` | error | `resample` is the length the vector is interpolated onto: ≥ 1 |
| `row.vector.pad` | error | `pad` is an object `{length: ≥ 1, mode: edge \| zero, side: end \| start}` — a missing `length` or an unknown key is an error (a typo would otherwise leave the vector at a varying length) |
| `baselines.emit.duplicate` | error | `emit` name collides with a column or input field |

## Context

| code | level | meaning / fix |
|---|---|---|
| `context.unknown` | error | `context` must name a declared context |
| `context.ops` / `context.fields` | error | `ops` required; a field-taking op needs `fields` or block `inputs` |
| `context.op` / `context.op.type` | error | unknown op, or wrong input type (numeric op on a categorical field) |
| `context.softmax.offset` | error | `offset` must be a `baselines[].name` or a numeric column |
| `context.softmax.temperature` / `.offsetScale` / `.scoreNull` | error | temperature must be a number > 0; offsetScale probability / log; scoreNull zero / null |
| `context.softmax.temperatureFrom.unresolved` | error | `temperatureFrom` must be a URI (resolved before compile); a document that is neither a number nor JSON with `temperature` / `T` fails at resolve |
| `context.softmax.excludeSelf` | warning | `excludeSelf` has no effect on softmax |
| `context.ratingProb.sigma` / `.beta` | error | `sigma` names a numeric column (the row's rating uncertainty, e.g. `<rating>_team_sigma`; not a number) and belongs to one strength: an op that names it takes one `field` (one op per field otherwise), `beta` (required, > 0) is the performance noise of the rating the field comes from (its `beta`: `sigma / 2` of the prior by default, 25 / 6 for the default prior) |
| `context.ratingProb.excludeSelf` | warning | `excludeSelf` has no effect on ratingProb (the row is part of its own contest) |
| `context.residualize.against` | error | `residualize` needs `against` — one numeric field / column or a list of distinct ones, none of them the field itself (the key is `against`; a bare `on` is a YAML boolean). A regressor produced by the same block must be declared **before** the `residualize` op; one declared later is not yet a column |
| `context.harville.top` / `.discount` / `context.op.maxGroupSize` | error | `top` lists distinct integer places in 1..3; `discount` at most two positive exponents (2nd, 3rd place); `maxGroupSize` ≥ 2 |
| `context.op.maxGroupSize` | warning | `maxGroupSize` on `residualize`: the key is read by `harville` only and is ignored |
| `context.op.groupSolver` | info | a group solver runs on one worker: `harville` is quadratic (2nd place) / cubic (3rd) in the group size and a group with more than `maxGroupSize` valid rows reads null. `residualize` is linear in the group size with or without `excludeSelf` and raises no cost info |
| `context.harville.excludeSelf` | warning | `excludeSelf` has no effect on harville |
| `context.shuffle.seed` | error | `shuffle` needs an integer `seed` |
| `context.shuffle.identity` | warning | no `time.orderTieBreak`: rows sharing a timestamp are ordered by their input values only |

## Sequence

| code | level | meaning / fix |
|---|---|---|
| `sequence.entity` / `sequence.ops` | error | `entity` must name an entity; `ops` required |
| `sequence.op` / `sequence.op.type` | error | unknown op / wrong input type |
| `sequence.fields` | error | the op needs `field` / `fields` / `expr` (only `aggregate` with `funcs: [count]` may omit them) |
| `sequence.predicate` | error | `sinceEvent` / `countMatch` need `predicate` |
| `sequence.self` | error | `$self` inside an op `expr` / `predicate`; use `window.filter`, or `lag` + a row `expr` |
| `sequence.aggregate.func` | error | unknown aggregate function |
| `sequence.weightBy.op` / `sequence.weightBy.func` | error | `weightBy` is only defined on `aggregate`, for count / sum / mean / avg / rate / std (min / max / first / last have no weighted form) |
| `sequence.weightBy.type` / `sequence.weightBy.parse` | error | the weight is a numeric expression: operands (past fields by name, `$self.<field>` for the current row) must be numeric / bool — or a string, compared by identity (a timestamp is neither) |
| `sequence.weightBy.identity` | info / error | a string operand is compared by identity (the expression reads a stable hash of its text): `c == $self.c ? 1 : 0.25` is exact, and so is `!=`. Reading such an operand outside `==` / `!=` (a difference, a kernel, `<` / `>`) is an **error** — the hash is not a magnitude. A numeric-looking text is read as its number, so `"007"` and `"7"` are one category |
| `sequence.weightBy.block` | warning | `weightBy` declared on the block is the default of its aggregate ops (an op's own wins). The warning says it applied to nothing: the block has no aggregate op, or the op's `funcs` (`min` / `max` / `first` / `last`) have no weighted form and were left unweighted |
| `sequence.weightBy.scan` | info | a weighted aggregate has no running state and scans its window per row: give the window `maxAge` or `maxEvents` (otherwise `sequence.window.unbounded`) |
| `sequence.ewma.halflife` | error | `halflife` required and positive |
| `clock.unknown` | error | `window.clock` / `decayBy` / `fit.blocks.clock` names neither a built-in clock (`time`, `events`) nor a calendar declared in the sources' `clocks:` |
| `clock.fit` | error | a keySet window on a calendar under `fit.mode: forward` needs `fit.blocks` on the same clock (`{size: <ticks>, clock: <name>}`) |
| `clock.direction` | error | a `direction: future` window measures its horizon on wall time (an ISO-8601 `maxAge`) |
| `window.clock` / `fit.blocks.clock` | error | on a calendar, `maxAge` / `blocks.size` are whole numbers of ticks (`clock: events` is spelled `maxEvents`; blocks on a clock take no `bucket`) |
| `window.clock.hidden` | warning | a window on a calendar clock whose wall-time shift (an outcome's settlement + ingestion lag) covers all its ticks on average: the window holds no row. Widen `maxAge` beyond the shift in ticks (the `availability.windowShift` info states it), or read a wall-time window |
| `sources.clocks` / `.name` / `.type` / `.dates` / `.uri` | error | `clocks:` entries are `{name, type: calendar, dates: [yyyy-MM-dd, ...] \| uri}`; the name is not `time` / `events` |
| `sequence.form` | error | a block has both `ops` and `lift` / `summarize`: split it into two blocks |
| `sequence.lift` / `sequence.lift.type` | error | the general form needs `lift: {fields / exprs / timeAugment}`; channels must be numeric (or bool) |
| `sequence.lift.timeAugment` | warning | `timeAugment` at order 0 adds no column (the constant channel's component 0 is always 1) |
| `sequence.lift.align` | info | the block's channels are available at different times; the `time` channel follows the latest (its window is shifted like that channel's) |
| `sequence.expr.anonymous` | info | an op `expr` without `as:` is named `<block>__e{n}` by a spec-wide counter (renumbers when another expression is added / removed): add `as:` |
| `sequence.lift.anonymous` | info | an unnamed `lift.exprs` entry is named `<block>__e{n}` by a spec-wide counter (renumbers when another expression is added / removed): write `{expr: "...", as: name}` |
| `sequence.lift.name` | error | two channels of a block share a name (a field and an `as`, or two `as`): set a distinct `as` |
| `sequence.summarize` | error | the general form needs `summarize: {dynamics: {family: lti, measure: ...}}` |
| `sequence.dynamics.family` | error | `family` is `lti` or `bilinear` (`probabilistic` is not implemented) |
| `sequence.dynamics.type` / `sequence.dynamics.depth` | error | bilinear takes `type: logsignature` and `depth` 1..4 |
| `sequence.dynamics.channels` | warning | a log-signature of one channel is its total increment only: lift two channels or add `timeAugment` |
| `sequence.dynamics.logsignature` | info | the letter legend of the log-signature columns (a = the first lift channel, …) |
| `sequence.dynamics.measure` / `.order` / `.halflife` / `.period` / `.decayBy` / `.parameter` | error | `measure` exponential / fourier / legendre; order 0..16 (legendre 0..8, fourier from 1); exponential needs `halflife`, legendre has none; fourier needs `period`; `decayBy` events / time / a declared calendar (`clock.unknown` otherwise); no other keys |
| `sequence.dynamics.size` | error | the block emits more than 64 component columns (lower `order` / `depth`, fewer halflifes / windows, or split the channels over blocks), or a log-signature lifts more than 26 channels |
| `sequence.compress` | error / info | `compress` is `{svd: {rank, center, standardize, outputs, fit}, keep}` with two or more component columns (any other key under `compress` or `compress.svd` is an error); the info names the svd block (`<name>_svd`) |
| `sequence.runLength.value` | error | `runLength` needs `value` |
| `sequence.regression.against` / `.func` / `.lag` | error | `regression` needs a numeric `against` field (the key is `against`, a bare `on` is a YAML boolean); funcs are cov / corr / beta / intercept / r2; `lag` ≥ 0 (swap the fields for the other direction) |
| `sequence.fracdiff.d` / `sequence.fracdiff.k` | error | `fracdiff` needs `d` in (0, 2]; `k` ≥ 2 |
| `sequence.rating.context` / `.method` / `.order` / `.func` / `.parameter` | error | `rating` needs `context` (a `contexts[].name`); method is elo / bradleyTerry / plackettLuce / gaussian; order ascending / descending; funcs mu / sigma / count / delta / deviation / z (`sigma` not under elo); `sigma` / `beta` / `tau` belong to the Bayesian methods, `kFactor` / `scale` to elo; `sigma` / `beta` / `kFactor` / `scale` > 0, `tau` ≥ 0 (declare `sigma` when `mu` is 0) |
| `sequence.rating.window` | error | a rating reads every earlier contest of its pool: remove `maxAge` / `maxEvents` / the filter — only `filter: "f = $self.f"` on a pre-event field is accepted (independent pools); `tau` is what ages an old rating |
| `sequence.rating.as` | error | two `rating` ops of one block resolve to the same column segment with different parameters (they would share one running state): give them different `as:` names |
| `sequence.rating.with` | error / info | a team rating: `with` lists `entities[].name` other than the block's entity, each once (no entity called `team`), members take `entity` / `mu` / `sigma` (> 0) / `tau` (≥ 0) only; a team needs `as:` and one field, `plackettLuce`, `bradleyTerry` or `gaussian` (elo keeps no variance to share a change by), and `team:` reads `mu` / `sigma` / `count` / `deviation` of a declared team. As an info it describes the team — nothing to fix |
| `sequence.rating.globalKey` | hint | the rating stage replays every row under one key (one worker thread) — expected; split independent pools with a pre-event `$self` equality filter when the data has them |
| `sequence.direction` / `features.direction` | error | `direction` is `past` (default) or `future`, on sequence blocks only |
| `sequence.direction.maxAge` | error | a `direction: future` window needs `maxAge` (the label horizon) |
| `sequence.direction.op` | error | the op reads the window in one direction (`delta`, `trend`, `fracdiff`, a lagged `regression`), or the general form `lift` + `summarize` (past only) — not defined over the future; the message lists what is |
| `sequence.direction.future` | info | the block's columns are labels (status `label`): referencing one from a feature is a violation |
| `sequence.barrier.levels` / `sequence.barrier.direction` | error | `barrier` needs `up` > 0 and / or `down` < 0, and exists only under `direction: future` |
| `sequence.filter.reduced` | info | a same-field `$self` equality filter became an extra partition key (good: hot entities split) |
| `sequence.aggregate.func` (series) | error | also raised for `acf<j>` / `pacf<j>` / `ar<p>_<i>` with j, p outside 1..20 or i outside 1..p; the message lists every available func |
| `sequence.aggregate.encoding` | hint | `mean` / `rate` over an outcome field has no shrinkage: use a population encoding with a windowed keySet |
| `sequence.window.unbounded` | hint | the column keeps every past row of its key (no `maxAge` on a scan-path op / filtered window): add `maxAge` |
| `sequence.rating.gaussian.units` | error | `method: gaussian` reads the outcome as a margin, so `sigma` (the spread of strengths) and `beta` (the noise of one outcome) are in the outcome's units and have no default: declare both (`sigma: 1, beta: 0.5` for a standardised margin; `mu` defaults to 0, a typical outcome otherwise) |
| `sequence.fit` | error | a sequence block takes `fit: {artifact: ...}` only (a rating's state snapshot); `mode` and the other fit settings belong to population blocks |
| `sequence.fit.ignored` | warning | `fit.artifact` on a sequence block without a rating op: nothing to snapshot |
| `sequence.rating.artifact` | info | the block's `fit.artifact` (not inherited from the top level) snapshots every pool's state to `<uri>/<planHash>/<block>.rating/` after the replay; a run whose input starts after a pool's last folded contest continues from its snapshot (serving: the input = the rows to serve + every contest since), a run whose input reaches back to it replays from scratch and rewrites it (a backfill advances it; a retry never reads its own write); `refit: true` replays from scratch whatever the input; `require: true` fails a pool without a snapshot instead of replaying from the prior (set it on the serving config). Run-time counter `feature/ratingSnapshot_<state>_rowsBefore` = rows of a continued pool whose near edge lies before its snapshot (they read too much) |

## Population

| code | level | meaning / fix |
|---|---|---|
| `population.type` / `population.unsupported` | error | `type` required; every registered type is implemented: `encoding`, `factorization`, `discretize`, `quantileTransform`, `svd`, `smooth`, `transitionStats`, `spectralEmbedding` |
| `encoding.keySets` / `encoding.targets` / `encoding.keySet.keys` | error | required parts |
| `encoding.keySet.structure` | error | `flat \| hierarchy \| cross \| sequence` |
| `encoding.keySet.sequence` | error / warning / info | `structure: sequence` needs at least two keys (a path, most recent first); the info lists the derived suffix chain, the warning says the block declares no `shrinkage` so the chain is never composed (raw full-path statistic) |
| `encoding.keySet.parentRef` / `encoding.keySet.cross` | error | `hierarchy` needs `parentRef`; `cross` needs ≥ 2 keys |
| `encoding.keySet.as` | error | a keySet's name for the `{keys}` segment of the generated columns: letters, digits and `_` starting with a letter |
| `encoding.hierarchy.entry` / `encoding.hierarchy.key` | error | entries are key lists, `additive` or `[]`; keys must exist |
| `encoding.hierarchy.additive` | error | `additive` once, last before `[]`, and the single-key keySets (same windows) must exist in the block |
| `encoding.hierarchy.scale` | error | a lattice with `additive` needs `shrinkage.scale` |
| `encoding.stat` | error | unknown stat (available: count, share, mean, rate, std, distribution, quantile, quantile<NN>, q<NN>) |
| `encoding.stat.target` | error | the stat needs a target `field` / `expr` |
| `encoding.stat.static` | error | `quantile` / `distribution` are expanding-only; use `fit.mode: expanding` or another stat |
| `encoding.target.values` | error | `targets[].values` lists the categories of a `distribution` to emit as flat FLOAT64 columns (`<column>_<value>`, like `countByValue`); the target declares no `distribution` stat |
| `encoding.nested` | error | nested targets (`field.ref`) not implemented |
| `encoding.offset` / `encoding.offset.computeAt` | error | offset must name a baseline; offset blocks compute at `predictAt` |
| `encoding.offset.additive` | info | offset on a logit / log scale: the composed value is the additive term on that scale (the key's score-type estimate `Σ(y − b) / information`, one scoring step from the baseline, shrunk toward the parent's term by information) — a log-odds / log-rate ratio against the baseline, not a probability / rate; finite for a key with no success |
| `encoding.shrinkage.estimator` | error | `backoff` on an overlapping lattice (additive / cross) is invalid (use `sequential` or `joint`); `joint` needs `fit.mode: static \| fold \| forward` (rejected under `expanding`; a `distribution` there is `encoding.stat.static`) |
| `encoding.shrinkage.joint` | info / error | what the joint solve fits: the levels, λ rule and scale of the lattice (one ridge / BLUP system per keySet × target on one worker); as error: two keySets of the block (the same keys, one of them named with `as:`) resolve to that one solve with different lattices or shrinkage — declare them in separate blocks |
| `encoding.shrinkage.weights` | error / info | `fixed \| varianceComponents` (`heldOut` not implemented); as info: variance components are estimated from the whole batch |
| `encoding.shrinkage.priorWeight` / `.scale` / `.output` | error / warning | numeric ≥ 0 / identity-logit-log / composed-deviations-effectiveN; as warning: `deviations` are not defined for a shrunk `distribution` (none emitted) |
| `encoding.shrinkage.family` | error | `gaussian \| betaBinomial \| gammaPoisson \| dirichletMultinomial` (default derived from the stat) |
| `encoding.shrinkage.family.stat` | error | the declared family does not shrink that stat (gaussian / betaBinomial / gammaPoisson: `mean` / `rate`; dirichletMultinomial: `distribution`) |
| `encoding.shrinkage.family.scale` | error / warning | a *declared* conjugate family needs `scale: identity` (on logit / log declare `family: gaussian` or leave it derived: `mean` / `rate` then shrink gaussian); as warning: a `distribution` on logit / log is emitted unshrunk |
| `encoding.shrinkage.family.lattice` | error | a shrunk `distribution` supports chain lattices only (no `additive` / `cross`) |
| `encoding.shrinkage.weights.distribution` | warning | `varianceComponents` is not estimated for a shrunk `distribution`; its levels use `priorWeight` |
| `encoding.shrinkage.parentStatistic` | warning | `type` not implemented, `token` used |
| `encoding.smoothing.type` / `.priorWeight` | error | legacy block: `type: bayesian`, numeric `priorWeight` |
| `encoding.emitConfidence` | warning | not implemented, ignored |
| `encoding.zip` | warning | `combine: zip` with unequal list lengths drops the extras |
| `encoding.empty` | error | the block expands to nothing |
| `encoding.maxFeatures` / `factorization.maxFeatures` | error | the expansion exceeds `maxFeatures`: raise it deliberately or cut keySets / targets |
| `encoding.globalKey` | hint | a key-less stage (global level / `share` denominator) runs on one thread and is the critical path: consider `fit.mode: static` / `fold` for that block (values change — a modeling decision) |
| `fit.mode.static` / `fit.mode.fold` | info | how the block is fitted, where the artifact goes, and the caveat (static: rows see their own outcome; fold: other folds contain later rows) |
| `fit.mode.static.windows` | warning | keySet windows are ignored in static / fold |
| `fit.mode.forward` | info | how the forward block fit reads the data (complete known blocks, own block excluded) |
| `fit.mode.forward.window` | info | `maxAge` rounded up to whole blocks |
| `fit.mode.forward.windowIgnored` | warning | `maxEvents` / `filter` windows are ignored in forward |
| `fit.mode.forward.dynamic` | error | the target's availability is not static (`atRowCreation`): the block boundary cannot be decided per row |
| `fit.fold.identity` | warning | fold by `time.field` alone (no `groupBy`, no tie-break): rows sharing a timestamp share a fold — declare `orderTieBreak` |
| `fit.groupBy.required` | error | a key derives from a past target and `fit.mode: fold` needs entity-level folds: set `fit.groupBy` |
| `factorization.fields` / `.latentDim` / `.task` / `.outputs` / `.offset` / `.variant` / `.als` | error | ≥ 2 categorical fields; latentDim ≥ 1; `task.target` or `task.expr`; outputs as `pair` / `embedding` / `sum` naming the block's fields; offset names a baseline; variant `fm \| fwfm`; numeric ALS settings |
| `factorization.fit.mode` / `discretize.fit.mode` | error | these types are always `static` |
| `quantileTransform.fit.mode` / `quantileTransform.fit.mode.static` | error / info | quantileTransform is `static` or `forward` (expanding / fold are rejected); the info says a block declared `static` under a top-level forward fit and therefore sees the whole input |
| `svd.fit.mode` | error | `static` \| `forward` only (`expanding` / `fold` are rejected) |
| `svd.fit.mode.static` | info | the block declares `fit.mode: static` while the top-level fit is `forward`, so it alone is fitted on the whole input; drop the block's `fit.mode` to inherit the forward walk |
| `factorization.fit.*` / `discretize.fit.*` / `quantileTransform.fit.*` / `svd.fit.*` / `smooth.fit.*` | warning | `cadence` / `warmStart` not implemented; `window` applies to a forward `svd` / `quantileTransform` / `smooth` only |
| `discretize.input` / `.bins` / `.minSamplesPerBin` / `.method` / `.target` | error / warning | numeric input; bins ≥ 2; minSamplesPerBin ≥ 1; only `quantile`; `target` is ignored by `quantile` |
| `quantileTransform.input` / `.bins` / `.distribution` | error | numeric input; bins ≥ 2; `uniform \| normal` |
| `quantileTransform.clip` | error / warning | error: `clip` must be a probability in `(0, 0.5)` (default `1e-6`); warning: `clip` with `distribution: uniform` has no effect on the output but still changes the plan hash — remove it or switch to `normal` |
| `clip.invalid` | error | `clip` is not a number |
| `svd.input` / `.rank` / `.maxFeatures` | error | two or more numeric `inputs`, or one `input` field declared `type: array<float64>` (or another numeric element type) in the sources contract — no feature op produces an array (then `rank` is required); 1 ≤ rank ≤ vector length; rank columns count towards `maxFeatures` |
| `smooth.input` / `.target` / `.method` | error | numeric `input`; `target` required, numeric or boolean; only `method: spline` (`isotonic` / `rff` are not implemented) |
| `smooth.range` | error / info | error: `range: [lo, hi]` with lo < hi is required — the knots are placed before the rows are read (keys beyond the range are clamped); info: the range defaulted to `[0, 1]` because the input is a uniform `quantileTransform` column (knots at the input's quantiles) |
| `smooth.segments` / `.degree` / `.penalty` / `.outputs` | error | segments ≥ 1 and `segments + degree` ≤ 64; degree 0..5; `penalty.order` 1 \| 2 \| 3 with more basis functions than the order, `penalty.lambda` `reml` or a positive number, no other penalty keys; outputs `curve \| residual` |
| `smooth.fit.mode` / `smooth.fit.mode.static` | error / info | `static` \| `forward` only; the info says the block declared `static` under a top-level forward fit |
| `transitionStats.sequenceOf` / `spectralEmbedding.sequenceOf` | error | `sequenceOf: {entity: <entities[].name>, field: <categorical field>}` is required; a numeric field must be discretized first |
| `transitionStats.parameters` / `spectralEmbedding.parameters` | error | an unknown key or entry in `sequenceOf` / `cooccur` / `of` / `emit` / `blend` (e.g. `emit: [{fromValueProb: x}]`, an `of` entry that is not a plain value or an empty `of: []`, or one of those blocks written as something other than an object), a parameter of the *other* sequence-of-values type (`cooccur` / `of` / `maxValues` / `rank` on `transitionStats`; `order` / `emit` / `blend` on `spectralEmbedding`), or a parameter of the blocks the type expands into (`keySets` / `targets` / `shrinkage` / `naming` / `windows`, and `fit` on `transitionStats`, whose fit is always expanding) — the message lists the accepted forms |
| `transitionStats.emit` / `.order` / `.blend` / `.maxFeatures` | error | `emit: [distribution \| {toValueProb: <value>} \| ownValueProb \| surprisal \| entropy \| expected]` is required (any subset); `order` 1..4; `blend.priorWeight` a positive number; `emit` must not produce more columns than `maxFeatures`; `emit: expected` needs an integer-coded field |
| `transitionStats.emit.own` | hint | `ownValueProb` / `surprisal` read the row's own value of an outcome field: those columns are availability violations (an intermediate target or a label, not a feature); `entropy` / `expected` / `toValueProb` read the distribution only |
| `transitionStats.expansion` | info | the desugaring: the lag path, the key chain and the pseudo-count the block became (it is always an expanding distribution encoding); back-off reads the deepest level with rows (the effective leaf), leave-node-out from that level |
| `spectralEmbedding.cooccur` / `.rank` / `.maxValues` / `.of` / `.maxFeatures` | error | `cooccur.window` 1..8, `weighting: ppmi` only; rank ≥ 1 (and ≤ `maxFeatures`); `maxValues` 2..1024; `of: current \| previous`, or the list of both (then `rank` × 2 columns count against `maxFeatures`) |
| `spectralEmbedding.fit.mode` | error | `static` \| `forward` only |
| `svd.rank` | info | array input: the rank cannot be checked against the array length at compile time — a shorter array caps the components at its length (run-time warning), the surplus score columns read null |

## Engine errors at assembly (after a clean compile)

Reported as `engineErrors` in the dry run / `IllegalModuleException` at launch:

| message | fix |
|---|---|
| `sequence / population features are supported in batch only` | the pipeline is streaming; keep row / context features only, or run in batch |
| `fit.mode fold is supported in batch only` | use `static` with an artifact for streaming |
| `fit.mode forward is supported in batch only` | use `static` with an artifact for streaming |
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
