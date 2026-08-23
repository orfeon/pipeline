---
type: Transform Module
title: Feature Transform Module
description: Declarative feature generation for machine learning with built-in leakage checking. Features are declared in four scopes (row, context, sequence, population) against a sources contract that states when each field becomes known (availableAt) and when it reaches the system (ingestionLag); the module derives the availability of every output column, rejects columns that would use information unavailable at prediction time, and shifts history windows so late-arriving outcomes are only used once they would really be present. Batch mode; sequence/population features use per-key time-ordered history.
tags: [transform, feature, machine-learning, leakage, batch]
timestamp: 2026-08-23T00:00:00Z
---

# Feature Transform Module

Transform Module that generates machine-learning features from a relation using a declarative
specification. The specification separates **facts** about the input data (the *sources contract*:
field types, when a value becomes known, how late it reaches the system, whether rows get corrected)
from the **intent** (which features to compute, in which scope). From the two, the module derives for
every output column the time at which it is available and checks it against the prediction time, so a
configuration that would leak future information fails at pipeline assembly instead of producing a
model that cannot be served.

Supports:

- **row** scope — expressions, calendar decomposition (optionally cyclical sin/cos), fixed-edge binning,
  categorical crosses, residuals against a named baseline.
- **context** scope — statistics relative to the rows that co-occur in the same group (rank, z-score,
  share of total, gap to best, percentile, median difference, group size, value counts / ratios, entropy).
- **sequence** scope — per-entity strictly-past history: lag, delta, trend, EWMA (decay by events or
  time), run length, events/days since a predicate last held, count of matching rows, windowed aggregates
  (count / mean / min / max / sum / std / first / last). Windows combine `maxEvents`, `maxAge` and a
  `filter` that can reference the current row through `$self.<field>`.
- **population** scope — expanding-fit encoding: conditional statistics of a target per key set
  (count / share / mean / rate / std / distribution), optionally windowed and offset by a baseline, with
  **shrinkage** along a generalization lattice (key → parent keys → global, `additive` main effects for
  crosses): fixed or variance-components pseudo-counts, leave-node-out, identity / logit / log scale,
  composed values, per-level deviations and effective sample size. **factorization** machines (fm / fwfm,
  ALS) over categorical fields: pair interaction scores, embeddings, linear predictor.
- **Leakage checking** — every column carries a derived availability time and lineage (source, kind,
  evidence). Columns available after `predictAt` are rejected unless they are only consumed as
  intermediates; history windows over late-arriving fields are shifted automatically.
- **Grouped output** — `output.groupBy` re-aggregates rows into one record per context with a child array.

The compiled plan (expanded columns, availability status, stages, diagnostics) is logged at assembly
time; warnings and hints from the compiler are part of that report.

## Transform module common parameters

| parameter  | optional | type                              | description                                                    |
|------------|----------|-----------------------------------|----------------------------------------------------------------|
| name       | required | String                            | Step name. specified to be unique in config file.              |
| module     | required | String                            | Specified `feature`                                            |
| inputs     | required | Array<String\>                    | Input step names (flattened into one relation).                |
| waits      | optional | Array<String\>                    | Step names to wait for before processing.                      |
| strategy   | optional | [Strategy](../common/strategy.md) | Windowing strategy (batch, global window recommended).         |
| parameters | required | Map<String,Object\>               | Specify the following individual parameters                    |

## Feature transform module parameters

| parameter  | optional | type                           | description |
|------------|----------|--------------------------------|-------------|
| sources    | required | String or Object               | Sources contract (see below): inline object, or a URI / local path / `data:` reference to a YAML or JSON document. File content is rendered with FreeMarker using the step `args`. |
| lineage    | required | Array<Object\>                 | Maps input fields to their source: `{fields: [...], from: <source name>, eventTime: <field>}`. Every field used by a feature must be declared here. |
| time       | required | Object                         | `field`: the event-time field of the input (must equal the sources' `eventTime`). The transform re-timestamps every element from this field, so it is the time axis of all history windows regardless of the source's `timestampAttribute`; rows whose value is null go to the failure output. `orderTieBreak`: fields declaring a total order for rows that share a timestamp. |
| predictAt  | required | String                         | When the features are used: `event_time - PT10M`, `event_time` (the literal event time), ... Every emitted column must be available at or before this time. |
| entities   | optional | Array<Object\>                 | Subjects of sequence features: `{name, keys: [...], minInterval: <ISO8601>}`. |
| contexts   | optional | Array<Object\>                 | Co-occurrence groups for context features: `{name, keys: [...]}`. |
| baselines  | optional | Array<Object\>                 | Named baselines: `{name, expr, context}`. `expr` may wrap a numeric expression in a context op, e.g. `share(1 / price)`. Referenced by `type: residual` (`baseline:`) and encoding `offset:`. |
| features   | required | Array<Object\> or String       | Feature blocks (see scopes below). A string is a URI / path to a document whose `features` list is used. |
| fit        | optional | Object                         | Defaults for population features (overridable per block with `fit:`): `orderBy` (= time.field), `mode` (`expanding` \| `static`; `fold` is not implemented yet), `groupBy` (entity name), `artifact` (`{uri, refit, id}` or the URI string — see *Static fits and artifacts*). `minHistory` is accepted but not implemented yet (warning). |
| output     | optional | Object                         | `prefix` (output name prefix), `nullPolicy` (`keep` \| `fillZero` — missing numeric feature values become 0 \| `indicator` — adds `<name>_isnull` flags for sequence / population / validFor columns), `exclude` (name globs such as `block.*` or lineage selectors `derivedFrom:market`, `evidence:declared`, `scope:population`, `block:<name>`), `groupBy` (context name), `parentFields` (input fields placed on the parent record). |

### Sources contract

```yaml
sources:
  - name: listings
    eventTime: session_time        # event-time field of the rows
    availability: atEventTime      # table default: known once the event exists (pre-event information)
    mutability: appendOnly         # appendOnly | corrections
    ingestionLag: PT0S             # delay between a value becoming known and reaching the input
    settlementLag: PT30M           # concretizes after(event): event_time + settlementLag
    snapshotOf: {source: listings_snapshot, at: "event_time - PT6H"}   # corrections: training values come from a point-in-time snapshot
    keys: [session_id, seller_id]
    fields:
      - {name: start_price, type: float64, kind: attribute}
      - {name: current_bid, type: float64, availableAt: "event_time - PT10M", observedAtField: snapshot_time, kind: market, validFor: PT15M}
      - {name: sold, type: int32, availableAt: after(event), kind: outcome}
```

| field attribute   | description |
|-------------------|-------------|
| `availableAt`     | `atEventTime` (pre-event), `event_time ± <duration>`, `after(event)` (= event_time + settlementLag), `atRowCreation`. Defaults to the table `availability`. |
| `ingestionLag`    | Upper bound of the delay until the value is present in the input relation. The checks use `availableAt + ingestionLag`. |
| `observedAtField` | Field holding the time the value was actually observed. **Required** for pre-event claims (`event_time - δ`, `atRowCreation`) unless `evidence: declared` is stated explicitly. |
| `evidence`        | `measured` (default with observedAtField) or `declared` (not auditable). `kind: market` with `declared` is an error unless `allowDeclared: true` with a `justification`. |
| `kind`            | Free lineage tag (`market`, `outcome`, `attribute`, ...), propagated to every derived column as `derivedFrom`. |
| `validFor`        | How long the value stays meaningful (freshness). |

### Feature scopes

```yaml
features:
  - {name: price_per_unit, scope: row, expr: "start_price / quantity"}
  - {name: time_parts, scope: row, type: datetime, input: session_time, derive: [month, dayOfWeek], cyclical: true}
  - {name: price_bin, scope: row, type: bin, input: start_price, edges: [10, 50, 100]}
  - {name: cat_grade, scope: row, type: cross, inputs: [category, condition_grade]}
  - {name: vs_market, scope: row, type: residual, input: share, baseline: market, on: identity}

  - name: relative                  # context: inputs × ops, or ops with their own fields
    scope: context
    context: session
    inputs: [start_price, current_bid]
    ops: [rank, zscore, shareOfTotal]
    excludeSelf: false
  - name: composition
    scope: context
    context: session
    ops:
      - {type: countByValue, fields: [condition_grade]}
      - {type: entropy, fields: [condition_grade]}
      - {type: groupSize}

  - name: recent                    # sequence: strictly-past rows of the entity
    scope: sequence
    entity: seller
    windows:
      - {maxEvents: 5}
      - {maxAge: P365D, filter: "category = $self.category"}
    ops:
      - {type: lag, fields: [sold, start_price], k: 2}
      - {type: delta, field: start_price, k: 1}
      - {type: trend, field: start_price, k: 5}
      - {type: ewma, expr: "sold >= 1", halflife: [3, 10], decayBy: events}
      - {type: runLength, field: condition_grade, value: good}
      - {type: sinceEvent, predicate: "sold = 1", unit: [events, days]}
      - {type: countMatch, predicate: "sold = 1"}
      - {type: aggregate, field: start_price, funcs: [count, mean, max]}

  - name: enc                       # population: expanding encoding, keySets × windows × targets × stats
    scope: population
    type: encoding
    keySets:
      - keys: [seller_id]
      - keys: [category]
        windows: [{maxAge: P365D}]
    targets:
      - {stats: [count]}
      - {field: sold, stats: [mean]}
      - {expr: "final_price / start_price", stats: [mean, std]}
    naming: "{block}__{keys}__{window}__{target}__{stat}"
    maxFeatures: 100
```

### Static fits and artifacts (fit.mode static)

```yaml
  fit:
    mode: static                                  # statistics fitted on the whole input, applied by lookup
    artifact: {uri: "gs://bucket/features", refit: false}
```

`expanding` (default) recomputes the statistics at every row from strictly-past, already-available
contributions and is the leak-safe choice for training backfill. `static` fits each lattice level once
over the whole input (windows are ignored) and applies it to every row as a pure map — rows then see
their own outcome, so use it for serving / offline analysis, not for training. With `artifact.uri` the
fitted statistics are written to `<uri>/<planHash>/<block>.avro` (+ `<block>.manifest.json`); the plan
hash covers the spec and the sources contract (everything except `fit.artifact` itself), so any change
produces a new directory. `artifact.id` pins an explicit version directory instead of the hash. When an artifact
for the current plan hash already exists it is loaded at worker setup instead of re-fitting (`refit: true`
forces a new fit) — this is the serving path: the same config, run on request data, applies the fitted
statistics without any history. Streaming runs require an existing artifact. Paths use the Beam
filesystems (`gs://`, `s3://`, relative local paths).

### Factorization (population, type: factorization)

```yaml
  - name: fm
    scope: population
    type: factorization
    variant: fwfm                                 # fm | fwfm (field-pair weights r_fg; bayesian is v2)
    fields: [seller_id, category, condition_grade] # categorical fields (≥ 2)
    latentDim: 8
    task: {target: sold, offset: market}          # target: <field> (alias field) or expr: <numeric expr>, optional baseline offset
    fit: {artifact: {uri: "gs://bucket/features"}} # always fit.mode static; cadence / window / warmStart are not implemented yet
    als: {epochs: 10, reg: 0.01, seed: 0}
    outputs:
      - {pair: [seller_id, category], as: fm_seller_category}   # r_fg · ⟨v_f[x_f], v_g[x_g]⟩
      - {embedding: category, as: cat_emb, dims: 4}            # cat_emb_0 .. cat_emb_3
      - {sum: true, as: fm_linear}                             # w0 + Σ w + Σ pairs (without the offset)
```

`ŷ = w0 + Σ_f w_f[x_f] + Σ_{f<g} r_fg ⟨v_f[x_f], v_g[x_g]⟩`, fitted by alternating least squares (closed-form
ridge update per parameter, deterministic for a seed). The whole training set is gathered on one worker
for the fit, so it must fit in memory. Unknown field values yield null outputs. The artifact
(`<planHash>/<block>.fm.avro` + manifest with the fwfm `pairWeights` ranking) is written and reused like
the static encoding artifacts.

### Shrinkage and key lattices (population)

```yaml
  - name: enc
    scope: population
    type: encoding
    keySets:
      - keys: [seller_id]                         # flat: seller → global
      - keys: [seller_id]
        structure: hierarchy                      # seller → parent (from a field of the row) → global
        parentRef: seller_group
      - keys: [seller_id]
        hierarchy: [[category], []]               # explicit lattice: seller → category → global
      - keys: [category]                          # main effect, required by the cross below
      - keys: [condition_grade]
      - keys: [category, condition_grade]
        structure: cross                          # cell → additive(main effects) → global (sequential estimator)
        shrinkage: {priorWeight: 50}              # keySet-level override
    targets:
      - {field: sold, stats: [mean]}
    shrinkage:
      weights: varianceComponents                 # fixed: w = n / (n + priorWeight); varianceComponents: λ = σ²/τ² per level (batch method of moments)
      priorWeight: 20                             # fixed pseudo-count, and the fallback when a level has too few keys
      scale: logit                                # identity | logit | log; required when a lattice uses additive
      leaveNodeOut: true                          # subtract the leaf's own statistics from every ancestor
      output: [composed, deviations, effectiveN]  # composed (default) | deviations (dev0, dev1, ...) | effectiveN (<stat>__neff)
```

`smoothing: {type: bayesian, priorWeight: N}` is accepted as the legacy spelling of fixed-weight
shrinkage toward the global mean. Every lattice level is evaluated as its own keyed stage over the same
window and target, and the composition is a per-row formula: `est(level) = est(parent) + w · (t(mean) −
est(parent))` from the global level down to the key, on the declared scale. `share` is
`n_key / n_global` over strictly-past rows.

A feature must not reuse the name of an input field (in-place overwrite is rejected). Generated column
names: row `<name>` (datetime `<name>_<derive>[_sin|_cos]`), context
`<name>_<field>_<op>`, sequence `<name>_<window>_<field>_<op><param>` (window token: `n5`, `365d`,
`365d_n20`, `all`), encoding per `naming` (empty segments collapse). `output.prefix` is prepended; columns
that are only usable offline get a leading `_` and are not emitted.

Inline `expr` in sequence ops and encoding targets is evaluated per past row (no `$self`); expressions are
numeric (Lucene expression syntax), predicates and window filters use the SQL-like
[Filter](../common/filter.md) syntax.

### Availability check

For each column the module derives `availableAt` from its inputs (max over inputs; sequence / population
columns add the constraint that a past row at t' contributes only when
`availableAt(t') + ingestionLag ≤ predictAt(t)`):

- **staticSafe** — provable at assembly.
- **windowShift** — the history window's near edge is moved back by `δ' − (predictAt − event_time)`; e.g.
  an outcome settled 30 minutes after the event and ingested within 6 days is only visible 6 days 30 minutes
  (+ the predictAt offset) later. This is what makes training features reproducible at serving time.
- **violation** — an emitted column would use information available after `predictAt`: assembly fails.
  Such a column may still exist as an intermediate consumed by a sequence feature (its past values are fine).

## Validating without running (validate --expand)

The same compiler is exposed without a pipeline run:

- REST: `POST /api/feature` with either `{parameters: {...}, inputSchema?: {fields: [...]}, args?: {...}}` or
  a whole pipeline config (the first `module: feature` step, or the one named by `name`). The response
  holds `ok`, `plan` (columns with availability / status / lineage, stages, diagnostics), `engineErrors`
  and a human-readable `describe` report.
- MCP tool `validate-feature` (arguments `config` or `parameters`, optional `name`, `inputSchema`, `args`,
  `streaming`, `format: text`).
- Pipeline Builder agent tool `validateFeature`.

## Limitations (current engine)

- Batch only for sequence / population features (per-key time-ordered replay). Row / context features also
  run in streaming within the configured window.
- `fit.mode: fold`, key set `structure: sequence`, nested encoding targets, the `quantile` stat (and
  `distribution` in static mode), and population types other than `encoding` / `factorization` are parsed
  but rejected. Factorization: `variant: bayesian`, `fit.cadence / window / warmStart`, and non-static fits. In `shrinkage`,
  `estimator: joint`, `weights: heldOut` and an `offset` on a logit / log scale are rejected;
  `parentStatistic: type` falls back to token with a warning. `weights: varianceComponents` estimates the
  per-level pseudo-count from the whole batch (a hyper-parameter, not time-expanding); a level whose
  between-key variance truncates to zero is fully shrunk to its parent (logged at run time).
- `atRowCreation` / `event_date THH:MM` availability needs per-row filtering that is not implemented yet.
- Rows whose key fields contain null bypass keyed evaluation (their keyed features are null).

## Example

```yaml
sources:
  - name: input
    module: bigquery
    timestampAttribute: session_time
    parameters:
      query: "SELECT * FROM `project.dataset.auction_rows`"
transforms:
  - name: features
    module: feature
    inputs: [input]
    parameters:
      sources: gs://my-bucket/feature/sources.yaml
      lineage:
        - {fields: [session_id, seller_id, category, start_price], from: listings}
        - {fields: [sold, final_price], from: auction_results}
      time: {field: session_time, orderTieBreak: [session_id]}
      predictAt: "event_time - PT10M"
      entities:
        - {name: seller, keys: [seller_id]}
      contexts:
        - {name: session, keys: [session_id]}
      features:
        - {name: relative, scope: context, context: session, inputs: [start_price], ops: [rank, zscore]}
        - name: recent
          scope: sequence
          entity: seller
          windows: [{maxEvents: 10}]
          ops:
            - {type: lag, fields: [sold], k: 3}
            - {type: aggregate, field: start_price, funcs: [mean]}
        - name: enc
          scope: population
          type: encoding
          keySets: [{keys: [seller_id]}, {keys: [category]}]
          targets: [{field: sold, stats: [mean]}]
      output:
        prefix: f_
sinks:
  - name: output
    module: bigquery
    inputs: [features]
    parameters:
      table: project.dataset.features
```
