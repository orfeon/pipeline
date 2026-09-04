# Feature Transform DSL (Design Document)

Status: **Accepted — v0 and the v0 additions implemented; v1 partially (static / fold fits, factorization, discretize, quantile stats). Implementation status and deferred items are tracked in [feature-engine.md](feature-engine.md) §9.**

Design of the declarative feature-engineering DSL behind the `feature` transform module: the
*sources contract*, the four feature scopes, the unified `encoding` with structured keys and
shrinkage, the availability-time algebra that makes leak checking a type check, and the
cross-cutting machinery (naming, lineage, `validate --expand`, fit artifacts). The Beam
implementation is described in [feature-engine.md](feature-engine.md); the user-facing reference is
[`server/docs/module/transform/feature.md`](../../src/main/resources/server/docs/module/transform/feature.md).

Section numbers are referenced from the code (`feature-dsl.md §x.y` in javadoc refers to this
document). Examples use a domain-neutral online-auction dataset: sellers list items in sessions
(the co-occurrence context), `current_bid_t10` is a market snapshot ten minutes before the
session closes, and `sold` / `final_price` are outcomes known after the session.

---

## 1. Design principles

### 1.1 Four scopes

Every feature definition is classified by *which rows it reads* (its scope). This is the first-class
axis of the DSL.

| scope | rows read | fit | typical examples |
|---|---|---|---|
| `row` | the row itself | none | differences, ratios, calendar decomposition, categorical crosses |
| `context` | the rows co-occurring in one event / group | none | rank within the group, z-score, composition statistics |
| `sequence` | the entity's past rows in time order | none (deterministic) | lag, EWMA, signatures, transition statistics |
| `population` | the whole training set (learned) | **required** | encodings, spectral embeddings, SVD |

The main benefit: **every transformation that needs a fit is localised, by type, in
`population`**. Leak management, fold design and train/serve consistency can then be enforced
mechanically per scope.

### 1.2 Reference graph (DAG)

Features reference other features by name (or by structured selector); the module evaluates the
reference graph in topological order. Explicit ordering (stage numbers) is not part of the language.
Compositions such as "compress a sequence's history vector with a population SVD" or "interact a
context statistic with a row attribute" are written purely declaratively.

### 1.3 Multiple entities and contexts

Sequence subjects (seller, category, seller×category pair, a parent key ...) and co-occurrence
groups (a session, a marketplace-day ...) can each be declared several times. "The seller's
history", "the pair's past outcomes" and "normalisation inside a session" are configuration
differences of one mechanism.

### 1.4 The unified frame for sequence features: Lift → Summarize → Compress

Every sequence method is a composition of three maps:

- **Lift**: event (one row) → vector. Numeric pass-through, one-hot, time augmentation, a
  pre-fitted state embedding.
- **Summarize**: a variable-length, irregularly spaced vector sequence → a fixed-length vector. A
  family of controlled linear / bilinear recurrences.
- **Compress**: post-processing. Identity, truncated SVD / PCA, random projection.

Lift's "pre-fitted state embedding" covers the spectral methods (§4.4 `spectralEmbedding`) and,
in v2, **HMM filtering posteriors**: a shared HMM fitted over the pooled entities (or per coarse
segment with MAP shrinkage to global) and applied with the forward algorithm to each entity's own
history, so per-entity individuality comes from inference, not from per-entity parameters. Its fit is
a distributed EM (E-step = per-entity forward-backward as a map, M-step = global aggregation of
expected counts), so it is scheduled with the probabilistic family; serving is one forward pass (a pure
map, representable in ONNX).

Theoretical grounding of the Summarize families:

| family | basis | subsumed methods |
|---|---|---|
| `lti` (linear time-invariant) | HiPPO (online projection onto orthogonal polynomial bases) | EWMA (exponential measure + order-0 basis), lag windows (uniform measure + sample basis), DCT / Fourier (FouT), Legendre projection (LegS) |
| `bilinear` | path signatures (controlled differential equations, universality) | log-signature, randomized signature ≈ echo state network |
| `probabilistic` | Kalman filtering / smoothing | posterior mean + variance as features |

Irregular spacing is handled by making the discretisation step of the continuous-time system the
elapsed time (`decayBy: time`).

The spectral population methods (PPMI + SVD categorical embeddings, lag-matrix SVD = SSA,
transition statistics) are all "truncated spectral decompositions of a linear operator counted from
the data" (Koopman operator view); their freedom reduces to *which operator, truncated at what rank*.

**Localised fit**: only part of Lift (state embeddings) and Compress (SVD) need a fit; Summarize is
always deterministic. Nested-learning instability is excluded structurally.

### 1.5 The data contract layer (sources)

Feature definitions (intent) and source definitions (facts) live in separate files. A source
definition states verifiable facts about the input data: field types, the event time, the
**availability time (`availableAt`)**, the **ingestion lag**, and the change characteristics
(`mutability` / `lateness` / `snapshotOf`). The feature spec references it.

Availability is two-layered: *when the value is known in the world* (`availableAt`) and *when it
appears in this system's input relation* (`availableAt + ingestionLag`). Leak checking (§6) must
protect the latter; checking only the former makes the check itself introduce train/serve skew by
admitting values that were usable in training but had not arrived at serving time (§6.2).

Benefits of the separation:

1. **Mechanised leak checking**: "when is this column known" becomes a declaration on the data side,
   so the inductive leak-safety argument (§6) turns from human reasoning into a type check.
2. **Single source of truth**: availability lives in the schema; naming conventions (`_` prefix) and
   per-column annotations are derived from it (§6.4).
3. **A data catalog for tooling**: field types and descriptions double as the catalog an external
   generator or agent reads (§8).

---

## 2. The sources document

### 2.1 Structure

```yaml
# sources.yaml
version: 1
sources:
  - name: listings
    description: "Listing entries of a session. Known when the session is scheduled, but withdrawals, seller changes and grading updates change both the row set and the values"
    eventTime: session_time          # the event time the rows of this table are attached to
    availability: atEventTime        # table default availability (§2.3)
    mutability: corrections          # appendOnly | corrections (§2.6)
    snapshotOf:                      # training values / row set come from an archived point-in-time snapshot (§2.6)
      source: listings_snapshot      # e.g. daily partitions
      at: "event_date T08:00"        # snapshot time expression (§2.3 language + absolute time of day)
    lateness: PT0S                   # allowed lateness (maps to the streaming watermark)
    keys: [session_id, seller_id]
    fields:
      - {name: session_id,      type: string, description: "session id"}
      - {name: seller_id,       type: string, description: "seller id"}
      - {name: category,        type: string, description: "listing category"}
      - {name: start_price,     type: double, description: "starting price", kind: attribute}
      - {name: condition_grade, type: string, description: "condition grade"}

  - name: price_snapshots
    description: "time series of bid snapshots"
    eventTime: session_time
    ingestionLag: PT1M               # upper bound on the delay between the value being known and reaching the input relation (§2.6)
    keys: [session_id, seller_id, snapshot_time]
    fields:
      - {name: current_bid_t10, type: double,
         availableAt: "event_time - PT10M",
         observedAtField: snapshot_time,   # a pre-event relative claim needs an observation-time column (§2.3)
         validFor: PT15M,
         kind: market,                     # origin tag, propagated into the lineage (§7)
         description: "highest bid ten minutes before close; stale after fifteen minutes"}
      - {name: final_bid, type: double,
         availableAt: after(event),
         kind: market,
         description: "closing bid; offline analysis only"}

  - name: auction_results
    description: "session outcomes; pre-event attributes and post-event results share a row"
    eventTime: session_time
    availability: atEventTime        # default: pre-event
    settlementLag: PT30M             # concretises after(event): delay from the event to the value being final (a fact about the world)
    ingestionLag: P6D                # weekly batch reflection, bounded from above (a fact about the system)
    mutability: corrections          # disputes and refunds correct rows
    lateness: PT2H
    keys: [session_id, seller_id]
    fields:
      - {name: quantity,    type: int,    ingestionLag: PT0S}   # pre-event attribute, arrives immediately (overrides the table default)
      - {name: sold,        type: int,    availableAt: after(event), kind: outcome}
      - {name: final_price, type: double, availableAt: after(event), kind: outcome}
```

`auction_results` is a table with both a settlement lag (until the world knows) and an ingestion
lag (until the system knows): the outcome is final thirty minutes after the session, but appears in
this system's input up to six days later. Sequence features (a seller's recent outcomes) may read it
only from the latter time on (§6.1).

### 2.2 Field specification

| item | required | meaning |
|---|---|---|
| `name` / `type` | yes | physical name and type (mapped onto the engine's type system, Avro-compatible) |
| `description` | recommended | for humans and for tooling that builds a search space from the catalog (§8) |
| `availableAt` | — | when the value is known in the world (§2.3); defaults to the table's `availability` |
| `ingestionLag` | — | upper bound on the delay before the value appears in the input relation (§2.6); defaults to the table's (then `PT0S`) |
| `observedAtField` | conditional | the column holding the actual observation time; required when `availableAt` is pre-event relative (`event_time - δ`) or `atRowCreation` (§2.3); the basis of the data audit (§7) |
| `evidence` | — | `measured` (default; `observedAtField` present) \| `declared` (no observation column). A pre-event relative claim without `observedAtField` must say `declared` explicitly |
| `allowDeclared` / `justification` | — | downgrade the default error for `kind: market` × `evidence: declared` to a warning, per field; `justification` is mandatory (§2.3) |
| `validFor` | — | how long the value stays meaningful (§2.4); unlimited when omitted |
| `kind` | recommended | origin tag (`market` \| `outcome` \| `attribute` \| free vocabulary). Propagated to every output column as `derivedFrom`; the axis of bulk exclusion and audits (§7) |

### 2.3 The availability expression language

Availability is a small closed set of expressions, evaluated per row to a concrete timestamp.

| expression | meaning |
|---|---|
| `atEventTime` | pre-event information: known once the event exists, usable at any `predictAt ≤ event_time`. Algebraically a lower bound "sufficiently before event_time" — **not** `event_time + PT0S` (that is `after(event)` with `settlementLag: PT0S`) |
| `event_time ± <ISO-8601 duration>` | relative to the event time (e.g. `event_time - PT10M`) |
| `after(event)` | final after the event: `event_time + settlementLag` (table-level, default `PT0S`) |
| `atRowCreation` | equal to the row's creation timestamp (a designated column) |

There is no boolean "known at prediction time": known-ness is a **derived, relative notion**
(a comparison with `predictAt`, §3) that availability subsumes (§6.1).

**Auditability of pre-event claims**: `event_time - δ` and `atRowCreation` claim that the value
was known before the event. When the claim disagrees with the data, validation passes and the
column leaks (a "morning snapshot" column whose content is the closing value; a "minus ten minutes"
column that is really "minus six" — both happened with columns that had no observation-time
column). Therefore a pre-event relative claim requires `observedAtField`, and a table without such a
column must declare `evidence: declared` explicitly (omission is an error). `measured` fields are
subject to the audit queries of §7; `declared` fields are recorded as "not auditable" in the
lineage and in the `validate --expand` output, and every output column derived from them carries a
warning. **`kind: market` with `declared` is an error by default** (mis-declared market columns
cause the worst leaks). Relaxation is per field, never global:

```yaml
- {name: bid_prev_day, kind: market, availableAt: "event_date T06:00",
   evidence: declared, allowDeclared: true,
   justification: "feed specification §x, confirmed against the 2026-08 reception logs"}
```

- `allowDeclared: true` without `justification` is a validation error.
- The lineage keeps `declared` (no promotion to `measured`); only the error becomes a warning.
- `validate --expand` lists every column `allowDeclared` applied to. `atEventTime` / `after(event)`
  err on the post-event side (they over-reject, never under-reject) and are exempt.

### 2.4 Validity (`validFor`) — the dual of `availableAt`

`availableAt` is the lower bound "usable from"; `validFor` is the upper bound "meaningful until"
(the feature-store ttl / max-staleness idea). It adds one inequality to the basic rule of §6.2:

```
availableAt ≤ predictAt ≤ availableAt + validFor
```

- **Opt-in**: declare it on freshness-sensitive market / state fields (bid snapshots); permanent
  attributes omit it (= ∞).
- **Expired semantics**: a value expired at `predictAt` is treated as null; `output.nullPolicy:
  indicator` adds a staleness flag column. This prevents "an old value wearing a fresh face" (a
  35-minute-old bid used as the last-minute bid after a feed outage) as a type, and the flag feeds a
  downstream abstain decision.
- **Declarable on feature outputs too**: a block (typically a sequence aggregate) may carry
  `validFor`, e.g. `validFor: P180D` on a history aggregate means "expired 180 days after the last
  contributing row" — an explicit null + flag instead of applying a stale aggregate to a long-dormant
  entity. This is a different axis from the window's `maxAge` (input selection vs. output expiry).
- Deriving an output's `validFor` from its inputs (min of `availableAt + validFor`) is a future
  extension; v0 uses explicit declarations only.

### 2.5 Defaults and inference

- Two layers: the table-level `availability` is the default, per-field `availableAt` overrides it.
  Tables mixing pre- and post-event fields (results tables) must override the post-event fields.
- **Inference is limited to proposing defaults**: a table whose rows are generated at one time equal
  to `eventTime` can be inferred as `availability: atEventTime`. Inferring from the row creation time
  of a post-event table would over-reject the pre-event fields, so inference results are hints
  (validate / import tooling) and the declaration is confirmed by the user.

### 2.6 Arrival and change characteristics

#### 2.6.1 Ingestion lag — the second layer of availability

`availableAt` says when the world knows a value; the input relation of this system sees it later
(final results reflected by a weekly batch, market data arriving minutes late). The difference is
declared as `ingestionLag` (table level, per-field override):

```
effectiveAvailableAt = availableAt + ingestionLag
```

- Definition: the **upper bound** of the delay between the value being known and appearing in the
  input relation. For schedule-dependent batch reflection, declare the worst case (conservative
  side). Omitted = `PT0S` (immediate).
- **All leak checks (§6) run on `effectiveAvailableAt`.** Checking on `availableAt` alone would
  admit, in training, values that had not reached the system at serving time (a same-day outcome
  used for a same-day later prediction) — the check itself would introduce train/serve skew. For
  entities appearing several times a day the `minInterval` static rule does not help either, so
  without this layer the availability filter of §6.2 is not semantically correct.
- The common hand-written practice "shift the near edge of the history window back by the
  settlement delay" (`RANGE BETWEEN ... AND <lag> PRECEDING` in SQL) is recovered as the
  deterministic compilation of the static rule of §6.2 once `ingestionLag` is declared.
- Reference point: `ingestionLag` is relative to **`availableAt`, not to event time**. A pre-event
  column (`event_time - PT10M` bids) with a few minutes of arrival delay and a post-event column with
  days of reflection delay use the same expression.

#### 2.6.2 Mutability and point-in-time snapshots (`snapshotOf`)

- `mutability: appendOnly | corrections` — whether rows can be corrected later. Listing entries
  (withdrawals, seller changes, grading updates change **both the row set and the values**) and
  results (disputes, refunds) are `corrections`. For `corrections`, "what was visible at prediction
  time" and "the final values / row set" differ, and the spec must say which one training uses.
- Three ways to obtain the training truth; the sources declaration selects one (§7 "path selection"):
  1. **backfill** (recompute from final values): `appendOnly` sources — first report = final value.
  2. **snapshot-backfill** (recompute from a point-in-time snapshot): `corrections` with
     `snapshotOf`. An archived snapshot (daily partitions) preserves "the values and row set visible at
     that time", so consistent training values exist for the whole past without waiting for logging.
  3. **log-and-wait** (feature logging, §7): `corrections` without a snapshot. Constructively correct,
     not retroactive.
- `snapshotOf: {source: <archive table>, at: <time expression>}` — the pre-event fields and the row
  set of a `corrections` source take their training values from the `at` snapshot stored in `source`.
  `at` uses the §2.3 language plus an event-date-anchored absolute time (`event_date T08:00`). Two
  semantic effects: (a) the field's `effectiveAvailableAt` is evaluated at `at` (corrections after the
  snapshot are invisible), (b) a `context` row set is the set as of the snapshot (§6.1).
  - **Responsibility** (consistent with §2.7): `snapshotOf` is a contractual declaration; in v0 the
    construction of the input relation from the snapshot (join / substitution) stays upstream. The
    module checks in the lineage that a field with `snapshotOf` is declared as coming from the snapshot
    source and applies the availability / row-set semantics above. When the as-of enrichment layer
    (§2.7) arrives, snapshot resolution moves into the module.
- `lateness` — allowed lateness, the declarative counterpart of watermark / trigger design.

These are not the leak semantics themselves; they contract the preconditions under which the
semantics hold at run time, so they belong to the sources document.

### 2.7 Several sources and the join responsibility

The module's input is **one relation** (`inputs: [records]`); joins are the responsibility of
upstream transforms. The feature spec declares the **lineage** of every input field (§3) and the
availability metadata propagates across the join (a joined field keeps the availability of its origin
field).

An enrichment layer with spine + point-in-time lookup (as-of join) inside the DSL is a future
extension: it would be equivalent to feature-store as-of semantics and is clearly useful, but widens
the scope. For the same reason `snapshotOf` (§2.6.2) is a declaration only in v0; the input relation
is built upstream.

---

## 3. Top-level structure (the feature spec)

```yaml
module: feature
name: featureGen
inputs: [records]
parameters:
  sources: sources.yaml            # the sources document (URI or inline)
  lineage:                         # where each input field comes from (§2.7)
    - {fields: [session_id, seller_id, category, start_price, condition_grade], from: listings}
    - {fields: [current_bid_t10], from: price_snapshots}
    - {fields: [sold, final_price], from: auction_results}
  time:
    field: session_time            # the time axis of all sequence processing
    orderTieBreak: [session_id]    # total order for rows sharing a timestamp
  predictAt: "event_time - PT10M"  # when the features are used; the reference of the leak check (§6)
  entities:                        # sequence subjects (several allowed)
    - name: seller
      keys: [seller_id]
    - name: category
      keys: [category]
    - name: pair
      keys: [seller_id, category]
  contexts:                        # co-occurrence groups (several allowed)
    - name: session                # rows competing in one session
      keys: [session_id]
    - name: marketplace_day        # a wide calibration group
      keys: [marketplace_id, event_date]
  baselines:                       # first-class baselines (market probabilities); referenced by residual / offset
    - name: market
      context: session
      expr: "share(1 / current_bid_t10)"   # includes a context normalisation; kind: market propagates into derivedFrom
  features: [...]                  # the feature blocks (§4); each block may carry computeAt (default predictAt)
  fit:                             # defaults for population blocks (overridable per block)
    orderBy: session_time
    mode: expanding                # expanding | fold | static
    minHistory: P90D
    groupBy: seller                # fold unit (an entity name); effective with mode: fold
  output:
    prefix: f_
    nullPolicy: keep               # keep | fillZero | indicator
    exclude: [intermediate.*, "derivedFrom:market"]   # name selectors / lineage selectors (§7)
    groupBy: session               # omit for input-grained rows; a context name re-aggregates per context (§3.1)
```

Notes:

- **baselines**: a baseline (market probability etc.) is declared once at the top level instead of
  being scattered across row `expr`s and factorization `task.offset`s. `baselines[].expr` uses the row /
  context expression language; with `context` it is evaluated as a context-normalised expression
  (`share` and the other context ops). Population targets, factorization `task.offset` and row residuals
  reference it by name (`offset: market` / `baseline: market`). Intent: the distinction between
  "features that imitate the market" and "features orthogonal to the market" is the central axis of
  evaluation, so (a) residual learning is defined in one place and (b) the `kind: market` lineage
  propagates mechanically to every residual column (§7). Rules:
  1. **No offset by default** (the target is used as is). `baselines` is passive; only blocks that
     reference `offset` / `baseline` change meaning.
  2. Validation hints when an encoding of an outcome target exists, a baseline is declared and no
     `offset` is given (never applied automatically).
  3. A block referencing an offset has `availableAt = max(target, baseline)`. Because a past row's
     baseline must be time-consistent or the offset itself leaks, the market fields a baseline reads
     must be `evidence: measured` (or per-field `allowDeclared`, §2.3).
  4. A block referencing an offset must have `computeAt = predictAt` (a market baseline is only final
     right before the event). The default is `predictAt`; an explicit different `computeAt` is an error.
  5. `offset` is an additive term on the `shrinkage.scale`: `logit(p) = logit(baseline) + δ` on logit,
     `target − baseline` on identity.
- **computeAt**: a block may declare `computeAt` (default `predictAt`). "When the prediction runs" and
  "when this feature is computed" differ in general — columns computable in the morning coexist with
  columns computed at the last minute after market data arrives. The check is
  `effectiveAvailableAt ≤ computeAt ≤ predictAt` (§6.2). `validate --expand` groups columns by
  `computeAt`, which maps onto the execution plan (precomputed vs. last-minute columns) and the
  workflow trigger order. v1, together with the serving implementation.

#### 3.1 Output shape (`output.groupBy`)

By default the output has the input's grain. When a consumer (a per-session inference API) expects
one record per context (parent + child array), `output.groupBy: <context name>` re-aggregates per
context. Column placement:

- **Parent**: the context keys and the columns structurally constant within the context — `scope:
  context` columns with `excludeSelf: false` and a group-composition op (`countByValue` /
  `ratioByValue` / `entropy` / group size), and context-constant `baselines`. Context-constant input
  attributes cannot be detected automatically and are listed in `output.parentFields` (fields whose
  lineage source is keyed by the same context keys are hinted as candidates).
- **Child** (array element): everything else (row / sequence / population columns, per-row context
  ops).
- The lineage records the placement (parent / child); names are identical on both sides.
- **Position**: `output.groupBy` is a **contract-shape declaration** for the consumer, not a
  performance mechanism. When the DAG ends on the same context key the final re-shuffle disappears,
  but that is an implementation benefit; the shuffle between sequence (entity key) and context (event
  key) stages remains. Performance visibility lives in the `validate --expand` estimates (§4.3).

- `time.field` is the **processing time axis** (sequence ordering, `fit.orderBy`) and carries no
  availability semantics. Validation checks it equals the `eventTime` of every source in the lineage
  (a mismatch means several event times are mixed and needs an explicit mapping).
- `predictAt` declares when this feature set is computed and used; validation checks derived
  availability ≤ `predictAt` for every emitted column (§6.2). Several profiles (T-10 prediction,
  offline analysis) are separate specs differing only in `predictAt`.
- **Total order requirement**: a sequence's "past" is `t' < t` on `time.field` with a strict
  inequality. To remove the ambiguity of rows sharing a timestamp, a total order including tie-break
  keys is declared with `time.orderTieBreak: [session_id]`. This is the precondition for identical
  results across execution engines (window frames vs. state update order).
- **Fold unit**: with `mode: fold` a per-row split lets nearby rows of one entity fall on the context
  side and the evaluated side of different folds — legal under the availability algebra (§6) yet a
  real leak. Especially when **past-target-derived values are keys or inputs** (an encoding keyed by a
  lagged outcome), validation requires an entity-level split via `fit.groupBy` (§6.2). `mode:
  expanding` avoids this class structurally through its one-sided time order, which is one reason it
  is the default.

---

## 4. Definition formats per scope

### 4.1 row — within-row transforms

```yaml
- name: price_change_ratio
  scope: row
  expr: "(start_price - prev_start_price) / nullif(start_price, 0)"

- name: time_parts
  scope: row
  type: datetime
  input: session_time
  derive: [month, dayOfWeek, weekOfYear]
  cyclical: true                 # sin / cos

- name: interval_bucket
  scope: row
  type: bin
  input: days_since_prev
  edges: [7, 14, 28, 56, 180]

- name: cross_cat
  scope: row
  type: cross                    # categorical cross (input to a high-cardinality encoding)
  inputs: [category, condition_grade]
```

The `expr` syntax is shared with the framework's expression engine (implementation and `validate
--expand` alike). A baseline residual references the top-level `baselines` (§3) instead of writing the
market column into the expression:

```yaml
- name: score_vs_market
  scope: row
  type: residual
  input: model_score
  baseline: market               # baselines[].name; market propagates into derivedFrom
  on: logit                      # identity | logit | log
```

### 4.2 context — transforms within a co-occurrence group

```yaml
- name: relative                 # sugar: inputs × ops
  scope: context
  context: session
  inputs: [start_price, current_bid_t10, quantity]
  ops: [rank, zscore, gapToBest, shareOfTotal, percentile]

- name: field_composition        # normal form: fields on the op
  scope: context
  context: session
  ops:
    - {type: countByValue, fields: [condition_grade]}
    - {type: ratioByValue, fields: [condition_grade]}
    - {type: entropy,      fields: [condition_grade]}
    - {type: zscore,       fields: [start_price]}

- name: day_normalized_price     # calibration over a wide group
  scope: context
  context: marketplace_day
  inputs: [start_price]
  ops: [zscore, median_diff]

- name: prob                     # group softmax on top of a baseline (serving: onnx score → probability)
  scope: context
  context: session
  ops:
    - {type: softmax, field: model_score, offset: market, temperature: 1.3, as: pWin}
      # p_i = w_i · exp(f_i / T) / Σ_j w_j · exp(f_j / T); w = offset in probability space (offsetScale: log
      # takes exp first); null offset → row null, out of the denominator; offset 0 → p = 0; null score → 0
      # (scoreNull: zero) or row null; temperatureFrom: <uri> reads T from a calibration document
      # (outside the plan hash, recorded in the manifest / output hash); validFor inherited from the offset

- name: placebo                  # shuffle: the field's values permuted within the group (seed + group key)
  scope: context
  context: session
  ops:
    - {type: shuffle, fields: [start_price], seed: 20260717}
      # the multiset per group is preserved; type and availability are the field's own
```

Placebo columns (`type: noise` at row scope — a draw from `seed` and the row identity `time.field` +
`orderTieBreak`, the fold rule; `shuffle` above) carry no information by construction and calibrate a
selection threshold or measure permutation importance through the same path as the candidates.
`baselines[].emit` writes a baseline value as an output column (the number the softmax offset reads),
which the `baseline` role of the output contract (§7) can name.

**Normal form and sugar**: the normal form carries `fields` on each op (`ops: [{type, fields:
[...]}]`); block-level `inputs` × `ops` is sugar for "give every op `fields: inputs`". Applying the
same ops to many numeric columns is one block; names are `{feature}_{field}_{op}` and the lineage
keeps the (field, op) coordinates. Type mismatches (numeric `zscore` on a categorical field) are
rejected by the operator catalog's signatures (§8). The canonical hash (§5.4, §8) is taken after
desugaring — sugar and normal form hash identically. `inputs` is an expandable field (§5.3 rule 3)
and counts towards `maxFeatures`.

`excludeSelf: true` evaluates the group without the row itself (difference to the group mean etc.).

### 4.3 sequence — entity history transforms

Frequent simple cases use sugar ops; the general form is the three-stage Lift / Summarize / Compress.

**Window semantics**: a sequence window reads **strictly past rows only** (`t' < t`, never the row
itself), ordered by the total order of §3 (`time.field` + `orderTieBreak`). This convention is the
premise of the availability propagation rule (§6.1).

```yaml
# --- simple form (sugar) ---
- name: recent
  scope: sequence
  entity: seller
  windows:                                                # several windows → product expansion (below)
    - {maxEvents: 5}
    - {maxAge: P365D}
  ops:
    - {type: lag, fields: [sold, start_price], k: 3}
    - {type: delta, field: start_price, k: 1}
    - {type: trend, field: start_price, k: 5}             # regression slope
    - {type: ewma, field: start_price, halflife: [2, 5, 10],
       decayBy: events}                                   # events | time
    - {type: ewma, expr: "sold >= 1", halflife: [5]}      # an expression (desugared to an anonymous row feature, below)
    - {type: runLength, field: condition_grade, value: good}
    - {type: sinceEvent, predicate: "sold = 1",
       unit: [events, days]}
    - {type: countMatch, predicate: "start_price > prev_start_price"}

# --- general form (three stages) ---
- name: hist_signature
  scope: sequence
  entity: seller
  window: {maxEvents: 20, maxAge: P2Y}
  lift:
    fields: [sold, start_price_z, bid_ratio_z]
    timeAugment: true
  summarize:
    dynamics: {family: bilinear, type: logsignature, depth: 3}

- name: hist_hippo
  scope: sequence
  entity: seller
  lift: {fields: [start_price]}
  summarize:
    dynamics: {family: lti, measure: legendre, order: 4, decayBy: time}

- name: pair_history             # an entity pair uses the same mechanism
  scope: sequence
  entity: pair
  ops:
    - {type: aggregate, field: sold, funcs: [count, mean, min]}
```

**Several windows (`windows`)**: `windows` is a list, an expandable field expanded as the product
`windows × fields × funcs` (`× halflife` for ewma; positional under `combine: zip`; counts towards
`maxFeatures`). A singular `window:` is sugar for a one-element list. Each element carries
`maxEvents` (count), `maxAge` (wall time) and `filter` (below); `maxEvents` and `maxAge` combine.
`decayBy: events | time` is the key to irregular spacing.

- **The near edge is not an expansion axis**: the near edge of a window (the recent side) is derived
  from the sources' `ingestionLag` (§2.6.1) by rule 1 of §6.2; a window element carries only the far
  edge (`maxAge`), the count (`maxEvents`) and the condition (`filter`). A per-element near edge would
  allow declarations contradicting `ingestionLag`, so there is none.
- **Naming tokens**: `maxAge: P365D → 365d`, `maxEvents: 20 → n20`, combined `365d_n20`. The naming
  template has a `{window}` axis and the lineage keeps the window coordinates.
- **Execution plan**: several windows on one entity and order share one sort (several frames over
  one `PARTITION BY ... ORDER BY` in SQL; several range reads of one per-entity timestamped buffer
  in the engine). Nested windows (same filter, different far edge) are computed from differences of
  running sufficient statistics. `validate --expand` reports **the number of partition switches
  (key changes = shuffles) and shared sorts** as the evaluation-order estimate.

**Expressions in ops / targets and desugaring**: every sequence op (aggregate / ewma / lag / trend /
delta ...) accepts `expr` instead of `field` / `fields` (encoding targets too, §5.2). Rule: `expr`
desugars into a reference to an anonymous row feature `{block}.__e{n}` under the same block;
availability propagation, lineage and the canonical hash all use the desugared form. "Define a row
feature and reference it" and "inline expr" hash identically, and `validate --expand` shows the
desugared form.

- **Semantic restriction**: an expression passed to a sequence op is evaluated on **past rows only**
  and cannot reference the current row (`$self`) — an aggregate depending on both the window rows and
  the current row is not a window function and would need a self-join. When a relation to the
  current row is needed, decompose:
  - equality (only past sessions of the same category) → `window.filter` + `$self` (reducible to a
    partition key)
  - difference (to the previous listing) → `lag` the past value into a column and subtract in a row `expr`

**Self-referencing filters (`window.filter`)**: aggregates such as "the same seller's recent sessions
in the same category as this one" narrow the past rows by equality with an attribute of the current
row. Multiplying entity definitions per attribute combination explodes (dozens of partition-key
combinations in practice), so `window.filter` accepts a predicate referencing the current row with
`$self.<field>`:

```yaml
- name: same_category_history
  scope: sequence
  entity: seller
  window: {maxEvents: 5, maxAge: P2Y, filter: "category = $self.category"}
  ops:
    - {type: aggregate, field: sold, funcs: [count, min]}
```

- Availability propagation (§6.1) includes **the availability of the `$self` fields the filter
  reads** (a post-event field in `$self` fails the check). The past-row side follows the ordinary
  sequence rule.
- An equality-only filter may be reduced to `PARTITION BY entity, <filter fields>` when compiling to
  SQL. The engine keeps the state per entity and filters on read (no extra shuffle) — or, when the
  filter is a same-field pre-event equality, reduces it to an additional partition key at compile
  time (see feature-engine.md).

**Division of labour between sequence and encoding**: sequence = **deterministic sequence
operations** (lag, delta, slope, decay, count, min / max, signatures); encoding (§5) = **shrunk
conditional statistics** (per-key target means and rates). Most history aggregates are "windowed
target means": `sequence.aggregate mean` has no shrinkage and overfits sparse entities, while
encodings had no window — the latter is solved by the keySet `windows` of §5.3. Validation turns
`sequence.aggregate` `mean | rate` over an outcome field (`availableAt > predictAt`) into a hint to
rewrite as an encoding (an error under `strict`), and keeps `count | min | max | lag` and the other
deterministic ops in sequence (§7).

### 4.4 population — learned transforms

```yaml
- name: enc                      # the unified encoding of §5
  scope: population
  type: encoding
  ...

- name: state_embed
  scope: population
  type: spectralEmbedding        # co-occurrence operator + PPMI + truncated SVD
  sequenceOf: {entity: seller, field: condition_grade}
  cooccur: {window: 2, weighting: ppmi}
  rank: 8

- name: transition_prob          # first-order transition statistics (another output of the same estimator)
  scope: population
  type: transitionStats
  sequenceOf: {entity: seller, field: condition_grade}
  emit: [toValueProb: good]
  blend: {perEntity: true, priorWeight: 20}   # Bayesian blend of entity × global
```

`blend: {perEntity: true, priorWeight: N}` is a special case of §5.5 shrinkage (Dirichlet-Multinomial
family × flat lattice × `weights: fixed`). Once the shrinkage block exists, `blend` becomes sugar for a
shrinkage reference so the shrinkage implementation and vocabulary live in one place (and
`weights: varianceComponents` becomes available to it).

```yaml
- name: lag_svd
  scope: population
  type: svd                      # compression of a sequence output (lag window) = SSA
  input: recent.lag_window
  rank: 5

- name: price_quantile
  scope: population
  type: quantileTransform
  input: start_price
  bins: 20

- name: price_bins               # fitted discretisation (an encoding key)
  scope: population
  type: discretize
  input: start_price
  method: quantile               # quantile | tree | optimal (monotone-constrained)
  bins: 8                        # or minSamplesPerBin for an automatic count
  target: sold                   # required for method tree | optimal (supervised)
```

**Bin numbering (implemented for `method: quantile`)**: the INT64 output is `-1` = missing, `0` =
below the fitted minimum, `1..B` = fitted bins (`edge <= v < next`; B = the number of bins after
dropping duplicate and extreme edges, ≤ `bins`), `B+1` = above the fitted maximum. Edges are the
type-7 (linearly interpolated) `i/B` quantiles. `B = min(bins (default 10), n / minSamplesPerBin)`.
An input without a single value still fits (n = 0): every non-missing value maps to bin 1 and the
artifact is written.

**Distinction from the row `type: bin`** (§4.1, hand-written edges, no fit): a discretisation whose
edges are learned from data involves a fit and lives in `population`. `method: tree | optimal`
consume a target, so together with an encoding keyed on the bins **the target is consumed twice**
(edge selection and encoded value): the fit convention (expanding / fold) applies to the composition
of both stages, and a configuration that fits the edges on all data while keeping only the encoding
in-fold is a validation error. Out-of-range and missing values always get dedicated bins. Edges are a
fit artifact under content addressing, and the per-bin inflow ratios are a drift audit target (§7).
Bin count and shrinkage are redundant regularisers — put the freedom in one of them (recommended:
fix `minSamplesPerBin`, leave shrinkage to `varianceComponents`). Bins used in a cross key should be
coarser than single-key bins (cardinality multiplies; 4–8 as a guide).

```yaml
- name: affinity_fm
  scope: population
  type: factorization            # low-rank interpolation of categorical crosses (FM family)
  variant: fwfm                  # fm | fwfm | bayesian (v2)
  fit: {cadence: daily, window: trailing(P3Y), warmStart: previous}
  fields: [seller_id, category, condition_grade]
  latentDim: 16
  task: {target: sold, offset: market}       # baselines[].name (§3); a raw column name is not accepted
  outputs:
    - {pair: [seller_id, category],        as: fm_seller_category}
    - {pair: [category, condition_grade],  as: fm_category_grade}
    - {embedding: seller_id, as: emb_seller, dims: 8}   # leading k dimensions of the latent vector (all when omitted)
    - {sum: true, as: fm_linear}                        # sum of all pair products = the linear predictor
```

The three `outputs` kinds are lookups of fitted parameters with no extra cost. `embedding` emits
`dims` columns, counts towards `maxFeatures`, and keeps the fit boundary in the lineage so ablation
removes the embedding columns as a group.

**Position of factorization**: an encoding (lattice shrinkage) counts observations in declared
contexts and estimates faithfully with shrinkage; factorization interpolates cross effects at low
rank through inner products of per-value latent vectors, **giving structural interaction scores to
sparse (empty) cells**. Roles: lattice + ANOVA = intrinsic interactions of well-observed cells,
factorization = transfer to sparse cells, tree embeddings = discovery of interaction regions.

- `variant: fwfm` multiplies each pair product by a per-field-pair scalar weight r. Few extra
  parameters, hard to overfit, and **the learned r matrix ranks field pairs by importance** — the
  material for choosing `outputs` pairs and candidate coarse crosses for a lattice (§8); it is emitted
  as lineage metadata.
- The default fit is ALS: each pass is an aggregation of sufficient statistics (combiner-friendly) and
  converges in a few passes per fit boundary (`warmStart: previous`). Implemented in-house (a few
  hundred lines) since no dependable JVM FM implementation exists. Inference is "embedding lookup +
  inner product (+ r)", a pure map that is easy to express in ONNX.
- `variant: bayesian` (Gibbs) estimates the regularisation together with a prior and yields posterior
  variances (the `emitConfidence` shape) but needs iterative sampling — v2, in the model-per-key
  iterative framework.
- Third-order and higher (HOFM / ANOVA kernels) wait for evidence from the r matrix / tree embeddings
  and are reserved as future `variant` values.

Every population definition carries fit metadata (top-level defaults + per-block overrides).
`mode: expanding` computes statistics from data up to each row's time (the generalisation of ordered
target statistics); `mode: fold` fits on out-of-fold data; `mode: static` fits once on the whole input.

---

## 5. The unified encoding (`type: encoding`)

### 5.1 Why one encoding

Target encoding and frequency encoding are one family: key-conditional aggregate statistics
`E[stat(t) | keys]`.

- target encoding = per-key target mean (+ smoothing)
- frequency encoding = the special case without a target (count / share of a constant 1)
- per-key std, quantiles and count encodings are configuration differences of the same frame

The fit convention is unified as well. Target statistics need `expanding` against target leaks.
Frequency statistics have no target leak, yet "counting future rows" is itself a train/serve
inconsistency, so `expanding` is the safe default for both. The practical difference is only whether
smoothing is needed, absorbed by per-target settings.

### 5.1.1 One level up: a shrinkage-smoothed estimator

Precisely, the abstraction is "**a smoothed estimator of conditional statistics over a structured
key space, with shrinkage**". Flat keys + Bayesian smoothing is its degenerate case (a two-level
lattice, fixed pseudo-counts, Gaussian family). The generalisation axes are orthogonal:

| axis | choices | meaning |
|---|---|---|
| key structure (§5.3) | flat / hierarchy / sequence / cross | the shape of the generalisation lattice towards coarser contexts |
| shrinkage (§5.5) | weights × family × composition rule | how estimates shrink along the lattice |
| estimator (§5.6) | partition (bin) / linear-basis / self-regularising | the hypothesis space; v0 = partition only |
| statistic | mean / rate / count / distribution / std / quantile | shrinkage semantics per the table below |

**Shrinkage semantics per statistic.** Shrinkage applies through different mechanisms depending on
the statistic; treating std and quantiles "like the rest" would interfere with shrinkage, so:

| statistic | shrinkage mechanism | note |
|---|---|---|
| mean | Gaussian family: pseudo-statistics added to the sufficient statistics (n, Σy, Σy²) | James-Stein / BLUP type |
| rate / share (binary) | Beta-Binomial: pseudo-counts (m·p̂_parent, m) | the conjugate formalisation of the usual Bayesian smoothing |
| count / intensity | Gamma-Poisson: pseudo-statistics on (Σ events, Σ exposure) | negative-binomial marginal (over-dispersion) |
| distribution (multi-valued) | Dirichlet-Multinomial: per-category pseudo-counts | the distribution itself as a feature |
| std / quantile | **not conjugate**: merge sufficient-statistic sketches (t-digest / moments) coarse→fine along the lattice, then take the statistic | "interpolating estimates" is wrong for quantiles (the quantile of an interpolated distribution differs) |

Conjugate shrinkage is uniformly "add pseudo sufficient statistics inherited from the parent", so the
top-down pass manipulates sufficient statistics, not estimates. Leave-node-out (§5.5) becomes a
subtraction of sufficient statistics and confidence is the posterior pseudo-count total (effective
sample size) with the same meaning in every family.

The recursion (plugging the parent posterior into the child prior) is an empirical-Bayes
approximation, not exact hierarchical Bayes; the exact solution needs iterative inference and is out of
scope under the "closed form, aggregation only, one pipeline" constraint.

### 5.2 Any column as target

The target is any column or expression (`expr: "sold >= 1"`), not a fixed label. `targets[].expr` is
sugar for an anonymous row feature `{block}.__e{n}` (§4.3), so availability, lineage and the canonical
hash are those of the desugared row feature. The target's availability comes from the sources'
`availableAt` (§2.3): only for outcome columns unknown at prediction time (`availableAt:
after(event)`) does expanding leak management carry its full meaning, and validation warns on that
basis. (A per-column "known at prediction time" annotation is subsumed by the sources' `availableAt`.)

### 5.3 The product of keySets × targets

```yaml
- name: enc
  scope: population
  type: encoding
  keySets:
    - keys: [seller_id]                      # structure omitted = flat
    - keys: [category]
      windows: [{maxAge: P365D}, {maxAge: P1095D}]   # windowed conditional statistics (rule 5); an expansion axis
    - keys: [condition_grade]                # the main-effect encoding an additive lattice (below) refers to
    - keys: [brand_id]
      structure: hierarchy                   # recursive shrinkage along an ancestor chain (§5.5)
      parentRef: parent_brand                # the field giving the parent key (a reference table is fine)
      maxDepth: 3
      shrinkage: {parentStatistic: type}     # per-keySet override (rule 6): for lineages dominated by a few large parents
    - keys: [sold_lag1, sold_lag2, sold_lag3]
      structure: sequence                    # suffix back-off lattice (declared most-recent first)
    - keys: [seller_id, condition_grade]
      hierarchy:                             # explicit lattice (§5.3.1): shrinkage targets fine → coarse
        - [seller_segment, condition_grade]  # a coarse cross (one side replaced by an intermediate level)
        - additive                           # shrink towards the sum of the main-effect encodings (ANOVA type)
        - []                                 # the global mean
    - keys: [category, marketplace_id]
      structure: cross                       # product lattice: the parents are the marginals of each key (derived)
  targets:
    - {stats: [count, share]}                # frequency (no target)
    - {field: sold, stats: [mean]}
    - {expr: "final_price > start_price", stats: [mean]}
  combine: product                # product | zip
  emitConfidence: true            # a companion confidence column per output (opt-in, §5.5 rule 6)
  shrinkage:                      # §5.5; block default, overridable per keySet. The legacy smoothing block stays as sugar
    weights: varianceComponents
    scale: logit                  # mandatory when the lattice contains additive (§5.5 rule 7)
    leaveNodeOut: true
  naming: "{keys}__{window}__{target}__{stat}"   # {window} is empty for keySets without windows (§4.3 tokens)
  maxFeatures: 200                # guard against explosion
```

Rules:

1. **Visible expansion**: the product hides the real column count, so a `validate --expand` dry run
   listing every expanded column, type and fit dependency is mandatory; `maxFeatures` caps it.
2. **Naming and lineage**: names are generated deterministically from the `naming` template; the
   lineage keeps the keySet × target × stat structure, so ablation removes groups and importance is
   aggregated per axis.
3. **One expansion rule**: plural fields (`keySets`, `targets`, `windows`, `inputs`, `fields`,
   `halflife` ...) take lists and expand as a product inside the block. Expandable fields are declared
   explicitly in the schema (no implicit list promotion) and listed in the operator catalog (§8) as the
   input of the column-count estimate.
4. **Structured keys (`structure` / `hierarchy`)**: a keySet declares a generalisation lattice
   towards coarser contexts. Two declaration styles: `structure:` **derives** the lattice (`flat` =
   none, shrink to the global mean only / `hierarchy` = ancestor chain via `parentRef` (lineages,
   regions, taxonomies) / `sequence` = suffix chain of the declared order (lag columns as keys) /
   `cross` = product lattice with the keys' marginals as parents); `hierarchy:` **declares** it
   explicitly (§5.3.1; coarse crosses and `additive` allowed). Unknown keys fall back the same way in
   both styles: a leaf unseen in training falls to its nearest known ancestor (the longest known
   suffix) as the continuation of the shrinkage structure. The classic `hierarchy: [[category], []]`
   (a chain of key lists) is the degenerate case of §5.3.1.
5. **Time windows**: a keySet may carry `windows: [{maxAge, maxEvents}, ...]` (singular `window:` is
   sugar). Under `mode: expanding`, at each point in time only the contributing rows inside the window
   (within `maxAge` / the last `maxEvents`) are aggregated. Same vocabulary, semantics and tokens as
   sequence windows (§4.3: strictly past, near edge from `ingestionLag`, the `{window}` naming axis),
   and unified with factorization's `fit.window: trailing(...)`. The expansion is the product `keySet ×
   window × target × stat` (positional under `combine: zip`; counts towards `maxFeatures`). Shrinkage
   (§5.5) applies to the windowed statistics and the parent levels use **the same window** (per-level
   windows are not allowed: leave-node-out subtraction would no longer hold). No window (= all-time
   expanding) is the default. This lets "the category's sold rate over the last 365 days, shrunk" be
   written as one encoding (§4.3 division of labour).
6. **Per-keySet shrinkage override**: a block commonly mixes chain lattices (ancestor chain →
   back-off) and overlapping lattices (crosses with `additive` → sequential / joint), which one
   block-level `shrinkage` cannot express. A keySet may carry `shrinkage:` overriding the block
   default field by field (`estimator` / `weights` / `priorWeight` / `parentStatistic` / `scale` /
   `poolVariance` / `output`). The lattice-kind checks of §7 (f) run on the overridden values. Settings
   only one lineage needs (`parentStatistic: type`) are localised this way.

#### 5.3.1 Semantics of `hierarchy` (the lattice)

`hierarchy` declares the keySet's generalisation lattice — a partial order from the key to coarser
contexts. Each entry lists the shrinkage targets fine → coarse; the last entry implicitly reaches
`[]` (global). Three entry kinds:

1. **Key lists** (classic): a subset of the keys (marginalisation) or a replacement of a key by a
   higher level (`seller_id → seller_segment`, `sub_category → category`). The higher level just needs
   to exist as a row-scope derived column (`type: cross` / a parent key column); the lattice needs no
   special mechanism. **Writing the product of intermediate levels of both sides (a coarse cross) as
   the shrinkage target of a cross key** is the main purpose of the generalisation: an effect like "this
   seller segment does well in this category" lives in the coarse cross, not in the leaf cell, and at that
   granularity there are enough observations for it to survive shrinkage.
2. **`additive`** (reserved word): the shrinkage target is the **sum** (on the chosen scale, §5.5
   rule 7) of the estimates of the single-key keySets of the constituent keys declared in the same
   block. The keySet's estimate is then "the deviation from the additive prediction (the sum of main
   effects) = the pure interaction component" (ANOVA decomposition). Validation checks that the
   single-key keySets exist in the block.
3. **`[]`** (classic): the global mean.

**Computation model**: shrinkage along the lattice runs as ordinary per-key aggregation per level
(combiner-friendly) plus one coarse → fine top-down interpolation pass (back-off). The interpolation
weight of each step follows the §5.5 weights rule (default `varianceComponents`; `fixed` gives the
closed form `w = n/(n+λ)`). Parent self-contamination is avoided by `shrinkage.leaveNodeOut`
(default true) uniformly for every lattice kind — subtracting the child's sufficient statistics from the
parent's, which keeps the combiner structure and the single pass (for well-observed keySets the
difference to "full" is negligible). Availability propagation (§6.1) is unchanged: each level's
aggregate is the max of its contributing rows' availability and the interpolation is a per-row
composition.

**Stated limitation**: the lattice shrinks along **declared** generalisation paths only; it does not
discover interactions. Candidate discovery belongs to external tooling and to factorization (§4.4,
§8). The meaning of the `additive` decomposition depends on the shrinkage scale (§5.5 rule 7).

### 5.4 Nested encodings (order-dependent)

Aggregating an encoding's value again under another key (the mean of per-seller target encodings per
category, say).

**Order is derived from references.** No stage numbers: structured selector references + the DAG's
topological sort.

```yaml
- name: enc_nested
  scope: population
  type: encoding
  keySets:
    - {keys: [category]}
    - {keys: [seller_segment]}
  targets:
    - field:
        ref: {feature: enc_base, keys: [seller_id], target: sold, stat: mean}
      stats: [mean, max]
      asOf: event
```

A wildcard in the ref (`target: "*"`) means "the referenced expansion × the referencing expansion"
and counts towards `maxFeatures`.

**Time consistency: `asOf: event` by default.** Two semantics exist for the first-level values the
second level aggregates:

- `asOf: event` (frozen snapshot): each past row contributes "the value it had at its own time".
  **Default.** Train/serve consistency is trivial (this is what serving has), one pass, and if every
  level is expanding the composition is expanding — the inductive leak-safety guarantee holds.
- `asOf: latest` (recompute): re-evaluate past rows with the statistics at aggregation time. Batch
  analysis only; validation warns that serving reproduction costs extra.

**Chained fit artifacts: content-addressed hashes including dependencies.** The canonical AST hash
of every definition is computed including the hashes of its dependencies. An upstream change
invalidates downstream artifacts automatically; unrelated changes keep the cache valid. The same id
system serves candidate identity in external tooling (§8).

**Guards**: `maxDepth: 2` by default; deeper needs explicit opt-in. Part of the nesting is replaceable
by a composite-key encoding (combination cells) or a context aggregate; nesting is essential only when
"the attribute distribution of a related entity" is wanted (the level of listings a seller usually
handles) — validation includes this criterion in its hints.

### 5.5 Shrinkage — the generalisation of smoothing

The legacy `smoothing: {type: bayesian, priorWeight: N}` is one point (fixed pseudo-count × Gaussian /
Beta family × flat lattice) and stays as sugar for:

```yaml
shrinkage:
  estimator: backoff | sequential | joint   # default: derived from the lattice shape (rule 1)
  weights: fixed | varianceComponents | heldOut   # default varianceComponents
  priorWeight: 50                        # weights: fixed only
  family: gaussian | betaBinomial | gammaPoisson | dirichletMultinomial  # derived from the statistic; explicit override allowed
  leaveNodeOut: true                     # default true
  parentStatistic: token | type          # default token
  scale: identity | logit | log          # the scale of shrinkage and additive composition (rule 7); mandatory with additive
  output: [composed]                     # composed | deviations | effectiveN
```

**Composition rules (normative)**:

1. **Chain lattices use back-off; overlapping lattices need sequential or joint.** Ancestor chains,
   suffix chains and nested bins are pure inclusions, so leave-node-out recursive shrinkage (one
   top-down pass) = `backoff` suffices. In lattices where contexts overlap (cross interactions,
   `additive`, n-grams) the naive addition of independently estimated context effects double-counts
   shared signal (an explicit `estimator: backoff` on an overlapping lattice is a validation error);
   choose one of:
   - `sequential` (sequential residuals, v0): global → main effects shrunk in closed-form EB → coarse
     crosses shrunk on the residual of the additive prediction → finer crosses, sweeping the lattice
     coarse → fine. Every step stays a scalar closed form and combiner-friendly. The closer the data is to
     balanced, the closer to the exact solution; under a fully balanced design the interaction estimate is
     exactly "cell mean − row mean − column mean + grand mean". **Under imbalance (strong confounding
     between keys) the earlier step absorbs part of the interaction — a bias that depends on the step
     order.**
   - `joint` (simultaneous, v1): span the indicator functions of every context of the lattice as a
     basis, aggregate the sufficient statistics (XᵀX, Xᵀy) in one pass, solve once on the driver (ridge /
     BLUP). Separates confounding correctly without iteration. No longer "a scalar formula per cell" but
     still "one computation". The variance components (shrinkage strengths) are fixed first by
     per-layer closed-form moment estimators (Henderson type).
   - Guide: important coarse crosses (the hypothesis itself) → joint; large exhaustive keySet lists →
     sequential. A `sequential` specification on strongly confounded data gets a validation hint towards
     joint (§7 data audit).
2. **Leave-node-out**: subtract the child's sufficient statistics from the parent's (`parent_sum −
   child_sum`) so the child's own data in the parent aggregate does not weaken its shrinkage
   self-confirmingly. The hierarchical version of out-of-fold (never encode yourself with your own
   target); on by default.
3. **`weights: varianceComponents` (closed-form EB)**: the shrinkage weight `w = τ²/(τ² + σ²/n)` uses
   the between-sibling variance τ² of each level estimated by the method of moments (closed form).
   Execution = three aggregation passes — per-level key aggregates, per-level variance components,
   the top-down shrinkage pass — with no iteration. A negative moment estimate of τ² is truncated to 0
   (= complete shrinkage of that level; "no signal at this level" is decided automatically) and reported
   by `validate --expand` (§7). Levels with few siblings and unstable τ² may pool τ across levels
   (`poolVariance: true`).
4. **`parentStatistic`**: the back-off parent's value answers "the expected value of an unseen child
   under this parent". The default `token` (observation-weighted parent mean) can be replaced by
   `type`, the unweighted mean of the child means. In hierarchies with extremely skewed child sizes (a
   few huge nodes dominating the token mean) `type` is the right target.
5. **Deviation outputs**: the composed value `enc(node) = enc(parent) + δ_node` can emit each term δ
   (the shrunk increment from the parent) as its own column at no extra cost. Uses: (a) orthogonalised
   contributions for downstream linear / regularised models, (b) diagnostics of leaves significantly off
   their parent via standardised deviations, (c) drift monitoring (deviation distributions move earlier
   than composed values). Deviation columns take the stat axis `dev{level}` (0 = leaf, increasing
   towards coarser levels; `@` is not a legal column character, so the level number is attached
   directly) and the lineage keeps the level and the parent key of the lattice.
6. **`effectiveN` output and confidence companions (`emitConfidence`)**: the effective sample size
   n_eff (posterior pseudo-count total in conjugate families; `n + m·(decayed parent n_eff)` in the
   recursion) can be emitted. Shrinking only the point estimate while reporting raw counts underrates
   the confidence of sparse leaves strongly pulled to their parent, so confidence propagates through the
   same recursion. `emitConfidence: true` on a block is the sugar: every encoding column gets a
   companion `{column}__conf` (effective observations — for lattices the effective n of the level that
   finally contributed — or a posterior-variance approximation). The output type of the generalised
   encoding becomes "point estimate + confidence", aligned with downstream abstain decisions and the
   Bayesian factorization variant (§4.4). The lineage records the pairing; ablation removes pairs.
7. **Shrinkage scale**: `scale` declares the scale of shrinkage and of `additive` composition
   (identity / logit / log). Default identity; a binary target (0/1 mean) typically logit, counts log.
   **Interaction is a scale-relative notion** — an effect that looks like an interaction on identity may
   vanish on logit as a sum of main effects (and vice versa), so a lattice containing `additive` must
   declare `scale` (undeclared is an error). A binary target with the inherited identity gets a hint. On
   logit / log the implementation is Gaussian shrinkage of the transformed cell statistics with a
   delta-method variance (conjugate closed forms of §5.1.1 and scale additivity do not coexist, so
   additive lattices use the Gaussian approximation; cells with a handful of observations are coarse
   but shrink away anyway).
8. **Effective sample size (a semantic limit)**: rows of one event are competitively dependent and
   repeated entities form clusters, so the effective n is below the nominal n; `w = n/(n+λ)` relaxes
   shrinkage with the nominal n and errs towards under-shrinkage. `weights: heldOut` (λ optimised on a
   held-out period declared by the fit convention) is allowed, with the caveat that λ then loses its
   interpretation as a variance ratio.

**Relation to the fit convention**: the variance components and parent statistics all consume the
target, so the expanding / fold convention applies to every stage including the estimation of the
shrinkage parameters (the same composite discipline as fitted discretisation, §5.6).

### 5.6 Numeric keys and smooth estimators — the method × regularisation support matrix (rationale)

Conditional statistics keyed by a numeric field generalise from the partition estimator (through
discretisation, §4.4) to smooth estimators. The hypothesis space (method) and the principle fixing the
regularisation strength (regularisation) are orthogonal axes; the support matrix:

| method | class | regularisation: eb | execution |
|---|---|---|---|
| bin (+ every structured key) | partition | **exact closed form** (moments); conjugate families (§5.1.1) stay closed | aggregation only (combiner-friendly) |
| spline / rff | linear basis | via the mixed-model correspondence (P-spline ↔ LMM) as REML | aggregated (XᵀX, Xᵀy) + one local eigen decomposition; no re-scan |
| isotonic | self-regularising | n/a (the shape constraint is the regulariser; no continuous tuning quantity) | PAVA |
| gbdt | self-regularising | n/a (learning rate / depth do not correspond to a variance ratio); heldOut only | iterative fit |

The three conditions "closed-form EB × conjugate family × aggregation-only execution" hold
simultaneously only for the partition class, which is therefore the default. The linear-basis class is
a Gaussian-only superset (confidence = pointwise posterior variance / effective degrees of freedom with
the same meaning); the self-regularising class is a separate heldOut-regularised bracket.

**Extension to crosses (varying coefficients)**: a categorical × numeric cross is the tensor product
of the one-hot and the numeric basis, `f_c(x) = f_global(x) + δ_c(x)` with δ_c shrunk to 0 — the §5.5
shrinkage structure lifted to function values. It is an overlapping lattice, so `joint` is forced.
Reducing the numeric basis to indicators (bins) degenerates exactly to "categorical × bin cross +
ANOVA back-off", keeping consistency with the partition spec. The tensor basis dimension (|category|
× basis size) is part of the `validate --expand` estimate (§7). A minimum-sample cut-off for categories
that get their own δ_c (below it, complete shrinkage to f_global) is an operational guard.

This section is the placeholder for v2; v0 / v1 implement the partition class only (§9).

---

## 6. The availability algebra and leak checking

The propagation rules connecting the sources' `availableAt` (§2.3) with the spec's `predictAt` (§3),
formulated as type rules validation checks mechanically.

### 6.1 Propagation

Each column's availability is derived from its inputs by a per-scope rule. The only operation is
**max (join)**.

**The algebra's input is `effectiveAvailableAt`** (§2.6.1): for every source field the leaf value is
`availableAt + ingestionLag` (the snapshot time `at` for fields with `snapshotOf`), and all propagation
uses it. "availableAt" in the table below means `effectiveAvailableAt`.

| scope | output availability |
|---|---|
| `row` | max over the input fields (the row itself) |
| `context` | max over the inputs of every row of the group (all but self under `excludeSelf`). **The row-set determination time is a lower bound too**: with `snapshotOf` the snapshot time `at` (the row set is the set at that time); without it, a `corrections` source's row set is the final set and validation warns about a possible composition drift (training denominator = final participants, serving = before withdrawals). No warning for `appendOnly` |
| `sequence` | max over the contributing past rows' availability(t'), plus the availability of the `$self` fields a `window.filter` reads (§4.3). Windows are strictly past (§4.3), so each t' value is settled on the t' side |
| `population` | fit-boundary semantics. `asOf: event` aggregates take the max of their contributing rows; a transform with a fit block (cadence) has "artifact of boundary D available at the start of D" (fitted on data before D) |
| `baselines` | the row or context rule of the fields the expression reads |

**Origin tags (`derivedFrom`)** propagate over the same DAG: leaves are the source fields' `kind`,
the operation is set union. Two different monoids (max and union) over one path — one propagation
pass carries both.

Consequences:

- A boolean "known at prediction time" is the special case that collapses availability to
  before / after the event.
- Fit-boundary semantics (cadence / asOf) are the availability declaration of population artifacts —
  two faces of one mechanism.
- "Post-event information becomes usable after passing through a sequence" (a past session's outcome
  feeds this prediction) is not a promotion rule but the natural consequence of evaluating availability
  per row: a past row's `after(event)` is `event_time(t') + settlementLag`, usable when it precedes
  `predictAt(t)`.

### 6.2 Validation rules and the limits of static checking

**Basic rule**: for every row of every emitted column (not excluded by `output.exclude`), derived
availability (= `effectiveAvailableAt`) ≤ `computeAt` ≤ `predictAt` (`computeAt` defaults to
`predictAt`, §3). For columns with `validFor` (§2.4) the upper side `predictAt ≤ availableAt + validFor`
is checked too; a violation (expiry) is not a static error but a **run-time null + staleness flag**
(`nullPolicy: indicator`), because expiry depends on data arrival, not on the definition.

An availability expression has the form `event_time(own or referenced row) + δ` (δ = the
`availableAt` offset plus the ingestion lag), so for row / context everything stays inside one event and
is **statically checkable** (compare the max δ with `predictAt`'s δ).

A sequence references other rows' event times, so whether `event_time(t') + δ' ≤ event_time(t) −
δ_predict` holds **depends on the event spacing and is not statically decidable in general** (only
`t' < t` is known). Three tiers:

1. **Static (near-edge shift)**: when δ' (the referenced field's availability offset + ingestion lag)
   is a constant duration, the window's near edge is set deterministically to `event_time(t) −
   δ_predict − δ'` — `RANGE BETWEEN <maxAge> PRECEDING AND <δ' + δ_predict> PRECEDING` in SQL, the
   upper bound of the state read in the engine. The hand-written "near-edge offset ≥ settlement
   delay" convention is this rule. This is the **standard path** for sequences; no per-row filter is
   evaluated.
2. **Static (conservative, `minInterval`)**: when a per-entity minimum event interval is declared on
   the source or the entity, `minInterval ≥ δ' + δ_predict` makes the window safe without a shift.
   Entities appearing several times a day defeat it, so tier 1 takes precedence.
3. **Run time (availability filter)**: when δ' is not bounded by a constant (`atRowCreation`, per-row
   delays), the engine filters the window's past rows by `effectiveAvailableAt(t') ≤ computeAt(t)`
   before aggregating. **Provided `ingestionLag` really bounds reality from above**, this matches the
   training computation with what serving actually has (an unsettled same-day outcome is excluded from
   the window in training too). Filtering on `availableAt` alone (without `ingestionLag`) would introduce
   train/serve skew instead (§2.6.1). Validation marks such sequence columns "availability filter
   required" and demands the engine implement it.

### 6.3 The two leak grades

The leak classification of the fit-boundary literature (structural = weak / value = strong) appears
here as: value leaks fail the basic rule; structural leaks (faint contamination of model structure
inside a fit boundary) are finer than the algebra's granularity (fit boundaries) and are outside its
scope — they are contained operationally by conservative fit boundaries.

### 6.4 The `_` prefix is derived (a lint rule)

Availability's source of truth is the sources' `availableAt`; naming is **derived** from it:

- The compiler adds the `_` prefix to columns with `availableAt > predictAt` (intermediate
  representations, offline-only outputs) automatically (or lints its absence).
- References inside the DAG use the canonical name (no prefix); the prefix is confined to the display
  / output name layer. A schema change cannot break references, and the state where prefix and
  declaration disagree (which one is right?) cannot occur.

A user sees from the column name whether it may be used at prediction time, and the reason is always
traceable to the schema.

### 6.5 Implicit premises of the encoding (hierarchy / additive)

The conditions under which the shrinkage / decomposition is faithful, and the fallback when they do
not hold (a statement of premises; the schema is unaffected):

| premise | content | fallback |
|---|---|---|
| scale additivity | effects compose additively on `shrinkage.scale`; interaction is scale-relative | change the scale / factorization (weaker scale assumption, §4.4) |
| Gaussian approximation | closed-form EB is the normal-normal BLUP; binary / skewed residual targets use transform + delta method (§5.5 rule 7) | tiny cells shrink away, little harm; a conjugate variant where exactness matters (§5.1.1, without additive) |
| within-layer exchangeability | one shrinkage strength λ per layer (nodes of a layer share a variance component) | per-layer heteroscedastic λ is intentionally out of scope; adjacency similarity of ordered levels belongs to splines / binned estimators (§5.6) |
| balance (sequential) | sequential residuals are order-dependent under imbalance | `estimator: joint` (§5.5 rule 1), confounding audit hint (§7) |
| independent observations | within-event dependence and entity repetition make effective n < nominal n → under-shrinkage | `weights: heldOut` (§5.5 rule 8) |
| no uncertainty propagation | sequential treats earlier point estimates as truth | belief propagation on the lattice would be exact — gain too small for the complexity, stated as unsupported |
| declared crosses | the lattice follows declared paths only; continuous × continuous interaction surfaces and higher-order search are out of scope | factorization / tree embeddings / external tooling (§8); tensor-product splines as a future extension |

---

## 7. Cross-cutting machinery

**Naming and lineage**: output names are generated as `{prefix}{feature}_{op}_{param}`. Every column
carries lineage metadata: definition block, scope, fit or not, expansion coordinates, **origin source
and derived availability**, **`derivedFrom`** (the set of origin kinds, §6.1), **`evidence`**
(whether a `declared` field is among the origins), `computeAt`, placement (parent / child, §3.1).
`exclude` accepts name selectors (`"hist_signature.*"`) and lineage selectors (`"derivedFrom:market"`,
`"evidence:declared"`, `"scope:population"`) to mechanise bulk ablation exclusion, market-derived
exclusion and fit-only leak audits. `validate --expand` lists `derivedFrom` per output column — an
incident of the type "most of the lift concentrates in one column" is visible beforehand when
market-derived columns are identifiable from the lineage.

**Output contract**: `output.roles` (`group` / `time` / `entity` / `label` / `baseline` / `weight`)
declares which output columns are the consumer's keys, ordering, label and baseline rather than
features — the rule "a role column is never a feature" becomes mechanical for the training, screening
and evaluation steps that share the table. `output.include` is the projection (a list or a URI to a
screening step's pass list; it replaces `exclude` when declared, unknown names are a warning), and
`output.manifest` writes the contract at assembly: roles, every emitted column with lineage,
availability, status and placement, the pass-through fields with their contract, the plan hash and an
**output hash** (plan hash + projection + roles + include content). `include` / `manifest` are outside
the plan hash — a projection does not change what is fitted, so fit artifacts stay valid — which is why
the output table carries a hash of its own. A batch run appends `manifest.run.json` with the row count
and the observedAt audit results.

**Validation (`validate --expand`)**: a dry run returning (a) DAG cycles, undefined references, ref
resolution; (b) rejection of a population reading a sequence's future; (c) every expanded (desugared)
output column with type and evaluation order, plus the estimate of partition switches (key changes)
and shared sorts (§4.3); (d) warnings on `asOf: latest` and nesting alternatives; (e) **the availability
check**: per output column the derived `effectiveAvailableAt` against `computeAt` / `predictAt`
(statically safe / near-edge shift / filter required / violation), grouping by `computeAt`, undeclared
lineage fields, `time.field` vs. the sources' `eventTime`, missing `observedAtField` on pre-event
relative claims (error unless `evidence: declared`), warnings on outputs derived from `declared`
fields (`kind: market` + declared = error by default; per-field `allowDeclared` + `justification`
downgrades and the applied columns are listed), the `measured` (or allowDeclared) requirement on
market fields a baseline reads, `computeAt ≠ predictAt` on blocks referencing an offset, a hint when an
outcome-target encoding has a declared baseline but no offset, a composition-drift warning for context
features on a corrections source without `snapshotOf`, the encoding-rewrite hint for
`sequence.aggregate` `mean | rate` on outcome fields (`strict` = error); (f) **shrinkage and
structured-key checks**: an explicit `estimator: backoff` on an overlapping lattice (cross interactions,
`additive`, n-grams) is an error; an `additive` keySet without the single-key keySets in the block is an
error; a lattice containing `additive` without `shrinkage.scale` is an error (a hint for binary targets
inheriting identity); fitted discretisation (`method: tree | optimal`) with an inconsistent fit
convention against the encoding keyed on it (one side of the two-stage target consumption in-fold) is
an error; a past-target-derived key without `fit.groupBy` is an error; varying-coefficient tensor
dimensions are part of the column estimate; negative-variance truncation (complete shrinkage of a
level) is reported as information — a detection of layers whose shrinkage is effectively void is
feedback to feature design. Structured errors are returned and are the basis of an agent
self-correction loop.

**Train/serve consistency**: fit artifacts (aggregate tables, embedding matrices, SVD bases) persist as
Avro-encoded parameters; inference reproduces them deterministically from the same YAML + artifact
reference, verified through the content hash. The sources document is part of the hash (changing an
`availableAt` invalidates the checks of every dependent feature).

**Typed fits**: blocks with a fit (population, and the encoder / compress inside a sequence) declare
`fit.scope` so in-fold fitting is enforced mechanically. Row-local deterministic transforms (signatures,
EWMA) are distinguished by type.

**Data audit (declaration vs. data)**: `availableAt` is a declaration the data may violate (rows of a
"t-10 bid" column timestamped after `event_time − 10 min`). For `measured` fields (`observedAtField`),
the engine audits every row at the entry (implemented): rows observed after the declared availability
(`late`), after `predictAt` (`afterPredictAt`, the actual leak) and rows without an observation time
are counted as metrics and, with `output.manifest`, written to the run manifest together with the
deciles of `predictAt − observedAt`; `audit.observedAt: fail` routes late rows to the failure output.
`declared` fields cannot be audited and are marked "not auditable" in the
`validate --expand` output and the lineage — an unauditable declaration is an untrusted declaration,
and its derived columns carry a warning (market = error, §2.3). Likewise for `ingestionLag`: when the
input relation has an ingestion-time column (`ingestedAtField`), an audit detects `ingestedAt >
availableAt + ingestionLag`. For cross keys (composite keySets with a lattice), a **confounding audit**
(summary statistics of the skew of the observed cross-cell distribution — one seller concentrated in one
category, say) is generated from the sources and the spec. When a strongly confounded keySet specifies
`estimator: sequential`, validation hints towards `joint` (§5.5 rule 1).

**Feature logging (serving-time logs)**: a log-and-wait mechanism that records the feature values
actually served at `predictAt` and joins them with the later-arriving labels. Execution is the engine's
responsibility (feature-engine.md); the spec's three touch points:

1. **Log schema derivation**: a log record = output column values + `predictAt` + the content hash of
   the definition and fit artifacts + profile id + lineage metadata. The hash prevents mixing logs across
   definition versions in one training set. The schema is derived deterministically from the output
   definition and the lineage.
2. **Re-entry as a logged source (closed loop)**: a logged feature table can be registered as a
   source, and its `availableAt` = the log time (= that row's `predictAt`) is **constructively correct**
   (the value really was served then). Feature logging is not a special mechanism outside the DSL but a
   closed loop producing a new source with trivially guaranteed availability; the training pipeline
   consumes the logged source and the ordinary checks apply.
3. **Skew audit**: comparing the logged value of a row with its recomputed value (online / offline
   skew) is a "log vs. recompute" audit of the same shape as the data audit (`validate --audit`). The
   interpretation follows from `mutability`: a difference in a column derived from an `appendOnly` source
   is an implementation bug; from a `corrections` source it is the image of a correction.

**Path selection**: the three ways of producing training data follow mechanically from the sources
declaration (`mutability` × `snapshotOf`, §2.6.2):

| mutability | snapshotOf | training truth | retroactive | train/serve consistency |
|---|---|---|---|---|
| appendOnly | — | **backfill** (recompute from final values) | yes | constructive (first report = final value) |
| corrections | present | **snapshot-backfill** (recompute from archived snapshots) | yes (where snapshots exist) | constructive when the snapshot time `at` ≤ `computeAt`; `at` > `computeAt` is a check error |
| corrections | absent | **log-and-wait** (feature logging) | no | constructive |

A `corrections` source without `snapshotOf` and before logging runs leaves only final-value backfill
(training on corrected values). Validation warns about this "inconsistent backfill" and records it in
the lineage (an operational compromise, not a definition error — no error). Retroactive evaluation of a
new definition over `corrections` sources presupposes a snapshot archive (daily partitions); log-and-wait
is not retroactive, so consistent training data exists only from the start of that archive on.

---

## 8. Contract points for external tooling

The DSL is *extensional and deterministic*: one definition = one determinate set of columns, and it
guarantees operational correctness (fit convention, leaks, train/serve). Generative or exploratory
tooling (a feature-search tool, an agent that proposes candidate blocks) is deliberately **not** unified
with it; it relates to the DSL by compilation, through these contract points of the compile layer:

1. **One operator catalog** — name, signature, scope, fit or not, cost estimate, expandable fields —
   used by the DSL for validation and available to tooling to build a search space
   (`OperatorCatalog`).
2. **Search dimensions = the DSL's configuration axes** (a search space lifts a field from a constant
   to a distribution or a choice list).
3. **Candidate identity by canonical hash** (`FeaturePlan.getHash`): syntactically different,
   semantically identical candidates are not evaluated twice.
4. **A shared validator** (`validate --expand`, exposed over REST / MCP / the agent): a candidate is
   validated before running, and the structured errors — including the availability check of §6.2 —
   are the search feedback.
5. **One data catalog**: the sources document's field types, descriptions and `availableAt` are the
   data catalog tooling reads ("what can be done" from the operator catalog, "to what" from sources).
6. **Fit-artifact metadata as feedback**: structural metadata attached to fit artifacts — the fwfm r
   matrix (field-pair importance), per-level effective n and shrinkage-weight distributions of an
   encoding — flows back as input for priors and candidate generation. The typical loop: a
   factorization discovers an important field pair → tooling proposes the corresponding coarse cross
   → it is promoted explicitly into a `hierarchy` (§5.3.1) → the faithful shrinkage estimate takes over.
   This complements the hypothesis-driven limitation of lattice + ANOVA (only declared crosses are
   estimated) by a division of labour between layers.

Rule of thumb: a systematic enumeration a human can oversee (tens to hundreds of columns) is written
statically as a DSL product; the moment evaluation-based selection or iteration enters, it is the
tooling's domain.

---

## 9. Implementation phases

**v0**: sources (types, descriptions, `availableAt`, **`ingestionLag`**, **`observedAtField` /
`evidence`** (+ the runtime observedAt audit, §7), **`kind`**, **`snapshotOf`** (declaration and lineage resolution; the snapshot join itself
stays upstream), `validFor`, default inheritance) + **the availability check for row / context /
sequence** (propagation on `effectiveAvailableAt`, static checks, near-edge shift, availability-filter
directive, `validFor` expiry, prefix lint, missing `observedAtField`, declared-origin warnings,
composition-drift warning) + `derivedFrom` lineage and lineage-selector `exclude` + **per-field
`allowDeclared` + `justification`** + row + context (**`inputs` × ops sugar and the op-side `fields`
normal form**) + sequence sugar ops (lag / delta / trend / ewma / runLength / sinceEvent; **`expr`
desugaring and the `$self` restriction, several `windows` with a fixed near edge**) + encoding (product
expansion, hierarchy, expanding fit) + **`output.groupBy`** (re-aggregated output per context). This
covers most practical use and stays within what compiles to window functions (a single-node SQL engine
could execute it). The sources contract items (`ingestionLag` / `observedAtField` / `snapshotOf`) are in
v0 because retrofitting leak checks forces a full revision of existing definitions, and checks without
them mass-produce "legal" skew.

**v0 additions**: the output contract (`output.roles` / `include` / `manifest`, §7) + the shrinkage block (`weights: fixed | varianceComponents`, recursive shrinkage of
chain lattices, `leaveNodeOut`; the legacy `smoothing` stays as sugar) + keySet `structure: hierarchy |
cross` + generalised `hierarchy` (§5.3.1: coarse-cross entries, `additive`) + **keySet `windows`
(§5.3 rule 5) and the sequence → encoding rewrite lint** + **per-keySet `shrinkage` override (§5.3 rule
6)** + **sequence `window.filter` (`$self`)** + **`baselines` and `offset: <baseline>` references
(measured requirement, `computeAt` consistency, additive term on the scale)** + `shrinkage.scale`
(identity / logit / log, mandatory with `additive`) + `estimator: sequential` + `fit.groupBy` + the
corresponding checks of §7 (f). All are configuration differences of the back-off aggregation (per-level
key aggregation + interpolation pass), closed under aggregation, and still within window functions +
staged joins.

**v1**: **`computeAt`** (per-block computation time, together with serving) + the general sequence
form (the lti family as one HiPPO-style recurrence → logsignature separately) + the spectral population
methods (`spectralEmbedding` / `transitionStats` / `svd` — counts + SVD on existing numeric assets) +
nested encodings (structured refs / `asOf` / dependency hashes including the sources document) +
fit-boundary availability (cadence artifacts as `availableAt`) + **feature logging** (log schema
derivation, logged-source re-entry, skew audit; starts with serving so logs accumulate from day one).

**v1 additions**: conjugate families (`family: betaBinomial | gammaPoisson | dirichletMultinomial` —
one implementation of pseudo-statistic addition) + `output: deviations | effectiveN` + `emitConfidence`
(`__conf` companions) + `estimator: joint` (sufficient-statistic aggregation + driver-side linear solve,
moment estimates of variance components) + population `type: discretize` (quantile | tree | optimal) +
`type: factorization` (fm | fwfm, in-house ALS, r-matrix lineage output, **`embedding` / `sum`
outputs**) + generated confounding audit queries (§7) + contract point 6 of §8 + `structure: sequence`
(suffix back-off; shares the lattice and recursive-shrinkage implementation) + `transitionStats` blend
unified into shrinkage (§4.4).

**v2 and later**: the probabilistic family (Kalman), randomized signatures (`bilinear` + `randomize:
{rank, seed}`), basis extensions (fourier / haar), the featureSpec link to tooling, the as-of join
enrichment layer (§2.7).

**v2 additions**: linear-basis estimators (spline | rff, REML regularisation, aggregated sufficient
statistics + local linear algebra) + varying coefficients (cross × numeric basis, joint) + isotonic +
HMM state embeddings for Lift (distributed EM, with the probabilistic family) + factorization `variant:
bayesian` (Gibbs, model-per-key iteration) + third-order (HOFM / ANOVA kernels, reserved variant
values) + `weights: heldOut` integrated into the fit (held-out λ). Per the support matrix of §5.6, all
enter as values on existing axes (method / regularisation / variant) — never as new modules.

The implementation status against these phases is tracked in [feature-engine.md](feature-engine.md) §9.
