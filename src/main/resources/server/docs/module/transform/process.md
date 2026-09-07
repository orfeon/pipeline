---
type: Transform Module
title: Process Transform Module
description: Process mining over an event log. Maps input rows to cases, activities (a field or first-match derivation rules), timestamps, resources and case attributes, replays every case in time order and emits the directly-follows graph (edges with frequency, distinct-case count and waiting-time statistics; nodes), the trace variants with case share and cycle-time statistics, one record per case, the resource handover-of-work network and a conformance summary of declared Declare constraints (existence, absence, exactly, init, end, response, precedence, succession, chainResponse, chainPrecedence, notCoexistence, duration / SLA) with per-case violations and fitness. Batch (global window) or streaming with session / fixed windows.
tags: [transform, process, process-mining, event-log, dfg, variants, conformance, declare, sla, analysis]
timestamp: 2026-09-07T00:00:00Z
---

# Process Transform Module

Transform module that runs **process mining** over an event log: every input row is an event of a case
(an order, a ticket, a user session, a loan application ...), the transform groups the events of each case,
replays them in time order and emits the standard process-mining outputs as tables:

- **discovery** — the directly-follows graph (which activity follows which, how often, in how many cases)
  and the trace variants (the distinct activity sequences and their share of the cases),
- **performance** — waiting time per edge and cycle time per variant / case (mean, min, max, median, p95),
- **conformance** — the verdict of declared [Declare](#constraints-declare) constraints per case
  (violations, fitness) and per constraint (violation rate), so a rule such as "every shipment is preceded by
  a payment" or "a case closes within 48 hours" becomes a measurable number,
- **organisation** — the handover-of-work network between resources (who passes work to whom).

The inputs are the rows the pipeline already moves into the warehouse — status-history tables, CDC change
records (`cdc` transform), application logs — so the event log is *derived in the pipeline* from the
operational data instead of being extracted by hand. The outputs are plain tables: write the default
`edges` output and `nodes` to BigQuery for a process map in any BI tool, `variants` for the variant
explorer, `cases` for case-level drill-down and joins with business attributes, `conformance` for a rule
dashboard, or route the `cases` output to an alerting sink in streaming.

## Outputs

| output | one record per | columns |
|---|---|---|
| (default) `edges` | directly-follows pair `source → target` | `source`, `target`, `frequency` (occurrences), `caseCount` (distinct cases), `durationMean`, `durationMin`, `durationMax`, `durationMedian`, `durationP95` (waiting time from `source` to `target`, in `performance.unit`; null on the synthetic start / end edges) |
| `nodes` | activity | `activity`, `frequency`, `caseCount`, `startCount` (cases starting with it), `endCount` (cases ending with it) |
| `variants` | distinct activity sequence | `variant` (activities joined by ` -> `), `activities` (array), `length`, `caseCount`, `caseShare` (fraction of all cases), `durationMean/Min/Max/Median/P95` (case cycle time) |
| `cases` | case | `caseId`, `variant`, `activities`, `length`, `truncated`, `startTime`, `endTime`, `duration`, `resources` (distinct, in order of appearance), `resourceCount`, the declared `attributes` (value of the first event where it is not null), and with constraints `violations` (names) and `fitness` (1 − violated / applicable) |
| `handovers` | resource pair | `source`, `target`, `frequency`, `caseCount` — consecutive events of a case handled by different resources |
| `conformance` | constraint | `constraint`, `type`, `cases`, `applicable`, `violations`, `violationRate` (violations / applicable) |

Downstream modules address the outputs as `<name>` (edges), `<name>.nodes`, `<name>.variants`,
`<name>.cases`, `<name>.handovers`, `<name>.conformance`. Every output exists even when it is empty
(`handovers` without a `resource`, `conformance` without constraints).

The synthetic nodes `__start__` and `__end__` appear as the source / target of the first and last edge
of every case (`dfg.startEnd`), so the process map has explicit entry and exit points and `nodes.startCount`
/ `endCount` are consistent with the edges.

## Parameters

| parameter | optional | type | description |
|---|---|---|---|
| caseId | required | String or Array<String\> | The field(s) identifying a case. Several fields form a composite id joined by `\|`. Events with a null case id are failure records. |
| activity | selective required | String | The field holding the activity name (any primitive type, converted to text). Exclusive with `activities`. |
| activities | selective required | Array<Object\> | Activity derivation rules `{name, filter}` evaluated in order; the first matching rule names the activity. `filter` is a [filter condition](../common/filter.md) — JSON or SQL-like text such as `"status = 'shipped' AND op = 'UPDATE'"`; a rule without `filter` matches every event. Events matching no rule are dropped (`engine.unmatched`). |
| timestamp | optional | String | The event time field (timestamp / date / ISO-8601 string / int64 epoch micros). Default: the element's event time. |
| sequence | optional | String | A numeric / string field ordering events that share a timestamp (a change-stream sequence number, a row version). Without it, ties keep an arbitrary order. |
| resource | optional | String | The field naming who / what performed the event (user, team, system). Enables `handovers`, `resources` and `resourceCount`. |
| attributes | optional | Array<String\> | Primitive input fields copied to the `cases` output (the first non-null value of the case), for breakdowns such as variant × country. Names may not collide with the `cases` columns or with the projected event columns (`activity`, `resource`, `sequenceInteger`, `sequenceNumber`, `sequenceText`). |
| dfg.minFrequency | optional | Integer | Edges observed fewer times are dropped from `edges` (noise filtering of the process map). Default `1` (keep all). |
| dfg.startEnd | optional | Boolean | Emit the `__start__` / `__end__` edges. Default `true`. |
| performance.unit | optional | Enum | Unit of every duration column: `millis`, `seconds` (default), `minutes`, `hours`, `days`. |
| constraints | optional | Array<Object\> | [Declare constraints](#constraints-declare) evaluated per case. |
| engine.maxTraceLength | optional | Integer | Activities kept in `activities` / the variant string per case. Longer cases are flagged `truncated: true` and their variant ends with `...(+n)`; every statistic still counts every event. Default `1000`. |
| engine.unmatched | optional | Enum | What happens to an event matching no `activities` rule: `drop` (default, logged under `unmatched` when `logs` asks for it) or `failure` (a failure record, subject to `failFast`). |
| engine.spillMemoryMB | optional | Integer | In-memory buffer per case while its events are sorted; larger cases spill sorted chunks to worker-local disk. Default: derived from the worker heap. |
| engine.spillDirectory | optional | String | Spill directory. Default: the worker's temp directory. |
| engine.spillCompress | optional | Boolean | Compress the spill chunks. Default `false`. |

### Constraints (Declare)

A constraint is `{name, type, activity, target, min, max, count, maxDuration}`; `name` defaults to a
readable form such as `precedence(Paid, Shipped)`. Each is evaluated on the full trace of every case and
reports whether it *applied* (its premise occurred) and whether it was *violated*:

| type | reads | violated when | applicable |
|---|---|---|---|
| `existence` | `activity`, `min` (default 1) | the activity occurs fewer than `min` times | always |
| `absence` | `activity`, `max` (default 0) | the activity occurs more than `max` times | always |
| `exactly` | `activity`, `count` | the activity does not occur exactly `count` times | always |
| `init` | `activity` | the case does not start with the activity | always |
| `end` | `activity` | the case does not end with the activity | always |
| `response` | `activity`, `target` | some occurrence of `activity` is not eventually followed by `target` | `activity` occurs |
| `precedence` | `activity`, `target` | some occurrence of `target` has no earlier `activity` | `target` occurs |
| `succession` | `activity`, `target` | response or precedence is violated | either occurs |
| `chainResponse` | `activity`, `target` | some occurrence of `activity` is not *immediately* followed by `target` | `activity` occurs |
| `chainPrecedence` | `activity`, `target` | some occurrence of `target` is not *immediately* preceded by `activity` | `target` occurs |
| `notCoexistence` | `activity`, `target` | both occur in the case | always |
| `duration` | `maxDuration` (ISO-8601, e.g. `PT48H`, `P7D`), optionally `activity` + `target` | the case lasts longer than `maxDuration`; with `activity` + `target`, the span from the first `activity` to the last `target` does | always; with the span, both occur and the last `target` is not before the first `activity` |

`cases.fitness` is `1 − violated / applicable` over the constraints that applied to the case (1.0 when
none applied); `conformance.violationRate` is the same ratio per constraint over all cases.

## Windows and streaming

The aggregates are computed **per window** of the input: in batch (global window) over the whole log; in
streaming the input must carry a non-global window from the [strategy](../common/strategy.md) — typically a
`session` window with a `gap` longer than the longest pause inside a case, so that a window closes a case
and its events are replayed together — or a `fixed` window for periodic snapshots of the process map. A
streaming input in the global window is rejected at assembly. Events of a case that arrive after its window
closed are not merged into the earlier replay: they form a new (partial) case in a later window. With a
session window the aggregates re-merge the sessions per output key, so overlapping cases share one
`edges` / `nodes` / `variants` row (and `caseShare` is relative to that merged window), while a case with no
overlap keeps a row of its own.

## Scale

The shuffle carries a compact event (case id, activity, timestamp, resource, sequence, attributes), not the
input row. A case's events are sorted with the same keyed spill sorter as the `feature` transform: in memory
up to the budget, sorted chunks on worker-local disk beyond, so one huge case (a device emitting millions of
events) never has to fit on the heap. The replay itself is streaming — only the capped activity list, the
per-activity and per-edge counters and the per-constraint state stay in memory (bounded by the distinct
activities of the case, not its events). `engine.spillMemoryMB` falls back to the `featureSpillMemoryMB`
pipeline option, as in the `feature` transform. The aggregated outputs are one merge-only
Combine per output; the duration quantiles are KLL sketches (k=200, error bound about 1.3% in rank).

## Example

A status-history table (one row per status change) mined into a process map, variants and rule checks:

```yaml
sources:
  - name: history
    module: bigquery
    parameters:
      query: |
        SELECT order_id, status, updated_at, updated_by, country
        FROM `myproject.shop.order_status_history`
        WHERE updated_at >= TIMESTAMP('2025-01-01')

transforms:
  - name: mining
    module: process
    inputs: [history]
    parameters:
      caseId: order_id
      activity: status
      timestamp: updated_at
      resource: updated_by
      attributes: [country]
      performance:
        unit: hours
      dfg:
        minFrequency: 10
      constraints:
        - {name: paidBeforeShipped, type: precedence, activity: Paid, target: Shipped}
        - {name: closedWithin7Days, type: duration, maxDuration: P7D}
        - {name: noDoubleRefund, type: absence, activity: Refunded, max: 1}

sinks:
  - name: edges
    module: bigquery
    inputs: [mining]
    parameters:
      table: myproject:process.order_edges
      createDisposition: CREATE_IF_NEEDED
      writeDisposition: WRITE_TRUNCATE
  - name: variants
    module: bigquery
    inputs: [mining.variants]
    parameters:
      table: myproject:process.order_variants
      createDisposition: CREATE_IF_NEEDED
      writeDisposition: WRITE_TRUNCATE
  - name: cases
    module: bigquery
    inputs: [mining.cases]
    parameters:
      table: myproject:process.order_cases
      createDisposition: CREATE_IF_NEEDED
      writeDisposition: WRITE_TRUNCATE
  - name: conformance
    module: bigquery
    inputs: [mining.conformance]
    parameters:
      table: myproject:process.order_conformance
      createDisposition: CREATE_IF_NEEDED
      writeDisposition: WRITE_TRUNCATE
```

### Deriving activities from change records

When the source is a CDC stream or a wide table rather than a status column, `activities` names the
activity from conditions on the row; a composite `caseId` and a `sequence` field keep events of one case in
their change order:

```yaml
transforms:
  - name: mining
    module: process
    inputs: [changes]
    parameters:
      caseId: [tenant_id, ticket_id]
      activities:
        - {name: opened,     filter: "op = 'INSERT'"}
        - {name: escalated,  filter: "op = 'UPDATE' AND priority = 'high'"}
        - {name: resolved,   filter: "op = 'UPDATE' AND status = 'resolved'"}
        - {name: reopened,   filter: "op = 'UPDATE' AND status = 'open'"}
        - {name: deleted,    filter: "op = 'DELETE'"}
      timestamp: commit_timestamp
      sequence: record_sequence
      resource: assignee
      engine:
        unmatched: drop        # other updates are not activities
```

### Streaming: deviations as they happen

With a session window, each case is replayed when its events stop for the gap, and the `cases` output
(with `violations`) can feed an alerting sink through a `select` filter:

```yaml
transforms:
  - name: mining
    module: process
    inputs: [events]
    strategy:
      window:
        type: session
        gap: 3600
    parameters:
      caseId: session_id
      activity: event_name
      constraints:
        - {name: checkoutWithin30m, type: duration, activity: add_to_cart, target: purchase, maxDuration: PT30M}
        - {name: paymentBeforeConfirm, type: precedence, activity: payment_succeeded, target: order_confirmed}

  - name: deviations
    module: select
    inputs: [mining.cases]
    parameters:
      filter: "fitness < 1"

sinks:
  - name: alerts
    module: pubsub
    inputs: [deviations]
    parameters:
      topic: projects/myproject/topics/process-deviations
```

## Limits

- **Case notion**: what a case is (an order, a ticket, an order × line item) is a modelling decision the
  config makes through `caseId`; the transform does not infer it. Object-centric logs (several case notions
  over the same events) are one `process` transform per notion.
- **Discovery is DFG-based**: the outputs are the directly-follows graph and the variants — the input of every
  process-map tool and of the Heuristic / Inductive miners — not a Petri net or BPMN model, and conformance
  is against Declare constraints, not against a model by token replay or alignment.
- **Lifecycle**: an event is a point in time; start / complete pairs of one activity are two activities
  unless merged upstream. Edge durations are therefore waiting times between event timestamps.
- **Variant strings** split on ` -> ` when the `activities` array is rebuilt; an activity name containing
  that sequence is split too.
