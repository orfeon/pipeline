---
name: query-lookup-sources
description: Developing and maintaining the query transform's SQL engine (Query2), its external lookup sources (jdbc/spanner/bigtable/rest), correlated LATERAL evaluation, and UDF/UDAF registration. Use when adding or changing a lookup source, touching util/pipeline/Query2 or util/pipeline/lookup, debugging "standalone scans are not supported" / "Lookup source id N is not registered" / lookup-join-not-chosen problems, adding UDFs to Query2, or extending the LATERAL machinery.
---

# Query engine & lookup sources

The `query` transform module runs one Calcite SQL statement per input element
inside a DoFn — no shuffle, windowing/timestamps inherited — optionally joining
external tables (JDBC / Spanner / Bigtable / REST) **on their keys** as batched
lookup-joins that never scan the external table. This subsystem was ported from
[orfeon/calcite-multi-engine](https://github.com/orfeon/calcite-multi-engine)
and is maintained independently here; this skill is the map plus the hard-won
knowledge from the port.

## Architecture map

Everything runs on the **Beam-vendored Calcite 1.40**
(`org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.*`). Never add a
regular `org.apache.calcite` dependency — two Calcites on one classpath is the
exact problem Beam's vendoring exists to avoid. Use the vendored Guava
(`...v1_40_0.com.google.common.*`) when a Calcite API needs it.

- `module/transform/QueryTransform.java` — the `query` module: config parsing
  (`sql` / `table` / `sources`), builds a `Query2`, runs it per element in a
  DoFn (`@Setup query.setup()`, `@ProcessElement query.execute(element, ts)`,
  `@Teardown query.teardown()`).
- `util/pipeline/Query2.java` — the reusable in-DoFn SQL engine. Lifecycle:
  construct at pipeline-construction time (plans the SQL, derives the output
  schema, releases clients) → serialize into the DoFn → `setup()` once per
  worker (re-opens clients, prepares the statement) → `execute()` repeatedly →
  `teardown()`. **The SQL→rel front-end is hand-assembled** (parse → validate →
  `SqlToRelConverter`, no decorrelation) — see the LATERAL section for why.
- `util/pipeline/lookup/` — the framework:
  - `LookupSource` (abstract): the SPI. Serializable config + transient
    clients; `setup()` registers in `LookupSourceRegistry` and derives table
    metadata once; `close()` deregisters and must leave the instance
    re-`setup()`-able. `tableSchemas()`, `keyCandidates(table)`,
    `lookup(table, indexName, batch, projects)`, `supportsKeyPrefixLookup()`.
  - `LookupJoinRule` / `LookupJoin` / `LookupJoinEnumerable` — the plain
    lookup-join: planner rule (key-prefix contract), enumerable rel (codegen
    embeds registry ids as constants), batched runtime (512-row batches, key
    dedup, one `lookup()` per batch, exact-match filtering).
  - `LookupLateralJoinRule` / `LookupLateralJoin` / `LookupLateralEnumerable` /
    `LookupLateralRuntime` — correlated LATERAL blocks (below).
  - `LookupTable` / `LookupSchema` — scan-rejected synthetic Calcite tables
    (huge row count so the optimizer prefers the lookup-join; `scan()` throws
    "standalone scans are not supported").
  - `CalciteValues` — primitive ↔ Calcite-internal value conversion (below).
  - `LookupKey` / `LookupRequest` / `LookupBatch` / `PerKeyLookup` — key model
    and the shared per-distinct-key loop for backends that can't array-bind.
- `util/pipeline/lookup/source/` — the four adapters: `JdbcLookupSource`,
  `SpannerLookupSource` (native tables + parameterized-query tables),
  `BigtableLookupSource`, `RestLookupSource`.
- `util/pipeline/udf/` — `UserDefinedFunctions` (serializable UDF/UDAF
  descriptors) and the built-in `DateTimeFunctions` (`CURRENT_DATE_`).

> The deprecated `util/domain/sql/calcite` package (MemorySchema etc.) is
> **not** part of this subsystem — only the old `util/pipeline/Query.java`
> still uses it. New work goes through `Query2`; migrating Query's remaining
> callers and deleting both is the standing cleanup.

## Value conventions — Calcite-internal values everywhere

Input-table scans and every `LookupSource.lookup()` produce values in
**Calcite's internal representation**, so join keys compare equal with no
coercion. When writing a new source, return exactly these (see `CalciteValues`):

| SQL type | Java value |
|---|---|
| VARCHAR | `String` |
| BOOLEAN | `Boolean` |
| SMALLINT / INTEGER / BIGINT | `Short` / `Integer` / `Long` |
| FLOAT / DOUBLE | `Double` (**float32 is widened** — never return `Float`) |
| DECIMAL | `BigDecimal` |
| DATE | epoch-day `Integer` |
| TIME | millis-of-day `Integer` |
| TIMESTAMP | epoch-millis `Long` (project micros are truncated) |
| BINARY | vendored Avatica `ByteString` |
| ARRAY / ROW | `List` / `Object[]` |

Result extraction inverts Avatica's local-wallclock rendering
(`Timestamp.toLocalDateTime().toInstant(UTC)` — NOT `toInstant()`, which is
wrong off-UTC); keep that pattern when converting result sets.

## The key contract (what joins become lookups)

A join is rewritten to a key-driven read when its condition constrains a
**contiguous prefix of a candidate key** (PK first, then unique indexes):

- **point** — equality on every key column;
- **prefix-only** — equality on a strict leading prefix, *only if the source
  opts in* via `supportsKeyPrefixLookup()` (ordered-key stores: jdbc, spanner —
  an index-backed prefix scan; REST/query tables must not opt in, their key
  columns are required request parameters);
- **prefix + bounded range** — leading equality plus both bounds on the next
  column.

Bound expressions may reference input columns **or literals** (that's how a
REST endpoint is fixed in SQL). INNER and LEFT only. A non-matching condition
leaves the join alone → the plan scans the lookup table → the scan throws the
rejection error at execution. That error is the intended UX, not a bug.

## Adding a new lookup source

1. Extend `LookupSource` in `util/pipeline/lookup/source/`. Serializable
   config fields; `transient` clients; derived metadata (schemas, keys) in
   serializable fields so workers don't repeat derivation — derivation happens
   at pipeline construction, so the **launcher needs connectivity**.
2. `setupInternal()` opens clients / derives metadata if absent (idempotent);
   `closeInternal()` closes clients and must tolerate re-setup. The base class
   owns registry register/deregister — forgetting nothing here; but a runtime
   error "Lookup source id N is not registered" means something closed the
   source while a statement still runs.
3. `lookup()` returns rows in `projects` column order (null = all columns),
   Calcite-internal values, **including the key columns** (fill them from the
   request via the request tuple when the backend doesn't echo them — see
   RestLookupSource.decodeRow). A superset is fine; the operator filters
   exactly. Backend can't array-bind a key set? Drive it with
   `PerKeyLookup.run` (rejects ranges, dedups tuples, skips null components).
4. Lookups must be **read-only and idempotent** — they run many times, in
   arbitrary order, and bundle retries repeat them. If an endpoint is
   input-derived, add an allow-list guard (see `allowedHosts`).
5. Wire it into `QueryTransform.createSource` + document it in
   `src/main/resources/server/docs/module/transform/query.md` (+ index.yaml).
6. Tests: hermetic first — H2 for JDBC-shaped sources, JDK
   `com.sun.net.httpserver.HttpServer` for HTTP (see `RestLookupSourceTest`),
   Testcontainers emulator `*IT` for cloud backends (see
   `SpannerQueryLookupIT`; run with `mvn verify -DskipITs=false
   -Dit.test=<Class>`; **no Maven profile** — a profile would deactivate the
   default dataflow profile). Always include: a result test, a LEFT-join
   null test, and a Java-serialization round trip
   (`JdbcLookupSourceTest#testSerializedRoundTripLikeDoFn`).

## Correlated LATERAL blocks (per-key set evaluation)

`JOIN LATERAL (SELECT ... FROM db.t WHERE t.key = i.k ...) ON TRUE` fetches
each key's rows once and evaluates the rest of the block (aggregation,
uncorrelated filters, ORDER BY / LIMIT) over that set in memory. How it works —
and why it's wired the way it is:

- **Upstream Calcite's `PlannerImpl.rel()` decorrelates unconditionally** (no
  flag; this is stock Calcite, not a vendoring artifact). Decorrelation
  rewrites equality-correlated blocks into join-of-aggregate-over-scan and
  range-correlated blocks into value-generator joins — both scan the external
  table. That is why `Query2.prepare` assembles the front-end by hand
  (`SqlParser` → `SqlValidatorUtil.newValidator` → `SqlToRelConverter`,
  matching PlannerImpl minus the decorrelate step) and claims lookup
  `LogicalCorrelate`s in a **HepPlanner pre-pass** before handing the tree to
  `RelRunners`. Non-lookup correlations (UNNEST over input arrays) still
  decorrelate later inside RelRunners' standard program — behavior unchanged.
- In Hep rules, children are `HepRelVertex` wrappers — unwrap before
  inspecting. A Hep-created physical rel must be **`Convention.NONE`** with a
  volcano `ConverterRule` to enumerable (`LookupLateralJoin.CONVERTER_RULE`);
  an enumerable rel with a logical child won't plan.
- The matched block must be a single Project/Filter/Aggregate/Sort chain over
  **one** lookup scan (derived-table nesting inside the block flattens into
  that chain and is fine); correlated conjuncts sit in filters directly over
  the scan and must fit the key contract (they become the fetch and are
  stripped); everything else stays. Residual correlations (non-key input
  columns in the block's WHERE/SELECT) are **not supported** — the rule
  declines.
- Multiple LATERAL blocks per FROM work, but **each block may correlate to a
  single source alias** — `SqlToRelConverter.getCorrelationUse` asserts "All
  correlation variables should resolve to the same namespace" when one block
  references two aliases (e.g. `i` and `s1`). This fails at conversion, before
  our rule. Chain instead: wrap the earlier join in a derived table and
  correlate the later block to that alias (test:
  `testChainedLateralViaDerivedTable`).
- The stripped block travels as **SQL text** (`RelToSqlConverter`, all
  identifiers double-quoted) and is compiled once per worker by
  `LookupLateralRuntime` against a mutable buffer table registered under the
  leaf's own `schema.table` name — plan-once/execute-many, one level down.
  `Query2` owns the runtimes' lifecycle (closed in `teardown`).
- LATERAL `ON TRUE` semantics: the block evaluates for **every** left row,
  including over the empty set — a global aggregate still yields one row
  (`COUNT(*) = 0`), so INNER keeps unmatched inputs there; a fan-out block
  yields none and INNER drops / LEFT pads.
- Rule injection for the volcano phase: add rules to
  `relNode.getCluster().getPlanner()` **and** thread-local `Hook.PLANNER`
  before `RelRunners.run` (both are kept deliberately; the pair is the proven
  combination and stays isolated across concurrent DoFns).

## UDF / UDAF

Register on the `Query2` builder — descriptors are serializable (class/method
names), materialized reflectively at setup:

```java
Query2.builder()
    .withScalarFunction("SCALE_", MyUdfs.class, "scale")   // all public static overloads
    .withAggregateFunction("SUM_OF_SQUARES", SumOfSquares.class) // init/add[/merge]/result
    ...
```

- The UDAF class follows Calcite's accumulator convention: public zero-arg
  constructor, `A init()`, `A add(A, V...)`, optional `A merge(A, A)`,
  `R result(A)`.
- **Register names in UPPERCASE.** The outer query resolves case-insensitively
  (BigQuery lex), but a UDF inside a LATERAL block round-trips through
  generated SQL whose unquoted identifiers are uppercased — uppercase matches
  both paths. Functions propagate into the per-block evaluator automatically.
- Built-ins live in `util/pipeline/udf/DateTimeFunctions` (`CURRENT_DATE_`)
  and are always registered; add new built-ins there.
- Functions are attached to the default schema via `SchemaPlus.add(name, fn)`
  in `Query2.createRootSchema` (and to the root schema in
  `LookupLateralRuntime.init`) — not via a custom operator table.

## Pitfalls (each of these cost real debugging time)

- **Per-element semantics**: the transform registers ONE element as a one-row
  table per evaluation. Batching elements would change GROUP BY semantics —
  don't, without a separate opted-in mode.
- **BigQuery lex**: reserved words as field names need backticks (`` `value` ``);
  output columns must have explicit aliases (`EXPR$0` is rejected at
  construction). `RUNNING` is reserved (a MATCH_RECOGNIZE keyword) — a
  surprising alias failure.
- The validator's operator table chains the **BigQuery library**
  (`SqlLibraryOperatorTableFactory`, in `Query2.prepare` and mirrored in
  `LookupLateralRuntime.init`) — that is what provides `ARRAY_AGG` etc.
  Window functions (`OVER`) work in the enumerable path, both outer and
  inside LATERAL blocks. One roundtrip wart: `ARRAY_AGG(x ORDER BY y)`
  inside a LATERAL block fails — `RelToSqlConverter` unparses the collation
  as `WITHIN GROUP`, which Calcite's own validator then rejects for
  ARRAY_AGG. Ordered folds go in the outer `GROUP BY` form instead. Array
  values crossing the lateral inner-statement boundary arrive as Avatica
  `java.sql.Array`s — `CalciteValues.asList/toInternalList` converts them
  back to internal `List`s (both in the lateral runtime and the outer
  result conversion).
- **`float32` → `FLOAT` (double)**: `CalciteSchemaUtil.convertSchema` maps
  float32 to Calcite `FLOAT`, which is 8-byte. Any source returning `Float`
  where the schema says float32 will CCE in generated code — widen to `Double`
  and surface the column as float64.
- **Timestamps are millis inside SQL** (Calcite TIMESTAMP(3)); the project's
  epoch-micros primitives are converted at the boundary, sub-millisecond
  precision is not observable in queries.
- Spanner query tables (parameterized GoogleSQL/GQL): the statement must
  return the key column; `SEARCH()` has no array form → `PER_KEY` (one
  single-use read per key — a single-use `ReadContext` dies after one query);
  schema derivation dry-runs via `analyzeQuery(PLAN)` retrying INT64/STRING/
  BYTES bind types.
- Emulator ITs: graph/search DDL goes in a separate `updateDatabaseDdl` with
  `Assumptions.abort` so an older emulator skips instead of failing; pin an
  image known to support both (`gcr.io/cloud-spanner-emulator/emulator:1.5.43`).
- Registries (`LookupSourceRegistry`, `LookupLateralRuntime`) exist because
  generated join code can only embed constants; ids are per-live-instance, so
  concurrent DoFns never collide, but every `setup()` must be paired with a
  close/teardown or the process leaks entries.
- **MATCH_RECOGNIZE does not work** on this stack (probed 2026-07, three
  independent blockers): (1) the validator throws "Cycle detected during
  type-checking" against our `StructKind.PEEK_FIELDS` row types
  (`CalciteSchemaUtil.convertSchema`) — a FULLY_QUALIFIED row type validates
  and converts to `LogicalMatch` fine, so this one is fixable on our side;
  but then (2) `RelRunners`' enumerable path cannot execute it — "Unable to
  implement EnumerableMatch" even for fixed-length patterns, and (3)
  quantified patterns (`UP+`) die earlier with "unknown kind:
  PATTERN_QUANTIFIER" — Calcite 1.40's enumerable MATCH_RECOGNIZE support is
  incomplete upstream. Supporting it would mean writing our own Match runtime
  operator (the origin investigation estimated this the riskiest option);
  the intended alternatives are ORDER BY/LIMIT/aggregation per key set, or a
  pattern-matching scalar UDF over the array column (the UDF registration
  hooks exist).
- **Sequence patterns: use the built-in `SEQ_MATCH` UDF**
  (`util/pipeline/udf/SequenceMatchFunctions` + `SequencePattern`), built on
  the **vendored Calcite pattern runtime**
  (`...calcite.runtime.Pattern/Automaton/Matcher`, the standalone machinery
  behind EnumerableMatch): zero new dependencies, `matcher.match(rows)` is
  per-call isolated (no cross-evaluation state), ~26µs per 6-row evaluation.
  Implementation notes that cost probing time: bounded `repeat(n,m)` /
  `plus` / `star` / `or` compile, but unbounded `repeat(n,-1)` throws —
  `SequencePattern.emit` rewrites `X{n,}` as n-1 copies + `X+`; the
  convenience `match()` gives predicates no history (`Memory.get(-1)`
  throws) — predicates instead receive the whole row list + index (`RowAt`),
  so `PREV` is plain list indexing; `Matcher.PartialMatch`'s fields are
  package-private — matched rows are read via one-time reflection. The UDF
  returns a fixed `ARRAY<ROW(matchNo,startIdx,endIdx))` (1-based) — the
  return type cannot be derived reflectively, hence the hand-built
  `ScalarFunction`+`ImplementableFunction` with
  `RexImpTable.createImplementor(new ReflectiveCallNotNullImplementor(m),
  NullPolicy.NONE, false)` — copy that plumbing for any future UDF whose
  return type isn't expressible in Java signatures. `UNNEST(udf(...))` and
  dynamic array indexing (`arr[m.startIdx].field`) both work in the
  enumerable path (verified). **Siddhi (io.siddhi:siddhi-core 5.1.33,
  Apache-2.0) was probed and rejected** for this use: detection and
  synchronous callbacks work, but a streaming CEP runtime has no reset —
  reusing a `SiddhiAppRuntime` across bounded evaluations produced phantom
  matches spanning arrays (proven), so correctness requires create+shutdown
  per evaluation at ~1.1ms each, and it drags
  quartz/disruptor/log4j-core/guava/snakeyaml onto the worker classpath.

## Origin & design rationale (for archaeology)

Ported 2026-07 from `orfeon/calcite-multi-engine` (regular Calcite 1.42,
`CalciteConnection`-based). Key deltas vs the origin, all deliberate:

- vendored Calcite instead of standalone (repo house style; zero new deps);
- one `Query2` engine instead of a per-query `CalciteConnection` (DoFn
  plan-once/execute-many lifecycle);
- prefix-only key equality added as a per-source opt-in (the origin required
  point or prefix+range; the fan-out enrich join `ON h.UserId = i.userId`
  against a composite-PK history table needs prefix-only);
- correlated LATERAL evaluation is new here (in the origin it was a design
  memo: LATERAL is the canonical per-key-set surface; the plain lookup-join is
  its degenerate case with an identity block);
- graph/search are configuration patterns of the generic Spanner query table,
  not separate source types.

Not ported (candidates for future work): the gRPC source (descriptor-set
driven dynamic client), residual correlations in LATERAL blocks, multi-leaf
LATERAL (UNION/INTERSECT of two sources), non-spanner parameterized-query
sources.
