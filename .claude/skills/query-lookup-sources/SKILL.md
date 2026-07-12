---
name: query-lookup-sources
description: Developing and maintaining the query transform's SQL engine (Query2), its external lookup sources (jdbc/spanner/bigtable/rest/grpc/sideinput/buffer), correlated LATERAL evaluation, and UDF/UDAF registration. Use when adding or changing a lookup source, touching util/pipeline/Query2 or util/pipeline/lookup, the buffer source's stateful DoFn in QueryTransform, debugging "standalone scans are not supported" / "Lookup source id N is not registered" / lookup-join-not-chosen problems, adding UDFs to Query2, or extending the LATERAL machinery.
---

# Query engine & lookup sources

The `query` transform module runs one Calcite SQL statement per input element
inside a DoFn — no shuffle, windowing/timestamps inherited — optionally joining
external tables (JDBC / Spanner / Bigtable / REST / gRPC) or other pipeline
collections delivered as Beam side inputs **on their keys** as batched
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
    The base class also owns the **on-memory lookup cache** (opt-in via
    `setCacheSpec` / the `cache` config block, Guava Cache, per-request
    granularity): runtime callers go through the final `lookupWithCache()`,
    which serves point/prefix-equality requests from cache, fetches only the
    misses and attributes returned rows to requests by key prefix; range
    batches and projections missing the key columns bypass it. Cached rows are
    shared — never mutate them. New runtime call sites should use
    `lookupWithCache`, not `lookup` (which stays the subclass SPI).
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
- `util/pipeline/lookup/source/` — the seven adapters: `JdbcLookupSource`,
  `SpannerLookupSource` (native tables + parameterized-query tables),
  `BigtableLookupSource`, `RestLookupSource`, `GrpcLookupSource`
  (descriptor-set-driven dynamic client — no generated stubs; the grpc/protobuf
  runtime is already on the classpath via Beam GCP IO, so it adds no
  dependency. Serializes the descriptor-set **bytes** so only the launcher
  needs the file; schema derived from the descriptor (float32 widened to
  float64 per the value convention, nested message → element row, repeated →
  array, `google.protobuf.Timestamp` → timestamp); one call per distinct key
  via `PerKeyLookup`; unary / `rowsFrom` fan-out / server-streaming. Tests
  run a descriptor-built in-JVM socket server, `DynamicGrpcTestServer` — no
  protoc), and `SideInputLookupSource` (another MCollection of the same
  pipeline via a Beam side input — **query transform only**, rejected in the
  beamsql/`createSources(List)` path. No clients, no launcher connectivity:
  schema/keyFields come from config; the DoFn feeds the window's contents via
  `setData(input, iterable, windowToken)` before each `execute` (same token →
  no-op; QueryTransform passes the `BoundedWindow`), rows are converted with
  `CalciteValues.toInternalRow` and hash-indexed lazily per constrained prefix
  length, once per window. Point/prefix/range all supported (opts into
  `supportsKeyPrefixLookup`); rows with null key columns are excluded from the
  index; `LookupRequest.prefix()` is an immutable list — `contains(null)`
  throws NPE, iterate to null-check. Don't add a `cache` block — data is
  already on-heap), and `BufferLookupSource` (the transform's own past input
  elements accumulated per group key in Beam state — see the buffer section
  below).
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

**Key columns pruned from the projection are fine.** `RelRunners`' standard
program field-trims before the volcano phase, so a query that never selects a
trailing key column (e.g. prefix-only `ON e.USER_ID = i.userId` selecting only
`CATEGORY`) reaches `LookupJoinRule` with that column missing from the
`Project` over the scan. The rule only requires the *constrained* key columns
(the matched prefix + range column) to be projected — a constrained column is
always projected because the join condition references it; unconstrained
trailing columns get a `-1` sentinel in `keyGlobalIndex` and simply never
match a conjunct. (Back-ported 2026-07 from the origin repo, where the same
over-restriction was found; test:
`JdbcLookupSourceTest#testPrefixLookupWithTrailingKeyColumnPruned`.)

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

- **`Query2.prepare` deliberately does NOT call
  `converter.flattenTypes(root.rel, true)`** (PlannerImpl does; stock JDBC
  prepare does not): the structured-type flattener rewrites any reference to
  a **nested ROW column of a lookup table** (`u.address.city`, or even
  selecting `u.address` whole) into a field-access `Project` directly over
  the scan, which `LookupJoinRule`'s InputRef-only projection matching cannot
  claim — the join silently degrades to the rejected standalone scan. The
  enumerable runtime executes unflattened `RexFieldAccess` fine, and removing
  the flattener was pinned behavior-neutral by the full suite. If a nested
  column of a lookup table ever stops joining, check this first.
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
- **Sequence patterns: use the built-in `SEQ_*` family** (ported from / kept in
  sync with orfeon/calcite-multi-engine, whose `sequence-patterns` skill is the
  full query-authoring guide; tests here: `Query2SeqMatchTest`,
  `Query2SeqFamilyTest`, `JdbcLookupSourceTest#testLookupHistoryToSequenceInOneQuery`):
  - `SEQ_MATCH(rows, fields, pattern, define [, mode])` — match ranges;
    optional mode `longest` (default) / `shortest` (pins the first occurrence)
    / `all` / `overlap` (every trigger evaluates).
  - `SEQ_MATCH_STEPS(...)` — one row per matched element with its **symbol**
    (`WHERE s.symbol = 'BUY'` aggregates exactly the matched purchases).
  - `SEQ_FOLD(_INT)(rows, from, to, field, op)` — range aggregation as an
    expression (field names resolved at plan time from the array's ROW type;
    `''` for scalar arrays, 0-based ordinal for untyped ones).
  - `SEQ_COLLECT(sortKey [, v1..v6])` — key-ordered collection aggregate:
    lookup fan-out + `GROUP BY` + `SEQ_COLLECT` = DB history as a sequence,
    order-independent. Prefer it over ordered `ARRAY_AGG` for sequence
    building (portable to the origin engine, and ordered ARRAY_AGG can't
    round-trip LATERAL blocks). Result is opaque — consume via
    SEQ_MATCH/STEPS/FOLD-with-ordinal, no UNNEST/projection.
  - `SEQ_SPLIT(rows, field, gap)` — time-gap sessionizer; expand with
    `UNNEST ... WITH ORDINALITY AS s(sess, sessionNo)` and match per session.
  - `SEQ_FUNNEL(rows, fields, timeField, window, steps)` /
    `SEQ_RETENTION(rows, fields, conditions)` — sliding-window funnel (max
    step reached) and cohort retention (`ARRAY<BOOLEAN>`, projectable).
    Conditions are positional `;`-separated DEFINE expressions without symbol
    names (`SequencePattern.compileConditions`), double-quoted under the
    BigQuery lex. `timeField` resolves against the *fields list* (ordinals
    cover SEQ_COLLECT results). Tests: `Query2AnalyticsFunctionsTest`.
  - Analytics companions (same origin sync): `QUANTILE(value, fraction)`
    (exact interpolated quantile UDAF), `APPROX_DISTINCT(value)` (HLL 2^12 —
    see the naming pitfall below), `ARRAY_DIFFERENCE(_INT)` /
    `ARRAY_CUM_SUM(_INT)` / `ARRAY_COMPACT` / `ARRAY_DISTINCT` (scalar-array
    transforms; `_INT`/DOUBLE variants project, COMPACT/DISTINCT are
    `ARRAY<ANY>` like SEQ_COLLECT), `TIME_BUCKET(ts, size)` (fixed-width
    timestamp floor; wrap in `UNIX_MILLIS` for a numeric key).
  - Linear-algebra built-ins (`MatrixFunctions`, thin adapters over the shared
    ojalgo core `util/domain/math/MatrixOps` — same core as the select
    module's matrix functions): `COSINE_SIMILARITY(a, b)`,
    `MATRIX_MULTIPLY(matrix, vector)` / `MATRIX_SOLVE(matrix, rhs)` (matrices
    are nested arrays — `ARRAY[ARRAY[…], …]` literals work; each also has a
    trailing-`columns` arity overload reading a **flat row-major** array —
    the SQL surface of `matrix`-typed fields, whose Calcite schema/value
    conversion maps them to flat `ARRAY<DOUBLE>` via `getMatrixValueType`,
    fixed in `CalciteSchemaUtil.createRelDataType` / `CalciteValues.toInternal`),
    `MAHALANOBIS(x, mean, precision [, columns])`, `POLY_FIT(xs, ys, degree)`, and the
    `LINEAR_REG(y, xs)` UDAF (Gram-matrix accumulator, mergeable; result is
    an opaque List like SEQ_COLLECT — wrap in `AS_DOUBLE_ARRAY(v)` to project
    an `ARRAY<DOUBLE>`). Tests: `Query2MatrixFunctionsTest`.
  - **BigQuery-lex quoting trap**: a define containing string literals must be
    a *double-quoted* SQL string (`"A: $action = 'promo'"`) — `''` escaping is
    the origin engine's Lex.JAVA convention and fails to parse here.
  The matcher core (`SequenceMatchFunctions` + `SequencePattern`) is built on
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

- **Never register a UDAF under a standard-operator name, and never register
  same-name same-arity overloads with heterogeneous parameter types.** Both
  crash validation with `AssertionError: ANY` from
  `SqlUtil.filterRoutinesByTypePrecedence` (once >1 candidate survives the
  parameter filter, the precedence comparator asserts on a param type — like
  our ANY — missing from the argument's precedence list). That is why the HLL
  aggregate is `APPROX_DISTINCT`, not `APPROX_COUNT_DISTINCT` (a std name),
  and why there is **no custom ARG_MAX/ARG_MIN** — the standard operators
  execute natively on the vendored-1.40 enumerable runtime (value's own
  return type, NULL keys ignored;
  `Query2AnalyticsFunctionsTest#testStandardArgMaxArgMinRunNatively` pins
  it). Arity-only overloads (SEQ_MATCH 4/5) remain fine.
- `CalciteValues.asList` accepts `List`, `Object[]` and **primitive arrays**
  — a NOT NULL element type (e.g. SEQ_RETENTION's `ARRAY<BOOLEAN NOT NULL>`)
  crosses the JDBC boundary as `boolean[]`, which the previous
  `(Object[]) array.getArray()` cast rejected.

## Buffer source (state-backed input history) — IMPLEMENTED 2026-07

`BufferLookupSource` + the `buffer` config type expose the query transform's
**own past input elements** as a lookup table: the input is keyed by
`groupFields` (`Union.withKeys`) and the query runs in a **stateful DoFn**
(`QueryTransform.BufferQueryProcessor` + a batch/streaming DoFn pair) that
accumulates each group's history in `OrderedListState<MElement>`. Design facts
that cost real thought:

- **setData-per-element**: like sideinput but with no window token — the DoFn
  feeds `setData(visibleRows, currentBufferElement)` before *every* evaluation
  (the buffer changes with every element). The `cache` block is **rejected in
  validation** — a cache over per-element-mutable data returns wrong rows.
- **Key affinity is the core correctness constraint.** Beam state is keyed per
  DoFn, so the source can only answer the current element's own group. Three
  layers enforce it: (1) `LookupSource.validateLookupBinding` — a planner hook
  both rules call with the input-side field name of each matched equality
  (rule-side extraction: plain join → unwrap CAST → `RexInputRef` into the left
  row type; LATERAL → unwrap CAST → `RexFieldAccess` over `RexCorrelVariable`);
  the buffer overrides it to require equality on **all** groupFields bound to
  same-named input columns, throwing an explanatory error at planning; (2) a
  runtime backstop in `lookup()` comparing each request prefix to the current
  element's key; (3) prefix-only lookups shorter than groupFields are rejected
  (state has no data for sibling groups — a silent miss otherwise).
- **Candidate key = `groupFields + __timestamp`** (synthetic TIMESTAMP column,
  the buffered element's event time; `__input` STRING = which `inputs` entry it
  came from — `MElement.getIndex()` set by Union). Keeping the synthetic
  columns **flat** (not nested under a `_` struct) is deliberate: nested
  columns can't be key columns (the rules match top-level InputRefs), SEQ_*
  `fields`/`timeField` parameters resolve flat names only, and nested access
  adds LATERAL SQL round-trip risk.
- **Stored-fields narrowing**: `LookupSource.markLookupUsage` — the rules
  report the scan columns a matched lookup uses (plain join: the projected
  columns; LATERAL: constrained key columns + leaf-coordinate references
  walked bottom-up until the first Project/Aggregate closes the demand). The
  buffer accumulates these during construction-time planning (serialized with
  the instance), and `QueryTransform.resolveStoredFields` persists only those
  input fields in state (explicit `fields` must cover them). The SQL-visible
  table schema stays full-width; unstored columns read as null — safe because
  the plan provably never reads them.
- **Ordering**: batch DoFn carries `@RequiresTimeSortedInput` (without it
  "past" is meaningless in batch); streaming processes in arrival order but
  the buffer *contents* stay event-time-ordered via OrderedListState (late
  elements insert at their correct position for later evaluations).
  `PCollection.isBounded()` picks the DoFn variant at expand time.
- **Retention**: approximate at write (`clearRange` works on timestamp
  boundaries — ties survive; the count-eviction boundary is capped at the
  current element's timestamp so a pending add is never range-cleared), exact
  at read (`visibleRows` trims duration first, then newest maxCount, current
  element included in the count when `includeCurrent`). A `count` ValueState
  avoids reading the buffer on non-trigger elements; it may drift up when
  duration eviction runs blind and self-corrects on the next overflow read.
- **State GC**: event-time TTL timer (`stateTtl` defaults to maxDuration) set
  to `maxTs + ttl` with **`withNoOutputTimestamp()`** — without it the timer
  holds the output watermark back by the whole TTL. `onTtl` re-checks
  `maxTs + ttl` against the firing timestamp to ignore stale firings. Batch
  variant declares no timer.
- **Write-before-evaluate**: the element is persisted before the SQL runs, so
  a failing evaluation still buffers (the failure goes to failureSinks).
  Elements with a null group-key component evaluate but don't persist (SQL
  null-equality could never look them up). Merging windows are rejected at
  construction; state (and therefore the buffer) is per key × window.
- **beamsql path**: rejected — `createSources(List, Map)` passes a null
  buffer input schema; only `QueryTransform` passes the union input schema.
- Runner note: needs OrderedListState (+ RequiresTimeSortedInput in batch) —
  Direct and Dataflow verified via tests; Flink/Spark unverified.
- Found while testing filters: `Filter.is()` compared strings via raw
  `compareTo` magnitude (`Math.abs(c) > 1 → return c > 0`, meant as the
  Float/Double NaN sentinel), so string equality silently passed for distant
  strings — fixed with a dedicated `INCOMPARABLE` sentinel (`Filter.java`).
- Tests: `BufferLookupSourceTest` (plan validation, range on `__timestamp`,
  runtime backstop, serialization round trip, narrowing) and
  `QueryTransformBufferTest` (batch cumulative via RequiresTimeSortedInput,
  streaming + TTL via TestStream driving the package-private DoFn directly —
  anonymous `TupleTag`s must be *static* fields in tests or they capture the
  non-serializable test instance, filters, maxCount eviction, `__input`).

## Reusing the sources from Beam SQL (`beamsql` module) — IMPLEMENTED

`util/pipeline/lookup/LookupSeekableTable` exposes any `LookupSource` table to
Beam SQL via the native seekable-table mechanism; `BeamSQLTransform` accepts
the same `sources` parameter as the query module (shared parsing:
`util/pipeline/lookup/source/LookupSourceConfig`, used by both transforms).
Wiring: build tables between `source.setup()`/`close()` at expand time
(metadata derived on the launcher), register per source via
`SqlTransform.withTableProvider(name, new ReadOnlyTableProvider(name,
LookupSeekableTable.tablesOf(source)))`; workers re-open clients in the
table's `setUp`. `seekRow(Row)` → candidate key matched by the sub-row's
field NAMES (any order, composite keys verified) → one point `lookup()` →
rows attached as Beam `Row`s.

Semantics & pitfalls (tests: `LookupSeekableTableTest`,
`BeamSQLTransformTest#testJdbcLookupJoin`):

- **Point equi-joins only** (no prefix/range/LATERAL), **one `seekRow` per
  input row** (no batching/dedup — jdbc/spanner/bigtable lose their batch
  advantage; the source-level on-memory cache (`cache` block →
  `lookupWithCache`) absorbs repeated keys). For heavy enrichment prefer the
  query module.
- Beam quirk: each equality must put the main-input column on the LEFT
  (`c.userId = u.USER_ID`); the reverse crashes in Beam's
  `JoinAsLookup.joinFieldsMapping` (AIOOBE).
- Beam quirk: the seekable-join output builder
  (`combineTwoRowsIntoOneHelper`) re-runs logical-type conversion on values
  that are already base-typed, so **any `java.time` logical column in the
  seek row breaks the join** regardless of how the Row was built — even when
  the column isn't selected (the seek row carries all table columns). Hence
  `LookupSeekableTable.beamSchema` surfaces DATE/TIME as ISO-8601 STRINGs
  (`CAST` in SQL when needed); TIMESTAMP is Beam's primitive DATETIME and
  passes through. Row values are `attachValues`'d in input form.

## Multi-statement sessions (`queries` / `exclusive` / buffer `insertSql`) — IMPLEMENTED 2026-07

`Query2` holds a list of named `Statement`s evaluated in order per
`executeAll()` (the transform's `queries` parameter; the single `sql` form is
a one-statement session). Facts that matter when touching it:

- **Every statement's result registers as an `IntermediateTable`** (raw
  `Object[]` rows, no MElement round-trip) in the next statements' root
  schema — output and non-output statements alike, so `output: true` results
  are also referenceable. The table's row type is captured from the producing
  statement's *logical* root rowType + `RelRoot.fields` names (identical on
  the construction/JDBC and worker/Bindable paths — the physical type is not
  needed) and re-materialized per planning type factory via
  `typeFactory.copyType` with `StructKind.PEEK_FIELDS`.
- **Schema derivation is keyed by statement index, not name**: the
  bufferInsert statement and the legacy unnamed single statement both have
  name `""` — keying by name silently hands the main statement's schema to
  `getBufferInsertSchema()`, which then poisons the stored fields (all-null
  buffer rows). Cost a debugging session.
- `exclusive` stops after the first **output** statement with rows (empty
  output does not stop; intermediates always run). Emission is
  collect-then-emit per element — no partial outputs on failure.
- The transform's named outputs ride the partition module's plumbing:
  `MCollectionTuple.and(name, ...)` → downstream `transformName.queryName`;
  the legacy single-sql form keeps the default unnamed tag.
- **buffer `insertSql`**: two-pass construction — pass 1 plans it *without*
  the buffer source (self-reference fails naturally with "not found") to get
  the row schema, then the real session includes it as a flagged first
  statement (`Builder.withBufferInsert`). Per element the processor runs
  `executeAll(…, insert=true, main=false)` first, persists the returned rows
  (multi-row `writeAndEvict`; `keep = maxCount - N`), then
  `executeAll(…, insert=false, main=true)` for the outputs — two calls
  because state write and `setData` must happen between them. Each insert
  row's groupFields values are validated equal to the input element's key
  (a transformed key would poison the key affinity). The key carrier passed
  to `setData` is built from the *input's* groupFields, since insert rows
  may be empty.
- Calcite validator quirk hit in tests: `UNNEST(arrayOfRow) AS e` where the
  element ROW has exactly **one field** does not resolve `e.field` ("Column
  not found in table 'e'") — with two or more fields it works. Not specific
  to sessions; keep test fixtures' element rows ≥ 2 fields or alias columns
  explicitly.

## Direct Bindable execution (`BindableQuery`) — IMPLEMENTED 2026-07

Per-element evaluation no longer goes through JDBC. `Query2.setup()` (and the
lateral runtime's `init()`) compile the plan once to a Calcite `Bindable` via
`util/pipeline/lookup/BindableQuery`; `execute()` binds a fresh `DataContext`
and drains the enumerable. Measured: simple filter+project **35µs → 0.8µs**
per evaluation (~44x); GROUP BY over 100 in-memory rows 44µs → 10µs (now
dominated by real work). Construction-time `deriveOutputSchema` still uses
the JDBC prepare (`RelRunners`) — schema derivation from `ResultSetMetaData`
is behavior we deliberately did not touch; only the worker execution path
moved. Hard-won facts:

- **The old 35µs fixed cost was NOT Avatica statement machinery.** RelRunners
  rewrites scans of `ScannableTable`s into `BindableTableScan`s, which plan to
  an `EnumerableInterpreter` whose generated `bind()` builds an `Interpreter`
  — including a HepPlanner optimization pass over the scan subtree — on
  *every execution*.
- **Scans use our own `InternalEnumerableScan`** (nested in BindableQuery), a
  physical rel whose generated code is the bare
  `Schemas.enumerable(table, root)` call (works because our
  `DataContext.getRootSchema()` returns the planning root schema — RelRunners
  can't assume that, hence its bindable rewrite). Two traps ruled out the
  stock alternatives: (1) leaving `EnumerableTableScan` to claim the scans
  generates a per-field conversion into synthesized record classes for
  array-of-ROW columns, which neither compiles (`Linq4j.asEnumerable(Object)`)
  nor matches our Object[]-based internal values; (2) the scan's PhysType
  must be built with `PhysTypeImpl.of(..., JavaRowFormat.ARRAY, false)` —
  optimize=true silently flips a single-column table to SCALAR format while
  the scan still emits Object[] rows → CCE in downstream generated code.
  Also: `RelOptTable.getExpression(ScannableTable.class)` already returns the
  full `Schemas.enumerable(...)` call expression — wrapping it again
  generates uncompilable nested calls.
- The pipeline mirrors the JDBC runner: `Programs.standard()` (sub-query
  removal — needed because `Query2` converts with `withExpand(false)` —
  decorrelate, trim, volcano, calc) run on the **cluster's own planner**.
  `RelOptUtil.registerDefaultRules(planner, false, false)` already includes
  the enumerable rules + `TO_INTERPRETER`; `BindableQuery.compile` re-invokes
  it defensively (idempotent) because the lateral runtime's Frameworks
  planner starts bare. The `Hook.PLANNER` dance is only needed on the JDBC
  path (where planning happens in the connection's own planner) and is gone
  from the worker path.
- **`EnumerableInterpretable.toBindable(internalParameters, ...)` stashes
  runtime objects into the passed map** (e.g. an interpreter's RelNode) and
  the generated code reads them back via `DataContext.get(name)` — the
  custom DataContext must serve that map or bind() NPEs. It must also supply
  the standard variables (`utcTimestamp`/`currentTimestamp`/`localTimestamp`/
  `timeZone`/`locale`/`cancelFlag`/`timeFrameSet`) or time functions break.
- **SQL output aliases live in `RelRoot.fields`, not the physical row type**:
  a trivial rename projection is removed by `ProjectRemoveRule` (it ignores
  names — `root.project(true)` does NOT survive planning). The JDBC layer
  renames at the metadata level; `Query2` threads `root.fields` into
  `CalciteValues.convertInternalRows`. Symptom when this is wrong: chained/
  derived-table queries silently return null for re-aliased columns.
- Single-column results may come out of the bindable as bare values or
  one-element arrays depending on the chosen PhysType (`Prefer.ARRAY` is a
  preference, not a guarantee) — `BindableQuery.toArray` normalizes.
- Values now arrive Calcite-internal (no Avatica local-wallclock rendering);
  `CalciteValues.toPrimitive`'s Number branches and the lateral runtime's
  `toInternal` already handled both forms, so conversions stayed shared.

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

Not ported (candidates for future work): residual correlations in LATERAL
blocks, multi-leaf LATERAL (UNION/INTERSECT of two sources), non-spanner
parameterized-query sources. (The gRPC source was ported 2026-07 — zero new
dependencies, since Beam GCP IO already ships grpc/protobuf.)
