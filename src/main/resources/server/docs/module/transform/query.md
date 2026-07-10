---
type: Transform Module
title: Query Transform Module
description: Runs a Calcite SQL query over each input element inside a DoFn, without any shuffle. Array-of-struct fields can be expanded with UNNEST/LATERAL, and external tables (JDBC / Spanner / Bigtable / REST / gRPC) or other pipeline collections delivered as side inputs can be joined on their keys as batched lookup-joins — the external table is never scanned. A buffer source additionally exposes the transform's own past input elements, accumulated per key in Beam state, as a lookup table — per-key history, sequences and funnels with no external store. One element yields zero or more output rows.
tags: [transform, query, batch, streaming, sql, calcite, unnest, lateral, lookup, join, jdbc, spanner, bigtable, rest, grpc, sideinput, buffer, state, history, cep]
timestamp: 2026-07-03T00:00:00Z
---

# Query Transform Module

Transform Module for processing each input element with a SQL query, evaluated in-memory inside the worker (a `DoFn`).

Unlike the [beamsql](beamsql.md) module — which plans SQL over whole PCollections and may introduce shuffles — this module registers **each single element as a one-row table** and evaluates the query per element as a bounded in-memory computation:

- **No shuffle, ever.** The query runs entirely inside the DoFn. Windowing, triggering, and element timestamps of the surrounding pipeline are preserved, so batch and streaming behave identically.
- **Collections live inside the element.** Array-of-struct fields can be expanded with `UNNEST` (optionally with `LATERAL` subqueries), so aggregation (`COUNT`/`SUM`/`AVG`/…, `GROUP BY`, `HAVING`), ordering with `ORDER BY` / `LIMIT`, and set operations (`UNION` / `INTERSECT` / `EXCEPT`) all operate over the per-element collection.
- **Fan-out or fold.** One input element yields zero or more output rows: a query without aggregation fans out (one row per unnested item), an aggregate query folds the element into one row, and a `WHERE` that matches nothing drops the element.
- **External lookup-joins.** With `sources`, tables in external systems (JDBC databases, Cloud Spanner, Cloud Bigtable, REST/gRPC APIs) are referenced in the SQL as `sourceName.tableName` and joined **on their key** — a batched, index-backed key read per join, never a scan of the external table.
- **In-pipeline lookup-joins.** A `sideinput` source exposes **another collection of the same pipeline** (declared in the module's `sideInputs`) as a lookup table — no external store: the data is broadcast to the workers as a Beam side input and hash-indexed by the declared key once per window.
- **Per-key history joins.** A `buffer` source exposes **this transform's own past input elements**, accumulated per group key in Beam state, as a lookup table — sequence detection, funnels and per-key aggregates over recent events with no external store. This is the one source type that changes the pipeline shape: the input is keyed by `groupFields` and the query runs in a stateful DoFn (see below).

This is a good fit for enriching a stream with reference data held in a database or API, and for post-processing enrichment results with per-element SQL (aggregate or select over a fetched collection).

## External lookup-join contract

A join between the input and a lookup table is executed as a key-driven read when its `ON` condition constrains a **contiguous prefix of one of the table's candidate keys** (primary key first, then unique secondary indexes; Bigtable's row key; a REST table's request-parameter columns):

- **Point**: equality on every key column — `ON t.key1 = i.a AND t.key2 = i.b`
- **Prefix**: equality on the leading key columns only (JDBC / Spanner, index-backed prefix scan; fans out to all rows under the prefix) — `ON t.key1 = i.a`
- **Prefix + range**: equality on the leading columns plus a bounded range on the next — `ON t.key1 = i.a AND t.key2 >= i.lo AND t.key2 <= i.hi`

The right-hand side of each condition may be any expression over input columns **or a literal** (so a REST endpoint or header can be fixed in SQL). `INNER` and `LEFT` joins are supported. Key values are batched and deduplicated across the join, and one backend request is issued per batch (JDBC: one OR-of-tuples query; Spanner: one `read(KeySet)`; Bigtable: one multi-range `readRows`; REST: one HTTP call per **distinct** key tuple).

### Correlated LATERAL blocks (per-key aggregation / top-N)

The rows fetched for one key can be processed as a set *inside* the join, by writing the lookup as a correlated `LATERAL` subquery. The correlated conditions must satisfy the same key contract (they become the fetch); **everything else in the block — aggregation, `HAVING`, `DISTINCT`, uncorrelated filters, `ORDER BY` / `LIMIT` — is evaluated over the fetched per-key row set in memory, inside the DoFn** (compiled once per worker, never a shuffle):

```sql
SELECT i.userId, s.cnt, s.total
FROM INPUT AS i
JOIN LATERAL (
  SELECT COUNT(*) AS cnt, SUM(e.amount) AS total
  FROM db.EVENTS AS e
  WHERE e.USER_ID = i.userId          -- key correlation → the fetch
    AND e.SEQ >= i.since AND e.SEQ <= i.until   -- range correlation → bounds the fetch
) AS s ON TRUE
```

- **Multiplicity**: a non-aggregating block **fans out** — the outer result has one row per inner row (standard SQL LATERAL semantics; with `JOIN ... ON TRUE` an empty block result drops the input row, with `LEFT JOIN ... ON TRUE` it is kept with nulls). An aggregating block folds each key's set into **one row** (`COUNT(*) = 0` for a key that matches nothing, so the input row survives even with `JOIN`). To receive the fetched set as an **array column** instead of a fan-out, aggregate with `ARRAY_AGG` — for ordered arrays use the outer `GROUP BY` form:

```sql
SELECT b.userId, b.amounts   -- one row per input, fetched values as an ordered array
FROM (
  SELECT i.userId AS userId, ARRAY_AGG(h.amount ORDER BY h.ts) AS amounts
  FROM INPUT AS i
  JOIN db.HISTORY AS h ON h.userId = i.userId
  GROUP BY i.userId
) AS b
```

(`ARRAY_AGG(x ORDER BY y)` *inside* a LATERAL block does not currently round-trip — use it without `ORDER BY` there, or the outer `GROUP BY` form above.)
- `ORDER BY` / `LIMIT` inside the block give per-input top-N.
- **Window functions (`OVER`) are supported**, both over the join fan-out (`RANK() OVER (PARTITION BY i.userId ORDER BY h.amount DESC)`, running totals) and inside a LATERAL block (rank within the fetched set, filtered outside). Partitions never span input elements — everything is per-element.
- Restrictions: the block must be a single `SELECT` over **one** lookup table (derived-table nesting inside the block is fine — it flattens); correlated conditions must sit in the `WHERE` directly over that table and fit the key contract (correlations in the select list or against non-key columns are not supported); the plain lookup-join is exactly the degenerate case with an identity block.
- Multiple LATERAL blocks in one `FROM` are supported, but each block may correlate to **one source alias only** (a Calcite converter limitation — one block referencing e.g. both `i` and `s1` fails to plan). To feed an earlier block's output into a later block's key, wrap the earlier join in a derived table and correlate to that alias:

```sql
SELECT b.uid, s2.amount
FROM (
  SELECT i.userId AS uid, s1.maxSeq AS maxSeq
  FROM INPUT AS i
  JOIN LATERAL (SELECT MAX(e.SEQ) AS maxSeq FROM db.EVENTS AS e
                WHERE e.USER_ID = i.userId) AS s1 ON TRUE
) AS b
JOIN LATERAL (SELECT e2.AMOUNT AS amount FROM db.EVENTS AS e2
              WHERE e2.USER_ID = b.uid AND e2.SEQ = b.maxSeq) AS s2 ON TRUE
```

Any other use of a lookup table — a standalone scan (`FROM db.table` without the key-join), a non-key join condition — fails at execution with an explanatory error; there is no silent fallback to a full scan.

## Limitations

- The SQL sees exactly **one element per evaluation**. Aggregation across elements is out of scope — use the [aggregation](aggregation.md) or [beamsql](beamsql.md) module for cross-element grouping. (Aggregation over the rows fetched *for* one element — e.g. `GROUP BY` over a lookup fan-out — is fine.)
- Every expression in the select list must have an **explicit alias** (`AS name`); auto-generated column names such as `EXPR$0` are rejected at pipeline construction.
- Reserved words used as field names (e.g. `value`) must be quoted with backticks.
- Lookup sources are read at pipeline construction time to derive table metadata (JDBC `DatabaseMetaData`, Spanner `INFORMATION_SCHEMA`), so the machine that launches the pipeline needs connectivity to them; workers reuse the serialized metadata. (`sideinput` sources take their schema from the pipeline itself and need no connectivity.)
- Lookups must be **read-only and idempotent** — a lookup may run many times, in any order, and per-bundle retries repeat it. Never point a REST table at a side-effecting endpoint.
- `MATCH_RECOGNIZE` is **not supported** (the underlying Calcite enumerable runtime cannot execute it). For per-element sequence patterns, use the built-in `SEQ_MATCH` function (below), or `ORDER BY`/`LIMIT`/aggregation over the collection (optionally in a LATERAL block).
- `float32` external columns are surfaced as `float64` in SQL; timestamps are compared at millisecond precision inside the SQL engine.

## Transform module common parameters

| parameter | optional | type                | description                                                                                                 |
|-----------|----------|---------------------|-------------------------------------------------------------------------------------------------------------|
| name      | required | String              | Step name. specified to be unique in config file.                                                           |
| module    | required | String              | Specified `query`                                                                                           |
| inputs    | required | Array<String\>      | Specify the names of the step from which you want to process the data. Multiple inputs are unioned into a single stream. |
| parameters | required | Map<String,Object\> | Specify the following individual parameters.                                                               |

## Query transform module parameters

| parameter | optional | type   | description                                                                                                                                                                                              |
|-----------|----------|--------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| sql       | required | String | SQL query text. Also accepts a GCS path (`gs://...`), a local file path, a Parameter Manager resource path, or a base64-encoded string (`data:...`), same as the `beamsql` module.                        |
| table     | optional | String | Table name under which the input element is visible in the SQL. Defaults to `INPUT`.                                                                                                                     |
| filter    | optional | Filter | Filter-DSL condition over the input element (same syntax as the `filter` module). Elements not matching are dropped **before** the SQL evaluation — the filter costs ~100ns per element, an order of magnitude less than even a simple SQL evaluation, so use it to skip elements the query would discard anyway. With a `buffer` source, non-matching elements are neither buffered nor evaluated (contrast with `bufferFilter`/`triggerFilter`, which split those concerns). |
| sources   | optional | Array<Source\> | External lookup sources. Each source's tables are referenced in the SQL as `sourceName.tableName` and joined on their keys (see the contract above).                                              |

### Source common parameters

| parameter | optional | type           | description                                                                    |
|-----------|----------|----------------|--------------------------------------------------------------------------------|
| name      | required | String         | Schema name the source's tables are referenced by in SQL (`name.table`).       |
| type      | required | String         | One of `jdbc`, `spanner`, `bigtable`, `rest`, `grpc`, `sideinput`, `buffer`.   |
| tables    | required | Array<Table\>  | Tables to expose. Per-type table parameters below.                             |
| cache     | optional | Cache          | On-memory cache over the source's lookups (see below). Off unless the block is present. |

### Lookup cache

Because the SQL is evaluated **per element**, the same key is looked up again for
every element that carries it. The `cache` block keeps recent lookup results in a
bounded in-memory cache on each worker, so repeated keys (hot users, small master
tables, REST endpoints) skip the backend call. Cached results are **stale for up to
`expireAfterSeconds`** (in addition to any backend-side staleness such as Spanner
`maxStalenessSeconds`), so enable it only when the looked-up tables are read-mostly.

| parameter          | optional | type    | description                                                                          |
|--------------------|----------|---------|----------------------------------------------------------------------------------------|
| enabled            | optional | Boolean | Set `false` to keep the cache off while leaving the block in place (default `true`). |
| maxSize            | optional | Integer | Max cached lookup keys (default 10000).                                              |
| expireAfterSeconds | optional | Integer | TTL from write in seconds; omit or `0` for no expiry.                                |
| cacheEmptyResults  | optional | Boolean | Also cache "no matching row" results — recommended for LEFT joins against sparse tables (default `true`). |

Point and key-prefix equality lookups are cached per key (and per column
projection); **range conditions always go to the backend**. The cache applies to
all of the source's tables, is per worker thread, and counts hits/misses in the
`lookup_cache` metrics namespace (`<sourceName>_hit` / `<sourceName>_miss`).

```yaml
sources:
  - name: db
    type: jdbc
    driver: org.postgresql.Driver
    url: jdbc:postgresql://localhost:5432/mydb
    cache:
      maxSize: 10000
      expireAfterSeconds: 300
    tables:
      - name: users
```

### jdbc source

| parameter | optional | type   | description                                                                     |
|-----------|----------|--------|----------------------------------------------------------------------------------|
| driver    | required | String | JDBC driver class name (e.g. `org.postgresql.Driver`).                          |
| url       | required | String | JDBC connection URL.                                                            |
| user      | optional | String | Database user.                                                                  |
| password  | optional | String | Database password.                                                              |

Table: `name` (SQL table name, required), `table` (physical, optionally `schema.table`; defaults to `name`). Columns, the primary key and unique secondary indexes are derived automatically from `DatabaseMetaData`; identifiers are matched in the exact case the database stores them (e.g. uppercase for H2). A unique-index join condition selects that index automatically.

### spanner source

| parameter           | optional | type    | description                                             |
|---------------------|----------|---------|----------------------------------------------------------|
| projectId           | required | String  | GCP project of the Spanner instance.                    |
| instanceId          | required | String  | Spanner instance id.                                    |
| databaseId          | required | String  | Spanner database id.                                    |
| emulator            | optional | Boolean | Connect to the emulator (`SPANNER_EMULATOR_HOST`).      |
| maxStalenessSeconds | optional | Integer | Allow stale reads up to this bound (default: strong).   |

Two kinds of tables:

**Native tables** — `name` (SQL table name), `table` (physical; defaults to `name`). Columns/keys derived from `INFORMATION_SCHEMA` (ARRAY / STRUCT / TOKENLIST columns are skipped). Primary-key joins use the native `read(KeySet)` (points, prefixes and ranges); unique-index joins use a two-step `readUsingIndex` inside one read-only snapshot.

**Query tables** — a caller-supplied *parameterized GoogleSQL or GQL statement* exposed as a key-driven table: the generic "input key → parameterized query → rows" shape behind **Spanner Graph traversals** and **full-text search**. The statement never goes through Calcite — only its result rows do.

| parameter | optional | type          | description                                                                                    |
|-----------|----------|---------------|--------------------------------------------------------------------------------------------------|
| name      | required | String        | SQL table name.                                                                                |
| sql       | required | String        | Parameterized GoogleSQL/GQL statement; must reference `@<paramName>` and return the key column. |
| keyField  | required | String        | The join-key column (returned by the statement, so rows join back).                            |
| paramName | optional | String        | Bind parameter name (default `keys`).                                                          |
| bindMode  | optional | String        | `array` (default): bind the batch's distinct keys to one array param, run once — for `WHERE k IN UNNEST(@keys)` (graph). `per_key`: run once per distinct key binding a scalar — for `SEARCH(tokens, @query)`, which has no array form. |
| fields    | optional | Array<Field\> | Result columns; omit to derive them by dry-running the statement through `analyzeQuery(PLAN)` (no execution; INT64/STRING/BYTES key types are tried until one plans). |

Only point equality on the (single) key column is supported for query tables. One key may fan out to many rows (search hits, traversal results) — combine with a correlated LATERAL block for per-key top-N or aggregation.

### bigtable source

| parameter    | optional | type   | description                          |
|--------------|----------|--------|---------------------------------------|
| projectId    | required | String | GCP project of the Bigtable instance.|
| instanceId   | required | String | Bigtable instance id.                |
| appProfileId | optional | String | App profile for the data client.     |

Table parameters:

| parameter   | optional | type            | description                                                                 |
|-------------|----------|-----------------|------------------------------------------------------------------------------|
| name        | required | String          | SQL table name.                                                             |
| tableId     | optional | String          | Bigtable table id (defaults to `name`).                                     |
| rowKeyField | optional | String          | Column name for the row key (default `rowKey`).                             |
| rowKeyType  | optional | String          | `string` (default), `int64`, `bytes`, `int32`, `float64`, `date`, `timestamp`. |
| columns     | optional | Array<Column\>  | Value columns: `name`, `family`, `qualifier` (defaults to `name`), `type`.  |

The row key is a single opaque column and the **only** key — composite key structure (`user#ts`) is built by the caller in SQL, e.g. `ON b.rowKey = i.userId || '#' || i.date`. Cell bytes use the HBase-compatible codec (`DATE`/`TIMESTAMP` as epoch-microsecond INT64, numbers big-endian, strings UTF-8); only the latest cell version is read.

### rest source

| parameter    | optional | type                | description                                                     |
|--------------|----------|---------------------|------------------------------------------------------------------|
| baseUrl      | optional | String              | Prefix for relative `endpoint`s.                                |
| headers      | optional | Map<String,String\> | Default headers merged into every request (e.g. auth).          |
| allowedHosts | optional | Array<String\>      | If set, requests to any other host fail (SSRF guard).           |
| timeoutMillis| optional | Integer             | Per-request timeout (default 60000).                            |

Table parameters:

| parameter | optional | type                | description                                                                                   |
|-----------|----------|---------------------|-------------------------------------------------------------------------------------------------|
| name      | required | String              | SQL table name.                                                                               |
| endpoint  | required | String              | URL template with `{column}` placeholders.                                                    |
| method    | optional | String              | HTTP method template (default `GET`).                                                        |
| headers   | optional | Map<String,String\> | Per-request header templates.                                                                 |
| params    | optional | Map<String,String\> | Query-parameter templates (values URL-encoded).                                               |
| body      | optional | String              | Request body template.                                                                        |
| keyFields | optional | Array<String\>      | Key columns in key order; defaults to the placeholders in appearance order.                   |
| rowsFrom  | optional | String              | JSON pointer to a response array for fan-out (e.g. `/items`); without it the response is one row. |
| fields    | optional | Array<Field\>       | Response columns (standard schema fields JSON: `name`, `type`, …). Use type `json` for nested objects. |

Every `{column}` placeholder is a key column; binding a key column to a **literal** in SQL makes that request part static, binding it to an input column makes it dynamic. One HTTP call is made per distinct key tuple (point equality only). HTTP 404 is treated as "no row" — use a `LEFT JOIN` to keep the input element.

### grpc source

Exposes RPC methods of **any gRPC service** as lookup tables — no service-specific
stubs: the contract is a protoc **descriptor set** file
(`protoc --include_imports --descriptor_set_out=service.desc your_service.proto`),
and requests/responses are built dynamically (the `grpcurl` mechanism). The
descriptor set file only needs to be readable **on the launcher** (its bytes are
shipped to the workers with the pipeline).

| parameter              | optional | type                | description                                                        |
|------------------------|----------|---------------------|----------------------------------------------------------------------|
| target                 | required | String              | gRPC target: `host:port` or any name-resolver URI.                |
| descriptorSetPath      | required | String              | Path to the protoc descriptor set (`--include_imports` required). |
| plaintext              | optional | Boolean             | Use plaintext instead of TLS (default `false`).                    |
| headers                | optional | Map<String,String\> | Static request headers sent on every call (e.g. an auth token).    |
| timeoutMillis          | optional | Integer             | Per-call deadline (default 60000; `0` = none).                     |
| maxInboundMessageBytes | optional | Integer             | Max inbound message size.                                          |

Table parameters:

| parameter       | optional | type           | description                                                                                      |
|-----------------|----------|----------------|----------------------------------------------------------------------------------------------------|
| name            | required | String         | SQL table name (avoid Calcite reserved words such as `stream`).                                 |
| method          | required | String         | Fully-qualified RPC method: `package.Service/Method`.                                           |
| keyField        | required | String         | Column carrying the request parameter — the join key (`ON t.keyField = i.someColumn`).          |
| requestKeyField | optional | String         | Request field the key is bound to; dotted path for nested fields (default: same as `keyField`). |
| serverStreaming | optional | Boolean        | Mark a server-streaming method: each streamed message becomes one row.                          |
| rowsFrom        | optional | String         | Top-level *repeated message* response field to fan out over (unary only; one row per element).  |
| requestTemplate | optional | String         | Constant request fields as protobuf JSON (the key is overlaid on top).                          |
| fields          | optional | Array<Field\>  | Explicit response columns, overriding descriptor-set derivation.                                |

The row schema is **derived from the descriptor set** when `fields` is omitted:
scalars map naturally (float32 is widened to float64), enums become strings,
`google.protobuf.Timestamp` becomes a timestamp, singular nested messages become
nested rows (accessible as `t.address.city`), repeated fields become arrays. The
key column is prepended when the response does not echo it. One gRPC call is made
per distinct key (point equality only); a key with no result yields no row (use
`LEFT JOIN` for a default). Only expose **read-only, idempotent** RPCs — lookups
run many times, in arbitrary order, and bundle retries repeat them.

### sideinput source

Exposes **another collection of the same pipeline** as a lookup table, with no
external store: list the collection in the module's common `sideInputs` field, and
reference it from a `sideinput` source. The collection is delivered to every worker
as a Beam side input and **hash-indexed by the declared key once per window** (not
per element); lookups are then in-memory map reads. Point, key-prefix and
prefix+bounded-range joins are all supported (the source behaves like an
ordered-key store), including correlated LATERAL blocks.

The source itself has no parameters beyond the common ones. Table parameters:

| parameter | optional | type           | description                                                        |
|-----------|----------|----------------|----------------------------------------------------------------------|
| name      | optional | String         | SQL table name (defaults to `input`).                              |
| input     | optional | String         | Side input collection name, as listed in `sideInputs` (defaults to `name`). |
| keyFields | required | Array<String\> | Key columns in key order — the (composite) key joins constrain.     |

Notes:

- **Memory**: the whole side input is broadcast to and indexed on every worker —
  intended for dimension-sized reference data. For data that does not fit in
  memory, use a jdbc/spanner/bigtable source instead. The `cache` block is
  pointless here (the data is already on-heap) — leave it off.
- **Windowing / streaming**: standard Beam side-input semantics. In batch the
  side input is read once. In streaming, the side input collection must be
  windowed (or triggered) so its panes are available to the main input's windows;
  the index is rebuilt when the window changes — the "slowly-changing reference
  data" pattern (periodic refresh into a global-window side input) works as-is.
  Within one window the data is read once; later trigger firings of the *same*
  window are not re-read.
- Rows with a `null` in a key column never match (SQL equality semantics).

```yaml
transforms:
  - name: enriched
    module: query
    inputs: [orders]
    sideInputs: [members]
    parameters:
      sql: |
        SELECT o.orderId AS orderId, o.amount AS amount, m.userName AS userName
        FROM INPUT AS o
        LEFT JOIN side.members AS m ON m.userId = o.userId
      sources:
        - name: side
          type: sideinput
          tables:
            - name: members
              keyFields: [userId]
```

### buffer source

Exposes **this transform's own past input elements**, accumulated per group key in
Beam state (`OrderedListState`), as a lookup table — per-key history with no
external store: sequence detection (`SEQ_MATCH` / `SEQ_FUNNEL` over the history),
"how many times did this user do X recently", per-key running aggregates.

Unlike every other source type, a `buffer` source **changes the pipeline shape**:
the input is keyed by `groupFields` (a shuffle) and the query runs in a **stateful
DoFn**, so parallelism is bounded by key cardinality and hot keys concentrate load.
Because Beam state is keyed per DoFn, **at most one buffer source per transform**.

The buffer table's schema is the (union) input schema plus two synthetic columns:

| column        | type      | description                                             |
|---------------|-----------|---------------------------------------------------------|
| `__timestamp` | Timestamp | The buffered element's event time.                      |
| `__input`     | String    | The name of the `inputs` entry the element came from (useful when multiple inputs share the buffer). |

**Key contract**: state only holds the *current element's own group*, so a join
must constrain **every** `groupFields` column by equality **to the input column of
the same name** (`WHERE b.userId = i.userId`); optionally add a bounded range or
equality on `__timestamp` (`groupFields + __timestamp` is the candidate key). Any
other binding — a different column, an expression, a partial key — is rejected at
pipeline construction with an explanatory error.

Source parameters (the single table has `name` and optional `fields`):

| parameter          | optional | type            | description                                                                                                          |
|--------------------|----------|-----------------|------------------------------------------------------------------------------------------------------------------------|
| groupFields        | required | Array<String\>  | Input fields forming the state key, in key order. Elements with a null key component are evaluated but not buffered.  |
| maxCount           | optional | Integer         | Maximum buffer rows an evaluation sees (including the current element when `includeCurrent`); older rows are evicted. |
| maxDurationSeconds | optional | Integer         | Event-time retention window relative to the current element's timestamp; older rows are invisible and evicted.        |
| includeCurrent     | optional | Boolean         | Whether the current element itself appears in the buffer table during its own evaluation (default `true` — e.g. the triggering event is part of the matched sequence). Independent of `bufferFilter`. |
| stateTtlSeconds    | optional | Integer         | Streaming state GC: a group's state is cleared when no element arrived for this long (event time). Defaults to `maxDurationSeconds`; omit both and state is kept indefinitely. |
| bufferFilter       | optional | Filter          | Filter-DSL condition selecting which elements are **persisted** (default: all).                                        |
| triggerFilter      | optional | Filter          | Filter-DSL condition selecting which elements **evaluate the SQL** and produce output (default: all). Elements matching neither filter are dropped without state access. |
| tables[0].name     | required | String          | SQL table name (`sourceName.name`).                                                                                     |
| tables[0].fields   | optional | Array<String\>  | Input fields persisted per buffered element. Defaults to **the columns the SQL references** (derived from the plan), so state size follows the query automatically; when set explicitly, it must cover every referenced column. `groupFields` are always stored. |

```yaml
transforms:
  - name: funnel
    module: query
    inputs: [events]
    parameters:
      sql: |
        SELECT i.userId AS userId, i.amount AS amount, s.viewCnt AS viewCnt
        FROM INPUT AS i
        JOIN LATERAL (
          SELECT COUNT(*) AS viewCnt
          FROM buf.history AS b
          WHERE b.userId = i.userId
        ) AS s ON TRUE
      sources:
        - name: buf
          type: buffer
          groupFields: [userId]
          maxCount: 1000
          maxDurationSeconds: 604800
          bufferFilter:
            - { key: action, op: "=", value: view }
          triggerFilter:
            - { key: action, op: "=", value: purchase }
          tables:
            - name: history
```

Notes:

- **Batch vs streaming order**: in batch the DoFn requires **time-sorted input**
  (`@RequiresTimeSortedInput`), so each element's "past" is exactly the
  earlier-timestamped elements of its group — deterministic. In streaming,
  elements are processed in **arrival order**: the buffer's *contents* are always
  event-time-ordered (late elements are inserted at their correct position for
  later evaluations), but which elements are visible at evaluation time depends
  on arrival. Meaningful event timestamps on the input are a prerequisite (batch
  sources often assign a constant timestamp — set `timestampAttribute` or assign
  timestamps upstream).
- **Retention**: eviction works on state timestamp boundaries and is approximate
  under identical timestamps; what an evaluation *sees* is trimmed exactly
  (`maxDurationSeconds` first, then the newest `maxCount`).
- **Windowing**: state is per key **and window** — with non-global windows the
  buffer resets at window boundaries. Merging windows (sessions) are rejected.
- A failing SQL evaluation still buffers the element (the failure goes to the
  module's error handling as usual).
- The `cache` block is not supported (the buffer changes with every element).
- **Runner support**: requires `OrderedListState` and (batch)
  `@RequiresTimeSortedInput` — supported on Dataflow and the Direct runner;
  check the Beam capability matrix before using Flink/Spark.

## Built-in functions

Module-specific functions, registered for every query:

| function | description |
|----------|-------------|
| `CURRENT_DATE_([timezone])` | Current date, optionally in a time zone (`CURRENT_DATE_('Asia/Tokyo')`). |
| `SEQ_MATCH(rows, fields, pattern, define [, mode])` | Sequence-pattern matching over an array column — the bounded, per-element replacement for `MATCH_RECOGNIZE`. Returns `ARRAY<ROW(matchNo INT, startIdx INT, endIdx INT)>` with **1-based** indexes; expand with `UNNEST` (one row per match) and read matched values via array indexing. |
| `SEQ_MATCH_STEPS(rows, fields, pattern, define [, mode])` | Like `SEQ_MATCH` but one row per **matched element**: `ARRAY<ROW(matchNo INT, idx INT, symbol VARCHAR)>` — exposes which pattern symbol each element matched, so `WHERE s.symbol = 'BUY'` aggregates exactly the matched purchases without re-filtering. |
| `SEQ_FOLD(rows, from, to, field, op)` / `SEQ_FOLD_INT(...)` | Fold one field of an array slice into a scalar (`DOUBLE` / `BIGINT`) — range aggregation as a plain expression, usable in `SELECT`/`WHERE`/`HAVING`. `from`/`to` are 1-based inclusive (match indexes fit as-is, clamped); `op` is `sum`/`min`/`max`/`count` (+`avg` on `SEQ_FOLD`); SQL aggregate null semantics. `field` is a ROW field name (resolved at plan time), `''` for scalar arrays, or a 0-based ordinal for untyped arrays. |
| `SEQ_COLLECT(sortKey [, v1 .. v6])` | **Aggregate**: collects `[key, v...]` rows **sorted by the key** — an order-independent sequence builder (unlike `COLLECT`, and unlike ordered `ARRAY_AGG` it also works when the aggregation feeds `SEQ_MATCH` inside expressions). Turns lookup-table history into a sequence: `SEQ_MATCH(SEQ_COLLECT(h.TS, h.ACTION), 'ts,action', ...)` under `GROUP BY`. The result is an opaque collection: consume with `SEQ_MATCH`/`SEQ_MATCH_STEPS`/`SEQ_FOLD` (ordinal field spec); it cannot be `UNNEST`ed or projected. |
| `SEQ_SPLIT(rows, field, gap)` | Split an array into **sessions**: a new session starts when consecutive elements' `field` values differ by more than `gap` (or either is null). Returns one opaque row list per session — expand with `UNNEST ... WITH ORDINALITY AS s(sess, sessionNo)` and match per session (`SEQ_MATCH(s.sess, ...)`; indexes are session-relative). |
| `SEQ_FUNNEL(rows, fields, timeField, window, steps)` | **Sliding-window funnel**: `steps` is a `;`-separated list of positional conditions in the `define` expression language (no symbol names — `"$action = 'view'; $action = 'cart'; $action = 'buy'"`, double-quoted). Scanning in array order (= time order), a step-1 event anchors a window and each following step must match within `window` (millis) of that anchor; returns the **maximum step reached** (`0`..n, `INTEGER`). One row advances a chain by at most one step; a later step-1 event re-anchors; NULL-time rows are skipped. `timeField` resolves against `fields` (`''` = scalar element, digits = 0-based ordinal, e.g. `'0'` for a `SEQ_COLLECT` key). |
| `SEQ_RETENTION(rows, fields, conditions)` | **Cohort retention**: the first condition is the anchor. Returns `ARRAY<BOOLEAN>` where element 1 = "some row matches condition 1" and element i = "element 1 AND some row matches condition i". No time semantics — encode period membership in the conditions. Projectable and 1-based accessible (`ret[2]`). |
| `QUANTILE(value, fraction)` | **Aggregate**: exact quantile with linear interpolation (`PERCENTILE_CONT` semantics), `fraction` in `[0, 1]` (0.5 = median), returns `DOUBLE`. NULL values are skipped; an empty group yields NULL. Holds the group's values in memory (groups are bounded per evaluation). |
| `APPROX_DISTINCT(value)` | **Aggregate**: fixed-memory approximate distinct count (HyperLogLog, 2^12 registers ≈ 4 KB/group, ~1.6% error; small sets are near-exact via linear counting). NULLs ignored. Unlike `COUNT(DISTINCT x)`, memory does not grow with cardinality. (Named `APPROX_DISTINCT` because `APPROX_COUNT_DISTINCT` is a standard operator.) |
| `ARRAY_DIFFERENCE(arr)` / `ARRAY_DIFFERENCE_INT(arr)` | Adjacent differences over a numeric array (`ARRAY<DOUBLE>` / `ARRAY<BIGINT>`): element i = `arr[i] - arr[i-1]`, first element 0 (same length — indexes stay aligned), NULL neighbor → NULL delta. Over a timestamp array this is the **inter-event-interval sequence** — feed it to `SEQ_MATCH` via `$0`. |
| `ARRAY_CUM_SUM(arr)` / `ARRAY_CUM_SUM_INT(arr)` | Running total over a numeric array; NULL elements contribute 0. |
| `ARRAY_COMPACT(arr)` | Removes **consecutive** duplicates (state-transition normalization before pattern matching). Returns `ARRAY<ANY>` — consume with `SEQ_*` / `CARDINALITY` / `UNNEST`, not direct projection. |
| `ARRAY_DISTINCT(arr)` | Removes all duplicates keeping first-occurrence order. Returns `ARRAY<ANY>` (same consumption rule as `ARRAY_COMPACT`). |
| `TIME_BUCKET(ts, size)` | Floor a timestamp to a **fixed-width bucket** (`TIMESTAMP`): the "5 minutes" that `TIMESTAMP_TRUNC` cannot express. `ts` is a TIMESTAMP or epoch-millis number; `size` is millis or a day-time `INTERVAL` literal (`INTERVAL '5' MINUTE`). Combine with `UNIX_MILLIS(...)` for a numeric bucket key. NULL → NULL; non-positive size is an error. |
| `COSINE_SIMILARITY(a, b)` | Cosine similarity of two equal-length numeric arrays (`DOUBLE`) — embedding similarity. NULL when either argument or either norm is zero/NULL. |
| `MATRIX_MULTIPLY(matrix, vector [, columns])` | Matrix-vector product (`ARRAY<DOUBLE>`): project an embedding through a fixed matrix (PCA components, linear-model weights). `matrix` is a nested array (an `ARRAY[ARRAY[...], ...]` literal or a lookup column); with the trailing `columns` argument it is instead a **flat row-major** array split into `columns`-wide rows — the form of `matrix`-typed fields (ONNX tensors, `reshape` outputs), which surface in SQL as flat `ARRAY<DOUBLE>` columns. |
| `MATRIX_SOLVE(matrix, rhs [, columns])` | Solve `A x = b` (`ARRAY<DOUBLE>`) by SVD: exact for full-rank square systems, least-squares when overdetermined, minimum-norm when rank-deficient. Same nested/flat matrix forms as `MATRIX_MULTIPLY`. |
| `MAHALANOBIS(x, mean, precision [, columns])` | Mahalanobis distance (`DOUBLE`) of vector `x` from `mean` under a **precision** (inverse covariance) matrix — an anomaly score against a distribution fitted upstream (e.g. delivered via a lookup source). Same nested/flat matrix forms as `MATRIX_MULTIPLY`. |
| `POLY_FIT(xs, ys, degree)` | **Least-squares polynomial fit** (`ARRAY<DOUBLE>`): coefficients in ascending order over two equal-length arrays. Degree 1 fits a line — `POLY_FIT(...)[2]` is the slope (SQL arrays are 1-based). |
| `LINEAR_REG(y, xs)` | **Aggregate**: multivariate linear regression of `y` on the feature vector `xs` (`ARRAY[x1, x2, ...]`, same length every row); an intercept term is prepended automatically, so the coefficients are `[b0, b1, ..., bk]`. Accumulates the Gram matrix (constant memory per group) and solves at result time. Rows with NULL `y`/`xs` are skipped. The raw result is an opaque collection — wrap in `AS_DOUBLE_ARRAY` to project it. |
| `AS_DOUBLE_ARRAY(v)` | Re-types an opaque numeric collection (a `LINEAR_REG` result, an `ARRAY_COMPACT`ed numeric array) into a projectable `ARRAY<DOUBLE>`. |

For the value of one column at the row where another column is maximal/minimal ("the latest status per key"), use the **standard** `ARG_MAX(value, key)` / `ARG_MIN(value, key)` aggregates — they run natively, return the value's own type and ignore NULL keys.

`SEQ_MATCH` / `SEQ_MATCH_STEPS` arguments:

- `rows` — an `ARRAY<ROW>` (or array of scalars) column; array order is the sequence order (build with `SEQ_COLLECT` if it isn't).
- `fields` — the row field names in row order (`'seq,amount'`), used by `$name` references (`$0`, `$1`, … work without it; for scalar arrays `$0` is the element itself).
- `pattern` — a regex over symbols: sequence, alternation `|`, grouping `( )`, quantifiers `? * + {n} {n,} {n,m}`. Example: `'STRT UP{2,}'`.
- `define` — per-symbol predicates, `;`-separated: `$field` refs, `PREV($f [, n])`, literals, `+ - * /`, comparisons, `AND OR NOT`, parentheses. A symbol without a definition always matches; comparisons with null are false. Example: `'UP: $amount > PREV($amount)'`. Tip: when a predicate contains string literals, write the define as a **double-quoted** SQL string (`"A: $action = 'promo'"`) — the module's BigQuery-style lexer does not use `''` escaping.
- `mode` (optional) — how matches are selected: `'longest'` (default — longest per start, non-overlapping), `'shortest'` (pins the first occurrence), `'all'` (every distinct range), `'overlap'` (longest per start, overlaps allowed — every trigger evaluates).

Under the default mode, matches are non-overlapping and longest-per-start (MATCH_RECOGNIZE defaults: after a match, scanning resumes past its last row). No match (or a null/empty array) yields an empty array, so `UNNEST` drops the element.

```sql
-- Intervals where the amount rose at least twice in a row, with their values
SELECT
  i.userId,
  m.matchNo,
  i.events[m.startIdx].amount AS fromAmount,
  i.events[m.endIdx].amount   AS toAmount
FROM INPUT AS i,
UNNEST(SEQ_MATCH(
  i.events, 'seq,amount',
  'STRT UP{2,}',
  'UP: $amount > PREV($amount)'
)) AS m
```

## Standard SQL function reference

The SQL surface is Apache Calcite's **standard operator set plus the BigQuery
function library** (matching the module's BigQuery-style lexical rules). The
tables below list the functions available to queries, by category (generated
from the registered operator table). Most have full runtime support; a few
niche ones may be rejected at planning time — the error names the function.
Timestamps compare at millisecond precision inside queries.

### Aggregate functions (usable in `GROUP BY` queries and as `OVER` windows)

| | functions |
|---|---|
| basic | `COUNT` `COUNTIF` `SUM` `AVG` `MIN` `MAX` `ANY_VALUE` `MODE` `ARG_MAX` `ARG_MIN` `APPROX_COUNT_DISTINCT` |
| logical | `EVERY` `SOME` `LOGICAL_AND` `LOGICAL_OR` `BIT_AND` `BIT_OR` `BIT_XOR` |
| statistical | `STDDEV` `STDDEV_POP` `STDDEV_SAMP` `VARIANCE` `VAR_POP` `VAR_SAMP` `COVAR_POP` `COVAR_SAMP` `REGR_COUNT` `REGR_SXX` `REGR_SYY` `PERCENTILE_CONT` `PERCENTILE_DISC` |
| collecting | `ARRAY_AGG` `ARRAY_CONCAT_AGG` `STRING_AGG` `LISTAGG` `COLLECT` `JSON_ARRAYAGG` `JSON_OBJECTAGG` — note: `ARRAY_AGG(x ORDER BY y)` works in outer `GROUP BY` queries but not *inside* a LATERAL block (see above) |

### Window-only functions (require `OVER`)

`ROW_NUMBER` `RANK` `DENSE_RANK` `PERCENT_RANK` `CUME_DIST` `NTILE` `LEAD` `LAG` `FIRST_VALUE` `LAST_VALUE` `NTH_VALUE`

### String

`ASCII` `CHR` `CHAR_LENGTH` `CHARACTER_LENGTH` `CONCAT` (and `||`) `CONTAINS_SUBSTR` `ENDS_WITH` `STARTS_WITH` `INITCAP` `INSTR` `LEFT` `RIGHT` `LENGTH` `LOWER` `UPPER` `LPAD` `RPAD` `LTRIM` `RTRIM` `TRIM` `OCTET_LENGTH` `OVERLAY` `POSITION` `STRPOS` `REPEAT` `REPLACE` `REVERSE` `SPLIT` `SUBSTR` `SUBSTRING` `TRANSLATE` `SOUNDEX` — regex: `REGEXP_CONTAINS` `REGEXP_EXTRACT` `REGEXP_EXTRACT_ALL` `REGEXP_INSTR` `REGEXP_REPLACE` `REGEXP_SUBSTR` `LIKE` `SIMILAR TO` — encoding/hash: `MD5` `SHA1` `SHA256` `SHA512` `TO_BASE32` `FROM_BASE32` `FROM_BASE64` `TO_HEX` `FROM_HEX` `TO_CODE_POINTS` `CODE_POINTS_TO_BYTES` `CODE_POINTS_TO_STRING`

### Numeric

`ABS` `CEIL` `FLOOR` `ROUND` `TRUNC` `TRUNCATE` `MOD` `SIGN` `EXP` `LN` `LOG` `LOG10` `POW` `POWER` `SQRT` `CBRT` `PI` `RAND` `RAND_INTEGER` `DEGREES` `RADIANS` `IS_INF` `IS_NAN` — trigonometric: `SIN` `COS` `TAN` `COT` `SEC` `CSC` `ASIN` `ACOS` `ATAN` `ATAN2` and hyperbolic variants (`SINH` `COSH` `TANH` `COTH` `SECH` `CSCH` `ASINH` `ACOSH` `ATANH`) — overflow-safe: `SAFE_ADD` `SAFE_SUBTRACT` `SAFE_MULTIPLY` `SAFE_DIVIDE` `SAFE_NEGATE` — bitwise: `BITAND` `BITOR` `BITXOR` `BITNOT` `BIT_COUNT`

### Date / time

`CURRENT_DATE` `CURRENT_TIME` `CURRENT_DATETIME` `CURRENT_TIMESTAMP` `LOCALTIME` `LOCALTIMESTAMP` (see also the built-in `CURRENT_DATE_` with a time-zone argument) — constructors: `DATE` `TIME` `DATETIME` `TIMESTAMP` `DATE_FROM_UNIX_DATE` `TIMESTAMP_SECONDS` `TIMESTAMP_MILLIS` `TIMESTAMP_MICROS` — arithmetic: `DATE_ADD` `DATE_SUB` `DATE_DIFF` `DATE_TRUNC` `TIME_ADD` `TIME_SUB` `TIME_DIFF` `TIME_TRUNC` `DATETIME_ADD` `DATETIME_SUB` `DATETIME_DIFF` `DATETIME_TRUNC` `TIMESTAMP_ADD` `TIMESTAMP_SUB` `TIMESTAMP_DIFF` `TIMESTAMP_TRUNC` `TIMESTAMPADD` `TIMESTAMPDIFF` `LAST_DAY` — parts: `EXTRACT` `YEAR` `QUARTER` `MONTH` `WEEK` `DAYOFMONTH` `DAYOFWEEK` `DAYOFYEAR` `HOUR` `MINUTE` `SECOND` — format/parse: `FORMAT_DATE` `FORMAT_TIME` `FORMAT_DATETIME` `FORMAT_TIMESTAMP` `PARSE_DATE` `PARSE_TIME` `PARSE_DATETIME` `PARSE_TIMESTAMP` — epoch: `UNIX_DATE` `UNIX_SECONDS` `UNIX_MILLIS` `UNIX_MICROS`

### Conditional / comparison

`CASE` `COALESCE` `IF` `IFNULL` `NULLIF` `GREATEST` `LEAST` `BETWEEN` `IN` `EXISTS` `IS [NOT] NULL` `IS [NOT] TRUE/FALSE/UNKNOWN` `IS [NOT] DISTINCT FROM`

### Array / complex types

`ARRAY[...]` constructor, `arr[i]` element access (**1-based**), `ARRAY_CONCAT` `ARRAY_LENGTH` `ARRAY_REVERSE` `ARRAY_TO_STRING` `CARDINALITY` `ELEMENT` `UNNEST` (+ `WITH ORDINALITY`), `ROW` constructor, `MAP` — plus the built-ins (above): the `SEQ_*` family (`SEQ_MATCH` `SEQ_MATCH_STEPS` `SEQ_FOLD` `SEQ_FOLD_INT` `SEQ_COLLECT` `SEQ_SPLIT` `SEQ_FUNNEL` `SEQ_RETENTION`), the array transforms (`ARRAY_DIFFERENCE(_INT)` `ARRAY_CUM_SUM(_INT)` `ARRAY_COMPACT` `ARRAY_DISTINCT`), and the aggregates `QUANTILE` / `APPROX_DISTINCT` and `TIME_BUCKET`

### Type conversion & JSON

`CAST` `SAFE_CAST` `TYPEOF` `CONVERT` — JSON: `JSON_VALUE` `JSON_QUERY` `JSON_EXISTS` `JSON_OBJECT` `JSON_ARRAY` `JSON_TYPE` `JSON_DEPTH` `JSON_KEYS` `JSON_LENGTH` `JSON_PRETTY` `JSON_INSERT` `JSON_REPLACE` `JSON_REMOVE` `JSON_SET` `JSON_STORAGE_SIZE` `IS [NOT] JSON [VALUE|OBJECT|ARRAY|SCALAR]`

### Not available

- `MATCH_RECOGNIZE` and its navigation functions (`PREV` `NEXT` `FIRST` `LAST` `CLASSIFIER` `MATCH_NUMBER`) — use `SEQ_MATCH` instead.
- Streaming group-window functions (`TUMBLE` `HOP` `SESSION`) — windowing belongs to the surrounding pipeline, not to this per-element SQL.
- User-defined functions can be added when using `Query2` directly (Java API: `withScalarFunction` / `withAggregateFunction`); the `query` module config does not currently expose UDF registration.

## Examples

### Aggregate over an array field per element

Each element carries an `events` array; the query folds it into one row per element.
Elements whose `events` array is empty produce no output row.

```yaml
transforms:
  - name: eventStats
    module: query
    inputs: [users]
    parameters:
      sql: |
        SELECT
          userId,
          COUNT(*) AS cnt,
          SUM(e.amount) AS total
        FROM
          INPUT, UNNEST(events) AS e
        GROUP BY
          userId
```

### Keep the element even when the array is empty (LEFT JOIN LATERAL)

Postgres-style `LEFT JOIN LATERAL ... ON TRUE` keeps the input row and fills the folded
columns with defaults when the per-element collection is empty.

```yaml
transforms:
  - name: eventStats
    module: query
    inputs: [users]
    parameters:
      sql: |
        SELECT
          t.userId,
          COALESCE(s.total, 0) AS total
        FROM
          INPUT AS t
        LEFT JOIN LATERAL (
          SELECT SUM(e.amount) AS total FROM UNNEST(t.events) AS e
        ) AS s ON TRUE
```

### Fan-out with per-element top-N

`ORDER BY` / `LIMIT` apply within each element's collection — top-N per element without any shuffle.

```yaml
transforms:
  - name: topEvents
    module: query
    inputs: [users]
    parameters:
      table: users
      sql: |
        SELECT
          userId,
          e.category AS category,
          e.amount AS amount
        FROM
          users, UNNEST(events) AS e
        ORDER BY e.amount DESC
        LIMIT 3
```

### Scalar per-element computation and filtering

Without arrays, the module works as a per-element SQL `SELECT`/`WHERE`.

```yaml
transforms:
  - name: scale
    module: query
    inputs: [numbers]
    parameters:
      sql: "SELECT `value` AS id, `value` * 10 AS scaled FROM INPUT WHERE `value` > 1"
```

### Enrich from a database (JDBC lookup-join)

Each element's `itemId` is looked up against the `ITEMS` table by primary key.
Keys are batched and deduplicated; the table is never scanned. `LEFT JOIN` keeps
elements whose key has no match (columns become null).

```yaml
transforms:
  - name: enrich
    module: query
    inputs: [carts]
    parameters:
      sql: |
        SELECT
          i.itemId AS itemId,
          m.TITLE AS title,
          i.qty * m.PRICE AS total
        FROM
          INPUT AS i
        LEFT JOIN db.ITEMS AS m ON m.ITEM_ID = i.itemId
      sources:
        - name: db
          type: jdbc
          driver: org.postgresql.Driver
          url: jdbc:postgresql://localhost:5432/shop
          user: app
          password: secret
          tables:
            - name: ITEMS
```

### Per-key aggregation with a correlated LATERAL block

The correlated conditions become the key-driven fetch (prefix equality + a range
bounded by input columns); the aggregation runs over each key's fetched rows
inside the DoFn. `ORDER BY`/`LIMIT` in the block would give per-input top-N the
same way.

```yaml
transforms:
  - name: stats
    module: query
    inputs: [users]
    parameters:
      sql: |
        SELECT
          i.userId AS userId,
          s.purchases AS purchases,
          s.total AS total
        FROM
          INPUT AS i
        JOIN LATERAL (
          SELECT COUNT(*) AS purchases, SUM(h.amount) AS total
          FROM db.UserHistory AS h
          WHERE h.UserId = i.userId
            AND h.PurchasedAt >= i.since AND h.PurchasedAt <= i.until
        ) AS s ON TRUE
      sources:
        - name: db
          type: jdbc
          driver: org.postgresql.Driver
          url: jdbc:postgresql://localhost:5432/shop
          tables:
            - name: UserHistory
```

### Fan-out from Spanner history rows and fold per element

Prefix-only key equality (leading primary-key column) fans out to every row under
the prefix; a range on the next key column bounds it; `GROUP BY` folds the fetched
collection back to one row per element — all inside the DoFn.

```yaml
transforms:
  - name: history
    module: query
    inputs: [users]
    parameters:
      sql: |
        SELECT
          i.userId AS userId,
          COUNT(*) AS purchases,
          SUM(h.amount) AS total
        FROM
          INPUT AS i
        JOIN sp.UserHistory AS h
          ON h.UserId = i.userId
          AND h.PurchasedAt >= i.since AND h.PurchasedAt <= i.until
        GROUP BY i.userId
      sources:
        - name: sp
          type: spanner
          projectId: my-project
          instanceId: my-instance
          databaseId: my-database
          tables:
            - name: UserHistory
```

### Spanner Graph traversal as a lookup (query table, array bind)

The GQL traversal pattern is adapter configuration; the caller only joins on the
start key. The batch's distinct start keys are bound to `@keys` and the traversal
runs once per batch.

```yaml
transforms:
  - name: related
    module: query
    inputs: [events]
    parameters:
      sql: |
        SELECT
          e.userId AS userId,
          COALESCE(g.relatedCount, 0) AS relatedCount
        FROM
          INPUT AS e
        LEFT JOIN graph.relatedPeople AS g ON g.userId = e.userId
      sources:
        - name: graph
          type: spanner
          projectId: my-project
          instanceId: my-instance
          databaseId: my-database
          tables:
            - name: relatedPeople
              keyField: userId
              sql: >
                GRAPH SocialGraph
                MATCH (a:Users)-[:Interacted]-(b:Users)
                WHERE a.id IN UNNEST(@keys)
                RETURN a.id AS userId, COUNT(DISTINCT b.id) AS relatedCount
                GROUP BY userId
```

### Spanner full-text search with per-term top-N (query table, per-key bind)

`SEARCH(tokenlist, @query)` takes a single query string, so the statement runs
once per distinct search term (`bindMode: per_key`); the term is returned as
`@query AS queryKey` so hits join back. One term fans out to all hits; the
LATERAL block keeps the best one per input row.

```yaml
transforms:
  - name: bestHit
    module: query
    inputs: [queries]
    parameters:
      sql: |
        SELECT
          i.term AS term,
          s.docId AS docId,
          s.title AS title
        FROM
          INPUT AS i
        JOIN LATERAL (
          SELECT d.docId, d.title
          FROM fts.docs AS d
          WHERE d.queryKey = i.term
          ORDER BY d.score DESC
          LIMIT 1
        ) AS s ON TRUE
      sources:
        - name: fts
          type: spanner
          projectId: my-project
          instanceId: my-instance
          databaseId: my-database
          tables:
            - name: docs
              keyField: queryKey
              paramName: query
              bindMode: per_key
              sql: >
                SELECT @query AS queryKey, docId, title,
                       SCORE(title_tokens, @query) AS score
                FROM Documents WHERE SEARCH(title_tokens, @query)
              fields:
                - { name: queryKey, type: string }
                - { name: docId, type: int64 }
                - { name: title, type: string }
                - { name: score, type: float64 }
```

### Bigtable row-key lookup with a caller-built composite key

```yaml
transforms:
  - name: attrs
    module: query
    inputs: [events]
    parameters:
      sql: |
        SELECT
          i.userId AS userId,
          b.plan AS plan,
          b.score AS score
        FROM
          INPUT AS i
        JOIN bt.user_attrs AS b ON b.rowKey = i.userId
      sources:
        - name: bt
          type: bigtable
          projectId: my-project
          instanceId: my-instance
          tables:
            - name: user_attrs
              tableId: user-attrs
              rowKeyField: rowKey
              rowKeyType: string
              columns:
                - { name: plan, family: cf, type: string }
                - { name: score, family: cf, type: float64 }
```

### REST API lookup with response-array fan-out

The `{userId}` placeholder is the key column; one GET per distinct key. `rowsFrom`
points at the response array, so one key may yield many rows.

```yaml
transforms:
  - name: orders
    module: query
    inputs: [users]
    parameters:
      sql: |
        SELECT
          i.userId AS userId,
          o.orderId AS orderId,
          o.amount AS amount
        FROM
          INPUT AS i
        JOIN api.orders AS o ON o.userId = i.userId
      sources:
        - name: api
          type: rest
          baseUrl: https://internal.example.com
          allowedHosts: [internal.example.com]
          headers:
            Authorization: Bearer xxxx
          tables:
            - name: orders
              endpoint: /v1/orders
              params:
                userId: "{userId}"
              rowsFrom: /items
              fields:
                - { name: orderId, type: string }
                - { name: amount, type: int64 }
```

### Enrich from another pipeline collection (side input lookup-join)

Joins the stream against a collection produced elsewhere in the same pipeline —
no external store. `members` is read once per window as a Beam side input and
hash-indexed by `userId` on each worker; the composite-key / prefix / range forms
of the contract work the same way as for database sources.

```yaml
sources:
  - name: members
    module: bigquery
    parameters:
      query: "SELECT userId, userName, grade FROM `myproject.mydataset.members`"

transforms:
  - name: enriched
    module: query
    inputs: [orders]
    sideInputs: [members]
    parameters:
      sql: |
        SELECT
          o.orderId AS orderId,
          o.amount AS amount,
          m.userName AS userName,
          m.grade AS grade
        FROM
          INPUT AS o
        LEFT JOIN side.members AS m ON m.userId = o.userId
      sources:
        - name: side
          type: sideinput
          tables:
            - name: members
              keyFields: [userId]
```
