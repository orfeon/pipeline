---
type: Transform Module
title: CDC Transform Module
description: Normalizes provider-specific change data capture records (Spanner change streams, PostgreSQL logical replication, TiCDC canal-json) into a unified change record envelope consumed by apply-capable sinks such as the bigquery sink cdc mode. Emits table-level control records (TRUNCATE, SCHEMA on schema drift), splits primary key changes into DELETE+INSERT, and supports per-key accumulation to collapse a batch of changes into the latest state.
tags: [transform, cdc, changestream, changedatacapture, spanner, postgres, tidb, ticdc, canal, streaming, batch]
timestamp: 2026-08-17T00:00:00Z
---

# CDC Transform Module

Transform Module that normalizes provider-specific change data capture (CDC) records into a single
unified **change record envelope**. Provider formats differ (Spanner change stream records, TiCDC
canal-json events, ...), but after this transform every change is an ordinary record with the same
schema, so it can be:

- applied to a destination by an apply-capable sink (`bigquery` sink with `cdc: true`),
- archived as-is with any file sink (`storage` sink as Avro/Parquet — including Iceberg changelog tables),
- filtered/reshaped by any other transform (`select`, `query`, `partition`, ...).

The same normalization works for live streams and for records replayed from archived files, so an
archive-then-batch-apply pipeline reuses the exact conversion logic of the streaming pipeline.

## Output schema (unified change record)

| field           | type      | description                                                                                                         |
|-----------------|-----------|---------------------------------------------------------------------------------------------------------------------|
| table           | String    | Source table name.                                                                                                  |
| op              | Enum      | Row change: `INSERT`, `UPDATE`, `DELETE`, `SNAPSHOT` (initial snapshot read; applied like INSERT). Control record: `TRUNCATE`, `SCHEMA`, `SNAPSHOT_BEGIN`, `SNAPSHOT_END` (see [Control records](#control-records)). |
| keys            | JSON      | Primary key values of the changed row, as a JSON object — the key **after** the change (the deleted row's key for DELETE). Null on control records. |
| before          | JSON      | Row values before the change (when the provider captures them; otherwise null).                                     |
| after           | JSON      | Row values after the change (null for DELETE). May exclude key columns — the full row is always `keys` ∪ `after`.   |
| commitTimestamp | Timestamp | Commit timestamp of the change.                                                                                     |
| sequence        | String    | Change order within the same table: 1-4 sections of hex digits joined by `/`, compared numerically section by section. Compatible with the BigQuery `_CHANGE_SEQUENCE_NUMBER` pseudocolumn format. Control records share the ordering of the row changes of their table. |
| source          | Record    | Provenance: `provider` (`spanner`, `postgres`, `ticdc`), `database` (nullable), `metadata` (provider-specific values as JSON, nullable). |
| transaction     | Record    | Source transaction (nullable): `id` (provider transaction identifier), `totalRecords` (records in the transaction when the provider reports it, else null), `index` (position within the transaction when known, else null). |
| schema          | JSON      | Row schema on `SCHEMA` records (null otherwise): an array of `{name, type, key}` columns with provider-independent type names (`BOOL INT64 FLOAT32 FLOAT64 NUMERIC STRING BYTES DATE DATETIME TIMESTAMP JSON ARRAY<T>`). |
| statement       | String    | Provider DDL text on `SCHEMA`/`TRUNCATE` records when the provider delivers it (ticdc), else null.                  |

### Primary key changes

An `UPDATE` whose `before` and `after` both carry every key column with different values (a
primary key change — postgres, ticdc) is normalized into two records: a `DELETE` of the old key
and an `INSERT` of the new key, sequenced `<original>/0` and `<original>/1`. Consumers may
therefore assume an `UPDATE` never moves a row. Providers that do not ship the old key image
(postgres `REPLICA IDENTITY NOTHING`) cannot be normalized this way — keep `REPLICA IDENTITY`
at `DEFAULT` or above to replicate key changes.

### Control records

Besides row changes the transform emits table-level control records. They have null `keys` /
`before` / `after`, flow through the same output (so archives and replays keep them in order),
and are skipped by apply-sinks unless configured otherwise (see the
[bigquery sink `onTruncate`](../sink/bigquery.md#cdc-apply-mode)). Filter them out with a
`select` / `partition` filter on `op` when a consumer only wants row changes.

| op               | meaning | spanner | postgres | ticdc |
|------------------|---------|---------|----------|-------|
| `TRUNCATE`       | Every row of the table sequenced before this record is gone. | never (change streams carry no DDL) | `TRUNCATE` events | DDL events whose SQL starts with `TRUNCATE` or `DROP TABLE` (`statement` = the SQL) |
| `SCHEMA`         | The row schema of the table changed; `schema` holds the new schema. | synthesized from `rowType` drift | synthesized from relation column drift | DDL events (`statement` = the SQL, `schema` null) **and** synthesized from `mysqlType` drift on the next row event |
| `SNAPSHOT_BEGIN` / `SNAPSHOT_END` | Reserved for a future source snapshot feature; not emitted. | — | — | — |

**Schema drift detection** (`emitSchemaChanges: true`, the default): the transform remembers the
last row schema seen per table on each worker and emits a `SCHEMA` record — sequenced right
before the change that revealed it — whenever the schema of a new record differs. Consequences
of the worker-local memory: the same change is reported once **per worker** (consumers must be
idempotent — e.g. `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`), and the first record of a table
after a (re)start never reports. With Spanner the detection needs a full row type, i.e. the
change stream's `valueCaptureType` should be `NEW_ROW` or `NEW_ROW_AND_OLD_VALUES`
(`OLD_AND_NEW_VALUES` lists only the modified columns, so a new column is only seen when a row
sets it). PostgreSQL relation columns are carried by the provider-native record (`columnsJson`);
records archived before that field existed produce no `SCHEMA` records on replay.

## CDC transform module parameters

| parameter  | optional | type    | description                                                                                                                                      |
|------------|----------|---------|--------------------------------------------------------------------------------------------------------------------------------------------------|
| format     | required | Enum    | Input format. One of `spanner`, `postgres`, `ticdc`, `envelope` (unified change records themselves, for replay — see [format: `envelope`](#format-envelope)). |
| field      | optional | String  | (`ticdc`) Name of the input field carrying canal-json event text (String or Bytes). Default: a field named `payload` (kafka/pubsub message) or `content` (files source). (`envelope`) Name of the field (may be nested, e.g. `record.json`) carrying envelope JSON text; omit when the input records are envelopes. |
| accumulate | optional | Boolean | Collapse changes per (table, keys) to the single latest change by `sequence` within each window. A `TRUNCATE` record acts as a barrier: row changes of the table sequenced before the latest TRUNCATE are dropped; control records pass through. Intended for batch replay; in streaming it requires a windowing `strategy`. Default: `false`. |
| emitSchemaChanges | optional | Boolean | Emit a `SCHEMA` control record when the row schema of a table drifts (see [Control records](#control-records)). Default: `true`. |
| schemaChanges | optional | Object | Generate destination DDL into the `statement` of `SCHEMA` / `TRUNCATE` records, so an action can apply them (see [Destination DDL generation](#destination-ddl-generation)). |

### schemaChanges parameters

| parameter    | optional | type   | description |
|--------------|----------|--------|-------------|
| dialect      | required | Enum   | DDL dialect of the destination. One of `bigquery`. |
| table        | required | String | Destination table name; a template on the envelope table is resolved per record (e.g. `myproject.mydataset.${table}`). |
| onTypeChange | optional | Enum   | When a column's type changed: `skip` (no DDL for that column, log) or `fail` (route the change to the failure output). Default: `skip`. |
| onDropColumn | optional | Enum   | When a column disappeared: `skip` (the destination column is kept). Default and only value: `skip`. |
| baseline     | optional | Enum   | Comparison baseline for a table first seen on a worker: `destination` (fetch the destination table schema, so a change that happened while the pipeline was down is still reported) or `none` (the first observation never reports). Default: `destination`. Requires read access to the destination table; a missing table falls back to `none`. |

### format: `spanner`

Input must be the provider-native records emitted by the `spanner` source module in
`mode: changeDataCapture` — either directly connected, or read back from files archived by a file
sink (Avro/Parquet via the `storage` source). One input record yields one envelope record per entry
of its `mods` array. `sequence` is composed of the commit timestamp micros, the record sequence and
the mod index. `transaction.id` is the `serverTransactionId`; `source.metadata` carries
`isLastRecordInTransactionInPartition`, `numberOfRecordsInTransaction` and
`numberOfPartitionsInTransaction` for consumers that reassemble transactions.

### format: `postgres`

Input must be the provider-native records emitted by the `postgres` source module in
`mode: changeDataCapture` (logical replication via the pgoutput plugin) — either directly connected,
or read back from archived files. One input record yields one envelope record; `TRUNCATE` events
become a `TRUNCATE` control record. The envelope `table` is the bare
table name for `public`-schema tables and `schema.table` otherwise (matching the postgres source
batch `tables` mode tags). `sequence` is composed of the commit LSN and the change index within the
transaction, so it is totally ordered across the stream. `transaction.id` is the transaction xid and
`transaction.index` the change index within it. `before` is only populated when the source
table has `REPLICA IDENTITY FULL` (otherwise deletes carry key values only).

### format: `ticdc`

Input records carry [canal-json](https://docs.pingcap.com/tidb/stable/ticdc-canal-json) event text in
the field named by `field`. Both TiCDC delivery paths are supported:

- **Kafka**: `kafka` source (`format: message`) — one event per message `payload`.
- **Storage sink (GCS/S3)**: TiCDC syncs newline-delimited canal-json files to object storage; read
  them with the `files` source (`withContent: true`) and the events in each file's `content` are
  split and normalized.

DDL events (`isDdl: true`) become `TRUNCATE` / `SCHEMA` control records; watermark events are
skipped. `sequence` uses the TiDB TSO (`_tidb.commitTs`, when the canal-json extension is enabled)
or the event time `es`, plus the row index within the event; `transaction.id` is that TSO / event
time (canal-json has no transaction identifier).

### format: `envelope`

Input records are unified change records themselves: envelope records read back from an archive
(`storage` source over files written by a `storage` sink fed by this transform), or envelope JSON
text in a field — the `record.json` of the [bigquery sink cdc failure records](../sink/bigquery.md#failure-output)
is such a field. Records are re-validated (`table`, `op`, `sequence`, `commitTimestamp`) and
re-emitted; no schema drift detection or key-change splitting is applied (they happened when the
envelope was first produced). Combine with `accumulate: true` for replay.

## Schema evolution

Row data travels through the envelope as JSON, so a schema change on the source database (e.g.
`ADD COLUMN`) never breaks this transform or the pipeline — new columns simply appear inside the
`after` values, and archived envelope records keep full fidelity across schema versions. The
change itself is reported as a `SCHEMA` control record (see [Control records](#control-records)).
Keeping the *destination* table in sync is an apply-sink concern: see
[Schema evolution in the bigquery sink CDC apply mode](../sink/bigquery.md#schema-evolution) for
the recommended setup (`schemaChanges` DDL → `action.bigquery`, `autoSchemaUpdate`, archive replay).

### Destination DDL generation

With `schemaChanges` configured, the transform also writes **destination DDL** into the
`statement` of control records:

- `SCHEMA` (synthesized from schema drift): one `ALTER TABLE ... ADD COLUMN IF NOT EXISTS ...`
  per added non-key column, mapped to the dialect's types (`DATETIME` stays `DATETIME`,
  `FLOAT32` widens to `FLOAT64`, `STRUCT<...>` becomes `JSON`). Added key columns, type changes
  and dropped columns never produce DDL (logged; `onTypeChange: fail` fails the record instead).
  The statements are idempotent, so the same change reported by several workers, a retry, or a
  replay is harmless.
- `TRUNCATE`: `TRUNCATE TABLE ...`.
- A provider DDL text (ticdc `SCHEMA` records from DDL events) moves to `source.metadata.ddl`,
  so `statement` only ever carries statements of the configured dialect — a `SCHEMA` record
  without column information has a null `statement`.

Route the control records to an action to apply them, or to a notification sink when a person
must approve schema changes (see the bigquery sink example).

## Examples

### Example 1: Spanner change stream to BigQuery CDC upsert (streaming)

```yaml
sources:
  - name: change_stream
    module: spanner
    parameters:
      projectId: myproject
      instanceId: myinstance
      databaseId: mydatabase
      mode: changeDataCapture
      changeStream:
        changeStreamName: MyChangeStream

transforms:
  - name: normalize
    module: cdc
    inputs: [change_stream]
    parameters:
      format: spanner

sinks:
  - name: bq
    module: bigquery
    inputs: [normalize]
    parameters:
      table: myproject.mydataset.${table}   # routes each change to the table of the same name
      cdc: true
```

Every table the template resolves to must already exist on BigQuery (with a primary key). To apply
only some tables, filter on the envelope `table` field upstream (e.g. `select` transform with
`filter`), or use a fixed `table` name for a single-table sink.

### Example 2: Archive a Spanner change stream to GCS, then batch-apply to BigQuery

Archive pipeline (streaming):

```yaml
sources:
  - name: change_stream
    module: spanner
    parameters:
      projectId: myproject
      instanceId: myinstance
      databaseId: mydatabase
      mode: changeDataCapture
      changeStream:
        changeStreamName: MyChangeStream

sinks:
  - name: archive
    module: storage
    inputs: [change_stream]
    parameters:
      output: gs://mybucket/cdc/spanner/
      format: avro
```

Replay pipeline (batch) — the same normalization as the streaming path, collapsed per key:

```yaml
sources:
  - name: archived
    module: storage
    parameters:
      input: gs://mybucket/cdc/spanner/*.avro
      format: avro

transforms:
  - name: normalize
    module: cdc
    inputs: [archived]
    parameters:
      format: spanner
      accumulate: true

  - name: users_only
    module: select
    inputs: [normalize]
    parameters:
      filter:
        key: table
        op: "="
        value: Users

sinks:
  - name: bq
    module: bigquery
    inputs: [users_only]
    parameters:
      table: myproject.mydataset.Users
      cdc: true
      method: STORAGE_WRITE_API
```

### Example 3: TiCDC canal-json from Kafka

```yaml
sources:
  - name: ticdc_events
    module: kafka
    parameters:
      bootstrapServers: broker:9092
      topic: ticdc-mydb
      format: message

transforms:
  - name: normalize
    module: cdc
    inputs: [ticdc_events]
    parameters:
      format: ticdc
```

### Example 4: TiCDC canal-json files synced to GCS

```yaml
sources:
  - name: ticdc_files
    module: files
    parameters:
      pattern: gs://mybucket/ticdc/mydb/mytable/*.json
      withContent: true

transforms:
  - name: normalize
    module: cdc
    inputs: [ticdc_files]
    parameters:
      format: ticdc
      accumulate: true
```
