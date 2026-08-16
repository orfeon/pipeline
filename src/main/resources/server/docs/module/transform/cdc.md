---
type: Transform Module
title: CDC Transform Module
description: Normalizes provider-specific change data capture records (Spanner change streams, PostgreSQL logical replication, TiCDC canal-json) into a unified change record envelope consumed by apply-capable sinks such as the bigquery sink cdc mode. Supports per-key accumulation to collapse a batch of changes into the latest state.
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
| op              | Enum      | Change operation: `INSERT`, `UPDATE`, `DELETE`, `SNAPSHOT` (initial snapshot read; applied like INSERT).            |
| keys            | JSON      | Primary key values of the changed row, as a JSON object.                                                            |
| before          | JSON      | Row values before the change (when the provider captures them; otherwise null).                                     |
| after           | JSON      | Row values after the change (null for DELETE). May exclude key columns — the full row is always `keys` ∪ `after`.   |
| commitTimestamp | Timestamp | Commit timestamp of the change.                                                                                     |
| sequence        | String    | Change order within the same key: 1-4 sections of hex digits joined by `/`, compared numerically section by section. Compatible with the BigQuery `_CHANGE_SEQUENCE_NUMBER` pseudocolumn format. |
| source          | Record    | Provenance: `provider` (`spanner`, `ticdc`, ...), `database` (nullable), `metadata` (provider-specific values as JSON, nullable). |

## CDC transform module parameters

| parameter  | optional | type    | description                                                                                                                                      |
|------------|----------|---------|--------------------------------------------------------------------------------------------------------------------------------------------------|
| format     | required | Enum    | Input provider format. One of `spanner`, `postgres`, `ticdc`.                                                                                    |
| field      | optional | String  | (`ticdc` only) Name of the input field carrying canal-json event text (String or Bytes). Default: a field named `payload` (kafka/pubsub message) or `content` (files source). |
| accumulate | optional | Boolean | Collapse changes per (table, keys) to the single latest change by `sequence` within each window. Intended for batch replay; in streaming it requires a windowing `strategy`. Default: `false`. |

### format: `spanner`

Input must be the provider-native records emitted by the `spanner` source module in
`mode: changeDataCapture` — either directly connected, or read back from files archived by a file
sink (Avro/Parquet via the `storage` source). One input record yields one envelope record per entry
of its `mods` array. `sequence` is composed of the commit timestamp micros, the record sequence and
the mod index.

### format: `postgres`

Input must be the provider-native records emitted by the `postgres` source module in
`mode: changeDataCapture` (logical replication via the pgoutput plugin) — either directly connected,
or read back from archived files. One input record yields one envelope record; `TRUNCATE` records
have no per-row representation in the envelope and are skipped. The envelope `table` is the bare
table name for `public`-schema tables and `schema.table` otherwise (matching the postgres source
batch `tables` mode tags). `sequence` is composed of the commit LSN and the change index within the
transaction, so it is totally ordered across the stream. `before` is only populated when the source
table has `REPLICA IDENTITY FULL` (otherwise deletes carry key values only).

### format: `ticdc`

Input records carry [canal-json](https://docs.pingcap.com/tidb/stable/ticdc-canal-json) event text in
the field named by `field`. Both TiCDC delivery paths are supported:

- **Kafka**: `kafka` source (`format: message`) — one event per message `payload`.
- **Storage sink (GCS/S3)**: TiCDC syncs newline-delimited canal-json files to object storage; read
  them with the `files` source (`withContent: true`) and the events in each file's `content` are
  split and normalized.

DDL events (`isDdl: true` / type `QUERY`) and watermark events are skipped. `sequence` uses the TiDB
TSO (`_tidb.commitTs`, when the canal-json extension is enabled) or the event time `es`, plus the row
index within the event.

## Schema evolution

Row data travels through the envelope as JSON, so a schema change on the source database (e.g.
`ADD COLUMN`) never breaks this transform or the pipeline — new columns simply appear inside the
`after` values, and archived envelope records keep full fidelity across schema versions. Keeping
the *destination* table in sync is an apply-sink concern: see
[Schema evolution in the bigquery sink CDC apply mode](../sink/bigquery.md#schema-evolution) for the
recommended operation (`ignoreUnknownValues` + `ALTER TABLE` + archive replay backfill).

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
