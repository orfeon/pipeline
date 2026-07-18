# Built-in modules

This page lists the modules registered in the current codebase.

> **Note**: The canonical, self-contained per-module reference (parameters and examples) lives in
> [`src/main/resources/server/docs/module/`](../../../src/main/resources/server/docs/module/) together with the
> module catalog [`index.yaml`](../../../src/main/resources/server/docs/module/index.yaml). Those files are bundled
> on the classpath and read by the Pipeline server's AI agent, MCP server, and the Pipeline Builder UI.
> Module names below link to the canonical doc when available, otherwise to the legacy doc in this directory.
>
> The registered module names are defined by the `@Source.Module` / `@Transform.Module` / `@Sink.Module`
> annotations. If this list drifts from the code, regenerate it with:
> `grep -rhoE '@(Source|Transform|Sink|FailureSink)\.Module\([^)]*\)' src/main/java | sort -u`

## Source Modules

| module                                                                          | description                                                                                            |
|----------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------|
| [bigquery](../../../src/main/resources/server/docs/module/source/bigquery.md)   | Import data from BigQuery with a specified query or table                                              |
| [spanner](../../../src/main/resources/server/docs/module/source/spanner.md)     | Import data from Cloud Spanner via query or table scan (also change streams, view and microbatch mode) |
| [bigtable](../../../src/main/resources/server/docs/module/source/bigtable.md)   | Import rows from Cloud Bigtable with key range/prefix and column filters                               |
| [datastore](../../../src/main/resources/server/docs/module/source/datastore.md) | Import entities from Cloud Datastore with a specified GQL query                                        |
| [firestore](../../../src/main/resources/server/docs/module/source/firestore.md) | Import documents from Cloud Firestore with a specified filter condition                                |
| iceberg                                                                          | Import data from Apache Iceberg tables                                                                 |
| [jdbc](../../../src/main/resources/server/docs/module/source/jdbc.md)           | Import data from RDB using JDBC connector with a specified query                                       |
| [postgres](../../../src/main/resources/server/docs/module/source/postgres.md)   | Import data from PostgreSQL in parallel using COPY BINARY format                                       |
| [tidb](../../../src/main/resources/server/docs/module/source/tidb.md)           | Import data from TiDB in parallel using TiKV region based split                                        |
| [storage](../../../src/main/resources/server/docs/module/source/storage.md)     | Import and parse file contents (Avro/Parquet/CSV/JSON) from GCS, S3, or local file systems             |
| [files](../../../src/main/resources/server/docs/module/source/files.md)         | Import file metadata (and optionally content) matched by glob patterns from GCS, S3, or local files    |
| [drive](../../../src/main/resources/server/docs/module/source/drive.md)         | Import file metadata and content from Google Drive                                                     |
| [http](../../../src/main/resources/server/docs/module/source/http.md)           | Send HTTP requests and output the responses (pagination, chaining, retry)                              |
| [pubsub](../../../src/main/resources/server/docs/module/source/pubsub.md)       | Import messages from Cloud Pub/Sub topics or subscriptions                                             |
| kafka                                                                            | Import data from Apache Kafka topics                                                                   |
| [create](../../../src/main/resources/server/docs/module/source/create.md)       | Generate data with specified conditions (explicit elements or sequences)                               |

## Transform Modules

| module                                                                                | description                                                                                                             |
|-----------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------|
| [select](../../../src/main/resources/server/docs/module/transform/select.md)           | Filter rows and transform field values with a rich set of select functions                                              |
| [aggregation](../../../src/main/resources/server/docs/module/transform/aggregation.md) | Perform aggregation with grouping, filtering, and field selection in both batch and streaming                           |
| [beamsql](../../../src/main/resources/server/docs/module/transform/beamsql.md)         | Process and combine input data using SQL queries based on Apache Beam SQL (Calcite)                                     |
| [query](../../../src/main/resources/server/docs/module/transform/query.md)             | Run a Calcite SQL query over each input element inside a DoFn (no shuffle), with lookup joins to external sources       |
| [partition](../../../src/main/resources/server/docs/module/transform/partition.md)     | Split input data into multiple named outputs based on filter conditions                                                 |
| compare                                                                                  | Compare records across multiple inputs by primary key and output differences                                            |
| [reshuffle](../../../src/main/resources/server/docs/module/transform/reshuffle.md)     | Insert a reshuffle stage to prevent fusion optimizations and enable checkpointing                                       |
| [onnx](transform/onnx.md)                                                               | Make inferences using the specified [ONNX](https://onnxruntime.ai/) model file                                          |
| onnx_gen                                                                                 | Run generative inference (prompt-based) using ONNX generative models                                                    |
| [pdfextract](transform/pdfextract.md)                                                   | Extract text and metadata from PDF files                                                                                |

## Sink Modules

| module                                                                        | description                                                                                  |
|----------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------|
| [bigquery](../../../src/main/resources/server/docs/module/sink/bigquery.md)     | Write input data to BigQuery tables                                                              |
| [spanner](../../../src/main/resources/server/docs/module/sink/spanner.md)       | Write input data to Cloud Spanner tables using mutations                                         |
| [bigtable](../../../src/main/resources/server/docs/module/sink/bigtable.md)     | Write, update, or delete cells and rows in Cloud Bigtable                                        |
| [datastore](../../../src/main/resources/server/docs/module/sink/datastore.md)   | Write or delete entities in Cloud Datastore                                                      |
| [firestore](../../../src/main/resources/server/docs/module/sink/firestore.md)   | Write or delete documents in Cloud Firestore                                                     |
| iceberg                                                                          | Write input data to Apache Iceberg tables                                                        |
| [jdbc](../../../src/main/resources/server/docs/module/sink/jdbc.md)             | Write data to RDB tables using JDBC statements                                                   |
| [pubsub](../../../src/main/resources/server/docs/module/sink/pubsub.md)         | Publish input data as messages to Cloud Pub/Sub topics                                           |
| [storage](../../../src/main/resources/server/docs/module/sink/storage.md)       | Write input data as files (Avro/Parquet/JSON/CSV) to GCS, S3, or local file systems              |
| [files](../../../src/main/resources/server/docs/module/sink/files.md)           | Write each input record as an individual file with template-driven path and content              |
| [debug](../../../src/main/resources/server/docs/module/sink/debug.md)           | Output input data to logs for debugging and inspection                                           |
| action                                                                           | Execute actions on external services (Dataflow, BigQuery, Vertex AI Gemini)                      |
| [auxia](../../../src/main/resources/server/docs/module/sink/auxia.md)           | Send input data as events to the Auxia platform via its ingestion API                            |
| tasks                                                                            | Send input records as tasks to a Cloud Tasks queue                                               |
| localH2                                                                          | Load input records into a local H2 database and write the database file out                      |

## Failure Modules

Used in the top-level `failures` block and in each module's `failureSinks` to route failed records (dead-letter).

| module                        | description                                                    |
|-------------------------------|-----------------------------------------------------------------|
| bigquery                      | Write failure records to a BigQuery table                       |
| [pubsub](failure/pubsub.md)   | Publish failure record messages to a specified Cloud Pub/Sub topic |
| storage                       | Write failure records to Cloud Storage                          |
