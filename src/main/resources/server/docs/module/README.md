# Built-in modules

This page lists the modules registered in the current codebase.

> **Note**: The self-contained per-module reference (parameters and examples) lives in this directory
> together with the module catalog [`index.yaml`](index.yaml). These files are bundled on the classpath
> and read by the Pipeline server's AI agent, MCP server, and the Pipeline Builder UI.
>
> The registered module names are defined by the `@Source.Module` / `@Transform.Module` / `@Sink.Module`
> annotations. If this list drifts from the code, regenerate it with:
> `grep -rhoE '@(Source|Transform|Sink|FailureSink)\.Module\([^)]*\)' src/main/java | sort -u`

## Source Modules

| module                                                                          | description                                                                                            |
|----------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------|
| [bigquery](source/bigquery.md)   | Import data from BigQuery with a specified query or table                                              |
| [spanner](source/spanner.md)     | Import data from Cloud Spanner via query or table scan (also change streams, view and microbatch mode) |
| [bigtable](source/bigtable.md)   | Import rows from Cloud Bigtable with key range/prefix and column filters                               |
| [datastore](source/datastore.md) | Import entities from Cloud Datastore with a specified GQL query                                        |
| [firestore](source/firestore.md) | Import documents from Cloud Firestore with a specified filter condition                                |
| [iceberg](source/iceberg.md)                                                     | Import data from Apache Iceberg tables (experimental, not yet functional)                              |
| [jdbc](source/jdbc.md)           | Import data from RDB using JDBC connector with a specified query                                       |
| [postgres](source/postgres.md)   | Import data from PostgreSQL in parallel using COPY BINARY format                                       |
| [tidb](source/tidb.md)           | Import data from TiDB in parallel using TiKV region based split                                        |
| [storage](source/storage.md)     | Import and parse file contents (Avro/Parquet/CSV/JSON) from GCS, S3, or local file systems             |
| [files](source/files.md)         | Import file metadata (and optionally content) matched by glob patterns from GCS, S3, or local files    |
| [drive](source/drive.md)         | Import file metadata and content from Google Drive                                                     |
| [http](source/http.md)           | Send HTTP requests and output the responses (pagination, chaining, retry)                              |
| [pubsub](source/pubsub.md)       | Import messages from Cloud Pub/Sub topics or subscriptions                                             |
| [kafka](source/kafka.md)                                                         | Import data from Apache Kafka topics                                                                   |
| [create](source/create.md)       | Generate data with specified conditions (explicit elements or sequences)                               |
| [request](source/request.md)     | Turn the HTTP request body into source data in serve mode (Cloud Run Service)                          |

## Transform Modules

| module                                                                                | description                                                                                                             |
|-----------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------|
| [select](transform/select.md)           | Filter rows and transform field values with a rich set of select functions                                              |
| [aggregation](transform/aggregation.md) | Perform aggregation with grouping, filtering, and field selection in both batch and streaming                           |
| [beamsql](transform/beamsql.md)         | Process and combine input data using SQL queries based on Apache Beam SQL (Calcite)                                     |
| [query](transform/query.md)             | Run a Calcite SQL query over each input element inside a DoFn (no shuffle), with lookup joins to external sources       |
| [partition](transform/partition.md)     | Split input data into multiple named outputs based on filter conditions                                                 |
| [compare](transform/compare.md)                                                          | Compare records across multiple inputs by primary key and output differences                                            |
| [reshuffle](transform/reshuffle.md)     | Insert a reshuffle stage to prevent fusion optimizations and enable checkpointing                                       |
| [onnx](transform/onnx.md)                                                               | Make inferences using the specified [ONNX](https://onnxruntime.ai/) model file                                          |
| [onnx_gen](transform/onnx_gen.md)                                                        | Run generative inference (prompt-based) using ONNX generative models (experimental)                                     |
| [pdfextract](transform/pdfextract.md)                                                   | Extract text and metadata from PDF files                                                                                |

## Sink Modules

| module                                                                        | description                                                                                  |
|----------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------|
| [bigquery](sink/bigquery.md)     | Write input data to BigQuery tables                                                              |
| [spanner](sink/spanner.md)       | Write input data to Cloud Spanner tables using mutations                                         |
| [bigtable](sink/bigtable.md)     | Write, update, or delete cells and rows in Cloud Bigtable                                        |
| [datastore](sink/datastore.md)   | Write or delete entities in Cloud Datastore                                                      |
| [firestore](sink/firestore.md)   | Write or delete documents in Cloud Firestore                                                     |
| [iceberg](sink/iceberg.md)                                                       | Write input data to Apache Iceberg tables (experimental, not yet functional)                     |
| [jdbc](sink/jdbc.md)             | Write data to RDB tables using JDBC statements                                                   |
| [pubsub](sink/pubsub.md)         | Publish input data as messages to Cloud Pub/Sub topics                                           |
| [storage](sink/storage.md)       | Write input data as files (Avro/Parquet/JSON/CSV) to GCS, S3, or local file systems              |
| [files](sink/files.md)           | Write each input record as an individual file with template-driven path and content              |
| [debug](sink/debug.md)           | Output input data to logs for debugging and inspection                                           |
| [auxia](sink/auxia.md)           | Send input data as events to the Auxia platform via its ingestion API                            |
| [tasks](sink/tasks.md)                                                           | Enqueue each record as a Cloud Tasks HTTP task (rate limit, retry, schedule, dedup by the queue)  |
| [localH2](sink/localh2.md)                                                       | Load input records into a local H2 database and write the database file out                      |

## Action Modules

Action modules (`action.<service>`) execute an operation against an external service at a point in the pipeline (run a job, write a result history) — lightweight workflow steps. They are placeable in any of the `sources` / `transforms` / `sinks` sections; see the [action modules overview](action/README.md) for placement, trigger semantics (`once` / `perElement` / `collect`) and the common output envelope.

| module                                                | description                                                                                       |
|-------------------------------------------------------|----------------------------------------------------------------------------------------------------|
| [action.bigquery](action/bigquery.md)                 | Run a BigQuery job (query or load) and wait for it, with idempotent deterministic job ids           |
| [action.vertexai_gemini](action/vertexai_gemini.md)   | Launch a Vertex AI Gemini batch prediction job and wait for it                                      |
| [action.storage](action/storage.md)                   | Write a small file from the triggering records (result histories, summary reports, marker files)    |
| [action.tasks](action/tasks.md)                       | Cloud Tasks queue operations (create/update/pause/resume/purge/delete, waitForEmpty, run/delete task) |

## Failure Modules

Used in the top-level `failures` block and in each module's `failureSinks` to route failed records (dead-letter).

| module                        | description                                                    |
|-------------------------------|-----------------------------------------------------------------|
| bigquery                      | Write failure records to a BigQuery table                       |
| [pubsub](failure/pubsub.md)   | Publish failure record messages to a specified Cloud Pub/Sub topic |
| storage                       | Write failure records to Cloud Storage                          |
