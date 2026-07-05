# Mercari Pipeline

[![test](https://github.com/orfeon/pipeline/actions/workflows/test.yml/badge.svg)](https://github.com/orfeon/pipeline/actions/workflows/test.yml)

The Mercari Pipeline enables you to run various data pipelines without writing programs by simply defining a configuration file.

* **Config-driven**: describe a pipeline as sources → transforms → sinks in a YAML/JSON file — no code required.
* **Rich built-in modules**: BigQuery, Cloud Spanner, Bigtable, Datastore, Firestore, Iceberg, JDBC, Pub/Sub, Kafka, Cloud Storage, HTTP, ONNX inference, per-element SQL and more. See the [module list](docs/config/module/README.md).
* **Portable runners**: the primary target is Cloud Dataflow, and the same configuration file can also run locally (DirectRunner) or on Apache Flink / Apache Spark clusters via Maven profiles.
* **Pipeline API Server**: an auxiliary web UI / REST API / MCP server with a built-in AI agent that helps you create, validate, debug, and deploy pipelines.

> **Note**: Mercari Dataflow Template has been renamed **Mercari Pipeline**. If you are looking for the old name, this is the same project.

## Documentation

* [How to Deploy Pipeline](docs/deploy/README.md) — build container images / bundled jars for each runner
* [How to Define Pipeline](docs/config/README.md) — configuration file reference ([module list](docs/config/module/README.md))
* [How to Execute Pipeline](docs/exec/README.md) — run on Dataflow, locally, or on Flink/Spark
* [Examples](examples/README.md) — ready-to-use configuration files for common use cases
* [Developer docs](docs/developer/README.md) — internals and how to extend modules

## Quick Start

Mercari Pipeline is deployed as a [FlexTemplate](https://cloud.google.com/dataflow/docs/guides/templates/using-flex-templates) for use with the [Cloud Dataflow](https://cloud.google.com/dataflow) runner.
Pipelines are assembled based on the defined configuration file and executed as Cloud Dataflow jobs.

### 1. Deploy the template (first time only)

Requirements: Java 21, [Maven 3](https://maven.apache.org/index.html), and the [gcloud command-line tool](https://cloud.google.com/sdk/gcloud).

```sh
# Build the source code and push the FlexTemplate container image to Artifact Registry
gcloud auth configure-docker {region}-docker.pkg.dev
mvn clean package -DskipTests -Dimage={region}-docker.pkg.dev/{deploy_project}/{template_repo_name}/dataflow:latest

# Generate a template file from the container image and upload it to GCS
gcloud dataflow flex-template build gs://{path/to/template_file} \
  --image "{region}-docker.pkg.dev/{deploy_project}/{template_repo_name}/dataflow:latest" \
  --sdk-language "JAVA"
```

See [How to Deploy Pipeline](docs/deploy/README.md) for details, including images for local execution, the Pipeline API server, and bundled jars for Flink/Spark.

### 2. Write a configuration file

This configuration file stores BigQuery query results in the specified Cloud Spanner table.

```yaml
sources:
  - name: bigquery
    module: bigquery
    parameters:
      query: |-
        SELECT
          *
        FROM
          `myproject.mydataset.mytable`
sinks:
  - name: spanner
    module: spanner
    inputs:
      - bigquery
    parameters:
      projectId: myproject
      instanceId: myinstance
      databaseId: mydatabase
      table: mytable
```

### 3. Run the job

```sh
gcloud dataflow flex-template run bigquery-to-spanner \
  --project={gcp_project} \
  --region={region} \
  --template-file-gcs-location=gs://{path/to/template_file} \
  --parameters=config="$(cat path/to/config.yaml)"
```

The Dataflow job will be started, and you can check the execution status of the job in the console screen.

<img src="docs/images/bigquery-to-spanner.png">

You can also pass the config as a GCS path (`--parameters=config=gs://{path/to/config.yaml}`), run in streaming mode, launch via the REST API, run the pipeline locally with Docker, or submit it to a Flink/Spark cluster.
See [How to Execute Pipeline](docs/exec/README.md) for all execution methods.

## Supported Runners (Maven profiles)

| Maven profile        | Runner         | Build artifact                                                 |
|----------------------|----------------|----------------------------------------------------------------|
| `dataflow` (default) | DataflowRunner | FlexTemplate container image                                   |
| `direct`             | DirectRunner   | Container image for local execution                            |
| `flink`              | FlinkRunner    | Bundled jar (`target/pipeline-bundled-{version}.jar`)          |
| `spark`              | SparkRunner    | Bundled jar (`target/pipeline-bundled-{version}.jar`)          |
| `server`             | —              | Pipeline API server container image (WAR on Jetty)             |

(Other profiles: `prism`, `portable`, `dataflow-gpu`.)

## Pipeline API Server

The Pipeline server is an auxiliary tool for creating, debugging, and deploying pipelines. It provides:

* **Web UI (Pipeline Builder)** — build and edit pipeline configs in a GUI editor
* **REST API** — validate config files and launch jobs programmatically
* **MCP server & built-in AI agent** — AI integration that references the bundled module documentation to help you design pipelines

Build and run it locally as follows, then open `http://localhost:8080/` in your browser.

```sh
# Build and push the server container image
mvn clean package -DskipTests -Pserver -Dimage="{region}-docker.pkg.dev/{deploy_project}/{template_repo_name}/server"

# Run locally
docker run \
  -p "8080:8080" \
  -v ~/.config/gcloud:/mnt/gcloud:ro \
  --rm {region}-docker.pkg.dev/{deploy_project}/{template_repo_name}/server
```

See [How to Deploy Pipeline](docs/deploy/README.md) for deploying the server to Cloud Run.

## Committers

 * Yoichi Nagai ([@orfeon](https://github.com/orfeon))

## Contribution

Please read the CLA carefully before submitting your contribution to Mercari.
Under any circumstances, by submitting your contribution, you are deemed to accept and agree to be bound by the terms and conditions of the CLA.

https://www.mercari.com/cla/

## License

Copyright (c) 2020 mercari

Licensed under the [MIT License](LICENSE).
