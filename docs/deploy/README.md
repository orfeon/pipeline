# Deploy Mercari Pipeline

Mercari Pipeline is a portable pipeline tool developed with Apache Beam.
It is deployed as a Docker image (for Cloud Dataflow, local execution, and the Pipeline API server) or as a bundled jar (for Apache Flink and Apache Spark clusters).

## Requirements

* Java 21
* [Maven 3](https://maven.apache.org/index.html)
* [gcloud command-line tool](https://cloud.google.com/sdk/gcloud)

## Ready for pushing pipeline image to Cloud Artifact Registry.

The first step is to build the source code and register it as a container image to the [Cloud Artifact Registry](https://cloud.google.com/artifact-registry).

To upload container images to the Artifact Registry via Docker commands, you will first need to execute the following commands, depending on the repository region.
(Specify multiple registries separated by commas, without spaces.)

```sh
gcloud auth login
gcloud auth configure-docker us-central1-docker.pkg.dev,asia-northeast1-docker.pkg.dev
```

## Deploy Cloud Dataflow Flex Template

### Push Docker Image to GAR.

The following command will generate a container for FlexTemplate from the source code and upload it to Artifact Registry.

```sh
mvn clean package -DskipTests -Dimage={region}-docker.pkg.dev/{deploy_project}/{template_repo_name}/dataflow:latest
```

### Upload template file.

The next step is to generate a template file to start a job from the container image and upload it to GCS.

Use the following command to generate a template file that can execute a dataflow job from a container image, and upload it to GCS.

```sh
gcloud dataflow flex-template build gs://{path/to/template_file} \
  --image "{region}-docker.pkg.dev/{deploy_project}/{template_repo_name}/dataflow:latest" \
  --sdk-language "JAVA"
```

## Deploy Direct Runner (for local execution)

The `direct` profile builds a container image that runs the pipeline with DirectRunner.
This is useful when you want to process small data quickly without launching a Dataflow job.

```sh
mvn clean package -DskipTests -Pdirect -Dimage={region}-docker.pkg.dev/{deploy_project}/{template_repo_name}/direct:latest

# Pull the direct container image on the machine where you run the pipeline
docker pull {region}-docker.pkg.dev/{deploy_project}/{template_repo_name}/direct:latest
```

See [How to Execute Pipeline](../exec/README.md#run-pipeline-locally-directrunner) for how to run it.

## Build bundled jar for Apache Flink / Apache Spark

The `flink` and `spark` Maven profiles skip the container build and instead produce a bundled ("fat") jar
`target/pipeline-bundled-{version}.jar` that contains the corresponding Beam runner.
Submit this jar to your cluster to run pipelines.

```sh
# For Apache Flink clusters (FlinkRunner)
mvn clean package -DskipTests -Pflink

# For Apache Spark clusters (SparkRunner)
mvn clean package -DskipTests -Pspark
```

See [How to Execute Pipeline](../exec/README.md#run-on-apache-flink--apache-spark) for how to submit the jar to a cluster.

## Deploy Pipeline API Server (for pipeline API server)

### Push Docker Image to GAR

```sh
mvn clean package -DskipTests -Pserver -Dimage={region}-docker.pkg.dev/{deploy_project}/{template_repo_name}/server:latest
```

### Deploy Cloud Run

```sh
gcloud run deploy {service_name} \
  --project={project} \
  --image={region}-docker.pkg.dev/{deploy_project}/{template_repo_name}/server:latest \
  --platform=managed \
  --region={region} \
  --execution-environment=gen2 \
  --port=8080 \
  --no-allow-unauthenticated
```
