# Deploy Mercari Pipeline

Mercari Pipeline is a portable pipeline tool developed with Apache Beam.
It is deployed as a Docker image (for Cloud Dataflow, local execution, and the Pipeline API server) or as a bundled jar (for Apache Flink and Apache Spark clusters).

To run on AWS (EMR / Managed Service for Apache Flink) or to access the other cloud's
resources from either side without distributing keys, see
[Cross-Cloud Authentication Setup](cross-cloud-auth.md).

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

The same image also runs serverlessly — it supports all three Cloud Run forms, and any
Kubernetes cluster:

| deployment | use case |
| --- | --- |
| [Cloud Run Jobs](cloud-run-jobs.md) | one-shot or scheduled batch pipelines |
| [Cloud Run Worker Pools](cloud-run-worker-pools.md) | streaming pipelines; config-queue worker (configs via Pub/Sub subscription) |
| [Cloud Run Services](cloud-run-service.md) | HTTP-triggered runs (serve mode): Cloud Scheduler, Pub/Sub push, request-carried data via the `request` source |
| [Kubernetes](kubernetes.md) | the same patterns as Job/CronJob/Deployment on GKE or any cluster |

## Deploy Prism Runner (for local / Cloud Run execution)

The `prism` profile builds the same self-contained image with Beam's
[Prism runner](https://beam.apache.org/documentation/runners/prism/) — the portable successor of
DirectRunner — as the entrypoint. It runs everywhere the direct image runs (local docker, all three
Cloud Run forms, Kubernetes) with the same arguments.

```sh
mvn clean package -DskipTests -Pprism -Dimage={region}-docker.pkg.dev/{deploy_project}/{template_repo_name}/prism:latest
```

Prefer it over the direct image when the pipeline has heavy keyed stages (GroupByKey over coarse or
global keys, e.g. the `feature` transform's global encoding levels): DirectRunner's GroupByKey copies
each key's buffered state per bundle and such stages slow down by orders of magnitude as rows grow,
while Prism executes them at proper speed.

* The image does not bundle the prism binary: the runner downloads it from the Beam GitHub release at
  startup, so the runtime needs outbound network access (otherwise point
  `options.prism.prismLocation` at a pre-downloaded binary — see [Prism Options](../options/prism.md)).
* Prism executes **in memory** (no disk spill): the container memory must fit the pipeline's working
  set, which grows roughly linearly with the input. When the data outgrows the machine (a Cloud Run
  Job caps at 32 GiB), run on Dataflow instead — prism is the subset-verification / reproduction tier,
  not the full-production one.
* See [How to Execute Pipeline](../exec/README.md#run-pipeline-locally--on-cloud-run-prism) for how to run it.

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

Everything the server reads from its environment — including what the Builder's **Launch**
button needs for Dataflow, Cloud Run Jobs / Worker Pools and Dataproc — is listed in
[server.md](server.md); the MCP server it exposes (tools, workflows, client setup) in [mcp.md](mcp.md).
