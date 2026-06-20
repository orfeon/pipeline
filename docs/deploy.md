# Deploy Mercari Pipeline

Mercari Pipeline is a portable pipeline tool developed by Apache Beam.
It is deployed as a Docker image for each of data processing frameworks such as Cloud Dataflow, Apache Flink and Apache Spark.

The server also provides pipeline validation, dry runs, and job execution capabilities.
It can be used for debugging pipeline processing by developers or as a convenient pipeline tool for users.
Additionally, an MCP server has been added, enabling the invocation of pipeline functions and tools from the MCP Host.

The following models are the supported distributed processing frameworks.

1. Cloud Dataflow
2. Apache Spark
3. Apache Flink

Deployment models for use on standard servers, not distributed clusters.

1. Direct
2. Server

## Requirements

* Java 21
* [Maven 3](https://maven.apache.org/index.html)
* [gcloud command-line tool](https://cloud.google.com/sdk/gcloud)

## Ready for pushing pipeline image to Cloud Artifact Registry.

The first step is to build the source code and register it as a container image to the [Cloud Artifact Registry](https://cloud.google.com/artifact-registry).

To upload container images to the Artifact registry via Docker commands, you will first need to execute the following commands, depending on the repository region.

```sh
gcloud auth login
gcloud auth configure-docker us-central1-docker.pkg.dev, asia-northeast1-docker.pkg.dev
```

## Deploy for Cloud Dataflow

Mercari Pipeline for Cloud Dataflow, it is deployed as a Flex Template.
Deploy the Flex Template Docker image to GAR and create a template file in GCS that references the image.
Launch a Dataflow job using the Dataflow Flex Template Launch API.

### Push Docker Image to GAR.

The following command will generate a container for FlexTemplate from the source code and upload it to Artifact Registry.

```sh
mvn clean package -DskipTests -Dimage={region}-docker.pkg.dev/{deploy_project}/{template_repo_name}/dataflow
```

### Upload template file.

The next step is to generate a template file to start a job from the container image and upload it to GCS.

Use the following command to generate a template file that can execute a dataflow job from a container image, and upload it to GCS.

```sh
gcloud dataflow flex-template build gs://{path/to/template_file} \
  --image "{region}-docker.pkg.dev/{deploy_project}/{template_repo_name}/dataflow" \
  --sdk-language "JAVA"
```

## Deploy for Apache Flink

Mercari Pipeline for Apache Flink is deployed as a Docker image that can run in [Flink Application Mode](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/overview/#application-mode).
The image contains the pipeline fat JAR and can serve as both JobManager and TaskManager.

### Build Docker Image

#### Dockerfile

```Dockerfile
FROM flink:1.20-java21
COPY target/pipeline-bundled-*.jar /opt/flink/usrlib/pipeline.jar
```

```sh
# 1. Build the fat JAR with the Flink profile
mvn clean package -DskipTests -Pflink

# 2. Build the Docker image
docker build -f containers/flink/Dockerfile -t {region}-docker.pkg.dev/{project}/{repo}/flink:latest .
```

### Push Docker Image to GAR

```sh
docker push {region}-docker.pkg.dev/{project}/{repo}/flink:latest
```

### Deploy with Flink Kubernetes Operator

Example `FlinkDeployment` resource for [Flink Kubernetes Operator](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/):

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: mercari-pipeline
spec:
  image: {region}-docker.pkg.dev/{project}/{repo}/flink:latest
  flinkVersion: "v1_20"
  flinkConfiguration:
    taskmanager.numberOfTaskSlots: "2"
  serviceAccount: flink
  jobManager:
    resource:
      memory: "2048m"
      cpu: 1
  taskManager:
    resource:
      memory: "2048m"
      cpu: 1
  job:
    entryClass: com.mercari.solution.MPipeline
    args:
      - "--config=gs://path/to/config.json"
    parallelism: 2
```

### Deploy as Standalone Flink Cluster

```sh
# Start JobManager
docker run -d --name jobmanager \
  -p 8081:8081 \
  {region}-docker.pkg.dev/{project}/{repo}/flink:latest \
  standalone-job --job-classname com.mercari.solution.MPipeline \
  --config=gs://path/to/config.json

# Start TaskManager
docker run -d --name taskmanager \
  --link jobmanager:jobmanager \
  -e JOB_MANAGER_RPC_ADDRESS=jobmanager \
  {region}-docker.pkg.dev/{project}/{repo}/flink:latest \
  taskmanager
```

## Deploy for Apache Spark

Mercari Pipeline for Apache Spark is deployed as a Docker image that can run in Spark Application Mode on [Kubernetes](https://spark.apache.org/docs/latest/running-on-kubernetes.html) or standalone clusters.
The image contains the pipeline fat JAR and can serve as both driver and executor.

### Build Docker Image

#### Dockerfile

```Dockerfile
FROM apache/spark:4.0.0-java21
COPY target/pipeline-bundled-*.jar /opt/spark/usrlib/pipeline.jar
```

```sh
# 1. Build the fat JAR with the Spark profile
mvn clean package -DskipTests -Pspark

# 2. Build the Docker image
docker build -f containers/spark/Dockerfile -t {region}-docker.pkg.dev/{project}/{repo}/spark:latest .
```

### Push Docker Image to GAR

```sh
docker push {region}-docker.pkg.dev/{project}/{repo}/spark:latest
```

### Deploy on Kubernetes with spark-submit

```sh
spark-submit \
  --master k8s://https://{k8s-apiserver-host}:{port} \
  --deploy-mode cluster \
  --class com.mercari.solution.MPipeline \
  --conf spark.kubernetes.container.image={region}-docker.pkg.dev/{project}/{repo}/spark:latest \
  --conf spark.kubernetes.namespace={namespace} \
  --conf spark.executor.instances=2 \
  local:///opt/spark/usrlib/pipeline.jar \
  --config=gs://path/to/config.json
```

### Deploy with Spark Kubernetes Operator

Example `SparkApplication` resource for [Spark on Kubernetes Operator](https://github.com/kubeflow/spark-operator):

```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: mercari-pipeline
spec:
  type: Java
  mode: cluster
  image: {region}-docker.pkg.dev/{project}/{repo}/spark:latest
  mainClass: com.mercari.solution.MPipeline
  mainApplicationFile: local:///opt/spark/usrlib/pipeline.jar
  arguments:
    - "--config=gs://path/to/config.json"
  sparkVersion: "4.0.0"
  driver:
    cores: 1
    memory: "2048m"
  executor:
    cores: 1
    instances: 2
    memory: "2048m"
```

### Deploy as Standalone Spark Cluster

```sh
docker run -d --name spark-master \
  -p 8080:8080 -p 7077:7077 \
  {region}-docker.pkg.dev/{project}/{repo}/spark:latest \
  /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master

docker run -d --name spark-worker \
  --link spark-master:spark-master \
  -e SPARK_MASTER=spark://spark-master:7077 \
  {region}-docker.pkg.dev/{project}/{repo}/spark:latest \
  /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077
```

## Deploy for Direct Runner (for single node execution)

Direct runner is for pipeline processing on a single-node server, not a specific distributed processing framework.

### Push Docker Image to GAR

```sh
mvn clean package -DskipTests -Pdirect -Dimage={region}-docker.pkg.dev/{deploy_project}/{template_repo_name}/direct:latest
```

### Deploy Cloud Run

```sh
mvn clean package -DskipTests -Pserver -Dimage={region}-docker.pkg.dev/{deploy_project}/{template_repo_name}/server:latest
```


## Deploy Pipeline API Server (for pipeline API server)

Pipeline API server is for calling several Mercari Pipeline functions via API or operating them through the UI.
Direct Runner is used for pipeline execution.

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
  --region={project} \
  --execution-environment=gen2 \
  --port=8080 \
  --no-allow-unauthenticated
```

To start the server locally using the repository code, execute the following command using the Jetty plugin in the repository's top directory

```sh
mvn jetty:run -Pserver
```
