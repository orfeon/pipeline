# Execute Mercari Pipeline

## gcloud command

gcloud command allows you to execute a configuration file uploaded to GCS with parameters as follows

```sh
gcloud storage cp config.yaml gs://{path/to/config.yaml}

gcloud dataflow flex-template run {job_name} \
  --template-file-gcs-location=gs://{path/to/template_file} \
  --parameters=config=gs://{path/to/config.yaml}
```

You can also pass the config content directly instead of uploading it to GCS.

```sh
gcloud dataflow flex-template run {job_name} \
  --template-file-gcs-location=gs://{path/to/template_file} \
  --parameters=config="$(cat path/to/config.yaml)"
```

## REST API

You can also run template by [REST API](https://cloud.google.com/dataflow/docs/reference/rest/v1b3/projects.locations.flexTemplates/launch).

In the following example, instead of uploading the config file to GCS, the contents are specified directly from a local file.
If you want to specify the contents of the config file directly via REST API, you should be aware that you need to escape the JSON string in the config file (quotes and newlines in the config break the request body if embedded as-is).

```sh
PROJECT_ID=[PROJECT_ID]
REGION=[REGION]
CONFIG="$(cat examples/xxx.yaml)"

curl -X POST -H "Content-Type: application/json"  -H "Authorization: Bearer $(gcloud auth print-access-token)" "https://dataflow.googleapis.com/v1b3/projects/${PROJECT_ID}/locations/${REGION}/flexTemplates:launch" -d "{
  'launchParameter': {
    'jobName': 'myJobName',
    'containerSpecGcsPath': 'gs://{path/to/template_file}',
    'parameters': {
      'config': '$(echo "$CONFIG")',
      'stagingLocation': 'gs://{path/to/staging}'
    },
    'environment': {
      'tempLocation': 'gs://{path/to/temp}'
    }
  }
}"
```

(The options `tempLocation` and `stagingLocation` are optional. If not specified, a bucket named `dataflow-staging-{region}-{project_no}` will be automatically generated and used)

### Run Template in streaming mode

To run Template in streaming mode, specify `streaming=true` in the parameter.

```sh
gcloud dataflow flex-template run {job_name} \
  --template-file-gcs-location=gs://{path/to/template_file} \
  --parameters=config=gs://{path/to/config.yaml} \
  --parameters=streaming=true
```

## Run Pipeline locally (DirectRunner)

You can run a pipeline locally using the container image built with the `direct` profile
(see [How to Deploy Pipeline](../deploy/README.md#deploy-direct-runner-for-local-execution)).
This is useful when you want to process small data quickly.

For local execution, execute the following command to grant the necessary permissions.

```sh
gcloud auth application-default login
```

The following are examples of locally executed commands.
The authentication file (and the config file, when passed as a file path) is mounted for access by the container.
The other arguments (such as `project` and `config`) are the same as for normal execution.

If you want to run in streaming mode, specify `streaming=true` in the argument as you would in normal execution.

### Mac OS

The config content is passed inline here, so only the gcloud credentials need to be mounted.

```sh
docker run \
  -v ~/.config/gcloud:/mnt/gcloud:ro \
  --rm {region}-docker.pkg.dev/{deploy_project}/{template_repo_name}/direct \
  --config="$(cat path/to/config.yaml)"
```

### Windows OS

```sh
docker run ^
  -v C:\Users\{YourUserName}\AppData\Roaming\gcloud:/mnt/gcloud:ro ^
  -v C:\Users\{YourWorkingDirPath}\:/mnt/config:ro ^
  --rm {region}-docker.pkg.dev/{deploy_project}/{template_repo_name}/direct ^
  --config=/mnt/config/{MyConfig}.yaml
```

* Note:
  * If you use BigQuery module locally, you will need to specify the `tempLocation` argument.
  * If the pipeline is to access an emulator running on a local machine, such as Cloud Spanner, the `--net=host` option is required.

## Run on Apache Flink / Apache Spark

Build the bundled jar with the `flink` or `spark` Maven profile
(see [How to Deploy Pipeline](../deploy/README.md#build-bundled-jar-for-apache-flink--apache-spark)),
then submit it to your cluster with `--runner` and `--config` arguments.
The main class is `com.mercari.solution.MPipeline`.

### Apache Flink

```sh
mvn clean package -DskipTests -Pflink

flink run \
  -c com.mercari.solution.MPipeline \
  target/pipeline-bundled-{version}.jar \
  --runner=FlinkRunner \
  --config="$(cat path/to/config.yaml)"
```

### Apache Spark

```sh
mvn clean package -DskipTests -Pspark

spark-submit \
  --class com.mercari.solution.MPipeline \
  --master {spark_master_url} \
  target/pipeline-bundled-{version}.jar \
  --runner=SparkRunner \
  --config="$(cat path/to/config.yaml)"
```

* Note:
  * If the pipeline uses Google Cloud modules (BigQuery, Spanner, GCS, ...), the cluster workers need Google Cloud credentials (e.g. set `GOOGLE_APPLICATION_CREDENTIALS` on the workers).
