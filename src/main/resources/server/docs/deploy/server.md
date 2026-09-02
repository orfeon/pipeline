# Pipeline Server (Builder UI / API / MCP): configuration reference

The image built with the `server` profile (see [Deploy](README.md#deploy-pipeline-api-server))
serves the Pipeline Builder UI, the REST API, the MCP server and the agent. This page lists
everything it reads from its environment, and what the **Launch** feature needs to run a
config from the UI.

## Launch targets

`POST /api/launch` (the Builder's Launch button) submits the current config to a runner on an
execution environment. The modal is rendered from `/api/spec/launch`; the server shows the values
it resolved from its environment as placeholders (an untouched field is submitted empty, so config
options still take precedence).

| runner | environment | what happens | pre-requisites |
|---|---|---|---|
| `dataflow` | `flexTemplate` (default) | `flexTemplates.launch` with the deployed dataflow image | Flex Template spec on GCS ([deploy](README.md#deploy-cloud-dataflow-flex-template)) |
| `direct` | `cloudRunJob` (default) | `jobs.run` on a **pre-created** Cloud Run Job built from the direct image, overriding its container args with `--config=…` / `--args.*` | the job exists ([Cloud Run Jobs](cloud-run-jobs.md#launch-from-the-pipeline-builder)) |
| `direct` | `cloudRunWorkerPool` | creates a Cloud Run Worker Pool running the config until you delete it | direct image URI ([Worker Pools](cloud-run-worker-pools.md#launch-from-the-pipeline-builder)) |
| `prism` | `cloudRunJob` (default) / `cloudRunWorkerPool` | the same two targets with a job / image built from the **prism** profile (Beam's portable local runner) — prefer it over `direct` for pipelines with keyed stages over coarse or global keys, such as `feature` transforms; in-memory, so subset-sized inputs | a prism job exists / prism image URI ([Cloud Run Jobs](cloud-run-jobs.md#running-with-the-prism-image), [Deploy Prism](README.md#deploy-prism-runner-for-local--cloud-run-execution)) |
| `spark` | `dataprocServerless` (default) | Dataproc Serverless batch with the bundled jar | jar on GCS ([deploy](README.md#build-bundled-jar-for-apache-flink--apache-spark)) |

### How each value is resolved

For every value a launch needs (project, region, service account, job name, ...), the first
match wins:

1. the launch modal's parameters,
2. the config's `options` — the runner block first (`options.dataflow.project` / `region` /
   `serviceAccount` / `templateLocation`, Dataflow only), then the common `options.gcp.project` /
   `options.gcp.workerRegion` (all runners; non-Dataflow runners still consult
   `options.dataflow.project` / `region` last, so Dataflow-only configs keep resolving),
3. the runner-specific env var `MERCARI_PIPELINE_LAUNCH_<RUNNER>_<KEY>`,
4. the common env var `MERCARI_PIPELINE_LAUNCH_<KEY>`,
5. the environment the server runs in: `GOOGLE_CLOUD_PROJECT`, then the GCE metadata server
   (project id, region of the Cloud Run service, default service account).

So a server deployed on Cloud Run in the project and region you launch into needs no
project/region configuration at all.

The same launch is available to AI clients: the MCP tool `launch-pipeline` (`config`, `runner`,
`environment`, `parameters`, `args`) and the Pipeline Builder agent tool `launchPipeline` call this
resolution; `run-pipeline` with `dryRun: true` validates a config beforehand (returning every step's
resolved schema and the feature transforms' plans), and `get-job` / `get-job-logs` / `list-job-errors`
follow the launched job whatever the runner. The server's service account needs the launch
permissions listed below for every target the clients may use.

## Environment variables

### Launch (`MERCARI_PIPELINE_LAUNCH[_<RUNNER>]_<KEY>`)

Common keys accept a runner-specific override (`_DATAFLOW_`, `_DIRECT_`, `_PRISM_`, `_SPARK_`). `JOB` and
`IMAGE` exist only under a runner name (`_DIRECT_JOB`, `_PRISM_JOB`, `_DIRECT_IMAGE`, `_PRISM_IMAGE`): a common
`MERCARI_PIPELINE_LAUNCH_JOB` / `_IMAGE` is not read, because the job / image decides which runner executes the
pipeline.

| Variable | Meaning |
|---|---|
| `MERCARI_PIPELINE_LAUNCH_PROJECT` | Project to launch into (fallback: `GOOGLE_CLOUD_PROJECT`, metadata server) |
| `MERCARI_PIPELINE_LAUNCH_REGION` | Region to launch into (fallback: metadata server) |
| `MERCARI_PIPELINE_LAUNCH_SERVICE_ACCOUNT` | Service account of the launched workers (Dataflow) / worker pool (fallback: the server's own, via metadata) |
| `MERCARI_PIPELINE_LAUNCH_SUBNETWORK` | Subnetwork (Dataflow workers; Cloud Run direct VPC egress for worker pools) |
| `MERCARI_PIPELINE_LAUNCH_STAGING_LOCATION` | `gs://` prefix. Dataflow staging, and where direct launches stage the config (`launch/yyyy/MM/dd/<launchId>/config.yaml`). Without it the config is passed inline (`--config=data:…`, up to ~24KB) |
| `MERCARI_PIPELINE_LAUNCH_TEMP_LOCATION` | Dataflow temp location |
| `MERCARI_PIPELINE_LAUNCH_LABELS` | Extra labels for every launched resource, `k=v,k=v` |
| `MERCARI_PIPELINE_LAUNCH_DATAFLOW_TEMPLATE_LOCATION` | Flex Template spec (`gs://…`) |
| `MERCARI_PIPELINE_LAUNCH_DIRECT_JOB` | Cloud Run Job to execute (name only; project/region from the keys above) |
| `MERCARI_PIPELINE_LAUNCH_DIRECT_TASK_TIMEOUT` | Default task timeout override for Cloud Run Job executions, seconds |
| `MERCARI_PIPELINE_LAUNCH_DIRECT_IMAGE` | Direct image URI for worker pools |
| `MERCARI_PIPELINE_LAUNCH_DIRECT_CPU` / `_MEMORY` / `_INSTANCES` | Worker pool sizing defaults (`4` / `6Gi` / `1`) |
| `MERCARI_PIPELINE_LAUNCH_PRISM_JOB` / `_TASK_TIMEOUT` / `_IMAGE` / `_CPU` / `_MEMORY` / `_INSTANCES` | The same keys for `prism` launches: the Cloud Run Job built from the prism image, and the prism image / sizing for worker pools. A `prism` launch never falls back to the `_DIRECT_` job / image (nor to a common one): without `_PRISM_JOB` (or `jobName`) it fails naming the variable to set |
| `MERCARI_PIPELINE_LAUNCH_SPARK_JARS` / `_VERSION` | Bundled jar (`gs://…`) and Dataproc Serverless runtime version (`3.0`) |

Every launched Dataflow job / worker pool carries the labels `mercari-pipeline-version` and
`mercari-pipeline-user` (the IAP user, from `X-Goog-Authenticated-User-Email`), which the
[diagnosis tools](diagnosis.md) rely on.

**Deprecated names** (still read, with a startup warning): `MERCARI_PIPELINE_DATAFLOW_PROJECT`,
`MERCARI_PIPELINE_DATAFLOW_REGION`, `MERCARI_PIPELINE_DATAFLOW_SERVICE_ACCOUNT`,
`MERCARI_PIPELINE_DATAFLOW_SUBNETWORK`, `MERCARI_PIPELINE_DATAFLOW_STAGING_LOCATION`,
`MERCARI_PIPELINE_DATAFLOW_TEMPLATE_LOCATION`, `MERCARI_PIPELINE_TEMP_LOCATION`. Project, region and
temp location map to the common keys (every runner); service account, subnetwork, staging and
template location stay Dataflow-only (`_LAUNCH_DATAFLOW_*`).

### Other server features

| Variable | Meaning |
|---|---|
| `MERCARI_PIPELINE_WAIT_SECONDS` | How long `/api/run` (in-server DirectRunner run) waits for the pipeline (default 10) |
| `MERCARI_PIPELINE_AGENT_MODEL` / `MERCARI_PIPELINE_AGENT_LOCATION` | Gemini model / location for the agent (default `gemini-3.7-flash` / `global`) |
| `MERCARI_PIPELINE_AGENT_RESPONSE_FORMAT` | `text` (default) or `json` |
| `MERCARI_PIPELINE_AGENT_DEBUG_LOG` | File path to log agent model exchanges to |
| `MERCARI_PIPELINE_DIAGNOSIS_SLACK_WEBHOOK` / `_WEBHOOK` / `_LOG_NAME` | [Diagnosis](diagnosis.md) outputs |
| `MERCARI_PIPELINE_WEBHOOK_TOKEN` | Shared secret for the diagnosis webhook |
| `MERCARI_PIPELINE_SOURCES_PATH` | Where the code-reading tools find `src/main/java` |
| `MERCARI_PIPELINE_VERSION` | Overrides the build version reported by the server |
| `MERCARI_PIPELINE_GCP_CREDENTIALS` / `MERCARI_PIPELINE_ON_GCP` | Credentials source / metadata-server override shared with the pipeline runtime ([cross-cloud auth](cross-cloud-auth.md)) |

The pipeline images themselves (not the server) read `MPIPELINE_CONFIG`,
`MPIPELINE_CONFIG_RELOAD` and `MPIPELINE_MAX_CONCURRENCY` in
[serve mode](cloud-run-service.md).

## IAM for the server's service account

| Launch target | Roles |
|---|---|
| Dataflow Flex Template | `roles/dataflow.developer`; `roles/iam.serviceAccountUser` on the worker service account; write access to the staging/temp buckets |
| Cloud Run Job | `roles/run.invoker` on the job (plus `run.jobs.get` to describe it — `roles/run.viewer`). No `actAs`: the job's own service account was set when the job was created |
| Cloud Run Worker Pool | `roles/run.developer`; `roles/iam.serviceAccountUser` on the pool's service account (a service account does **not** implicitly have `actAs` on itself, so grant it even when the pool runs as the server's account) |
| Dataproc Serverless | `roles/dataproc.editor`; `roles/iam.serviceAccountUser` on the batch service account |
| config staging | `roles/storage.objectAdmin` (or objectCreator) on the staging bucket |

The diagnosis tools additionally need the roles listed in [diagnosis.md](diagnosis.md#3-configure-the-server).

## Connecting an MCP client

The MCP surface (tools, resources, prompt, recommended workflows) and the client setup — Cloud Run
through `gcloud run services proxy`, an identity token, or a local `mvn jetty:run -Pserver` — are
documented in [mcp.md](mcp.md).
