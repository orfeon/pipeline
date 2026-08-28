# Pipeline Server (Builder UI / API / MCP): configuration reference

The image built with the `server` profile (see [Deploy](README.md#deploy-pipeline-api-server))
serves the Pipeline Builder UI, the REST API, the MCP server and the agent. This page lists
everything it reads from its environment, and what the **Launch** feature needs to run a
config from the UI.

## Launch targets

`POST /api/launch` (the Builder's Launch button) submits the current config to a runner on an
execution environment. The modal is rendered from `/api/spec/launch`; the server fills the
defaults it resolved from its environment into the form.

| runner | environment | what happens | pre-requisites |
|---|---|---|---|
| `dataflow` | `flexTemplate` (default) | `flexTemplates.launch` with the deployed dataflow image | Flex Template spec on GCS ([deploy](README.md#deploy-cloud-dataflow-flex-template)) |
| `direct` | `cloudRunJob` (default) | `jobs.run` on a **pre-created** Cloud Run Job built from the direct image, overriding its container args with `--config=…` / `--args.*` | the job exists ([Cloud Run Jobs](cloud-run-jobs.md#launch-from-the-pipeline-builder)) |
| `direct` | `cloudRunWorkerPool` | creates a Cloud Run Worker Pool running the config until you delete it | direct image URI ([Worker Pools](cloud-run-worker-pools.md#launch-from-the-pipeline-builder)) |
| `spark` | `dataprocServerless` (default) | Dataproc Serverless batch with the bundled jar | jar on GCS ([deploy](README.md#build-bundled-jar-for-apache-flink--apache-spark)) |

### How each value is resolved

For every value a launch needs (project, region, service account, job name, ...), the first
match wins:

1. the launch modal's parameters,
2. the config's `options` — the runner block first (`options.dataflow.project` / `region` /
   `serviceAccount` / `templateLocation`, Dataflow only), then the common `options.gcp.project` /
   `options.gcp.workerRegion` (all runners),
3. the runner-specific env var `MERCARI_PIPELINE_LAUNCH_<RUNNER>_<KEY>`,
4. the common env var `MERCARI_PIPELINE_LAUNCH_<KEY>`,
5. the environment the server runs in: `GOOGLE_CLOUD_PROJECT`, then the GCE metadata server
   (project id, region of the Cloud Run service, default service account).

So a server deployed on Cloud Run in the project and region you launch into needs no
project/region configuration at all.

## Environment variables

### Launch (`MERCARI_PIPELINE_LAUNCH[_<RUNNER>]_<KEY>`)

Common keys accept a runner-specific override (`_DATAFLOW_`, `_DIRECT_`, `_SPARK_`).

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
| `MERCARI_PIPELINE_LAUNCH_SPARK_JARS` / `_VERSION` | Bundled jar (`gs://…`) and Dataproc Serverless runtime version (`3.0`) |

Every launched Dataflow job / worker pool carries the labels `mercari-pipeline-version` and
`mercari-pipeline-user` (the IAP user, from `X-Goog-Authenticated-User-Email`), which the
[diagnosis tools](diagnosis.md) rely on.

**Deprecated names** (still read, with a startup warning): `MERCARI_PIPELINE_DATAFLOW_PROJECT`,
`MERCARI_PIPELINE_DATAFLOW_REGION`, `MERCARI_PIPELINE_DATAFLOW_SERVICE_ACCOUNT`,
`MERCARI_PIPELINE_DATAFLOW_SUBNETWORK`, `MERCARI_PIPELINE_DATAFLOW_STAGING_LOCATION`,
`MERCARI_PIPELINE_DATAFLOW_TEMPLATE_LOCATION`, `MERCARI_PIPELINE_TEMP_LOCATION`. They map to the
common (`_LAUNCH_`) keys, so they keep applying to every runner.

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
