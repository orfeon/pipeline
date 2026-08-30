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

The MCP server is the same deployment: Streamable HTTP at `https://<service>/mcp`
(`web.xml` maps `/mcp`; the legacy SSE transport is at `/mcp/sse`). It exposes the tools
`list-modules` / `read-docs` (a module by id or any document by path), `validate-feature`,
`run-pipeline` (in-server DirectRunner run, or `dryRun: true`), `launch-pipeline`,
`get-job` / `get-job-progress` / `get-job-logs` / `list-job-errors` / `list-failed-jobs` (Dataflow jobs and Cloud Run Job
executions alike; `get-job-progress` = workers, autoscaling decisions, stage timeline, running stage and feature plan mapping) / `resolve-stack-trace`, plus the docs as `docs://` resources. The Pipeline Builder agent's tools are
thin wrappers over the same implementations (`McpToolBridge`), so both surfaces behave identically; agent tool
names are the camelCase form of the MCP names (`run-pipeline` / `runPipeline`, `get-job-logs` / `getJobLogs`).
Every tool carries MCP annotations: only `run-pipeline` and `launch-pipeline` are not read-only (clients may
ask for confirmation before calling them); the job tools are marked open-world. Three ways to reach it:

### Cloud Run through `gcloud run services proxy` (recommended for developers)

The service is deployed with `--no-allow-unauthenticated`, so every request needs a Google
identity token. The proxy adds it for you and keeps it fresh, so the client talks plain HTTP to
localhost:

```sh
gcloud run services proxy <service> --project=<project> --region=<region> --port=8080
```

Then register `http://localhost:8080/mcp` in the client. Claude Code:

```sh
claude mcp add --transport http mercari-pipeline http://localhost:8080/mcp
```

JSON-configured clients that speak Streamable HTTP themselves (Claude Code `.mcp.json`, Cursor
`.cursor/mcp.json`, VS Code `mcp.json`, ...). Keep `"type": "http"`: Claude Code and VS Code treat an
entry without `type` as a stdio server (`command`) and fail to start it, while Cursor infers the
transport from `url` — the explicit type works everywhere:

```json
{
  "mcpServers": {
    "mercari-pipeline": {
      "type": "http",
      "url": "http://localhost:8080/mcp"
    }
  }
}
```

A client that only supports stdio servers (Claude Desktop's `claude_desktop_config.json`, older
clients) bridges with `mcp-remote`:

```json
{
  "mcpServers": {
    "mercari-pipeline": {
      "command": "npx",
      "args": ["-y", "mcp-remote", "http://localhost:8080/mcp"]
    }
  }
}
```

The user running the proxy needs `roles/run.invoker` on the service (and, if the server is behind
IAP, access to the IAP resource). Actions the tools perform — launching jobs, reading Dataflow /
Cloud Run state — run with the **server's** service account, not the user's.

### Cloud Run directly with an identity token

Without the proxy, send the token yourself. It expires after an hour, so wrap it in a shell
expansion the client re-evaluates at start-up:

```sh
claude mcp add --transport http mercari-pipeline https://<service>.run.app/mcp \
  --header "Authorization: Bearer $(gcloud auth print-identity-token)"
```

```json
{
  "mcpServers": {
    "mercari-pipeline": {
      "command": "npx",
      "args": ["-y", "mcp-remote", "https://<service>.run.app/mcp",
               "--header", "Authorization: Bearer ${ID_TOKEN}"],
      "env": { "ID_TOKEN": "<output of: gcloud auth print-identity-token>" }
    }
  }
}
```

### Local server (`mvn jetty:run -Pserver`)

For development, run the server from the repository (no authentication):

```sh
gcloud auth application-default login   # credentials the launch / job tools use
export MERCARI_PIPELINE_LAUNCH_PROJECT=<project>
export MERCARI_PIPELINE_LAUNCH_REGION=<region>
export MERCARI_PIPELINE_LAUNCH_DATAFLOW_TEMPLATE_LOCATION=gs://<bucket>/templates/dataflow.json
export MERCARI_PIPELINE_LAUNCH_DIRECT_JOB=<cloud run job>
mvn jetty:run -Pserver
```

and register `http://localhost:8080/mcp` exactly as in the proxy case. Everything the tools do
(Dataflow / Cloud Run API calls, GCS staging) then uses your application-default credentials,
so `run-pipeline` without `dryRun` executes the pipeline on your machine with DirectRunner.

### A typical session

1. `validate-feature` on the feature step (or `run-pipeline` with `dryRun: true` on the whole
   config — it also returns every step's resolved schema and the feature plans with their hot-key
   audit SQL).
2. `launch-pipeline` with `runner: dataflow` (Flex Template) or `runner: direct` (a pre-created
   Cloud Run Job, quicker to iterate on), passing template arguments in `args`.
3. Poll with `get-job` (job id or execution name); when it looks slow or stuck, `get-job-progress`
   (workers, stage timeline, running stage); on failure `list-job-errors`, `get-job-logs` (context,
   `contains` to grep) and `resolve-stack-trace`.
