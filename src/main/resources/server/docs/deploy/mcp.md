# MCP server: tools, resources and workflows

The Pipeline Server (the image built with the `server` Maven profile, see [server.md](server.md)) is
also an [MCP](https://modelcontextprotocol.io/) server: an AI client (Claude Code, Cursor, VS Code,
Claude Desktop, ...) connected to it can read the module documentation, validate and dry-run
configs, launch them on Dataflow / Cloud Run, and follow and diagnose the jobs — the same loop the
Pipeline Builder's built-in agent runs, exposed to any MCP host.

- Endpoint: `https://<service>/mcp` (Streamable HTTP; `web.xml` maps `/mcp`).
- Capabilities: 14 tools, the documentation tree as `docs://` resources, one prompt.
- The Pipeline Builder agent's tools are thin wrappers over the same implementations
  (`McpToolBridge`); an agent tool is the camelCase form of the MCP tool name (`run-pipeline` /
  `runPipeline`), so everything below applies to both.

## Tools

Every tool returns text (JSON or Markdown) and carries MCP annotations: only `run-pipeline` and
`launch-pipeline` are **not read-only** (clients may ask for confirmation before calling them); the
launch and job tools are marked open-world (they talk to Google Cloud APIs).

### Documentation and source code

| tool | agent name | arguments | what it returns |
|---|---|---|---|
| `list-modules` | `listModules` | `type?` (source / transform / sink / action) | the module catalog (`module/index.yaml`): one line per module with description and tags |
| `read-docs` | `readDocs` | `module` (`{type}/{name}`, e.g. `transform/feature`) **or** `path` (e.g. `module/common/filter.md`, `system.md`) | a module's full reference (parameters, examples) or any bundled document |
| `search-code` | `searchCode` | `pattern`, `pathFilter?` | regex search over the framework's Java sources (`path:line: text`) |
| `read-source` | `readSource` | `path`, `startLine?`, `endLine?` | a slice of a source file with line numbers (500 lines max per call) |
| `find-module-source` | `findModuleSource` | `type`, `name` | the class implementing a module |
| `resolve-stack-trace` | `resolveStackTrace` | `stackTrace` | every `com.mercari.solution` frame with its source lines |
| `upgrade-config` | — | `config` | a config rewritten from deprecated schema notations |

The source tools read `MERCARI_PIPELINE_SOURCES_PATH` (or `WEB-INF/sources` in the image).

### Validation and execution

| tool | agent name | arguments | what it returns |
|---|---|---|---|
| `run-pipeline` | `runPipeline` | `config`, `dryRun?`, `args?` | **`dryRun: true`**: assembles the whole config (module validation, schema resolution, feature plan compilation against the real input schemas) and returns every step's resolved schema (`spec.modules`) plus `featurePlans` (the feature transforms' plans with stages, columns, availability status, hot-key audit SQL, diagnostics). **`dryRun: false`**: runs the pipeline inside the server with DirectRunner and returns `debug` sink outputs — for small test data only |
| `validate-feature` | `validateFeature` | `config` or `parameters`, `name?`, `inputSchema?`, `args?`, `format?` | a feature transform's `validate --expand` report without a full config |
| `launch-pipeline` | `launchPipeline` | `config`, `runner` (dataflow / direct / prism / spark), `environment?`, `parameters?`, `args?` | submits the config — Dataflow Flex Template, a pre-created Cloud Run Job, a Cloud Run Worker Pool or Dataproc Serverless — and returns the job (`id`, `name`, `project`, `location`, `state`, `consoleUrl`). Launch parameters default from the config's `options`, then the server's `MERCARI_PIPELINE_LAUNCH_*` environment |

Template arguments: the config refers to them as `${args.<name>}`; defaults come from the config's
`args` / `system.args` block and `args` passed to the tool override them. A placeholder that is left
without a value is refused before launching.

### Jobs (Dataflow jobs and Cloud Run Job executions alike)

A job reference is a Dataflow job id (`2026-07-17_22_25_11-...`) or exact job name, or a Cloud Run
execution name (`projects/.../jobs/.../executions/...`); the tool infers the runner from it, `runner`
forces it. `project` / `region` default to the server's launch configuration.

| tool | agent name | what it returns |
|---|---|---|
| `get-job` | `getJob` | status; for Dataflow also the config the job was launched with; with `runner: direct` and no job, the latest executions of the configured Cloud Run Job |
| `get-job-progress` | `getJobProgress` | why a Dataflow job is slow or not scaling: current / target workers with the autoscaler's decisions, the stage completion timeline with durations, the running fused stage (its transforms, the element counts of its inputs / outputs — few groups read but most rows already emitted means a tail of hot keys), and the feature plan's stages / keys mapped to the Dataflow stages |
| `list-job-errors` | `listJobErrors` | the error picture: Dataflow error messages / execution conditions plus the deduplicated worker error logs (with stack traces) from Cloud Logging |
| `get-job-logs` | `getJobLogs` | Cloud Logging entries of the job (`minSeverity`, `contains`, `limit`; latest first) — the INFO / WARNING context around an error, the feature plan report a job logs at startup, progress messages. Cloud Run container stdout carries severity DEFAULT, so for `runner: direct` any threshold below WARNING returns all lines (stderr maps to ERROR) |
| `list-failed-jobs` | `listFailedJobs` | jobs that failed in the last `hours` (default 24): Dataflow jobs and the failed executions of the configured Cloud Run Job |

## Resources and prompt

- `docs://<path>` — every Markdown file of this documentation tree (`docs://module/transform/feature.md`,
  `docs://deploy/mcp.md`, ...), for clients that browse resources rather than call tools.
- prompt `design-pipeline` — instructions for designing a config from a requirement, pointing at the
  module resources and at `run-pipeline` for validation.

## Workflows

**Design and validate a config**

1. `list-modules` (optionally per type) → `read-docs module=<type>/<name>` for the modules you will use
   (shared parameter docs such as `module/common/filter.md` by `path`).
2. Write the config; `run-pipeline` with `dryRun: true`. Fix every error; read the diagnostics and,
   for a `feature` transform, the `featurePlans` report (availability status of each column, stages and
   shuffles with their dependency `waves` — how deep the chain would be if independent stages ran in parallel —, hot-key audit SQL to run on your warehouse before a large backfill).
3. Optionally `run-pipeline` without `dryRun` on a test config (a `create` source with a few records
   and a `debug` sink) to see actual output rows.

**Launch and follow a job**

1. `launch-pipeline` with `runner: dataflow` (Flex Template), `runner: direct` (a pre-created Cloud
   Run Job — quicker to iterate on) or `runner: prism` (the same with the prism image — the choice for
   subset runs of pipelines with coarse-key stages such as `feature`, which DirectRunner crawls on),
   template arguments in `args`, sizing in `parameters`
   (`workerMachineType`, `numWorkers`, `maxNumWorkers`, `diskSizeGb`, `jobName`).
2. `get-job` with the returned id / execution name until it finishes.
3. If it looks slow or stays on one worker: `get-job-progress`.

**Diagnose a failure**

1. `list-failed-jobs` when the job id is unknown.
2. `list-job-errors` — the deduplicated errors with stack traces.
3. `resolve-stack-trace` on a stack trace to see the failing framework code; `get-job-logs` with
   `contains` / `minSeverity` for the surrounding context; `search-code` / `read-source` to go deeper.
4. Fix the config (`read-docs` for the parameter reference) and `run-pipeline dryRun` again before
   relaunching.

## Connecting a client

The service is deployed with `--no-allow-unauthenticated`, so every request needs a Google identity
token. Three ways:

### Cloud Run through `gcloud run services proxy` (recommended)

The proxy adds and refreshes the token, so the client talks plain HTTP to localhost:

```sh
gcloud run services proxy <service> --project=<project> --region=<region> --port=8080
```

Claude Code:

```sh
claude mcp add --transport http mercari-pipeline http://localhost:8080/mcp
```

JSON-configured clients that speak Streamable HTTP themselves (Claude Code `.mcp.json`, Cursor
`.cursor/mcp.json`, VS Code `mcp.json`). Keep `"type": "http"`: Claude Code and VS Code treat an entry
without `type` as a stdio server and fail to start it, while Cursor infers the transport from `url`:

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

The user running the proxy needs `roles/run.invoker` on the service (and, if the server is behind IAP,
access to the IAP resource).

### Cloud Run directly with an identity token

The token expires after an hour, so wrap it in a shell expansion the client re-evaluates at start-up:

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

For development, run the server from the repository (no authentication) and register
`http://localhost:8080/mcp` as above:

```sh
gcloud auth application-default login   # credentials the launch / job tools use
export MERCARI_PIPELINE_LAUNCH_PROJECT=<project>
export MERCARI_PIPELINE_LAUNCH_REGION=<region>
export MERCARI_PIPELINE_LAUNCH_DATAFLOW_TEMPLATE_LOCATION=gs://<bucket>/templates/dataflow.json
export MERCARI_PIPELINE_LAUNCH_DIRECT_JOB=<cloud run job>
export MERCARI_PIPELINE_LAUNCH_PRISM_JOB=<cloud run job built from the prism image>   # optional
mvn jetty:run -Pserver
```

## Permissions and caveats

- Everything the tools do — launching jobs, reading Dataflow / Cloud Run state and Cloud Logging — runs
  with the **server's** service account (see [IAM](server.md#iam-for-the-servers-service-account)), not
  the connected user's; with the local server it is your application-default credentials.
- `run-pipeline` without `dryRun` executes inside the server process (DirectRunner, bounded by
  `MERCARI_PIPELINE_WAIT_SECONDS`); use it for small test data, `launch-pipeline` for real runs.
- The tools return plain text; clients that require structured results get the JSON as text content.
- Configuration of the launch defaults and the environment variables: [server.md](server.md).
