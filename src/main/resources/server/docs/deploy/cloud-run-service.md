# Run Pipeline on Cloud Run Services (HTTP serve mode)

The container image built with the `direct` profile
(see [Deploy Direct Runner](README.md#deploy-direct-runner-for-local-execution))
can also be deployed as a [Cloud Run Service](https://cloud.google.com/run/docs/deploying).
Each HTTP request assembles and runs one pipeline with DirectRunner, synchronously — the HTTP
status reflects the pipeline result the same way the batch exit code does.

This enables HTTP-triggered pipelines without creating a Job per config:

* **Cloud Scheduler → HTTP** for periodic batches (with per-run `args.*` parameters),
* **Pub/Sub push subscriptions** for event-driven pipelines that scale to zero
  (a [Worker Pool](cloud-run-worker-pools.md) with a pull subscription needs an always-on
  instance),
* **Cloud Workflows / Eventarc** composition, and request-response style small ETL,
* request-carried data processing via the [request source module](../module/source/request.md).

Serve mode activates automatically: Cloud Run Services always set the `PORT` environment
variable (Jobs and Worker Pools never do), and the entrypoint switches to an HTTP server when
it is present. `--serve=true` / `--serve=false` in the container args force the mode explicitly.
The server is the JDK built-in `com.sun.net.httpserver` — no extra dependencies, same image.

## Deploy

### Fixed config (recommended)

Fix the pipeline config at deploy time; requests then only supply template args and data.
The config can be given by container args or by environment variable — both are revision-level
settings, no image rebuild involved:

```sh
# via container args (same style as Cloud Run Jobs)
gcloud run deploy {service_name} \
  --project={project} \
  --region={region} \
  --image={region}-docker.pkg.dev/{deploy_project}/{template_repo_name}/direct:latest \
  --service-account={service_account_email} \
  --no-allow-unauthenticated \
  --memory=6Gi \
  --cpu=4 \
  --timeout=3600 \
  --concurrency=1 \
  --args="--config=gs://{bucket}/{path/to/config.yaml}"

# or via environment variable (easier from the console / YAML)
gcloud run deploy {service_name} \
  ... \
  --set-env-vars="MPIPELINE_CONFIG=gs://{bucket}/{path/to/config.yaml}"
```

`--config` takes precedence over `MPIPELINE_CONFIG`. The value accepts every form
`Config.load` resolves: a GCS path, a Parameter Manager parameter version, `ar://…`,
`data:` base64, a local file path (e.g. a mounted Secret Manager or GCS volume), or the raw
config text itself.

* `--concurrency=1`: DirectRunner holds pipeline data in memory; let Cloud Run scale out
  instead of running pipelines concurrently in one instance. An instance already running a
  pipeline answers additional `/run` requests with `429`.
* `--timeout`: covers the whole synchronous run — size it to your batch runtime
  (Cloud Run's maximum is 60 minutes; longer batches belong on Cloud Run Jobs).
* `--no-allow-unauthenticated`: always require IAM (`roles/run.invoker`) — see
  [Security](#security).
* `--memory` / `--cpu`: same sizing guidance as
  [Cloud Run Jobs](cloud-run-jobs.md#create-the-job) (the JVM heap scales to 75% of
  container memory).

### Without a fixed config

Deployed with neither `--config` nor `MPIPELINE_CONFIG`, each request supplies the config
itself: `?config={resource}` as a query parameter, or the config text as the request body.
This is flexible for development, but the endpoint then executes arbitrary configs with the
service account's permissions — see [Security](#security).

## Endpoints

| endpoint | description |
| --- | --- |
| `GET /healthz` | liveness/startup probe, returns `200 ok` |
| `POST /run` | run the pipeline once, synchronously |

### `POST /run`

| input | meaning |
| --- | --- |
| query `args.xxx={value}` | template args filling `${args.xxx}` placeholders (same as the CLI's `--args.xxx`) |
| query `config={resource}` | config to run (only honored without a fixed config) |
| query `context`, `format` | optional overrides of the config context / format |
| request body | **data** for the [request source module](../module/source/request.md) when a config is fixed (or given via `?config=`); otherwise the **config text** itself |

A [Pub/Sub push](https://cloud.google.com/pubsub/docs/push) envelope is unwrapped
transparently: `message.data` (base64) becomes the effective body and `message.attributes`
become template args, so a push subscription pointing at `/run` drives the pipeline directly.

Response (the HTTP status mirrors the pipeline result — `200` on `DONE`, `500` on failure,
`400` for config/body errors, `429` while another run occupies the instance):

```json
{"state": "DONE", "startedAt": "...", "finishedAt": "...", "durationMillis": 1234}
```

### Example

```sh
curl -X POST "https://{service}.run.app/run?args.targetDate=2026-08-19" \
  -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  -d '[{"userId":"u1","amount":100}]'
```

## Environment variables

| variable | description |
| --- | --- |
| `MPIPELINE_CONFIG` | fixed config (any `Config.load` form); `--config` in container args wins |
| `MPIPELINE_CONFIG_RELOAD` | `true` re-fetches the config resource on every request (dev convenience — config changes on GCS apply without a new revision). Default: fetch once at startup, so behavior is revision-stable |
| `MPIPELINE_MAX_CONCURRENCY` | max pipelines running concurrently in one instance (default `1`; excess requests get `429`) |

## Security

* **Always deploy with `--no-allow-unauthenticated`** and grant `roles/run.invoker` to the
  callers (Cloud Scheduler / Pub/Sub push service accounts, users). The endpoint runs
  pipelines with the service's service account.
* Prefer the **fixed config** deployment: the endpoint then only accepts data and template
  args for a predefined pipeline, never an arbitrary config.
* Without a fixed config, anyone allowed to invoke the service can execute any config with
  the service account's permissions — restrict invoker bindings accordingly.

## Notes

* Streaming (unbounded) configs never finish within a request — run those on
  [Worker Pools](cloud-run-worker-pools.md) or Dataflow. Serve mode is for bounded batch
  pipelines.
* The synchronous model works with Cloud Run's default request-based billing: all CPU is
  consumed during the request.
* On shutdown (SIGTERM), the server stops accepting requests; in-flight runs get the
  Cloud Run termination grace period.
* The `direct` image entrypoint flags (`--enforceImmutability=false`,
  `--enforceEncodability=false`) apply to serve-mode runs as well; re-enable them per config
  via `options.direct.*` as described in [Cloud Run Jobs — Notes](cloud-run-jobs.md#notes).
