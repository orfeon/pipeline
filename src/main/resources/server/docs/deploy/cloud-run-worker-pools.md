# Run Pipeline on Cloud Run Worker Pools (DirectRunner)

The container image built with the `direct` profile
(see [Deploy Direct Runner](README.md#deploy-direct-runner-for-local-execution))
can be deployed as a [Cloud Run Worker Pool](https://cloud.google.com/run/docs/deploy-worker-pools).
Worker pools have no HTTP endpoint and no request timeout — instances just run the container —
which makes them the right Cloud Run form for pipelines that are not request-shaped:

* **streaming pipelines** (Pub/Sub / Kafka sources) that run indefinitely, and
* a **config-queue worker** that processes pipeline configs submitted to a Pub/Sub
  subscription, one after another.

Since worker pools never set the `PORT` environment variable, the image starts in normal batch
mode (never in [HTTP serve mode](cloud-run-service.md)) — the entrypoint runs
`MPipeline` with `--runner=DirectRunner`, exactly as on [Cloud Run Jobs](cloud-run-jobs.md).

## Prerequisites

Same as [Cloud Run Jobs](cloud-run-jobs.md#prerequisites): the `direct` image in Artifact
Registry and a service account with access to the config and to every source/sink the
pipeline touches.

## Pattern 1: streaming pipeline

Deploy with a fixed streaming config. The process runs until the revision is replaced or
scaled to zero:

```sh
gcloud run worker-pools deploy {worker_pool_name} \
  --project={project} \
  --region={region} \
  --image={region}-docker.pkg.dev/{deploy_project}/{template_repo_name}/direct:latest \
  --service-account={service_account_email} \
  --memory=6Gi \
  --cpu=4 \
  --instances=1 \
  --args="--config=gs://{bucket}/{path/to/streaming_config.yaml}"
```

* `--instances`: worker pools use manual scaling; each instance runs an **independent copy of
  the same pipeline**. For a Pub/Sub-source pipeline this is horizontal scaling (Pub/Sub
  distributes messages across the instances' subscribers); for sources without a natural
  work-sharing mechanism, more than one instance means duplicate processing — keep
  `--instances=1`. Set `--instances=0` to pause the worker pool.
* All requested instances are billed as active (instance-based billing) — worker pools are
  always-on. For event-driven pipelines that should scale to zero between events, prefer a
  [Cloud Run Service with a Pub/Sub push subscription](cloud-run-service.md) instead.
* Sizing (`--memory` / `--cpu`, JVM heap at 75% of container memory) follows the same
  guidance as [Cloud Run Jobs](cloud-run-jobs.md#create-the-job).

## Pattern 2: config-queue worker

Point `--config` at a Pub/Sub **subscription**; `Config.load` then takes the config text from
one pulled message per run:

```sh
gcloud run worker-pools deploy {worker_pool_name} \
  ... \
  --args="--config=projects/{project}/subscriptions/{config_subscription}"
```

The lifecycle makes this a simple job queue:

1. the container starts and pulls one message from the subscription,
2. the message body (a config YAML/JSON) is parsed and the pipeline runs to completion,
3. the process exits; Cloud Run maintains the requested instance count and restarts the
   container, which pulls the next message,
4. with no message available, an empty pipeline runs and the process exits immediately —
   the instance idles in this restart loop until work arrives.

Publish a config to run it:

```sh
gcloud pubsub topics publish {config_topic} --message="$(cat config.yaml)"
```

Multiple instances process the queue in parallel (one config per instance at a time).

## Launch from the Pipeline Builder

The Builder UI's **Launch** (runner `Direct`, environment `Cloud Run Worker Pool`) deploys the
direct image as a new worker pool running the current config — the Pattern 1 form above, with
`--config` pointing at the config staged to `MERCARI_PIPELINE_LAUNCH_STAGING_LOCATION` (or inlined
as `data:…`). One launch is one pool; the name defaults to `mp-<config name>-<timestamp>` and an
existing name is only redeployed when *Replace existing* is checked.

```sh
MERCARI_PIPELINE_LAUNCH_DIRECT_IMAGE={region}-docker.pkg.dev/{deploy_project}/{template_repo_name}/direct:latest
# defaults for the form (all overridable per launch):
MERCARI_PIPELINE_LAUNCH_DIRECT_SERVICE_ACCOUNT={service_account_email}   # default: the server's own account
MERCARI_PIPELINE_LAUNCH_DIRECT_CPU=4
MERCARI_PIPELINE_LAUNCH_DIRECT_MEMORY=6Gi
MERCARI_PIPELINE_LAUNCH_DIRECT_INSTANCES=1
```

**The pool is not stopped by the Builder.** It keeps running — and billing — until you delete it
or scale it to zero; the launch result shows the command:

```sh
gcloud run worker-pools delete {worker_pool_name} --project={project} --region={region}
```

The server's service account needs `roles/run.developer` and `roles/iam.serviceAccountUser` on
the pool's service account (see [server.md](server.md#iam-for-the-servers-service-account)).

## Updating

`gcloud run worker-pools update {worker_pool_name} --args=...` (or `deploy` again) creates a
new revision; `--instances` can be changed without a new revision. Note that replacing a
revision stops in-flight pipelines — drain streaming pipelines accordingly.

## Notes

* No HTTP endpoint exists: liveness is the process itself. Pipeline failure exits the process
  non-zero and the container is restarted, which pulls the **next** message.
* The config message is acknowledged when pulled, before the pipeline runs (at-most-once):
  a failed pipeline does not re-run its config automatically. Monitor failures (exit logs,
  `failureSinks`) and republish the config to retry.
* The notes on entrypoint flags and project resolution in
  [Cloud Run Jobs — Notes](cloud-run-jobs.md#notes) apply unchanged.
