# Run Pipeline on Cloud Run Jobs (DirectRunner / Prism)

The container image built with the `direct` profile
(see [Deploy Direct Runner](README.md#deploy-direct-runner-for-local-execution))
can be executed as a [Cloud Run Job](https://cloud.google.com/run/docs/create-jobs).
This is a lightweight way to run small to medium batch pipelines on a schedule or on demand,
without launching a Dataflow job.
(The same image also runs as a [Cloud Run Worker Pool](cloud-run-worker-pools.md) for
streaming/queue workloads, as an HTTP-triggered
[Cloud Run Service](cloud-run-service.md), and on [Kubernetes](kubernetes.md).)

The image's entrypoint already launches `com.mercari.solution.MPipeline` with
`--runner=DirectRunner`, so the job only needs to supply the pipeline arguments
(`--config=...` and optional `--args.*=...`) as container arguments.

## Prerequisites

* The `direct` profile image is pushed to Artifact Registry.
* A service account for the job with permissions to:
  * read the config file (e.g. `roles/storage.objectViewer` for a config on GCS), and
  * access every source/sink the pipeline touches (BigQuery, Spanner, Pub/Sub, ...).

## Create the job

```sh
gcloud run jobs create {job_name} \
  --project={project} \
  --region={region} \
  --image={region}-docker.pkg.dev/{deploy_project}/{template_repo_name}/direct:latest \
  --service-account={service_account_email} \
  --memory=6Gi \
  --cpu=4 \
  --task-timeout=3600 \
  --max-retries=0 \
  --args="--config=gs://{bucket}/{path/to/config.yaml}"
```

* `--memory`: the JVM heap scales to 75% of the container memory
  (`-XX:MaxRAMPercentage=75.0` in the image entrypoint), the rest being left for metaspace,
  thread stacks and gRPC direct buffers. Size `--memory` to your data volume — DirectRunner
  holds pipeline data in memory (e.g. `--memory=6Gi` gives a ~4.5GiB heap).
* `--task-timeout`: the Cloud Run default is 10 minutes; raise it to cover your batch runtime.
* `--cpu`: DirectRunner parallelism defaults to the number of available cores
  (tune with the `options.direct.targetParallelism` config option).
* `--max-retries`: the process exits non-zero when the pipeline fails, which triggers
  Cloud Run's task retry. Set it to 0 unless the pipeline is safe to re-run (e.g. idempotent sinks).

## Running with the Prism image

The job definition is identical with the `prism` profile image (Beam's portable local runner —
see [Deploy Prism Runner](README.md#deploy-prism-runner-for-local--cloud-run-execution)): swap
`--image` and keep the arguments. Prefer it when the pipeline has heavy keyed stages (GroupByKey over
coarse or global keys), which DirectRunner slows down on by orders of magnitude.

* The container downloads the prism binary from the Beam GitHub release at startup, so the job needs
  outbound network access (or set `options.prism.prismLocation`).
* Prism executes in memory (no disk spill): size `--memory` to the pipeline's working set — it grows
  roughly linearly with the input, and a sudden loss of every gRPC channel early in the run is the
  prism process being OOM-killed. Data beyond the 32 GiB Cloud Run ceiling belongs on Dataflow.
* The process waits for the pipeline result: `Pipeline finished with state: DONE` in the logs marks a
  completed run, and a failed pipeline fails the execution. A `ManagedChannel allocation site` stack
  at shutdown is gRPC's channel-leak detector, not a failure.
* A Cloud Run Job pins the image **digest** when created or updated: after pushing a new image to the
  same tag, run `gcloud run jobs update {job_name} --image=...` again to pick it up.

## Specify the config

The `--config` value accepts the same forms as everywhere else (resolved by `Config.load`):

| Form | Example |
| --- | --- |
| GCS path (recommended) | `gs://{bucket}/{path/to/config.yaml}` |
| Parameter Manager parameter version | `projects/{project}/locations/{location}/parameters/{parameter}/versions/{version}` |
| Artifact Registry path | `ar://...` |
| Base64-encoded config body | `data:eyJzb3VyY2VzIjp7Li4u` |
| Pub/Sub subscription (config text is taken from a message) | `projects/{project}/subscriptions/{subscription}` |

Template placeholders `${args.xxx}` in the config are filled from `--args.xxx={value}`
container arguments (they override the defaults defined in the config's `system.args`):

```sh
  --args="--config=gs://{bucket}/config.yaml,--args.targetDate=2026-08-08"
```

Note that `gcloud` splits the `--args` value on commas. If an argument value itself contains
commas (e.g. inline JSON), switch the delimiter with the `^;^` prefix:

```sh
  --args="^;^--config=gs://{bucket}/config.yaml;--args.filter={\"a\":1,\"b\":2}"
```

## Execute

```sh
gcloud run jobs execute {job_name} --project={project} --region={region} --wait
```

To change arguments per execution (e.g. a date parameter), use container overrides
instead of updating the job:

```sh
gcloud run jobs execute {job_name} \
  --project={project} \
  --region={region} \
  --args="--config=gs://{bucket}/config.yaml,--args.targetDate=2026-08-09" \
  --wait
```

For scheduled runs, trigger the job from
[Cloud Scheduler](https://cloud.google.com/run/docs/execute/jobs-on-schedule).

## Launch from the Pipeline Builder

The Builder UI's **Launch** (runner `Direct`, environment `Cloud Run Job`) executes an existing
job like the `gcloud run jobs execute --args=...` form above: it calls `jobs.run` on the job with
this launch's `--config=...` / `--args.*` as container-argument overrides (plus an optional task
timeout, task count and extra env vars). It never creates, updates or deletes the job — the
image, service account, cpu, memory and network stay whatever you set when you created it, and
every launch is one more execution of the same job.

Create the job once as shown in [Create the job](#create-the-job) (the `--args` given at creation
are replaced per launch, so any placeholder such as `--args="--config=gs://bucket/placeholder.yaml"`
will do), then tell the server which job to run:

```sh
MERCARI_PIPELINE_LAUNCH_DIRECT_JOB={job_name}
# project / region default to the ones the server itself runs in; override with
MERCARI_PIPELINE_LAUNCH_PROJECT={project}
MERCARI_PIPELINE_LAUNCH_REGION={region}
# optional: stage configs to GCS instead of passing them inline (needed above ~24KB)
MERCARI_PIPELINE_LAUNCH_STAGING_LOCATION=gs://{bucket}/{prefix}
```

The modal shows these as the form's defaults; a different job name, project or region can be
typed per launch. The server's service account needs `roles/run.invoker` on the job (and the
job's own service account needs read access to the staging bucket if one is configured). The
full list of variables and roles is in [server.md](server.md).

## Notes

* The `direct` image disables DirectRunner's per-element enforcement checks by default
  (`--enforceImmutability=false`, `--enforceEncodability=false` in the entrypoint).
  To re-enable them for a run, set the config options instead of passing the flags again —
  duplicating a flag already present in the entrypoint fails argument parsing:

  ```yaml
  options:
    direct:
      enforceImmutability: true
      enforceEncodability: true
  ```

* The pipeline process blocks until completion (`blockOnRun` defaults to `true`) and the
  exit code reflects the pipeline result, so Cloud Run's success/failure status and retry
  semantics work as-is.
* The GCP project is resolved from the metadata server; setting `GOOGLE_CLOUD_PROJECT` is
  usually unnecessary.
