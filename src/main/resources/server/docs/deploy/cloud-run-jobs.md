# Run Pipeline on Cloud Run Jobs (DirectRunner)

The container image built with the `direct` profile
(see [Deploy Direct Runner](README.md#deploy-direct-runner-for-local-execution))
can be executed as a [Cloud Run Job](https://cloud.google.com/run/docs/create-jobs).
This is a lightweight way to run small to medium batch pipelines on a schedule or on demand,
without launching a Dataflow job.

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

* `--memory`: the image's entrypoint fixes the JVM heap at `-Xmx4096m`, so allocate at least
  5GiB (6GiB recommended) or the task will be OOM-killed. Changing the heap size requires
  rebuilding the image (the value is set in the `direct` profile's jib entrypoint in `pom.xml`).
* `--task-timeout`: the Cloud Run default is 10 minutes; raise it to cover your batch runtime.
* `--cpu`: DirectRunner parallelism defaults to the number of available cores
  (tune with the `options.direct.targetParallelism` config option).
* `--max-retries`: the process exits non-zero when the pipeline fails, which triggers
  Cloud Run's task retry. Set it to 0 unless the pipeline is safe to re-run (e.g. idempotent sinks).

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
