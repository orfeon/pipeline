---
type: Action Module
title: Cloud Build Action Module
description: Cloud Build operations as workflow steps (Cloud Build REST API v1). Runs a build — a full Build resource (steps / source / substitutions / options / serviceAccount) or the image + script shorthand for a one-step container run — and waits for it, returning the Build resource plus the decoded step outputs (results.buildStepOutputs) as payload; reads or lists builds as a guard (failWhen / skipWhen — is the latest build of a trigger green), waits for builds started elsewhere (buildId, filter, or jobIdField with collect), cancels a build, and runs a build trigger (triggers.run with a RepoSource). Idempotent on retry via deterministic tags (reuseExisting adopts a queued / working / succeeded build with the same tags). Runs from any runner — a generic "run this container once" step without pre-registering a Cloud Run job.
tags: [action, build, cloudbuild, ci, cd, container, script, trigger, wait, cancel, guard, orchestration, workflow, gcp]
timestamp: 2026-08-28T00:00:00Z
---

# Cloud Build Action Module

Action module (`actions` section, `module: build`) for [Cloud Build](https://cloud.google.com/build/docs/api/reference/rest) operations. It lets a pipeline run arbitrary container work as a build (a script in any image, a multi-step build, a trigger), wait for it and use its outputs, or gate on the state of builds. See [action modules](README.md) for the `actions` section, trigger semantics and the output envelope.

Typical uses:

- **Run a script / container once from the pipeline**: after a [bigquery](bigquery.md) load, run `dbt run`, `terraform apply`, a `bq` / `gcloud` command or a validation script in any image with the `image` + `script` shorthand — no Cloud Run job to register first. The step's `$BUILDER_OUTPUT/output` comes back as `payload.outputs[]` for `failWhen` or a later [http](http.md) notification.
- **Artifacts → CI/CD**: once a storage sink has written a model / dataset / site content, run a build trigger (`triggers.run`) or a `builds.create` with `source.storageSource`, then continue with a [dataflow](dataflow.md) launch or a deploy call.
- **Guard / wait**: `builds.list` (`filter: build_trigger_id="…" AND status="SUCCESS"`) + `failWhen` — do not deploy unless the latest build is green; `builds.wait` with a `filter` to wait for a build started by a git push before running smoke tests.
- **Fan-out**: one build per element (`trigger: perElement`, `wait: false`, deterministic `tags`), then a single `builds.wait` step (`trigger: collect`, `jobIdField`).

## Operations

`operation` is a module-level field (next to `name` / `module`), named after the REST API `resource.method`.

| operation | effect | state in envelope | idempotent on bundle retry |
|---|---|---|---|
| `builds.create` | Run a [`Build`](https://cloud.google.com/build/docs/api/reference/rest/v1/projects.builds#Build) and, by default, wait for it. | The build's `status` after the wait (`SUCCESS`, `WORKING`, …); with `wait: false` the status right after creation (`QUEUED`), or `EXISTS` when a build with the same `tags` was adopted (`payload.adopted = true` in both cases). | with deterministic `tags` (see below) |
| `builds.get` | Read one build (`buildId`). | `status` | yes |
| `builds.list` | List builds newest first (`filter`, `pageSize`). Payload: `builds[]`, `count`, `firstBuild`. | `DONE` | yes |
| `builds.wait` | Wait for builds started elsewhere: `buildId`, or `filter` (the newest matching build; payload: the `Build`, state = `status`), or with `trigger: collect` every id in `jobIdField` in one poll loop (payload: always `builds[]` / `count` / `firstBuild`, state `DONE`). | see left | yes |
| `builds.cancel` | [Cancel](https://cloud.google.com/build/docs/api/reference/rest/v1/projects.locations.builds/cancel) a build (`buildId`), by default waiting until it is `CANCELLED`. | `status` | yes |
| `triggers.run` | [Run a build trigger](https://cloud.google.com/build/docs/api/reference/rest/v1/projects.locations.triggers/run) (`triggerId`, optional `source` = [`RepoSource`](https://cloud.google.com/build/docs/api/reference/rest/v1/RepoSource) with `branchName` / `tagName` / `commitSha` / `substitutions`) and, by default, wait for the build. | like `builds.create` | no (a run always starts a build) |

`jobId` in the envelope is the build id (comma-joined for a multi-build `builds.wait` / `builds.list`).

### Idempotency and failure handling

Cloud Build has no unique build name and no client-supplied request id, so a retried bundle would start a second build. `builds.create` therefore dedupes on **tags**: with `reuseExisting` (default `true`) and non-empty `tags`, it first looks up the **newest** build carrying every tag (`tags="a" AND tags="b"`) and adopts it when it is `PENDING` / `QUEUED` / `WORKING` / `SUCCESS` (`payload.adopted = true`). If the newest one failed, was cancelled or expired, a new build runs (an older success behind it does not count). An adopted build is not cancelled on timeout (it is not ours). Make the tags deterministic (`report-${args.run_id}-${tenant}`) — the check is best-effort (a list → create race can still duplicate). Without `tags` the step logs a WARN and is not idempotent.

| situation | behaviour |
|---|---|
| Build ends `FAILURE` / `INTERNAL_ERROR` / `TIMEOUT` / `EXPIRED` | Non-retryable failure; `statusDetail`, `failureInfo` and `logUrl` are attached to the failure description. |
| Build is `CANCELLED` by someone else while waited for | Non-retryable failure (`builds.cancel` itself waits for `CANCELLED` successfully). |
| `timeoutSeconds` exceeded | Non-retryable failure; a build this step started is cancelled first when `cancelOnTimeout` is true (default for `builds.create` / `triggers.run`); adopted or merely waited-for builds are never cancelled. |
| Rejected request (400, 401, 403, 404, 409, 412) | Non-retryable — re-execution cannot fix it. |
| Transient API errors (429, 5xx) on create / run / cancel | Retryable — use the module-level `retry`. While waiting, transient poll errors are retried inside the wait (the firing is not re-run, so no second build is started). |

A build that requires [approval](https://cloud.google.com/build/docs/securing-builds/gate-builds-on-approval) stays `PENDING` (`payload.approval.state = PENDING`) until someone approves it; `waitUntil: terminal` waits through that, so set `timeoutSeconds` accordingly or use `wait: false` + `failWhen: payload.approval.state = 'PENDING'`.

Waiting happens inside the action's DoFn (the same model as the [dataflow](dataflow.md) action): a `perElement` create with `wait: true` occupies one worker thread per element and elements of the same bundle wait one after another. For fan-out prefer `wait: false` + a `collect` `builds.wait` (example below). Concurrency is bounded by the project's concurrent-builds quota (builds beyond it wait `QUEUED`; `queueTtl` bounds the queue time, after which the build ends `EXPIRED`).

## Parameters

### Common

| parameter | optional | type | description |
|---|---|---|---|
| projectId | optional | String | Project of the build. Default: the pipeline's project. Template allowed. |
| location | optional | String | Cloud Build location (`global`, `asia-northeast1`, …). Default `global`. A private pool (`options.pool`) requires its region. Template allowed. |
| buildId | conditionally required | String | Target build for `builds.get` / `builds.wait` / `builds.cancel`. Template allowed, e.g. `${jobId}` from a create envelope. |
| filter | conditionally required | String | `builds.list`: the [list filter](https://cloud.google.com/build/docs/view-build-results#filtering_build_results_using_queries) (`status="SUCCESS"`, `tags="nightly"`, `build_trigger_id="…"`, `create_time>="…"`, joined with `AND`); `builds.wait`: polls until a build matches (it may not exist yet), then waits for the newest match — bound it with `create_time>=` when older builds could match. Template allowed. |
| pageSize | optional | Integer | `builds.list`: max builds returned. Default `100`. |
| jobIdField | optional | String | `builds.wait` with `trigger: collect`: field of the collected elements holding build ids (`jobId` of create envelopes). |
| wait | optional | Boolean | `builds.create` / `triggers.run` / `builds.cancel`: wait for the build. Default `true`. |
| waitUntil | optional | Enum | `terminal` (default: SUCCESS / FAILURE / INTERNAL_ERROR / TIMEOUT / CANCELLED / EXPIRED), `working` (the build left the queue: WORKING, or terminal), `none` (report the current status without waiting). |
| timeoutSeconds | optional | Long | Wait deadline; exceeding it fails the step. Default `86400`. Distinct from the build's own `timeout` (Cloud Build's limit for the build, default 10 minutes). |
| cancelOnTimeout | optional | Boolean | Cancel the build when the deadline passes. Default `true` for `builds.create` / `triggers.run`, `false` otherwise (a build started elsewhere is not ours to cancel). |
| endpoint | optional | String | Custom REST endpoint for tests. |

### builds.create

The `Build` resource fields are written at the top level of `parameters` with their REST names: `steps`, `source`, `images`, `artifacts`, `substitutions`, `options`, `timeout`, `queueTtl`, `serviceAccount`, `tags`, `logsBucket`, `availableSecrets`, `secrets`. Every string inside them is a template (with `trigger: perElement`, `${field}` of the element).

**Templates vs Cloud Build substitutions**: both use `${…}`. Only expressions whose first identifier is a template variable (an input field, `args`, `size`, …) are expanded by the pipeline; anything else — Cloud Build substitutions (`${PROJECT_ID}`, `${_TAG}`) and shell expansions (`${BUILDER_OUTPUT}`) — is passed to Cloud Build verbatim, so idiomatic `cloudbuild.yaml` content can be pasted as is. Do not name an input field like a Cloud Build substitution.

On top, the one-step shorthand:

| parameter | optional | type | description |
|---|---|---|---|
| image | conditionally required | String | Shorthand: the image the `script` runs in (`python:3.12`, `gcr.io/cloud-builders/gcloud`, `hashicorp/terraform`, an Artifact Registry image, …). Template allowed. |
| script | conditionally required | String | Shorthand: a shell script run as the single build step (`steps: [{name: image, script: script, env: […], automapSubstitutions: true}]`). Exclusive with `steps`. Substitutions are available as environment variables (`$_TENANT`); write results to `$BUILDER_OUTPUT/output` (first 50 KB) to get them back as `payload.outputs[]`. Template allowed. |
| env | optional | Map<String,String\> | Shorthand: environment variables of the step (values template-able). |
| steps | conditionally required | Array<[BuildStep](https://cloud.google.com/build/docs/api/reference/rest/v1/projects.builds#BuildStep)\> | The build steps (`name`, `args`, `env`, `dir`, `id`, `waitFor`, `entrypoint`, `script`, `secretEnv`, `volumes`, `timeout`, `allowFailure`, `allowExitCodes`, `automapSubstitutions`). |
| source | optional | [Source](https://cloud.google.com/build/docs/api/reference/rest/v1/projects.builds#Source) | `storageSource` (`bucket`, `object` — a tar.gz / zip archive, optional `generation`), `repoSource`, `gitSource`, `connectedRepository`, `developerConnectConfig`. Omit for steps that need no source. |
| substitutions | optional | Map<String,String\> | User-defined substitutions; keys must start with `_`. |
| options | optional | [BuildOptions](https://cloud.google.com/build/docs/api/reference/rest/v1/projects.builds#BuildOptions) | `machineType` (`E2_HIGHCPU_8` / `E2_HIGHCPU_32` / `E2_MEDIUM` / …), `diskSizeGb`, `pool.name` (private pool; requires a regional `location`), `logging` (`CLOUD_LOGGING_ONLY` / `GCS_ONLY` / `NONE`), `env`, `secretEnv`, `volumes`, `automapSubstitutions`, `defaultLogsBucketBehavior`, … |
| timeout / queueTtl | optional | Duration string | `timeout`: the build's time limit (`1800s`, default 10 minutes, max 24 hours). `queueTtl`: how long the build may wait in the queue before `EXPIRED`. |
| serviceAccount | optional | String | `projects/{project}/serviceAccounts/{email}` the build runs as (default: the Cloud Build default service account). A user-specified service account requires `options.logging` (`CLOUD_LOGGING_ONLY` / `GCS_ONLY` / `NONE`) or `logsBucket`. |
| tags | optional | List<String\> | Build tags — also the idempotency key (see above). Template allowed. |
| images / artifacts / logsBucket / availableSecrets / secrets | optional | as in the REST resource | Passed through unchanged. |
| reuseExisting | optional | Boolean | Adopt a queued / working / succeeded build carrying all of `tags` instead of creating a new one. Default `true`. |

### triggers.run

| parameter | optional | type | description |
|---|---|---|---|
| triggerId | required | String | The trigger's id or name. Template allowed. |
| source | optional | [RepoSource](https://cloud.google.com/build/docs/api/reference/rest/v1/RepoSource) | `branchName` / `tagName` / `commitSha` (one of them), `substitutions`, `projectId`, `repoName`, `dir`. Omitted: the trigger's configured source. |

The executing service account needs `roles/cloudbuild.builds.editor` (create / cancel / run trigger; `roles/cloudbuild.builds.viewer` for get / list / wait) and `iam.serviceAccounts.actAs` on the build's service account when `serviceAccount` is set.

## Payload

The payload is the [`Build`](https://cloud.google.com/build/docs/api/reference/rest/v1/projects.builds#Build) resource as returned by the API: `id`, `status`, `statusDetail`, `createTime` / `startTime` / `finishTime`, `logUrl`, `results` (`images[]` with `digest`, `buildStepImages`, `numArtifacts`, `buildStepOutputs`), `failureInfo` (`type`, `detail`), `approval`, `substitutions`, `tags`, `buildTriggerId`, … — the API reference is the dictionary for `failWhen` / `skipWhen` paths, e.g. ``failWhen: payload.`results`.`numArtifacts` = '0'`` (int64 fields are strings in the REST JSON). Added by this module:

- `outputs[]` — `results.buildStepOutputs` decoded from base64 (one entry per step, in step order); an entry that is a JSON object is parsed.
- `output` — the first non-empty entry of `outputs[]`, for conditions (which have no array index syntax): ``failWhen: payload.`output`.`rows` = 0``.
- `adopted` — `true` when `builds.create` adopted an existing build.

`builds.list` and a collected `builds.wait` wrap the builds as `builds[]` / `count` / `firstBuild`.

## Notes

- A pipeline has no try/finally: a trailing cleanup build does not run when an earlier step fails.
- `availableSecrets` / `secretEnv` reference Secret Manager versions — never put secret values in the config.
- `triggers.run` with a `RepoSource` targets the trigger's repository; for 2nd-gen (connected) repositories `commitSha` is the reliable revision selector.
- Cloud Build logs are not read by this module; `payload.logUrl` links to them.

## Examples

### Run a script after a load, gate on its output

```yaml
actions:
  - name: load
    module: bigquery
    operation: jobs.load
    waits: [store]
    parameters: { sourceUris: [gs://mybucket/export/*.avro], destinationTable: myproject.app.events }
  - name: dbt
    module: build
    operation: builds.create
    waits: [load]
    failWhen: payload.`output`.`failed` > 0
    parameters:
      projectId: myproject
      location: asia-northeast1
      tags: ["dbt-${args.run_id}"]                     # deterministic: a retried bundle adopts the running build
      image: ghcr.io/dbt-labs/dbt-bigquery:1.9.latest
      script: |
        dbt run --profiles-dir . --target prod
        echo "{\"failed\": $(grep -c ERROR logs/dbt.log || true)}" > $BUILDER_OUTPUT/output
      source: { storageSource: { bucket: mybucket, object: dbt/project.tar.gz } }
      options: { machineType: E2_HIGHCPU_8, logging: CLOUD_LOGGING_ONLY }
      serviceAccount: projects/myproject/serviceAccounts/dbt@myproject.iam.gserviceaccount.com
      timeout: 1800s
```

### One build per tenant, wait for all, then publish

```yaml
sources:
  - name: tenants
    module: bigquery
    parameters:
      query: SELECT tenant_id FROM `myproject.app.tenants` WHERE active
actions:
  - name: render
    module: build
    operation: builds.create
    trigger: perElement
    inputs: [tenants]
    parameters:
      location: asia-northeast1
      tags: ["report-${args.run_id}-${tenant_id}"]
      substitutions: { _TENANT: "${tenant_id}", _RUN: "${args.run_id}" }
      image: asia-northeast1-docker.pkg.dev/myproject/tools/report:latest
      script: python render.py --tenant $_TENANT --out gs://mybucket/reports/$_RUN/$_TENANT.pdf
      queueTtl: 3600s
      wait: false                                       # create only; one step below waits for all
  - name: render_done
    module: build
    operation: builds.wait
    trigger: collect
    inputs: [render]
    parameters:
      location: asia-northeast1
      jobIdField: jobId
      timeoutSeconds: 7200
  - name: publish
    module: build
    operation: triggers.run
    waits: [render_done]
    parameters:
      location: asia-northeast1
      triggerId: site-deploy
      source: { branchName: main, substitutions: { _RUN: "${args.run_id}" } }
```

### Guard: deploy only when the trigger's latest build is green

```yaml
actions:
  - name: main_green
    module: build
    operation: builds.list
    failWhen: payload.`count` = 0
    parameters:
      location: asia-northeast1
      filter: build_trigger_id="0123abcd-…" AND status="SUCCESS" AND create_time>="2026-08-28T00:00:00Z"
      pageSize: 1
  - name: deploy
    module: http
    waits: [main_green]
    parameters: { url: "https://deploy.example.com/hooks/prod", method: POST }
```

### Wait for a build started by a git push, then run smoke tests

```yaml
actions:
  - name: image_built
    module: build
    operation: builds.wait
    parameters:
      location: asia-northeast1
        filter: build_trigger_id="0123abcd-…" AND create_time>="${args.pushed_at}"   # bound it: the previous build must not match
      timeoutSeconds: 1800
  - name: smoke
    module: build
    operation: builds.create
    waits: [image_built]
    parameters:
      location: asia-northeast1
      tags: ["smoke-${args.commit}"]
      image: asia-northeast1-docker.pkg.dev/myproject/app/api:${args.commit}
      script: pytest tests/smoke
```
