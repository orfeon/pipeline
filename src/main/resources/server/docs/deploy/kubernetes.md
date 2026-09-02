# Run Pipeline on Kubernetes (DirectRunner)

The container image built with the `direct` profile
(see [Deploy Direct Runner](README.md#deploy-direct-runner-for-local-execution))
is a plain container — an entrypoint running `MPipeline` with `--runner=DirectRunner` that
takes pipeline arguments as container args — so it runs on any Kubernetes cluster (GKE or
elsewhere). The Cloud Run forms map directly onto Kubernetes workload kinds:

| use case | Cloud Run | Kubernetes |
| --- | --- | --- |
| one-shot / scheduled batch | [Jobs](cloud-run-jobs.md) | `Job` / `CronJob` |
| streaming, config-queue worker | [Worker Pools](cloud-run-worker-pools.md) | `Deployment` |
| HTTP-triggered runs ([serve mode](cloud-run-service.md)) | [Services](cloud-run-service.md) | `Deployment` + `Service` |

## Batch: Job / CronJob

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: mpipeline-batch
spec:
  backoffLimit: 0            # the exit code reflects the pipeline result; retry only if idempotent
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: pipeline
          image: {region}-docker.pkg.dev/{project}/{repo}/direct:latest
          args:
            - --config=gs://{bucket}/{path/to/config.yaml}
            - --args.targetDate=2026-08-20
          resources:
            requests: { memory: 6Gi, cpu: "4" }
            limits: { memory: 6Gi }
```

For scheduled runs wrap the same pod template in a `CronJob` (`spec.schedule: "0 3 * * *"`,
`concurrencyPolicy: Forbid` unless overlapping runs are safe).

## Streaming / config-queue worker: Deployment

A `Deployment` with `restartPolicy: Always` (the default) reproduces the
[Worker Pools](cloud-run-worker-pools.md) patterns:

* fixed streaming config (`args: [--config=gs://.../streaming_config.yaml]`) — the process
  runs indefinitely; `replicas` scales horizontally for Pub/Sub-source pipelines (Pub/Sub
  distributes messages across subscribers), while sources without a work-sharing mechanism
  should stay at `replicas: 1`;
* config-queue worker (`args: [--config=projects/{project}/subscriptions/{subscription}]`) —
  each container run pulls one config message, runs it, and exits; kubelet restarts the
  container, which pulls the next message. An idle queue produces a fast exit/restart loop —
  expect `CrashLoopBackOff`-style backoff delays between polls on an empty queue.

## HTTP serve mode: Deployment + Service

Serve mode activates when the `PORT` environment variable is set (on Cloud Run, Services set
it automatically; on Kubernetes set it yourself) or with an explicit `--serve=true` arg:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mpipeline-serve
spec:
  replicas: 1
  selector: { matchLabels: { app: mpipeline-serve } }
  template:
    metadata:
      labels: { app: mpipeline-serve }
    spec:
      containers:
        - name: pipeline
          image: {region}-docker.pkg.dev/{project}/{repo}/direct:latest
          env:
            - name: PORT
              value: "8080"
            - name: MPIPELINE_CONFIG
              value: gs://{bucket}/{path/to/config.yaml}
          ports:
            - containerPort: 8080
          readinessProbe:
            httpGet: { path: /healthz, port: 8080 }
          resources:
            requests: { memory: 6Gi, cpu: "4" }
            limits: { memory: 6Gi }
---
apiVersion: v1
kind: Service
metadata:
  name: mpipeline-serve
spec:
  selector: { app: mpipeline-serve }
  ports:
    - port: 80
      targetPort: 8080
```

The endpoints, fixed-config resolution (`MPIPELINE_CONFIG` env or `--config` arg — a config
mounted from a `ConfigMap`/`Secret` volume works too, since local file paths are accepted)
and the `MPIPELINE_*` environment variables are documented in
[Run Pipeline on Cloud Run Services](cloud-run-service.md). Runs execute synchronously in the
request, so one pipeline runs per replica at a time (extra requests get `429`) — scale with
`replicas`. Unlike Cloud Run there is no platform auth in front: keep the Service internal
(`ClusterIP`) or put authentication at your ingress.

## GCP credentials and sizing

* **GKE**: use [Workload Identity Federation](https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity)
  to bind the pod's Kubernetes ServiceAccount to a Google service account with access to the
  config and to every source/sink the pipeline touches.
* **Outside Google Cloud**: mount a service account key as a Secret and set
  `GOOGLE_APPLICATION_CREDENTIALS` to its path; there is no metadata server, so also set
  `GOOGLE_CLOUD_PROJECT`.
* **Memory**: the entrypoint sets `-XX:MaxRAMPercentage=75.0`, so the JVM heap follows the
  container memory **limit** — always set `resources.limits.memory`, sized to your data
  volume (DirectRunner holds pipeline data in memory), and keep request = limit to avoid
  overcommit evictions.
* The entrypoint flag notes in [Cloud Run Jobs — Notes](cloud-run-jobs.md#notes)
  (`--enforceImmutability` etc.) apply unchanged.
