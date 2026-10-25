# Dataflow Options

Dataflow runner options

Dataflow config parameters can be specified as [pipeline options](https://cloud.google.com/dataflow/docs/reference/pipeline-options).
If you wish to define these parameters in the config file, use this option.

## Job and location

| parameter | type | description |
|---|---|---|
| project | String | The Google Cloud project the job runs in. Also sets `gcp.project` (see [GCP Options](gcp.md) for how it is resolved when unset). |
| region | String | The [regional endpoint](https://cloud.google.com/dataflow/docs/concepts/regional-endpoints) the job is deployed to and managed from, e.g. `asia-northeast1`. |
| workerRegion | String | Compute Engine region to run the workers in, when it differs from `region`. Cannot be combined with `workerZone`. |
| workerZone | String | Compute Engine zone to run the workers in. Cannot be combined with `workerRegion`. |
| tempLocation | String | Cloud Storage path for temporary files. Must be a valid Cloud Storage URL, beginning with `gs://BUCKET-NAME/` |
| stagingLocation | String | Cloud Storage path for staging local files. Must be a valid Cloud Storage URL, beginning with `gs://BUCKET-NAME/`. If not set, defaults to what you specified for `tempLocation` |
| labels | Map<String,String\> | User-defined [labels](https://cloud.google.com/resource-manager/docs/creating-managing-labels), also known as additional-user-labels. User-specified labels are available in billing exports, which you can use for cost attribution. |

The job name is the top-level `jobName` option (see [Options](README.md)).

## Workers and autoscaling

| parameter | type | description |
|---|---|---|
| autoscalingAlgorithm | Enum | The autoscaling mode for Dataflow job. Possible values are `THROUGHPUT_BASED` to enable autoscaling, or `NONE` to disable. The default autoscaler judges a batch job by its currently running fused stage and can scale *down* right before a wide fan-out (e.g. the `feature` transform's parallel waves); for such jobs — and for any measurement — pass `NONE` with `numWorkers` = `maxNumWorkers` to hold the pool. |
| flexRSGoal | Enum | Specify Flexible Resource Scheduling (FlexRS) for autoscaled batch jobs. `COST_OPTIMIZED`, where the cost-optimized goal means that the Dataflow service chooses any available discounted resources. `SPEED_OPTIMIZED`, which is the same as omitting this option. |
| numWorkers | Integer | The initial number of Google Compute Engine instances for the job. |
| maxNumWorkers | Integer | The maximum number of Google Compute Engine instances to be made available to your pipeline during execution, from 1 to 1000. A lower bound for the autoscaler is set with the service option `min_num_workers=N` in `dataflowServiceOptions`. |
| workerMachineType | String | The [machine type](https://cloud.google.com/compute/docs/machine-types) to use for the job. Defaults to the value from the template if not specified. |
| workerDiskType | String | The type of Persistent Disk to use, specified by a full URL of the disk type resource. For example, use `compute.googleapis.com/projects/PROJECT/zones/ZONE/diskTypes/pd-ssd` to specify an SSD Persistent Disk. When using Streaming Engine, do not specify a Persistent Disk. Supported values: `pd-ssd`, `pd-standard`. |
| diskSizeGb | Integer | Worker disk size, in gigabytes. |
| numberOfWorkerHarnessThreads | Integer | The number of threads per worker harness process. Lower it for memory-heavy DoFns (fewer elements in flight per worker), raise it for I/O-bound DoFns. |
| workerCacheMb | Integer | Size in MB of the worker cache for side inputs (legacy, non Runner v2 worker). See also [`sdkHarness.maxCacheMemoryUsageMb`](sdkharness.md). |
| sdkContainerImage | String | Custom SDK container image for the workers (Runner v2), e.g. an image with extra native libraries. |
| sdkHarnessContainerImageOverrides | String | Overrides of SDK harness container images, as comma-separated `regex,image` pairs (cross-language pipelines). |

## Security and networking

| parameter | type | description |
|---|---|---|
| serviceAccount | String | Email of the [worker service account](https://cloud.google.com/dataflow/docs/concepts/security-and-permissions#worker-service-account) the workers run as. Default: the Compute Engine default service account. |
| impersonateServiceAccount | String | Service account (or comma-separated delegation chain) to impersonate when submitting the job. Same as `gcp.impersonateServiceAccount`. |
| network | String | Compute Engine network for the workers (default `default`). |
| subnetwork | String | The Compute Engine subnetwork for launching Compute Engine instances to run your pipeline. Expected format is `regions/REGION/subnetworks/SUBNETWORK` or the fully qualified subnetwork name, beginning with https://..., e.g. `https://www.googleapis.com/compute/alpha/projects/PROJECT/` |
| usePublicIps | Boolean | Specify whether Dataflow workers use external IP addresses. If the value is set to false, Dataflow workers use internal IP addresses for all communication. The default is true |

CMEK (`dataflowKmsKey`) is set in [GCP Options](gcp.md).

## Streaming

| parameter | type | description |
|---|---|---|
| enableStreamingEngine | Boolean | Specify whether Dataflow Streaming Engine is enabled or disabled. The default is true. Only applied to streaming jobs. |
| update | Boolean | Replace the running streaming job that has the same `jobName` with this pipeline ([updating a pipeline](https://cloud.google.com/dataflow/docs/guides/updating-a-pipeline)). When the Beam version changes as well, set the top-level `updateCompatibilityVersion` to the Beam version of the running job. |
| transformNameMapping | Map<String,String\> | With `update`: maps transform names of the running job to their new names (renamed steps), e.g. `{"oldStep": "newStep"}`. |
| createFromSnapshot | String | ID of a [Dataflow snapshot](https://cloud.google.com/dataflow/docs/guides/using-snapshots) to start the streaming job from. |
| streamingSideInputCacheMb | Integer | Size in MB of the streaming side-input cache on each worker. |
| streamingSideInputCacheExpirationMillis | Integer | How long (ms) a side-input value stays in the streaming side-input cache before it is re-read. |

## Service options and experiments

| parameter | type | description |
|---|---|---|
| dataflowServiceOptions | Array<String\> | Specify additional job [modes and configurations](https://cloud.google.com/dataflow/docs/reference/service-options), e.g. `enable_prime`, `min_num_workers=2`, `enable_google_cloud_profiler`. Merged with values given on the command line. |
| experiments | Array<String\> | Enables experimental or pre-GA Dataflow features, e.g. `use_runner_v2`. Merged with values given on the command line. |

## Debugging

| parameter | type | description |
|---|---|---|
| hotKeyLoggingEnabled | Boolean | Log the key of a detected [hot key](https://cloud.google.com/dataflow/docs/guides/common-errors#hot-key-detected) (by default only its presence is reported). The key appears in Cloud Logging, so avoid it when keys are sensitive. |
| dumpHeapOnOOM | Boolean | Write a heap dump when a worker runs out of memory (default false). Set `saveHeapDumpsToGcsPath` to retrieve it. |
| saveHeapDumpsToGcsPath | String | Cloud Storage path (`gs://...`) the heap dumps are uploaded to. Use with `dumpHeapOnOOM`; the worker disk (`diskSizeGb`) must have room for a dump as large as the worker memory. |

Worker log levels are set with [`sdkHarness`](sdkharness.md) (`defaultSdkHarnessLogLevel`, `sdkHarnessLogLevelOverrides`).

## Flex Template launch

| parameter | type | description |
|---|---|---|
| templateLocation | String | Flex Template spec file (`gs://...`) used when the pipeline is launched from the server (`/api/launch`). |
| launcherMachineType | String | Machine type of the Flex Template launcher VM, which assembles the pipeline (raise it for very large configs). |

#### Example

```yaml
options:
  jobName: orders-aggregation
  dataflow:
    region: asia-northeast1
    workerMachineType: n2-highmem-4
    maxNumWorkers: 20
    usePublicIps: false
    subnetwork: regions/asia-northeast1/subnetworks/dataflow
    serviceAccount: dataflow-worker@my-project.iam.gserviceaccount.com
    dataflowServiceOptions:
      - min_num_workers=2
    dumpHeapOnOOM: true
    saveHeapDumpsToGcsPath: gs://my-bucket/heapdumps
  sdkHarness:
    defaultSdkHarnessLogLevel: WARN
```
