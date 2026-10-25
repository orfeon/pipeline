# Flink Options

Flink runner options

These options configure the [Apache Flink runner](https://beam.apache.org/documentation/runners/flink/) for pipeline execution.

## Flink pipeline options

| parameter              | type    | description                                                                                                                                                                                                                                    |
|------------------------|---------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| flinkMaster            | String  | Address of the Flink master (JobManager) where the pipeline will be submitted. Use `[local]` for a local embedded Flink cluster or `[auto]` to let the runner detect the cluster from the environment. Default is `[auto]`.                    |
| parallelism            | Integer | The degree of parallelism to be used when distributing operations onto Flink workers. If not set, the parallelism of the Flink cluster or the default parallelism is used.                                                                     |
| maxParallelism         | Integer | The pipeline-wide maximum degree of parallelism to be used. The maximum parallelism specifies the upper bound for dynamic scaling and the number of key groups used for partitioned state.                                                     |
| allowNonRestoredState  | Boolean | Allow to skip savepoint state that cannot be restored. Set to `true` when evolving a pipeline and removing operators. Default is `false`.                                                                                                      |
| attachedMode           | Boolean | Specify whether the pipeline should be submitted in attached mode (`true`) or detached mode (`false`). In attached mode, the submitting client waits for the job to finish. Default is `true`.                                                 |
| executionModeForBatch  | String  | The execution mode for batch pipelines. Possible values: `PIPELINED` (all operators connected via pipelined data exchanges) or `BATCH` (uses sort-based shuffle with blocking exchanges for improved performance). Default is `PIPELINED`.     |
| flinkConfDir           | String  | Directory containing Flink YAML configuration files. Its properties are applied to the submitted job and take precedence over `FLINK_CONF_DIR`.                                                                                                |
| filesToStage           | Array<String\> | Jar files to ship to the Flink cluster with the job. Defaults to the classpath of the submitting process.                                                                                                                          |

## Checkpointing

Without `checkpointingInterval` a **streaming** pipeline takes no checkpoints: a task failure restarts it from the
beginning of its sources and loses in-flight state. Set it (and a durable `stateBackendStoragePath`) for any
production streaming job. On Amazon Managed Service for Apache Flink, checkpointing is configured by the service
(the application's checkpoint configuration) and the options below may not take effect.

| parameter                                   | type    | description                                                                                                                                         |
|---------------------------------------------|---------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| checkpointingInterval                       | Long    | Interval in milliseconds at which checkpoints are triggered. Default: no checkpointing.                                                              |
| checkpointingMode                           | String  | `EXACTLY_ONCE` (default) or `AT_LEAST_ONCE`.                                                                                                         |
| checkpointTimeoutMillis                     | Long    | Maximum time in milliseconds a checkpoint may take before it is discarded.                                                                           |
| minPauseBetweenCheckpoints                  | Long    | Minimal pause in milliseconds between the end of a checkpoint and the start of the next.                                                             |
| numConcurrentCheckpoints                    | Integer | Maximum number of checkpoints in progress at the same time (default 1).                                                                              |
| tolerableCheckpointFailureNumber            | Integer | Number of consecutive checkpoint failures tolerated before the job fails (default 0).                                                                |
| unalignedCheckpointEnabled                  | Boolean | Use unaligned checkpoints (in-flight buffers become part of the checkpoint), which keeps checkpoint duration independent of backpressure (default false). |
| finishBundleBeforeCheckpointing             | Boolean | Finish the current bundle and flush its output before checkpointing, instead of buffering the remaining output in the checkpoint (default false).   |
| externalizedCheckpointsEnabled              | Boolean | Persist checkpoints externally so that a job can be restarted from them (default false).                                                            |
| retainExternalizedCheckpointsOnCancellation | Boolean | Keep externalized checkpoints when the job is cancelled (default false: they are deleted).                                                           |

## State and recovery

| parameter                | type    | description                                                                                                                                       |
|--------------------------|---------|---------------------------------------------------------------------------------------------------------------------------------------------------|
| stateBackend             | String  | State backend for Beam's state: `rocksdb` (state on local disk, for large state) or `hashmap` (on the heap; same as `filesystem`). Defaults to the cluster's `state.backend`. |
| stateBackendStoragePath  | String  | Durable path where the state backend writes checkpoints, e.g. `s3://bucket/checkpoints` or `gs://bucket/checkpoints`. Required with `stateBackend`. |
| savepointPath            | String  | Restore the streaming pipeline from this savepoint. Combine with `allowNonRestoredState` when operators were removed.                               |
| numberOfExecutionRetries | Integer | Number of times failed tasks are re-executed (`0` disables fault tolerance; default: the cluster's restart strategy).                              |
| executionRetryDelay      | Long    | Delay in milliseconds between re-executions (default: the cluster's restart strategy).                                                            |

## Execution tuning

| parameter                  | type    | description                                                                                                                                                        |
|----------------------------|---------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| maxBundleSize              | Long    | Maximum number of elements in a bundle (default 1,000 for streaming, 1,000,000 for batch). Smaller bundles lower latency; larger ones raise throughput.            |
| maxBundleTimeMills         | Long    | Maximum time in milliseconds before a bundle is finished (default 1,000 for streaming, 10,000 for batch).                                                          |
| autoWatermarkInterval      | Long    | Interval in milliseconds at which watermarks are emitted.                                                                                                          |
| objectReuse                | Boolean | Let Flink reuse objects between operators instead of copying them (default false). Faster; safe because Beam elements are not mutated in place.                  |
| fasterCopy                 | Boolean | Skip unneeded deep copies between operators (default false).                                                                                                       |
| operatorChaining           | Boolean | Chain operators into the same task (default true). Disable to see each step separately in the Flink UI.                                                            |
| shutdownSourcesAfterIdleMs | Long    | Shut down sources idle for this many milliseconds; once all sources are shut down the job finishes. Checkpointing stops working after a source has shut down.     |
| disableMetrics             | Boolean | Disable reporting Beam metrics to Flink (default false).                                                                                                           |

#### Example

```yaml
options:
  streaming: false
  flink:
    flinkMaster: "[auto]"
    parallelism: 4
    maxParallelism: 128
    executionModeForBatch: BATCH
```

```yaml
options:
  streaming: true
  flink:
    parallelism: 8
    checkpointingInterval: 60000
    minPauseBetweenCheckpoints: 30000
    externalizedCheckpointsEnabled: true
    retainExternalizedCheckpointsOnCancellation: true
    stateBackend: rocksdb
    stateBackendStoragePath: "s3://bucket/flink/checkpoints"
```
