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
