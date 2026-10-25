# Define pipeline options

options defines pipeline settings.
The following items can be defined as options.

| parameter                  | type                                | description                                                                                                |
|----------------------------|-------------------------------------|------------------------------------------------------------------------------------------------------------|
| jobName                    | String                              | jobName.                                                                                                   |
| streaming                  | Boolean                             | Specify whether the dataflow job starts in streaming mode or not.                                          |
| tempLocation               | String                              | Path for temporary files (`gs://…` on Dataflow).                                                           |
| updateCompatibilityVersion | String                              | Beam SDK version whose transform expansions should be kept, e.g. `2.53.0`. Set it when upgrading the Beam version of a pipeline that is updated in place (Dataflow `update`, a Flink savepoint) so that transforms whose expansion changed between versions stay update-compatible. |
| resourceHints              | Array<String\>                      | Pipeline-level [resource hints](https://cloud.google.com/dataflow/docs/guides/right-fitting), e.g. `["min_ram=16GB"]` (Dataflow). |
| sdkHarness                 | [SDK Harness Options](sdkharness.md) | Worker-side settings of the Java SDK harness: log levels, state cache size, stuck-element timeout, `--add-opens`. Accepted for every runner; takes effect where user code runs in the Java SDK harness (Dataflow, prism, portable job services), not with the direct or classic Flink / Spark runners. |
| dataflow                   | [Dataflow Options](dataflow.md)     | Specify [Cloud Dataflow runner](https://beam.apache.org/documentation/runners/dataflow/) specific options. |
| direct                     | [Direct Options](direct.md)         | Specify [Direct runner](https://beam.apache.org/documentation/runners/direct/) specific options.           |
| prism                      | [Prism Options](prism.md)           | Specify [Prism runner](https://beam.apache.org/documentation/runners/prism/) specific options.             |
| portable                   | [Portable Options](portable.md)     | Specify portable runner (job service) specific options. Also applied for the prism runner.                 |
| flink                      | [Flink Options](flink.md)           | Specify [Apache Flink runner](https://beam.apache.org/documentation/runners/flink/) specific options.      |
| spark                      | [Spark Options](spark.md)           | Specify [Apache Spark runner](https://beam.apache.org/documentation/runners/spark/) specific options.      |
| gcp                        | [GCP Options](gcp.md)               | Specify Google Cloud options.                                                                              |
| aws                        | [AWS Options](aws.md)               | Specify AWS options (region, endpoint, credentials).                                                      |
| beamsql                    | [Beam SQL Options](beamsql.md)      | Specify Beam SQL common settings (planner, timezone).                                                      |

Only the block of the runner in use is read (`dataflow` for the Dataflow runner, `flink` for Flink, …); the
common parameters and `sdkHarness` / `gcp` / `aws` / `beamsql` apply to every runner. A runner option that does
not exist in the runner's Beam version, or an enum value it does not know, fails the pipeline at startup.


#### Example

```JSON:options
{
  "options": {
    "streaming": true,
    "dataflow": {
      "workerMachineType": "n2-custom-2-131072-ext",
      "numWorkers": 1,
      "diskSizeGb": 256,
      "workerDiskType": "compute.googleapis.com/projects//zones//diskTypes/pd-ssd"
    },
    "beamsql": {
      "plannerName": "org.apache.beam.sdk.extensions.sql.impl.CalciteQueryPlanner"
    }
  },
  "sources": [],
  "transforms": [],
  "sinks": []   
}
```
