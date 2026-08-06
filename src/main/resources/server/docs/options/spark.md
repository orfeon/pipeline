# Spark Options

Spark runner options

These options configure the [Apache Spark runner](https://beam.apache.org/documentation/runners/spark/) for pipeline execution.

## Spark common pipeline options

| parameter                             | type    | description                                                                                                                                              |
|---------------------------------------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------|
| sparkMaster                           | String  | The Spark master URL. Use `local[*]` for local mode, `spark://host:port` for standalone, `yarn` for YARN, or `k8s://https://host:port` for Kubernetes.  |
| checkpointDir                         | String  | A checkpoint directory for streaming resilience. Used to store intermediate state for fault tolerance.                                                    |
| storageLevel                          | String  | The [storage level](https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence) for caching RDDs. Default is `MEMORY_ONLY`.        |
| enableSparkMetricSinks                | Boolean | Enable reporting metrics to Spark metric sinks. Default is `true`.                                                                                       |
| preferGroupByKeyToHandleHugeValues    | Boolean | Prefer GroupByKey over Combine for handling huge values. Can avoid OOM issues with large values at the cost of performance. Default is `false`.           |

## Spark pipeline options

| parameter                | type          | description                                                                                                                            |
|--------------------------|---------------|----------------------------------------------------------------------------------------------------------------------------------------|
| bundleSize               | Long          | The size of the bundle for read and write operations. Controls how data is partitioned.                                                |
| batchIntervalMillis      | Long          | The batch interval in milliseconds for the Spark streaming context.                                                                    |
| checkpointDurationMillis | Long          | The checkpoint duration in milliseconds for Spark streaming.                                                                           |
| maxRecordsPerBatch       | Long          | The maximum number of records per batch for bounded reads.                                                                             |
| minReadTimeMillis        | Long          | The minimum read time in milliseconds for micro-batch reads.                                                                           |
| readTimePercentage       | Double        | The percentage of batch interval to use as the read time. Value between 0 and 1.                                                       |
| cacheDisabled            | Boolean       | Disable RDD caching. Set to `true` to reduce memory usage at the cost of recomputation. Default is `false`.                            |
| usesProvidedSparkContext | Boolean       | Use a provided Spark context instead of creating a new one. Useful for testing or embedding in existing Spark applications.             |
| filesToStage             | Array<String> | A list of local files to stage on all Spark workers. Useful for distributing additional resources or configuration files.               |

#### Example

```yaml
options:
  streaming: true
  spark:
    sparkMaster: "k8s://https://k8s-apiserver:443"
    checkpointDir: "gs://bucket/checkpoints"
    batchIntervalMillis: 1000
    storageLevel: MEMORY_AND_DISK
```
