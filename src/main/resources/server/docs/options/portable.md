# Portable Options

Portable runner options.

These are Beam's [PortablePipelineOptions](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/options/PortablePipelineOptions.html),
used when the pipeline is submitted to a Beam job service (`--runner=PortableRunner`, e.g. a Flink / Spark job
server). The block is also applied for the [prism](prism.md) runner, whose options extend them.

| parameter                   | type           | description |
|-----------------------------|----------------|-------------|
| jobEndpoint                 | String         | Job service endpoint (`host:port`) to submit the pipeline to. |
| jobServerTimeout            | Integer        | Timeout in seconds for the connection to the job service (default 60). |
| defaultEnvironmentType      | String         | How the runner starts the Java SDK harness: `DOCKER` (default), `PROCESS`, `EXTERNAL` (an already running worker pool) or `LOOPBACK` (the submitting process runs the user code — local testing only). |
| defaultEnvironmentConfig    | String         | Environment-type specific configuration: the container image for `DOCKER`, the worker pool address (`host:port`) for `EXTERNAL`, a JSON command spec for `PROCESS`. |
| environmentOptions          | Array<String\> | Additional environment options as `key=value`, e.g. `docker_container_image=…`, `process_command=…`, `worker_pool_address=…`. |
| sdkWorkerParallelism        | Integer        | Number of SDK workers per runner worker (default 1; 0 lets the runner decide). |
| environmentCacheMillis      | Integer        | How long (ms) an SDK environment is cached for reuse across bundles (default 0 = no caching). |
| environmentExpirationMillis | Integer        | Duration (ms) after which a cached SDK environment expires (default 0 = never). |
| loadBalanceBundles          | Boolean        | Send each bundle to the next free SDK worker instead of pinning SDK workers to runner tasks; helps with long, skewed bundles (default false). |
| enableHeapDumps             | Boolean        | Save a heap dump before an SDK worker shuts down on out-of-memory / GC thrashing and upload it to `tempLocation` (default false). |
| filesToStage                | Array<String\> | Local files (jars) to stage for the SDK workers; defaults to the classpath. |
| outputExecutablePath        | String         | Instead of running, write the pipeline as an executable jar to this path (Flink / Spark job servers). |

#### Example

```yaml
options:
  portable:
    jobEndpoint: localhost:8099
    defaultEnvironmentType: EXTERNAL
    defaultEnvironmentConfig: localhost:50000
    sdkWorkerParallelism: 2
```
