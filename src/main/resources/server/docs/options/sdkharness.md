# SDK Harness Options

Worker-side settings of the Java SDK harness, the process that runs the pipeline's user code. These are
Beam's [SdkHarnessOptions](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/options/SdkHarnessOptions.html),
honoured by Dataflow and the portable runners (prism, portable, Flink / Spark job services).

| parameter                       | type                | description |
|---------------------------------|---------------------|-------------|
| defaultSdkHarnessLogLevel       | Enum                | Default log level of the worker: `OFF`, `ERROR`, `WARN`, `INFO` (default), `DEBUG`, `TRACE`. |
| sdkHarnessLogLevelOverrides     | Map<String,String\> | Per logger (class or package name) log levels, e.g. `{"org.apache.kafka": "WARN", "com.mercari.solution": "DEBUG"}`. The most specific name wins. |
| logMdc                          | Boolean             | Include the SLF4J MDC in worker log entries (default false). |
| maxCacheMemoryUsageMb           | Integer             | Fixed size in MB of the SDK harness's process-wide cache (side inputs and user state, within and across bundles). Takes precedence over `maxCacheMemoryUsagePercent`. |
| maxCacheMemoryUsagePercent      | Float               | Size of that cache as a percentage (0–100) of the JVM heap (default 20). Too large a cache makes the worker run out of memory more easily. |
| elementProcessingTimeoutMinutes | Integer             | Terminate the SDK harness (the runner then retries the work) when a single element has been processing longer than this (minutes; default 0 = no limit). A safety net against stuck calls to external services. Applies to SDK-harness workers (Dataflow Runner v2, portable runners). |
| jdkAddOpenModules               | Array<String\>      | `--add-opens` flags for the worker JVM (`module/package=target-module`, e.g. `java.base/java.nio=ALL-UNNAMED`), for libraries that need reflective access on Java 17+. |

On **Dataflow** `defaultSdkHarnessLogLevel` / `sdkHarnessLogLevelOverrides` are also applied to the legacy worker's
`defaultWorkerLogLevel` / `workerLogLevelOverrides`, so the levels take effect with and without Runner v2.

The log levels only change what the *worker* emits; the launcher / driver process (where the pipeline is
assembled) keeps its own logging configuration.

#### Example

```yaml
options:
  sdkHarness:
    defaultSdkHarnessLogLevel: WARN
    sdkHarnessLogLevelOverrides:
      com.mercari.solution: INFO
    elementProcessingTimeoutMinutes: 30
```
