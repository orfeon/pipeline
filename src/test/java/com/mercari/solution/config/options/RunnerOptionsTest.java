package com.mercari.solution.config.options;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Options;
import com.mercari.solution.util.domain.file.JsonUtil;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.resourcehints.ResourceHintsOptions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Options blocks other than dataflow / prism / gcp / aws. The flink and spark runner classes are referenced by name
 * only, so their tests are skipped unless the build runs with -Pflink / -Pspark.
 */
public class RunnerOptionsTest {

    private static Options parse(final String json) {
        return JsonUtil.fromJson(json, Options.class);
    }

    @SuppressWarnings("unchecked")
    private static Class<? extends PipelineOptions> runnerOptionsClass(final String className) {
        try {
            return (Class<? extends PipelineOptions>) Class.forName(className);
        } catch (final ClassNotFoundException e) {
            Assumptions.abort(className + " is not on the classpath");
            return null;
        }
    }

    private static Object get(final PipelineOptions pipelineOptions, final Class<? extends PipelineOptions> clazz, final String getter) throws Exception {
        return clazz.getMethod(getter).invoke(pipelineOptions.as(clazz));
    }

    @Test
    public void testCommonOptions() {
        final Options options = parse("""
                {
                  "updateCompatibilityVersion": "2.53.0",
                  "resourceHints": ["min_ram=16GB"],
                  "gcp": { "project": "example-project" }
                }
                """);
        final PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        Options.setOptions(pipelineOptions, options);
        Assertions.assertEquals("2.53.0", pipelineOptions.as(StreamingOptions.class).getUpdateCompatibilityVersion());
        Assertions.assertEquals(List.of("min_ram=16GB"), pipelineOptions.as(ResourceHintsOptions.class).getResourceHints());
    }

    @Test
    public void testSdkHarnessOptions() {
        final Options options = parse("""
                {
                  "sdkHarness": {
                    "defaultSdkHarnessLogLevel": "DEBUG",
                    "sdkHarnessLogLevelOverrides": { "org.apache.kafka": "WARN" },
                    "logMdc": true,
                    "maxCacheMemoryUsageMb": 512,
                    "maxCacheMemoryUsagePercent": 25.5,
                    "elementProcessingTimeoutMinutes": 30,
                    "jdkAddOpenModules": ["java.base/java.nio=ALL-UNNAMED"]
                  }
                }
                """);
        final PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        SdkHarnessOptions.setOptions(pipelineOptions, options.getSdkHarness(), MPipeline.Runner.direct);
        final org.apache.beam.sdk.options.SdkHarnessOptions harness = pipelineOptions.as(org.apache.beam.sdk.options.SdkHarnessOptions.class);
        Assertions.assertEquals(org.apache.beam.sdk.options.SdkHarnessOptions.LogLevel.DEBUG, harness.getDefaultSdkHarnessLogLevel());
        Assertions.assertEquals(org.apache.beam.sdk.options.SdkHarnessOptions.LogLevel.WARN, harness.getSdkHarnessLogLevelOverrides().get("org.apache.kafka"));
        Assertions.assertTrue(harness.getLogMdc());
        Assertions.assertEquals(512, harness.getMaxCacheMemoryUsageMb());
        Assertions.assertEquals(25.5f, harness.getMaxCacheMemoryUsagePercent());
        Assertions.assertEquals(30, harness.getElementProcessingTimeoutMinutes());
        Assertions.assertEquals(List.of("java.base/java.nio=ALL-UNNAMED"), harness.getJdkAddOpenModules());
    }

    /** Each value lands on its own option (environmentCacheMillis / environmentExpirationMillis were swapped once). */
    @Test
    public void testPortableOptions() {
        final Options options = parse("""
                {
                  "portable": {
                    "jobEndpoint": "localhost:8099",
                    "defaultEnvironmentType": "LOOPBACK",
                    "environmentCacheMillis": 1000,
                    "environmentExpirationMillis": 2000,
                    "sdkWorkerParallelism": 4,
                    "loadBalanceBundles": true,
                    "filesToStage": ["/tmp/a.jar"]
                  }
                }
                """);
        final PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        PortableOptions.setOptions(pipelineOptions, options.getPortable());
        final PortablePipelineOptions portable = pipelineOptions.as(PortablePipelineOptions.class);
        Assertions.assertEquals("localhost:8099", portable.getJobEndpoint());
        Assertions.assertEquals("LOOPBACK", portable.getDefaultEnvironmentType());
        Assertions.assertEquals(1000, portable.getEnvironmentCacheMillis());
        Assertions.assertEquals(2000, portable.getEnvironmentExpirationMillis());
        Assertions.assertEquals(4, portable.getSdkWorkerParallelism());
        Assertions.assertTrue(portable.getLoadBalanceBundles());
        Assertions.assertEquals(List.of("/tmp/a.jar"), portable.getFilesToStage());
    }

    @Test
    public void testFlinkOptions() throws Exception {
        final Class<? extends PipelineOptions> clazz = runnerOptionsClass("org.apache.beam.runners.flink.FlinkPipelineOptions");
        final Options options = parse("""
                {
                  "flink": {
                    "flinkMaster": "[local]",
                    "parallelism": 4,
                    "allowNonRestoredState": true,
                    "attachedMode": false,
                    "checkpointingInterval": 60000,
                    "checkpointingMode": "AT_LEAST_ONCE",
                    "numConcurrentCheckpoints": 2,
                    "unalignedCheckpointEnabled": true,
                    "externalizedCheckpointsEnabled": true,
                    "stateBackend": "rocksdb",
                    "stateBackendStoragePath": "s3://bucket/checkpoints",
                    "savepointPath": "s3://bucket/savepoints/sp-1",
                    "numberOfExecutionRetries": 3,
                    "objectReuse": true,
                    "maxBundleSize": 500,
                    "filesToStage": ["/opt/a.jar"]
                  }
                }
                """);
        final PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        FlinkOptions.setOptions(pipelineOptions, options.getFlink());
        Assertions.assertEquals("[local]", get(pipelineOptions, clazz, "getFlinkMaster"));
        Assertions.assertEquals(4, get(pipelineOptions, clazz, "getParallelism"));
        Assertions.assertEquals(true, get(pipelineOptions, clazz, "getAllowNonRestoredState"));
        Assertions.assertEquals(false, get(pipelineOptions, clazz, "getAttachedMode"));
        Assertions.assertEquals(60000L, get(pipelineOptions, clazz, "getCheckpointingInterval"));
        Assertions.assertEquals("AT_LEAST_ONCE", get(pipelineOptions, clazz, "getCheckpointingMode"));
        Assertions.assertEquals(2, get(pipelineOptions, clazz, "getNumConcurrentCheckpoints"));
        Assertions.assertEquals(true, get(pipelineOptions, clazz, "getUnalignedCheckpointEnabled"));
        Assertions.assertEquals(true, get(pipelineOptions, clazz, "isExternalizedCheckpointsEnabled"));
        Assertions.assertEquals("rocksdb", get(pipelineOptions, clazz, "getStateBackend"));
        Assertions.assertEquals("s3://bucket/checkpoints", get(pipelineOptions, clazz, "getStateBackendStoragePath"));
        Assertions.assertEquals("s3://bucket/savepoints/sp-1", get(pipelineOptions, clazz, "getSavepointPath"));
        Assertions.assertEquals(3, get(pipelineOptions, clazz, "getNumberOfExecutionRetries"));
        Assertions.assertEquals(true, get(pipelineOptions, clazz, "getObjectReuse"));
        Assertions.assertEquals(500L, get(pipelineOptions, clazz, "getMaxBundleSize"));
        Assertions.assertEquals(List.of("/opt/a.jar"), get(pipelineOptions, clazz, "getFilesToStage"));
    }

    @Test
    public void testSparkOptions() throws Exception {
        final Class<? extends PipelineOptions> clazz = runnerOptionsClass("org.apache.beam.runners.spark.SparkPipelineOptions");
        final Options options = parse("""
                {
                  "spark": {
                    "sparkMaster": "local[2]",
                    "storageLevel": "MEMORY_AND_DISK",
                    "batchIntervalMillis": 1000,
                    "readTimePercentage": 0.2,
                    "cacheDisabled": true,
                    "usesProvidedSparkContext": false,
                    "filesToStage": ["/opt/a.jar"]
                  }
                }
                """);
        final PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        SparkOptions.setOptions(pipelineOptions, options.getSpark());
        Assertions.assertEquals("local[2]", get(pipelineOptions, clazz, "getSparkMaster"));
        Assertions.assertEquals("MEMORY_AND_DISK", get(pipelineOptions, clazz, "getStorageLevel"));
        Assertions.assertEquals(1000L, get(pipelineOptions, clazz, "getBatchIntervalMillis"));
        Assertions.assertEquals(0.2, get(pipelineOptions, clazz, "getReadTimePercentage"));
        Assertions.assertEquals(true, get(pipelineOptions, clazz, "isCacheDisabled"));
        Assertions.assertEquals(false, get(pipelineOptions, clazz, "getUsesProvidedSparkContext"));
        Assertions.assertEquals(List.of("/opt/a.jar"), get(pipelineOptions, clazz, "getFilesToStage"));
    }

}
