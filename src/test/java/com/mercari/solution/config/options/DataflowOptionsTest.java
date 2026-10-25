package com.mercari.solution.config.options;

import com.mercari.solution.config.Options;
import com.mercari.solution.util.domain.file.JsonUtil;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

public class DataflowOptionsTest {

    // The DataflowRunner classes are referenced by name only (as in DataflowOptions itself), so this
    // test compiles under every runner profile and is skipped where the dataflow runner is absent.
    private static PipelineOptions applyOptions(final String optionsJson) {
        final Options options = JsonUtil.fromJson(optionsJson, Options.class);
        final PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        DataflowOptions.setOptions(pipelineOptions, options.getDataflow());
        return pipelineOptions;
    }

    @SuppressWarnings("unchecked")
    private static Object as(final PipelineOptions pipelineOptions, final Class<?> clazz) {
        return pipelineOptions.as((Class<? extends PipelineOptions>) clazz);
    }

    @Test
    public void testAutoscalingAlgorithmAndFlexRSGoal() throws Exception {
        final Class<?> clazz;
        try {
            clazz = Class.forName("org.apache.beam.runners.dataflow.options.DataflowPipelineOptions");
        } catch (final ClassNotFoundException e) {
            Assumptions.abort("dataflow runner is not on the classpath");
            return;
        }

        final PipelineOptions pipelineOptions = applyOptions("""
                {
                  "dataflow": {
                    "autoscalingAlgorithm": "NONE",
                    "flexRSGoal": "COST_OPTIMIZED",
                    "numWorkers": 12,
                    "maxNumWorkers": 12
                  }
                }
                """);
        final Object dataflowOptions = as(pipelineOptions, clazz);
        Assertions.assertEquals("NONE",
                clazz.getMethod("getAutoscalingAlgorithm").invoke(dataflowOptions).toString());
        Assertions.assertEquals("COST_OPTIMIZED",
                clazz.getMethod("getFlexRSGoal").invoke(dataflowOptions).toString());
        Assertions.assertEquals(12, clazz.getMethod("getNumWorkers").invoke(dataflowOptions));
        Assertions.assertEquals(12, clazz.getMethod("getMaxNumWorkers").invoke(dataflowOptions));
    }

    /** update / allowNonRestored-style boolean setters take a primitive: a Boolean.class lookup used to fail. */
    @Test
    public void testUpdateAndDebugOptions() throws Exception {
        final Class<?> clazz;
        try {
            clazz = Class.forName("org.apache.beam.runners.dataflow.options.DataflowPipelineOptions");
        } catch (final ClassNotFoundException e) {
            Assumptions.abort("dataflow runner is not on the classpath");
            return;
        }

        final PipelineOptions pipelineOptions = applyOptions("""
                {
                  "dataflow": {
                    "update": true,
                    "transformNameMapping": { "oldStep": "newStep" },
                    "usePublicIps": false,
                    "hotKeyLoggingEnabled": true,
                    "dumpHeapOnOOM": true,
                    "saveHeapDumpsToGcsPath": "gs://bucket/heapdumps",
                    "streamingSideInputCacheMb": 200,
                    "experiments": ["use_runner_v2"]
                  }
                }
                """);
        final Object dataflowOptions = as(pipelineOptions, clazz);
        Assertions.assertEquals(true, clazz.getMethod("isUpdate").invoke(dataflowOptions));
        Assertions.assertEquals(java.util.Map.of("oldStep", "newStep"), clazz.getMethod("getTransformNameMapping").invoke(dataflowOptions));
        Assertions.assertEquals(false, clazz.getMethod("getUsePublicIps").invoke(dataflowOptions));
        Assertions.assertEquals(true, clazz.getMethod("isHotKeyLoggingEnabled").invoke(dataflowOptions));
        Assertions.assertEquals(true, clazz.getMethod("getDumpHeapOnOOM").invoke(dataflowOptions));
        Assertions.assertEquals("gs://bucket/heapdumps", clazz.getMethod("getSaveHeapDumpsToGcsPath").invoke(dataflowOptions));
        Assertions.assertEquals(200, clazz.getMethod("getStreamingSideInputCacheMb").invoke(dataflowOptions));
        Assertions.assertTrue(((java.util.List<?>) clazz.getMethod("getExperiments").invoke(dataflowOptions)).contains("use_runner_v2"));
    }

    /** On Dataflow the SDK harness log levels are mirrored to the legacy worker's logging options. */
    @Test
    public void testSdkHarnessLogLevelsAreMirroredToWorkerLogging() throws Exception {
        final Class<?> clazz;
        try {
            clazz = Class.forName("org.apache.beam.runners.dataflow.options.DataflowWorkerLoggingOptions");
        } catch (final ClassNotFoundException e) {
            Assumptions.abort("dataflow runner is not on the classpath");
            return;
        }
        final Options options = JsonUtil.fromJson("""
                {
                  "sdkHarness": {
                    "defaultSdkHarnessLogLevel": "WARN",
                    "sdkHarnessLogLevelOverrides": { "com.mercari.solution": "DEBUG" }
                  }
                }
                """, Options.class);
        final PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        SdkHarnessOptions.setOptions(pipelineOptions, options.getSdkHarness(), com.mercari.solution.MPipeline.Runner.dataflow);
        final Object logging = as(pipelineOptions, clazz);
        Assertions.assertEquals("WARN", clazz.getMethod("getDefaultWorkerLogLevel").invoke(logging).toString());
        Assertions.assertEquals("DEBUG", ((java.util.Map<?, ?>) clazz.getMethod("getWorkerLogLevelOverrides").invoke(logging))
                .get("com.mercari.solution").toString());
    }

    @Test
    public void testUnknownAutoscalingAlgorithmThrows() throws Exception {
        try {
            Class.forName("org.apache.beam.runners.dataflow.options.DataflowPipelineOptions");
        } catch (final ClassNotFoundException e) {
            Assumptions.abort("dataflow runner is not on the classpath");
            return;
        }
        Assertions.assertThrows(IllegalArgumentException.class, () -> applyOptions("""
                {
                  "dataflow": {
                    "autoscalingAlgorithm": "none"
                  }
                }
                """));
    }

}
