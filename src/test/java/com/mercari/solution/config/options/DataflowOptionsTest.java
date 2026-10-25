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

    /** Modules read Dataflow options on every runner: an absent runner class yields null instead of an exception. */
    @Test
    public void testGetOptionIsNullWithoutTheRunnerClass() {
        final PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        Assertions.assertNull(DataflowOptions.getOption(pipelineOptions,
                "org.apache.beam.runners.dataflow.options.NoSuchPipelineOptions", "getServiceAccount"));
    }

    @Test
    public void testGetServiceAccountAndMaxNumWorkers() throws Exception {
        try {
            Class.forName(DataflowOptions.DATAFLOW_PIPELINE_OPTIONS);
        } catch (final ClassNotFoundException e) {
            // without the dataflow runner: no service account, maxNumWorkers as if not set
            final org.apache.beam.sdk.Pipeline pipeline = org.apache.beam.sdk.Pipeline.create(PipelineOptionsFactory.create());
            Assertions.assertNull(DataflowOptions.getServiceAccount(pipeline.getOptions()));
            Assertions.assertEquals(0, DataflowOptions.getMaxNumWorkers(org.apache.beam.sdk.values.PBegin.in(pipeline)));
            return;
        }
        Assertions.assertNull(DataflowOptions.getServiceAccount(PipelineOptionsFactory.create()), "not set");

        final PipelineOptions pipelineOptions = applyOptions("""
                {
                  "dataflow": {
                    "serviceAccount": "worker@my-project.iam.gserviceaccount.com",
                    "maxNumWorkers": 8
                  }
                }
                """);
        final org.apache.beam.sdk.Pipeline pipeline = org.apache.beam.sdk.Pipeline.create(pipelineOptions);
        Assertions.assertEquals("worker@my-project.iam.gserviceaccount.com", DataflowOptions.getServiceAccount(pipelineOptions));
        Assertions.assertEquals(8, DataflowOptions.getMaxNumWorkers(org.apache.beam.sdk.values.PBegin.in(pipeline)));
    }

}
