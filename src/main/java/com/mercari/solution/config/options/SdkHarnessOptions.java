package com.mercari.solution.config.options;

import com.mercari.solution.MPipeline;
import org.apache.beam.sdk.options.PipelineOptions;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Beam core {@link org.apache.beam.sdk.options.SdkHarnessOptions}: settings of the Java SDK harness that runs
 * the pipeline's user code (worker log levels, state cache, stuck-bundle timeout). Honoured by Dataflow and the
 * portable runners (prism / portable / flink / spark job services).
 *
 * <p>On Dataflow the log levels are mirrored to {@code DataflowWorkerLoggingOptions}
 * ({@code defaultWorkerLogLevel} / {@code workerLogLevelOverrides}), which the legacy (non runner v2) worker
 * reads instead, so the setting takes effect whichever worker runs the job.
 */
public class SdkHarnessOptions implements Serializable {

    private String defaultSdkHarnessLogLevel;
    private Map<String, String> sdkHarnessLogLevelOverrides;
    private Boolean logMdc;
    private Integer maxCacheMemoryUsageMb;
    private Float maxCacheMemoryUsagePercent;
    private Integer elementProcessingTimeoutMinutes;
    private List<String> jdkAddOpenModules;

    public static void setOptions(
            final PipelineOptions pipelineOptions,
            final SdkHarnessOptions sdkHarness,
            final MPipeline.Runner runner) {

        if(sdkHarness == null) {
            return;
        }

        RunnerOptionSetter.of(pipelineOptions, org.apache.beam.sdk.options.SdkHarnessOptions.class)
                .set("defaultSdkHarnessLogLevel", sdkHarness.defaultSdkHarnessLogLevel)
                .set("sdkHarnessLogLevelOverrides", sdkHarness.sdkHarnessLogLevelOverrides)
                .set("logMdc", sdkHarness.logMdc)
                .set("maxCacheMemoryUsageMb", sdkHarness.maxCacheMemoryUsageMb)
                .set("maxCacheMemoryUsagePercent", sdkHarness.maxCacheMemoryUsagePercent)
                .set("elementProcessingTimeoutMinutes", sdkHarness.elementProcessingTimeoutMinutes)
                .set("jdkAddOpenModules", sdkHarness.jdkAddOpenModules);

        if(runner == MPipeline.Runner.dataflow
                && (sdkHarness.defaultSdkHarnessLogLevel != null || sdkHarness.sdkHarnessLogLevelOverrides != null)) {
            try {
                RunnerOptionSetter.of(pipelineOptions, "org.apache.beam.runners.dataflow.options.DataflowWorkerLoggingOptions")
                        .set("defaultWorkerLogLevel", sdkHarness.defaultSdkHarnessLogLevel)
                        .set("workerLogLevelOverrides", sdkHarness.sdkHarnessLogLevelOverrides);
            } catch (final ClassNotFoundException e) {
                throw new RuntimeException("Failed to set dataflow worker logging options", e);
            }
        }
    }

}
