package com.mercari.solution.config.options;

import org.apache.beam.sdk.options.PipelineOptions;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Beam core {@link org.apache.beam.sdk.options.SdkHarnessOptions}: settings of the Java SDK harness that runs
 * the pipeline's user code (worker log levels, state cache, stuck-bundle timeout). Honoured by Dataflow and the
 * portable runners (prism / portable / flink / spark job services).
 *
 * <p>The log levels are not copied to the deprecated {@code DataflowWorkerLoggingOptions}: the legacy (non runner
 * v2) Dataflow worker falls back to these options when the deprecated ones are unset, and setting the deprecated
 * ones would log a deprecation warning on every worker and override a command-line {@code --defaultWorkerLogLevel}.
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
            final SdkHarnessOptions sdkHarness) {

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
    }

}
