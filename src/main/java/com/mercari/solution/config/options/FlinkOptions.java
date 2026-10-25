package com.mercari.solution.config.options;

import org.apache.beam.sdk.options.PipelineOptions;

import java.io.Serializable;
import java.util.List;

public class FlinkOptions implements Serializable {

    private String flinkMaster;
    private Integer parallelism;
    private Integer maxParallelism;
    private Boolean allowNonRestoredState;
    private Boolean attachedMode;
    private String executionModeForBatch;
    private String flinkConfDir;
    private List<String> filesToStage;

    // checkpointing
    private Long checkpointingInterval;
    private String checkpointingMode;
    private Long checkpointTimeoutMillis;
    private Long minPauseBetweenCheckpoints;
    private Integer numConcurrentCheckpoints;
    private Integer tolerableCheckpointFailureNumber;
    private Boolean unalignedCheckpointEnabled;
    private Boolean finishBundleBeforeCheckpointing;
    private Boolean externalizedCheckpointsEnabled;
    private Boolean retainExternalizedCheckpointsOnCancellation;

    // state & recovery
    private String stateBackend;
    private String stateBackendStoragePath;
    private String savepointPath;
    private Integer numberOfExecutionRetries;
    private Long executionRetryDelay;

    // execution tuning
    private Boolean objectReuse;
    private Boolean fasterCopy;
    private Boolean operatorChaining;
    private Long maxBundleSize;
    private Long maxBundleTimeMills;
    private Long autoWatermarkInterval;
    private Long shutdownSourcesAfterIdleMs;
    private Boolean disableMetrics;

    public static void setOptions(
            final PipelineOptions pipelineOptions,
            final FlinkOptions flink) {

        if(flink == null) {
            return;
        }

        final RunnerOptionSetter setter;
        try {
            setter = RunnerOptionSetter.of(pipelineOptions, "org.apache.beam.runners.flink.FlinkPipelineOptions");
        } catch (final ClassNotFoundException e) {
            throw new RuntimeException("Failed to set flink runner pipeline options", e);
        }

        setter.set("flinkMaster", flink.flinkMaster)
                .set("parallelism", flink.parallelism)
                .set("maxParallelism", flink.maxParallelism)
                .set("allowNonRestoredState", flink.allowNonRestoredState)
                .set("attachedMode", flink.attachedMode)
                .set("executionModeForBatch", flink.executionModeForBatch)
                .set("flinkConfDir", flink.flinkConfDir)
                .set("filesToStage", flink.filesToStage)

                .set("checkpointingInterval", flink.checkpointingInterval)
                .set("checkpointingMode", flink.checkpointingMode)
                .set("checkpointTimeoutMillis", flink.checkpointTimeoutMillis)
                .set("minPauseBetweenCheckpoints", flink.minPauseBetweenCheckpoints)
                .set("numConcurrentCheckpoints", flink.numConcurrentCheckpoints)
                .set("tolerableCheckpointFailureNumber", flink.tolerableCheckpointFailureNumber)
                .set("unalignedCheckpointEnabled", flink.unalignedCheckpointEnabled)
                .set("finishBundleBeforeCheckpointing", flink.finishBundleBeforeCheckpointing)
                .set("externalizedCheckpointsEnabled", flink.externalizedCheckpointsEnabled)
                .set("retainExternalizedCheckpointsOnCancellation", flink.retainExternalizedCheckpointsOnCancellation)

                .set("stateBackend", flink.stateBackend)
                .set("stateBackendStoragePath", flink.stateBackendStoragePath)
                .set("savepointPath", flink.savepointPath)
                .set("numberOfExecutionRetries", flink.numberOfExecutionRetries)
                .set("executionRetryDelay", flink.executionRetryDelay)

                .set("objectReuse", flink.objectReuse)
                .set("fasterCopy", flink.fasterCopy)
                .set("operatorChaining", flink.operatorChaining)
                .set("maxBundleSize", flink.maxBundleSize)
                .set("maxBundleTimeMills", flink.maxBundleTimeMills)
                .set("autoWatermarkInterval", flink.autoWatermarkInterval)
                .set("shutdownSourcesAfterIdleMs", flink.shutdownSourcesAfterIdleMs)
                .set("disableMetrics", flink.disableMetrics);
    }
}
