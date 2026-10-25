package com.mercari.solution.config.options;

import org.apache.beam.sdk.options.PipelineOptions;

import java.io.Serializable;
import java.util.List;

public class SparkOptions implements Serializable {

    // SparkCommonPipelineOptions
    private String sparkMaster;
    private String checkpointDir;
    private String storageLevel;
    private Boolean enableSparkMetricSinks;
    private Boolean preferGroupByKeyToHandleHugeValues;
    private List<String> filesToStage;

    // SparkPipelineOptions
    private Long bundleSize;
    private Long batchIntervalMillis;
    private Long checkpointDurationMillis;
    private Long maxRecordsPerBatch;
    private Long minReadTimeMillis;
    private Double readTimePercentage;
    private Boolean cacheDisabled;
    private Boolean usesProvidedSparkContext;

    public static void setOptions(
            final PipelineOptions pipelineOptions,
            final SparkOptions spark) {

        if(spark == null) {
            return;
        }

        final RunnerOptionSetter setter;
        try {
            setter = RunnerOptionSetter.of(pipelineOptions, "org.apache.beam.runners.spark.SparkPipelineOptions");
        } catch (final ClassNotFoundException e) {
            throw new RuntimeException("Failed to set spark runner pipeline options", e);
        }

        setter.set("sparkMaster", spark.sparkMaster)
                .set("checkpointDir", spark.checkpointDir)
                .set("storageLevel", spark.storageLevel)
                .set("enableSparkMetricSinks", spark.enableSparkMetricSinks)
                .set("preferGroupByKeyToHandleHugeValues", spark.preferGroupByKeyToHandleHugeValues)
                .set("filesToStage", spark.filesToStage)

                .set("bundleSize", spark.bundleSize)
                .set("batchIntervalMillis", spark.batchIntervalMillis)
                .set("checkpointDurationMillis", spark.checkpointDurationMillis)
                .set("maxRecordsPerBatch", spark.maxRecordsPerBatch)
                .set("minReadTimeMillis", spark.minReadTimeMillis)
                .set("readTimePercentage", spark.readTimePercentage)
                .set("cacheDisabled", spark.cacheDisabled)
                .set("usesProvidedSparkContext", spark.usesProvidedSparkContext);
    }

}
