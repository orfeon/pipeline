package com.mercari.solution.config.options;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PortablePipelineOptions;

import java.io.Serializable;
import java.util.List;

/**
 * {@link PortablePipelineOptions} (Beam core). Applied for the {@code portable} runner and, since
 * {@code PrismPipelineOptions} extends them, for the {@code prism} runner too.
 */
public class PortableOptions implements Serializable {

    private String defaultEnvironmentConfig;
    private String defaultEnvironmentType;
    private Boolean enableHeapDumps;
    private Integer environmentCacheMillis;
    private Integer environmentExpirationMillis;
    private List<String> environmentOptions;
    private String jobEndpoint;
    private Integer jobServerTimeout;
    private Boolean loadBalanceBundles;
    private String outputExecutablePath;
    private Integer sdkWorkerParallelism;
    private List<String> filesToStage;

    public static void setOptions(
            final PipelineOptions pipelineOptions,
            final PortableOptions portable) {

        if(portable == null) {
            return;
        }

        final PortablePipelineOptions portableOptions = pipelineOptions.as(PortablePipelineOptions.class);
        if(portable.defaultEnvironmentConfig != null) {
            portableOptions.setDefaultEnvironmentConfig(portable.defaultEnvironmentConfig);
        }
        if(portable.defaultEnvironmentType != null) {
            portableOptions.setDefaultEnvironmentType(portable.defaultEnvironmentType);
        }
        if(portable.enableHeapDumps != null) {
            portableOptions.setEnableHeapDumps(portable.enableHeapDumps);
        }
        if(portable.environmentCacheMillis != null) {
            portableOptions.setEnvironmentCacheMillis(portable.environmentCacheMillis);
        }
        if(portable.environmentExpirationMillis != null) {
            portableOptions.setEnvironmentExpirationMillis(portable.environmentExpirationMillis);
        }
        if(portable.environmentOptions != null) {
            portableOptions.setEnvironmentOptions(portable.environmentOptions);
        }
        if(portable.jobEndpoint != null) {
            portableOptions.setJobEndpoint(portable.jobEndpoint);
        }
        if(portable.jobServerTimeout != null) {
            portableOptions.setJobServerTimeout(portable.jobServerTimeout);
        }
        if(portable.loadBalanceBundles != null) {
            portableOptions.setLoadBalanceBundles(portable.loadBalanceBundles);
        }
        if(portable.outputExecutablePath != null) {
            portableOptions.setOutputExecutablePath(portable.outputExecutablePath);
        }
        if(portable.sdkWorkerParallelism != null) {
            portableOptions.setSdkWorkerParallelism(portable.sdkWorkerParallelism);
        }
        if(portable.filesToStage != null) {
            portableOptions.setFilesToStage(portable.filesToStage);
        }
    }
}
