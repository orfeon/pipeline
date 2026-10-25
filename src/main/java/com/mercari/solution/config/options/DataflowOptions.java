package com.mercari.solution.config.options;

import com.google.dataflow.v1beta3.FlexTemplateRuntimeEnvironment;
import com.google.dataflow.v1beta3.LaunchFlexTemplateParameter;
import com.google.dataflow.v1beta3.LaunchTemplateParameters;
import com.google.dataflow.v1beta3.RuntimeEnvironment;
import com.mercari.solution.config.Options;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.values.PInput;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class DataflowOptions implements Serializable {

    private String project;
    private String tempLocation;
    private String stagingLocation;
    private Map<String, String> labels;
    private String autoscalingAlgorithm;
    private String flexRSGoal;
    private Integer numWorkers;
    private Integer maxNumWorkers;
    private String workerMachineType;
    private Integer diskSizeGb;
    private String region;
    private String workerDiskType;
    private String workerRegion;
    private String workerZone;
    private String serviceAccount;
    private String impersonateServiceAccount;
    private String network;
    private String subnetwork;
    private Boolean usePublicIps;
    private Integer numberOfWorkerHarnessThreads;
    private Integer workerCacheMb;
    private String createFromSnapshot;
    private String sdkContainerImage;
    private String sdkHarnessContainerImageOverrides;
    private Boolean enableStreamingEngine;
    private Integer streamingSideInputCacheExpirationMillis;
    private Boolean update;
    private Map<String, String> transformNameMapping;
    private List<String> dataflowServiceOptions;
    private List<String> experiments;

    private String templateLocation;

    // for launcher parameter
    private String launcherMachineType;


    public String getProject() {
        return project;
    }

    public String getRegion() {
        return region;
    }

    public String getTemplateLocation() {
        return templateLocation;
    }

    public String getServiceAccount() {
        return serviceAccount;
    }

    public String getSubnetwork() {
        return subnetwork;
    }

    public String getTempLocation() {
        return tempLocation;
    }

    public String getStagingLocation() {
        return stagingLocation;
    }

    public static void setOptions(
            final PipelineOptions pipelineOptions,
            final DataflowOptions dataflow) {

        if(dataflow == null) {
            return;
        }

        try {
            final Class<? extends PipelineOptions> clazz = (Class<? extends PipelineOptions>)Class.forName(DATAFLOW_PIPELINE_OPTIONS);
            final PipelineOptions dataflowOptions = pipelineOptions.as(clazz);

            if(dataflow.tempLocation != null) {
                pipelineOptions.setTempLocation(dataflow.tempLocation);
            }
            if(dataflow.project != null) {
                clazz.getMethod("setProject", String.class).invoke(dataflowOptions, dataflow.project);
            }
            if(dataflow.stagingLocation != null) {
                clazz.getMethod("setStagingLocation", String.class).invoke(dataflowOptions, dataflow.stagingLocation);
            }
            if(dataflow.labels != null && !dataflow.labels.isEmpty()) {
                clazz.getMethod("setLabels", Map.class).invoke(dataflowOptions, dataflow.labels);
            }
            if(dataflow.autoscalingAlgorithm != null) {
                final Class enumClazz = Class.forName("org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions$AutoscalingAlgorithmType");
                clazz.getMethod("setAutoscalingAlgorithm", enumClazz).invoke(dataflowOptions, Enum.valueOf(enumClazz, dataflow.autoscalingAlgorithm));
            }
            if(dataflow.flexRSGoal != null) {
                final Class enumClazz = Class.forName("org.apache.beam.runners.dataflow.options.DataflowPipelineOptions$FlexResourceSchedulingGoal");
                clazz.getMethod("setFlexRSGoal", enumClazz).invoke(dataflowOptions, Enum.valueOf(enumClazz, dataflow.flexRSGoal));
            }
            if(dataflow.numWorkers != null && dataflow.numWorkers > 0) {
                clazz.getMethod("setNumWorkers", int.class).invoke(dataflowOptions, dataflow.numWorkers);
            }
            if(dataflow.maxNumWorkers != null && dataflow.maxNumWorkers > 0) {
                clazz.getMethod("setMaxNumWorkers", int.class).invoke(dataflowOptions, dataflow.maxNumWorkers);
            }
            if(dataflow.numberOfWorkerHarnessThreads != null && dataflow.numberOfWorkerHarnessThreads > 0) {
                clazz.getMethod("setNumberOfWorkerHarnessThreads", int.class).invoke(dataflowOptions, dataflow.numberOfWorkerHarnessThreads);
            }
            if(dataflow.workerMachineType != null) {
                clazz.getMethod("setWorkerMachineType", String.class).invoke(dataflowOptions, dataflow.workerMachineType);
            }
            if(dataflow.region != null) {
                clazz.getMethod("setRegion", String.class).invoke(dataflowOptions, dataflow.region);
            }
            if(dataflow.workerRegion != null) {
                pipelineOptions.as(GcpOptions.class).setWorkerRegion(dataflow.workerRegion);
            }
            if(dataflow.workerZone != null) {
                pipelineOptions.as(GcpOptions.class).setWorkerZone(dataflow.workerZone);
            }
            if(dataflow.diskSizeGb != null) {
                clazz.getMethod("setDiskSizeGb", int.class).invoke(dataflowOptions, dataflow.diskSizeGb);
            }
            if(dataflow.workerDiskType != null) {
                clazz.getMethod("setWorkerDiskType", String.class).invoke(dataflowOptions, dataflow.workerDiskType);
            }
            if(dataflow.serviceAccount != null) {
                clazz.getMethod("setServiceAccount", String.class).invoke(dataflowOptions, dataflow.serviceAccount);
            }
            if(dataflow.impersonateServiceAccount != null) {
                pipelineOptions.as(GcpOptions.class).setImpersonateServiceAccount(dataflow.impersonateServiceAccount);
            }
            if(dataflow.network != null) {
                clazz.getMethod("setNetwork", String.class).invoke(dataflowOptions, dataflow.network);
            }
            if(dataflow.subnetwork != null) {
                clazz.getMethod("setSubnetwork", String.class).invoke(dataflowOptions, dataflow.subnetwork);
            }
            if(dataflow.usePublicIps != null) {
                clazz.getMethod("setUsePublicIps", Boolean.class).invoke(dataflowOptions, dataflow.usePublicIps);
            }
            if(dataflow.workerCacheMb != null && dataflow.workerCacheMb > 0) {
                clazz.getMethod("setWorkerCacheMb", Integer.class).invoke(dataflowOptions, dataflow.workerCacheMb);
            }
            if(dataflow.createFromSnapshot != null) {
                clazz.getMethod("setCreateFromSnapshot", String.class).invoke(dataflowOptions, dataflow.createFromSnapshot);
            }
            if(dataflow.sdkContainerImage != null) {
                clazz.getMethod("setSdkContainerImage", String.class).invoke(dataflowOptions, dataflow.sdkContainerImage);
            }
            if(dataflow.sdkHarnessContainerImageOverrides != null) {
                clazz.getMethod("setSdkHarnessContainerImageOverrides", String.class).invoke(dataflowOptions, dataflow.sdkHarnessContainerImageOverrides);
            }
            if(dataflow.streamingSideInputCacheExpirationMillis != null) {
                clazz.getMethod("setStreamingSideInputCacheExpirationMillis", Integer.class).invoke(dataflowOptions, dataflow.streamingSideInputCacheExpirationMillis);
            }
            if(dataflow.update != null) {
                clazz.getMethod("setUpdate", Boolean.class).invoke(dataflowOptions, dataflow.update);
                if(dataflow.update) {
                    if(dataflow.transformNameMapping != null && !dataflow.transformNameMapping.isEmpty()) {
                        clazz.getMethod("setTransformNameMapping", Map.class).invoke(dataflowOptions, dataflow.transformNameMapping);
                    }
                }
            }
            if(dataflow.enableStreamingEngine != null && pipelineOptions.as(StreamingOptions.class).isStreaming()) {
                dataflowOptions.as(GcpOptions.class).setEnableStreamingEngine(dataflow.enableStreamingEngine);
            }
            if(dataflow.dataflowServiceOptions != null && !dataflow.dataflowServiceOptions.isEmpty()) {
                final List<String> existingDataflowServiceOptions = Optional
                        .ofNullable((List<String>)clazz.getMethod("getDataflowServiceOptions").invoke(dataflowOptions))
                        .orElseGet(ArrayList::new);
                existingDataflowServiceOptions.addAll(dataflow.dataflowServiceOptions);
                clazz.getMethod("setDataflowServiceOptions", List.class).invoke(dataflowOptions, existingDataflowServiceOptions.stream().distinct().toList());
            }
            if(dataflow.experiments != null && !dataflow.experiments.isEmpty()) {
                final List<String> existingExperiments = Optional
                        .ofNullable((List<String>)clazz.getMethod("getExperiments").invoke(dataflowOptions))
                        .orElseGet(ArrayList::new);
                existingExperiments.addAll(dataflow.experiments);
                clazz.getMethod("setExperiments", List.class).invoke(dataflowOptions, existingExperiments.stream().distinct().toList());
            }
            /*
            if(dataflow.templateLocation != null) {
                clazz.getMethod("setTemplateLocation", String.class).invoke(dataflowOptions, dataflow.templateLocation);
            }
             */

        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Failed to set dataflow runner pipeline options", e);
        }

    }

    static final String DATAFLOW_PIPELINE_OPTIONS = "org.apache.beam.runners.dataflow.options.DataflowPipelineOptions";

    /**
     * The Dataflow maxNumWorkers, as a parallelism hint for modules; 0 when it is not set or the Dataflow runner is
     * not on the classpath (the direct / prism / portable / flink / spark builds do not ship it).
     */
    public static int getMaxNumWorkers(final PInput input) {
        final Object value = getOption(input.getPipeline().getOptions(), "getMaxNumWorkers");
        return value == null ? 0 : (Integer) value;
    }

    public static String getProject(final PipelineOptions options) {
        return options.as(GcpOptions.class).getProject();
    }

    /**
     * The Dataflow worker service account ({@code options.dataflow.serviceAccount} / {@code --serviceAccount});
     * {@code null} when it is not set (Dataflow then uses the Compute Engine default service account) or the
     * Dataflow runner is not on the classpath.
     */
    public static String getServiceAccount(final PipelineOptions options) {
        return (String) getOption(options, "getServiceAccount");
    }

    /**
     * Invokes a {@code DataflowPipelineOptions} getter by name; {@code null} when the Dataflow runner is not on the
     * classpath.
     */
    public static Object getOption(final PipelineOptions options, final String getter) {
        return getOption(options, DATAFLOW_PIPELINE_OPTIONS, getter);
    }

    /**
     * Invokes a getter of a Dataflow runner options interface referenced by name; {@code null} when the interface
     * is not on the classpath.
     */
    @SuppressWarnings("unchecked")
    static Object getOption(final PipelineOptions options, final String className, final String getter) {
        final Class<? extends PipelineOptions> clazz;
        try {
            clazz = (Class<? extends PipelineOptions>) Class.forName(className);
        } catch (final ClassNotFoundException e) {
            return null;
        }
        try {
            return clazz.getMethod(getter).invoke(options.as(clazz));
        } catch (final NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException("Failed to get dataflow runner pipeline option: " + getter, e);
        }
    }

    public static LaunchTemplateParameters createLaunchTemplateParameter(
            final Map<String, String> parameters,
            final Options options) {

        final LaunchTemplateParameters.Builder builder = LaunchTemplateParameters.newBuilder();
        builder.putAllParameters(parameters);
        if(options != null) {
            if(options.getJobName() != null) {
                builder.setJobName(options.getJobName());
            }
            if(options.getDataflow() != null) {
                if(options.getDataflow().update != null) {
                    builder.setUpdate(options.getDataflow().update);
                }
                final RuntimeEnvironment environment = createRuntimeEnvironment(options.getDataflow());
                builder.setEnvironment(environment);
            }
        }
        return builder.build();
    }

    public static LaunchFlexTemplateParameter createLaunchFlexTemplateParameter(
            final String templatePath,
            final Map<String, String> parameters,
            final Options options) {

        final LaunchFlexTemplateParameter.Builder builder = LaunchFlexTemplateParameter.newBuilder();
        builder.setContainerSpecGcsPath(templatePath);
        builder.putAllParameters(parameters);
        if(options != null) {
            if(options.getJobName() != null) {
                builder.setJobName(options.getJobName());
            }
            if(options.getDataflow() != null) {
                if(options.getDataflow().update != null) {
                    builder.setUpdate(options.getDataflow().update);
                }
                final FlexTemplateRuntimeEnvironment environment = createFlexTemplateRuntimeEnvironment(options.getDataflow());
                builder.setEnvironment(environment);
            }
        }

        return builder.build();
    }

    private static RuntimeEnvironment createRuntimeEnvironment(final DataflowOptions dataflow) {
        final RuntimeEnvironment.Builder builder = RuntimeEnvironment.newBuilder();
        if(dataflow.tempLocation != null) {
            builder.setTempLocation(dataflow.tempLocation);
        }
        if(dataflow.serviceAccount != null) {
            builder.setServiceAccountEmail(dataflow.serviceAccount);
        }
        if(dataflow.network != null) {
            builder.setNetwork(dataflow.network);
        }
        if(dataflow.subnetwork != null) {
            builder.setSubnetwork(dataflow.subnetwork);
        }
        if(dataflow.workerRegion != null) {
            builder.setWorkerRegion(dataflow.workerRegion);
        }
        if(dataflow.workerZone != null) {
            builder.setWorkerZone(dataflow.workerZone);
        }
        if(dataflow.workerMachineType != null) {
            builder.setMachineType(dataflow.workerMachineType);
        }
        if(dataflow.numWorkers != null) {
            builder.setNumWorkers(dataflow.numWorkers);
        }
        if(dataflow.maxNumWorkers != null) {
            builder.setMaxWorkers(dataflow.maxNumWorkers);
        }
        if(dataflow.experiments != null && !dataflow.experiments.isEmpty()) {
            builder.addAllAdditionalExperiments(dataflow.experiments);
        }
        if(dataflow.enableStreamingEngine != null) {
            builder.setEnableStreamingEngine(dataflow.enableStreamingEngine);
        }
        return builder.build();
    }

    private static FlexTemplateRuntimeEnvironment createFlexTemplateRuntimeEnvironment(final DataflowOptions dataflow) {
        final FlexTemplateRuntimeEnvironment.Builder builder = FlexTemplateRuntimeEnvironment.newBuilder();
        if(dataflow.tempLocation != null) {
            builder.setTempLocation(dataflow.tempLocation);
        }
        if(dataflow.stagingLocation != null) {
            builder.setStagingLocation(dataflow.stagingLocation);
        }
        if(dataflow.serviceAccount != null) {
            builder.setServiceAccountEmail(dataflow.serviceAccount);
        }
        if(dataflow.network != null) {
            builder.setNetwork(dataflow.network);
        }
        if(dataflow.subnetwork != null) {
            builder.setSubnetwork(dataflow.subnetwork);
        }
        if(dataflow.workerRegion != null) {
            builder.setWorkerRegion(dataflow.workerRegion);
        }
        if(dataflow.workerZone != null) {
            builder.setWorkerZone(dataflow.workerZone);
        }
        if(dataflow.workerMachineType != null) {
            builder.setMachineType(dataflow.workerMachineType);
        }
        if(dataflow.numWorkers != null) {
            builder.setNumWorkers(dataflow.numWorkers);
        }
        if(dataflow.maxNumWorkers != null) {
            builder.setMaxWorkers(dataflow.maxNumWorkers);
        }
        if(dataflow.experiments != null && !dataflow.experiments.isEmpty()) {
            builder.addAllAdditionalExperiments(dataflow.experiments);
        }
        if(dataflow.enableStreamingEngine != null) {
            builder.setEnableStreamingEngine(dataflow.enableStreamingEngine);
        }
        if(dataflow.launcherMachineType != null) {
            builder.setLauncherMachineType(dataflow.launcherMachineType);
        }
        return builder.build();
    }

}
