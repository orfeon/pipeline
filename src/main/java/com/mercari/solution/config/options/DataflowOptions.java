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
    private Integer streamingSideInputCacheMb;
    private Integer streamingSideInputCacheExpirationMillis;
    private Boolean update;
    private Map<String, String> transformNameMapping;
    private List<String> dataflowServiceOptions;
    private List<String> experiments;

    // debugging
    private Boolean hotKeyLoggingEnabled;
    private Boolean dumpHeapOnOOM;
    private String saveHeapDumpsToGcsPath;

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

        final RunnerOptionSetter setter;
        try {
            setter = RunnerOptionSetter.of(pipelineOptions, "org.apache.beam.runners.dataflow.options.DataflowPipelineOptions");
        } catch (final ClassNotFoundException e) {
            throw new RuntimeException("Failed to set dataflow runner pipeline options", e);
        }

        if(dataflow.tempLocation != null) {
            pipelineOptions.setTempLocation(dataflow.tempLocation);
        }
        setter.set("project", dataflow.project)
                .set("stagingLocation", dataflow.stagingLocation)
                .set("autoscalingAlgorithm", dataflow.autoscalingAlgorithm)
                .set("flexRSGoal", dataflow.flexRSGoal)
                .set("workerMachineType", dataflow.workerMachineType)
                .set("region", dataflow.region)
                .set("workerRegion", dataflow.workerRegion)
                .set("workerZone", dataflow.workerZone)
                .set("diskSizeGb", dataflow.diskSizeGb)
                .set("workerDiskType", dataflow.workerDiskType)
                .set("serviceAccount", dataflow.serviceAccount)
                .set("impersonateServiceAccount", dataflow.impersonateServiceAccount)
                .set("network", dataflow.network)
                .set("subnetwork", dataflow.subnetwork)
                .set("usePublicIps", dataflow.usePublicIps)
                .set("createFromSnapshot", dataflow.createFromSnapshot)
                .set("sdkContainerImage", dataflow.sdkContainerImage)
                .set("sdkHarnessContainerImageOverrides", dataflow.sdkHarnessContainerImageOverrides)
                .set("streamingSideInputCacheMb", dataflow.streamingSideInputCacheMb)
                .set("streamingSideInputCacheExpirationMillis", dataflow.streamingSideInputCacheExpirationMillis)
                .set("hotKeyLoggingEnabled", dataflow.hotKeyLoggingEnabled)
                .set("dumpHeapOnOOM", dataflow.dumpHeapOnOOM)
                .set("saveHeapDumpsToGcsPath", dataflow.saveHeapDumpsToGcsPath)
                .set("update", dataflow.update);

        if(dataflow.labels != null && !dataflow.labels.isEmpty()) {
            setter.set("labels", dataflow.labels);
        }
        if(dataflow.numWorkers != null && dataflow.numWorkers > 0) {
            setter.set("numWorkers", dataflow.numWorkers);
        }
        if(dataflow.maxNumWorkers != null && dataflow.maxNumWorkers > 0) {
            setter.set("maxNumWorkers", dataflow.maxNumWorkers);
        }
        if(dataflow.numberOfWorkerHarnessThreads != null && dataflow.numberOfWorkerHarnessThreads > 0) {
            setter.set("numberOfWorkerHarnessThreads", dataflow.numberOfWorkerHarnessThreads);
        }
        if(dataflow.workerCacheMb != null && dataflow.workerCacheMb > 0) {
            setter.set("workerCacheMb", dataflow.workerCacheMb);
        }
        if(Boolean.TRUE.equals(dataflow.update)
                && dataflow.transformNameMapping != null && !dataflow.transformNameMapping.isEmpty()) {
            setter.set("transformNameMapping", dataflow.transformNameMapping);
        }
        if(dataflow.enableStreamingEngine != null && pipelineOptions.as(StreamingOptions.class).isStreaming()) {
            pipelineOptions.as(GcpOptions.class).setEnableStreamingEngine(dataflow.enableStreamingEngine);
        }
        if(dataflow.dataflowServiceOptions != null && !dataflow.dataflowServiceOptions.isEmpty()) {
            setter.set("dataflowServiceOptions", merge((List<String>) setter.get("dataflowServiceOptions"), dataflow.dataflowServiceOptions));
        }
        if(dataflow.experiments != null && !dataflow.experiments.isEmpty()) {
            setter.set("experiments", merge((List<String>) setter.get("experiments"), dataflow.experiments));
        }
        /*
        if(dataflow.templateLocation != null) {
            setter.set("templateLocation", dataflow.templateLocation);
        }
         */
    }

    private static List<String> merge(final List<String> existing, final List<String> additional) {
        final List<String> merged = new ArrayList<>(Optional.ofNullable(existing).orElseGet(ArrayList::new));
        merged.addAll(additional);
        return merged.stream().distinct().toList();
    }

    public static DataflowOptions copy(final PipelineOptions pipelineOptions) {
        final DataflowOptions dataflow = new DataflowOptions();
        try {
            final Class<? extends PipelineOptions> clazz = (Class<? extends PipelineOptions>)Class.forName("org.apache.beam.runners.dataflow.options.DataflowPipelineOptions");
            final PipelineOptions dataflowOptions = pipelineOptions.as(clazz);

            if(clazz.getMethod("getProject").invoke(dataflowOptions) != null) {
                dataflow.project = (String)clazz.getMethod("getProject").invoke(dataflowOptions);
            }
            if(clazz.getMethod("getTempLocation").invoke(dataflowOptions) != null) {
                dataflow.tempLocation = (String)clazz.getMethod("getTempLocation").invoke(dataflowOptions);
            }
            if(clazz.getMethod("getStagingLocation").invoke(dataflowOptions) != null) {
                dataflow.stagingLocation = (String)clazz.getMethod("getStagingLocation").invoke(dataflowOptions);
            }
            if(clazz.getMethod("getLabels").invoke(dataflowOptions) != null) {
                dataflow.labels = (Map<String, String>)clazz.getMethod("getLabels").invoke(dataflowOptions);
            }
            /*
            if(dataflow.autoscalingAlgorithm != null) {
                final Class enumClazz = Class.forName("org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType");
                clazz.getMethod("setAutoscalingAlgorithm", Enum.class).invoke(dataflowOptions, Enum.valueOf(enumClazz, dataflow.autoscalingAlgorithm));
            }
            if(dataflow.flexRSGoal != null) {
                final Class enumClazz = Class.forName("org.apache.beam.runners.dataflow.options.DataflowPipelineOptions.FlexResourceSchedulingGoal");
                clazz.getMethod("setFlexRSGoal", Enum.class).invoke(dataflowOptions, Enum.valueOf(enumClazz, dataflow.flexRSGoal));
            }
             */
            if(clazz.getMethod("getNumWorkers").invoke(dataflowOptions) != null) {
                dataflow.numWorkers = (Integer) clazz.getMethod("getNumWorkers").invoke(dataflowOptions);
            }
            if(clazz.getMethod("getMaxNumWorkers").invoke(dataflowOptions) != null) {
                dataflow.maxNumWorkers = (Integer) clazz.getMethod("getMaxNumWorkers").invoke(dataflowOptions);
            }
            if(clazz.getMethod("getNumberOfWorkerHarnessThreads").invoke(dataflowOptions) != null) {
                dataflow.numberOfWorkerHarnessThreads = (Integer) clazz.getMethod("getNumberOfWorkerHarnessThreads").invoke(dataflowOptions);
            }
            if(clazz.getMethod("getWorkerMachineType").invoke(dataflowOptions) != null) {
                dataflow.workerMachineType = (String)clazz.getMethod("getWorkerMachineType").invoke(dataflowOptions);
            }
            if(clazz.getMethod("getRegion").invoke(dataflowOptions) != null) {
                dataflow.region = (String)clazz.getMethod("getRegion").invoke(dataflowOptions);
            }
            if(clazz.getMethod("getWorkerRegion").invoke(dataflowOptions) != null) {
                dataflow.workerRegion = (String)clazz.getMethod("getWorkerRegion").invoke(dataflowOptions);
            }
            if(clazz.getMethod("getWorkerZone").invoke(dataflowOptions) != null) {
                dataflow.workerZone = (String)clazz.getMethod("getWorkerZone").invoke(dataflowOptions);
            }
            if(clazz.getMethod("getDiskSizeGb").invoke(dataflowOptions) != null) {
                dataflow.diskSizeGb = (Integer) clazz.getMethod("getDiskSizeGb").invoke(dataflowOptions);
            }
            if(clazz.getMethod("getWorkerDiskType").invoke(dataflowOptions) != null) {
                dataflow.workerDiskType = (String)clazz.getMethod("getWorkerDiskType").invoke(dataflowOptions);
            }
            if(clazz.getMethod("getServiceAccount").invoke(dataflowOptions) != null) {
                dataflow.serviceAccount = (String)clazz.getMethod("getServiceAccount").invoke(dataflowOptions);
            }
            if(clazz.getMethod("getImpersonateServiceAccount").invoke(dataflowOptions) != null) {
                dataflow.impersonateServiceAccount = (String)clazz.getMethod("getImpersonateServiceAccount").invoke(dataflowOptions);
            }
            if(clazz.getMethod("getNetwork").invoke(dataflowOptions) != null) {
                dataflow.network = (String)clazz.getMethod("getNetwork").invoke(dataflowOptions);
            }
            if(clazz.getMethod("getSubnetwork").invoke(dataflowOptions) != null) {
                dataflow.subnetwork = (String)clazz.getMethod("getSubnetwork").invoke(dataflowOptions);
            }
            if(clazz.getMethod("getUsePublicIps").invoke(dataflowOptions) != null) {
                dataflow.usePublicIps = (Boolean) clazz.getMethod("getUsePublicIps").invoke(dataflowOptions);
            }
            if(clazz.getMethod("getWorkerCacheMb").invoke(dataflowOptions) != null) {
                dataflow.workerCacheMb = (Integer) clazz.getMethod("getWorkerCacheMb").invoke(dataflowOptions);
            }
            if(clazz.getMethod("getCreateFromSnapshot").invoke(dataflowOptions) != null) {
                dataflow.createFromSnapshot = (String)clazz.getMethod("getCreateFromSnapshot").invoke(dataflowOptions);
            }
            if(clazz.getMethod("getSdkContainerImage").invoke(dataflowOptions) != null) {
                dataflow.sdkContainerImage = (String)clazz.getMethod("getSdkContainerImage").invoke(dataflowOptions);
            }
            if(clazz.getMethod("getSdkHarnessContainerImageOverrides").invoke(dataflowOptions) != null) {
                dataflow.sdkHarnessContainerImageOverrides = (String)clazz.getMethod("getSdkHarnessContainerImageOverrides").invoke(dataflowOptions);
            }
            if(clazz.getMethod("getStreamingSideInputCacheExpirationMillis").invoke(dataflowOptions) != null) {
                dataflow.streamingSideInputCacheExpirationMillis = (Integer) clazz.getMethod("getStreamingSideInputCacheExpirationMillis").invoke(dataflowOptions);
            }
            if(pipelineOptions.as(StreamingOptions.class).isStreaming()) {
                if(clazz.getMethod("getUpdate").invoke(dataflowOptions) != null) {
                    dataflow.update = (Boolean) clazz.getMethod("getUpdate").invoke(dataflowOptions);
                    if(dataflow.update) {
                        if(clazz.getMethod("getTransformNameMapping").invoke(dataflowOptions) != null) {
                            dataflow.transformNameMapping = (Map<String, String>) clazz.getMethod("getTransformNameMapping").invoke(dataflowOptions);
                        }
                    }
                }
                if(clazz.getMethod("getEnableStreamingEngine").invoke(dataflowOptions) != null) {
                    dataflow.enableStreamingEngine = (Boolean) clazz.getMethod("getEnableStreamingEngine").invoke(dataflowOptions);
                }
            }

            if(clazz.getMethod("getDataflowServiceOptions").invoke(dataflowOptions) != null) {
                dataflow.dataflowServiceOptions = (List<String>) clazz.getMethod("getDataflowServiceOptions").invoke(dataflowOptions);
            }
            if(clazz.getMethod("getExperiments").invoke(dataflowOptions) != null) {
                dataflow.experiments = (List<String>) clazz.getMethod("getExperiments").invoke(dataflowOptions);
            }
            /*
            if(clazz.getMethod("getTemplateLocation").invoke(dataflowOptions) != null) {
                dataflow.templateLocation = (String) clazz.getMethod("getTemplateLocation").invoke(dataflowOptions);
            }
             */
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Failed to set dataflow runner pipeline options", e);
        }
        return dataflow;
    }

    public static Integer getMaxNumWorkers(final PInput input) {
        try {
            final Class<? extends PipelineOptions> clazz = (Class<? extends PipelineOptions>)Class.forName("org.apache.beam.runners.dataflow.options.DataflowPipelineOptions");
            final PipelineOptions dataflowOptions = input.getPipeline().getOptions().as(clazz);
            return (Integer)clazz.getMethod("getMaxNumWorkers").invoke(dataflowOptions);
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Failed to set dataflow runner pipeline options", e);
        }
    }

    public static String getProject(final PipelineOptions options) {
        return options.as(GcpOptions.class).getProject();
    }

    public static String getServiceAccount(final PipelineOptions options) {
        try {
            final Class<? extends PipelineOptions> clazz = (Class<? extends PipelineOptions>)Class.forName("org.apache.beam.runners.dataflow.options.DataflowPipelineOptions");
            final PipelineOptions dataflowOptions = options.as(clazz);
            return (String)clazz.getMethod("getServiceAccount").invoke(dataflowOptions);
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Failed to getServiceAccount", e);
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
