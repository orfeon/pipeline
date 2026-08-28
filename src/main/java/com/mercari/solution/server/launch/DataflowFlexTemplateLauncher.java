package com.mercari.solution.server.launch;

import com.google.dataflow.v1beta3.FlexTemplateRuntimeEnvironment;
import com.google.dataflow.v1beta3.Job;
import com.google.dataflow.v1beta3.LaunchFlexTemplateParameter;
import com.google.dataflow.v1beta3.LaunchFlexTemplateResponse;
import com.google.gson.JsonObject;
import com.mercari.solution.config.Options;
import com.mercari.solution.config.options.DataflowOptions;
import com.mercari.solution.server.ServerVersion;
import com.mercari.solution.util.cloud.google.DataflowUtil;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * {@code dataflow/flexTemplate}: launch the config as a Dataflow Flex Template job
 * (template spec from {@code MERCARI_PIPELINE_LAUNCH_DATAFLOW_TEMPLATE_LOCATION} unless given).
 */
public class DataflowFlexTemplateLauncher implements Launcher {

    public static final String KEY_TEMPLATE_LOCATION = "TEMPLATE_LOCATION";

    @Override
    public String runner() {
        return "dataflow";
    }

    @Override
    public String environment() {
        return "flexTemplate";
    }

    @Override
    public boolean isDefaultEnvironment() {
        return true;
    }

    @Override
    public JsonObject launch(final LaunchRequest request) throws Exception {
        final LaunchDefaults defaults = request.defaults();
        final Options options = request.config().getOptions();
        final DataflowOptions dataflow = options == null ? null : options.getDataflow();
        final String runner = runner();

        final String project = defaults.require(runner, LaunchDefaults.KEY_PROJECT,
                request.param("project"), LaunchDefaults.optionsProject(runner, options));
        final String region = defaults.require(runner, LaunchDefaults.KEY_REGION,
                request.param("region"), LaunchDefaults.optionsRegion(runner, options));
        final String template = defaults.resolve(runner, KEY_TEMPLATE_LOCATION,
                        request.param("templateLocation"), dataflow == null ? null : dataflow.getTemplateLocation())
                .orElseThrow(() -> new IllegalArgumentException("Flex Template location is required: specify templateLocation"
                        + " in the launch parameters, options.dataflow.templateLocation in the config, or set "
                        + LaunchDefaults.envName(runner, KEY_TEMPLATE_LOCATION)));
        if(!template.startsWith("gs://")) {
            throw new IllegalArgumentException("templateLocation must start with gs://, but: " + template);
        }

        final Map<String, String> parameters = new HashMap<>();
        parameters.put("config", request.config().getContent());
        final Map<String, String> args = request.argsMap();
        for(final Map.Entry<String, String> entry : args.entrySet()) {
            parameters.put("args." + entry.getKey(), entry.getValue());
        }

        final LaunchFlexTemplateParameter original = DataflowOptions
                .createLaunchFlexTemplateParameter(template, parameters, options);
        final LaunchFlexTemplateParameter launchParameter = withEnvironment(original, request, dataflow);

        final LaunchFlexTemplateResponse resp = DataflowUtil.launchFlexTemplate(project, region, launchParameter, false);
        if(!resp.hasJob()) {
            throw new IllegalStateException("Dataflow did not return a job for the flex template launch: " + resp);
        }
        final Job job = resp.getJob();
        return LaunchResult.job(this)
                .id(job.getId())
                .name(job.getName())
                .project(job.getProjectId())
                .location(job.getLocation())
                .createTime(Instant.ofEpochSecond(job.getCreateTime().getSeconds(), job.getCreateTime().getNanos()).toString())
                .state(job.getCurrentState().name())
                .consoleUrl(DataflowUtil.consoleUrl(job))
                .build();
    }

    /**
     * Fill the runtime environment from the launch defaults where the config options left it empty,
     * and attach the labels the diagnosis tools rely on.
     */
    private LaunchFlexTemplateParameter withEnvironment(
            final LaunchFlexTemplateParameter original,
            final LaunchRequest request,
            final DataflowOptions dataflow) {

        final LaunchDefaults defaults = request.defaults();
        final String runner = runner();
        final FlexTemplateRuntimeEnvironment.Builder builder = FlexTemplateRuntimeEnvironment
                .newBuilder(original.getEnvironment());

        builder.putAllAdditionalUserLabels(LaunchResult.labels(request, runner, ServerVersion.get()));

        // Launch parameters are explicit and win over the config options already copied into the
        // builder; the environment only fills what both left empty.
        if(request.param("serviceAccount") != null) {
            builder.setServiceAccountEmail(request.param("serviceAccount"));
        } else if(builder.getServiceAccountEmail().isEmpty()) {
            defaults.resolve(runner, LaunchDefaults.KEY_SERVICE_ACCOUNT,
                    dataflow == null ? null : dataflow.getServiceAccount()).ifPresent(builder::setServiceAccountEmail);
        }
        if(request.param("subnetwork") != null) {
            builder.setSubnetwork(request.param("subnetwork"));
        } else if(builder.getSubnetwork().isEmpty()) {
            defaults.resolve(runner, LaunchDefaults.KEY_SUBNETWORK,
                    dataflow == null ? null : dataflow.getSubnetwork()).ifPresent(builder::setSubnetwork);
        }
        if(builder.getStagingLocation().isEmpty()) {
            defaults.resolve(runner, LaunchDefaults.KEY_STAGING_LOCATION,
                    dataflow == null ? null : dataflow.getStagingLocation()).ifPresent(builder::setStagingLocation);
        }
        if(builder.getTempLocation().isEmpty()) {
            defaults.resolve(runner, LaunchDefaults.KEY_TEMP_LOCATION,
                    dataflow == null ? null : dataflow.getTempLocation()).ifPresent(builder::setTempLocation);
        }

        return LaunchFlexTemplateParameter
                .newBuilder(original)
                .setEnvironment(builder.build())
                .build();
    }

}
