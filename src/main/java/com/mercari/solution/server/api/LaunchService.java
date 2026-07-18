package com.mercari.solution.server.api;

import com.google.dataflow.v1beta3.FlexTemplateRuntimeEnvironment;
import com.google.dataflow.v1beta3.Job;
import com.google.dataflow.v1beta3.LaunchFlexTemplateParameter;
import com.google.dataflow.v1beta3.LaunchFlexTemplateResponse;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.config.Options;
import com.mercari.solution.config.options.DataflowOptions;
import com.mercari.solution.server.ServerVersion;
import com.mercari.solution.server.dataflow.DataflowJobReader;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.util.FailureUtil;
import com.mercari.solution.util.cloud.google.DataflowUtil;
import com.mercari.solution.util.cloud.google.DataprocUtil;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class LaunchService {

    private static final Logger LOG = LoggerFactory.getLogger(LaunchService.class);

    private static final String ENV_VARIABLE_WAIT_SECONDS = "MERCARI_PIPELINE_WAIT_SECONDS";
    private static final String ENV_VARIABLE_DATAFLOW_PROJECT = "MERCARI_PIPELINE_DATAFLOW_PROJECT";
    private static final String ENV_VARIABLE_DATAFLOW_REGION = "MERCARI_PIPELINE_DATAFLOW_REGION";
    private static final String ENV_VARIABLE_DATAFLOW_SERVICE_ACCOUNT = "MERCARI_PIPELINE_DATAFLOW_SERVICE_ACCOUNT";
    private static final String ENV_VARIABLE_DATAFLOW_SUBNETWORK = "MERCARI_PIPELINE_DATAFLOW_SUBNETWORK";
    private static final String ENV_VARIABLE_DATAFLOW_STAGING_LOCATION = "MERCARI_PIPELINE_DATAFLOW_STAGING_LOCATION";
    private static final String ENV_VARIABLE_TEMP_LOCATION = "MERCARI_PIPELINE_TEMP_LOCATION";
    private static final String ENV_VARIABLE_DATAFLOW_TEMPLATE_LOCATION = "MERCARI_PIPELINE_DATAFLOW_TEMPLATE_LOCATION";
    private static final String ENV_VARIABLE_GOOGLE_CLOUD_PROJECT = "GOOGLE_CLOUD_PROJECT";

    private static final String HEADER_NAME_USER_EMAIL = "X-Goog-Authenticated-User-Email";

    private static final int WAIT_SECONDS = Optional
            .ofNullable(System.getenv(ENV_VARIABLE_WAIT_SECONDS))
            .map(Integer::valueOf)
            .orElse(10);

    public static void serve(
            final HttpServletRequest request,
            final HttpServletResponse response) throws IOException {

        final String userEmail = request.getHeader(HEADER_NAME_USER_EMAIL);

        final long startMillis = Instant.now().toEpochMilli();
        try(final Reader reader = request.getReader()) {
            final JsonObject jsonObject = Config.convertConfigJson(reader, Config.Format.unknown);

            if (!jsonObject.has("config")) {
                throw new IllegalArgumentException("request parameter config is not found");
            }
            final String configText = jsonObject.get("config").getAsString();
            final String argsText;
            if (jsonObject.has("args")) {
                argsText = jsonObject.get("args").getAsString();
            } else {
                argsText = null;
            }

            if (!jsonObject.has("launch")) {
                throw new IllegalArgumentException("request parameter launch is not found");
            }
            final JsonObject launch = jsonObject.getAsJsonObject("launch");

            launch(configText, argsText, launch, response, userEmail);
        } catch (final Throwable e) {
            final long endMillis = Instant.now().toEpochMilli();
            final JsonObject responseJson = new JsonObject();
            responseJson.addProperty("type", "launch");
            responseJson.addProperty("status", "error");
            responseJson.addProperty("millis", (endMillis - startMillis));
            {
                final JsonObject error = new JsonObject();
                error.addProperty("name", "");
                error.addProperty("module", "server");
                error.addProperty("message", FailureUtil.convertThrowableMessage(e));
                responseJson.add("error", error);
            }
            response.getWriter().println(responseJson);
        }
    }

    private static void launch(
            final String configText,
            final String argsText,
            final JsonObject launch,
            final HttpServletResponse response,
            final String userEmail) throws IOException {


        String configContent = null;
        final long startMillis = Instant.now().toEpochMilli();
        try {
            final Config config = Config.load(configText, null, Config.Format.unknown, argsText);
            configContent = config.getContent();

            if(!launch.has("runner")) {
                throw new IllegalArgumentException("request parameter launch must have runner property");
            }
            final String runner = launch.get("runner").getAsString();
            final JsonObject responseJson = switch (runner) {
                case "dataflow" -> launchDataflow(config);
                case "dataflowTemplate" -> launchFlexDataflowTemplate(config, launch, userEmail);
                case "spark" -> launchSpark(config, launch);
                case "flink" -> launchFlink(config, launch);
                case "direct" -> launchDirect(config, launch);
                default -> throw new IllegalArgumentException("Not supported runner: " + runner);
            };

            log(userEmail, "Launch", true, configContent, null);

            response.getWriter().println(responseJson);
        } catch (final Throwable e) {
            final JsonObject responseJson = new JsonObject();
            final long endMillis = Instant.now().toEpochMilli();
            responseJson.addProperty("status", "error");
            responseJson.addProperty("millis", (endMillis - startMillis));
            {
                final JsonObject error = new JsonObject();
                error.addProperty("name", "");
                error.addProperty("module", "pipeline");
                error.addProperty("message", FailureUtil.convertThrowableMessage(e));
                responseJson.add("error", error);
            }

            log(userEmail, "Launch", false, configContent, FailureUtil.convertThrowableMessage(e));

            response.getWriter().println(responseJson);
        }

    }

    private static String getTemplateLocation(Options options, String templateLocation_) {
        if(options != null && options.getDataflow() != null && options.getDataflow().getTemplateLocation() != null) {
            return options.getDataflow().getTemplateLocation();
        }

        final String templateLocation = Optional
                .ofNullable(templateLocation_)
                .orElseGet(() -> System.getenv(ENV_VARIABLE_DATAFLOW_TEMPLATE_LOCATION));
        if(templateLocation == null) {
            throw new IllegalModuleException("To launch dataflow, environment variable must be set: MERCARI_PIPELINE_DATAFLOW_TEMPLATE_LOCATION");
        } else if(!templateLocation.startsWith("gs://")) {
            throw new IllegalModuleException("templateLocation must be starts with gs://. but: " + templateLocation);
        }
        return templateLocation;
    }

    private static String getProject(Options options) {
        if(options != null) {
            if(options.getDataflow() != null && options.getDataflow().getProject() != null) {
                return options.getDataflow().getProject();
            }
            if(options.getGcp() != null && options.getGcp().getProject() != null) {
                return options.getGcp().getProject();
            }
        }

        String project = System.getenv(ENV_VARIABLE_DATAFLOW_PROJECT);
        if(project != null) {
            return project;
        }

        project = System.getenv(ENV_VARIABLE_GOOGLE_CLOUD_PROJECT);
        if(project != null) {
            return project;
        }

        throw new IllegalModuleException("To launch dataflow, environment variable must be set: MERCARI_PIPELINE_DATAFLOW_PROJECT");
    }

    private static String getRegion(Options options) {
        if(options != null) {
            if(options.getDataflow() != null && options.getDataflow().getRegion() != null) {
                return options.getDataflow().getRegion();
            }
            if(options.getGcp() != null && options.getGcp().getWorkerRegion() != null) {
                return options.getGcp().getWorkerRegion();
            }
        }

        String region = System.getenv(ENV_VARIABLE_DATAFLOW_REGION);
        if(region != null) {
            return region;
        }

        throw new IllegalModuleException("To launch dataflow, environment variable must be set: MERCARI_PIPELINE_DATAFLOW_REGION");
    }

    private static LaunchFlexTemplateParameter updateLaunchParameter(
            LaunchFlexTemplateParameter originalLaunchParameter,
            final String userEmail) {

        final FlexTemplateRuntimeEnvironment.Builder builder = FlexTemplateRuntimeEnvironment
                .newBuilder(originalLaunchParameter.getEnvironment());

        // Labels let the diagnosis tools recover who launched the job and from which build
        // (DataflowJobReader compares the version label against the server's own version).
        final String version = ServerVersion.get();
        if(version != null) {
            builder.putAdditionalUserLabels(DataflowJobReader.VERSION_LABEL, sanitizeLabelValue(version));
        }
        if(userEmail != null && !userEmail.isBlank()) {
            builder.putAdditionalUserLabels("mercari-pipeline-user", sanitizeLabelValue(userEmail));
        }

        final String serviceAccount = System.getenv(ENV_VARIABLE_DATAFLOW_SERVICE_ACCOUNT);
        if(serviceAccount != null) {
            builder.setServiceAccountEmail(serviceAccount);
        }

        final String subnetwork = System.getenv(ENV_VARIABLE_DATAFLOW_SUBNETWORK);
        if(subnetwork != null) {
            builder.setSubnetwork(subnetwork);
        }

        final String stagingLocation = System.getenv(ENV_VARIABLE_DATAFLOW_STAGING_LOCATION);
        if(stagingLocation != null) {
            builder.setStagingLocation(stagingLocation);
        }

        final String tempLocation = System.getenv(ENV_VARIABLE_TEMP_LOCATION);
        if(tempLocation != null) {
            builder.setTempLocation(tempLocation);
        }

        return LaunchFlexTemplateParameter
                .newBuilder(originalLaunchParameter)
                .setEnvironment(builder.build())
                .build();
    }

    /** GCP label values allow only lowercase letters, digits, '-' and '_', up to 63 chars. */
    private static String sanitizeLabelValue(final String value) {
        final String sanitized = value.trim().toLowerCase().replaceAll("[^a-z0-9_-]", "-");
        return sanitized.length() > 63 ? sanitized.substring(0, 63) : sanitized;
    }

    private static JsonObject launchDataflow(final Config config) {
        /*
        final PipelineOptions pipelineOptions = PipelineOptionsFactory
                .fromArgs(
                        "--runner=DataflowRunner",
                        "--region=asia-northeast1",
                        "--project=kouzoh-p-orfeon",
                        "--tempLocation=gs://kouzoh-p-orfeon-dataflow/temp",
                        "--stagingLocation=gs://kouzoh-p-orfeon/staging")
                .as(MPipeline.MPipelineOptions.class);
         */

        final String defaultProjectId = "";


        final PipelineOptions pipelineOptions = PipelineOptionsFactory
                .fromArgs("--runner=DataflowRunner")
                .as(MPipeline.MPipelineOptions.class);
        Options.setOptions(pipelineOptions, config.getOptions());

        final Pipeline pipeline = Pipeline.create(pipelineOptions);

        final long startMillis = Instant.now().toEpochMilli();
        MPipeline.apply(pipeline, config);
        final PipelineResult pipelineResult = pipeline.run();

        final long endMillis = Instant.now().toEpochMilli();

        final JsonObject responseJson = new JsonObject();
        responseJson.addProperty("millis", (endMillis - startMillis));
        responseJson.addProperty("status", pipelineResult.getState().name());

        return responseJson;
    }

    private static JsonObject launchFlexDataflowTemplate(Config config, JsonObject launch, String userEmail) throws IOException {

        final String templateLocation;
        if(launch.has("parameters") && launch.get("parameters").isJsonObject()) {
            templateLocation = launch.getAsJsonObject("parameters").get("templateLocation").getAsString();
        } else {
            templateLocation = null;
        }

        final String project = getProject(config.getOptions());
        final String region = getRegion(config.getOptions());
        final String template = getTemplateLocation(config.getOptions(), templateLocation);

        final Map<String, String> parameter = new HashMap<>();
        parameter.put("config", config.getContent());

        final JsonObject responseJson = new JsonObject();
        final long startMillis = Instant.now().toEpochMilli();
        final LaunchFlexTemplateParameter launchParameter = updateLaunchParameter(DataflowOptions
                .createLaunchFlexTemplateParameter(template, parameter, config.getOptions()), userEmail);
        final LaunchFlexTemplateResponse resp = DataflowUtil
                .launchFlexTemplate(project, region, launchParameter, false);

        final long endMillis = Instant.now().toEpochMilli();
        responseJson.addProperty("millis", (endMillis - startMillis));
        if (!resp.hasJob()) {
            responseJson.addProperty("status", "error");
            responseJson.addProperty("module", "pipeline");
            {
                final JsonObject error = new JsonObject();
                error.addProperty("name", "");
                error.addProperty("message", "Job not found: " + resp.getJob());
                responseJson.add("error", error);
            }
        } else {
            responseJson.addProperty("status", "ok");
            final Job job = resp.getJob();
            {
                final JsonObject jobObject = new JsonObject();
                jobObject.addProperty("id", job.getId());
                jobObject.addProperty("name", job.getName());
                jobObject.addProperty("project", job.getProjectId());
                jobObject.addProperty("location", job.getLocation());
                jobObject.addProperty("createTime", Instant.ofEpochSecond(job.getCreateTime().getSeconds(), job.getCreateTime().getNanos()).toString());
                responseJson.add("job", jobObject);
            }
        }
        return responseJson;
    }

    private static JsonObject launchSpark(Config config, JsonObject launch) {
        final String project = getProject(config.getOptions());
        final String region = getRegion(config.getOptions());

        final String jars;
        final String version;
        if(launch.has("parameters") && launch.get("parameters").isJsonObject()) {
            final JsonObject parameters = launch.getAsJsonObject("parameters");
            if(parameters.has("jars")) {
                jars = parameters.get("jars").getAsString();
            } else {
                jars = null;
            }
            if(parameters.has("version")) {
                version = parameters.get("version").getAsString();
            } else {
                version = null;
            }
        } else {
            throw new IllegalArgumentException("launch.parameters is null");
        }

        final String str = new Gson().toJson(config.getContent());
        final Map<String, String> args = new HashMap<>();
        args.put("--runner", "SparkRunner");
        args.put("--config", str.substring(1, str.length() - 1));



        final long startMillis = Instant.now().toEpochMilli();
        final JsonObject jobResponse = DataprocUtil
                .launchServerlessBatchJob(jars, version, args, project, region,null);
        final long endMillis = Instant.now().toEpochMilli();

        final JsonObject responseJson = new JsonObject();
        responseJson.addProperty("millis", (endMillis - startMillis));
        responseJson.addProperty("status", "ok");
        responseJson.add("job", jobResponse);

        System.out.println(responseJson);

        return responseJson;
    }

    private static JsonObject launchFlink(Config config, JsonObject launch) {
        return new JsonObject();
    }

    private static JsonObject launchDirect(Config config, JsonObject launch) {
        return new JsonObject();
    }

    private static void log(
            final String userEmail,
            final String type,
            final boolean succeeded,
            final String configText,
            final String errorMessage) {

        LOG.info("mercari-pipeline-server: user={}, type={}, succeeded={}, config={}, error={}",
                Optional.ofNullable(userEmail).orElse("unknown"),
                type,
                succeeded,
                configText,
                errorMessage);
    }

}
