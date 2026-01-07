package com.mercari.solution.server.api;

import com.google.dataflow.v1beta3.*;
import com.google.gson.*;
import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.config.Options;
import com.mercari.solution.config.Config.Format;
import com.mercari.solution.config.options.DataflowOptions;
import com.mercari.solution.config.options.DirectOptions;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.util.FailureUtil;
import com.mercari.solution.util.gcp.DataflowUtil;
import com.mercari.solution.util.pipeline.OptionUtil;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class PipelineService {

    private static final Logger LOG = LoggerFactory.getLogger(PipelineService.class);

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

    public static class RunResult implements Serializable {

        public final String configText;
        public final String responseText;
        public final String errorMessage;
        public final Boolean isError;

        RunResult(
                final String configText,
                final String responseText,
                final String errorMessage,
                final Boolean isError) {

            this.configText = configText;
            this.responseText = responseText;
            this.errorMessage = errorMessage;
            this.isError = isError;
        }

        public static RunResult succeed(
                final String configText,
                final String responseText) {

            return new RunResult(configText, responseText, null, false);
        }

        public static RunResult failure(
                final String configText,
                final String responseText,
                final String errorMessage) {

            return new RunResult(configText, responseText, errorMessage, true);
        }

    }

    public static void serve(
            final HttpServletRequest request,
            final HttpServletResponse response) throws IOException {

        final String userEmail = request.getHeader(HEADER_NAME_USER_EMAIL);

        String type = "";
        final long startMillis = Instant.now().toEpochMilli();
        try(final Reader reader = request.getReader()) {
            final JsonObject jsonObject = Config.convertConfigJson(reader, Config.Format.unknown);

            if (!jsonObject.has("type")) {
                LOG.info("mercari-pipeline-server");
                throw new IllegalArgumentException("request parameter type is not found");
            }
            type = jsonObject.get("type").getAsString();

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

            switch (type.toLowerCase()) {
                case "run", "dryrun" -> {
                    final boolean dryRun = type.startsWith("dry");
                    final RunResult result = run(configText, argsText, dryRun);
                    log(userEmail, dryRun ? "DryRun" : "Run", !result.isError, result.configText, result.errorMessage);
                    response.getWriter().println(result.responseText);
                }
                case "launch" -> {
                    launch(configText, argsText, response, userEmail);
                }
                default -> throw new IllegalArgumentException("Not supported type: " + type);
            }
        } catch (final Throwable e) {
            final long endMillis = Instant.now().toEpochMilli();
            final JsonObject responseJson = new JsonObject();
            responseJson.addProperty("type", type);
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

    public static RunResult run(
            final String configText,
            final String argsText,
            final Boolean dryRun) {

        String configContent = null;
        final long startMillis = Instant.now().toEpochMilli();
        try {

            //response.setBufferSize(0);
            //response.setHeader("Transfer-Encoding", "chunked");
            //response.getWriter().println("okokokokokok");
            //response.getWriter().flush();
            //response.flushBuffer();

            final Config config = Config.load(configText, null, Format.unknown, parseArgs(argsText));
            configContent = config.getContent();

            final MPipeline.MPipelineOptions pipelineOptions = createPipelineOptions(new String[0]);
            Options.setOptions(pipelineOptions, config.getOptions());
            if(OptionUtil.isStreaming(pipelineOptions)) {
                if(DirectOptions.isBlockOnRun(pipelineOptions)) {
                    DirectOptions.setBlockOnRun(pipelineOptions, false);
                }
            }

            final Pipeline pipeline = Pipeline.create(pipelineOptions);

            final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

            final PipelineResult result = dryRun ? null : pipeline.run();

            final long endMillis = Instant.now().toEpochMilli();

            final JsonObject responseJson = new JsonObject();
            responseJson.addProperty("status", "ok");
            responseJson.addProperty("millis", (endMillis - startMillis));

            {
                final JsonArray outputsArray = new JsonArray();
                for(final Map.Entry<String, MCollection> entry : outputs.entrySet()) {
                    outputsArray.add(entry.getValue().toJsonObject());
                }
                responseJson.add("outputs", outputsArray);
            }
            if(result != null) {
                if(OptionUtil.isStreaming(pipelineOptions)) {
                    final PipelineResult.State state = result.waitUntilFinish(Duration.standardSeconds(WAIT_SECONDS));
                    if(state == null || !state.isTerminal()) {
                        result.cancel();
                    }
                }
                final String metricsJson = Optional
                        .of(result.metrics())
                        .map(MetricResults::allMetrics)
                        .map(MetricQueryResults::toString)
                        .orElse("");
                responseJson.addProperty("metrics", metricsJson);
            }

            return RunResult.succeed(configContent, responseJson.toString());
        } catch (final IllegalModuleException e) {
            final long endMillis = Instant.now().toEpochMilli();
            final JsonObject responseJson = new JsonObject();
            responseJson.addProperty("status", "error");
            responseJson.addProperty("millis", (endMillis - startMillis));
            {
                final JsonObject error = new JsonObject();
                error.addProperty("name", e.name);
                error.addProperty("module", e.module);
                final JsonArray messages = new JsonArray();
                for(final String message : e.errorMessages) {
                    messages.add(message);
                }
                error.add("messages", messages);
                responseJson.add("error", error);
            }

            return RunResult.failure(configContent, responseJson.toString(), String.join(",", e.errorMessages));
        } catch (final Throwable e) {
            final long endMillis = Instant.now().toEpochMilli();
            final JsonObject responseJson = new JsonObject();
            responseJson.addProperty("status", "error");
            responseJson.addProperty("millis", (endMillis - startMillis));
            {
                final JsonObject error = new JsonObject();
                error.addProperty("name", "");
                error.addProperty("module", "pipeline");
                error.addProperty("message", FailureUtil.convertThrowableMessage(e));
                responseJson.add("error", error);
            }

            return RunResult.failure(configContent, responseJson.toString(), FailureUtil.convertThrowableMessage(e));
        }
    }

    private static void launch(
            final String configText,
            final String argsText,
            final HttpServletResponse response,
            final String userEmail) throws IOException {

        final JsonObject responseJson = new JsonObject();
        responseJson.addProperty("type", "launch");

        String configContent = null;
        final long startMillis = Instant.now().toEpochMilli();
        try {
            final Config config = Config.load(configText, null, Format.unknown, parseArgs(argsText));
            configContent = config.getContent();

            final String project = getProject(config.getOptions());
            final String region = getRegion(config.getOptions());
            final String template = getTemplateLocation(config.getOptions());

            final Map<String, String> parameter = new HashMap<>();
            parameter.put("config", configText);
            final LaunchFlexTemplateParameter launchParameter = updateLaunchParameter(DataflowOptions
                    .createLaunchFlexTemplateParameter(template, parameter, config.getOptions()));
            final LaunchFlexTemplateResponse resp = DataflowUtil
                    .launchFlexTemplate(project, region, launchParameter, false);

            final long endMillis = Instant.now().toEpochMilli();
            responseJson.addProperty("millis", (endMillis - startMillis));
            if(!resp.hasJob()) {
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

            log(userEmail, "Launch", true, configContent, null);

            response.getWriter().println(responseJson);
        } catch (final Throwable e) {
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

    private static MPipeline.MPipelineOptions createPipelineOptions(final String[] args) {

        final String defaultProjectId = OptionUtil.getDefaultProject();
        if(defaultProjectId != null) {
            String[] strs = new String[1];
            strs[0] = defaultProjectId;
            return PipelineOptionsFactory
                    .fromArgs(OptionUtil.filterPipelineArgs(args))
                    .as(MPipeline.MPipelineOptions.class);
        } else {
            return PipelineOptionsFactory
                    .fromArgs(OptionUtil.filterPipelineArgs(args))
                    .as(MPipeline.MPipelineOptions.class);
        }

    }

    private static String getTemplateLocation(Options options) {
        if(options != null && options.getDataflow() != null && options.getDataflow().getTemplateLocation() != null) {
            return options.getDataflow().getTemplateLocation();
        }

        final String templateLocation = System.getenv(ENV_VARIABLE_DATAFLOW_TEMPLATE_LOCATION);
        if(templateLocation == null) {
            throw new IllegalModuleException("To launch dataflow, environment variable must be set: MERCARI_PIPELINE_DATAFLOW_TEMPLATE_LOCATION");
        } else if(!templateLocation.startsWith("gs://")) {
            throw new IllegalModuleException("templateLocation must be starts with gs://. but: " + templateLocation);
        }
        return templateLocation;
    }

    private static String getProject(Options options) {
        if(options != null && options.getDataflow() != null && options.getDataflow().getProject() != null) {
            return options.getDataflow().getProject();
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
        if(options != null && options.getDataflow() != null && options.getDataflow().getRegion() != null) {
            return options.getDataflow().getRegion();
        }

        String region = System.getenv(ENV_VARIABLE_DATAFLOW_REGION);
        if(region != null) {
            return region;
        }

        throw new IllegalModuleException("To launch dataflow, environment variable must be set: MERCARI_PIPELINE_DATAFLOW_REGION");
    }

    private static LaunchFlexTemplateParameter updateLaunchParameter(LaunchFlexTemplateParameter originalLaunchParameter) {
        final FlexTemplateRuntimeEnvironment.Builder builder = FlexTemplateRuntimeEnvironment
                .newBuilder(originalLaunchParameter.getEnvironment());

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

    private static Map<String, String> parseArgs(String argsText) {
        if(argsText == null || argsText.isEmpty()) {
            return new HashMap<>();
        }

        try {
            final Map<String, String> parsed = new HashMap<>();
            final JsonObject jsonObject = new Gson().fromJson(argsText, JsonObject.class);
            for(final Map.Entry<String, ?> entry : jsonObject.entrySet()) {
                parsed.put(entry.getKey(), entry.getValue().toString());
            }
            return parsed;
        } catch (final Throwable t) {
            throw new IllegalArgumentException("Failed to parse pipeline args: " + argsText, t);
        }
    }
}
