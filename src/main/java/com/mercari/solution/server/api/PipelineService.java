package com.mercari.solution.server.api;

import com.google.cloud.ServiceOptions;
import com.google.gson.*;
import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.config.Options;
import com.mercari.solution.config.Config.Format;
import com.mercari.solution.config.options.DirectOptions;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.util.FailureUtil;
import com.mercari.solution.util.pipeline.Debug;
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
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;

public class PipelineService {

    private static final Logger LOG = LoggerFactory.getLogger(PipelineService.class);

    private static final String ENV_VARIABLE_WAIT_SECONDS = "MERCARI_PIPELINE_WAIT_SECONDS";
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

            final boolean dryRun = type.startsWith("dry");

            final RunResult result = run(configText, argsText, dryRun);
            log(userEmail, dryRun ? "DryRun" : "Run", !result.isError, result.configText, result.errorMessage);
            response.getWriter().println(result.responseText);
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
            final boolean dryRun) {

        String configContent = null;
        final long startMillis = Instant.now().toEpochMilli();
        try(final Debug.TempDirectory tempDirectory = Debug.createTempDirectory(dryRun)) {
            final Config config = Config.load(configText, null, Format.unknown, argsText);
            configContent = config.getContent();

            final MPipeline.MPipelineOptions pipelineOptions = createPipelineOptions(new String[0]);
            Options.setOptions(pipelineOptions, config.getOptions());
            pipelineOptions
                    .as(MPipeline.MPipelineServerOptions.class)
                    .setWorkDir(Optional
                            .ofNullable(tempDirectory.getPath())
                            .map(Path::toString)
                            .orElse(null));

            final Pipeline pipeline = Pipeline.create(pipelineOptions);
            final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

            final JsonObject responseJson = new JsonObject();
            if(!dryRun) {
                if(OptionUtil.isUnbounded(pipeline)) {
                    if(DirectOptions.isBlockOnRun(pipelineOptions)) {
                        DirectOptions.setBlockOnRun(pipelineOptions, false);
                    }
                }
                final PipelineResult result = pipeline.run(pipelineOptions);
                final PipelineResult.State state = result.waitUntilFinish(Duration.standardSeconds(WAIT_SECONDS));
                if(OptionUtil.isUnbounded(pipeline)) {
                    if(state == null || !state.isTerminal()) {
                        LOG.info("timeout cancel state: {}", state);
                        result.cancel();
                    }
                }
                final JsonArray outputsArray = Debug.readDebugOutputs(tempDirectory);
                responseJson.add("outputs", outputsArray);
                final String metricsJson = Optional
                        .of(result.metrics())
                        .map(MetricResults::allMetrics)
                        .map(MetricQueryResults::toString)
                        .orElse("");
                responseJson.addProperty("metrics", metricsJson);
            }

            final long endMillis = Instant.now().toEpochMilli();

            responseJson.addProperty("status", "ok");
            responseJson.addProperty("millis", (endMillis - startMillis));

            final JsonObject specJson = new JsonObject();
            final JsonArray modulesArray = new JsonArray();
            for(final Map.Entry<String, MCollection> entry : outputs.entrySet()) {
                modulesArray.add(entry.getValue().toJsonObject());
            }
            specJson.add("modules", modulesArray);
            responseJson.add("spec", specJson);

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

    private static MPipeline.MPipelineOptions createPipelineOptions(final String[] args) {
        final String defaultProjectId = ServiceOptions.getDefaultProjectId();
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
