package com.mercari.solution.module.action.vertexai;

import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.Sleeper;
import com.google.auth.oauth2.AccessToken;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.mercari.solution.config.options.DataflowOptions;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.MElement;
import com.mercari.solution.util.cloud.google.IAMUtil;
import com.mercari.solution.util.cloud.google.vertexai.GeminiUtil;
import com.mercari.solution.module.action.Action;
import com.mercari.solution.module.action.ActionResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.http.HttpClient;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Action service that launches a Vertex AI batch prediction job for a Gemini model
 * (the {@code batchPredictionJobs} REST API).
 *
 * Note: the batchPredictionJobs API has no client-supplied job id, so submission is NOT
 * idempotent — a retried Beam bundle may submit a duplicate job. Use the {@code once} trigger
 * (the default) unless duplicates are acceptable.
 */
@Action.Service(name = "vertexai_gemini")
public class GeminiAction implements Action {

    private static final Logger LOG = LoggerFactory.getLogger(GeminiAction.class);

    private static final Set<String> TERMINAL_STATES = Set.of(
            "JOB_STATE_SUCCEEDED",
            "JOB_STATE_PARTIALLY_SUCCEEDED",
            "JOB_STATE_FAILED",
            "JOB_STATE_CANCELLED",
            "JOB_STATE_EXPIRED");

    private static final Set<String> FAILED_STATES = Set.of(
            "JOB_STATE_FAILED",
            "JOB_STATE_CANCELLED",
            "JOB_STATE_EXPIRED");

    public static class Parameters implements Serializable {

        public Op op;
        public String project;
        public String region;
        public GeminiUtil.BatchPredictionJobsRequest batchPredictionJobsRequest;
        public Boolean wait;
        public Long timeoutSeconds;

        public List<String> validate(final String name) {
            final List<String> errorMessages = new ArrayList<>();
            if(this.region == null) {
                errorMessages.add("action sink[" + name + "].parameters.region must not be null");
            }
            if(this.op == null) {
                errorMessages.add("action sink[" + name + "].parameters.op must not be null");
            } else {
                switch (this.op) {
                    case batchPrediction -> {
                        if(this.batchPredictionJobsRequest == null) {
                            errorMessages.add("action sink[" + name + "].parameters.batchPredictionJobsRequest must not be null");
                        } else {
                            errorMessages.addAll(this.batchPredictionJobsRequest.validate());
                        }
                    }
                }
            }
            if(this.timeoutSeconds != null && this.timeoutSeconds <= 0) {
                errorMessages.add("action sink[" + name + "].parameters.timeoutSeconds must be positive");
            }
            return errorMessages;
        }

        public void setDefaults(final String defaultProjectId) {
            if(this.project == null) {
                this.project = defaultProjectId;
            }
            if(this.wait == null) {
                this.wait = true;
            }
            if(this.timeoutSeconds == null) {
                this.timeoutSeconds = 86400L;
            }
            switch (this.op) {
                case batchPrediction -> this.batchPredictionJobsRequest.setDefaults();
            }
        }

    }

    public enum Op {
        batchPrediction
    }

    private String name;
    private Parameters parameters;


    @Override
    public void configure(final String name, final JsonObject parametersJson, final PipelineOptions options) {
        this.name = name;
        this.parameters = new Gson().fromJson(parametersJson, Parameters.class);
        if(this.parameters == null) {
            throw new IllegalModuleException("action sink[" + name + "].parameters must not be empty");
        }
        final List<String> errorMessages = this.parameters.validate(name);
        if(!errorMessages.isEmpty()) {
            throw new IllegalModuleException(errorMessages);
        }
        this.parameters.setDefaults(DataflowOptions.getProject(options));
    }

    @Override
    public void setup() {

    }

    @Override
    public ActionResult execute(final List<MElement> elements) throws Exception {
        // parameter templating is not supported for this service: elements only control firing
        return switch (parameters.op) {
            case batchPrediction -> batchPredict();
        };
    }

    private ActionResult batchPredict() throws Exception {
        try(final HttpClient httpClient = HttpClient.newHttpClient()) {
            final AccessToken accessToken = IAMUtil.getAccessToken();
            JsonObject job = GeminiUtil.batchPredictionJobs(
                    httpClient, accessToken.getTokenValue(), parameters.project, parameters.region, parameters.batchPredictionJobsRequest);
            final String jobResourceName = getAsString(job, "name");
            LOG.info("action sink[{}] submitted vertexai batch prediction job: {}", name, jobResourceName);

            if(parameters.wait && jobResourceName != null) {
                job = waitJob(httpClient, jobResourceName, job);
            }

            final String state = getAsString(job, "state");
            if(parameters.wait && state != null && FAILED_STATES.contains(state)) {
                throw new IllegalStateException(
                        "vertexai batch prediction job: " + jobResourceName + " ended in state: " + state
                                + ", error: " + Optional.ofNullable(job.get("error")).map(Object::toString).orElse("unknown"));
            }
            return ActionResult.of(Op.batchPrediction.name(), jobResourceName, state, job.toString());
        }
    }

    private JsonObject waitJob(
            final HttpClient httpClient,
            final String jobResourceName,
            final JsonObject submitted) throws Exception {

        final BackOff backOff = new ExponentialBackOff.Builder()
                .setInitialIntervalMillis(5000)
                .setMaxIntervalMillis(60000)
                .setMaxElapsedTimeMillis(Math.toIntExact(Math.min(parameters.timeoutSeconds * 1000L, Integer.MAX_VALUE)))
                .build();
        JsonObject job = submitted;
        String state = getAsString(job, "state");
        while(state == null || !TERMINAL_STATES.contains(state)) {
            if(!BackOffUtils.next(Sleeper.DEFAULT, backOff)) {
                throw new IllegalStateException(
                        "vertexai batch prediction job: " + jobResourceName + " did not complete within timeoutSeconds: " + parameters.timeoutSeconds);
            }
            // Tokens can expire during long waits; fetch per poll (the util is a single request)
            final AccessToken accessToken = IAMUtil.getAccessToken();
            job = GeminiUtil.getBatchPredictionJob(httpClient, accessToken.getTokenValue(), parameters.region, jobResourceName);
            state = getAsString(job, "state");
            LOG.info("action sink[{}] waiting vertexai batch prediction job: {} in state: {}", name, jobResourceName, state);
        }
        return job;
    }

    private static String getAsString(final JsonObject json, final String field) {
        if(json == null || !json.has(field) || json.get(field).isJsonNull()) {
            return null;
        }
        return json.get(field).getAsString();
    }

}
