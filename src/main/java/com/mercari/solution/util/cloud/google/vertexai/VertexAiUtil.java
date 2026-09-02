package com.mercari.solution.util.cloud.google.vertexai;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.util.cloud.google.CloudRunUtil;
import com.mercari.solution.util.cloud.google.DataflowUtil;
import com.mercari.solution.util.cloud.google.IAMUtil;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Thin Vertex AI API v1 client (REST over the JDK HttpClient, like {@code CloudBuildUtil}) for the
 * generative AI resources: {@code publishers.models:generateContent} / {@code :countTokens},
 * {@code batchPredictionJobs}, {@code cachedContents} and {@code tuningJobs}.
 * <p>
 * Methods map 1:1 onto {@code projects.locations.*} REST calls and exchange raw resource JSON
 * ({@code JsonObject}). The regional endpoint ({@code https://{location}-aiplatform.googleapis.com/v1/})
 * is derived from the location; {@code global} uses {@code https://aiplatform.googleapis.com/v1/}.
 * Errors surface as {@link VertexAiException} carrying the HTTP status and response body.
 */
public class VertexAiUtil {

    public static final String LOCATION_GLOBAL = "global";

    /** Batch prediction job states that no longer change. */
    public static final Set<String> TERMINAL_JOB_STATES = Set.of(
            "JOB_STATE_SUCCEEDED", "JOB_STATE_PARTIALLY_SUCCEEDED", "JOB_STATE_FAILED", "JOB_STATE_CANCELLED", "JOB_STATE_EXPIRED");

    public static class VertexAiException extends RuntimeException {
        public final int status;
        public final String body;

        public VertexAiException(final int status, final String body, final String message) {
            super(message);
            this.status = status;
            this.body = body;
        }

        public boolean isNotFound() {
            return status == 404;
        }

        public boolean isRetryable() {
            return status == 429 || status >= 500;
        }
    }

    /** The access token could not be obtained (missing / invalid credentials) - not a transport error. */
    public static class CredentialsException extends RuntimeException {
        public CredentialsException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }

    /** A fixed endpoint (tests / private endpoints), or null to derive the regional endpoint from the location. */
    private final String endpoint;
    private final CloudRunUtil.TokenSupplier tokenSupplier;
    private final HttpClient client;

    public VertexAiUtil() {
        this(null, IAMUtil::getTokenValue);
    }

    public VertexAiUtil(final String endpoint, final CloudRunUtil.TokenSupplier tokenSupplier) {
        this.endpoint = endpoint == null ? null : (endpoint.endsWith("/") ? endpoint : endpoint + "/");
        this.tokenSupplier = tokenSupplier;
        this.client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    }

    // ---- resource names ----

    public static String endpoint(final String location) {
        if(location == null || LOCATION_GLOBAL.equals(location)) {
            return "https://aiplatform.googleapis.com/v1/";
        }
        return "https://" + location + "-aiplatform.googleapis.com/v1/";
    }

    public static String parent(final String project, final String location) {
        return "projects/" + project + "/locations/" + location;
    }

    /**
     * The model resource used in request paths and batch job bodies: a bare Gemini model id
     * ({@code gemini-2.5-flash}) becomes {@code publishers/google/models/gemini-2.5-flash};
     * {@code publishers/...}, {@code projects/.../endpoints/...} and {@code endpoints/...} are kept.
     */
    public static String modelResource(final String model) {
        if(model == null || model.startsWith("publishers/") || model.startsWith("projects/") || model.startsWith("endpoints/")) {
            return model;
        }
        return "publishers/google/models/" + model;
    }

    /** The full model path under a parent (a {@code projects/...} resource is already absolute). */
    public static String modelPath(final String project, final String location, final String model) {
        final String resource = modelResource(model);
        return resource.startsWith("projects/") ? resource : parent(project, location) + "/" + resource;
    }

    public static String batchPredictionJobName(final String project, final String location, final String jobId) {
        if(jobId.startsWith("projects/")) {
            return jobId;
        }
        return parent(project, location) + "/batchPredictionJobs/" + jobId;
    }

    /** The last path segment of a resource name (the numeric id of a batch prediction job). */
    public static String id(final String resourceName) {
        if(resourceName == null) {
            return null;
        }
        final int index = resourceName.lastIndexOf('/');
        return index < 0 ? resourceName : resourceName.substring(index + 1);
    }

    /** Cloud Console page of a batch prediction job. */
    public static String consoleUrl(final String project, final String location, final String jobId) {
        return "https://console.cloud.google.com/vertex-ai/locations/" + location + "/batch-predictions/" + id(jobId) + "?project=" + project;
    }

    // ---- models ----

    /** {@code POST {model}:generateContent} with a {@code GenerateContentRequest} body — returns the {@code GenerateContentResponse}. */
    public JsonObject generateContent(final String project, final String location, final String model, final JsonObject request) {
        return send(location, "POST", modelPath(project, location, model) + ":generateContent", request);
    }

    /** {@code POST {model}:countTokens} — returns {@code totalTokens} / {@code totalBillableCharacters}. */
    public JsonObject countTokens(final String project, final String location, final String model, final JsonObject request) {
        return send(location, "POST", modelPath(project, location, model) + ":countTokens", request);
    }

    // ---- batchPredictionJobs ----

    /** {@code POST locations/{location}/batchPredictionJobs} with a {@code BatchPredictionJob} body — returns the created job. */
    public JsonObject createBatchPredictionJob(final String project, final String location, final JsonObject job) {
        return send(location, "POST", parent(project, location) + "/batchPredictionJobs", job);
    }

    /** {@code GET batchPredictionJobs/{job}} */
    public JsonObject getBatchPredictionJob(final String project, final String location, final String jobId) {
        return send(location, "GET", batchPredictionJobName(project, location, jobId), null);
    }

    /** Upper bound on the jobs fetched by {@link #listBatchPredictionJobs} before the client-side sort. */
    public static final int LIST_FETCH_LIMIT = 1000;

    /**
     * {@code GET locations/{location}/batchPredictionJobs?filter=&pageSize=} — the jobs matching the
     * filter, newest first. The API documents no ordering and has no {@code orderBy}, so every page
     * (up to {@link #LIST_FETCH_LIMIT} jobs) is fetched, sorted by {@code createTime} client side and
     * then cut to {@code limit} — a {@code limit} of 1 really is the newest match.
     */
    public List<JsonObject> listBatchPredictionJobs(final String project, final String location, final String filter, final int limit) {
        final List<JsonObject> jobs = new ArrayList<>();
        String pageToken = null;
        while(true) {
            final StringBuilder path = new StringBuilder(parent(project, location))
                    .append("/batchPredictionJobs?pageSize=100");
            if(filter != null && !filter.isBlank()) {
                path.append("&filter=").append(URLEncoder.encode(filter, StandardCharsets.UTF_8));
            }
            if(pageToken != null) {
                path.append("&pageToken=").append(URLEncoder.encode(pageToken, StandardCharsets.UTF_8));
            }
            final JsonObject response = send(location, "GET", path.toString(), null);
            for(final JsonElement e : array(response, "batchPredictionJobs")) {
                jobs.add(e.getAsJsonObject());
            }
            if(jobs.size() >= LIST_FETCH_LIMIT || !response.has("nextPageToken") || response.get("nextPageToken").getAsString().isEmpty()) {
                break;
            }
            pageToken = response.get("nextPageToken").getAsString();
        }
        jobs.sort((a, b) -> Optional.ofNullable(string(b, "createTime")).orElse("").compareTo(Optional.ofNullable(string(a, "createTime")).orElse("")));
        return jobs.size() > limit ? new ArrayList<>(jobs.subList(0, limit)) : jobs;
    }

    /** {@code POST batchPredictionJobs/{job}:cancel} — returns an empty object. */
    public JsonObject cancelBatchPredictionJob(final String project, final String location, final String jobId) {
        return send(location, "POST", batchPredictionJobName(project, location, jobId) + ":cancel", new JsonObject());
    }

    // ---- cachedContents ----

    public JsonObject createCachedContent(final String project, final String location, final JsonObject cachedContent) {
        return send(location, "POST", parent(project, location) + "/cachedContents", cachedContent);
    }

    public JsonObject getCachedContent(final String location, final String name) {
        return send(location, "GET", name, null);
    }

    public JsonObject deleteCachedContent(final String location, final String name) {
        return send(location, "DELETE", name, null);
    }

    // ---- tuningJobs ----

    public JsonObject createTuningJob(final String project, final String location, final JsonObject tuningJob) {
        return send(location, "POST", parent(project, location) + "/tuningJobs", tuningJob);
    }

    public JsonObject getTuningJob(final String location, final String name) {
        return send(location, "GET", name, null);
    }

    public JsonObject cancelTuningJob(final String location, final String name) {
        return send(location, "POST", name + ":cancel", new JsonObject());
    }

    // ---- helpers over resources ----

    public static String state(final JsonObject job) {
        final String state = string(job, "state");
        return state == null ? "JOB_STATE_UNSPECIFIED" : state;
    }

    public static boolean isTerminal(final String state) {
        return TERMINAL_JOB_STATES.contains(state);
    }

    /** A string field, or null when absent / JSON null. */
    public static String string(final JsonObject object, final String field) {
        return object != null && object.has(field) && object.get(field).isJsonPrimitive() ? object.get(field).getAsString() : null;
    }

    public static JsonArray array(final JsonObject object, final String field) {
        return object != null && object.has(field) && object.get(field).isJsonArray() ? object.getAsJsonArray(field) : new JsonArray();
    }

    public static JsonObject object(final JsonObject object, final String field) {
        return object != null && object.has(field) && object.get(field).isJsonObject() ? object.getAsJsonObject(field) : null;
    }

    /** {@code error.message} / {@code completionStats} of a batch prediction job as one line, for failure messages. */
    public static String describeFailure(final JsonObject job) {
        final StringBuilder sb = new StringBuilder();
        final JsonObject error = object(job, "error");
        if(error != null) {
            sb.append(" [").append(Optional.ofNullable(string(error, "code")).orElse("?"));
            final String message = string(error, "message");
            if(message != null) {
                sb.append(": ").append(message);
            }
            sb.append("]");
        }
        final JsonObject stats = object(job, "completionStats");
        if(stats != null) {
            sb.append(" completionStats: ").append(stats);
        }
        return sb.toString();
    }

    /** The text parts of the first candidate, concatenated; null when the response has no text. */
    public static String responseText(final JsonObject response) {
        final JsonArray candidates = array(response, "candidates");
        if(candidates.isEmpty() || !candidates.get(0).isJsonObject()) {
            return null;
        }
        final JsonObject content = object(candidates.get(0).getAsJsonObject(), "content");
        if(content == null) {
            return null;
        }
        final StringBuilder sb = new StringBuilder();
        boolean found = false;
        for(final JsonElement part : array(content, "parts")) {
            if(!part.isJsonObject()) {
                continue;
            }
            final JsonObject p = part.getAsJsonObject();
            // thought parts (thinking models) are not the answer
            if(p.has("thought") && p.get("thought").isJsonPrimitive() && p.get("thought").getAsBoolean()) {
                continue;
            }
            final String text = string(p, "text");
            if(text != null) {
                sb.append(text);
                found = true;
            }
        }
        return found ? sb.toString() : null;
    }

    /** {@code candidates[0].finishReason}, or {@code BLOCKED} when the prompt was blocked, or null. */
    public static String finishReason(final JsonObject response) {
        final JsonObject promptFeedback = object(response, "promptFeedback");
        if(promptFeedback != null && string(promptFeedback, "blockReason") != null) {
            return "BLOCKED";
        }
        final JsonArray candidates = array(response, "candidates");
        if(candidates.isEmpty() || !candidates.get(0).isJsonObject()) {
            return null;
        }
        return string(candidates.get(0).getAsJsonObject(), "finishReason");
    }

    /**
     * A resource JSON as a nested map for the action envelope payload (JSON numbers as
     * {@code Long} / {@code Double}; int64 fields, which the REST JSON carries as strings, stay strings).
     */
    public static Map<String, Object> toPayload(final JsonObject json) {
        final Object value = DataflowUtil.toValue(json);
        if(value instanceof Map<?, ?> map) {
            @SuppressWarnings("unchecked")
            final Map<String, Object> result = (Map<String, Object>) map;
            return result;
        }
        return new LinkedHashMap<>();
    }

    // ---- transport ----

    private JsonObject send(final String location, final String method, final String path, final JsonObject body) {
        final String url = (endpoint != null ? endpoint : endpoint(location)) + path;
        final String token;
        try {
            token = tokenSupplier.get();
        } catch (final IOException e) {
            throw new CredentialsException("Vertex AI API " + method + " " + path + ": failed to obtain an access token: " + e.getMessage(), e);
        }
        try {
            final HttpRequest.Builder builder = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(300))
                    .header("Authorization", "Bearer " + token)
                    .header("Content-Type", "application/json");
            final HttpRequest.BodyPublisher publisher = body == null
                    ? HttpRequest.BodyPublishers.noBody()
                    : HttpRequest.BodyPublishers.ofString(body.toString());
            final HttpRequest request = builder.method(method, publisher).build();
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if(response.statusCode() >= 400) {
                throw new VertexAiException(response.statusCode(), response.body(),
                        "Vertex AI API " + method + " " + path + " failed with status " + response.statusCode()
                                + ": " + errorMessage(response.body()));
            }
            if(response.body() == null || response.body().isBlank()) {
                return new JsonObject();
            }
            final JsonElement element = new Gson().fromJson(response.body(), JsonElement.class);
            return element != null && element.isJsonObject() ? element.getAsJsonObject() : new JsonObject();
        } catch (final IOException e) {
            throw new IllegalStateException("Vertex AI API " + method + " " + path + " failed: " + e.getMessage(), e);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Vertex AI API " + method + " " + path + " interrupted", e);
        }
    }

    private static String errorMessage(final String body) {
        try {
            final JsonElement element = new Gson().fromJson(body, JsonElement.class);
            return Optional.ofNullable(element)
                    .filter(JsonElement::isJsonObject)
                    .map(JsonElement::getAsJsonObject)
                    .filter(o -> o.has("error") && o.get("error").isJsonObject())
                    .map(o -> o.getAsJsonObject("error"))
                    .filter(o -> o.has("message"))
                    .map(o -> o.get("message").getAsString())
                    .orElse(body);
        } catch (final Exception e) {
            return body;
        }
    }

}
