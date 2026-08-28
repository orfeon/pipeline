package com.mercari.solution.util.cloud.google;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Thin Cloud Build API v1 client (REST over the JDK HttpClient, like {@link CloudRunUtil}).
 * <p>
 * Methods map 1:1 onto {@code projects.locations.*} REST calls and exchange raw resource JSON
 * ({@code JsonObject}). Long-running operations ({@code builds.create}, {@code builds.retry},
 * {@code builds.approve}, {@code triggers.run}) are returned as-is; their {@code metadata.build.id}
 * identifies the build to poll with {@link #getBuild}. Errors surface as {@link CloudBuildException}
 * carrying the HTTP status and response body.
 */
public class CloudBuildUtil {

    public static final String DEFAULT_ENDPOINT = "https://cloudbuild.googleapis.com/v1/";

    /** Build statuses that no longer change. */
    public static final Set<String> TERMINAL_STATUSES = Set.of("SUCCESS", "FAILURE", "INTERNAL_ERROR", "TIMEOUT", "CANCELLED", "EXPIRED");

    public static class CloudBuildException extends RuntimeException {
        public final int status;
        public final String body;

        public CloudBuildException(final int status, final String body, final String message) {
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

    private final String endpoint;
    private final CloudRunUtil.TokenSupplier tokenSupplier;
    private final HttpClient client;

    public CloudBuildUtil() {
        this(DEFAULT_ENDPOINT, IAMUtil::getTokenValue);
    }

    public CloudBuildUtil(final String endpoint, final CloudRunUtil.TokenSupplier tokenSupplier) {
        this.endpoint = endpoint.endsWith("/") ? endpoint : endpoint + "/";
        this.tokenSupplier = tokenSupplier;
        this.client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    }

    // ---- resource names ----

    public static String parent(final String project, final String location) {
        return "projects/" + project + "/locations/" + location;
    }

    public static String buildName(final String project, final String location, final String buildId) {
        return parent(project, location) + "/builds/" + buildId;
    }

    public static String triggerName(final String project, final String location, final String trigger) {
        return parent(project, location) + "/triggers/" + trigger;
    }

    /** Cloud Console page of a build. */
    public static String consoleUrl(final String project, final String location, final String buildId) {
        return "https://console.cloud.google.com/cloud-build/builds;region=" + location + "/" + buildId + "?project=" + project;
    }

    // ---- builds ----

    /** {@code POST locations/{location}/builds} with a {@code Build} body — returns the operation ({@code metadata.build}). */
    public JsonObject createBuild(final String project, final String location, final JsonObject build) {
        return send("POST", parent(project, location) + "/builds", build);
    }

    /** {@code GET builds/{build}} */
    public JsonObject getBuild(final String project, final String location, final String buildId) {
        return send("GET", buildName(project, location, buildId), null);
    }

    /**
     * {@code GET locations/{location}/builds?filter=&pageSize=} — the builds (newest first), following
     * pages up to {@code limit}.
     */
    public List<JsonObject> listBuilds(final String project, final String location, final String filter, final int limit) {
        final List<JsonObject> builds = new ArrayList<>();
        String pageToken = null;
        while(true) {
            final StringBuilder path = new StringBuilder(parent(project, location))
                    .append("/builds?pageSize=").append(Math.min(limit, 100));
            if(filter != null && !filter.isBlank()) {
                path.append("&filter=").append(URLEncoder.encode(filter, StandardCharsets.UTF_8));
            }
            if(pageToken != null) {
                path.append("&pageToken=").append(URLEncoder.encode(pageToken, StandardCharsets.UTF_8));
            }
            final JsonObject response = send("GET", path.toString(), null);
            if(response.has("builds") && response.get("builds").isJsonArray()) {
                for(final JsonElement e : response.getAsJsonArray("builds")) {
                    builds.add(e.getAsJsonObject());
                    if(builds.size() >= limit) {
                        return builds;
                    }
                }
            }
            if(!response.has("nextPageToken") || response.get("nextPageToken").getAsString().isEmpty()) {
                return builds;
            }
            pageToken = response.get("nextPageToken").getAsString();
        }
    }

    /** {@code POST builds/{build}:cancel} — returns the (cancelled) {@code Build}. */
    public JsonObject cancelBuild(final String project, final String location, final String buildId) {
        return send("POST", buildName(project, location, buildId) + ":cancel", new JsonObject());
    }

    /** {@code POST builds/{build}:retry} — returns the operation of the new build. */
    public JsonObject retryBuild(final String project, final String location, final String buildId) {
        return send("POST", buildName(project, location, buildId) + ":retry", new JsonObject());
    }

    /** {@code POST builds/{build}:approve} with {@code approvalResult} — returns the operation. */
    public JsonObject approveBuild(final String project, final String location, final String buildId, final JsonObject approvalResult) {
        final JsonObject body = new JsonObject();
        body.add("approvalResult", approvalResult);
        return send("POST", buildName(project, location, buildId) + ":approve", body);
    }

    // ---- triggers ----

    /** {@code POST triggers/{trigger}:run} with an optional {@code source} ({@code RepoSource}) — returns the operation. */
    public JsonObject runTrigger(final String project, final String location, final String trigger, final JsonObject source) {
        final JsonObject body = new JsonObject();
        if(source != null) {
            body.add("source", source);
        }
        return send("POST", triggerName(project, location, trigger) + ":run", body);
    }

    /** {@code GET triggers/{trigger}} */
    public JsonObject getTrigger(final String project, final String location, final String trigger) {
        return send("GET", triggerName(project, location, trigger), null);
    }

    // ---- helpers over resources ----

    /** The {@code Build} embedded in a build operation ({@code metadata.build}), or null. */
    public static JsonObject operationBuild(final JsonObject operation) {
        if(operation == null || !operation.has("metadata") || !operation.get("metadata").isJsonObject()) {
            return null;
        }
        final JsonObject metadata = operation.getAsJsonObject("metadata");
        return metadata.has("build") && metadata.get("build").isJsonObject() ? metadata.getAsJsonObject("build") : null;
    }

    public static String status(final JsonObject build) {
        return build != null && build.has("status") && !build.get("status").isJsonNull() ? build.get("status").getAsString() : "STATUS_UNKNOWN";
    }

    public static String id(final JsonObject build) {
        return build != null && build.has("id") && !build.get("id").isJsonNull() ? build.get("id").getAsString() : null;
    }

    public static boolean isTerminal(final String status) {
        return TERMINAL_STATUSES.contains(status);
    }

    /** {@code statusDetail} / {@code failureInfo} / {@code logUrl} of a build as one line, for failure messages. */
    public static String describeFailure(final JsonObject build) {
        final StringBuilder sb = new StringBuilder();
        if(build.has("statusDetail")) {
            sb.append(" ").append(build.get("statusDetail").getAsString());
        }
        if(build.has("failureInfo") && build.get("failureInfo").isJsonObject()) {
            final JsonObject info = build.getAsJsonObject("failureInfo");
            sb.append(" [").append(info.has("type") ? info.get("type").getAsString() : "?");
            if(info.has("detail")) {
                sb.append(": ").append(info.get("detail").getAsString());
            }
            sb.append("]");
        }
        if(build.has("logUrl")) {
            sb.append(" log: ").append(build.get("logUrl").getAsString());
        }
        return sb.toString();
    }

    /**
     * The {@code Build} resource as a nested map for the action envelope payload (JSON numbers as
     * {@code Long} / {@code Double}; int64 fields, which the REST JSON carries as strings, stay strings).
     */
    public static Map<String, Object> toPayload(final JsonObject build) {
        final Object value = DataflowUtil.toValue(build);
        if(value instanceof Map<?, ?> map) {
            @SuppressWarnings("unchecked")
            final Map<String, Object> result = (Map<String, Object>) map;
            return result;
        }
        return new java.util.LinkedHashMap<>();
    }

    /** google.rpc.Code of an operation error to HTTP status. */
    public static int httpStatus(final int rpcCode) {
        return CloudRunUtil.httpStatus(rpcCode);
    }

    // ---- transport ----

    private JsonObject send(final String method, final String path, final JsonObject body) {
        final String url = endpoint + path;
        try {
            final HttpRequest.Builder builder = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(60))
                    .header("Authorization", "Bearer " + tokenSupplier.get())
                    .header("Content-Type", "application/json");
            final HttpRequest.BodyPublisher publisher = body == null
                    ? HttpRequest.BodyPublishers.noBody()
                    : HttpRequest.BodyPublishers.ofString(body.toString());
            final HttpRequest request = builder.method(method, publisher).build();
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if(response.statusCode() >= 400) {
                throw new CloudBuildException(response.statusCode(), response.body(),
                        "Cloud Build API " + method + " " + path + " failed with status " + response.statusCode()
                                + ": " + errorMessage(response.body()));
            }
            if(response.body() == null || response.body().isBlank()) {
                return new JsonObject();
            }
            final JsonElement element = new Gson().fromJson(response.body(), JsonElement.class);
            return element != null && element.isJsonObject() ? element.getAsJsonObject() : new JsonObject();
        } catch (final IOException e) {
            throw new IllegalStateException("Cloud Build API " + method + " " + path + " failed: " + e.getMessage(), e);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Cloud Build API " + method + " " + path + " interrupted", e);
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

    public static JsonArray array(final JsonObject object, final String field) {
        return object.has(field) && object.get(field).isJsonArray() ? object.getAsJsonArray(field) : new JsonArray();
    }

}
