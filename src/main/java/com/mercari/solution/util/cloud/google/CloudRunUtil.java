package com.mercari.solution.util.cloud.google;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Optional;

/**
 * Thin Cloud Run Admin API v2 client (REST over the JDK HttpClient, like {@link DataprocUtil}).
 * <p>
 * Methods map 1:1 onto REST calls and exchange raw resource JSON ({@code JsonObject}), so the same
 * client serves the server's launch feature and a future {@code run} action service without either
 * knowing about the other. Long-running operations are returned as-is; use {@link #waitOperation}.
 * Errors surface as {@link CloudRunException} carrying the HTTP status and response body.
 */
public class CloudRunUtil {

    public static final String DEFAULT_ENDPOINT = "https://run.googleapis.com/v2/";

    @FunctionalInterface
    public interface TokenSupplier {
        String get() throws IOException;
    }

    public static class CloudRunException extends RuntimeException {
        public final int status;
        public final String body;

        public CloudRunException(final int status, final String body, final String message) {
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
    private final TokenSupplier tokenSupplier;
    private final HttpClient client;

    public CloudRunUtil() {
        this(DEFAULT_ENDPOINT, IAMUtil::getTokenValue);
    }

    public CloudRunUtil(final String endpoint, final TokenSupplier tokenSupplier) {
        this.endpoint = endpoint.endsWith("/") ? endpoint : endpoint + "/";
        this.tokenSupplier = tokenSupplier;
        this.client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    }

    // ---- resource names ----

    public static String jobName(final String project, final String location, final String job) {
        return "projects/" + project + "/locations/" + location + "/jobs/" + job;
    }

    public static String workerPoolName(final String project, final String location, final String workerPool) {
        return "projects/" + project + "/locations/" + location + "/workerPools/" + workerPool;
    }

    /** {@code projects/p/locations/r/jobs/j/executions/e} → {@code e}; any resource name → its last segment. */
    public static String lastSegment(final String resourceName) {
        if(resourceName == null) {
            return null;
        }
        return resourceName.substring(resourceName.lastIndexOf('/') + 1);
    }

    /** Cloud Console page for an execution ({@code projects/p/locations/r/jobs/j/executions/e}). */
    public static String executionConsoleUrl(final String executionName, final String project) {
        final String[] parts = executionName.split("/");
        // projects/{p}/locations/{r}/jobs/{j}/executions/{e}
        if(parts.length < 8) {
            return null;
        }
        return "https://console.cloud.google.com/run/jobs/executions/details/" + parts[3] + "/" + parts[7]
                + "/general?project=" + project;
    }

    /** Cloud Console page for a worker pool ({@code projects/p/locations/r/workerPools/w}). */
    public static String workerPoolConsoleUrl(final String workerPoolName, final String project) {
        final String[] parts = workerPoolName.split("/");
        if(parts.length < 6) {
            return null;
        }
        return "https://console.cloud.google.com/run/workerpools/details/" + parts[3] + "/" + parts[5]
                + "?project=" + project;
    }

    // ---- jobs ----

    /** {@code GET jobs/{job}} */
    public JsonObject getJob(final String jobName) {
        return send("GET", jobName, null);
    }

    /** {@code POST jobs/{job}:run} with a {@code RunJobRequest} body ({@code overrides}, {@code etag}, ...). Returns the operation. */
    public JsonObject runJob(final String jobName, final JsonObject runJobRequest) {
        return send("POST", jobName + ":run", runJobRequest == null ? new JsonObject() : runJobRequest);
    }

    /** {@code GET jobs/{job}/executions/{execution}} */
    public JsonObject getExecution(final String executionName) {
        return send("GET", executionName, null);
    }

    /** {@code GET jobs/{job}/executions} */
    public JsonObject listExecutions(final String jobName, final Integer pageSize) {
        return send("GET", jobName + "/executions" + (pageSize == null ? "" : "?pageSize=" + pageSize), null);
    }

    // ---- worker pools ----

    /** {@code GET workerPools/{workerPool}} */
    public JsonObject getWorkerPool(final String workerPoolName) {
        return send("GET", workerPoolName, null);
    }

    /** {@code POST locations/{location}/workerPools?workerPoolId=} — returns the operation. */
    public JsonObject createWorkerPool(final String project, final String location, final String workerPoolId, final JsonObject workerPool) {
        return send("POST", "projects/" + project + "/locations/" + location + "/workerPools?workerPoolId=" + workerPoolId, workerPool);
    }

    /** {@code PATCH workerPools/{workerPool}} — returns the operation. */
    public JsonObject patchWorkerPool(final String workerPoolName, final JsonObject workerPool) {
        return send("PATCH", workerPoolName, workerPool);
    }

    /** {@code DELETE workerPools/{workerPool}} — returns the operation. */
    public JsonObject deleteWorkerPool(final String workerPoolName) {
        return send("DELETE", workerPoolName, null);
    }

    // ---- operations ----

    /** {@code GET operations/{operation}} */
    public JsonObject getOperation(final String operationName) {
        return send("GET", operationName, null);
    }

    /**
     * Poll an operation until {@code done}. Returns the finished operation; throws {@link CloudRunException}
     * if it finished with an {@code error}, or {@link IllegalStateException} on timeout.
     */
    public JsonObject waitOperation(final JsonObject operation, final Duration timeout, final Duration interval) {
        JsonObject current = operation;
        final long deadline = System.currentTimeMillis() + timeout.toMillis();
        while(!current.has("done") || !current.get("done").getAsBoolean()) {
            if(System.currentTimeMillis() > deadline) {
                throw new IllegalStateException("Operation did not finish within " + timeout + ": " + name(current));
            }
            sleep(interval);
            current = getOperation(name(current));
        }
        if(current.has("error")) {
            final JsonObject error = current.getAsJsonObject("error");
            final int code = error.has("code") ? error.get("code").getAsInt() : 0;
            throw new CloudRunException(code, error.toString(), "Operation " + name(current) + " failed: "
                    + (error.has("message") ? error.get("message").getAsString() : error));
        }
        return current;
    }

    /**
     * Poll an execution until it has completed (succeeded, failed or cancelled). Returns the last
     * execution resource; a null timeout returns the current resource without waiting.
     */
    public JsonObject waitExecution(final String executionName, final Duration timeout, final Duration interval) {
        JsonObject execution = getExecution(executionName);
        if(timeout == null) {
            return execution;
        }
        final long deadline = System.currentTimeMillis() + timeout.toMillis();
        while(!execution.has("completionTime") && System.currentTimeMillis() < deadline) {
            sleep(interval);
            execution = getExecution(executionName);
        }
        return execution;
    }

    /** SUCCEEDED / FAILED / CANCELLED / RUNNING derived from an execution resource's counters and conditions. */
    public static String executionState(final JsonObject execution) {
        if(execution == null) {
            return null;
        }
        if(!execution.has("completionTime")) {
            return "RUNNING";
        }
        final int succeeded = intField(execution, "succeededCount");
        final int failed = intField(execution, "failedCount");
        final int cancelled = intField(execution, "cancelledCount");
        if(failed > 0) {
            return "FAILED";
        }
        if(cancelled > 0) {
            return "CANCELLED";
        }
        if(succeeded > 0) {
            return "SUCCEEDED";
        }
        return "COMPLETED";
    }

    private static int intField(final JsonObject object, final String field) {
        return object.has(field) && !object.get(field).isJsonNull() ? object.get(field).getAsInt() : 0;
    }

    private static String name(final JsonObject operation) {
        return operation.has("name") ? operation.get("name").getAsString() : "?";
    }

    private static void sleep(final Duration interval) {
        try {
            Thread.sleep(interval.toMillis());
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while waiting for Cloud Run", e);
        }
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
                throw new CloudRunException(response.statusCode(), response.body(),
                        "Cloud Run API " + method + " " + path + " failed with status " + response.statusCode()
                                + ": " + errorMessage(response.body()));
            }
            if(response.body() == null || response.body().isBlank()) {
                return new JsonObject();
            }
            final JsonElement element = new Gson().fromJson(response.body(), JsonElement.class);
            return element != null && element.isJsonObject() ? element.getAsJsonObject() : new JsonObject();
        } catch (final IOException e) {
            throw new IllegalStateException("Cloud Run API " + method + " " + path + " failed: " + e.getMessage(), e);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Cloud Run API " + method + " " + path + " interrupted", e);
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
