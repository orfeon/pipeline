package com.mercari.solution.server.launch;

import com.google.gson.JsonObject;

import java.util.Map;

/**
 * Builds the common {@code job} object every launcher returns:
 * <pre>
 * { runner, environment, id, name, project, location, createTime, state, consoleUrl, ... }
 * </pre>
 */
public class LaunchResult {

    public static final String VERSION_LABEL = "mercari-pipeline-version";
    public static final String USER_LABEL = "mercari-pipeline-user";
    public static final String MANAGED_LABEL = "mercari-pipeline-managed";

    private final JsonObject job = new JsonObject();

    public static LaunchResult job(final Launcher launcher) {
        final LaunchResult result = new LaunchResult();
        result.job.addProperty("runner", launcher.runner());
        result.job.addProperty("environment", launcher.environment());
        return result;
    }

    public LaunchResult id(final String id) {
        return put("id", id);
    }

    public LaunchResult name(final String name) {
        return put("name", name);
    }

    public LaunchResult project(final String project) {
        return put("project", project);
    }

    public LaunchResult location(final String location) {
        return put("location", location);
    }

    public LaunchResult createTime(final String createTime) {
        return put("createTime", createTime);
    }

    public LaunchResult state(final String state) {
        return put("state", state);
    }

    public LaunchResult consoleUrl(final String url) {
        return put("consoleUrl", url);
    }

    /** Free-form extra fields (e.g. the stop command for a worker pool). */
    public LaunchResult put(final String key, final String value) {
        if(value != null) {
            job.addProperty(key, value);
        }
        return this;
    }

    public LaunchResult put(final String key, final JsonObject value) {
        if(value != null) {
            job.add(key, value);
        }
        return this;
    }

    public JsonObject build() {
        return job;
    }

    /** GCP label values allow only lowercase letters, digits, '-' and '_', up to 63 chars. */
    public static String sanitizeLabelValue(final String value) {
        final String sanitized = value.trim().toLowerCase().replaceAll("[^a-z0-9_-]", "-");
        return sanitized.length() > 63 ? sanitized.substring(0, 63) : sanitized;
    }

    /** The labels attached to every launched resource (version + user + env-configured extras). */
    public static Map<String, String> labels(final LaunchRequest request, final String runner, final String version) {
        final Map<String, String> labels = request.defaults().labels(runner);
        if(version != null && !version.isBlank()) {
            labels.put(VERSION_LABEL, sanitizeLabelValue(version));
        }
        if(request.userEmail() != null && !request.userEmail().isBlank()) {
            labels.put(USER_LABEL, sanitizeLabelValue(request.userEmail()));
        }
        return labels;
    }

}
