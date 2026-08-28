package com.mercari.solution.server.launch;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mercari.solution.util.cloud.google.CloudRunUtil;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * {@code direct/cloudRunJob}: run a <b>pre-created</b> Cloud Run Job built from the {@code direct}
 * image, overriding its container args with {@code --config=...} / {@code --args.*} for this launch.
 * The job's resources (image, service account, cpu, memory, network) are owned by whoever created
 * it (see {@code docs/deploy/cloud-run-jobs.md}); only what {@code jobs.run} can override is exposed:
 * args, task timeout, task count and extra env vars.
 */
public class CloudRunJobLauncher implements Launcher {

    public static final String KEY_JOB = "JOB";
    public static final String KEY_TASK_TIMEOUT = "TASK_TIMEOUT";

    private static final Duration WAIT_INTERVAL = Duration.ofSeconds(5);
    /** Below the Builder request timeout of 300s: a launch must answer before the browser gives up. */
    static final int MAX_WAIT_SECONDS = 240;

    private final CloudRunUtil cloudRun;
    private final ConfigStager stager;

    public CloudRunJobLauncher() {
        this(new CloudRunUtil(), new ConfigStager());
    }

    public CloudRunJobLauncher(final CloudRunUtil cloudRun, final ConfigStager stager) {
        this.cloudRun = cloudRun;
        this.stager = stager;
    }

    @Override
    public String runner() {
        return "direct";
    }

    @Override
    public String environment() {
        return "cloudRunJob";
    }

    @Override
    public boolean isDefaultEnvironment() {
        return true;
    }

    @Override
    public JsonObject launch(final LaunchRequest request) throws Exception {
        final LaunchDefaults defaults = request.defaults();
        final String runner = runner();
        final String project = defaults.require(runner, LaunchDefaults.KEY_PROJECT,
                request.param("project"), LaunchDefaults.optionsProject(runner, request.config().getOptions()));
        final String region = defaults.require(runner, LaunchDefaults.KEY_REGION,
                request.param("region"), LaunchDefaults.optionsRegion(runner, request.config().getOptions()));
        final String job = defaults.resolve(runner, KEY_JOB, request.param("jobName"))
                .orElseThrow(() -> new IllegalArgumentException("Cloud Run Job name is required: specify jobName in the launch"
                        + " parameters or set " + LaunchDefaults.envName(runner, KEY_JOB)
                        + " (create the job first with `gcloud run jobs create`, see docs/deploy/cloud-run-jobs.md)"));
        final String jobName = CloudRunUtil.jobName(project, region, job);

        final String launchId = ConfigStager.newLaunchId();
        final String stagingLocation = defaults.resolve(runner, LaunchDefaults.KEY_STAGING_LOCATION,
                request.param("stagingLocation")).orElse(null);
        final Map<String, String> templateArgs = request.argsMap();
        final String configValue = stager.stage(stagingLocation, launchId, request.config().getContent(),
                ConfigStager.argsBytes(templateArgs));
        final List<String> args = ConfigStager.containerArgs(configValue, templateArgs);

        final JsonObject containerOverride = new JsonObject();
        final JsonArray argsArray = new JsonArray();
        args.forEach(argsArray::add);
        containerOverride.add("args", argsArray);
        final JsonArray envArray = envOverrides(request.param("env"));
        if(envArray.size() > 0) {
            containerOverride.add("env", envArray);
        }
        final JsonArray containerOverrides = new JsonArray();
        containerOverrides.add(containerOverride);

        final JsonObject overrides = new JsonObject();
        overrides.add("containerOverrides", containerOverrides);
        final Integer taskCount = request.paramInt("taskCount");
        if(taskCount != null && taskCount > 0) {
            overrides.addProperty("taskCount", taskCount);
        }
        final Integer taskTimeout = request.paramInt("taskTimeout");
        final String timeoutSeconds = taskTimeout != null
                ? String.valueOf(taskTimeout)
                : defaults.resolve(runner, KEY_TASK_TIMEOUT).orElse(null);
        if(timeoutSeconds != null) {
            overrides.addProperty("timeout", timeoutDuration(timeoutSeconds));
        }

        final JsonObject runRequest = new JsonObject();
        runRequest.add("overrides", overrides);

        final JsonObject operation;
        try {
            operation = cloudRun.runJob(jobName, runRequest);
        } catch (final CloudRunUtil.CloudRunException e) {
            if(e.isNotFound()) {
                throw new IllegalArgumentException("Cloud Run Job " + job + " was not found in " + project + "/" + region
                        + ". Create it first with `gcloud run jobs create " + job + " --project=" + project + " --region=" + region
                        + " --image=<direct image> ...` (see docs/deploy/cloud-run-jobs.md), or launch a different job with jobName", e);
            }
            throw e;
        }

        // The run operation's metadata is the Execution resource.
        JsonObject execution = operation.has("metadata") && operation.get("metadata").isJsonObject()
                ? operation.getAsJsonObject("metadata")
                : new JsonObject();
        final String executionName = execution.has("name") ? execution.get("name").getAsString() : null;

        final Integer wait = request.paramInt("wait");
        if(wait != null && wait > 0 && executionName != null) {
            execution = cloudRun.waitExecution(executionName, Duration.ofSeconds(Math.min(wait, MAX_WAIT_SECONDS)), WAIT_INTERVAL);
        }

        final LaunchResult result = LaunchResult.job(this)
                .id(CloudRunUtil.lastSegment(executionName))
                .name(executionName)
                .project(project)
                .location(region)
                .put("job", job)
                .put("launchId", launchId)
                .put("config", configValue.startsWith("gs://") ? configValue : null)
                .createTime(execution.has("createTime") ? execution.get("createTime").getAsString() : null)
                .state(CloudRunUtil.executionState(execution));
        if(executionName != null) {
            result.consoleUrl(CloudRunUtil.executionConsoleUrl(executionName, project));
        }
        return result.build();
    }

    /** Seconds ({@code 1800} or {@code 1800s}) to the {@code Duration} string Cloud Run expects. */
    static String timeoutDuration(final String value) {
        final String text = value.trim();
        if(text.matches("^[0-9]+s?$")) {
            return text.endsWith("s") ? text : text + "s";
        }
        throw new IllegalArgumentException("task timeout must be a number of seconds (e.g. 1800 or 1800s), but: " + value);
    }

    /** {@code K=V,K=V} (or JSON object text) → Cloud Run {@code EnvVar[]}. */
    static JsonArray envOverrides(final String text) {
        final JsonArray array = new JsonArray();
        if(text == null || text.isBlank()) {
            return array;
        }
        if(text.stripLeading().startsWith("{")) {
            final JsonObject object = new com.google.gson.Gson().fromJson(text, JsonObject.class);
            for(final Map.Entry<String, com.google.gson.JsonElement> entry : object.entrySet()) {
                array.add(envVar(entry.getKey(), entry.getValue().isJsonPrimitive() ? entry.getValue().getAsString() : entry.getValue().toString()));
            }
            return array;
        }
        for(final String pair : text.split(",")) {
            final int eq = pair.indexOf('=');
            if(eq > 0) {
                array.add(envVar(pair.substring(0, eq).trim(), pair.substring(eq + 1).trim()));
            }
        }
        return array;
    }

    private static JsonObject envVar(final String name, final String value) {
        final JsonObject envVar = new JsonObject();
        envVar.addProperty("name", name);
        envVar.addProperty("value", value);
        return envVar;
    }

}
