package com.mercari.solution.server.launch;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.mercari.solution.util.cloud.google.DataprocUtil;

import java.util.HashMap;
import java.util.Map;

/**
 * {@code spark/dataprocServerless}: submit the bundled jar (built with the {@code spark} profile)
 * as a Dataproc Serverless batch running {@code SparkRunner}.
 */
public class DataprocServerlessLauncher implements Launcher {

    public static final String KEY_JARS = "JARS";
    public static final String KEY_VERSION = "VERSION";

    private static final String DEFAULT_VERSION = "3.0";

    @Override
    public String runner() {
        return "spark";
    }

    @Override
    public String environment() {
        return "dataprocServerless";
    }

    @Override
    public boolean isDefaultEnvironment() {
        return true;
    }

    @Override
    public JsonObject launch(final LaunchRequest request) {
        final LaunchDefaults defaults = request.defaults();
        final String runner = runner();
        final String project = defaults.require(runner, LaunchDefaults.KEY_PROJECT,
                request.param("project"), LaunchDefaults.optionsProject(runner, request.config().getOptions()));
        final String region = defaults.require(runner, LaunchDefaults.KEY_REGION,
                request.param("region"), LaunchDefaults.optionsRegion(runner, request.config().getOptions()));
        final String jars = defaults.resolve(runner, KEY_JARS, request.param("jars"))
                .orElseThrow(() -> new IllegalArgumentException("pipeline jar location is required: specify jars in the launch"
                        + " parameters or set " + LaunchDefaults.envName(runner, KEY_JARS)));
        final String version = defaults.resolve(runner, KEY_VERSION, request.param("version")).orElse(DEFAULT_VERSION);

        final String str = new Gson().toJson(request.config().getContent());
        final Map<String, String> args = new HashMap<>();
        args.put("--runner", "SparkRunner");
        args.put("--config", str.substring(1, str.length() - 1));
        for(final Map.Entry<String, String> entry : request.argsMap().entrySet()) {
            args.put("--args." + entry.getKey(), entry.getValue());
        }

        final JsonObject batch = DataprocUtil
                .launchServerlessBatchJob(jars, version, args, project, region, null);

        final String name = batch.has("name") ? batch.get("name").getAsString() : null;
        final String id = name == null ? null : name.substring(name.lastIndexOf('/') + 1);
        return LaunchResult.job(this)
                .id(id)
                .name(name)
                .project(project)
                .location(region)
                .createTime(batch.has("createTime") ? batch.get("createTime").getAsString() : null)
                .state(batch.has("state") ? batch.get("state").getAsString() : null)
                .consoleUrl(id == null ? null
                        : "https://console.cloud.google.com/dataproc/batches/" + region + "/" + id + "?project=" + project)
                .put("batch", batch)
                .build();
    }

}
