package com.mercari.solution.server.launch;

import com.google.gson.JsonObject;

/**
 * One way to run a pipeline config somewhere: a Beam {@link #runner()} on an execution
 * {@link #environment()} (e.g. {@code dataflow/flexTemplate}, {@code direct/cloudRunJob}).
 * Selected by {@code launch.runner} + {@code launch.environment} of the {@code /api/launch} request;
 * the schema the Builder UI renders the launch modal from lives in
 * {@code server/api/spec/launch.json} and must list the same runner/environment ids.
 */
public interface Launcher {

    String runner();

    String environment();

    default String key() {
        return runner() + "/" + environment();
    }

    /** Whether this is the environment used when the request names only the runner. */
    default boolean isDefaultEnvironment() {
        return false;
    }

    /**
     * Submit the pipeline. Returns the {@code job} object of the launch response
     * (see {@link LaunchResult#job}); throw to report a failure.
     */
    JsonObject launch(LaunchRequest request) throws Exception;

}
