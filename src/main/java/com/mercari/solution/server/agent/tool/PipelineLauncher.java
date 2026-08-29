package com.mercari.solution.server.agent.tool;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mercari.solution.server.api.LaunchService;
import dev.langchain4j.agent.tool.P;
import dev.langchain4j.agent.tool.Tool;

/** Agent tool: submit a config to a launch target (Dataflow / Cloud Run Job / Worker Pool / Dataproc). */
public class PipelineLauncher {

    @Tool(name = "launchPipeline", value = """
        Submit a pipeline config to an execution target and return the created job (id, name, project,
        location, state, consoleUrl). runner 'dataflow' launches a Dataflow Flex Template job; runner
        'direct' runs a pre-created Cloud Run Job (environment 'cloudRunJob', default) or creates a Cloud Run
        Worker Pool ('cloudRunWorkerPool'); runner 'spark' submits a Dataproc Serverless batch.
        Only launch after `run` with dryRun succeeded and the user asked to launch. Afterwards report the
        job id / console URL and poll with getDataflowJob / listJobErrors (Dataflow) or
        getCloudRunExecution (Cloud Run Job).
    """)
    public String launch(
            @P(name = "config", description = "Pipeline configuration content in YAML format") String config,
            @P(name = "runner", description = "dataflow | direct | spark") String runner,
            @P(name = "environment", description = "flexTemplate | cloudRunJob | cloudRunWorkerPool | dataprocServerless (default: the runner's default)", required = false) String environment,
            @P(name = "parameters", description = "Launch parameters as a JSON object string: project, region, jobName, serviceAccount, templateLocation, workerMachineType, numWorkers, taskTimeout, wait, ...", required = false) String parameters,
            @P(name = "args", description = "Template arguments as a JSON object string", required = false) String args) {

        try {
            final JsonObject launch = new JsonObject();
            launch.addProperty("runner", runner);
            if (environment != null && !environment.isBlank()) launch.addProperty("environment", environment);
            if (parameters != null && !parameters.isBlank()) launch.add("parameters", JsonParser.parseString(parameters).getAsJsonObject());
            if (args != null && !args.isBlank()) launch.add("args", JsonParser.parseString(args).getAsJsonObject());
            final JsonObject job = LaunchService.launchJob(config, null, launch, null);
            return "SUCCESS: launched\n" + job;
        } catch (final Throwable e) {
            return "ERROR: " + LaunchService.launchErrorMessage(e);
        }
    }

    public static PipelineLauncher create() {
        return new PipelineLauncher();
    }

}
