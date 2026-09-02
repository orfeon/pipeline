package com.mercari.solution.server.mcp.tool;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mercari.solution.server.api.LaunchService;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import jakarta.servlet.ServletContext;

/**
 * MCP counterpart of {@code POST /api/launch}: submit a config to a runner / environment target
 * (Dataflow Flex Template, Cloud Run Job / Worker Pool of the direct or prism image, Dataproc Serverless).
 */
@Tool.Module(
    name = "launch-pipeline",
    readOnly = false, idempotent = false, openWorld = true,
    title = "Launch Pipeline",
    description = """
        Submit a pipeline config to an execution target and return the created job:
        runner 'dataflow' (environment 'flexTemplate', default) launches a Dataflow Flex Template job;
        runner 'direct' launches a pre-created Cloud Run Job ('cloudRunJob', default) or creates a Cloud Run
        Worker Pool ('cloudRunWorkerPool') from the direct (DirectRunner) image; runner 'prism' does the same
        with the prism image (Beam's portable local runner — prefer it over 'direct' for pipelines with keyed
        stages over coarse or global keys, e.g. feature transforms; it runs in memory, so keep the input to
        what the job's memory holds); runner 'spark' submits a Dataproc Serverless batch.
        Validate first (run-pipeline with dryRun=true), then launch; afterwards follow the job with
        get-job / get-job-progress / get-job-logs / list-job-errors (pass the returned job id, or the execution name for Cloud Run).
        Launch parameters not given are resolved from the config's options, then the server's
        MERCARI_PIPELINE_LAUNCH[_<RUNNER>]_<KEY> environment, then the server's own project / region.
        Returns the job object {runner, environment, id, name, project, location, state, consoleUrl, ...}.
        """,
    inputSchema = """
        {
          "type": "object",
          "properties": {
            "config": {
              "type": "string",
              "description": "Definition of pipeline. YAML or JSON format."
            },
            "runner": {
              "type": "string",
              "description": "dataflow | direct | prism | spark"
            },
            "environment": {
              "type": "string",
              "description": "flexTemplate (dataflow) | cloudRunJob | cloudRunWorkerPool (direct, prism) | dataprocServerless (spark). Defaults to the runner's default environment."
            },
            "parameters": {
              "type": "object",
              "description": "Launch parameters: project, region, jobName; dataflow: templateLocation, serviceAccount, workerMachineType, numWorkers, maxNumWorkers, ...; cloudRunJob: jobName (Cloud Run Job), taskTimeout, taskCount, env, wait (seconds to wait for completion); cloudRunWorkerPool: image, serviceAccount, cpu, memory, instances."
            },
            "args": {
              "type": ["object", "string"],
              "description": "Template arguments as a JSON object or JSON text. The config refers to them as ${args.<name>} (a bare ${name} is not substituted); values default from the config's own args block, and a placeholder left without a value is refused before launching."
            }
          },
          "required": ["config", "runner"]
        }
        """,
    outputSchema = """
        {
          "type": "object",
          "properties": {
            "status": { "type": "string" },
            "job": { "type": "object" },
            "error": { "type": "string" }
          },
          "required": ["status"]
        }
        """
)
public class LaunchPipelineTool implements Tool {

    private static final Gson GSON = new Gson();

    @Override
    public void init(final ServletContext servletContext) {
    }

    @Override
    public McpSchema.CallToolResult sync(
            final McpSyncServerExchange exchange,
            final McpSchema.CallToolRequest request) {

        final Object config = request.arguments().get("config");
        final Object runner = request.arguments().get("runner");
        if (config == null || runner == null) {
            return McpSchema.CallToolResult.builder()
                    .addTextContent("launch-pipeline mcp tool requires config and runner parameters")
                    .isError(true)
                    .build();
        }
        final JsonObject response = new JsonObject();
        try {
            final JsonObject launch = new JsonObject();
            launch.addProperty("runner", runner.toString());
            final Object environment = request.arguments().get("environment");
            if (environment != null) launch.addProperty("environment", environment.toString());
            final Object parameters = request.arguments().get("parameters");
            if (parameters != null) launch.add("parameters", toJsonObject(parameters, "parameters"));
            final Object args = request.arguments().get("args");
            if (args != null) launch.add("args", toJsonObject(args, "args"));
            final JsonObject job = LaunchService.launchJob(config.toString(), null, launch, null);
            response.addProperty("status", "ok");
            response.add("job", job);
            return McpSchema.CallToolResult.builder().addTextContent(response.toString()).isError(false).build();
        } catch (final Throwable e) {
            response.addProperty("status", "error");
            response.addProperty("error", LaunchService.launchErrorMessage(e));
            return McpSchema.CallToolResult.builder().addTextContent(response.toString()).isError(true).build();
        }
    }

    static JsonObject toJsonObject(final Object value, final String name) {
        try {
            final com.google.gson.JsonElement element = value instanceof String s ? JsonParser.parseString(s) : JsonParser.parseString(GSON.toJson(value));
            if (!element.isJsonObject()) throw new IllegalArgumentException("not a JSON object");
            return element.getAsJsonObject();
        } catch (final RuntimeException e) {
            throw new IllegalArgumentException("launch parameter '" + name + "' must be a JSON object: " + e.getMessage());
        }
    }

}
