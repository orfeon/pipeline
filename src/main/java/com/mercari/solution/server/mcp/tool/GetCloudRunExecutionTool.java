package com.mercari.solution.server.mcp.tool;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.server.launch.LaunchDefaults;
import com.mercari.solution.util.cloud.google.CloudRunUtil;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import jakarta.servlet.ServletContext;

import java.util.List;

/**
 * Status of Cloud Run Job executions (the {@code direct/cloudRunJob} launch target): one execution by
 * name, or the latest executions of a job. Complements the Dataflow job tools.
 */
@Tool.Module(
    name = "get-cloud-run-execution",
    title = "Get Cloud Run Job Execution",
    description = """
        Get the status of a Cloud Run Job execution launched by launch-pipeline (runner 'direct',
        environment 'cloudRunJob'), or list the latest executions of a Cloud Run Job.
        Pass 'executionName' (the job object's 'name': projects/.../locations/.../jobs/.../executions/...)
        for one execution, or 'jobName' (+ optional 'project' / 'region', defaulting to the server's launch
        configuration) with optional 'limit' (default 5) for the latest executions.
        Each execution is summarised as {name, id, state (RUNNING | SUCCEEDED | FAILED | CANCELLED),
        createTime, startTime, completionTime, taskCount, succeededCount, failedCount, cancelledCount,
        runningCount, conditions: [{type, state, message}], logUri, consoleUrl}; read the logs at logUri
        (Cloud Logging) when an execution failed.
        """,
    inputSchema = """
        {
          "type": "object",
          "properties": {
            "executionName": {
              "type": "string",
              "description": "Full execution resource name (projects/{project}/locations/{region}/jobs/{job}/executions/{execution})."
            },
            "jobName": {
              "type": "string",
              "description": "Cloud Run Job name (short id) to list executions of. Defaults to the server's MERCARI_PIPELINE_LAUNCH_DIRECT_JOB."
            },
            "project": {
              "type": "string",
              "description": "GCP project id. Defaults to the server's configured project."
            },
            "region": {
              "type": "string",
              "description": "Cloud Run region. Defaults to the server's configured region."
            },
            "limit": {
              "type": "integer",
              "description": "Maximum number of executions to list (default 5)."
            }
          }
        }
        """,
    outputSchema = """
        {
          "type": "object",
          "properties": {
            "status": { "type": "string" },
            "execution": { "type": "object" },
            "executions": { "type": "array" },
            "error": { "type": "string" }
          },
          "required": ["status"]
        }
        """
)
public class GetCloudRunExecutionTool implements Tool {

    private static final List<String> COPIED = List.of(
            "createTime", "startTime", "completionTime", "taskCount",
            "succeededCount", "failedCount", "cancelledCount", "runningCount", "logUri");

    @Override
    public void init(final ServletContext servletContext) {
    }

    @Override
    public McpSchema.CallToolResult sync(
            final McpSyncServerExchange exchange,
            final McpSchema.CallToolRequest request) {

        final String executionName = optionalString(request, "executionName");
        final String jobName = optionalString(request, "jobName");
        final JsonObject response = new JsonObject();
        try {
            final CloudRunUtil cloudRun = new CloudRunUtil();
            if (executionName != null) {
                final JsonObject execution = cloudRun.getExecution(executionName);
                response.addProperty("status", "ok");
                response.add("execution", summarize(execution, projectOf(executionName)));
            } else {
                final LaunchDefaults defaults = LaunchDefaults.get();
                final String project = defaults.require("direct", LaunchDefaults.KEY_PROJECT, optionalString(request, "project"));
                final String region = defaults.require("direct", LaunchDefaults.KEY_REGION, optionalString(request, "region"));
                final String job = defaults.resolve("direct", "JOB", jobName)
                        .orElseThrow(() -> new IllegalArgumentException("get-cloud-run-execution requires executionName or jobName"
                                + " (or set " + LaunchDefaults.envName("direct", "JOB") + ")"));
                final Object limitValue = request.arguments().get("limit");
                final int limit = limitValue == null ? 5 : Integer.parseInt(limitValue.toString());
                final JsonObject list = cloudRun.listExecutions(CloudRunUtil.jobName(project, region, job), limit);
                final JsonArray executions = new JsonArray();
                if (list.has("executions") && list.get("executions").isJsonArray()) {
                    for (final JsonElement e : list.getAsJsonArray("executions")) {
                        executions.add(summarize(e.getAsJsonObject(), project));
                    }
                }
                response.addProperty("status", "ok");
                response.add("executions", executions);
            }
            return McpSchema.CallToolResult.builder().addTextContent(response.toString()).isError(false).build();
        } catch (final Throwable e) {
            response.addProperty("status", "error");
            response.addProperty("error", e.getMessage() == null ? e.toString() : e.getMessage());
            return McpSchema.CallToolResult.builder().addTextContent(response.toString()).isError(true).build();
        }
    }

    static JsonObject summarize(final JsonObject execution, final String project) {
        final JsonObject o = new JsonObject();
        final String name = execution.has("name") ? execution.get("name").getAsString() : null;
        o.addProperty("name", name);
        o.addProperty("id", name == null ? null : CloudRunUtil.lastSegment(name));
        o.addProperty("state", CloudRunUtil.executionState(execution));
        for (final String key : COPIED) {
            if (execution.has(key)) o.add(key, execution.get(key));
        }
        final JsonArray conditions = new JsonArray();
        if (execution.has("conditions") && execution.get("conditions").isJsonArray()) {
            for (final JsonElement c : execution.getAsJsonArray("conditions")) {
                final JsonObject condition = c.getAsJsonObject();
                final JsonObject summary = new JsonObject();
                for (final String key : List.of("type", "state", "message", "reason")) {
                    if (condition.has(key)) summary.add(key, condition.get(key));
                }
                conditions.add(summary);
            }
        }
        o.add("conditions", conditions);
        if (name != null && project != null) o.addProperty("consoleUrl", CloudRunUtil.executionConsoleUrl(name, project));
        return o;
    }

    /** {@code projects/{project}/locations/...} → project. */
    static String projectOf(final String resourceName) {
        final String[] parts = resourceName.split("/");
        return parts.length > 1 && "projects".equals(parts[0]) ? parts[1] : null;
    }

    static String optionalString(final McpSchema.CallToolRequest request, final String key) {
        final Object value = request.arguments().get(key);
        return value == null || value.toString().isBlank() ? null : value.toString();
    }

}
