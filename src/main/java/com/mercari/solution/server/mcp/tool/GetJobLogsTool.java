package com.mercari.solution.server.mcp.tool;

import com.mercari.solution.server.job.JobReader;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import jakarta.servlet.ServletContext;

/** Cloud Logging entries of a job (Dataflow workers / launcher, or a Cloud Run Job execution). */
@Tool.Module(
    name = "get-job-logs",
    title = "Get Job Logs",
    description = """
        Read the Cloud Logging entries of a launched job: the worker / harness / launcher logs of a
        Dataflow job (by id or exact name) or the container logs of a Cloud Run Job execution (by execution
        name; runner 'direct'). Returns the latest entries (oldest first) at or above 'minSeverity'
        (default INFO), optionally only those containing 'contains'. Use list-job-errors for the
        deduplicated error summary; use this to see what happened around an error (INFO / WARNING context,
        the feature plan report, progress messages) or to grep for a specific text.
        """,
    inputSchema = """
        {
          "type": "object",
          "properties": {
            "job": {
              "type": "string",
              "description": "Dataflow job id / exact job name, or a Cloud Run execution name (projects/.../jobs/.../executions/...)."
            },
            "runner": {
              "type": "string",
              "enum": ["dataflow", "direct"],
              "description": "Force the runner (default: inferred from the job reference)."
            },
            "minSeverity": {
              "type": "string",
              "enum": ["DEBUG", "INFO", "NOTICE", "WARNING", "ERROR", "CRITICAL"],
              "description": "Lowest severity to include (default INFO)."
            },
            "contains": {
              "type": "string",
              "description": "Only entries whose text contains this substring (e.g. 'feature plan', a stage name, an exception class)."
            },
            "limit": {
              "type": "integer",
              "description": "Maximum number of entries (default 100, at most 300); the latest ones are returned."
            },
            "project": {
              "type": "string",
              "description": "GCP project id. Defaults to the server's configured project."
            },
            "region": {
              "type": "string",
              "description": "Region. Defaults to the server's configured region."
            }
          },
          "required": ["job"]
        }
        """,
    outputSchema = """
        {
          "type": "string"
        }
        """
)
public class GetJobLogsTool implements Tool {

    @Override
    public void init(final ServletContext servletContext) {
    }

    @Override
    public McpSchema.CallToolResult sync(
            final McpSyncServerExchange exchange,
            final McpSchema.CallToolRequest request) {
        final String job = GetJobTool.optionalString(request, "job");
        if (job == null) {
            return McpSchema.CallToolResult.builder().addTextContent("get-job-logs mcp tool requires job parameter").isError(true).build();
        }
        final String result = JobReader.getJobLogs(job,
                GetJobTool.optionalString(request, "runner"),
                GetJobTool.optionalString(request, "project"),
                GetJobTool.optionalString(request, "region"),
                GetJobTool.optionalString(request, "minSeverity"),
                GetJobTool.optionalInt(request, "limit"),
                GetJobTool.optionalString(request, "contains"));
        return McpSchema.CallToolResult.builder().addTextContent(result).isError(result.startsWith("ERROR")).build();
    }

}
