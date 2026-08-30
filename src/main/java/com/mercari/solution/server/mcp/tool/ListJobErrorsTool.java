package com.mercari.solution.server.mcp.tool;

import com.mercari.solution.server.job.JobReader;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import jakarta.servlet.ServletContext;

/** Deduplicated error picture of a job, whatever the runner. */
@Tool.Module(
    name = "list-job-errors",
    openWorld = true,
    title = "List Job Errors",
    description = """
        Collect the error information of a launched job: for a Dataflow job (id or exact name) the job
        status, the error job messages from the Dataflow service and the deduplicated worker error logs
        (with exception stack traces) from Cloud Logging; for a Cloud Run Job execution (execution name,
        runner 'direct') the execution status / conditions and its deduplicated error logs.
        Use this first when diagnosing why a job failed. If the result contains Java stack traces, pass
        them to resolve-stack-trace to see the failing source code; use get-job-logs for surrounding context.
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
public class ListJobErrorsTool implements Tool {

    @Override
    public void init(final ServletContext servletContext) {
    }

    @Override
    public McpSchema.CallToolResult sync(
            final McpSyncServerExchange exchange,
            final McpSchema.CallToolRequest request) {
        // 'jobIdOrName' kept as an alias of 'job' for older clients
        final String job = GetJobTool.optionalString(request, "job") != null
                ? GetJobTool.optionalString(request, "job") : GetJobTool.optionalString(request, "jobIdOrName");
        if (job == null) {
            return McpSchema.CallToolResult.builder().addTextContent("list-job-errors mcp tool requires job parameter").isError(true).build();
        }
        final String result = JobReader.listJobErrors(job,
                GetJobTool.optionalString(request, "runner"),
                GetJobTool.optionalString(request, "project"),
                GetJobTool.optionalString(request, "region"));
        return McpSchema.CallToolResult.builder().addTextContent(result).isError(result.startsWith("ERROR")).build();
    }

}
