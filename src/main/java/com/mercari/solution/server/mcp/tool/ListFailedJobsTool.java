package com.mercari.solution.server.mcp.tool;

import com.mercari.solution.server.job.JobReader;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import jakarta.servlet.ServletContext;

/** Recently failed jobs: Dataflow jobs and, when the server has a configured Cloud Run Job, its failed executions. */
@Tool.Module(
    name = "list-failed-jobs",
    title = "List Recently Failed Jobs",
    description = """
        List the pipeline jobs that failed recently: Dataflow jobs in the project / region, plus the
        failed executions of the server's configured Cloud Run Job (direct launches) when there is one.
        Use this when the user mentions a failure but does not know the job id, or to check whether
        anything failed lately; then list-job-errors with the job id / execution name.
        """,
    inputSchema = """
        {
          "type": "object",
          "properties": {
            "hours": {
              "type": "integer",
              "description": "Look-back window in hours. Defaults to 24."
            },
            "project": {
              "type": "string",
              "description": "GCP project id. Defaults to the server's configured project."
            },
            "region": {
              "type": "string",
              "description": "Region. Defaults to the server's configured region."
            }
          }
        }
        """,
    outputSchema = """
        {
          "type": "string"
        }
        """
)
public class ListFailedJobsTool implements Tool {

    @Override
    public void init(final ServletContext servletContext) {
    }

    @Override
    public McpSchema.CallToolResult sync(
            final McpSyncServerExchange exchange,
            final McpSchema.CallToolRequest request) {
        final String result = JobReader.listFailedJobs(
                GetJobTool.optionalInt(request, "hours"),
                GetJobTool.optionalString(request, "project"),
                GetJobTool.optionalString(request, "region"));
        return McpSchema.CallToolResult.builder().addTextContent(result).isError(result.startsWith("ERROR")).build();
    }

}
