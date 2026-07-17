package com.mercari.solution.server.mcp.tool;

import com.mercari.solution.server.dataflow.DataflowJobReader;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import jakarta.servlet.ServletContext;


@Tool.Module(
    name = "list-job-errors",
    title = "List Dataflow Job Errors",
    description = """
        Collect the error information of a Dataflow job: job status, error job messages from the
        Dataflow service, and deduplicated worker error logs (including exception stack traces)
        from Cloud Logging.
        Parameters: 'jobIdOrName' (required), 'project' (optional), 'region' (optional).
        Use this when diagnosing why a job failed. If the result contains Java stack traces,
        pass them to tool: 'resolve-stack-trace' to see the failing source code.
        """,
    inputSchema = """
        {
          "type": "object",
          "properties": {
            "jobIdOrName": {
              "type": "string",
              "title": "Job Id or Name",
              "description": "Dataflow job id or exact job name."
            },
            "project": {
              "type": "string",
              "title": "Project",
              "description": "GCP project id. Defaults to the server's configured project."
            },
            "region": {
              "type": "string",
              "title": "Region",
              "description": "Dataflow region. Defaults to the server's configured region."
            }
          },
          "required": ["jobIdOrName"]
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
    public void init(ServletContext servletContext) {
    }

    @Override
    public McpSchema.CallToolResult sync(
            final McpSyncServerExchange exchange,
            final McpSchema.CallToolRequest request) {

        final Object jobIdOrName = request.arguments().get("jobIdOrName");
        if (jobIdOrName == null) {
            return McpSchema.CallToolResult.builder()
                    .addTextContent("list-job-errors mcp tool requires jobIdOrName parameter")
                    .isError(true)
                    .build();
        }
        final String result = DataflowJobReader.listJobErrors(
                jobIdOrName.toString(),
                GetDataflowJobTool.optionalString(request, "project"),
                GetDataflowJobTool.optionalString(request, "region"));
        return McpSchema.CallToolResult.builder().addTextContent(result).isError(false).build();
    }

}
