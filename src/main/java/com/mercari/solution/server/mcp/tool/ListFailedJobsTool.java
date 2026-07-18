package com.mercari.solution.server.mcp.tool;

import com.mercari.solution.server.dataflow.DataflowJobReader;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import jakarta.servlet.ServletContext;


@Tool.Module(
    name = "list-failed-jobs",
    title = "List Recently Failed Dataflow Jobs",
    description = """
        List Dataflow jobs that failed recently.
        Parameters: 'hours' (optional look-back window, default 24), 'project' (optional),
        'region' (optional). Use this to discover job ids when a failure is reported without one,
        then inspect a job with tool: 'list-job-errors'.
        """,
    inputSchema = """
        {
          "type": "object",
          "properties": {
            "hours": {
              "type": "integer",
              "title": "Hours",
              "description": "Look-back window in hours. Defaults to 24."
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
          "required": []
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
    public void init(ServletContext servletContext) {
    }

    @Override
    public McpSchema.CallToolResult sync(
            final McpSyncServerExchange exchange,
            final McpSchema.CallToolRequest request) {

        final Object hours = request.arguments().get("hours");
        Integer hoursValue = null;
        if (hours instanceof Number number) {
            hoursValue = number.intValue();
        } else if (hours != null) {
            try {
                hoursValue = Integer.parseInt(hours.toString().trim());
            } catch (final NumberFormatException e) {
                // fall back to the default window
            }
        }
        final String result = DataflowJobReader.listRecentFailedJobs(
                hoursValue,
                GetDataflowJobTool.optionalString(request, "project"),
                GetDataflowJobTool.optionalString(request, "region"));
        return McpSchema.CallToolResult.builder().addTextContent(result).isError(false).build();
    }

}
