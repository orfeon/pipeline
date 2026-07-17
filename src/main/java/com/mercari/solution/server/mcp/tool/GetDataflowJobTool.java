package com.mercari.solution.server.mcp.tool;

import com.mercari.solution.server.dataflow.DataflowJobReader;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import jakarta.servlet.ServletContext;


@Tool.Module(
    name = "get-dataflow-job",
    title = "Get Dataflow Job",
    description = """
        Get a Dataflow job's status and the pipeline config it was launched with.
        Parameters: 'jobIdOrName' (required, a job id such as '2026-07-17_22_25_11-123...' or an
        exact job name), 'project' (optional), 'region' (optional; both default to the server's
        configured project/region).
        Returns state, timing, SDK version, labels, and the pipeline config recovered from the
        job's launch parameters.
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
              "description": "Dataflow region (e.g. 'asia-northeast1'). Defaults to the server's configured region."
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
public class GetDataflowJobTool implements Tool {

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
                    .addTextContent("get-dataflow-job mcp tool requires jobIdOrName parameter")
                    .isError(true)
                    .build();
        }
        final String result = DataflowJobReader.getJob(
                jobIdOrName.toString(),
                optionalString(request, "project"),
                optionalString(request, "region"));
        return McpSchema.CallToolResult.builder().addTextContent(result).isError(false).build();
    }

    static String optionalString(final McpSchema.CallToolRequest request, final String key) {
        final Object value = request.arguments().get(key);
        return value == null ? null : value.toString();
    }

}
