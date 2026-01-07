package com.mercari.solution.api.mcp.tool;

import com.mercari.solution.api.PipelineService;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import jakarta.servlet.ServletContext;


@Tool.Module(
    name="validate-pipeline",
    title="Validate Pipeline",
    description= """
        Validate the pipeline defined in config parameter.
        Results are returned in JSON format.
        If the pipeline definition is valid, outputs the output schema for each step in 'outputs' attribute.
        If the pipeline definition has errors, the error content is output in 'errors' attribute.
        """,
    inputSchema = """
        {
          "type": "object",
          "properties": {
            "config": {
              "type": "string",
              "description": "Definition of pipeline. YAML or JSON format."
            }
          },
          "required": ["config"]
        }
        """,
    outputSchema = """
        """
)
public class ValidatePipelineTool implements Tool {

    @Override
    public void init(ServletContext servletContext) {

    }

    @Override
    public McpSchema.CallToolResult sync(
            final McpSyncServerExchange exchange,
            final McpSchema.CallToolRequest request) {

        if(!request.arguments().containsKey("config")) {
            return new McpSchema.CallToolResult("validate-pipeline mcp tool requires config parameter", true);
        }

        final String config = request.arguments().get("config").toString();
        final PipelineService.RunResult result = PipelineService.run(config, null, true);
        return McpSchema.CallToolResult.builder()
                .addTextContent(result.responseText)
                .isError(result.isError)
                .build();
    }

}
