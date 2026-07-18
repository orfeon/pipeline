package com.mercari.solution.server.mcp.tool;

import com.mercari.solution.server.code.CodeRepository;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import jakarta.servlet.ServletContext;


@Tool.Module(
    name = "find-module-source",
    title = "Find Pipeline Module Implementation Source",
    description = """
        Find the source file implementing a mercari/pipeline module by its config 'module' name.
        Parameters: 'type' (required: source, transform, sink, or failure),
        'name' (required: the module name as written in a config's 'module' field, e.g. 'bigquery').
        Use this to jump from a module in a pipeline config straight to its implementation,
        then read it with tool: 'read-source'.
        """,
    inputSchema = """
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "title": "Module Type",
              "enum": ["source", "transform", "sink", "failure"],
              "description": "Module type."
            },
            "name": {
              "type": "string",
              "title": "Module Name",
              "description": "Module name as written in the config's 'module' field (e.g. 'bigquery', 'select', 'storage')."
            }
          },
          "required": ["type", "name"]
        }
        """,
    outputSchema = """
        {
          "type": "string"
        }
        """
)
public class FindModuleSourceTool implements Tool {

    @Override
    public void init(ServletContext servletContext) {
        CodeRepository.init(servletContext);
    }

    @Override
    public McpSchema.CallToolResult sync(
            final McpSyncServerExchange exchange,
            final McpSchema.CallToolRequest request) {

        final Object type = request.arguments().get("type");
        final Object name = request.arguments().get("name");
        if (type == null || name == null) {
            return McpSchema.CallToolResult.builder()
                    .addTextContent("find-module-source mcp tool requires type and name parameters")
                    .isError(true)
                    .build();
        }
        final CodeRepository.ModuleType moduleType;
        try {
            moduleType = CodeRepository.ModuleType.valueOf(type.toString().trim().toLowerCase());
        } catch (final IllegalArgumentException e) {
            return McpSchema.CallToolResult.builder()
                    .addTextContent("Unknown module type: '" + type + "'. Specify one of: source, transform, sink, failure.")
                    .isError(true)
                    .build();
        }
        final String result = CodeRepository.findModuleSource(moduleType, name.toString());
        return McpSchema.CallToolResult.builder().addTextContent(result).isError(false).build();
    }

}
