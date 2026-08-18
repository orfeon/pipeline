package com.mercari.solution.server.mcp.tool;

import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import jakarta.servlet.ServletContext;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Set;

@Tool.Module(
    name="describe-module",
    title="Describe Pipeline Module",
    description= """
        Get the full documentation (parameters, examples) of the module specified by parameter 'id'.
        The id format is '{type}/{name}' where type is one of source, transform, sink, action
        (e.g. 'source/bigquery', 'transform/select', 'sink/spanner', 'action/bigquery').
        Use tool 'list-modules' to discover available module names.
        Shared documents referenced from module docs (e.g. module/common/filter.md) can be read
        with tool 'read-docs'.
        """,
    inputSchema = """
        {
          "type": "object",
          "properties": {
            "id": {
              "type": "string",
              "description": "Module id in '{type}/{name}' format, e.g. 'source/bigquery'."
            }
          },
          "required": ["id"]
        }
        """,
    outputSchema = """
        """
)
public class DescribeModuleTool implements Tool {

    // Same docs tree as the agent's DocsReader (src/main/resources/server/docs)
    private static final String DOCS_MODULE_PATH = "/server/docs/module/";
    private static final Set<String> MODULE_TYPES = Set.of("source", "transform", "sink", "action");

    @Override
    public void init(ServletContext servletContext) {
    }

    @Override
    public McpSchema.CallToolResult sync(
            final McpSyncServerExchange exchange,
            final McpSchema.CallToolRequest request) {

        final Object idObj = request.arguments().get("id");
        if(idObj == null || idObj.toString().isBlank()) {
            return McpSchema.CallToolResult.builder()
                    .addTextContent("describe-module mcp tool requires id parameter in '{type}/{name}' format, e.g. 'source/bigquery'")
                    .isError(true)
                    .build();
        }

        final String id = idObj.toString().trim();
        final String[] parts = id.split("/");
        if(parts.length != 2 || !MODULE_TYPES.contains(parts[0]) || parts[1].isBlank()) {
            return McpSchema.CallToolResult.builder()
                    .addTextContent("Invalid module id: '" + id + "'. Specify '{type}/{name}' where type is one of source, transform, sink, action (e.g. 'source/bigquery', 'action/bigquery').")
                    .isError(true)
                    .build();
        }

        final String resourcePath = DOCS_MODULE_PATH + parts[0] + "/" + parts[1].toLowerCase() + ".md";
        try (final InputStream is = getClass().getResourceAsStream(resourcePath)) {
            if(is == null) {
                return McpSchema.CallToolResult.builder()
                        .addTextContent("Not found module: " + id + ". Use tool 'list-modules' to see available modules.")
                        .isError(true)
                        .build();
            }
            final String content = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            return McpSchema.CallToolResult.builder()
                    .addTextContent(content)
                    .isError(false)
                    .build();
        } catch (Exception e) {
            return McpSchema.CallToolResult.builder()
                    .addTextContent("Failed to read documentation for module: " + id + ", cause: " + e.getMessage())
                    .isError(true)
                    .build();
        }
    }

}
