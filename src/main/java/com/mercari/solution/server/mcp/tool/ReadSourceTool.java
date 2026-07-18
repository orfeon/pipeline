package com.mercari.solution.server.mcp.tool;

import com.mercari.solution.server.code.CodeRepository;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import jakarta.servlet.ServletContext;


@Tool.Module(
    name = "read-source",
    title = "Read Pipeline Framework Source File",
    description = """
        Read a slice of a mercari/pipeline framework source file with line numbers.
        Parameters: 'path' (required, e.g. 'src/main/java/com/mercari/solution/MPipeline.java'),
        'startLine' (optional, 1-based), 'endLine' (optional, inclusive).
        At most 500 lines are returned per call; use startLine/endLine to read further.
        Paths come from tools: 'search-code', 'resolve-stack-trace', 'find-module-source'.
        """,
    inputSchema = """
        {
          "type": "object",
          "properties": {
            "path": {
              "type": "string",
              "title": "Source Path",
              "description": "Source file path, e.g. 'src/main/java/com/mercari/solution/MPipeline.java'."
            },
            "startLine": {
              "type": "integer",
              "title": "Start Line",
              "description": "First line to read (1-based). Defaults to 1."
            },
            "endLine": {
              "type": "integer",
              "title": "End Line",
              "description": "Last line to read (inclusive). Defaults to startLine+499."
            }
          },
          "required": ["path"]
        }
        """,
    outputSchema = """
        {
          "type": "string"
        }
        """
)
public class ReadSourceTool implements Tool {

    @Override
    public void init(ServletContext servletContext) {
        CodeRepository.init(servletContext);
    }

    @Override
    public McpSchema.CallToolResult sync(
            final McpSyncServerExchange exchange,
            final McpSchema.CallToolRequest request) {

        final Object path = request.arguments().get("path");
        if (path == null) {
            return McpSchema.CallToolResult.builder()
                    .addTextContent("read-source mcp tool requires path parameter")
                    .isError(true)
                    .build();
        }
        final String result = CodeRepository.read(
                path.toString(),
                toInteger(request.arguments().get("startLine")),
                toInteger(request.arguments().get("endLine")));
        return McpSchema.CallToolResult.builder().addTextContent(result).isError(false).build();
    }

    private static Integer toInteger(final Object value) {
        return switch (value) {
            case null -> null;
            case Number number -> number.intValue();
            default -> {
                try {
                    yield Integer.parseInt(value.toString().trim());
                } catch (final NumberFormatException e) {
                    yield null;
                }
            }
        };
    }

}
