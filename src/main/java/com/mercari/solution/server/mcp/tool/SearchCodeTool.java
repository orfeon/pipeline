package com.mercari.solution.server.mcp.tool;

import com.mercari.solution.server.code.CodeRepository;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import jakarta.servlet.ServletContext;


@Tool.Module(
    name = "search-code",
    title = "Search Pipeline Framework Source Code",
    description = """
        Search mercari/pipeline's own Java source code with a regular expression.
        Parameters: 'pattern' (required, Java regex; invalid regex is searched as a literal string),
        'pathFilter' (optional, substring filter on file paths such as 'module/sink').
        Returns matching lines as 'path:line: text'.
        Use this to locate the implementation of a feature, a class, an error message string,
        or a config parameter, then read the file with tool: 'read-source'.
        """,
    inputSchema = """
        {
          "type": "object",
          "properties": {
            "pattern": {
              "type": "string",
              "title": "Search Pattern",
              "description": "Java regular expression to search for. Invalid regex is searched as a literal string."
            },
            "pathFilter": {
              "type": "string",
              "title": "Path Filter",
              "description": "Optional substring filter on file paths, e.g. 'module/sink' or 'StorageSink'."
            }
          },
          "required": ["pattern"]
        }
        """,
    outputSchema = """
        {
          "type": "string"
        }
        """
)
public class SearchCodeTool implements Tool {

    @Override
    public void init(ServletContext servletContext) {
        CodeRepository.init(servletContext);
    }

    @Override
    public McpSchema.CallToolResult sync(
            final McpSyncServerExchange exchange,
            final McpSchema.CallToolRequest request) {

        final Object pattern = request.arguments().get("pattern");
        if (pattern == null) {
            return McpSchema.CallToolResult.builder()
                    .addTextContent("search-code mcp tool requires pattern parameter")
                    .isError(true)
                    .build();
        }
        final Object pathFilter = request.arguments().get("pathFilter");
        final String result = CodeRepository.search(
                pattern.toString(),
                pathFilter == null ? null : pathFilter.toString());
        return McpSchema.CallToolResult.builder().addTextContent(result).isError(false).build();
    }

}
