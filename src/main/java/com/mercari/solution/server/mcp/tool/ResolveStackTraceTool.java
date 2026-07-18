package com.mercari.solution.server.mcp.tool;

import com.mercari.solution.server.code.CodeRepository;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import jakarta.servlet.ServletContext;


@Tool.Module(
    name = "resolve-stack-trace",
    title = "Resolve Stack Trace Against Framework Sources",
    description = """
        Resolve a Java stack trace against mercari/pipeline's bundled source code.
        Parameter: 'stackTrace' (required) — the stack trace text, e.g. from a failed run or a
        Dataflow error log. Returns, for every com.mercari.solution frame, the source file with
        the surrounding lines and the failing line marked.
        Use this FIRST when diagnosing an error that includes a stack trace.
        """,
    inputSchema = """
        {
          "type": "object",
          "properties": {
            "stackTrace": {
              "type": "string",
              "title": "Stack Trace",
              "description": "The stack trace text, including 'at package.Class.method(File.java:line)' frames."
            }
          },
          "required": ["stackTrace"]
        }
        """,
    outputSchema = """
        {
          "type": "string"
        }
        """
)
public class ResolveStackTraceTool implements Tool {

    @Override
    public void init(ServletContext servletContext) {
        CodeRepository.init(servletContext);
    }

    @Override
    public McpSchema.CallToolResult sync(
            final McpSyncServerExchange exchange,
            final McpSchema.CallToolRequest request) {

        final Object stackTrace = request.arguments().get("stackTrace");
        if (stackTrace == null) {
            return McpSchema.CallToolResult.builder()
                    .addTextContent("resolve-stack-trace mcp tool requires stackTrace parameter")
                    .isError(true)
                    .build();
        }
        final String result = CodeRepository.resolveStackTrace(stackTrace.toString());
        return McpSchema.CallToolResult.builder().addTextContent(result).isError(false).build();
    }

}
