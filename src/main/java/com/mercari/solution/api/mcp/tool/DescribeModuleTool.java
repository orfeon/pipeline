package com.mercari.solution.api.mcp.tool;

import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import jakarta.servlet.ServletContext;

import java.io.File;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;


@Tool.Module(
    name="describe-module",
    title="Describe Pipeline Module",
    description= """
        Get detailed specification of the module specified by parameter 'id'.
        The response is in JSON format and includes the detailed module specification.
        """,
    inputSchema = """
        {
          "type": "object",
          "properties": {
            "id": {
              "type": "string",
              "description": "Specify module id."
            }
          },
          "required": ["id"]
        }
        """,
    outputSchema = """
        """
)
public class DescribeModuleTool implements Tool {

    private File docsDir;

    @Override
    public void init(ServletContext servletContext) {
        try {
            final URL docsUrl = servletContext.getResource("/docs");
            if(docsUrl == null || !docsUrl.getProtocol().equals("file")) {
                this.docsDir = null;
            }
            this.docsDir = new File(docsUrl.toURI());
        } catch (Throwable e){
            System.out.println("error: " + e.getMessage());
        }
    }

    @Override
    public McpSchema.CallToolResult sync(
            final McpSyncServerExchange exchange,
            final McpSchema.CallToolRequest request) {

        if(!request.arguments().containsKey("id")) {
            return new McpSchema.CallToolResult("describe-module mcp tool requires id parameter", true);
        }

        final String id = request.arguments().get("id").toString();
        try {
            final File file = Paths
                    .get(docsDir.getPath(), "/config/module/" + id + ".md")
                    .toFile();
            if(!file.exists()) {
                return new McpSchema.CallToolResult("Not found module: " + id, true);
            }

            final List<String> lines = Files.readAllLines(file.toPath(), StandardCharsets.UTF_8);
            final String content = getContent(lines);
            return new McpSchema.CallToolResult(content, false);
        } catch (Exception e) {
            return new McpSchema.CallToolResult("Not found module: " + id + ", cause: " + e.getMessage(), true);
        }
    }

    private static String getContent(final List<String> lines) {
        return String.join("\n", lines);
    }

}
