package com.mercari.solution.server.mcp.tool;

import com.mercari.solution.server.agent.tool.DocsReader;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import jakarta.servlet.ServletContext;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

@Tool.Module(
    name="read-docs",
    title="Read Pipeline Documentation",
    description= """
        Read any bundled documentation file by its path relative to the docs root.
        Module documentation (tool 'describe-module') may reference shared documents,
        e.g. a link "../common/filter.md" inside "module/transform/select.md" resolves to
        "module/common/filter.md". Use this tool to follow such references.
        Available documents include: README.md (the config file structure reference),
        system.md (the config's system block reference), options/README.md (pipeline options,
        with per-runner pages options/dataflow.md, options/direct.md, options/flink.md,
        options/spark.md, options/portable.md, options/gcp.md, options/aws.md,
        options/beamsql.md), module/README.md (module list), shared parameter docs
        module/common/{filter,select,strategy,expression,schema,schema-migration,logging,
        template,union,bigtable}.md, and module/failure/pubsub.md (dead-letter sink).
        """,
    inputSchema = """
        {
          "type": "object",
          "properties": {
            "path": {
              "type": "string",
              "description": "Document path relative to the docs root, e.g. 'module/common/filter.md' or 'system.md'."
            }
          },
          "required": ["path"]
        }
        """,
    outputSchema = """
        """
)
public class ReadDocsTool implements Tool {

    // Same docs tree as the agent's DocsReader (src/main/resources/server/docs)
    private static final String DOCS_ROOT_PATH = "/server/docs/";

    @Override
    public void init(ServletContext servletContext) {
    }

    @Override
    public McpSchema.CallToolResult sync(
            final McpSyncServerExchange exchange,
            final McpSchema.CallToolRequest request) {

        final Object pathObj = request.arguments().get("path");
        final String normalized = DocsReader.normalizeDocPath(pathObj == null ? null : pathObj.toString());
        if(normalized == null) {
            return McpSchema.CallToolResult.builder()
                    .addTextContent("read-docs mcp tool requires a .md path relative to the docs root, e.g. 'module/common/filter.md' or 'system.md'")
                    .isError(true)
                    .build();
        }

        final String resourcePath = DOCS_ROOT_PATH + normalized;
        try (final InputStream is = getClass().getResourceAsStream(resourcePath)) {
            if(is == null) {
                return McpSchema.CallToolResult.builder()
                        .addTextContent("Document not found: '" + normalized + "'. Relative links resolve against the referencing document's directory (e.g. '../common/filter.md' in 'module/transform/select.md' is 'module/common/filter.md').")
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
                    .addTextContent("Failed to read document: '" + normalized + "', cause: " + e.getMessage())
                    .isError(true)
                    .build();
        }
    }

}
