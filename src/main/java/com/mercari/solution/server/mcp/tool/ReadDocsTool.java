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
        Read bundled documentation: a module's reference by 'module' ('{type}/{name}', e.g. 'source/bigquery',
        'transform/feature' - parameters, examples; use 'list-modules' to discover names), or any document by
        'path' relative to the docs root. Module documentation may reference shared documents,
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
            "module": {
              "type": "string",
              "description": "Module id in '{type}/{name}' format (type: source | transform | sink | action), e.g. 'source/bigquery'. Alternative to 'path'."
            },
            "path": {
              "type": "string",
              "description": "Document path relative to the docs root, e.g. 'module/common/filter.md' or 'system.md'."
            }
          },
          "required": []
        }
        """,
    outputSchema = """
        """
)
public class ReadDocsTool implements Tool {

    // Same docs tree as the agent's DocsReader (src/main/resources/server/docs)
    private static final String DOCS_ROOT_PATH = "/server/docs/";
    private static final java.util.Set<String> MODULE_TYPES = java.util.Set.of("source", "transform", "sink", "action");

    @Override
    public void init(ServletContext servletContext) {
    }

    @Override
    public McpSchema.CallToolResult sync(
            final McpSyncServerExchange exchange,
            final McpSchema.CallToolRequest request) {

        final Object moduleObj = request.arguments().get("module");
        final Object pathObj = request.arguments().get("path");
        final String normalized;
        if(moduleObj != null && !moduleObj.toString().isBlank()) {
            // module id '{type}/{name}' → module/{type}/{name}.md (the former describe-module tool)
            final String id = moduleObj.toString().trim();
            final String[] parts = id.split("/");
            if(parts.length != 2 || !MODULE_TYPES.contains(parts[0]) || parts[1].isBlank()) {
                return McpSchema.CallToolResult.builder()
                        .addTextContent("Invalid module id: '" + id + "'. Specify '{type}/{name}' where type is one of source, transform, sink, action (e.g. 'source/bigquery', 'action/bigquery'); use tool 'list-modules' to discover names.")
                        .isError(true)
                        .build();
            }
            normalized = "module/" + parts[0] + "/" + parts[1].toLowerCase() + ".md";
        } else {
            normalized = DocsReader.normalizeDocPath(pathObj == null ? null : pathObj.toString());
        }
        if(normalized == null) {
            return McpSchema.CallToolResult.builder()
                    .addTextContent("read-docs mcp tool requires 'module' ('{type}/{name}', e.g. 'source/bigquery') or 'path' (a .md path relative to the docs root, e.g. 'module/common/filter.md' or 'system.md')")
                    .isError(true)
                    .build();
        }

        final String resourcePath = DOCS_ROOT_PATH + normalized;
        try (final InputStream is = getClass().getResourceAsStream(resourcePath)) {
            if(is == null) {
                final String message = moduleObj != null && !moduleObj.toString().isBlank()
                        ? "Not found module: " + moduleObj.toString().trim() + ". Use tool 'list-modules' to see available modules."
                        : "Document not found: '" + normalized + "'. Relative links resolve against the referencing document's directory (e.g. '../common/filter.md' in 'module/transform/select.md' is 'module/common/filter.md').";
                return McpSchema.CallToolResult.builder()
                        .addTextContent(message)
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
