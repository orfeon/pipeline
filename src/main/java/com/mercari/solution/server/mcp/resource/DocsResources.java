package com.mercari.solution.server.mcp.resource;

import com.google.common.reflect.ClassPath;
import io.modelcontextprotocol.server.McpServerFeatures;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import jakarta.servlet.ServletContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class DocsResources implements Resources {

    private static final Logger LOG = LoggerFactory.getLogger(DocsResources.class);

    // Same docs tree as the agent's DocsReader (src/main/resources/server/docs)
    private static final String DOCS_RESOURCE_PREFIX = "server/docs/";

    public List<McpServerFeatures.SyncResourceSpecification> sync(ServletContext servletContext) {
        final List<McpServerFeatures.SyncResourceSpecification> resources = new ArrayList<>();
        try {
            final ClassPath classPath = ClassPath.from(DocsResources.class.getClassLoader());
            for (final ClassPath.ResourceInfo resourceInfo : classPath.getResources()) {
                final String resourceName = resourceInfo.getResourceName();
                if (!resourceName.startsWith(DOCS_RESOURCE_PREFIX) || !resourceName.endsWith(".md")) {
                    continue;
                }
                final String name = resourceName.substring(DOCS_RESOURCE_PREFIX.length());
                try {
                    final String content = resourceInfo.asCharSource(StandardCharsets.UTF_8).read();
                    final List<String> lines = content.lines().toList();

                    final McpServerFeatures.SyncResourceSpecification resource = new McpServerFeatures.SyncResourceSpecification(
                            McpSchema.Resource.builder()
                                    .uri("docs://" + name)
                                    .name(name) // required by the MCP schema (programmatic id); title is the human-readable one
                                    .title(getTitle(lines, name))
                                    .description(getDescription(lines))
                                    .mimeType("text/plain")
                                    .annotations(new McpSchema.Annotations(List.of(McpSchema.Role.ASSISTANT), 0.5D))
                                    .build(),
                            (McpSyncServerExchange exchange, McpSchema.ReadResourceRequest request) -> {
                                final McpSchema.ResourceContents resourceContents = new McpSchema.TextResourceContents(
                                        request.uri(),
                                        "text/plain",
                                        content);
                                return new McpSchema.ReadResourceResult(List.of(resourceContents));
                            });

                    resources.add(resource);
                } catch (Throwable e) {
                    LOG.error("Failed to read docs resource: " + resourceName, e);
                }
            }
        } catch (Throwable e){
            LOG.error("Failed to list docs resources", e);
        }

        return resources;
    }

    private static String getTitle(final List<String> lines, final String name) {
        final String fromFrontMatter = getFrontMatterValue(lines, "title:");
        if (fromFrontMatter != null) {
            return fromFrontMatter;
        }
        for (final String line : lines) {
            if (line.startsWith("# ")) {
                return line.substring(2).trim();
            }
        }
        return name;
    }

    private static String getDescription(final List<String> lines) {
        final String fromFrontMatter = getFrontMatterValue(lines, "description:");
        if (fromFrontMatter != null) {
            return fromFrontMatter;
        }
        // first non-empty line after the H1 title
        boolean afterTitle = false;
        for (final String line : lines) {
            if (!afterTitle) {
                if (line.startsWith("# ")) {
                    afterTitle = true;
                }
                continue;
            }
            if (!line.isBlank()) {
                return line.trim();
            }
        }
        return "";
    }

    private static String getFrontMatterValue(final List<String> lines, final String key) {
        if (lines.isEmpty() || !lines.getFirst().trim().equals("---")) {
            return null;
        }
        for (int i = 1; i < lines.size(); i++) {
            final String line = lines.get(i).trim();
            if (line.equals("---")) {
                return null;
            }
            if (line.startsWith(key)) {
                return line.substring(key.length()).trim();
            }
        }
        return null;
    }

}
