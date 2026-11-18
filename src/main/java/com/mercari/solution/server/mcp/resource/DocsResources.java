package com.mercari.solution.server.mcp.resource;

import com.google.common.reflect.ClassPath;
import com.mercari.solution.server.mcp.McpCaching;
import io.modelcontextprotocol.modern.McpSchema;
import io.modelcontextprotocol.modern.server.McpRequestContext;
import io.modelcontextprotocol.modern.server.McpSyncResponse;
import io.modelcontextprotocol.modern.server.feature.McpSyncResource;
import io.modelcontextprotocol.modern.server.feature.McpSyncResourceTemplate;
import jakarta.servlet.ServletContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

public class DocsResources implements Resources {

    private static final Logger LOG = LoggerFactory.getLogger(DocsResources.class);

    // Same docs tree as the agent's DocsReader (src/main/resources/server/docs)
    private static final String DOCS_RESOURCE_PREFIX = "server/docs/";

    private static final String MIME_TYPE = "text/markdown";

    /** The per-module reference pages: {@code docs://module/<type>/<name>.md}. */
    static final String MODULE_TEMPLATE = "docs://module/{type}/{name}.md";
    private static final String MODULE_URI_PREFIX = "docs://module/";
    private static final int MAX_COMPLETIONS = 100;

    /** URIs of the published documents, in order (filled by {@link #sync}; completions are answered from it). */
    private final TreeSet<String> uris = new TreeSet<>();

    @Override
    public List<Entry> sync(ServletContext servletContext) {
        final List<Entry> resources = new ArrayList<>();
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
                    final String uri = "docs://" + name;

                    final McpSchema.Resource resource = McpSchema.Resource.builder(uri, name) // name: programmatic id; title: human-readable
                            .title(getTitle(lines, name))
                            .description(getDescription(lines))
                            .mimeType(MIME_TYPE)
                            .size((long) content.getBytes(StandardCharsets.UTF_8).length)
                            .annotations(McpSchema.Annotations.builder()
                                    .audience(List.of(McpSchema.Role.ASSISTANT))
                                    .priority(0.5D)
                                    .build())
                            .build();
                    // The documents are bundled in the image: the same for every caller until the next deployment.
                    final McpSyncResource handler = McpSyncResource.of(
                            (McpRequestContext ctx, McpSchema.ReadResourceRequest request) -> McpSyncResponse.result(
                                    McpSchema.ReadResourceResult
                                            .builder(List.of(new McpSchema.TextResourceContents(request.uri(), MIME_TYPE, content, null)))
                                            .ttlMs(McpCaching.DOCUMENT_TTL_MS)
                                            .cacheScope(McpSchema.CacheScope.PUBLIC)
                                            .build()));

                    resources.add(new Entry(resource, handler));
                    uris.add(uri);
                } catch (Throwable e) {
                    LOG.error("Failed to read docs resource: " + resourceName, e);
                }
            }
        } catch (Throwable e){
            LOG.error("Failed to list docs resources", e);
        }

        return resources;
    }

    @Override
    public List<McpSyncResourceTemplate> templates() {
        final McpSchema.ResourceTemplate template = McpSchema.ResourceTemplate.builder(MODULE_TEMPLATE, "module-reference")
                .title("Module reference")
                .description("Configuration reference (parameters, examples) of a pipeline module: "
                        + "type is source, transform, sink or action, name is the module name (e.g. docs://module/source/bigquery.md)")
                .mimeType(MIME_TYPE)
                .build();
        return List.of(McpSyncResourceTemplate.of(template).withCompletionHandler((ctx, request) -> {
            final Map<String, String> given = request.context() == null || request.context().arguments() == null
                    ? Map.of() : request.context().arguments();
            return McpSchema.CompleteResult.of(completion(
                    completeModule(request.argument().name(), request.argument().value(), given.get("type"))));
        }));
    }

    /**
     * Values of the module template's {@code type} / {@code name} variable that start with {@code value},
     * taken from the published documents; names are narrowed to {@code type} when the client has it.
     */
    List<String> completeModule(final String argument, final String value, final String type) {
        final String prefix = value == null ? "" : value;
        final TreeSet<String> values = new TreeSet<>();
        for (final String uri : uris) {
            if (!uri.startsWith(MODULE_URI_PREFIX)) {
                continue;
            }
            // <type>/<name>.md only: module/README.md and deeper pages are not instances of the template
            final String[] parts = uri.substring(MODULE_URI_PREFIX.length(), uri.length() - ".md".length()).split("/");
            if (parts.length != 2) {
                continue;
            }
            final String candidate;
            if ("type".equals(argument)) {
                candidate = parts[0];
            } else if (type == null || type.isBlank() || type.equals(parts[0])) {
                candidate = parts[1];
            } else {
                continue;
            }
            if (candidate.startsWith(prefix)) {
                values.add(candidate);
            }
        }
        return List.copyOf(values);
    }

    private static McpSchema.CompleteResult.Completion completion(final List<String> values) {
        final boolean hasMore = values.size() > MAX_COMPLETIONS;
        return new McpSchema.CompleteResult.Completion(
                hasMore ? values.subList(0, MAX_COMPLETIONS) : values, values.size(), hasMore);
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
