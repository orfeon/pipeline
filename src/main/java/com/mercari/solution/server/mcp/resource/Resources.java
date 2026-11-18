package com.mercari.solution.server.mcp.resource;

import com.google.common.reflect.ClassPath;
import com.mercari.solution.server.mcp.McpCaching;
import io.modelcontextprotocol.modern.McpException;
import io.modelcontextprotocol.modern.McpSchema;
import io.modelcontextprotocol.modern.server.McpRequestContext;
import io.modelcontextprotocol.modern.server.feature.McpSyncResource;
import io.modelcontextprotocol.modern.server.feature.McpSyncResourceRepository;
import io.modelcontextprotocol.modern.server.feature.McpSyncResourceTemplate;
import io.modelcontextprotocol.modern.server.feature.ResourceTemplatesPage;
import io.modelcontextprotocol.modern.server.feature.ResourcesPage;
import jakarta.servlet.ServletContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * A provider of MCP resources. Implementations in this package are discovered by scanning it; together
 * they form the server's resource catalog ({@link #repository}).
 */
public interface Resources {

    Logger LOG = LoggerFactory.getLogger(Resources.class);

    /** {@code resources/list} page size. */
    int PAGE_SIZE = 100;

    /** A published resource and how it is read. */
    record Entry(McpSchema.Resource resource, McpSyncResource handler) {}

    List<Entry> sync(ServletContext servletContext);

    /** URI templates (with their completion handlers) for the resources this provider publishes. */
    default List<McpSyncResourceTemplate> templates() {
        return List.of();
    }

    /**
     * The MCP server's resource catalog. The resources are bundled in the image, so the catalog is fixed
     * for the life of the server: lists are in URI order (the deterministic order the protocol asks for),
     * paged by an offset cursor and cacheable by anyone ({@link McpCaching}).
     */
    static McpSyncResourceRepository repository(final ServletContext servletContext) {
        final Map<String, Entry> entries = new TreeMap<>();
        final Map<String, McpSyncResourceTemplate> templates = new LinkedHashMap<>();
        for (final Resources resources : providers()) {
            try {
                for (final Entry entry : resources.sync(servletContext)) {
                    entries.put(entry.resource().uri(), entry);
                }
                for (final McpSyncResourceTemplate template : resources.templates()) {
                    templates.put(template.definition().uriTemplate(), template);
                }
            } catch (final RuntimeException e) {
                LOG.error("Failed to load mcp.resources: " + resources.getClass().getName(), e);
            }
        }
        final List<McpSchema.Resource> definitions = entries.values().stream().map(Entry::resource).toList();
        final List<McpSchema.ResourceTemplate> templateDefinitions = templates.values().stream()
                .map(McpSyncResourceTemplate::definition)
                .toList();

        return new McpSyncResourceRepository() {
            @Override
            public ResourcesPage list(final McpRequestContext ctx, final String cursor) {
                final int from = offset(cursor, definitions.size());
                final int to = Math.min(from + PAGE_SIZE, definitions.size());
                return new ResourcesPage(
                        definitions.subList(from, to),
                        to < definitions.size() ? String.valueOf(to) : null,
                        McpCaching.CATALOG_TTL_MS,
                        McpSchema.CacheScope.PUBLIC);
            }

            @Override
            public ResourceTemplatesPage listTemplates(final McpRequestContext ctx, final String cursor) {
                if (cursor != null) {
                    throw McpException.invalidParams("Unknown cursor: " + cursor);
                }
                return new ResourceTemplatesPage(templateDefinitions, null, McpCaching.CATALOG_TTL_MS, McpSchema.CacheScope.PUBLIC);
            }

            @Override
            public McpSyncResource find(final McpRequestContext ctx, final String uri) {
                final Entry entry = entries.get(uri);
                return entry == null ? null : entry.handler();
            }

            @Override
            public McpSyncResourceTemplate findTemplate(final McpRequestContext ctx, final String uriTemplate) {
                return templates.get(uriTemplate);
            }

            @Override
            public boolean supportsCompletion() {
                return templates.values().stream().anyMatch(t -> t.completionHandler() != null);
            }
        };
    }

    /** The cursor is the offset of the next page; anything else is not a cursor this server issued. */
    private static int offset(final String cursor, final int size) {
        if (cursor == null) {
            return 0;
        }
        try {
            final int offset = Integer.parseInt(cursor);
            if (offset > 0 && offset < size) {
                return offset;
            }
        } catch (final NumberFormatException e) {
            // falls through
        }
        throw McpException.invalidParams("Unknown cursor: " + cursor);
    }

    @SuppressWarnings("unchecked")
    private static List<Resources> providers() {
        final ClassPath classPath;
        try {
            classPath = ClassPath.from(Resources.class.getClassLoader());
        } catch (IOException ioe) {
            throw new RuntimeException("Reading classpath resource failed", ioe);
        }
        final List<Resources> providers = new ArrayList<>();
        for (final ClassPath.ClassInfo info : classPath.getTopLevelClasses(Resources.class.getPackageName())) {
            final Class<?> clazz = info.load();
            if (!Resources.class.isAssignableFrom(clazz) || clazz.isInterface()) {
                continue;
            }
            try {
                providers.add(((Class<Resources>) clazz).getDeclaredConstructor().newInstance());
            } catch (final ReflectiveOperationException e) {
                throw new RuntimeException("Failed to instantiate mcp.resources: " + clazz, e);
            }
        }
        return providers;
    }

}
