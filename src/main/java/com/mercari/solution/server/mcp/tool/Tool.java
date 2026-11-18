package com.mercari.solution.server.mcp.tool;

import com.google.common.reflect.ClassPath;
import com.mercari.solution.server.mcp.McpCaching;
import io.modelcontextprotocol.modern.McpException;
import io.modelcontextprotocol.modern.McpSchema;
import io.modelcontextprotocol.modern.server.McpRequestContext;
import io.modelcontextprotocol.modern.server.McpSyncResponse;
import io.modelcontextprotocol.modern.server.feature.McpSyncTool;
import io.modelcontextprotocol.modern.server.feature.McpSyncToolRepository;
import io.modelcontextprotocol.modern.server.feature.ToolsPage;
import jakarta.servlet.ServletContext;

import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.List;
import java.util.stream.Collectors;

public interface Tool {

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    @interface Module {
        String name();
        String title();
        String description();
        String inputSchema();
        String outputSchema();
        /** MCP tool annotations (hints for clients: confirmation prompts, caching). Defaults describe a read-only lookup. */
        boolean readOnly() default true;
        /** Only meaningful when not read-only: may the tool delete / overwrite existing state? */
        boolean destructive() default false;
        boolean idempotent() default true;
        /** Interacts with systems outside the server (cloud APIs, launched jobs). */
        boolean openWorld() default false;
        /**
         * May run for minutes (a local pipeline run, a launch that waits for completion). Such a tool answers
         * over a response stream: the transport keeps the connection alive while it works and a client that
         * sent a progress token is told when the work starts.
         */
        boolean longRunning() default false;
    }

    /**
     * The tool's work. {@code ctx} is the MCP request context (protocol version, client capabilities,
     * transport context); it is null when the in-server agent calls the tool through {@code McpToolBridge}.
     */
    McpSchema.CallToolResult sync(
            final McpRequestContext ctx,
            final McpSchema.CallToolRequest request);

    /**
     * How the MCP server answers {@code tools/call}: a plain result, or for a {@link Module#longRunning()}
     * tool a streamed one. The protocol is stateless (2026-07-28): every call is self-contained, so a tool
     * that needs more input from the user overrides this and returns an
     * {@link McpSchema.InputRequiredResult} instead of calling back into the client.
     */
    default McpSyncResponse<McpSchema.CallToolOutcome> call(
            final McpRequestContext ctx,
            final McpSchema.CallToolRequest request) {

        final Module properties = getClass().getAnnotation(Module.class);
        if (properties == null || !properties.longRunning()) {
            return McpSyncResponse.result(sync(ctx, request));
        }
        return McpSyncResponse.streaming(notifier -> {
            notifier.progress(0, null, properties.name() + " started");
            return sync(ctx, request);
        });
    }

    /** A text result (every tool returns its JSON / Markdown as text content). */
    static McpSchema.CallToolResult result(final String text, final boolean isError) {
        return McpSchema.CallToolResult.builder()
                .addContent(McpSchema.TextContent.builder(text == null ? "" : text).build())
                .isError(isError)
                .build();
    }

    static McpSchema.CallToolRequest request(final String name, final java.util.Map<String, Object> arguments) {
        return McpSchema.CallToolRequest.builder(name)
                .arguments(arguments == null ? java.util.Map.of() : arguments)
                .build();
    }

    void init(ServletContext servletContext);

    /**
     * The tool implementation published under {@code name}, initialised (without a servlet context when the
     * MCP servlet has not initialised it yet — every tool tolerates that). Used by the agent's wrappers.
     */
    static Tool find(final String name) {
        final Tool tool = Registry.INSTANCES.get(name);
        if (tool == null) {
            throw new IllegalArgumentException("unknown mcp tool: " + name + " (available: " + Registry.INSTANCES.keySet() + ")");
        }
        Registry.initialize(name, tool, null);
        return tool;
    }

    /**
     * One instance per tool class, shared by the MCP server and the agent bridge. Instantiation never
     * touches the environment; {@link #initialize} runs {@code init} once per tool, the first caller's
     * servlet context winning, and an {@code init} failure is confined to that tool.
     */
    final class Registry {
        private static final java.util.Map<String, Tool> INSTANCES = instantiate();
        private static final java.util.Set<String> INITIALIZED = java.util.concurrent.ConcurrentHashMap.newKeySet();
        private static final java.util.Map<String, RuntimeException> FAILED = new java.util.concurrent.ConcurrentHashMap<>();

        private Registry() {}

        private static java.util.Map<String, Tool> instantiate() {
            final java.util.Map<String, Tool> tools = new java.util.TreeMap<>();
            for (final Class<Tool> clazz : toolClasses()) {
                try {
                    tools.put(clazz.getAnnotation(Module.class).name(), clazz.getDeclaredConstructor().newInstance());
                } catch (final ReflectiveOperationException e) {
                    throw new RuntimeException("Failed to instantiate mcp.tool: " + clazz, e);
                }
            }
            return java.util.Collections.unmodifiableMap(tools);
        }

        static synchronized void initialize(final String name, final Tool tool, final ServletContext servletContext) {
            if (INITIALIZED.contains(name)) return;
            if (FAILED.containsKey(name)) throw FAILED.get(name);
            try {
                tool.init(servletContext);
                INITIALIZED.add(name);
            } catch (final RuntimeException e) {
                FAILED.put(name, new IllegalStateException("mcp tool " + name + " failed to initialise: " + e.getMessage(), e));
                throw FAILED.get(name);
            }
        }
    }

    @SuppressWarnings("unchecked")
    static List<Class<Tool>> toolClasses() {
        final ClassPath classPath;
        try {
            classPath = ClassPath.from(Tool.class.getClassLoader());
        } catch (IOException ioe) {
            throw new RuntimeException("Reading classpath resource failed", ioe);
        }
        return classPath.getTopLevelClassesRecursive(Tool.class.getPackageName())
                .stream()
                .map(ClassPath.ClassInfo::load)
                .filter(Tool.class::isAssignableFrom)
                .map(clazz -> (Class<Tool>) clazz.asSubclass(Tool.class))
                .filter(clazz -> clazz.isAnnotationPresent(Module.class))
                .collect(Collectors.toList());
    }

    /**
     * The MCP server's tool catalog: the shared registry instances, initialised with the servlet context.
     * The catalog is fixed for the life of the server, so the list is one page in name order and
     * cacheable by anyone ({@link McpCaching}).
     */
    static McpSyncToolRepository repository(final ServletContext servletContext) {
        final java.util.Map<String, McpSyncTool> tools = new java.util.TreeMap<>();
        for (final java.util.Map.Entry<String, Tool> e : Registry.INSTANCES.entrySet()) {
            Registry.initialize(e.getKey(), e.getValue(), servletContext);
            final Tool tool = e.getValue();
            final Module properties = tool.getClass().getAnnotation(Module.class);
            tools.put(e.getKey(), McpSyncTool.of(toolSchema(properties),
                    (ctx, request) -> tool.call(ctx, withArguments(request))));
        }
        final List<McpSchema.Tool> definitions = tools.values().stream().map(McpSyncTool::definition).toList();
        return new McpSyncToolRepository() {
            @Override
            public ToolsPage list(final McpRequestContext ctx, final String cursor) {
                if (cursor != null) {
                    throw McpException.invalidParams("Unknown cursor: " + cursor);
                }
                return new ToolsPage(definitions, null, McpCaching.CATALOG_TTL_MS, McpSchema.CacheScope.PUBLIC);
            }

            @Override
            public McpSyncTool find(final McpRequestContext ctx, final String name) {
                return tools.get(name);
            }
        };
    }

    /**
     * {@code arguments} is optional on the wire while the tools read it unconditionally: an absent one
     * becomes empty, the rest of the request (input responses, request state, {@code _meta}) is kept.
     */
    private static McpSchema.CallToolRequest withArguments(final McpSchema.CallToolRequest request) {
        if (request.arguments() != null) {
            return request;
        }
        return new McpSchema.CallToolRequest(
                request.name(), java.util.Map.of(), request.inputResponses(), request.requestState(), request.meta());
    }

    /**
     * The tool definition published to clients. The annotation's schemas are JSON text; they are parsed
     * into maps here (the SDK's {@code inputSchema(McpJsonMapper, String)} needs a JSON provider on the
     * classpath, and an unset input schema is published as "no parameters", which hides every argument
     * from the client).
     */
    static McpSchema.Tool toolSchema(final Module properties) {
        // outputSchema is deliberately NOT published: with an output schema the SDK requires every result to
        // carry structuredContent (validated against it) and turns a text-only result into an error. The
        // tools return their JSON as text content; the annotation's outputSchema documents that shape.
        // The input schema IS enforced: the SDK validates a call's arguments against it before the handler runs.
        return McpSchema.Tool.builder(properties.name(), schemaMap(properties.name(), "inputSchema", properties.inputSchema()))
                .title(properties.title())
                .description(properties.description())
                .annotations(McpSchema.ToolAnnotations.builder()
                        .title(properties.title())
                        .readOnlyHint(properties.readOnly())
                        .destructiveHint(!properties.readOnly() && properties.destructive())
                        .idempotentHint(properties.idempotent())
                        .openWorldHint(properties.openWorld())
                        .build())
                .build();
    }

    @SuppressWarnings("unchecked")
    private static java.util.Map<String, Object> schemaMap(final String tool, final String kind, final String json) {
        try {
            final java.util.Map<String, Object> map = new com.google.gson.Gson().fromJson(json, java.util.Map.class);
            if (map == null || !map.containsKey("type")) {
                throw new IllegalArgumentException("schema must be a JSON object with a 'type'");
            }
            return map;
        } catch (final RuntimeException e) {
            throw new IllegalStateException("mcp.tool " + tool + " has an invalid " + kind + ": " + e.getMessage(), e);
        }
    }

}
