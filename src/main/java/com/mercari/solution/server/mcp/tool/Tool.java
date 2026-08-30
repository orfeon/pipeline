package com.mercari.solution.server.mcp.tool;

import com.google.common.reflect.ClassPath;
import io.modelcontextprotocol.server.McpServerFeatures;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
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
    }

    McpSchema.CallToolResult sync(
            final McpSyncServerExchange exchange,
            final McpSchema.CallToolRequest request);

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

    /** The MCP server's tool specifications: the shared registry instances, initialised with the servlet context. */
    static List<McpServerFeatures.SyncToolSpecification> syncTools(ServletContext servletContext) {
        final List<McpServerFeatures.SyncToolSpecification> specifications = new java.util.ArrayList<>();
        for (final java.util.Map.Entry<String, Tool> e : Registry.INSTANCES.entrySet()) {
            Registry.initialize(e.getKey(), e.getValue(), servletContext);
            final Module properties = e.getValue().getClass().getAnnotation(Module.class);
            specifications.add(McpServerFeatures.SyncToolSpecification.builder()
                    .tool(toolSchema(properties))
                    .callHandler(e.getValue()::sync)
                    .build());
        }
        return specifications;
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
        return McpSchema.Tool.builder()
                .name(properties.name())
                .title(properties.title())
                .description(properties.description())
                .inputSchema(schemaMap(properties.name(), "inputSchema", properties.inputSchema()))
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
