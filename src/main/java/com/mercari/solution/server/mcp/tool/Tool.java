package com.mercari.solution.server.mcp.tool;

import com.google.common.reflect.ClassPath;
import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.server.McpServerFeatures;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import jakarta.servlet.ServletContext;

import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.InvocationTargetException;
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
     * Registry of the tool implementations by their published name, for callers other than the MCP
     * server (the Pipeline Builder agent's tools are thin wrappers over these). Instances are created and
     * initialised once, without a servlet context (every tool tolerates that).
     */
    static java.util.Map<String, Tool> registry() {
        return Registry.TOOLS;
    }

    static Tool find(final String name) {
        final Tool tool = Registry.TOOLS.get(name);
        if (tool == null) {
            throw new IllegalArgumentException("unknown mcp tool: " + name + " (available: " + Registry.TOOLS.keySet() + ")");
        }
        return tool;
    }

    final class Registry {
        private static final java.util.Map<String, Tool> TOOLS = load();

        private Registry() {}

        private static java.util.Map<String, Tool> load() {
            final java.util.Map<String, Tool> tools = new java.util.TreeMap<>();
            for (final Class<Tool> clazz : toolClasses()) {
                try {
                    final Tool tool = clazz.getDeclaredConstructor().newInstance();
                    tool.init(null);
                    tools.put(clazz.getAnnotation(Module.class).name(), tool);
                } catch (final ReflectiveOperationException e) {
                    throw new RuntimeException("Failed to instantiate mcp.tool: " + clazz, e);
                }
            }
            return java.util.Collections.unmodifiableMap(tools);
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

    static List<McpServerFeatures.SyncToolSpecification> syncTools(ServletContext servletContext) {
        final ClassPath classPath;
        try {
            ClassLoader loader = Tool.class.getClassLoader();
            classPath = ClassPath.from(loader);
        } catch (IOException ioe) {
            throw new RuntimeException("Reading classpath resource failed", ioe);
        }

        return classPath.getTopLevelClassesRecursive(Tool.class.getPackageName())
                .stream()
                .map(ClassPath.ClassInfo::load)
                .filter(Tool.class::isAssignableFrom)
                .map(clazz -> (Class<Tool>)clazz.asSubclass(Tool.class))
                .filter(clazz -> clazz.isAnnotationPresent(Module.class))
                .map(clazz -> {
                    final Tool tool;
                    try {
                        tool = clazz.getDeclaredConstructor().newInstance();
                        tool.init(servletContext);
                    } catch (InstantiationException | IllegalAccessException | NoSuchMethodException |
                             InvocationTargetException e) {
                        throw new RuntimeException("Failed to instantiate mcp.tool: " + clazz, e);
                    }
                    final Module properties = tool.getClass().getAnnotation(Module.class);
                    return McpServerFeatures.SyncToolSpecification.builder()
                            .tool(toolSchema(properties))
                            .callHandler(tool::sync)
                            .build();
                })
                .collect(Collectors.toList());
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
