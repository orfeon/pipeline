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
    }

    McpSchema.CallToolResult sync(
            final McpSyncServerExchange exchange,
            final McpSchema.CallToolRequest request);

    void init(ServletContext servletContext);

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
        final McpSchema.Tool.Builder builder = McpSchema.Tool.builder()
                .name(properties.name())
                .title(properties.title())
                .description(properties.description())
                .inputSchema(schemaMap(properties.name(), "inputSchema", properties.inputSchema()));
        final java.util.Map<String, Object> output = properties.outputSchema() == null || properties.outputSchema().isBlank()
                ? null : schemaMap(properties.name(), "outputSchema", properties.outputSchema());
        // only object output schemas are valid for structured content; scalar ones are documentation only
        if (output != null && "object".equals(output.get("type"))) {
            builder.outputSchema(output);
        }
        return builder.build();
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
