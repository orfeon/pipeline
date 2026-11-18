package com.mercari.solution.server.mcp.prompt;

import com.google.common.reflect.ClassPath;
import com.mercari.solution.server.mcp.McpCaching;
import io.modelcontextprotocol.modern.McpException;
import io.modelcontextprotocol.modern.McpSchema;
import io.modelcontextprotocol.modern.server.McpRequestContext;
import io.modelcontextprotocol.modern.server.McpSyncResponse;
import io.modelcontextprotocol.modern.server.feature.McpSyncPrompt;
import io.modelcontextprotocol.modern.server.feature.McpSyncPromptRepository;
import io.modelcontextprotocol.modern.server.feature.PromptsPage;

import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public interface Prompt {

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    @interface Module {
        String name();
        String title() default "";
        String description();
    }

    McpSchema.GetPromptResult sync(
            final McpRequestContext ctx,
            final McpSchema.GetPromptRequest request);

    List<McpSchema.PromptArgument> arguments();

    /**
     * The MCP server's prompt catalog: every annotated class of this package, listed in name order as one
     * page and cacheable by anyone ({@link McpCaching}) — the prompts only change with a deployment.
     */
    @SuppressWarnings("unchecked")
    static McpSyncPromptRepository repository() {
        final ClassPath classPath;
        try {
            classPath = ClassPath.from(Prompt.class.getClassLoader());
        } catch (IOException ioe) {
            throw new RuntimeException("Reading classpath resource failed", ioe);
        }

        final Map<String, McpSyncPrompt> prompts = new TreeMap<>();
        for (final ClassPath.ClassInfo info : classPath.getTopLevelClassesRecursive(Prompt.class.getPackageName())) {
            final Class<?> clazz = info.load();
            if (!Prompt.class.isAssignableFrom(clazz) || !clazz.isAnnotationPresent(Module.class)) {
                continue;
            }
            final Prompt prompt;
            try {
                prompt = ((Class<Prompt>) clazz).getDeclaredConstructor().newInstance();
            } catch (final ReflectiveOperationException e) {
                throw new RuntimeException("Failed to instantiate mcp.prompt: " + clazz, e);
            }
            final Module properties = clazz.getAnnotation(Module.class);
            final McpSchema.Prompt definition = McpSchema.Prompt.builder(properties.name())
                    .title(properties.title().isBlank() ? null : properties.title())
                    .description(properties.description().strip())
                    .arguments(prompt.arguments())
                    .build();
            prompts.put(properties.name(), McpSyncPrompt.of(definition,
                    (ctx, request) -> McpSyncResponse.result(prompt.sync(ctx, request))));
        }
        final List<McpSchema.Prompt> definitions = prompts.values().stream().map(McpSyncPrompt::definition).toList();

        return new McpSyncPromptRepository() {
            @Override
            public PromptsPage list(final McpRequestContext ctx, final String cursor) {
                if (cursor != null) {
                    throw McpException.invalidParams("Unknown cursor: " + cursor);
                }
                return new PromptsPage(definitions, null, McpCaching.CATALOG_TTL_MS, McpSchema.CacheScope.PUBLIC);
            }

            @Override
            public McpSyncPrompt find(final McpRequestContext ctx, final String name) {
                return prompts.get(name);
            }
        };
    }

}
