package com.mercari.solution.server.mcp.prompt;

import com.google.common.reflect.ClassPath;
import io.modelcontextprotocol.server.McpServerFeatures;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;

import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.stream.Collectors;

public interface Prompt {

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    @interface Module {
        String name();
        String description();
    }

    McpSchema.GetPromptResult sync(
            final McpSyncServerExchange exchange,
            final McpSchema.GetPromptRequest request);

    List<McpSchema.PromptArgument> arguments();


    static List<McpServerFeatures.SyncPromptSpecification> syncPrompts() {
        final ClassPath classPath;
        try {
            ClassLoader loader = Prompt.class.getClassLoader();
            classPath = ClassPath.from(loader);
        } catch (IOException ioe) {
            throw new RuntimeException("Reading classpath resource failed", ioe);
        }

        return classPath.getTopLevelClassesRecursive(Prompt.class.getPackageName())
                .stream()
                .map(ClassPath.ClassInfo::load)
                .filter(Prompt.class::isAssignableFrom)
                .map(clazz -> (Class<Prompt>)clazz.asSubclass(Prompt.class))
                .filter(clazz -> clazz.isAnnotationPresent(Module.class))
                .map(clazz -> {
                    final Prompt prompt;
                    try {
                        prompt = clazz.getDeclaredConstructor().newInstance();
                    } catch (InstantiationException | IllegalAccessException | NoSuchMethodException |
                             InvocationTargetException e) {
                        throw new RuntimeException("Failed to instantiate mcp.prompt: " + clazz, e);
                    }
                    final Module properties = prompt.getClass().getAnnotation(Module.class);
                    return new McpServerFeatures.SyncPromptSpecification(
                            new McpSchema.Prompt(
                                    properties.name(),
                                    properties.description(),
                                    prompt.arguments()),
                            prompt::sync);
                })
                .collect(Collectors.toList());
    }

}
