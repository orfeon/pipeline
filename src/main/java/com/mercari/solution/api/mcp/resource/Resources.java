package com.mercari.solution.api.mcp.resource;

import com.google.common.reflect.ClassPath;
import io.modelcontextprotocol.server.McpServerFeatures;
import jakarta.servlet.ServletContext;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.stream.Collectors;

public interface Resources {

    List<McpServerFeatures.SyncResourceSpecification> sync(ServletContext servletContext);

    static List<McpServerFeatures.SyncResourceSpecification> syncResources(ServletContext servletContext) {
        final ClassPath classPath;
        try {
            ClassLoader loader = Resources.class.getClassLoader();
            classPath = ClassPath.from(loader);
        } catch (IOException ioe) {
            throw new RuntimeException("Reading classpath resource failed", ioe);
        }

        try {
            return classPath.getTopLevelClassesRecursive(Resources.class.getPackageName())
                    .stream()
                    .map(ClassPath.ClassInfo::load)
                    .filter(Resources.class::isAssignableFrom)
                    .map(clazz -> (Class<Resources>) clazz.asSubclass(Resources.class))
                    .filter(clazz -> !clazz.getSimpleName().equals("Resources"))
                    .flatMap(clazz -> {
                        final Resources resources;
                        try {
                            resources = clazz.getDeclaredConstructor().newInstance();
                        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException |
                                 InvocationTargetException e) {
                            throw new RuntimeException("Failed to instantiate mcp.resources: " + clazz, e);
                        }
                        return resources.sync(servletContext).stream();
                    })
                    .collect(Collectors.toList());
        } catch (Throwable e) {
            return List.of();
        }
    }
}
