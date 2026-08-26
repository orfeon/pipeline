package com.mercari.solution.module;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Guards server/docs/module/index.yaml, the module catalog behind the Builder UI's /api/spec.
 * The server-side tests (com.mercari.solution.server.**) only compile under -Pserver, so this
 * lives with the module tests to run on every `mvn test`.
 * Regression: an unquoted description containing ": " broke SnakeYAML parsing, which made the
 * API servlet fail at init and every /api/* request return 404 (PR #74).
 */
public class ModuleIndexYamlTest {

    private static final String RESOURCE = "server/docs/module/index.yaml";

    @Test
    public void testIndexYamlParsesAndListsModules() throws Exception {
        final Map<String, Object> index;
        try (final InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(RESOURCE)) {
            Assertions.assertNotNull(is, "resource not found: " + RESOURCE);
            index = new Yaml().load(is);
        }
        for (final String section : List.of("sources", "transforms", "sinks", "actions")) {
            Assertions.assertInstanceOf(List.class, index.get(section), section);
            final List<?> entries = (List<?>) index.get(section);
            Assertions.assertFalse(entries.isEmpty(), section);
            for (final Object entryObj : entries) {
                Assertions.assertInstanceOf(Map.class, entryObj, section);
                final Map<?, ?> entry = (Map<?, ?>) entryObj;
                Assertions.assertInstanceOf(String.class, entry.get("title"), section + " title");
                Assertions.assertInstanceOf(String.class, entry.get("description"),
                        section + "/" + entry.get("title") + " description");
            }
        }
    }

    // actions[].title is the config's actions[].module value: it must name a registered service
    @Test
    public void testActionEntriesMatchRegisteredServices() throws Exception {
        final Map<String, Object> index;
        try (final InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(RESOURCE)) {
            index = new Yaml().load(is);
        }
        final Set<String> serviceNames = Action.serviceNames();
        for (final Object entryObj : (List<?>) index.get("actions")) {
            final String title = String.valueOf(((Map<?, ?>) entryObj).get("title"));
            Assertions.assertTrue(serviceNames.contains(title), "unknown action service in index.yaml: " + title);
        }
    }
}
