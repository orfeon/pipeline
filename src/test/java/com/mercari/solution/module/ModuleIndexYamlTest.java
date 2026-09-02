package com.mercari.solution.module;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
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

    // Registered modules that are deliberately not in the catalog: the `example` transform is
    // the add-module skill's template (module/transform/ExampleTransform.java), and the `mock`
    // action service is a test fixture (src/test/.../module/action/MockAction.java) picked up by
    // the package scan on the test classpath.
    private static final Set<String> UNLISTED = Set.of("transforms/example", "actions/mock");

    private static final Map<String, Set<String>> REGISTRIES = Map.of(
            "sources", Source.moduleNames(),
            "transforms", Transform.moduleNames(),
            "sinks", Sink.moduleNames(),
            "actions", Action.serviceNames());

    private static Map<String, Object> loadIndex() throws IOException {
        try (final InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(RESOURCE)) {
            Assertions.assertNotNull(is, "resource not found: " + RESOURCE);
            final Object loaded = new Yaml().load(is);
            Assertions.assertInstanceOf(Map.class, loaded, "index.yaml root");
            @SuppressWarnings("unchecked")
            final Map<String, Object> index = (Map<String, Object>) loaded;
            return index;
        }
    }

    /** Entries of one section, each verified to be a mapping. */
    private static List<Map<?, ?>> entries(final Map<String, Object> index, final String section) {
        Assertions.assertInstanceOf(List.class, index.get(section), section);
        final List<Map<?, ?>> result = new ArrayList<>();
        for (final Object entryObj : (List<?>) index.get(section)) {
            Assertions.assertInstanceOf(Map.class, entryObj, section + " entry");
            result.add((Map<?, ?>) entryObj);
        }
        Assertions.assertFalse(result.isEmpty(), section);
        return result;
    }

    @Test
    public void testIndexYamlParsesAndEntriesAreWellFormed() throws Exception {
        final Map<String, Object> index = loadIndex();
        for (final String section : REGISTRIES.keySet()) {
            final Set<String> titles = new HashSet<>();
            for (final Map<?, ?> entry : entries(index, section)) {
                Assertions.assertInstanceOf(String.class, entry.get("title"), section + " title");
                final String title = (String) entry.get("title");
                Assertions.assertTrue(titles.add(title), section + " has a duplicate title: " + title);
                Assertions.assertInstanceOf(String.class, entry.get("description"),
                        section + "/" + title + " description");
                Assertions.assertFalse(((String) entry.get("description")).isBlank(),
                        section + "/" + title + " description is blank");
                // SpecService.toModuleArray silently coerces a non-list tags value to []
                Assertions.assertInstanceOf(List.class, entry.get("tags"), section + "/" + title + " tags");
                Assertions.assertFalse(((List<?>) entry.get("tags")).isEmpty(),
                        section + "/" + title + " tags is empty");
            }
        }
    }

    // title is the config's `module` value (for actions the service name): every entry must name a
    // registered module, and every registered module must be listed — a module missing from
    // index.yaml does not appear in the Builder UI (CLAUDE.md).
    @Test
    public void testTitlesMatchRegisteredModules() throws Exception {
        final Map<String, Object> index = loadIndex();
        for (final Map.Entry<String, Set<String>> registry : REGISTRIES.entrySet()) {
            final String section = registry.getKey();
            final Set<String> titles = new HashSet<>();
            for (final Map<?, ?> entry : entries(index, section)) {
                titles.add(String.valueOf(entry.get("title")));
            }
            final Set<String> unknown = new HashSet<>(titles);
            unknown.removeAll(registry.getValue());
            Assertions.assertTrue(unknown.isEmpty(), section + " lists unregistered modules: " + unknown);
            final Set<String> missing = new HashSet<>(registry.getValue());
            missing.removeAll(titles);
            missing.removeIf(name -> UNLISTED.contains(section + "/" + name));
            Assertions.assertTrue(missing.isEmpty(), section + " is missing registered modules: " + missing);
        }
    }
}
