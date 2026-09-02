package com.mercari.solution.config;

import com.mercari.solution.module.Action;
import com.mercari.solution.module.Sink;
import com.mercari.solution.module.Source;
import com.mercari.solution.module.Transform;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;

/**
 * Every pipeline config under {@code examples/} must load and reference registered modules,
 * so a hand-edited example cannot ship with a syntax error or a module that does not exist.
 * (Module parameters are only validated at assembly, which needs credentials for most examples.)
 */
public class ExamplesConfigTest {

    private static final Path EXAMPLES = Paths.get("examples");

    /**
     * Examples that reference modules no longer in the codebase (automl, bandit, bar, crypto,
     * dummy, eventtime, matchingEngine, protobuf, setoperation, spannerBackup, text, tokenize,
     * union, window). Excluded here so the gate applies to everything else; they should be
     * rewritten against current modules or removed, together with their README entries.
     */
    private static final java.util.Set<String> LEGACY = java.util.Set.of(
            "bigquery-to-automl-to-spanner.json",
            "bigquery-to-bandit-to-bigquery.json",
            "bigquery-to-onnx-to-vectorsearch.json",
            "bigquery-to-parquet.json",
            "bigquery-to-text.json",
            "bigquery-to-tokenize-to-bigquery.json",
            "datastore-to-avro.json",
            "dummy-to-pubsub.json",
            "import-spanner-backup.json",
            "pubsub-protobuf-to-beamsql-to-pubsub-protobuf.json",
            "pubsub-to-bandit-to-pubsub-bigquery.json",
            "pubsub-to-bar-to-bigquery.json",
            "pubsub-to-beamsql-to-pubsub.json",
            "pubsub-to-matchingengine.json",
            "pubsub-to-union-to-bigquery.json",
            "setoperation-replace-spanner.json",
            "spanner-to-decrypt-to-avro.json",
            "spanner-to-protobuf-to-avro.json");

    static Stream<Path> examples() throws IOException {
        try(final Stream<Path> files = Files.list(EXAMPLES)) {
            return files
                    .filter(p -> {
                        final String name = p.getFileName().toString();
                        return (name.endsWith(".yaml") || name.endsWith(".json"))
                                && !name.contains("cloudbuild")   // Cloud Build specs, not pipeline configs
                                && !LEGACY.contains(name);
                    })
                    .sorted()
                    .toList()
                    .stream();
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("examples")
    public void testExampleLoadsAndReferencesRegisteredModules(final Path path) throws IOException {
        final Config config = Config.load(Files.readString(path));
        Assertions.assertNotNull(config, "config did not load: " + path);
        final int modules = config.getSources().size() + config.getTransforms().size()
                + config.getSinks().size() + config.getActions().size();
        Assertions.assertTrue(modules > 0, "no modules in: " + path);

        for(final SourceConfig c : config.getSources()) {
            assertRegistered(path, "source", c.getModule(), Source.moduleNames());
        }
        for(final TransformConfig c : config.getTransforms()) {
            assertRegistered(path, "transform", c.getModule(), Transform.moduleNames());
        }
        for(final SinkConfig c : config.getSinks()) {
            assertRegistered(path, "sink", c.getModule(), Sink.moduleNames());
        }
        for(final ActionConfig c : config.getActions()) {
            assertRegistered(path, "action", c.getModule(), Action.serviceNames());
        }
    }

    private static void assertRegistered(final Path path, final String kind, final String module, final java.util.Set<String> registered) {
        Assertions.assertTrue(module != null && registered.contains(module),
                path + ": unknown " + kind + " module `" + module + "` (registered: " + List.copyOf(registered).stream().sorted().toList() + ")");
    }
}
