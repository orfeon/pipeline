package com.mercari.solution;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * {@code --dryRun=true}: the config is loaded and the pipeline assembled (module validation, schema
 * resolution, feature plan compilation) but never run. Uses a storage sink under target/ so a run
 * would leave files behind — their absence proves the pipeline did not execute.
 */
@Execution(ExecutionMode.SAME_THREAD) // both tests swap System.out
public class MPipelineDryRunTest {

    private static final String OUTPUT_DIR = "target/mpipeline-dryrun-test/";

    private static final String CONFIG = """
            sources:
              - name: events
                module: create
                parameters:
                  type: element
                  schema:
                    fields:
                      - {name: session_id, type: string}
                      - {name: seller_id, type: string}
                      - {name: quantity, type: int32}
                      - {name: start_price, type: float64}
                      - {name: session_time, type: timestamp}
                  elements:
                    - {session_id: "A", seller_id: "s1", quantity: 2, start_price: 100.0, session_time: "2024-01-01T00:00:00Z"}
                    - {session_id: "B", seller_id: "s1", quantity: 1, start_price: 200.0, session_time: "2024-01-02T00:00:00Z"}
            transforms:
              - name: features
                module: feature
                inputs: [events]
                parameters:
                  sources:
                    - name: listings
                      eventTime: session_time
                      availability: atEventTime
                      fields:
                        - {name: session_id, type: string}
                        - {name: seller_id, type: string}
                        - {name: quantity, type: int}
                        - {name: start_price, type: double}
                  lineage:
                    - {fields: [session_id, seller_id, quantity, start_price], from: listings}
                  time: {field: session_time}
                  predictAt: "event_time - PT1H"
                  entities:
                    - {name: seller, keys: [seller_id]}
                  features:
                    - name: unit
                      scope: row
                      expr: "start_price / quantity"
                    - name: hist
                      scope: sequence
                      entity: seller
                      ops:
                        - {type: aggregate, fields: [start_price], funcs: [mean]}
            sinks:
              - name: out
                module: storage
                inputs: [features]
                parameters:
                  output: %sfeatures
                  format: json
            """.formatted(OUTPUT_DIR);

    @Test
    public void testDryRunAssemblesWithoutRunning() throws Exception {
        final Path dir = Path.of(OUTPUT_DIR);
        Files.createDirectories(dir);
        try (var files = Files.list(dir)) {
            for (final Path p : files.toList()) Files.deleteIfExists(p);
        }
        final PrintStream original = System.out;
        final ByteArrayOutputStream captured = new ByteArrayOutputStream();
        try {
            System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
            MPipeline.main(new String[]{"--dryRun=true", "--config=" + CONFIG});
        } finally {
            System.setOut(original);
        }
        final String out = captured.toString(StandardCharsets.UTF_8);
        Assertions.assertTrue(out.contains("dry run: pipeline assembled successfully"), out);
        Assertions.assertTrue(out.contains("output features:"), out);
        // the feature plan report — including the hot-key audit SQL — is printed for the operator
        Assertions.assertTrue(out.contains("feature plan for features:"), out);
        Assertions.assertTrue(out.contains("-- audit"), out);
        Assertions.assertTrue(out.contains("GROUP BY seller_id"), out);
        try (var files = Files.list(dir)) {
            Assertions.assertEquals(0, files.count(), "a dry run must not write sink output");
        }
    }

    @Test
    public void testDryRunFailsOnInvalidConfig() {
        // a feature referencing an undeclared field fails at assembly time, before any run
        final String broken = CONFIG.replace("expr: \"start_price / quantity\"", "expr: \"start_price / missing_field\"");
        final PrintStream original = System.out;
        try {
            System.setOut(new PrintStream(new ByteArrayOutputStream()));
            Assertions.assertThrows(RuntimeException.class, () -> MPipeline.main(new String[]{"--dryRun=true", "--config=" + broken}));
        } finally {
            System.setOut(original);
        }
    }

}
