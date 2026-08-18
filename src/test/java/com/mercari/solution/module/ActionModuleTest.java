package com.mercari.solution.module;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Tests for the action modules (action.<service>): placement in sources/transforms/sinks,
 * trigger semantics (once/perElement/collect), the common output envelope, control-record
 * chaining, failure routing and validation. Uses the test-only 'mock' action service and the
 * real 'storage' action service (local files).
 */
public class ActionModuleTest {

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    private static final String SOURCE_YAML = """
            sources:
              - name: input
                module: create
                parameters:
                  type: element
                  elements:
                    - field_string: a
                      field_long: 1
                    - field_string: b
                      field_long: 2
                    - field_string: c
                      field_long: 3
                schema:
                  fields:
                    - name: field_string
                      type: string
                    - name: field_long
                      type: int64
            """;

    @Test
    public void testTriggerOnceInSinksWithInputs() throws Exception {
        final String configYaml = SOURCE_YAML + """
                sinks:
                  - name: action
                    module: action.mock
                    inputs:
                      - input
                    parameters:
                      message: fixed message
                """;
        final Config config = Config.load(configYaml);
        final MCollection output = MPipeline.apply(pipeline, config).get("action");

        // once trigger: inputs act only as completion signals, so exactly one envelope record is emitted
        PAssert.that(output.getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement element : elements) {
                count++;
                Assertions.assertEquals("mock", element.getPrimitiveValue("service"));
                Assertions.assertEquals("echo", element.getPrimitiveValue("op"));
                Assertions.assertEquals("mock-job", element.getPrimitiveValue("jobId"));
                Assertions.assertEquals("DONE", element.getPrimitiveValue("state"));
                Assertions.assertEquals("fixed message", element.getPrimitiveValue("payload"));
                Assertions.assertNotNull(element.getPrimitiveValue("startedAt"));
                Assertions.assertNotNull(element.getPrimitiveValue("finishedAt"));
            }
            Assertions.assertEquals(1, count);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testTriggerOnceStandalone() throws Exception {
        final String configYaml = """
                sinks:
                  - name: action
                    module: action.mock
                    parameters:
                      message: standalone
                """;
        final Config config = Config.load(configYaml);
        final MCollection output = MPipeline.apply(pipeline, config).get("action");

        PAssert.that(output.getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement element : elements) {
                count++;
                Assertions.assertEquals("standalone", element.getPrimitiveValue("payload"));
            }
            Assertions.assertEquals(1, count);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testActionInSources() throws Exception {
        // pipeline-start action (no upstream) gating another step via waits
        final String configYaml = """
                sources:
                  - name: prepare
                    module: action.mock
                    parameters:
                      message: prepared
                sinks:
                  - name: after
                    module: action.mock
                    waits:
                      - prepare
                    parameters:
                      message: after prepare
                """;
        final Config config = Config.load(configYaml);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        PAssert.that(outputs.get("prepare").getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement element : elements) {
                count++;
                Assertions.assertEquals("prepared", element.getPrimitiveValue("payload"));
            }
            Assertions.assertEquals(1, count);
            return null;
        });
        PAssert.that(outputs.get("after").getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement element : elements) {
                count++;
                Assertions.assertEquals("after prepare", element.getPrimitiveValue("payload"));
            }
            Assertions.assertEquals(1, count);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testActionInTransformsWaitsOnlyAndChained() throws Exception {
        // mid-flow action gated by waits alone; its envelope (control records) is consumed
        // downstream by another action via inputs — the sanctioned control-plane chaining
        final String configYaml = SOURCE_YAML + """
                transforms:
                  - name: mid
                    module: action.mock
                    waits:
                      - input
                    parameters:
                      message: mid done
                sinks:
                  - name: notify
                    module: action.mock
                    inputs:
                      - mid
                    parameters:
                      trigger: perElement
                      message: got ${payload} from ${service}
                """;
        final Config config = Config.load(configYaml);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        PAssert.that(outputs.get("notify").getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement element : elements) {
                count++;
                Assertions.assertEquals("got mid done from mock", element.getPrimitiveValue("payload"));
            }
            Assertions.assertEquals(1, count);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testTriggerPerElementWithTemplate() throws Exception {
        final String configYaml = SOURCE_YAML + """
                sinks:
                  - name: action
                    module: action.mock
                    inputs:
                      - input
                    parameters:
                      trigger: perElement
                      message: value is ${field_string}-${field_long}
                """;
        final Config config = Config.load(configYaml);
        final MCollection output = MPipeline.apply(pipeline, config).get("action");

        PAssert.that(output.getCollection()).satisfies(elements -> {
            final Set<String> payloads = new HashSet<>();
            for(final MElement element : elements) {
                Assertions.assertEquals("mock", element.getPrimitiveValue("service"));
                payloads.add((String) element.getPrimitiveValue("payload"));
            }
            Assertions.assertEquals(
                    Set.of("value is a-1", "value is b-2", "value is c-3"), payloads);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testTriggerCollectWithTemplate() throws Exception {
        // collect: all input elements gathered into one execution, exposed to templates
        // as `elements` (list of field maps) and `size`
        final String configYaml = SOURCE_YAML + """
                sinks:
                  - name: action
                    module: action.mock
                    inputs:
                      - input
                    parameters:
                      trigger: collect
                      message: "${size} records:<#list elements?sort_by('field_string') as e> ${e.field_string}=${e.field_long}</#list>"
                """;
        final Config config = Config.load(configYaml);
        final MCollection output = MPipeline.apply(pipeline, config).get("action");

        PAssert.that(output.getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement element : elements) {
                count++;
                Assertions.assertEquals("3 records: a=1 b=2 c=3", element.getPrimitiveValue("payload"));
            }
            Assertions.assertEquals(1, count);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testStorageActionCollect() throws Exception {
        // real storage action service: write the collected elements as a JSONL history file
        final String dir = "target/action-storage-test";
        final Path file = Path.of(dir, "history.jsonl");
        Files.createDirectories(Path.of(dir));
        Files.deleteIfExists(file);

        final String configYaml = SOURCE_YAML + """
                sinks:
                  - name: history
                    module: action.storage
                    inputs:
                      - input
                    parameters:
                      trigger: collect
                      output: %s
                """.formatted(dir + "/history.jsonl");
        final Config config = Config.load(configYaml);
        MPipeline.apply(pipeline, config);
        pipeline.run().waitUntilFinish();

        Assertions.assertTrue(Files.exists(file));
        final String content = Files.readString(file, StandardCharsets.UTF_8);
        Assertions.assertEquals(3, content.strip().lines().count());
        Assertions.assertTrue(content.contains("\"field_string\":\"a\""));
        Assertions.assertTrue(content.contains("\"field_string\":\"b\""));
        Assertions.assertTrue(content.contains("\"field_string\":\"c\""));
    }

    @Test
    public void testStorageSinkResultsConsumedByAction() throws Exception {
        // the two-plane poster case: storage sink emits its written-files records (control
        // records), and an action consumes them via inputs — here a collect mock building
        // a summary message over the written paths
        final String dir = "target/action-module-test/storage-results";
        final String configYaml = SOURCE_YAML + """
                sinks:
                  - name: store
                    module: storage
                    inputs:
                      - input
                    parameters:
                      output: %s/data
                      format: json
                      suffix: .json
                      numShards: 1
                  - name: notify
                    module: action.mock
                    inputs:
                      - store
                    parameters:
                      trigger: collect
                      message: "wrote ${size} files:<#list elements as e> ${e.path}</#list>"
                """.formatted(dir);
        final Config config = Config.load(configYaml);
        final MCollection output = MPipeline.apply(pipeline, config).get("notify");

        PAssert.that(output.getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement element : elements) {
                count++;
                final String payload = (String) element.getPrimitiveValue("payload");
                Assertions.assertTrue(payload.startsWith("wrote 1 files:"), payload);
                Assertions.assertTrue(payload.contains("data"), payload);
            }
            Assertions.assertEquals(1, count);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testFailureIsRoutedNotOutput() throws Exception {
        // failFast: false routes the BadRecord to failure handling; the output must stay empty
        final String configYaml = SOURCE_YAML + """
                sinks:
                  - name: action
                    module: action.mock
                    failFast: false
                    inputs:
                      - input
                    parameters:
                      message: boom
                      fail: true
                """;
        final Config config = Config.load(configYaml);
        final MCollection output = MPipeline.apply(pipeline, config).get("action");

        PAssert.that(output.getCollection()).empty();

        pipeline.run();
    }

    @Test
    public void testValidationErrors() {
        // unknown service
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, Config.load(SOURCE_YAML + """
                sinks:
                  - name: action
                    module: action.nosuchservice
                    inputs:
                      - input
                    parameters:
                      message: msg
                """)));

        // bare 'action' module without service suffix
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, Config.load(SOURCE_YAML + """
                sinks:
                  - name: action
                    module: action
                    inputs:
                      - input
                    parameters:
                      message: msg
                """)));

        // perElement without inputs
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, Config.load("""
                sinks:
                  - name: action
                    module: action.mock
                    parameters:
                      trigger: perElement
                      message: msg
                """)));

        // collect without inputs
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, Config.load("""
                sinks:
                  - name: action
                    module: action.mock
                    parameters:
                      trigger: collect
                      message: msg
                """)));

        // bigquery service parameter validation is reached through the module (query op without query)
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, Config.load(SOURCE_YAML + """
                sinks:
                  - name: action
                    module: action.bigquery
                    inputs:
                      - input
                    parameters:
                      op: query
                """)));

        // sourceUrisField requires trigger: collect
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, Config.load(SOURCE_YAML + """
                sinks:
                  - name: action
                    module: action.bigquery
                    inputs:
                      - input
                    parameters:
                      op: load
                      sourceUrisField: path
                      destinationTable: p.d.t
                """)));
    }

}
