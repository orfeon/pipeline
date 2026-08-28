package com.mercari.solution.module;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.action.MockAction;
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
 * Tests for the action module (config section {@code actions}): gating by inputs/waits/nothing,
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
    public void testTriggerOnceWithInputs() throws Exception {
        final String configYaml = SOURCE_YAML + """
                actions:
                  - name: action
                    module: mock
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
                Assertions.assertEquals("echo", element.getPrimitiveValue("operation"));
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
                actions:
                  - name: action
                    module: mock
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
    public void testPipelineStartActionGatingAnother() throws Exception {
        // pipeline-start action (no upstream) gating another step via waits
        final String configYaml = """
                actions:
                  - name: prepare
                    module: mock
                    parameters:
                      message: prepared
                  - name: after
                    module: mock
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
    public void testWaitsOnlyActionAndChained() throws Exception {
        // mid-flow action gated by waits alone; its envelope (control records) is consumed
        // downstream by another action via inputs — the sanctioned control-plane chaining
        final String configYaml = SOURCE_YAML + """
                actions:
                  - name: mid
                    module: mock
                    waits:
                      - input
                    parameters:
                      message: mid done
                  - name: notify
                    module: mock
                    trigger: perElement
                    inputs:
                      - mid
                    parameters:
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
                actions:
                  - name: action
                    module: mock
                    trigger: perElement
                    inputs:
                      - input
                    parameters:
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
                actions:
                  - name: action
                    module: mock
                    trigger: collect
                    inputs:
                      - input
                    parameters:
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
                actions:
                  - name: history
                    module: storage
                    trigger: collect
                    inputs:
                      - input
                    parameters:
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
                actions:
                  - name: notify
                    module: mock
                    trigger: collect
                    inputs:
                      - store
                    parameters:
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
                actions:
                  - name: action
                    module: mock
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
    public void testRetryRecoversTransientFailure() throws Exception {
        // the first two executions fail, the third succeeds within maxAttempts: 3 -> one envelope, no failure
        MockAction.EXECUTIONS.remove("flaky");
        final String configYaml = SOURCE_YAML + """
                actions:
                  - name: flaky
                    module: mock
                    inputs:
                      - input
                    retry:
                      maxAttempts: 3
                      initialBackoff: 10ms
                      maxBackoff: 50ms
                    parameters:
                      message: recovered
                      failTimes: 2
                """;
        final Config config = Config.load(configYaml);
        final MCollection output = MPipeline.apply(pipeline, config).get("flaky");

        PAssert.that(output.getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement element : elements) {
                count++;
                Assertions.assertEquals("recovered", element.getPrimitiveValue("payload"));
            }
            Assertions.assertEquals(1, count);
            return null;
        });

        pipeline.run().waitUntilFinish();
        Assertions.assertEquals(3, MockAction.EXECUTIONS.get("flaky").get());
    }

    @Test
    public void testRetryExhaustedIsRoutedToFailure() throws Exception {
        // still failing after maxAttempts: routed as BadRecord (failFast false), output stays empty
        MockAction.EXECUTIONS.remove("hopeless");
        final String configYaml = SOURCE_YAML + """
                actions:
                  - name: hopeless
                    module: mock
                    failFast: false
                    inputs:
                      - input
                    retry:
                      maxAttempts: 2
                      initialBackoff: 10ms
                    parameters:
                      message: never
                      failTimes: 5
                """;
        final Config config = Config.load(configYaml);
        final MCollection output = MPipeline.apply(pipeline, config).get("hopeless");

        PAssert.that(output.getCollection()).empty();

        pipeline.run().waitUntilFinish();
        Assertions.assertEquals(2, MockAction.EXECUTIONS.get("hopeless").get());
    }

    @Test
    public void testRetrySkipsNonRetryableFailure() throws Exception {
        // a NonRetryableException goes straight to failure handling: no further attempts
        MockAction.EXECUTIONS.remove("rejected");
        final String configYaml = SOURCE_YAML + """
                actions:
                  - name: rejected
                    module: mock
                    failFast: false
                    inputs:
                      - input
                    retry:
                      maxAttempts: 3
                      initialBackoff: 10ms
                    parameters:
                      message: never
                      failTimes: 5
                      nonRetryable: true
                """;
        final Config config = Config.load(configYaml);
        final MCollection output = MPipeline.apply(pipeline, config).get("rejected");

        PAssert.that(output.getCollection()).empty();

        pipeline.run().waitUntilFinish();
        Assertions.assertEquals(1, MockAction.EXECUTIONS.get("rejected").get());
    }

    @Test
    public void testFailWhenMatchRoutesToFailure() throws Exception {
        // the mock echoes a JSON payload; a matching failWhen fails the firing without retry
        MockAction.EXECUTIONS.remove("guarded");
        final String configYaml = SOURCE_YAML + """
                actions:
                  - name: guarded
                    module: mock
                    failFast: false
                    inputs:
                      - input
                    retry:
                      maxAttempts: 3
                      initialBackoff: 10ms
                    failWhen: "payload.cnt = 0 AND state = 'DONE'"
                    parameters:
                      message: '{"cnt": 0, "table": "t"}'
                """;
        final Config config = Config.load(configYaml);
        final MCollection output = MPipeline.apply(pipeline, config).get("guarded");

        PAssert.that(output.getCollection()).empty();

        pipeline.run().waitUntilFinish();
        Assertions.assertEquals(1, MockAction.EXECUTIONS.get("guarded").get());
    }

    @Test
    public void testFailWhenNoMatchAndSkipWhenMatch() throws Exception {
        final String configYaml = SOURCE_YAML + """
                actions:
                  - name: passed
                    module: mock
                    inputs:
                      - input
                    failWhen: payload.cnt > 10
                    parameters:
                      message: '{"cnt": 3}'
                  - name: skipped
                    module: mock
                    inputs:
                      - input
                    skipWhen: payload.cnt < 5
                    parameters:
                      message: '{"cnt": 3}'
                  - name: after
                    module: mock
                    waits:
                      - skipped
                    parameters:
                      message: ran
                """;
        final Config config = Config.load(configYaml);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        PAssert.that(outputs.get("passed").getCollection()).satisfies(elements -> {
            for(final MElement element : elements) {
                Assertions.assertEquals("DONE", element.getPrimitiveValue("state"));
            }
            return null;
        });
        PAssert.that(outputs.get("skipped").getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement element : elements) {
                count++;
                Assertions.assertEquals("SKIPPED", element.getPrimitiveValue("state"));
                Assertions.assertEquals("mock-job", element.getPrimitiveValue("jobId"));
                Assertions.assertEquals("{\"cnt\": 3}", element.getPrimitiveValue("payload"));
            }
            Assertions.assertEquals(1, count);
            return null;
        });
        // a skipped action still releases its waiters
        PAssert.that(outputs.get("after").getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement ignored : elements) {
                count++;
            }
            Assertions.assertEquals(1, count);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testFailWhenMessageIsBounded() {
        Assertions.assertEquals("abc", Action.abbreviate("abc", 5));
        Assertions.assertEquals("abcde...(10 chars)", Action.abbreviate("abcdefghij", 5));
        Assertions.assertNull(Action.abbreviate(null, 5));
    }

    @Test
    public void testConditionOnNonJsonPayloadDoesNotFail() throws Exception {
        // a dotted path into a plain-text payload cannot match; it must not fail the firing
        final String configYaml = SOURCE_YAML + """
                actions:
                  - name: plain
                    module: mock
                    inputs:
                      - input
                    failWhen: payload.cnt = 0
                    skipWhen: "payload = 'bytes: 3'"
                    parameters:
                      message: "bytes: 3"
                """;
        final Config config = Config.load(configYaml);
        final MCollection output = MPipeline.apply(pipeline, config).get("plain");
        PAssert.that(output.getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement element : elements) {
                count++;
                Assertions.assertEquals("SKIPPED", element.getPrimitiveValue("state"));
            }
            Assertions.assertEquals(1, count);
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testIllegalConditionIsAssemblyError() throws Exception {
        final String configYaml = SOURCE_YAML + """
                actions:
                  - name: bad
                    module: mock
                    inputs:
                      - input
                    failWhen: "payload.cnt >"
                    parameters:
                      message: x
                """;
        final Config config = Config.load(configYaml);
        final IllegalModuleException e = Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, config));
        Assertions.assertTrue(e.getMessage().contains("failWhen"), e.getMessage());
    }

    @Test
    public void testOperationsDeclaredAndEnumsStayInSync() {
        // the @Action.Service(operations) list (validated at assembly) and each service's Op enum
        // (used to branch in configure/execute) must describe the same set of config values
        Assertions.assertEquals(
                java.util.Arrays.stream(com.mercari.solution.module.action.BigQueryAction.Op.values()).map(o -> o.operation).collect(java.util.stream.Collectors.toSet()),
                new HashSet<>(Action.operations("bigquery")));
        Assertions.assertEquals(
                java.util.Arrays.stream(com.mercari.solution.module.action.TasksAction.Op.values()).map(o -> o.operation).collect(java.util.stream.Collectors.toSet()),
                new HashSet<>(Action.operations("tasks")));
        Assertions.assertEquals(
                java.util.Arrays.stream(com.mercari.solution.module.action.vertexai.GeminiAction.Op.values()).map(o -> o.operation).collect(java.util.stream.Collectors.toSet()),
                new HashSet<>(Action.operations("vertexai_gemini")));
        Assertions.assertEquals(
                java.util.Arrays.stream(com.mercari.solution.module.action.DataflowAction.Op.values()).map(o -> o.operation).collect(java.util.stream.Collectors.toSet()),
                new HashSet<>(Action.operations("dataflow")));
        Assertions.assertEquals(
                java.util.Arrays.stream(com.mercari.solution.module.action.BuildAction.Op.values()).map(o -> o.operation).collect(java.util.stream.Collectors.toSet()),
                new HashSet<>(Action.operations("build")));
        Assertions.assertTrue(Action.operations("storage").isEmpty());
        Assertions.assertTrue(Action.operations("http").isEmpty());
    }

    @Test
    public void testCollectFireOnEmpty() throws Exception {
        // every input record is filtered out; without fireOnEmpty the collect action does not fire,
        // with it the action fires once with zero elements
        final String base = SOURCE_YAML + """
                transforms:
                  - name: none
                    module: select
                    inputs:
                      - input
                    parameters:
                      filter:
                        key: field_long
                        op: ">"
                        value: 100
                """;
        final String silent = base + """
                actions:
                  - name: silent
                    module: mock
                    trigger: collect
                    inputs:
                      - none
                    parameters:
                      message: "${size} records"
                """;
        final TestPipeline p1 = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        PAssert.that(MPipeline.apply(p1, Config.load(silent)).get("silent").getCollection()).empty();
        p1.run();

        final String fired = base + """
                actions:
                  - name: fired
                    module: mock
                    trigger: collect
                    fireOnEmpty: true
                    inputs:
                      - none
                    parameters:
                      message: "${size} records"
                """;
        final TestPipeline p2 = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        PAssert.that(MPipeline.apply(p2, Config.load(fired)).get("fired").getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement element : elements) {
                count++;
                Assertions.assertEquals("0 records", element.getPrimitiveValue("payload"));
            }
            Assertions.assertEquals(1, count);
            return null;
        });
        p2.run();
    }

    @Test
    public void testValidationErrors() {
        // unknown service
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, Config.load(SOURCE_YAML + """
                actions:
                  - name: action
                    module: nosuchservice
                    inputs:
                      - input
                    parameters:
                      message: msg
                """)));

        // the former action.<service> placement inside sources/transforms/sinks is not a module any more
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, Config.load(SOURCE_YAML + """
                sinks:
                  - name: action
                    module: action.mock
                    inputs:
                      - input
                    parameters:
                      message: msg
                """)));

        // trigger is a module-level field: parameters.trigger is rejected
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, Config.load(SOURCE_YAML + """
                actions:
                  - name: action
                    module: mock
                    inputs:
                      - input
                    parameters:
                      trigger: perElement
                      message: msg
                """)));

        // unknown trigger value
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, Config.load(SOURCE_YAML + """
                actions:
                  - name: action
                    module: mock
                    trigger: sometimes
                    inputs:
                      - input
                    parameters:
                      message: msg
                """)));

        // operation: required for multi-operation services, must match the declared list,
        // and must be absent for single-operation services; parameters.op is rejected
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, Config.load(SOURCE_YAML + """
                actions:
                  - name: action
                    module: bigquery
                    inputs:
                      - input
                    parameters:
                      query: SELECT 1
                """)));
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, Config.load(SOURCE_YAML + """
                actions:
                  - name: action
                    module: bigquery
                    operation: jobs.insert
                    inputs:
                      - input
                    parameters:
                      query: SELECT 1
                """)));
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, Config.load(SOURCE_YAML + """
                actions:
                  - name: action
                    module: mock
                    operation: echo
                    inputs:
                      - input
                    parameters:
                      message: msg
                """)));
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, Config.load(SOURCE_YAML + """
                actions:
                  - name: action
                    module: bigquery
                    operation: jobs.query
                    inputs:
                      - input
                    parameters:
                      op: query
                      query: SELECT 1
                """)));

        // fireOnEmpty only applies to collect
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, Config.load(SOURCE_YAML + """
                actions:
                  - name: action
                    module: mock
                    fireOnEmpty: true
                    inputs:
                      - input
                    parameters:
                      message: msg
                """)));

        // fireOnEmpty needs the global window: a windowing strategy is rejected at assembly
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, Config.load(SOURCE_YAML + """
                actions:
                  - name: action
                    module: mock
                    trigger: collect
                    fireOnEmpty: true
                    inputs:
                      - input
                    strategy:
                      window:
                        type: fixed
                        unit: minute
                        size: 1
                    parameters:
                      message: msg
                """)));

        // retry.maxAttempts must be >= 1, backoffs must be durations
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, Config.load(SOURCE_YAML + """
                actions:
                  - name: action
                    module: mock
                    inputs:
                      - input
                    retry:
                      maxAttempts: 0
                    parameters:
                      message: msg
                """)));
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, Config.load(SOURCE_YAML + """
                actions:
                  - name: action
                    module: mock
                    inputs:
                      - input
                    retry:
                      initialBackoff: soon
                    parameters:
                      message: msg
                """)));

        // perElement without inputs
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, Config.load("""
                actions:
                  - name: action
                    module: mock
                    trigger: perElement
                    parameters:
                      message: msg
                """)));

        // collect without inputs
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, Config.load("""
                actions:
                  - name: action
                    module: mock
                    trigger: collect
                    parameters:
                      message: msg
                """)));

        // bigquery service parameter validation is reached through the module (query op without query)
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, Config.load(SOURCE_YAML + """
                actions:
                  - name: action
                    module: bigquery
                    operation: jobs.query
                    inputs:
                      - input
                    parameters:
                """)));

        // sourceUrisField requires trigger: collect
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, Config.load(SOURCE_YAML + """
                actions:
                  - name: action
                    module: bigquery
                    operation: jobs.load
                    inputs:
                      - input
                    parameters:
                      sourceUrisField: path
                      destinationTable: p.d.t
                """)));
    }

}
