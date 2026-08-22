package com.mercari.solution.module.sink;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class GrpcSinkTest {

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    private static GrpcSinkTestServer server;
    private static Path descriptorSetPath;

    @BeforeAll
    public static void startServer() throws Exception {
        server = new GrpcSinkTestServer();
        descriptorSetPath = server.writeDescriptorSet(Files.createTempDirectory("grpc-sink-test"));
    }

    @AfterAll
    public static void stopServer() {
        if(server != null) {
            server.shutdown();
        }
    }

    private static String desc() {
        return descriptorSetPath.toString().replace("\\", "/");
    }

    private static String source(final String... ids) {
        final StringBuilder sb = new StringBuilder("""
                sources:
                  - name: input
                    module: create
                    parameters:
                      type: element
                      elements:
                """);
        int i = 1;
        for(final String id : ids) {
            sb.append("        - { id: ").append(id).append(", name: item-").append(id).append(", price: ").append(i * 1.5).append(", tenant: t").append(i % 2).append(" }\n");
            i++;
        }
        sb.append("""
                    schema:
                      fields:
                        - { name: id, type: string }
                        - { name: name, type: string }
                        - { name: price, type: float64 }
                        - { name: tenant, type: string }
                """);
        return sb.toString();
    }

    private static Map<String, Integer> states(final Iterable<MElement> elements) {
        final Map<String, Integer> states = new TreeMap<>();
        for(final MElement element : elements) {
            states.merge((String) element.getPrimitiveValue("state"), 1, Integer::sum);
        }
        return states;
    }

    @Test
    public void testUnaryPerElementWithRetryAndFailure() throws Exception {
        server.upserted.clear();
        server.flakyCalls.set(0);
        server.metadataSeen.clear();
        final String configYaml = source("a1", "flaky", "bad") + """
                sinks:
                  - name: grpc
                    module: grpc
                    inputs: [input]
                    failFast: false
                    parameters:
                      target: localhost:%d
                      plaintext: true
                      descriptorSetPath: %s
                      method: demo.ItemService/Upsert
                      metadata:
                        x-tenant: ${tenant}
                      request:
                        fields: [id, name, price]
                      response:
                        retry: { statuses: [UNAVAILABLE], maxAttempts: 3, initialBackoff: 10ms, maxBackoff: 20ms }
                      concurrency: 2
                """.formatted(server.port(), desc());
        final Config config = Config.load(configYaml);
        final MCollection output = MPipeline.apply(pipeline, config).get("grpc");
        PAssert.that(output.getCollection()).satisfies(elements -> {
            final Map<String, Integer> states = states(elements);
            Assertions.assertEquals(Map.of("SUCCEEDED", 2, "FAILED", 1), states);
            for(final MElement e : elements) {
                Assertions.assertEquals("demo.ItemService/Upsert", e.getPrimitiveValue("method"));
                Assertions.assertEquals(1L, e.getPrimitiveValue("elementCount"));
                if("FAILED".equals(e.getPrimitiveValue("state"))) {
                    Assertions.assertEquals("INVALID_ARGUMENT", e.getPrimitiveValue("status"));
                    Assertions.assertTrue(((String) e.getPrimitiveValue("error")).contains("bad id"));
                } else {
                    Assertions.assertEquals("OK", e.getPrimitiveValue("status"));
                    Assertions.assertTrue(((String) e.getPrimitiveValue("payload")).contains("\"version\""));
                    Assertions.assertTrue((Long) e.getPrimitiveValue("bytes") > 0);
                }
            }
            return null;
        });
        pipeline.run();

        Assertions.assertEquals(Set.of("a1:item-a1:1.5", "flaky:item-flaky:3.0"), new HashSet<>(server.upserted));
        Assertions.assertEquals(2, server.flakyCalls.get());
        // per-element metadata template
        Assertions.assertTrue(server.metadataSeen.stream().anyMatch(m -> m.startsWith("t1|")) && server.metadataSeen.stream().anyMatch(m -> m.startsWith("t0|")), server.metadataSeen.toString());
    }

    @Test
    public void testBatchRepeatedFieldAndClientStreaming() throws Exception {
        server.bulks.clear();
        final String configYaml = source("b1", "b2", "b3", "b4") + """
                sinks:
                  - name: bulk
                    module: grpc
                    inputs: [input]
                    parameters:
                      target: localhost:%d
                      plaintext: true
                      descriptorSetPath: %s
                      method: demo.ItemService/BulkUpsert
                      request: { fields: [id, name] }
                      batch: { maxSize: 10, shards: 1, repeatedField: items }
                      response:
                        successCondition: { key: payload.ok, op: "=", value: true }
                  - name: stream
                    module: grpc
                    inputs: [input]
                    parameters:
                      target: localhost:%d
                      plaintext: true
                      descriptorSetPath: %s
                      method: demo.ItemService/StreamUpsert
                      batch: { maxSize: 2, key: "${tenant}" }
                """.formatted(server.port(), desc(), server.port(), desc());
        final Config config = Config.load(configYaml);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);
        PAssert.that(outputs.get("bulk").getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement e : elements) {
                count++;
                Assertions.assertEquals("SUCCEEDED", e.getPrimitiveValue("state"));
                Assertions.assertEquals(4L, e.getPrimitiveValue("elementCount"));
                Assertions.assertTrue(((String) e.getPrimitiveValue("payload")).contains("\"count\":\"4\"") || ((String) e.getPrimitiveValue("payload")).contains("\"count\":4"), (String) e.getPrimitiveValue("payload"));
            }
            Assertions.assertEquals(1, count);
            return null;
        });
        PAssert.that(outputs.get("stream").getCollection()).satisfies(elements -> {
            long total = 0;
            for(final MElement e : elements) {
                Assertions.assertEquals("SUCCEEDED", e.getPrimitiveValue("state"));
                total += (Long) e.getPrimitiveValue("elementCount");
            }
            Assertions.assertEquals(4L, total);
            return null;
        });
        pipeline.run();

        final List<List<String>> bulks = new ArrayList<>(server.bulks);
        Assertions.assertTrue(bulks.stream().anyMatch(b -> new HashSet<>(b).equals(Set.of("b1", "b2", "b3", "b4"))), bulks.toString());
        // streamed batches are keyed by tenant: t1 = {b1, b3}, t0 = {b2, b4}
        Assertions.assertTrue(bulks.stream().anyMatch(b -> new HashSet<>(b).equals(Set.of("b1", "b3"))), bulks.toString());
        Assertions.assertTrue(bulks.stream().anyMatch(b -> new HashSet<>(b).equals(Set.of("b2", "b4"))), bulks.toString());
    }

    @Test
    public void testTemplateMappingAndSuccessConditionFailure() throws Exception {
        server.bulks.clear();
        final String configYaml = source("reject", "ok1") + """
                sinks:
                  - name: bulk
                    module: grpc
                    inputs: [input]
                    failFast: false
                    parameters:
                      target: localhost:%d
                      plaintext: true
                      descriptorSetPath: %s
                      method: demo.ItemService/BulkUpsert
                      request:
                        template: '{"tenant": "${key}", "items": [<#list elements as e>{"id": "${e.id}", "name": "${e.name}"}<#sep>, </#list>]}'
                      batch: { maxSize: 10, key: "${tenant}" }
                      response:
                        successCondition: { key: payload.ok, op: "=", value: true }
                """.formatted(server.port(), desc());
        final Config config = Config.load(configYaml);
        final MCollection output = MPipeline.apply(pipeline, config).get("bulk");
        PAssert.that(output.getCollection()).satisfies(elements -> {
            // tenant t1 = {reject} -> ok=false -> FAILED ; tenant t0 = {ok1} -> SUCCEEDED
            Assertions.assertEquals(Map.of("SUCCEEDED", 1, "FAILED", 1), states(elements));
            for(final MElement e : elements) {
                if("FAILED".equals(e.getPrimitiveValue("state"))) {
                    Assertions.assertTrue(((String) e.getPrimitiveValue("error")).contains("successCondition"));
                }
            }
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testValidate() {
        // unknown method fails at assembly
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, Config.load(source("x") + """
                sinks:
                  - name: grpc
                    module: grpc
                    inputs: [input]
                    parameters:
                      target: localhost:%d
                      plaintext: true
                      descriptorSetPath: %s
                      method: demo.ItemService/Nope
                """.formatted(server.port(), desc()))));
        // batching into a unary method requires repeatedField
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, Config.load(source("x") + """
                sinks:
                  - name: grpc
                    module: grpc
                    inputs: [input]
                    parameters:
                      target: localhost:%d
                      plaintext: true
                      descriptorSetPath: %s
                      method: demo.ItemService/Upsert
                      batch: { maxSize: 10 }
                """.formatted(server.port(), desc()))));
        // unknown retry status
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, Config.load(source("x") + """
                sinks:
                  - name: grpc
                    module: grpc
                    inputs: [input]
                    parameters:
                      target: localhost:%d
                      plaintext: true
                      descriptorSetPath: %s
                      method: demo.ItemService/Upsert
                      response: { retry: { statuses: [BUSY] } }
                """.formatted(server.port(), desc()))));
    }
}
