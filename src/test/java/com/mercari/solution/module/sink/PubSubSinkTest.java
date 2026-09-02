package com.mercari.solution.module.sink;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.IllegalModuleException;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Assembly-time tests for the PubSub sink's schema handling (schema-redesign.md Phase 4).
 * No emulator: the pipeline is assembled but never run.
 */
public class PubSubSinkTest {

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    private static String config(final String sinkParameters) {
        return """
                {
                  "sources": [
                    {
                      "name": "input",
                      "module": "create",
                      "parameters": {
                        "type": "int64",
                        "elements": [0, 1]
                      }
                    }
                  ],
                  "sinks": [
                    {
                      "name": "output",
                      "module": "pubsub",
                      "inputs": ["input"],
                      "parameters": %s
                    }
                  ]
                }
                """.formatted(sinkParameters);
    }

    @Test
    public void testFormatDerivedFromEncoding() throws Exception {
        // parameters.format omitted -> derived from schema.encoding.format
        // (without the derivation this fails with "parameters.format must not be null")
        final String configJson = config("""
                {
                  "topic": "projects/myproject/topics/mytopic",
                  "schema": {
                    "fields": [ { "name": "stringField", "type": "string" } ],
                    "encoding": { "format": "avro" }
                  }
                }
                """);
        final Config config = Config.load(configJson);
        MPipeline.apply(pipeline, config);
    }

    @Test
    public void testDynamicTopicTemplate() throws Exception {
        // dynamic topic: writeMessagesDynamic without .to() reads the message topic field,
        // and the message PCollection gets a topic-preserving coder
        final String configJson = config("""
                {
                  "topic": "projects/myproject/topics/topic_${value}",
                  "format": "json"
                }
                """);
        final Config config = Config.load(configJson);
        MPipeline.apply(pipeline, config);
    }

    @Test
    public void testDynamicTopicTemplateWithOrderingKeyFields() throws Exception {
        // orderingKeyFields + dynamic topic: schema coder (preserves topic and ordering key)
        // and withOrderingKey on the write
        final String configJson = config("""
                {
                  "topic": "projects/myproject/topics/topic_${value}",
                  "format": "json",
                  "orderingKeyFields": ["value"]
                }
                """);
        final Config config = Config.load(configJson);
        MPipeline.apply(pipeline, config);
    }

    @Test
    public void testStaticTopicWithOrderingKeyFields() throws Exception {
        final String configJson = config("""
                {
                  "topic": "projects/myproject/topics/mytopic",
                  "format": "json",
                  "orderingKeyFields": ["value"]
                }
                """);
        final Config config = Config.load(configJson);
        MPipeline.apply(pipeline, config);
    }

    @Test
    public void testMissingFormatStillFailsWithoutEncoding() throws Exception {
        final String configJson = config("""
                {
                  "topic": "projects/myproject/topics/mytopic"
                }
                """);
        final Config config = Config.load(configJson);
        final IllegalModuleException e = Assertions.assertThrows(IllegalModuleException.class,
                () -> MPipeline.apply(pipeline, config));
        Assertions.assertTrue(e.getMessage().contains("parameters.format must not be null"),
                "unexpected message: " + e.getMessage());
    }

    @Test
    public void testProtobufRequiresDefinition() throws Exception {
        // format protobuf with a fields-only schema: the definition (descriptor) is missing
        final String configJson = config("""
                {
                  "topic": "projects/myproject/topics/mytopic",
                  "format": "protobuf",
                  "schema": {
                    "fields": [ { "name": "stringField", "type": "string" } ]
                  }
                }
                """);
        final Config config = Config.load(configJson);
        final IllegalModuleException e = Assertions.assertThrows(IllegalModuleException.class,
                () -> MPipeline.apply(pipeline, config));
        Assertions.assertTrue(e.getMessage().contains("schema.protobuf is required if format is protobuf"),
                "unexpected message: " + e.getMessage());
    }

}
