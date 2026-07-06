package com.mercari.solution.module.source;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.MCollection;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * Characterization tests for the PubSub source's assembly-time schema validation and schema
 * registration (docs/developer/schema-redesign.md Phase 0). These exercise only pipeline assembly
 * (no emulator, the pipeline is never run): the schema-requirement rules reference the top-level
 * config schema that Phase 3 moves into parameters, so their messages and trigger conditions are
 * pinned here.
 */
public class PubSubSourceTest {

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    private void setStreaming() {
        pipeline.getOptions().as(StreamingOptions.class).setStreaming(true);
    }

    @Test
    public void testProtobufFormatRequiresSchema() throws Exception {
        setStreaming();
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "pubsubinput",
                      "module": "pubsub",
                      "parameters": {
                        "subscription": "projects/myproject/subscriptions/mysubscription",
                        "format": "protobuf"
                      }
                    }
                  ]
                }
                """;
        final Config config = Config.load(configJson);
        final IllegalModuleException e = Assertions.assertThrows(IllegalModuleException.class,
                () -> MPipeline.apply(pipeline, config));
        Assertions.assertTrue(e.getMessage().contains("schema is required if format is protobuf"),
                "unexpected message: " + e.getMessage());
    }

    @Test
    public void testProtobufFormatRequiresProtobufBlock() throws Exception {
        setStreaming();
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "pubsubinput",
                      "module": "pubsub",
                      "schema": {
                        "fields": [ { "name": "stringField", "type": "string" } ]
                      },
                      "parameters": {
                        "subscription": "projects/myproject/subscriptions/mysubscription",
                        "format": "protobuf"
                      }
                    }
                  ]
                }
                """;
        final Config config = Config.load(configJson);
        final IllegalModuleException e = Assertions.assertThrows(IllegalModuleException.class,
                () -> MPipeline.apply(pipeline, config));
        Assertions.assertTrue(e.getMessage().contains("schema.protobuf is required if format is protobuf"),
                "unexpected message: " + e.getMessage());
    }

    @Test
    public void testBatchModeIsRejected() throws Exception {
        // streaming left unset: assembly must fail before any schema handling
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "pubsubinput",
                      "module": "pubsub",
                      "parameters": {
                        "subscription": "projects/myproject/subscriptions/mysubscription"
                      }
                    }
                  ]
                }
                """;
        final Config config = Config.load(configJson);
        final IllegalModuleException e = Assertions.assertThrows(IllegalModuleException.class,
                () -> MPipeline.apply(pipeline, config));
        Assertions.assertTrue(e.getMessage().contains("streaming"),
                "unexpected message: " + e.getMessage());
    }

    @Test
    public void testJsonFormatRegistersDeclaredSchema() throws Exception {
        setStreaming();
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "pubsubinput",
                      "module": "pubsub",
                      "schema": {
                        "fields": [
                          { "name": "stringField", "type": "string" },
                          { "name": "longField", "type": "long" }
                        ]
                      },
                      "parameters": {
                        "subscription": "projects/myproject/subscriptions/mysubscription",
                        "format": "json"
                      }
                    }
                  ]
                }
                """;
        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        Assertions.assertTrue(outputs.containsKey("pubsubinput"));
        final MCollection output = outputs.get("pubsubinput");
        Assertions.assertTrue(output.getSchema().hasField("stringField"));
        Assertions.assertTrue(output.getSchema().hasField("longField"));
    }

}
