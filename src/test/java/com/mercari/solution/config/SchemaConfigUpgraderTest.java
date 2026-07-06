package com.mercari.solution.config;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class SchemaConfigUpgraderTest {

    private static JsonObject parse(final String json) {
        return new Gson().fromJson(json, JsonObject.class);
    }

    @Test
    public void testMoveTopLevelSchemaAndRewriteProtobuf() {
        final JsonObject config = parse("""
                {
                  "sources": [
                    {
                      "name": "input",
                      "module": "pubsub",
                      "schema": {
                        "fields": [ { "name": "stringField", "type": "string" } ],
                        "protobuf": {
                          "descriptorFile": "gs://bucket/schema/test.desc",
                          "messageName": "com.example.Event"
                        }
                      },
                      "parameters": {
                        "subscription": "projects/p/subscriptions/s",
                        "format": "protobuf"
                      }
                    }
                  ]
                }
                """);

        final SchemaConfigUpgrader.Result result = SchemaConfigUpgrader.upgrade(config);

        // the input is not mutated
        Assertions.assertTrue(config.getAsJsonArray("sources").get(0).getAsJsonObject().has("schema"));

        final JsonObject module = result.config().getAsJsonArray("sources").get(0).getAsJsonObject();
        Assertions.assertFalse(module.has("schema"));
        final JsonObject schema = module.getAsJsonObject("parameters").getAsJsonObject("schema");
        Assertions.assertFalse(schema.has("protobuf"));
        Assertions.assertEquals("protobuf", schema.getAsJsonObject("encoding").get("format").getAsString());
        Assertions.assertEquals("com.example.Event", schema.getAsJsonObject("encoding").get("messageName").getAsString());
        Assertions.assertEquals("gs://bucket/schema/test.desc", schema.getAsJsonObject("reference").get("uri").getAsString());

        Assertions.assertEquals(2, result.changes().size());

        // the upgraded schema block parses under the new format
        final Schema parsed = Schema.parse(schema);
        Assertions.assertEquals(Schema.Encoding.Format.protobuf, parsed.getEncoding().getFormat());
    }

    @Test
    public void testRewriteAvroInlineAndDeprecatedAlias() {
        final JsonObject config = parse("""
                {
                  "sources": [
                    {
                      "name": "a",
                      "module": "pubsub",
                      "parameters": {
                        "schema": { "avro": { "json": "{\\"type\\":\\"record\\"}" } }
                      }
                    },
                    {
                      "name": "b",
                      "module": "storage",
                      "parameters": {
                        "schema": {
                          "fields": [ { "name": "f", "type": "string" } ],
                          "avroSchema": "gs://bucket/schema/test.avsc"
                        }
                      }
                    }
                  ]
                }
                """);

        final SchemaConfigUpgrader.Result result = SchemaConfigUpgrader.upgrade(config);

        final JsonObject schemaA = result.config().getAsJsonArray("sources").get(0).getAsJsonObject()
                .getAsJsonObject("parameters").getAsJsonObject("schema");
        Assertions.assertEquals("avro", schemaA.getAsJsonObject("encoding").get("format").getAsString());
        Assertions.assertTrue(schemaA.getAsJsonObject("reference").has("inline"));

        final JsonObject schemaB = result.config().getAsJsonArray("sources").get(1).getAsJsonObject()
                .getAsJsonObject("parameters").getAsJsonObject("schema");
        Assertions.assertFalse(schemaB.has("avroSchema"));
        Assertions.assertEquals("gs://bucket/schema/test.avsc", schemaB.getAsJsonObject("reference").get("uri").getAsString());
        // fields are preserved
        Assertions.assertTrue(schemaB.has("fields"));
    }

    @Test
    public void testRewriteUseDestinationSchemaAndPubSubSinkParameter() {
        final JsonObject config = parse("""
                {
                  "sinks": [
                    {
                      "name": "out1",
                      "module": "bigquery",
                      "parameters": {
                        "schema": { "useDestinationSchema": true }
                      }
                    },
                    {
                      "name": "out2",
                      "module": "pubsub",
                      "parameters": {
                        "topic": "projects/p/topics/t",
                        "format": "json",
                        "useDestinationSchema": true
                      }
                    }
                  ]
                }
                """);

        final SchemaConfigUpgrader.Result result = SchemaConfigUpgrader.upgrade(config);

        final JsonObject schema1 = result.config().getAsJsonArray("sinks").get(0).getAsJsonObject()
                .getAsJsonObject("parameters").getAsJsonObject("schema");
        Assertions.assertFalse(schema1.has("useDestinationSchema"));
        Assertions.assertTrue(schema1.getAsJsonObject("reference").get("destination").getAsBoolean());

        final JsonObject parameters2 = result.config().getAsJsonArray("sinks").get(1).getAsJsonObject()
                .getAsJsonObject("parameters");
        Assertions.assertFalse(parameters2.has("useDestinationSchema"));
        Assertions.assertTrue(parameters2.getAsJsonObject("schema")
                .getAsJsonObject("reference").get("destination").getAsBoolean());
    }

    @Test
    public void testUnsafeCasesAreLeftUnchangedWithNotes() {
        final JsonObject config = parse("""
                {
                  "sources": [
                    {
                      "name": "ambiguous",
                      "module": "pubsub",
                      "parameters": {
                        "schema": {
                          "avro": { "json": "{}" },
                          "protobuf": { "descriptorFile": "gs://b/f.desc", "messageName": "m" }
                        }
                      }
                    },
                    {
                      "name": "mixed",
                      "module": "pubsub",
                      "parameters": {
                        "schema": {
                          "avro": { "json": "{}" },
                          "encoding": { "format": "avro" }
                        }
                      }
                    },
                    {
                      "name": "bothLocations",
                      "module": "pubsub",
                      "schema": { "fields": [ { "name": "f", "type": "string" } ] },
                      "parameters": {
                        "schema": { "fields": [ { "name": "f", "type": "string" } ] }
                      }
                    }
                  ]
                }
                """);

        final SchemaConfigUpgrader.Result result = SchemaConfigUpgrader.upgrade(config);

        // ambiguous avro+protobuf: untouched
        final JsonObject ambiguous = result.config().getAsJsonArray("sources").get(0).getAsJsonObject()
                .getAsJsonObject("parameters").getAsJsonObject("schema");
        Assertions.assertTrue(ambiguous.has("avro"));
        Assertions.assertTrue(ambiguous.has("protobuf"));

        // old+new mixed: untouched
        final JsonObject mixed = result.config().getAsJsonArray("sources").get(1).getAsJsonObject()
                .getAsJsonObject("parameters").getAsJsonObject("schema");
        Assertions.assertTrue(mixed.has("avro"));

        // both locations: top-level stays where it was
        final JsonObject bothLocations = result.config().getAsJsonArray("sources").get(2).getAsJsonObject();
        Assertions.assertTrue(bothLocations.has("schema"));

        final List<String> changes = result.changes();
        Assertions.assertTrue(changes.stream().anyMatch(c -> c.contains("ambiguous")), changes.toString());
        Assertions.assertTrue(changes.stream().anyMatch(c -> c.contains("mixes old-format and new-format")), changes.toString());
        Assertions.assertTrue(changes.stream().anyMatch(c -> c.contains("both at the module top level and in parameters.schema")), changes.toString());
    }

    @Test
    public void testNewFormatConfigNeedsNoChanges() {
        final JsonObject config = parse("""
                {
                  "sources": [
                    {
                      "name": "input",
                      "module": "pubsub",
                      "parameters": {
                        "subscription": "projects/p/subscriptions/s",
                        "schema": {
                          "fields": [ { "name": "f", "type": "string" } ],
                          "encoding": { "format": "avro" }
                        }
                      }
                    }
                  ]
                }
                """);
        final SchemaConfigUpgrader.Result result = SchemaConfigUpgrader.upgrade(config);
        Assertions.assertTrue(result.changes().isEmpty(), result.changes().toString());
        Assertions.assertEquals(config, result.config());
    }

}
