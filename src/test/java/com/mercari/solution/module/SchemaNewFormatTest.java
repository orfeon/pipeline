package com.mercari.solution.module;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests for the new-format schema keys (docs/developer/schema-redesign.md Phase 2):
 * {@code encoding} (wire format) and {@code reference} (definition source). The new format
 * normalizes into the same internals as the old format, so the legacy accessors
 * ({@code getAvro()} / {@code getProtobuf()} / {@code getUseDestinationSchema()}) must behave
 * identically for equivalent declarations.
 */
public class SchemaNewFormatTest {

    private static final String AVRO_JSON = """
            {
              "type": "record",
              "name": "root",
              "fields": [
                { "name": "stringField", "type": ["null", "string"] },
                { "name": "longField", "type": "long" }
              ]
            }
            """;

    private static JsonObject avroInlineConfig() {
        final JsonObject reference = new JsonObject();
        reference.add("inline", new JsonPrimitive(AVRO_JSON));
        final JsonObject encoding = new JsonObject();
        encoding.add("format", new JsonPrimitive("avro"));
        final JsonObject config = new JsonObject();
        config.add("encoding", encoding);
        config.add("reference", reference);
        return config;
    }

    @Test
    public void testAvroInlineEquivalentToLegacy() {
        final Schema schema = Schema.parse(avroInlineConfig());

        // declaration is captured
        Assertions.assertEquals(Schema.Encoding.Format.avro, schema.getEncoding().getFormat());
        Assertions.assertNotNull(schema.getReference().getInline());
        Assertions.assertNull(schema.getReference().getUri());

        // same observable behavior as the legacy {"avro": {"json": ...}} form
        Assertions.assertTrue(schema.hasField("stringField"));
        Assertions.assertTrue(schema.hasField("longField"));
        Assertions.assertNotNull(schema.getAvro().getJson());
        Assertions.assertEquals("root", schema.getAvroSchema().getName());

        // equivalence with the legacy spelling
        final JsonObject legacyAvro = new JsonObject();
        legacyAvro.add("json", new JsonPrimitive(AVRO_JSON));
        final JsonObject legacyConfig = new JsonObject();
        legacyConfig.add("avro", legacyAvro);
        final Schema legacy = Schema.parse(legacyConfig);
        Assertions.assertEquals(legacy.getFields().size(), schema.getFields().size());
        Assertions.assertEquals(legacy.getAvroSchema(), schema.getAvroSchema());
    }

    @Test
    public void testAvroWithFieldsOnlyKeepsEncodingDeclaration() {
        final Schema schema = Schema.parse("""
                {
                  "fields": [ { "name": "stringField", "type": "string" } ],
                  "encoding": { "format": "avro" }
                }
                """);
        Assertions.assertEquals(Schema.Encoding.Format.avro, schema.getEncoding().getFormat());
        Assertions.assertNull(schema.getReference());
        // no document declared: the avro representation derives from fields as usual
        Assertions.assertNotNull(schema.getAvro().getSchema().getField("stringField"));
    }

    @Test
    public void testProtobufPopulatesLegacyAccessors() {
        final Schema schema = Schema.parse("""
                {
                  "fields": [ { "name": "stringField", "type": "string" } ],
                  "encoding": {
                    "format": "protobuf",
                    "messageName": "com.mercari.solution.entity.TestMessage"
                  },
                  "reference": { "uri": "gs://example-bucket/schema/test.desc" }
                }
                """);
        Assertions.assertEquals(Schema.Encoding.Format.protobuf, schema.getEncoding().getFormat());
        Assertions.assertEquals("com.mercari.solution.entity.TestMessage", schema.getEncoding().getMessageName());
        Assertions.assertEquals("gs://example-bucket/schema/test.desc", schema.getReference().getUri());

        // module validations (pubsub etc.) read these legacy accessors
        Assertions.assertNotNull(schema.getProtobuf());
        Assertions.assertEquals("gs://example-bucket/schema/test.desc", schema.getProtobuf().getDescriptorFile());
        Assertions.assertEquals("com.mercari.solution.entity.TestMessage", schema.getProtobuf().getMessageName());
    }

    @Test
    public void testDestinationReference() {
        final Schema schema = Schema.parse("""
                {
                  "fields": [ { "name": "stringField", "type": "string" } ],
                  "reference": { "destination": true }
                }
                """);
        Assertions.assertNull(schema.getEncoding());
        Assertions.assertEquals(Boolean.TRUE, schema.getReference().getDestination());
        // legacy accessor keeps working
        Assertions.assertEquals(Boolean.TRUE, schema.getUseDestinationSchema());
    }

    @Test
    public void testDestinationOnlyReference() {
        // Phase 4: a destination-only declaration parses into a placeholder schema
        // whose definition the sink resolves from the write destination
        final Schema schema = Schema.parse("""
                { "reference": { "destination": true } }
                """);
        Assertions.assertTrue(schema.isDestinationReference());
        Assertions.assertTrue(schema.getFields() == null || schema.getFields().isEmpty());

        // legacy spelling now parses too (it used to throw with no fields)
        final Schema legacy = Schema.parse("""
                { "useDestinationSchema": true }
                """);
        Assertions.assertTrue(legacy.isDestinationReference());

        // a schema with its own fields is not a destination placeholder even when flagged
        final Schema withFields = Schema.parse("""
                {
                  "useDestinationSchema": true,
                  "fields": [ { "name": "stringField", "type": "string" } ]
                }
                """);
        Assertions.assertFalse(withFields.isDestinationReference());
    }

    @Test
    public void testMixingOldAndNewKeysThrows() {
        final IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class, () ->
                Schema.parse("""
                        {
                          "useDestinationSchema": true,
                          "reference": { "destination": true },
                          "fields": [ { "name": "stringField", "type": "string" } ]
                        }
                        """));
        Assertions.assertTrue(e.getMessage().contains("must not mix"),
                "unexpected message: " + e.getMessage());
    }

    @Test
    public void testValidationErrors() {
        // unknown format
        final IllegalArgumentException e1 = Assertions.assertThrows(IllegalArgumentException.class, () ->
                Schema.parse("""
                        { "encoding": { "format": "msgpack" },
                          "fields": [ { "name": "f", "type": "string" } ] }
                        """));
        Assertions.assertTrue(e1.getMessage().contains("is not supported"),
                "unexpected message: " + e1.getMessage());

        // format missing
        final IllegalArgumentException e2 = Assertions.assertThrows(IllegalArgumentException.class, () ->
                Schema.parse("""
                        { "encoding": {},
                          "fields": [ { "name": "f", "type": "string" } ] }
                        """));
        Assertions.assertTrue(e2.getMessage().contains("schema.encoding.format is required"),
                "unexpected message: " + e2.getMessage());

        // reference document without encoding
        final IllegalArgumentException e3 = Assertions.assertThrows(IllegalArgumentException.class, () ->
                Schema.parse("""
                        { "reference": { "uri": "gs://example-bucket/schema/test.avsc" },
                          "fields": [ { "name": "f", "type": "string" } ] }
                        """));
        Assertions.assertTrue(e3.getMessage().contains("requires schema.encoding.format"),
                "unexpected message: " + e3.getMessage());

        // protobuf requires messageName and uri
        final IllegalArgumentException e4 = Assertions.assertThrows(IllegalArgumentException.class, () ->
                Schema.parse("""
                        { "encoding": { "format": "protobuf" },
                          "fields": [ { "name": "f", "type": "string" } ] }
                        """));
        Assertions.assertTrue(e4.getMessage().contains("schema.encoding.messageName is required"),
                "unexpected message: " + e4.getMessage());
        Assertions.assertTrue(e4.getMessage().contains("requires schema.reference.uri"),
                "unexpected message: " + e4.getMessage());

        // uri and inline are mutually exclusive
        final JsonObject reference = new JsonObject();
        reference.add("uri", new JsonPrimitive("gs://example-bucket/schema/test.avsc"));
        reference.add("inline", new JsonPrimitive(AVRO_JSON));
        final JsonObject encoding = new JsonObject();
        encoding.add("format", new JsonPrimitive("avro"));
        final JsonObject config = new JsonObject();
        config.add("encoding", encoding);
        config.add("reference", reference);
        final IllegalArgumentException e5 = Assertions.assertThrows(IllegalArgumentException.class, () ->
                Schema.parse(config));
        Assertions.assertTrue(e5.getMessage().contains("must not have both uri and inline"),
                "unexpected message: " + e5.getMessage());

        // avro encoding with no definition source at all
        final IllegalArgumentException e6 = Assertions.assertThrows(IllegalArgumentException.class, () ->
                Schema.parse("""
                        { "encoding": { "format": "avro" } }
                        """));
        Assertions.assertTrue(e6.getMessage().contains("requires schema.reference or schema.fields"),
                "unexpected message: " + e6.getMessage());
    }

    @Test
    public void testCopyPreservesDeclaration() {
        final Schema schema = Schema.parse(avroInlineConfig());
        final Schema copied = schema.copy();
        Assertions.assertEquals(Schema.Encoding.Format.avro, copied.getEncoding().getFormat());
        Assertions.assertNotNull(copied.getReference().getInline());
    }

}
