package com.mercari.solution.module;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.protobuf.Descriptors;
import com.mercari.solution.util.ResourceUtil;
import com.mercari.solution.util.schema.ProtoSchemaUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/**
 * Characterization tests for the current {@code Schema.parse} / {@code setup} behavior.
 *
 * These pin the pre-redesign semantics of the schema config block (fields / avro / protobuf /
 * useDestinationSchema and the deprecated aliases) so that the internal restructuring described in
 * docs/developer/schema-redesign.md (Phase 1) can be verified not to change observable behavior.
 * When a behavior is changed intentionally in a later phase, update the corresponding test and
 * reference the design document section that mandates the change.
 */
public class SchemaParseTest {

    private static final String AVRO_JSON = """
            {
              "type": "record",
              "name": "root",
              "fields": [
                { "name": "stringField", "type": ["null", "string"] },
                { "name": "longField", "type": "long" },
                { "name": "avroOnlyField", "type": "double" }
              ]
            }
            """;

    @Test
    public void testParseFieldsTypesAndModes() {
        final Schema schema = Schema.parse("""
                {
                  "fields": [
                    { "name": "stringField", "type": "string" },
                    { "name": "requiredIntField", "type": "int", "mode": "required" },
                    { "name": "longField", "type": "long" },
                    { "name": "boolField", "type": "boolean" },
                    { "name": "repeatedField", "type": "string", "mode": "repeated" },
                    { "name": "enumField", "type": "enum", "symbols": ["red", "blue"] },
                    { "name": "elementField", "type": "element", "fields": [
                      { "name": "nestedField", "type": "string" }
                    ] }
                  ]
                }
                """);

        Assertions.assertNotNull(schema);
        final List<Schema.Field> fields = schema.getFields();
        Assertions.assertEquals(7, fields.size());

        // type aliases resolve to canonical types
        Assertions.assertEquals(Schema.Type.string, schema.getField("stringField").getFieldType().getType());
        Assertions.assertEquals(Schema.Type.int32, schema.getField("requiredIntField").getFieldType().getType());
        Assertions.assertEquals(Schema.Type.int64, schema.getField("longField").getFieldType().getType());
        Assertions.assertEquals(Schema.Type.bool, schema.getField("boolField").getFieldType().getType());

        // mode: nullable is the default, required sets nullable=false
        Assertions.assertNotEquals(Boolean.FALSE, schema.getField("stringField").getFieldType().getNullable());
        Assertions.assertEquals(Boolean.FALSE, schema.getField("requiredIntField").getFieldType().getNullable());

        // mode: repeated becomes an array type wrapping the declared type
        final Schema.FieldType repeated = schema.getField("repeatedField").getFieldType();
        Assertions.assertEquals(Schema.Type.array, repeated.getType());
        Assertions.assertEquals(Schema.Type.string, repeated.getArrayValueType().getType());

        // enum keeps symbols
        Assertions.assertEquals(List.of("red", "blue"),
                schema.getField("enumField").getFieldType().getSymbols());

        // nested element keeps its own fields
        final Schema.FieldType element = schema.getField("elementField").getFieldType();
        Assertions.assertEquals(Schema.Type.element, element.getType());
        Assertions.assertTrue(element.getElementSchema().hasField("nestedField"));
    }

    @Test
    public void testParseFieldsDerivesAvroAndRowSchemas() {
        final Schema schema = Schema.parse("""
                {
                  "fields": [
                    { "name": "stringField", "type": "string" },
                    { "name": "longField", "type": "long" }
                  ]
                }
                """);

        // avro/row representations are derived on demand from fields
        final org.apache.avro.Schema avroSchema = schema.getAvroSchema();
        Assertions.assertEquals(org.apache.avro.Schema.Type.RECORD, avroSchema.getType());
        Assertions.assertNotNull(avroSchema.getField("stringField"));
        Assertions.assertNotNull(avroSchema.getField("longField"));

        final org.apache.beam.sdk.schemas.Schema rowSchema = schema.getRowSchema();
        Assertions.assertTrue(rowSchema.hasField("stringField"));
        Assertions.assertTrue(rowSchema.hasField("longField"));

        Assertions.assertNull(schema.getUseDestinationSchema());
        Assertions.assertNull(schema.getProtobuf());
    }

    @Test
    public void testParseAvroJsonOnlyDerivesFields() {
        final JsonObject avro = new JsonObject();
        avro.add("json", new JsonPrimitive(AVRO_JSON));
        final JsonObject config = new JsonObject();
        config.add("avro", avro);

        final Schema schema = Schema.parse(config);

        Assertions.assertNotNull(schema);
        // fields are derived from the avro definition at build time
        Assertions.assertTrue(schema.hasField("stringField"));
        Assertions.assertTrue(schema.hasField("longField"));
        Assertions.assertTrue(schema.hasField("avroOnlyField"));
        Assertions.assertEquals(Schema.Type.float64,
                schema.getField("avroOnlyField").getFieldType().getType());

        // the declared avro json is preserved
        Assertions.assertNotNull(schema.getAvro().getJson());
        Assertions.assertEquals("root", schema.getAvroSchema().getName());
    }

    @Test
    public void testParseFieldsAndAvroMixedPrecedence() {
        // When both are present: fields win for the logical field list,
        // the declared avro schema wins for getAvroSchema (it is NOT re-derived from fields).
        final JsonObject avro = new JsonObject();
        avro.add("json", new JsonPrimitive(AVRO_JSON));
        final JsonObject config = new JsonObject();
        config.add("avro", avro);
        final JsonObject field = new JsonObject();
        field.add("name", new JsonPrimitive("stringField"));
        field.add("type", new JsonPrimitive("string"));
        final com.google.gson.JsonArray fields = new com.google.gson.JsonArray();
        fields.add(field);
        config.add("fields", fields);

        final Schema schema = Schema.parse(config);

        Assertions.assertEquals(1, schema.getFields().size());
        Assertions.assertTrue(schema.hasField("stringField"));
        Assertions.assertFalse(schema.hasField("avroOnlyField"));

        final org.apache.avro.Schema avroSchema = schema.getAvroSchema();
        Assertions.assertNotNull(avroSchema.getField("avroOnlyField"));
    }

    @Test
    public void testParseUseDestinationSchema() {
        final Schema schema = Schema.parse("""
                {
                  "fields": [ { "name": "stringField", "type": "string" } ],
                  "useDestinationSchema": true
                }
                """);
        Assertions.assertEquals(Boolean.TRUE, schema.getUseDestinationSchema());
    }

    @Test
    public void testParseProtobufKeepsConfigWithoutIO() {
        // With fields present, the protobuf block is attached as-is and no descriptor is loaded at parse time.
        final Schema schema = Schema.parse("""
                {
                  "fields": [ { "name": "stringField", "type": "string" } ],
                  "protobuf": {
                    "descriptorFile": "gs://example-bucket/schema/test.desc",
                    "messageName": "com.mercari.solution.entity.TestMessage"
                  }
                }
                """);
        Assertions.assertNotNull(schema.getProtobuf());
        Assertions.assertEquals("gs://example-bucket/schema/test.desc", schema.getProtobuf().getDescriptorFile());
        Assertions.assertEquals("com.mercari.solution.entity.TestMessage", schema.getProtobuf().getMessageName());
    }

    @Test
    public void testParseProtobufOnlyWithoutDescriptorFileThrows() {
        // Without fields, build() eagerly resolves the descriptor; a missing descriptorFile fails.
        final IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class, () ->
                Schema.parse("""
                        {
                          "protobuf": { "messageName": "com.mercari.solution.entity.TestMessage" }
                        }
                        """));
        Assertions.assertTrue(e.getMessage().contains("is not found in descriptors"),
                "unexpected message: " + e.getMessage());
    }

    @Test
    public void testParseDeprecatedAvroSchemaAliasWithFieldsIsShadowed() {
        // Known quirk (pinned deliberately): when fields are present alongside a file-based avro
        // definition (deprecated "avroSchema" alias, or "avro": {"file": ...}), getAvro() replaces
        // the declared avro (json == null) with one derived from fields — the file reference is
        // silently discarded and never read. See schema-redesign.md §1 problem 3.
        final Schema schema = Schema.parse("""
                {
                  "fields": [ { "name": "stringField", "type": "string" } ],
                  "avroSchema": "gs://example-bucket/schema/test.avsc"
                }
                """);
        final Schema.AvroSchema avro = schema.getAvro();
        Assertions.assertNotNull(avro);
        Assertions.assertNull(avro.getFile());
        Assertions.assertNotNull(avro.getSchema().getField("stringField"));
    }

    @Test
    public void testParseDeprecatedProtobufDescriptorAliasKeepsFile() {
        final Schema schema = Schema.parse("""
                {
                  "fields": [ { "name": "stringField", "type": "string" } ],
                  "protobufDescriptor": "gs://example-bucket/schema/test.desc"
                }
                """);
        Assertions.assertNotNull(schema.getProtobuf());
        Assertions.assertEquals("gs://example-bucket/schema/test.desc", schema.getProtobuf().getDescriptorFile());
    }

    @Test
    public void testParseNullOrNonObjectReturnsNull() {
        Assertions.assertNull(Schema.parse((com.google.gson.JsonElement) null));
        Assertions.assertNull(Schema.parse(com.google.gson.JsonNull.INSTANCE));
        Assertions.assertNull(Schema.parse(new JsonPrimitive("text")));
    }

    @Test
    public void testParseEmptyObjectThrows() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> Schema.parse("{}"));
    }

    @Test
    public void testParseInvalidShapesThrow() {
        final IllegalArgumentException e1 = Assertions.assertThrows(IllegalArgumentException.class, () ->
                Schema.parse("""
                        { "fields": "notAnArray" }
                        """));
        Assertions.assertTrue(e1.getMessage().contains("schema.fields must be array"),
                "unexpected message: " + e1.getMessage());

        final IllegalArgumentException e2 = Assertions.assertThrows(IllegalArgumentException.class, () ->
                Schema.parse("""
                        { "avro": "notAnObject" }
                        """));
        Assertions.assertTrue(e2.getMessage().contains("schema.avro must be object"),
                "unexpected message: " + e2.getMessage());

        // a field entry missing name/type is reported as illegal format
        final IllegalArgumentException e3 = Assertions.assertThrows(IllegalArgumentException.class, () ->
                Schema.parse("""
                        { "fields": [ { "name": "missingType" } ] }
                        """));
        Assertions.assertTrue(e3.getMessage().contains("illegal format"),
                "unexpected message: " + e3.getMessage());
    }

    @Test
    public void testParseNormalizesEncodingAndReference() {
        // Phase 1 (schema-redesign.md §3): old-format keys are normalized into the internal
        // Encoding / Reference declaration model at build time.

        // fields-only: no encoding, no reference
        final Schema fieldsOnly = Schema.parse("""
                { "fields": [ { "name": "stringField", "type": "string" } ] }
                """);
        Assertions.assertNull(fieldsOnly.getEncoding());
        Assertions.assertNull(fieldsOnly.getReference());

        // avro inline json -> encoding avro + inline reference
        final JsonObject avro = new JsonObject();
        avro.add("json", new JsonPrimitive(AVRO_JSON));
        final JsonObject avroConfig = new JsonObject();
        avroConfig.add("avro", avro);
        final Schema avroInline = Schema.parse(avroConfig);
        Assertions.assertEquals(Schema.Encoding.Format.avro, avroInline.getEncoding().getFormat());
        Assertions.assertNotNull(avroInline.getReference().getInline());
        Assertions.assertNull(avroInline.getReference().getUri());

        // protobuf -> encoding protobuf with messageName + uri reference
        final Schema protobuf = Schema.parse("""
                {
                  "fields": [ { "name": "stringField", "type": "string" } ],
                  "protobuf": {
                    "descriptorFile": "gs://example-bucket/schema/test.desc",
                    "messageName": "com.mercari.solution.entity.TestMessage"
                  }
                }
                """);
        Assertions.assertEquals(Schema.Encoding.Format.protobuf, protobuf.getEncoding().getFormat());
        Assertions.assertEquals("com.mercari.solution.entity.TestMessage", protobuf.getEncoding().getMessageName());
        Assertions.assertEquals("gs://example-bucket/schema/test.desc", protobuf.getReference().getUri());

        // useDestinationSchema -> destination reference
        final Schema destination = Schema.parse("""
                {
                  "fields": [ { "name": "stringField", "type": "string" } ],
                  "useDestinationSchema": true
                }
                """);
        Assertions.assertNull(destination.getEncoding());
        Assertions.assertEquals(Boolean.TRUE, destination.getReference().getDestination());

        // deprecated avroSchema alias with fields: the runtime getter shadows the file
        // (see testParseDeprecatedAvroSchemaAliasWithFieldsIsShadowed), but the normalized
        // declaration preserves what the config actually said
        final Schema alias = Schema.parse("""
                {
                  "fields": [ { "name": "stringField", "type": "string" } ],
                  "avroSchema": "gs://example-bucket/schema/test.avsc"
                }
                """);
        Assertions.assertEquals(Schema.Encoding.Format.avro, alias.getEncoding().getFormat());
        Assertions.assertEquals("gs://example-bucket/schema/test.avsc", alias.getReference().getUri());
    }

    @Test
    public void testSchemaOfProtobufDescriptorDerivesFields() {
        // setup() derives fields from a protobuf descriptor; exercised via the local test descriptor
        // (the descriptorFile path itself is GCS-only in the current implementation).
        final byte[] descBytes = ResourceUtil.getResourceFileAsBytes("schema/test.desc");
        final Map<String, Descriptors.Descriptor> descriptors = ProtoSchemaUtil.getDescriptors(descBytes);
        final Descriptors.Descriptor descriptor = descriptors.get("com.mercari.solution.entity.TestMessage");
        Assertions.assertNotNull(descriptor);

        final Schema schema = Schema.of(descriptor);
        Assertions.assertTrue(schema.hasField("stringValue"));
        Assertions.assertTrue(schema.hasField("longValue"));
        Assertions.assertEquals(Schema.Type.bool, schema.getField("boolValue").getFieldType().getType());
    }

}
