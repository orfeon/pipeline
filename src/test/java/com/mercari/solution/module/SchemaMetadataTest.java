package com.mercari.solution.module;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * Field-level metadata ({@code description} / {@code options}) on {@link Schema.Field}:
 * config parsing, JSON output (the dry-run {@code spec.modules[].schema} shape) and round-trip.
 */
public class SchemaMetadataTest {

    @Test
    public void testParseDescriptionAndOptions() {
        final Schema schema = Schema.parse("""
                {
                  "fields": [
                    { "name": "id", "type": "long", "mode": "required", "description": "member id" },
                    { "name": "name", "type": "string", "options": { "source": "users.name", "sensitivity": "pii" } },
                    { "name": "plain", "type": "string" },
                    { "name": "nested", "type": "element", "fields": [
                      { "name": "child", "type": "string", "description": "child doc" }
                    ] }
                  ]
                }
                """);

        Assertions.assertEquals("member id", schema.getField("id").getDescription());
        Assertions.assertNull(schema.getField("plain").getDescription());
        Assertions.assertTrue(schema.getField("plain").getOptions().isEmpty());
        Assertions.assertEquals(Map.of("source", "users.name", "sensitivity", "pii"), schema.getField("name").getOptions());
        Assertions.assertEquals("child doc",
                schema.getField("nested").getFieldType().getElementSchema().getField("child").getDescription());
    }

    @Test
    public void testToJsonObjectEmitsMetadataOnlyWhenPresent() {
        final Schema schema = Schema.parse("""
                {
                  "fields": [
                    { "name": "id", "type": "long", "description": "member id", "options": { "primaryKey": "true" } },
                    { "name": "plain", "type": "string" }
                  ]
                }
                """);

        final JsonObject json = schema.toJsonObject();
        final JsonObject id = json.getAsJsonArray("fields").get(0).getAsJsonObject();
        final JsonObject plain = json.getAsJsonArray("fields").get(1).getAsJsonObject();

        Assertions.assertEquals("member id", id.get("description").getAsString());
        Assertions.assertEquals("true", id.getAsJsonObject("options").get("primaryKey").getAsString());
        Assertions.assertFalse(plain.has("description"));
        Assertions.assertFalse(plain.has("options"));
    }

    @Test
    public void testRoundTrip() {
        final Schema schema = Schema.parse("""
                {
                  "fields": [
                    { "name": "id", "type": "long", "description": "member id", "options": { "k": "v" } },
                    { "name": "nested", "type": "element", "fields": [
                      { "name": "child", "type": "string", "description": "child doc" }
                    ] }
                  ]
                }
                """);

        final JsonObject config = new JsonObject();
        config.add("fields", schema.toJsonObject().getAsJsonArray("fields"));
        final Schema reparsed = Schema.parse(JsonParser.parseString(config.toString()).getAsJsonObject());

        Assertions.assertEquals("member id", reparsed.getField("id").getDescription());
        Assertions.assertEquals(Map.of("k", "v"), reparsed.getField("id").getOptions());
        Assertions.assertEquals("child doc",
                reparsed.getField("nested").getFieldType().getElementSchema().getField("child").getDescription());
    }

    @Test
    public void testSchemaDescription() {
        final Schema schema = Schema.parse(
                "{ \"description\": \"members table\", \"fields\": [ { \"name\": \"id\", \"type\": \"long\" } ] }");
        Assertions.assertEquals("members table", schema.getDescription());
        Assertions.assertEquals("members table", schema.toJsonObject().get("description").getAsString());
        Assertions.assertFalse(Schema.parse("{ \"fields\": [ { \"name\": \"id\", \"type\": \"long\" } ] }")
                .toJsonObject().has("description"));

        // copy() keeps it, but a schema derived through builder(schema) does not claim to be the source table
        Assertions.assertEquals("members table", schema.copy().getDescription());
        Assertions.assertNull(Schema.builder(schema).build().getDescription());

        // avro record doc round trip
        final Schema fromAvro = Schema.of(schema.getAvroSchema());
        Assertions.assertEquals("members table", fromAvro.getDescription());
    }

    @Test
    public void testCopyKeepsMetadata() {
        final Schema schema = Schema.parse("""
                { "fields": [ { "name": "id", "type": "long", "description": "member id", "options": { "k": "v" } } ] }
                """);
        final Schema copied = schema.copy();
        Assertions.assertEquals("member id", copied.getField("id").getDescription());
        Assertions.assertEquals("v", copied.getField("id").getOptions().get("k"));
    }

}
