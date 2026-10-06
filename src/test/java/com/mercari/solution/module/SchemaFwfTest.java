package com.mercari.solution.module;

import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.pipeline.Serialize;
import com.mercari.solution.util.schema.fwf.FwfDecoder;
import com.mercari.solution.util.schema.fwf.FwfOptions;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.SerializableUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * {@code schema.encoding.format: fwf} with the layout document in {@code schema.reference}
 * (work_fixedwidth.md §3.2 / §4.1 / §4.2).
 */
public class SchemaFwfTest {

    private static final String LAYOUT = """
            { "recordLength": 14, "fields": [
                { "name": "code",   "type": "string",  "len": 4, "description": "a code" },
                { "name": "amount", "type": "decimal", "len": 4, "scale": 1 },
                { "name": "marks",  "type": "int32",   "len": 1, "repeat": 2 },
                { "name": "item", "fields": [
                    { "name": "sku", "type": "string", "len": 2 },
                    { "name": "qty", "type": "int32",  "len": 2 } ] } ] }
            """;

    @Test
    public void testInlineObject() {
        final Schema schema = Schema.parse("""
                {
                  "encoding": { "format": "fwf", "charset": "windows-31j", "trim": "right" },
                  "reference": { "inline": %s }
                }
                """.formatted(LAYOUT));

        Assertions.assertEquals(Schema.Encoding.Format.fwf, schema.getEncoding().getFormat());
        Assertions.assertEquals(Map.of("charset", "windows-31j", "trim", "right"), schema.getEncoding().getOptions());
        Assertions.assertEquals(List.of("code", "amount", "marks", "item"),
                schema.getFields().stream().map(Schema.Field::getName).toList());
        Assertions.assertEquals("a code", schema.getField("code").getDescription());
        Assertions.assertEquals(Schema.Type.array, schema.getField("marks").getFieldType().getType());
        Assertions.assertEquals(14, schema.getFwfLayout().getRecordLength());

        final FwfOptions options = schema.getFwfOptions();
        Assertions.assertEquals("windows-31j", options.getCharsetName());
        Assertions.assertEquals(FwfOptions.Trim.right, options.getTrim());
    }

    @Test
    public void testInlineString() {
        // the inline document as a JSON string also works (same as avro)
        final Schema schema = Schema.parse("""
                { "encoding": { "format": "fwf" }, "reference": { "inline": %s } }
                """.formatted(new com.google.gson.JsonPrimitive(LAYOUT)));
        Assertions.assertEquals(4, schema.getFields().size());
        Assertions.assertEquals("UTF-8", schema.getFwfOptions().getCharsetName());
    }

    @Test
    public void testUri() throws Exception {
        final Path dir = Path.of("target", "fwf-schema-test");
        Files.createDirectories(dir);
        final Path file = dir.resolve("layout.fwf.json");
        Files.writeString(file, LAYOUT, StandardCharsets.UTF_8);

        final Schema schema = Schema.parse("""
                { "encoding": { "format": "fwf" }, "reference": { "uri": %s } }
                """.formatted(new com.google.gson.JsonPrimitive(file.toAbsolutePath().toString())));
        Assertions.assertEquals(file.toAbsolutePath().toString(), schema.getFwf().getFile());
        Assertions.assertEquals(4, schema.getFields().size());
    }

    @Test
    public void testDeclaredFieldsErrors() {
        final IllegalArgumentException unknown = Assertions.assertThrows(IllegalArgumentException.class, () -> Schema.parse("""
                {
                  "encoding": { "format": "fwf" },
                  "reference": { "inline": %s },
                  "fields": [ { "name": "nothing", "type": "string" } ]
                }
                """.formatted(LAYOUT)));
        Assertions.assertTrue(unknown.getMessage().contains("[nothing]"), unknown.getMessage());

        final IllegalArgumentException incompatible = Assertions.assertThrows(IllegalArgumentException.class, () -> Schema.parse("""
                {
                  "encoding": { "format": "fwf" },
                  "reference": { "inline": %s },
                  "fields": [ { "name": "code", "type": "int64" } ]
                }
                """.formatted(LAYOUT)));
        Assertions.assertTrue(incompatible.getMessage().contains("incompatible with the fwf layout type string"), incompatible.getMessage());

        final Schema projected = Schema.parse("""
                {
                  "encoding": { "format": "fwf" },
                  "reference": { "inline": %s },
                  "fields": [ { "name": "code", "type": "string" } ]
                }
                """.formatted(LAYOUT));
        Assertions.assertEquals(List.of("code"), projected.getFields().stream().map(Schema.Field::getName).toList());
        Assertions.assertEquals(4, projected.getFwfLayout().getFields().size());

        // a duplicated name is reported as such (not as an index error against the deduplicated projection)
        assertParseError("""
                {
                  "encoding": { "format": "fwf" },
                  "reference": { "inline": %s },
                  "fields": [ { "name": "code", "type": "string" }, { "name": "code", "type": "string" } ]
                }
                """.formatted(LAYOUT), "duplicated field name: code");

        // a field can not be made required by the declaration: the layout decides
        assertParseError("""
                {
                  "encoding": { "format": "fwf" },
                  "reference": { "inline": %s },
                  "fields": [ { "name": "code", "type": "string", "mode": "required" } ]
                }
                """.formatted(LAYOUT), "schema.fields[0] code is declared required, but the fwf layout field is nullable");

        // the element type of an array and the fields of a record are compared too
        assertParseError("""
                {
                  "encoding": { "format": "fwf" },
                  "reference": { "inline": %s },
                  "fields": [ { "name": "marks", "type": "string", "mode": "repeated" } ]
                }
                """.formatted(LAYOUT), "schema.fields[0] marks[] type string is incompatible with the fwf layout type int32");
        assertParseError("""
                {
                  "encoding": { "format": "fwf" },
                  "reference": { "inline": %s },
                  "fields": [ { "name": "item", "type": "record", "fields": [ { "name": "qty", "type": "string" } ] } ]
                }
                """.formatted(LAYOUT), "schema.fields[0] item.qty type string is incompatible with the fwf layout type int32");
        assertParseError("""
                {
                  "encoding": { "format": "fwf" },
                  "reference": { "inline": %s },
                  "fields": [ { "name": "item", "type": "record", "fields": [ { "name": "other", "type": "string" } ] } ]
                }
                """.formatted(LAYOUT), "schema.fields[0] item.other is not in the fwf layout");

        final Schema nested = Schema.parse("""
                {
                  "encoding": { "format": "fwf" },
                  "reference": { "inline": %s },
                  "fields": [
                    { "name": "marks", "type": "int32", "mode": "repeated" },
                    { "name": "item", "type": "record", "fields": [ { "name": "qty", "type": "int32" } ] } ]
                }
                """.formatted(LAYOUT));
        Assertions.assertEquals(List.of("marks", "item"), nested.getFields().stream().map(Schema.Field::getName).toList());
    }

    @Test
    public void testDerivedSchemaIsNotAProjection() {
        // only the fields declared in the config are validated against the layout: a schema derived
        // from an fwf schema (a transform output, additional fields of a source) may add fields
        final Schema schema = Schema.parse("""
                { "encoding": { "format": "fwf" }, "reference": { "inline": %s } }
                """.formatted(LAYOUT));
        final Schema derived = Schema.builder(schema)
                .withField("resource", Schema.FieldType.STRING)
                .build();
        Assertions.assertEquals(List.of("code", "amount", "marks", "item", "resource"),
                derived.getFields().stream().map(Schema.Field::getName).toList());
        Assertions.assertEquals(14, derived.getFwfLayout().getRecordLength());
        Assertions.assertEquals(5, derived.copy().getFields().size());
    }

    @Test
    public void testFwfOptionsOfAnotherFormat() {
        // the option keys of another format are not parsed as fwf options
        final Schema schema = Schema.parse("""
                { "encoding": { "format": "avro", "codec": "snappy" }, "fields": [ { "name": "a", "type": "string" } ] }
                """);
        Assertions.assertNull(schema.getFwfLayout());
        Assertions.assertEquals("UTF-8", schema.getFwfOptions().getCharsetName());
    }

    @Test
    public void testDeclarationErrors() {
        assertParseError("""
                { "encoding": { "format": "fwf" } }
                """, "requires schema.reference.uri or schema.reference.inline");
        assertParseError("""
                { "encoding": { "format": "fwf", "charsett": "windows-31j" }, "reference": { "inline": %s } }
                """.formatted(LAYOUT), "charsett is not supported for format fwf");
        assertParseError("""
                { "encoding": { "format": "fwf" }, "reference": { "inline": { "fields": [ { "name": "a", "offset": 1, "len": 1 } ] } } }
                """, "(use 'pos')");
        assertParseError("""
                { "encoding": { "format": "avro" }, "reference": { "inline": { "type": "record" } } }
                """, "schema.reference.inline must be a string. but");
        assertParseError("""
                { "encoding": { "format": "fwf", "trim": { "x": 1 } }, "reference": { "inline": %s } }
                """.formatted(LAYOUT), "schema.encoding.trim must be a primitive value");
    }

    private static void assertParseError(final String json, final String expected) {
        final IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class, () -> Schema.parse(json));
        Assertions.assertTrue(e.getMessage().contains(expected), "expected '" + expected + "' in: " + e.getMessage());
    }

    @Test
    public void testCopyAndSerialize() {
        final Schema schema = Schema.parse("""
                { "encoding": { "format": "fwf" }, "reference": { "inline": %s } }
                """.formatted(LAYOUT));
        Assertions.assertEquals(14, schema.copy().getFwfLayout().getRecordLength());
        Assertions.assertEquals(14, Schema.builder(schema).build().getFwfLayout().getRecordLength());
        final Schema deserialized = SerializableUtils.ensureSerializable(schema);
        Assertions.assertEquals(14, deserialized.getFwfLayout().getRecordLength());
        Assertions.assertEquals(Schema.Encoding.Format.fwf, deserialized.getEncoding().getFormat());
    }

    @Test
    public void testMessageModulesRejectFwf() {
        final Schema schema = Schema.parse("""
                { "encoding": { "format": "fwf" }, "reference": { "inline": %s } }
                """.formatted(LAYOUT));
        final IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> Serialize.resolveFormat(null, schema));
        Assertions.assertTrue(e.getMessage().contains("fwf is not supported by this module"), e.getMessage());
    }

    @Test
    public void testDecodedValuesRoundTripElementCoder() throws Exception {
        final Schema schema = Schema.parse("""
                { "encoding": { "format": "fwf", "charset": "windows-31j" }, "reference": { "inline": %s } }
                """.formatted(LAYOUT));
        final FwfDecoder decoder = FwfDecoder.of(schema.getFwfLayout(), schema.getFwfOptions());
        final byte[] record = ("ＡＢ" + "0123" + "1 " + "x105").getBytes(Charset.forName("windows-31j"));
        final Map<String, Object> values = decoder.decode(record);
        Assertions.assertEquals("ＡＢ", values.get("code"));
        Assertions.assertEquals(new BigDecimal("12.3"), values.get("amount"));
        Assertions.assertEquals(Arrays.asList(1, null), values.get("marks"));

        final MElement element = MElement.of(values, 0L);
        final ElementCoder coder = ElementCoder.of(schema);
        final MElement decoded = CoderUtils.clone(coder, element);
        // UnionMapCoder carries a BigDecimal as its string form (same as the csv / json converters);
        // MElement accessors read it back
        Assertions.assertEquals(0, new BigDecimal("12.3").compareTo(decoded.getAsBigDecimal("amount")));
        final Map<String, Object> expected = new java.util.HashMap<>(values);
        expected.remove("amount");
        final Map<String, Object> actual = new java.util.HashMap<>((Map<String, Object>) decoded.getValue());
        actual.remove("amount");
        Assertions.assertEquals(expected, actual);
    }

}
