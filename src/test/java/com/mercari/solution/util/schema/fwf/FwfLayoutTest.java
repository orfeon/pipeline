package com.mercari.solution.util.schema.fwf;

import com.mercari.solution.module.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/**
 * Layout document parsing, position resolution, validation and schema derivation
 * (work_fixedwidth.md §4.2 / §4.3 / §8).
 */
public class FwfLayoutTest {

    @Test
    public void testPositions() {
        final FwfLayout layout = FwfLayout.parse("""
                { "recordLength": 40, "fields": [
                    { "name": "a", "type": "string", "len": 2 },
                    { "name": "b", "type": "int32", "len": 3, "repeat": 2 },
                    { "name": "c", "pos": 12, "repeat": 3, "fields": [
                        { "name": "x", "type": "string", "len": 2 },
                        { "name": "y", "type": "string", "pos": 4, "len": 3 } ] },
                    { "name": "d", "type": "string", "len": 1 },
                    { "name": "e", "pos": 35, "size": 6, "fields": [
                        { "name": "z", "type": "string", "len": 1 } ] } ] }
                """);
        final List<FwfLayout.Field> fields = layout.getFields();
        Assertions.assertEquals(40, layout.getRecordLength());

        // a: 1-2, b: 3-8 (2 x 3), c: pos 12, element = x(1-2) gap y(4-6) -> size 6, 3 x 6 = 18 -> 12-29
        Assertions.assertEquals(0, fields.get(0).getStart());
        Assertions.assertEquals(2, fields.get(1).getStart());
        Assertions.assertEquals(6, fields.get(1).extent());
        Assertions.assertEquals(11, fields.get(2).getStart());
        Assertions.assertEquals(6, fields.get(2).getSize());
        Assertions.assertEquals(3, fields.get(2).getChildren().get(1).getStart());
        Assertions.assertEquals(18, fields.get(2).extent());
        // d follows c without pos
        Assertions.assertEquals(29, fields.get(3).getStart());
        // explicit size larger than the children
        Assertions.assertEquals(6, fields.get(4).getSize());
    }

    @Test
    public void testSchemaFields() {
        final FwfLayout layout = FwfLayout.parse("""
                { "fields": [
                    { "name": "code",   "type": "string",  "len": 2, "description": "a code" },
                    { "name": "amount", "type": "decimal", "len": 5, "mode": "required" },
                    { "name": "marks",  "type": "int32",   "len": 2, "repeat": 3 },
                    { "name": "key", "fields": [ { "name": "k", "len": 1 } ] },
                    { "name": "items", "repeat": 2, "options": { "unit": "x" }, "fields": [
                        { "name": "sku", "type": "string", "len": 4 },
                        { "name": "dates", "type": "date", "len": 8, "repeat": 2 } ] } ] }
                """);
        final List<Schema.Field> fields = layout.toSchemaFields();
        Assertions.assertEquals(5, fields.size());

        Assertions.assertEquals(Schema.Type.string, fields.get(0).getFieldType().getType());
        Assertions.assertEquals("a code", fields.get(0).getDescription());

        Assertions.assertEquals(Schema.Type.decimal, fields.get(1).getFieldType().getType());
        Assertions.assertFalse(fields.get(1).getFieldType().getNullable());

        Assertions.assertEquals(Schema.Type.array, fields.get(2).getFieldType().getType());
        Assertions.assertEquals(Schema.Type.int32, fields.get(2).getFieldType().getArrayValueType().getType());

        // a leaf without type is a string
        final Schema.FieldType key = fields.get(3).getFieldType();
        Assertions.assertEquals(Schema.Type.element, key.getType());
        Assertions.assertEquals(Schema.Type.string, key.getElementSchema().getField("k").getFieldType().getType());

        final Schema.FieldType items = fields.get(4).getFieldType();
        Assertions.assertEquals(Schema.Type.array, items.getType());
        Assertions.assertEquals(Schema.Type.element, items.getArrayValueType().getType());
        final Schema.FieldType dates = items.getArrayValueType().getElementSchema().getField("dates").getFieldType();
        Assertions.assertEquals(Schema.Type.array, dates.getType());
        Assertions.assertEquals(Schema.Type.date, dates.getArrayValueType().getType());
        Assertions.assertEquals(Map.of("unit", "x"), fields.get(4).getOptions());
    }

    @Test
    public void testProject() {
        final FwfLayout layout = FwfLayout.parse("""
                [ { "name": "a", "len": 1 }, { "name": "b", "len": 1 }, { "name": "c", "len": 1 } ]
                """);
        final FwfLayout projected = layout.project(List.of("c", "a"));
        Assertions.assertEquals(List.of("c", "a"), projected.getFields().stream().map(FwfLayout.Field::getName).toList());
        // positions are kept
        Assertions.assertEquals(2, projected.getFields().get(0).getStart());

        final IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> layout.project(List.of("a", "x")));
        Assertions.assertTrue(e.getMessage().contains("[x]"), e.getMessage());
    }

    @Test
    public void testYaml() {
        final FwfLayout layout = FwfLayout.parse("""
                recordLength: 4
                fields:
                  - { name: a, type: string, len: 2 }
                  - { name: b, type: int32, len: 2, defaultValue: 0 }
                """);
        Assertions.assertEquals(4, layout.getRecordLength());
        Assertions.assertEquals(0, layout.getFields().get(1).getDefaultValue());
    }

    @Test
    public void testValidationErrors() {
        assertError("""
                { "fields": [ { "name": "a", "type": "string", "offset": 1, "len": 2 } ] }
                """, "offset is not supported (use 'pos')");
        assertError("""
                { "fields": [ { "name": "a", "type": "string" } ] }
                """, ".len is required");
        assertError("""
                { "fields": [ { "name": "a", "len": 2 }, { "name": "a", "len": 2 } ] }
                """, "duplicated field name: a");
        assertError("""
                { "recordLength": 3, "fields": [ { "name": "a", "len": 2 }, { "name": "b", "len": 2 } ] }
                """, "exceeds recordLength 3");
        assertError("""
                { "fields": [ { "name": "a", "type": "string", "len": 2, "scale": 1 } ] }
                """, "scale is only allowed on decimal");
        assertError("""
                { "fields": [ { "name": "g", "size": 2, "fields": [ { "name": "x", "len": 3 } ] } ] }
                """, "size 2 is smaller than the extent of its fields 3");
        assertError("""
                { "fields": [ { "name": "g", "len": 3, "fields": [ { "name": "x", "len": 3 } ] } ] }
                """, "len is not allowed on a group field");
        assertError("""
                { "fields": [ { "name": "a", "type": "map", "len": 3 } ] }
                """, "is not supported by fwf");
        assertError("""
                { "fields": [ { "name": "a", "type": "int32", "len": 3, "defaultValue": "x" } ] }
                """, "defaultValue: x can not be converted");
        assertError("""
                { "fields": [ { "name": "a", "len": 3, "mode": "repeated" } ] }
                """, "use repeat instead");
        assertError("""
                { "fields": [ { "name": "a", "type": "int32", "len": 0 } ] }
                """, "len must be a positive integer");
        assertError("""
                { "fields": [ { "name": "a", "type": "date", "len": 8, "pattern": "yyyyMMdd", "zone": "Asia/Tokyo" } ] }
                """, "zone is only allowed on timestamp");
        assertError("""
                { "recordLen": 3, "fields": [ { "name": "a", "len": 2 } ] }
                """, "layout.recordLen is not supported");
        assertError("""
                { "fields": [] }
                """, "layout.fields must not be empty");
    }

    private static void assertError(final String layout, final String expected) {
        final IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class, () -> FwfLayout.parse(layout));
        Assertions.assertTrue(e.getMessage().contains(expected), "expected '" + expected + "' in: " + e.getMessage());
    }

    @Test
    public void testOptions() {
        final FwfOptions defaults = FwfOptions.of(Map.of());
        Assertions.assertEquals("UTF-8", defaults.getCharsetName());
        Assertions.assertEquals(FwfOptions.Unit.byte_, defaults.getUnit());
        Assertions.assertEquals(FwfOptions.OnParseError.fail, defaults.getOnParseError());

        final FwfOptions options = FwfOptions.of(Map.of(
                "charset", "windows-31j", "unit", "char", "trim", "right",
                "emptyAsNull", "false", "onLengthMismatch", "pad", "onParseError", "null"));
        Assertions.assertEquals("windows-31j", options.getCharsetName());
        Assertions.assertEquals(FwfOptions.Unit.char_, options.getUnit());
        Assertions.assertEquals(FwfOptions.Trim.right, options.getTrim());
        Assertions.assertFalse(options.isEmptyAsNull());
        Assertions.assertEquals(FwfOptions.OnLengthMismatch.pad, options.getOnLengthMismatch());
        Assertions.assertEquals(FwfOptions.OnParseError.nullify, options.getOnParseError());

        final IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> FwfOptions.of(Map.of("charsett", "x", "unit", "word")));
        Assertions.assertTrue(e.getMessage().contains("charsett is not supported"), e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("unit must be byte or char"), e.getMessage());
        Assertions.assertThrows(IllegalArgumentException.class, () -> FwfOptions.of(Map.of("charset", "no-such-charset")));
    }

}
