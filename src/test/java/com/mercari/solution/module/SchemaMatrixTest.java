package com.mercari.solution.module;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.mercari.solution.util.pipeline.Query2;
import com.mercari.solution.util.pipeline.select.SelectFunction;
import com.mercari.solution.util.schema.converter.JsonToElementConverter;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/**
 * Config-declared matrix fields: {@code type: matrix} + {@code shape}
 * (+ optional numeric {@code valueType}, default float64) in a schema
 * declaration parses to the canonical flat row-major matrix FieldType,
 * round-trips through {@code toJsonObject}, ingests flat or nested JSON
 * values ({@code JsonToElementConverter} — the create source / storage JSON
 * path), and feeds the select module's shape-aware matrix functions and the
 * query module's flat-matrix overloads.
 */
public class SchemaMatrixTest {

    private static final Instant TIMESTAMP = Instant.parse("2025-05-01T00:00:00Z");
    private static final double DELTA = 1e-9;

    private static Schema.Field parseField(final String json) {
        return Schema.Field.parse(new Gson().fromJson(json, JsonObject.class));
    }

    @Test
    public void testParseMatrixField() {
        final Schema.Field field = parseField("""
                { "name": "mat", "type": "matrix", "shape": [2, 3] }
                """);
        Assertions.assertEquals("mat", field.getName());
        Assertions.assertEquals(Schema.Type.matrix, field.getFieldType().getType());
        Assertions.assertEquals(List.of(2, 3), field.getFieldType().getShape());
        // valueType defaults to float64.
        Assertions.assertEquals(Schema.Type.float64, field.getFieldType().getMatrixValueType().getType());
        Assertions.assertTrue(field.getFieldType().getNullable());

        final Schema.Field typed = parseField("""
                { "name": "mat", "type": "matrix", "valueType": "float32", "shape": [4] }
                """);
        Assertions.assertEquals(Schema.Type.float32, typed.getFieldType().getMatrixValueType().getType());
        Assertions.assertEquals(List.of(4), typed.getFieldType().getShape());

        final Schema.Field required = parseField("""
                { "name": "mat", "type": "matrix", "shape": [2, 2], "mode": "required" }
                """);
        Assertions.assertFalse(required.getFieldType().getNullable());
    }

    @Test
    public void testParseMatrixFieldErrors() {
        // shape is required.
        Assertions.assertThrows(IllegalArgumentException.class, () -> parseField("""
                { "name": "mat", "type": "matrix" }
                """));
        Assertions.assertThrows(IllegalArgumentException.class, () -> parseField("""
                { "name": "mat", "type": "matrix", "shape": [] }
                """));
        Assertions.assertThrows(IllegalArgumentException.class, () -> parseField("""
                { "name": "mat", "type": "matrix", "shape": [2, 0] }
                """));
        Assertions.assertThrows(IllegalArgumentException.class, () -> parseField("""
                { "name": "mat", "type": "matrix", "shape": [2, "x"] }
                """));
        // valueType must be numeric.
        Assertions.assertThrows(IllegalArgumentException.class, () -> parseField("""
                { "name": "mat", "type": "matrix", "valueType": "string", "shape": [2, 2] }
                """));
    }

    @Test
    public void testRoundTripThroughToJsonObject() {
        final Schema.Field field = parseField("""
                { "name": "mat", "type": "matrix", "valueType": "float32", "shape": [2, 2] }
                """);
        // toJsonObject echoes matrix fields with mode=repeated; parse must accept it.
        final JsonObject echoed = field.toJsonObject();
        final Schema.Field reparsed = Schema.Field.parse(echoed);
        Assertions.assertEquals(Schema.Type.matrix, reparsed.getFieldType().getType());
        Assertions.assertEquals(List.of(2, 2), reparsed.getFieldType().getShape());
        Assertions.assertEquals(Schema.Type.float32, reparsed.getFieldType().getMatrixValueType().getType());
    }

    private static List<Schema.Field> matrixSchemaFields() {
        return List.of(
                parseField("{ \"name\": \"userId\", \"type\": \"string\" }"),
                parseField("{ \"name\": \"vec\", \"type\": \"float64\", \"mode\": \"repeated\" }"),
                parseField("{ \"name\": \"mat\", \"type\": \"matrix\", \"shape\": [2, 2] }"));
    }

    @Test
    public void testJsonValueConversion() {
        final List<Schema.Field> fields = matrixSchemaFields();
        // Nested rows are flattened row-major; a flat array passes as-is.
        for (final String json : List.of(
                "{ \"userId\": \"u1\", \"vec\": [5, 10], \"mat\": [[2, 1], [1, 3]] }",
                "{ \"userId\": \"u1\", \"vec\": [5, 10], \"mat\": [2, 1, 1, 3] }")) {
            final Map<String, Object> values = JsonToElementConverter.convert(fields, json);
            Assertions.assertEquals(List.of(2d, 1d, 1d, 3d), values.get("mat"));
        }
        // Element count must match the shape.
        Assertions.assertThrows(IllegalStateException.class, () ->
                JsonToElementConverter.convert(fields,
                        "{ \"userId\": \"u1\", \"vec\": [5, 10], \"mat\": [2, 1, 1] }"));
    }

    @Test
    public void testDeclaredMatrixFeedsSelectAndQuery() {
        final List<Schema.Field> fields = matrixSchemaFields();
        final Map<String, Object> values = JsonToElementConverter.convert(fields,
                "{ \"userId\": \"u1\", \"vec\": [5, 10], \"mat\": [[2, 1], [1, 3]] }");

        // Select: the matrix-typed field's schema shape drives the interpretation.
        final List<SelectFunction> selectFunctions = SelectFunction.of(
                new Gson().fromJson("""
                        [{ "name": "solved", "func": "matrix_solve", "matrixField": "mat", "field": "vec" }]
                        """, com.google.gson.JsonArray.class), fields);
        selectFunctions.forEach(SelectFunction::setup);
        final Map<String, Object> selected = SelectFunction.apply(selectFunctions, values, TIMESTAMP);
        assertDoubleArray(List.of(1d, 3d), selected.get("solved"));

        // Query: the matrix field surfaces as a flat ARRAY<DOUBLE>; the
        // trailing-columns overload reads it (2x + y = 5, x + 3y = 10 -> [1, 3]).
        final Query2 query = Query2.builder()
                .withInput("INPUT", Schema.of(fields))
                .withSql("""
                        SELECT i.userId AS userId,
                               MATRIX_SOLVE(i.mat, i.vec, 2) AS solved
                        FROM INPUT AS i
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(List.of(
                    MElement.of(values, TIMESTAMP)), TIMESTAMP);
            Assertions.assertEquals(1, outputs.size());
            assertDoubleArray(List.of(1d, 3d), outputs.get(0).getPrimitiveValue("solved"));
        } finally {
            query.teardown();
        }
    }

    private static void assertDoubleArray(List<Double> expected, Object actual) {
        Assertions.assertInstanceOf(List.class, actual);
        final List<?> actualValues = (List<?>) actual;
        Assertions.assertEquals(expected.size(), actualValues.size());
        for (int i = 0; i < expected.size(); i++) {
            Assertions.assertEquals(expected.get(i), ((Number) actualValues.get(i)).doubleValue(), DELTA);
        }
    }
}
