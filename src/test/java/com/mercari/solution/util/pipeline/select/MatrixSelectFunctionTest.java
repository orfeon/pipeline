package com.mercari.solution.util.pipeline.select;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.mercari.solution.module.Schema;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for the matrix select functions (cosine_similarity / matrix_multiply /
 * matrix_solve / mahalanobis / poly_fit) — the select-side adapters over
 * {@code util/domain/math/MatrixOps}, sharing the core with the query
 * module's built-ins.
 */
public class MatrixSelectFunctionTest {

    private static final Instant TIMESTAMP = Instant.parse("2025-05-01T00:00:00Z");
    private static final double DELTA = 1e-9;

    private static final List<Schema.Field> INPUT_FIELDS = List.of(
            Schema.Field.of("emb1", Schema.FieldType.array(Schema.FieldType.FLOAT64)),
            Schema.Field.of("emb2", Schema.FieldType.array(Schema.FieldType.FLOAT64)),
            Schema.Field.of("features", Schema.FieldType.array(Schema.FieldType.FLOAT64)),
            Schema.Field.of("xs", Schema.FieldType.array(Schema.FieldType.FLOAT64)),
            Schema.Field.of("ys", Schema.FieldType.array(Schema.FieldType.FLOAT64)),
            Schema.Field.of("designMatrix", Schema.FieldType.array(
                    Schema.FieldType.array(Schema.FieldType.FLOAT64))),
            Schema.Field.of("flatMatrix", Schema.FieldType.array(Schema.FieldType.FLOAT64)),
            Schema.Field.of("tensorField", Schema.FieldType.matrix(
                    Schema.FieldType.FLOAT64, List.of(2, 2))));

    private static List<SelectFunction> functions(String config) {
        final JsonArray array = new Gson().fromJson(config, JsonArray.class);
        final List<SelectFunction> selectFunctions = SelectFunction.of(array, INPUT_FIELDS);
        selectFunctions.forEach(SelectFunction::setup);
        return selectFunctions;
    }

    @Test
    public void testFunctionsAndSchema() {
        final String config = """
                [
                  { "name": "sim", "func": "cosine_similarity", "left": "emb1", "right": "emb2" },
                  { "name": "projected", "func": "matrix_multiply",
                    "matrix": [[1, 0, 0], [0, 1, 0]], "field": "features" },
                  { "name": "solved", "func": "matrix_solve",
                    "matrix": [[2, 1], [1, 3]], "field": "emb2" },
                  { "name": "score", "func": "mahalanobis", "field": "emb1",
                    "mean": [0, 0], "covariance": [[1, 0], [0, 1]] },
                  { "name": "trend", "func": "poly_fit", "y": "ys", "degree": 1 },
                  { "name": "curve", "func": "poly_fit", "x": "xs", "y": "ys", "degree": 2 }
                ]
                """;
        final List<SelectFunction> selectFunctions = functions(config);

        final Schema outputSchema = SelectFunction.createSchema(selectFunctions);
        Assertions.assertEquals(Schema.Type.float64, outputSchema.getField("sim").getFieldType().getType());
        Assertions.assertEquals(Schema.Type.float64, outputSchema.getField("score").getFieldType().getType());
        for (final String field : List.of("projected", "solved", "trend", "curve")) {
            Assertions.assertEquals(Schema.Type.array, outputSchema.getField(field).getFieldType().getType());
            Assertions.assertEquals(Schema.Type.float64,
                    outputSchema.getField(field).getFieldType().getArrayValueType().getType());
        }

        final Map<String, Object> input = new HashMap<>();
        input.put("emb1", List.of(3d, 4d));
        input.put("emb2", List.of(5d, 10d));
        input.put("features", List.of(7d, 8d, 9d));
        // y = 2 - x + 3x^2 over xs; ys(trend) is fit against 0..n-1.
        input.put("xs", List.of(-1d, 0d, 1d, 2d));
        input.put("ys", List.of(6d, 2d, 4d, 12d));

        final Map<String, Object> output = SelectFunction.apply(selectFunctions, input, TIMESTAMP);

        // (3,4)·(5,10) / (5 * sqrt(125))
        Assertions.assertEquals(55d / (5d * Math.sqrt(125d)), (Double) output.get("sim"), DELTA);
        assertDoubleArray(List.of(7d, 8d), output.get("projected"));
        // 2x + y = 5, x + 3y = 10 -> [1, 3]
        assertDoubleArray(List.of(1d, 3d), output.get("solved"));
        // Identity covariance: euclidean distance of (3, 4).
        Assertions.assertEquals(5d, (Double) output.get("score"), DELTA);
        assertDoubleArray(List.of(2d, -1d, 3d), output.get("curve"));
        // Degree-1 fit of [6, 2, 4, 12] against x = 0..3.
        final List<?> trend = (List<?>) output.get("trend");
        Assertions.assertEquals(2, trend.size());

        // NULL input yields NULL.
        final Map<String, Object> nullInput = new HashMap<>(input);
        nullInput.put("emb2", null);
        final Map<String, Object> nullOutput = SelectFunction.apply(selectFunctions, nullInput, TIMESTAMP);
        Assertions.assertNull(nullOutput.get("sim"));
        Assertions.assertNull(nullOutput.get("solved"));
        Assertions.assertNotNull(nullOutput.get("score"));
    }

    @Test
    public void testMatrixFromField() {
        final String config = """
                [
                  { "name": "solved", "func": "matrix_solve", "matrixField": "designMatrix", "field": "emb2" }
                ]
                """;
        final List<SelectFunction> selectFunctions = functions(config);
        final Map<String, Object> input = new HashMap<>();
        input.put("designMatrix", List.of(List.of(2d, 1d), List.of(1d, 3d)));
        input.put("emb2", List.of(5d, 10d));
        final Map<String, Object> output = SelectFunction.apply(selectFunctions, input, TIMESTAMP);
        assertDoubleArray(List.of(1d, 3d), output.get("solved"));
    }

    @Test
    public void testMatrixTypedFieldUsesSchemaShape() {
        // A matrix-typed field (flat row-major values, 2D shape in the schema —
        // an ONNX tensor or reshape output) needs no columns parameter.
        final String config = """
                [
                  { "name": "solved", "func": "matrix_solve", "matrixField": "tensorField", "field": "emb2" }
                ]
                """;
        final List<SelectFunction> selectFunctions = functions(config);
        final Map<String, Object> input = new HashMap<>();
        input.put("tensorField", List.of(2d, 1d, 1d, 3d));  // [[2, 1], [1, 3]]
        input.put("emb2", List.of(5d, 10d));
        final Map<String, Object> output = SelectFunction.apply(selectFunctions, input, TIMESTAMP);
        assertDoubleArray(List.of(1d, 3d), output.get("solved"));
    }

    @Test
    public void testFlatArrayFieldWithColumnsParameter() {
        final String config = """
                [
                  { "name": "solved", "func": "matrix_solve", "matrixField": "flatMatrix", "field": "emb2", "columns": 2 }
                ]
                """;
        final List<SelectFunction> selectFunctions = functions(config);
        final Map<String, Object> input = new HashMap<>();
        input.put("flatMatrix", List.of(2d, 1d, 1d, 3d));
        input.put("emb2", List.of(5d, 10d));
        final Map<String, Object> output = SelectFunction.apply(selectFunctions, input, TIMESTAMP);
        assertDoubleArray(List.of(1d, 3d), output.get("solved"));

        // Without columns, a flat array field is read nested and fails clearly.
        final List<SelectFunction> nested = functions("""
                [
                  { "name": "solved", "func": "matrix_solve", "matrixField": "flatMatrix", "field": "emb2" }
                ]
                """);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> SelectFunction.apply(nested, input, TIMESTAMP));
    }

    @Test
    public void testMahalanobisWithPrecisionField() {
        final String config = """
                [
                  { "name": "score", "func": "mahalanobis", "field": "emb1",
                    "meanField": "emb2", "precisionField": "designMatrix" }
                ]
                """;
        final List<SelectFunction> selectFunctions = functions(config);
        final Map<String, Object> input = new HashMap<>();
        input.put("emb1", List.of(3d, 4d));
        input.put("emb2", List.of(0d, 0d));
        input.put("designMatrix", List.of(List.of(1d, 0d), List.of(0d, 1d)));
        final Map<String, Object> output = SelectFunction.apply(selectFunctions, input, TIMESTAMP);
        Assertions.assertEquals(5d, (Double) output.get("score"), DELTA);
    }

    @Test
    public void testInvalidConfigurations() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> functions("""
                [{ "name": "sim", "func": "cosine_similarity", "left": "emb1" }]
                """));
        Assertions.assertThrows(IllegalArgumentException.class, () -> functions("""
                [{ "name": "projected", "func": "matrix_multiply", "field": "features" }]
                """));
        Assertions.assertThrows(IllegalArgumentException.class, () -> functions("""
                [{ "name": "trend", "func": "poly_fit", "y": "ys", "degree": -1 }]
                """));
        // Dimension mismatch surfaces with the function name and select field name.
        final List<SelectFunction> selectFunctions = functions("""
                [{ "name": "sim", "func": "cosine_similarity", "left": "emb1", "right": "emb2" }]
                """);
        final Map<String, Object> input = new HashMap<>();
        input.put("emb1", List.of(1d, 2d));
        input.put("emb2", List.of(1d, 2d, 3d));
        final IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> SelectFunction.apply(selectFunctions, input, TIMESTAMP));
        Assertions.assertTrue(e.getMessage().contains("sim"));
    }

    private static void assertDoubleArray(List<Double> expected, Object actual) {
        Assertions.assertInstanceOf(List.class, actual);
        final List<?> values = (List<?>) actual;
        Assertions.assertEquals(expected.size(), values.size());
        for (int i = 0; i < expected.size(); i++) {
            Assertions.assertEquals(expected.get(i), ((Number) values.get(i)).doubleValue(), DELTA);
        }
    }
}
