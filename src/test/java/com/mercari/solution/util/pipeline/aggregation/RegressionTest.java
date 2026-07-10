package com.mercari.solution.util.pipeline.aggregation;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.coder.AccumulatorCoder;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for the multivariate regression aggregate ({@code op: regression}) —
 * Gram-matrix accumulation solved through the shared ojalgo core
 * ({@code util/domain/math/MatrixOps}), the multi-x extension of
 * {@code simple_regression}.
 */
public class RegressionTest {

    private static final Instant TIMESTAMP = Instant.parse("2025-05-01T00:00:00Z");
    private static final double DELTA = 1e-9;

    private static final List<Schema.Field> INPUT_FIELDS = List.of(
            Schema.Field.of("y", Schema.FieldType.FLOAT64),
            Schema.Field.of("x1", Schema.FieldType.FLOAT64),
            Schema.Field.of("x2", Schema.FieldType.FLOAT64));

    private static AggregateFunction create(final String json) {
        final AggregateFunction function = AggregateFunction.of(
                new Gson().fromJson(json, JsonElement.class), INPUT_FIELDS);
        function.setup();
        return function;
    }

    private static MElement element(Double y, Double x1, Double x2) {
        final Map<String, Object> values = new HashMap<>();
        values.put("y", y);
        values.put("x1", x1);
        values.put("x2", x2);
        return MElement.of(values, TIMESTAMP);
    }

    /** Exact samples of y = 1 + 2 x1 + 3 x2. */
    private static List<MElement> samples() {
        final double[][] xs = {{0, 0}, {1, 0}, {0, 1}, {1, 1}, {2, 1}, {1, 2}};
        return java.util.Arrays.stream(xs)
                .map(x -> element(1 + 2 * x[0] + 3 * x[1], x[0], x[1]))
                .toList();
    }

    @Test
    public void testOfAndOutputSchema() {
        final AggregateFunction function = create("""
                { "name": "model", "op": "regression", "field": "y", "xFields": ["x1", "x2"] }
                """);
        Assertions.assertInstanceOf(Regression.class, function);
        Assertions.assertEquals("model", function.getName());
        final Schema.FieldType outputType = function.getOutputFieldType();
        Assertions.assertEquals(Schema.Type.element, outputType.getType());
        final Schema outputSchema = outputType.getElementSchema();
        Assertions.assertEquals(Schema.Type.array, outputSchema.getField("Coefficients").getFieldType().getType());
        Assertions.assertEquals(Schema.Type.float64, outputSchema.getField("RMSE").getFieldType().getType());
        Assertions.assertEquals(Schema.Type.int64, outputSchema.getField("N").getFieldType().getType());
    }

    @Test
    public void testExactFitWithIntercept() {
        final AggregateFunction function = create("""
                { "name": "model", "op": "regression", "field": "y", "xFields": ["x1", "x2"] }
                """);
        Accumulator accumulator = Accumulator.of();
        for (final MElement input : samples()) {
            accumulator = function.addInput(accumulator, input);
        }
        final Map<String, Object> output = output(function, accumulator);
        assertCoefficients(List.of(1d, 2d, 3d), output.get("Coefficients"));
        Assertions.assertEquals(0d, (Double) output.get("RMSE"), 1e-6);
        Assertions.assertEquals(6L, output.get("N"));
    }

    @Test
    public void testNoInterceptAndNullRowsSkipped() {
        final AggregateFunction function = create("""
                { "name": "model", "op": "regression", "field": "y",
                  "xFields": ["x1", "x2"], "hasIntercept": false }
                """);
        Accumulator accumulator = Accumulator.of();
        // y = 2 x1 + 3 x2 exactly.
        for (final double[] x : new double[][]{{1, 0}, {0, 1}, {1, 1}, {2, 1}}) {
            accumulator = function.addInput(accumulator, element(2 * x[0] + 3 * x[1], x[0], x[1]));
        }
        // Null y and null x rows must be skipped, not poison the fit.
        accumulator = function.addInput(accumulator, element(null, 9d, 9d));
        accumulator = function.addInput(accumulator, element(9d, null, 9d));

        final Map<String, Object> output = output(function, accumulator);
        assertCoefficients(List.of(2d, 3d), output.get("Coefficients"));
        Assertions.assertEquals(4L, output.get("N"));
    }

    @Test
    public void testMergeMatchesSingleAccumulation() {
        final AggregateFunction function = create("""
                { "name": "model", "op": "regression", "field": "y", "xFields": ["x1", "x2"] }
                """);
        final List<MElement> samples = samples();

        Accumulator whole = Accumulator.of();
        for (final MElement input : samples) {
            whole = function.addInput(whole, input);
        }

        Accumulator part1 = Accumulator.of();
        Accumulator part2 = Accumulator.of();
        for (int i = 0; i < samples.size(); i++) {
            if (i % 2 == 0) {
                part1 = function.addInput(part1, samples.get(i));
            } else {
                part2 = function.addInput(part2, samples.get(i));
            }
        }
        // Merging into an empty base first mirrors the combiner's lifecycle.
        Accumulator merged = function.mergeAccumulator(Accumulator.of(), part1);
        merged = function.mergeAccumulator(merged, part2);

        final Map<String, Object> fromWhole = output(function, whole);
        final Map<String, Object> fromMerged = output(function, merged);
        assertCoefficients(List.of(1d, 2d, 3d), fromMerged.get("Coefficients"));
        Assertions.assertEquals(fromWhole.get("N"), fromMerged.get("N"));
        Assertions.assertEquals((Double) fromWhole.get("RMSE"), (Double) fromMerged.get("RMSE"), DELTA);
    }

    @Test
    public void testAccumulatorCoderRoundTrip() throws Exception {
        final AggregateFunction function = create("""
                { "name": "model", "op": "regression", "field": "y", "xFields": ["x1", "x2"] }
                """);
        final List<MElement> samples = samples();
        Accumulator accumulator = Accumulator.of();
        for (int i = 0; i < 3; i++) {
            accumulator = function.addInput(accumulator, samples.get(i));
        }

        // The Gram lists must survive the Avro union accumulator coder mid-stream.
        final AccumulatorCoder coder = AccumulatorCoder.of();
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        coder.encode(accumulator, bytes);
        Accumulator decoded = coder.decode(new ByteArrayInputStream(bytes.toByteArray()));

        for (int i = 3; i < samples.size(); i++) {
            decoded = function.addInput(decoded, samples.get(i));
        }
        assertCoefficients(List.of(1d, 2d, 3d), output(function, decoded).get("Coefficients"));
    }

    @Test
    public void testRidgeShrinksCoefficients() {
        final AggregateFunction plain = create("""
                { "name": "model", "op": "regression", "field": "y", "xFields": ["x1", "x2"] }
                """);
        final AggregateFunction ridged = create("""
                { "name": "model", "op": "regression", "field": "y",
                  "xFields": ["x1", "x2"], "ridge": 100.0 }
                """);
        Accumulator a1 = Accumulator.of();
        Accumulator a2 = Accumulator.of();
        for (final MElement input : samples()) {
            a1 = plain.addInput(a1, input);
            a2 = ridged.addInput(a2, input);
        }
        final List<?> plainCoef = (List<?>) output(plain, a1).get("Coefficients");
        final List<?> ridgedCoef = (List<?>) output(ridged, a2).get("Coefficients");
        Assertions.assertTrue(Math.abs(((Number) ridgedCoef.get(1)).doubleValue())
                < Math.abs(((Number) plainCoef.get(1)).doubleValue()));
        Assertions.assertTrue(Math.abs(((Number) ridgedCoef.get(2)).doubleValue())
                < Math.abs(((Number) plainCoef.get(2)).doubleValue()));
    }

    @Test
    public void testEmptyAccumulator() {
        final AggregateFunction function = create("""
                { "name": "model", "op": "regression", "field": "y", "xFields": ["x1", "x2"] }
                """);
        final Map<String, Object> output = output(function, Accumulator.of());
        Assertions.assertEquals(0L, output.get("N"));
        Assertions.assertNull(output.get("Coefficients"));
        Assertions.assertNull(output.get("RMSE"));
    }

    @Test
    public void testValidate() {
        final AggregateFunction noX = create("""
                { "name": "model", "op": "regression", "field": "y" }
                """);
        Assertions.assertFalse(noX.validate(0, 0).isEmpty());
        final AggregateFunction noY = create("""
                { "name": "model", "op": "regression", "xFields": ["x1"] }
                """);
        Assertions.assertFalse(noY.validate(0, 0).isEmpty());
        final AggregateFunction negativeRidge = create("""
                { "name": "model", "op": "regression", "field": "y",
                  "xFields": ["x1"], "ridge": -1.0 }
                """);
        Assertions.assertFalse(negativeRidge.validate(0, 0).isEmpty());
        final AggregateFunction valid = create("""
                { "name": "model", "op": "regression", "field": "y", "xFields": ["x1"] }
                """);
        Assertions.assertTrue(valid.validate(0, 0).isEmpty());
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> output(AggregateFunction function, Accumulator accumulator) {
        return (Map<String, Object>) function.extractOutput(accumulator, new HashMap<>());
    }

    private static void assertCoefficients(List<Double> expected, Object actual) {
        Assertions.assertInstanceOf(List.class, actual);
        final List<?> values = (List<?>) actual;
        Assertions.assertEquals(expected.size(), values.size());
        for (int i = 0; i < expected.size(); i++) {
            Assertions.assertEquals(expected.get(i), ((Number) values.get(i)).doubleValue(), 1e-6);
        }
    }
}
