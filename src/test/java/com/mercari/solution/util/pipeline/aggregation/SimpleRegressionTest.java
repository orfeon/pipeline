package com.mercari.solution.util.pipeline.aggregation;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SimpleRegressionTest {

    private static final double DELTA = 1e-9;

    private static final List<Schema.Field> INPUT_FIELDS = List.of(
            Schema.Field.of("longField", Schema.FieldType.INT64),
            Schema.Field.of("doubleField", Schema.FieldType.FLOAT64),
            Schema.Field.of("weightField", Schema.FieldType.FLOAT64));

    private static AggregateFunction create(final String json) {
        final AggregateFunction fn = AggregateFunction.of(new Gson().fromJson(json, JsonElement.class), INPUT_FIELDS);
        fn.setup();
        return fn;
    }

    private static MElement element(final long epochMillis, final Object... kv) {
        final Map<String, Object> values = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            values.put((String) kv[i], kv[i + 1]);
        }
        return MElement.of(values, epochMillis);
    }

    private static Map<String, Object> output(final AggregateFunction fn, final Accumulator accumulator) {
        return (Map<String, Object>) fn.extractOutput(accumulator, new HashMap<>());
    }

    private static Accumulator addPoints(final AggregateFunction fn, Accumulator accumulator, final double[][] xy) {
        for (final double[] point : xy) {
            accumulator = fn.addInput(accumulator, element(0L, "longField", (long) point[0], "doubleField", point[1]));
        }
        return accumulator;
    }

    @Test
    public void testPerfectLine() {
        // y = 2x + 1
        final AggregateFunction reg = create("{ \"name\": \"r\", \"op\": \"simple_regression\", \"field\": \"doubleField\", \"xField\": \"longField\" }");
        final Accumulator accumulator = addPoints(reg, Accumulator.of(),
                new double[][]{ {1, 3}, {2, 5}, {3, 7}, {4, 9} });

        final Map<String, Object> out = output(reg, accumulator);
        Assertions.assertEquals(2.0D, (Double) out.get("Slope"), DELTA);
        Assertions.assertEquals(1.0D, (Double) out.get("Intercept"), DELTA);
        Assertions.assertEquals(0.0D, (Double) out.get("RMSE"), DELTA);
        Assertions.assertEquals(4.0D, ((Number) out.get("N")).doubleValue(), DELTA);
    }

    @Test
    public void testNoisyData() {
        // x={1,2,3,4}, y={2,4,5,8}: slope 1.9, intercept 0, SSE 0.7
        final AggregateFunction reg = create("{ \"name\": \"r\", \"op\": \"simple_regression\", \"field\": \"doubleField\", \"xField\": \"longField\" }");
        final Accumulator accumulator = addPoints(reg, Accumulator.of(),
                new double[][]{ {1, 2}, {2, 4}, {3, 5}, {4, 8} });

        final Map<String, Object> out = output(reg, accumulator);
        Assertions.assertEquals(1.9D, (Double) out.get("Slope"), DELTA);
        Assertions.assertEquals(0.0D, (Double) out.get("Intercept"), DELTA);
        Assertions.assertEquals(Math.sqrt(0.7D / 4D), (Double) out.get("RMSE"), DELTA);
        Assertions.assertEquals(4.0D, ((Number) out.get("N")).doubleValue(), DELTA);
    }

    @Test
    public void testMergeMatchesSequential() {
        final AggregateFunction reg = create("{ \"name\": \"r\", \"op\": \"simple_regression\", \"field\": \"doubleField\", \"xField\": \"longField\" }");

        final Accumulator sequential = addPoints(reg, Accumulator.of(),
                new double[][]{ {1, 2}, {2, 4}, {3, 5}, {4, 8} });
        final Accumulator acc1 = addPoints(reg, Accumulator.of(), new double[][]{ {1, 2}, {2, 4} });
        final Accumulator acc2 = addPoints(reg, Accumulator.of(), new double[][]{ {3, 5}, {4, 8} });
        final Accumulator merged = reg.mergeAccumulator(acc1, acc2);

        final Map<String, Object> expected = output(reg, sequential);
        final Map<String, Object> actual = output(reg, merged);
        Assertions.assertEquals((Double) expected.get("Slope"), (Double) actual.get("Slope"), DELTA);
        Assertions.assertEquals((Double) expected.get("Intercept"), (Double) actual.get("Intercept"), DELTA);
        Assertions.assertEquals((Double) expected.get("RMSE"), (Double) actual.get("RMSE"), DELTA);
        Assertions.assertEquals(4.0D, ((Number) actual.get("N")).doubleValue(), DELTA);
    }

    @Test
    public void testMergeWithEmptyAccumulator() {
        final AggregateFunction reg = create("{ \"name\": \"r\", \"op\": \"simple_regression\", \"field\": \"doubleField\", \"xField\": \"longField\" }");
        final Accumulator acc = addPoints(reg, Accumulator.of(), new double[][]{ {1, 3}, {2, 5}, {3, 7} });

        final Accumulator merged = reg.mergeAccumulator(Accumulator.of(), acc);
        final Map<String, Object> out = output(reg, merged);
        Assertions.assertEquals(2.0D, (Double) out.get("Slope"), DELTA);
        Assertions.assertEquals(1.0D, (Double) out.get("Intercept"), DELTA);

        final Accumulator merged2 = reg.mergeAccumulator(merged, Accumulator.of());
        final Map<String, Object> out2 = output(reg, merged2);
        Assertions.assertEquals(2.0D, (Double) out2.get("Slope"), DELTA);
        Assertions.assertEquals(1.0D, (Double) out2.get("Intercept"), DELTA);
    }

    @Test
    public void testNoIntercept() {
        // y = 2x through the origin
        final AggregateFunction reg = create("{ \"name\": \"r\", \"op\": \"simple_regression\", \"field\": \"doubleField\", \"xField\": \"longField\", \"hasIntercept\": false }");
        final Accumulator accumulator = addPoints(reg, Accumulator.of(),
                new double[][]{ {1, 2}, {2, 4}, {3, 6} });

        final Map<String, Object> out = output(reg, accumulator);
        Assertions.assertEquals(2.0D, (Double) out.get("Slope"), DELTA);
        Assertions.assertEquals(0.0D, (Double) out.get("Intercept"), DELTA);
        Assertions.assertEquals(0.0D, (Double) out.get("RMSE"), DELTA);
    }

    @Test
    public void testDefaultXIsTimestamp() {
        final AggregateFunction reg = create("{ \"name\": \"r\", \"op\": \"simple_regression\", \"field\": \"doubleField\" }");
        Accumulator accumulator = Accumulator.of();
        accumulator = reg.addInput(accumulator, element(0L, "doubleField", 5.0D));
        accumulator = reg.addInput(accumulator, element(1000L, "doubleField", 7.0D));

        final Map<String, Object> out = output(reg, accumulator);
        Assertions.assertEquals(0.002D, (Double) out.get("Slope"), DELTA);
        Assertions.assertEquals(5.0D, (Double) out.get("Intercept"), DELTA);
    }

    @Test
    public void testExpressionY() {
        // y = doubleField * 2 + 1 where doubleField = x -> slope 2, intercept 1
        final AggregateFunction reg = create("{ \"name\": \"r\", \"op\": \"simple_regression\", \"expression\": \"doubleField * 2 + 1\", \"xField\": \"longField\" }");
        Accumulator accumulator = Accumulator.of();
        for (long x = 1; x <= 3; x++) {
            accumulator = reg.addInput(accumulator, element(0L, "longField", x, "doubleField", (double) x));
        }
        final Map<String, Object> out = output(reg, accumulator);
        Assertions.assertEquals(2.0D, (Double) out.get("Slope"), DELTA);
        Assertions.assertEquals(1.0D, (Double) out.get("Intercept"), DELTA);
    }

    @Test
    public void testNullValuesIgnored() {
        final AggregateFunction reg = create("{ \"name\": \"r\", \"op\": \"simple_regression\", \"field\": \"doubleField\", \"xField\": \"longField\" }");
        Accumulator accumulator = Accumulator.of();
        accumulator = reg.addInput(accumulator, element(0L, "longField", 1L, "doubleField", null));
        accumulator = reg.addInput(accumulator, element(1L, "longField", 2L, "doubleField", 3.0D));
        accumulator = reg.addInput(accumulator, element(2L, "longField", 3L, "doubleField", 5.0D));

        final Map<String, Object> out = output(reg, accumulator);
        Assertions.assertEquals(2.0D, ((Number) out.get("N")).doubleValue(), DELTA);
        Assertions.assertEquals(2.0D, (Double) out.get("Slope"), DELTA);
        Assertions.assertEquals(-1.0D, (Double) out.get("Intercept"), DELTA);
    }

    @Test
    public void testSinglePointSlopeIsNaN() {
        final AggregateFunction reg = create("{ \"name\": \"r\", \"op\": \"simple_regression\", \"field\": \"doubleField\", \"xField\": \"longField\" }");
        final Accumulator accumulator = addPoints(reg, Accumulator.of(), new double[][]{ {1, 3} });

        final Map<String, Object> out = output(reg, accumulator);
        Assertions.assertTrue(Double.isNaN((Double) out.get("Slope")));
        Assertions.assertEquals(1.0D, ((Number) out.get("N")).doubleValue(), DELTA);
    }

    @Test
    public void testConstantXSlopeIsNaN() {
        final AggregateFunction reg = create("{ \"name\": \"r\", \"op\": \"simple_regression\", \"field\": \"doubleField\", \"xField\": \"longField\" }");
        final Accumulator accumulator = addPoints(reg, Accumulator.of(), new double[][]{ {1, 2}, {1, 4} });
        Assertions.assertTrue(Double.isNaN((Double) output(reg, accumulator).get("Slope")));
    }

    @Test
    public void testExtractOutputEmpty() {
        final AggregateFunction reg = create("{ \"name\": \"r\", \"op\": \"simple_regression\", \"field\": \"doubleField\", \"xField\": \"longField\" }");
        final Map<String, Object> out = output(reg, Accumulator.of());
        Assertions.assertTrue(Double.isNaN((Double) out.get("Slope")));
        Assertions.assertEquals(0.0D, ((Number) out.get("N")).doubleValue(), DELTA);
    }

    @Test
    public void testWeightedMatchesRepeatedPoints() {
        // (0,0) w=1, (1,1) w=1, (2,4) w=2 must equal unweighted {(0,0),(1,1),(2,4),(2,4)}
        final AggregateFunction plain = create("{ \"name\": \"r\", \"op\": \"simple_regression\", \"field\": \"doubleField\", \"xField\": \"longField\" }");
        final Accumulator plainAcc = addPoints(plain, Accumulator.of(),
                new double[][]{ {0, 0}, {1, 1}, {2, 4}, {2, 4} });

        final AggregateFunction weighted = create("{ \"name\": \"r\", \"op\": \"simple_regression\", \"field\": \"doubleField\", \"xField\": \"longField\", \"weightField\": \"weightField\" }");
        Accumulator weightedAcc = Accumulator.of();
        weightedAcc = weighted.addInput(weightedAcc, element(0L, "longField", 0L, "doubleField", 0.0D, "weightField", 1.0D));
        weightedAcc = weighted.addInput(weightedAcc, element(1L, "longField", 1L, "doubleField", 1.0D, "weightField", 1.0D));
        weightedAcc = weighted.addInput(weightedAcc, element(2L, "longField", 2L, "doubleField", 4.0D, "weightField", 2.0D));

        final Map<String, Object> expected = output(plain, plainAcc);
        final Map<String, Object> actual = output(weighted, weightedAcc);
        Assertions.assertEquals(23.0D / 11.0D, (Double) expected.get("Slope"), DELTA);
        Assertions.assertEquals((Double) expected.get("Slope"), (Double) actual.get("Slope"), DELTA);
        Assertions.assertEquals((Double) expected.get("Intercept"), (Double) actual.get("Intercept"), DELTA);
        Assertions.assertEquals((Double) expected.get("RMSE"), (Double) actual.get("RMSE"), DELTA);
    }

    @Test
    public void testOfUnknownFieldThrows() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> create("{ \"name\": \"r\", \"op\": \"simple_regression\", \"field\": \"unknownField\" }"));
    }

    @Test
    public void testOutputFieldType() {
        final AggregateFunction reg = create("{ \"name\": \"r\", \"op\": \"simple_regression\", \"field\": \"doubleField\", \"xField\": \"longField\" }");
        Assertions.assertEquals(Schema.Type.element, reg.getOutputFieldType().getType());
        final Schema elementSchema = reg.getOutputFieldType().getElementSchema();
        Assertions.assertTrue(elementSchema.hasField("Slope"));
        Assertions.assertTrue(elementSchema.hasField("Intercept"));
        Assertions.assertTrue(elementSchema.hasField("RMSE"));
        Assertions.assertTrue(elementSchema.hasField("N"));
    }

}
