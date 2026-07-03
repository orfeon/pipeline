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

public class AvgTest {

    private static final double DELTA = 1e-9;

    private static final List<Schema.Field> INPUT_FIELDS = List.of(
            Schema.Field.of("stringField", Schema.FieldType.STRING),
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

    private static Double output(final AggregateFunction fn, final Accumulator accumulator) {
        return (Double) fn.extractOutput(accumulator, new HashMap<>());
    }

    @Test
    public void testSimpleAvg() {
        final AggregateFunction avg = create("{ \"name\": \"a\", \"op\": \"avg\", \"field\": \"longField\" }");
        Accumulator accumulator = Accumulator.of();
        for (long v = 1; v <= 6; v++) {
            accumulator = avg.addInput(accumulator, element(v, "longField", v));
        }
        Assertions.assertEquals(3.5D, output(avg, accumulator), DELTA);
    }

    @Test
    public void testWeightedAvg() {
        final AggregateFunction avg = create("{ \"name\": \"a\", \"op\": \"avg\", \"field\": \"doubleField\", \"weightField\": \"weightField\" }");
        Accumulator accumulator = Accumulator.of();
        accumulator = avg.addInput(accumulator, element(0L, "doubleField", 1.0D, "weightField", 1.0D));
        accumulator = avg.addInput(accumulator, element(1L, "doubleField", 4.0D, "weightField", 3.0D));
        // (1*1 + 4*3) / 4 = 3.25
        Assertions.assertEquals(3.25D, output(avg, accumulator), DELTA);
    }

    @Test
    public void testWeightExpression() {
        final AggregateFunction avg = create("{ \"name\": \"a\", \"op\": \"avg\", \"field\": \"doubleField\", \"weightExpression\": \"weightField * 2\" }");
        Accumulator accumulator = Accumulator.of();
        accumulator = avg.addInput(accumulator, element(0L, "doubleField", 1.0D, "weightField", 1.0D));
        accumulator = avg.addInput(accumulator, element(1L, "doubleField", 4.0D, "weightField", 3.0D));
        // doubling all weights must not change the weighted average
        Assertions.assertEquals(3.25D, output(avg, accumulator), DELTA);
    }

    @Test
    public void testExpressionAvg() {
        final AggregateFunction avg = create("{ \"name\": \"a\", \"op\": \"avg\", \"expression\": \"longField + doubleField\" }");
        Accumulator accumulator = Accumulator.of();
        accumulator = avg.addInput(accumulator, element(0L, "longField", 1L, "doubleField", 1.0D));
        accumulator = avg.addInput(accumulator, element(1L, "longField", 2L, "doubleField", 2.0D));
        // (2 + 4) / 2 = 3
        Assertions.assertEquals(3.0D, output(avg, accumulator), DELTA);
    }

    @Test
    public void testNullValuesIgnored() {
        final AggregateFunction avg = create("{ \"name\": \"a\", \"op\": \"avg\", \"field\": \"longField\" }");
        Accumulator accumulator = Accumulator.of();
        accumulator = avg.addInput(accumulator, element(0L, "longField", 2L));
        accumulator = avg.addInput(accumulator, element(1L, "longField", null));
        accumulator = avg.addInput(accumulator, element(2L, "longField", 4L));
        Assertions.assertEquals(3.0D, output(avg, accumulator), DELTA);
    }

    @Test
    public void testNullWeightIgnored() {
        // element with a null weight must be ignored, not blow up
        final AggregateFunction avg = create("{ \"name\": \"a\", \"op\": \"avg\", \"field\": \"doubleField\", \"weightField\": \"weightField\" }");
        Accumulator accumulator = Accumulator.of();
        accumulator = avg.addInput(accumulator, element(0L, "doubleField", 2.0D, "weightField", 1.0D));
        accumulator = avg.addInput(accumulator, element(1L, "doubleField", 100.0D, "weightField", null));
        accumulator = avg.addInput(accumulator, element(2L, "doubleField", 4.0D, "weightField", 1.0D));
        Assertions.assertEquals(3.0D, output(avg, accumulator), DELTA);
    }

    @Test
    public void testExtractOutputEmpty() {
        final AggregateFunction avg = create("{ \"name\": \"a\", \"op\": \"avg\", \"field\": \"longField\" }");
        Assertions.assertNull(output(avg, Accumulator.of()));
    }

    @Test
    public void testMergeAccumulator() {
        final AggregateFunction avg = create("{ \"name\": \"a\", \"op\": \"avg\", \"field\": \"longField\" }");
        Accumulator acc1 = Accumulator.of();
        acc1 = avg.addInput(acc1, element(0L, "longField", 1L));
        acc1 = avg.addInput(acc1, element(1L, "longField", 2L));

        Accumulator acc2 = Accumulator.of();
        acc2 = avg.addInput(acc2, element(2L, "longField", 4L));
        acc2 = avg.addInput(acc2, element(3L, "longField", 5L));
        acc2 = avg.addInput(acc2, element(4L, "longField", 6L));

        final Accumulator merged = avg.mergeAccumulator(acc1, acc2);
        // (1+2+4+5+6) / 5 = 3.6
        Assertions.assertEquals(3.6D, output(avg, merged), DELTA);
    }

    @Test
    public void testMergeAccumulatorWithEmpty() {
        final AggregateFunction avg = create("{ \"name\": \"a\", \"op\": \"avg\", \"field\": \"longField\" }");
        Accumulator acc = Accumulator.of();
        acc = avg.addInput(acc, element(0L, "longField", 2L));
        acc = avg.addInput(acc, element(1L, "longField", 4L));

        final Accumulator merged = avg.mergeAccumulator(Accumulator.of(), acc);
        Assertions.assertEquals(3.0D, output(avg, merged), DELTA);

        final Accumulator merged2 = avg.mergeAccumulator(merged, Accumulator.of());
        Assertions.assertEquals(3.0D, output(avg, merged2), DELTA);
    }

    @Test
    public void testMergeMatchesSequential() {
        final AggregateFunction avg = create("{ \"name\": \"a\", \"op\": \"avg\", \"field\": \"doubleField\", \"weightField\": \"weightField\" }");

        Accumulator sequential = Accumulator.of();
        Accumulator acc1 = Accumulator.of();
        Accumulator acc2 = Accumulator.of();
        final double[][] data = { {1.0D, 1.0D}, {2.0D, 2.0D}, {5.0D, 1.0D}, {8.0D, 0.5D} };
        for (int i = 0; i < data.length; i++) {
            final MElement e = element(i, "doubleField", data[i][0], "weightField", data[i][1]);
            sequential = avg.addInput(sequential, e);
            if (i < 2) {
                acc1 = avg.addInput(acc1, e);
            } else {
                acc2 = avg.addInput(acc2, e);
            }
        }
        final Accumulator merged = avg.mergeAccumulator(acc1, acc2);
        Assertions.assertEquals(output(avg, sequential), output(avg, merged), DELTA);
    }

    @Test
    public void testConditionFilter() {
        final AggregateFunction avg = create("""
                { "name": "a", "op": "avg", "field": "longField", "condition": { "key": "longField", "op": ">", "value": 0 } }
                """);
        Assertions.assertTrue(avg.filter(element(0L, "longField", 1L)));
        Assertions.assertFalse(avg.filter(element(0L, "longField", -1L)));
    }

    @Test
    public void testOutputFieldType() {
        final AggregateFunction avg = create("{ \"name\": \"a\", \"op\": \"avg\", \"field\": \"longField\" }");
        Assertions.assertEquals(Schema.Type.float64, avg.getOutputFieldType().getType());
    }

}
