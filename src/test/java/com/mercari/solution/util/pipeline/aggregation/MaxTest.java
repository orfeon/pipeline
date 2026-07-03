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

public class MaxTest {

    private static final double DELTA = 1e-9;

    private static final List<Schema.Field> INPUT_FIELDS = List.of(
            Schema.Field.of("stringField", Schema.FieldType.STRING),
            Schema.Field.of("longField", Schema.FieldType.INT64),
            Schema.Field.of("doubleField", Schema.FieldType.FLOAT64));

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

    @Test
    public void testMaxLongField() {
        final AggregateFunction max = create("{ \"name\": \"m\", \"op\": \"max\", \"field\": \"longField\" }");
        Accumulator accumulator = Accumulator.of();
        accumulator = max.addInput(accumulator, element(0L, "longField", 3L));
        accumulator = max.addInput(accumulator, element(1L, "longField", 1L));
        accumulator = max.addInput(accumulator, element(2L, "longField", 4L));
        Assertions.assertEquals(4L, max.extractOutput(accumulator, new HashMap<>()));
    }

    @Test
    public void testMinLongField() {
        final AggregateFunction min = create("{ \"name\": \"m\", \"op\": \"min\", \"field\": \"longField\" }");
        Accumulator accumulator = Accumulator.of();
        accumulator = min.addInput(accumulator, element(0L, "longField", 3L));
        accumulator = min.addInput(accumulator, element(1L, "longField", 1L));
        accumulator = min.addInput(accumulator, element(2L, "longField", 4L));
        Assertions.assertEquals(1L, min.extractOutput(accumulator, new HashMap<>()));
    }

    @Test
    public void testMaxStringField() {
        final AggregateFunction max = create("{ \"name\": \"m\", \"op\": \"max\", \"field\": \"stringField\" }");
        Accumulator accumulator = Accumulator.of();
        accumulator = max.addInput(accumulator, element(0L, "stringField", "a"));
        accumulator = max.addInput(accumulator, element(1L, "stringField", "c"));
        accumulator = max.addInput(accumulator, element(2L, "stringField", "b"));
        Assertions.assertEquals("c", max.extractOutput(accumulator, new HashMap<>()));
    }

    @Test
    public void testMaxExpression() {
        final AggregateFunction max = create("{ \"name\": \"m\", \"op\": \"max\", \"expression\": \"longField * doubleField\" }");
        Accumulator accumulator = Accumulator.of();
        accumulator = max.addInput(accumulator, element(0L, "longField", 2L, "doubleField", 3.0D));
        accumulator = max.addInput(accumulator, element(1L, "longField", 10L, "doubleField", 0.1D));
        Assertions.assertEquals(6.0D, (Double) max.extractOutput(accumulator, new HashMap<>()), DELTA);
    }

    @Test
    public void testNullValuesIgnored() {
        final AggregateFunction max = create("{ \"name\": \"m\", \"op\": \"max\", \"field\": \"longField\" }");
        Accumulator accumulator = Accumulator.of();
        accumulator = max.addInput(accumulator, element(0L, "longField", null));
        accumulator = max.addInput(accumulator, element(1L, "longField", 2L));
        accumulator = max.addInput(accumulator, element(2L, "longField", null));
        Assertions.assertEquals(2L, max.extractOutput(accumulator, new HashMap<>()));
    }

    @Test
    public void testExtractOutputEmpty() {
        final AggregateFunction max = create("{ \"name\": \"m\", \"op\": \"max\", \"field\": \"longField\" }");
        Assertions.assertNull(max.extractOutput(Accumulator.of(), new HashMap<>()));
    }

    @Test
    public void testMergeAccumulator() {
        final AggregateFunction max = create("{ \"name\": \"m\", \"op\": \"max\", \"field\": \"longField\" }");
        Accumulator acc1 = Accumulator.of();
        acc1 = max.addInput(acc1, element(0L, "longField", 5L));

        Accumulator acc2 = Accumulator.of();
        acc2 = max.addInput(acc2, element(1L, "longField", 9L));

        final Accumulator merged = max.mergeAccumulator(acc1, acc2);
        Assertions.assertEquals(9L, max.extractOutput(merged, new HashMap<>()));
    }

    @Test
    public void testMergeAccumulatorMin() {
        final AggregateFunction min = create("{ \"name\": \"m\", \"op\": \"min\", \"field\": \"longField\" }");
        Accumulator acc1 = Accumulator.of();
        acc1 = min.addInput(acc1, element(0L, "longField", 5L));

        Accumulator acc2 = Accumulator.of();
        acc2 = min.addInput(acc2, element(1L, "longField", 9L));

        final Accumulator merged = min.mergeAccumulator(acc1, acc2);
        Assertions.assertEquals(5L, min.extractOutput(merged, new HashMap<>()));
    }

    @Test
    public void testMergeAccumulatorWithEmpty() {
        final AggregateFunction max = create("{ \"name\": \"m\", \"op\": \"max\", \"field\": \"longField\" }");
        Accumulator acc = Accumulator.of();
        acc = max.addInput(acc, element(0L, "longField", 5L));

        final Accumulator merged = max.mergeAccumulator(Accumulator.of(), acc);
        Assertions.assertEquals(5L, max.extractOutput(merged, new HashMap<>()));

        final Accumulator merged2 = max.mergeAccumulator(merged, Accumulator.of());
        Assertions.assertEquals(5L, max.extractOutput(merged2, new HashMap<>()));
    }

    @Test
    public void testValidate() {
        final AggregateFunction noName = create("{ \"op\": \"max\", \"field\": \"longField\" }");
        Assertions.assertEquals(1, noName.validate(0, 0).size());

        final AggregateFunction valid = create("{ \"name\": \"m\", \"op\": \"max\", \"field\": \"longField\" }");
        Assertions.assertTrue(valid.validate(0, 0).isEmpty());
    }

    @Test
    public void testConditionFilter() {
        final AggregateFunction max = create("""
                { "name": "m", "op": "max", "field": "longField", "condition": { "key": "stringField", "op": "=", "value": "ok" } }
                """);
        Assertions.assertTrue(max.filter(element(0L, "stringField", "ok", "longField", 1L)));
        Assertions.assertFalse(max.filter(element(0L, "stringField", "ng", "longField", 1L)));
    }

}
