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

public class SumTest {

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
    public void testSumLongField() {
        final AggregateFunction sum = create("{ \"name\": \"s\", \"op\": \"sum\", \"field\": \"longField\" }");
        Accumulator accumulator = Accumulator.of();
        accumulator = sum.addInput(accumulator, element(0L, "longField", 1L));
        accumulator = sum.addInput(accumulator, element(1L, "longField", 2L));
        accumulator = sum.addInput(accumulator, element(2L, "longField", 3L));
        Assertions.assertEquals(6L, sum.extractOutput(accumulator, new HashMap<>()));
    }

    @Test
    public void testSumDoubleField() {
        final AggregateFunction sum = create("{ \"name\": \"s\", \"op\": \"sum\", \"field\": \"doubleField\" }");
        Accumulator accumulator = Accumulator.of();
        accumulator = sum.addInput(accumulator, element(0L, "doubleField", 1.5D));
        accumulator = sum.addInput(accumulator, element(1L, "doubleField", 2.25D));
        Assertions.assertEquals(3.75D, (Double) sum.extractOutput(accumulator, new HashMap<>()), DELTA);
    }

    @Test
    public void testSumStringFieldConcatenates() {
        final AggregateFunction sum = create("{ \"name\": \"s\", \"op\": \"sum\", \"field\": \"stringField\" }");
        Accumulator accumulator = Accumulator.of();
        accumulator = sum.addInput(accumulator, element(0L, "stringField", "a"));
        accumulator = sum.addInput(accumulator, element(1L, "stringField", "b"));
        Assertions.assertEquals("ab", sum.extractOutput(accumulator, new HashMap<>()));
    }

    @Test
    public void testSumExpression() {
        final AggregateFunction sum = create("{ \"name\": \"s\", \"op\": \"sum\", \"expression\": \"longField * doubleField\" }");
        Accumulator accumulator = Accumulator.of();
        accumulator = sum.addInput(accumulator, element(0L, "longField", 2L, "doubleField", 1.5D));
        accumulator = sum.addInput(accumulator, element(1L, "longField", 3L, "doubleField", 2.0D));
        Assertions.assertEquals(9.0D, (Double) sum.extractOutput(accumulator, new HashMap<>()), DELTA);
    }

    @Test
    public void testNullValuesIgnored() {
        final AggregateFunction sum = create("{ \"name\": \"s\", \"op\": \"sum\", \"field\": \"longField\" }");
        Accumulator accumulator = Accumulator.of();
        accumulator = sum.addInput(accumulator, element(0L, "longField", 1L));
        accumulator = sum.addInput(accumulator, element(1L, "longField", null));
        accumulator = sum.addInput(accumulator, element(2L, "longField", 4L));
        Assertions.assertEquals(5L, sum.extractOutput(accumulator, new HashMap<>()));
    }

    @Test
    public void testExtractOutputEmptyReturnsTypedZero() {
        final AggregateFunction sumLong = create("{ \"name\": \"s\", \"op\": \"sum\", \"field\": \"longField\" }");
        Assertions.assertEquals(0L, sumLong.extractOutput(Accumulator.of(), new HashMap<>()));

        final AggregateFunction sumDouble = create("{ \"name\": \"s\", \"op\": \"sum\", \"field\": \"doubleField\" }");
        Assertions.assertEquals(0.0D, sumDouble.extractOutput(Accumulator.of(), new HashMap<>()));

        // expression based sum output type is float64
        final AggregateFunction sumExp = create("{ \"name\": \"s\", \"op\": \"sum\", \"expression\": \"longField + 1\" }");
        Assertions.assertEquals(0.0D, sumExp.extractOutput(Accumulator.of(), new HashMap<>()));
    }

    @Test
    public void testMergeAccumulator() {
        final AggregateFunction sum = create("{ \"name\": \"s\", \"op\": \"sum\", \"field\": \"longField\" }");
        Accumulator acc1 = Accumulator.of();
        acc1 = sum.addInput(acc1, element(0L, "longField", 1L));
        acc1 = sum.addInput(acc1, element(1L, "longField", 2L));

        Accumulator acc2 = Accumulator.of();
        acc2 = sum.addInput(acc2, element(2L, "longField", 10L));

        final Accumulator merged = sum.mergeAccumulator(acc1, acc2);
        Assertions.assertEquals(13L, sum.extractOutput(merged, new HashMap<>()));
    }

    @Test
    public void testMergeAccumulatorWithEmpty() {
        final AggregateFunction sum = create("{ \"name\": \"s\", \"op\": \"sum\", \"field\": \"longField\" }");
        Accumulator acc = Accumulator.of();
        acc = sum.addInput(acc, element(0L, "longField", 7L));

        final Accumulator merged = sum.mergeAccumulator(acc, Accumulator.of());
        Assertions.assertEquals(7L, sum.extractOutput(merged, new HashMap<>()));

        final Accumulator merged2 = sum.mergeAccumulator(Accumulator.of(), merged);
        Assertions.assertEquals(7L, sum.extractOutput(merged2, new HashMap<>()));
    }

    @Test
    public void testValidate() {
        final AggregateFunction noName = create("{ \"op\": \"sum\", \"field\": \"longField\" }");
        Assertions.assertEquals(1, noName.validate(0, 0).size());

        final AggregateFunction valid = create("{ \"name\": \"s\", \"op\": \"sum\", \"field\": \"longField\" }");
        Assertions.assertTrue(valid.validate(0, 0).isEmpty());
    }

    @Test
    public void testOutputFieldType() {
        final AggregateFunction sumLong = create("{ \"name\": \"s\", \"op\": \"sum\", \"field\": \"longField\" }");
        Assertions.assertEquals(Schema.Type.int64, sumLong.getOutputFieldType().getType());

        final AggregateFunction sumExp = create("{ \"name\": \"s\", \"op\": \"sum\", \"expression\": \"longField + 1\" }");
        Assertions.assertEquals(Schema.Type.float64, sumExp.getOutputFieldType().getType());
    }

}
