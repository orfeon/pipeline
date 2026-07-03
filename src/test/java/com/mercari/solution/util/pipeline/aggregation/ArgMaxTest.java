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

public class ArgMaxTest {

    private static final List<Schema.Field> INPUT_FIELDS = List.of(
            Schema.Field.of("stringField", Schema.FieldType.STRING),
            Schema.Field.of("longField", Schema.FieldType.INT64),
            Schema.Field.of("doubleField", Schema.FieldType.FLOAT64));

    private static AggregateFunction create(final String json) {
        final AggregateFunction fn = AggregateFunction.of(new Gson().fromJson(json, JsonElement.class), INPUT_FIELDS);
        fn.setup();
        return fn;
    }

    private static MElement element(final long epochMillis, final String s, final Long l, final Double d) {
        final Map<String, Object> values = new HashMap<>();
        values.put("stringField", s);
        values.put("longField", l);
        values.put("doubleField", d);
        return MElement.of(values, epochMillis);
    }

    @Test
    public void testArgMaxSingleField() {
        final AggregateFunction argmax = create("{ \"name\": \"a\", \"op\": \"arg_max\", \"field\": \"stringField\", \"comparingField\": \"longField\" }");
        Accumulator accumulator = Accumulator.of();
        accumulator = argmax.addInput(accumulator, element(0L, "a", 1L, 0.5D));
        accumulator = argmax.addInput(accumulator, element(1L, "c", 3L, 1.5D));
        accumulator = argmax.addInput(accumulator, element(2L, "b", 2L, 2.5D));
        Assertions.assertEquals("c", argmax.extractOutput(accumulator, new HashMap<>()));
    }

    @Test
    public void testArgMinSingleField() {
        final AggregateFunction argmin = create("{ \"name\": \"a\", \"op\": \"arg_min\", \"field\": \"stringField\", \"comparingField\": \"longField\" }");
        Accumulator accumulator = Accumulator.of();
        accumulator = argmin.addInput(accumulator, element(0L, "a", 1L, 0.5D));
        accumulator = argmin.addInput(accumulator, element(1L, "c", 3L, 1.5D));
        accumulator = argmin.addInput(accumulator, element(2L, "b", 2L, 2.5D));
        Assertions.assertEquals("a", argmin.extractOutput(accumulator, new HashMap<>()));
    }

    @Test
    public void testArgMaxMultiFields() {
        final AggregateFunction argmax = create("{ \"name\": \"a\", \"op\": \"arg_max\", \"fields\": [\"stringField\", \"longField\"], \"comparingField\": \"doubleField\" }");
        Accumulator accumulator = Accumulator.of();
        accumulator = argmax.addInput(accumulator, element(0L, "a", 1L, 0.5D));
        accumulator = argmax.addInput(accumulator, element(1L, "b", 2L, 2.5D));
        accumulator = argmax.addInput(accumulator, element(2L, "c", 3L, 1.5D));

        final Object output = argmax.extractOutput(accumulator, new HashMap<>());
        Assertions.assertInstanceOf(Map.class, output);
        final Map<String, Object> map = (Map<String, Object>) output;
        Assertions.assertEquals("b", map.get("stringField"));
        Assertions.assertEquals(2L, map.get("longField"));
    }

    @Test
    public void testArgMaxComparingExpression() {
        // negated value flips the winner
        final AggregateFunction argmax = create("{ \"name\": \"a\", \"op\": \"arg_max\", \"field\": \"stringField\", \"comparingExpression\": \"longField * -1\" }");
        Accumulator accumulator = Accumulator.of();
        accumulator = argmax.addInput(accumulator, element(0L, "a", 1L, 0.5D));
        accumulator = argmax.addInput(accumulator, element(1L, "c", 3L, 1.5D));
        Assertions.assertEquals("a", argmax.extractOutput(accumulator, new HashMap<>()));
    }

    @Test
    public void testNullComparingValueIgnored() {
        final AggregateFunction argmax = create("{ \"name\": \"a\", \"op\": \"arg_max\", \"field\": \"stringField\", \"comparingField\": \"longField\" }");
        Accumulator accumulator = Accumulator.of();
        accumulator = argmax.addInput(accumulator, element(0L, "a", 1L, 0.5D));
        accumulator = argmax.addInput(accumulator, element(1L, "x", null, 1.5D));
        Assertions.assertEquals("a", argmax.extractOutput(accumulator, new HashMap<>()));
    }

    @Test
    public void testMergeAccumulator() {
        final AggregateFunction argmax = create("{ \"name\": \"a\", \"op\": \"arg_max\", \"field\": \"stringField\", \"comparingField\": \"longField\" }");
        Accumulator acc1 = Accumulator.of();
        acc1 = argmax.addInput(acc1, element(0L, "a", 1L, 0.5D));

        Accumulator acc2 = Accumulator.of();
        acc2 = argmax.addInput(acc2, element(1L, "b", 5L, 1.5D));

        final Accumulator merged = argmax.mergeAccumulator(acc1, acc2);
        Assertions.assertEquals("b", argmax.extractOutput(merged, new HashMap<>()));
    }

    @Test
    public void testMergeAccumulatorWithEmpty() {
        final AggregateFunction argmax = create("{ \"name\": \"a\", \"op\": \"arg_max\", \"field\": \"stringField\", \"comparingField\": \"longField\" }");
        Accumulator acc = Accumulator.of();
        acc = argmax.addInput(acc, element(0L, "a", 1L, 0.5D));

        final Accumulator merged = argmax.mergeAccumulator(Accumulator.of(), acc);
        Assertions.assertEquals("a", argmax.extractOutput(merged, new HashMap<>()));

        final Accumulator merged2 = argmax.mergeAccumulator(merged, Accumulator.of());
        Assertions.assertEquals("a", argmax.extractOutput(merged2, new HashMap<>()));
    }

    @Test
    public void testValidate() {
        final AggregateFunction invalid = create("{ \"op\": \"arg_max\" }");
        final List<String> errors = invalid.validate(0, 0);
        Assertions.assertEquals(3, errors.size());

        final AggregateFunction valid = create("{ \"name\": \"a\", \"op\": \"arg_max\", \"field\": \"stringField\", \"comparingField\": \"longField\" }");
        Assertions.assertTrue(valid.validate(0, 0).isEmpty());
    }

    @Test
    public void testOutputFieldType() {
        final AggregateFunction single = create("{ \"name\": \"a\", \"op\": \"arg_max\", \"field\": \"stringField\", \"comparingField\": \"longField\" }");
        Assertions.assertEquals(Schema.Type.string, single.getOutputFieldType().getType());

        final AggregateFunction multi = create("{ \"name\": \"a\", \"op\": \"arg_max\", \"fields\": [\"stringField\", \"longField\"], \"comparingField\": \"doubleField\" }");
        Assertions.assertEquals(Schema.Type.element, multi.getOutputFieldType().getType());
    }

}
