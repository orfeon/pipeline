package com.mercari.solution.util.pipeline.aggregation;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ArrayAggTest {

    private static final List<Schema.Field> INPUT_FIELDS = List.of(
            Schema.Field.of("stringField", Schema.FieldType.STRING),
            Schema.Field.of("longField", Schema.FieldType.INT64));

    private static AggregateFunction create(final String json) {
        final AggregateFunction fn = AggregateFunction.of(new Gson().fromJson(json, JsonElement.class), INPUT_FIELDS);
        fn.setup();
        return fn;
    }

    private static MElement element(final long epochMillis, final String s, final Long l) {
        final Map<String, Object> values = new HashMap<>();
        values.put("stringField", s);
        values.put("longField", l);
        return MElement.of(values, epochMillis);
    }

    @Test
    public void testSingleField() {
        final AggregateFunction agg = create("{ \"name\": \"a\", \"op\": \"array_agg\", \"field\": \"longField\" }");
        Accumulator accumulator = Accumulator.of();
        accumulator = agg.addInput(accumulator, element(0L, "a", 1L));
        accumulator = agg.addInput(accumulator, element(1L, "b", 2L));
        accumulator = agg.addInput(accumulator, element(2L, "c", 3L));

        final Object output = agg.extractOutput(accumulator, new HashMap<>());
        Assertions.assertEquals(List.of(1L, 2L, 3L), output);
    }

    @Test
    public void testSingleFieldKeepsNulls() {
        final AggregateFunction agg = create("{ \"name\": \"a\", \"op\": \"array_agg\", \"field\": \"longField\" }");
        Accumulator accumulator = Accumulator.of();
        accumulator = agg.addInput(accumulator, element(0L, "a", 1L));
        accumulator = agg.addInput(accumulator, element(1L, "b", null));
        accumulator = agg.addInput(accumulator, element(2L, "c", 3L));

        final List<?> output = (List<?>) agg.extractOutput(accumulator, new HashMap<>());
        Assertions.assertEquals(3, output.size());
        Assertions.assertEquals(1L, output.get(0));
        Assertions.assertNull(output.get(1));
        Assertions.assertEquals(3L, output.get(2));
    }

    @Test
    public void testMultiFields() {
        final AggregateFunction agg = create("{ \"name\": \"a\", \"op\": \"array_agg\", \"fields\": [\"stringField\", \"longField\"] }");
        Accumulator accumulator = Accumulator.of();
        accumulator = agg.addInput(accumulator, element(0L, "a", 1L));
        accumulator = agg.addInput(accumulator, element(1L, "b", 2L));

        final List<?> output = (List<?>) agg.extractOutput(accumulator, new HashMap<>());
        Assertions.assertEquals(2, output.size());
        final Map<String, Object> first = (Map<String, Object>) output.get(0);
        Assertions.assertEquals("a", first.get("stringField"));
        Assertions.assertEquals(1L, first.get("longField"));
        final Map<String, Object> second = (Map<String, Object>) output.get(1);
        Assertions.assertEquals("b", second.get("stringField"));
        Assertions.assertEquals(2L, second.get("longField"));
    }

    @Test
    public void testMergeAccumulator() {
        final AggregateFunction agg = create("{ \"name\": \"a\", \"op\": \"array_agg\", \"field\": \"longField\" }");
        Accumulator acc1 = Accumulator.of();
        acc1 = agg.addInput(acc1, element(0L, "a", 1L));
        acc1 = agg.addInput(acc1, element(1L, "b", 2L));

        Accumulator acc2 = Accumulator.of();
        acc2 = agg.addInput(acc2, element(2L, "c", 3L));

        final Accumulator merged = agg.mergeAccumulator(acc1, acc2);
        Assertions.assertEquals(List.of(1L, 2L, 3L), agg.extractOutput(merged, new HashMap<>()));
    }

    @Test
    public void testMergeAccumulatorWithEmpty() {
        final AggregateFunction agg = create("{ \"name\": \"a\", \"op\": \"array_agg\", \"field\": \"longField\" }");
        Accumulator acc = Accumulator.of();
        acc = agg.addInput(acc, element(0L, "a", 1L));

        final Accumulator merged = agg.mergeAccumulator(Accumulator.of(), acc);
        Assertions.assertEquals(List.of(1L), agg.extractOutput(merged, new HashMap<>()));

        final Accumulator merged2 = agg.mergeAccumulator(merged, Accumulator.of());
        Assertions.assertEquals(List.of(1L), agg.extractOutput(merged2, new HashMap<>()));
    }

    @Test
    public void testExtractOutputEmpty() {
        final AggregateFunction agg = create("{ \"name\": \"a\", \"op\": \"array_agg\", \"field\": \"longField\" }");
        Assertions.assertEquals(List.of(), agg.extractOutput(Accumulator.of(), new HashMap<>()));
    }

    @Test
    public void testRequiresFieldOrFields() {
        Assertions.assertThrows(
                IllegalModuleException.class,
                () -> create("{ \"name\": \"a\", \"op\": \"array_agg\" }"));
    }

    @Test
    public void testOutputFieldType() {
        final AggregateFunction single = create("{ \"name\": \"a\", \"op\": \"array_agg\", \"field\": \"longField\" }");
        Assertions.assertEquals(Schema.Type.array, single.getOutputFieldType().getType());
        Assertions.assertEquals(Schema.Type.int64, single.getOutputFieldType().getArrayValueType().getType());

        final AggregateFunction multi = create("{ \"name\": \"a\", \"op\": \"array_agg\", \"fields\": [\"stringField\", \"longField\"] }");
        Assertions.assertEquals(Schema.Type.array, multi.getOutputFieldType().getType());
        Assertions.assertEquals(Schema.Type.element, multi.getOutputFieldType().getArrayValueType().getType());
    }

}
