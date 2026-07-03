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

public class LastTest {

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
    public void testLastSingleField() {
        final AggregateFunction last = create("{ \"name\": \"l\", \"op\": \"last\", \"field\": \"stringField\" }");
        Accumulator accumulator = Accumulator.of();
        // added out of timestamp order
        accumulator = last.addInput(accumulator, element(1000L, "a", 1L));
        accumulator = last.addInput(accumulator, element(3000L, "c", 3L));
        accumulator = last.addInput(accumulator, element(2000L, "b", 2L));
        Assertions.assertEquals("c", last.extractOutput(accumulator, new HashMap<>()));
    }

    @Test
    public void testFirstSingleField() {
        final AggregateFunction first = create("{ \"name\": \"f\", \"op\": \"first\", \"field\": \"stringField\" }");
        Accumulator accumulator = Accumulator.of();
        accumulator = first.addInput(accumulator, element(2000L, "b", 2L));
        accumulator = first.addInput(accumulator, element(1000L, "a", 1L));
        accumulator = first.addInput(accumulator, element(3000L, "c", 3L));
        Assertions.assertEquals("a", first.extractOutput(accumulator, new HashMap<>()));
    }

    @Test
    public void testLastMultiFields() {
        final AggregateFunction last = create("{ \"name\": \"l\", \"op\": \"last\", \"fields\": [\"stringField\", \"longField\"] }");
        Accumulator accumulator = Accumulator.of();
        accumulator = last.addInput(accumulator, element(1000L, "a", 1L));
        accumulator = last.addInput(accumulator, element(2000L, "b", 2L));

        final Object output = last.extractOutput(accumulator, new HashMap<>());
        Assertions.assertInstanceOf(Map.class, output);
        final Map<String, Object> map = (Map<String, Object>) output;
        Assertions.assertEquals("b", map.get("stringField"));
        Assertions.assertEquals(2L, map.get("longField"));
    }

    @Test
    public void testMergeAccumulatorLast() {
        final AggregateFunction last = create("{ \"name\": \"l\", \"op\": \"last\", \"field\": \"stringField\" }");
        Accumulator acc1 = Accumulator.of();
        acc1 = last.addInput(acc1, element(1000L, "a", 1L));
        acc1 = last.addInput(acc1, element(2000L, "b", 2L));

        Accumulator acc2 = Accumulator.of();
        acc2 = last.addInput(acc2, element(3000L, "c", 3L));

        final Accumulator merged = last.mergeAccumulator(acc1, acc2);
        Assertions.assertEquals("c", last.extractOutput(merged, new HashMap<>()));
    }

    @Test
    public void testMergeAccumulatorFirst() {
        final AggregateFunction first = create("{ \"name\": \"f\", \"op\": \"first\", \"field\": \"stringField\" }");
        Accumulator acc1 = Accumulator.of();
        acc1 = first.addInput(acc1, element(2000L, "b", 2L));

        Accumulator acc2 = Accumulator.of();
        acc2 = first.addInput(acc2, element(1000L, "a", 1L));

        final Accumulator merged = first.mergeAccumulator(acc1, acc2);
        Assertions.assertEquals("a", first.extractOutput(merged, new HashMap<>()));
    }

    @Test
    public void testMergeAccumulatorWithEmpty() {
        final AggregateFunction last = create("{ \"name\": \"l\", \"op\": \"last\", \"field\": \"stringField\" }");
        Accumulator acc = Accumulator.of();
        acc = last.addInput(acc, element(1000L, "a", 1L));

        final Accumulator merged = last.mergeAccumulator(Accumulator.of(), acc);
        Assertions.assertEquals("a", last.extractOutput(merged, new HashMap<>()));

        final Accumulator merged2 = last.mergeAccumulator(merged, Accumulator.of());
        Assertions.assertEquals("a", last.extractOutput(merged2, new HashMap<>()));
    }

    @Test
    public void testLastNullValueKept() {
        // a later element with a null field value should still win
        final AggregateFunction last = create("{ \"name\": \"l\", \"op\": \"last\", \"field\": \"stringField\" }");
        Accumulator accumulator = Accumulator.of();
        accumulator = last.addInput(accumulator, element(1000L, "a", 1L));
        accumulator = last.addInput(accumulator, element(2000L, null, 2L));
        Assertions.assertNull(last.extractOutput(accumulator, new HashMap<>()));
    }

    @Test
    public void testValidate() {
        final AggregateFunction noNameNoFields = create("{ \"op\": \"last\" }");
        final List<String> errors = noNameNoFields.validate(0, 0);
        Assertions.assertEquals(2, errors.size());

        final AggregateFunction valid = create("{ \"name\": \"l\", \"op\": \"last\", \"field\": \"stringField\" }");
        Assertions.assertTrue(valid.validate(0, 0).isEmpty());
    }

    @Test
    public void testOutputFieldType() {
        final AggregateFunction single = create("{ \"name\": \"l\", \"op\": \"last\", \"field\": \"longField\" }");
        Assertions.assertEquals(Schema.Type.int64, single.getOutputFieldType().getType());

        final AggregateFunction multi = create("{ \"name\": \"l\", \"op\": \"last\", \"fields\": [\"stringField\", \"longField\"] }");
        Assertions.assertEquals(Schema.Type.element, multi.getOutputFieldType().getType());
    }

}
