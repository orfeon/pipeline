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

public class CountTest {

    private static final List<Schema.Field> INPUT_FIELDS = List.of(
            Schema.Field.of("stringField", Schema.FieldType.STRING),
            Schema.Field.of("longField", Schema.FieldType.INT64));

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
    public void testAddInputAndExtractOutput() {
        final AggregateFunction count = create("{ \"name\": \"c\", \"op\": \"count\" }");
        Accumulator accumulator = Accumulator.of();
        accumulator = count.addInput(accumulator, element(0L, "longField", 1L));
        accumulator = count.addInput(accumulator, element(1L, "longField", 2L));
        accumulator = count.addInput(accumulator, element(2L, "longField", 3L));
        Assertions.assertEquals(3L, count.extractOutput(accumulator, new HashMap<>()));
    }

    @Test
    public void testExtractOutputEmpty() {
        final AggregateFunction count = create("{ \"name\": \"c\", \"op\": \"count\" }");
        Assertions.assertEquals(0L, count.extractOutput(Accumulator.of(), new HashMap<>()));
    }

    @Test
    public void testMergeAccumulator() {
        final AggregateFunction count = create("{ \"name\": \"c\", \"op\": \"count\" }");
        Accumulator acc1 = Accumulator.of();
        acc1 = count.addInput(acc1, element(0L));
        acc1 = count.addInput(acc1, element(1L));

        Accumulator acc2 = Accumulator.of();
        acc2 = count.addInput(acc2, element(2L));
        acc2 = count.addInput(acc2, element(3L));
        acc2 = count.addInput(acc2, element(4L));

        final Accumulator merged = count.mergeAccumulator(acc1, acc2);
        Assertions.assertEquals(5L, count.extractOutput(merged, new HashMap<>()));
    }

    @Test
    public void testMergeAccumulatorWithEmpty() {
        final AggregateFunction count = create("{ \"name\": \"c\", \"op\": \"count\" }");
        Accumulator acc = Accumulator.of();
        acc = count.addInput(acc, element(0L));

        final Accumulator merged = count.mergeAccumulator(acc, Accumulator.of());
        Assertions.assertEquals(1L, count.extractOutput(merged, new HashMap<>()));

        final Accumulator merged2 = count.mergeAccumulator(Accumulator.of(), merged);
        Assertions.assertEquals(1L, count.extractOutput(merged2, new HashMap<>()));
    }

    @Test
    public void testValidate() {
        final AggregateFunction valid = create("{ \"name\": \"c\", \"op\": \"count\" }");
        Assertions.assertTrue(valid.validate(0, 0).isEmpty());

        final AggregateFunction noName = create("{ \"op\": \"count\" }");
        final List<String> errors = noName.validate(1, 2);
        Assertions.assertEquals(1, errors.size());
        Assertions.assertTrue(errors.getFirst().contains("aggregations[1].fields[2].name"));
    }

    @Test
    public void testConditionFilter() {
        final AggregateFunction count = create("""
                { "name": "c", "op": "count", "condition": { "key": "longField", "op": ">", "value": 2 } }
                """);
        Assertions.assertTrue(count.filter(element(0L, "longField", 3L)));
        Assertions.assertFalse(count.filter(element(0L, "longField", 1L)));
    }

    @Test
    public void testFilterWithoutCondition() {
        final AggregateFunction count = create("{ \"name\": \"c\", \"op\": \"count\" }");
        Assertions.assertTrue(count.filter(element(0L, "longField", 1L)));
    }

    @Test
    public void testIgnore() {
        final AggregateFunction ignored = create("{ \"name\": \"c\", \"op\": \"count\", \"ignore\": true }");
        Assertions.assertTrue(ignored.ignore());
        final AggregateFunction notIgnored = create("{ \"name\": \"c\", \"op\": \"count\" }");
        Assertions.assertFalse(notIgnored.ignore());
    }

    @Test
    public void testOutputFieldType() {
        final AggregateFunction count = create("{ \"name\": \"c\", \"op\": \"count\" }");
        Assertions.assertEquals(Schema.Type.int64, count.getOutputFieldType().getType());
    }

}
