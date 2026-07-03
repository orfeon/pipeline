package com.mercari.solution.util.pipeline;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.aggregation.Accumulator;
import com.mercari.solution.util.pipeline.aggregation.Aggregators;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AggregationTest {

    private static Schema createSchema() {
        return Schema.of(List.of(
                Schema.Field.of("stringField", Schema.FieldType.STRING),
                Schema.Field.of("longField", Schema.FieldType.INT64)));
    }

    private static Aggregators createAggregators() {
        final String fieldsJson = """
                [
                  { "name": "count", "op": "count" },
                  { "name": "sumLong", "op": "sum", "field": "longField" },
                  { "name": "maxLong", "op": "max", "field": "longField" }
                ]
                """;
        final JsonArray fields = new Gson().fromJson(fieldsJson, JsonArray.class);
        return Aggregators.of("input1", List.of("stringField"), createSchema(), fields);
    }

    private static MElement element(String key, long value, long epochMillis) {
        final Map<String, Object> values = new HashMap<>();
        values.put("stringField", key);
        values.put("longField", value);
        return MElement.of(values, Instant.ofEpochMilli(epochMillis));
    }

    @Test
    public void testCreateOutputSchema() {
        final Aggregators aggregators = createAggregators();
        final Schema outputSchema = Aggregation.createOutputSchema(
                Map.of("input1", createSchema()), List.of("stringField"), List.of(aggregators));

        Assertions.assertTrue(outputSchema.hasField("stringField"));
        Assertions.assertTrue(outputSchema.hasField("count"));
        Assertions.assertTrue(outputSchema.hasField("sumLong"));
        Assertions.assertTrue(outputSchema.hasField("maxLong"));
    }

    @Test
    public void testAggregationCombineFn() {
        final Aggregators aggregators = createAggregators();
        final Aggregation.AggregationCombineFn combineFn = Aggregation.AggregationCombineFn
                .combine(List.of("input1"), List.of("stringField"), List.of(aggregators));

        Accumulator accumulator = combineFn.createAccumulator();
        accumulator = combineFn.addInput(accumulator, element("a", 1L, 1000L));
        accumulator = combineFn.addInput(accumulator, element("a", 4L, 2000L));

        final Map<String, Object> output = combineFn.extractOutput(accumulator);
        Assertions.assertEquals("a", output.get("stringField"));
        Assertions.assertEquals(2L, ((Number) output.get("count")).longValue());
        Assertions.assertEquals(5L, ((Number) output.get("sumLong")).longValue());
        Assertions.assertEquals(4L, ((Number) output.get("maxLong")).longValue());
    }

    @Test
    public void testMergeAccumulators() {
        final Aggregators aggregators = createAggregators();
        final Aggregation.AggregationCombineFn combineFn = Aggregation.AggregationCombineFn
                .combine(List.of("input1"), List.of("stringField"), List.of(aggregators));

        Accumulator accumulator1 = combineFn.createAccumulator();
        accumulator1 = combineFn.addInput(accumulator1, element("a", 1L, 1000L));

        Accumulator accumulator2 = combineFn.createAccumulator();
        accumulator2 = combineFn.addInput(accumulator2, element("a", 9L, 2000L));

        final Accumulator merged = combineFn.mergeAccumulators(List.of(accumulator1, accumulator2));
        final Map<String, Object> output = combineFn.extractOutput(merged);
        Assertions.assertEquals(2L, ((Number) output.get("count")).longValue());
        Assertions.assertEquals(10L, ((Number) output.get("sumLong")).longValue());
        Assertions.assertEquals(9L, ((Number) output.get("maxLong")).longValue());
    }

    @Test
    public void testExtractOutputEmpty() {
        final Aggregators aggregators = createAggregators();
        final Aggregation.AggregationCombineFn combineFn = Aggregation.AggregationCombineFn
                .combine(List.of("input1"), List.of("stringField"), List.of(aggregators));

        Assertions.assertTrue(combineFn.extractOutput(null).isEmpty());
        Assertions.assertTrue(combineFn.extractOutput(combineFn.createAccumulator()).isEmpty());
    }

}
