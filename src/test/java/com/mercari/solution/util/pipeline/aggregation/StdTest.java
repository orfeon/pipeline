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

public class StdTest {

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

    private static Double output(final AggregateFunction fn, final Accumulator accumulator) {
        return (Double) fn.extractOutput(accumulator, new HashMap<>());
    }

    private static Accumulator addAll(final AggregateFunction fn, final Accumulator accumulator, final double... values) {
        Accumulator acc = accumulator;
        for (int i = 0; i < values.length; i++) {
            acc = fn.addInput(acc, element(i, "doubleField", values[i]));
        }
        return acc;
    }

    @Test
    public void testSampleStd() {
        final AggregateFunction std = create("{ \"name\": \"s\", \"op\": \"std\", \"field\": \"doubleField\" }");
        final Accumulator accumulator = addAll(std, Accumulator.of(), 1D, 2D, 3D, 4D);
        // mean 2.5, M2 = 5, sample std = sqrt(5/3)
        Assertions.assertEquals(Math.sqrt(5D / 3D), output(std, accumulator), DELTA);
    }

    @Test
    public void testPopulationStdWithDdofZero() {
        final AggregateFunction std = create("{ \"name\": \"s\", \"op\": \"std\", \"field\": \"doubleField\", \"ddof\": 0 }");
        final Accumulator accumulator = addAll(std, Accumulator.of(), 1D, 2D, 3D, 4D);
        // population std = sqrt(5/4)
        Assertions.assertEquals(Math.sqrt(5D / 4D), output(std, accumulator), DELTA);
    }

    @Test
    public void testSingleElementSampleStdIsNull() {
        final AggregateFunction std = create("{ \"name\": \"s\", \"op\": \"std\", \"field\": \"doubleField\" }");
        final Accumulator accumulator = addAll(std, Accumulator.of(), 5D);
        // weight - ddof == 0 for a single element with default ddof=1
        Assertions.assertNull(output(std, accumulator));
    }

    @Test
    public void testSingleElementPopulationStdIsZero() {
        final AggregateFunction std = create("{ \"name\": \"s\", \"op\": \"std\", \"field\": \"doubleField\", \"ddof\": 0 }");
        final Accumulator accumulator = addAll(std, Accumulator.of(), 5D);
        Assertions.assertEquals(0D, output(std, accumulator), DELTA);
    }

    @Test
    public void testExtractOutputEmpty() {
        final AggregateFunction std = create("{ \"name\": \"s\", \"op\": \"std\", \"field\": \"doubleField\" }");
        Assertions.assertNull(output(std, Accumulator.of()));
    }

    @Test
    public void testNullValuesIgnored() {
        final AggregateFunction std = create("{ \"name\": \"s\", \"op\": \"std\", \"field\": \"doubleField\" }");
        Accumulator accumulator = Accumulator.of();
        accumulator = std.addInput(accumulator, element(0L, "doubleField", 2D));
        accumulator = std.addInput(accumulator, element(1L, "doubleField", null));
        accumulator = std.addInput(accumulator, element(2L, "doubleField", 4D));
        // values {2, 4}: M2 = 2, sample std = sqrt(2)
        Assertions.assertEquals(Math.sqrt(2D), output(std, accumulator), DELTA);
    }

    @Test
    public void testExpressionStd() {
        final AggregateFunction std = create("{ \"name\": \"s\", \"op\": \"std\", \"expression\": \"doubleField * 2\" }");
        final Accumulator accumulator = addAll(std, Accumulator.of(), 1D, 2D, 3D);
        // values {2, 4, 6}: M2 = 8, sample std = sqrt(8/2) = 2
        Assertions.assertEquals(2D, output(std, accumulator), DELTA);
    }

    @Test
    public void testMergeAccumulators() {
        final AggregateFunction std = create("{ \"name\": \"s\", \"op\": \"std\", \"field\": \"doubleField\" }");
        // group1 = {1,2,3}, group2 = {4,5,6,7}; combined {1..7}: mean 4, M2 = 28
        final Accumulator acc1 = addAll(std, Accumulator.of(), 1D, 2D, 3D);
        final Accumulator acc2 = addAll(std, Accumulator.of(), 4D, 5D, 6D, 7D);
        final Accumulator merged = std.mergeAccumulator(acc1, acc2);
        Assertions.assertEquals(Math.sqrt(28D / 6D), output(std, merged), DELTA);
    }

    @Test
    public void testMergeMatchesSequential() {
        final AggregateFunction std = create("{ \"name\": \"s\", \"op\": \"std\", \"field\": \"doubleField\" }");
        // groups with distinct means: the merged variance must include the between-group term
        final Accumulator sequential = addAll(std, Accumulator.of(), 0D, 0D, 10D, 10D);
        final Accumulator acc1 = addAll(std, Accumulator.of(), 0D, 0D);
        final Accumulator acc2 = addAll(std, Accumulator.of(), 10D, 10D);
        final Accumulator merged = std.mergeAccumulator(acc1, acc2);

        // hand computed: M2 of {0,0,10,10} = 100, sample std = sqrt(100/3)
        Assertions.assertEquals(Math.sqrt(100D / 3D), output(std, sequential), DELTA);
        Assertions.assertEquals(Math.sqrt(100D / 3D), output(std, merged), DELTA);
    }

    @Test
    public void testMergeWithEmptyAccumulator() {
        final AggregateFunction std = create("{ \"name\": \"s\", \"op\": \"std\", \"field\": \"doubleField\" }");
        final Accumulator acc = addAll(std, Accumulator.of(), 1D, 2D, 3D);

        final Accumulator merged = std.mergeAccumulator(Accumulator.of(), acc);
        Assertions.assertEquals(1D, output(std, merged), DELTA);

        final Accumulator merged2 = std.mergeAccumulator(merged, Accumulator.of());
        Assertions.assertEquals(1D, output(std, merged2), DELTA);
    }

    @Test
    public void testMergeWithSingleElementAccumulator() {
        final AggregateFunction std = create("{ \"name\": \"s\", \"op\": \"std\", \"field\": \"doubleField\" }");
        // {1,2,3} + {10}: mean 4, M2 = 9+4+1+36 = 50
        final Accumulator acc1 = addAll(std, Accumulator.of(), 1D, 2D, 3D);
        final Accumulator acc2 = addAll(std, Accumulator.of(), 10D);
        final Accumulator merged = std.mergeAccumulator(acc1, acc2);
        Assertions.assertEquals(Math.sqrt(50D / 3D), output(std, merged), DELTA);

        // single element accumulator as base
        final Accumulator acc3 = addAll(std, Accumulator.of(), 10D);
        final Accumulator acc4 = addAll(std, Accumulator.of(), 1D, 2D, 3D);
        final Accumulator merged2 = std.mergeAccumulator(acc3, acc4);
        Assertions.assertEquals(Math.sqrt(50D / 3D), output(std, merged2), DELTA);
    }

    @Test
    public void testWeightedStdMatchesRepeatedValues() {
        // {1 (w=1), 2 (w=2), 3 (w=1)} must equal {1, 2, 2, 3}
        final AggregateFunction plain = create("{ \"name\": \"s\", \"op\": \"std\", \"field\": \"doubleField\" }");
        final Accumulator plainAcc = addAll(plain, Accumulator.of(), 1D, 2D, 2D, 3D);

        final AggregateFunction weighted = create("{ \"name\": \"s\", \"op\": \"std\", \"field\": \"doubleField\", \"weightField\": \"weightField\" }");
        Accumulator weightedAcc = Accumulator.of();
        weightedAcc = weighted.addInput(weightedAcc, element(0L, "doubleField", 1D, "weightField", 1D));
        weightedAcc = weighted.addInput(weightedAcc, element(1L, "doubleField", 2D, "weightField", 2D));
        weightedAcc = weighted.addInput(weightedAcc, element(2L, "doubleField", 3D, "weightField", 1D));

        // mean 2, M2 = 2, weight 4 -> sqrt(2/3)
        Assertions.assertEquals(Math.sqrt(2D / 3D), output(plain, plainAcc), DELTA);
        Assertions.assertEquals(output(plain, plainAcc), output(weighted, weightedAcc), DELTA);
    }

    @Test
    public void testNegativeWeightUsesAbsoluteValue() {
        final AggregateFunction weighted = create("{ \"name\": \"s\", \"op\": \"std\", \"field\": \"doubleField\", \"weightField\": \"weightField\" }");
        Accumulator negAcc = Accumulator.of();
        negAcc = weighted.addInput(negAcc, element(0L, "doubleField", 1D, "weightField", -1D));
        negAcc = weighted.addInput(negAcc, element(1L, "doubleField", 3D, "weightField", 1D));

        Accumulator posAcc = Accumulator.of();
        posAcc = weighted.addInput(posAcc, element(0L, "doubleField", 1D, "weightField", 1D));
        posAcc = weighted.addInput(posAcc, element(1L, "doubleField", 3D, "weightField", 1D));

        Assertions.assertEquals(output(weighted, posAcc), output(weighted, negAcc), DELTA);
    }

    @Test
    public void testOutputFieldType() {
        final AggregateFunction std = create("{ \"name\": \"s\", \"op\": \"std\", \"field\": \"doubleField\" }");
        Assertions.assertEquals(Schema.Type.float64, std.getOutputFieldType().getType());
    }

    @Test
    public void testFrequencyWeightedStdReferenceValue() {
        // frequency weights semantics: variance = M2 / (sumWeights - ddof)
        // values {1 (w=3), 5 (w=2)}: mean = (3*1 + 2*5)/5 = 2.6,
        // M2 = 3*(1-2.6)^2 + 2*(5-2.6)^2 = 7.68 + 11.52 = 19.2
        final AggregateFunction sample = create("{ \"name\": \"s\", \"op\": \"std\", \"field\": \"doubleField\", \"weightField\": \"weightField\" }");
        Accumulator sampleAcc = Accumulator.of();
        sampleAcc = sample.addInput(sampleAcc, element(0L, "doubleField", 1D, "weightField", 3D));
        sampleAcc = sample.addInput(sampleAcc, element(1L, "doubleField", 5D, "weightField", 2D));
        Assertions.assertEquals(Math.sqrt(19.2D / 4D), output(sample, sampleAcc), DELTA);

        final AggregateFunction population = create("{ \"name\": \"s\", \"op\": \"std\", \"field\": \"doubleField\", \"weightField\": \"weightField\", \"ddof\": 0 }");
        Accumulator populationAcc = Accumulator.of();
        populationAcc = population.addInput(populationAcc, element(0L, "doubleField", 1D, "weightField", 3D));
        populationAcc = population.addInput(populationAcc, element(1L, "doubleField", 5D, "weightField", 2D));
        Assertions.assertEquals(Math.sqrt(19.2D / 5D), output(population, populationAcc), DELTA);
    }

    @Test
    public void testValidate() {
        final AggregateFunction valid = create("{ \"name\": \"s\", \"op\": \"std\", \"field\": \"doubleField\" }");
        Assertions.assertTrue(valid.validate(0, 0).isEmpty());

        // neither field nor expression: must be reported as a validation error
        final AggregateFunction invalid = create("{ \"name\": \"s\", \"op\": \"std\" }");
        final List<String> errorMessages = invalid.validate(1, 2);
        Assertions.assertEquals(1, errorMessages.size());
        Assertions.assertEquals("aggregations[1].fields[2].field or expression must not be null", errorMessages.get(0));
    }

}
