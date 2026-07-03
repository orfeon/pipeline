package com.mercari.solution.util.pipeline.aggregation;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.mercari.solution.module.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

public class AggregateFunctionTest {

    private static final List<Schema.Field> INPUT_FIELDS = List.of(
            Schema.Field.of("stringField", Schema.FieldType.STRING),
            Schema.Field.of("longField", Schema.FieldType.INT64),
            Schema.Field.of("doubleField", Schema.FieldType.FLOAT64));

    private static AggregateFunction create(final String json) {
        return AggregateFunction.of(new Gson().fromJson(json, JsonElement.class), INPUT_FIELDS);
    }

    @Test
    public void testOfReturnsNullForNonObject() {
        Assertions.assertNull(AggregateFunction.of(null, INPUT_FIELDS));
        Assertions.assertNull(create("null"));
        Assertions.assertNull(create("[]"));
    }

    @Test
    public void testOfRequiresOpOrFunc() {
        final IllegalArgumentException e = Assertions.assertThrows(
                IllegalArgumentException.class, () -> create("{ \"name\": \"a\" }"));
        Assertions.assertTrue(e.getMessage().contains("requires func or op"));
    }

    @Test
    public void testOfUnknownOp() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> create("{ \"name\": \"a\", \"op\": \"unknownOp\" }"));
    }

    @Test
    public void testOfUnknownField() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> create("{ \"name\": \"a\", \"op\": \"sum\", \"field\": \"unknownField\" }"));
    }

    @Test
    public void testOfFuncAlias() {
        final AggregateFunction fn = create("{ \"name\": \"c\", \"func\": \"count\" }");
        Assertions.assertInstanceOf(Count.class, fn);
        Assertions.assertEquals("c", fn.getName());
    }

    @Test
    public void testOfCreatesExpectedTypes() {
        Assertions.assertInstanceOf(Count.class, create("{ \"name\": \"f\", \"op\": \"count\" }"));
        Assertions.assertInstanceOf(Sum.class, create("{ \"name\": \"f\", \"op\": \"sum\", \"field\": \"longField\" }"));
        Assertions.assertInstanceOf(Max.class, create("{ \"name\": \"f\", \"op\": \"max\", \"field\": \"longField\" }"));
        Assertions.assertInstanceOf(Max.class, create("{ \"name\": \"f\", \"op\": \"min\", \"field\": \"longField\" }"));
        Assertions.assertInstanceOf(Last.class, create("{ \"name\": \"f\", \"op\": \"last\", \"field\": \"longField\" }"));
        Assertions.assertInstanceOf(Last.class, create("{ \"name\": \"f\", \"op\": \"first\", \"field\": \"longField\" }"));
        Assertions.assertInstanceOf(ArgMax.class, create("{ \"name\": \"f\", \"op\": \"arg_max\", \"field\": \"longField\", \"comparingField\": \"doubleField\" }"));
        Assertions.assertInstanceOf(ArgMax.class, create("{ \"name\": \"f\", \"op\": \"arg_min\", \"field\": \"longField\", \"comparingField\": \"doubleField\" }"));
        Assertions.assertInstanceOf(Avg.class, create("{ \"name\": \"f\", \"op\": \"avg\", \"field\": \"longField\" }"));
        Assertions.assertInstanceOf(Std.class, create("{ \"name\": \"f\", \"op\": \"std\", \"field\": \"longField\" }"));
        Assertions.assertInstanceOf(SimpleRegression.class, create("{ \"name\": \"f\", \"op\": \"simple_regression\", \"field\": \"doubleField\" }"));
        Assertions.assertInstanceOf(ArrayAgg.class, create("{ \"name\": \"f\", \"op\": \"array_agg\", \"field\": \"longField\" }"));
    }

    @Test
    public void testCompare() {
        Assertions.assertFalse(AggregateFunction.compare(null, 1L));
        Assertions.assertTrue(AggregateFunction.compare(1L, null));
        Assertions.assertTrue(AggregateFunction.compare(2L, 1L));
        Assertions.assertFalse(AggregateFunction.compare(1L, 2L));
        Assertions.assertFalse(AggregateFunction.compare(1L, 1L));

        // opposite (min-like) comparison
        Assertions.assertTrue(AggregateFunction.compare(1L, 2L, true));
        Assertions.assertFalse(AggregateFunction.compare(2L, 1L, true));
        Assertions.assertTrue(AggregateFunction.compare(1L, 1L, true));
        // null handling short-circuits before opposite is applied
        Assertions.assertFalse(AggregateFunction.compare(null, 1L, true));
        Assertions.assertTrue(AggregateFunction.compare(1L, null, true));
    }

    @Test
    public void testMax() {
        Assertions.assertEquals(2L, AggregateFunction.max(null, 2L));
        Assertions.assertEquals(1L, AggregateFunction.max(1L, null));
        Assertions.assertEquals(2L, AggregateFunction.max(1L, 2L));
        Assertions.assertEquals(2L, AggregateFunction.max(2L, 1L));
        Assertions.assertEquals("b", AggregateFunction.max("a", "b"));

        Assertions.assertEquals(1L, AggregateFunction.max(1L, 2L, true));
        Assertions.assertEquals(1L, AggregateFunction.max(2L, 1L, true));
        Assertions.assertEquals(2L, AggregateFunction.max(null, 2L, true));
    }

    @Test
    public void testSum() {
        Assertions.assertEquals(2L, AggregateFunction.sum(null, 2L));
        Assertions.assertEquals(1L, AggregateFunction.sum(1L, null));
        Assertions.assertEquals(3L, AggregateFunction.sum(1L, 2L));
        Assertions.assertEquals(3.5D, AggregateFunction.sum(1.5D, 2D));
        Assertions.assertEquals(3.5F, AggregateFunction.sum(1.5F, 2F));
        Assertions.assertEquals(3, AggregateFunction.sum(1, 2));
        Assertions.assertEquals(new BigDecimal("3.5"), AggregateFunction.sum(new BigDecimal("1.5"), new BigDecimal("2")));
        Assertions.assertEquals(true, AggregateFunction.sum(true, false));
        Assertions.assertEquals(false, AggregateFunction.sum(false, false));
        Assertions.assertEquals("ab", AggregateFunction.sum("a", "b"));
    }

    @Test
    public void testSumShortStaysAccumulable() {
        // sum of shorts must stay a Short so that subsequent accumulation does not fail
        final Object first = AggregateFunction.sum((short) 1, (short) 2);
        final Object second = AggregateFunction.sum(first, (short) 3);
        Assertions.assertEquals((short) 6, second);
    }

    @Test
    public void testSumUnsupportedTypeThrows() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> AggregateFunction.sum(new Object(), new Object()));
    }

    @Test
    public void testAvg() {
        Assertions.assertEquals(2.0D, AggregateFunction.avg(null, null, 2.0D, 3.0D));
        Assertions.assertEquals(1.0D, AggregateFunction.avg(1.0D, 2.0D, null, null));
        // zero or null weight means the other side wins
        Assertions.assertEquals(3.0D, AggregateFunction.avg(1.0D, 0.0D, 3.0D, 1.0D));
        Assertions.assertEquals(1.0D, AggregateFunction.avg(1.0D, 2.0D, 3.0D, 0.0D));
        Assertions.assertEquals(3.0D, AggregateFunction.avg(1.0D, null, 3.0D, 1.0D));
        // weighted merge
        Assertions.assertEquals(3.0D, AggregateFunction.avg(2.0D, 2.0D, 4.0D, 2.0D), 1e-9);
        Assertions.assertEquals(3.25D, AggregateFunction.avg(1.0D, 1.0D, 4.0D, 3.0D), 1e-9);
    }

}
