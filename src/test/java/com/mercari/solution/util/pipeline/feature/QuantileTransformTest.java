package com.mercari.solution.util.pipeline.feature;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class QuantileTransformTest {

    @Test
    public void testPositionsInterpolateBetweenKnots() {
        final double[] values = {200, 50, 120, 80, 60, 100};
        final double[] copy = values.clone();
        // bins = n − 1: every sorted value is a knot at i / 5
        final QuantileTransform q = QuantileTransform.fit(values, values.length, 5, QuantileTransform.UNIFORM);
        Assertions.assertArrayEquals(copy, values, "the input must not be sorted in place");
        Assertions.assertArrayEquals(new double[]{50, 60, 80, 100, 120, 200}, q.knots, 1e-12);
        Assertions.assertEquals(5, q.bins());
        Assertions.assertEquals(0.0, q.transform(50.0), 1e-12);
        Assertions.assertEquals(0.2, q.transform(60.0), 1e-12);
        Assertions.assertEquals(0.4, q.transform(80.0), 1e-12);
        Assertions.assertEquals(1.0, q.transform(200.0), 1e-12);
        // half-way between the 80 and 100 knots
        Assertions.assertEquals(0.5, q.transform(90.0), 1e-12);
        // out of range clamps, missing is null
        Assertions.assertEquals(0.0, q.transform(-5.0), 1e-12);
        Assertions.assertEquals(1.0, q.transform(1e9), 1e-12);
        Assertions.assertNull(q.transform(null));
        Assertions.assertNull(q.transform(Double.NaN));
        // fewer bins: type-7 quantiles between the values
        final QuantileTransform coarse = QuantileTransform.fit(values, values.length, 2, QuantileTransform.UNIFORM);
        Assertions.assertArrayEquals(new double[]{50, 90, 200}, coarse.knots, 1e-12);
        Assertions.assertEquals(0.25, coarse.transform(70.0), 1e-12);
    }

    @Test
    public void testTiedKnotsMapToTheMiddleOfTheirRange() {
        // a mass point at 1 covers 60 % of the distribution: knots 0, 1, 1, 1, 2 (bins 4)
        final double[] values = {0, 1, 1, 1, 2, 1, 1, 0, 2, 1};
        final QuantileTransform q = QuantileTransform.fit(values, values.length, 4, QuantileTransform.UNIFORM);
        final double[] knots = q.knots;
        Assertions.assertEquals(0.0, knots[0], 1e-12);
        Assertions.assertEquals(2.0, knots[4], 1e-12);
        int first = -1, last = -1;
        for (int i = 0; i < knots.length; i++) if (knots[i] == 1.0) { if (first < 0) first = i; last = i; }
        Assertions.assertTrue(first >= 0 && last > first, java.util.Arrays.toString(knots));
        Assertions.assertEquals((first + last) / 8d, q.transform(1.0), 1e-12);
        // the answer does not depend on which side the search approached from: strictly between is monotone around it
        Assertions.assertTrue(q.transform(0.999) < q.transform(1.0) && q.transform(1.0) < q.transform(1.001));
    }

    @Test
    public void testMassPointAtTheMinimumReadsTheMiddleOfItsRun() {
        // a zero-inflated count: 60 % zeros → knots 0 ×6, 0.4, 1.3, 2.2, 3.1, 4
        final double[] values = {0, 0, 0, 0, 0, 0, 1, 2, 3, 4};
        final QuantileTransform q = QuantileTransform.fit(values, values.length, 10, QuantileTransform.UNIFORM);
        Assertions.assertEquals(0.0, q.knots[5], 1e-12);
        Assertions.assertTrue(q.knots[6] > 0);
        Assertions.assertEquals(0.25, q.transform(0.0), 1e-12);
        Assertions.assertTrue(q.transform(0.0) < q.transform(0.2));
        Assertions.assertEquals(0.0, q.transform(-1.0), 1e-12);
        // the normal score of the mass point is Φ⁻¹(0.25), not the ±4.75 clamp
        final QuantileTransform normal = QuantileTransform.fit(values, values.length, 10, QuantileTransform.NORMAL);
        Assertions.assertEquals(-0.6744897502, normal.transform(0.0), 1e-7);
        // a mass point at the maximum mirrors it; a constant field reads 0.5
        final double[] top = {1, 2, 3, 4, 9, 9, 9, 9, 9, 9};
        final QuantileTransform t = QuantileTransform.fit(top, top.length, 10, QuantileTransform.UNIFORM);
        Assertions.assertEquals(1.0, t.transform(10.0), 1e-12);
        Assertions.assertTrue(t.transform(9.0) < 1.0 && t.transform(9.0) > t.transform(4.0));
        final double[] constant = {7, 7, 7, 7};
        Assertions.assertEquals(0.5, QuantileTransform.fit(constant, constant.length, 4, QuantileTransform.UNIFORM).transform(7.0), 1e-12);
    }

    @Test
    public void testNormalScores() {
        final double[] values = new double[1001];
        for (int i = 0; i < values.length; i++) values[i] = i;
        final QuantileTransform q = QuantileTransform.fit(values, values.length, 100, QuantileTransform.NORMAL);
        Assertions.assertEquals(0.0, q.transform(500.0), 1e-9);
        Assertions.assertEquals(1.959963985, q.transform(975.0), 1e-6);
        Assertions.assertEquals(-1.959963985, q.transform(25.0), 1e-6);
        // the extremes are clamped, never infinite
        final double top = q.transform(1000.0);
        Assertions.assertTrue(Double.isFinite(top) && top > 4.7 && top < 4.8, Double.toString(top));
        Assertions.assertEquals(-top, q.transform(0.0), 1e-9);
        // probit accuracy against tabulated values
        Assertions.assertEquals(-1.281551566, QuantileTransform.probit(0.1), 1e-7);
        Assertions.assertEquals(2.326347874, QuantileTransform.probit(0.99), 1e-7);
        Assertions.assertEquals(0.6744897502, QuantileTransform.probit(0.75), 1e-7);
    }

    @Test
    public void testEmptyFitAndJsonRoundTrip() {
        final QuantileTransform empty = QuantileTransform.fit(new double[0], 0, 10, QuantileTransform.UNIFORM);
        Assertions.assertEquals(0, empty.n);
        Assertions.assertNull(empty.transform(1.0));
        Assertions.assertNull(QuantileTransform.fromJson(empty.toJson()).transform(1.0));

        final double[] values = {3, 1, 4, 1, 5, 9, 2, 6};
        final QuantileTransform q = QuantileTransform.fit(values, values.length, 7, QuantileTransform.NORMAL);
        final QuantileTransform back = QuantileTransform.fromJson(q.toJson());
        Assertions.assertEquals(QuantileTransform.NORMAL, back.distribution);
        for (final double v : new double[]{0, 1, 1.5, 2, 4.2, 6, 9, 10}) Assertions.assertEquals(q.transform(v), back.transform(v), 1e-12, "v=" + v);
        Assertions.assertThrows(IllegalStateException.class, () -> {
            final com.google.gson.JsonObject json = q.toJson();
            json.remove("n");
            QuantileTransform.fromJson(json);
        });
    }

}
