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
    public void testNormalClipBoundsTheExtremeRows() {
        final double[] values = {200, 50, 120, 80, 60, 100};
        // default clip 1e-6: the fitted minimum / maximum read ±Φ⁻¹(1 − 1e−6) = ±4.7534 whatever n
        final QuantileTransform q = QuantileTransform.fit(values, values.length, 5, QuantileTransform.NORMAL);
        Assertions.assertEquals(QuantileTransform.DEFAULT_CLIP, q.clip);
        Assertions.assertEquals(-4.753424, q.transform(50.0), 1e-5);
        Assertions.assertEquals(4.753424, q.transform(200.0), 1e-5);
        Assertions.assertEquals(0.0, q.transform(90.0), 1e-12, "the median is unaffected by the clip");
        // clip 0.01: p is clamped to [0.01, 0.99] before the probit, so the extremes read ±2.3263 and the interior is unchanged
        final QuantileTransform clipped = QuantileTransform.fit(values, values.length, 5, QuantileTransform.NORMAL, 0.01);
        Assertions.assertEquals(-2.326348, clipped.transform(50.0), 1e-5);
        Assertions.assertEquals(2.326348, clipped.transform(200.0), 1e-5);
        Assertions.assertEquals(-2.326348, clipped.transform(-1e9), 1e-5, "below the minimum clamps to the same bound");
        Assertions.assertEquals(q.transform(90.0), clipped.transform(90.0), 1e-12);
        Assertions.assertEquals(q.transform(60.0), clipped.transform(60.0), 1e-12, "p = 0.2 is inside the clip");
        // the clip rides in the artifact (normal only); an artifact without it reads the default
        final com.google.gson.JsonObject json = clipped.toJson();
        Assertions.assertEquals(0.01, json.get("clip").getAsDouble());
        Assertions.assertEquals(0.01, QuantileTransform.fromJson(json).clip);
        Assertions.assertNull(QuantileTransform.fit(values, values.length, 5, QuantileTransform.UNIFORM, 0.01).toJson().get("clip"));
        json.remove("clip");
        Assertions.assertEquals(QuantileTransform.DEFAULT_CLIP, QuantileTransform.fromJson(json).clip);
        // the clip is apply-time: a loaded artifact takes the config's clip (a pinned or pre-clip artifact included)
        final QuantileTransform reclipped = QuantileTransform.fromJson(json).withClip(0.01);
        Assertions.assertEquals(0.01, reclipped.clip);
        Assertions.assertEquals(-2.326348, reclipped.transform(50.0), 1e-5);
        Assertions.assertSame(clipped, clipped.withClip(0.01), "the same clip keeps the instance");
        // an empty fit is flagged
        Assertions.assertTrue(QuantileTransform.fit(values, 0, 5, QuantileTransform.NORMAL).isEmpty());
        Assertions.assertFalse(clipped.isEmpty());
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

    private static QuantileTransform.Values values(final double... xs) {
        final QuantileTransform.Values v = QuantileTransform.VALUES.create();
        for (final double x : xs) QuantileTransform.VALUES.update(v, x, 1);
        return v;
    }

    /**
     * The fit state as a summary family: a monoid (merge = concatenation, any order gives the same knots) that cannot
     * remove; a forward model at a change point is exactly the static fit on the values of the blocks readable there,
     * whole past or a window of blocks; the state survives serialization trimmed to its size.
     */
    @Test
    public void testValuesFamilyAndBlockSeries() throws Exception {
        final Summary<QuantileTransform.Values> family = QuantileTransform.VALUES;
        Assertions.assertFalse(family.invertible());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> family.update(values(1, 2), 1.0, -1));
        Assertions.assertEquals(3L, family.read(values(4, 5, 6), Summary.Readout.of("count")));
        Assertions.assertEquals(0, family.count(family.create()), 0);

        final java.util.Random random = new java.util.Random(41);
        final java.util.Map<Long, QuantileTransform.Values> parts = new java.util.HashMap<>();
        final java.util.Map<Long, double[]> raw = new java.util.TreeMap<>();
        for (final long block : new long[]{10, 11, 13, 16}) {
            final double[] xs = random.doubles(5 + random.nextInt(40)).map(d -> Math.round(d * 1000) / 10.0).toArray();
            raw.put(block, xs);
            parts.put(block, values(xs));
        }
        final BlockSeries<QuantileTransform.Values> series = new BlockSeries<>(family, parts);
        final java.util.function.BiFunction<Long, Integer, QuantileTransform> direct = (at, window) -> {
            final java.util.List<Double> readable = new java.util.ArrayList<>();
            for (final java.util.Map.Entry<Long, double[]> e : raw.entrySet()) {
                if (e.getKey() <= at && (window <= 0 || e.getKey() > at - window)) for (final double x : e.getValue()) readable.add(x);
            }
            java.util.Collections.shuffle(readable, random); // the order of the values must not matter
            final double[] xs = readable.stream().mapToDouble(d -> d).toArray();
            return QuantileTransform.fit(xs, xs.length, 8, QuantileTransform.UNIFORM);
        };
        for (final int window : new int[]{0, 2, 4}) {
            final java.util.TreeMap<Long, QuantileTransform> models = series.models(window, v -> QuantileTransform.fit(v, 8, QuantileTransform.UNIFORM, QuantileTransform.DEFAULT_CLIP, false));
            Assertions.assertEquals(series.changePoints(window), models.keySet());
            for (final java.util.Map.Entry<Long, QuantileTransform> e : models.entrySet()) {
                final QuantileTransform expected = direct.apply(e.getKey(), window);
                Assertions.assertEquals(expected.n, e.getValue().n, "n at " + e.getKey() + " window " + window);
                Assertions.assertArrayEquals(expected.knots, e.getValue().knots, 0, "knots at " + e.getKey() + " window " + window);
            }
        }
        // under a window of 2 the block 13 has left at 15: nothing is readable there, and the model says so quietly
        final QuantileTransform gap = series.models(2, v -> QuantileTransform.fit(v, 8, QuantileTransform.UNIFORM, QuantileTransform.DEFAULT_CLIP, false)).get(15L);
        Assertions.assertTrue(gap.isEmpty());
        Assertions.assertNull(gap.transform(1.0));
        // the parts are not modified by the merges
        for (final java.util.Map.Entry<Long, double[]> e : raw.entrySet()) Assertions.assertEquals(e.getValue().length, parts.get(e.getKey()).size());

        // serialization: what travels is the values, not the spare capacity; the fit is unchanged
        final QuantileTransform.Values v = values(3, 1, 2);
        final java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
        try (java.io.ObjectOutputStream out = new java.io.ObjectOutputStream(bytes)) {
            out.writeObject(v);
        }
        Assertions.assertTrue(bytes.size() < 200, "three doubles, not a 16-slot buffer: " + bytes.size());
        final QuantileTransform.Values back;
        try (java.io.ObjectInputStream in = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(bytes.toByteArray()))) {
            back = (QuantileTransform.Values) in.readObject();
        }
        Assertions.assertEquals(3, back.size());
        family.update(back, 9.0, 1); // a deserialized state can still grow
        Assertions.assertArrayEquals(QuantileTransform.fit(new double[]{3, 1, 2, 9}, 4, 2, QuantileTransform.UNIFORM).knots,
                QuantileTransform.fit(back, 2, QuantileTransform.UNIFORM, QuantileTransform.DEFAULT_CLIP, true).knots, 0);
    }

}
