package com.mercari.solution.util.pipeline.feature;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VectorOpsTest {

    private static final double[] X = {4, 1, 3, 9, 9, 2};

    private static Object read(final String func, final double... x) {
        return VectorOps.read(func, x, VectorOps.positions(x.length, false));
    }

    @Test
    public void testToVector() {
        Assertions.assertArrayEquals(new double[]{1, 2.5, 3}, VectorOps.toVector(List.of(1L, 2.5d, 3f)));
        Assertions.assertArrayEquals(new double[]{1, 2}, VectorOps.toVector(new double[]{1, 2}));
        Assertions.assertArrayEquals(new double[0], VectorOps.toVector(List.of()));
        Assertions.assertNull(VectorOps.toVector(null));
        Assertions.assertNull(VectorOps.toVector(3.0d), "a scalar is not a vector");
        Assertions.assertNull(VectorOps.toVector(Arrays.asList(1d, null, 3d)), "a missing component leaves no vector");
        Assertions.assertNull(VectorOps.toVector(List.of(1d, Double.NaN)));
        Assertions.assertNull(VectorOps.toVector(new double[]{1, Double.POSITIVE_INFINITY}));
    }

    @Test
    public void testSlice() {
        Assertions.assertSame(X, VectorOps.slice(X, null, null));
        Assertions.assertArrayEquals(new double[]{4, 1}, VectorOps.slice(X, 0, 2));
        Assertions.assertArrayEquals(new double[]{3, 9, 9, 2}, VectorOps.slice(X, 2, null));
        Assertions.assertArrayEquals(new double[]{9, 9, 2}, VectorOps.slice(X, -3, null), "a negative index counts from the end");
        Assertions.assertArrayEquals(new double[]{4, 1, 3, 9}, VectorOps.slice(X, null, -2));
        Assertions.assertArrayEquals(new double[]{1, 3}, VectorOps.slice(X, -5, -3));
        Assertions.assertArrayEquals(X, VectorOps.slice(X, -100, 100), "bounds are clamped");
        Assertions.assertArrayEquals(new double[0], VectorOps.slice(X, 4, 2), "an inverted range is empty");
        Assertions.assertArrayEquals(new double[0], VectorOps.slice(X, 10, null));
    }

    @Test
    public void testDiff() {
        Assertions.assertSame(X, VectorOps.diff(X, 0));
        Assertions.assertArrayEquals(new double[]{-3, 2, 6, 0, -7}, VectorOps.diff(X, 1));
        Assertions.assertArrayEquals(new double[]{5, 4, -6, -7}, VectorOps.diff(X, 2));
        Assertions.assertArrayEquals(new double[0], VectorOps.diff(new double[]{7}, 1));
        Assertions.assertArrayEquals(new double[0], VectorOps.diff(new double[]{7, 8}, 5), "differencing past the length ends at the empty vector");
    }

    @Test
    public void testNormalize() {
        final double[] x = {1, 2, 3, 6};
        Assertions.assertArrayEquals(new double[]{1 / 12d, 2 / 12d, 3 / 12d, 6 / 12d}, VectorOps.normalize(x, "sum"), 1e-12);
        Assertions.assertArrayEquals(new double[]{1 / 3d, 2 / 3d, 1d, 2d}, VectorOps.normalize(x, "mean"), 1e-12);
        Assertions.assertArrayEquals(new double[]{0.6, 0.8}, VectorOps.normalize(new double[]{3, 4}, "l2"), 1e-12);
        final double[] z = VectorOps.normalize(x, "zscore");
        Assertions.assertEquals(0d, Arrays.stream(z).sum(), 1e-12);
        Assertions.assertEquals(4d, Arrays.stream(z).map(v -> v * v).sum(), 1e-12, "unit population variance");
        // a zero denominator or an empty vector has no normalised form
        Assertions.assertNull(VectorOps.normalize(new double[]{1, -1}, "sum"));
        Assertions.assertNull(VectorOps.normalize(new double[]{5, 5, 5}, "zscore"));
        Assertions.assertNull(VectorOps.normalize(new double[]{0, 0}, "l2"));
        Assertions.assertNull(VectorOps.normalize(new double[0], "mean"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> VectorOps.normalize(x, "softmax"));
    }

    @Test
    public void testReadouts() {
        Assertions.assertEquals(6L, read("length", X));
        Assertions.assertEquals(28d, (Double) read("sum", X), 1e-12);
        Assertions.assertEquals(28 / 6d, (Double) read("mean", X), 1e-12);
        // population std: sqrt(Σx²/n − mean²) = sqrt(192/6 − (14/3)²)
        Assertions.assertEquals(Math.sqrt(32 - 196 / 9d), (Double) read("std", X), 1e-12);
        Assertions.assertEquals(1d, read("min", X));
        Assertions.assertEquals(9d, read("max", X));
        Assertions.assertEquals(1L, read("argmin", X));
        Assertions.assertEquals(3L, read("argmax", X), "the first of tied maxima");
        Assertions.assertEquals(4d, read("first", X));
        Assertions.assertEquals(2d, read("last", X));
        Assertions.assertEquals(Math.sqrt(192), (Double) read("norm", X), 1e-12);
        // slope of 2 + 3i is 3 per index; over unit positions the same line rises 3 (n − 1) across the span
        final double[] line = {2, 5, 8, 11, 14};
        Assertions.assertEquals(3d, (Double) read("slope", line), 1e-12);
        Assertions.assertEquals(12d, (Double) VectorOps.read("slope", line, VectorOps.positions(5, true)), 1e-12);
        Assertions.assertEquals(SequenceEvaluator.slope(List.of(4d, 1d, 3d, 9d, 9d, 2d)), (Double) read("slope", X), 1e-12, "the sequence trend's slope");
        Assertions.assertThrows(IllegalArgumentException.class, () -> read("kurtosis", X));
    }

    @Test
    public void testReadoutsOfShortVectors() {
        Assertions.assertEquals(0L, read("length"));
        for (final String func : List.of("sum", "mean", "std", "min", "max", "argmin", "argmax", "first", "last", "norm", "slope")) {
            Assertions.assertNull(read(func), func + " of the empty vector");
        }
        Assertions.assertEquals(7d, read("mean", 7));
        Assertions.assertEquals(0L, read("argmax", 7));
        Assertions.assertNull(read("std", 7), "std needs two elements");
        Assertions.assertNull(read("slope", 7), "slope needs two elements");
        Assertions.assertNull(read("sum", Double.MAX_VALUE, Double.MAX_VALUE), "an overflow reads null, never infinity");
    }

    @Test
    public void testPolyfit() {
        // y = 1 − 2p + 0.5p² sampled at p = 0..5 is recovered exactly
        final double[] y = new double[6];
        for (int i = 0; i < y.length; i++) y[i] = 1 - 2 * i + 0.5 * i * i;
        Assertions.assertArrayEquals(new double[]{1, -2, 0.5}, VectorOps.polyfit(y, VectorOps.positions(6, false), 2), 1e-9);
        // over unit positions p = i / 5 the same curve is 1 − 10p + 12.5p²
        Assertions.assertArrayEquals(new double[]{1, -10, 12.5}, VectorOps.polyfit(y, VectorOps.positions(6, true), 2), 1e-9);
        // degree 1 is the regression line: its slope is the slope readout
        Assertions.assertEquals((Double) read("slope", X), VectorOps.polyfit(X, VectorOps.positions(6, false), 1)[1], 1e-9);
        Assertions.assertNull(VectorOps.polyfit(new double[]{1, 2}, VectorOps.positions(2, false), 2), "degree + 1 elements are needed");
        Assertions.assertNotNull(VectorOps.polyfit(new double[]{1, 2, 4}, VectorOps.positions(3, false), 2));
    }

    /**
     * {@code resample} interpolates the vector onto a fixed length (the first and last elements kept, a straight
     * line stays a straight line whether the array shrinks or grows), {@code pad} extends a short vector with its
     * edge or zeros at either side and never truncates; the {@code vector} readout emits the stepped vector, so an
     * array of varying length reaches an array svd at one length.
     */
    @Test
    public void testResampleAndPad() {
        Assertions.assertArrayEquals(new double[]{0, 2.5, 5, 7.5, 10}, VectorOps.resample(new double[]{0, 5, 10}, 5), 1e-12);
        Assertions.assertArrayEquals(new double[]{0, 10}, VectorOps.resample(new double[]{0, 2.5, 5, 7.5, 10}, 2), 1e-12);
        Assertions.assertArrayEquals(new double[]{1, 1, 1}, VectorOps.resample(new double[]{1}, 3), 0d);
        Assertions.assertArrayEquals(new double[]{0}, VectorOps.resample(new double[]{0, 4, 8}, 1), 0d, "one position: the first element");
        Assertions.assertEquals(0, VectorOps.resample(new double[0], 3).length);
        Assertions.assertArrayEquals(new double[]{1, 2, 2, 2}, VectorOps.pad(new double[]{1, 2}, 4, "edge", "end"), 0d);
        Assertions.assertArrayEquals(new double[]{1, 1, 1, 2}, VectorOps.pad(new double[]{1, 2}, 4, "edge", "start"), 0d);
        Assertions.assertArrayEquals(new double[]{0, 0, 1, 2}, VectorOps.pad(new double[]{1, 2}, 4, "zero", "start"), 0d);
        Assertions.assertArrayEquals(new double[]{1, 2, 3}, VectorOps.pad(new double[]{1, 2, 3}, 2, "edge", "end"), 0d, "never truncated");
        Assertions.assertEquals(0, VectorOps.pad(new double[0], 2, "zero", "end").length, "nothing to pad from: the empty vector stays empty");
        Assertions.assertEquals(List.of(1d, 3d, 2d), VectorOps.read("vector", new double[]{1, 3, 2}, VectorOps.positions(3, false)), "every catalog readout is served by read");
        final Map<String, String> coordinates = new HashMap<>(Map.of("func", "vector", "resample", "4"));
        Assertions.assertEquals(List.of(0d, 1d, 2d, 3d), VectorOps.Plan.of(coordinates).evaluate(List.of(0d, 3d)));
        Assertions.assertNull(VectorOps.Plan.of(coordinates).evaluate(List.of()), "the empty vector has no readout but its length (an svd must not fix its length on it)");
        coordinates.put("padLength", "6");
        Assertions.assertEquals(List.of(0d, 1d, 2d, 3d, 3d, 3d), VectorOps.Plan.of(coordinates).evaluate(List.of(0d, 3d)));
        Assertions.assertNull(VectorOps.Plan.of(coordinates).evaluate(List.of()), "the empty vector stays empty under pad: no vector, so no svd observation");
        coordinates.put("func", "length");
        Assertions.assertEquals(0L, VectorOps.Plan.of(coordinates).evaluate(List.of()));
        coordinates.put("func", "length");
        Assertions.assertEquals(6L, VectorOps.Plan.of(coordinates).evaluate(List.of(0d, 3d)));
        Assertions.assertNull(VectorOps.Plan.of(coordinates).evaluate(null));
    }

    @Test
    public void testPlan() {
        final Map<String, String> coordinates = new HashMap<>();
        coordinates.put("func", "mean");
        coordinates.put("sliceFrom", "-3");
        // the mean of the last three elements
        Assertions.assertEquals(20 / 3d, (Double) VectorOps.Plan.of(coordinates).evaluate(List.of(4d, 1d, 3d, 9d, 9d, 2d)), 1e-12);
        // steps run slice → diff → normalize: [9, 9, 2] → [0, −7] → / l2 → [0, −1]
        coordinates.put("diff", "1");
        coordinates.put("normalize", "l2");
        coordinates.put("func", "last");
        Assertions.assertEquals(-1d, (Double) VectorOps.Plan.of(coordinates).evaluate(X), 1e-12);
        coordinates.put("func", "length");
        Assertions.assertEquals(2L, VectorOps.Plan.of(coordinates).evaluate(X));
        // null array, a hole in the array, an undefined normalisation: every readout is null (length included)
        Assertions.assertNull(VectorOps.Plan.of(coordinates).evaluate(null));
        Assertions.assertNull(VectorOps.Plan.of(coordinates).evaluate(Arrays.asList(1d, null, 2d, 3d)));
        Assertions.assertNull(VectorOps.Plan.of(coordinates).evaluate(List.of(5d, 5d, 5d, 5d)), "constant tail: the differences have no l2 direction");
        // a slice beyond the array is the empty vector: length 0, no other readout
        final Map<String, String> beyond = new HashMap<>(Map.of("func", "length", "sliceFrom", "10"));
        Assertions.assertEquals(0L, VectorOps.Plan.of(beyond).evaluate(X));
        beyond.put("func", "max");
        Assertions.assertNull(VectorOps.Plan.of(beyond).evaluate(X));

        final Map<String, String> poly = new HashMap<>(Map.of("func", "polyfit", "degree", "2", "coefficient", "2", "position", "index"));
        Assertions.assertEquals(0.5, (Double) VectorOps.Plan.of(poly).evaluate(List.of(1d, -0.5d, -1d, -0.5d, 1d)), 1e-9);
        Assertions.assertNull(VectorOps.Plan.of(poly).evaluate(List.of(1d, 2d)), "too short for the degree");
        // the input array is never modified
        Assertions.assertArrayEquals(new double[]{4, 1, 3, 9, 9, 2}, X);
    }

}
