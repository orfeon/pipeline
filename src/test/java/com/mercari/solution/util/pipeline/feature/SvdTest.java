package com.mercari.solution.util.pipeline.feature;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class SvdTest {

    /** Points a · (0.6, 0.8) + b · (−0.8, 0.6) with Σa = Σb = Σab = 0: the axes are exactly the singular vectors. */
    private static Svd.Moments axes() {
        final double[] a = {2, -2, 1, -1}, b = {0.5, 0.5, -0.5, -0.5};
        final Svd.Moments m = new Svd.Moments();
        for (int i = 0; i < a.length; i++) m.add(new double[]{0.6 * a[i] - 0.8 * b[i], 0.8 * a[i] + 0.6 * b[i]});
        return m;
    }

    @Test
    public void testRecoversTheAxesAndScores() {
        final Svd svd = Svd.fit(axes(), 2, true, false);
        Assertions.assertEquals(2, svd.dimension);
        Assertions.assertEquals(4, svd.n);
        Assertions.assertArrayEquals(new double[]{0, 0}, svd.mean, 1e-12);
        // variances Σa² / 3 and Σb² / 3, components oriented with their largest loading positive
        Assertions.assertEquals(10.0 / 3, svd.variances[0], 1e-9);
        Assertions.assertEquals(1.0 / 3, svd.variances[1], 1e-9);
        Assertions.assertEquals(11.0 / 3, svd.totalVariance, 1e-9);
        Assertions.assertArrayEquals(new double[]{0.6, 0.8}, svd.components[0], 1e-9);
        Assertions.assertArrayEquals(new double[]{0.8, -0.6}, svd.components[1], 1e-9);
        // the point a = 2, b = 0.5 scores (2, −0.5)
        final double[] scores = svd.transform(new double[]{0.6 * 2 - 0.8 * 0.5, 0.8 * 2 + 0.6 * 0.5});
        Assertions.assertEquals(2.0, scores[0], 1e-9);
        Assertions.assertEquals(-0.5, scores[1], 1e-9);
        // a truncated rank keeps the leading component only
        final Svd rank1 = Svd.fit(axes(), 1, true, false);
        Assertions.assertEquals(1, rank1.rank());
        Assertions.assertEquals(1, rank1.transform(new double[]{1, 1}).length);
        Assertions.assertEquals(1.4, rank1.transform(new double[]{1, 1})[0], 1e-9);
        // rank beyond the dimension is capped
        Assertions.assertEquals(2, Svd.fit(axes(), 5, true, false).rank());
        // missing components and wrong lengths map to null
        Assertions.assertNull(svd.transform(null));
        Assertions.assertNull(svd.transform(new double[]{1}));
        Assertions.assertNull(svd.transform(new double[]{1, Double.NaN}));
    }

    @Test
    public void testCenteringAndStandardisation() {
        // x and y = 10 x + 5: one direction of variance; centring removes the offset, standardising equalises the scales
        final Svd.Moments m = new Svd.Moments();
        for (final double x : new double[]{1, 2, 3, 4, 5}) m.add(new double[]{x, 10 * x + 5});
        final Svd centred = Svd.fit(m, 2, true, false);
        Assertions.assertArrayEquals(new double[]{3, 35}, centred.mean, 1e-12);
        Assertions.assertEquals(0.0, centred.variances[1], 1e-9);
        Assertions.assertEquals(centred.totalVariance, centred.variances[0], 1e-9);
        Assertions.assertEquals(0.0, centred.transform(new double[]{3, 35})[0], 1e-9);
        final Svd standardised = Svd.fit(m, 2, true, true);
        Assertions.assertArrayEquals(new double[]{Math.sqrt(2.5), 10 * Math.sqrt(2.5)}, standardised.scale, 1e-9);
        Assertions.assertEquals(2.0, standardised.totalVariance, 1e-9); // the trace of a correlation matrix
        Assertions.assertArrayEquals(new double[]{Math.sqrt(0.5), Math.sqrt(0.5)}, standardised.components[0], 1e-9);
        final Svd uncentred = Svd.fit(m, 1, false, false);
        Assertions.assertArrayEquals(new double[]{0, 0}, uncentred.mean, 1e-12);
        Assertions.assertTrue(uncentred.variances[0] > centred.variances[0], "the uncentred second moment includes the offset");
    }

    @Test
    public void testMomentsMergeAndSkipRules() {
        final Svd.Moments a = new Svd.Moments(), b = new Svd.Moments();
        a.add(new double[]{1, 2});
        a.add(new double[]{3, 4});
        a.add(null);
        a.add(new double[]{1, Double.NaN});
        b.add(new double[]{5, 6});
        b.add(new double[]{1, 2, 3}); // a different length: skipped
        final Svd.Moments merged = new Svd.Moments();
        merged.merge(b);
        merged.merge(a);
        Assertions.assertEquals(3, merged.n);
        Assertions.assertEquals(2, merged.skipped, "null and NaN");
        Assertions.assertEquals(1, merged.mismatched, "the length-3 vector");
        // the anchored sums reproduce the plain moments of (1,2), (3,4), (5,6) whichever accumulator is merged first
        final Svd fit = Svd.fit(merged, 2, true, false);
        Assertions.assertArrayEquals(new double[]{3, 4}, fit.mean, 1e-12);
        Assertions.assertEquals(8.0, fit.totalVariance, 1e-12); // Σ(x − 3)² / 2 + Σ(y − 4)² / 2
        final Svd.Moments reversed = new Svd.Moments();
        reversed.merge(a);
        reversed.merge(b);
        final Svd fit2 = Svd.fit(reversed, 2, true, false);
        Assertions.assertArrayEquals(fit.mean, fit2.mean, 1e-12);
        Assertions.assertArrayEquals(fit.variances, fit2.variances, 1e-12);
        // uncentred: Σx² / n on the diagonal
        Assertions.assertEquals((1 + 9 + 25) / 3d + (4 + 16 + 36) / 3d, Svd.fit(merged, 2, false, false).totalVariance, 1e-12);
        // fewer than two vectors: no components, every vector maps to null
        final Svd.Moments one = new Svd.Moments();
        one.add(new double[]{1, 2});
        final Svd empty = Svd.fit(one, 2, true, false);
        Assertions.assertEquals(0, empty.rank());
        Assertions.assertNull(empty.transform(new double[]{1, 2}));
        Assertions.assertEquals(0, Svd.fit(new Svd.Moments(), 2, true, false).dimension);
    }

    @Test
    public void testJacobiOnAKnownSpectrum() {
        // Q diag(5, 2, 1) Qᵀ with an orthonormal Q
        final double[][] q = {
                {2 / 3d, -2 / 3d, 1 / 3d},
                {1 / 3d, 2 / 3d, 2 / 3d},
                {2 / 3d, 1 / 3d, -2 / 3d}};
        final double[] lambda = {1, 5, 2};
        final double[][] m = new double[3][3];
        for (int i = 0; i < 3; i++) for (int j = 0; j < 3; j++) for (int k = 0; k < 3; k++) m[i][j] += q[k][i] * lambda[k] * q[k][j];
        final double[][] eigen = Svd.jacobi(m);
        Assertions.assertArrayEquals(new double[]{5, 2, 1}, eigen[0], 1e-9);
        for (int r = 0; r < 3; r++) {
            final int source = r == 0 ? 1 : r == 1 ? 2 : 0;
            double dot = 0, norm = 0;
            for (int i = 0; i < 3; i++) {
                dot += eigen[r + 1][i] * q[source][i];
                norm += eigen[r + 1][i] * eigen[r + 1][i];
            }
            Assertions.assertEquals(1.0, Math.abs(dot), 1e-9, "eigenvector " + r);
            Assertions.assertEquals(1.0, norm, 1e-9);
        }
    }

    @Test
    public void testEqualDiagonalsStillRotate() {
        // [[2,1],[1,2]] has equal diagonals: theta = 0 must rotate by 45°, not stall
        final double[][] eigen = Svd.jacobi(new double[][]{{2, 1}, {1, 2}});
        Assertions.assertArrayEquals(new double[]{3, 1}, eigen[0], 1e-12);
        Assertions.assertEquals(Math.sqrt(0.5), Math.abs(eigen[1][0]), 1e-12);
        Assertions.assertEquals(Math.sqrt(0.5), Math.abs(eigen[1][1]), 1e-12);
        // a correlation matrix (standardize) has an all-ones diagonal: the components must be decorrelated
        final Svd.Moments m = new Svd.Moments();
        for (final double[] p : new double[][]{{1, 0}, {-1, 0}, {0, 1}, {0, -1}, {1, 1}, {-1, -1}}) m.add(p);
        final Svd svd = Svd.fit(m, 2, true, true);
        Assertions.assertEquals(1.5, svd.variances[0], 1e-9);
        Assertions.assertEquals(0.5, svd.variances[1], 1e-9);
        Assertions.assertArrayEquals(new double[]{Math.sqrt(0.5), Math.sqrt(0.5)}, svd.components[0], 1e-9);
    }

    @Test
    public void testLargeOffsetDoesNotCancelTheCovariance() {
        // epoch-millis-like values with a small spread: the variance must survive
        final Svd.Moments a = new Svd.Moments(), b = new Svd.Moments();
        final double base = 1.7e12;
        for (int i = 0; i < 50; i++) a.add(new double[]{base + i, base - 2 * i});
        for (int i = 50; i < 100; i++) b.add(new double[]{base + i, base - 2 * i});
        a.merge(b);
        final Svd svd = Svd.fit(a, 2, true, false);
        // var(i) = Σ(i − 49.5)² / 99 = 841.6666… for i = 0..99; the second coordinate scales by 4, and the two are
        // perfectly correlated, so the whole trace 5 · var(i) sits on the first component
        final double v = 841.6666666666666;
        Assertions.assertEquals(5 * v, svd.totalVariance, 1e-6 * v);
        Assertions.assertEquals(5 * v, svd.variances[0], 1e-6 * v);
        Assertions.assertEquals(0.0, svd.variances[1], 1e-6 * v);
        Assertions.assertArrayEquals(new double[]{base + 49.5, base - 99}, svd.mean, 1e-3);
        // tiny-scale inputs converge too (relative threshold)
        final Svd.Moments tiny = new Svd.Moments();
        for (final double[] p : new double[][]{{1, 0}, {-1, 0}, {0, 1}, {0, -1}, {1, 1}, {-1, -1}}) tiny.add(new double[]{p[0] * 1e-12, p[1] * 1e-12});
        final Svd small = Svd.fit(tiny, 2, true, false);
        Assertions.assertArrayEquals(new double[]{Math.sqrt(0.5), Math.sqrt(0.5)}, small.components[0], 1e-9);
    }

    @Test
    public void testJsonRoundTrip() {
        final Svd svd = Svd.fit(axes(), 2, true, true);
        final Svd back = Svd.fromJson(svd.toJson());
        for (final double[] x : List.of(new double[]{1, 1}, new double[]{-2, 0.5}, new double[]{0, 0})) {
            Assertions.assertArrayEquals(svd.transform(x), back.transform(x), 1e-12);
        }
        Assertions.assertEquals(svd.totalVariance, back.totalVariance, 1e-12);
        Assertions.assertThrows(IllegalStateException.class, () -> {
            final com.google.gson.JsonObject json = svd.toJson();
            json.remove("n");
            Svd.fromJson(json);
        });
    }

}
