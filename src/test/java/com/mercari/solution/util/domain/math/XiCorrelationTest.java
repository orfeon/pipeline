package com.mercari.solution.util.domain.math;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class XiCorrelationTest {

    private static final double DELTA = 1e-12;

    @Test
    public void testRankMaximum() {
        // Ties get the group's maximum 1-based rank (commons-math TiesStrategy.MAXIMUM).
        Assertions.assertArrayEquals(new double[]{2, 4, 4, 1, 5},
                XiCorrelation.rankMaximum(new double[]{20, 30, 30, 10, 40}), DELTA);
        Assertions.assertArrayEquals(new double[]{3, 3, 3},
                XiCorrelation.rankMaximum(new double[]{7, 7, 7}), DELTA);
        Assertions.assertArrayEquals(new double[]{1, 2, 3},
                XiCorrelation.rankMaximum(new double[]{-1, 0, 1}), DELTA);
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                XiCorrelation.rankMaximum(new double[]{1, Double.NaN}));
    }

    @Test
    public void testPerfectMonotone() {
        // For strictly increasing y with no ties, xi = 1 - 3 / (n + 1).
        final int n = 21;
        final double[] x = new double[n];
        final double[] y = new double[n];
        for (int i = 0; i < n; i++) {
            x[i] = i;
            y[i] = 2 * i + 1;
        }
        Assertions.assertEquals(1d - 3d / (n + 1), XiCorrelation.calculate(x, y), DELTA);
        // xi is designed to detect any functional dependence: decreasing scores the same.
        for (int i = 0; i < n; i++) {
            y[i] = -y[i];
        }
        Assertions.assertEquals(1d - 3d / (n + 1), XiCorrelation.calculate(x, y), DELTA);
    }

    @Test
    public void testFunctionalButNonMonotone() {
        // y = x^2 is a deterministic function of x: xi must be clearly positive
        // (Pearson correlation would be 0 here).
        final int n = 21;
        final double[] x = new double[n];
        final double[] y = new double[n];
        for (int i = 0; i < n; i++) {
            final double value = i - 10;
            x[i] = value;
            y[i] = value * value;
        }
        Assertions.assertTrue(XiCorrelation.calculate(x, y) > 0.5,
                "xi of a deterministic relation should be high");
    }

    @Test
    public void testConstantYAndErrors() {
        Assertions.assertEquals(0.0, XiCorrelation.calculate(
                new double[]{1, 2, 3}, new double[]{5, 5, 5}), DELTA);
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                XiCorrelation.calculate(new double[]{1, 2}, new double[]{1}));
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                XiCorrelation.calculate(null, new double[]{1}));
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                XiCorrelation.calculate(new double[]{1}, new double[]{1}));
    }
}
