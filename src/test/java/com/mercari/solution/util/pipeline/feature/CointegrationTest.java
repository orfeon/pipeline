package com.mercari.solution.util.pipeline.feature;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** {@link Cointegration}: the spread's readouts against numpy (least squares, the lag-1 regression of the residuals, the Dickey–Fuller statistic). */
public class CointegrationTest {

    private static final double[] X = {10.0, 10.4, 10.1, 10.9, 11.3, 11.0, 11.8, 12.1, 11.7, 12.5, 12.9, 12.6};
    private static final double[] Y = {7.5, 7.4, 7.45, 7.15, 7.55, 7.8, 8.0, 7.65, 7.65, 8.45, 8.75, 8.2};

    @Test
    public void testHandValues() {
        final int n = X.length;
        Assertions.assertEquals(-0.048483667264363817, Cointegration.residual(X, Y, n), 1e-12);
        Assertions.assertEquals(-0.17195574457577453, Cointegration.residualZ(X, Y, n), 1e-12);
        Assertions.assertEquals(0.095839587638023346, Cointegration.ar1(X, Y, n), 1e-12);
        Assertions.assertEquals(0.2955751377114077, Cointegration.halfLife(X, Y, n), 1e-12);
        Assertions.assertEquals(-3.0080605584300146, Cointegration.adf(X, Y, n), 1e-12);
        for (final String func : Cointegration.FUNCS) Assertions.assertNotNull(Cointegration.of(func, X, Y, n), func);
        // the first n pairs only: the arrays may be longer than the window
        final double[] x = new double[n + 3], y = new double[n + 3];
        System.arraycopy(X, 0, x, 0, n);
        System.arraycopy(Y, 0, y, 0, n);
        Assertions.assertEquals(Cointegration.adf(X, Y, n), Cointegration.adf(x, y, n), 0d);
        // three pairs: the line through the points leaves residuals (−34.27 at the latest), phi below 0 (no half-life), adf needs four
        final double[] x3 = {100, 200, 80}, y3 = {150, 0, 95};
        Assertions.assertEquals(-34.274193548387032, Cointegration.residual(x3, y3, 3), 1e-9);
        Assertions.assertEquals(-0.63500063500095127, Cointegration.residualZ(x3, y3, 3), 1e-12);
        Assertions.assertEquals(-0.027027027027027323, Cointegration.ar1(x3, y3, 3), 1e-12);
        Assertions.assertNull(Cointegration.halfLife(x3, y3, 3));
        Assertions.assertNull(Cointegration.adf(x3, y3, 3));
        // four pairs: adf defined
        final double[] x4 = {1, 2, 3, 4}, y4 = {1.1, 1.9, 3.2, 3.8};
        Assertions.assertEquals(-0.80829756795422136, Cointegration.ar1(x4, y4, 4), 1e-12);
        Assertions.assertEquals(-3.5520755978064238, Cointegration.adf(x4, y4, 4), 1e-12);
        Assertions.assertEquals(-0.11, Cointegration.residual(x4, y4, 4), 1e-12);
    }

    @Test
    public void testProperties() {
        final int n = X.length;
        // the spread is the residual of y on x: a shift or a scale of y moves the residual with it, a shift of x not at all
        final double[] shifted = new double[n], scaled = new double[n], xShifted = new double[n];
        for (int i = 0; i < n; i++) {
            shifted[i] = Y[i] + 100;
            scaled[i] = 3 * Y[i];
            xShifted[i] = X[i] + 1000;
        }
        Assertions.assertEquals(Cointegration.residual(X, Y, n), Cointegration.residual(X, shifted, n), 1e-9);
        Assertions.assertEquals(3 * Cointegration.residual(X, Y, n), Cointegration.residual(X, scaled, n), 1e-9);
        Assertions.assertEquals(Cointegration.residualZ(X, Y, n), Cointegration.residualZ(X, scaled, n), 1e-9, "z is scale-free");
        Assertions.assertEquals(Cointegration.ar1(X, Y, n), Cointegration.ar1(X, scaled, n), 1e-9);
        Assertions.assertEquals(Cointegration.adf(X, Y, n), Cointegration.adf(xShifted, Y, n), 1e-6);
        // a random walk spread: phi near 1, no half-life when at or above 1, adf near 0; a spread that alternates: phi near -1
        final double[] walk = new double[n];
        double level = 0;
        for (int i = 0; i < n; i++) walk[i] = 2 + 0.5 * X[i] + (level += (i % 3 == 0 ? 0.4 : 0.3));
        final Double phiWalk = Cointegration.ar1(X, walk, n);
        Assertions.assertNotNull(phiWalk);
        final double[] alternating = new double[n];
        for (int i = 0; i < n; i++) alternating[i] = 2 + 0.5 * X[i] + (i % 2 == 0 ? 1 : -1);
        final Double phiAlt = Cointegration.ar1(X, alternating, n);
        Assertions.assertTrue(phiAlt < 0, "alternating spread: " + phiAlt);
        Assertions.assertNull(Cointegration.halfLife(X, alternating, n), "no half-life for an overshooting spread");
        Assertions.assertTrue(Cointegration.adf(X, alternating, n) < Cointegration.adf(X, Y, n), "stronger reversion reads more negative");
        // the half-life is in events: phi 0.5 halves in one event, phi 0.5^(1/3) in three
        final double[] half = new double[n], third = new double[n];
        double e = 1;
        for (int i = 0; i < n; i++) {
            half[i] = e;
            e *= 0.5;
        }
        // an exact geometric spread has phi exactly 0.5 whatever the line (the line is fitted on x alone here: y = e)
        Assertions.assertEquals(0.5, phi(half, n), 1e-12);
        Assertions.assertEquals(1d, -Math.log(2) / Math.log(phi(half, n)), 1e-12);
        e = 1;
        for (int i = 0; i < n; i++) {
            third[i] = e;
            e *= Math.pow(0.5, 1d / 3);
        }
        Assertions.assertEquals(3d, -Math.log(2) / Math.log(phi(third, n)), 1e-9);
        // nothing to read: fewer than three pairs, a constant x (no line), a spread of nothing (y on the line)
        Assertions.assertNull(Cointegration.ar1(X, Y, 2));
        Assertions.assertNull(Cointegration.residual(X, Y, 2));
        Assertions.assertNull(Cointegration.residual(new double[]{1, 1, 1, 1}, Y, 4));
        // (integers: the line is recovered exactly, so the residuals are 0 bit for bit, not rounding noise)
        final double[] grid = {1, 2, 3, 4, 5, 6}, exact = {3, 5, 7, 9, 11, 13};
        Assertions.assertNull(Cointegration.residualZ(grid, exact, 6));
        Assertions.assertNull(Cointegration.ar1(grid, exact, 6));
        Assertions.assertNull(Cointegration.adf(grid, exact, 6));
        Assertions.assertEquals(0d, Cointegration.residual(grid, exact, 6), 0d);
    }

    /** The lag-1 regression of a series on itself (no intercept), as the readouts take it over the residuals. */
    private static double phi(final double[] e, final int n) {
        double cross = 0, lagged = 0;
        for (int i = 1; i < n; i++) {
            cross += e[i] * e[i - 1];
            lagged += e[i - 1] * e[i - 1];
        }
        return cross / lagged;
    }
}
