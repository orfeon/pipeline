package com.mercari.solution.util.pipeline.feature;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Random;

public class SeriesStatsTest {

    @Test
    public void testParse() {
        Assertions.assertEquals(new SeriesStats.Readout("zeroCross", 0, 0), SeriesStats.parse("zeroCross"));
        Assertions.assertEquals(new SeriesStats.Readout("peaks", 0, 0), SeriesStats.parse("peaks"));
        Assertions.assertEquals(new SeriesStats.Readout("acf", 3, 0), SeriesStats.parse("acf3"));
        Assertions.assertEquals(new SeriesStats.Readout("pacf", 12, 0), SeriesStats.parse("pacf12"));
        Assertions.assertEquals(new SeriesStats.Readout("ar", 2, 1), SeriesStats.parse("ar2_1"));
        Assertions.assertTrue(SeriesStats.isCount(SeriesStats.parse("peaks")));
        Assertions.assertFalse(SeriesStats.isCount(SeriesStats.parse("acf1")));
        // lags / orders run 1..MAX_LAG, a coefficient index 1..order; anything else is not a series readout
        for (final String token : new String[]{"acf", "acf0", "acf21", "pacf0", "ar2", "ar2_0", "ar2_3", "ar0_1", "ar21_1", "mean", "acf1x", null}) {
            Assertions.assertNull(SeriesStats.parse(token), String.valueOf(token));
        }
        Assertions.assertNotNull(SeriesStats.parse("acf" + SeriesStats.MAX_LAG));
    }

    @Test
    public void testZeroCrossAndPeaks() {
        Assertions.assertEquals(3, SeriesStats.zeroCross(new double[]{1, -1, 2, 3, -4}));
        Assertions.assertEquals(1, SeriesStats.zeroCross(new double[]{1, 0, 0, -2}), "a zero has no sign and does not reset the last one");
        Assertions.assertEquals(0, SeriesStats.zeroCross(new double[]{0, 0, 5, 6}));
        Assertions.assertEquals(0, SeriesStats.zeroCross(new double[]{7}));
        Assertions.assertEquals(2, SeriesStats.peaks(new double[]{1, 3, 2, 5, 4}));
        Assertions.assertEquals(0, SeriesStats.peaks(new double[]{1, 2, 2, 1}), "a plateau is not a strict maximum");
        Assertions.assertEquals(0, SeriesStats.peaks(new double[]{5, 1}), "the ends never count");
        // through the readout: counts as longs, null on an empty series
        Assertions.assertEquals(3L, SeriesStats.read(SeriesStats.parse("zeroCross"), new double[]{1, -1, 2, 3, -4}));
        Assertions.assertEquals(2L, SeriesStats.read(SeriesStats.parse("peaks"), new double[]{1, 3, 2, 5, 4}));
        Assertions.assertNull(SeriesStats.read(SeriesStats.parse("peaks"), new double[0]));
        Assertions.assertEquals(0L, SeriesStats.read(SeriesStats.parse("zeroCross"), new double[]{4}));
    }

    /** x = 1..5: deviations −2..2, c₀ = 10; lag 1: 2 + 0 + 0 + 2 = 4, lag 2: 0 − 1 + 0 = −1. */
    @Test
    public void testAcf() {
        final double[] x = {1, 2, 3, 4, 5};
        Assertions.assertEquals(0.4, SeriesStats.acf(x, 1), 1e-12);
        Assertions.assertEquals(-0.1, SeriesStats.acf(x, 2), 1e-12);
        Assertions.assertEquals(0.4, (Double) SeriesStats.read(SeriesStats.parse("acf1"), x), 1e-12);
        // an alternating series is perfectly anti-correlated with itself one step back, up to the (n − 1) / n of the biased estimator
        Assertions.assertEquals(-5.0 / 6, SeriesStats.acf(new double[]{1, -1, 1, -1, 1, -1}, 1), 1e-12);
        // a level offset changes nothing (the estimator is centred)
        Assertions.assertEquals(0.4, SeriesStats.acf(new double[]{1e6 + 1, 1e6 + 2, 1e6 + 3, 1e6 + 4, 1e6 + 5}, 1), 1e-9);
        // undefined: not longer than the lag, or no spread
        Assertions.assertNull(SeriesStats.acf(new double[]{1, 2}, 2));
        Assertions.assertNull(SeriesStats.acf(new double[]{3, 3, 3, 3}, 1));
        Assertions.assertNull(SeriesStats.acf(new double[]{0.1, 0.1, 0.1, 0.1}, 1), "a constant that is not exactly representable");
        Assertions.assertNull(SeriesStats.acf(new double[0], 1));
    }

    /** Yule–Walker in closed form: φ₁ = r₁ at order one; φ₁ = r₁ (1 − r₂) / (1 − r₁²), φ₂ = (r₂ − r₁²) / (1 − r₁²) at order two. */
    @Test
    public void testYuleWalker() {
        final Random random = new Random(29);
        final double[] x = new double[200];
        for (int t = 2; t < x.length; t++) x[t] = 0.6 * x[t - 1] - 0.3 * x[t - 2] + random.nextGaussian();
        final double r1 = SeriesStats.acf(x, 1), r2 = SeriesStats.acf(x, 2);
        Assertions.assertArrayEquals(new double[]{r1}, SeriesStats.yuleWalker(x, 1), 1e-12);
        final double phi1 = r1 * (1 - r2) / (1 - r1 * r1), phi2 = (r2 - r1 * r1) / (1 - r1 * r1);
        Assertions.assertArrayEquals(new double[]{phi1, phi2}, SeriesStats.yuleWalker(x, 2), 1e-12);
        // the estimate recovers the generating coefficients (200 points: loosely)
        Assertions.assertEquals(0.6, phi1, 0.15);
        Assertions.assertEquals(-0.3, phi2, 0.15);
        // the readouts: ar<p>_<i> is the i-th coefficient, pacf<j> the last coefficient of the order-j fit
        Assertions.assertEquals(phi1, (Double) SeriesStats.read(SeriesStats.parse("ar2_1"), x), 1e-12);
        Assertions.assertEquals(phi2, (Double) SeriesStats.read(SeriesStats.parse("ar2_2"), x), 1e-12);
        Assertions.assertEquals(phi2, (Double) SeriesStats.read(SeriesStats.parse("pacf2"), x), 1e-12);
        Assertions.assertEquals(r1, (Double) SeriesStats.read(SeriesStats.parse("pacf1"), x), 1e-12);
        // order 3 satisfies its own Yule–Walker equations: r_j = Σ φ_i r_|j−i|
        final double[] phi = SeriesStats.yuleWalker(x, 3);
        final double[] r = {1, r1, r2, SeriesStats.acf(x, 3)};
        for (int j = 1; j <= 3; j++) {
            double sum = 0;
            for (int i = 1; i <= 3; i++) sum += phi[i - 1] * r[Math.abs(j - i)];
            Assertions.assertEquals(r[j], sum, 1e-12, "equation " + j);
        }
        // too short for the order / no spread: null, never NaN
        Assertions.assertNull(SeriesStats.yuleWalker(new double[]{1, 2, 3}, 3));
        Assertions.assertNull(SeriesStats.read(SeriesStats.parse("pacf2"), new double[]{5, 5, 5, 5, 5}));
    }

}
