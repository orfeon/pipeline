package com.mercari.solution.util.pipeline.glm;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;

public class StatMathTest {

    @Test
    public void testErfc() {
        Assertions.assertEquals(1d, StatMath.erfc(0d), 1e-15);
        Assertions.assertEquals(0.4795001221869535, StatMath.erfc(0.5), 1e-13);
        Assertions.assertEquals(0.15729920705028513, StatMath.erfc(1d), 1e-13);
        Assertions.assertEquals(0.004677734981047266, StatMath.erfc(2d), 1e-14);
        Assertions.assertEquals(2.209049699858544e-05, StatMath.erfc(3d), 1e-17);
        Assertions.assertEquals(1.5374597944280349e-12, StatMath.erfc(5d), 1e-24);
        Assertions.assertEquals(2d - 0.15729920705028513, StatMath.erfc(-1d), 1e-13);
        // continuity at the series / continued-fraction switch
        Assertions.assertEquals(StatMath.erfc(2.4999999), StatMath.erfc(2.5000001), 1e-9);
    }

    @Test
    public void testChiSquareTails() {
        // P(chi2(1) > 3.841459) = 0.05
        Assertions.assertEquals(0.05, StatMath.chiSquare1UpperTail(3.841458820694124), 1e-10);
        Assertions.assertEquals(3.841458820694124, StatMath.chiSquare1Quantile(0.95), 1e-9);
        Assertions.assertEquals(6.634896601021213, StatMath.chiSquare1Quantile(0.99), 1e-9);
        Assertions.assertEquals(1d, StatMath.chiSquare1UpperTail(0d));
    }

    /**
     * The lower tail computed directly: equal to 1 − the upper tail where both are representable, and with its digits
     * near 0, where 1 − Q has none left (df = 2 has the closed form 1 − e^{−x/2}, df = 1 erf(sqrt(x / 2))).
     */
    @Test
    public void testChiSquareCdf() {
        for (final int df : new int[]{1, 2, 3, 5, 20, 60}) {
            for (final double x : new double[]{0.05, 0.5, 1, 3, 10, 30, 80}) {
                Assertions.assertEquals(1 - StatMath.chiSquareUpperTail(x, df), StatMath.chiSquareCdf(x, df), 1e-12, "df " + df + ", x " + x);
            }
        }
        Assertions.assertEquals(0d, StatMath.chiSquareCdf(0, 3));
        Assertions.assertEquals(0d, StatMath.chiSquareCdf(-1, 3));
        Assertions.assertTrue(Double.isNaN(StatMath.chiSquareCdf(Double.NaN, 3)));
        // near 0: relative precision, where 1 − Q reads 0 or noise
        final double tiny = 1e-14;
        Assertions.assertEquals(-Math.expm1(-tiny / 2), StatMath.chiSquareCdf(tiny, 2), 1e-12 * tiny);
        // df = 1 near 0: erf(sqrt(x / 2)) ≈ sqrt(2x / π)
        Assertions.assertEquals(Math.sqrt(2 * tiny / Math.PI), StatMath.chiSquareCdf(tiny, 1), 1e-6 * Math.sqrt(2 * tiny / Math.PI));
        // a central row in 20 dimensions: P(chi2(20) <= 1) ≈ 1.7e-10, its digits intact
        final double p = StatMath.chiSquareCdf(1, 20);
        Assertions.assertTrue(p > 1e-10 && p < 1e-9, "P(chi2(20) <= 1) = " + p);
    }

    @Test
    public void testLogChiSquareTailsPastTheUnderflow() {
        // the log of the tail where the tail itself is representable
        for (final int df : new int[]{1, 2, 5, 30}) {
            for (final double chi2 : new double[]{0.5, 3, 40, 400}) {
                Assertions.assertEquals(Math.log(StatMath.chiSquareUpperTail(chi2, df)), StatMath.logChiSquareUpperTail(chi2, df),
                        1e-9 * Math.max(1d, Math.abs(Math.log(StatMath.chiSquareUpperTail(chi2, df)))), "df " + df + " chi2 " + chi2);
            }
        }
        Assertions.assertEquals(0d, StatMath.logChiSquareUpperTail(0d, 3));
        // past it (z = 40: the tail is 0 as a double) the logs stay finite and ordered — a χ²(df) statistic compares
        // with the z tail there
        Assertions.assertEquals(0d, StatMath.chiSquare1UpperTail(1600));
        final double z40 = StatMath.logChiSquareUpperTail(1600, 1);
        Assertions.assertTrue(Double.isFinite(z40) && z40 < -800 && z40 > -810, Double.toString(z40));
        Assertions.assertTrue(StatMath.logChiSquareUpperTail(1700, 4) < z40);
        Assertions.assertTrue(StatMath.logChiSquareUpperTail(1500, 4) > z40);
    }

    @Test
    public void testInverseNormal() {
        Assertions.assertEquals(0d, StatMath.inverseNormal(0.5), 1e-15);
        Assertions.assertEquals(1.959963984540054, StatMath.inverseNormal(0.975), 1e-12);
        Assertions.assertEquals(-2.3263478740408408, StatMath.inverseNormal(0.01), 1e-12);
        Assertions.assertEquals(3.090232306167813, StatMath.inverseNormal(0.999), 1e-11);
    }

    @Test
    public void testQuantile() {
        final double[] sorted = {1, 2, 3, 4, 5};
        Assertions.assertEquals(3d, StatMath.quantile(sorted, 0.5));
        Assertions.assertEquals(4.96, StatMath.quantile(sorted, 0.99), 1e-12);
        Assertions.assertEquals(1d, StatMath.quantile(sorted, 0d));
        Assertions.assertEquals(5d, StatMath.quantile(sorted, 1d));
        Assertions.assertTrue(Double.isNaN(StatMath.quantile(new double[0], 0.5)));
        Assertions.assertEquals(2.5, StatMath.medianFinite(new double[]{4, Double.NaN, 1, 3, 2}));
    }

    @Test
    public void testBenjaminiHochberg() {
        final double[] q = StatMath.benjaminiHochberg(new double[]{0.01, 0.04, 0.03, 0.5, Double.NaN});
        // sorted p: 0.01 (rank 1), 0.03 (2), 0.04 (3), 0.5 (4); m = 4
        Assertions.assertEquals(0.04, q[0], 1e-12);            // min(0.01*4/1, 0.06, 0.0533, 0.5) = 0.04
        Assertions.assertEquals(0.05333333333333333, q[1], 1e-12); // 0.04*4/3
        Assertions.assertEquals(0.05333333333333333, q[2], 1e-12); // min(0.03*4/2 = 0.06, 0.0533)
        Assertions.assertEquals(0.5, q[3], 1e-12);
        Assertions.assertTrue(Double.isNaN(q[4]));
    }

    @Test
    public void testPeriodBucket() {
        final long t = Instant.parse("2025-05-17T10:00:00Z").toEpochMilli();
        Assertions.assertEquals("2025", StatMath.periodBucket(t, "year"));
        Assertions.assertEquals("2025-Q2", StatMath.periodBucket(t, "quarter"));
        Assertions.assertEquals("2025-05", StatMath.periodBucket(t, "month"));
        Assertions.assertEquals("2025-W20", StatMath.periodBucket(t, "week"));
        Assertions.assertEquals("2025-05-17", StatMath.periodBucket(t, "day"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> StatMath.periodBucket(t, "decade"));
    }

    @Test
    public void testGlob() {
        Assertions.assertTrue(StatMath.glob("f_*").matcher("f_price").matches());
        Assertions.assertFalse(StatMath.glob("f_*").matcher("g_price").matches());
        Assertions.assertTrue(StatMath.glob("*").matcher("anything.with.dots").matches());
        Assertions.assertTrue(StatMath.glob("a?c").matcher("abc").matches());
        Assertions.assertFalse(StatMath.glob("a.c").matcher("abc").matches());
    }

    /** Reference values from scipy.special.betainc; the closed forms I_x(1, b) = 1 − (1 − x)^b and I_x(a, 1) = x^a. */
    @Test
    public void testRegularizedBeta() {
        Assertions.assertEquals(0.0189271240719457, StatMath.regularizedBeta(2.5, 0.5, 0.3), 1e-14);
        Assertions.assertEquals(0.795167235300867, StatMath.regularizedBeta(0.5, 0.5, 0.9), 1e-13);
        Assertions.assertEquals(0.231787008, StatMath.regularizedBeta(7, 3, 0.6), 1e-12);
        Assertions.assertEquals(4.2441330890241e-10, StatMath.regularizedBeta(1.5, 0.5, 1e-6), 1e-22, "a small lower tail keeps its digits");
        for (final double x : new double[]{0.01, 0.3, 0.77}) {
            Assertions.assertEquals(1 - Math.pow(1 - x, 3.5), StatMath.regularizedBeta(1, 3.5, x), 1e-14);
            Assertions.assertEquals(Math.pow(x, 2.25), StatMath.regularizedBeta(2.25, 1, x), 1e-14);
        }
        Assertions.assertEquals(0d, StatMath.regularizedBeta(2, 3, 0));
        Assertions.assertEquals(1d, StatMath.regularizedBeta(2, 3, 1));
        // the inverse round-trips, including far into the tails and with b < 1 (the t quantile's shape)
        for (final double[] ab : new double[][]{{2.5, 0.5}, {0.5, 0.5}, {7, 3}, {1.25, 0.5}, {40, 0.5}, {0.3, 2}}) {
            for (final double p : new double[]{1e-15, 1e-9, 1e-4, 0.01, 0.2, 0.5, 0.8, 0.99, 1 - 1e-6}) {
                final double x = StatMath.inverseRegularizedBeta(ab[0], ab[1], p);
                Assertions.assertTrue(x > 0 && x < 1, ab[0] + "," + ab[1] + " p=" + p + " x=" + x);
                // to the digits x itself has: near 1 with b < 1 the density is steep, so one ulp of x moves p by more than 1e-12
                final double density = Math.exp((ab[0] - 1) * Math.log(x) + (ab[1] - 1) * Math.log(1 - x) + StatMath.logGamma(ab[0] + ab[1]) - StatMath.logGamma(ab[0]) - StatMath.logGamma(ab[1]));
                Assertions.assertEquals(p, StatMath.regularizedBeta(ab[0], ab[1], x), 1e-11 * Math.max(p, 1e-3) + 2 * Math.ulp(x) * density, ab[0] + "," + ab[1] + " p=" + p);
            }
        }
        Assertions.assertEquals(0d, StatMath.inverseRegularizedBeta(2, 3, 0));
        Assertions.assertEquals(1d, StatMath.inverseRegularizedBeta(2, 3, 1));
    }

    /** Reference values from scipy.stats.t / f / gamma. */
    @Test
    public void testStudentTAndF() {
        Assertions.assertEquals(0.0333832724059941, StatMath.studentTCdf(-2.5, 4), 1e-14);
        Assertions.assertEquals(0.849756605364646, StatMath.studentTCdf(1.3, 2.5), 1e-14);
        Assertions.assertEquals(4.06764021358198e-05, StatMath.studentTCdf(-30, 3), 1e-18);
        Assertions.assertEquals(0.5, StatMath.studentTCdf(0, 7));
        // df = 1 is the Cauchy, an infinite df the normal
        Assertions.assertEquals(0.5 + Math.atan(1.7) / Math.PI, StatMath.studentTCdf(1.7, 1), 1e-13);
        Assertions.assertEquals(StatMath.studentTCdf(1.7, Double.POSITIVE_INFINITY), 0.5 * StatMath.erfc(-1.7 / Math.sqrt(2)), 1e-15);
        Assertions.assertEquals(2.77644510519779, StatMath.studentTQuantile(0.975, 4), 1e-11);
        Assertions.assertEquals(-10.2145318524074, StatMath.studentTQuantile(0.001, 3), 1e-10);
        Assertions.assertEquals(-3489.58339536998, StatMath.studentTQuantile(1e-9, 2.5), 1e-7);
        Assertions.assertEquals(0.527760452706597, StatMath.studentTQuantile(0.7, 50), 1e-12);
        Assertions.assertEquals(0d, StatMath.studentTQuantile(0.5, 3));
        Assertions.assertEquals(Double.NEGATIVE_INFINITY, StatMath.studentTQuantile(0, 3));
        for (final double df : new double[]{1, 2.5, 4, 30}) {
            for (final double p : new double[]{1e-12, 1e-6, 0.03, 0.4, 0.5, 0.6, 0.97, 1 - 1e-6}) {
                Assertions.assertEquals(p, StatMath.studentTCdf(StatMath.studentTQuantile(p, df), df), 1e-12 * Math.max(p, 1e-3), "df " + df + " p " + p);
            }
        }
        Assertions.assertEquals(0.743612802471824, StatMath.fCdf(2.0, 3, 4), 1e-13);
        Assertions.assertEquals(0.21495909499123, StatMath.fCdf(0.25, 2, 7.5), 1e-13);
        Assertions.assertEquals(0d, StatMath.fCdf(0, 2, 3));
        // F(1, df) is the square of a t(df); F(k, inf) is chi2(k) / k
        Assertions.assertEquals(2 * StatMath.studentTCdf(2.5, 4) - 1, StatMath.fCdf(6.25, 1, 4), 1e-13);
        Assertions.assertEquals(StatMath.chiSquareCdf(7.5, 3), StatMath.fCdf(2.5, 3, Double.POSITIVE_INFINITY), 1e-15);
        Assertions.assertEquals(StatMath.chiSquareCdf(7.5, 3), StatMath.chiSquareCdfReal(7.5, 3), 0d);
        Assertions.assertEquals(1.41422022908297e-05, StatMath.gammaQuantile(2, 1e-10), 1e-18);
        Assertions.assertEquals(0.172519800333608, StatMath.gammaQuantile(0.7, 0.3), 1e-13);
        Assertions.assertEquals(35.2286297490232, StatMath.gammaQuantile(5.5, 1 - 1e-10), 1e-9);
    }
}
