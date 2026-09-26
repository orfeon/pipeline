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
}
