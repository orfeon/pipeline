package com.mercari.solution.util.pipeline.screen;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;

public class ScreenMathTest {

    @Test
    public void testErfc() {
        Assertions.assertEquals(1d, ScreenMath.erfc(0d), 1e-15);
        Assertions.assertEquals(0.4795001221869535, ScreenMath.erfc(0.5), 1e-13);
        Assertions.assertEquals(0.15729920705028513, ScreenMath.erfc(1d), 1e-13);
        Assertions.assertEquals(0.004677734981047266, ScreenMath.erfc(2d), 1e-14);
        Assertions.assertEquals(2.209049699858544e-05, ScreenMath.erfc(3d), 1e-17);
        Assertions.assertEquals(1.5374597944280349e-12, ScreenMath.erfc(5d), 1e-24);
        Assertions.assertEquals(2d - 0.15729920705028513, ScreenMath.erfc(-1d), 1e-13);
        // continuity at the series / continued-fraction switch
        Assertions.assertEquals(ScreenMath.erfc(2.4999999), ScreenMath.erfc(2.5000001), 1e-9);
    }

    @Test
    public void testChiSquareTails() {
        // P(chi2(1) > 3.841459) = 0.05
        Assertions.assertEquals(0.05, ScreenMath.chiSquare1UpperTail(3.841458820694124), 1e-10);
        Assertions.assertEquals(3.841458820694124, ScreenMath.chiSquare1Quantile(0.95), 1e-9);
        Assertions.assertEquals(6.634896601021213, ScreenMath.chiSquare1Quantile(0.99), 1e-9);
        Assertions.assertEquals(1d, ScreenMath.chiSquare1UpperTail(0d));
    }

    @Test
    public void testInverseNormal() {
        Assertions.assertEquals(0d, ScreenMath.inverseNormal(0.5), 1e-15);
        Assertions.assertEquals(1.959963984540054, ScreenMath.inverseNormal(0.975), 1e-12);
        Assertions.assertEquals(-2.3263478740408408, ScreenMath.inverseNormal(0.01), 1e-12);
        Assertions.assertEquals(3.090232306167813, ScreenMath.inverseNormal(0.999), 1e-11);
    }

    @Test
    public void testQuantile() {
        final double[] sorted = {1, 2, 3, 4, 5};
        Assertions.assertEquals(3d, ScreenMath.quantile(sorted, 0.5));
        Assertions.assertEquals(4.96, ScreenMath.quantile(sorted, 0.99), 1e-12);
        Assertions.assertEquals(1d, ScreenMath.quantile(sorted, 0d));
        Assertions.assertEquals(5d, ScreenMath.quantile(sorted, 1d));
        Assertions.assertTrue(Double.isNaN(ScreenMath.quantile(new double[0], 0.5)));
        Assertions.assertEquals(2.5, ScreenMath.medianFinite(new double[]{4, Double.NaN, 1, 3, 2}));
    }

    @Test
    public void testBenjaminiHochberg() {
        final double[] q = ScreenMath.benjaminiHochberg(new double[]{0.01, 0.04, 0.03, 0.5, Double.NaN});
        // sorted p: 0.01 (rank 1), 0.03 (2), 0.04 (3), 0.5 (4); m = 4
        Assertions.assertEquals(0.04, q[0], 1e-12);            // min(0.01*4/1, 0.06, 0.0533, 0.5) = 0.04
        Assertions.assertEquals(0.05333333333333333, q[1], 1e-12); // 0.04*4/3
        Assertions.assertEquals(0.05333333333333333, q[2], 1e-12); // min(0.03*4/2 = 0.06, 0.0533)
        Assertions.assertEquals(0.5, q[3], 1e-12);
        Assertions.assertTrue(Double.isNaN(q[4]));
    }

    @Test
    public void testSeededRandomIsDeterministic() {
        Assertions.assertEquals(ScreenMath.seededRandom(7, "a").nextLong(), ScreenMath.seededRandom(7, "a").nextLong());
        Assertions.assertNotEquals(ScreenMath.seededRandom(7, "a").nextLong(), ScreenMath.seededRandom(8, "a").nextLong());
        Assertions.assertNotEquals(ScreenMath.seededRandom(7, "a").nextLong(), ScreenMath.seededRandom(7, "b").nextLong());
    }

    @Test
    public void testPeriodBucket() {
        final long t = Instant.parse("2025-05-17T10:00:00Z").toEpochMilli();
        Assertions.assertEquals("2025", ScreenMath.periodBucket(t, "year"));
        Assertions.assertEquals("2025-Q2", ScreenMath.periodBucket(t, "quarter"));
        Assertions.assertEquals("2025-05", ScreenMath.periodBucket(t, "month"));
        Assertions.assertEquals("2025-W20", ScreenMath.periodBucket(t, "week"));
        Assertions.assertEquals("2025-05-17", ScreenMath.periodBucket(t, "day"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> ScreenMath.periodBucket(t, "decade"));
    }

    @Test
    public void testGlob() {
        Assertions.assertTrue(ScreenMath.glob("f_*").matcher("f_price").matches());
        Assertions.assertFalse(ScreenMath.glob("f_*").matcher("g_price").matches());
        Assertions.assertTrue(ScreenMath.glob("*").matcher("anything.with.dots").matches());
        Assertions.assertTrue(ScreenMath.glob("a?c").matcher("abc").matches());
        Assertions.assertFalse(ScreenMath.glob("a.c").matcher("abc").matches());
    }

    @Test
    public void testCoercions() {
        Assertions.assertEquals(1d, ScreenMath.toDouble(true));
        Assertions.assertEquals(2.5, ScreenMath.toDouble("2.5"));
        Assertions.assertNull(ScreenMath.toDouble("x"));
        Assertions.assertEquals(1_700_000_000_000L, ScreenMath.toEpochMillis(1_700_000_000_000_000L, "timestamp"));
        Assertions.assertEquals(86_400_000L * 2, ScreenMath.toEpochMillis(2, "date"));
        Assertions.assertEquals(Instant.parse("2025-01-01T00:00:00Z").toEpochMilli(), ScreenMath.toEpochMillis("2025-01-01T00:00:00Z", null));
    }
}
