package com.mercari.solution.util.pipeline.feature;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

public class RankDependenceTest {

    /** τ_b by its definition, over every pair: (concordant − discordant) / √((n0 − tx)(n0 − ty)). */
    private static Double kendallBruteForce(final double[] x, final double[] y, final int n) {
        long concordant = 0, discordant = 0, tiesX = 0, tiesY = 0;
        for (int i = 0; i < n; i++) {
            for (int j = i + 1; j < n; j++) {
                final double dx = Math.signum(x[i] - x[j]), dy = Math.signum(y[i] - y[j]);
                if (dx == 0) tiesX++;
                if (dy == 0) tiesY++;
                if (dx * dy > 0) concordant++;
                if (dx * dy < 0) discordant++;
            }
        }
        final long pairs = (long) n * (n - 1) / 2;
        final double denominator = Math.sqrt((double) (pairs - tiesX) * (pairs - tiesY));
        return denominator > 0 ? (concordant - discordant) / denominator : null;
    }

    private static Double pearson(final double[] x, final double[] y, final int n) {
        double mx = 0, my = 0;
        for (int i = 0; i < n; i++) {
            mx += x[i] / n;
            my += y[i] / n;
        }
        double sxy = 0, sxx = 0, syy = 0;
        for (int i = 0; i < n; i++) {
            sxy += (x[i] - mx) * (y[i] - my);
            sxx += (x[i] - mx) * (x[i] - mx);
            syy += (y[i] - my) * (y[i] - my);
        }
        return sxx > 0 && syy > 0 ? sxy / Math.sqrt(sxx * syy) : null;
    }

    /** Knight's O(n log n) τ_b equals the pairwise definition, ties in x, in y and in both included. */
    @Test
    public void testKendallMatchesTheDefinition() {
        final Random random = new Random(1);
        for (int trial = 0; trial < 200; trial++) {
            final int n = 2 + random.nextInt(60);
            final double[] x = new double[n], y = new double[n];
            // few distinct values: plenty of ties of every kind
            final int levels = 1 + random.nextInt(8);
            for (int i = 0; i < n; i++) {
                x[i] = random.nextInt(levels);
                y[i] = random.nextBoolean() ? x[i] + random.nextInt(3) : random.nextInt(levels);
            }
            final Double expected = kendallBruteForce(x, y, n);
            final Double actual = RankDependence.kendall(x, y, n);
            if (expected == null) Assertions.assertNull(actual, "trial " + trial);
            else Assertions.assertEquals(expected, actual, 1e-12, "trial " + trial);
        }
        // the closed cases: perfect agreement, reversal
        Assertions.assertEquals(1.0, RankDependence.kendall(new double[]{1, 2, 3, 4}, new double[]{10, 20, 30, 40}, 4), 1e-15);
        Assertions.assertEquals(-1.0, RankDependence.kendall(new double[]{1, 2, 3, 4}, new double[]{4, 3, 2, 1}, 4), 1e-15);
        // -0.0 and 0.0 tie (numeric equality), in x and in y: one concordant, one discordant pair, one tie
        final double[] zx = {-0.0, 0.0, 1}, zy = {5, 1, 3};
        Assertions.assertEquals(kendallBruteForce(zx, zy, 3), RankDependence.kendall(zx, zy, 3), 1e-12);
        Assertions.assertEquals(0.0, RankDependence.kendall(zx, zy, 3), 1e-12);
        Assertions.assertEquals(kendallBruteForce(zy, zx, 3), RankDependence.kendall(zy, zx, 3), 1e-12);
        Assertions.assertArrayEquals(new double[]{1.5, 1.5, 3}, RankDependence.midRanks(zx, 3));
    }

    /** Spearman = Pearson over the mid-ranks; invariant under a monotone transform of either series. */
    @Test
    public void testSpearmanIsThePearsonOfMidRanks() {
        final double[] x = {3, 1, 4, 1, 5, 9, 2, 6};
        final double[] y = {2, 7, 1, 8, 2, 8, 1, 8};
        final double[] rx = RankDependence.midRanks(x, 8), ry = RankDependence.midRanks(y, 8);
        // sorted 1, 1, 2, 3, 4, 5, 6, 9: the two 1s share ranks 1 and 2
        Assertions.assertArrayEquals(new double[]{4, 1.5, 5, 1.5, 6, 8, 3, 7}, rx);
        Assertions.assertEquals(pearson(rx, ry, 8), RankDependence.spearman(x, y, 8), 1e-12);
        final double[] logged = new double[8], cubed = new double[8];
        for (int i = 0; i < 8; i++) {
            logged[i] = Math.log(x[i]);
            cubed[i] = y[i] * y[i] * y[i];
        }
        Assertions.assertEquals(RankDependence.spearman(x, y, 8), RankDependence.spearman(logged, cubed, 8), 1e-12);
        Assertions.assertEquals(RankDependence.kendall(x, y, 8), RankDependence.kendall(logged, cubed, 8), 1e-12);
        // only the first n pairs are read
        Assertions.assertEquals(RankDependence.spearman(new double[]{1, 2, 3}, new double[]{3, 2, 1}, 3),
                RankDependence.spearman(new double[]{1, 2, 3, 99}, new double[]{3, 2, 1, 99}, 3));
    }

    /**
     * quadrantAsymmetry counts the corners of the pseudo-observations: positive when the series fall together more
     * often than they rise together, 0 for a symmetric relation, null while a corner is too small to read.
     */
    @Test
    public void testQuadrantAsymmetry() {
        // 20 pairs: u = rank / 21, so the lower corner at q = 0.75 is ranks 1..5 and the upper ranks 16..20
        final int n = 20;
        final double[] x = new double[n], y = new double[n];
        for (int i = 0; i < n; i++) {
            x[i] = i;
            // the bottom five of x are the bottom five of y; the top five of x sit in the middle of y (10..14)
            y[i] = i < 5 ? i : i < 10 ? i + 10 : i - 5;
        }
        // lower: x ranks 1..5 have y ranks 1..5 (5 pairs); upper: x ranks 16..20 have y = 10..14, ranks 11..15 (none)
        Assertions.assertEquals(5.0 / 20, RankDependence.quadrantAsymmetry(x, y, n, 0.75), 1e-15);
        // the mirror: rising together, not falling together
        final double[] nx = new double[n], ny = new double[n];
        for (int i = 0; i < n; i++) {
            nx[i] = -x[i];
            ny[i] = -y[i];
        }
        Assertions.assertEquals(-5.0 / 20, RankDependence.quadrantAsymmetry(nx, ny, n, 0.75), 1e-15);
        // perfect agreement: both corners full, no asymmetry
        Assertions.assertEquals(0.0, RankDependence.quadrantAsymmetry(x, x, n, 0.75), 1e-15);
        // (1 − q) n below MIN_TAIL: null (q = 0.8 over 20 pairs puts four in a corner)
        Assertions.assertNull(RankDependence.quadrantAsymmetry(x, y, n, 0.8));
        Assertions.assertNotNull(RankDependence.quadrantAsymmetry(x, y, n, 0.75));
        // at MIN_TAIL exactly, though 1 − q rounds below the decimal (1 − 0.9 = 0.0999…98, 1 − 0.8 = 0.1999…96)
        final double[] fifty = new double[50];
        for (int i = 0; i < 50; i++) fifty[i] = i;
        Assertions.assertNotNull(RankDependence.quadrantAsymmetry(fifty, fifty, 50, 0.9));
        Assertions.assertNotNull(RankDependence.quadrantAsymmetry(fifty, fifty, 25, 0.8));
        Assertions.assertNull(RankDependence.quadrantAsymmetry(fifty, fifty, 49, 0.9));
        // the corners mirror each other whatever q rounds to: a window in perfect agreement is symmetric
        for (final double q : new double[]{0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95}) {
            for (int m = 2; m <= 50; m++) {
                final Double value = RankDependence.quadrantAsymmetry(fifty, fifty, m, q);
                if (value != null) Assertions.assertEquals(0.0, value, 1e-15, "q " + q + ", n " + m);
            }
        }
        // Gaussian pairs: elliptical, so near 0
        final Random random = new Random(2);
        final int m = 20_000;
        final double[] gx = new double[m], gy = new double[m];
        for (int i = 0; i < m; i++) {
            gx[i] = random.nextGaussian();
            gy[i] = 0.6 * gx[i] + 0.8 * random.nextGaussian();
        }
        Assertions.assertEquals(0.0, RankDependence.quadrantAsymmetry(gx, gy, m, 0.75), 0.01);
    }

    @Test
    public void testDegenerateWindowsAreNull() {
        for (final String func : RankDependence.FUNCS) {
            Assertions.assertNull(RankDependence.of(func, new double[]{1}, new double[]{2}, 1, 0.75), func + ": one pair");
            Assertions.assertNull(RankDependence.of(func, new double[0], new double[0], 0, 0.75), func + ": no pair");
            final double[] constant = new double[30], varying = new double[30];
            for (int i = 0; i < 30; i++) varying[i] = i;
            Assertions.assertNull(RankDependence.of(func, constant, varying, 30, 0.75), func + ": a constant series");
            Assertions.assertNull(RankDependence.of(func, varying, constant, 30, 0.75), func + ": a constant series");
        }
    }

    /**
     * The cell funcs are their regression namesakes over the sample the counts stand for: a table of counts expanded
     * into pairs (a cell = a tie in both series) gives the same τ_b and Spearman, and so does the table as shares.
     */
    @Test
    public void testCellFuncsAreTheSampleLimits() {
        final Random random = new Random(3);
        for (int trial = 0; trial < 200; trial++) {
            final int bx = 2 + random.nextInt(5), by = 2 + random.nextInt(5);
            final Map<String, Object> counts = new HashMap<>(), shares = new HashMap<>();
            final java.util.List<double[]> pairs = new java.util.ArrayList<>();
            int total = 0;
            for (int i = 0; i < bx; i++) {
                for (int j = 0; j < by; j++) {
                    // sparse, skewed tables: empty cells, a dominant diagonal now and then
                    final int n = random.nextInt(3) == 0 ? 0 : random.nextInt(i == j ? 12 : 4);
                    if (n == 0) continue;
                    counts.put(i + "|" + j, (long) n);
                    for (int k = 0; k < n; k++) pairs.add(new double[]{i, j});
                    total += n;
                }
            }
            for (final Map.Entry<String, Object> e : counts.entrySet()) shares.put(e.getKey(), ((Long) e.getValue()) / (double) total);
            final double[] x = new double[pairs.size()], y = new double[pairs.size()];
            for (int k = 0; k < pairs.size(); k++) {
                x[k] = pairs.get(k)[0];
                y[k] = pairs.get(k)[1];
            }
            for (final String func : java.util.List.of(RankDependence.KENDALL, RankDependence.SPEARMAN)) {
                final Double sample = RankDependence.of(func, x, y, x.length, 0.75);
                final Double fromCounts = RankDependence.ofCells(func, counts, 0.75);
                final Double fromShares = RankDependence.ofCells(func, shares, 0.75);
                if (sample == null) {
                    Assertions.assertNull(fromCounts, func + " trial " + trial);
                    Assertions.assertNull(fromShares, func + " trial " + trial);
                } else {
                    Assertions.assertEquals(sample, fromCounts, 1e-12, func + " trial " + trial);
                    Assertions.assertEquals(sample, fromShares, 1e-12, func + " trial " + trial);
                }
            }
        }
    }

    /**
     * The corners of a 5 × 5 table of quantile bins: every code holds a fifth, so the mid-rank shares are 0.1 .. 0.9 and
     * at q = 0.75 the corners are the extreme bins.
     */
    @Test
    public void testCellCorners() {
        // counts over 30 pairs, every code 6: the lowest bins move together (x = 0 -> y = 0), the highest half the time
        final Map<String, Object> cells = new LinkedHashMap<>();
        cells.put("0|0", 6L);
        cells.put("1|1", 6L);
        final long[][] middle = {{3, 1, 2}, {2, 3, 1}, {1, 2, 3}};
        for (int i = 0; i < 3; i++) for (int j = 0; j < 3; j++) cells.put((i + 2) + "|" + (j + 2), middle[i][j]);
        Assertions.assertEquals(1.0, RankDependence.ofCells(RankDependence.LOWER_TAIL, cells, 0.75), 1e-12);
        Assertions.assertEquals(0.5, RankDependence.ofCells(RankDependence.UPPER_TAIL, cells, 0.75), 1e-12);
        // both lower 6 / 30, both upper 3 / 30
        Assertions.assertEquals(0.1, RankDependence.ofCells(RankDependence.QUADRANT_ASYMMETRY, cells, 0.75), 1e-12);
        // independence: the tails are the corner's mass, no asymmetry
        final Map<String, Object> independent = new HashMap<>();
        for (int i = 0; i < 5; i++) for (int j = 0; j < 5; j++) independent.put(i + "|" + j, 1L);
        Assertions.assertEquals(0.2, RankDependence.ofCells(RankDependence.LOWER_TAIL, independent, 0.75), 1e-12);
        Assertions.assertEquals(0.2, RankDependence.ofCells(RankDependence.UPPER_TAIL, independent, 0.75), 1e-12);
        Assertions.assertEquals(0.0, RankDependence.ofCells(RankDependence.QUADRANT_ASYMMETRY, independent, 0.75), 1e-12);
        Assertions.assertEquals(0.0, RankDependence.ofCells(RankDependence.KENDALL, independent, 0.75), 1e-12);
        // q = 0.9: a fifth's mid-rank share (0.1) is not below 0.1 — the corner holds no code
        Assertions.assertNull(RankDependence.ofCells(RankDependence.LOWER_TAIL, independent, 0.9));
        Assertions.assertNull(RankDependence.ofCells(RankDependence.QUADRANT_ASYMMETRY, independent, 0.9));
        // q = 0.6: the two extreme bins on each side (mid-rank shares 0.1, 0.3 < 0.4)
        Assertions.assertEquals(0.4, RankDependence.ofCells(RankDependence.LOWER_TAIL, independent, 0.6), 1e-12);
    }

    @Test
    public void testCellsThatAreNotATableAreNull() {
        for (final String func : RankDependence.CELL_FUNCS) {
            Assertions.assertNull(RankDependence.ofCells(func, null, 0.75), func);
            Assertions.assertNull(RankDependence.ofCells(func, Map.of(), 0.75), func);
            // a one-dimensional distribution (a transitionStats map), text codes, a third component, a negative share
            Assertions.assertNull(RankDependence.ofCells(func, Map.of("0", 0.5, "1", 0.5), 0.75), func);
            Assertions.assertNull(RankDependence.ofCells(func, Map.of("good|fair", 0.5, "fair|good", 0.5), 0.75), func);
            Assertions.assertNull(RankDependence.ofCells(func, Map.of("0|0|0", 0.5, "1|1|1", 0.5), 0.75), func);
            Assertions.assertNull(RankDependence.ofCells(func, Map.of("0|0", -0.5, "1|1", 1.5), 0.75), func);
            // a single value in one series
            Assertions.assertNull(RankDependence.ofCells(func, Map.of("0|0", 0.5, "0|1", 0.5), 0.75), func);
        }
        // -0.0 is the code 0; a zero-mass cell is no code at all
        final Map<String, Object> signed = Map.of("-0.0|0", 1L, "0|1", 1L, "1|0", 1L, "1|1", 3L, "2|5", 0L);
        final Map<String, Object> plain = Map.of("0|0", 1L, "0|1", 1L, "1|0", 1L, "1|1", 3L);
        Assertions.assertEquals(RankDependence.ofCells(RankDependence.KENDALL, plain, 0.75), RankDependence.ofCells(RankDependence.KENDALL, signed, 0.75));
    }
}
