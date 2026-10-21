package com.mercari.solution.util.pipeline.feature;

import java.util.Arrays;
import java.util.List;

/**
 * Rank statistics of the dependence between two series over one window — the {@code regression} funcs
 * {@code spearman}, {@code kendall} and {@code quadrantAsymmetry}. They read the window's pairs as ranks, so a monotone
 * transform of either series changes nothing, and they have no {@link Summary} family: an event entering the window
 * moves every other event's rank, so the window is ranked again at every row (the scan path, bounded by the window's
 * {@code maxEvents} / {@code maxAge}).
 *
 * <ul>
 *   <li>{@link #spearman}: the Pearson correlation of the mid-ranks (ties share the mean of their ranks);</li>
 *   <li>{@link #kendall}: τ_b, the tie-corrected Kendall correlation, in O(n log n) (Knight 1966: sort by x, count the
 *       exchanges a merge sort by y needs);</li>
 *   <li>{@link #quadrantAsymmetry}: {@code (#{u_x < 1 − q ∧ u_y < 1 − q} − #{u_x > q ∧ u_y > q}) / n} over the pairs'
 *       pseudo-observations {@code u = midrank / (n + 1)} — positive when the series fall together more often than they
 *       rise together. Its expectation is 0 under any elliptical copula (normal, t), so it is an asymmetry no correlation
 *       can express, without assuming a family.</li>
 * </ul>
 *
 * Every statistic is null with fewer than two pairs or when a series is constant over them; {@code quadrantAsymmetry}
 * is also null while a corner holds fewer than {@link #MIN_TAIL} pairs in expectation ({@code (1 − q) n < MIN_TAIL}),
 * where it is noise. The values are finite (the evaluator drops a pair with a missing one); ties are numeric equality,
 * so {@code -0.0} and {@code 0.0} tie.
 */
public final class RankDependence {

    public static final String SPEARMAN = "spearman";
    public static final String KENDALL = "kendall";
    public static final String QUADRANT_ASYMMETRY = "quadrantAsymmetry";
    /** The regression funcs computed here (scan-only). */
    public static final List<String> FUNCS = List.of(SPEARMAN, KENDALL, QUADRANT_ASYMMETRY);
    public static final double DEFAULT_Q = 0.75;
    /** quadrantAsymmetry is null while {@code (1 − q) n} is below this: a corner of a few pairs is noise. */
    public static final int MIN_TAIL = 5;
    /**
     * The slack of the comparisons against {@code (1 − q)}: a decimal q is not exact in binary ({@code 1 − 0.9} is
     * {@code 0.0999…98}), so a product that equals a whole number or a rank would otherwise round to either side.
     */
    private static final double EPSILON = 1e-9;

    private RankDependence() {
    }

    /** The func over the first {@code n} pairs ({@code x[i]}, {@code y[i]}); {@code q} is read by quadrantAsymmetry only. */
    public static Double of(final String func, final double[] x, final double[] y, final int n, final double q) {
        return switch (func) {
            case SPEARMAN -> spearman(x, y, n);
            case KENDALL -> kendall(x, y, n);
            case QUADRANT_ASYMMETRY -> quadrantAsymmetry(x, y, n, q);
            default -> throw new IllegalArgumentException("not a rank dependence func: " + func);
        };
    }

    public static Double spearman(final double[] x, final double[] y, final int n) {
        if (n < 2) return null;
        final double[] rx = midRanks(x, n), ry = midRanks(y, n);
        // the mean of the mid-ranks is (n + 1) / 2 whatever the ties
        final double mean = (n + 1) / 2d;
        double sxy = 0, sxx = 0, syy = 0;
        for (int i = 0; i < n; i++) {
            final double dx = rx[i] - mean, dy = ry[i] - mean;
            sxy += dx * dy;
            sxx += dx * dx;
            syy += dy * dy;
        }
        if (!(sxx > 0) || !(syy > 0)) return null;
        return sxy / Math.sqrt(sxx * syy);
    }

    public static Double kendall(final double[] x, final double[] y, final int n) {
        if (n < 2) return null;
        // τ reads the order alone: each pair as (rank of x, rank of y) packed into one primitive key, sorted by x then y
        // (no boxed index sort per row; ranks tie by numeric equality, -0.0 with 0.0 included)
        final int[] rx = lowerRanks(x, n), ry = lowerRanks(y, n);
        final long[] keys = new long[n];
        for (int i = 0; i < n; i++) keys[i] = (long) rx[i] * n + ry[i];
        Arrays.sort(keys);
        final long pairs = (long) n * (n - 1) / 2;
        // ties in x (runs of the x rank), and joint ties in (x, y) (runs of the whole key)
        final long tiesX = tiedPairs(keys, n, true), tiesXY = tiedPairs(keys, n, false);
        final int[] ys = new int[n];
        for (int i = 0; i < n; i++) ys[i] = (int) (keys[i] % n);
        // the exchanges a stable merge sort by y needs = the discordant pairs (pairs tied in x are already in y order)
        final long swaps = mergeSort(ys, new int[n], 0, n);
        long tiesY = 0;
        for (int i = 0; i < n; ) {
            int j = i;
            while (j + 1 < n && ys[j + 1] == ys[i]) j++;
            final long t = j - i + 1;
            tiesY += t * (t - 1) / 2;
            i = j + 1;
        }
        final double denominator = Math.sqrt((double) (pairs - tiesX) * (pairs - tiesY));
        if (!(denominator > 0)) return null;
        return (pairs - tiesX - tiesY + tiesXY - 2d * swaps) / denominator;
    }

    public static Double quadrantAsymmetry(final double[] x, final double[] y, final int n, final double q) {
        if (n < 2 || (1 - q) * n < MIN_TAIL - EPSILON) return null;
        final double[] rx = midRanks(x, n), ry = midRanks(y, n);
        if (constant(rx, n) || constant(ry, n)) return null;
        // u < 1 − q ⇔ rank < (1 − q)(n + 1), and u > q ⇔ (n + 1) − rank < (1 − q)(n + 1): one threshold for both
        // corners, so they mirror each other exactly (comparing u with 1 − q and with q apart rounds the two corners
        // differently, e.g. at q = 0.7, and reads an asymmetry into a perfectly symmetric window)
        final double corner = (1 - q) * (n + 1) - EPSILON;
        long lower = 0, upper = 0;
        for (int i = 0; i < n; i++) {
            if (rx[i] < corner && ry[i] < corner) lower++;
            if (n + 1 - rx[i] < corner && n + 1 - ry[i] < corner) upper++;
        }
        return (double) (lower - upper) / n;
    }

    /** The mid-ranks (1-based; ties share the mean of their ranks) of the first {@code n} values. */
    static double[] midRanks(final double[] v, final int n) {
        final double[] sorted = Arrays.copyOf(v, n);
        Arrays.sort(sorted);
        final double[] ranks = new double[n];
        // a value's ties occupy the ranks below + 1 .. notAbove
        for (int i = 0; i < n; i++) ranks[i] = (below(sorted, v[i]) + notAbove(sorted, v[i]) + 1) / 2d;
        return ranks;
    }

    /** Each of the first {@code n} values' count of values strictly below it (0-based; ties share it). */
    private static int[] lowerRanks(final double[] v, final int n) {
        final double[] sorted = Arrays.copyOf(v, n);
        Arrays.sort(sorted);
        final int[] ranks = new int[n];
        for (int i = 0; i < n; i++) ranks[i] = below(sorted, v[i]);
        return ranks;
    }

    /** The number of {@code sorted} values numerically below {@code value} ({@code <}: -0.0 is not below 0.0). */
    private static int below(final double[] sorted, final double value) {
        int lo = 0, hi = sorted.length;
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if (sorted[mid] < value) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    /** The number of {@code sorted} values numerically at most {@code value}. */
    private static int notAbove(final double[] sorted, final double value) {
        int lo = 0, hi = sorted.length;
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if (sorted[mid] <= value) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    /** The tied pairs of the sorted {@code (x rank) · n + (y rank)} keys: in x alone, or in both. */
    private static long tiedPairs(final long[] keys, final int n, final boolean xOnly) {
        long ties = 0;
        for (int i = 0; i < keys.length; ) {
            int j = i;
            while (j + 1 < keys.length && (xOnly ? keys[j + 1] / n == keys[i] / n : keys[j + 1] == keys[i])) j++;
            final long t = j - i + 1;
            ties += t * (t - 1) / 2;
            i = j + 1;
        }
        return ties;
    }

    private static boolean constant(final double[] ranks, final int n) {
        for (int i = 1; i < n; i++) if (ranks[i] != ranks[0]) return false;
        return true;
    }

    /** Sorts {@code v[from, to)} ascending, returning the number of exchanges of a bubble sort (strict inversions). */
    private static long mergeSort(final int[] v, final int[] buffer, final int from, final int to) {
        if (to - from < 2) return 0;
        final int mid = (from + to) >>> 1;
        long swaps = mergeSort(v, buffer, from, mid) + mergeSort(v, buffer, mid, to);
        int i = from, j = mid, k = from;
        while (i < mid && j < to) {
            if (v[j] < v[i]) {
                // v[j] jumps every element left in the first half: each is a strict inversion
                swaps += mid - i;
                buffer[k++] = v[j++];
            } else {
                buffer[k++] = v[i++];
            }
        }
        while (i < mid) buffer[k++] = v[i++];
        while (j < to) buffer[k++] = v[j++];
        System.arraycopy(buffer, from, v, from, to - from);
        return swaps;
    }
}
