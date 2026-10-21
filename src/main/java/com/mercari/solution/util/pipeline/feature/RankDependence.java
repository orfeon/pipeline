package com.mercari.solution.util.pipeline.feature;

import java.util.Arrays;

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
 * where it is noise.
 */
public final class RankDependence {

    public static final String SPEARMAN = "spearman";
    public static final String KENDALL = "kendall";
    public static final String QUADRANT_ASYMMETRY = "quadrantAsymmetry";
    /** The regression funcs computed here (scan-only). */
    public static final java.util.List<String> FUNCS = java.util.List.of(SPEARMAN, KENDALL, QUADRANT_ASYMMETRY);
    public static final double DEFAULT_Q = 0.75;
    /** quadrantAsymmetry is null while {@code (1 − q) n} is below this: a corner of a few pairs is noise. */
    public static final int MIN_TAIL = 5;

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
        final Integer[] order = new Integer[n];
        for (int i = 0; i < n; i++) order[i] = i;
        Arrays.sort(order, (a, b) -> {
            final int c = Double.compare(x[a], x[b]);
            return c != 0 ? c : Double.compare(y[a], y[b]);
        });
        final double[] ys = new double[n];
        for (int i = 0; i < n; i++) ys[i] = y[order[i]];
        final long pairs = (long) n * (n - 1) / 2;
        // ties in x, and joint ties in (x, y): runs of the sorted order
        long tiesX = 0, tiesXY = 0;
        for (int i = 0; i < n; ) {
            int j = i;
            while (j + 1 < n && x[order[j + 1]] == x[order[i]]) j++;
            final long t = j - i + 1;
            tiesX += t * (t - 1) / 2;
            for (int k = i; k <= j; ) {
                int l = k;
                while (l + 1 <= j && ys[l + 1] == ys[k]) l++;
                final long u = l - k + 1;
                tiesXY += u * (u - 1) / 2;
                k = l + 1;
            }
            i = j + 1;
        }
        // the exchanges a stable merge sort by y needs = the discordant pairs (pairs tied in x are already in y order)
        final long swaps = mergeSort(ys, new double[n], 0, n);
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
        if (n < 2 || (1 - q) * n < MIN_TAIL) return null;
        final double[] rx = midRanks(x, n), ry = midRanks(y, n);
        if (constant(rx, n) || constant(ry, n)) return null;
        long lower = 0, upper = 0;
        for (int i = 0; i < n; i++) {
            final double ux = rx[i] / (n + 1), uy = ry[i] / (n + 1);
            if (ux < 1 - q && uy < 1 - q) lower++;
            if (ux > q && uy > q) upper++;
        }
        return (double) (lower - upper) / n;
    }

    /** The mid-ranks (1-based; ties share the mean of their ranks) of the first {@code n} values. */
    static double[] midRanks(final double[] v, final int n) {
        final Integer[] order = new Integer[n];
        for (int i = 0; i < n; i++) order[i] = i;
        Arrays.sort(order, (a, b) -> Double.compare(v[a], v[b]));
        final double[] ranks = new double[n];
        for (int i = 0; i < n; ) {
            int j = i;
            while (j + 1 < n && v[order[j + 1]] == v[order[i]]) j++;
            final double rank = (i + j) / 2d + 1;
            for (int k = i; k <= j; k++) ranks[order[k]] = rank;
            i = j + 1;
        }
        return ranks;
    }

    private static boolean constant(final double[] ranks, final int n) {
        for (int i = 1; i < n; i++) if (ranks[i] != ranks[0]) return false;
        return true;
    }

    /** Sorts {@code v[from, to)} ascending, returning the number of exchanges of a bubble sort (strict inversions). */
    private static long mergeSort(final double[] v, final double[] buffer, final int from, final int to) {
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
