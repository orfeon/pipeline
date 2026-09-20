package com.mercari.solution.util.pipeline.feature;

import java.util.Arrays;

/**
 * Context ops that solve a small model over one group and hand every row its own part of the solution
 * ({@code double[][] channels → double[]}): the rows of a group are the observations, a missing value is
 * {@code NaN} on the way in and on the way out. Pure functions; {@link ContextEvaluator} owns the extraction.
 *
 * <p>The linear algebra is kept here rather than taken from {@code util/domain/math/MatrixOps}: this runs once per
 * group in a DoFn, and the leave-one-out residuals need the pseudo-inverse of the normal-equation matrix itself
 * (the leverages), which a solve-only API does not hand back.
 */
public final class GroupOps {

    private GroupOps() {}

    /** A pivot below this share of its own column's starting diagonal marks a column the others already explain. */
    private static final double PIVOT_TOLERANCE = 1e-10;

    /** A row whose leverage leaves less than this of itself decides its own fit: its leave-one-out residual is NaN. */
    private static final double LEVERAGE_TOLERANCE = 1e-10;

    /**
     * The residuals of {@code y} regressed, with an intercept, on the columns {@code x[k]} over the rows of the group
     * (neutralisation: what is left of y once the group's linear dependence on x is taken out). A row takes part when
     * y and every x are finite; the others read NaN. The group must hold at least {@code p + 2} such rows (with fewer
     * the fit passes through every point and the residual says nothing) — else every row reads NaN. A regressor that is
     * constant or a combination of the others adds nothing and is left out (the residual is the same either way).
     *
     * @param excludeSelf fit every row's line on the OTHER rows (leave-one-out): the residual is then a prediction
     *                    error. The whole group is fitted once and every row's leave-one-out residual read off it as
     *                    {@code e_i / (1 - h_i)} with the leverage {@code h_i = 1/m + c_i' S+ c_i} (c = the row's
     *                    regressors centred on the group) — the same numbers as m separate fits, in one of them.
     *                    The identity holds for the regressors the whole group keeps: if leaving a row out would also
     *                    change which regressors are collinear, the value is the one the group's own active set gives.
     *                    A row that decides its own fit ({@code h_i} at 1) reads NaN.
     */
    public static double[] residualize(final double[] y, final double[][] x, final boolean excludeSelf) {
        final int n = y.length, p = x.length;
        final double[] out = new double[n];
        Arrays.fill(out, Double.NaN);
        final int[] rows = new int[n];
        int m = 0;
        for (int i = 0; i < n; i++) {
            boolean complete = Double.isFinite(y[i]);
            for (int k = 0; k < p && complete; k++) complete = Double.isFinite(x[k][i]);
            if (complete) rows[m++] = i;
        }
        // a leave-one-out fit reads one row fewer, so it needs one more
        if (m - (excludeSelf ? 1 : 0) < p + 2) return out;
        // the sums are taken in an order the values decide, so the result does not depend on how the group arrives
        sort(rows, m, (a, b) -> {
            int c = Double.compare(y[a], y[b]);
            for (int k = 0; k < p && c == 0; k++) c = Double.compare(x[k][a], x[k][b]);
            return c;
        });
        final Fit fit = fit(y, x, rows, m, excludeSelf);
        for (int r = 0; r < m; r++) {
            final int i = rows[r];
            final double e = y[i] - predict(fit, x, i);
            if (!excludeSelf) {
                out[i] = e;
                continue;
            }
            final double rest = 1d - leverage(fit, x, i);
            out[i] = rest > LEVERAGE_TOLERANCE ? e / rest : Double.NaN;
        }
        return out;
    }

    /** One least-squares fit of a group: {@code inverse} = S+ over the regressors the sweep kept (null when not asked for). */
    private record Fit(double[] beta, double[] mean, double yMean, double[][] inverse, int count) {}

    private static double predict(final Fit fit, final double[][] x, final int row) {
        double value = fit.yMean();
        for (int k = 0; k < x.length; k++) value += fit.beta()[k] * (x[k][row] - fit.mean()[k]);
        return value;
    }

    /** The row's share of its own fitted value: {@code h_i = 1/m + c_i' S+ c_i}, between 1/m and 1. */
    private static double leverage(final Fit fit, final double[][] x, final int row) {
        final int p = x.length;
        final double[] centred = new double[p];
        for (int k = 0; k < p; k++) centred[k] = x[k][row] - fit.mean()[k];
        double h = 1d / fit.count();
        for (int k = 0; k < p; k++) {
            if (centred[k] == 0) continue;
            for (int l = 0; l < p; l++) h += centred[k] * fit.inverse()[k][l] * centred[l];
        }
        return h;
    }

    /** Least squares over {@code rows[0..m)}: the centred normal equations, so the intercept drops out. */
    private static Fit fit(final double[] y, final double[][] x, final int[] rows, final int m, final boolean withInverse) {
        final int p = x.length;
        final double[] mean = new double[p];
        double yMean = 0;
        for (int r = 0; r < m; r++) {
            yMean += y[rows[r]];
            for (int k = 0; k < p; k++) mean[k] += x[k][rows[r]];
        }
        yMean /= m;
        for (int k = 0; k < p; k++) mean[k] /= m;
        final double[][] a = new double[p][p];
        final double[] b = new double[p];
        for (int r = 0; r < m; r++) {
            final int i = rows[r];
            for (int k = 0; k < p; k++) {
                final double xk = x[k][i] - mean[k];
                b[k] += xk * (y[i] - yMean);
                for (int l = k; l < p; l++) a[k][l] += xk * (x[l][i] - mean[l]);
            }
        }
        for (int k = 0; k < p; k++) for (int l = 0; l < k; l++) a[k][l] = a[l][k];
        final double[][] inverse = withInverse ? new double[p][p] : null;
        return new Fit(solve(a, b, inverse), mean, yMean, inverse, m);
    }

    static double[] solve(final double[][] a, final double[] b) {
        return solve(a, b, null);
    }

    /**
     * Solves the symmetric positive semi-definite system {@code a·beta = b} by sweeping the largest remaining diagonal
     * pivot <em>relative to that column's own starting diagonal</em>; a variable whose pivot has (numerically) vanished
     * against its own scale is a combination of the ones already swept and keeps the coefficient 0 — one of the least
     * squares solutions, all of which give the same fitted values. {@code a} and {@code b} are swept in place.
     *
     * @param inverse when non-null, receives {@code a}'s pseudo-inverse over the swept variables (zero rows for the
     *                ones left out): the row operations are accumulated from the identity, so the sweep that solves
     *                the system also inverts it.
     */
    static double[] solve(final double[][] a, final double[] b, final double[][] inverse) {
        final int p = b.length;
        final double[] beta = new double[p];
        final boolean[] swept = new boolean[p];
        // the pivot is weighed against the column's OWN scale: a regressor measured in small units (a rate next to
        // a price) is not a redundant one, and a shared tolerance would sweep it away with the collinear columns
        final double[] scale = new double[p];
        for (int k = 0; k < p; k++) scale[k] = a[k][k];
        if (inverse != null) {
            for (int k = 0; k < p; k++) {
                Arrays.fill(inverse[k], 0d);
                inverse[k][k] = 1d;
            }
        }
        for (int step = 0; step < p; step++) {
            int pivot = -1;
            for (int k = 0; k < p; k++) {
                if (swept[k] || !(scale[k] > 0)) continue;
                if (pivot < 0 || a[k][k] / scale[k] > a[pivot][pivot] / scale[pivot]) pivot = k;
            }
            if (pivot < 0 || !(a[pivot][pivot] > PIVOT_TOLERANCE * scale[pivot])) break;
            swept[pivot] = true;
            final double d = a[pivot][pivot];
            for (int k = 0; k < p; k++) {
                if (k == pivot) continue;
                final double factor = a[k][pivot] / d;
                if (factor == 0) continue;
                for (int l = 0; l < p; l++) a[k][l] -= factor * a[pivot][l];
                b[k] -= factor * b[pivot];
                if (inverse != null) for (int l = 0; l < p; l++) inverse[k][l] -= factor * inverse[pivot][l];
            }
        }
        for (int k = 0; k < p; k++) if (swept[k]) beta[k] = b[k] / a[k][k];
        if (inverse != null) {
            for (int k = 0; k < p; k++) {
                // a variable the sweep left out is no part of the solved subsystem: its row of the pseudo-inverse is
                // zero (what the row operations left there belongs to the others)
                if (!swept[k]) Arrays.fill(inverse[k], 0d);
                else for (int l = 0; l < p; l++) inverse[k][l] /= a[k][k];
            }
        }
        return beta;
    }

    /** The places the Harville forward computation is defined for here. */
    public static final int MAX_TOP = 3;

    /** {@link #harvillePlaces} read at one place. */
    public static double[] harville(final double[] p, final int top, final double[] discount, final int maxGroupSize) {
        return harvillePlaces(p, top, discount, maxGroupSize)[top - 1];
    }

    /**
     * The probability of finishing within the first k places, {@code [k - 1][row]} for every k up to {@code maxTop},
     * from win probabilities by the Harville forward computation: the winner is drawn by {@code p}, the next place
     * among the rest in proportion to their strengths, and so on. Every place contains the one before it, so a single
     * pass gives them all. {@code discount[r - 2]} is the exponent applied to the probabilities when place r is drawn
     * ({@code w = p^λ}; 1 = plain Harville — smaller values flatten the later places, which plain Harville gives too
     * readily to the favourites). A row takes part when its value is finite and ≥ 0; the values are normalised over
     * those rows, so implied probabilities that sum past 1 are accepted. When the pool a place is drawn from has no
     * strength left (every remaining row is 0), the place falls to those rows in equal shares — so every place is
     * still taken by exactly one row and a group of at most k rows is entirely within the first k. Every row reads
     * NaN when no value is positive or more than {@code maxGroupSize} rows take part (the third place is cubic in
     * the group size).
     */
    public static double[][] harvillePlaces(final double[] p, final int maxTop, final double[] discount, final int maxGroupSize) {
        if (maxTop < 1 || maxTop > MAX_TOP) throw new IllegalArgumentException("harville top must be in [1, " + MAX_TOP + "]: " + maxTop);
        final int n = p.length;
        final double[][] out = new double[maxTop][n];
        for (final double[] place : out) Arrays.fill(place, Double.NaN);
        final int[] rows = new int[n];
        int m = 0;
        double total = 0;
        for (int i = 0; i < n; i++) {
            if (Double.isFinite(p[i]) && p[i] >= 0) {
                rows[m++] = i;
                total += p[i];
            }
        }
        if (m == 0 || m > maxGroupSize || !(total > 0)) return out;
        // summed in the order of the values: the result does not depend on how the group arrives
        sort(rows, m, (a, b) -> Double.compare(p[a], p[b]));
        total = 0;
        for (int r = 0; r < m; r++) total += p[rows[r]];
        final double[] p1 = new double[m], w2 = new double[m], w3 = new double[m];
        double sum2 = 0, sum3 = 0;
        for (int r = 0; r < m; r++) {
            p1[r] = p[rows[r]] / total;
            w2[r] = weight(p1[r], discount, 0);
            w3[r] = weight(p1[r], discount, 1);
            sum2 += w2[r];
            sum3 += w3[r];
        }
        for (int i = 0; i < m; i++) {
            double value = p1[i];
            // the sums of three places can pass 1 by rounding only
            out[0][rows[i]] = Math.min(1d, value);
            if (maxTop < 2) continue;
            double second = 0, third = 0;
            for (int j = 0; j < m; j++) {
                if (j == i || p1[j] == 0) continue;
                // no strength left in the pool after the winner: the place falls to the rest in equal shares
                final double rest2 = sum2 - w2[j];
                second += p1[j] * (rest2 > 0 ? w2[i] / rest2 : 1d / (m - 1));
                if (maxTop < 3) continue;
                for (int k = 0; k < m; k++) {
                    if (k == i || k == j) continue;
                    final double drawn2 = rest2 > 0 ? w2[k] / rest2 : 1d / (m - 1);
                    if (drawn2 == 0) continue;
                    final double rest3 = sum3 - w3[j] - w3[k];
                    third += p1[j] * drawn2 * (rest3 > 0 ? w3[i] / rest3 : 1d / (m - 2));
                }
            }
            value += second;
            out[1][rows[i]] = Math.min(1d, value);
            if (maxTop < 3) continue;
            value += third;
            out[2][rows[i]] = Math.min(1d, value);
        }
        return out;
    }

    /** Orders {@code rows[0..m)} by the given comparison, without an index boxed per row. */
    private static void sort(final int[] rows, final int m, final Order order) {
        if (m > 1) mergeSort(rows, new int[m], 0, m, order);
    }

    /** Compares two row indices of the group. */
    private interface Order {
        int compare(int a, int b);
    }

    private static void mergeSort(final int[] rows, final int[] scratch, final int from, final int to, final Order order) {
        if (to - from < 2) return;
        final int mid = (from + to) >>> 1;
        mergeSort(rows, scratch, from, mid, order);
        mergeSort(rows, scratch, mid, to, order);
        if (order.compare(rows[mid - 1], rows[mid]) <= 0) return; // the halves are already in order
        int i = from, j = mid, k = from;
        while (i < mid && j < to) scratch[k++] = order.compare(rows[i], rows[j]) <= 0 ? rows[i++] : rows[j++];
        while (i < mid) scratch[k++] = rows[i++];
        while (j < to) scratch[k++] = rows[j++];
        System.arraycopy(scratch, from, rows, from, to - from);
    }

    private static double weight(final double p, final double[] discount, final int index) {
        final double lambda = discount == null || index >= discount.length ? 1d : discount[index];
        return lambda == 1d || p == 0 ? p : Math.pow(p, lambda);
    }

}
