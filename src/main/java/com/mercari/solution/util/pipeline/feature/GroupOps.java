package com.mercari.solution.util.pipeline.feature;

import java.util.Arrays;

/**
 * Context ops that solve a small model over one group and hand every row its own part of the solution
 * ({@code double[][] channels → double[]}): the rows of a group are the observations, a missing value is
 * {@code NaN} on the way in and on the way out. Pure functions; {@link ContextEvaluator} owns the extraction.
 */
public final class GroupOps {

    private GroupOps() {}

    /** A pivot below this share of the largest diagonal entry marks a column the others already explain. */
    private static final double PIVOT_TOLERANCE = 1e-10;

    /**
     * The residuals of {@code y} regressed, with an intercept, on the columns {@code x[k]} over the rows of the group
     * (neutralisation: what is left of y once the group's linear dependence on x is taken out). A row takes part when
     * y and every x are finite; the others read NaN. The group must hold at least {@code p + 2} such rows (with fewer
     * the fit passes through every point and the residual says nothing) — else every row reads NaN. A regressor that is
     * constant or a combination of the others adds nothing and is left out (the residual is the same either way).
     *
     * @param excludeSelf fit every row's line on the OTHER rows (leave-one-out): the residual is then a prediction error
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
        // the sums are taken in an order the values decide, so the result does not depend on how the group arrives
        sort(rows, m, (a, b) -> {
            int c = Double.compare(y[a], y[b]);
            for (int k = 0; k < p && c == 0; k++) c = Double.compare(x[k][a], x[k][b]);
            return c;
        });
        if (!excludeSelf) {
            if (m < p + 2) return out;
            final double[] fit = fit(y, x, rows, m, -1);
            for (int r = 0; r < m; r++) out[rows[r]] = y[rows[r]] - predict(fit, x, rows[r]);
            return out;
        }
        if (m - 1 < p + 2) return out;
        for (int r = 0; r < m; r++) {
            final double[] fit = fit(y, x, rows, m, r);
            out[rows[r]] = y[rows[r]] - predict(fit, x, rows[r]);
        }
        return out;
    }

    private static double predict(final double[] fit, final double[][] x, final int row) {
        double value = fit[0];
        for (int k = 0; k < x.length; k++) value += fit[k + 1] * x[k][row];
        return value;
    }

    /** Least squares over {@code rows[0..m)} without {@code rows[skip]}: {@code [intercept, b1 .. bp]}. */
    private static double[] fit(final double[] y, final double[][] x, final int[] rows, final int m, final int skip) {
        final int p = x.length;
        final int count = skip < 0 ? m : m - 1;
        final double[] mean = new double[p];
        double yMean = 0;
        for (int r = 0; r < m; r++) {
            if (r == skip) continue;
            yMean += y[rows[r]];
            for (int k = 0; k < p; k++) mean[k] += x[k][rows[r]];
        }
        yMean /= count;
        for (int k = 0; k < p; k++) mean[k] /= count;
        // centred normal equations: the intercept drops out
        final double[][] a = new double[p][p];
        final double[] b = new double[p];
        for (int r = 0; r < m; r++) {
            if (r == skip) continue;
            final int i = rows[r];
            for (int k = 0; k < p; k++) {
                final double xk = x[k][i] - mean[k];
                b[k] += xk * (y[i] - yMean);
                for (int l = k; l < p; l++) a[k][l] += xk * (x[l][i] - mean[l]);
            }
        }
        for (int k = 0; k < p; k++) for (int l = 0; l < k; l++) a[k][l] = a[l][k];
        final double[] beta = solve(a, b);
        final double[] fit = new double[p + 1];
        fit[0] = yMean;
        for (int k = 0; k < p; k++) {
            fit[k + 1] = beta[k];
            fit[0] -= beta[k] * mean[k];
        }
        return fit;
    }

    /**
     * Solves the symmetric positive semi-definite system {@code a·beta = b} by sweeping the largest remaining diagonal
     * pivot; a variable whose pivot has (numerically) vanished is a combination of the ones already swept and keeps the
     * coefficient 0 — one of the least-squares solutions, all of which give the same fitted values.
     */
    static double[] solve(final double[][] a, final double[] b) {
        final int p = b.length;
        final double[] beta = new double[p];
        final boolean[] swept = new boolean[p];
        double scale = 0;
        for (int k = 0; k < p; k++) scale = Math.max(scale, a[k][k]);
        if (!(scale > 0)) return beta;
        for (int step = 0; step < p; step++) {
            int pivot = -1;
            for (int k = 0; k < p; k++) if (!swept[k] && (pivot < 0 || a[k][k] > a[pivot][pivot])) pivot = k;
            if (!(a[pivot][pivot] > PIVOT_TOLERANCE * scale)) break;
            swept[pivot] = true;
            final double d = a[pivot][pivot];
            for (int k = 0; k < p; k++) {
                if (k == pivot) continue;
                final double factor = a[k][pivot] / d;
                if (factor == 0) continue;
                for (int l = 0; l < p; l++) a[k][l] -= factor * a[pivot][l];
                b[k] -= factor * b[pivot];
            }
        }
        for (int k = 0; k < p; k++) if (swept[k]) beta[k] = b[k] / a[k][k];
        return beta;
    }

    /** The places the Harville forward computation is defined for here. */
    public static final int MAX_TOP = 3;

    /**
     * The probability of finishing within the first {@code top} places from win probabilities, by the Harville
     * forward computation: the winner is drawn by {@code p}, the next place among the rest in proportion to their
     * strengths, and so on. {@code discount[r − 2]} is the exponent applied to the probabilities when place r is
     * drawn ({@code w = p^λ}; 1 = plain Harville — smaller values flatten the later places, which plain Harville
     * gives too readily to the favourites). A row takes part when its value is finite and ≥ 0; the values are
     * normalised over those rows, so implied probabilities that sum past 1 are accepted. Every row reads NaN when no
     * value is positive or more than {@code maxGroupSize} rows take part (the third place is cubic in the group size).
     */
    public static double[] harville(final double[] p, final int top, final double[] discount, final int maxGroupSize) {
        if (top < 1 || top > MAX_TOP) throw new IllegalArgumentException("harville top must be in [1, " + MAX_TOP + "]: " + top);
        final int n = p.length;
        final double[] out = new double[n];
        Arrays.fill(out, Double.NaN);
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
            if (top >= 2) {
                double second = 0, third = 0;
                for (int j = 0; j < m; j++) {
                    if (j == i || p1[j] == 0) continue;
                    final double rest2 = sum2 - w2[j];
                    if (!(rest2 > 0)) continue;
                    second += p1[j] * w2[i] / rest2;
                    if (top < 3) continue;
                    for (int k = 0; k < m; k++) {
                        if (k == i || k == j || w2[k] == 0) continue;
                        final double rest3 = sum3 - w3[j] - w3[k];
                        if (!(rest3 > 0)) continue;
                        third += p1[j] * (w2[k] / rest2) * (w3[i] / rest3);
                    }
                }
                value += second + third;
            }
            // the sums of three places can pass 1 by rounding only
            out[rows[i]] = Math.min(1d, value);
        }
        return out;
    }

    private static void sort(final int[] rows, final int m, final java.util.Comparator<Integer> order) {
        final Integer[] boxed = new Integer[m];
        for (int r = 0; r < m; r++) boxed[r] = rows[r];
        Arrays.sort(boxed, order);
        for (int r = 0; r < m; r++) rows[r] = boxed[r];
    }

    private static double weight(final double p, final double[] discount, final int index) {
        final double lambda = discount == null || index >= discount.length ? 1d : discount[index];
        return lambda == 1d || p == 0 ? p : Math.pow(p, lambda);
    }

}
