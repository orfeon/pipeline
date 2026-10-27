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

    /**
     * The shapes the race is solved for. Below the lower bound the running time's 1e-10 quantile underflows, and the
     * quadrature with it (every group would read null); above the upper one the incomplete gamma's series runs out of
     * terms — and at 1000 the running times already spread by 3% of their mean, the normal model in all but name.
     */
    public static final double RACE_MIN_SHAPE = 0.05, RACE_MAX_SHAPE = 1000;
    /**
     * The Newton calibration of the race model stops when the log win probability of every free rate is within this of
     * its target. The gauge row's own equation is implied by the others (the win probabilities sum to 1) up to the
     * quadrature's truncation, about 1e-9: a tolerance this tight could never be met on it.
     */
    private static final double RACE_TOLERANCE = 1e-10;
    /** Newton steps (each one quadrature pass with the Jacobian) before the calibration is given up. */
    private static final int RACE_MAX_STEPS = 60;
    /** Quadrature nodes per unit of log time per unit of sqrt(shape): about 24 nodes across the width of a running-time density. */
    private static final double RACE_NODES_PER_WIDTH = 24;
    private static final int RACE_MIN_INTERVALS = 200, RACE_MAX_INTERVALS = 4000;

    /**
     * The probability of finishing within the first k places, {@code [k - 1][row]} for every k up to {@code maxTop},
     * under the <b>gamma race model</b> (Stern 1990): row i's running time is Gamma(shape, rate θ_i), the fastest
     * finishes first. The rates are calibrated so that the model's win probabilities equal the normalised values of
     * {@code p} (Newton on log θ, the win probabilities and their Jacobian by one quadrature pass per step), and
     * "within the first k" follows from the same quadrature: {@code P(i within k) = ∫ f_i(t) P(at most k − 1 others
     * have finished by t) dt}, the count a Poisson-binomial over the others' CDFs. {@code shape} 1 is the exponential
     * race — Plackett–Luce, exactly Harville — and is served by {@link #harvillePlaces} in closed form; a larger shape
     * narrows the running times around their means, so a favourite that does not win is less sure of the next places
     * than Harville says (the bias the {@code discount} exponents approximate), a shape below 1 the opposite. Place 1 is
     * the normalised input exactly; every other place is accurate to about 1e-6.
     *
     * <p>Same conventions as {@link #harvillePlaces}: a row takes part when its value is finite and ≥ 0, the values
     * are normalised over those rows, a row at 0 can only finish behind every positive row and the zero rows share the
     * places left in equal parts, every row reads NaN when no value is positive, more than {@code maxGroupSize} rows
     * take part, or — a numerical failure the closed form cannot have — the calibration does not converge. The sums
     * run in the order of the values and rows of equal value read the same, so the result does not depend on the
     * order the group arrives in. {@code shape} must lie in [{@link #RACE_MIN_SHAPE}, {@link #RACE_MAX_SHAPE}].
     */
    public static double[][] racePlaces(final double[] p, final int maxTop, final double shape, final int maxGroupSize) {
        if (!(shape >= RACE_MIN_SHAPE && shape <= RACE_MAX_SHAPE)) {
            throw new IllegalArgumentException("race shape must be in [" + RACE_MIN_SHAPE + ", " + RACE_MAX_SHAPE + "]: " + shape);
        }
        if (shape == 1d) return harvillePlaces(p, maxTop, null, maxGroupSize);
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
        sort(rows, m, (a, b) -> Double.compare(p[a], p[b]));
        // the zero rows come first in the order; the race is run among the positive ones
        int zeros = 0;
        while (zeros < m && p[rows[zeros]] == 0) zeros++;
        final int live = m - zeros;
        total = 0;
        for (int r = zeros; r < m; r++) total += p[rows[r]];
        final double[] q = new double[live];
        for (int r = 0; r < live; r++) q[r] = p[rows[zeros + r]] / total;
        final double[][] within = raceWithin(q, maxTop, shape);
        if (within == null) return out;
        // rows of equal value are the same runner, but the solver's rounding (the gauge rate is fixed, the others solved
        // for) tells them apart in the last digits: which row got which would follow the arrival order. A run of equal
        // values reads its mean, summed in position order
        for (int r = 0, end; r < live; r = end) {
            end = r + 1;
            while (end < live && q[end] == q[r]) end++;
            if (end - r < 2) continue;
            for (int k = 1; k < maxTop; k++) {
                double sum = 0;
                for (int t = r; t < end; t++) sum += within[k][t];
                Arrays.fill(within[k], r, end, sum / (end - r));
            }
        }
        for (int k = 1; k <= maxTop; k++) {
            for (int r = 0; r < live; r++) out[k - 1][rows[zeros + r]] = Math.min(1d, within[k - 1][r]);
            // the places the positive rows cannot fill fall to the zero rows in equal shares
            for (int z = 0; z < zeros; z++) out[k - 1][rows[z]] = k > live ? Math.min(1d, (k - live) / (double) zeros) : 0d;
        }
        return out;
    }

    /**
     * {@link #racePlaces} over normalised positive win probabilities {@code q} (sum 1): {@code [k - 1][i]}, or null when
     * the calibration does not converge. Package-private for the tests (the exponential shape 1 is computed here too,
     * which the closed form checks).
     */
    static double[][] raceWithin(final double[] q, final int maxTop, final double shape) {
        final int m = q.length;
        final double[][] within = new double[maxTop][m];
        // only a place between the first and the size of the field needs the race: with top 1 alone, or a field of two,
        // the input and the ones below are the whole answer
        if (maxTop > 1 && m > 2) {
            final Race race = new Race(shape, m);
            final double[] theta = raceRates(race, q);
            if (theta == null) return null;
            race.integrate(theta, new double[m], null, within);
        }
        // place 1 is the input itself, and once k reaches the field everyone is within it
        System.arraycopy(q, 0, within[0], 0, m);
        for (int k = 1; k < maxTop; k++) if (k + 1 >= m) Arrays.fill(within[k], 1d);
        return within;
    }

    /**
     * The rates θ of the gamma race model whose win probabilities are {@code q} (normalised, positive; the last rate is
     * fixed at its starting value, since a common scale changes nothing), or null when the Newton iteration does not
     * converge. Package-private for the tests (a simulation from these rates is the reference for {@link #raceWithin}).
     */
    static double[] raceRates(final double[] q, final double shape) {
        return raceRates(new Race(shape, q.length), q);
    }

    private static double[] raceRates(final Race race, final double[] q) {
        final int m = q.length;
        // the starting point: the exponential race's rates are the win probabilities themselves, and a log running time
        // spreads about 1 / sqrt(shape) as wide as the exponential's, so q^(1 / sqrt(shape)) keeps the field's gaps in
        // units of that spread. Starting from q itself at a large shape, the favourite finishes before the others have
        // started and their win probabilities underflow to 0: no Newton step could be taken from there
        final double power = 1 / Math.sqrt(race.shape);
        double[] theta = new double[m];
        for (int i = 0; i < m; i++) theta[i] = Math.pow(q[i], power);
        final double[] logQ = new double[m];
        for (int i = 0; i < m; i++) logQ[i] = Math.log(q[i]);
        final double[] win = new double[m];
        final double[][] jacobian = new double[m][m];
        race.integrate(theta, win, jacobian, null);
        final int d = m - 1;
        double norm = residualNorm(win, logQ, d);
        if (!Double.isFinite(norm)) return null;
        for (int step = 0; step < RACE_MAX_STEPS && norm > RACE_TOLERANCE; step++) {
            // Newton on log θ with the last rate fixed (the model is invariant to a common scale of the rates): the
            // Jacobian is symmetric, so the step solves J Δ = P ∘ (log q − log P) over i, j < m − 1 — the system
            // (J_ij / P_i) Δ_j = −(log P_i − log q_i) with every row multiplied by its P_i
            final double[][] a = new double[d][d];
            final double[] g = new double[d];
            for (int i = 0; i < d; i++) {
                System.arraycopy(jacobian[i], 0, a[i], 0, d);
                g[i] = -(Math.log(win[i]) - logQ[i]) * win[i];
            }
            final double[] delta = solve(a, g);
            // a step that does not reduce the residual is halved, up to a point
            boolean accepted = false;
            final double[] winNext = new double[m];
            final double[][] jacobianNext = new double[m][m];
            for (double scale = 1d; scale > 1e-3; scale /= 2) {
                final double[] next = new double[m];
                boolean finite = true;
                for (int j = 0; j < m; j++) {
                    next[j] = theta[j] * Math.exp(scale * (j < d ? delta[j] : 0d));
                    finite &= Double.isFinite(next[j]) && next[j] > 0;
                }
                if (!finite) continue;
                race.integrate(next, winNext, jacobianNext, null);
                final double normNext = residualNorm(winNext, logQ, d);
                if (normNext < norm) {
                    theta = next;
                    System.arraycopy(winNext, 0, win, 0, m);
                    for (int i = 0; i < m; i++) System.arraycopy(jacobianNext[i], 0, jacobian[i], 0, m);
                    norm = normNext;
                    accepted = true;
                    break;
                }
            }
            if (!accepted) break;
        }
        // accepted on every row, the gauge's included
        return residualNorm(win, logQ, m) <= 1e-7 ? theta : null;
    }

    /** The largest |log P_i − log q_i| over the first {@code count} rows; infinite when one of them has no win probability. */
    private static double residualNorm(final double[] win, final double[] logQ, final int count) {
        double norm = 0;
        for (int i = 0; i < count; i++) {
            if (!(win[i] > 0)) return Double.POSITIVE_INFINITY;
            norm = Math.max(norm, Math.abs(Math.log(win[i]) - logQ[i]));
        }
        return norm;
    }

    /**
     * One quadrature pass of the race: the win probabilities {@code P_i = ∫ f_i Π_{j≠i} S_j dt}, their Jacobian
     * {@code ∂P_i / ∂log θ_j} ({@code ∫ f_i (shape − θ_i t) Π_{j≠i} S_j} on the diagonal, {@code −∫ f_i t f_j Π_{l≠i,j} S_l}
     * off it — symmetric) and the within-k probabilities. Simpson in {@code s = log t}: the running-time density
     * times t is bell-shaped in s for every shape (width about 1 / sqrt(shape)), so the node count follows the span
     * of the rates and the shape; the span runs from the 1e-10 quantile of the fastest row to the 1 − 1e-10 quantile
     * of the slowest.
     */
    private static final class Race {
        private final double shape, logGamma, quantileLow, quantileHigh;
        private final double[] f, s, cdf, prefix, suffix, dp, shapeLogTheta;

        Race(final double shape, final int m) {
            this.shape = shape;
            this.logGamma = com.mercari.solution.util.pipeline.glm.StatMath.logGamma(shape);
            this.quantileLow = com.mercari.solution.util.pipeline.glm.StatMath.gammaQuantile(shape, 1e-10);
            this.quantileHigh = com.mercari.solution.util.pipeline.glm.StatMath.gammaQuantile(shape, 1 - 1e-10);
            this.f = new double[m];
            this.s = new double[m];
            this.cdf = new double[m];
            this.prefix = new double[m + 1];
            this.suffix = new double[m + 1];
            this.dp = new double[MAX_TOP];
            this.shapeLogTheta = new double[m];
        }

        void integrate(final double[] theta, final double[] win, final double[][] jacobian, final double[][] within) {
            final int m = theta.length;
            double min = Double.POSITIVE_INFINITY, max = 0;
            for (int i = 0; i < m; i++) {
                min = Math.min(min, theta[i]);
                max = Math.max(max, theta[i]);
                // per row, not per node
                shapeLogTheta[i] = shape * Math.log(theta[i]);
            }
            final double lo = Math.log(quantileLow / max), hi = Math.log(quantileHigh / min);
            int intervals = (int) Math.ceil(RACE_NODES_PER_WIDTH * (hi - lo) * Math.max(1d, Math.sqrt(shape)));
            intervals = Math.max(RACE_MIN_INTERVALS, Math.min(RACE_MAX_INTERVALS, intervals));
            if (intervals % 2 == 1) intervals++;
            final double h = (hi - lo) / intervals;
            Arrays.fill(win, 0d);
            if (jacobian != null) for (final double[] row : jacobian) Arrays.fill(row, 0d);
            final int places = within == null ? 0 : within.length;
            if (within != null) for (final double[] place : within) Arrays.fill(place, 0d);
            for (int node = 0; node <= intervals; node++) {
                final double logT = lo + node * h;
                final double t = Math.exp(logT);
                // Simpson weight times dt = t ds
                final double w = (node == 0 || node == intervals ? 1 : node % 2 == 1 ? 4 : 2) * h / 3 * t;
                for (int i = 0; i < m; i++) {
                    final double x = theta[i] * t;
                    // ln Γ(shape) is the same at every node: computed once, not once per call
                    s[i] = com.mercari.solution.util.pipeline.glm.StatMath.regularizedGammaQ(shape, x, logGamma);
                    cdf[i] = 1 - s[i];
                    f[i] = Math.exp(shapeLogTheta[i] + (shape - 1) * logT - x - logGamma);
                }
                prefix[0] = 1;
                for (int i = 0; i < m; i++) prefix[i + 1] = prefix[i] * s[i];
                suffix[m] = 1;
                for (int i = m - 1; i >= 0; i--) suffix[i] = suffix[i + 1] * s[i];
                for (int i = 0; i < m; i++) {
                    final double others = prefix[i] * suffix[i + 1];
                    final double wf = w * f[i];
                    win[i] += wf * others;
                    if (jacobian != null) {
                        jacobian[i][i] += wf * (shape - theta[i] * t) * others;
                        // Π_{l≠i,j} S_l for j > i: prefix[i] · Π_{i<l<j} S_l · suffix[j + 1]
                        double middle = 1;
                        for (int j = i + 1; j < m; j++) {
                            final double pair = -w * t * f[i] * f[j] * prefix[i] * middle * suffix[j + 1];
                            jacobian[i][j] += pair;
                            jacobian[j][i] += pair;
                            middle *= s[j];
                        }
                    }
                    if (places > 1) {
                        // the number of others that have finished by t: a Poisson-binomial over their CDFs, kept up to k − 1
                        Arrays.fill(dp, 0, places, 0d);
                        dp[0] = 1;
                        for (int j = 0; j < m; j++) {
                            if (j == i) continue;
                            for (int c = places - 1; c >= 1; c--) dp[c] = dp[c] * s[j] + dp[c - 1] * cdf[j];
                            dp[0] *= s[j];
                        }
                        double cumulative = dp[0];
                        for (int k = 1; k < places; k++) {
                            cumulative += dp[k];
                            within[k][i] += wf * cumulative;
                        }
                    }
                }
            }
        }
    }

    /**
     * The result of {@link #svd}: per row its scores on the leading components and its residual per channel (null for a
     * row that took no part), the residual's norm (NaN), the share of the group's variance the components carry (NaN
     * when nothing was fitted) and the rows that entered the fit.
     */
    public record GroupSvd(double[][] scores, double[][] residual, double[] residualNorm, double absorption, int count) {}

    /**
     * The principal components of the group: its rows as vectors over the channels (a row with a missing or
     * non-finite channel takes no part and reads null), centred — and standardised by the channels' sample standard
     * deviations under {@code standardize}: the correlation matrix's components, for channels of unlike scale — and
     * projected on the leading {@code rank} eigenvectors of their covariance matrix, each oriented so its largest
     * loading is positive, the first of the loadings tied with it on a tie ({@link #orientation}: the same field
     * dominating two groups gives their scores one sign). The scores say where a row sits on the group's main axes of
     * variation (ordered by variance, {@code 0} the most), the residual what the axes do not explain of it (per
     * channel, in the channel's units), {@code absorption} the share of the variance the axes carry (1 with every
     * component kept, NaN for a group without variance). A group needs {@code rank + 1} complete rows, else nothing is
     * read: with fewer, the last components carry no variance and point anywhere. At exactly {@code rank + 1} rows the
     * components pass through the points — the scores are their coordinates on them, the residual is 0 and
     * {@code absorption} 1. The vectors enter the moments in an order the values decide (lexicographic over the
     * channels), so the result does not depend on the order the rows of a group arrive in.
     */
    public static GroupSvd svd(final double[][] channels, final int rank, final boolean standardize) {
        final int k = channels.length, n = k == 0 ? 0 : channels[0].length;
        final int[] rows = new int[n];
        int m = 0;
        for (int i = 0; i < n; i++) {
            boolean complete = true;
            // an infinite value would turn every moment into NaN: like residualize, only finite rows take part
            for (int c = 0; c < k && complete; c++) complete = Double.isFinite(channels[c][i]);
            if (complete) rows[m++] = i;
        }
        final double[][] scores = new double[n][], residual = new double[n][];
        final double[] residualNorm = new double[n];
        Arrays.fill(residualNorm, Double.NaN);
        if (rank < 1 || m < rank + 1) return new GroupSvd(scores, residual, residualNorm, Double.NaN, m);
        sort(rows, m, (a, b) -> {
            for (int c = 0; c < k; c++) {
                final int cmp = Double.compare(channels[c][a], channels[c][b]);
                if (cmp != 0) return cmp;
            }
            return 0;
        });
        final Svd.Moments moments = new Svd.Moments();
        final double[][] vectors = new double[m][];
        for (int r = 0; r < m; r++) moments.add(vectors[r] = vector(channels, rows[r]));
        final Svd fit = Svd.fit(moments, rank, true, standardize, false);
        final double[] orientation = orientation(fit);
        for (int r = 0; r < m; r++) {
            final int i = rows[r];
            final double[] x = vectors[r];
            final double[] s = fit.transform(x);
            // the residual is read off the fit's own orientation (score × loading), before the scores are re-oriented
            residual[i] = fit.residual(x, s);
            for (int j = 0; j < s.length; j++) s[j] *= orientation[j];
            scores[i] = s;
            double sum = 0;
            for (final double v : residual[i]) sum += v * v;
            residualNorm[i] = Math.sqrt(sum);
        }
        double carried = 0;
        for (final double v : fit.variances) carried += v;
        return new GroupSvd(scores, residual, residualNorm, fit.totalVariance > 0 ? Math.min(1d, carried / fit.totalVariance) : Double.NaN, m);
    }

    /** Loadings within this share of a component's largest one are tied with it ({@link #orientation}). */
    private static final double ORIENTATION_TIE = 1e-9;

    /**
     * The sign each component's scores are read with. {@link Svd#fit} makes the largest-magnitude loading positive,
     * the first one on an exact tie; but a group's loadings often tie in magnitude only up to rounding — two rows
     * under {@code standardize} give every loading ±1/√k — and the last bits would then pick which field orients the
     * scores, flipping their sign from one group to the next. The first loading within {@link #ORIENTATION_TIE} of the
     * largest decides instead: {@code -1} when it is negative (the fit oriented the component by a later one).
     */
    private static double[] orientation(final Svd fit) {
        final double[] signs = new double[fit.components.length];
        for (int r = 0; r < signs.length; r++) {
            final double[] loadings = fit.components[r];
            double largest = 0;
            for (final double v : loadings) largest = Math.max(largest, Math.abs(v));
            int first = 0;
            while (first < loadings.length - 1 && Math.abs(loadings[first]) < largest * (1 - ORIENTATION_TIE)) first++;
            signs[r] = loadings[first] < 0 ? -1 : 1;
        }
        return signs;
    }

    /** Row {@code row} of the group as a vector over the channels. */
    private static double[] vector(final double[][] channels, final int row) {
        final double[] x = new double[channels.length];
        for (int c = 0; c < channels.length; c++) x[c] = channels[c][row];
        return x;
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
