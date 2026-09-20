package com.mercari.solution.util.pipeline.feature;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * A smooth curve of a target over a numeric key (docs/design/feature-dsl.md §5.6, the linear-basis class;
 * {@code type: smooth}): a penalised B-spline regression (P-spline, Eilers &amp; Marx) — uniform B-splines of
 * {@code degree} over {@code segments} intervals of a declared {@code [lo, hi]}, a difference penalty of
 * {@code penaltyOrder} on the coefficients, and a strength {@code λ} that is either declared or chosen by
 * restricted maximum likelihood through the mixed-model correspondence of the penalty.
 *
 * <p>Everything is solved from sufficient statistics: a row contributes the vector {@code [B_0(x) … B_{m−1}(x), y]}
 * to the {@link Svd.Moments} family — the same (n, Σz, Σzzᵀ) accumulator an svd block uses — which holds
 * {@code XᵀX}, {@code Xᵀy} and {@code yᵀy}, so no row leaves the workers, the fit shares the stage's one
 * {@code Combine} with the svd blocks, and {@code static} / {@code forward} / {@code window} come from
 * {@link BlockSeries} without a second implementation. The REML criterion needs nothing else:
 * {@code (n − d) log RSS_pen(λ) + log|XᵀX + λP| − (m − d) log λ}, with {@code RSS_pen = y'ᵀy' − β'ᵀXᵀy'}.
 *
 * <p>The knots must be known before the single pass, which is why the range is declared and not read off the
 * data (data-driven knots: feed a {@code quantileTransform} column, whose range is {@code [0, 1]}). A key
 * outside the range is clamped to it — the curve is constant beyond its ends — in the fit and in the apply alike.
 *
 * <p>Numerics: the target is centred before the solve (the B-splines sum to one and a difference penalty ignores
 * constants, so the mean is added back to every coefficient — exact, and a target with a large level costs no
 * digits), and the moments are read through their centred form, which does not depend on the accumulator's anchor.
 */
public final class Smooth implements Serializable, FitArtifact.Model {

    private static final Logger LOG = LoggerFactory.getLogger(Smooth.class);

    public static final String SPLINE = "spline";
    public static final String REML = "reml";
    public static final int DEFAULT_SEGMENTS = 10;
    public static final int DEFAULT_DEGREE = 3;
    public static final int DEFAULT_PENALTY_ORDER = 2;
    /** Basis functions a block may span: the fit state is (m + 1)² doubles per time block. */
    public static final int MAX_BASIS = 64;

    /** REML search: decades around the scale tr(XᵀX) / tr(P), the grid step, and the refinement iterations. */
    private static final double SEARCH_DECADES = 8, SEARCH_STEP = 0.5;
    private static final int REFINE_ITERATIONS = 40;

    /**
     * Uniform B-splines of {@code degree} over {@code segments} equal intervals of {@code [lo, hi]}: {@code segments +
     * degree} functions, non-negative, summing to one, each non-zero over {@code degree + 1} intervals.
     */
    public record Basis(double lo, double hi, int segments, int degree) implements Serializable {

        public int size() {
            return segments + degree;
        }

        /** The basis functions at {@code x} (clamped into the range); a fresh dense vector of {@link #size}. */
        public double[] evaluate(final double x) {
            final double[] out = new double[size()];
            evaluateInto(x, out, 0);
            return out;
        }

        /** Writes the {@link #size} basis values at {@code x} (clamped into the range) to {@code out[offset …]}; the rest of {@code out} is untouched. */
        public void evaluateInto(final double x, final double[] out, final int offset) {
            final double u = (Math.min(Math.max(x, lo), hi) - lo) / (hi - lo) * segments;
            final int k = Math.min((int) Math.floor(u), segments - 1);
            final double t = u - k;
            // Cox–de Boor over uniform knots (the knot spacing cancels): the degree + 1 functions alive on interval k
            final double[] n = new double[degree + 1];
            n[0] = 1;
            for (int j = 1; j <= degree; j++) {
                double saved = 0;
                for (int r = 0; r < j; r++) {
                    // left = t + (j − r) − 1, right = (r + 1) − t, and their sum is j
                    final double temp = n[r] / j;
                    n[r] = saved + (r + 1 - t) * temp;
                    saved = (t + j - r - 1) * temp;
                }
                n[j] = saved;
            }
            for (int i = 0; i < size(); i++) out[offset + i] = 0;
            for (int j = 0; j <= degree; j++) out[offset + k + j] = n[j];
        }
    }

    public final Basis basis;
    public final int penaltyOrder;
    /** The penalty strength the coefficients were solved with. */
    public final double lambda;
    /** Whether {@link #lambda} was chosen by REML (else declared). */
    public final boolean estimated;
    /** One coefficient per basis function; empty when nothing could be fitted (every key maps to null). */
    public final double[] coefficients;
    /** Effective degrees of freedom {@code tr((XᵀX + λP)⁻¹XᵀX)}: {@code penaltyOrder} = a polynomial, {@code basis.size()} = unpenalised. */
    public final double edf;
    /** Residual variance {@code RSS / (n − edf)} (NaN when not estimable). */
    public final double sigma2;
    public final long n;

    Smooth(final Basis basis, final int penaltyOrder, final double lambda, final boolean estimated,
           final double[] coefficients, final double edf, final double sigma2, final long n) {
        this.basis = basis;
        this.penaltyOrder = penaltyOrder;
        this.lambda = lambda;
        this.estimated = estimated;
        this.coefficients = coefficients;
        this.edf = edf;
        this.sigma2 = sigma2;
        this.n = n;
    }

    @Override
    public boolean isEmpty() {
        return coefficients.length == 0;
    }

    @Override
    public String describe() {
        return basis.size() + " basis functions, lambda = " + lambda + ", edf = " + edf + ", n=" + n;
    }

    /** The curve at a key (clamped into the range), or null for a missing key or an empty fit. */
    public Double curve(final Double x) {
        if (x == null || x.isNaN() || isEmpty()) return null;
        final double[] b = basis.evaluate(x);
        double f = 0;
        for (int i = 0; i < b.length; i++) f += b[i] * coefficients[i];
        return f;
    }

    /** What the curve does not explain of a target: {@code y − f(x)}, null when either side is. */
    public Double residual(final Double x, final Double y) {
        final Double f = curve(x);
        return f == null || y == null || y.isNaN() ? null : y - f;
    }

    /** A row's contribution to the moments: {@code [B(x), y]}, or null when the key or the target is missing. */
    public static double[] contribution(final Basis basis, final Double x, final Double y) {
        if (x == null || y == null || x.isNaN() || y.isNaN() || y.isInfinite()) return null;
        final double[] z = new double[basis.size() + 1];
        basis.evaluateInto(x, z, 0);
        z[basis.size()] = y;
        return z;
    }

    /** The difference penalty {@code P = DᵀD} of the given order over {@code m} coefficients. */
    static double[][] penalty(final int m, final int order) {
        // the coefficients of the order-th difference: (−1)^(order − j) C(order, j)
        final double[] stencil = new double[order + 1];
        stencil[0] = (order % 2 == 0) ? 1 : -1;
        for (int j = 1; j <= order; j++) stencil[j] = -stencil[j - 1] * (order - j + 1) / j;
        final double[][] p = new double[m][m];
        for (int row = 0; row + order < m; row++) {
            for (int a = 0; a <= order; a++) for (int b = 0; b <= order; b++) p[row + a][row + b] += stencil[a] * stencil[b];
        }
        return p;
    }

    /**
     * @param lambda the declared penalty strength, or null to choose it by REML
     * @param warn   whether a fit that cannot be solved is reported: a forward fit solves one model per change point
     *               ({@link BlockSeries#models}), and an empty window at a leave point is normal
     */
    public static Smooth fit(final Svd.Moments moments, final Basis basis, final int penaltyOrder, final Double lambda, final boolean warn) {
        final int m = basis.size();
        // fewer rows than the penalty's null space (a polynomial of degree penaltyOrder − 1) has parameters: nothing to estimate
        if (moments.n <= penaltyOrder) {
            if (warn) LOG.warn("smooth: {} row(s) with a key and a target to fit (penalty.order {} needs more); no curve, every key maps to null", moments.n, penaltyOrder);
            return empty(basis, penaltyOrder, moments.n);
        }
        if (moments.dimension != m + 1) {
            if (warn) LOG.warn("smooth: the fitted moments carry {} number(s) per row but the basis of {} function(s) plus the target needs {}; no curve",
                    moments.dimension, m, m + 1);
            return empty(basis, penaltyOrder, moments.n);
        }
        final double n = moments.n;
        final int d = m + 1;
        // centred moments Σ(z − z̄)(z − z̄)ᵀ do not depend on the anchor; XᵀX is rebuilt from them and the basis means
        final double[] mean = new double[d];
        for (int i = 0; i < d; i++) mean[i] = moments.shift[i] + moments.sum[i] / n;
        final double[][] a = new double[m][m];
        final double[] r = new double[m];
        for (int i = 0; i < m; i++) {
            for (int j = 0; j < m; j++) a[i][j] = moments.products[i * d + j] - moments.sum[i] * moments.sum[j] / n + n * mean[i] * mean[j];
            r[i] = moments.products[i * d + m] - moments.sum[i] * moments.sum[m] / n;
        }
        final double yy = Math.max(0, moments.products[m * d + m] - moments.sum[m] * moments.sum[m] / n);
        final double[][] p = penalty(m, penaltyOrder);

        double traceA = 0, traceP = 0;
        for (int i = 0; i < m; i++) {
            traceA += a[i][i];
            traceP += p[i][i];
        }
        final Solver solver = new Solver(a, r, p, yy, n, penaltyOrder, traceA / m * 1e-10);
        final double chosen;
        if (lambda != null) {
            chosen = lambda;
        } else {
            final double center = Math.log10(traceA / traceP);
            // a non-finite centre (an empty design, or a penalty with nothing to penalise: basis size ≤ the order)
            // would make the grid below step from ±∞ by a finite amount and never terminate
            if (!Double.isFinite(center)) {
                if (warn) LOG.warn("smooth: no strength scale to search (tr(XᵀX) = {}, tr(P) = {}, {} basis function(s) at penalty order {}); no curve",
                        traceA, traceP, m, penaltyOrder);
                return empty(basis, penaltyOrder, moments.n);
            }
            double best = Double.NaN, bestValue = Double.POSITIVE_INFINITY;
            for (double e = center - SEARCH_DECADES; e <= center + SEARCH_DECADES + 1e-9; e += SEARCH_STEP) {
                final double value = solver.reml(Math.pow(10, e));
                if (value < bestValue) {
                    bestValue = value;
                    best = e;
                }
            }
            if (Double.isNaN(best)) {
                if (warn) LOG.warn("smooth: the penalised system could not be solved at any strength (n = {}); no curve", moments.n);
                return empty(basis, penaltyOrder, moments.n);
            }
            chosen = Math.pow(10, refine(solver, best - SEARCH_STEP, best + SEARCH_STEP));
        }
        // one factorisation of (A + λP) serves the coefficients and the effective degrees of freedom
        final double[][] factor = solver.factor(chosen);
        if (factor == null) {
            if (warn) LOG.warn("smooth: the penalised system is singular at λ = {} (n = {}); no curve", chosen, moments.n);
            return empty(basis, penaltyOrder, moments.n);
        }
        final double[] beta = solver.solve(factor);
        final double edf = solver.edf(factor);
        final double rss = solver.rss(beta);
        final double sigma2 = n - edf > 0 ? rss / (n - edf) : Double.NaN;
        for (int i = 0; i < m; i++) beta[i] += mean[m];
        return new Smooth(basis, penaltyOrder, chosen, lambda == null, beta, edf, sigma2, moments.n);
    }

    private static Smooth empty(final Basis basis, final int penaltyOrder, final long n) {
        return new Smooth(basis, penaltyOrder, Double.NaN, false, new double[0], 0, Double.NaN, n);
    }

    /** Golden-section minimisation of the REML criterion over log10 λ in {@code [lo, hi]} (a fixed iteration count: deterministic). */
    private static double refine(final Solver solver, double lo, double hi) {
        final double phi = (Math.sqrt(5) - 1) / 2;
        double x1 = hi - phi * (hi - lo), x2 = lo + phi * (hi - lo);
        double f1 = solver.reml(Math.pow(10, x1)), f2 = solver.reml(Math.pow(10, x2));
        for (int i = 0; i < REFINE_ITERATIONS; i++) {
            if (f1 <= f2) {
                hi = x2;
                x2 = x1;
                f2 = f1;
                x1 = hi - phi * (hi - lo);
                f1 = solver.reml(Math.pow(10, x1));
            } else {
                lo = x1;
                x1 = x2;
                f1 = f2;
                x2 = lo + phi * (hi - lo);
                f2 = solver.reml(Math.pow(10, x2));
            }
        }
        return (lo + hi) / 2;
    }

    /** The penalised normal equations {@code (A + λP) β = r} of one fit, by Cholesky (m is tens). */
    private static final class Solver {
        private final double[][] a, p;
        private final double[] r;
        private final double yy, n, jitter;
        private final int order, m;

        Solver(final double[][] a, final double[] r, final double[][] p, final double yy, final double n, final int order, final double jitter) {
            this.a = a;
            this.r = r;
            this.p = p;
            this.yy = yy;
            this.n = n;
            this.order = order;
            this.jitter = jitter;
            this.m = r.length;
        }

        /** The lower Cholesky factor of {@code A + λP} (with a relative jitter on the diagonal when it is not positive definite as is), or null. */
        double[][] factor(final double lambda) {
            for (final double ridge : new double[]{0, jitter}) {
                final double[][] l = new double[m][m];
                boolean ok = true;
                for (int i = 0; i < m && ok; i++) {
                    for (int j = 0; j <= i; j++) {
                        double s = a[i][j] + lambda * p[i][j] + (i == j ? ridge : 0);
                        for (int k = 0; k < j; k++) s -= l[i][k] * l[j][k];
                        if (i == j) {
                            if (!(s > 0)) {
                                ok = false;
                                break;
                            }
                            l[i][i] = Math.sqrt(s);
                        } else {
                            l[i][j] = s / l[j][j];
                        }
                    }
                }
                if (ok) return l;
            }
            return null;
        }

        static double[] solve(final double[][] l, final double[] b) {
            final int m = b.length;
            final double[] x = new double[m];
            for (int i = 0; i < m; i++) {
                double s = b[i];
                for (int k = 0; k < i; k++) s -= l[i][k] * x[k];
                x[i] = s / l[i][i];
            }
            for (int i = m - 1; i >= 0; i--) {
                double s = x[i];
                for (int k = i + 1; k < m; k++) s -= l[k][i] * x[k];
                x[i] = s / l[i][i];
            }
            return x;
        }

        /** The coefficients of the system whose lower Cholesky factor is {@code l}. */
        double[] solve(final double[][] l) {
            return solve(l, r);
        }

        /** {@code tr((A + λP)⁻¹A)} from the factor of {@code A + λP}. */
        double edf(final double[][] l) {
            double trace = 0;
            final double[] column = new double[m];
            for (int j = 0; j < m; j++) {
                for (int i = 0; i < m; i++) column[i] = a[i][j];
                trace += solve(l, column)[j];
            }
            return trace;
        }

        /** The residual sum of squares of the centred target: {@code y'ᵀy' − 2βᵀr + βᵀAβ}. */
        double rss(final double[] beta) {
            double s = yy;
            for (int i = 0; i < m; i++) {
                double ab = 0;
                for (int j = 0; j < m; j++) ab += a[i][j] * beta[j];
                s += beta[i] * (ab - 2 * r[i]);
            }
            return Math.max(0, s);
        }

        /** The restricted likelihood criterion at λ (smaller is better), +∞ where the system cannot be factored. */
        double reml(final double lambda) {
            final double[][] l = factor(lambda);
            if (l == null) return Double.POSITIVE_INFINITY;
            final double[] beta = solve(l, r);
            double fitted = 0, logDet = 0;
            for (int i = 0; i < m; i++) {
                fitted += beta[i] * r[i];
                logDet += 2 * Math.log(l[i][i]);
            }
            // the penalised residual sum of squares, floored at the rounding level of the sums it is formed from
            final double penalised = Math.max(yy - fitted, yy * 1e-14 + Double.MIN_NORMAL);
            return (n - order) * Math.log(penalised) + logDet - (m - order) * Math.log(lambda);
        }
    }

    // ------------------------------------------------------------------------------------------
    // artifact
    // ------------------------------------------------------------------------------------------

    public static final FitArtifact.Json<Smooth> ARTIFACT = new FitArtifact.Json<>("smooth", "smooth", Smooth::fromJson,
            "the columns", "on an input with keys and targets");

    @Override
    public JsonObject toJson() {
        final JsonObject json = new JsonObject();
        json.addProperty("method", SPLINE);
        json.addProperty("lo", basis.lo());
        json.addProperty("hi", basis.hi());
        json.addProperty("segments", basis.segments());
        json.addProperty("degree", basis.degree());
        json.addProperty("penaltyOrder", penaltyOrder);
        json.addProperty("n", n);
        json.addProperty("estimated", estimated);
        // JSON has no ±Infinity / NaN: an empty fit has no strength, and a saturated one no variance, so those go
        // through the shared writer that keeps a finite value a number and falls back to its string form otherwise
        json.add("lambda", FitArtifact.lambdaJson(lambda));
        json.add("edf", FitArtifact.lambdaJson(edf));
        json.add("sigma2", FitArtifact.lambdaJson(sigma2));
        final JsonArray array = new JsonArray();
        for (final double c : coefficients) array.add(c);
        json.add("coefficients", array);
        return json;
    }

    public static Smooth fromJson(final JsonObject json) {
        final JsonElement n = json.get("n");
        if (n == null || !n.isJsonPrimitive()) throw new IllegalStateException("smooth artifact lacks 'n': " + json);
        final JsonArray array = json.getAsJsonArray("coefficients");
        final double[] coefficients = new double[array.size()];
        for (int i = 0; i < coefficients.length; i++) coefficients[i] = array.get(i).getAsDouble();
        final Basis basis = new Basis(json.get("lo").getAsDouble(), json.get("hi").getAsDouble(), json.get("segments").getAsInt(), json.get("degree").getAsInt());
        // getAsDouble accepts both forms lambdaJson writes (a number, or its string form when non-finite)
        return new Smooth(basis, json.get("penaltyOrder").getAsInt(), json.get("lambda").getAsDouble(),
                json.get("estimated").getAsBoolean(), coefficients, json.get("edf").getAsDouble(),
                json.get("sigma2").getAsDouble(), n.getAsLong());
    }

}
