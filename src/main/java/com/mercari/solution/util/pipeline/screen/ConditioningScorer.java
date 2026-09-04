package com.mercari.solution.util.pipeline.screen;

import com.mercari.solution.util.domain.math.MatrixOps;

import java.io.Serializable;
import java.util.Map;

/**
 * Per-unit computations of the conditioning (partial test) passes, pure like {@link GroupScorer}:
 * <ul>
 *   <li>{@link #moments}: sums for standardising the conditioning columns F (one pass);</li>
 *   <li>{@link #evaluate}: log-likelihood, gradient and Fisher information of η = offset + F̃·θ at a given θ
 *       (one pass per Newton iteration, driven by {@link FitState});</li>
 *   <li>{@link #partial}: at the fitted p̂, the bilinear sums {@code [s, b, a]} per column x transform from
 *       which the report orthogonalises x against F in the Fisher metric and reads the partial score test.</li>
 * </ul>
 * The grouped family is the conditional logit (group intercepts implicit); the binomial family always carries
 * an intercept column in F̃ (a calibration shift beyond the baseline, the prior rate without one).
 */
public final class ConditioningScorer implements Serializable {

    private final ScreenSpec spec;
    private final int offset;
    private final int kF;
    private final boolean intercept;
    /** number of fitted coefficients (conditioning columns + intercept for the binomial family) */
    public final int k;

    public ConditioningScorer(final ScreenSpec spec) {
        this.spec = spec;
        this.kF = spec.conditioningFields.size();
        this.intercept = !spec.isGroupedMultinomial();
        this.k = kF + (intercept ? 1 : 0);
        this.offset = spec.conditioningOffset();
    }

    /**
     * Standardisation sums of one row: {@code [n, Σ, Σ²]} per conditioning column over finite values, then the
     * weighted label sums {@code [Σ w y, Σ w]} (the starting point of the intercept, see {@link #initialTheta}).
     */
    public double[] moments(final ScreenRow row) {
        final double[] m = new double[3 * kF + 2];
        for (int j = 0; j < kF; j++) {
            final double v = row.x[offset + j];
            if (!ScreenMath.isFinite(v)) continue;
            m[3 * j] += 1;
            m[3 * j + 1] += v;
            m[3 * j + 2] += v * v;
        }
        m[3 * kF] = row.weight * row.label;
        m[3 * kF + 1] = row.weight;
        return m;
    }

    /**
     * The starting point of the Newton passes: θ = 0, except that without a baseline the intercept starts at the
     * link of the weighted label mean, so the first pass already sits at the prior-mean model. At θ = 0 the poisson
     * mean is 1 for every row and the first step on the intercept is ≈ log ȳ in one jump: for count labels with a
     * large mean the proposal overshoots (exp(η) overflows), the step halvings eat the pass budget and the partial
     * test runs at a non-MLE point. The binomial start moves from 0.5 to the prior rate, the gaussian from 0 to ȳ.
     */
    public double[] initialTheta(final double[] moments) {
        final double[] theta = new double[k];
        if (!intercept || spec.hasBaseline() || moments == null || moments.length < 3 * kF + 2) return theta;
        final double w = moments[3 * kF + 1];
        if (!(w > 0)) return theta;
        double mean = moments[3 * kF] / w;
        if (spec.isBinomial()) mean = Math.min(1 - GroupScorer.EPS, Math.max(GroupScorer.EPS, mean));
        if (spec.isPoisson() && !(mean > 0)) return theta;
        final double eta = spec.link(mean);
        if (Double.isFinite(eta)) theta[kF] = eta;
        return theta;
    }

    /** {@code [mean[], std[]]} from the summed moments; a constant column keeps std 1 (it becomes all zeros). */
    static double[][] scaling(final double[] moments, final int kF) {
        final double[] mean = new double[kF];
        final double[] std = new double[kF];
        for (int j = 0; j < kF; j++) {
            final double n = moments == null || moments.length < 3 * kF ? 0 : moments[3 * j];
            if (n > 0) {
                mean[j] = moments[3 * j + 1] / n;
                final double var = moments[3 * j + 2] / n - mean[j] * mean[j];
                std[j] = var > 1e-24 ? Math.sqrt(var) : 1d;
            } else {
                std[j] = 1d;
            }
        }
        return new double[][]{mean, std};
    }

    /** The standardised design F̃ of the unit (n × k): (x − mean) / std, missing → 0, intercept column last. */
    public double[][] design(final GroupScorer.Unit unit, final double[] moments) {
        final double[][] scale = scaling(moments, kF);
        final int n = unit.size();
        final double[][] f = new double[n][k];
        for (int i = 0; i < n; i++) {
            final double[] x = unit.rows.get(i).x;
            for (int j = 0; j < kF; j++) {
                final double v = x[offset + j];
                f[i][j] = ScreenMath.isFinite(v) ? (v - scale[0][j]) / scale[1][j] : 0d;
            }
            if (intercept) f[i][kF] = 1d;
        }
        return f;
    }

    /** Key of the gaussian residual-variance sums {@code [Σ w r̂², Σ w]} in the partial-pass map (never a column key). */
    public static final int SIGMA_KEY = -2;

    /**
     * Fitted means at θ: grouped softmax of log p + F̃θ within the unit; binomial σ(logit p + F̃θ); gaussian
     * μ + F̃θ (identity link); poisson exp(log μ + F̃θ). Without a baseline the offset is 0 and the intercept
     * column of F̃ carries the prior.
     */
    public double[] fitted(final GroupScorer.Unit unit, final double[][] f, final double[] theta) {
        final int n = unit.size();
        final double[] eta = new double[n];
        final boolean prior = !spec.hasBaseline();
        for (int i = 0; i < n; i++) {
            double e = MatrixOps.dot(f[i], theta);
            if (spec.isGroupedMultinomial()) {
                e += unit.p[i] > 0 ? Math.log(unit.p[i]) : Double.NEGATIVE_INFINITY;
            } else if (!prior) {
                if (spec.isBinomial()) e += Math.log(unit.p[i] / (1 - unit.p[i]));
                else if (spec.isPoisson()) e += Math.log(unit.p[i]);
                else e += unit.p[i];
            }
            eta[i] = e;
        }
        final double[] p = new double[n];
        if (spec.isGroupedMultinomial()) {
            double max = Double.NEGATIVE_INFINITY;
            for (final double e : eta) if (e > max) max = e;
            double sum = 0;
            for (int i = 0; i < n; i++) {
                p[i] = Math.exp(eta[i] - max);
                sum += p[i];
            }
            for (int i = 0; i < n; i++) p[i] /= sum;
        } else if (spec.isBinomial()) {
            for (int i = 0; i < n; i++) {
                final double s = 1d / (1d + Math.exp(-eta[i]));
                p[i] = Math.min(1 - GroupScorer.EPS, Math.max(GroupScorer.EPS, s));
            }
        } else if (spec.isPoisson()) {
            for (int i = 0; i < n; i++) p[i] = Math.exp(Math.min(eta[i], 700d));
        } else {
            System.arraycopy(eta, 0, p, 0, n);
        }
        return p;
    }

    /** Log-likelihood term of one row (gaussian at σ² = 1: the fit is least squares, σ² enters the report). */
    private double rowLogLikelihood(final double y, final double mu) {
        if (spec.isBinomial()) return y * Math.log(mu) + (1 - y) * Math.log(1 - mu);
        if (spec.isPoisson()) return y * Math.log(Math.max(mu, 1e-300)) - mu;
        return -0.5 * (y - mu) * (y - mu);
    }

    /**
     * One Newton pass evaluation of the unit at θ: {@code [units, ll, g(k), G(k*k)]} (weighted). Units are 1 per
     * group (grouped family) or the row count (binomial), each weighted like ll / g / G so that the average
     * objective of {@link FitState} is invariant to a rescaling of the weight column.
     */
    public double[] evaluate(final GroupScorer.Unit unit, final double[] theta, final double[] moments) {
        final double[][] f = design(unit, moments);
        final double[] p = fitted(unit, f, theta);
        final int n = unit.size();
        final double[] out = new double[FitState.evaluationLength(k)];
        if (spec.isGroupedMultinomial()) {
            final double w = unit.unitWeight;
            double ll = 0;
            final double[] pf = new double[k];
            for (int i = 0; i < n; i++) {
                if (unit.y[i] > 0) ll += unit.y[i] * Math.log(Math.max(p[i], 1e-300));
                for (int a = 0; a < k; a++) {
                    out[2 + a] += w * (unit.y[i] - p[i]) * f[i][a];
                    pf[a] += p[i] * f[i][a];
                    for (int b = 0; b < k; b++) out[2 + k + a * k + b] += w * p[i] * f[i][a] * f[i][b];
                }
            }
            for (int a = 0; a < k; a++) for (int b = 0; b < k; b++) out[2 + k + a * k + b] -= w * pf[a] * pf[b];
            // the unit count carries the same weight as ll / g / G, so the average objective is weight-invariant
            out[0] = w;
            out[1] = w * ll;
        } else {
            double ll = 0;
            double wsum = 0;
            for (int i = 0; i < n; i++) {
                final double w = unit.w[i];
                final double y = unit.y[i];
                wsum += w;
                ll += w * rowLogLikelihood(y, p[i]);
                final double v = spec.fisherWeight(p[i]);
                for (int a = 0; a < k; a++) {
                    out[2 + a] += w * (y - p[i]) * f[i][a];
                    for (int b = 0; b < k; b++) out[2 + k + a * k + b] += w * v * f[i][a] * f[i][b];
                }
            }
            out[0] = wsum;
            out[1] = ll;
        }
        return out;
    }

    /** Layout of a partial-test accumulator: {@code [s, b, a(k)]}. */
    public int partialLength() {
        return 2 + k;
    }

    /**
     * Adds, for every column x transform, the sums at the fitted p̂: s = x̃'(ỹ − p̂), b = x̃'W x̃, a = F̃'W x̃ with the
     * Fisher metric W (grouped: block diagonal diag(p̂) − p̂p̂' with x̃ centred by p̂ within the unit; binomial:
     * diag(p̂(1 − p̂)), the intercept column of F̃ doing the centring).
     */
    public void partial(final GroupScorer.Unit unit, final double[][] cols, final double[] theta, final double[] moments,
                        final Map<Integer, double[]> into) {
        final double[][] f = design(unit, moments);
        final double[] p = fitted(unit, f, theta);
        final int n = unit.size();
        final int nTransforms = spec.transforms.size();
        final double[] pf = new double[k];
        if (spec.isGroupedMultinomial()) {
            for (int i = 0; i < n; i++) for (int a = 0; a < k; a++) pf[a] += p[i] * f[i][a];
        }
        if (spec.isGaussian()) {
            // residual variance at the fitted model: the report divides the partial S / H by it
            final double[] sig = into.computeIfAbsent(SIGMA_KEY, key -> new double[partialLength()]);
            for (int i = 0; i < n; i++) {
                sig[0] += unit.w[i] * (unit.y[i] - p[i]) * (unit.y[i] - p[i]);
                sig[1] += unit.w[i];
            }
        }
        for (int c = 0; c < cols.length; c++) {
            for (int t = 0; t < nTransforms; t++) {
                final double[] v = GroupScorer.transform(spec.transforms.get(t), cols[c]);
                final double[] acc = into.computeIfAbsent(spec.key(c, t), key -> new double[partialLength()]);
                if (spec.isGroupedMultinomial()) {
                    double pm = 0, psum = 0;
                    for (int i = 0; i < n; i++) {
                        if (ScreenMath.isFinite(v[i])) {
                            pm += p[i] * v[i];
                            psum += p[i];
                        }
                    }
                    final double mean = psum > 0 ? pm / psum : 0d;
                    double s = 0, b = 0, px = 0;
                    final double[] a = new double[k];
                    for (int i = 0; i < n; i++) {
                        final double xt = ScreenMath.isFinite(v[i]) ? v[i] - mean : 0d;
                        s += xt * (unit.y[i] - p[i]);
                        b += p[i] * xt * xt;
                        px += p[i] * xt;
                        for (int j = 0; j < k; j++) a[j] += p[i] * xt * f[i][j];
                    }
                    final double w = unit.unitWeight;
                    acc[0] += w * s;
                    acc[1] += w * (b - px * px);
                    for (int j = 0; j < k; j++) acc[2 + j] += w * (a[j] - px * pf[j]);
                } else {
                    for (int i = 0; i < n; i++) {
                        if (!ScreenMath.isFinite(v[i])) continue;
                        final double w = unit.w[i];
                        final double vv = spec.fisherWeight(p[i]);
                        acc[0] += w * v[i] * (unit.y[i] - p[i]);
                        acc[1] += w * vv * v[i] * v[i];
                        for (int j = 0; j < k; j++) acc[2 + j] += w * vv * v[i] * f[i][j];
                    }
                }
            }
        }
    }
}
