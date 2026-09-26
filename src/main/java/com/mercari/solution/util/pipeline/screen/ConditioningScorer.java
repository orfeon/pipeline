package com.mercari.solution.util.pipeline.screen;

import com.mercari.solution.util.pipeline.glm.Baselines;
import com.mercari.solution.util.pipeline.glm.FitState;
import com.mercari.solution.util.pipeline.glm.GlmFit;
import com.mercari.solution.util.pipeline.glm.StatMath;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
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
    /** conditioning.missing: groupMean — a missing value is filled per unit (see {@link #design}) */
    private final boolean groupMeanFill;
    /** number of fitted coefficients (conditioning columns + intercept for the binomial family) */
    public final int k;

    public ConditioningScorer(final ScreenSpec spec) {
        this(spec, spec.conditioningOffset());
    }

    /** @param offset position of the first conditioning column in {@link ScreenRow#x} (0 for the projected rows of the fit passes) */
    public ConditioningScorer(final ScreenSpec spec, final int offset) {
        this(spec, offset, null);
    }

    /**
     * @param periodGram whether the partial pass carries the fitted model's Gram matrix per period (exact per-period
     *                   partial information); null = {@code k ≤ PERIOD_GRAM_MAX_K}
     */
    ConditioningScorer(final ScreenSpec spec, final int offset, final Boolean periodGram) {
        this.spec = spec;
        this.kF = spec.conditioningFields.size();
        this.intercept = !spec.isGroupedMultinomial();
        this.groupMeanFill = ScreenSpec.MISSING_GROUP_MEAN.equals(spec.conditioningMissing);
        this.k = kF + (intercept ? 1 : 0);
        this.offset = offset;
        this.periodGram = periodGram != null ? periodGram : k <= PERIOD_GRAM_MAX_K;
    }

    /**
     * Key of the fitted model's sums per period in the partial-pass map (never a column key): {@code [n, g(k), G(k*k)]}
     * — the unit mass, gradient and Gram matrix at θ̂, whose period slices decompose the fit's {@code bestGrad} /
     * {@code bestG} — or {@code [n, g(k)]} without the Gram (see {@link #PERIOD_GRAM_MAX_K}).
     */
    public static final int FIT_PERIOD_KEY = -3;

    /**
     * Conditioning size up to which the per-period Gram matrices are carried (periods × k² doubles in one accumulator).
     * Beyond it the report scales the window's γ'Gγ by the period's share of the unit mass instead: the per-period
     * partial score S⊥_p stays exact (it needs the gradient only), the per-period information is approximate.
     */
    public static final int PERIOD_GRAM_MAX_K = 100;

    /** whether the partial pass carries the per-period Gram matrices (see {@link #PERIOD_GRAM_MAX_K}) */
    private final boolean periodGram;

    /** Length of the {@link #FIT_PERIOD_KEY} vector: {@code 1 + k} plus {@code k²} with the Gram. */
    public int fitPeriodLength() {
        return 1 + k + (periodGram ? k * k : 0);
    }

    /**
     * Standardisation sums of one row: {@code [n, Σ, Σ²]} per conditioning column over finite values, then the
     * weighted label sums {@code [Σ w y, Σ w]} (the starting point of the intercept, see {@link #initialTheta}).
     */
    public double[] moments(final ScreenRow row) {
        final double[] m = new double[3 * kF + 2];
        for (int j = 0; j < kF; j++) {
            final double v = row.x[offset + j];
            if (!StatMath.isFinite(v)) continue;
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
        if (spec.isBinomial()) mean = Baselines.clamp(mean);
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

    /**
     * The standardised design F̃ of the unit (n × k): (x − mean) / std, intercept column last. A missing value is
     * the window mean (0 after standardising; {@code conditioning.missing: mean}) or, for the grouped family under
     * {@code groupMean}, the unit's baseline-weighted mean of its observed values: under the baseline p that fill
     * is the value at which the missing row contributes nothing to the unit's p-centred design — the rule the
     * candidate columns follow ({@link GroupScorer#groupedContribution}) — so the first Newton pass (θ = 0) is
     * exact; the later passes centre by the fitted p̂, where the fill stays the closest fixed value but is no
     * longer exactly neutral. A unit with no observed value falls back to the window mean.
     */
    public double[][] design(final GroupScorer.Unit unit, final double[] moments) {
        final double[][] scale = scaling(moments, kF);
        final int n = unit.size();
        final double[][] f = new double[n][k];
        final double[] fill = new double[kF];
        if (groupMeanFill) {
            for (int j = 0; j < kF; j++) {
                double pm = 0, psum = 0;
                for (int i = 0; i < n; i++) {
                    final double v = unit.rows.get(i).x[offset + j];
                    if (!StatMath.isFinite(v)) continue;
                    pm += unit.p[i] * v;
                    psum += unit.p[i];
                }
                if (psum > 0) fill[j] = (pm / psum - scale[0][j]) / scale[1][j];
            }
        }
        for (int i = 0; i < n; i++) {
            final double[] x = unit.rows.get(i).x;
            for (int j = 0; j < kF; j++) {
                final double v = x[offset + j];
                f[i][j] = StatMath.isFinite(v) ? (v - scale[0][j]) / scale[1][j] : fill[j];
            }
            if (intercept) f[i][kF] = 1d;
        }
        return f;
    }

    /** Key of the gaussian residual-variance sums {@code [Σ w r̂², Σ w]} in the partial-pass map (never a column key). */
    public static final int SIGMA_KEY = -2;

    /**
     * Fitted means at θ ({@link GlmFit#fitted}): grouped softmax of log p + F̃θ within the unit; binomial
     * σ(logit p + F̃θ); gaussian μ + F̃θ (identity link); poisson exp(log μ + F̃θ). Without a baseline the offset
     * is 0 and the intercept column of F̃ carries the prior.
     */
    public double[] fitted(final GroupScorer.Unit unit, final double[][] f, final double[] theta) {
        return GlmFit.fitted(spec.family(), !spec.hasBaseline(), unit.p, f, theta);
    }

    /**
     * One Newton pass evaluation of the unit at θ ({@link GlmFit#evaluate}): {@code [units, ll, g(k), G(k*k)]}
     * (weighted). Units are 1 per group (grouped family) or the row count (binomial), each weighted like ll / g /
     * G so that the average objective of {@link FitState} is invariant to a rescaling of the weight column.
     */
    public double[] evaluate(final GroupScorer.Unit unit, final double[] theta, final double[] moments) {
        final double[][] f = design(unit, moments);
        final double[] p = fitted(unit, f, theta);
        return GlmFit.evaluate(spec.family(), unit.y, p, unit.w, unit.unitWeight, f, k);
    }

    /** Layout of a partial-test accumulator: {@code [s, b, a(k)]}. */
    public int partialLength() {
        return 2 + k;
    }

    /** the window's quantile sketches (independent rows with rank / absdev), set per bundle from the side input; null = within-unit transforms */
    private transient WindowQuantiles quantiles;

    /** Sets the rank / absdev reference of independent rows (the same sketches the marginal pass used). */
    public ConditioningScorer withWindowQuantiles(final WindowQuantiles quantiles) {
        this.quantiles = quantiles;
        return this;
    }

    /**
     * Adds, for every column x transform, the sums at the fitted p̂: s = x̃'(ỹ − p̂), b = x̃'W x̃, a = F̃'W x̃ with the
     * Fisher metric W (grouped: block diagonal diag(p̂) − p̂p̂' with x̃ centred by p̂ within the unit; binomial:
     * diag(p̂(1 − p̂)), the intercept column of F̃ doing the centring). With {@code periods} the same sums go to the
     * unit's period (grouped) or each row's period (row families), and the fitted model's own sums at θ̂ —
     * {@code [n, g, G]} — go per period under {@link #FIT_PERIOD_KEY}, so the report can decompose the partial
     * statistic by period with the window's orthogonalisation coefficients.
     */
    public void partial(final GroupScorer.Unit unit, final double[][] cols, final double[] theta, final double[] moments,
                        final Map<Integer, PartialAccumulator> into) {
        GroupScorer.requireWindowQuantiles(spec, quantiles);
        final double[][] f = design(unit, moments);
        final double[] p = fitted(unit, f, theta);
        final int n = unit.size();
        final int nTransforms = spec.transforms.size();
        final boolean periods = spec.periodsBucket != null;
        if (spec.isGaussian()) {
            // residual variance at the fitted model: the report divides the partial S / H by it
            final double[] sig = new double[partialLength()];
            for (int i = 0; i < n; i++) {
                sig[0] += unit.w[i] * (unit.y[i] - p[i]) * (unit.y[i] - p[i]);
                sig[1] += unit.w[i];
            }
            into.computeIfAbsent(SIGMA_KEY, key -> new PartialAccumulator()).add(null, sig);
        }
        if (spec.isGroupedMultinomial()) {
            final String period = unit.period();
            final double[] pf = new double[k];
            for (int i = 0; i < n; i++) for (int a = 0; a < k; a++) pf[a] += p[i] * f[i][a];
            if (periods) into.computeIfAbsent(FIT_PERIOD_KEY, key -> new PartialAccumulator()).add(period, fitPeriodSums(unit, p, f, null));
            for (int c = 0; c < cols.length; c++) {
                for (int t = 0; t < nTransforms; t++) {
                    final double[] v = GroupScorer.transform(spec, quantiles, c, spec.transforms.get(t), cols[c]);
                    // the same pivot shift as the marginal test: a within-unit constant gives b = 0 exactly
                    final double pivot = GroupScorer.pivot(v);
                    double pm = 0, psum = 0;
                    for (int i = 0; i < n; i++) {
                        if (StatMath.isFinite(v[i])) {
                            pm += p[i] * (v[i] - pivot);
                            psum += p[i];
                        }
                    }
                    final double mean = psum > 0 ? pm / psum : 0d;
                    double s = 0, b = 0, px = 0;
                    final double[] a = new double[k];
                    for (int i = 0; i < n; i++) {
                        final double xt = StatMath.isFinite(v[i]) ? v[i] - pivot - mean : 0d;
                        s += xt * (unit.y[i] - p[i]);
                        b += p[i] * xt * xt;
                        px += p[i] * xt;
                        for (int j = 0; j < k; j++) a[j] += p[i] * xt * f[i][j];
                    }
                    final double w = unit.unitWeight;
                    final double[] acc = new double[partialLength()];
                    acc[0] = w * s;
                    acc[1] = w * (b - px * px);
                    for (int j = 0; j < k; j++) acc[2 + j] = w * (a[j] - px * pf[j]);
                    into.computeIfAbsent(spec.key(c, t), key -> new PartialAccumulator()).add(period, acc);
                }
            }
            return;
        }
        // row families: every row is its own period; the rows are bucketed once (one bucket without periods)
        final Map<String, List<Integer>> buckets = new LinkedHashMap<>();
        for (int i = 0; i < n; i++) buckets.computeIfAbsent(periods ? unit.rows.get(i).period : null, key -> new ArrayList<>()).add(i);
        if (periods) {
            final PartialAccumulator fit = into.computeIfAbsent(FIT_PERIOD_KEY, key -> new PartialAccumulator());
            for (final Map.Entry<String, List<Integer>> bucket : buckets.entrySet()) {
                fit.add(bucket.getKey(), fitPeriodSums(unit, p, f, buckets.size() == 1 ? null : bucket.getValue()));
            }
        }
        for (int c = 0; c < cols.length; c++) {
            for (int t = 0; t < nTransforms; t++) {
                // the transform is taken once over the whole unit (rank / absdev within the unit, or against the
                // window's sketches for independent rows), then summed per bucket
                final double[] v = GroupScorer.transform(spec, quantiles, c, spec.transforms.get(t), cols[c]);
                final PartialAccumulator target = into.computeIfAbsent(spec.key(c, t), key -> new PartialAccumulator());
                for (final Map.Entry<String, List<Integer>> bucket : buckets.entrySet()) {
                    final double[] acc = new double[partialLength()];
                    for (final int i : bucket.getValue()) {
                        if (!StatMath.isFinite(v[i])) continue;
                        final double w = unit.w[i];
                        final double vv = spec.fisherWeight(p[i]);
                        acc[0] += w * v[i] * (unit.y[i] - p[i]);
                        acc[1] += w * vv * v[i] * v[i];
                        for (int j = 0; j < k; j++) acc[2 + j] += w * vv * v[i] * f[i][j];
                    }
                    target.add(bucket.getKey(), acc);
                }
            }
        }
    }

    /**
     * The fitted model's {@code [n, g(k), G(k*k)?]} at θ̂ over {@code rows} of the unit (all rows when null; the
     * grouped family always passes the whole unit): with the Gram, the pass evaluation itself ({@link GlmFit#evaluate},
     * so the period slices sum to the fit's {@code bestGrad} / {@code bestG}); without it (k above
     * {@link #PERIOD_GRAM_MAX_K}) the unit mass and gradient only, skipping the O(n k²) Gram the evaluation would drop.
     */
    private double[] fitPeriodSums(final GroupScorer.Unit unit, final double[] p, final double[][] f, final List<Integer> rows) {
        final double[] y, mu, w;
        final double[][] ff;
        if (rows == null) {
            y = unit.y;
            mu = p;
            w = unit.w;
            ff = f;
        } else {
            final int size = rows.size();
            y = new double[size];
            mu = new double[size];
            w = new double[size];
            ff = new double[size][];
            for (int r = 0; r < size; r++) {
                final int i = rows.get(r);
                y[r] = unit.y[i];
                mu[r] = p[i];
                w[r] = unit.w[i];
                ff[r] = f[i];
            }
        }
        final int m = ff.length;
        final double[] out = new double[fitPeriodLength()];
        if (periodGram) {
            final double[] eval = GlmFit.evaluate(spec.family(), y, mu, w, unit.unitWeight, ff, k);
            out[0] = eval[0];
            System.arraycopy(eval, 2, out, 1, k + k * k);
            return out;
        }
        final boolean grouped = spec.isGroupedMultinomial();
        for (int r = 0; r < m; r++) {
            final double wr = grouped ? unit.unitWeight : w[r];
            if (!grouped) out[0] += wr;
            for (int a = 0; a < k; a++) out[1 + a] += wr * (y[r] - mu[r]) * ff[r][a];
        }
        if (grouped) out[0] = unit.unitWeight;
        return out;
    }
}
