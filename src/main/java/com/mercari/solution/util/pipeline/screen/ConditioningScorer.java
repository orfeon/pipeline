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
    /** the joint columns' sums at the fitted p̂ (DSL doc §9.5), laid out by {@link JointPartialLayout} */
    public static final int JOINT_PARTIAL_KEY = -4;
    /**
     * the slice of {@link #FIT_PERIOD_KEY} holding the fit's {@code [n, g, G]} over the suggestions' discovery half
     * (DSL doc §9.4): the halves' partial blocks are orthogonalised with the half's own gradient and Gram (the
     * confirmation half's being the window's less this); no period is named like this
     */
    public static final String DISCOVERY_SLICE = "\u0001discovery";

    /**
     * The offsets of the partial joint sums vector for m joint columns and k fitted coefficients (DSL doc §9.5):
     * {@code [S (m), H (packed m), M (packed m), A = Σ w v x̃ f̃' (m × k), Mxf = Σ w r x̃ f̃' (m × k), Mff = Σ w r f̃f̃'
     * (packed k), g = Σ w r f̃ (k), G = Σ w v f̃f̃' (packed k), used, filled, dropped]}; the packed blocks are upper
     * triangles, row-major ({@link GroupScorer#packed}). g and G are the fit's gradient and Gram over the rows the joint
     * sums keep — the fit's own {@code bestGrad} / {@code bestG} unless a row family left rows out for a missing value.
     */
    record JointPartialLayout(int m, int k, int h, int mm, int a, int mxf, int mff, int g, int gff, int used, int length) {
        static JointPartialLayout of(final int m, final int k) {
            final int pm = m * (m + 1) / 2, pk = k * (k + 1) / 2;
            final int h = m, mm = h + pm, a = mm + pm, mxf = a + m * k, mff = mxf + m * k, g = mff + pk, gff = g + k, used = gff + pk;
            return new JointPartialLayout(m, k, h, mm, a, mxf, mff, g, gff, used, used + 3);
        }

        /** The slot counting the rows (grouped: units) added with a missing joint value filled. */
        int filled() {
            return used + 1;
        }

        /** The slot counting the rows left out for a missing joint value with no fill (a row family without the window means). */
        int dropped() {
            return used + 2;
        }
    }

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

    /** the window's quantile sketches (the rank / absdev reference of independent rows, the value bins' and the pair grids' edges), set per bundle from the side input */
    private transient WindowQuantiles quantiles;

    /** Sets the rank / absdev reference of independent rows (the same sketches the marginal pass used). */
    public ConditioningScorer withWindowQuantiles(final WindowQuantiles quantiles) {
        this.quantiles = quantiles;
        if (binner != null) binner.withWindowQuantiles(quantiles);
        return this;
    }

    /** the binned test's bin assignment and the pair grids' edges (the marginal scorer's rules and edge caches) */
    private transient GroupScorer binner;

    private GroupScorer binner() {
        if (binner == null) binner = new GroupScorer(spec).withWindowQuantiles(quantiles);
        return binner;
    }

    private int[] bins(final int column, final double[] v) {
        return binner().bins(column, v);
    }

    /**
     * Adds the binned block's partial sums at the fitted p̂ into {@code into} (DSL doc §6.1, the generalisation of
     * {@code [s, b, a]} to a one-hot block of B bins): {@code [s (B), H (B² grouped / B diagonal for the row families),
     * A (B × k)]}. Grouped: s_b = w Σ_{i in b} (ỹ_i − p̂_i), H = w (diag(P̂) − P̂ P̂'), A_bj = w (Σ_{i in b} p̂_i f_ij −
     * P̂_b Σ_i p̂_i f_ij); row families: s_b = Σ_{i in b} w_i (y_i − p̂_i), H_bb = Σ_{i in b} w_i v̂_i,
     * A_bj = Σ_{i in b} w_i v̂_i f_ij. Only the bins the unit occupies are touched (an empty bin's sums are zero).
     */
    private void binnedPartial(final GroupScorer.Unit unit, final int[] bins, final double[][] f, final double[] p, final PartialAccumulator into,
                               final boolean discovery) {
        binnedPartial(spec.binCount(), unit, bins, f, p, into, spec.suggestionsOn, discovery);
    }

    /** The length of a block's partial sums over {@code nb} cells: {@code [s (B), H (B² grouped / B), A (B × k)]}. */
    int binnedPartialLength(final int nb) {
        return spec.isGroupedMultinomial() ? nb + nb * nb + nb * k : nb + nb + nb * k;
    }

    /**
     * {@link #binnedPartial(GroupScorer.Unit, int[], double[][], double[], PartialAccumulator, boolean)} over a block of
     * {@code nb} cells (a categorical column's levels, DSL doc §6.2). With {@code split} the accumulator's vector is kept
     * twice — the window's sums, then the discovery half's (the same seeded split of the units as the marginal pass,
     * {@link GroupScorer#discovery}, whose verdict for the unit is {@code discovery}) — so the suggestions can be chosen
     * and confirmed on the partial sums (DSL doc §9.4). Both halves are added in place over the occupied bins only.
     */
    private void binnedPartial(final int nb, final GroupScorer.Unit unit, final int[] bins, final double[][] f, final double[] p,
                               final PartialAccumulator into, final boolean split, final boolean discovery) {
        final int len = binnedPartialLength(nb);
        final double[] out = into.total(split ? 2 * len : len);
        binnedPartialInto(nb, unit, bins, f, p, out, 0);
        if (split && discovery) binnedPartialInto(nb, unit, bins, f, p, out, len);
    }

    /** Adds the block's partial sums of the unit into {@code out} from {@code base} on (the window's or the discovery half's slot). */
    private void binnedPartialInto(final int nb, final GroupScorer.Unit unit, final int[] bins, final double[][] f, final double[] p, final double[] out,
                                   final int base) {
        final int n = unit.size();
        if (spec.isGroupedMultinomial()) {
            final int aOffset = base + nb + nb * nb;
            final double[] sb = new double[nb];
            final double[] pb = new double[nb];
            // Σ_{i in b} p̂_i f_i, allocated for the occupied bins only
            final double[][] pbf = new double[nb][];
            final double[] pf = new double[k];
            final int[] occupied = new int[Math.min(nb, n)];
            int m = 0;
            for (int i = 0; i < n; i++) {
                final int b = bins[i];
                if (pbf[b] == null) {
                    pbf[b] = new double[k];
                    occupied[m++] = b;
                }
                sb[b] += unit.y[i] - p[i];
                pb[b] += p[i];
                for (int j = 0; j < k; j++) {
                    final double pfij = p[i] * f[i][j];
                    pf[j] += pfij;
                    pbf[b][j] += pfij;
                }
            }
            final double w = unit.unitWeight;
            for (int x = 0; x < m; x++) {
                final int b = occupied[x];
                out[base + b] += w * sb[b];
                for (int z = 0; z < m; z++) {
                    final int c = occupied[z];
                    out[base + nb + b * nb + c] += w * ((b == c ? pb[b] : 0d) - pb[b] * pb[c]);
                }
                for (int j = 0; j < k; j++) out[aOffset + b * k + j] += w * (pbf[b][j] - pb[b] * pf[j]);
            }
            return;
        }
        for (int i = 0; i < n; i++) {
            final double w = unit.w[i];
            final double vv = spec.fisherWeight(p[i]);
            out[base + bins[i]] += w * (unit.y[i] - p[i]);
            out[base + nb + bins[i]] += w * vv;
            for (int j = 0; j < k; j++) out[base + 2 * nb + bins[i] * k + j] += w * vv * f[i][j];
        }
    }

    /**
     * Adds, for every column x transform, the sums at the fitted p̂: s = x̃'(ỹ − p̂), b = x̃'W x̃, a = F̃'W x̃ with the
     * Fisher metric W (grouped: block diagonal diag(p̂) − p̂p̂' with x̃ centred by p̂ within the unit; binomial:
     * diag(p̂(1 − p̂)), the intercept column of F̃ doing the centring). With {@code periods} the same sums go to the
     * unit's period (grouped) or each row's period (row families), and the fitted model's own sums at θ̂ —
     * {@code [n, g, G]} — go per period under {@link #FIT_PERIOD_KEY}, so the report can decompose the partial
     * statistic by period with the window's orthogonalisation coefficients. A heterogeneity modifier's level gets the
     * same sums as a slice under {@link ScoreAccumulator#LEVEL_PREFIX} (DSL doc §7.1).
     */
    public void partial(final GroupScorer.Unit unit, final double[][] cols, final double[] theta, final double[] moments,
                        final Map<Integer, PartialAccumulator> into) {
        GroupScorer.requireWindowQuantiles(spec, quantiles);
        final double[][] f = design(unit, moments);
        final double[] p = fitted(unit, f, theta);
        final int n = unit.size();
        final int nTransforms = spec.transforms.size();
        final boolean periods = spec.periodsBucket != null;
        // the suggestions' half of this unit: one seeded hash per unit, not per column (as the marginal pass)
        final boolean discovery = spec.suggestionsOn && binner().discovery(unit.key);
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
            final double[] pf = new double[k];
            for (int i = 0; i < n; i++) for (int a = 0; a < k; a++) pf[a] += p[i] * f[i][a];
            // the unit's own (period, level) cell: the modifier is a unit-level field for the grouped family
            final Cell cell = new Cell(periods ? unit.period() : null, unit.level());
            if (periods || cell.level() != null) {
                cell.addTo(into.computeIfAbsent(FIT_PERIOD_KEY, key -> new PartialAccumulator()), fitPeriodSums(unit, p, f, null));
            }
            for (int c = 0; c < cols.length; c++) {
                for (int t = 0; t < nTransforms; t++) {
                    if (ScreenSpec.isBinned(spec.transforms.get(t))) {
                        // the block's sums carry no period slices (the binned test has no sign to agree on)
                        binnedPartial(unit, bins(c, cols[c]), f, p, into.computeIfAbsent(spec.key(c, t), key -> new PartialAccumulator()), discovery);
                        continue;
                    }
                    final double[] v = GroupScorer.transform(spec, quantiles, c, spec.transforms.get(t), cols[c]);
                    final double[] acc = groupedPartialSums(unit, p, f, pf, v);
                    cell.addTo(into.computeIfAbsent(spec.key(c, t), key -> new PartialAccumulator()), acc);
                }
            }
            // the declared pairs: the product of two standardised conditioning columns (a placebo pair: a member
            // times a noise column) as one more column of the partial pass, no period slices (DSL doc §8.6)
            for (int q = 0; q < spec.pairCount(); q++) {
                into.computeIfAbsent(spec.pairKey(q), key -> new PartialAccumulator()).add(null, groupedPartialSums(unit, p, f, pf, pairColumn(q, unit, f, cols)));
            }
            // the real pairs' 2-D grids (DSL doc §8.7): the one-hot block of k × k cells at the fitted means
            for (int q = 0; q < spec.pairs.size(); q++) {
                final double[] grid = pairGrid(unit, q, p);
                if (grid != null) into.computeIfAbsent(spec.pairGridKey(q), key -> new PartialAccumulator()).add(null, grid);
            }
            categoricalBlocks(unit, f, p, into);
            addJointPartial(unit, cols, f, p, into);
            if (discovery) addDiscoveryFit(unit, f, p, into);
            return;
        }
        // row families: every row is its own period and modifier level; the rows are bucketed once into (period, level)
        // cells (one cell without periods or a modifier) and each cell's sums, computed once, go to the total, its
        // period and — as a slice — its level (the total holds the rows once)
        final Map<Cell, List<Integer>> cells = new LinkedHashMap<>();
        boolean levels = false;
        for (int i = 0; i < n; i++) {
            final ScreenRow row = unit.rows.get(i);
            levels |= row.level != null;
            cells.computeIfAbsent(new Cell(periods ? row.period : null, row.level), key -> new ArrayList<>()).add(i);
        }
        if (periods || levels) {
            final PartialAccumulator fit = into.computeIfAbsent(FIT_PERIOD_KEY, key -> new PartialAccumulator());
            for (final Map.Entry<Cell, List<Integer>> cell : cells.entrySet()) {
                cell.getKey().addTo(fit, fitPeriodSums(unit, p, f, cells.size() == 1 ? null : cell.getValue()));
            }
        }
        for (int c = 0; c < cols.length; c++) {
            for (int t = 0; t < nTransforms; t++) {
                if (ScreenSpec.isBinned(spec.transforms.get(t))) {
                    binnedPartial(unit, bins(c, cols[c]), f, p, into.computeIfAbsent(spec.key(c, t), key -> new PartialAccumulator()), discovery);
                    continue;
                }
                // the transform is taken once over the whole unit (rank / absdev within the unit, or against the
                // window's sketches for independent rows), then summed per cell
                final double[] v = GroupScorer.transform(spec, quantiles, c, spec.transforms.get(t), cols[c]);
                final PartialAccumulator target = into.computeIfAbsent(spec.key(c, t), key -> new PartialAccumulator());
                for (final Map.Entry<Cell, List<Integer>> cell : cells.entrySet()) {
                    cell.getKey().addTo(target, rowPartialSums(unit, p, f, v, cell.getValue()));
                }
            }
        }
        // the declared pairs (DSL doc §8.6): one more column each over every row, no period slices
        for (int q = 0; q < spec.pairCount(); q++) {
            into.computeIfAbsent(spec.pairKey(q), key -> new PartialAccumulator()).add(null, rowPartialSums(unit, p, f, pairColumn(q, unit, f, cols), null));
        }
        // the real pairs' 2-D grids (DSL doc §8.7): the one-hot block of k × k cells at the fitted means
        for (int q = 0; q < spec.pairs.size(); q++) {
            final double[] grid = pairGrid(unit, q, p);
            if (grid != null) into.computeIfAbsent(spec.pairGridKey(q), key -> new PartialAccumulator()).add(null, grid);
        }
        categoricalBlocks(unit, f, p, into);
        addJointPartial(unit, cols, f, p, into);
        if (discovery) addDiscoveryFit(unit, f, p, into);
    }

    /**
     * Adds the fit's {@code [n, g, G]} over a unit of the suggestions' discovery half (the marginal pass's seeded split)
     * to the {@link #DISCOVERY_SLICE} of {@link #FIT_PERIOD_KEY}, so the report orthogonalises the halves' binned blocks
     * exactly (DSL doc §9.4). Called only with {@code suggestions}, for a discovery unit.
     */
    private void addDiscoveryFit(final GroupScorer.Unit unit, final double[][] f, final double[] p, final Map<Integer, PartialAccumulator> into) {
        into.computeIfAbsent(FIT_PERIOD_KEY, key -> new PartialAccumulator()).addSlice(DISCOVERY_SLICE, fitPeriodSums(unit, p, f, null));
    }

    /**
     * Adds the unit's contribution to the joint columns' sums at the fitted p̂ (DSL doc §9.5) under
     * {@link #JOINT_PARTIAL_KEY}, laid out by {@link JointPartialLayout}: S = Σ w r x̃, H = Σ w v x̃x̃', M = Σ w r x̃x̃',
     * A = Σ w v x̃ f̃', Mxf = Σ w r x̃ f̃', Mff = Σ w r f̃f̃', and the fit's own g = Σ w r f̃ and G = Σ w v f̃f̃' over the
     * same rows. The report orthogonalises against F with Γ = G⁻¹A (S⊥ = S − Γ'g, H⊥ = H − Γ'A' − AΓ + Γ'GΓ,
     * M⊥ = M − Γ'Mxf' − MxfΓ + Γ'MffΓ) and reads the several-candidate suggestions off the partial sums. Grouped: x̃
     * centred by p̂ over the unit's observed rows (a missing value 0, the marginal joint's rule — Σ p̂ x̃ = 0, so W x̃
     * needs no further centring), f̃ = f − Σ p̂ f and W = diag(p̂) − p̂p̂' as {@link #groupedPartialSums}; row families:
     * x shifted by the window mean (a missing value 0; a row missing a value without a mean is left out and counted),
     * F̃ with its intercept doing the centring, v the Fisher weight at p̂. Nothing without {@code joint}.
     */
    private void addJointPartial(final GroupScorer.Unit unit, final double[][] cols, final double[][] f, final double[] p,
                                 final Map<Integer, PartialAccumulator> into) {
        final int m = spec.jointOn ? spec.jointColumnCount() : 0;
        if (m < 2) return;
        final JointPartialLayout at = JointPartialLayout.of(m, k);
        final double[] out = into.computeIfAbsent(JOINT_PARTIAL_KEY, key -> new PartialAccumulator()).total(at.length());
        final int n = unit.size();
        final double[] x = new double[m];
        if (spec.isGroupedMultinomial()) {
            final double[][] xt = new double[n][m];
            boolean filled = false;
            for (int j = 0; j < m; j++) {
                final double[] col = cols[spec.jointColumn(j)];
                final double pivot = GroupScorer.pivot(col);
                double pm = 0, psum = 0;
                for (int i = 0; i < n; i++) {
                    if (!StatMath.isFinite(col[i])) continue;
                    pm += p[i] * (col[i] - pivot);
                    psum += p[i];
                }
                final double mean = psum > 0 ? pm / psum : 0d;
                for (int i = 0; i < n; i++) {
                    if (StatMath.isFinite(col[i])) xt[i][j] = col[i] - pivot - mean;
                    else filled = true;
                }
            }
            final double[] pf = new double[k];
            for (int i = 0; i < n; i++) for (int a = 0; a < k; a++) pf[a] += p[i] * f[i][a];
            final double w = unit.unitWeight;
            final double[] fc = new double[k];
            for (int i = 0; i < n; i++) {
                final double r = unit.y[i] - p[i];
                for (int a = 0; a < k; a++) fc[a] = f[i][a] - pf[a];
                System.arraycopy(xt[i], 0, x, 0, m);
                addJointOuter(out, at, x, fc, w * p[i], w * r);
            }
            out[at.used()] += 1;
            if (filled) out[at.filled()] += 1;
            return;
        }
        final double[] means = binner().jointMeans();
        int used = 0, filled = 0, dropped = 0;
        for (int i = 0; i < n; i++) {
            boolean finite = true, fill = false;
            for (int j = 0; j < m && finite; j++) {
                final double v = cols[spec.jointColumn(j)][i];
                final boolean shift = means != null && Double.isFinite(means[j]);
                if (StatMath.isFinite(v)) {
                    x[j] = shift ? v - means[j] : v;
                } else if (shift) {
                    x[j] = 0d;
                    fill = true;
                } else {
                    finite = false;
                }
            }
            if (!finite) {
                dropped++;
                continue;
            }
            if (fill) filled++;
            final double w = unit.w[i];
            addJointOuter(out, at, x, f[i], w * spec.fisherWeight(p[i]), w * (unit.y[i] - p[i]));
            used++;
        }
        out[at.used()] += used;
        out[at.filled()] += filled;
        out[at.dropped()] += dropped;
    }

    /**
     * Adds S += b·x, H += a·xx', M += b·xx', A += a·x f', Mxf += b·x f', Mff += b·f f', g += b·f, G += a·f f' (packed
     * upper triangles).
     */
    private static void addJointOuter(final double[] out, final JointPartialLayout at, final double[] x, final double[] f, final double a, final double b) {
        final int m = x.length, k = f.length;
        int q = 0;
        for (int j = 0; j < m; j++) {
            final double aj = a * x[j], bj = b * x[j];
            out[j] += bj;
            for (int l = j; l < m; l++, q++) {
                out[at.h() + q] += aj * x[l];
                out[at.mm() + q] += bj * x[l];
            }
            for (int c = 0; c < k; c++) {
                out[at.a() + j * k + c] += aj * f[c];
                out[at.mxf() + j * k + c] += bj * f[c];
            }
        }
        q = 0;
        for (int c = 0; c < k; c++) {
            final double ac = a * f[c], bc = b * f[c];
            out[at.g() + c] += bc;
            for (int d = c; d < k; d++, q++) {
                out[at.mff() + q] += bc * f[d];
                out[at.gff() + q] += ac * f[d];
            }
        }
    }

    /**
     * The categorical candidates' blocks at the fitted p̂ (DSL doc §6.2): a column's levels (and its placebos'
     * redrawn levels) as a one-hot block of {@code [s, H, A]} sums under the marginal keys, no period slices.
     */
    private void categoricalBlocks(final GroupScorer.Unit unit, final double[][] f, final double[] p, final Map<Integer, PartialAccumulator> into) {
        if (!spec.hasCategoricals()) return;
        if (binner == null) binner = new GroupScorer(spec).withWindowQuantiles(quantiles);
        for (int c = 0; c < spec.categoricals.size(); c++) {
            final WindowQuantiles.Levels levels = binner.categoricalLevels(c);
            if (levels == null || levels.size() < 2) continue;
            for (int r = -1; r < spec.categoricalPlacebo; r++) {
                final int[] idx = r < 0 ? GroupScorer.levelIndices(unit, c, levels) : binner.placeboLevels(unit, c, r, levels);
                binnedPartial(levels.size(), unit, idx, f, p, into.computeIfAbsent(spec.categoricalKey(c, r), key -> new PartialAccumulator()), false, false);
            }
        }
    }

    /**
     * A real pair's 2-D grid at the fitted means (DSL doc §8.7): the members' raw values binned by their window
     * quantile edges (k = pairs.shape per member, cell = a_bin × k + b_bin; a row with a missing member left out),
     * as the one-hot block of K = k² cells — row families {@code [Σ w (y − p̂) per cell, Σ w v̂ per cell]}, grouped
     * {@code [S_cell, P_cell, (P P')]} scaled by the unit weight. Null without both members' edges or any cell hit.
     */
    private double[] pairGrid(final GroupScorer.Unit unit, final int pair, final double[] p) {
        if (!spec.hasPairShape()) return null;
        final int[] members = spec.pairMembers(pair);
        // the edges by sketch (the full row's x column), the values at this scorer's offset
        final double[] ea = binner().gridEdges(spec.conditioningColumn(members[0]));
        final double[] eb = binner().gridEdges(spec.conditioningColumn(members[1]));
        if (ea == null || eb == null) return null;
        final int kk = spec.pairShapeBins, cells = kk * kk, n = unit.size();
        final int[] cell = new int[n];
        boolean any = false;
        for (int i = 0; i < n; i++) {
            final double[] x = unit.rows.get(i).x;
            final int a = GroupScorer.gridBin(ea, x[offset + members[0]]);
            final int b = GroupScorer.gridBin(eb, x[offset + members[1]]);
            cell[i] = a < 0 || b < 0 ? -1 : a * kk + b;
            any |= cell[i] >= 0;
        }
        if (!any) return null;
        if (spec.isGroupedMultinomial()) {
            final double[] s = new double[cells], pc = new double[cells];
            for (int i = 0; i < n; i++) {
                if (cell[i] < 0) continue;
                s[cell[i]] += unit.y[i] - p[i];
                pc[cell[i]] += p[i];
            }
            final double w = unit.unitWeight;
            final double[] out = new double[2 * cells + cells * cells];
            for (int c = 0; c < cells; c++) {
                out[c] = w * s[c];
                out[cells + c] = w * pc[c];
                for (int d = 0; d < cells; d++) out[2 * cells + c * cells + d] = w * pc[c] * pc[d];
            }
            return out;
        }
        final double[] out = new double[2 * cells];
        for (int i = 0; i < n; i++) {
            if (cell[i] < 0) continue;
            out[cell[i]] += unit.w[i] * (unit.y[i] - p[i]);
            out[cells + cell[i]] += unit.w[i] * spec.fisherWeight(p[i]);
        }
        return out;
    }

    /**
     * A pair's column: the product of its members' standardised conditioning columns, or for a placebo pair the
     * first member times a noise placebo column (a standard normal draw independent of everything, the
     * calibration of the pair kind). A row missing a member is missing (NaN: it contributes nothing, as a missing
     * candidate value does), not the product of the design's fill — the recipe {@code a * b} is null there, and a
     * filled product would carry the members' missingness (0 where the observed products average their
     * correlation) as a spurious interaction the member × noise placebos do not calibrate.
     */
    double[] pairColumn(final int pair, final GroupScorer.Unit unit, final double[][] f, final double[][] cols) {
        final int[] members = spec.pairMembers(pair);
        final int n = f.length;
        final double[] z = new double[n];
        final double[] noise = members[1] >= 0 ? null : cols[spec.candidates.size() + (-1 - members[1])];
        for (int i = 0; i < n; i++) {
            final double[] x = unit.rows.get(i).x;
            final boolean missing = !StatMath.isFinite(x[offset + members[0]]) || (noise == null && !StatMath.isFinite(x[offset + members[1]]));
            z[i] = missing ? Double.NaN : f[i][members[0]] * (noise == null ? f[i][members[1]] : noise[i]);
        }
        return z;
    }

    /**
     * The grouped family's {@code [s, b, a]} of one column at the fitted p̂, scaled by the unit weight: the column
     * shifted by its pivot and centred by p̂ over its finite values (a within-unit constant gives b = 0 exactly),
     * s = x̃'(ỹ − p̂), b = x̃'Wx̃, a = F̃'Wx̃ with W = diag(p̂) − p̂p̂' and {@code pf} = Σ p̂ F̃ over the unit.
     */
    private double[] groupedPartialSums(final GroupScorer.Unit unit, final double[] p, final double[][] f, final double[] pf, final double[] v) {
        final int n = unit.size();
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
        return acc;
    }

    /** A bucket of the partial pass (a grouped unit, or rows of a row-family unit): a period (null without periods) and a modifier level (null without one). */
    private record Cell(String period, String level) {
        /** Adds the cell's sums to the total and its period, and to its level as a slice (the accumulator copies them). */
        void addTo(final PartialAccumulator into, final double[] sums) {
            into.add(period, sums);
            if (level != null) into.addSlice(ScoreAccumulator.LEVEL_PREFIX + level, sums);
        }
    }

    /** A row family's {@code [s, b, a]} over {@code rows} of the unit at the fitted p̂ (one (period, level) cell). */
    private double[] rowPartialSums(final GroupScorer.Unit unit, final double[] p, final double[][] f, final double[] v, final List<Integer> rows) {
        final double[] acc = new double[partialLength()];
        final int m = rows == null ? unit.size() : rows.size();
        for (int r = 0; r < m; r++) {
            final int i = rows == null ? r : rows.get(r);
            if (!StatMath.isFinite(v[i])) continue;
            final double w = unit.w[i];
            final double vv = spec.fisherWeight(p[i]);
            acc[0] += w * v[i] * (unit.y[i] - p[i]);
            acc[1] += w * vv * v[i] * v[i];
            for (int j = 0; j < k; j++) acc[2 + j] += w * vv * v[i] * f[i][j];
        }
        return acc;
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
