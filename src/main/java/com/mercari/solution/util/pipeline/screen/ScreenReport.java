package com.mercari.solution.util.pipeline.screen;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.domain.math.MatrixOps;
import com.mercari.solution.util.pipeline.glm.Baselines;
import com.mercari.solution.util.pipeline.glm.FitState;
import com.mercari.solution.util.pipeline.glm.StatMath;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Turns the combined accumulators into the scoring records and the run summary. Pure: the statistics,
 * placebo threshold, q-values, flags and (with conditioning) the orthogonalisation are all closed-form over
 * the (small) accumulator set.
 */
public final class ScreenReport {

    private ScreenReport() {}

    public static final String METHOD = "scoreTest";

    /** One column x transform statistic. */
    public record Stats(double s, double h, double beta, double chi2, double z, double estGain, double pValue, long nObs, boolean degenerate) {
        static Stats degenerate(final long nObs) {
            return new Stats(0d, 0d, Double.NaN, 0d, 0d, 0d, 1d, nObs, true);
        }
    }

    /** The partial test of a column: its statistics after orthogonalisation and the redundancy r²_F. */
    public record Partial(Stats stats, double r2) {}

    /** Result of {@link #build}: the scoring records and the summary, as output-schema maps. */
    public record Result(List<Map<String, Object>> records, Map<String, Object> summary) {}

    /**
     * Relative floor of Σ x̃² for the row families, whose centring is a difference of moment sums
     * (c3 − c4² / c5): below this ratio of c3 the difference holds fewer than four significant digits, so a
     * window-constant column — or one whose spread is below 1e-6 of its magnitude — is degenerate. The grouped
     * family needs no floor: its centring is exact per unit ({@link GroupScorer#pivot}).
     */
    static final double ROW_DEGENERATE_REL = 1e-12;

    /**
     * Score-test statistics from one accumulator slot array.
     *
     * @param nUnits the number of scored units (groups, or rows when independent): {@code est_gain = chi2 / (2 nUnits)}
     */
    public static Stats stats(final ScreenSpec spec, final double[] a, final double nUnits) {
        final long nObs = (long) a[ScoreAccumulator.N_OBS];
        final double s;
        final double h;
        if (spec.isGroupedMultinomial()) {
            s = a[ScoreAccumulator.S];
            h = a[ScoreAccumulator.H];
        } else {
            final double c1 = a[ScoreAccumulator.C1], c2 = a[ScoreAccumulator.C2], c3 = a[ScoreAccumulator.C3], c4 = a[ScoreAccumulator.C4], c5 = a[ScoreAccumulator.C5], c6 = a[ScoreAccumulator.C6];
            if (!(c5 > 0)) return Stats.degenerate(nObs);
            final double xMean = c4 / c5;
            final double sxx = c3 - c4 * c4 / c5;
            if (sxx <= ROW_DEGENERATE_REL * c3) return Stats.degenerate(nObs);
            final double rMean = c2 / c5;
            if (spec.isGaussian()) {
                // identity link: S = Σ x̃ r / σ², H = Σ x̃² / σ² with σ² the residual (offset) / label (prior) variance
                final double sigma2 = c6 / c5 - rMean * rMean;
                if (!(sigma2 > 0)) return Stats.degenerate(nObs);
                s = (c1 - xMean * c2) / sigma2;
                h = sxx / sigma2;
            } else {
                s = c1 - xMean * c2;
                // offset mode: the Fisher weight is already inside c3..c5; prior mode: raw moments, weight at ȳ
                h = spec.hasBaseline() ? sxx : spec.fisherWeight(rMean) * sxx;
            }
        }
        return fromScore(s, h, nObs, nUnits);
    }

    static Stats fromScore(final double s, final double h, final long nObs, final double nUnits) {
        if (nObs < 2 || !(h > 0) || Double.isNaN(s) || h < 1e-300) return Stats.degenerate(nObs);
        final double chi2 = s * s / h;
        final double z = Math.signum(s) * Math.sqrt(chi2);
        final double estGain = nUnits > 0 ? chi2 / (2 * nUnits) : Double.NaN;
        return new Stats(s, h, s / h, chi2, z, estGain, StatMath.chiSquare1UpperTail(chi2), nObs, false);
    }

    /**
     * Partial test from the sums {@code [s, b, a]} at the fitted p̂ and the fit's (g, G): γ = (G + l2·N·I)⁻¹ a
     * with N the fit's (weighted) unit mass, i.e. the same ridge as the fit's Newton system, S⊥ = s − γ'g,
     * H⊥ = b − 2γ'a + γ'Gγ, r²_F = 1 − H⊥ / b. A column fully explained by F (H⊥ ≈ 0) is degenerate with
     * r²_F = 1. {@code nUnits} is the bookkeeping unit count of the marginal test (the gain's denominator);
     * {@code sigma2} is the gaussian family's residual variance at the fitted model (1 for the other families).
     * <p>
     * {@code gammas} computes γ for every key at once: one Cholesky factorisation of the fit's Gram matrix serves
     * every column x transform (a multi-right-hand-side solve); {@code partial} then applies one column's γ.
     */
    static Map<Integer, double[]> gammas(final Map<Integer, PartialAccumulator> partials, final FitState fit, final double l2) {
        final List<Integer> keys = new ArrayList<>();
        for (final Map.Entry<Integer, PartialAccumulator> e : partials.entrySet()) {
            // the sigma / fit sums are not a column; a column whose sums overflowed stays out of the batched solve
            // (no gamma = degenerate, as the per-column solve reported it) instead of failing every column
            if (e.getKey() >= 0 && solvable(e.getValue().getTotal(), fit.k)) keys.add(e.getKey());
        }
        final Map<Integer, double[]> out = new HashMap<>();
        if (keys.isEmpty()) return out;
        final double[][] rhs = new double[fit.k][keys.size()];
        for (int c = 0; c < keys.size(); c++) {
            final double[] vec = partials.get(keys.get(c)).getTotal();
            for (int i = 0; i < fit.k; i++) rhs[i][c] = vec[2 + i];
        }
        final double[][] solution = MatrixOps.solveGram(fit.bestG, rhs, l2 * fit.nUnits);
        for (int c = 0; c < keys.size(); c++) {
            final double[] gamma = new double[fit.k];
            for (int i = 0; i < fit.k; i++) gamma[i] = solution[i][c];
            out.put(keys.get(c), gamma);
        }
        return out;
    }

    /** Whether a column's sums {@code [s, b, a]} can be orthogonalised: a positive H and finite a. */
    private static boolean solvable(final double[] vec, final int k) {
        if (vec.length < 2 + k || !(vec[1] > 0)) return false;
        for (int i = 2; i < 2 + k; i++) {
            if (!Double.isFinite(vec[i])) return false;
        }
        return true;
    }

    /**
     * The partial test given the column's orthogonalisation coefficients from {@link #gammas}, the one degeneracy
     * gate: a column without γ (no information or non-finite sums, or a fit without an accepted point / residual
     * variance, for which the caller computes no γ at all) is degenerate.
     */
    static Partial partial(final double[] vec, final FitState fit, final double nUnits, final long nObs, final double sigma2, final double[] gamma) {
        final int k = fit.k;
        final double s = vec[0];
        final double b = vec[1];
        if (gamma == null) return new Partial(Stats.degenerate(nObs), Double.NaN);
        final double[] a = Arrays.copyOfRange(vec, 2, 2 + k);
        final double hPerp = b - 2 * MatrixOps.dot(gamma, a) + quadratic(gamma, fit.bestG, k);
        if (hPerp <= 1e-10 * b) return new Partial(Stats.degenerate(nObs), 1d);
        final double r2 = Math.min(1d, Math.max(0d, 1d - hPerp / b));
        return new Partial(perpendicular(s - MatrixOps.dot(gamma, fit.bestGrad), hPerp, b, nUnits, nObs, sigma2), r2);
    }

    /**
     * One period's slice of the partial test with the window's γ: S⊥_p = s_p − γ'g_p, H⊥_p = b_p − 2γ'a_p + γ'G_pγ,
     * where {@code fitVec} is the period's {@code [n, g, G]} ({@link ConditioningScorer#FIT_PERIOD_KEY}); without
     * the Gram the window's γ'Gγ ({@code windowGGg}, computed once per column) is scaled by the period's share of
     * the unit mass. The slices sum to the window's S⊥ / H⊥ (exactly with the Gram), so the period signs decompose
     * the partial statistic.
     */
    static Stats partialPeriod(final double[] vec, final double[] fitVec, final FitState fit, final double nUnits, final long nObs,
                               final double sigma2, final double[] gamma, final double windowGGg) {
        final int k = fit.k;
        if (gamma == null || fitVec == null || fitVec.length < 1 + k) return Stats.degenerate(nObs);
        final double gGg;
        if (fitVec.length >= 1 + k + k * k) {
            double q = 0;
            for (int i = 0; i < k; i++) for (int j = 0; j < k; j++) q += gamma[i] * fitVec[1 + k + i * k + j] * gamma[j];
            gGg = q;
        } else {
            gGg = fit.nUnits > 0 ? fitVec[0] / fit.nUnits * windowGGg : 0d;
        }
        final double[] a = Arrays.copyOfRange(vec, 2, 2 + k);
        final double[] g = Arrays.copyOfRange(fitVec, 1, 1 + k);
        final double b = vec[1];
        return perpendicular(vec[0] - MatrixOps.dot(gamma, g), b - 2 * MatrixOps.dot(gamma, a) + gGg, b, nUnits, nObs, sigma2);
    }

    private static double quadratic(final double[] gamma, final double[][] G, final int k) {
        double q = 0;
        for (int i = 0; i < k; i++) for (int j = 0; j < k; j++) q += gamma[i] * G[i][j] * gamma[j];
        return q;
    }

    /** The score test of x⊥ from S⊥ / H⊥; degenerate when nothing is left of x ({@code hPerp} ≈ 0 relative to {@code b}). */
    private static Stats perpendicular(final double sPerp, final double hPerp, final double b, final double nUnits, final long nObs,
                                       final double sigma2) {
        if (hPerp <= 1e-10 * b) return Stats.degenerate(nObs);
        return fromScore(sPerp / sigma2, hPerp / sigma2, nObs, nUnits);
    }

    /**
     * The binned block test of one column (DSL doc §12.1): the score test of the one-hot block of B bins with the
     * intercept profiled out — χ²(df) with df = active bins − 1, no sign (z is NaN) — and its per-bin score S_b,
     * information H_bb and weight mass n_b for the report.
     */
    public record Block(Stats stats, int df, double[] s, double[] h, double[] n) {
        static Block degenerate(final long nObs, final int nb) {
            return new Block(Stats.degenerate(nObs), 0, new double[nb], new double[nb], new double[nb]);
        }
    }

    static Block binnedStats(final ScreenSpec spec, final double[] extra, final double nUnits, final long nObs) {
        final int nb = spec.binCount();
        if (extra == null) return Block.degenerate(nObs, nb);
        final double[] s = new double[nb];
        final double[] n = new double[nb];
        final double[][] h;
        if (spec.isGroupedMultinomial()) {
            // [S (B), P (B), P P' (B²)]: H = diag(P) − P P'
            if (extra.length < 2 * nb + nb * nb) return Block.degenerate(nObs, nb);
            h = new double[nb][nb];
            for (int b = 0; b < nb; b++) {
                s[b] = extra[b];
                n[b] = extra[nb + b];
                for (int c = 0; c < nb; c++) h[b][c] = (b == c ? extra[nb + b] : 0d) - extra[2 * nb + b * nb + c];
            }
        } else {
            // per bin [Σ w, Σ w r, Σ w v] then the totals [Σ w, Σ w r, Σ w r²]
            if (extra.length < 3 * nb + 3) return Block.degenerate(nObs, nb);
            final double wsum = extra[3 * nb], rsum = extra[3 * nb + 1], rr = extra[3 * nb + 2];
            if (!(wsum > 0)) return Block.degenerate(nObs, nb);
            final double rMean = rsum / wsum;
            final boolean prior = !spec.hasBaseline();
            double sigma2 = 1d;
            if (spec.isGaussian()) {
                sigma2 = rr / wsum - rMean * rMean;
                if (!(sigma2 > 0)) return Block.degenerate(nObs, nb);
            }
            final double priorWeight = prior && !spec.isGaussian() ? spec.fisherWeight(rMean) : 1d;
            h = new double[nb][nb];
            for (int b = 0; b < nb; b++) {
                n[b] = extra[3 * b];
                s[b] = (prior ? extra[3 * b + 1] - rMean * extra[3 * b] : extra[3 * b + 1]) / sigma2;
                h[b][b] = (spec.isGaussian() ? extra[3 * b] : prior ? priorWeight * extra[3 * b] : extra[3 * b + 2]) / sigma2;
            }
        }
        final double[] hd = new double[nb];
        for (int b = 0; b < nb; b++) hd[b] = h[b][b];
        final Stats[] out = new Stats[1];
        final int df = blockChi2(s, h, hd, nUnits, nObs, out);
        return new Block(out[0], df, s, hd, n);
    }

    /**
     * χ² = S' H⁺ S over the active bins (positive diagonal information) with one bin dropped as the reference —
     * the block's rank is one less than its active bins whether the intercept is profiled (row families) or the
     * shares sum to one within the unit (grouped) — solved by Cholesky on the reduced system. Returns df; the
     * statistic goes to {@code out[0]}.
     */
    private static int blockChi2(final double[] s, final double[][] h, final double[] diag, final double nUnits, final long nObs, final Stats[] out) {
        final int nb = s.length;
        double maxDiag = 0;
        for (int b = 0; b < nb; b++) maxDiag = Math.max(maxDiag, diag[b]);
        final List<Integer> active = new ArrayList<>();
        int ref = -1;
        for (int b = 0; b < nb; b++) {
            if (diag[b] > 1e-12 * maxDiag && diag[b] > 1e-300 && Double.isFinite(s[b])) {
                active.add(b);
                if (ref < 0 || diag[b] > diag[ref]) ref = b;
            }
        }
        final int df = active.size() - 1;
        if (df < 1 || nObs < 2) {
            out[0] = Stats.degenerate(nObs);
            return 0;
        }
        final List<Integer> kept = new ArrayList<>(active);
        kept.remove(Integer.valueOf(ref));
        final double[][] hr = new double[df][df];
        final double[] sr = new double[df];
        for (int i = 0; i < df; i++) {
            sr[i] = s[kept.get(i)];
            for (int j = 0; j < df; j++) hr[i][j] = h[kept.get(i)][kept.get(j)];
        }
        double chi2;
        try {
            chi2 = MatrixOps.dot(sr, MatrixOps.solveGram(hr, sr, 0d));
        } catch (final RuntimeException e) {
            chi2 = Double.NaN;
        }
        if (!Double.isFinite(chi2) || chi2 < 0) {
            out[0] = Stats.degenerate(nObs);
            return 0;
        }
        final double estGain = nUnits > 0 ? chi2 / (2 * nUnits) : Double.NaN;
        out[0] = new Stats(Double.NaN, Double.NaN, Double.NaN, chi2, Double.NaN, estGain, StatMath.chiSquareUpperTail(chi2, df), nObs, false);
        return df;
    }

    /**
     * The block's partial test from {@code [s (B), H (B² grouped / B diagonal), A (B × k)]} at the fitted p̂:
     * Γ = (G + l2·N·I)⁻¹ A (a multi-right-hand-side solve), S⊥ = s − Γ'g, H⊥ = H − Γ'A' − A Γ + Γ'GΓ, then χ² = S⊥' H⊥⁺ S⊥
     * over the bins the marginal block found active; r²_F = 1 − tr(H⊥) / tr(H). Gaussian divides by σ².
     */
    /** The block's partial test with its degrees of freedom. */
    public record BlockPartial(Partial partial, int df) {
        static BlockPartial degenerate(final long nObs, final double r2) {
            return new BlockPartial(new Partial(Stats.degenerate(nObs), r2), 0);
        }
    }

    static BlockPartial blockPartial(final ScreenSpec spec, final double[] vec, final Block marginal, final FitState fit, final double nUnits,
                                     final long nObs, final double sigma2, final double l2) {
        final int nb = spec.binCount();
        final int k = fit.k;
        final boolean grouped = spec.isGroupedMultinomial();
        final int hLen = grouped ? nb * nb : nb;
        if (vec == null || vec.length < nb + hLen + nb * k || marginal.df < 1) return BlockPartial.degenerate(nObs, Double.NaN);
        final double[][] a = new double[k][nb];   // A' : k × B, the right-hand sides
        for (int b = 0; b < nb; b++) for (int j = 0; j < k; j++) a[j][b] = vec[nb + hLen + b * k + j];
        for (final double[] row : a) for (final double v : row) if (!Double.isFinite(v)) return BlockPartial.degenerate(nObs, Double.NaN);
        final double[][] gamma;
        try {
            gamma = MatrixOps.solveGram(fit.bestG, a, l2 * fit.nUnits);   // k × B
        } catch (final RuntimeException e) {
            return BlockPartial.degenerate(nObs, Double.NaN);
        }
        final double[] s = new double[nb];
        final double[][] h = new double[nb][nb];
        double trH = 0, trHp = 0;
        for (int b = 0; b < nb; b++) {
            double gg = 0;
            for (int j = 0; j < k; j++) gg += gamma[j][b] * fit.bestGrad[j];
            s[b] = (vec[b] - gg) / sigma2;
            for (int c = 0; c < nb; c++) {
                final double hbc = grouped ? vec[nb + b * nb + c] : (b == c ? vec[nb + b] : 0d);
                double ga = 0, ag = 0, ggg = 0;
                for (int j = 0; j < k; j++) {
                    ga += gamma[j][b] * a[j][c];
                    ag += a[j][b] * gamma[j][c];
                    double gj = 0;
                    for (int l = 0; l < k; l++) gj += fit.bestG[j][l] * gamma[l][c];
                    ggg += gamma[j][b] * gj;
                }
                h[b][c] = (hbc - ga - ag + ggg) / sigma2;
                if (b == c) {
                    trH += hbc / sigma2;
                    trHp += h[b][c];
                }
            }
        }
        // the bins the marginal test kept; a bin F explains fully (no information left) drops out
        final double[] diag = new double[nb];
        for (int b = 0; b < nb; b++) diag[b] = marginal.h[b] > 0 && h[b][b] > 1e-10 * (vec[nb + (grouped ? b * nb + b : b)] / sigma2) ? h[b][b] : 0d;
        final Stats[] out = new Stats[1];
        final int df = blockChi2(s, h, diag, nUnits, nObs, out);
        final double r2 = trH > 0 ? Math.min(1d, Math.max(0d, 1d - trHp / trH)) : Double.NaN;
        if (df < 1) return BlockPartial.degenerate(nObs, trH > 0 && trHp <= 1e-10 * trH ? 1d : r2);
        return new BlockPartial(new Partial(out[0], r2), df);
    }

    public static Result build(final ScreenSpec spec, final Map<Integer, ScoreAccumulator> accumulators) {
        return build(spec, accumulators, null, null);
    }

    /**
     * @param partials the partial-test sums per key (null without conditioning)
     * @param fit      the final fit state (null without conditioning)
     */
    public static Result build(final ScreenSpec spec, final Map<Integer, ScoreAccumulator> accumulators,
                               final Map<Integer, PartialAccumulator> partials, final FitState fit) {
        final ScoreAccumulator book = accumulators.getOrDefault(ScoreAccumulator.BOOKKEEPING_KEY, new ScoreAccumulator());
        final double[] b = book.getTotal();
        final double nUnits = b[ScoreAccumulator.UNITS_SCORED];
        final List<String> names = spec.columnNames();
        final int nTransforms = spec.transforms.size();
        final boolean fitted = spec.hasConditioning() && fit != null && fit.hasBest && partials != null;
        // gaussian: the residual variance at the fitted conditioning model ([Σ w r̂², Σ w] under SIGMA_KEY)
        double sigma2 = 1d;
        if (fitted && spec.isGaussian()) {
            final PartialAccumulator sig = partials.get(ConditioningScorer.SIGMA_KEY);
            sigma2 = sig != null && !sig.isEmpty() && sig.getTotal()[1] > 0 ? sig.getTotal()[0] / sig.getTotal()[1] : Double.NaN;
        }
        // an exact fit (no residual) leaves nothing to divide the partial statistics by: the marginal test decides
        final boolean conditioned = fitted && sigma2 > 0;
        final Map<Integer, double[]> gammas = conditioned ? gammas(partials, fit, spec.conditioningL2) : Map.of();
        // the fitted model's [n, g, G] per period: the partial statistic's decomposition by period
        final PartialAccumulator fitPeriods = conditioned ? partials.get(ConditioningScorer.FIT_PERIOD_KEY) : null;
        final List<String> notes = new ArrayList<>(spec.notes);
        if (spec.hasConditioning() && !fitted) {
            notes.add("conditioning: the fit accepted no point (no scorable unit); partial statistics are null and passed / threshold / qValue follow the marginal test");
        } else if (fitted && !conditioned) {
            notes.add("conditioning: the gaussian residual variance at the fitted model is " + sigma2 + " (an exact fit or no weighted row); partial statistics are null and passed / threshold / qValue follow the marginal test");
        }
        // the leak flag reads the partial z when asked to and the partial test is there; else the marginal z
        final boolean leakOnPartial = spec.leakOnPartial() && conditioned;
        if (spec.leakOnPartial() && !conditioned) {
            notes.add("flags.leakZ.on partial: no partial statistics (see the conditioning note); the leak flag reads the marginal z");
        }
        if (fitPeriods != null && !fitPeriods.isEmpty() && fitPeriods.getTotal().length < 1 + fit.k + fit.k * fit.k) {
            notes.add("conditioning: k = " + fit.k + " exceeds " + ConditioningScorer.PERIOD_GRAM_MAX_K + ", so the per-period partial information is approximate (the window's Gram scaled by the period's unit mass); the per-period partial score and sign are exact");
        }
        // skipped units past the share worth a look: which reason, and the way out of an invalid-baseline skip
        final long skipped = (long) b[ScoreAccumulator.UNITS_SKIPPED];
        final long skippedBaseline = (long) b[ScoreAccumulator.UNITS_SKIPPED_BASELINE];
        final long dropped = (long) b[ScoreAccumulator.ROWS_DROPPED];
        if (Baselines.skipShareNoted(skipped, nUnits)) {
            notes.add(skipped + " of " + (long) (nUnits + skipped) + " units skipped (" + Baselines.percent(skipped, nUnits + skipped)
                    + ": invalid baseline " + skippedBaseline + ", no positive label " + (skipped - skippedBaseline) + ")"
                    + (skippedBaseline > 0 && !spec.baselineDropsRows() ? "; an invalid baseline value (a null, or a 0 / negative one under form inverseShare / rate) skips the whole unit: declare baseline.invalid: dropRow to score its remaining rows" : ""));
        }
        if (dropped > 0) notes.add(dropped + " rows dropped (baseline.invalid: dropRow); their units were scored on the remaining rows, a unit left without a row or a positive label is counted as skipped");

        // statistics per key
        final List<Map<String, Object>> records = new ArrayList<>();
        final List<Stats> effective = new ArrayList<>();
        /** the effective test's {@code [periods_agree, n_periods]} per record (pass.minPeriodsAgree) */
        final List<long[]> effectiveAgree = new ArrayList<>();
        /** the placebo columns' effective gains per statistic kind (df1 / binned): one threshold per kind */
        final Map<String, List<Double>> placeboGains = new LinkedHashMap<>();
        for (int c = 0; c < names.size(); c++) {
            for (int t = 0; t < nTransforms; t++) {
                final int key = spec.key(c, t);
                final ScoreAccumulator acc = accumulators.getOrDefault(key, new ScoreAccumulator());
                if (ScreenSpec.isBinned(spec.transforms.get(t))) {
                    // the binned block test: χ²(df) without a sign, no period slices, its own placebo kind
                    final long nObs = (long) acc.getTotal()[ScoreAccumulator.N_OBS];
                    final Block block = binnedStats(spec, acc.getExtra(), nUnits, nObs);
                    final Stats st = block.stats;
                    final Map<String, Object> r = new LinkedHashMap<>();
                    r.put("candidate", names.get(c));
                    r.put("transform", spec.transforms.get(t));
                    r.put("method", METHOD);
                    r.put("family", spec.family);
                    r.put("S", null);
                    r.put("H", null);
                    r.put("beta", null);
                    r.put("chi2", st.chi2);
                    r.put("z", null);
                    r.put("est_gain", st.estGain);
                    r.put("df", (long) block.df);
                    r.put("pValue", st.pValue);
                    r.put("qValue", null);
                    r.put("n_groups", (long) nUnits);
                    r.put("n_obs", st.nObs);
                    r.put("periods_agree", null);
                    r.put("n_periods", null);
                    r.put("period_z", null);
                    final List<Map<String, Object>> binStats = new ArrayList<>();
                    for (int bi = 0; bi < spec.binCount(); bi++) {
                        final Map<String, Object> bs = new LinkedHashMap<>();
                        bs.put("bin", (long) bi);
                        bs.put("S", block.s[bi]);
                        bs.put("H", block.h[bi]);
                        bs.put("n", block.n[bi]);
                        binStats.add(bs);
                    }
                    r.put("bin_stats", binStats);
                    Stats used = st;
                    if (conditioned) {
                        final PartialAccumulator pacc = partials.get(key);
                        final double[] vec = pacc == null || pacc.isEmpty() ? null : pacc.getTotal();
                        final BlockPartial bp = vec == null || st.degenerate
                                ? BlockPartial.degenerate(st.nObs, Double.NaN)
                                : blockPartial(spec, vec, block, fit, nUnits, st.nObs, sigma2, spec.conditioningL2);
                        final Stats pst = bp.partial.stats;
                        r.put("r2_F", Double.isNaN(bp.partial.r2) ? null : bp.partial.r2);
                        r.put("partial_S", null);
                        r.put("partial_H", null);
                        r.put("partial_chi2", pst.chi2);
                        r.put("partial_z", null);
                        r.put("partial_gain", pst.estGain);
                        r.put("partial_pValue", pst.pValue);
                        r.put("partial_df", (long) bp.df);
                        r.put("partial_periods_agree", null);
                        r.put("partial_n_periods", null);
                        r.put("partial_period_z", null);
                        used = pst;
                    } else {
                        for (final String f : List.of("r2_F", "partial_S", "partial_H", "partial_chi2", "partial_z", "partial_gain", "partial_pValue", "partial_df", "partial_periods_agree", "partial_n_periods", "partial_period_z")) r.put(f, null);
                    }
                    effective.add(used);
                    effectiveAgree.add(null);
                    if (spec.isPlacebo(c)) placeboGains.computeIfAbsent(ScreenSpec.KIND_BINNED, kind -> new ArrayList<>()).add(used.degenerate ? 0d : used.estGain);
                    r.put("placebo", spec.isPlacebo(c));
                    r.put("degenerate", st.degenerate);
                    records.add(r);
                    continue;
                }
                final Stats st = stats(spec, acc.getTotal(), nUnits);
                final Map<String, Object> r = new LinkedHashMap<>();
                r.put("candidate", names.get(c));
                r.put("transform", spec.transforms.get(t));
                r.put("method", METHOD);
                r.put("family", spec.family);
                r.put("S", st.s);
                r.put("H", st.h);
                r.put("beta", st.degenerate ? null : st.beta);
                r.put("chi2", st.chi2);
                r.put("z", st.z);
                r.put("est_gain", st.estGain);
                r.put("df", 1L);
                r.put("pValue", st.pValue);
                r.put("qValue", null);
                r.put("n_groups", (long) nUnits);
                r.put("n_obs", st.nObs);
                // periods
                final List<Map<String, Object>> periodRecords = new ArrayList<>();
                final Set<String> scorablePeriods = new HashSet<>();
                long agree = 0, nPeriods = 0;
                for (final Map.Entry<String, double[]> e : acc.getPeriods().entrySet()) {
                    final Stats ps = stats(spec, e.getValue(), nUnits);
                    final Map<String, Object> pr = new LinkedHashMap<>();
                    pr.put("period", e.getKey());
                    pr.put("z", ps.degenerate ? null : ps.z);
                    pr.put("S", ps.s);
                    pr.put("H", ps.h);
                    pr.put("n", ps.nObs);
                    periodRecords.add(pr);
                    if (!ps.degenerate) {
                        scorablePeriods.add(e.getKey());
                        nPeriods++;
                        if (!st.degenerate && st.z != 0 && Math.signum(ps.z) == Math.signum(st.z)) agree++;
                    }
                }
                r.put("periods_agree", agree);
                r.put("n_periods", nPeriods);
                r.put("period_z", periodRecords);
                r.put("bin_stats", null);
                // partial test
                Stats used = st;
                long usedAgree = agree, usedPeriods = nPeriods;
                if (conditioned) {
                    final PartialAccumulator pacc = partials.get(key);
                    final double[] vec = pacc == null || pacc.isEmpty() ? null : pacc.getTotal();
                    // a column the marginal test cannot score has no partial either: its sums are the same
                    // rounding noise, and the effective test must not pass what is reported degenerate
                    final double[] gamma = gammas.get(key);
                    final Partial partial = vec == null || st.degenerate
                            ? new Partial(Stats.degenerate(st.nObs), Double.NaN)
                            : partial(vec, fit, nUnits, st.nObs, sigma2, gamma);
                    final Stats pst = partial.stats;
                    r.put("r2_F", Double.isNaN(partial.r2) ? null : partial.r2);
                    r.put("partial_S", pst.s);
                    r.put("partial_H", pst.h);
                    r.put("partial_chi2", pst.chi2);
                    r.put("partial_z", pst.z);
                    r.put("partial_gain", pst.estGain);
                    r.put("partial_pValue", pst.pValue);
                    r.put("partial_df", null);
                    // the partial statistic by period, with the window's γ: the sign agreement of the effective test
                    final List<Map<String, Object>> partialPeriods = new ArrayList<>();
                    long pAgree = 0, pPeriods = 0;
                    if (vec != null && !st.degenerate && fitPeriods != null) {
                        // without the per-period Gram every slice scales the window's γ'Gγ: computed once per column
                        final double windowGGg = gamma != null && fitPeriods.getTotal().length < 1 + fit.k + fit.k * fit.k
                                ? quadratic(gamma, fit.bestG, fit.k) : 0d;
                        for (final Map.Entry<String, double[]> e : pacc.getPeriods().entrySet()) {
                            final double[] marginalSlot = acc.getPeriods().get(e.getKey());
                            final long pObs = marginalSlot == null ? 0 : (long) marginalSlot[ScoreAccumulator.N_OBS];
                            // the window rule per period: a period the marginal test cannot score (no observed or
                            // within-unit variation of x) has no partial slice either — its S⊥_p / H⊥_p would be the
                            // fit's own −γ'g_p / γ'G_pγ, the conditioning model's period misfit rather than the candidate
                            final Stats ps = scorablePeriods.contains(e.getKey())
                                    ? partialPeriod(e.getValue(), fitPeriods.getPeriods().get(e.getKey()), fit, nUnits, pObs, sigma2, gamma, windowGGg)
                                    : Stats.degenerate(pObs);
                            final Map<String, Object> pr = new LinkedHashMap<>();
                            pr.put("period", e.getKey());
                            pr.put("z", ps.degenerate ? null : ps.z);
                            pr.put("S", ps.s);
                            pr.put("H", ps.h);
                            pr.put("n", pObs);
                            partialPeriods.add(pr);
                            if (!ps.degenerate) {
                                pPeriods++;
                                if (!pst.degenerate && pst.z != 0 && Math.signum(ps.z) == Math.signum(pst.z)) pAgree++;
                            }
                        }
                    }
                    r.put("partial_periods_agree", pAgree);
                    r.put("partial_n_periods", pPeriods);
                    r.put("partial_period_z", partialPeriods);
                    used = pst;
                    usedAgree = pAgree;
                    usedPeriods = pPeriods;
                } else {
                    r.put("r2_F", null);
                    r.put("partial_S", null);
                    r.put("partial_H", null);
                    r.put("partial_chi2", null);
                    r.put("partial_z", null);
                    r.put("partial_gain", null);
                    r.put("partial_pValue", null);
                    r.put("partial_df", null);
                    r.put("partial_periods_agree", null);
                    r.put("partial_n_periods", null);
                    r.put("partial_period_z", null);
                }
                effective.add(used);
                effectiveAgree.add(new long[]{usedAgree, usedPeriods});
                if (spec.isPlacebo(c)) placeboGains.computeIfAbsent(ScreenSpec.KIND_DF1, kind -> new ArrayList<>()).add(used.degenerate ? 0d : used.estGain);
                r.put("placebo", spec.isPlacebo(c));
                r.put("degenerate", st.degenerate);
                records.add(r);
            }
        }

        // placebo threshold per statistic kind (the theoretical chi2(df) quantile when no placebo column is
        // configured): the df = 1 transforms pool one cut, the binned block test its own
        final Map<String, Double> thresholds = new LinkedHashMap<>();
        final Map<String, Double> thresholdsTheoretical = new LinkedHashMap<>();
        for (final String kind : spec.hasBinned() ? List.of(ScreenSpec.KIND_DF1, ScreenSpec.KIND_BINNED) : List.of(ScreenSpec.KIND_DF1)) {
            final int df = ScreenSpec.KIND_BINNED.equals(kind) ? spec.binsK - 1 : 1;
            final double theoretical = nUnits > 0 ? StatMath.chiSquareQuantile(spec.quantile, df) / (2 * nUnits) : Double.NaN;
            final List<Double> gains = placeboGains.getOrDefault(kind, List.of());
            final double cut = gains.isEmpty() ? theoretical : StatMath.quantile(gains.stream().mapToDouble(Double::doubleValue).sorted().toArray(), spec.quantile);
            thresholds.put(kind, cut);
            thresholdsTheoretical.put(kind, theoretical);
        }
        final double thresholdTheoretical = thresholdsTheoretical.get(ScreenSpec.KIND_DF1);
        final double threshold = thresholds.get(ScreenSpec.KIND_DF1);

        // q-values over the candidate records (of the effective test: partial when conditioned)
        final List<Integer> candidateRecords = new ArrayList<>();
        for (int i = 0; i < records.size(); i++) if (!(Boolean) records.get(i).get("placebo")) candidateRecords.add(i);
        final double[] p = new double[candidateRecords.size()];
        for (int i = 0; i < p.length; i++) p[i] = effective.get(candidateRecords.get(i)).pValue;
        final double[] q = StatMath.benjaminiHochberg(p);
        for (int i = 0; i < p.length; i++) records.get(candidateRecords.get(i)).put("qValue", q[i]);

        // flags: the gain cut is the record's kind's placebo threshold lifted to pass.minGain (the practical
        // floor) when higher; the period rule applies to the df = 1 tests (a block has no sign to agree on)
        long nPassed = 0, nLeak = 0;
        final Map<String, Double> passedBest = new HashMap<>();
        for (int i = 0; i < records.size(); i++) {
            final Map<String, Object> r = records.get(i);
            final Stats st = effective.get(i);
            final boolean placebo = (Boolean) r.get("placebo");
            final long[] agreement = effectiveAgree.get(i);
            final double kindThreshold = thresholds.get(ScreenSpec.kind((String) r.get("transform")));
            final double gainCut = spec.gainCut(kindThreshold);
            final boolean passed = !placebo && !st.degenerate && !Double.isNaN(gainCut) && st.estGain > gainCut
                    && (agreement == null || spec.periodsAgree(agreement[0], agreement[1]));
            // st is the effective test: the partial statistics whenever leakOnPartial (which implies conditioned);
            // a block test has no z, so the leak flag does not read it
            final Double marginalZ = (Double) r.get("z");
            final double flagZ = marginalZ == null ? Double.NaN : leakOnPartial ? st.z() : marginalZ;
            final boolean leak = spec.leakZ != null && Math.abs(flagZ) > spec.leakZ;
            r.put("threshold", kindThreshold);
            r.put("passed", passed);
            r.put("leakSuspect", leak);
            if (passed) {
                nPassed++;
                passedBest.merge((String) r.get("candidate"), st.estGain, Math::max);
            }
            if (leak && !placebo) nLeak++;
        }
        final List<String> passedColumns = new ArrayList<>(passedBest.keySet());
        passedColumns.sort(Comparator.<String, Double>comparing(passedBest::get, Comparator.reverseOrder()).thenComparing(Comparator.<String>naturalOrder()));

        final Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("family", spec.family);
        summary.put("method", METHOD);
        summary.put("group", spec.group);
        summary.put("label", spec.labelExpr != null ? spec.labelExpr : spec.labelField);
        summary.put("baseline", spec.baselineField);
        summary.put("baselineForm", spec.hasBaseline() ? spec.baselineForm : null);
        summary.put("weight", spec.weightField);
        summary.put("test", conditioned ? "partial" : "marginal");
        summary.put("passRule", passRule(spec, conditioned));
        summary.put("minPeriodsAgree", spec.minPeriodsAgree);
        summary.put("minGain", spec.minGain);
        summary.put("threshold", threshold);
        summary.put("thresholdTheoretical", thresholdTheoretical);
        // the cut per statistic kind (df1 = the pooled df = 1 transforms; binned = the block test)
        summary.put("thresholds", new LinkedHashMap<>(thresholds));
        summary.put("thresholdsTheoretical", new LinkedHashMap<>(thresholdsTheoretical));
        summary.put("bins", spec.hasBinned() ? spec.binsEdges + "/" + spec.binsK : null);
        summary.put("quantile", spec.quantile);
        summary.put("seed", spec.seed);
        summary.put("nRows", (long) b[ScoreAccumulator.ROWS_IN]);
        summary.put("nRowsTimeFiltered", (long) b[ScoreAccumulator.ROWS_TIME_FILTERED]);
        summary.put("nRowsInvalid", (long) b[ScoreAccumulator.ROWS_INVALID]);
        summary.put("nRowsScored", (long) b[ScoreAccumulator.ROWS_SCORED]);
        summary.put("nUnits", (long) nUnits);
        summary.put("nUnitsSkipped", skipped);
        summary.put("nUnitsSkippedInvalidBaseline", skippedBaseline);
        summary.put("nRowsDropped", dropped);
        summary.put("nCandidates", (long) spec.candidates.size());
        summary.put("nTransforms", (long) nTransforms);
        summary.put("nScored", (long) candidateRecords.size());
        summary.put("nPassed", nPassed);
        summary.put("nPlacebo", placeboGains.values().stream().mapToLong(List::size).sum());
        summary.put("nLeakSuspect", nLeak);
        summary.put("leakOn", spec.leakZ == null ? null : leakOnPartial ? ScreenSpec.LEAK_ON_PARTIAL : ScreenSpec.LEAK_ON_MARGINAL);
        summary.put("timeField", spec.timeField);
        summary.put("timeFrom", spec.timeFrom);
        summary.put("timeTo", spec.timeTo);
        summary.put("minTime", book.getMinTime() == Long.MAX_VALUE ? null : book.getMinTime() * 1000L);
        summary.put("maxTime", book.getMaxTime() == Long.MIN_VALUE ? null : book.getMaxTime() * 1000L);
        summary.put("periodsBucket", spec.periodsBucket);
        summary.put("transforms", new ArrayList<>(spec.transforms));
        summary.put("candidates", new ArrayList<>(spec.candidates));
        summary.put("passedColumns", passedColumns);
        summary.put("conditioningFields", new ArrayList<>(spec.conditioningFields));
        summary.put("conditioningK", fit == null ? null : (long) fit.k);
        summary.put("conditioningIterations", fit == null ? null : (long) fit.iteration);
        summary.put("conditioningRejectedSteps", fit == null ? null : (long) fit.rejected);
        summary.put("conditioningConverged", fit == null ? null : fit.hasBest && fit.converged);
        // gaussian: the fit is least squares at σ² = 1, so the gain is divided by the residual variance at the fit
        // (label-scale free, the units of est_gain / partial_gain)
        final double gain = fit == null ? Double.NaN : conditioned && spec.isGaussian() ? fit.gainPerUnit() / sigma2 : fit.gainPerUnit();
        summary.put("conditioningGain", Double.isNaN(gain) ? null : gain);
        summary.put("conditioningL2", spec.hasConditioning() ? spec.conditioningL2 : null);
        summary.put("conditioningMissing", spec.hasConditioning() ? spec.conditioningMissing : null);
        summary.put("notes", notes);
        return new Result(records, summary);
    }

    /**
     * The pass list written to {@code output.selection}: {@code columns} is what the feature transform's
     * {@code output.include} reads; the rest records how the list was produced (thresholds, the effective test,
     * the upstream manifest identities, this configuration's hash) and the passing records' statistics.
     */
    public static JsonObject selection(final ScreenSpec spec, final Result result) {
        final Map<String, Object> summary = result.summary();
        final JsonObject o = new JsonObject();
        o.addProperty("version", 1);
        final JsonArray columns = new JsonArray();
        for (final Object name : (List<?>) summary.get("passedColumns")) columns.add((String) name);
        o.add("columns", columns);
        o.addProperty("test", (String) summary.get("test"));
        o.addProperty("passRule", (String) summary.get("passRule"));
        o.addProperty("minPeriodsAgree", spec.minPeriodsAgree);
        o.addProperty("minGain", spec.minGain);
        // the z the passing records' leakSuspect was read on (null without a flag)
        o.addProperty("leakZ", spec.leakZ);
        o.addProperty("leakOn", (String) summary.get("leakOn"));
        o.addProperty("family", spec.family);
        o.addProperty("method", METHOD);
        // NaN (no scored unit) is not JSON: written as null
        o.addProperty("threshold", finiteOrNull((Double) summary.get("threshold")));
        o.addProperty("thresholdTheoretical", finiteOrNull((Double) summary.get("thresholdTheoretical")));
        // the cut per statistic kind (the scalar threshold is the df1 one)
        final JsonObject kinds = new JsonObject();
        for (final Map.Entry<?, ?> e : ((Map<?, ?>) summary.get("thresholds")).entrySet()) kinds.addProperty((String) e.getKey(), finiteOrNull((Double) e.getValue()));
        o.add("thresholds", kinds);
        o.addProperty("bins", (String) summary.get("bins"));
        o.addProperty("quantile", spec.quantile);
        o.addProperty("nCandidates", (Long) summary.get("nCandidates"));
        o.addProperty("nPassed", (Long) summary.get("nPassed"));
        o.addProperty("nUnits", (Long) summary.get("nUnits"));
        o.addProperty("timeFrom", spec.timeFrom);
        o.addProperty("timeTo", spec.timeTo);
        o.addProperty("screenHash", spec.parametersHash);
        o.addProperty("planHash", spec.manifestPlanHash);
        o.addProperty("outputHash", spec.manifestOutputHash);
        o.addProperty("manifest", spec.candidateManifest);
        if (spec.hasConditioning()) {
            final JsonArray fields = new JsonArray();
            spec.conditioningFields.forEach(fields::add);
            o.add("conditioningFields", fields);
        }
        o.addProperty("createdAt", Instant.now().toString());
        final JsonArray details = new JsonArray();
        for (final Map<String, Object> r : result.records()) {
            if (!Boolean.TRUE.equals(r.get("passed"))) continue;
            final JsonObject d = new JsonObject();
            d.addProperty("candidate", (String) r.get("candidate"));
            d.addProperty("transform", (String) r.get("transform"));
            d.addProperty("est_gain", (Double) r.get("est_gain"));
            d.addProperty("z", (Double) r.get("z"));
            if (r.get("partial_gain") != null) {
                d.addProperty("partial_gain", (Double) r.get("partial_gain"));
                d.addProperty("partial_z", (Double) r.get("partial_z"));
                d.addProperty("r2_F", (Double) r.get("r2_F"));
            }
            if (spec.periodsBucket != null) {
                final boolean partial = r.get("partial_gain") != null;
                d.addProperty("periods_agree", (Long) r.get(partial ? "partial_periods_agree" : "periods_agree"));
                d.addProperty("n_periods", (Long) r.get(partial ? "partial_n_periods" : "n_periods"));
            }
            d.addProperty("leakSuspect", (Boolean) r.get("leakSuspect"));
            details.add(d);
        }
        o.add("passed", details);
        return o;
    }

    /**
     * The rule behind {@code passed} as applied: the effective test's gain against the threshold (lifted to
     * {@code pass.minGain} when declared), then the period agreement.
     */
    static String passRule(final ScreenSpec spec, final boolean conditioned) {
        final String gain = (conditioned ? "partial_gain" : "est_gain") + " > " + (spec.minGain == null ? "threshold" : "max(threshold, " + spec.minGain + ")");
        if (spec.minPeriodsAgree == null) return gain;
        final String agree = conditioned ? "partial_periods_agree" : "periods_agree";
        final String periods = conditioned ? "partial_n_periods" : "n_periods";
        return gain + " and " + agree + " >= " + (spec.minPeriodsAgree <= 1 ? spec.minPeriodsAgree + " * " + periods : String.valueOf(spec.minPeriodsAgree.longValue()));
    }

    private static Double finiteOrNull(final Double v) {
        return v == null || Double.isNaN(v) || Double.isInfinite(v) ? null : v;
    }

    public static Schema recordSchema() {
        final Schema period = Schema.builder()
                .withField("period", Schema.FieldType.STRING)
                .withField("z", Schema.FieldType.FLOAT64)
                .withField("S", Schema.FieldType.FLOAT64)
                .withField("H", Schema.FieldType.FLOAT64)
                .withField("n", Schema.FieldType.INT64)
                .build();
        final Schema bin = Schema.builder()
                .withField("bin", Schema.FieldType.INT64)
                .withField("S", Schema.FieldType.FLOAT64)
                .withField("H", Schema.FieldType.FLOAT64)
                .withField("n", Schema.FieldType.FLOAT64)
                .build();
        return Schema.builder()
                .withField("candidate", Schema.FieldType.STRING)
                .withField("transform", Schema.FieldType.STRING)
                .withField("method", Schema.FieldType.STRING)
                .withField("family", Schema.FieldType.STRING)
                .withField("S", Schema.FieldType.FLOAT64)
                .withField("H", Schema.FieldType.FLOAT64)
                .withField("beta", Schema.FieldType.FLOAT64)
                .withField("chi2", Schema.FieldType.FLOAT64)
                .withField("z", Schema.FieldType.FLOAT64)
                .withField("est_gain", Schema.FieldType.FLOAT64)
                .withField("df", Schema.FieldType.INT64)
                .withField("pValue", Schema.FieldType.FLOAT64)
                .withField("qValue", Schema.FieldType.FLOAT64)
                .withField("n_groups", Schema.FieldType.INT64)
                .withField("n_obs", Schema.FieldType.INT64)
                .withField("periods_agree", Schema.FieldType.INT64)
                .withField("n_periods", Schema.FieldType.INT64)
                .withField("period_z", Schema.FieldType.array(Schema.FieldType.element(period)))
                .withField("bin_stats", Schema.FieldType.array(Schema.FieldType.element(bin)))
                .withField("r2_F", Schema.FieldType.FLOAT64)
                .withField("partial_S", Schema.FieldType.FLOAT64)
                .withField("partial_H", Schema.FieldType.FLOAT64)
                .withField("partial_chi2", Schema.FieldType.FLOAT64)
                .withField("partial_z", Schema.FieldType.FLOAT64)
                .withField("partial_gain", Schema.FieldType.FLOAT64)
                .withField("partial_pValue", Schema.FieldType.FLOAT64)
                .withField("partial_df", Schema.FieldType.INT64)
                .withField("partial_periods_agree", Schema.FieldType.INT64)
                .withField("partial_n_periods", Schema.FieldType.INT64)
                .withField("partial_period_z", Schema.FieldType.array(Schema.FieldType.element(period)))
                .withField("threshold", Schema.FieldType.FLOAT64)
                .withField("passed", Schema.FieldType.BOOLEAN)
                .withField("leakSuspect", Schema.FieldType.BOOLEAN)
                .withField("placebo", Schema.FieldType.BOOLEAN)
                .withField("degenerate", Schema.FieldType.BOOLEAN)
                .build();
    }

    public static Schema summarySchema() {
        return Schema.builder()
                .withField("family", Schema.FieldType.STRING)
                .withField("method", Schema.FieldType.STRING)
                .withField("group", Schema.FieldType.STRING)
                .withField("label", Schema.FieldType.STRING)
                .withField("baseline", Schema.FieldType.STRING)
                .withField("baselineForm", Schema.FieldType.STRING)
                .withField("weight", Schema.FieldType.STRING)
                .withField("test", Schema.FieldType.STRING)
                .withField("passRule", Schema.FieldType.STRING)
                .withField("minPeriodsAgree", Schema.FieldType.FLOAT64)
                .withField("minGain", Schema.FieldType.FLOAT64)
                .withField("threshold", Schema.FieldType.FLOAT64)
                .withField("thresholdTheoretical", Schema.FieldType.FLOAT64)
                .withField("thresholds", Schema.FieldType.map(Schema.FieldType.FLOAT64))
                .withField("thresholdsTheoretical", Schema.FieldType.map(Schema.FieldType.FLOAT64))
                .withField("bins", Schema.FieldType.STRING)
                .withField("quantile", Schema.FieldType.FLOAT64)
                .withField("seed", Schema.FieldType.INT64)
                .withField("nRows", Schema.FieldType.INT64)
                .withField("nRowsTimeFiltered", Schema.FieldType.INT64)
                .withField("nRowsInvalid", Schema.FieldType.INT64)
                .withField("nRowsScored", Schema.FieldType.INT64)
                .withField("nUnits", Schema.FieldType.INT64)
                .withField("nUnitsSkipped", Schema.FieldType.INT64)
                .withField("nUnitsSkippedInvalidBaseline", Schema.FieldType.INT64)
                .withField("nRowsDropped", Schema.FieldType.INT64)
                .withField("nCandidates", Schema.FieldType.INT64)
                .withField("nTransforms", Schema.FieldType.INT64)
                .withField("nScored", Schema.FieldType.INT64)
                .withField("nPassed", Schema.FieldType.INT64)
                .withField("nPlacebo", Schema.FieldType.INT64)
                .withField("nLeakSuspect", Schema.FieldType.INT64)
                .withField("leakOn", Schema.FieldType.STRING)
                .withField("timeField", Schema.FieldType.STRING)
                .withField("timeFrom", Schema.FieldType.STRING)
                .withField("timeTo", Schema.FieldType.STRING)
                .withField("minTime", Schema.FieldType.TIMESTAMP)
                .withField("maxTime", Schema.FieldType.TIMESTAMP)
                .withField("periodsBucket", Schema.FieldType.STRING)
                .withField("transforms", Schema.FieldType.array(Schema.FieldType.STRING))
                .withField("candidates", Schema.FieldType.array(Schema.FieldType.STRING))
                .withField("passedColumns", Schema.FieldType.array(Schema.FieldType.STRING))
                .withField("conditioningFields", Schema.FieldType.array(Schema.FieldType.STRING))
                .withField("conditioningK", Schema.FieldType.INT64)
                .withField("conditioningIterations", Schema.FieldType.INT64)
                .withField("conditioningRejectedSteps", Schema.FieldType.INT64)
                .withField("conditioningConverged", Schema.FieldType.BOOLEAN)
                .withField("conditioningGain", Schema.FieldType.FLOAT64)
                .withField("conditioningL2", Schema.FieldType.FLOAT64)
                .withField("conditioningMissing", Schema.FieldType.STRING)
                .withField("notes", Schema.FieldType.array(Schema.FieldType.STRING))
                .build();
    }

    /** One-paragraph description of the resolved spec for the assembly log. */
    public static String describe(final ScreenSpec spec) {
        final Set<String> parts = new LinkedHashSet<>();
        parts.add("family=" + spec.family);
        if (spec.group != null) parts.add("group=" + spec.group);
        parts.add("label=" + (spec.labelExpr != null ? "expr(" + spec.labelExpr + ")" : spec.labelField));
        parts.add("baseline=" + (spec.hasBaseline() ? spec.baselineField + ":" + spec.baselineForm + (spec.baselineDropsRows() ? "[dropRow]" : "") : "prior"));
        if (spec.timeField != null) parts.add("time=" + spec.timeField + (spec.timeFrom != null ? " from " + spec.timeFrom : "") + (spec.timeTo != null ? " to " + spec.timeTo : ""));
        if (spec.weightField != null) parts.add("weight=" + spec.weightField);
        parts.add("candidates=" + spec.candidates.size() + " " + spec.candidates);
        parts.add("transforms=" + spec.transforms + (spec.hasBinned() ? " bins=" + spec.binsEdges + "/" + spec.binsK : ""));
        parts.add("placebo=noise:" + spec.noise + (spec.hasShuffle() ? " shuffle:" + spec.shuffleN + "(" + spec.shuffleField + ")" : "") + " q" + spec.quantile + " seed=" + spec.seed);
        if (spec.periodsBucket != null) parts.add("periods=" + spec.periodsField + "/" + spec.periodsBucket);
        if (spec.minPeriodsAgree != null || spec.minGain != null) parts.add("pass=" + passRule(spec, spec.hasConditioning()));
        if (spec.leakZ != null) parts.add("leakZ=" + spec.leakZ + (spec.leakOnPartial() ? " on=partial" : ""));
        if (spec.hasConditioning()) parts.add("conditioning=" + spec.conditioningFields.size() + " " + spec.conditioningFields + " l2=" + spec.conditioningL2 + " maxIter=" + spec.conditioningMaxIter + " missing=" + spec.conditioningMissing + " (" + spec.conditioningMaxIter + " + 2 passes)");
        if (!spec.notes.isEmpty()) parts.add("notes=" + spec.notes);
        return "screen " + String.join(" ", parts);
    }
}
