package com.mercari.solution.util.pipeline.screen;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.domain.math.MatrixOps;
import com.mercari.solution.util.pipeline.glm.Baselines;
import com.mercari.solution.util.pipeline.glm.FitState;
import com.mercari.solution.util.pipeline.glm.StatMath;
import com.mercari.solution.util.pipeline.feature.SymmetricEigen;

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
import java.util.function.IntFunction;
import java.util.function.IntToDoubleFunction;

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
    public record Result(List<Map<String, Object>> records, Map<String, Object> summary, List<Map<String, Object>> suggestions) {
        public Result(final List<Map<String, Object>> records, final Map<String, Object> summary) {
            this(records, summary, List.of());
        }
    }

    /**
     * Relative floor of Σ x̃² for the row families, whose centring is a difference of moment sums
     * (c3 − c4² / c5): below this ratio of c3 the difference holds fewer than four significant digits, so a
     * window-constant column — or one whose spread is below 1e-6 of its magnitude — is degenerate. The grouped
     * family needs no floor: its centring is exact per unit ({@link GroupScorer#pivot}).
     */
    static final double ROW_DEGENERATE_REL = 1e-12;

    /**
     * Relative floor of a pair's per-period information b_p (of the window's b) below which the period carries none
     * of the pair (a member missing throughout it: the sums are exactly 0) and its partial slice is degenerate.
     */
    static final double PAIR_PERIOD_REL = 1e-12;

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
        return gammas(partials, fit, l2, Integer.MAX_VALUE);
    }

    /** {@link #gammas} over the keys below {@code keyLimit} (the pair grids' keys after it are not {@code [s, b, a]} columns). */
    static Map<Integer, double[]> gammas(final Map<Integer, PartialAccumulator> partials, final FitState fit, final double l2, final int keyLimit) {
        final List<Integer> keys = new ArrayList<>();
        for (final Map.Entry<Integer, PartialAccumulator> e : partials.entrySet()) {
            // the sigma / fit sums are not a column; a column whose sums overflowed stays out of the batched solve
            // (no gamma = degenerate, as the per-column solve reported it) instead of failing every column
            if (e.getKey() >= 0 && e.getKey() < keyLimit && solvable(e.getValue().getTotal(), fit.k)) keys.add(e.getKey());
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
     * The binned block test of one column (DSL doc §6.1): the score test of the one-hot block of B bins with the
     * intercept profiled out — χ²(df) with df = active bins − 1, no sign (z is NaN) — and its per-bin score S_b,
     * information H_bb and weight mass n_b for the report.
     */
    public record Block(Stats stats, int df, double[] s, double[] h, double[] n, double[][] hFull) {
        static Block degenerate(final long nObs, final int nb) {
            return new Block(Stats.degenerate(nObs), 0, new double[nb], new double[nb], new double[nb], new double[nb][nb]);
        }
    }

    static Block binnedStats(final ScreenSpec spec, final double[] extra, final double nUnits, final long nObs) {
        return binnedStats(spec, spec.binCount(), extra, nUnits, nObs);
    }

    /** {@link #binnedStats(ScreenSpec, double[], double, long)} over a block of {@code nb} cells (a categorical column's levels, DSL doc §6.2). */
    static Block binnedStats(final ScreenSpec spec, final int nb, final double[] extra, final double nUnits, final long nObs) {
        if (extra == null) return Block.degenerate(nObs, nb);
        final double[] s = new double[nb];
        final double[] n = new double[nb];
        final double[][] h;
        final double[] hd = new double[nb];
        final Stats[] out = new Stats[1];
        final int df;
        if (spec.isGroupedMultinomial()) {
            // [S (B), P (B), P P' (B²)]: H = diag(P) − P P', already of rank active − 1 (the shares sum to one per unit)
            if (extra.length < 2 * nb + nb * nb) return Block.degenerate(nObs, nb);
            h = new double[nb][nb];
            for (int b = 0; b < nb; b++) {
                s[b] = extra[b];
                n[b] = extra[nb + b];
                for (int c = 0; c < nb; c++) h[b][c] = (b == c ? extra[nb + b] : 0d) - extra[2 * nb + b * nb + c];
            }
            for (int b = 0; b < nb; b++) hd[b] = h[b][b];
            df = blockChi2(s, h, hd, nUnits, nObs, out);
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
            double sumS = 0, sumD = 0;
            for (int b = 0; b < nb; b++) {
                n[b] = extra[3 * b];
                s[b] = (prior ? extra[3 * b + 1] - rMean * extra[3 * b] : extra[3 * b + 1]) / sigma2;
                hd[b] = (spec.isGaussian() ? extra[3 * b] : prior ? priorWeight * extra[3 * b] : extra[3 * b + 2]) / sigma2;
                sumS += s[b];
                sumD += hd[b];
            }
            // the intercept profiled out: H = D − d d' / Σd and S centred by the d-weighted mean, so the reduced system
            // (one reference bin dropped) gives χ² = Σ S_b² / H_b − (Σ S_b)² / Σ H_b — the diagonal D alone is full
            // rank, and dropping a bin from it would lose that bin's term instead of the intercept direction
            if (!(sumD > 0)) return Block.degenerate(nObs, nb);
            final double mean = sumS / sumD;
            final double[] sp = new double[nb];
            h = new double[nb][nb];
            for (int b = 0; b < nb; b++) {
                sp[b] = s[b] - hd[b] * mean;
                for (int c = 0; c < nb; c++) h[b][c] = (b == c ? hd[b] : 0d) - hd[b] * hd[c] / sumD;
            }
            df = blockChi2(sp, h, hd, nUnits, nObs, out);
        }
        return new Block(out[0], df, s, hd, n, h);
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

    /** The block's partial test with its degrees of freedom. */
    public record BlockPartial(Partial partial, int df) {
        static BlockPartial degenerate(final long nObs, final double r2) {
            return new BlockPartial(new Partial(Stats.degenerate(nObs), r2), 0);
        }
    }

    /**
     * The block's partial test from {@code [s (B), H (B² grouped / B diagonal), A (B × k)]} at the fitted p̂:
     * Γ = (G + l2·N·I)⁻¹ A (a multi-right-hand-side solve), S⊥ = s − Γ'g, H⊥ = H − Γ'A' − A Γ + Γ'GΓ, then χ² = S⊥' H⊥⁺ S⊥
     * over the bins the marginal block found active; r²_F = 1 − tr(H⊥) / tr(H). Gaussian divides by σ².
     */
    static BlockPartial blockPartial(final ScreenSpec spec, final double[] vec, final Block marginal, final FitState fit, final double nUnits,
                                     final long nObs, final double sigma2, final double l2) {
        return blockPartial(spec, spec.binCount(), vec, marginal, fit, nUnits, nObs, sigma2, l2);
    }

    /** {@link #blockPartial(ScreenSpec, double[], Block, FitState, double, long, double, double)} over a block of {@code nb} cells. */
    static BlockPartial blockPartial(final ScreenSpec spec, final int nb, final double[] vec, final Block marginal, final FitState fit, final double nUnits,
                                     final long nObs, final double sigma2, final double l2) {
        if (marginal.df < 1) return BlockPartial.degenerate(nObs, Double.NaN);
        final double[][] gamma = blockGamma(spec, nb, vec, fit, l2);
        if (gamma == null) return BlockPartial.degenerate(nObs, Double.NaN);
        final Orthogonal o = orthogonalBlock(spec, nb, vec, gamma, fit.bestGrad, fit.bestG, sigma2);
        final Block block = partialBlock(spec, nb, vec, o, marginal, sigma2, nUnits, nObs);
        final double r2 = o.trH > 0 ? Math.min(1d, Math.max(0d, 1d - o.trHp / o.trH)) : Double.NaN;
        if (block.df < 1) return BlockPartial.degenerate(nObs, o.trH > 0 && o.trHp <= 1e-10 * o.trH ? 1d : r2);
        return new BlockPartial(new Partial(block.stats, r2), block.df);
    }

    /** A block's sums after orthogonalisation against the fitted F: S⊥, the full H⊥, and the traces of H / H⊥ (r²_F). */
    record Orthogonal(double[] s, double[][] h, double trH, double trHp) {
    }

    /**
     * Γ (k × B) of a block's partial sums {@code [s (B), H, A (B × k)]} against the fitted F: (G + l2·N·I)⁻¹ A' (a
     * multi-right-hand-side solve); null when the sums are missing, not finite, or the solve fails.
     */
    static double[][] blockGamma(final ScreenSpec spec, final int nb, final double[] vec, final FitState fit, final double l2) {
        final int k = fit.k;
        final int hLen = spec.isGroupedMultinomial() ? nb * nb : nb;
        if (vec == null || vec.length < nb + hLen + nb * k) return null;
        final double[][] a = new double[k][nb];   // A' : k × B, the right-hand sides
        for (int b = 0; b < nb; b++) for (int j = 0; j < k; j++) a[j][b] = vec[nb + hLen + b * k + j];
        for (final double[] row : a) for (final double v : row) if (!Double.isFinite(v)) return null;
        try {
            return MatrixOps.solveGram(fit.bestG, a, l2 * fit.nUnits);   // k × B
        } catch (final RuntimeException e) {
            return null;
        }
    }

    /**
     * S⊥ = s − Γ'g, H⊥ = H − Γ'A' − AΓ + Γ'GΓ over σ² for a block's sums with the given Γ (k × B) and the fit's gradient
     * g and Gram G over the same rows: the window's sums with the window's (g, G); a half's (the suggestions' discovery /
     * confirmation, DSL doc §9.4) with the window's Γ and the half's own (g, G) — the discovery slice of the fit sums,
     * the confirmation being the window's less it.
     */
    static Orthogonal orthogonalBlock(final ScreenSpec spec, final int nb, final double[] vec, final double[][] gamma, final double[] grad,
                                      final double[][] gram, final double sigma2) {
        final int k = grad.length;
        final boolean grouped = spec.isGroupedMultinomial();
        final int hLen = grouped ? nb * nb : nb;
        // G Γ (k × B) once: Γ'GΓ is then O(B² k) instead of O(B² k²)
        final double[][] gGamma = new double[k][nb];
        for (int j = 0; j < k; j++) {
            for (int l = 0; l < k; l++) {
                final double gjl = gram[j][l];
                if (gjl == 0d) continue;
                for (int c = 0; c < nb; c++) gGamma[j][c] += gjl * gamma[l][c];
            }
        }
        final double[] s = new double[nb];
        final double[][] h = new double[nb][nb];
        double trH = 0, trHp = 0;
        for (int b = 0; b < nb; b++) {
            double gg = 0;
            for (int j = 0; j < k; j++) gg += gamma[j][b] * grad[j];
            s[b] = (vec[b] - gg) / sigma2;
            for (int c = 0; c < nb; c++) {
                final double hbc = grouped ? vec[nb + b * nb + c] : (b == c ? vec[nb + b] : 0d);
                double ga = 0, ag = 0, ggg = 0;
                for (int j = 0; j < k; j++) {
                    ga += gamma[j][b] * vec[nb + hLen + c * k + j];
                    ag += vec[nb + hLen + b * k + j] * gamma[j][c];
                    ggg += gamma[j][b] * gGamma[j][c];
                }
                h[b][c] = (hbc - ga - ag + ggg) / sigma2;
                if (b == c) {
                    trH += hbc / sigma2;
                    trHp += h[b][c];
                }
            }
        }
        return new Orthogonal(s, h, trH, trHp);
    }

    /**
     * The block test over the orthogonalised sums as a {@link Block}: the bins the marginal block found active keep
     * their information (a bin F explains fully drops out), the block χ²(df) by {@link #blockChi2}, the per-bin masses
     * from the marginal block.
     */
    static Block partialBlock(final ScreenSpec spec, final int nb, final double[] vec, final Orthogonal o, final Block marginal,
                              final double sigma2, final double nUnits, final long nObs) {
        final boolean grouped = spec.isGroupedMultinomial();
        final double[] diag = new double[nb];
        for (int b = 0; b < nb; b++) diag[b] = marginal.h[b] > 0 && o.h[b][b] > 1e-10 * (vec[nb + (grouped ? b * nb + b : b)] / sigma2) ? o.h[b][b] : 0d;
        final Stats[] out = new Stats[1];
        final int df = blockChi2(o.s, o.h, diag, nUnits, nObs, out);
        return new Block(out[0], df, o.s, diag, marginal.n, o.h);
    }

    /**
     * The heterogeneity test across a modifier's levels (DSL doc §7.1): from the levels' own score tests (each
     * centred within its level) the total Σ S_l² / H_l (df L) splits into the common effect (Σ S_l)² / Σ H_l (df 1)
     * and the heterogeneity Σ S_l² / H_l − (Σ S_l)² / Σ H_l (df L − 1) — a candidate whose effect differs across
     * the levels, up to a sign flip the marginal test cannot see. Degenerate below two usable levels.
     */
    public record Het(double chi2, int df, double pValue, double gain, int levels, boolean degenerate) {
        static Het degenerate(final int levels) {
            return new Het(0d, 0, 1d, 0d, levels, true);
        }
    }

    static Het heterogeneity(final List<Stats> levels, final double nUnits) {
        double total = 0, s = 0, h = 0;
        int usable = 0;
        for (final Stats l : levels) {
            if (l.degenerate || !(l.h > 0)) continue;
            total += l.s * l.s / l.h;
            s += l.s;
            h += l.h;
            usable++;
        }
        if (usable < 2 || !(h > 0)) return Het.degenerate(usable);
        final double chi2 = Math.max(0d, total - s * s / h);
        if (!Double.isFinite(chi2)) return Het.degenerate(usable);
        final int df = usable - 1;
        return new Het(chi2, df, StatMath.chiSquareUpperTail(chi2, df), nUnits > 0 ? chi2 / (2 * nUnits) : Double.NaN, usable, false);
    }

    /** One slice's entry of {@code period_z} / {@code partial_period_z} / {@code level_z}: its name, z (null when degenerate), S, H, n. */
    private static Map<String, Object> sliceRecord(final String nameField, final String name, final Stats ps, final long n) {
        final Map<String, Object> r = new LinkedHashMap<>();
        r.put(nameField, name);
        r.put("z", ps.degenerate ? null : ps.z);
        r.put("S", ps.s);
        r.put("H", ps.h);
        r.put("n", n);
        return r;
    }

    private static void putHet(final Map<String, Object> r, final String prefix, final Het het) {
        r.put(prefix + "het_chi2", het == null ? null : het.chi2);
        r.put(prefix + "het_df", het == null ? null : (long) het.df);
        r.put(prefix + "het_pValue", het == null ? null : het.pValue);
        r.put(prefix + "het_gain", het == null ? null : het.gain);
        r.put(prefix + "het_levels", het == null ? null : (long) het.levels);
    }

    /**
     * The bins' geometry: a representative value per value bin (the suggestions' shapes), and the k − 1 edges (the
     * suggestions' cuts, the records' {@code bin_edges} and the pass list's block recipes; null for position bins) by
     * column, the pair grids' edges by x column (a pair member's conditioning column, the interaction shapes), and a
     * candidate column's smallest finite value in the window (NaN when unknown: the joint's ratio reading).
     */
    public record Bins(IntFunction<double[]> representatives, IntFunction<double[]> edges, IntFunction<double[]> gridEdges, IntToDoubleFunction minimum,
                       IntFunction<WindowQuantiles.Levels> levels) {
        public Bins(final IntFunction<double[]> representatives, final IntFunction<double[]> edges) {
            this(representatives, edges, i -> null, i -> Double.NaN, i -> null);
        }

        public Bins(final IntFunction<double[]> representatives, final IntFunction<double[]> edges, final IntFunction<double[]> gridEdges) {
            this(representatives, edges, gridEdges, i -> Double.NaN, i -> null);
        }

        public Bins(final IntFunction<double[]> representatives, final IntFunction<double[]> edges, final IntFunction<double[]> gridEdges,
                    final IntToDoubleFunction minimum) {
            this(representatives, edges, gridEdges, minimum, i -> null);
        }
    }

    /**
     * A shape over the bins: its name, the contrast φ_b per value bin, the cut it uses (NaN when none), and for a
     * hinge which side of the cut it rises on ({@code below}: max(0, c − x), else max(0, x − c)).
     */
    private record Shape(String name, double[] phi, double cut, boolean below) {
        Shape(final String name, final double[] phi, final double cut) {
            this(name, phi, cut, false);
        }
    }

    /**
     * The one-candidate derivation suggestions (DSL doc §9.4) from the binned sums: the shapes (linear / log /
     * sqrt / rank / step / hinge / abs) scored by the share of the block's χ² their contrast captures, the best
     * cut (the split gain), the missing bin's own effect and the fill value that matches it, and the isotonic
     * (monotone) fit with its sign consistency. Every choice is made on the discovery half's sums and its gain
     * reported on the confirmation half's (the seeded split in {@link GroupScorer#discovery}); placebo columns
     * go through the same search and give each kind its cut.
     */
    static List<Map<String, Object>> suggestions(final ScreenSpec spec, final Map<Integer, ScoreAccumulator> accumulators,
                                                 final Map<Integer, PartialAccumulator> partials, final FitState fit, final boolean conditioned,
                                                 final double sigma2, final double nUnits, final Bins bins) {
        final List<Map<String, Object>> out = new ArrayList<>();
        if (!spec.suggestionsOn || bins == null) return out;
        final int t = spec.transforms.indexOf(ScreenSpec.TRANSFORM_BINNED);
        if (t < 0) return out;
        final List<String> names = spec.columnNames();
        final int nb = spec.binCount();
        final int k = spec.binsK;
        final Map<String, List<Double>> placeboGains = new LinkedHashMap<>();
        final List<Map<String, Object>> candidates = new ArrayList<>();
        // under conditioning: the fit's gradient and Gram over each half (the discovery slice of the fit sums; the
        // confirmation half is the window's less it; without the Gram — k above PERIOD_GRAM_MAX_K — the window's Gram
        // scaled by the half's unit mass, as the period slices do)
        double[] gDisc = null, gConf = null;
        double[][] gramDisc = null, gramConf = null;
        if (conditioned) {
            final int kf = fit.k;
            final PartialAccumulator fitAcc = partials.get(ConditioningScorer.FIT_PERIOD_KEY);
            final double[] slice = fitAcc == null ? null : fitAcc.getPeriods().get(ConditioningScorer.DISCOVERY_SLICE);
            if (slice != null && slice.length >= 1 + kf) {
                gDisc = Arrays.copyOfRange(slice, 1, 1 + kf);
                gConf = new double[kf];
                for (int j = 0; j < kf; j++) gConf[j] = fit.bestGrad[j] - gDisc[j];
                gramDisc = new double[kf][kf];
                gramConf = new double[kf][kf];
                final boolean gram = slice.length >= 1 + kf + kf * kf;
                final double share = fit.nUnits > 0 ? slice[0] / fit.nUnits : 0.5;
                for (int i = 0; i < kf; i++) {
                    for (int j = 0; j < kf; j++) {
                        gramDisc[i][j] = gram ? slice[1 + kf + i * kf + j] : share * fit.bestG[i][j];
                        gramConf[i][j] = fit.bestG[i][j] - gramDisc[i][j];
                    }
                }
            }
        }
        for (int c = 0; c < names.size(); c++) {
            final ScoreAccumulator acc = accumulators.get(spec.key(c, t));
            final double[] extra = acc == null ? null : acc.getExtra();
            if (extra == null || extra.length % 2 != 0) continue;
            final int len = extra.length / 2;
            final double[] full = Arrays.copyOfRange(extra, 0, len);
            final double[] disc = Arrays.copyOfRange(extra, len, 2 * len);
            final double[] conf = new double[len];
            for (int i = 0; i < len; i++) conf[i] = full[i] - disc[i];
            // the halves' unit counts, in proportion to their weight mass
            final double massFull = mass(spec, full), massDisc = mass(spec, disc);
            if (!(massFull > 0)) continue;
            final double nDisc = nUnits * massDisc / massFull, nConf = nUnits - nDisc;
            final long nObs = (long) acc.getTotal()[ScoreAccumulator.N_OBS];
            Block bd = binnedStats(spec, disc, nDisc, nObs);
            Block bc = binnedStats(spec, conf, nConf, nObs);
            if (bd.stats.degenerate || bc.stats.degenerate) continue;
            // under conditioning the halves are orthogonalised against F with the window's Γ (DSL doc §9.4): the
            // recipes then say what F does not already carry; r²_F is the window block's
            String basis = "marginal";
            double r2 = Double.NaN;
            if (conditioned && gDisc != null) {
                final PartialAccumulator pacc = partials.get(spec.key(c, t));
                final double[] pv = pacc == null || pacc.isEmpty() ? null : pacc.getTotal();
                final int plen = pv == null ? 0 : pv.length / 2;
                final double[][] gamma = pv != null && pv.length % 2 == 0 && plen > 0 ? blockGamma(spec, nb, Arrays.copyOfRange(pv, 0, plen), fit, spec.conditioningL2) : null;
                final Block window = gamma == null ? null : binnedStats(spec, full, nUnits, nObs);
                if (gamma != null && !window.stats.degenerate) {
                    final double[] pfull = Arrays.copyOfRange(pv, 0, plen), pdisc = Arrays.copyOfRange(pv, plen, 2 * plen), pconf = new double[plen];
                    for (int i = 0; i < plen; i++) pconf[i] = pfull[i] - pdisc[i];
                    final Orthogonal ow = orthogonalBlock(spec, nb, pfull, gamma, fit.bestGrad, fit.bestG, sigma2);
                    final Block pd = partialBlock(spec, nb, pdisc, orthogonalBlock(spec, nb, pdisc, gamma, gDisc, gramDisc, sigma2), bd, sigma2, nDisc, nObs);
                    final Block pc = partialBlock(spec, nb, pconf, orthogonalBlock(spec, nb, pconf, gamma, gConf, gramConf, sigma2), bc, sigma2, nConf, nObs);
                    if (!pd.stats.degenerate && !pc.stats.degenerate) {
                        bd = pd;
                        bc = pc;
                        basis = "partial";
                        r2 = ow.trH > 0 ? Math.min(1d, Math.max(0d, 1d - ow.trHp / ow.trH)) : Double.NaN;
                    }
                }
            }
            final int before = candidates.size();
            final double[] x = bins.representatives.apply(c);
            final double[] edges = bins.edges.apply(c);
            final boolean position = ScreenSpec.EDGES_RANK.equals(spec.binsEdges);
            final boolean placebo = spec.isPlacebo(c);
            final String name = names.get(c);
            // the bound over the value bins alone (the missing bin is its own suggestion)
            final double blockDisc = contrastBound(bd, k), blockConf = contrastBound(bc, k);
            if (blockDisc > 0 && blockConf > 0) {
                // shapes: chosen on discovery, reported on confirmation
                final List<Shape> shapes = shapes(x, edges, k, position);
                Shape best = null;
                double bestChi2 = -1;
                Shape bestStep = null;
                double bestStepChi2 = -1;
                for (final Shape sh : shapes) {
                    final double chi2 = contrastChi2(bd, sh.phi, k);
                    if (chi2 > bestChi2) {
                        bestChi2 = chi2;
                        best = sh;
                    }
                    if (sh.name.equals("step") && chi2 > bestStepChi2) {
                        bestStepChi2 = chi2;
                        bestStep = sh;
                    }
                }
                if (best != null) {
                    candidates.add(suggestion(name, placebo, "shape", best.name, best.cut, direction(bd, best.phi, k), Double.NaN, Double.NaN,
                            bestChi2 / blockDisc, bestChi2, bc, best.phi, k, blockConf, nConf, fragment(name, best, position), placeboGains));
                }
                if (bestStep != null) {
                    candidates.add(suggestion(name, placebo, "cut", "step", bestStep.cut, direction(bd, bestStep.phi, k), Double.NaN, Double.NaN,
                            bestStepChi2 / blockDisc, bestStepChi2, bc, bestStep.phi, k, blockConf, nConf,
                            position ? "the rows above rank " + fmt(bestStep.cut) + " within the unit" : rowBinFragment(name, new double[]{bestStep.cut}), placeboGains));
                }
                // monotone: the isotonic fit of the bin effects (H-weighted) in the better direction, over the bins
                // with information (a bin without it would put an arbitrary effect into the fit)
                final boolean[] useD = informative(bd, k);
                final double[] effects = new double[k], weights = new double[k];
                for (int b = 0; b < k; b++) {
                    weights[b] = useD[b] ? bd.h[b] : 0d;
                    effects[b] = useD[b] ? bd.s[b] / bd.h[b] : 0d;
                }
                final double[] up = isotonic(effects, weights, true), down = isotonic(effects, weights, false);
                final double chiUp = contrastChi2(bd, up, k), chiDown = contrastChi2(bd, down, k);
                final boolean increasing = chiUp >= chiDown;
                final double[] phi = increasing ? up : down;
                // adjacent among the bins with information: an empty bin (tied edges) does not break the chain
                int consistent = 0, pairs = 0, previous = -1;
                for (int b = 0; b < k; b++) {
                    if (!(weights[b] > 0)) continue;
                    if (previous >= 0) {
                        pairs++;
                        final double d = effects[b] - effects[previous];
                        if (increasing ? d >= 0 : d <= 0) consistent++;
                    }
                    previous = b;
                }
                if (pairs > 0) {
                    candidates.add(suggestion(name, placebo, "monotone", increasing ? "increasing" : "decreasing", Double.NaN, increasing ? "+" : "-", Double.NaN,
                            (double) consistent / pairs, Math.max(chiUp, chiDown) / blockDisc, Math.max(chiUp, chiDown), bc, phi, k, blockConf, nConf,
                            "a monotone " + (increasing ? "increasing" : "decreasing") + " constraint on " + name, placeboGains));
                }
            }
            // missingness: the missing bin against the rest, and the value bin whose effect matches it (both halves
            // holding the missing bin with information)
            final boolean[] useDAll = informative(bd, nb), useCAll = informative(bc, nb);
            if (useDAll[k] && useCAll[k]) {
                final double[] phiMiss = new double[nb];
                phiMiss[k] = 1;
                final double chiDisc = contrastChi2(bd, phiMiss, nb);
                final double blockAllDisc = contrastBound(bd, nb), blockAllConf = contrastBound(bc, nb);
                final double missEffect = bd.s[k] / bd.h[k];
                double fill = Double.NaN, gap = Double.POSITIVE_INFINITY;
                for (int b = 0; b < k; b++) {
                    if (!useDAll[b] || x == null) continue;
                    final double g = Math.abs(bd.s[b] / bd.h[b] - missEffect);
                    if (g < gap) {
                        gap = g;
                        fill = x[b];
                    }
                }
                // the direction of the missing-vs-rest contrast (not the missing bin's raw effect, which an offset
                // miscalibrated overall would carry)
                final String fillText = Double.isNaN(fill) ? ""
                        : position ? ", or place it at rank " + fmt(fill) + " within the unit" : ", or fill with " + fmt(fill);
                candidates.add(suggestion(name, placebo, "missing", "isnull", Double.NaN, direction(bd, phiMiss, nb), fill, Double.NaN,
                        blockAllDisc > 0 ? chiDisc / blockAllDisc : 0d, chiDisc, bc, phiMiss, nb, blockAllConf, nConf,
                        "{scope: row, expr: \"" + name + " == null ? 1 : 0\"}" + fillText, placeboGains));
            }
            for (int i = before; i < candidates.size(); i++) {
                candidates.get(i).put("basis", basis);
                candidates.get(i).put("r2_F", Double.isNaN(r2) ? null : r2);
            }
        }
        // the cut per kind from the placebo columns' confirmation gains, else the theoretical chi2(1) / 2N of the half
        final Map<String, Double> thresholds = new LinkedHashMap<>();
        for (final Map<String, Object> s : candidates) {
            final String kind = (String) s.get("kind");
            if (thresholds.containsKey(kind)) continue;
            final List<Double> gains = placeboGains.getOrDefault(kind, List.of());
            final double nConf = (Double) s.get("_nConf");
            thresholds.put(kind, gains.isEmpty() ? (nConf > 0 ? StatMath.chiSquare1Quantile(spec.quantile) / (2 * nConf) : Double.NaN)
                    : StatMath.quantile(gains.stream().mapToDouble(Double::doubleValue).sorted().toArray(), spec.quantile));
        }
        for (final Map<String, Object> s : candidates) {
            final double cut = thresholds.get((String) s.get("kind"));
            final double gain = (Double) s.get("confirmation_gain");
            final double nConf = (Double) s.remove("_nConf");
            s.put("threshold", cut);
            // a df = 1 contrast on the confirmation half: the floor reads its excess over 1 / 2N of the half
            s.put("passed", !(Boolean) s.get("placebo") && spec.passesGain(gain, 1, nConf, cut));
            out.add(s);
        }
        return out;
    }

    /** One suggestion record; the confirmation statistics come from the confirmation block along the chosen contrast. */
    private static Map<String, Object> suggestion(final String candidate, final boolean placebo, final String kind, final String name, final double cut,
                                                  final String direction, final double fill, final double consistency, final double share,
                                                  final double chi2, final Block confirmation, final double[] phi, final int over, final double blockConf,
                                                  final double nConf, final String fragment, final Map<String, List<Double>> placeboGains) {
        final double chiConf = contrastChi2(confirmation, phi, over);
        final double gainConf = nConf > 0 ? chiConf / (2 * nConf) : Double.NaN;
        final Map<String, Object> s = new LinkedHashMap<>();
        s.put("candidate", candidate);
        s.put("kind", kind);
        s.put("name", name);
        s.put("cut", Double.isNaN(cut) ? null : cut);
        s.put("direction", direction);
        s.put("fill", Double.isNaN(fill) ? null : fill);
        s.put("consistency", Double.isNaN(consistency) ? null : consistency);
        s.put("share", share);
        s.put("chi2", chi2);
        s.put("confirmation_chi2", chiConf);
        s.put("confirmation_share", blockConf > 0 ? chiConf / blockConf : 0d);
        s.put("confirmation_gain", gainConf);
        s.put("confirmation_pValue", StatMath.chiSquare1UpperTail(chiConf));
        s.put("threshold", null);
        s.put("passed", null);
        s.put("placebo", placebo);
        s.put("fragment", fragment);
        s.put("basis", null);
        s.put("r2_F", null);
        s.put("_nConf", nConf);
        if (placebo) placeboGains.computeIfAbsent(kind, key -> new ArrayList<>()).add(Double.isNaN(gainConf) ? 0d : gainConf);
        return s;
    }

    /** The weight mass of a binned sum vector (the row families' Σ w total, the grouped family's Σ_b P_b). */
    private static double mass(final ScreenSpec spec, final double[] v) {
        final int nb = spec.binCount();
        if (spec.isGroupedMultinomial()) {
            double m = 0;
            for (int b = 0; b < nb; b++) m += v[nb + b];
            return m;
        }
        return v[3 * nb];
    }

    /**
     * The bound the shares are read against: the maximum of {@link #contrastChi2} over every contrast φ on the first
     * {@code over} bins. S and H go through the same H-weighted centring P = I − 1w' (w_b = H_bb / Σ H_bb over the
     * bins with information): S̃ = P'S, H̃ = P'HP, then S̃'H̃⁺S̃ — 1 spans H̃'s null space and S̃ ⊥ 1, so one
     * reference bin is dropped exactly. The block's own χ² is not that bound: a row family's H is diagonal (the
     * intercept is not profiled out of it), and the value bins alone leave the missing bin's coupling out, so a
     * single contrast could exceed it (a share above 1).
     */
    private static double contrastBound(final Block block, final int over) {
        final boolean[] use = informative(block, over);
        double hsum = 0, ssum = 0;
        for (int b = 0; b < over; b++) {
            if (!use[b]) continue;
            hsum += block.h[b];
            ssum += block.s[b];
        }
        if (!(hsum > 0)) return 0d;
        // w: the centring weights; r = H1 and t = 1'H1 over the bins with information
        final double[] w = new double[over], r = new double[over];
        double t = 0;
        for (int b = 0; b < over; b++) {
            if (!use[b]) continue;
            w[b] = block.h[b] / hsum;
            for (int c = 0; c < over; c++) if (use[c]) r[b] += block.hFull[b][c];
            t += r[b];
        }
        final double[] s = new double[over], diag = new double[over];
        final double[][] h = new double[over][over];
        for (int b = 0; b < over; b++) {
            if (!use[b]) continue;
            s[b] = block.s[b] - w[b] * ssum;
            for (int c = 0; c < over; c++) {
                if (use[c]) h[b][c] = block.hFull[b][c] - r[b] * w[c] - w[b] * r[c] + w[b] * w[c] * t;
            }
            diag[b] = h[b][b];
        }
        final Stats[] out = new Stats[1];
        final int df = blockChi2(s, h, diag, 1, 2, out);
        return df < 1 ? 0d : out[0].chi2;
    }

    /**
     * The df = 1 score test along a bin-constant contrast φ over the first {@code over} bins: φ centred by the
     * H-weighted mean (the intercept profiled out), S_φ = φ_c'S, H_φ = φ_c'Hφ_c. Bins without information carry
     * nothing. Bounded by the block's χ² (the block is the maximum over its contrasts).
     */
    /** the share of a block's information below which a bin carries none for a contrast (DSL doc §9.4) */
    static final double CONTRAST_H_FLOOR = 1e-9;

    /**
     * Whether each of the first {@code over} bins of a block carries information a contrast may read: H_b above
     * {@link #CONTRAST_H_FLOOR} of the bins' total and a positive mass. A confirmation half is a difference of sums,
     * so a bin it does not hold keeps a rounding residue of H (and of S) that a contrast isolating the bin would
     * divide by; a lone row at p̂ ≈ 0 (H ≈ 0, |S| ≈ 1) would do the same.
     */
    static boolean[] informative(final Block block, final int over) {
        double hsum = 0;
        for (int b = 0; b < over; b++) if (block.h[b] > 0) hsum += block.h[b];
        final boolean[] use = new boolean[over];
        for (int b = 0; b < over; b++) use[b] = block.h[b] > CONTRAST_H_FLOOR * hsum && block.n[b] > 0;
        return use;
    }

    static double contrastChi2(final Block block, final double[] phi, final int over) {
        final boolean[] use = informative(block, over);
        double hsum = 0, hphi = 0;
        for (int b = 0; b < over; b++) {
            if (!use[b] || !Double.isFinite(phi[b])) continue;
            hsum += block.h[b];
            hphi += block.h[b] * phi[b];
        }
        if (!(hsum > 0)) return 0d;
        final double mean = hphi / hsum;
        final double[] pc = new double[over];
        for (int b = 0; b < over; b++) pc[b] = use[b] && Double.isFinite(phi[b]) ? phi[b] - mean : 0d;
        double s = 0, h = 0;
        for (int b = 0; b < over; b++) {
            s += pc[b] * block.s[b];
            for (int c = 0; c < over; c++) h += pc[b] * block.hFull[b][c] * pc[c];
        }
        if (!(h > 1e-300)) return 0d;
        final double chi2 = s * s / h;
        return Double.isFinite(chi2) ? chi2 : 0d;
    }

    /** The sign of the contrast's score on the discovery block: "+" when the label rises with φ. */
    private static String direction(final Block block, final double[] phi, final int over) {
        final boolean[] use = informative(block, over);
        double hsum = 0, hphi = 0;
        for (int b = 0; b < over; b++) if (use[b]) {
            hsum += block.h[b];
            hphi += block.h[b] * phi[b];
        }
        final double mean = hsum > 0 ? hphi / hsum : 0d;
        double s = 0;
        for (int b = 0; b < over; b++) if (use[b]) s += (phi[b] - mean) * block.s[b];
        return s >= 0 ? "+" : "-";
    }

    /**
     * The sign of the centred contrast's score ({@link #direction}) as ±1: a level's effect against the rest, not the
     * sign of its raw S_l (which carries the window's total residual in offset mode).
     */
    private static double contrastSign(final Block block, final double[] phi, final int over) {
        return "+".equals(direction(block, phi, over)) ? 1d : -1d;
    }

    /** The shapes tried on the value bins: the smooth ones on the representatives, the cut ones at every edge. */
    private static List<Shape> shapes(final double[] x, final double[] edges, final int k, final boolean position) {
        final List<Shape> out = new ArrayList<>();
        final double[] index = new double[k];
        for (int b = 0; b < k; b++) index[b] = b;
        out.add(new Shape("rank", index, Double.NaN));
        if (x != null) {
            // position bins: the representatives are the rank positions, so linear would repeat rank exactly
            if (!position) out.add(new Shape("linear", x.clone(), Double.NaN));
            boolean positive = true, nonNegative = true;
            for (final double v : x) {
                if (!(v > 0)) positive = false;
                if (!(v >= 0)) nonNegative = false;
            }
            if (positive) {
                final double[] lg = new double[k];
                for (int b = 0; b < k; b++) lg[b] = Math.log(x[b]);
                out.add(new Shape("log", lg, Double.NaN));
            }
            if (nonNegative) {
                final double[] sq = new double[k];
                for (int b = 0; b < k; b++) sq[b] = Math.sqrt(x[b]);
                out.add(new Shape("sqrt", sq, Double.NaN));
            }
        }
        for (int j = 0; j < k - 1; j++) {
            final double cut = position ? (double) (j + 1) / k : edges == null ? Double.NaN : edges[j];
            final double[] step = new double[k];
            for (int b = 0; b < k; b++) step[b] = b > j ? 1 : 0;
            out.add(new Shape("step", step, cut));
            if (x == null || Double.isNaN(cut)) continue;
            final double[] up = new double[k], down = new double[k], abs = new double[k];
            for (int b = 0; b < k; b++) {
                up[b] = Math.max(0, x[b] - cut);
                down[b] = Math.max(0, cut - x[b]);
                abs[b] = Math.abs(x[b] - cut);
            }
            out.add(new Shape("hinge", up, cut, false));
            out.add(new Shape("hinge", down, cut, true));
            out.add(new Shape("abs", abs, cut));
        }
        return out;
    }

    /** Pool-adjacent-violators: the weighted isotonic fit of {@code y} (increasing, or decreasing when {@code up} is false). */
    static double[] isotonic(final double[] y, final double[] w, final boolean up) {
        final int n = y.length;
        final double[] value = new double[n], weight = new double[n];
        final int[] size = new int[n];
        int m = 0;
        for (int i = 0; i < n; i++) {
            value[m] = up ? y[i] : -y[i];
            weight[m] = Math.max(w[i], 0);
            size[m] = 1;
            m++;
            while (m > 1 && value[m - 2] > value[m - 1]) {
                final double tw = weight[m - 2] + weight[m - 1];
                value[m - 2] = tw > 0 ? (value[m - 2] * weight[m - 2] + value[m - 1] * weight[m - 1]) / tw : 0.5 * (value[m - 2] + value[m - 1]);
                weight[m - 2] = tw;
                size[m - 2] += size[m - 1];
                m--;
            }
        }
        final double[] out = new double[n];
        int pos = 0;
        for (int b = 0; b < m; b++) for (int i = 0; i < size[b]; i++) out[pos++] = up ? value[b] : -value[b];
        return out;
    }

    private static String fragment(final String name, final Shape shape, final boolean position) {
        final String c = fmt(shape.cut);
        return switch (shape.name) {
            case "linear" -> name + " as is";
            // position bins: the shape is of the within-unit rank, not of the raw value
            case "log" -> position ? "the log of the rank of " + name + " within the unit" : "{scope: row, expr: \"log(" + name + ")\"}";
            case "sqrt" -> position ? "the square root of the rank of " + name + " within the unit" : "{scope: row, expr: \"sqrt(" + name + ")\"}";
            case "rank" -> position ? "the rank of " + name + " within the unit" : "the rank of " + name + " over the window (a quantile transform upstream)";
            case "step" -> position ? "the rows above rank " + c + " within the unit" : "{scope: row, expr: \"" + name + " > " + c + " ? 1 : 0\"}";
            case "hinge" -> position
                    ? "a hinge " + (shape.below ? "below" : "above") + " rank " + c + " of " + name + " within the unit"
                    : "{scope: row, expr: \"" + (shape.below ? "max(0, " + c + " - " + name + ")" : "max(0, " + name + " - " + c + ")") + "\"}";
            case "abs" -> position ? "the distance to rank " + c + " of " + name + " within the unit" : "{scope: row, expr: \"abs(" + name + " - " + c + ")\"}";
            default -> shape.name;
        };
    }

    private static String fmt(final double v) {
        if (Double.isNaN(v)) return "";
        return v == Math.rint(v) && Math.abs(v) < 1e15 ? String.valueOf((long) v) : String.format(java.util.Locale.ROOT, "%.6g", v);
    }

    /**
     * The several-candidate suggestions (DSL doc §9.5) from the candidates' joint sums under {@link ScoreAccumulator#JOINT_KEY}:
     * the score vector S, the m × m Fisher matrix H and the pHd matrix M = Σ w r x̃x̃' over the joint columns (the chosen
     * candidates, then a few noise placebos for the null scale; row families: r with the intercept profiled out as in S,
     * gaussian S / H / M over σ² as in {@link #stats}, a degenerate column dropped). Reported: the principal Hessian
     * directions of H^(−1/2) M H^(−1/2) (residual curvature: quadratic effects and interactions in bulk, the scale-free
     * loadings v_j √H_jj naming the candidates; a diagnostic, never a pass flag), the redundancy clusters (|correlation| in the Fisher metric at or
     * above {@code joint.redundancy}), a report-time forward selection (the score test of a candidate given the
     * selected set, closed form at β = 0, stopped at the df = 1 cut), the linear composite of the selected set, and the
     * differences / ratios: every candidate pair's 2 × 2 Newton direction, equal and opposite on the standardised
     * scale, whose joint χ² exceeds the better single one by {@code joint.excess} with the increment clearing the
     * df = 1 cut (a ratio too when both columns' window minima from {@code bins} are positive).
     * In-sample, one-step: hypotheses for a feature spec, not decisions. Under conditioning the sums are the partial
     * pass's, orthogonalised against F ({@link #partialJoint}); every record says which ({@code basis}).
     */
    static List<Map<String, Object>> joint(final ScreenSpec spec, final Map<Integer, ScoreAccumulator> accumulators,
                                           final Map<Integer, PartialAccumulator> partials, final FitState fit, final boolean conditioned,
                                           final double sigma2, final double nUnits, final double df1Cut, final Bins bins) {
        final List<Map<String, Object>> out = new ArrayList<>();
        if (!spec.jointOn) return out;
        final int m = spec.jointColumnCount();
        final List<String> names = spec.columnNames();
        final int nCand = spec.jointColumns.size();
        // the sums the suggestions read: under conditioning the partial pass's, orthogonalised against F (what F does
        // not already carry); else the marginal ones (what the baseline misses)
        JointSums js = conditioned ? partialJoint(spec, partials, fit, sigma2) : null;
        if (js == null) js = marginalJoint(spec, accumulators);
        if (js == null) return out;
        final double[] s = js.s;
        final double[][] h = js.h, mm = js.mm, hRaw = js.hRaw;
        // overflowing sums leave nothing to decompose (a non-finite matrix may not terminate the eigensolver)
        if (!allFinite(s) || !allFinite(h) || !allFinite(mm)) return out;
        double trace = 0;
        final double[] hd = new double[m];
        for (int j = 0; j < m; j++) {
            hd[j] = h[j][j];
            trace += hd[j];
        }
        if (!(trace > 0)) return out;
        final double ridge = 1e-8 * trace / m;
        // pHd: eigenpairs of H^(-1/2) M H^(-1/2) (H ridged to stay definite), the directions mapped back through H^(-1/2)
        try {
            final double[][] hr = new double[m][];
            for (int j = 0; j < m; j++) {
                hr[j] = h[j].clone();
                hr[j][j] += ridge;
            }
            final SymmetricEigen.Result eh = SymmetricEigen.leading(hr, m, false);
            final double[][] hm = new double[m][m];
            for (int i = 0; i < eh.values().length; i++) {
                if (!(eh.values()[i] > 1e-12 * trace)) continue;
                final double[] u = eh.vectors()[i];
                final double f = 1 / Math.sqrt(eh.values()[i]);
                for (int j = 0; j < m; j++) for (int l = 0; l < m; l++) hm[j][l] += f * u[j] * u[l];
            }
            // B = hm M hm as two m³ products
            final double[][] b = multiply(multiply(hm, mm), hm);
            final SymmetricEigen.Result eb = SymmetricEigen.leading(b, m, true);
            double total = 0;
            for (final double v : eb.values()) total += Math.abs(v);
            final boolean noise = nCand < m;
            for (int k = 0; k < Math.min(spec.jointDirections, eb.values().length); k++) {
                final double[] q = eb.vectors()[k];
                // v: the projection's coefficients in the columns' units (the recipe v'x); z: the scale-free loadings
                // v_j √H_jj, which name the candidates and compare with the noise columns' (the null scale)
                final double[] v = new double[m], z = new double[m];
                double norm = 0, zNorm = 0;
                for (int j = 0; j < m; j++) {
                    for (int l = 0; l < m; l++) v[j] += hm[j][l] * q[l];
                    z[j] = v[j] * Math.sqrt(Math.max(hd[j], 0d));
                    norm += v[j] * v[j];
                    zNorm += z[j] * z[j];
                }
                norm = Math.sqrt(norm);
                zNorm = Math.sqrt(zNorm);
                if (!(norm > 0) || !(zNorm > 0)) continue;
                for (int j = 0; j < m; j++) {
                    v[j] /= norm;
                    z[j] /= zNorm;
                }
                final Integer[] order = new Integer[nCand];
                for (int j = 0; j < nCand; j++) order[j] = j;
                Arrays.sort(order, Comparator.comparingDouble(j -> -Math.abs(z[j])));
                double noiseLoading = noise ? 0d : Double.NaN;
                for (int j = nCand; j < m; j++) noiseLoading = Math.max(noiseLoading, Math.abs(z[j]));
                final StringBuilder fragment = new StringBuilder();
                for (int i = 0; i < Math.min(5, nCand); i++) {
                    final int j = order[i];
                    if (Math.abs(z[j]) < 0.05) break;
                    fragment.append(fragment.length() == 0 ? (v[j] < 0 ? "-" : "") : (v[j] < 0 ? " - " : " + "))
                            .append(fmt(Math.abs(v[j]))).append("*").append(names.get(spec.jointColumn(j)));
                }
                out.add(jointRecord(names.get(spec.jointColumn(order[0])), "phd", "direction" + (k + 1), noiseLoading,
                        total > 0 ? Math.abs(eb.values()[k]) / total : 0d, eb.values()[k], Double.NaN, null, Double.NaN,
                        "the projection v'x and its square, v = " + fragment
                                + (noise ? " (max noise loading " + fmt(noiseLoading) + ")" : " (no noise column: no null scale)")));
            }
        } catch (final RuntimeException ex) {
            // a singular metric or a failed decomposition leaves the pHd diagnostic out
        }
        // redundancy clusters among the candidates: single linkage at |corr| >= joint.redundancy in the Fisher metric
        final int[] parent = new int[nCand];
        for (int j = 0; j < nCand; j++) parent[j] = j;
        final double[][] corr = new double[nCand][nCand];
        for (int j = 0; j < nCand; j++) {
            for (int l = j + 1; l < nCand; l++) {
                corr[j][l] = corr[l][j] = hRaw[j][j] > 0 && hRaw[l][l] > 0 ? hRaw[j][l] / Math.sqrt(hRaw[j][j] * hRaw[l][l]) : 0d;
                if (Math.abs(corr[j][l]) >= spec.jointRedundancy) parent[find(parent, j)] = find(parent, l);
            }
        }
        final Map<Integer, List<Integer>> clusters = new LinkedHashMap<>();
        for (int j = 0; j < nCand; j++) clusters.computeIfAbsent(find(parent, j), k -> new ArrayList<>()).add(j);
        for (final List<Integer> members : clusters.values()) {
            if (members.size() < 2) continue;
            int head = members.get(0);
            double best = -1, minCorr = 1;
            for (final int j : members) {
                final double chi2 = h[j][j] > 0 ? s[j] * s[j] / h[j][j] : 0;
                if (chi2 > best) {
                    best = chi2;
                    head = j;
                }
                for (final int l : members) if (l > j) minCorr = Math.min(minCorr, Math.abs(corr[j][l]));
            }
            final List<String> others = new ArrayList<>();
            for (final int j : members) if (j != head) others.add(names.get(spec.jointColumn(j)));
            out.add(jointRecord(names.get(spec.jointColumn(head)), "redundant", "cluster", Double.NaN, minCorr, best, Double.NaN, null, Double.NaN,
                    "near-duplicates of " + names.get(spec.jointColumn(head)) + ": " + others + " (|corr| >= " + fmt(spec.jointRedundancy) + "); keep one, or average / project them"));
        }
        // forward selection at beta = 0: the score test of a candidate given the selected set, closed form
        final List<Integer> selected = new ArrayList<>();
        final boolean[] taken = new boolean[nCand];
        for (int step = 0; step < spec.jointSelect; step++) {
            // γ_j = H_AA⁻¹ H_Aj for every candidate at once: one factorisation of the selected block per step
            final int a = selected.size();
            final double[] sa = new double[a];
            final double[][] hac = new double[a][nCand];
            double[][] gamma = null;
            if (a > 0) {
                final double[][] haa = new double[a][a];
                for (int i = 0; i < a; i++) {
                    final int si = selected.get(i);
                    sa[i] = s[si];
                    for (int l = 0; l < a; l++) haa[i][l] = h[si][selected.get(l)];
                    for (int j = 0; j < nCand; j++) hac[i][j] = h[si][j];
                }
                gamma = MatrixOps.solveGram(haa, hac, ridge);
            }
            int bestJ = -1;
            double bestChi2 = 0;
            for (int j = 0; j < nCand; j++) {
                if (taken[j] || !(h[j][j] > 0)) continue;
                double sPerp = s[j], hPerp = h[j][j];
                for (int i = 0; i < a; i++) {
                    sPerp -= gamma[i][j] * sa[i];
                    hPerp -= gamma[i][j] * hac[i][j];
                }
                if (!(hPerp > 1e-10 * h[j][j])) continue;
                final double chi2 = sPerp * sPerp / hPerp;
                if (Double.isFinite(chi2) && chi2 > bestChi2) {
                    bestChi2 = chi2;
                    bestJ = j;
                }
            }
            final double gain = nUnits > 0 ? bestChi2 / (2 * nUnits) : Double.NaN;
            if (bestJ < 0 || !spec.passesGain(gain, 1, nUnits, df1Cut)) break;
            final List<String> given = new ArrayList<>();
            for (final int j : selected) given.add(names.get(spec.jointColumn(j)));
            out.add(jointRecord(names.get(spec.jointColumn(bestJ)), "select", "step" + (step + 1), Double.NaN, gain, bestChi2, gain, true, df1Cut,
                    "adds " + fmt(gain) + " given " + given));
            selected.add(bestJ);
            taken[bestJ] = true;
        }
        if (selected.size() >= 2) {
            final int a = selected.size();
            final double[][] haa = new double[a][a];
            final double[] sa = new double[a];
            for (int i = 0; i < a; i++) {
                sa[i] = s[selected.get(i)];
                for (int l = 0; l < a; l++) haa[i][l] = h[selected.get(i)][selected.get(l)];
            }
            try {
                final double[] beta = MatrixOps.solveGram(haa, sa, ridge);
                final double chi2 = MatrixOps.dot(beta, sa);
                final StringBuilder expr = new StringBuilder();
                for (int i = 0; i < a; i++) {
                    expr.append(i == 0 ? (beta[i] < 0 ? "-" : "") : (beta[i] < 0 ? " - " : " + "))
                            .append(fmt(Math.abs(beta[i]))).append("*").append(names.get(spec.jointColumn(selected.get(i))));
                }
                final double gain = nUnits > 0 ? chi2 / (2 * nUnits) : Double.NaN;
                out.add(jointRecord(names.get(spec.jointColumn(selected.get(0))), "composite", "composite", Double.NaN, gain, chi2, gain, null, Double.NaN,
                        "{scope: row, expr: \"" + expr + "\"}"));
            } catch (final RuntimeException ex) {
                // a singular selected block leaves the composite out
            }
        }
        // ratios and differences: a pair whose two-dimensional Newton direction is equal and opposite on the
        // standardised scale, and whose joint chi2 clearly exceeds the better single one — by the excess factor, and
        // the other member's gain given the better one (the increment, a df = 1 score test) clearing the df = 1 cut
        // (DSL doc §9.5)
        final List<PairDirection> pairsFound = new ArrayList<>();
        for (int i = 0; i < nCand && spec.jointPairs > 0 && nUnits > 0; i++) {
            for (int j = i + 1; j < nCand; j++) {
                if (!(h[i][i] > 0) || !(h[j][j] > 0)) continue;
                final double det = h[i][i] * h[j][j] - h[i][j] * h[i][j];
                if (!(det > 1e-12 * h[i][i] * h[j][j])) continue;
                final double bi = (h[j][j] * s[i] - h[i][j] * s[j]) / det, bj = (h[i][i] * s[j] - h[i][j] * s[i]) / det;
                final double chi2 = bi * s[i] + bj * s[j];
                final double single = Math.max(s[i] * s[i] / h[i][i], s[j] * s[j] / h[j][j]);
                if (!(single > 0) || !(chi2 > spec.jointExcess * single) || !spec.passesGain((chi2 - single) / (2 * nUnits), 1, nUnits, df1Cut)) continue;
                // standardised coefficients: opposite signs, comparable magnitudes
                final double si = bi * Math.sqrt(h[i][i]), sj = bj * Math.sqrt(h[j][j]);
                if (si * sj >= 0) continue;
                final double consistency = Math.min(Math.abs(si), Math.abs(sj)) / Math.max(Math.abs(si), Math.abs(sj));
                if (consistency < 0.5) continue;
                pairsFound.add(new PairDirection(i, j, chi2 / single, chi2, bi, bj, consistency));
            }
        }
        pairsFound.sort(Comparator.comparingDouble((PairDirection p) -> -p.excess()));
        for (int n = 0; n < Math.min(spec.jointPairs, pairsFound.size()); n++) {
            final PairDirection p = pairsFound.get(n);
            final String a = names.get(spec.jointColumn(p.i())), b = names.get(spec.jointColumn(p.j()));
            final double gain = p.chi2() / (2 * nUnits);
            // x_i − r x_j with r the raw-scale coefficient ratio; the sign of beta_i decides which is subtracted
            final boolean iPositive = p.bi() > 0;
            final String first = iPositive ? a : b, second = iPositive ? b : a;
            final double r = Math.abs(iPositive ? p.bj() / p.bi() : p.bi() / p.bj());
            out.add(jointRecord(first, "difference", first + " - " + second, p.consistency(), p.excess(), p.chi2(), gain, null, Double.NaN,
                    "{scope: row, expr: \"" + first + " - " + fmt(r) + "*" + second + "\"} (joint chi2 " + fmt(p.chi2()) + ", " + fmt(p.excess()) + "x the better single)"));
            // the ratio reading needs positive columns (the log-scale direction); the sketch minima tell (NaN = unknown)
            final double minI = bins == null ? Double.NaN : bins.minimum().applyAsDouble(spec.jointColumn(p.i()));
            final double minJ = bins == null ? Double.NaN : bins.minimum().applyAsDouble(spec.jointColumn(p.j()));
            if (minI > 0 && minJ > 0) {
                out.add(jointRecord(first, "ratio", first + " / " + second, p.consistency(), p.excess(), p.chi2(), gain, null, Double.NaN,
                        "{scope: row, expr: \"" + first + " / " + second + "\"} (both positive; the difference's log-scale reading, approximate)"));
            }
        }
        // the basis every record was read on, and the candidate's own r²_F under conditioning (the share of its
        // information F carries) for the recipes that name one candidate's contribution
        final Map<String, Integer> index = new HashMap<>();
        for (int j = 0; j < nCand; j++) index.put(names.get(spec.jointColumn(j)), j);
        for (final Map<String, Object> rec : out) {
            rec.put("basis", js.basis);
            final String kind = (String) rec.get("kind");
            final Integer j = "select".equals(kind) || "difference".equals(kind) || "ratio".equals(kind) ? index.get((String) rec.get("candidate")) : null;
            rec.put("r2_F", j != null && js.r2 != null && !Double.isNaN(js.r2[j]) ? js.r2[j] : null);
        }
        return out;
    }

    /**
     * The joint sums the several-candidate suggestions read: the centred score vector S, the Fisher block H, the pHd
     * block M, the un-orthogonalised H the redundancy clusters read ({@code hRaw}: the same as H on the marginal basis),
     * each candidate's r²_F (null on the marginal basis) and the basis ({@code marginal} / {@code partial}).
     */
    record JointSums(double[] s, double[][] h, double[][] mm, double[][] hRaw, double[] r2, String basis) {
    }

    /** The marginal joint sums of DSL doc §9.5 centred at report time; null without usable sums. */
    static JointSums marginalJoint(final ScreenSpec spec, final Map<Integer, ScoreAccumulator> accumulators) {
        final ScoreAccumulator acc = accumulators.get(ScoreAccumulator.JOINT_KEY);
        final int m = spec.jointColumnCount();
        final GroupScorer.JointLayout at = GroupScorer.JointLayout.of(m);
        final double[] e = acc == null ? null : acc.getExtra();
        if (e == null || e.length != at.length() || !(e[0] > 0)) return null;
        // centred S, H (Fisher), M (pHd)
        final double[] s = new double[m];
        final double[][] h = new double[m][m], mm = new double[m][m];
        if (spec.isGroupedMultinomial()) {
            for (int j = 0; j < m; j++) {
                s[j] = e[at.s() + j];
                for (int l = 0; l < m; l++) {
                    h[j][l] = e[at.h() + GroupScorer.packed(m, j, l)];
                    mm[j][l] = e[at.mm() + GroupScorer.packed(m, j, l)];
                }
            }
        } else {
            final double n0 = e[0], rsum = e[at.r()];
            final double[] mu = new double[m];
            for (int j = 0; j < m; j++) mu[j] = e[1 + j] / n0;
            for (int j = 0; j < m; j++) {
                s[j] = e[at.s() + j] - mu[j] * rsum;
                for (int l = 0; l < m; l++) {
                    h[j][l] = e[at.h() + GroupScorer.packed(m, j, l)] - n0 * mu[j] * mu[l];
                    mm[j][l] = e[at.mm() + GroupScorer.packed(m, j, l)] - mu[j] * e[at.s() + l] - mu[l] * e[at.s() + j] + mu[j] * mu[l] * rsum;
                }
            }
            // a column whose centred spread the raw moments cannot hold (window-constant, or a spread below 1e-6 of its
            // magnitude) is degenerate, as in stats(): it leaves the joint metric rather than enter it as rounding residue
            for (int j = 0; j < m; j++) {
                if (h[j][j] > ROW_DEGENERATE_REL * e[at.h() + GroupScorer.packed(m, j, j)]) continue;
                s[j] = 0d;
                for (int l = 0; l < m; l++) h[j][l] = h[l][j] = mm[j][l] = mm[l][j] = 0d;
            }
            // the intercept is profiled out of M as it is out of S: the residual at its one-step estimate
            // Σ w r / Σ w v (prior mode: y − ȳ), so a miscalibrated baseline does not add its mean residual times H
            final double rMean = rsum / n0;
            for (int j = 0; j < m; j++) for (int l = 0; l < m; l++) mm[j][l] -= rMean * h[j][l];
            if (spec.isGaussian()) {
                // identity link: S, H and M over σ², the residual (offset) / label (prior) variance, as in stats()
                final double sigma2 = e[at.r2()] / n0 - rMean * rMean;
                if (!(sigma2 > 0)) return null;
                for (int j = 0; j < m; j++) {
                    s[j] /= sigma2;
                    for (int l = 0; l < m; l++) {
                        h[j][l] /= sigma2;
                        mm[j][l] /= sigma2;
                    }
                }
            } else if (!spec.hasBaseline()) {
                // prior mode: raw moments, the Fisher weight at ȳ (offset mode: already inside the sums)
                final double weight = spec.fisherWeight(rMean);
                for (int j = 0; j < m; j++) for (int l = 0; l < m; l++) h[j][l] *= weight;
            }
        }
        return new JointSums(s, h, mm, h, null, "marginal");
    }

    /**
     * The joint sums at the fitted p̂ orthogonalised against F (DSL doc §9.5, {@link ConditioningScorer#JOINT_PARTIAL_KEY}):
     * Γ = (G + l2·N·I)⁻¹A', S⊥ = S − Γ'g, H⊥ = H − Γ'A' − AΓ + Γ'GΓ, M⊥ = M − Γ'Mxf' − MxfΓ + Γ'MffΓ, over σ²; a column
     * F explains fully (H⊥_jj ≤ 1e-10 H_jj, r²_F = 1) leaves the metric. Null without the sums or when the solve fails.
     */
    static JointSums partialJoint(final ScreenSpec spec, final Map<Integer, PartialAccumulator> partials, final FitState fit, final double sigma2) {
        if (partials == null || fit == null || !fit.hasBest) return null;
        final PartialAccumulator acc = partials.get(ConditioningScorer.JOINT_PARTIAL_KEY);
        final int m = spec.jointColumnCount(), k = fit.k;
        final ConditioningScorer.JointPartialLayout at = ConditioningScorer.JointPartialLayout.of(m, k);
        final double[] e = acc == null || acc.isEmpty() ? null : acc.getTotal();
        if (e == null || e.length != at.length() || !(e[at.used()] > 0) || !allFinite(e)) return null;
        final double[][] aT = new double[k][m];   // A' : k × m, the right-hand sides
        for (int j = 0; j < m; j++) for (int c = 0; c < k; c++) aT[c][j] = e[at.a() + j * k + c];
        final double[][] gamma;
        try {
            gamma = MatrixOps.solveGram(fit.bestG, aT, spec.conditioningL2 * fit.nUnits);   // k × m
        } catch (final RuntimeException ex) {
            return null;
        }
        // G Γ and Mff Γ (k × m) once
        final double[][] gG = new double[k][m], fG = new double[k][m];
        for (int c = 0; c < k; c++) {
            for (int d = 0; d < k; d++) {
                final double g = fit.bestG[c][d], f = e[at.mff() + GroupScorer.packed(k, c, d)];
                for (int j = 0; j < m; j++) {
                    gG[c][j] += g * gamma[d][j];
                    fG[c][j] += f * gamma[d][j];
                }
            }
        }
        final double[] s = new double[m];
        final double[][] h = new double[m][m], mm = new double[m][m], hr = new double[m][m];
        for (int j = 0; j < m; j++) {
            double gg = 0;
            for (int c = 0; c < k; c++) gg += gamma[c][j] * fit.bestGrad[c];
            s[j] = (e[j] - gg) / sigma2;
            for (int l = 0; l < m; l++) {
                final int q = GroupScorer.packed(m, j, l);
                double ga = 0, ag = 0, ggg = 0, gx = 0, xg = 0, gfg = 0;
                for (int c = 0; c < k; c++) {
                    ga += gamma[c][j] * e[at.a() + l * k + c];
                    ag += e[at.a() + j * k + c] * gamma[c][l];
                    ggg += gamma[c][j] * gG[c][l];
                    gx += gamma[c][j] * e[at.mxf() + l * k + c];
                    xg += e[at.mxf() + j * k + c] * gamma[c][l];
                    gfg += gamma[c][j] * fG[c][l];
                }
                hr[j][l] = e[at.h() + q] / sigma2;
                h[j][l] = (e[at.h() + q] - ga - ag + ggg) / sigma2;
                mm[j][l] = (e[at.mm() + q] - gx - xg + gfg) / sigma2;
            }
        }
        final int nCand = spec.jointColumns.size();
        final double[] r2 = new double[nCand];
        for (int j = 0; j < m; j++) {
            final double rj = hr[j][j] > 0 ? Math.min(1d, Math.max(0d, 1d - h[j][j] / hr[j][j])) : Double.NaN;
            if (j < nCand) r2[j] = rj;
            if (h[j][j] > 1e-10 * hr[j][j]) continue;
            // nothing left of the column beyond F: it leaves the metric rather than enter it as rounding residue
            if (j < nCand) r2[j] = 1d;
            s[j] = 0d;
            for (int l = 0; l < m; l++) h[j][l] = h[l][j] = mm[j][l] = mm[l][j] = 0d;
        }
        return new JointSums(s, h, mm, hr, r2, "partial");
    }

    /** A pair's two-dimensional Newton direction kept for a difference / ratio suggestion (joint column indices). */
    private record PairDirection(int i, int j, double excess, double chi2, double bi, double bj, double consistency) {
    }

    private static int find(final int[] parent, final int j) {
        int r = j;
        while (parent[r] != r) r = parent[r];
        return r;
    }

    /** The product of two square matrices (the pHd metric's B = H^(−1/2) M H^(−1/2)), i-k-j order. */
    private static double[][] multiply(final double[][] x, final double[][] y) {
        final int n = x.length;
        final double[][] out = new double[n][n];
        for (int i = 0; i < n; i++) {
            for (int k = 0; k < n; k++) {
                final double xik = x[i][k];
                if (xik == 0d) continue;
                final double[] yk = y[k], oi = out[i];
                for (int j = 0; j < n; j++) oi[j] += xik * yk[j];
            }
        }
        return out;
    }

    private static boolean allFinite(final double[] v) {
        for (final double d : v) if (!Double.isFinite(d)) return false;
        return true;
    }

    private static boolean allFinite(final double[][] m) {
        for (final double[] row : m) if (!allFinite(row)) return false;
        return true;
    }

    /** A joint suggestion record in the suggestions schema (the discovery / confirmation fields carry the in-sample values). */
    private static Map<String, Object> jointRecord(final String candidate, final String kind, final String name, final double consistency,
                                                   final double share, final double chi2, final double gain, final Boolean passed,
                                                   final double threshold, final String fragment) {
        final Map<String, Object> s = new LinkedHashMap<>();
        s.put("candidate", candidate);
        s.put("kind", kind);
        s.put("name", name);
        s.put("cut", null);
        s.put("direction", null);
        s.put("fill", null);
        s.put("consistency", Double.isNaN(consistency) ? null : consistency);
        s.put("share", share);
        s.put("chi2", chi2);
        s.put("confirmation_chi2", null);
        s.put("confirmation_share", null);
        s.put("confirmation_gain", Double.isNaN(gain) ? null : gain);
        s.put("confirmation_pValue", null);
        s.put("threshold", Double.isNaN(threshold) ? null : threshold);
        s.put("passed", passed);
        s.put("placebo", false);
        s.put("fragment", fragment);
        s.put("basis", null);
        s.put("r2_F", null);
        return s;
    }

    /**
     * The interaction shape of every real pair (DSL doc §8.7) from its 2-D grid at the fitted means: the best depth-2
     * tree over the k × k cells — a first split on one member at an edge, then in each side the best split on the
     * other member — its gain (the split gains G_L² / H_L + G_R² / H_R − G² / H, the intercept profiled per node,
     * diagonal information) as a share of the grid's block χ² (the bound), and the asymmetry of the two sides'
     * second-level gains: near 0 the other member matters on one side only ("b matters only when a > c"), near 1 it
     * matters on both (no conditional shape). In-sample, a diagnostic; the recipe is the crossed bins or a
     * conditional expression. Gaussian divides the sums by the residual variance {@code sigma2} (1 otherwise).
     */
    static List<Map<String, Object>> interactions(final ScreenSpec spec, final Map<Integer, PartialAccumulator> partials, final boolean conditioned,
                                                  final double nUnits, final double sigma2, final Bins bins) {
        final List<Map<String, Object>> out = new ArrayList<>();
        if (!spec.hasPairShape() || !conditioned || partials == null || bins == null) return out;
        final int kk = spec.pairShapeBins, cells = kk * kk;
        for (int q = 0; q < spec.pairs.size(); q++) {
            final PartialAccumulator pacc = partials.get(spec.pairGridKey(q));
            final double[] vec = pacc == null || pacc.isEmpty() ? null : pacc.getTotal();
            if (vec == null) continue;
            final int[] members = spec.pairMembers(q);
            final String a = spec.conditioningFields.get(members[0]), b = spec.conditioningFields.get(members[1]);
            final double[] ea = bins.gridEdges().apply(spec.conditioningColumn(members[0]));
            final double[] eb = bins.gridEdges().apply(spec.conditioningColumn(members[1]));
            if (ea == null || eb == null) continue;
            final boolean grouped = spec.isGroupedMultinomial();
            final double[] s = new double[cells], diag = new double[cells];
            final double[][] h = grouped ? new double[cells][cells] : null;
            for (int c = 0; c < cells; c++) {
                s[c] = vec[c] / sigma2;
                if (grouped) {
                    for (int d = 0; d < cells; d++) h[c][d] = ((c == d ? vec[cells + c] : 0d) - vec[2 * cells + c * cells + d]) / sigma2;
                    diag[c] = h[c][c];
                } else {
                    diag[c] = vec[cells + c] / sigma2;
                }
            }
            // the bound: the grid's block χ². Grouped: the full Fisher block (the shares' sum is the implicit
            // reference). Row families: the diagonal block with the intercept profiled, Σ S_c² / H_c − (Σ S)² / Σ H —
            // the tree's gain (the same diagonal information, profiled per node) never exceeds it
            final double chi2;
            if (grouped) {
                final Stats[] block = new Stats[1];
                chi2 = blockChi2(s, h, diag, nUnits, 2, block) < 1 ? Double.NaN : block[0].chi2;
            } else {
                chi2 = profiledDiagonalChi2(s, diag);
            }
            if (!(chi2 > 0)) continue;
            // the best depth-2 tree: first split on a (cells with a_bin <= j) or on b, then the best split of the
            // other member within each side
            double best = -1;
            int bestVar = -1, bestCut = -1, bestLeftCut = -1, bestRightCut = -1;
            double bestLeft = 0, bestRight = 0;
            for (int var = 0; var < 2; var++) {
                for (int j = 0; j < kk - 1; j++) {
                    final List<Integer> left = new ArrayList<>(), right = new ArrayList<>();
                    for (int c = 0; c < cells; c++) ((var == 0 ? c / kk : c % kk) <= j ? left : right).add(c);
                    final double first = splitGain(s, diag, left, right);
                    if (!(first >= 0)) continue;
                    final int[] lc = new int[1], rc = new int[1];
                    final double second1 = bestSplit(s, diag, left, 1 - var, kk, lc), second2 = bestSplit(s, diag, right, 1 - var, kk, rc);
                    final double total = first + Math.max(second1, 0) + Math.max(second2, 0);
                    if (total > best) {
                        best = total;
                        bestVar = var;
                        bestCut = j;
                        bestLeft = Math.max(second1, 0);
                        bestRight = Math.max(second2, 0);
                        bestLeftCut = lc[0];
                        bestRightCut = rc[0];
                    }
                }
            }
            if (bestVar < 0) continue;
            final String first = bestVar == 0 ? a : b, other = bestVar == 0 ? b : a;
            final double[] firstEdges = bestVar == 0 ? ea : eb, otherEdges = bestVar == 0 ? eb : ea;
            final double cut = firstEdges[bestCut];
            final double share = Math.min(1d, best / chi2), gain = nUnits > 0 ? best / (2 * nUnits) : Double.NaN;
            final double sideGain = Math.max(bestLeft, bestRight), otherGain = Math.min(bestLeft, bestRight);
            if (!(sideGain > 0)) {
                // no split of the other member adds anything on either side: the grid's gain is the first member's
                // own split, not an interaction — no side, no fill, no consistency (0 / 0)
                final String reading = "no split of " + other + " adds to " + first + " at " + fmt(cut) + " on either side: no conditional shape";
                final Map<String, Object> rec = jointRecord(spec.pairName(q), "interaction", first + " at " + fmt(cut), Double.NaN, share, best, gain, null, Double.NaN,
                        reading + " -> " + rowBinFragment(first, new double[]{cut}));
                rec.put("cut", cut);
            rec.put("basis", "partial");
                out.add(rec);
                continue;
            }
            final boolean rightSide = bestRight >= bestLeft;
            final double consistency = otherGain / sideGain;
            final int sideCut = rightSide ? bestRightCut : bestLeftCut;
            final double fill = otherEdges[sideCut];
            final String side = first + (rightSide ? " > " : " <= ");
            final String reading = other + " matters " + (consistency < 0.5 ? "mainly" : "on both sides, and most") + " when " + side + fmt(cut)
                    + " (its own cut at " + fmt(fill) + "); second-level gains " + fmt(bestLeft) + " / " + fmt(bestRight);
            // the recipe reproduces the screen's partition exactly: the row bin op at the next double above each edge
            // (rowBinFragment), the conditional expression at the full-precision cut
            final String fragment = "cross of " + rowBinFragment(first, new double[]{cut}) + " and " + rowBinFragment(other, new double[]{fill})
                    + " (a row type: cross of the two bins)"
                    + (consistency < 0.5 ? ", or {scope: row, expr: \"" + side + plain(cut) + " ? " + other + " : 0\"}" : "");
            final Map<String, Object> rec = jointRecord(spec.pairName(q), "interaction", first + " at " + fmt(cut), consistency,
                    share, best, gain, null, Double.NaN, reading + " -> " + fragment);
            rec.put("cut", cut);
            rec.put("basis", "partial");
            rec.put("direction", rightSide ? ">" : "<=");
            rec.put("fill", fill);
            out.add(rec);
        }
        return out;
    }

    /**
     * The score statistic of a one-hot block with diagonal information and the intercept profiled:
     * Σ S_c² / H_c − (Σ S_c)² / Σ H_c over the cells with information (NaN below two of them).
     */
    private static double profiledDiagonalChi2(final double[] s, final double[] h) {
        double maxH = 0;
        for (final double v : h) maxH = Math.max(maxH, v);
        double chi2 = 0, gs = 0, hs = 0;
        int active = 0;
        for (int c = 0; c < s.length; c++) {
            if (!(h[c] > 1e-12 * maxH) || !(h[c] > 1e-300) || !Double.isFinite(s[c])) continue;
            chi2 += s[c] * s[c] / h[c];
            gs += s[c];
            hs += h[c];
            active++;
        }
        return active < 2 ? Double.NaN : Math.max(0d, chi2 - gs * gs / hs);
    }

    /** A cut written in full, in plain notation (a rounded cut moves the rows between it and the true one to the other side). */
    private static String plain(final double v) {
        return java.math.BigDecimal.valueOf(v).stripTrailingZeros().toPlainString();
    }

    /** The split gain of a node's cells into left / right with the intercept profiled: G_L² / H_L + G_R² / H_R − G² / H. */
    private static double splitGain(final double[] s, final double[] h, final List<Integer> left, final List<Integer> right) {
        double gl = 0, hl = 0, gr = 0, hr = 0;
        for (final int c : left) {
            gl += s[c];
            hl += h[c];
        }
        for (final int c : right) {
            gr += s[c];
            hr += h[c];
        }
        if (!(hl > 0) || !(hr > 0)) return Double.NaN;
        return gl * gl / hl + gr * gr / hr - (gl + gr) * (gl + gr) / (hl + hr);
    }

    /** The best split of a node's cells on member {@code var} (0 = a, 1 = b) at an edge; the edge index in {@code cut[0]} (−1 when none). */
    private static double bestSplit(final double[] s, final double[] h, final List<Integer> node, final int var, final int kk, final int[] cut) {
        double best = 0;
        cut[0] = -1;
        for (int j = 0; j < kk - 1; j++) {
            final List<Integer> left = new ArrayList<>(), right = new ArrayList<>();
            for (final int c : node) ((var == 0 ? c / kk : c % kk) <= j ? left : right).add(c);
            final double gain = splitGain(s, h, left, right);
            if (gain > best) {
                best = gain;
                cut[0] = j;
            }
        }
        return best;
    }

    /**
     * The categorical grouping suggestions (DSL doc §6.2): a column's named levels sorted by effect S_l / H_l and cut
     * once at the best split gain (the boosted-tree categorical split) — kind {@code grouping}, the two groups in
     * the fragment, the split's share of the block χ² — and every level whose own contrast against the rest is
     * strong (|z| ≥ 3) as a one-hot indicator — kind {@code onehot} (the feature transform's row {@code indicator} op;
     * the folded {@code (other)} level has no single value to flag and gets none). In-sample, hypotheses, for the
     * passing columns only ({@code passed}: the column names that passed their test).
     */
    static List<Map<String, Object>> groupings(final ScreenSpec spec, final Map<Integer, ScoreAccumulator> accumulators, final double nUnits, final Bins bins,
                                               final Set<String> passed) {
        final List<Map<String, Object>> out = new ArrayList<>();
        if (!spec.hasCategoricals() || bins == null) return out;
        for (int c = 0; c < spec.categoricals.size(); c++) {
            if (!passed.contains(spec.categoricals.get(c))) continue;
            final WindowQuantiles.Levels levels = bins.levels().apply(c);
            if (levels == null || levels.size() < 2) continue;
            final int nb = levels.size();
            final ScoreAccumulator acc = accumulators.get(spec.categoricalKey(c, -1));
            if (acc == null || acc.getExtra() == null) continue;
            final Block block = binnedStats(spec, nb, acc.getExtra(), nUnits, (long) acc.getTotal()[ScoreAccumulator.N_OBS]);
            if (block.stats.degenerate || !(block.stats.chi2 > 0)) continue;
            final String name = spec.categoricals.get(c);
            // levels by effect, the best single cut along that order
            final List<Integer> order = new ArrayList<>();
            for (int l = 0; l < nb; l++) if (block.h[l] > 0) order.add(l);
            order.sort(Comparator.comparingDouble(l -> block.s[l] / block.h[l]));
            double bestGain = 0;
            int bestCut = -1;
            for (int j = 1; j < order.size(); j++) {
                final double gain = splitGain(block.s, block.h, order.subList(0, j), order.subList(j, order.size()));
                if (gain > bestGain) {
                    bestGain = gain;
                    bestCut = j;
                }
            }
            if (bestCut > 0) {
                final List<String> low = new ArrayList<>(), high = new ArrayList<>();
                for (int j = 0; j < order.size(); j++) (j < bestCut ? low : high).add(levels.names().get(order.get(j)));
                out.add(jointRecord(name, "grouping", "split", Double.NaN, Math.min(1d, bestGain / block.stats.chi2), bestGain,
                        nUnits > 0 ? bestGain / (2 * nUnits) : Double.NaN, null, Double.NaN,
                        "group " + name + " into " + low + " (lower effect) vs " + high + " (higher): a level grouping, or one-hot the few strong levels and a shrunk encoding for the rest"));
            }
            // strong single levels
            for (int l = 0; l < nb; l++) {
                if (!(block.h[l] > 0)) continue;
                final String level = levels.names().get(l);
                // the fold of the levels beyond maxLevels is no value a row indicator can name
                if (levels.folded() && l == nb - 1) continue;
                final double[] phi = new double[nb];
                phi[l] = 1;
                final double chi2 = contrastChi2(block, phi, nb);
                if (chi2 < 9) continue;
                final String indicator = ScreenSpec.LEVEL_NULL.equals(level)
                        ? "{scope: row, expr: \"" + name + " == null ? 1 : 0\"}"
                        : "{name: " + name + "_is_" + level.replaceAll("[^A-Za-z0-9_]", "_") + ", scope: row, type: indicator, input: " + name + ", values: [" + new JsonPrimitive(level) + "]}";
                out.add(jointRecord(name, "onehot", level, Double.NaN, Math.min(1d, chi2 / block.stats.chi2), chi2,
                        nUnits > 0 ? chi2 / (2 * nUnits) : Double.NaN, null, Double.NaN,
                        indicator + " (z " + fmt(contrastSign(block, phi, nb) * Math.sqrt(chi2)) + ")"));
            }
        }
        return out;
    }

    public static Schema suggestionSchema() {
        return Schema.builder()
                .withField("candidate", Schema.FieldType.STRING)
                .withField("kind", Schema.FieldType.STRING)
                .withField("name", Schema.FieldType.STRING)
                .withField("cut", Schema.FieldType.FLOAT64)
                .withField("direction", Schema.FieldType.STRING)
                .withField("fill", Schema.FieldType.FLOAT64)
                .withField("consistency", Schema.FieldType.FLOAT64)
                .withField("share", Schema.FieldType.FLOAT64)
                .withField("chi2", Schema.FieldType.FLOAT64)
                .withField("confirmation_chi2", Schema.FieldType.FLOAT64)
                .withField("confirmation_share", Schema.FieldType.FLOAT64)
                .withField("confirmation_gain", Schema.FieldType.FLOAT64)
                .withField("confirmation_pValue", Schema.FieldType.FLOAT64)
                .withField("threshold", Schema.FieldType.FLOAT64)
                .withField("passed", Schema.FieldType.BOOLEAN)
                .withField("placebo", Schema.FieldType.BOOLEAN)
                .withField("fragment", Schema.FieldType.STRING)
                .withField("basis", Schema.FieldType.STRING)
                .withField("r2_F", Schema.FieldType.FLOAT64)
                .build();
    }

    public static Result build(final ScreenSpec spec, final Map<Integer, ScoreAccumulator> accumulators) {
        return build(spec, accumulators, null, null);
    }

    public static Result build(final ScreenSpec spec, final Map<Integer, ScoreAccumulator> accumulators,
                               final Map<Integer, PartialAccumulator> partials, final FitState fit) {
        return build(spec, accumulators, partials, fit, null);
    }

    /**
     * @param partials the partial-test sums per key (null without conditioning)
     * @param fit      the final fit state (null without conditioning)
     * @param bins     the bins' geometry for the suggestions and the block records' {@code bin_edges} (null = no
     *                 one-candidate or interaction suggestions, no ratio, and null edges: a passing value block then
     *                 has no row bin op in the pass list)
     */
    public static Result build(final ScreenSpec spec, final Map<Integer, ScoreAccumulator> accumulators,
                               final Map<Integer, PartialAccumulator> partials, final FitState fit, final Bins bins) {
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
        // the df = 1 keys' γ in one batched solve; a binned key's sums have the block layout [s (B), H, A], not
        // [s, b, a], and blockPartial solves its Γ itself. Only a column × transform key names a transform: a pair
        // key (after them) is a df = 1 column whatever its index modulo the transforms
        final Map<Integer, double[]> gammas;
        if (conditioned) {
            final Map<Integer, PartialAccumulator> scalar = new HashMap<>();
            final int columnKeys = spec.pairKey(0);
            for (final Map.Entry<Integer, PartialAccumulator> e : partials.entrySet()) {
                if (e.getKey() >= 0 && e.getKey() < columnKeys && ScreenSpec.isBinned(spec.transforms.get(e.getKey() % nTransforms))) continue;
                scalar.put(e.getKey(), e.getValue());
            }
            gammas = gammas(scalar, fit, spec.conditioningL2, spec.pairGridKey(0));
        } else {
            gammas = Map.of();
        }
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
            notes.add("conditioning: k = " + fit.k + " exceeds " + ConditioningScorer.PERIOD_GRAM_MAX_K + ", so the per-period"
                    + (spec.hasHeterogeneity() && !spec.heterogeneityByPeriods() ? " / per-level" : "")
                    + " partial information is approximate (the window's Gram scaled by the slice's unit mass); the partial score and sign per slice are exact");
        }
        // skipped units past the share worth a look: which reason, and the way out of an invalid-baseline skip
        final long skipped = (long) b[ScoreAccumulator.UNITS_SKIPPED];
        final long skippedBaseline = (long) b[ScoreAccumulator.UNITS_SKIPPED_BASELINE];
        final long dropped = (long) b[ScoreAccumulator.ROWS_DROPPED];
        // a field modifier on the grouped family: the units whose rows carry more than one level (read as the most
        // frequent), a unit-level field being what the test expects
        final boolean unitModifier = spec.isGroupedMultinomial() && ScreenSpec.HET_FIELD.equals(spec.heterogeneityBy);
        final Long hetMixed = unitModifier ? (long) b[ScoreAccumulator.UNITS_HET_MIXED] : null;
        if (hetMixed != null && hetMixed > 0 && nUnits > 0) {
            notes.add("heterogeneity by " + spec.heterogeneityField + ": " + hetMixed + " of " + (long) nUnits + " units (" + fmt(100d * hetMixed / nUnits)
                    + "%) carry more than one level; each such unit takes its rows' most frequent level (ties to the smallest) — a unit-level modifier is expected");
        }
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
        /** the placebo columns' effective gains per statistic kind (df1 / binned / het): one threshold per kind */
        final Map<String, List<Double>> placeboGains = new LinkedHashMap<>();
        /** the effective heterogeneity test per record (null without a modifier, or for a block record) */
        final List<Het> effectiveHet = new ArrayList<>();
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
                    // the distinct value edges (k − 1 unless tied), the pass list's material to reproduce a passing
                    // block as a row bin op; null for position bins or without a sketch value. A discrete column's
                    // quantile edges can tie (an empty bin the block test already leaves out of df): the record and the
                    // pass list carry the distinct edges, bin_stats keeps the k bins (a tied edge's bin empty)
                    final double[] edges = bins == null ? null : bins.edges().apply(c);
                    r.put("bin_edges", edges == null ? null : new ArrayList<>(Arrays.stream(distinctEdges(edges)).boxed().toList()));
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
                    // a block has no direction to differ across levels
                    putHet(r, "", null);
                    putHet(r, "partial_", null);
                    r.put("level_z", null);
                    effectiveHet.add(null);
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
                final List<Stats> periodStats = new ArrayList<>();
                // the heterogeneity modifier's level slices (a declared field) live in the same map under a prefix
                final List<Map<String, Object>> levelRecords = new ArrayList<>();
                final List<Stats> levelStats = new ArrayList<>();
                final Set<String> scorableLevels = new HashSet<>();
                long agree = 0, nPeriods = 0;
                for (final Map.Entry<String, double[]> e : acc.getPeriods().entrySet()) {
                    final Stats ps = stats(spec, e.getValue(), nUnits);
                    if (ScoreAccumulator.isLevel(e.getKey())) {
                        levelRecords.add(sliceRecord("level", ScoreAccumulator.levelName(e.getKey()), ps, ps.nObs));
                        levelStats.add(ps);
                        if (!ps.degenerate) scorableLevels.add(e.getKey());
                        continue;
                    }
                    periodRecords.add(sliceRecord("period", e.getKey(), ps, ps.nObs));
                    periodStats.add(ps);
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
                r.put("bin_edges", null);
                // the heterogeneity test across the modifier's levels (marginal; the partial one follows the partial slices)
                final Het het = !spec.hasHeterogeneity() ? null : st.degenerate ? Het.degenerate(0)
                        : heterogeneity(spec.heterogeneityByPeriods() ? periodStats : levelStats, nUnits);
                putHet(r, "", het);
                r.put("level_z", spec.hasHeterogeneity() && !spec.heterogeneityByPeriods() ? levelRecords : null);
                Het usedHet = het;
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
                    final List<Stats> partialPeriodStats = new ArrayList<>();
                    final List<Stats> partialLevelStats = new ArrayList<>();
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
                            if (ScoreAccumulator.isLevel(e.getKey())) {
                                // a modifier level: the same slice rule, feeding the partial heterogeneity test
                                partialLevelStats.add(scorableLevels.contains(e.getKey())
                                        ? partialPeriod(e.getValue(), fitPeriods.getPeriods().get(e.getKey()), fit, nUnits, pObs, sigma2, gamma, windowGGg)
                                        : Stats.degenerate(pObs));
                                continue;
                            }
                            final Stats ps = scorablePeriods.contains(e.getKey())
                                    ? partialPeriod(e.getValue(), fitPeriods.getPeriods().get(e.getKey()), fit, nUnits, pObs, sigma2, gamma, windowGGg)
                                    : Stats.degenerate(pObs);
                            partialPeriods.add(sliceRecord("period", e.getKey(), ps, pObs));
                            partialPeriodStats.add(ps);
                            if (!ps.degenerate) {
                                pPeriods++;
                                if (!pst.degenerate && pst.z != 0 && Math.signum(ps.z) == Math.signum(pst.z)) pAgree++;
                            }
                        }
                    }
                    r.put("partial_periods_agree", pAgree);
                    r.put("partial_n_periods", pPeriods);
                    r.put("partial_period_z", partialPeriods);
                    final Het partialHet = !spec.hasHeterogeneity() ? null : pst.degenerate ? Het.degenerate(0)
                            : heterogeneity(spec.heterogeneityByPeriods() ? partialPeriodStats : partialLevelStats, nUnits);
                    putHet(r, "partial_", partialHet);
                    usedHet = partialHet;
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
                    putHet(r, "partial_", null);
                    r.put("partial_periods_agree", null);
                    r.put("partial_n_periods", null);
                    r.put("partial_period_z", null);
                }
                effective.add(used);
                effectiveAgree.add(new long[]{usedAgree, usedPeriods});
                effectiveHet.add(usedHet);
                if (spec.isPlacebo(c) && usedHet != null) placeboGains.computeIfAbsent(ScreenSpec.KIND_HET, kind -> new ArrayList<>()).add(usedHet.degenerate ? 0d : usedHet.gain);
                if (spec.isPlacebo(c)) placeboGains.computeIfAbsent(ScreenSpec.KIND_DF1, kind -> new ArrayList<>()).add(used.degenerate ? 0d : used.estGain);
                r.put("placebo", spec.isPlacebo(c));
                r.put("degenerate", st.degenerate);
                records.add(r);
            }
        }

        // the declared pairs (DSL doc §8.6): partial tests only — the product of two conditioning columns at the
        // fitted means, orthogonalised against F by the same γ solve as any column; placebo pairs calibrate the kind
        for (int q = 0; q < spec.pairCount(); q++) {
            final int key = spec.pairKey(q);
            final Map<String, Object> r = new LinkedHashMap<>();
            r.put("candidate", spec.pairName(q));
            r.put("transform", ScreenSpec.TRANSFORM_PRODUCT);
            r.put("method", METHOD);
            r.put("family", spec.family);
            for (final String f : List.of("S", "H", "beta", "chi2", "z", "est_gain")) r.put(f, null);
            r.put("df", 1L);
            r.put("pValue", null);
            r.put("qValue", null);
            r.put("n_groups", (long) nUnits);
            r.put("n_obs", (long) nUnits);
            r.put("periods_agree", null);
            r.put("n_periods", null);
            r.put("period_z", null);
            r.put("bin_stats", null);
            r.put("bin_edges", null);
            putHet(r, "", null);
            r.put("level_z", null);
            final PartialAccumulator pacc = conditioned ? partials.get(key) : null;
            final double[] vec = pacc == null || pacc.isEmpty() ? null : pacc.getTotal();
            final double[] pairGamma = conditioned ? gammas.get(key) : null;
            final Partial pt = vec == null ? new Partial(Stats.degenerate((long) nUnits), Double.NaN)
                    : partial(vec, fit, nUnits, (long) nUnits, sigma2, pairGamma);
            final Stats pst = pt.stats;
            r.put("r2_F", Double.isNaN(pt.r2) ? null : pt.r2);
            r.put("partial_S", conditioned ? pst.s : null);
            r.put("partial_H", conditioned ? pst.h : null);
            r.put("partial_chi2", conditioned ? pst.chi2 : null);
            r.put("partial_z", conditioned ? pst.z : null);
            r.put("partial_gain", conditioned ? pst.estGain : null);
            r.put("partial_pValue", conditioned ? pst.pValue : null);
            r.put("partial_df", null);
            putHet(r, "partial_", null);
            // the pair's partial statistic by period with the window's γ, as a column's (DSL doc §8.2): the period
            // agreement applies to a pair too; a period's rows come from the fit's own slice (no marginal pair test)
            final boolean pairPeriodsOn = conditioned && spec.periodsBucket != null;
            final List<Map<String, Object>> pairPeriods = new ArrayList<>();
            long pAgree = 0, pPeriods = 0;
            if (pairPeriodsOn && vec != null && !pst.degenerate && fitPeriods != null && pairGamma != null) {
                final double windowGGg = fitPeriods.getTotal().length < 1 + fit.k + fit.k * fit.k ? quadratic(pairGamma, fit.bestG, fit.k) : 0d;
                for (final Map.Entry<String, double[]> e : pacc.getPeriods().entrySet()) {
                    if (ScoreAccumulator.isLevel(e.getKey())) continue;
                    final double[] fitVec = fitPeriods.getPeriods().get(e.getKey());
                    // the period's share of the fit's (weighted) unit mass on the unit count's scale: a count, whatever
                    // the weights' scale (the raw mass of weights below 1 would read as fewer than two observations)
                    final long pObs = fitVec == null || !(fit.nUnits > 0) ? 0 : Math.round(fitVec[0] / fit.nUnits * nUnits);
                    // the column's rule per period: a period without the pair's information (a member missing
                    // throughout it, or no variation) has no slice — its S⊥_p / H⊥_p would be the fit's own −γ'g_p /
                    // γ'G_pγ, the conditioning model's period misfit rather than the pair
                    final Stats ps = e.getValue()[1] > PAIR_PERIOD_REL * vec[1]
                            ? partialPeriod(e.getValue(), fitVec, fit, nUnits, pObs, sigma2, pairGamma, windowGGg)
                            : Stats.degenerate(pObs);
                    pairPeriods.add(sliceRecord("period", e.getKey(), ps, pObs));
                    if (!ps.degenerate) {
                        pPeriods++;
                        if (pst.z != 0 && Math.signum(ps.z) == Math.signum(pst.z)) pAgree++;
                    }
                }
            }
            r.put("partial_periods_agree", pairPeriodsOn ? pAgree : null);
            r.put("partial_n_periods", pairPeriodsOn ? pPeriods : null);
            r.put("partial_period_z", pairPeriodsOn ? pairPeriods : null);
            effective.add(pst);
            effectiveAgree.add(pairPeriodsOn ? new long[]{pAgree, pPeriods} : null);
            effectiveHet.add(null);
            final boolean placebo = spec.isPlaceboPair(q);
            if (placebo) placeboGains.computeIfAbsent(ScreenSpec.KIND_PAIR, kind -> new ArrayList<>()).add(pst.degenerate ? 0d : pst.estGain);
            r.put("placebo", placebo);
            r.put("degenerate", pst.degenerate);
            records.add(r);
        }
        if (spec.hasPairs() && !conditioned) notes.add("pairs: no partial test (see the conditioning note), so the pair records are degenerate");

        // the categorical candidates (DSL doc §6.2): a column's levels as a block (df = active levels − 1), its
        // per-level contrasts in level_z, its own placebo kind (the levels redrawn from the window frequencies)
        for (int c = 0; c < spec.categoricals.size(); c++) {
            final WindowQuantiles.Levels levels = bins == null ? null : bins.levels().apply(c);
            if (levels == null) {
                if (bins != null) notes.add("categorical " + spec.categoricals.get(c) + ": no level counted (no scorable row)");
                continue;
            }
            final int nb = levels.size();
            for (int r = -1; r < spec.categoricalPlacebo; r++) {
                final int key = spec.categoricalKey(c, r);
                final ScoreAccumulator acc = accumulators.getOrDefault(key, new ScoreAccumulator());
                final long nObs = (long) acc.getTotal()[ScoreAccumulator.N_OBS];
                final Block block = nb < 2 ? Block.degenerate(nObs, Math.max(nb, 1)) : binnedStats(spec, nb, acc.getExtra(), nUnits, nObs);
                final Stats st = block.stats;
                final boolean placebo = r >= 0;
                final Map<String, Object> rec = new LinkedHashMap<>();
                rec.put("candidate", placebo ? spec.categoricalPlaceboName(c, r) : spec.categoricals.get(c));
                rec.put("transform", ScreenSpec.TRANSFORM_LEVELS);
                rec.put("method", METHOD);
                rec.put("family", spec.family);
                for (final String f : List.of("S", "H", "beta", "z")) rec.put(f, null);
                rec.put("chi2", st.chi2);
                rec.put("est_gain", st.estGain);
                rec.put("df", (long) block.df);
                rec.put("pValue", st.pValue);
                rec.put("qValue", null);
                rec.put("n_groups", (long) nUnits);
                rec.put("n_obs", st.nObs);
                rec.put("periods_agree", null);
                rec.put("n_periods", null);
                rec.put("period_z", null);
                rec.put("bin_stats", null);
                rec.put("bin_edges", null);
                putHet(rec, "", null);
                // each level against the rest: the df = 1 contrast's signed z, its score and information
                final List<Map<String, Object>> levelRecords = new ArrayList<>();
                if (!placebo) {
                    for (int l = 0; l < nb; l++) {
                        final double[] phi = new double[nb];
                        phi[l] = 1;
                        final double chi2 = st.degenerate ? 0d : contrastChi2(block, phi, nb);
                        final Map<String, Object> lr = new LinkedHashMap<>();
                        lr.put("level", levels.names().get(l));
                        lr.put("z", st.degenerate || !(block.h[l] > 0) ? null : contrastSign(block, phi, nb) * Math.sqrt(chi2));
                        lr.put("S", block.s[l]);
                        lr.put("H", block.h[l]);
                        lr.put("n", Math.round(block.n[l]));
                        levelRecords.add(lr);
                    }
                }
                rec.put("level_z", placebo ? null : levelRecords);
                Stats used = st;
                if (conditioned) {
                    final PartialAccumulator pacc = partials.get(key);
                    final double[] vec = pacc == null || pacc.isEmpty() ? null : pacc.getTotal();
                    final BlockPartial bp = vec == null || st.degenerate ? BlockPartial.degenerate(st.nObs, Double.NaN)
                            : blockPartial(spec, nb, vec, block, fit, nUnits, st.nObs, sigma2, spec.conditioningL2);
                    final Stats pst = bp.partial.stats;
                    rec.put("r2_F", Double.isNaN(bp.partial.r2) ? null : bp.partial.r2);
                    rec.put("partial_S", null);
                    rec.put("partial_H", null);
                    rec.put("partial_chi2", pst.chi2);
                    rec.put("partial_z", null);
                    rec.put("partial_gain", pst.estGain);
                    rec.put("partial_pValue", pst.pValue);
                    rec.put("partial_df", (long) bp.df);
                    used = pst;
                } else {
                    for (final String f : List.of("r2_F", "partial_S", "partial_H", "partial_chi2", "partial_z", "partial_gain", "partial_pValue", "partial_df")) rec.put(f, null);
                }
                putHet(rec, "partial_", null);
                rec.put("partial_periods_agree", null);
                rec.put("partial_n_periods", null);
                rec.put("partial_period_z", null);
                effective.add(used);
                effectiveAgree.add(null);
                effectiveHet.add(null);
                if (placebo) placeboGains.computeIfAbsent(ScreenSpec.KIND_LEVELS, kind -> new ArrayList<>()).add(used.degenerate ? 0d : used.estGain);
                rec.put("placebo", placebo);
                rec.put("degenerate", st.degenerate);
                records.add(rec);
            }
        }

        // placebo threshold per statistic kind (the theoretical chi2(df) quantile when no placebo column is
        // configured): the df = 1 transforms pool one cut, the binned block test its own
        final Map<String, Double> thresholds = new LinkedHashMap<>();
        final Map<String, Double> thresholdsTheoretical = new LinkedHashMap<>();
        final List<String> kinds = new ArrayList<>(List.of(ScreenSpec.KIND_DF1));
        if (spec.hasBinned()) kinds.add(ScreenSpec.KIND_BINNED);
        if (spec.hasHeterogeneity()) kinds.add(ScreenSpec.KIND_HET);
        if (spec.hasPairs()) kinds.add(ScreenSpec.KIND_PAIR);
        if (spec.hasCategoricals()) kinds.add(ScreenSpec.KIND_LEVELS);
        // the heterogeneity test's nominal df: the most levels any record found usable, less one
        int hetDf = 1;
        for (final Het h : effectiveHet) if (h != null && !h.degenerate) hetDf = Math.max(hetDf, h.df);
        // the categorical blocks' nominal df: the largest df any of them found
        int levelsDf = 1;
        for (final Map<String, Object> r : records) if (ScreenSpec.TRANSFORM_LEVELS.equals(r.get("transform"))) levelsDf = Math.max(levelsDf, ((Long) r.get("df")).intValue());
        for (final String kind : kinds) {
            final int df = ScreenSpec.KIND_BINNED.equals(kind) ? spec.binsK - 1 : ScreenSpec.KIND_HET.equals(kind) ? hetDf : ScreenSpec.KIND_LEVELS.equals(kind) ? levelsDf : 1;
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

        // flags: the effective gain above the record's kind's placebo threshold and, under pass.minGain (the
        // practical floor), its excess over df / 2N above the floor; the period rule applies to the df = 1 tests (a
        // block has no sign to agree on)
        long nPassed = 0, nLeak = 0, nHetPassed = 0;
        final Map<String, Double> passedBest = new HashMap<>();
        final Map<String, Double> hetPassedBest = new HashMap<>();
        final List<String> passedPairs = new ArrayList<>();
        for (int i = 0; i < records.size(); i++) {
            final Map<String, Object> r = records.get(i);
            final Stats st = effective.get(i);
            final boolean placebo = (Boolean) r.get("placebo");
            final long[] agreement = effectiveAgree.get(i);
            final double kindThreshold = thresholds.get(ScreenSpec.kind((String) r.get("transform")));
            // the degrees of freedom of the marginal and the effective test (a block's partial df may be fewer)
            final double marginalDf = r.get("df") == null ? 1d : ((Long) r.get("df")).doubleValue();
            final double effectiveDf = conditioned && r.get("partial_df") != null ? ((Long) r.get("partial_df")).doubleValue() : marginalDf;
            final Double estGain = (Double) r.get("est_gain"), partialGain = (Double) r.get("partial_gain");
            r.put("excess_gain", estGain == null ? null : ScreenSpec.excessGain(estGain, marginalDf, nUnits));
            r.put("partial_excess_gain", partialGain == null ? null : ScreenSpec.excessGain(partialGain, effectiveDf, nUnits));
            final boolean passed = !placebo && !st.degenerate && spec.passesGain(st.estGain, effectiveDf, nUnits, kindThreshold)
                    && (agreement == null || spec.periodsAgree(agreement[0], agreement[1]));
            // st is the effective test: the partial statistics whenever leakOnPartial (which implies conditioned).
            // A df = 1 test is flagged on |z| > leakZ; a block (no z) on the same tail — its p-value below
            // P(|Z| > leakZ), so a χ²(df) as unlikely under the null as a z of leakZ flags the block too
            final Double marginalZ = (Double) r.get("z");
            final Double marginalP = (Double) r.get("pValue");
            final boolean leak;
            if (spec.leakZ == null) {
                leak = false;
            } else if (marginalZ != null) {
                leak = Math.abs(leakOnPartial ? st.z() : marginalZ) > spec.leakZ;
            } else {
                final double flagP = leakOnPartial ? (st.degenerate ? Double.NaN : st.pValue) : marginalP == null ? Double.NaN : marginalP;
                leak = flagP < StatMath.chiSquare1UpperTail(spec.leakZ * spec.leakZ);
            }
            r.put("threshold", kindThreshold);
            r.put("passed", passed);
            r.put("leakSuspect", leak);
            final boolean pair = ScreenSpec.TRANSFORM_PRODUCT.equals(r.get("transform"));
            if (passed) {
                // a passing pair is a recipe (a product to build upstream), never one of the pass list's columns: it
                // counts in nPairsPassed, not nPassed (nPassed > 0 keeps meaning the pass list has a column)
                if (pair) {
                    passedPairs.add((String) r.get("candidate"));
                } else {
                    nPassed++;
                    passedBest.merge((String) r.get("candidate"), st.estGain, Math::max);
                }
            }
            if (leak && !placebo) nLeak++;
            // the heterogeneity test's own flag (never folded into passed): its kind's cut, lifted to the floor
            final Het het = effectiveHet.get(i);
            Boolean hetPassed = null;
            if (het != null) {
                hetPassed = !placebo && !het.degenerate && spec.passesGain(het.gain, het.df, nUnits, thresholds.get(ScreenSpec.KIND_HET));
                if (hetPassed) {
                    nHetPassed++;
                    hetPassedBest.merge((String) r.get("candidate"), het.gain, Math::max);
                }
            }
            r.put("het_passed", hetPassed);
        }
        final List<String> hetPassedColumns = new ArrayList<>(hetPassedBest.keySet());
        hetPassedColumns.sort(Comparator.<String, Double>comparing(hetPassedBest::get, Comparator.reverseOrder()).thenComparing(Comparator.<String>naturalOrder()));
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
        summary.put("heterogeneity", spec.heterogeneityLabel());
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
        summary.put("nHetMixedUnits", hetMixed);
        summary.put("nCandidates", (long) spec.candidates.size());
        summary.put("nTransforms", (long) nTransforms);
        summary.put("nScored", (long) candidateRecords.size());
        summary.put("nPassed", nPassed);
        // placebo records (not the placebo gains: a df = 1 placebo record feeds both the df1 and the het kind)
        summary.put("nPlacebo", (long) (records.size() - candidateRecords.size()));
        summary.put("nLeakSuspect", nLeak);
        summary.put("nHetPassed", spec.hasHeterogeneity() ? nHetPassed : null);
        summary.put("hetPassedColumns", spec.hasHeterogeneity() ? hetPassedColumns : null);
        summary.put("nPairs", spec.hasPairs() ? (long) spec.pairs.size() : null);
        summary.put("nPairsPassed", spec.hasPairs() ? (long) passedPairs.size() : null);
        summary.put("passedPairs", spec.hasPairs() ? passedPairs : null);
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
        // the joint sums' row set (DSL doc §9.5): the units added, those added with a missing joint value filled, and
        // the rows a row family left out for want of a window mean (a merging window)
        Long nJointUnits = null, nJointFilled = null, nJointDropped = null;
        if (spec.jointOn) {
            final ScoreAccumulator jointAcc = accumulators.get(ScoreAccumulator.JOINT_KEY);
            final GroupScorer.JointLayout at = GroupScorer.JointLayout.of(spec.jointColumnCount());
            final double[] e = jointAcc == null ? null : jointAcc.getExtra();
            final boolean present = e != null && e.length == at.length();
            nJointUnits = present ? (long) e[at.used()] : 0L;
            nJointFilled = present ? (long) e[at.filled()] : 0L;
            nJointDropped = present ? (long) e[at.dropped()] : 0L;
            if (nJointUnits > 0 && nJointFilled > 0.1 * nJointUnits) {
                notes.add("joint: " + nJointFilled + " of " + nJointUnits + " " + (spec.isGroupedMultinomial() ? "units" : "rows") + " ("
                        + fmt(100d * nJointFilled / nJointUnits) + "%) had a missing joint value filled with "
                        + (spec.isGroupedMultinomial() ? "the unit's p-weighted mean" : "the window mean")
                        + " (no information after centring); the several-candidate suggestions read those fills");
            }
            if (nJointDropped > 0) {
                notes.add("joint: " + nJointDropped + " rows with a missing joint value were left out of the joint sums (no window mean to fill with: the sketch view does not carry over a merging window)");
            }
        }
        summary.put("nJointUnits", nJointUnits);
        summary.put("nJointFilled", nJointFilled);
        summary.put("nJointDropped", nJointDropped);
        summary.put("notes", notes);
        final List<Map<String, Object>> suggested = new ArrayList<>(suggestions(spec, accumulators, partials, fit, conditioned, sigma2, nUnits, bins));
        // the several-candidate suggestions from the joint sums (the df = 1 cut is the forward selection's stop rule)
        suggested.addAll(joint(spec, accumulators, partials, fit, conditioned, sigma2, nUnits, threshold, bins));
        // the real pairs' interaction shapes from their 2-D grids at the fitted means
        suggested.addAll(interactions(spec, partials, conditioned, nUnits, sigma2, bins));
        // the passing categorical candidates' level groupings and strong single levels
        suggested.addAll(groupings(spec, accumulators, nUnits, bins, passedBest.keySet()));
        // every suggestion says the basis it was read on (the marginal sums unless a kind reads the partial ones)
        for (final Map<String, Object> sg : suggested) if (sg.get("basis") == null) sg.put("basis", "marginal");
        // the candidates' suggestions (placebo records excluded, as nScored)
        summary.put("nSuggestions", spec.suggestionsOn || spec.jointOn || spec.hasPairShape() || spec.hasCategoricals() ? suggested.stream().filter(s -> !(Boolean) s.get("placebo")).count() : null);
        summary.put("nJointColumns", spec.jointOn ? (long) spec.jointColumnCount() : null);
        summary.put("nCategoricals", spec.hasCategoricals() ? (long) spec.categoricals.size() : null);
        return new Result(records, summary, suggested);
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
        // the heterogeneity test's modifier and the columns it flagged (a separate list: never part of columns)
        o.addProperty("heterogeneity", (String) summary.get("heterogeneity"));
        if (summary.get("hetPassedColumns") != null) {
            final JsonArray hetColumns = new JsonArray();
            for (final Object name : (List<?>) summary.get("hetPassedColumns")) hetColumns.add((String) name);
            o.add("hetPassedColumns", hetColumns);
        }
        // the passing pairs (a product to build upstream: {scope: row, expr: "a * b"}), apart from columns
        if (summary.get("passedPairs") != null) {
            // the members from the spec (not split from the record name: a field name may hold the separator)
            final Map<String, String[]> members = new HashMap<>();
            for (int q = 0; q < spec.pairs.size(); q++) members.put(spec.pairName(q), spec.pairFieldNames(q));
            final JsonArray pairs = new JsonArray();
            for (final Object name : (List<?>) summary.get("passedPairs")) {
                final String[] ab = members.get((String) name);
                if (ab == null) continue;
                final JsonObject pair = new JsonObject();
                pair.addProperty("a", ab[0]);
                pair.addProperty("b", ab[1]);
                pair.addProperty("fragment", "{scope: row, expr: \"" + ab[0] + " * " + ab[1] + "\"}");
                pairs.add(pair);
            }
            o.add("passedPairs", pairs);
        }
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
        final JsonArray blocks = new JsonArray();
        for (final Map<String, Object> r : result.records()) {
            if (!Boolean.TRUE.equals(r.get("passed"))) continue;
            final JsonObject d = new JsonObject();
            d.addProperty("candidate", (String) r.get("candidate"));
            d.addProperty("transform", (String) r.get("transform"));
            d.addProperty("est_gain", (Double) r.get("est_gain"));
            d.addProperty("excess_gain", (Double) r.get("excess_gain"));
            d.addProperty("z", (Double) r.get("z"));
            if (r.get("partial_gain") != null) {
                d.addProperty("partial_gain", (Double) r.get("partial_gain"));
                d.addProperty("partial_excess_gain", (Double) r.get("partial_excess_gain"));
                d.addProperty("partial_z", (Double) r.get("partial_z"));
                d.addProperty("r2_F", (Double) r.get("r2_F"));
            }
            if (spec.periodsBucket != null) {
                final boolean partial = r.get("partial_gain") != null;
                d.addProperty("periods_agree", (Long) r.get(partial ? "partial_periods_agree" : "periods_agree"));
                d.addProperty("n_periods", (Long) r.get(partial ? "partial_n_periods" : "n_periods"));
            }
            d.addProperty("leakSuspect", (Boolean) r.get("leakSuspect"));
            if (ScreenSpec.isBinned((String) r.get("transform"))) {
                // a passing block: its edges (value bins) or rank cut points (position bins) and the row op that
                // reproduces it upstream (DSL doc §6.1 — the pass list's material for closing the loop on a block)
                final JsonObject block = blockRecipe(spec, (String) r.get("candidate"), r.get("bin_edges"));
                d.add("bins", block);
                d.addProperty("fragment", block.get("fragment").getAsString());
                blocks.add(block);
            }
            details.add(d);
        }
        o.add("passed", details);
        if (spec.hasBinned()) o.add("passedBlocks", blocks);
        return o;
    }

    /**
     * The recipe of a passing binned block: {@code k} (the configured bin count), {@code edges} (value bins: the
     * distinct window quantile edges — k − 1, fewer when a discrete column's edges tie — bin i = (edge_{i−1}, edge_i])
     * or {@code rankCuts} (position bins: the rank fractions i / k), whether the
     * missing values had their own bin, and the feature transform's row {@code bin} op that reproduces those bins
     * ({@link #rowBinFragment}: the next double above each edge; a position block has no row op: the within-unit
     * rank is a context op, the fragment says so).
     */
    static JsonObject blockRecipe(final ScreenSpec spec, final String candidate, final Object edges) {
        final JsonObject block = new JsonObject();
        block.addProperty("candidate", candidate);
        block.addProperty("k", spec.binsK);
        block.addProperty("edgesKind", spec.binsEdges);
        block.addProperty("missingBin", true);
        final JsonArray cuts = new JsonArray();
        if (ScreenSpec.EDGES_RANK.equals(spec.binsEdges)) {
            for (int j = 1; j < spec.binsK; j++) cuts.add((double) j / spec.binsK);
            block.add("rankCuts", cuts);
            block.add("edges", null);
            block.addProperty("fragment", "the rank of " + candidate + " within the unit, cut at " + cuts + " (a context op upstream, then a row bin)");
        } else if (edges instanceof List<?> list) {
            final double[] values = list.stream().mapToDouble(v -> (Double) v).toArray();
            for (final double v : values) cuts.add(v);
            block.add("edges", cuts);
            block.add("rankCuts", null);
            block.addProperty("fragment", rowBinFragment(candidate, values));
        } else {
            // no value edges (no window sketch value for the column, or a report built without the bins' geometry):
            // an empty edge list is not a row bin op (the feature transform rejects it), so say so instead
            block.add("edges", null);
            block.add("rankCuts", null);
            block.addProperty("fragment", "no value edges for " + candidate + " (no window sketch value): no row bin op reproduces the block");
        }
        return block;
    }

    /**
     * The feature transform's row {@code bin} op over the screen's value edges. The screen's bin i is
     * (edge_{i−1}, edge_i] (a value equal to an edge falls below it, and a sketch edge is an observed value) while
     * the row op counts the edges a value reaches (bin i = [edge_{i−1}, edge_i)), so the op takes the next double
     * above each edge — v ≥ nextUp(e) exactly when v > e, the same partition with ties and repeated edges — written
     * in full (a rounded edge moves the rows between it and the true one to the neighbouring bin).
     */
    static String rowBinFragment(final String input, final double[] edges) {
        final StringBuilder list = new StringBuilder();
        final double[] distinct = distinctEdges(edges);
        for (int i = 0; i < distinct.length; i++) list.append(i == 0 ? "" : ", ").append(Double.toString(Math.nextUp(distinct[i])));
        return "{scope: row, type: bin, input: " + input + ", edges: [" + list + "]}";
    }

    /**
     * The distinct values of non-decreasing value edges, in order: an edge is kept when it is above the last one
     * kept, by numeric comparison — so −0.0 and 0.0 are one edge (they bin alike, and {@link Math#nextUp} maps both
     * to the same double), where {@code DoubleStream.distinct} would keep both.
     */
    static double[] distinctEdges(final double[] edges) {
        final double[] out = new double[edges.length];
        int n = 0;
        for (final double e : edges) {
            if (n == 0 || e > out[n - 1]) out[n++] = e;
        }
        return n == edges.length ? edges : Arrays.copyOf(out, n);
    }

    /**
     * The rule behind {@code passed} as applied: the effective test's gain against the threshold, its excess gain
     * (gain − df / 2N) against {@code pass.minGain} when declared, then the period agreement.
     */
    static String passRule(final ScreenSpec spec, final boolean conditioned) {
        final String gain = (conditioned ? "partial_gain" : "est_gain") + " > threshold"
                + (spec.minGain == null ? "" : " and " + (conditioned ? "partial_excess_gain" : "excess_gain") + " > " + spec.minGain);
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
        final Schema level = Schema.builder()
                .withField("level", Schema.FieldType.STRING)
                .withField("z", Schema.FieldType.FLOAT64)
                .withField("S", Schema.FieldType.FLOAT64)
                .withField("H", Schema.FieldType.FLOAT64)
                .withField("n", Schema.FieldType.INT64)
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
                .withField("excess_gain", Schema.FieldType.FLOAT64)
                .withField("df", Schema.FieldType.INT64)
                .withField("pValue", Schema.FieldType.FLOAT64)
                .withField("qValue", Schema.FieldType.FLOAT64)
                .withField("n_groups", Schema.FieldType.INT64)
                .withField("n_obs", Schema.FieldType.INT64)
                .withField("periods_agree", Schema.FieldType.INT64)
                .withField("n_periods", Schema.FieldType.INT64)
                .withField("period_z", Schema.FieldType.array(Schema.FieldType.element(period)))
                .withField("bin_stats", Schema.FieldType.array(Schema.FieldType.element(bin)))
                .withField("bin_edges", Schema.FieldType.array(Schema.FieldType.FLOAT64))
                .withField("het_chi2", Schema.FieldType.FLOAT64)
                .withField("het_df", Schema.FieldType.INT64)
                .withField("het_pValue", Schema.FieldType.FLOAT64)
                .withField("het_gain", Schema.FieldType.FLOAT64)
                .withField("het_levels", Schema.FieldType.INT64)
                .withField("level_z", Schema.FieldType.array(Schema.FieldType.element(level)))
                .withField("r2_F", Schema.FieldType.FLOAT64)
                .withField("partial_S", Schema.FieldType.FLOAT64)
                .withField("partial_H", Schema.FieldType.FLOAT64)
                .withField("partial_chi2", Schema.FieldType.FLOAT64)
                .withField("partial_z", Schema.FieldType.FLOAT64)
                .withField("partial_gain", Schema.FieldType.FLOAT64)
                .withField("partial_excess_gain", Schema.FieldType.FLOAT64)
                .withField("partial_pValue", Schema.FieldType.FLOAT64)
                .withField("partial_df", Schema.FieldType.INT64)
                .withField("partial_het_chi2", Schema.FieldType.FLOAT64)
                .withField("partial_het_df", Schema.FieldType.INT64)
                .withField("partial_het_pValue", Schema.FieldType.FLOAT64)
                .withField("partial_het_gain", Schema.FieldType.FLOAT64)
                .withField("partial_het_levels", Schema.FieldType.INT64)
                .withField("partial_periods_agree", Schema.FieldType.INT64)
                .withField("partial_n_periods", Schema.FieldType.INT64)
                .withField("partial_period_z", Schema.FieldType.array(Schema.FieldType.element(period)))
                .withField("threshold", Schema.FieldType.FLOAT64)
                .withField("passed", Schema.FieldType.BOOLEAN)
                .withField("leakSuspect", Schema.FieldType.BOOLEAN)
                .withField("het_passed", Schema.FieldType.BOOLEAN)
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
                .withField("heterogeneity", Schema.FieldType.STRING)
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
                .withField("nHetMixedUnits", Schema.FieldType.INT64)
                .withField("nCandidates", Schema.FieldType.INT64)
                .withField("nTransforms", Schema.FieldType.INT64)
                .withField("nScored", Schema.FieldType.INT64)
                .withField("nPassed", Schema.FieldType.INT64)
                .withField("nPlacebo", Schema.FieldType.INT64)
                .withField("nLeakSuspect", Schema.FieldType.INT64)
                .withField("nHetPassed", Schema.FieldType.INT64)
                .withField("hetPassedColumns", Schema.FieldType.array(Schema.FieldType.STRING))
                .withField("nSuggestions", Schema.FieldType.INT64)
                .withField("nJointColumns", Schema.FieldType.INT64)
                .withField("nJointUnits", Schema.FieldType.INT64)
                .withField("nJointFilled", Schema.FieldType.INT64)
                .withField("nJointDropped", Schema.FieldType.INT64)
                .withField("nCategoricals", Schema.FieldType.INT64)
                .withField("nPairs", Schema.FieldType.INT64)
                .withField("nPairsPassed", Schema.FieldType.INT64)
                .withField("passedPairs", Schema.FieldType.array(Schema.FieldType.STRING))
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
        if (spec.hasHeterogeneity()) parts.add("heterogeneity=" + spec.heterogeneityLabel());
        if (spec.hasPairs()) parts.add("pairs=" + spec.pairs.size() + " (+" + spec.pairPlacebos.size() + " placebo pairs)" + (spec.hasPairShape() ? " shape=" + spec.pairShapeBins + "x" + spec.pairShapeBins : ""));
        if (spec.jointOn) parts.add("joint=" + spec.jointColumns.size() + "+" + spec.jointNoiseCount() + " columns directions=" + spec.jointDirections + " redundancy=" + spec.jointRedundancy + " select=" + spec.jointSelect + " pairs=" + spec.jointPairs + " excess=" + spec.jointExcess);
        if (spec.hasCategoricals()) parts.add("categorical=" + spec.categoricals.size() + " " + spec.categoricals + " maxLevels=" + spec.categoricalMaxLevels + " placebo=" + spec.categoricalPlacebo);
        parts.add("placebo=noise:" + spec.noise + (spec.hasShuffle() ? " shuffle:" + spec.shuffleN + "(" + spec.shuffleField + ")" : "") + " q" + spec.quantile + " seed=" + spec.seed);
        if (spec.periodsBucket != null) parts.add("periods=" + spec.periodsField + "/" + spec.periodsBucket);
        if (spec.minPeriodsAgree != null || spec.minGain != null) parts.add("pass=" + passRule(spec, spec.hasConditioning()));
        if (spec.leakZ != null) parts.add("leakZ=" + spec.leakZ + (spec.leakOnPartial() ? " on=partial" : ""));
        if (spec.hasConditioning()) parts.add("conditioning=" + spec.conditioningFields.size() + " " + spec.conditioningFields + " l2=" + spec.conditioningL2 + " maxIter=" + spec.conditioningMaxIter + " missing=" + spec.conditioningMissing + " (" + spec.conditioningMaxIter + " + 2 passes)");
        if (!spec.notes.isEmpty()) parts.add("notes=" + spec.notes);
        return "screen " + String.join(" ", parts);
    }
}
