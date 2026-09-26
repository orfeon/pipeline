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
import java.util.function.IntFunction;

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
        final int nb = spec.binCount();
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
        // G Γ (k × B) once: Γ'GΓ is then O(B² k) instead of O(B² k²)
        final double[][] gGamma = new double[k][nb];
        for (int j = 0; j < k; j++) {
            for (int l = 0; l < k; l++) {
                final double gjl = fit.bestG[j][l];
                if (gjl == 0d) continue;
                for (int c = 0; c < nb; c++) gGamma[j][c] += gjl * gamma[l][c];
            }
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
                    ggg += gamma[j][b] * gGamma[j][c];
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

    /** The bins' geometry the suggestions read: a representative value per value bin, and the k − 1 edges (null for position bins). */
    public record Bins(IntFunction<double[]> representatives, IntFunction<double[]> edges) {}

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
                                                 final double nUnits, final Bins bins) {
        final List<Map<String, Object>> out = new ArrayList<>();
        if (!spec.suggestionsOn || bins == null) return out;
        final int t = spec.transforms.indexOf(ScreenSpec.TRANSFORM_BINNED);
        if (t < 0) return out;
        final List<String> names = spec.columnNames();
        final int nb = spec.binCount();
        final int k = spec.binsK;
        final Map<String, List<Double>> placeboGains = new LinkedHashMap<>();
        final List<Map<String, Object>> candidates = new ArrayList<>();
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
            final Block bd = binnedStats(spec, disc, nDisc, nObs);
            final Block bc = binnedStats(spec, conf, nConf, nObs);
            if (bd.stats.degenerate || bc.stats.degenerate) continue;
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
                            position ? "the rows above rank " + fmt(bestStep.cut) + " within the unit" : "{scope: row, type: bin, input: " + name + ", edges: [" + fmt(bestStep.cut) + "]}", placeboGains));
                }
                // monotone: the isotonic fit of the bin effects (H-weighted) in the better direction
                final double[] effects = new double[k], weights = new double[k];
                for (int b = 0; b < k; b++) {
                    weights[b] = bd.h[b];
                    effects[b] = bd.h[b] > 0 ? bd.s[b] / bd.h[b] : 0d;
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
            // missingness: the missing bin against the rest, and the value bin whose effect matches it
            if (bd.h[k] > 0 && bc.h[k] > 0) {
                final double[] phiMiss = new double[nb];
                phiMiss[k] = 1;
                final double chiDisc = contrastChi2(bd, phiMiss, nb);
                final double blockAllDisc = contrastBound(bd, nb), blockAllConf = contrastBound(bc, nb);
                final double missEffect = bd.s[k] / bd.h[k];
                double fill = Double.NaN, gap = Double.POSITIVE_INFINITY;
                for (int b = 0; b < k; b++) {
                    if (!(bd.h[b] > 0) || x == null) continue;
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
            final double cut = spec.gainCut(thresholds.get((String) s.get("kind")));
            final double gain = (Double) s.get("confirmation_gain");
            s.remove("_nConf");
            s.put("threshold", cut);
            s.put("passed", !(Boolean) s.get("placebo") && !Double.isNaN(cut) && gain > cut);
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
        double hsum = 0, ssum = 0;
        for (int b = 0; b < over; b++) {
            if (!(block.h[b] > 0)) continue;
            hsum += block.h[b];
            ssum += block.s[b];
        }
        if (!(hsum > 0)) return 0d;
        // w: the centring weights; r = H1 and t = 1'H1 over the bins with information
        final double[] w = new double[over], r = new double[over];
        double t = 0;
        for (int b = 0; b < over; b++) {
            if (!(block.h[b] > 0)) continue;
            w[b] = block.h[b] / hsum;
            for (int c = 0; c < over; c++) if (block.h[c] > 0) r[b] += block.hFull[b][c];
            t += r[b];
        }
        final double[] s = new double[over], diag = new double[over];
        final double[][] h = new double[over][over];
        for (int b = 0; b < over; b++) {
            if (!(block.h[b] > 0)) continue;
            s[b] = block.s[b] - w[b] * ssum;
            for (int c = 0; c < over; c++) {
                if (block.h[c] > 0) h[b][c] = block.hFull[b][c] - r[b] * w[c] - w[b] * r[c] + w[b] * w[c] * t;
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
    private static double contrastChi2(final Block block, final double[] phi, final int over) {
        double hsum = 0, hphi = 0;
        for (int b = 0; b < over; b++) {
            if (!(block.h[b] > 0) || !Double.isFinite(phi[b])) continue;
            hsum += block.h[b];
            hphi += block.h[b] * phi[b];
        }
        if (!(hsum > 0)) return 0d;
        final double mean = hphi / hsum;
        final double[] pc = new double[over];
        for (int b = 0; b < over; b++) pc[b] = block.h[b] > 0 && Double.isFinite(phi[b]) ? phi[b] - mean : 0d;
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
        double hsum = 0, hphi = 0;
        for (int b = 0; b < over; b++) if (block.h[b] > 0) {
            hsum += block.h[b];
            hphi += block.h[b] * phi[b];
        }
        final double mean = hsum > 0 ? hphi / hsum : 0d;
        double s = 0;
        for (int b = 0; b < over; b++) if (block.h[b] > 0) s += (phi[b] - mean) * block.s[b];
        return s >= 0 ? "+" : "-";
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
     * @param bins     the bins' geometry for the suggestions (null = no suggestions)
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
        // [s, b, a], and blockPartial solves its Γ itself
        final Map<Integer, double[]> gammas;
        if (conditioned) {
            final Map<Integer, PartialAccumulator> scalar = new HashMap<>();
            for (final Map.Entry<Integer, PartialAccumulator> e : partials.entrySet()) {
                if (e.getKey() >= 0 && ScreenSpec.isBinned(spec.transforms.get(e.getKey() % nTransforms))) continue;
                scalar.put(e.getKey(), e.getValue());
            }
            gammas = gammas(scalar, fit, spec.conditioningL2);
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
            putHet(r, "", null);
            r.put("level_z", null);
            final PartialAccumulator pacc = conditioned ? partials.get(key) : null;
            final double[] vec = pacc == null || pacc.isEmpty() ? null : pacc.getTotal();
            final Partial pt = vec == null ? new Partial(Stats.degenerate((long) nUnits), Double.NaN)
                    : partial(vec, fit, nUnits, (long) nUnits, sigma2, gammas.get(key));
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
            r.put("partial_periods_agree", null);
            r.put("partial_n_periods", null);
            r.put("partial_period_z", null);
            effective.add(pst);
            effectiveAgree.add(null);
            effectiveHet.add(null);
            final boolean placebo = spec.isPlaceboPair(q);
            if (placebo) placeboGains.computeIfAbsent(ScreenSpec.KIND_PAIR, kind -> new ArrayList<>()).add(pst.degenerate ? 0d : pst.estGain);
            r.put("placebo", placebo);
            r.put("degenerate", pst.degenerate);
            records.add(r);
        }
        if (spec.hasPairs() && !conditioned) notes.add("pairs: no partial test (see the conditioning note), so the pair records are degenerate");

        // placebo threshold per statistic kind (the theoretical chi2(df) quantile when no placebo column is
        // configured): the df = 1 transforms pool one cut, the binned block test its own
        final Map<String, Double> thresholds = new LinkedHashMap<>();
        final Map<String, Double> thresholdsTheoretical = new LinkedHashMap<>();
        final List<String> kinds = new ArrayList<>(List.of(ScreenSpec.KIND_DF1));
        if (spec.hasBinned()) kinds.add(ScreenSpec.KIND_BINNED);
        if (spec.hasHeterogeneity()) kinds.add(ScreenSpec.KIND_HET);
        if (spec.hasPairs()) kinds.add(ScreenSpec.KIND_PAIR);
        // the heterogeneity test's nominal df: the most levels any record found usable, less one
        int hetDf = 1;
        for (final Het h : effectiveHet) if (h != null && !h.degenerate) hetDf = Math.max(hetDf, h.df);
        for (final String kind : kinds) {
            final int df = ScreenSpec.KIND_BINNED.equals(kind) ? spec.binsK - 1 : ScreenSpec.KIND_HET.equals(kind) ? hetDf : 1;
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
                final double hetCut = spec.gainCut(thresholds.get(ScreenSpec.KIND_HET));
                hetPassed = !placebo && !het.degenerate && !Double.isNaN(hetCut) && het.gain > hetCut;
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
        summary.put("notes", notes);
        final List<Map<String, Object>> suggested = suggestions(spec, accumulators, nUnits, bins);
        // the candidates' suggestions (placebo records excluded, as nScored)
        summary.put("nSuggestions", spec.suggestionsOn ? suggested.stream().filter(s -> !(Boolean) s.get("placebo")).count() : null);
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
                .withField("df", Schema.FieldType.INT64)
                .withField("pValue", Schema.FieldType.FLOAT64)
                .withField("qValue", Schema.FieldType.FLOAT64)
                .withField("n_groups", Schema.FieldType.INT64)
                .withField("n_obs", Schema.FieldType.INT64)
                .withField("periods_agree", Schema.FieldType.INT64)
                .withField("n_periods", Schema.FieldType.INT64)
                .withField("period_z", Schema.FieldType.array(Schema.FieldType.element(period)))
                .withField("bin_stats", Schema.FieldType.array(Schema.FieldType.element(bin)))
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
                .withField("nCandidates", Schema.FieldType.INT64)
                .withField("nTransforms", Schema.FieldType.INT64)
                .withField("nScored", Schema.FieldType.INT64)
                .withField("nPassed", Schema.FieldType.INT64)
                .withField("nPlacebo", Schema.FieldType.INT64)
                .withField("nLeakSuspect", Schema.FieldType.INT64)
                .withField("nHetPassed", Schema.FieldType.INT64)
                .withField("hetPassedColumns", Schema.FieldType.array(Schema.FieldType.STRING))
                .withField("nSuggestions", Schema.FieldType.INT64)
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
        if (spec.hasPairs()) parts.add("pairs=" + spec.pairs.size() + " (+" + spec.pairPlacebos.size() + " placebo pairs)");
        parts.add("placebo=noise:" + spec.noise + (spec.hasShuffle() ? " shuffle:" + spec.shuffleN + "(" + spec.shuffleField + ")" : "") + " q" + spec.quantile + " seed=" + spec.seed);
        if (spec.periodsBucket != null) parts.add("periods=" + spec.periodsField + "/" + spec.periodsBucket);
        if (spec.minPeriodsAgree != null || spec.minGain != null) parts.add("pass=" + passRule(spec, spec.hasConditioning()));
        if (spec.leakZ != null) parts.add("leakZ=" + spec.leakZ + (spec.leakOnPartial() ? " on=partial" : ""));
        if (spec.hasConditioning()) parts.add("conditioning=" + spec.conditioningFields.size() + " " + spec.conditioningFields + " l2=" + spec.conditioningL2 + " maxIter=" + spec.conditioningMaxIter + " missing=" + spec.conditioningMissing + " (" + spec.conditioningMaxIter + " + 2 passes)");
        if (!spec.notes.isEmpty()) parts.add("notes=" + spec.notes);
        return "screen " + String.join(" ", parts);
    }
}
