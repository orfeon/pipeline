package com.mercari.solution.util.pipeline.screen;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;

/**
 * Scores one unit (a group, or a single independent row) against every column x transform: builds the
 * placebo columns, applies the transforms, and adds the score-test contribution of the unit into the
 * accumulator map keyed by {@link ScreenSpec#key}. Pure and deterministic: the rows are sorted by (time,
 * identity) and every random draw is seeded from the spec seed and the unit key.
 */
public final class GroupScorer implements Serializable {

    private static final String SEP = String.valueOf((char) 0);
    private static final double EPS = 1e-12;

    private final ScreenSpec spec;
    private final int nCandidates;
    private final int nColumns;
    private final int shuffleRef;

    public GroupScorer(final ScreenSpec spec) {
        this.spec = spec;
        this.nCandidates = spec.candidates.size();
        this.nColumns = spec.columnCount();
        this.shuffleRef = spec.hasShuffle() ? nCandidates : -1;
    }

    /** Skip reasons of a unit, counted in the bookkeeping accumulator. */
    public enum Skip { NONE, NO_POSITIVE_LABEL, INVALID_BASELINE }

    /**
     * Adds the unit's contributions into {@code into} (created on demand). Returns why the unit was skipped,
     * {@link Skip#NONE} when it was scored.
     */
    public Skip score(final List<ScreenRow> input, final String unitKey, final Map<Integer, ScoreAccumulator> into) {
        final List<ScreenRow> rows = new ArrayList<>(input);
        rows.sort(Comparator.comparingLong(ScreenRow::getTime).thenComparing(ScreenRow::getIdentity));
        final int n = rows.size();
        final ScoreAccumulator book = into.computeIfAbsent(ScoreAccumulator.BOOKKEEPING_KEY, k -> new ScoreAccumulator());
        final double[] bookSlots = new double[ScoreAccumulator.SLOTS];

        // baseline probabilities
        final double[] p = new double[n];
        final boolean prior = !spec.hasBaseline();
        if (!prior) {
            final Skip skip = probabilities(rows, p);
            if (skip != Skip.NONE) {
                bookSlots[ScoreAccumulator.UNITS_SKIPPED] = 1;
                book.add(null, bookSlots);
                return skip;
            }
        } else if (spec.isGroupedMultinomial()) {
            Arrays.fill(p, 1d / n);
        }

        // labels
        final double[] y = new double[n];
        for (int i = 0; i < n; i++) y[i] = rows.get(i).label;
        if (spec.isGroupedMultinomial()) {
            double sum = 0;
            for (final double v : y) sum += v;
            if (!(sum > 0)) {
                bookSlots[ScoreAccumulator.UNITS_SKIPPED] = 1;
                book.add(null, bookSlots);
                return Skip.NO_POSITIVE_LABEL;
            }
            if (spec.normalizeTies) for (int i = 0; i < n; i++) y[i] /= sum;
        }

        // weights: per row for binomial, the unit mean for the grouped family
        final double[] w = new double[n];
        double wsum = 0;
        for (int i = 0; i < n; i++) {
            w[i] = rows.get(i).weight;
            wsum += w[i];
        }
        final double unitWeight = wsum / n;

        // columns: candidates, noise placebos, shuffle placebos
        final double[][] cols = new double[nColumns][n];
        for (int i = 0; i < n; i++) {
            final double[] x = rows.get(i).x;
            for (int c = 0; c < nCandidates; c++) cols[c][i] = x[c];
        }
        int next = nCandidates;
        if (spec.noise > 0) {
            final SplittableRandom rng = ScreenMath.seededRandom(spec.seed, unitKey + SEP + "noise");
            for (int i = 0; i < n; i++) {
                for (int j = 0; j < spec.noise; j++) cols[next + j][i] = rng.nextGaussian();
            }
            next += spec.noise;
        }
        if (shuffleRef >= 0) {
            final double[] ref = new double[n];
            for (int i = 0; i < n; i++) ref[i] = rows.get(i).x[shuffleRef];
            for (int j = 0; j < spec.shuffleN; j++) {
                final SplittableRandom rng = ScreenMath.seededRandom(spec.seed, unitKey + SEP + "shuffle" + j);
                final int[] perm = new int[n];
                for (int i = 0; i < n; i++) perm[i] = i;
                for (int i = n - 1; i > 0; i--) {
                    final int k = rng.nextInt(i + 1);
                    final int t = perm[i];
                    perm[i] = perm[k];
                    perm[k] = t;
                }
                for (int i = 0; i < n; i++) cols[next + j][i] = ref[perm[i]];
            }
        }

        final String unitPeriod = rows.get(0).period;
        final int nTransforms = spec.transforms.size();
        final double[] contribution = new double[ScoreAccumulator.SLOTS];
        for (int c = 0; c < nColumns; c++) {
            for (int t = 0; t < nTransforms; t++) {
                final double[] v = transform(spec.transforms.get(t), cols[c]);
                final ScoreAccumulator acc = into.computeIfAbsent(spec.key(c, t), k -> new ScoreAccumulator());
                if (spec.isGroupedMultinomial()) {
                    groupedContribution(v, y, p, unitWeight, contribution);
                    acc.add(unitPeriod, contribution);
                } else {
                    binomialContributions(rows, v, y, p, w, prior, acc);
                }
            }
        }
        bookSlots[ScoreAccumulator.UNITS_SCORED] = spec.isGroupedMultinomial() ? 1 : n;
        bookSlots[ScoreAccumulator.ROWS_SCORED] = n;
        book.add(null, bookSlots);
        for (final ScreenRow r : rows) book.time(r.time);
        return Skip.NONE;
    }

    /** Fills {@code p} from the baselines; NONE when every row is usable. */
    private Skip probabilities(final List<ScreenRow> rows, final double[] p) {
        final int n = rows.size();
        final String form = spec.baselineForm;
        double max = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < n; i++) {
            final double b = rows.get(i).baseline;
            if (Double.isNaN(b) || Double.isInfinite(b)) return Skip.INVALID_BASELINE;
            switch (form) {
                case ScreenSpec.FORM_PROB -> {
                    if (b < 0 || b > 1) return Skip.INVALID_BASELINE;
                    p[i] = b;
                }
                case ScreenSpec.FORM_LOG_PROB -> {
                    if (b > 0) return Skip.INVALID_BASELINE;
                    p[i] = b;
                    if (b > max) max = b;
                }
                case ScreenSpec.FORM_INVERSE_SHARE -> {
                    if (!(b > 0)) return Skip.INVALID_BASELINE;
                    p[i] = 1d / b;
                }
                default -> throw new IllegalStateException("unknown baseline form " + form);
            }
        }
        if (ScreenSpec.FORM_LOG_PROB.equals(form)) {
            for (int i = 0; i < n; i++) p[i] = Math.exp(p[i] - (spec.isGroupedMultinomial() ? max : 0d));
        }
        if (spec.isGroupedMultinomial() || ScreenSpec.FORM_INVERSE_SHARE.equals(form)) {
            double sum = 0;
            for (final double v : p) sum += v;
            if (!(sum > 0)) return Skip.INVALID_BASELINE;
            for (int i = 0; i < n; i++) p[i] /= sum;
        }
        if (!spec.isGroupedMultinomial()) {
            for (int i = 0; i < n; i++) p[i] = Math.min(1 - EPS, Math.max(EPS, p[i]));
        }
        return Skip.NONE;
    }

    /**
     * Grouped multinomial (conditional logit) contribution: x centred by the p-weighted mean over the observed
     * rows (missing → 0), S = Σ x̃ (ỹ − p), H = Σ p x̃² − (Σ p x̃)², both scaled by the unit weight.
     */
    static void groupedContribution(final double[] v, final double[] y, final double[] p, final double weight, final double[] out) {
        final int n = v.length;
        double pm = 0, psum = 0;
        int nObs = 0;
        for (int i = 0; i < n; i++) {
            if (ScreenMath.isFinite(v[i])) {
                pm += p[i] * v[i];
                psum += p[i];
                nObs++;
            }
        }
        final double mean = psum > 0 ? pm / psum : 0d;
        double s = 0, h = 0, px = 0;
        for (int i = 0; i < n; i++) {
            final double xt = ScreenMath.isFinite(v[i]) ? v[i] - mean : 0d;
            s += xt * (y[i] - p[i]);
            h += p[i] * xt * xt;
            px += p[i] * xt;
        }
        Arrays.fill(out, 0d);
        out[ScoreAccumulator.S] = weight * s;
        out[ScoreAccumulator.H] = weight * (h - px * px);
        out[ScoreAccumulator.N_OBS] = nObs;
    }

    /**
     * Binomial contributions, one per row (its own period), as moment sums that the report centres at the end:
     * offset mode (a baseline) c1 = Σ w x r, c2 = Σ w r, c3 = Σ w v x², c4 = Σ w v x, c5 = Σ w v with r = y − p,
     * v = p(1 − p); prior mode (no baseline) c1 = Σ w x y, c2 = Σ w y, c3 = Σ w x², c4 = Σ w x, c5 = Σ w.
     */
    private static void binomialContributions(final List<ScreenRow> rows, final double[] v, final double[] y, final double[] p,
                                              final double[] w, final boolean prior, final ScoreAccumulator acc) {
        final Map<String, double[]> byPeriod = new HashMap<>();
        for (int i = 0; i < v.length; i++) {
            if (!ScreenMath.isFinite(v[i])) continue;
            final double[] c = byPeriod.computeIfAbsent(rows.get(i).period, k -> new double[ScoreAccumulator.SLOTS]);
            final double x = v[i];
            c[ScoreAccumulator.N_OBS] += 1;
            if (prior) {
                c[ScoreAccumulator.C1] += w[i] * x * y[i];
                c[ScoreAccumulator.C2] += w[i] * y[i];
                c[ScoreAccumulator.C3] += w[i] * x * x;
                c[ScoreAccumulator.C4] += w[i] * x;
                c[ScoreAccumulator.C5] += w[i];
            } else {
                final double r = y[i] - p[i];
                final double vv = p[i] * (1 - p[i]);
                c[ScoreAccumulator.C1] += w[i] * x * r;
                c[ScoreAccumulator.C2] += w[i] * r;
                c[ScoreAccumulator.C3] += w[i] * vv * x * x;
                c[ScoreAccumulator.C4] += w[i] * vv * x;
                c[ScoreAccumulator.C5] += w[i] * vv;
            }
        }
        for (final Map.Entry<String, double[]> e : byPeriod.entrySet()) acc.add(e.getKey(), e.getValue());
    }

    /** Applies a transform variant within the unit; NaN inputs stay NaN. */
    static double[] transform(final String transform, final double[] v) {
        switch (transform) {
            case ScreenSpec.TRANSFORM_RAW -> {
                return v;
            }
            case ScreenSpec.TRANSFORM_RANK -> {
                return percentileRank(v);
            }
            case ScreenSpec.TRANSFORM_ABSDEV -> {
                final double median = ScreenMath.medianFinite(v);
                final double[] out = new double[v.length];
                for (int i = 0; i < v.length; i++) out[i] = ScreenMath.isFinite(v[i]) ? Math.abs(v[i] - median) : Double.NaN;
                return out;
            }
            default -> throw new IllegalArgumentException("unknown transform " + transform);
        }
    }

    /**
     * Percentile rank within the unit over the finite values: (number of smaller values + half the ties) /
     * (finite count − 1), in [0, 1]; 0.5 when only one value is finite.
     */
    static double[] percentileRank(final double[] v) {
        final int n = v.length;
        final double[] out = new double[n];
        Arrays.fill(out, Double.NaN);
        final Integer[] idx = new Integer[n];
        int m = 0;
        for (int i = 0; i < n; i++) if (ScreenMath.isFinite(v[i])) idx[m++] = i;
        if (m == 0) return out;
        if (m == 1) {
            out[idx[0]] = 0.5;
            return out;
        }
        final Integer[] order = Arrays.copyOf(idx, m);
        Arrays.sort(order, Comparator.comparingDouble(i -> v[i]));
        int i = 0;
        while (i < m) {
            int j = i;
            while (j + 1 < m && v[order[j + 1]] == v[order[i]]) j++;
            // positions i..j share the value: smaller = i, ties (others) = j - i
            final double rank = (i + 0.5 * (j - i)) / (m - 1);
            for (int k = i; k <= j; k++) out[order[k]] = rank;
            i = j + 1;
        }
        return out;
    }
}
