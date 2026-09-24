package com.mercari.solution.util.pipeline.screen;

import com.mercari.solution.util.pipeline.feature.FeatureValues;
import com.mercari.solution.util.pipeline.glm.Baselines;
import com.mercari.solution.util.pipeline.glm.Baselines.Skip;
import com.mercari.solution.util.pipeline.glm.StatMath;
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
 * identity) and every random draw is seeded from the spec seed and the unit key. {@link #prepare} and
 * {@link #columns} are shared with the conditioning passes ({@link ConditioningScorer}).
 */
public final class GroupScorer implements Serializable {

    private static final String SEP = String.valueOf((char) 0);

    private final ScreenSpec spec;
    private final int nCandidates;
    private final int nColumns;
    private final int shuffleRef;
    /** {@code baseline.invalid: dropRow}: the invalid rows leave their unit before it is prepared */
    private final boolean dropsRows;

    public GroupScorer(final ScreenSpec spec) {
        this.spec = spec;
        this.nCandidates = spec.candidates.size();
        this.nColumns = spec.columnCount();
        this.shuffleRef = spec.hasShuffle() ? spec.shuffleIndex() : -1;
        this.dropsRows = spec.baselineDropsRows();
    }

    /** A prepared unit: rows sorted by (time, identity), baseline probabilities, normalised labels, weights. */
    public static final class Unit {
        public final List<ScreenRow> rows;
        public final String key;
        public final Skip skip;
        /** baseline probability per row (grouped: shares summing to 1; binomial: clamped probabilities; zeros in prior mode) */
        public final double[] p;
        public final double[] y;
        public final double[] w;
        /** unit weight (the row mean) for the grouped family */
        public final double unitWeight;
        /** rows removed before scoring by {@code baseline.invalid: dropRow} (not in {@link #rows}, except on a unit that lost every row: skipped, its rows are the dropped ones) */
        public final int dropped;

        Unit(final List<ScreenRow> rows, final String key, final Skip skip, final double[] p, final double[] y, final double[] w, final double unitWeight, final int dropped) {
            this.rows = rows;
            this.key = key;
            this.skip = skip;
            this.p = p;
            this.y = y;
            this.w = w;
            this.unitWeight = unitWeight;
            this.dropped = dropped;
        }

        public int size() {
            return rows.size();
        }

        public String period() {
            return rows.get(0).period;
        }
    }

    /**
     * Sorts the rows, removes the rows an invalid baseline value drops ({@code baseline.invalid: dropRow}) and
     * derives p / ỹ / w over the rest; {@code skip} says why the unit cannot be scored.
     */
    public Unit prepare(final List<ScreenRow> input, final String unitKey) {
        final List<ScreenRow> sorted = new ArrayList<>(input);
        sorted.sort(Comparator.comparingLong(ScreenRow::getTime).thenComparing(ScreenRow::getIdentity));
        final List<ScreenRow> rows;
        int dropped = 0;
        if (dropsRows) {
            rows = new ArrayList<>(sorted.size());
            for (final ScreenRow r : sorted) {
                if (Baselines.validRow(spec.baselineForm, r.baseline)) rows.add(r);
                else dropped++;
            }
            if (rows.isEmpty()) return new Unit(sorted, unitKey, Skip.INVALID_BASELINE, new double[sorted.size()], null, null, 0, dropped);
        } else {
            rows = sorted;
        }
        final int n = rows.size();
        final double[] p = new double[n];
        if (spec.hasBaseline()) {
            // Baselines.means reads each baseline before writing the mean at the same index, so p carries both
            for (int i = 0; i < n; i++) p[i] = rows.get(i).baseline;
            final Skip skip = Baselines.means(spec.family(), spec.baselineForm, p, p);
            if (skip != Skip.NONE) return new Unit(rows, unitKey, skip, p, null, null, 0, dropped);
        } else if (spec.isGroupedMultinomial()) {
            Arrays.fill(p, 1d / n);
        }
        final double[] y = new double[n];
        for (int i = 0; i < n; i++) y[i] = rows.get(i).label;
        final Skip labels = Baselines.normalizeLabels(spec.family(), spec.normalizeTies, y);
        if (labels != Skip.NONE) return new Unit(rows, unitKey, labels, p, y, null, 0, dropped);
        final double[] w = new double[n];
        double wsum = 0;
        for (int i = 0; i < n; i++) {
            w[i] = rows.get(i).weight;
            wsum += w[i];
        }
        return new Unit(rows, unitKey, Skip.NONE, p, y, w, wsum / n, dropped);
    }

    /**
     * Adds the unit's contributions into {@code into} (created on demand). Returns why the unit was skipped,
     * {@link Skip#NONE} when it was scored.
     */
    public Skip score(final List<ScreenRow> input, final String unitKey, final Map<Integer, ScoreAccumulator> into) {
        final Unit unit = prepare(input, unitKey);
        final ScoreAccumulator book = into.computeIfAbsent(ScoreAccumulator.BOOKKEEPING_KEY, k -> new ScoreAccumulator());
        final double[] bookSlots = new double[ScoreAccumulator.SLOTS];
        bookSlots[ScoreAccumulator.ROWS_DROPPED] = unit.dropped;
        if (unit.skip != Skip.NONE) {
            // same unit as UNITS_SCORED: groups for the grouped family, rows for binomial
            final double units = spec.isGroupedMultinomial() ? 1 : unit.size();
            bookSlots[ScoreAccumulator.UNITS_SKIPPED] = units;
            if (unit.skip == Skip.INVALID_BASELINE) bookSlots[ScoreAccumulator.UNITS_SKIPPED_BASELINE] = units;
            book.add(null, bookSlots);
            return unit.skip;
        }
        final int n = unit.size();
        final boolean prior = !spec.hasBaseline();
        final double[][] cols = columns(unit);
        final String unitPeriod = unit.period();
        final int nTransforms = spec.transforms.size();
        final double[] contribution = new double[ScoreAccumulator.SLOTS];
        for (int c = 0; c < nColumns; c++) {
            for (int t = 0; t < nTransforms; t++) {
                final double[] v = transform(spec.transforms.get(t), cols[c]);
                final ScoreAccumulator acc = into.computeIfAbsent(spec.key(c, t), k -> new ScoreAccumulator());
                if (spec.isGroupedMultinomial()) {
                    groupedContribution(v, unit.y, unit.p, unit.unitWeight, contribution);
                    acc.add(unitPeriod, contribution);
                } else {
                    rowContributions(unit.rows, v, unit.y, unit.p, unit.w, prior, acc);
                }
            }
        }
        bookSlots[ScoreAccumulator.UNITS_SCORED] = spec.isGroupedMultinomial() ? 1 : n;
        bookSlots[ScoreAccumulator.ROWS_SCORED] = n;
        book.add(null, bookSlots);
        for (final ScreenRow r : unit.rows) if (r.time != ScreenRow.NO_TIME) book.time(r.time);
        return Skip.NONE;
    }

    /** The unit's columns in key order: candidates, noise placebos (deterministic), shuffle placebos. */
    public double[][] columns(final Unit unit) {
        final int n = unit.size();
        final double[][] cols = new double[nColumns][n];
        for (int i = 0; i < n; i++) {
            final double[] x = unit.rows.get(i).x;
            for (int c = 0; c < nCandidates; c++) cols[c][i] = x[c];
        }
        int next = nCandidates;
        if (spec.noise > 0) {
            final SplittableRandom rng = FeatureValues.seededRandom(spec.seed, unit.key + SEP + "noise");
            for (int i = 0; i < n; i++) {
                for (int j = 0; j < spec.noise; j++) cols[next + j][i] = rng.nextGaussian();
            }
            next += spec.noise;
        }
        if (shuffleRef >= 0) {
            final double[] ref = new double[n];
            for (int i = 0; i < n; i++) ref[i] = unit.rows.get(i).x[shuffleRef];
            for (int j = 0; j < spec.shuffleN; j++) {
                final SplittableRandom rng = FeatureValues.seededRandom(spec.seed, unit.key + SEP + "shuffle" + j);
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
        return cols;
    }

    /**
     * Grouped multinomial (conditional logit) contribution: x centred by the p-weighted mean over the observed
     * rows (a missing row contributes nothing), S = Σ x̃ (ỹ − p), H = Σ p x̃² − (Σ p x̃)², both scaled by the
     * unit weight. The values are shifted by the unit's {@link #pivot} first, so a column constant within the
     * unit gives H = 0 exactly rather than the rounding residue of centring large values.
     */
    static void groupedContribution(final double[] v, final double[] y, final double[] p, final double weight, final double[] out) {
        final int n = v.length;
        final double pivot = pivot(v);
        double pm = 0, psum = 0;
        int nObs = 0;
        for (int i = 0; i < n; i++) {
            if (StatMath.isFinite(v[i])) {
                pm += p[i] * (v[i] - pivot);
                psum += p[i];
                nObs++;
            }
        }
        final double mean = psum > 0 ? pm / psum : 0d;
        double s = 0, h = 0, px = 0;
        for (int i = 0; i < n; i++) {
            if (!StatMath.isFinite(v[i])) continue;
            final double xt = v[i] - pivot - mean;
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
     * The shift applied before a column is centred within a unit: its first finite value (0 when none). The
     * grouped statistic is invariant to a constant shift within the unit, and {@code v − pivot} is exact for
     * values within a factor of two of each other (Sterbenz), so the centring works on the column's spread
     * rather than its magnitude: a within-unit constant gives exactly zero, a large-valued column with a small
     * spread keeps every digit of that spread. Shared by the marginal test and {@link ConditioningScorer#partial}.
     */
    static double pivot(final double[] v) {
        for (final double x : v) if (StatMath.isFinite(x)) return x;
        return 0d;
    }

    /**
     * Row-family contributions (binomial / gaussian / poisson), one per row (its own period), as moment sums that
     * the report centres at the end: offset mode (a baseline μ) c1 = Σ w x r, c2 = Σ w r, c3 = Σ w v x², c4 = Σ w v x,
     * c5 = Σ w v, c6 = Σ w r² with r = y − μ and the Fisher weight v = μ(1 − μ) (binomial), μ (poisson), 1 (gaussian);
     * prior mode (no baseline) the raw moments c1 = Σ w x y, c2 = Σ w y, c3 = Σ w x², c4 = Σ w x, c5 = Σ w, c6 = Σ w y²
     * (the report supplies the prior-rate weight and, for gaussian, the variance).
     */
    private void rowContributions(final List<ScreenRow> rows, final double[] v, final double[] y, final double[] mu,
                                  final double[] w, final boolean prior, final ScoreAccumulator acc) {
        final Map<String, double[]> byPeriod = new HashMap<>();
        for (int i = 0; i < v.length; i++) {
            if (!StatMath.isFinite(v[i])) continue;
            final double[] c = byPeriod.computeIfAbsent(rows.get(i).period, k -> new double[ScoreAccumulator.SLOTS]);
            final double x = v[i];
            c[ScoreAccumulator.N_OBS] += 1;
            if (prior) {
                c[ScoreAccumulator.C1] += w[i] * x * y[i];
                c[ScoreAccumulator.C2] += w[i] * y[i];
                c[ScoreAccumulator.C3] += w[i] * x * x;
                c[ScoreAccumulator.C4] += w[i] * x;
                c[ScoreAccumulator.C5] += w[i];
                c[ScoreAccumulator.C6] += w[i] * y[i] * y[i];
            } else {
                final double r = y[i] - mu[i];
                final double vv = spec.fisherWeight(mu[i]);
                c[ScoreAccumulator.C1] += w[i] * x * r;
                c[ScoreAccumulator.C2] += w[i] * r;
                c[ScoreAccumulator.C3] += w[i] * vv * x * x;
                c[ScoreAccumulator.C4] += w[i] * vv * x;
                c[ScoreAccumulator.C5] += w[i] * vv;
                c[ScoreAccumulator.C6] += w[i] * r * r;
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
                final double median = StatMath.medianFinite(v);
                final double[] out = new double[v.length];
                for (int i = 0; i < v.length; i++) out[i] = StatMath.isFinite(v[i]) ? Math.abs(v[i] - median) : Double.NaN;
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
        for (int i = 0; i < n; i++) if (StatMath.isFinite(v[i])) idx[m++] = i;
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
