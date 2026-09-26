package com.mercari.solution.util.pipeline.screen;

import com.mercari.solution.util.domain.math.NormalDistribution;
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
    /** the window's quantile sketches, set per bundle from the side input: the rank / absdev reference of independent rows and the binned test's value edges */
    private transient WindowQuantiles quantiles;
    /** the binned test's value edges per column, derived once from the sketches (reset with them) */
    private transient double[][] edgesCache;

    public GroupScorer(final ScreenSpec spec) {
        this.spec = spec;
        this.nCandidates = spec.candidates.size();
        this.nColumns = spec.columnCount();
        this.shuffleRef = spec.hasShuffle() ? spec.shuffleIndex() : -1;
        this.dropsRows = spec.baselineDropsRows();
    }

    /** Sets the window's quantile sketches (the rank / absdev reference of independent rows); null = within-unit transforms. */
    public GroupScorer withWindowQuantiles(final WindowQuantiles quantiles) {
        if (this.quantiles != quantiles) edgesCache = null;
        this.quantiles = quantiles;
        return this;
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

        /** The heterogeneity modifier's level of a grouped unit: its first row's (the modifier is a unit-level field). */
        public String level() {
            return rows.get(0).level;
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
        requireWindowQuantiles(spec, quantiles);
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
        final String unitLevel = spec.isGroupedMultinomial() ? unit.level() : null;
        final int nTransforms = spec.transforms.size();
        final double[] contribution = new double[ScoreAccumulator.SLOTS];
        for (int c = 0; c < nColumns; c++) {
            for (int t = 0; t < nTransforms; t++) {
                final ScoreAccumulator acc = into.computeIfAbsent(spec.key(c, t), k -> new ScoreAccumulator());
                if (ScreenSpec.isBinned(spec.transforms.get(t))) {
                    // the binned block test: per-bin sums in the accumulator's variable-length vector, added in
                    // place over the occupied bins only; no period slices (the block has no sign to agree on)
                    final int[] bins = bins(c, cols[c]);
                    Arrays.fill(contribution, 0d);
                    contribution[ScoreAccumulator.N_OBS] = observed(cols[c]);
                    acc.add(null, contribution);
                    if (spec.isGroupedMultinomial()) {
                        binnedGroupedContribution(bins, unit.y, unit.p, unit.unitWeight, acc.extra(binnedGroupedLength()));
                    } else {
                        binnedRowContribution(bins, unit.y, unit.p, unit.w, prior, acc.extra(binnedRowLength()));
                    }
                    continue;
                }
                final double[] v = transform(spec, quantiles, c, spec.transforms.get(t), cols[c]);
                if (spec.isGroupedMultinomial()) {
                    groupedContribution(v, unit.y, unit.p, unit.unitWeight, contribution);
                    acc.add(unitPeriod, contribution);
                    if (unitLevel != null) acc.addSlice(ScoreAccumulator.LEVEL_PREFIX + unitLevel, contribution);
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
        // the heterogeneity modifier's levels: the same sums per level, added as slices (the total holds the row once)
        final Map<String, double[]> byLevel = new HashMap<>();
        final double[] d = new double[ScoreAccumulator.SLOTS];
        for (int i = 0; i < v.length; i++) {
            if (!StatMath.isFinite(v[i])) continue;
            final double x = v[i];
            d[ScoreAccumulator.N_OBS] = 1;
            if (prior) {
                d[ScoreAccumulator.C1] = w[i] * x * y[i];
                d[ScoreAccumulator.C2] = w[i] * y[i];
                d[ScoreAccumulator.C3] = w[i] * x * x;
                d[ScoreAccumulator.C4] = w[i] * x;
                d[ScoreAccumulator.C5] = w[i];
                d[ScoreAccumulator.C6] = w[i] * y[i] * y[i];
            } else {
                final double r = y[i] - mu[i];
                final double vv = spec.fisherWeight(mu[i]);
                d[ScoreAccumulator.C1] = w[i] * x * r;
                d[ScoreAccumulator.C2] = w[i] * r;
                d[ScoreAccumulator.C3] = w[i] * vv * x * x;
                d[ScoreAccumulator.C4] = w[i] * vv * x;
                d[ScoreAccumulator.C5] = w[i] * vv;
                d[ScoreAccumulator.C6] = w[i] * r * r;
            }
            final double[] c = byPeriod.computeIfAbsent(rows.get(i).period, k -> new double[ScoreAccumulator.SLOTS]);
            for (int s = 0; s < ScoreAccumulator.SLOTS; s++) c[s] += d[s];
            final String level = rows.get(i).level;
            if (level != null) {
                final double[] cl = byLevel.computeIfAbsent(level, k -> new double[ScoreAccumulator.SLOTS]);
                for (int s = 0; s < ScoreAccumulator.SLOTS; s++) cl[s] += d[s];
            }
        }
        for (final Map.Entry<String, double[]> e : byPeriod.entrySet()) acc.add(e.getKey(), e.getValue());
        for (final Map.Entry<String, double[]> e : byLevel.entrySet()) acc.addSlice(ScoreAccumulator.LEVEL_PREFIX + e.getKey(), e.getValue());
    }

    /** Finite values of a column (the binned test's n_obs). */
    static int observed(final double[] v) {
        int n = 0;
        for (final double x : v) if (StatMath.isFinite(x)) n++;
        return n;
    }

    /**
     * The bin of every value of column {@code column} (DSL doc §6.1): value bins from the window's quantile
     * edges (a candidate or the shuffle reference's sketch; a noise placebo takes the exact normal quantiles),
     * or position bins from the within-unit rank ({@code bins.edges: rank}); a missing value goes to the missing
     * bin (the last index). Bin i holds the values in (edge_{i−1}, edge_i]: a value equal to an edge falls below it.
     */
    int[] bins(final int column, final double[] v) {
        final int k = spec.binsK;
        final int[] out = new int[v.length];
        if (ScreenSpec.EDGES_RANK.equals(spec.binsEdges)) {
            final double[] rank = percentileRank(v);
            for (int i = 0; i < v.length; i++) out[i] = StatMath.isFinite(rank[i]) ? Math.min(k - 1, (int) Math.floor(rank[i] * k)) : spec.missingBin();
            return out;
        }
        final double[] edges = edges(column);
        for (int i = 0; i < v.length; i++) {
            if (!StatMath.isFinite(v[i]) || edges == null) {
                out[i] = spec.missingBin();
                continue;
            }
            int b = 0;
            while (b < edges.length && edges[b] < v[i]) b++;
            out[i] = b;
        }
        return out;
    }

    /** The k − 1 interior value edges of a column, cached per column while the sketches are set; null without a sketch value. */
    private double[] edges(final int column) {
        if (edgesCache == null) edgesCache = new double[nColumns][];
        double[] edges = edgesCache[column];
        if (edges != null) return edges;
        final int k = spec.binsK;
        if (column < nCandidates || (shuffleRef >= 0 && column >= nCandidates + spec.noise)) {
            final int sketch = column < nCandidates ? column : shuffleRef;
            if (quantiles == null || sketch >= quantiles.columns() || quantiles.count(sketch) == 0) return null;
            edges = quantiles.edges(sketch, k);
        } else {
            edges = new double[k - 1];
            for (int i = 1; i < k; i++) edges[i - 1] = StatMath.inverseNormal((double) i / k);
        }
        edgesCache[column] = edges;
        return edges;
    }

    /** Length of the grouped binned sums {@code [S_b (B), P_b (B), (P P')_bb' (B²)]}. */
    int binnedGroupedLength() {
        final int nb = spec.binCount();
        return 2 * nb + nb * nb;
    }

    /** Length of the row-family binned sums: {@code [Σ w, Σ w r, Σ w v]} per bin, then the three window totals. */
    int binnedRowLength() {
        return 3 * spec.binCount() + 3;
    }

    /**
     * Adds the grouped (conditional logit) binned sums into {@code into}, scaled by the unit weight:
     * {@code [S_b (B), P_b (B), (P P')_bb' (B²)]} with S_b = Σ_{i in b} (ỹ_i − p_i) and P_b = Σ_{i in b} p_i — the score
     * of the one-hot block and the pieces of its Fisher block diag(P) − P P' (DSL doc §6.1). Only the bins the unit
     * occupies are touched: O(n + occupied²) instead of O(B²) per unit.
     */
    void binnedGroupedContribution(final int[] bins, final double[] y, final double[] p, final double weight, final double[] into) {
        final int nb = spec.binCount();
        final double[] s = new double[nb];
        final double[] pb = new double[nb];
        final boolean[] seen = new boolean[nb];
        final int[] occupied = new int[Math.min(nb, bins.length)];
        int m = 0;
        for (int i = 0; i < bins.length; i++) {
            final int b = bins[i];
            if (!seen[b]) {
                seen[b] = true;
                occupied[m++] = b;
            }
            s[b] += y[i] - p[i];
            pb[b] += p[i];
        }
        for (int x = 0; x < m; x++) {
            final int b = occupied[x];
            into[b] += weight * s[b];
            into[nb + b] += weight * pb[b];
            for (int z = 0; z < m; z++) {
                final int c = occupied[z];
                into[2 * nb + b * nb + c] += weight * pb[b] * pb[c];
            }
        }
    }

    /**
     * Fails a scoring call of independent rows with rank / absdev that was not handed the window's sketches: the
     * within-unit fallback would read a single row (rank 0.5, absdev 0) and report degenerate records silently.
     */
    static void requireWindowQuantiles(final ScreenSpec spec, final WindowQuantiles quantiles) {
        if (quantiles == null && spec.needsWindowQuantiles()) {
            throw new IllegalStateException("rank / absdev of independent rows need the window quantile sketches (withWindowQuantiles)");
        }
    }

    /**
     * Adds the row-family binned sums into {@code into}: per bin {@code [Σ w, Σ w r, Σ w v]} (r = y − μ and v the
     * Fisher weight in offset mode; r = y and v unused in prior mode, where the report supplies the prior weight) and
     * the window totals {@code [Σ w, Σ w r, Σ w r²]} after the bins (the prior mean and the gaussian variance).
     */
    void binnedRowContribution(final int[] bins, final double[] y, final double[] mu, final double[] w, final boolean prior, final double[] into) {
        final int nb = spec.binCount();
        for (int i = 0; i < bins.length; i++) {
            final double r = prior ? y[i] : y[i] - mu[i];
            final double v = prior ? 0d : spec.fisherWeight(mu[i]);
            final int o = 3 * bins[i];
            into[o] += w[i];
            into[o + 1] += w[i] * r;
            into[o + 2] += w[i] * v;
            into[3 * nb] += w[i];
            into[3 * nb + 1] += w[i] * r;
            into[3 * nb + 2] += w[i] * r * r;
        }
    }

    /**
     * Applies a transform variant to column {@code column} of a unit: within the unit for a grouped run (or when
     * {@code quantiles} is null), else against the window's sketches (independent rows, DSL doc §6) — a candidate's
     * rank is its mid-rank among the window's finite values and its absdev the distance to the window median;
     * a noise placebo, standard normal by construction, takes the exact normal cdf and |x| (its median is 0). A
     * grouped run holds the sketches only for the binned test's value edges: its rank / absdev stay within the unit.
     */
    static double[] transform(final ScreenSpec spec, final WindowQuantiles quantiles, final int column, final String transform, final double[] v) {
        if (quantiles == null || spec.isGrouped() || ScreenSpec.TRANSFORM_RAW.equals(transform)) return transform(transform, v);
        final boolean candidate = !spec.isPlacebo(column);
        final double[] out = new double[v.length];
        switch (transform) {
            case ScreenSpec.TRANSFORM_RANK -> {
                for (int i = 0; i < v.length; i++) {
                    out[i] = !StatMath.isFinite(v[i]) ? Double.NaN : candidate ? quantiles.rank(column, v[i]) : NormalDistribution.cdf(v[i]);
                }
            }
            case ScreenSpec.TRANSFORM_ABSDEV -> {
                final double median = candidate ? quantiles.median(column) : 0d;
                for (int i = 0; i < v.length; i++) out[i] = StatMath.isFinite(v[i]) && StatMath.isFinite(median) ? Math.abs(v[i] - median) : Double.NaN;
            }
            default -> throw new IllegalArgumentException("unknown transform " + transform);
        }
        return out;
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
