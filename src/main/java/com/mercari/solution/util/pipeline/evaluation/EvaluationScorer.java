package com.mercari.solution.util.pipeline.evaluation;

import com.mercari.solution.util.domain.math.MatrixOps;
import com.mercari.solution.util.pipeline.glm.Baselines;
import com.mercari.solution.util.pipeline.glm.Family;
import com.mercari.solution.util.pipeline.glm.FitState;
import com.mercari.solution.util.pipeline.glm.GlmFit;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The per-unit computations of the evaluation transform, pure and deterministic (design §4): {@link #prepare}
 * aligns a unit (the baseline and every prediction set as means per row, the normalised labels, the weights),
 * {@link #derive} adds the derived sets of the calibration fits, {@link #score} reduces the unit to its loss
 * decomposition per set, {@link #accumulate} adds that into the metrics accumulators (overall and per slice
 * value) with the unit's Poisson bootstrap weights, {@link #aligned} gives the rows as the calibration tables
 * read them, and {@link #temperatureLogLikelihoods} / {@link #blendEvaluate} are the fit passes' contributions.
 */
public final class EvaluationScorer implements Serializable {

    private static final String SEP = MetricAccumulator.SEP;
    /**
     * floor of a mean inside a log score (a prediction giving a positive row probability 0 scores log EPS): the same
     * clamp {@link Baselines#means} applies, on purpose not GlmFit's 1e-300 optimizer guard (this is a reported metric's cap)
     */
    static final double LOG_FLOOR = Baselines.EPS;
    private static final double LOG_LOG_FLOOR = Math.log(LOG_FLOOR);

    private final EvaluationSpec spec;
    private final Family family;
    /** declared prediction sets */
    private final int k;
    /** compared sets: declared + derived */
    private final int sets;
    /**
     * per declared slice: a period bucket of the time field, the unit's by definition (the bucket of its earliest
     * row, as the unit's time is): a unit spanning two periods is not a row-level slice
     */
    private final boolean[] unitPeriodSlices;
    /** per fit: the positions of the blend coefficients it estimates (null for a temperature fit), see {@link #blendFree} */
    private final int[][] blendFree;

    public EvaluationScorer(final EvaluationSpec spec) {
        this.spec = spec;
        this.family = spec.family();
        this.k = spec.predictions.size();
        this.sets = spec.setCount();
        this.unitPeriodSlices = new boolean[spec.slices.size()];
        for (int s = 0; s < unitPeriodSlices.length; s++) {
            final EvaluationSpec.Slice sl = spec.slices.get(s);
            unitPeriodSlices[s] = sl.bucket != null && sl.field != null && sl.field.equals(spec.timeField);
        }
        this.blendFree = new int[spec.fits.size()][];
        for (int f = 0; f < blendFree.length; f++) {
            final EvaluationSpec.Fit fit = spec.fits.get(f);
            if (fit.isTemperature()) continue;
            final List<Integer> free = new ArrayList<>();
            for (int c = 0; c < blendK(); c++) if (!fit.fixes(EvaluationSpec.BLEND_COEFFICIENTS.get(c))) free.add(c);
            blendFree[f] = free.stream().mapToInt(Integer::intValue).toArray();
        }
    }

    /** Equality of two numeric values with NaN equal to NaN and 0.0 equal to −0.0. */
    private static boolean sameValue(final double a, final double b) {
        return a == b || (Double.isNaN(a) && Double.isNaN(b));
    }

    /** Why a unit cannot be scored (the common unit set: any invalid side skips the unit for every side). */
    public enum Skip { NONE, NO_POSITIVE_LABEL, INVALID_BASELINE, INVALID_PREDICTION }

    /** A prepared unit: rows sorted by (time, identity); means per row of the baseline (index 0), every prediction set and every derived set. */
    public static final class Unit {
        public final List<EvaluationRow> rows;
        public final String key;
        public final String split;
        public final Skip skip;
        /** [1 + sets][n]: the baseline means (NaN in binomial prior mode), each prediction set's, then each derived set's (NaN until derived) */
        public final double[][] means;
        public final double[] y;
        public final double[] w;
        public final double unitWeight;
        /** rows whose identity repeats an earlier row's (the same row twice in the unit, at any time) */
        public final int duplicates;
        /** rows removed before scoring by a column declared {@code invalid: dropRow} (not in {@link #rows}) */
        public final int dropped;
        /**
         * per declared slice / discovery dimension: whether the unit's rows disagree on the value (the first row's is
         * used); a period bucket of the time field never varies (it is the unit's), a dimension only on the discovery splits
         */
        public final boolean[] sliceVaries;
        public final boolean[] dimensionVaries;

        Unit(final List<EvaluationRow> rows, final String key, final Skip skip, final double[][] means, final double[] y, final double[] w, final double unitWeight,
             final int duplicates, final int dropped, final boolean[] sliceVaries, final boolean[] dimensionVaries) {
            this.rows = rows;
            this.key = key;
            this.split = rows.get(0).split;
            this.skip = skip;
            this.means = means;
            this.y = y;
            this.w = w;
            this.unitWeight = unitWeight;
            this.duplicates = duplicates;
            this.dropped = dropped;
            this.sliceVaries = sliceVaries;
            this.dimensionVaries = dimensionVaries;
        }

        public int size() {
            return rows.size();
        }

        /** The unit's slice values: those of its first row (a group-level attribute is the same on every row). */
        public String[] slices() {
            return rows.get(0).slices;
        }

        public long time() {
            return rows.get(0).time;
        }

        /** The bootstrap resampling key: the declared unit field's value, else the unit's own key. */
        public String bootKey() {
            final String declared = rows.get(0).bootKey;
            return declared != null ? declared : key;
        }
    }

    /**
     * The loss decomposition of one unit: per set (index 0 = the baseline), plus the unit's utility — the flat
     * return Σ u·y / n over its rows (y as declared; a null utility is a zero return), NaN without a utility
     * field. The utility is a property of the outcomes, the same under every set.
     */
    public static final class Metrics {
        public final double[] logScore;
        public final double[] hitAt1;
        public final double[] brier;
        public final double utility;

        Metrics(final double[] logScore, final double[] hitAt1, final double[] brier, final double utility) {
            this.logScore = logScore;
            this.hitAt1 = hitAt1;
            this.brier = brier;
            this.utility = utility;
        }
    }

    /**
     * Sorts the rows by (time, identity), removes the rows a column declared {@code invalid: dropRow} rejects
     * (the common row set: every set is compared on the remaining rows), derives the baseline and every set's
     * means, the labels and the weights; {@code skip} says why the unit cannot be scored — an invalid value in a
     * column that keeps its rows (the common unit set), no positive label among the remaining rows.
     */
    public Unit prepare(final List<EvaluationRow> input, final String unitKey) {
        final List<EvaluationRow> sorted = new ArrayList<>(input);
        sorted.sort(Comparator.comparingLong(EvaluationRow::getTime).thenComparing(EvaluationRow::getIdentity));
        final List<EvaluationRow> rows;
        int dropped = 0;
        if (spec.dropsRows()) {
            rows = new ArrayList<>(sorted.size());
            Skip emptied = Skip.NONE;
            for (final EvaluationRow r : sorted) {
                final Skip reason = dropReason(r);
                if (reason == Skip.NONE) rows.add(r);
                else {
                    dropped++;
                    if (emptied == Skip.NONE) emptied = reason;
                }
            }
            if (rows.isEmpty()) {
                // every row dropped: nothing to score; reported under the reason of the first row
                final EvaluationRow first = sorted.get(0);
                return new Unit(sorted, unitKey, emptied, new double[1 + sets][sorted.size()], null, null, 0, 0, dropped,
                        new boolean[first.slices.length], new boolean[spec.hasDiscovery() ? spec.discovery.dimensions.size() : 0]);
            }
        } else {
            rows = sorted;
        }
        final int n = rows.size();
        // the unit's integrity: the same row twice, and a slice / dimension the rows disagree on (a group-level
        // attribute by contract; the first row's value is used)
        int duplicates = 0;
        final EvaluationRow first = rows.get(0);
        final boolean[] sliceVaries = new boolean[first.slices.length];
        final boolean[] dimensionVaries = new boolean[spec.hasDiscovery() ? spec.discovery.dimensions.size() : 0];
        // the dimensions are only read (and their disagreement only counted) on the discovery and confirmation splits
        final boolean discoverySplit = dimensionVaries.length > 0
                && (first.split.equals(spec.discovery.discoverOn) || first.split.equals(spec.discovery.confirmOn));
        if (n > 1) {
            // an identity seen earlier in the unit, whatever its time: the rowId names the row
            final Set<String> seen = new HashSet<>(2 * n);
            seen.add(first.identity);
            for (int i = 1; i < n; i++) {
                final EvaluationRow r = rows.get(i);
                if (!seen.add(r.identity)) duplicates++;
                for (int s = 0; s < sliceVaries.length; s++) {
                    if (sliceVaries[s] || (s < unitPeriodSlices.length && unitPeriodSlices[s])) continue;
                    sliceVaries[s] = !Objects.equals(r.slices[s], first.slices[s]);
                }
                if (!discoverySplit) continue;
                for (int d = 0; d < dimensionVaries.length; d++) {
                    if (dimensionVaries[d]) continue;
                    final EvaluationSpec.Dimension dim = spec.discovery.dimensions.get(d);
                    if (dim.index < 0) continue;
                    dimensionVaries[d] = dim.isNumeric()
                            ? !sameValue(r.x[dim.index], first.x[dim.index])
                            : !Objects.equals(r.dims[dim.index], first.dims[dim.index]);
                }
            }
        }
        final double[][] means = new double[1 + sets][n];
        for (int j = 1 + k; j <= sets; j++) Arrays.fill(means[j], Double.NaN);
        // baseline
        if (spec.hasBaseline()) {
            final double[] baseline = new double[n];
            for (int i = 0; i < n; i++) baseline[i] = rows.get(i).baseline;
            if (Baselines.means(family, spec.baselineForm, baseline, means[0]) != Baselines.Skip.NONE) {
                return new Unit(rows, unitKey, Skip.INVALID_BASELINE, means, null, null, 0, duplicates, dropped, sliceVaries, dimensionVaries);
            }
        } else if (family.isGrouped()) {
            Arrays.fill(means[0], 1d / n);
        } else {
            Arrays.fill(means[0], Double.NaN);
        }
        // prediction sets
        for (int j = 0; j < k; j++) {
            final EvaluationSpec.Prediction d = spec.predictions.get(j);
            if (d.isScore()) {
                if (!softmax(rows, d, means[1 + j])) return new Unit(rows, unitKey, Skip.INVALID_PREDICTION, means, null, null, 0, duplicates, dropped, sliceVaries, dimensionVaries);
            } else {
                final double[] values = new double[n];
                for (int i = 0; i < n; i++) values[i] = rows.get(i).x[d.offset];
                if (Baselines.means(family, d.form, values, means[1 + j]) != Baselines.Skip.NONE) {
                    return new Unit(rows, unitKey, Skip.INVALID_PREDICTION, means, null, null, 0, duplicates, dropped, sliceVaries, dimensionVaries);
                }
            }
        }
        // labels and weights
        final double[] y = new double[n];
        for (int i = 0; i < n; i++) y[i] = rows.get(i).label;
        if (Baselines.normalizeLabels(family, spec.normalizeTies, y) != Baselines.Skip.NONE) {
            return new Unit(rows, unitKey, Skip.NO_POSITIVE_LABEL, means, y, null, 0, duplicates, dropped, sliceVaries, dimensionVaries);
        }
        final double[] w = new double[n];
        double wsum = 0;
        for (int i = 0; i < n; i++) {
            w[i] = rows.get(i).weight;
            wsum += w[i];
        }
        return new Unit(rows, unitKey, Skip.NONE, means, y, w, wsum / n, duplicates, dropped, sliceVaries, dimensionVaries);
    }

    /**
     * Why a row leaves its unit before scoring: the first column declared {@code invalid: dropRow} whose value the
     * row fails ({@link Baselines#validRow} for the baseline and a probability set; a finite score and an offset
     * valid for its form for a score set, {@link #offsetLogWeight} — the values {@link #softmax} would reject), {@link Skip#NONE}
     * when every dropping column accepts it.
     */
    private Skip dropReason(final EvaluationRow r) {
        if (spec.baselineDropsRows() && !Baselines.validRow(spec.baselineForm, r.baseline)) return Skip.INVALID_BASELINE;
        for (final EvaluationSpec.Prediction d : spec.predictions) {
            if (d.dropsRows() && !validValue(r, d)) return Skip.INVALID_PREDICTION;
        }
        return Skip.NONE;
    }

    /** Whether a row's value(s) of a prediction set are usable (the per-row conditions of {@link #softmax} / {@link Baselines#means}). */
    private static boolean validValue(final EvaluationRow r, final EvaluationSpec.Prediction d) {
        final double[] x = r.x;
        if (!d.isScore()) return Baselines.validRow(d.form, x[d.offset]);
        if (!Double.isFinite(x[d.offset])) return false;
        return d.offsetField == null || !Double.isNaN(offsetLogWeight(d, x[d.offset + 1]));
    }

    /**
     * The log weight log w of a score set's offset value in its form — log x ({@code prob}), x ({@code logProb}),
     * −log x ({@code inverseShare}); −∞ for a {@code prob} of 0 or a {@code logProb} of −∞ (a valid row of mass 0) —
     * or NaN when the value is invalid for its form: a null or a +∞ (no finite weight), a negative {@code prob}, a
     * non-finite or non-positive {@code inverseShare} (the baseline's rule, {@link Baselines#validRow}). The drop
     * rule ({@link #validValue}), {@link #softmax} and {@link #fitInputs} read an offset only through it, so a row
     * the drop rule keeps is one the softmax accepts.
     */
    static double offsetLogWeight(final EvaluationSpec.Prediction d, final double offset) {
        if (Double.isNaN(offset) || offset == Double.POSITIVE_INFINITY) return Double.NaN;
        if (d.offsetLog()) return offset;
        if (d.offsetInverse()) return Baselines.validRow(Family.FORM_INVERSE_SHARE, offset) ? -Math.log(offset) : Double.NaN;
        if (offset < 0) return Double.NaN;
        return offset > 0 ? Math.log(offset) : Double.NEGATIVE_INFINITY;
    }

    /**
     * Grouped softmax of a score set: q_i ∝ w_i · exp(score_i / T), w the offset value (form prob), exp(offset)
     * (logProb) or 1 / offset (inverseShare), 1 without an offset. A null score or an offset invalid for its form
     * ({@link #offsetLogWeight}) makes the unit invalid; a zero prob offset gives q_i = 0.
     */
    private static boolean softmax(final List<EvaluationRow> rows, final EvaluationSpec.Prediction d, final double[] out) {
        final int n = rows.size();
        final double[] eta = new double[n];
        for (int i = 0; i < n; i++) {
            final double[] x = rows.get(i).x;
            final double score = x[d.offset];
            if (!Double.isFinite(score)) return false;
            double e = score / d.temperature;
            if (d.offsetField != null) {
                final double logWeight = offsetLogWeight(d, x[d.offset + 1]);
                if (Double.isNaN(logWeight)) return false;
                e += logWeight;
            }
            eta[i] = e;
        }
        GlmFit.softmax(eta, out);
        for (final double q : out) if (Double.isNaN(q)) return false;
        return true;
    }

    // ---- calibration fits ----------------------------------------------------------------------------------

    /**
     * The fit inputs of a declared set (design §7.1): {@code [f, o]} per row — a score set's score (over its
     * declared temperature) and its offset on the log scale; a probability set's log share (grouped) / logit
     * (binomial). {@code o} falls back to the baseline's log share / logit; NaN without one.
     */
    public double[][] fitInputs(final Unit unit, final int base) {
        final int n = unit.size();
        final EvaluationSpec.Prediction d = spec.predictions.get(base);
        final double[] f = new double[n];
        final double[] o = new double[n];
        for (int i = 0; i < n; i++) {
            final double[] x = unit.rows.get(i).x;
            if (d.isScore()) {
                f[i] = x[d.offset] / d.temperature;
                if (d.offsetField != null) {
                    // a row of mass 0 (a 0 prob offset, a −∞ logProb one) enters the fit at the log floor, as a zero
                    // share does — a −∞ column would make the blend's gradient NaN; the prob form floors every value
                    final double logWeight = offsetLogWeight(d, x[d.offset + 1]);
                    o[i] = logWeight == Double.NEGATIVE_INFINITY || Family.FORM_PROB.equals(d.offsetForm) ? Math.max(logWeight, LOG_LOG_FLOOR) : logWeight;
                } else {
                    o[i] = link(unit.means[0][i]);
                }
            } else {
                f[i] = link(unit.means[1 + base][i]);
                o[i] = link(unit.means[0][i]);
            }
        }
        return new double[][]{f, o};
    }

    /** Whether a set carries its own offset (a score set with an offset field): the temperature fit keeps it in the predictor. */
    private boolean ownOffset(final int base) {
        final EvaluationSpec.Prediction d = spec.predictions.get(base);
        return d.isScore() && d.offsetField != null;
    }

    /** log of a share (grouped) / logit of a probability (binomial), floored; NaN stays NaN (no baseline). */
    private double link(final double p) {
        if (Double.isNaN(p)) return Double.NaN;
        final double c = Math.max(p, LOG_FLOOR);
        return family.isGrouped() ? Math.log(c) : Math.log(c / Math.max(1 - c, LOG_FLOOR));
    }

    /**
     * Fills the derived sets' means from the fitted parameters: temperature η = o + f / T (o only for a score
     * set with its own offset: a probability set's log share / logit is the whole predictor); blend η = a·f +
     * b·o (+ c). A derived set without parameters (a fit that produced none) stays NaN. A row the base set
     * excludes (mean exactly 0: a zero prob-scale offset, a zero share) keeps mass 0 in the derived set — the fit
     * inputs floor its log at {@link #LOG_FLOOR}, which would otherwise leak a small mass onto it.
     */
    public void derive(final Unit unit, final FitResults fits) {
        if (fits == null || unit.skip != Skip.NONE) return;
        final int n = unit.size();
        for (int i = 0; i < spec.derived.size(); i++) {
            final EvaluationSpec.Derived dv = spec.derived.get(i);
            final double[] params = fits.parameters(dv.name);
            if (params == null) continue;
            final EvaluationSpec.Fit fit = spec.fits.get(dv.fit);
            final double[][] fo = fitInputs(unit, dv.base);
            final double[] eta = new double[n];
            if (fit.isTemperature()) {
                final boolean own = ownOffset(dv.base);
                for (int r = 0; r < n; r++) eta[r] = (own ? fo[1][r] : 0d) + fo[0][r] / params[0];
            } else {
                for (int r = 0; r < n; r++) eta[r] = params[0] * fo[0][r] + params[1] * fo[1][r] + (params.length > 2 ? params[2] : 0d);
            }
            excludeZeroMass(unit, dv.base, eta);
            unit.means[1 + k + i] = GlmFit.means(family, eta);
        }
    }

    /** η = −∞ on the rows the base set gives mass exactly 0, so a derived set never puts mass where its base has none. */
    private void excludeZeroMass(final Unit unit, final int base, final double[] eta) {
        final double[] means = unit.means[1 + base];
        for (int r = 0; r < eta.length; r++) if (means[r] == 0d) eta[r] = Double.NEGATIVE_INFINITY;
    }

    /** Layout of a temperature fit's pass vector: per base set, the grid's weighted log scores; then the unit mass. */
    public static int temperatureLength(final EvaluationSpec.Fit fit, final int bases) {
        return bases * fit.gridSize + 1;
    }

    /**
     * A unit's contribution to a temperature fit: for every base set and every grid value T, the weighted log
     * score of η = o + f / T (see {@link #derive}); the last entry is the unit's weight mass.
     */
    public double[] temperatureLogLikelihoods(final Unit unit, final int fitIndex) {
        final EvaluationSpec.Fit fit = spec.fits.get(fitIndex);
        final List<Integer> derivedSets = spec.derivedOf(fitIndex);
        final double[] grid = fit.grid();
        final double[] out = new double[temperatureLength(fit, derivedSets.size())];
        final int n = unit.size();
        for (int s = 0; s < derivedSets.size(); s++) {
            final EvaluationSpec.Derived dv = spec.derived.get(derivedSets.get(s) - k);
            final boolean own = ownOffset(dv.base);
            final double[][] fo = fitInputs(unit, dv.base);
            final double[] eta = new double[n];
            for (int g = 0; g < grid.length; g++) {
                for (int r = 0; r < n; r++) eta[r] = (own ? fo[1][r] : 0d) + fo[0][r] / grid[g];
                excludeZeroMass(unit, dv.base, eta);
                out[s * fit.gridSize + g] = unit.unitWeight * logScore(GlmFit.means(family, eta), unit);
            }
        }
        out[out.length - 1] = unit.unitWeight;
        return out;
    }

    /** Number of blend coefficients: [a, b] for the grouped family, [a, b, intercept] for binomial. */
    public int blendK() {
        return spec.blendK();
    }

    /** The positions (in {@link #blendK} order) of the coefficients a blend fit estimates: those its {@code fix} does not hold (shared, read only). */
    public int[] blendFree(final int fitIndex) {
        return blendFree[fitIndex];
    }

    /** The full coefficient vector {@code [a, b(, intercept)]} of a blend fit from its free part: the fixed ones at their values. */
    public double[] blendFull(final int fitIndex, final double[] free) {
        final EvaluationSpec.Fit fit = spec.fits.get(fitIndex);
        final int kk = blendK();
        final double[] theta = new double[kk];
        int i = 0;
        for (int c = 0; c < kk; c++) {
            final Double fixed = fit.fix.get(EvaluationSpec.BLEND_COEFFICIENTS.get(c));
            theta[c] = fixed != null ? fixed : free[i++];
        }
        return theta;
    }

    /** The blend design of a unit: rows {@code [f, o]} (+ 1). */
    public double[][] blendDesign(final Unit unit, final int base) {
        final double[][] fo = fitInputs(unit, base);
        final int n = unit.size();
        final int kk = blendK();
        final double[][] design = new double[n][kk];
        for (int r = 0; r < n; r++) {
            design[r][0] = fo[0][r];
            design[r][1] = fo[1][r];
            if (kk > 2) design[r][2] = 1d;
        }
        return design;
    }

    /**
     * One Newton pass evaluation of a unit for a blend fit of the base set at the free coefficients {@code theta}:
     * {@code [n, ll, g, G]} over the free columns of the design (the fixed coefficients' columns enter the linear
     * predictor at their values and carry no gradient), via the shared offset GLM (the grouped family's softmax
     * of the whole predictor, no separate offset).
     */
    public double[] blendEvaluate(final Unit unit, final int base, final int fitIndex, final double[] theta) {
        final double[][] design = blendDesign(unit, base);
        final double[] full = blendFull(fitIndex, theta);
        final int[] free = blendFree(fitIndex);
        final int n = unit.size();
        final double[] eta = new double[n];
        final double[][] f = new double[n][free.length];
        for (int r = 0; r < n; r++) {
            eta[r] = MatrixOps.dot(design[r], full);
            for (int i = 0; i < free.length; i++) f[r][i] = design[r][free[i]];
        }
        final double[] mu = GlmFit.means(family, eta);
        return GlmFit.evaluate(family, unit.y, mu, unit.w, unit.unitWeight, f, free.length);
    }

    /**
     * The starting point of a blend of a base set, over the fit's free coefficients: the set as declared, so the
     * fit's identity log score is the declared set's — a = 1; b = 1 for a score set with its own offset
     * (η = f + o), b = 0 otherwise (a probability set's log share / logit, or a score set without an offset, is
     * the whole predictor and the baseline enters only through the fit); intercept 0. A fixed coefficient is
     * not part of it (its value enters through {@link #blendFull}), so with a fix the identity is the declared set
     * at the fixed values.
     */
    public double[] blendStart(final int base, final int fitIndex) {
        final double[] full = new double[blendK()];
        full[0] = 1d;
        full[1] = ownOffset(base) ? 1d : 0d;
        final int[] free = blendFree(fitIndex);
        final double[] theta = new double[free.length];
        for (int i = 0; i < free.length; i++) theta[i] = full[free[i]];
        return theta;
    }

    /**
     * Standard errors of a fitted blend: the square roots of the inverse Fisher information's diagonal; NaN
     * when the information matrix is not positive definite (the parameters are not identified).
     */
    public static double[] standardErrors(final FitState state) {
        final double[] se = new double[state.k];
        Arrays.fill(se, Double.NaN);
        if (!state.hasBest || state.bestG == null) return se;
        final double[][] inverse = MatrixOps.inverseSpd(state.bestG);
        if (inverse == null) return se;
        for (int i = 0; i < state.k; i++) se[i] = inverse[i][i] > 0 ? Math.sqrt(inverse[i][i]) : Double.NaN;
        return se;
    }

    // ---- scoring -------------------------------------------------------------------------------------------

    private double logScore(final double[] q, final Unit unit) {
        final int n = unit.size();
        double ls = 0;
        if (family.isGrouped()) {
            for (int i = 0; i < n; i++) if (unit.y[i] > 0) ls += unit.y[i] * Math.log(Math.max(q[i], LOG_FLOOR));
        } else {
            final double y = unit.y[0];
            final double p = q[0];
            ls = y * Math.log(Math.max(p, LOG_FLOOR)) + (1 - y) * Math.log(Math.max(1 - p, LOG_FLOOR));
        }
        return ls;
    }

    /** The loss decomposition of a scored unit (design §4.1); a set whose means are not available scores NaN. */
    public Metrics score(final Unit unit) {
        final int n = unit.size();
        final double[] logScore = new double[1 + sets];
        final double[] hit = new double[1 + sets];
        final double[] brier = new double[1 + sets];
        for (int j = 0; j <= sets; j++) {
            final double[] q = unit.means[j];
            if (Double.isNaN(q[0])) {
                // binomial prior mode (the reference is a function of the split's label mean, derived by the report),
                // or a derived set without parameters
                logScore[j] = Double.NaN;
                hit[j] = Double.NaN;
                brier[j] = Double.NaN;
                continue;
            }
            double br = 0;
            if (family.isGrouped()) {
                double max = Double.NEGATIVE_INFINITY;
                for (int i = 0; i < n; i++) {
                    br += (q[i] - unit.y[i]) * (q[i] - unit.y[i]);
                    if (q[i] > max) max = q[i];
                }
                // ties at the maximum share the credit
                double hitSum = 0;
                int ties = 0;
                for (int i = 0; i < n; i++) {
                    if (q[i] == max) {
                        hitSum += unit.y[i];
                        ties++;
                    }
                }
                hit[j] = ties > 0 ? hitSum / ties : 0d;
            } else {
                final double y = unit.y[0];
                final double p = q[0];
                br = (p - y) * (p - y);
                hit[j] = Double.NaN;
            }
            logScore[j] = logScore(q, unit);
            brier[j] = br;
        }
        return new Metrics(logScore, hit, brier, utility(unit));
    }

    /**
     * The unit's flat return Σ u·y / n over its rows (the label as declared; a null utility and a losing row
     * count 0, see {@link EvaluationReport#payout}); NaN without a utility field.
     */
    private double utility(final Unit unit) {
        if (!spec.hasUtility()) return Double.NaN;
        double sum = 0;
        for (final EvaluationRow r : unit.rows) sum += EvaluationReport.payout(r.x[spec.utilityIndex], r.label);
        return sum / unit.size();
    }

    /** Accumulator key of (split, prediction index, slice index, slice value); slice −1 = the overall record. */
    public static String key(final String split, final int prediction, final int slice, final String value) {
        return split + SEP + prediction + SEP + slice + SEP + (value == null ? "" : value);
    }

    /** The parts of a metrics key: {@code [split, prediction index, slice index, slice value]} (null for a bookkeeping key). */
    public static String[] parseKey(final String key) {
        if (key.startsWith(SEP)) return null;
        final String[] parts = key.split(SEP, -1);
        return parts.length == 4 ? parts : null;
    }

    /**
     * Adds a scored unit into the accumulators: one key per set (the baseline first) for the overall record and
     * for each of its slice values (a null slice value is skipped), as a contribution under the unit's bootstrap
     * key (the replicate sums are expanded once a key's pending contributions pass the bound, else by the
     * Combine, see {@link MetricAccumulator#bound}); and the
     * split's bookkeeping (units, rows, time range).
     */
    public void accumulate(final Unit unit, final Metrics m, final Map<String, MetricAccumulator> into) {
        final String boot = spec.bootstrapSamples > 0 ? unit.bootKey() : null;
        final int n = unit.size();
        double positives = 0;
        for (int i = 0; i < n; i++) positives += unit.w[i] * unit.y[i];
        final double wu = unit.unitWeight;
        final String[] slices = unit.slices();
        for (int j = 0; j <= sets; j++) {
            final double[] slots = new double[MetricAccumulator.SLOTS];
            slots[MetricAccumulator.N_UNITS] = 1;
            slots[MetricAccumulator.N_ROWS] = n;
            slots[MetricAccumulator.W] = wu;
            // grouped: the unit's positives at the unit weight (Σ ỹ = 1); binomial: w y
            slots[MetricAccumulator.WY] = family.isGrouped() ? wu : positives;
            slots[MetricAccumulator.LOG] = wu * m.logScore[j];
            slots[MetricAccumulator.LOG_BASE] = wu * m.logScore[0];
            slots[MetricAccumulator.HIT] = wu * m.hitAt1[j];
            slots[MetricAccumulator.BRIER] = wu * m.brier[j];
            slots[MetricAccumulator.UTILITY] = spec.hasUtility() ? wu * m.utility : 0d;
            add(into, key(unit.split, j, -1, null), slots, boot);
            for (int s = 0; s < slices.length; s++) {
                if (slices[s] != null) add(into, key(unit.split, j, s, slices[s]), slots, boot);
            }
        }
        final MetricAccumulator book = into.computeIfAbsent(MetricAccumulator.SPLIT_KEY_PREFIX + unit.split, key -> new MetricAccumulator());
        final double[] slots = new double[MetricAccumulator.SLOTS];
        slots[MetricAccumulator.UNITS] = 1;
        slots[MetricAccumulator.ROWS] = n;
        slots[MetricAccumulator.ROWS_DUPLICATE] = unit.duplicates;
        slots[MetricAccumulator.ROWS_DROPPED] = unit.dropped;
        book.add(slots);
        for (final EvaluationRow r : unit.rows) if (r.time != EvaluationRow.NO_TIME) book.time(r.time);
        integrity(unit, into);
    }

    /** Counts the unit under every slice / dimension its rows disagree on (the summary's notes; dimensions: discovery splits only, see {@link #prepare}). */
    private static void integrity(final Unit unit, final Map<String, MetricAccumulator> into) {
        for (int s = 0; s < unit.sliceVaries.length; s++) {
            if (unit.sliceVaries[s]) count(into, MetricAccumulator.SLICE_VARIES_KEY_PREFIX + s);
        }
        for (int d = 0; d < unit.dimensionVaries.length; d++) {
            if (unit.dimensionVaries[d]) count(into, MetricAccumulator.DIMENSION_VARIES_KEY_PREFIX + d);
        }
    }

    private static void count(final Map<String, MetricAccumulator> into, final String key) {
        final double[] slots = new double[MetricAccumulator.SLOTS];
        slots[0] = 1;
        into.computeIfAbsent(key, k -> new MetricAccumulator()).add(slots);
    }

    private void add(final Map<String, MetricAccumulator> into, final String key, final double[] slots, final String boot) {
        final MetricAccumulator acc = into.computeIfAbsent(key, x -> new MetricAccumulator());
        acc.contribute(slots, boot);
        // a large bundle expands the key's replicate sums here (bounded memory; the shuffle carries the expanded
        // form at most once per key per bundle); a one-unit bundle ships the contribution unexpanded
        acc.bound(spec.bootstrapSeed, spec.bootstrapSamples);
    }

    // ---- slice discovery -----------------------------------------------------------------------------------

    /** The per-unit value of the discovery metric for a set (NaN when not available). */
    public double discoveryValue(final Metrics m, final int set, final String metric) {
        final int j = 1 + set;
        return switch (metric) {
            case "excessLogScore" -> m.logScore[j] - m.logScore[0];
            case "logScore" -> m.logScore[j];
            case "hitAt1" -> m.hitAt1[j];
            case "brier" -> m.brier[j];
            case "utility" -> m.utility;
            default -> throw new IllegalArgumentException("unknown discovery metric " + metric);
        };
    }

    /**
     * The unit's dimension values as the discovery reads them: a categorical dimension's text, a numeric one's
     * quantile bin (from the discovery split's edges; null without edges), null when missing.
     */
    public String[] dimensionValues(final Unit unit, final Map<Integer, double[]> edges) {
        final List<EvaluationSpec.Dimension> dims = spec.discovery.dimensions;
        final String[] values = new String[dims.size()];
        final EvaluationRow first = unit.rows.get(0);
        for (int i = 0; i < dims.size(); i++) {
            final EvaluationSpec.Dimension d = dims.get(i);
            if (d.index < 0) continue;
            if (d.isNumeric()) {
                final double v = first.x[d.index];
                final double[] e = edges == null ? null : edges.get(i);
                if (Double.isNaN(v) || e == null) continue;
                values[i] = "q" + EvaluationReport.bin(v, e);
            } else {
                values[i] = first.dims[d.index];
            }
        }
        return values;
    }

    /** Accumulator key of a candidate slice: (split, set, the dimension indices, their values); an empty cell is the split's overall. */
    public static String discoveryKey(final String split, final int set, final int[] dims, final String[] values) {
        final StringBuilder sb = new StringBuilder(split).append(SEP).append(set).append(SEP);
        for (int i = 0; i < dims.length; i++) {
            if (i > 0) sb.append(',');
            sb.append(dims[i]);
        }
        sb.append(SEP);
        for (int i = 0; i < values.length; i++) {
            if (i > 0) sb.append((char) 2);
            sb.append(values[i]);
        }
        return sb.toString();
    }

    /** The parts of a discovery key: {@code [split, set, dims csv, values]} (values joined by \u0002). */
    public static String[] parseDiscoveryKey(final String key) {
        // exactly four parts, like parseKey: a categorical value carrying the separator is not a cell
        final String[] parts = key.split(SEP, -1);
        return parts.length == 4 ? parts : null;
    }

    /**
     * Adds the unit's discovery contributions: for each set of the discovery and each combination of up to
     * {@code maxDepth} dimensions with non-null values, the cell's {@code [n, Σd, Σd²]}, and the split's overall cell.
     * Only the discovery and confirmation splits contribute.
     */
    public void accumulateDiscovery(final Unit unit, final Metrics m, final Map<Integer, double[]> edges, final Map<String, double[]> into) {
        final EvaluationSpec.Discovery d = spec.discovery;
        if (!unit.split.equals(d.discoverOn) && !unit.split.equals(d.confirmOn)) return;
        final String[] values = dimensionValues(unit, edges);
        final List<Integer> present = new ArrayList<>();
        for (int i = 0; i < values.length; i++) if (values[i] != null) present.add(i);
        final List<int[]> combos = new ArrayList<>();
        combos.add(new int[0]);
        combinations(present, d.maxDepth, 0, new ArrayList<>(), combos);
        for (final int set : d.sets) {
            final double v = discoveryValue(m, set, d.metric);
            if (Double.isNaN(v)) continue;
            for (final int[] combo : combos) {
                final String[] vals = new String[combo.length];
                for (int i = 0; i < combo.length; i++) vals[i] = values[combo[i]];
                final double[] cell = into.computeIfAbsent(discoveryKey(unit.split, set, combo, vals), key -> new double[3]);
                cell[0] += 1;
                cell[1] += v;
                cell[2] += v * v;
            }
        }
    }

    private static void combinations(final List<Integer> items, final int maxDepth, final int from, final List<Integer> current, final List<int[]> out) {
        if (current.size() == maxDepth) return;
        for (int i = from; i < items.size(); i++) {
            current.add(items.get(i));
            out.add(current.stream().mapToInt(Integer::intValue).toArray());
            combinations(items, maxDepth, i + 1, current, out);
            current.remove(current.size() - 1);
        }
    }

    /** Counts a skipped unit in its split's bookkeeping (by reason), with the rows it dropped and its duplicates. */
    public void skipped(final Unit unit, final Map<String, MetricAccumulator> into) {
        final MetricAccumulator book = into.computeIfAbsent(MetricAccumulator.SPLIT_KEY_PREFIX + unit.split, key -> new MetricAccumulator());
        final double[] slots = new double[MetricAccumulator.SLOTS];
        final double units = family.isGrouped() ? 1 : unit.size();
        slots[MetricAccumulator.UNITS_SKIPPED] = units;
        if (unit.skip == Skip.INVALID_BASELINE) slots[MetricAccumulator.UNITS_SKIPPED_BASELINE] = units;
        else if (unit.skip == Skip.INVALID_PREDICTION) slots[MetricAccumulator.UNITS_SKIPPED_PREDICTION] = units;
        slots[MetricAccumulator.ROWS_DUPLICATE] = unit.duplicates;
        slots[MetricAccumulator.ROWS_DROPPED] = unit.dropped;
        book.add(slots);
        integrity(unit, into);
    }

    /** The unit's rows as the calibration tables read them (every compared set, derived ones included). */
    public List<AlignedRow> aligned(final Unit unit) {
        final int n = unit.size();
        final List<AlignedRow> out = new ArrayList<>(n);
        final int nFields = spec.tables.size();
        for (int i = 0; i < n; i++) {
            final double[] q = new double[sets];
            for (int j = 0; j < sets; j++) q[j] = unit.means[1 + j][i];
            final double[] fields = new double[nFields];
            for (int t = 0; t < nFields; t++) {
                final int idx = spec.tables.get(t).fieldIndex;
                fields[t] = idx >= 0 ? unit.rows.get(i).x[idx] : Double.NaN;
            }
            final double utility = spec.hasUtility() ? unit.rows.get(i).x[spec.utilityIndex] : Double.NaN;
            out.add(new AlignedRow(unit.split, unit.rows.get(i).label, unit.y[i], unit.means[0][i], q, fields, utility));
        }
        return out;
    }

    /**
     * The unit's rows as the rows output carries them (design §8.6): one record per row with its identity
     * (the rowId values), the label as declared and its share, the baseline mean, every compared set's mean
     * (declared and derived, so the calibrated probabilities of `@T` / `@blend` are here) and the utility.
     */
    public List<Map<String, Object>> rowRecords(final Unit unit) {
        final int n = unit.size();
        final List<Map<String, Object>> records = new ArrayList<>(n);
        final List<String> names = spec.predictionNames();
        for (int i = 0; i < n; i++) {
            final EvaluationRow row = unit.rows.get(i);
            final Map<String, Object> r = new LinkedHashMap<>();
            r.put("split", unit.split);
            r.put("unit", unit.key);
            final List<Map<String, Object>> ids = new ArrayList<>(spec.rowId.size());
            for (int f = 0; f < spec.rowId.size(); f++) ids.add(fieldValue(spec.rowId.get(f), f < row.ids.length ? row.ids[f] : null));
            r.put("rowId", ids);
            r.put("time", row.time == EvaluationRow.NO_TIME ? null : row.time * 1000L);
            r.put("label", row.label);
            r.put("labelShare", unit.y[i]);
            r.put("baseline", EvaluationReport.finiteOrNull(unit.means[0][i]));
            final List<Map<String, Object>> predictions = new ArrayList<>(sets);
            for (int j = 0; j < sets; j++) {
                final Map<String, Object> p = new LinkedHashMap<>();
                p.put("prediction", names.get(1 + j));
                p.put("p", EvaluationReport.finiteOrNull(unit.means[1 + j][i]));
                predictions.add(p);
            }
            r.put("predictions", predictions);
            r.put("utility", spec.hasUtility() ? EvaluationReport.finiteOrNull(row.x[spec.utilityIndex]) : null);
            records.add(r);
        }
        return records;
    }

    /** The unit's output records (design §8.4): one per set, the baseline first. */
    public List<Map<String, Object>> unitRecords(final Unit unit, final Metrics m) {
        final List<Map<String, Object>> records = new ArrayList<>(1 + sets);
        final List<String> names = spec.predictionNames();
        final List<Map<String, Object>> slices = new ArrayList<>();
        final String[] values = unit.slices();
        for (int s = 0; s < values.length; s++) slices.add(fieldValue(spec.slices.get(s).name(), values[s]));
        final boolean priorBase = Double.isNaN(m.logScore[0]);
        for (int j = 0; j <= sets; j++) {
            final Map<String, Object> r = new LinkedHashMap<>();
            r.put("split", unit.split);
            r.put("unit", unit.key);
            r.put("time", unit.time() == EvaluationRow.NO_TIME ? null : unit.time() * 1000L);   // epoch micros: the primitive form of a TIMESTAMP
            r.put("prediction", names.get(j));
            r.put("n_rows", (long) unit.size());
            r.put("weight", unit.unitWeight);
            r.put("logScore", EvaluationReport.finiteOrNull(m.logScore[j]));
            r.put("logScoreBaseline", priorBase ? null : m.logScore[0]);
            r.put("excessLogScore", priorBase ? null : EvaluationReport.finiteOrNull(m.logScore[j] - m.logScore[0]));
            r.put("hitAt1", EvaluationReport.finiteOrNull(m.hitAt1[j]));
            r.put("brier", EvaluationReport.finiteOrNull(m.brier[j]));
            r.put("utility", EvaluationReport.finiteOrNull(m.utility));
            r.put("slices", slices);
            records.add(r);
        }
        return records;
    }

    /** A {@code {field, value}} record: a unit's slice value, a row's rowId value. */
    private static Map<String, Object> fieldValue(final String field, final String value) {
        final Map<String, Object> m = new LinkedHashMap<>();
        m.put("field", field);
        m.put("value", value);
        return m;
    }

}
