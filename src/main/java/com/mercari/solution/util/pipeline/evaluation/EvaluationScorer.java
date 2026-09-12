package com.mercari.solution.util.pipeline.evaluation;

import com.mercari.solution.util.pipeline.feature.FeatureValues;
import com.mercari.solution.util.pipeline.glm.Baselines;
import com.mercari.solution.util.pipeline.glm.Family;
import com.mercari.solution.util.pipeline.glm.GlmFit;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;

/**
 * The per-unit computations of the evaluation transform, pure and deterministic (design §4): {@link #prepare}
 * aligns a unit (the baseline and every prediction set as means per row, the normalised labels, the weights),
 * {@link #score} reduces it to its loss decomposition per prediction set, {@link #accumulate} adds that into
 * the metrics accumulators (overall and per slice value) with the unit's Poisson bootstrap weights, and
 * {@link #aligned} gives the rows as the calibration tables read them.
 */
public final class EvaluationScorer implements Serializable {

    private static final String SEP = String.valueOf((char) 1);
    /** floor of a mean inside a log score (a prediction giving a positive row probability 0 scores log EPS) */
    static final double LOG_FLOOR = 1e-12;

    private final EvaluationSpec spec;
    private final Family family;
    private final int k;

    public EvaluationScorer(final EvaluationSpec spec) {
        this.spec = spec;
        this.family = spec.family();
        this.k = spec.predictions.size();
    }

    /** Why a unit cannot be scored (the common unit set: any invalid side skips the unit for every side). */
    public enum Skip { NONE, NO_POSITIVE_LABEL, INVALID_BASELINE, INVALID_PREDICTION }

    /** A prepared unit: rows sorted by (time, identity); means per row of the baseline (index 0) and every prediction set. */
    public static final class Unit {
        public final List<EvaluationRow> rows;
        public final String key;
        public final String split;
        public final Skip skip;
        /** [1 + k][n]: the baseline means (NaN in binomial prior mode), then each prediction set's */
        public final double[][] means;
        public final double[] y;
        public final double[] w;
        public final double unitWeight;

        Unit(final List<EvaluationRow> rows, final String key, final Skip skip, final double[][] means, final double[] y, final double[] w, final double unitWeight) {
            this.rows = rows;
            this.key = key;
            this.split = rows.get(0).split;
            this.skip = skip;
            this.means = means;
            this.y = y;
            this.w = w;
            this.unitWeight = unitWeight;
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

    /** The loss decomposition of one unit: per prediction set (index 0 = the baseline). */
    public static final class Metrics {
        public final double[] logScore;
        public final double[] hitAt1;
        public final double[] brier;

        Metrics(final double[] logScore, final double[] hitAt1, final double[] brier) {
            this.logScore = logScore;
            this.hitAt1 = hitAt1;
            this.brier = brier;
        }
    }

    public Unit prepare(final List<EvaluationRow> input, final String unitKey) {
        final List<EvaluationRow> rows = new ArrayList<>(input);
        rows.sort(Comparator.comparingLong(EvaluationRow::getTime).thenComparing(EvaluationRow::getIdentity));
        final int n = rows.size();
        final double[][] means = new double[1 + k][n];
        // baseline
        if (spec.hasBaseline()) {
            final double[] baseline = new double[n];
            for (int i = 0; i < n; i++) baseline[i] = rows.get(i).baseline;
            if (Baselines.means(family, spec.baselineForm, baseline, means[0]) != Baselines.Skip.NONE) {
                return new Unit(rows, unitKey, Skip.INVALID_BASELINE, means, null, null, 0);
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
                if (!softmax(rows, d, means[1 + j])) return new Unit(rows, unitKey, Skip.INVALID_PREDICTION, means, null, null, 0);
            } else {
                final double[] values = new double[n];
                for (int i = 0; i < n; i++) values[i] = rows.get(i).x[d.offset];
                if (Baselines.means(family, d.form, values, means[1 + j]) != Baselines.Skip.NONE) {
                    return new Unit(rows, unitKey, Skip.INVALID_PREDICTION, means, null, null, 0);
                }
            }
        }
        // labels and weights
        final double[] y = new double[n];
        for (int i = 0; i < n; i++) y[i] = rows.get(i).label;
        if (Baselines.normalizeLabels(family, spec.normalizeTies, y) != Baselines.Skip.NONE) {
            return new Unit(rows, unitKey, Skip.NO_POSITIVE_LABEL, means, y, null, 0);
        }
        final double[] w = new double[n];
        double wsum = 0;
        for (int i = 0; i < n; i++) {
            w[i] = rows.get(i).weight;
            wsum += w[i];
        }
        return new Unit(rows, unitKey, Skip.NONE, means, y, w, wsum / n);
    }

    /**
     * Grouped softmax of a score set: q_i ∝ w_i · exp(score_i / T), w the offset value (prob scale) or exp(offset)
     * (log scale), 1 without an offset. A null score or offset makes the unit invalid; a non-positive prob-scale
     * offset gives q_i = 0.
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
                final double offset = x[d.offset + 1];
                if (Double.isNaN(offset)) return false;
                if (EvaluationSpec.OFFSET_SCALE_LOG.equals(d.offsetScale)) {
                    e += offset;
                } else {
                    if (offset < 0) return false;
                    e += offset > 0 ? Math.log(offset) : Double.NEGATIVE_INFINITY;
                }
            }
            eta[i] = e;
        }
        GlmFit.softmax(eta, out);
        for (final double q : out) if (Double.isNaN(q)) return false;
        return true;
    }

    /** The loss decomposition of a scored unit (design §4.1). */
    public Metrics score(final Unit unit) {
        final int n = unit.size();
        final double[] logScore = new double[1 + k];
        final double[] hit = new double[1 + k];
        final double[] brier = new double[1 + k];
        for (int j = 0; j <= k; j++) {
            final double[] q = unit.means[j];
            if (j == 0 && Double.isNaN(q[0])) {
                // binomial prior mode: the reference is a function of the split's label mean, derived by the report
                logScore[j] = Double.NaN;
                hit[j] = Double.NaN;
                brier[j] = Double.NaN;
                continue;
            }
            double ls = 0, br = 0;
            if (family.isGrouped()) {
                double max = Double.NEGATIVE_INFINITY;
                for (int i = 0; i < n; i++) {
                    if (unit.y[i] > 0) ls += unit.y[i] * Math.log(Math.max(q[i], LOG_FLOOR));
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
                // one independent row per unit
                final double y = unit.y[0];
                final double p = q[0];
                ls = y * Math.log(Math.max(p, LOG_FLOOR)) + (1 - y) * Math.log(Math.max(1 - p, LOG_FLOOR));
                br = (p - y) * (p - y);
                hit[j] = Double.NaN;
            }
            logScore[j] = ls;
            brier[j] = br;
        }
        return new Metrics(logScore, hit, brier);
    }

    /** Poisson(1) replicate weights of a resampling unit: a pure function of (seed, key). */
    public static double[] poissonWeights(final long seed, final String key, final int samples) {
        final double[] w = new double[samples];
        if (samples == 0) return w;
        final SplittableRandom rng = FeatureValues.seededRandom(seed, key + SEP + "bootstrap");
        final double limit = Math.exp(-1d);
        for (int b = 0; b < samples; b++) {
            int c = 0;
            double p = 1d;
            do {
                c++;
                p *= rng.nextDouble();
            } while (p > limit);
            w[b] = c - 1;
        }
        return w;
    }

    /** Accumulator key of (split, prediction index, slice index, slice value); slice −1 = the overall record. */
    public static String key(final String split, final int prediction, final int slice, final String value) {
        return split + SEP + prediction + SEP + slice + SEP + (value == null ? "" : value);
    }

    /** The parts of a metrics key: {@code [split, prediction index, slice index, slice value]} (null for a bookkeeping key). */
    public static String[] parseKey(final String key) {
        if (key.startsWith("")) return null;
        final String[] parts = key.split(SEP, -1);
        return parts.length == 4 ? parts : null;
    }

    /**
     * Adds a scored unit into the accumulators: one key per prediction set (the baseline first) for the overall
     * record and for each of its slice values (a null slice value is skipped), with the unit's bootstrap
     * weights; and the split's bookkeeping (units, rows, time range).
     */
    public void accumulate(final Unit unit, final Metrics m, final Map<String, MetricAccumulator> into) {
        final double[] boot = poissonWeights(spec.bootstrapSeed, unit.bootKey(), spec.bootstrapSamples);
        final int n = unit.size();
        double positives = 0;
        for (int i = 0; i < n; i++) positives += unit.w[i] * unit.y[i];
        final double wu = unit.unitWeight;
        final String[] slices = unit.slices();
        for (int j = 0; j <= k; j++) {
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
            add(into, key(unit.split, j, -1, null), slots, boot);
            for (int s = 0; s < slices.length; s++) {
                if (slices[s] != null) add(into, key(unit.split, j, s, slices[s]), slots, boot);
            }
        }
        final MetricAccumulator book = into.computeIfAbsent(MetricAccumulator.SPLIT_KEY_PREFIX + unit.split, key -> new MetricAccumulator());
        final double[] slots = new double[MetricAccumulator.SLOTS];
        slots[MetricAccumulator.UNITS] = 1;
        slots[MetricAccumulator.ROWS] = n;
        book.add(slots);
        for (final EvaluationRow r : unit.rows) if (r.time != EvaluationRow.NO_TIME) book.time(r.time);
    }

    private void add(final Map<String, MetricAccumulator> into, final String key, final double[] slots, final double[] boot) {
        final MetricAccumulator acc = into.computeIfAbsent(key, x -> new MetricAccumulator(spec.bootstrapSamples));
        acc.add(slots);
        if (boot.length > 0) acc.addReplicates(slots, boot);
    }

    /** Counts a skipped unit in its split's bookkeeping. */
    public void skipped(final Unit unit, final Map<String, MetricAccumulator> into) {
        final MetricAccumulator book = into.computeIfAbsent(MetricAccumulator.SPLIT_KEY_PREFIX + unit.split, key -> new MetricAccumulator());
        final double[] slots = new double[MetricAccumulator.SLOTS];
        slots[MetricAccumulator.UNITS_SKIPPED] = family.isGrouped() ? 1 : unit.size();
        book.add(slots);
    }

    /** The unit's rows as the calibration tables read them. */
    public List<AlignedRow> aligned(final Unit unit) {
        final int n = unit.size();
        final List<AlignedRow> out = new ArrayList<>(n);
        final int nFields = spec.tables.size();
        for (int i = 0; i < n; i++) {
            final double[] q = new double[k];
            for (int j = 0; j < k; j++) q[j] = unit.means[1 + j][i];
            final double[] fields = new double[nFields];
            for (int t = 0; t < nFields; t++) {
                final int idx = spec.tables.get(t).fieldIndex;
                fields[t] = idx >= 0 ? unit.rows.get(i).x[idx] : Double.NaN;
            }
            final double utility = spec.hasUtility() ? unit.rows.get(i).x[spec.utilityIndex] : Double.NaN;
            out.add(new AlignedRow(unit.split, unit.y[i], unit.means[0][i], q, fields, utility));
        }
        return out;
    }

    /** The unit's output records (design §8.4): one per prediction set, the baseline first. */
    public List<Map<String, Object>> unitRecords(final Unit unit, final Metrics m) {
        final List<Map<String, Object>> records = new ArrayList<>(1 + k);
        final List<String> names = spec.predictionNames();
        final List<Map<String, Object>> slices = new ArrayList<>();
        final String[] values = unit.slices();
        for (int s = 0; s < values.length; s++) {
            final Map<String, Object> sl = new LinkedHashMap<>();
            sl.put("field", spec.slices.get(s).name());
            sl.put("value", values[s]);
            slices.add(sl);
        }
        final boolean priorBase = Double.isNaN(m.logScore[0]);
        for (int j = 0; j <= k; j++) {
            final Map<String, Object> r = new LinkedHashMap<>();
            r.put("split", unit.split);
            r.put("unit", unit.key);
            r.put("time", unit.time() == EvaluationRow.NO_TIME ? null : unit.time() * 1000L);   // epoch micros: the primitive form of a TIMESTAMP
            r.put("prediction", names.get(j));
            r.put("n_rows", (long) unit.size());
            r.put("weight", unit.unitWeight);
            r.put("logScore", finiteOrNull(m.logScore[j]));
            r.put("logScoreBaseline", priorBase ? null : m.logScore[0]);
            r.put("excessLogScore", priorBase ? null : m.logScore[j] - m.logScore[0]);
            r.put("hitAt1", finiteOrNull(m.hitAt1[j]));
            r.put("brier", finiteOrNull(m.brier[j]));
            r.put("slices", slices);
            records.add(r);
        }
        return records;
    }

    static Double finiteOrNull(final double v) {
        return Double.isNaN(v) || Double.isInfinite(v) ? null : v;
    }
}
