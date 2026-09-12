package com.mercari.solution.util.pipeline.evaluation;

import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.glm.Baselines;
import com.mercari.solution.util.pipeline.glm.StatMath;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Turns the combined accumulators into the output records (design §8), pure: the metrics with their bootstrap
 * intervals and pair differences, the calibration tables, the summary; the output schemas; {@code describe}
 * for the assembly log.
 */
public final class EvaluationReport {

    private EvaluationReport() {}

    private static final String SEP = MetricAccumulator.SEP;
    public static final List<String> METRICS = List.of("logScore", "excessLogScore", "hitAt1", "brier");
    static final double Z95 = 1.959963984540054;

    /** Result of {@link #build}: the metrics records and the summary, as output-schema maps. */
    public record Result(List<Map<String, Object>> records, Map<String, Object> summary) {}

    // ---- metrics -------------------------------------------------------------------------------------------

    /**
     * A metric over the sums of one key: the weighted mean of the slot, the excess as the difference of two
     * weighted means; in binomial prior mode the baseline log score is the entropy of the label mean.
     */
    static double metric(final EvaluationSpec spec, final String name, final double[] sums) {
        final double w = sums[MetricAccumulator.W];
        if (!(w > 0)) return Double.NaN;
        return switch (name) {
            case "logScore" -> sums[MetricAccumulator.LOG] / w;
            case "excessLogScore" -> sums[MetricAccumulator.LOG] / w - baselineLogScore(spec, sums);
            case "hitAt1" -> sums[MetricAccumulator.HIT] / w;
            case "brier" -> sums[MetricAccumulator.BRIER] / w;
            default -> throw new IllegalArgumentException("unknown metric " + name);
        };
    }

    /** The reference log score per unit: the accumulated one, or the entropy of the split's label mean (binomial prior mode). */
    static double baselineLogScore(final EvaluationSpec spec, final double[] sums) {
        final double w = sums[MetricAccumulator.W];
        if (spec.hasBaseline() || spec.isGrouped()) return sums[MetricAccumulator.LOG_BASE] / w;
        final double mean = Baselines.clamp(sums[MetricAccumulator.WY] / w);
        return mean * Math.log(mean) + (1 - mean) * Math.log(1 - mean);
    }

    /** The replicate's sums in the slot layout (W .. BRIER from the replicate, the counts from the total). */
    static double[] replicate(final MetricAccumulator acc, final int b) {
        final double[] sums = new double[MetricAccumulator.SLOTS];
        final double[] boot = acc.getBoot();
        for (int s = 0; s < MetricAccumulator.BOOT_SLOTS; s++) sums[MetricAccumulator.BOOT_FIRST + s] = boot[b * MetricAccumulator.BOOT_SLOTS + s];
        return sums;
    }

    /** The 2.5 / 97.5 percentiles of a replicate series (NaN pair when empty or non-finite). */
    static double[] interval(final double[] values) {
        int m = 0;
        for (final double v : values) if (Double.isFinite(v)) m++;
        if (m < 2) return new double[]{Double.NaN, Double.NaN};
        final double[] finite = new double[m];
        int i = 0;
        for (final double v : values) if (Double.isFinite(v)) finite[i++] = v;
        Arrays.sort(finite);
        return new double[]{StatMath.quantile(finite, 0.025), StatMath.quantile(finite, 0.975)};
    }

    public static Result build(final EvaluationSpec spec, final Map<String, MetricAccumulator> accumulators) {
        final List<String> names = spec.predictionNames();
        final int k = spec.predictions.size();
        final Map<String, String> roles = spec.roles();
        // (split, slice, value) → prediction index → accumulator
        final Map<String, Map<Integer, MetricAccumulator>> cells = new LinkedHashMap<>();
        final Map<String, String[]> cellParts = new LinkedHashMap<>();
        for (final Map.Entry<String, MetricAccumulator> e : accumulators.entrySet()) {
            final String[] parts = EvaluationScorer.parseKey(e.getKey());
            if (parts == null) continue;
            final String cell = parts[0] + SEP + parts[2] + SEP + parts[3];
            cells.computeIfAbsent(cell, c -> new TreeMap<>()).put(Integer.parseInt(parts[1]), e.getValue());
            cellParts.putIfAbsent(cell, parts);
        }
        // ordered: splits as declared, the overall cell first, then the slices in order with sorted values
        final List<String> ordered = new ArrayList<>(cells.keySet());
        final List<String> splitNames = spec.splitNames();
        ordered.sort((a, b) -> {
            final String[] pa = cellParts.get(a), pb = cellParts.get(b);
            final int sa = splitNames.indexOf(pa[0]), sb = splitNames.indexOf(pb[0]);
            if (sa != sb) return Integer.compare(sa, sb);
            final int ia = Integer.parseInt(pa[2]), ib = Integer.parseInt(pb[2]);
            if (ia != ib) return Integer.compare(ia, ib);
            return pa[3].compareTo(pb[3]);
        });

        final List<Map<String, Object>> records = new ArrayList<>();
        for (final String cell : ordered) {
            final String[] parts = cellParts.get(cell);
            final Map<Integer, MetricAccumulator> byPrediction = cells.get(cell);
            final int sliceIndex = Integer.parseInt(parts[2]);
            final String sliceName = sliceIndex < 0 ? null : spec.slices.get(sliceIndex).name();
            final String sliceValue = sliceIndex < 0 ? null : parts[3];
            // value and replicate series per prediction
            final Map<Integer, Map<String, double[]>> series = new LinkedHashMap<>();
            for (final Map.Entry<Integer, MetricAccumulator> e : byPrediction.entrySet()) {
                final MetricAccumulator acc = e.getValue();
                final Map<String, double[]> s = new LinkedHashMap<>();
                final int b = acc.samples();
                for (final String m : METRICS) {
                    final double[] values = new double[b + 1];
                    values[0] = metric(spec, m, acc.getTotal());
                    s.put(m, values);
                }
                for (int r = 0; r < b; r++) {
                    final double[] sums = replicate(acc, r);   // one replicate vector feeds every metric
                    for (final String m : METRICS) s.get(m)[1 + r] = metric(spec, m, sums);
                }
                series.put(e.getKey(), s);
            }
            for (final Map.Entry<Integer, MetricAccumulator> e : byPrediction.entrySet()) {
                final int j = e.getKey();
                final MetricAccumulator acc = e.getValue();
                final Map<String, Object> r = new LinkedHashMap<>();
                r.put("split", parts[0]);
                r.put("role", roles.get(parts[0]));
                r.put("prediction", names.get(j));
                r.put("pair", null);
                r.put("slice", sliceName);
                r.put("value", sliceValue);
                putCounts(r, acc);
                for (final String m : METRICS) {
                    final double[] values = series.get(j).get(m);
                    final boolean baselineExcess = j == 0 && "excessLogScore".equals(m);
                    r.put(m, baselineExcess ? Double.valueOf(0d) : finiteOrNull(values[0]));
                    final double[] ci = baselineExcess || !spec.hasBootstrap() ? new double[]{Double.NaN, Double.NaN} : interval(Arrays.copyOfRange(values, 1, values.length));
                    r.put(m + "_lo", finiteOrNull(ci[0]));
                    r.put(m + "_hi", finiteOrNull(ci[1]));
                }
                final Double logScore = (Double) r.get("logScore");
                r.put("logloss", logScore == null ? null : -logScore);
                records.add(r);
            }
            // pair differences A − B over the same units (the weights are per unit, so the series subtract)
            for (int a = 1; a <= k; a++) {
                for (int b = a + 1; b <= k; b++) {
                    if (!series.containsKey(a) || !series.containsKey(b)) continue;
                    final Map<String, Object> r = new LinkedHashMap<>();
                    r.put("split", parts[0]);
                    r.put("role", roles.get(parts[0]));
                    r.put("prediction", names.get(a));
                    r.put("pair", names.get(b));
                    r.put("slice", sliceName);
                    r.put("value", sliceValue);
                    putCounts(r, byPrediction.get(a));
                    for (final String m : METRICS) {
                        final double[] va = series.get(a).get(m), vb = series.get(b).get(m);
                        final double[] diff = new double[va.length];
                        for (int i = 0; i < diff.length; i++) diff[i] = va[i] - vb[i];
                        r.put(m, finiteOrNull(diff[0]));
                        final double[] ci = spec.hasBootstrap() ? interval(Arrays.copyOfRange(diff, 1, diff.length)) : new double[]{Double.NaN, Double.NaN};
                        r.put(m + "_lo", finiteOrNull(ci[0]));
                        r.put(m + "_hi", finiteOrNull(ci[1]));
                    }
                    final Double logScore = (Double) r.get("logScore");
                    r.put("logloss", logScore == null ? null : -logScore);
                    records.add(r);
                }
            }
        }
        return new Result(records, summary(spec, accumulators));
    }

    private static void putCounts(final Map<String, Object> r, final MetricAccumulator acc) {
        final double[] t = acc.getTotal();
        r.put("n_units", (long) t[MetricAccumulator.N_UNITS]);
        r.put("n_rows", (long) t[MetricAccumulator.N_ROWS]);
        r.put("positives", t[MetricAccumulator.WY]);
        r.put("weight", t[MetricAccumulator.W]);
    }

    // ---- summary -------------------------------------------------------------------------------------------

    static Map<String, Object> summary(final EvaluationSpec spec, final Map<String, MetricAccumulator> accumulators) {
        final Map<String, Object> s = new LinkedHashMap<>();
        final MetricAccumulator rows = accumulators.getOrDefault(MetricAccumulator.ROWS_KEY, new MetricAccumulator());
        final List<String> notes = new ArrayList<>(spec.notes);
        s.put("family", spec.family);
        s.put("group", spec.group);
        s.put("label", spec.labelExpr != null ? spec.labelExpr : spec.labelField);
        s.put("baseline", spec.baselineField);
        s.put("baselineForm", spec.hasBaseline() ? spec.baselineForm : null);
        s.put("weight", spec.weightField);
        s.put("timeField", spec.timeField);
        s.put("splitField", spec.splitField);
        final List<String> predictions = new ArrayList<>();
        for (final EvaluationSpec.Prediction p : spec.predictions) predictions.add(p.name);
        s.put("predictions", predictions);
        final List<Map<String, Object>> splits = new ArrayList<>();
        long nUnits = 0, nUnitsSkipped = 0;
        final Map<String, long[]> observed = new LinkedHashMap<>();
        for (final EvaluationSpec.Split sp : spec.splits) {
            final MetricAccumulator book = accumulators.getOrDefault(MetricAccumulator.SPLIT_KEY_PREFIX + sp.name, new MetricAccumulator());
            final Map<String, Object> r = new LinkedHashMap<>();
            r.put("name", sp.name);
            r.put("role", sp.role);
            r.put("from", sp.from);
            r.put("to", sp.to);
            r.put("minTime", book.getMinTime() == Long.MAX_VALUE ? null : book.getMinTime() * 1000L);
            r.put("maxTime", book.getMaxTime() == Long.MIN_VALUE ? null : book.getMaxTime() * 1000L);
            r.put("nUnits", (long) book.getTotal()[MetricAccumulator.UNITS]);
            r.put("nUnitsSkipped", (long) book.getTotal()[MetricAccumulator.UNITS_SKIPPED]);
            r.put("nRows", (long) book.getTotal()[MetricAccumulator.ROWS]);
            splits.add(r);
            nUnits += (long) book.getTotal()[MetricAccumulator.UNITS];
            nUnitsSkipped += (long) book.getTotal()[MetricAccumulator.UNITS_SKIPPED];
            if (book.getMinTime() != Long.MAX_VALUE) observed.put(sp.name, new long[]{book.getMinTime(), book.getMaxTime()});
        }
        // a selection split whose observed range overlaps a report split's: the selection has seen the report period
        for (final EvaluationSpec.Split sel : spec.splits) {
            if (!sel.isSelection() || !observed.containsKey(sel.name)) continue;
            for (final EvaluationSpec.Split rep : spec.splits) {
                if (rep.isSelection() || !observed.containsKey(rep.name)) continue;
                final long[] a = observed.get(sel.name), b = observed.get(rep.name);
                if (a[1] >= b[0]) notes.add("split " + sel.name + " (selection) overlaps split " + rep.name + " (report) in time: a random split reports on rows the selection has seen");
            }
        }
        s.put("splits", splits);
        s.put("nRows", (long) rows.getTotal()[MetricAccumulator.ROWS_IN]);
        s.put("nRowsInvalid", (long) rows.getTotal()[MetricAccumulator.ROWS_INVALID]);
        s.put("nRowsUnassigned", (long) rows.getTotal()[MetricAccumulator.ROWS_UNASSIGNED]);
        s.put("nUnits", nUnits);
        s.put("nUnitsSkipped", nUnitsSkipped);
        s.put("bootstrapSamples", (long) spec.bootstrapSamples);
        s.put("bootstrapSeed", spec.bootstrapSeed);
        s.put("bootstrapUnit", spec.bootstrapUnit);
        s.put("nCalibrationTables", (long) spec.tables.size());
        final List<String> slices = new ArrayList<>();
        for (final EvaluationSpec.Slice sl : spec.slices) slices.add(sl.name());
        s.put("slices", slices);
        s.put("parametersHash", spec.parametersHash);
        s.put("planHash", spec.manifestPlanHash);
        s.put("outputHash", spec.manifestOutputHash);
        s.put("notes", notes);
        return s;
    }

    // ---- calibration ---------------------------------------------------------------------------------------

    /** Layout of a calibration bin vector. */
    public static final int BIN_N = 0, BIN_POSITIVES = 1, BIN_Q = 2, BIN_P = 3, BIN_UTILITY = 4, BIN_SLOTS = 5;

    public static String tableKey(final String split, final int prediction, final int table) {
        return split + SEP + prediction + SEP + table;
    }

    public static String binKey(final String split, final int prediction, final int table, final int bin) {
        return tableKey(split, prediction, table) + SEP + bin;
    }

    static double logit(final double p) {
        final double c = Baselines.clamp(p);
        return Math.log(c / (1 - c));
    }

    /**
     * The value a reliability table bins a row by (NaN = the row does not enter the table): the prediction, the
     * divergence logit q − logit p, or the table's field ({@link AlignedRow#fields} is indexed by table position).
     *
     * @param t     the table at position {@code tableIndex} in {@code calibration[]}
     * @param j     the prediction index (0-based, without the baseline)
     */
    public static double tableValue(final EvaluationSpec.Table t, final int tableIndex, final AlignedRow row, final int j) {
        return switch (t.by) {
            case EvaluationSpec.BY_PREDICTION -> row.predictions[j];
            case EvaluationSpec.BY_DIVERGENCE -> Double.isNaN(row.baseline) ? Double.NaN : logit(row.predictions[j]) - logit(row.baseline);
            case EvaluationSpec.BY_FIELD -> row.fields[tableIndex];
            default -> throw new IllegalArgumentException("unknown by " + t.by);
        };
    }

    /** The bin of a value against ascending interior edges: {@code (edges[i-1], edges[i]]}, the first bin open below, the last open above. */
    public static int bin(final double value, final double[] edges) {
        int b = 0;
        while (b < edges.length && value > edges[b]) b++;
        return b;
    }

    /** Wilson 95% interval of a rate. */
    static double[] wilson(final double positives, final double n) {
        if (!(n > 0)) return new double[]{Double.NaN, Double.NaN};
        final double p = Math.min(1d, Math.max(0d, positives / n));
        final double z2 = Z95 * Z95;
        final double denominator = 1 + z2 / n;
        final double centre = (p + z2 / (2 * n)) / denominator;
        final double half = Z95 * Math.sqrt(p * (1 - p) / n + z2 / (4 * n * n)) / denominator;
        return new double[]{Math.max(0d, centre - half), Math.min(1d, centre + half)};
    }

    /**
     * The calibration records: every bin of every table per split × prediction set, the bounds from the
     * declared edges / thresholds or from the sketch of the (split, prediction, table) stream.
     */
    public static List<Map<String, Object>> calibration(final EvaluationSpec spec, final Map<String, double[]> bins, final Map<String, SketchAccumulator> sketches) {
        final List<Map<String, Object>> records = new ArrayList<>();
        final List<String> names = spec.predictionNames();
        for (final String split : spec.splitNames()) {
            for (int j = 1; j < names.size(); j++) {
                for (int t = 0; t < spec.tables.size(); t++) {
                    final EvaluationSpec.Table table = spec.tables.get(t);
                    final int count = table.binCount();
                    double[] edges = null;
                    double min = Double.NaN, max = Double.NaN;
                    if (table.isQuantile()) {
                        final SketchAccumulator sketch = sketches == null ? null : sketches.get(tableKey(split, j, t));
                        if (sketch == null || sketch.isEmpty()) continue;
                        edges = sketch.edges(table.bins);
                        min = sketch.min();
                        max = sketch.max();
                    } else if (!table.isEdge()) {
                        edges = table.edges;
                    }
                    for (int b = 0; b < count; b++) {
                        final double[] v = bins.getOrDefault(binKey(split, j, t, b), new double[BIN_SLOTS]);
                        final Map<String, Object> r = new LinkedHashMap<>();
                        r.put("split", split);
                        r.put("prediction", names.get(j));
                        r.put("type", table.type);
                        r.put("by", table.by);
                        r.put("field", table.field);
                        r.put("table", (long) t);
                        r.put("bin", (long) b);
                        if (table.isEdge()) {
                            r.put("lower", table.thresholds[b]);
                            r.put("upper", null);
                        } else {
                            r.put("lower", b == 0 ? finiteOrNull(min) : Double.valueOf(edges[b - 1]));
                            r.put("upper", b == count - 1 ? finiteOrNull(max) : Double.valueOf(edges[b]));
                        }
                        final double n = v[BIN_N];
                        r.put("n", (long) n);
                        r.put("positives", v[BIN_POSITIVES]);
                        r.put("p_model", n > 0 ? v[BIN_Q] / n : null);
                        r.put("p_baseline", n > 0 && !Double.isNaN(v[BIN_P]) ? v[BIN_P] / n : null);
                        r.put("rate", n > 0 ? v[BIN_POSITIVES] / n : null);
                        final double[] ci = wilson(v[BIN_POSITIVES], n);
                        r.put("rate_lo", finiteOrNull(ci[0]));
                        r.put("rate_hi", finiteOrNull(ci[1]));
                        r.put("utility", spec.hasUtility() && n > 0 ? v[BIN_UTILITY] / n : null);
                        records.add(r);
                    }
                }
            }
        }
        return records;
    }

    static Double finiteOrNull(final double v) {
        return Double.isNaN(v) || Double.isInfinite(v) ? null : v;
    }

    // ---- schemas -------------------------------------------------------------------------------------------

    public static Schema metricsSchema() {
        final Schema.Builder b = Schema.builder()
                .withField("split", Schema.FieldType.STRING)
                .withField("role", Schema.FieldType.STRING)
                .withField("prediction", Schema.FieldType.STRING)
                .withField("pair", Schema.FieldType.STRING)
                .withField("slice", Schema.FieldType.STRING)
                .withField("value", Schema.FieldType.STRING)
                .withField("n_units", Schema.FieldType.INT64)
                .withField("n_rows", Schema.FieldType.INT64)
                .withField("positives", Schema.FieldType.FLOAT64)
                .withField("weight", Schema.FieldType.FLOAT64);
        for (final String m : METRICS) {
            b.withField(m, Schema.FieldType.FLOAT64)
                    .withField(m + "_lo", Schema.FieldType.FLOAT64)
                    .withField(m + "_hi", Schema.FieldType.FLOAT64);
        }
        return b.withField("logloss", Schema.FieldType.FLOAT64).build();
    }

    public static Schema calibrationSchema() {
        return Schema.builder()
                .withField("split", Schema.FieldType.STRING)
                .withField("prediction", Schema.FieldType.STRING)
                .withField("type", Schema.FieldType.STRING)
                .withField("by", Schema.FieldType.STRING)
                .withField("field", Schema.FieldType.STRING)
                .withField("table", Schema.FieldType.INT64)
                .withField("bin", Schema.FieldType.INT64)
                .withField("lower", Schema.FieldType.FLOAT64)
                .withField("upper", Schema.FieldType.FLOAT64)
                .withField("n", Schema.FieldType.INT64)
                .withField("positives", Schema.FieldType.FLOAT64)
                .withField("p_model", Schema.FieldType.FLOAT64)
                .withField("p_baseline", Schema.FieldType.FLOAT64)
                .withField("rate", Schema.FieldType.FLOAT64)
                .withField("rate_lo", Schema.FieldType.FLOAT64)
                .withField("rate_hi", Schema.FieldType.FLOAT64)
                .withField("utility", Schema.FieldType.FLOAT64)
                .build();
    }

    public static Schema unitsSchema() {
        final Schema slice = Schema.builder()
                .withField("field", Schema.FieldType.STRING)
                .withField("value", Schema.FieldType.STRING)
                .build();
        return Schema.builder()
                .withField("split", Schema.FieldType.STRING)
                .withField("unit", Schema.FieldType.STRING)
                .withField("time", Schema.FieldType.TIMESTAMP)
                .withField("prediction", Schema.FieldType.STRING)
                .withField("n_rows", Schema.FieldType.INT64)
                .withField("weight", Schema.FieldType.FLOAT64)
                .withField("logScore", Schema.FieldType.FLOAT64)
                .withField("logScoreBaseline", Schema.FieldType.FLOAT64)
                .withField("excessLogScore", Schema.FieldType.FLOAT64)
                .withField("hitAt1", Schema.FieldType.FLOAT64)
                .withField("brier", Schema.FieldType.FLOAT64)
                .withField("slices", Schema.FieldType.array(Schema.FieldType.element(slice)))
                .build();
    }

    public static Schema summarySchema() {
        final Schema split = Schema.builder()
                .withField("name", Schema.FieldType.STRING)
                .withField("role", Schema.FieldType.STRING)
                .withField("from", Schema.FieldType.STRING)
                .withField("to", Schema.FieldType.STRING)
                .withField("minTime", Schema.FieldType.TIMESTAMP)
                .withField("maxTime", Schema.FieldType.TIMESTAMP)
                .withField("nUnits", Schema.FieldType.INT64)
                .withField("nUnitsSkipped", Schema.FieldType.INT64)
                .withField("nRows", Schema.FieldType.INT64)
                .build();
        return Schema.builder()
                .withField("family", Schema.FieldType.STRING)
                .withField("group", Schema.FieldType.STRING)
                .withField("label", Schema.FieldType.STRING)
                .withField("baseline", Schema.FieldType.STRING)
                .withField("baselineForm", Schema.FieldType.STRING)
                .withField("weight", Schema.FieldType.STRING)
                .withField("timeField", Schema.FieldType.STRING)
                .withField("splitField", Schema.FieldType.STRING)
                .withField("predictions", Schema.FieldType.array(Schema.FieldType.STRING))
                .withField("splits", Schema.FieldType.array(Schema.FieldType.element(split)))
                .withField("nRows", Schema.FieldType.INT64)
                .withField("nRowsInvalid", Schema.FieldType.INT64)
                .withField("nRowsUnassigned", Schema.FieldType.INT64)
                .withField("nUnits", Schema.FieldType.INT64)
                .withField("nUnitsSkipped", Schema.FieldType.INT64)
                .withField("bootstrapSamples", Schema.FieldType.INT64)
                .withField("bootstrapSeed", Schema.FieldType.INT64)
                .withField("bootstrapUnit", Schema.FieldType.STRING)
                .withField("nCalibrationTables", Schema.FieldType.INT64)
                .withField("slices", Schema.FieldType.array(Schema.FieldType.STRING))
                .withField("parametersHash", Schema.FieldType.STRING)
                .withField("planHash", Schema.FieldType.STRING)
                .withField("outputHash", Schema.FieldType.STRING)
                .withField("notes", Schema.FieldType.array(Schema.FieldType.STRING))
                .build();
    }

    /** One-paragraph description of the resolved spec for the assembly log. */
    public static String describe(final EvaluationSpec spec) {
        final List<String> parts = new ArrayList<>();
        parts.add("family=" + spec.family);
        if (spec.group != null) parts.add("group=" + spec.group);
        parts.add("label=" + (spec.labelExpr != null ? "expr(" + spec.labelExpr + ")" : spec.labelField));
        parts.add("baseline=" + (spec.hasBaseline() ? spec.baselineField + ":" + spec.baselineForm : "prior"));
        final List<String> predictions = new ArrayList<>();
        for (final EvaluationSpec.Prediction p : spec.predictions) predictions.add(p.describe());
        parts.add("predictions=" + predictions);
        final List<String> splits = new ArrayList<>();
        for (final EvaluationSpec.Split s : spec.splits) splits.add(s.name + ":" + s.role + (s.from != null || s.to != null ? "[" + s.from + ".." + s.to + "]" : ""));
        parts.add("splits=" + (spec.splitField != null ? spec.splitField + " " : "") + splits);
        if (spec.timeField != null) parts.add("time=" + spec.timeField);
        if (spec.weightField != null) parts.add("weight=" + spec.weightField);
        parts.add("bootstrap=" + spec.bootstrapSamples + " seed=" + spec.bootstrapSeed + (spec.bootstrapUnit != null ? " unit=" + spec.bootstrapUnit : ""));
        if (!spec.tables.isEmpty()) parts.add("calibration=" + spec.tables.size() + " tables" + (spec.hasQuantileTables() ? " (+1 sketch pass)" : ""));
        if (!spec.slices.isEmpty()) {
            final List<String> slices = new ArrayList<>();
            for (final EvaluationSpec.Slice sl : spec.slices) slices.add(sl.name());
            parts.add("slices=" + slices);
        }
        if (spec.utilityField != null) parts.add("utility=" + spec.utilityField);
        if (!spec.notes.isEmpty()) parts.add("notes=" + spec.notes);
        return "evaluation " + String.join(" ", parts);
    }
}
