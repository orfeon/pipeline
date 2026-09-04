package com.mercari.solution.util.pipeline.screen;

import com.mercari.solution.module.Schema;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Turns the combined accumulators into the scoring records and the run summary. Pure: the statistics,
 * placebo threshold, q-values and flags are all closed-form over the (small) accumulator set.
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

    /** Result of {@link #build}: the scoring records and the summary, as output-schema maps. */
    public record Result(List<Map<String, Object>> records, Map<String, Object> summary) {}

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
            final double c1 = a[ScoreAccumulator.C1], c2 = a[ScoreAccumulator.C2], c3 = a[ScoreAccumulator.C3], c4 = a[ScoreAccumulator.C4], c5 = a[ScoreAccumulator.C5];
            if (!(c5 > 0)) return Stats.degenerate(nObs);
            final double xMean = c4 / c5;
            s = c1 - xMean * c2;
            final double sxx = c3 - c4 * c4 / c5;
            if (spec.hasBaseline()) {
                h = sxx;
            } else {
                final double yMean = c2 / c5;
                h = yMean * (1 - yMean) * sxx;
            }
        }
        if (nObs < 2 || !(h > 0) || Double.isNaN(s) || h < 1e-300) return Stats.degenerate(nObs);
        final double chi2 = s * s / h;
        final double z = Math.signum(s) * Math.sqrt(chi2);
        final double estGain = nUnits > 0 ? chi2 / (2 * nUnits) : Double.NaN;
        return new Stats(s, h, s / h, chi2, z, estGain, ScreenMath.chiSquare1UpperTail(chi2), nObs, false);
    }

    public static Result build(final ScreenSpec spec, final Map<Integer, ScoreAccumulator> accumulators) {
        final ScoreAccumulator book = accumulators.getOrDefault(ScoreAccumulator.BOOKKEEPING_KEY, new ScoreAccumulator());
        final double[] b = book.getTotal();
        final double nUnits = b[ScoreAccumulator.UNITS_SCORED];
        final List<String> names = spec.columnNames();
        final int nTransforms = spec.transforms.size();

        // statistics per key
        final List<Map<String, Object>> records = new ArrayList<>();
        final List<Stats> statsList = new ArrayList<>();
        final List<Double> placeboGains = new ArrayList<>();
        for (int c = 0; c < names.size(); c++) {
            for (int t = 0; t < nTransforms; t++) {
                final int key = spec.key(c, t);
                final ScoreAccumulator acc = accumulators.getOrDefault(key, new ScoreAccumulator());
                final Stats st = stats(spec, acc.getTotal(), nUnits);
                statsList.add(st);
                if (spec.isPlacebo(c)) placeboGains.add(st.degenerate ? 0d : st.estGain);
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
                        nPeriods++;
                        if (!st.degenerate && st.z != 0 && Math.signum(ps.z) == Math.signum(st.z)) agree++;
                    }
                }
                r.put("periods_agree", agree);
                r.put("n_periods", nPeriods);
                r.put("period_z", periodRecords);
                r.put("placebo", spec.isPlacebo(c));
                r.put("degenerate", st.degenerate);
                records.add(r);
            }
        }

        // placebo threshold (theoretical chi2(1) quantile when no placebo columns are configured)
        final double thresholdTheoretical = nUnits > 0 ? ScreenMath.chiSquare1Quantile(spec.quantile) / (2 * nUnits) : Double.NaN;
        final double threshold;
        if (placeboGains.isEmpty()) {
            threshold = thresholdTheoretical;
        } else {
            final double[] sorted = placeboGains.stream().mapToDouble(Double::doubleValue).sorted().toArray();
            threshold = ScreenMath.quantile(sorted, spec.quantile);
        }

        // q-values over the candidate records
        final List<Integer> candidateRecords = new ArrayList<>();
        for (int i = 0; i < records.size(); i++) if (!(Boolean) records.get(i).get("placebo")) candidateRecords.add(i);
        final double[] p = new double[candidateRecords.size()];
        for (int i = 0; i < p.length; i++) p[i] = statsList.get(candidateRecords.get(i)).pValue;
        final double[] q = ScreenMath.benjaminiHochberg(p);
        for (int i = 0; i < p.length; i++) records.get(candidateRecords.get(i)).put("qValue", q[i]);

        // flags
        long nPassed = 0, nLeak = 0;
        final Map<String, Double> passedBest = new HashMap<>();
        for (int i = 0; i < records.size(); i++) {
            final Map<String, Object> r = records.get(i);
            final Stats st = statsList.get(i);
            final boolean placebo = (Boolean) r.get("placebo");
            final boolean passed = !placebo && !st.degenerate && !Double.isNaN(threshold) && st.estGain > threshold;
            final boolean leak = spec.leakZ != null && !st.degenerate && Math.abs(st.z) > spec.leakZ;
            r.put("threshold", threshold);
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
        summary.put("threshold", threshold);
        summary.put("thresholdTheoretical", thresholdTheoretical);
        summary.put("quantile", spec.quantile);
        summary.put("seed", spec.seed);
        summary.put("nRows", (long) b[ScoreAccumulator.ROWS_IN]);
        summary.put("nRowsTimeFiltered", (long) b[ScoreAccumulator.ROWS_TIME_FILTERED]);
        summary.put("nRowsInvalid", (long) b[ScoreAccumulator.ROWS_INVALID]);
        summary.put("nRowsScored", (long) b[ScoreAccumulator.ROWS_SCORED]);
        summary.put("nUnits", (long) nUnits);
        summary.put("nUnitsSkipped", (long) b[ScoreAccumulator.UNITS_SKIPPED]);
        summary.put("nCandidates", (long) spec.candidates.size());
        summary.put("nTransforms", (long) nTransforms);
        summary.put("nScored", (long) candidateRecords.size());
        summary.put("nPassed", nPassed);
        summary.put("nPlacebo", (long) placeboGains.size());
        summary.put("nLeakSuspect", nLeak);
        summary.put("timeField", spec.timeField);
        summary.put("timeFrom", spec.timeFrom);
        summary.put("timeTo", spec.timeTo);
        summary.put("minTime", book.getMinTime() == Long.MAX_VALUE ? null : book.getMinTime() * 1000L);
        summary.put("maxTime", book.getMaxTime() == Long.MIN_VALUE ? null : book.getMaxTime() * 1000L);
        summary.put("periodsBucket", spec.periodsBucket);
        summary.put("transforms", new ArrayList<>(spec.transforms));
        summary.put("candidates", new ArrayList<>(spec.candidates));
        summary.put("passedColumns", passedColumns);
        summary.put("notes", new ArrayList<>(spec.notes));
        return new Result(records, summary);
    }

    public static Schema recordSchema() {
        final Schema period = Schema.builder()
                .withField("period", Schema.FieldType.STRING)
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
                .withField("threshold", Schema.FieldType.FLOAT64)
                .withField("thresholdTheoretical", Schema.FieldType.FLOAT64)
                .withField("quantile", Schema.FieldType.FLOAT64)
                .withField("seed", Schema.FieldType.INT64)
                .withField("nRows", Schema.FieldType.INT64)
                .withField("nRowsTimeFiltered", Schema.FieldType.INT64)
                .withField("nRowsInvalid", Schema.FieldType.INT64)
                .withField("nRowsScored", Schema.FieldType.INT64)
                .withField("nUnits", Schema.FieldType.INT64)
                .withField("nUnitsSkipped", Schema.FieldType.INT64)
                .withField("nCandidates", Schema.FieldType.INT64)
                .withField("nTransforms", Schema.FieldType.INT64)
                .withField("nScored", Schema.FieldType.INT64)
                .withField("nPassed", Schema.FieldType.INT64)
                .withField("nPlacebo", Schema.FieldType.INT64)
                .withField("nLeakSuspect", Schema.FieldType.INT64)
                .withField("timeField", Schema.FieldType.STRING)
                .withField("timeFrom", Schema.FieldType.STRING)
                .withField("timeTo", Schema.FieldType.STRING)
                .withField("minTime", Schema.FieldType.TIMESTAMP)
                .withField("maxTime", Schema.FieldType.TIMESTAMP)
                .withField("periodsBucket", Schema.FieldType.STRING)
                .withField("transforms", Schema.FieldType.array(Schema.FieldType.STRING))
                .withField("candidates", Schema.FieldType.array(Schema.FieldType.STRING))
                .withField("passedColumns", Schema.FieldType.array(Schema.FieldType.STRING))
                .withField("notes", Schema.FieldType.array(Schema.FieldType.STRING))
                .build();
    }

    /** One-paragraph description of the resolved spec for the assembly log. */
    public static String describe(final ScreenSpec spec) {
        final Set<String> parts = new LinkedHashSet<>();
        parts.add("family=" + spec.family);
        if (spec.group != null) parts.add("group=" + spec.group);
        parts.add("label=" + (spec.labelExpr != null ? "expr(" + spec.labelExpr + ")" : spec.labelField));
        parts.add("baseline=" + (spec.hasBaseline() ? spec.baselineField + ":" + spec.baselineForm : "prior"));
        if (spec.timeField != null) parts.add("time=" + spec.timeField + (spec.timeFrom != null ? " from " + spec.timeFrom : "") + (spec.timeTo != null ? " to " + spec.timeTo : ""));
        if (spec.weightField != null) parts.add("weight=" + spec.weightField);
        parts.add("candidates=" + spec.candidates.size() + " " + spec.candidates);
        parts.add("transforms=" + spec.transforms);
        parts.add("placebo=noise:" + spec.noise + (spec.hasShuffle() ? " shuffle:" + spec.shuffleN + "(" + spec.shuffleField + ")" : "") + " q" + spec.quantile + " seed=" + spec.seed);
        if (spec.periodsBucket != null) parts.add("periods=" + spec.periodsField + "/" + spec.periodsBucket);
        if (spec.leakZ != null) parts.add("leakZ=" + spec.leakZ);
        if (!spec.notes.isEmpty()) parts.add("notes=" + spec.notes);
        return "screen " + String.join(" ", parts);
    }
}
