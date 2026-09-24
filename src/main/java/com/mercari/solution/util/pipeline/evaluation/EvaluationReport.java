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
    public static final List<String> METRICS = List.of("logScore", "excessLogScore", "hitAt1", "brier", "utility");
    /** the metrics that describe the outcomes, not a prediction set: the same under every set, no pair difference */
    public static final List<String> SET_INDEPENDENT = List.of("utility");
    static final double Z95 = 1.959963984540054;

    /** Result of {@link #build}: the metrics records, the slice discovery records and the summary, as output-schema maps. */
    public record Result(List<Map<String, Object>> records, List<Map<String, Object>> slices, Map<String, Object> summary) {}

    /** key prefix of a discovery cell in the metrics accumulator map */
    public static final String DISCOVERY_PREFIX = "\u0001disc\u0001";

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
            case "utility" -> spec.hasUtility() ? sums[MetricAccumulator.UTILITY] / w : Double.NaN;
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

    /** The replicate's sums in the slot layout (W .. UTILITY from the replicate; the counts stay 0). */
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
        return build(spec, accumulators, null);
    }

    /** @param fits the calibration fit results (null without fits): the summary's {@code fits} records */
    public static Result build(final EvaluationSpec spec, final Map<String, MetricAccumulator> accumulators, final FitResults fits) {
        final List<String> names = spec.predictionNames();
        final int k = spec.setCount();
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
                        if (SET_INDEPENDENT.contains(m)) {
                            r.put(m, null);
                            r.put(m + "_lo", null);
                            r.put(m + "_hi", null);
                            continue;
                        }
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
        final Discovery discovery = spec.hasDiscovery() ? discovery(spec, accumulators) : null;
        return new Result(records, discovery == null ? List.of() : discovery.records, summary(spec, accumulators, fits, discovery));
    }

    private static void putCounts(final Map<String, Object> r, final MetricAccumulator acc) {
        final double[] t = acc.getTotal();
        r.put("n_units", (long) t[MetricAccumulator.N_UNITS]);
        r.put("n_rows", (long) t[MetricAccumulator.N_ROWS]);
        r.put("positives", t[MetricAccumulator.WY]);
        r.put("weight", t[MetricAccumulator.W]);
    }

    // ---- summary -------------------------------------------------------------------------------------------

    static Map<String, Object> summary(final EvaluationSpec spec, final Map<String, MetricAccumulator> accumulators, final FitResults fits, final Discovery discovery) {
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
            final long duplicates = (long) book.getTotal()[MetricAccumulator.ROWS_DUPLICATE];
            r.put("nRowsDuplicate", duplicates);
            splits.add(r);
            nUnits += (long) book.getTotal()[MetricAccumulator.UNITS];
            nUnitsSkipped += (long) book.getTotal()[MetricAccumulator.UNITS_SKIPPED];
            if (book.getMinTime() != Long.MAX_VALUE) observed.put(sp.name, new long[]{book.getMinTime(), book.getMaxTime()});
            // integrity: a split without a unit (a report split has nothing to report), a row twice in a unit
            if (book.getTotal()[MetricAccumulator.UNITS] == 0) {
                notes.add("split " + sp.name + " (" + sp.role + ") has no scored unit" + (sp.isSelection()
                        ? ": the fits and the discovery on it have no data" : ": nothing to report; check the split range and the input"));
            }
            if (duplicates > 0) {
                notes.add("split " + sp.name + ": " + duplicates + " duplicate rows (the same rowId twice within a unit); their units' metrics count them twice");
            }
        }
        // a slice / dimension declared as a group-level attribute whose value the rows of a unit disagree on
        for (int i = 0; i < spec.slices.size(); i++) {
            final MetricAccumulator varies = accumulators.get(MetricAccumulator.SLICE_VARIES_KEY_PREFIX + i);
            if (varies == null) continue;
            notes.add("slice " + spec.slices.get(i).name() + " is not constant within a unit (" + (long) varies.getTotal()[0]
                    + " units; the first row's value was used): a row-level slice needs family: binomial");
        }
        if (spec.hasDiscovery()) {
            for (int d = 0; d < spec.discovery.dimensions.size(); d++) {
                final MetricAccumulator varies = accumulators.get(MetricAccumulator.DIMENSION_VARIES_KEY_PREFIX + d);
                if (varies == null) continue;
                notes.add("sliceDiscovery dimension " + spec.discovery.dimensions.get(d).field + " is not constant within a unit (" + (long) varies.getTotal()[0]
                        + " units; the first row's value was used): a row-level dimension needs family: binomial");
            }
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
        final List<Map<String, Object>> fitRecords = new ArrayList<>();
        if (fits != null) {
            for (final Map<String, Object> r : fits.records) {
                fitRecords.add(new LinkedHashMap<>(r));
                if (Boolean.FALSE.equals(r.get("fitted"))) notes.add("calibration " + r.get("type") + " on " + r.get("prediction") + " produced no estimate" + (r.get("note") != null ? ": " + r.get("note") : ""));
            }
        }
        s.put("fits", fitRecords);
        s.put("discovery", discovery == null ? new ArrayList<>() : discovery.summary);
        if (discovery != null) notes.addAll(discovery.notes);
        final List<String> slices = new ArrayList<>();
        for (final EvaluationSpec.Slice sl : spec.slices) slices.add(sl.name());
        s.put("slices", slices);
        s.put("parametersHash", spec.parametersHash);
        s.put("planHash", spec.manifestPlanHash);
        s.put("outputHash", spec.manifestOutputHash);
        s.put("notes", notes);
        return s;
    }

    // ---- slice discovery -----------------------------------------------------------------------------------

    /** The slice discovery's outcome: the records (filtered by {@code output}), one summary record per set, notes. */
    public static final class Discovery {
        public final List<Map<String, Object>> records = new ArrayList<>();
        public final List<Map<String, Object>> summary = new ArrayList<>();
        public final List<String> notes = new ArrayList<>();
    }

    /**
     * The null threshold of the maximum |z| over {@code candidates} independent standard normals at level
     * {@code quantile}: Φ⁻¹((1 + quantile^(1/K)) / 2). Candidates overlap, so the true maximum is smaller: the
     * threshold is conservative.
     */
    public static double discoveryThreshold(final int candidates, final double quantile) {
        if (candidates <= 0) return Double.NaN;
        return StatMath.inverseNormal((1 + Math.pow(quantile, 1d / candidates)) / 2);
    }

    /**
     * The z of a slice's mean against the split's overall mean under the random-subset (exchangeability) null:
     * (m_s − m) / (σ √((1/n)(1 − n/N))), with σ the unit-level standard deviation over the split.
     */
    public static double discoveryZ(final double n, final double sum, final double total, final double totalSum, final double totalSumSq) {
        if (!(n > 1) || !(total > n)) return Double.NaN;
        final double mean = totalSum / total;
        final double variance = totalSumSq / total - mean * mean;
        if (!(variance > 0)) return Double.NaN;
        final double se = Math.sqrt(variance * (1d / n) * (1d - n / total));
        return (sum / n - mean) / se;
    }

    static Discovery discovery(final EvaluationSpec spec, final Map<String, MetricAccumulator> accumulators) {
        final EvaluationSpec.Discovery d = spec.discovery;
        final Discovery out = new Discovery();
        final List<String> names = spec.predictionNames();
        // cells per (split, set): dims csv + values → [n, Σd, Σd²]
        final Map<String, Map<String, double[]>> cells = new LinkedHashMap<>();
        for (final Map.Entry<String, MetricAccumulator> e : accumulators.entrySet()) {
            if (!e.getKey().startsWith(DISCOVERY_PREFIX)) continue;
            final String[] parts = EvaluationScorer.parseDiscoveryKey(e.getKey().substring(DISCOVERY_PREFIX.length()));
            if (parts == null) continue;
            final double[] t = e.getValue().getTotal();
            cells.computeIfAbsent(parts[0] + SEP + parts[1], k -> new LinkedHashMap<>()).put(parts[2] + SEP + parts[3], new double[]{t[0], t[1], t[2]});
        }
        for (final int set : d.sets) {
            final String name = names.get(1 + set);
            final Map<String, double[]> discover = cells.getOrDefault(d.discoverOn + SEP + set, Map.of());
            final Map<String, double[]> confirm = cells.getOrDefault(d.confirmOn + SEP + set, Map.of());
            final double[] all = discover.get(SEP);
            final double[] allConfirm = confirm.get(SEP);
            final Map<String, Object> sr = new LinkedHashMap<>();
            sr.put("prediction", name);
            sr.put("metric", d.metric);
            if (all == null || !(all[0] > 1)) {
                sr.put("nCandidates", 0L);
                sr.put("threshold", null);
                sr.put("nPassed", 0L);
                sr.put("nConfirmed", 0L);
                sr.put("note", "no scored unit in split " + d.discoverOn);
                out.summary.add(sr);
                continue;
            }
            // candidates: the well-formed cells with enough support that are a proper subset of the split
            final List<Map.Entry<String, double[]>> candidates = new ArrayList<>();
            for (final Map.Entry<String, double[]> e : discover.entrySet()) {
                if (e.getKey().equals(SEP)) continue;
                final String[] parts = e.getKey().split(SEP, -1);
                // a categorical value carrying the value separator desynchronises dimensions and values: not a slice
                if (parts[0].split(",").length != parts[1].split(String.valueOf((char) 2), -1).length) continue;
                if (e.getValue()[0] >= d.minSupport && e.getValue()[0] < all[0]) candidates.add(e);
            }
            String note = null;
            if (candidates.size() > d.maxCandidates) {
                // support descending, ties by key: the kept set does not depend on the accumulators' arrival order
                candidates.sort((a, b) -> {
                    final int c = Double.compare(b.getValue()[0], a.getValue()[0]);
                    return c != 0 ? c : a.getKey().compareTo(b.getKey());
                });
                note = candidates.size() + " candidate slices exceed maxCandidates " + d.maxCandidates + ": the " + d.maxCandidates + " best supported were kept";
                candidates.subList(d.maxCandidates, candidates.size()).clear();
            }
            final int k = candidates.size();
            final double threshold = discoveryThreshold(k, d.quantile);
            long passed = 0, confirmed = 0;
            for (final Map.Entry<String, double[]> e : candidates) {
                final String[] parts = e.getKey().split(SEP, -1);
                final String[] dimIdx = parts[0].split(",");
                final String[] values = parts[1].split(String.valueOf((char) 2), -1);
                final double[] c = e.getValue();
                final double z = discoveryZ(c[0], c[1], all[0], all[1], all[2]);
                final boolean pass = Double.isFinite(z) && Math.abs(z) > threshold;
                final double[] cc = confirm.get(e.getKey());
                final double zc = cc == null || allConfirm == null ? Double.NaN : discoveryZ(cc[0], cc[1], allConfirm[0], allConfirm[1], allConfirm[2]);
                final boolean confirm2 = pass && Double.isFinite(zc) && Math.signum(zc) == Math.signum(z) && Math.abs(zc) > Z95;
                if (pass) passed++;
                if (confirm2) confirmed++;
                if (!pass && EvaluationSpec.DISCOVERY_OUTPUT_PASSED.equals(d.output)) continue;
                final Map<String, Object> r = new LinkedHashMap<>();
                r.put("prediction", name);
                r.put("metric", d.metric);
                r.put("depth", (long) dimIdx.length);
                final List<String> dimNames = new ArrayList<>();
                for (final String i : dimIdx) dimNames.add(d.dimensions.get(Integer.parseInt(i)).name());
                r.put("dimensions", dimNames);
                r.put("values", new ArrayList<>(Arrays.asList(values)));
                r.put("n_discover", (long) c[0]);
                r.put("mean_discover", c[1] / c[0]);
                r.put("delta_discover", c[1] / c[0] - all[1] / all[0]);
                r.put("z_discover", finiteOrNull(z));
                r.put("threshold", finiteOrNull(threshold));
                r.put("passed", pass);
                r.put("n_confirm", cc == null ? 0L : (long) cc[0]);
                r.put("mean_confirm", cc == null || !(cc[0] > 0) ? null : cc[1] / cc[0]);
                r.put("delta_confirm", cc == null || !(cc[0] > 0) || allConfirm == null ? null : cc[1] / cc[0] - allConfirm[1] / allConfirm[0]);
                r.put("z_confirm", finiteOrNull(zc));
                r.put("confirmed", confirm2);
                out.records.add(r);
            }
            sr.put("nCandidates", (long) k);
            sr.put("threshold", finiteOrNull(threshold));
            sr.put("nPassed", passed);
            sr.put("nConfirmed", confirmed);
            sr.put("note", note);
            if (note != null) out.notes.add("sliceDiscovery on " + name + ": " + note);
            out.summary.add(sr);
        }
        // records ordered: confirmed first, then by |z_discover|
        out.records.sort((a, b) -> {
            final int c = Boolean.compare((Boolean) b.get("confirmed"), (Boolean) a.get("confirmed"));
            if (c != 0) return c;
            final Double za = (Double) a.get("z_discover"), zb = (Double) b.get("z_discover");
            return Double.compare(zb == null ? 0 : Math.abs(zb), za == null ? 0 : Math.abs(za));
        });
        return out;
    }

    // ---- calibration ---------------------------------------------------------------------------------------

    /** Layout of a calibration bin vector: {@code [n, Σy, Σq, Σp, Σ u·y, Σỹ]}. */
    public static final int BIN_N = 0, BIN_POSITIVES = 1, BIN_Q = 2, BIN_P = 3, BIN_UTILITY = 4, BIN_SHARE = 5, BIN_SLOTS = 6;

    /**
     * Adds a row to a bin vector with the set's mean q: the realised outcome y counts the positives, the rate and
     * the utility (a tie or a second positive in a unit is a whole positive, as the payout side counts it); the
     * share ỹ — the likelihood's label — only its own column, for the comparison with the share `p_model`.
     */
    public static void addBin(final double[] v, final AlignedRow row, final double q) {
        v[BIN_N] += 1;
        v[BIN_POSITIVES] += row.label;
        v[BIN_SHARE] += row.share;
        v[BIN_Q] += q;
        v[BIN_P] += row.baseline;
        v[BIN_UTILITY] += payout(row.utility, row.label);
    }

    /**
     * A row's realised return u·y (y as declared): 0 for a row that did not pay (y = 0) whatever its utility — an
     * infinite payout on a losing row would otherwise turn the sum into NaN (∞·0) — and 0 for a null utility.
     */
    static double payout(final double utility, final double label) {
        return label == 0d || Double.isNaN(utility) ? 0d : utility * label;
    }

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

    /** The bin of a value against ascending interior edges, right-closed: {@code (edges[i-1], edges[i]]}, the first bin open below, the last open above. */
    public static int bin(final double value, final double[] edges) {
        return bin(value, edges, false);
    }

    /**
     * The bin of a value against ascending interior edges: left-closed {@code [edges[i-1], edges[i])} (an edge
     * belongs to the bin above it) or right-closed {@code (edges[i-1], edges[i]]}; the first bin is open below,
     * the last open above. A binary search for the count of edges below the value (non-decreasing edges: a
     * quantile sketch may repeat a boundary); NaN falls in the first bin.
     */
    public static int bin(final double value, final double[] edges, final boolean closedLeft) {
        int lo = 0, hi = edges.length;
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if (closedLeft ? value >= edges[mid] : value > edges[mid]) lo = mid + 1;
            else hi = mid;
        }
        return lo;
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
                        r.put("positivesShare", v[BIN_SHARE]);
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
                .withField("positivesShare", Schema.FieldType.FLOAT64)
                .withField("p_model", Schema.FieldType.FLOAT64)
                .withField("p_baseline", Schema.FieldType.FLOAT64)
                .withField("rate", Schema.FieldType.FLOAT64)
                .withField("rate_lo", Schema.FieldType.FLOAT64)
                .withField("rate_hi", Schema.FieldType.FLOAT64)
                .withField("utility", Schema.FieldType.FLOAT64)
                .build();
    }

    /** The {@code {field, value}} struct of the units' slices and the rows' rowId. */
    private static Schema fieldValueSchema() {
        return Schema.builder()
                .withField("field", Schema.FieldType.STRING)
                .withField("value", Schema.FieldType.STRING)
                .build();
    }

    public static Schema unitsSchema() {
        final Schema slice = fieldValueSchema();
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
                .withField("utility", Schema.FieldType.FLOAT64)
                .withField("slices", Schema.FieldType.array(Schema.FieldType.element(slice)))
                .build();
    }

    public static Schema rowsSchema() {
        final Schema id = fieldValueSchema();
        final Schema prediction = Schema.builder()
                .withField("prediction", Schema.FieldType.STRING)
                .withField("p", Schema.FieldType.FLOAT64)
                .build();
        return Schema.builder()
                .withField("split", Schema.FieldType.STRING)
                .withField("unit", Schema.FieldType.STRING)
                .withField("rowId", Schema.FieldType.array(Schema.FieldType.element(id)))
                .withField("time", Schema.FieldType.TIMESTAMP)
                .withField("label", Schema.FieldType.FLOAT64)
                .withField("labelShare", Schema.FieldType.FLOAT64)
                .withField("baseline", Schema.FieldType.FLOAT64)
                .withField("predictions", Schema.FieldType.array(Schema.FieldType.element(prediction)))
                .withField("utility", Schema.FieldType.FLOAT64)
                .build();
    }

    public static Schema slicesSchema() {
        return Schema.builder()
                .withField("prediction", Schema.FieldType.STRING)
                .withField("metric", Schema.FieldType.STRING)
                .withField("depth", Schema.FieldType.INT64)
                .withField("dimensions", Schema.FieldType.array(Schema.FieldType.STRING))
                .withField("values", Schema.FieldType.array(Schema.FieldType.STRING))
                .withField("n_discover", Schema.FieldType.INT64)
                .withField("mean_discover", Schema.FieldType.FLOAT64)
                .withField("delta_discover", Schema.FieldType.FLOAT64)
                .withField("z_discover", Schema.FieldType.FLOAT64)
                .withField("threshold", Schema.FieldType.FLOAT64)
                .withField("passed", Schema.FieldType.BOOLEAN)
                .withField("n_confirm", Schema.FieldType.INT64)
                .withField("mean_confirm", Schema.FieldType.FLOAT64)
                .withField("delta_confirm", Schema.FieldType.FLOAT64)
                .withField("z_confirm", Schema.FieldType.FLOAT64)
                .withField("confirmed", Schema.FieldType.BOOLEAN)
                .build();
    }

    /** One slice discovery summary record per set (the summary's {@code discovery}). */
    public static Schema discoverySummarySchema() {
        return Schema.builder()
                .withField("prediction", Schema.FieldType.STRING)
                .withField("metric", Schema.FieldType.STRING)
                .withField("nCandidates", Schema.FieldType.INT64)
                .withField("threshold", Schema.FieldType.FLOAT64)
                .withField("nPassed", Schema.FieldType.INT64)
                .withField("nConfirmed", Schema.FieldType.INT64)
                .withField("note", Schema.FieldType.STRING)
                .build();
    }

    /** One calibration fit record (the summary's {@code fits}, the {@code output.calibration} file). */
    public static Schema fitSchema() {
        return Schema.builder()
                .withField("prediction", Schema.FieldType.STRING)
                .withField("derived", Schema.FieldType.STRING)
                .withField("type", Schema.FieldType.STRING)
                .withField("fitOn", Schema.FieldType.STRING)
                .withField("fitted", Schema.FieldType.BOOLEAN)
                .withField("temperature", Schema.FieldType.FLOAT64)
                .withField("a", Schema.FieldType.FLOAT64)
                .withField("b", Schema.FieldType.FLOAT64)
                .withField("intercept", Schema.FieldType.FLOAT64)
                .withField("se_a", Schema.FieldType.FLOAT64)
                .withField("se_b", Schema.FieldType.FLOAT64)
                .withField("se_intercept", Schema.FieldType.FLOAT64)
                .withField("z_a", Schema.FieldType.FLOAT64)
                .withField("nUnits", Schema.FieldType.FLOAT64)
                .withField("logScore", Schema.FieldType.FLOAT64)
                .withField("logScoreAtIdentity", Schema.FieldType.FLOAT64)
                .withField("gainPerUnit", Schema.FieldType.FLOAT64)
                .withField("iterations", Schema.FieldType.INT64)
                .withField("rejectedSteps", Schema.FieldType.INT64)
                .withField("converged", Schema.FieldType.BOOLEAN)
                .withField("note", Schema.FieldType.STRING)
                .build();
    }

    /** The {@code output.calibration} document: the fit records with the run's identity. */
    public static com.google.gson.JsonObject calibrationJson(final EvaluationSpec spec, final FitResults fits) {
        final com.google.gson.JsonObject o = new com.google.gson.JsonObject();
        o.addProperty("version", 1);
        o.addProperty("family", spec.family);
        o.addProperty("group", spec.group);
        o.addProperty("baseline", spec.baselineField);
        o.addProperty("baselineForm", spec.hasBaseline() ? spec.baselineForm : null);
        o.addProperty("parametersHash", spec.parametersHash);
        o.addProperty("planHash", spec.manifestPlanHash);
        o.addProperty("outputHash", spec.manifestOutputHash);
        o.addProperty("createdAt", java.time.Instant.now().toString());
        // the records' numbers are finite or null (normalised where they are built), so they serialise as is
        final com.google.gson.JsonArray array = new com.google.gson.JsonArray();
        if (fits != null) {
            for (final Map<String, Object> r : fits.records) array.add(com.mercari.solution.util.schema.converter.MapToJsonConverter.convertObject(r));
        }
        o.add("fits", array);
        return o;
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
                .withField("nRowsDuplicate", Schema.FieldType.INT64)
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
                .withField("fits", Schema.FieldType.array(Schema.FieldType.element(fitSchema())))
                .withField("discovery", Schema.FieldType.array(Schema.FieldType.element(discoverySummarySchema())))
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
        if (spec.hasRows()) parts.add("rows=" + spec.rowSplits);
        if (spec.hasFits()) {
            final List<String> fits = new ArrayList<>();
            for (int i = 0; i < spec.fits.size(); i++) {
                final EvaluationSpec.Fit f = spec.fits.get(i);
                final List<String> names = new ArrayList<>();
                for (final int d : spec.derivedOf(i)) names.add(spec.derived.get(d - spec.predictions.size()).name);
                fits.add(f.type + " on " + f.fitOn + " -> " + names + (f.isTemperature() ? " (grid " + f.gridSize + ", 1 pass)" : " (Newton, " + f.maxIter + " passes)"));
            }
            parts.add("fits=" + fits);
        }
        if (!spec.slices.isEmpty()) {
            final List<String> slices = new ArrayList<>();
            for (final EvaluationSpec.Slice sl : spec.slices) slices.add(sl.name());
            parts.add("slices=" + slices);
        }
        if (spec.utilityField != null) parts.add("utility=" + spec.utilityField);
        if (spec.hasDiscovery()) {
            final List<String> dims = new ArrayList<>();
            for (final EvaluationSpec.Dimension dim : spec.discovery.dimensions) dims.add(dim.name());
            parts.add("sliceDiscovery=" + dims + " depth<=" + spec.discovery.maxDepth + " support>=" + spec.discovery.minSupport + " " + spec.discovery.discoverOn + "->" + spec.discovery.confirmOn
                    + " metric=" + spec.discovery.metric + " q" + spec.discovery.quantile + (spec.discovery.hasNumeric() ? " (+1 sketch pass)" : ""));
        }
        if (!spec.notes.isEmpty()) parts.add("notes=" + spec.notes);
        return "evaluation " + String.join(" ", parts);
    }
}
