package com.mercari.solution.util.pipeline.evaluation;

import com.google.gson.JsonParser;
import com.mercari.solution.module.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Hand-computed checks of the per-unit computations, the accumulators and the report (no Beam). */
public class EvaluationScorerTest {

    static final Schema SCHEMA = Schema.builder()
            .withField("g", Schema.FieldType.STRING)
            .withField("y", Schema.FieldType.INT64)
            .withField("b", Schema.FieldType.FLOAT64)
            .withField("t", Schema.FieldType.TIMESTAMP)
            .withField("qa", Schema.FieldType.FLOAT64)
            .withField("qb", Schema.FieldType.FLOAT64)
            .withField("s", Schema.FieldType.FLOAT64)
            .withField("u", Schema.FieldType.FLOAT64)
            .withField("region", Schema.FieldType.STRING)
            .build();

    static final String SPLITS = "splits: {valid: {from: '2024-01-01', to: '2024-06-30', role: selection}, test: {from: '2024-07-01', to: '2024-12-31', role: report}}";

    static EvaluationSpec spec(final String json) {
        return EvaluationSpec.parse(JsonParser.parseString(json).getAsJsonObject()).resolve(SCHEMA, null);
    }

    private static final java.util.concurrent.atomic.AtomicInteger SEQ = new java.util.concurrent.atomic.AtomicInteger();

    /** x = the spec's row columns in order; the identity follows the creation order, so a unit's rows sort as written. */
    static EvaluationRow row(final String split, final String g, final double y, final double b, final String slice, final double... x) {
        return new EvaluationRow(split, g, String.format("%09d", SEQ.incrementAndGet()), 1_700_000_000_000L, null, y, b, 1d, slice == null ? new String[0] : new String[]{slice}, x);
    }

    @Test
    public void testGroupedUnitHandComputed() {
        // y = [1,0,0], baseline [0.5,0.3,0.2], prediction A [0.6,0.2,0.2]
        final EvaluationSpec spec = spec("{family: groupedMultinomial, group: g, label: y, baseline: b, time: t, predictions: [{name: A, prob: qa}], " + SPLITS + ", bootstrap: {samples: 0}}");
        final EvaluationScorer scorer = new EvaluationScorer(spec);
        final EvaluationScorer.Unit unit = scorer.prepare(List.of(row("test", "g1", 1, 0.5, null, 0.6), row("test", "g1", 0, 0.3, null, 0.2), row("test", "g1", 0, 0.2, null, 0.2)), "g1");
        Assertions.assertEquals(EvaluationScorer.Skip.NONE, unit.skip);
        final EvaluationScorer.Metrics m = scorer.score(unit);
        Assertions.assertEquals(Math.log(0.5), m.logScore[0], 1e-12);
        Assertions.assertEquals(Math.log(0.6), m.logScore[1], 1e-12);
        Assertions.assertEquals(1d, m.hitAt1[0], 0d);
        Assertions.assertEquals(1d, m.hitAt1[1], 0d);
        Assertions.assertEquals(0.38, m.brier[0], 1e-12);
        Assertions.assertEquals(0.24, m.brier[1], 1e-12);
        final List<Map<String, Object>> records = scorer.unitRecords(unit, m);
        Assertions.assertEquals(2, records.size());
        Assertions.assertEquals("baseline", records.get(0).get("prediction"));
        Assertions.assertEquals("A", records.get(1).get("prediction"));
        Assertions.assertEquals(Math.log(1.2), (Double) records.get(1).get("excessLogScore"), 1e-12);
        Assertions.assertEquals(0d, (Double) records.get(0).get("excessLogScore"), 0d);
    }

    @Test
    public void testTiesArgmaxAndScoreSet() {
        // tied labels share the likelihood term; a tie at the maximum shares the hit; a zero score with the baseline
        // as prob-scale offset reproduces the baseline
        final EvaluationSpec spec = spec("{family: groupedMultinomial, group: g, label: y, baseline: b, time: t, predictions: [{name: A, prob: qa}, {name: S, score: s, offset: b}], " + SPLITS + ", bootstrap: false}");
        final EvaluationScorer scorer = new EvaluationScorer(spec);
        final EvaluationScorer.Unit unit = scorer.prepare(List.of(row("test", "g1", 1, 0.5, null, 0.4, 0, 0.5), row("test", "g1", 1, 0.3, null, 0.4, 0, 0.3), row("test", "g1", 0, 0.2, null, 0.2, 0, 0.2)), "g1");
        Assertions.assertArrayEquals(new double[]{0.5, 0.5, 0}, unit.y, 1e-12);
        final EvaluationScorer.Metrics m = scorer.score(unit);
        Assertions.assertEquals(0.5 * Math.log(0.4) + 0.5 * Math.log(0.4), m.logScore[1], 1e-12);
        Assertions.assertEquals(0.5, m.hitAt1[1], 1e-12);            // two rows tie at 0.4: (0.5 + 0.5) / 2
        Assertions.assertEquals(Math.log(0.5) * 0.5 + Math.log(0.3) * 0.5, m.logScore[2], 1e-12);
        Assertions.assertEquals(m.logScore[0], m.logScore[2], 1e-12);
        Assertions.assertArrayEquals(unit.means[0], unit.means[2], 1e-12);
    }

    @Test
    public void testSkipsAndCommonUnitSet() {
        final EvaluationSpec spec = spec("{family: groupedMultinomial, group: g, label: y, baseline: b, time: t, predictions: [{name: A, prob: qa}], " + SPLITS + "}");
        final EvaluationScorer scorer = new EvaluationScorer(spec);
        Assertions.assertEquals(EvaluationScorer.Skip.NO_POSITIVE_LABEL, scorer.prepare(List.of(row("test", "g", 0, 0.5, null, 0.5), row("test", "g", 0, 0.5, null, 0.5)), "g").skip);
        Assertions.assertEquals(EvaluationScorer.Skip.INVALID_BASELINE, scorer.prepare(List.of(row("test", "g", 1, Double.NaN, null, 0.5), row("test", "g", 0, 0.5, null, 0.5)), "g").skip);
        Assertions.assertEquals(EvaluationScorer.Skip.INVALID_PREDICTION, scorer.prepare(List.of(row("test", "g", 1, 0.5, null, Double.NaN), row("test", "g", 0, 0.5, null, 0.5)), "g").skip);
        Assertions.assertEquals(EvaluationScorer.Skip.INVALID_PREDICTION, scorer.prepare(List.of(row("test", "g", 1, 0.5, null, 1.5), row("test", "g", 0, 0.5, null, 0.5)), "g").skip);
        final Map<String, MetricAccumulator> acc = new HashMap<>();
        scorer.skipped(scorer.prepare(List.of(row("test", "g", 0, 0.5, null, 0.5)), "g"), acc);
        Assertions.assertEquals(1d, acc.get(MetricAccumulator.SPLIT_KEY_PREFIX + "test").getTotal()[MetricAccumulator.UNITS_SKIPPED]);
    }

    @Test
    public void testPoissonWeightsAreDeterministicAndMeanOne() {
        final double[] a = EvaluationScorer.poissonWeights(7, "unit-1", 2000);
        final double[] b = EvaluationScorer.poissonWeights(7, "unit-1", 2000);
        Assertions.assertArrayEquals(a, b);
        double sum = 0;
        for (final double w : a) sum += w;
        Assertions.assertEquals(1d, sum / a.length, 0.08);
        Assertions.assertFalse(java.util.Arrays.equals(a, EvaluationScorer.poissonWeights(8, "unit-1", 2000)));
        Assertions.assertFalse(java.util.Arrays.equals(a, EvaluationScorer.poissonWeights(7, "unit-2", 2000)));
    }

    @Test
    public void testReportWithBootstrapSlicesAndPairs() {
        // two prediction sets, B identical to A: the pair difference is 0 with a [0, 0] interval; a single-slice
        // record equals the overall record; the excess interval brackets the point estimate
        final EvaluationSpec spec = spec("{family: groupedMultinomial, group: g, label: y, baseline: b, time: t, predictions: [{name: A, prob: qa}, {name: B, prob: qb}], "
                + SPLITS + ", bootstrap: {samples: 200, seed: 3}, slices: [{field: region}]}");
        final EvaluationScorer scorer = new EvaluationScorer(spec);
        final Map<String, MetricAccumulator> acc = new HashMap<>();
        final double[][] a = {{0.6, 0.2, 0.2}, {0.3, 0.5, 0.2}, {0.7, 0.2, 0.1}, {0.2, 0.2, 0.6}};
        final double[][] base = {{0.5, 0.3, 0.2}, {0.4, 0.4, 0.2}, {0.6, 0.3, 0.1}, {0.3, 0.3, 0.4}};
        final int[] winner = {0, 1, 1, 2};
        for (int u = 0; u < 4; u++) {
            final List<EvaluationRow> rows = new java.util.ArrayList<>();
            for (int i = 0; i < 3; i++) rows.add(row("test", "u" + u, i == winner[u] ? 1 : 0, base[u][i], u < 3 ? "east" : "west", a[u][i], a[u][i]));
            final EvaluationScorer.Unit unit = scorer.prepare(rows, "u" + u);
            Assertions.assertEquals(EvaluationScorer.Skip.NONE, unit.skip);
            scorer.accumulate(unit, scorer.score(unit), acc);
        }
        final EvaluationReport.Result result = EvaluationReport.build(spec, acc);
        // overall: baseline, A, B, pair A-B; east: 4 records; west: 4 records
        Assertions.assertEquals(12, result.records().size());
        final Map<String, Object> overallA = result.records().get(1);
        Assertions.assertEquals("A", overallA.get("prediction"));
        Assertions.assertNull(overallA.get("slice"));
        Assertions.assertEquals(4L, overallA.get("n_units"));
        Assertions.assertEquals(12L, overallA.get("n_rows"));
        final double expected = (Math.log(0.6 / 0.5) + Math.log(0.5 / 0.4) + Math.log(0.2 / 0.3) + Math.log(0.6 / 0.4)) / 4;
        Assertions.assertEquals(expected, (Double) overallA.get("excessLogScore"), 1e-12);
        Assertions.assertEquals(-(Double) overallA.get("logScore"), (Double) overallA.get("logloss"), 1e-15);
        Assertions.assertTrue((Double) overallA.get("excessLogScore_lo") <= expected && expected <= (Double) overallA.get("excessLogScore_hi"));
        Assertions.assertEquals(0d, (Double) result.records().get(0).get("excessLogScore"));
        Assertions.assertNull(result.records().get(0).get("excessLogScore_lo"));
        final Map<String, Object> pair = result.records().get(3);
        Assertions.assertEquals("A", pair.get("prediction"));
        Assertions.assertEquals("B", pair.get("pair"));
        Assertions.assertEquals(0d, (Double) pair.get("excessLogScore"), 1e-15);
        Assertions.assertEquals(0d, (Double) pair.get("excessLogScore_lo"), 1e-15);
        Assertions.assertEquals(0d, (Double) pair.get("excessLogScore_hi"), 1e-15);
        Assertions.assertEquals(0d, (Double) pair.get("brier"), 1e-15);
        final Map<String, Object> west = result.records().get(9);
        Assertions.assertEquals("region", west.get("slice"));
        Assertions.assertEquals("west", west.get("value"));
        Assertions.assertEquals(1L, west.get("n_units"));
        Assertions.assertEquals(Math.log(0.6 / 0.4), (Double) west.get("excessLogScore"), 1e-12);
        // a replicate mean of one unit is the unit's value whenever its weight is positive: a degenerate interval
        Assertions.assertEquals(Math.log(0.6 / 0.4), (Double) west.get("excessLogScore_lo"), 1e-12);
        Assertions.assertEquals(Math.log(0.6 / 0.4), (Double) west.get("excessLogScore_hi"), 1e-12);
        // summary
        final Map<String, Object> summary = result.summary();
        Assertions.assertEquals(4L, summary.get("nUnits"));
        Assertions.assertEquals(List.of("A", "B"), summary.get("predictions"));
        Assertions.assertEquals(List.of("region"), summary.get("slices"));
        // the same input gives the same intervals (deterministic weights)
        final Map<String, MetricAccumulator> again = new HashMap<>();
        for (int u = 0; u < 4; u++) {
            final List<EvaluationRow> rows = new java.util.ArrayList<>();
            for (int i = 0; i < 3; i++) rows.add(row("test", "u" + u, i == winner[u] ? 1 : 0, base[u][i], u < 3 ? "east" : "west", a[u][i], a[u][i]));
            final EvaluationScorer.Unit unit = scorer.prepare(rows, "u" + u);
            scorer.accumulate(unit, scorer.score(unit), again);
        }
        Assertions.assertEquals(overallA.get("excessLogScore_lo"), EvaluationReport.build(spec, again).records().get(1).get("excessLogScore_lo"));
    }

    @Test
    public void testBinomialPriorModeDerivesTheReference() {
        // y = [1,0,0,1], q = 0.8 on the positives and 0.2 on the negatives: logScore = log 0.8; the prior is the
        // label mean 0.5, so the reference is log 0.5 and the excess log(1.6)
        final EvaluationSpec spec = spec("{family: binomial, label: y, time: t, predictions: [{name: A, prob: qa}], " + SPLITS + ", bootstrap: {samples: 50, seed: 1}}");
        final EvaluationScorer scorer = new EvaluationScorer(spec);
        final Map<String, MetricAccumulator> acc = new HashMap<>();
        final double[] y = {1, 0, 0, 1};
        for (int i = 0; i < 4; i++) {
            final EvaluationScorer.Unit unit = scorer.prepare(List.of(row("test", null, y[i], Double.NaN, null, y[i] > 0 ? 0.8 : 0.2)), "r" + i);
            Assertions.assertEquals(EvaluationScorer.Skip.NONE, unit.skip);
            final EvaluationScorer.Metrics m = scorer.score(unit);
            Assertions.assertTrue(Double.isNaN(m.logScore[0]));
            Assertions.assertTrue(Double.isNaN(m.hitAt1[1]));
            scorer.accumulate(unit, m, acc);
        }
        final EvaluationReport.Result result = EvaluationReport.build(spec, acc);
        final Map<String, Object> a = result.records().get(1);
        Assertions.assertEquals("A", a.get("prediction"), a.toString());
        Assertions.assertEquals(Math.log(0.8), (Double) a.get("logScore"), 1e-12, a.toString());
        Assertions.assertEquals(Math.log(1.6), (Double) a.get("excessLogScore"), 1e-12);
        Assertions.assertNull(a.get("hitAt1"));
        Assertions.assertEquals(0.04, (Double) a.get("brier"), 1e-12);
        Assertions.assertNotNull(a.get("excessLogScore_lo"));
        Assertions.assertTrue(((List<?>) result.summary().get("notes")).toString().contains("no baseline"));
    }

    @Test
    public void testCalibrationBinsAndWilson() {
        Assertions.assertEquals(0, EvaluationReport.bin(0.5, new double[]{1, 2}));
        Assertions.assertEquals(0, EvaluationReport.bin(1, new double[]{1, 2}));
        Assertions.assertEquals(1, EvaluationReport.bin(1.5, new double[]{1, 2}));
        Assertions.assertEquals(2, EvaluationReport.bin(3, new double[]{1, 2}));
        // Wilson: 30 of 100 → [0.2189, 0.3985]
        final double[] ci = EvaluationReport.wilson(30, 100);
        Assertions.assertEquals(0.2189, ci[0], 5e-4);
        Assertions.assertEquals(0.3958, ci[1], 5e-4);
        Assertions.assertTrue(Double.isNaN(EvaluationReport.wilson(0, 0)[0]));

        final EvaluationSpec spec = spec("{family: groupedMultinomial, group: g, label: y, baseline: b, time: t, predictions: [{name: A, prob: qa}], " + SPLITS
                + ", utility: {field: u}, calibration: [{type: reliability, by: field, field: u, edges: [1, 2]}, {type: edge, thresholds: [1.0, 2.0]}]}");
        final EvaluationScorer scorer = new EvaluationScorer(spec);
        final EvaluationScorer.Unit unit = scorer.prepare(List.of(row("test", "g1", 1, 0.5, null, 0.6, 3.0), row("test", "g1", 0, 0.3, null, 0.2, 1.5), row("test", "g1", 0, 0.2, null, 0.2, 0.5)), "g1");
        final List<AlignedRow> aligned = scorer.aligned(unit);
        Assertions.assertEquals(3, aligned.size());
        Assertions.assertEquals(3.0, aligned.get(0).utility);
        Assertions.assertEquals(3.0, EvaluationReport.tableValue(spec.tables.get(0), 0, aligned.get(0), 0));
        // bins as the engine fills them: u = 3.0 → bin 2 (above 2), 1.5 → bin 1, 0.5 → bin 0; edge: q/p = 1.2, 0.67, 1.0 → only row 0 exceeds 1.0, none exceeds 2.0
        final Map<String, double[]> bins = new HashMap<>();
        bins.put(EvaluationReport.binKey("test", 1, 0, 2), new double[]{1, 1, 0.6, 0.5, 3.0});
        bins.put(EvaluationReport.binKey("test", 1, 0, 1), new double[]{1, 0, 0.2, 0.3, 0});
        bins.put(EvaluationReport.binKey("test", 1, 0, 0), new double[]{1, 0, 0.2, 0.2, 0});
        bins.put(EvaluationReport.binKey("test", 1, 1, 0), new double[]{1, 1, 0.6, 0.5, 3.0});
        final List<Map<String, Object>> records = EvaluationReport.calibration(spec, bins, Map.of());
        // 2 splits x 1 prediction x (3 + 2) bins
        Assertions.assertEquals(10, records.size());
        final Map<String, Object> top = records.stream().filter(r -> "test".equals(r.get("split")) && (Long) r.get("table") == 0 && (Long) r.get("bin") == 2).findFirst().orElseThrow();
        Assertions.assertEquals(2d, top.get("lower"));
        Assertions.assertNull(top.get("upper"));
        Assertions.assertEquals(1L, top.get("n"));
        Assertions.assertEquals(1d, top.get("rate"));
        Assertions.assertEquals(3d, top.get("utility"));
        Assertions.assertEquals(0.6, top.get("p_model"));
        final Map<String, Object> edge2 = records.stream().filter(r -> "test".equals(r.get("split")) && (Long) r.get("table") == 1 && (Long) r.get("bin") == 1).findFirst().orElseThrow();
        Assertions.assertEquals(2.0, edge2.get("lower"));
        Assertions.assertEquals(0L, edge2.get("n"));
        Assertions.assertNull(edge2.get("rate"));
        final Map<String, Object> valid = records.stream().filter(r -> "valid".equals(r.get("split"))).findFirst().orElseThrow();
        Assertions.assertEquals(0L, valid.get("n"));
    }

    @Test
    public void testTemperatureGridAndDerivedSet() {
        // a probability set under temperature T: q ∝ q^(1/T) within the group; at T = 1 the set is unchanged
        final EvaluationSpec spec = spec("{family: groupedMultinomial, group: g, label: y, baseline: b, time: t, predictions: [{name: A, prob: qa}], " + SPLITS
                + ", bootstrap: false, calibration: [{type: temperature, fitOn: valid, grid: [0.5, 2.0, 4]}]}");
        Assertions.assertEquals(List.of("baseline", "A", "A@T"), spec.predictionNames());
        Assertions.assertEquals(2, spec.setCount());
        final EvaluationScorer scorer = new EvaluationScorer(spec);
        final EvaluationScorer.Unit unit = scorer.prepare(List.of(row("valid", "g1", 1, 0.5, null, 0.6), row("valid", "g1", 0, 0.3, null, 0.2), row("valid", "g1", 0, 0.2, null, 0.2)), "g1");
        Assertions.assertTrue(Double.isNaN(unit.means[2][0]));
        final double[] v = scorer.temperatureLogLikelihoods(unit, 0);
        // grid 0.5, 1.0, 1.5, 2.0: at T = 1 the log score is log 0.6; at T = 0.5 the shares are q² normalised: 0.36 / 0.44
        Assertions.assertEquals(5, v.length);
        Assertions.assertEquals(Math.log(0.36 / 0.44), v[0], 1e-12);
        Assertions.assertEquals(Math.log(0.6), v[1], 1e-12);
        Assertions.assertEquals(1d, v[4], 0d);
        // the fitted parameter derives the set: T = 0.5 sharpens the winner's share
        final FitResults fits = new FitResults();
        fits.parameters.put("A@T", new double[]{0.5});
        scorer.derive(unit, fits);
        Assertions.assertEquals(0.36 / 0.44, unit.means[2][0], 1e-12);
        final EvaluationScorer.Metrics m = scorer.score(unit);
        Assertions.assertEquals(Math.log(0.36 / 0.44), m.logScore[2], 1e-12);
        Assertions.assertEquals("A@T", scorer.unitRecords(unit, m).get(2).get("prediction"));
        Assertions.assertEquals(2, scorer.aligned(unit).get(0).predictions.length);
        // without parameters the derived set stays NaN and scores NaN
        final EvaluationScorer.Unit again = scorer.prepare(unit.rows, "g1");
        scorer.derive(again, new FitResults());
        Assertions.assertTrue(Double.isNaN(scorer.score(again).logScore[2]));
    }

    @Test
    public void testBlendEvaluationAndDerivedSet() {
        // blend of a score set with the baseline as its offset: at (a, b) = (0, 1) the fitted means are the baseline
        // shares, so the gradient on a is Σ (ỹ − p) f and the information Σ p f² − (Σ p f)²
        final EvaluationSpec spec = spec("{family: groupedMultinomial, group: g, label: y, baseline: b, time: t, predictions: [{name: S, score: s}], " + SPLITS
                + ", bootstrap: false, calibration: [{type: blend, fitOn: valid}]}");
        Assertions.assertEquals(List.of("baseline", "S", "S@blend"), spec.predictionNames());
        final EvaluationScorer scorer = new EvaluationScorer(spec);
        Assertions.assertEquals(2, scorer.blendK());
        Assertions.assertArrayEquals(new double[]{1, 1}, scorer.blendStart(), 0d);
        final EvaluationScorer.Unit unit = scorer.prepare(List.of(row("valid", "g1", 1, 0.5, null, 1.0), row("valid", "g1", 0, 0.3, null, 0.0), row("valid", "g1", 0, 0.2, null, -1.0)), "g1");
        final double[][] fo = scorer.fitInputs(unit, 0);
        Assertions.assertArrayEquals(new double[]{1, 0, -1}, fo[0], 1e-12);
        Assertions.assertArrayEquals(new double[]{Math.log(0.5), Math.log(0.3), Math.log(0.2)}, fo[1], 1e-12);
        final double[] eval = scorer.blendEvaluate(unit, 0, new double[]{0, 1});
        Assertions.assertEquals(1d, eval[0], 0d);
        Assertions.assertEquals(Math.log(0.5), eval[1], 1e-12);
        final double pf = 0.5 * 1 + 0.3 * 0 + 0.2 * -1;
        Assertions.assertEquals(1 - pf, eval[2], 1e-12);                       // g_a = Σ (ỹ − p) f
        Assertions.assertEquals(0.5 + 0.2 - pf * pf, eval[4], 1e-12);          // G_aa = Σ p f² − (Σ p f)²
        // a Newton chain on this one unit moves a upward (the winner has the largest score)
        com.mercari.solution.util.pipeline.glm.FitState state = com.mercari.solution.util.pipeline.glm.FitState.initial(2, scorer.blendStart());
        for (int it = 0; it < 6; it++) state = state.advance(scorer.blendEvaluate(unit, 0, state.proposal), 1e-4, 1e-10);
        Assertions.assertTrue(state.hasBest);
        Assertions.assertTrue(state.bestTheta[0] > 1d, "a: " + state.bestTheta[0]);
        final double[] se = EvaluationScorer.standardErrors(state);
        Assertions.assertEquals(2, se.length);
        Assertions.assertTrue(se[0] > 0);
        // deriving with (a, b) = (1, 1) reproduces the declared combination: softmax(f + log p)
        final FitResults fits = new FitResults();
        fits.parameters.put("S@blend", new double[]{1, 1});
        scorer.derive(unit, fits);
        final double z = 0.5 * Math.E + 0.3 + 0.2 / Math.E;
        Assertions.assertEquals(0.5 * Math.E / z, unit.means[2][0], 1e-12);
    }

    @Test
    public void testBinomialBlendCarriesAnIntercept() {
        final EvaluationSpec spec = spec("{family: binomial, label: y, baseline: b, time: t, predictions: [{name: A, prob: qa}], " + SPLITS
                + ", bootstrap: false, calibration: [{type: blend, fitOn: valid}, {type: temperature, fitOn: valid}]}");
        final EvaluationScorer scorer = new EvaluationScorer(spec);
        Assertions.assertEquals(3, scorer.blendK());
        final EvaluationScorer.Unit unit = scorer.prepare(List.of(row("valid", null, 1, 0.25, null, 0.8)), "r1");
        final double[][] fo = scorer.fitInputs(unit, 0);
        Assertions.assertEquals(Math.log(4), fo[0][0], 1e-12);                 // logit 0.8
        Assertions.assertEquals(Math.log(1d / 3), fo[1][0], 1e-12);            // logit 0.25
        final FitResults fits = new FitResults();
        fits.parameters.put("A@blend", new double[]{1, 0, 0});                 // the set alone
        fits.parameters.put("A@T", new double[]{2});                           // logit / 2
        scorer.derive(unit, fits);
        Assertions.assertEquals(0.8, unit.means[2][0], 1e-12);
        Assertions.assertEquals(1d / (1 + Math.exp(-Math.log(4) / 2)), unit.means[3][0], 1e-12);
    }
}
