package com.mercari.solution.util.pipeline.screen;

import com.google.gson.JsonParser;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.feature.FeatureLineage;
import com.mercari.solution.util.pipeline.glm.Baselines;
import com.mercari.solution.util.pipeline.glm.StatMath;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GroupScorerTest {

    private static final Schema SCHEMA = Schema.builder()
            .withField("g", Schema.FieldType.STRING)
            .withField("y", Schema.FieldType.INT64)
            .withField("b", Schema.FieldType.FLOAT64)
            .withField("t", Schema.FieldType.TIMESTAMP)
            .withField("x", Schema.FieldType.FLOAT64)
            .withField("x2", Schema.FieldType.FLOAT64)
            .build();

    private static ScreenSpec spec(final String json) {
        return ScreenSpec.parse(JsonParser.parseString(json).getAsJsonObject()).resolve(SCHEMA, null);
    }

    private static ScreenRow row(final String g, final long t, final double y, final double b, final double... x) {
        return new ScreenRow(g, g + ":" + t + ":" + x[0], t, null, y, b, 1d, x);
    }

    @Test
    public void testGroupedMultinomialHandComputed() {
        // uniform p = 1/3, y = [1,0,0], x = [3,1,2]: centred x = [1,-1,0]
        // S = 1*(1-1/3) + (-1)*(0-1/3) = 1 ; H = (1/3)(1+1+0) - 0 = 2/3 ; chi2 = 1.5
        final ScreenSpec spec = spec("{family: groupedMultinomial, group: g, label: y, time: t, candidates: [x], transforms: [raw], placebo: {noise: 0}}");
        final GroupScorer scorer = new GroupScorer(spec);
        final Map<Integer, ScoreAccumulator> acc = new HashMap<>();
        Assertions.assertEquals(Baselines.Skip.NONE, scorer.score(List.of(row("a", 1, 1, Double.NaN, 3), row("a", 1, 0, Double.NaN, 1), row("a", 1, 0, Double.NaN, 2)), "a", acc));
        final double[] a = acc.get(spec.key(0, 0)).getTotal();
        Assertions.assertEquals(1d, a[ScoreAccumulator.S], 1e-12);
        Assertions.assertEquals(2d / 3, a[ScoreAccumulator.H], 1e-12);
        Assertions.assertEquals(3, a[ScoreAccumulator.N_OBS]);
        final ScreenReport.Stats st = ScreenReport.stats(spec, a, 1);
        Assertions.assertEquals(1.5, st.chi2(), 1e-12);
        Assertions.assertEquals(Math.sqrt(1.5), st.z(), 1e-12);
        Assertions.assertEquals(0.75, st.estGain(), 1e-12);
        final double[] book = acc.get(ScoreAccumulator.BOOKKEEPING_KEY).getTotal();
        Assertions.assertEquals(1, book[ScoreAccumulator.UNITS_SCORED]);
        Assertions.assertEquals(3, book[ScoreAccumulator.ROWS_SCORED]);
    }

    @Test
    public void testGroupedStatisticIsScaleAndShiftInvariant() {
        final ScreenSpec spec = spec("{family: groupedMultinomial, group: g, label: y, time: t, candidates: [x], transforms: [raw], placebo: {noise: 0}}");
        final GroupScorer scorer = new GroupScorer(spec);
        final Map<Integer, ScoreAccumulator> acc1 = new HashMap<>();
        final Map<Integer, ScoreAccumulator> acc2 = new HashMap<>();
        scorer.score(List.of(row("a", 1, 1, Double.NaN, 3), row("a", 1, 0, Double.NaN, 1), row("a", 1, 0, Double.NaN, 2)), "a", acc1);
        scorer.score(List.of(row("a", 1, 1, Double.NaN, 35), row("a", 1, 0, Double.NaN, 15), row("a", 1, 0, Double.NaN, 25)), "a", acc2);
        final ScreenReport.Stats s1 = ScreenReport.stats(spec, acc1.get(spec.key(0, 0)).getTotal(), 1);
        final ScreenReport.Stats s2 = ScreenReport.stats(spec, acc2.get(spec.key(0, 0)).getTotal(), 1);
        Assertions.assertEquals(s1.chi2(), s2.chi2(), 1e-12);
        Assertions.assertEquals(s1.beta() / 10, s2.beta(), 1e-12);
    }

    @Test
    public void testGroupedBaselineForms() {
        // inverseShare: b = [2, 4, 4] -> 1/b = [.5, .25, .25] -> p = [.5, .25, .25]
        final ScreenSpec spec = spec("{family: groupedMultinomial, group: g, label: y, baseline: {field: b, form: inverseShare}, time: t, candidates: [x], transforms: [raw], placebo: {noise: 0}}");
        final Map<Integer, ScoreAccumulator> acc = new HashMap<>();
        new GroupScorer(spec).score(List.of(row("a", 1, 1, 2, 1), row("a", 1, 0, 4, 0), row("a", 1, 0, 4, 0)), "a", acc);
        // mean = .5 ; x~ = [.5, -.5, -.5] ; S = .5*(1-.5) + (-.5)(0-.25)*2 = .25 + .25 = .5
        // H = .5*.25 + .25*.25*2 - 0 = .125 + .125 = .25
        final double[] a = acc.get(spec.key(0, 0)).getTotal();
        Assertions.assertEquals(0.5, a[ScoreAccumulator.S], 1e-12);
        Assertions.assertEquals(0.25, a[ScoreAccumulator.H], 1e-12);
        // a group whose baseline is invalid for the form is skipped
        final Map<Integer, ScoreAccumulator> skipped = new HashMap<>();
        Assertions.assertEquals(Baselines.Skip.INVALID_BASELINE, new GroupScorer(spec).score(List.of(row("b", 1, 1, 0, 1), row("b", 1, 0, 4, 0)), "b", skipped));
        Assertions.assertEquals(1, skipped.get(ScoreAccumulator.BOOKKEEPING_KEY).getTotal()[ScoreAccumulator.UNITS_SKIPPED]);
        // a group without a positive label is skipped
        final Map<Integer, ScoreAccumulator> noPositive = new HashMap<>();
        Assertions.assertEquals(Baselines.Skip.NO_POSITIVE_LABEL, new GroupScorer(spec).score(List.of(row("c", 1, 0, 2, 1), row("c", 1, 0, 4, 0)), "c", noPositive));
    }

    @Test
    public void testBaselineInvalidDropRow() {
        final ScreenSpec spec = spec("{family: groupedMultinomial, group: g, label: y, baseline: {field: b, form: inverseShare, invalid: dropRow}, time: t, candidates: [x], transforms: [raw], placebo: {noise: 0}}");
        final GroupScorer scorer = new GroupScorer(spec);
        final Map<Integer, ScoreAccumulator> acc = new HashMap<>();
        // odds [2, 4, 0]: the third row leaves the unit; p = [2/3, 1/3] over the rest, x = [1, 0]
        // mean = 2/3 ; x~ = [1/3, -2/3] ; S = (1/3)(1 - 2/3) + (-2/3)(0 - 1/3) = 1/3 ; H = (2/3)(1/9) + (1/3)(4/9) - 0 = 2/9
        Assertions.assertEquals(Baselines.Skip.NONE, scorer.score(List.of(row("a", 1, 1, 2, 1), row("a", 1, 0, 4, 0), row("a", 1, 0, 0, 5)), "a", acc));
        final double[] a = acc.get(spec.key(0, 0)).getTotal();
        Assertions.assertEquals(1d / 3, a[ScoreAccumulator.S], 1e-12);
        Assertions.assertEquals(2d / 9, a[ScoreAccumulator.H], 1e-12);
        Assertions.assertEquals(2, a[ScoreAccumulator.N_OBS]);
        double[] book = acc.get(ScoreAccumulator.BOOKKEEPING_KEY).getTotal();
        Assertions.assertEquals(1, book[ScoreAccumulator.UNITS_SCORED]);
        Assertions.assertEquals(2, book[ScoreAccumulator.ROWS_SCORED]);
        Assertions.assertEquals(1, book[ScoreAccumulator.ROWS_DROPPED]);
        // the positive row is the invalid one: no positive among the rest
        Assertions.assertEquals(Baselines.Skip.NO_POSITIVE_LABEL, scorer.score(List.of(row("b", 1, 1, Double.NaN, 1), row("b", 1, 0, 4, 0)), "b", acc));
        // every row invalid: an invalid baseline
        Assertions.assertEquals(Baselines.Skip.INVALID_BASELINE, scorer.score(List.of(row("c", 1, 1, 0, 1), row("c", 1, 0, -1, 0)), "c", acc));
        book = acc.get(ScoreAccumulator.BOOKKEEPING_KEY).getTotal();
        Assertions.assertEquals(2, book[ScoreAccumulator.UNITS_SKIPPED]);
        Assertions.assertEquals(1, book[ScoreAccumulator.UNITS_SKIPPED_BASELINE]);
        Assertions.assertEquals(4, book[ScoreAccumulator.ROWS_DROPPED]);
        final ScreenReport.Result result = ScreenReport.build(spec, acc);
        Assertions.assertEquals(4L, result.summary().get("nRowsDropped"));
        Assertions.assertEquals(1L, result.summary().get("nUnitsSkippedInvalidBaseline"));
        final String notes = result.summary().get("notes").toString();
        Assertions.assertTrue(notes.contains("2 of 3 units skipped (66.7%: invalid baseline 1, no positive label 1)"), notes);
        Assertions.assertFalse(notes.contains("declare baseline.invalid"), notes);
        Assertions.assertTrue(notes.contains("4 rows dropped (baseline.invalid: dropRow)"), notes);
        // the default keeps the whole-unit skip and says the way out
        final ScreenSpec plain = spec("{family: groupedMultinomial, group: g, label: y, baseline: {field: b, form: inverseShare}, time: t, candidates: [x], transforms: [raw], placebo: {noise: 0}}");
        final Map<Integer, ScoreAccumulator> plainAcc = new HashMap<>();
        new GroupScorer(plain).score(List.of(row("a", 1, 1, 2, 1), row("a", 1, 0, 4, 0)), "a", plainAcc);
        Assertions.assertEquals(Baselines.Skip.INVALID_BASELINE, new GroupScorer(plain).score(List.of(row("b", 1, 1, 2, 1), row("b", 1, 0, 0, 0)), "b", plainAcc));
        final String plainNotes = ScreenReport.build(plain, plainAcc).summary().get("notes").toString();
        Assertions.assertTrue(plainNotes.contains("1 of 2 units skipped (50.0%: invalid baseline 1, no positive label 0); an invalid baseline value (a null, or a 0 / negative one under form inverseShare / rate) skips the whole unit: declare baseline.invalid: dropRow"), plainNotes);
        Assertions.assertEquals(0L, ScreenReport.build(plain, plainAcc).summary().get("nRowsDropped"));
        // an unknown policy is a spec error
        final String error = Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: groupedMultinomial, group: g, label: y, baseline: {field: b, invalid: drop}, time: t, candidates: [x]}")).getMessage();
        Assertions.assertTrue(error.contains("baseline.invalid 'drop' is unknown"), error);
    }

    @Test
    public void testBinomialPriorHandComputed() {
        // independent rows y = [1,1,0,0], x = [2,3,0,1]: ybar = .5, xbar = 1.5, S = 5 - 1.5*2 = 2, sxx = 14 - 9 = 5, H = .25*5 = 1.25, chi2 = 3.2
        final ScreenSpec spec = spec("{family: binomial, label: y, time: t, candidates: [x], placebo: {noise: 0}}");
        final GroupScorer scorer = new GroupScorer(spec);
        final Map<Integer, ScoreAccumulator> acc = new HashMap<>();
        final double[][] data = {{1, 2}, {1, 3}, {0, 0}, {0, 1}};
        for (int i = 0; i < data.length; i++) {
            scorer.score(List.of(row(null, i, data[i][0], Double.NaN, data[i][1])), "r" + i, acc);
        }
        final ScreenReport.Stats st = ScreenReport.stats(spec, acc.get(spec.key(0, 0)).getTotal(), 4);
        Assertions.assertEquals(2d, st.s(), 1e-12);
        Assertions.assertEquals(1.25, st.h(), 1e-12);
        Assertions.assertEquals(3.2, st.chi2(), 1e-12);
        Assertions.assertEquals(3.2 / 8, st.estGain(), 1e-12);
        Assertions.assertEquals(4, acc.get(ScoreAccumulator.BOOKKEEPING_KEY).getTotal()[ScoreAccumulator.UNITS_SCORED]);
    }

    @Test
    public void testBinomialOffsetHandComputed() {
        // y = [1,0], p = [.5,.5], x = [1,0]: r = [.5,-.5], v = .25
        // c1 = .5, c2 = 0, c3 = .25, c4 = .25, c5 = .5 ; xbar = .5 ; S = .5 ; H = .25 - .0625/.5 = .125 ; chi2 = 2
        final ScreenSpec spec = spec("{family: binomial, label: y, baseline: {field: b, form: prob}, time: t, candidates: [x], placebo: {noise: 0}}");
        final GroupScorer scorer = new GroupScorer(spec);
        final Map<Integer, ScoreAccumulator> acc = new HashMap<>();
        scorer.score(List.of(row(null, 1, 1, 0.5, 1)), "r1", acc);
        scorer.score(List.of(row(null, 2, 0, 0.5, 0)), "r2", acc);
        final ScreenReport.Stats st = ScreenReport.stats(spec, acc.get(spec.key(0, 0)).getTotal(), 2);
        Assertions.assertEquals(0.5, st.s(), 1e-12);
        Assertions.assertEquals(0.125, st.h(), 1e-12);
        Assertions.assertEquals(2d, st.chi2(), 1e-12);
        // a row whose x is missing carries no information
        scorer.score(List.of(row(null, 3, 1, 0.5, Double.NaN)), "r3", acc);
        Assertions.assertEquals(2d, ScreenReport.stats(spec, acc.get(spec.key(0, 0)).getTotal(), 3).chi2(), 1e-12);
        Assertions.assertEquals(2, acc.get(spec.key(0, 0)).getTotal()[ScoreAccumulator.N_OBS]);
    }

    @Test
    public void testTransforms() {
        final double[] rank = GroupScorer.percentileRank(new double[]{10, Double.NaN, 30, 20, 20});
        Assertions.assertEquals(0d, rank[0], 1e-12);
        Assertions.assertTrue(Double.isNaN(rank[1]));
        Assertions.assertEquals(1d, rank[2], 1e-12);
        Assertions.assertEquals(0.5, rank[3], 1e-12);   // (1 smaller + 0.5 * 1 tie) / 3
        Assertions.assertEquals(0.5, rank[4], 1e-12);
        Assertions.assertEquals(0.5, GroupScorer.percentileRank(new double[]{7, Double.NaN})[0]);
        final double[] absdev = GroupScorer.transform(ScreenSpec.TRANSFORM_ABSDEV, new double[]{1, 5, 3, Double.NaN});
        Assertions.assertArrayEquals(new double[]{2, 2, 0}, new double[]{absdev[0], absdev[1], absdev[2]}, 1e-12);
        Assertions.assertTrue(Double.isNaN(absdev[3]));
    }

    @Test
    public void testPlacebosAreDeterministic() {
        final ScreenSpec spec = spec("{family: groupedMultinomial, group: g, label: y, time: t, candidates: [x], transforms: [raw], placebo: {noise: 3, shuffle: {field: x2, n: 2}, seed: 11}}");
        Assertions.assertEquals(1 + 3 + 2, spec.columnCount());
        Assertions.assertEquals(List.of("x", "__noise_0", "__noise_1", "__noise_2", "__shuffle_0", "__shuffle_1"), spec.columnNames());
        final List<ScreenRow> rows = List.of(
                new ScreenRow("a", "1", 1, null, 1, Double.NaN, 1, new double[]{3, 30}),
                new ScreenRow("a", "2", 1, null, 0, Double.NaN, 1, new double[]{1, 10}),
                new ScreenRow("a", "3", 1, null, 0, Double.NaN, 1, new double[]{2, 20}));
        final Map<Integer, ScoreAccumulator> acc1 = new HashMap<>();
        final Map<Integer, ScoreAccumulator> acc2 = new HashMap<>();
        new GroupScorer(spec).score(rows, "a", acc1);
        new GroupScorer(spec).score(List.of(rows.get(2), rows.get(0), rows.get(1)), "a", acc2);   // input order must not matter
        for (int c = 0; c < spec.columnCount(); c++) {
            Assertions.assertArrayEquals(acc1.get(spec.key(c, 0)).getTotal(), acc2.get(spec.key(c, 0)).getTotal(), 0d, "column " + c);
        }
        // the shuffle keeps the reference column's values (only the order changes): H of a permutation is bounded by the data
        Assertions.assertTrue(acc1.get(spec.key(4, 0)).getTotal()[ScoreAccumulator.N_OBS] == 3);
        // a different seed draws different noise
        final ScreenSpec other = spec("{family: groupedMultinomial, group: g, label: y, time: t, candidates: [x], transforms: [raw], placebo: {noise: 3, shuffle: {field: x2, n: 2}, seed: 12}}");
        final Map<Integer, ScoreAccumulator> acc3 = new HashMap<>();
        new GroupScorer(other).score(rows, "a", acc3);
        Assertions.assertNotEquals(acc1.get(spec.key(1, 0)).getTotal()[ScoreAccumulator.S], acc3.get(spec.key(1, 0)).getTotal()[ScoreAccumulator.S]);
    }

    @Test
    public void testReportThresholdAndFlags() {
        final ScreenSpec spec = spec("{family: groupedMultinomial, group: g, label: y, time: t, candidates: [x, x2], transforms: [raw, rank], placebo: {noise: 0}, flags: {leakZ: 1.0}}");
        final GroupScorer scorer = new GroupScorer(spec);
        final Map<Integer, ScoreAccumulator> acc = new HashMap<>();
        for (int g = 0; g < 20; g++) {
            // x separates the positive perfectly, x2 is constant (degenerate)
            scorer.score(List.of(
                    new ScreenRow("g" + g, "p", g, "2025", 1, Double.NaN, 1, new double[]{3, 1}),
                    new ScreenRow("g" + g, "q", g, "2025", 0, Double.NaN, 1, new double[]{1, 1}),
                    new ScreenRow("g" + g, "r", g, "2025", 0, Double.NaN, 1, new double[]{2, 1})), "g" + g, acc);
        }
        final ScreenReport.Result result = ScreenReport.build(spec, acc);
        Assertions.assertEquals(4, result.records().size());
        final Map<String, Object> xRaw = result.records().get(0);
        Assertions.assertEquals("x", xRaw.get("candidate"));
        Assertions.assertEquals("raw", xRaw.get("transform"));
        Assertions.assertEquals(20L, xRaw.get("n_groups"));
        Assertions.assertEquals(Math.sqrt(30), (Double) xRaw.get("z"), 1e-9);   // chi2 = 20 * 1.5
        // no placebo column: the threshold is the theoretical chi2(1) quantile / 2N
        Assertions.assertEquals(StatMath.chiSquare1Quantile(0.99) / 40, (Double) xRaw.get("threshold"), 1e-12);
        Assertions.assertEquals(StatMath.chiSquare1Quantile(0.99) / 40, (Double) result.summary().get("thresholdTheoretical"), 1e-12);
        Assertions.assertEquals(Boolean.TRUE, xRaw.get("passed"));
        Assertions.assertEquals(Boolean.TRUE, xRaw.get("leakSuspect"));
        Assertions.assertEquals(1L, xRaw.get("periods_agree"));
        Assertions.assertEquals(1L, xRaw.get("n_periods"));
        Assertions.assertNotNull(xRaw.get("qValue"));
        final Map<String, Object> x2Raw = result.records().get(2);
        Assertions.assertEquals("x2", x2Raw.get("candidate"));
        Assertions.assertEquals(Boolean.TRUE, x2Raw.get("degenerate"));
        Assertions.assertEquals(Boolean.FALSE, x2Raw.get("passed"));
        Assertions.assertEquals(0d, x2Raw.get("est_gain"));
        // the placeholder gain of a degenerate test carries no excess (not a negative df / 2N)
        Assertions.assertNull(x2Raw.get("excess_gain"));
        Assertions.assertEquals((Double) xRaw.get("est_gain") - 1d / 40, (Double) xRaw.get("excess_gain"), 1e-12);
        Assertions.assertEquals(List.of("x"), result.summary().get("passedColumns"));
        Assertions.assertEquals(2L, result.summary().get("nPassed"));
        Assertions.assertEquals(20L, result.summary().get("nUnits"));
        Assertions.assertEquals(60L, result.summary().get("nRowsScored"));
    }

    @Test
    public void testPassRuleMinPeriodsAgree() {
        // x separates the positive in 2025 and (more weakly) the other way in 2024: the window z is positive, one of
        // the two periods agrees; the placebo cut alone passes it, a full period agreement does not
        for (final String pass : new String[]{"", ", pass: {minPeriodsAgree: 0.5}", ", pass: {minPeriodsAgree: 1.0}", ", pass: {minPeriodsAgree: 2}"}) {
            final ScreenSpec spec = spec("{family: groupedMultinomial, group: g, label: y, time: t, candidates: [x], transforms: [raw], placebo: {noise: 0}, periods: year" + pass + "}");
            final GroupScorer scorer = new GroupScorer(spec);
            final Map<Integer, ScoreAccumulator> acc = new HashMap<>();
            for (int g = 0; g < 30; g++) {
                scorer.score(List.of(
                        new ScreenRow("g" + g, "p", g, "2025", 1, Double.NaN, 1, new double[]{3}),
                        new ScreenRow("g" + g, "q", g, "2025", 0, Double.NaN, 1, new double[]{1}),
                        new ScreenRow("g" + g, "r", g, "2025", 0, Double.NaN, 1, new double[]{2})), "g" + g, acc);
            }
            for (int g = 0; g < 8; g++) {
                scorer.score(List.of(
                        new ScreenRow("h" + g, "p", g, "2024", 0, Double.NaN, 1, new double[]{3}),
                        new ScreenRow("h" + g, "q", g, "2024", 1, Double.NaN, 1, new double[]{1}),
                        new ScreenRow("h" + g, "r", g, "2024", 0, Double.NaN, 1, new double[]{2})), "h" + g, acc);
            }
            final ScreenReport.Result result = ScreenReport.build(spec, acc);
            final Map<String, Object> xRaw = result.records().get(0);
            Assertions.assertTrue((Double) xRaw.get("z") > 0);
            Assertions.assertTrue((Double) xRaw.get("est_gain") > (Double) xRaw.get("threshold"));
            Assertions.assertEquals(2L, xRaw.get("n_periods"));
            Assertions.assertEquals(1L, xRaw.get("periods_agree"));
            final boolean expected = pass.isEmpty() || pass.contains("0.5");
            Assertions.assertEquals(expected, xRaw.get("passed"), pass);
            Assertions.assertEquals(expected ? List.of("x") : List.of(), result.summary().get("passedColumns"), pass);
            final String rule = (String) result.summary().get("passRule");
            Assertions.assertTrue(rule.startsWith("est_gain > threshold"), rule);
            Assertions.assertEquals(!pass.isEmpty(), rule.contains("periods_agree"), rule);
            final com.google.gson.JsonObject selection = ScreenReport.selection(spec, result);
            Assertions.assertEquals(rule, selection.get("passRule").getAsString());
            if (expected) {
                Assertions.assertEquals(1L, selection.getAsJsonArray("passed").get(0).getAsJsonObject().get("periods_agree").getAsLong());
                Assertions.assertEquals(2L, selection.getAsJsonArray("passed").get(0).getAsJsonObject().get("n_periods").getAsLong());
            }
        }
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: groupedMultinomial, group: g, label: y, candidates: [x], pass: {minPeriodsAgree: 0.5}}"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: groupedMultinomial, group: g, label: y, candidates: [x], periods: year, pass: {minPeriodsAgree: 0}}"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: groupedMultinomial, group: g, label: y, candidates: [x], periods: year, pass: {minPeriodsAgree: 1.5}}"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: groupedMultinomial, group: g, label: y, candidates: [x], periods: year, pass: 0.5}"));
    }

    @Test
    public void testWindowQuantilesTransformsForIndependentRows() throws Exception {
        // 40 independent rows: x = 1..40 (with one missing), the label the top half. Within a single-row unit
        // rank / absdev carry nothing; against the window sketch (exact below k values) they are the mid-rank
        // fraction and the distance to the window median.
        final ScreenSpec spec = spec("{family: binomial, label: y, candidates: [x], transforms: [raw, rank, absdev], placebo: {noise: 2, seed: 3}}");
        final List<ScreenRow> rows = new java.util.ArrayList<>();
        for (int i = 1; i <= 40; i++) {
            rows.add(new ScreenRow("r" + i, "r" + i, i, null, i > 20 ? 1 : 0, Double.NaN, 1, new double[]{i == 7 ? Double.NaN : i}));
        }
        WindowQuantiles q = new WindowQuantiles(1);
        WindowQuantiles other = new WindowQuantiles(1);
        for (int i = 0; i < rows.size(); i++) (i % 2 == 0 ? q : other).update(rows.get(i).x);
        // the coder round-trips and the merge is column-wise
        final java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
        WindowQuantiles.CODER.encode(other, bytes);
        q = new WindowQuantiles.Fn().mergeAccumulators(List.of(new WindowQuantiles(0), q, WindowQuantiles.CODER.decode(new java.io.ByteArrayInputStream(bytes.toByteArray()))));
        Assertions.assertEquals(39L, q.count(0));
        Assertions.assertEquals(21d, q.median(0), 1e-12);   // 39 values: the inclusive median is the 20th, value 21 (7 is missing)
        final double[] rank = GroupScorer.transform(spec, q, 0, "rank", new double[]{1, 40, 21, Double.NaN});
        Assertions.assertEquals(0.5 / 39, rank[0], 1e-12);    // nothing below, half of itself
        Assertions.assertEquals(38.5 / 39, rank[1], 1e-12);
        Assertions.assertEquals(19.5 / 39, rank[2], 1e-12);   // 19 values below 21 (7 missing), half of itself
        Assertions.assertTrue(Double.isNaN(rank[3]));
        final double[] absdev = GroupScorer.transform(spec, q, 0, "absdev", new double[]{1, 40, Double.NaN});
        Assertions.assertEquals(Math.abs(1 - q.median(0)), absdev[0], 1e-12);
        Assertions.assertEquals(Math.abs(40 - q.median(0)), absdev[1], 1e-12);
        Assertions.assertTrue(Double.isNaN(absdev[2]));
        // a noise placebo (column index beyond the candidates) takes the exact normal cdf and |x|
        final double[] noiseRank = GroupScorer.transform(spec, q, 1, "rank", new double[]{0, 1.96, -1.96});
        Assertions.assertEquals(0.5, noiseRank[0], 1e-12);
        Assertions.assertEquals(0.975, noiseRank[1], 1e-4);
        Assertions.assertEquals(0.025, noiseRank[2], 1e-4);
        Assertions.assertArrayEquals(new double[]{0, 1.96, 1.96}, GroupScorer.transform(spec, q, 1, "absdev", new double[]{0, 1.96, -1.96}), 1e-12);
        // raw is untouched, and without sketches the within-unit transform applies
        Assertions.assertArrayEquals(new double[]{3, 1}, GroupScorer.transform(spec, q, 0, "raw", new double[]{3, 1}), 0d);
        Assertions.assertArrayEquals(new double[]{1, 0}, GroupScorer.transform(spec, null, 0, "rank", new double[]{3, 1}), 0d);

        // scored as independent units: rank carries the monotone effect (x itself is monotone in the label, so
        // rank and raw agree in sign and the rank z is close to the raw one), absdev sees the symmetric "extremeness"
        final GroupScorer scorer = new GroupScorer(spec).withWindowQuantiles(q);
        final Map<Integer, ScoreAccumulator> acc = new HashMap<>();
        for (final ScreenRow r : rows) scorer.score(List.of(r), r.getIdentity(), acc);
        final ScreenReport.Result result = ScreenReport.build(spec, acc);
        final Map<String, Map<String, Object>> byKey = new HashMap<>();
        for (final Map<String, Object> r : result.records()) byKey.put(r.get("candidate") + ":" + r.get("transform"), r);
        final double zRaw = (Double) byKey.get("x:raw").get("z");
        final double zRank = (Double) byKey.get("x:rank").get("z");
        Assertions.assertTrue(zRaw > 3 && zRank > 3, "raw " + zRaw + " rank " + zRank);
        Assertions.assertEquals(zRaw, zRank, 0.25 * zRaw);
        Assertions.assertTrue(Math.abs((Double) byKey.get("x:absdev").get("z")) < 1.5, "absdev z " + byKey.get("x:absdev").get("z"));
        Assertions.assertEquals(39L, byKey.get("x:rank").get("n_obs"));
        Assertions.assertTrue(byKey.containsKey("__noise_0:rank") && byKey.containsKey("__noise_1:absdev"));
        Assertions.assertFalse((Boolean) byKey.get("x:rank").get("degenerate"));

        // an even count: the type-7 median (the mean of the two middle values), like the within-unit absdev
        final WindowQuantiles even = new WindowQuantiles(1);
        for (int i = 1; i <= 40; i++) even.update(new double[]{i});
        Assertions.assertEquals(20.5, even.median(0), 1e-12);
        Assertions.assertEquals(StatMath.medianFinite(new double[]{1, 2, 3, 4}), windowOf(1, 2, 3, 4).median(0), 1e-12);
        // the Combine's column-less default reads as no reference (NaN), not an index error
        Assertions.assertTrue(Double.isNaN(new WindowQuantiles(0).rank(0, 1d)));
        Assertions.assertTrue(Double.isNaN(new WindowQuantiles(0).median(0)));

        // addInput never aliases (then mutates) a Combine input: an empty accumulator adopts a copy
        final WindowQuantiles.Fn fn = new WindowQuantiles.Fn();
        final WindowQuantiles in1 = windowOf(1, 2);
        final WindowQuantiles acc1 = fn.addInput(fn.createAccumulator(), in1);
        Assertions.assertNotSame(in1, acc1);
        fn.addInput(acc1, windowOf(3, 4, 5));
        Assertions.assertEquals(2L, in1.count(0));
        Assertions.assertEquals(5L, acc1.count(0));

        // independent rows with rank / absdev: a scorer without the window reference fails instead of scoring
        // single-row units (rank 0.5, absdev 0) as silently degenerate records
        Assertions.assertThrows(IllegalStateException.class, () -> new GroupScorer(spec).score(List.of(rows.get(0)), "r1", new HashMap<>()));
    }

    private static WindowQuantiles windowOf(final double... values) {
        final WindowQuantiles q = new WindowQuantiles(1);
        for (final double v : values) q.update(new double[]{v});
        return q;
    }

    @Test
    public void testWindowQuantilesRejectMergingWindows() {
        // the window reference is a side input: a session window cannot map to it (Beam throws at assembly)
        final ScreenSpec rank = spec("{family: binomial, label: y, candidates: [x], transforms: [raw, rank]}");
        final ScreenSpec raw = spec("{family: binomial, label: y, candidates: [x]}");
        final org.apache.beam.sdk.Pipeline p = org.apache.beam.sdk.Pipeline.create();
        final org.apache.beam.sdk.values.PCollection<com.mercari.solution.module.MElement> input = p
                .apply(org.apache.beam.sdk.transforms.Create.empty(org.apache.beam.sdk.coders.SerializableCoder.of(com.mercari.solution.module.MElement.class)));
        final org.apache.beam.sdk.values.PCollection<com.mercari.solution.module.MElement> sessions = input
                .apply(org.apache.beam.sdk.transforms.windowing.Window.into(org.apache.beam.sdk.transforms.windowing.Sessions.withGapDuration(org.joda.time.Duration.standardMinutes(10))));
        final org.apache.beam.sdk.values.PCollection<com.mercari.solution.module.MElement> fixed = input
                .apply(org.apache.beam.sdk.transforms.windowing.Window.into(org.apache.beam.sdk.transforms.windowing.FixedWindows.of(org.joda.time.Duration.standardDays(1))));
        Assertions.assertTrue(ScreenStages.engineConstraints(sessions, rank).stream().anyMatch(m -> m.contains("merging (session) windows")));
        Assertions.assertTrue(ScreenStages.engineConstraints(sessions, raw).isEmpty());
        Assertions.assertTrue(ScreenStages.engineConstraints(fixed, rank).isEmpty());
        Assertions.assertTrue(ScreenStages.engineConstraints(input, rank).isEmpty());
    }

    @Test
    public void testBinnedBlockTest() throws Exception {
        // independent binomial rows, x = 1..40, the label a band effect (1 iff 15 <= x <= 25): invisible to the linear
        // test, caught by the binned block. 4 value bins from the window sketch (exact below k values) + the
        // missing bin; no placebo, so each kind takes its theoretical chi2(df) cut.
        final ScreenSpec spec = spec("{family: binomial, label: y, candidates: [x], transforms: [raw, binned], bins: {k: 4}, placebo: {noise: 0}}");
        Assertions.assertTrue(spec.hasBinned() && spec.needsWindowQuantiles());
        Assertions.assertEquals(5, spec.binCount());
        final List<ScreenRow> rows = new java.util.ArrayList<>();
        for (int i = 1; i <= 40; i++) rows.add(new ScreenRow("r" + i, "r" + i, i, null, i >= 15 && i <= 25 ? 1 : 0, Double.NaN, 1, new double[]{i}));
        final WindowQuantiles q = new WindowQuantiles(1);
        for (final ScreenRow r : rows) q.update(r.x);
        final GroupScorer scorer = new GroupScorer(spec).withWindowQuantiles(q);
        final int[] bins = scorer.bins(0, new double[]{1, 10, 11, 20, 30, 40, Double.NaN});
        Assertions.assertArrayEquals(new int[]{0, 0, 1, 1, 2, 3, 4}, bins);   // edges 10 / 20 / 30 (inclusive upper)
        final Map<Integer, ScoreAccumulator> acc = new HashMap<>();
        for (final ScreenRow r : rows) scorer.score(List.of(r), r.getIdentity(), acc);
        final ScreenReport.Result result = ScreenReport.build(spec, acc, null, null, new ScreenReport.Bins(scorer::binRepresentatives, scorer::binEdges));
        final Map<String, Map<String, Object>> byKey = new HashMap<>();
        for (final Map<String, Object> r : result.records()) byKey.put(r.get("candidate") + ":" + r.get("transform"), r);
        final Map<String, Object> raw = byKey.get("x:raw");
        final Map<String, Object> binned = byKey.get("x:binned");
        // the block's edges travel in the record and, for a passing block, into the pass list as a row bin op
        Assertions.assertEquals(List.of(10d, 20d, 30d), binned.get("bin_edges"));
        Assertions.assertNull(raw.get("bin_edges"));
        final com.google.gson.JsonObject selection = ScreenReport.selection(spec, result);
        Assertions.assertEquals(1, selection.getAsJsonArray("passedBlocks").size());
        final com.google.gson.JsonObject block = selection.getAsJsonArray("passedBlocks").get(0).getAsJsonObject();
        Assertions.assertEquals("x", block.get("candidate").getAsString());
        Assertions.assertEquals(4, block.get("k").getAsInt());
        Assertions.assertEquals(3, block.getAsJsonArray("edges").size());
        final String fragment = block.get("fragment").getAsString();
        Assertions.assertEquals("{scope: row, type: bin, input: x, edges: [10.000000000000002, 20.000000000000004, 30.000000000000004]}", fragment);
        // the fragment reproduces the block's bins under the row bin op's rule (the count of edges a value reaches),
        // the values at an edge included
        final String[] opEdges = fragment.substring(fragment.indexOf("edges: [") + 8, fragment.lastIndexOf(']')).split(", ");
        final double[] probe = {1, 10, 11, 20, 30, 40};
        final int[] screenBins = scorer.bins(0, probe);
        for (int i = 0; i < probe.length; i++) {
            int op = 0;
            for (final String e : opEdges) if (probe[i] >= Double.parseDouble(e)) op++;
            Assertions.assertEquals(screenBins[i], op, "value " + probe[i]);
        }
        Assertions.assertEquals("x", selection.getAsJsonArray("passed").get(0).getAsJsonObject().getAsJsonObject("bins").get("candidate").getAsString());
        Assertions.assertTrue(Math.abs((Double) raw.get("z")) < 2, "raw z " + raw.get("z"));
        Assertions.assertEquals(3L, binned.get("df"));
        // the excess gain subtracts the record's own df / 2N: 3 / 80 for the block, 1 / 80 for raw
        Assertions.assertEquals((Double) binned.get("est_gain") - 3d / 80, (Double) binned.get("excess_gain"), 1e-12);
        Assertions.assertEquals((Double) raw.get("est_gain") - 1d / 80, (Double) raw.get("excess_gain"), 1e-12);
        Assertions.assertNull(binned.get("partial_excess_gain"));
        Assertions.assertNull(binned.get("z"));
        Assertions.assertNull(binned.get("S"));
        Assertions.assertTrue((Double) binned.get("chi2") > 8, "binned chi2 " + binned.get("chi2"));   // bins 0 / 6 / 5 / 0 of 10 positives
        Assertions.assertTrue((Double) binned.get("pValue") < 0.01);
        Assertions.assertEquals(40L, binned.get("n_obs"));
        Assertions.assertEquals(Boolean.TRUE, binned.get("passed"));
        Assertions.assertEquals(Boolean.FALSE, raw.get("passed"));
        // per-kind theoretical thresholds: chi2(1) for raw, chi2(3) for the block, both over 2N
        Assertions.assertEquals(StatMath.chiSquare1Quantile(0.99) / 80, (Double) raw.get("threshold"), 1e-12);
        Assertions.assertEquals(StatMath.chiSquareQuantile(0.99, 3) / 80, (Double) binned.get("threshold"), 1e-9);
        Assertions.assertTrue((Double) binned.get("threshold") > (Double) raw.get("threshold"));
        @SuppressWarnings("unchecked") final List<Map<String, Object>> binStats = (List<Map<String, Object>>) binned.get("bin_stats");
        Assertions.assertEquals(5, binStats.size());
        double n = 0, s = 0;
        for (final Map<String, Object> b : binStats) {
            n += (Double) b.get("n");
            s += (Double) b.get("S");
        }
        Assertions.assertEquals(40d, n, 1e-12);
        Assertions.assertEquals(0d, s, 1e-9);   // prior mode: the bin scores sum to zero (the intercept profiled out)
        Assertions.assertEquals(0d, (Double) binStats.get(4).get("n"));   // the missing bin is empty
        // the intercept profiled out, every active bin counted (none dropped as a reference): S = [-2.75, 3.25,
        // 2.25, -2.75] against H = 10 * 0.275 * 0.725 per bin
        Assertions.assertEquals(30.75 / 1.99375, (Double) binned.get("chi2"), 1e-9);
        Assertions.assertEquals(profiledChi2(binStats), (Double) binned.get("chi2"), 1e-9);
        @SuppressWarnings("unchecked") final Map<String, Double> thresholds = (Map<String, Double>) result.summary().get("thresholds");
        Assertions.assertEquals(2, thresholds.size());
        Assertions.assertEquals("value/4", result.summary().get("bins"));
        Assertions.assertTrue(ScreenReport.describe(spec).contains("bins=value/4"));

        // grouped, position bins: a column whose largest value wins (x = 3, 1, 2 with the label on 3) — the block
        // sees the effect with df = 2 (three occupied position bins), a within-group constant is degenerate
        final ScreenSpec grouped = spec("{family: groupedMultinomial, group: g, label: y, candidates: [x, x2], transforms: [binned], bins: {k: 3, edges: rank}, placebo: {noise: 0}}");
        Assertions.assertFalse(grouped.needsWindowQuantiles());
        final GroupScorer gs = new GroupScorer(grouped);
        final Map<Integer, ScoreAccumulator> gacc = new HashMap<>();
        for (int g = 0; g < 30; g++) {
            gs.score(List.of(
                    new ScreenRow("g" + g, "p", g, null, 1, Double.NaN, 1, new double[]{3, 7}),
                    new ScreenRow("g" + g, "q", g, null, 0, Double.NaN, 1, new double[]{1, 7}),
                    new ScreenRow("g" + g, "r", g, null, 0, Double.NaN, 1, new double[]{2, 7})), "g" + g, gacc);
        }
        final ScreenReport.Result gr = ScreenReport.build(grouped, gacc);
        final Map<String, Object> xb = gr.records().get(0);
        final Map<String, Object> cb = gr.records().get(1);
        Assertions.assertEquals("x", xb.get("candidate"));
        Assertions.assertEquals(2L, xb.get("df"));
        Assertions.assertTrue((Double) xb.get("chi2") > 20, "grouped binned chi2 " + xb.get("chi2"));
        Assertions.assertEquals(Boolean.TRUE, cb.get("degenerate"));
        Assertions.assertEquals(0L, cb.get("df"));
        // a passing position block: no value edges, the rank cut points i / k
        Assertions.assertNull(xb.get("bin_edges"));
        Assertions.assertEquals(Boolean.TRUE, xb.get("passed"));
        final com.google.gson.JsonObject rankBlock = ScreenReport.selection(grouped, gr).getAsJsonArray("passedBlocks").get(0).getAsJsonObject();
        Assertions.assertEquals("rank", rankBlock.get("edgesKind").getAsString());
        Assertions.assertTrue(rankBlock.get("edges").isJsonNull());
        Assertions.assertEquals(2, rankBlock.getAsJsonArray("rankCuts").size());
        Assertions.assertEquals(1d / 3, rankBlock.getAsJsonArray("rankCuts").get(0).getAsDouble(), 1e-15);
        // a value block without its edges (a report built without the bins' geometry) writes no empty row op
        final com.google.gson.JsonObject noEdges = ScreenReport.blockRecipe(spec, "x", null);
        Assertions.assertTrue(noEdges.get("edges").isJsonNull());
        Assertions.assertFalse(noEdges.get("fragment").getAsString().contains("type: bin"));

        // offset mode: a baseline miscalibrated overall (0.1 against a rate of 0.275) is the intercept's misfit,
        // which the block must profile out rather than score: χ² = Σ S_b² / H_b − (Σ S_b)² / Σ H_b on the raw S_b
        final ScreenSpec offset = spec("{family: binomial, label: y, baseline: {field: b, form: prob}, candidates: [x], transforms: [binned], bins: {k: 4}, placebo: {noise: 0}}");
        final GroupScorer os = new GroupScorer(offset).withWindowQuantiles(q);
        final Map<Integer, ScoreAccumulator> oacc = new HashMap<>();
        for (int i = 1; i <= 40; i++) {
            final ScreenRow r = new ScreenRow("r" + i, "r" + i, i, null, i >= 15 && i <= 25 ? 1 : 0, 0.1, 1, new double[]{i});
            os.score(List.of(r), r.getIdentity(), oacc);
        }
        final Map<String, Object> ob = ScreenReport.build(offset, oacc).records().get(0);
        @SuppressWarnings("unchecked") final List<Map<String, Object>> obStats = (List<Map<String, Object>>) ob.get("bin_stats");
        Assertions.assertEquals(3L, ob.get("df"));
        // S = [-1, 5, 4, -1], H = 0.9 per bin: 43 / 0.9 − 49 / 3.6
        Assertions.assertEquals(43 / 0.9 - 49 / 3.6, (Double) ob.get("chi2"), 1e-9);
        Assertions.assertEquals(profiledChi2(obStats), (Double) ob.get("chi2"), 1e-9);

        // grouped with value bins: the run holds the window sketches for the edges, but rank / absdev stay within the
        // unit (a shuffle placebo is not standard normal, a candidate's window rank is not its within-unit rank)
        final ScreenSpec groupedValue = spec("{family: groupedMultinomial, group: g, label: y, candidates: [x], transforms: [rank, absdev, binned], placebo: {noise: 0}}");
        Assertions.assertTrue(groupedValue.needsWindowQuantiles());
        final double[] unitValues = {5, 30, 12};
        for (final String t : List.of(ScreenSpec.TRANSFORM_RANK, ScreenSpec.TRANSFORM_ABSDEV)) {
            Assertions.assertArrayEquals(GroupScorer.transform(t, unitValues), GroupScorer.transform(groupedValue, q, 0, t, unitValues), 1e-12, t);
        }
        Assertions.assertTrue(groupedValue.notes.stream().noneMatch(note -> note.contains("rank / absdev of independent rows")));

        // validation
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: binomial, label: y, candidates: [x], bins: {k: 4}}"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: binomial, label: y, candidates: [x], transforms: [binned], bins: {k: 1}}"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: binomial, label: y, candidates: [x], transforms: [binned], bins: {edges: rank}}"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: binomial, label: y, candidates: [x], transforms: [binned], bins: {edges: median}}"));
        Assertions.assertEquals(10, spec("{family: binomial, label: y, candidates: [x], transforms: [binned]}").binsK);
    }

    /** The row families' block statistic from its bin_stats: Σ S_b² / H_b − (Σ S_b)² / Σ H_b over the bins with information. */
    private static double profiledChi2(final List<Map<String, Object>> binStats) {
        double q = 0, sumS = 0, sumH = 0;
        for (final Map<String, Object> b : binStats) {
            final double s = (Double) b.get("S");
            final double h = (Double) b.get("H");
            if (!(h > 0)) continue;
            q += s * s / h;
            sumS += s;
            sumH += h;
        }
        return q - sumS * sumS / sumH;
    }

    @Test
    public void testHeterogeneityAcrossModifier() throws Exception {
        // independent binomial rows in two segments (the modifier field g, not a candidate): x pushes the label up
        // in segment A and down in segment B, so the window effect cancels (|z| small) while the heterogeneity
        // test (df 1 over two levels) is large and the level slices carry opposite signs
        final ScreenSpec spec = spec("{family: binomial, label: y, candidates: [x], transforms: [raw], heterogeneity: {field: g}, placebo: {noise: 0}}");
        Assertions.assertTrue(spec.hasHeterogeneity() && !spec.heterogeneityByPeriods());
        Assertions.assertEquals("field:g", spec.heterogeneityLabel());
        Assertions.assertEquals(List.of("x"), spec.candidates);   // g is reserved
        final GroupScorer scorer = new GroupScorer(spec);
        final Map<Integer, ScoreAccumulator> acc = new HashMap<>();
        for (int i = 1; i <= 40; i++) {
            final ScreenRow a = new ScreenRow("a" + i, "a" + i, i, null, "A", i > 20 ? 1 : 0, Double.NaN, 1, new double[]{i});
            final ScreenRow b = new ScreenRow("b" + i, "b" + i, i, null, "B", i > 20 ? 0 : 1, Double.NaN, 1, new double[]{i});
            scorer.score(List.of(a), a.getIdentity(), acc);
            scorer.score(List.of(b), b.getIdentity(), acc);
        }
        final ScreenReport.Result result = ScreenReport.build(spec, acc);
        final Map<String, Object> r = result.records().get(0);
        Assertions.assertTrue(Math.abs((Double) r.get("z")) < 1, "window z " + r.get("z"));
        Assertions.assertEquals(1L, r.get("het_df"));
        Assertions.assertEquals(2L, r.get("het_levels"));
        Assertions.assertTrue((Double) r.get("het_chi2") > 20, "het chi2 " + r.get("het_chi2"));
        Assertions.assertTrue((Double) r.get("het_pValue") < 1e-4);
        Assertions.assertEquals((Double) r.get("het_chi2") / 160, (Double) r.get("het_gain"), 1e-12);
        Assertions.assertEquals(Boolean.TRUE, r.get("het_passed"));
        Assertions.assertEquals(Boolean.FALSE, r.get("passed"));
        Assertions.assertNull(r.get("partial_het_chi2"));
        @SuppressWarnings("unchecked") final List<Map<String, Object>> levels = (List<Map<String, Object>>) r.get("level_z");
        Assertions.assertEquals(2, levels.size());
        Assertions.assertEquals("A", levels.get(0).get("level"));
        Assertions.assertTrue((Double) levels.get(0).get("z") > 3 && (Double) levels.get(1).get("z") < -3, levels.toString());
        Assertions.assertEquals(40L, levels.get(0).get("n"));
        // the het test's own theoretical cut: chi2(1) over 2N, next to the df1 cut
        @SuppressWarnings("unchecked") final Map<String, Double> thresholds = (Map<String, Double>) result.summary().get("thresholds");
        Assertions.assertEquals(StatMath.chiSquare1Quantile(0.99) / 160, thresholds.get("het"), 1e-12);
        Assertions.assertEquals("field:g", result.summary().get("heterogeneity"));
        Assertions.assertEquals(1L, result.summary().get("nHetPassed"));
        Assertions.assertEquals(List.of("x"), result.summary().get("hetPassedColumns"));
        Assertions.assertEquals(List.of(), result.summary().get("passedColumns"));
        final com.google.gson.JsonObject selection = ScreenReport.selection(spec, result);
        Assertions.assertEquals("field:g", selection.get("heterogeneity").getAsString());
        Assertions.assertEquals(1, selection.getAsJsonArray("hetPassedColumns").size());
        Assertions.assertEquals(0, selection.getAsJsonArray("columns").size());
        Assertions.assertTrue(ScreenReport.describe(spec).contains("heterogeneity=field:g"));

        // by periods: the data of testPassRuleMinPeriodsAgree (2025 positive, 2024 the other way) — the period
        // slices are the levels, so level_z stays null and period_z holds them
        final ScreenSpec byPeriods = spec("{family: groupedMultinomial, group: g, label: y, time: t, candidates: [x], transforms: [raw], placebo: {noise: 0}, periods: year, heterogeneity: periods}");
        Assertions.assertTrue(byPeriods.heterogeneityByPeriods());
        final GroupScorer gs = new GroupScorer(byPeriods);
        final Map<Integer, ScoreAccumulator> gacc = new HashMap<>();
        for (int g = 0; g < 30; g++) {
            gs.score(List.of(
                    new ScreenRow("g" + g, "p", g, "2025", 1, Double.NaN, 1, new double[]{3}),
                    new ScreenRow("g" + g, "q", g, "2025", 0, Double.NaN, 1, new double[]{1}),
                    new ScreenRow("g" + g, "r", g, "2025", 0, Double.NaN, 1, new double[]{2})), "g" + g, gacc);
        }
        for (int g = 0; g < 8; g++) {
            gs.score(List.of(
                    new ScreenRow("h" + g, "p", g, "2024", 0, Double.NaN, 1, new double[]{3}),
                    new ScreenRow("h" + g, "q", g, "2024", 1, Double.NaN, 1, new double[]{1}),
                    new ScreenRow("h" + g, "r", g, "2024", 0, Double.NaN, 1, new double[]{2})), "h" + g, gacc);
        }
        final Map<String, Object> pr = ScreenReport.build(byPeriods, gacc).records().get(0);
        Assertions.assertEquals(1L, pr.get("het_df"));
        Assertions.assertTrue((Double) pr.get("het_chi2") > 0, "het chi2 by periods " + pr.get("het_chi2"));
        Assertions.assertNull(pr.get("level_z"));
        Assertions.assertEquals(2L, pr.get("n_periods"));

        // validation
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: binomial, label: y, candidates: [x], heterogeneity: periods}"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: binomial, label: y, candidates: [x], heterogeneity: {by: segment}}"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: binomial, label: y, candidates: [x], heterogeneity: {field: nope}}"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: binomial, label: y, candidates: [x], heterogeneity: {by: field}}"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: binomial, label: y, time: t, candidates: [x], periods: year, heterogeneity: {by: periods, field: g}}"));
        // the block test has no direction: a modifier with binned alone would test nothing
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: binomial, label: y, candidates: [x], transforms: [binned], heterogeneity: {field: g}}"));

        // with noise placebos a df = 1 placebo record feeds two placebo kinds (df1 and het) but counts once
        final ScreenSpec withNoise = spec("{family: binomial, label: y, candidates: [x], transforms: [raw], heterogeneity: {field: g}, placebo: {noise: 3}}");
        final GroupScorer ns = new GroupScorer(withNoise);
        final Map<Integer, ScoreAccumulator> nacc = new HashMap<>();
        for (int i = 1; i <= 40; i++) {
            final ScreenRow a = new ScreenRow("a" + i, "a" + i, i, null, "A", i > 20 ? 1 : 0, Double.NaN, 1, new double[]{i});
            ns.score(List.of(a), a.getIdentity(), nacc);
        }
        final ScreenReport.Result nr = ScreenReport.build(withNoise, nacc);
        Assertions.assertEquals(3L, nr.summary().get("nPlacebo"));
        Assertions.assertEquals(4, nr.records().size());
    }

    @Test
    public void testSuggestionsFromBinnedSums() throws Exception {
        // 200 independent binomial rows: x = 1..180 with a band effect (label 1 for 75 <= x <= 125), then 20 rows
        // with x missing and the label 1 — the missing bin carries its own effect. Four value bins from the window
        // sketch; the shapes are chosen on the discovery half (a seeded split) and reported on the confirmation half.
        final ScreenSpec spec = spec("{family: binomial, label: y, candidates: [x], transforms: [raw, binned], bins: {k: 4}, suggestions: true, placebo: {noise: 0}}");
        Assertions.assertTrue(spec.suggestionsOn);
        final List<ScreenRow> rows = new java.util.ArrayList<>();
        for (int i = 1; i <= 200; i++) {
            final boolean missing = i > 180;
            rows.add(new ScreenRow("r" + i, "r" + i, i, null, missing || (i >= 75 && i <= 125) ? 1 : 0, Double.NaN, 1, new double[]{missing ? Double.NaN : i}));
        }
        final WindowQuantiles q = new WindowQuantiles(1);
        for (final ScreenRow r : rows) q.update(r.x);
        final GroupScorer scorer = new GroupScorer(spec).withWindowQuantiles(q);
        final Map<Integer, ScoreAccumulator> acc = new HashMap<>();
        int discovery = 0;
        for (final ScreenRow r : rows) {
            scorer.score(List.of(r), r.getIdentity(), acc);
            if (scorer.discovery(r.getIdentity())) discovery++;
        }
        Assertions.assertTrue(discovery > 60 && discovery < 140, "discovery half " + discovery);
        // the binned key carries the window sums and the discovery half's (twice the length)
        final double[] extra = acc.get(spec.key(0, 1)).getExtra();
        Assertions.assertEquals(2 * (3 * 5 + 3), extra.length);
        final ScreenReport.Result result = ScreenReport.build(spec, acc, null, null, new ScreenReport.Bins(scorer::binRepresentatives, scorer::binEdges));
        final Map<String, Map<String, Object>> byKind = new HashMap<>();
        for (final Map<String, Object> s : result.suggestions()) byKind.put((String) s.get("kind"), s);
        Assertions.assertEquals(4, result.suggestions().size(), result.suggestions().toString());
        Assertions.assertEquals(4L, result.summary().get("nSuggestions"));
        // the shape: a band is a distance to its centre (or a hinge / step near it), not a line
        final Map<String, Object> shape = byKind.get("shape");
        Assertions.assertTrue(List.of("abs", "hinge", "step").contains(shape.get("name")), shape.toString());
        Assertions.assertTrue((Double) shape.get("share") > 0.5 && (Double) shape.get("share") <= 1.0 + 1e-9, shape.toString());
        Assertions.assertTrue((Double) shape.get("confirmation_share") > 0.3, shape.toString());
        Assertions.assertTrue((Double) shape.get("cut") > 40 && (Double) shape.get("cut") < 140, shape.toString());
        Assertions.assertEquals(Boolean.TRUE, shape.get("passed"));
        Assertions.assertTrue(((String) shape.get("fragment")).contains("x"), shape.toString());
        // the cut: a step at one of the edges (45 / 90 / 135)
        final Map<String, Object> cut = byKind.get("cut");
        Assertions.assertEquals("step", cut.get("name"));
        Assertions.assertTrue((Double) cut.get("cut") > 40 && (Double) cut.get("cut") < 140, cut.toString());
        Assertions.assertTrue(((String) cut.get("fragment")).contains("type: bin"), cut.toString());
        // missingness: the missing rows are all positive — an effect of its own, matched by a value bin inside the band
        final Map<String, Object> missing = byKind.get("missing");
        Assertions.assertEquals("+", missing.get("direction"));
        Assertions.assertTrue((Double) missing.get("chi2") > 5, missing.toString());
        Assertions.assertTrue((Double) missing.get("fill") > 40 && (Double) missing.get("fill") < 140, missing.toString());
        Assertions.assertTrue(((String) missing.get("fragment")).contains("== null"), missing.toString());
        // monotone: a band is not monotone — the isotonic fit keeps a share, the sign consistency is partial
        final Map<String, Object> monotone = byKind.get("monotone");
        Assertions.assertTrue((Double) monotone.get("consistency") < 1.0, monotone.toString());
        Assertions.assertTrue((Double) monotone.get("share") >= 0 && (Double) monotone.get("share") <= 1.0 + 1e-9, monotone.toString());
        for (final Map<String, Object> s : result.suggestions()) {
            Assertions.assertEquals("x", s.get("candidate"));
            Assertions.assertEquals(Boolean.FALSE, s.get("placebo"));
            Assertions.assertNotNull(s.get("threshold"));
            Assertions.assertTrue((Double) s.get("confirmation_pValue") >= 0 && (Double) s.get("confirmation_pValue") <= 1);
        }
        // the isotonic fit itself
        Assertions.assertArrayEquals(new double[]{1, 2.5, 2.5, 4}, ScreenReport.isotonic(new double[]{1, 3, 2, 4}, new double[]{1, 1, 1, 1}, true), 1e-12);
        Assertions.assertArrayEquals(new double[]{4, 2.5, 2.5, 1}, ScreenReport.isotonic(new double[]{4, 2, 3, 1}, new double[]{1, 1, 1, 1}, false), 1e-12);
        // without the discovery split (suggestions off) the block reads the plain sums, and no suggestion is produced
        Assertions.assertTrue(ScreenReport.build(spec("{family: binomial, label: y, candidates: [x], transforms: [binned], placebo: {noise: 0}}"), new HashMap<>()).suggestions().isEmpty());
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: binomial, label: y, candidates: [x], suggestions: true}"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: binomial, label: y, candidates: [x], transforms: [binned], suggestions: {enabled: {}}}"));
        Assertions.assertTrue(spec("{family: binomial, label: y, candidates: [x], transforms: [binned], suggestions: {}}").suggestionsOn);
    }

    @Test
    public void testSuggestionShareBoundedByContrasts() throws Exception {
        // a pure step at the median (binomial, no baseline): the step contrast carries the whole effect. The share is
        // read against the maximum over the centred contrasts, so it stays in [0, 1] — against the block χ² with a
        // reference bin dropped from a diagonal H it would exceed 1 (about 1.25 here)
        final ScreenSpec spec = spec("{family: binomial, label: y, candidates: [x], transforms: [binned], bins: {k: 4}, suggestions: true, placebo: {noise: 0}}");
        final List<ScreenRow> rows = new java.util.ArrayList<>();
        for (int i = 1; i <= 400; i++) {
            final double y = i > 200 ? (i % 5 == 0 ? 0 : 1) : (i % 5 == 0 ? 1 : 0);
            rows.add(new ScreenRow("r" + i, "r" + i, i, null, y, Double.NaN, 1, new double[]{i}));
        }
        final WindowQuantiles q = new WindowQuantiles(1);
        for (final ScreenRow r : rows) q.update(r.x);
        final GroupScorer scorer = new GroupScorer(spec).withWindowQuantiles(q);
        final Map<Integer, ScoreAccumulator> acc = new HashMap<>();
        for (final ScreenRow r : rows) scorer.score(List.of(r), r.getIdentity(), acc);
        final ScreenReport.Result result = ScreenReport.build(spec, acc, null, null, new ScreenReport.Bins(scorer::binRepresentatives, scorer::binEdges));
        Assertions.assertFalse(result.suggestions().isEmpty());
        for (final Map<String, Object> s : result.suggestions()) {
            Assertions.assertTrue((Double) s.get("share") >= 0 && (Double) s.get("share") <= 1 + 1e-9, s.toString());
            Assertions.assertTrue((Double) s.get("confirmation_share") >= 0 && (Double) s.get("confirmation_share") <= 1 + 1e-9, s.toString());
            // a hinge names its side: one expression, not both
            if ("hinge".equals(s.get("name"))) Assertions.assertFalse(((String) s.get("fragment")).contains(" or "), s.toString());
        }
        for (final Map<String, Object> s : result.suggestions()) {
            if (!"cut".equals(s.get("kind"))) continue;
            Assertions.assertEquals(200d, (Double) s.get("cut"), 1e-9);
            Assertions.assertTrue((Double) s.get("share") > 0.95, s.toString());
        }
    }

    @Test
    public void testPairsAtTheFittedMeans() throws Exception {
        // grouped units of 4 whose winner follows softmax(x + x2 + 1.5 x·x2): the product carries information beyond
        // the main effects. Conditioning on both members (x doubles as the candidate), the pair [x, x2] is tested at
        // the fitted means and passes its own placebo kind (x × noise pairs); the placebo pairs do not.
        final ScreenSpec spec = spec("{family: groupedMultinomial, group: g, label: y, candidates: [x], transforms: [raw], placebo: {noise: 2, seed: 7}, "
                + "conditioning: {fields: [x, x2], l2: 1.0e-4, maxIter: 8}, pairs: {fields: [[x, x2]], placebo: 2}}");
        Assertions.assertTrue(spec.hasPairs());
        Assertions.assertEquals(1, spec.pairs.size());
        Assertions.assertEquals(3, spec.pairCount());
        Assertions.assertEquals("x*x2", spec.pairName(0));
        Assertions.assertEquals("x*__noise_1", spec.pairName(2));
        Assertions.assertArrayEquals(new int[]{0, -2}, spec.pairMembers(2));
        Assertions.assertEquals(List.of("x", "x", "x2"), spec.rowColumns());
        final java.util.Random random = new java.util.Random(11);
        final List<List<ScreenRow>> units = new java.util.ArrayList<>();
        for (int g = 0; g < 120; g++) {
            final double[] x1 = new double[4], x2 = new double[4], score = new double[4];
            int best = 0;
            for (int i = 0; i < 4; i++) {
                x1[i] = random.nextGaussian();
                x2[i] = random.nextGaussian();
                score[i] = x1[i] + x2[i] + 1.5 * x1[i] * x2[i] + 0.3 * random.nextGaussian();
                if (score[i] > score[best]) best = i;
            }
            final List<ScreenRow> rows = new java.util.ArrayList<>();
            for (int i = 0; i < 4; i++) rows.add(new ScreenRow("g" + g, "g" + g + ":" + i, i, null, i == best ? 1 : 0, Double.NaN, 1, new double[]{x1[i], x1[i], x2[i]}));
            units.add(rows);
        }
        // the pair's 2-D grid needs the members' value edges: the sketch pre-pass covers the conditioning columns too
        Assertions.assertTrue(spec.hasPairShape() && spec.needsWindowQuantiles());
        Assertions.assertEquals(3, spec.sketchColumns());
        final WindowQuantiles quantiles = new WindowQuantiles(spec.sketchColumns());
        for (final List<ScreenRow> rows : units) for (final ScreenRow r : rows) quantiles.update(r.x);
        final GroupScorer groups = new GroupScorer(spec).withWindowQuantiles(quantiles);
        final ConditioningScorer scorer = new ConditioningScorer(spec).withWindowQuantiles(quantiles);
        final com.mercari.solution.util.pipeline.glm.VectorAccumulator moments = new com.mercari.solution.util.pipeline.glm.VectorAccumulator();
        for (final List<ScreenRow> rows : units) for (final ScreenRow r : rows) moments.add(scorer.moments(r));
        com.mercari.solution.util.pipeline.glm.FitState state = com.mercari.solution.util.pipeline.glm.FitState.initial(scorer.k);
        for (int it = 0; it < 8 && !state.converged; it++) {
            final com.mercari.solution.util.pipeline.glm.VectorAccumulator eval = new com.mercari.solution.util.pipeline.glm.VectorAccumulator();
            for (final List<ScreenRow> rows : units) eval.add(scorer.evaluate(groups.prepare(rows, rows.get(0).getGroup()), state.proposal, moments.getValues()));
            state.advance(eval.getValues(), spec.conditioningL2, spec.conditioningTol);
        }
        Assertions.assertTrue(state.hasBest);
        final Map<Integer, PartialAccumulator> partials = new HashMap<>();
        final Map<Integer, ScoreAccumulator> marginal = new HashMap<>();
        for (final List<ScreenRow> rows : units) {
            final GroupScorer.Unit unit = groups.prepare(rows, rows.get(0).getGroup());
            scorer.partial(unit, groups.columns(unit), state.bestTheta, moments.getValues(), partials);
            groups.score(rows, rows.get(0).getGroup(), marginal);
        }
        Assertions.assertTrue(partials.containsKey(spec.pairKey(0)) && partials.containsKey(spec.pairKey(2)));
        Assertions.assertTrue(partials.containsKey(spec.pairGridKey(0)));
        Assertions.assertEquals(2 * 16 + 16 * 16, partials.get(spec.pairGridKey(0)).getTotal().length);   // 4 x 4 cells, grouped block
        final ScreenReport.Result result = ScreenReport.build(spec, marginal, partials, state, new ScreenReport.Bins(groups::binRepresentatives, groups::binEdges, groups::gridEdges));
        // the pair's interaction shape from its 2-D grid: a depth-2 tree over the cells, its share of the grid's block
        final List<Map<String, Object>> shapes = result.suggestions().stream().filter(s -> "interaction".equals(s.get("kind"))).toList();
        Assertions.assertEquals(1, shapes.size(), result.suggestions().toString());
        final Map<String, Object> shape = shapes.get(0);
        Assertions.assertEquals("x*x2", shape.get("candidate"));
        Assertions.assertTrue((Double) shape.get("share") > 0 && (Double) shape.get("share") <= 1.0 + 1e-9, shape.toString());
        Assertions.assertTrue((Double) shape.get("consistency") >= 0 && (Double) shape.get("consistency") <= 1.0, shape.toString());
        Assertions.assertTrue(Double.isFinite((Double) shape.get("cut")), shape.toString());
        Assertions.assertTrue(List.of(">", "<=").contains(shape.get("direction")), shape.toString());
        // the recipe: the two row bin ops at the next double above each cut (the screen's partition exactly), crossed
        Assertions.assertTrue(((String) shape.get("fragment")).contains("cross of {scope: row, type: bin, input: "), shape.toString());
        Assertions.assertTrue(((String) shape.get("fragment")).contains("edges: [" + Math.nextUp((Double) shape.get("cut")) + "]"), shape.toString());
        Assertions.assertNull(shape.get("passed"));
        Assertions.assertTrue(ScreenReport.describe(spec).contains("shape=4x4"));
        final Map<String, Map<String, Object>> byKey = new HashMap<>();
        for (final Map<String, Object> r : result.records()) byKey.put(r.get("candidate") + ":" + r.get("transform"), r);
        Assertions.assertEquals(3 + 3, result.records().size(), byKey.keySet().toString());   // x, 2 noise; the pair + 2 placebo pairs
        final Map<String, Object> pair = byKey.get("x*x2:product");
        Assertions.assertNull(pair.get("z"));
        Assertions.assertNull(pair.get("est_gain"));
        Assertions.assertTrue((Double) pair.get("partial_z") > 3, "partial z of the pair: " + pair.get("partial_z"));
        Assertions.assertTrue((Double) pair.get("r2_F") < 0.5, "r2_F of the pair: " + pair.get("r2_F"));
        Assertions.assertEquals(Boolean.TRUE, pair.get("passed"));
        Assertions.assertEquals(Boolean.FALSE, pair.get("placebo"));
        Assertions.assertEquals(Boolean.FALSE, pair.get("leakSuspect"));
        for (final String placebo : List.of("x*__noise_0:product", "x*__noise_1:product")) {
            final Map<String, Object> p = byKey.get(placebo);
            Assertions.assertEquals(Boolean.TRUE, p.get("placebo"));
            Assertions.assertEquals(Boolean.FALSE, p.get("passed"));
            Assertions.assertTrue(Math.abs((Double) p.get("partial_z")) < 3, placebo + " partial z " + p.get("partial_z"));
        }
        // the marginal test of x survives next to the pair; the main effect of x is in F, so its partial is ~ 0
        Assertions.assertTrue((Double) byKey.get("x:raw").get("r2_F") > 0.99, byKey.get("x:raw").toString());
        @SuppressWarnings("unchecked") final Map<String, Double> thresholds = (Map<String, Double>) result.summary().get("thresholds");
        Assertions.assertTrue(thresholds.containsKey("pair") && thresholds.containsKey("df1"));
        Assertions.assertEquals(1L, result.summary().get("nPairs"));
        Assertions.assertEquals(1L, result.summary().get("nPairsPassed"));
        Assertions.assertEquals(List.of("x*x2"), result.summary().get("passedPairs"));
        Assertions.assertEquals(List.of(), result.summary().get("passedColumns"));   // a pair is never a column
        Assertions.assertEquals(0L, result.summary().get("nPassed"));                // nor counted in nPassed
        final com.google.gson.JsonObject selection = ScreenReport.selection(spec, result);
        Assertions.assertEquals(0, selection.getAsJsonArray("columns").size());
        Assertions.assertEquals("x", selection.getAsJsonArray("passedPairs").get(0).getAsJsonObject().get("a").getAsString());
        Assertions.assertEquals("x2", selection.getAsJsonArray("passedPairs").get(0).getAsJsonObject().get("b").getAsString());
        Assertions.assertTrue(selection.getAsJsonArray("passedPairs").get(0).getAsJsonObject().get("fragment").getAsString().contains("x * x2"));
        Assertions.assertTrue(ScreenReport.describe(spec).contains("pairs=1"));
        // validation: pairs need conditioning holding both members, two different fields, a placebo count within the noise
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: groupedMultinomial, group: g, label: y, candidates: [x], pairs: {fields: [[x, x2]]}}"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: groupedMultinomial, group: g, label: y, candidates: [x], conditioning: {fields: [x2]}, pairs: {fields: [[x, x2]]}}"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: groupedMultinomial, group: g, label: y, candidates: [x], conditioning: {fields: [x, x2]}, pairs: {fields: [[x, x]]}}"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: groupedMultinomial, group: g, label: y, candidates: [x], placebo: {noise: 1}, conditioning: {fields: [x, x2]}, pairs: {fields: [[x, x2]], placebo: 3}}"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: groupedMultinomial, group: g, label: y, candidates: [x], conditioning: {fields: [x, x2]}, pairs: {among: ['x*'], maxPairs: 0}}"));
        final ScreenSpec among = spec("{family: groupedMultinomial, group: g, label: y, candidates: [x], conditioning: {fields: [x, x2]}, pairs: {among: ['x*'], placebo: 0}}");
        Assertions.assertEquals(1, among.pairs.size());
        Assertions.assertEquals(1, among.pairCount());
        // pairs.shape: one bin per member has no edge to split at; false turns the grid (and its pre-pass) off
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: groupedMultinomial, group: g, label: y, candidates: [x], conditioning: {fields: [x, x2]}, pairs: {fields: [[x, x2]], shape: 1}}"));
        final ScreenSpec off = spec("{family: groupedMultinomial, group: g, label: y, candidates: [x], transforms: [raw], conditioning: {fields: [x, x2]}, pairs: {fields: [[x, x2]], shape: false}}");
        Assertions.assertFalse(off.hasPairShape() || off.needsWindowQuantiles());
        // among resolving to a single field tests nothing: an error, not a silent no-op
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: groupedMultinomial, group: g, label: y, candidates: [x], conditioning: {fields: [x, x2]}, pairs: {among: [x2]}}"));
        // a null / object member is a configuration error (not an escaping UnsupportedOperationException)
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: groupedMultinomial, group: g, label: y, candidates: [x], conditioning: {fields: [x, x2]}, pairs: {fields: [[x, null]]}}"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: groupedMultinomial, group: g, label: y, candidates: [x], conditioning: {fields: [x, x2]}, pairs: {fields: 'x', among: ['x*']}}"));
    }

    @Test
    public void testPairKeysKeepTheirPartialUnderABinnedTransform() throws Exception {
        // transforms: [binned] alone — every key's modulus is the binned index, the pair keys' too: the pair keys after
        // the columns are [s, b, a] sums and must stay in the batched γ solve (the testPairsAtTheFittedMeans data)
        final ScreenSpec spec = spec("{family: groupedMultinomial, group: g, label: y, candidates: [x], transforms: [binned], bins: {k: 4, edges: value}, "
                + "placebo: {noise: 2, seed: 7}, conditioning: {fields: [x, x2], l2: 1.0e-4, maxIter: 8}, pairs: {fields: [[x, x2]], placebo: 2}}");
        final java.util.Random random = new java.util.Random(11);
        final List<List<ScreenRow>> units = new java.util.ArrayList<>();
        for (int g = 0; g < 120; g++) {
            final double[] x1 = new double[4], x2 = new double[4], score = new double[4];
            int best = 0;
            for (int i = 0; i < 4; i++) {
                x1[i] = random.nextGaussian();
                x2[i] = random.nextGaussian();
                score[i] = x1[i] + x2[i] + 1.5 * x1[i] * x2[i] + 0.3 * random.nextGaussian();
                if (score[i] > score[best]) best = i;
            }
            final List<ScreenRow> rows = new java.util.ArrayList<>();
            for (int i = 0; i < 4; i++) rows.add(new ScreenRow("g" + g, "g" + g + ":" + i, i, null, i == best ? 1 : 0, Double.NaN, 1, new double[]{x1[i], x1[i], x2[i]}));
            units.add(rows);
        }
        final WindowQuantiles quantiles = new WindowQuantiles(spec.sketchColumns());
        for (final List<ScreenRow> rows : units) for (final ScreenRow r : rows) quantiles.update(r.x);
        final GroupScorer groups = new GroupScorer(spec).withWindowQuantiles(quantiles);
        final ConditioningScorer scorer = new ConditioningScorer(spec).withWindowQuantiles(quantiles);
        final com.mercari.solution.util.pipeline.glm.VectorAccumulator moments = new com.mercari.solution.util.pipeline.glm.VectorAccumulator();
        for (final List<ScreenRow> rows : units) for (final ScreenRow r : rows) moments.add(scorer.moments(r));
        com.mercari.solution.util.pipeline.glm.FitState state = com.mercari.solution.util.pipeline.glm.FitState.initial(scorer.k);
        for (int it = 0; it < 8 && !state.converged; it++) {
            final com.mercari.solution.util.pipeline.glm.VectorAccumulator eval = new com.mercari.solution.util.pipeline.glm.VectorAccumulator();
            for (final List<ScreenRow> rows : units) eval.add(scorer.evaluate(groups.prepare(rows, rows.get(0).getGroup()), state.proposal, moments.getValues()));
            state.advance(eval.getValues(), spec.conditioningL2, spec.conditioningTol);
        }
        Assertions.assertTrue(state.hasBest);
        final Map<Integer, PartialAccumulator> partials = new HashMap<>();
        final Map<Integer, ScoreAccumulator> marginal = new HashMap<>();
        for (final List<ScreenRow> rows : units) {
            final GroupScorer.Unit unit = groups.prepare(rows, rows.get(0).getGroup());
            scorer.partial(unit, groups.columns(unit), state.bestTheta, moments.getValues(), partials);
            groups.score(rows, rows.get(0).getGroup(), marginal);
        }
        final ScreenReport.Result result = ScreenReport.build(spec, marginal, partials, state, null);
        Map<String, Object> pair = null;
        for (final Map<String, Object> r : result.records()) if ("x*x2".equals(r.get("candidate"))) pair = r;
        Assertions.assertNotNull(pair, result.records().toString());
        Assertions.assertTrue((Double) pair.get("partial_z") > 3, "partial z of the pair: " + pair.get("partial_z"));
        Assertions.assertEquals(List.of("x*x2"), result.summary().get("passedPairs"));
    }

    @Test
    public void testPairSketchesLeaveGroupedTransformsWithinTheGroup() throws Exception {
        // a grouped run with the default transforms (raw, rank, absdev) and a pair: the pre-pass sketches the pair's
        // members only (their grid edges), and rank / absdev stay within the group — the window's sketches are the
        // independent rows' reference, never a grouped run's
        final ScreenSpec spec = spec("{family: groupedMultinomial, group: g, label: y, candidates: [x], conditioning: {fields: [x, x2]}, pairs: {fields: [[x, x2]], placebo: 0}}");
        Assertions.assertTrue(spec.transforms.contains("rank") && spec.transforms.contains("absdev"));
        Assertions.assertTrue(spec.needsWindowQuantiles());
        Assertions.assertFalse(spec.needsCandidateSketches());
        Assertions.assertArrayEquals(new int[]{1, 2}, spec.sketchedColumns());
        Assertions.assertEquals(3, spec.sketchColumns());
        final WindowQuantiles q = new WindowQuantiles(spec.sketchColumns());
        for (int i = 0; i < 50; i++) q.update(new double[]{i, i, -i}, spec.sketchedColumns());
        Assertions.assertEquals(0, q.count(0));
        Assertions.assertEquals(50, q.count(1));
        final double[] v = {3, 1, 2};
        Assertions.assertArrayEquals(GroupScorer.transform("rank", v), GroupScorer.transform(spec, q, 0, "rank", v), 0d);
        Assertions.assertArrayEquals(GroupScorer.transform("absdev", v), GroupScorer.transform(spec, q, 0, "absdev", v), 0d);
        // independent rows asking for a pair shape only: no rank / absdev note, no candidate sketch
        final ScreenSpec independent = spec("{family: binomial, label: y, candidates: [x], conditioning: {fields: [x, x2]}, pairs: {fields: [[x, x2]], placebo: 0}}");
        Assertions.assertTrue(independent.needsWindowQuantiles() && !independent.needsCandidateSketches());
        Assertions.assertTrue(independent.notes.stream().noneMatch(n -> n.contains("rank / absdev")), independent.notes.toString());
    }

    @Test
    public void testPairShapeOfRowFamilies() throws Exception {
        // independent gaussian rows with a product interaction: the shape's statistics are label-scale free (the sums
        // are divided by the residual variance at the fit) and its share is a share of the profiled grid χ², below 1
        final ScreenSpec spec = spec("{family: gaussian, label: y, candidates: [x], transforms: [raw], placebo: {noise: 2, seed: 7}, "
                + "conditioning: {fields: [x, x2], l2: 1.0e-4, maxIter: 4}, pairs: {fields: [[x, x2]], placebo: 2}}");
        final java.util.Random random = new java.util.Random(5);
        final List<ScreenRow> rows = new java.util.ArrayList<>(), scaled = new java.util.ArrayList<>();
        for (int i = 0; i < 300; i++) {
            final double x = random.nextGaussian(), x2 = random.nextGaussian();
            final double y = x + x2 + 1.5 * x * x2 + random.nextGaussian();
            rows.add(new ScreenRow("r" + i, "r" + i, i, null, y, Double.NaN, 1, new double[]{x, x, x2}));
            scaled.add(new ScreenRow("r" + i, "r" + i, i, null, 10 * y, Double.NaN, 1, new double[]{x, x, x2}));
        }
        final WindowQuantiles quantiles = new WindowQuantiles(spec.sketchColumns());
        for (final ScreenRow r : rows) quantiles.update(r.x, spec.sketchedColumns());
        final Map<String, Object> shape = rowFamilyShape(spec, rows, quantiles);
        final Map<String, Object> shapeScaled = rowFamilyShape(spec, scaled, quantiles);
        Assertions.assertNotNull(shape);
        Assertions.assertEquals("x*x2", shape.get("candidate"));
        Assertions.assertEquals((Double) shape.get("chi2"), (Double) shapeScaled.get("chi2"), 1e-6 * (Double) shape.get("chi2"));
        Assertions.assertEquals((Double) shape.get("share"), (Double) shapeScaled.get("share"), 1e-9);
        Assertions.assertTrue((Double) shape.get("share") > 0 && (Double) shape.get("share") < 1, shape.toString());
    }

    /** The interaction record of a row-family pair run over independent rows (one row per unit); null when none. */
    private static Map<String, Object> rowFamilyShape(final ScreenSpec spec, final List<ScreenRow> rows, final WindowQuantiles quantiles) {
        final GroupScorer groups = new GroupScorer(spec).withWindowQuantiles(quantiles);
        final ConditioningScorer scorer = new ConditioningScorer(spec).withWindowQuantiles(quantiles);
        final com.mercari.solution.util.pipeline.glm.VectorAccumulator moments = new com.mercari.solution.util.pipeline.glm.VectorAccumulator();
        for (final ScreenRow r : rows) moments.add(scorer.moments(r));
        com.mercari.solution.util.pipeline.glm.FitState state = com.mercari.solution.util.pipeline.glm.FitState.initial(scorer.k, scorer.initialTheta(moments.getValues()));
        for (int it = 0; it < spec.conditioningMaxIter && !state.converged; it++) {
            final com.mercari.solution.util.pipeline.glm.VectorAccumulator eval = new com.mercari.solution.util.pipeline.glm.VectorAccumulator();
            for (final ScreenRow r : rows) eval.add(scorer.evaluate(groups.prepare(List.of(r), r.getIdentity()), state.proposal, moments.getValues()));
            state.advance(eval.getValues(), spec.conditioningL2, spec.conditioningTol);
        }
        Assertions.assertTrue(state.hasBest);
        final Map<Integer, PartialAccumulator> partials = new HashMap<>();
        final Map<Integer, ScoreAccumulator> marginal = new HashMap<>();
        for (final ScreenRow r : rows) {
            final GroupScorer.Unit unit = groups.prepare(List.of(r), r.getIdentity());
            scorer.partial(unit, groups.columns(unit), state.bestTheta, moments.getValues(), partials);
            groups.score(List.of(r), r.getIdentity(), marginal);
        }
        final ScreenReport.Result result = ScreenReport.build(spec, marginal, partials, state, new ScreenReport.Bins(groups::binRepresentatives, groups::binEdges, groups::gridEdges));
        return result.suggestions().stream().filter(s -> "interaction".equals(s.get("kind"))).findFirst().orElse(null);
    }


    @Test
    public void testPairPlacebosAreDistinct() {
        // b is the first member of two pairs: its placebo pairs take different noise columns, so no record name repeats
        final ScreenSpec spec = spec("{family: groupedMultinomial, group: g, label: y, candidates: [x], placebo: {noise: 4}, "
                + "conditioning: {fields: [b, x, x2]}, pairs: {among: [b, 'x*'], placebo: 2}}");
        Assertions.assertEquals(3, spec.pairs.size());
        Assertions.assertEquals(3 + 6, spec.pairCount());
        final java.util.Set<String> names = new java.util.HashSet<>();
        for (int q = 0; q < spec.pairCount(); q++) names.add(spec.pairName(q));
        Assertions.assertEquals(spec.pairCount(), names.size(), names.toString());
        // saturated: a member already paired with every noise column brings no further placebo (never a repeat)
        final ScreenSpec saturated = spec("{family: groupedMultinomial, group: g, label: y, candidates: [x], placebo: {noise: 2}, "
                + "conditioning: {fields: [b, x, x2]}, pairs: {among: [b, 'x*'], placebo: 2}}");
        final java.util.Set<String> saturatedNames = new java.util.HashSet<>();
        for (int q = 0; q < saturated.pairCount(); q++) saturatedNames.add(saturated.pairName(q));
        Assertions.assertEquals(saturated.pairCount(), saturatedNames.size(), saturatedNames.toString());
        Assertions.assertEquals(3 + 4, saturated.pairCount());
    }

    @Test
    public void testPairColumnMissingMember() {
        // a row missing a member is missing for the product (the recipe a * b is null there), not the fill's product
        final ScreenSpec spec = spec("{family: groupedMultinomial, group: g, label: y, candidates: [x], transforms: [raw], placebo: {noise: 1}, "
                + "conditioning: {fields: [x, x2]}, pairs: {fields: [[x, x2]], placebo: 1}}");
        final List<ScreenRow> rows = List.of(
                new ScreenRow("g0", "g0:0", 0, null, 1, Double.NaN, 1, new double[]{1.0, 1.0, 2.0}),
                new ScreenRow("g0", "g0:1", 1, null, 0, Double.NaN, 1, new double[]{-1.0, -1.0, Double.NaN}),
                new ScreenRow("g0", "g0:2", 2, null, 0, Double.NaN, 1, new double[]{Double.NaN, Double.NaN, 0.5}),
                new ScreenRow("g0", "g0:3", 3, null, 0, Double.NaN, 1, new double[]{0.5, 0.5, -1.0}));
        final GroupScorer groups = new GroupScorer(spec);
        final ConditioningScorer scorer = new ConditioningScorer(spec);
        final com.mercari.solution.util.pipeline.glm.VectorAccumulator moments = new com.mercari.solution.util.pipeline.glm.VectorAccumulator();
        for (final ScreenRow r : rows) moments.add(scorer.moments(r));
        final GroupScorer.Unit unit = groups.prepare(rows, "g0");
        final double[][] f = scorer.design(unit, moments.getValues());
        final double[][] cols = groups.columns(unit);
        final double[] product = scorer.pairColumn(0, unit, f, cols);
        Assertions.assertEquals(f[0][0] * f[0][1], product[0], 1e-12);
        Assertions.assertTrue(Double.isNaN(product[1]), "x2 missing");
        Assertions.assertTrue(Double.isNaN(product[2]), "x missing");
        Assertions.assertEquals(f[3][0] * f[3][1], product[3], 1e-12);
        // the placebo pair (x × noise) is missing only where x is
        final double[] placebo = scorer.pairColumn(1, unit, f, cols);
        Assertions.assertFalse(Double.isNaN(placebo[1]));
        Assertions.assertTrue(Double.isNaN(placebo[2]));
    }

    @Test
    public void testContrastsIgnoreBinsWithoutInformation() {
        // a block whose third bin holds a rounding residue (the confirmation half of a bin the discovery half held
        // entirely): a contrast isolating that bin would divide S² ≈ 0.01 by H ≈ 1e-17; the guard leaves the bin out
        final double[] s = {2, -1, 0.1, -1};
        final double[] h = {10, 12, 1e-17, 9};
        final double[] n = {20, 24, 0, 18};
        final double[][] hFull = new double[4][4];
        for (int b = 0; b < 4; b++) hFull[b][b] = h[b];
        final ScreenReport.Block block = new ScreenReport.Block(ScreenReport.Stats.degenerate(62), 2, s, h, n, hFull);
        Assertions.assertArrayEquals(new boolean[]{true, true, false, true}, ScreenReport.informative(block, 4));
        final double isolating = ScreenReport.contrastChi2(block, new double[]{0, 0, 1, 0}, 4);
        Assertions.assertEquals(0d, isolating, 0d);
        // a monotone contrast over the bins with information is unchanged by the residue bin's value
        final double up = ScreenReport.contrastChi2(block, new double[]{0, 1, 2, 3}, 4);
        final double upResidueFlipped = ScreenReport.contrastChi2(block, new double[]{0, 1, 1e9, 3}, 4);
        Assertions.assertTrue(up > 0);
        Assertions.assertEquals(up, upResidueFlipped, 1e-9 * up);
        // a lone row at p̂ ≈ 0 (H ≈ 1e-12 of the block, |S| ≈ 1) is no information either; a small but real bin is
        final double[] h2 = {10, 12, 1e-11, 9};
        final double[] n2 = {20, 24, 1, 18};
        final double[][] hFull2 = new double[4][4];
        for (int b = 0; b < 4; b++) hFull2[b][b] = h2[b];
        Assertions.assertFalse(ScreenReport.informative(new ScreenReport.Block(ScreenReport.Stats.degenerate(63), 2, new double[]{2, -1, 1, -1}, h2, n2, hFull2), 4)[2]);
        final double[] h3 = {10, 12, 1e-3, 9};
        final double[][] hFull3 = new double[4][4];
        for (int b = 0; b < 4; b++) hFull3[b][b] = h3[b];
        Assertions.assertTrue(ScreenReport.informative(new ScreenReport.Block(ScreenReport.Stats.degenerate(63), 2, s, h3, n2, hFull3), 4)[2]);
    }

    @Test
    public void testBinRepresentativesAreTheBinMedians() {
        final ScreenSpec spec = spec("{family: binomial, label: y, candidates: [x], transforms: [binned], bins: {k: 4, edges: value}, placebo: {noise: 1, seed: 1}}");
        final WindowQuantiles q = new WindowQuantiles(spec.sketchColumns(), 0);
        // 0..99 with one outlier at 10000: the top bin's midpoint of its edges would sit near 5000, its median near 87
        for (int i = 0; i < 100; i++) q.update(new double[]{i == 99 ? 10000 : i}, spec.sketchedColumns());
        final GroupScorer scorer = new GroupScorer(spec).withWindowQuantiles(q);
        final double[] x = scorer.binRepresentatives(0);
        Assertions.assertEquals(4, x.length);
        Assertions.assertEquals(12, x[0], 2);
        Assertions.assertEquals(37, x[1], 2);
        Assertions.assertEquals(62, x[2], 2);
        Assertions.assertEquals(87, x[3], 2);
        // tied values collapse the edges: 0 in 90 rows and 1..10 — edges (0, 0, 0), the top bin holds the positives and
        // its median is 5 (a quantile at 0.875 would be 0, the bottom bin's value); every representative lies in its bin
        final WindowQuantiles tied = new WindowQuantiles(spec.sketchColumns(), 0);
        for (int i = 0; i < 100; i++) tied.update(new double[]{i < 90 ? 0 : i - 89}, spec.sketchedColumns());
        final GroupScorer tiedScorer = new GroupScorer(spec).withWindowQuantiles(tied);
        final double[] tiedEdges = tiedScorer.binEdges(0);
        Assertions.assertArrayEquals(new double[]{0, 0, 0}, tiedEdges, 0d);
        final double[] xt = tiedScorer.binRepresentatives(0);
        Assertions.assertEquals(0d, xt[0], 0d);
        Assertions.assertEquals(5d, xt[3], 0d);
        // a noise placebo: the normal quantiles at the same ranks
        final double[] noise = scorer.binRepresentatives(1);
        Assertions.assertEquals(StatMath.inverseNormal(0.125), noise[0], 1e-12);
        Assertions.assertEquals(StatMath.inverseNormal(0.875), noise[3], 1e-12);
        // position bins: the positions' centres
        final ScreenSpec rank = spec("{family: groupedMultinomial, group: g, label: y, time: t, candidates: [x], transforms: [binned], bins: {k: 4, edges: rank}, placebo: {noise: 0}}");
        Assertions.assertArrayEquals(new double[]{0.125, 0.375, 0.625, 0.875}, new GroupScorer(rank).binRepresentatives(0), 1e-12);
    }

    @Test
    public void testGroupedModifierLevelIsTheUnitsMostFrequentValue() {
        // a grouped unit's modifier level is its rows' most frequent value (ties to the smallest), whatever the rows'
        // order; the summary counts the units whose rows disagree
        final Schema schema = Schema.builder()
                .withField("g", Schema.FieldType.STRING)
                .withField("seg", Schema.FieldType.STRING)
                .withField("y", Schema.FieldType.INT64)
                .withField("t", Schema.FieldType.TIMESTAMP)
                .withField("x", Schema.FieldType.FLOAT64)
                .build();
        final ScreenSpec spec = ScreenSpec.parse(JsonParser.parseString("{family: groupedMultinomial, group: g, label: y, time: t, candidates: [x], transforms: [raw], placebo: {noise: 0}, heterogeneity: {field: seg}}").getAsJsonObject()).resolve(schema, null);
        Assertions.assertTrue(spec.notes.stream().anyMatch(n -> n.contains("most frequent")), spec.notes.toString());
        final GroupScorer scorer = new GroupScorer(spec);
        final Map<Integer, ScoreAccumulator> acc = new HashMap<>();
        final java.util.function.BiFunction<String, String[], List<ScreenRow>> unit = (g, levels) -> {
            final List<ScreenRow> rows = new ArrayList<>();
            for (int i = 0; i < levels.length; i++) rows.add(new ScreenRow(g, g + ":" + i, 1, null, levels[i], i == 0 ? 1 : 0, Double.NaN, 1, new double[]{i}));
            return rows;
        };
        // B, A, A -> A (majority, not the first row); B, A -> A (a tie, the smallest); C, C -> C (constant)
        final List<ScreenRow> mixed1 = unit.apply("u1", new String[]{"B", "A", "A"});
        final List<ScreenRow> mixed2 = unit.apply("u2", new String[]{"B", "A"});
        final List<ScreenRow> constant = unit.apply("u3", new String[]{"C", "C"});
        Assertions.assertEquals("A", scorer.prepare(mixed1, "u1").level());
        Assertions.assertEquals("A", scorer.prepare(mixed2, "u2").level());
        Assertions.assertEquals("C", scorer.prepare(constant, "u3").level());
        Assertions.assertTrue(scorer.prepare(mixed1, "u1").mixedLevels());
        Assertions.assertFalse(scorer.prepare(constant, "u3").mixedLevels());
        // the rows' order does not decide: prepare sorts by (time, identity), so the levels are laid on the
        // identities in the other order (the sorted first row now carries A, then B) and the level is the same
        Assertions.assertEquals("A", scorer.prepare(unit.apply("u1", new String[]{"A", "A", "B"}), "u1").level());
        Assertions.assertEquals("A", scorer.prepare(unit.apply("u2", new String[]{"A", "B"}), "u2").level());
        Assertions.assertEquals("B", scorer.prepare(unit.apply("u4", new String[]{"A", "B", "B"}), "u4").level());
        scorer.score(mixed1, "u1", acc);
        scorer.score(mixed2, "u2", acc);
        scorer.score(constant, "u3", acc);
        final double[] book = acc.get(ScoreAccumulator.BOOKKEEPING_KEY).getTotal();
        Assertions.assertEquals(3, book[ScoreAccumulator.UNITS_SCORED]);
        Assertions.assertEquals(2, book[ScoreAccumulator.UNITS_HET_MIXED]);
        // the level slices: u1 and u2 under A, u3 under C
        final ScoreAccumulator x = acc.get(spec.key(0, 0));
        Assertions.assertEquals(java.util.Set.of(ScoreAccumulator.LEVEL_PREFIX + "A", ScoreAccumulator.LEVEL_PREFIX + "C"), x.getPeriods().keySet());
        final ScreenReport.Result result = ScreenReport.build(spec, acc);
        Assertions.assertEquals(2L, result.summary().get("nHetMixedUnits"));
        Assertions.assertTrue(((List<?>) result.summary().get("notes")).stream().anyMatch(n -> ((String) n).startsWith("heterogeneity by seg: 2 of 3 units")), result.summary().get("notes").toString());
        // a row family reads the modifier per row: the count is null
        final ScreenSpec rows = spec("{family: binomial, label: y, candidates: [x], transforms: [raw], heterogeneity: {field: g}, placebo: {noise: 0}}");
        final Map<Integer, ScoreAccumulator> racc = new HashMap<>();
        final GroupScorer rs = new GroupScorer(rows);
        for (int i = 0; i < 4; i++) {
            final ScreenRow r = new ScreenRow("r" + i, "r" + i, i, null, i % 2 == 0 ? "A" : "B", i % 2, Double.NaN, 1, new double[]{i});
            rs.score(List.of(r), r.getIdentity(), racc);
        }
        Assertions.assertNull(ScreenReport.build(rows, racc).summary().get("nHetMixedUnits"));
    }

    @Test
    public void testLeakFlagReadsABlockOnTheSameTail() {
        // a strong effect: the raw column's |z| clears leakZ, and the binned block (no z) is flagged on the same
        // tail — its p-value below P(|Z| > leakZ); a placebo column is flagged by neither
        final java.util.Random random = new java.util.Random(23);
        final List<ScreenRow> rows = new ArrayList<>();
        for (int i = 0; i < 400; i++) {
            final double x = random.nextGaussian();
            final double y = random.nextDouble() < 1 / (1 + Math.exp(-2.5 * x)) ? 1 : 0;
            rows.add(new ScreenRow("r" + i, "r" + i, i, null, y, Double.NaN, 1, new double[]{x}));
        }
        for (final double leakZ : new double[]{4, 40}) {
            final ScreenSpec spec = spec("{family: binomial, label: y, candidates: [x], transforms: [raw, binned], bins: {k: 4, edges: value}, placebo: {noise: 2, seed: 5}, flags: {leakZ: " + leakZ + "}}");
            final WindowQuantiles q = new WindowQuantiles(spec.sketchColumns(), 0);
            for (final ScreenRow r : rows) q.update(r.x, spec.sketchedColumns());
            final GroupScorer scorer = new GroupScorer(spec).withWindowQuantiles(q);
            final Map<Integer, ScoreAccumulator> acc = new HashMap<>();
            for (final ScreenRow r : rows) scorer.score(List.of(r), r.getIdentity(), acc);
            final ScreenReport.Result result = ScreenReport.build(spec, acc);
            final Map<String, Map<String, Object>> byKey = new HashMap<>();
            for (final Map<String, Object> r : result.records()) byKey.put(r.get("candidate") + ":" + r.get("transform"), r);
            final Map<String, Object> raw = byKey.get("x:raw"), binned = byKey.get("x:binned");
            final boolean expected = leakZ < 10;
            Assertions.assertEquals(Math.abs((Double) raw.get("z")) > leakZ, raw.get("leakSuspect"), raw.toString());
            Assertions.assertEquals(expected, raw.get("leakSuspect"), raw.toString());
            Assertions.assertNull(binned.get("z"));
            Assertions.assertEquals((Double) binned.get("pValue") < StatMath.chiSquare1UpperTail(leakZ * leakZ), binned.get("leakSuspect"), binned.toString());
            Assertions.assertEquals(expected, binned.get("leakSuspect"), binned.toString());
            Assertions.assertEquals(Boolean.FALSE, byKey.get("__noise_0:binned").get("leakSuspect"));
            Assertions.assertEquals(expected ? 2L : 0L, result.summary().get("nLeakSuspect"));
        }
    }

    @Test
    public void testNullLevelIsAlwaysItsOwnAndExcludeReachesTheCategoricals() {
        // the dictionary: (null) is named whatever its rank, outside the maxLevels count; the rest fold beyond it
        final WindowQuantiles q = new WindowQuantiles(0, 1);
        final String[] levels = {"A", "A", "A", "A", "A", "B", "B", "B", "B", "C", "C", "C", ScreenSpec.LEVEL_NULL};
        for (final String l : levels) q.updateLevels(new String[]{l});
        final WindowQuantiles.Levels two = q.levels(0, 2);
        Assertions.assertEquals(List.of("A", "B", ScreenSpec.LEVEL_NULL, ScreenSpec.LEVEL_OTHER), two.names());
        Assertions.assertTrue(two.folded());
        Assertions.assertEquals(2, two.indexOf(ScreenSpec.LEVEL_NULL));
        Assertions.assertEquals(3, two.indexOf("C"));
        Assertions.assertEquals(1d / 13, two.frequency()[2], 1e-12);
        Assertions.assertEquals(List.of("A", "B", "C", ScreenSpec.LEVEL_NULL), q.levels(0, 32).names());

        // candidates.exclude reaches the categoricals: a lineage selector drops the outcome-kind field from both
        // lists, and the resolution notes an outcome-kind pass-through field left among the candidates
        final Schema schema = Schema.builder()
                .withField("y", Schema.FieldType.INT64)
                .withField("x", Schema.FieldType.FLOAT64)
                .withField("z", Schema.FieldType.FLOAT64)
                .withField("cat", Schema.FieldType.STRING)
                .withField("seg", Schema.FieldType.STRING)
                .build();
        final FeatureLineage lineage = new FeatureLineage();
        lineage.columns.put("x", new FeatureLineage.Entry("input", null, java.util.Set.of("outcome"), null, "outcome"));
        lineage.columns.put("z", new FeatureLineage.Entry("input", null, java.util.Set.of("market"), null, "market"));
        lineage.columns.put("cat", new FeatureLineage.Entry("input", null, java.util.Set.of("outcome"), null, "outcome"));
        lineage.columns.put("seg", new FeatureLineage.Entry("input", null, java.util.Set.of("market"), null, "market"));
        final ScreenSpec kept = ScreenSpec.parse(JsonParser.parseString("{family: binomial, label: y, candidates: ['*'], categorical: ['*']}").getAsJsonObject()).resolve(schema, lineage);
        Assertions.assertEquals(List.of("x", "z"), kept.candidates);
        Assertions.assertEquals(List.of("cat", "seg"), kept.categoricals);
        Assertions.assertTrue(kept.notes.stream().anyMatch(n -> n.startsWith("outcome-kind input candidates [x, cat]")), kept.notes.toString());
        final ScreenSpec excluded = ScreenSpec.parse(JsonParser.parseString("{family: binomial, label: y, candidates: {include: ['*'], exclude: ['kind:outcome']}, categorical: ['*']}").getAsJsonObject()).resolve(schema, lineage);
        Assertions.assertEquals(List.of("z"), excluded.candidates);
        Assertions.assertEquals(List.of("seg"), excluded.categoricals);
        Assertions.assertTrue(excluded.notes.stream().anyMatch(n -> n.startsWith("categorical excluded by lineage: [cat (kind:outcome)]")), excluded.notes.toString());
        Assertions.assertTrue(excluded.notes.stream().noneMatch(n -> n.startsWith("outcome-kind input candidates")), excluded.notes.toString());
        // a name glob excludes a categorical too; excluding every categorical is an error that says so
        Assertions.assertEquals(List.of("seg"), ScreenSpec.parse(JsonParser.parseString("{family: binomial, label: y, candidates: {include: ['*'], exclude: ['c*']}, categorical: ['*']}").getAsJsonObject()).resolve(schema, lineage).categoricals);
        final IllegalArgumentException none = Assertions.assertThrows(IllegalArgumentException.class,
                () -> ScreenSpec.parse(JsonParser.parseString("{family: binomial, label: y, candidates: {include: ['*'], exclude: ['cat', 'seg']}, categorical: ['*']}").getAsJsonObject()).resolve(schema, lineage));
        Assertions.assertTrue(none.getMessage().contains("candidates.exclude applies to the categoricals too"), none.getMessage());
    }

    @Test
    public void testOnehotNamesStayUniqueWhenLevelsShareASanitizedName() {
        // "a b" and "a-b" both become a_b in the indicator name: the later level (by count then name) takes its position
        final ScreenSpec spec = spec("{family: binomial, label: y, candidates: [x], transforms: [raw], placebo: {noise: 0}, categorical: {include: [g], placebo: 5}}");
        final String[] levels = {"a-b", "a b", "C", "D"};
        final double[] rate = {0.05, 0.05, 0.9, 0.9};
        final java.util.Random random = new java.util.Random(29);
        final List<ScreenRow> rows = new ArrayList<>();
        final WindowQuantiles q = new WindowQuantiles(spec.sketchColumns(), 1);
        for (int i = 0; i < 400; i++) {
            final int l = i % 4;
            final double y = random.nextDouble() < rate[l] ? 1 : 0;
            final ScreenRow r = new ScreenRow("r" + i, "r" + i, i, null, null, y, Double.NaN, 1, new double[]{random.nextGaussian()}, new String[]{levels[l]});
            rows.add(r);
            q.update(r.x);
            q.updateLevels(r.cat);
        }
        final GroupScorer scorer = new GroupScorer(spec).withWindowQuantiles(q);
        final Map<Integer, ScoreAccumulator> acc = new HashMap<>();
        for (final ScreenRow r : rows) scorer.score(List.of(r), r.getIdentity(), acc);
        final ScreenReport.Result result = ScreenReport.build(spec, acc, null, null,
                new ScreenReport.Bins(scorer::binRepresentatives, scorer::binEdges, scorer::gridEdges, scorer::columnMin, scorer::categoricalLevels));
        final Map<String, String> fragments = new HashMap<>();
        for (final Map<String, Object> s : result.suggestions()) if ("onehot".equals(s.get("kind"))) fragments.put((String) s.get("name"), (String) s.get("fragment"));
        Assertions.assertTrue(fragments.containsKey("a b") && fragments.containsKey("a-b"), fragments.toString());
        Assertions.assertTrue(fragments.get("a b").startsWith("{name: g_is_a_b, "), fragments.toString());
        Assertions.assertTrue(fragments.get("a-b").startsWith("{name: g_is_a_b_3, "), fragments.toString());
    }

    @Test
    public void testJointFillsAMissingValueAsTheMarginalTestDoes() throws Exception {
        // grouped: a unit with a missing value in a joint column stays in the joint sums, the column centred by the
        // unit's p-weighted mean over its observed rows — so the joint's S for that column equals the marginal raw S
        final ScreenSpec grouped = spec("{family: groupedMultinomial, group: g, label: y, time: t, candidates: [b, x], transforms: [raw], placebo: {noise: 0}, joint: {noise: 0, select: 2}}");
        Assertions.assertFalse(grouped.needsJointFill());
        final GroupScorer gs = new GroupScorer(grouped);
        final Map<Integer, ScoreAccumulator> gacc = new HashMap<>();
        Assertions.assertEquals(Baselines.Skip.NONE, gs.score(List.of(row("u", 1, 1, Double.NaN, 3, 1), row("u", 1, 0, Double.NaN, Double.NaN, 2), row("u", 1, 0, Double.NaN, 2, 5)), "u", gacc));
        Assertions.assertEquals(Baselines.Skip.NONE, gs.score(List.of(row("v", 2, 0, Double.NaN, 1, 1), row("v", 2, 1, Double.NaN, 4, 0)), "v", gacc));
        final GroupScorer.JointLayout gl = GroupScorer.JointLayout.of(2);
        final double[] ge = gacc.get(ScoreAccumulator.JOINT_KEY).getExtra();
        Assertions.assertEquals(2d, ge[gl.used()]);
        Assertions.assertEquals(1d, ge[gl.filled()]);
        Assertions.assertEquals(0d, ge[gl.dropped()]);
        Assertions.assertEquals(gacc.get(grouped.key(0, 0)).getTotal()[ScoreAccumulator.S], ge[gl.s()], 1e-12);
        Assertions.assertEquals(gacc.get(grouped.key(1, 0)).getTotal()[ScoreAccumulator.S], ge[gl.s() + 1], 1e-12);
        final ScreenReport.Result gr = ScreenReport.build(grouped, gacc);
        Assertions.assertEquals(2L, gr.summary().get("nJointUnits"));
        Assertions.assertEquals(1L, gr.summary().get("nJointFilled"));
        Assertions.assertEquals(0L, gr.summary().get("nJointDropped"));
        Assertions.assertTrue(((List<?>) gr.summary().get("notes")).stream().anyMatch(n -> ((String) n).startsWith("joint: 1 of 2 units (50%)")), gr.summary().get("notes").toString());

        // independent rows: with the window means (the sketch pre-pass feeds the joint columns) a missing value is the
        // mean — 0 after the shift — and the row stays; without them the row is left out and counted
        final ScreenSpec rows = spec("{family: binomial, label: y, candidates: [b, x], transforms: [raw], placebo: {noise: 0}, joint: {noise: 0, select: 2}}");
        Assertions.assertTrue(rows.needsJointFill());
        Assertions.assertArrayEquals(new int[]{0, 1}, rows.sketchedColumns());
        final java.util.Random random = new java.util.Random(11);
        final List<ScreenRow> data = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            final double a = random.nextGaussian(), b = random.nextGaussian();
            final double y = random.nextDouble() < 1 / (1 + Math.exp(-(a + 0.5 * b))) ? 1 : 0;
            data.add(new ScreenRow("r" + i, "r" + i, i, null, y, Double.NaN, 1, new double[]{i % 10 == 0 ? Double.NaN : a, b}));
        }
        final WindowQuantiles q = new WindowQuantiles(rows.sketchColumns(), 0);
        for (final ScreenRow r : data) q.update(r.x, rows.sketchedColumns());
        Assertions.assertEquals(180L, q.count(0));
        double sumA = 0;
        for (final ScreenRow r : data) if (Double.isFinite(r.x[0])) sumA += r.x[0];
        Assertions.assertEquals(sumA / 180, q.mean(0), 1e-12);
        final GroupScorer filled = new GroupScorer(rows).withWindowQuantiles(q);
        final Map<Integer, ScoreAccumulator> facc = new HashMap<>();
        for (final ScreenRow r : data) filled.score(List.of(r), r.getIdentity(), facc);
        final GroupScorer.JointLayout rl = GroupScorer.JointLayout.of(2);
        final double[] fe = facc.get(ScoreAccumulator.JOINT_KEY).getExtra();
        Assertions.assertEquals(200d, fe[rl.used()]);
        Assertions.assertEquals(20d, fe[rl.filled()]);
        Assertions.assertEquals(0d, fe[rl.dropped()]);
        // the shifted Σ w v x of the filled column is (about) 0: the observed values centre on the mean, the fills are 0
        Assertions.assertEquals(0d, fe[1], 1e-9);
        final ScreenReport.Result fr = ScreenReport.build(rows, facc);
        Assertions.assertEquals(200L, fr.summary().get("nJointUnits"));
        Assertions.assertEquals(20L, fr.summary().get("nJointFilled"));
        Assertions.assertFalse(((List<?>) fr.summary().get("notes")).stream().anyMatch(n -> ((String) n).startsWith("joint:")), fr.summary().get("notes").toString());
        Assertions.assertTrue(fr.suggestions().stream().anyMatch(s -> "select".equals(s.get("kind"))), fr.suggestions().toString());
        final GroupScorer dropping = new GroupScorer(rows);
        final Map<Integer, ScoreAccumulator> dacc = new HashMap<>();
        for (final ScreenRow r : data) dropping.score(List.of(r), r.getIdentity(), dacc);
        final double[] de = dacc.get(ScoreAccumulator.JOINT_KEY).getExtra();
        Assertions.assertEquals(180d, de[rl.used()]);
        Assertions.assertEquals(0d, de[rl.filled()]);
        Assertions.assertEquals(20d, de[rl.dropped()]);
        final ScreenReport.Result dr = ScreenReport.build(rows, dacc);
        Assertions.assertEquals(20L, dr.summary().get("nJointDropped"));
        Assertions.assertTrue(((List<?>) dr.summary().get("notes")).stream().anyMatch(n -> ((String) n).startsWith("joint: 20 rows with a missing joint value were left out")), dr.summary().get("notes").toString());
        // the coder carries the sketch's running sum
        final java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
        WindowQuantiles.CODER.encode(q, bytes);
        Assertions.assertEquals(q.mean(0), WindowQuantiles.CODER.decode(new java.io.ByteArrayInputStream(bytes.toByteArray())).mean(0), 0d);

        // a column constant over its observed values has that value as its mean exactly (0.1 fed ten times sums to
        // 0.9999999999999999): its shifted values are 0 like the fills, not a rounding residue times the observed
        // indicator that the report would score as the missingness
        final WindowQuantiles constant = new WindowQuantiles(1, 0);
        for (int i = 0; i < 10; i++) constant.update(new double[]{0.1});
        Assertions.assertEquals(0.1, constant.mean(0), 0d);

        // a joint column with no value in the window: every row is a fill (the column is degenerate) instead of every
        // row leaving the joint sums
        final List<ScreenRow> empty = new ArrayList<>();
        for (final ScreenRow r : data) empty.add(new ScreenRow(r.group, r.identity, r.time, null, r.label, Double.NaN, 1, new double[]{Double.NaN, r.x[1]}));
        final WindowQuantiles qe = new WindowQuantiles(rows.sketchColumns(), 0);
        for (final ScreenRow r : empty) qe.update(r.x, rows.sketchedColumns());
        final GroupScorer emptyScorer = new GroupScorer(rows).withWindowQuantiles(qe);
        final Map<Integer, ScoreAccumulator> eacc = new HashMap<>();
        for (final ScreenRow r : empty) emptyScorer.score(List.of(r), r.getIdentity(), eacc);
        final double[] ee = eacc.get(ScoreAccumulator.JOINT_KEY).getExtra();
        Assertions.assertEquals(200d, ee[rl.used()]);
        Assertions.assertEquals(200d, ee[rl.filled()]);
        Assertions.assertEquals(0d, ee[rl.dropped()]);
        final ScreenReport.Result er = ScreenReport.build(rows, eacc);
        Assertions.assertEquals(0L, er.summary().get("nJointDropped"));
    }

    @Test
    public void testSuggestionsAndJointReadThePartialSumsUnderConditioning() throws Exception {
        // independent binomial rows: x carries the main effect, b an independent one, and the conditioning column x2
        // is a re-encoding of x. Marginally x is the strongest candidate; on the partial basis x is explained by F
        // (r2_F ≈ 1), so the forward selection takes b first and x's recipes carry a small confirmation gain
        final ScreenSpec spec = spec("{family: binomial, label: y, candidates: [b, x], transforms: [raw, binned], bins: {k: 4, edges: value}, placebo: {noise: 2, seed: 5}, "
                + "suggestions: true, joint: {noise: 2, select: 3, pairs: 0}, conditioning: {fields: [x2], l2: 1.0e-4}}");
        Assertions.assertTrue(spec.jointOn && spec.suggestionsOn && spec.hasConditioning());
        final java.util.Random random = new java.util.Random(19);
        final List<ScreenRow> rows = new ArrayList<>();
        for (int i = 0; i < 600; i++) {
            final double x = random.nextGaussian(), b = random.nextGaussian();
            final double x2 = x + 0.02 * random.nextGaussian();
            final double y = random.nextDouble() < 1 / (1 + Math.exp(-(1.2 * x + 0.8 * b))) ? 1 : 0;
            rows.add(new ScreenRow("r" + i, "r" + i, i, null, y, Double.NaN, 1, new double[]{b, x, x2}));
        }
        final WindowQuantiles quantiles = new WindowQuantiles(spec.sketchColumns(), 0);
        for (final ScreenRow r : rows) quantiles.update(r.x, spec.sketchedColumns());
        final GroupScorer groups = new GroupScorer(spec).withWindowQuantiles(quantiles);
        final ConditioningScorer scorer = new ConditioningScorer(spec).withWindowQuantiles(quantiles);
        final com.mercari.solution.util.pipeline.glm.VectorAccumulator moments = new com.mercari.solution.util.pipeline.glm.VectorAccumulator();
        for (final ScreenRow r : rows) moments.add(scorer.moments(r));
        com.mercari.solution.util.pipeline.glm.FitState state = com.mercari.solution.util.pipeline.glm.FitState.initial(scorer.k, scorer.initialTheta(moments.getValues()));
        for (int it = 0; it < spec.conditioningMaxIter && !state.converged; it++) {
            final com.mercari.solution.util.pipeline.glm.VectorAccumulator eval = new com.mercari.solution.util.pipeline.glm.VectorAccumulator();
            for (final ScreenRow r : rows) eval.add(scorer.evaluate(groups.prepare(List.of(r), r.getIdentity()), state.proposal, moments.getValues()));
            state.advance(eval.getValues(), spec.conditioningL2, spec.conditioningTol);
        }
        Assertions.assertTrue(state.hasBest);
        final Map<Integer, PartialAccumulator> partials = new HashMap<>();
        final Map<Integer, ScoreAccumulator> marginal = new HashMap<>();
        for (final ScreenRow r : rows) {
            final GroupScorer.Unit unit = groups.prepare(List.of(r), r.getIdentity());
            scorer.partial(unit, groups.columns(unit), state.bestTheta, moments.getValues(), partials);
            groups.score(List.of(r), r.getIdentity(), marginal);
        }
        // the partial pass keeps the binned block twice (window, discovery half) and the joint sums at p̂
        final int nb = spec.binCount();
        final double[] binnedPartial = partials.get(spec.key(1, 1)).getTotal();
        Assertions.assertEquals(2 * scorer.binnedPartialLength(nb), binnedPartial.length);
        final double[] marginalBinned = marginal.get(spec.key(1, 1)).getExtra();
        // the discovery half is the same seeded split as the marginal pass: a bin holds discovery rows in both halves'
        // sums or in neither (the partial H_bb = Σ w v̂ against the marginal Σ w of the discovery half)
        final int plen = scorer.binnedPartialLength(nb), mlen = marginalBinned.length / 2;
        double discMass = 0;
        for (int b = 0; b < nb; b++) {
            Assertions.assertEquals(marginalBinned[mlen + 3 * b] > 0, binnedPartial[plen + nb + b] > 0, "bin " + b);
            discMass += marginalBinned[mlen + 3 * b];
        }
        final ConditioningScorer.JointPartialLayout at = ConditioningScorer.JointPartialLayout.of(4, scorer.k);
        final double[] joint = partials.get(ConditioningScorer.JOINT_PARTIAL_KEY).getTotal();
        Assertions.assertEquals(at.length(), joint.length);
        Assertions.assertEquals(600d, joint[at.used()]);
        final ScreenReport.JointSums js = ScreenReport.partialJoint(spec, partials, state, 1d);
        Assertions.assertNotNull(js);
        Assertions.assertEquals("partial", js.basis());
        // x (joint column 1) is carried by F, b (joint column 0) is not
        Assertions.assertTrue(js.r2()[1] > 0.9, "r2_F of x: " + js.r2()[1]);
        Assertions.assertTrue(js.r2()[0] < 0.2, "r2_F of b: " + js.r2()[0]);
        // the marginal and the partial Fisher blocks agree on the near-identity of nothing here: the raw H is the
        // un-orthogonalised one, symmetric and positive on the diagonal
        Assertions.assertTrue(js.hRaw()[0][0] > 0 && js.hRaw()[1][1] > js.h()[1][1]);

        final ScreenReport.Result result = ScreenReport.build(spec, marginal, partials, state, new ScreenReport.Bins(groups::binRepresentatives, groups::binEdges, groups::gridEdges));
        // under conditioning the block's partial excess subtracts the partial block's df (600 rows as units)
        final Map<String, Object> bBinned = result.records().stream()
                .filter(r -> "b".equals(r.get("candidate")) && ScreenSpec.TRANSFORM_BINNED.equals(r.get("transform"))).findFirst().orElseThrow();
        Assertions.assertFalse((Boolean) bBinned.get("degenerate"));
        Assertions.assertEquals((Double) bBinned.get("partial_gain") - ((Long) bBinned.get("partial_df")).doubleValue() / 1200,
                (Double) bBinned.get("partial_excess_gain"), 1e-12);
        Assertions.assertEquals((Double) bBinned.get("est_gain") - ((Long) bBinned.get("df")).doubleValue() / 1200,
                (Double) bBinned.get("excess_gain"), 1e-12);
        final Map<String, Map<String, Object>> select = new HashMap<>();
        final List<Map<String, Object>> shapes = new ArrayList<>();
        for (final Map<String, Object> sg : result.suggestions()) {
            Assertions.assertNotNull(sg.get("basis"), sg.toString());
            if ("select".equals(sg.get("kind"))) select.put((String) sg.get("name"), sg);
            if ("shape".equals(sg.get("kind")) && !(Boolean) sg.get("placebo")) shapes.add(sg);
            if (java.util.Set.of("phd", "redundant", "select", "composite").contains((String) sg.get("kind"))) Assertions.assertEquals("partial", sg.get("basis"), sg.toString());
        }
        // the forward selection on the partial basis takes b first; x, explained by F, is not a step
        Assertions.assertTrue(select.containsKey("step1"), select.keySet().toString());
        Assertions.assertEquals("b", select.get("step1").get("candidate"), select.toString());
        Assertions.assertTrue((Double) select.get("step1").get("r2_F") < 0.2, select.get("step1").toString());
        Assertions.assertTrue(select.values().stream().noneMatch(sg -> "x".equals(sg.get("candidate"))), select.toString());
        // the one-candidate recipes are read on the partial block: x's carry r2_F ≈ 1 and a confirmation gain far
        // below b's; every candidate's shape record names the basis
        Map<String, Object> shapeB = null, shapeX = null;
        for (final Map<String, Object> sg : shapes) {
            Assertions.assertEquals("partial", sg.get("basis"), sg.toString());
            if ("b".equals(sg.get("candidate"))) shapeB = sg;
            if ("x".equals(sg.get("candidate"))) shapeX = sg;
        }
        Assertions.assertNotNull(shapeB);
        Assertions.assertNotNull(shapeX);
        // a linear F explains the block's linear direction (one of its four) on top of the intercept's, which every
        // block shares, so the block r2_F of x is well above b's without reaching 1 — the joint's r2_F above is the
        // column's own
        Assertions.assertTrue((Double) shapeX.get("r2_F") > (Double) shapeB.get("r2_F") + 0.1, shapeX + " vs " + shapeB);
        Assertions.assertTrue((Double) shapeB.get("r2_F") < 0.35, shapeB.toString());
        Assertions.assertTrue((Double) shapeX.get("confirmation_gain") < (Double) shapeB.get("confirmation_gain") / 3, shapeX + " vs " + shapeB);
        // the discovery half's fit sums are there for the halves' orthogonalisation
        final double[] discFit = partials.get(ConditioningScorer.FIT_PERIOD_KEY).getPeriods().get(ConditioningScorer.DISCOVERY_SLICE);
        Assertions.assertNotNull(discFit);
        Assertions.assertEquals(scorer.fitPeriodLength(), discFit.length);
        Assertions.assertTrue(discFit[0] > 200 && discFit[0] < 400, "discovery rows " + discFit[0]);
        // the fit's discovery slice covers the same rows as the marginal discovery half (unit weights)
        Assertions.assertEquals(discMass, discFit[0], 1e-9);
        Assertions.assertEquals(Boolean.TRUE, shapeB.get("passed"), shapeB.toString());

        // without conditioning the same data reads the marginal sums: basis marginal, no r2_F, x the first step
        final ScreenSpec plain = spec("{family: binomial, label: y, candidates: [b, x], transforms: [raw, binned], bins: {k: 4, edges: value}, placebo: {noise: 2, seed: 5}, "
                + "suggestions: true, joint: {noise: 2, select: 3, pairs: 0}}");
        final GroupScorer plainGroups = new GroupScorer(plain).withWindowQuantiles(quantiles);
        final Map<Integer, ScoreAccumulator> plainAcc = new HashMap<>();
        for (final ScreenRow r : rows) plainGroups.score(List.of(r), r.getIdentity(), plainAcc);
        final ScreenReport.Result plainResult = ScreenReport.build(plain, plainAcc, null, null, new ScreenReport.Bins(plainGroups::binRepresentatives, plainGroups::binEdges, plainGroups::gridEdges));
        boolean firstStepIsX = false;
        for (final Map<String, Object> sg : plainResult.suggestions()) {
            Assertions.assertEquals("marginal", sg.get("basis"), sg.toString());
            Assertions.assertNull(sg.get("r2_F"), sg.toString());
            if ("select".equals(sg.get("kind")) && "step1".equals(sg.get("name"))) firstStepIsX = "x".equals(sg.get("candidate"));
        }
        Assertions.assertTrue(firstStepIsX);
    }

    @Test
    public void testPartialJointCentresAndOrthogonalisesOverTheKeptRows() throws Exception {
        // no sketch view (no transform needs one): the row family's joint columns are not shifted and a row missing a
        // joint value is left out. b and c are independent with large means, x (30% missing) is re-encoded by the
        // conditioning column x2. The partial basis must orthogonalise x over the rows it keeps (r2_F ≈ 1 despite the
        // dropped rows) and read b / c on the intercept-centred metric (no r2_F from their means, no redundancy)
        final Schema schema = Schema.builder()
                .withField("y", Schema.FieldType.INT64)
                .withField("b", Schema.FieldType.FLOAT64)
                .withField("x", Schema.FieldType.FLOAT64)
                .withField("c", Schema.FieldType.FLOAT64)
                .withField("x2", Schema.FieldType.FLOAT64)
                .build();
        final ScreenSpec spec = ScreenSpec.parse(JsonParser.parseString("{family: binomial, label: y, candidates: [b, x, c], transforms: [raw], placebo: {noise: 2, seed: 5}, "
                + "joint: {noise: 2, select: 3, pairs: 0}, conditioning: {fields: [x2], l2: 1.0e-4}}").getAsJsonObject()).resolve(schema, null);
        Assertions.assertEquals(List.of("b", "x", "c"), spec.candidates);
        Assertions.assertEquals(3, spec.conditioningOffset());
        Assertions.assertFalse(spec.needsWindowQuantiles());
        final java.util.Random random = new java.util.Random(23);
        final List<ScreenRow> rows = new ArrayList<>();
        for (int i = 0; i < 800; i++) {
            final double x = random.nextGaussian(), b = random.nextGaussian(), c = random.nextGaussian();
            final double x2 = x + 0.02 * random.nextGaussian();
            final double y = random.nextDouble() < 1 / (1 + Math.exp(-(1.2 * x + 0.8 * b))) ? 1 : 0;
            final double xObserved = random.nextDouble() < 0.3 ? Double.NaN : x;
            rows.add(new ScreenRow("r" + i, "r" + i, i, null, y, Double.NaN, 1, new double[]{50 + b, xObserved, 80 + c, x2}));
        }
        final GroupScorer groups = new GroupScorer(spec);
        final ConditioningScorer scorer = new ConditioningScorer(spec);
        final com.mercari.solution.util.pipeline.glm.VectorAccumulator moments = new com.mercari.solution.util.pipeline.glm.VectorAccumulator();
        for (final ScreenRow r : rows) moments.add(scorer.moments(r));
        com.mercari.solution.util.pipeline.glm.FitState state = com.mercari.solution.util.pipeline.glm.FitState.initial(scorer.k, scorer.initialTheta(moments.getValues()));
        for (int it = 0; it < spec.conditioningMaxIter && !state.converged; it++) {
            final com.mercari.solution.util.pipeline.glm.VectorAccumulator eval = new com.mercari.solution.util.pipeline.glm.VectorAccumulator();
            for (final ScreenRow r : rows) eval.add(scorer.evaluate(groups.prepare(List.of(r), r.getIdentity()), state.proposal, moments.getValues()));
            state.advance(eval.getValues(), spec.conditioningL2, spec.conditioningTol);
        }
        Assertions.assertTrue(state.hasBest);
        final Map<Integer, PartialAccumulator> partials = new HashMap<>();
        final Map<Integer, ScoreAccumulator> marginal = new HashMap<>();
        for (final ScreenRow r : rows) {
            final GroupScorer.Unit unit = groups.prepare(List.of(r), r.getIdentity());
            scorer.partial(unit, groups.columns(unit), state.bestTheta, moments.getValues(), partials);
            groups.score(List.of(r), r.getIdentity(), marginal);
        }
        final ConditioningScorer.JointPartialLayout at = ConditioningScorer.JointPartialLayout.of(spec.jointColumnCount(), scorer.k);
        final double[] joint = partials.get(ConditioningScorer.JOINT_PARTIAL_KEY).getTotal();
        Assertions.assertTrue(joint[at.dropped()] > 150 && joint[at.dropped()] < 330, "dropped " + joint[at.dropped()]);
        final ScreenReport.JointSums js = ScreenReport.partialJoint(spec, partials, state, 1d);
        Assertions.assertNotNull(js);
        // joint columns in order b, x, c: x is F's over the kept rows, b and c carry nothing of F
        Assertions.assertTrue(js.r2()[1] > 0.9, "r2_F of x: " + js.r2()[1]);
        Assertions.assertTrue(js.r2()[0] < 0.2, "r2_F of b: " + js.r2()[0]);
        Assertions.assertTrue(js.r2()[2] < 0.2, "r2_F of c: " + js.r2()[2]);
        final double[][] hr = js.hRaw();
        Assertions.assertTrue(Math.abs(hr[0][2] / Math.sqrt(hr[0][0] * hr[2][2])) < 0.3, "corr(b, c)");
        final ScreenReport.Result result = ScreenReport.build(spec, marginal, partials, state, null);
        for (final Map<String, Object> sg : result.suggestions()) {
            Assertions.assertNotEquals("redundant", sg.get("kind"), sg.toString());
            if ("select".equals(sg.get("kind")) && "step1".equals(sg.get("name"))) Assertions.assertEquals("b", sg.get("candidate"), sg.toString());
        }
    }

    @Test
    public void testJointSuggestionsFromTheCandidatesSums() throws Exception {
        // independent binomial rows over three candidates (schema order b, x, x2): x carries the main effect, x2 is a
        // near copy of x (redundant), b an independent effect and an interaction with x. The joint sums give the
        // redundancy cluster {x, x2}, a forward selection that takes one of the pair and then b, the composite of the
        // two, and pHd directions that load on x and b (the interaction's curvature) rather than on the noise columns.
        final ScreenSpec spec = spec("{family: binomial, label: y, candidates: [b, x, x2], transforms: [raw], placebo: {noise: 2, seed: 5}, joint: {directions: 2, select: 3}}");
        Assertions.assertTrue(spec.jointOn);
        Assertions.assertEquals(List.of(0, 1, 2), spec.jointColumns);
        Assertions.assertEquals(5, spec.jointColumnCount());   // 3 candidates + 2 noise columns
        Assertions.assertEquals(3, spec.jointColumn(3));        // the first noise column
        final java.util.Random random = new java.util.Random(3);
        final GroupScorer scorer = new GroupScorer(spec);
        final Map<Integer, ScoreAccumulator> acc = new HashMap<>();
        for (int i = 0; i < 400; i++) {
            final double x = random.nextGaussian(), b = random.nextGaussian();
            final double x2 = x + 0.05 * random.nextGaussian();
            final double logit = 1.0 * x + 0.8 * b + 1.2 * x * b;
            final double y = random.nextDouble() < 1 / (1 + Math.exp(-logit)) ? 1 : 0;
            final ScreenRow r = new ScreenRow("r" + i, "r" + i, i, null, y, Double.NaN, 1, new double[]{b, x, x2});
            scorer.score(List.of(r), r.getIdentity(), acc);
        }
        final ScoreAccumulator joint = acc.get(ScoreAccumulator.JOINT_KEY);
        Assertions.assertNotNull(joint);
        Assertions.assertEquals(GroupScorer.jointLength(5), joint.getExtra().length);
        Assertions.assertEquals(400d, joint.getTotal()[ScoreAccumulator.N_OBS]);
        Assertions.assertEquals(400d, joint.getExtra()[GroupScorer.JointLayout.of(5).used()]);
        final ScreenReport.Result result = ScreenReport.build(spec, acc);
        Assertions.assertEquals(5L, result.summary().get("nJointColumns"));
        for (final Map<String, Object> sg : result.suggestions()) Assertions.assertEquals("marginal", sg.get("basis"), sg.toString());
        Assertions.assertEquals(400L, result.summary().get("nJointUnits"));
        Assertions.assertEquals(0L, result.summary().get("nJointFilled"));
        Assertions.assertEquals(0L, result.summary().get("nJointDropped"));
        final Map<String, List<Map<String, Object>>> byKind = new HashMap<>();
        for (final Map<String, Object> s : result.suggestions()) byKind.computeIfAbsent((String) s.get("kind"), k -> new java.util.ArrayList<>()).add(s);
        Assertions.assertEquals(java.util.Set.of("phd", "redundant", "select", "composite"), byKind.keySet(), byKind.toString());
        // redundancy: x and x2 are one cluster; the head is whichever scores higher, the other is named
        final Map<String, Object> cluster = byKind.get("redundant").get(0);
        Assertions.assertEquals(1, byKind.get("redundant").size());
        Assertions.assertTrue(List.of("x", "x2").contains(cluster.get("candidate")), cluster.toString());
        Assertions.assertTrue((Double) cluster.get("share") >= 0.95, cluster.toString());
        Assertions.assertTrue(((String) cluster.get("fragment")).contains(cluster.get("candidate").equals("x") ? "x2" : "x"), cluster.toString());
        // forward selection: one of the redundant pair, then b (never the other twin); each step above the df1 cut
        final List<Map<String, Object>> steps = byKind.get("select");
        Assertions.assertTrue(steps.size() >= 2, steps.toString());
        Assertions.assertEquals("step1", steps.get(0).get("name"));
        final java.util.Set<String> picked = new java.util.HashSet<>();
        for (final Map<String, Object> s : steps) picked.add((String) s.get("candidate"));
        Assertions.assertTrue(picked.contains("b"), steps.toString());
        Assertions.assertFalse(picked.contains("x") && picked.contains("x2"), steps.toString());
        for (final Map<String, Object> s : steps) {
            Assertions.assertEquals(Boolean.TRUE, s.get("passed"));
            Assertions.assertTrue((Double) s.get("confirmation_gain") > 0);
        }
        // the composite: a row expression over the selected set
        final Map<String, Object> composite = byKind.get("composite").get(0);
        Assertions.assertTrue(((String) composite.get("fragment")).startsWith("{scope: row, expr: \""), composite.toString());
        Assertions.assertTrue(((String) composite.get("fragment")).contains("*b"), composite.toString());
        Assertions.assertTrue((Double) composite.get("chi2") >= (Double) steps.get(0).get("chi2") - 1e-9, composite.toString());
        // pHd: two directions, the leading one loading on x (or x2) and b, the noise columns near zero
        final List<Map<String, Object>> phd = byKind.get("phd");
        Assertions.assertEquals(2, phd.size());
        Assertions.assertEquals("direction1", phd.get(0).get("name"));
        Assertions.assertTrue(List.of("x", "x2", "b").contains(phd.get(0).get("candidate")), phd.toString());
        Assertions.assertTrue((Double) phd.get(0).get("consistency") < 0.3, "noise loading " + phd.get(0).get("consistency"));
        Assertions.assertTrue((Double) phd.get(0).get("share") > (Double) phd.get(1).get("share") - 1e-12);
        Assertions.assertNull(phd.get(0).get("passed"));
        Assertions.assertTrue(((String) phd.get(0).get("fragment")).contains("*"), phd.toString());
        Assertions.assertTrue(ScreenReport.describe(spec).contains("joint=3+2"));
        // validation: at least two joint columns, the bound, an include that keeps one column
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: binomial, label: y, candidates: [x], joint: true}"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: binomial, label: y, candidates: [b, x, x2], joint: {maxColumns: 2}}"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: binomial, label: y, candidates: [b, x, x2], joint: {include: ['b']}}"));
        Assertions.assertEquals(List.of(1, 2), spec("{family: binomial, label: y, candidates: [b, x, x2], joint: {include: ['x*'], noise: 0}}").jointColumns);
        // a lineage selector in joint.include needs lineage, as in candidates.include
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: binomial, label: y, candidates: [b, x, x2], joint: {include: ['block:a']}}"));
    }

    @Test
    public void testJointGroupedWithAUnitConstantColumn() {
        // grouped units of four: x drives the choice, x2 is a large unit-level value (constant within each unit, so it
        // carries no within-unit information): its Fisher block is exactly 0, it is never selected nor clustered
        final ScreenSpec spec = spec("{family: groupedMultinomial, group: g, label: y, time: t, candidates: [x, x2], transforms: [raw], placebo: {noise: 2, seed: 1}, joint: {select: 2, directions: 1}}");
        final java.util.Random random = new java.util.Random(7);
        final GroupScorer scorer = new GroupScorer(spec);
        final Map<Integer, ScoreAccumulator> acc = new HashMap<>();
        for (int u = 0; u < 200; u++) {
            final double[] x = new double[4];
            final double[] weights = new double[4];
            double total = 0;
            for (int i = 0; i < 4; i++) {
                x[i] = random.nextGaussian();
                weights[i] = Math.exp(1.5 * x[i]);
                total += weights[i];
            }
            double draw = random.nextDouble() * total;
            int winner = 3;
            for (int i = 0; i < 4; i++) {
                draw -= weights[i];
                if (draw <= 0) {
                    winner = i;
                    break;
                }
            }
            final List<ScreenRow> rows = new java.util.ArrayList<>();
            for (int i = 0; i < 4; i++) rows.add(row("g" + u, i, i == winner ? 1 : 0, Double.NaN, x[i], 1e6 + u / 3d));
            scorer.score(rows, "g" + u, acc);
        }
        final double[] e = acc.get(ScoreAccumulator.JOINT_KEY).getExtra();
        final GroupScorer.JointLayout at = GroupScorer.JointLayout.of(4);
        Assertions.assertEquals(0d, e[at.h() + GroupScorer.packed(4, 1, 1)]);   // x2's Fisher information: exactly 0
        Assertions.assertEquals(200d, e[at.used()]);
        final ScreenReport.Result result = ScreenReport.build(spec, acc);
        final List<Map<String, Object>> steps = result.suggestions().stream().filter(s -> "select".equals(s.get("kind"))).toList();
        Assertions.assertFalse(steps.isEmpty(), result.suggestions().toString());
        Assertions.assertEquals("x", steps.get(0).get("candidate"));
        Assertions.assertTrue(steps.stream().noneMatch(s -> "x2".equals(s.get("candidate"))), steps.toString());
        Assertions.assertTrue(result.suggestions().stream().noneMatch(s -> "redundant".equals(s.get("kind"))), result.suggestions().toString());
    }

    @Test
    public void testJointGaussianOffsetScaleAndInterceptInvariance() {
        // gaussian rows with a baseline and a label on a large scale (residual sd about 100): the forward selection's
        // first step is the marginal score test on the same rows, so its gain equals the raw record's est_gain (both
        // divided by the residual variance). Shifting the baseline by a constant (a miscalibrated baseline) moves the
        // mean residual only: the intercept is profiled out of S, H and M alike, so nothing reported changes.
        final ScreenSpec spec = spec("{family: gaussian, label: y, baseline: b, candidates: [x, x2], transforms: [raw], placebo: {noise: 0}, joint: {noise: 0, directions: 1, select: 2}}");
        Assertions.assertEquals(List.of("x", "x2"), spec.candidates);
        final List<List<Map<String, Object>>> runs = new java.util.ArrayList<>();
        ScreenReport.Result first = null;
        for (final double shift : new double[]{0d, 30d}) {
            final java.util.Random random = new java.util.Random(11);
            final GroupScorer scorer = new GroupScorer(spec);
            final Map<Integer, ScoreAccumulator> acc = new HashMap<>();
            for (int i = 0; i < 300; i++) {
                final double x = random.nextGaussian(), x2 = random.nextGaussian(), base = 5 * random.nextGaussian();
                final double y = base + 40 * x + 15 * x * x2 + 100 * random.nextGaussian();
                final ScreenRow r = new ScreenRow("r" + i, "r" + i, i, null, y, base + shift, 1, new double[]{x, x2});
                scorer.score(List.of(r), r.getIdentity(), acc);
            }
            final ScreenReport.Result result = ScreenReport.build(spec, acc);
            if (first == null) first = result;
            runs.add(result.suggestions());
        }
        final Map<String, Object> step1 = first.suggestions().stream().filter(s -> "select".equals(s.get("kind"))).findFirst().orElseThrow();
        final Map<String, Object> marginal = first.records().stream().filter(r -> step1.get("candidate").equals(r.get("candidate"))).findFirst().orElseThrow();
        Assertions.assertEquals((Double) marginal.get("est_gain"), (Double) step1.get("confirmation_gain"), 1e-9 * (Double) marginal.get("est_gain"), step1.toString());
        Assertions.assertEquals(first.summary().get("threshold"), step1.get("threshold"));
        // no noise column: no null scale to report
        final Map<String, Object> phd = first.suggestions().stream().filter(s -> "phd".equals(s.get("kind"))).findFirst().orElseThrow();
        Assertions.assertNull(phd.get("consistency"), phd.toString());
        // the baseline shift changes nothing
        Assertions.assertEquals(runs.get(0).size(), runs.get(1).size());
        for (int i = 0; i < runs.get(0).size(); i++) {
            final Map<String, Object> a = runs.get(0).get(i), b = runs.get(1).get(i);
            Assertions.assertEquals(a.get("kind"), b.get("kind"));
            Assertions.assertEquals(a.get("candidate"), b.get("candidate"));
            Assertions.assertEquals((Double) a.get("chi2"), (Double) b.get("chi2"), 1e-6 * Math.abs((Double) a.get("chi2")) + 1e-12, a + " / " + b);
        }
    }

    @Test
    public void testJointDifferenceAndRatio() throws Exception {
        // independent binomial rows over two positive candidates (schema order b, x) whose label follows the
        // difference b − x: each alone carries part of the effect, the pair's joint chi2 clearly exceeds the better
        // single one and the two-dimensional Newton direction is equal and opposite — a difference, and, both
        // columns being positive, a ratio
        final ScreenSpec spec = spec("{family: binomial, label: y, candidates: [b, x], transforms: [raw], placebo: {noise: 0}, joint: {noise: 0, select: 0, directions: 1}}");
        Assertions.assertEquals(2, spec.jointColumnCount());
        final java.util.Random random = new java.util.Random(9);
        final GroupScorer scorer = new GroupScorer(spec);
        final Map<Integer, ScoreAccumulator> acc = new HashMap<>();
        final WindowQuantiles q = new WindowQuantiles(2);
        for (int i = 0; i < 600; i++) {
            final double b = 2 + random.nextGaussian(), x = 2 + random.nextGaussian();
            final double logit = 1.5 * (b - x);
            final double y = random.nextDouble() < 1 / (1 + Math.exp(-logit)) ? 1 : 0;
            final ScreenRow r = new ScreenRow("r" + i, "r" + i, i, null, y, Double.NaN, 1, new double[]{Math.max(b, 0.1), Math.max(x, 0.1)});
            q.update(r.x);
            scorer.score(List.of(r), r.getIdentity(), acc);
        }
        final GroupScorer withSketch = new GroupScorer(spec).withWindowQuantiles(q);
        final ScreenReport.Result result = ScreenReport.build(spec, acc, null, null, new ScreenReport.Bins(withSketch::binRepresentatives, withSketch::binEdges, withSketch::gridEdges, withSketch::columnMin));
        final Map<String, List<Map<String, Object>>> byKind = new HashMap<>();
        for (final Map<String, Object> s : result.suggestions()) byKind.computeIfAbsent((String) s.get("kind"), k -> new java.util.ArrayList<>()).add(s);
        Assertions.assertTrue(byKind.containsKey("difference"), byKind.keySet().toString());
        final Map<String, Object> diff = byKind.get("difference").get(0);
        Assertions.assertEquals("b", diff.get("candidate"));
        Assertions.assertEquals("b - x", diff.get("name"));
        Assertions.assertTrue((Double) diff.get("share") > 1.5, "excess " + diff.get("share"));   // share = the joint chi2 over the better single
        Assertions.assertTrue((Double) diff.get("consistency") > 0.5, "magnitude ratio " + diff.get("consistency"));
        Assertions.assertTrue(((String) diff.get("fragment")).startsWith("{scope: row, expr: \"b - "), diff.toString());
        // the ratio: both columns positive in the window (the sketch minima)
        Assertions.assertTrue(byKind.containsKey("ratio"), byKind.keySet().toString());
        Assertions.assertEquals("{scope: row, expr: \"b / x\"} (both positive; the difference's log-scale reading, approximate)", byKind.get("ratio").get(0).get("fragment"));
        // without the sketch minima no ratio is read (the sign of the columns is unknown)
        final ScreenReport.Result noSketch = ScreenReport.build(spec, acc);
        Assertions.assertTrue(noSketch.suggestions().stream().anyMatch(s -> "difference".equals(s.get("kind"))));
        Assertions.assertTrue(noSketch.suggestions().stream().noneMatch(s -> "ratio".equals(s.get("kind"))));
        // a pair whose members pull the same way (a sum, not a difference) is not suggested
        final Map<Integer, ScoreAccumulator> sumAcc = new HashMap<>();
        for (int i = 0; i < 600; i++) {
            final double b = random.nextGaussian(), x = random.nextGaussian();
            final double y = random.nextDouble() < 1 / (1 + Math.exp(-(b + x))) ? 1 : 0;
            final ScreenRow r = new ScreenRow("r" + i, "r" + i, i, null, y, Double.NaN, 1, new double[]{b, x});
            scorer.score(List.of(r), r.getIdentity(), sumAcc);
        }
        Assertions.assertTrue(ScreenReport.build(spec, sumAcc).suggestions().stream().noneMatch(s -> "difference".equals(s.get("kind"))));
        // pure noise: the excess and sign rules alone let a noise pair through about one run in five (equal |z| of
        // opposite signs: the joint chi2 up to twice the better single); the increment's df = 1 cut keeps it out
        boolean looseSeen = false;
        for (int seed = 0; seed < 100 && !looseSeen; seed++) {
            final java.util.Random rnd = new java.util.Random(seed);
            final Map<Integer, ScoreAccumulator> noiseAcc = new HashMap<>();
            for (int i = 0; i < 600; i++) {
                final double b = rnd.nextGaussian(), x = rnd.nextGaussian();
                final double y = rnd.nextBoolean() ? 1 : 0;
                final ScreenRow r = new ScreenRow("r" + i, "r" + i, i, null, y, Double.NaN, 1, new double[]{b, x});
                scorer.score(List.of(r), r.getIdentity(), noiseAcc);
            }
            if (ScreenReport.joint(spec, noiseAcc, null, null, false, 1d, 600, Double.NEGATIVE_INFINITY, null).stream().noneMatch(s -> "difference".equals(s.get("kind")))) continue;
            looseSeen = true;
            Assertions.assertTrue(ScreenReport.build(spec, noiseAcc).suggestions().stream().noneMatch(s -> "difference".equals(s.get("kind"))), "seed " + seed);
        }
        Assertions.assertTrue(looseSeen);
        // the ratio's positivity needs the window sketches even without a rank / absdev / binned / pair-shape reader
        Assertions.assertFalse(spec.needsWindowQuantiles());
        Assertions.assertTrue(spec.needsJointMinima());
        // ... and the pre-pass then feeds the candidates, so their sketches are not left empty
        Assertions.assertArrayEquals(new int[]{0, 1}, spec.sketchedColumns());
        Assertions.assertFalse(spec("{family: binomial, label: y, candidates: [b, x], joint: {pairs: 0}}").needsJointMinima());
        // a row family feeds the joint columns for the missing-value fill (the window means) even without the minima;
        // the grouped family fills within the unit and feeds nothing
        Assertions.assertArrayEquals(new int[]{0, 1}, spec("{family: binomial, label: y, candidates: [b, x], joint: {pairs: 0}}").sketchedColumns());
        Assertions.assertArrayEquals(new int[0], spec("{family: groupedMultinomial, group: g, label: y, time: t, candidates: [b, x], joint: {pairs: 0}}").sketchedColumns());
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: binomial, label: y, candidates: [b, x], joint: {excess: 0.5}}"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: binomial, label: y, candidates: [b, x], joint: {pairs: 1.5}}"));
    }

    @Test
    public void testCategoricalCandidates() throws Exception {
        // independent binomial rows with a string field g (four levels with different positive rates) as a categorical
        // candidate: the pre-pass counts the levels, the score pass keeps a block per level (df = 3) and five placebo
        // columns with the levels redrawn from the window frequencies; the level dictionary folds beyond maxLevels
        final ScreenSpec spec = spec("{family: binomial, label: y, candidates: [x], transforms: [raw], placebo: {noise: 0}, categorical: {include: [g], placebo: 5}}");
        Assertions.assertEquals(List.of("g"), spec.categoricals);
        Assertions.assertTrue(spec.needsWindowQuantiles());
        Assertions.assertEquals(ScreenSpec.CATEGORICAL_KEY_BASE, spec.categoricalKey(0, -1));
        Assertions.assertEquals("g*__noise_2", spec.categoricalPlaceboName(0, 2));
        final String[] levels = {"A", "B", "C", "D"};
        final double[] rate = {0.1, 0.3, 0.6, 0.85};
        final java.util.Random random = new java.util.Random(21);
        final List<ScreenRow> rows = new java.util.ArrayList<>();
        final WindowQuantiles q = new WindowQuantiles(spec.sketchColumns(), 1);
        for (int i = 0; i < 400; i++) {
            final int l = i % 4;
            final double y = random.nextDouble() < rate[l] ? 1 : 0;
            final ScreenRow r = new ScreenRow("r" + i, "r" + i, i, null, null, y, Double.NaN, 1, new double[]{random.nextGaussian()}, new String[]{levels[l]});
            rows.add(r);
            q.update(r.x);
            q.updateLevels(r.cat);
        }
        // the dictionary: by count then name; folded beyond maxLevels
        final WindowQuantiles.Levels dict = q.levels(0, 32);
        Assertions.assertEquals(List.of("A", "B", "C", "D"), dict.names());
        Assertions.assertFalse(dict.folded());
        Assertions.assertEquals(2, dict.indexOf("C"));
        Assertions.assertEquals(-1, dict.indexOf("Z"));
        Assertions.assertEquals(0.25, dict.frequency()[1], 1e-12);
        final WindowQuantiles.Levels folded = q.levels(0, 2);
        Assertions.assertEquals(List.of("A", "B", "(other)"), folded.names());
        Assertions.assertTrue(folded.folded());
        Assertions.assertEquals(2, folded.indexOf("D"));
        Assertions.assertEquals(0.5, folded.frequency()[2], 1e-12);
        // the coder round-trips the counts
        final java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
        WindowQuantiles.CODER.encode(q, bytes);
        Assertions.assertEquals(dict.names(), WindowQuantiles.CODER.decode(new java.io.ByteArrayInputStream(bytes.toByteArray())).levels(0, 32).names());

        final GroupScorer scorer = new GroupScorer(spec).withWindowQuantiles(q);
        final Map<Integer, ScoreAccumulator> acc = new HashMap<>();
        for (final ScreenRow r : rows) scorer.score(List.of(r), r.getIdentity(), acc);
        Assertions.assertTrue(acc.containsKey(spec.categoricalKey(0, -1)) && acc.containsKey(spec.categoricalKey(0, 4)));
        final ScreenReport.Result result = ScreenReport.build(spec, acc, null, null,
                new ScreenReport.Bins(scorer::binRepresentatives, scorer::binEdges, scorer::gridEdges, scorer::columnMin, scorer::categoricalLevels));
        final Map<String, Map<String, Object>> byKey = new HashMap<>();
        for (final Map<String, Object> r : result.records()) byKey.put(r.get("candidate") + ":" + r.get("transform"), r);
        Assertions.assertEquals(1 + 1 + 5, result.records().size(), byKey.keySet().toString());
        final Map<String, Object> g = byKey.get("g:levels");
        Assertions.assertEquals(3L, g.get("df"));
        Assertions.assertNull(g.get("z"));
        Assertions.assertTrue((Double) g.get("chi2") > 50, "categorical chi2 " + g.get("chi2"));   // rates 0.1 / 0.3 / 0.6 / 0.85 over 100 rows each
        Assertions.assertEquals(Boolean.TRUE, g.get("passed"));
        Assertions.assertEquals(Boolean.FALSE, g.get("degenerate"));
        Assertions.assertEquals(400L, g.get("n_obs"));
        @SuppressWarnings("unchecked") final List<Map<String, Object>> levelZ = (List<Map<String, Object>>) g.get("level_z");
        Assertions.assertEquals(4, levelZ.size());
        Assertions.assertEquals("A", levelZ.get(0).get("level"));
        Assertions.assertTrue((Double) levelZ.get(0).get("z") < -3 && (Double) levelZ.get(3).get("z") > 3, levelZ.toString());
        Assertions.assertEquals(100L, levelZ.get(0).get("n"));
        for (int r = 0; r < 5; r++) {
            final Map<String, Object> placebo = byKey.get("g*__noise_" + r + ":levels");
            Assertions.assertEquals(Boolean.TRUE, placebo.get("placebo"));
            Assertions.assertEquals(Boolean.FALSE, placebo.get("passed"));
            Assertions.assertNull(placebo.get("level_z"));
            Assertions.assertTrue((Double) placebo.get("chi2") < 30, placebo.toString());
        }
        @SuppressWarnings("unchecked") final Map<String, Double> thresholds = (Map<String, Double>) result.summary().get("thresholds");
        Assertions.assertTrue(thresholds.containsKey("levels"));
        Assertions.assertEquals(1L, result.summary().get("nCategoricals"));
        Assertions.assertEquals(List.of("g"), result.summary().get("passedColumns"));
        // the grouping suggestion cuts the levels by effect, the strong levels get a one-hot
        final Map<String, List<Map<String, Object>>> byKind = new HashMap<>();
        for (final Map<String, Object> s : result.suggestions()) byKind.computeIfAbsent((String) s.get("kind"), k -> new java.util.ArrayList<>()).add(s);
        Assertions.assertTrue(byKind.containsKey("grouping") && byKind.containsKey("onehot"), byKind.keySet().toString());
        final Map<String, Object> grouping = byKind.get("grouping").get(0);
        Assertions.assertEquals("g", grouping.get("candidate"));
        Assertions.assertTrue((Double) grouping.get("share") > 0.5 && (Double) grouping.get("share") <= 1.0 + 1e-9, grouping.toString());
        Assertions.assertTrue(((String) grouping.get("fragment")).contains("[A, B]") || ((String) grouping.get("fragment")).contains("[C, D]"), grouping.toString());
        Assertions.assertTrue(byKind.get("onehot").stream().anyMatch(s -> "D".equals(s.get("name"))), byKind.get("onehot").toString());
        // the one-hot fragment is the feature transform's row indicator op (its expressions have no string literal)
        Assertions.assertTrue(byKind.get("onehot").stream().filter(s -> "D".equals(s.get("name")))
                .allMatch(s -> ((String) s.get("fragment")).startsWith("{name: g_is_D, scope: row, type: indicator, input: g, values: [\"D\"]}")), byKind.get("onehot").toString());
        Assertions.assertTrue(ScreenReport.describe(spec).contains("categorical=1 [g]"));
        // validation
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: binomial, label: y, candidates: [x], categorical: {maxLevels: 4}}"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: binomial, label: y, candidates: [x], categorical: {include: [x]}}"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: groupedMultinomial, group: g, label: y, candidates: [x], categorical: {include: [g]}}"));
    }

    @Test
    public void testCategoricalContrastSignAndCategoricalsAlone() throws Exception {
        // a baseline below both levels (0.2 against rates 0.3 / 0.5): both raw level scores are positive, yet level A lies
        // below the rest — its contrast z is negative. No numeric candidate matches: a screen of categoricals alone.
        for (final String pass : new String[]{"", ", pass: {minGain: 1000}"}) {
            final ScreenSpec spec = spec("{family: binomial, label: y, baseline: {field: b, form: prob}, candidates: [nomatch], transforms: [raw], placebo: {noise: 0}, categorical: {include: [g], placebo: 0}" + pass + "}");
            Assertions.assertTrue(spec.candidates.isEmpty());
            Assertions.assertEquals(List.of("g"), spec.categoricals);
            final WindowQuantiles q = new WindowQuantiles(spec.sketchColumns(), 1);
            final List<ScreenRow> rows = new java.util.ArrayList<>();
            for (int i = 0; i < 200; i++) {
                final boolean a = i < 100;
                final double y = (a ? i < 30 : i - 100 < 50) ? 1 : 0;
                final ScreenRow r = new ScreenRow("r" + i, "r" + i, i, null, null, y, 0.2, 1, new double[0], new String[]{a ? "A" : "B"});
                rows.add(r);
                q.update(r.x);
                q.updateLevels(r.cat);
            }
            final GroupScorer scorer = new GroupScorer(spec).withWindowQuantiles(q);
            final Map<Integer, ScoreAccumulator> acc = new HashMap<>();
            for (final ScreenRow r : rows) scorer.score(List.of(r), r.getIdentity(), acc);
            final ScreenReport.Result result = ScreenReport.build(spec, acc, null, null,
                    new ScreenReport.Bins(scorer::binRepresentatives, scorer::binEdges, scorer::gridEdges, scorer::columnMin, scorer::categoricalLevels));
            final Map<String, Object> g = result.records().stream().filter(r -> "g".equals(r.get("candidate"))).findFirst().orElseThrow();
            @SuppressWarnings("unchecked") final List<Map<String, Object>> levelZ = (List<Map<String, Object>>) g.get("level_z");
            Assertions.assertEquals("A", levelZ.get(0).get("level"));
            Assertions.assertTrue((Double) levelZ.get(0).get("S") > 0, levelZ.toString());
            Assertions.assertTrue((Double) levelZ.get(0).get("z") < 0 && (Double) levelZ.get(1).get("z") > 0, levelZ.toString());
            if (pass.isEmpty()) {
                Assertions.assertEquals(Boolean.TRUE, g.get("passed"));
                final Map<String, Object> onehotA = result.suggestions().stream().filter(s -> "onehot".equals(s.get("kind")) && "A".equals(s.get("name"))).findFirst().orElseThrow();
                Assertions.assertTrue(((String) onehotA.get("fragment")).contains("(z -"), onehotA.toString());
            } else {
                // a column that did not pass gets no grouping / one-hot suggestion
                Assertions.assertEquals(Boolean.FALSE, g.get("passed"));
                Assertions.assertTrue(result.suggestions().isEmpty(), result.suggestions().toString());
            }
        }
    }

    @Test
    public void testCategoricalLevelWithoutInformation() throws Exception {
        // levels A / B (100 rows each at baseline 0.2) and a lone row of level C at p̂ ≈ 0 with y = 1 (H ≈ 1e-12, S ≈ 1):
        // C has no z and takes no part in the grouping, whose split would otherwise isolate it with a gain of 1 / H
        final ScreenSpec spec = spec("{family: binomial, label: y, baseline: {field: b, form: prob}, candidates: [nomatch], transforms: [raw], placebo: {noise: 0}, categorical: {include: [g], placebo: 0}}");
        final WindowQuantiles q = new WindowQuantiles(spec.sketchColumns(), 1);
        final List<ScreenRow> rows = new java.util.ArrayList<>();
        for (int i = 0; i < 201; i++) {
            final String level = i < 100 ? "A" : i < 200 ? "B" : "C";
            final double y = i == 200 || (i < 100 ? i < 30 : i - 100 < 50) ? 1 : 0;
            final ScreenRow r = new ScreenRow("r" + i, "r" + i, i, null, null, y, i == 200 ? 0 : 0.2, 1, new double[0], new String[]{level});
            rows.add(r);
            q.update(r.x);
            q.updateLevels(r.cat);
        }
        final GroupScorer scorer = new GroupScorer(spec).withWindowQuantiles(q);
        final Map<Integer, ScoreAccumulator> acc = new HashMap<>();
        for (final ScreenRow r : rows) scorer.score(List.of(r), r.getIdentity(), acc);
        final ScreenReport.Result result = ScreenReport.build(spec, acc, null, null,
                new ScreenReport.Bins(scorer::binRepresentatives, scorer::binEdges, scorer::gridEdges, scorer::columnMin, scorer::categoricalLevels));
        final Map<String, Object> g = result.records().stream().filter(r -> "g".equals(r.get("candidate"))).findFirst().orElseThrow();
        @SuppressWarnings("unchecked") final List<Map<String, Object>> levelZ = (List<Map<String, Object>>) g.get("level_z");
        final Map<String, Object> c = levelZ.stream().filter(l -> "C".equals(l.get("level"))).findFirst().orElseThrow();
        Assertions.assertNull(c.get("z"), levelZ.toString());
        Assertions.assertNotNull(levelZ.stream().filter(l -> "A".equals(l.get("level"))).findFirst().orElseThrow().get("z"), levelZ.toString());
        Assertions.assertEquals(Boolean.TRUE, g.get("passed"), g.toString());
        final Map<String, Object> grouping = result.suggestions().stream().filter(s -> "grouping".equals(s.get("kind"))).findFirst().orElseThrow();
        Assertions.assertTrue(((String) grouping.get("fragment")).startsWith("group g into [A] (lower effect) vs [B]"), grouping.toString());
        for (final Map<String, Object> s : result.suggestions()) {
            Assertions.assertTrue((Double) s.get("chi2") < 1e3, s.toString());
            Assertions.assertNotEquals("C", s.get("name"), s.toString());
        }
    }

    @Test
    public void testPassRuleMinGain() {
        // x separates the positive in every group: it clears the placebo cut by far. A floor below its gain keeps
        // it, a floor above drops it; the record's threshold stays the placebo cut, the rule names the floor.
        Double gainOfX = null;
        final Double[] floors = {null, 0.001, 10d};
        for (final Double floor : floors) {
            final String pass = floor == null ? "" : ", pass: {minGain: " + floor + "}";
            final ScreenSpec spec = spec("{family: groupedMultinomial, group: g, label: y, candidates: [x], transforms: [raw], placebo: {noise: 0}" + pass + "}");
            final GroupScorer scorer = new GroupScorer(spec);
            final Map<Integer, ScoreAccumulator> acc = new HashMap<>();
            for (int g = 0; g < 30; g++) {
                scorer.score(List.of(
                        new ScreenRow("g" + g, "p", g, null, 1, Double.NaN, 1, new double[]{3}),
                        new ScreenRow("g" + g, "q", g, null, 0, Double.NaN, 1, new double[]{1}),
                        new ScreenRow("g" + g, "r", g, null, 0, Double.NaN, 1, new double[]{2})), "g" + g, acc);
            }
            final ScreenReport.Result result = ScreenReport.build(spec, acc);
            final Map<String, Object> xRaw = result.records().get(0);
            final double gain = (Double) xRaw.get("est_gain");
            if (gainOfX == null) gainOfX = gain; else Assertions.assertEquals(gainOfX, gain, pass);
            final double threshold = (Double) xRaw.get("threshold");
            Assertions.assertTrue(gain > 0.001 && gain > threshold && gain < 10, pass + " gain=" + gain);
            Assertions.assertEquals(threshold, result.summary().get("threshold"), pass);
            // the floor reads the excess gain: the gain less df / 2N (30 units, df = 1)
            final double excess = (Double) xRaw.get("excess_gain");
            Assertions.assertEquals(gain - 1d / 60, excess, 1e-12, pass);
            Assertions.assertNull(xRaw.get("partial_excess_gain"));
            final boolean expected = floor == null || floor < excess;
            Assertions.assertEquals(expected, xRaw.get("passed"), pass);
            Assertions.assertEquals(expected ? List.of("x") : List.of(), result.summary().get("passedColumns"), pass);
            Assertions.assertEquals(expected ? 1L : 0L, result.summary().get("nPassed"), pass);
            final String rule = (String) result.summary().get("passRule");
            Assertions.assertEquals(floor == null ? "est_gain > threshold" : "est_gain > threshold and excess_gain > " + floor, rule);
            Assertions.assertEquals(floor, result.summary().get("minGain"), pass);
            final com.google.gson.JsonObject selection = ScreenReport.selection(spec, result);
            Assertions.assertEquals(rule, selection.get("passRule").getAsString());
            Assertions.assertEquals(floor == null, selection.get("minGain").isJsonNull(), pass);
            Assertions.assertEquals(expected ? 1 : 0, selection.getAsJsonArray("passed").size(), pass);
            Assertions.assertEquals(floor != null, ScreenReport.describe(spec).contains("pass=" + rule), pass);
        }
        // with conditioning the floor applies to partial_gain, and it composes with the period agreement
        final ScreenSpec both = spec("{family: groupedMultinomial, group: g, label: y, time: t, candidates: [x], periods: year, pass: {minPeriodsAgree: 0.66, minGain: 1e-5}}");
        Assertions.assertEquals("partial_gain > threshold and partial_excess_gain > 1.0E-5 and partial_periods_agree >= 0.66 * partial_n_periods", ScreenReport.passRule(both, true));
        Assertions.assertEquals("est_gain > threshold and excess_gain > 1.0E-5 and periods_agree >= 0.66 * n_periods", ScreenReport.passRule(both, false));
        // a floor never lowers the cut below the placebo threshold
        final ScreenSpec low = spec("{family: groupedMultinomial, group: g, label: y, candidates: [x], pass: {minGain: 1e-12}}");
        Assertions.assertTrue(low.passesGain(0.5, 1, 1000, 0.4));
        Assertions.assertFalse(low.passesGain(0.5, 1, 1000, 0.6));
        Assertions.assertFalse(low.passesGain(0.5, 1, 1000, Double.NaN));
        Assertions.assertFalse(low.passesGain(Double.NaN, 1, 1000, 0.4));
        // the excess: a df = 10 block at chi2 = 10 over 100 units has gain 0.05 and no excess; the same gain with
        // df = 1 has 0.045 of it, so a floor between the two admits the df = 1 test alone
        Assertions.assertEquals(0d, ScreenSpec.excessGain(0.05, 10, 100), 1e-15);
        Assertions.assertEquals(0.045, ScreenSpec.excessGain(0.05, 1, 100), 1e-15);
        final ScreenSpec mid = spec("{family: groupedMultinomial, group: g, label: y, candidates: [x], pass: {minGain: 0.02}}");
        Assertions.assertTrue(mid.passesGain(0.05, 1, 100, 0.01));
        Assertions.assertFalse(mid.passesGain(0.05, 10, 100, 0.01));
        Assertions.assertTrue(spec("{family: groupedMultinomial, group: g, label: y, candidates: [x]}").passesGain(0.05, 10, 100, 0.01));
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: groupedMultinomial, group: g, label: y, candidates: [x], pass: {minGain: 0}}"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: groupedMultinomial, group: g, label: y, candidates: [x], pass: {minGain: -1}}"));
        // a declared but malformed floor is an error, never silently no floor
        for (final String bad : new String[]{"[1e-5]", "{value: 1e-5}", "abc", "true"}) {
            final IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                    () -> spec("{family: groupedMultinomial, group: g, label: y, candidates: [x], pass: {minGain: " + bad + "}}"), bad);
            Assertions.assertTrue(e.getMessage().contains("pass.minGain"), bad + ": " + e.getMessage());
        }
    }

    @Test
    public void testLeakZForms() {
        final ScreenSpec number = spec("{family: groupedMultinomial, group: g, label: y, candidates: [x], flags: {leakZ: 20}}");
        Assertions.assertEquals(20d, number.leakZ);
        Assertions.assertEquals(ScreenSpec.LEAK_ON_MARGINAL, number.leakOn);
        final ScreenSpec object = spec("{family: groupedMultinomial, group: g, label: y, candidates: [x], conditioning: [x2], flags: {leakZ: {z: 8, on: partial}}}");
        Assertions.assertEquals(8d, object.leakZ);
        Assertions.assertEquals(ScreenSpec.LEAK_ON_PARTIAL, object.leakOn);
        Assertions.assertTrue(ScreenReport.describe(object).contains("leakZ=8.0 on=partial"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: groupedMultinomial, group: g, label: y, candidates: [x], flags: {leakZ: {z: 8, on: partial}}}"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: groupedMultinomial, group: g, label: y, candidates: [x], flags: {leakZ: {on: marginal}}}"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: groupedMultinomial, group: g, label: y, candidates: [x], flags: {leakZ: {z: 8, on: both}}}"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: groupedMultinomial, group: g, label: y, candidates: [x], flags: {leakZ: {z: -1}}}"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: groupedMultinomial, group: g, label: y, candidates: [x], flags: {leakZ: partial}}"));
        // a quoted number is the number (as the scalar form always read it); NaN and a non-string `on` are rejected
        Assertions.assertEquals(20d, spec("{family: groupedMultinomial, group: g, label: y, candidates: [x], flags: {leakZ: '20'}}").leakZ);
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: groupedMultinomial, group: g, label: y, candidates: [x], flags: {leakZ: 'NaN'}}"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: groupedMultinomial, group: g, label: y, candidates: [x], flags: {leakZ: {z: abc}}}"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: groupedMultinomial, group: g, label: y, candidates: [x], conditioning: [x2], flags: {leakZ: {z: 8, on: [partial]}}}"));
    }

    @Test
    public void testLeakOnPartialFallsBackWithoutAFit() {
        // on: partial with conditioning, but no accepted fit: the flag reads the marginal z and a note says so
        final ScreenSpec spec = spec("{family: groupedMultinomial, group: g, label: y, time: t, candidates: [x], conditioning: [x2], transforms: [raw], placebo: {noise: 0}, flags: {leakZ: {z: 1, on: partial}}}");
        final GroupScorer scorer = new GroupScorer(spec);
        final Map<Integer, ScoreAccumulator> acc = new HashMap<>();
        for (int g = 0; g < 20; g++) {
            scorer.score(List.of(
                    new ScreenRow("g" + g, "p", g, "2025", 1, Double.NaN, 1, new double[]{3, 1}),
                    new ScreenRow("g" + g, "q", g, "2025", 0, Double.NaN, 1, new double[]{1, 1}),
                    new ScreenRow("g" + g, "r", g, "2025", 0, Double.NaN, 1, new double[]{2, 1})), "g" + g, acc);
        }
        final ScreenReport.Result result = ScreenReport.build(spec, acc);
        Assertions.assertEquals(Boolean.TRUE, result.records().get(0).get("leakSuspect"));   // marginal z = sqrt(30) > 1
        Assertions.assertEquals(ScreenSpec.LEAK_ON_MARGINAL, result.summary().get("leakOn"));
        Assertions.assertEquals(1L, result.summary().get("nLeakSuspect"));
        final List<?> notes = (List<?>) result.summary().get("notes");
        Assertions.assertTrue(notes.stream().anyMatch(n -> n.toString().startsWith("flags.leakZ.on partial")), "notes: " + notes);
        final com.google.gson.JsonObject selection = ScreenReport.selection(spec, result);
        Assertions.assertEquals(1d, selection.get("leakZ").getAsDouble());
        Assertions.assertEquals(ScreenSpec.LEAK_ON_MARGINAL, selection.get("leakOn").getAsString());
    }

    @Test
    public void testSpecValidation() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: gamma, label: y, candidates: [x]}"));
        // rank / absdev of independent rows read the window quantile sketch; shuffle still needs a group
        final ScreenSpec windowRank = spec("{family: binomial, label: y, transforms: [rank], candidates: [x]}");
        Assertions.assertTrue(windowRank.needsWindowQuantiles());
        Assertions.assertTrue(windowRank.notes.stream().anyMatch(n -> n.contains("window's quantile sketch")), windowRank.notes.toString());
        Assertions.assertFalse(spec("{family: binomial, label: y, candidates: [x]}").needsWindowQuantiles());
        Assertions.assertFalse(spec("{family: groupedMultinomial, group: g, label: y, candidates: [x]}").needsWindowQuantiles());
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: binomial, label: y, candidates: [x], placebo: {shuffle: {field: x}}}"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: groupedMultinomial, label: y, candidates: [x]}"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: binomial, label: y, candidates: [nothing_matches]}"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: binomial, label: y, candidates: {exclude: ['derivedFrom:market']}}"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: binomial, label: y, conditioning: {fields: [nothing_matches]}}"));
        final ScreenSpec s = spec("{family: binomial, label: {expr: 'y > 0 ? 1 : 0'}, baseline: b, time: {field: t, to: '2025-01-01T00:00:00Z'}, candidates: {include: ['x*'], exclude: [x2]}}");
        Assertions.assertEquals(List.of("x"), s.candidates);
        Assertions.assertEquals(List.of("raw"), s.transforms);
        Assertions.assertEquals("prob", s.baselineForm);
        Assertions.assertNotNull(s.timeToMillis);
    }

    @Test
    public void testLineageDefaultsAndSelectors() {
        final String manifest = "{timeField: t, roles: {group: {name: g, column: g}, label: {name: y, column: y}, baseline: {name: b, column: b}},"
                + " columns: [{name: x, scope: row, block: blk, lineage: {derivedFrom: [market], evidence: declared}}, {name: x2, scope: context, block: ctx, lineage: {derivedFrom: [attribute], evidence: measured}}]}";
        final FeatureLineage lineage = FeatureLineage.fromManifest(manifest, "candidates.manifest");
        final ScreenSpec s = ScreenSpec.parse(JsonParser.parseString("{candidates: {exclude: ['derivedFrom:market']}, placebo: {noise: 0}}").getAsJsonObject()).resolve(SCHEMA, lineage);
        Assertions.assertEquals("g", s.group);
        Assertions.assertEquals("y", s.labelField);
        Assertions.assertEquals("b", s.baselineField);
        Assertions.assertEquals("t", s.timeField);
        Assertions.assertEquals(List.of("x2"), s.candidates);
        Assertions.assertEquals(List.of("raw", "rank", "absdev"), s.transforms);
        final ScreenSpec byScope = ScreenSpec.parse(JsonParser.parseString("{candidates: {include: ['scope:row']}, placebo: {noise: 0}}").getAsJsonObject()).resolve(SCHEMA, lineage);
        Assertions.assertEquals(List.of("x"), byScope.candidates);
    }

    @Test
    public void testLineageFromManifestFieldsAndFromSchemaRoles() {
        // manifest: the pass-through fields carry scope input and their kind as derivedFrom (an older manifest has
        // kind only), so a selector drops a passed-through market input on the manifest path too
        final String manifest = "{timeField: t, roles: {group: {name: g, column: g}, label: {name: y, column: y}, baseline: {name: b, column: b}},"
                + " fields: [{name: x, scope: input, kind: market}, {name: x2, kind: attribute, derivedFrom: [attribute]}], columns: []}";
        final FeatureLineage fromManifest = FeatureLineage.fromManifest(manifest, "candidates.manifest");
        Assertions.assertEquals("input", fromManifest.columns.get("x2").scope());
        final ScreenSpec byManifest = ScreenSpec.parse(JsonParser.parseString("{candidates: {include: ['scope:input'], exclude: ['derivedFrom:market']}, placebo: {noise: 0}}").getAsJsonObject())
                .resolve(SCHEMA, fromManifest);
        Assertions.assertEquals(List.of("x2"), byManifest.candidates);
        // schema: the direct upstream's field options carry the roles and the time field as feature.role
        final Schema schema = Schema.builder()
                .withField(Schema.Field.of("g", Schema.FieldType.STRING).withOptions(options("feature.scope", "input", "feature.role", "group")))
                .withField(Schema.Field.of("y", Schema.FieldType.INT64).withOptions(options("feature.scope", "input", "feature.role", "label")))
                .withField(Schema.Field.of("b", Schema.FieldType.FLOAT64).withOptions(options("feature.scope", "row", "feature.role", "baseline")))
                .withField(Schema.Field.of("t", Schema.FieldType.TIMESTAMP).withOptions(options("feature.scope", "input", "feature.role", "time")))
                .withField(Schema.Field.of("x", Schema.FieldType.FLOAT64).withOptions(options("feature.scope", "input", "feature.derivedFrom", "market")))
                .withField("x2", Schema.FieldType.FLOAT64)
                .build();
        final ScreenSpec bySchema = ScreenSpec.parse(JsonParser.parseString("{candidates: {exclude: ['derivedFrom:market']}, placebo: {noise: 0}}").getAsJsonObject())
                .resolve(schema, FeatureLineage.fromSchema(schema));
        Assertions.assertEquals("g", bySchema.group);
        Assertions.assertEquals("y", bySchema.labelField);
        Assertions.assertEquals("b", bySchema.baselineField);
        Assertions.assertEquals("t", bySchema.timeField);
        Assertions.assertEquals(List.of("x2"), bySchema.candidates);
    }

    private static Map<String, String> options(final String... keyValues) {
        final Map<String, String> options = new HashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) options.put(keyValues[i], keyValues[i + 1]);
        return options;
    }

    @Test
    public void testKindSelector() {
        // kind: is the source field's origin tag on both lineage paths (pass-through inputs only; derived columns carry kinds in derivedFrom)
        final String manifest = "{roles: {group: {column: g}, label: {column: y}, baseline: {column: b}}, fields: [{name: x, scope: input, kind: market, derivedFrom: [market]}, {name: x2, scope: input, kind: attribute}], columns: []}";
        final ScreenSpec byManifest = ScreenSpec.parse(JsonParser.parseString("{candidates: {exclude: ['kind:market']}, placebo: {noise: 0}}").getAsJsonObject())
                .resolve(SCHEMA, FeatureLineage.fromManifest(manifest, "candidates.manifest"));
        Assertions.assertEquals(List.of("x2"), byManifest.candidates);
        Assertions.assertTrue(byManifest.notes.stream().anyMatch(n -> n.contains("x (kind:market)")), byManifest.notes::toString);
        final Schema schema = Schema.builder()
                .withField("g", Schema.FieldType.STRING).withField("y", Schema.FieldType.INT64).withField("t", Schema.FieldType.TIMESTAMP).withField("b", Schema.FieldType.FLOAT64)
                .withField(Schema.Field.of("x", Schema.FieldType.FLOAT64).withOptions(options("feature.scope", "input", "feature.kind", "market", "feature.derivedFrom", "market")))
                .withField(Schema.Field.of("x2", Schema.FieldType.FLOAT64).withOptions(options("feature.scope", "row", "feature.derivedFrom", "market")))
                .build();
        final ScreenSpec bySchema = ScreenSpec.parse(JsonParser.parseString("{group: g, label: y, baseline: b, candidates: {exclude: ['kind:market']}, placebo: {noise: 0}}").getAsJsonObject())
                .resolve(schema, FeatureLineage.fromSchema(schema));
        Assertions.assertEquals(List.of("x2"), bySchema.candidates);   // the derived column has no kind: derivedFrom:market would drop it, kind:market does not
    }

    @Test
    public void testDegenerateIsExactWithinTheUnit() {
        // a column constant within every unit gives H = 0 exactly whatever its magnitude: the centring shifts by
        // the unit's first value, so it works on the spread, not on the values (no rounding residue of the mean)
        final ScreenSpec grouped = spec("{family: groupedMultinomial, group: g, label: y, baseline: b, time: t, candidates: [x], transforms: [raw], placebo: {noise: 0}}");
        final GroupScorer scorer = new GroupScorer(grouped);
        final Map<Integer, ScoreAccumulator> constant = new HashMap<>();
        // a non-uniform baseline: the p-weighted mean of the raw values would round
        scorer.score(List.of(row("a", 1, 1, 0.37, 1e6), row("a", 1, 0, 0.41, 1e6), row("a", 1, 0, 0.22, 1e6)), "a", constant);
        final double[] c = constant.get(grouped.key(0, 0)).getTotal();
        Assertions.assertEquals(0d, c[ScoreAccumulator.H]);
        Assertions.assertEquals(0d, c[ScoreAccumulator.S]);
        Assertions.assertTrue(ScreenReport.stats(grouped, c, 1).degenerate());
        // the same magnitude with a within-unit spread of 1 is a real column: x̃ = [1, -1, 0], H = 2/3 (uniform p)
        final Map<Integer, ScoreAccumulator> offset = new HashMap<>();
        scorer.score(List.of(row("a", 1, 1, 1d, 1e6 + 1), row("a", 1, 0, 1d, 1e6 - 1), row("a", 1, 0, 1d, 1e6)), "a", offset);
        final ScreenReport.Stats st = ScreenReport.stats(grouped, offset.get(grouped.key(0, 0)).getTotal(), 1);
        Assertions.assertFalse(st.degenerate());
        Assertions.assertEquals(2d / 3, st.h(), 1e-9);
        // a spread of 1e-10 of the magnitude (an epoch-millisecond column, rows 100 ms apart) keeps every digit:
        // x̃ = [-100, 0, 100], H = 20000/3
        final Map<Integer, ScoreAccumulator> narrow = new HashMap<>();
        scorer.score(List.of(row("a", 1, 1, 1d, 1.7e12), row("a", 1, 0, 1d, 1.7e12 + 100), row("a", 1, 0, 1d, 1.7e12 + 200)), "a", narrow);
        final ScreenReport.Stats ns = ScreenReport.stats(grouped, narrow.get(grouped.key(0, 0)).getTotal(), 1);
        Assertions.assertFalse(ns.degenerate());
        Assertions.assertEquals(20000d / 3, ns.h(), 1e-6);
        // row family: a window-constant column of magnitude 1e6 (its Σ x̃² is a difference of moment sums, so the
        // report applies a relative floor)
        final ScreenSpec binomial = spec("{family: binomial, label: y, time: t, candidates: [x], placebo: {noise: 0}}");
        final GroupScorer rows = new GroupScorer(binomial);
        final Map<Integer, ScoreAccumulator> acc = new HashMap<>();
        final double[] y = {1, 1, 0, 0};
        for (int i = 0; i < y.length; i++) rows.score(List.of(row(null, i, y[i], Double.NaN, 1e6 + 1e-3 * (i % 2))), "r" + i, acc);
        Assertions.assertTrue(ScreenReport.stats(binomial, acc.get(binomial.key(0, 0)).getTotal(), 4).degenerate());
    }

    @Test
    public void testConditioningMissingGroupMean() {
        // x2 = [2, missing, 4] under baseline p = [.5, .3, .2]: window moments n = 2, mean 3, std 1;
        // mean → the missing row is 0 (the window mean), groupMean → the unit's p-weighted mean of the observed
        // values (.5·2 + .2·4) / .7 = 18/7, standardised (18/7 − 3) = −3/7
        final String base = "{family: groupedMultinomial, group: g, label: y, baseline: b, time: t, candidates: [x], transforms: [raw], placebo: {noise: 0}, conditioning: {fields: [x2]";
        final List<ScreenRow> rows = List.of(row("a", 1, 1, 0.5, 1, 2), row("a", 1, 0, 0.3, 2, Double.NaN), row("a", 1, 0, 0.2, 3, 4));
        for (final String missing : List.of("mean", "groupMean")) {
            final ScreenSpec spec = spec(base + ", missing: " + missing + "}}");
            final ConditioningScorer scorer = new ConditioningScorer(spec);
            final double[] moments = new double[3 + 2];
            for (final ScreenRow r : rows) {
                final double[] m = scorer.moments(r);
                for (int i = 0; i < m.length; i++) moments[i] += m[i];
            }
            final GroupScorer.Unit unit = new GroupScorer(spec).prepare(rows, "a");
            final double[][] f = scorer.design(unit, moments);
            Assertions.assertEquals(-1d, f[0][0], 1e-12);
            Assertions.assertEquals(1d, f[2][0], 1e-12);
            Assertions.assertEquals("mean".equals(missing) ? 0d : -3d / 7, f[1][0], 1e-12, missing);
        }
        Assertions.assertEquals("mean", spec(base + "}}").conditioningMissing);
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec(base + ", missing: nope}}"));
        // the row families have no unit to average over
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: binomial, label: y, time: t, candidates: [x], placebo: {noise: 0}, conditioning: {fields: [x2], missing: groupMean}}"));
    }

    @Test
    public void testExplicitTransformsSurviveGroupDefault() {
        final String manifest = "{timeField: t, roles: {group: {name: g, column: g}, label: {name: y, column: y}}}";
        final FeatureLineage lineage = FeatureLineage.fromManifest(manifest, "candidates.manifest");
        final ScreenSpec raw = ScreenSpec.parse(JsonParser.parseString("{transforms: [raw], candidates: [x], placebo: {noise: 0}}").getAsJsonObject()).resolve(SCHEMA, lineage);
        Assertions.assertEquals("g", raw.group);
        Assertions.assertEquals(List.of("raw"), raw.transforms);
        final ScreenSpec defaulted = ScreenSpec.parse(JsonParser.parseString("{candidates: [x], placebo: {noise: 0}}").getAsJsonObject()).resolve(SCHEMA, lineage);
        Assertions.assertEquals(List.of("raw", "rank", "absdev"), defaulted.transforms);
    }

    @Test
    public void testResolveRejectsUnusableFields() {
        // a time window needs an event-time field: the element timestamp of a bounded source is TIMESTAMP_MIN_VALUE
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: binomial, label: y, candidates: [x], time: {from: '2024-01-01T00:00:00Z'}}"));
        // a non-numeric shuffle reference would make every shuffle placebo degenerate
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{label: y, group: g, candidates: [x], placebo: {noise: 0, shuffle: {field: g, n: 2}}}"));
        // the periods field type is resolved like the time field's
        final Schema withDate = Schema.builder().withField("g", Schema.FieldType.STRING).withField("y", Schema.FieldType.INT64)
                .withField("t", Schema.FieldType.TIMESTAMP).withField("d", Schema.FieldType.DATE).withField("x", Schema.FieldType.FLOAT64).build();
        final ScreenSpec s = ScreenSpec.parse(JsonParser.parseString("{family: binomial, label: y, candidates: [x], time: {field: t}, periods: {field: d, bucket: month}, placebo: {noise: 0}}").getAsJsonObject()).resolve(withDate, null);
        Assertions.assertEquals("timestamp", s.timeFieldType);
        Assertions.assertEquals("date", s.periodsFieldType);
        // a manifest that is not JSON (e.g. a local path that does not exist) is reported as a config error
        Assertions.assertThrows(IllegalArgumentException.class, () -> FeatureLineage.fromManifest("manifests/missing.json", "candidates.manifest"));
        // the list form of conditioning must not silently disable the partial test
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: binomial, label: y, candidates: [x], conditioning: []}"));
        // the baseline is the model offset: it is a role field for conditioning like it is for candidates
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: binomial, label: y, baseline: b, candidates: [x], conditioning: {fields: [b]}}"));
    }
}
