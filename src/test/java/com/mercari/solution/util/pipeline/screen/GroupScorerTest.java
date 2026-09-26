package com.mercari.solution.util.pipeline.screen;

import com.google.gson.JsonParser;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.feature.FeatureLineage;
import com.mercari.solution.util.pipeline.glm.Baselines;
import com.mercari.solution.util.pipeline.glm.StatMath;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
        final ScreenReport.Result result = ScreenReport.build(spec, acc);
        final Map<String, Map<String, Object>> byKey = new HashMap<>();
        for (final Map<String, Object> r : result.records()) byKey.put(r.get("candidate") + ":" + r.get("transform"), r);
        final Map<String, Object> raw = byKey.get("x:raw");
        final Map<String, Object> binned = byKey.get("x:binned");
        Assertions.assertTrue(Math.abs((Double) raw.get("z")) < 2, "raw z " + raw.get("z"));
        Assertions.assertEquals(3L, binned.get("df"));
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
        final GroupScorer groups = new GroupScorer(spec);
        final ConditioningScorer scorer = new ConditioningScorer(spec);
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
        final ScreenReport.Result result = ScreenReport.build(spec, marginal, partials, state);
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
        // among resolving to a single field tests nothing: an error, not a silent no-op
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: groupedMultinomial, group: g, label: y, candidates: [x], conditioning: {fields: [x, x2]}, pairs: {among: [x2]}}"));
        // a null / object member is a configuration error (not an escaping UnsupportedOperationException)
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: groupedMultinomial, group: g, label: y, candidates: [x], conditioning: {fields: [x, x2]}, pairs: {fields: [[x, null]]}}"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: groupedMultinomial, group: g, label: y, candidates: [x], conditioning: {fields: [x, x2]}, pairs: {fields: 'x', among: ['x*']}}"));
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
            final boolean expected = floor == null || floor < gain;
            Assertions.assertEquals(expected, xRaw.get("passed"), pass);
            Assertions.assertEquals(expected ? List.of("x") : List.of(), result.summary().get("passedColumns"), pass);
            Assertions.assertEquals(expected ? 1L : 0L, result.summary().get("nPassed"), pass);
            final String rule = (String) result.summary().get("passRule");
            Assertions.assertEquals(floor == null ? "est_gain > threshold" : "est_gain > max(threshold, " + floor + ")", rule);
            Assertions.assertEquals(floor, result.summary().get("minGain"), pass);
            final com.google.gson.JsonObject selection = ScreenReport.selection(spec, result);
            Assertions.assertEquals(rule, selection.get("passRule").getAsString());
            Assertions.assertEquals(floor == null, selection.get("minGain").isJsonNull(), pass);
            Assertions.assertEquals(expected ? 1 : 0, selection.getAsJsonArray("passed").size(), pass);
            Assertions.assertEquals(floor != null, ScreenReport.describe(spec).contains("pass=" + rule), pass);
        }
        // with conditioning the floor applies to partial_gain, and it composes with the period agreement
        final ScreenSpec both = spec("{family: groupedMultinomial, group: g, label: y, time: t, candidates: [x], periods: year, pass: {minPeriodsAgree: 0.66, minGain: 1e-5}}");
        Assertions.assertEquals("partial_gain > max(threshold, 1.0E-5) and partial_periods_agree >= 0.66 * partial_n_periods", ScreenReport.passRule(both, true));
        Assertions.assertEquals("est_gain > max(threshold, 1.0E-5) and periods_agree >= 0.66 * n_periods", ScreenReport.passRule(both, false));
        // a floor never lowers the cut below the placebo threshold
        final ScreenSpec low = spec("{family: groupedMultinomial, group: g, label: y, candidates: [x], pass: {minGain: 1e-12}}");
        Assertions.assertEquals(0.5, low.gainCut(0.5));
        Assertions.assertEquals(1e-12, low.gainCut(1e-13));
        Assertions.assertTrue(Double.isNaN(low.gainCut(Double.NaN)));
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
