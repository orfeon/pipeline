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
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: binomial, label: y, transforms: [rank], candidates: [x]}"));
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
