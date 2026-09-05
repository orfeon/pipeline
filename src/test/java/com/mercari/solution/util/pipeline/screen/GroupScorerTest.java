package com.mercari.solution.util.pipeline.screen;

import com.google.gson.JsonParser;
import com.mercari.solution.module.Schema;
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
        Assertions.assertEquals(GroupScorer.Skip.NONE, scorer.score(List.of(row("a", 1, 1, Double.NaN, 3), row("a", 1, 0, Double.NaN, 1), row("a", 1, 0, Double.NaN, 2)), "a", acc));
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
        Assertions.assertEquals(GroupScorer.Skip.INVALID_BASELINE, new GroupScorer(spec).score(List.of(row("b", 1, 1, 0, 1), row("b", 1, 0, 4, 0)), "b", skipped));
        Assertions.assertEquals(1, skipped.get(ScoreAccumulator.BOOKKEEPING_KEY).getTotal()[ScoreAccumulator.UNITS_SKIPPED]);
        // a group without a positive label is skipped
        final Map<Integer, ScoreAccumulator> noPositive = new HashMap<>();
        Assertions.assertEquals(GroupScorer.Skip.NO_POSITIVE_LABEL, new GroupScorer(spec).score(List.of(row("c", 1, 0, 2, 1), row("c", 1, 0, 4, 0)), "c", noPositive));
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
        Assertions.assertEquals(ScreenMath.chiSquare1Quantile(0.99) / 40, (Double) xRaw.get("threshold"), 1e-12);
        Assertions.assertEquals(ScreenMath.chiSquare1Quantile(0.99) / 40, (Double) result.summary().get("thresholdTheoretical"), 1e-12);
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
        final ScreenSpec.Lineage lineage = ScreenSpec.Lineage.fromManifest(manifest);
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
        final ScreenSpec.Lineage fromManifest = ScreenSpec.Lineage.fromManifest(manifest);
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
                .resolve(schema, ScreenSpec.Lineage.fromSchema(schema));
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
    public void testExplicitTransformsSurviveGroupDefault() {
        final String manifest = "{timeField: t, roles: {group: {name: g, column: g}, label: {name: y, column: y}}}";
        final ScreenSpec.Lineage lineage = ScreenSpec.Lineage.fromManifest(manifest);
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
        Assertions.assertThrows(IllegalArgumentException.class, () -> ScreenSpec.Lineage.fromManifest("manifests/missing.json"));
        // the list form of conditioning must not silently disable the partial test
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: binomial, label: y, candidates: [x], conditioning: []}"));
        // the baseline is the model offset: it is a role field for conditioning like it is for candidates
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: binomial, label: y, baseline: b, candidates: [x], conditioning: {fields: [b]}}"));
    }
}
