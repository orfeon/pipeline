package com.mercari.solution.util.pipeline.evaluation;

import com.google.gson.JsonParser;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.feature.FeatureLineage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class EvaluationSpecTest {

    private static EvaluationSpec parse(final String json) {
        return EvaluationSpec.parse(JsonParser.parseString(json).getAsJsonObject());
    }

    private static String error(final String json) {
        return Assertions.assertThrows(IllegalArgumentException.class, () -> parse(json).resolve(EvaluationScorerTest.SCHEMA, null)).getMessage();
    }

    private static final String OK = "{family: groupedMultinomial, group: g, label: y, baseline: b, time: t, predictions: [{name: A, prob: qa}], " + EvaluationScorerTest.SPLITS + "}";

    @Test
    public void testParseAndLayout() {
        final EvaluationSpec s = parse("{family: groupedMultinomial, group: g, label: {expr: 'y > 0 ? 1 : 0'}, baseline: {field: b, form: inverseShare}, time: {field: t}, "
                + "predictions: [{name: A, prob: qa}, {name: S, score: s, offset: b, offsetScale: log, temperature: 2}], " + EvaluationScorerTest.SPLITS
                + ", bootstrap: {samples: 10, seed: 5, unit: region}, utility: u, calibration: [{by: divergence, bins: 5}, {type: reliability, by: field, field: u, edges: [1, 2]}, {type: edge, thresholds: [1.5]}], slices: [region, {field: t, bucket: month}]}")
                .resolve(EvaluationScorerTest.SCHEMA, null);
        Assertions.assertEquals("inverseShare", s.baselineForm);
        Assertions.assertEquals(List.of("qa", "s", "b", "u"), s.rowColumns);
        Assertions.assertEquals(0, s.predictions.get(0).offset);
        Assertions.assertEquals(1, s.predictions.get(1).offset);
        Assertions.assertEquals(2d, s.predictions.get(1).temperature);
        Assertions.assertEquals("logProb", s.predictions.get(1).offsetForm);
        Assertions.assertEquals(3, s.tables.get(1).fieldIndex);
        Assertions.assertEquals(3, s.utilityIndex);
        Assertions.assertEquals("region", s.bootstrapUnit);
        Assertions.assertEquals(List.of("baseline", "A", "S"), s.predictionNames());
        Assertions.assertEquals(List.of("valid", "test"), s.splitNames());
        Assertions.assertTrue(s.split("valid").isSelection());
        Assertions.assertEquals("valid", s.splitOf(java.time.Instant.parse("2024-03-01T00:00:00Z").toEpochMilli()));
        Assertions.assertEquals("test", s.splitOf(java.time.Instant.parse("2024-12-31T23:59:59.999Z").toEpochMilli()));
        Assertions.assertNull(s.splitOf(java.time.Instant.parse("2025-01-01T00:00:00Z").toEpochMilli()));
        Assertions.assertEquals("t/month", s.slices.get(1).name());
        Assertions.assertEquals("timestamp", s.slices.get(1).fieldType);
        Assertions.assertTrue(s.hasQuantileTables());
        Assertions.assertEquals(16, s.parametersHash.length());
        Assertions.assertEquals(s.parametersHash, parse("{family: groupedMultinomial, group: g, label: {expr: 'y > 0 ? 1 : 0'}, baseline: {field: b, form: inverseShare}, time: {field: t}, "
                + "predictions: [{name: A, prob: qa}, {name: S, score: s, offset: b, offsetScale: log, temperature: 2}], " + EvaluationScorerTest.SPLITS
                + ", bootstrap: {samples: 10, seed: 5, unit: region}, utility: u, calibration: [{by: divergence, bins: 5}, {type: reliability, by: field, field: u, edges: [1, 2]}, {type: edge, thresholds: [1.5]}], slices: [region, {field: t, bucket: month}], manifest: 'gs://x/manifest.json'}").parametersHash);
    }

    @Test
    public void testInvalidPolicy() {
        Assertions.assertTrue(error("{group: g, label: y, baseline: {field: b, invalid: drop}, time: t, predictions: [{name: A, prob: qa}], " + EvaluationScorerTest.SPLITS + "}").contains("baseline.invalid 'drop' is unknown"));
        Assertions.assertTrue(error("{group: g, label: y, baseline: b, time: t, predictions: [{name: A, prob: qa, invalid: skip}], " + EvaluationScorerTest.SPLITS + "}").contains("predictions[0].invalid 'skip' is unknown"));
        final EvaluationSpec s = parse("{group: g, label: y, baseline: {field: b, form: inverseShare, invalid: dropRow}, time: t, predictions: [{name: A, prob: qa}, {name: B, prob: qb, invalid: dropRow}], " + EvaluationScorerTest.SPLITS + "}").resolve(EvaluationScorerTest.SCHEMA, null);
        Assertions.assertTrue(s.baselineDropsRows());
        Assertions.assertFalse(s.predictions.get(0).dropsRows());
        Assertions.assertTrue(s.predictions.get(1).dropsRows());
        Assertions.assertTrue(EvaluationReport.describe(s).contains("b:inverseShare[dropRow]"));
        Assertions.assertTrue(EvaluationReport.describe(s).contains("B=qb:prob[dropRow]"));
        final EvaluationSpec plain = parse(OK).resolve(EvaluationScorerTest.SCHEMA, null);
        Assertions.assertFalse(plain.dropsRows());
    }

    @Test
    public void testSplitRules() {
        Assertions.assertTrue(error("{group: g, label: y, baseline: b, time: t, predictions: [{name: A, prob: qa}]}").contains("splits is required"));
        Assertions.assertTrue(error("{group: g, label: y, baseline: b, time: t, predictions: [{name: A, prob: qa}], splits: {valid: {from: '2024-01-01', to: '2024-06-30', role: selection}}}").contains("role report"));
        Assertions.assertTrue(error("{group: g, label: y, baseline: b, time: t, predictions: [{name: A, prob: qa}], splits: {valid: {from: '2024-01-01', to: '2024-06-30', role: selection}, test: {from: '2024-06-30', to: '2024-12-31', role: report}}}").contains("overlap"));
        Assertions.assertTrue(error("{group: g, label: y, baseline: b, time: t, predictions: [{name: A, prob: qa}], splits: {valid: {from: '2024-07-01', to: '2024-12-31', role: selection}, test: {from: '2024-01-01', to: '2024-06-30', role: report}}}").contains("must end before"));
        Assertions.assertTrue(error("{group: g, label: y, baseline: b, time: t, predictions: [{name: A, prob: qa}], splits: {test: {from: '2024-01-01', to: '2024-06-30', role: holdout}}}").contains("role must be one of"));
        Assertions.assertTrue(error("{group: g, label: y, baseline: b, predictions: [{name: A, prob: qa}], " + EvaluationScorerTest.SPLITS + "}").contains("time.field is not set"));
        // a range-less split takes every row, with or without a time (a bounded source has no event time)
        final EvaluationSpec rangeless = parse("{group: g, label: y, baseline: b, predictions: [{name: A, prob: qa}], splits: {all: {role: report}}}").resolve(EvaluationScorerTest.SCHEMA, null);
        Assertions.assertEquals("all", rangeless.splitOf(EvaluationRow.NO_TIME));
        Assertions.assertEquals("all", rangeless.splitOf(0L));
        final EvaluationSpec ranged = parse(OK).resolve(EvaluationScorerTest.SCHEMA, null);
        Assertions.assertNull(ranged.splitOf(EvaluationRow.NO_TIME));
        Assertions.assertTrue(error("{group: g, label: y, baseline: b, time: t, predictions: [{name: A, prob: qa}], splits: {valid: {from: 'yesterday', role: selection}, test: {role: report}}}").contains("ISO-8601"));
        // by column: no ordering check, the summary reports the observed ranges
        final EvaluationSpec byColumn = parse("{group: g, label: y, baseline: b, predictions: [{name: A, prob: qa}], splits: {field: region, roles: {east: selection, west: report}}}").resolve(EvaluationScorerTest.SCHEMA, null);
        Assertions.assertEquals("region", byColumn.splitField);
        Assertions.assertEquals(List.of("east", "west"), byColumn.splitNames());
        Assertions.assertTrue(error("{group: g, label: y, baseline: b, predictions: [{name: A, prob: qa}], splits: {field: region}}").contains("splits.roles is required"));
    }

    @Test
    public void testPredictionAndFamilyRules() {
        Assertions.assertTrue(error("{group: g, label: y, baseline: b, time: t, predictions: [], " + EvaluationScorerTest.SPLITS + "}").contains("at least one prediction"));
        Assertions.assertTrue(error("{group: g, label: y, baseline: b, time: t, predictions: [{name: baseline, prob: qa}], " + EvaluationScorerTest.SPLITS + "}").contains("reserved"));
        Assertions.assertTrue(error("{group: g, label: y, baseline: b, time: t, predictions: [{name: A, prob: qa}, {name: A, prob: qb}], " + EvaluationScorerTest.SPLITS + "}").contains("duplicated"));
        Assertions.assertTrue(error("{group: g, label: y, baseline: b, time: t, predictions: [{name: A, prob: qa, score: s}], " + EvaluationScorerTest.SPLITS + "}").contains("not both"));
        Assertions.assertTrue(error("{group: g, label: y, baseline: b, time: t, predictions: [{name: A, score: s, temperature: 0}], " + EvaluationScorerTest.SPLITS + "}").contains("temperature must be > 0"));
        Assertions.assertTrue(error("{group: g, label: y, baseline: b, time: t, predictions: [{name: A, score: s, offset: qa, offsetScale: exp}], " + EvaluationScorerTest.SPLITS + "}").contains("offsetScale 'exp' is unknown"));
        Assertions.assertTrue(error("{group: g, label: y, baseline: b, time: t, predictions: [{name: A, score: s, offsetScale: log}], " + EvaluationScorerTest.SPLITS + "}").contains("offsetScale needs an offset"));
        // offset as {field, form}: the probability forms; offsetScale is the legacy spelling of a field-name offset
        Assertions.assertTrue(error("{group: g, label: y, baseline: b, time: t, predictions: [{name: A, score: s, offset: {field: qa, form: rate}}], " + EvaluationScorerTest.SPLITS + "}").contains("offset.form 'rate' is not valid"));
        Assertions.assertTrue(error("{group: g, label: y, baseline: b, time: t, predictions: [{name: A, score: s, offset: {form: prob}}], " + EvaluationScorerTest.SPLITS + "}").contains("offset.field is required"));
        Assertions.assertTrue(error("{group: g, label: y, baseline: b, time: t, predictions: [{name: A, score: s, offset: {field: qa}, offsetScale: log}], " + EvaluationScorerTest.SPLITS + "}").contains("offsetScale applies to a field-name offset"));
        Assertions.assertTrue(error("{group: g, label: y, baseline: b, time: t, predictions: [{name: A, score: s, offset: [qa]}], " + EvaluationScorerTest.SPLITS + "}").contains("offset must be a field name or an object"));
        Assertions.assertTrue(error("{group: g, label: y, baseline: b, time: t, predictions: [{name: A, prob: qa, offsetScale: log}], " + EvaluationScorerTest.SPLITS + "}").contains("apply to a score set only"));
        final EvaluationSpec forms = parse("{group: g, label: y, baseline: b, time: t, predictions: [{name: A, score: s, offset: {field: qa, form: inverseShare}}, {name: B, score: s, offset: qb, offsetScale: log}, {name: C, score: s, offset: {field: u}}], " + EvaluationScorerTest.SPLITS + "}").resolve(EvaluationScorerTest.SCHEMA, null);
        Assertions.assertEquals("inverseShare", forms.predictions.get(0).offsetForm);
        Assertions.assertEquals("logProb", forms.predictions.get(1).offsetForm);
        Assertions.assertEquals("prob", forms.predictions.get(2).offsetForm);
        Assertions.assertEquals("qa", forms.predictions.get(0).offsetField);
        Assertions.assertTrue(error("{group: g, label: y, baseline: b, time: t, predictions: [{name: A, field: qa, form: rate}], " + EvaluationScorerTest.SPLITS + "}").contains("not valid for family"));
        Assertions.assertTrue(error("{family: gaussian, label: y, baseline: b, time: t, predictions: [{name: A, prob: qa}], " + EvaluationScorerTest.SPLITS + "}").contains("not supported"));
        Assertions.assertTrue(error("{family: groupedMultinomial, label: y, baseline: b, time: t, predictions: [{name: A, prob: qa}], " + EvaluationScorerTest.SPLITS + "}").contains("group is required"));
        Assertions.assertTrue(error("{family: binomial, label: y, baseline: b, time: t, predictions: [{name: A, score: s}], " + EvaluationScorerTest.SPLITS + "}").contains("needs family groupedMultinomial"));
        // the family, not the group, decides: a score set on a binomial spec with a group would softmax over one row
        Assertions.assertTrue(error("{family: binomial, group: g, label: y, baseline: b, time: t, predictions: [{name: A, score: s}], " + EvaluationScorerTest.SPLITS + "}").contains("needs family groupedMultinomial"));
        Assertions.assertTrue(error("{family: binomial, group: g, label: y, baseline: {field: b, form: inverseShare}, time: t, predictions: [{name: A, prob: qa}], " + EvaluationScorerTest.SPLITS + "}").contains("inverseShare needs family"));
        Assertions.assertTrue(error("{group: g, label: y, baseline: b, time: t, predictions: [{name: A, prob: qa, form: logProb}], " + EvaluationScorerTest.SPLITS + "}").contains("either prob or field + form"));
        Assertions.assertTrue(error("{group: g, label: y, baseline: b, time: t, predictions: [{name: A, prob: missing}], " + EvaluationScorerTest.SPLITS + "}").contains("not an input field"));
        Assertions.assertTrue(error("{group: g, label: y, baseline: b, time: t, predictions: [{name: A, prob: region}], " + EvaluationScorerTest.SPLITS + "}").contains("must be numeric"));
    }

    @Test
    public void testTableSliceAndBootstrapRules() {
        Assertions.assertTrue(error(OK.replace("}}}", "}}, calibration: [{type: histogram}]}")).contains("type 'histogram'"));
        Assertions.assertTrue(error(OK.replace("}}}", "}}, calibration: [{by: field}]}")).contains("field is required"));
        Assertions.assertTrue(error(OK.replace("}}}", "}}, calibration: [{by: field, field: u, edges: [2, 1]}]}")).contains("strictly ascending"));
        Assertions.assertTrue(error(OK.replace("}}}", "}}, calibration: [{by: prediction, bins: 1}]}")).contains("bins must be"));
        // edges close on the left by default, right on request; k belongs to quantile tables
        final EvaluationSpec closed = parse(OK.replace("}}}", "}}, calibration: [{by: field, field: u, edges: [1, 2]}, {by: field, field: u, edges: [1, 2], closed: right}, {by: prediction, k: 4000}]}")).resolve(EvaluationScorerTest.SCHEMA, null);
        Assertions.assertTrue(closed.tables.get(0).closedLeft);
        Assertions.assertFalse(closed.tables.get(1).closedLeft);
        Assertions.assertEquals(SketchAccumulator.K, closed.tables.get(0).k);
        Assertions.assertEquals(4000, closed.tables.get(2).k);
        Assertions.assertTrue(error(OK.replace("}}}", "}}, calibration: [{by: field, field: u, edges: [1], closed: both}]}")).contains("closed 'both'"));
        Assertions.assertTrue(error(OK.replace("}}}", "}}, calibration: [{by: prediction, closed: left}]}")).contains("closed applies to by: field"));
        Assertions.assertTrue(error(OK.replace("}}}", "}}, calibration: [{by: field, field: u, edges: [1], k: 500}]}")).contains("k applies to quantile"));
        Assertions.assertTrue(error(OK.replace("}}}", "}}, calibration: [{by: divergence, k: 4}]}")).contains("k must be in"));
        Assertions.assertTrue(closed.tables.get(0).binsClosedLeft());
        Assertions.assertFalse(closed.tables.get(2).binsClosedLeft(), "quantile bins are right-closed");
        Assertions.assertTrue(error(OK.replace("}}}", "}}, calibration: [{by: field, field: u, edges: [1], closed: [right]}]}")).contains("closed '[\"right\"]' is unknown"));
        Assertions.assertTrue(error(OK.replace("}}}", "}}, calibration: [{type: edge, thresholds: [1.5], closed: right}]}")).contains("closed applies to by: field"));
        Assertions.assertTrue(error(OK.replace("}}}", "}}, calibration: [{type: edge, thresholds: [1.5], k: 4000}]}")).contains("k applies to quantile"));
        Assertions.assertEquals(SketchAccumulator.K, parse(OK.replace("}}}", "}}, calibration: [{by: prediction, closed: null}]}")).tables.get(0).k, "an explicit null reads as absent");
        Assertions.assertTrue(error(OK.replace("}}}", "}}, calibration: [{type: edge}]}")).contains("thresholds is required"));
        // rows output: needs rowId; true = the selection splits; named splits must exist
        Assertions.assertTrue(error(OK.replace("}}}", "}}, rows: true}")).contains("needs rowId"));
        Assertions.assertEquals(List.of("valid"), parse(OK.replace("}}}", "}}, rowId: [g], rows: true}")).resolve(EvaluationScorerTest.SCHEMA, null).rowSplits);
        Assertions.assertEquals(List.of("test"), parse(OK.replace("}}}", "}}, rowId: [g], rows: {splits: [test]}}")).resolve(EvaluationScorerTest.SCHEMA, null).rowSplits);
        Assertions.assertNull(parse(OK.replace("}}}", "}}, rowId: [g], rows: false}")).resolve(EvaluationScorerTest.SCHEMA, null).rowSplits);
        Assertions.assertTrue(error(OK.replace("}}}", "}}, rowId: [g], rows: {splits: [later]}}")).contains("not a declared split"));
        Assertions.assertTrue(error(OK.replace("}}}", "}}, rowId: [g], rows: [valid]}")).contains("rows must be true"));
        Assertions.assertTrue(error(OK.replace("}}}", "}}, rowId: [g], rows: {splits: [{name: valid}]}}")).contains("rows.splits must be a list of strings"));
        // resolving twice keeps the selection splits once
        final EvaluationSpec twice = parse(OK.replace("}}}", "}}, rowId: [g], rows: true}"));
        Assertions.assertEquals(List.of("valid"), twice.resolve(EvaluationScorerTest.SCHEMA, null).resolve(EvaluationScorerTest.SCHEMA, null).rowSplits);
        // the rows output does not change the parameters hash (an output selection, like output.calibration)
        Assertions.assertEquals(parse(OK.replace("}}}", "}}, rowId: [g]}")).parametersHash, parse(OK.replace("}}}", "}}, rowId: [g], rows: true}")).parametersHash);
        Assertions.assertTrue(error(OK.replace("}}}", "}}, slices: [{field: region, bucket: decade}]}")).contains("bucket 'decade'"));
        Assertions.assertTrue(error(OK.replace("}}}", "}}, slices: [{field: u, bucket: month}]}")).contains("needs a timestamp"));
        Assertions.assertTrue(error(OK.replace("}}}", "}}, slices: [{field: y, bucket: month}]}")).contains("needs a timestamp"));   // int64 would read as micros
        Assertions.assertTrue(error(OK.replace("time: t", "time: y")).contains("time.field 'y' must be"));
        Assertions.assertTrue(error(OK.replace("}}}", "}}, bootstrap: {samples: 20000}}")).contains("bootstrap.samples"));
        Assertions.assertEquals(0, parse(OK.replace("}}}", "}}, bootstrap: false}")).bootstrapSamples);
    }

    @Test
    public void testRoleDefaultsFromLineage() {
        final Schema schema = Schema.builder()
                .withField(Schema.Field.of("g", Schema.FieldType.STRING).withOptions(java.util.Map.of("feature.scope", "input", "feature.role", "group")))
                .withField(Schema.Field.of("y", Schema.FieldType.INT64).withOptions(java.util.Map.of("feature.scope", "input", "feature.role", "label")))
                .withField(Schema.Field.of("b", Schema.FieldType.FLOAT64).withOptions(java.util.Map.of("feature.scope", "input", "feature.role", "baseline")))
                .withField(Schema.Field.of("t", Schema.FieldType.TIMESTAMP).withOptions(java.util.Map.of("feature.scope", "input", "feature.role", "time")))
                .withField("qa", Schema.FieldType.FLOAT64)
                .build();
        final EvaluationSpec s = parse("{predictions: [{name: A, prob: qa}], " + EvaluationScorerTest.SPLITS + "}").resolve(schema, FeatureLineage.fromSchema(schema));
        Assertions.assertEquals("g", s.group);
        Assertions.assertEquals("y", s.labelField);
        Assertions.assertEquals("b", s.baselineField);
        Assertions.assertEquals("prob", s.baselineForm);
        Assertions.assertEquals("t", s.timeField);
        Assertions.assertEquals(4, s.notes.size());
    }

    @Test
    public void testFitRules() {
        final EvaluationSpec s = parse(OK.replace("}}}", "}}, calibration: [{type: temperature, fitOn: valid, grid: [0.5, 2, 4]}, {type: blend, fitOn: valid, of: [A], l2: 0.01, maxIter: 5}, {by: prediction}], output: {calibration: 'target/cal.json'}}"))
                .resolve(EvaluationScorerTest.SCHEMA, null);
        Assertions.assertEquals(2, s.fits.size());
        Assertions.assertEquals(1, s.tables.size());
        Assertions.assertArrayEquals(new double[]{0.5, 1, 1.5, 2}, s.fits.get(0).grid(), 1e-12);
        Assertions.assertEquals(0.01, s.fits.get(1).l2);
        Assertions.assertEquals(0d, parse(OK.replace("}}}", "}}, calibration: [{type: blend, fitOn: valid}]}")).fits.get(0).l2, "a blend is an unpenalised MLE by default");
        Assertions.assertEquals(List.of("baseline", "A", "A@T", "A@blend"), s.predictionNames());
        Assertions.assertEquals(List.of(1), s.derivedOf(0));
        Assertions.assertEquals(List.of(2), s.derivedOf(1));
        Assertions.assertEquals("target/cal.json", s.calibrationUri);
        Assertions.assertEquals(s.parametersHash, parse(OK.replace("}}}", "}}, calibration: [{type: temperature, fitOn: valid, grid: [0.5, 2, 4]}, {type: blend, fitOn: valid, of: [A], l2: 0.01, maxIter: 5}, {by: prediction}]}")).parametersHash);
        Assertions.assertTrue(error(OK.replace("}}}", "}}, calibration: [{type: temperature}]}")).contains("fitOn is required"));
        Assertions.assertTrue(error(OK.replace("}}}", "}}, calibration: [{type: temperature, fitOn: test}]}")).contains("never on the report split"));
        Assertions.assertTrue(error(OK.replace("}}}", "}}, calibration: [{type: temperature, fitOn: holdout}]}")).contains("not a declared split"));
        Assertions.assertTrue(error(OK.replace("}}}", "}}, calibration: [{type: blend, fitOn: valid, of: [Z]}]}")).contains("not a declared prediction set"));
        Assertions.assertTrue(error(OK.replace("}}}", "}}, calibration: [{type: blend, fitOn: valid}, {type: blend, fitOn: valid}]}")).contains("declared twice"));
        Assertions.assertTrue(error(OK.replace("}}}", "}}, calibration: [{type: temperature, fitOn: valid, grid: [0, 2, 4]}]}")).contains("grid must be"));
        Assertions.assertTrue(error(OK.replace("}}}", "}}, calibration: [{type: temperature, fitOn: valid, grid: [1, 1, 4]}]}")).contains("grid must be"));
        Assertions.assertTrue(error(OK.replace("}}}", "}}, calibration: [{type: temperature, fitOn: valid, grid: [0.5, 2, 3.5]}]}")).contains("grid must be"));
        // a declared set may not take a derived set's name
        Assertions.assertTrue(error("{group: g, label: y, baseline: b, time: t, predictions: [{name: A, prob: qa}, {name: 'A@T', prob: qb}], " + EvaluationScorerTest.SPLITS + ", calibration: [{type: temperature, fitOn: valid, of: [A]}]}").contains("collides with a declared prediction set"));
        Assertions.assertTrue(error(OK.replace("}}}", "}}, calibration: [{type: blend, fitOn: valid, maxIter: 0}]}")).contains("maxIter"));
        // a blend needs an offset: the baseline, or the score set's own
        Assertions.assertTrue(error("{group: g, label: y, time: t, predictions: [{name: A, prob: qa}], " + EvaluationScorerTest.SPLITS + ", calibration: [{type: blend, fitOn: valid}]}").contains("needs an offset"));
        final EvaluationSpec own = parse("{group: g, label: y, time: t, predictions: [{name: S, score: s, offset: qa, offsetScale: log}], " + EvaluationScorerTest.SPLITS + ", calibration: [{type: blend, fitOn: valid}]}").resolve(EvaluationScorerTest.SCHEMA, null);
        Assertions.assertEquals(List.of("baseline", "S", "S@blend"), own.predictionNames());
        // fix: coefficients held at a value; as: the derived set's suffix (two blends of one set need distinct suffixes)
        final EvaluationSpec fixed = parse(OK.replace("}}}", "}}, calibration: [{type: blend, fitOn: valid, fix: {b: 1}, as: rebase}, {type: blend, fitOn: valid}, {type: temperature, fitOn: valid, as: temp}]}")).resolve(EvaluationScorerTest.SCHEMA, null);
        Assertions.assertEquals(List.of("baseline", "A", "A@rebase", "A@blend", "A@temp"), fixed.predictionNames());
        Assertions.assertEquals(1d, fixed.fits.get(0).fix.get("b"));
        Assertions.assertTrue(fixed.fits.get(0).fixes("b"));
        Assertions.assertFalse(fixed.fits.get(0).fixes("a"));
        Assertions.assertTrue(EvaluationReport.describe(fixed).contains("fix b=1.0"));
        Assertions.assertNotEquals(fixed.parametersHash, parse(OK.replace("}}}", "}}, calibration: [{type: blend, fitOn: valid, fix: {b: 2}, as: rebase}, {type: blend, fitOn: valid}, {type: temperature, fitOn: valid, as: temp}]}")).parametersHash);
        Assertions.assertTrue(error(OK.replace("}}}", "}}, calibration: [{type: blend, fitOn: valid, fix: {c: 1}}]}")).contains("fix.c is not a blend coefficient"));
        Assertions.assertTrue(error(OK.replace("}}}", "}}, calibration: [{type: blend, fitOn: valid, fix: {b: x}}]}")).contains("fix.b must be a finite number"));
        Assertions.assertTrue(error(OK.replace("}}}", "}}, calibration: [{type: blend, fitOn: valid, fix: {a: 1, b: 1}}]}")).contains("holds every coefficient"));
        Assertions.assertTrue(error(OK.replace("}}}", "}}, calibration: [{type: blend, fitOn: valid, fix: {intercept: 0}}]}")).contains("grouped blend has no intercept"));
        Assertions.assertTrue(error(OK.replace("}}}", "}}, calibration: [{type: temperature, fitOn: valid, fix: {b: 1}}]}")).contains("fix applies to type blend"));
        Assertions.assertTrue(error(OK.replace("}}}", "}}, calibration: [{type: blend, fitOn: valid, as: 'x@y'}]}")).contains("as must be a non-blank suffix"));
        Assertions.assertTrue(error(OK.replace("}}}", "}}, calibration: [{type: blend, fitOn: valid, as: T}, {type: temperature, fitOn: valid}]}")).contains("declared twice"));
        final EvaluationSpec binomial = parse("{family: binomial, label: y, baseline: b, time: t, predictions: [{name: A, prob: qa}], " + EvaluationScorerTest.SPLITS + ", calibration: [{type: blend, fitOn: valid, fix: {intercept: 0, b: 1}}]}").resolve(EvaluationScorerTest.SCHEMA, null);
        Assertions.assertEquals(2, binomial.fits.get(0).fix.size());
    }

    @Test
    public void testDiscoveryRules() {
        final EvaluationSpec s = parse(OK.replace("}}}", "}}, sliceDiscovery: {dimensions: [region, {field: u, bins: 4}], discoverOn: valid, confirmOn: test, of: [A], minSupport: 50}}")).resolve(EvaluationScorerTest.SCHEMA, null);
        Assertions.assertTrue(s.hasDiscovery());
        Assertions.assertEquals(2, s.discovery.maxDepth);
        Assertions.assertEquals(50, s.discovery.minSupport);
        Assertions.assertEquals("u/q4", s.discovery.dimensions.get(1).name());
        Assertions.assertEquals(List.of("region"), s.dimColumns);
        Assertions.assertEquals(List.of(0), s.discovery.sets);
        Assertions.assertTrue(s.discovery.hasNumeric());
        Assertions.assertTrue(error(OK.replace("}}}", "}}, sliceDiscovery: {dimensions: [region], discoverOn: valid, confirmOn: valid}}")).contains("must be different"));
        Assertions.assertTrue(error(OK.replace("}}}", "}}, sliceDiscovery: {dimensions: [region], discoverOn: test, confirmOn: valid}}")).contains("discovered on a selection split"));
        Assertions.assertTrue(error(OK.replace("}}}", "}}, sliceDiscovery: {dimensions: [region], discoverOn: valid, confirmOn: later}}")).contains("not a declared split"));
        Assertions.assertTrue(error(OK.replace("}}}", "}}, sliceDiscovery: {discoverOn: valid, confirmOn: test}}")).contains("dimensions is required"));
        Assertions.assertTrue(error(OK.replace("}}}", "}}, sliceDiscovery: {dimensions: [u], discoverOn: valid, confirmOn: test}}")).contains("give it bins"));
        // the utility metric needs the utility field and runs once (it does not depend on the set)
        Assertions.assertTrue(error(OK.replace("}}}", "}}, sliceDiscovery: {dimensions: [region], discoverOn: valid, confirmOn: test, metric: utility}}")).contains("needs utility.field"));
        final EvaluationSpec utility = parse("{group: g, label: y, baseline: b, time: t, predictions: [{name: A, prob: qa}, {name: B, prob: qb}], " + EvaluationScorerTest.SPLITS
                + ", utility: u, sliceDiscovery: {dimensions: [region], discoverOn: valid, confirmOn: test, metric: utility}}").resolve(EvaluationScorerTest.SCHEMA, null);
        Assertions.assertEquals(List.of(0), utility.discovery.sets);
        Assertions.assertTrue(utility.notes.toString().contains("runs once, reported under A"), utility.notes.toString());
        Assertions.assertTrue(error(OK.replace("}}}", "}}, sliceDiscovery: {dimensions: [{field: region, bins: 3}], discoverOn: valid, confirmOn: test}}")).contains("not numeric"));
        Assertions.assertTrue(error(OK.replace("}}}", "}}, sliceDiscovery: {dimensions: [missing], discoverOn: valid, confirmOn: test}}")).contains("not an input field"));
        Assertions.assertTrue(error(OK.replace("}}}", "}}, sliceDiscovery: {dimensions: [region], discoverOn: valid, confirmOn: test, of: [Z]}}")).contains("not a compared prediction set"));
        Assertions.assertTrue(error(OK.replace("}}}", "}}, sliceDiscovery: {dimensions: [region], discoverOn: valid, confirmOn: test, maxDepth: 4}}")).contains("maxDepth"));
        Assertions.assertTrue(error(OK.replace("}}}", "}}, sliceDiscovery: {dimensions: [region], discoverOn: valid, confirmOn: test, metric: auc}}")).contains("metric 'auc'"));
        Assertions.assertTrue(error("{family: binomial, label: y, time: t, predictions: [{name: A, prob: qa}], " + EvaluationScorerTest.SPLITS + ", sliceDiscovery: {dimensions: [region], discoverOn: valid, confirmOn: test}}").contains("needs a baseline"));
        Assertions.assertTrue(error("{family: binomial, label: y, baseline: b, time: t, predictions: [{name: A, prob: qa}], " + EvaluationScorerTest.SPLITS + ", sliceDiscovery: {dimensions: [region], discoverOn: valid, confirmOn: test, metric: hitAt1}}").contains("hitAt1 needs family"));
        // a mis-typed numeric dimension is reported once, not again as a non-numeric column
        Assertions.assertFalse(error(OK.replace("}}}", "}}, sliceDiscovery: {dimensions: [{field: region, bins: 3}], discoverOn: valid, confirmOn: test}}")).contains("must be numeric"));
    }
}
