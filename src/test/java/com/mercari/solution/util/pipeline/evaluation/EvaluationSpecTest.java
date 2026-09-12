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
        Assertions.assertEquals("log", s.predictions.get(1).offsetScale);
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
    public void testSplitRules() {
        Assertions.assertTrue(error("{group: g, label: y, baseline: b, time: t, predictions: [{name: A, prob: qa}]}").contains("splits is required"));
        Assertions.assertTrue(error("{group: g, label: y, baseline: b, time: t, predictions: [{name: A, prob: qa}], splits: {valid: {from: '2024-01-01', to: '2024-06-30', role: selection}}}").contains("role report"));
        Assertions.assertTrue(error("{group: g, label: y, baseline: b, time: t, predictions: [{name: A, prob: qa}], splits: {valid: {from: '2024-01-01', to: '2024-06-30', role: selection}, test: {from: '2024-06-30', to: '2024-12-31', role: report}}}").contains("overlap"));
        Assertions.assertTrue(error("{group: g, label: y, baseline: b, time: t, predictions: [{name: A, prob: qa}], splits: {valid: {from: '2024-07-01', to: '2024-12-31', role: selection}, test: {from: '2024-01-01', to: '2024-06-30', role: report}}}").contains("must end before"));
        Assertions.assertTrue(error("{group: g, label: y, baseline: b, time: t, predictions: [{name: A, prob: qa}], splits: {test: {from: '2024-01-01', to: '2024-06-30', role: holdout}}}").contains("role must be one of"));
        Assertions.assertTrue(error("{group: g, label: y, baseline: b, predictions: [{name: A, prob: qa}], " + EvaluationScorerTest.SPLITS + "}").contains("time.field is not set"));
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
        Assertions.assertTrue(error("{group: g, label: y, baseline: b, time: t, predictions: [{name: A, score: s, offsetScale: exp}], " + EvaluationScorerTest.SPLITS + "}").contains("offsetScale"));
        Assertions.assertTrue(error("{group: g, label: y, baseline: b, time: t, predictions: [{name: A, field: qa, form: rate}], " + EvaluationScorerTest.SPLITS + "}").contains("not valid for family"));
        Assertions.assertTrue(error("{family: gaussian, label: y, baseline: b, time: t, predictions: [{name: A, prob: qa}], " + EvaluationScorerTest.SPLITS + "}").contains("not supported"));
        Assertions.assertTrue(error("{family: groupedMultinomial, label: y, baseline: b, time: t, predictions: [{name: A, prob: qa}], " + EvaluationScorerTest.SPLITS + "}").contains("group is required"));
        Assertions.assertTrue(error("{family: binomial, label: y, baseline: b, time: t, predictions: [{name: A, score: s}], " + EvaluationScorerTest.SPLITS + "}").contains("needs group"));
        Assertions.assertTrue(error("{group: g, label: y, baseline: b, time: t, predictions: [{name: A, prob: missing}], " + EvaluationScorerTest.SPLITS + "}").contains("not an input field"));
        Assertions.assertTrue(error("{group: g, label: y, baseline: b, time: t, predictions: [{name: A, prob: region}], " + EvaluationScorerTest.SPLITS + "}").contains("must be numeric"));
    }

    @Test
    public void testTableSliceAndBootstrapRules() {
        Assertions.assertTrue(error(OK.replace("}}}", "}}, calibration: [{type: histogram}]}")).contains("type 'histogram'"));
        Assertions.assertTrue(error(OK.replace("}}}", "}}, calibration: [{by: field}]}")).contains("field is required"));
        Assertions.assertTrue(error(OK.replace("}}}", "}}, calibration: [{by: field, field: u, edges: [2, 1]}]}")).contains("strictly ascending"));
        Assertions.assertTrue(error(OK.replace("}}}", "}}, calibration: [{by: prediction, bins: 1}]}")).contains("bins must be"));
        Assertions.assertTrue(error(OK.replace("}}}", "}}, calibration: [{type: edge}]}")).contains("thresholds is required"));
        Assertions.assertTrue(error(OK.replace("}}}", "}}, slices: [{field: region, bucket: decade}]}")).contains("bucket 'decade'"));
        Assertions.assertTrue(error(OK.replace("}}}", "}}, slices: [{field: u, bucket: month}]}")).contains("needs a timestamp"));
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
}
