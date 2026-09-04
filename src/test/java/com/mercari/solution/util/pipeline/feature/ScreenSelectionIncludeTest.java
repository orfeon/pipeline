package com.mercari.solution.util.pipeline.feature;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.screen.GroupScorer;
import com.mercari.solution.util.pipeline.screen.ScoreAccumulator;
import com.mercari.solution.util.pipeline.screen.ScreenReport;
import com.mercari.solution.util.pipeline.screen.ScreenRow;
import com.mercari.solution.util.pipeline.screen.ScreenSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The closed loop: the screen transform's {@code output.selection} file must be readable by the feature
 * transform's {@code output.include} (FeaturePlanService.parseIncludeList) as the projection list.
 */
public class ScreenSelectionIncludeTest {

    @Test
    public void testSelectionFileIsAnIncludeList() {
        final Schema schema = Schema.builder()
                .withField("g", Schema.FieldType.STRING)
                .withField("y", Schema.FieldType.INT64)
                .withField("t", Schema.FieldType.TIMESTAMP)
                .withField("x", Schema.FieldType.FLOAT64)
                .withField("x2", Schema.FieldType.FLOAT64)
                .build();
        final ScreenSpec.Lineage lineage = ScreenSpec.Lineage.fromManifest("{planHash: abc123, outputHash: def456, timeField: t}");
        final ScreenSpec spec = ScreenSpec.parse(JsonParser.parseString(
                        "{family: groupedMultinomial, group: g, label: y, candidates: [x, x2], transforms: [raw], placebo: {noise: 0}, output: {selection: 'target/unused.json'}}").getAsJsonObject())
                .resolve(schema, lineage);
        Assertions.assertEquals("target/unused.json", spec.selectionUri);
        Assertions.assertEquals("abc123", spec.manifestPlanHash);
        Assertions.assertEquals("def456", spec.manifestOutputHash);

        // x separates the positive in every group, x2 is constant: only x passes
        final GroupScorer scorer = new GroupScorer(spec);
        final Map<Integer, ScoreAccumulator> acc = new HashMap<>();
        for (int g = 0; g < 20; g++) {
            scorer.score(List.of(
                    new ScreenRow("g" + g, "p", g, null, 1, Double.NaN, 1, new double[]{3, 1}),
                    new ScreenRow("g" + g, "q", g, null, 0, Double.NaN, 1, new double[]{1, 1}),
                    new ScreenRow("g" + g, "r", g, null, 0, Double.NaN, 1, new double[]{2, 1})), "g" + g, acc);
        }
        final ScreenReport.Result result = ScreenReport.build(spec, acc);
        final JsonObject selection = ScreenReport.selection(spec, result);
        Assertions.assertEquals("abc123", selection.get("planHash").getAsString());
        Assertions.assertEquals("marginal", selection.get("test").getAsString());
        Assertions.assertEquals(1, selection.getAsJsonArray("passed").size());
        Assertions.assertEquals(64, spec.parametersHash.length());

        final List<String> include = FeaturePlanService.parseIncludeList(selection.toString(), "screen selection");
        Assertions.assertEquals(List.of("x"), include);
    }

    @Test
    public void testParametersHashIsOrderIndependent() {
        final ScreenSpec a = ScreenSpec.parse(JsonParser.parseString("{family: binomial, label: y, candidates: [x], placebo: {noise: 0, seed: 1}}").getAsJsonObject());
        final ScreenSpec b = ScreenSpec.parse(JsonParser.parseString("{placebo: {seed: 1, noise: 0}, candidates: [x], label: y, family: binomial}").getAsJsonObject());
        final ScreenSpec c = ScreenSpec.parse(JsonParser.parseString("{placebo: {seed: 2, noise: 0}, candidates: [x], label: y, family: binomial}").getAsJsonObject());
        Assertions.assertEquals(a.parametersHash, b.parametersHash);
        Assertions.assertNotEquals(a.parametersHash, c.parametersHash);
    }
}
