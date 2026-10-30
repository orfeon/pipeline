package com.mercari.solution.module.transform;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import com.mercari.solution.util.domain.ml.onnx.OnnxTestModels;
import com.mercari.solution.util.domain.ml.tabular.KumoFixture;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

/**
 * The whole pipeline on the real models against the predictions of structured-data-models. It runs by hand with
 * {@code KUMO_MODEL_DIR}, a directory of {@code classifier} and {@code regressor} that each hold the {@code onnx} directory of
 * onnx-community/kumo-tabular-small-classifier-ONNX and onnx-community/kumo-tabular-small-regressor-ONNX.
 */
public class KumoTabularModelTest implements Serializable {

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    private static String modelDir() {
        final String dir = System.getenv("KUMO_MODEL_DIR");
        Assumptions.assumeTrue(dir != null, "KUMO_MODEL_DIR is not set");
        return dir;
    }

    private static String config(final KumoFixture fixture, final Path model, final String predictions) {
        final List<String> elements = new ArrayList<>();
        for(int r=0; r<fixture.rows().size(); r++) {
            final List<String> values = new ArrayList<>(List.of("group: g", "row: " + r));
            for(final String column : fixture.header()) {
                final Object value = column.equals(fixture.target()) && r >= fixture.contextRows() ? null : fixture.value(r, column);
                if(value != null) {
                    values.add(column + ": " + (value instanceof String ? "\"" + value + "\"" : value));
                }
            }
            elements.add("        - { " + String.join(", ", values) + " }");
        }
        final List<String> numerical = fixture.columns("numerical");
        final List<String> categorical = fixture.columns("categorical");
        final String schema = fixture.header().stream()
                .map(column -> "        - { name: " + column + ", type: " + (fixture.isNumberColumn(column) ? "float64" : "string") + " }")
                .collect(Collectors.joining("\n"));
        final String fields = String.join(", ", fixture.header());
        return """
                sources:
                  - name: create
                    module: create
                    parameters:
                      type: element
                      elements:
                %s
                    schema:
                      fields:
                        - { name: group, type: string }
                        - { name: row, type: int64 }
                %s
                transforms:
                  - name: groups
                    module: aggregation
                    inputs: [create]
                    parameters:
                      groupFields: [group]
                      aggregations:
                        - input: create
                          fields:
                            - { name: rows, op: array_agg, fields: [row, %s] }
                  - name: onnx
                    module: onnx
                    inputs: [groups]
                    failFast: true
                    parameters:
                      model:
                        path: "%s"
                        file: onnx/model.onnx
                      inputs:
                        - { name: tab, func: tabular_encode, preset: kumo, task: %s, field: rows, key: row, target: %s, numerical: %s, categorical: %s, maxContextRows: 10000 }
                        - { name: x, field: tab.x }
                        - { name: y_context, field: tab.yContext }
                        - { name: categorical_mask, field: tab.categoricalMask }
                      outputs:
                        - { name: predictions, func: tabular_decode, preset: kumo, field: logits, encoded: tab%s }
                """.formatted(String.join("\n", elements), schema, fields, OnnxTestModels.configPath(model), fixture.meta().get("task").getAsString(),
                fixture.target(), numerical, categorical, predictions);
    }

    @Test
    public void testClassifier() throws Exception {
        final Path model = Path.of(modelDir(), "classifier");
        final KumoFixture fixture = KumoFixture.load(KumoFixture.RESOURCES.resolve("mixed-churn"));
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(config(fixture, model, "")));

        final float[] expected = fixture.floats("pred.bin");
        final List<String> classes = fixture.classes();
        final int contextRows = fixture.contextRows();
        PAssert.that(outputs.get("onnx").getCollection()).satisfies(rows -> {
            final MElement element = rows.iterator().next();
            final List<?> predictions = (List<?>) element.getPrimitiveValue("predictions");
            Assertions.assertEquals(expected.length / classes.size(), predictions.size());
            double worst = 0;
            for(int r=0; r<predictions.size(); r++) {
                final Map<?, ?> prediction = (Map<?, ?>) predictions.get(r);
                Assertions.assertEquals((long) (contextRows + r), ((Number) prediction.get("key")).longValue());
                final List<?> probabilities = (List<?>) prediction.get("probabilities");
                int best = 0;
                for(int k=0; k<classes.size(); k++) {
                    final Map<?, ?> item = (Map<?, ?>) probabilities.get(k);
                    Assertions.assertEquals(classes.get(k), item.get("label").toString());
                    worst = Math.max(worst, Math.abs(expected[r * classes.size() + k] - ((Number) item.get("probability")).doubleValue()));
                    if(expected[r * classes.size() + k] > expected[r * classes.size() + best]) {
                        best = k;
                    }
                }
                Assertions.assertEquals(classes.get(best), prediction.get("prediction").toString());
            }
            System.out.println("kumo classifier: the largest difference of a probability from sdm: " + worst);
            Assertions.assertTrue(worst < 1e-3, "the largest difference of a probability: " + worst);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testRegressor() throws Exception {
        final Path model = Path.of(modelDir(), "regressor");
        final KumoFixture fixture = KumoFixture.load(KumoFixture.RESOURCES.resolve("mixed-ltv"));
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(config(fixture, model, ", quantiles: [0.05, 0.5, 0.95]")));

        final float[] expected = fixture.floats("pred.bin");
        PAssert.that(outputs.get("onnx").getCollection()).satisfies(rows -> {
            final List<?> predictions = (List<?>) rows.iterator().next().getPrimitiveValue("predictions");
            Assertions.assertEquals(expected.length, predictions.size());
            double worst = 0;
            for(int r=0; r<expected.length; r++) {
                final Map<?, ?> prediction = (Map<?, ?>) predictions.get(r);
                final double point = ((Number) prediction.get("prediction")).doubleValue();
                worst = Math.max(worst, Math.abs(expected[r] - point) / Math.max(1, Math.abs(expected[r])));
                Assertions.assertTrue(((Number) prediction.get("q05")).doubleValue() <= ((Number) prediction.get("q50")).doubleValue());
                Assertions.assertTrue(((Number) prediction.get("q50")).doubleValue() <= ((Number) prediction.get("q95")).doubleValue());
            }
            System.out.println("kumo regressor: the largest relative difference of a prediction from sdm: " + worst);
            Assertions.assertTrue(worst < 1e-3, "the largest relative difference of a prediction: " + worst);
            return null;
        });

        pipeline.run();
    }

}
