package com.mercari.solution.util.pipeline.select;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.domain.math.Tensor;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;

public class TabularTest {

    private static final List<Schema.Field> ROW_FIELDS = List.of(
            Schema.Field.of("id", Schema.FieldType.INT64),
            Schema.Field.of("price", Schema.FieldType.FLOAT64),
            Schema.Field.of("brand", Schema.FieldType.STRING),
            Schema.Field.of("label", Schema.FieldType.STRING),
            Schema.Field.of("amount", Schema.FieldType.FLOAT64),
            Schema.Field.of("tags", Schema.FieldType.array(Schema.FieldType.STRING)),
            Schema.Field.of("nested", Schema.FieldType.element(List.of(Schema.Field.of("size", Schema.FieldType.FLOAT32)))),
            Schema.Field.of("items", Schema.FieldType.array(Schema.FieldType.element(List.of(Schema.Field.of("size", Schema.FieldType.FLOAT32))))));

    private static final List<Schema.Field> FIELDS = List.of(
            Schema.Field.of("rows", Schema.FieldType.array(Schema.FieldType.element(ROW_FIELDS))),
            Schema.Field.of("text", Schema.FieldType.STRING),
            Schema.Field.of("logits", Schema.FieldType.matrix(Schema.FieldType.FLOAT32, List.of(-1, 10))),
            Schema.Field.of("flat", Schema.FieldType.array(Schema.FieldType.FLOAT32)));

    private static final String CLASSIFICATION = "\"preset\": \"kumo\", \"task\": \"classification\", \"field\": \"rows\", \"key\": \"id\", \"target\": \"label\", \"maxContextRows\": 10, \"numerical\": [\"price\"], \"categorical\": [\"brand\"]";
    private static final String REGRESSION = "\"preset\": \"kumo\", \"task\": \"regression\", \"field\": \"rows\", \"key\": \"id\", \"target\": \"amount\", \"maxContextRows\": 10, \"numerical\": [\"price\"]";

    private static SelectFunction function(final String func, final String parameters, final List<Schema.Field> fields) {
        final String json = "{ \"name\": \"o\", \"func\": \"" + func + "\", " + parameters + " }";
        final SelectFunction function = SelectFunction.of(new Gson().fromJson(json, JsonObject.class), fields);
        function.setup();
        return function;
    }

    private static SelectFunction encoder(final String parameters) {
        return function("tabular_encode", parameters, FIELDS);
    }

    private static Map<String, Object> row(final long id, final Double price, final String brand, final String label, final Double amount) {
        final Map<String, Object> row = new HashMap<>();
        row.put("id", id);
        row.put("price", price);
        row.put("brand", brand);
        row.put("label", label);
        row.put("amount", amount);
        return row;
    }

    private static Map<?, ?> encode(final SelectFunction function, final List<Map<String, Object>> rows) {
        final Map<String, Object> input = new HashMap<>();
        input.put("rows", rows);
        return (Map<?, ?>) function.apply(input, Instant.EPOCH);
    }

    private static Exception failure(final SelectFunction function, final List<Map<String, Object>> rows) {
        return Assertions.assertThrows(IllegalArgumentException.class, () -> encode(function, rows));
    }

    @Test
    public void testOutputTypes() {
        final Schema classification = encoder(CLASSIFICATION).getOutputFieldType().getElementSchema();
        Assertions.assertEquals(List.of("x", "yContext", "categoricalMask", "queryKeys", "classes"), classification.getFields().stream().map(Schema.Field::getName).toList());
        Assertions.assertEquals(Schema.Type.int32, classification.getField("yContext").getFieldType().getArrayValueType().getType());
        Assertions.assertEquals(Schema.Type.int64, classification.getField("queryKeys").getFieldType().getArrayValueType().getType());

        final Schema regression = encoder(REGRESSION).getOutputFieldType().getElementSchema();
        Assertions.assertEquals(List.of("x", "yContext", "categoricalMask", "queryKeys", "yMean", "yScale"), regression.getFields().stream().map(Schema.Field::getName).toList());
        Assertions.assertEquals(Schema.Type.float32, regression.getField("yContext").getFieldType().getArrayValueType().getType());
    }

    // array_agg gives no order: the same rows give the same result, with the context first, in the order of the keys
    @Test
    public void testRowsInAnyOrder() {
        final List<Map<String, Object>> rows = new ArrayList<>(List.of(
                row(4, 10d, "a", null, null),
                row(1, 1d, "a", "yes", null),
                row(3, 3d, "b", "no", null),
                row(2, 2d, "b", null, null),
                row(5, 5d, "a", "yes", null)));
        final SelectFunction function = encoder(CLASSIFICATION);
        final Map<?, ?> encoded = encode(function, rows);
        Assertions.assertEquals(List.of(2L, 4L), encoded.get("queryKeys"));
        Assertions.assertEquals(List.of("yes", "no"), encoded.get("classes"));
        Assertions.assertEquals(List.of(0, 1, 0), encoded.get("yContext"));
        Assertions.assertEquals(List.of(false, true), encoded.get("categoricalMask"));
        Assertions.assertEquals(5, ((List<?>) encoded.get("x")).size());

        Collections.reverse(rows);
        Assertions.assertEquals(encoded, encode(function, rows));
    }

    // a key compares as its kind: 9 comes before 10
    @Test
    public void testNumberKeysCompareAsNumbers() {
        final Map<?, ?> encoded = encode(encoder(CLASSIFICATION), List.of(
                row(10, 1d, "a", "x", null),
                row(9, 2d, "a", "y", null),
                row(11, 3d, "a", null, null)));
        Assertions.assertEquals(List.of("y", "x"), encoded.get("classes"));
    }

    @Test
    public void testRegressionTarget() {
        final Map<?, ?> encoded = encode(encoder(REGRESSION), List.of(
                row(1, 1d, "a", null, 1d),
                row(2, 2d, "a", null, 3d),
                // NaN is a missing value, as null: a row to predict
                row(3, 3d, "a", null, Double.NaN),
                row(4, 4d, "a", null, null)));
        Assertions.assertEquals(2d, encoded.get("yMean"));
        Assertions.assertEquals(1d, encoded.get("yScale"));
        Assertions.assertEquals(List.of(-1f, 1f), encoded.get("yContext"));
        Assertions.assertEquals(List.of(3L, 4L), encoded.get("queryKeys"));
    }

    // an infinity is left out of the scale, so that yContext would give it to the model
    @Test
    public void testInfiniteRegressionTarget() {
        final Exception e = failure(encoder(REGRESSION), List.of(
                row(1, 1d, "a", null, 1d),
                row(2, 2d, "a", null, Double.POSITIVE_INFINITY),
                row(3, 3d, "a", null, null)));
        Assertions.assertTrue(e.getMessage().contains("infinite"), e.getMessage());
    }

    // the label of a float is what the field holds, not its value widened to a double
    @Test
    public void testFloatLabels() {
        final String parameters = "\"preset\": \"kumo\", \"task\": \"classification\", \"field\": \"rows\", \"key\": \"id\", \"target\": \"nested.size\", \"maxContextRows\": 10, \"numerical\": [\"price\"]";
        final List<Map<String, Object>> rows = new ArrayList<>();
        for(int i=0; i<2; i++) {
            final Map<String, Object> row = row(i, (double) i, null, null, null);
            row.put("nested", Map.of("size", i == 0 ? 0.1f : 0.2f));
            rows.add(row);
        }
        Assertions.assertEquals(List.of("0.1", "0.2"), encode(encoder(parameters), rows).get("classes"));
    }

    @Test
    public void testNestedFields() {
        final String parameters = "\"preset\": \"kumo\", \"task\": \"classification\", \"field\": \"rows\", \"key\": \"id\", \"target\": \"label\", \"maxContextRows\": 10, \"numerical\": [\"nested.size\"]";
        final List<Map<String, Object>> rows = new ArrayList<>();
        for(int i=0; i<3; i++) {
            final Map<String, Object> row = row(i, null, null, i < 2 ? "x" : null, null);
            row.put("nested", Map.of("size", (float) i));
            rows.add(row);
        }
        final Map<?, ?> encoded = encode(encoder(parameters), rows);
        Assertions.assertEquals(1, ((List<?>) ((List<?>) encoded.get("x")).getFirst()).size());
    }

    // the same sample in whatever order, in the order of the keys, and the rows to predict are all kept
    @Test
    public void testContextSample() {
        final String parameters = CLASSIFICATION.replace("\"maxContextRows\": 10", "\"maxContextRows\": 4");
        final List<Map<String, Object>> rows = new ArrayList<>();
        for(int i=0; i<20; i++) {
            rows.add(row(i, (double) i, "a", "c" + (i % 3), null));
        }
        rows.add(row(100, 1d, "a", null, null));
        rows.add(row(101, 1d, "a", null, null));
        final SelectFunction function = encoder(parameters);
        final Map<?, ?> encoded = encode(function, rows);
        Assertions.assertEquals(4, ((List<?>) encoded.get("yContext")).size());
        Assertions.assertEquals(6, ((List<?>) encoded.get("x")).size());
        Assertions.assertEquals(List.of(100L, 101L), encoded.get("queryKeys"));

        final List<Map<String, Object>> shuffled = new ArrayList<>(rows);
        Collections.shuffle(shuffled, new Random(1));
        Assertions.assertEquals(encoded, encode(function, shuffled));
    }

    @Test
    public void testFailures() {
        final SelectFunction function = encoder(CLASSIFICATION);
        // no context row: not an array, no row, no row with a target
        final Map<String, Object> input = new HashMap<>();
        input.put("rows", null);
        Assertions.assertThrows(IllegalArgumentException.class, () -> function.apply(input, Instant.EPOCH));
        Assertions.assertTrue(failure(function, List.of()).getMessage().contains("no context row"));
        Assertions.assertTrue(failure(function, List.of(row(1, 1d, "a", null, null))).getMessage().contains("no context row"));
        // rows of no key or the same key have no order
        Assertions.assertTrue(failure(function, List.of(row(1, 1d, "a", "x", null), row(1, 2d, "b", "y", null))).getMessage().contains("more than once"));
        final Map<String, Object> noKey = row(1, 1d, "a", "x", null);
        noKey.put("id", null);
        Assertions.assertTrue(failure(function, List.of(noKey)).getMessage().contains("is null"));
        // no feature left
        Assertions.assertTrue(failure(function, List.of(row(1, 1d, "a", "x", null), row(2, 1d, "a", "y", null))).getMessage().contains("no feature left"));
        // over 10 classes
        final List<Map<String, Object>> rows = new ArrayList<>();
        for(int i=0; i<11; i++) {
            rows.add(row(i, (double) i, "a", "c" + i, null));
        }
        Assertions.assertTrue(failure(function, rows).getMessage().contains("classes"));
    }

    @Test
    public void testSkipsModelWithoutRowsToPredict() {
        final SelectFunction function = encoder(CLASSIFICATION);
        final Map<?, ?> none = encode(function, List.of(row(1, 1d, "a", "x", null), row(2, 2d, "a", "y", null)));
        Assertions.assertEquals(List.of(), none.get("queryKeys"));
        Assertions.assertTrue(function.skipsModel(none));
        Assertions.assertFalse(function.skipsModel(encode(function, List.of(row(1, 1d, "a", "x", null), row(2, 2d, "a", "y", null), row(3, 2d, "a", null, null)))));
        Assertions.assertFalse(function.skipsModel(null));
    }

    @Test
    public void testEncodeConfigErrors() {
        final Map<String, String> replacements = new LinkedHashMap<>();
        replacements.put("\"preset\": \"kumo\", ", "");
        replacements.put("\"task\": \"classification\", ", "");
        replacements.put("\"maxContextRows\": 10", "\"maxContextRows\": 0");
        replacements.put("\"preset\": \"kumo\"", "\"preset\": \"other\"");
        replacements.put("\"task\": \"classification\"", "\"task\": \"other\"");
        replacements.put("\"field\": \"rows\"", "\"field\": \"text\"");
        replacements.put("\"key\": \"id\"", "\"key\": \"tags\"");
        replacements.put("\"target\": \"label\"", "\"target\": \"tags\"");
        replacements.put("\"numerical\": [\"price\"]", "\"numerical\": [\"brand\"]");
        replacements.put("\"categorical\": [\"brand\"]", "\"categorical\": [\"tags\"]");
        for(final Map.Entry<String, String> entry : replacements.entrySet()) {
            Assertions.assertTrue(CLASSIFICATION.contains(entry.getKey()), entry.getKey());
            final String parameters = CLASSIFICATION.replace(entry.getKey(), entry.getValue());
            Assertions.assertThrows(IllegalArgumentException.class, () -> encoder(parameters), parameters);
        }
        for(final String parameters : List.of(
                CLASSIFICATION.replace("\"numerical\": [\"price\"], \"categorical\": [\"brand\"]", "\"numerical\": []"),
                // the same feature twice, and the target as a feature
                CLASSIFICATION.replace("\"numerical\": [\"price\"]", "\"numerical\": [\"price\", \"price\"]"),
                CLASSIFICATION.replace("\"categorical\": [\"brand\"]", "\"categorical\": [\"price\"]"),
                CLASSIFICATION.replace("\"categorical\": [\"brand\"]", "\"categorical\": [\"label\"]"),
                CLASSIFICATION.replace("\"categorical\": [\"brand\"]", "\"categorical\": \"brand\""),
                // a path through an array has a type, but a list per row
                CLASSIFICATION.replace("\"key\": \"id\"", "\"key\": \"items.size\""),
                CLASSIFICATION.replace("\"target\": \"label\"", "\"target\": \"items.size\""),
                CLASSIFICATION.replace("\"numerical\": [\"price\"]", "\"numerical\": [\"items.size\"]"),
                CLASSIFICATION.replace("\"categorical\": [\"brand\"]", "\"categorical\": [\"items.size\"]"),
                REGRESSION.replace("\"target\": \"amount\"", "\"target\": \"label\""))) {
            Assertions.assertThrows(IllegalArgumentException.class, () -> encoder(parameters), parameters);
        }
    }

    private static List<Schema.Field> decodeFields(final String encodeParameters) {
        final List<Schema.Field> fields = new ArrayList<>(FIELDS);
        fields.add(Schema.Field.of("tab", encoder(encodeParameters).getOutputFieldType()));
        return fields;
    }

    private static Map<String, Object> decodeInput(final Object logits, final Map<String, Object> encoded) {
        final Map<String, Object> input = new HashMap<>();
        input.put("logits", logits);
        input.put("tab", encoded);
        return input;
    }

    @Test
    public void testDecodeClassification() {
        final SelectFunction function = function("tabular_decode", "\"preset\": \"kumo\", \"field\": \"logits\", \"encoded\": \"tab\"", decodeFields(CLASSIFICATION));
        Assertions.assertEquals(List.of(), function.getOutputFieldType().getArrayValueType().getElementSchema().getFields().stream().filter(f -> f.getName().startsWith("q")).toList());
        Assertions.assertEquals(List.of("logits", "tab"), function.getInputFields().stream().map(Schema.Field::getName).toList());
        Assertions.assertTrue(function.acceptsTensor());

        final Map<String, Object> encoded = new HashMap<>();
        encoded.put("queryKeys", List.of(7L, 8L));
        encoded.put("classes", List.of("no", "yes"));
        // the 10 logits of a row: only the first two are classes, whatever the others are
        final float[] logits = new float[20];
        logits[1] = (float) Math.log(3);
        logits[5] = 100;
        logits[10] = (float) Math.log(4);
        logits[19] = 100;
        final List<?> results = (List<?>) function.apply(decodeInput(Tensor.of(logits, new long[]{2, 10}), encoded), Instant.EPOCH);
        final Map<?, ?> first = (Map<?, ?>) results.get(0);
        Assertions.assertEquals(7L, first.get("key"));
        Assertions.assertEquals("yes", first.get("prediction"));
        final List<?> probabilities = (List<?>) first.get("probabilities");
        Assertions.assertEquals("no", ((Map<?, ?>) probabilities.get(0)).get("label"));
        Assertions.assertEquals(0.25, (Double) ((Map<?, ?>) probabilities.get(0)).get("probability"), 1e-6);
        Assertions.assertEquals(0.75, (Double) ((Map<?, ?>) probabilities.get(1)).get("probability"), 1e-6);
        Assertions.assertEquals("no", ((Map<?, ?>) results.get(1)).get("prediction"));
    }

    // logits that are not finite leave no most likely class: not the first one of the row
    @Test
    public void testDecodeNonFiniteLogits() {
        final SelectFunction function = function("tabular_decode", "\"preset\": \"kumo\", \"field\": \"logits\", \"encoded\": \"tab\"", decodeFields(CLASSIFICATION));
        final Map<String, Object> encoded = new HashMap<>();
        encoded.put("queryKeys", List.of(7L, 8L));
        encoded.put("classes", List.of("no", "yes"));
        final Tensor logits = Tensor.of(new float[]{Float.NaN, 1, 0, 1}, new long[]{2, 2});
        final List<?> results = (List<?>) function.apply(decodeInput(logits, encoded), Instant.EPOCH);
        Assertions.assertNull(((Map<?, ?>) results.get(0)).get("prediction"));
        Assertions.assertEquals("yes", ((Map<?, ?>) results.get(1)).get("prediction"));
    }

    // the outputs of the model are sorted, and put back in the scale of the target
    @Test
    public void testDecodeRegression() {
        final SelectFunction function = function("tabular_decode", "\"preset\": \"kumo\", \"field\": \"logits\", \"encoded\": \"tab\", \"quantiles\": [0.25, 0.5, 0.75]", decodeFields(REGRESSION));
        Assertions.assertEquals(List.of("key", "prediction", "q25", "q50", "q75"), function.getOutputFieldType().getArrayValueType().getElementSchema().getFields().stream().map(Schema.Field::getName).toList());

        final Map<String, Object> encoded = new HashMap<>();
        encoded.put("queryKeys", List.of(7L));
        encoded.put("yMean", 100d);
        encoded.put("yScale", 10d);
        final Tensor logits = Tensor.of(new float[]{3, 1, 2}, new long[]{1, 3});
        final Map<?, ?> result = (Map<?, ?>) ((List<?>) function.apply(decodeInput(logits, encoded), Instant.EPOCH)).getFirst();
        Assertions.assertEquals(120d, result.get("prediction"));
        Assertions.assertEquals(110d, result.get("q25"));
        Assertions.assertEquals(120d, result.get("q50"));
        Assertions.assertEquals(130d, result.get("q75"));

        // a matrix is flat in a record
        final Map<?, ?> flat = (Map<?, ?>) ((List<?>) function.apply(decodeInput(List.of(3f, 1f, 2f), encoded), Instant.EPOCH)).getFirst();
        Assertions.assertEquals(120d, flat.get("prediction"));
    }

    // without a row to predict the model did not run: there is no output to read
    @Test
    public void testDecodeWithoutRowsToPredict() {
        final SelectFunction function = function("tabular_decode", "\"preset\": \"kumo\", \"field\": \"logits\", \"encoded\": \"tab\"", decodeFields(CLASSIFICATION));
        final Map<String, Object> encoded = new HashMap<>();
        encoded.put("queryKeys", List.of());
        encoded.put("classes", List.of("no"));
        Assertions.assertEquals(List.of(), function.apply(decodeInput(null, encoded), Instant.EPOCH));
        Assertions.assertNull(function.apply(decodeInput(null, null), Instant.EPOCH));

        encoded.put("queryKeys", List.of(1L));
        Assertions.assertThrows(IllegalArgumentException.class, () -> function.apply(decodeInput(null, encoded), Instant.EPOCH));
    }

    @Test
    public void testDecodeConfigErrors() {
        final List<Schema.Field> classification = decodeFields(CLASSIFICATION);
        final List<Schema.Field> regression = decodeFields(REGRESSION);
        for(final String parameters : List.of(
                "\"field\": \"logits\", \"encoded\": \"tab\"",
                "\"preset\": \"kumo\", \"encoded\": \"tab\"",
                "\"preset\": \"kumo\", \"field\": \"logits\"",
                "\"preset\": \"other\", \"field\": \"logits\", \"encoded\": \"tab\"",
                "\"preset\": \"kumo\", \"field\": \"flat\", \"encoded\": \"tab\"",
                "\"preset\": \"kumo\", \"field\": \"logits\", \"encoded\": \"text\"",
                "\"preset\": \"kumo\", \"field\": \"logits\", \"encoded\": \"tab\", \"quantiles\": [0.5]")) {
            Assertions.assertThrows(IllegalArgumentException.class, () -> function("tabular_decode", parameters, classification), parameters);
        }
        for(final String quantiles : List.of("[]", "[0]", "[1.5]", "[0.5, 0.50]", "0.5")) {
            final String parameters = "\"preset\": \"kumo\", \"field\": \"logits\", \"encoded\": \"tab\", \"quantiles\": " + quantiles;
            final Exception e = Assertions.assertThrows(IllegalArgumentException.class, () -> function("tabular_decode", parameters, regression), parameters);
            Assertions.assertTrue(e.getMessage().contains("tabular_decode"), e.getMessage());
        }
    }

}
