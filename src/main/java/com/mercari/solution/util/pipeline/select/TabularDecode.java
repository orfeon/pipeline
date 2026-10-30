package com.mercari.solution.util.pipeline.select;

import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.domain.math.Tensor;
import com.mercari.solution.util.domain.ml.tabular.KumoTabular;
import com.mercari.solution.util.schema.ElementSchemaUtil;
import org.joda.time.Instant;

import java.util.*;

/**
 * Turns the output of a tabular model to a result per row to predict, with the {@code tabular_encode} result it was given.
 * A classification gives {@code key}, {@code prediction} and {@code probabilities}, a regression {@code key},
 * {@code prediction} and a field per level of {@code quantiles}.
 *
 * <pre>{@code
 * { "name": "predictions", "func": "tabular_decode", "preset": "kumo", "field": "logits", "encoded": "tab", "quantiles": [0.05, 0.5, 0.95] }
 * }</pre>
 */
public class TabularDecode implements SelectFunction {

    private record Quantile(String name, double level) implements java.io.Serializable {}

    private final String name;
    private final String field;
    private final String encoded;
    private final boolean classification;
    private final List<Quantile> quantiles;

    private final List<Schema.Field> inputFields;
    private final Schema.FieldType outputFieldType;
    private final boolean ignore;

    private TabularDecode(
            final String name,
            final String field,
            final String encoded,
            final boolean classification,
            final List<Quantile> quantiles,
            final List<Schema.Field> inputFields,
            final Schema.FieldType outputFieldType,
            final boolean ignore) {

        this.name = name;
        this.field = field;
        this.encoded = encoded;
        this.classification = classification;
        this.quantiles = quantiles;
        this.inputFields = inputFields;
        this.outputFieldType = outputFieldType;
        this.ignore = ignore;
    }

    public static TabularDecode of(final String name, final JsonObject jsonObject, final List<Schema.Field> inputFields, final boolean ignore) {
        for(final String parameter : List.of("preset", "field", "encoded")) {
            if(!jsonObject.has(parameter)) {
                throw new IllegalArgumentException("SelectField: " + name + " func: tabular_decode requires " + parameter + " parameter");
            }
        }
        TabularEncode.Preset.valueOf(jsonObject.get("preset").getAsString());

        final String field = jsonObject.get("field").getAsString();
        final Schema.FieldType type = ElementSchemaUtil.getInputFieldType(field, inputFields);
        if(type.getType() != Schema.Type.matrix || type.getShape().size() != 2 || !Vectors.isNumeric(type.getMatrixValueType())) {
            throw new IllegalArgumentException("SelectField: " + name + " func: tabular_decode field: " + field + " must be a numeric matrix of two dimensions, but got: " + type);
        }

        final String encoded = jsonObject.get("encoded").getAsString();
        final Schema.FieldType encodedType = ElementSchemaUtil.getInputFieldType(encoded, inputFields);
        if(encodedType.getType() != Schema.Type.element || !encodedType.getElementSchema().hasField("queryKeys")) {
            throw new IllegalArgumentException("SelectField: " + name + " func: tabular_decode encoded: " + encoded + " must be a tabular_encode result, but got: " + encodedType);
        }
        final boolean classification = encodedType.getElementSchema().hasField("classes");
        if(classification && jsonObject.has("quantiles")) {
            throw new IllegalArgumentException("SelectField: " + name + " func: tabular_decode quantiles is for a regression, but encoded: " + encoded + " is a classification");
        }

        final List<Schema.Field> fields = new ArrayList<>();
        fields.add(Schema.Field.of("key", encodedType.getElementSchema().getField("queryKeys").getFieldType().getArrayValueType()));
        final List<Quantile> quantiles = new ArrayList<>();
        if(classification) {
            fields.add(Schema.Field.of("prediction", Schema.FieldType.STRING));
            fields.add(Schema.Field.of("probabilities", Schema.FieldType.array(Schema.FieldType.element(List.of(
                    Schema.Field.of("label", Schema.FieldType.STRING),
                    Schema.Field.of("probability", Schema.FieldType.FLOAT64))))));
        } else {
            fields.add(Schema.Field.of("prediction", Schema.FieldType.FLOAT64));
            if(jsonObject.has("quantiles")) {
                for(final String level : TimeseriesQuantiles.levels(name, "tabular_decode", "quantiles", jsonObject.get("quantiles"))) {
                    final Quantile quantile = new Quantile(TimeseriesQuantiles.quantileName(level), Double.parseDouble(level));
                    quantiles.add(quantile);
                    fields.add(Schema.Field.of(quantile.name, Schema.FieldType.FLOAT64));
                }
            }
        }

        final Map<String, Schema.Field> parents = new LinkedHashMap<>();
        for(final String input : List.of(field, encoded)) {
            final String parent = ElementSchemaUtil.getInputFieldName(input, inputFields);
            parents.put(parent, Schema.Field.of(parent, ElementSchemaUtil.getInputFieldType(parent, inputFields)));
        }
        return new TabularDecode(name, field, encoded, classification, quantiles, new ArrayList<>(parents.values()),
                Schema.FieldType.array(Schema.FieldType.element(fields)), ignore);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean ignore() {
        return ignore;
    }

    @Override
    public List<Schema.Field> getInputFields() {
        return inputFields;
    }

    @Override
    public Schema.FieldType getOutputFieldType() {
        return outputFieldType;
    }

    @Override
    public boolean acceptsTensor() {
        return true;
    }

    @Override
    public void setup() {

    }

    @Override
    public Object apply(final Map<String, Object> input, final Instant timestamp) {
        if(!(ElementSchemaUtil.getValue(input, encoded) instanceof Map<?, ?> values)) {
            return null;
        }
        final List<?> keys = (List<?>) values.get("queryKeys");
        // the model did not run: there is no output to read
        if(keys.isEmpty()) {
            return new ArrayList<>();
        }
        final Tensor logits = logits(ElementSchemaUtil.getValue(input, field), keys.size());
        return classification
                ? classification(logits, keys, (List<?>) values.get("classes"))
                : regression(logits, keys, new KumoTabular.Scale(((Number) values.get("yMean")).doubleValue(), ((Number) values.get("yScale")).doubleValue()));
    }

    private Tensor logits(final Object value, final int rows) {
        return switch (value) {
            case Tensor tensor -> tensor;
            // a matrix is flat in a record
            case List<?> list -> Tensor.of(Tensor.Type.DOUBLE, list, new long[]{rows, list.size() / rows});
            case null, default -> throw new IllegalArgumentException("SelectField: " + name + " func: tabular_decode field: " + field + " must be a matrix, but got: " + value);
        };
    }

    private List<Map<String, Object>> classification(final Tensor logits, final List<?> keys, final List<?> classes) {
        final double[][] probabilities = KumoTabular.probabilities(logits, keys.size(), classes.size());
        final List<Map<String, Object>> results = new ArrayList<>(keys.size());
        for(int r=0; r<keys.size(); r++) {
            int best = 0;
            final List<Map<String, Object>> items = new ArrayList<>(classes.size());
            for(int k=0; k<classes.size(); k++) {
                if(probabilities[r][k] > probabilities[r][best]) {
                    best = k;
                }
                final Map<String, Object> item = new HashMap<>();
                item.put("label", classes.get(k));
                item.put("probability", probabilities[r][k]);
                items.add(item);
            }
            final Map<String, Object> result = new HashMap<>();
            result.put("key", keys.get(r));
            result.put("prediction", classes.get(best));
            result.put("probabilities", items);
            results.add(result);
        }
        return results;
    }

    private List<Map<String, Object>> regression(final Tensor logits, final List<?> keys, final KumoTabular.Scale scale) {
        final double[][] sorted = KumoTabular.quantiles(logits, keys.size(), scale);
        final List<Map<String, Object>> results = new ArrayList<>(keys.size());
        for(int r=0; r<keys.size(); r++) {
            final double[] row = sorted[r];
            final Map<String, Object> result = new HashMap<>();
            result.put("key", keys.get(r));
            result.put("prediction", Arrays.stream(row).average().orElse(Double.NaN));
            // levels 1 / (n + 1), ..., n / (n + 1): q001 to q999 for 999 values
            for(final Quantile quantile : quantiles) {
                final long position = Math.round(quantile.level * (row.length + 1)) - 1;
                result.put(quantile.name, row[(int) Math.max(0, Math.min(row.length - 1, position))]);
            }
            results.add(result);
        }
        return results;
    }

}
