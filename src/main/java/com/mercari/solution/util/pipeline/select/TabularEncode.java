package com.mercari.solution.util.pipeline.select;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.domain.ml.tabular.KumoTabular;
import com.mercari.solution.util.schema.ElementSchemaUtil;
import org.joda.time.Instant;

import java.io.Serializable;
import java.util.*;

/**
 * Turns the rows of a group (an {@code array_agg} result) to the inputs of a tabular model that learns in context. A row
 * with a target is context, and a row without one is to be predicted. The steps are chosen by {@code preset}.
 *
 * <pre>{@code
 * { "name": "tab", "func": "tabular_encode", "preset": "kumo", "task": "classification", "field": "rows", "key": "item_id",
 *   "target": "label", "numerical": ["price"], "categorical": ["brand"], "maxContextRows": 2000 }
 * }</pre>
 */
public class TabularEncode implements SelectFunction {

    public enum Preset implements Serializable {
        kumo
    }

    public enum Task implements Serializable {
        classification,
        regression
    }

    // the order is fixed so that the same rows give the same sample, in whatever order they come
    private static final long SAMPLE_SEED = 0L;

    private final String name;
    private final Task task;
    private final String field;
    private final String key;
    private final String target;
    private final List<String> numerical;
    private final List<String> categorical;
    private final int maxContextRows;

    private final List<Schema.Field> inputFields;
    private final Schema.FieldType outputFieldType;
    private final boolean ignore;

    private TabularEncode(
            final String name,
            final Task task,
            final String field,
            final String key,
            final String target,
            final List<String> numerical,
            final List<String> categorical,
            final int maxContextRows,
            final Schema.Field inputField,
            final Schema.FieldType outputFieldType,
            final boolean ignore) {

        this.name = name;
        this.task = task;
        this.field = field;
        this.key = key;
        this.target = target;
        this.numerical = numerical;
        this.categorical = categorical;
        this.maxContextRows = maxContextRows;
        this.inputFields = new ArrayList<>();
        this.inputFields.add(inputField);
        this.outputFieldType = outputFieldType;
        this.ignore = ignore;
    }

    public static TabularEncode of(final String name, final JsonObject jsonObject, final List<Schema.Field> inputFields, final boolean ignore) {
        for(final String parameter : List.of("preset", "task", "field", "key", "target", "maxContextRows")) {
            if(!jsonObject.has(parameter)) {
                throw new IllegalArgumentException("SelectField: " + name + " func: tabular_encode requires " + parameter + " parameter");
            }
        }
        Preset.valueOf(jsonObject.get("preset").getAsString());
        final Task task = Task.valueOf(jsonObject.get("task").getAsString());

        final String field = jsonObject.get("field").getAsString();
        final Schema.FieldType type = ElementSchemaUtil.getInputFieldType(field, inputFields);
        if(type.getType() != Schema.Type.array || type.getArrayValueType().getType() != Schema.Type.element) {
            throw new IllegalArgumentException("SelectField: " + name + " func: tabular_encode field: " + field + " must be an array of elements, but got: " + type);
        }
        final List<Schema.Field> rowFields = type.getArrayValueType().getElementSchema().getFields();

        final String key = jsonObject.get("key").getAsString();
        final Schema.FieldType keyType = ElementSchemaUtil.getInputFieldType(key, rowFields);
        requireScalar(name, "key", key, keyType);

        final String target = jsonObject.get("target").getAsString();
        final Schema.FieldType targetType = ElementSchemaUtil.getInputFieldType(target, rowFields);
        if(task == Task.regression && !Vectors.isNumeric(targetType)) {
            throw new IllegalArgumentException("SelectField: " + name + " func: tabular_encode target: " + target + " of a regression must be numeric, but got: " + targetType);
        }
        requireScalar(name, "target", target, targetType);

        final List<String> numerical = names(name, jsonObject, "numerical");
        final List<String> categorical = names(name, jsonObject, "categorical");
        if(numerical.isEmpty() && categorical.isEmpty()) {
            throw new IllegalArgumentException("SelectField: " + name + " func: tabular_encode requires numerical or categorical parameter");
        }
        final Set<String> features = new HashSet<>();
        for(final String feature : numerical) {
            final Schema.FieldType featureType = ElementSchemaUtil.getInputFieldType(feature, rowFields);
            if(!Vectors.isNumeric(featureType)) {
                throw new IllegalArgumentException("SelectField: " + name + " func: tabular_encode numerical: " + feature + " must be numeric, but got: " + featureType);
            }
            if(!features.add(feature)) {
                throw new IllegalArgumentException("SelectField: " + name + " func: tabular_encode has feature: " + feature + " more than once");
            }
        }
        for(final String feature : categorical) {
            requireScalar(name, "categorical", feature, ElementSchemaUtil.getInputFieldType(feature, rowFields));
            if(!features.add(feature)) {
                throw new IllegalArgumentException("SelectField: " + name + " func: tabular_encode has feature: " + feature + " more than once");
            }
        }
        // a target among the features would give the model the answer in the context rows
        if(features.contains(target)) {
            throw new IllegalArgumentException("SelectField: " + name + " func: tabular_encode target: " + target + " must not be a feature");
        }

        final int maxContextRows = jsonObject.get("maxContextRows").getAsInt();
        if(maxContextRows < 1) {
            throw new IllegalArgumentException("SelectField: " + name + " func: tabular_encode maxContextRows must be over than zero: " + maxContextRows);
        }

        final List<Schema.Field> outputFields = new ArrayList<>();
        outputFields.add(Schema.Field.of("x", Schema.FieldType.array(Schema.FieldType.array(Schema.FieldType.FLOAT32))));
        outputFields.add(Schema.Field.of("yContext", Schema.FieldType.array(task == Task.classification ? Schema.FieldType.INT32 : Schema.FieldType.FLOAT32)));
        outputFields.add(Schema.Field.of("categoricalMask", Schema.FieldType.array(Schema.FieldType.BOOLEAN)));
        outputFields.add(Schema.Field.of("queryKeys", Schema.FieldType.array(keyType)));
        if(task == Task.classification) {
            outputFields.add(Schema.Field.of("classes", Schema.FieldType.array(Schema.FieldType.STRING)));
        } else {
            outputFields.add(Schema.Field.of("yMean", Schema.FieldType.FLOAT64));
            outputFields.add(Schema.Field.of("yScale", Schema.FieldType.FLOAT64));
        }

        final String parent = ElementSchemaUtil.getInputFieldName(field, inputFields);
        final Schema.Field inputField = Schema.Field.of(parent, ElementSchemaUtil.getInputFieldType(parent, inputFields));
        return new TabularEncode(name, task, field, key, target, numerical, categorical, maxContextRows, inputField, Schema.FieldType.element(outputFields), ignore);
    }

    private static List<String> names(final String name, final JsonObject jsonObject, final String parameter) {
        final JsonElement element = jsonObject.get(parameter);
        if(element == null) {
            return List.of();
        }
        if(!element.isJsonArray()) {
            throw new IllegalArgumentException("SelectField: " + name + " func: tabular_encode " + parameter + " must be an array of fields: " + element);
        }
        final JsonArray array = element.getAsJsonArray();
        final List<String> names = new ArrayList<>();
        for(final JsonElement item : array) {
            names.add(item.getAsString());
        }
        return names;
    }

    private static void requireScalar(final String name, final String parameter, final String field, final Schema.FieldType type) {
        switch (type.getType()) {
            case string, uuid, bool, int8, int16, int32, int64, float16, float32, float64, date, time, timestamp -> {}
            default -> throw new IllegalArgumentException("SelectField: " + name + " func: tabular_encode " + parameter + ": " + field + " must be a string, a number, a bool or a time, but got: " + type);
        }
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
    public void setup() {

    }

    // without a row to predict there is nothing to ask the model
    @Override
    public boolean skipsModel(final Object result) {
        return result instanceof Map<?, ?> map && map.get("queryKeys") instanceof List<?> keys && keys.isEmpty();
    }

    @Override
    public Object apply(final Map<String, Object> input, final Instant timestamp) {
        if(!(ElementSchemaUtil.getValue(input, field) instanceof List<?> rows)) {
            throw new IllegalArgumentException("SelectField: " + name + " func: tabular_encode has no context row: field: " + field + " is not an array");
        }

        // array_agg gives no order, while the numbers of the classes and the sample depend on it
        final List<Map<?, ?>> context = new ArrayList<>();
        final List<Map<?, ?>> query = new ArrayList<>();
        for(final Map<?, ?> row : sortedRows(rows)) {
            (hasTarget(row) ? context : query).add(row);
        }
        if(context.isEmpty()) {
            throw new IllegalArgumentException("SelectField: " + name + " func: tabular_encode has no context row: no row of field: " + field + " has target: " + target);
        }
        // on all the context rows: a sample without a class must not decide whether the group can be predicted
        if(task == Task.classification) {
            KumoTabular.classes(labels(context));
        }
        final List<Map<?, ?>> selected = sample(context);

        final List<Map<?, ?>> all = new ArrayList<>(selected);
        all.addAll(query);
        final List<double[]> numericalColumns = new ArrayList<>();
        for(final String feature : numerical) {
            final double[] values = new double[all.size()];
            for(int i=0; i<values.length; i++) {
                values[i] = ElementSchemaUtil.getValue(all.get(i), feature) instanceof Number number ? number.doubleValue() : Double.NaN;
            }
            numericalColumns.add(values);
        }
        final List<Comparable<?>[]> categoricalColumns = new ArrayList<>();
        for(final String feature : categorical) {
            final Comparable<?>[] values = new Comparable<?>[all.size()];
            for(int i=0; i<values.length; i++) {
                values[i] = comparable(ElementSchemaUtil.getValue(all.get(i), feature));
            }
            categoricalColumns.add(values);
        }
        final KumoTabular.Features features = KumoTabular.features(numericalColumns, categoricalColumns, all.size(), selected.size());
        if(features.categoricalMask().length == 0) {
            throw new IllegalArgumentException("SelectField: " + name + " func: tabular_encode has no feature left: all columns are constant in the context rows");
        }

        final List<List<Float>> x = new ArrayList<>(features.x().length);
        for(final float[] row : features.x()) {
            final List<Float> values = new ArrayList<>(row.length);
            for(final float value : row) {
                values.add(value);
            }
            x.add(values);
        }
        final List<Boolean> mask = new ArrayList<>(features.categoricalMask().length);
        for(final boolean categoricalColumn : features.categoricalMask()) {
            mask.add(categoricalColumn);
        }
        final List<Object> queryKeys = new ArrayList<>(query.size());
        for(final Map<?, ?> row : query) {
            queryKeys.add(ElementSchemaUtil.getValue(row, key));
        }

        final Map<String, Object> encoded = new HashMap<>();
        encoded.put("x", x);
        encoded.put("categoricalMask", mask);
        encoded.put("queryKeys", queryKeys);
        switch (task) {
            case classification -> putClasses(encoded, selected);
            case regression -> putScale(encoded, selected);
        }
        return encoded;
    }

    private List<Map<?, ?>> sortedRows(final List<?> rows) {
        final TreeMap<Comparable<?>, Map<?, ?>> sorted = new TreeMap<>();
        for(final Object row : rows) {
            if(!(row instanceof Map<?, ?> map)) {
                throw new IllegalArgumentException("SelectField: " + name + " func: tabular_encode field: " + field + " must be an array of elements, but got: " + row);
            }
            final Comparable<?> rowKey = comparable(ElementSchemaUtil.getValue(map, key));
            if(rowKey == null) {
                throw new IllegalArgumentException("SelectField: " + name + " func: tabular_encode key: " + key + " is null in a row");
            }
            // two rows of the same key have no order, so the result would differ from one run to another
            if(sorted.put(rowKey, map) != null) {
                throw new IllegalArgumentException("SelectField: " + name + " func: tabular_encode key: " + key + " has " + rowKey + " more than once");
            }
        }
        return new ArrayList<>(sorted.values());
    }

    private boolean hasTarget(final Map<?, ?> row) {
        return comparable(ElementSchemaUtil.getValue(row, target)) != null;
    }

    // in the order of the keys, as the context rows that were not cut are
    private List<Map<?, ?>> sample(final List<Map<?, ?>> context) {
        if(context.size() <= maxContextRows) {
            return context;
        }
        final List<Integer> indexes = new ArrayList<>(context.size());
        for(int i=0; i<context.size(); i++) {
            indexes.add(i);
        }
        Collections.shuffle(indexes, new Random(SAMPLE_SEED));
        final List<Integer> selected = new ArrayList<>(indexes.subList(0, maxContextRows));
        Collections.sort(selected);
        final List<Map<?, ?>> rows = new ArrayList<>(maxContextRows);
        for(final int index : selected) {
            rows.add(context.get(index));
        }
        return rows;
    }

    private void putClasses(final Map<String, Object> encoded, final List<Map<?, ?>> context) {
        final KumoTabular.Classes classes = KumoTabular.classes(labels(context));
        final List<Integer> codes = new ArrayList<>(classes.codes().length);
        for(final int code : classes.codes()) {
            codes.add(code);
        }
        encoded.put("yContext", codes);
        encoded.put("classes", classes.labels());
    }

    private List<String> labels(final List<Map<?, ?>> context) {
        final List<String> labels = new ArrayList<>(context.size());
        for(final Map<?, ?> row : context) {
            labels.add(comparable(ElementSchemaUtil.getValue(row, target)).toString());
        }
        return labels;
    }

    private void putScale(final Map<String, Object> encoded, final List<Map<?, ?>> context) {
        final double[] values = new double[context.size()];
        for(int i=0; i<values.length; i++) {
            values[i] = ((Number) ElementSchemaUtil.getValue(context.get(i), target)).doubleValue();
        }
        final KumoTabular.Scale scale = KumoTabular.scale(values);
        final List<Float> standardized = new ArrayList<>(values.length);
        for(final double value : values) {
            standardized.add((float) ((value - scale.mean()) / scale.scale()));
        }
        encoded.put("yContext", standardized);
        encoded.put("yMean", scale.mean());
        encoded.put("yScale", scale.scale());
    }

    // values of one field in one type, so that they compare as their kind does: 9 comes before 10. NaN is a missing value
    private static Comparable<?> comparable(final Object value) {
        return switch (value) {
            case null -> null;
            case Float number -> number.isNaN() ? null : number.doubleValue();
            case Double number -> number.isNaN() ? null : number;
            case Number number -> number.longValue();
            case Boolean bool -> bool;
            default -> value.toString();
        };
    }

}
