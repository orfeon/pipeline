package com.mercari.solution.util.pipeline.select;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.domain.math.Tensor;
import com.mercari.solution.util.schema.ElementSchemaUtil;
import com.mercari.solution.util.schema.converter.JsonToMapConverter;
import org.joda.time.Instant;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Select functions that build an array, typically the inputs of the next run of a model in a loop of the onnx transform.
 * A Tensor in the record is left alone, so that the values of a model output are not boxed. {@code array_append} and
 * {@code range} of a NULL field give NULL, and {@code array} keeps a NULL field as a NULL value of the array.
 *
 * <pre>{@code
 * { "name": "input_ids",      "func": "array",        "fields": ["token"] },
 * { "name": "generated",      "func": "array_append", "field": "generated", "valueField": "token" },
 * { "name": "attention_mask", "func": "array_append", "field": "attention_mask", "value": 1 },
 * { "name": "position_ids",   "func": "range",        "field": "attention_mask" },
 * { "name": "position_ids",   "func": "range",        "field": "attention_mask", "last": 1 }
 * }</pre>
 */
public class ArrayFunctions implements SelectFunction {

    public enum Op implements Serializable {
        array,
        array_append,
        range
    }

    private final String name;
    private final Op op;
    private final String field;
    private final List<String> fields;
    private final String valueField;
    private final String value;
    private final Integer last;

    private final List<Schema.Field> inputFields;
    private final Schema.FieldType outputFieldType;
    private final boolean ignore;

    private transient Object constant;

    private ArrayFunctions(
            final String name,
            final Op op,
            final String field,
            final List<String> fields,
            final String valueField,
            final String value,
            final Integer last,
            final List<Schema.Field> inputFields,
            final Schema.FieldType outputFieldType,
            final boolean ignore) {

        this.name = name;
        this.op = op;
        this.field = field;
        this.fields = fields;
        this.valueField = valueField;
        this.value = value;
        this.last = last;
        this.inputFields = inputFields;
        this.outputFieldType = outputFieldType;
        this.ignore = ignore;
    }

    public static ArrayFunctions of(final String name, final JsonObject jsonObject, final List<Schema.Field> inputFields, final Op op, final boolean ignore) {
        return switch (op) {
            case array -> ofArray(name, jsonObject, inputFields, ignore);
            case array_append -> ofAppend(name, jsonObject, inputFields, ignore);
            case range -> ofRange(name, jsonObject, inputFields, ignore);
        };
    }

    private static ArrayFunctions ofArray(final String name, final JsonObject jsonObject, final List<Schema.Field> inputFields, final boolean ignore) {
        if(!jsonObject.has("fields") || !jsonObject.get("fields").isJsonArray() || jsonObject.getAsJsonArray("fields").isEmpty()) {
            throw new IllegalArgumentException("SelectField: " + name + " func: array requires fields parameter");
        }
        final List<String> fields = new ArrayList<>();
        final Map<String, Schema.Field> parents = new LinkedHashMap<>();
        Schema.FieldType valueType = null;
        for(final JsonElement element : jsonObject.getAsJsonArray("fields")) {
            final String field = element.getAsString();
            final Schema.FieldType type = ElementSchemaUtil.getInputFieldType(field, inputFields);
            if(valueType != null && valueType.getType() != type.getType()) {
                throw new IllegalArgumentException("SelectField: " + name + " func: array fields must have one type, but got: " + valueType + " and " + type);
            }
            valueType = type;
            fields.add(field);
            putParent(parents, field, inputFields);
        }
        return new ArrayFunctions(name, Op.array, null, fields, null, null, null, new ArrayList<>(parents.values()), Schema.FieldType.array(valueType), ignore);
    }

    private static ArrayFunctions ofAppend(final String name, final JsonObject jsonObject, final List<Schema.Field> inputFields, final boolean ignore) {
        if(!jsonObject.has("field")) {
            throw new IllegalArgumentException("SelectField: " + name + " func: array_append requires field parameter");
        }
        if(jsonObject.has("value") == jsonObject.has("valueField")) {
            throw new IllegalArgumentException("SelectField: " + name + " func: array_append requires either value or valueField parameter");
        }
        final String field = jsonObject.get("field").getAsString();
        final Schema.FieldType type = ElementSchemaUtil.getInputFieldType(field, inputFields);
        if(type.getType() != Schema.Type.array) {
            throw new IllegalArgumentException("SelectField: " + name + " func: array_append field: " + field + " must be an array, but got: " + type);
        }
        final Map<String, Schema.Field> parents = new LinkedHashMap<>();
        putParent(parents, field, inputFields);
        String valueField = null;
        if(jsonObject.has("valueField")) {
            valueField = jsonObject.get("valueField").getAsString();
            final Schema.FieldType valueType = ElementSchemaUtil.getInputFieldType(valueField, inputFields);
            final boolean numbers = Vectors.isNumeric(type.getArrayValueType()) && Vectors.isNumeric(valueType);
            if(!numbers && type.getArrayValueType().getType() != valueType.getType()) {
                throw new IllegalArgumentException("SelectField: " + name + " func: array_append valueField: " + valueField + " of type " + valueType + " can not be appended to " + type);
            }
            putParent(parents, valueField, inputFields);
        }
        String value = null;
        if(jsonObject.has("value")) {
            value = jsonObject.get("value").toString();
            // here rather than at setup: a value that can not be appended would fail every worker
            try {
                constantValue(type, value);
            } catch (final RuntimeException e) {
                throw new IllegalArgumentException("SelectField: " + name + " func: array_append value: " + value + " can not be appended to " + type, e);
            }
        }
        return new ArrayFunctions(name, Op.array_append, field, null, valueField, value, null, new ArrayList<>(parents.values()), type, ignore);
    }

    private static Object constantValue(final Schema.FieldType arrayType, final String value) {
        final JsonElement element = new Gson().fromJson(value, JsonElement.class);
        if(element.isJsonNull()) {
            throw new IllegalArgumentException("value must not be null");
        }
        return JsonToMapConverter.getAsPrimitiveValue(arrayType.getArrayValueType(), element);
    }

    private static ArrayFunctions ofRange(final String name, final JsonObject jsonObject, final List<Schema.Field> inputFields, final boolean ignore) {
        if(!jsonObject.has("field")) {
            throw new IllegalArgumentException("SelectField: " + name + " func: range requires field parameter");
        }
        final String field = jsonObject.get("field").getAsString();
        final Schema.FieldType type = ElementSchemaUtil.getInputFieldType(field, inputFields);
        // the length of a matrix with several dimensions other than 1 would be that of none of them
        final boolean vector = type.getType() == Schema.Type.matrix && type.getShape().stream().filter(dim -> dim != 1).count() <= 1;
        if(type.getType() != Schema.Type.array && !vector && !Vectors.isNumeric(type)) {
            throw new IllegalArgumentException("SelectField: " + name + " func: range field: " + field + " must be a number or an array, but got: " + type);
        }
        final Integer last = jsonObject.has("last") ? Integer.valueOf(jsonObject.get("last").getAsInt()) : null;
        if(last != null && last < 1) {
            throw new IllegalArgumentException("SelectField: " + name + " func: range last must be over than zero: " + last);
        }
        final Map<String, Schema.Field> parents = new LinkedHashMap<>();
        putParent(parents, field, inputFields);
        return new ArrayFunctions(name, Op.range, field, null, null, null, last, new ArrayList<>(parents.values()),
                Schema.FieldType.array(Schema.FieldType.INT64.withNullable(false)), ignore);
    }

    static void putParent(final Map<String, Schema.Field> parents, final String field, final List<Schema.Field> inputFields) {
        final String parent = ElementSchemaUtil.getInputFieldName(field, inputFields);
        parents.put(parent, Schema.Field.of(parent, ElementSchemaUtil.getInputFieldType(parent, inputFields)));
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
        if(value != null) {
            this.constant = constantValue(outputFieldType, value);
        }
    }

    @Override
    public Object apply(final Map<String, Object> input, final Instant timestamp) {
        return switch (op) {
            case array -> {
                final List<Object> values = new ArrayList<>(fields.size());
                for(final String item : fields) {
                    values.add(Tensor.materialize(ElementSchemaUtil.getValue(input, item)));
                }
                yield values;
            }
            case array_append -> {
                final Object values = ElementSchemaUtil.getValue(input, field);
                if(values == null) {
                    yield null;
                }
                final List<?> list = list(values);
                final List<Object> appended = new ArrayList<>(list.size() + 1);
                appended.addAll(list);
                appended.add(constant != null ? constant : element(ElementSchemaUtil.getValue(input, valueField)));
                yield appended;
            }
            case range -> {
                final Object source = ElementSchemaUtil.getValue(input, field);
                if(source == null) {
                    yield null;
                }
                final long length = source instanceof Number number ? number.longValue() : list(source).size();
                final List<Long> values = new ArrayList<>();
                for(long i=last == null ? 0 : Math.max(0, length - last); i<length; i++) {
                    values.add(i);
                }
                yield values;
            }
        };
    }

    private List<?> list(final Object value) {
        return switch (value) {
            case Tensor tensor -> tensor.asList();
            case List<?> list -> list;
            default -> throw new IllegalArgumentException("SelectField: " + name + " func: " + op + " field: " + field + " must be an array, but got: " + value);
        };
    }

    // as the values of the array: a token is an int64 whatever the type of the model output, and a row of a model output a list
    private Object element(final Object value) {
        if(!(value instanceof Number number)) {
            return Tensor.materialize(value);
        }
        return switch (outputFieldType.getArrayValueType().getType()) {
            case int8, int16, int32 -> number.intValue();
            case int64 -> number.longValue();
            case float16, float32 -> number.floatValue();
            case float64 -> number.doubleValue();
            default -> value;
        };
    }

}
