package com.mercari.solution.util.pipeline;

import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.schema.ElementSchemaUtil;

import java.io.Serializable;
import java.util.*;

public class Unnest implements Serializable {

    private final String flattenField;
    private final String outputField;

    public String getFlattenField() {
        return flattenField;
    }

    public String getOutputField() {
        return outputField;
    }

    private Unnest(final String flattenField) {
        this.flattenField = flattenField;
        if(flattenField == null) {
            this.outputField = null;
            return;
        }

        int lastDotIndex = flattenField.lastIndexOf(".");
        if(lastDotIndex == -1 || lastDotIndex == flattenField.length() - 1) {
            this.outputField = flattenField;
        } else {
            this.outputField = flattenField.substring(lastDotIndex + 1);
        }
    }

    public static Unnest of(final String flattenField) {
        return new Unnest(flattenField);
    }

    public static Schema createSchema(final List<Schema.Field> inputFields, final String flattenField) {
        final Schema.Field field = ElementSchemaUtil.getInputField(flattenField, inputFields);
        if(field == null) {
            throw new IllegalArgumentException("flattenField: " + flattenField + " does not exist in input schema: " + inputFields);
        }
        if(!Schema.Type.array.equals(field.getFieldType().getType())) {
            throw new IllegalArgumentException("flattenField: " + flattenField + " type: " + field.getFieldType() + " is not array type");
        }
        final List<Schema.Field> flattenFields = ElementSchemaUtil.setFieldType(
                inputFields, flattenField, field.getFieldType().getArrayValueType().withNullable(true));
        return Schema
                .builder(Schema.of(flattenFields))
                .build();
    }

    public void setup() {

    }

    public boolean useUnnest() {
        return flattenField != null;
    }

    public List<Map<String, Object>> unnest(final MElement input) {
        if(input == null) {
            return new ArrayList<>();
        }
        return unnest(input.asPrimitiveMap());
    }

    public List<Map<String, Object>> unnest(final Map<String, Object> primitiveValues) {
        final List<Map<String, Object>> outputs = new ArrayList<>();
        if (flattenField == null || primitiveValues == null || primitiveValues.isEmpty()) {
            outputs.add(primitiveValues);
            return outputs;
        }

        final List<?> flattenList = Optional
                .ofNullable((List<?>) ElementSchemaUtil.getValue(primitiveValues, flattenField))
                .orElseGet(ArrayList::new);
        if (flattenList.isEmpty()) {
            final Map<String, Object> flattenValues = new HashMap<>(primitiveValues);
            ElementSchemaUtil.setValue(flattenValues, flattenField,null);
            outputs.add(flattenValues);
        } else {
            for (final Object value : flattenList) {
                final Map<String, Object> flattenValues = ElementSchemaUtil.deepCopyMap(primitiveValues);
                ElementSchemaUtil.setValue(flattenValues, flattenField, value);
                outputs.add(flattenValues);
            }
        }
        return outputs;
    }

}
