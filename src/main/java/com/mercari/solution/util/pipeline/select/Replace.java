package com.mercari.solution.util.pipeline.select;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.schema.ElementSchemaUtil;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Replace implements SelectFunction {

    private static final Logger LOG = LoggerFactory.getLogger(Replace.class);

    private final String name;
    private final String field;
    private final Map<String, String> mapping;
    private final String defaultValue;
    private final Schema.FieldType inputFieldType;

    private final List<Schema.Field> inputFields;
    private final Schema.FieldType outputFieldType;
    private final boolean ignore;

    Replace(String name, String field, Map<String, String> mapping, String defaultValue, Schema.Field inputField, Schema.FieldType outputFieldType, boolean ignore) {
        this.name = name;
        this.field = field;
        this.mapping = mapping;
        this.defaultValue = defaultValue;
        this.inputFieldType = inputField.getFieldType();
        this.inputFields = new ArrayList<>();
        this.inputFields.add(inputField);
        this.outputFieldType = outputFieldType;
        this.ignore = ignore;
    }

    public static Replace of(String name, JsonObject jsonObject, List<Schema.Field> inputFields, boolean ignore) {
        if(!jsonObject.has("field")) {
            throw new IllegalArgumentException("SelectField: " + name + " requires field parameter");
        }

        final Map<String, String> mapping = new HashMap<>();
        if(jsonObject.has("mapping")) {
            final JsonObject mappingJsonObject = jsonObject.getAsJsonObject("mapping");
            for(final Map.Entry<String, JsonElement> entry : mappingJsonObject.entrySet()) {
                mapping.put(entry.getKey(), entry.getValue().getAsString());
            }
        }

        final String defaultValue;
        if(jsonObject.has("default")) {
            defaultValue = jsonObject.get("default").getAsString();
        } else {
            defaultValue = null;
        }

        final String inputFieldName;
        final String field = jsonObject.get("field").getAsString();
        if(field.contains(".")) {
            final String[] fields = field.split("\\.", 2);
            inputFieldName = fields[0];
        } else {
            inputFieldName = field;
        }
        final Schema.FieldType inputFieldType = ElementSchemaUtil.getInputFieldType(inputFieldName, inputFields);
        final Schema.Field inputField = Schema.Field.of(inputFieldName, inputFieldType);

        final Schema.FieldType outputFieldType;
        if(jsonObject.has("type")) {
            final String type = jsonObject.get("type").getAsString();
            outputFieldType = Schema.FieldType.type(Schema.Type.of(type));
        } else {
            outputFieldType = ElementSchemaUtil.getInputFieldType(field, inputFields);
        }

        switch (inputFieldType.getType()) {
            case enumeration -> {
                if(!inputFieldType.getSymbols().containsAll(mapping.keySet())) {
                    LOG.warn("replace mapping keys: " + mapping.keySet() + " are not included in enum symbols: " + inputFieldType.getSymbols());
                    //throw new IllegalModuleException("replace mapping keys: " + mapping.keySet() + " are not included in enum symbols: " + inputFieldType.getSymbols());
                }
            }
        }

        return new Replace(name, field, mapping, defaultValue, inputField, outputFieldType, ignore);
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

    @Override
    public Object apply(Map<String, Object> input, Instant timestamp) {
        Object value = ElementSchemaUtil.getValue(input, field);
        if(value == null) {
            return null;
        }
        value = switch (inputFieldType.getType()) {
            case enumeration -> switch (value) {
                case Integer i -> {
                    if(i < inputFieldType.getSymbols().size()) {
                        yield inputFieldType.getSymbols().get(i);
                    } else {
                        yield defaultValue;
                    }
                }
                case Number n -> inputFieldType.getSymbols().get(n.intValue());
                case String s -> s;
                default -> value;
            };
            default -> value;
        };

        final String mappingValue = mapping.getOrDefault(value.toString(), defaultValue);
        if(mappingValue == null) {
            return null;
        }
        return ElementSchemaUtil.getAsPrimitive(outputFieldType, mappingValue);
    }

}