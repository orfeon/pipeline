package com.mercari.solution.util.pipeline.select;

import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.schema.ElementSchemaUtil;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonPath implements SelectFunction {

    private static final Logger LOG = LoggerFactory.getLogger(JsonPath.class);

    private final String name;
    private final String field;
    private final String path;

    private final List<Schema.Field> inputFields;
    private final Schema.FieldType outputFieldType;
    private final boolean ignore;

    JsonPath(String name, String field, String path, Schema.Field inputField, Schema.FieldType outputFieldType, boolean ignore) {
        this.name = name;
        this.field = field;
        this.path = path;
        this.inputFields = new ArrayList<>();
        this.inputFields.add(inputField);
        this.outputFieldType = outputFieldType;
        this.ignore = ignore;
    }

    public static JsonPath of(String name, JsonObject jsonObject, List<Schema.Field> inputFields, boolean ignore) {
        if(!jsonObject.has("field")) {
            throw new IllegalArgumentException("SelectField: " + name + " requires field parameter");
        }
        if(!jsonObject.has("path")) {
            throw new IllegalArgumentException("SelectField: " + name + " requires path parameter");
        }

        final String field = jsonObject.get("field").getAsString();
        final String path = jsonObject.get("path").getAsString();
        final Schema.Field inputField = ElementSchemaUtil.getInputField(field, inputFields);

        Schema.FieldType outputFieldType;
        if(jsonObject.has("type")) {
            final String typeString = jsonObject.get("type").getAsString();
            final Schema.Type type = Schema.Type.of(typeString);
            outputFieldType = switch (type) {
                case element -> {
                    if(!jsonObject.has("fields")) {
                        throw new IllegalArgumentException("SelectField: " + name + " requires fields parameter when type is element");
                    }
                    yield Schema.FieldType.element(Schema.parse(jsonObject));
                }
                default -> Schema.FieldType.type(type);
            };
            if(jsonObject.has("mode")) {
                final String modeString = jsonObject.get("mode").getAsString();
                final Schema.Mode mode = Schema.Mode.valueOf(modeString);
                outputFieldType = switch (mode) {
                    case nullable -> outputFieldType.withNullable(true);
                    case required -> outputFieldType.withNullable(false);
                    case repeated -> Schema.FieldType.array(outputFieldType);
                };
            }
        } else {
            outputFieldType = Schema.FieldType.STRING.withNullable(true);
        }

        return new JsonPath(name, field, path, inputField, outputFieldType, ignore);
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
        final Object value = ElementSchemaUtil.getValue(input, field);
        final String json = (String)ElementSchemaUtil.getAsPrimitive(Schema.FieldType.STRING, value);
        try {
            final Object str = org.apache.beam.vendor.calcite.v1_40_0.com.jayway.jsonpath.JsonPath.read(json, path);
            return extract(outputFieldType, str);
        } catch (Throwable e) {
            return null;
        }
    }

    private static Object extract(final Schema.FieldType fieldType, Object value) {
        return switch (fieldType.getType()) {
            case array -> {
                final List<Object> list = new ArrayList<>();
                for(final Object element : (Iterable<? extends Object>) value) {
                    final Object object = extract(fieldType.getArrayValueType(), element);
                    list.add(object);
                }
                yield list;
            }
            case element -> switch (value) {
                case Map<?,?> map -> {
                    final Map<String, Object> result = new HashMap<>();
                    for(final Schema.Field f : fieldType.getElementSchema().getFields()) {
                        final Object mapValue = extract(f.getFieldType(), map.get(f.getName()));
                        result.put(f.getName(), mapValue);
                    }
                    yield result;
                }
                default -> value;
            };
            case map -> switch (value) {
                case Map<?,?> map -> {
                    final Map<String, Object> result = new HashMap<>();
                    for(final Map.Entry<?,?> entry : map.entrySet()) {
                        final Object mapValue = extract(fieldType.getMapValueType(), entry.getValue());
                        result.put(entry.getKey().toString(), mapValue);
                    }
                    for(final Schema.Field f : fieldType.getElementSchema().getFields()) {
                        final Object mapValue = extract(f.getFieldType(), map.get(f.getName()));
                        result.put(f.getName(), mapValue);
                    }
                    yield result;
                }
                default -> value;
            };
            default -> value;
        };
    }

}