package com.mercari.solution.util.pipeline.select;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.schema.ElementSchemaUtil;
import org.joda.time.Instant;

import java.io.Serializable;
import java.util.*;

/**
 * Zips array fields of the same length to an array of structs. An item of {@code fields} is a field, whose last path
 * segment names the struct field, or {@code {name, field}}.
 *
 * <pre>{@code
 * { "name": "scores", "func": "array_zip", "fields": ["tok.tokens", { "name": "score", "field": "logits" }] }
 * }</pre>
 */
public class ArrayZip implements SelectFunction {

    private record Input(String name, String field) implements Serializable {}

    private final String name;
    private final List<Input> inputs;

    private final List<Schema.Field> inputFields;
    private final Schema.FieldType outputFieldType;
    private final boolean ignore;

    private ArrayZip(
            final String name,
            final List<Input> inputs,
            final List<Schema.Field> inputFields,
            final Schema.FieldType outputFieldType,
            final boolean ignore) {

        this.name = name;
        this.inputs = inputs;
        this.inputFields = inputFields;
        this.outputFieldType = outputFieldType;
        this.ignore = ignore;
    }

    public static ArrayZip of(final String name, final JsonObject jsonObject, final List<Schema.Field> inputFields, final boolean ignore) {
        if(!jsonObject.has("fields") || !jsonObject.get("fields").isJsonArray() || jsonObject.getAsJsonArray("fields").isEmpty()) {
            throw new IllegalArgumentException("SelectField: " + name + " func: array_zip requires fields parameter");
        }
        final List<Input> inputs = new ArrayList<>();
        // by name: fields of one parent such as tok.ids and tok.tokens read it once
        final Map<String, Schema.Field> parents = new LinkedHashMap<>();
        final Map<String, Schema.Field> structFields = new LinkedHashMap<>();
        for(final JsonElement element : jsonObject.getAsJsonArray("fields")) {
            final Input input = parseInput(name, element);
            final Schema.FieldType type = ElementSchemaUtil.getInputFieldType(input.field, inputFields);
            if(type.getType() != Schema.Type.array) {
                throw new IllegalArgumentException("SelectField: " + name + " func: array_zip field: " + input.field + " must be an array, but got: " + type);
            }
            if(structFields.put(input.name, Schema.Field.of(input.name, type.getArrayValueType().withNullable(true))) != null) {
                throw new IllegalArgumentException("SelectField: " + name + " func: array_zip has more than one " + input.name + ": name them with {name, field}");
            }
            inputs.add(input);
            final String parent = ElementSchemaUtil.getInputFieldName(input.field, inputFields);
            parents.put(parent, Schema.Field.of(parent, ElementSchemaUtil.getInputFieldType(parent, inputFields)));
        }
        final Schema.FieldType outputFieldType = Schema.FieldType.array(Schema.FieldType.element(new ArrayList<>(structFields.values())));
        return new ArrayZip(name, inputs, new ArrayList<>(parents.values()), outputFieldType, ignore);
    }

    private static Input parseInput(final String name, final JsonElement element) {
        if(element.isJsonPrimitive()) {
            final String field = element.getAsString();
            return new Input(field.substring(field.lastIndexOf('.') + 1), field);
        }
        if(!element.isJsonObject() || !element.getAsJsonObject().has("name") || !element.getAsJsonObject().has("field")) {
            throw new IllegalArgumentException("SelectField: " + name + " func: array_zip fields must be a field or {name, field}, but got: " + element);
        }
        return new Input(element.getAsJsonObject().get("name").getAsString(), element.getAsJsonObject().get("field").getAsString());
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
    public Object apply(final Map<String, Object> input, final Instant timestamp) {
        final List<List<?>> arrays = new ArrayList<>();
        for(final Input zipped : inputs) {
            final Object value = ElementSchemaUtil.getValue(input, zipped.field);
            if(value == null) {
                return null;
            }
            if(!(value instanceof List<?> list)) {
                throw new IllegalArgumentException("SelectField: " + name + " func: array_zip field: " + zipped.field + " must be an array, but got: " + value);
            }
            if(!arrays.isEmpty() && arrays.getFirst().size() != list.size()) {
                throw new IllegalArgumentException("SelectField: " + name + " func: array_zip requires arrays of the same length, but got "
                        + arrays.getFirst().size() + " and " + list.size() + " of " + zipped.field);
            }
            arrays.add(list);
        }
        final List<Map<String, Object>> zipped = new ArrayList<>();
        for(int i=0; i<arrays.getFirst().size(); i++) {
            final Map<String, Object> struct = new HashMap<>();
            for(int f=0; f<inputs.size(); f++) {
                struct.put(inputs.get(f).name, arrays.get(f).get(i));
            }
            zipped.add(struct);
        }
        return zipped;
    }

}
