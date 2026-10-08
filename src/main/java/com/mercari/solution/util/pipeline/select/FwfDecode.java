package com.mercari.solution.util.pipeline.select;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.schema.ElementSchemaUtil;
import com.mercari.solution.util.schema.fwf.FwfDecoder;
import org.joda.time.Instant;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Decodes one fixed-width record held in a bytes or string field into a record value
 * (work_fixedwidth.md §7), with the same decoder as the storage source's {@code format: fwf}.
 *
 * <pre>
 * { "name": "order", "func": "fwf_decode", "field": "payload",
 *   "schema": { "encoding": { "format": "fwf", "charset": "windows-31j" },
 *               "reference": { "uri": "gs://bucket/layouts/orders.fwf.json" } } }
 * </pre>
 *
 * The output type is the record of the layout fields (narrowed by the schema's declared fields and
 * then by {@code fields}). A record that can not be decoded throws: the select transform routes it
 * like any other failing record.
 */
public class FwfDecode implements SelectFunction {

    private final String name;
    private final String field;
    private final List<Schema.Field> inputFields;
    private final Schema.FieldType outputFieldType;
    private final FwfDecoder decoder;
    private final boolean ignore;

    FwfDecode(
            final String name,
            final String field,
            final List<Schema.Field> inputFields,
            final Schema.FieldType outputFieldType,
            final FwfDecoder decoder,
            final boolean ignore) {

        this.name = name;
        this.field = field;
        this.inputFields = inputFields;
        this.outputFieldType = outputFieldType;
        this.decoder = decoder;
        this.ignore = ignore;
    }

    public static FwfDecode of(
            final String name,
            final JsonObject jsonObject,
            final List<Schema.Field> inputFields,
            final boolean ignore) {

        final String field = SelectFunction.getStringParameter(name, jsonObject, "field", name);
        // throws when the field is not in the input
        final Schema.FieldType inputFieldType = ElementSchemaUtil.getInputFieldType(field, inputFields);
        switch (inputFieldType.getType()) {
            case bytes, string -> {}
            default -> throw new IllegalArgumentException("SelectField fwf_decode: " + name
                    + " inputField: " + field + " must be bytes or string. but: " + inputFieldType.getType());
        }

        if(!jsonObject.has("schema") || !jsonObject.get("schema").isJsonObject()) {
            throw new IllegalArgumentException("SelectField fwf_decode: " + name
                    + " requires schema parameter (encoding.format: fwf with the layout in reference)");
        }
        final Schema schema;
        try {
            schema = Schema.parse(jsonObject.getAsJsonObject("schema"));
        } catch (final RuntimeException e) {
            throw new IllegalArgumentException("SelectField fwf_decode: " + name + ".schema: "
                    + (e.getMessage() == null ? "it defines no fields (declare encoding.format: fwf with reference)" : e.getMessage()), e);
        }
        if(schema == null || schema.getFwfLayout() == null) {
            throw new IllegalArgumentException("SelectField fwf_decode: " + name
                    + ".schema must declare encoding.format: fwf with the layout in reference (uri or inline)");
        }

        final List<String> projection = new ArrayList<>();
        if(jsonObject.has("fields")) {
            final JsonElement fields = jsonObject.get("fields");
            if(!fields.isJsonArray()) {
                throw new IllegalArgumentException("SelectField fwf_decode: " + name + ".fields must be an array of field names");
            }
            for(final JsonElement element : fields.getAsJsonArray()) {
                if(!element.isJsonPrimitive()) {
                    throw new IllegalArgumentException("SelectField fwf_decode: " + name + ".fields must be an array of field names");
                }
                projection.add(element.getAsString());
            }
        }

        final FwfDecoder decoder;
        try {
            decoder = FwfDecoder.of(schema, projection);
        } catch (final IllegalArgumentException e) {
            throw new IllegalArgumentException("SelectField fwf_decode: " + name + ".fields: " + e.getMessage(), e);
        }
        // nullable (the default of a field type): a null input gives a null record
        final Schema.FieldType outputFieldType = Schema.FieldType.element(decoder.getLayout().toSchemaFields());

        final List<Schema.Field> fields = new ArrayList<>();
        fields.add(Schema.Field.of(field, inputFieldType));
        return new FwfDecode(name, field, fields, outputFieldType, decoder, ignore);
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
        final Object value = ElementSchemaUtil.getValue(input, field);
        return switch (value) {
            case null -> null;
            case byte[] bytes -> decoder.decode(bytes);
            // a heap buffer is read in place (it may be a slice of a larger array); a read-only or direct one is copied
            case ByteBuffer buffer -> buffer.hasArray()
                    ? decoder.decode(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining())
                    : decoder.decode(ElementSchemaUtil.toBytes(buffer));
            // with unit: byte the text is encoded back with the charset of the schema before it is cut
            case String text -> decoder.decode(text);
            default -> throw new IllegalArgumentException("SelectField fwf_decode: " + name
                    + " inputField: " + field + " must be bytes or string. but: " + value.getClass().getSimpleName());
        };
    }

}
