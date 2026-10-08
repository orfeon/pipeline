package com.mercari.solution.util.pipeline.select;


import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.schema.ElementSchemaUtil;
import org.joda.time.Instant;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class Cast implements SelectFunction {

    private final String name;
    private final String field;
    private final List<Schema.Field> inputFields;
    private final Schema.FieldType outputFieldType;
    private final boolean ignore;

    Cast(String name, String field, List<Schema.Field> inputFields, Schema.FieldType outputFieldType, boolean ignore) {
        this.name = name;
        this.field = field;
        this.inputFields = inputFields;
        this.outputFieldType = outputFieldType;
        this.ignore = ignore;
    }

    public static Cast of(String name, JsonObject jsonObject, List<Schema.Field> inputFields, boolean ignore) {
        if(!jsonObject.has("type")) {
            throw new IllegalArgumentException("SelectField cast: " + name + " requires type parameter");
        }
        final String type = jsonObject.get("type").getAsString();

        final String field;
        if(jsonObject.has("field")) {
            if(!jsonObject.get("field").isJsonPrimitive()) {
                throw new IllegalArgumentException("SelectField cast: " + name + ".field parameter must be string");
            }
            field = jsonObject.get("field").getAsString();
        } else {
            field = name;
        }

        final List<Schema.Field> fields = new ArrayList<>();
        final Schema.FieldType inputFieldType = ElementSchemaUtil.getInputFieldType(field, inputFields);
        if(inputFieldType == null) {
            throw new IllegalArgumentException("SelectField cast: " + name + " missing inputField: " + field);
        }
        fields.add(Schema.Field.of(field, inputFieldType));

        final Schema.FieldType outputFieldType = Schema.FieldType.type(Schema.Type.of(type));
        return new Cast(name, field, fields, outputFieldType, ignore);
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
        final Schema.Type inputType = inputFields.getFirst().getFieldType().getType();
        if(Schema.Type.uuid.equals(outputFieldType.getType()) && Schema.Type.bytes.equals(inputType)) {
            return bytesToUuid(value);
        } else if(Schema.Type.bytes.equals(outputFieldType.getType()) && Schema.Type.uuid.equals(inputType)) {
            return uuidToBytes(value);
        }
        if(Schema.Type.decimal.equals(inputType)) {
            return castDecimal(value, inputFields.getFirst().getFieldType());
        }
        return ElementSchemaUtil.getAsPrimitive(outputFieldType, value);
    }

    // A decimal has no single representation: a BigDecimal (the json / fwf decoders), its text once
    // the element has passed the map coder, or the bytes of the unscaled value when it is read from
    // an avro record. All of them are read as the number first, so that the result of the cast does
    // not depend on what is upstream (nor on whether the runner fused the steps).
    private Object castDecimal(final Object value, final Schema.FieldType decimalType) {
        // a cast to bytes keeps the bytes as they are
        if(value == null || Schema.Type.bytes.equals(outputFieldType.getType())) {
            return ElementSchemaUtil.getAsPrimitive(outputFieldType, value);
        }
        BigDecimal decimal;
        try {
            decimal = ElementSchemaUtil.getAsBigDecimal(value, decimalType.getScale() == null ? 9 : decimalType.getScale());
        } catch (final NumberFormatException e) {
            decimal = null;
        }
        if(decimal == null) {
            return ElementSchemaUtil.getAsPrimitive(outputFieldType, value);
        }
        return switch (outputFieldType.getType()) {
            // plain notation without trailing zeros: neither "12.300000000" (the scale of the bytes) nor
            // "1E+2" / "1E-7" (BigDecimal.toString)
            case string, json -> decimal.stripTrailingZeros().toPlainString();
            default -> ElementSchemaUtil.getAsPrimitive(outputFieldType, decimal);
        };
    }

    private static String bytesToUuid(final Object value) {
        if(value == null) {
            return null;
        }
        final ByteBuffer buffer = switch (value) {
            case ByteBuffer b -> b.duplicate();
            case byte[] b -> ByteBuffer.wrap(b);
            default -> throw new IllegalArgumentException("UUID bytes value must be byte[] or ByteBuffer");
        };
        if(buffer.remaining() != 16) {
            throw new IllegalArgumentException("UUID bytes value must be exactly 16 bytes");
        }
        return new UUID(buffer.getLong(), buffer.getLong()).toString();
    }

    private static ByteBuffer uuidToBytes(final Object value) {
        if(value == null) {
            return null;
        }
        final UUID uuid = UUID.fromString(value.toString());
        return ByteBuffer.allocate(16)
                .putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits())
                .flip();
    }

}
