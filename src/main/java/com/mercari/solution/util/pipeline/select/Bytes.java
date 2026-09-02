package com.mercari.solution.util.pipeline.select;

import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.HBaseBytes;
import com.mercari.solution.util.schema.ElementSchemaUtil;
import org.joda.time.Instant;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class Bytes implements SelectFunction {

    private final String name;
    private final String field;
    private final List<Schema.Field> inputFields;
    private final Schema.FieldType outputFieldType;
    private final boolean encode;
    private final boolean ignore;

    Bytes(String name, String field, List<Schema.Field> inputFields, Schema.FieldType outputFieldType, boolean encode, boolean ignore) {
        this.name = name;
        this.field = field;
        this.inputFields = inputFields;
        this.outputFieldType = outputFieldType;
        this.encode = encode;
        this.ignore = ignore;
    }

    public static Bytes of(String name, JsonObject jsonObject, List<Schema.Field> inputFields, boolean encode, boolean ignore) {
        final String field;
        if(jsonObject.has("field")) {
            if(!jsonObject.get("field").isJsonPrimitive()) {
                throw new IllegalArgumentException("SelectField bytes: " + name + ".field parameter must be string");
            }
            field = jsonObject.get("field").getAsString();
        } else {
            field = name;
        }

        final String type;
        if(!jsonObject.has("type")) {
            if(!encode) {
                throw new IllegalArgumentException("SelectField bytes: " + name + " requires type parameter");
            }
            type = "bytes";
        } else {
            type = jsonObject.get("type").getAsString();
        }

        final List<Schema.Field> fields = new ArrayList<>();
        final Schema.FieldType inputFieldType = ElementSchemaUtil.getInputFieldType(field, inputFields);
        if(inputFieldType == null) {
            throw new IllegalArgumentException("SelectField bytes: " + name + " missing inputField: " + field);
        } else if(!Schema.Type.bytes.equals(inputFieldType.getType()) && !encode) {
            throw new IllegalArgumentException("SelectField bytes: " + name + " input inputField must be bytes but : " + inputFieldType.getType());
        }
        fields.add(Schema.Field.of(field, inputFieldType));

        final Schema.FieldType outputFieldType = Schema.FieldType.type(Schema.Type.of(type));
        return new Bytes(name, field, fields, outputFieldType, encode, ignore);
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
        final Object primitiveValue = ElementSchemaUtil.getValue(input, field);
        if(primitiveValue == null) {
            return null;
        }
        if(encode) {
            return encode(primitiveValue);
        } else {
            return switch (primitiveValue) {
                case byte[] bytes -> decode(outputFieldType.getType(), bytes);
                case ByteBuffer byteBuffer -> decode(outputFieldType.getType(), byteBuffer.array());
                default -> throw new IllegalArgumentException();
            };
        }
    }

    private static Object decode(final Schema.Type type, byte[] bytes) {
        return switch (type) {
            case bool -> HBaseBytes.toBoolean(bytes);
            case string, json -> HBaseBytes.toString(bytes);
            case uuid -> UUID.fromString(HBaseBytes.toString(bytes)).toString();
            case int16 -> HBaseBytes.toShort(bytes);
            case int32, date -> HBaseBytes.toInt(bytes);
            case int64, time, timestamp -> HBaseBytes.toLong(bytes);
            case float32 -> HBaseBytes.toFloat(bytes);
            case float64 -> HBaseBytes.toDouble(bytes);
            case decimal -> HBaseBytes.toBigDecimal(bytes);
            case bytes -> ByteBuffer.wrap(bytes);
            default -> throw new IllegalArgumentException("");
        };
    }

    private static ByteBuffer encode(final Object object) {
        if(object == null) {
            return null;
        }
        final byte[] bytes = switch (object) {
            case Boolean b -> HBaseBytes.toBytes(b);
            case String s -> HBaseBytes.toBytes(s);
            case Short s -> HBaseBytes.toBytes(s);
            case Integer i -> HBaseBytes.toBytes(i);
            case Long l -> HBaseBytes.toBytes(l);
            case Float f -> HBaseBytes.toBytes(f);
            case Double d -> HBaseBytes.toBytes(d);
            case BigDecimal b -> HBaseBytes.toBytes(b);
            case ByteBuffer b -> b.array();
            case byte[] b -> b;
            default -> throw new IllegalArgumentException();
        };
        return ByteBuffer.wrap(bytes);
    }

}
