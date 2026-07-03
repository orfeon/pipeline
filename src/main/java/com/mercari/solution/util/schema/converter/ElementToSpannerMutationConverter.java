package com.mercari.solution.util.schema.converter;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.schema.StructSchemaUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.values.Row;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class ElementToSpannerMutationConverter {

    public static Mutation convert(
            final Schema schema,
            final MElement element,
            final String table,
            final String mutationOp,
            final List<String> keyFields,
            final List<String> allowCommitTimestampFields) {

        return switch (element.getType()) {
            case ELEMENT -> convert(schema, (Map<String, Object>) element.getValue(),
                    table, mutationOp,
                    keyFields, allowCommitTimestampFields, null);
            case AVRO -> AvroToMutationConverter
                    .convert(schema.getAvroSchema(), (GenericRecord) element.getValue(),
                            table, mutationOp,
                            keyFields, allowCommitTimestampFields, null, null);
            case ROW -> RowToMutationConverter
                    .convert(schema.getRowSchema(), (Row) element.getValue(),
                            table, mutationOp, keyFields,
                            allowCommitTimestampFields, null, null);
            case STRUCT -> StructToMutationConverter
                    .convert(schema.getRowSchema(), (Struct) element.getValue(),
                            table, mutationOp,
                            keyFields, allowCommitTimestampFields, null, null);
            default -> throw new IllegalArgumentException();
        };
    }

    private static Mutation convert(
            final Schema schema,
            final Map<String, Object> primitiveValues,
            final String table,
            final String mutationOp,
            final List<String> keyFields,
            final List<String> allowCommitTimestampFields,
            final Set<String> excludeFields) {

        if(mutationOp != null && "DELETE".equalsIgnoreCase(mutationOp.trim())) {
            if(keyFields == null) {
                throw new IllegalArgumentException("keyFields is null. Set keyFields when using mutationOp:DELETE");
            }

            final List<Schema.Field> keySchemaFields = keyFields.stream()
                    .map(schema::getField)
                    .filter(Objects::nonNull)
                    .toList();
            final Key key = createKey(primitiveValues, keySchemaFields);
            return Mutation.delete(table, key);
        }

        final Mutation.WriteBuilder builder = StructSchemaUtil.createMutationWriteBuilder(table, mutationOp);
        for(Schema.Field field : schema.getFields()) {
            if(excludeFields != null && excludeFields.contains(field.getName())) {
                continue;
            }
            final String fieldName = field.getName();
            final Object value = primitiveValues.getOrDefault(fieldName, null);
            final boolean isCommitTimestampField = allowCommitTimestampFields != null && allowCommitTimestampFields.contains(fieldName);
            setValue(builder, fieldName, field.getFieldType(), value, isCommitTimestampField);
        }

        if(allowCommitTimestampFields != null) {
            for(final String commitTimestampField : allowCommitTimestampFields) {
                if(schema.getField(commitTimestampField) == null) {
                    builder.set(commitTimestampField).to(Value.COMMIT_TIMESTAMP);
                }
            }
        }
        return builder.build();
    }

    public static Key createKey(final Map<String, Object> values, final Iterable<Schema.Field> keyFields) {
        Key.Builder keyBuilder = Key.newBuilder();
        for(final Schema.Field field : keyFields) {
            final Object fieldValue = values.get(field.getName());
            switch (field.getFieldType().getType()) {
                case bool -> keyBuilder = keyBuilder.append((Boolean)fieldValue);
                case enumeration, string -> keyBuilder = keyBuilder.append(fieldValue == null ? null : fieldValue.toString());
                case float32 -> keyBuilder = keyBuilder.append((Float)fieldValue);
                case float64 -> keyBuilder = keyBuilder.append((Double)fieldValue);
                case bytes -> {
                    if(fieldValue == null) {
                        keyBuilder = keyBuilder.append((ByteArray) null);

                    } else {
                        final ByteArray bytes = ByteArray.copyFrom(((ByteBuffer) fieldValue).array());
                        keyBuilder = keyBuilder.append(bytes);
                    }
                }
                case int32 -> {
                    final Integer intValue = (Integer)fieldValue;
                    keyBuilder = keyBuilder.append(intValue);
                }
                case int64 -> {
                    final Long longValue = (Long)fieldValue;
                    keyBuilder = keyBuilder.append(longValue);
                }
                default -> throw new IllegalStateException();
            }
        }
        return keyBuilder.build();
    }

    private static void setValue(final Mutation.WriteBuilder builder,
                                 final String fieldName,
                                 final Schema.FieldType fieldType,
                                 final Object value,
                                 final boolean isCommitTimestampField) {

        final boolean isNullField = value == null;
        switch(fieldType.getType()) {
            case bool -> {
                final Boolean booleanValue = (Boolean) value;
                builder.set(fieldName).to(booleanValue);
            }
            case enumeration, string -> {
                final String stringValue = Optional.ofNullable(value).map(Object::toString).orElse(null);
                builder.set(fieldName).to(stringValue);
            }
            case json -> {
                final String jsonValue = Optional.ofNullable(value).map(Object::toString).orElse(null);
                builder.set(fieldName).to(Value.json(jsonValue));
            }
            case bytes -> {
                final ByteArray bytesValue;
                if(value == null) {
                    bytesValue = null;
                } else {
                    bytesValue = ByteArray.copyFrom(((ByteBuffer) value).array());
                }
                builder.set(fieldName).to(bytesValue);
            }
            case int32 -> {
                final Long intValue = Optional.ofNullable(value).map(v -> Long.valueOf((Integer) v)).orElse(null);
                builder.set(fieldName).to(intValue);
            }
            case int64 -> {
                final Long longValue = (Long) value;
                builder.set(fieldName).to(longValue);
            }
            case float32 -> {
                final Float floatValue = (Float) value;
                builder.set(fieldName).to(floatValue);
            }
            case float64 -> {
                final Double doubleValue = (Double) value;
                builder.set(fieldName).to(doubleValue);
            }
            case date -> {
                // date is represented as epoch days (Integer)
                final Date dateValue;
                if(isNullField) {
                    dateValue = null;
                } else {
                    final LocalDate localDate = LocalDate.ofEpochDay(((Number) value).longValue());
                    dateValue = Date.fromYearMonthDay(
                            localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth());
                }
                builder.set(fieldName).to(dateValue);
            }
            case time -> {
                // time is represented as micros of day (Long). Spanner has no TIME type: write as string
                final String timeValue;
                if(isNullField) {
                    timeValue = null;
                } else {
                    timeValue = LocalTime
                            .ofNanoOfDay(((Number) value).longValue() * 1000L)
                            .format(DateTimeFormatter.ISO_LOCAL_TIME);
                }
                builder.set(fieldName).to(timeValue);
            }
            case timestamp -> {
                // timestamp is represented as epoch micros (Long)
                if(isCommitTimestampField) {
                    builder.set(fieldName).to(Value.COMMIT_TIMESTAMP);
                } else {
                    final Timestamp timestampValue;
                    if(isNullField) {
                        timestampValue = null;
                    } else {
                        timestampValue = Timestamp.ofTimeMicroseconds(((Number) value).longValue());
                    }
                    builder.set(fieldName).to(timestampValue);
                }
            }
        }
    }

}
