package com.mercari.solution.util.schema.converter;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.Timestamps;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.ProtoSchemaUtil;
import com.mercari.solution.util.ResourceUtil;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.schemas.logicaltypes.Date;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;
import java.util.stream.Collectors;

public class ProtoToAvroConverterTest {

    @Test
    public void testToSchema() {
        final byte[] descBytes = ResourceUtil.getResourceFileAsBytes("schema/test.desc");
        final Map<String, Descriptors.Descriptor> descriptors = ProtoSchemaUtil.getDescriptors(descBytes);
        final Descriptors.Descriptor descriptor = descriptors.get("com.mercari.solution.entity.TestMessage");
        final Schema schema = ProtoToAvroConverter.convertSchema(descriptor);

        assertSchemaFields(schema, descriptor);

        final Schema childSchema = getFieldSchema(schema, "child");
        final Descriptors.Descriptor childDescriptor = ProtoSchemaUtil.getField(descriptor, "child").getMessageType();
        assertSchemaFields(childSchema, childDescriptor);

        final Schema grandchildSchema = getFieldSchema(childSchema, "grandchild");
        final Descriptors.Descriptor grandchildDescriptor = ProtoSchemaUtil.getField(childDescriptor, "grandchild").getMessageType();
        assertSchemaFields(grandchildSchema, grandchildDescriptor);
    }

    @Test
    public void testToRecord() throws Exception {
        testToRecord("data/test.pb");
    }

    @Test
    public void testToRecordNull() throws Exception {
        testToRecord("data/test_null.pb");
    }

    private void testToRecord(final String protoPath) throws Exception {

        final byte[] descBytes = ResourceUtil.getResourceFileAsBytes("schema/test.desc");
        final byte[] protoBytes = ResourceUtil.getResourceFileAsBytes(protoPath);

        final Map<String, Descriptors.Descriptor> descriptors = ProtoSchemaUtil.getDescriptors(descBytes);
        final Descriptors.Descriptor descriptor = descriptors.get("com.mercari.solution.entity.TestMessage");
        final Schema schema = ProtoToAvroConverter.convertSchema(descriptor);
        final DynamicMessage message = ProtoSchemaUtil.convert(descriptor, protoBytes);

        final JsonFormat.Printer printer = ProtoSchemaUtil.createJsonPrinter(descriptors);

        final GenericRecord record = ProtoToAvroConverter.convert(schema, descriptor, protoBytes, printer);
        assertRecordValues(message, record, printer);

        if(ProtoSchemaUtil.hasField(message, "child")) {
            final GenericRecord child = (GenericRecord) record.get("child");
            final DynamicMessage childMessage = (DynamicMessage) ProtoSchemaUtil.getFieldValue(message, "child");
            assertRecordValues(childMessage, child, printer);

            final List<GenericRecord> grandchildren = (List<GenericRecord>)child.get("grandchildren");
            final List<DynamicMessage> grandchildrenMessages = (List<DynamicMessage>) ProtoSchemaUtil.getFieldValue(childMessage, "grandchildren");
            int i = 0;
            for(final GenericRecord r : grandchildren) {
                assertRecordValues(grandchildrenMessages.get(i), r, printer);
                i++;
            }

            if(ProtoSchemaUtil.hasField(childMessage, "grandchild")) {
                final GenericRecord grandchild = (GenericRecord) child.get("grandchild");
                final DynamicMessage grandchildMessage = (DynamicMessage) ProtoSchemaUtil.getFieldValue(childMessage, "grandchild");
                assertRecordValues(grandchildMessage, grandchild, printer);
            }
        }

        final List<GenericRecord> children = (List<GenericRecord>)record.get("children");
        final List<DynamicMessage> childrenMessages = (List<DynamicMessage>) ProtoSchemaUtil.getFieldValue(message, "children");
        int i = 0;
        for(final GenericRecord r : children) {
            assertRecordValues(childrenMessages.get(i), r, printer);
            i++;
        }

    }

    private void assertSchemaFields(final Schema schema, final Descriptors.Descriptor descriptor) {

        // Build-in types
        Assertions.assertEquals(Schema.Type.BOOLEAN, getFieldSchema(schema, "boolValue").getType());
        Assertions.assertEquals(Schema.Type.STRING, getFieldSchema(schema, "stringValue").getType());
        Assertions.assertEquals(Schema.Type.BYTES, getFieldSchema(schema, "bytesValue").getType());
        Assertions.assertEquals(Schema.Type.INT, getFieldSchema(schema, "intValue").getType());
        Assertions.assertEquals(Schema.Type.LONG, getFieldSchema(schema, "longValue").getType());
        Assertions.assertEquals(Schema.Type.FLOAT, getFieldSchema(schema, "floatValue").getType());
        Assertions.assertEquals(Schema.Type.DOUBLE, getFieldSchema(schema, "doubleValue").getType());
        Assertions.assertEquals(Schema.Type.INT, getFieldSchema(schema, "uintValue").getType());
        Assertions.assertEquals(Schema.Type.LONG, getFieldSchema(schema, "ulongValue").getType());

        // Google provided types
        Assertions.assertEquals(Schema.Type.INT, getFieldSchema(schema, "dateValue").getType());
        Assertions.assertEquals(LogicalTypes.date(), getFieldSchema(schema, "dateValue").getLogicalType());
        Assertions.assertEquals(Schema.Type.LONG, getFieldSchema(schema, "timeValue").getType());
        Assertions.assertEquals(LogicalTypes.timeMicros(), getFieldSchema(schema, "timeValue").getLogicalType());
        Assertions.assertEquals(Schema.Type.LONG, getFieldSchema(schema, "datetimeValue").getType());
        Assertions.assertEquals(LogicalTypes.timestampMicros(), getFieldSchema(schema, "datetimeValue").getLogicalType());
        Assertions.assertEquals(Schema.Type.LONG, getFieldSchema(schema, "timestampValue").getType());
        Assertions.assertEquals(LogicalTypes.timestampMicros(), getFieldSchema(schema, "timestampValue").getLogicalType());

        // Google provided types wrappedValues
        Assertions.assertEquals(Schema.Type.BOOLEAN, getFieldSchema(schema, "wrappedBoolValue").getType());
        Assertions.assertEquals(Schema.Type.STRING, getFieldSchema(schema, "wrappedStringValue").getType());
        Assertions.assertEquals(Schema.Type.BYTES, getFieldSchema(schema, "wrappedBytesValue").getType());
        Assertions.assertEquals(Schema.Type.INT, getFieldSchema(schema, "wrappedInt32Value").getType());
        Assertions.assertEquals(Schema.Type.LONG, getFieldSchema(schema, "wrappedInt64Value").getType());
        Assertions.assertEquals(Schema.Type.FLOAT, getFieldSchema(schema, "wrappedFloatValue").getType());
        Assertions.assertEquals(Schema.Type.DOUBLE, getFieldSchema(schema, "wrappedDoubleValue").getType());
        Assertions.assertEquals(Schema.Type.INT, getFieldSchema(schema, "wrappedUInt32Value").getType());
        Assertions.assertEquals(Schema.Type.LONG, getFieldSchema(schema, "wrappedUInt64Value").getType());

        // Any
        Assertions.assertEquals(Schema.Type.STRING, getFieldSchema(schema, "anyValue").getType());

        // Enum
        Assertions.assertEquals(Schema.Type.ENUM, getFieldSchema(schema, "enumValue").getType());
        Assertions.assertEquals(
                ProtoSchemaUtil.getField(descriptor, "enumValue").getEnumType().getValues().stream()
                        .map(Descriptors.EnumValueDescriptor::getName)
                        .collect(Collectors.toList()),
                getFieldSchema(schema, "enumValue").getEnumSymbols());

        // OneOf
        Assertions.assertEquals(Schema.Type.STRING, getFieldSchema(schema, "entityName").getType());
        Assertions.assertEquals(Schema.Type.INT, getFieldSchema(schema, "entityAge").getType());

        // Map
        Assertions.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "strIntMapValue").getType());
        Assertions.assertEquals(Schema.Type.RECORD, getFieldSchema(schema, "strIntMapValue").getElementType().getType());
        Assertions.assertEquals(Schema.Type.STRING, getFieldSchema(getFieldSchema(schema, "strIntMapValue").getElementType(), "key").getType());
        Assertions.assertEquals(Schema.Type.INT, getFieldSchema(getFieldSchema(schema, "strIntMapValue").getElementType(), "value").getType());
        Assertions.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "longDoubleMapValue").getType());
        Assertions.assertEquals(Schema.Type.RECORD, getFieldSchema(schema, "longDoubleMapValue").getElementType().getType());
        Assertions.assertEquals(Schema.Type.LONG, getFieldSchema(getFieldSchema(schema, "longDoubleMapValue").getElementType(), "key").getType());
        Assertions.assertEquals(Schema.Type.DOUBLE, getFieldSchema(getFieldSchema(schema, "longDoubleMapValue").getElementType(), "value").getType());

        // Repeated
        Assertions.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "boolValues").getType());
        Assertions.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "stringValues").getType());
        Assertions.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "bytesValues").getType());
        Assertions.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "intValues").getType());
        Assertions.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "longValues").getType());
        Assertions.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "floatValues").getType());
        Assertions.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "doubleValues").getType());
        Assertions.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "uintValues").getType());
        Assertions.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "ulongValues").getType());
        Assertions.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "dateValues").getType());
        Assertions.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "timeValues").getType());
        Assertions.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "datetimeValues").getType());
        Assertions.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "wrappedBoolValues").getType());
        Assertions.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "wrappedStringValues").getType());
        Assertions.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "wrappedBytesValues").getType());
        Assertions.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "wrappedInt32Values").getType());
        Assertions.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "wrappedInt64Values").getType());
        Assertions.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "wrappedUInt32Values").getType());
        Assertions.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "wrappedUInt64Values").getType());
        Assertions.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "wrappedFloatValues").getType());
        Assertions.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "wrappedDoubleValues").getType());
        Assertions.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "anyValues").getType());
        Assertions.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "enumValues").getType());

        Assertions.assertEquals(Schema.Type.BOOLEAN, getFieldSchema(schema, "boolValues").getElementType().getType());
        Assertions.assertEquals(Schema.Type.STRING, getFieldSchema(schema, "stringValues").getElementType().getType());
        Assertions.assertEquals(Schema.Type.BYTES, getFieldSchema(schema, "bytesValues").getElementType().getType());
        Assertions.assertEquals(Schema.Type.INT, getFieldSchema(schema, "intValues").getElementType().getType());
        Assertions.assertEquals(Schema.Type.LONG, getFieldSchema(schema, "longValues").getElementType().getType());
        Assertions.assertEquals(Schema.Type.FLOAT, getFieldSchema(schema, "floatValues").getElementType().getType());
        Assertions.assertEquals(Schema.Type.DOUBLE, getFieldSchema(schema, "doubleValues").getElementType().getType());
        Assertions.assertEquals(Schema.Type.INT, getFieldSchema(schema, "uintValues").getElementType().getType());
        Assertions.assertEquals(Schema.Type.LONG, getFieldSchema(schema, "ulongValues").getElementType().getType());
        Assertions.assertEquals(Schema.Type.INT, getFieldSchema(schema, "dateValues").getElementType().getType());
        Assertions.assertEquals(LogicalTypes.date(), getFieldSchema(schema, "dateValues").getElementType().getLogicalType());
        Assertions.assertEquals(Schema.Type.LONG, getFieldSchema(schema, "timeValues").getElementType().getType());
        Assertions.assertEquals(LogicalTypes.timeMicros(), getFieldSchema(schema, "timeValues").getElementType().getLogicalType());
        Assertions.assertEquals(Schema.Type.LONG, getFieldSchema(schema, "datetimeValues").getElementType().getType());
        Assertions.assertEquals(LogicalTypes.timestampMicros(), getFieldSchema(schema, "datetimeValues").getElementType().getLogicalType());
        Assertions.assertEquals(Schema.Type.BOOLEAN, getFieldSchema(schema, "wrappedBoolValues").getElementType().getType());
        Assertions.assertEquals(Schema.Type.STRING, getFieldSchema(schema, "wrappedStringValues").getElementType().getType());
        Assertions.assertEquals(Schema.Type.BYTES, getFieldSchema(schema, "wrappedBytesValues").getElementType().getType());
        Assertions.assertEquals(Schema.Type.INT, getFieldSchema(schema, "wrappedInt32Values").getElementType().getType());
        Assertions.assertEquals(Schema.Type.LONG, getFieldSchema(schema, "wrappedInt64Values").getElementType().getType());
        Assertions.assertEquals(Schema.Type.INT, getFieldSchema(schema, "wrappedUInt32Values").getElementType().getType());
        Assertions.assertEquals(Schema.Type.LONG, getFieldSchema(schema, "wrappedUInt64Values").getElementType().getType());
        Assertions.assertEquals(Schema.Type.FLOAT, getFieldSchema(schema, "wrappedFloatValues").getElementType().getType());
        Assertions.assertEquals(Schema.Type.DOUBLE, getFieldSchema(schema, "wrappedDoubleValues").getElementType().getType());
        Assertions.assertEquals(Schema.Type.STRING, getFieldSchema(schema, "anyValues").getElementType().getType());
        Assertions.assertEquals(Schema.Type.ENUM, getFieldSchema(schema, "enumValues").getElementType().getType());

        Assertions.assertEquals(
                ProtoSchemaUtil.getField(descriptor, "enumValues").getEnumType().getValues().stream()
                        .map(Descriptors.EnumValueDescriptor::getName)
                        .collect(Collectors.toList()),
                getFieldSchema(schema, "enumValues").getElementType().getEnumSymbols());
    }

    private void assertRecordValues(final DynamicMessage message, final GenericRecord record, final JsonFormat.Printer printer) throws InvalidProtocolBufferException {

        // Build-in type
        Assertions.assertEquals(ProtoSchemaUtil.getValue(message, "boolValue", printer), record.get("boolValue"));
        Assertions.assertEquals(ProtoSchemaUtil.getValue(message, "stringValue", printer), record.get("stringValue"));
        Assertions.assertEquals(
                new String((byte[]) ProtoSchemaUtil.getValue(message, "bytesValue", printer), StandardCharsets.UTF_8),
                new String(((ByteBuffer)record.get("bytesValue")).array(), StandardCharsets.UTF_8));
        Assertions.assertEquals(ProtoSchemaUtil.getValue(message, "intValue", printer), record.get("intValue"));
        Assertions.assertEquals(ProtoSchemaUtil.getValue(message, "longValue", printer), record.get("longValue"));
        Assertions.assertEquals(ProtoSchemaUtil.getValue(message, "floatValue", printer), record.get("floatValue"));
        Assertions.assertEquals(ProtoSchemaUtil.getValue(message, "doubleValue", printer), record.get("doubleValue"));
        Assertions.assertEquals(ProtoSchemaUtil.getValue(message, "uintValue", printer), record.get("uintValue"));
        Assertions.assertEquals(ProtoSchemaUtil.getValue(message, "ulongValue", printer), record.get("ulongValue"));

        // Google-provided type
        Assertions.assertEquals(
                (int)((LocalDate) ProtoSchemaUtil.getValue(message, "dateValue", printer)).toEpochDay(),
                record.get("dateValue"));
        Assertions.assertEquals(
                ((LocalTime) ProtoSchemaUtil.getValue(message, "timeValue", printer)).toSecondOfDay() * 1000_000L,
                record.get("timeValue"));
        Assertions.assertEquals(
                ((Instant) ProtoSchemaUtil.getValue(message, "datetimeValue", printer)).getMillis() * 1000,
                record.get("datetimeValue"));
        Assertions.assertEquals(
                ((Instant) ProtoSchemaUtil.getValue(message, "timestampValue", printer)).getMillis() * 1000,
                record.get("timestampValue"));

        Assertions.assertEquals(
                ProtoSchemaUtil.getValue(message, "wrappedBoolValue", printer),
                record.get("wrappedBoolValue"));
        Assertions.assertEquals(
                ProtoSchemaUtil.getValue(message, "wrappedStringValue", printer),
                record.get("wrappedStringValue"));
        Assertions.assertEquals(
                new String((byte[]) ProtoSchemaUtil.getValue(message, "wrappedBytesValue", printer), StandardCharsets.UTF_8),
                new String(((ByteBuffer)record.get("wrappedBytesValue")).array(), StandardCharsets.UTF_8));
        Assertions.assertEquals(
                ProtoSchemaUtil.getValue(message, "wrappedInt32Value", printer),
                record.get("wrappedInt32Value"));
        Assertions.assertEquals(
                ProtoSchemaUtil.getValue(message, "wrappedInt64Value", printer),
                record.get("wrappedInt64Value"));
        Assertions.assertEquals(
                ProtoSchemaUtil.getValue(message, "wrappedFloatValue", printer),
                record.get("wrappedFloatValue"));
        Assertions.assertEquals(
                ProtoSchemaUtil.getValue(message, "wrappedDoubleValue", printer),
                record.get("wrappedDoubleValue"));
        Assertions.assertEquals(
                ProtoSchemaUtil.getValue(message, "wrappedUInt32Value", printer),
                record.get("wrappedUInt32Value"));
        Assertions.assertEquals(
                ProtoSchemaUtil.getValue(message, "wrappedUInt64Value", printer),
                record.get("wrappedUInt64Value"));

        // Any
        Assertions.assertTrue(ProtoSchemaUtil.getValue(message, "anyValue", printer).equals(record.get("anyValue")));

        // Enum
        GenericData.EnumSymbol enumSymbol = (GenericData.EnumSymbol)record.get("enumValue");
        if(ProtoSchemaUtil.hasField(message, "enumValue")) {
            Assertions.assertEquals(
                    ((Descriptors.EnumValueDescriptor) ProtoSchemaUtil.getFieldValue(message, "enumValue")).getIndex(),
                    enumSymbol.getSchema().getEnumOrdinal(record.get("enumValue").toString()));
        } else {
            Assertions.assertEquals(0, enumSymbol.getSchema().getEnumOrdinal(record.get("enumValue").toString()));
        }

        // OneOf
        Assertions.assertEquals(ProtoSchemaUtil.getValue(message, "entityName", printer), record.get("entityName"));
        Assertions.assertEquals(ProtoSchemaUtil.getValue(message, "entityAge", printer), record.get("entityAge"));

        // Map
        List<DynamicMessage> mapMessages = (List<DynamicMessage>) ProtoSchemaUtil.getValue(message, "strIntMapValue", printer);
        List<GenericRecord> mapRecords = (List<GenericRecord>)record.get("strIntMapValue");
        Assertions.assertEquals(mapMessages.size(), mapRecords.size());
        for(int i=0; i<mapMessages.size(); i++) {
            Assertions.assertEquals(
                    ProtoSchemaUtil.getValue(mapMessages.get(i), "key", printer),
                    mapRecords.get(i).get("key"));
            Assertions.assertEquals(
                    ProtoSchemaUtil.getValue(mapMessages.get(i), "value", printer),
                    mapRecords.get(i).get("value"));
        }

        mapRecords = (List<GenericRecord>)record.get("longDoubleMapValue");
        mapMessages = (List<DynamicMessage>) ProtoSchemaUtil.getValue(message, "longDoubleMapValue", printer);
        Assertions.assertEquals(mapMessages.size(), mapRecords.size());
        for (int i = 0; i < mapMessages.size(); i++) {
            Assertions.assertEquals(
                    ProtoSchemaUtil.getValue(mapMessages.get(i), "key", printer),
                    mapRecords.get(i).get("key"));
            Assertions.assertEquals(
                    ProtoSchemaUtil.getValue(mapMessages.get(i), "value", printer),
                    mapRecords.get(i).get("value"));
        }

        // Repeated
        Assertions.assertEquals(ProtoSchemaUtil.getValue(message, "boolValues", printer), record.get("boolValues"));
        Assertions.assertEquals(ProtoSchemaUtil.getValue(message, "stringValues", printer), record.get("stringValues"));
        Assertions.assertEquals(ProtoSchemaUtil.getValue(message, "intValues", printer), record.get("intValues"));
        Assertions.assertEquals(ProtoSchemaUtil.getValue(message, "longValues", printer), record.get("longValues"));
        Assertions.assertEquals(ProtoSchemaUtil.getValue(message, "floatValues", printer), record.get("floatValues"));
        Assertions.assertEquals(ProtoSchemaUtil.getValue(message, "doubleValues", printer), record.get("doubleValues"));
        Assertions.assertEquals(ProtoSchemaUtil.getValue(message, "uintValues", printer), record.get("uintValues"));
        Assertions.assertEquals(ProtoSchemaUtil.getValue(message, "ulongValues", printer), record.get("ulongValues"));

        List<DynamicMessage> list = (List<DynamicMessage>) ProtoSchemaUtil.getFieldValue(message, "dateValues");
        int i = 0;
        if(ProtoSchemaUtil.hasField(message, "dateValues")) {
            Assertions.assertEquals(list.size(), ((List)record.get("dateValues")).size());
            for (int epochDay : (List<Integer>)record.get("dateValues")) {
                Assertions.assertEquals(
                        (int) ProtoSchemaUtil.getEpochDay(
                                (com.google.type.Date) (ProtoSchemaUtil.convertBuildInValue("google.type.Date", list.get(i)))),
                        epochDay);
                i++;
            }
        } else {
            Assertions.assertEquals(new ArrayList<Date>(), record.get("dateValues"));
        }

        if(ProtoSchemaUtil.hasField(message, "timeValues")) {
            list = (List<DynamicMessage>) ProtoSchemaUtil.getFieldValue(message, "timeValues");
            Assertions.assertEquals(list.size(), ((List)record.get("timeValues")).size());
            i = 0;
            for (long microSecondOfDay : (List<Long>)record.get("timeValues")) {
                Assertions.assertEquals(
                        1000_000 * ProtoSchemaUtil.getSecondOfDay(
                                (com.google.type.TimeOfDay) (ProtoSchemaUtil.convertBuildInValue("google.type.TimeOfDay", list.get(i)))),
                        microSecondOfDay);
                i++;
            }
        } else {
            Assertions.assertEquals(new ArrayList<LocalTime>(), record.get("timeValues"));
        }

        if(ProtoSchemaUtil.hasField(message, "datetimeValues")) {
            list = (List<DynamicMessage>) ProtoSchemaUtil.getFieldValue(message, "datetimeValues");
            Assertions.assertEquals(list.size(), ((List)record.get("datetimeValues")).size());
            i = 0;
            for (long epochMicros : (List<Long>)record.get("datetimeValues")) {
                Assertions.assertEquals(
                        1000 * ProtoSchemaUtil.getEpochMillis(
                                (com.google.type.DateTime) (ProtoSchemaUtil.convertBuildInValue("google.type.DateTime", list.get(i)))),
                        epochMicros);
                i++;
            }
        } else {
            Assertions.assertEquals(new ArrayList<Instant>(), record.get("datetimeValues"));
        }

        if(ProtoSchemaUtil.hasField(message, "timestampValues")) {
            list = (List<DynamicMessage>) ProtoSchemaUtil.getFieldValue(message, "timestampValues");
            Assertions.assertEquals(list.size(), ((List)record.get("timestampValues")).size());
            i = 0;
            for (long epochMicros : (List<Long>)record.get("timestampValues")) {
                Assertions.assertEquals(
                        Timestamps.toMicros(
                                (com.google.protobuf.Timestamp) (ProtoSchemaUtil.convertBuildInValue("google.protobuf.Timestamp", list.get(i)))),
                        epochMicros);
                i++;
            }
        } else {
            Assertions.assertEquals(new ArrayList<Instant>(), record.get("timestampValues"));
        }

        if(ProtoSchemaUtil.getFieldValue(message, "anyValues") != null && record.get("anyValues") != null) {
            list = (List<DynamicMessage>) ProtoSchemaUtil.getFieldValue(message, "anyValues");
            Assertions.assertEquals(list.size(), ((List)record.get("anyValues")).size());
            i = 0;
            for (var json : (List<String>)record.get("anyValues")) {
                Assertions.assertEquals(
                        printer.print(list.get(i)),
                        (json));
                i++;
            }
        }

        if(ProtoSchemaUtil.hasField(message, "enumValues")) {
            List<Descriptors.EnumValueDescriptor> enums = (List<Descriptors.EnumValueDescriptor>) ProtoSchemaUtil.getFieldValue(message, "enumValues");
            Assertions.assertEquals(enums.size(), ((List)record.get("enumValues")).size());
            i = 0;
            for(var enumValue : (List<GenericData.EnumSymbol>)record.get("enumValues")) {
                Assertions.assertEquals(
                        enums.get(i).getName(),
                        enumValue.toString());
                i++;
            }
        } else {
            Assertions.assertEquals(new ArrayList<EnumerationType.Value>(), record.get("enumValues"));
        }
    }

    private Schema getFieldSchema(Schema schema, final String field) {
        final Schema fieldSchema = schema.getFields().stream()
                .filter(f -> f.name().equals(field))
                .map(Schema.Field::schema)
                .findAny()
                .orElse(null);
        if(fieldSchema == null) {
            return null;
        }
        return AvroSchemaUtil.unnestUnion(fieldSchema);
    }


}
