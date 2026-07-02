package com.mercari.solution.util.schema.converter;

import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.Timestamps;
import com.mercari.solution.util.schema.ProtoSchemaUtil;
import com.mercari.solution.util.ResourceUtil;
import com.mercari.solution.util.schema.StructSchemaUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.LocalTime;
import java.util.*;
import java.util.stream.Collectors;

public class ProtoToStructConverterTest {

    private static final double DELTA = 1e-15;

    @Test
    public void testToSchema() {
        final byte[] descBytes = ResourceUtil.getResourceFileAsBytes("schema/test.desc");
        final Map<String, Descriptors.Descriptor> descriptors = ProtoSchemaUtil.getDescriptors(descBytes);
        final Descriptors.Descriptor descriptor = descriptors.get("com.mercari.solution.entity.TestMessage");
        final Type type = ProtoToStructConverter.convertSchema(descriptor);

        assertSchemaFields(type);

        final Type childType = getField(type, "child").getType();
        assertSchemaFields(childType);

        final Type grandchildType = getField(childType, "grandchild").getType();
        assertSchemaFields(grandchildType);
    }

    @Test
    public void testToStruct() throws Exception {
        testToStruct("data/test.pb");
    }

    @Test
    public void testToStructNull() throws Exception {
        testToStruct("data/test_null.pb");
    }

    private void testToStruct(final String protoPath) throws Exception {

        final byte[] descBytes = ResourceUtil.getResourceFileAsBytes("schema/test.desc");
        final byte[] protoBytes = ResourceUtil.getResourceFileAsBytes(protoPath);

        final Map<String, Descriptors.Descriptor> descriptors = ProtoSchemaUtil.getDescriptors(descBytes);
        final Descriptors.Descriptor descriptor = descriptors.get("com.mercari.solution.entity.TestMessage");
        final Type type = ProtoToStructConverter.convertSchema(descriptor);
        final DynamicMessage message = ProtoSchemaUtil.convert(descriptor, protoBytes);

        final JsonFormat.Printer printer = ProtoSchemaUtil.createJsonPrinter(descriptors);

        final Struct struct = ProtoToStructConverter.convert(type, descriptor, protoBytes, printer);
        assertStructValues(message, struct, printer);

        if(ProtoSchemaUtil.hasField(message, "child")) {
            final Struct child = (Struct) StructSchemaUtil.getValue(struct, "child");
            final DynamicMessage childMessage = (DynamicMessage) ProtoSchemaUtil.getFieldValue(message, "child");
            assertStructValues(childMessage, child, printer);

            final List<Struct> grandchildren = child.getStructList("grandchildren");
            final List<DynamicMessage> grandchildrenMessages = (List<DynamicMessage>) ProtoSchemaUtil.getFieldValue(childMessage, "grandchildren");
            int i = 0;
            for(final Struct s : grandchildren) {
                assertStructValues(grandchildrenMessages.get(i), s, printer);
                i++;
            }

            if(ProtoSchemaUtil.hasField(childMessage, "grandchild")) {
                final Struct grandchild = (Struct)StructSchemaUtil.getValue(child, "grandchild");
                final DynamicMessage grandchildMessage = (DynamicMessage) ProtoSchemaUtil.getFieldValue(childMessage, "grandchild");
                assertStructValues(grandchildMessage, grandchild, printer);
            }
        }

        final List<Struct> children = struct.getStructList("children");
        final List<DynamicMessage> childrenMessages = (List<DynamicMessage>) ProtoSchemaUtil.getFieldValue(message, "children");
        int i = 0;
        for(final Struct s : children) {
            assertStructValues(childrenMessages.get(i), s, printer);
            i++;
        }

    }

    private void assertSchemaFields(final Type type) {

        // Build-in types
        Assertions.assertEquals(Type.Code.BOOL, getField(type, "boolValue").getType().getCode());
        Assertions.assertEquals(Type.Code.STRING, getField(type, "stringValue").getType().getCode());
        Assertions.assertEquals(Type.Code.BYTES, getField(type, "bytesValue").getType().getCode());
        Assertions.assertEquals(Type.Code.INT64, getField(type, "intValue").getType().getCode());
        Assertions.assertEquals(Type.Code.INT64, getField(type, "longValue").getType().getCode());
        Assertions.assertEquals(Type.Code.FLOAT32, getField(type, "floatValue").getType().getCode());
        Assertions.assertEquals(Type.Code.FLOAT64, getField(type, "doubleValue").getType().getCode());
        Assertions.assertEquals(Type.Code.INT64, getField(type, "uintValue").getType().getCode());
        Assertions.assertEquals(Type.Code.INT64, getField(type, "ulongValue").getType().getCode());

        // Google provided types
        Assertions.assertEquals(Type.Code.DATE, getField(type, "dateValue").getType().getCode());
        Assertions.assertEquals(Type.Code.STRING, getField(type, "timeValue").getType().getCode());
        Assertions.assertEquals(Type.Code.TIMESTAMP, getField(type, "datetimeValue").getType().getCode());
        Assertions.assertEquals(Type.Code.TIMESTAMP, getField(type, "timestampValue").getType().getCode());

        // Google provided types wrappedValues
        Assertions.assertEquals(Type.Code.BOOL, getField(type, "wrappedBoolValue").getType().getCode());
        Assertions.assertEquals(Type.Code.STRING, getField(type, "wrappedStringValue").getType().getCode());
        Assertions.assertEquals(Type.Code.BYTES, getField(type, "wrappedBytesValue").getType().getCode());
        Assertions.assertEquals(Type.Code.INT64, getField(type, "wrappedInt32Value").getType().getCode());
        Assertions.assertEquals(Type.Code.INT64, getField(type, "wrappedInt64Value").getType().getCode());
        Assertions.assertEquals(Type.Code.FLOAT32, getField(type, "wrappedFloatValue").getType().getCode());
        Assertions.assertEquals(Type.Code.FLOAT64, getField(type, "wrappedDoubleValue").getType().getCode());
        Assertions.assertEquals(Type.Code.INT64, getField(type, "wrappedUInt32Value").getType().getCode());
        Assertions.assertEquals(Type.Code.INT64, getField(type, "wrappedUInt64Value").getType().getCode());

        // Any
        Assertions.assertEquals(Type.Code.STRING, getField(type, "anyValue").getType().getCode());

        // Enum
        Assertions.assertEquals(Type.Code.STRING, getField(type, "enumValue").getType().getCode());

        // OneOf
        Assertions.assertEquals(Type.Code.STRING, getField(type, "entityName").getType().getCode());
        Assertions.assertEquals(Type.Code.INT64, getField(type, "entityAge").getType().getCode());

        // Map
        Assertions.assertEquals(Type.Code.ARRAY, getField(type, "strIntMapValue").getType().getCode());
        Assertions.assertEquals(Type.Code.STRUCT, getField(type, "strIntMapValue").getType().getArrayElementType().getCode());
        Assertions.assertEquals(Type.Code.STRING, getField(type, "strIntMapValue").getType().getArrayElementType().getStructFields().get(0).getType().getCode());
        Assertions.assertEquals(Type.Code.INT64, getField(type, "strIntMapValue").getType().getArrayElementType().getStructFields().get(1).getType().getCode());

        Assertions.assertEquals(Type.Code.ARRAY, getField(type, "longDoubleMapValue").getType().getCode());
        Assertions.assertEquals(Type.Code.STRUCT, getField(type, "longDoubleMapValue").getType().getArrayElementType().getCode());
        Assertions.assertEquals(Type.Code.INT64, getField(type, "longDoubleMapValue").getType().getArrayElementType().getStructFields().get(0).getType().getCode());
        Assertions.assertEquals(Type.Code.FLOAT64, getField(type, "longDoubleMapValue").getType().getArrayElementType().getStructFields().get(1).getType().getCode());

        //Assertions.assertEquals(Type.Code.ARRAY, getField(type, "intChildMapValue").getType().getCode());
        //Assertions.assertEquals(Type.Code.STRUCT, getField(type, "intChildMapValue").getType().getArrayElementType().getCode());
        //Assertions.assertEquals(Type.Code.INT64, getField(type, "intChildMapValue").getType().getArrayElementType().getStructFields().get(0).getType().getCode());
        //Assertions.assertEquals(Type.Code.STRUCT, getField(type, "intChildMapValue").getType().getArrayElementType().getStructFields().get(1).getType().getCode());

        // Repeated
        Assertions.assertEquals(Type.Code.ARRAY, getField(type, "boolValues").getType().getCode());
        Assertions.assertEquals(Type.Code.ARRAY, getField(type, "stringValues").getType().getCode());
        Assertions.assertEquals(Type.Code.ARRAY, getField(type, "bytesValues").getType().getCode());
        Assertions.assertEquals(Type.Code.ARRAY, getField(type, "intValues").getType().getCode());
        Assertions.assertEquals(Type.Code.ARRAY, getField(type, "longValues").getType().getCode());
        Assertions.assertEquals(Type.Code.ARRAY, getField(type, "floatValues").getType().getCode());
        Assertions.assertEquals(Type.Code.ARRAY, getField(type, "doubleValues").getType().getCode());
        Assertions.assertEquals(Type.Code.ARRAY, getField(type, "uintValues").getType().getCode());
        Assertions.assertEquals(Type.Code.ARRAY, getField(type, "ulongValues").getType().getCode());
        Assertions.assertEquals(Type.Code.ARRAY, getField(type, "dateValues").getType().getCode());
        Assertions.assertEquals(Type.Code.ARRAY, getField(type, "timeValues").getType().getCode());
        Assertions.assertEquals(Type.Code.ARRAY, getField(type, "datetimeValues").getType().getCode());
        Assertions.assertEquals(Type.Code.ARRAY, getField(type, "timestampValues").getType().getCode());
        Assertions.assertEquals(Type.Code.ARRAY, getField(type, "wrappedBoolValues").getType().getCode());
        Assertions.assertEquals(Type.Code.ARRAY, getField(type, "wrappedStringValues").getType().getCode());
        Assertions.assertEquals(Type.Code.ARRAY, getField(type, "wrappedBytesValues").getType().getCode());
        Assertions.assertEquals(Type.Code.ARRAY, getField(type, "wrappedInt32Values").getType().getCode());
        Assertions.assertEquals(Type.Code.ARRAY, getField(type, "wrappedInt64Values").getType().getCode());
        Assertions.assertEquals(Type.Code.ARRAY, getField(type, "wrappedUInt32Values").getType().getCode());
        Assertions.assertEquals(Type.Code.ARRAY, getField(type, "wrappedUInt64Values").getType().getCode());
        Assertions.assertEquals(Type.Code.ARRAY, getField(type, "wrappedFloatValues").getType().getCode());
        Assertions.assertEquals(Type.Code.ARRAY, getField(type, "wrappedDoubleValues").getType().getCode());
        Assertions.assertEquals(Type.Code.ARRAY, getField(type, "anyValues").getType().getCode());
        Assertions.assertEquals(Type.Code.ARRAY, getField(type, "enumValues").getType().getCode());

        Assertions.assertEquals(Type.Code.BOOL, getField(type, "boolValues").getType().getArrayElementType().getCode());
        Assertions.assertEquals(Type.Code.STRING, getField(type, "stringValues").getType().getArrayElementType().getCode());
        Assertions.assertEquals(Type.Code.BYTES, getField(type, "bytesValues").getType().getArrayElementType().getCode());
        Assertions.assertEquals(Type.Code.INT64, getField(type, "intValues").getType().getArrayElementType().getCode());
        Assertions.assertEquals(Type.Code.INT64, getField(type, "longValues").getType().getArrayElementType().getCode());
        Assertions.assertEquals(Type.Code.FLOAT32, getField(type, "floatValues").getType().getArrayElementType().getCode());
        Assertions.assertEquals(Type.Code.FLOAT64, getField(type, "doubleValues").getType().getArrayElementType().getCode());
        Assertions.assertEquals(Type.Code.INT64, getField(type, "uintValues").getType().getArrayElementType().getCode());
        Assertions.assertEquals(Type.Code.INT64, getField(type, "ulongValues").getType().getArrayElementType().getCode());
        Assertions.assertEquals(Type.Code.DATE, getField(type, "dateValues").getType().getArrayElementType().getCode());
        Assertions.assertEquals(Type.Code.STRING, getField(type, "timeValues").getType().getArrayElementType().getCode());
        Assertions.assertEquals(Type.Code.TIMESTAMP, getField(type, "datetimeValues").getType().getArrayElementType().getCode());
        Assertions.assertEquals(Type.Code.BOOL, getField(type, "wrappedBoolValues").getType().getArrayElementType().getCode());
        Assertions.assertEquals(Type.Code.STRING, getField(type, "wrappedStringValues").getType().getArrayElementType().getCode());
        Assertions.assertEquals(Type.Code.BYTES, getField(type, "wrappedBytesValues").getType().getArrayElementType().getCode());
        Assertions.assertEquals(Type.Code.INT64, getField(type, "wrappedInt32Values").getType().getArrayElementType().getCode());
        Assertions.assertEquals(Type.Code.INT64, getField(type, "wrappedInt64Values").getType().getArrayElementType().getCode());
        Assertions.assertEquals(Type.Code.INT64, getField(type, "wrappedUInt32Values").getType().getArrayElementType().getCode());
        Assertions.assertEquals(Type.Code.INT64, getField(type, "wrappedUInt64Values").getType().getArrayElementType().getCode());
        Assertions.assertEquals(Type.Code.FLOAT32, getField(type, "wrappedFloatValues").getType().getArrayElementType().getCode());
        Assertions.assertEquals(Type.Code.FLOAT64, getField(type, "wrappedDoubleValues").getType().getArrayElementType().getCode());
        Assertions.assertEquals(Type.Code.STRING, getField(type, "anyValues").getType().getArrayElementType().getCode());
        Assertions.assertEquals(Type.Code.STRING, getField(type, "enumValues").getType().getArrayElementType().getCode());
    }

    private void assertStructValues(final DynamicMessage message, final Struct struct, final JsonFormat.Printer printer) throws InvalidProtocolBufferException {

        // Build-in type
        Assertions.assertEquals(ProtoSchemaUtil.getValue(message, "boolValue", printer), struct.getBoolean("boolValue"));
        Assertions.assertEquals(ProtoSchemaUtil.getValue(message, "stringValue", printer), struct.getString("stringValue"));
        Assertions.assertEquals(
                new String((byte[]) ProtoSchemaUtil.getValue(message, "bytesValue", printer), StandardCharsets.UTF_8),
                new String(struct.getBytes("bytesValue").toByteArray(), StandardCharsets.UTF_8));
        Assertions.assertEquals(((Integer) ProtoSchemaUtil.getValue(message, "intValue", printer)).intValue(), struct.getLong("intValue"));
        Assertions.assertEquals(ProtoSchemaUtil.getValue(message, "longValue", printer), struct.getLong("longValue"));
        Assertions.assertEquals(((Float) ProtoSchemaUtil.getValue(message, "floatValue", printer)).doubleValue(), struct.getFloat("floatValue"), DELTA);
        Assertions.assertEquals(ProtoSchemaUtil.getValue(message, "doubleValue", printer), struct.getDouble("doubleValue"));
        Assertions.assertEquals(((Integer) ProtoSchemaUtil.getValue(message, "uintValue", printer)).longValue(), struct.getLong("uintValue"));
        Assertions.assertEquals(ProtoSchemaUtil.getValue(message, "ulongValue", printer), struct.getLong("ulongValue"));

        // Google-provided type
        if(ProtoSchemaUtil.hasField(message, "dateValue")) {
            Assertions.assertEquals(
                    ProtoSchemaUtil.getEpochDay(
                            (com.google.type.Date)(ProtoSchemaUtil.convertBuildInValue("google.type.Date",
                                    (DynamicMessage) ProtoSchemaUtil.getFieldValue(message, "dateValue")))),
                    StructSchemaUtil.getEpochDay(struct.getDate("dateValue")));
        } else {
            Assertions.assertEquals(Date.fromYearMonthDay(1,1,1), struct.getDate("dateValue"));
        }

        if(ProtoSchemaUtil.hasField(message, "timeValue")) {
            var timeOfDay = ((com.google.type.TimeOfDay) (ProtoSchemaUtil.convertBuildInValue("google.type.TimeOfDay",
                    (DynamicMessage) ProtoSchemaUtil.getFieldValue(message, "timeValue"))));
            Assertions.assertEquals(
                    String.format("%02d:%02d:%02d", timeOfDay.getHours(), timeOfDay.getMinutes(), timeOfDay.getSeconds()),
                    (struct.getString("timeValue")));
        } else {
            Assertions.assertEquals("00:00:00", struct.getString("timeValue"));
        }

        if(ProtoSchemaUtil.hasField(message, "datetimeValue")) {
            Assertions.assertEquals(
                    ProtoSchemaUtil.getEpochMillis((com.google.type.DateTime) (ProtoSchemaUtil.convertBuildInValue("google.type.DateTime",
                            (DynamicMessage) ProtoSchemaUtil.getFieldValue(message, "datetimeValue")))),
                    (Timestamps.toMillis(struct.getTimestamp("datetimeValue").toProto())));
        } else {
            Assertions.assertEquals(Timestamp.parseTimestamp("0001-01-01T00:00:00Z"), struct.getTimestamp("datetimeValue"));
        }

        if(ProtoSchemaUtil.hasField(message, "timestampValue")) {
            Assertions.assertEquals(
                    Timestamps.toMillis((com.google.protobuf.Timestamp) (ProtoSchemaUtil.convertBuildInValue("google.protobuf.Timestamp",
                            (DynamicMessage) ProtoSchemaUtil.getFieldValue(message, "timestampValue")))),
                    (Timestamps.toMillis(struct.getTimestamp("timestampValue").toProto())));
        } else {
            Assertions.assertEquals(Timestamp.parseTimestamp("0001-01-01T00:00:00Z"), struct.getTimestamp("timestampValue"));
        }

        Assertions.assertEquals(
                ProtoSchemaUtil.getValue(message, "wrappedBoolValue", printer),
                struct.getBoolean("wrappedBoolValue"));
        Assertions.assertEquals(
                ProtoSchemaUtil.getValue(message, "wrappedStringValue", printer),
                struct.getString("wrappedStringValue"));
        Assertions.assertEquals(
                new String((byte[]) ProtoSchemaUtil.getValue(message, "wrappedBytesValue", printer), StandardCharsets.UTF_8),
                new String(struct.getBytes("wrappedBytesValue").toByteArray(), StandardCharsets.UTF_8));
        Assertions.assertEquals(
                ((Integer) ProtoSchemaUtil.getValue(message, "wrappedInt32Value", printer)).longValue(),
                struct.getLong("wrappedInt32Value"));
        Assertions.assertEquals(
                ProtoSchemaUtil.getValue(message, "wrappedInt64Value", printer),
                struct.getLong("wrappedInt64Value"));
        Assertions.assertEquals(
                ((Float) ProtoSchemaUtil.getValue(message, "wrappedFloatValue", printer)),
                struct.getFloat("wrappedFloatValue"), DELTA);
        Assertions.assertEquals(
                ProtoSchemaUtil.getValue(message, "wrappedDoubleValue", printer),
                struct.getDouble("wrappedDoubleValue"));
        Assertions.assertEquals(
                ((Integer) ProtoSchemaUtil.getValue(message, "wrappedUInt32Value", printer)).longValue(),
                struct.getLong("wrappedUInt32Value"));
        Assertions.assertEquals(
                ProtoSchemaUtil.getValue(message, "wrappedUInt64Value", printer),
                struct.getLong("wrappedUInt64Value"));

        // Any
        if(ProtoSchemaUtil.hasField(message, "anyValue")) {
            Assertions.assertEquals(printer.print((DynamicMessage) ProtoSchemaUtil.getFieldValue(message, "anyValue")), struct.getString("anyValue"));
        } else {
            Assertions.assertEquals("", struct.getString("anyValue"));
        }

        // Enum
        if(ProtoSchemaUtil.hasField(message, "enumValue")) {
            Assertions.assertEquals(((Descriptors.EnumValueDescriptor) ProtoSchemaUtil.getFieldValue(message, "enumValue")).getName(), struct.getString("enumValue"));
        } else {
            Assertions.assertEquals(ProtoSchemaUtil.getField(message, "enumValue").getEnumType().getValues().get(0).getName(), struct.getString("enumValue"));
        }

        // OneOf
        final Object entityName = ProtoSchemaUtil.getFieldValue(message, "entityName");
        final Object entityAge  = ProtoSchemaUtil.getFieldValue(message, "entityAge");
        Assertions.assertEquals(entityName == null ? "" : entityName, struct.getString("entityName"));
        Assertions.assertEquals(entityAge == null ? 0L : ((Integer) ProtoSchemaUtil.getFieldValue(message, "entityAge")).longValue(), struct.getLong("entityAge"));

        // Map
        List<DynamicMessage> mapMessages = (List<DynamicMessage>) ProtoSchemaUtil.getFieldValue(message, "strIntMapValue");
        List<Struct> mapStructs = struct.getStructList("strIntMapValue");
        if(ProtoSchemaUtil.hasField(message, "strIntMapValue")) {
            Assertions.assertEquals(mapMessages.size(), mapStructs.size());
            for(int i=0; i<mapMessages.size(); i++) {
                Assertions.assertEquals(
                        ProtoSchemaUtil.getValue(mapMessages.get(i), "key", printer),
                        StructSchemaUtil.getValue(mapStructs.get(i), "key"));
                Assertions.assertEquals(
                        ((Integer) ProtoSchemaUtil.getValue(mapMessages.get(i), "value", printer)).longValue(),
                        StructSchemaUtil.getValue(mapStructs.get(i), "value"));
            }
        } else {
            Assertions.assertEquals(0, mapStructs.size());
        }

        mapStructs = struct.getStructList("longDoubleMapValue");
        if(ProtoSchemaUtil.hasField(message, "longDoubleMapValue")) {
            mapMessages = (List<DynamicMessage>) ProtoSchemaUtil.getFieldValue(message, "longDoubleMapValue");
            Assertions.assertEquals(mapMessages.size(), mapStructs.size());
            for (int i = 0; i < mapMessages.size(); i++) {
                Assertions.assertEquals(
                        ProtoSchemaUtil.getValue(mapMessages.get(i), "key", printer),
                        StructSchemaUtil.getValue(mapStructs.get(i), "key"));
                Assertions.assertEquals(
                        ProtoSchemaUtil.getValue(mapMessages.get(i), "value", printer),
                        StructSchemaUtil.getValue(mapStructs.get(i), "value"));
            }
        } else {
            Assertions.assertEquals(0, mapStructs.size());
        }

        // Repeated
        Assertions.assertEquals(Optional.ofNullable(ProtoSchemaUtil.getFieldValue(message, "boolValues")).orElse(new ArrayList<>()), struct.getBooleanList("boolValues"));
        Assertions.assertEquals(Optional.ofNullable(ProtoSchemaUtil.getFieldValue(message, "stringValues")).orElse(new ArrayList<>()), struct.getStringList("stringValues"));
        Assertions.assertEquals(Optional.ofNullable(ProtoSchemaUtil.getFieldValue(message, "intValues")).orElse(new ArrayList<>()), struct.getLongList("intValues").stream()
                .map(Long::intValue).collect(Collectors.toList()));
        Assertions.assertEquals(Optional.ofNullable(ProtoSchemaUtil.getFieldValue(message, "longValues")).orElse(new ArrayList<>()), struct.getLongList("longValues"));
        Assertions.assertEquals(Optional.ofNullable(ProtoSchemaUtil.getFieldValue(message, "floatValues")).orElse(new ArrayList<>()), new ArrayList<>(struct.getFloatList("floatValues")));
        Assertions.assertEquals(Optional.ofNullable(ProtoSchemaUtil.getFieldValue(message, "doubleValues")).orElse(new ArrayList<>()), struct.getDoubleList("doubleValues"));
        Assertions.assertEquals(Optional.ofNullable(ProtoSchemaUtil.getFieldValue(message, "uintValues")).orElse(new ArrayList<>()), struct.getLongList("uintValues").stream()
                .map(Long::intValue).collect(Collectors.toList()));
        Assertions.assertEquals(Optional.ofNullable(ProtoSchemaUtil.getFieldValue(message, "ulongValues")).orElse(new ArrayList<>()), struct.getLongList("ulongValues"));

        List<DynamicMessage> list = (List<DynamicMessage>) ProtoSchemaUtil.getFieldValue(message, "dateValues");
        int i = 0;
        if(ProtoSchemaUtil.hasField(message, "dateValues")) {
            Assertions.assertEquals(list.size(), struct.getDateList("dateValues").size());

            for (var date : struct.getDateList("dateValues")) {
                Assertions.assertEquals(
                        ProtoSchemaUtil.getEpochDay(
                                (com.google.type.Date) (ProtoSchemaUtil.convertBuildInValue("google.type.Date", list.get(i)))),
                        StructSchemaUtil.getEpochDay(date));
                i++;
            }
        } else {
            Assertions.assertEquals(0, struct.getDateList("dateValues").size());
        }

        if(ProtoSchemaUtil.hasField(message, "timeValues")) {
            list = (List<DynamicMessage>) ProtoSchemaUtil.getFieldValue(message, "timeValues");
            Assertions.assertEquals(list.size(), struct.getStringList("timeValues").size());
            i = 0;
            for (var localTime : struct.getStringList("timeValues")) {
                Assertions.assertEquals(
                        ProtoSchemaUtil.getSecondOfDay(
                                (com.google.type.TimeOfDay) (ProtoSchemaUtil.convertBuildInValue("google.type.TimeOfDay", list.get(i)))),
                        LocalTime.parse(localTime).toSecondOfDay());
                i++;
            }
        } else {
            Assertions.assertEquals(0, struct.getStringList("timeValues").size());
        }

        if(ProtoSchemaUtil.hasField(message, "datetimeValues")) {
            list = (List<DynamicMessage>) ProtoSchemaUtil.getFieldValue(message, "datetimeValues");
            Assertions.assertEquals(list.size(), struct.getTimestampList("datetimeValues").size());
            i = 0;
            for (var instant : struct.getTimestampList("datetimeValues")) {
                Assertions.assertEquals(
                        ProtoSchemaUtil.getEpochMillis(
                                (com.google.type.DateTime) (ProtoSchemaUtil.convertBuildInValue("google.type.DateTime", list.get(i)))),
                        Timestamps.toMillis(instant.toProto()));
                i++;
            }
        } else {
            Assertions.assertEquals(0, struct.getTimestampList("datetimeValues").size());
        }

        if(ProtoSchemaUtil.hasField(message, "timestampValues")) {
            list = (List<DynamicMessage>) ProtoSchemaUtil.getFieldValue(message, "timestampValues");
            Assertions.assertEquals(list.size(), struct.getTimestampList("timestampValues").size());
            i = 0;
            for (var instant : struct.getTimestampList("timestampValues")) {
                Assertions.assertEquals(
                        Timestamps.toMillis(
                                (com.google.protobuf.Timestamp) (ProtoSchemaUtil.convertBuildInValue("google.protobuf.Timestamp", list.get(i)))),
                        Timestamps.toMillis(instant.toProto()));
                i++;
            }
        } else {
            Assertions.assertEquals(0, struct.getTimestampList("timestampValues").size());
        }

        if(ProtoSchemaUtil.hasField(message, "anyValues")) {
            list = (List<DynamicMessage>) ProtoSchemaUtil.getFieldValue(message, "anyValues");
            Assertions.assertEquals(list.size(), struct.getStringList("anyValues").size());
            i = 0;
            for (var json : struct.getStringList("anyValues")) {
                Assertions.assertEquals(
                        printer.print(list.get(i)),
                        (json));
                i++;
            }
        } else {
            Assertions.assertEquals(0, struct.getStringList("anyValues").size());
        }

        if(ProtoSchemaUtil.hasField(message, "enumValues")) {
            List<Descriptors.EnumValueDescriptor> enums = (List<Descriptors.EnumValueDescriptor>) ProtoSchemaUtil.getFieldValue(message, "enumValues");
            Assertions.assertEquals(enums.size(), struct.getStringList("enumValues").size());
            i = 0;
            for(var e : struct.getStringList("enumValues")) {
                Assertions.assertEquals(
                        enums.get(i).getName(),
                        e);
                i++;
            }
        } else {
            Assertions.assertEquals(0, struct.getStringList("enumValues").size());
        }

    }

    private Type.StructField getField(Type type, final String field) {
        return type.getStructFields().stream()
                .filter(f -> f.getName().equals(field))
                .findAny()
                .orElse(null);
    }

}
