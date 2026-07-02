package com.mercari.solution.util.schema.converter;

import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.Timestamps;
import com.mercari.solution.util.ResourceUtil;
import com.mercari.solution.util.schema.ProtoSchemaUtil;
import com.mercari.solution.util.schema.StructSchemaUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;
import java.util.stream.Collectors;

public class StructToProtoConverterTest {

    private static final double DELTA = 1e-15;

    @Test
    public void testConvert() {
        final byte[] descBytes = ResourceUtil.getResourceFileAsBytes("schema/test.desc");
        final byte[] protoBytes = ResourceUtil.getResourceFileAsBytes("data/test.pb");

        final Map<String, Descriptors.Descriptor> descriptors = ProtoSchemaUtil.getDescriptors(descBytes);
        final Descriptors.Descriptor descriptor = descriptors.get("com.mercari.solution.entity.TestMessage");
        final Type type = ProtoToStructConverter.convertSchema(descriptor);

        final JsonFormat.TypeRegistry.Builder builder = JsonFormat.TypeRegistry.newBuilder();
        descriptors.forEach((k, v) -> builder.add(v));
        final JsonFormat.Printer printer = JsonFormat.printer().usingTypeRegistry(builder.build());

        final Struct struct = ProtoToStructConverter.convert(type, descriptor, protoBytes, printer);

        final DynamicMessage message1 = StructToProtoConverter.convert(descriptor, struct);
        final DynamicMessage message2 = ProtoSchemaUtil.convert(descriptor, message1.toByteArray());

        assertStructValues(message1, struct, printer);
        assertStructValues(message2, struct, printer);

        testNest(message1, struct, printer);
        testNest(message2, struct, printer);
    }

    @Test
    public void testConvertNull() {
        final byte[] descBytes = ResourceUtil.getResourceFileAsBytes("schema/test.desc");
        final byte[] protoBytes = ResourceUtil.getResourceFileAsBytes("data/test_null.pb");

        final Map<String, Descriptors.Descriptor> descriptors = ProtoSchemaUtil.getDescriptors(descBytes);
        final Descriptors.Descriptor descriptor = descriptors.get("com.mercari.solution.entity.TestMessage");
        final Type type = ProtoToStructConverter.convertSchema(descriptor);

        final JsonFormat.TypeRegistry.Builder builder = JsonFormat.TypeRegistry.newBuilder();
        descriptors.forEach((k, v) -> builder.add(v));
        final JsonFormat.Printer printer = JsonFormat.printer().usingTypeRegistry(builder.build());

        final Struct struct = ProtoToStructConverter.convert(type, descriptor, protoBytes, printer);

        final DynamicMessage message1 = StructToProtoConverter.convert(descriptor, struct);
        final DynamicMessage message2 = ProtoSchemaUtil.convert(descriptor, message1.toByteArray());

        assertRowNullValues(message1, printer);
        assertRowNullValues(message2, printer);
    }

    private void testNest(final DynamicMessage message, final Struct struct, final JsonFormat.Printer printer) {
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

    private void assertStructValues(final DynamicMessage message, final Struct struct, final JsonFormat.Printer printer) {

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

        // Any is not supported

        // Enum
        if(ProtoSchemaUtil.hasField(message, "enumValue")) {
            Assertions.assertEquals(((Descriptors.EnumValueDescriptor) ProtoSchemaUtil.getFieldValue(message, "enumValue")).getName(), struct.getString("enumValue"));
        } else {
            Assertions.assertEquals(ProtoSchemaUtil.getField(message, "enumValue").getEnumType().getValues().get(0).getName(), struct.getString("enumValue"));
        }

        // OneOf
        if(ProtoSchemaUtil.hasField(message, "entityName")) {
            final Object entityName = ProtoSchemaUtil.getFieldValue(message, "entityName");
            Assertions.assertEquals(entityName == null ? "" : entityName, struct.getString("entityName"));
        } else if(ProtoSchemaUtil.hasField(message, "entityAge")) {
            final Object entityAge  = ProtoSchemaUtil.getFieldValue(message, "entityAge");
            Assertions.assertEquals(entityAge == null ? 0L : ((Integer) ProtoSchemaUtil.getFieldValue(message, "entityAge")).longValue(), struct.getLong("entityAge"));
        }

        // Map is not supported

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

    private void assertRowNullValues(final DynamicMessage message, final JsonFormat.Printer printer) {
        // Build-in type
        Assertions.assertEquals(false, ProtoSchemaUtil.getValue(message, "boolValue", printer));
        Assertions.assertEquals("", ProtoSchemaUtil.getValue(message, "stringValue", printer));
        Assertions.assertEquals("", new String((byte[]) ProtoSchemaUtil.getValue(message, "bytesValue", printer), StandardCharsets.UTF_8));
        Assertions.assertEquals(0, ProtoSchemaUtil.getValue(message, "intValue", printer));
        Assertions.assertEquals(0L, ProtoSchemaUtil.getValue(message, "longValue", printer));
        Assertions.assertEquals(0F, ProtoSchemaUtil.getValue(message, "floatValue", printer));
        Assertions.assertEquals(0D, ProtoSchemaUtil.getValue(message, "doubleValue", printer));
        Assertions.assertEquals(0, ProtoSchemaUtil.getValue(message, "uintValue", printer));
        Assertions.assertEquals(0L, ProtoSchemaUtil.getValue(message, "ulongValue", printer));

        // Google-provided type
        if(ProtoSchemaUtil.hasField(message,"dateValue")) {
            Assertions.assertEquals(
                    LocalDate.of(1,1,1).toEpochDay(),
                    ProtoSchemaUtil.getEpochDay(
                            (com.google.type.Date)(ProtoSchemaUtil.convertBuildInValue("google.type.Date",
                                    (DynamicMessage) ProtoSchemaUtil.getFieldValue(message, "dateValue"))))
            );
        }

        // TODO Somehow it becomes null when it is 00:00:00.
        /*
        if(ProtoSchemaUtil.hasField(message,"timeValue")) {
            Assertions.assertEquals(
                    LocalTime.of(0,0,0).toSecondOfDay(),
                    ProtoSchemaUtil.getSecondOfDay((com.google.type.TimeOfDay)(ProtoSchemaUtil.convertBuildInValue("google.type.TimeOfDay",
                            (DynamicMessage) ProtoSchemaUtil.getFieldValue(message, "timeValue")))));
        }
        */
        if(ProtoSchemaUtil.hasField(message,"datetimeValue")) {
            Assertions.assertEquals(
                    -62135596800000L,
                    ProtoSchemaUtil.getEpochMillis((com.google.type.DateTime)(ProtoSchemaUtil.convertBuildInValue("google.type.DateTime",
                            (DynamicMessage) ProtoSchemaUtil.getFieldValue(message, "datetimeValue")))));
        }

        Assertions.assertEquals(
                false,
                ProtoSchemaUtil.getValue(message, "wrappedBoolValue", printer));
        Assertions.assertEquals(
                "",
                ProtoSchemaUtil.getValue(message, "wrappedStringValue", printer));
        Assertions.assertEquals(
                "",
                new String((byte[]) ProtoSchemaUtil.getValue(message, "wrappedBytesValue", printer), StandardCharsets.UTF_8));
        Assertions.assertEquals(
                0,
                ProtoSchemaUtil.getValue(message, "wrappedInt32Value", printer));
        Assertions.assertEquals(
                0L,
                ProtoSchemaUtil.getValue(message, "wrappedInt64Value", printer));
        Assertions.assertEquals(
                0F,
                ProtoSchemaUtil.getValue(message, "wrappedFloatValue", printer));
        Assertions.assertEquals(
                0D,
                ProtoSchemaUtil.getValue(message, "wrappedDoubleValue", printer));
        Assertions.assertEquals(
                0,
                ProtoSchemaUtil.getValue(message, "wrappedUInt32Value", printer));
        Assertions.assertEquals(
                0L,
                ProtoSchemaUtil.getValue(message, "wrappedUInt64Value", printer));

        // Enum
        if(ProtoSchemaUtil.hasField(message, "enumValue")) {
            Assertions.assertEquals(
                    0,
                    ((Descriptors.EnumValueDescriptor) ProtoSchemaUtil.getFieldValue(message, "enumValue")).getIndex());
        }

        // OneOf
        if(ProtoSchemaUtil.hasField(message, "entityName")) {
            Assertions.assertEquals("", ProtoSchemaUtil.getValue(message, "entityName", printer));
        } else if(ProtoSchemaUtil.hasField(message, "entityAge")) {
            Assertions.assertEquals(0, ProtoSchemaUtil.getValue(message, "entityAge", printer));
        }

        // Repeated
        Assertions.assertEquals(new ArrayList<>(), ProtoSchemaUtil.getValue(message, "boolValues", printer));
        Assertions.assertEquals(new ArrayList<>(), ProtoSchemaUtil.getValue(message, "stringValues", printer));
        Assertions.assertEquals(new ArrayList<>(), ProtoSchemaUtil.getValue(message, "intValues", printer));
        Assertions.assertEquals(new ArrayList<>(), ProtoSchemaUtil.getValue(message, "longValues", printer));
        Assertions.assertEquals(new ArrayList<>(), ProtoSchemaUtil.getValue(message, "floatValues", printer));
        Assertions.assertEquals(new ArrayList<>(), ProtoSchemaUtil.getValue(message, "doubleValues", printer));
        Assertions.assertEquals(new ArrayList<>(), ProtoSchemaUtil.getValue(message, "uintValues", printer));
        Assertions.assertEquals(new ArrayList<>(), ProtoSchemaUtil.getValue(message, "ulongValues", printer));
    }

}
