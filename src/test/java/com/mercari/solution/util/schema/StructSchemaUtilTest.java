package com.mercari.solution.util.schema;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import com.google.protobuf.util.Timestamps;
import com.mercari.solution.TestDatum;
import com.mercari.solution.module.MElement;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.pipeline.mutation.MutationOp;
import com.mercari.solution.util.pipeline.mutation.UnifiedMutation;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ColumnType;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ModType;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.TypeCode;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ValueCaptureType;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.values.KV;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;
import java.util.stream.Collectors;

public class StructSchemaUtilTest {

    private static final double DELTA = 1e-15;

    @Test
    public void testSelectFields() {
        final Struct struct = TestDatum.generateStruct();
        final List<String> fields = Arrays.asList(
                "stringField", "intField", "longField",
                "recordField.stringField", "recordField.doubleField", "recordField.booleanField",
                "recordField.recordField.intField", "recordField.recordField.floatField",
                "recordField.recordArrayField.intField", "recordField.recordArrayField.floatField",
                "recordArrayField.stringField", "recordArrayField.timestampField",
                "recordArrayField.recordField.intField", "recordArrayField.recordField.floatField",
                "recordArrayField.recordArrayField.intField", "recordArrayField.recordArrayField.floatField");

        final Type type = StructSchemaUtil.selectFields(struct.getType(), fields);

        // schema test
        Assertions.assertEquals(5, type.getStructFields().size());
        Assertions.assertTrue(type.getFieldIndex("stringField") >= 0);
        Assertions.assertTrue(type.getFieldIndex("intField") >= 0);
        Assertions.assertTrue(type.getFieldIndex("longField") >= 0);
        Assertions.assertTrue(type.getFieldIndex("recordField") >= 0);
        Assertions.assertTrue(type.getFieldIndex("recordArrayField") >= 0);

        final Type typeChild = type.getStructFields().get(type.getFieldIndex("recordField")).getType();
        Assertions.assertEquals(5, typeChild.getStructFields().size());
        Assertions.assertTrue(typeChild.getFieldIndex("stringField") >= 0);
        Assertions.assertTrue(typeChild.getFieldIndex("doubleField") >= 0);
        Assertions.assertTrue(typeChild.getFieldIndex("booleanField") >= 0);
        Assertions.assertTrue(typeChild.getFieldIndex("recordField") >= 0);
        Assertions.assertTrue(typeChild.getFieldIndex("recordArrayField") >= 0);

        Assertions.assertEquals(Type.Code.ARRAY, typeChild.getStructFields().get(typeChild.getFieldIndex("recordArrayField")).getType().getCode());
        final Type typeChildChildren = typeChild.getStructFields().get(typeChild.getFieldIndex("recordArrayField")).getType().getArrayElementType();
        Assertions.assertEquals(2, typeChildChildren.getStructFields().size());
        Assertions.assertTrue(typeChildChildren.getFieldIndex("intField") >= 0);
        Assertions.assertTrue(typeChildChildren.getFieldIndex("floatField") >= 0);

        final Type typeGrandchild = typeChild.getStructFields().get(typeChild.getFieldIndex("recordField")).getType();
        Assertions.assertEquals(2, typeGrandchild.getStructFields().size());
        Assertions.assertTrue(typeGrandchild.getFieldIndex("intField") >= 0);
        Assertions.assertTrue(typeGrandchild.getFieldIndex("floatField") >= 0);

        Assertions.assertEquals(Type.Code.ARRAY, type.getStructFields().get(type.getFieldIndex("recordArrayField")).getType().getCode());
        final Type typeChildren = type.getStructFields().get(type.getFieldIndex("recordArrayField")).getType().getArrayElementType();
        Assertions.assertEquals(4, typeChildren.getStructFields().size());
        Assertions.assertTrue(typeChildren.getFieldIndex("stringField") >= 0);
        Assertions.assertTrue(typeChildren.getFieldIndex("timestampField") >= 0);
        Assertions.assertTrue(typeChildren.getFieldIndex("recordField") >= 0);
        Assertions.assertTrue(typeChildren.getFieldIndex("recordArrayField") >= 0);

        final Type typeChildrenChild = typeChildren.getStructFields().get(typeChildren.getFieldIndex("recordField")).getType();
        Assertions.assertEquals(2, typeChildrenChild.getStructFields().size());
        Assertions.assertTrue(typeChildrenChild.getFieldIndex("intField") >= 0);
        Assertions.assertTrue(typeChildrenChild.getFieldIndex("floatField") >= 0);

        Assertions.assertEquals(Type.Code.ARRAY, typeChildren.getStructFields().get(typeChildren.getFieldIndex("recordArrayField")).getType().getCode());
        final Type typeChildrenChildren = typeChildren.getStructFields().get(typeChildren.getFieldIndex("recordArrayField")).getType().getArrayElementType();
        Assertions.assertEquals(2, typeChildrenChildren.getStructFields().size());
        Assertions.assertTrue(typeChildrenChildren.getFieldIndex("intField") >= 0);
        Assertions.assertTrue(typeChildrenChildren.getFieldIndex("floatField") >= 0);

        // row test
        final Struct selectedStruct = StructSchemaUtil.toBuilder(type, struct).build();
        Assertions.assertEquals(5, selectedStruct.getColumnCount());
        Assertions.assertEquals(TestDatum.getStringFieldValue(), selectedStruct.getString("stringField"));
        Assertions.assertEquals(TestDatum.getIntFieldValue().intValue(), ((Long)selectedStruct.getLong("intField")).intValue());
        Assertions.assertEquals(TestDatum.getLongFieldValue().longValue(), selectedStruct.getLong("longField"));

        final Struct selectedStructChild = selectedStruct.getStruct("recordField");
        Assertions.assertEquals(5, selectedStructChild.getColumnCount());
        Assertions.assertEquals(TestDatum.getStringFieldValue(), selectedStructChild.getString("stringField"));
        Assertions.assertEquals(TestDatum.getDoubleFieldValue().doubleValue(), selectedStructChild.getDouble("doubleField"), DELTA);
        Assertions.assertEquals(TestDatum.getBooleanFieldValue(), selectedStructChild.getBoolean("booleanField"));

        final Struct selectedStructGrandchild = selectedStructChild.getStruct("recordField");
        Assertions.assertEquals(2, selectedStructGrandchild.getColumnCount());
        Assertions.assertEquals(TestDatum.getIntFieldValue().intValue(), Long.valueOf(selectedStructGrandchild.getLong("intField")).intValue());
        Assertions.assertEquals(TestDatum.getFloatFieldValue().floatValue(), Double.valueOf(selectedStructGrandchild.getFloat("floatField")).floatValue(), DELTA);

        Assertions.assertEquals(2, selectedStruct.getStructList("recordArrayField").size());
        for(final Struct child : selectedStruct.getStructList("recordArrayField")) {
            Assertions.assertEquals(4, child.getColumnCount());
            Assertions.assertEquals(TestDatum.getStringFieldValue(), child.getString("stringField"));
            Assertions.assertEquals(TestDatum.getTimestampFieldValue().getMillis(), Timestamps.toMillis(child.getTimestamp("timestampField").toProto()));

            Assertions.assertEquals(2, child.getStructList("recordArrayField").size());
            for(final Struct grandchild : child.getStructList("recordArrayField")) {
                Assertions.assertEquals(2, grandchild.getColumnCount());
                Assertions.assertEquals(TestDatum.getIntFieldValue().intValue(), Long.valueOf(grandchild.getLong("intField")).intValue());
                Assertions.assertEquals(TestDatum.getFloatFieldValue().floatValue(), Double.valueOf(grandchild.getFloat("floatField")).floatValue(), DELTA);
            }

            final Struct grandchild = child.getStruct("recordField");
            Assertions.assertEquals(TestDatum.getIntFieldValue().intValue(), Long.valueOf(grandchild.getLong("intField")).intValue());
            Assertions.assertEquals(TestDatum.getFloatFieldValue().floatValue(), Double.valueOf(grandchild.getFloat("floatField")).floatValue(), DELTA);
        }

        // null fields row test
        final Struct structNull = TestDatum.generateStructNull();
        final List<String> newFields = new ArrayList<>(fields);
        newFields.add("recordFieldNull");
        newFields.add("recordArrayFieldNull");
        final Type typeNull = StructSchemaUtil.selectFields(structNull.getType(), newFields);

        final Struct selectedStructNull = StructSchemaUtil.toBuilder(typeNull, structNull).build();
        Assertions.assertEquals(7, selectedStructNull.getColumnCount());
        Assertions.assertTrue(selectedStructNull.isNull("stringField"));
        Assertions.assertTrue(selectedStructNull.isNull("intField"));
        Assertions.assertTrue(selectedStructNull.isNull("longField"));
        Assertions.assertTrue(selectedStructNull.isNull("recordFieldNull"));
        Assertions.assertTrue(selectedStructNull.isNull("recordArrayFieldNull"));

        final Struct selectedStructChildNull = selectedStructNull.getStruct("recordField");
        Assertions.assertEquals(5, selectedStructChildNull.getColumnCount());
        Assertions.assertTrue(selectedStructChildNull.isNull("stringField"));
        Assertions.assertTrue(selectedStructChildNull.isNull("doubleField"));
        Assertions.assertTrue(selectedStructChildNull.isNull("booleanField"));

        final Struct selectedStructGrandchildNull = selectedStructChildNull.getStruct("recordField");
        Assertions.assertEquals(2, selectedStructGrandchildNull.getColumnCount());
        Assertions.assertTrue(selectedStructGrandchildNull.isNull("intField"));
        Assertions.assertTrue(selectedStructGrandchildNull.isNull("floatField"));

        Assertions.assertEquals(2, selectedStructNull.getStructList("recordArrayField").size());
        for(final Struct child : selectedStructNull.getStructList("recordArrayField")) {
            Assertions.assertEquals(4, child.getColumnCount());
            Assertions.assertTrue(child.isNull("stringField"));
            Assertions.assertTrue(child.isNull("timestampField"));

            Assertions.assertEquals(2, child.getStructList("recordArrayField").size());
            for(final Struct grandchild : child.getStructList("recordArrayField")) {
                Assertions.assertEquals(2, grandchild.getColumnCount());
                Assertions.assertTrue(grandchild.isNull("intField"));
                Assertions.assertTrue(grandchild.isNull("floatField"));
            }

            final Struct grandchild = child.getStruct("recordField");
            Assertions.assertEquals(2, grandchild.getColumnCount());
            Assertions.assertTrue(grandchild.isNull("intField"));
            Assertions.assertTrue(grandchild.isNull("floatField"));
        }

    }

    @Test
    public void testFlattenType() {
        final Struct struct = createTestStruct();
        Type type = StructSchemaUtil.flatten(struct.getType(), "children", true);

        Set<String> fieldNames1 = type.getStructFields().stream()
                .map(Type.StructField::getName)
                .collect(Collectors.toSet());
        Assertions.assertEquals(
                Set.of("stringField", "children_cstringField", "children_grandchild", "children_grandchildren","children_grandchildrenNull"),
                fieldNames1);
        struct.getType().getStructFields().forEach(f -> {
            if(f.getName().equals("stringField") || f.getName().equals("children_cstringField")) {
                Assertions.assertEquals(
                        Type.Code.STRING,
                        f.getType().getCode());
            } else if(f.getName().equals("children_grandchild")) {
                Assertions.assertEquals(
                        Type.Code.STRUCT,
                        f.getType().getCode());
            } else {
                Assertions.assertEquals(
                        Type.Code.ARRAY,
                        f.getType().getCode());
                Assertions.assertEquals(
                        Type.Code.STRUCT,
                        f.getType().getArrayElementType().getCode());
            }
        });

        final Type resultType2 = StructSchemaUtil.flatten(struct.getType(), "children.grandchildren", true);
        Set<String> fieldNames2 = resultType2.getStructFields().stream().map(Type.StructField::getName).collect(Collectors.toSet());
        Assertions.assertEquals(
                Set.of("stringField", "children_cstringField", "children_grandchild", "children_grandchildren_gcstringField","children_grandchildrenNull"),
                fieldNames2);

        final Type resultType3 = StructSchemaUtil.flatten(struct.getType(), "children.grandchildrenNull", true);
        final Set<String> fieldNames3 = resultType3.getStructFields().stream()
                .map(Type.StructField::getName)
                .collect(Collectors.toSet());
        Assertions.assertEquals(
                Set.of("stringField", "children_cstringField", "children_grandchild", "children_grandchildrenNull_gcstringField","children_grandchildren"),
                fieldNames3);

    }

    @Test
    public void testFlattenValues() {
        final Struct struct = createTestStruct();
        Type type = StructSchemaUtil.flatten(struct.getType(), "children", true);
        List<Struct> resultStructChildren1 = StructSchemaUtil.flatten(type, struct, "children", true);

        Assertions.assertEquals(2, resultStructChildren1.size());

        // one path
        for(final Struct childrenStruct : resultStructChildren1) {
            Assertions.assertEquals("stringValue", childrenStruct.getString("stringField"));
            Assertions.assertEquals("cstringValue", childrenStruct.getString("children_cstringField"));

            final Struct grandchildStruct = childrenStruct.getStruct("children_grandchild");
            Assertions.assertEquals("gcstringValue", grandchildStruct.getString("gcstringField"));

            final List<Struct> grandchildrenStructs = childrenStruct.getStructList("children_grandchildren");
            Assertions.assertEquals(2, grandchildrenStructs.size());
            for(final Struct grandchildrenStruct : grandchildrenStructs) {
                Assertions.assertEquals("gcstringValue", grandchildrenStruct.getString("gcstringField"));
            }
        }

        // two path
        final Type resultTypeChildren2 = StructSchemaUtil.flatten(type, "children.grandchildren", true);
        final List<Struct> resultStructChildren2 = StructSchemaUtil.flatten(resultTypeChildren2, struct, "children.grandchildren", true);
        Assertions.assertEquals(4, resultStructChildren2.size());

        for(final Struct childrenStruct : resultStructChildren2) {
            Assertions.assertEquals("stringValue", childrenStruct.getString("stringField"));
            Assertions.assertEquals("cstringValue", childrenStruct.getString("children_cstringField"));
            Assertions.assertEquals("gcstringValue", childrenStruct.getString("children_grandchildren_gcstringField"));

            final Struct grandchildStruct = childrenStruct.getStruct("children_grandchild");
            Assertions.assertEquals("gcstringValue", grandchildStruct.getString("gcstringField"));
        }

        // one path without prefix
        final Type resultTypeChildren1WP = StructSchemaUtil.flatten(type, "children", false);
        final List<Struct> resultStructChildren1WP = StructSchemaUtil.flatten(resultTypeChildren1WP, struct, "children", false);
        Assertions.assertEquals(2, resultStructChildren1WP.size());

        for(final Struct childrenStruct : resultStructChildren1WP) {
            Assertions.assertEquals("stringValue", childrenStruct.getString("stringField"));
            Assertions.assertEquals("cstringValue", childrenStruct.getString("cstringField"));

            final Struct grandchildStruct = childrenStruct.getStruct("grandchild");
            Assertions.assertEquals("gcstringValue", grandchildStruct.getString("gcstringField"));

            final Collection<Struct> grandchildrenStructs = childrenStruct.getStructList("grandchildren");
            Assertions.assertEquals(2, grandchildrenStructs.size());
            for(final Struct grandchildrenStruct : grandchildrenStructs) {
                Assertions.assertEquals("gcstringValue", grandchildrenStruct.getString("gcstringField"));
            }
        }

        // two path without prefix
        final Type resultTypeChildren2WP = StructSchemaUtil.flatten(type, "children.grandchildren", false);
        final List<Struct> resultStructChildren2WP = StructSchemaUtil.flatten(resultTypeChildren2WP, struct, "children.grandchildren", false);
        Assertions.assertEquals(4, resultStructChildren2WP.size());

        for(final Struct childrenStruct : resultStructChildren2WP) {
            Assertions.assertEquals("stringValue", childrenStruct.getString("stringField"));
            Assertions.assertEquals("cstringValue", childrenStruct.getString("cstringField"));
            Assertions.assertEquals("gcstringValue", childrenStruct.getString("gcstringField"));

            final Struct grandchildStruct = childrenStruct.getStruct("grandchild");
            Assertions.assertEquals("gcstringValue", grandchildStruct.getString("gcstringField"));
        }

        // Null check
        // two path null
        final Type resultTypeChildren2Null = StructSchemaUtil.flatten(type, "children.grandchildrenNull", true);
        final List<Struct> resultStructChildren2Null = StructSchemaUtil.flatten(resultTypeChildren2Null, struct, "children.grandchildrenNull", true);
        Assertions.assertEquals(2, resultStructChildren2Null.size());
        System.out.println(resultStructChildren2Null.get(0).getType().getStructFields().stream().map(f -> f.getName()).collect(Collectors.toList()));

        for(final Struct childrenStruct : resultStructChildren2Null) {
            Assertions.assertEquals("stringValue", childrenStruct.getString("stringField"));
            Assertions.assertEquals("cstringValue", childrenStruct.getString("children_cstringField"));
        }

    }

    @Test
    public void testMerge() {
        final Struct struct = Struct.newBuilder()
                .set("str").to("a")
                .build();
        final Type childType = Type.struct(
                Type.StructField.of("str", Type.string()),
                Type.StructField.of("int", Type.string()),
                Type.StructField.of("float", Type.float64())
        );
        final Type type = Type.struct(
                Type.StructField.of("str", Type.string()),
                Type.StructField.of("int", Type.int64()),
                Type.StructField.of("float", Type.float64()),
                Type.StructField.of("struct", Type.struct(
                        Type.StructField.of("str", Type.string()),
                        Type.StructField.of("float", Type.float64())
                )),
                Type.StructField.of("structNo", Type.struct(
                        Type.StructField.of("str", Type.string()),
                        Type.StructField.of("float", Type.float64())
                )),
                Type.StructField.of("structNull", Type.struct(
                        Type.StructField.of("str", Type.string()),
                        Type.StructField.of("float", Type.float64())
                )),
                Type.StructField.of("boolArray", Type.array(Type.bool())),
                Type.StructField.of("dateArray", Type.array(Type.date())),
                Type.StructField.of("structArray", Type.array(childType))
        );

        final Map<String, Object> values = new HashMap<>();
        final Struct child = Struct.newBuilder().set("float").to(1D).build();
        final List<Struct> children = new ArrayList<>();
        children.add(Struct.newBuilder().set("str").to("b").build());
        values.put("struct", child);
        values.put("structNull", null);
        values.put("structArray", children);
        values.put("float", null);
        values.put("int", -1);
        values.put("boolArray", Arrays.asList(true, false, true));
        final Struct merged = StructSchemaUtil.merge(type, struct, values);

        Assertions.assertEquals(9, merged.getType().getStructFields().size());
        Assertions.assertEquals("a", merged.getString("str"));
        Assertions.assertEquals(-1, merged.getLong("int"));
        Assertions.assertEquals(Arrays.asList(true, false, true), merged.getBooleanList("boolArray"));
        Assertions.assertTrue(merged.isNull("float"));
        Assertions.assertTrue(merged.isNull("structNo"));
        Assertions.assertTrue(merged.isNull("structNull"));
        Assertions.assertTrue(merged.isNull("dateArray"));

        final Struct mergedChild = merged.getStruct("struct");
        Assertions.assertEquals(2, mergedChild.getType().getStructFields().size());
        Assertions.assertEquals(1D, mergedChild.getDouble("float"), DELTA);
        Assertions.assertTrue(mergedChild.isNull("str"));

        Assertions.assertEquals(1, merged.getStructList("structArray").size());
        for(final Struct c : merged.getStructList("structArray")) {
            Assertions.assertEquals(3, c.getType().getStructFields().size());
            Assertions.assertEquals("b", c.getString("str"));
            Assertions.assertTrue(c.isNull("int"));
            Assertions.assertTrue(c.isNull("float"));
        }
    }

    @Test
    public void testHasField() {
        final Struct struct = Struct.newBuilder().set("a").to("v").build();
        Assertions.assertTrue(StructSchemaUtil.hasField(struct, "a"));
        Assertions.assertFalse(StructSchemaUtil.hasField(struct, "b"));
        Assertions.assertFalse(StructSchemaUtil.hasField(struct, null));
        Assertions.assertFalse(StructSchemaUtil.hasField((Struct) null, "a"));
        Assertions.assertTrue(StructSchemaUtil.hasField(struct.getType(), "a"));
        Assertions.assertFalse(StructSchemaUtil.hasField((Type) null, "a"));
    }

    private Struct createPrimitiveStruct() {
        final Struct child = Struct.newBuilder().set("v").to("x").build();
        return Struct.newBuilder()
                .set("bool").to(true)
                .set("bytes").to(ByteArray.copyFrom(new byte[]{1, 2}))
                .set("str").to("12")
                .set("json").to(Value.json("{\"k\":1}"))
                .set("int").to(5L)
                .set("f32").to(1.5F)
                .set("f64").to(2.5D)
                .set("num").to(BigDecimal.valueOf(7))
                .set("date").to(Date.parseDate("1970-01-11"))
                .set("ts").to(Timestamp.ofTimeMicroseconds(123456789L))
                .set("time").to("12:34:56")
                .set("child").to(child)
                .set("nullstr").to((String) null)
                .set("boolArr").toBoolArray(Arrays.asList(true, false))
                .set("strArr").toStringArray(Arrays.asList("a", "b"))
                .set("intArr").toInt64Array(Arrays.asList(1L, 2L))
                .set("f32Arr").toFloat32Array(Arrays.asList(1.5F))
                .set("f64Arr").toFloat64Array(Arrays.asList(2.5D))
                .set("numArr").toNumericArray(Arrays.asList(BigDecimal.ONE))
                .set("dateArr").toDateArray(Arrays.asList(Date.parseDate("1970-01-11")))
                .set("tsArr").toTimestampArray(Arrays.asList(Timestamp.ofTimeMicroseconds(1000000L)))
                .set("childArr").toStructArray(child.getType(), Arrays.asList(child))
                .build();
    }

    @Test
    public void testGetValue() {
        final Struct struct = createPrimitiveStruct();
        Assertions.assertEquals(true, StructSchemaUtil.getValue(struct, "bool"));
        Assertions.assertArrayEquals(new byte[]{1, 2}, (byte[]) StructSchemaUtil.getValue(struct, "bytes"));
        Assertions.assertEquals("12", StructSchemaUtil.getValue(struct, "str"));
        Assertions.assertEquals("{\"k\":1}", StructSchemaUtil.getValue(struct, "json"));
        Assertions.assertEquals(5L, StructSchemaUtil.getValue(struct, "int"));
        Assertions.assertEquals(1.5F, StructSchemaUtil.getValue(struct, "f32"));
        Assertions.assertEquals(2.5D, StructSchemaUtil.getValue(struct, "f64"));
        Assertions.assertEquals(BigDecimal.valueOf(7), StructSchemaUtil.getValue(struct, "num"));
        Assertions.assertEquals(LocalDate.of(1970, 1, 11), StructSchemaUtil.getValue(struct, "date"));
        Assertions.assertEquals(org.joda.time.Instant.ofEpochMilli(123456L), StructSchemaUtil.getValue(struct, "ts"));
        Assertions.assertNull(StructSchemaUtil.getValue(struct, "nullstr"));
        Assertions.assertEquals(Arrays.asList(true, false), StructSchemaUtil.getValue(struct, "boolArr"));
        Assertions.assertEquals(Arrays.asList("a", "b"), StructSchemaUtil.getValue(struct, "strArr"));
        Assertions.assertEquals(Arrays.asList(1L, 2L), StructSchemaUtil.getValue(struct, "intArr"));
        Assertions.assertEquals(Arrays.asList(LocalDate.of(1970, 1, 11)), StructSchemaUtil.getValue(struct, "dateArr"));
        Assertions.assertEquals(Arrays.asList(org.joda.time.Instant.ofEpochMilli(1000L)), StructSchemaUtil.getValue(struct, "tsArr"));
        final Struct child = (Struct) StructSchemaUtil.getValue(struct, "child");
        Assertions.assertEquals("x", child.getString("v"));
    }

    @Test
    public void testGetRawValue() {
        final Struct struct = createPrimitiveStruct();
        Assertions.assertNull(StructSchemaUtil.getRawValue(null, "str"));
        Assertions.assertNull(StructSchemaUtil.getRawValue(struct, "missing"));
        Assertions.assertNull(StructSchemaUtil.getRawValue(struct, "nullstr"));
        Assertions.assertEquals(true, StructSchemaUtil.getRawValue(struct, "bool"));
        Assertions.assertEquals(ByteArray.copyFrom(new byte[]{1, 2}), StructSchemaUtil.getRawValue(struct, "bytes"));
        Assertions.assertEquals("12", StructSchemaUtil.getRawValue(struct, "str"));
        Assertions.assertEquals("{\"k\":1}", StructSchemaUtil.getRawValue(struct, "json"));
        Assertions.assertEquals(5L, StructSchemaUtil.getRawValue(struct, "int"));
        Assertions.assertEquals(1.5F, StructSchemaUtil.getRawValue(struct, "f32"));
        Assertions.assertEquals(2.5D, StructSchemaUtil.getRawValue(struct, "f64"));
        Assertions.assertEquals(BigDecimal.valueOf(7), StructSchemaUtil.getRawValue(struct, "num"));
        Assertions.assertEquals(Date.parseDate("1970-01-11"), StructSchemaUtil.getRawValue(struct, "date"));
        Assertions.assertEquals(Timestamp.ofTimeMicroseconds(123456789L), StructSchemaUtil.getRawValue(struct, "ts"));
        Assertions.assertEquals(Arrays.asList(true, false), StructSchemaUtil.getRawValue(struct, "boolArr"));
        Assertions.assertEquals(Arrays.asList("a", "b"), StructSchemaUtil.getRawValue(struct, "strArr"));
        Assertions.assertEquals(Arrays.asList(1L, 2L), StructSchemaUtil.getRawValue(struct, "intArr"));
        Assertions.assertEquals(Arrays.asList(1.5F), StructSchemaUtil.getRawValue(struct, "f32Arr"));
        Assertions.assertEquals(Arrays.asList(2.5D), StructSchemaUtil.getRawValue(struct, "f64Arr"));
        Assertions.assertEquals(Arrays.asList(BigDecimal.ONE), StructSchemaUtil.getRawValue(struct, "numArr"));
        Assertions.assertEquals(Arrays.asList(Date.parseDate("1970-01-11")), StructSchemaUtil.getRawValue(struct, "dateArr"));
        Assertions.assertEquals(Arrays.asList(Timestamp.ofTimeMicroseconds(1000000L)), StructSchemaUtil.getRawValue(struct, "tsArr"));
        Assertions.assertEquals(1, ((List<Struct>) StructSchemaUtil.getRawValue(struct, "childArr")).size());
        final Struct child = (Struct) StructSchemaUtil.getRawValue(struct, "child");
        Assertions.assertEquals("x", child.getString("v"));
    }

    @Test
    public void testGetCSVLineValue() {
        final Struct struct = createPrimitiveStruct();
        Assertions.assertNull(StructSchemaUtil.getCSVLineValue(null, "str"));
        Assertions.assertNull(StructSchemaUtil.getCSVLineValue(struct, "missing"));
        Assertions.assertNull(StructSchemaUtil.getCSVLineValue(struct, "nullstr"));
        Assertions.assertEquals(true, StructSchemaUtil.getCSVLineValue(struct, "bool"));
        Assertions.assertEquals(Base64.getEncoder().encodeToString(new byte[]{1, 2}), StructSchemaUtil.getCSVLineValue(struct, "bytes"));
        Assertions.assertEquals("12", StructSchemaUtil.getCSVLineValue(struct, "str"));
        Assertions.assertEquals(5L, StructSchemaUtil.getCSVLineValue(struct, "int"));
        Assertions.assertEquals(1.5F, StructSchemaUtil.getCSVLineValue(struct, "f32"));
        Assertions.assertEquals(2.5D, StructSchemaUtil.getCSVLineValue(struct, "f64"));
        Assertions.assertEquals("1970-01-11", StructSchemaUtil.getCSVLineValue(struct, "date"));
        Assertions.assertEquals(Timestamp.ofTimeMicroseconds(123456789L).toString(), StructSchemaUtil.getCSVLineValue(struct, "ts"));
    }

    @Test
    public void testGetStructValue() {
        final Struct struct = createPrimitiveStruct();
        Assertions.assertNull(StructSchemaUtil.getStructValue(null, "str"));
        Assertions.assertNull(StructSchemaUtil.getStructValue(struct, "missing"));
        Assertions.assertNull(StructSchemaUtil.getStructValue(struct, "nullstr"));
        Assertions.assertEquals(Value.bool(true), StructSchemaUtil.getStructValue(struct, "bool"));
        Assertions.assertEquals(Value.bytes(ByteArray.copyFrom(new byte[]{1, 2})), StructSchemaUtil.getStructValue(struct, "bytes"));
        Assertions.assertEquals(Value.string("12"), StructSchemaUtil.getStructValue(struct, "str"));
        Assertions.assertEquals(Value.json("{\"k\":1}"), StructSchemaUtil.getStructValue(struct, "json"));
        Assertions.assertEquals(Value.int64(5L), StructSchemaUtil.getStructValue(struct, "int"));
        Assertions.assertEquals(Value.float32(1.5F), StructSchemaUtil.getStructValue(struct, "f32"));
        Assertions.assertEquals(Value.float64(2.5D), StructSchemaUtil.getStructValue(struct, "f64"));
        Assertions.assertEquals(Value.numeric(BigDecimal.valueOf(7)), StructSchemaUtil.getStructValue(struct, "num"));
        Assertions.assertEquals(Value.date(Date.parseDate("1970-01-11")), StructSchemaUtil.getStructValue(struct, "date"));
        Assertions.assertEquals(Value.timestamp(Timestamp.ofTimeMicroseconds(123456789L)), StructSchemaUtil.getStructValue(struct, "ts"));
        Assertions.assertEquals(Value.boolArray(Arrays.asList(true, false)), StructSchemaUtil.getStructValue(struct, "boolArr"));
        Assertions.assertEquals(Value.stringArray(Arrays.asList("a", "b")), StructSchemaUtil.getStructValue(struct, "strArr"));
        Assertions.assertEquals(Value.int64Array(Arrays.asList(1L, 2L)), StructSchemaUtil.getStructValue(struct, "intArr"));
        Assertions.assertEquals(Value.float32Array(Arrays.asList(1.5F)), StructSchemaUtil.getStructValue(struct, "f32Arr"));
        Assertions.assertEquals(Value.float64Array(Arrays.asList(2.5D)), StructSchemaUtil.getStructValue(struct, "f64Arr"));
        Assertions.assertEquals(Value.numericArray(Arrays.asList(BigDecimal.ONE)), StructSchemaUtil.getStructValue(struct, "numArr"));
        Assertions.assertEquals(Value.dateArray(Arrays.asList(Date.parseDate("1970-01-11"))), StructSchemaUtil.getStructValue(struct, "dateArr"));
        Assertions.assertEquals(Value.timestampArray(Arrays.asList(Timestamp.ofTimeMicroseconds(1000000L))), StructSchemaUtil.getStructValue(struct, "tsArr"));
        final Value structValue = StructSchemaUtil.getStructValue(struct, "child");
        Assertions.assertEquals(Type.Code.STRUCT, structValue.getType().getCode());
        Assertions.assertEquals("x", structValue.getStruct().getString("v"));
        final Value structArrayValue = StructSchemaUtil.getStructValue(struct, "childArr");
        Assertions.assertEquals(1, structArrayValue.getStructArray().size());
    }

    @Test
    public void testGetAsString() {
        final Struct struct = createPrimitiveStruct();
        Assertions.assertNull(StructSchemaUtil.getAsString((Object) null, "str"));
        Assertions.assertNull(StructSchemaUtil.getAsString(struct, "missing"));
        Assertions.assertNull(StructSchemaUtil.getAsString(struct, "nullstr"));
        Assertions.assertEquals("true", StructSchemaUtil.getAsString(struct, "bool"));
        Assertions.assertEquals(Base64.getEncoder().encodeToString(new byte[]{1, 2}), StructSchemaUtil.getAsString(struct, "bytes"));
        Assertions.assertEquals("12", StructSchemaUtil.getAsString((Object) struct, "str"));
        Assertions.assertEquals("5", StructSchemaUtil.getAsString(struct, "int"));
        Assertions.assertEquals("1.5", StructSchemaUtil.getAsString(struct, "f32"));
        Assertions.assertEquals("2.5", StructSchemaUtil.getAsString(struct, "f64"));
        Assertions.assertEquals("7", StructSchemaUtil.getAsString(struct, "num"));
        Assertions.assertEquals("1970-01-11", StructSchemaUtil.getAsString(struct, "date"));
        Assertions.assertEquals("[1,2]", StructSchemaUtil.getAsString(struct, "intArr"));
        Assertions.assertEquals("[a,b]", StructSchemaUtil.getAsString(struct, "strArr"));
        Assertions.assertEquals("[1.5]", StructSchemaUtil.getAsString(struct, "f32Arr"));
        Assertions.assertEquals("[2.5]", StructSchemaUtil.getAsString(struct, "f64Arr"));
    }

    @Test
    public void testGetAsNumbers() {
        final Struct struct = createPrimitiveStruct();
        // getAsLong
        Assertions.assertNull(StructSchemaUtil.getAsLong(struct, "nullstr"));
        Assertions.assertEquals(1L, StructSchemaUtil.getAsLong(struct, "bool"));
        Assertions.assertEquals(12L, StructSchemaUtil.getAsLong(struct, "str"));
        Assertions.assertEquals(5L, StructSchemaUtil.getAsLong(struct, "int"));
        Assertions.assertEquals(1L, StructSchemaUtil.getAsLong(struct, "f32"));
        Assertions.assertEquals(2L, StructSchemaUtil.getAsLong(struct, "f64"));
        Assertions.assertEquals(7L, StructSchemaUtil.getAsLong(struct, "num"));
        Assertions.assertNull(StructSchemaUtil.getAsLong(struct, "time")); // "12:34:56" is not a long
        // getAsDouble
        Assertions.assertNull(StructSchemaUtil.getAsDouble(struct, "nullstr"));
        Assertions.assertEquals(1D, StructSchemaUtil.getAsDouble(struct, "bool"), DELTA);
        Assertions.assertEquals(12D, StructSchemaUtil.getAsDouble(struct, "str"), DELTA);
        Assertions.assertEquals(5D, StructSchemaUtil.getAsDouble(struct, "int"), DELTA);
        Assertions.assertEquals(1.5D, StructSchemaUtil.getAsDouble(struct, "f32"), DELTA);
        Assertions.assertEquals(2.5D, StructSchemaUtil.getAsDouble(struct, "f64"), DELTA);
        Assertions.assertEquals(7D, StructSchemaUtil.getAsDouble(struct, "num"), DELTA);
        // getAsBigDecimal
        Assertions.assertNull(StructSchemaUtil.getAsBigDecimal(struct, "nullstr"));
        Assertions.assertEquals(BigDecimal.valueOf(1D), StructSchemaUtil.getAsBigDecimal(struct, "bool"));
        Assertions.assertEquals(BigDecimal.valueOf(12D), StructSchemaUtil.getAsBigDecimal(struct, "str"));
        Assertions.assertEquals(BigDecimal.valueOf(5L), StructSchemaUtil.getAsBigDecimal(struct, "int"));
        Assertions.assertEquals(BigDecimal.valueOf(2.5D), StructSchemaUtil.getAsBigDecimal(struct, "f64"));
        Assertions.assertEquals(BigDecimal.valueOf(7), StructSchemaUtil.getAsBigDecimal(struct, "num"));
    }

    @Test
    public void testGetBytesAndAsBytes() {
        final String base64 = Base64.getEncoder().encodeToString(new byte[]{3, 4});
        final Struct struct = Struct.newBuilder()
                .set("bytes").to(ByteArray.copyFrom(new byte[]{1, 2}))
                .set("str").to(base64)
                .set("int").to(1L)
                .build();
        Assertions.assertArrayEquals(new byte[]{1, 2}, StructSchemaUtil.getBytes(struct, "bytes"));
        Assertions.assertArrayEquals(new byte[]{3, 4}, StructSchemaUtil.getBytes(struct, "str"));
        Assertions.assertNull(StructSchemaUtil.getBytes(struct, "int"));
        Assertions.assertNull(StructSchemaUtil.getBytes(struct, "missing"));
        Assertions.assertEquals(ByteBuffer.wrap(new byte[]{1, 2}), StructSchemaUtil.getAsBytes(struct, "bytes"));
        Assertions.assertEquals(ByteBuffer.wrap(new byte[]{3, 4}), StructSchemaUtil.getAsBytes(struct, "str"));
        Assertions.assertNull(StructSchemaUtil.getAsBytes(struct, "missing"));
        // getAsByteString
        Assertions.assertNull(StructSchemaUtil.getAsByteString(struct, "missing"));
        Assertions.assertNull(StructSchemaUtil.getAsByteString(null, "bytes"));
        Assertions.assertEquals(BigtableSchemaUtil.toByteString(base64), StructSchemaUtil.getAsByteString(struct, "str"));
        Assertions.assertEquals(BigtableSchemaUtil.toByteString(1L), StructSchemaUtil.getAsByteString(struct, "int"));
    }

    @Test
    public void testGetAsFloatList() {
        final Struct struct = createPrimitiveStruct();
        Assertions.assertEquals(new ArrayList<Float>(), StructSchemaUtil.getAsFloatList(null, "f32Arr"));
        Assertions.assertEquals(new ArrayList<Float>(), StructSchemaUtil.getAsFloatList(struct, "missing"));
        Assertions.assertEquals(new ArrayList<Float>(), StructSchemaUtil.getAsFloatList(struct, "str"));
        Assertions.assertEquals(Arrays.asList(1F, 0F), StructSchemaUtil.getAsFloatList(struct, "boolArr"));
        Assertions.assertEquals(Arrays.asList(1F, 2F), StructSchemaUtil.getAsFloatList(struct, "intArr"));
        Assertions.assertEquals(Arrays.asList(1.5F), StructSchemaUtil.getAsFloatList(struct, "f32Arr"));
        Assertions.assertEquals(Arrays.asList(2.5F), StructSchemaUtil.getAsFloatList(struct, "f64Arr"));
        Assertions.assertEquals(Arrays.asList(1F), StructSchemaUtil.getAsFloatList(struct, "numArr"));
    }

    @Test
    public void testGetEpochDayAndToInstant() {
        Assertions.assertEquals(10L, StructSchemaUtil.getEpochDay(Date.parseDate("1970-01-11")));
        Assertions.assertEquals(
                org.joda.time.Instant.ofEpochMilli(1500L),
                StructSchemaUtil.toInstant(Timestamp.ofTimeMicroseconds(1500000L)));
    }

    @Test
    public void testGetTimestamp() {
        final org.joda.time.Instant defaultInstant = org.joda.time.Instant.ofEpochMilli(0L);
        final Struct struct = Struct.newBuilder()
                .set("str").to("2024-01-02T03:04:05Z")
                .set("date").to(Date.parseDate("1970-01-11"))
                .set("ts").to(Timestamp.ofTimeMicroseconds(123000000L))
                .set("nullts").to((Timestamp) null)
                .build();
        Assertions.assertEquals(
                DateTimeUtil.toJodaInstant("2024-01-02T03:04:05Z"),
                StructSchemaUtil.getTimestamp(struct, "str", defaultInstant));
        Assertions.assertEquals(
                org.joda.time.Instant.parse("1970-01-11T00:00:00Z"),
                StructSchemaUtil.getTimestamp(struct, "date", defaultInstant));
        Assertions.assertEquals(
                org.joda.time.Instant.ofEpochMilli(123000L),
                StructSchemaUtil.getTimestamp(struct, "ts", defaultInstant));
        Assertions.assertEquals(defaultInstant, StructSchemaUtil.getTimestamp(struct, "nullts", defaultInstant));
    }

    @Test
    public void testGetAsPrimitiveWithFieldTypeAndValue() {
        Assertions.assertNull(StructSchemaUtil.getAsPrimitive(Schema.FieldType.STRING, null));
        Assertions.assertEquals("a", StructSchemaUtil.getAsPrimitive(Schema.FieldType.STRING, "a"));
        Assertions.assertEquals(5L, StructSchemaUtil.getAsPrimitive(Schema.FieldType.INT64, 5L));
        Assertions.assertEquals(3, StructSchemaUtil.getAsPrimitive(Schema.FieldType.INT32, 3L));
        Assertions.assertEquals(123456789L, StructSchemaUtil.getAsPrimitive(Schema.FieldType.DATETIME, Timestamp.ofTimeMicroseconds(123456789L)));
        Assertions.assertEquals(10, StructSchemaUtil.getAsPrimitive(CalciteUtils.DATE, Date.parseDate("1970-01-11")));
        Assertions.assertEquals(LocalTime.parse("12:34:56").toNanoOfDay() / 1000L, StructSchemaUtil.getAsPrimitive(CalciteUtils.TIME, "12:34:56"));
        final Schema.FieldType enumType = Schema.FieldType.logicalType(EnumerationType.create("RED", "GREEN"));
        Assertions.assertEquals("RED", StructSchemaUtil.getAsPrimitive(enumType, "RED"));
        // arrays
        Assertions.assertEquals(Arrays.asList(1L, 2L), StructSchemaUtil.getAsPrimitive(Schema.FieldType.array(Schema.FieldType.INT64), Arrays.asList(1L, 2L)));
        Assertions.assertEquals(Arrays.asList(1, 2), StructSchemaUtil.getAsPrimitive(Schema.FieldType.array(Schema.FieldType.INT32), Arrays.asList(1L, 2L)));
        Assertions.assertEquals(
                Arrays.asList(1000000L),
                StructSchemaUtil.getAsPrimitive(Schema.FieldType.array(Schema.FieldType.DATETIME), Arrays.asList(Timestamp.ofTimeMicroseconds(1000000L))));
        Assertions.assertEquals(
                Arrays.asList(10),
                StructSchemaUtil.getAsPrimitive(Schema.FieldType.array(CalciteUtils.DATE), Arrays.asList(Date.parseDate("1970-01-11"))));
    }

    @Test
    public void testGetAsPrimitiveFromStruct() {
        final Struct struct = createPrimitiveStruct();
        Assertions.assertNull(StructSchemaUtil.getAsPrimitive(null, Schema.FieldType.STRING, "str"));
        Assertions.assertNull(StructSchemaUtil.getAsPrimitive(struct, Schema.FieldType.STRING, "nullstr"));
        Assertions.assertEquals(true, StructSchemaUtil.getAsPrimitive(struct, Schema.FieldType.BOOLEAN, "bool"));
        Assertions.assertEquals("12", StructSchemaUtil.getAsPrimitive(struct, Schema.FieldType.STRING, "str"));
        // INT32
        Assertions.assertEquals(12, StructSchemaUtil.getAsPrimitive(struct, Schema.FieldType.INT32, "str"));
        Assertions.assertEquals(5, StructSchemaUtil.getAsPrimitive(struct, Schema.FieldType.INT32, "int"));
        Assertions.assertEquals(1, StructSchemaUtil.getAsPrimitive(struct, Schema.FieldType.INT32, "bool"));
        Assertions.assertEquals(2, StructSchemaUtil.getAsPrimitive(struct, Schema.FieldType.INT32, "f64"));
        Assertions.assertEquals(10, StructSchemaUtil.getAsPrimitive(struct, Schema.FieldType.INT32, "date"));
        Assertions.assertEquals(7, StructSchemaUtil.getAsPrimitive(struct, Schema.FieldType.INT32, "num"));
        // INT64
        Assertions.assertEquals(12L, StructSchemaUtil.getAsPrimitive(struct, Schema.FieldType.INT64, "str"));
        Assertions.assertEquals(5L, StructSchemaUtil.getAsPrimitive(struct, Schema.FieldType.INT64, "int"));
        Assertions.assertEquals(10L, StructSchemaUtil.getAsPrimitive(struct, Schema.FieldType.INT64, "date"));
        Assertions.assertEquals(123456789L, StructSchemaUtil.getAsPrimitive(struct, Schema.FieldType.INT64, "ts"));
        // FLOAT
        Assertions.assertEquals(1.5F, StructSchemaUtil.getAsPrimitive(struct, Schema.FieldType.FLOAT, "f32"));
        Assertions.assertEquals(12F, StructSchemaUtil.getAsPrimitive(struct, Schema.FieldType.FLOAT, "str"));
        Assertions.assertEquals(5F, StructSchemaUtil.getAsPrimitive(struct, Schema.FieldType.FLOAT, "int"));
        // DOUBLE
        Assertions.assertEquals(2.5D, StructSchemaUtil.getAsPrimitive(struct, Schema.FieldType.DOUBLE, "f64"));
        Assertions.assertEquals(5D, StructSchemaUtil.getAsPrimitive(struct, Schema.FieldType.DOUBLE, "int"));
        Assertions.assertEquals(1D, StructSchemaUtil.getAsPrimitive(struct, Schema.FieldType.DOUBLE, "bool"));
        // DATETIME, logical types
        Assertions.assertEquals(123456789L, StructSchemaUtil.getAsPrimitive(struct, Schema.FieldType.DATETIME, "ts"));
        Assertions.assertEquals(10, StructSchemaUtil.getAsPrimitive(struct, CalciteUtils.DATE, "date"));
        Assertions.assertEquals(LocalTime.parse("12:34:56").toNanoOfDay() / 1000L, StructSchemaUtil.getAsPrimitive(struct, CalciteUtils.TIME, "time"));
        final Schema.FieldType enumType = Schema.FieldType.logicalType(EnumerationType.create("RED", "GREEN"));
        Assertions.assertEquals("12", StructSchemaUtil.getAsPrimitive(struct, enumType, "str"));
        // arrays
        Assertions.assertEquals(Arrays.asList(true, false), StructSchemaUtil.getAsPrimitive(struct, Schema.FieldType.array(Schema.FieldType.BOOLEAN), "boolArr"));
        Assertions.assertEquals(Arrays.asList("a", "b"), StructSchemaUtil.getAsPrimitive(struct, Schema.FieldType.array(Schema.FieldType.STRING), "strArr"));
        Assertions.assertEquals(Arrays.asList(1, 2), StructSchemaUtil.getAsPrimitive(struct, Schema.FieldType.array(Schema.FieldType.INT32), "intArr"));
        Assertions.assertEquals(Arrays.asList(1L, 2L), StructSchemaUtil.getAsPrimitive(struct, Schema.FieldType.array(Schema.FieldType.INT64), "intArr"));
        Assertions.assertEquals(Arrays.asList(1.5F), StructSchemaUtil.getAsPrimitive(struct, Schema.FieldType.array(Schema.FieldType.FLOAT), "f32Arr"));
        Assertions.assertEquals(Arrays.asList(2.5D), StructSchemaUtil.getAsPrimitive(struct, Schema.FieldType.array(Schema.FieldType.DOUBLE), "f64Arr"));
        Assertions.assertEquals(Arrays.asList(10), StructSchemaUtil.getAsPrimitive(struct, Schema.FieldType.array(CalciteUtils.DATE), "dateArr"));
        // nested field
        Assertions.assertEquals("x", StructSchemaUtil.getAsPrimitive(struct, Schema.FieldType.STRING, "child.v"));
    }

    @Test
    public void testGetAsPrimitiveFromValue() {
        Assertions.assertNull(StructSchemaUtil.getAsPrimitive((Value) null));
        Assertions.assertNull(StructSchemaUtil.getAsPrimitive(Value.string(null)));
        Assertions.assertEquals("a", StructSchemaUtil.getAsPrimitive(Value.string("a")));
        Assertions.assertEquals(true, StructSchemaUtil.getAsPrimitive(Value.bool(true)));
        Assertions.assertEquals("{}", StructSchemaUtil.getAsPrimitive(Value.json("{}")));
        Assertions.assertEquals(5L, StructSchemaUtil.getAsPrimitive(Value.int64(5L)));
        Assertions.assertEquals(1.5F, StructSchemaUtil.getAsPrimitive(Value.float32(1.5F)));
        Assertions.assertEquals(2.5D, StructSchemaUtil.getAsPrimitive(Value.float64(2.5D)));
        Assertions.assertEquals(10, StructSchemaUtil.getAsPrimitive(Value.date(Date.parseDate("1970-01-11"))));
        Assertions.assertEquals(123456789L, StructSchemaUtil.getAsPrimitive(Value.timestamp(Timestamp.ofTimeMicroseconds(123456789L))));
        Assertions.assertArrayEquals(new byte[]{1, 2}, (byte[]) StructSchemaUtil.getAsPrimitive(Value.bytes(ByteArray.copyFrom(new byte[]{1, 2}))));
        Assertions.assertEquals(BigDecimal.valueOf(7), StructSchemaUtil.getAsPrimitive(Value.numeric(BigDecimal.valueOf(7))));
        final Struct child = Struct.newBuilder().set("v").to("x").build();
        final String json = (String) StructSchemaUtil.getAsPrimitive(Value.struct(child));
        Assertions.assertTrue(json.contains("\"v\""));
        // arrays
        Assertions.assertEquals(Arrays.asList("a"), StructSchemaUtil.getAsPrimitive(Value.stringArray(Arrays.asList("a"))));
        Assertions.assertEquals(Arrays.asList(true), StructSchemaUtil.getAsPrimitive(Value.boolArray(Arrays.asList(true))));
        Assertions.assertEquals(Arrays.asList(1L), StructSchemaUtil.getAsPrimitive(Value.int64Array(Arrays.asList(1L))));
        Assertions.assertEquals(Arrays.asList(1.5F), StructSchemaUtil.getAsPrimitive(Value.float32Array(Arrays.asList(1.5F))));
        Assertions.assertEquals(Arrays.asList(2.5D), StructSchemaUtil.getAsPrimitive(Value.float64Array(Arrays.asList(2.5D))));
        Assertions.assertEquals(Arrays.asList(10), StructSchemaUtil.getAsPrimitive(Value.dateArray(Arrays.asList(Date.parseDate("1970-01-11")))));
        Assertions.assertEquals(Arrays.asList(1000000L), StructSchemaUtil.getAsPrimitive(Value.timestampArray(Arrays.asList(Timestamp.ofTimeMicroseconds(1000000L)))));
    }

    @Test
    public void testGetAsStandard() {
        Assertions.assertNull(StructSchemaUtil.getAsStandard(null));
        Assertions.assertNull(StructSchemaUtil.getAsStandard(Value.int64(null)));
        Assertions.assertEquals("a", StructSchemaUtil.getAsStandard(Value.string("a")));
        Assertions.assertEquals(5L, StructSchemaUtil.getAsStandard(Value.int64(5L)));
        Assertions.assertEquals(LocalDate.of(1970, 1, 11), StructSchemaUtil.getAsStandard(Value.date(Date.parseDate("1970-01-11"))));
        Assertions.assertEquals(java.time.Instant.ofEpochSecond(123L), StructSchemaUtil.getAsStandard(Value.timestamp(Timestamp.ofTimeSecondsAndNanos(123L, 0))));
        Assertions.assertEquals(ByteBuffer.wrap(new byte[]{1, 2}), StructSchemaUtil.getAsStandard(Value.bytes(ByteArray.copyFrom(new byte[]{1, 2}))));
        final Struct child = Struct.newBuilder().set("v").to("x").build();
        final Map<String, Object> map = (Map<String, Object>) StructSchemaUtil.getAsStandard(Value.struct(child));
        Assertions.assertEquals("x", map.get("v"));
        Assertions.assertEquals(
                Arrays.asList(LocalDate.of(1970, 1, 11)),
                StructSchemaUtil.getAsStandard(Value.dateArray(Arrays.asList(Date.parseDate("1970-01-11")))));
        Assertions.assertEquals(
                Arrays.asList(java.time.Instant.ofEpochSecond(1L)),
                StructSchemaUtil.getAsStandard(Value.timestampArray(Arrays.asList(Timestamp.ofTimeSecondsAndNanos(1L, 0)))));
    }

    @Test
    public void testAsPrimitiveMapAndStandardMap() {
        final Struct struct = Struct.newBuilder()
                .set("str").to("a")
                .set("int").to(1L)
                .set("date").to(Date.parseDate("1970-01-11"))
                .build();
        final Map<String, Object> primitiveMap = StructSchemaUtil.asPrimitiveMap(struct);
        Assertions.assertEquals(3, primitiveMap.size());
        Assertions.assertEquals("a", primitiveMap.get("str"));
        Assertions.assertEquals(1L, primitiveMap.get("int"));
        Assertions.assertEquals(10, primitiveMap.get("date"));
        Assertions.assertTrue(StructSchemaUtil.asPrimitiveMap(null).isEmpty());
        final Map<String, Object> filtered = StructSchemaUtil.asPrimitiveMap(struct, Arrays.asList("str"));
        Assertions.assertEquals(1, filtered.size());

        final Map<String, Object> standardMap = StructSchemaUtil.asStandardMap(struct, null);
        Assertions.assertEquals(3, standardMap.size());
        Assertions.assertEquals("a", standardMap.get("str"));
        Assertions.assertEquals(1L, standardMap.get("int"));
        Assertions.assertEquals(LocalDate.of(1970, 1, 11), standardMap.get("date"));
        Assertions.assertTrue(StructSchemaUtil.asStandardMap(null, null).isEmpty());
    }

    @Test
    public void testConvertPrimitive() {
        Assertions.assertNull(StructSchemaUtil.convertPrimitive(Schema.FieldType.STRING, null));
        Assertions.assertEquals("a", StructSchemaUtil.convertPrimitive(Schema.FieldType.STRING, "a"));
        Assertions.assertEquals(5L, StructSchemaUtil.convertPrimitive(Schema.FieldType.INT64, 5L));
        Assertions.assertEquals(
                Timestamp.ofTimeMicroseconds(123456789L),
                StructSchemaUtil.convertPrimitive(Schema.FieldType.DATETIME, 123456789L));
        Assertions.assertEquals(
                Date.parseDate("1970-01-11"),
                StructSchemaUtil.convertPrimitive(CalciteUtils.DATE, 10));
        Assertions.assertEquals(
                LocalTime.parse("12:34:56").toString(),
                StructSchemaUtil.convertPrimitive(CalciteUtils.TIME, LocalTime.parse("12:34:56").toNanoOfDay()));
        final Schema.FieldType enumType = Schema.FieldType.logicalType(EnumerationType.create("RED", "GREEN"));
        Assertions.assertEquals(
                enumType.getLogicalType(EnumerationType.class).valueOf(1),
                StructSchemaUtil.convertPrimitive(enumType, 1));
        // arrays
        Assertions.assertEquals(
                Arrays.asList(1L, 2L),
                StructSchemaUtil.convertPrimitive(Schema.FieldType.array(Schema.FieldType.INT64), Arrays.asList(1L, 2L)));
        Assertions.assertEquals(
                Arrays.asList(Timestamp.ofTimeMicroseconds(1000000L)),
                StructSchemaUtil.convertPrimitive(Schema.FieldType.array(Schema.FieldType.DATETIME), Arrays.asList(1000000L)));
        Assertions.assertEquals(
                Arrays.asList(Date.parseDate("1970-01-11")),
                StructSchemaUtil.convertPrimitive(Schema.FieldType.array(CalciteUtils.DATE), Arrays.asList(10)));
    }

    @Test
    public void testToBuilderCopy() {
        final Struct child = Struct.newBuilder().set("v").to("x").build();
        final Struct struct = Struct.newBuilder()
                .set("bool").to(true)
                .set("bytes").to(ByteArray.copyFrom(new byte[]{1, 2}))
                .set("str").to("12")
                .set("int").to(5L)
                .set("f32").to(1.5F)
                .set("f64").to(2.5D)
                .set("num").to(BigDecimal.valueOf(7))
                .set("date").to(Date.parseDate("1970-01-11"))
                .set("ts").to(Timestamp.ofTimeMicroseconds(123456789L))
                .set("child").to(child)
                .set("boolArr").toBoolArray(Arrays.asList(true, false))
                .set("strArr").toStringArray(Arrays.asList("a", "b"))
                .set("intArr").toInt64Array(Arrays.asList(1L, 2L))
                .set("f32Arr").toFloat32Array(Arrays.asList(1.5F))
                .set("f64Arr").toFloat64Array(Arrays.asList(2.5D))
                .set("numArr").toNumericArray(Arrays.asList(BigDecimal.ONE))
                .set("dateArr").toDateArray(Arrays.asList(Date.parseDate("1970-01-11")))
                .set("tsArr").toTimestampArray(Arrays.asList(Timestamp.ofTimeMicroseconds(1000000L)))
                .set("childArr").toStructArray(child.getType(), Arrays.asList(child))
                .build();
        final Struct copied = StructSchemaUtil.toBuilder(struct).build();
        Assertions.assertEquals(struct.getType(), copied.getType());
        Assertions.assertEquals("12", copied.getString("str"));
        Assertions.assertEquals(5L, copied.getLong("int"));
        Assertions.assertEquals(1.5F, copied.getFloat("f32"), DELTA);
        Assertions.assertEquals(2.5D, copied.getDouble("f64"), DELTA);
        Assertions.assertEquals(BigDecimal.valueOf(7), copied.getBigDecimal("num"));
        Assertions.assertEquals(Date.parseDate("1970-01-11"), copied.getDate("date"));
        Assertions.assertEquals(Arrays.asList(1L, 2L), copied.getLongList("intArr"));
        Assertions.assertEquals("x", copied.getStruct("child").getString("v"));

        final Struct included = StructSchemaUtil.toBuilder(struct, Arrays.asList("str", "int"), null).build();
        Assertions.assertEquals(2, included.getColumnCount());
        final Struct excluded = StructSchemaUtil.toBuilder(struct, Arrays.asList("str", "int"), Arrays.asList("int")).build();
        Assertions.assertEquals(1, excluded.getColumnCount());
        Assertions.assertEquals("12", excluded.getString("str"));
    }

    @Test
    public void testToBuilderWithTypeNullValues() {
        final Type childType = Type.struct(Type.StructField.of("v", Type.string()));
        final Struct struct = Struct.newBuilder()
                .set("bool").to((Boolean) null)
                .set("str").to((String) null)
                .set("json").to(Value.json(null))
                .set("bytes").to((ByteArray) null)
                .set("int").to((Long) null)
                .set("f32").to((Float) null)
                .set("f64").to((Double) null)
                .set("num").to((BigDecimal) null)
                .set("date").to((Date) null)
                .set("ts").to((Timestamp) null)
                .set("child").to(childType, null)
                .set("boolArr").toBoolArray((Iterable<Boolean>) null)
                .set("bytesArr").toBytesArray(null)
                .set("strArr").toStringArray(null)
                .set("intArr").toInt64Array((Iterable<Long>) null)
                .set("f32Arr").toFloat32Array((Iterable<Float>) null)
                .set("f64Arr").toFloat64Array((Iterable<Double>) null)
                .set("numArr").toNumericArray(null)
                .set("dateArr").toDateArray(null)
                .set("tsArr").toTimestampArray(null)
                .set("childArr").toStructArray(childType, null)
                .build();
        final Struct result = StructSchemaUtil.toBuilder(struct.getType(), struct).build();
        Assertions.assertEquals(struct.getColumnCount(), result.getColumnCount());
        for(final Type.StructField field : result.getType().getStructFields()) {
            Assertions.assertTrue(result.isNull(field.getName()), "field: " + field.getName() + " should be null");
        }
    }

    @Test
    public void testToBuilderWithRenameFields() {
        final Struct struct = Struct.newBuilder()
                .set("a").to("x")
                .set("b").to(1L)
                .build();

        // renameFields maps new field name -> original field name
        final Type type1 = Type.struct(
                Type.StructField.of("a2", Type.string()),
                Type.StructField.of("b", Type.int64()),
                Type.StructField.of("c", Type.string()));
        final Struct result1 = StructSchemaUtil.toBuilder(type1, struct, Map.of("a2", "a")).build();
        Assertions.assertEquals("x", result1.getString("a2"));
        Assertions.assertEquals(1L, result1.getLong("b"));
        Assertions.assertTrue(result1.isNull("c"));

        // renameFields maps original field name -> new field name
        final Type type2 = Type.struct(
                Type.StructField.of("a3", Type.string()),
                Type.StructField.of("b", Type.int64()));
        final Struct result2 = StructSchemaUtil.toBuilder(type2, struct, Map.of("a", "a3")).build();
        Assertions.assertEquals("x", result2.getString("a3"));
        Assertions.assertEquals(1L, result2.getLong("b"));
    }

    @Test
    public void testCreate() {
        final Type type = Type.struct(
                Type.StructField.of("bool", Type.bool()),
                Type.StructField.of("str", Type.string()),
                Type.StructField.of("bytes", Type.bytes()),
                Type.StructField.of("intAsInteger", Type.int64()),
                Type.StructField.of("intAsLong", Type.int64()),
                Type.StructField.of("f32", Type.float32()),
                Type.StructField.of("f64", Type.float64()),
                Type.StructField.of("num", Type.numeric()),
                Type.StructField.of("date", Type.date()),
                Type.StructField.of("ts", Type.timestamp()),
                Type.StructField.of("child", Type.struct(Type.StructField.of("v", Type.string()))));
        final Struct child = Struct.newBuilder().set("v").to("x").build();
        final Map<String, Object> values = new HashMap<>();
        values.put("bool", true);
        values.put("str", "a");
        values.put("bytes", ByteArray.copyFrom(new byte[]{1}));
        values.put("intAsInteger", 3);
        values.put("intAsLong", 4L);
        values.put("f32", 1.5F);
        values.put("f64", 2.5D);
        values.put("num", BigDecimal.ONE);
        values.put("date", Date.parseDate("2020-01-02"));
        values.put("ts", Timestamp.ofTimeMicroseconds(1000L));
        values.put("child", child);

        final Struct struct = StructSchemaUtil.create(type, values);
        Assertions.assertTrue(struct.getBoolean("bool"));
        Assertions.assertEquals("a", struct.getString("str"));
        Assertions.assertEquals(ByteArray.copyFrom(new byte[]{1}), struct.getBytes("bytes"));
        Assertions.assertEquals(3L, struct.getLong("intAsInteger"));
        Assertions.assertEquals(4L, struct.getLong("intAsLong"));
        Assertions.assertEquals(1.5F, struct.getFloat("f32"), DELTA);
        Assertions.assertEquals(2.5D, struct.getDouble("f64"), DELTA);
        Assertions.assertEquals(BigDecimal.ONE, struct.getBigDecimal("num"));
        Assertions.assertEquals(Date.parseDate("2020-01-02"), struct.getDate("date"));
        Assertions.assertEquals(Timestamp.ofTimeMicroseconds(1000L), struct.getTimestamp("ts"));
        Assertions.assertEquals("x", struct.getStruct("child").getString("v"));
    }

    @Test
    public void testAddStructFieldAndRenameFields() {
        final Type type = Type.struct(Type.StructField.of("a", Type.string()));
        final Type added = StructSchemaUtil.addStructField(type, Arrays.asList(Type.StructField.of("b", Type.int64())));
        Assertions.assertEquals(2, added.getStructFields().size());
        Assertions.assertEquals(Type.Code.INT64, added.getStructFields().get(added.getFieldIndex("b")).getType().getCode());

        final Type renamed = StructSchemaUtil.renameFields(added, Map.of("a", "a2"));
        Assertions.assertTrue(renamed.getStructFields().stream().anyMatch(f -> f.getName().equals("a2")));
        Assertions.assertTrue(renamed.getStructFields().stream().anyMatch(f -> f.getName().equals("b")));
        Assertions.assertTrue(renamed.getStructFields().stream().noneMatch(f -> f.getName().equals("a")));
    }

    private Struct createInformationSchemaStruct(final String name, final String spannerType, final String nullable) {
        return Struct.newBuilder()
                .set("COLUMN_NAME").to(name)
                .set("SPANNER_TYPE").to(spannerType)
                .set("IS_NULLABLE").to(nullable)
                .build();
    }

    @Test
    public void testConvertFromInformationSchema() {
        final List<Struct> structs = Arrays.asList(
                createInformationSchemaStruct("id", "INT64", "NO"),
                createInformationSchemaStruct("name", "STRING(MAX)", "YES"),
                createInformationSchemaStruct("score", "FLOAT32", "YES"),
                createInformationSchemaStruct("price", "FLOAT64", "YES"),
                createInformationSchemaStruct("amount", "NUMERIC", "YES"),
                createInformationSchemaStruct("flag", "BOOL", "YES"),
                createInformationSchemaStruct("attrs", "JSON", "YES"),
                createInformationSchemaStruct("birth", "DATE", "YES"),
                createInformationSchemaStruct("created", "TIMESTAMP", "YES"),
                createInformationSchemaStruct("data", "BYTES(10)", "YES"),
                createInformationSchemaStruct("tags", "ARRAY<STRING(MAX)>", "YES"),
                createInformationSchemaStruct("skipped", "INT64", "YES"));
        final List<String> columns = Arrays.asList(
                "id", "name", "score", "price", "amount", "flag", "attrs", "birth", "created", "data", "tags");

        final Schema schema = StructSchemaUtil.convertSchemaFromInformationSchema(structs, columns);
        Assertions.assertEquals(11, schema.getFieldCount());
        Assertions.assertEquals(Schema.TypeName.INT64, schema.getField("id").getType().getTypeName());
        Assertions.assertFalse(schema.getField("id").getType().getNullable());
        Assertions.assertEquals(Schema.TypeName.STRING, schema.getField("name").getType().getTypeName());
        Assertions.assertTrue(schema.getField("name").getType().getNullable());
        Assertions.assertEquals(Schema.TypeName.FLOAT, schema.getField("score").getType().getTypeName());
        Assertions.assertEquals(Schema.TypeName.DOUBLE, schema.getField("price").getType().getTypeName());
        Assertions.assertEquals(Schema.TypeName.DECIMAL, schema.getField("amount").getType().getTypeName());
        Assertions.assertEquals(Schema.TypeName.BOOLEAN, schema.getField("flag").getType().getTypeName());
        Assertions.assertEquals(Schema.TypeName.STRING, schema.getField("attrs").getType().getTypeName());
        Assertions.assertEquals(Schema.TypeName.LOGICAL_TYPE, schema.getField("birth").getType().getTypeName());
        Assertions.assertEquals(Schema.TypeName.DATETIME, schema.getField("created").getType().getTypeName());
        Assertions.assertEquals(Schema.TypeName.BYTES, schema.getField("data").getType().getTypeName());
        Assertions.assertEquals(Schema.TypeName.ARRAY, schema.getField("tags").getType().getTypeName());
        Assertions.assertEquals(Schema.TypeName.STRING, schema.getField("tags").getType().getCollectionElementType().getTypeName());

        final Type type = StructSchemaUtil.convertTypeFromInformationSchema(structs, columns);
        Assertions.assertEquals(11, type.getStructFields().size());
        Assertions.assertEquals(Type.Code.INT64, type.getStructFields().get(type.getFieldIndex("id")).getType().getCode());
        Assertions.assertEquals(Type.Code.STRING, type.getStructFields().get(type.getFieldIndex("name")).getType().getCode());
        Assertions.assertEquals(Type.Code.FLOAT32, type.getStructFields().get(type.getFieldIndex("score")).getType().getCode());
        Assertions.assertEquals(Type.Code.FLOAT64, type.getStructFields().get(type.getFieldIndex("price")).getType().getCode());
        Assertions.assertEquals(Type.Code.NUMERIC, type.getStructFields().get(type.getFieldIndex("amount")).getType().getCode());
        Assertions.assertEquals(Type.Code.BOOL, type.getStructFields().get(type.getFieldIndex("flag")).getType().getCode());
        Assertions.assertEquals(Type.Code.JSON, type.getStructFields().get(type.getFieldIndex("attrs")).getType().getCode());
        Assertions.assertEquals(Type.Code.DATE, type.getStructFields().get(type.getFieldIndex("birth")).getType().getCode());
        Assertions.assertEquals(Type.Code.TIMESTAMP, type.getStructFields().get(type.getFieldIndex("created")).getType().getCode());
        Assertions.assertEquals(Type.Code.BYTES, type.getStructFields().get(type.getFieldIndex("data")).getType().getCode());
        Assertions.assertEquals(Type.Code.ARRAY, type.getStructFields().get(type.getFieldIndex("tags")).getType().getCode());
        Assertions.assertEquals(Type.Code.STRING, type.getStructFields().get(type.getFieldIndex("tags")).getType().getArrayElementType().getCode());
    }

    @Test
    public void testAdjustInsert() {
        final Type type = Type.struct(
                Type.StructField.of("str", Type.string()),
                Type.StructField.of("bool", Type.bool()),
                Type.StructField.of("int", Type.int64()),
                Type.StructField.of("dbl", Type.float64()),
                Type.StructField.of("date", Type.date()),
                Type.StructField.of("ts", Type.timestamp()),
                Type.StructField.of("nullint", Type.int64()));
        final Mutation insert = Mutation.newInsertBuilder("t")
                .set("str").to(123L)
                .set("bool").to("true")
                .set("int").to("42")
                .set("dbl").to(3L)
                .set("date").to("2020-01-02")
                .set("ts").to("2024-01-02T03:04:05Z")
                .set("nullint").to((String) null)
                .build();
        final Mutation adjusted = StructSchemaUtil.adjust(type, insert);
        Assertions.assertEquals(Mutation.Op.INSERT, adjusted.getOperation());
        final Map<String, Value> values = adjusted.asMap();
        Assertions.assertEquals(Value.string("123"), values.get("str"));
        Assertions.assertEquals(Value.bool(true), values.get("bool"));
        Assertions.assertEquals(Value.int64(42L), values.get("int"));
        Assertions.assertEquals(Value.float64(3D), values.get("dbl"));
        Assertions.assertEquals(Value.date(Date.parseDate("2020-01-02")), values.get("date"));
        Assertions.assertEquals(Value.timestamp(Timestamp.parseTimestamp("2024-01-02T03:04:05Z")), values.get("ts"));
        Assertions.assertTrue(values.get("nullint").isNull());
        Assertions.assertEquals(Type.Code.INT64, values.get("nullint").getType().getCode());
    }

    @Test
    public void testAdjustUpdateAndDelete() {
        final Type type = Type.struct(
                Type.StructField.of("str", Type.string()),
                Type.StructField.of("bool", Type.bool()),
                Type.StructField.of("int", Type.int64()),
                Type.StructField.of("dbl", Type.float64()),
                Type.StructField.of("date", Type.date()),
                Type.StructField.of("ts", Type.timestamp()));
        final Mutation update = Mutation.newUpdateBuilder("t")
                .set("str").to("ok")
                .set("bool").to(1L)
                .set("int").to(Value.date(Date.parseDate("1970-01-11")))
                .set("dbl").to(Value.float32(1.5F))
                .set("date").to(10L)
                .set("ts").to(123456789L)
                .build();
        final Mutation adjusted = StructSchemaUtil.adjust(type, update);
        Assertions.assertEquals(Mutation.Op.UPDATE, adjusted.getOperation());
        final Map<String, Value> values = adjusted.asMap();
        Assertions.assertEquals(Value.string("ok"), values.get("str"));
        Assertions.assertEquals(Value.bool(true), values.get("bool"));
        Assertions.assertEquals(Value.int64(10L), values.get("int"));
        Assertions.assertEquals(Value.float64(1.5D), values.get("dbl"));
        Assertions.assertEquals(Value.date(Date.parseDate("1970-01-11")), values.get("date"));
        Assertions.assertEquals(Value.timestamp(Timestamp.ofTimeMicroseconds(123456789L)), values.get("ts"));

        final Mutation delete = Mutation.delete("t", Key.of("k"));
        final Mutation adjustedDelete = StructSchemaUtil.adjust(type, delete);
        Assertions.assertEquals(Mutation.Op.DELETE, adjustedDelete.getOperation());
        Assertions.assertEquals("t", adjustedDelete.getTable());
    }

    @Test
    public void testValidate() {
        final Type type = Type.struct(
                Type.StructField.of("str", Type.string()),
                Type.StructField.of("int", Type.int64()));

        final Mutation validInsert = Mutation.newInsertBuilder("t")
                .set("str").to("a").set("int").to(1L).build();
        Assertions.assertTrue(StructSchemaUtil.validate(type, validInsert));

        final Mutation missingFieldInsert = Mutation.newInsertBuilder("t")
                .set("str").to("a").build();
        Assertions.assertFalse(StructSchemaUtil.validate(type, missingFieldInsert));

        final Mutation wrongTypeInsert = Mutation.newInsertBuilder("t")
                .set("str").to("a").set("int").to("b").build();
        Assertions.assertFalse(StructSchemaUtil.validate(type, wrongTypeInsert));

        final Mutation validUpdate = Mutation.newUpdateBuilder("t")
                .set("int").to(1L).build();
        Assertions.assertTrue(StructSchemaUtil.validate(type, validUpdate));

        final Mutation unknownColumnUpdate = Mutation.newUpdateBuilder("t")
                .set("unknown").to(1L).build();
        Assertions.assertFalse(StructSchemaUtil.validate(type, unknownColumnUpdate));

        Assertions.assertTrue(StructSchemaUtil.validate(type, Mutation.delete("t", Key.of("a"))));

        Assertions.assertThrows(IllegalArgumentException.class, () -> StructSchemaUtil.validate(null, validInsert));
        Assertions.assertThrows(IllegalArgumentException.class, () -> StructSchemaUtil.validate(Type.string(), validInsert));
    }

    @Test
    public void testCreateMutationWriteBuilder() {
        Assertions.assertEquals(Mutation.Op.INSERT_OR_UPDATE,
                StructSchemaUtil.createMutationWriteBuilder("t", null).set("a").to(1L).build().getOperation());
        Assertions.assertEquals(Mutation.Op.INSERT,
                StructSchemaUtil.createMutationWriteBuilder("t", "insert").set("a").to(1L).build().getOperation());
        Assertions.assertEquals(Mutation.Op.UPDATE,
                StructSchemaUtil.createMutationWriteBuilder("t", "UPDATE").set("a").to(1L).build().getOperation());
        Assertions.assertEquals(Mutation.Op.INSERT_OR_UPDATE,
                StructSchemaUtil.createMutationWriteBuilder("t", "INSERT_OR_UPDATE").set("a").to(1L).build().getOperation());
        Assertions.assertEquals(Mutation.Op.REPLACE,
                StructSchemaUtil.createMutationWriteBuilder("t", "replace").set("a").to(1L).build().getOperation());
        Assertions.assertEquals(Mutation.Op.INSERT_OR_UPDATE,
                StructSchemaUtil.createMutationWriteBuilder("t", "other").set("a").to(1L).build().getOperation());
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> StructSchemaUtil.createMutationWriteBuilder("t", "DELETE"));
    }

    @Test
    public void testGetKeys() {
        final Mutation delete = Mutation.delete("t", com.google.cloud.spanner.KeySet.newBuilder()
                .addKey(Key.of("a")).addKey(Key.of("b")).build());
        final List<String> keys = StructSchemaUtil.getKeys(delete);
        Assertions.assertEquals(2, keys.size());
        Assertions.assertTrue(StructSchemaUtil.getKeys(
                Mutation.newInsertBuilder("t").set("a").to(1L).build()).isEmpty());
    }

    @Test
    public void testConvertChangeRecordTypeCode() {
        Assertions.assertEquals("INT64", StructSchemaUtil.convertChangeRecordTypeCode("{\"code\":\"INT64\"}"));
        Assertions.assertEquals("STRING", StructSchemaUtil.convertChangeRecordTypeCode("{\"code\":\"STRING\"}"));
        Assertions.assertEquals("INT64", StructSchemaUtil.convertChangeRecordTypeCode("INT64"));
    }

    @Test
    public void testCreateDataChangeRecordSchemas() {
        final Schema rowSchema = StructSchemaUtil.createDataChangeRecordRowSchema();
        Assertions.assertEquals(13, rowSchema.getFieldCount());
        Assertions.assertTrue(rowSchema.hasField("partitionToken"));
        Assertions.assertTrue(rowSchema.hasField("commitTimestamp"));
        Assertions.assertTrue(rowSchema.hasField("rowType"));
        Assertions.assertTrue(rowSchema.hasField("mods"));
        Assertions.assertTrue(rowSchema.hasField("modType"));
        Assertions.assertTrue(rowSchema.hasField("metadata"));

        final org.apache.avro.Schema avroSchema = StructSchemaUtil.createDataChangeRecordAvroSchema();
        Assertions.assertEquals("DataChangeRecord", avroSchema.getName());
        Assertions.assertEquals(13, avroSchema.getFields().size());
        Assertions.assertNotNull(avroSchema.getField("mods"));
        Assertions.assertNotNull(avroSchema.getField("rowType"));
        Assertions.assertEquals(org.apache.avro.Schema.Type.ENUM,
                avroSchema.getField("modType").schema().getType());

        final Schema mutationSchema = StructSchemaUtil.createMutationSchema();
        Assertions.assertEquals(5, mutationSchema.getFieldCount());
        Assertions.assertTrue(mutationSchema.hasField("table"));
        Assertions.assertTrue(mutationSchema.hasField("op"));
        Assertions.assertTrue(mutationSchema.hasField("mutation"));
    }

    private DataChangeRecord createDataChangeRecord(final String table, final ModType modType,
                                                    final List<ColumnType> rowType, final List<Mod> mods,
                                                    final String recordSequence) {
        return new DataChangeRecord(
                "token",
                Timestamp.ofTimeMicroseconds(1704164645000000L),
                "tx",
                true,
                recordSequence,
                table,
                rowType,
                mods,
                modType,
                ValueCaptureType.OLD_AND_NEW_VALUES,
                1L,
                1L,
                "",
                false,
                null);
    }

    @Test
    public void testConvertToMutation() {
        final Type type = Type.struct(
                Type.StructField.of("id", Type.int64()),
                Type.StructField.of("name", Type.string()),
                Type.StructField.of("score", Type.float64()),
                Type.StructField.of("active", Type.bool()),
                Type.StructField.of("birth", Type.date()),
                Type.StructField.of("created", Type.timestamp()),
                Type.StructField.of("tags", Type.array(Type.string())));

        // INSERT with empty rowType: field types are resolved from the given Type
        final Mod insertMod = new Mod(
                "{\"id\":1}",
                null,
                "{\"name\":\"a\",\"score\":1.5,\"active\":true,\"birth\":\"2020-01-02\",\"created\":\"2024-01-02T03:04:05Z\",\"tags\":[\"x\",\"y\"]}");
        final DataChangeRecord insertRecord = createDataChangeRecord(
                "MyTable", ModType.INSERT, new ArrayList<>(), Arrays.asList(insertMod), "001");
        final List<Mutation> inserts = StructSchemaUtil.convertToMutation(type, insertRecord);
        Assertions.assertEquals(1, inserts.size());
        Assertions.assertEquals(Mutation.Op.INSERT, inserts.get(0).getOperation());
        Assertions.assertEquals("MyTable", inserts.get(0).getTable());
        final Map<String, Value> insertValues = inserts.get(0).asMap();
        Assertions.assertEquals(Value.int64(1L), insertValues.get("id"));
        Assertions.assertEquals(Value.string("a"), insertValues.get("name"));
        Assertions.assertEquals(Value.float64(1.5D), insertValues.get("score"));
        Assertions.assertEquals(Value.bool(true), insertValues.get("active"));
        Assertions.assertEquals(Value.date(Date.parseDate("2020-01-02")), insertValues.get("birth"));
        Assertions.assertEquals(Value.timestamp(Timestamp.parseTimestamp("2024-01-02T03:04:05Z")), insertValues.get("created"));
        Assertions.assertEquals(Value.stringArray(Arrays.asList("x", "y")), insertValues.get("tags"));

        // UPDATE with rowType: field types are resolved from column type codes
        final List<ColumnType> rowType = Arrays.asList(
                new ColumnType("id", new TypeCode("{\"code\":\"INT64\"}"), true, 1L),
                new ColumnType("name", new TypeCode("{\"code\":\"STRING\"}"), false, 2L));
        final Mod updateMod = new Mod("{\"id\":2}", null, "{\"name\":\"b\"}");
        final DataChangeRecord updateRecord = createDataChangeRecord(
                "MyTable", ModType.UPDATE, rowType, Arrays.asList(updateMod), "002");
        final List<Mutation> updates = StructSchemaUtil.convertToMutation(type, updateRecord, new HashMap<>(), false, true);
        Assertions.assertEquals(1, updates.size());
        Assertions.assertEquals(Mutation.Op.INSERT_OR_UPDATE, updates.get(0).getOperation());
        Assertions.assertEquals(Value.int64(2L), updates.get(0).asMap().get("id"));
        Assertions.assertEquals(Value.string("b"), updates.get(0).asMap().get("name"));

        // DELETE
        final Mod deleteMod = new Mod("{\"id\":5}", null, null);
        final DataChangeRecord deleteRecord = createDataChangeRecord(
                "MyTable", ModType.DELETE, new ArrayList<>(), Arrays.asList(deleteMod), "003");
        final List<Mutation> deletes = StructSchemaUtil.convertToMutation(type, deleteRecord);
        Assertions.assertEquals(1, deletes.size());
        Assertions.assertEquals(Mutation.Op.DELETE, deletes.get(0).getOperation());
        final List<Key> keys = new ArrayList<>();
        deletes.get(0).getKeySet().getKeys().forEach(keys::add);
        Assertions.assertEquals(Arrays.asList(Key.of(5L)), keys);

        // rename table
        final List<Mutation> renamed = StructSchemaUtil.convertToMutation(
                type, insertRecord, Map.of("MyTable", "OtherTable"), true, false);
        Assertions.assertEquals("OtherTable", renamed.get(0).getTable());
        Assertions.assertEquals(Mutation.Op.INSERT_OR_UPDATE, renamed.get(0).getOperation());

        // mutation group
        final MutationGroup group = StructSchemaUtil.convertToMutationGroup(
                Map.of("MyTable", type), Arrays.asList(insertRecord, deleteRecord));
        Assertions.assertEquals(Mutation.Op.INSERT, group.primary().getOperation());
        Assertions.assertEquals(1, group.attached().size());
    }

    @Test
    public void testAccumulateChangeRecordsForUnifiedMutations() {
        final Mutation insert = Mutation.newInsertBuilder("t1")
                .set("a").to(1L).set("b").to("x").build();
        final Mutation update = Mutation.newUpdateBuilder("t1")
                .set("b").to("y").build();
        final Mutation delete = Mutation.delete("t1", Key.of(1L));

        // insert followed by update accumulates into a single insert
        final List<UnifiedMutation> accumulated = StructSchemaUtil.accumulateChangeRecords(
                Arrays.asList(
                        UnifiedMutation.of(insert, "t1", 1000L, 0),
                        UnifiedMutation.of(update, "t1", 2000L, 1)),
                new HashMap<>(), false, false);
        Assertions.assertEquals(1, accumulated.size());
        Assertions.assertEquals(MutationOp.INSERT, accumulated.get(0).getOp());
        Assertions.assertEquals(2000L, accumulated.get(0).getCommitTimestampMicros());
        final Map<String, Value> values = accumulated.get(0).getSpannerMutation().asMap();
        Assertions.assertEquals(Value.int64(1L), values.get("a"));
        Assertions.assertEquals(Value.string("y"), values.get("b"));

        // insert followed by delete results in nothing
        final List<UnifiedMutation> inserted = StructSchemaUtil.accumulateChangeRecords(
                Arrays.asList(
                        UnifiedMutation.of(insert, "t1", 1000L, 0),
                        UnifiedMutation.of(delete, "t1", 2000L, 1)),
                new HashMap<>(), false, false);
        Assertions.assertTrue(inserted.isEmpty());

        // update followed by delete results in the delete
        final List<UnifiedMutation> deleted = StructSchemaUtil.accumulateChangeRecords(
                Arrays.asList(
                        UnifiedMutation.of(update, "t1", 1000L, 0),
                        UnifiedMutation.of(delete, "t1", 2000L, 1)),
                new HashMap<>(), false, false);
        Assertions.assertEquals(1, deleted.size());
        Assertions.assertEquals(MutationOp.DELETE, deleted.get(0).getOp());

        // single mutation without renames/upserts is passed through
        final List<UnifiedMutation> single = Arrays.asList(UnifiedMutation.of(insert, "t1", 1000L, 0));
        Assertions.assertSame(single, StructSchemaUtil.accumulateChangeRecords(single, new HashMap<>(), false, false));

        // rename tables
        final List<UnifiedMutation> renamed = StructSchemaUtil.accumulateChangeRecords(
                single, Map.of("t1", "t2"), false, false);
        Assertions.assertEquals(1, renamed.size());
        Assertions.assertEquals("t2", renamed.get(0).getTable());

        // applyUpsertForInsert
        final List<UnifiedMutation> upserted = StructSchemaUtil.accumulateChangeRecords(
                single, new HashMap<>(), true, false);
        Assertions.assertEquals(1, upserted.size());
        Assertions.assertEquals(Mutation.Op.INSERT_OR_UPDATE, upserted.get(0).getSpannerMutation().getOperation());

        // empty input
        Assertions.assertTrue(StructSchemaUtil.accumulateChangeRecords(
                new ArrayList<>(), new HashMap<>(), false, false).isEmpty());
    }

    private GenericRecord createChangeRecordAvro(final org.apache.avro.Schema schema,
                                                 final String tableName,
                                                 final String modType,
                                                 final List<GenericRecord> rowType,
                                                 final List<GenericRecord> mods,
                                                 final long commitTimestampMicros,
                                                 final String recordSequence) {
        final GenericData.Record record = new GenericData.Record(schema);
        record.put("partitionToken", "token");
        record.put("commitTimestamp", commitTimestampMicros);
        record.put("serverTransactionId", "tx");
        record.put("isLastRecordInTransactionInPartition", true);
        record.put("recordSequence", recordSequence);
        record.put("tableName", tableName);
        record.put("rowType", rowType);
        record.put("mods", mods);
        record.put("modType", modType);
        record.put("valueCaptureType", "NEW_ROW");
        record.put("numberOfRecordsInTransaction", 1L);
        record.put("numberOfPartitionsInTransaction", 1L);
        record.put("metadata", null);
        return record;
    }

    private GenericRecord createRowTypeAvro(final org.apache.avro.Schema rowTypeSchema,
                                            final String name, final String code,
                                            final boolean isPrimaryKey, final long ordinalPosition) {
        final GenericData.Record record = new GenericData.Record(rowTypeSchema);
        record.put("name", name);
        record.put("Type", code);
        record.put("isPrimaryKey", isPrimaryKey);
        record.put("ordinalPosition", ordinalPosition);
        return record;
    }

    private GenericRecord createModAvro(final org.apache.avro.Schema modSchema,
                                        final String keysJson, final String newValuesJson) {
        final GenericData.Record record = new GenericData.Record(modSchema);
        record.put("keysJson", keysJson);
        record.put("oldValuesJson", null);
        record.put("newValuesJson", newValuesJson);
        return record;
    }

    @Test
    public void testConvertChangeRecordToMutationsFromElement() {
        final org.apache.avro.Schema schema = StructSchemaUtil.createDataChangeRecordAvroSchema();
        final org.apache.avro.Schema rowTypeSchema = schema.getField("rowType").schema().getElementType();
        final org.apache.avro.Schema modSchema = schema.getField("mods").schema().getElementType();

        final List<GenericRecord> rowType = Arrays.asList(
                createRowTypeAvro(rowTypeSchema, "id", "INT64", true, 1L),
                createRowTypeAvro(rowTypeSchema, "name", "STRING", false, 2L));

        // INSERT
        final GenericRecord insertRecord = createChangeRecordAvro(
                schema, "tbl", "INSERT", rowType,
                Arrays.asList(createModAvro(modSchema, "{\"id\":1}", "{\"name\":\"a\"}")),
                1000L, "1");
        Assertions.assertEquals("tbl", StructSchemaUtil.getChangeRecordTableName(insertRecord));
        final MElement insertElement = MElement.of(insertRecord, 0L);
        Assertions.assertEquals(1000L, StructSchemaUtil.getChangeDataCommitTimestampMicros(insertElement));

        final List<KV<KV<String, String>, UnifiedMutation>> inserts =
                StructSchemaUtil.convertChangeRecordToMutations(insertElement);
        Assertions.assertEquals(1, inserts.size());
        Assertions.assertEquals("tbl", inserts.get(0).getKey().getKey());
        final UnifiedMutation insertMutation = inserts.get(0).getValue();
        Assertions.assertEquals(MutationOp.INSERT, insertMutation.getOp());
        Assertions.assertEquals(1000L, insertMutation.getCommitTimestampMicros());
        Assertions.assertEquals(1, insertMutation.getSequence());
        final Map<String, Value> insertValues = insertMutation.getSpannerMutation().asMap();
        Assertions.assertEquals(Value.int64(1L), insertValues.get("id"));
        Assertions.assertEquals(Value.string("a"), insertValues.get("name"));

        // DELETE
        final GenericRecord deleteRecord = createChangeRecordAvro(
                schema, "tbl", "DELETE", rowType,
                Arrays.asList(createModAvro(modSchema, "{\"id\":1}", null)),
                2000L, "2");
        final List<KV<KV<String, String>, UnifiedMutation>> deletes =
                StructSchemaUtil.convertChangeRecordToMutations(MElement.of(deleteRecord, 0L));
        Assertions.assertEquals(1, deletes.size());
        Assertions.assertEquals(MutationOp.DELETE, deletes.get(0).getValue().getOp());
    }

    @Test
    public void testCreateChangeRecordKey() {
        final org.apache.avro.Schema schema = StructSchemaUtil.createDataChangeRecordAvroSchema();
        final org.apache.avro.Schema rowTypeSchema = schema.getField("rowType").schema().getElementType();
        final org.apache.avro.Schema modSchema = schema.getField("mods").schema().getElementType();

        final List<GenericRecord> rowType = Arrays.asList(
                createRowTypeAvro(rowTypeSchema, "id", "INT64", true, 1L),
                createRowTypeAvro(rowTypeSchema, "name", "STRING", true, 2L));
        final GenericRecord changeRecord = createChangeRecordAvro(
                schema, "tbl", "INSERT", rowType,
                Arrays.asList(
                        createModAvro(modSchema, "{\"id\":1,\"name\":\"a\"}", "{}"),
                        createModAvro(modSchema, "{\"id\":2,\"name\":\"b\"}", "{}")),
                1000L, "1");

        final List<KV<Key, GenericRecord>> keyAndRecords = StructSchemaUtil.createChangeRecordKey(null, changeRecord);
        Assertions.assertEquals(2, keyAndRecords.size());
        Assertions.assertEquals(Key.of(1L, "a"), keyAndRecords.get(0).getKey());
        Assertions.assertEquals(Key.of(2L, "b"), keyAndRecords.get(1).getKey());
        for(final KV<Key, GenericRecord> keyAndRecord : keyAndRecords) {
            Assertions.assertEquals(1, ((List<GenericRecord>) keyAndRecord.getValue().get("mods")).size());
            Assertions.assertEquals("tbl", keyAndRecord.getValue().get("tableName").toString());
        }
    }

    @Test
    public void testAccumulateChangeRecordsForGenericRecords() {
        final org.apache.avro.Schema tableSchema = org.apache.avro.SchemaBuilder
                .record("Tbl").fields()
                .requiredLong("id")
                .optionalString("name")
                .endRecord();
        final org.apache.avro.Schema schema = StructSchemaUtil.createDataChangeRecordAvroSchema();
        final org.apache.avro.Schema rowTypeSchema = schema.getField("rowType").schema().getElementType();
        final org.apache.avro.Schema modSchema = schema.getField("mods").schema().getElementType();

        final GenericData.Record snapshot = new GenericData.Record(tableSchema);
        snapshot.put("id", 1L);
        snapshot.put("name", "old");

        // no change records: snapshot is returned as is
        Assertions.assertSame(snapshot, StructSchemaUtil.accumulateChangeRecords(tableSchema, snapshot, new ArrayList<>()));

        final List<GenericRecord> rowType = Arrays.asList(
                createRowTypeAvro(rowTypeSchema, "id", "INT64", true, 1L),
                createRowTypeAvro(rowTypeSchema, "name", "STRING", false, 2L));

        // INSERT without snapshot
        final GenericRecord insertRecord = createChangeRecordAvro(
                schema, "tbl", "INSERT", rowType,
                Arrays.asList(createModAvro(modSchema, "{\"id\":2}", "{\"name\":\"new\"}")),
                1000L, "1");
        final GenericRecord accumulated = StructSchemaUtil.accumulateChangeRecords(
                tableSchema, null, Arrays.asList(insertRecord));
        Assertions.assertEquals(2L, accumulated.get("id"));
        Assertions.assertEquals("new", accumulated.get("name").toString());

        // UPDATE on top of a snapshot
        final GenericRecord updateRecord = createChangeRecordAvro(
                schema, "tbl", "UPDATE", rowType,
                Arrays.asList(createModAvro(modSchema, "{\"id\":1}", "{\"name\":\"updated\"}")),
                2000L, "2");
        final GenericRecord updated = StructSchemaUtil.accumulateChangeRecords(
                tableSchema, snapshot, Arrays.asList(updateRecord));
        Assertions.assertEquals(1L, updated.get("id"));
        Assertions.assertEquals("updated", updated.get("name").toString());

        // last record DELETE: null is returned
        final GenericRecord deleteRecord = createChangeRecordAvro(
                schema, "tbl", "DELETE", rowType,
                Arrays.asList(createModAvro(modSchema, "{\"id\":1}", null)),
                3000L, "3");
        Assertions.assertNull(StructSchemaUtil.accumulateChangeRecords(
                tableSchema, snapshot, Arrays.asList(updateRecord, deleteRecord)));
    }

    private Struct createTestStruct() {
        final Struct grandchild = Struct.newBuilder()
                .set("gcstringField").to("gcstringValue")
                .build();
        final Struct child = Struct.newBuilder()
                .set("cstringField").to("cstringValue")
                .set("grandchild").to(grandchild)
                .set("grandchildren").toStructArray(grandchild.getType(), Arrays.asList(grandchild, grandchild))
                .set("grandchildrenNull").toStructArray(grandchild.getType(),null)
                .build();
        final Struct struct = Struct.newBuilder()
                .set("stringField").to("stringValue")
                .set("children").toStructArray(child.getType(), Arrays.asList(child, child))
                //.set("childrenNull").to((Struct)null)
                .build();

        return struct;
    }

}
