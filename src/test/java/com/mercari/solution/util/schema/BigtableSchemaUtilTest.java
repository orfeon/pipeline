package com.mercari.solution.util.schema;

import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Row;
import com.google.cloud.ByteArray;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutationFactory;
import com.google.cloud.bigtable.data.v2.models.DeleteCells;
import com.google.cloud.bigtable.data.v2.models.DeleteFamily;
import com.google.cloud.bigtable.data.v2.models.Range;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.SetCell;
import com.google.cloud.bigtable.data.v2.models.sql.ColumnMetadata;
import com.google.cloud.bigtable.data.v2.models.sql.ResultSet;
import com.google.cloud.bigtable.data.v2.models.sql.ResultSetMetadata;
import com.google.cloud.bigtable.data.v2.models.sql.SqlType;
import com.google.cloud.bigtable.data.v2.models.sql.Struct;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.ByteString;
import com.google.protobuf.ProtocolMessageEnum;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.DateTimeUtil;
import org.apache.avro.util.Utf8;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class BigtableSchemaUtilTest {

    public static class Parameters {
        public String text;
        public List<BigtableSchemaUtil.ColumnFamilyProperties> columns;
    }

    @Test
    public void testCreateSchema() {



        final String config = """
                {
                  "text": "okok",
                  "columns": [
                    {
                      "family": "a",
                      "qualifiers": [
                        { "name": "f1", "field": "field1", "type": "element", "format": "avro", "fields" :[
                          { "name": "childField1", "type": "string" }
                        ] }
                      ],
                      "format": "bytes"
                    }
                  ]
                }
                """;

        final Parameters parameters = new GsonBuilder().create().fromJson(config, Parameters.class);
        parameters.columns.forEach(BigtableSchemaUtil.ColumnFamilyProperties::setupSource);
        final Schema schema = BigtableSchemaUtil.createSchema(parameters.columns);
        Assertions.assertTrue(schema.hasField("field1"));
        Assertions.assertEquals(Schema.Type.element, schema.getField("field1").getFieldType().getType());
        final Schema childSchema = schema.getField("field1").getFieldType().getElementSchema();
        Assertions.assertTrue(childSchema.hasField("childField1"));
    }

    @Test
    public void testByteStringBytes() {
        ByteString byteString = BigtableSchemaUtil.toByteString("abc");
        Object value = BigtableSchemaUtil.toPrimitiveValueFromBytes(Schema.FieldType.STRING, byteString);
        Assertions.assertEquals("abc", value);
    }

    private static List<BigtableSchemaUtil.ColumnFamilyProperties> parseFamilies(final String json) {
        return new GsonBuilder().create()
                .fromJson(json, new TypeToken<List<BigtableSchemaUtil.ColumnFamilyProperties>>(){}.getType());
    }

    @Test
    public void testToByteStringObject() {
        Assertions.assertEquals(ByteString.EMPTY, BigtableSchemaUtil.toByteString(null));
        Assertions.assertEquals("abc", BigtableSchemaUtil.toByteString("abc").toStringUtf8());
        Assertions.assertTrue(Bytes.toBoolean(BigtableSchemaUtil.toByteString(true).toByteArray()));
        Assertions.assertArrayEquals(new byte[]{1,2,3}, BigtableSchemaUtil.toByteString(new byte[]{1,2,3}).toByteArray());
        Assertions.assertArrayEquals(new byte[]{1,2,3}, BigtableSchemaUtil.toByteString(ByteBuffer.wrap(new byte[]{1,2,3})).toByteArray());
        Assertions.assertArrayEquals(new byte[]{1,2,3}, BigtableSchemaUtil.toByteString(ByteString.copyFrom(new byte[]{1,2,3})).toByteArray());
        Assertions.assertArrayEquals(new byte[]{1,2,3}, BigtableSchemaUtil.toByteString(ByteArray.copyFrom(new byte[]{1,2,3})).toByteArray());
        Assertions.assertEquals(0, new BigDecimal("1.23").compareTo(Bytes.toBigDecimal(BigtableSchemaUtil.toByteString(new BigDecimal("1.23")).toByteArray())));
        Assertions.assertEquals((short) 3, Bytes.toShort(BigtableSchemaUtil.toByteString((short) 3).toByteArray()));
        Assertions.assertEquals(42, Bytes.toInt(BigtableSchemaUtil.toByteString(42).toByteArray()));
        Assertions.assertEquals(42L, Bytes.toLong(BigtableSchemaUtil.toByteString(42L).toByteArray()));
        Assertions.assertEquals(1.5F, Bytes.toFloat(BigtableSchemaUtil.toByteString(1.5F).toByteArray()));
        Assertions.assertEquals(2.5D, Bytes.toDouble(BigtableSchemaUtil.toByteString(2.5D).toByteArray()));
        Assertions.assertThrows(IllegalArgumentException.class, () -> BigtableSchemaUtil.toByteString(LocalDate.parse("2024-01-01")));
    }

    @Test
    public void testToByteStringReadOnlyByteBuffer() {
        // read-only ByteBuffers (e.g. ByteString.asReadOnlyByteBuffer()) have no accessible backing array
        final ByteBuffer readOnly = ByteString.copyFromUtf8("xyz").asReadOnlyByteBuffer();
        Assertions.assertEquals("xyz", BigtableSchemaUtil.toByteString(readOnly).toStringUtf8());

        final ByteBuffer readOnlyForText = ByteString.copyFromUtf8("xyz").asReadOnlyByteBuffer();
        Assertions.assertEquals(
                Base64.getEncoder().encodeToString("xyz".getBytes(StandardCharsets.UTF_8)),
                BigtableSchemaUtil.toByteStringText(readOnlyForText).toStringUtf8());

        final ByteBuffer readOnlyForBytes = ByteString.copyFromUtf8("xyz").asReadOnlyByteBuffer();
        Assertions.assertEquals("xyz", BigtableSchemaUtil.toByteStringBytes(readOnlyForBytes, null).toStringUtf8());
    }

    @Test
    public void testToByteStringText() {
        Assertions.assertEquals(ByteString.EMPTY, BigtableSchemaUtil.toByteStringText(null));
        Assertions.assertEquals("abc", BigtableSchemaUtil.toByteStringText("abc").toStringUtf8());
        Assertions.assertEquals("u", BigtableSchemaUtil.toByteStringText(new Utf8("u")).toStringUtf8());
        Assertions.assertEquals(
                Base64.getEncoder().encodeToString(new byte[]{1,2}),
                BigtableSchemaUtil.toByteStringText(new byte[]{1,2}).toStringUtf8());
        Assertions.assertEquals(
                Base64.getEncoder().encodeToString(new byte[]{1,2}),
                BigtableSchemaUtil.toByteStringText(ByteString.copyFrom(new byte[]{1,2})).toStringUtf8());
        Assertions.assertEquals(
                Base64.getEncoder().encodeToString(new byte[]{1,2}),
                BigtableSchemaUtil.toByteStringText(ByteArray.copyFrom(new byte[]{1,2})).toStringUtf8());
        Assertions.assertEquals("123", BigtableSchemaUtil.toByteStringText(123).toStringUtf8());
    }

    @Test
    public void testToByteStringBytesWithDynamicType() {
        Assertions.assertNull(BigtableSchemaUtil.toByteStringBytes(null, null));
        Assertions.assertEquals("u", Bytes.toString(BigtableSchemaUtil.toByteStringBytes(new Utf8("u"), null).toByteArray()));
        Assertions.assertEquals(123L, Bytes.toLong(BigtableSchemaUtil.toByteStringBytes("123", Schema.Type.int64).toByteArray()));
        Assertions.assertEquals(12, Bytes.toInt(BigtableSchemaUtil.toByteStringBytes(12L, Schema.Type.int32).toByteArray()));
        Assertions.assertEquals("42", Bytes.toString(BigtableSchemaUtil.toByteStringBytes(42, Schema.Type.string).toByteArray()));
    }

    @Test
    public void testConvertDynamicFieldValue() {
        // null dynamic type passes value through, null value stays null
        Assertions.assertEquals("v", BigtableSchemaUtil.convertDynamicFieldValue(null, "v"));
        Assertions.assertNull(BigtableSchemaUtil.convertDynamicFieldValue(Schema.Type.int64, null));

        Assertions.assertEquals("123", BigtableSchemaUtil.convertDynamicFieldValue(Schema.Type.string, 123));
        Assertions.assertEquals("j", BigtableSchemaUtil.convertDynamicFieldValue(Schema.Type.json, "j"));

        Assertions.assertEquals(Boolean.TRUE, BigtableSchemaUtil.convertDynamicFieldValue(Schema.Type.bool, "true"));
        Assertions.assertEquals(Boolean.TRUE, BigtableSchemaUtil.convertDynamicFieldValue(Schema.Type.bool, new Utf8("true")));
        Assertions.assertEquals(Boolean.TRUE, BigtableSchemaUtil.convertDynamicFieldValue(Schema.Type.bool, 1L));
        Assertions.assertEquals(Boolean.FALSE, BigtableSchemaUtil.convertDynamicFieldValue(Schema.Type.bool, 0.0D));
        Assertions.assertNull(BigtableSchemaUtil.convertDynamicFieldValue(Schema.Type.bool, List.of()));

        Assertions.assertEquals(Short.valueOf((short) 3), BigtableSchemaUtil.convertDynamicFieldValue(Schema.Type.int16, "3"));
        Assertions.assertEquals(Short.valueOf((short) 4), BigtableSchemaUtil.convertDynamicFieldValue(Schema.Type.int16, new Utf8("4")));
        Assertions.assertEquals(Short.valueOf((short) 5), BigtableSchemaUtil.convertDynamicFieldValue(Schema.Type.int16, 5L));
        Assertions.assertEquals(Short.valueOf((short) 1), BigtableSchemaUtil.convertDynamicFieldValue(Schema.Type.int16, Boolean.TRUE));

        Assertions.assertEquals(Integer.valueOf(7), BigtableSchemaUtil.convertDynamicFieldValue(Schema.Type.int32, "7"));
        Assertions.assertEquals(Integer.valueOf(1), BigtableSchemaUtil.convertDynamicFieldValue(Schema.Type.int32, Boolean.TRUE));
        Assertions.assertEquals(Integer.valueOf(8), BigtableSchemaUtil.convertDynamicFieldValue(Schema.Type.int32, 8.9D));

        Assertions.assertEquals(Long.valueOf(8L), BigtableSchemaUtil.convertDynamicFieldValue(Schema.Type.int64, "8"));
        Assertions.assertEquals(Long.valueOf(1L), BigtableSchemaUtil.convertDynamicFieldValue(Schema.Type.int64, Boolean.TRUE));
        Assertions.assertEquals(Long.valueOf(9L), BigtableSchemaUtil.convertDynamicFieldValue(Schema.Type.int64, 9));

        Assertions.assertEquals(Float.valueOf(1.5F), BigtableSchemaUtil.convertDynamicFieldValue(Schema.Type.float32, "1.5"));
        Assertions.assertEquals(Float.valueOf(1F), BigtableSchemaUtil.convertDynamicFieldValue(Schema.Type.float32, Boolean.TRUE));

        Assertions.assertEquals(Double.valueOf(2.5D), BigtableSchemaUtil.convertDynamicFieldValue(Schema.Type.float64, "2.5"));
        Assertions.assertEquals(Double.valueOf(0D), BigtableSchemaUtil.convertDynamicFieldValue(Schema.Type.float64, Boolean.FALSE));

        Assertions.assertEquals(
                Integer.valueOf((int) LocalDate.parse("2024-03-01").toEpochDay()),
                BigtableSchemaUtil.convertDynamicFieldValue(Schema.Type.date, "2024-03-01"));
        Assertions.assertEquals(Integer.valueOf(100), BigtableSchemaUtil.convertDynamicFieldValue(Schema.Type.date, 100L));

        // TIME is micros-of-day
        Assertions.assertEquals(
                Long.valueOf(((12 * 3600 + 34 * 60 + 56) * 1_000_000L)),
                BigtableSchemaUtil.convertDynamicFieldValue(Schema.Type.time, "12:34:56"));

        // timestamps are epoch micros
        Assertions.assertEquals(
                Long.valueOf(1704067200000000L),
                BigtableSchemaUtil.convertDynamicFieldValue(Schema.Type.timestamp, "2024-01-01T00:00:00Z"));
        Assertions.assertEquals(Long.valueOf(9L), BigtableSchemaUtil.convertDynamicFieldValue(Schema.Type.timestamp, 9));

        Assertions.assertThrows(RuntimeException.class,
                () -> BigtableSchemaUtil.convertDynamicFieldValue(Schema.Type.bytes, "x"));
    }

    private static Object hadoopRoundTrip(final Object value, final Schema.FieldType fieldType) {
        final ByteString byteString = BigtableSchemaUtil.toByteStringHadoop(value);
        return BigtableSchemaUtil.toPrimitiveValueFromWritable(fieldType, byteString);
    }

    @Test
    public void testHadoopFormatScalarRoundTrip() {
        Assertions.assertEquals(Boolean.TRUE, hadoopRoundTrip(true, Schema.FieldType.BOOLEAN));
        Assertions.assertEquals("hello", hadoopRoundTrip("hello", Schema.FieldType.STRING));
        Assertions.assertEquals(Short.valueOf((short) 3), hadoopRoundTrip((short) 3, Schema.FieldType.type(Schema.Type.int16)));
        Assertions.assertEquals(Integer.valueOf(7), hadoopRoundTrip(7, Schema.FieldType.INT32));
        Assertions.assertEquals(Long.valueOf(7L), hadoopRoundTrip(7L, Schema.FieldType.INT64));
        Assertions.assertEquals(Float.valueOf(1.5F), hadoopRoundTrip(1.5F, Schema.FieldType.FLOAT32));
        Assertions.assertEquals(Double.valueOf(2.5D), hadoopRoundTrip(2.5D, Schema.FieldType.FLOAT64));
        Assertions.assertArrayEquals(new byte[]{1,2,3}, (byte[]) hadoopRoundTrip(new byte[]{1,2,3}, Schema.FieldType.BYTES));
        Assertions.assertArrayEquals(new byte[]{4,5}, (byte[]) hadoopRoundTrip(ByteString.copyFrom(new byte[]{4,5}), Schema.FieldType.BYTES));
        Assertions.assertArrayEquals(new byte[]{4,5}, (byte[]) hadoopRoundTrip(ByteBuffer.wrap(new byte[]{4,5}), Schema.FieldType.BYTES));
        Assertions.assertArrayEquals(new byte[]{4,5}, (byte[]) hadoopRoundTrip(ByteArray.copyFrom(new byte[]{4,5}), Schema.FieldType.BYTES));

        Assertions.assertNull(BigtableSchemaUtil.toPrimitiveValueFromWritable(Schema.FieldType.STRING, (ByteString) null));
        Assertions.assertNull(BigtableSchemaUtil.toPrimitiveValueFromWritable(null, new byte[]{1}));
        Assertions.assertNull(BigtableSchemaUtil.toPrimitiveValueFromWritable(Schema.FieldType.STRING, (byte[]) null));
    }

    @Test
    public void testHadoopFormatArrayRoundTrip() {
        Assertions.assertEquals(List.of("a", "b"), hadoopRoundTrip(List.of("a", "b"), Schema.FieldType.array(Schema.FieldType.STRING)));
        Assertions.assertEquals(List.of(1, 2), hadoopRoundTrip(List.of(1, 2), Schema.FieldType.array(Schema.FieldType.INT32)));
        Assertions.assertEquals(List.of(1L, 2L), hadoopRoundTrip(List.of(1L, 2L), Schema.FieldType.array(Schema.FieldType.INT64)));
        Assertions.assertEquals(List.of(1.5F), hadoopRoundTrip(List.of(1.5F), Schema.FieldType.array(Schema.FieldType.FLOAT32)));
        Assertions.assertEquals(List.of(2.5D), hadoopRoundTrip(List.of(2.5D), Schema.FieldType.array(Schema.FieldType.FLOAT64)));
        Assertions.assertEquals(List.of(true, false), hadoopRoundTrip(List.of(true, false), Schema.FieldType.array(Schema.FieldType.BOOLEAN)));
        Assertions.assertEquals(List.of((short) 1, (short) 2), hadoopRoundTrip(List.of((short) 1, (short) 2), Schema.FieldType.array(Schema.FieldType.type(Schema.Type.int16))));
        Assertions.assertEquals(List.of(), hadoopRoundTrip(List.of(), Schema.FieldType.array(Schema.FieldType.STRING)));
    }

    @Test
    public void testHadoopFormatMapRoundTrip() {
        final Map<String, Object> input = Map.of(
                "a", 1,
                "b", "x",
                "c", List.of("p", "q"),
                "d", Map.of("e", 5L));
        @SuppressWarnings("unchecked")
        final Map<String, Object> output = (Map<String, Object>) hadoopRoundTrip(input, Schema.FieldType.map(Schema.FieldType.STRING));
        Assertions.assertEquals(Integer.valueOf(1), output.get("a"));
        Assertions.assertEquals("x", output.get("b"));
        Assertions.assertEquals(List.of("p", "q"), output.get("c"));
        Assertions.assertEquals(Map.of("e", 5L), output.get("d"));
    }

    @Test
    public void testToMutationsSetCell() {
        final String config = """
                [
                  {
                    "family": "cf1",
                    "format": "bytes",
                    "timestampType": "fixed",
                    "timestampValue": "2024-01-01T00:00:00Z",
                    "qualifiers": [
                      { "name": "q1", "field": "stringField" },
                      { "name": "q2", "field": "longField" },
                      { "name": "q3", "field": "textField", "format": "text" },
                      { "name": "q4", "field": "dynamicField", "type": "int64" },
                      { "name": "q5", "field": "avroField", "format": "avro" },
                      { "name": "q6", "field": "stringField", "timestampType": "zero" },
                      { "name": "q7", "field": "stringField", "timestampType": "event" },
                      { "name": "q8", "field": "stringField", "timestampType": "field", "timestampField": "eventTimeField" },
                      { "name": "q9", "field": "missingField" }
                    ]
                  }
                ]
                """;

        final List<BigtableSchemaUtil.ColumnFamilyProperties> families = parseFamilies(config);
        for (int i = 0; i < families.size(); i++) {
            Assertions.assertTrue(families.get(i).validate(i).isEmpty());
        }
        for (final BigtableSchemaUtil.ColumnFamilyProperties family : families) {
            family.setDefaults(BigtableSchemaUtil.Format.bytes, null, null, null, null, (List<Schema.Field>) null);
            family.setupSink();
        }

        final Map<String, Object> primitiveValues = new HashMap<>();
        primitiveValues.put("stringField", "hello");
        primitiveValues.put("longField", 42L);
        primitiveValues.put("textField", "txt");
        primitiveValues.put("dynamicField", "123");
        primitiveValues.put("avroField", "avroValue");
        primitiveValues.put("eventTimeField", 1704067200123456L);
        final Map<String, Object> standardValues = new HashMap<>(primitiveValues);

        final org.joda.time.Instant eventTime = org.joda.time.Instant.parse("2024-01-01T00:00:00Z");
        final List<Mutation> mutations = BigtableSchemaUtil.toMutations(families, primitiveValues, standardValues, eventTime);

        Assertions.assertEquals(8, mutations.size()); // q9 has no value and is skipped
        for (final Mutation mutation : mutations) {
            Assertions.assertTrue(mutation.hasSetCell());
            Assertions.assertEquals("cf1", mutation.getSetCell().getFamilyName());
        }

        final long fixedMicros = 1704067200000000L;

        final Mutation.SetCell setCell1 = mutations.get(0).getSetCell();
        Assertions.assertEquals("q1", setCell1.getColumnQualifier().toStringUtf8());
        Assertions.assertEquals("hello", Bytes.toString(setCell1.getValue().toByteArray()));
        Assertions.assertEquals(fixedMicros, setCell1.getTimestampMicros());

        final Mutation.SetCell setCell2 = mutations.get(1).getSetCell();
        Assertions.assertEquals(42L, Bytes.toLong(setCell2.getValue().toByteArray()));

        final Mutation.SetCell setCell3 = mutations.get(2).getSetCell();
        Assertions.assertEquals("txt", setCell3.getValue().toStringUtf8());

        final Mutation.SetCell setCell4 = mutations.get(3).getSetCell();
        Assertions.assertEquals(123L, Bytes.toLong(setCell4.getValue().toByteArray()));

        final Mutation.SetCell setCell5 = mutations.get(4).getSetCell();
        final Object avroDecoded = BigtableSchemaUtil.toPrimitiveValue(
                BigtableSchemaUtil.Format.avro, Schema.FieldType.STRING, setCell5.getValue());
        Assertions.assertEquals("avroValue", avroDecoded.toString());

        Assertions.assertEquals(0L, mutations.get(5).getSetCell().getTimestampMicros());
        Assertions.assertEquals(eventTime.getMillis() * 1000L, mutations.get(6).getSetCell().getTimestampMicros());
        Assertions.assertEquals(1704067200123000L, mutations.get(7).getSetCell().getTimestampMicros());
    }

    @Test
    public void testToMutationsOtherOps() {
        final String config = """
                [
                  { "family": "cf2", "mutationOp": "DELETE_FROM_FAMILY" },
                  {
                    "family": "cf3",
                    "format": "bytes",
                    "qualifiers": [
                      { "name": "d1", "field": "f1", "mutationOp": "DELETE_FROM_COLUMN" },
                      { "name": "a1", "field": "longField", "mutationOp": "ADD_TO_CELL", "timestampType": "zero" },
                      { "name": "m1", "field": "doubleField", "mutationOp": "MERGE_TO_CELL" },
                      { "name": "t1", "field": "stringField", "mutationOp": "${op}" }
                    ]
                  }
                ]
                """;

        final List<BigtableSchemaUtil.ColumnFamilyProperties> families = parseFamilies(config);
        for (int i = 0; i < families.size(); i++) {
            Assertions.assertTrue(families.get(i).validate(i).isEmpty());
        }
        for (final BigtableSchemaUtil.ColumnFamilyProperties family : families) {
            family.setDefaults(BigtableSchemaUtil.Format.bytes, null, null, null, null, (List<Schema.Field>) null);
            family.setupSink();
        }

        final Map<String, Object> primitiveValues = new HashMap<>();
        primitiveValues.put("longField", 42L);
        primitiveValues.put("doubleField", 3.5D);
        primitiveValues.put("stringField", "hello");
        final Map<String, Object> standardValues = new HashMap<>(primitiveValues);
        standardValues.put("op", "SET_CELL");

        final org.joda.time.Instant eventTime = org.joda.time.Instant.parse("2024-01-01T00:00:00Z");
        final List<Mutation> mutations = BigtableSchemaUtil.toMutations(families, primitiveValues, standardValues, eventTime);

        Assertions.assertEquals(5, mutations.size());

        Assertions.assertTrue(mutations.get(0).hasDeleteFromFamily());
        Assertions.assertEquals("cf2", mutations.get(0).getDeleteFromFamily().getFamilyName());

        Assertions.assertTrue(mutations.get(1).hasDeleteFromColumn());
        Assertions.assertEquals("cf3", mutations.get(1).getDeleteFromColumn().getFamilyName());
        Assertions.assertEquals("d1", mutations.get(1).getDeleteFromColumn().getColumnQualifier().toStringUtf8());

        Assertions.assertTrue(mutations.get(2).hasAddToCell());
        final Mutation.AddToCell addToCell = mutations.get(2).getAddToCell();
        Assertions.assertEquals("cf3", addToCell.getFamilyName());
        Assertions.assertEquals("a1", addToCell.getColumnQualifier().getBytesValue().toStringUtf8());
        Assertions.assertEquals(42L, addToCell.getInput().getIntValue());
        Assertions.assertEquals(0L, addToCell.getTimestamp().getTimestampValue().getSeconds());

        Assertions.assertTrue(mutations.get(3).hasMergeToCell());
        final Mutation.MergeToCell mergeToCell = mutations.get(3).getMergeToCell();
        Assertions.assertEquals("m1", mergeToCell.getColumnQualifier().getBytesValue().toStringUtf8());
        Assertions.assertEquals(3.5D, mergeToCell.getInput().getFloatValue());

        // template mutationOp resolved from standard values
        Assertions.assertTrue(mutations.get(4).hasSetCell());
        final Mutation.SetCell setCell = mutations.get(4).getSetCell();
        Assertions.assertEquals("t1", setCell.getColumnQualifier().toStringUtf8());
        Assertions.assertEquals("hello", Bytes.toString(setCell.getValue().toByteArray()));
        Assertions.assertEquals(-1L, setCell.getTimestampMicros());
    }

    private static List<BigtableSchemaUtil.ColumnFamilyProperties> createSourceFamilies() {
        final String config = """
                [
                  {
                    "family": "cf1",
                    "format": "bytes",
                    "qualifiers": [
                      { "name": "q1", "field": "stringField", "type": "string" },
                      { "name": "q2", "field": "longField", "type": "int64" }
                    ]
                  },
                  {
                    "family": "cf2",
                    "format": "text",
                    "cellType": "all",
                    "qualifiers": [
                      { "name": "q3", "field": "allField", "type": "string" }
                    ]
                  },
                  {
                    "family": "cf3",
                    "format": "bytes",
                    "cellType": "first",
                    "qualifiers": [
                      { "name": "q4", "field": "firstField", "type": "string" }
                    ]
                  }
                ]
                """;
        final List<BigtableSchemaUtil.ColumnFamilyProperties> families = parseFamilies(config);
        for (final BigtableSchemaUtil.ColumnFamilyProperties family : families) {
            family.setDefaults(BigtableSchemaUtil.Format.bytes, (BigtableSchemaUtil.CellType) null);
            family.setupSource();
        }
        return families;
    }

    private static Row createProtoRow() {
        return Row.newBuilder()
                .setKey(ByteString.copyFromUtf8("rowKey1"))
                .addFamilies(Family.newBuilder()
                        .setName("cf1")
                        .addColumns(Column.newBuilder()
                                .setQualifier(ByteString.copyFromUtf8("q1"))
                                .addCells(Cell.newBuilder()
                                        .setTimestampMicros(2000L)
                                        .setValue(ByteString.copyFrom(Bytes.toBytes("hello")))))
                        .addColumns(Column.newBuilder()
                                .setQualifier(ByteString.copyFromUtf8("q2"))
                                .addCells(Cell.newBuilder()
                                        .setTimestampMicros(1000L)
                                        .setValue(ByteString.copyFrom(Bytes.toBytes(42L))))))
                .addFamilies(Family.newBuilder()
                        .setName("cf2")
                        .addColumns(Column.newBuilder()
                                .setQualifier(ByteString.copyFromUtf8("q3"))
                                // Bigtable returns cells in descending timestamp order (newest first)
                                .addCells(Cell.newBuilder()
                                        .setTimestampMicros(2000L)
                                        .setValue(ByteString.copyFromUtf8("new")))
                                .addCells(Cell.newBuilder()
                                        .setTimestampMicros(1000L)
                                        .setValue(ByteString.copyFromUtf8("old")))))
                .addFamilies(Family.newBuilder()
                        .setName("cf3")
                        .addColumns(Column.newBuilder()
                                .setQualifier(ByteString.copyFromUtf8("q4"))
                                .addCells(Cell.newBuilder()
                                        .setTimestampMicros(2000L)
                                        .setValue(ByteString.copyFrom(Bytes.toBytes("new"))))
                                .addCells(Cell.newBuilder()
                                        .setTimestampMicros(1000L)
                                        .setValue(ByteString.copyFrom(Bytes.toBytes("old"))))))
                .addFamilies(Family.newBuilder()
                        .setName("unknownFamily")
                        .addColumns(Column.newBuilder()
                                .setQualifier(ByteString.copyFromUtf8("qx"))
                                .addCells(Cell.newBuilder()
                                        .setTimestampMicros(9000L)
                                        .setValue(ByteString.copyFromUtf8("ignored")))))
                .build();
    }

    @Test
    public void testToPrimitiveValuesFromProtoRow() {
        final List<BigtableSchemaUtil.ColumnFamilyProperties> families = createSourceFamilies();
        final Map<String, BigtableSchemaUtil.ColumnFamilyProperties> familyMap = BigtableSchemaUtil.toMap(families);
        Assertions.assertEquals(3, familyMap.size());
        Assertions.assertTrue(BigtableSchemaUtil.toMap(null).isEmpty());

        final Row row = createProtoRow();
        final Map<String, Object> primitiveValues = BigtableSchemaUtil.toPrimitiveValues(row, familyMap);

        Assertions.assertEquals("hello", primitiveValues.get("stringField"));
        Assertions.assertEquals(42L, primitiveValues.get("longField"));
        // cellType all keeps every cell value
        Assertions.assertEquals(List.of("new", "old"), primitiveValues.get("allField"));
        // cells are ordered newest first, so cellType first refers to the oldest (last in list)
        Assertions.assertEquals("old", primitiveValues.get("firstField"));
        Assertions.assertFalse(primitiveValues.containsKey("qx"));
    }

    @Test
    public void testGetRowMinMaxTimestamps() {
        final KV<Long, Long> minMax = BigtableSchemaUtil.getRowMinMaxTimestamps(createProtoRow());
        Assertions.assertEquals(1000L, minMax.getKey());
        Assertions.assertEquals(9000L, minMax.getValue());
    }

    @Test
    public void testConvertModelsRow() {
        final String config = """
                [
                  {
                    "family": "cf1",
                    "format": "bytes",
                    "qualifiers": [
                      { "name": "q1", "field": "stringField", "type": "string" },
                      { "name": "q2", "field": "longField", "type": "int64" }
                    ]
                  }
                ]
                """;
        final List<BigtableSchemaUtil.ColumnFamilyProperties> families = parseFamilies(config);
        for (final BigtableSchemaUtil.ColumnFamilyProperties family : families) {
            family.setDefaults(BigtableSchemaUtil.Format.bytes, (BigtableSchemaUtil.CellType) null);
            family.setupSource();
        }

        final com.google.cloud.bigtable.data.v2.models.Row row = com.google.cloud.bigtable.data.v2.models.Row.create(
                ByteString.copyFromUtf8("rowKey1"),
                List.of(
                        RowCell.create("cf1", ByteString.copyFromUtf8("q1"), 3000L, List.of(), ByteString.copyFrom(Bytes.toBytes("hello"))),
                        RowCell.create("cf1", ByteString.copyFromUtf8("q2"), 3000L, List.of(), ByteString.copyFrom(Bytes.toBytes(42L)))));

        final org.joda.time.Instant timestamp = org.joda.time.Instant.parse("2024-01-01T00:00:00Z");
        final MElement element = BigtableSchemaUtil.convert(row, BigtableSchemaUtil.toMap(families), timestamp);
        Assertions.assertEquals("hello", element.getPrimitiveValue("stringField"));
        Assertions.assertEquals(42L, element.getPrimitiveValue("longField"));
    }

    @Test
    public void testConvertChangeStreamMutation() {
        final java.time.Instant commitTime = java.time.Instant.parse("2024-01-01T00:00:00.123456Z");
        final java.time.Instant lowWatermark = java.time.Instant.parse("2024-01-01T00:00:01Z");
        final ChangeStreamMutation mutation = ChangeStreamMutationFactory.createUserMutation(
                ByteString.copyFromUtf8("rk"), "cluster1", commitTime, 7,
                "token1", lowWatermark,
                "cf", ByteString.copyFromUtf8("q"), 1000L, ByteString.copyFromUtf8("v"),
                "cfDel",
                "cfCells", ByteString.copyFromUtf8("qc"), 0L, 2000L);

        final org.joda.time.Instant timestamp = org.joda.time.Instant.parse("2024-01-01T00:00:00Z");
        final MElement element = BigtableSchemaUtil.convert(mutation, timestamp);

        Assertions.assertEquals(ByteBuffer.wrap("rk".getBytes(StandardCharsets.UTF_8)), element.getPrimitiveValue("rowKey"));
        Assertions.assertEquals(1704067200123456L, element.getPrimitiveValue("commitTimestamp"));
        Assertions.assertEquals(7, element.getPrimitiveValue("tieBreaker"));
        Assertions.assertEquals("cluster1", element.getPrimitiveValue("sourceCluster"));
        Assertions.assertEquals(1704067201000000L, element.getPrimitiveValue("estimatedLowWatermarkTime"));

        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> entries = (List<Map<String, Object>>) element.getPrimitiveValue("entries");
        Assertions.assertEquals(3, entries.size());

        final Map<String, Object> setCellEntry = entries.get(0);
        Assertions.assertEquals("cf", setCellEntry.get("familyName"));
        Assertions.assertEquals(ByteBuffer.wrap("q".getBytes(StandardCharsets.UTF_8)), setCellEntry.get("qualifier"));
        Assertions.assertEquals(ByteBuffer.wrap("v".getBytes(StandardCharsets.UTF_8)), setCellEntry.get("value"));
        Assertions.assertEquals(1000L, setCellEntry.get("timestamp"));
        Assertions.assertEquals(BigtableSchemaUtil.ModType.SET_CELL.getId(), setCellEntry.get("modType"));

        final Map<String, Object> deleteFamilyEntry = entries.get(1);
        Assertions.assertEquals("cfDel", deleteFamilyEntry.get("familyName"));
        Assertions.assertEquals(BigtableSchemaUtil.ModType.DELETE_FAMILY.getId(), deleteFamilyEntry.get("modType"));

        final Map<String, Object> deleteCellsEntry = entries.get(2);
        Assertions.assertEquals("cfCells", deleteCellsEntry.get("familyName"));
        Assertions.assertEquals(ByteBuffer.wrap("qc".getBytes(StandardCharsets.UTF_8)), deleteCellsEntry.get("qualifier"));
        Assertions.assertEquals(BigtableSchemaUtil.ModType.DELETE_CELLS.getId(), deleteCellsEntry.get("modType"));
    }

    @Test
    public void testGetModType() {
        Assertions.assertEquals(
                BigtableSchemaUtil.ModType.SET_CELL,
                BigtableSchemaUtil.getModType(SetCell.create("cf", ByteString.copyFromUtf8("q"), 1L, ByteString.copyFromUtf8("v"))));
        Assertions.assertEquals(
                BigtableSchemaUtil.ModType.DELETE_FAMILY,
                BigtableSchemaUtil.getModType(DeleteFamily.create("cf")));
        Assertions.assertEquals(
                BigtableSchemaUtil.ModType.DELETE_CELLS,
                BigtableSchemaUtil.getModType(DeleteCells.create("cf", ByteString.copyFromUtf8("q"), Range.TimestampRange.create(0L, 1000L))));
        Assertions.assertEquals(0, BigtableSchemaUtil.ModType.SET_CELL.getId());
        Assertions.assertEquals(1, BigtableSchemaUtil.ModType.DELETE_FAMILY.getId());
        Assertions.assertEquals(2, BigtableSchemaUtil.ModType.DELETE_CELLS.getId());
        Assertions.assertEquals(3, BigtableSchemaUtil.ModType.UNKNOWN.getId());
    }

    @Test
    public void testResolveMutationOp() {
        Assertions.assertEquals(
                BigtableSchemaUtil.MutationOp.SET_CELL,
                BigtableSchemaUtil.resolveMutationOp("SET_CELL", null, null));
        Assertions.assertEquals(
                BigtableSchemaUtil.MutationOp.DELETE_FROM_ROW,
                BigtableSchemaUtil.resolveMutationOp("DELETE_FROM_ROW", null, null));
    }

    @Test
    public void testCreateCellSchemas() {
        final Schema cellSchema = BigtableSchemaUtil.createCellSchema();
        Assertions.assertTrue(cellSchema.hasField("rowKey"));
        Assertions.assertTrue(cellSchema.hasField("family"));
        Assertions.assertTrue(cellSchema.hasField("qualifier"));
        Assertions.assertTrue(cellSchema.hasField("value"));
        Assertions.assertTrue(cellSchema.hasField("timestamp"));
        Assertions.assertEquals(Schema.Type.timestamp, cellSchema.getField("timestamp").getFieldType().getType());

        final org.apache.avro.Schema avroSchema = BigtableSchemaUtil.createCellAvroSchema();
        Assertions.assertNotNull(avroSchema.getField("rowKey"));
        Assertions.assertNotNull(avroSchema.getField("family"));
        Assertions.assertNotNull(avroSchema.getField("qualifier"));
        Assertions.assertNotNull(avroSchema.getField("value"));
        Assertions.assertNotNull(avroSchema.getField("timestamp"));
    }

    @Test
    public void testCreateChangeRecordMutationSchema() {
        final Schema schema = BigtableSchemaUtil.createChangeRecordMutationSchema();
        Assertions.assertTrue(schema.hasField("rowKey"));
        Assertions.assertTrue(schema.hasField("modType"));
        Assertions.assertTrue(schema.hasField("commitTimestamp"));
        Assertions.assertTrue(schema.hasField("sourceCluster"));
        Assertions.assertTrue(schema.hasField("tieBreaker"));
        Assertions.assertTrue(schema.hasField("value"));
    }

    private static ColumnMetadata columnMetadata(final String name, final SqlType<?> type) {
        return new ColumnMetadata() {
            @Override
            public String name() {
                return name;
            }
            @Override
            public SqlType<?> type() {
                return type;
            }
        };
    }

    private static ResultSetMetadata metadataOf(final List<ColumnMetadata> columns) {
        return new ResultSetMetadata() {
            @Override
            public List<ColumnMetadata> getColumns() {
                return columns;
            }
            @Override
            public SqlType<?> getColumnType(int index) {
                return columns.get(index).type();
            }
            @Override
            public SqlType<?> getColumnType(String columnName) {
                return getColumnType(getColumnIndex(columnName));
            }
            @Override
            public int getColumnIndex(String columnName) {
                for (int i = 0; i < columns.size(); i++) {
                    if (columns.get(i).name().equals(columnName)) {
                        return i;
                    }
                }
                throw new IllegalArgumentException(columnName);
            }
        };
    }

    private static class FakeResultSet implements ResultSet {

        private final ResultSetMetadata metadata;
        private final Map<String, Object> values;

        FakeResultSet(final ResultSetMetadata metadata, final Map<String, Object> values) {
            this.metadata = metadata;
            this.values = values;
        }

        @Override
        public boolean next() {
            return false;
        }

        @Override
        public ResultSetMetadata getMetadata() {
            return metadata;
        }

        @Override
        public void close() {
        }

        @Override
        public boolean isNull(int index) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isNull(String name) {
            return values.get(name) == null;
        }

        @Override
        public ByteString getBytes(int index) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ByteString getBytes(String name) {
            return (ByteString) values.get(name);
        }

        @Override
        public String getString(int index) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getString(String name) {
            return (String) values.get(name);
        }

        @Override
        public long getLong(int index) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getLong(String name) {
            return (Long) values.get(name);
        }

        @Override
        public double getDouble(int index) {
            throw new UnsupportedOperationException();
        }

        @Override
        public double getDouble(String name) {
            return (Double) values.get(name);
        }

        @Override
        public float getFloat(int index) {
            throw new UnsupportedOperationException();
        }

        @Override
        public float getFloat(String name) {
            return (Float) values.get(name);
        }

        @Override
        public boolean getBoolean(int index) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean getBoolean(String name) {
            return (Boolean) values.get(name);
        }

        @Override
        public java.time.Instant getTimestamp(int index) {
            throw new UnsupportedOperationException();
        }

        @Override
        public java.time.Instant getTimestamp(String name) {
            return (java.time.Instant) values.get(name);
        }

        @Override
        public com.google.cloud.Date getDate(int index) {
            throw new UnsupportedOperationException();
        }

        @Override
        public com.google.cloud.Date getDate(String name) {
            return (com.google.cloud.Date) values.get(name);
        }

        @Override
        public Struct getStruct(int index) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Struct getStruct(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <ElemType> List<ElemType> getList(int index, SqlType.Array<ElemType> arrayType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <ElemType> List<ElemType> getList(String name, SqlType.Array<ElemType> arrayType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <K, V> Map<K, V> getMap(int index, SqlType.Map<K, V> mapType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <K, V> Map<K, V> getMap(String name, SqlType.Map<K, V> mapType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <MsgType extends AbstractMessage> MsgType getProtoMessage(int index, MsgType message) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <MsgType extends AbstractMessage> MsgType getProtoMessage(String name, MsgType message) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <EnumType extends ProtocolMessageEnum> EnumType getProtoEnum(int index, Function<Integer, EnumType> creator) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <EnumType extends ProtocolMessageEnum> EnumType getProtoEnum(String name, Function<Integer, EnumType> creator) {
            throw new UnsupportedOperationException();
        }
    }

    @Test
    public void testConvertResultSet() throws Exception {
        final List<ColumnMetadata> columns = List.of(
                columnMetadata("boolField", SqlType.bool()),
                columnMetadata("stringField", SqlType.string()),
                columnMetadata("bytesField", SqlType.bytes()),
                columnMetadata("longField", SqlType.int64()),
                columnMetadata("floatField", SqlType.float32()),
                columnMetadata("doubleField", SqlType.float64()),
                columnMetadata("dateField", SqlType.date()),
                columnMetadata("timestampField", SqlType.timestamp()),
                columnMetadata("nullField", SqlType.string()));
        final ResultSetMetadata metadata = metadataOf(columns);

        final Schema schema = BigtableSchemaUtil.convertSchema(metadata);
        Assertions.assertEquals(Schema.Type.bool, schema.getField("boolField").getFieldType().getType());
        Assertions.assertEquals(Schema.Type.string, schema.getField("stringField").getFieldType().getType());
        Assertions.assertEquals(Schema.Type.bytes, schema.getField("bytesField").getFieldType().getType());
        Assertions.assertEquals(Schema.Type.int64, schema.getField("longField").getFieldType().getType());
        Assertions.assertEquals(Schema.Type.float32, schema.getField("floatField").getFieldType().getType());
        Assertions.assertEquals(Schema.Type.float64, schema.getField("doubleField").getFieldType().getType());
        Assertions.assertEquals(Schema.Type.date, schema.getField("dateField").getFieldType().getType());
        Assertions.assertEquals(Schema.Type.timestamp, schema.getField("timestampField").getFieldType().getType());

        final Map<String, Object> values = new HashMap<>();
        values.put("boolField", true);
        values.put("stringField", "s");
        values.put("bytesField", ByteString.copyFromUtf8("b"));
        values.put("longField", 5L);
        values.put("floatField", 1.5F);
        values.put("doubleField", 2.5D);
        values.put("dateField", com.google.cloud.Date.fromYearMonthDay(2024, 3, 1));
        values.put("timestampField", java.time.Instant.parse("2024-01-01T00:00:00.123456Z"));
        values.put("nullField", null);

        final org.joda.time.Instant timestamp = org.joda.time.Instant.parse("2024-01-01T00:00:00Z");
        final MElement element = BigtableSchemaUtil.convert(new FakeResultSet(metadata, values), timestamp);

        Assertions.assertEquals(Boolean.TRUE, element.getPrimitiveValue("boolField"));
        Assertions.assertEquals("s", element.getPrimitiveValue("stringField"));
        Assertions.assertEquals(ByteString.copyFromUtf8("b"), element.getPrimitiveValue("bytesField"));
        Assertions.assertEquals(5L, element.getPrimitiveValue("longField"));
        Assertions.assertEquals(1.5F, element.getPrimitiveValue("floatField"));
        Assertions.assertEquals(2.5D, element.getPrimitiveValue("doubleField"));
        Assertions.assertEquals((int) LocalDate.parse("2024-03-01").toEpochDay(), element.getPrimitiveValue("dateField"));
        // convertFieldValue truncates the timestamp to milli precision (epoch micros value)
        Assertions.assertEquals(1704067200123000L, element.getPrimitiveValue("timestampField"));
        Assertions.assertNull(element.getPrimitiveValue("nullField"));
    }

    // remove hadoop serialize format
    /*
    @Test
    public void testToByteStringHadoop() {
        final Schema schema = Schema.builder()
                .withField("stringField", Schema.FieldType.STRING)
                .withField("intField", Schema.FieldType.INT32)
                .withField("longField", Schema.FieldType.INT64)
                .withField("floatField", Schema.FieldType.FLOAT32)
                .withField("doubleField", Schema.FieldType.FLOAT64)
                .withField("timestampField", Schema.FieldType.TIMESTAMP)
                .withField("dateField", Schema.FieldType.DATE)
                .withField("stringArrayField", Schema.FieldType.array(Schema.FieldType.STRING))
                .withField("intArrayField", Schema.FieldType.array(Schema.FieldType.INT32))
                .withField("elementField", Schema.FieldType.element(Schema.builder()
                        .withField("stringField", Schema.FieldType.STRING)
                        .withField("longField", Schema.FieldType.STRING)
                        .withField("stringArrayField", Schema.FieldType.array(Schema.FieldType.STRING))
                        .build()))
                .build();

        final MElement element = MElement.builder()
                .withString("stringField", "stringValue")
                .withInt32("intField", 10)
                .withInt64("longField", 10L)
                .withFloat32("floatField", 10.10F)
                .withFloat64("doubleField", 10.10D)
                .withTimestamp("timestampField", Instant.parse("2024-11-01T00:00:00Z"))
                .withDate("dateField", LocalDate.parse("2024-10-10"))
                .withStringList("stringArrayField", List.of("a","b","c"))
                .withStringList("intArrayField", List.of())
                .withMap("elementField", Map.of(
                        "stringField", "childStringValue",
                        "longField", 10L,
                        "stringArrayField", List.of("A", "B", "C")))
                .build();

        final ByteString byteString = BigtableSchemaUtil.toByteStringHadoop(element.asPrimitiveMap());
        final Map<String, Object> values = (Map<String, Object>) BigtableSchemaUtil.toPrimitiveValueFromWritable(Schema.FieldType.element(schema), byteString);
        Assertions.assertEquals("stringValue", values.get("stringField"));
        Assertions.assertEquals(10, values.get("intField"));
        Assertions.assertEquals(10L, values.get("longField"));
        Assertions.assertEquals(10.10F, values.get("floatField"));
        Assertions.assertEquals(10.10D, values.get("doubleField"));
        Assertions.assertEquals(DateTimeUtil.toEpochMicroSecond(Instant.parse("2024-11-01T00:00:00Z")), values.get("timestampField"));
        Assertions.assertEquals(Long.valueOf(LocalDate.of(2024,10,10).toEpochDay()).intValue(), values.get("dateField"));
        Assertions.assertEquals(List.of("a","b","c"), values.get("stringArrayField"));
        final Map<String, Object> child = (Map<String, Object>) values.get("elementField");
        Assertions.assertEquals("childStringValue", child.get("stringField"));
        Assertions.assertEquals(10L, child.get("longField"));
        Assertions.assertEquals(List.of("A", "B", "C"), child.get("stringArrayField"));
    }
     */

}
