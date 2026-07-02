package com.mercari.solution.util.domain.db;

import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.List;

public class PostgresUtilTest {

    private static final Schema TEST_SCHEMA = SchemaBuilder.record("root").fields()
            .name("boolField").type(AvroSchemaUtil.NULLABLE_BOOLEAN).noDefault()
            .name("shortField").type(AvroSchemaUtil.NULLABLE_INT).noDefault()
            .name("intField").type(AvroSchemaUtil.NULLABLE_INT).noDefault()
            .name("longField").type(AvroSchemaUtil.NULLABLE_LONG).noDefault()
            .name("floatField").type(AvroSchemaUtil.NULLABLE_FLOAT).noDefault()
            .name("doubleField").type(AvroSchemaUtil.NULLABLE_DOUBLE).noDefault()
            .name("decimalField").type(AvroSchemaUtil.NULLABLE_LOGICAL_DECIMAL_TYPE).noDefault()
            .name("textField").type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
            .name("bytesField").type(AvroSchemaUtil.NULLABLE_BYTES).noDefault()
            .name("dateField").type(AvroSchemaUtil.NULLABLE_LOGICAL_DATE_TYPE).noDefault()
            .name("timeField").type(AvroSchemaUtil.NULLABLE_LOGICAL_TIME_MICRO_TYPE).noDefault()
            .name("timestampField").type(AvroSchemaUtil.NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
            .name("timestamptzField").type(AvroSchemaUtil.NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
            .name("uuidField").type(AvroSchemaUtil.NULLABLE_LOGICAL_UUID_TYPE).noDefault()
            .name("jsonField").type(AvroSchemaUtil.NULLABLE_JSON).noDefault()
            .name("jsonbField").type(AvroSchemaUtil.NULLABLE_JSON).noDefault()
            .endRecord();

    private static final List<PostgresUtil.Column> TEST_COLUMNS = Arrays.asList(
            new PostgresUtil.Column("boolField", PostgresUtil.ColumnType.BOOL),
            new PostgresUtil.Column("shortField", PostgresUtil.ColumnType.INT2),
            new PostgresUtil.Column("intField", PostgresUtil.ColumnType.INT4),
            new PostgresUtil.Column("longField", PostgresUtil.ColumnType.INT8),
            new PostgresUtil.Column("floatField", PostgresUtil.ColumnType.FLOAT4),
            new PostgresUtil.Column("doubleField", PostgresUtil.ColumnType.FLOAT8),
            new PostgresUtil.Column("decimalField", PostgresUtil.ColumnType.NUMERIC),
            new PostgresUtil.Column("textField", PostgresUtil.ColumnType.TEXT),
            new PostgresUtil.Column("bytesField", PostgresUtil.ColumnType.BYTEA),
            new PostgresUtil.Column("dateField", PostgresUtil.ColumnType.DATE),
            new PostgresUtil.Column("timeField", PostgresUtil.ColumnType.TIME),
            new PostgresUtil.Column("timestampField", PostgresUtil.ColumnType.TIMESTAMP),
            new PostgresUtil.Column("timestamptzField", PostgresUtil.ColumnType.TIMESTAMPTZ),
            new PostgresUtil.Column("uuidField", PostgresUtil.ColumnType.UUID),
            new PostgresUtil.Column("jsonField", PostgresUtil.ColumnType.JSON),
            new PostgresUtil.Column("jsonbField", PostgresUtil.ColumnType.JSONB));

    @Test
    public void testCopyBinaryRoundTrip() throws IOException {

        final GenericData.Record record = new GenericData.Record(TEST_SCHEMA);
        record.put("boolField", true);
        record.put("shortField", 12);
        record.put("intField", -123456);
        record.put("longField", 1234567890123L);
        record.put("floatField", 1.25F);
        record.put("doubleField", -2.5D);
        record.put("decimalField", toDecimalBytes(new BigDecimal("123.45")));
        record.put("textField", "hello ' postgres");
        record.put("bytesField", ByteBuffer.wrap("binary".getBytes(StandardCharsets.UTF_8)));
        record.put("dateField", (int) LocalDate.of(2024, 1, 15).toEpochDay());
        record.put("timeField", LocalTime.of(12, 34, 56, 789000000).toNanoOfDay() / 1000L);
        record.put("timestampField", 1700000000000000L);
        record.put("timestamptzField", -1000000L);
        record.put("uuidField", "123e4567-e89b-12d3-a456-426614174000");
        record.put("jsonField", "{\"a\":1}");
        record.put("jsonbField", "{\"b\":[1,2]}");

        final GenericRecord output = roundTrip(record);

        Assertions.assertEquals(true, output.get("boolField"));
        Assertions.assertEquals(12, output.get("shortField"));
        Assertions.assertEquals(-123456, output.get("intField"));
        Assertions.assertEquals(1234567890123L, output.get("longField"));
        Assertions.assertEquals(1.25F, output.get("floatField"));
        Assertions.assertEquals(-2.5D, output.get("doubleField"));
        Assertions.assertEquals(toDecimalBytes(new BigDecimal("123.45")), output.get("decimalField"));
        Assertions.assertEquals("hello ' postgres", output.get("textField"));
        Assertions.assertEquals(ByteBuffer.wrap("binary".getBytes(StandardCharsets.UTF_8)), output.get("bytesField"));
        Assertions.assertEquals((int) LocalDate.of(2024, 1, 15).toEpochDay(), output.get("dateField"));
        Assertions.assertEquals(LocalTime.of(12, 34, 56, 789000000).toNanoOfDay() / 1000L, output.get("timeField"));
        Assertions.assertEquals(1700000000000000L, output.get("timestampField"));
        Assertions.assertEquals(-1000000L, output.get("timestamptzField"));
        Assertions.assertEquals("123e4567-e89b-12d3-a456-426614174000", output.get("uuidField"));
        Assertions.assertEquals("{\"a\":1}", output.get("jsonField"));
        Assertions.assertEquals("{\"b\":[1,2]}", output.get("jsonbField"));
    }

    @Test
    public void testCopyBinaryRoundTripNulls() throws IOException {

        final GenericData.Record record = new GenericData.Record(TEST_SCHEMA);
        for(final Schema.Field field : TEST_SCHEMA.getFields()) {
            record.put(field.name(), null);
        }

        final GenericRecord output = roundTrip(record);

        for(final Schema.Field field : TEST_SCHEMA.getFields()) {
            Assertions.assertNull(output.get(field.name()));
        }
    }

    @Test
    public void testNumericRoundTrip() throws IOException {

        final List<BigDecimal> decimals = Arrays.asList(
                new BigDecimal("0"),
                new BigDecimal("1"),
                new BigDecimal("-1"),
                new BigDecimal("123.45"),
                new BigDecimal("-123.45"),
                new BigDecimal("0.000000001"),
                new BigDecimal("-0.000000001"),
                new BigDecimal("10000"),
                new BigDecimal("12345678901234567890.123456789"),
                new BigDecimal("-99999999999999999999.999999999"));

        final Schema schema = SchemaBuilder.record("root").fields()
                .name("decimalField").type(AvroSchemaUtil.NULLABLE_LOGICAL_DECIMAL_TYPE).noDefault()
                .endRecord();
        final List<PostgresUtil.Column> columns = List.of(
                new PostgresUtil.Column("decimalField", PostgresUtil.ColumnType.NUMERIC));

        for(final BigDecimal decimal : decimals) {
            final GenericData.Record record = new GenericData.Record(schema);
            record.put("decimalField", toDecimalBytes(decimal));

            final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try(final DataOutputStream output = new DataOutputStream(bytes)) {
                PostgresUtil.writeHeader(output);
                PostgresUtil.write(output, columns, schema.getFields(), record);
                PostgresUtil.writeTrailer(output);
            }
            try(final DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
                PostgresUtil.readHeader(input);
                final GenericRecord output = PostgresUtil.read(input, schema, columns);
                Assertions.assertEquals(toDecimalBytes(decimal), output.get("decimalField"),
                        decimal.toPlainString());
                Assertions.assertNull(PostgresUtil.read(input, schema, columns));
            }
        }
    }

    @Test
    public void testCreateQueryAndCopyStatement() {

        Assertions.assertEquals(
                "SELECT * FROM mytable",
                PostgresUtil.createQuery("mytable", "*", null, null));
        Assertions.assertEquals(
                "SELECT id,name FROM mytable WHERE id >= 1 AND id < 10 AND (name IS NOT NULL)",
                PostgresUtil.createQuery("mytable", "id,name", "name IS NOT NULL", "id >= 1 AND id < 10"));
        Assertions.assertEquals(
                "COPY (SELECT * FROM mytable) TO STDOUT (FORMAT BINARY)",
                PostgresUtil.createCopyOutStatement("SELECT * FROM mytable"));
        Assertions.assertEquals(
                "COPY mytable (id,name) FROM STDIN (FORMAT BINARY)",
                PostgresUtil.createCopyInStatement("mytable", Arrays.asList("id", "name")));
    }

    @Test
    public void testRangeCondition() {

        final PostgresUtil.Range range = PostgresUtil.Range.of(0, 100);
        Assertions.assertEquals("ctid >= '(0,0)'::tid AND ctid < '(100,0)'::tid", range.createCondition());

        final PostgresUtil.Range last = PostgresUtil.Range.from(100);
        Assertions.assertEquals("ctid >= '(100,0)'::tid", last.createCondition());

        final PostgresUtil.Range full = PostgresUtil.Range.full();
        Assertions.assertNull(full.createCondition());
    }

    @Test
    public void testCreateBlockRanges() {

        // density 50 rows/block, splitSize 1000 rows -> 20 blocks per split
        final List<PostgresUtil.Range> ranges = PostgresUtil.createBlockRanges(45, 50d * 45, 1000);
        Assertions.assertEquals(3, ranges.size());
        Assertions.assertEquals("ctid >= '(0,0)'::tid AND ctid < '(20,0)'::tid", ranges.get(0).createCondition());
        Assertions.assertEquals("ctid >= '(20,0)'::tid AND ctid < '(40,0)'::tid", ranges.get(1).createCondition());
        // last range is open-ended
        Assertions.assertEquals("ctid >= '(40,0)'::tid", ranges.get(2).createCondition());

        // empty table -> single full range
        final List<PostgresUtil.Range> empty = PostgresUtil.createBlockRanges(0, 0d, 1000);
        Assertions.assertEquals(1, empty.size());
        Assertions.assertTrue(empty.getFirst().isFull());
        Assertions.assertNull(empty.getFirst().createCondition());

        // un-analyzed table (estimatedRows <= 0) falls back to default density without error
        final List<PostgresUtil.Range> unanalyzed = PostgresUtil.createBlockRanges(1000, -1d, 1000);
        Assertions.assertFalse(unanalyzed.isEmpty());
        Assertions.assertEquals(0L, (long) unanalyzed.getFirst().startBlock);
    }

    private static GenericRecord roundTrip(final GenericRecord record) throws IOException {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try(final DataOutputStream output = new DataOutputStream(bytes)) {
            PostgresUtil.writeHeader(output);
            PostgresUtil.write(output, TEST_COLUMNS, TEST_SCHEMA.getFields(), record);
            PostgresUtil.writeTrailer(output);
        }
        try(final DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            PostgresUtil.readHeader(input);
            final GenericRecord output = PostgresUtil.read(input, TEST_SCHEMA, TEST_COLUMNS);
            Assertions.assertNotNull(output);
            Assertions.assertNull(PostgresUtil.read(input, TEST_SCHEMA, TEST_COLUMNS));
            return output;
        }
    }

    private static ByteBuffer toDecimalBytes(final BigDecimal decimal) {
        return ByteBuffer.wrap(decimal.setScale(9).unscaledValue().toByteArray());
    }

}