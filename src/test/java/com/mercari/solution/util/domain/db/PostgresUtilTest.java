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

    private static final Schema EXTENDED_SCHEMA = SchemaBuilder.record("root").fields()
            .name("timetzField").type(AvroSchemaUtil.NULLABLE_LOGICAL_TIME_MICRO_TYPE).noDefault()
            .name("xmlField").type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
            .name("inetField").type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
            .name("inet6Field").type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
            .name("cidrField").type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
            .name("macaddrField").type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
            .name("macaddr8Field").type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
            .name("enumField").type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
            .name("intArrayField").type(Schema.createUnion(
                    Schema.createArray(Schema.create(Schema.Type.INT)), Schema.create(Schema.Type.NULL))).noDefault()
            .name("textArrayField").type(Schema.createUnion(
                    Schema.createArray(Schema.create(Schema.Type.STRING)), Schema.create(Schema.Type.NULL))).noDefault()
            .name("enumArrayField").type(Schema.createUnion(
                    Schema.createArray(Schema.create(Schema.Type.STRING)), Schema.create(Schema.Type.NULL))).noDefault()
            .name("decimalArrayField").type(Schema.createUnion(
                    Schema.createArray(AvroSchemaUtil.REQUIRED_LOGICAL_DECIMAL_TYPE), Schema.create(Schema.Type.NULL))).noDefault()
            .name("timestampArrayField").type(Schema.createUnion(
                    Schema.createArray(AvroSchemaUtil.REQUIRED_LOGICAL_TIMESTAMP_MICRO_TYPE), Schema.create(Schema.Type.NULL))).noDefault()
            .name("emptyArrayField").type(Schema.createUnion(
                    Schema.createArray(Schema.create(Schema.Type.LONG)), Schema.create(Schema.Type.NULL))).noDefault()
            .endRecord();

    private static final List<PostgresUtil.Column> EXTENDED_COLUMNS = Arrays.asList(
            new PostgresUtil.Column("timetzField", PostgresUtil.ColumnType.TIMETZ),
            new PostgresUtil.Column("xmlField", PostgresUtil.ColumnType.XML),
            new PostgresUtil.Column("inetField", PostgresUtil.ColumnType.INET),
            new PostgresUtil.Column("inet6Field", PostgresUtil.ColumnType.INET),
            new PostgresUtil.Column("cidrField", PostgresUtil.ColumnType.CIDR),
            new PostgresUtil.Column("macaddrField", PostgresUtil.ColumnType.MACADDR),
            new PostgresUtil.Column("macaddr8Field", PostgresUtil.ColumnType.MACADDR8),
            new PostgresUtil.Column("enumField", PostgresUtil.ColumnType.ENUM),
            PostgresUtil.Column.arrayOf("intArrayField", PostgresUtil.ColumnType.INT4),
            PostgresUtil.Column.arrayOf("textArrayField", PostgresUtil.ColumnType.TEXT),
            PostgresUtil.Column.arrayOf("enumArrayField", PostgresUtil.ColumnType.ENUM, 99999),
            PostgresUtil.Column.arrayOf("decimalArrayField", PostgresUtil.ColumnType.NUMERIC),
            PostgresUtil.Column.arrayOf("timestampArrayField", PostgresUtil.ColumnType.TIMESTAMPTZ),
            PostgresUtil.Column.arrayOf("emptyArrayField", PostgresUtil.ColumnType.INT8));

    @Test
    public void testCopyBinaryRoundTripExtendedTypes() throws IOException {

        final GenericData.Record record = new GenericData.Record(EXTENDED_SCHEMA);
        record.put("timetzField", LocalTime.of(3, 34, 56, 789000000).toNanoOfDay() / 1000L);
        record.put("xmlField", "<a attr=\"v\">1</a>");
        record.put("inetField", "192.168.0.1");
        record.put("inet6Field", "2001:db8:0:0:0:0:0:1/128");
        record.put("cidrField", "192.168.100.0/24");
        record.put("macaddrField", "08:00:2b:01:02:03");
        record.put("macaddr8Field", "08:00:2b:01:02:03:04:05");
        record.put("enumField", "happy");
        record.put("intArrayField", Arrays.asList(1, -2, 3));
        record.put("textArrayField", Arrays.asList("a", "b c", ""));
        record.put("enumArrayField", Arrays.asList("sad", "happy"));
        record.put("decimalArrayField", Arrays.asList(
                toDecimalBytes(new BigDecimal("123.45")), toDecimalBytes(new BigDecimal("-0.01"))));
        record.put("timestampArrayField", Arrays.asList(1700000000000000L, null, 1700000001000000L));
        record.put("emptyArrayField", List.of());

        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try(final DataOutputStream output = new DataOutputStream(bytes)) {
            PostgresUtil.writeHeader(output);
            PostgresUtil.write(output, EXTENDED_COLUMNS, EXTENDED_SCHEMA.getFields(), record);
            PostgresUtil.writeTrailer(output);
        }
        final GenericRecord output;
        try(final DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            PostgresUtil.readHeader(input);
            output = PostgresUtil.read(input, EXTENDED_SCHEMA, EXTENDED_COLUMNS);
            Assertions.assertNotNull(output);
            Assertions.assertNull(PostgresUtil.read(input, EXTENDED_SCHEMA, EXTENDED_COLUMNS));
        }

        Assertions.assertEquals(LocalTime.of(3, 34, 56, 789000000).toNanoOfDay() / 1000L, output.get("timetzField"));
        Assertions.assertEquals("<a attr=\"v\">1</a>", output.get("xmlField"));
        // a missing netmask suffix defaults to a single host on write; read always appends it
        Assertions.assertEquals("192.168.0.1/32", output.get("inetField"));
        Assertions.assertEquals("2001:db8:0:0:0:0:0:1/128", output.get("inet6Field"));
        Assertions.assertEquals("192.168.100.0/24", output.get("cidrField"));
        Assertions.assertEquals("08:00:2b:01:02:03", output.get("macaddrField"));
        Assertions.assertEquals("08:00:2b:01:02:03:04:05", output.get("macaddr8Field"));
        Assertions.assertEquals("happy", output.get("enumField"));
        Assertions.assertEquals(Arrays.asList(1, -2, 3), output.get("intArrayField"));
        Assertions.assertEquals(Arrays.asList("a", "b c", ""), output.get("textArrayField"));
        Assertions.assertEquals(Arrays.asList("sad", "happy"), output.get("enumArrayField"));
        Assertions.assertEquals(Arrays.asList(
                toDecimalBytes(new BigDecimal("123.45")), toDecimalBytes(new BigDecimal("-0.01"))),
                output.get("decimalArrayField"));
        // null array elements are skipped on read
        Assertions.assertEquals(Arrays.asList(1700000000000000L, 1700000001000000L), output.get("timestampArrayField"));
        Assertions.assertEquals(List.of(), output.get("emptyArrayField"));
    }

    @Test
    public void testCopyBinaryRoundTripExtendedTypesNulls() throws IOException {

        final GenericData.Record record = new GenericData.Record(EXTENDED_SCHEMA);
        for(final Schema.Field field : EXTENDED_SCHEMA.getFields()) {
            record.put(field.name(), null);
        }

        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try(final DataOutputStream output = new DataOutputStream(bytes)) {
            PostgresUtil.writeHeader(output);
            PostgresUtil.write(output, EXTENDED_COLUMNS, EXTENDED_SCHEMA.getFields(), record);
            PostgresUtil.writeTrailer(output);
        }
        try(final DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            PostgresUtil.readHeader(input);
            final GenericRecord output = PostgresUtil.read(input, EXTENDED_SCHEMA, EXTENDED_COLUMNS);
            for(final Schema.Field field : EXTENDED_SCHEMA.getFields()) {
                Assertions.assertNull(output.get(field.name()));
            }
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

    // sink statement generation

    private static final PostgresUtil.TableId USERS = new PostgresUtil.TableId("public", "users");

    @Test
    public void testParseTableId() {
        Assertions.assertEquals(new PostgresUtil.TableId("public", "users"), PostgresUtil.parseTableId("users"));
        Assertions.assertEquals(new PostgresUtil.TableId("app", "users"), PostgresUtil.parseTableId("app.users"));
        Assertions.assertEquals(new PostgresUtil.TableId("App", "My.Table"), PostgresUtil.parseTableId("\"App\".\"My.Table\""));
        Assertions.assertEquals(new PostgresUtil.TableId("public", "a\"b"), PostgresUtil.parseTableId("\"a\"\"b\""));
        Assertions.assertThrows(IllegalArgumentException.class, () -> PostgresUtil.parseTableId("a.b.c"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> PostgresUtil.parseTableId(""));
        Assertions.assertEquals("\"app\".\"users\"", PostgresUtil.parseTableId("app.users").quotedName());
    }

    @Test
    public void testCreateStagingStatements() {
        final String staging = PostgresUtil.createStagingTableName(USERS, List.of("id", "name"), PostgresUtil.WriteOp.INSERT_OR_UPDATE);
        Assertions.assertTrue(staging.startsWith("_mp_users_"), staging);
        Assertions.assertTrue(staging.length() <= 63);
        // a different column layout yields a different staging table
        Assertions.assertNotEquals(staging, PostgresUtil.createStagingTableName(USERS, List.of("id"), PostgresUtil.WriteOp.INSERT_OR_UPDATE));
        Assertions.assertNotEquals(staging, PostgresUtil.createStagingTableName(USERS, List.of("id", "name"), PostgresUtil.WriteOp.MERGE));

        final PostgresUtil.TableId longName = new PostgresUtil.TableId("public", "x".repeat(80));
        Assertions.assertTrue(PostgresUtil.createStagingTableName(longName, List.of("id"), PostgresUtil.WriteOp.DELETE).length() <= 63);

        // CTAS (not LIKE) so that the destination's NOT NULL constraints are not inherited by the staging table
        Assertions.assertEquals(
                "CREATE TEMP TABLE IF NOT EXISTS \"stg\" ON COMMIT DELETE ROWS AS SELECT \"id\", \"name\" FROM \"public\".\"users\" WITH NO DATA",
                PostgresUtil.createStagingTableStatement("stg", USERS, List.of("id", "name"), false));
        Assertions.assertEquals(
                "CREATE TEMP TABLE IF NOT EXISTS \"stg\" ON COMMIT DELETE ROWS AS SELECT \"id\", NULL::text AS \"_mp_op\" FROM \"public\".\"users\" WITH NO DATA",
                PostgresUtil.createStagingTableStatement("stg", USERS, List.of("id"), true));
        Assertions.assertEquals("TRUNCATE TABLE \"public\".\"users\"", PostgresUtil.createTruncateStatement(USERS));
        Assertions.assertEquals("DELETE FROM \"public\".\"users\"", PostgresUtil.createDeleteAllStatement(USERS));
    }

    @Test
    public void testCreateApplyStatementInsertOrUpdate() {
        final PostgresUtil.ApplySpec spec = new PostgresUtil.ApplySpec(
                PostgresUtil.WriteOp.INSERT_OR_UPDATE, USERS, "stg",
                List.of("id", "name", "age"), List.of("id"), List.of("name", "age"), null, null);
        Assertions.assertEquals(
                "INSERT INTO \"public\".\"users\" AS target (\"id\", \"name\", \"age\")"
                        + " SELECT DISTINCT ON (\"id\") \"id\", \"name\", \"age\" FROM \"stg\" ORDER BY \"id\", ctid DESC"
                        + " ON CONFLICT (\"id\") DO UPDATE SET \"name\" = EXCLUDED.\"name\", \"age\" = EXCLUDED.\"age\"",
                PostgresUtil.createApplyStatement(spec));

        final PostgresUtil.ApplySpec conditional = new PostgresUtil.ApplySpec(
                PostgresUtil.WriteOp.INSERT_OR_UPDATE, USERS, "stg",
                List.of("id", "name", "updated"), List.of("id"), List.of("name", "updated"),
                "target.updated < excluded.updated", null);
        Assertions.assertTrue(PostgresUtil.createApplyStatement(conditional)
                .endsWith(" WHERE target.updated < excluded.updated"));

        // nothing to update (key-only table) degrades to DO NOTHING
        final PostgresUtil.ApplySpec keyOnly = new PostgresUtil.ApplySpec(
                PostgresUtil.WriteOp.INSERT_OR_UPDATE, USERS, "stg",
                List.of("id"), List.of("id"), List.of(), null, null);
        Assertions.assertTrue(PostgresUtil.createApplyStatement(keyOnly).endsWith(" ON CONFLICT (\"id\") DO NOTHING"));
    }

    @Test
    public void testCreateApplyStatementInsertOrDoNothingAndDelete() {
        final PostgresUtil.ApplySpec doNothing = new PostgresUtil.ApplySpec(
                PostgresUtil.WriteOp.INSERT_OR_DONOTHING, USERS, "stg",
                List.of("id", "name"), List.of("id"), List.of(), null, null);
        Assertions.assertEquals(
                "INSERT INTO \"public\".\"users\" (\"id\", \"name\") SELECT \"id\", \"name\" FROM \"stg\" ORDER BY \"id\", ctid"
                        + " ON CONFLICT (\"id\") DO NOTHING",
                PostgresUtil.createApplyStatement(doNothing));

        final PostgresUtil.ApplySpec delete = new PostgresUtil.ApplySpec(
                PostgresUtil.WriteOp.DELETE, USERS, "stg",
                List.of("tenant", "id"), List.of("tenant", "id"), List.of(), null, null);
        Assertions.assertEquals(
                "DELETE FROM \"public\".\"users\" USING (SELECT DISTINCT \"tenant\", \"id\" FROM \"stg\") AS s"
                        + " WHERE \"public\".\"users\".\"tenant\" = s.\"tenant\" AND \"public\".\"users\".\"id\" = s.\"id\"",
                PostgresUtil.createApplyStatement(delete));

        Assertions.assertThrows(IllegalArgumentException.class, () -> PostgresUtil.createApplyStatement(new PostgresUtil.ApplySpec(
                PostgresUtil.WriteOp.INSERT, USERS, "stg", List.of("id"), List.of(), List.of(), null, null)));
    }

    @Test
    public void testCreateApplyStatementMerge() {
        final PostgresUtil.ApplySpec merge = new PostgresUtil.ApplySpec(
                PostgresUtil.WriteOp.MERGE, USERS, "stg",
                List.of("id", "name"), List.of("id"), List.of("name"), null, null);
        Assertions.assertEquals(
                "MERGE INTO \"public\".\"users\" AS t"
                        + " USING (SELECT DISTINCT ON (\"id\") \"id\", \"name\", \"_mp_op\" FROM \"stg\" AS s0 ORDER BY \"id\", s0.ctid DESC) AS s"
                        + " ON t.\"id\" = s.\"id\""
                        + " WHEN MATCHED AND s.\"_mp_op\" = 'DELETE' THEN DELETE"
                        + " WHEN MATCHED THEN UPDATE SET \"name\" = s.\"name\""
                        + " WHEN NOT MATCHED AND s.\"_mp_op\" <> 'DELETE' THEN INSERT (\"id\", \"name\") VALUES (s.\"id\", s.\"name\")",
                PostgresUtil.createApplyStatement(merge));

        final PostgresUtil.ApplySpec guarded = new PostgresUtil.ApplySpec(
                PostgresUtil.WriteOp.MERGE, USERS, "stg",
                List.of("id", "name", "seq"), List.of("id"), List.of("name", "seq"), null, "seq");
        final String sql = PostgresUtil.createApplyStatement(guarded);
        Assertions.assertTrue(sql.contains("ORDER BY \"id\", s0.\"seq\" COLLATE \"C\" DESC"), sql);
        Assertions.assertTrue(sql.contains(
                " WHEN MATCHED AND t.\"seq\" IS NOT NULL AND t.\"seq\" COLLATE \"C\" >= s.\"seq\" COLLATE \"C\" THEN DO NOTHING"
                        + " WHEN MATCHED AND s.\"_mp_op\" = 'DELETE' THEN DELETE"), sql);
    }

    @Test
    public void testCreateCreateTableStatement() {
        final Schema schema = SchemaBuilder.record("root").fields()
                .name("id").type(AvroSchemaUtil.REQUIRED_LONG).noDefault()
                .name("name").type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                .name("price").type(AvroSchemaUtil.NULLABLE_LOGICAL_DECIMAL_TYPE).noDefault()
                .name("created").type(AvroSchemaUtil.NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
                .name("birthday").type(AvroSchemaUtil.NULLABLE_LOGICAL_DATE_TYPE).noDefault()
                .name("tags").type(Schema.createUnion(
                        Schema.createArray(Schema.create(Schema.Type.STRING)), Schema.create(Schema.Type.NULL))).noDefault()
                .name("attrs").type(Schema.createUnion(
                        Schema.createMap(Schema.create(Schema.Type.STRING)), Schema.create(Schema.Type.NULL))).noDefault()
                .endRecord();
        Assertions.assertEquals(
                "CREATE TABLE IF NOT EXISTS \"public\".\"users\" ("
                        + "\"id\" bigint NOT NULL, \"name\" text, \"price\" numeric(38, 9), \"created\" timestamptz, \"birthday\" date,"
                        + " \"tags\" text[], \"attrs\" jsonb, PRIMARY KEY (\"id\"))",
                PostgresUtil.createCreateTableStatement(USERS, schema, List.of("id")));
    }

    @Test
    public void testFromJsonValueAndParseTimestamp() {
        Assertions.assertEquals(1700000000000000L, PostgresUtil.parseTimestampMicros("2023-11-14T22:13:20Z"));
        Assertions.assertEquals(1700000000000000L, PostgresUtil.parseTimestampMicros("2023-11-14T22:13:20"));
        Assertions.assertEquals(1700000000500000L, PostgresUtil.parseTimestampMicros("2023-11-15T07:13:20.5+09:00"));
        // database text form (canal-json / postgres text output): space separator
        Assertions.assertEquals(1700000000000000L, PostgresUtil.parseTimestampMicros("2023-11-14 22:13:20"));

        final com.google.gson.JsonObject json = com.google.gson.JsonParser.parseString("""
                {"b": true, "i": 12, "n": "12345.67", "s": "hello", "d": "2024-01-15", "t": "12:34:56.789",
                 "ts": "2023-11-14T22:13:20Z", "bytes": "YmluYXJ5", "arr": [1, 2], "j": {"a": 1}, "nul": null}
                """).getAsJsonObject();
        Assertions.assertEquals(Boolean.TRUE, PostgresUtil.fromJsonValue(new PostgresUtil.Column("b", PostgresUtil.ColumnType.BOOL), json.get("b")));
        Assertions.assertEquals(12L, PostgresUtil.fromJsonValue(new PostgresUtil.Column("i", PostgresUtil.ColumnType.INT4), json.get("i")));
        Assertions.assertEquals(new BigDecimal("12345.67"), PostgresUtil.fromJsonValue(new PostgresUtil.Column("n", PostgresUtil.ColumnType.NUMERIC), json.get("n")));
        Assertions.assertEquals("hello", PostgresUtil.fromJsonValue(new PostgresUtil.Column("s", PostgresUtil.ColumnType.TEXT), json.get("s")));
        Assertions.assertEquals((int) LocalDate.of(2024, 1, 15).toEpochDay(), PostgresUtil.fromJsonValue(new PostgresUtil.Column("d", PostgresUtil.ColumnType.DATE), json.get("d")));
        Assertions.assertEquals(LocalTime.of(12, 34, 56, 789000000).toNanoOfDay() / 1000L, PostgresUtil.fromJsonValue(new PostgresUtil.Column("t", PostgresUtil.ColumnType.TIME), json.get("t")));
        Assertions.assertEquals(1700000000000000L, PostgresUtil.fromJsonValue(new PostgresUtil.Column("ts", PostgresUtil.ColumnType.TIMESTAMPTZ), json.get("ts")));
        Assertions.assertArrayEquals("binary".getBytes(StandardCharsets.UTF_8), (byte[]) PostgresUtil.fromJsonValue(new PostgresUtil.Column("bytes", PostgresUtil.ColumnType.BYTEA), json.get("bytes")));
        Assertions.assertEquals(List.of(1L, 2L), PostgresUtil.fromJsonValue(PostgresUtil.Column.arrayOf("arr", PostgresUtil.ColumnType.INT8), json.get("arr")));
        Assertions.assertEquals("{\"a\":1}", PostgresUtil.fromJsonValue(new PostgresUtil.Column("j", PostgresUtil.ColumnType.JSONB), json.get("j")));
        Assertions.assertNull(PostgresUtil.fromJsonValue(new PostgresUtil.Column("nul", PostgresUtil.ColumnType.TEXT), json.get("nul")));
        Assertions.assertNull(PostgresUtil.fromJsonValue(new PostgresUtil.Column("missing", PostgresUtil.ColumnType.TEXT), json.get("missing")));
    }

    @Test
    public void testEncodeRejectsOutOfRangeIntegers() throws IOException {
        final GenericData.Record record = new GenericData.Record(TEST_SCHEMA);
        record.put("shortField", 40000);
        Assertions.assertThrows(IllegalArgumentException.class, () -> roundTrip(record));

        final GenericData.Record record2 = new GenericData.Record(TEST_SCHEMA);
        record2.put("intField", 1);
        Assertions.assertEquals(1, roundTrip(record2).get("intField"));
    }

    @Test
    public void testEncodeStructuredValuesAsJson() throws IOException {
        final Schema nested = SchemaBuilder.record("nested").fields()
                .name("a").type(AvroSchemaUtil.REQUIRED_LONG).noDefault()
                .name("b").type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                .endRecord();
        final GenericData.Record child = new GenericData.Record(nested);
        child.put("a", 1L);
        child.put("b", "x");

        final GenericData.Record record = new GenericData.Record(TEST_SCHEMA);
        record.put("jsonField", child);
        record.put("jsonbField", java.util.Map.of("k", List.of(1, 2)));
        final GenericRecord output = roundTrip(record);
        Assertions.assertEquals("{\"a\":1,\"b\":\"x\"}", output.get("jsonField"));
        Assertions.assertEquals("{\"k\":[1,2]}", output.get("jsonbField"));
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