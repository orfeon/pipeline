package com.mercari.solution.util.pipeline.cdc;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mercari.solution.util.domain.db.PostgresUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ChangeSchemaTest {

    @Test
    public void testJsonRoundTrip() {
        final List<ChangeSchema.Column> columns = List.of(
                new ChangeSchema.Column("id", ChangeSchema.TYPE_INT64, true),
                new ChangeSchema.Column("name", ChangeSchema.TYPE_STRING, false),
                new ChangeSchema.Column("tags", "ARRAY<STRING>", false));
        final String json = ChangeSchema.toJson(columns);
        Assertions.assertEquals(columns, ChangeSchema.fromJson(json));
        Assertions.assertNull(ChangeSchema.toJson(null));
        Assertions.assertNull(ChangeSchema.fromJson(null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> ChangeSchema.fromJson("{}"));
    }

    @Test
    public void testFingerprintIsOrderIndependent() {
        final List<ChangeSchema.Column> a = List.of(
                new ChangeSchema.Column("id", ChangeSchema.TYPE_INT64, true),
                new ChangeSchema.Column("name", ChangeSchema.TYPE_STRING, false));
        final List<ChangeSchema.Column> b = List.of(
                new ChangeSchema.Column("name", ChangeSchema.TYPE_STRING, false),
                new ChangeSchema.Column("id", ChangeSchema.TYPE_INT64, true));
        Assertions.assertEquals(ChangeSchema.fingerprint(a), ChangeSchema.fingerprint(b));

        final List<ChangeSchema.Column> typeChanged = List.of(
                new ChangeSchema.Column("id", ChangeSchema.TYPE_STRING, true),
                new ChangeSchema.Column("name", ChangeSchema.TYPE_STRING, false));
        Assertions.assertNotEquals(ChangeSchema.fingerprint(a), ChangeSchema.fingerprint(typeChanged));
        final List<ChangeSchema.Column> keyChanged = List.of(
                new ChangeSchema.Column("id", ChangeSchema.TYPE_INT64, false),
                new ChangeSchema.Column("name", ChangeSchema.TYPE_STRING, false));
        Assertions.assertNotEquals(ChangeSchema.fingerprint(a), ChangeSchema.fingerprint(keyChanged));
        Assertions.assertNull(ChangeSchema.fingerprint(null));
    }

    @Test
    public void testAddedColumns() {
        final List<ChangeSchema.Column> previous = List.of(new ChangeSchema.Column("id", ChangeSchema.TYPE_INT64, true));
        final List<ChangeSchema.Column> current = List.of(
                new ChangeSchema.Column("id", ChangeSchema.TYPE_INT64, true),
                new ChangeSchema.Column("age", ChangeSchema.TYPE_INT64, false));
        Assertions.assertEquals(List.of(new ChangeSchema.Column("age", ChangeSchema.TYPE_INT64, false)),
                ChangeSchema.addedColumns(previous, current));
        Assertions.assertEquals(current, ChangeSchema.addedColumns(null, current));
        Assertions.assertTrue(ChangeSchema.addedColumns(current, null).isEmpty());
    }

    @Test
    public void testFromSpannerRowType() {
        final Map<String, Object> id = new HashMap<>();
        id.put("name", "Id");
        id.put("code", "INT64");
        id.put("isPrimaryKey", true);
        final Map<String, Object> tags = new HashMap<>();
        tags.put("name", "Tags");
        tags.put("code", "{\"code\":\"ARRAY\",\"array_element_type\":{\"code\":\"STRING\"}}");
        tags.put("isPrimaryKey", false);
        final Map<String, Object> attr = new HashMap<>();
        attr.put("name", "Attr");
        attr.put("code", "{\"code\":\"JSON\"}");
        attr.put("isPrimaryKey", "false");

        final List<ChangeSchema.Column> columns = ChangeSchema.fromSpannerRowType(List.of(id, tags, attr));
        Assertions.assertEquals(List.of(
                new ChangeSchema.Column("Id", ChangeSchema.TYPE_INT64, true),
                new ChangeSchema.Column("Tags", "ARRAY<STRING>", false),
                new ChangeSchema.Column("Attr", ChangeSchema.TYPE_JSON, false)), columns);

        Assertions.assertEquals("ARRAY<INT64>", ChangeSchema.fromSpannerTypeCode("ARRAY<INT64>"));
        Assertions.assertEquals(ChangeSchema.TYPE_NUMERIC, ChangeSchema.fromSpannerTypeCode("PG_NUMERIC"));
        Assertions.assertEquals(ChangeSchema.TYPE_STRING, ChangeSchema.fromSpannerTypeCode(null));
    }

    @Test
    public void testFromPostgresType() {
        Assertions.assertEquals(ChangeSchema.TYPE_INT64, ChangeSchema.fromPostgresType(PostgresUtil.ColumnType.INT4, null));
        Assertions.assertEquals(ChangeSchema.TYPE_DATETIME, ChangeSchema.fromPostgresType(PostgresUtil.ColumnType.TIMESTAMP, null));
        Assertions.assertEquals(ChangeSchema.TYPE_TIMESTAMP, ChangeSchema.fromPostgresType(PostgresUtil.ColumnType.TIMESTAMPTZ, null));
        Assertions.assertEquals(ChangeSchema.TYPE_JSON, ChangeSchema.fromPostgresType(PostgresUtil.ColumnType.JSONB, null));
        Assertions.assertEquals("ARRAY<STRING>", ChangeSchema.fromPostgresType(PostgresUtil.ColumnType.ARRAY, PostgresUtil.ColumnType.TEXT));
        Assertions.assertEquals(ChangeSchema.TYPE_STRING, ChangeSchema.fromPostgresType(PostgresUtil.ColumnType.UUID, null));
    }

    @Test
    public void testFromMysqlType() {
        Assertions.assertEquals(ChangeSchema.TYPE_INT64, ChangeSchema.fromMysqlType("int(11)"));
        Assertions.assertEquals(ChangeSchema.TYPE_INT64, ChangeSchema.fromMysqlType("bigint(20) unsigned"));
        Assertions.assertEquals(ChangeSchema.TYPE_NUMERIC, ChangeSchema.fromMysqlType("decimal(10,2)"));
        Assertions.assertEquals(ChangeSchema.TYPE_DATETIME, ChangeSchema.fromMysqlType("datetime(6)"));
        Assertions.assertEquals(ChangeSchema.TYPE_TIMESTAMP, ChangeSchema.fromMysqlType("timestamp"));
        Assertions.assertEquals(ChangeSchema.TYPE_STRING, ChangeSchema.fromMysqlType("varchar(255)"));
        Assertions.assertEquals(ChangeSchema.TYPE_BYTES, ChangeSchema.fromMysqlType("longblob"));
        Assertions.assertEquals(ChangeSchema.TYPE_STRING, ChangeSchema.fromMysqlType(null));
    }

    @Test
    public void testFromTiCdcEvent() {
        final JsonObject mysqlType = JsonParser.parseString("{\"id\":\"int(11)\",\"name\":\"varchar(64)\"}").getAsJsonObject();
        final List<ChangeSchema.Column> columns = ChangeSchema.fromTiCdcEvent(mysqlType, List.of("id"));
        Assertions.assertEquals(List.of(
                new ChangeSchema.Column("id", ChangeSchema.TYPE_INT64, true),
                new ChangeSchema.Column("name", ChangeSchema.TYPE_STRING, false)), columns);
        Assertions.assertNull(ChangeSchema.fromTiCdcEvent(null, List.of()));
    }

}
