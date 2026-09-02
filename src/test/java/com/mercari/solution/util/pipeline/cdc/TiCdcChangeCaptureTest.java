package com.mercari.solution.util.pipeline.cdc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class TiCdcChangeCaptureTest {

    @Test
    public void testNormalizeInsert() {
        final String event = """
                {"id":0,"database":"testdb","table":"users","pkNames":["id"],"isDdl":false,"type":"INSERT",
                 "es":1723190400000,"ts":1723190401000,"sql":"",
                 "data":[{"id":"1","name":"alice"}],"old":null,
                 "_tidb":{"commitTs":429918007904436226}}
                """.replace("\n", "");

        final List<Map<String, Object>> envelopes = TiCdcChangeCapture.normalize(event);
        Assertions.assertEquals(1, envelopes.size());
        final Map<String, Object> envelope = envelopes.getFirst();
        Assertions.assertEquals("users", envelope.get(ChangeRecord.FIELD_TABLE));
        Assertions.assertEquals(ChangeRecord.Op.INSERT.getId(), envelope.get(ChangeRecord.FIELD_OP));
        Assertions.assertEquals("{\"id\":\"1\"}", envelope.get(ChangeRecord.FIELD_KEYS));
        Assertions.assertNull(envelope.get(ChangeRecord.FIELD_BEFORE));
        Assertions.assertEquals("{\"id\":\"1\",\"name\":\"alice\"}", envelope.get(ChangeRecord.FIELD_AFTER));
        Assertions.assertEquals(1723190400000000L, envelope.get(ChangeRecord.FIELD_COMMIT_TIMESTAMP));
        Assertions.assertEquals(ChangeRecord.sequence(429918007904436226L, 0), envelope.get(ChangeRecord.FIELD_SEQUENCE));

        @SuppressWarnings("unchecked")
        final Map<String, Object> source = (Map<String, Object>) envelope.get(ChangeRecord.FIELD_SOURCE);
        Assertions.assertEquals(TiCdcChangeCapture.PROVIDER, source.get(ChangeRecord.FIELD_SOURCE_PROVIDER));
        Assertions.assertEquals("testdb", source.get(ChangeRecord.FIELD_SOURCE_DATABASE));
    }

    @Test
    public void testNormalizeUpdate() {
        final String event = """
                {"database":"testdb","table":"users","pkNames":["id"],"isDdl":false,"type":"UPDATE",
                 "es":1723190400000,"ts":1723190401000,
                 "data":[{"id":"1","name":"bob"}],"old":[{"name":"alice"}]}
                """.replace("\n", "");

        final List<Map<String, Object>> envelopes = TiCdcChangeCapture.normalize(event);
        Assertions.assertEquals(1, envelopes.size());
        final Map<String, Object> envelope = envelopes.getFirst();
        Assertions.assertEquals(ChangeRecord.Op.UPDATE.getId(), envelope.get(ChangeRecord.FIELD_OP));
        Assertions.assertEquals("{\"name\":\"alice\"}", envelope.get(ChangeRecord.FIELD_BEFORE));
        Assertions.assertEquals("{\"id\":\"1\",\"name\":\"bob\"}", envelope.get(ChangeRecord.FIELD_AFTER));
        // without the _tidb extension the sequence falls back to the event time
        Assertions.assertEquals(ChangeRecord.sequence(1723190400000000L, 0), envelope.get(ChangeRecord.FIELD_SEQUENCE));
    }

    @Test
    public void testNormalizeDelete() {
        final String event = """
                {"database":"testdb","table":"users","pkNames":["id"],"isDdl":false,"type":"DELETE",
                 "es":1723190400000,"ts":1723190401000,
                 "data":[{"id":"1","name":"alice"}],"old":null}
                """.replace("\n", "");

        final List<Map<String, Object>> envelopes = TiCdcChangeCapture.normalize(event);
        Assertions.assertEquals(1, envelopes.size());
        final Map<String, Object> envelope = envelopes.getFirst();
        Assertions.assertEquals(ChangeRecord.Op.DELETE.getId(), envelope.get(ChangeRecord.FIELD_OP));
        Assertions.assertEquals("{\"id\":\"1\",\"name\":\"alice\"}", envelope.get(ChangeRecord.FIELD_BEFORE));
        Assertions.assertNull(envelope.get(ChangeRecord.FIELD_AFTER));
    }

    @Test
    public void testNormalizeMultiRowEvent() {
        final String event = """
                {"database":"testdb","table":"users","pkNames":["id"],"isDdl":false,"type":"INSERT",
                 "es":1723190400000,"data":[{"id":"1"},{"id":"2"}]}
                """.replace("\n", "");

        final List<Map<String, Object>> envelopes = TiCdcChangeCapture.normalize(event);
        Assertions.assertEquals(2, envelopes.size());
        Assertions.assertNotEquals(
                envelopes.get(0).get(ChangeRecord.FIELD_SEQUENCE),
                envelopes.get(1).get(ChangeRecord.FIELD_SEQUENCE));
    }

    @Test
    public void testDdlBecomesControlAndWatermarkIsSkipped() {
        final List<Map<String, Object>> ddl = TiCdcChangeCapture.normalize(
                "{\"database\":\"testdb\",\"table\":\"users\",\"isDdl\":true,\"type\":\"QUERY\",\"sql\":\"ALTER TABLE users ADD c INT\"}");
        Assertions.assertEquals(1, ddl.size());
        Assertions.assertEquals(ChangeRecord.Op.SCHEMA.getId(), ddl.getFirst().get(ChangeRecord.FIELD_OP));
        Assertions.assertTrue(TiCdcChangeCapture.normalize(
                "{\"database\":\"\",\"table\":\"\",\"isDdl\":false,\"type\":\"TIDB_WATERMARK\"}").isEmpty());
    }

    @Test
    public void testIllegalEvent() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> TiCdcChangeCapture.normalize("[1,2,3]"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> TiCdcChangeCapture.normalize(
                "{\"database\":\"testdb\",\"isDdl\":false,\"type\":\"INSERT\",\"data\":[{\"id\":\"1\"}]}"));
    }


    @Test
    public void testDdlEvents() {
        final String alter = "{\"database\":\"d\",\"table\":\"users\",\"isDdl\":true,\"type\":\"QUERY\",\"es\":1723190400000,\"sql\":\"ALTER TABLE users ADD COLUMN age INT\"}";
        final List<Map<String, Object>> schema = TiCdcChangeCapture.normalize(alter);
        Assertions.assertEquals(1, schema.size());
        Assertions.assertEquals(ChangeRecord.Op.SCHEMA.getId(), schema.getFirst().get(ChangeRecord.FIELD_OP));
        Assertions.assertEquals("users", schema.getFirst().get(ChangeRecord.FIELD_TABLE));
        Assertions.assertEquals("ALTER TABLE users ADD COLUMN age INT", schema.getFirst().get(ChangeRecord.FIELD_STATEMENT));
        Assertions.assertNull(schema.getFirst().get(ChangeRecord.FIELD_KEYS));
        Assertions.assertNull(schema.getFirst().get(ChangeRecord.FIELD_SCHEMA));

        final String truncate = "{\"database\":\"d\",\"table\":\"users\",\"isDdl\":true,\"type\":\"QUERY\",\"es\":1723190400000,\"sql\":\"truncate   table users\"}";
        Assertions.assertEquals(ChangeRecord.Op.TRUNCATE.getId(), TiCdcChangeCapture.normalize(truncate).getFirst().get(ChangeRecord.FIELD_OP));
        final String drop = "{\"database\":\"d\",\"table\":\"users\",\"isDdl\":true,\"type\":\"QUERY\",\"es\":1723190400000,\"sql\":\"DROP TABLE users\"}";
        Assertions.assertEquals(ChangeRecord.Op.TRUNCATE.getId(), TiCdcChangeCapture.normalize(drop).getFirst().get(ChangeRecord.FIELD_OP));
        // database-level DDL has no table: skipped
        final String createDb = "{\"database\":\"d\",\"table\":\"\",\"isDdl\":true,\"type\":\"QUERY\",\"es\":1723190400000,\"sql\":\"CREATE DATABASE d\"}";
        Assertions.assertTrue(TiCdcChangeCapture.normalize(createDb).isEmpty());
        // watermark
        Assertions.assertTrue(TiCdcChangeCapture.normalize("{\"type\":\"TIDB_WATERMARK\",\"es\":1723190400000}").isEmpty());
    }

    @Test
    public void testColumnsAndTransaction() {
        final String event = "{\"database\":\"d\",\"table\":\"users\",\"pkNames\":[\"id\"],\"isDdl\":false,\"type\":\"INSERT\",\"es\":1723190400000,\"mysqlType\":{\"id\":\"int(11)\",\"name\":\"varchar(64)\"},\"data\":[{\"id\":\"1\",\"name\":\"alice\"}],\"_tidb\":{\"commitTs\":441}}";
        final com.google.gson.JsonObject parsed = TiCdcChangeCapture.parse(event);
        Assertions.assertEquals("users", TiCdcChangeCapture.table(parsed));
        Assertions.assertEquals(List.of(
                new ChangeSchema.Column("id", ChangeSchema.TYPE_INT64, true),
                new ChangeSchema.Column("name", ChangeSchema.TYPE_STRING, false)), TiCdcChangeCapture.columns(parsed));

        final Map<String, Object> envelope = TiCdcChangeCapture.normalize(parsed).getFirst();
        @SuppressWarnings("unchecked")
        final Map<String, Object> transaction = (Map<String, Object>) envelope.get(ChangeRecord.FIELD_TRANSACTION);
        Assertions.assertEquals("441", transaction.get(ChangeRecord.FIELD_TRANSACTION_ID));
        Assertions.assertEquals(0L, transaction.get(ChangeRecord.FIELD_TRANSACTION_INDEX));

        final Map<String, Object> schemaChange = TiCdcChangeCapture.schemaChange(parsed, "[]");
        Assertions.assertEquals(ChangeRecord.Op.SCHEMA.getId(), schemaChange.get(ChangeRecord.FIELD_OP));
        Assertions.assertTrue(ChangeRecord.compareSequence(
                (String) schemaChange.get(ChangeRecord.FIELD_SEQUENCE), (String) envelope.get(ChangeRecord.FIELD_SEQUENCE)) < 0);
    }

}
