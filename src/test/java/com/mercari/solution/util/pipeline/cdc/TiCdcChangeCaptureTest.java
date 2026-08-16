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
    public void testSkipDdlAndWatermark() {
        Assertions.assertTrue(TiCdcChangeCapture.normalize(
                "{\"database\":\"testdb\",\"table\":\"users\",\"isDdl\":true,\"type\":\"QUERY\",\"sql\":\"ALTER TABLE users ADD c INT\"}").isEmpty());
        Assertions.assertTrue(TiCdcChangeCapture.normalize(
                "{\"database\":\"\",\"table\":\"\",\"isDdl\":false,\"type\":\"TIDB_WATERMARK\"}").isEmpty());
    }

    @Test
    public void testIllegalEvent() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> TiCdcChangeCapture.normalize("[1,2,3]"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> TiCdcChangeCapture.normalize(
                "{\"database\":\"testdb\",\"isDdl\":false,\"type\":\"INSERT\",\"data\":[{\"id\":\"1\"}]}"));
    }

}
