package com.mercari.solution.util.pipeline.cdc;

import com.mercari.solution.module.MElement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SpannerChangeCaptureTest {

    @Test
    public void testSchema() {
        final var schema = SpannerChangeCapture.schema();
        for(final String field : List.of(
                SpannerChangeCapture.FIELD_COMMIT_TIMESTAMP,
                SpannerChangeCapture.FIELD_RECORD_SEQUENCE,
                SpannerChangeCapture.FIELD_TABLE_NAME,
                SpannerChangeCapture.FIELD_ROW_TYPE,
                SpannerChangeCapture.FIELD_MODS,
                SpannerChangeCapture.FIELD_MOD_TYPE,
                SpannerChangeCapture.FIELD_VALUE_CAPTURE_TYPE,
                SpannerChangeCapture.FIELD_TRANSACTION_TAG,
                SpannerChangeCapture.FIELD_IS_SYSTEM_TRANSACTION)) {
            Assertions.assertTrue(schema.hasField(field), "missing field: " + field);
        }
    }

    @Test
    public void testNormalize() {
        final MElement element = MElement.of(createRecordValues(
                "UPDATE",
                List.of(
                        mod("{\"userId\":\"u1\"}", "{\"name\":\"alice\"}", "{\"name\":\"bob\"}"),
                        mod("{\"userId\":\"u2\"}", "{}", "{\"name\":\"carol\"}"))), 0L);

        final List<Map<String, Object>> envelopes = SpannerChangeCapture.normalize(element);
        Assertions.assertEquals(2, envelopes.size());

        final Map<String, Object> first = envelopes.getFirst();
        Assertions.assertEquals("Users", first.get(ChangeRecord.FIELD_TABLE));
        Assertions.assertEquals(ChangeRecord.Op.UPDATE.getId(), first.get(ChangeRecord.FIELD_OP));
        Assertions.assertEquals("{\"userId\":\"u1\"}", first.get(ChangeRecord.FIELD_KEYS));
        Assertions.assertEquals("{\"name\":\"alice\"}", first.get(ChangeRecord.FIELD_BEFORE));
        Assertions.assertEquals("{\"name\":\"bob\"}", first.get(ChangeRecord.FIELD_AFTER));
        Assertions.assertEquals(1723190400000000L, first.get(ChangeRecord.FIELD_COMMIT_TIMESTAMP));
        Assertions.assertEquals(ChangeRecord.sequence(1723190400000000L, 1L, 0L), first.get(ChangeRecord.FIELD_SEQUENCE));

        final Map<String, Object> second = envelopes.get(1);
        // empty oldValuesJson must be normalized to null
        Assertions.assertNull(second.get(ChangeRecord.FIELD_BEFORE));
        Assertions.assertEquals(ChangeRecord.sequence(1723190400000000L, 1L, 1L), second.get(ChangeRecord.FIELD_SEQUENCE));

        @SuppressWarnings("unchecked")
        final Map<String, Object> source = (Map<String, Object>) first.get(ChangeRecord.FIELD_SOURCE);
        Assertions.assertEquals(SpannerChangeCapture.PROVIDER, source.get(ChangeRecord.FIELD_SOURCE_PROVIDER));
        Assertions.assertNotNull(source.get(ChangeRecord.FIELD_SOURCE_METADATA));
    }

    @Test
    public void testNormalizeDelete() {
        final MElement element = MElement.of(createRecordValues(
                "DELETE",
                List.of(mod("{\"userId\":\"u1\"}", "{\"name\":\"alice\"}", "{}"))), 0L);

        final List<Map<String, Object>> envelopes = SpannerChangeCapture.normalize(element);
        Assertions.assertEquals(1, envelopes.size());
        final Map<String, Object> envelope = envelopes.getFirst();
        Assertions.assertEquals(ChangeRecord.Op.DELETE.getId(), envelope.get(ChangeRecord.FIELD_OP));
        Assertions.assertEquals("{\"name\":\"alice\"}", envelope.get(ChangeRecord.FIELD_BEFORE));
        // empty newValuesJson must be normalized to null
        Assertions.assertNull(envelope.get(ChangeRecord.FIELD_AFTER));
    }

    @Test
    public void testNormalizeIllegalModType() {
        final MElement element = MElement.of(createRecordValues(
                "TRUNCATE",
                List.of(mod("{\"userId\":\"u1\"}", null, null))), 0L);
        Assertions.assertThrows(IllegalArgumentException.class, () -> SpannerChangeCapture.normalize(element));
    }

    private static Map<String, Object> createRecordValues(final String modType, final List<Map<String, Object>> mods) {
        final Map<String, Object> values = new HashMap<>();
        values.put(SpannerChangeCapture.FIELD_PARTITION_TOKEN, "token");
        values.put(SpannerChangeCapture.FIELD_COMMIT_TIMESTAMP, 1723190400000000L);
        values.put(SpannerChangeCapture.FIELD_SERVER_TRANSACTION_ID, "tx1");
        values.put(SpannerChangeCapture.FIELD_IS_LAST_RECORD, true);
        values.put(SpannerChangeCapture.FIELD_RECORD_SEQUENCE, "00000001");
        values.put(SpannerChangeCapture.FIELD_TABLE_NAME, "Users");
        values.put(SpannerChangeCapture.FIELD_ROW_TYPE, new ArrayList<>());
        values.put(SpannerChangeCapture.FIELD_MODS, mods);
        values.put(SpannerChangeCapture.FIELD_MOD_TYPE, modType);
        values.put(SpannerChangeCapture.FIELD_VALUE_CAPTURE_TYPE, "OLD_AND_NEW_VALUES");
        values.put(SpannerChangeCapture.FIELD_TRANSACTION_TAG, "tag");
        values.put(SpannerChangeCapture.FIELD_IS_SYSTEM_TRANSACTION, false);
        values.put("numberOfRecordsInTransaction", 1L);
        values.put("numberOfPartitionsInTransaction", 1L);
        return values;
    }

    private static Map<String, Object> mod(final String keysJson, final String oldValuesJson, final String newValuesJson) {
        final Map<String, Object> mod = new HashMap<>();
        mod.put(SpannerChangeCapture.FIELD_KEYS_JSON, keysJson);
        mod.put(SpannerChangeCapture.FIELD_OLD_VALUES_JSON, oldValuesJson);
        mod.put(SpannerChangeCapture.FIELD_NEW_VALUES_JSON, newValuesJson);
        return mod;
    }

}
