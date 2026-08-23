package com.mercari.solution.util.pipeline.cdc;

import com.mercari.solution.module.MElement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PostgresChangeCaptureTest {

    @Test
    public void testSchema() {
        final var schema = PostgresChangeCapture.schema();
        for(final String field : List.of(
                PostgresChangeCapture.FIELD_COMMIT_LSN, PostgresChangeCapture.FIELD_TRANSACTION_ID,
                PostgresChangeCapture.FIELD_KEYS_JSON, PostgresChangeCapture.FIELD_COLUMNS_JSON)) {
            Assertions.assertTrue(schema.hasField(field), "missing field: " + field);
        }
    }

    @Test
    public void testNormalizeUpdate() {
        final MElement element = MElement.of(createRecordValues("UPDATE", "public", "items",
                "{\"id\":1}", "{\"id\":1,\"name\":\"a\"}", "{\"id\":1,\"name\":\"b\"}"), 0L);
        final List<Map<String, Object>> envelopes = PostgresChangeCapture.normalize(element);
        Assertions.assertEquals(1, envelopes.size());
        final Map<String, Object> envelope = envelopes.getFirst();
        Assertions.assertEquals("items", envelope.get(ChangeRecord.FIELD_TABLE));
        Assertions.assertEquals(ChangeRecord.Op.UPDATE.getId(), envelope.get(ChangeRecord.FIELD_OP));
        Assertions.assertEquals("{\"id\":1}", envelope.get(ChangeRecord.FIELD_KEYS));
        Assertions.assertEquals(ChangeRecord.sequence(5000L, 3L), envelope.get(ChangeRecord.FIELD_SEQUENCE));
        @SuppressWarnings("unchecked")
        final Map<String, Object> transaction = (Map<String, Object>) envelope.get(ChangeRecord.FIELD_TRANSACTION);
        Assertions.assertEquals("42", transaction.get(ChangeRecord.FIELD_TRANSACTION_ID));
        Assertions.assertEquals(3L, transaction.get(ChangeRecord.FIELD_TRANSACTION_INDEX));
        Assertions.assertNull(transaction.get(ChangeRecord.FIELD_TRANSACTION_TOTAL_RECORDS));

        // non-public schema tables are qualified
        final MElement other = MElement.of(createRecordValues("INSERT", "audit", "items",
                "{\"id\":1}", null, "{\"id\":1}"), 0L);
        Assertions.assertEquals("audit.items", PostgresChangeCapture.normalize(other).getFirst().get(ChangeRecord.FIELD_TABLE));
    }

    @Test
    public void testNormalizeTruncate() {
        final MElement element = MElement.of(createRecordValues("TRUNCATE", "public", "items", "{}", null, null), 0L);
        final List<Map<String, Object>> envelopes = PostgresChangeCapture.normalize(element);
        Assertions.assertEquals(1, envelopes.size());
        final Map<String, Object> envelope = envelopes.getFirst();
        Assertions.assertEquals(ChangeRecord.Op.TRUNCATE.getId(), envelope.get(ChangeRecord.FIELD_OP));
        Assertions.assertEquals("items", envelope.get(ChangeRecord.FIELD_TABLE));
        Assertions.assertNull(envelope.get(ChangeRecord.FIELD_KEYS));
        Assertions.assertEquals(ChangeRecord.sequence(5000L, 3L), envelope.get(ChangeRecord.FIELD_SEQUENCE));
    }

    @Test
    public void testColumnsAndSchemaChange() {
        final Map<String, Object> values = createRecordValues("INSERT", "public", "items", "{\"id\":1}", null, "{\"id\":1}");
        values.put(PostgresChangeCapture.FIELD_COLUMNS_JSON, "[{\"name\":\"id\",\"type\":\"INT64\",\"key\":true}]");
        final MElement element = MElement.of(values, 0L);
        final List<ChangeSchema.Column> columns = PostgresChangeCapture.columns(element);
        Assertions.assertEquals(List.of(new ChangeSchema.Column("id", ChangeSchema.TYPE_INT64, true)), columns);

        final Map<String, Object> schemaChange = PostgresChangeCapture.schemaChange(element, ChangeSchema.toJson(columns));
        Assertions.assertEquals(ChangeRecord.Op.SCHEMA.getId(), schemaChange.get(ChangeRecord.FIELD_OP));
        // sorts before every change of the transaction
        Assertions.assertTrue(ChangeRecord.compareSequence(
                (String) schemaChange.get(ChangeRecord.FIELD_SEQUENCE), ChangeRecord.sequence(5000L, 0L)) < 0);

        // archived before columnsJson existed
        values.remove(PostgresChangeCapture.FIELD_COLUMNS_JSON);
        Assertions.assertNull(PostgresChangeCapture.columns(MElement.of(values, 0L)));
    }

    private static Map<String, Object> createRecordValues(
            final String op, final String schema, final String table,
            final String keysJson, final String oldValuesJson, final String newValuesJson) {
        final Map<String, Object> values = new HashMap<>();
        values.put(PostgresChangeCapture.FIELD_LSN, 4002L);
        values.put(PostgresChangeCapture.FIELD_COMMIT_LSN, 5000L);
        values.put(PostgresChangeCapture.FIELD_COMMIT_TIMESTAMP, 1723190400000000L);
        values.put(PostgresChangeCapture.FIELD_TRANSACTION_ID, 42L);
        values.put(PostgresChangeCapture.FIELD_SEQUENCE, 3L);
        values.put(PostgresChangeCapture.FIELD_DATABASE, "db");
        values.put(PostgresChangeCapture.FIELD_SCHEMA, schema);
        values.put(PostgresChangeCapture.FIELD_TABLE, table);
        values.put(PostgresChangeCapture.FIELD_OP, op);
        values.put(PostgresChangeCapture.FIELD_KEYS_JSON, keysJson);
        values.put(PostgresChangeCapture.FIELD_OLD_VALUES_JSON, oldValuesJson);
        values.put(PostgresChangeCapture.FIELD_NEW_VALUES_JSON, newValuesJson);
        return values;
    }

}
