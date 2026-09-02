package com.mercari.solution.util.pipeline.cdc;

import com.google.api.services.bigquery.model.TableRow;
import com.mercari.solution.module.MElement;
import org.apache.beam.sdk.io.gcp.bigquery.RowMutationInformation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ChangeRecordTest {

    @Test
    public void testSchema() {
        final var schema = ChangeRecord.schema();
        for(final String field : List.of(
                ChangeRecord.FIELD_TABLE, ChangeRecord.FIELD_OP, ChangeRecord.FIELD_KEYS,
                ChangeRecord.FIELD_BEFORE, ChangeRecord.FIELD_AFTER,
                ChangeRecord.FIELD_COMMIT_TIMESTAMP, ChangeRecord.FIELD_SEQUENCE, ChangeRecord.FIELD_SOURCE,
                ChangeRecord.FIELD_TRANSACTION, ChangeRecord.FIELD_SCHEMA, ChangeRecord.FIELD_STATEMENT)) {
            Assertions.assertTrue(schema.hasField(field), "missing field: " + field);
        }
        // control records carry no keys
        Assertions.assertTrue(schema.getField(ChangeRecord.FIELD_KEYS).getFieldType().getNullable());
    }

    @Test
    public void testOpIds() {
        // symbol ids are part of the archived data: they must never change
        Assertions.assertEquals(0, ChangeRecord.Op.INSERT.getId());
        Assertions.assertEquals(1, ChangeRecord.Op.UPDATE.getId());
        Assertions.assertEquals(2, ChangeRecord.Op.DELETE.getId());
        Assertions.assertEquals(3, ChangeRecord.Op.SNAPSHOT.getId());
        Assertions.assertEquals(4, ChangeRecord.Op.TRUNCATE.getId());
        Assertions.assertEquals(5, ChangeRecord.Op.SCHEMA.getId());
        Assertions.assertFalse(ChangeRecord.Op.UPDATE.isControl());
        Assertions.assertTrue(ChangeRecord.Op.TRUNCATE.isControl());
        Assertions.assertTrue(ChangeRecord.Op.SCHEMA.isControl());
        Assertions.assertTrue(ChangeRecord.Op.SNAPSHOT_BEGIN.isControl());
        Assertions.assertEquals(ChangeRecord.Op.SCHEMA, ChangeRecord.getOp(5));
    }

    @Test
    public void testControl() {
        final Map<String, Object> source = new HashMap<>();
        source.put(ChangeRecord.FIELD_SOURCE_PROVIDER, "postgres");
        final Map<String, Object> control = ChangeRecord.control(
                "items", ChangeRecord.Op.TRUNCATE, 1L, "ff/1", source, ChangeRecord.transaction("42", null, 3L), null, null);
        Assertions.assertEquals(ChangeRecord.Op.TRUNCATE.getId(), control.get(ChangeRecord.FIELD_OP));
        Assertions.assertNull(control.get(ChangeRecord.FIELD_KEYS));
        Assertions.assertNull(control.get(ChangeRecord.FIELD_AFTER));
        @SuppressWarnings("unchecked")
        final Map<String, Object> transaction = (Map<String, Object>) control.get(ChangeRecord.FIELD_TRANSACTION);
        Assertions.assertEquals("42", transaction.get(ChangeRecord.FIELD_TRANSACTION_ID));
        Assertions.assertEquals(3L, transaction.get(ChangeRecord.FIELD_TRANSACTION_INDEX));
        Assertions.assertThrows(IllegalArgumentException.class, () -> ChangeRecord.control(
                "items", ChangeRecord.Op.INSERT, 1L, "ff/1", source, null, null, null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> ChangeRecord
                .toRowMutationInformation(ChangeRecord.Op.TRUNCATE, "ff/1"));
        Assertions.assertNull(ChangeRecord.transaction(null, null, null));
    }

    @Test
    public void testSplitKeyChange() {
        final Map<String, Object> update = new HashMap<>();
        update.put(ChangeRecord.FIELD_TABLE, "items");
        update.put(ChangeRecord.FIELD_OP, ChangeRecord.Op.UPDATE.getId());
        update.put(ChangeRecord.FIELD_KEYS, "{\"id\":1}");
        update.put(ChangeRecord.FIELD_BEFORE, "{\"id\":1,\"name\":\"a\"}");
        update.put(ChangeRecord.FIELD_AFTER, "{\"id\":2,\"name\":\"a\"}");
        update.put(ChangeRecord.FIELD_SEQUENCE, "ff/1");

        final List<Map<String, Object>> split = ChangeRecord.splitKeyChange(update);
        Assertions.assertEquals(2, split.size());
        final Map<String, Object> delete = split.get(0);
        Assertions.assertEquals(ChangeRecord.Op.DELETE.getId(), delete.get(ChangeRecord.FIELD_OP));
        Assertions.assertEquals("{\"id\":1}", delete.get(ChangeRecord.FIELD_KEYS));
        Assertions.assertNull(delete.get(ChangeRecord.FIELD_AFTER));
        Assertions.assertEquals("ff/1/0", delete.get(ChangeRecord.FIELD_SEQUENCE));
        final Map<String, Object> insert = split.get(1);
        Assertions.assertEquals(ChangeRecord.Op.INSERT.getId(), insert.get(ChangeRecord.FIELD_OP));
        Assertions.assertEquals("{\"id\":2}", insert.get(ChangeRecord.FIELD_KEYS));
        Assertions.assertNull(insert.get(ChangeRecord.FIELD_BEFORE));
        Assertions.assertEquals("{\"id\":2,\"name\":\"a\"}", insert.get(ChangeRecord.FIELD_AFTER));
        Assertions.assertEquals("ff/1/1", insert.get(ChangeRecord.FIELD_SEQUENCE));
        Assertions.assertTrue(ChangeRecord.compareSequence("ff/1/0", "ff/1/1") < 0);

        // same key: untouched
        update.put(ChangeRecord.FIELD_AFTER, "{\"id\":1,\"name\":\"b\"}");
        Assertions.assertSame(update, ChangeRecord.splitKeyChange(update).getFirst());
        // before without the key columns (partial old image): untouched
        update.put(ChangeRecord.FIELD_BEFORE, "{\"name\":\"a\"}");
        update.put(ChangeRecord.FIELD_AFTER, "{\"id\":2,\"name\":\"b\"}");
        Assertions.assertEquals(1, ChangeRecord.splitKeyChange(update).size());
        // not an UPDATE: untouched
        update.put(ChangeRecord.FIELD_OP, ChangeRecord.Op.DELETE.getId());
        Assertions.assertEquals(1, ChangeRecord.splitKeyChange(update).size());
        // no room in the sequence
        update.put(ChangeRecord.FIELD_OP, ChangeRecord.Op.UPDATE.getId());
        update.put(ChangeRecord.FIELD_BEFORE, "{\"id\":1}");
        update.put(ChangeRecord.FIELD_SEQUENCE, "1/2/3/4");
        Assertions.assertThrows(IllegalArgumentException.class, () -> ChangeRecord.splitKeyChange(update));
    }

    @Test
    public void testLatest() {
        final Map<String, Object> v1 = new HashMap<>();
        v1.put(ChangeRecord.FIELD_SEQUENCE, "ff/1");
        final Map<String, Object> v2 = new HashMap<>();
        v2.put(ChangeRecord.FIELD_SEQUENCE, "ff/2");
        final MElement latest = ChangeRecord.latest(List.of(MElement.of(v2, 0L), MElement.of(v1, 0L)));
        Assertions.assertEquals("ff/2", latest.getAsString(ChangeRecord.FIELD_SEQUENCE));
        Assertions.assertNull(ChangeRecord.latest(List.of()));
    }

    @Test
    public void testSequence() {
        Assertions.assertEquals("ff", ChangeRecord.sequence(255L));
        Assertions.assertEquals("ff/1/0", ChangeRecord.sequence(255L, 1L, 0L));
        Assertions.assertTrue(ChangeRecord.isValidSequence(ChangeRecord.sequence(1723190400000000L, 12L, 3L)));
        Assertions.assertFalse(ChangeRecord.isValidSequence(null));
        Assertions.assertFalse(ChangeRecord.isValidSequence(""));
        Assertions.assertFalse(ChangeRecord.isValidSequence("xyz"));
        Assertions.assertFalse(ChangeRecord.isValidSequence("1/2/3/4/5"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> ChangeRecord.sequence());
        Assertions.assertThrows(IllegalArgumentException.class, () -> ChangeRecord.sequence(1L, 2L, 3L, 4L, 5L));
        Assertions.assertThrows(IllegalArgumentException.class, () -> ChangeRecord.sequence(-1L));
    }

    @Test
    public void testCompareSequence() {
        Assertions.assertEquals(0, ChangeRecord.compareSequence("ff/1", "FF/1"));
        Assertions.assertTrue(ChangeRecord.compareSequence("100", "ff") > 0);
        Assertions.assertTrue(ChangeRecord.compareSequence("ff/1", "ff/2") < 0);
        Assertions.assertTrue(ChangeRecord.compareSequence("ff/1/1", "ff/1") > 0);
        Assertions.assertTrue(ChangeRecord.compareSequence("ff", "ff/0") < 0);
        // ordering must follow numeric value, not string order
        Assertions.assertTrue(ChangeRecord.compareSequence("a", "9") > 0);
    }

    @Test
    public void testGetOp() {
        Assertions.assertEquals(ChangeRecord.Op.INSERT, ChangeRecord.getOp(0));
        Assertions.assertEquals(ChangeRecord.Op.UPDATE, ChangeRecord.getOp(1L));
        Assertions.assertEquals(ChangeRecord.Op.DELETE, ChangeRecord.getOp("DELETE"));
        Assertions.assertEquals(ChangeRecord.Op.SNAPSHOT, ChangeRecord.getOp(ChangeRecord.Op.SNAPSHOT));
        Assertions.assertThrows(IllegalArgumentException.class, () -> ChangeRecord.getOp((Object) null));
    }

    @Test
    public void testToRowMutationInformation() {
        final RowMutationInformation upsert = ChangeRecord
                .toRowMutationInformation(ChangeRecord.Op.INSERT, "ff/1");
        Assertions.assertEquals(RowMutationInformation.MutationType.UPSERT, upsert.getMutationType());

        final RowMutationInformation delete = ChangeRecord
                .toRowMutationInformation(ChangeRecord.Op.DELETE, "ff/2");
        Assertions.assertEquals(RowMutationInformation.MutationType.DELETE, delete.getMutationType());

        Assertions.assertEquals(RowMutationInformation.MutationType.UPSERT,
                ChangeRecord.toRowMutationInformation(ChangeRecord.Op.SNAPSHOT, "1").getMutationType());

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ChangeRecord.toRowMutationInformation(ChangeRecord.Op.INSERT, "not-hex!"));
    }

    @Test
    public void testToTableRowUpsert() {
        final Map<String, Object> values = new HashMap<>();
        values.put(ChangeRecord.FIELD_TABLE, "Users");
        values.put(ChangeRecord.FIELD_OP, ChangeRecord.Op.UPDATE.getId());
        values.put(ChangeRecord.FIELD_KEYS, "{\"userId\":\"u1\"}");
        values.put(ChangeRecord.FIELD_AFTER, "{\"name\":\"alice\",\"age\":20,\"active\":true,\"tags\":[\"a\",\"b\"],\"address\":{\"city\":\"tokyo\"}}");
        values.put(ChangeRecord.FIELD_SEQUENCE, "ff/1");

        final TableRow tableRow = ChangeRecord.toTableRow(MElement.of(values, 0L));
        Assertions.assertEquals("u1", tableRow.get("userId"));
        Assertions.assertEquals("alice", tableRow.get("name"));
        // numbers are carried as strings and parsed by the destination table schema
        Assertions.assertEquals("20", tableRow.get("age"));
        Assertions.assertEquals(true, tableRow.get("active"));
        Assertions.assertEquals(List.of("a", "b"), tableRow.get("tags"));
        Assertions.assertEquals("tokyo", ((TableRow) tableRow.get("address")).get("city"));
    }

    @Test
    public void testToTableRowDelete() {
        final Map<String, Object> values = new HashMap<>();
        values.put(ChangeRecord.FIELD_TABLE, "Users");
        values.put(ChangeRecord.FIELD_OP, ChangeRecord.Op.DELETE.getId());
        values.put(ChangeRecord.FIELD_KEYS, "{\"userId\":\"u1\"}");
        values.put(ChangeRecord.FIELD_BEFORE, "{\"name\":\"alice\"}");
        values.put(ChangeRecord.FIELD_SEQUENCE, "ff/2");

        final TableRow tableRow = ChangeRecord.toTableRow(MElement.of(values, 0L));
        Assertions.assertEquals("u1", tableRow.get("userId"));
        // non-key values must not be written on delete
        Assertions.assertNull(tableRow.get("name"));
    }


    @Test
    public void testEnvelopeJsonRoundTrip() {
        final Map<String, Object> source = new HashMap<>();
        source.put(ChangeRecord.FIELD_SOURCE_PROVIDER, "spanner");
        source.put(ChangeRecord.FIELD_SOURCE_DATABASE, null);
        source.put(ChangeRecord.FIELD_SOURCE_METADATA, "{\"serverTransactionId\":\"tx1\"}");
        final Map<String, Object> values = new HashMap<>();
        values.put(ChangeRecord.FIELD_TABLE, "Users");
        values.put(ChangeRecord.FIELD_OP, ChangeRecord.Op.UPDATE.getId());
        values.put(ChangeRecord.FIELD_KEYS, "{\"userId\":\"u1\"}");
        values.put(ChangeRecord.FIELD_BEFORE, null);
        values.put(ChangeRecord.FIELD_AFTER, "{\"name\":\"alice\",\"age\":20}");
        values.put(ChangeRecord.FIELD_COMMIT_TIMESTAMP, 1723190400000000L);
        values.put(ChangeRecord.FIELD_SEQUENCE, "ff/1/0");
        values.put(ChangeRecord.FIELD_SOURCE, source);
        values.put(ChangeRecord.FIELD_TRANSACTION, ChangeRecord.transaction("tx1", null, 65536L));
        values.put(ChangeRecord.FIELD_SCHEMA, null);
        values.put(ChangeRecord.FIELD_STATEMENT, null);

        final TableRow row = ChangeRecord.toEnvelopeTableRow(MElement.of(values, 0L));
        Assertions.assertEquals("UPDATE", row.get(ChangeRecord.FIELD_OP));
        Assertions.assertEquals("{\"userId\":\"u1\"}", row.get(ChangeRecord.FIELD_KEYS));
        Assertions.assertEquals(1723190400000000L, row.get(ChangeRecord.FIELD_COMMIT_TIMESTAMP));

        // the failure record json is the TableRow's json form
        final Map<String, Object> parsed = ChangeRecord.fromJson(row.toString());
        Assertions.assertEquals("Users", parsed.get(ChangeRecord.FIELD_TABLE));
        Assertions.assertEquals(ChangeRecord.Op.UPDATE.getId(), parsed.get(ChangeRecord.FIELD_OP));
        Assertions.assertEquals("{\"userId\":\"u1\"}", parsed.get(ChangeRecord.FIELD_KEYS));
        Assertions.assertNull(parsed.get(ChangeRecord.FIELD_BEFORE));
        Assertions.assertEquals("{\"name\":\"alice\",\"age\":20}", parsed.get(ChangeRecord.FIELD_AFTER));
        Assertions.assertEquals(1723190400000000L, parsed.get(ChangeRecord.FIELD_COMMIT_TIMESTAMP));
        Assertions.assertEquals("ff/1/0", parsed.get(ChangeRecord.FIELD_SEQUENCE));
        @SuppressWarnings("unchecked")
        final Map<String, Object> parsedSource = (Map<String, Object>) parsed.get(ChangeRecord.FIELD_SOURCE);
        Assertions.assertEquals("spanner", parsedSource.get(ChangeRecord.FIELD_SOURCE_PROVIDER));
        Assertions.assertEquals("{\"serverTransactionId\":\"tx1\"}", parsedSource.get(ChangeRecord.FIELD_SOURCE_METADATA));
        @SuppressWarnings("unchecked")
        final Map<String, Object> parsedTransaction = (Map<String, Object>) parsed.get(ChangeRecord.FIELD_TRANSACTION);
        Assertions.assertEquals("tx1", parsedTransaction.get(ChangeRecord.FIELD_TRANSACTION_ID));
        Assertions.assertEquals(65536L, parsedTransaction.get(ChangeRecord.FIELD_TRANSACTION_INDEX));

        // envelope json written by other tools: nested objects, op symbol, ISO commit timestamp
        final Map<String, Object> lenient = ChangeRecord.fromJson(
                "{\"table\":\"t\",\"op\":\"DELETE\",\"keys\":{\"id\":1},\"commitTimestamp\":\"2024-08-09T08:00:00Z\",\"sequence\":\"a/b\"}");
        Assertions.assertEquals(ChangeRecord.Op.DELETE.getId(), lenient.get(ChangeRecord.FIELD_OP));
        Assertions.assertEquals("{\"id\":1}", lenient.get(ChangeRecord.FIELD_KEYS));
        Assertions.assertEquals(1723190400000000L, lenient.get(ChangeRecord.FIELD_COMMIT_TIMESTAMP));
        Assertions.assertEquals("envelope", ((Map<?, ?>) lenient.get(ChangeRecord.FIELD_SOURCE)).get(ChangeRecord.FIELD_SOURCE_PROVIDER));
        Assertions.assertNull(lenient.get(ChangeRecord.FIELD_TRANSACTION));

        Assertions.assertThrows(IllegalArgumentException.class, () -> ChangeRecord.fromJson("[]"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> ChangeRecord.fromJson("{\"table\":\"t\"}"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> ChangeRecord.fromJson(
                "{\"table\":\"t\",\"op\":\"INSERT\",\"commitTimestamp\":1,\"sequence\":\"not-hex!\"}"));

        // archived envelope records
        final Map<String, Object> fromElement = ChangeRecord.fromElement(MElement.of(values, 0L));
        Assertions.assertEquals(parsed.get(ChangeRecord.FIELD_SEQUENCE), fromElement.get(ChangeRecord.FIELD_SEQUENCE));
        Assertions.assertEquals(ChangeRecord.Op.UPDATE.getId(), fromElement.get(ChangeRecord.FIELD_OP));
    }

}
