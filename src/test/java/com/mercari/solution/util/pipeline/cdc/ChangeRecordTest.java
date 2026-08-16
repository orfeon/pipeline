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
                ChangeRecord.FIELD_COMMIT_TIMESTAMP, ChangeRecord.FIELD_SEQUENCE, ChangeRecord.FIELD_SOURCE)) {
            Assertions.assertTrue(schema.hasField(field), "missing field: " + field);
        }
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

}
