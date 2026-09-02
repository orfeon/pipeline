package com.mercari.solution.util.pipeline.cdc;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.mercari.solution.util.schema.RowSchemaUtil;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class ChangeRecordMutationConverterTest {

    private static final Schema SCHEMA = Schema.builder()
            .addStringField("id")
            .addInt64Field("seq")
            .addNullableField("name", Schema.FieldType.STRING)
            .addNullableField("active", Schema.FieldType.BOOLEAN)
            .addNullableField("score", Schema.FieldType.DOUBLE)
            .addNullableField("birthday", Schema.FieldType.logicalType(SqlTypes.DATE))
            .build();

    private static final ChangeRecordMutationConverter.TableSchema TABLE =
            new ChangeRecordMutationConverter.TableSchema("Users", SCHEMA, List.of("id", "seq"));

    private static Map<String, Object> envelope(final ChangeRecord.Op op, final String keys, final String after, final String sequence) {
        final Map<String, Object> values = new HashMap<>();
        values.put(ChangeRecord.FIELD_TABLE, "Users");
        values.put(ChangeRecord.FIELD_OP, op.getId());
        values.put(ChangeRecord.FIELD_KEYS, keys);
        values.put(ChangeRecord.FIELD_AFTER, after);
        values.put(ChangeRecord.FIELD_SEQUENCE, sequence);
        return values;
    }

    @Test
    public void testUpsertSetsOnlyPresentColumns() {
        final ChangeRecordMutationConverter converter = new ChangeRecordMutationConverter();
        final Mutation mutation = converter.convert(TABLE, envelope(ChangeRecord.Op.UPDATE,
                "{\"id\":\"u1\",\"seq\":2}", "{\"name\":\"alice\",\"active\":true,\"birthday\":\"2020-01-02\",\"unknown\":1}", "ff/1"));
        Assertions.assertEquals(Mutation.Op.INSERT_OR_UPDATE, mutation.getOperation());
        Assertions.assertEquals("Users", mutation.getTable());
        final Map<String, Value> values = mutation.asMap();
        Assertions.assertEquals("u1", values.get("id").getString());
        Assertions.assertEquals(2L, values.get("seq").getInt64());
        Assertions.assertEquals("alice", values.get("name").getString());
        Assertions.assertTrue(values.get("active").getBool());
        Assertions.assertEquals("2020-01-02", values.get("birthday").getDate().toString());
        // a partial after leaves absent columns untouched, unknown columns are ignored
        Assertions.assertFalse(values.containsKey("score"));
        Assertions.assertFalse(values.containsKey("unknown"));
    }

    @Test
    public void testKeysWinOverAfter() {
        final ChangeRecordMutationConverter converter = new ChangeRecordMutationConverter();
        final Mutation mutation = converter.convert(TABLE, envelope(ChangeRecord.Op.INSERT,
                "{\"id\":\"u1\",\"seq\":1}", "{\"id\":\"other\",\"seq\":1,\"name\":\"a\"}", "ff/1"));
        Assertions.assertEquals("u1", mutation.asMap().get("id").getString());
    }

    @Test
    public void testDeleteUsesPrimaryKeyOrder() {
        final ChangeRecordMutationConverter converter = new ChangeRecordMutationConverter();
        // keys json in the "wrong" order: the key parts must follow the table's primary key
        final Mutation mutation = converter.convert(TABLE, envelope(ChangeRecord.Op.DELETE,
                "{\"seq\":3,\"id\":\"u1\"}", null, "ff/2"));
        Assertions.assertEquals(Mutation.Op.DELETE, mutation.getOperation());
        final List<Object> parts = new java.util.ArrayList<>();
        mutation.getKeySet().getKeys().iterator().next().getParts().forEach(parts::add);
        Assertions.assertEquals(List.of("u1", 3L), parts);

        Assertions.assertThrows(IllegalArgumentException.class, () -> converter.convert(TABLE, envelope(ChangeRecord.Op.DELETE,
                "{\"id\":\"u1\"}", null, "ff/2")));
    }

    @Test
    public void testControlRecordsHaveNoMutation() {
        final ChangeRecordMutationConverter converter = new ChangeRecordMutationConverter();
        final Map<String, Object> truncate = envelope(ChangeRecord.Op.TRUNCATE, null, null, "ff/3");
        Assertions.assertNull(converter.convert(TABLE, truncate));
        Assertions.assertFalse(ChangeRecordMutationConverter.isApplicable(ChangeRecord.Op.SCHEMA));
        Assertions.assertThrows(IllegalArgumentException.class, () -> converter.convert(TABLE,
                envelope(ChangeRecord.Op.INSERT, null, "{\"name\":\"a\"}", "ff/1")));
    }

    @Test
    public void testCollapse() {
        final Map<String, Object> first = envelope(ChangeRecord.Op.INSERT, "{\"id\":\"u1\",\"seq\":1}", "{\"name\":\"a\",\"active\":true}", "ff/1");
        final Map<String, Object> second = envelope(ChangeRecord.Op.UPDATE, "{\"id\":\"u1\",\"seq\":1}", "{\"name\":\"b\"}", "ff/2");
        // first seen after u1's INSERT but before its UPDATE: u1 (first appearance ff/1) stays ahead
        final Map<String, Object> other = envelope(ChangeRecord.Op.INSERT, "{\"id\":\"u2\",\"seq\":1}", "{\"name\":\"c\"}", "ff/1/5");
        final Map<String, Object> control = envelope(ChangeRecord.Op.SCHEMA, null, null, "ff");
        final List<Map<String, Object>> collapsed = ChangeRecordMutationConverter.collapse(List.of(second, control, first, other));
        Assertions.assertEquals(2, collapsed.size());
        // ordered by each key's first appearance, after values merged in sequence order
        Assertions.assertEquals(ChangeRecord.Op.UPDATE.getId(), collapsed.get(0).get(ChangeRecord.FIELD_OP));
        Assertions.assertEquals("{\"name\":\"b\",\"active\":true}", collapsed.get(0).get(ChangeRecord.FIELD_AFTER));
        Assertions.assertSame(other, collapsed.get(1));

        // a DELETE discards what came before, an INSERT after a DELETE starts fresh
        final Map<String, Object> delete = envelope(ChangeRecord.Op.DELETE, "{\"id\":\"u1\",\"seq\":1}", null, "ff/3");
        Assertions.assertSame(delete, ChangeRecordMutationConverter.collapse(List.of(first, second, delete)).getFirst());
        final Map<String, Object> again = envelope(ChangeRecord.Op.INSERT, "{\"id\":\"u1\",\"seq\":1}", "{\"name\":\"z\"}", "ff/4");
        final List<Map<String, Object>> reinserted = ChangeRecordMutationConverter.collapse(List.of(first, delete, again));
        Assertions.assertEquals(1, reinserted.size());
        Assertions.assertSame(again, reinserted.getFirst());
    }

    @Test
    public void testKeyParts() {
        Assertions.assertEquals(5L, ChangeRecordMutationConverter.toKeyPart(SCHEMA.getField("seq"), com.google.gson.JsonParser.parseString("5")));
        Assertions.assertEquals("x", ChangeRecordMutationConverter.toKeyPart(SCHEMA.getField("id"), com.google.gson.JsonParser.parseString("\"x\"")));
        Assertions.assertEquals("2020-01-02", ChangeRecordMutationConverter.toKeyPart(SCHEMA.getField("birthday"), com.google.gson.JsonParser.parseString("\"2020-01-02\"")).toString());
        Assertions.assertNull(ChangeRecordMutationConverter.toKeyPart(SCHEMA.getField("id"), null));
    }

    /** Native Spanner UUID columns (spannerType=UUID on a STRING field) bind as UUID keys and values. */
    @Test
    public void testUuidColumns() {
        final String uuid = "550e8400-e29b-41d4-a716-446655440000";
        final Schema schema = Schema.builder()
                .addField(Schema.Field.of("id", Schema.FieldType.STRING).withOptions(RowSchemaUtil.createSpannerTypeOptions("UUID")))
                .addField(Schema.Field.of("refs", Schema.FieldType.array(Schema.FieldType.STRING).withNullable(true)).withOptions(RowSchemaUtil.createSpannerTypeOptions("UUID")))
                .addNullableField("name", Schema.FieldType.STRING)
                .build();
        final ChangeRecordMutationConverter.TableSchema table = new ChangeRecordMutationConverter.TableSchema("Docs", schema, List.of("id"));
        final ChangeRecordMutationConverter converter = new ChangeRecordMutationConverter();

        Assertions.assertEquals(UUID.fromString(uuid), ChangeRecordMutationConverter.toKeyPart(schema.getField("id"), com.google.gson.JsonParser.parseString("\"" + uuid + "\"")));

        final Mutation upsert = converter.convert(table, envelope(ChangeRecord.Op.UPDATE,
                "{\"id\":\"" + uuid + "\"}", "{\"name\":\"a\",\"refs\":[\"" + uuid + "\"]}", "ff/1"));
        final Map<String, Value> values = upsert.asMap();
        Assertions.assertEquals(Value.uuid(UUID.fromString(uuid)), values.get("id"));
        Assertions.assertEquals(Value.uuidArray(List.of(UUID.fromString(uuid))), values.get("refs"));
        Assertions.assertEquals(Value.string("a"), values.get("name"));

        final Mutation delete = converter.convert(table, envelope(ChangeRecord.Op.DELETE, "{\"id\":\"" + uuid + "\"}", null, "ff/2"));
        Assertions.assertEquals(List.of(UUID.fromString(uuid)), delete.getKeySet().getKeys().iterator().next().getParts());
    }

}
