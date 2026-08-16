package com.mercari.solution.util.domain.db;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mercari.solution.module.MElement;
import com.mercari.solution.util.pipeline.cdc.ChangeRecord;
import com.mercari.solution.util.pipeline.cdc.PostgresChangeCapture;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for the pgoutput binary message decoder with synthetic protocol messages
 * (message formats per the PostgreSQL logical streaming replication protocol docs),
 * and for the normalization of decoded events into the unified envelope.
 */
public class PgOutputTest {

    private static final int REL_ID = 100;
    private static final long UNIX_MICROS = 1700000000000000L; // 2023-11-14T22:13:20Z
    private static final long PG_MICROS = UNIX_MICROS - 946684800000000L;

    private static byte[] beginMessage(final long commitLsn, final long xid) {
        return message(out -> {
            out.writeByte('B');
            out.writeLong(commitLsn);
            out.writeLong(PG_MICROS);
            out.writeInt((int) xid);
        });
    }

    private static byte[] commitMessage(final long commitLsn) {
        return message(out -> {
            out.writeByte('C');
            out.writeByte(0);
            out.writeLong(commitLsn);
            out.writeLong(commitLsn + 8);
            out.writeLong(PG_MICROS);
        });
    }

    private record TestColumn(boolean key, String name, int typeOid) { }

    private static byte[] relationMessage(final int relationId, final String namespace, final String name, final TestColumn... columns) {
        return message(out -> {
            out.writeByte('R');
            out.writeInt(relationId);
            writeString(out, namespace);
            writeString(out, name);
            out.writeByte('d'); // replica identity
            out.writeShort(columns.length);
            for(final TestColumn column : columns) {
                out.writeByte(column.key() ? 1 : 0);
                writeString(out, column.name());
                out.writeInt(column.typeOid());
                out.writeInt(-1); // type modifier
            }
        });
    }

    private interface TupleWriter {
        void write(DataOutputStream out) throws IOException;
    }

    private static TupleWriter binaryInt4(final int value) {
        return out -> { out.writeByte('b'); out.writeInt(4); out.writeInt(value); };
    }

    private static TupleWriter binaryInt8(final long value) {
        return out -> { out.writeByte('b'); out.writeInt(8); out.writeLong(value); };
    }

    private static TupleWriter binaryBool(final boolean value) {
        return out -> { out.writeByte('b'); out.writeInt(1); out.writeByte(value ? 1 : 0); };
    }

    private static TupleWriter binaryText(final String value) {
        return out -> {
            final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            out.writeByte('b'); out.writeInt(bytes.length); out.write(bytes);
        };
    }

    private static TupleWriter binaryTimestamptz(final long unixMicros) {
        return out -> { out.writeByte('b'); out.writeInt(8); out.writeLong(unixMicros - 946684800000000L); };
    }

    private static TupleWriter textValue(final String value) {
        return out -> {
            final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            out.writeByte('t'); out.writeInt(bytes.length); out.write(bytes);
        };
    }

    private static TupleWriter nullValue() {
        return out -> out.writeByte('n');
    }

    private static TupleWriter unchangedValue() {
        return out -> out.writeByte('u');
    }

    private static void writeTuple(final DataOutputStream out, final TupleWriter... values) throws IOException {
        out.writeShort(values.length);
        for(final TupleWriter value : values) {
            value.write(out);
        }
    }

    private static byte[] insertMessage(final int relationId, final TupleWriter... values) {
        return message(out -> {
            out.writeByte('I');
            out.writeInt(relationId);
            out.writeByte('N');
            writeTuple(out, values);
        });
    }

    private static byte[] updateMessage(final int relationId, final TupleWriter[] oldValues, final TupleWriter[] newValues) {
        return message(out -> {
            out.writeByte('U');
            out.writeInt(relationId);
            if(oldValues != null) {
                out.writeByte('O');
                writeTuple(out, oldValues);
            }
            out.writeByte('N');
            writeTuple(out, newValues);
        });
    }

    private static byte[] deleteMessage(final int relationId, final TupleWriter... oldValues) {
        return message(out -> {
            out.writeByte('D');
            out.writeInt(relationId);
            out.writeByte('K');
            writeTuple(out, oldValues);
        });
    }

    private static byte[] truncateMessage(final int... relationIds) {
        return message(out -> {
            out.writeByte('T');
            out.writeInt(relationIds.length);
            out.writeByte(0);
            for(final int relationId : relationIds) {
                out.writeInt(relationId);
            }
        });
    }

    private interface MessageWriter {
        void write(DataOutputStream out) throws IOException;
    }

    private static byte[] message(final MessageWriter writer) {
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try(final DataOutputStream out = new DataOutputStream(bos)) {
            writer.write(out);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
        return bos.toByteArray();
    }

    private static void writeString(final DataOutputStream out, final String value) throws IOException {
        out.write(value.getBytes(StandardCharsets.UTF_8));
        out.writeByte(0);
    }

    private static List<PgOutput.ChangeEvent> decode(final PgOutput.Decoder decoder, final byte[] messageBytes, final long lsn) {
        return decoder.decode(ByteBuffer.wrap(messageBytes), lsn);
    }

    @Test
    public void testDecodeInsertUpdateDelete() {
        final PgOutput.Decoder decoder = new PgOutput.Decoder();

        Assertions.assertTrue(decode(decoder, beginMessage(5000L, 42L), 4000L).isEmpty());
        Assertions.assertTrue(decode(decoder, relationMessage(REL_ID, "public", "items",
                new TestColumn(true, "id", 23),      // int4 key
                new TestColumn(false, "name", 25),   // text
                new TestColumn(false, "active", 16), // bool
                new TestColumn(false, "updated", 1184) // timestamptz
        ), 4001L).isEmpty());

        final List<PgOutput.ChangeEvent> inserts = decode(decoder, insertMessage(REL_ID,
                binaryInt4(1), binaryText("hello"), binaryBool(true), binaryTimestamptz(UNIX_MICROS)), 4002L);
        Assertions.assertEquals(1, inserts.size());
        final PgOutput.ChangeEvent insert = inserts.get(0);
        Assertions.assertEquals("INSERT", insert.op);
        Assertions.assertEquals("public", insert.schema);
        Assertions.assertEquals("items", insert.table);
        Assertions.assertEquals(4002L, insert.lsn);
        Assertions.assertEquals(5000L, insert.commitLsn);
        Assertions.assertEquals(UNIX_MICROS, insert.commitTimestampMicros);
        Assertions.assertEquals(42L, insert.transactionId);
        Assertions.assertEquals(0L, insert.sequence);
        Assertions.assertEquals("{\"id\":1}", insert.keysJson);
        Assertions.assertNull(insert.oldValuesJson);
        final JsonObject newValues = JsonParser.parseString(insert.newValuesJson).getAsJsonObject();
        Assertions.assertEquals(1, newValues.get("id").getAsInt());
        Assertions.assertEquals("hello", newValues.get("name").getAsString());
        Assertions.assertTrue(newValues.get("active").getAsBoolean());
        Assertions.assertEquals("2023-11-14T22:13:20Z", newValues.get("updated").getAsString());

        // update without an old tuple (no key change): keys come from the new tuple
        final List<PgOutput.ChangeEvent> updates = decode(decoder, updateMessage(REL_ID,
                null,
                new TupleWriter[]{binaryInt4(1), binaryText("world"), nullValue(), binaryTimestamptz(UNIX_MICROS)}), 4003L);
        Assertions.assertEquals(1, updates.size());
        final PgOutput.ChangeEvent update = updates.get(0);
        Assertions.assertEquals("UPDATE", update.op);
        Assertions.assertEquals(1L, update.sequence);
        Assertions.assertEquals("{\"id\":1}", update.keysJson);
        Assertions.assertNull(update.oldValuesJson);
        final JsonObject updatedValues = JsonParser.parseString(update.newValuesJson).getAsJsonObject();
        Assertions.assertEquals("world", updatedValues.get("name").getAsString());
        Assertions.assertTrue(updatedValues.get("active").isJsonNull());

        // delete: keys come from the replica identity (key) tuple
        final List<PgOutput.ChangeEvent> deletes = decode(decoder, deleteMessage(REL_ID,
                binaryInt4(1), nullValue(), nullValue(), nullValue()), 4004L);
        Assertions.assertEquals(1, deletes.size());
        final PgOutput.ChangeEvent delete = deletes.get(0);
        Assertions.assertEquals("DELETE", delete.op);
        Assertions.assertEquals(2L, delete.sequence);
        Assertions.assertEquals("{\"id\":1}", delete.keysJson);
        Assertions.assertNull(delete.newValuesJson);

        Assertions.assertTrue(decode(decoder, commitMessage(5000L), 5000L).isEmpty());
    }

    @Test
    public void testDecodeUpdateWithOldTupleAndUnchangedToast() {
        final PgOutput.Decoder decoder = new PgOutput.Decoder();
        decode(decoder, beginMessage(6000L, 43L), 5001L);
        decode(decoder, relationMessage(REL_ID, "public", "items",
                new TestColumn(true, "id", 23),
                new TestColumn(false, "payload", 25)), 5002L);

        final List<PgOutput.ChangeEvent> events = decode(decoder, updateMessage(REL_ID,
                new TupleWriter[]{binaryInt4(7), binaryText("old")},
                new TupleWriter[]{binaryInt4(7), unchangedValue()}), 5003L);
        Assertions.assertEquals(1, events.size());
        final PgOutput.ChangeEvent event = events.get(0);
        Assertions.assertEquals("{\"id\":7}", event.keysJson);
        Assertions.assertEquals("{\"id\":7,\"payload\":\"old\"}", event.oldValuesJson);
        // the unchanged toasted value is absent from the new image, not null
        Assertions.assertEquals("{\"id\":7}", event.newValuesJson);
    }

    @Test
    public void testDecodeTextFormatValues() {
        final PgOutput.Decoder decoder = new PgOutput.Decoder();
        decode(decoder, beginMessage(7000L, 44L), 6001L);
        decode(decoder, relationMessage(REL_ID, "public", "textual",
                new TestColumn(true, "id", 20),      // int8
                new TestColumn(false, "flag", 16),   // bool
                new TestColumn(false, "price", 1700), // numeric
                new TestColumn(false, "data", 17)),   // bytea
                6002L);

        final List<PgOutput.ChangeEvent> events = decode(decoder, insertMessage(REL_ID,
                textValue("123456789012"), textValue("t"), textValue("12345.67"), textValue("\\x0102ff")), 6003L);
        final JsonObject values = JsonParser.parseString(events.get(0).newValuesJson).getAsJsonObject();
        Assertions.assertEquals(123456789012L, values.get("id").getAsLong());
        Assertions.assertTrue(values.get("flag").getAsBoolean());
        Assertions.assertEquals("12345.67", values.get("price").getAsString());
        Assertions.assertEquals("AQL/", values.get("data").getAsString()); // base64 of 0x0102ff
    }

    @Test
    public void testDecodeTruncateAndNonPublicSchema() {
        final PgOutput.Decoder decoder = new PgOutput.Decoder();
        decode(decoder, beginMessage(8000L, 45L), 7001L);
        decode(decoder, relationMessage(REL_ID, "audit", "log", new TestColumn(true, "id", 23)), 7002L);

        final List<PgOutput.ChangeEvent> events = decode(decoder, truncateMessage(REL_ID), 7003L);
        Assertions.assertEquals(1, events.size());
        Assertions.assertEquals("TRUNCATE", events.get(0).op);
        Assertions.assertEquals("audit", events.get(0).schema);
        Assertions.assertEquals("{}", events.get(0).keysJson);
    }

    @Test
    public void testKeyColumnOverrideWithFullReplicaIdentity() {
        // REPLICA IDENTITY FULL flags every column as key in the protocol; the launch-resolved
        // primary key columns pin keysJson to the actual primary key
        final PgOutput.Decoder decoder = new PgOutput.Decoder(Map.of("public.items", List.of("id")));
        decode(decoder, beginMessage(9500L, 48L), 9201L);
        decode(decoder, relationMessage(REL_ID, "public", "items",
                new TestColumn(true, "id", 23),
                new TestColumn(true, "name", 25)), 9202L);

        final PgOutput.ChangeEvent delete = decode(decoder, deleteMessage(REL_ID,
                binaryInt4(9), binaryText("gone")), 9203L).get(0);
        Assertions.assertEquals("{\"id\":9}", delete.keysJson);
        Assertions.assertEquals("{\"id\":9,\"name\":\"gone\"}", delete.oldValuesJson);

        // a relation not in the map falls back to the protocol key flags
        decode(decoder, relationMessage(REL_ID + 1, "public", "others",
                new TestColumn(true, "id", 23),
                new TestColumn(true, "name", 25)), 9204L);
        final PgOutput.ChangeEvent fallback = decode(decoder, deleteMessage(REL_ID + 1,
                binaryInt4(9), binaryText("gone")), 9205L).get(0);
        Assertions.assertEquals("{\"id\":9,\"name\":\"gone\"}", fallback.keysJson);
    }

    @Test
    public void testNormalizeToEnvelope() {
        final PgOutput.Decoder decoder = new PgOutput.Decoder();
        decode(decoder, beginMessage(0x1AB0L, 46L), 9001L);
        decode(decoder, relationMessage(REL_ID, "public", "items",
                new TestColumn(true, "id", 23),
                new TestColumn(false, "name", 25)), 9002L);
        final PgOutput.ChangeEvent event = decode(decoder, insertMessage(REL_ID,
                binaryInt4(1), binaryText("hello")), 9003L).get(0);

        final MElement element = PostgresChangeCapture.convert(event, "mydb", Instant.ofEpochMilli(UNIX_MICROS / 1000L));
        final List<Map<String, Object>> envelopes = PostgresChangeCapture.normalize(element);
        Assertions.assertEquals(1, envelopes.size());
        final Map<String, Object> envelope = envelopes.get(0);
        Assertions.assertEquals("items", envelope.get(ChangeRecord.FIELD_TABLE));
        Assertions.assertEquals(ChangeRecord.Op.INSERT.getId(), envelope.get(ChangeRecord.FIELD_OP));
        Assertions.assertEquals("{\"id\":1}", envelope.get(ChangeRecord.FIELD_KEYS));
        Assertions.assertEquals("{\"id\":1,\"name\":\"hello\"}", envelope.get(ChangeRecord.FIELD_AFTER));
        Assertions.assertEquals(UNIX_MICROS, envelope.get(ChangeRecord.FIELD_COMMIT_TIMESTAMP));
        // sequence = (commit LSN hex, change index in tx)
        Assertions.assertEquals("1ab0/0", envelope.get(ChangeRecord.FIELD_SEQUENCE));
        Assertions.assertTrue(ChangeRecord.isValidSequence((String) envelope.get(ChangeRecord.FIELD_SEQUENCE)));

        @SuppressWarnings("unchecked")
        final Map<String, Object> source = (Map<String, Object>) envelope.get(ChangeRecord.FIELD_SOURCE);
        Assertions.assertEquals("postgres", source.get(ChangeRecord.FIELD_SOURCE_PROVIDER));
        Assertions.assertEquals("mydb", source.get(ChangeRecord.FIELD_SOURCE_DATABASE));
        final JsonObject metadata = JsonParser.parseString((String) source.get(ChangeRecord.FIELD_SOURCE_METADATA)).getAsJsonObject();
        Assertions.assertEquals(9003L, metadata.get("lsn").getAsLong());
        Assertions.assertEquals(46L, metadata.get("transactionId").getAsLong());
        Assertions.assertEquals("public", metadata.get("schema").getAsString());
    }

    @Test
    public void testNormalizeSkipsTruncateAndQualifiesSchema() {
        final PgOutput.Decoder decoder = new PgOutput.Decoder();
        decode(decoder, beginMessage(9000L, 47L), 9101L);
        decode(decoder, relationMessage(REL_ID, "audit", "log", new TestColumn(true, "id", 23)), 9102L);

        final PgOutput.ChangeEvent truncate = decode(decoder, truncateMessage(REL_ID), 9103L).get(0);
        Assertions.assertTrue(PostgresChangeCapture
                .normalize(PostgresChangeCapture.convert(truncate, "mydb", Instant.now()))
                .isEmpty());

        final PgOutput.ChangeEvent insert = decode(decoder, insertMessage(REL_ID, binaryInt4(1)), 9104L).get(0);
        final Map<String, Object> envelope = PostgresChangeCapture
                .normalize(PostgresChangeCapture.convert(insert, "mydb", Instant.now()))
                .get(0);
        // non-public schema tables are schema-qualified, matching the batch tables mode tag
        Assertions.assertEquals("audit.log", envelope.get(ChangeRecord.FIELD_TABLE));
    }

}
