package com.mercari.solution.util.pipeline.lookup.source;

import com.google.datastore.v1.ArrayValue;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Value;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.Query2;
import com.mercari.solution.util.pipeline.lookup.LookupBatch;
import com.mercari.solution.util.pipeline.lookup.LookupKey;
import com.mercari.solution.util.pipeline.lookup.LookupRequest;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;

/**
 * Hermetic tests for {@link DatastoreLookupSource}: table schema construction,
 * value decoding, the point-only key contract, and DoFn-style serialization.
 * The end-to-end lookup-join against the Datastore emulator is covered by
 * {@code DatastoreQueryLookupIT}.
 */
public class DatastoreLookupSourceTest {

    private static final Instant TIMESTAMP = Instant.parse("2026-01-01T00:00:00Z");

    private static DatastoreLookupSource source() {
        return DatastoreLookupSource.builder()
                .withName("ds")
                .withProjectId("test-project")
                // Never reached in these tests: clients are created lazily and
                // no lookup is executed.
                .withEmulatorHost("localhost:1")
                .withTable(DatastoreLookupSource.table()
                        .withName("users")
                        .withKind("Users")
                        .withField("name", Schema.FieldType.STRING)
                        .withField("age", Schema.FieldType.INT64)
                        .build())
                .build();
    }

    @Test
    public void testTableSchemaAndKeyCandidates() {
        final DatastoreLookupSource source = source();

        final Schema schema = source.tableSchemas().get("users");
        Assertions.assertNotNull(schema);
        // The key column comes first, typed by keyType.
        Assertions.assertEquals("__name__", schema.getField(0).getName());
        Assertions.assertEquals(Schema.Type.string, schema.getField(0).getFieldType().getType());
        Assertions.assertEquals(3, schema.countFields());

        final List<LookupKey> keys = source.keyCandidates("users");
        Assertions.assertEquals(1, keys.size());
        Assertions.assertEquals(List.of("__name__"), keys.get(0).columns());
        Assertions.assertFalse(source.supportsKeyPrefixLookup());
    }

    @Test
    public void testNumericKeyAndKeyFieldDedup() {
        final DatastoreLookupSource source = DatastoreLookupSource.builder()
                .withName("ds")
                .withProjectId("test-project")
                .withTable(DatastoreLookupSource.table()
                        .withName("items")
                        .withKeyField("itemId")
                        .withNumericKey(true)
                        // Redeclaring the key column in fields must not duplicate it.
                        .withField("itemId", Schema.FieldType.STRING)
                        .withField("title", Schema.FieldType.STRING)
                        .build())
                .build();
        final Schema schema = source.tableSchemas().get("items");
        Assertions.assertEquals(2, schema.countFields());
        Assertions.assertEquals("itemId", schema.getField(0).getName());
        Assertions.assertEquals(Schema.Type.int64, schema.getField(0).getFieldType().getType());
    }

    @Test
    public void testRangeLookupRejected() {
        final DatastoreLookupSource source = source();
        final LookupBatch range = LookupBatch.of(List.of(
                LookupRequest.range(List.of(), "a", true, "b", true)));
        final IllegalStateException e = Assertions.assertThrows(IllegalStateException.class,
                () -> source.lookup("users", null, range, null));
        Assertions.assertTrue(e.getMessage().contains("point equality"), e.getMessage());
    }

    @Test
    public void testDecodeValues() {
        Assertions.assertEquals("a", DatastoreLookupSource.decodeValue(
                string("a"), Schema.FieldType.STRING));
        Assertions.assertEquals("1", DatastoreLookupSource.decodeValue(
                integer(1L), Schema.FieldType.STRING));
        Assertions.assertEquals(1L, DatastoreLookupSource.decodeValue(
                integer(1L), Schema.FieldType.INT64));
        Assertions.assertEquals(1, DatastoreLookupSource.decodeValue(
                integer(1L), Schema.FieldType.INT32));
        // float64 surfaces as Double, and INTEGER values widen.
        Assertions.assertEquals(1.5D, DatastoreLookupSource.decodeValue(
                Value.newBuilder().setDoubleValue(1.5D).build(), Schema.FieldType.FLOAT64));
        Assertions.assertEquals(2.0D, DatastoreLookupSource.decodeValue(
                integer(2L), Schema.FieldType.FLOAT64));
        Assertions.assertEquals(Boolean.TRUE, DatastoreLookupSource.decodeValue(
                Value.newBuilder().setBooleanValue(true).build(), Schema.FieldType.BOOLEAN));

        // TIMESTAMP → Calcite epoch millis; DATE → epoch days (from string or timestamp).
        final Value ts = Value.newBuilder().setTimestampValue(
                Timestamp.newBuilder().setSeconds(1735689600L).setNanos(123_000_000)).build();
        Assertions.assertEquals(1735689600_123L, DatastoreLookupSource.decodeValue(
                ts, Schema.FieldType.TIMESTAMP));
        Assertions.assertEquals((int) java.time.LocalDate.parse("2025-01-01").toEpochDay(),
                DatastoreLookupSource.decodeValue(string("2025-01-01"), Schema.FieldType.DATE));
        Assertions.assertEquals((int) java.time.LocalDate.parse("2025-01-01").toEpochDay(),
                DatastoreLookupSource.decodeValue(ts, Schema.FieldType.DATE));

        // BYTES → Avatica ByteString.
        final Object bytes = DatastoreLookupSource.decodeValue(
                Value.newBuilder().setBlobValue(ByteString.copyFromUtf8("xy")).build(),
                Schema.FieldType.BYTES);
        Assertions.assertArrayEquals("xy".getBytes(),
                ((org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.avatica.util.ByteString) bytes)
                        .getBytes());

        // ARRAY → List of decoded elements.
        final Value array = Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                .addValues(integer(1L)).addValues(integer(2L))).build();
        Assertions.assertEquals(List.of(1L, 2L), DatastoreLookupSource.decodeValue(
                array, Schema.FieldType.array(Schema.FieldType.INT64)));

        // Embedded entity → json column.
        final Value entity = Value.newBuilder().setEntityValue(Entity.newBuilder()
                .putProperties("city", string("tokyo"))
                .putProperties("zip", integer(100L))).build();
        Assertions.assertEquals("{\"city\":\"tokyo\",\"zip\":100}",
                DatastoreLookupSource.decodeValue(entity, Schema.FieldType.JSON));

        // Key reference as string.
        final Value key = Value.newBuilder().setKeyValue(Key.newBuilder()
                .addPath(Key.PathElement.newBuilder().setKind("Users").setName("u1"))).build();
        Assertions.assertEquals("u1",
                DatastoreLookupSource.decodeValue(key, Schema.FieldType.STRING));

        // NULL / absent → null.
        Assertions.assertNull(DatastoreLookupSource.decodeValue(null, Schema.FieldType.STRING));
        Assertions.assertNull(DatastoreLookupSource.decodeValue(
                Value.newBuilder().setNullValue(com.google.protobuf.NullValue.NULL_VALUE).build(),
                Schema.FieldType.STRING));

        // Declared type incompatible with the stored value → explanatory error.
        Assertions.assertThrows(IllegalStateException.class, () ->
                DatastoreLookupSource.decodeValue(string("a"), Schema.FieldType.INT64));
    }

    @Test
    public void testSerializedRoundTripLikeDoFn() throws Exception {
        // The Query2 instance must survive Java serialization (DoFn shipping):
        // table metadata comes from configuration, planning needs no connectivity.
        final Query2 query = Query2.builder()
                .withInput("INPUT", Schema.of(List.of(
                        Schema.Field.of("userId", Schema.FieldType.STRING))))
                .withSource(source())
                .withSql("""
                        SELECT i.userId AS userId, u.name AS name, u.age AS age
                        FROM INPUT AS i
                        JOIN ds.users AS u ON u.__name__ = i.userId
                        """)
                .build();

        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(query);
        }
        final Query2 restored;
        try (ObjectInputStream in = new ObjectInputStream(
                new ByteArrayInputStream(bytes.toByteArray()))) {
            restored = (Query2) in.readObject();
        }

        restored.setup();
        try {
            // No input rows → no lookup RPC; verifies the compiled plan runs.
            final List<MElement> outputs = restored.execute(List.of(), TIMESTAMP);
            Assertions.assertTrue(outputs.isEmpty());
        } finally {
            restored.teardown();
        }
    }

    @Test
    public void testUnsupportedColumnTypeRejectedAtConstruction() {
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                DatastoreLookupSource.table()
                        .withName("users")
                        .withField("nested", Schema.FieldType.element(List.of(
                                Schema.Field.of("a", Schema.FieldType.STRING),
                                Schema.Field.of("b", Schema.FieldType.STRING))))
                        .build());
    }

    private static Value string(String value) {
        return Value.newBuilder().setStringValue(value).build();
    }

    private static Value integer(long value) {
        return Value.newBuilder().setIntegerValue(value).build();
    }
}
