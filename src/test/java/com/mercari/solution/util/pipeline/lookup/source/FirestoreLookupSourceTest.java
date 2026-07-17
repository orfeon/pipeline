package com.mercari.solution.util.pipeline.lookup.source;

import com.google.cloud.Timestamp;
import com.google.cloud.firestore.GeoPoint;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Hermetic tests for {@link FirestoreLookupSource}: table schema construction,
 * value decoding, the point-only key contract, and DoFn-style serialization.
 * The end-to-end lookup-join against the Firestore emulator is covered by
 * {@code FirestoreQueryLookupIT}.
 */
public class FirestoreLookupSourceTest {

    private static final Instant TIMESTAMP = Instant.parse("2026-01-01T00:00:00Z");

    private static FirestoreLookupSource source() {
        return FirestoreLookupSource.builder()
                .withName("fs")
                .withProjectId("test-project")
                // Never reached in these tests: clients are created lazily and
                // no lookup is executed.
                .withEmulatorHost("localhost:1")
                .withTable(FirestoreLookupSource.table()
                        .withName("users")
                        .withCollection("Users")
                        .withField("name", Schema.FieldType.STRING)
                        .withField("age", Schema.FieldType.INT64)
                        .build())
                .build();
    }

    @Test
    public void testTableSchemaAndKeyCandidates() {
        final FirestoreLookupSource source = source();

        final Schema schema = source.tableSchemas().get("users");
        Assertions.assertNotNull(schema);
        // The document id column comes first and is always a string.
        Assertions.assertEquals("__name__", schema.getField(0).getName());
        Assertions.assertEquals(Schema.Type.string, schema.getField(0).getFieldType().getType());
        Assertions.assertEquals(3, schema.countFields());

        final List<LookupKey> keys = source.keyCandidates("users");
        Assertions.assertEquals(1, keys.size());
        Assertions.assertEquals(List.of("__name__"), keys.get(0).columns());
        Assertions.assertFalse(source.supportsKeyPrefixLookup());
    }

    @Test
    public void testRangeLookupRejected() {
        final FirestoreLookupSource source = source();
        final LookupBatch range = LookupBatch.of(List.of(
                LookupRequest.range(List.of(), "a", true, "b", true)));
        final IllegalStateException e = Assertions.assertThrows(IllegalStateException.class,
                () -> source.lookup("users", null, range, null));
        Assertions.assertTrue(e.getMessage().contains("point equality"), e.getMessage());
    }

    @Test
    public void testDecodeValues() {
        Assertions.assertEquals("a", FirestoreLookupSource.decodeValue(
                "a", Schema.FieldType.STRING));
        Assertions.assertEquals("1", FirestoreLookupSource.decodeValue(
                1L, Schema.FieldType.STRING));
        Assertions.assertEquals(1L, FirestoreLookupSource.decodeValue(
                1L, Schema.FieldType.INT64));
        Assertions.assertEquals(1, FirestoreLookupSource.decodeValue(
                1L, Schema.FieldType.INT32));
        // float64 surfaces as Double, and integer values widen.
        Assertions.assertEquals(1.5D, FirestoreLookupSource.decodeValue(
                1.5D, Schema.FieldType.FLOAT64));
        Assertions.assertEquals(2.0D, FirestoreLookupSource.decodeValue(
                2L, Schema.FieldType.FLOAT64));
        Assertions.assertEquals(Boolean.TRUE, FirestoreLookupSource.decodeValue(
                Boolean.TRUE, Schema.FieldType.BOOLEAN));

        // TIMESTAMP → Calcite epoch millis; DATE → epoch days (from string or timestamp).
        final Timestamp ts = Timestamp.ofTimeSecondsAndNanos(1735689600L, 123_000_000);
        Assertions.assertEquals(1735689600_123L, FirestoreLookupSource.decodeValue(
                ts, Schema.FieldType.TIMESTAMP));
        Assertions.assertEquals((int) java.time.LocalDate.parse("2025-01-01").toEpochDay(),
                FirestoreLookupSource.decodeValue("2025-01-01", Schema.FieldType.DATE));
        Assertions.assertEquals((int) java.time.LocalDate.parse("2025-01-01").toEpochDay(),
                FirestoreLookupSource.decodeValue(ts, Schema.FieldType.DATE));

        // ARRAY → List of decoded elements.
        Assertions.assertEquals(List.of(1L, 2L), FirestoreLookupSource.decodeValue(
                List.of(1L, 2L), Schema.FieldType.array(Schema.FieldType.INT64)));

        // Nested map → json column (insertion order preserved).
        final Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("city", "tokyo");
        nested.put("zip", 100L);
        Assertions.assertEquals("{\"city\":\"tokyo\",\"zip\":100}",
                FirestoreLookupSource.decodeValue(nested, Schema.FieldType.JSON));

        // GeoPoint → json column.
        Assertions.assertEquals("{\"latitude\":35.0,\"longitude\":139.0}",
                FirestoreLookupSource.decodeValue(
                        new GeoPoint(35.0, 139.0), Schema.FieldType.JSON));

        // Absent → null.
        Assertions.assertNull(FirestoreLookupSource.decodeValue(null, Schema.FieldType.STRING));

        // Declared type incompatible with the stored value → explanatory error.
        Assertions.assertThrows(IllegalStateException.class, () ->
                FirestoreLookupSource.decodeValue("a", Schema.FieldType.INT64));
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
                        JOIN fs.users AS u ON u.__name__ = i.userId
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
                FirestoreLookupSource.table()
                        .withName("users")
                        .withField("nested", Schema.FieldType.element(List.of(
                                Schema.Field.of("a", Schema.FieldType.STRING),
                                Schema.Field.of("b", Schema.FieldType.STRING))))
                        .build());
    }
}
