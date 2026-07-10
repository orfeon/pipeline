package com.mercari.solution.util.pipeline.lookup.source;

import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.Query2;
import com.mercari.solution.util.pipeline.lookup.LookupBatch;
import com.mercari.solution.util.pipeline.lookup.LookupRequest;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BufferLookupSourceTest {

    private static final Instant T1 = Instant.parse("2025-05-01T00:00:01Z");
    private static final Instant T2 = Instant.parse("2025-05-01T00:00:02Z");
    private static final Instant T3 = Instant.parse("2025-05-01T00:00:03Z");

    private static Schema inputSchema() {
        return Schema.of(List.of(
                Schema.Field.of("userId", Schema.FieldType.STRING),
                Schema.Field.of("category", Schema.FieldType.STRING),
                Schema.Field.of("amount", Schema.FieldType.INT64)));
    }

    private static MElement input(String userId, String category, long amount, Instant timestamp) {
        final Map<String, Object> values = new HashMap<>();
        values.put("userId", userId);
        values.put("category", category);
        values.put("amount", amount);
        return MElement.of(values, timestamp);
    }

    private static final List<String> ALL_FIELDS = List.of("userId", "category", "amount");

    private static MElement bufferElement(String userId, String category, long amount,
            Instant timestamp, String inputName) {
        return BufferLookupSource.createBufferElement(
                input(userId, category, amount, timestamp), timestamp, inputName, ALL_FIELDS);
    }

    private static BufferLookupSource source() {
        return BufferLookupSource.builder()
                .withName("buf")
                .withTable("history")
                .withGroupFields(List.of("userId"))
                .withInputSchema(inputSchema())
                .build();
    }

    @Test
    public void testLateralAggregateOverBuffer() {
        final BufferLookupSource source = source();
        final String sql = """
                SELECT
                  i.userId AS userId,
                  s.cnt AS cnt,
                  s.total AS total
                FROM INPUT AS i
                JOIN LATERAL (
                  SELECT COUNT(*) AS cnt, SUM(b.amount) AS total
                  FROM buf.history AS b
                  WHERE b.userId = i.userId
                ) AS s ON TRUE
                """;
        final Query2 query = Query2.builder()
                .withInput("INPUT", inputSchema())
                .withSource(source)
                .withSql(sql)
                .build();

        // The plan referenced the buffer's amount and userId columns.
        Assertions.assertTrue(source.referencedColumns().contains("amount"));
        Assertions.assertTrue(source.referencedColumns().contains("userId"));

        query.setup();
        try {
            final MElement current = input("u1", "a", 30L, T3);
            final MElement currentBuffer = bufferElement("u1", "a", 30L, T3, "in");
            final List<MElement> visible = new ArrayList<>(List.of(
                    bufferElement("u1", "a", 10L, T1, "in"),
                    bufferElement("u1", "b", 20L, T2, "in")));

            // excluding the current element
            source.setData(visible, currentBuffer);
            List<MElement> outputs = query.execute(current, T3);
            Assertions.assertEquals(1, outputs.size());
            Assertions.assertEquals(2L, outputs.get(0).getAsLong("cnt"));
            Assertions.assertEquals(30L, outputs.get(0).getAsLong("total"));

            // including the current element
            visible.add(currentBuffer);
            source.setData(visible, currentBuffer);
            outputs = query.execute(current, T3);
            Assertions.assertEquals(3L, outputs.get(0).getAsLong("cnt"));
            Assertions.assertEquals(60L, outputs.get(0).getAsLong("total"));

            // the empty buffer: the global aggregate still yields one row
            source.setData(List.of(), currentBuffer);
            outputs = query.execute(current, T3);
            Assertions.assertEquals(0L, outputs.get(0).getAsLong("cnt"));
            Assertions.assertNull(outputs.get(0).getPrimitiveValue("total"));
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testFanOutJoinWithSyntheticColumns() {
        final BufferLookupSource source = source();
        final String sql = """
                SELECT
                  i.userId AS userId,
                  b.amount AS amount,
                  b.__input AS src,
                  b.__timestamp AS ts
                FROM INPUT AS i
                JOIN buf.history AS b
                ON b.userId = i.userId
                """;
        final Query2 query = Query2.builder()
                .withInput("INPUT", inputSchema())
                .withSource(source)
                .withSql(sql)
                .build();

        query.setup();
        try {
            final MElement current = input("u1", "a", 30L, T3);
            final MElement currentBuffer = bufferElement("u1", "a", 30L, T3, "purchases");
            source.setData(List.of(
                    bufferElement("u1", "a", 10L, T1, "views"),
                    bufferElement("u1", "b", 20L, T2, "purchases")), currentBuffer);

            final List<MElement> outputs = query.execute(current, T3);
            Assertions.assertEquals(2, outputs.size());
            final Map<Long, MElement> byAmount = new HashMap<>();
            for (final MElement output : outputs) {
                byAmount.put(output.getAsLong("amount"), output);
            }
            Assertions.assertEquals("views", byAmount.get(10L).getAsString("src"));
            Assertions.assertEquals("purchases", byAmount.get(20L).getAsString("src"));
            // __timestamp carries the buffered element's event time (micros primitive)
            Assertions.assertEquals(T1.getMillis() * 1000L, byAmount.get(10L).getAsLong("ts"));
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testTimestampRangeLookup() {
        final BufferLookupSource source = source();
        // Literal bounds on the __timestamp key column: prefix + bounded range.
        final String sql = """
                SELECT
                  i.userId AS userId,
                  s.cnt AS cnt
                FROM INPUT AS i
                JOIN LATERAL (
                  SELECT COUNT(*) AS cnt
                  FROM buf.history AS b
                  WHERE b.userId = i.userId
                    AND b.__timestamp >= TIMESTAMP '2025-05-01 00:00:02'
                    AND b.__timestamp <= TIMESTAMP '2025-05-01 00:00:03'
                ) AS s ON TRUE
                """;
        final Query2 query = Query2.builder()
                .withInput("INPUT", inputSchema())
                .withSource(source)
                .withSql(sql)
                .build();

        query.setup();
        try {
            final MElement current = input("u1", "a", 30L, T3);
            final MElement currentBuffer = bufferElement("u1", "a", 30L, T3, "in");
            source.setData(List.of(
                    bufferElement("u1", "a", 10L, T1, "in"),
                    bufferElement("u1", "b", 20L, T2, "in"),
                    currentBuffer), currentBuffer);

            final List<MElement> outputs = query.execute(current, T3);
            Assertions.assertEquals(1, outputs.size());
            // T2 and T3 fall in the range, T1 does not
            Assertions.assertEquals(2L, outputs.get(0).getAsLong("cnt"));
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testWrongKeyCorrelationRejectedAtPlanning() {
        final BufferLookupSource source = source();
        // Correlates the buffer's key to a different input column: the buffer
        // only holds the current element's own group, so this must fail at
        // planning with an explanatory message, not return wrong results.
        final String sql = """
                SELECT i.userId AS userId, s.cnt AS cnt
                FROM INPUT AS i
                JOIN LATERAL (
                  SELECT COUNT(*) AS cnt
                  FROM buf.history AS b
                  WHERE b.userId = i.category
                ) AS s ON TRUE
                """;
        final Throwable thrown = Assertions.assertThrows(Throwable.class, () -> Query2.builder()
                .withInput("INPUT", inputSchema())
                .withSource(source)
                .withSql(sql)
                .build());
        Assertions.assertTrue(messageChain(thrown).contains("must be joined to the input column"),
                "unexpected error: " + messageChain(thrown));
    }

    @Test
    public void testPartialGroupKeyRejectedAtPlanning() {
        final BufferLookupSource source = BufferLookupSource.builder()
                .withName("buf")
                .withTable("history")
                .withGroupFields(List.of("userId", "category"))
                .withInputSchema(inputSchema())
                .build();
        // Only the first of two groupFields is constrained: a prefix lookup
        // would silently miss the other categories' data (state only holds the
        // current element's full group), so it must be rejected.
        final String sql = """
                SELECT i.userId AS userId, s.cnt AS cnt
                FROM INPUT AS i
                JOIN LATERAL (
                  SELECT COUNT(*) AS cnt
                  FROM buf.history AS b
                  WHERE b.userId = i.userId
                ) AS s ON TRUE
                """;
        final Throwable thrown = Assertions.assertThrows(Throwable.class, () -> Query2.builder()
                .withInput("INPUT", inputSchema())
                .withSource(source)
                .withSql(sql)
                .build());
        Assertions.assertTrue(messageChain(thrown).contains("requires equality on all"),
                "unexpected error: " + messageChain(thrown));
    }

    @Test
    public void testRuntimeKeyAffinityBackstop() {
        final BufferLookupSource source = source();
        source.setup();
        try {
            final MElement currentBuffer = bufferElement("u1", "a", 30L, T3, "in");
            source.setData(List.of(bufferElement("u1", "a", 10L, T1, "in")), currentBuffer);

            // own key: answered
            final Iterable<Object[]> rows = source.lookup("history", null,
                    LookupBatch.of(List.of(LookupRequest.point(List.of("u1", T1.getMillis())))),
                    null);
            Assertions.assertEquals(1, ((List<Object[]>) rows).size());

            // another group's key: rejected loudly (never silently empty)
            final IllegalStateException thrown = Assertions.assertThrows(
                    IllegalStateException.class,
                    () -> source.lookup("history", null,
                            LookupBatch.of(List.of(
                                    LookupRequest.point(List.of("u2", T1.getMillis())))),
                            null));
            Assertions.assertTrue(thrown.getMessage().contains("own group key"));
        } finally {
            source.close();
        }
    }

    @Test
    public void testReservedFieldNameRejected() {
        final Schema colliding = Schema.of(List.of(
                Schema.Field.of("userId", Schema.FieldType.STRING),
                Schema.Field.of("__timestamp", Schema.FieldType.TIMESTAMP)));
        final IllegalArgumentException thrown = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> BufferLookupSource.builder()
                        .withName("buf")
                        .withTable("history")
                        .withGroupFields(List.of("userId"))
                        .withInputSchema(colliding)
                        .build());
        Assertions.assertTrue(thrown.getMessage().contains("__timestamp"));
    }

    @Test
    public void testUnstoredColumnsReadAsNull() {
        final BufferLookupSource source = source();
        final String sql = """
                SELECT i.userId AS userId, s.total AS total
                FROM INPUT AS i
                JOIN LATERAL (
                  SELECT SUM(b.amount) AS total
                  FROM buf.history AS b
                  WHERE b.userId = i.userId
                ) AS s ON TRUE
                """;
        final Query2 query = Query2.builder()
                .withInput("INPUT", inputSchema())
                .withSource(source)
                .withSql(sql)
                .build();
        // The SQL references only userId/amount — category may be dropped from
        // the stored fields without affecting results.
        Assertions.assertFalse(source.referencedColumns().contains("category"));

        query.setup();
        try {
            final MElement current = input("u1", "a", 30L, T3);
            final List<String> narrowed = List.of("userId", "amount");
            final MElement currentBuffer = BufferLookupSource.createBufferElement(
                    current, T3, "in", narrowed);
            source.setData(List.of(
                    BufferLookupSource.createBufferElement(
                            input("u1", "a", 10L, T1), T1, "in", narrowed)), currentBuffer);
            final List<MElement> outputs = query.execute(current, T3);
            Assertions.assertEquals(10L, outputs.get(0).getAsLong("total"));
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testSerializedRoundTripLikeDoFn() throws Exception {
        final BufferLookupSource source = source();
        final String sql = """
                SELECT i.userId AS userId, s.cnt AS cnt
                FROM INPUT AS i
                JOIN LATERAL (
                  SELECT COUNT(*) AS cnt
                  FROM buf.history AS b
                  WHERE b.userId = i.userId
                ) AS s ON TRUE
                """;
        final Query2 query = Query2.builder()
                .withInput("INPUT", inputSchema())
                .withSource(source)
                .withSql(sql)
                .build();

        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (final ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(query);
        }
        final Query2 restored;
        try (final ObjectInputStream in = new ObjectInputStream(
                new ByteArrayInputStream(bytes.toByteArray()))) {
            restored = (Query2) in.readObject();
        }

        restored.setup();
        try {
            BufferLookupSource restoredSource = null;
            for (final var restoredSourceCandidate : restored.getSources()) {
                if (restoredSourceCandidate instanceof BufferLookupSource buffer) {
                    restoredSource = buffer;
                }
            }
            Assertions.assertNotNull(restoredSource);
            // referenced columns survived serialization
            Assertions.assertTrue(restoredSource.referencedColumns().contains("userId"));

            final MElement current = input("u1", "a", 30L, T3);
            final MElement currentBuffer = bufferElement("u1", "a", 30L, T3, "in");
            restoredSource.setData(List.of(
                    bufferElement("u1", "a", 10L, T1, "in"),
                    bufferElement("u1", "b", 20L, T2, "in")), currentBuffer);
            final List<MElement> outputs = restored.execute(current, T3);
            Assertions.assertEquals(1, outputs.size());
            Assertions.assertEquals(2L, outputs.get(0).getAsLong("cnt"));
        } finally {
            restored.teardown();
        }
    }

    private static String messageChain(Throwable throwable) {
        final StringBuilder sb = new StringBuilder();
        for (Throwable t = throwable; t != null; t = t.getCause()) {
            sb.append(t.getMessage()).append(" | ");
        }
        return sb.toString();
    }
}
