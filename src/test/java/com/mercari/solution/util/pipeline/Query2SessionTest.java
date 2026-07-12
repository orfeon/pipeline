package com.mercari.solution.util.pipeline;

import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.lookup.LookupBatch;
import com.mercari.solution.util.pipeline.lookup.LookupKey;
import com.mercari.solution.util.pipeline.lookup.LookupRequest;
import com.mercari.solution.util.pipeline.lookup.LookupSource;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class Query2SessionTest {

    private static final Instant TIMESTAMP = Instant.parse("2025-05-01T00:00:00Z");

    /** In-memory lookup source counting its lookup invocations. */
    private static class MemoryLookupSource extends LookupSource {

        private final Schema schema = Schema.of(List.of(
                Schema.Field.of("USER_ID", Schema.FieldType.INT64),
                Schema.Field.of("AMOUNT", Schema.FieldType.INT64)));
        private final List<Object[]> rows = List.of(
                new Object[]{1L, 10L},
                new Object[]{1L, 20L},
                new Object[]{2L, 150L});

        int lookupCount;

        MemoryLookupSource() {
            super("db");
        }

        @Override
        protected void setupInternal() {
        }

        @Override
        protected void closeInternal() {
        }

        @Override
        public Map<String, Schema> tableSchemas() {
            final Map<String, Schema> schemas = new LinkedHashMap<>();
            schemas.put("EVENTS", schema);
            return schemas;
        }

        @Override
        public List<LookupKey> keyCandidates(String table) {
            return List.of(LookupKey.primaryKey(List.of("USER_ID", "AMOUNT")));
        }

        @Override
        public boolean supportsKeyPrefixLookup() {
            return true;
        }

        @Override
        public Iterable<Object[]> lookup(String table, String indexName, LookupBatch batch,
                int[] projects) {
            lookupCount++;
            final List<Object[]> result = new ArrayList<>();
            for (final Object[] row : rows) {
                for (final LookupRequest request : batch.requests()) {
                    boolean match = true;
                    for (int i = 0; i < request.prefix().size(); i++) {
                        if (!Objects.equals(row[i], request.prefix().get(i))) {
                            match = false;
                            break;
                        }
                    }
                    if (match) {
                        result.add(project(row, projects));
                        break;
                    }
                }
            }
            return result;
        }

        private static Object[] project(Object[] row, int[] projects) {
            if (projects == null) {
                return row;
            }
            final Object[] out = new Object[projects.length];
            for (int i = 0; i < projects.length; i++) {
                out[i] = row[projects[i]];
            }
            return out;
        }
    }

    private static Schema inputSchema() {
        return Schema.of(List.of(
                Schema.Field.of("userId", Schema.FieldType.INT64)));
    }

    private static MElement input(long userId) {
        final Map<String, Object> values = new HashMap<>();
        values.put("userId", userId);
        return MElement.of(values, TIMESTAMP);
    }

    private static Query2.Builder sessionBuilder(MemoryLookupSource source) {
        return Query2.builder()
                .withInput("INPUT", inputSchema())
                .withSource(source)
                .withQuery("enriched", """
                        SELECT i.userId AS userId, s.total AS total
                        FROM INPUT AS i
                        JOIN LATERAL (
                          SELECT SUM(e.AMOUNT) AS total
                          FROM db.EVENTS AS e
                          WHERE e.USER_ID = i.userId
                        ) AS s ON TRUE
                        """, false)
                .withQuery("big", "SELECT userId, total FROM enriched WHERE total >= 100", true)
                .withQuery("small", "SELECT userId, total FROM enriched WHERE total < 100", true);
    }

    @Test
    public void testChainedStatementsShareIntermediate() {
        final MemoryLookupSource source = new MemoryLookupSource();
        final Query2 query = sessionBuilder(source).build();

        Assertions.assertEquals(List.of("big", "small"),
                new ArrayList<>(query.getOutputSchemas().keySet()));

        query.setup();
        try {
            source.lookupCount = 0;
            final Query2.SessionResult result =
                    query.executeAll(Map.of("INPUT", List.of(input(1L))), TIMESTAMP);
            // both output statements read the intermediate: the expensive
            // LATERAL fetch ran exactly once for the element
            Assertions.assertEquals(1, source.lookupCount);
            Assertions.assertEquals(0, result.outputs().get("big").size());
            Assertions.assertEquals(1, result.outputs().get("small").size());
            Assertions.assertEquals(30L, result.outputs().get("small").get(0).getAsLong("total"));

            final Query2.SessionResult result2 =
                    query.executeAll(Map.of("INPUT", List.of(input(2L))), TIMESTAMP);
            Assertions.assertEquals(1, result2.outputs().get("big").size());
            Assertions.assertEquals(150L, result2.outputs().get("big").get(0).getAsLong("total"));
            Assertions.assertEquals(0, result2.outputs().get("small").size());
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testExclusiveStopsAfterFirstNonEmptyOutput() {
        final MemoryLookupSource source = new MemoryLookupSource();
        final Query2 query = sessionBuilder(source).withExclusive(true).build();

        query.setup();
        try {
            // u2 matches 'big' → 'small' is never evaluated (no entry at all)
            final Query2.SessionResult big =
                    query.executeAll(Map.of("INPUT", List.of(input(2L))), TIMESTAMP);
            Assertions.assertEquals(1, big.outputs().get("big").size());
            Assertions.assertFalse(big.outputs().containsKey("small"));

            // u1 does not match 'big' (empty output does not stop) → 'small' runs
            final Query2.SessionResult small =
                    query.executeAll(Map.of("INPUT", List.of(input(1L))), TIMESTAMP);
            Assertions.assertEquals(0, small.outputs().get("big").size());
            Assertions.assertEquals(1, small.outputs().get("small").size());
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testStateRestoreKindEvaluation() {
        final Query2 query = Query2.builder()
                .withInput("INPUT", inputSchema())
                .withStateRestore("SELECT userId FROM INPUT")
                .withSql("SELECT userId FROM INPUT")
                .build();
        query.setup();
        try {
            final Map<String, List<MElement>> inputs = Map.of("INPUT", List.of(input(1L)));
            // restore-only: restore rows returned, no outputs evaluated
            final Query2.SessionResult restoreOnly = query.executeAll(inputs, TIMESTAMP,
                    java.util.EnumSet.of(Query2.Statement.Kind.STATE_RESTORE));
            Assertions.assertEquals(1, restoreOnly.restoreRows().size());
            Assertions.assertTrue(restoreOnly.outputs().isEmpty());
            // query-only: outputs evaluated, restore skipped
            final Query2.SessionResult queryOnly = query.executeAll(inputs, TIMESTAMP,
                    java.util.EnumSet.of(Query2.Statement.Kind.QUERY));
            Assertions.assertNull(queryOnly.restoreRows());
            Assertions.assertEquals(1, queryOnly.outputs().get("").size());
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testForwardReferenceRejected() {
        // statement order matters: a statement may only reference earlier ones
        final Throwable thrown = Assertions.assertThrows(Throwable.class, () -> Query2.builder()
                .withInput("INPUT", inputSchema())
                .withQuery("first", "SELECT userId FROM later", true)
                .withQuery("later", "SELECT userId FROM INPUT", true)
                .build());
        Assertions.assertTrue(messageChain(thrown).contains("not found"),
                "unexpected error: " + messageChain(thrown));
    }

    @Test
    public void testStatementNameCollisionRejected() {
        final Throwable thrown = Assertions.assertThrows(Throwable.class, () -> Query2.builder()
                .withInput("INPUT", inputSchema())
                .withQuery("INPUT", "SELECT userId FROM INPUT", true)
                .build());
        Assertions.assertTrue(messageChain(thrown).contains("collides"),
                "unexpected error: " + messageChain(thrown));
    }

    @Test
    public void testNoOutputStatementRejected() {
        final Throwable thrown = Assertions.assertThrows(Throwable.class, () -> Query2.builder()
                .withInput("INPUT", inputSchema())
                .withQuery("a", "SELECT userId FROM INPUT", false)
                .build());
        Assertions.assertTrue(messageChain(thrown).contains("output"),
                "unexpected error: " + messageChain(thrown));
    }

    @Test
    public void testSerializedRoundTripLikeDoFn() throws Exception {
        final MemoryLookupSource source = new MemoryLookupSource();
        final Query2 query = sessionBuilder(source).build();

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
            final Query2.SessionResult result =
                    restored.executeAll(Map.of("INPUT", List.of(input(2L))), TIMESTAMP);
            Assertions.assertEquals(1, result.outputs().get("big").size());
            Assertions.assertEquals(150L, result.outputs().get("big").get(0).getAsLong("total"));
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
