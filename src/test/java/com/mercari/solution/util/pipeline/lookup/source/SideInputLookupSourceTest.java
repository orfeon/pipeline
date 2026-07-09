package com.mercari.solution.util.pipeline.lookup.source;

import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.Query2;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SideInputLookupSourceTest {

    private static final Instant TIMESTAMP = Instant.parse("2025-05-01T00:00:00Z");

    private static Schema inputSchema() {
        return Schema.of(List.of(
                Schema.Field.of("userId", Schema.FieldType.STRING),
                Schema.Field.of("qty", Schema.FieldType.INT64)));
    }

    private static Schema usersSchema() {
        return Schema.of(List.of(
                Schema.Field.of("userId", Schema.FieldType.STRING),
                Schema.Field.of("name", Schema.FieldType.STRING),
                Schema.Field.of("score", Schema.FieldType.INT64)));
    }

    private static Schema historySchema() {
        return Schema.of(List.of(
                Schema.Field.of("userId", Schema.FieldType.STRING),
                Schema.Field.of("seq", Schema.FieldType.INT64),
                Schema.Field.of("action", Schema.FieldType.STRING)));
    }

    private static SideInputLookupSource usersSource() {
        return SideInputLookupSource.builder()
                .withName("side")
                .withTable(SideInputLookupSource.table()
                        .withName("users")
                        .withInput("users")
                        .withKeyFields(List.of("userId"))
                        .withSchema(usersSchema())
                        .build())
                .build();
    }

    private static List<MElement> usersData() {
        return List.of(
                MElement.of(Map.of("userId", "u1", "name", "alice", "score", 10L), TIMESTAMP),
                MElement.of(Map.of("userId", "u2", "name", "bob", "score", 20L), TIMESTAMP));
    }

    private static SideInputLookupSource findSideInputSource(final Query2 query) {
        return (SideInputLookupSource) query.getSources().stream()
                .filter(s -> s instanceof SideInputLookupSource)
                .findAny()
                .orElseThrow();
    }

    @Test
    public void testPointLookupJoin() {
        final Query2 query = Query2.builder()
                .withInput("INPUT", inputSchema())
                .withSource(usersSource())
                .withSql("""
                        SELECT i.userId AS userId, i.qty AS qty, u.name AS name, u.score AS score
                        FROM INPUT AS i
                        JOIN side.users AS u ON u.userId = i.userId
                        """)
                .build();

        query.setup();
        try {
            final SideInputLookupSource source = findSideInputSource(query);
            source.setData("users", usersData(), "w1");
            final List<MElement> outputs = query.execute(List.of(
                    MElement.of(Map.of("userId", "u1", "qty", 2L), TIMESTAMP),
                    MElement.of(Map.of("userId", "u3", "qty", 5L), TIMESTAMP)), TIMESTAMP);

            // u3 has no match: INNER join drops it
            Assertions.assertEquals(1, outputs.size());
            final MElement output = outputs.getFirst();
            Assertions.assertEquals("u1", output.getAsString("userId"));
            Assertions.assertEquals("alice", output.getAsString("name"));
            Assertions.assertEquals(10L, output.getAsLong("score"));
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testLeftJoinPadsNullForMissingKey() {
        final Query2 query = Query2.builder()
                .withInput("INPUT", inputSchema())
                .withSource(usersSource())
                .withSql("""
                        SELECT i.userId AS userId, u.name AS name
                        FROM INPUT AS i
                        LEFT JOIN side.users AS u ON u.userId = i.userId
                        """)
                .build();

        query.setup();
        try {
            findSideInputSource(query).setData("users", usersData(), "w1");
            final List<MElement> outputs = query.execute(List.of(
                    MElement.of(Map.of("userId", "u1", "qty", 1L), TIMESTAMP),
                    MElement.of(Map.of("userId", "u9", "qty", 1L), TIMESTAMP)), TIMESTAMP);

            Assertions.assertEquals(2, outputs.size());
            final Map<String, MElement> byUser = new HashMap<>();
            for (final MElement output : outputs) {
                byUser.put(output.getAsString("userId"), output);
            }
            Assertions.assertEquals("alice", byUser.get("u1").getAsString("name"));
            Assertions.assertNull(byUser.get("u9").getAsString("name"));
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testPrefixLookupOverCompositeKey() {
        // Equality on the leading key column only: an index-backed prefix
        // lookup (the source opts in) fanning out all rows under the prefix.
        final Query2 query = Query2.builder()
                .withInput("INPUT", inputSchema())
                .withSource(SideInputLookupSource.builder()
                        .withName("side")
                        .withTable(SideInputLookupSource.table()
                                .withName("history")
                                .withInput("history")
                                .withKeyFields(List.of("userId", "seq"))
                                .withSchema(historySchema())
                                .build())
                        .build())
                .withSql("""
                        SELECT i.userId AS userId, COUNT(*) AS cnt
                        FROM INPUT AS i
                        JOIN side.history AS h ON h.userId = i.userId
                        GROUP BY i.userId
                        """)
                .build();

        query.setup();
        try {
            findSideInputSource(query).setData("history", List.of(
                    MElement.of(Map.of("userId", "u1", "seq", 1L, "action", "view"), TIMESTAMP),
                    MElement.of(Map.of("userId", "u1", "seq", 2L, "action", "buy"), TIMESTAMP),
                    MElement.of(Map.of("userId", "u2", "seq", 1L, "action", "view"), TIMESTAMP)), "w1");
            final List<MElement> outputs = query.execute(List.of(
                    MElement.of(Map.of("userId", "u1", "qty", 1L), TIMESTAMP)), TIMESTAMP);

            Assertions.assertEquals(1, outputs.size());
            Assertions.assertEquals(2L, outputs.getFirst().getAsLong("cnt"));
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testPrefixPlusBoundedRangeLookup() {
        final Schema inputSchema = Schema.of(List.of(
                Schema.Field.of("userId", Schema.FieldType.STRING),
                Schema.Field.of("fromSeq", Schema.FieldType.INT64),
                Schema.Field.of("toSeq", Schema.FieldType.INT64)));
        final Query2 query = Query2.builder()
                .withInput("INPUT", inputSchema)
                .withSource(SideInputLookupSource.builder()
                        .withName("side")
                        .withTable(SideInputLookupSource.table()
                                .withName("history")
                                .withInput("history")
                                .withKeyFields(List.of("userId", "seq"))
                                .withSchema(historySchema())
                                .build())
                        .build())
                .withSql("""
                        SELECT i.userId AS userId, h.seq AS seq, h.action AS action
                        FROM INPUT AS i
                        JOIN side.history AS h
                          ON h.userId = i.userId AND h.seq >= i.fromSeq AND h.seq <= i.toSeq
                        """)
                .build();

        query.setup();
        try {
            findSideInputSource(query).setData("history", List.of(
                    MElement.of(Map.of("userId", "u1", "seq", 1L, "action", "view"), TIMESTAMP),
                    MElement.of(Map.of("userId", "u1", "seq", 2L, "action", "cart"), TIMESTAMP),
                    MElement.of(Map.of("userId", "u1", "seq", 3L, "action", "buy"), TIMESTAMP)), "w1");
            final List<MElement> outputs = query.execute(List.of(
                    MElement.of(Map.of("userId", "u1", "fromSeq", 2L, "toSeq", 3L), TIMESTAMP)), TIMESTAMP);

            Assertions.assertEquals(2, outputs.size());
            for (final MElement output : outputs) {
                Assertions.assertTrue(output.getAsLong("seq") >= 2L && output.getAsLong("seq") <= 3L);
            }
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testMultipleSideInputTables() {
        final Schema categoriesSchema = Schema.of(List.of(
                Schema.Field.of("categoryId", Schema.FieldType.STRING),
                Schema.Field.of("label", Schema.FieldType.STRING)));
        final Schema inputSchema = Schema.of(List.of(
                Schema.Field.of("userId", Schema.FieldType.STRING),
                Schema.Field.of("categoryId", Schema.FieldType.STRING)));
        final Query2 query = Query2.builder()
                .withInput("INPUT", inputSchema)
                .withSource(SideInputLookupSource.builder()
                        .withName("side")
                        .withTable(SideInputLookupSource.table()
                                .withName("users")
                                .withInput("users")
                                .withKeyFields(List.of("userId"))
                                .withSchema(usersSchema())
                                .build())
                        .withTable(SideInputLookupSource.table()
                                .withName("categories")
                                .withInput("categories")
                                .withKeyFields(List.of("categoryId"))
                                .withSchema(categoriesSchema)
                                .build())
                        .build())
                .withSql("""
                        SELECT i.userId AS userId, u.name AS name, c.label AS label
                        FROM INPUT AS i
                        JOIN side.users AS u ON u.userId = i.userId
                        JOIN side.categories AS c ON c.categoryId = i.categoryId
                        """)
                .build();

        query.setup();
        try {
            final SideInputLookupSource source = findSideInputSource(query);
            Assertions.assertEquals(List.of("users", "categories"), source.inputNames());
            source.setData("users", usersData(), "w1");
            source.setData("categories", List.of(
                    MElement.of(Map.of("categoryId", "c1", "label", "food"), TIMESTAMP)), "w1");
            final List<MElement> outputs = query.execute(List.of(
                    MElement.of(Map.of("userId", "u2", "categoryId", "c1"), TIMESTAMP)), TIMESTAMP);

            Assertions.assertEquals(1, outputs.size());
            Assertions.assertEquals("bob", outputs.getFirst().getAsString("name"));
            Assertions.assertEquals("food", outputs.getFirst().getAsString("label"));
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testSameTokenKeepsIndexAndNewTokenRebuilds() {
        final Query2 query = Query2.builder()
                .withInput("INPUT", inputSchema())
                .withSource(usersSource())
                .withSql("""
                        SELECT i.userId AS userId, u.name AS name
                        FROM INPUT AS i
                        JOIN side.users AS u ON u.userId = i.userId
                        """)
                .build();

        query.setup();
        try {
            final SideInputLookupSource source = findSideInputSource(query);
            final List<MElement> input =
                    List.of(MElement.of(Map.of("userId", "u1", "qty", 1L), TIMESTAMP));

            source.setData("users", usersData(), "w1");
            Assertions.assertEquals("alice",
                    query.execute(input, TIMESTAMP).getFirst().getAsString("name"));

            // Same token: the call is a no-op, the existing index is kept.
            source.setData("users", List.of(
                    MElement.of(Map.of("userId", "u1", "name", "updated", "score", 0L), TIMESTAMP)), "w1");
            Assertions.assertEquals("alice",
                    query.execute(input, TIMESTAMP).getFirst().getAsString("name"));

            // New token (next window): the data is replaced and re-indexed.
            source.setData("users", List.of(
                    MElement.of(Map.of("userId", "u1", "name", "updated", "score", 0L), TIMESTAMP)), "w2");
            Assertions.assertEquals("updated",
                    query.execute(input, TIMESTAMP).getFirst().getAsString("name"));
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testLookupWithoutDataFails() {
        final Query2 query = Query2.builder()
                .withInput("INPUT", inputSchema())
                .withSource(usersSource())
                .withSql("""
                        SELECT i.userId AS userId, u.name AS name
                        FROM INPUT AS i
                        JOIN side.users AS u ON u.userId = i.userId
                        """)
                .build();

        query.setup();
        try {
            // The lookup's IllegalStateException surfaces through the enumerable
            // runtime; only the root message is stable.
            final RuntimeException e = Assertions.assertThrows(RuntimeException.class,
                    () -> query.execute(List.of(
                            MElement.of(Map.of("userId", "u1", "qty", 1L), TIMESTAMP)), TIMESTAMP));
            Assertions.assertTrue(rootMessage(e).contains("has not been provided"),
                    "unexpected message: " + rootMessage(e));
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testKeyFieldMustExistInSchema() {
        final IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> SideInputLookupSource.table()
                        .withName("users")
                        .withKeyFields(List.of("unknownField"))
                        .withSchema(usersSchema())
                        .build());
        Assertions.assertTrue(e.getMessage().contains("unknownField"));
    }

    @Test
    public void testSerializedRoundTripLikeDoFn() throws Exception {
        // The Query2 instance must survive Java serialization (DoFn shipping);
        // the side input data is runtime state and is re-fed after setup().
        final Query2 query = Query2.builder()
                .withInput("INPUT", inputSchema())
                .withSource(usersSource())
                .withSql("""
                        SELECT i.userId AS userId, u.name AS name
                        FROM INPUT AS i
                        JOIN side.users AS u ON u.userId = i.userId
                        """)
                .build();

        final java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
        try (java.io.ObjectOutputStream out = new java.io.ObjectOutputStream(bytes)) {
            out.writeObject(query);
        }
        final Query2 restored;
        try (java.io.ObjectInputStream in = new java.io.ObjectInputStream(
                new java.io.ByteArrayInputStream(bytes.toByteArray()))) {
            restored = (Query2) in.readObject();
        }

        restored.setup();
        try {
            findSideInputSource(restored).setData("users", usersData(), "w1");
            final List<MElement> outputs = restored.execute(List.of(
                    MElement.of(Map.of("userId", "u1", "qty", 1L), TIMESTAMP)), TIMESTAMP);
            Assertions.assertEquals(1, outputs.size());
            Assertions.assertEquals("alice", outputs.getFirst().getAsString("name"));
        } finally {
            restored.teardown();
        }
    }

    private static String rootMessage(final Throwable e) {
        Throwable cause = e;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return String.valueOf(cause.getMessage());
    }
}
