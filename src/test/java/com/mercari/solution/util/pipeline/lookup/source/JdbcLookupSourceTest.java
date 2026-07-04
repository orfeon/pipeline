package com.mercari.solution.util.pipeline.lookup.source;

import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.Query2;
import org.joda.time.Instant;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * End-to-end lookup-join tests against an in-memory H2 database: point keys,
 * composite keys with a range on the trailing column, unique secondary indexes,
 * LEFT joins and column pruning. H2 stores unquoted identifiers uppercase, so
 * table/column names are configured and referenced uppercase.
 */
public class JdbcLookupSourceTest {

    private static final String URL = "jdbc:h2:mem:lookuptest;DB_CLOSE_DELAY=-1";
    private static final Instant TIMESTAMP = Instant.parse("2025-05-01T00:00:00Z");

    // Keeps the named in-memory database alive across connections.
    private static Connection keepAlive;

    @BeforeAll
    public static void createTables() throws Exception {
        keepAlive = DriverManager.getConnection(URL, "sa", "");
        try (Statement statement = keepAlive.createStatement()) {
            statement.execute("""
                    CREATE TABLE USERS (
                      USER_ID BIGINT PRIMARY KEY,
                      NAME VARCHAR(64),
                      EMAIL VARCHAR(64),
                      SCORE DOUBLE
                    )""");
            statement.execute("CREATE UNIQUE INDEX IDX_USERS_EMAIL ON USERS(EMAIL)");
            statement.execute("""
                    INSERT INTO USERS VALUES
                      (1, 'alice', 'alice@example.com', 1.5),
                      (2, 'bob', 'bob@example.com', 2.5),
                      (3, 'carol', 'carol@example.com', 3.5)""");
            statement.execute("""
                    CREATE TABLE EVENTS (
                      USER_ID BIGINT NOT NULL,
                      SEQ BIGINT NOT NULL,
                      CATEGORY VARCHAR(16),
                      PRIMARY KEY (USER_ID, SEQ)
                    )""");
            statement.execute("""
                    INSERT INTO EVENTS VALUES
                      (1, 1, 'a'), (1, 2, 'b'), (1, 3, 'c'),
                      (2, 1, 'x'), (2, 9, 'y')""");
        }
    }

    @AfterAll
    public static void dropDatabase() throws Exception {
        try (Statement statement = keepAlive.createStatement()) {
            statement.execute("DROP ALL OBJECTS");
        }
        keepAlive.close();
    }

    private static JdbcLookupSource.Builder source() {
        return JdbcLookupSource.builder()
                .withName("db")
                .withDriver("org.h2.Driver")
                .withUrl(URL)
                .withUser("sa")
                .withPassword("");
    }

    private static Schema inputSchema() {
        return Schema.of(List.of(
                Schema.Field.of("userId", Schema.FieldType.INT64),
                Schema.Field.of("maxSeq", Schema.FieldType.INT64),
                Schema.Field.of("email", Schema.FieldType.STRING)));
    }

    private static MElement input(long userId, long maxSeq, String email) {
        return MElement.of(Map.of(
                "userId", userId, "maxSeq", maxSeq, "email", email), TIMESTAMP);
    }

    @Test
    public void testPointLookupOnPrimaryKey() {
        final Query2 query = Query2.builder()
                .withInput("INPUT", inputSchema())
                .withSource(source().withTable("USERS").build())
                .withSql("""
                        SELECT i.userId AS userId, u.NAME AS name, u.SCORE AS score
                        FROM INPUT AS i
                        JOIN db.USERS AS u ON u.USER_ID = i.userId
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(
                    List.of(input(1L, 0L, "-"), input(3L, 0L, "-"), input(99L, 0L, "-")),
                    TIMESTAMP);
            Assertions.assertEquals(2, outputs.size());
            final Map<Long, MElement> byUser = collectByUser(outputs);
            Assertions.assertEquals("alice", byUser.get(1L).getAsString("name"));
            Assertions.assertEquals(1.5d, byUser.get(1L).getAsDouble("score"), 1e-9);
            Assertions.assertEquals("carol", byUser.get(3L).getAsString("name"));
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testLeftJoinKeepsUnmatched() {
        final Query2 query = Query2.builder()
                .withInput("INPUT", inputSchema())
                .withSource(source().withTable("USERS").build())
                .withSql("""
                        SELECT i.userId AS userId, u.NAME AS name
                        FROM INPUT AS i
                        LEFT JOIN db.USERS AS u ON u.USER_ID = i.userId
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(
                    List.of(input(2L, 0L, "-"), input(99L, 0L, "-")), TIMESTAMP);
            Assertions.assertEquals(2, outputs.size());
            final Map<Long, MElement> byUser = collectByUser(outputs);
            Assertions.assertEquals("bob", byUser.get(2L).getAsString("name"));
            Assertions.assertNull(byUser.get(99L).getPrimitiveValue("name"));
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testUniqueIndexLookup() {
        final Query2 query = Query2.builder()
                .withInput("INPUT", inputSchema())
                .withSource(source().withTable("USERS").build())
                .withSql("""
                        SELECT i.userId AS userId, u.USER_ID AS resolvedId, u.NAME AS name
                        FROM INPUT AS i
                        JOIN db.USERS AS u ON u.EMAIL = i.email
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(
                    List.of(input(0L, 0L, "bob@example.com")), TIMESTAMP);
            Assertions.assertEquals(1, outputs.size());
            Assertions.assertEquals(2L, outputs.getFirst().getAsLong("resolvedId"));
            Assertions.assertEquals("bob", outputs.getFirst().getAsString("name"));
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testCompositeKeyPrefixWithRange() {
        // Leading key column bound by equality, trailing column by a bounded range.
        final Query2 query = Query2.builder()
                .withInput("INPUT", inputSchema())
                .withSource(source().withTable("EVENTS").build())
                .withSql("""
                        SELECT i.userId AS userId, e.SEQ AS seq, e.CATEGORY AS category
                        FROM INPUT AS i
                        JOIN db.EVENTS AS e
                          ON e.USER_ID = i.userId AND e.SEQ >= 1 AND e.SEQ <= i.maxSeq
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(
                    List.of(input(1L, 2L, "-"), input(2L, 5L, "-")), TIMESTAMP);
            // user 1: seq 1,2 (3 is out of range); user 2: seq 1 (9 is out of range)
            Assertions.assertEquals(3, outputs.size());
            long user1Rows = outputs.stream()
                    .filter(o -> o.getAsLong("userId") == 1L).count();
            long user2Rows = outputs.stream()
                    .filter(o -> o.getAsLong("userId") == 2L).count();
            Assertions.assertEquals(2, user1Rows);
            Assertions.assertEquals(1, user2Rows);
            for (final MElement output : outputs) {
                if (output.getAsLong("userId") == 2L) {
                    Assertions.assertEquals(1L, output.getAsLong("seq"));
                    Assertions.assertEquals("x", output.getAsString("category"));
                }
            }
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testAggregationOverLookupFanOut() {
        // Fan-out rows from the lookup can be folded per input row by GROUP BY.
        final Query2 query = Query2.builder()
                .withInput("INPUT", inputSchema())
                .withSource(source().withTable("EVENTS").build())
                .withSql("""
                        SELECT i.userId AS userId, COUNT(*) AS cnt, MAX(e.SEQ) AS maxSeq
                        FROM INPUT AS i
                        JOIN db.EVENTS AS e ON e.USER_ID = i.userId
                        GROUP BY i.userId
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(
                    List.of(input(1L, 0L, "-")), TIMESTAMP);
            Assertions.assertEquals(1, outputs.size());
            Assertions.assertEquals(3L, outputs.getFirst().getAsLong("cnt"));
            Assertions.assertEquals(3L, outputs.getFirst().getAsLong("maxSeq"));
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testSerializedRoundTripLikeDoFn() throws Exception {
        // The Query2 instance must survive Java serialization (DoFn shipping):
        // metadata derived at construction, clients re-opened at setup().
        final Query2 query = Query2.builder()
                .withInput("INPUT", inputSchema())
                .withSource(source().withTable("USERS").build())
                .withSql("""
                        SELECT i.userId AS userId, u.NAME AS name
                        FROM INPUT AS i
                        JOIN db.USERS AS u ON u.USER_ID = i.userId
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
            final List<MElement> outputs = restored.execute(
                    List.of(input(1L, 0L, "-")), TIMESTAMP);
            Assertions.assertEquals(1, outputs.size());
            Assertions.assertEquals("alice", outputs.getFirst().getAsString("name"));
        } finally {
            restored.teardown();
        }
    }

    private static Map<Long, MElement> collectByUser(final List<MElement> outputs) {
        final Map<Long, MElement> byUser = new HashMap<>();
        for (final MElement output : outputs) {
            byUser.put(output.getAsLong("userId"), output);
        }
        return byUser;
    }
}
