package com.mercari.solution.util.pipeline;

import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.lookup.source.JdbcLookupSource;
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
 * Correlated LATERAL blocks over lookup tables, evaluated per key inside the
 * DoFn: the correlated conditions (point / prefix / prefix + bounded range)
 * become the key-driven fetch, and the rest of the block — aggregation,
 * uncorrelated filters, ORDER BY / LIMIT — runs over the fetched per-key row
 * set in memory. The external table is never scanned.
 */
public class Query2LateralTest {

    private static final String URL = "jdbc:h2:mem:lateraltest;DB_CLOSE_DELAY=-1";
    private static final Instant TIMESTAMP = Instant.parse("2025-05-01T00:00:00Z");

    private static Connection keepAlive;

    @BeforeAll
    public static void createTables() throws Exception {
        keepAlive = DriverManager.getConnection(URL, "sa", "");
        try (Statement statement = keepAlive.createStatement()) {
            statement.execute("""
                    CREATE TABLE EVENTS (
                      USER_ID BIGINT NOT NULL,
                      SEQ BIGINT NOT NULL,
                      CATEGORY VARCHAR(16),
                      AMOUNT BIGINT,
                      PRIMARY KEY (USER_ID, SEQ)
                    )""");
            statement.execute("""
                    INSERT INTO EVENTS VALUES
                      (1, 1, 'a', 10), (1, 2, 'b', 20), (1, 3, 'a', 30),
                      (2, 1, 'a', 5), (2, 2, 'b', 50)""");
            statement.execute("""
                    CREATE TABLE USERS (
                      USER_ID BIGINT PRIMARY KEY,
                      NAME VARCHAR(64)
                    )""");
            statement.execute("""
                    INSERT INTO USERS VALUES
                      (1, 'alice'), (2, 'bob'), (5, 'eve'), (8, 'hal')""");
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
                Schema.Field.of("lo", Schema.FieldType.INT64),
                Schema.Field.of("hi", Schema.FieldType.INT64)));
    }

    private static MElement input(long userId, long lo, long hi) {
        return MElement.of(Map.of("userId", userId, "lo", lo, "hi", hi), TIMESTAMP);
    }

    @Test
    public void testAggregatingLateral() {
        // The inner aggregate folds each key's fetched set into one row; a key
        // that matches nothing aggregates over the empty set (COUNT = 0), so the
        // input row survives even with an INNER lateral.
        final Query2 query = Query2.builder()
                .withInput("INPUT", inputSchema())
                .withSource(source().withTable("EVENTS").build())
                .withSql("""
                        SELECT i.userId AS userId, s.cnt AS cnt, s.total AS total
                        FROM INPUT AS i
                        JOIN LATERAL (
                          SELECT COUNT(*) AS cnt, SUM(e.AMOUNT) AS total
                          FROM db.EVENTS AS e
                          WHERE e.USER_ID = i.userId
                        ) AS s ON TRUE
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(
                    List.of(input(1L, 0L, 0L), input(2L, 0L, 0L), input(99L, 0L, 0L)),
                    TIMESTAMP);
            Assertions.assertEquals(3, outputs.size());
            final Map<Long, MElement> byUser = collectByUser(outputs);
            Assertions.assertEquals(3L, byUser.get(1L).getAsLong("cnt"));
            Assertions.assertEquals(60L, byUser.get(1L).getAsLong("total"));
            Assertions.assertEquals(2L, byUser.get(2L).getAsLong("cnt"));
            Assertions.assertEquals(55L, byUser.get(2L).getAsLong("total"));
            Assertions.assertEquals(0L, byUser.get(99L).getAsLong("cnt"));
            Assertions.assertNull(byUser.get(99L).getPrimitiveValue("total"));
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testRangeCorrelatedLateral() {
        // The range bounds come from input columns; the bounded set is fetched
        // per key (prefix equality + range on the next key column) and folded.
        final Query2 query = Query2.builder()
                .withInput("INPUT", inputSchema())
                .withSource(source().withTable("EVENTS").build())
                .withSql("""
                        SELECT i.userId AS userId, s.total AS total
                        FROM INPUT AS i
                        JOIN LATERAL (
                          SELECT SUM(e.AMOUNT) AS total
                          FROM db.EVENTS AS e
                          WHERE e.USER_ID = i.userId AND e.SEQ >= i.lo AND e.SEQ <= i.hi
                        ) AS s ON TRUE
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(
                    List.of(input(1L, 1L, 2L), input(1L, 2L, 3L), input(2L, 2L, 9L)),
                    TIMESTAMP);
            Assertions.assertEquals(3, outputs.size());
            long total12 = -1, total23 = -1, total2 = -1;
            for (final MElement output : outputs) {
                final long total = output.getAsLong("total");
                if (output.getAsLong("userId") == 2L) {
                    total2 = total;
                } else if (total == 30L) {
                    total12 = total;
                } else {
                    total23 = total;
                }
            }
            Assertions.assertEquals(30L, total12);  // user1 seq 1..2: 10+20
            Assertions.assertEquals(50L, total23);  // user1 seq 2..3: 20+30
            Assertions.assertEquals(50L, total2);   // user2 seq 2..9: 50
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testTopNPerKeyLateral() {
        // ORDER BY / LIMIT inside the block runs per key over the fetched set —
        // per-input top-N without any shuffle. Uncorrelated predicates stay in
        // the block and are applied after the fetch.
        final Query2 query = Query2.builder()
                .withInput("INPUT", inputSchema())
                .withSource(source().withTable("EVENTS").build())
                .withSql("""
                        SELECT i.userId AS userId, s.SEQ AS seq, s.AMOUNT AS amount
                        FROM INPUT AS i
                        JOIN LATERAL (
                          SELECT e.SEQ, e.AMOUNT
                          FROM db.EVENTS AS e
                          WHERE e.USER_ID = i.userId AND e.CATEGORY = 'a'
                          ORDER BY e.AMOUNT DESC
                          LIMIT 1
                        ) AS s ON TRUE
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(
                    List.of(input(1L, 0L, 0L), input(2L, 0L, 0L)), TIMESTAMP);
            Assertions.assertEquals(2, outputs.size());
            final Map<Long, MElement> byUser = collectByUser(outputs);
            // user1 category 'a': amounts 10, 30 → top is seq 3 / 30
            Assertions.assertEquals(3L, byUser.get(1L).getAsLong("seq"));
            Assertions.assertEquals(30L, byUser.get(1L).getAsLong("amount"));
            // user2 category 'a': only seq 1 / 5
            Assertions.assertEquals(1L, byUser.get(2L).getAsLong("seq"));
            Assertions.assertEquals(5L, byUser.get(2L).getAsLong("amount"));
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testLeftLateralFanOutKeepsUnmatched() {
        // A fan-out block (no aggregate) yields no rows for an unmatched key;
        // LEFT ... ON TRUE keeps the input row with nulls.
        final Query2 query = Query2.builder()
                .withInput("INPUT", inputSchema())
                .withSource(source().withTable("EVENTS").build())
                .withSql("""
                        SELECT i.userId AS userId, s.SEQ AS seq
                        FROM INPUT AS i
                        LEFT JOIN LATERAL (
                          SELECT e.SEQ
                          FROM db.EVENTS AS e
                          WHERE e.USER_ID = i.userId
                        ) AS s ON TRUE
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(
                    List.of(input(2L, 0L, 0L), input(99L, 0L, 0L)), TIMESTAMP);
            Assertions.assertEquals(3, outputs.size());
            int user2Rows = 0;
            boolean unmatchedKept = false;
            for (final MElement output : outputs) {
                if (output.getAsLong("userId") == 2L) {
                    user2Rows++;
                } else {
                    Assertions.assertNull(output.getPrimitiveValue("seq"));
                    unmatchedKept = true;
                }
            }
            Assertions.assertEquals(2, user2Rows);
            Assertions.assertTrue(unmatchedKept);
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testPureRangeJoinFetchesMultipleRows() {
        // Plain (non-LATERAL) join with only a bounded range on the leading key
        // column: one range fetch per input row, fan-out to every row in range.
        final Query2 query = Query2.builder()
                .withInput("INPUT", inputSchema())
                .withSource(source().withTable("USERS").build())
                .withSql("""
                        SELECT i.userId AS userId, u.USER_ID AS matchedId, u.NAME AS name
                        FROM INPUT AS i
                        JOIN db.USERS AS u
                          ON u.USER_ID >= i.lo AND u.USER_ID <= i.hi
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(
                    List.of(input(7L, 1L, 5L)), TIMESTAMP);
            // USERS ids in [1, 5]: 1, 2, 5
            Assertions.assertEquals(3, outputs.size());
            final Map<Long, String> names = new HashMap<>();
            for (final MElement output : outputs) {
                Assertions.assertEquals(7L, output.getAsLong("userId"));
                names.put(output.getAsLong("matchedId"), output.getAsString("name"));
            }
            Assertions.assertEquals(Map.of(1L, "alice", 2L, "bob", 5L, "eve"), names);
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testLateralIsRepeatable() {
        // The prepared statement (outer and inner) must be reusable across
        // executions, as in a DoFn.
        final Query2 query = Query2.builder()
                .withInput("INPUT", inputSchema())
                .withSource(source().withTable("EVENTS").build())
                .withSql("""
                        SELECT i.userId AS userId, s.total AS total
                        FROM INPUT AS i
                        JOIN LATERAL (
                          SELECT SUM(e.AMOUNT) AS total
                          FROM db.EVENTS AS e
                          WHERE e.USER_ID = i.userId
                        ) AS s ON TRUE
                        """)
                .build();
        query.setup();
        try {
            for (int i = 0; i < 3; i++) {
                final List<MElement> outputs = query.execute(
                        List.of(input(1L, 0L, 0L)), TIMESTAMP);
                Assertions.assertEquals(1, outputs.size());
                Assertions.assertEquals(60L, outputs.getFirst().getAsLong("total"));
            }
        } finally {
            query.teardown();
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
