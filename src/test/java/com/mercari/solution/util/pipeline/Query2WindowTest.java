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
 * Window functions (OVER) and array folding over lookup-join results — all
 * evaluated per element, so a PARTITION BY spans only the rows fetched for the
 * current element's evaluation.
 */
public class Query2WindowTest {

    private static final String URL = "jdbc:h2:mem:windowtest;DB_CLOSE_DELAY=-1";
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
                      AMOUNT BIGINT,
                      PRIMARY KEY (USER_ID, SEQ)
                    )""");
            statement.execute("""
                    INSERT INTO EVENTS VALUES
                      (1, 1, 10), (1, 2, 30), (1, 3, 20),
                      (2, 1, 5), (2, 2, 50)""");
        }
    }

    @AfterAll
    public static void dropDatabase() throws Exception {
        try (Statement statement = keepAlive.createStatement()) {
            statement.execute("DROP ALL OBJECTS");
        }
        keepAlive.close();
    }

    private static JdbcLookupSource source() {
        return JdbcLookupSource.builder()
                .withName("db")
                .withDriver("org.h2.Driver")
                .withUrl(URL)
                .withUser("sa")
                .withPassword("")
                .withTable("EVENTS")
                .build();
    }

    private static Schema inputSchema() {
        return Schema.of(List.of(
                Schema.Field.of("userId", Schema.FieldType.INT64)));
    }

    private static MElement input(long userId) {
        return MElement.of(Map.of("userId", userId), TIMESTAMP);
    }

    @Test
    public void testWindowOverLookupFanOut() {
        // RANK by amount within each user's fetched rows + running total.
        final Query2 query = Query2.builder()
                .withInput("INPUT", inputSchema())
                .withSource(source())
                .withSql("""
                        SELECT
                          i.userId AS userId,
                          e.SEQ AS seq,
                          RANK() OVER (PARTITION BY i.userId ORDER BY e.AMOUNT DESC) AS rnk,
                          SUM(e.AMOUNT) OVER (PARTITION BY i.userId ORDER BY e.SEQ) AS runningTotal
                        FROM INPUT AS i
                        JOIN db.EVENTS AS e ON e.USER_ID = i.userId
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(
                    List.of(input(1L), input(2L)), TIMESTAMP);
            Assertions.assertEquals(5, outputs.size());
            final Map<String, MElement> byKey = new HashMap<>();
            for (final MElement output : outputs) {
                byKey.put(output.getAsLong("userId") + "#" + output.getAsLong("seq"), output);
            }
            // user1 amounts by seq: 10, 30, 20 → rank by amount desc: seq2=1, seq3=2, seq1=3
            Assertions.assertEquals(3L, byKey.get("1#1").getAsLong("rnk"));
            Assertions.assertEquals(1L, byKey.get("1#2").getAsLong("rnk"));
            Assertions.assertEquals(2L, byKey.get("1#3").getAsLong("rnk"));
            // running totals by seq: 10, 40, 60
            Assertions.assertEquals(10L, byKey.get("1#1").getAsLong("runningTotal"));
            Assertions.assertEquals(40L, byKey.get("1#2").getAsLong("runningTotal"));
            Assertions.assertEquals(60L, byKey.get("1#3").getAsLong("runningTotal"));
            // user2: seq2 (50) rank 1, seq1 (5) rank 2
            Assertions.assertEquals(2L, byKey.get("2#1").getAsLong("rnk"));
            Assertions.assertEquals(1L, byKey.get("2#2").getAsLong("rnk"));
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testWindowInsideLateralBlock() {
        // Rank within the block's fetched set, filter on it outside.
        final Query2 query = Query2.builder()
                .withInput("INPUT", inputSchema())
                .withSource(source())
                .withSql("""
                        SELECT i.userId AS userId, s.seq AS seq, s.rnk AS rnk
                        FROM INPUT AS i
                        JOIN LATERAL (
                          SELECT e.SEQ AS seq,
                                 RANK() OVER (ORDER BY e.AMOUNT DESC) AS rnk
                          FROM db.EVENTS AS e
                          WHERE e.USER_ID = i.userId
                        ) AS s ON TRUE
                        WHERE s.rnk <= 2
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(List.of(input(1L)), TIMESTAMP);
            // user1 top-2 by amount: seq2 (30) rank1, seq3 (20) rank2
            Assertions.assertEquals(2, outputs.size());
            final Map<Long, Long> rankBySeq = new HashMap<>();
            for (final MElement output : outputs) {
                rankBySeq.put(output.getAsLong("seq"), output.getAsLong("rnk"));
            }
            Assertions.assertEquals(Map.of(2L, 1L, 3L, 2L), rankBySeq);
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testArrayAggOverLookupFanOut() {
        // Folding the fetched set into ONE row per input with an array column
        // (the alternative to the default fan-out): outer GROUP BY + ARRAY_AGG,
        // then pattern-matching the array with SEQ_MATCH.
        final Query2 query = Query2.builder()
                .withInput("INPUT", inputSchema())
                .withSource(source())
                .withSql("""
                        SELECT b.userId AS userId, m.startIdx AS s, m.endIdx AS e
                        FROM (
                          SELECT i.userId AS userId,
                                 ARRAY_AGG(e.AMOUNT ORDER BY e.SEQ) AS amounts
                          FROM INPUT AS i
                          JOIN db.EVENTS AS e ON e.USER_ID = i.userId
                          GROUP BY i.userId
                        ) AS b,
                        UNNEST(SEQ_MATCH(b.amounts, NULL, 'UP', 'UP: $0 > PREV($0)')) AS m
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(List.of(input(1L)), TIMESTAMP);
            // user1 amounts by seq: [10, 30, 20] → single rising step at index 2
            Assertions.assertEquals(1, outputs.size());
            Assertions.assertEquals(2, ((Number) outputs.getFirst()
                    .getPrimitiveValue("s")).intValue());
            Assertions.assertEquals(2, ((Number) outputs.getFirst()
                    .getPrimitiveValue("e")).intValue());
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testUnorderedArrayAggInsideLateral() {
        // ARRAY_AGG without ORDER BY round-trips into the lateral block
        // (with ORDER BY it does not: Calcite unparses the collation as
        // WITHIN GROUP, which its own validator then rejects — use the outer
        // GROUP BY form above when ordering matters).
        final Query2 query = Query2.builder()
                .withInput("INPUT", inputSchema())
                .withSource(source())
                .withSql("""
                        SELECT i.userId AS userId, CARDINALITY(s.amounts) AS n
                        FROM INPUT AS i
                        JOIN LATERAL (
                          SELECT ARRAY_AGG(e.AMOUNT) AS amounts
                          FROM db.EVENTS AS e
                          WHERE e.USER_ID = i.userId
                        ) AS s ON TRUE
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(List.of(input(1L)), TIMESTAMP);
            Assertions.assertEquals(1, outputs.size());
            Assertions.assertEquals(3, ((Number) outputs.getFirst()
                    .getPrimitiveValue("n")).intValue());
        } finally {
            query.teardown();
        }
    }
}
