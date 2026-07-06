package com.mercari.solution.util.pipeline;

import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * SQL-level tests for the SEQ_* family beyond the original SEQ_MATCH:
 * SEQ_MATCH_STEPS (per-element symbol detail), match modes, SEQ_FOLD (range
 * aggregation as an expression), SEQ_SPLIT (sessionization) and SEQ_COLLECT
 * (key-ordered collection), ported from orfeon/calcite-multi-engine.
 */
public class Query2SeqFamilyTest {

    private static final Instant TIMESTAMP = Instant.parse("2025-05-01T00:00:00Z");

    private static Schema trailSchema() {
        return Schema.of(List.of(
                Schema.Field.of("userId", Schema.FieldType.STRING),
                Schema.Field.of("events", Schema.FieldType.array(Schema.FieldType.element(List.of(
                        Schema.Field.of("ts", Schema.FieldType.INT64),
                        Schema.Field.of("action", Schema.FieldType.STRING),
                        Schema.Field.of("amount", Schema.FieldType.INT64)))))));
    }

    private static MElement trail(String userId, Object[]... events) {
        final List<Map<String, Object>> list = new ArrayList<>();
        for (final Object[] e : events) {
            list.add(Map.of("ts", ((Number) e[0]).longValue(),
                    "action", e[1], "amount", ((Number) e[2]).longValue()));
        }
        return MElement.of(Map.of("userId", userId, "events", list), TIMESTAMP);
    }

    private static Schema readingSchema() {
        return Schema.of(List.of(
                Schema.Field.of("userId", Schema.FieldType.STRING),
                Schema.Field.of("ts", Schema.FieldType.INT64),
                Schema.Field.of("amount", Schema.FieldType.INT64)));
    }

    private static MElement reading(String userId, long ts, long amount) {
        return MElement.of(Map.of("userId", userId, "ts", ts, "amount", amount), TIMESTAMP);
    }

    @Test
    public void testStepsAggregateExactlyTheMatchedPurchases() {
        final Query2 query = Query2.builder()
                .withInput("INPUT", trailSchema())
                .withSql("""
                        SELECT i.userId AS userId, s.matchNo AS matchNo,
                               SUM(i.events[s.idx].amount) AS total
                        FROM INPUT AS i,
                        UNNEST(SEQ_MATCH_STEPS(i.events, 'ts,action,amount',
                          'PROMO (SKIP* BUY){3}',
                          "PROMO: $action = 'promo';
                           BUY: $action = 'purchase';
                           SKIP: $action <> 'purchase'")) AS s
                        WHERE s.symbol = 'BUY'
                        GROUP BY i.userId, s.matchNo
                        HAVING SUM(i.events[s.idx].amount) > 1000
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(List.of(
                    trail("u1", new Object[]{100, "promo", 0}, new Object[]{200, "purchase", 400},
                            new Object[]{300, "view", 0}, new Object[]{400, "purchase", 300},
                            new Object[]{500, "purchase", 400}),
                    trail("u2", new Object[]{10, "promo", 0}, new Object[]{20, "purchase", 100},
                            new Object[]{30, "purchase", 200}, new Object[]{40, "purchase", 300})),
                    TIMESTAMP);
            Assertions.assertEquals(1, outputs.size());
            Assertions.assertEquals("u1", outputs.getFirst().getAsString("userId"));
            Assertions.assertEquals(1100L, outputs.getFirst().getAsLong("total"));
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testShortestModePinsFirstOccurrence() {
        final Query2 query = Query2.builder()
                .withInput("INPUT", trailSchema())
                .withSql("""
                        SELECT m.endIdx AS e
                        FROM INPUT AS i,
                        UNNEST(SEQ_MATCH(i.events, 'ts,action,amount',
                          'A ANY* B', "A: $action = 'a'; B: $action = 'b'",
                          'shortest')) AS m
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(List.of(
                    trail("u1", new Object[]{1, "a", 0}, new Object[]{2, "b", 0},
                            new Object[]{3, "y", 0}, new Object[]{4, "b", 0})), TIMESTAMP);
            Assertions.assertEquals(1, outputs.size());
            Assertions.assertEquals(2, ((Number) outputs.getFirst()
                    .getPrimitiveValue("e")).intValue());
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testFoldAggregatesMatchRangeAsExpression() {
        final Query2 query = Query2.builder()
                .withInput("INPUT", trailSchema())
                .withSql("""
                        SELECT i.userId AS userId,
                               SEQ_FOLD_INT(i.events, m.startIdx, m.endIdx,
                                            'amount', 'sum') AS total
                        FROM INPUT AS i,
                        UNNEST(SEQ_MATCH(i.events, 'ts,action,amount',
                          'PROMO BUY{3}',
                          "PROMO: $action = 'promo';
                           BUY: $action = 'purchase'")) AS m
                        WHERE SEQ_FOLD_INT(i.events, m.startIdx, m.endIdx,
                                           'amount', 'sum') > 1000
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(List.of(
                    trail("u1", new Object[]{1, "promo", 0}, new Object[]{2, "purchase", 400},
                            new Object[]{3, "purchase", 300}, new Object[]{4, "purchase", 400}),
                    trail("u2", new Object[]{1, "promo", 0}, new Object[]{2, "purchase", 100},
                            new Object[]{3, "purchase", 200}, new Object[]{4, "purchase", 300})),
                    TIMESTAMP);
            Assertions.assertEquals(1, outputs.size());
            Assertions.assertEquals("u1", outputs.getFirst().getAsString("userId"));
            Assertions.assertEquals(1100L, outputs.getFirst().getAsLong("total"));
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testSplitSessionsAndMatchPerSession() {
        final Query2 query = Query2.builder()
                .withInput("INPUT", trailSchema())
                .withSql("""
                        SELECT i.userId AS userId, s.sessionNo AS sessionNo,
                               m.startIdx AS ms, m.endIdx AS me
                        FROM INPUT AS i,
                        UNNEST(SEQ_SPLIT(i.events, 'ts', 1000))
                          WITH ORDINALITY AS s(sess, sessionNo),
                        UNNEST(SEQ_MATCH(s.sess, 'ts,action,amount',
                          'STRT UP{2,}', 'UP: $amount > PREV($amount)')) AS m
                        """)
                .build();
        query.setup();
        try {
            // Sessions (gap > 1000): [100,200,300] rising 10<20<30; [5000,5100] not.
            final List<MElement> outputs = query.execute(List.of(
                    trail("u1", new Object[]{100, "x", 10}, new Object[]{200, "x", 20},
                            new Object[]{300, "x", 30}, new Object[]{5000, "x", 5},
                            new Object[]{5100, "x", 4})), TIMESTAMP);
            Assertions.assertEquals(1, outputs.size());
            Assertions.assertEquals(1L, outputs.getFirst().getAsLong("sessionNo"));
            Assertions.assertEquals(1, ((Number) outputs.getFirst()
                    .getPrimitiveValue("ms")).intValue());
            Assertions.assertEquals(3, ((Number) outputs.getFirst()
                    .getPrimitiveValue("me")).intValue());
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testCollectBuildsOrderedSequenceFromGroupedRows() {
        // Rows arrive shuffled; SEQ_COLLECT sorts by ts, then the rising run
        // over amounts (1,2 at ts 100,200) is detected and folded.
        final Query2 query = Query2.builder()
                .withInput("INPUT", readingSchema())
                .withSql("""
                        SELECT r.userId AS userId,
                               SEQ_FOLD_INT(SEQ_COLLECT(r.ts, r.amount), 1, 2, '1', 'sum')
                                 AS firstTwo,
                               CARDINALITY(SEQ_MATCH(SEQ_COLLECT(r.ts, r.amount),
                                 'ts,amount', 'UP+', 'UP: $amount > PREV($amount)')) AS runs
                        FROM INPUT AS r
                        GROUP BY r.userId
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(List.of(
                    reading("u1", 5000, 1), reading("u1", 100, 1),
                    reading("u1", 200, 2), reading("u1", 5100, 0)), TIMESTAMP);
            Assertions.assertEquals(1, outputs.size());
            // ts order: (100,1) (200,2) (5000,1) (5100,0) -> first two = 3.
            Assertions.assertEquals(3L, outputs.getFirst().getAsLong("firstTwo"));
            // one rising run: 1 -> 2.
            Assertions.assertEquals(1, ((Number) outputs.getFirst()
                    .getPrimitiveValue("runs")).intValue());
        } finally {
            query.teardown();
        }
    }
}
