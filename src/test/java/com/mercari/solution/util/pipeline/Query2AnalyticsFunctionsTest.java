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
 * SQL-level tests for the analytics built-ins ported from
 * orfeon/calcite-multi-engine: SEQ_FUNNEL (sliding-window funnel),
 * SEQ_RETENTION (cohort retention), QUANTILE (exact interpolated quantile),
 * APPROX_DISTINCT (HyperLogLog), the scalar-array transforms
 * (ARRAY_DIFFERENCE(_INT) / ARRAY_CUM_SUM(_INT) / ARRAY_COMPACT /
 * ARRAY_DISTINCT), TIME_BUCKET — plus the pinned behavior of Calcite's
 * <em>standard</em> ARG_MAX / ARG_MIN aggregates (deliberately no custom
 * UDAF: the standard operators run natively on the enumerable runtime, and a
 * same-name user aggregate breaks the validator's overload resolution).
 */
public class Query2AnalyticsFunctionsTest {

    private static final Instant TIMESTAMP = Instant.parse("2025-05-01T00:00:00Z");

    private static final String FUNNEL_STEPS =
            "\"$action = 'view'; $action = 'cart'; $action = 'buy'\"";

    private static Schema trailSchema() {
        return Schema.of(List.of(
                Schema.Field.of("userId", Schema.FieldType.STRING),
                Schema.Field.of("events", Schema.FieldType.array(Schema.FieldType.element(List.of(
                        Schema.Field.of("ts", Schema.FieldType.INT64),
                        Schema.Field.of("action", Schema.FieldType.STRING)))))));
    }

    private static MElement trail(String userId, Object[]... events) {
        final List<Map<String, Object>> list = new ArrayList<>();
        for (final Object[] e : events) {
            list.add(Map.of("ts", ((Number) e[0]).longValue(), "action", e[1]));
        }
        return MElement.of(Map.of("userId", userId, "events", list), TIMESTAMP);
    }

    private static Schema readingSchema() {
        return Schema.of(List.of(
                Schema.Field.of("userId", Schema.FieldType.STRING),
                Schema.Field.of("ts", Schema.FieldType.INT64),
                Schema.Field.of("action", Schema.FieldType.STRING),
                Schema.Field.of("amount", Schema.FieldType.INT64)));
    }

    private static MElement reading(String userId, long ts, String action, long amount) {
        return MElement.of(Map.of(
                "userId", userId, "ts", ts, "action", action, "amount", amount), TIMESTAMP);
    }

    private static Schema seriesSchema() {
        return Schema.of(List.of(
                Schema.Field.of("userId", Schema.FieldType.STRING),
                Schema.Field.of("ts", Schema.FieldType.array(Schema.FieldType.INT64)),
                Schema.Field.of("states", Schema.FieldType.array(Schema.FieldType.STRING))));
    }

    private static MElement series(String userId, List<Long> ts, List<String> states) {
        return MElement.of(Map.of("userId", userId, "ts", ts, "states", states), TIMESTAMP);
    }

    @Test
    public void testFunnelMaxStepAndWindow() {
        final Query2 query = Query2.builder()
                .withInput("INPUT", trailSchema())
                .withSql("""
                        SELECT i.userId AS userId,
                               SEQ_FUNNEL(i.events, 'ts,action', 'ts', 1000,
                                 """ + FUNNEL_STEPS + """
                        ) AS step
                        FROM INPUT AS i
                        ORDER BY i.userId
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(List.of(
                    trail("u1", new Object[]{0, "view"}, new Object[]{100, "cart"},
                            new Object[]{200, "buy"}),
                    // cart 2000ms after the only view: out of the 1000ms window.
                    trail("u2", new Object[]{0, "view"}, new Object[]{2000, "cart"}),
                    // a later view re-anchors; cart lands within its window.
                    trail("u3", new Object[]{0, "view"}, new Object[]{1500, "view"},
                            new Object[]{2000, "cart"}),
                    // no anchor: later steps never open a chain.
                    trail("u4", new Object[]{0, "cart"}, new Object[]{100, "buy"})),
                    TIMESTAMP);
            Assertions.assertEquals(4, outputs.size());
            Assertions.assertEquals(3, step(outputs.get(0)));
            Assertions.assertEquals(1, step(outputs.get(1)));
            Assertions.assertEquals(2, step(outputs.get(2)));
            Assertions.assertEquals(0, step(outputs.get(3)));
        } finally {
            query.teardown();
        }
    }

    private static int step(MElement element) {
        return ((Number) element.getPrimitiveValue("step")).intValue();
    }

    @Test
    public void testFunnelOverCollectedRows() {
        // Rows arrive shuffled; SEQ_COLLECT sorts by ts, the funnel then sees
        // view -> cart -> buy in order. Field names are positional.
        final Query2 query = Query2.builder()
                .withInput("INPUT", readingSchema())
                .withSql("""
                        SELECT r.userId AS userId,
                               SEQ_FUNNEL(SEQ_COLLECT(r.ts, r.action), 'ts,action', 'ts',
                                 1000, """ + FUNNEL_STEPS + """
                        ) AS step
                        FROM INPUT AS r
                        GROUP BY r.userId
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(List.of(
                    reading("u1", 300, "buy", 0), reading("u1", 100, "view", 0),
                    reading("u1", 200, "cart", 0)), TIMESTAMP);
            Assertions.assertEquals(1, outputs.size());
            Assertions.assertEquals(3, step(outputs.getFirst()));
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testRetentionAnchorsOnFirstCondition() {
        final Query2 query = Query2.builder()
                .withInput("INPUT", trailSchema())
                .withSql("""
                        SELECT i.userId AS userId,
                               SEQ_RETENTION(i.events, 'ts,action',
                                 "$action = 'signup'; $action = 'day1'; $action = 'day7'")
                                 AS ret,
                               SEQ_RETENTION(i.events, 'ts,action',
                                 "$action = 'signup'; $action = 'day1'")[2] AS d1
                        FROM INPUT AS i
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(List.of(
                    trail("u1", new Object[]{0, "signup"}, new Object[]{7, "day7"}),
                    // no anchor: matching later conditions stay masked.
                    trail("u2", new Object[]{1, "day1"}, new Object[]{7, "day7"})),
                    TIMESTAMP);
            Assertions.assertEquals(2, outputs.size());
            Assertions.assertEquals(List.of(true, false, true),
                    outputs.get(0).getPrimitiveValue("ret"));
            Assertions.assertEquals(List.of(false, false, false),
                    outputs.get(1).getPrimitiveValue("ret"));
            Assertions.assertEquals(false, outputs.get(0).getPrimitiveValue("d1"));
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testStandardArgMaxArgMinRunNatively() {
        // Deliberately NOT custom UDAFs: Calcite's standard ARG_MAX/ARG_MIN
        // execute on the enumerable runtime, return the value's own type and
        // ignore NULL keys. This pins that contract.
        final Query2 query = Query2.builder()
                .withInput("INPUT", readingSchema())
                .withSql("""
                        SELECT r.userId AS userId,
                               ARG_MAX(r.action, r.ts) AS latest,
                               ARG_MIN(r.action, r.ts) AS earliest,
                               ARG_MAX(r.amount, r.ts) AS lastAmount
                        FROM INPUT AS r
                        GROUP BY r.userId
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(List.of(
                    reading("u1", 300, "done", 5), reading("u1", 100, "created", 9),
                    reading("u1", 200, "running", 7)), TIMESTAMP);
            Assertions.assertEquals(1, outputs.size());
            Assertions.assertEquals("done", outputs.getFirst().getAsString("latest"));
            Assertions.assertEquals("created", outputs.getFirst().getAsString("earliest"));
            Assertions.assertEquals(5L, outputs.getFirst().getAsLong("lastAmount"));
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testQuantileInterpolates() {
        final Query2 query = Query2.builder()
                .withInput("INPUT", readingSchema())
                .withSql("""
                        SELECT r.userId AS userId,
                               QUANTILE(r.amount, 0.5) AS median,
                               QUANTILE(r.amount, 1) AS hi
                        FROM INPUT AS r
                        GROUP BY r.userId
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(List.of(
                    reading("u1", 1, "x", 10), reading("u1", 2, "x", 20),
                    reading("u1", 3, "x", 30), reading("u1", 4, "x", 40)), TIMESTAMP);
            Assertions.assertEquals(1, outputs.size());
            Assertions.assertEquals(25.0d,
                    ((Number) outputs.getFirst().getPrimitiveValue("median")).doubleValue());
            Assertions.assertEquals(40.0d,
                    ((Number) outputs.getFirst().getPrimitiveValue("hi")).doubleValue());
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testApproxDistinctSmallCardinalityIsExact() {
        final Query2 query = Query2.builder()
                .withInput("INPUT", readingSchema())
                .withSql("""
                        SELECT r.userId AS userId,
                               APPROX_DISTINCT(r.action) AS actions
                        FROM INPUT AS r
                        GROUP BY r.userId
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(List.of(
                    reading("u1", 1, "home", 0), reading("u1", 2, "search", 0),
                    reading("u1", 3, "home", 0), reading("u1", 4, "detail", 0)), TIMESTAMP);
            Assertions.assertEquals(1, outputs.size());
            Assertions.assertEquals(3L, outputs.getFirst().getAsLong("actions"));
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testArrayTransformsComposeWithSeqMatch() {
        final Query2 query = Query2.builder()
                .withInput("INPUT", seriesSchema())
                .withSql("""
                        SELECT i.userId AS userId,
                               CARDINALITY(SEQ_MATCH(ARRAY_DIFFERENCE_INT(i.ts), '',
                                 'GAP', 'GAP: $0 > 100')) AS gaps,
                               CARDINALITY(ARRAY_COMPACT(i.states)) AS transitions,
                               CARDINALITY(ARRAY_DISTINCT(i.states)) AS uniq
                        FROM INPUT AS i
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(List.of(
                    series("u1", List.of(0L, 50L, 400L, 450L),
                            List.of("a", "a", "b", "b", "a"))), TIMESTAMP);
            Assertions.assertEquals(1, outputs.size());
            final MElement first = outputs.getFirst();
            // one inter-event gap > 100 (50 -> 400).
            Assertions.assertEquals(1, ((Number) first.getPrimitiveValue("gaps")).intValue());
            // a,a,b,b,a -> a,b,a.
            Assertions.assertEquals(3,
                    ((Number) first.getPrimitiveValue("transitions")).intValue());
            Assertions.assertEquals(2, ((Number) first.getPrimitiveValue("uniq")).intValue());
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testArrayDifferenceAndCumSumProject() {
        final Query2 query = Query2.builder()
                .withInput("INPUT", seriesSchema())
                .withSql("""
                        SELECT i.userId AS userId,
                               ARRAY_DIFFERENCE_INT(i.ts) AS gaps,
                               ARRAY_CUM_SUM_INT(i.ts) AS totals
                        FROM INPUT AS i
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(List.of(
                    series("u1", List.of(10L, 13L, 21L), List.of("a"))), TIMESTAMP);
            Assertions.assertEquals(1, outputs.size());
            Assertions.assertEquals(List.of(0L, 3L, 8L),
                    outputs.getFirst().getPrimitiveValue("gaps"));
            Assertions.assertEquals(List.of(10L, 23L, 44L),
                    outputs.getFirst().getPrimitiveValue("totals"));
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testTimeBucketGroupsFixedWidthWindows() {
        final long base = 1_700_000_100_000L; // not on a 5-min boundary
        final long bucket = base - base % 300_000;
        final Query2 query = Query2.builder()
                .withInput("INPUT", readingSchema())
                .withSql("""
                        SELECT UNIX_MILLIS(TIME_BUCKET(r.ts, 300000)) AS bucket,
                               COUNT(*) AS hits
                        FROM INPUT AS r
                        GROUP BY TIME_BUCKET(r.ts, 300000)
                        ORDER BY bucket
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(List.of(
                    reading("u1", base, "x", 0), reading("u2", base + 60_000, "x", 0),
                    reading("u3", base + 300_000, "x", 0)), TIMESTAMP);
            Assertions.assertEquals(2, outputs.size());
            Assertions.assertEquals(bucket, outputs.get(0).getAsLong("bucket"));
            Assertions.assertEquals(2L, outputs.get(0).getAsLong("hits"));
            Assertions.assertEquals(bucket + 300_000, outputs.get(1).getAsLong("bucket"));
        } finally {
            query.teardown();
        }
    }
}
