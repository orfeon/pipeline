package com.mercari.solution.util.pipeline;

import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SQL-level tests for the stateful-model companions: drift detection
 * (SEQ_PAGE_HINKLEY / SEQ_CUSUM), exponential smoothing (SEQ_EXP_SMOOTH /
 * SEQ_HOLT), the deterministic Thompson-sampling primitive (BETA_SAMPLE), and
 * the time-decayed aggregates (DECAY_SUM / DECAY_AVG / DECAY_COUNT).
 */
public class Query2ModelFunctionsTest {

    private static final Instant TIMESTAMP = Instant.parse("2025-05-01T00:00:00Z");
    private static final double DELTA = 1e-9;

    private static Schema seriesSchema() {
        return Schema.of(List.of(
                Schema.Field.of("id", Schema.FieldType.STRING),
                Schema.Field.of("series", Schema.FieldType.array(Schema.FieldType.element(List.of(
                        Schema.Field.of("v", Schema.FieldType.FLOAT64.withNullable(true)),
                        Schema.Field.of("ts", Schema.FieldType.INT64.withNullable(true)))))),
                Schema.Field.of("values", Schema.FieldType.array(Schema.FieldType.FLOAT64))));
    }

    private static MElement element(String id, List<Map<String, Object>> series,
            List<Double> values) {
        final Map<String, Object> row = new HashMap<>();
        row.put("id", id);
        row.put("series", series);
        row.put("values", values);
        return MElement.of(row, TIMESTAMP);
    }

    private static List<Map<String, Object>> series(double... vs) {
        final List<Map<String, Object>> out = new ArrayList<>();
        for (final double v : vs) {
            out.add(Map.of("v", v, "ts", 0L));
        }
        return out;
    }

    @Test
    public void testPageHinkleyDetectsShift() {
        final Query2 query = Query2.builder()
                .withInput("INPUT", seriesSchema())
                .withSql("""
                        SELECT i.id AS id,
                               SEQ_PAGE_HINKLEY(i.series, 'v', 0.0, 5.0) AS onRows,
                               SEQ_PAGE_HINKLEY(i.`values`, '', 0.0, 5.0) AS onScalars
                        FROM INPUT AS i
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(List.of(
                    // Upward shift at position 5: mean drifts to 2, deviation 8 > 5.
                    element("up", series(0, 0, 0, 0, 10, 10, 10),
                            List.of(0d, 0d, 0d, 0d, 10d, 10d, 10d)),
                    // Downward shift, detected on the mirrored side.
                    element("down", series(10, 10, 10, 10, 0, 0),
                            List.of(10d, 10d, 10d, 10d, 0d, 0d)),
                    // No drift.
                    element("flat", series(1, 1, 1, 1), List.of(1d, 1d, 1d, 1d))),
                    TIMESTAMP);
            Assertions.assertEquals(3, outputs.size());
            Assertions.assertEquals(5, intValue(outputs.get(0), "onRows"));
            Assertions.assertEquals(5, intValue(outputs.get(0), "onScalars"));
            Assertions.assertEquals(5, intValue(outputs.get(1), "onRows"));
            Assertions.assertEquals(0, intValue(outputs.get(2), "onRows"));
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testCusumDetectsShift() {
        final Query2 query = Query2.builder()
                .withInput("INPUT", seriesSchema())
                .withSql("""
                        SELECT i.id AS id,
                               SEQ_CUSUM(i.series, 'v', 0.0, 0.5, 4.0) AS detected
                        FROM INPUT AS i
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(List.of(
                    // S+ reaches 2.5 at position 4 and 5.0 > 4 at position 5.
                    element("up", series(0, 0, 0, 3, 3, 3), List.of()),
                    // Mirrored negative shift trips the lower side.
                    element("down", series(0, 0, 0, -3, -3, -3), List.of()),
                    // Wander within the slack never accumulates.
                    element("flat", series(0.3, -0.3, 0.3, -0.3), List.of())),
                    TIMESTAMP);
            Assertions.assertEquals(3, outputs.size());
            Assertions.assertEquals(5, intValue(outputs.get(0), "detected"));
            Assertions.assertEquals(5, intValue(outputs.get(1), "detected"));
            Assertions.assertEquals(0, intValue(outputs.get(2), "detected"));
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testExpSmoothAndHolt() {
        final Query2 query = Query2.builder()
                .withInput("INPUT", seriesSchema())
                .withSql("""
                        SELECT i.id AS id,
                               SEQ_EXP_SMOOTH(i.series, 'v', 0.5) AS level,
                               SEQ_HOLT(i.series, 'v', 0.5, 0.5) AS next1,
                               SEQ_HOLT(i.series, 'v', 0.5, 0.5, 3) AS next3
                        FROM INPUT AS i
                        """)
                .build();
        query.setup();
        try {
            // A NULL value must be skipped, not reset the smoothing.
            final List<Map<String, Object>> withNull = new ArrayList<>(series(1, 2, 3));
            final Map<String, Object> nullRow = new HashMap<>();
            nullRow.put("v", null);
            nullRow.put("ts", 0L);
            withNull.add(nullRow);
            withNull.addAll(series(4));

            final List<MElement> outputs = query.execute(List.of(
                    element("linear", withNull, List.of()),
                    element("single", series(7), List.of())),
                    TIMESTAMP);
            Assertions.assertEquals(2, outputs.size());
            // Levels over [1,2,3,4] with alpha 0.5: 1, 1.5, 2.25, 3.125.
            Assertions.assertEquals(3.125d, doubleValue(outputs.get(0), "level"), DELTA);
            // Exact linear data: Holt tracks it exactly, forecast = 4 + h.
            Assertions.assertEquals(5d, doubleValue(outputs.get(0), "next1"), DELTA);
            Assertions.assertEquals(7d, doubleValue(outputs.get(0), "next3"), DELTA);
            // A single value has no trend.
            Assertions.assertEquals(7d, doubleValue(outputs.get(1), "level"), DELTA);
            Assertions.assertEquals(7d, doubleValue(outputs.get(1), "next1"), DELTA);
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testBetaSampleDeterministicAndInRange() {
        final Query2 query = Query2.builder()
                .withInput("INPUT", seriesSchema())
                .withSql("""
                        SELECT i.id AS id,
                               BETA_SAMPLE(1.0, 1.0, 42) AS uniformDraw,
                               BETA_SAMPLE(1.0, 1.0, 43) AS otherSeed,
                               BETA_SAMPLE(50.0, 1.0, 42) AS skewed,
                               BETA_SAMPLE(0.5, 0.5, 42) AS bathtub
                        FROM INPUT AS i
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(List.of(
                    element("r1", List.of(), List.of()),
                    element("r2", List.of(), List.of())),
                    TIMESTAMP);
            Assertions.assertEquals(2, outputs.size());
            final double draw = doubleValue(outputs.get(0), "uniformDraw");
            // Same arguments on another row: the same draw (repeatable under
            // bundle retries); a different seed: a different draw.
            Assertions.assertEquals(draw, doubleValue(outputs.get(1), "uniformDraw"), 0d);
            Assertions.assertNotEquals(draw, doubleValue(outputs.get(0), "otherSeed"));
            for (final String field : List.of("uniformDraw", "otherSeed", "skewed", "bathtub")) {
                final double value = doubleValue(outputs.get(0), field);
                Assertions.assertTrue(value > 0 && value < 1, field + " out of range: " + value);
            }
            // Beta(50, 1) concentrates near 1.
            Assertions.assertTrue(doubleValue(outputs.get(0), "skewed") > 0.8);
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testDecayAggregates() {
        final Query2 query = Query2.builder()
                .withInput("INPUT", seriesSchema())
                .withSql("""
                        SELECT i.id AS id,
                               DECAY_SUM(s.v, s.ts, 1000, 2000) AS decayedSum,
                               DECAY_AVG(s.v, s.ts, 1000, 2000) AS decayedAvg,
                               DECAY_COUNT(s.ts, 1000, 2000) AS decayedCount
                        FROM INPUT AS i, UNNEST(i.series) AS s
                        GROUP BY i.id
                        """)
                .build();
        query.setup();
        try {
            // v=1 at the reference time (weight 1), v=4 one half-life earlier
            // (weight 0.5): sum = 1 + 2 = 3, avg = 3 / 1.5 = 2, count = 1.5.
            final List<Map<String, Object>> rows = new ArrayList<>();
            rows.add(Map.of("v", 1d, "ts", 2000L));
            rows.add(Map.of("v", 4d, "ts", 1000L));
            // A NULL value (or NULL ts) row is skipped by every aggregate.
            final Map<String, Object> nullValue = new HashMap<>();
            nullValue.put("v", null);
            nullValue.put("ts", 500L);
            rows.add(nullValue);

            final List<MElement> outputs = query.execute(List.of(
                    element("g1", rows, List.of())),
                    TIMESTAMP);
            Assertions.assertEquals(1, outputs.size());
            Assertions.assertEquals(3d, doubleValue(outputs.get(0), "decayedSum"), DELTA);
            Assertions.assertEquals(2d, doubleValue(outputs.get(0), "decayedAvg"), DELTA);
            // DECAY_COUNT counts the NULL-v row too (its ts is present).
            Assertions.assertEquals(1.5d + Math.pow(2d, -1.5d),
                    doubleValue(outputs.get(0), "decayedCount"), DELTA);
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testDecayOverAllNullRows() {
        final Query2 query = Query2.builder()
                .withInput("INPUT", seriesSchema())
                .withSql("""
                        SELECT i.id AS id,
                               DECAY_SUM(s.v, s.ts, 1000, 2000) AS decayedSum,
                               DECAY_AVG(s.v, s.ts, 1000, 2000) AS decayedAvg,
                               DECAY_COUNT(s.ts, 1000, 2000) AS decayedCount,
                               COALESCE(DECAY_COUNT(s.ts, 1000, 2000), 0.0) AS countOrZero
                        FROM INPUT AS i, UNNEST(i.series) AS s
                        GROUP BY i.id
                        """)
                .build();
        query.setup();
        try {
            final Map<String, Object> nullRow = new HashMap<>();
            nullRow.put("v", null);
            nullRow.put("ts", null);
            final List<MElement> outputs = query.execute(List.of(
                    element("g1", List.of(nullRow), List.of())),
                    TIMESTAMP);
            Assertions.assertEquals(1, outputs.size());
            Assertions.assertNull(outputs.get(0).getPrimitiveValue("decayedSum"));
            Assertions.assertNull(outputs.get(0).getPrimitiveValue("decayedAvg"));
            // Calcite's strict user-aggregate convention: an all-NULL group
            // yields NULL (rows with a NULL argument are skipped before add);
            // COALESCE restores COUNT semantics.
            Assertions.assertNull(outputs.get(0).getPrimitiveValue("decayedCount"));
            Assertions.assertEquals(0d, doubleValue(outputs.get(0), "countOrZero"), DELTA);
        } finally {
            query.teardown();
        }
    }

    private static double doubleValue(MElement element, String field) {
        return ((Number) element.getPrimitiveValue(field)).doubleValue();
    }

    private static int intValue(MElement element, String field) {
        return ((Number) element.getPrimitiveValue(field)).intValue();
    }
}
