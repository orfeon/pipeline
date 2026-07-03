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
 * SQL-level tests for the built-in SEQ_MATCH UDF: sequence-pattern matching
 * over an array column, fanned out with UNNEST, with matched values extracted
 * via dynamic array indexing ({@code events[m.startIdx].amount}).
 */
public class Query2SeqMatchTest {

    private static final Instant TIMESTAMP = Instant.parse("2025-05-01T00:00:00Z");

    private static Schema inputSchema() {
        return Schema.of(List.of(
                Schema.Field.of("userId", Schema.FieldType.STRING),
                Schema.Field.of("events", Schema.FieldType.array(Schema.FieldType.element(List.of(
                        Schema.Field.of("seq", Schema.FieldType.INT64),
                        Schema.Field.of("amount", Schema.FieldType.INT64)))))));
    }

    private static MElement input(String userId, long... amounts) {
        final List<Map<String, Object>> events = new ArrayList<>();
        for (int i = 0; i < amounts.length; i++) {
            events.add(Map.of("seq", (long) (i + 1), "amount", amounts[i]));
        }
        return MElement.of(Map.of("userId", userId, "events", events), TIMESTAMP);
    }

    @Test
    public void testRisingRunsWithValueExtraction() {
        final Query2 query = Query2.builder()
                .withInput("INPUT", inputSchema())
                .withSql("""
                        SELECT
                          i.userId AS userId,
                          m.matchNo AS matchNo,
                          m.startIdx AS s,
                          m.endIdx AS e,
                          i.events[m.startIdx].amount AS fromAmount,
                          i.events[m.endIdx].amount AS toAmount
                        FROM INPUT AS i,
                        UNNEST(SEQ_MATCH(
                          i.events,
                          'seq,amount',
                          'STRT UP{2,}',
                          'UP: $amount > PREV($amount)'
                        )) AS m
                        """)
                .build();

        final Schema outputSchema = query.getOutputSchema();
        Assertions.assertEquals(Schema.Type.int32,
                outputSchema.getField("s").getFieldType().getType());

        query.setup();
        try {
            // u1: rising runs 1..3 (10→30) and 4..6 (5→40); u2: none
            final List<MElement> outputs = query.execute(List.of(
                    input("u1", 10, 20, 30, 5, 8, 40),
                    input("u2", 9, 8, 7)), TIMESTAMP);
            Assertions.assertEquals(2, outputs.size());
            for (final MElement output : outputs) {
                Assertions.assertEquals("u1", output.getAsString("userId"));
            }
            final MElement first = outputs.stream()
                    .filter(o -> ((Number) o.getPrimitiveValue("matchNo")).intValue() == 1)
                    .findFirst().orElseThrow();
            final MElement second = outputs.stream()
                    .filter(o -> ((Number) o.getPrimitiveValue("matchNo")).intValue() == 2)
                    .findFirst().orElseThrow();
            Assertions.assertEquals(1, ((Number) first.getPrimitiveValue("s")).intValue());
            Assertions.assertEquals(3, ((Number) first.getPrimitiveValue("e")).intValue());
            Assertions.assertEquals(10L, first.getAsLong("fromAmount"));
            Assertions.assertEquals(30L, first.getAsLong("toAmount"));
            Assertions.assertEquals(4, ((Number) second.getPrimitiveValue("s")).intValue());
            Assertions.assertEquals(6, ((Number) second.getPrimitiveValue("e")).intValue());
            Assertions.assertEquals(5L, second.getAsLong("fromAmount"));
            Assertions.assertEquals(40L, second.getAsLong("toAmount"));
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testAggregatePerMatchViaLateral() {
        // Count matches per element by folding the UNNEST fan-out back.
        final Query2 query = Query2.builder()
                .withInput("INPUT", inputSchema())
                .withSql("""
                        SELECT i.userId AS userId, COUNT(*) AS cnt
                        FROM INPUT AS i,
                        UNNEST(SEQ_MATCH(
                          i.events, 'seq,amount', 'UP+', 'UP: $amount > PREV($amount)'
                        )) AS m
                        GROUP BY i.userId
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(List.of(
                    input("u1", 1, 2, 1, 2, 1)), TIMESTAMP);
            Assertions.assertEquals(1, outputs.size());
            Assertions.assertEquals(2L, outputs.getFirst().getAsLong("cnt"));
        } finally {
            query.teardown();
        }
    }
}
