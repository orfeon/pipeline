package com.mercari.solution.util.pipeline;

import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/**
 * Output columns that JDBC metadata cannot describe — a struct, an array of structs, a map, an
 * array of arrays: their schema comes from the planned row type and their values are converted
 * by that type.
 */
public class Query2NestedOutputTest {

    private static final Instant TIMESTAMP = Instant.parse("2026-10-03T00:00:00Z");
    private static final long AT = 1_790_000_000_000_000L;

    private static Schema oddsSchema() {
        return Schema.of(List.of(
                Schema.Field.of("Umaban", Schema.FieldType.INT64),
                Schema.Field.of("Odds", Schema.FieldType.INT64),
                Schema.Field.of("At", Schema.FieldType.TIMESTAMP)));
    }

    private static Schema inputSchema() {
        return Schema.of(List.of(
                Schema.Field.of("id", Schema.FieldType.STRING),
                Schema.Field.of("info", Schema.FieldType.array(Schema.FieldType.element(oddsSchema()))),
                Schema.Field.of("head", Schema.FieldType.element(oddsSchema())),
                Schema.Field.of("attrs", Schema.FieldType.map(Schema.FieldType.STRING)),
                Schema.Field.of("laps", Schema.FieldType.array(Schema.FieldType.INT64))));
    }

    private static MElement input() {
        return MElement.of(Map.of(
                "id", "a",
                "info", List.of(
                        Map.of("Umaban", 1L, "Odds", 25L, "At", AT),
                        Map.of("Umaban", 2L, "Odds", 0L, "At", AT)),
                "head", Map.of("Umaban", 9L, "Odds", 99L, "At", AT),
                "attrs", Map.of("k", "v"),
                "laps", List.of(120L, 0L, 118L)), TIMESTAMP);
    }

    private static MElement evaluate(final Query2 query) {
        query.setup();
        try {
            return query.execute(List.of(input()), TIMESTAMP).getFirst();
        } finally {
            query.teardown();
        }
    }

    private static List<String> fieldNames(final Schema schema) {
        return schema.getFields().stream().map(Schema.Field::getName).toList();
    }

    @Test
    public void testPassThrough() {
        // An array of structs, a struct and a map column passed through as they are.
        final Query2 query = Query2.of("INPUT", inputSchema(), """
                SELECT i.id, i.info AS info, i.head AS head, i.attrs AS attrs, i.laps AS laps
                FROM INPUT AS i
                """);
        final Schema schema = query.getOutputSchema();

        final Schema.FieldType info = schema.getField("info").getFieldType();
        Assertions.assertEquals(Schema.Type.array, info.getType());
        Assertions.assertEquals(Schema.Type.element, info.getArrayValueType().getType());
        final Schema infoSchema = info.getArrayValueType().getElementSchema();
        Assertions.assertEquals(List.of("Umaban", "Odds", "At"), fieldNames(infoSchema));
        Assertions.assertEquals(Schema.Type.int64, infoSchema.getField("Umaban").getFieldType().getType());
        Assertions.assertEquals(Schema.Type.timestamp, infoSchema.getField("At").getFieldType().getType());

        final Schema.FieldType head = schema.getField("head").getFieldType();
        Assertions.assertEquals(Schema.Type.element, head.getType());
        Assertions.assertEquals(List.of("Umaban", "Odds", "At"), fieldNames(head.getElementSchema()));

        final Schema.FieldType attrs = schema.getField("attrs").getFieldType();
        Assertions.assertEquals(Schema.Type.map, attrs.getType());
        Assertions.assertEquals(Schema.Type.string, attrs.getMapValueType().getType());

        // an array of scalars keeps its (metadata-derived) type
        Assertions.assertEquals(Schema.Type.int64,
                schema.getField("laps").getFieldType().getArrayValueType().getType());

        final Map<String, Object> output = evaluate(query).asPrimitiveMap();
        Assertions.assertEquals(List.of(
                Map.of("Umaban", 1L, "Odds", 25L, "At", AT),
                Map.of("Umaban", 2L, "Odds", 0L, "At", AT)), output.get("info"));
        // the timestamp inside the struct is epoch micros again, as a top-level one is
        Assertions.assertEquals(Map.of("Umaban", 9L, "Odds", 99L, "At", AT), output.get("head"));
        Assertions.assertEquals(Map.of("k", "v"), output.get("attrs"));
        Assertions.assertEquals(List.of(120L, 0L, 118L), output.get("laps"));
    }

    @Test
    public void testSelectStarWithArrayOfStructs() {
        final Schema schema = Schema.of(List.of(
                Schema.Field.of("id", Schema.FieldType.STRING),
                Schema.Field.of("info", Schema.FieldType.array(Schema.FieldType.element(oddsSchema())))));
        final Query2 query = Query2.of("INPUT", schema, "SELECT i.*, CARDINALITY(i.info) AS n FROM INPUT AS i");
        Assertions.assertEquals(List.of("id", "info", "n"), fieldNames(query.getOutputSchema()));
        query.setup();
        try {
            final Map<String, Object> output = query.execute(List.of(MElement.of(Map.of(
                    "id", "a",
                    "info", List.of(Map.of("Umaban", 1L, "Odds", 25L, "At", AT))), TIMESTAMP)), TIMESTAMP)
                    .getFirst().asPrimitiveMap();
            Assertions.assertEquals(
                    List.of(Map.of("Umaban", 1L, "Odds", 25L, "At", AT)), output.get("info"));
            Assertions.assertEquals(1, ((Number) output.get("n")).intValue());
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testRebuiltArrayOfStructs() {
        // A multi-column ARRAY subquery is an array of structs named by the column aliases.
        final Query2 query = Query2.of("INPUT", inputSchema(), """
                SELECT
                  ARRAY(SELECT e.Umaban AS umaban, e.Odds / 10e0 AS odds
                        FROM UNNEST(i.info) AS e WHERE e.Odds > 0) AS odds,
                  ARRAY(SELECT CAST(ROW(e.Umaban, e.Odds / 10e0) AS ROW(umaban BIGINT, odds DOUBLE))
                        FROM UNNEST(i.info) AS e) AS casted
                FROM INPUT AS i
                """);
        final Schema elementSchema = query.getOutputSchema().getField("odds")
                .getFieldType().getArrayValueType().getElementSchema();
        Assertions.assertEquals(List.of("umaban", "odds"), fieldNames(elementSchema));
        Assertions.assertEquals(Schema.Type.int64, elementSchema.getField("umaban").getFieldType().getType());
        Assertions.assertEquals(Schema.Type.float64, elementSchema.getField("odds").getFieldType().getType());

        final Map<String, Object> output = evaluate(query).asPrimitiveMap();
        Assertions.assertEquals(List.of(Map.of("umaban", 1L, "odds", 2.5d)), output.get("odds"));
        Assertions.assertEquals(List.of(
                Map.of("umaban", 1L, "odds", 2.5d),
                Map.of("umaban", 2L, "odds", 0d)), output.get("casted"));
    }

    @Test
    public void testStructAndNestedArray() {
        final Query2 query = Query2.of("INPUT", inputSchema(), """
                SELECT
                  CAST(ROW(i.id, 1) AS ROW(x VARCHAR, y BIGINT)) AS s,
                  ARRAY[i.laps, i.laps] AS nested
                FROM INPUT AS i
                """);
        final Schema schema = query.getOutputSchema();
        Assertions.assertEquals(List.of("x", "y"),
                fieldNames(schema.getField("s").getFieldType().getElementSchema()));
        Assertions.assertEquals(Schema.Type.array,
                schema.getField("nested").getFieldType().getArrayValueType().getType());

        final Map<String, Object> output = evaluate(query).asPrimitiveMap();
        Assertions.assertEquals(Map.of("x", "a", "y", 1L), output.get("s"));
        Assertions.assertEquals(
                List.of(List.of(120L, 0L, 118L), List.of(120L, 0L, 118L)), output.get("nested"));
    }

    @Test
    public void testNullNestedValues() {
        final Query2 query = Query2.of("INPUT", inputSchema(),
                "SELECT i.info AS info, i.head AS head, i.attrs AS attrs FROM INPUT AS i");
        query.setup();
        try {
            final MElement output = query.execute(
                    List.of(MElement.of(Map.of("id", "a"), TIMESTAMP)), TIMESTAMP).getFirst();
            Assertions.assertNull(output.getPrimitiveValue("info"));
            Assertions.assertNull(output.getPrimitiveValue("head"));
            Assertions.assertNull(output.getPrimitiveValue("attrs"));
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testUnnamedStructFieldIsRejected() {
        // ROW(...) has no field names: rejected at construction, as an unnamed column is.
        final RuntimeException e = Assertions.assertThrows(RuntimeException.class,
                () -> Query2.of("INPUT", inputSchema(), """
                        SELECT ARRAY(SELECT ROW(e.Umaban, e.Odds / 10e0) FROM UNNEST(i.info) AS e) AS odds
                        FROM INPUT AS i
                        """));
        final StringBuilder messages = new StringBuilder();
        for (Throwable t = e; t != null; t = t.getCause() == t ? null : t.getCause()) {
            messages.append(t.getMessage()).append('\n');
        }
        Assertions.assertTrue(messages.toString().contains("struct field without a name in output column: odds"),
                messages.toString());
    }
}
