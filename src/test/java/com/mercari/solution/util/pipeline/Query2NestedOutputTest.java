package com.mercari.solution.util.pipeline;

import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Output columns that JDBC metadata cannot describe — a struct, an array of structs, a map: their schema comes from the planned row type and their values are converted
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
        return evaluate(query, input());
    }

    private static MElement evaluate(final Query2 query, final MElement input) {
        query.setup();
        try {
            return query.execute(List.of(input), TIMESTAMP).getFirst();
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
    public void testStruct() {
        final Query2 query = Query2.of("INPUT", inputSchema(), """
                SELECT CAST(ROW(i.id, 1) AS ROW(x VARCHAR, y BIGINT)) AS s
                FROM INPUT AS i
                """);
        final Schema schema = query.getOutputSchema();
        Assertions.assertEquals(List.of("x", "y"),
                fieldNames(schema.getField("s").getFieldType().getElementSchema()));

        final Map<String, Object> output = evaluate(query).asPrimitiveMap();
        Assertions.assertEquals(Map.of("x", "a", "y", 1L), output.get("s"));
    }

    @Test
    public void testArrayOfArraysIsRejected() {
        // The element coder cannot encode a list directly inside a list: rejected at
        // construction rather than failing when the pipeline runs.
        final RuntimeException e = Assertions.assertThrows(RuntimeException.class,
                () -> Query2.of("INPUT", inputSchema(),
                        "SELECT ARRAY[i.laps, i.laps] AS nested FROM INPUT AS i"));
        final StringBuilder messages = new StringBuilder();
        for (Throwable t = e; t != null; t = t.getCause() == t ? null : t.getCause()) {
            messages.append(t.getMessage()).append('\n');
        }
        Assertions.assertTrue(messages.toString().contains("Not supported array of arrays"),
                messages.toString());
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

    @Test
    public void testMapValuesByTheirType() {
        // The values of a map are internal values inside SQL and are converted back by the map's
        // value type: a timestamp / time value must come out as it went in, read from the map
        // (m['k']) or passed through with it.
        final Schema schema = Schema.of(List.of(
                Schema.Field.of("id", Schema.FieldType.STRING),
                Schema.Field.of("stamps", Schema.FieldType.map(Schema.FieldType.TIMESTAMP)),
                Schema.Field.of("times", Schema.FieldType.map(Schema.FieldType.TIME)),
                Schema.Field.of("ratios", Schema.FieldType.map(Schema.FieldType.FLOAT32))));
        final long time = 3_600_000_000L;
        final MElement input = MElement.of(Map.of(
                "id", "a",
                "stamps", Map.of("k", AT),
                "times", Map.of("k", time),
                "ratios", Map.of("k", 1.5f)), TIMESTAMP);
        final Query2 query = Query2.of("INPUT", schema, """
                SELECT
                  i.stamps AS stamps, i.times AS times, i.ratios AS ratios,
                  i.stamps['k'] AS stamp, i.times['k'] AS t, i.ratios['k'] AS ratio
                FROM INPUT AS i
                """);
        final Schema output = query.getOutputSchema();
        Assertions.assertEquals(Schema.Type.timestamp,
                output.getField("stamps").getFieldType().getMapValueType().getType());
        Assertions.assertEquals(Schema.Type.time,
                output.getField("times").getFieldType().getMapValueType().getType());

        final Map<String, Object> values = evaluate(query, input).asPrimitiveMap();
        Assertions.assertEquals(Map.of("k", AT), values.get("stamps"));
        Assertions.assertEquals(Map.of("k", time), values.get("times"));
        Assertions.assertEquals(Map.of("k", 1.5d), values.get("ratios"));
        Assertions.assertEquals(AT, values.get("stamp"));
        Assertions.assertEquals(time, values.get("t"));
        Assertions.assertEquals(1.5d, values.get("ratio"));
    }

    @Test
    public void testMapOfNestedValues() {
        // A map of arrays / structs / maps: the value type keeps its shape in the planned type.
        final Schema schema = Schema.of(List.of(
                Schema.Field.of("id", Schema.FieldType.STRING),
                Schema.Field.of("lists", Schema.FieldType.map(Schema.FieldType.array(Schema.FieldType.INT64))),
                Schema.Field.of("structs", Schema.FieldType.map(Schema.FieldType.element(oddsSchema()))),
                Schema.Field.of("maps", Schema.FieldType.map(Schema.FieldType.map(Schema.FieldType.STRING)))));
        final MElement input = MElement.of(Map.of(
                "id", "a",
                "lists", Map.of("k", List.of(1L, 2L)),
                "structs", Map.of("k", Map.of("Umaban", 1L, "Odds", 25L, "At", AT)),
                "maps", Map.of("k", Map.of("x", "y"))), TIMESTAMP);
        final Query2 query = Query2.of("INPUT", schema,
                "SELECT i.*, i.lists['k'] AS list, i.structs['k'].Odds AS odds FROM INPUT AS i");
        final Schema output = query.getOutputSchema();
        Assertions.assertEquals(Schema.Type.int64, output.getField("lists")
                .getFieldType().getMapValueType().getArrayValueType().getType());
        Assertions.assertEquals(List.of("Umaban", "Odds", "At"), fieldNames(output.getField("structs")
                .getFieldType().getMapValueType().getElementSchema()));
        Assertions.assertEquals(Schema.Type.string, output.getField("maps")
                .getFieldType().getMapValueType().getMapValueType().getType());

        final Map<String, Object> values = evaluate(query, input).asPrimitiveMap();
        Assertions.assertEquals(Map.of("k", List.of(1L, 2L)), values.get("lists"));
        Assertions.assertEquals(
                Map.of("k", Map.of("Umaban", 1L, "Odds", 25L, "At", AT)), values.get("structs"));
        Assertions.assertEquals(Map.of("k", Map.of("x", "y")), values.get("maps"));
        Assertions.assertEquals(List.of(1L, 2L), values.get("list"));
        Assertions.assertEquals(25L, values.get("odds"));
    }

    @Test
    public void testSingleColumnOneFieldStruct() {
        // The only output column is a struct of one field: the result row is the struct's own
        // one-element array, which must not be taken for the row's.
        final Schema single = Schema.of(List.of(Schema.Field.of("v", Schema.FieldType.INT64)));
        final Schema schema = Schema.of(List.of(
                Schema.Field.of("id", Schema.FieldType.STRING),
                Schema.Field.of("solo", Schema.FieldType.element(single)),
                Schema.Field.of("deep", Schema.FieldType.element(Schema.of(List.of(
                        Schema.Field.of("inner", Schema.FieldType.element(single))))))));
        final MElement input = MElement.of(Map.of(
                "id", "a",
                "solo", Map.of("v", 7L),
                "deep", Map.of("inner", Map.of("v", 8L))), TIMESTAMP);

        final Query2 passed = Query2.of("INPUT", schema, "SELECT i.solo AS solo FROM INPUT AS i");
        Assertions.assertEquals(Map.of("v", 7L), evaluate(passed, input).getPrimitiveValue("solo"));
        // a NULL field is not a NULL struct, and a NULL struct stays one
        final Map<String, Object> nullField = new HashMap<>();
        nullField.put("v", null);
        Assertions.assertEquals(nullField, evaluate(passed, MElement.of(
                Map.of("id", "a", "solo", Map.of()), TIMESTAMP)).getPrimitiveValue("solo"));
        Assertions.assertNull(evaluate(passed, MElement.of(
                Map.of("id", "a"), TIMESTAMP)).getPrimitiveValue("solo"));

        final Query2 nested = Query2.of("INPUT", schema, "SELECT i.deep AS deep FROM INPUT AS i");
        Assertions.assertEquals(Map.of("inner", Map.of("v", 8L)),
                evaluate(nested, input).getPrimitiveValue("deep"));

        final Query2 built = Query2.of("INPUT", schema,
                "SELECT CAST(ROW(i.id) AS ROW(x VARCHAR)) AS s FROM INPUT AS i");
        Assertions.assertEquals(Map.of("x", "a"), evaluate(built, input).getPrimitiveValue("s"));

        // a single scalar column is unaffected
        final Query2 scalar = Query2.of("INPUT", schema, "SELECT i.id FROM INPUT AS i");
        Assertions.assertEquals("a", evaluate(scalar, input).getPrimitiveValue("id"));
    }

    @Test
    public void testUntypedNullStructField() {
        // An untyped NULL inside a struct is a string, as a top-level one is.
        final Query2 query = Query2.of("INPUT", inputSchema(), """
                SELECT
                  NULL AS n,
                  ARRAY(SELECT e.Umaban AS umaban, NULL AS note FROM UNNEST(i.info) AS e) AS noted
                FROM INPUT AS i
                """);
        final Schema schema = query.getOutputSchema();
        Assertions.assertEquals(Schema.Type.string, schema.getField("n").getFieldType().getType());
        final Schema elementSchema = schema.getField("noted")
                .getFieldType().getArrayValueType().getElementSchema();
        Assertions.assertEquals(Schema.Type.string, elementSchema.getField("note").getFieldType().getType());

        final List<?> noted = (List<?>) evaluate(query).getPrimitiveValue("noted");
        Assertions.assertEquals(2, noted.size());
        final Map<?, ?> first = (Map<?, ?>) noted.getFirst();
        Assertions.assertEquals(1L, first.get("umaban"));
        Assertions.assertNull(first.get("note"));
    }
}
