package com.mercari.solution.util.pipeline;

import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/**
 * {@code SELECT * EXCEPT(...)} / {@code SELECT * REPLACE(...)}: not in the parser, rewritten into
 * an explicit column list before planning.
 */
public class Query2StarModifierTest {

    private static final Instant TIMESTAMP = Instant.parse("2026-10-03T00:00:00Z");

    private static Schema itemSchema() {
        return Schema.of(List.of(
                Schema.Field.of("num", Schema.FieldType.INT64),
                Schema.Field.of("price", Schema.FieldType.INT64)));
    }

    private static Schema inputSchema() {
        return Schema.of(List.of(
                Schema.Field.of("id", Schema.FieldType.STRING),
                Schema.Field.of("n", Schema.FieldType.INT64),
                Schema.Field.of("items", Schema.FieldType.array(Schema.FieldType.element(itemSchema()))),
                Schema.Field.of("laps", Schema.FieldType.array(Schema.FieldType.INT64))));
    }

    private static MElement input() {
        return MElement.of(Map.of(
                "id", "a",
                "n", 3L,
                "items", List.of(
                        Map.of("num", 1L, "price", 25L),
                        Map.of("num", 2L, "price", 0L)),
                "laps", List.of(120L, 0L, 118L)), TIMESTAMP);
    }

    private static Schema withHeadSchema() {
        return Schema.of(List.of(
                Schema.Field.of("id", Schema.FieldType.STRING),
                Schema.Field.of("head", Schema.FieldType.element(itemSchema())),
                Schema.Field.of("n", Schema.FieldType.INT64)));
    }

    private static MElement withHead() {
        return MElement.of(Map.of(
                "id", "a",
                "head", Map.of("num", 9L, "price", 99L),
                "n", 3L), TIMESTAMP);
    }

    private static Map<String, Object> evaluate(final Query2 query, final MElement input) {
        query.setup();
        try {
            return query.execute(List.of(input), TIMESTAMP).getFirst().asPrimitiveMap();
        } finally {
            query.teardown();
        }
    }

    private static List<String> fieldNames(final Query2 query) {
        return query.getOutputSchema().getFields().stream().map(Schema.Field::getName).toList();
    }

    private static String messages(final Throwable e) {
        final StringBuilder messages = new StringBuilder();
        for (Throwable t = e; t != null; t = t.getCause() == t ? null : t.getCause()) {
            messages.append(t.getMessage()).append('\n');
        }
        return messages.toString();
    }

    @Test
    public void testExcept() {
        final Query2 query = Query2.of("INPUT", inputSchema(), "SELECT * EXCEPT(items, laps) FROM INPUT");
        Assertions.assertEquals(List.of("id", "n"), fieldNames(query));
        Assertions.assertEquals(Map.of("id", "a", "n", 3L), evaluate(query, input()));
    }

    @Test
    public void testExceptAndRebuildColumn() {
        // The use this is for: every column as it is, but one rebuilt.
        final Query2 query = Query2.of("INPUT", inputSchema(), """
                SELECT
                  i.* EXCEPT (items),
                  ARRAY(SELECT e.num AS num, e.price / 10e0 AS price FROM UNNEST(i.items) AS e WHERE e.price > 0) AS items
                FROM INPUT AS i
                """);
        Assertions.assertEquals(List.of("id", "n", "laps", "items"), fieldNames(query));
        final Map<String, Object> output = evaluate(query, input());
        Assertions.assertEquals("a", output.get("id"));
        Assertions.assertEquals(List.of(120L, 0L, 118L), output.get("laps"));
        Assertions.assertEquals(List.of(Map.of("num", 1L, "price", 2.5D)), output.get("items"));
    }

    @Test
    public void testReplace() {
        // The replaced columns keep their place.
        final Query2 query = Query2.of("INPUT", inputSchema(), """
                SELECT * REPLACE (UPPER(id) AS id, CARDINALITY(laps) AS laps)
                FROM INPUT
                """);
        Assertions.assertEquals(List.of("id", "n", "items", "laps"), fieldNames(query));
        final Map<String, Object> output = evaluate(query, input());
        Assertions.assertEquals("A", output.get("id"));
        Assertions.assertEquals(3L, output.get("n"));
        Assertions.assertEquals(3, ((Number) output.get("laps")).intValue());
        Assertions.assertEquals(2, ((List<?>) output.get("items")).size());
    }

    @Test
    public void testExceptAndReplace() {
        final Query2 query = Query2.of("INPUT", inputSchema(), """
                SELECT i.* EXCEPT(items, laps) REPLACE(i.n * 2 AS n), 'x' AS tag
                FROM INPUT AS i
                """);
        Assertions.assertEquals(List.of("id", "n", "tag"), fieldNames(query));
        Assertions.assertEquals(Map.of("id", "a", "n", 6L, "tag", "x"), evaluate(query, input()));
    }

    @Test
    public void testLayoutAndQuoting() {
        // Line breaks (CRLF), tabs, comments, quoted names; a string that looks like a modifier.
        final String sql = "SELECT -- * EXCEPT(id)\r\n"
                + "\t/* * EXCEPT(n) */ i.*\r\n"
                + "\t\tEXCEPT (\r\n\t`items` , -- the array\r\n\t LAPS )\r\n"
                + "\tREPLACE ( CONCAT(i.id, ' * EXCEPT(n), ') AS `id` ),\r\n"
                + "  '* EXCEPT(n)' AS s\r\n"
                + "FROM INPUT AS i";
        final Query2 query = Query2.of("INPUT", inputSchema(), sql);
        Assertions.assertEquals(List.of("id", "n", "s"), fieldNames(query));
        Assertions.assertEquals(
                Map.of("id", "a * EXCEPT(n), ", "n", 3L, "s", "* EXCEPT(n)"), evaluate(query, input()));

        // a // comment is a comment as well: its quote does not open a string
        final Query2 slashes = Query2.of("INPUT", inputSchema(),
                "SELECT // don't keep the arrays\n * EXCEPT(items, laps) // SELECT * EXCEPT(n)\nFROM INPUT");
        Assertions.assertEquals(List.of("id", "n"), fieldNames(slashes));

        // a quoted name with an escaped backtick
        final Query2 escaped = Query2.of("INPUT", inputSchema(),
                "SELECT t.* EXCEPT(`a\\`b`) FROM (SELECT id AS `a\\`b`, n FROM INPUT) AS t");
        Assertions.assertEquals(List.of("n"), fieldNames(escaped));
    }

    @Test
    public void testSubqueryAndWith() {
        final Query2 inSubquery = Query2.of("INPUT", inputSchema(), """
                SELECT t.id, t.n + 1 AS m
                FROM (SELECT * EXCEPT(items) FROM INPUT) AS t
                """);
        Assertions.assertEquals(Map.of("id", "a", "m", 4L), evaluate(inSubquery, input()));

        // over a subquery without an alias, and a common table expression
        final Query2 overSubquery = Query2.of("INPUT", inputSchema(), """
                WITH c AS (SELECT * REPLACE(n * 10 AS n) FROM (SELECT id, n, laps FROM INPUT))
                SELECT * EXCEPT(laps) FROM c
                """);
        Assertions.assertEquals(List.of("id", "n"), fieldNames(overSubquery));
        Assertions.assertEquals(Map.of("id", "a", "n", 30L), evaluate(overSubquery, input()));
    }

    @Test
    public void testNestedModifiers() {
        // A modified star over a select list that has modifiers itself is expanded from what
        // those leave: `items` is no column of t.
        final Query2 overSubquery = Query2.of("INPUT", inputSchema(), """
                SELECT t.* EXCEPT(n) FROM (SELECT * EXCEPT(items) FROM INPUT) AS t
                """);
        Assertions.assertEquals(List.of("id", "laps"), fieldNames(overSubquery));
        Assertions.assertEquals(Map.of("id", "a", "laps", List.of(120L, 0L, 118L)), evaluate(overSubquery, input()));

        final Query2 chained = Query2.of("INPUT", inputSchema(), """
                WITH base AS (SELECT * EXCEPT(items, laps) FROM INPUT),
                     bumped AS (SELECT * REPLACE(n + 1 AS n) FROM base)
                SELECT a.* EXCEPT(id), b.n AS n2, 'x' AS tag
                FROM bumped AS a JOIN base AS b ON a.id = b.id
                """);
        Assertions.assertEquals(List.of("n", "n2", "tag"), fieldNames(chained));
        Assertions.assertEquals(Map.of("n", 4L, "n2", 3L, "tag", "x"), evaluate(chained, input()));

        // in a replacement expression: an array of structs rebuilt in place, one field changed
        final Query2 inReplace = Query2.of("INPUT", inputSchema(), """
                SELECT * EXCEPT(laps)
                  REPLACE(ARRAY(SELECT e.* REPLACE(e.price * 2 AS price) FROM UNNEST(items) AS e) AS items)
                FROM INPUT
                """);
        Assertions.assertEquals(List.of("id", "n", "items"), fieldNames(inReplace));
        Assertions.assertEquals(
                List.of(Map.of("num", 1L, "price", 50L), Map.of("num", 2L, "price", 0L)),
                evaluate(inReplace, input()).get("items"));

        // in the same select list as a modified star
        final Query2 sideBySide = Query2.of("INPUT", inputSchema(), """
                SELECT i.* EXCEPT(items, laps), ARRAY(SELECT e.* EXCEPT(num) FROM UNNEST(i.items) AS e) AS prices
                FROM INPUT AS i
                """);
        Assertions.assertEquals(Map.of("id", "a", "n", 3L, "prices", List.of(25L, 0L)), evaluate(sideBySide, input()));
    }

    @Test
    public void testValidOnlyWithTheModifiers() {
        // The statement with the bare star does not validate: the star has more columns than
        // the other side of the UNION, and a column that is not grouped.
        final Query2 union = Query2.of("INPUT", inputSchema(), """
                SELECT * EXCEPT(items, laps) FROM INPUT
                UNION ALL
                SELECT id, 1.5e0 AS n FROM INPUT
                """);
        Assertions.assertEquals(List.of("id", "n"), fieldNames(union));

        // the columns of a UNION are cast to a common type: `n` of the star here
        final Query2 coerced = Query2.of("INPUT", inputSchema(), """
                SELECT * REPLACE(UPPER(id) AS id) FROM (SELECT id, n FROM INPUT)
                UNION ALL
                SELECT id, 1.5e0 AS n FROM INPUT
                """);
        Assertions.assertEquals(List.of("id", "n"), fieldNames(coerced));

        final Query2 grouped = Query2.of("INPUT", inputSchema(), """
                SELECT * REPLACE(SUM(n) AS n) FROM (SELECT id, n FROM INPUT) GROUP BY id
                """);
        Assertions.assertEquals(Map.of("id", "a", "n", 3L), evaluate(grouped, input()));

        // what does not validate with the modifiers either is reported
        final RuntimeException notGrouped = Assertions.assertThrows(RuntimeException.class,
                () -> Query2.of("INPUT", inputSchema(),
                        "SELECT * REPLACE(SUM(n) AS n) FROM (SELECT id, n FROM INPUT)"));
        Assertions.assertTrue(messages(notGrouped).contains("'id' is not being grouped"), messages(notGrouped));
    }

    @Test
    public void testNaturalAndUsingJoin() {
        // A bare star has the key of a NATURAL / USING join once, first.
        final String from = " FROM (SELECT id, n FROM INPUT) AS a %s (SELECT id, n * 2 AS m FROM INPUT) AS b %s";
        for (final String join : List.of(from.formatted("JOIN", "USING (id)"), from.formatted("NATURAL JOIN", ""))) {
            final Query2 all = Query2.of("INPUT", inputSchema(), "SELECT *" + join);
            Assertions.assertEquals(List.of("id", "n", "m"), fieldNames(all), join);

            final Query2 except = Query2.of("INPUT", inputSchema(), "SELECT * EXCEPT(m)" + join);
            Assertions.assertEquals(List.of("id", "n"), fieldNames(except), join);
            Assertions.assertEquals(Map.of("id", "a", "n", 3L), evaluate(except, input()), join);

            final Query2 exceptKey = Query2.of("INPUT", inputSchema(), "SELECT * EXCEPT(id)" + join);
            Assertions.assertEquals(List.of("n", "m"), fieldNames(exceptKey), join);

            final Query2 replaceKey = Query2.of("INPUT", inputSchema(), "SELECT * REPLACE(UPPER(a.id) AS id)" + join);
            Assertions.assertEquals(Map.of("id", "A", "n", 3L, "m", 6L), evaluate(replaceKey, input()), join);
        }

        // the key given after a modified star is not a column of that star
        final Query2 keyAfterStar = Query2.of("INPUT", inputSchema(),
                "SELECT a.* EXCEPT(id), id" + from.formatted("JOIN", "USING (id)"));
        Assertions.assertEquals(List.of("n", "id"), fieldNames(keyAfterStar));
    }

    @Test
    public void testSeveralStars() {
        final Query2 query = Query2.of("INPUT", inputSchema(), """
                SELECT i.* EXCEPT(items, laps), j.* EXCEPT(k), i.n AS n2
                FROM INPUT AS i, (SELECT id AS k, n * 2 AS m FROM INPUT) AS j
                """);
        Assertions.assertEquals(List.of("id", "n", "m", "n2"), fieldNames(query));
        Assertions.assertEquals(Map.of("id", "a", "n", 3L, "m", 6L, "n2", 3L), evaluate(query, input()));

        // a star without a modifier next to one with
        final Query2 mixed = Query2.of("INPUT", inputSchema(), """
                SELECT j.*, i.* EXCEPT(items, laps, id)
                FROM INPUT AS i, (SELECT id AS k FROM INPUT) AS j
                """);
        Assertions.assertEquals(List.of("k", "n"), fieldNames(mixed));
    }

    @Test
    public void testStructColumn() {
        // A star expands a top-level struct column into its fields: the modifiers take the
        // column as a whole.
        final Query2 all = Query2.of("INPUT", withHeadSchema(), "SELECT * FROM INPUT");
        Assertions.assertEquals(List.of("id", "num", "price", "n"), fieldNames(all));

        for (final String star : List.of("*", "i.*")) {
            final Query2 except = Query2.of("INPUT", withHeadSchema(),
                    "SELECT " + star + " EXCEPT(head) FROM INPUT AS i");
            Assertions.assertEquals(List.of("id", "n"), fieldNames(except), star);
            Assertions.assertEquals(Map.of("id", "a", "n", 3L), evaluate(except, withHead()), star);

            final Query2 replace = Query2.of("INPUT", withHeadSchema(),
                    "SELECT " + star + " REPLACE(i.head.price AS head) FROM INPUT AS i");
            Assertions.assertEquals(List.of("id", "head", "n"), fieldNames(replace), star);
            Assertions.assertEquals(Map.of("id", "a", "head", 99L, "n", 3L), evaluate(replace, withHead()), star);

            // one of the fields the star expanded the column into
            final Query2 field = Query2.of("INPUT", withHeadSchema(),
                    "SELECT " + star + " EXCEPT(price) FROM INPUT AS i");
            Assertions.assertEquals(List.of("id", "num", "n"), fieldNames(field), star);
        }
    }

    @Test
    public void testStructColumnFirst() {
        // The fields of a struct column have no position of their own under a bare star: they
        // are columns of the star all the same when the struct is its first column.
        final Schema schema = Schema.of(List.of(
                Schema.Field.of("head", Schema.FieldType.element(itemSchema())),
                Schema.Field.of("id", Schema.FieldType.STRING),
                Schema.Field.of("n", Schema.FieldType.INT64)));

        for (final String star : List.of("*", "i.*")) {
            final Query2 except = Query2.of("INPUT", schema, "SELECT " + star + " EXCEPT(n) FROM INPUT AS i");
            Assertions.assertEquals(List.of("num", "price", "id"), fieldNames(except), star);
            Assertions.assertEquals(Map.of("num", 9L, "price", 99L, "id", "a"), evaluate(except, withHead()), star);

            final Query2 replace = Query2.of("INPUT", schema,
                    "SELECT " + star + " REPLACE(i.head.price AS head) FROM INPUT AS i");
            Assertions.assertEquals(List.of("head", "id", "n"), fieldNames(replace), star);
        }

        // ... and those of a star without a modifier are not columns of the modified star before it
        final Query2 mixed = Query2.of("INPUT", schema, """
                SELECT j.* EXCEPT(k), *
                FROM INPUT AS i, (SELECT id AS k, n AS m FROM INPUT) AS j
                """);
        Assertions.assertEquals(List.of("m", "num", "price", "id", "n", "k", "m0"), fieldNames(mixed));
    }

    @Test
    public void testNotAModifier() {
        // a multiplication by the REPLACE function, and the EXCEPT set operator
        Assertions.assertTrue(SelectStarModifiers
                .extract("SELECT n * REPLACE(s, 'a', 'b') AS m, COUNT(*) AS c FROM t").modifiers().isEmpty());
        Assertions.assertTrue(SelectStarModifiers
                .extract("SELECT * FROM a EXCEPT (SELECT * FROM b)").modifiers().isEmpty());
        Assertions.assertTrue(SelectStarModifiers
                .extract("SELECT '* EXCEPT(a)' AS s, `* EXCEPT(a)` FROM t -- * EXCEPT(a)").modifiers().isEmpty());
        Assertions.assertTrue(SelectStarModifiers
                .extract("SELECT a // was: SELECT * EXCEPT(b)\nFROM t").modifiers().isEmpty());

        final Query2 query = Query2.of("INPUT", inputSchema(),
                "SELECT i.n * CHAR_LENGTH(REPLACE(i.id, 'a', 'bb')) AS m FROM INPUT AS i");
        Assertions.assertEquals(Map.of("m", 6L), evaluate(query, input()));
    }

    @Test
    public void testExtractKeepsPositions() {
        final String sql = "SELECT i.* EXCEPT(a,\n b) REPLACE(x + 1 AS x), c\nFROM t AS i";
        final SelectStarModifiers.Extracted extracted = SelectStarModifiers.extract(sql);
        Assertions.assertEquals(
                "SELECT i.*" + " ".repeat(10) + "\n" + " ".repeat(23) + ", c\nFROM t AS i", extracted.sql());
        Assertions.assertEquals(1, extracted.modifiers().size());
        final SelectStarModifiers.Modifier modifier = extracted.modifiers().getFirst();
        Assertions.assertEquals(1, modifier.line());
        Assertions.assertEquals(10, modifier.column());
        Assertions.assertEquals(List.of("a", "b"), modifier.except());
        Assertions.assertEquals("x", modifier.replaces().getFirst().name());
        Assertions.assertEquals("x + 1", sql.substring(
                modifier.replaces().getFirst().start(), modifier.replaces().getFirst().end()));
        Assertions.assertEquals("EXCEPT(a,\n b) REPLACE(x + 1 AS x)", sql.substring(modifier.start(), modifier.end()));

        // the modifiers of a star in a replacement expression are found as well
        final SelectStarModifiers.Extracted nested = SelectStarModifiers.extract(
                "SELECT * REPLACE(ARRAY(SELECT e.* EXCEPT(a) FROM UNNEST(x) AS e) AS x) FROM t");
        Assertions.assertEquals("SELECT *" + " ".repeat(62) + " FROM t", nested.sql());
        Assertions.assertEquals(List.of(8, 33),
                nested.modifiers().stream().map(SelectStarModifiers.Modifier::column).toList());
    }

    @Test
    public void testErrors() {
        final RuntimeException unknown = Assertions.assertThrows(RuntimeException.class,
                () -> Query2.of("INPUT", inputSchema(), "SELECT * EXCEPT(nope) FROM INPUT"));
        Assertions.assertTrue(messages(unknown).contains(
                "SELECT * EXCEPT: column nope is not a column of the star at line 1, column 8"
                        + " (columns: id, n, items, laps)"), messages(unknown));

        final RuntimeException unknownReplace = Assertions.assertThrows(RuntimeException.class,
                () -> Query2.of("INPUT", inputSchema(), "SELECT * REPLACE(1 AS nope) FROM INPUT"));
        Assertions.assertTrue(messages(unknownReplace).contains("SELECT * REPLACE: column nope is not a column"),
                messages(unknownReplace));

        final RuntimeException nothingLeft = Assertions.assertThrows(RuntimeException.class,
                () -> Query2.of("INPUT", inputSchema(), "SELECT * EXCEPT(id, n, items, laps) FROM INPUT"));
        Assertions.assertTrue(messages(nothingLeft).contains("no column is left"), messages(nothingLeft));

        final RuntimeException twice = Assertions.assertThrows(RuntimeException.class,
                () -> Query2.of("INPUT", inputSchema(), "SELECT * EXCEPT(n) REPLACE(1 AS n) FROM INPUT"));
        Assertions.assertTrue(messages(twice).contains("given more than once"), messages(twice));

        final RuntimeException exceptTwice = Assertions.assertThrows(RuntimeException.class,
                () -> Query2.of("INPUT", inputSchema(), "SELECT * EXCEPT(items, ITEMS) FROM INPUT"));
        Assertions.assertTrue(messages(exceptTwice).contains(
                "SELECT * EXCEPT: column ITEMS is given more than once"), messages(exceptTwice));

        // a struct column is named in the list of the columns, not its fields
        final RuntimeException unknownOfStruct = Assertions.assertThrows(RuntimeException.class,
                () -> Query2.of("INPUT", withHeadSchema(), "SELECT * EXCEPT(nope) FROM INPUT"));
        Assertions.assertTrue(messages(unknownOfStruct).contains("(columns: id, head, n)"), messages(unknownOfStruct));

        // an error in a select list over a rewritten one
        final RuntimeException overRewritten = Assertions.assertThrows(RuntimeException.class,
                () -> Query2.of("INPUT", inputSchema(),
                        "SELECT t.* EXCEPT(n), t.items AS x FROM (SELECT * EXCEPT(items) FROM INPUT) AS t"));
        Assertions.assertTrue(messages(overRewritten).contains("Column 'items' not found in table 't'"),
                messages(overRewritten));

        // an error elsewhere in the statement is reported at its place in the original SQL
        final RuntimeException elsewhere = Assertions.assertThrows(RuntimeException.class,
                () -> Query2.of("INPUT", inputSchema(), "SELECT * EXCEPT(items, laps), nope AS x FROM INPUT"));
        Assertions.assertTrue(messages(elsewhere).contains("line 1, column 31"), messages(elsewhere));

        final RuntimeException inExpression = Assertions.assertThrows(RuntimeException.class,
                () -> Query2.of("INPUT", inputSchema(), "SELECT * REPLACE(nope + 1 AS n) FROM INPUT"));
        Assertions.assertTrue(messages(inExpression).contains("line 1, column 18"), messages(inExpression));
    }

}
