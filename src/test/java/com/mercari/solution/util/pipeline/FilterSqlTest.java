package com.mercari.solution.util.pipeline;

import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tests for SQL-like filter condition text ({@link FilterSqlParser}) and
 * field-to-field comparison ({@code valueKey}).
 */
public class FilterSqlTest {

    private static boolean eval(final String filter, final Map<String, Object> values) {
        return Filter.filter(Filter.parse(filter), values);
    }

    private static void assertEquivalent(final String sql, final String json, final List<Map<String, Object>> records) {
        final Filter.ConditionNode sqlNode = Filter.parse(sql);
        final Filter.ConditionNode jsonNode = Filter.parse(json);
        for(final Map<String, Object> record : records) {
            Assertions.assertEquals(
                    Filter.filter(jsonNode, record),
                    Filter.filter(sqlNode, record),
                    "sql: " + sql + " vs json: " + json + " for record: " + record);
        }
    }

    @Test
    public void testSqlJsonEquivalence() {
        final List<Map<String, Object>> records = List.of(
                Map.of("stringField", "stringValue", "longField", 100L),
                Map.of("stringField", "stringValue", "longField", 99L),
                Map.of("stringField", "other", "longField", 150L),
                Map.of("stringField", "another", "longField", 50L));

        assertEquivalent(
                "stringField = 'stringValue' AND longField >= 100",
                """
                [
                  { "key": "stringField", "op": "=", "value": "stringValue" },
                  { "key": "longField", "op": ">=", "value": 100 }
                ]
                """,
                records);

        assertEquivalent(
                "longField = 100 OR (stringField = 'other' AND longField > 120)",
                """
                { "or": [
                  { "key": "longField", "op": "=", "value": 100 },
                  { "and": [
                    { "key": "stringField", "op": "=", "value": "other" },
                    { "key": "longField", "op": ">", "value": 120 }
                  ] }
                ] }
                """,
                records);

        assertEquivalent(
                "stringField IN ('stringValue', 'other')",
                """
                [ { "key": "stringField", "op": "in", "value": ["stringValue", "other"] } ]
                """,
                records);

        assertEquivalent(
                "longField NOT IN (99, 150)",
                """
                [ { "key": "longField", "op": "not in", "value": [99, 150] } ]
                """,
                records);

        // NOT is pushed down (De Morgan)
        assertEquivalent(
                "NOT (stringField = 'stringValue' OR longField < 100)",
                """
                [
                  { "key": "stringField", "op": "!=", "value": "stringValue" },
                  { "key": "longField", "op": ">=", "value": 100 }
                ]
                """,
                records);

        // literal on the left side
        assertEquivalent(
                "100 <= longField",
                """
                [ { "key": "longField", "op": ">=", "value": 100 } ]
                """,
                records);

        // <> and != are equivalent
        assertEquivalent(
                "stringField <> 'other'",
                "stringField != 'other'",
                records);
    }

    @Test
    public void testFieldToFieldComparison() {
        Assertions.assertTrue(eval("field1 = field2", Map.of("field1", "a", "field2", "a")));
        Assertions.assertFalse(eval("field1 = field2", Map.of("field1", "a", "field2", "b")));
        Assertions.assertTrue(eval("field1 != field2", Map.of("field1", "a", "field2", "b")));

        // numeric comparison across integer types
        Assertions.assertTrue(eval("field1 > field2", Map.of("field1", 10L, "field2", 5)));
        Assertions.assertTrue(eval("field1 = field2", Map.of("field1", 1L, "field2", 1.0D)));

        // large longs beyond double precision must compare exactly
        Assertions.assertTrue(eval("field1 > field2", Map.of("field1", 9007199254740993L, "field2", 9007199254740992L)));
        Assertions.assertFalse(eval("field1 = field2", Map.of("field1", 9007199254740993L, "field2", 9007199254740992L)));

        // timestamps (joda and java.time, also mixed)
        Assertions.assertTrue(eval("updatedAt > createdAt", Map.of(
                "updatedAt", Instant.parse("2024-01-02T00:00:00Z"),
                "createdAt", Instant.parse("2024-01-01T00:00:00Z"))));
        Assertions.assertFalse(eval("updatedAt > createdAt", Map.of(
                "updatedAt", Instant.parse("2024-01-01T00:00:00Z"),
                "createdAt", Instant.parse("2024-01-01T00:00:00Z"))));
        Assertions.assertTrue(eval("updatedAt = createdAt", Map.of(
                "updatedAt", java.time.Instant.parse("2024-01-01T00:00:00Z"),
                "createdAt", Instant.parse("2024-01-01T00:00:00Z"))));

        // null handling: mirrors the constant comparison semantics
        final Map<String, Object> withNull = new HashMap<>();
        withNull.put("field1", "a");
        withNull.put("field2", null);
        Assertions.assertFalse(eval("field1 = field2", withNull));
        Assertions.assertTrue(eval("field1 != field2", withNull));

        final Map<String, Object> bothNull = new HashMap<>();
        bothNull.put("field1", null);
        bothNull.put("field2", null);
        Assertions.assertTrue(eval("field1 = field2", bothNull));
        Assertions.assertFalse(eval("field1 != field2", bothNull));
    }

    @Test
    public void testValueKeyJsonSyntax() {
        final String json = """
                [ { "key": "field1", "op": "=", "valueKey": "field2" } ]
                """;
        final Filter.ConditionNode node = Filter.parse(json);
        Assertions.assertTrue(Filter.filter(node, Map.of("field1", "a", "field2", "a")));
        Assertions.assertFalse(Filter.filter(node, Map.of("field1", "a", "field2", "b")));
        Assertions.assertEquals(Set.of("field1", "field2"), node.getRequiredVariables());

        // valueKey is not allowed for in / match ops
        Assertions.assertThrows(IllegalArgumentException.class, () -> Filter.parse("""
                [ { "key": "field1", "op": "in", "valueKey": "field2" } ]
                """));
        // value and valueKey are exclusive
        Assertions.assertThrows(IllegalArgumentException.class, () -> Filter.parse("""
                [ { "key": "field1", "op": "=", "value": 1, "valueKey": "field2" } ]
                """));
    }

    @Test
    public void testLike() {
        Assertions.assertTrue(eval("name LIKE 'ab%z'", Map.of("name", "abcz")));
        Assertions.assertTrue(eval("name LIKE 'ab%z'", Map.of("name", "abz")));
        Assertions.assertFalse(eval("name LIKE 'ab%z'", Map.of("name", "abc")));
        Assertions.assertFalse(eval("name LIKE 'ab%z'", Map.of("name", "xabz")));

        Assertions.assertTrue(eval("name LIKE 'a_c'", Map.of("name", "abc")));
        Assertions.assertFalse(eval("name LIKE 'a_c'", Map.of("name", "abbc")));

        // regex meta characters in the pattern are escaped
        Assertions.assertTrue(eval("name LIKE '10.5%'", Map.of("name", "10.55")));
        Assertions.assertFalse(eval("name LIKE '10.5%'", Map.of("name", "1035")));

        Assertions.assertTrue(eval("name NOT LIKE 'ab%'", Map.of("name", "xyz")));
        Assertions.assertFalse(eval("name NOT LIKE 'ab%'", Map.of("name", "abc")));

        // NOT (... LIKE ...) equals NOT LIKE
        Assertions.assertTrue(eval("NOT (name LIKE 'ab%')", Map.of("name", "xyz")));
        Assertions.assertFalse(eval("NOT (name LIKE 'ab%')", Map.of("name", "abc")));
    }

    @Test
    public void testIsNull() {
        final Map<String, Object> nullValue = new HashMap<>();
        nullValue.put("field1", null);

        Assertions.assertTrue(eval("field1 IS NULL", nullValue));
        Assertions.assertFalse(eval("field1 IS NULL", Map.of("field1", "a")));
        Assertions.assertFalse(eval("field1 IS NOT NULL", nullValue));
        Assertions.assertTrue(eval("field1 IS NOT NULL", Map.of("field1", "a")));
    }

    @Test
    public void testBetween() {
        Assertions.assertTrue(eval("v BETWEEN 5 AND 10", Map.of("v", 5L)));
        Assertions.assertTrue(eval("v BETWEEN 5 AND 10", Map.of("v", 7L)));
        Assertions.assertTrue(eval("v BETWEEN 5 AND 10", Map.of("v", 10L)));
        Assertions.assertFalse(eval("v BETWEEN 5 AND 10", Map.of("v", 4L)));
        Assertions.assertFalse(eval("v BETWEEN 5 AND 10", Map.of("v", 11L)));

        Assertions.assertFalse(eval("v NOT BETWEEN 5 AND 10", Map.of("v", 7L)));
        Assertions.assertTrue(eval("v NOT BETWEEN 5 AND 10", Map.of("v", 11L)));

        // field bounds
        Assertions.assertTrue(eval("v BETWEEN low AND high", Map.of("v", 7L, "low", 5L, "high", 10L)));
        Assertions.assertFalse(eval("v BETWEEN low AND high", Map.of("v", 12L, "low", 5L, "high", 10L)));
    }

    @Test
    public void testArithmeticExpression() {
        // one side arithmetic, one side numeric literal: same as JSON expression syntax
        assertEquivalent(
                "(field1 - field2) / field3 >= 1",
                """
                [ { "expression": "(field1 - field2) / field3", "op": ">=", "value": 1 } ]
                """,
                List.of(
                        Map.of("field1", 10L, "field2", 2L, "field3", 4L),
                        Map.of("field1", 3L, "field2", 2L, "field3", 4L)));

        // both sides reference fields: folded comparison
        Assertions.assertTrue(eval("price * qty > total", Map.of("price", 2.0D, "qty", 3L, "total", 5.0D)));
        Assertions.assertFalse(eval("price * qty > total", Map.of("price", 2.0D, "qty", 3L, "total", 7.0D)));

        // negation inverts the folded comparison
        Assertions.assertFalse(eval("NOT (price * qty > total)", Map.of("price", 2.0D, "qty", 3L, "total", 5.0D)));
        Assertions.assertTrue(eval("NOT (price * qty > total)", Map.of("price", 2.0D, "qty", 3L, "total", 7.0D)));
    }

    @Test
    public void testBooleanField() {
        Assertions.assertTrue(eval("flag", Map.of("flag", true)));
        Assertions.assertFalse(eval("flag", Map.of("flag", false)));
        Assertions.assertFalse(eval("NOT flag", Map.of("flag", true)));
        Assertions.assertTrue(eval("NOT flag", Map.of("flag", false)));
        Assertions.assertTrue(eval("flag = true", Map.of("flag", true)));
        Assertions.assertTrue(eval("flag IS TRUE", Map.of("flag", true)));
        Assertions.assertTrue(eval("flag IS FALSE", Map.of("flag", false)));

        // null boolean matches neither `flag` nor `NOT flag` (SQL three-valued logic)
        final Map<String, Object> nullFlag = new HashMap<>();
        nullFlag.put("flag", null);
        Assertions.assertFalse(eval("flag", nullFlag));
        Assertions.assertFalse(eval("NOT flag", nullFlag));
    }

    @Test
    public void testNestedFieldAndBackQuote() {
        Assertions.assertTrue(eval("attr.category = 'x'", Map.of("attr", Map.of("category", "x"))));
        Assertions.assertFalse(eval("attr.category = 'x'", Map.of("attr", Map.of("category", "y"))));

        // reserved words as field names need back-quotes (BigQuery lex)
        Assertions.assertTrue(eval("`timestamp` = 'a'", Map.of("timestamp", "a")));
    }

    @Test
    public void testInWithFieldReferences() {
        Assertions.assertTrue(eval("a IN (b, c)", Map.of("a", 1L, "b", 1L, "c", 2L)));
        Assertions.assertTrue(eval("a IN (b, c)", Map.of("a", 2L, "b", 1L, "c", 2L)));
        Assertions.assertFalse(eval("a IN (b, c)", Map.of("a", 3L, "b", 1L, "c", 2L)));
        Assertions.assertFalse(eval("a NOT IN (b, 5)", Map.of("a", 5L, "b", 1L)));
        Assertions.assertTrue(eval("a NOT IN (b, 5)", Map.of("a", 3L, "b", 1L)));
    }

    @Test
    public void testTimestampStringComparison() {
        Assertions.assertTrue(eval("ts > '2021-08-21T00:00:00Z'",
                Map.of("ts", Instant.parse("2021-08-22T00:00:00Z"))));
        Assertions.assertFalse(eval("ts > '2021-08-21T00:00:00Z'",
                Map.of("ts", Instant.parse("2021-08-20T00:00:00Z"))));
    }

    @Test
    public void testConstantTrueCondition() {
        // constant TRUE means "no condition": always matched
        Assertions.assertTrue(eval("true", Map.of("a", 1L)));
        Assertions.assertTrue(eval("a = 1 OR true", Map.of("a", 2L)));
        Assertions.assertTrue(eval("true AND true", Map.of("a", 1L)));
        // neutral TRUE term under AND is dropped
        Assertions.assertFalse(eval("a = 1 AND true", Map.of("a", 2L)));

        // boolean primitive false (e.g. YAML `filter: false`) never matches
        Assertions.assertFalse(eval("false", Map.of("a", 1L)));
    }

    @Test
    public void testRequiredVariablesAndValidate() {
        final Filter.ConditionNode node = Filter.parse("field1 = field2 AND field3 > 1");
        Assertions.assertEquals(Set.of("field1", "field2", "field3"), node.getRequiredVariables());
    }

    @Test
    public void testFilterLifecycle() {
        final Filter filter = Filter.of("stringField = 'a' AND longField > 1");
        filter.setup();
        Assertions.assertTrue(filter.filter(Map.of("stringField", "a", "longField", 2L)));
        Assertions.assertFalse(filter.filter(Map.of("stringField", "b", "longField", 2L)));
        // toJson keeps the SQL text as a string value
        Assertions.assertTrue(filter.toJson().isJsonPrimitive());
        Assertions.assertEquals("stringField = 'a' AND longField > 1", filter.toJson().getAsString());
    }

    @Test
    public void testParseErrors() {
        // broken syntax
        Assertions.assertThrows(IllegalArgumentException.class, () -> Filter.parse("a >>> b"));
        // constant comparison
        Assertions.assertThrows(IllegalArgumentException.class, () -> Filter.parse("1 = 2"));
        // constant false inside a SQL condition
        Assertions.assertThrows(IllegalArgumentException.class, () -> Filter.parse("a = 1 AND false"));
        // LIKE pattern must be a literal
        Assertions.assertThrows(IllegalArgumentException.class, () -> Filter.parse("name LIKE field2"));
        // string function is not supported (arithmetic comparison expects numeric)
        Assertions.assertThrows(IllegalArgumentException.class, () -> Filter.parse("upper(name) = 'A'"));
        // unknown function in arithmetic fails at expression compile
        Assertions.assertThrows(IllegalArgumentException.class, () -> Filter.parse("myfunc(a) > 1"));
    }

    @Test
    public void testNotMatchOpJsonSyntax() {
        final Filter.ConditionNode node = Filter.parse("""
                [ { "key": "name", "op": "not match", "value": "^ab" } ]
                """);
        Assertions.assertTrue(Filter.filter(node, Map.of("name", "xyz")));
        Assertions.assertFalse(Filter.filter(node, Map.of("name", "abc")));
    }

}
