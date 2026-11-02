package com.mercari.solution.util.pipeline.select;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;

public class RegexpTest {

    private static final List<Schema.Field> FIELDS = List.of(
            Schema.Field.of("s", Schema.FieldType.STRING),
            Schema.Field.of("n", Schema.FieldType.INT64),
            Schema.Field.of("nested", Schema.FieldType.element(List.of(
                    Schema.Field.of("t", Schema.FieldType.STRING)))),
            Schema.Field.of("records", Schema.FieldType.array(Schema.FieldType.element(List.of(
                    Schema.Field.of("t", Schema.FieldType.STRING))))));

    // items in order, the tied ones in parentheses
    private static final String RANKING = "14-11(1,2,7)(3,8)6(4,12)(13,5)(16,9,15)10";
    private static final String TOKENS = "\\\\([^)]*\\\\)|[0-9]+";

    private static SelectFunction function(final String json) {
        final SelectFunction function = SelectFunction.of(new Gson().fromJson(json, JsonObject.class), FIELDS);
        function.setup();
        return function;
    }

    private static Object apply(final SelectFunction function, final Object s) {
        final Map<String, Object> input = new HashMap<>();
        input.put("s", s);
        return function.apply(input, Instant.EPOCH);
    }

    @Test
    public void testExtractAll() {
        final SelectFunction all = function("{ \"name\": \"o\", \"func\": \"regexp_extract_all\", \"field\": \"s\", \"pattern\": \"" + TOKENS + "\" }");
        Assertions.assertEquals(Schema.Type.array, all.getOutputFieldType().getType());
        Assertions.assertEquals(Schema.Type.string, all.getOutputFieldType().getArrayValueType().getType());
        Assertions.assertEquals(List.of("14", "11", "(1,2,7)", "(3,8)", "6", "(4,12)", "(13,5)", "(16,9,15)", "10"), apply(all, RANKING));
        // no match: an empty array; null: null
        Assertions.assertEquals(List.of(), apply(all, "-"));
        Assertions.assertNull(apply(all, null));

        // a group in place of the whole match, by number or by name; one that takes no part in a match is null
        final SelectFunction inside = function("{ \"name\": \"o\", \"func\": \"regexp_extract_all\", \"field\": \"s\", \"pattern\": \"\\\\(([^)]*)\\\\)|([0-9]+)\", \"group\": 1 }");
        Assertions.assertEquals(Arrays.asList(null, null, "1,2,7", "3,8"), ((List<?>) apply(inside, RANKING)).subList(0, 4));
        final SelectFunction named = function("{ \"name\": \"o\", \"func\": \"regexp_extract_all\", \"field\": \"s\", \"pattern\": \"\\\\((?<tied>[^)]*)\\\\)\", \"group\": \"tied\" }");
        Assertions.assertEquals(List.of("1,2,7", "3,8", "4,12", "13,5", "16,9,15"), apply(named, RANKING));
    }

    @Test
    public void testExtract() {
        final SelectFunction first = function("{ \"name\": \"o\", \"func\": \"regexp_extract\", \"field\": \"s\", \"pattern\": \"\\\\(([^)]*)\\\\)\", \"group\": 1 }");
        Assertions.assertEquals(Schema.Type.string, first.getOutputFieldType().getType());
        Assertions.assertEquals("1,2,7", apply(first, RANKING));
        Assertions.assertNull(apply(first, "14-11"));
        Assertions.assertNull(apply(first, null));
        final SelectFunction whole = function("{ \"name\": \"o\", \"func\": \"regexp_extract\", \"field\": \"nested.t\", \"pattern\": \"[0-9]+\" }");
        final Map<String, Object> input = new HashMap<>();
        input.put("nested", Map.of("t", "ab 42 7"));
        Assertions.assertEquals("42", whole.apply(input, Instant.EPOCH));
        Assertions.assertEquals(List.of("nested"), whole.getInputFields().stream().map(Schema.Field::getName).toList());
    }

    @Test
    public void testReplace() {
        final SelectFunction strip = function("{ \"name\": \"o\", \"func\": \"regexp_replace\", \"field\": \"s\", \"pattern\": \"[()]\", \"replacement\": \"\" }");
        Assertions.assertEquals("14-111,2,73,864,1213,516,9,1510", apply(strip, RANKING));
        Assertions.assertNull(apply(strip, null));
        final SelectFunction groups = function("{ \"name\": \"o\", \"func\": \"regexp_replace\", \"field\": \"s\", \"pattern\": \"(?<a>[0-9]+)-([0-9]+)\", \"replacement\": \"$2/${a} \\\\$\" }");
        Assertions.assertEquals("2/1 $, 4/3 $", apply(groups, "1-2, 3-4"));
        // $10 with one group reads $1 then a 0, as Matcher does
        final SelectFunction digits = function("{ \"name\": \"o\", \"func\": \"regexp_replace\", \"field\": \"s\", \"pattern\": \"([a-z])\", \"replacement\": \"$10\" }");
        Assertions.assertEquals("a0b0", apply(digits, "ab"));
    }

    @Test
    public void testConfigErrors() {
        for(final String json : List.of(
                // no field, no pattern, not a string, a bad pattern
                "{ \"name\": \"o\", \"func\": \"regexp_extract\", \"pattern\": \"a\" }",
                "{ \"name\": \"o\", \"func\": \"regexp_extract\", \"field\": \"s\" }",
                "{ \"name\": \"o\", \"func\": \"regexp_extract\", \"field\": \"n\", \"pattern\": \"a\" }",
                "{ \"name\": \"o\", \"func\": \"regexp_extract_all\", \"field\": \"s\", \"pattern\": \"(a\" }",
                // a field of the records of an array: a list of strings, not a string
                "{ \"name\": \"o\", \"func\": \"regexp_extract\", \"field\": \"records.t\", \"pattern\": \"a\" }",
                "{ \"name\": \"o\", \"func\": \"regexp_replace\", \"field\": \"records.t\", \"pattern\": \"a\", \"replacement\": \"b\" }",
                // a group the pattern lacks, by number and by name; a fraction of a group; a replacement
                "{ \"name\": \"o\", \"func\": \"regexp_extract\", \"field\": \"s\", \"pattern\": \"(a)\", \"group\": 2 }",
                "{ \"name\": \"o\", \"func\": \"regexp_extract_all\", \"field\": \"s\", \"pattern\": \"(a)\", \"group\": \"x\" }",
                "{ \"name\": \"o\", \"func\": \"regexp_extract\", \"field\": \"s\", \"pattern\": \"(a)(b)\", \"group\": 1.5 }",
                "{ \"name\": \"o\", \"func\": \"regexp_extract\", \"field\": \"s\", \"pattern\": \"(a)\", \"replacement\": \"b\" }",
                // a digit of another script is no group number for Matcher
                "{ \"name\": \"o\", \"func\": \"regexp_replace\", \"field\": \"s\", \"pattern\": \"(a)\", \"replacement\": \"$１\" }",
                // replace: no replacement, a group, a reference the pattern lacks, a bare $
                "{ \"name\": \"o\", \"func\": \"regexp_replace\", \"field\": \"s\", \"pattern\": \"a\" }",
                "{ \"name\": \"o\", \"func\": \"regexp_replace\", \"field\": \"s\", \"pattern\": \"(a)\", \"replacement\": \"b\", \"group\": 1 }",
                "{ \"name\": \"o\", \"func\": \"regexp_replace\", \"field\": \"s\", \"pattern\": \"(a)\", \"replacement\": \"$2\" }",
                "{ \"name\": \"o\", \"func\": \"regexp_replace\", \"field\": \"s\", \"pattern\": \"(a)\", \"replacement\": \"${x}\" }",
                "{ \"name\": \"o\", \"func\": \"regexp_replace\", \"field\": \"s\", \"pattern\": \"(a)\", \"replacement\": \"5$\" }")) {
            Assertions.assertThrows(IllegalArgumentException.class, () -> function(json), json);
        }
    }

}
