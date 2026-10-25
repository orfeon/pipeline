package com.mercari.solution.util.pipeline;

import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.udf.ParseDateTimeFunctions;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The PARSE_DATE / PARSE_TIME / PARSE_DATETIME / PARSE_TIMESTAMP built-ins, which replace the
 * Calcite BigQuery-library functions: those misread an unseparated time ({@code %H%M}).
 */
public class QueryParseDateTimeTest {

    private static final Instant TIMESTAMP = Instant.parse("2026-10-03T00:00:00Z");

    private static Schema inputSchema() {
        return Schema.of(List.of(
                Schema.Field.of("hhmm", Schema.FieldType.STRING),
                Schema.Field.of("stamp", Schema.FieldType.STRING)));
    }

    private static MElement evaluate(final String sql, final Map<String, Object> input) {
        final Query query = Query.of("INPUT", inputSchema(), sql);
        query.setup();
        try {
            return query.execute(List.of(MElement.of(input, TIMESTAMP)), TIMESTAMP).getFirst();
        } finally {
            query.teardown();
        }
    }

    private static long timeMicros(final String time) {
        return LocalTime.parse(time).toNanoOfDay() / 1000L;
    }

    private static long epochMicros(final String instant) {
        return java.time.Instant.parse(instant).toEpochMilli() * 1000L;
    }

    @Test
    public void testUnseparatedTime() {
        // Calcite read "1935" as hour 1 + minute 935 = 16:35, and "1005" as 01:05.
        final MElement output = evaluate("""
                SELECT
                  PARSE_TIME("%H%M", "1935") AS t1,
                  PARSE_TIME("%H%M", "1005") AS t2,
                  PARSE_TIME("%H%M", "0005") AS t3,
                  PARSE_TIME("%H%M%S", "193512") AS t4,
                  PARSE_TIME("%I%M%p", "0735PM") AS t5,
                  PARSE_TIME("%H%M", hhmm) AS t6
                FROM INPUT
                """, Map.of("hhmm", "2359", "stamp", "x"));
        Assertions.assertEquals(timeMicros("19:35:00"), output.getAsLong("t1"));
        Assertions.assertEquals(timeMicros("10:05:00"), output.getAsLong("t2"));
        Assertions.assertEquals(timeMicros("00:05:00"), output.getAsLong("t3"));
        Assertions.assertEquals(timeMicros("19:35:12"), output.getAsLong("t4"));
        Assertions.assertEquals(timeMicros("19:35:00"), output.getAsLong("t5"));
        Assertions.assertEquals(timeMicros("23:59:00"), output.getAsLong("t6"));
    }

    @Test
    public void testUnseparatedTimestamp() {
        final MElement output = evaluate("""
                SELECT
                  PARSE_TIMESTAMP("%Y%m%d%H%M", "202610031935") AS ts1,
                  PARSE_TIMESTAMP("%Y%m%d%H%M%S", stamp) AS ts2,
                  PARSE_DATETIME("%Y%m%d%H%M", "202610031005") AS dt1,
                  PARSE_DATE("%Y%m%d", "20261003") AS d1,
                  PARSE_DATE("%Y%j", "2026276") AS d2
                FROM INPUT
                """, Map.of("hhmm", "x", "stamp", "20261003193512"));
        Assertions.assertEquals(epochMicros("2026-10-03T19:35:00Z"), output.getAsLong("ts1"));
        Assertions.assertEquals(epochMicros("2026-10-03T19:35:12Z"), output.getAsLong("ts2"));
        Assertions.assertEquals(epochMicros("2026-10-03T10:05:00Z"), output.getAsLong("dt1"));
        final int day = (int) LocalDate.parse("2026-10-03").toEpochDay();
        Assertions.assertEquals(day, output.getPrimitiveValue("d1"));
        Assertions.assertEquals(day, output.getPrimitiveValue("d2"));
    }

    @Test
    public void testSeparatedFormatsAndTypes() {
        // Separated formats parsed correctly before; they must keep their results and types.
        final Query query = Query.of("INPUT", inputSchema(), """
                SELECT
                  PARSE_TIME("%H:%M", "19:35") AS t1,
                  PARSE_TIME("%H:%M:%S", "9:5:7") AS t2,
                  PARSE_TIME("%T", "19:35:12") AS t3,
                  PARSE_TIMESTAMP("%Y-%m-%d %H:%M", "2026-10-03 19:35") AS ts1,
                  PARSE_DATETIME("%Y-%m-%d %H:%M:%S", "2026-10-03 19:35:12") AS dt1,
                  PARSE_DATE("%F", "2026-10-03") AS d1,
                  PARSE_DATE("%d %b %Y", "03 Oct 2026") AS d2
                FROM INPUT
                """);
        final Schema schema = query.getOutputSchema();
        Assertions.assertEquals(Schema.Type.time, schema.getField("t1").getFieldType().getType());
        Assertions.assertEquals(Schema.Type.timestamp, schema.getField("ts1").getFieldType().getType());
        Assertions.assertEquals(Schema.Type.timestamp, schema.getField("dt1").getFieldType().getType());
        Assertions.assertEquals(Schema.Type.date, schema.getField("d1").getFieldType().getType());
        query.setup();
        try {
            final MElement output = query.execute(
                    List.of(MElement.of(Map.of("hhmm", "x", "stamp", "x"), TIMESTAMP)), TIMESTAMP).getFirst();
            Assertions.assertEquals(timeMicros("19:35:00"), output.getAsLong("t1"));
            Assertions.assertEquals(timeMicros("09:05:07"), output.getAsLong("t2"));
            Assertions.assertEquals(timeMicros("19:35:12"), output.getAsLong("t3"));
            Assertions.assertEquals(epochMicros("2026-10-03T19:35:00Z"), output.getAsLong("ts1"));
            Assertions.assertEquals(epochMicros("2026-10-03T19:35:12Z"), output.getAsLong("dt1"));
            final int day = (int) LocalDate.parse("2026-10-03").toEpochDay();
            Assertions.assertEquals(day, output.getPrimitiveValue("d1"));
            Assertions.assertEquals(day, output.getPrimitiveValue("d2"));
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testTimestampTimeZone() {
        // The string is a wall clock in the given zone; the result is the instant.
        final MElement output = evaluate("""
                SELECT
                  PARSE_TIMESTAMP("%Y%m%d%H%M", "202610031005", "Asia/Tokyo") AS jst,
                  PARSE_TIMESTAMP("%Y%m%d%H%M", "202610031005", "UTC") AS utc,
                  PARSE_TIMESTAMP("%Y%m%d%H%M", "202610031005", "+09:00") AS fixed
                FROM INPUT
                """, Map.of("hhmm", "x", "stamp", "x"));
        Assertions.assertEquals(epochMicros("2026-10-03T01:05:00Z"), output.getAsLong("jst"));
        Assertions.assertEquals(epochMicros("2026-10-03T10:05:00Z"), output.getAsLong("utc"));
        Assertions.assertEquals(epochMicros("2026-10-03T01:05:00Z"), output.getAsLong("fixed"));
    }

    @Test
    public void testTimestampInDaylightSavingTransition() {
        // 02:30 does not exist on 2026-03-08 in New York: shifted forward by the gap (03:30 EDT).
        // 01:30 occurs twice on 2026-11-01: the earlier one (EDT).
        final MElement output = evaluate("""
                SELECT
                  PARSE_TIMESTAMP("%Y-%m-%d %H:%M", "2026-03-08 02:30", "America/New_York") AS gap,
                  PARSE_TIMESTAMP("%Y-%m-%d %H:%M", "2026-11-01 01:30", "America/New_York") AS overlap,
                  PARSE_TIMESTAMP("%Y-%m-%d %H:%M", "2026-03-08 03:30", "America/New_York") AS summer
                FROM INPUT
                """, Map.of("hhmm", "x", "stamp", "x"));
        Assertions.assertEquals(epochMicros("2026-03-08T07:30:00Z"), output.getAsLong("gap"));
        Assertions.assertEquals(epochMicros("2026-11-01T05:30:00Z"), output.getAsLong("overlap"));
        Assertions.assertEquals(epochMicros("2026-03-08T07:30:00Z"), output.getAsLong("summer"));
    }

    @Test
    public void testLiteralTextInFormat() {
        // Literal text is matched as is: Calcite passes it to the Java pattern unquoted, where
        // "T" is an illegal pattern letter.
        final MElement output = evaluate("""
                SELECT
                  PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%S", "2026-10-03T19:35:12") AS iso,
                  PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ", stamp) AS zulu,
                  PARSE_TIME("%H'%M", "19'35") AS quote
                FROM INPUT
                """, Map.of("hhmm", "x", "stamp", "2026-10-03T19:35:12Z"));
        Assertions.assertEquals(epochMicros("2026-10-03T19:35:12Z"), output.getAsLong("iso"));
        Assertions.assertEquals(epochMicros("2026-10-03T19:35:12Z"), output.getAsLong("zulu"));
        Assertions.assertEquals(timeMicros("19:35:00"), output.getAsLong("quote"));
    }

    @Test
    public void testOtherUnseparatedFields() {
        // %W (week of the year) and %e (day of the month) are two digits wide as well:
        // "2026401" was week 4 + weekday 01, and %e directly before %H did not parse at all.
        final MElement output = evaluate("""
                SELECT
                  PARSE_DATE("%Y%W%u", "2026401") AS w1,
                  PARSE_DATE("%Y-%W-%u", "2026-40-1") AS w2,
                  PARSE_TIMESTAMP("%Y%m%e%H", "2026101319") AS e1,
                  PARSE_TIMESTAMP("%Y%m%e%H", "202610 319") AS e2
                FROM INPUT
                """, Map.of("hhmm", "x", "stamp", "x"));
        Assertions.assertEquals(output.getPrimitiveValue("w2"), output.getPrimitiveValue("w1"));
        Assertions.assertEquals(epochMicros("2026-10-13T19:00:00Z"), output.getAsLong("e1"));
        Assertions.assertEquals(epochMicros("2026-10-03T19:00:00Z"), output.getAsLong("e2"));
    }

    @Test
    public void testNonStringArgumentIsCast() {
        // As for the library functions, a non-string argument is cast to a string implicitly.
        final Query query = Query.of("INPUT",
                Schema.of(List.of(Schema.Field.of("ymd", Schema.FieldType.INT64))), """
                SELECT
                  PARSE_DATE("%Y%m%d", ymd) AS d1,
                  PARSE_TIME("%H%M", 1935) AS t1
                FROM INPUT
                """);
        query.setup();
        try {
            final MElement output = query.execute(
                    List.of(MElement.of(Map.of("ymd", 20261003L), TIMESTAMP)), TIMESTAMP).getFirst();
            Assertions.assertEquals(
                    (int) LocalDate.parse("2026-10-03").toEpochDay(), output.getPrimitiveValue("d1"));
            Assertions.assertEquals(timeMicros("19:35:00"), output.getAsLong("t1"));
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testDatesBeforeTheGregorianCutover() {
        // Proleptic Gregorian, as the internal dates are: the Julian calendar is 10 days off in 1500.
        final MElement output = evaluate("""
                SELECT
                  PARSE_DATE("%Y-%m-%d", "1500-03-01") AS d1,
                  PARSE_DATE("%Y%m%d", "00010101") AS d2
                FROM INPUT
                """, Map.of("hhmm", "x", "stamp", "x"));
        Assertions.assertEquals(
                (int) LocalDate.parse("1500-03-01").toEpochDay(), output.getPrimitiveValue("d1"));
        Assertions.assertEquals(
                (int) LocalDate.parse("0001-01-01").toEpochDay(), output.getPrimitiveValue("d2"));
    }

    @Test
    public void testZoneNameInStringDoesNotAffectLaterParses() throws Exception {
        // A zone name in the string switches the SimpleDateFormat to that zone, and an ambiguous
        // abbreviation ("CST") is resolved against the current zone first: the cached parser
        // must not carry the zone over, or the result depends on what the thread parsed before.
        final String format = "%Y-%m-%d %H:%M %Z";
        try {
            ParseDateTimeFunctions.parseTimestamp(format, "2026-01-03 10:05 China Standard Time");
        } catch (final IllegalArgumentException e) {
            Assumptions.abort("this JDK does not know the zone name: " + e.getMessage());
        }
        final Long after = ParseDateTimeFunctions.parseTimestamp(format, "2026-01-03 10:05 CST");
        final Long[] fresh = new Long[1];
        final Thread thread = new Thread(() ->
                fresh[0] = ParseDateTimeFunctions.parseTimestamp(format, "2026-01-03 10:05 CST"));
        thread.start();
        thread.join();
        Assertions.assertNotNull(fresh[0]);
        Assertions.assertEquals(fresh[0], after);
    }

    @Test
    public void testNullArguments() {
        final Map<String, Object> input = new HashMap<>();
        input.put("hhmm", null);
        input.put("stamp", null);
        final MElement output = evaluate("""
                SELECT
                  PARSE_TIME("%H%M", hhmm) AS t1,
                  PARSE_TIMESTAMP("%Y%m%d%H%M%S", stamp) AS ts1,
                  PARSE_DATE("%Y%m%d", stamp) AS d1,
                  PARSE_DATETIME("%Y%m%d%H%M%S", stamp) AS dt1
                FROM INPUT
                """, input);
        Assertions.assertNull(output.getPrimitiveValue("t1"));
        Assertions.assertNull(output.getPrimitiveValue("ts1"));
        Assertions.assertNull(output.getPrimitiveValue("d1"));
        Assertions.assertNull(output.getPrimitiveValue("dt1"));
    }

    @Test
    public void testInvalidInputIsAnError() {
        final Map<String, String> cases = Map.of(
                // out of range
                "PARSE_TIME(\"%H%M\", \"2460\")", "invalid format",
                // the format does not cover the whole string
                "PARSE_TIME(\"%H%M\", \"193512\")", "invalid format",
                "PARSE_DATE(\"%Y%m%d\", \"2026-10-03\")", "invalid format",
                "PARSE_DATE(\"%Y-%m-%d\", \"2026-02-30\")", "invalid format",
                "PARSE_TIME(\"%H:%M\", \"25:00\")", "invalid format",
                "PARSE_TIMESTAMP(\"%Y%m%d\", \"20261003\", \"Nowhere/City\")", "Nowhere/City");
        for (final Map.Entry<String, String> entry : cases.entrySet()) {
            final RuntimeException e = Assertions.assertThrows(RuntimeException.class,
                    () -> evaluate("SELECT " + entry.getKey() + " AS v FROM INPUT",
                            Map.of("hhmm", "x", "stamp", "x")),
                    entry.getKey());
            Throwable root = e;
            while (root.getCause() != null && root.getCause() != root) {
                root = root.getCause();
            }
            Assertions.assertTrue(String.valueOf(root.getMessage()).contains(entry.getValue()),
                    entry.getKey() + ": unexpected message: " + root.getMessage());
        }
    }

    @Test
    public void testUnsupportedFormatNamesTheFunction() {
        // An element without a Java pattern (%Q) is reported with the function and the format.
        final Map<String, String> cases = Map.of(
                "PARSE_DATE(\"%Y-%Q\", \"2026-4\")", "PARSE_DATE: not supported format: '%Y-%Q'");
        for (final Map.Entry<String, String> entry : cases.entrySet()) {
            final RuntimeException e = Assertions.assertThrows(RuntimeException.class,
                    () -> evaluate("SELECT " + entry.getKey() + " AS v FROM INPUT",
                            Map.of("hhmm", "x", "stamp", "x")),
                    entry.getKey());
            final StringBuilder messages = new StringBuilder();
            for (Throwable t = e; t != null; t = t.getCause() == t ? null : t.getCause()) {
                messages.append(t.getMessage()).append('\n');
            }
            Assertions.assertTrue(messages.toString().contains(entry.getValue()),
                    entry.getKey() + ": unexpected messages: " + messages);
        }
    }
}
