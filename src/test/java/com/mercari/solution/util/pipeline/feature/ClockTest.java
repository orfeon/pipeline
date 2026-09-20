package com.mercari.solution.util.pipeline.feature;

import com.google.gson.JsonObject;
import com.mercari.solution.config.Config;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.*;

/**
 * Calendar clocks: the ordinal / far edge arithmetic, the parsing of a clock declaration, and windows / decay
 * measured in ticks — the incremental and the scan path against a direct count over the business days.
 */
public class ClockTest {

    private static long millis(final String date, final int hour) {
        return LocalDate.parse(date).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli() + hour * 3_600_000L;
    }

    /** Monday 2025-01-06 .. Friday 2025-01-17, weekdays only (10 ticks). */
    private static Clock twoWeeks() {
        final List<LocalDate> dates = new ArrayList<>();
        for (LocalDate d = LocalDate.parse("2025-01-06"); d.isBefore(LocalDate.parse("2025-01-18")); d = d.plusDays(1)) {
            if (d.getDayOfWeek() != DayOfWeek.SATURDAY && d.getDayOfWeek() != DayOfWeek.SUNDAY) dates.add(d);
        }
        return Clock.of("business", dates);
    }

    @Test
    public void testOrdinalAndFarEdge() {
        final Clock clock = twoWeeks();
        Assertions.assertEquals(10, clock.size());
        Assertions.assertEquals(-1, clock.ordinal(millis("2025-01-05", 12)), "before the first tick");
        Assertions.assertEquals(0, clock.ordinal(millis("2025-01-06", 0)));
        Assertions.assertEquals(4, clock.ordinal(millis("2025-01-10", 23)), "Friday");
        Assertions.assertEquals(4, clock.ordinal(millis("2025-01-12", 9)), "a Sunday sits on the Friday before it");
        Assertions.assertEquals(5, clock.ordinal(millis("2025-01-13", 9)));
        Assertions.assertEquals(9, clock.ordinal(millis("2025-02-01", 9)), "after the last tick: the last ordinal");
        // Monday − 1 tick = the previous Friday; the window keeps the Friday, the weekend and the Monday
        Assertions.assertEquals(millis("2025-01-10", 0), clock.farEdgeMillis(millis("2025-01-13", 15), 1));
        Assertions.assertEquals(millis("2025-01-13", 0), clock.farEdgeMillis(millis("2025-01-13", 15), 0));
        Assertions.assertEquals(millis("2025-01-06", 0), clock.farEdgeMillis(millis("2025-01-13", 15), 5));
        Assertions.assertEquals(Long.MIN_VALUE, clock.farEdgeMillis(millis("2025-01-13", 15), 6), "reaches before the first tick");
        Assertions.assertEquals(3.0, clock.distance(millis("2025-01-15", 1), millis("2025-01-10", 23)), 0);
        Assertions.assertEquals(11 * 86_400_000L / 9, clock.meanSpacingMillis(), "11 days over 9 intervals");
    }

    @Test
    public void testParseDatesAndDeclaration() {
        Assertions.assertEquals(List.of("2025-01-06", "2025-01-07"), Clock.parseDates("date,open\n2025-01-06,1\n# holiday\n2025-01-07,1\n"));
        Assertions.assertEquals(List.of("2025-01-06"), Clock.parseDates("[\"2025-01-06\"]"));
        Assertions.assertEquals(List.of("2025-01-06"), Clock.parseDates("{\"dates\": [\"2025-01-06\"]}"));

        final Diagnostics diagnostics = new Diagnostics();
        final Map<String, Clock> clocks = Clock.parseAll(Config.convertConfigJson("""
                sources: []
                clocks:
                  - {name: business, type: calendar, dates: [2025-01-07, 2025-01-06, 2025-01-06]}
                  - {name: time, dates: [2025-01-06]}
                  - {name: bad, dates: [yesterday]}
                  - {name: empty, dates: []}
                  - {name: remote, uri: "gs://bucket/days.csv"}
                """, Config.Format.yaml), diagnostics);
        Assertions.assertEquals(Set.of("business"), clocks.keySet(), "a built-in name, bad / missing dates and an unresolved uri are rejected");
        Assertions.assertEquals(2, clocks.get("business").size(), "sorted, duplicates removed");
        final Set<String> codes = new HashSet<>();
        diagnostics.getMessages().forEach(m -> codes.add(m.code()));
        Assertions.assertTrue(codes.containsAll(Set.of("sources.clocks.name", "sources.clocks.dates", "sources.clocks.uri")), codes::toString);
    }

    /** A clock's {@code uri} is read at resolution into its dates and the content hash (what the plan hash then covers). */
    @Test
    public void testClockUriIsResolved() throws java.io.IOException {
        final java.nio.file.Path dir = java.nio.file.Files.createDirectories(java.nio.file.Path.of("target", "clock-test"));
        final java.nio.file.Path file = dir.resolve(UUID.randomUUID() + ".csv");
        java.nio.file.Files.writeString(file, "date\n2025-01-06\n2025-01-07\n");
        final JsonObject parameters = Config.convertConfigJson("""
                sources:
                  sources: []
                  clocks:
                    - {name: business, uri: "FILE"}
                features: []
                """.replace("FILE", file.toString().replace('\\', '/')), Config.Format.yaml);
        final FeaturePlanService.Documents documents = FeaturePlanService.resolve(parameters, Map.of());
        final JsonObject clock = documents.sources().getAsJsonObject().getAsJsonArray("clocks").get(0).getAsJsonObject();
        Assertions.assertEquals(2, clock.getAsJsonArray("dates").size());
        // the hash covers the ticks, not the file's formatting: a comment / header change leaves the plan hash put
        Assertions.assertEquals(FeaturePlanCompiler.sha256(FeaturePlanCompiler.canonical(clock.getAsJsonArray("dates"))),
                clock.get("hash").getAsString());
        final java.nio.file.Path reformatted = dir.resolve(UUID.randomUUID() + ".csv");
        java.nio.file.Files.writeString(reformatted, "# trading days\nsession_date,open\n2025-01-06,1\n2025-01-07,1\n");
        final JsonObject other = FeaturePlanService.resolve(Config.convertConfigJson("""
                sources:
                  sources: []
                  clocks:
                    - {name: business, uri: "FILE"}
                features: []
                """.replace("FILE", reformatted.toString().replace('\\', '/')), Config.Format.yaml), Map.of())
                .sources().getAsJsonObject().getAsJsonArray("clocks").get(0).getAsJsonObject();
        Assertions.assertEquals(clock.get("hash").getAsString(), other.get("hash").getAsString());
        final Diagnostics diagnostics = new Diagnostics();
        Assertions.assertEquals(2, Clock.parseAll(documents.sources(), diagnostics).get("business").size());
        Assertions.assertFalse(diagnostics.hasErrors());
    }

    private static final String SOURCES = """
            sources:
              - name: listings
                eventTime: session_time
                keys: [session_id, seller_id]
                fields:
                  - {name: session_id, type: string}
                  - {name: seller_id, type: string}
                  - {name: start_price, type: float64}
            clocks:
              - {name: business, type: calendar, dates: [DATES]}
            """;

    private static final String SPEC = """
            lineage:
              - {fields: [session_id, seller_id, start_price], from: listings}
            time: {field: session_time}
            predictAt: "event_time - PT10M"
            entities:
              - {name: seller, keys: [seller_id]}
            features:
              - name: days
                scope: sequence
                entity: seller
                windows: [{maxAge: 3, clock: business}, {maxAge: 10, clock: business}]
                ops:
                  - {type: aggregate, field: start_price, funcs: [count, mean, max]}
                  - {type: ewma, field: start_price, halflife: [2], decayBy: business}
              - name: path
                scope: sequence
                entity: seller
                windows: [{maxAge: 10, clock: business}]
                lift: {fields: [start_price]}
                summarize:
                  dynamics: {family: lti, measure: exponential, order: 2, halflife: [3], decayBy: business}
            """;

    /** Weekdays from 2023-11-01 to 2025-12-31, minus every 7th one (holidays). */
    private static List<LocalDate> businessDays() {
        final List<LocalDate> dates = new ArrayList<>();
        int n = 0;
        for (LocalDate d = LocalDate.parse("2023-11-01"); d.isBefore(LocalDate.parse("2026-01-01")); d = d.plusDays(1)) {
            if (d.getDayOfWeek() == DayOfWeek.SATURDAY || d.getDayOfWeek() == DayOfWeek.SUNDAY) continue;
            if (++n % 7 == 0) continue;
            dates.add(d);
        }
        return dates;
    }

    @Test
    public void testCalendarWindowsAndDecay() {
        final List<LocalDate> days = businessDays();
        final String sources = SOURCES.replace("DATES", String.join(", ", days.stream().map(LocalDate::toString).toList()));
        final FeaturePlan plan = FeaturePlanCompiler.compile(Config.convertConfigJson(sources, Config.Format.yaml),
                Config.convertConfigJson(SPEC, Config.Format.yaml), null);
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        final List<OutputColumn> columns = plan.getColumns().stream().filter(c -> c.getScope() == FeatureSpec.Scope.sequence).toList();
        final OutputColumn count3 = plan.getColumn("days_3business_start_price_count");
        Assertions.assertNotNull(count3, plan::describe);
        Assertions.assertEquals("3", count3.getCoordinates().get("maxAgeTicks"));
        Assertions.assertEquals("business", count3.getCoordinates().get("windowClock"));
        Assertions.assertSame(plan.getColumn("days_10business_start_price_count").getClocks().get("business"), count3.getClocks().get("business"),
                "one calendar instance shared by the columns");
        Assertions.assertNull(SequenceEvaluator.unboundedReason(count3));

        final Clock clock = Clock.of("business", days);
        final SequenceEvaluator evaluator = new SequenceEvaluator(columns);
        evaluator.setup();
        final Random random = new Random(3);
        long millis = millis("2023-11-06", 9);
        final List<SequenceEvaluator.Past> history = new ArrayList<>();
        final List<SequenceEvaluator.Past> pending = new ArrayList<>();
        long pendingMillis = Long.MIN_VALUE;
        final SequenceEvaluator.KeyState state = new SequenceEvaluator.KeyState();
        int compared = 0;
        for (int i = 0; i < 500; i++) {
            if (random.nextDouble() > 0.1) millis += 3_600_000L * (1 + random.nextInt(60));
            final Map<String, Object> row = new HashMap<>();
            row.put("seller_id", "s1");
            row.put("start_price", random.nextInt(8) == 0 ? null : (double) (50 + random.nextInt(50)));
            if (millis != pendingMillis) {
                history.addAll(pending);
                pending.clear();
                pendingMillis = millis;
            }
            for (final OutputColumn c : columns) {
                final Object incremental = evaluator.evaluateColumn(c, row, millis, history, state);
                final Object scan = evaluator.evaluateColumn(c, row, millis, history, null);
                assertSame(c.getCanonicalName() + "@" + i, scan, incremental);
                compared++;
            }
            // the direct count: the strictly-past rows at least 3 ticks back from the row's tick
            final long now = millis;
            long expected = history.stream().filter(p -> p.values().get("start_price") != null
                    && clock.ordinal(p.millis()) >= clock.ordinal(now) - 3).count();
            Assertions.assertEquals(expected, evaluator.evaluateColumn(count3, row, millis, history, state), "count@" + i);
            // the ewma over the 10-tick window decays per tick: weights 0.5^(ticks / 2) (the ratio is the same whichever
            // event the ages are counted from)
            double num = 0, den = 0;
            for (final SequenceEvaluator.Past p : history) {
                final Object v = p.values().get("start_price");
                if (v == null || clock.ordinal(p.millis()) < clock.ordinal(now) - 10) continue;
                final double w = Math.pow(0.5, (clock.ordinal(now) - clock.ordinal(p.millis())) / 2.0);
                num += w * (Double) v;
                den += w;
            }
            assertSame("ewma@" + i, den == 0 ? null : num / den,
                    evaluator.evaluateColumn(plan.getColumn("days_10business_start_price_ewma2"), row, millis, history, state));
            pending.add(new SequenceEvaluator.Past(millis, new HashMap<>(row)));
        }
        Assertions.assertTrue(compared > 3000);
    }

    private static void assertSame(final String at, final Object expected, final Object actual) {
        if (expected == null || actual == null) {
            Assertions.assertEquals(expected, actual, at);
            return;
        }
        if (expected instanceof Number e && actual instanceof Number a) {
            Assertions.assertEquals(e.doubleValue(), a.doubleValue(), Math.max(1e-9, Math.abs(e.doubleValue()) * 1e-9), at);
            return;
        }
        Assertions.assertEquals(expected, actual, at);
    }
}
