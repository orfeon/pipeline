package com.mercari.solution.util.pipeline.feature;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * A calendar clock declared in the sources document ({@code clocks:}): the tick dates (business days, trading
 * sessions — UTC dates) on which windows, decay and fit blocks are counted instead of wall time
 * (docs/design/feature-dsl.md §2.1, feature-engine.md §9.6.7). The position of an instant is the ordinal of the
 * last tick on or before its date; a window of {@code n} ticks keeps the rows whose position is at least the row's
 * minus {@code n}. Availability is never measured on a clock: it stays on wall time.
 *
 * <p>The two built-in clocks are not instances: {@code time} (wall time, the default) and {@code events} (the
 * event ordinal: {@code maxEvents}, {@code decayBy: events}).
 */
public final class Clock implements Serializable {

    public static final List<String> BUILT_IN = List.of("time", "events");
    static final long DAY_MILLIS = 86_400_000L;

    private final String name;
    /** Tick dates as epoch days, ascending, unique. */
    private final long[] days;

    Clock(final String name, final long[] days) {
        this.name = name;
        this.days = days;
    }

    public static Clock of(final String name, final List<LocalDate> dates) {
        final TreeSet<Long> sorted = new TreeSet<>();
        for (final LocalDate d : dates) sorted.add(d.toEpochDay());
        return new Clock(name, sorted.stream().mapToLong(Long::longValue).toArray());
    }

    public String name() { return name; }
    public int size() { return days.length; }
    public LocalDate first() { return LocalDate.ofEpochDay(days[0]); }
    public LocalDate last() { return LocalDate.ofEpochDay(days[days.length - 1]); }

    /** The ordinal of the last tick on or before the instant's UTC date; −1 before the first tick. */
    public long ordinal(final long millis) {
        final long day = Math.floorDiv(millis, DAY_MILLIS);
        int lo = 0, hi = days.length;
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if (days[mid] <= day) lo = mid + 1;
            else hi = mid;
        }
        return lo - 1;
    }

    /** Ticks between two instants: {@code ordinal(later) − ordinal(earlier)}. */
    public double distance(final long later, final long earlier) {
        return ordinal(later) - ordinal(earlier);
    }

    /**
     * The earliest instant a window of {@code ticks} ticks ending at {@code nowMillis} keeps: a row is inside when its
     * position is at least {@code ordinal(now) − ticks}, i.e. when it is on or after the start (UTC) of that tick's
     * date. {@link Long#MIN_VALUE} when the window reaches before the first tick.
     */
    public long farEdgeMillis(final long nowMillis, final long ticks) {
        final long k = ordinal(nowMillis) - ticks;
        return k <= 0 ? (k == 0 ? days[0] * DAY_MILLIS : Long.MIN_VALUE) : days[(int) k] * DAY_MILLIS;
    }

    /** The mean spacing of the ticks: the nominal length of a tick when a wall-time span is rounded to ticks. */
    public long meanSpacingMillis() {
        if (days.length < 2) return DAY_MILLIS;
        return (days[days.length - 1] - days[0]) * DAY_MILLIS / (days.length - 1);
    }

    public String describe() {
        return name + " (" + days.length + " ticks, " + first() + " .. " + last() + ")";
    }

    @Override
    public boolean equals(final Object o) {
        return o instanceof Clock c && c.name.equals(name) && Arrays.equals(c.days, days);
    }

    @Override
    public int hashCode() {
        return name.hashCode() * 31 + Arrays.hashCode(days);
    }

    /**
     * The {@code clocks:} of a sources document ({@code {sources: [...], clocks: [{name, type: calendar, dates:
     * [...]}]}}; a {@code uri} was resolved into {@code dates} by {@code FeaturePlanService} before compile).
     */
    public static Map<String, Clock> parseAll(final JsonElement document, final Diagnostics diagnostics) {
        final Map<String, Clock> clocks = new LinkedHashMap<>();
        if (document == null || !document.isJsonObject() || !document.getAsJsonObject().has("clocks")) return clocks;
        final JsonElement list = document.getAsJsonObject().get("clocks");
        if (!list.isJsonArray()) {
            diagnostics.error("sources.clocks", "clocks", "clocks must be a list of {name, type: calendar, dates: [...] | uri}");
            return clocks;
        }
        for (final JsonElement e : list.getAsJsonArray()) {
            if (!e.isJsonObject()) {
                diagnostics.error("sources.clocks", "clocks", "each clock must be an object {name, type: calendar, dates | uri}");
                continue;
            }
            final JsonObject o = e.getAsJsonObject();
            final String name = SourceContract.Json.string(o, "name");
            final String loc = "clocks." + name;
            if (name == null || BUILT_IN.contains(name)) {
                diagnostics.error("sources.clocks.name", loc, "a clock needs a name other than the built-in " + BUILT_IN + ": " + name);
                continue;
            }
            final String type = SourceContract.Json.string(o, "type");
            if (type != null && !"calendar".equals(type)) {
                diagnostics.error("sources.clocks.type", loc, "clock type must be calendar: " + type);
                continue;
            }
            if (o.has("uri") && !o.has("dates")) {
                diagnostics.error("sources.clocks.uri", loc, "clock uri " + o.get("uri") + " was not resolved (read it through FeaturePlanService.resolve)");
                continue;
            }
            final List<String> texts = SourceContract.Json.strings(o, "dates");
            final List<LocalDate> dates = new java.util.ArrayList<>();
            for (final String text : texts) {
                try {
                    dates.add(LocalDate.parse(text.trim()));
                } catch (final RuntimeException ex) {
                    diagnostics.error("sources.clocks.dates", loc, "clock date is not an ISO date (yyyy-MM-dd): " + text);
                }
            }
            if (dates.isEmpty()) {
                diagnostics.error("sources.clocks.dates", loc, "a calendar clock needs at least one tick date");
                continue;
            }
            if (clocks.containsKey(name)) diagnostics.error("sources.clocks.name", loc, "duplicate clock name");
            clocks.put(name, of(name, dates));
        }
        return clocks;
    }

    /**
     * Tick dates from a text: a JSON array of dates, a JSON object with a {@code dates} array, or one date per line —
     * the first CSV column, {@code #} comments and a non-date header skipped.
     */
    public static List<String> parseDates(final String text) {
        final String trimmed = text.trim();
        if (trimmed.startsWith("[") || trimmed.startsWith("{")) {
            final JsonElement parsed = com.google.gson.JsonParser.parseString(trimmed);
            final JsonElement array = parsed.isJsonObject() ? parsed.getAsJsonObject().get("dates") : parsed;
            final List<String> dates = new java.util.ArrayList<>();
            if (array != null && array.isJsonArray()) for (final JsonElement d : array.getAsJsonArray()) dates.add(d.getAsString());
            return dates;
        }
        final List<String> dates = new java.util.ArrayList<>();
        for (final String line : trimmed.split("\\R")) {
            final String cell = line.split("[,\\t]", 2)[0].trim();
            if (cell.isEmpty() || cell.startsWith("#")) continue;
            if (dates.isEmpty() && !Character.isDigit(cell.charAt(0))) continue; // a header
            dates.add(cell);
        }
        return dates;
    }
}
