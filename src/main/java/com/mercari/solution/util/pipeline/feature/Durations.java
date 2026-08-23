package com.mercari.solution.util.pipeline.feature;

import java.time.Duration;
import java.time.Period;
import java.time.format.DateTimeParseException;

/**
 * ISO 8601 duration parsing that also accepts date-based periods (P2Y, P6M, P90D).
 * Years and months are converted with fixed lengths (365 / 30 days); the compile layer only
 * needs a total order over offsets, so the approximation is acceptable for window sizing.
 */
public final class Durations {

    private Durations() {}

    public static Duration parse(final String text) {
        if (text == null) {
            throw new IllegalArgumentException("duration must not be null");
        }
        final String s = text.trim();
        try {
            return Duration.parse(s);
        } catch (final DateTimeParseException ignored) {
            // fall through to period handling
        }
        final int t = s.indexOf('T');
        final String periodPart = t < 0 ? s : s.substring(0, t);
        final String timePart = t < 0 ? null : "PT" + s.substring(t + 1);
        final Period period = Period.parse(periodPart);
        Duration duration = Duration.ofDays(period.getYears() * 365L + period.getMonths() * 30L + period.getDays());
        if (timePart != null) {
            duration = duration.plus(Duration.parse(timePart));
        }
        return duration;
    }

    /** Short token used in generated column names: P365D → 365d, PT10M → 10m, P2Y → 730d. */
    public static String shortName(final Duration duration) {
        if (duration.toDays() > 0 && duration.minusDays(duration.toDays()).isZero()) {
            return duration.toDays() + "d";
        }
        if (duration.toHours() > 0 && duration.minusHours(duration.toHours()).isZero()) {
            return duration.toHours() + "h";
        }
        if (duration.toMinutes() > 0 && duration.minusMinutes(duration.toMinutes()).isZero()) {
            return duration.toMinutes() + "m";
        }
        return duration.toSeconds() + "s";
    }

}
