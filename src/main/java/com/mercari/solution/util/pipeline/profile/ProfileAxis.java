package com.mercari.solution.util.pipeline.profile;

import java.io.Serializable;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAdjusters;
import java.util.List;
import java.util.Set;

/**
 * One comparison axis of the profile (declared {@code segments} field or the {@code time} field).
 * A row is assigned to at most one group per axis; per-group sub-profiles are computed with the
 * same CombineFn as the global profile, keyed by {@link #groupKey}.
 */
public class ProfileAxis implements Serializable {

    public static final String NULL_GROUP = "(null)";
    public static final Set<String> GRANULARITIES = Set.of("hour", "day", "week", "month", "year");

    /** Separator between axis id and group value in the shuffle key (cannot appear in field names). */
    private static final char KEY_SEPARATOR = 0x1F; // ASCII unit separator

    public enum Kind implements Serializable {
        segments,
        time,
        inputs
    }

    public Kind kind;
    public String field;
    public int fieldIndex = -1;      // index into ProfileSpec.getFields()
    public String sourceType;
    public List<String> symbols;
    public int topK = 20;            // segments: max groups kept in the report
    public String granularity;       // time: hour | day | week | month | year
    public List<String> inputNames;  // inputs: union input order (element index → input name)
    public String baseline;          // inputs: reference group for drift metrics

    public String id() {
        return kind.name() + ":" + field;
    }

    public String groupKey(final String group) {
        return id() + KEY_SEPARATOR + group;
    }

    /** Extracts this axis's group value back out of a shuffle key, or null if the key is not this axis's. */
    public String groupOfKey(final String key) {
        final String prefix = id() + KEY_SEPARATOR;
        return key.startsWith(prefix) ? key.substring(prefix.length()) : null;
    }

    /**
     * Group label for one raw primitive value, or null when the row has no group on this axis
     * (unparsable time value). Null segment values map to {@link #NULL_GROUP}.
     */
    /** Group for the inputs axis: the union input the element came from. */
    public String groupOfInputIndex(final int index) {
        if(inputNames == null || index < 0 || index >= inputNames.size()) {
            return null;
        }
        return inputNames.get(index);
    }

    public String groupValue(final Object raw) {
        return switch (kind) {
            case inputs -> null;   // resolved from the element index, not a field value
            case segments -> {
                if(raw == null) {
                    yield NULL_GROUP;
                }
                final Double numeric = raw instanceof Number ? ProfileSpec.toDouble(raw) : null;
                if(numeric != null && symbols == null) {
                    yield canonicalNumeric(numeric);
                }
                final String s = ProfileSpec.toStringValue(raw, symbols);
                yield s == null ? NULL_GROUP : (s.length() > 128 ? s.substring(0, 128) : s);
            }
            case time -> {
                final Double ms = ProfileSpec.toEpochMillis(raw, sourceType);
                yield ms == null ? null : truncateLabel(ms.longValue(), granularity);
            }
        };
    }

    private static String canonicalNumeric(final double v) {
        if(v == Math.rint(v) && Double.isFinite(v) && Math.abs(v) < 1e15) {
            return String.valueOf((long) v);
        }
        return String.valueOf(v);
    }

    /** UTC bucket label, e.g. month → {@code 2025-01}, week → the Monday date, hour → {@code 2025-01-15T03}. */
    public static String truncateLabel(final long epochMillis, final String granularity) {
        final LocalDateTime dt = LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneOffset.UTC);
        final LocalDate date = dt.toLocalDate();
        return switch (granularity) {
            case "hour" -> String.format("%sT%02d", date, dt.getHour());
            case "day" -> date.toString();
            case "week" -> date.with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY)).toString();
            case "year" -> String.valueOf(date.getYear());
            default -> String.format("%d-%02d", date.getYear(), date.getMonthValue());
        };
    }
}
