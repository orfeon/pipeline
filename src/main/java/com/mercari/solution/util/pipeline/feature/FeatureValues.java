package com.mercari.solution.util.pipeline.feature;

import com.mercari.solution.util.ExpressionUtil;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Value helpers shared by the stage evaluators. Rows are primitive maps ({@code MElement.asPrimitiveMap()}
 * convention: timestamps are epoch microseconds) keyed by canonical column names.
 */
public final class FeatureValues {

    static final String SELF_PREFIX = "__self_";

    private FeatureValues() {}

    public static Double toDouble(final Object value) {
        if (value == null) return null;
        if (value instanceof Number n) return n.doubleValue();
        if (value instanceof Boolean b) return b ? 1d : 0d;
        if (value instanceof String s) {
            try {
                return Double.parseDouble(s);
            } catch (final NumberFormatException e) {
                return null;
            }
        }
        if (value instanceof Instant i) return (double) i.toEpochMilli();
        if (value instanceof org.joda.time.Instant i) return (double) i.getMillis();
        return null;
    }

    static String toText(final Object value) {
        return value == null ? null : value.toString();
    }

    /** Epoch millis of a timestamp-like primitive (micros Long, Instant, ISO or framework-accepted string). */
    static Long toEpochMillis(final Object value) {
        return toEpochMillis(value, null);
    }

    /**
     * @param type schema type name of the field: {@code date} values are epoch days (Integer/Long) in the
     *             primitive-map convention; everything else numeric is epoch microseconds
     */
    public static Long toEpochMillis(final Object value, final String type) {
        if ("date".equals(type)) {
            if (value instanceof Number n) return n.longValue() * 86_400_000L;
            if (value instanceof String s) {
                try {
                    return java.time.LocalDate.parse(s).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
                } catch (final RuntimeException e) {
                    return null;
                }
            }
            return null;
        }
        return toEpochMillisDefault(value);
    }

    private static Long toEpochMillisDefault(final Object value) {
        if (value == null) return null;
        if (value instanceof Long l) return l / 1000L;
        if (value instanceof Integer i) return i.longValue() / 1000L;
        if (value instanceof Instant i) return i.toEpochMilli();
        if (value instanceof org.joda.time.Instant i) return i.getMillis();
        if (value instanceof String s) {
            try {
                return Instant.parse(s).toEpochMilli();
            } catch (final RuntimeException e) {
                final Instant parsed = com.mercari.solution.util.DateTimeUtil.toInstant(s, true);
                return parsed == null ? null : parsed.toEpochMilli();
            }
        }
        return null;
    }

    static LocalDateTime toDateTime(final Object value) {
        return toDateTime(value, null);
    }

    /**
     * @param inputType schema type name of the field: {@code date} values are epoch days (Integer/Long) in
     *                  the primitive-map convention, everything else is treated as a timestamp
     */
    static LocalDateTime toDateTime(final Object value, final String inputType) {
        if (value == null) return null;
        final Long millis = toEpochMillis(value, inputType);
        return millis == null ? null : LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC);
    }

    /** Evaluates a compiled numeric expression over a row; NaN / infinite results become null. */
    static Double evaluate(final ExpressionUtil.Expression expression, final Map<String, Object> row) {
        final Map<String, Double> values = new HashMap<>();
        for (final String name : expression.getVariableNames()) {
            final Double d = toDouble(row.get(name));
            values.put(name, d == null ? Double.NaN : d);
        }
        final double result = expression.evaluate(values);
        return Double.isNaN(result) || Double.isInfinite(result) ? null : result;
    }

    /** Group key from key field values; null when any component is null (the row skips keyed processing). */
    public static String key(final Map<String, Object> row, final List<String> keys) {
        final StringBuilder sb = new StringBuilder();
        for (final String k : keys) {
            final Object v = row.get(k);
            if (v == null) return null;
            appendKeyComponent(sb, v);
        }
        return sb.toString();
    }

    /** Length-prefixed component encoding: values containing the separator cannot collide across fields. */
    static void appendKeyComponent(final StringBuilder sb, final Object value) {
        final String s = value.toString();
        sb.append(s.length()).append(':').append(s).append('\u0001');
    }

    /**
     * Like {@link #key} but a null component becomes a token instead of nulling the whole key, so the key is
     * a deterministic function of the row (a row id must survive a retry; rows genuinely colliding on it
     * then surface through the fan-out merge's uniqueness rejection). The token cannot collide with a
     * value: {@link #appendKeyComponent} always starts a component with its length.
     */
    /** A generator seeded by a 64-bit hash of (seed, key): a pure function of its arguments (same across re-runs, workers and branches). */
    public static java.util.SplittableRandom seededRandom(final long seed, final String key) {
        final long h = com.google.common.hash.Hashing.murmur3_128((int) (seed ^ (seed >>> 32)))
                .hashString(seed + "\u0000" + key, java.nio.charset.StandardCharsets.UTF_8).asLong();
        return new java.util.SplittableRandom(h);
    }

    static String keyWithNullTokens(final Map<String, Object> row, final List<String> keys) {
        final StringBuilder sb = new StringBuilder();
        for (final String k : keys) {
            final Object v = row.get(k);
            if (v == null) {
                sb.append('\u0000').append('\u0001');
            } else {
                appendKeyComponent(sb, v);
            }
        }
        return sb.toString();
    }

    static Object cast(final Double value, final com.mercari.solution.module.Schema.FieldType type) {
        if (value == null) return null;
        return switch (type.getType()) {
            case int8, int16, int32 -> value.intValue();
            case int64 -> value.longValue();
            case float32 -> value.floatValue();
            case bool -> value != 0d;
            default -> value;
        };
    }

}
