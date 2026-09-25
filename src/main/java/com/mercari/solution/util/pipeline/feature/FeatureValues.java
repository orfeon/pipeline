package com.mercari.solution.util.pipeline.feature;

import com.mercari.solution.util.ExpressionUtil;
import org.apache.beam.sdk.values.KV;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
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

    /**
     * A {@code weightBy} operand as the double the expression engine evaluates: a number as itself, a boolean as 0 / 1,
     * a numeric string as its number — and any other string as a stable 52-bit hash of its text (FNV-1a), so an
     * equality {@code field == $self.field} holds exactly for equal texts. Only {@code ==} / {@code !=} are meaningful
     * on such a value; the compiler says so (info {@code sequence.weightBy.identity}). Null for null / an unsupported value.
     */
    public static Double weightOperand(final Object value) {
        final Double d = toDouble(value);
        if (d != null || !(value instanceof String s)) return d;
        long hash = 0xcbf29ce484222325L;
        for (int i = 0; i < s.length(); i++) {
            hash ^= s.charAt(i);
            hash *= 0x100000001b3L;
        }
        return (double) (hash >>> 12); // 52 bits: exactly representable, non-negative
    }

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

    /**
     * Whether a row value equals a declared one (a {@code value:} / {@code values:} scalar, kept as text): a number
     * compares as a number — a row expression is always float64, so {@code value: 1} must match 1.0 —, anything else
     * by its exact text (a string category {@code "2.0"} is not {@code "2"}). A float32 value compares in its own
     * precision (0.1f is the declared 0.1, as its text was), and NaN matches a declared NaN. False for null.
     */
    static boolean matchesDeclared(final Object value, final String declared) {
        if (value == null || declared == null) return false;
        if (value instanceof Number n) {
            if ((value instanceof Long || value instanceof Integer) && isIntegerText(declared)) {
                try {
                    return n.longValue() == Long.parseLong(declared);
                } catch (final NumberFormatException beyondLong) {
                    // more digits than a long holds: compared as a double below
                }
            }
            try {
                if (value instanceof Float f) {
                    final float g = Float.parseFloat(declared);
                    return f == g || (Float.isNaN(f) && Float.isNaN(g));
                }
                final double x = n.doubleValue();
                final double d = Double.parseDouble(declared);
                return x == d || (Double.isNaN(x) && Double.isNaN(d));
            } catch (final NumberFormatException e) {
                return false;
            }
        }
        return value.toString().equals(declared);
    }

    /** An optional sign then ASCII digits: the only declarations tried as a long (a decimal would throw per row). */
    private static boolean isIntegerText(final String s) {
        final int start = !s.isEmpty() && (s.charAt(0) == '-' || s.charAt(0) == '+') ? 1 : 0;
        if (start >= s.length()) return false;
        for (int i = start; i < s.length(); i++) {
            final char ch = s.charAt(i);
            if (ch < '0' || ch > '9') return false;
        }
        return true;
    }

    /**
     * The numeric target of a row under an optional baseline offset, as the pair {@code (y − b, b)} — {@code b} null
     * without an offset — or null when the target or the baseline is missing / NaN (the row contributes to no
     * statistic, and to no count). A null {@code field} is a target-less statistic (count / share denominator):
     * every row contributes {@code y = 0} and the baseline is not consulted. The single rule behind the expanding
     * replay ({@link PopulationEvaluator}) and the static / fold / forward and joint fits ({@link VarianceComponents},
     * {@link FeatureStages}), so the engines count the same rows.
     */
    static KV<Double, Double> offsetTarget(final Map<String, Object> row, final String field, final String offsetColumn) {
        if (field == null) return KV.of(0d, null);
        final Double y = toDouble(row.get(field));
        if (y == null || y.isNaN()) return null;
        if (offsetColumn == null) return KV.of(y, null);
        final Double b = toDouble(row.get(offsetColumn));
        if (b == null || b.isNaN()) return null;
        return KV.of(y - b, b);
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

    /** The key of already-textual components (the inverse of {@link #keyComponents}). */
    static String keyOf(final List<String> components) {
        final StringBuilder sb = new StringBuilder();
        for (final String c : components) appendKeyComponent(sb, c);
        return sb.toString();
    }

    /**
     * Like {@link #key} but a null component is encoded (length {@code -1}) instead of nulling the whole key, so
     * a row takes part in every projection of the key whose fields are present; {@link #keyComponents} decodes
     * it back to {@code null}. The joint fit's cell key: a row with a null coarse-level key still belongs to the
     * cells of the levels it has.
     */
    static String keyWithNulls(final Map<String, Object> row, final List<String> keys) {
        final StringBuilder sb = new StringBuilder();
        for (final String k : keys) {
            final Object v = row.get(k);
            if (v == null) sb.append("-1:").append('\u0001');
            else appendKeyComponent(sb, v);
        }
        return sb.toString();
    }

    /** Decodes a {@link #key} (or {@link #keyWithNulls}) back into its components (the length prefix makes the split exact). */
    static List<String> keyComponents(final String key) {
        final List<String> components = new ArrayList<>();
        int i = 0;
        while (i < key.length()) {
            final int colon = key.indexOf(':', i);
            final int length = Integer.parseInt(key.substring(i, colon));
            if (length < 0) {
                components.add(null);
                i = colon + 2; // the terminator only
                continue;
            }
            components.add(key.substring(colon + 1, colon + 1 + length));
            i = colon + 1 + length + 1; // the component and its terminator
        }
        return components;
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
