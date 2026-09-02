package com.mercari.solution.util.pipeline.outbound;

import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Duration text parsing shared by outbound modules: ISO-8601 ({@code PT10M}) or short form ({@code 10m}, {@code 500ms}). */
public final class Durations {

    private static final Pattern PATTERN_SHORT = Pattern.compile("^(\\d+)\\s*(ms|s|m|h|d)$");

    private Durations() {}

    public static Duration parse(final String text) {
        if(text == null || text.isBlank()) {
            throw new IllegalArgumentException("duration must not be empty");
        }
        final String t = text.trim();
        final Matcher matcher = PATTERN_SHORT.matcher(t);
        if(matcher.matches()) {
            final long n = Long.parseLong(matcher.group(1));
            return switch (matcher.group(2)) {
                case "ms" -> Duration.ofMillis(n);
                case "s" -> Duration.ofSeconds(n);
                case "m" -> Duration.ofMinutes(n);
                case "h" -> Duration.ofHours(n);
                default -> Duration.ofDays(n);
            };
        }
        try {
            return Duration.parse(t);
        } catch (final Exception e) {
            throw new IllegalArgumentException("illegal duration: " + text + " (expected ISO-8601 like PT10M or short form like 10m)");
        }
    }

    /**
     * Exponential backoff before the retry that follows the given (1-based) failed attempt:
     * {@code initial * 2^(attempt-1)}, capped at {@code max} (shift capped at 20 to avoid overflow).
     */
    public static Duration exponentialBackoff(final Duration initial, final Duration max, final int attempt) {
        final long base = initial.toMillis() * (1L << Math.min(Math.max(attempt, 1) - 1, 20));
        return Duration.ofMillis(Math.min(base, max.toMillis()));
    }

    /** Parses byte sizes: plain number, or with KB / MB / GB suffix (binary). */
    public static long parseBytes(final String text) {
        if(text == null || text.isBlank()) {
            throw new IllegalArgumentException("size must not be empty");
        }
        final String t = text.trim().toUpperCase();
        final Matcher m = Pattern.compile("^(\\d+)\\s*(B|KB|MB|GB)?$").matcher(t);
        if(!m.matches()) {
            throw new IllegalArgumentException("illegal size: " + text + " (expected e.g. 1024, 100KB, 5MB)");
        }
        final long n = Long.parseLong(m.group(1));
        final String unit = m.group(2) == null ? "B" : m.group(2);
        return switch (unit) {
            case "KB" -> n * 1024L;
            case "MB" -> n * 1024L * 1024L;
            case "GB" -> n * 1024L * 1024L * 1024L;
            default -> n;
        };
    }
}
