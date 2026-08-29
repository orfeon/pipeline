package com.mercari.solution.util.pipeline.feature;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Availability time algebra (work-feature.md §2.3, §6.1).
 *
 * <p>A value is either <b>static</b> — expressible as {@code event_time + offset} for every row, so it
 * can be compared with {@code predictAt} / {@code computeAt} at compile time — or <b>dynamic</b>, when it
 * depends on a per-row timestamp field ({@code atRowCreation}) or an absolute time of day
 * ({@code event_date T08:00}). Dynamic values keep the max of the static parts they were joined with as a
 * lower bound, so violations that are provable from the static part are still reported statically.
 *
 * <p>The only operation needed by the propagation rules is {@link #max(AvailableAt, AvailableAt)} (join).
 */
public final class AvailableAt implements Serializable {

    private static final Pattern EVENT_RELATIVE = Pattern.compile(
            "^event_time\\s*(?:([+-])\\s*(P\\S+))?$", Pattern.CASE_INSENSITIVE);
    private static final Pattern DATE_ABSOLUTE = Pattern.compile(
            "^event_date\\s*T?\\s*(\\d{1,2}:\\d{2}(?::\\d{2})?)$", Pattern.CASE_INSENSITIVE);

    /**
     * {@code atEventTime}: information fixed once the event exists (entries, attributes). It is available
     * for any predictAt at or before event_time, so it is modelled as a far-past offset rather than
     * {@code event_time + 0} — the latter is {@code after(event)} with settlementLag PT0S.
     */
    private static final Duration PRE_EVENT = Duration.ofSeconds(Long.MIN_VALUE / 16);
    private static final Duration DYNAMIC_UNKNOWN = Duration.ofSeconds(Long.MIN_VALUE / 4);

    /** Offset from event_time for the static part (lower bound when dynamic). */
    private final Duration offset;
    /** Non-empty when the value cannot be decided statically; describes why. */
    private final List<String> dynamicReasons;

    private AvailableAt(final Duration offset, final List<String> dynamicReasons) {
        this.offset = offset;
        this.dynamicReasons = dynamicReasons;
    }

    public static AvailableAt atEventTime() {
        return new AvailableAt(PRE_EVENT, List.of());
    }

    public boolean isPreEvent() {
        return offset.equals(PRE_EVENT);
    }

    public static AvailableAt eventRelative(final Duration offset) {
        return new AvailableAt(offset, List.of());
    }

    public static AvailableAt dynamic(final String reason) {
        return new AvailableAt(DYNAMIC_UNKNOWN, List.of(reason));
    }

    /**
     * Parses a field/table level {@code availableAt} expression.
     *
     * @param settlementLag concretizes {@code after(event)}; null means PT0S
     */
    public static AvailableAt parse(final String expression, final Duration settlementLag) {
        final String expr = expression == null ? "atEventTime" : expression.trim();
        return switch (expr.toLowerCase()) {
            case "ateventtime" -> atEventTime();
            case "after(event)" -> eventRelative(settlementLag == null ? Duration.ZERO : settlementLag);
            case "atrowcreation" -> dynamic("atRowCreation");
            default -> parseTimeExpression(expr);
        };
    }

    /** Parses {@code predictAt} / {@code computeAt} / {@code snapshotOf.at} expressions. */
    public static AvailableAt parseTimeExpression(final String expression) {
        final String expr = expression.trim();
        final Matcher rel = EVENT_RELATIVE.matcher(expr);
        if (rel.matches()) {
            if (rel.group(1) == null) {
                // the literal event time (predictAt: "event_time"); the pre-event keyword is atEventTime
                return eventRelative(Duration.ZERO);
            }
            final Duration d = Durations.parse(rel.group(2));
            return eventRelative("-".equals(rel.group(1)) ? d.negated() : d);
        }
        final Matcher abs = DATE_ABSOLUTE.matcher(expr);
        if (abs.matches()) {
            return dynamic("event_date T" + abs.group(1));
        }
        throw new IllegalArgumentException("Unsupported time expression: " + expression
                + " (expected atEventTime | event_time ± <ISO8601 duration> | after(event) | atRowCreation | event_date THH:MM)");
    }

    public boolean isStatic() {
        return dynamicReasons.isEmpty();
    }

    public Duration getOffset() {
        return offset;
    }

    public List<String> getDynamicReasons() {
        return dynamicReasons;
    }

    /** Adds a delay (ingestionLag). A dynamic value stays dynamic; its lower bound moves accordingly. */
    public AvailableAt plus(final Duration lag) {
        if (lag == null || lag.isZero() || isPreEvent() || offset.equals(DYNAMIC_UNKNOWN)) {
            return this;
        }
        return new AvailableAt(offset.plus(lag), dynamicReasons);
    }

    /** Join: the later of two availability times. */
    public static AvailableAt max(final AvailableAt a, final AvailableAt b) {
        if (a == null) return b;
        if (b == null) return a;
        final Duration off = a.offset.compareTo(b.offset) >= 0 ? a.offset : b.offset;
        if (a.isStatic() && b.isStatic()) {
            return new AvailableAt(off, List.of());
        }
        final List<String> reasons = new ArrayList<>(a.dynamicReasons);
        for (final String r : b.dynamicReasons) {
            if (!reasons.contains(r)) reasons.add(r);
        }
        return new AvailableAt(off, reasons);
    }

    /** True when this value is known to be at or before {@code other} for every row. */
    public boolean isStaticallyAtOrBefore(final AvailableAt other) {
        return isStatic() && other.isStatic() && offset.compareTo(other.offset) <= 0;
    }

    /** True when the static lower bound alone proves this value is after {@code other}. */
    public boolean isProvablyAfter(final AvailableAt other) {
        if (!other.isStatic()) return false;
        if (isStatic()) return offset.compareTo(other.offset) > 0;
        return !offset.equals(DYNAMIC_UNKNOWN) && offset.compareTo(other.offset) > 0;
    }

    public String describe() {
        final String base;
        if (offset.equals(DYNAMIC_UNKNOWN)) {
            base = "?";
        } else if (isPreEvent()) {
            base = "pre-event";
        } else if (offset.isZero()) {
            base = "event_time";
        } else if (offset.isNegative()) {
            base = "event_time - " + offset.negated();
        } else {
            base = "event_time + " + offset;
        }
        return isStatic() ? base : base + " (dynamic: " + String.join(", ", dynamicReasons) + ")";
    }

    @Override
    public String toString() {
        return describe();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof AvailableAt that)) return false;
        return offset.equals(that.offset) && dynamicReasons.equals(that.dynamicReasons);
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset, dynamicReasons);
    }

}
