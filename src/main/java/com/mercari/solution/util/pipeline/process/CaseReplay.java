package com.mercari.solution.util.pipeline.process;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Replays the time-ordered events of one case and produces everything the process outputs are built from:
 * the directly-follows edges with their waiting times, the activity counts, the variant, the resource
 * handovers and the Declare constraint verdicts. Pure Java (no Beam), streaming over the events: only the
 * capped activity list, the per-activity and per-edge counters and the per-constraint state stay in memory, so
 * a case with millions of events is replayed in memory bounded by its distinct activities and edges.
 */
public final class CaseReplay {

    private CaseReplay() {}

    /** One event of a case after the log projection. {@code sequence} breaks ties between equal timestamps. */
    public record Event(String activity, long millis, String resource, Comparable<?> sequence) implements Serializable {}

    /** A directly-follows pair; the synthetic start / end edges use {@link ProcessSpec#START_NODE} / {@link ProcessSpec#END_NODE}. */
    public record Edge(String source, String target) implements Serializable {}

    /** The verdict of one constraint on one case. */
    public record Verdict(String name, boolean applicable, boolean violated) implements Serializable {}

    /** Per-activity counters of one case. */
    public static final class ActivityCount implements Serializable {
        public long frequency;
        public boolean first;
        public boolean last;
    }

    public static final class Result implements Serializable {
        public final List<String> activities = new ArrayList<>();
        public int length;
        public boolean truncated;
        public String variant;
        public long startMillis;
        public long endMillis;
        public String firstActivity;
        public String lastActivity;
        public final Map<String, ActivityCount> activityCounts = new LinkedHashMap<>();
        /** Per-edge frequency and waiting-time distribution (in {@link ProcessSpec#unit}); no durations on the start / end edges. */
        public final Map<Edge, ProcessStats> edges = new LinkedHashMap<>();
        public final List<String> resources = new ArrayList<>();
        public final Map<String, Long> handovers = new LinkedHashMap<>();
        public final List<Verdict> verdicts = new ArrayList<>();

        public long durationMillis() {
            return endMillis - startMillis;
        }

        public List<String> violations() {
            final List<String> names = new ArrayList<>();
            for (final Verdict v : verdicts) if (v.violated()) names.add(v.name());
            return names;
        }

        /** 1 − violated / applicable; 1.0 when no constraint applies. */
        public double fitness() {
            int applicable = 0;
            int violated = 0;
            for (final Verdict v : verdicts) {
                if (!v.applicable()) continue;
                applicable++;
                if (v.violated()) violated++;
            }
            return applicable == 0 ? 1D : 1D - (double) violated / applicable;
        }
    }

    /** The separator of the variant string. */
    public static final String VARIANT_SEPARATOR = " -> ";
    static final String HANDOVER_SEPARATOR = "\u0000";

    /**
     * @param events the events of the case in ascending time order (ties in any order; broken by {@code sequence})
     */
    public static Result replay(final Iterable<Event> events, final ProcessSpec spec) {
        final Result result = new Result();
        final List<ConstraintState> constraints = new ArrayList<>();
        for (final ProcessSpec.Constraint c : spec.constraints) constraints.add(new ConstraintState(c));
        final Set<String> seenResources = new LinkedHashSet<>();
        final Map<String, Long> firstSeen = new HashMap<>();
        final Map<String, Long> lastSeen = new HashMap<>();
        Event previous = null;
        if (spec.sequence == null) {
            // no tie-break field: the events stream straight through in the given order
            for (final Event event : events) {
                previous = step(event, previous, result, constraints, seenResources, firstSeen, lastSeen, spec);
            }
        } else {
            // buffer the events of one timestamp so that the sequence field can order them
            final List<Event> tie = new ArrayList<>();
            long tieMillis = Long.MIN_VALUE;
            for (final Event event : events) {
                if (!tie.isEmpty() && event.millis() != tieMillis) {
                    previous = flush(tie, previous, result, constraints, seenResources, firstSeen, lastSeen, spec);
                    tie.clear();
                }
                tieMillis = event.millis();
                tie.add(event);
            }
            if (!tie.isEmpty()) previous = flush(tie, previous, result, constraints, seenResources, firstSeen, lastSeen, spec);
        }
        if (previous == null) return result;
        result.endMillis = previous.millis();
        result.lastActivity = previous.activity();
        result.activityCounts.get(previous.activity()).last = true;
        if (spec.dfgStartEnd) {
            edge(result, previous.activity(), ProcessSpec.END_NODE).frequency++;
        }
        result.resources.addAll(seenResources);
        // the variant string: every activity when the trace fits, the visible prefix plus the hidden count otherwise
        final StringBuilder sb = new StringBuilder();
        for (final String a : result.activities) {
            if (sb.length() > 0) sb.append(VARIANT_SEPARATOR);
            sb.append(a);
        }
        if (result.truncated) sb.append(VARIANT_SEPARATOR).append("...(+").append(result.length - result.activities.size()).append(')');
        result.variant = sb.toString();
        for (final ConstraintState c : constraints) result.verdicts.add(c.verdict(result, firstSeen, lastSeen));
        return result;
    }

    /** Sorts the events of one timestamp by their sequence value and feeds them to the replay. */
    private static Event flush(final List<Event> tie, Event previous, final Result result, final List<ConstraintState> constraints,
                               final Set<String> seenResources, final Map<String, Long> firstSeen, final Map<String, Long> lastSeen,
                               final ProcessSpec spec) {
        if (tie.size() > 1) {
            tie.sort((a, b) -> compareSequence(a.sequence(), b.sequence()));
        }
        for (final Event event : tie) {
            previous = step(event, previous, result, constraints, seenResources, firstSeen, lastSeen, spec);
        }
        return previous;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static int compareSequence(final Comparable<?> a, final Comparable<?> b) {
        if (a == null) return b == null ? 0 : 1;
        if (b == null) return -1;
        if (a instanceof Long la && b instanceof Long lb) return Long.compare(la, lb);
        if (a instanceof Number na && b instanceof Number nb) return Double.compare(na.doubleValue(), nb.doubleValue());
        if (a.getClass() == b.getClass()) return ((Comparable) a).compareTo(b);
        return a.toString().compareTo(b.toString());
    }

    private static Event step(final Event event, final Event previous, final Result result, final List<ConstraintState> constraints,
                              final Set<String> seenResources, final Map<String, Long> firstSeen, final Map<String, Long> lastSeen,
                              final ProcessSpec spec) {
        final String activity = event.activity();
        final long index = result.length;
        result.length++;
        if (result.activities.size() < spec.maxTraceLength) {
            result.activities.add(activity);
        } else {
            result.truncated = true;
        }
        final ActivityCount count = result.activityCounts.computeIfAbsent(activity, k -> new ActivityCount());
        count.frequency++;
        firstSeen.putIfAbsent(activity, event.millis());
        lastSeen.put(activity, event.millis());
        if (previous == null) {
            result.startMillis = event.millis();
            result.firstActivity = activity;
            count.first = true;
            if (spec.dfgStartEnd) edge(result, ProcessSpec.START_NODE, activity).frequency++;
        } else {
            final ProcessStats edge = edge(result, previous.activity(), activity);
            edge.frequency++;
            edge.addDuration(spec.unit.fromMillis(event.millis() - previous.millis()));
            if (previous.resource() != null && event.resource() != null && !previous.resource().equals(event.resource())) {
                result.handovers.merge(previous.resource() + HANDOVER_SEPARATOR + event.resource(), 1L, Long::sum);
            }
        }
        if (event.resource() != null) seenResources.add(event.resource());
        for (final ConstraintState c : constraints) c.observe(activity, previous == null ? null : previous.activity(), index);
        return event;
    }

    private static ProcessStats edge(final Result result, final String source, final String target) {
        return result.edges.computeIfAbsent(new Edge(source, target), k -> {
            final ProcessStats stats = new ProcessStats();
            stats.cases = 1;
            return stats;
        });
    }

    /** Incremental state of one Declare constraint over the trace. */
    private static final class ConstraintState {
        private final ProcessSpec.Constraint constraint;
        private long activityCount;
        private long targetCount;
        private long lastActivityIndex = -1;
        private long lastTargetIndex = -1;
        private long firstActivityIndex = -1;
        private long firstTargetIndex = -1;
        /** chainResponse: an activity not immediately followed by the target; chainPrecedence: a target not immediately preceded */
        private boolean chainViolated;

        ConstraintState(final ProcessSpec.Constraint constraint) {
            this.constraint = constraint;
        }

        void observe(final String activity, final String previous, final long index) {
            final boolean isActivity = activity.equals(constraint.activity);
            final boolean isTarget = activity.equals(constraint.target);
            if (isActivity) {
                activityCount++;
                lastActivityIndex = index;
                if (firstActivityIndex < 0) firstActivityIndex = index;
            }
            if (isTarget) {
                targetCount++;
                lastTargetIndex = index;
                if (firstTargetIndex < 0) firstTargetIndex = index;
            }
            switch (constraint.type) {
                case chainResponse -> {
                    // the previous event was the activity and this one is not the target
                    if (previous != null && previous.equals(constraint.activity) && !isTarget) chainViolated = true;
                }
                case chainPrecedence -> {
                    if (isTarget && (previous == null || !previous.equals(constraint.activity))) chainViolated = true;
                }
                default -> {}
            }
        }

        Verdict verdict(final Result result, final Map<String, Long> firstSeen, final Map<String, Long> lastSeen) {
            final String name = constraint.name;
            return switch (constraint.type) {
                case existence -> new Verdict(name, true, activityCount < (constraint.min == null ? 1 : constraint.min));
                case absence -> new Verdict(name, true, activityCount > (constraint.max == null ? 0 : constraint.max));
                case exactly -> new Verdict(name, true, activityCount != constraint.count);
                case init -> new Verdict(name, true, !Objects.equals(result.firstActivity, constraint.activity));
                case end -> new Verdict(name, true, !Objects.equals(result.lastActivity, constraint.activity));
                case response -> new Verdict(name, activityCount > 0, activityCount > 0 && lastActivityIndex > lastTargetIndex);
                case precedence -> new Verdict(name, targetCount > 0, targetCount > 0 && (firstActivityIndex < 0 || firstTargetIndex < firstActivityIndex));
                case succession -> {
                    final boolean applicable = activityCount > 0 || targetCount > 0;
                    final boolean response = activityCount > 0 && lastActivityIndex > lastTargetIndex;
                    final boolean precedence = targetCount > 0 && (firstActivityIndex < 0 || firstTargetIndex < firstActivityIndex);
                    yield new Verdict(name, applicable, response || precedence);
                }
                case chainResponse -> new Verdict(name, activityCount > 0, chainViolated || (activityCount > 0 && lastActivityIndex == result.length - 1));
                case chainPrecedence -> new Verdict(name, targetCount > 0, chainViolated);
                case notCoexistence -> new Verdict(name, true, activityCount > 0 && targetCount > 0);
                case duration -> {
                    final long limit = constraint.maxDurationValue().toMillis();
                    if (constraint.activity == null) {
                        yield new Verdict(name, true, result.durationMillis() > limit);
                    }
                    final Long from = firstSeen.get(constraint.activity);
                    final Long to = lastSeen.get(constraint.target);
                    final boolean applicable = from != null && to != null && to >= from;
                    yield new Verdict(name, applicable, applicable && to - from > limit);
                }
            };
        }
    }
}
