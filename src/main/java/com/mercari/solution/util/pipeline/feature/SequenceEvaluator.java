package com.mercari.solution.util.pipeline.feature;

import com.mercari.solution.util.ExpressionUtil;
import com.mercari.solution.util.pipeline.Filter;

import java.io.Serializable;
import java.time.Duration;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Evaluates sequence-scope columns for one row from that entity's strictly-past history
 * (rows ordered by event time, ascending). Implements the window semantics of docs/design/feature-dsl.md §4.3 and
 * the near-edge shift of §6.2: a past row contributes only if {@code t' ≤ t − windowShift}.
 *
 * <p>Two evaluation paths share the same semantics:
 * <ul>
 *   <li><b>Incremental</b>: a {@link Summary} state per column — and per filter value for single-equality
 *       {@code $self} filters — advanced by two monotonic pointers over the time-ordered history (fold a
 *       contribution in once its row is visible, evict it once older than maxAge). A column runs here when its
 *       statistic has a summary family ({@link OperatorCatalog#summary}), the window has no {@code maxEvents} and
 *       no general filter, and either has no {@code maxAge} or the family is {@link Summary#invertible}
 *       (max / min cannot evict). Used for {@code aggregate} / encoding statistics; turns the per-key cost from
 *       O(n²) into O(n).</li>
 *   <li><b>Scan</b>: binary-searched window bounds + a {@code subList} view (no copying) for everything
 *       else (lag / trend / predicates / maxEvents windows / general filters / a {@code weightBy}
 *       aggregate, whose weights depend on the current row).</li>
 * </ul>
 *
 * <p>The evaluator owns the extraction — which value of a past row contributes ({@link #contribution}) and the
 * null convention of its outputs ({@link #readStatistic}); the summary owns the arithmetic.
 */
public class SequenceEvaluator implements Serializable {

    /** A buffered past row: event time millis + the projected field values. */
    /** One replayed row's projection. {@code values} must be a MUTABLE map: {@link History#trim} removes fields from it. */
    public record Past(long millis, Map<String, Object> values) implements Serializable {}

    /** {@code past = $self.self} equality filter, dispatchable to per-value accumulators. */
    record EqualityFilter(String pastField, String selfField) implements Serializable {}

    private static final Pattern EQUALITY = Pattern.compile("^\\s*([A-Za-z_][A-Za-z0-9_]*)\\s*=\\s*\\$self\\.([A-Za-z_][A-Za-z0-9_]*)\\s*$");

    /** Pre-resolved execution plan of one column (parsed once in {@link #setup()}). */
    static final class ColumnPlan implements Serializable {
        long shiftMillis;
        Long maxAgeMillis;
        /** A window on a calendar clock: its {@code maxAge} in ticks and the calendar (null on wall time). */
        Long maxAgeTicks;
        Clock windowClock;
        Integer maxEvents;
        String filterText;
        EqualityFilter equality;
        /** An order-dependent aggregate ({@link SeriesStats}: zeroCross / peaks / acf / pacf / ar), or null. */
        SeriesStats.Readout series;
        String offset;
        boolean incremental;
        String field;
        /** regression: the explanatory field ({@code field} is regressed against it) and the events it leads by. */
        String against;
        int lag;
        /** fracdiff: the truncated filter, newest event first ({@code w[0] = 1}). */
        double[] fracdiffWeights;
        /** barrier: the relative moves that touch the upper / lower barrier (±∞ when not declared). */
        double barrierUp, barrierDown;
        /** The aggregate / regression func as the replay reads it ({@link #func}), or null. */
        String func;
        String stat; // aggregate func / encoding stat token (drives the extraction of a contribution)
        /** The summary family and readout the statistic runs on incrementally, or null (scan only). */
        Summary.Spec summary;
        /** The family's empty state, read for a filter value with no visible contribution (never mutated). */
        Serializable empty;
        /** The aggregate's per-event weight expression ({@code weightBy}), or null. */
        String weightBy;
        /** rating: the update rule and its keys, or null. Its running state is a {@link Rating.State}, not a summary. */
        Rating rating;
        /** rating: the team member the column reads (0 = the rated player), or the whole team. */
        int ratingMember;
        boolean ratingTeam;
        /**
         * The key of the running state in {@link KeyState}: the column's canonical name, or the one shared by the
         * components of a dynamics channel (one state, read out once per component column).
         */
        String stateKey;
    }

    /**
     * A compiled {@code weightBy}: the expression and its variables split into the current row's ({@code $self.f},
     * spelled {@code __self_f} at runtime) and the event's.
     */
    private record Weight(ExpressionUtil.Expression expression, List<String> selfVariables, List<String> selfFields, List<String> pastFields) {

        static Weight of(final String text) {
            final ExpressionUtil.Expression expression = ExpressionUtil.createDefaultExpression(text.replace("$self.", FeatureValues.SELF_PREFIX));
            final List<String> selfVariables = new ArrayList<>(), selfFields = new ArrayList<>(), pastFields = new ArrayList<>();
            for (final String name : expression.getVariableNames()) {
                if (name.startsWith(FeatureValues.SELF_PREFIX)) {
                    selfVariables.add(name);
                    selfFields.add(name.substring(FeatureValues.SELF_PREFIX.length()));
                } else {
                    pastFields.add(name);
                }
            }
            return new Weight(expression, selfVariables, selfFields, pastFields);
        }
    }

    /** Running state of one column: fold / evict pointers and one summary state per filter value (key "" without a filter). */
    static final class ColumnState {
        int foldIndex;
        int evictIndex;
        final Map<String, Serializable> bySubkey = new HashMap<>();

        Serializable state(final String subkey, final ColumnPlan plan) {
            return bySubkey.computeIfAbsent(subkey, k -> plan.summary.family().create());
        }
    }

    /** Incremental state of one key's group; created per key by the keyed DoFn. */
    public static final class KeyState {
        final Map<String, ColumnState> columns = new HashMap<>();

        ColumnState column(final String canonical) {
            return columns.computeIfAbsent(canonical, k -> new ColumnState());
        }
    }

    /**
     * A key's strictly-past history with a droppable prefix. Indices are absolute (they never shift), so the
     * fold / evict pointers in {@link ColumnState} stay valid after {@link #trimBefore(int)} discards the
     * entries no column will read again; the memory held per key is then bounded by the longest window
     * instead of the key's whole past. Reading a trimmed index is a programming error.
     */
    public static final class History extends java.util.AbstractList<Past> {
        private final ArrayList<Past> entries = new ArrayList<>();
        private int base;

        @Override
        public Past get(final int index) {
            if (index < base) throw new IndexOutOfBoundsException("history index " + index + " was trimmed (base " + base + ")");
            return entries.get(index - base);
        }

        /** Absolute size: trimmed prefix included. */
        @Override
        public int size() {
            return base + entries.size();
        }

        @Override
        public boolean add(final Past p) {
            return entries.add(p);
        }

        /** Entries currently held in memory. */
        public int retained() {
            return entries.size();
        }

        public int base() {
            return base;
        }

        /** Drops entries below {@code absoluteIndex}; amortised so a caller may invoke it per row. */
        public void trimBefore(final int absoluteIndex) {
            final int drop = Math.min(absoluteIndex, size()) - base;
            if (drop <= 0) return;
            // shifting the ArrayList is O(retained); only pay it once the droppable prefix is a good share
            if (drop < 1024 && drop * 2 < entries.size()) return;
            entries.subList(0, drop).clear();
            base += drop;
        }

        private int[] cleared;

        /**
         * Per-field retention: drops the entries below {@link Watermarks#all()} and removes every field from
         * the entries below its own watermark, so a column that reads the whole history of a key keeps only
         * the fields it reads there while the other fields are trimmed to their own windows. Each field's
         * pointer only moves forward: amortised O(1) per row. An entry whose last field is removed keeps a
         * shared empty map (the skeleton — {@code Past} + list slot — still costs ~40 bytes per row, which is
         * why an unbounded column governs the retained ROW COUNT even though the other fields are trimmed).
         * Entries below the overall watermark are dropped wholesale and skipped by the field pass.
         */
        public void trim(final Watermarks w) {
            if (cleared == null) cleared = new int[w.fields.length];
            for (int f = 0; f < w.fields.length; f++) {
                final int to = Math.min(w.byField[f], size());
                final int from = Math.max(cleared[f], Math.max(base, w.all));
                if (to <= from) continue;
                for (int i = from; i < to; i++) {
                    final Past p = entries.get(i - base);
                    final Map<String, Object> values = p.values();
                    if (values.isEmpty()) continue;
                    values.remove(w.fields[f]);
                    if (values.isEmpty()) entries.set(i - base, new Past(p.millis(), Map.of()));
                }
                cleared[f] = to;
            }
            trimBefore(w.all);
        }
    }

    /**
     * Reusable trim watermarks of a keyed stage (one instance per stage, refilled per row — no allocation on
     * the replay hot path): {@code all} = first absolute index any column may still read (entries below it are
     * dropped); {@code byField} = per projected field, the first index a column reading that field may still
     * read (the field is removed from older entries). Fields read by an always-unbounded column (scan path, no
     * maxAge, no bounded tail) are pinned at 0 once at registration, and columns whose watermarks are fully
     * pinned are skipped per row.
     */
    public static final class Watermarks {
        final String[] fields;
        final int[] byField;
        final boolean[] pinned;
        private final Map<String, Integer> index = new HashMap<>();
        int all;
        boolean anyUnbounded;

        public Watermarks(final Collection<String> fields) {
            this.fields = fields.toArray(new String[0]);
            this.byField = new int[this.fields.length];
            this.pinned = new boolean[this.fields.length];
            for (int i = 0; i < this.fields.length; i++) index.put(this.fields[i], i);
        }

        public int all() {
            return all;
        }

        public int of(final String field) {
            final Integer i = index.get(field);
            return i == null ? Integer.MAX_VALUE : byField[i];
        }

        public void reset(final int historySize) {
            all = anyUnbounded ? 0 : historySize;
            for (int i = 0; i < byField.length; i++) byField[i] = pinned[i] ? 0 : historySize;
        }
    }

    /** Per-column state for {@link #retainInto}: watermark ordinals and the statically-skippable flag. */
    private static final class RetainPlan {
        int[] ordinals;
        boolean skip;
    }

    /**
     * Registers this evaluator's columns on the stage's {@link Watermarks}: resolves each column's projected
     * fields to ordinals, pins the fields of always-unbounded columns at 0, and marks the columns whose every
     * watermark is already pinned (their per-row computation would change nothing) as skippable.
     * Call after {@link #setup()}, once per evaluator, with the watermarks shared by the stage.
     */
    public void register(final Watermarks w) {
        retainPlans = new HashMap<>();
        final List<OutputColumn> unboundedFirst = new ArrayList<>(columns);
        unboundedFirst.sort(Comparator.comparing(c -> !unbounded(plans.get(c.canonicalName), c)));
        for (final OutputColumn c : unboundedFirst) {
            final RetainPlan rp = new RetainPlan();
            rp.ordinals = c.pastInputs.stream().map(w.index::get).filter(Objects::nonNull).mapToInt(Integer::intValue).toArray();
            if (unbounded(plans.get(c.canonicalName), c)) {
                w.anyUnbounded = true;
                for (final int i : rp.ordinals) w.pinned[i] = true;
                rp.skip = true; // its watermarks are pinned at 0 by reset()
            }
            retainPlans.put(c.canonicalName, rp);
        }
        for (final OutputColumn c : columns) {
            final RetainPlan rp = retainPlans.get(c.canonicalName);
            if (rp.skip || !w.anyUnbounded) continue;
            boolean allPinned = true;
            for (final int i : rp.ordinals) allPinned &= w.pinned[i];
            rp.skip = allPinned; // `all` is pinned at 0 too, so this column cannot lower any watermark
        }
    }

    /** Folds this evaluator's columns' retention into the stage watermarks ({@code reset} first). */
    public void retainInto(final KeyState state, final long nowMillis, final List<Past> history, final Watermarks w) {
        for (final OutputColumn c : columns) {
            final RetainPlan rp = retainPlans.get(c.canonicalName);
            if (rp.skip) continue;
            final int from = columnRetainFrom(c, state, nowMillis, history);
            if (from < w.all) w.all = from;
            for (final int i : rp.ordinals) {
                if (from < w.byField[i]) w.byField[i] = from;
            }
        }
    }

    /**
     * How many rows before the near edge a scan-path column without {@code maxAge} can need, or null when
     * unbounded. Without a filter the window is a suffix of the history, so {@code maxEvents} bounds it, and
     * {@code lag} / {@code delta} / {@code trend} read only their last {@code k} (+1) rows. A filter (equality
     * included) may skip arbitrarily many rows, so it is unbounded on this path.
     */
    static Integer tailSize(final ColumnPlan plan, final OutputColumn c) {
        if (plan.filterText != null) return null;
        if (plan.maxEvents != null) return plan.maxEvents;
        final String k = c.coordinates.get("k");
        return switch (c.operator) {
            case "lag", "trend", "fracdiff" -> k == null ? null : Integer.parseInt(k);
            case "delta" -> k == null ? null : Integer.parseInt(k) + 1;
            default -> null;
        };
    }

    /** Columns that read the whole history (scan path, no maxAge, no bounded tail): their past inputs are kept for every row of the key. */
    public List<String> unboundedColumns() {
        final List<String> names = new ArrayList<>();
        for (final OutputColumn c : columns) {
            if (unbounded(plans.get(c.canonicalName), c)) names.add(c.canonicalName);
        }
        return names;
    }

    private static boolean unbounded(final ColumnPlan plan, final OutputColumn c) {
        // a rating on the scan path replays every visible contest from the start of its window — no bounded tail
        // exists for it, whatever tailSize() would make of its coordinates. With checkWindowContract in force this
        // says exactly what the general rule below already says (a rating carries no maxAge / maxEvents / filter, so
        // tailSize is null); it is stated so that admitting one of those cannot silently make a rating trimmable.
        if (plan.rating != null) return !plan.incremental;
        return !plan.incremental && !hasMaxAge(plan) && tailSize(plan, c) == null;
    }

    /**
     * Whether a sequence column keeps the whole projected history of its key (compile-time view of
     * {@link #unboundedColumns()}), or null when it is bounded: the reason to show in a diagnostic.
     */
    public static String unboundedReason(final OutputColumn c) {
        final SequenceEvaluator evaluator = c.scope == FeatureSpec.Scope.population
                ? new PopulationEvaluator(List.of(c)) : new SequenceEvaluator(List.of(c));
        final ColumnPlan plan = evaluator.plan(c);
        if (!unbounded(plan, c)) return null;
        if (plan.filterText != null) return "a window with a filter and no maxAge";
        if (plan.weightBy != null) return "a weightBy aggregate without maxAge or maxEvents";
        return c.operator + " without maxAge";
    }

    /**
     * First absolute history index this column may still read (its trim watermark): the evict pointer (or
     * fold pointer for an unbounded window) on the incremental path, the {@code maxAge} far edge on the scan
     * path (or the near edge minus the {@link #tailSize bounded tail} without maxAge); {@code 0} when the
     * column is an unbounded scan.
     */
    private int columnRetainFrom(final OutputColumn c, final KeyState state, final long nowMillis, final List<Past> history) {
        final ColumnPlan plan = plans.get(c.canonicalName);
        if (plan.incremental) {
            final ColumnState cs = state.columns.get(plan.stateKey);
            return cs == null ? 0 : (!hasMaxAge(plan) ? cs.foldIndex : cs.evictIndex);
        }
        if (!hasMaxAge(plan)) {
            final Integer tail = tailSize(plan, c);
            if (tail == null) return 0;
            // the window is the suffix of the history before the near edge; the near edge only moves
            // forward, so rows more than `tail` behind it are never read again
            return Math.max(0, upperBound(history, nowMillis - plan.shiftMillis) - tail);
        }
        return lowerBound(history, farEdge(plan, nowMillis));
    }

    /** Whether the window has a far edge ({@code maxAge} on wall time or on a calendar clock). */
    static boolean hasMaxAge(final ColumnPlan plan) {
        return plan.maxAgeMillis != null || plan.maxAgeTicks != null;
    }

    /**
     * The window's far edge at {@code nowMillis}: the earliest event time still inside ({@code now − maxAge} on wall
     * time; on a calendar clock the start of the tick {@code maxAge} ticks before the row's, which only moves forward
     * with {@code now} — the eviction pointers rely on that).
     */
    static long farEdge(final ColumnPlan plan, final long nowMillis) {
        return plan.maxAgeTicks != null ? plan.windowClock.farEdgeMillis(nowMillis, plan.maxAgeTicks) : nowMillis - plan.maxAgeMillis;
    }

    private final List<OutputColumn> columns;
    private final boolean forceScan;
    private transient Map<String, Filter.ConditionNode> conditions;
    private transient Map<String, ColumnPlan> plans;
    private transient Map<String, RetainPlan> retainPlans;
    private transient Map<String, Weight> weights;

    public SequenceEvaluator(final List<OutputColumn> columns) {
        this(columns, false);
    }

    /** @param forceScan disable the incremental path (equivalence testing) */
    SequenceEvaluator(final List<OutputColumn> columns, final boolean forceScan) {
        this.columns = columns;
        this.forceScan = forceScan;
    }

    public List<OutputColumn> getColumns() {
        return columns;
    }

    /** Fields that must be kept in the per-entity buffer for these columns. */
    public Set<String> bufferedFields() {
        final Set<String> fields = new LinkedHashSet<>();
        for (final OutputColumn c : columns) fields.addAll(c.pastInputs);
        return fields;
    }

    public void setup() {
        conditions = new HashMap<>();
        plans = new HashMap<>();
        weights = new HashMap<>();
        for (final OutputColumn c : columns) {
            final String weightBy = c.coordinates.get("weightBy");
            if (weightBy != null) weights.computeIfAbsent(weightBy, Weight::of);
            for (final String key : List.of("filter", "predicate")) {
                final String text = c.coordinates.get(key);
                if (text != null && !conditions.containsKey(text)) {
                    conditions.put(text, Filter.parse(text.replace("$self.", FeatureValues.SELF_PREFIX)));
                }
            }
            final ColumnPlan plan = plan(c);
            checkWindowContract(c, plan);
            plans.put(c.canonicalName, plan);
        }
        checkSharedStates();
    }

    /**
     * Columns that share one running state (the readouts of a rating op, the components of a dynamics channel) share
     * its fold pointer: the first one read advances it to ITS near edge. They must therefore agree on the window shift
     * — a column with a shorter one (a self side classified apart: a violation kept as an intermediate has none) would
     * fold contests / events the others must not see yet, and hand them over without a word. The compiler gives the
     * columns of one state one availability contract; this is that invariant, checked where the state is shared.
     */
    private void checkSharedStates() {
        final Map<String, OutputColumn> first = new HashMap<>();
        for (final OutputColumn c : columns) {
            final ColumnPlan plan = plans.get(c.canonicalName);
            if (plan.stateKey == null) continue;
            final OutputColumn other = first.putIfAbsent(plan.stateKey, c);
            if (other != null && plans.get(other.canonicalName).shiftMillis != plan.shiftMillis) {
                throw new IllegalStateException("columns " + other.canonicalName + " and " + c.canonicalName + " share the running state '" + plan.stateKey
                        + "' but not its window shift (" + plans.get(other.canonicalName).shiftMillis + " ms vs " + plan.shiftMillis + " ms): the shorter one"
                        + " would advance the state past the other's near edge - the columns of one state need one availability contract (the same self and past inputs)");
            }
        }
    }

    /**
     * The window contracts this evaluator cannot honour, checked once per column when a stage is about to RUN it.
     * It deliberately sits outside {@link #plan}: the compile layer calls that through {@link #unboundedReason} to
     * describe a column, where an exception would replace a diagnostic with a crash of the compiler.
     *
     * <p>A {@code rating} carries no eviction: its running state cannot take an update back ({@link #advanceRating}
     * only folds forward), and truncating the scan window would hand {@link Rating#replay} a contest cut in half.
     * The compiler therefore admits the unbounded, filter-less window only (`sequence.rating.window`); a column that
     * arrives here carrying one is a compile layer that relaxed the rule without implementing it.
     */
    private static void checkWindowContract(final OutputColumn c, final ColumnPlan plan) {
        if (plan.rating == null) return;
        if (plan.ratingMember < 0 || plan.ratingMember >= plan.rating.members().size()) {
            throw new IllegalStateException("rating column " + c.canonicalName + " reads member " + plan.ratingMember + " of a team of " + plan.rating.members().size());
        }
        if (plan.maxEvents != null || plan.filterText != null || hasMaxAge(plan)) {
            throw new IllegalStateException("rating column " + c.canonicalName + " carries a window this evaluator cannot honour"
                    + " (maxEvents / filter / maxAge): the compiler rejects it with sequence.rating.window - admitting one means"
                    + " implementing the eviction the running state lacks (advanceRating) and keeping the contests of a truncated"
                    + " window whole (select / Rating.replay)");
        }
    }

    private ColumnPlan plan(final OutputColumn c) {
        final ColumnPlan plan = new ColumnPlan();
        plan.shiftMillis = c.windowShift == null ? 0L : c.windowShift.toMillis();
        final String maxAge = c.coordinates.get("maxAge");
        plan.maxAgeMillis = maxAge == null ? null : Duration.parse(maxAge).toMillis();
        final String maxAgeTicks = c.coordinates.get("maxAgeTicks");
        if (maxAgeTicks != null) {
            plan.maxAgeTicks = Long.parseLong(maxAgeTicks);
            plan.windowClock = c.clocks.get(c.coordinates.get("windowClock"));
            if (plan.windowClock == null) throw new IllegalStateException("the calendar clock '" + c.coordinates.get("windowClock") + "' of " + c.canonicalName + " is not attached to the column");
        }
        final String maxEvents = c.coordinates.get("maxEvents");
        plan.maxEvents = maxEvents == null ? null : Integer.parseInt(maxEvents);
        plan.filterText = c.coordinates.get("filter");
        if (plan.filterText != null) {
            final Matcher m = EQUALITY.matcher(plan.filterText);
            if (m.matches()) plan.equality = new EqualityFilter(m.group(1), m.group(2));
        }
        plan.field = c.coordinates.get("field");
        plan.against = c.coordinates.get("against");
        plan.lag = Integer.parseInt(c.coordinates.getOrDefault("lag", "0"));
        if ("fracdiff".equals(c.operator)) {
            plan.fracdiffWeights = fracdiffWeights(Double.parseDouble(c.coordinates.get("d")), Integer.parseInt(c.coordinates.get("k")));
        }
        if ("barrier".equals(c.operator)) {
            final String up = c.coordinates.get("up"), down = c.coordinates.get("down");
            plan.barrierUp = up == null ? Double.POSITIVE_INFINITY : Double.parseDouble(up);
            plan.barrierDown = down == null ? Double.NEGATIVE_INFINITY : Double.parseDouble(down);
        }
        plan.offset = c.coordinates.containsKey("offset") ? "__baseline_" + c.coordinates.get("offset") : null;
        plan.func = func(c);
        plan.stat = statToken(c);
        plan.weightBy = c.coordinates.get("weightBy");
        plan.stateKey = c.coordinates.getOrDefault("stateKey", c.canonicalName);
        plan.summary = summaryOf(c);
        plan.empty = plan.summary == null ? null : plan.summary.family().create();
        plan.series = "aggregate".equals(c.operator) ? SeriesStats.parse(plan.func) : null;
        plan.incremental = !forceScan
                && plan.summary != null
                // a weight may read the current row: the catalog declares weighted statistics scan-only
                && (plan.weightBy == null || OperatorCatalog.summary(plan.stat, true) != null)
                && plan.maxEvents == null
                && (plan.filterText == null || plan.equality != null)
                // a window evicts: only a group (invertible family) can remove a contribution again
                && (!hasMaxAge(plan) || plan.summary.family().invertible());
        if ("rating".equals(c.operator)) {
            // not a summary (an update reads the state the earlier contests left), yet a running state all the same:
            // the compiler admits the unbounded, filter-less window only (`sequence.rating.window`), which the fold
            // pointer serves. Nothing here re-derives that contract — a column that breaks it is rejected by
            // checkWindowContract rather than quietly routed to a scan path that cannot honour it either.
            plan.rating = Rating.of(c.coordinates);
            plan.ratingTeam = "team".equals(c.coordinates.get("readout"));
            plan.ratingMember = c.coordinates.get("memberIndex") == null ? 0 : Integer.parseInt(c.coordinates.get("memberIndex"));
            plan.incremental = !forceScan;
        }
        return plan;
    }

    /**
     * The {@code aggregate} / {@code regression} func the evaluator runs. A future column ({@code direction: future})
     * is replayed on the mirrored clock, where the replay's newest event is the nearest one: the declared
     * {@code first} (nearest) reads the replay's {@code last}, and {@code last} (furthest) its {@code first}. The
     * coordinates keep the declared func — what the column name, the schema options and the manifest say.
     */
    static String func(final OutputColumn c) {
        final String func = c.coordinates.get("func");
        if (func == null || !"future".equals(c.coordinates.get("direction"))) return func;
        return switch (func) {
            case "first" -> "last";
            case "last" -> "first";
            default -> func;
        };
    }

    /** The statistic token of the column ({@code aggregate} / {@code regression} func); overridden for encoding stats. */
    String statToken(final OutputColumn c) {
        return "aggregate".equals(c.operator) || "regression".equals(c.operator) ? func(c) : null;
    }

    /**
     * The summary family the column's statistic runs on incrementally, or null when it is scan-only. A lagged
     * regression pairs an event with an earlier one: that pair is not a contribution of one event (evicting the
     * far edge would need the rows before it), so it has no family. {@code ewma} is the order-0 exponential
     * {@link Dynamics} (sugar over the same coordinates as the general form's {@code dynamics} columns).
     */
    Summary.Spec summaryOf(final OutputColumn c) {
        return switch (c.operator) {
            case "ewma", "dynamics" -> "bilinear".equals(c.coordinates.get("family"))
                    ? Signature.spec(c.coordinates, c.clocks) : Dynamics.spec(c.coordinates, c.clocks);
            case "aggregate" -> OperatorCatalog.summary(func(c));
            case "regression" -> c.coordinates.containsKey("lag") ? null : OperatorCatalog.summary(func(c));
            default -> null;
        };
    }

    public void evaluate(final Map<String, Object> row, final long nowMillis, final List<Past> history) {
        for (final OutputColumn c : columns) {
            row.put(c.canonicalName, evaluateColumn(c, row, nowMillis, history, null));
        }
    }

    Object evaluateColumn(final OutputColumn c, final Map<String, Object> row, final long nowMillis,
                          final List<Past> history, final KeyState state) {
        final ColumnPlan plan = plans.get(c.canonicalName);
        if (plan.incremental && state != null) {
            // a rating's running state is a Rating.State the fold pointer advances, not a summary
            if (plan.rating != null) {
                return readRating(plan, advanceRating(plan, state, nowMillis, history), row, nowMillis);
            }
            final Serializable summary = advance(c, plan, state, nowMillis, history, row);
            return readStatistic(c, plan, summary == null ? plan.empty : summary, nowMillis);
        }
        final List<Past> window = select(plan, row, nowMillis, history);
        return evaluateScan(c, plan, row, nowMillis, window);
    }

    /**
     * Advances the column's fold / evict pointers to {@code now} and returns the summary state to read: the one of
     * the row's own filter value (null when that value has no visible contribution yet or the row's value is null).
     */
    final Serializable advance(final OutputColumn c, final ColumnPlan plan, final KeyState state,
                               final long nowMillis, final List<Past> history, final Map<String, Object> row) {
        final ColumnState cs = state.column(plan.stateKey);
        final long nearEdge = nowMillis - plan.shiftMillis;
        while (cs.foldIndex < history.size() && history.get(cs.foldIndex).millis() <= nearEdge) {
            apply(plan, cs, history.get(cs.foldIndex), 1);
            cs.foldIndex++;
        }
        if (hasMaxAge(plan)) {
            final long farEdge = farEdge(plan, nowMillis);
            while (cs.evictIndex < cs.foldIndex && history.get(cs.evictIndex).millis() < farEdge) {
                apply(plan, cs, history.get(cs.evictIndex), -1);
                cs.evictIndex++;
            }
        }
        final String subkey = plan.equality == null ? "" : FeatureValues.toText(row.get(plan.equality.selfField()));
        return subkey == null ? null : cs.bySubkey.get(subkey);
    }

    /** A rating column's value for a row: the rating of the member it reads — the rated player unless told otherwise — or of the row's whole team. */
    private static Object readRating(final ColumnPlan plan, final Rating.State ratings, final Map<String, Object> row, final long nowMillis) {
        if (plan.ratingTeam) return plan.rating.readTeam(ratings, plan.rating.teamOf(row), plan.func, nowMillis);
        return plan.rating.read(ratings, plan.ratingMember, plan.rating.memberKey(row, plan.ratingMember), plan.func, nowMillis);
    }

    /**
     * Advances a rating's fold pointer to {@code now}, one event time at a time: the rows sharing a time are the
     * contests held then (they joined the history together), and a contest updates all its players at once. The
     * readout columns of one op share the state ({@code stateKey}): the first one read advances it.
     */
    private Rating.State advanceRating(final ColumnPlan plan, final KeyState state, final long nowMillis, final List<Past> history) {
        final ColumnState cs = state.column(plan.stateKey);
        final Rating.State ratings = (Rating.State) cs.bySubkey.computeIfAbsent("", k -> new Rating.State());
        final long nearEdge = nowMillis - plan.shiftMillis;
        while (cs.foldIndex < history.size() && history.get(cs.foldIndex).millis() <= nearEdge) {
            final long millis = history.get(cs.foldIndex).millis();
            int end = cs.foldIndex + 1;
            while (end < history.size() && history.get(end).millis() == millis) end++;
            plan.rating.fold(ratings, history.subList(cs.foldIndex, end));
            cs.foldIndex = end;
        }
        return ratings;
    }

    private void apply(final ColumnPlan plan, final ColumnState cs, final Past p, final int sign) {
        final String subkey = plan.equality == null ? "" : FeatureValues.toText(p.values().get(plan.equality.pastField()));
        if (subkey == null) return;
        final Object contribution = contribution(plan, p);
        if (contribution == null) return;
        plan.summary.<Serializable>typed().update(cs.state(subkey, plan), contribution, sign);
    }

    /**
     * What one past row contributes to the column's summary, or null when it contributes nothing (a missing
     * value); overridden by the population evaluator. A field-less count contributes a bare 0 (every visible row
     * counts, nulls included); a field contributes its numeric value when it is {@link #finite}.
     */
    Object contribution(final ColumnPlan plan, final Past p) {
        // a path event: missing values still advance the events clock (a field-less channel is the constant 1)
        if (plan.summary.family() instanceof Dynamics dynamics) return dynamics.event(p, plan.field);
        // a point of a multi-channel path (none when a channel is missing)
        if (plan.summary.family() instanceof Signature signature) return signature.event(p);
        if (plan.field == null) return 0d;
        if (plan.against != null) return pair(p.values().get(plan.against), p.values().get(plan.field));
        return finite(p.values().get(plan.field));
    }

    /**
     * A past value as a statistic of the window reads it: null when missing — null, non-numeric, NaN or ±∞. The
     * incremental and the scan path share this rule; a non-finite contribution would also poison a running sum for
     * good (NaN stays NaN, and evicting an ∞ leaves ∞ − ∞ = NaN), where the scan recovers once it leaves the window.
     */
    static Double finite(final Object value) {
        final Double d = FeatureValues.toDouble(value);
        return d == null || !Double.isFinite(d) ? null : d;
    }

    /** The (x, y) contribution of a regression, or null when either value is missing ({@link #finite}). */
    private static double[] pair(final Object x, final Object y) {
        final Double dx = finite(x), dy = finite(y);
        return dx == null || dy == null ? null : new double[]{dx, dy};
    }

    /**
     * Reads the column's value from a summary state (the family's empty state when the filter value has none) as of
     * the row's time; overridden by the population evaluator. Extrema are cast to the column type (they carry the
     * input type).
     */
    Object readStatistic(final OutputColumn c, final ColumnPlan plan, final Serializable state, final long nowMillis) {
        final Object value = plan.summary.<Serializable>typed().readAt(state, plan.summary.readout(), nowMillis);
        if (plan.stat == null) return value;
        return switch (plan.stat) {
            case "max", "min" -> value == null ? null : FeatureValues.cast((Double) value, c.fieldType);
            default -> value;
        };
    }

    Object evaluateScan(final OutputColumn c, final ColumnPlan plan, final Map<String, Object> row,
                        final long nowMillis, final List<Past> window) {
        final String field = plan.field;
        // an order-dependent aggregate reads the window's present values as one series, in time order
        if (plan.series != null) return SeriesStats.read(plan.series, series(window, field));
        switch (c.operator) {
            case "lag" -> {
                final int k = Integer.parseInt(c.coordinates.get("k"));
                return window.size() >= k ? window.get(window.size() - k).values().get(field) : null;
            }
            case "delta" -> {
                final int k = Integer.parseInt(c.coordinates.get("k"));
                if (window.size() < k + 1) return null;
                final Double a = FeatureValues.toDouble(window.get(window.size() - k).values().get(field));
                final Double b = FeatureValues.toDouble(window.get(window.size() - k - 1).values().get(field));
                return a == null || b == null ? null : a - b;
            }
            case "trend" -> {
                // sugar: the regression of the last k present values on their order (0, 1, …) — the beta readout of the
                // same family the regression op folds, over a bounded tail
                final int k = Integer.parseInt(c.coordinates.get("k"));
                final Summary<Summary.Regression.State> family = Summary.Summaries.REGRESSION;
                final Summary.Regression.State state = family.create();
                int x = 0;
                for (int i = Math.max(0, window.size() - k); i < window.size(); i++) {
                    final Double y = finite(window.get(i).values().get(field));
                    if (y != null) family.update(state, new double[]{x++, y}, 1);
                }
                return family.read(state, Summary.Readout.of("beta"));
            }
            case "ewma", "dynamics" -> {
                // the direct projection of the window: the reference the running state is equal to
                if (plan.summary.family() instanceof Signature signature) return signature.project(window, plan.summary.readout().parameter().intValue());
                return ((Dynamics) plan.summary.family()).project(window, field, nowMillis, plan.summary.readout().parameter().intValue());
            }
            case "runLength" -> {
                final String value = c.coordinates.get("value");
                long run = 0;
                for (int i = window.size() - 1; i >= 0; i--) {
                    final Object v = window.get(i).values().get(field);
                    if (v != null && value.equals(v.toString())) run++;
                    else break;
                }
                return run;
            }
            case "sinceEvent" -> {
                final Filter.ConditionNode condition = conditions.get(c.coordinates.get("predicate"));
                for (int i = window.size() - 1; i >= 0; i--) {
                    if (Filter.filter(condition, window.get(i).values())) {
                        final String unit = c.coordinates.getOrDefault("unit", "events");
                        return "events".equals(unit) ? (Object) (long) (window.size() - i)
                                : (nowMillis - window.get(i).millis()) / 86_400_000d;
                    }
                }
                return null;
            }
            case "countMatch" -> {
                final Filter.ConditionNode condition = conditions.get(c.coordinates.get("predicate"));
                long n = 0;
                for (final Past p : window) if (Filter.filter(condition, p.values())) n++;
                return n;
            }
            case "aggregate" -> {
                if (plan.weightBy != null) return weightedAggregate(plan.func, window, field, weights.get(plan.weightBy), row);
                return aggregate(plan.func, window, field, c);
            }
            case "regression" -> {
                // the same family as the incremental path, folded over the window; under a lag the field of event
                // i is paired with `against` of event i − lag, both inside the window
                final Summary<Summary.Regression.State> family = Summary.Summaries.REGRESSION;
                final Summary.Regression.State state = family.create();
                for (int i = plan.lag; i < window.size(); i++) {
                    final double[] pair = pair(window.get(i - plan.lag).values().get(plan.against), window.get(i).values().get(field));
                    if (pair != null) family.update(state, pair, 1);
                }
                return family.read(state, Summary.Readout.of(plan.func));
            }
            case "fracdiff" -> {
                final double[] w = plan.fracdiffWeights;
                if (window.size() < w.length) return null;
                double value = 0;
                for (int j = 0; j < w.length; j++) {
                    final Double x = FeatureValues.toDouble(window.get(window.size() - 1 - j).values().get(field));
                    if (x == null || x.isNaN()) return null;
                    value += w[j] * x;
                }
                return Double.isFinite(value) ? value : null;
            }
            case "rating" -> {
                // the reference the running state is equal to: every visible contest folded from scratch
                return readRating(plan, plan.rating.replay(window), row, nowMillis);
            }
            case "barrier" -> {
                // a future window on the mirrored clock: the nearest event is the window's newest, so the path runs
                // from the end of the list; the entry is the current row's own value
                final Double entry = finite(row.get(field));
                if (entry == null || entry == 0) return null;
                boolean path = false;
                for (int i = window.size() - 1; i >= 0; i--) {
                    final Double x = finite(window.get(i).values().get(field));
                    if (x == null) continue;
                    path = true;
                    // (x − entry) / |entry|: exact at a declared level (100 → 90 is −0.1, where 90 / 100 − 1 rounds
                    // above it), and "up" stays an increase for a negative entry
                    final double move = (x - entry) / Math.abs(entry);
                    if (move >= plan.barrierUp) return 1L;
                    if (move <= plan.barrierDown) return -1L;
                }
                return path ? 0L : null;
            }
            default -> throw new IllegalStateException("unsupported sequence operator: " + c.operator);
        }
    }

    /**
     * Window selection for the scan path: binary-searched bounds, list views instead of copies. The window never
     * spans a trimmed entry, so a readout may walk it from its own start (the {@link Rating} replay does) instead of
     * only touching a bounded tail: both bounds clamp to the {@link History#base()}, and {@link History#trim} never
     * moves that base past any column's {@link #columnRetainFrom} (the watermark is their minimum), so the entries a
     * column can need are all held. The clamp is the identity for a bounded-tail column — the trimmed prefix is by
     * construction outside the tail it reads — so narrowing the view cannot change what such a column returns.
     */
    protected List<Past> select(final ColumnPlan plan, final Map<String, Object> row, final long nowMillis, final List<Past> history) {
        final long nearEdge = nowMillis - plan.shiftMillis;
        // rows sharing the current timestamp are excluded upstream (history holds strictly-past rows only)
        final int hi = upperBound(history, nearEdge);
        final int lo = !hasMaxAge(plan) ? base(history) : lowerBound(history, farEdge(plan, nowMillis));
        if (lo >= hi) return List.of();
        List<Past> ranged = history.subList(lo, hi);
        if (plan.filterText != null) {
            final Filter.ConditionNode condition = conditions.get(plan.filterText);
            final Map<String, Object> selfValues = new HashMap<>();
            for (final Map.Entry<String, Object> e : row.entrySet()) selfValues.put(FeatureValues.SELF_PREFIX + e.getKey(), e.getValue());
            final List<Past> filtered = new ArrayList<>();
            final Map<String, Object> scope = new HashMap<>();
            for (final Past p : ranged) {
                scope.clear();
                scope.putAll(p.values());
                scope.putAll(selfValues);
                if (Filter.filter(condition, scope)) filtered.add(p);
            }
            ranged = filtered;
        }
        if (plan.maxEvents != null && ranged.size() > plan.maxEvents) {
            ranged = ranged.subList(ranged.size() - plan.maxEvents, ranged.size());
        }
        return ranged;
    }

    /** First readable index of a history: its {@link History#base()}, or 0 for a plain list (the tests' oracle). */
    static int base(final List<Past> history) {
        return history instanceof History h ? h.base() : 0;
    }

    /** First index whose millis >= bound. */
    static int lowerBound(final List<Past> history, final long bound) {
        int lo = base(history), hi = history.size();
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if (history.get(mid).millis() < bound) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    /** First index whose millis > bound. */
    static int upperBound(final List<Past> history, final long bound) {
        int lo = base(history), hi = history.size();
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if (history.get(mid).millis() <= bound) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    /** The present ({@link #finite}) values of a field over the window, oldest first. */
    private static double[] series(final List<Past> window, final String field) {
        final double[] x = new double[window.size()];
        int n = 0;
        for (final Past p : window) {
            final Double d = finite(p.values().get(field));
            if (d != null) x[n++] = d;
        }
        return n == x.length ? x : Arrays.copyOf(x, n);
    }

    static Object aggregate(final String func, final List<Past> window, final String field, final OutputColumn c) {
        if (field == null) {
            return (long) window.size();
        }
        final List<Double> values = new ArrayList<>();
        Object first = null, last = null;
        for (final Past p : window) {
            final Object v = p.values().get(field);
            if (v == null) continue;
            if (first == null) first = v;
            last = v;
            final Double d = finite(v);
            if (d != null) values.add(d);
        }
        return switch (func) {
            case "count" -> (long) values.size();
            case "first" -> first;
            case "last" -> last;
            case "sum" -> values.isEmpty() ? null : values.stream().mapToDouble(d -> d).sum();
            case "mean", "avg", "rate" -> values.isEmpty() ? null : values.stream().mapToDouble(d -> d).average().orElse(Double.NaN);
            case "min" -> values.isEmpty() ? null : FeatureValues.cast(values.stream().mapToDouble(d -> d).min().orElse(Double.NaN), c.fieldType);
            case "max" -> values.isEmpty() ? null : FeatureValues.cast(values.stream().mapToDouble(d -> d).max().orElse(Double.NaN), c.fieldType);
            case "std" -> {
                if (values.size() < 2) yield null;
                final double mean = values.stream().mapToDouble(d -> d).average().orElse(Double.NaN);
                yield Math.sqrt(values.stream().mapToDouble(d -> (d - mean) * (d - mean)).sum() / values.size());
            }
            case "skew", "kurt" -> {
                // the family of the incremental path, folded over the window (one arithmetic, one null rule)
                final Summary<Summary.Shape.State> shape = Summary.Summaries.SHAPE;
                final Summary.Shape.State state = shape.create();
                for (final Double d : values) shape.update(state, d, 1);
                yield shape.read(state, Summary.Readout.of(func));
            }
            default -> throw new IllegalStateException("unsupported aggregate func: " + func);
        };
    }

    /**
     * An aggregate under {@code weightBy}: every event of the window is weighed against the current row — the
     * expression reads the event's fields by name and the row's as {@code $self.f} — and contributes when its value
     * is present and its weight is a positive finite number (a null operand makes the weight NaN: no contribution).
     * {@code count} = Σw (0 without contributions), {@code sum} = Σw·x, {@code mean} = Σw·x / Σw, {@code std} = the
     * weighted population deviation (two contributing events at least); with every weight 1 these are the plain
     * aggregates. A field-less count weighs every visible row.
     */
    private static Object weightedAggregate(final String func, final List<Past> window, final String field, final Weight weight, final Map<String, Object> row) {
        final Map<String, Double> variables = new HashMap<>();
        for (int i = 0; i < weight.selfVariables().size(); i++) {
            variables.put(weight.selfVariables().get(i), FeatureValues.toDouble(row.get(weight.selfFields().get(i))));
        }
        final double[] ws = new double[window.size()], xs = new double[window.size()];
        int n = 0;
        double sumW = 0, sumWx = 0;
        for (final Past p : window) {
            final Double x = field == null ? Double.valueOf(0d) : finite(p.values().get(field));
            if (x == null) continue;
            for (final String f : weight.pastFields()) variables.put(f, FeatureValues.toDouble(p.values().get(f)));
            final double w = weight.expression().evaluate(variables);
            if (!(w > 0) || Double.isInfinite(w)) continue;
            ws[n] = w;
            xs[n++] = x;
            sumW += w;
            sumWx += w * x;
        }
        if ("count".equals(func)) return sumW;
        if (n == 0) return null;
        return switch (func) {
            case "sum" -> sumWx;
            case "mean", "avg", "rate" -> sumWx / sumW;
            case "std" -> {
                if (n < 2) yield null;
                final double mean = sumWx / sumW;
                double ss = 0;
                for (int i = 0; i < n; i++) ss += ws[i] * (xs[i] - mean) * (xs[i] - mean);
                yield Math.sqrt(ss / sumW);
            }
            default -> throw new IllegalStateException("aggregate " + func + " has no weighted form");
        };
    }

    static Double slope(final List<Double> ys) {
        final int n = ys.size();
        if (n < 2) return null;
        final double xMean = (n - 1) / 2d;
        final double yMean = ys.stream().mapToDouble(d -> d).average().orElse(0);
        double num = 0, den = 0;
        for (int i = 0; i < n; i++) {
            num += (i - xMean) * (ys.get(i) - yMean);
            den += (i - xMean) * (i - xMean);
        }
        return den == 0 ? null : num / den;
    }

    /**
     * The first {@code k} coefficients of (1 − B)^d: {@code w[0] = 1}, {@code w[j] = −w[j−1] (d − j + 1) / j}
     * ({@code w[j]} weighs the value j events back). d = 1 gives the first difference (1, −1, 0, …).
     */
    static double[] fracdiffWeights(final double d, final int k) {
        final double[] w = new double[k];
        w[0] = 1;
        for (int j = 1; j < k; j++) w[j] = -w[j - 1] * (d - j + 1) / j;
        return w;
    }

}
