package com.mercari.solution.util.pipeline.feature;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The {@code bilinear} family of the Summarize stage (docs/design/feature-dsl.md §1.4, §4.3): the truncated
 * log-signature of the path the window's events trace through the lifted channels. The path is piecewise linear
 * through the events' points (every channel present; {@code timeAugment} adds the event's clock position as a
 * channel), its signature the product {@code exp(Δ₁) ⊗ exp(Δ₂) ⊗ …} of the increments in the truncated tensor algebra,
 * and a readout the coefficient of one Lyndon word in {@code log S} — the Lyndon words index a basis of the free Lie
 * algebra the log-signature lives in, and the coordinate map is triangular, so the columns determine it.
 *
 * <p>Chen's identity makes a concatenation of paths the tensor product of their signatures ({@link #merge}: the two
 * paths joined by the increment between them), and the reversed path the inverse ({@link #inverse}). The state keeps
 * the first and last point with the product; removing the oldest event would need the increment to the one after it,
 * which the state does not hold — the family is a monoid, so a {@code maxAge} window re-reads its events (the scan
 * path), and an unbounded one is folded in O(dim) per row.
 */
public final class Signature implements Summary<Signature.State> {

    /** The largest depth (a level-k tensor has channels^k entries). */
    public static final int MAX_DEPTH = 4;

    /** One event: its time and its channel values ({@code null} when a channel is missing: no point). */
    public record Event(long millis, double[] values) implements Serializable {}

    public static final class State implements Serializable {
        /** Points folded in (events with every channel present). */
        long points;
        double[] first;
        double[] last;
        /** The signature of the path so far, level-major (level 0 = the scalar 1). */
        double[] tensor;
        /**
         * {@code log(tensor)}, computed on the first read and dropped whenever the state changes: every Lyndon-word
         * column of a window shares one state (the {@code stateKey}), so the logarithm is taken once per row instead
         * of once per column. Derived, hence not serialised.
         */
        private transient double[] logarithm;
    }

    private final int channels;
    private final int depth;
    /** The value channels (canonical names), read from each past row. */
    private final List<String> fields;
    /** A time channel (the event's position on the clock) is appended after the value channels. */
    private final boolean timeAugment;
    /** The clock of the time channel: {@code time} (days), {@code events} (the point's ordinal) or a calendar (ticks). */
    private final boolean byEvents;
    private final Clock calendar;
    /** {@code powers[k]} = channels^k, the number of entries of level k. */
    private final int[] powers;
    /** {@code offsets[k]} = where level k starts in a tensor; {@code offsets[depth + 1]} is the tensor's length. */
    private final int[] offsets;
    private final int size;
    private final List<int[]> words;

    public Signature(final List<String> fields, final boolean timeAugment, final int depth, final boolean byEvents, final Clock calendar) {
        this.fields = fields;
        this.timeAugment = timeAugment;
        this.channels = fields.size() + (timeAugment ? 1 : 0);
        this.depth = depth;
        this.byEvents = byEvents;
        this.calendar = calendar;
        this.offsets = new int[depth + 2];
        this.powers = new int[depth + 1];
        int length = 1;
        for (int k = 0; k <= depth; k++) {
            powers[k] = length;
            offsets[k] = k == 0 ? 0 : offsets[k - 1] + powers[k - 1];
            length *= channels;
        }
        offsets[depth + 1] = offsets[depth] + powers[depth];
        this.size = offsets[depth + 1];
        this.words = lyndonWords(channels, depth);
    }

    /**
     * The family and readout of a column: {@code fields} (comma-separated channels), {@code timeAugment}, {@code depth},
     * {@code decayBy} (the time channel's clock) and {@code word} (the index of the column's Lyndon word).
     */
    public static Summary.Spec spec(final Map<String, String> coordinates, final Map<String, Clock> clocks) {
        final String decayBy = coordinates.getOrDefault("decayBy", "events");
        final Clock calendar = Clock.BUILT_IN.contains(decayBy) ? null : clocks.get(decayBy);
        if (!Clock.BUILT_IN.contains(decayBy) && calendar == null) {
            throw new IllegalStateException("the calendar clock '" + decayBy + "' of the time channel is not attached to the column");
        }
        final String fields = coordinates.getOrDefault("fields", "");
        final Signature family = new Signature(fields.isEmpty() ? List.of() : List.of(fields.split(",")),
                "true".equals(coordinates.get("timeAugment")), Integer.parseInt(coordinates.get("depth")), "events".equals(decayBy), calendar);
        return new Summary.Spec(family, Summary.Readout.of("word", Integer.parseInt(coordinates.get("word"))));
    }

    public int channels() { return channels; }
    public List<int[]> words() { return words; }

    /** The event of a past row: its channel values, or none when one is missing ({@link SequenceEvaluator#finite}). */
    public Event event(final SequenceEvaluator.Past p) {
        final double[] values = new double[fields.size()];
        for (int i = 0; i < values.length; i++) {
            final Double v = SequenceEvaluator.finite(p.values().get(fields.get(i)));
            if (v == null) return null;
            values[i] = v;
        }
        return new Event(p.millis(), values);
    }

    /** The point of an event: its values, and the time channel for the {@code ordinal}-th point of the path. */
    private double[] point(final Event e, final long ordinal) {
        if (!timeAugment) return e.values();
        final double[] point = java.util.Arrays.copyOf(e.values(), channels);
        point[channels - 1] = byEvents ? ordinal : calendar != null ? calendar.ordinal(e.millis()) : e.millis() / 86_400_000d;
        return point;
    }

    @Override
    public State create() {
        return new State();
    }

    @Override
    public boolean invertible() {
        return false;
    }

    @Override
    public void update(final State s, final Object contribution, final int sign) {
        if (sign < 0) throw new UnsupportedOperationException("a signature state cannot evict its oldest event (the increment after it is not kept)");
        final double[] point = point((Event) contribution, s.points);
        if (s.points == 0) {
            s.first = point;
            s.tensor = identity();
        } else {
            s.tensor = multiply(s.tensor, exp(difference(point, s.last)));
        }
        s.last = point;
        s.points++;
        s.logarithm = null;
    }

    /** {@code into ← into ⊕ other}, {@code other} holding the later events: the paths joined by the increment between them (Chen). */
    @Override
    public void merge(final State into, final State other) {
        if (other.points == 0) return;
        into.logarithm = null;
        if (into.points == 0) {
            into.points = other.points;
            into.first = other.first.clone();
            into.last = other.last.clone();
            into.tensor = other.tensor.clone();
            return;
        }
        // on the events clock the later path's time channel continues the earlier one's count
        final double[] shift = new double[channels];
        if (timeAugment && byEvents) shift[channels - 1] = into.points;
        final double[] otherFirst = add(other.first, shift);
        into.tensor = multiply(multiply(into.tensor, exp(difference(otherFirst, into.last))), other.tensor);
        into.last = add(other.last, shift);
        into.points += other.points;
    }

    @Override
    public double count(final State s) {
        return s.points;
    }

    /**
     * The coefficient of the readout's Lyndon word in {@code log S}; null without an increment (fewer than two points).
     * The logarithm is memoised on the state: the window's word columns share one state, so they share one {@code log}.
     */
    @Override
    public Object read(final State s, final Readout readout) {
        if (s.points < 2) return null;
        if (s.logarithm == null) s.logarithm = log(s.tensor);
        final double value = s.logarithm[index(words.get(readout.parameter().intValue()))];
        return Double.isFinite(value) ? value : null;
    }

    /** The direct computation over a window (the scan path): the same fold over its points, in time order. */
    public Object project(final List<SequenceEvaluator.Past> window, final int word) {
        final State s = create();
        for (final SequenceEvaluator.Past p : window) {
            final Event e = event(p);
            if (e != null) update(s, e, 1);
        }
        return read(s, Readout.of("word", word));
    }

    // ------------------------------------------------------------------------------------------
    // truncated tensor algebra
    // ------------------------------------------------------------------------------------------

    double[] identity() {
        final double[] t = new double[size];
        t[0] = 1;
        return t;
    }

    /** The flat index of a word (a tensor coordinate: level = word length, digits in base channels). */
    int index(final int[] word) {
        int i = 0;
        for (final int letter : word) i = i * channels + letter;
        return offsets[word.length] + i;
    }

    /** {@code exp(v) = Σ v^{⊗k} / k!} of a level-1 element. */
    double[] exp(final double[] v) {
        final double[] t = identity();
        double[] level = new double[]{1};
        for (int k = 1; k <= depth; k++) {
            final double[] next = new double[level.length * channels];
            for (int i = 0; i < level.length; i++) {
                for (int j = 0; j < channels; j++) next[i * channels + j] = level[i] * v[j] / k;
            }
            level = next;
            System.arraycopy(level, 0, t, offsets[k], level.length);
        }
        return t;
    }

    /** The product in the truncated tensor algebra: level k of {@code a ⊗ b} = Σ_i a_i ⊗ b_{k−i}. */
    double[] multiply(final double[] a, final double[] b) {
        final double[] c = new double[size];
        for (int k = 0; k <= depth; k++) {
            for (int i = 0; i <= k; i++) {
                final int j = k - i;
                final int la = powers[i], lb = powers[j];
                for (int x = 0; x < la; x++) {
                    final double ax = a[offsets[i] + x];
                    if (ax == 0) continue;
                    for (int y = 0; y < lb; y++) c[offsets[k] + x * lb + y] += ax * b[offsets[j] + y];
                }
            }
        }
        return c;
    }

    /** The inverse of a group-like element (scalar part 1): {@code Σ_m (1 − a)^m} — the signature of the reversed path. */
    double[] inverse(final double[] a) {
        final double[] minusT = new double[size];
        for (int i = 1; i < size; i++) minusT[i] = -a[i];
        double[] result = identity();
        double[] power = identity();
        for (int m = 1; m <= depth; m++) {
            power = multiply(power, minusT);
            for (int i = 0; i < size; i++) result[i] += power[i];
        }
        return result;
    }

    /** {@code log(1 + T) = Σ_{m ≥ 1} (−1)^{m+1} T^m / m}, T nilpotent beyond the depth. */
    double[] log(final double[] a) {
        final double[] t = a.clone();
        t[0] = 0;
        final double[] result = new double[size];
        double[] power = identity();
        for (int m = 1; m <= depth; m++) {
            power = multiply(power, t);
            final double factor = (m % 2 == 1 ? 1d : -1d) / m;
            for (int i = 0; i < size; i++) result[i] += factor * power[i];
        }
        return result;
    }

    private static double[] difference(final double[] a, final double[] b) {
        final double[] d = new double[a.length];
        for (int i = 0; i < a.length; i++) d[i] = a[i] - b[i];
        return d;
    }

    private static double[] add(final double[] a, final double[] b) {
        final double[] d = new double[a.length];
        for (int i = 0; i < a.length; i++) d[i] = a[i] + b[i];
        return d;
    }

    /** The Lyndon words over {@code 0..letters−1} of length 1..maxLength, by length then lexicographically (Duval). */
    public static List<int[]> lyndonWords(final int letters, final int maxLength) {
        final List<int[]> all = new ArrayList<>();
        if (letters < 1) return all;
        final int[] w = new int[maxLength];
        int length = 1;
        w[0] = 0;
        while (length > 0) {
            all.add(java.util.Arrays.copyOf(w, length));
            // Duval: repeat the word to maxLength, then increment the last letter that can be
            for (int i = length; i < maxLength; i++) w[i] = w[i - length];
            length = maxLength;
            while (length > 0 && w[length - 1] == letters - 1) length--;
            if (length > 0) w[length - 1]++;
        }
        all.sort((a, b) -> a.length != b.length ? Integer.compare(a.length, b.length) : java.util.Arrays.compare(a, b));
        return all;
    }

    /** The name of a word: channel letters {@code a, b, c, …} ({@code ab} = the Lévy area of channels a and b). */
    public static String wordName(final int[] word) {
        final StringBuilder sb = new StringBuilder();
        for (final int letter : word) sb.append((char) ('a' + letter));
        return sb.toString();
    }
}
