package com.mercari.solution.util.pipeline.feature;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * A mergeable summary of a stream of contributions — the typed accumulator behind every statistic the engine
 * can serve without re-reading the rows. One family, one state class; the same implementation is meant to
 * back (1) the incremental path of the keyed replay ({@link SequenceEvaluator}: fold a contribution in when its
 * row becomes visible, evict it when it leaves a {@code maxAge} window), (2) a per-block {@code Combine} of a
 * fit stage, (3) the prefix-scan of a hot key and (4) the state of a streaming keyed stage — so a statistic is
 * implemented once and its algebraic properties decide where it may run:
 *
 * <ul>
 *   <li>every family is a <b>monoid</b>: {@link #merge} is associative with {@link #create} as the identity, so
 *       summaries of disjoint row sets combine in any order (blocks, partitions, folds);</li>
 *   <li>an <b>{@link #invertible}</b> family is a group: a contribution can be removed again, which is what lets a
 *       window evict in O(1) per row instead of rescanning. Families that only know how to grow (extrema) stay
 *       incremental over an unbounded past and fall back to the scan path under a window.</li>
 * </ul>
 *
 * <p>The evaluator owns the <em>extraction</em> (which value of a past row contributes — target minus baseline,
 * a category, a bare 1 for a count) and the null convention of its scope; the summary owns the arithmetic.
 * {@link OperatorCatalog#summary} maps a statistic token to its family and {@link Readout}; {@link Summaries}
 * holds the built-in families.
 *
 * @param <S> the state (serializable so it can travel as a Combine accumulator or a stateful DoFn value)
 */
public interface Summary<S extends Serializable> extends Serializable {

    /** The empty state (the monoid identity). */
    S create();

    /**
     * Folds one contribution in ({@code sign = +1}) or, for an {@link #invertible} family, out ({@code sign = −1}).
     * The contribution is never null (the evaluator skips missing values before calling).
     *
     * @throws UnsupportedOperationException on removal from a family that is not invertible
     */
    void update(S state, Object contribution, int sign);

    /** Whether {@link #update} accepts {@code sign = −1} (the family is a group, so a window can evict). */
    boolean invertible();

    /** {@code into ← into ⊕ other} (associative; {@code other} is not modified). */
    void merge(S into, S other);

    /** Reads a statistic from the state; null when the state cannot answer it (too few contributions). */
    Object read(S state, Readout readout);

    /** Number of contributions currently summarised. */
    double count(S state);

    /**
     * What to read from a summary: the statistic's name plus its numeric parameter when it has one (the
     * probability of a quantile). Resolved once per column at setup, never per row.
     */
    record Readout(String name, Double parameter) implements Serializable {
        public static Readout of(final String name) {
            return new Readout(name, null);
        }

        public static Readout of(final String name, final double parameter) {
            return new Readout(name, parameter);
        }
    }

    /** A family bound to a readout: what a column runs on the incremental path. */
    record Spec(Summary<?> family, Readout readout) implements Serializable {
        @SuppressWarnings("unchecked")
        <S extends Serializable> Summary<S> typed() {
            return (Summary<S>) family;
        }
    }

    // ------------------------------------------------------------------------------------------
    // built-in families
    // ------------------------------------------------------------------------------------------

    /** The built-in families; each is a stateless singleton (the state lives in the {@code S} instances). */
    final class Summaries {
        private Summaries() {}

        /** Power sums up to order two: count / sum / mean / std. Invertible. */
        public static final Summary<Moments.State> MOMENTS = new Moments();
        /** Running maximum and minimum. Not invertible (a removed extreme cannot be recovered). */
        public static final Summary<Extrema.State> EXTREMA = new Extrema();
        /** Count per distinct value (the string form): a value distribution. Invertible. */
        public static final Summary<Counts.State> COUNTS = new Counts();
        /** Exact order statistics (a multiset with deletion): quantiles. Invertible. */
        public static final Summary<Order.State> ORDER = new Order();
    }

    /** (n, Σx, Σx²) — the sufficient statistics of a mean and a variance. */
    final class Moments implements Summary<Moments.State> {
        public static final class State implements Serializable {
            public double n, sum, sumSq;
        }

        @Override
        public State create() {
            return new State();
        }

        @Override
        public void update(final State s, final Object contribution, final int sign) {
            final double v = ((Number) contribution).doubleValue();
            s.n += sign;
            s.sum += sign * v;
            s.sumSq += sign * v * v;
        }

        @Override
        public boolean invertible() {
            return true;
        }

        @Override
        public void merge(final State into, final State other) {
            into.n += other.n;
            into.sum += other.sum;
            into.sumSq += other.sumSq;
        }

        @Override
        public double count(final State s) {
            return s.n;
        }

        /** count → long; sum → null when empty; mean / avg / rate → null when empty; std → null below two (population std). */
        @Override
        public Object read(final State s, final Readout readout) {
            return switch (readout.name()) {
                case "count" -> (long) s.n;
                case "sum" -> s.n == 0 ? null : s.sum;
                case "mean", "avg", "rate" -> s.n == 0 ? null : s.sum / s.n;
                case "std" -> {
                    if (s.n < 2) yield null;
                    final double mean = s.sum / s.n;
                    yield Math.sqrt(Math.max(0, s.sumSq / s.n - mean * mean));
                }
                default -> throw new IllegalArgumentException("moments cannot read " + readout.name());
            };
        }
    }

    /** Running max / min of a numeric contribution. */
    final class Extrema implements Summary<Extrema.State> {
        public static final class State implements Serializable {
            public double n;
            public Double max, min;
        }

        @Override
        public State create() {
            return new State();
        }

        @Override
        public void update(final State s, final Object contribution, final int sign) {
            if (sign < 0) throw new UnsupportedOperationException("extrema are not invertible");
            final double v = ((Number) contribution).doubleValue();
            s.n += sign;
            if (s.max == null || v > s.max) s.max = v;
            if (s.min == null || v < s.min) s.min = v;
        }

        @Override
        public boolean invertible() {
            return false;
        }

        @Override
        public void merge(final State into, final State other) {
            into.n += other.n;
            if (other.max != null && (into.max == null || other.max > into.max)) into.max = other.max;
            if (other.min != null && (into.min == null || other.min < into.min)) into.min = other.min;
        }

        @Override
        public double count(final State s) {
            return s.n;
        }

        @Override
        public Object read(final State s, final Readout readout) {
            return switch (readout.name()) {
                case "max" -> s.max;
                case "min" -> s.min;
                case "count" -> (long) s.n;
                default -> throw new IllegalArgumentException("extrema cannot read " + readout.name());
            };
        }
    }

    /** Count per distinct value; {@code distribution} reads the shares of the values with a positive count. */
    final class Counts implements Summary<Counts.State> {
        public static final class State implements Serializable {
            public double n;
            public final TreeMap<String, Long> counts = new TreeMap<>();
        }

        @Override
        public State create() {
            return new State();
        }

        @Override
        public void update(final State s, final Object contribution, final int sign) {
            s.n += sign;
            s.counts.merge(contribution.toString(), (long) sign, Long::sum);
        }

        @Override
        public boolean invertible() {
            return true;
        }

        @Override
        public void merge(final State into, final State other) {
            into.n += other.n;
            for (final Map.Entry<String, Long> e : other.counts.entrySet()) into.counts.merge(e.getKey(), e.getValue(), Long::sum);
        }

        @Override
        public double count(final State s) {
            return s.n;
        }

        @Override
        public Object read(final State s, final Readout readout) {
            return switch (readout.name()) {
                case "count" -> (long) s.n;
                case "distribution" -> {
                    if (s.n == 0) yield null;
                    final Map<String, Object> dist = new LinkedHashMap<>();
                    for (final Map.Entry<String, Long> e : s.counts.entrySet()) {
                        if (e.getValue() > 0) dist.put(e.getKey(), e.getValue() / s.n);
                    }
                    yield dist;
                }
                default -> throw new IllegalArgumentException("counts cannot read " + readout.name());
            };
        }
    }

    /** Exact quantiles over a multiset with deletion ({@link OrderStatistics}). */
    final class Order implements Summary<Order.State> {
        public static final class State implements Serializable {
            public final OrderStatistics order = new OrderStatistics();
        }

        @Override
        public State create() {
            return new State();
        }

        @Override
        public void update(final State s, final Object contribution, final int sign) {
            final double v = ((Number) contribution).doubleValue();
            if (sign > 0) s.order.add(v);
            else s.order.remove(v);
        }

        @Override
        public boolean invertible() {
            return true;
        }

        @Override
        public void merge(final State into, final State other) {
            for (int i = 0; i < other.order.size(); i++) into.order.add(other.order.select(i));
        }

        @Override
        public double count(final State s) {
            return s.order.size();
        }

        @Override
        public Object read(final State s, final Readout readout) {
            return switch (readout.name()) {
                case "count" -> (long) s.order.size();
                case "quantile" -> s.order.size() == 0 ? null : s.order.quantile(readout.parameter());
                default -> throw new IllegalArgumentException("order statistics cannot read " + readout.name());
            };
        }
    }

}
