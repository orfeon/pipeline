package com.mercari.solution.util.pipeline.udf;

import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.Function;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.impl.AggregateFunctionImpl;

import java.util.List;
import java.util.Map;

/**
 * {@code DECAY_SUM} / {@code DECAY_AVG} / {@code DECAY_COUNT} —
 * exponentially time-decayed aggregates, always registered as built-ins. The
 * streaming-feature staple ("recent behaviour weighs more") over a buffer
 * table's history, with the reference time passed <b>explicitly</b> so the
 * functions work identically over any source (and inside re-evaluated,
 * order-independent windows):
 *
 * <pre>{@code
 * SELECT i.userId,
 *        DECAY_SUM(h.amount, h.__timestamp, 3600000, i.__timestamp) AS ewmaAmount
 * FROM INPUT i JOIN buffer.history h ON h.userId = i.userId
 * GROUP BY i.userId, i.__timestamp
 * }</pre>
 *
 * <p>Each row's weight is {@code 2^(-(asOf - ts) / halfLife)}: a row exactly
 * {@code halfLife} old counts half, a row at {@code asOf} counts fully (a row
 * <em>after</em> {@code asOf} weighs more than 1 — pre-filter if that is not
 * wanted). {@code ts} / {@code asOf} are TIMESTAMPs (internally epoch millis)
 * or epoch-millis numbers; {@code halfLife} is a positive width in millis
 * (day-time {@code INTERVAL} literals work). Rows with a NULL value or NULL
 * {@code ts} are skipped. All three are mergeable (weights are additive for
 * a fixed {@code asOf}).
 *
 * <ul>
 * <li><b>{@code DECAY_SUM(value, ts, halfLife, asOf)}</b> → DOUBLE:
 *     {@code Σ value·w}; NULL over an empty/all-NULL group.</li>
 * <li><b>{@code DECAY_AVG(value, ts, halfLife, asOf)}</b> → DOUBLE: the
 *     weighted mean {@code Σ value·w / Σ w}; NULL over an empty/all-NULL
 *     group.</li>
 * <li><b>{@code DECAY_COUNT(ts, halfLife, asOf)}</b> → DOUBLE: {@code Σ w} —
 *     the decayed event count (an event-rate feature); NULL over an
 *     empty/all-NULL group like the others (Calcite's strict user-aggregate
 *     convention — wrap in {@code COALESCE(…, 0.0)} for COUNT
 *     semantics).</li>
 * </ul>
 */
public class DecayFunctions {

    private DecayFunctions() {
    }

    /** The mutable accumulator; public because generated code manipulates it. */
    public static class Acc {
        public double weightedSum;
        public double weightTotal;
        public boolean any;
    }

    static double weight(String functionName, Object ts, Object halfLife, Object asOf) {
        if (!(halfLife instanceof Number width)) {
            throw new IllegalArgumentException(functionName + " halfLife must be a millis"
                    + " number or a day-time INTERVAL, but got "
                    + (halfLife == null ? "NULL" : halfLife.getClass().getSimpleName()));
        }
        final double half = width.doubleValue();
        if (half <= 0) {
            throw new IllegalArgumentException(
                    functionName + " halfLife must be positive, but was " + half);
        }
        if (!(asOf instanceof Number reference)) {
            throw new IllegalArgumentException(functionName + " asOf must be a TIMESTAMP or"
                    + " epoch-millis number, but got "
                    + (asOf == null ? "NULL" : asOf.getClass().getSimpleName()));
        }
        if (!(ts instanceof Number time)) {
            throw new IllegalArgumentException(functionName + " ts must be a TIMESTAMP or"
                    + " epoch-millis number, but got " + ts.getClass().getSimpleName());
        }
        return Math.pow(2d, -(reference.doubleValue() - time.doubleValue()) / half);
    }

    private static double value(String functionName, Object value) {
        if (!(value instanceof Number number)) {
            throw new IllegalArgumentException(functionName + " requires numeric values,"
                    + " but got " + value.getClass().getSimpleName());
        }
        return number.doubleValue();
    }

    static Acc mergeAcc(Acc a, Acc b) {
        a.weightedSum += b.weightedSum;
        a.weightTotal += b.weightTotal;
        a.any |= b.any;
        return a;
    }

    /** {@code DECAY_SUM(value, ts, halfLife, asOf)} accumulator host. */
    public static class SumHost {

        public Acc init() {
            return new Acc();
        }

        public Acc add(Acc acc, Object value, Object ts, Object halfLife, Object asOf) {
            if (value == null || ts == null) {
                return acc;
            }
            acc.weightedSum += value("DECAY_SUM", value)
                    * weight("DECAY_SUM", ts, halfLife, asOf);
            acc.any = true;
            return acc;
        }

        public Acc merge(Acc a, Acc b) {
            return mergeAcc(a, b);
        }

        public Double result(Acc acc) {
            return acc.any ? acc.weightedSum : null;
        }
    }

    /** {@code DECAY_AVG(value, ts, halfLife, asOf)} accumulator host. */
    public static class AvgHost {

        public Acc init() {
            return new Acc();
        }

        public Acc add(Acc acc, Object value, Object ts, Object halfLife, Object asOf) {
            if (value == null || ts == null) {
                return acc;
            }
            final double w = weight("DECAY_AVG", ts, halfLife, asOf);
            acc.weightedSum += value("DECAY_AVG", value) * w;
            acc.weightTotal += w;
            acc.any = true;
            return acc;
        }

        public Acc merge(Acc a, Acc b) {
            return mergeAcc(a, b);
        }

        public Double result(Acc acc) {
            if (!acc.any || acc.weightTotal == 0d) {
                return null;
            }
            return acc.weightedSum / acc.weightTotal;
        }
    }

    /** {@code DECAY_COUNT(ts, halfLife, asOf)} accumulator host. */
    public static class CountHost {

        public Acc init() {
            return new Acc();
        }

        public Acc add(Acc acc, Object ts, Object halfLife, Object asOf) {
            if (ts == null) {
                return acc;
            }
            acc.weightTotal += weight("DECAY_COUNT", ts, halfLife, asOf);
            acc.any = true;
            return acc;
        }

        public Acc merge(Acc a, Acc b) {
            return mergeAcc(a, b);
        }

        public Double result(Acc acc) {
            return acc.weightTotal;
        }
    }

    /** The Calcite function objects, name → definition. */
    static List<Map.Entry<String, Function>> builtIns() {
        return List.of(
                Map.entry("DECAY_SUM", aggregate("DECAY_SUM", SumHost.class)),
                Map.entry("DECAY_AVG", aggregate("DECAY_AVG", AvgHost.class)),
                Map.entry("DECAY_COUNT", aggregate("DECAY_COUNT", CountHost.class)));
    }

    private static Function aggregate(String name, Class<?> host) {
        final Function function = AggregateFunctionImpl.create(host);
        if (function == null) {
            throw new IllegalStateException(
                    name + " host does not match the aggregate convention");
        }
        return function;
    }
}
