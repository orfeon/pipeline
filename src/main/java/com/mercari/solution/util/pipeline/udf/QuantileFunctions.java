package com.mercari.solution.util.pipeline.udf;

import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.Function;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.impl.AggregateFunctionImpl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * {@code QUANTILE} — an exact quantile aggregate with linear interpolation
 * ({@code PERCENTILE_CONT} semantics, ClickHouse {@code quantileExact}
 * territory), always registered as a built-in:
 *
 * <pre>{@code
 * SELECT userId, QUANTILE(latency, 0.95) AS p95
 * FROM input GROUP BY userId
 * }</pre>
 *
 * <p>{@code QUANTILE(value, fraction)} → {@code DOUBLE}: {@code fraction} is a
 * constant in {@code [0, 1]} (0.5 = median). NULL values are skipped; an empty
 * group (or all NULLs) returns NULL. The computation is <b>exact</b>, holding
 * the group's values in memory — the natural fit for per-element evaluation,
 * where groups are bounded by one input element's rows (fan-out included).
 */
public class QuantileFunctions {

    private QuantileFunctions() {
    }

    /** The mutable accumulator; public because generated code manipulates it. */
    public static class Acc {
        public final List<Double> values = new ArrayList<>();
        public double fraction = Double.NaN;
    }

    /** The accumulator host. Arguments are ANY-typed; numbers are required. */
    public static class Host {
        public Acc init() {
            return new Acc();
        }

        public Acc add(Acc acc, Object value, Object fraction) {
            if (!(fraction instanceof Number f)) {
                throw new IllegalArgumentException("QUANTILE fraction must be a number in"
                        + " [0, 1], but got " + (fraction == null
                                ? "NULL" : fraction.getClass().getSimpleName()));
            }
            double p = f.doubleValue();
            if (p < 0 || p > 1) {
                throw new IllegalArgumentException(
                        "QUANTILE fraction must be in [0, 1], but was " + p);
            }
            acc.fraction = p;
            if (value == null) {
                return acc;
            }
            if (!(value instanceof Number number)) {
                throw new IllegalArgumentException("QUANTILE requires numeric values, but got "
                        + value.getClass().getSimpleName());
            }
            acc.values.add(number.doubleValue());
            return acc;
        }

        public Acc merge(Acc a, Acc b) {
            a.values.addAll(b.values);
            if (Double.isNaN(a.fraction)) {
                a.fraction = b.fraction;
            }
            return a;
        }

        public Double result(Acc acc) {
            if (acc.values.isEmpty()) {
                return null;
            }
            Collections.sort(acc.values);
            double h = (acc.values.size() - 1) * acc.fraction;
            int lower = (int) Math.floor(h);
            int upper = (int) Math.ceil(h);
            double lowerValue = acc.values.get(lower);
            return lower == upper
                    ? lowerValue
                    : lowerValue + (acc.values.get(upper) - lowerValue) * (h - lower);
        }
    }

    /** The Calcite function object. */
    static List<Map.Entry<String, Function>> builtIns() {
        Function function = AggregateFunctionImpl.create(Host.class);
        if (function == null) {
            throw new IllegalStateException(
                    "QUANTILE host does not match the aggregate convention");
        }
        return List.of(Map.entry("QUANTILE", function));
    }
}
