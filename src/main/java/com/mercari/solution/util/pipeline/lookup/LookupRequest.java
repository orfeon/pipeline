package com.mercari.solution.util.pipeline.lookup;

import java.util.List;

/**
 * One left (input) row's lookup need against an external source's key.
 *
 * <p>Two shapes, expressed against a contiguous prefix of the target's key
 * columns (in key order), using Calcite-internal value types (String, Integer,
 * Long, ...):
 * <ul>
 *   <li><b>Point</b>: {@link #prefix} holds equality values for <em>all</em>
 *       key columns; no range.</li>
 *   <li><b>Range</b>: {@link #prefix} holds equality values for the leading key
 *       columns, and {@link #lower}/{@link #upper} bound the next key column.</li>
 * </ul>
 */
public final class LookupRequest {

    private final List<Object> prefix;
    private final Object lower;
    private final Object upper;
    private final boolean lowerInclusive;
    private final boolean upperInclusive;
    private final boolean range;

    private LookupRequest(List<Object> prefix, Object lower, Object upper,
            boolean lowerInclusive, boolean upperInclusive, boolean range) {
        this.prefix = List.copyOf(prefix);
        this.lower = lower;
        this.upper = upper;
        this.lowerInclusive = lowerInclusive;
        this.upperInclusive = upperInclusive;
        this.range = range;
    }

    public static LookupRequest point(List<Object> keyValues) {
        return new LookupRequest(keyValues, null, null, false, false, false);
    }

    public static LookupRequest range(List<Object> prefixValues, Object lower,
            boolean lowerInclusive, Object upper, boolean upperInclusive) {
        return new LookupRequest(prefixValues, lower, upper,
                lowerInclusive, upperInclusive, true);
    }

    /** Equality values for the leading key columns (all key columns when point). */
    public List<Object> prefix() {
        return prefix;
    }

    public boolean isRange() {
        return range;
    }

    public Object lower() {
        return lower;
    }

    public Object upper() {
        return upper;
    }

    public boolean lowerInclusive() {
        return lowerInclusive;
    }

    public boolean upperInclusive() {
        return upperInclusive;
    }
}
