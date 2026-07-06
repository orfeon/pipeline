package com.mercari.solution.util.pipeline.udf;


import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.Function;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.impl.AggregateFunctionImpl;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * {@code SEQ_COLLECT} — an <em>ordered</em> collection aggregate, always
 * registered as a built-in. Collects one row per aggregated input row and
 * returns them <b>sorted by the first argument</b> (the sequence key), which
 * makes it order-independent: unlike {@code COLLECT} (unordered multiset) or
 * {@code ARRAY_AGG} (whose ordered form cannot round-trip through a LATERAL
 * block's generated SQL on this stack), the result is a deterministic
 * sequence regardless of the rows' arrival order — and the same SQL works on
 * the origin engine, which has no {@code ARRAY_AGG} at all. This is what
 * turns <em>lookup-table history</em> into a SEQ_MATCH sequence in one query:
 *
 * <pre>{@code
 * SELECT e.userId,
 *        CARDINALITY(SEQ_MATCH(
 *          SEQ_COLLECT(h.TS, h.ACTION, h.AMOUNT),   -- rows sorted by TS
 *          'ts,action,amount',                      -- key first, values after
 *          'PROMO (SKIP* BUY){3}', '...')) > 0 AS converted
 * FROM input.events e
 * JOIN db.HISTORY h ON h.USER_ID = e.userId          -- prefix-only lookup fan-out
 * GROUP BY e.userId
 * }</pre>
 *
 * <p>Arity overloads take the sort key plus 0–6 value columns; each collected
 * row is {@code [key, v1, ..., vn]}, so the key is field 0 (name it first in
 * SEQ_MATCH's field list; with no value columns the result is a scalar
 * sequence — reference {@code $0}). Rows with a NULL key sort last. The key
 * must be {@link Comparable} (every Calcite-internal scalar is).
 *
 * <p><b>The result is an opaque collection</b> (a Java {@code List}, not a SQL
 * {@code ARRAY<ROW>}): consume it with {@code SEQ_MATCH} /
 * {@code SEQ_MATCH_STEPS} (whose array argument is ANY-typed) or
 * {@code SEQ_FOLD} using the 0-based <em>ordinal</em> field spec (the name
 * resolution needs a typed array). It cannot be {@code UNNEST}ed or projected
 * into the result schema — extract values via {@code SEQ_FOLD} over match
 * ranges instead.
 */
public class SequenceCollectFunctions {

    private SequenceCollectFunctions() {
    }

    /** The Calcite function objects, name → arity overloads. */
    static List<Map.Entry<String, Function>> builtIns() {
        List<Map.Entry<String, Function>> entries = new ArrayList<>();
        for (Class<?> host : List.of(V0.class, V1.class, V2.class, V3.class,
                V4.class, V5.class, V6.class)) {
            Function function = AggregateFunctionImpl.create(host);
            if (function == null) {
                throw new IllegalArgumentException(
                        "SEQ_COLLECT host does not match the aggregate convention: " + host);
            }
            entries.add(Map.entry("SEQ_COLLECT", function));
        }
        return List.copyOf(entries);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static List<Object[]> sortByKey(List<Object[]> rows) {
        rows.sort(Comparator.comparing(row -> (Comparable) row[0],
                Comparator.nullsLast(Comparator.naturalOrder())));
        return rows;
    }

    static List<Object[]> concat(List<Object[]> a, List<Object[]> b) {
        List<Object[]> out = new ArrayList<>(a.size() + b.size());
        out.addAll(a);
        out.addAll(b);
        return out;
    }

    /** {@code SEQ_COLLECT(key)} — a scalar sequence of the keys themselves. */
    public static class V0 {
        public List<Object[]> init() {
            return new ArrayList<>();
        }

        public List<Object[]> add(List<Object[]> acc, Object key) {
            acc.add(new Object[]{key});
            return acc;
        }

        public List<Object[]> merge(List<Object[]> a, List<Object[]> b) {
            return concat(a, b);
        }

        public List<Object[]> result(List<Object[]> acc) {
            return sortByKey(acc);
        }
    }

    /** {@code SEQ_COLLECT(key, v1)}. */
    public static class V1 {
        public List<Object[]> init() {
            return new ArrayList<>();
        }

        public List<Object[]> add(List<Object[]> acc, Object key, Object v1) {
            acc.add(new Object[]{key, v1});
            return acc;
        }

        public List<Object[]> merge(List<Object[]> a, List<Object[]> b) {
            return concat(a, b);
        }

        public List<Object[]> result(List<Object[]> acc) {
            return sortByKey(acc);
        }
    }

    /** {@code SEQ_COLLECT(key, v1, v2)}. */
    public static class V2 {
        public List<Object[]> init() {
            return new ArrayList<>();
        }

        public List<Object[]> add(List<Object[]> acc, Object key, Object v1, Object v2) {
            acc.add(new Object[]{key, v1, v2});
            return acc;
        }

        public List<Object[]> merge(List<Object[]> a, List<Object[]> b) {
            return concat(a, b);
        }

        public List<Object[]> result(List<Object[]> acc) {
            return sortByKey(acc);
        }
    }

    /** {@code SEQ_COLLECT(key, v1, v2, v3)}. */
    public static class V3 {
        public List<Object[]> init() {
            return new ArrayList<>();
        }

        public List<Object[]> add(List<Object[]> acc, Object key, Object v1, Object v2,
                Object v3) {
            acc.add(new Object[]{key, v1, v2, v3});
            return acc;
        }

        public List<Object[]> merge(List<Object[]> a, List<Object[]> b) {
            return concat(a, b);
        }

        public List<Object[]> result(List<Object[]> acc) {
            return sortByKey(acc);
        }
    }

    /** {@code SEQ_COLLECT(key, v1, v2, v3, v4)}. */
    public static class V4 {
        public List<Object[]> init() {
            return new ArrayList<>();
        }

        public List<Object[]> add(List<Object[]> acc, Object key, Object v1, Object v2,
                Object v3, Object v4) {
            acc.add(new Object[]{key, v1, v2, v3, v4});
            return acc;
        }

        public List<Object[]> merge(List<Object[]> a, List<Object[]> b) {
            return concat(a, b);
        }

        public List<Object[]> result(List<Object[]> acc) {
            return sortByKey(acc);
        }
    }

    /** {@code SEQ_COLLECT(key, v1, v2, v3, v4, v5)}. */
    public static class V5 {
        public List<Object[]> init() {
            return new ArrayList<>();
        }

        public List<Object[]> add(List<Object[]> acc, Object key, Object v1, Object v2,
                Object v3, Object v4, Object v5) {
            acc.add(new Object[]{key, v1, v2, v3, v4, v5});
            return acc;
        }

        public List<Object[]> merge(List<Object[]> a, List<Object[]> b) {
            return concat(a, b);
        }

        public List<Object[]> result(List<Object[]> acc) {
            return sortByKey(acc);
        }
    }

    /** {@code SEQ_COLLECT(key, v1, v2, v3, v4, v5, v6)}. */
    public static class V6 {
        public List<Object[]> init() {
            return new ArrayList<>();
        }

        public List<Object[]> add(List<Object[]> acc, Object key, Object v1, Object v2,
                Object v3, Object v4, Object v5, Object v6) {
            acc.add(new Object[]{key, v1, v2, v3, v4, v5, v6});
            return acc;
        }

        public List<Object[]> merge(List<Object[]> a, List<Object[]> b) {
            return concat(a, b);
        }

        public List<Object[]> result(List<Object[]> acc) {
            return sortByKey(acc);
        }
    }
}
