package com.mercari.solution.util.pipeline.lookup;

import java.util.ArrayList;
import java.util.List;

/**
 * Shared driver for lookup sources whose backend <em>cannot array-bind</em> a key
 * set and must issue one call per distinct key — e.g. REST (one HTTP request per
 * key tuple). It factors out the common loop: reject range requests, iterate the
 * batch's <em>distinct</em> key tuples (skipping any with a null component, which
 * can never match), invoke the backend once per tuple, and collect the resulting
 * rows.
 *
 * <p>It does <b>not</b> apply to sources that array-bind a whole batch in one call
 * (JDBC's OR-of-tuples, Spanner's {@code read(KeySet)}, Bigtable's multi-range
 * {@code readRows}) — those keep their own batch-shaped {@code lookup()}.
 *
 * <p>A single key may yield zero, one, or many rows (fan-out); every produced row
 * is emitted and the lookup-join operator matches it back to its input row by key.
 */
public final class PerKeyLookup {

    /** Produces the backend row-sources for one distinct key tuple (in key order). */
    @FunctionalInterface
    public interface KeyCall<S> {
        Iterable<S> rowSources(List<Object> keyValues);
    }

    /** Turns one backend row-source into a result row, given its key tuple. */
    @FunctionalInterface
    public interface RowDecoder<S> {
        Object[] decode(S rowSource, List<Object> keyValues);
    }

    private PerKeyLookup() {
    }

    public static <S> Iterable<Object[]> run(LookupBatch batch, String tableLabel,
            KeyCall<S> call, RowDecoder<S> decoder) {
        if (batch.isRange()) {
            throw new IllegalStateException(tableLabel
                    + " supports only point equality on its key columns");
        }
        List<Object[]> rows = new ArrayList<>();
        for (List<Object> keyValues : batch.distinctPrefixes()) {
            if (containsNull(keyValues)) {
                continue;
            }
            for (S source : call.rowSources(keyValues)) {
                rows.add(decoder.decode(source, keyValues));
            }
        }
        return rows;
    }

    /** Convenience form for backends that decode their own rows. */
    public static Iterable<Object[]> run(LookupBatch batch, String tableLabel,
            KeyCall<Object[]> call) {
        return run(batch, tableLabel, call, (row, keyValues) -> row);
    }

    private static boolean containsNull(List<Object> values) {
        for (Object value : values) {
            if (value == null) {
                return true;
            }
        }
        return false;
    }
}
