package com.mercari.solution.util.pipeline.lookup;

import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.linq4j.Enumerable;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.linq4j.Enumerator;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.linq4j.Linq4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Runtime for the correlated-LATERAL lookup join. Like
 * {@link LookupJoinEnumerable} it batches left rows, derives one
 * {@link LookupRequest} per row from the pre-appended key/bound columns, and
 * fetches all matching leaf rows in one {@link LookupSource#lookup} call per
 * batch. Then, instead of joining leaf rows back directly, it evaluates the
 * LATERAL block's inner plan (via {@link LookupLateralRuntime}) over each
 * <em>distinct request's</em> row set — so aggregation, ORDER BY / LIMIT and
 * DISTINCT inside the block run per key, in-memory, inside the DoFn — and joins
 * the inner result rows to the originating left rows.
 *
 * <p>SQL semantics of {@code JOIN LATERAL ... ON TRUE}: the inner plan is
 * evaluated for <em>every</em> left row — a row whose key is null or matches
 * nothing evaluates against an empty set (a global aggregate then still yields
 * one row, e.g. {@code COUNT(*) = 0}). A left row is dropped only when the
 * inner result itself is empty (INNER), or padded with nulls (LEFT).
 */
public final class LookupLateralEnumerable extends AbstractEnumerable<Object[]> {

    private static final int DEFAULT_BATCH = 512;

    private final Enumerable<Object[]> left;
    private final LookupSource source;
    private final LookupLateralRuntime runtime;
    private final String table;
    private final String indexName;
    private final boolean range;
    private final int leftFieldCount;
    private final int prefixLength;
    private final int lowerOrdinal;
    private final int upperOrdinal;
    private final boolean lowerInclusive;
    private final boolean upperInclusive;
    private final int[] leafKeyPos;
    private final int innerFieldCount;
    private final boolean leftJoin;

    public LookupLateralEnumerable(Enumerable<Object[]> left, long sourceId, long runtimeId,
            String table, String indexName, boolean range, int leftFieldCount, int prefixLength,
            int lowerOrdinal, int upperOrdinal, boolean lowerInclusive, boolean upperInclusive,
            String leafKeyPos, int innerFieldCount, boolean leftJoin) {
        this.left = left;
        this.source = LookupSourceRegistry.get(sourceId);
        this.runtime = LookupLateralRuntime.get(runtimeId);
        this.table = table;
        this.indexName = indexName;
        this.range = range;
        this.leftFieldCount = leftFieldCount;
        this.prefixLength = prefixLength;
        this.lowerOrdinal = lowerOrdinal;
        this.upperOrdinal = upperOrdinal;
        this.lowerInclusive = lowerInclusive;
        this.upperInclusive = upperInclusive;
        this.leafKeyPos = parsePositions(leafKeyPos);
        this.innerFieldCount = innerFieldCount;
        this.leftJoin = leftJoin;
    }

    private static int[] parsePositions(String csv) {
        if (csv == null || csv.isEmpty()) {
            return new int[0];
        }
        final String[] parts = csv.split(",");
        final int[] out = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            out[i] = Integer.parseInt(parts[i].trim());
        }
        return out;
    }

    @Override
    public Enumerator<Object[]> enumerator() {
        final List<Object[]> output = new ArrayList<>();
        final List<Object[]> batch = new ArrayList<>(DEFAULT_BATCH);
        try (Enumerator<Object[]> leftRows = left.enumerator()) {
            while (leftRows.moveNext()) {
                batch.add(leftRows.current());
                if (batch.size() >= DEFAULT_BATCH) {
                    processBatch(batch, output);
                    batch.clear();
                }
            }
        }
        if (!batch.isEmpty()) {
            processBatch(batch, output);
        }
        return Linq4j.enumerator(output);
    }

    private void processBatch(List<Object[]> batch, List<Object[]> output) {
        // One request per left row (null when a key/bound value is absent);
        // fetch once per batch over the distinct requests.
        final List<LookupRequest> perRow = new ArrayList<>(batch.size());
        final Map<LookupRequest, List<Object[]>> matchedByRequest = new LinkedHashMap<>();
        for (final Object[] leftRow : batch) {
            final LookupRequest request = requestFor(leftRow);
            perRow.add(request);
            if (request != null) {
                matchedByRequest.putIfAbsent(request, new ArrayList<>());
            }
        }

        if (!matchedByRequest.isEmpty()) {
            final Iterable<Object[]> fetched = source.lookup(table, indexName,
                    LookupBatch.of(new ArrayList<>(matchedByRequest.keySet())), null);
            // Index leaf rows by key prefix, then assign each request its exact
            // (range-filtered) row set.
            final Map<List<Object>, List<Object[]>> byPrefix = new HashMap<>();
            for (final Object[] row : fetched) {
                final List<Object> prefixKey = new ArrayList<>(prefixLength);
                for (int i = 0; i < prefixLength; i++) {
                    prefixKey.add(row[leafKeyPos[i]]);
                }
                byPrefix.computeIfAbsent(prefixKey, k -> new ArrayList<>()).add(row);
            }
            for (final Map.Entry<LookupRequest, List<Object[]>> entry
                    : matchedByRequest.entrySet()) {
                final LookupRequest request = entry.getKey();
                final List<Object[]> candidates = byPrefix.get(request.prefix());
                if (candidates == null) {
                    continue;
                }
                for (final Object[] row : candidates) {
                    if (!range || inRange(row[leafKeyPos[prefixLength]], request)) {
                        entry.getValue().add(row);
                    }
                }
            }
        }

        // Evaluate the inner plan once per distinct request (and once for the
        // empty set, shared by all null-key rows), then join back.
        final Map<LookupRequest, List<Object[]>> innerByRequest = new HashMap<>();
        List<Object[]> innerForEmpty = null;
        for (int i = 0; i < batch.size(); i++) {
            final Object[] leftRow = batch.get(i);
            final LookupRequest request = perRow.get(i);
            final List<Object[]> innerRows;
            if (request == null) {
                if (innerForEmpty == null) {
                    innerForEmpty = runtime.evaluate(List.of());
                }
                innerRows = innerForEmpty;
            } else {
                innerRows = innerByRequest.computeIfAbsent(request,
                        r -> runtime.evaluate(matchedByRequest.get(r)));
            }
            if (innerRows.isEmpty()) {
                if (leftJoin) {
                    output.add(join(leftRow, null));
                }
            } else {
                for (final Object[] innerRow : innerRows) {
                    output.add(join(leftRow, innerRow));
                }
            }
        }
    }

    private LookupRequest requestFor(Object[] leftRow) {
        final List<Object> prefix = new ArrayList<>(prefixLength);
        for (int i = 0; i < prefixLength; i++) {
            final Object value = leftRow[leftFieldCount + i];
            if (value == null) {
                return null;
            }
            prefix.add(value);
        }
        if (!range) {
            return LookupRequest.point(prefix);
        }
        final Object lower = leftRow[lowerOrdinal];
        final Object upper = leftRow[upperOrdinal];
        if (lower == null || upper == null) {
            return null;
        }
        return LookupRequest.range(prefix, lower, lowerInclusive, upper, upperInclusive);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private boolean inRange(Object value, LookupRequest request) {
        if (value == null) {
            return false;
        }
        final Comparable cv = (Comparable) value;
        final int cl = cv.compareTo(request.lower());
        if (cl < 0 || (cl == 0 && !request.lowerInclusive())) {
            return false;
        }
        final int cu = cv.compareTo(request.upper());
        return cu < 0 || (cu == 0 && request.upperInclusive());
    }

    /** Builds an output row: original left columns followed by inner result columns. */
    private Object[] join(Object[] leftRow, Object[] innerRow) {
        final Object[] out = new Object[leftFieldCount + innerFieldCount];
        System.arraycopy(leftRow, 0, out, 0, leftFieldCount);
        if (innerRow != null) {
            System.arraycopy(innerRow, 0, out, leftFieldCount, innerFieldCount);
        }
        return out;
    }
}
