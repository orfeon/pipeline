package com.mercari.solution.util.pipeline.lookup;

import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.linq4j.Enumerable;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.linq4j.Enumerator;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.linq4j.Linq4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Source-agnostic runtime for the lookup-join. For each batch of left (input)
 * rows it derives {@link LookupRequest}s from pre-computed key/bound columns,
 * fetches rows from the {@link LookupSource} in one call, and joins them back to
 * the originating left rows by comparing Calcite-internal key values (point:
 * exact key-tuple match; range: prefix equality + range containment). Supports
 * INNER and LEFT joins.
 *
 * <p>Left rows arrive with the original left columns in {@code [0, leftFieldCount)}
 * followed by columns appended upstream by {@link LookupJoinRule}: the prefix key
 * values {@code [leftFieldCount, leftFieldCount+prefixLength)}, then (range only)
 * the lower and upper bound columns.
 *
 * <p>This class is instantiated from generated code ({@link LookupJoin#implement}),
 * so all constructor arguments are constants; position arrays travel as CSV strings.
 */
public final class LookupJoinEnumerable extends AbstractEnumerable<Object[]> {

    private static final int DEFAULT_BATCH = 512;

    private final Enumerable<Object[]> left;
    private final LookupSource source;
    private final String table;
    private final String indexName;
    private final boolean range;
    private final int leftFieldCount;
    private final int prefixLength;
    private final int lowerOrdinal;
    private final int upperOrdinal;
    private final boolean lowerInclusive;
    private final boolean upperInclusive;
    private final int[] rightPrefixPos;
    private final int rightRangePos;
    private final int rightFieldCount;
    private final int[] rightProjects;
    private final boolean leftJoin;

    public LookupJoinEnumerable(Enumerable<Object[]> left, long sourceId, String table,
            String indexName, boolean range, int leftFieldCount, int prefixLength,
            int lowerOrdinal, int upperOrdinal, boolean lowerInclusive, boolean upperInclusive,
            String rightPrefixPos, int rightRangePos, int rightFieldCount,
            String rightProjects, boolean leftJoin) {
        this.left = left;
        this.source = LookupSourceRegistry.get(sourceId);
        this.table = table;
        this.indexName = indexName;
        this.range = range;
        this.leftFieldCount = leftFieldCount;
        this.prefixLength = prefixLength;
        this.lowerOrdinal = lowerOrdinal;
        this.upperOrdinal = upperOrdinal;
        this.lowerInclusive = lowerInclusive;
        this.upperInclusive = upperInclusive;
        // Empty prefix positions (a range on the leading key column) → int[0].
        this.rightPrefixPos = parsePositions(rightPrefixPos);
        this.rightRangePos = rightRangePos;
        this.rightFieldCount = rightFieldCount;
        // Empty projects → null, meaning "all columns".
        this.rightProjects = rightProjects == null || rightProjects.isEmpty()
                ? null : parsePositions(rightProjects);
        this.leftJoin = leftJoin;
    }

    private static int[] parsePositions(String csv) {
        if (csv == null || csv.isEmpty()) {
            return new int[0];
        }
        String[] parts = csv.split(",");
        int[] out = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            out[i] = Integer.parseInt(parts[i].trim());
        }
        return out;
    }

    @Override
    public Enumerator<Object[]> enumerator() {
        List<Object[]> output = new ArrayList<>();
        List<Object[]> batch = new ArrayList<>(DEFAULT_BATCH);
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
        // Build a lookup request per left row (null when a key value is absent).
        List<LookupRequest> perRow = new ArrayList<>(batch.size());
        List<LookupRequest> requests = new ArrayList<>();
        for (Object[] leftRow : batch) {
            LookupRequest request = requestFor(leftRow);
            perRow.add(request);
            if (request != null) {
                requests.add(request);
            }
        }

        // Fetch (possibly a superset) and index the right rows by key prefix.
        Map<List<Object>, List<Object[]>> byPrefix = new HashMap<>();
        if (!requests.isEmpty()) {
            Iterable<Object[]> fetched =
                    source.lookupWithCache(table, indexName, LookupBatch.of(requests), rightProjects);
            for (Object[] row : fetched) {
                List<Object> prefixKey = new ArrayList<>(rightPrefixPos.length);
                for (int pos : rightPrefixPos) {
                    prefixKey.add(row[pos]);
                }
                byPrefix.computeIfAbsent(prefixKey, k -> new ArrayList<>()).add(row);
            }
        }

        // Match each left row to its right rows and emit joined output.
        for (int i = 0; i < batch.size(); i++) {
            Object[] leftRow = batch.get(i);
            LookupRequest request = perRow.get(i);
            boolean matched = false;
            if (request != null) {
                List<Object[]> candidates = byPrefix.get(request.prefix());
                if (candidates != null) {
                    for (Object[] right : candidates) {
                        if (!range || inRange(right[rightRangePos], request)) {
                            output.add(join(leftRow, right));
                            matched = true;
                        }
                    }
                }
            }
            if (!matched && leftJoin) {
                output.add(join(leftRow, null));
            }
        }
    }

    private LookupRequest requestFor(Object[] leftRow) {
        List<Object> prefix = new ArrayList<>(prefixLength);
        for (int i = 0; i < prefixLength; i++) {
            Object value = leftRow[leftFieldCount + i];
            if (value == null) {
                return null;
            }
            prefix.add(value);
        }
        if (!range) {
            return LookupRequest.point(prefix);
        }
        Object lower = leftRow[lowerOrdinal];
        Object upper = leftRow[upperOrdinal];
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
        Comparable cv = (Comparable) value;
        int cl = cv.compareTo(request.lower());
        if (cl < 0 || (cl == 0 && !request.lowerInclusive())) {
            return false;
        }
        int cu = cv.compareTo(request.upper());
        return cu < 0 || (cu == 0 && request.upperInclusive());
    }

    /** Builds an output row: original left columns followed by right columns. */
    private Object[] join(Object[] leftRow, Object[] rightRow) {
        Object[] out = new Object[leftFieldCount + rightFieldCount];
        System.arraycopy(leftRow, 0, out, 0, leftFieldCount);
        if (rightRow != null) {
            System.arraycopy(rightRow, 0, out, leftFieldCount, rightFieldCount);
        }
        return out;
    }
}
