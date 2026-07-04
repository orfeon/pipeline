package com.mercari.solution.util.pipeline.lookup;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * A batch of {@link LookupRequest}s of a single shape (all point or all range),
 * handed to {@link LookupSource#lookup} so the source can issue one efficient
 * read. The source may return a <em>superset</em> of the matching rows — the
 * lookup-join operator filters to exact matches.
 *
 * <p>Helpers expose what a source typically needs: the distinct key prefixes
 * (for an {@code IN} list / key set) and, for range batches, the global lower
 * and upper bounds over the range column (for a single bounded fetch).
 */
public final class LookupBatch {

    private final List<LookupRequest> requests;
    private final boolean range;

    private LookupBatch(List<LookupRequest> requests, boolean range) {
        this.requests = List.copyOf(requests);
        this.range = range;
    }

    public static LookupBatch of(List<LookupRequest> requests) {
        boolean range = !requests.isEmpty() && requests.get(0).isRange();
        return new LookupBatch(requests, range);
    }

    public List<LookupRequest> requests() {
        return requests;
    }

    public boolean isRange() {
        return range;
    }

    public boolean isEmpty() {
        return requests.isEmpty();
    }

    /** Number of leading key columns constrained by equality (the prefix length). */
    public int prefixLength() {
        return requests.isEmpty() ? 0 : requests.get(0).prefix().size();
    }

    /** Distinct prefix tuples across the batch (key-column equality values). */
    public List<List<Object>> distinctPrefixes() {
        Set<List<Object>> seen = new LinkedHashSet<>();
        for (LookupRequest request : requests) {
            seen.add(request.prefix());
        }
        return new ArrayList<>(seen);
    }

    /** Smallest lower bound over the range column (for a superset fetch), or null. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Object globalLower() {
        Object min = null;
        for (LookupRequest request : requests) {
            Object lower = request.lower();
            if (lower != null && (min == null || ((Comparable) lower).compareTo(min) < 0)) {
                min = lower;
            }
        }
        return min;
    }

    /** Largest upper bound over the range column (for a superset fetch), or null. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Object globalUpper() {
        Object max = null;
        for (LookupRequest request : requests) {
            Object upper = request.upper();
            if (upper != null && (max == null || ((Comparable) upper).compareTo(max) > 0)) {
                max = upper;
            }
        }
        return max;
    }
}
