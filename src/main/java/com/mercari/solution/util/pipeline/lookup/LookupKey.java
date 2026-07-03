package com.mercari.solution.util.pipeline.lookup;

import java.io.Serializable;
import java.util.List;

/**
 * A candidate key a lookup-join can use to access an external table: the primary
 * key, or a unique secondary index. The join condition must constrain a contiguous
 * prefix of {@link #columns()} (point equality on the full key, or prefix equality
 * plus a range on the next column).
 *
 * @param indexName the unique index name, or {@code null} for the primary key
 * @param columns   the key columns in key order
 */
public record LookupKey(String indexName, List<String> columns) implements Serializable {

    public LookupKey {
        columns = List.copyOf(columns);
    }

    public static LookupKey primaryKey(List<String> columns) {
        return new LookupKey(null, columns);
    }

    public static LookupKey index(String indexName, List<String> columns) {
        return new LookupKey(indexName, columns);
    }

    public boolean isPrimaryKey() {
        return indexName == null;
    }
}
