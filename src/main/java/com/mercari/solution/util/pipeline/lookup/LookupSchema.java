package com.mercari.solution.util.pipeline.lookup;

import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.Table;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.impl.AbstractSchema;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Calcite schema exposing one {@link LookupSource}'s tables as
 * {@link LookupTable}s under the source's name, so SQL can reference them as
 * {@code sourceName.tableName}.
 */
public final class LookupSchema extends AbstractSchema {

    private final Map<String, Table> tables;

    private LookupSchema(final Map<String, Table> tables) {
        this.tables = tables;
    }

    public static LookupSchema of(final LookupSource source) {
        final Map<String, Table> tables = new LinkedHashMap<>();
        for (final Map.Entry<String, com.mercari.solution.module.Schema> entry
                : source.tableSchemas().entrySet()) {
            tables.put(entry.getKey(), new LookupTable(source.getName(), entry.getKey(),
                    entry.getValue(), source.keyCandidates(entry.getKey())));
        }
        return new LookupSchema(tables);
    }

    @Override
    protected Map<String, Table> getTableMap() {
        return tables;
    }
}
