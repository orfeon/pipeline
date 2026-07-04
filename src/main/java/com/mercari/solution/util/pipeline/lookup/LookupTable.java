package com.mercari.solution.util.pipeline.lookup;

import com.mercari.solution.util.schema.CalciteSchemaUtil;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.DataContext;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.linq4j.Enumerable;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.ScannableTable;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.Statistic;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.Statistics;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.impl.AbstractTable;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.List;

/**
 * A synthetic, key-driven lookup table: it never holds rows of its own and is
 * only ever the lookup side of an input-driven join on one of its candidate
 * keys — {@link LookupJoinRule} rewrites that join before execution. A
 * standalone scan has no key binding and is rejected at execution time.
 *
 * <p>Reporting a very large row count makes the optimizer prefer the key-driven
 * lookup-join over any plan that would materialize the table; candidate keys are
 * advertised as unique so the rule selects them — at runtime a single key may
 * still fan out to many rows (e.g. a REST response array), and every one is
 * emitted.
 */
public final class LookupTable extends AbstractTable implements ScannableTable {

    private static final double LARGE_ROW_COUNT = 1.0e9;

    private final String name;
    private final com.mercari.solution.module.Schema schema;
    private final List<ImmutableBitSet> uniqueKeys;
    private final String scanRejectionMessage;

    private RelDataType rowType;

    public LookupTable(final String sourceName, final String name,
            final com.mercari.solution.module.Schema schema, final List<LookupKey> keyCandidates) {
        this.name = name;
        this.schema = schema;
        this.uniqueKeys = new ArrayList<>();
        final List<String> fieldNames = new ArrayList<>();
        for (int i = 0; i < schema.countFields(); i++) {
            fieldNames.add(schema.getField(i).getName());
        }
        for (final LookupKey candidate : keyCandidates) {
            final List<Integer> indexes = new ArrayList<>();
            boolean resolved = true;
            for (final String column : candidate.columns()) {
                final int index = fieldNames.indexOf(column);
                if (index < 0) {
                    resolved = false;
                    break;
                }
                indexes.add(index);
            }
            if (resolved) {
                uniqueKeys.add(ImmutableBitSet.of(indexes));
            }
        }
        this.scanRejectionMessage = "Lookup table '" + sourceName + "." + name
                + "' can only be used as the lookup side of an input-driven join on its key"
                + " (point equality on the full key, or leading-column equality plus a bounded"
                + " range on the next column); standalone scans are not supported";
    }

    public String name() {
        return name;
    }

    @Override
    public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
        if (this.rowType == null) {
            this.rowType = CalciteSchemaUtil.convertSchema(schema, typeFactory);
        }
        return this.rowType;
    }

    @Override
    public Statistic getStatistic() {
        return Statistics.of(LARGE_ROW_COUNT, uniqueKeys, List.of(), List.of());
    }

    @Override
    public Enumerable<Object[]> scan(final DataContext root) {
        throw new IllegalStateException(scanRejectionMessage);
    }
}
