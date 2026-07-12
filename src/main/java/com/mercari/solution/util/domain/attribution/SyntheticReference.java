package com.mercari.solution.util.domain.attribution;

import java.util.Map;

/**
 * Synthesizes a baseline from the target itself.
 * The {@code marginal} strategy replaces every baseline column with the independence model
 * {@code baseline(leaf) = T * Π_d (T_d(value_d) / T)} where {@code T} is the column's target total
 * and {@code T_d} the per-dimension marginal totals. Slices whose target deviates strongly from
 * this baseline are evidence of interaction structure between dimensions.
 */
public final class SyntheticReference {

    private SyntheticReference() {
    }

    public static LeafTable marginal(final LeafTable table) {
        final int columnCount = table.columnCount();
        final int dimensionCount = table.dimensionCount();
        final double[][] baseline = new double[columnCount][table.leafCount()];

        for(int c = 0; c < columnCount; c++) {
            for(int leaf = 0; leaf < table.leafCount(); leaf++) {
                if(table.targetValue(c, leaf) < 0) {
                    throw new IllegalArgumentException(
                            "synthetic.marginal requires nonnegative target values, but column "
                                    + table.getColumnNames().get(c) + " has "
                                    + table.targetValue(c, leaf) + " at leaf " + leaf);
                }
            }
            final double total = table.targetTotal(c);
            if(total == 0) {
                continue;
            }
            @SuppressWarnings("unchecked")
            final Map<String, Double>[] marginals = new Map[dimensionCount];
            for(int d = 0; d < dimensionCount; d++) {
                marginals[d] = table.targetMarginals(c, d);
            }
            for(int leaf = 0; leaf < table.leafCount(); leaf++) {
                double expected = total;
                for(int d = 0; d < dimensionCount; d++) {
                    expected *= marginals[d].get(table.dimValue(leaf, d)) / total;
                }
                baseline[c][leaf] = expected;
            }
        }
        return table.withBaseline(baseline);
    }
}
