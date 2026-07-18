package com.mercari.solution.util.pipeline.lookup;

import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptTable;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.core.TableScan;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Plan-time guard over lookup-table access: verifies that every access to a
 * (scan-rejected) lookup table in an optimized physical plan became a
 * key-driven read. When a join condition does not fit the key contract, the
 * planner leaves the join alone and the plan keeps a scan of the lookup table
 * — which would fail on the workers, per element, with the scan-rejection
 * error. This validator surfaces that outcome at pipeline construction time
 * instead, naming the offending tables.
 *
 * <p>The check is structural, not policy: a surviving lookup-table scan can
 * never execute successfully ({@link LookupTable#scan} throws), so failing
 * early is always correct — there is no configuration to relax it.
 */
public final class LookupAccessValidator {

    private LookupAccessValidator() {
    }

    /**
     * Throws when {@code physicalPlan} still scans any lookup table (i.e. the
     * access did not become a {@link LookupJoin}/{@link LookupLateralJoin}).
     *
     * @param physicalPlan the optimized physical plan ({@code BindableQuery.physicalPlan()})
     * @param sql          the statement text, for the error message
     */
    public static void validateKeyDrivenAccess(final RelNode physicalPlan, final String sql) {
        final Set<String> scanned = new LinkedHashSet<>();
        collectLookupScans(physicalPlan, scanned);
        if (scanned.isEmpty()) {
            return;
        }
        throw new IllegalArgumentException(
                "query would scan lookup table(s) " + scanned + " instead of performing a"
                        + " key-driven lookup; the join condition must constrain a contiguous"
                        + " prefix of a candidate key (point equality on the full key, an"
                        + " opted-in leading-prefix equality, or leading equality plus a bounded"
                        + " range on the next column) — standalone scans are not supported."
                        + " sql: " + sql);
    }

    private static void collectLookupScans(final RelNode rel, final Set<String> scanned) {
        if (rel instanceof TableScan scan && isLookupTable(scan.getTable())) {
            scanned.add(String.join(".", scan.getTable().getQualifiedName()));
        }
        for (final RelNode input : rel.getInputs()) {
            collectLookupScans(input, scanned);
        }
    }

    private static boolean isLookupTable(final RelOptTable table) {
        return table != null && table.unwrap(LookupTable.class) != null;
    }
}
