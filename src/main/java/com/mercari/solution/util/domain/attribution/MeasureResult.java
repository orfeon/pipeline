package com.mercari.solution.util.domain.attribution;

import java.io.Serializable;
import java.util.List;

/**
 * Attribution result for a single measure. An empty {@code findings} list means no significant
 * root cause was localized (the caller decides how to represent "no finding").
 * {@code epBasis} records which explanatory-power basis was actually used — the two bases answer
 * different questions (netDelta: "share of the net change", absoluteDelta: "share of the total
 * churn including canceling changes") and report consumers must not confuse them.
 * {@code quantile} is set only for distribution measures (one result per analyzed quantile);
 * the totals and finding sums are then quantile values of merged sketches, not sums.
 */
public record MeasureResult(
        String measure,
        Double quantile,
        double baselineTotal,
        double targetTotal,
        EngineConfig.EpBasis epBasis,
        List<Finding> findings) implements Serializable {

    public MeasureResult(
            final String measure,
            final double baselineTotal,
            final double targetTotal,
            final EngineConfig.EpBasis epBasis,
            final List<Finding> findings) {
        this(measure, null, baselineTotal, targetTotal, epBasis, findings);
    }
}
