package com.mercari.solution.util.domain.attribution;

import java.io.Serializable;
import java.util.List;

/**
 * Attribution result for a single measure. An empty {@code findings} list means no significant
 * root cause was localized (the caller decides how to represent "no finding").
 * {@code epBasis} records which explanatory-power basis was actually used — the two bases answer
 * different questions (netDelta: "share of the net change", absoluteDelta: "share of the total
 * churn including canceling changes") and report consumers must not confuse them.
 */
public record MeasureResult(
        String measure,
        double baselineTotal,
        double targetTotal,
        EngineConfig.EpBasis epBasis,
        List<Finding> findings) implements Serializable {
}
