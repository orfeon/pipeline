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

    /**
     * Share of the measure's change (on the recorded {@code epBasis}) that the reported findings
     * do <b>not</b> explain, clamped to [0, 1]. A high value together with a substantial
     * total delta is evidence of an <b>external root cause</b>: the change is real but not
     * localizable in the declared dimensions (or was suppressed by thresholds/guards).
     * With no findings this is 1 — interpret it together with the total delta, since an
     * unchanged measure also reports no findings.
     */
    public double unexplainedShare() {
        double explained = 0;
        for(final Finding finding : findings) {
            explained += Math.abs(finding.explanatoryPower());
        }
        return Math.max(0.0, Math.min(1.0, 1.0 - explained));
    }
}
