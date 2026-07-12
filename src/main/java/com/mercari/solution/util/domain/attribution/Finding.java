package com.mercari.solution.util.domain.attribution;

import java.io.Serializable;
import java.util.List;

/**
 * One localized root cause.
 * RiskLoc/Exhaustive emit one slice per finding; Adtributor emits one finding per culprit dimension
 * whose {@code slices} are the selected values of that single dimension.
 * {@code riskScore} and {@code surprise} are null when the algorithm does not compute them.
 */
public record Finding(
        List<Slice> slices,
        Double riskScore,
        double explanatoryPower,
        Double surprise,
        double baselineSum,
        double targetSum,
        int leafCount) implements Serializable {

    public int layer() {
        return slices.stream().mapToInt(Slice::layer).max().orElse(0);
    }
}
