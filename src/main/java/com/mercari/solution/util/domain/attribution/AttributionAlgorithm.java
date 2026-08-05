package com.mercari.solution.util.domain.attribution;

import com.mercari.solution.util.domain.attribution.algorithm.Adtributor;
import com.mercari.solution.util.domain.attribution.algorithm.Exhaustive;
import com.mercari.solution.util.domain.attribution.algorithm.RiskLoc;

import java.util.List;

/**
 * A root-cause localization algorithm over a leaf table and one (possibly pseudo) measure.
 * Implementations must be deterministic: equal inputs produce identical findings in identical order.
 */
public interface AttributionAlgorithm {

    List<Finding> localize(LeafTable table, MeasureVector measure, EngineConfig config);

    static AttributionAlgorithm of(final EngineConfig.Algorithm algorithm) {
        return switch (algorithm) {
            case riskloc -> new RiskLoc();
            case adtributor -> new Adtributor();
            case exhaustive -> new Exhaustive();
        };
    }
}
