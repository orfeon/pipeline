package com.mercari.solution.util.domain.attribution;

import java.io.Serializable;

/**
 * Algorithm selection and tuning for {@link AttributionEngine}.
 * Defaults follow work.md / the RiskLoc paper: riskThreshold 0.5, pepThreshold 0.02, pruningLayers 1;
 * Adtributor teep 0.1 / tep 0.67 (NSDI 2014); guards minSupport 0.005, maxLayer 3, maxCardinality 200.
 */
public record EngineConfig(
        Algorithm algorithm,
        RiskLocParams riskloc,
        AdtributorParams adtributor,
        Guards guards,
        DerivedAllocation.Method derivedAllocation,
        EpBasis epBasis,
        int topK) implements Serializable {

    public EngineConfig(
            final Algorithm algorithm,
            final RiskLocParams riskloc,
            final AdtributorParams adtributor,
            final Guards guards,
            final DerivedAllocation.Method derivedAllocation,
            final int topK) {
        this(algorithm, riskloc, adtributor, guards, derivedAllocation, EpBasis.auto, topK);
    }

    public enum Algorithm {
        riskloc,
        adtributor,
        exhaustive
    }

    /**
     * Basis of the explanatory power computation.
     * {@code netDelta} = share of the net change {@code (v-f)/(V-F)};
     * {@code absoluteDelta} = share of the total churn {@code |v-f|/Σ|v-f|}, which also covers
     * changes that cancel out in the totals (mix shifts, synthetic marginal baselines);
     * {@code auto} (default) = netDelta, falling back to absoluteDelta when the net change is
     * negligible relative to the churn (see {@link AttributionEngine}).
     */
    public enum EpBasis {
        auto,
        netDelta,
        absoluteDelta
    }

    public record RiskLocParams(
            double riskThreshold,
            double pepThreshold,
            int pruningLayers,
            boolean degenerateGuard) implements Serializable {

        /** Reference-parity constructor: the degenerate-cutoff guard defaults to enabled. */
        public RiskLocParams(final double riskThreshold, final double pepThreshold, final int pruningLayers) {
            this(riskThreshold, pepThreshold, pruningLayers, true);
        }

        public static RiskLocParams defaults() {
            return new RiskLocParams(0.5, 0.02, 1);
        }
    }

    public record AdtributorParams(
            double teep,
            double tep) implements Serializable {

        public static AdtributorParams defaults() {
            return new AdtributorParams(0.1, 0.67);
        }
    }

    public record Guards(
            double minSupport,
            int maxLayer,
            int maxCardinality) implements Serializable {

        public static Guards defaults() {
            return new Guards(0.005, 3, 200);
        }
    }

    public static EngineConfig defaults() {
        return new EngineConfig(
                Algorithm.riskloc,
                RiskLocParams.defaults(),
                AdtributorParams.defaults(),
                Guards.defaults(),
                DerivedAllocation.Method.gre,
                3);
    }
}
