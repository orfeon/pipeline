package com.mercari.solution.util.domain.attribution.algorithm;

/**
 * Algorithm-neutral scoring functions shared across localization algorithms.
 */
final class Scores {

    private Scores() {
    }

    /**
     * Jensen-Shannon divergence term of one element:
     * {@code 0.5 * (p*ln(2p/(p+q)) + q*ln(2q/(p+q)))} with non-finite terms treated as 0.
     * (Adtributor's surprise, NSDI 2014; also used by the exhaustive oracle.)
     */
    static double jsDivergence(final double f, final double v, final double baselineTotal, final double targetTotal) {
        final double p = baselineTotal == 0 ? 0 : f / baselineTotal;
        final double q = targetTotal == 0 ? 0 : v / targetTotal;
        final double pTerm = p * Math.log(2 * p / (p + q));
        final double qTerm = q * Math.log(2 * q / (p + q));
        return 0.5 * ((Double.isFinite(pTerm) ? pTerm : 0) + (Double.isFinite(qTerm) ? qTerm : 0));
    }
}
