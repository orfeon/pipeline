package com.mercari.solution.util.domain.attribution;

/**
 * Per-leaf numeric vectors of the measure being localized.
 * {@code baseline} plays the forecast/expected role (f) and {@code target} the actual role (v).
 * {@code explanatoryPower} is optional: when null, algorithms derive the default additive
 * explanatory power {@code (v - f) / (V - F)}; derived measures supply their own allocation
 * (see {@link DerivedAllocation}).
 */
public record MeasureVector(
        double[] baseline,
        double[] target,
        double[] explanatoryPower) {

    public static MeasureVector of(final double[] baseline, final double[] target) {
        return new MeasureVector(baseline, target, null);
    }

    public int size() {
        return baseline.length;
    }

    public double baselineTotal() {
        double sum = 0;
        for(final double f : baseline) {
            sum += f;
        }
        return sum;
    }

    public double targetTotal() {
        double sum = 0;
        for(final double v : target) {
            sum += v;
        }
        return sum;
    }

    /**
     * Deviation score per leaf: {@code 2(f - v) / (f + v)}, 0 when {@code f + v == 0}.
     * (RiskLoc arXiv:2205.10004, reference implementation element_scores.add_deviation_score)
     */
    public double[] deviations() {
        final double[] d = new double[baseline.length];
        for(int i = 0; i < d.length; i++) {
            final double sum = baseline[i] + target[i];
            d[i] = sum == 0 ? 0.0 : 2 * (baseline[i] - target[i]) / sum;
        }
        return d;
    }

    /**
     * Explanatory power per leaf. Uses the supplied allocation when present, otherwise the
     * additive default {@code (v - f) / (V - F)}. When the total delta is 0 the default is all-zero.
     */
    public double[] explanatoryPowers() {
        if(explanatoryPower != null) {
            return explanatoryPower;
        }
        final double delta = targetTotal() - baselineTotal();
        final double[] ep = new double[baseline.length];
        if(delta == 0) {
            return ep;
        }
        for(int i = 0; i < ep.length; i++) {
            ep[i] = (target[i] - baseline[i]) / delta;
        }
        return ep;
    }
}
