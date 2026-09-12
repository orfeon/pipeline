package com.mercari.solution.util.pipeline.glm;

/**
 * Baseline and label preparation of one unit, shared by the supervised transforms: the baseline column in its
 * declared form → the mean per row (a share summing to 1 within the group, a clamped probability, a value, a
 * rate), and the grouped labels → shares summing to 1. Pure; a unit that cannot be prepared is reported as a
 * {@link Skip} reason and never partially used.
 */
public final class Baselines {

    private Baselines() {}

    /** Clamp of binomial probabilities and of the prior rate: [EPS, 1 − EPS]. */
    public static final double EPS = 1e-12;

    /** Why a unit cannot be scored. */
    public enum Skip { NONE, NO_POSITIVE_LABEL, INVALID_BASELINE }

    /**
     * Fills {@code p} (the baseline mean per row) from the baseline values in {@code form}: grouped shares are
     * normalised within the unit (log probabilities shifted by the unit maximum first), {@code inverseShare}
     * takes 1 / x and normalises, binomial probabilities are clamped to [EPS, 1 − EPS], rates must be positive.
     * Returns {@link Skip#NONE} when every row is usable, {@link Skip#INVALID_BASELINE} otherwise (a NaN /
     * infinite value, a probability outside [0, 1], a positive log probability, a non-positive rate or share
     * sum).
     */
    public static Skip means(final Family family, final String form, final double[] baseline, final double[] p) {
        final int n = baseline.length;
        final boolean grouped = family.isGrouped();
        double max = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < n; i++) {
            final double b = baseline[i];
            if (Double.isNaN(b) || Double.isInfinite(b)) return Skip.INVALID_BASELINE;
            switch (form) {
                case Family.FORM_PROB -> {
                    if (b < 0 || b > 1) return Skip.INVALID_BASELINE;
                    p[i] = b;
                }
                case Family.FORM_LOG_PROB -> {
                    if (b > 0) return Skip.INVALID_BASELINE;
                    p[i] = b;
                    if (b > max) max = b;
                }
                case Family.FORM_INVERSE_SHARE -> {
                    if (!(b > 0)) return Skip.INVALID_BASELINE;
                    p[i] = 1d / b;
                }
                case Family.FORM_VALUE -> p[i] = b;
                case Family.FORM_RATE -> {
                    if (!(b > 0)) return Skip.INVALID_BASELINE;
                    p[i] = b;
                }
                case Family.FORM_LOG_RATE -> {
                    p[i] = Math.exp(b);
                    if (!(p[i] > 0) || Double.isInfinite(p[i])) return Skip.INVALID_BASELINE;
                }
                default -> throw new IllegalStateException("unknown baseline form " + form);
            }
        }
        if (Family.FORM_LOG_PROB.equals(form)) {
            for (int i = 0; i < n; i++) p[i] = Math.exp(p[i] - (grouped ? max : 0d));
        }
        if (grouped || Family.FORM_INVERSE_SHARE.equals(form)) {
            double sum = 0;
            for (final double v : p) sum += v;
            if (!(sum > 0)) return Skip.INVALID_BASELINE;
            for (int i = 0; i < n; i++) p[i] /= sum;
        }
        if (family == Family.BINOMIAL) {
            for (int i = 0; i < n; i++) p[i] = clamp(p[i]);
        }
        return Skip.NONE;
    }

    /**
     * Grouped labels → shares: returns {@link Skip#NO_POSITIVE_LABEL} when the unit has no positive label;
     * otherwise divides by the sum when {@code normalizeTies} (a tie shares the unit's one likelihood term). The
     * row families keep their labels as they are.
     */
    public static Skip normalizeLabels(final Family family, final boolean normalizeTies, final double[] y) {
        if (!family.isGrouped()) return Skip.NONE;
        double sum = 0;
        for (final double v : y) sum += v;
        if (!(sum > 0)) return Skip.NO_POSITIVE_LABEL;
        if (normalizeTies) for (int i = 0; i < y.length; i++) y[i] /= sum;
        return Skip.NONE;
    }

    /** A probability clamped to [EPS, 1 − EPS]. */
    public static double clamp(final double p) {
        return Math.min(1 - EPS, Math.max(EPS, p));
    }
}
