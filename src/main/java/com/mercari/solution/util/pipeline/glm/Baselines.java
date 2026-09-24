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

    /** {@code invalid}: what an invalid value of a column does to its unit — skip the unit whole (the default) or drop the row. */
    public static final String INVALID_SKIP_UNIT = "skipUnit";
    public static final String INVALID_DROP_ROW = "dropRow";
    public static final java.util.List<String> INVALIDS = java.util.List.of(INVALID_SKIP_UNIT, INVALID_DROP_ROW);

    /** The share of skipped units above which a summary notes them with their reasons (both supervised transforms). */
    public static final double SKIP_SHARE_NOTE = 0.01;

    /** Whether {@code skipped} of the {@code skipped + scored} units pass {@link #SKIP_SHARE_NOTE}. */
    public static boolean skipShareNoted(final double skipped, final double scored) {
        return skipped > SKIP_SHARE_NOTE * (skipped + scored);
    }

    /** {@code part / whole} as a percentage with one decimal ("12.5%"). */
    public static String percent(final double part, final double whole) {
        return String.format(java.util.Locale.ROOT, "%.1f%%", 100d * part / whole);
    }

    /**
     * Whether one baseline value is usable in {@code form}: finite, and within the form's domain — a probability in
     * [0, 1] (0 is a valid share of 0), a log probability ≤ 0, a positive value under {@code inverseShare} / {@code
     * rate} (a null, 0 or negative odds / price / rate is invalid), a log rate whose exponential is positive and
     * finite, any finite value under {@code value}.
     */
    public static boolean validRow(final String form, final double b) {
        if (Double.isNaN(b) || Double.isInfinite(b)) return false;
        return switch (form) {
            case Family.FORM_PROB -> b >= 0 && b <= 1;
            case Family.FORM_LOG_PROB -> b <= 0;
            case Family.FORM_INVERSE_SHARE, Family.FORM_RATE -> b > 0;
            case Family.FORM_LOG_RATE -> {
                final double r = Math.exp(b);
                yield r > 0 && !Double.isInfinite(r);
            }
            case Family.FORM_VALUE -> true;
            default -> throw new IllegalStateException("unknown baseline form " + form);
        };
    }

    /**
     * Fills {@code p} (the baseline mean per row) from the baseline values in {@code form}: grouped shares are
     * normalised within the unit (log probabilities shifted by the unit maximum first), {@code inverseShare}
     * takes 1 / x and normalises, binomial probabilities are clamped to [EPS, 1 − EPS], rates must be positive.
     * Returns {@link Skip#NONE} when every row is usable, {@link Skip#INVALID_BASELINE} otherwise (a row
     * {@link #validRow} rejects, or a non-positive share sum). A caller that drops the invalid rows first
     * ({@code invalid: dropRow}) reaches only the share-sum case.
     */
    public static Skip means(final Family family, final String form, final double[] baseline, final double[] p) {
        final int n = baseline.length;
        final boolean grouped = family.isGrouped();
        double max = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < n; i++) {
            final double b = baseline[i];
            if (!validRow(form, b)) return Skip.INVALID_BASELINE;
            switch (form) {
                case Family.FORM_PROB, Family.FORM_VALUE, Family.FORM_RATE -> p[i] = b;
                case Family.FORM_LOG_PROB -> {
                    p[i] = b;
                    if (b > max) max = b;
                }
                case Family.FORM_INVERSE_SHARE -> p[i] = 1d / b;
                case Family.FORM_LOG_RATE -> p[i] = Math.exp(b);
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
