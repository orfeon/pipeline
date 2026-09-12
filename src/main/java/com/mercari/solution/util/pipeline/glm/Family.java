package com.mercari.solution.util.pipeline.glm;

import java.util.List;

/**
 * The response families shared by the supervised transforms (screen, evaluation): the unit of a likelihood
 * term (a group, or a row), the baseline forms a family accepts, and the Fisher weight / link of the row
 * families at a mean μ. One definition for every scorer and report that reads a family.
 */
public enum Family {

    /** mutually exclusive samples within a group (conditional logit / softmax): one likelihood term per group */
    GROUPED_MULTINOMIAL("groupedMultinomial"),
    /** independent binary rows */
    BINOMIAL("binomial"),
    /** independent continuous rows (identity link) */
    GAUSSIAN("gaussian"),
    /** independent count rows (log link) */
    POISSON("poisson");

    /** grouped / binomial baseline: a probability, its log, or 1 / x made a share within the group (odds, prices) */
    public static final String FORM_PROB = "prob";
    public static final String FORM_LOG_PROB = "logProb";
    public static final String FORM_INVERSE_SHARE = "inverseShare";
    /** gaussian baseline: the predicted value itself */
    public static final String FORM_VALUE = "value";
    /** poisson baseline: the predicted rate, or its log */
    public static final String FORM_RATE = "rate";
    public static final String FORM_LOG_RATE = "logRate";

    public static final List<String> PROBABILITY_FORMS = List.of(FORM_PROB, FORM_LOG_PROB, FORM_INVERSE_SHARE);
    public static final List<String> NAMES = List.of(GROUPED_MULTINOMIAL.id, BINOMIAL.id, GAUSSIAN.id, POISSON.id);

    private final String id;

    Family(final String id) {
        this.id = id;
    }

    /** The config name of the family. */
    public String id() {
        return id;
    }

    /** The family of a config name, or null when unknown. */
    public static Family of(final String name) {
        if (name == null) return null;
        for (final Family f : values()) if (f.id.equals(name)) return f;
        return null;
    }

    /** Baseline forms accepted by the family, the first one being the default. */
    public List<String> forms() {
        return switch (this) {
            case GAUSSIAN -> List.of(FORM_VALUE);
            case POISSON -> List.of(FORM_RATE, FORM_LOG_RATE);
            default -> PROBABILITY_FORMS;
        };
    }

    /** Baseline forms accepted by a family name (the probability forms for an unknown name, so parsing can go on). */
    public static List<String> formsFor(final String family) {
        final Family f = of(family);
        return f == null ? PROBABILITY_FORMS : f.forms();
    }

    public boolean isGrouped() {
        return this == GROUPED_MULTINOMIAL;
    }

    /**
     * The Fisher weight of a row family at the mean μ: binomial μ(1 − μ), poisson μ, gaussian 1 (σ² is applied by
     * the report). One definition for the marginal moments, the conditioning fit and the report's prior mode.
     */
    public double fisherWeight(final double mu) {
        return switch (this) {
            case BINOMIAL -> mu * (1 - mu);
            case POISSON -> mu;
            default -> 1d;
        };
    }

    /** The link of a row family at the mean μ (the intercept that reproduces μ without a baseline). */
    public double link(final double mu) {
        return switch (this) {
            case BINOMIAL -> Math.log(mu / (1 - mu));
            case POISSON -> Math.log(mu);
            default -> mu;
        };
    }
}
