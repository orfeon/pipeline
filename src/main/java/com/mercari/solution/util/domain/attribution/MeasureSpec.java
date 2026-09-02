package com.mercari.solution.util.domain.attribution;

import java.io.Serializable;
import java.util.List;

/**
 * Measure definition for attribution analysis.
 * A {@code fundamental} measure is a sum-additive column; a {@code derived} measure is an arithmetic
 * expression (Lucene expressions, JavaScript-like syntax) over fundamental columns,
 * e.g. {@code "orders / sessions"}. The caller extracts {@code variables} from the expression;
 * evaluation uses the pipeline's shared {@code ExpressionUtil} function registry.
 * A {@code distribution} measure localizes shifts of the named value column's distribution at the
 * given {@code quantiles} (each in (0, 1)); per-leaf distributions are held as mergeable KLL
 * sketches (see {@link LeafTable}).
 * A {@code distinct} measure localizes changes of the named identity column's distinct count
 * (e.g. daily active users); per-leaf identity sets are held as mergeable Theta sketches, so a
 * slice's distinct count is the estimate of the union of its leaves' sketches, never a sum.
 */
public record MeasureSpec(
        String name,
        Type type,
        String expression,
        List<String> variables,
        List<Double> quantiles) implements Serializable {

    public MeasureSpec(final String name, final Type type, final String expression, final List<String> variables) {
        this(name, type, expression, variables, null);
    }

    public enum Type {
        fundamental,
        derived,
        distribution,
        distinct
    }

    public static MeasureSpec fundamental(final String name) {
        return new MeasureSpec(name, Type.fundamental, null, List.of(name), null);
    }

    public static MeasureSpec derived(final String name, final String expression, final List<String> variables) {
        return new MeasureSpec(name, Type.derived, expression, variables, null);
    }

    public static MeasureSpec distribution(final String name, final List<Double> quantiles) {
        if(quantiles == null || quantiles.isEmpty()) {
            throw new IllegalArgumentException("distribution measure " + name + " requires quantiles");
        }
        for(final Double quantile : quantiles) {
            if(quantile == null || !(quantile > 0 && quantile < 1)) {
                throw new IllegalArgumentException("distribution measure " + name
                        + " quantiles must be in (0, 1): " + quantiles);
            }
        }
        return new MeasureSpec(name, Type.distribution, null, List.of(name), List.copyOf(quantiles));
    }

    public static MeasureSpec distinct(final String name) {
        return new MeasureSpec(name, Type.distinct, null, List.of(name), null);
    }
}
