package com.mercari.solution.util.domain.attribution;

import java.io.Serializable;
import java.util.List;

/**
 * Measure definition for attribution analysis.
 * A {@code fundamental} measure is a sum-additive column; a {@code derived} measure is an arithmetic
 * expression (exp4j syntax) over fundamental columns, e.g. {@code "orders / sessions"}.
 * The caller extracts {@code variables} from the expression (this library does not depend on the
 * pipeline's expression utilities).
 */
public record MeasureSpec(
        String name,
        Type type,
        String expression,
        List<String> variables) implements Serializable {

    public enum Type {
        fundamental,
        derived
    }

    public static MeasureSpec fundamental(final String name) {
        return new MeasureSpec(name, Type.fundamental, null, List.of(name));
    }

    public static MeasureSpec derived(final String name, final String expression, final List<String> variables) {
        return new MeasureSpec(name, Type.derived, expression, variables);
    }
}
