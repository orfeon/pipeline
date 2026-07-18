package com.mercari.solution.util.domain.attribution;

import java.io.Serializable;
import java.util.List;

/**
 * Dimension definition for attribution analysis.
 * Only {@code flat} and {@code binned} are supported (hierarchy/embedding are reserved for future versions
 * and must be rejected before reaching this core library).
 */
public record DimensionSpec(
        String name,
        Type type,
        Binning binning) implements Serializable {

    public enum Type {
        flat,
        binned
    }

    public record Binning(Method method, int bins) implements Serializable {
        public enum Method {
            quantile,
            width
        }
    }

    public static DimensionSpec flat(final String name) {
        return new DimensionSpec(name, Type.flat, null);
    }

    public static DimensionSpec binned(final String name, final Binning.Method method, final int bins) {
        return new DimensionSpec(name, Type.binned, new Binning(method, bins));
    }

    public static List<String> names(final List<DimensionSpec> specs) {
        return specs.stream().map(DimensionSpec::name).toList();
    }
}
