package com.mercari.solution.util.domain.attribution;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A slice element: a conjunction of dimension=value predicates over a cuboid.
 * {@code dims} holds dimension indexes (ascending), {@code values} the matching values.
 * Ordering is deterministic (layer, then dimension indexes, then values) and is used as the
 * tie-breaker everywhere so that results are reproducible.
 */
public record Slice(int[] dims, String[] values) implements Comparable<Slice>, Serializable {

    public int layer() {
        return dims.length;
    }

    public boolean contains(final String[] leafDims) {
        for(int i = 0; i < dims.length; i++) {
            if(!values[i].equals(leafDims[dims[i]])) {
                return false;
            }
        }
        return true;
    }

    public List<String> describe(final List<String> dimensionNames) {
        return Arrays.stream(dims)
                .mapToObj(dimensionNames::get)
                .collect(Collectors.toList());
    }

    @Override
    public int compareTo(final Slice o) {
        int c = Integer.compare(dims.length, o.dims.length);
        if(c != 0) {
            return c;
        }
        c = Arrays.compare(dims, o.dims);
        if(c != 0) {
            return c;
        }
        return Arrays.compare(values, o.values);
    }

    @Override
    public boolean equals(final Object o) {
        if(this == o) {
            return true;
        }
        if(!(o instanceof Slice other)) {
            return false;
        }
        return Arrays.equals(dims, other.dims) && Arrays.equals(values, other.values);
    }

    @Override
    public int hashCode() {
        return 31 * Arrays.hashCode(dims) + Arrays.hashCode(values);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        for(int i = 0; i < dims.length; i++) {
            if(i > 0) {
                sb.append(" & ");
            }
            sb.append(dims[i]).append("=").append(values[i]);
        }
        return sb.toString();
    }
}
