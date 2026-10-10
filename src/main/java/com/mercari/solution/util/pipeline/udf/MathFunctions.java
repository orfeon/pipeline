package com.mercari.solution.util.pipeline.udf;

import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.linq4j.tree.Types;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.Function;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.util.List;
import java.util.Map;

/**
 * Built-in numeric scalar UDFs registered by {@code Query2} for every query:
 * {@code DIV(x, y)} — BigQuery's integer division (the quotient truncated
 * toward zero), which the Calcite BigQuery function library does not provide.
 */
public final class MathFunctions {

    private MathFunctions() {
    }

    static List<Map.Entry<String, Function>> builtIns() {
        return List.of(
                Map.entry("DIV", ScalarFunctionImpl.create(
                        Types.lookupMethod(MathFunctions.class, "div", Long.class, Long.class))));
    }

    // boxed: a NULL argument must reach the method (and yield NULL) instead of being unboxed
    public static Long div(Long x, Long y) {
        if (x == null || y == null) {
            return null;
        }
        if (y == 0) {
            throw new ArithmeticException("DIV: division by zero");
        }
        return x / y;
    }
}
