package com.mercari.solution.util.pipeline.udf;

import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.adapter.enumerable.CallImplementor;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.adapter.enumerable.ReflectiveCallNotNullImplementor;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.linq4j.tree.Types;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.Function;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.FunctionParameter;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.ImplementableFunction;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.ScalarFunction;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.type.SqlTypeName;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

/**
 * Built-in numeric scalar UDFs registered by {@code Query} for every query:
 * {@code DIV(x, y)} — BigQuery's integer division (the quotient truncated
 * toward zero), which the Calcite BigQuery function library does not provide.
 *
 * <p>{@code DIV(x, y)} → {@code BIGINT}: both arguments are integers of any
 * width (a {@code BIGINT} or {@code INTEGER} column, a literal, an integer
 * expression such as {@code MOD(...)}). NULL in → NULL out; division by zero,
 * the int64 overflow ({@code MIN / -1}) and a non-integer argument are errors.
 */
public final class MathFunctions {

    private MathFunctions() {
    }

    /**
     * Reflective evaluation target. The parameters are ANY-typed: with a typed
     * {@code Long} signature Calcite passes an INTEGER argument as-is (no cast
     * to BIGINT), so the generated call does not compile for an int32 column
     * or an INTEGER expression, and it silently casts a DECIMAL / DOUBLE /
     * VARCHAR argument to BIGINT before dividing.
     */
    public static Long div(Object x, Object y) {
        if (x == null || y == null) {
            return null;
        }
        final long dividend = integer("dividend", x);
        final long divisor = integer("divisor", y);
        if (divisor == 0) {
            throw new ArithmeticException("DIV: division by zero");
        }
        if (dividend == Long.MIN_VALUE && divisor == -1) {
            // the quotient does not fit an int64: fail instead of wrapping around
            throw new ArithmeticException("DIV: int64 overflow: " + dividend + " / " + divisor);
        }
        return dividend / divisor;
    }

    private static long integer(String parameter, Object value) {
        if (value instanceof Long || value instanceof Integer
                || value instanceof Short || value instanceof Byte) {
            return ((Number) value).longValue();
        }
        throw new IllegalArgumentException("DIV " + parameter + " must be an integer, but got "
                + value.getClass().getSimpleName() + " (cast it explicitly, e.g. CAST(x AS BIGINT))");
    }

    /** The Calcite function object. Hand-built for the ANY-typed parameters. */
    static List<Map.Entry<String, Function>> builtIns() {
        return List.of(Map.entry("DIV", new DivFunction()));
    }

    private static final class DivFunction
            implements ScalarFunction, ImplementableFunction {

        private static final Method METHOD = Types.lookupMethod(
                MathFunctions.class, "div", Object.class, Object.class);

        @Override
        public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(
                    typeFactory.createSqlType(SqlTypeName.BIGINT), true);
        }

        @Override
        public List<FunctionParameter> getParameters() {
            return SequenceFunnelFunctions.parameters(
                    new String[]{"x", "y"}, ordinal -> SqlTypeName.ANY);
        }

        @Override
        public CallImplementor getImplementor() {
            return RexImpTable.createImplementor(
                    new ReflectiveCallNotNullImplementor(METHOD), NullPolicy.NONE, false);
        }
    }
}
