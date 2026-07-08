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
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Array transformation built-ins over <em>scalar</em> arrays (ClickHouse's
 * lambda-free array helpers), always registered. They compose with the
 * {@code SEQ_*} family: {@code ARRAY_DIFFERENCE} over a timestamp array is
 * the inter-event-interval sequence, {@code ARRAY_COMPACT} normalizes a state
 * sequence before pattern matching.
 *
 * <ul>
 * <li><b>{@code ARRAY_DIFFERENCE(arr)}</b> → {@code ARRAY<DOUBLE>} /
 *     <b>{@code ARRAY_DIFFERENCE_INT(arr)}</b> → {@code ARRAY<BIGINT>}:
 *     element {@code i} = {@code arr[i] - arr[i-1]}; the first element is 0
 *     (same length as the input, so indexes stay aligned); a NULL neighbor
 *     yields a NULL delta.</li>
 * <li><b>{@code ARRAY_CUM_SUM(arr)}</b> → {@code ARRAY<DOUBLE>} /
 *     <b>{@code ARRAY_CUM_SUM_INT(arr)}</b> → {@code ARRAY<BIGINT>}: running
 *     total; NULL elements contribute 0 (the running total continues).</li>
 * <li><b>{@code ARRAY_COMPACT(arr)}</b> → {@code ARRAY<ANY>}: removes
 *     <em>consecutive</em> duplicates (state-transition normalization).</li>
 * <li><b>{@code ARRAY_DISTINCT(arr)}</b> → {@code ARRAY<ANY>}: removes all
 *     duplicates, keeping first-occurrence order.</li>
 * </ul>
 *
 * <p>All take the array as an ANY argument (typed array columns and opaque
 * engine-internal lists both work) and return NULL for a NULL input. The
 * numeric variants require numeric elements. {@code ARRAY_COMPACT} /
 * {@code ARRAY_DISTINCT} return {@code ARRAY<ANY>}: over scalar arrays the
 * elements stay {@code Comparable}, so {@code UNNEST} and the {@code SEQ_*}
 * consumers work, but the untyped result is not directly projectable into the
 * output schema (same rule as SEQ_COLLECT) — extract values downstream.
 */
public class ArrayTransformFunctions {

    private ArrayTransformFunctions() {
    }

    /** Reflective evaluation targets (arrays arrive as Calcite-internal Lists). */
    public static List arrayDifference(Object arr) {
        List<?> values = SequenceFieldArgs.asRows("ARRAY_DIFFERENCE", arr);
        if (values == null) {
            return null;
        }
        List<Double> out = new ArrayList<>(values.size());
        for (int i = 0; i < values.size(); i++) {
            if (i == 0) {
                out.add(0d);
                continue;
            }
            Number current = numeric("ARRAY_DIFFERENCE", values.get(i), i);
            Number previous = numeric("ARRAY_DIFFERENCE", values.get(i - 1), i - 1);
            out.add(current == null || previous == null
                    ? null : current.doubleValue() - previous.doubleValue());
        }
        return out;
    }

    public static List arrayDifferenceInt(Object arr) {
        List<?> values = SequenceFieldArgs.asRows("ARRAY_DIFFERENCE_INT", arr);
        if (values == null) {
            return null;
        }
        List<Long> out = new ArrayList<>(values.size());
        for (int i = 0; i < values.size(); i++) {
            if (i == 0) {
                out.add(0L);
                continue;
            }
            Number current = numeric("ARRAY_DIFFERENCE_INT", values.get(i), i);
            Number previous = numeric("ARRAY_DIFFERENCE_INT", values.get(i - 1), i - 1);
            out.add(current == null || previous == null
                    ? null : current.longValue() - previous.longValue());
        }
        return out;
    }

    public static List arrayCumSum(Object arr) {
        List<?> values = SequenceFieldArgs.asRows("ARRAY_CUM_SUM", arr);
        if (values == null) {
            return null;
        }
        List<Double> out = new ArrayList<>(values.size());
        double sum = 0;
        for (int i = 0; i < values.size(); i++) {
            Number value = numeric("ARRAY_CUM_SUM", values.get(i), i);
            if (value != null) {
                sum += value.doubleValue();
            }
            out.add(sum);
        }
        return out;
    }

    public static List arrayCumSumInt(Object arr) {
        List<?> values = SequenceFieldArgs.asRows("ARRAY_CUM_SUM_INT", arr);
        if (values == null) {
            return null;
        }
        List<Long> out = new ArrayList<>(values.size());
        long sum = 0;
        for (int i = 0; i < values.size(); i++) {
            Number value = numeric("ARRAY_CUM_SUM_INT", values.get(i), i);
            if (value != null) {
                sum += value.longValue();
            }
            out.add(sum);
        }
        return out;
    }

    public static List arrayCompact(Object arr) {
        List<?> values = SequenceFieldArgs.asRows("ARRAY_COMPACT", arr);
        if (values == null) {
            return null;
        }
        List<Object> out = new ArrayList<>();
        boolean first = true;
        Object previous = null;
        for (Object value : values) {
            if (first || !Objects.equals(previous, value)) {
                out.add(value);
            }
            previous = value;
            first = false;
        }
        return out;
    }

    public static List arrayDistinct(Object arr) {
        List<?> values = SequenceFieldArgs.asRows("ARRAY_DISTINCT", arr);
        if (values == null) {
            return null;
        }
        Set<Object> out = new LinkedHashSet<>(values);
        return new ArrayList<>(out);
    }

    private static Number numeric(String functionName, Object value, int index) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            return number;
        }
        throw new IllegalArgumentException(functionName + " requires numeric elements,"
                + " but position " + (index + 1) + " holds "
                + value.getClass().getSimpleName());
    }

    /** The Calcite function objects. Hand-built for the explicit ARRAY return types. */
    static List<Map.Entry<String, Function>> builtIns() {
        return List.of(
                Map.entry("ARRAY_DIFFERENCE",
                        new ArrayFunction("arrayDifference", SqlTypeName.DOUBLE)),
                Map.entry("ARRAY_DIFFERENCE_INT",
                        new ArrayFunction("arrayDifferenceInt", SqlTypeName.BIGINT)),
                Map.entry("ARRAY_CUM_SUM",
                        new ArrayFunction("arrayCumSum", SqlTypeName.DOUBLE)),
                Map.entry("ARRAY_CUM_SUM_INT",
                        new ArrayFunction("arrayCumSumInt", SqlTypeName.BIGINT)),
                Map.entry("ARRAY_COMPACT",
                        new ArrayFunction("arrayCompact", SqlTypeName.ANY)),
                Map.entry("ARRAY_DISTINCT",
                        new ArrayFunction("arrayDistinct", SqlTypeName.ANY)));
    }

    private static final class ArrayFunction
            implements ScalarFunction, ImplementableFunction {

        private final Method method;
        private final SqlTypeName elementType;

        private ArrayFunction(String methodName, SqlTypeName elementType) {
            this.method = Types.lookupMethod(
                    ArrayTransformFunctions.class, methodName, Object.class);
            this.elementType = elementType;
        }

        @Override
        public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
            RelDataType element = typeFactory.createTypeWithNullability(
                    typeFactory.createSqlType(elementType), true);
            return typeFactory.createTypeWithNullability(
                    typeFactory.createArrayType(element, -1L), true);
        }

        @Override
        public List<FunctionParameter> getParameters() {
            return SequenceFunnelFunctions.parameters(
                    new String[]{"arr"}, ordinal -> SqlTypeName.ANY);
        }

        @Override
        public CallImplementor getImplementor() {
            return RexImpTable.createImplementor(
                    new ReflectiveCallNotNullImplementor(method), NullPolicy.NONE, false);
        }
    }
}
