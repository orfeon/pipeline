package com.mercari.solution.util.pipeline.udf;

import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.adapter.enumerable.CallImplementor;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.adapter.enumerable.NullPolicy;
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
 * {@code SEQ_EXP_SMOOTH} / {@code SEQ_HOLT} — exponential-smoothing forecasts
 * over an ordered sequence, always registered as built-ins. The lightweight
 * per-key demand-forecast models: re-fit per element over the visible history
 * (a buffer table's rows are already event-time-ordered; order
 * external-source rows with {@code SEQ_COLLECT}):
 *
 * <pre>{@code
 * SELECT i.skuId,
 *        SEQ_HOLT(SEQ_COLLECT(h.__timestamp, h.demand), 'v1', 0.4, 0.2, 7) AS demand7d
 * FROM INPUT i JOIN buffer.history h ON h.skuId = i.skuId
 * GROUP BY i.skuId
 * }</pre>
 *
 * <p>{@code field} follows the {@code SEQ_FOLD} spec: a ROW field name
 * resolved at plan time, {@code ''} for a scalar array, or a 0-based ordinal
 * for untyped arrays. NULL values are skipped; an empty (or all-NULL)
 * sequence — or a NULL rows argument — yields NULL.
 *
 * <ul>
 * <li><b>{@code SEQ_EXP_SMOOTH(rows, field, alpha)}</b> → DOUBLE: simple
 *     exponential smoothing with level weight {@code alpha} ∈ (0, 1]; returns
 *     the final smoothed level (= the one-step-ahead forecast).</li>
 * <li><b>{@code SEQ_HOLT(rows, field, alpha, beta [, horizon])}</b> → DOUBLE:
 *     Holt's linear-trend method with level weight {@code alpha} ∈ (0, 1] and
 *     trend weight {@code beta} ∈ (0, 1]; returns the forecast
 *     {@code horizon} ≥ 1 steps ahead (default 1). A single-value sequence
 *     has no trend and forecasts that value.</li>
 * </ul>
 */
public class SequenceSmoothFunctions {

    private SequenceSmoothFunctions() {
    }

    /** Reflective evaluation targets; {@code fieldIndex} is injected at plan time. */
    public static Double seqExpSmooth(Object rows, Integer fieldIndex, Object alpha) {
        final double level = weight("SEQ_EXP_SMOOTH", "alpha", alpha);
        final List<?> values = SequenceFieldArgs.asRows("SEQ_EXP_SMOOTH", rows);
        if (values == null) {
            return null;
        }
        Double smoothed = null;
        for (int i = 0; i < values.size(); i++) {
            final Double value = numeric("SEQ_EXP_SMOOTH",
                    SequenceFieldArgs.fieldValue(values.get(i), fieldIndex), i);
            if (value == null) {
                continue;
            }
            smoothed = smoothed == null ? value : level * value + (1 - level) * smoothed;
        }
        return smoothed;
    }

    public static Double seqHolt(Object rows, Integer fieldIndex, Object alpha, Object beta) {
        return seqHolt(rows, fieldIndex, alpha, beta, 1);
    }

    public static Double seqHolt(Object rows, Integer fieldIndex, Object alpha, Object beta,
            Object horizon) {
        final double levelWeight = weight("SEQ_HOLT", "alpha", alpha);
        final double trendWeight = weight("SEQ_HOLT", "beta", beta);
        if (!(horizon instanceof Number h)) {
            throw new IllegalArgumentException("SEQ_HOLT horizon must be a number >= 1, but got "
                    + (horizon == null ? "NULL" : horizon.getClass().getSimpleName()));
        }
        final long steps = h.longValue();
        if (steps < 1) {
            throw new IllegalArgumentException(
                    "SEQ_HOLT horizon must be >= 1, but was " + steps);
        }
        final List<?> values = SequenceFieldArgs.asRows("SEQ_HOLT", rows);
        if (values == null) {
            return null;
        }
        Double level = null;
        double trend = 0d;
        boolean hasTrend = false;
        for (int i = 0; i < values.size(); i++) {
            final Double value = numeric("SEQ_HOLT",
                    SequenceFieldArgs.fieldValue(values.get(i), fieldIndex), i);
            if (value == null) {
                continue;
            }
            if (level == null) {
                level = value;
            } else if (!hasTrend) {
                trend = value - level;
                level = value;
                hasTrend = true;
            } else {
                final double previous = level;
                level = levelWeight * value + (1 - levelWeight) * (level + trend);
                trend = trendWeight * (level - previous) + (1 - trendWeight) * trend;
            }
        }
        if (level == null) {
            return null;
        }
        return level + steps * trend;
    }

    private static double weight(String functionName, String parameter, Object value) {
        if (!(value instanceof Number number)) {
            throw new IllegalArgumentException(functionName + " " + parameter
                    + " must be a number in (0, 1], but got "
                    + (value == null ? "NULL" : value.getClass().getSimpleName()));
        }
        final double out = number.doubleValue();
        if (out <= 0 || out > 1) {
            throw new IllegalArgumentException(functionName + " " + parameter
                    + " must be in (0, 1], but was " + out);
        }
        return out;
    }

    private static Double numeric(String functionName, Object value, int index) {
        if (value == null) {
            return null;
        }
        if (!(value instanceof Number number)) {
            throw new IllegalArgumentException(functionName + " requires a numeric field,"
                    + " but position " + (index + 1) + " holds "
                    + value.getClass().getSimpleName());
        }
        return number.doubleValue();
    }

    /** The Calcite function objects, name → definition. */
    static List<Map.Entry<String, Function>> builtIns() {
        return List.of(
                Map.entry("SEQ_EXP_SMOOTH", new SeqSmoothFunction("seqExpSmooth",
                        "SEQ_EXP_SMOOTH", 3, "rows", "field", "alpha")),
                Map.entry("SEQ_HOLT", new SeqSmoothFunction("seqHolt",
                        "SEQ_HOLT", 4, "rows", "field", "alpha", "beta")),
                Map.entry("SEQ_HOLT", new SeqSmoothFunction("seqHolt",
                        "SEQ_HOLT", 5, "rows", "field", "alpha", "beta", "horizon")));
    }

    private static final class SeqSmoothFunction
            implements ScalarFunction, ImplementableFunction {

        private final Method method;
        private final String functionName;
        private final String[] parameterNames;

        private SeqSmoothFunction(String methodName, String functionName, int arity,
                String... parameterNames) {
            final Class<?>[] parameterTypes = new Class<?>[arity];
            parameterTypes[0] = Object.class;
            parameterTypes[1] = Integer.class;
            for (int i = 2; i < arity; i++) {
                parameterTypes[i] = Object.class;
            }
            this.method = Types.lookupMethod(SequenceSmoothFunctions.class, methodName,
                    parameterTypes);
            this.functionName = functionName;
            this.parameterNames = parameterNames;
        }

        @Override
        public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(
                    typeFactory.createSqlType(SqlTypeName.DOUBLE), true);
        }

        @Override
        public List<FunctionParameter> getParameters() {
            return SequenceFunnelFunctions.parameters(parameterNames,
                    ordinal -> ordinal == 1 ? SqlTypeName.VARCHAR : SqlTypeName.ANY);
        }

        @Override
        public CallImplementor getImplementor() {
            return RexImpTable.createImplementor(
                    new SequenceFieldArgs.FieldResolvingImplementor(method, 1, functionName),
                    NullPolicy.NONE, false);
        }
    }
}
