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
 * {@code SEQ_PAGE_HINKLEY} / {@code SEQ_CUSUM} — change/drift detection over
 * an ordered sequence, always registered as built-ins. Re-evaluated per
 * element over the visible history (a buffer table's rows are already
 * event-time-ordered; order external-source rows with {@code SEQ_COLLECT}),
 * so the detection is robust to out-of-order arrival — the per-key
 * "when should this model retrain?" trigger:
 *
 * <pre>{@code
 * SELECT i.userId
 * FROM INPUT i JOIN buffer.history h ON h.userId = i.userId
 * GROUP BY i.userId
 * HAVING SEQ_PAGE_HINKLEY(SEQ_COLLECT(h.__timestamp, h.error), '', 0.005, 50.0) > 0
 * }</pre>
 *
 * <p>Both return the <b>1-based index</b> of the first sequence position where
 * the detection fired, or {@code 0} when no change was detected (NULL rows
 * argument yields NULL). {@code field} follows the {@code SEQ_FOLD} spec: a
 * ROW field name resolved at plan time, {@code ''} for a scalar array, or a
 * 0-based ordinal for untyped arrays; NULL values are skipped (they advance
 * the index but not the statistics).
 *
 * <ul>
 * <li><b>{@code SEQ_PAGE_HINKLEY(rows, field, delta, lambda)}</b> → INTEGER:
 *     two-sided Page–Hinkley test. Per value the cumulative deviation from
 *     the running mean (minus the tolerance {@code delta} ≥ 0) is tracked;
 *     a change is signalled when it leaves its running extremum by more than
 *     the threshold {@code lambda} > 0 in either direction.</li>
 * <li><b>{@code SEQ_CUSUM(rows, field, target, slack, threshold)}</b> →
 *     INTEGER: two-sided tabular CUSUM around a known {@code target} level.
 *     {@code S⁺ = max(0, S⁺ + x - target - slack)} (and mirrored {@code S⁻});
 *     a change is signalled when either side exceeds {@code threshold} > 0.
 *     {@code slack} (≥ 0, often written {@code k}) absorbs acceptable
 *     wander around the target.</li>
 * </ul>
 */
public class SequenceDriftFunctions {

    private SequenceDriftFunctions() {
    }

    /** Reflective evaluation targets; {@code fieldIndex} is injected at plan time. */
    public static Integer seqPageHinkley(Object rows, Integer fieldIndex,
            Object delta, Object lambda) {
        final double tolerance = positive("SEQ_PAGE_HINKLEY", "delta", delta, true);
        final double threshold = positive("SEQ_PAGE_HINKLEY", "lambda", lambda, false);
        final List<?> values = SequenceFieldArgs.asRows("SEQ_PAGE_HINKLEY", rows);
        if (values == null) {
            return null;
        }
        long count = 0;
        double mean = 0d;
        double cumulative = 0d;
        double minimum = 0d;
        double maximum = 0d;
        for (int i = 0; i < values.size(); i++) {
            final Double value = numeric("SEQ_PAGE_HINKLEY",
                    SequenceFieldArgs.fieldValue(values.get(i), fieldIndex), i);
            if (value == null) {
                continue;
            }
            count++;
            mean += (value - mean) / count;
            cumulative += value - mean - tolerance;
            minimum = Math.min(minimum, cumulative);
            maximum = Math.max(maximum, cumulative);
            if (cumulative - minimum > threshold || maximum - cumulative > threshold) {
                return i + 1;
            }
        }
        return 0;
    }

    public static Integer seqCusum(Object rows, Integer fieldIndex,
            Object target, Object slack, Object threshold) {
        if (!(target instanceof Number t)) {
            throw new IllegalArgumentException("SEQ_CUSUM target must be a number, but got "
                    + (target == null ? "NULL" : target.getClass().getSimpleName()));
        }
        final double center = t.doubleValue();
        final double allowance = positive("SEQ_CUSUM", "slack", slack, true);
        final double limit = positive("SEQ_CUSUM", "threshold", threshold, false);
        final List<?> values = SequenceFieldArgs.asRows("SEQ_CUSUM", rows);
        if (values == null) {
            return null;
        }
        double upper = 0d;
        double lower = 0d;
        for (int i = 0; i < values.size(); i++) {
            final Double value = numeric("SEQ_CUSUM",
                    SequenceFieldArgs.fieldValue(values.get(i), fieldIndex), i);
            if (value == null) {
                continue;
            }
            upper = Math.max(0d, upper + value - center - allowance);
            lower = Math.max(0d, lower + center - value - allowance);
            if (upper > limit || lower > limit) {
                return i + 1;
            }
        }
        return 0;
    }

    private static double positive(String functionName, String parameter, Object value,
            boolean zeroAllowed) {
        if (!(value instanceof Number number)) {
            throw new IllegalArgumentException(functionName + " " + parameter
                    + " must be a number, but got "
                    + (value == null ? "NULL" : value.getClass().getSimpleName()));
        }
        final double out = number.doubleValue();
        if (zeroAllowed ? out < 0 : out <= 0) {
            throw new IllegalArgumentException(functionName + " " + parameter + " must be "
                    + (zeroAllowed ? ">= 0" : "> 0") + ", but was " + out);
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
                Map.entry("SEQ_PAGE_HINKLEY", new SeqDriftFunction("seqPageHinkley",
                        "SEQ_PAGE_HINKLEY", "rows", "field", "delta", "lambda")),
                Map.entry("SEQ_CUSUM", new SeqDriftFunction("seqCusum",
                        "SEQ_CUSUM", "rows", "field", "target", "slack", "threshold")));
    }

    private static final class SeqDriftFunction
            implements ScalarFunction, ImplementableFunction {

        private final Method method;
        private final String functionName;
        private final String[] parameterNames;

        private SeqDriftFunction(String methodName, String functionName,
                String... parameterNames) {
            final Class<?>[] parameterTypes = new Class<?>[parameterNames.length];
            parameterTypes[0] = Object.class;
            parameterTypes[1] = Integer.class;
            for (int i = 2; i < parameterTypes.length; i++) {
                parameterTypes[i] = Object.class;
            }
            this.method = Types.lookupMethod(SequenceDriftFunctions.class, methodName,
                    parameterTypes);
            this.functionName = functionName;
            this.parameterNames = parameterNames;
        }

        @Override
        public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(
                    typeFactory.createSqlType(SqlTypeName.INTEGER), true);
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
