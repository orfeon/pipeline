package com.mercari.solution.util.pipeline.udf;

import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.Function;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.impl.AggregateFunctionImpl;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * User-defined function registration for {@code Query2}: serializable
 * descriptors (a class name + method name travel to the workers; the Calcite
 * function objects are built reflectively at setup time) covering:
 *
 * <ul>
 *   <li><b>Scalar UDFs</b> — every public static method of the class with the
 *       given method name becomes an overload of the SQL function.</li>
 *   <li><b>Aggregate UDFs (UDAF)</b> — a class following Calcite's accumulator
 *       convention: {@code A init()}, {@code A add(A, V...)}, optional
 *       {@code A merge(A, A)}, {@code R result(A)} (public instance methods,
 *       public zero-arg constructor).</li>
 * </ul>
 *
 * <p>Register function names in UPPERCASE: the outer query resolves names
 * case-insensitively (BigQuery lex), but a function used <em>inside a
 * correlated LATERAL block</em> is re-planned from generated SQL whose
 * unquoted identifiers are uppercased — an uppercase registration matches both.
 */
public final class UserDefinedFunctions {

    /** Serializable descriptor of one UDF (scalar when {@code methodName} != null). */
    public record FunctionSpec(String name, String className, String methodName)
            implements Serializable {
    }

    private UserDefinedFunctions() {
    }

    /** A scalar UDF: all public static overloads of {@code clazz.methodName}. */
    public static FunctionSpec scalar(String name, Class<?> clazz, String methodName) {
        return new FunctionSpec(name, clazz.getName(), methodName);
    }

    /** An aggregate UDF: {@code clazz} follows the init/add[/merge]/result convention. */
    public static FunctionSpec aggregate(String name, Class<?> clazz) {
        return new FunctionSpec(name, clazz.getName(), null);
    }

    /**
     * Materializes the descriptors into Calcite functions (name → overloads),
     * prepending the built-in functions ({@link DateTimeFunctions}, the
     * {@code SEQ_*} sequence-pattern family: {@link SequenceMatchFunctions
     * SEQ_MATCH / SEQ_MATCH_STEPS}, {@link SequenceFoldFunctions SEQ_FOLD /
     * SEQ_FOLD_INT}, {@link SequenceCollectFunctions SEQ_COLLECT},
     * {@link SequenceSplitFunctions SEQ_SPLIT},
     * {@link SequenceFunnelFunctions SEQ_FUNNEL / SEQ_RETENTION}, and the
     * analytics built-ins {@link QuantileFunctions QUANTILE},
     * {@link ApproxDistinctFunctions APPROX_DISTINCT},
     * {@link ArrayTransformFunctions ARRAY_DIFFERENCE(_INT) /
     * ARRAY_CUM_SUM(_INT) / ARRAY_COMPACT / ARRAY_DISTINCT},
     * {@link TimeBucketFunctions TIME_BUCKET}, and the linear-algebra
     * built-ins {@link MatrixFunctions COSINE_SIMILARITY / MATRIX_MULTIPLY /
     * MATRIX_SOLVE / MAHALANOBIS / POLY_FIT / LINEAR_REG /
     * AS_DOUBLE_ARRAY}). For ARG_MAX / ARG_MIN use
     * Calcite's standard operators (implemented natively by the enumerable
     * runtime) — a same-name user aggregate would break overload resolution.
     */
    public static Map<String, List<Function>> build(List<FunctionSpec> specs) {
        final Map<String, List<Function>> functions = new LinkedHashMap<>();
        for (final Map.Entry<String, Function> entry : DateTimeFunctions.functions()) {
            functions.computeIfAbsent(entry.getKey(), k -> new ArrayList<>())
                    .add(entry.getValue());
        }
        final List<Map.Entry<String, Function>> sequenceFamily = new ArrayList<>();
        sequenceFamily.addAll(SequenceMatchFunctions.builtIns());
        sequenceFamily.addAll(SequenceFoldFunctions.builtIns());
        sequenceFamily.addAll(SequenceCollectFunctions.builtIns());
        sequenceFamily.addAll(SequenceSplitFunctions.builtIns());
        sequenceFamily.addAll(SequenceFunnelFunctions.builtIns());
        sequenceFamily.addAll(QuantileFunctions.builtIns());
        sequenceFamily.addAll(ApproxDistinctFunctions.builtIns());
        sequenceFamily.addAll(ArrayTransformFunctions.builtIns());
        sequenceFamily.addAll(TimeBucketFunctions.builtIns());
        sequenceFamily.addAll(MatrixFunctions.builtIns());
        for (final Map.Entry<String, Function> entry : sequenceFamily) {
            functions.computeIfAbsent(entry.getKey(), k -> new ArrayList<>())
                    .add(entry.getValue());
        }
        for (final FunctionSpec spec : specs) {
            final Class<?> clazz;
            try {
                clazz = Class.forName(spec.className());
            } catch (final ClassNotFoundException e) {
                throw new IllegalStateException(
                        "UDF class not found: " + spec.className(), e);
            }
            final List<Function> overloads =
                    functions.computeIfAbsent(spec.name(), k -> new ArrayList<>());
            if (spec.methodName() != null) {
                boolean found = false;
                for (final Method method : clazz.getMethods()) {
                    if (method.getName().equals(spec.methodName())
                            && Modifier.isStatic(method.getModifiers())) {
                        overloads.add(ScalarFunctionImpl.create(method));
                        found = true;
                    }
                }
                if (!found) {
                    throw new IllegalStateException("scalar UDF '" + spec.name()
                            + "': no public static method '" + spec.methodName()
                            + "' on " + spec.className());
                }
            } else {
                final Function aggregate = AggregateFunctionImpl.create(clazz);
                if (aggregate == null) {
                    throw new IllegalStateException("aggregate UDF '" + spec.name() + "': "
                            + spec.className() + " does not follow the accumulator convention"
                            + " (public zero-arg constructor + init/add[/merge]/result)");
                }
                overloads.add(aggregate);
            }
        }
        return functions;
    }
}
