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
import java.util.SplittableRandom;

/**
 * {@code BETA_SAMPLE} — a deterministic Beta-distribution sample, always
 * registered as a built-in. The Thompson-sampling primitive: keep per-arm
 * successes/failures in a buffer (or an external table) as plain counts and
 * sample the posterior at decision time:
 *
 * <pre>{@code
 * SELECT i.storeId, v.variant
 * FROM INPUT i, UNNEST(i.variants) AS v
 * ORDER BY BETA_SAMPLE(1 + v.successes, 1 + v.failures,
 *                      UNIX_MILLIS(i.__timestamp) + v.variantNo) DESC
 * LIMIT 1
 * }</pre>
 *
 * <p>{@code BETA_SAMPLE(alpha, beta, seed)} → {@code DOUBLE} in {@code (0, 1)}:
 * {@code alpha}/{@code beta} are the Beta shape parameters (> 0); {@code seed}
 * is any number. The sample is a pure function of the three arguments —
 * evaluations are repeatable, so bundle retries and multi-statement re-reads
 * see the same decision. Derive the seed from element data (a timestamp, an
 * id hash) and vary it per row (e.g. {@code + v.variantNo}) for independent
 * draws. NULL in → NULL out.
 *
 * <p>Sampling is via two Marsaglia–Tsang gamma draws
 * ({@code X ~ Γ(alpha), Y ~ Γ(beta), X / (X + Y)}) on a
 * {@link SplittableRandom} seeded from the arguments.
 */
public class StatFunctions {

    private StatFunctions() {
    }

    /** Reflective evaluation target. */
    public static Double betaSample(Object alpha, Object beta, Object seed) {
        if (alpha == null || beta == null || seed == null) {
            return null;
        }
        final double a = shape("alpha", alpha);
        final double b = shape("beta", beta);
        if (!(seed instanceof Number s)) {
            throw new IllegalArgumentException("BETA_SAMPLE seed must be a number, but got "
                    + seed.getClass().getSimpleName());
        }
        // Mix the shapes into the seed so equal seeds with different
        // posteriors do not walk the same uniform sequence.
        final long mixed = s.longValue()
                ^ Double.doubleToLongBits(a) * 0x9E3779B97F4A7C15L
                ^ Long.rotateLeft(Double.doubleToLongBits(b), 31);
        final SplittableRandom random = new SplittableRandom(mixed);
        final double x = gamma(random, a);
        final double y = gamma(random, b);
        if (x + y == 0d) {
            return 0.5d;
        }
        return x / (x + y);
    }

    private static double shape(String parameter, Object value) {
        if (!(value instanceof Number number)) {
            throw new IllegalArgumentException("BETA_SAMPLE " + parameter
                    + " must be a number > 0, but got " + value.getClass().getSimpleName());
        }
        final double out = number.doubleValue();
        if (!(out > 0) || Double.isInfinite(out)) {
            throw new IllegalArgumentException("BETA_SAMPLE " + parameter
                    + " must be a finite number > 0, but was " + out);
        }
        return out;
    }

    /** Marsaglia–Tsang Γ(shape, 1) sampling; shape < 1 boosts through Γ(shape + 1). */
    private static double gamma(SplittableRandom random, double shape) {
        if (shape < 1d) {
            final double boost = Math.pow(random.nextDouble(), 1d / shape);
            return gamma(random, shape + 1d) * boost;
        }
        final double d = shape - 1d / 3d;
        final double c = 1d / Math.sqrt(9d * d);
        while (true) {
            final double x = random.nextGaussian();
            final double t = 1d + c * x;
            if (t <= 0d) {
                continue;
            }
            final double v = t * t * t;
            final double u = random.nextDouble();
            if (u < 1d - 0.0331d * x * x * x * x) {
                return d * v;
            }
            if (Math.log(u) < 0.5d * x * x + d * (1d - v + Math.log(v))) {
                return d * v;
            }
        }
    }

    /** The Calcite function object. */
    static List<Map.Entry<String, Function>> builtIns() {
        return List.of(Map.entry("BETA_SAMPLE", new BetaSampleFunction()));
    }

    private static final class BetaSampleFunction
            implements ScalarFunction, ImplementableFunction {

        private static final Method METHOD = Types.lookupMethod(StatFunctions.class,
                "betaSample", Object.class, Object.class, Object.class);

        @Override
        public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(
                    typeFactory.createSqlType(SqlTypeName.DOUBLE), true);
        }

        @Override
        public List<FunctionParameter> getParameters() {
            return SequenceFunnelFunctions.parameters(
                    new String[]{"alpha", "beta", "seed"}, ordinal -> SqlTypeName.ANY);
        }

        @Override
        public CallImplementor getImplementor() {
            return RexImpTable.createImplementor(
                    new ReflectiveCallNotNullImplementor(METHOD), NullPolicy.NONE, false);
        }
    }
}
