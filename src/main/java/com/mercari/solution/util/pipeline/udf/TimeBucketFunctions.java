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
 * {@code TIME_BUCKET} — floor a timestamp to a fixed-size bucket, always
 * registered as a built-in. Fills the gap left by unit-based truncation
 * ({@code FLOOR(ts TO unit)} / {@code TIMESTAMP_TRUNC}), which cannot express
 * "5 minutes":
 *
 * <pre>{@code
 * SELECT TIME_BUCKET(ts, 300000) AS bucket, COUNT(*) AS hits
 * FROM input GROUP BY TIME_BUCKET(ts, 300000)
 * -- or with an interval literal:  TIME_BUCKET(ts, INTERVAL '5' MINUTE)
 * }</pre>
 *
 * <p>{@code TIME_BUCKET(ts, size)} → {@code TIMESTAMP}: both arguments are
 * numeric under the hood — {@code ts} is a TIMESTAMP column (internally epoch
 * millis) or any epoch-millis number, {@code size} is a bucket width in
 * millis (day-time {@code INTERVAL} literals work — their internal value is
 * millis; year-month intervals do not). The result is the greatest multiple
 * of {@code size} ≤ {@code ts} (floor division, correct for pre-epoch
 * values). NULL in → NULL out; a non-positive size is an error.
 */
public class TimeBucketFunctions {

    private TimeBucketFunctions() {
    }

    /** Reflective evaluation target (TIMESTAMP arrives as internal epoch millis). */
    public static Long timeBucket(Object ts, Object size) {
        if (ts == null || size == null) {
            return null;
        }
        if (!(ts instanceof Number time)) {
            throw new IllegalArgumentException("TIME_BUCKET timestamp must be a TIMESTAMP or"
                    + " epoch-millis number, but got " + ts.getClass().getSimpleName());
        }
        if (!(size instanceof Number width)) {
            throw new IllegalArgumentException("TIME_BUCKET size must be a millis number or a"
                    + " day-time INTERVAL, but got " + size.getClass().getSimpleName());
        }
        long bucket = width.longValue();
        if (bucket <= 0) {
            throw new IllegalArgumentException(
                    "TIME_BUCKET size must be positive, but was " + bucket);
        }
        return Math.floorDiv(time.longValue(), bucket) * bucket;
    }

    /** The Calcite function object. Hand-built for the TIMESTAMP return type. */
    static List<Map.Entry<String, Function>> builtIns() {
        return List.of(Map.entry("TIME_BUCKET", new TimeBucketFunction()));
    }

    private static final class TimeBucketFunction
            implements ScalarFunction, ImplementableFunction {

        private static final Method METHOD = Types.lookupMethod(
                TimeBucketFunctions.class, "timeBucket", Object.class, Object.class);

        @Override
        public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(
                    typeFactory.createSqlType(SqlTypeName.TIMESTAMP), true);
        }

        @Override
        public List<FunctionParameter> getParameters() {
            return SequenceFunnelFunctions.parameters(
                    new String[]{"ts", "size"}, ordinal -> SqlTypeName.ANY);
        }

        @Override
        public CallImplementor getImplementor() {
            return RexImpTable.createImplementor(
                    new ReflectiveCallNotNullImplementor(METHOD), NullPolicy.NONE, false);
        }
    }
}
