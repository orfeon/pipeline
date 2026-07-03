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
import java.util.List;

/**
 * {@code SEQ_MATCH} — sequence-pattern matching over an array column (the
 * bounded, per-element replacement for {@code MATCH_RECOGNIZE}, which the
 * underlying engine cannot execute):
 *
 * <pre>{@code
 * SELECT i.userId, m.matchNo, m.startIdx, m.endIdx,
 *        i.events[m.endIdx].amount AS lastAmount
 * FROM INPUT AS i,
 * UNNEST(SEQ_MATCH(
 *   i.events,                        -- ARRAY<ROW> (array order = sequence order)
 *   'seq,amount',                    -- row field names, in row order
 *   'STRT UP{2,}',                   -- pattern: symbols with ? * + {n[,m]} | ( )
 *   'UP: $amount > PREV($amount)'    -- symbol predicates (undefined = always true)
 * )) AS m
 * }</pre>
 *
 * <p>Returns {@code ARRAY<ROW(matchNo INT, startIdx INT, endIdx INT)>} with
 * <b>1-based</b> indexes (aligned with SQL array indexing, so
 * {@code arr[m.startIdx]} addresses the first matched element). Matches are
 * non-overlapping and longest-per-start; an empty or null array (or no match)
 * yields an empty array — {@code UNNEST} then produces no rows (wrap in a
 * LATERAL LEFT JOIN to keep the input row). See {@link SequencePattern} for
 * the pattern / predicate language.
 */
public class SequenceMatchFunctions {

    private SequenceMatchFunctions() {
    }

    /** The reflective evaluation target (rows arrive as the Calcite-internal List of Object[]). */
    public static List seqMatch(List rows, String fields, String pattern, String define) {
        if (rows == null || rows.isEmpty() || pattern == null) {
            return List.of();
        }
        final SequencePattern compiled = SequencePattern.compile(
                fields == null ? "" : fields, pattern, define == null ? "" : define);
        final List<Object[]> out = new ArrayList<>();
        int matchNo = 1;
        for (final int[] range : compiled.match(rows)) {
            out.add(new Object[]{matchNo++, range[0] + 1, range[1] + 1});
        }
        return out;
    }

    /**
     * The Calcite function object. Hand-built (not {@code ScalarFunctionImpl})
     * because the return type — {@code ARRAY<ROW(matchNo, startIdx, endIdx)>} —
     * cannot be derived from the reflective {@code List} return type.
     */
    static Function function() {
        return new SeqMatchFunction();
    }

    private static final class SeqMatchFunction
            implements ScalarFunction, ImplementableFunction {

        private static final Method METHOD = Types.lookupMethod(SequenceMatchFunctions.class,
                "seqMatch", List.class, String.class, String.class, String.class);

        private static final String[] PARAMETER_NAMES = {"rows", "fields", "pattern", "define"};

        @Override
        public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
            final RelDataType row = typeFactory.createStructType(
                    List.of(typeFactory.createSqlType(SqlTypeName.INTEGER),
                            typeFactory.createSqlType(SqlTypeName.INTEGER),
                            typeFactory.createSqlType(SqlTypeName.INTEGER)),
                    List.of("matchNo", "startIdx", "endIdx"));
            return typeFactory.createTypeWithNullability(
                    typeFactory.createArrayType(row, -1L), true);
        }

        @Override
        public List<FunctionParameter> getParameters() {
            final List<FunctionParameter> parameters = new ArrayList<>();
            for (int i = 0; i < PARAMETER_NAMES.length; i++) {
                final int ordinal = i;
                parameters.add(new FunctionParameter() {
                    @Override
                    public int getOrdinal() {
                        return ordinal;
                    }

                    @Override
                    public String getName() {
                        return PARAMETER_NAMES[ordinal];
                    }

                    @Override
                    public RelDataType getType(RelDataTypeFactory typeFactory) {
                        return typeFactory.createTypeWithNullability(
                                ordinal == 0
                                        ? typeFactory.createSqlType(SqlTypeName.ANY)
                                        : typeFactory.createSqlType(SqlTypeName.VARCHAR),
                                true);
                    }

                    @Override
                    public boolean isOptional() {
                        return false;
                    }
                });
            }
            return parameters;
        }

        @Override
        public CallImplementor getImplementor() {
            return RexImpTable.createImplementor(
                    new ReflectiveCallNotNullImplementor(METHOD), NullPolicy.NONE, false);
        }
    }
}
