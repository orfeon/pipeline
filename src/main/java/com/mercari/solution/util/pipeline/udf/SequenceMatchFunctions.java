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
import java.util.Map;

/**
 * {@code SEQ_MATCH} / {@code SEQ_MATCH_STEPS} — sequence-pattern matching over
 * an array column (the bounded, per-element replacement for
 * {@code MATCH_RECOGNIZE}, which the underlying engine cannot execute):
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
 * <p>{@code SEQ_MATCH} returns
 * {@code ARRAY<ROW(matchNo INT, startIdx INT, endIdx INT)>} — one row per
 * match. {@code SEQ_MATCH_STEPS} returns
 * {@code ARRAY<ROW(matchNo INT, idx INT, symbol VARCHAR)>} — one row per
 * <em>matched array element</em>, exposing which pattern symbol each element
 * matched (e.g. filter {@code WHERE s.symbol = 'BUY'} to aggregate exactly the
 * matched purchases). All indexes are <b>1-based</b> (aligned with SQL array
 * indexing).
 *
 * <p>Both take an optional trailing <b>mode</b> argument selecting the match
 * set: {@code 'longest'} (default — longest per start position,
 * non-overlapping), {@code 'shortest'} (pins the first occurrence),
 * {@code 'all'} (every distinct range), {@code 'overlap'} (longest per start,
 * overlaps allowed — every trigger evaluates). An empty or null array (or no
 * match) yields an empty array — {@code UNNEST} then produces no rows (wrap in
 * a LATERAL LEFT JOIN to keep the input row). See {@link SequencePattern} for
 * the pattern / predicate language.
 */
public class SequenceMatchFunctions {

    private SequenceMatchFunctions() {
    }

    /**
     * Reflective evaluation targets. The array parameter is {@code Object}:
     * ANY-typed values (an UNNEST'ed session, a SEQ_COLLECT result) reach the
     * method as {@code Object}-typed expressions and Janino will not call a
     * {@code List}-typed parameter with them.
     */
    public static List seqMatch(Object rows, String fields, String pattern, String define) {
        return seqMatch(rows, fields, pattern, define, null);
    }

    public static List seqMatch(Object rows, String fields, String pattern, String define,
            String mode) {
        final List<Object[]> out = new ArrayList<>();
        int matchNo = 1;
        for (final SequencePattern.Match match : matches(rows, fields, pattern, define, mode)) {
            out.add(new Object[]{matchNo++, match.start() + 1, match.end() + 1});
        }
        return out;
    }

    public static List seqMatchSteps(Object rows, String fields, String pattern, String define) {
        return seqMatchSteps(rows, fields, pattern, define, null);
    }

    public static List seqMatchSteps(Object rows, String fields, String pattern, String define,
            String mode) {
        final List<Object[]> out = new ArrayList<>();
        int matchNo = 1;
        for (final SequencePattern.Match match : matches(rows, fields, pattern, define, mode)) {
            for (int i = 0; i < match.indexes().size(); i++) {
                out.add(new Object[]{
                        matchNo, match.indexes().get(i) + 1, match.symbols().get(i)});
            }
            matchNo++;
        }
        return out;
    }

    private static List<SequencePattern.Match> matches(Object rowsArg, String fields,
            String pattern, String define, String mode) {
        final List<?> rows = SequenceFieldArgs.asRows("SEQ_MATCH", rowsArg);
        if (rows == null || rows.isEmpty() || pattern == null) {
            return List.of();
        }
        final SequencePattern compiled = SequencePattern.compile(
                fields == null ? "" : fields, pattern, define == null ? "" : define);
        return compiled.match(rows, SequencePattern.Mode.of(mode));
    }

    /**
     * The Calcite function objects, name → overloads. Hand-built (not
     * {@code ScalarFunctionImpl}) because the return types —
     * {@code ARRAY<ROW(...)>} — cannot be derived from the reflective
     * {@code List} return type.
     */
    static List<Map.Entry<String, Function>> builtIns() {
        return List.of(
                Map.entry("SEQ_MATCH", new SeqFunction("seqMatch", 4, false)),
                Map.entry("SEQ_MATCH", new SeqFunction("seqMatch", 5, false)),
                Map.entry("SEQ_MATCH_STEPS", new SeqFunction("seqMatchSteps", 4, true)),
                Map.entry("SEQ_MATCH_STEPS", new SeqFunction("seqMatchSteps", 5, true)));
    }

    private static final class SeqFunction
            implements ScalarFunction, ImplementableFunction {

        private static final String[] PARAMETER_NAMES =
                {"rows", "fields", "pattern", "define", "mode"};

        private final Method method;
        private final int parameterCount;
        private final boolean steps;

        private SeqFunction(String methodName, int parameterCount, boolean steps) {
            final Class<?>[] parameterTypes = new Class<?>[parameterCount];
            parameterTypes[0] = Object.class;
            for (int i = 1; i < parameterCount; i++) {
                parameterTypes[i] = String.class;
            }
            this.method = Types.lookupMethod(
                    SequenceMatchFunctions.class, methodName, parameterTypes);
            this.parameterCount = parameterCount;
            this.steps = steps;
        }

        @Override
        public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
            final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
            final RelDataType row = steps
                    ? typeFactory.createStructType(
                            List.of(intType, intType,
                                    typeFactory.createSqlType(SqlTypeName.VARCHAR)),
                            List.of("matchNo", "idx", "symbol"))
                    : typeFactory.createStructType(
                            List.of(intType, intType, intType),
                            List.of("matchNo", "startIdx", "endIdx"));
            return typeFactory.createTypeWithNullability(
                    typeFactory.createArrayType(row, -1L), true);
        }

        @Override
        public List<FunctionParameter> getParameters() {
            final List<FunctionParameter> parameters = new ArrayList<>();
            for (int i = 0; i < parameterCount; i++) {
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
                    new ReflectiveCallNotNullImplementor(method), NullPolicy.NONE, false);
        }
    }
}
