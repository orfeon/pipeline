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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * {@code SEQ_SPLIT} — split an event array into <em>sessions</em> by a time
 * gap, always registered as a built-in. Separates the sessionization concern
 * from the pattern: instead of threading {@code $ts - PREV($ts)} constraints
 * through every pattern symbol, split first and match per session:
 *
 * <pre>{@code
 * SELECT t.userId, s.sessionNo, m.matchNo
 * FROM input.trails t,
 * UNNEST(SEQ_SPLIT(t.events, 'ts', 1800000))            -- 30-min session gap
 *   WITH ORDINALITY AS s(sess, sessionNo),
 * UNNEST(SEQ_MATCH(s.sess, 'ts,action,amount', '...', '...')) AS m
 * }</pre>
 *
 * <p>{@code (array, field, gap)}: a new session starts whenever the
 * {@code field} value of an element exceeds the previous element's by more
 * than {@code gap} (or either is NULL). The array's order is taken as the
 * time order — sort first (ship it sorted, or build it with
 * {@code SEQ_COLLECT}) if it isn't. The field spec is the same as
 * {@code SEQ_FOLD}'s: a ROW field name resolved at plan time, {@code ''} for
 * a scalar array, or a 0-based ordinal for untyped arrays (e.g. a
 * {@code SEQ_COLLECT} result).
 *
 * <p>Returns {@code ARRAY<ANY>}: each element is one session — an opaque row
 * list preserving the input rows — consumable by {@code SEQ_MATCH} /
 * {@code SEQ_MATCH_STEPS} (ANY-typed array argument) and {@code SEQ_FOLD}
 * with an ordinal field spec. An empty or NULL array (or NULL gap) yields an
 * empty array.
 */
public class SequenceSplitFunctions {

    private SequenceSplitFunctions() {
    }

    /** The reflective evaluation target; {@code fieldIndex} is injected at plan time. */
    public static List seqSplit(Object rowsArg, Integer fieldIndex, Long gap) {
        List<?> rows = SequenceFieldArgs.asRows("SEQ_SPLIT", rowsArg);
        if (rows == null || rows.isEmpty() || gap == null) {
            return List.of();
        }
        List<List<Object>> sessions = new ArrayList<>();
        Session current = new Session();
        Number previous = null;
        for (Object row : rows) {
            Object value = SequenceFieldArgs.fieldValue(row, fieldIndex);
            if (value != null && !(value instanceof Number)) {
                throw new IllegalArgumentException("SEQ_SPLIT requires a numeric field, but found "
                        + value.getClass().getSimpleName());
            }
            Number ts = (Number) value;
            boolean split = !current.isEmpty()
                    && (ts == null || previous == null
                            || ts.doubleValue() - previous.doubleValue() > gap);
            if (split) {
                sessions.add(current);
                current = new Session();
            }
            current.add(row);
            previous = ts;
        }
        if (!current.isEmpty()) {
            sessions.add(current);
        }
        return sessions;
    }

    /**
     * A session row list that is {@link Comparable}: {@code UNNEST}'s
     * flat-product runtime casts scalar array elements to {@code Comparable}
     * when wrapping them into result rows (it never actually compares them),
     * so a plain {@code ArrayList} session would fail that cast.
     */
    static final class Session extends ArrayList<Object> implements Comparable<Object> {
        @Override
        public int compareTo(Object other) {
            return 0; // never invoked by the flat-product path
        }
    }

    /** The Calcite function object. */
    static List<Map.Entry<String, Function>> builtIns() {
        return List.of(Map.entry("SEQ_SPLIT", new SeqSplitFunction()));
    }

    private static final class SeqSplitFunction
            implements ScalarFunction, ImplementableFunction {

        private static final Method METHOD = Types.lookupMethod(SequenceSplitFunctions.class,
                "seqSplit", Object.class, Integer.class, Long.class);

        private static final String[] PARAMETER_NAMES = {"rows", "field", "gap"};

        @Override
        public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
            RelDataType session = typeFactory.createTypeWithNullability(
                    typeFactory.createSqlType(SqlTypeName.ANY), true);
            return typeFactory.createTypeWithNullability(
                    typeFactory.createArrayType(session, -1L), true);
        }

        @Override
        public List<FunctionParameter> getParameters() {
            List<FunctionParameter> parameters = new ArrayList<>();
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
                        SqlTypeName type = switch (ordinal) {
                            case 0 -> SqlTypeName.ANY;
                            case 1 -> SqlTypeName.VARCHAR;
                            default -> SqlTypeName.BIGINT;
                        };
                        return typeFactory.createTypeWithNullability(
                                typeFactory.createSqlType(type), true);
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
                    new SequenceFieldArgs.FieldResolvingImplementor(METHOD, 1, "SEQ_SPLIT"),
                    NullPolicy.NONE, false);
        }
    }
}
