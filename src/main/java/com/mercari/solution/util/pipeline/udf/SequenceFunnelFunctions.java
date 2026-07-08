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
import java.util.function.IntFunction;

/**
 * {@code SEQ_FUNNEL} / {@code SEQ_RETENTION} — funnel and cohort-retention
 * analysis over a sequence, always registered as built-ins. Both take the
 * step/event conditions as a positional {@code ';'}-separated list in the
 * {@code SEQ_MATCH} DEFINE expression language (without symbol names):
 *
 * <pre>{@code
 * SELECT userId,
 *        SEQ_FUNNEL(events, 'ts,action', 'ts', 3600000,
 *          "$action = 'view'; $action = 'cart'; $action = 'buy'") AS step
 * FROM input
 * }</pre>
 *
 * <p><b>{@code SEQ_FUNNEL(rows, fields, timeField, window, steps)}</b> — the
 * sliding-window funnel (ClickHouse {@code windowFunnel} semantics): scanning
 * the sequence in array order (= time order — build with {@code SEQ_COLLECT}
 * first if unordered), it searches for chains where an event matching step 1
 * anchors a window, and each following step matches within {@code window} of
 * that anchor's time. Returns the maximum step reached ({@code 0} when no
 * step-1 event exists, {@code n} when the whole funnel completed). A row may
 * advance at most one step of a given chain; a later step-1 event re-anchors
 * a fresh window, so an expired chain does not block detection.
 * {@code timeField} names the row's time field — resolved against the
 * {@code fields} list ({@code ''} = the element itself for a scalar array; a
 * digit string = 0-based ordinal, e.g. {@code '0'} for a SEQ_COLLECT key).
 * Rows whose time value is NULL are skipped.
 *
 * <p><b>{@code SEQ_RETENTION(rows, fields, conditions)}</b> — cohort retention
 * (ClickHouse {@code retention} semantics): the first condition is the anchor;
 * the result is {@code ARRAY<BOOLEAN>} of the same length as the condition
 * list where element 1 is "some row matches condition 1" and element
 * {@code i} is "element 1 holds AND some row matches condition {@code i}".
 * There is no time/window semantics — encode period membership in the
 * conditions themselves. The result is projectable and 1-based accessible.
 *
 * <p>Both return NULL for a NULL sequence (and SEQ_FUNNEL for a NULL window);
 * an empty sequence yields step 0 / all-false. Under the BigQuery lex, write
 * the condition list as a <em>double-quoted</em> SQL string so embedded
 * string literals keep their single quotes. Condition parse errors surface at
 * first evaluation, like SEQ_MATCH pattern errors; compiled condition lists
 * are cached by their definition strings — keep fields/steps as literals.
 */
public class SequenceFunnelFunctions {

    private SequenceFunnelFunctions() {
    }

    /** Reflective evaluation target for {@code SEQ_FUNNEL}. */
    public static Integer seqFunnel(Object rowsArg, String fields, String timeField,
            Long window, String steps) {
        List<?> rows = SequenceFieldArgs.asRows("SEQ_FUNNEL", rowsArg);
        if (rows == null || window == null) {
            return null;
        }
        List<SequencePattern.Expr> conditions =
                SequencePattern.compileConditions("SEQ_FUNNEL", fields, steps);
        int timeIndex = resolveField("SEQ_FUNNEL", fields, timeField);
        int n = conditions.size();
        // chainStart[k] = anchor time (the step-1 event's time) of some chain
        // that has reached step k; kept at the latest such anchor, which gives
        // later steps the most window room.
        Long[] chainStart = new Long[n + 1];
        int best = 0;
        for (int i = 0; i < rows.size(); i++) {
            Object timeValue = SequenceFieldArgs.fieldValue(rows.get(i), timeIndex);
            if (timeValue == null) {
                continue;
            }
            if (!(timeValue instanceof Number number)) {
                throw new IllegalArgumentException("SEQ_FUNNEL requires a numeric time field,"
                        + " but position " + (i + 1) + " holds "
                        + timeValue.getClass().getSimpleName());
            }
            long ts = number.longValue();
            // Highest step first, so one row advances a chain by at most one step.
            for (int k = n; k >= 2; k--) {
                Long anchor = chainStart[k - 1];
                if (anchor == null || ts - anchor > window) {
                    continue;
                }
                if (Boolean.TRUE.equals(conditions.get(k - 1).eval(rows, i))) {
                    if (chainStart[k] == null || anchor > chainStart[k]) {
                        chainStart[k] = anchor;
                    }
                    if (k > best) {
                        best = k;
                    }
                }
            }
            if (Boolean.TRUE.equals(conditions.get(0).eval(rows, i))) {
                if (chainStart[1] == null || ts > chainStart[1]) {
                    chainStart[1] = ts;
                }
                if (best < 1) {
                    best = 1;
                }
            }
        }
        return best;
    }

    /** Reflective evaluation target for {@code SEQ_RETENTION}. */
    public static List seqRetention(Object rowsArg, String fields, String conditionList) {
        List<?> rows = SequenceFieldArgs.asRows("SEQ_RETENTION", rowsArg);
        if (rows == null) {
            return null;
        }
        List<SequencePattern.Expr> conditions =
                SequencePattern.compileConditions("SEQ_RETENTION", fields, conditionList);
        boolean[] matched = new boolean[conditions.size()];
        for (int i = 0; i < rows.size(); i++) {
            for (int c = 0; c < conditions.size(); c++) {
                if (!matched[c] && Boolean.TRUE.equals(conditions.get(c).eval(rows, i))) {
                    matched[c] = true;
                }
            }
        }
        List<Boolean> out = new ArrayList<>(conditions.size());
        out.add(matched[0]);
        for (int c = 1; c < conditions.size(); c++) {
            out.add(matched[0] && matched[c]);
        }
        return out;
    }

    /**
     * Resolves the time-field spec against the caller-supplied field list (not
     * the array's ROW type — the list is already an argument here, and it also
     * covers opaque SEQ_COLLECT sequences): a name from the list, {@code ''}
     * for the element itself, or a digit string as a 0-based ordinal.
     */
    private static int resolveField(String functionName, String fields, String fieldSpec) {
        if (fieldSpec == null || fieldSpec.isBlank()) {
            return -1;
        }
        String name = fieldSpec.trim();
        if (name.chars().allMatch(Character::isDigit)) {
            return Integer.parseInt(name);
        }
        Integer index = SequencePattern.parseFields(fields).get(name);
        if (index == null) {
            throw new IllegalArgumentException(functionName + " unknown time field '" + name
                    + "' (declared fields: " + SequencePattern.parseFields(fields).keySet()
                    + "); pass '' for a scalar array or a 0-based ordinal");
        }
        return index;
    }

    /** The Calcite function objects. Hand-built for the ANY/typed signatures. */
    static List<Map.Entry<String, Function>> builtIns() {
        return List.of(
                Map.entry("SEQ_FUNNEL", new FunnelFunction()),
                Map.entry("SEQ_RETENTION", new RetentionFunction()));
    }

    private static final class FunnelFunction
            implements ScalarFunction, ImplementableFunction {

        private static final Method METHOD = Types.lookupMethod(
                SequenceFunnelFunctions.class, "seqFunnel",
                Object.class, String.class, String.class, Long.class, String.class);

        private static final String[] PARAMETER_NAMES =
                {"rows", "fields", "timeField", "window", "steps"};

        @Override
        public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(
                    typeFactory.createSqlType(SqlTypeName.INTEGER), true);
        }

        @Override
        public List<FunctionParameter> getParameters() {
            return parameters(PARAMETER_NAMES, ordinal -> switch (ordinal) {
                case 0 -> SqlTypeName.ANY;
                case 3 -> SqlTypeName.BIGINT;
                default -> SqlTypeName.VARCHAR;
            });
        }

        @Override
        public CallImplementor getImplementor() {
            return RexImpTable.createImplementor(
                    new ReflectiveCallNotNullImplementor(METHOD), NullPolicy.NONE, false);
        }
    }

    private static final class RetentionFunction
            implements ScalarFunction, ImplementableFunction {

        private static final Method METHOD = Types.lookupMethod(
                SequenceFunnelFunctions.class, "seqRetention",
                Object.class, String.class, String.class);

        private static final String[] PARAMETER_NAMES = {"rows", "fields", "conditions"};

        @Override
        public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
            RelDataType element = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
            return typeFactory.createTypeWithNullability(
                    typeFactory.createArrayType(element, -1L), true);
        }

        @Override
        public List<FunctionParameter> getParameters() {
            return parameters(PARAMETER_NAMES,
                    ordinal -> ordinal == 0 ? SqlTypeName.ANY : SqlTypeName.VARCHAR);
        }

        @Override
        public CallImplementor getImplementor() {
            return RexImpTable.createImplementor(
                    new ReflectiveCallNotNullImplementor(METHOD), NullPolicy.NONE, false);
        }
    }

    /** Builds the boilerplate positional parameter list. */
    static List<FunctionParameter> parameters(String[] names, IntFunction<SqlTypeName> typeOf) {
        List<FunctionParameter> parameters = new ArrayList<>();
        for (int i = 0; i < names.length; i++) {
            final int ordinal = i;
            parameters.add(new FunctionParameter() {
                @Override
                public int getOrdinal() {
                    return ordinal;
                }

                @Override
                public String getName() {
                    return names[ordinal];
                }

                @Override
                public RelDataType getType(RelDataTypeFactory typeFactory) {
                    return typeFactory.createTypeWithNullability(
                            typeFactory.createSqlType(typeOf.apply(ordinal)), true);
                }

                @Override
                public boolean isOptional() {
                    return false;
                }
            });
        }
        return parameters;
    }
}
