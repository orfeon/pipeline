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
import java.util.Locale;
import java.util.Map;

/**
 * {@code SEQ_FOLD} / {@code SEQ_FOLD_INT} — fold one field of an array-of-rows
 * slice into a scalar, always registered as built-ins. The natural companion
 * of {@code SEQ_MATCH}: aggregate a match range as a plain expression, usable
 * anywhere in SELECT / WHERE / HAVING without the
 * {@code UNNEST ... WITH ORDINALITY} + GROUP BY dance:
 *
 * <pre>{@code
 * SELECT t.userId,
 *        SEQ_FOLD_INT(t.events, m.startIdx, m.endIdx, 'amount', 'sum') AS total
 * FROM input.trails t, UNNEST(SEQ_MATCH(...)) AS m
 * WHERE SEQ_FOLD_INT(t.events, m.startIdx, m.endIdx, 'amount', 'sum') > 1000
 * }</pre>
 *
 * <p>{@code (array, from, to, field, op)}: {@code from}/{@code to} are
 * <b>1-based inclusive</b> (match indexes fit directly) and are clamped to the
 * array bounds. {@code field} is a field <em>name</em> of the array's ROW
 * component — resolved at plan time from the argument's type — or {@code ''}
 * for a scalar array (fold the element itself), or a 0-based ordinal for
 * untyped arrays. {@code op} is {@code sum} / {@code min} / {@code max} /
 * {@code count}, plus {@code avg} on {@code SEQ_FOLD} only.
 *
 * <p>SQL aggregate null semantics: NULL values are skipped; over an empty (or
 * exhausted) slice {@code count} is 0 and every other op is NULL.
 * {@code SEQ_FOLD} returns DOUBLE, {@code SEQ_FOLD_INT} returns BIGINT; both
 * fold over the whole slice — to fold only the elements a specific pattern
 * symbol matched, use {@code SEQ_MATCH_STEPS} and aggregate in SQL.
 */
public class SequenceFoldFunctions {

    private SequenceFoldFunctions() {
    }

    /** Reflective evaluation targets; {@code fieldIndex} is injected at plan time. */
    public static Double seqFold(Object rows, Integer from, Integer to, Integer fieldIndex,
            String op) {
        Fold fold = fold(rows, from, to, fieldIndex, op, true);
        if (fold == null) {
            return null;
        }
        return switch (fold.op()) {
            case "count" -> (double) fold.count();
            case "sum" -> fold.count() == 0 ? null : fold.sum();
            case "min" -> fold.count() == 0 ? null : fold.min();
            case "max" -> fold.count() == 0 ? null : fold.max();
            case "avg" -> fold.count() == 0 ? null : fold.sum() / fold.count();
            default -> null; // unreachable
        };
    }

    public static Long seqFoldInt(Object rows, Integer from, Integer to, Integer fieldIndex,
            String op) {
        Fold fold = fold(rows, from, to, fieldIndex, op, false);
        if (fold == null) {
            return null;
        }
        return switch (fold.op()) {
            case "count" -> fold.count();
            case "sum" -> fold.count() == 0 ? null : fold.longSum();
            case "min" -> fold.count() == 0 ? null : fold.longMin();
            case "max" -> fold.count() == 0 ? null : fold.longMax();
            default -> null; // unreachable
        };
    }

    private record Fold(String op, long count, double sum, double min, double max,
            long longSum, long longMin, long longMax) {
    }

    private static Fold fold(Object rowsArg, Integer from, Integer to, Integer fieldIndex,
            String op, boolean allowAvg) {
        String normalized = op == null ? "" : op.trim().toLowerCase(Locale.ROOT);
        boolean known = switch (normalized) {
            case "sum", "min", "max", "count" -> true;
            case "avg" -> allowAvg;
            default -> false;
        };
        if (!known) {
            throw new IllegalArgumentException("SEQ_FOLD op must be one of sum/min/max/count"
                    + (allowAvg ? "/avg" : " (avg needs SEQ_FOLD, which returns DOUBLE)")
                    + ", but was: '" + op + "'");
        }
        List<?> rows = SequenceFieldArgs.asRows("SEQ_FOLD", rowsArg);
        if (rows == null || from == null || to == null) {
            return null;
        }
        int start = Math.max(1, from);
        int end = Math.min(rows.size(), to);
        long count = 0;
        double sum = 0;
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        long longSum = 0;
        long longMin = Long.MAX_VALUE;
        long longMax = Long.MIN_VALUE;
        for (int i = start; i <= end; i++) {
            Object row = rows.get(i - 1);
            Object value = SequenceFieldArgs.fieldValue(row, fieldIndex);
            if (value == null) {
                continue;
            }
            if (!(value instanceof Number number)) {
                throw new IllegalArgumentException("SEQ_FOLD requires a numeric field, but position "
                        + i + " holds " + value.getClass().getSimpleName());
            }
            count++;
            double d = number.doubleValue();
            sum += d;
            min = Math.min(min, d);
            max = Math.max(max, d);
            long l = number.longValue();
            longSum += l;
            longMin = Math.min(longMin, l);
            longMax = Math.max(longMax, l);
        }
        return new Fold(normalized, count, sum, min, max, longSum, longMin, longMax);
    }

    /** The Calcite function objects, name → definition. */
    static List<Map.Entry<String, Function>> builtIns() {
        return List.of(
                Map.entry("SEQ_FOLD", new SeqFoldFunction("seqFold", SqlTypeName.DOUBLE)),
                Map.entry("SEQ_FOLD_INT", new SeqFoldFunction("seqFoldInt", SqlTypeName.BIGINT)));
    }

    private static final class SeqFoldFunction
            implements ScalarFunction, ImplementableFunction {

        private static final String[] PARAMETER_NAMES = {"rows", "from", "to", "field", "op"};

        private final Method method;
        private final SqlTypeName returnType;

        private SeqFoldFunction(String methodName, SqlTypeName returnType) {
            this.method = Types.lookupMethod(SequenceFoldFunctions.class, methodName,
                    Object.class, Integer.class, Integer.class, Integer.class, String.class);
            this.returnType = returnType;
        }

        @Override
        public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(
                    typeFactory.createSqlType(returnType), true);
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
                            case 1, 2 -> SqlTypeName.INTEGER;
                            default -> SqlTypeName.VARCHAR;
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
                    new SequenceFieldArgs.FieldResolvingImplementor(method, 3, "SEQ_FOLD"),
                    NullPolicy.NONE, false);
        }
    }

}
