package com.mercari.solution.util.pipeline.udf;


import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.adapter.enumerable.ReflectiveCallNotNullImplementor;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.linq4j.tree.Expression;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.linq4j.tree.Expressions;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.linq4j.tree.Types;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexCall;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexLiteral;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlKind;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * Shared machinery for built-ins whose SQL surface takes a <em>field name</em>
 * of an array-of-rows argument ({@code SEQ_FOLD}, {@code SEQ_SPLIT}): the
 * runtime rows are positional, so the name is resolved to an ordinal at plan
 * time from the array operand's ROW type and injected as a constant. Public
 * because the generated code calls {@link #badField}.
 */
public final class SequenceFieldArgs {

    private SequenceFieldArgs() {
    }

    /**
     * Runtime trap for plan-time field-resolution failures: throwing during
     * codegen would be swallowed into Calcite's generic "Unable to implement"
     * — instead the generated code calls this in place of the field ordinal,
     * surfacing the resolution message on first evaluation (like pattern
     * errors).
     */
    public static Integer badField(String message) {
        throw new IllegalArgumentException(message);
    }

    /**
     * Coerces an ANY-typed array argument to a row list. Values arrive as the
     * static type of the call site — a typed {@code List} for array columns,
     * plain {@code Object} for ANY-typed values (an UNNEST'ed session, a
     * SEQ_COLLECT result) — so the parameter is declared {@code Object} and
     * cast here.
     */
    static List<?> asRows(String functionName, Object rows) {
        if (rows == null) {
            return null;
        }
        if (rows instanceof List<?> list) {
            return list;
        }
        throw new IllegalArgumentException(functionName + " expects an array/collection argument,"
                + " but got " + rows.getClass().getSimpleName());
    }

    /** Reads the resolved field of a collected/array row (see the field spec). */
    static Object fieldValue(Object row, Integer fieldIndex) {
        if (row == null) {
            return null;
        }
        if (fieldIndex == null || fieldIndex < 0) {
            return row;
        }
        if (row instanceof Object[] values) {
            return fieldIndex < values.length ? values[fieldIndex] : null;
        }
        if (row instanceof List<?> values) {
            return fieldIndex < values.size() ? values.get(fieldIndex) : null;
        }
        return fieldIndex == 0 ? row : null;
    }

    /**
     * Resolves the field-name argument (at {@code fieldArgIndex}, over the
     * array argument at position 0) to a component-row ordinal at plan time
     * and injects the ordinal as a constant in place of the name.
     */
    static final class FieldResolvingImplementor extends ReflectiveCallNotNullImplementor {

        private static final Method BAD_FIELD = Types.lookupMethod(
                SequenceFieldArgs.class, "badField", String.class);

        private final int fieldArgIndex;
        private final String functionName;

        FieldResolvingImplementor(Method method, int fieldArgIndex, String functionName) {
            super(method);
            this.fieldArgIndex = fieldArgIndex;
            this.functionName = functionName;
        }

        @Override
        public Expression implement(RexToLixTranslator translator, RexCall call,
                List<Expression> translatedOperands) {
            List<Expression> operands = new ArrayList<>(translatedOperands);
            try {
                operands.set(fieldArgIndex,
                        Expressions.constant(resolveFieldIndex(translator, call),
                                Integer.class));
            } catch (IllegalArgumentException e) {
                // Surface resolution failures at first evaluation with their
                // own message (a codegen throw is swallowed by Calcite).
                operands.set(fieldArgIndex,
                        Expressions.call(BAD_FIELD, Expressions.constant(e.getMessage())));
            }
            return super.implement(translator, call, operands);
        }

        private int resolveFieldIndex(RexToLixTranslator translator, RexCall call) {
            // Inside a Calc program the operands are RexLocalRefs; deref
            // expands them through the program back to the literal / the typed
            // array expression.
            RexNode fieldArg = translator.deref(call.getOperands().get(fieldArgIndex));
            if (!(fieldArg instanceof RexLiteral literal)) {
                throw new IllegalArgumentException(
                        functionName + " field must be a string literal (a ROW field name,"
                                + " '' for a scalar array, or a 0-based ordinal)");
            }
            String name = literal.getValueAs(String.class);
            if (name == null || name.isBlank()) {
                return -1;
            }
            if (name.chars().allMatch(Character::isDigit)) {
                return Integer.parseInt(name);
            }
            RexNode array = translator.deref(call.getOperands().get(0));
            while (array instanceof RexCall cast && cast.getKind() == SqlKind.CAST) {
                array = translator.deref(cast.getOperands().get(0));
            }
            RelDataType component = array.getType().getComponentType();
            if (component == null || !component.isStruct()) {
                throw new IllegalArgumentException(functionName + " field '" + name + "' requires an"
                        + " ARRAY<ROW> argument (this argument's type is "
                        + array.getType() + "); pass '' for a scalar array or a"
                        + " 0-based ordinal for untyped arrays");
            }
            int index = component.getFieldNames().indexOf(name);
            if (index < 0) {
                throw new IllegalArgumentException(functionName + " unknown field '" + name
                        + "' (fields: " + component.getFieldNames() + ")");
            }
            return index;
        }
    }
}
