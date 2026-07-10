package com.mercari.solution.util.pipeline.aggregation;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.ExpressionUtil;
import com.mercari.solution.util.domain.math.MatrixOps;
import com.mercari.solution.util.pipeline.Filter;
import com.mercari.solution.util.pipeline.select.stateful.StatefulFunction;
import com.mercari.solution.util.schema.ElementSchemaUtil;
import net.objecthunter.exp4j.Expression;
import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Multivariate linear regression ({@code op: regression}) — the
 * multiple-explanatory-variable extension of {@link SimpleRegression}, sharing
 * its numerics with the query module's {@code LINEAR_REG} built-in through
 * {@link MatrixOps}: the accumulator holds the Gram matrix {@code X^T X} and
 * {@code X^T y} (flattened to plain double lists so the Avro union
 * accumulator coder handles them, additively mergeable, constant size per
 * group), and {@code extractOutput} solves the normal equations — with an
 * optional ridge penalty — via Cholesky/SVD.
 *
 * <pre>{@code
 * { "name": "model", "op": "regression", "field": "sales",
 *   "xFields": ["price", "temperature"], "ridge": 0.1 }
 * }</pre>
 *
 * <p>Output element: {@code Coefficients} (ARRAY&lt;FLOAT64&gt; —
 * {@code [intercept, b1, ..., bk]} in {@code xFields} order when
 * {@code hasIntercept}, otherwise {@code [b1, ..., bk]}), {@code RMSE}
 * and {@code N}. Rows where y or any x is null are skipped.
 */
public class Regression implements AggregateFunction {

    private List<Schema.Field> inputFields;
    private Schema.FieldType outputFieldType;

    private String name;
    private String field;
    private String expression;
    private List<String> xFields;
    private Boolean hasIntercept;
    private Double ridge;
    private String condition;

    private Range range;

    private Boolean ignore;

    private String accumKeyCountName;
    private String accumKeySumYYName;
    private String accumKeyXtxName;
    private String accumKeyXtyName;

    private transient Expression exp;
    private transient Set<String> variables;

    private transient Filter.ConditionNode conditionNode;

    public static Regression of(
            final String name,
            final List<Schema.Field> inputFields,
            final String field,
            final String expression,
            final String condition,
            final Range range,
            final Boolean ignore,
            final JsonObject params) {

        final Regression regression = new Regression();
        regression.name = name;
        regression.field = field;
        regression.expression = expression;
        regression.condition = condition;
        regression.range = range;
        regression.ignore = ignore;

        regression.accumKeyCountName = name + ".count";
        regression.accumKeySumYYName = name + ".sumYY";
        regression.accumKeyXtxName = name + ".xtx";
        regression.accumKeyXtyName = name + ".xty";

        regression.inputFields = new ArrayList<>();

        if(field != null) {
            final Schema.FieldType inputFieldType = ElementSchemaUtil.getInputFieldType(field, inputFields);
            regression.inputFields.add(Schema.Field.of(field, inputFieldType));
        } else if(expression != null) {
            for(final String variable : ExpressionUtil.estimateVariables(expression)) {
                final Schema.FieldType inputFieldType = ElementSchemaUtil.getInputFieldType(variable, inputFields);
                regression.inputFields.add(Schema.Field.of(variable, inputFieldType));
            }
        }

        regression.xFields = new ArrayList<>();
        if(params.has("xFields") && params.get("xFields").isJsonArray()) {
            for(final JsonElement xField : params.get("xFields").getAsJsonArray()) {
                final String xFieldName = xField.getAsString();
                regression.xFields.add(xFieldName);
                final Schema.FieldType inputFieldType = ElementSchemaUtil.getInputFieldType(xFieldName, inputFields);
                regression.inputFields.add(Schema.Field.of(xFieldName, inputFieldType));
            }
        }

        if(params.has("hasIntercept")) {
            regression.hasIntercept = params.get("hasIntercept").getAsBoolean();
        } else {
            regression.hasIntercept = true;
        }

        if(params.has("ridge")) {
            regression.ridge = params.get("ridge").getAsDouble();
        } else {
            regression.ridge = 0D;
        }

        regression.outputFieldType = Schema.FieldType.element(List.of(
                Schema.Field.of("Coefficients", Schema.FieldType.array(Schema.FieldType.FLOAT64).withNullable(true)),
                Schema.Field.of("RMSE", Schema.FieldType.FLOAT64.withNullable(true)),
                Schema.Field.of("N", Schema.FieldType.INT64.withNullable(true))
        ));

        return regression;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public boolean ignore() {
        return Optional.ofNullable(this.ignore).orElse(false);
    }

    @Override
    public Boolean filter(final MElement element) {
        return StatefulFunction.filter(conditionNode, element);
    }

    @Override
    public Range getRange() {
        return range;
    }

    @Override
    public List<String> validate(int parent, int index) {
        final List<String> errorMessages = new ArrayList<>();
        if(this.field == null && this.expression == null) {
            errorMessages.add("aggregations[" + parent + "].fields[" + index + "].field or expression must not be null");
        }
        if(this.xFields == null || this.xFields.isEmpty()) {
            errorMessages.add("aggregations[" + parent + "].fields[" + index + "].xFields must not be empty");
        }
        if(this.ridge != null && this.ridge < 0) {
            errorMessages.add("aggregations[" + parent + "].fields[" + index + "].ridge must be >= 0");
        }
        return errorMessages;
    }

    @Override
    public void setup() {
        if(this.expression != null) {
            final Set<String> variables = ExpressionUtil.estimateVariables(this.expression);
            this.variables = variables;
            this.exp = ExpressionUtil.createDefaultExpression(this.expression, variables);
        }
        if(this.condition != null) {
            this.conditionNode = Filter.parse(new Gson().fromJson(this.condition, JsonElement.class));
        }
    }

    @Override
    public Object apply(Map<String, Object> input, Instant timestamp) {
        return null;
    }

    @Override
    public List<Schema.Field> getInputFields() {
        return inputFields;
    }

    @Override
    public Schema.FieldType getOutputFieldType() {
        return outputFieldType;
    }

    @Override
    public Accumulator addInput(final Accumulator accumulator, final MElement input, final Integer co, final Instant timestamp) {
        final Double y;
        if(field != null) {
            y = input.getAsDouble(field);
        } else {
            y = ExpressionUtil.eval(this.exp, variables, input);
        }
        if(y == null) {
            return accumulator;
        }

        final int dimension = dimension();
        final double[] row = new double[dimension];
        int offset = 0;
        if(hasIntercept) {
            row[offset++] = 1D;
        }
        for(final String xField : xFields) {
            final Double x = input.getAsDouble(xField);
            if(x == null) {
                return accumulator;
            }
            row[offset++] = x;
        }

        final List<Double> xtx = doubles(accumulator.getList(accumKeyXtxName), dimension * dimension);
        final List<Double> xty = doubles(accumulator.getList(accumKeyXtyName), dimension);
        for(int i = 0; i < dimension; i++) {
            for(int j = 0; j < dimension; j++) {
                xtx.set(i * dimension + j, xtx.get(i * dimension + j) + row[i] * row[j]);
            }
            xty.set(i, xty.get(i) + row[i] * y);
        }

        accumulator.put(accumKeyXtxName, xtx);
        accumulator.put(accumKeyXtyName, xty);
        accumulator.put(accumKeySumYYName, getDouble(accumulator, accumKeySumYYName, 0D) + y * y);
        accumulator.put(accumKeyCountName, getDouble(accumulator, accumKeyCountName, 0D) + 1);

        return accumulator;
    }

    @Override
    public Accumulator addInput(final Accumulator accumulator, final MElement input) {
        return addInput(accumulator, input, null, null);
    }

    @Override
    public Accumulator mergeAccumulator(final Accumulator base, final Accumulator input) {
        final double inputCount = getDouble(input, accumKeyCountName, 0D);
        if(inputCount == 0) {
            return base;
        }
        final double baseCount = getDouble(base, accumKeyCountName, 0D);
        if(baseCount == 0) {
            base.put(accumKeyXtxName, doubles(input.getList(accumKeyXtxName), dimension() * dimension()));
            base.put(accumKeyXtyName, doubles(input.getList(accumKeyXtyName), dimension()));
        } else {
            final int dimension = dimension();
            final List<Double> baseXtx = doubles(base.getList(accumKeyXtxName), dimension * dimension);
            final List<Double> baseXty = doubles(base.getList(accumKeyXtyName), dimension);
            final List<Double> inputXtx = doubles(input.getList(accumKeyXtxName), dimension * dimension);
            final List<Double> inputXty = doubles(input.getList(accumKeyXtyName), dimension);
            for(int i = 0; i < baseXtx.size(); i++) {
                baseXtx.set(i, baseXtx.get(i) + inputXtx.get(i));
            }
            for(int i = 0; i < baseXty.size(); i++) {
                baseXty.set(i, baseXty.get(i) + inputXty.get(i));
            }
            base.put(accumKeyXtxName, baseXtx);
            base.put(accumKeyXtyName, baseXty);
        }
        base.put(accumKeySumYYName, getDouble(base, accumKeySumYYName, 0D) + getDouble(input, accumKeySumYYName, 0D));
        base.put(accumKeyCountName, baseCount + inputCount);
        return base;
    }

    @Override
    public Object extractOutput(final Accumulator accumulator,
                                final Map<String, Object> values) {

        final Map<String, Object> output = new HashMap<>();
        final long count = (long) getDouble(accumulator, accumKeyCountName, 0D);
        output.put("N", count);
        if(count == 0) {
            output.put("Coefficients", null);
            output.put("RMSE", null);
            return output;
        }

        final int dimension = dimension();
        final List<Double> xtxList = doubles(accumulator.getList(accumKeyXtxName), dimension * dimension);
        final List<Double> xtyList = doubles(accumulator.getList(accumKeyXtyName), dimension);
        final double[][] xtx = new double[dimension][dimension];
        final double[] xty = new double[dimension];
        for(int i = 0; i < dimension; i++) {
            for(int j = 0; j < dimension; j++) {
                xtx[i][j] = xtxList.get(i * dimension + j);
            }
            xty[i] = xtyList.get(i);
        }

        final double[] beta = MatrixOps.solveGram(xtx, xty, Optional.ofNullable(ridge).orElse(0D));
        output.put("Coefficients", MatrixOps.toList(beta));

        // SSE = y^T y - 2 b^T X^T y + b^T (X^T X) b, from the same accumulated sums.
        double sse = getDouble(accumulator, accumKeySumYYName, 0D);
        for(int i = 0; i < dimension; i++) {
            sse -= 2 * beta[i] * xty[i];
            for(int j = 0; j < dimension; j++) {
                sse += beta[i] * xtx[i][j] * beta[j];
            }
        }
        output.put("RMSE", Math.sqrt(Math.max(0D, sse) / count));

        return output;
    }

    private int dimension() {
        return xFields.size() + (hasIntercept ? 1 : 0);
    }

    /** Accumulator lists round-trip through the Avro union coder as {@code List<Object>} of numbers. */
    private static List<Double> doubles(final List<Object> values, final int size) {
        final List<Double> out = new ArrayList<>(size);
        if(values == null || values.isEmpty()) {
            for(int i = 0; i < size; i++) {
                out.add(0D);
            }
            return out;
        }
        if(values.size() != size) {
            throw new IllegalStateException("regression accumulator size mismatch: expected "
                    + size + " but got " + values.size());
        }
        for(final Object value : values) {
            out.add(((Number) value).doubleValue());
        }
        return out;
    }

    private static double getDouble(final Accumulator input, final String keyName, final Double defaultValue) {
        return Optional.ofNullable(input.getAsDouble(keyName)).orElse(defaultValue);
    }

}
