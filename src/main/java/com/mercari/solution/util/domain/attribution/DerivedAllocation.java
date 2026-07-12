package com.mercari.solution.util.domain.attribution;

import net.objecthunter.exp4j.Expression;
import net.objecthunter.exp4j.ExpressionBuilder;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Converts a derived measure {@code h(x1..xK)} into pseudo per-leaf baseline/target vectors so the
 * localization algorithms never need to know about derived measures.
 *
 * <ul>
 *   <li>{@code gre} (default) — leaf-level evaluation of h for deviation scoring plus the
 *       generalized ripple-effect explanatory power: the exact effect on the global value of
 *       substituting one leaf's components, {@code h(F + δ(leaf)) - h(F)}, normalized to sum 1.
 *       For {@code h = a/b} this reproduces the RiskLoc reference implementation's derived EP
 *       (element_scores.add_explanatory_power with derived=True).</li>
 *   <li>{@code partialDerivative} — Adtributor's (NSDI 2014 §4.3) finite-difference linearization:
 *       pseudo columns are gradient-weighted sums of the component columns.</li>
 *   <li>{@code shapley} — exact Shapley value over the component variables (K ≤ {@value #MAX_SHAPLEY_VARIABLES})
 *       evaluated at global totals, distributed to leaves proportionally to each component's leaf delta.
 *       The pseudo delta satisfies the efficiency axiom: it sums to {@code h(V) - h(F)}.</li>
 * </ul>
 */
public final class DerivedAllocation {

    public static final int MAX_SHAPLEY_VARIABLES = 10;

    public enum Method {
        partialDerivative,
        gre,
        shapley
    }

    private DerivedAllocation() {
    }

    public static MeasureVector allocate(final Method method, final LeafTable table, final MeasureSpec measure) {
        if(!MeasureSpec.Type.derived.equals(measure.type())) {
            throw new IllegalArgumentException("measure " + measure.name() + " is not derived");
        }
        final Expression expression = createExpression(measure);
        return switch (method) {
            case gre -> gre(expression, table, measure);
            case partialDerivative -> partialDerivative(expression, table, measure);
            case shapley -> shapley(expression, table, measure);
        };
    }

    /** Evaluates the derived expression at the given component values (0 when non-finite). */
    public static double evaluate(final MeasureSpec measure, final Map<String, Double> componentValues) {
        final Expression expression = createExpression(measure);
        return evalSafe(expression, measure.variables(), componentValues);
    }

    private static MeasureVector gre(final Expression expression, final LeafTable table, final MeasureSpec measure) {
        final List<String> variables = measure.variables();
        final int leafCount = table.leafCount();
        final int k = variables.size();
        final int[] columns = columnIndexes(table, variables);
        final double[] totalsF = new double[k];
        final double[] totalsV = new double[k];
        for(int i = 0; i < k; i++) {
            totalsF[i] = table.baselineTotal(columns[i]);
            totalsV[i] = table.targetTotal(columns[i]);
        }
        final double globalValue = eval(expression, variables, totalsF);

        final double[] pseudoBaseline = new double[leafCount];
        final double[] pseudoTarget = new double[leafCount];
        final double[] effects = new double[leafCount];
        final double[] leafValues = new double[k];
        final double[] substituted = new double[k];
        double effectSum = 0;
        for(int leaf = 0; leaf < leafCount; leaf++) {
            for(int i = 0; i < k; i++) {
                leafValues[i] = table.baselineValue(columns[i], leaf);
            }
            pseudoBaseline[leaf] = eval(expression, variables, leafValues);
            for(int i = 0; i < k; i++) {
                substituted[i] = totalsF[i] + table.targetValue(columns[i], leaf) - leafValues[i];
                leafValues[i] = table.targetValue(columns[i], leaf);
            }
            pseudoTarget[leaf] = eval(expression, variables, leafValues);
            effects[leaf] = eval(expression, variables, substituted) - globalValue;
            effectSum += effects[leaf];
        }

        final double[] ep = new double[leafCount];
        if(effectSum != 0) {
            for(int leaf = 0; leaf < leafCount; leaf++) {
                ep[leaf] = effects[leaf] / effectSum;
            }
        }
        return new MeasureVector(pseudoBaseline, pseudoTarget, ep);
    }

    private static MeasureVector partialDerivative(final Expression expression, final LeafTable table, final MeasureSpec measure) {
        final List<String> variables = measure.variables();
        final int leafCount = table.leafCount();
        final int k = variables.size();
        final int[] columns = columnIndexes(table, variables);
        final double[] totalsF = new double[k];
        for(int i = 0; i < k; i++) {
            totalsF[i] = table.baselineTotal(columns[i]);
        }
        final double base = eval(expression, variables, totalsF);

        final double[] gradients = new double[k];
        final double[] point = totalsF.clone();
        for(int i = 0; i < k; i++) {
            double delta = table.targetTotal(columns[i]) - totalsF[i];
            if(delta == 0) {
                delta = Math.max(Math.abs(totalsF[i]), 1.0) * 1e-6;
            }
            point[i] = totalsF[i] + delta;
            gradients[i] = (eval(expression, variables, point) - base) / delta;
            point[i] = totalsF[i];
        }

        final double[] pseudoBaseline = new double[leafCount];
        final double[] pseudoTarget = new double[leafCount];
        for(int leaf = 0; leaf < leafCount; leaf++) {
            double f = 0;
            double v = 0;
            for(int i = 0; i < k; i++) {
                f += gradients[i] * table.baselineValue(columns[i], leaf);
                v += gradients[i] * table.targetValue(columns[i], leaf);
            }
            pseudoBaseline[leaf] = f;
            pseudoTarget[leaf] = v;
        }
        return MeasureVector.of(pseudoBaseline, pseudoTarget);
    }

    private static MeasureVector shapley(final Expression expression, final LeafTable table, final MeasureSpec measure) {
        final List<String> variables = measure.variables();
        final int k = variables.size();
        if(k > MAX_SHAPLEY_VARIABLES) {
            throw new IllegalArgumentException("shapley allocation supports at most "
                    + MAX_SHAPLEY_VARIABLES + " variables, but expression of measure "
                    + measure.name() + " has " + k);
        }
        final int leafCount = table.leafCount();
        final int[] columns = columnIndexes(table, variables);
        final double[] totalsF = new double[k];
        final double[] totalsV = new double[k];
        for(int i = 0; i < k; i++) {
            totalsF[i] = table.baselineTotal(columns[i]);
            totalsV[i] = table.targetTotal(columns[i]);
        }

        // Coalition values: variable in S -> target total, else baseline total
        final double[] coalitionValues = new double[1 << k];
        final double[] point = new double[k];
        for(int mask = 0; mask < (1 << k); mask++) {
            for(int i = 0; i < k; i++) {
                point[i] = (mask & (1 << i)) != 0 ? totalsV[i] : totalsF[i];
            }
            coalitionValues[mask] = eval(expression, variables, point);
        }

        final double[] factorials = new double[k + 1];
        factorials[0] = 1;
        for(int i = 1; i <= k; i++) {
            factorials[i] = factorials[i - 1] * i;
        }
        final double[] shapleyValues = new double[k];
        for(int i = 0; i < k; i++) {
            for(int mask = 0; mask < (1 << k); mask++) {
                if((mask & (1 << i)) != 0) {
                    continue;
                }
                final int size = Integer.bitCount(mask);
                final double weight = factorials[size] * factorials[k - size - 1] / factorials[k];
                shapleyValues[i] += weight * (coalitionValues[mask | (1 << i)] - coalitionValues[mask]);
            }
        }

        // Pseudo baseline: gradient linearization (as partialDerivative); pseudo delta: Shapley shares
        // distributed proportionally to each component's leaf delta
        final MeasureVector linearized = partialDerivative(expression, table, measure);
        final double[] pseudoBaseline = linearized.baseline();
        final double[] pseudoTarget = new double[leafCount];
        for(int leaf = 0; leaf < leafCount; leaf++) {
            double delta = 0;
            for(int i = 0; i < k; i++) {
                final double totalDelta = totalsV[i] - totalsF[i];
                if(totalDelta != 0) {
                    final double leafDelta = table.targetValue(columns[i], leaf) - table.baselineValue(columns[i], leaf);
                    delta += shapleyValues[i] * leafDelta / totalDelta;
                }
            }
            pseudoTarget[leaf] = pseudoBaseline[leaf] + delta;
        }
        return MeasureVector.of(pseudoBaseline, pseudoTarget);
    }

    private static int[] columnIndexes(final LeafTable table, final List<String> variables) {
        final int[] columns = new int[variables.size()];
        for(int i = 0; i < variables.size(); i++) {
            columns[i] = table.columnIndex(variables.get(i));
        }
        return columns;
    }

    private static Expression createExpression(final MeasureSpec measure) {
        if(measure.expression() == null || measure.variables() == null || measure.variables().isEmpty()) {
            throw new IllegalArgumentException("derived measure " + measure.name()
                    + " requires expression and variables");
        }
        return new ExpressionBuilder(measure.expression())
                .variables(new HashSet<>(measure.variables()))
                .build();
    }

    private static double eval(final Expression expression, final List<String> variables, final double[] values) {
        for(int i = 0; i < variables.size(); i++) {
            expression.setVariable(variables.get(i), values[i]);
        }
        try {
            final double result = expression.evaluate();
            return Double.isFinite(result) ? result : 0.0;
        } catch (final ArithmeticException e) {
            // exp4j throws on division by zero; treat undefined points as 0 (empty slice semantics)
            return 0.0;
        }
    }

    private static double evalSafe(final Expression expression, final List<String> variables, final Map<String, Double> values) {
        final Map<String, Double> map = new HashMap<>();
        for(final String variable : variables) {
            map.put(variable, values.getOrDefault(variable, 0.0));
        }
        expression.setVariables(map);
        try {
            final double result = expression.evaluate();
            return Double.isFinite(result) ? result : 0.0;
        } catch (final ArithmeticException e) {
            return 0.0;
        }
    }
}
