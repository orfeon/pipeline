package com.mercari.solution.util;

import com.mercari.solution.util.ExpressionUtil.Expression;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.*;

public class ExpressionUtilTest {

    private static final double DELTA = 1e-15;

    @Test
    public void testEstimateVariables() {

        final Random random = new Random();

        final String expressionText1 = "(a - b) >= 1.5 * ((a - b_1) + (a_2 - b_2) + if(x > y, zz_aa_1, 0)) / 5";
        final Set<String> variables1 = ExpressionUtil.estimateVariables(expressionText1);

        Assertions.assertEquals(8, variables1.size());
        Assertions.assertTrue(variables1.containsAll(Arrays.asList("a","b","a_2","b_1","b_2","x","y","zz_aa_1")));

        final Map<String,Integer> bufferSizes1 = ExpressionUtil.extractBufferSizes(variables1, "_");
        Assertions.assertEquals(5, bufferSizes1.size());
        Assertions.assertTrue(bufferSizes1.keySet().containsAll(Arrays.asList("a","b","x","y","zz_aa")));
        Assertions.assertEquals(2, bufferSizes1.get("a").intValue());
        Assertions.assertEquals(2, bufferSizes1.get("b").intValue());
        Assertions.assertEquals(0, bufferSizes1.get("x").intValue());
        Assertions.assertEquals(0, bufferSizes1.get("y").intValue());
        Assertions.assertEquals(1, bufferSizes1.get("zz_aa").intValue());

        final Map<String,Double> values1 = new HashMap<>();
        for(final String variable : variables1) {
            values1.put(variable, random.nextDouble());
        }

        final Expression expression1 = ExpressionUtil.createDefaultExpression(expressionText1, variables1);
        Assertions.assertEquals(variables1, expression1.getVariableNames());
        final double result1 = expression1.evaluate(values1);
        Assertions.assertTrue(Arrays.asList(0D, 1D).contains(result1));

        // only number formula
        final String expressionText2 = "(120 - 12) / 4.5";
        final Set<String> variables2 = ExpressionUtil.estimateVariables(expressionText2);
        Assertions.assertEquals(0, variables2.size());

        final Map<String,Integer> bufferSizes2 = ExpressionUtil.extractBufferSizes(variables2, "_");
        Assertions.assertEquals(0, bufferSizes2.size());

        final Expression expression2 = ExpressionUtil.createDefaultExpression(expressionText2, variables2);
        final Map<String,Double> values2 = new HashMap<>();
        final double result2 = expression2.evaluate(values2);
        Assertions.assertEquals((120 - 12) / 4.5, result2, DELTA);

        // ternary expression
        final String expressionText3 = "a > b ? a : b";
        final Set<String> variables3 = ExpressionUtil.estimateVariables(expressionText3);
        Assertions.assertEquals(2, variables3.size());
        Assertions.assertTrue(variables3.containsAll(Arrays.asList("a","b")));

    }

    @Test
    public void testOperators() {
        final Expression eq = ExpressionUtil.createDefaultExpression("a == b");
        Assertions.assertEquals(1D, eq.evaluate(Map.of("a", 1D, "b", 1D)), DELTA);
        Assertions.assertEquals(0D, eq.evaluate(Map.of("a", 1D, "b", 2D)), DELTA);

        final Expression ne = ExpressionUtil.createDefaultExpression("a != b");
        Assertions.assertEquals(0D, ne.evaluate(Map.of("a", 1D, "b", 1D)), DELTA);
        Assertions.assertEquals(1D, ne.evaluate(Map.of("a", 1D, "b", 2D)), DELTA);

        final Expression and = ExpressionUtil.createDefaultExpression("a > 0 && b > 0");
        Assertions.assertEquals(1D, and.evaluate(Map.of("a", 1D, "b", 1D)), DELTA);
        Assertions.assertEquals(0D, and.evaluate(Map.of("a", 1D, "b", -1D)), DELTA);

        final Expression or = ExpressionUtil.createDefaultExpression("a > 0 || b > 0");
        Assertions.assertEquals(1D, or.evaluate(Map.of("a", -1D, "b", 1D)), DELTA);
        Assertions.assertEquals(0D, or.evaluate(Map.of("a", -1D, "b", -1D)), DELTA);

        final Expression not = ExpressionUtil.createDefaultExpression("!(a > 0)");
        Assertions.assertEquals(0D, not.evaluate(Map.of("a", 1D)), DELTA);
        Assertions.assertEquals(1D, not.evaluate(Map.of("a", -1D)), DELTA);

        final Expression ternary = ExpressionUtil.createDefaultExpression("a > b ? a : b");
        Assertions.assertEquals(2D, ternary.evaluate(Map.of("a", 2D, "b", 1D)), DELTA);
        Assertions.assertEquals(3D, ternary.evaluate(Map.of("a", 2D, "b", 3D)), DELTA);
    }

    @Test
    public void testFunctions() {
        final Expression pow = ExpressionUtil.createDefaultExpression("pow(a, 2)");
        Assertions.assertEquals(9D, pow.evaluate(Map.of("a", 3D)), DELTA);

        final Expression log = ExpressionUtil.createDefaultExpression("log(a)");
        Assertions.assertEquals(Math.log(5D), log.evaluate(Map.of("a", 5D)), DELTA);

        final Expression log2 = ExpressionUtil.createDefaultExpression("log2(a)");
        Assertions.assertEquals(3D, log2.evaluate(Map.of("a", 8D)), DELTA);

        final Expression cbrt = ExpressionUtil.createDefaultExpression("cbrt(a)");
        Assertions.assertEquals(2D, cbrt.evaluate(Map.of("a", 8D)), DELTA);

        final Expression signum = ExpressionUtil.createDefaultExpression("signum(a)");
        Assertions.assertEquals(-1D, signum.evaluate(Map.of("a", -5D)), DELTA);

        final Expression ifExp = ExpressionUtil.createDefaultExpression("if(a > b, a * 10, b * 10)");
        Assertions.assertEquals(20D, ifExp.evaluate(Map.of("a", 2D, "b", 1D)), DELTA);
        Assertions.assertEquals(30D, ifExp.evaluate(Map.of("a", 2D, "b", 3D)), DELTA);

        final Expression switchExp = ExpressionUtil.createDefaultExpression("switch3(a > 2, 100, a > 1, 200, a > 0, 300)");
        Assertions.assertEquals(100D, switchExp.evaluate(Map.of("a", 3D)), DELTA);
        Assertions.assertEquals(200D, switchExp.evaluate(Map.of("a", 2D)), DELTA);
        Assertions.assertEquals(300D, switchExp.evaluate(Map.of("a", 1D)), DELTA);
        Assertions.assertEquals(0D, switchExp.evaluate(Map.of("a", -1D)), DELTA);

        // pi and e are provided as constants
        final Expression constants = ExpressionUtil.createDefaultExpression("cos(pi) + log(e)");
        Assertions.assertEquals(0D, constants.evaluate(Map.of()), DELTA);
    }

    @Test
    public void testErrors() {
        // invalid syntax must fail at compile time
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ExpressionUtil.createDefaultExpression("a +* b"));

        // evaluating with a missing variable must fail
        final Expression expression = ExpressionUtil.createDefaultExpression("a + b");
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> expression.evaluate(Map.of("a", 1D)));

        // NaN propagates
        Assertions.assertTrue(Double.isNaN(expression.evaluate(Map.of("a", Double.NaN, "b", 1D))));
    }

    @Test
    public void testTimestampToDate() {

        {
            final String expressionText = "timestamp_to_date(a, b)";
            final Set<String> variables = ExpressionUtil.estimateVariables(expressionText);
            Assertions.assertEquals(2, variables.size());
            Assertions.assertTrue(variables.containsAll(Arrays.asList("a","b")));

            final Map<String,Double> values = new HashMap<>();

            Instant a = Instant.parse("2023-01-15T14:59:59.999Z");
            values.put("a", Long.valueOf(a.getMillis() * 1000L).doubleValue());
            values.put("b", 9D);
            Expression expression = ExpressionUtil.createDefaultExpression(expressionText, variables);
            double result = expression.evaluate(values);
            Assertions.assertEquals(LocalDate.of(2023,1,15).toEpochDay(), result, DELTA);

            a = Instant.parse("2023-01-15T15:00:00.000Z");
            values.put("a", Long.valueOf(a.getMillis() * 1000L).doubleValue());
            values.put("b", 9D);
            expression = ExpressionUtil.createDefaultExpression(expressionText, variables);
            result = expression.evaluate(values);
            Assertions.assertEquals(LocalDate.of(2023,1,16).toEpochDay(), result, DELTA);
        }

        {
            final String expressionText = "timestamp_to_date(a, b) - timestamp_to_date(c, d)";
            final Set<String> variables = ExpressionUtil.estimateVariables(expressionText);
            Assertions.assertEquals(4, variables.size());
            Assertions.assertTrue(variables.containsAll(Arrays.asList("a","b","c","d")));

            final Map<String,Double> values = new HashMap<>();

            Instant a = Instant.parse("2023-01-15T15:00:00.000Z");
            Instant b = Instant.parse("2023-01-14T14:59:59.999Z");
            values.put("a", Long.valueOf(a.getMillis() * 1000L).doubleValue());
            values.put("b", 9D);
            values.put("c", Long.valueOf(b.getMillis() * 1000L).doubleValue());
            values.put("d", 9D);
            final Expression expression = ExpressionUtil.createDefaultExpression(expressionText, variables);
            final double result = expression.evaluate(values);
            Assertions.assertEquals(2D, result, DELTA);
        }
    }

    @Test
    public void testTimestampDiff() {
        // millisecond
        final String expressionText1 = "timestamp_diff_millisecond(a,b)";
        final Set<String> variables1 = ExpressionUtil.estimateVariables(expressionText1);
        Assertions.assertEquals(2, variables1.size());
        Assertions.assertTrue(variables1.containsAll(Arrays.asList("a","b")));
        final Map<String,Double> values1 = new HashMap<>();
        Instant a = Instant.parse("2023-01-15T00:00:00.000Z");
        Instant b = Instant.parse("2023-01-17T12:32:12.543Z");
        values1.put("a", Long.valueOf(a.getMillis() * 1000L).doubleValue());
        values1.put("b", Long.valueOf(b.getMillis() * 1000L).doubleValue());
        final Expression expression1 = ExpressionUtil.createDefaultExpression(expressionText1, variables1);
        final double result1 = expression1.evaluate(values1);
        Assertions.assertEquals((a.getMillis() - b.getMillis()), result1, DELTA);

        // second
        final String expressionText2 = "timestamp_diff_second(a,b)";
        final Set<String> variables2 = ExpressionUtil.estimateVariables(expressionText2);
        Assertions.assertEquals(2, variables2.size());
        Assertions.assertTrue(variables2.containsAll(Arrays.asList("a","b")));
        final Map<String,Double> values2 = new HashMap<>();
        values2.put("a", Long.valueOf(a.getMillis() * 1000L).doubleValue());
        values2.put("b", Long.valueOf(b.getMillis() * 1000L).doubleValue());
        final Expression expression2 = ExpressionUtil.createDefaultExpression(expressionText2, variables2);
        final double result2 = expression2.evaluate(values2);
        Assertions.assertEquals((a.getMillis() - b.getMillis()) / 1000, result2, DELTA);

        // minute
        final String expressionText3 = "timestamp_diff_minute(a,b)";
        final Set<String> variables3 = ExpressionUtil.estimateVariables(expressionText3);
        Assertions.assertEquals(2, variables2.size());
        Assertions.assertTrue(variables3.containsAll(Arrays.asList("a","b")));
        final Map<String,Double> values3 = new HashMap<>();
        values3.put("a", Long.valueOf(a.getMillis() * 1000L).doubleValue());
        values3.put("b", Long.valueOf(b.getMillis() * 1000L).doubleValue());
        final Expression expression3 = ExpressionUtil.createDefaultExpression(expressionText3, variables3);
        final double result3 = expression3.evaluate(values3);
        Assertions.assertEquals((a.getMillis() - b.getMillis()) / (1000 * 60), result3, DELTA);

        // hour
        final String expressionText4 = "timestamp_diff_hour(a,b)";
        final Set<String> variables4 = ExpressionUtil.estimateVariables(expressionText4);
        Assertions.assertEquals(2, variables4.size());
        Assertions.assertTrue(variables4.containsAll(Arrays.asList("a","b")));
        final Map<String,Double> values4 = new HashMap<>();
        values4.put("a", Long.valueOf(a.getMillis() * 1000L).doubleValue());
        values4.put("b", Long.valueOf(b.getMillis() * 1000L).doubleValue());
        final Expression expression4 = ExpressionUtil.createDefaultExpression(expressionText4, variables4);
        final double result4 = expression4.evaluate(values4);
        Assertions.assertEquals((a.getMillis() - b.getMillis()) / (1000 * 60 * 60), result4, DELTA);

        // day
        final String expressionText5 = "timestamp_diff_day(a,b)";
        final Set<String> variables5 = ExpressionUtil.estimateVariables(expressionText5);
        Assertions.assertEquals(2, variables5.size());
        Assertions.assertTrue(variables5.containsAll(Arrays.asList("a","b")));
        final Map<String,Double> values5 = new HashMap<>();
        values5.put("a", Long.valueOf(a.getMillis() * 1000L).doubleValue());
        values5.put("b", Long.valueOf(b.getMillis() * 1000L).doubleValue());
        final Expression expression5 = ExpressionUtil.createDefaultExpression(expressionText5, variables5);
        final double result5 = expression5.evaluate(values5);
        Assertions.assertEquals((a.getMillis() - b.getMillis()) / (1000 * 60 * 60 * 24), result5, DELTA);

    }

}
