package com.mercari.solution.util.pipeline.feature;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** countByValue / ratioByValue keys: `values: [1]` must match int, long and double fields alike. */
public class ContextEvaluatorTest {

    private static java.util.Map<String, Object> row(final Double mu, final Double sigma) {
        final java.util.Map<String, Object> row = new java.util.HashMap<>();
        row.put("mu", mu);
        row.put("sigma", sigma);
        return row;
    }

    /** ratingProb: the Plackett-Luce contest over the group's (mu, sigma), rows without a value out of it, one row = 1. */
    @Test
    public void testRatingProbIsThePlackettLuceContest() {
        final OutputColumn c = new OutputColumn();
        c.canonicalName = "p";
        c.scope = FeatureSpec.Scope.context;
        c.operator = "ratingProb";
        c.coordinates.put("field", "mu");
        c.coordinates.put("sigma", "sigma");
        c.coordinates.put("beta", "4");
        final ContextEvaluator evaluator = new ContextEvaluator(java.util.List.of(c));
        final java.util.List<java.util.Map<String, Object>> rows = new java.util.ArrayList<>(java.util.List.of(
                row(30d, 3d), row(25d, 8d), row(20d, 1d), row(null, 2d), row(27d, null)));
        evaluator.evaluateColumn(c, rows);
        // by hand: c² = (9 + 16) + (64 + 16) + (1 + 16) over the three rows with both values
        final double scale = Math.sqrt(25 + 80 + 17);
        final double sum = Math.exp(30 / scale) + Math.exp(25 / scale) + Math.exp(20 / scale);
        Assertions.assertEquals(Math.exp(30 / scale) / sum, (Double) rows.get(0).get("p"), 1e-12);
        Assertions.assertEquals(Math.exp(25 / scale) / sum, (Double) rows.get(1).get("p"), 1e-12);
        Assertions.assertEquals(Math.exp(20 / scale) / sum, (Double) rows.get(2).get("p"), 1e-12);
        Assertions.assertNull(rows.get(3).get("p"), "no mu: out of the contest");
        Assertions.assertNull(rows.get(4).get("p"), "no sigma while a sigma column is named: out of the contest");
        // the favourite is the higher mu, and a larger field scale flattens the contest
        Assertions.assertTrue((Double) rows.get(0).get("p") > (Double) rows.get(1).get("p"));
        Assertions.assertTrue((Double) rows.get(0).get("p") < Math.exp(30 / 4d) / (Math.exp(30 / 4d) + Math.exp(25 / 4d) + Math.exp(20 / 4d)), "a smaller c would make the favourite surer");
        // without a sigma column the uncertainty is 0: c² = n · beta²; a contest of one reads 1; a shifted group reads the same
        final OutputColumn plain = new OutputColumn();
        plain.canonicalName = "q";
        plain.scope = FeatureSpec.Scope.context;
        plain.operator = "ratingProb";
        plain.coordinates.put("field", "mu");
        plain.coordinates.put("beta", "4");
        final ContextEvaluator plainEvaluator = new ContextEvaluator(java.util.List.of(plain));
        final java.util.List<java.util.Map<String, Object>> two = new java.util.ArrayList<>(java.util.List.of(row(30d, null), row(20d, null)));
        plainEvaluator.evaluateColumn(plain, two);
        final double c2 = Math.sqrt(2 * 16);
        Assertions.assertEquals(Math.exp(30 / c2) / (Math.exp(30 / c2) + Math.exp(20 / c2)), (Double) two.get(0).get("q"), 1e-12);
        final java.util.List<java.util.Map<String, Object>> shifted = new java.util.ArrayList<>(java.util.List.of(row(1030d, null), row(1020d, null)));
        plainEvaluator.evaluateColumn(plain, shifted);
        Assertions.assertEquals((Double) two.get(0).get("q"), (Double) shifted.get(0).get("q"), 1e-12);
        final java.util.List<java.util.Map<String, Object>> one = new java.util.ArrayList<>(java.util.List.of(row(30d, null)));
        plainEvaluator.evaluateColumn(plain, one);
        Assertions.assertEquals(1d, (Double) one.get(0).get("q"), 0d);
    }

    @Test
    public void testValueKeyNormalisesIntegralNumbers() {
        Assertions.assertEquals("1", ContextEvaluator.valueKey(1));
        Assertions.assertEquals("1", ContextEvaluator.valueKey(1L));
        Assertions.assertEquals("1", ContextEvaluator.valueKey(1.0d));
        Assertions.assertEquals("1", ContextEvaluator.valueKey(1.0f));
        Assertions.assertEquals("1", ContextEvaluator.valueKey("1.0"));
        Assertions.assertEquals("1.5", ContextEvaluator.valueKey(1.5d));
        Assertions.assertEquals("good", ContextEvaluator.valueKey("good"));
        Assertions.assertEquals("true", ContextEvaluator.valueKey(true));
        Assertions.assertEquals("007", ContextEvaluator.valueKey("007")); // strings without a fraction are kept verbatim
    }

}
