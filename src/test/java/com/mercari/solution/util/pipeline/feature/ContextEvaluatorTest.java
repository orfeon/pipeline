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

    /** The context svd: the columns of one op read one fit of the group (the first score, the residual norm, the share), a row with a hole reads null. */
    @Test
    public void testGroupSvdColumns() {
        final java.util.List<OutputColumn> columns = new java.util.ArrayList<>();
        for (final String[] readout : new String[][]{{"pc_0", "scores", "0"}, {"pc_1", "scores", "1"}, {"pc_residnorm", "residualNorm", null}, {"pc_absorption", "absorption", null}, {"pc_resid_b", "residual", "1"}}) {
            final OutputColumn c = new OutputColumn();
            c.canonicalName = readout[0];
            c.scope = FeatureSpec.Scope.context;
            c.operator = "svd";
            c.coordinates.put("fields", "a,b,c");
            c.coordinates.put("rank", "2");
            c.coordinates.put("func", readout[1]);
            if (readout[2] != null) c.coordinates.put("index", readout[2]);
            columns.add(c);
        }
        final ContextEvaluator evaluator = new ContextEvaluator(columns);
        evaluator.setup();
        final double[][] channels = {{1, 2, 3, 4, 5}, {2, 2.5, 5, 4.5, 7}, {0.5, 1.5, 1.0, 3.5, 2.5}};
        final java.util.List<java.util.Map<String, Object>> rows = new java.util.ArrayList<>();
        for (int i = 0; i < 5; i++) {
            final java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("a", channels[0][i]);
            row.put("b", channels[1][i]);
            row.put("c", channels[2][i]);
            rows.add(row);
        }
        final java.util.Map<String, Object> holed = new java.util.HashMap<>(java.util.Map.of("a", 9d, "c", 0d));
        rows.add(holed);
        evaluator.evaluate(rows);
        final GroupOps.GroupSvd reference = GroupOps.svd(channels, 2, false);
        for (int i = 0; i < 5; i++) {
            Assertions.assertEquals(reference.scores()[i][0], (Double) rows.get(i).get("pc_0"), 0d);
            Assertions.assertEquals(reference.scores()[i][1], (Double) rows.get(i).get("pc_1"), 0d);
            Assertions.assertEquals(reference.residualNorm()[i], (Double) rows.get(i).get("pc_residnorm"), 0d);
            Assertions.assertEquals(reference.residual()[i][1], (Double) rows.get(i).get("pc_resid_b"), 0d);
            Assertions.assertEquals(reference.absorption(), (Double) rows.get(i).get("pc_absorption"), 0d);
        }
        for (final String name : java.util.List.of("pc_0", "pc_1", "pc_residnorm", "pc_absorption", "pc_resid_b")) Assertions.assertNull(holed.get(name), name);
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
        // a pure function of the group: the same rows in another arrival order read the same bits
        final java.util.List<java.util.Map<String, Object>> forward = new java.util.ArrayList<>();
        for (int i = 0; i < 12; i++) forward.add(row(0.1 * i * i + 1d / (i + 3), 0.37 * (i % 5) + 0.01 * i));
        final java.util.List<java.util.Map<String, Object>> backward = new java.util.ArrayList<>();
        for (final java.util.Map<String, Object> r : forward) backward.add(0, new java.util.HashMap<>(r));
        evaluator.evaluateColumn(c, forward);
        evaluator.evaluateColumn(c, backward);
        for (int i = 0; i < forward.size(); i++) {
            Assertions.assertEquals(forward.get(i).get("p"), backward.get(forward.size() - 1 - i).get("p"), "row " + i);
        }
    }

    /** poissonProb: a group of two rated rows is a match every column reads from one model; one or three rows, or a side without a rating, read null. */
    @Test
    public void testPoissonProbColumns() {
        final java.util.List<OutputColumn> columns = new java.util.ArrayList<>();
        for (final String[] f : new String[][]{{"expected", null}, {"win", null}, {"draw", null}, {"lose", null}, {"over", "2.5"}, {"bothScore", null}}) {
            final OutputColumn c = new OutputColumn();
            c.canonicalName = "m_" + f[0];
            c.scope = FeatureSpec.Scope.context;
            c.operator = "poissonProb";
            c.coordinates.put("field", "attack");
            c.coordinates.put("defence", "defence");
            c.coordinates.put("offset", "home");
            c.coordinates.put("rho", "-0.1");
            c.coordinates.put("func", f[0]);
            if (f[1] != null) c.coordinates.put("threshold", f[1]);
            columns.add(c);
        }
        final ContextEvaluator evaluator = new ContextEvaluator(columns);
        evaluator.setup();
        final java.util.List<java.util.Map<String, Object>> rows = new java.util.ArrayList<>(java.util.List.of(match(0.3, 0.1, 0.2), match(-0.1, 0.25, 0d)));
        evaluator.evaluate(rows);
        // the first row's rate exp(0.3 - 0.25 + 0.2) = exp(0.25), the second's exp(-0.1 - 0.1); the model reads the smaller rate as side 0
        final PoissonMatch reference = PoissonMatch.of(Math.exp(-0.2), Math.exp(0.25), -0.1, 0d);
        Assertions.assertEquals(reference.expected(1), (Double) rows.get(0).get("m_expected"), 0d);
        Assertions.assertEquals(reference.expected(0), (Double) rows.get(1).get("m_expected"), 0d);
        Assertions.assertEquals(reference.win(1), (Double) rows.get(0).get("m_win"), 0d);
        Assertions.assertEquals(reference.win(0), (Double) rows.get(1).get("m_win"), 0d);
        Assertions.assertEquals(reference.lose(1), (Double) rows.get(0).get("m_lose"), 0d);
        Assertions.assertEquals(reference.draw(), (Double) rows.get(0).get("m_draw"), 0d);
        Assertions.assertEquals(reference.draw(), (Double) rows.get(1).get("m_draw"), 0d);
        Assertions.assertEquals(reference.over(2.5), (Double) rows.get(0).get("m_over"), 0d);
        Assertions.assertEquals(reference.bothScore(), (Double) rows.get(1).get("m_bothScore"), 0d);
        Assertions.assertEquals((Double) rows.get(0).get("m_win"), (Double) rows.get(1).get("m_lose"), 0d);
        Assertions.assertEquals(1d, (Double) rows.get(0).get("m_win") + (Double) rows.get(0).get("m_draw") + (Double) rows.get(0).get("m_lose"), 1e-12);
        Assertions.assertTrue((Double) rows.get(0).get("m_win") > (Double) rows.get(1).get("m_win"), "the higher rate is the favourite");
        // the arrival order: the same two rows the other way round read the same bits
        final java.util.List<java.util.Map<String, Object>> reversed = new java.util.ArrayList<>(java.util.List.of(match(-0.1, 0.25, 0d), match(0.3, 0.1, 0.2)));
        evaluator.evaluate(reversed);
        for (final String f : java.util.List.of("m_expected", "m_win", "m_draw", "m_lose", "m_over", "m_bothScore")) {
            Assertions.assertEquals(rows.get(0).get(f), reversed.get(1).get(f), f);
            Assertions.assertEquals(rows.get(1).get(f), reversed.get(0).get(f), f);
        }
        // no match: a group of one, of three rated rows, a side without a defence, a side without the offset that is named
        for (final java.util.List<java.util.Map<String, Object>> group : java.util.List.of(
                new java.util.ArrayList<>(java.util.List.of(match(0.3, 0.1, 0.2))),
                new java.util.ArrayList<>(java.util.List.of(match(0.3, 0.1, 0.2), match(-0.1, 0.25, 0d), match(0d, 0d, 0d))),
                new java.util.ArrayList<>(java.util.List.of(match(0.3, 0.1, 0.2), match(-0.1, null, 0d))),
                new java.util.ArrayList<>(java.util.List.of(match(0.3, 0.1, null), match(-0.1, 0.25, 0d))))) {
            evaluator.evaluate(group);
            for (final java.util.Map<String, Object> row : group) {
                for (final String f : java.util.List.of("m_expected", "m_win", "m_over")) Assertions.assertNull(row.get(f), f);
            }
        }
        // a third row without a rating is not a side: the two rated rows are still a match
        final java.util.List<java.util.Map<String, Object>> withSpectator = new java.util.ArrayList<>(java.util.List.of(match(0.3, 0.1, 0.2), match(null, null, null), match(-0.1, 0.25, 0d)));
        evaluator.evaluate(withSpectator);
        Assertions.assertEquals(rows.get(0).get("m_win"), withSpectator.get(0).get("m_win"));
        Assertions.assertNull(withSpectator.get(1).get("m_win"));
        Assertions.assertEquals(rows.get(1).get("m_win"), withSpectator.get(2).get("m_win"));
    }

    private static java.util.Map<String, Object> match(final Double attack, final Double defence, final Double home) {
        final java.util.Map<String, Object> row = new java.util.HashMap<>();
        row.put("attack", attack);
        row.put("defence", defence);
        row.put("home", home);
        return row;
    }

    private static OutputColumn ratingProb(final String name, final String offsetScale) {
        final OutputColumn c = new OutputColumn();
        c.canonicalName = name;
        c.scope = FeatureSpec.Scope.context;
        c.operator = "ratingProb";
        c.coordinates.put("field", "mu");
        c.coordinates.put("sigma", "sigma");
        c.coordinates.put("beta", "4");
        c.coordinates.put("offset", "w");
        c.coordinates.put("offsetScale", offsetScale);
        return c;
    }

    /**
     * ratingProb with an offset: the benchmark weighs every row's strength, p_i = w_i exp(mu_i / c) / Σ w_j exp(mu_j / c) —
     * a row without a valid offset is out of the contest (its uncertainty too), an offset of 0 reads 0 and adds no
     * uncertainty to the scale either (the rating's ln 0 offset keeps the row out of its contest); offsetScale log reads ln w.
     */
    @Test
    public void testRatingProbOffset() {
        final OutputColumn c = ratingProb("p", "probability");
        final java.util.List<java.util.Map<String, Object>> rows = new java.util.ArrayList<>();
        final double[][] values = {{30, 3, 0.5}, {25, 8, 0.3}, {20, 1, 0.2}, {27, 2, Double.NaN}, {22, 2, 0}};
        for (final double[] v : values) {
            final java.util.Map<String, Object> row = row(v[0], v[1]);
            row.put("w", Double.isNaN(v[2]) ? null : v[2]);
            rows.add(row);
        }
        new ContextEvaluator(java.util.List.of(c)).evaluateColumn(c, rows);
        // c² over the rows the rating's contest holds: the null offset is out, and so is the zero one (ln 0)
        final double scale = Math.sqrt((9 + 16) + (64 + 16) + (1 + 16));
        final double sum = 0.5 * Math.exp(30 / scale) + 0.3 * Math.exp(25 / scale) + 0.2 * Math.exp(20 / scale);
        Assertions.assertEquals(0.5 * Math.exp(30 / scale) / sum, (Double) rows.get(0).get("p"), 1e-12);
        Assertions.assertEquals(0.3 * Math.exp(25 / scale) / sum, (Double) rows.get(1).get("p"), 1e-12);
        Assertions.assertEquals(0.2 * Math.exp(20 / scale) / sum, (Double) rows.get(2).get("p"), 1e-12);
        Assertions.assertNull(rows.get(3).get("p"), "no offset: out of the contest");
        Assertions.assertEquals(0d, (Double) rows.get(4).get("p"), 0d, "an offset of 0 reads 0");
        // the log scale reads ln w: the same probabilities from log offsets
        final OutputColumn log = ratingProb("q", "log");
        final java.util.List<java.util.Map<String, Object>> logRows = new java.util.ArrayList<>();
        for (int i = 0; i < 3; i++) {
            final java.util.Map<String, Object> row = new java.util.HashMap<>(rows.get(i));
            row.put("w", Math.log((Double) rows.get(i).get("w")));
            logRows.add(row);
        }
        final java.util.List<java.util.Map<String, Object>> probabilityRows = new java.util.ArrayList<>();
        for (int i = 0; i < 3; i++) probabilityRows.add(new java.util.HashMap<>(rows.get(i)));
        new ContextEvaluator(java.util.List.of(log)).evaluateColumn(log, logRows);
        new ContextEvaluator(java.util.List.of(c)).evaluateColumn(c, probabilityRows);
        for (int i = 0; i < 3; i++) Assertions.assertEquals((Double) probabilityRows.get(i).get("p"), (Double) logRows.get(i).get("q"), 1e-12, "row " + i);
        // a finite log offset whose exp underflows reads 0 but stays in the scale (the rating's contest held the row);
        // ln 0 itself does not
        for (final double tiny : new double[] {-800d, Double.NEGATIVE_INFINITY}) {
            final java.util.List<java.util.Map<String, Object>> underflow = new java.util.ArrayList<>();
            for (int i = 0; i < 3; i++) underflow.add(new java.util.HashMap<>(logRows.get(i)));
            final java.util.Map<String, Object> held = row(22d, 2d);
            held.put("w", tiny);
            underflow.add(held);
            new ContextEvaluator(java.util.List.of(log)).evaluateColumn(log, underflow);
            final double heldScale = Math.sqrt(scale * scale + (Double.isFinite(tiny) ? 4 + 16 : 0));
            final double heldSum = 0.5 * Math.exp(30 / heldScale) + 0.3 * Math.exp(25 / heldScale) + 0.2 * Math.exp(20 / heldScale);
            Assertions.assertEquals(0.5 * Math.exp(30 / heldScale) / heldSum, (Double) underflow.get(0).get("q"), 1e-12, "offset " + tiny);
            Assertions.assertEquals(0d, (Double) underflow.get(3).get("q"), 0d, "offset " + tiny);
        }
        // equal strengths read the benchmark back
        final java.util.List<java.util.Map<String, Object>> equal = new java.util.ArrayList<>();
        for (final double w : new double[] {0.6, 0.4}) {
            final java.util.Map<String, Object> row = row(25d, 2d);
            row.put("w", w);
            equal.add(row);
        }
        new ContextEvaluator(java.util.List.of(c)).evaluateColumn(c, equal);
        Assertions.assertEquals(0.6, (Double) equal.get(0).get("p"), 1e-12);
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

    /** A declared `value:` against a row value: numbers as numbers (a row expression is float64), texts as texts. */
    @Test
    public void testMatchesDeclared() {
        Assertions.assertTrue(FeatureValues.matchesDeclared(1.0d, "1"));
        Assertions.assertTrue(FeatureValues.matchesDeclared(1L, "1"));
        Assertions.assertTrue(FeatureValues.matchesDeclared(1, "1.0"));
        Assertions.assertTrue(FeatureValues.matchesDeclared(1.5d, "1.50"));
        Assertions.assertTrue(FeatureValues.matchesDeclared(9_007_199_254_740_993L, "9007199254740993"));
        Assertions.assertFalse(FeatureValues.matchesDeclared(9_007_199_254_740_993L, "9007199254740992"));
        Assertions.assertFalse(FeatureValues.matchesDeclared(0.0d, "1"));
        Assertions.assertFalse(FeatureValues.matchesDeclared(1.0d, "good"));
        Assertions.assertFalse(FeatureValues.matchesDeclared(null, "1"));
        // a float32 value in its own precision (its text "0.1" matched before), NaN as NaN, a long against a decimal
        Assertions.assertTrue(FeatureValues.matchesDeclared(0.1f, "0.1"));
        Assertions.assertTrue(FeatureValues.matchesDeclared(1.0f, "1"));
        Assertions.assertFalse(FeatureValues.matchesDeclared(0.1f, "0.2"));
        Assertions.assertTrue(FeatureValues.matchesDeclared(Double.NaN, "NaN"));
        Assertions.assertFalse(FeatureValues.matchesDeclared(Double.NaN, "1"));
        Assertions.assertTrue(FeatureValues.matchesDeclared(2L, "2.0"));
        Assertions.assertTrue(FeatureValues.matchesDeclared(-3, "-3"));
        Assertions.assertFalse(FeatureValues.matchesDeclared(1L, "yes"));
        Assertions.assertFalse(FeatureValues.matchesDeclared("electronics", "toys"));
        // a text (a string field) only by its exact text: "2.0" and "2" are distinct categories
        Assertions.assertTrue(FeatureValues.matchesDeclared("good", "good"));
        Assertions.assertFalse(FeatureValues.matchesDeclared("2", "2.00"));
        Assertions.assertFalse(FeatureValues.matchesDeclared("1.0", "1"));
        Assertions.assertFalse(FeatureValues.matchesDeclared("01", "1"));
        Assertions.assertTrue(FeatureValues.matchesDeclared(true, "true"));
    }

}
