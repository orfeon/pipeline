package com.mercari.solution.util.pipeline.feature;

import com.google.gson.JsonObject;
import com.mercari.solution.config.Config;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Random;

/** The group solvers behind the context ops {@code residualize} and {@code harville}, and their compile contract. */
public class GroupOpsTest {

    private static final double NaN = Double.NaN;

    @Test
    public void testResidualizeHandValues() {
        // y on x = 0, 1, 2, 3: Sxy = 11.5, Sxx = 5 -> slope 2.3, intercept 4.25 - 2.3 * 1.5 = 0.8
        final double[] y = {1, 3, 5, 8};
        final double[][] x = {{0, 1, 2, 3}};
        Assertions.assertArrayEquals(new double[]{0.2, -0.1, -0.4, 0.3}, GroupOps.residualize(y, x, false), 1e-12);
        // leave-one-out: e / (1 - h), h = 1 / n + (x - mean)^2 / Sxx = 0.7, 0.3, 0.3, 0.7
        Assertions.assertArrayEquals(new double[]{0.2 / 0.3, -0.1 / 0.7, -0.4 / 0.7, 0.3 / 0.3}, GroupOps.residualize(y, x, true), 1e-12);

        // a row with a missing value reads NaN and takes no part: the other four are the group above
        final double[] yMissing = {1, 3, NaN, 5, 8, 2};
        final double[][] xMissing = {{0, 1, 7, 2, 3, NaN}};
        Assertions.assertArrayEquals(new double[]{0.2, -0.1, NaN, -0.4, 0.3, NaN}, GroupOps.residualize(yMissing, xMissing, false), 1e-12);

        // fewer than p + 2 complete rows: the line passes through (almost) every point, nothing to read
        for (final double v : GroupOps.residualize(new double[]{1, 3}, new double[][]{{0, 1}}, false)) Assertions.assertTrue(Double.isNaN(v));
        for (final double v : GroupOps.residualize(new double[]{1, 3, 5}, new double[][]{{0, 1, 2}}, true)) Assertions.assertTrue(Double.isNaN(v));
        Assertions.assertFalse(Double.isNaN(GroupOps.residualize(new double[]{1, 3, 6}, new double[][]{{0, 1, 2}}, false)[0]));
    }

    @Test
    public void testResidualizeProperties() {
        final Random random = new Random(3);
        final int n = 12;
        final double[] y = new double[n];
        final double[][] x = new double[2][n];
        for (int i = 0; i < n; i++) {
            x[0][i] = random.nextGaussian() * 3 + 50;
            x[1][i] = random.nextGaussian() + 0.2 * x[0][i];
            y[i] = 4 + 0.5 * x[0][i] - 2 * x[1][i] + random.nextGaussian();
        }
        final double[] e = GroupOps.residualize(y, x, false);
        // residuals sum to zero and are orthogonal to every regressor
        double sum = 0, dot0 = 0, dot1 = 0;
        for (int i = 0; i < n; i++) {
            sum += e[i];
            dot0 += e[i] * x[0][i];
            dot1 += e[i] * x[1][i];
        }
        Assertions.assertEquals(0, sum, 1e-9);
        Assertions.assertEquals(0, dot0, 1e-7);
        Assertions.assertEquals(0, dot1, 1e-7);

        // a regressor that is a combination of the others, or constant, changes nothing
        final double[] combined = new double[n], constant = new double[n];
        for (int i = 0; i < n; i++) {
            combined[i] = 2 * x[0][i] - x[1][i] + 1;
            constant[i] = 7;
        }
        Assertions.assertArrayEquals(e, GroupOps.residualize(y, new double[][]{x[0], x[1], combined, constant}, false), 1e-7);
        // against a constant alone the residual is the deviation from the group mean
        final double[] centred = GroupOps.residualize(y, new double[][]{constant}, false);
        double mean = 0;
        for (final double v : y) mean += v / n;
        for (int i = 0; i < n; i++) Assertions.assertEquals(y[i] - mean, centred[i], 1e-9);

        // leave-one-out is the fit on the other rows, whatever the order the group arrives in
        final double[] loo = GroupOps.residualize(y, x, true);
        for (int skip = 0; skip < n; skip++) {
            final double[] yOthers = new double[n - 1];
            final double[][] xOthers = new double[2][n - 1];
            for (int i = 0, j = 0; i < n; i++) {
                if (i == skip) continue;
                yOthers[j] = y[i];
                xOthers[0][j] = x[0][i];
                xOthers[1][j++] = x[1][i];
            }
            // residuals of the others give their fitted plane: predict the skipped row from two of its points' fits
            final double[] eOthers = GroupOps.residualize(yOthers, xOthers, false);
            final double[] beta = plane(yOthers, xOthers, eOthers);
            Assertions.assertEquals(y[skip] - (beta[0] + beta[1] * x[0][skip] + beta[2] * x[1][skip]), loo[skip], 1e-6, "row " + skip);
        }
        final int[] order = {5, 0, 11, 3, 8, 1, 10, 2, 7, 4, 9, 6};
        final double[] yPermuted = new double[n];
        final double[][] xPermuted = new double[2][n];
        for (int i = 0; i < n; i++) {
            yPermuted[i] = y[order[i]];
            xPermuted[0][i] = x[0][order[i]];
            xPermuted[1][i] = x[1][order[i]];
        }
        final double[] ePermuted = GroupOps.residualize(yPermuted, xPermuted, false);
        for (int i = 0; i < n; i++) Assertions.assertEquals(e[order[i]], ePermuted[i], "bit for bit, row " + i);
    }

    /** The plane through the fitted values (y − e) of three rows: intercept and the two slopes. */
    private static double[] plane(final double[] y, final double[][] x, final double[] e) {
        final double[][] a = new double[3][3];
        final double[] b = new double[3];
        for (int i = 0; i < y.length; i++) {
            final double[] row = {1, x[0][i], x[1][i]};
            for (int k = 0; k < 3; k++) {
                b[k] += row[k] * (y[i] - e[i]);
                for (int l = 0; l < 3; l++) a[k][l] += row[k] * row[l];
            }
        }
        return GroupOps.solve(a, b);
    }

    @Test
    public void testHarvilleHandValues() {
        final double[] p = {0.5, 0.3, 0.2};
        // second place: sum over the winner j of p_j * p_i / (1 - p_j)
        final double a2 = 0.3 * 0.5 / 0.7 + 0.2 * 0.5 / 0.8, b2 = 0.5 * 0.3 / 0.5 + 0.2 * 0.3 / 0.8, c2 = 0.5 * 0.2 / 0.5 + 0.3 * 0.2 / 0.7;
        Assertions.assertArrayEquals(new double[]{0.5, 0.3, 0.2}, GroupOps.harville(p, 1, new double[0], 64), 1e-12);
        Assertions.assertArrayEquals(new double[]{0.5 + a2, 0.3 + b2, 0.2 + c2}, GroupOps.harville(p, 2, new double[0], 64), 1e-12);
        Assertions.assertArrayEquals(new double[]{1, 1, 1}, GroupOps.harville(p, 3, new double[0], 64), 1e-12);
        // implied probabilities are normalised; a missing or negative value takes no part; a zero one can only lose
        Assertions.assertArrayEquals(new double[]{0.5 + a2, NaN, 0.3 + b2, 0.2 + c2, NaN, 0},
                GroupOps.harville(new double[]{0.6, NaN, 0.36, 0.24, -1, 0}, 2, new double[0], 64), 1e-12);
        // two runners: both are within the first two (and three)
        Assertions.assertArrayEquals(new double[]{1, 1}, GroupOps.harville(new double[]{0.9, 0.1}, 3, new double[0], 64), 1e-12);
        // nothing positive, or more rows than the bound: null
        for (final double v : GroupOps.harville(new double[]{0, 0}, 2, new double[0], 64)) Assertions.assertTrue(Double.isNaN(v));
        for (final double v : GroupOps.harville(p, 2, new double[0], 2)) Assertions.assertTrue(Double.isNaN(v));
        Assertions.assertThrows(IllegalArgumentException.class, () -> GroupOps.harville(p, 4, new double[0], 64));
    }

    @Test
    public void testHarvilleProperties() {
        final Random random = new Random(9);
        for (final double[] discount : List.of(new double[0], new double[]{0.81, 0.65}, new double[]{0.9})) {
            for (int trial = 0; trial < 20; trial++) {
                final int n = 4 + random.nextInt(12);
                final double[] p = new double[n];
                for (int i = 0; i < n; i++) p[i] = random.nextDouble() + 0.01;
                for (int top = 1; top <= 3; top++) {
                    final double[] within = GroupOps.harville(p, top, discount, 64);
                    double sum = 0;
                    for (final double v : within) sum += v;
                    // every place is taken by exactly one runner
                    Assertions.assertEquals(top, sum, 1e-9, "top " + top);
                    // a stronger runner is more likely to be within the first k
                    for (int i = 0; i < n; i++) for (int j = 0; j < n; j++) {
                        if (p[i] > p[j]) Assertions.assertTrue(within[i] > within[j], "top " + top);
                    }
                }
                // the order the group arrives in does not reach the output
                final double[] reversed = new double[n];
                for (int i = 0; i < n; i++) reversed[i] = p[n - 1 - i];
                final double[] a = GroupOps.harville(p, 3, discount, 64), b = GroupOps.harville(reversed, 3, discount, 64);
                for (int i = 0; i < n; i++) Assertions.assertEquals(a[i], b[n - 1 - i], "bit for bit");
            }
        }
        // a discount below 1 flattens the later places: the favourite is placed less often, the outsider more
        final double[] p = {0.6, 0.2, 0.1, 0.1};
        final double[] plain = GroupOps.harville(p, 3, new double[0], 64), discounted = GroupOps.harville(p, 3, new double[]{0.81, 0.65}, 64);
        Assertions.assertTrue(discounted[0] < plain[0] && discounted[3] > plain[3]);
    }

    private static final String SOURCES = """
            sources:
              - name: listings
                eventTime: session_time
                keys: [session_id, seller_id]
                fields:
                  - {name: session_id, type: string}
                  - {name: seller_id, type: string}
                  - {name: category, type: string}
                  - {name: start_price, type: float64}
                  - {name: quantity, type: int32}
                  - {name: model_score, type: float64}
            """;

    private static final String SPEC = """
            lineage:
              - {fields: [session_id, seller_id, category, start_price, quantity, model_score], from: listings}
            time: {field: session_time, orderTieBreak: [session_id]}
            predictAt: "event_time - PT10M"
            contexts:
              - {name: session, keys: [session_id]}
            features:
              - name: neutral
                scope: context
                context: session
                ops:
                  - {type: residualize, field: model_score, against: [log_price, quantity]}
                  - {type: residualize, field: start_price, against: quantity, as: price}
              - name: log_price
                scope: row
                expr: "ln(start_price)"
              - name: prob
                scope: context
                context: session
                ops:
                  - {type: softmax, field: model_score, as: pWin}
              - name: placed
                scope: context
                context: session
                ops:
                  - {type: harville, field: prob_pWin_softmax, as: p, top: [2, 3], discount: [0.81, 0.65]}
            """;

    private static FeaturePlan compile(final String spec) {
        final JsonObject sources = Config.convertConfigJson(SOURCES, Config.Format.yaml);
        return FeaturePlanCompiler.compile(sources, Config.convertConfigJson(spec, Config.Format.yaml), null);
    }

    private static boolean hasCode(final FeaturePlan plan, final String code) {
        return plan.getDiagnostics().getMessages().stream().anyMatch(m -> m.code().equals(code));
    }

    @Test
    public void testCompile() {
        final FeaturePlan plan = compile(SPEC);
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        final OutputColumn neutral = plan.getColumn("neutral_model_score_residualize");
        Assertions.assertNotNull(neutral, plan::describe);
        // the regressors may be derived columns declared later in the config
        Assertions.assertEquals("log_price,quantity", neutral.getCoordinates().get("against"));
        Assertions.assertTrue(neutral.getInputs().containsAll(List.of("model_score", "log_price", "quantity", "session_id")), neutral.getInputs().toString());
        Assertions.assertEquals("quantity", plan.getColumn("neutral_price_residualize").getCoordinates().get("against"));
        final OutputColumn top2 = plan.getColumn("placed_p_harville_top2"), top3 = plan.getColumn("placed_p_harville_top3");
        Assertions.assertNotNull(top2, plan::describe);
        Assertions.assertEquals("3", top3.getCoordinates().get("top"));
        Assertions.assertEquals("0.81,0.65", top3.getCoordinates().get("discount"));
        Assertions.assertEquals("64", top3.getCoordinates().get("maxGroupSize"));
        Assertions.assertEquals("prob_pWin_softmax", top3.getCoordinates().get("field"));
        Assertions.assertTrue(hasCode(plan, "context.op.groupSolver"), plan::describe);
        // excludeSelf: a leave-one-out residual; meaningless for harville
        final FeaturePlan loo = compile(SPEC.replace("    context: session\n    ops:\n      - {type: residualize, field: model_score",
                "    context: session\n    excludeSelf: true\n    ops:\n      - {type: residualize, field: model_score"));
        Assertions.assertEquals("true", loo.getColumn("neutral_model_score_residualize").getCoordinates().get("excludeSelf"), loo::describe);
    }

    @Test
    public void testCompileErrors() {
        final String residualize = "      - {type: residualize, field: model_score, against: [log_price, quantity]}";
        final String harville = "      - {type: harville, field: prob_pWin_softmax, as: p, top: [2, 3], discount: [0.81, 0.65]}";
        Assertions.assertTrue(SPEC.contains(residualize) && SPEC.contains(harville));
        final Map<String, String> cases = new java.util.LinkedHashMap<>();
        cases.put(residualize.replace(", against: [log_price, quantity]", ""), "context.residualize.against");
        cases.put(residualize.replace("[log_price, quantity]", "[category]"), "context.residualize.against");
        cases.put(residualize.replace("[log_price, quantity]", "[model_score]"), "context.residualize.against");
        cases.put(residualize.replace("[log_price, quantity]", "[quantity, quantity]"), "context.residualize.against");
        for (final Map.Entry<String, String> e : cases.entrySet()) {
            final FeaturePlan plan = compile(SPEC.replace(residualize, e.getKey()));
            Assertions.assertTrue(hasCode(plan, e.getValue()), () -> e.getKey() + "\n" + plan.describe());
        }
        cases.clear();
        cases.put(harville.replace("top: [2, 3]", "top: [4]"), "context.harville.top");
        cases.put(harville.replace("top: [2, 3]", "top: [2, 2]"), "context.harville.top");
        cases.put(harville.replace("top: [2, 3]", "top: [1.5]"), "context.harville.top");
        cases.put(harville.replace("[0.81, 0.65]", "[0.8, 0.7, 0.6]"), "context.harville.discount");
        cases.put(harville.replace("[0.81, 0.65]", "[0]"), "context.harville.discount");
        cases.put(harville.replace("top: [2, 3]", "maxGroupSize: 1"), "context.op.maxGroupSize");
        cases.put(harville.replace("field: prob_pWin_softmax", "field: category"), "context.op.type");
        for (final Map.Entry<String, String> e : cases.entrySet()) {
            final FeaturePlan plan = compile(SPEC.replace(harville, e.getKey()));
            Assertions.assertTrue(hasCode(plan, e.getValue()), () -> e.getKey() + "\n" + plan.describe());
        }
        // a misspelled regressor is an unresolved reference of the block, like any other field
        final FeaturePlan typo = compile(SPEC.replace("[log_price, quantity]", "[log_prise, quantity]"));
        Assertions.assertTrue(typo.getDiagnostics().hasErrors(), typo::describe);
    }

}
