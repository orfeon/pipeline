package com.mercari.solution.util.pipeline.screen;

import com.google.gson.JsonParser;
import com.mercari.solution.module.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** The gaussian and poisson families: hand-computed marginal statistics and the conditioning links. */
public class FamilyScorerTest {

    private static final Schema SCHEMA = Schema.builder()
            .withField("y", Schema.FieldType.FLOAT64)
            .withField("b", Schema.FieldType.FLOAT64)
            .withField("t", Schema.FieldType.TIMESTAMP)
            .withField("x", Schema.FieldType.FLOAT64)
            .withField("f", Schema.FieldType.FLOAT64)
            .build();

    private static ScreenSpec spec(final String json) {
        return ScreenSpec.parse(JsonParser.parseString(json).getAsJsonObject()).resolve(SCHEMA, null);
    }

    /** x layout: [x (candidate), f (conditioning column when configured)] */
    private static ScreenRow row(final int i, final double y, final double b, final double x, final double f) {
        return new ScreenRow(null, "r" + i, i, null, y, b, 1d, new double[]{x, f});
    }

    private static ScreenReport.Stats marginal(final ScreenSpec spec, final double[] y, final double[] b, final double[] x) {
        final GroupScorer scorer = new GroupScorer(spec);
        final Map<Integer, ScoreAccumulator> acc = new HashMap<>();
        for (int i = 0; i < y.length; i++) scorer.score(List.of(row(i, y[i], b[i], x[i], 0)), "r" + i, acc);
        return ScreenReport.stats(spec, acc.get(spec.key(0, 0)).getTotal(), y.length);
    }

    @Test
    public void testGaussianPriorHandComputed() {
        // y = [1,3,2,6], x = [0,1,2,3]: sxy = 25 - 1.5*12 = 7, sxx = 5, sigma2 = 50/4 - 9 = 3.5
        // S = 7/3.5 = 2, H = 5/3.5, chi2 = 49 / (5 * 3.5) = 2.8
        final ScreenSpec s = spec("{family: gaussian, label: y, time: t, candidates: [x], placebo: {noise: 0}}");
        final ScreenReport.Stats st = marginal(s, new double[]{1, 3, 2, 6}, new double[]{Double.NaN, Double.NaN, Double.NaN, Double.NaN}, new double[]{0, 1, 2, 3});
        Assertions.assertEquals(2d, st.s(), 1e-12);
        Assertions.assertEquals(5 / 3.5, st.h(), 1e-12);
        Assertions.assertEquals(2.8, st.chi2(), 1e-12);
        Assertions.assertEquals(2.8 / 8, st.estGain(), 1e-12);
    }

    @Test
    public void testGaussianOffsetHandComputed() {
        // baseline value mu = [1,2,3,4]: r = [0,1,-1,2], c1 = 5, c2 = 2, sxx = 5, sigma2 = 6/4 - 0.25 = 1.25
        // S = (5 - 1.5*2) / 1.25 = 1.6, H = 5 / 1.25 = 4, chi2 = 0.64
        final ScreenSpec s = spec("{family: gaussian, label: y, baseline: b, time: t, candidates: [x], placebo: {noise: 0}}");
        Assertions.assertEquals("value", s.baselineForm);
        final ScreenReport.Stats st = marginal(s, new double[]{1, 3, 2, 6}, new double[]{1, 2, 3, 4}, new double[]{0, 1, 2, 3});
        Assertions.assertEquals(1.6, st.s(), 1e-12);
        Assertions.assertEquals(4d, st.h(), 1e-12);
        Assertions.assertEquals(0.64, st.chi2(), 1e-12);
        // the statistic is invariant to the scale of the label and the baseline together
        final ScreenReport.Stats scaled = marginal(s, new double[]{10, 30, 20, 60}, new double[]{10, 20, 30, 40}, new double[]{0, 1, 2, 3});
        Assertions.assertEquals(st.chi2(), scaled.chi2(), 1e-12);
    }

    @Test
    public void testPoissonPriorHandComputed() {
        // y = [0,1,2,5], x = [0,1,2,3]: ybar = 2, S = 20 - 1.5*8 = 8, H = ybar * sxx = 10, chi2 = 6.4
        final ScreenSpec s = spec("{family: poisson, label: y, time: t, candidates: [x], placebo: {noise: 0}}");
        final ScreenReport.Stats st = marginal(s, new double[]{0, 1, 2, 5}, new double[]{Double.NaN, Double.NaN, Double.NaN, Double.NaN}, new double[]{0, 1, 2, 3});
        Assertions.assertEquals(8d, st.s(), 1e-12);
        Assertions.assertEquals(10d, st.h(), 1e-12);
        Assertions.assertEquals(6.4, st.chi2(), 1e-12);
    }

    @Test
    public void testPoissonOffsetHandComputed() {
        // rate mu = [1,1,2,2]: r = [-1,0,0,3], c1 = 9, c2 = 2, c3 = 27, c4 = 11, c5 = 6
        // S = 9 - (11/6)*2, H = 27 - 121/6
        final ScreenSpec s = spec("{family: poisson, label: y, baseline: {field: b, form: rate}, time: t, candidates: [x], placebo: {noise: 0}}");
        final ScreenReport.Stats st = marginal(s, new double[]{0, 1, 2, 5}, new double[]{1, 1, 2, 2}, new double[]{0, 1, 2, 3});
        Assertions.assertEquals(9 - 11d / 6 * 2, st.s(), 1e-12);
        Assertions.assertEquals(27 - 121d / 6, st.h(), 1e-12);
        // logRate is the same baseline on the log scale
        final ScreenSpec log = spec("{family: poisson, label: y, baseline: {field: b, form: logRate}, time: t, candidates: [x], placebo: {noise: 0}}");
        final ScreenReport.Stats lst = marginal(log, new double[]{0, 1, 2, 5}, new double[]{0, 0, Math.log(2), Math.log(2)}, new double[]{0, 1, 2, 3});
        Assertions.assertEquals(st.chi2(), lst.chi2(), 1e-12);
        // a non-positive rate invalidates the unit
        final GroupScorer scorer = new GroupScorer(s);
        Assertions.assertEquals(GroupScorer.Skip.INVALID_BASELINE, scorer.score(List.of(row(9, 1, 0, 1, 0)), "r9", new HashMap<>()));
    }

    @Test
    public void testFamilyFormValidation() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: gaussian, label: y, baseline: {field: b, form: prob}, candidates: [x]}"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: poisson, label: y, baseline: {field: b, form: value}, candidates: [x]}"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: binomial, label: y, baseline: {field: b, form: rate}, candidates: [x]}"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: gamma, label: y, candidates: [x]}"));
        Assertions.assertEquals("rate", spec("{family: poisson, label: y, baseline: b, candidates: [x]}").baselineForm);
    }

    @Test
    public void testPoissonAndGaussianEvaluationHandComputed() {
        // two rows y = [1, 3], F standardised = [1, -1] (moments n = 2, sum 0, sum of squares 2), theta = 0
        final double[] moments = {2, 0, 2};
        final ScreenSpec poisson = spec("{family: poisson, label: y, time: t, candidates: [x], placebo: {noise: 0}, conditioning: {fields: [f], l2: 0}}");
        final GroupScorer groups = new GroupScorer(poisson);
        final ConditioningScorer ps = new ConditioningScorer(poisson);
        final VectorAccumulator pe = new VectorAccumulator();
        pe.add(ps.evaluate(groups.prepare(List.of(row(0, 1, Double.NaN, 2, 1)), "r0"), new double[]{0, 0}, moments));
        pe.add(ps.evaluate(groups.prepare(List.of(row(1, 3, Double.NaN, 0, -1)), "r1"), new double[]{0, 0}, moments));
        // mu = 1: ll = sum(y log 1 - 1) = -2 ; g = sum (y - 1) [F, 1] = [-2, 2] ; G = sum mu F F' = diag(2, 2)
        final double[] e = pe.getValues();
        Assertions.assertEquals(2, e[0]);
        Assertions.assertEquals(-2d, e[1], 1e-12);
        Assertions.assertEquals(-2d, e[2], 1e-12);
        Assertions.assertEquals(2d, e[3], 1e-12);
        Assertions.assertEquals(2d, e[4], 1e-12);
        Assertions.assertEquals(0d, e[5], 1e-12);
        Assertions.assertEquals(2d, e[7], 1e-12);

        final ScreenSpec gaussian = spec("{family: gaussian, label: y, time: t, candidates: [x], placebo: {noise: 0}, conditioning: {fields: [f], l2: 0}}");
        final GroupScorer ggroups = new GroupScorer(gaussian);
        final ConditioningScorer gs = new ConditioningScorer(gaussian);
        final VectorAccumulator ge = new VectorAccumulator();
        ge.add(gs.evaluate(ggroups.prepare(List.of(row(0, 1, Double.NaN, 2, 1)), "r0"), new double[]{0, 0}, moments));
        ge.add(gs.evaluate(ggroups.prepare(List.of(row(1, 3, Double.NaN, 0, -1)), "r1"), new double[]{0, 0}, moments));
        // mu = 0: ll = -(1 + 9)/2 = -5 ; g = sum y [F, 1] = [-2, 4] ; G = sum F F' = diag(2, 2)
        final double[] f = ge.getValues();
        Assertions.assertEquals(-5d, f[1], 1e-12);
        Assertions.assertEquals(-2d, f[2], 1e-12);
        Assertions.assertEquals(4d, f[3], 1e-12);
        Assertions.assertEquals(2d, f[4], 1e-12);
        Assertions.assertEquals(2d, f[7], 1e-12);
        // least squares: one Newton step lands on the exact fit (theta = G^-1 g per unit = [-1, 2] -> mu = y)
        final FitState state = FitState.initial(2).advance(f, 0d, 1e-10);
        Assertions.assertEquals(-1d, state.proposal[0], 1e-12);
        Assertions.assertEquals(2d, state.proposal[1], 1e-12);
    }

    @Test
    public void testFitStartsAtThePriorMeanWithoutBaseline() {
        // poisson counts with mean 1000: the intercept starts at log ȳ (at θ = 0 the mean is 1 and the first Newton
        // step overshoots by exp(999)); binomial at logit ȳ, gaussian at ȳ; with a baseline the offset carries it
        final ScreenSpec poisson = spec("{family: poisson, label: y, time: t, candidates: [x], placebo: {noise: 0}, conditioning: {fields: [f]}}");
        final ConditioningScorer ps = new ConditioningScorer(poisson);
        final VectorAccumulator moments = new VectorAccumulator();
        moments.add(ps.moments(row(0, 800, Double.NaN, 1, 1)));
        moments.add(ps.moments(row(1, 1200, Double.NaN, 1, -1)));
        final double[] theta = ps.initialTheta(moments.getValues());
        Assertions.assertEquals(2, theta.length);
        Assertions.assertEquals(0d, theta[0]);
        Assertions.assertEquals(Math.log(1000), theta[1], 1e-12);
        Assertions.assertEquals(2000d, moments.getValues()[3]);   // Σ w y after the [n, Σ, Σ²] of the one column
        Assertions.assertEquals(2d, moments.getValues()[4]);      // Σ w
        final FitState state = FitState.initial(2, theta);
        final VectorAccumulator eval = new VectorAccumulator();
        eval.add(ps.evaluate(new GroupScorer(poisson).prepare(List.of(row(0, 800, Double.NaN, 1, 1)), "r0"), state.proposal, moments.getValues()));
        eval.add(ps.evaluate(new GroupScorer(poisson).prepare(List.of(row(1, 1200, Double.NaN, 1, -1)), "r1"), state.proposal, moments.getValues()));
        state.advance(eval.getValues(), 0d, 1e-10);
        Assertions.assertTrue(state.hasBest);
        Assertions.assertEquals(0d, state.bestGrad[1], 1e-9);   // the intercept gradient vanishes at log ȳ

        final ScreenSpec binomial = spec("{family: binomial, label: y, time: t, candidates: [x], placebo: {noise: 0}, conditioning: {fields: [f]}}");
        final ConditioningScorer bs = new ConditioningScorer(binomial);
        final VectorAccumulator bm = new VectorAccumulator();
        bm.add(bs.moments(row(0, 1, Double.NaN, 1, 1)));
        bm.add(bs.moments(row(1, 0, Double.NaN, 1, -1)));
        bm.add(bs.moments(row(2, 0, Double.NaN, 1, 0)));
        bm.add(bs.moments(row(3, 0, Double.NaN, 1, 0)));
        Assertions.assertEquals(Math.log(0.25 / 0.75), bs.initialTheta(bm.getValues())[1], 1e-12);

        final ScreenSpec offset = spec("{family: poisson, label: y, baseline: b, time: t, candidates: [x], placebo: {noise: 0}, conditioning: {fields: [f]}}");
        Assertions.assertEquals(0d, new ConditioningScorer(offset).initialTheta(moments.getValues())[1]);
    }

    @Test
    public void testGaussianPartialUsesTheResidualVariance() {
        // three rows, y = 2 f + e; conditioning on f leaves a candidate x = f fully explained (r2_F = 1) and a
        // candidate x2 unrelated to f finite, scaled by the residual variance at the fitted model
        final ScreenSpec s = spec("{family: gaussian, label: y, time: t, candidates: [x], placebo: {noise: 0}, conditioning: {fields: [f], l2: 0, maxIter: 5}}");
        final GroupScorer groups = new GroupScorer(s);
        final ConditioningScorer scorer = new ConditioningScorer(s);
        final double[][] data = {{2.5, 1}, {-1.5, -1}, {0.5, 0}, {4.2, 2}, {-3.9, -2}};   // y, f (x = f)
        final double[] moments = {5, 0, 10};
        FitState state = FitState.initial(2);
        for (int it = 0; it < 5 && !state.converged; it++) {
            final VectorAccumulator eval = new VectorAccumulator();
            for (int i = 0; i < data.length; i++) eval.add(scorer.evaluate(groups.prepare(List.of(row(i, data[i][0], Double.NaN, data[i][1], data[i][1])), "r" + i), state.proposal, moments));
            state.advance(eval.getValues(), 0d, 1e-10);
        }
        Assertions.assertTrue(state.hasBest);
        final Map<Integer, double[]> partials = new HashMap<>();
        final Map<Integer, ScoreAccumulator> marginal = new HashMap<>();
        for (int i = 0; i < data.length; i++) {
            final List<ScreenRow> rows = List.of(row(i, data[i][0], Double.NaN, data[i][1], data[i][1]));
            final GroupScorer.Unit unit = groups.prepare(rows, "r" + i);
            scorer.partial(unit, groups.columns(unit), state.bestTheta, moments, partials);
            groups.score(rows, "r" + i, marginal);
        }
        final double[] sig = partials.get(ConditioningScorer.SIGMA_KEY);
        Assertions.assertNotNull(sig);
        Assertions.assertTrue(sig[0] > 0 && sig[1] == 5);
        final ScreenReport.Result result = ScreenReport.build(s, marginal, partials, state);
        final Map<String, Object> record = result.records().get(0);
        Assertions.assertEquals(1d, (Double) record.get("r2_F"), 1e-9);
        Assertions.assertEquals(0d, record.get("partial_gain"));
        Assertions.assertFalse(((Double) record.get("est_gain")).isNaN());
        Assertions.assertTrue((Double) record.get("z") > 1, "marginal z: " + record.get("z"));   // five rows only
    }
}
