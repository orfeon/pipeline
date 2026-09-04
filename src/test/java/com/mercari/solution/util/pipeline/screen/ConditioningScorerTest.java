package com.mercari.solution.util.pipeline.screen;

import com.google.gson.JsonParser;
import com.mercari.solution.module.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConditioningScorerTest {

    private static final Schema SCHEMA = Schema.builder()
            .withField("g", Schema.FieldType.STRING)
            .withField("y", Schema.FieldType.INT64)
            .withField("b", Schema.FieldType.FLOAT64)
            .withField("t", Schema.FieldType.TIMESTAMP)
            .withField("x", Schema.FieldType.FLOAT64)
            .withField("f", Schema.FieldType.FLOAT64)
            .build();

    private static ScreenSpec spec(final String json) {
        return ScreenSpec.parse(JsonParser.parseString(json).getAsJsonObject()).resolve(SCHEMA, null);
    }

    /** x layout: [x (candidate), f (conditioning)] — no shuffle column. */
    private static ScreenRow row(final String g, final int i, final double y, final double x, final double f) {
        return new ScreenRow(g, g + ":" + i, 1, null, y, Double.NaN, 1d, new double[]{x, f});
    }

    /** moments of a standardised column: n = 3, sum = 0, sum of squares = 3 → mean 0, std 1 */
    private static final double[] UNIT_MOMENTS = {3, 0, 3};

    @Test
    public void testSpecResolvesConditioningColumns() {
        final ScreenSpec s = spec("{family: groupedMultinomial, group: g, label: y, time: t, candidates: [x], transforms: [raw], placebo: {noise: 0}, conditioning: {fields: [f], l2: 0, maxIter: 5}}");
        Assertions.assertEquals(List.of("f"), s.conditioningFields);
        Assertions.assertEquals(List.of("x", "f"), s.rowColumns());
        Assertions.assertEquals(1, s.conditioningOffset());
        Assertions.assertEquals(5, s.conditioningMaxIter);
        Assertions.assertTrue(s.hasConditioning());
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: groupedMultinomial, group: g, label: y, candidates: [x], conditioning: {fields: [y]}}"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> spec("{family: groupedMultinomial, group: g, label: y, candidates: [x], conditioning: {fields: [f], maxIter: 0}}"));
    }

    @Test
    public void testGroupedEvaluationHandComputed() {
        // one group, uniform p = 1/3, y = [1,0,0], F = [1,0,-1] (already standardised)
        // ll(0) = log(1/3); g = (1-1/3)*1 + (0-1/3)*0 + (0-1/3)*(-1) = 1; G = (1/3)(1+0+1) - 0 = 2/3
        final ScreenSpec s = spec("{family: groupedMultinomial, group: g, label: y, time: t, candidates: [x], transforms: [raw], placebo: {noise: 0}, conditioning: {fields: [f], l2: 0}}");
        final GroupScorer groups = new GroupScorer(s);
        final ConditioningScorer scorer = new ConditioningScorer(s);
        Assertions.assertEquals(1, scorer.k);
        final GroupScorer.Unit unit = groups.prepare(List.of(row("a", 0, 1, 3, 1), row("a", 1, 0, 1, 0), row("a", 2, 0, 2, -1)), "a");
        final double[] eval = scorer.evaluate(unit, new double[]{0}, UNIT_MOMENTS);
        Assertions.assertEquals(1, eval[0]);
        Assertions.assertEquals(Math.log(1d / 3), eval[1], 1e-12);
        Assertions.assertEquals(1d, eval[2], 1e-12);
        Assertions.assertEquals(2d / 3, eval[3], 1e-12);

        // controller: the first evaluation is accepted and proposes the Newton step d = g / G = 1.5
        final FitState state = FitState.initial(1).advance(eval, 0d, 1e-8);
        Assertions.assertTrue(state.hasBest);
        Assertions.assertEquals(1.5, state.proposal[0], 1e-12);
        Assertions.assertEquals(Math.log(1d / 3), state.ll0, 1e-12);
        Assertions.assertFalse(state.converged);

        // at θ = 1.5: p̂ = softmax(1.5, 0, -1.5) = [0.787, 0.176, 0.039]; the objective improved, so the step is accepted
        final double[] eval2 = scorer.evaluate(unit, state.proposal, UNIT_MOMENTS);
        final double[] pHat = scorer.fitted(unit, scorer.design(unit, UNIT_MOMENTS), state.proposal);
        Assertions.assertEquals(Math.exp(1.5) / (Math.exp(1.5) + 1 + Math.exp(-1.5)), pHat[0], 1e-12);
        Assertions.assertTrue(eval2[1] > eval[1]);
        state.advance(eval2, 0d, 1e-8);
        Assertions.assertEquals(2, state.iteration);
        Assertions.assertEquals(1.5, state.bestTheta[0], 1e-12);
        Assertions.assertEquals(0, state.rejected);

        // a pass reporting a worse objective is rejected: the step is halved from the best point
        final double[] worse = eval2.clone();
        worse[1] = eval2[1] - 1;
        final double[] proposalBefore = state.proposal.clone();
        state.advance(worse, 0d, 1e-8);
        Assertions.assertEquals(1, state.rejected);
        Assertions.assertEquals(0.5, state.alpha, 1e-12);
        Assertions.assertEquals(1.5 + 0.5 * (proposalBefore[0] - 1.5), state.proposal[0], 1e-12);

        // an empty evaluation (skipped pass) leaves the state untouched; a converged state proposes its best point
        final int iteration = state.iteration;
        state.advance(new double[0], 0d, 1e-8);
        Assertions.assertEquals(iteration, state.iteration);
    }

    @Test
    public void testNewtonConvergesOnSeparableGroup() {
        // with L2 the fit of a perfectly separated group converges to a finite θ within a few passes
        final ScreenSpec s = spec("{family: groupedMultinomial, group: g, label: y, time: t, candidates: [x], transforms: [raw], placebo: {noise: 0}, conditioning: {fields: [f], l2: 0.1, maxIter: 20}}");
        final GroupScorer groups = new GroupScorer(s);
        final ConditioningScorer scorer = new ConditioningScorer(s);
        final GroupScorer.Unit unit = groups.prepare(List.of(row("a", 0, 1, 3, 1), row("a", 1, 0, 1, 0), row("a", 2, 0, 2, -1)), "a");
        FitState state = FitState.initial(1);
        int passes = 0;
        while (!state.converged && passes < 20) {
            state.advance(scorer.evaluate(unit, state.proposal, UNIT_MOMENTS), 0.1, 1e-10);
            passes++;
        }
        Assertions.assertTrue(state.converged, "passes " + passes);
        Assertions.assertTrue(passes < 15);
        // stationarity of the penalised objective: g / n = l2 θ
        Assertions.assertEquals(0.1 * state.bestTheta[0], state.bestGrad[0], 1e-6);
        Assertions.assertTrue(state.gainPerUnit() > 0);
    }

    @Test
    public void testPartialTestRemovesTheConditionedColumn() {
        // the candidate x IS the conditioning column: after orthogonalisation nothing is left (r2_F = 1)
        final ScreenSpec s = spec("{family: groupedMultinomial, group: g, label: y, time: t, candidates: [x], transforms: [raw], placebo: {noise: 0}, conditioning: {fields: [f], l2: 0}}");
        final GroupScorer groups = new GroupScorer(s);
        final ConditioningScorer scorer = new ConditioningScorer(s);
        final List<List<ScreenRow>> unitsRows = List.of(
                List.of(row("a", 0, 1, 1, 1), row("a", 1, 0, 0, 0), row("a", 2, 0, -1, -1)),
                List.of(row("b", 0, 0, 1, 1), row("b", 1, 1, 0, 0), row("b", 2, 0, -1, -1)),
                List.of(row("c", 0, 1, 1, 1), row("c", 1, 0, -1, -1), row("c", 2, 0, 0, 0)));
        final double[] moments = {9, 0, 6};
        FitState state = FitState.initial(1);
        for (int it = 0; it < 10 && !state.converged; it++) {
            final VectorAccumulator eval = new VectorAccumulator();
            for (final List<ScreenRow> rows : unitsRows) {
                final GroupScorer.Unit unit = groups.prepare(rows, rows.get(0).group);
                eval.add(scorer.evaluate(unit, state.proposal, moments));
            }
            state.advance(eval.getValues(), 0d, 1e-10);
        }
        Assertions.assertTrue(state.hasBest);
        final Map<Integer, double[]> partials = new HashMap<>();
        final Map<Integer, ScoreAccumulator> marginal = new HashMap<>();
        for (final List<ScreenRow> rows : unitsRows) {
            final GroupScorer.Unit unit = groups.prepare(rows, rows.get(0).group);
            scorer.partial(unit, groups.columns(unit), state.bestTheta, moments, partials);
            groups.score(rows, rows.get(0).group, marginal);
        }
        final double[] vec = partials.get(s.key(0, 0));
        // the sums at p̂: a = F̃'Wx = b / std when x = F (F̃ = F / std, std² = 6 / 9)
        Assertions.assertEquals(vec[1] / Math.sqrt(6d / 9), vec[2], 1e-9);
        final ScreenReport.Partial partial = ScreenReport.partial(vec, state, 3, 0d, 9);
        Assertions.assertEquals(1d, partial.r2(), 1e-9);
        Assertions.assertTrue(partial.stats().degenerate());
        Assertions.assertEquals(0d, partial.stats().estGain());
        // the marginal test still sees the column
        Assertions.assertFalse(ScreenReport.stats(s, marginal.get(s.key(0, 0)).getTotal(), 3).degenerate());

        // through the report: partial fields populated, passed follows the partial gain
        final ScreenReport.Result result = ScreenReport.build(s, marginal, partials, state);
        final Map<String, Object> record = result.records().get(0);
        Assertions.assertEquals(1d, (Double) record.get("r2_F"), 1e-9);
        Assertions.assertEquals(0d, record.get("partial_gain"));
        Assertions.assertEquals(Boolean.FALSE, record.get("passed"));
        Assertions.assertEquals(1L, result.summary().get("conditioningK"));
        Assertions.assertEquals(Boolean.TRUE, result.summary().get("conditioningConverged"));
        Assertions.assertEquals(List.of("f"), result.summary().get("conditioningFields"));
    }

    @Test
    public void testFitIsInvariantToWeightScale() {
        // rescaling the weight column must leave the fitted point, the pass count and the partial ridge unchanged
        final ScreenSpec s = spec("{family: groupedMultinomial, group: g, label: y, time: t, candidates: [x], transforms: [raw], placebo: {noise: 0}, conditioning: {fields: [f], l2: 0.1, maxIter: 20}}");
        final GroupScorer groups = new GroupScorer(s);
        final ConditioningScorer scorer = new ConditioningScorer(s);
        final FitState[] fits = new FitState[2];
        for (int run = 0; run < 2; run++) {
            final double w = run == 0 ? 1d : 1000d;
            final GroupScorer.Unit unit = groups.prepare(List.of(
                    new ScreenRow("a", "a:0", 1, null, 1, Double.NaN, w, new double[]{3, 1}),
                    new ScreenRow("a", "a:1", 1, null, 0, Double.NaN, w, new double[]{1, 0}),
                    new ScreenRow("a", "a:2", 1, null, 0, Double.NaN, w, new double[]{2, -1})), "a");
            FitState state = FitState.initial(1);
            int passes = 0;
            while (!state.converged && passes < 20) {
                state.advance(scorer.evaluate(unit, state.proposal, UNIT_MOMENTS), 0.1, 1e-10);
                passes++;
            }
            fits[run] = state;
        }
        Assertions.assertEquals(fits[0].iteration, fits[1].iteration);
        Assertions.assertEquals(fits[0].bestTheta[0], fits[1].bestTheta[0], 1e-9);
        Assertions.assertEquals(fits[0].gainPerUnit(), fits[1].gainPerUnit(), 1e-9);
        Assertions.assertEquals(1000d, fits[1].nUnits, 1e-9);
    }

    @Test
    public void testNonFiniteStartIsNotAcceptedAsBest() {
        final FitState state = FitState.initial(1).advance(new double[]{1, Double.NaN, Double.NaN, Double.NaN}, 0d, 1e-8);
        Assertions.assertFalse(state.hasBest);
        Assertions.assertTrue(state.converged);
        Assertions.assertEquals(1, state.iteration);
    }

    @Test
    public void testBinomialEvaluationCarriesAnIntercept() {
        // prior mode, two independent rows y = [1, 0], F = [1, -1] standardised: p̂(0) = 0.5
        // ll = 2 log 0.5 ; g = [(1-.5)*1 + (0-.5)*(-1), (1-.5) + (0-.5)] = [1, 0] ; G = 0.25 * [[2, 0], [0, 2]]
        final ScreenSpec s = spec("{family: binomial, label: y, time: t, candidates: [x], placebo: {noise: 0}, conditioning: {fields: [f], l2: 0}}");
        final GroupScorer groups = new GroupScorer(s);
        final ConditioningScorer scorer = new ConditioningScorer(s);
        Assertions.assertEquals(2, scorer.k);
        final VectorAccumulator eval = new VectorAccumulator();
        eval.add(scorer.evaluate(groups.prepare(List.of(row(null, 0, 1, 2, 1)), "r0"), new double[]{0, 0}, new double[]{2, 0, 2}));
        eval.add(scorer.evaluate(groups.prepare(List.of(row(null, 1, 0, 0, -1)), "r1"), new double[]{0, 0}, new double[]{2, 0, 2}));
        final double[] e = eval.getValues();
        Assertions.assertEquals(2, e[0]);
        Assertions.assertEquals(2 * Math.log(0.5), e[1], 1e-12);
        Assertions.assertEquals(1d, e[2], 1e-12);
        Assertions.assertEquals(0d, e[3], 1e-12);
        Assertions.assertEquals(0.5, e[4], 1e-12);
        Assertions.assertEquals(0d, e[5], 1e-12);
        Assertions.assertEquals(0.5, e[7], 1e-12);
    }
}
