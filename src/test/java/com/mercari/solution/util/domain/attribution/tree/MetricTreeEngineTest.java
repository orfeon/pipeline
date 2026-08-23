package com.mercari.solution.util.domain.attribution.tree;

import com.mercari.solution.util.domain.attribution.LeafTable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class MetricTreeEngineTest {

    private static final double EPS = 1e-9;
    private static final String[] ALL = { "all" };

    private static MetricTreeSpec.Node node(final String name, final String field, final String expression) {
        return new MetricTreeSpec.Node(name, field, expression, null, null, null, null, null);
    }

    private static MetricTreeSpec.Node sum(final String name, final String... components) {
        return new MetricTreeSpec.Node(name, null, null, MetricTreeSpec.Decomposition.sum, List.of(components), null, null, null);
    }

    private static MetricTreeSpec.Node product(final String name, final String volume, final String rate) {
        return new MetricTreeSpec.Node(name, null, null, MetricTreeSpec.Decomposition.product, null, volume, rate, null);
    }

    private static MetricTreeSpec spec(final MetricTreeSpec.Node... nodes) {
        return new MetricTreeSpec(List.of(nodes), List.of(), null, MetricTreeSpec.DEFAULT_MIN_PARENT_DELTA_RATIO);
    }

    private static Map<String, MetricTreeEngine.NodeResult> byPath(final MetricTreeEngine.TreeResult result) {
        final Map<String, MetricTreeEngine.NodeResult> map = new java.util.LinkedHashMap<>();
        for(final MetricTreeEngine.NodeResult row : result.nodes()) {
            map.put(row.path(), row);
        }
        return map;
    }

    /** Every child group must sum to its parent's contribution (MECE additivity). */
    private static void assertAdditive(final MetricTreeEngine.TreeResult result) {
        final Map<String, Double> contribution = new java.util.HashMap<>();
        for(final MetricTreeEngine.NodeResult row : result.nodes()) {
            contribution.put(row.path(), row.contribution());
        }
        // group children by (parent path, decomposition group): static children share the parent's
        // decomposition, breakdown children share dimension
        final Map<String, Double> groupSums = new java.util.HashMap<>();
        final Map<String, Boolean> degenerateParents = new java.util.HashMap<>();
        for(final MetricTreeEngine.NodeResult row : result.nodes()) {
            degenerateParents.put(row.path(), row.degenerate());
        }
        for(final MetricTreeEngine.NodeResult row : result.nodes()) {
            if(row.parent() == null) {
                continue;
            }
            final String group = row.parent() + "|" + (row.dimension() == null ? "static" : row.dimension());
            groupSums.merge(group, row.contribution(), Double::sum);
        }
        for(final Map.Entry<String, Double> e : groupSums.entrySet()) {
            final String parent = e.getKey().substring(0, e.getKey().indexOf('|'));
            if(Boolean.TRUE.equals(degenerateParents.get(parent))) {
                continue;
            }
            Assertions.assertEquals(contribution.get(parent), e.getValue(), 1e-6,
                    "children of " + e.getKey() + " must sum to the parent contribution");
        }
    }

    @Test
    public void testType1SumOfStaticComponents() {
        final LeafTable.Builder b = LeafTable.builder(List.of("_"), List.of("a", "b"));
        b.addBaseline(ALL, new double[]{ 100, 50 });
        b.addTarget(ALL, new double[]{ 130, 40 });
        final MetricTreeSpec spec = spec(sum("y", "a", "b"), node("a", "a", null), node("b", "b", null));
        final MetricTreeEngine.TreeResult result = new MetricTreeEngine(spec, b.build()).run("y");

        final Map<String, MetricTreeEngine.NodeResult> rows = byPath(result);
        Assertions.assertEquals(20, result.rootDelta(), EPS);
        Assertions.assertEquals(30, rows.get("y/a").contribution(), EPS);
        Assertions.assertEquals(-10, rows.get("y/b").contribution(), EPS);
        Assertions.assertEquals(1.5, rows.get("y/a").explanatoryPower(), EPS);
        Assertions.assertEquals(1, rows.get("y/a").rank());
        Assertions.assertEquals(2, rows.get("y/b").rank());
        Assertions.assertEquals(0, rows.get("y").rank());
        assertAdditive(result);
    }

    @Test
    public void testType2ProductOrderedAllocation() {
        // revenue = units * aup ; rate effect uses n0, volume effect uses aup1 (Fig. 1)
        final LeafTable.Builder b = LeafTable.builder(List.of("_"), List.of("units", "revenue"));
        b.addBaseline(ALL, new double[]{ 100, 1000 });   // aup0 = 10
        b.addTarget(ALL, new double[]{ 120, 1440 });     // aup1 = 12
        final MetricTreeSpec spec = spec(
                product("revenue", "units", "aup"),
                node("units", "units", null),
                node("aup", null, "revenue / units"));
        final MetricTreeEngine.TreeResult result = new MetricTreeEngine(spec, b.build()).run("revenue");

        final Map<String, MetricTreeEngine.NodeResult> rows = byPath(result);
        Assertions.assertEquals(440, result.rootDelta(), EPS);
        Assertions.assertEquals(20 * 12, rows.get("revenue/units").contribution(), EPS);  // Δn · X̄1
        Assertions.assertEquals(2 * 100, rows.get("revenue/aup").contribution(), EPS);    // ΔX̄ · n0
        Assertions.assertEquals(MetricTreeEngine.Effect.volume, rows.get("revenue/units").effect());
        Assertions.assertEquals(MetricTreeEngine.Effect.rate, rows.get("revenue/aup").effect());
        Assertions.assertNull(rows.get("revenue").residual());
        assertAdditive(result);
    }

    @Test
    public void testType4WeightedAverageBreakdown() {
        // cvr = orders / sessions, broken down by channel with weight = sessions (the denominator)
        final LeafTable.Builder b = LeafTable.builder(List.of("channel"), List.of("orders", "sessions"));
        b.addBaseline(new String[]{ "app" }, new double[]{ 50, 500 });    // 10%
        b.addBaseline(new String[]{ "web" }, new double[]{ 25, 500 });    // 5%  -> overall 7.5%
        b.addTarget(new String[]{ "app" }, new double[]{ 80, 800 });      // 10%, share 80%
        b.addTarget(new String[]{ "web" }, new double[]{ 8, 200 });       // 4%, share 20% -> overall 8.8%
        final MetricTreeSpec.Breakdown byChannel = new MetricTreeSpec.Breakdown(
                "channel", MetricTreeSpec.Decomposition.weightedAverage, null, "sessions", null);
        final MetricTreeSpec spec = spec(new MetricTreeSpec.Node(
                "cvr", null, "orders / sessions", null, null, null, null, List.of(byChannel)));
        final MetricTreeEngine.TreeResult result = new MetricTreeEngine(spec, b.build()).run("cvr");

        final Map<String, MetricTreeEngine.NodeResult> rows = byPath(result);
        Assertions.assertEquals(0.088 - 0.075, result.rootDelta(), EPS);
        // share_k = Δp_k · (ȳ_k,0 − ȳ_0); rate_k = Δȳ_k · p_k,1
        Assertions.assertEquals((0.8 - 0.5) * (0.10 - 0.075), rows.get("cvr/channel=app/share").contribution(), EPS);
        Assertions.assertEquals((0.2 - 0.5) * (0.05 - 0.075), rows.get("cvr/channel=web/share").contribution(), EPS);
        Assertions.assertEquals(0.0, rows.get("cvr/channel=app/rate").contribution(), EPS);
        Assertions.assertEquals((0.04 - 0.05) * 0.2, rows.get("cvr/channel=web/rate").contribution(), EPS);
        Assertions.assertEquals("app", rows.get("cvr/channel=app/share").value());
        Assertions.assertEquals("channel", rows.get("cvr/channel=app/share").dimension());
        Assertions.assertNull(rows.get("cvr").residual(), "weight = denominator gives an exact decomposition");
        assertAdditive(result);
    }

    @Test
    public void testType4ResidualWhenWeightIsNotTheDenominator() {
        final LeafTable.Builder b = LeafTable.builder(List.of("channel"), List.of("orders", "sessions", "users"));
        b.addBaseline(new String[]{ "app" }, new double[]{ 50, 500, 10 });
        b.addBaseline(new String[]{ "web" }, new double[]{ 25, 500, 90 });
        b.addTarget(new String[]{ "app" }, new double[]{ 80, 800, 50 });
        b.addTarget(new String[]{ "web" }, new double[]{ 8, 200, 50 });
        final MetricTreeSpec.Breakdown byChannel = new MetricTreeSpec.Breakdown(
                "channel", MetricTreeSpec.Decomposition.weightedAverage, null, "users", null);
        final MetricTreeSpec spec = spec(new MetricTreeSpec.Node(
                "cvr", null, "orders / sessions", null, null, null, null, List.of(byChannel)));
        final MetricTreeEngine.TreeResult result = new MetricTreeEngine(spec, b.build()).run("cvr");
        Assertions.assertNotNull(byPath(result).get("cvr").residual());
    }

    @Test
    public void testType3SumOfProductsBreakdown() {
        // orders = Σ_k sessions_k · cvr_k
        final LeafTable.Builder b = LeafTable.builder(List.of("channel"), List.of("orders", "sessions"));
        b.addBaseline(new String[]{ "app" }, new double[]{ 50, 500 });
        b.addBaseline(new String[]{ "web" }, new double[]{ 25, 500 });
        b.addTarget(new String[]{ "app" }, new double[]{ 80, 800 });
        b.addTarget(new String[]{ "web" }, new double[]{ 8, 200 });
        final MetricTreeSpec.Breakdown byChannel = new MetricTreeSpec.Breakdown(
                "channel", MetricTreeSpec.Decomposition.sumOfProducts, "sessions", null, null);
        final MetricTreeSpec spec = spec(new MetricTreeSpec.Node(
                "orders", "orders", null, null, null, null, null, List.of(byChannel)));
        final MetricTreeEngine.TreeResult result = new MetricTreeEngine(spec, b.build()).run("orders");

        final Map<String, MetricTreeEngine.NodeResult> rows = byPath(result);
        Assertions.assertEquals(13, result.rootDelta(), EPS);
        Assertions.assertEquals(300 * 0.10, rows.get("orders/channel=app/volume").contribution(), EPS);   // Δn_k · X̄_k,1
        Assertions.assertEquals(0.0, rows.get("orders/channel=app/rate").contribution(), EPS);
        Assertions.assertEquals(-300 * 0.04, rows.get("orders/channel=web/volume").contribution(), EPS);
        Assertions.assertEquals((0.04 - 0.05) * 500, rows.get("orders/channel=web/rate").contribution(), EPS); // ΔX̄_k · n_k,0
        Assertions.assertEquals("sessions", rows.get("orders/channel=app/volume").node());
        assertAdditive(result);
    }

    @Test
    public void testRecursiveScalingAndNestedBreakdown() {
        // revenue = units * aup; aup broken down by deal, then (nested) by account within each deal group
        final LeafTable.Builder b = LeafTable.builder(List.of("deal", "account"), List.of("units", "revenue"));
        b.addBaseline(new String[]{ "deal", "biz" }, new double[]{ 20, 160 });
        b.addBaseline(new String[]{ "deal", "ind" }, new double[]{ 30, 270 });
        b.addBaseline(new String[]{ "nodeal", "biz" }, new double[]{ 25, 300 });
        b.addBaseline(new String[]{ "nodeal", "ind" }, new double[]{ 25, 270 });
        b.addTarget(new String[]{ "deal", "biz" }, new double[]{ 40, 280 });
        b.addTarget(new String[]{ "deal", "ind" }, new double[]{ 30, 300 });
        b.addTarget(new String[]{ "nodeal", "biz" }, new double[]{ 20, 260 });
        b.addTarget(new String[]{ "nodeal", "ind" }, new double[]{ 30, 330 });
        final MetricTreeSpec.Breakdown byAccount = new MetricTreeSpec.Breakdown(
                "account", MetricTreeSpec.Decomposition.weightedAverage, null, "units", null);
        final MetricTreeSpec.Breakdown byDeal = new MetricTreeSpec.Breakdown(
                "deal", MetricTreeSpec.Decomposition.weightedAverage, null, "units", List.of(byAccount));
        final MetricTreeSpec spec = spec(
                product("revenue", "units", "aup"),
                node("units", "units", null),
                new MetricTreeSpec.Node("aup", null, "revenue / units", null, null, null, null, List.of(byDeal)));
        final MetricTreeEngine.TreeResult result = new MetricTreeEngine(spec, b.build()).run("revenue");

        final Map<String, MetricTreeEngine.NodeResult> rows = byPath(result);
        final MetricTreeEngine.NodeResult aup = rows.get("revenue/aup");
        // Children of aup are scaled by C_aup / Δaup (Algorithm 1 line 10)
        final double scale = aup.contribution() / aup.delta();
        final MetricTreeEngine.NodeResult dealRate = rows.get("revenue/aup/deal=deal/rate");
        Assertions.assertEquals(dealRate.localContribution() * scale, dealRate.contribution(), 1e-9);
        // nested account breakdown exists under the deal rate child and is scaled again
        final MetricTreeEngine.NodeResult nested = rows.get("revenue/aup/deal=deal/rate/account=biz/rate");
        Assertions.assertNotNull(nested);
        Assertions.assertEquals(3, nested.depth());
        Assertions.assertEquals(nested.localContribution() * dealRate.contribution() / dealRate.delta(),
                nested.contribution(), 1e-9);
        Assertions.assertEquals(Set.of("deal", "nodeal"),
                rows.values().stream().filter(r -> "deal".equals(r.dimension())).map(MetricTreeEngine.NodeResult::value)
                        .collect(java.util.stream.Collectors.toSet()));
        assertAdditive(result);
    }

    @Test
    public void testDegenerateParentGuard() {
        // a and b cancel: parent Δ = 0 -> children get contribution 0 and the parent is flagged,
        // but local contributions are preserved
        final LeafTable.Builder b = LeafTable.builder(List.of("_"), List.of("a", "b", "c"));
        b.addBaseline(ALL, new double[]{ 100, 100, 10 });
        b.addTarget(ALL, new double[]{ 130, 70, 20 });
        final MetricTreeSpec spec = spec(
                sum("y", "ab", "c"), sum("ab", "a", "b"),
                node("a", "a", null), node("b", "b", null), node("c", "c", null));
        final MetricTreeEngine.TreeResult result = new MetricTreeEngine(spec, b.build()).run("y");

        final Map<String, MetricTreeEngine.NodeResult> rows = byPath(result);
        Assertions.assertTrue(rows.get("y/ab").degenerate());
        Assertions.assertFalse(rows.get("y").degenerate());
        Assertions.assertEquals(0.0, rows.get("y/ab").contribution(), EPS);
        Assertions.assertEquals(0.0, rows.get("y/ab/a").contribution(), EPS);
        Assertions.assertEquals(30.0, rows.get("y/ab/a").localContribution(), EPS);
        Assertions.assertEquals(-30.0, rows.get("y/ab/b").localContribution(), EPS);
        Assertions.assertEquals(10.0, rows.get("y/c").contribution(), EPS);
        Assertions.assertEquals(1, rows.get("y/c").rank());
    }

    @Test
    public void testNearZeroParentIsDegenerateByRatio() {
        final LeafTable.Builder b = LeafTable.builder(List.of("_"), List.of("a", "b"));
        b.addBaseline(ALL, new double[]{ 1000, 1000 });
        b.addTarget(ALL, new double[]{ 1500, 501 });   // Δ = 1 vs Σ|contrib| = 999
        final MetricTreeSpec spec = spec(sum("y", "a", "b"), node("a", "a", null), node("b", "b", null));
        final MetricTreeEngine.TreeResult result = new MetricTreeEngine(spec, b.build()).run("y");
        Assertions.assertTrue(byPath(result).get("y").degenerate());
        Assertions.assertEquals(500.0, byPath(result).get("y/a").localContribution(), EPS);

        final MetricTreeSpec strict = new MetricTreeSpec(spec.nodes(), List.of(), null, 0.0);
        final MetricTreeEngine.TreeResult result2 = new MetricTreeEngine(strict, b.build()).run("y");
        Assertions.assertFalse(byPath(result2).get("y").degenerate());
        Assertions.assertEquals(500.0, byPath(result2).get("y/a").contribution(), EPS);
    }

    @Test
    public void testCausalEdgeRecoversRootCauseWhenOnlyRateChanged() {
        // Paper case 1a: AUP drives units (negative slope); only AUP shifts in the new period.
        // Plain MTCD blames units; the causal estimator gives units ≈ 0 and AUP ≈ Δrevenue.
        final Simulation sim = Simulation.linear(100, 1.0, 0.0, 7L);
        final MetricTreeEngine.TreeResult plain = run(sim, false, MetricTreeSpec.Estimator.auto);
        final MetricTreeEngine.TreeResult adjusted = run(sim, true, MetricTreeSpec.Estimator.auto);

        final double delta = adjusted.rootDelta();
        final MetricTreeEngine.NodeResult plainUnits = byPath(plain).get("revenue/units");
        final MetricTreeEngine.NodeResult adjUnits = byPath(adjusted).get("revenue/units");
        final MetricTreeEngine.NodeResult adjAup = byPath(adjusted).get("revenue/aup");
        Assertions.assertTrue(Math.abs(plainUnits.contribution()) > 0.3 * Math.abs(delta),
                "plain MTCD misattributes to units: " + plainUnits.contribution() + " of " + delta);
        Assertions.assertTrue(Math.abs(adjUnits.contribution()) < 0.1 * Math.abs(delta),
                "adjusted units contribution should vanish: " + adjUnits.contribution() + " of " + delta);
        Assertions.assertTrue(adjAup.contribution() > 0.9 * delta);
        Assertions.assertTrue(adjUnits.causalAdjusted());
        Assertions.assertEquals("simplified", adjUnits.estimator());
        Assertions.assertNotNull(adjUnits.diagnostics());
        Assertions.assertEquals(100, adjUnits.diagnostics().baselineGranules());
        Assertions.assertEquals(adjUnits.contribution() + adjAup.contribution(), delta, 1e-6);
    }

    @Test
    public void testCausalEdgeWhenBothChanged() {
        // Paper case 1b: AUP shifts and the units mechanism shifts by an intercept C.
        // True units contribution (total scale) = C · Σ_j AUP1_j.
        final Simulation sim = Simulation.linear(100, 1.0, 30.0, 11L);
        final MetricTreeEngine.TreeResult adjusted = run(sim, true, MetricTreeSpec.Estimator.auto);
        final MetricTreeEngine.TreeResult full = run(sim, true, MetricTreeSpec.Estimator.full);
        final double truth = sim.trueUnitsContribution();
        final double delta = adjusted.rootDelta();
        final double simplified = byPath(adjusted).get("revenue/units").contribution();
        final double fullValue = byPath(full).get("revenue/units").contribution();
        Assertions.assertEquals(truth, simplified, 0.1 * Math.abs(delta), "simplified estimator");
        Assertions.assertEquals(truth, fullValue, 0.1 * Math.abs(delta), "full estimator");
        Assertions.assertEquals("full", byPath(full).get("revenue/units").estimator());
    }

    @Test
    public void testCausalFallbackWithoutEnoughGranules() {
        final Simulation sim = Simulation.linear(5, 1.0, 0.0, 3L);
        final MetricTreeEngine.TreeResult adjusted = run(sim, true, MetricTreeSpec.Estimator.auto);
        final MetricTreeEngine.NodeResult units = byPath(adjusted).get("revenue/units");
        Assertions.assertEquals("fallback", units.estimator());
        Assertions.assertFalse(units.causalAdjusted());
        Assertions.assertNotNull(units.warning());
        // fallback = plain ordered allocation
        final MetricTreeEngine.TreeResult plain = run(sim, false, MetricTreeSpec.Estimator.auto);
        Assertions.assertEquals(byPath(plain).get("revenue/units").contribution(), units.contribution(), 1e-9);
    }

    @Test
    public void testElasticityEdgeWithoutGranularity() {
        final LeafTable.Builder b = LeafTable.builder(List.of("_"), List.of("units", "revenue"));
        b.addBaseline(ALL, new double[]{ 100, 1000 });   // aup0 = 10
        b.addTarget(ALL, new double[]{ 90, 1080 });      // aup1 = 12
        final MetricTreeSpec.Edge edge = new MetricTreeSpec.Edge("aup", "units", MetricTreeSpec.Model.linear, false,
                MetricTreeSpec.Estimator.auto, -5.0);
        final MetricTreeSpec spec = new MetricTreeSpec(List.of(
                product("revenue", "units", "aup"), node("units", "units", null), node("aup", null, "revenue / units")),
                List.of(edge), null, MetricTreeSpec.DEFAULT_MIN_PARENT_DELTA_RATIO);
        final MetricTreeEngine.TreeResult result = new MetricTreeEngine(spec, b.build()).run("revenue");
        // n0* = 100 + (-5)(12 - 10) = 90 -> units contribution (90 - 90) · 12 = 0, all to aup
        final MetricTreeEngine.NodeResult units = byPath(result).get("revenue/units");
        Assertions.assertEquals(0.0, units.contribution(), EPS);
        Assertions.assertEquals("elasticity", units.estimator());
        Assertions.assertEquals(80.0, byPath(result).get("revenue/aup").contribution(), EPS);
    }

    private static MetricTreeEngine.TreeResult run(
            final Simulation sim, final boolean causal, final MetricTreeSpec.Estimator estimator) {
        final List<MetricTreeSpec.Edge> edges = causal
                ? List.of(new MetricTreeSpec.Edge("aup", "units", MetricTreeSpec.Model.linear, false, estimator, null))
                : List.of();
        final MetricTreeSpec spec = new MetricTreeSpec(List.of(
                product("revenue", "units", "aup"), node("units", "units", null), node("aup", null, "revenue / units")),
                edges, new MetricTreeSpec.Causal("day", 0.05, 14), MetricTreeSpec.DEFAULT_MIN_PARENT_DELTA_RATIO);
        return new MetricTreeEngine(spec, sim.table()).run("revenue");
    }

    /** Paper Sec. 7 style simulation: AUP ~ N, units = a + b·AUP + ε, revenue = units · AUP. */
    static final class Simulation {
        final double[] aup0;
        final double[] units0;
        final double[] aup1;
        final double[] units1;
        final double interceptShift;

        private Simulation(final double[] aup0, final double[] units0, final double[] aup1, final double[] units1, final double interceptShift) {
            this.aup0 = aup0;
            this.units0 = units0;
            this.aup1 = aup1;
            this.units1 = units1;
            this.interceptShift = interceptShift;
        }

        static Simulation linear(final int days, final double aupShift, final double interceptShift, final long seed) {
            final java.util.Random random = new java.util.Random(seed);
            final Function<Double, Double> f0 = aup -> 500 - 20 * aup;
            final double[] aup0 = new double[days];
            final double[] units0 = new double[days];
            final double[] aup1 = new double[days];
            final double[] units1 = new double[days];
            for(int j = 0; j < days; j++) {
                aup0[j] = 10 + random.nextGaussian();
                units0[j] = f0.apply(aup0[j]) + 3 * random.nextGaussian();
                aup1[j] = 10 + aupShift + random.nextGaussian();
                units1[j] = f0.apply(aup1[j]) + interceptShift + 3 * random.nextGaussian();
            }
            return new Simulation(aup0, units0, aup1, units1, interceptShift);
        }

        double trueUnitsContribution() {
            double sum = 0;
            for(final double a : aup1) {
                sum += interceptShift * a;
            }
            return sum;
        }

        LeafTable table() {
            final LeafTable.Builder b = LeafTable.builder(List.of("day"), List.of("units", "revenue"));
            for(int j = 0; j < aup0.length; j++) {
                final String[] d0 = { "b" + j };
                b.addBaseline(d0, new double[]{ units0[j], units0[j] * aup0[j] });
                b.addBaselineRows(d0, 1);
                final String[] d1 = { "t" + j };
                b.addTarget(d1, new double[]{ units1[j], units1[j] * aup1[j] });
                b.addTargetRows(d1, 1);
            }
            return b.build();
        }
    }

    @Test
    public void testSpecValidation() {
        final MetricTreeSpec ok = spec(product("revenue", "units", "aup"), node("units", "units", null), node("aup", null, "revenue / units"));
        Assertions.assertTrue(ok.validate(Set.of("revenue")).isEmpty(), ok.validate(Set.of("revenue")).toString());

        final List<String> unknownRoot = ok.validate(Set.of("gmv"));
        Assertions.assertEquals(1, unknownRoot.size());

        final MetricTreeSpec cycle = spec(sum("a", "b"), sum("b", "a"));
        Assertions.assertTrue(cycle.validate(Set.of("a")).stream().anyMatch(m -> m.contains("cycle")));

        final MetricTreeSpec badEdge = new MetricTreeSpec(ok.nodes(),
                List.of(new MetricTreeSpec.Edge("units", "aup", MetricTreeSpec.Model.linear, false, MetricTreeSpec.Estimator.auto, null)),
                new MetricTreeSpec.Causal("day", 0.05, 14), 0.01);
        Assertions.assertTrue(badEdge.validate(Set.of("revenue")).stream().anyMatch(m -> m.contains("rate (from) and volume (to)")));

        final MetricTreeSpec noGranularity = new MetricTreeSpec(ok.nodes(),
                List.of(new MetricTreeSpec.Edge("aup", "units", MetricTreeSpec.Model.linear, false, MetricTreeSpec.Estimator.auto, null)),
                null, 0.01);
        Assertions.assertTrue(noGranularity.validate(Set.of("revenue")).stream().anyMatch(m -> m.contains("granularity")));

        final MetricTreeSpec badBreakdown = spec(new MetricTreeSpec.Node("orders", "orders", null, null, null, null, null,
                List.of(new MetricTreeSpec.Breakdown("channel", MetricTreeSpec.Decomposition.weightedAverage, null, null, null))));
        final List<String> errors = badBreakdown.validate(Set.of("orders"));
        Assertions.assertTrue(errors.stream().anyMatch(m -> m.contains("requires a rate node")), errors.toString());
        Assertions.assertTrue(errors.stream().anyMatch(m -> m.contains(".weight")), errors.toString());

        Assertions.assertEquals(Set.of("units", "revenue"), new java.util.HashSet<>(ok.referencedFields()));
        final List<String> reachable = new ArrayList<>(ok.reachable("revenue"));
        Assertions.assertEquals(List.of("revenue", "units", "aup"), reachable);
    }
}
