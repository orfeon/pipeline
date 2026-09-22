package com.mercari.solution.util.pipeline.feature;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FeaturePlanCompilerTest {

    private static final String SOURCES = """
            version: 1
            sources:
              - name: listings
                eventTime: session_time
                availability: atEventTime
                mutability: appendOnly
                keys: [session_id, seller_id]
                fields:
                  - {name: session_id, type: string}
                  - {name: seller_id, type: string}
                  - {name: category, type: string}
                  - {name: quantity, type: int}
                  - {name: start_price, type: double, kind: attribute}
                  - {name: condition_grade, type: string}
              - name: price_snapshots
                eventTime: session_time
                ingestionLag: PT1M
                keys: [session_id, seller_id]
                fields:
                  - {name: current_bid_t10, type: double, availableAt: "event_time - PT10M", observedAtField: snapshot_time, kind: market, validFor: PT15M}
                  - {name: snapshot_time, type: timestamp, availableAt: "event_time - PT10M", observedAtField: snapshot_time}
              - name: auction_results
                eventTime: session_time
                availability: atEventTime
                settlementLag: PT30M
                ingestionLag: P6D
                mutability: corrections
                keys: [session_id, seller_id]
                fields:
                  - {name: sold, type: int, availableAt: after(event), kind: outcome}
                  - {name: final_price, type: double, availableAt: after(event), kind: outcome}
            """;

    private static final String SPEC = """
            lineage:
              - {fields: [session_id, seller_id, category, quantity, start_price, condition_grade], from: listings}
              - {fields: [current_bid_t10], from: price_snapshots}
              - {fields: [sold, final_price], from: auction_results}
            time: {field: session_time, orderTieBreak: [session_id]}
            predictAt: "event_time - PT8M"
            entities:
              - {name: seller, keys: [seller_id]}
              - {name: cat, keys: [category]}
            contexts:
              - {name: session, keys: [session_id]}
            baselines:
              - {name: market, context: session, expr: "share(1 / current_bid_t10)"}
            features:
              - name: price_per_unit
                scope: row
                expr: "start_price / quantity"
              - name: time_parts
                scope: row
                type: datetime
                input: session_time
                derive: [month, dayOfWeek]
                cyclical: true
              - name: relative
                scope: context
                context: session
                inputs: [start_price, current_bid_t10]
                ops: [rank, zscore]
              - name: composition
                scope: context
                context: session
                ops:
                  - {type: countByValue, fields: [condition_grade]}
                  - {type: entropy, fields: [condition_grade]}
              - name: recent
                scope: sequence
                entity: seller
                windows:
                  - {maxEvents: 5}
                  - {maxAge: P365D}
                ops:
                  - {type: lag, fields: [sold, start_price], k: 2}
                  - {type: ewma, expr: "sold >= 1", halflife: [5]}
                  - {type: aggregate, field: sold, funcs: [count, mean]}
              - name: vs_market
                scope: row
                type: residual
                input: price_per_unit
                baseline: market
                on: identity
              - name: enc
                scope: population
                type: encoding
                keySets:
                  - keys: [seller_id]
                  - keys: [category]
                    windows: [{maxAge: P365D}]
                targets:
                  - {stats: [count, share]}
                  - {expr: "sold >= 1", stats: [mean]}
                maxFeatures: 50
            output:
              prefix: f_
            """;

    private static FeaturePlan compile(final String sources, final String spec) {
        final JsonObject sourcesJson = Config.convertConfigJson(sources, Config.Format.yaml);
        final JsonObject specJson = Config.convertConfigJson(spec, Config.Format.yaml);
        return FeaturePlanCompiler.compile(sourcesJson, specJson, null);
    }

    private static OutputColumn column(final FeaturePlan plan, final String canonical) {
        final OutputColumn c = plan.getColumn(canonical);
        Assertions.assertNotNull(c, () -> "missing column " + canonical + "\n" + plan.describe());
        return c;
    }

    private static boolean hasCode(final FeaturePlan plan, final String code) {
        return plan.getDiagnostics().getMessages().stream().anyMatch(m -> m.code().equals(code));
    }

    @Test
    public void testCompileHappyPath() {
        final FeaturePlan plan = compile(SOURCES, SPEC);
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);

        // row: expression over pre-event attributes is statically safe
        final OutputColumn ratio = column(plan, "price_per_unit");
        Assertions.assertEquals(OutputColumn.Status.staticSafe, ratio.getStatus());
        Assertions.assertEquals("f_price_per_unit", ratio.getOutputName());
        Assertions.assertEquals(List.of("start_price", "quantity"), List.copyOf(ratio.getInputs()));

        // row datetime cyclical → sin/cos
        column(plan, "time_parts_month_sin");
        column(plan, "time_parts_dayOfWeek_cos");

        // context: inputs × ops sugar, market lineage propagates
        final OutputColumn bidRank = column(plan, "relative_current_bid_t10_rank");
        Assertions.assertEquals(Schema.Type.int64, bidRank.getFieldType().getType());
        Assertions.assertTrue(bidRank.getDerivedFrom().contains("market"));
        Assertions.assertEquals(OutputColumn.Status.staticSafe, bidRank.getStatus());
        Assertions.assertEquals(OutputColumn.Placement.child, bidRank.getPlacement());
        final OutputColumn entropy = column(plan, "composition_condition_grade_entropy");
        Assertions.assertEquals(OutputColumn.Placement.child, entropy.getPlacement()); // no output.groupBy

        // sequence: lag of an outcome field needs the near edge shifted by settlementLag + ingestionLag + predict offset
        final OutputColumn lagRank = column(plan, "recent_n5_sold_lag1");
        Assertions.assertEquals(OutputColumn.Status.windowShift, lagRank.getStatus());
        Assertions.assertEquals(Duration.ofDays(6).plusMinutes(38), lagRank.getWindowShift());
        Assertions.assertTrue(lagRank.getDerivedFrom().contains("outcome"));
        Assertions.assertEquals(Schema.Type.int32, lagRank.getFieldType().getType());
        column(plan, "recent_365d_sold_lag2");

        // sequence: lag of a pre-event attribute is safe without a shift
        final OutputColumn lagPrice = column(plan, "recent_n5_start_price_lag1");
        Assertions.assertEquals(OutputColumn.Status.staticSafe, lagPrice.getStatus());

        // desugared expression → anonymous intermediate row column consumed by ewma
        final OutputColumn ewma = column(plan, "recent_n5_recent__e1_ewma5");
        final OutputColumn anonymous = column(plan, "recent__e1");
        Assertions.assertTrue(anonymous.isIntermediate());
        Assertions.assertTrue(anonymous.isAnonymous());
        Assertions.assertTrue(ewma.getInputs().contains("recent__e1"));
        // an unnamed op expr is hinted once (not per window); as: names its columns and silences it
        Assertions.assertEquals(1, plan.getDiagnostics().getMessages().stream().filter(m -> "sequence.expr.anonymous".equals(m.code())).count(), plan::describe);
        final FeaturePlan named = compile(SOURCES, SPEC.replace("expr: \"sold >= 1\", halflife: [5]", "expr: \"sold >= 1\", halflife: [5], as: won"));
        Assertions.assertNotNull(named.getColumn("recent_n5_won_ewma5"), named::describe);
        Assertions.assertFalse(hasCode(named, "sequence.expr.anonymous"), named::describe);

        // mean over an outcome field → hint to use encoding
        Assertions.assertTrue(hasCode(plan, "sequence.aggregate.encoding"), plan::describe);

        // baseline is intermediate and the residual consumes it
        final OutputColumn baseline = column(plan, "__baseline_market");
        Assertions.assertTrue(baseline.isIntermediate());
        final OutputColumn residual = column(plan, "vs_market");
        Assertions.assertTrue(residual.getInputs().contains("__baseline_market"));
        Assertions.assertTrue(residual.getDerivedFrom().contains("market"));

        // encoding: keySet × window × target × stat
        column(plan, "enc__seller_id__count");
        column(plan, "enc__seller_id__share");
        final OutputColumn encMean = column(plan, "enc__category__365d__e2__mean");
        Assertions.assertTrue(encMean.isFitted());
        Assertions.assertEquals(OutputColumn.Status.windowShift, encMean.getStatus());
        Assertions.assertEquals("expanding", encMean.getCoordinates().get("fit"));

        // schema carries lineage options
        final Schema.Field field = plan.getOutputSchema().getFields().stream()
                .filter(f -> f.getName().equals("f_recent_n5_sold_lag1")).findFirst().orElseThrow();
        Assertions.assertEquals("windowShift", field.getOptions().get("feature.status"));
        Assertions.assertEquals("outcome", field.getOptions().get("feature.derivedFrom"));

        // share = n_key / n_global: the global level is a hidden population stage shared by the block
        final OutputColumn share = column(plan, "enc__seller_id__share");
        Assertions.assertEquals("share", share.getOperator());
        Assertions.assertEquals(FeatureSpec.Scope.row, share.getScope());
        Assertions.assertTrue(column(plan, "enc__global__n").isIntermediate());
        Assertions.assertTrue(share.getInputs().contains("enc__global__n"));

        // stages: context(session) → keyed(seller_id: sequence + encoding, one replay) → global level → encoding(category)
        Assertions.assertEquals(4, plan.getStages().size(), plan::describe);
        Assertions.assertEquals(4, plan.getShuffleCount(), plan::describe);
        Assertions.assertEquals(FeaturePlan.StageKind.context, plan.getStages().get(0).kind());
        Assertions.assertEquals(List.of("seller_id"), plan.getStages().get(1).keys());
        Assertions.assertEquals(List.of("recent", "enc"), plan.getStages().get(1).blocks());
        Assertions.assertTrue(plan.getStages().get(1).columnNames().contains("recent_n5_sold_lag1"), plan::describe);
        Assertions.assertTrue(plan.getStages().get(1).columnNames().contains("enc__seller_id__count"), plan::describe);
        Assertions.assertEquals(List.of(), plan.getStages().get(2).keys());
        Assertions.assertEquals(16, plan.getHash().length());
    }

    @Test
    public void testOutcomeInFinalOutputIsViolation() {
        final String spec = SPEC.replace("expr: \"start_price / quantity\"", "expr: \"final_price / start_price\"");
        final FeaturePlan plan = compile(SOURCES, spec);
        Assertions.assertTrue(hasCode(plan, "availability.violation"), plan::describe);
    }

    @Test
    public void testConsumedOutcomeBecomesIntermediate() {
        final String spec = SPEC
                .replace("expr: \"start_price / quantity\"", "expr: \"final_price / start_price\"")
                .replace("fields: [sold, start_price], k: 2", "fields: [sold, start_price, price_per_unit], k: 2")
                .replace("input: price_per_unit", "input: start_price");
        final FeaturePlan plan = compile(SOURCES, spec);
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        final OutputColumn ratio = column(plan, "price_per_unit");
        Assertions.assertTrue(ratio.isIntermediate());
        Assertions.assertEquals("_f_price_per_unit", ratio.getOutputName());
        final OutputColumn lag = column(plan, "recent_n5_price_per_unit_lag1");
        Assertions.assertEquals(OutputColumn.Status.windowShift, lag.getStatus());
        Assertions.assertFalse(lag.isIntermediate());
    }

    @Test
    public void testDeclaredMarketIsErrorUnlessJustified() {
        final String declared = SOURCES.replace(
                "observedAtField: snapshot_time, kind: market, validFor: PT15M",
                "evidence: declared, kind: market");
        final FeaturePlan plan = compile(declared, SPEC);
        Assertions.assertTrue(hasCode(plan, "sources.fields.declaredMarket"), plan::describe);
        Assertions.assertTrue(plan.getDiagnostics().hasErrors());

        final String allowedNoJustification = SOURCES.replace(
                "observedAtField: snapshot_time, kind: market, validFor: PT15M",
                "evidence: declared, kind: market, allowDeclared: true");
        Assertions.assertTrue(hasCode(compile(allowedNoJustification, SPEC), "sources.fields.allowDeclared"));

        final String allowed = SOURCES.replace(
                "observedAtField: snapshot_time, kind: market, validFor: PT15M",
                "evidence: declared, kind: market, allowDeclared: true, justification: \"feed spec §3\"");
        final FeaturePlan ok = compile(allowed, SPEC);
        Assertions.assertFalse(ok.getDiagnostics().hasErrors(), ok::describe);
        Assertions.assertTrue(hasCode(ok, "evidence.declared"));
        Assertions.assertTrue(column(ok, "relative_current_bid_t10_rank").isDeclaredEvidence());
    }

    @Test
    public void testPreEventClaimRequiresObservedAtField() {
        final String sources = SOURCES.replace(
                "availableAt: \"event_time - PT10M\", observedAtField: snapshot_time, kind: market, validFor: PT15M",
                "availableAt: \"event_time - PT10M\", kind: market");
        Assertions.assertTrue(hasCode(compile(sources, SPEC), "sources.fields.observedAtField"));
    }

    /**
     * The two-series {@code regression} op (one column per func, both series projected into the history, the lagged
     * pairing marked for the scan path) and {@code fracdiff} (d and k in the coordinates, a bounded tail).
     */
    @Test
    public void testRegressionAndFracdiffExpansion() {
        final String plain = "- {type: aggregate, field: sold, funcs: [count, mean]}";
        Assertions.assertTrue(SPEC.contains(plain));
        final String spec = SPEC.replace(plain, plain
                + "\n      - {type: regression, field: final_price, against: start_price, funcs: [beta, corr, r2]}"
                + "\n      - {type: regression, field: final_price, against: start_price, lag: 1}"
                + "\n      - {type: fracdiff, field: start_price, d: 0.4, k: 10}");
        final FeaturePlan plan = compile(SOURCES, spec);
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        final OutputColumn beta = column(plan, "recent_365d_final_price_vs_start_price_beta");
        Assertions.assertEquals("regression", beta.getOperator());
        Assertions.assertEquals(Schema.Type.float64, beta.getFieldType().getType());
        Assertions.assertEquals("beta", beta.getCoordinates().get("func"));
        Assertions.assertEquals("final_price", beta.getCoordinates().get("field"));
        Assertions.assertEquals("start_price", beta.getCoordinates().get("against"));
        Assertions.assertNull(beta.getCoordinates().get("lag"));
        Assertions.assertEquals(Set.of("final_price", "start_price"), beta.getPastInputs());
        // final_price is an outcome: the pair's window is shifted like an aggregate of it
        Assertions.assertEquals(OutputColumn.Status.windowShift, beta.getStatus());
        Assertions.assertNotNull(column(plan, "recent_n5_final_price_vs_start_price_r2"));
        // the lagged pairing: default funcs, its own names, the lag in the coordinates (what sends it to the scan path)
        final OutputColumn lagged = column(plan, "recent_365d_final_price_vs_start_price_lag1_corr");
        Assertions.assertEquals("1", lagged.getCoordinates().get("lag"));
        Assertions.assertNotNull(column(plan, "recent_365d_final_price_vs_start_price_lag1_beta"));
        Assertions.assertNull(SequenceEvaluator.unboundedReason(beta));
        Assertions.assertNull(SequenceEvaluator.unboundedReason(lagged), "bounded by maxAge");

        final OutputColumn fracdiff = column(plan, "recent_n5_start_price_fracdiff0p4");
        Assertions.assertEquals("0.4", fracdiff.getCoordinates().get("d"));
        Assertions.assertEquals("10", fracdiff.getCoordinates().get("k"));
        Assertions.assertEquals(OutputColumn.Status.staticSafe, fracdiff.getStatus());

        final String reg = "- {type: regression, field: final_price, against: start_price, funcs: [beta, corr, r2]}";
        Assertions.assertTrue(hasCode(compile(SOURCES, spec.replace(reg, "- {type: regression, field: final_price, funcs: [beta]}")), "sequence.regression.against"));
        Assertions.assertTrue(hasCode(compile(SOURCES, spec.replace(reg, "- {type: regression, field: final_price, against: condition_grade}")), "sequence.regression.against"));
        Assertions.assertTrue(hasCode(compile(SOURCES, spec.replace(reg, "- {type: regression, field: condition_grade, against: start_price}")), "sequence.op.type"));
        final FeaturePlan func = compile(SOURCES, spec.replace("funcs: [beta, corr, r2]", "funcs: [beta, mean]"));
        Assertions.assertTrue(hasCode(func, "sequence.regression.func"));
        Assertions.assertTrue(func.getDiagnostics().getErrorMessages().stream().anyMatch(m -> m.contains("cov | corr | beta")));
        Assertions.assertTrue(hasCode(compile(SOURCES, spec.replace("lag: 1}", "lag: -1}")), "sequence.regression.lag"));
        Assertions.assertTrue(hasCode(compile(SOURCES, spec.replace(reg, "- {type: regression, field: final_price, against: nosuchfield}")), "reference.unresolved"));
        Assertions.assertTrue(hasCode(compile(SOURCES, spec.replace("d: 0.4, k: 10", "k: 10")), "sequence.fracdiff.d"));
        Assertions.assertTrue(hasCode(compile(SOURCES, spec.replace("d: 0.4", "d: 2.5")), "sequence.fracdiff.d"));
        Assertions.assertTrue(hasCode(compile(SOURCES, spec.replace("k: 10}", "k: 1}")), "sequence.fracdiff.k"));
    }

    /**
     * The general form (§4.3): lift channels × halflifes × components, one FLOAT64 column per component sharing one
     * running state per channel; ewma is the order-0 exponential measure over the same coordinates; the validation of
     * the dynamics parameters and the size bound.
     */
    @Test
    public void testDynamicsGeneralForm() {
        final String block = """
                  - name: hist
                    scope: sequence
                    entity: seller
                    windows: [{maxAge: P365D}]
                    lift: {fields: [start_price, final_price], exprs: ["quantity * 2"], timeAugment: true}
                    summarize:
                      dynamics: {family: lti, measure: exponential, order: 2, halflife: [7, 30.5], decayBy: time}
                  - name: shape
                    scope: sequence
                    entity: seller
                    lift: {fields: [start_price]}
                    summarize:
                      dynamics: {family: lti, measure: legendre, order: 3}
                  - name: season
                    scope: sequence
                    entity: seller
                    window: {maxEvents: 20}
                    lift: {fields: [start_price]}
                    summarize:
                      dynamics: {family: lti, measure: fourier, order: 2, period: 7, decayBy: time}
                """;
        final String anchor = "  - name: vs_market\n";
        Assertions.assertTrue(SPEC.contains(anchor));
        final String spec = SPEC.replace(anchor, block + anchor);
        final FeaturePlan plan = compile(SOURCES, spec);
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);

        // exponential: 3 components per channel and halflife; the fractional halflife names as 30p5 and parses as 30.5
        final OutputColumn laguerre = column(plan, "hist_365d_start_price_exp7_1");
        Assertions.assertEquals("dynamics", laguerre.getOperator());
        Assertions.assertEquals(Schema.Type.float64, laguerre.getFieldType().getType());
        Assertions.assertEquals(Map.of("family", "lti", "measure", "exponential", "order", "2", "component", "1", "halflife", "7",
                "decayBy", "time", "stateKey", "hist_365d_start_price_exp7", "field", "start_price"),
                Map.of("family", laguerre.getCoordinates().get("family"), "measure", laguerre.getCoordinates().get("measure"),
                        "order", laguerre.getCoordinates().get("order"), "component", laguerre.getCoordinates().get("component"),
                        "halflife", laguerre.getCoordinates().get("halflife"), "decayBy", laguerre.getCoordinates().get("decayBy"),
                        "stateKey", laguerre.getCoordinates().get("stateKey"), "field", laguerre.getCoordinates().get("field")));
        Assertions.assertEquals("30.5", column(plan, "hist_365d_start_price_exp30p5_2").getCoordinates().get("halflife"));
        Assertions.assertEquals(column(plan, "hist_365d_start_price_exp7_0").getCoordinates().get("stateKey"),
                column(plan, "hist_365d_start_price_exp7_2").getCoordinates().get("stateKey"), "the components of a channel share one state");
        // an outcome channel shifts its window like an aggregate of it; the expression channel is desugared once
        Assertions.assertEquals(OutputColumn.Status.windowShift, column(plan, "hist_365d_final_price_exp7_0").getStatus());
        Assertions.assertEquals(OutputColumn.Status.staticSafe, laguerre.getStatus());
        Assertions.assertEquals(1, plan.getColumns().stream().filter(c -> c.getCanonicalName().startsWith("hist__e")).count(), plan::describe);
        // the constant time channel skips its component 0 (always 1)
        Assertions.assertNull(plan.getColumn("hist_365d_time_exp7_0"));
        Assertions.assertNull(column(plan, "hist_365d_time_exp7_1").getCoordinates().get("field"));
        // the time channel reads no field but describes the events the value channels see: the outcome channel's shift
        final OutputColumn time = column(plan, "hist_365d_time_exp7_1");
        Assertions.assertEquals(OutputColumn.Status.windowShift, time.getStatus());
        Assertions.assertEquals(column(plan, "hist_365d_final_price_exp7_0").getWindowShift(), time.getWindowShift());
        Assertions.assertTrue(time.getPastInputs().isEmpty(), "aligned, not an input");
        Assertions.assertTrue(hasCode(plan, "sequence.lift.align"), plan::describe);
        Assertions.assertTrue(hasCode(plan, "sequence.lift.anonymous"), plan::describe);
        final FeaturePlan preEvent = compile(SOURCES, spec.replace("fields: [start_price, final_price]", "fields: [start_price]")
                .replace("exprs: [\"quantity * 2\"], ", ""));
        Assertions.assertFalse(preEvent.getDiagnostics().hasErrors(), preEvent::describe);
        Assertions.assertEquals(OutputColumn.Status.staticSafe, column(preEvent, "hist_365d_time_exp7_1").getStatus());
        Assertions.assertFalse(hasCode(preEvent, "sequence.lift.align"), preEvent::describe);
        // `as` names an expression channel (stable, unlike the spec-wide __e{n})
        final FeaturePlan named = compile(SOURCES, spec.replace("exprs: [\"quantity * 2\"]", "exprs: [{expr: \"quantity * 2\", as: qty2}]"));
        Assertions.assertFalse(named.getDiagnostics().hasErrors(), named::describe);
        Assertions.assertEquals("hist_365d_qty2_exp7", column(named, "hist_365d_qty2_exp7_1").getCoordinates().get("stateKey"));
        Assertions.assertFalse(hasCode(named, "sequence.lift.anonymous"), named::describe);
        Assertions.assertEquals(2 * (3 * 4 - 1), plan.getColumns().stream().filter(c -> "hist".equals(c.getBlock()) && "dynamics".equals(c.getOperator())).count());

        // legendre over an unbounded window runs on a running state (no unbounded hint); fourier names c0 / c<k> / s<k>
        final OutputColumn legendre = column(plan, "shape_all_start_price_leg_3");
        Assertions.assertNull(SequenceEvaluator.unboundedReason(legendre));
        Assertions.assertEquals("7", column(plan, "season_n20_start_price_fourier7_s2").getCoordinates().get("period"));
        Assertions.assertNotNull(column(plan, "season_n20_start_price_fourier7_c0"));

        // ewma is the order-0 exponential measure: running state, no unbounded hint, the plain halflife coordinate
        final OutputColumn ewma = plan.getColumns().stream().filter(c -> c.getCanonicalName().startsWith("recent_365d_recent__e")
                && c.getCanonicalName().endsWith("_ewma5")).findFirst().orElseThrow();
        Assertions.assertEquals("exponential", ewma.getCoordinates().get("measure"));
        Assertions.assertEquals("0", ewma.getCoordinates().get("order"));
        Assertions.assertTrue(plan.getDiagnostics().getMessages().stream()
                .noneMatch(m -> "sequence.window.unbounded".equals(m.code()) && m.message().contains("ewma")), plan::describe);
        final FeaturePlan fractional = compile(SOURCES, SPEC.replace("halflife: [5]", "halflife: [1.5]"));
        Assertions.assertTrue(fractional.getColumns().stream().filter(c -> c.getCanonicalName().endsWith("_ewma1p5"))
                .allMatch(c -> "1.5".equals(c.getCoordinates().get("halflife"))), fractional::describe);
        Assertions.assertTrue(hasCode(compile(SOURCES, SPEC.replace("halflife: [5]", "halflife: [0]")), "sequence.ewma.halflife"));

        // validation
        final String exp = "{family: lti, measure: exponential, order: 2, halflife: [7, 30.5], decayBy: time}";
        Assertions.assertTrue(spec.contains(exp));
        final Map<String, String> rejected = new java.util.LinkedHashMap<>();
        rejected.put("{family: probabilistic}", "sequence.dynamics.family");
        rejected.put("{measure: exponential, halflife: [7]}", "sequence.dynamics.family");
        rejected.put("{family: lti, measure: laplace, halflife: [7]}", "sequence.dynamics.measure");
        rejected.put("{family: lti, measure: exponential, order: 2}", "sequence.dynamics.halflife");
        rejected.put("{family: lti, measure: exponential, halflife: [-1]}", "sequence.dynamics.halflife");
        rejected.put("{family: lti, measure: legendre, halflife: [7]}", "sequence.dynamics.halflife");
        rejected.put("{family: lti, measure: exponential, order: 17, halflife: [7]}", "sequence.dynamics.order");
        rejected.put("{family: lti, measure: legendre, order: 9}", "sequence.dynamics.order");
        rejected.put("{family: lti, measure: fourier, order: 2}", "sequence.dynamics.period");
        rejected.put("{family: lti, measure: fourier, order: 0, period: 7}", "sequence.dynamics.order");
        rejected.put("{family: lti, measure: exponential, halflife: [7], period: 7}", "sequence.dynamics.period");
        rejected.put("{family: lti, measure: exponential, halflife: [7], decayBy: trading}", "clock.unknown");
        rejected.put("{family: lti, measure: exponential, halflife: [7], depth: 2}", "sequence.dynamics.parameter");
        rejected.put("{family: lti, measure: exponential, order: 8, halflife: [7, 30.5]}", "sequence.dynamics.size");
        for (final Map.Entry<String, String> e : rejected.entrySet()) {
            final FeaturePlan bad = compile(SOURCES, spec.replace(exp, e.getKey()));
            Assertions.assertTrue(hasCode(bad, e.getValue()), () -> e + "\n" + bad.describe());
        }
        final String lift = "lift: {fields: [start_price, final_price], exprs: [\"quantity * 2\"], timeAugment: true}";
        Assertions.assertTrue(hasCode(compile(SOURCES, spec.replace(lift, "lift: {fields: [condition_grade]}")), "sequence.lift.type"));
        Assertions.assertTrue(hasCode(compile(SOURCES, spec.replace(lift, "lift: {fields: []}")), "sequence.lift"));
        Assertions.assertTrue(hasCode(compile(SOURCES, spec.replace(lift, "lift: {fields: [start_price], exprs: [\"$self.quantity\"]}")), "sequence.self"));
        // compress is expanded, not rejected (the generated svd block is covered by testLogSignatureAndCompress):
        // an info naming the block, and an error only on a malformed one
        final FeaturePlan compressed = compile(SOURCES, spec.replace(lift, lift + "\n    compress: {svd: {rank: 2}}"));
        Assertions.assertFalse(compressed.getDiagnostics().hasErrors(), compressed::describe);
        Assertions.assertTrue(hasCode(compressed, "sequence.compress"));
        final FeaturePlan misspelled = compile(SOURCES, spec.replace(lift, lift + "\n    compress: {svd: {rnak: 2}}"));
        Assertions.assertTrue(misspelled.getDiagnostics().hasErrors(), misspelled::describe);
        Assertions.assertTrue(hasCode(misspelled, "sequence.compress"));
        Assertions.assertTrue(hasCode(compile(SOURCES, spec.replace(lift, lift + "\n    ops: [{type: lag, fields: [sold]}]")), "sequence.form"));
        Assertions.assertTrue(hasCode(compile(SOURCES, spec.replace("    summarize:\n      dynamics: " + exp + "\n", "")), "sequence.summarize"));
        // lift channels are block references: a typo is reported (not a silently empty block), a later block's column is waited for
        Assertions.assertTrue(hasCode(compile(SOURCES, spec.replace(lift, "lift: {fields: [start_prize]}")), "reference.unresolved"));
        Assertions.assertTrue(hasCode(compile(SOURCES, spec.replace(lift, "lift: {fields: [start_price], exprs: [\"start_prize * 2\"]}")), "reference.unresolved"));
        Assertions.assertTrue(hasCode(compile(SOURCES, spec.replace(lift, "lift: {fields: [start_price], timeaugment: true}")), "sequence.lift"));
        Assertions.assertTrue(hasCode(compile(SOURCES, spec.replace(lift, "lift: {fields: [start_price], exprs: [{expr: \"quantity * 2\", as: start_price}]}")), "sequence.lift.name"));
        Assertions.assertTrue(hasCode(compile(SOURCES, spec.replace(lift, "lift: {fields: [start_price], exprs: [{as: qty2}]}")), "sequence.lift"));
        Assertions.assertTrue(hasCode(compile(SOURCES, spec.replace(lift, "lift: {fields: [start_price], exprs: [{expr: \"quantity * 2\", name: qty2}]}")), "sequence.lift"));
        final String shapeLift = "lift: {fields: [start_price]}\n    summarize:\n      dynamics: {family: lti, measure: legendre, order: 3}";
        Assertions.assertTrue(spec.contains(shapeLift));
        // the general form reads the past window only
        Assertions.assertTrue(hasCode(compile(SOURCES, spec.replace(shapeLift, "direction: future\n    " + shapeLift)), "sequence.direction.op"));
        Assertions.assertTrue(hasCode(compile(SOURCES, spec.replace(shapeLift, "direction: sideways\n    " + shapeLift)), "sequence.direction"));
        final FeaturePlan forward = compile(SOURCES, spec.replace(shapeLift, shapeLift.replace("[start_price]", "[start_price, vs_market]")));
        Assertions.assertFalse(forward.getDiagnostics().hasErrors(), forward::describe);
        Assertions.assertNotNull(forward.getColumn("shape_all_vs_market_leg_3"), forward::describe);
        // dynamics has no op sugar (ewma is the order-0 exponential one)
        Assertions.assertTrue(hasCode(compile(SOURCES, SPEC.replace("- {type: aggregate, field: sold, funcs: [count, mean]}",
                "- {type: dynamics, field: sold}")), "sequence.op"));
        final FeaturePlan timeOnly = compile(SOURCES, spec.replace(exp, "{family: lti, measure: exponential, halflife: [7]}"));
        Assertions.assertTrue(hasCode(timeOnly, "sequence.lift.timeAugment"), timeOnly::describe);
        Assertions.assertFalse(timeOnly.getDiagnostics().hasErrors(), timeOnly::describe);
    }

    @Test
    public void testUnresolvedReferenceAndCycle() {
        final String spec = SPEC.replace("expr: \"start_price / quantity\"", "expr: \"start_price / nosuchfield\"");
        Assertions.assertTrue(hasCode(compile(SOURCES, spec), "reference.unresolved"));

        final String cyclic = SPEC.replace("expr: \"start_price / quantity\"", "expr: \"start_price / vs_market\"");
        final FeaturePlan plan = compile(SOURCES, cyclic);
        Assertions.assertTrue(hasCode(plan, "reference.cycle"), plan::describe);
        Assertions.assertTrue(plan.getDiagnostics().hasErrors());
    }

    /**
     * The scalar summaries of the aggregate op: the shape of the distribution (skew / kurt, a summary family — they
     * fold incrementally) and the order-dependent series readouts (zeroCross / peaks / acf / pacf / ar — scan only).
     */
    @Test
    public void testAggregateShapeAndSeriesFuncs() {
        final String plain = "- {type: aggregate, field: sold, funcs: [count, mean]}";
        Assertions.assertTrue(SPEC.contains(plain));
        final String spec = SPEC.replace(plain, "- {type: aggregate, field: start_price, funcs: [skew, kurt, zeroCross, peaks, acf1, pacf2, ar2_1]}");
        final FeaturePlan plan = compile(SOURCES, spec);
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        for (final String func : List.of("skew", "kurt", "acf1", "pacf2", "ar2_1")) {
            final OutputColumn c = column(plan, "recent_365d_start_price_" + func);
            Assertions.assertEquals("aggregate", c.getOperator());
            Assertions.assertEquals(func, c.getCoordinates().get("func"));
            Assertions.assertEquals(Schema.Type.float64, c.getFieldType().getType(), func);
        }
        Assertions.assertEquals(Schema.Type.int64, column(plan, "recent_n5_start_price_zeroCross").getFieldType().getType());
        Assertions.assertEquals(Schema.Type.int64, column(plan, "recent_n5_start_price_peaks").getFieldType().getType());
        // without a window: skew folds incrementally (bounded), a series readout scans the whole history (the hint)
        final FeaturePlan open = compile(SOURCES, spec.replace("      - {maxEvents: 5}\n      - {maxAge: P365D}\n", "      - {}\n"));
        Assertions.assertFalse(open.getDiagnostics().hasErrors(), open::describe);
        Assertions.assertNull(SequenceEvaluator.unboundedReason(column(open, "recent_all_start_price_skew")));
        Assertions.assertNotNull(SequenceEvaluator.unboundedReason(column(open, "recent_all_start_price_acf1")));
        // lags and orders outside 1..20, an index outside 1..order and unknown names are rejected with the list of funcs
        for (final String bad : List.of("acf0", "acf21", "ar2_3", "ar2", "kurtosis")) {
            final FeaturePlan rejected = compile(SOURCES, spec.replace("funcs: [skew,", "funcs: [" + bad + ","));
            Assertions.assertTrue(hasCode(rejected, "sequence.aggregate.func"), bad);
            Assertions.assertTrue(rejected.getDiagnostics().getErrorMessages().stream().anyMatch(m -> m.contains("skew") && m.contains("ar<p>_<i>")), bad);
        }
    }

    @Test
    public void testSelfInOpExpressionIsRejected() {
        final String spec = SPEC.replace("expr: \"sold >= 1\", halflife: [5]", "expr: \"start_price - $self.start_price\", halflife: [5]");
        Assertions.assertTrue(hasCode(compile(SOURCES, spec), "sequence.self"));
    }

    /**
     * {@code weightBy} on a sequence aggregate: the event side of the expression joins the projected history (and the
     * window shift), the {@code $self} side is a row input checked against computeAt; every func is FLOAT64 (count =
     * Σw); the aggregate is declared scan-only once per block.
     */
    @Test
    public void testSequenceWeightBy() {
        final String plain = "- {type: aggregate, field: sold, funcs: [count, mean]}";
        Assertions.assertTrue(SPEC.contains(plain));
        final String kernel = "exp(-abs(start_price - $self.start_price) / 50)";
        final String spec = SPEC.replace(plain, "- {type: aggregate, field: sold, funcs: [count, mean, std], weightBy: \"" + kernel + "\", as: near}");
        final FeaturePlan plan = compile(SOURCES, spec);
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        final OutputColumn mean = column(plan, "recent_n5_near_mean");
        Assertions.assertEquals("aggregate", mean.getOperator());
        Assertions.assertEquals(kernel, mean.getCoordinates().get("weightBy"));
        Assertions.assertEquals("sold", mean.getCoordinates().get("field"));
        Assertions.assertEquals(Set.of("sold", "start_price"), mean.getPastInputs(), "the event side of the weight is projected into the history");
        Assertions.assertTrue(mean.getInputs().containsAll(Set.of("start_price", "seller_id")));
        // sold is an outcome: the weighted window is shifted exactly like the plain aggregate's
        final OutputColumn plainMean = column(compile(SOURCES, SPEC), "recent_n5_sold_mean");
        Assertions.assertEquals(OutputColumn.Status.windowShift, mean.getStatus());
        Assertions.assertEquals(plainMean.getWindowShift(), mean.getWindowShift());
        Assertions.assertEquals(Schema.Type.float64, column(plan, "recent_n5_near_count").getFieldType().getType(), "a weighted count is Σw");
        Assertions.assertEquals(Schema.Type.int64, column(compile(SOURCES, SPEC), "recent_n5_sold_count").getFieldType().getType());
        Assertions.assertEquals(Schema.Type.float64, column(plan, "recent_365d_near_std").getFieldType().getType());
        Assertions.assertEquals(1, plan.getDiagnostics().getMessages().stream().filter(m -> m.code().equals("sequence.weightBy.scan")).count(), "reported once, not per window");
        Assertions.assertNotEquals(plan.getHash(), compile(SOURCES, spec.replace("/ 50)", "/ 25)")).getHash(), "the weight is a semantic parameter");

        // a string operand is compared by identity (a category match), with an info saying only == / != are meaningful
        final FeaturePlan identity = compile(SOURCES, SPEC.replace(plain, "- {type: aggregate, field: sold, funcs: [count], weightBy: \"category == $self.category ? 1 : 0.25\", as: same}"));
        Assertions.assertFalse(identity.getDiagnostics().hasErrors(), identity::describe);
        Assertions.assertTrue(hasCode(identity, "sequence.weightBy.identity"), identity::describe);
        Assertions.assertTrue(column(identity, "recent_n5_same_count").getPastInputs().contains("category"));
        // a block-level weightBy is the default of the aggregate ops without their own; without one it is ignored with a warning
        final FeaturePlan blockLevel = compile(SOURCES, SPEC.replace("  - name: recent\n    scope: sequence\n", "  - name: recent\n    scope: sequence\n    weightBy: \"" + kernel + "\"\n")
                .replace(plain, plain + "\n      - {type: aggregate, field: start_price, funcs: [mean], weightBy: \"1\", as: flat}"));
        Assertions.assertFalse(blockLevel.getDiagnostics().hasErrors(), blockLevel::describe);
        Assertions.assertEquals(kernel, column(blockLevel, "recent_n5_sold_mean").getCoordinates().get("weightBy"));
        Assertions.assertEquals("1", column(blockLevel, "recent_n5_flat_mean").getCoordinates().get("weightBy"), "the op's own weight wins");
        Assertions.assertNull(column(blockLevel, "recent_n5_sold_lag1").getCoordinates().get("weightBy"), "a lag is not weighted");
        Assertions.assertTrue(hasCode(compile(SOURCES, SPEC.replace("  - name: relative\n    scope: context\n", "  - name: relative\n    scope: context\n    weightBy: \"1\"\n")), "sequence.weightBy.block"));

        // a field-less weighted count: Σw over the visible rows
        final FeaturePlan count = compile(SOURCES, SPEC.replace(plain, "- {type: aggregate, weightBy: \"" + kernel + "\", as: near}"));
        Assertions.assertFalse(count.getDiagnostics().hasErrors(), count::describe);
        Assertions.assertEquals(Schema.Type.float64, column(count, "recent_n5_near_count").getFieldType().getType());
        Assertions.assertEquals(Set.of("start_price"), column(count, "recent_n5_near_count").getPastInputs());

        // the event side may read an outcome (it is past, the window shift covers it); the current row's outcome is a leak
        final FeaturePlan pastOutcome = compile(SOURCES, spec.replace(kernel, "1 + final_price"));
        Assertions.assertFalse(pastOutcome.getDiagnostics().hasErrors(), pastOutcome::describe);
        final FeaturePlan leak = compile(SOURCES, spec.replace(kernel, "exp(-abs(final_price - $self.final_price))"));
        Assertions.assertTrue(hasCode(leak, "availability.violation"), leak::describe);

        // validation
        Assertions.assertTrue(hasCode(compile(SOURCES, SPEC.replace("- {type: lag, fields: [sold, start_price], k: 2}", "- {type: lag, fields: [sold, start_price], k: 2, weightBy: \"1\"}")), "sequence.weightBy.op"));
        final FeaturePlan func = compile(SOURCES, spec.replace("funcs: [count, mean, std]", "funcs: [mean, max]"));
        Assertions.assertTrue(hasCode(func, "sequence.weightBy.func"), func::describe);
        Assertions.assertTrue(func.getDiagnostics().getErrorMessages().stream().anyMatch(m -> m.contains("count | sum | mean")), "the message lists the weighted funcs");
        // a timestamp is neither numeric nor a text compared by identity
        Assertions.assertTrue(hasCode(compile(SOURCES, spec.replace(kernel, "session_time - $self.start_price")), "sequence.weightBy.type"));
        Assertions.assertTrue(hasCode(compile(SOURCES, spec.replace(kernel, "start_price - $self.session_time")), "sequence.weightBy.type"));
        Assertions.assertTrue(hasCode(compile(SOURCES, spec.replace(kernel, "exp(-abs(start_price")), "sequence.weightBy.parse"));
        Assertions.assertTrue(hasCode(compile(SOURCES, spec.replace(kernel, "start_price - $self.nosuchfield")), "reference.unresolved"));

        // block.column and baseline references are stored by the canonical names the history and the row map carry
        final FeaturePlan qualified = compile(SOURCES, spec.replace(kernel, "exp(-abs(relative.start_price_rank - $self.market)) * market"));
        Assertions.assertFalse(qualified.getDiagnostics().hasErrors(), qualified::describe);
        final OutputColumn qualifiedMean = column(qualified, "recent_n5_near_mean");
        Assertions.assertEquals("exp(-abs(relative_start_price_rank - $self.__baseline_market)) * __baseline_market", qualifiedMean.getCoordinates().get("weightBy"));
        Assertions.assertTrue(qualifiedMean.getPastInputs().containsAll(Set.of("relative_start_price_rank", "__baseline_market")));
    }

    @Test
    public void testWindowFilterWithSelfIsAllowed() {
        final String spec = SPEC.replace("- {maxEvents: 5}", "- {maxEvents: 5, filter: \"condition_grade = $self.condition_grade\"}");
        final FeaturePlan plan = compile(SOURCES, spec);
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        final OutputColumn lag = column(plan, "recent_n5_start_price_lag1");
        Assertions.assertTrue(lag.getInputs().contains("condition_grade"));
        // same-field equality filters are reduced to an additional partition key (kept as a filter otherwise)
        Assertions.assertNull(lag.getCoordinates().get("filter"));
        Assertions.assertEquals("seller_id,condition_grade", lag.getCoordinates().get("stageKeys"));
    }

    @Test
    public void testContextRowSetDriftWarning() {
        final String corrections = SOURCES.replace("mutability: appendOnly", "mutability: corrections");
        Assertions.assertTrue(hasCode(compile(corrections, SPEC), "context.rowSetDrift"));

        final String snapshot = corrections.replace("mutability: corrections\n    keys: [session_id, seller_id]\n    fields:\n      - {name: session_id",
                "mutability: corrections\n    snapshotOf: {source: listings_snapshot, at: \"event_time - PT6H\"}\n    keys: [session_id, seller_id]\n    fields:\n      - {name: session_id");
        final FeaturePlan plan = compile(snapshot, SPEC);
        Assertions.assertFalse(hasCode(plan, "context.rowSetDrift"), plan::describe);
        Assertions.assertEquals(SourceContract.TrainingPath.snapshotBackfill, plan.getSources().get("listings").getTrainingPath());
        Assertions.assertEquals(SourceContract.TrainingPath.logAndWait, plan.getSources().get("auction_results").getTrainingPath());
    }

    @Test
    public void testIngestionLagRelativeToAvailableAt() {
        final FeaturePlan plan = compile(SOURCES, SPEC);
        final SourceContract.FieldContract bid = plan.getInputFields().get("current_bid_t10");
        Assertions.assertEquals(Duration.ofMinutes(-9), bid.getEffectiveAvailableAt().getOffset());
        final SourceContract.FieldContract rank = plan.getInputFields().get("sold");
        Assertions.assertEquals(Duration.ofDays(6).plusMinutes(30), rank.getEffectiveAvailableAt().getOffset());
    }

    @Test
    public void testExcludeByLineageSelector() {
        final String spec = SPEC.replace("prefix: f_", "prefix: f_\n  exclude: [\"derivedFrom:market\", \"composition.*\"]");
        final FeaturePlan plan = compile(SOURCES, spec);
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        Assertions.assertTrue(column(plan, "relative_current_bid_t10_rank").isIntermediate());
        Assertions.assertTrue(column(plan, "vs_market").isIntermediate());
        Assertions.assertTrue(column(plan, "composition_condition_grade_entropy").isIntermediate());
        Assertions.assertFalse(column(plan, "relative_start_price_rank").isIntermediate());
        Assertions.assertFalse(hasCode(plan, "output.exclude.unmatched"), plan::describe);
    }

    /** A pattern that selects nothing (a glob / regex the syntax does not have, a misspelling) is reported, per pattern. */
    @Test
    public void testExcludeUnmatchedPattern() {
        final String spec = SPEC.replace("prefix: f_",
                "prefix: f_\n  exclude: [\"composition.*_entropy\", \"derivedFrom:market\", \"vs_market\", \"nosuchblock.*\", \"composition.*\"]");
        final FeaturePlan plan = compile(SOURCES, spec);
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        final List<String> unmatched = plan.getDiagnostics().getMessages().stream()
                .filter(d -> "output.exclude.unmatched".equals(d.code())).map(Diagnostics.Message::message).toList();
        Assertions.assertEquals(2, unmatched.size(), plan::describe);
        Assertions.assertTrue(unmatched.get(0).contains("'composition.*_entropy'"), unmatched::toString);
        Assertions.assertTrue(unmatched.get(1).contains("'nosuchblock.*'"), unmatched::toString);
        // the second pattern to match a column is still credited (no early exit): composition.* is not reported
        Assertions.assertTrue(column(plan, "composition_condition_grade_entropy").isIntermediate());
        // exclude is ignored (and not lint-checked) when include is declared
        final FeaturePlan included = compile(SOURCES, spec.replace("  exclude:", "  include: [price_per_unit]\n  exclude:"));
        Assertions.assertFalse(hasCode(included, "output.exclude.unmatched"), included::describe);
        Assertions.assertTrue(hasCode(included, "output.include.exclude"), included::describe);
    }

    @Test
    public void testGroupByPlacementAndIndicator() {
        final String spec = SPEC.replace("prefix: f_", "prefix: f_\n  groupBy: session\n  nullPolicy: indicator");
        final FeaturePlan plan = compile(SOURCES, spec);
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        Assertions.assertEquals(OutputColumn.Placement.parent, column(plan, "composition_condition_grade_entropy").getPlacement());
        Assertions.assertEquals(OutputColumn.Placement.child, column(plan, "relative_start_price_rank").getPlacement());
        Assertions.assertEquals("f_recent_n5_start_price_lag1_isnull", column(plan, "recent_n5_start_price_lag1_isnull").getOutputName());
        Assertions.assertEquals(FeaturePlan.StageKind.groupBy, plan.getStages().get(plan.getStages().size() - 1).kind());
    }

    @Test
    public void testHashIsOrderIndependent() {
        final String reordered = SPEC.replace("predictAt: \"event_time - PT8M\"\n", "")
                .replace("lineage:", "predictAt: \"event_time - PT8M\"\nlineage:");
        Assertions.assertEquals(compile(SOURCES, SPEC).getHash(), compile(SOURCES, reordered).getHash());
        Assertions.assertNotEquals(compile(SOURCES, SPEC).getHash(), compile(SOURCES, SPEC.replace("PT8M", "PT5M")).getHash());
    }

    @Test
    public void testUnsupportedPopulationTypeAndFitMode() {
        // every registered population type is implemented: an unknown one is a type error, and an encoding's
        // parameters do not make a sequence-of-values block
        Assertions.assertTrue(hasCode(compile(SOURCES, SPEC.replace("type: encoding", "type: kernelEmbedding")), "population.type"));
        Assertions.assertTrue(hasCode(compile(SOURCES, SPEC.replace("type: encoding", "type: spectralEmbedding")), "spectralEmbedding.sequenceOf"));
        Assertions.assertTrue(OperatorCatalog.IMPLEMENTED_POPULATION_TYPES.containsAll(List.of("spectralEmbedding", "transitionStats")));
        final String folds = SPEC.replace("output:\n  prefix: f_", "fit: {mode: fold, folds: 1}\noutput:\n  prefix: f_");
        Assertions.assertTrue(hasCode(compile(SOURCES, folds), "fit.folds"));
    }

    @Test
    public void testFoldFitExpansion() {
        final String block = """
                  - name: enc
                    scope: population
                    type: encoding
                    fit: {mode: fold, folds: 3, groupBy: seller, artifact: "gs://bucket/features"}
                    keySets:
                      - keys: [seller_id]
                        windows: [{maxAge: P365D}]
                    targets:
                      - {stats: [count]}
                      - {expr: "sold >= 1", stats: [mean, std]}
                    shrinkage: {priorWeight: 5}
            """;
        final FeaturePlan plan = compile(SOURCES, withEncoding(block));
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        Assertions.assertTrue(hasCode(plan, "fit.mode.fold"));
        Assertions.assertTrue(hasCode(plan, "fit.mode.static.windows"));
        Assertions.assertFalse(hasCode(plan, "fit.fold.identity")); // entity folds
        // same hidden statistics as static, tagged with the fold unit (the seller entity's keys)
        for (final String hidden : List.of("enc__seller_id__e2__n", "enc__seller_id__e2__sumsq", "enc__global__e2__n")) {
            final OutputColumn c = column(plan, hidden);
            Assertions.assertTrue(c.isIntermediate(), hidden);
            Assertions.assertEquals("fold", c.getCoordinates().get("fit"));
            Assertions.assertEquals("seller_id", c.getCoordinates().get("foldKeys"));
            Assertions.assertEquals("3", c.getCoordinates().get("folds"));
            Assertions.assertEquals("gs://bucket/features", c.getCoordinates().get("artifactUri"));
            Assertions.assertEquals(OutputColumn.Status.staticSafe, c.getStatus());
        }
        Assertions.assertEquals("fitStat", column(plan, "enc__seller_id__count").getOperator());
        Assertions.assertEquals("compose", column(plan, "enc__seller_id__e2__mean").getOperator());
        Assertions.assertTrue(plan.getStages().stream().anyMatch(s -> s.kind() == FeaturePlan.StageKind.fit), plan::describe);

        // a block-level groupBy naming an unknown entity is an error (it must not fall back to row folds)
        final FeaturePlan typo = compile(SOURCES, withEncoding(block.replace("groupBy: seller", "groupBy: sellr")));
        Assertions.assertTrue(hasCode(typo, "fit.groupBy"), typo::describe);
        // fold is batch-only: the engine rejects it in streaming even with an artifact
        Assertions.assertTrue(FeatureStages.engineConstraints(plan, true).stream().anyMatch(m -> m.contains("fit.mode fold")));
        Assertions.assertTrue(FeatureStages.engineConstraints(plan, false).stream().noneMatch(m -> m.contains("fit.mode fold")));

        // without groupBy the fold unit is the row identity: time.field + orderTieBreak
        final FeaturePlan rows = compile(SOURCES, withEncoding(block.replace(", groupBy: seller", "")));
        Assertions.assertFalse(rows.getDiagnostics().hasErrors(), rows::describe);
        Assertions.assertEquals("session_time,session_id", column(rows, "enc__seller_id__e2__n").getCoordinates().get("foldKeys"));
        Assertions.assertFalse(hasCode(rows, "fit.fold.identity"));
        // ... and time.field alone (with a warning) when no tie-break is declared
        final FeaturePlan noTie = compile(SOURCES, withEncoding(block.replace(", groupBy: seller", "")).replace(", orderTieBreak: [session_id]", ""));
        Assertions.assertFalse(noTie.getDiagnostics().hasErrors(), noTie::describe);
        Assertions.assertTrue(hasCode(noTie, "fit.fold.identity"));
        Assertions.assertEquals("session_time", column(noTie, "enc__seller_id__e2__n").getCoordinates().get("foldKeys"));
    }

    @Test
    public void testOffsetRequiresPredictAtComputeAt() {
        final String spec = SPEC.replace("maxFeatures: 50", "maxFeatures: 50\n    offset: market\n    computeAt: \"event_time - PT1H\"");
        final FeaturePlan plan = compile(SOURCES, spec);
        Assertions.assertTrue(hasCode(plan, "encoding.offset.computeAt"), plan::describe);
        final String ok = SPEC.replace("maxFeatures: 50", "maxFeatures: 50\n    offset: market");
        final FeaturePlan okPlan = compile(SOURCES, ok);
        Assertions.assertFalse(okPlan.getDiagnostics().hasErrors(), okPlan::describe);
        Assertions.assertEquals("market", column(okPlan, "enc__seller_id__count").getCoordinates().get("offset"));
    }

    /**
     * An offset on a logit / log shrinkage scale is accepted: the block's levels keep a hidden Σ baseline
     * ({@code __sumoff}) next to Σ(y − b), the composed column reads it through its {@code levels} coordinate, and
     * an info diagnostic says the value is the additive term on the scale. On identity nothing changes (no extra
     * column), and the joint estimator accepts the same declaration.
     */
    @Test
    public void testOffsetOnLogitScaleKeepsBaselineSum() {
        final String logit = SPEC.replace("maxFeatures: 50", "maxFeatures: 50\n    offset: market\n    shrinkage: {priorWeight: 2, scale: logit}");
        final FeaturePlan plan = compile(SOURCES, logit);
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        Assertions.assertTrue(hasCode(plan, "encoding.offset.additive"), plan::describe);
        final OutputColumn off = column(plan, "enc__seller_id__e2__sumoff");
        Assertions.assertTrue(off.isIntermediate());
        Assertions.assertEquals("sumoff", off.getCoordinates().get("stat"));
        Assertions.assertEquals("market", off.getCoordinates().get("offset"));
        Assertions.assertEquals(off.getCoordinates().get("field"), column(plan, "enc__seller_id__e2__sum").getCoordinates().get("field"));
        Assertions.assertNotNull(plan.getColumn("enc__global__e2__sumoff"));
        Assertions.assertNotNull(plan.getColumn("enc__category__365d__e2__sumoff"));
        final OutputColumn composed = column(plan, "enc__seller_id__e2__mean");
        Assertions.assertEquals("compose", composed.getOperator());
        Assertions.assertEquals("logit", composed.getCoordinates().get("scale"));
        Assertions.assertTrue(composed.getCoordinates().get("levels").contains("enc__seller_id__e2__sumoff"), composed.getCoordinates().get("levels"));
        Assertions.assertTrue(composed.getInputs().contains("enc__seller_id__e2__sumoff"));
        // the target-less count / share levels have no baseline sum
        Assertions.assertNull(plan.getColumn("enc__seller_id__sumoff"));

        final FeaturePlan identity = compile(SOURCES, SPEC.replace("maxFeatures: 50", "maxFeatures: 50\n    offset: market\n    shrinkage: {priorWeight: 2}"));
        Assertions.assertFalse(identity.getDiagnostics().hasErrors(), identity::describe);
        Assertions.assertFalse(hasCode(identity, "encoding.offset.additive"));
        Assertions.assertNull(identity.getColumn("enc__seller_id__e2__sumoff"));
        Assertions.assertFalse(column(identity, "enc__seller_id__e2__mean").getCoordinates().get("levels").contains("sumoff"));

        final FeaturePlan joint = compile(SOURCES, SPEC.replace("maxFeatures: 50", "maxFeatures: 50\n    offset: market\n    shrinkage: {priorWeight: 2, scale: log, estimator: joint}\n    fit: {mode: static}"));
        Assertions.assertFalse(joint.getDiagnostics().hasErrors(), joint::describe);
        Assertions.assertTrue(hasCode(joint, "encoding.offset.additive"), joint::describe);
        Assertions.assertEquals("joint", column(joint, "enc__seller_id__e2__mean").getOperator());
        Assertions.assertEquals("market", column(joint, "enc__seller_id__e2__mean").getCoordinates().get("offset"));
    }

    /**
     * quantileTransform accepts {@code fit.mode: forward} like svd: the column carries the block geometry, an outcome
     * input delays the blocks it may read (forwardLagMillis), a block without its own mode follows a top-level forward
     * fit, and the other modes stay rejected.
     */
    @Test
    public void testQuantileTransformForwardFit() {
        final String qt = SPEC.replace("output:\n  prefix: f_", """
                  - name: price_q
                    scope: population
                    type: quantileTransform
                    input: start_price
                    bins: 10
                    fit: {mode: forward, blocks: {size: P7D}, window: P10D, minHistory: P21D}
                output:
                  prefix: f_""".replaceAll("(?m)^                ", ""));
        final FeaturePlan plan = compile(SOURCES, qt);
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        Assertions.assertTrue(hasCode(plan, "fit.mode.forward"), plan::describe);
        final OutputColumn c = column(plan, "price_q");
        Assertions.assertEquals("forward", c.getCoordinates().get("fit"));
        Assertions.assertEquals(Long.toString(7 * 86_400_000L), c.getCoordinates().get("blockSizeMillis"));
        Assertions.assertEquals("2", c.getCoordinates().get("windowBlocks"), "P10D rounds up to 2 weekly blocks");
        Assertions.assertEquals("3", c.getCoordinates().get("minBlocks"), "P21D = 3 weekly blocks");
        Assertions.assertEquals("0", c.getCoordinates().get("forwardLagMillis"));
        Assertions.assertEquals(Long.toString(-8 * 60_000L), c.getCoordinates().get("predictOffsetMillis"));
        Assertions.assertEquals("session_time", c.getCoordinates().get("blockField"));
        Assertions.assertEquals("10", c.getCoordinates().get("bins"));
        Assertions.assertEquals(OutputColumn.Status.staticSafe, c.getStatus());
        // one fit stage, like the static form
        Assertions.assertEquals(1, plan.getStages().stream().filter(s -> s.kind() == FeaturePlan.StageKind.fit && s.columnNames().contains("price_q")).count());
        // static keeps its coordinates (and warns that window is forward-only); expanding / fold stay rejected
        final FeaturePlan statik = compile(SOURCES, qt.replace("mode: forward, ", ""));
        Assertions.assertFalse(statik.getDiagnostics().hasErrors(), statik::describe);
        Assertions.assertEquals("static", column(statik, "price_q").getCoordinates().get("fit"));
        Assertions.assertNull(column(statik, "price_q").getCoordinates().get("windowBlocks"));
        Assertions.assertTrue(hasCode(statik, "quantileTransform.fit.window"), statik::describe);
        Assertions.assertTrue(hasCode(compile(SOURCES, qt.replace("mode: forward", "mode: fold")), "quantileTransform.fit.mode"));
        Assertions.assertNotEquals(plan.getHash(), compile(SOURCES, qt.replace("window: P10D", "window: P30D")).getHash());
        // the knots of an outcome are only known after settlement + ingestion: the readable blocks are delayed by that lag
        final OutputColumn outcome = column(compile(SOURCES, qt.replace("input: start_price", "input: final_price")), "price_q");
        Assertions.assertTrue(Long.parseLong(outcome.getCoordinates().get("forwardLagMillis")) > 6L * 86_400_000L, outcome.getCoordinates()::toString);

        // inheritance: no fit.mode of its own under a top-level forward fit → forward; an explicit static opts out
        final String blockFit = "    fit: {mode: forward, blocks: {size: P7D}, window: P10D, minHistory: P21D}\n";
        final String topFit = "fit: {mode: forward, blocks: {size: P7D}, window: P10D, minHistory: P21D}\noutput:\n  prefix: f_";
        final String inherited = qt.replace(blockFit, "").replace("output:\n  prefix: f_", topFit);
        final FeaturePlan inheritedPlan = compile(SOURCES, inherited);
        Assertions.assertFalse(inheritedPlan.getDiagnostics().hasErrors(), inheritedPlan::describe);
        Assertions.assertEquals("forward", column(inheritedPlan, "price_q").getCoordinates().get("fit"), inheritedPlan::describe);
        Assertions.assertEquals("2", column(inheritedPlan, "price_q").getCoordinates().get("windowBlocks"));
        final FeaturePlan optedOut = compile(SOURCES, inherited.replace("    type: quantileTransform", "    type: quantileTransform\n    fit: {mode: static}"));
        Assertions.assertEquals("static", column(optedOut, "price_q").getCoordinates().get("fit"));
        Assertions.assertTrue(hasCode(optedOut, "quantileTransform.fit.mode.static"), optedOut::describe);
        // discretize stays static-only
        Assertions.assertTrue(hasCode(compile(SOURCES, qt.replace("type: quantileTransform", "type: discretize")), "discretize.fit.mode"));
    }

    /**
     * svd accepts {@code fit.mode: forward}: the score columns carry the block geometry, the window / minimum history
     * rounded to blocks, the inputs' availability lag and the predictAt offset; the other lookup modes stay rejected.
     * An encoding's forward fit takes {@code fit.window} / {@code fit.minHistory} as block-level defaults.
     */
    @Test
    public void testForwardFitWindowAndMinHistory() {
        final String svd = SPEC.replace("output:\n  prefix: f_", """
                  - name: pc
                    scope: population
                    type: svd
                    inputs: [start_price, current_bid_t10]
                    rank: 2
                    fit: {mode: forward, blocks: {size: P7D}, window: P10D, minHistory: P21D}
                output:
                  prefix: f_""".replaceAll("(?m)^                ", ""));
        final FeaturePlan plan = compile(SOURCES, svd);
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        Assertions.assertTrue(hasCode(plan, "fit.mode.forward"), plan::describe);
        Assertions.assertTrue(hasCode(plan, "fit.mode.forward.window"), plan::describe);
        final OutputColumn c = column(plan, "pc_0");
        Assertions.assertEquals("forward", c.getCoordinates().get("fit"));
        Assertions.assertEquals(Long.toString(7 * 86_400_000L), c.getCoordinates().get("blockSizeMillis"));
        Assertions.assertEquals("2", c.getCoordinates().get("windowBlocks"), "P10D rounds up to 2 weekly blocks");
        Assertions.assertEquals("3", c.getCoordinates().get("minBlocks"), "P21D = 3 weekly blocks");
        Assertions.assertEquals("0", c.getCoordinates().get("forwardLagMillis"), "attribute / pre-event market inputs have no lag");
        Assertions.assertEquals(Long.toString(-8 * 60_000L), c.getCoordinates().get("predictOffsetMillis"));
        Assertions.assertEquals("session_time", c.getCoordinates().get("blockField"));
        // an explicit minBlocks wins over minHistory; a static fit warns that window is forward-only; fold stays rejected
        Assertions.assertEquals("5", column(compile(SOURCES, svd.replace("minHistory: P21D", "minHistory: P21D, minBlocks: 5")), "pc_0").getCoordinates().get("minBlocks"));
        final FeaturePlan statik = compile(SOURCES, svd.replace("mode: forward, ", ""));
        Assertions.assertFalse(statik.getDiagnostics().hasErrors(), statik::describe);
        Assertions.assertEquals("static", column(statik, "pc_0").getCoordinates().get("fit"));
        Assertions.assertNull(column(statik, "pc_0").getCoordinates().get("windowBlocks"));
        Assertions.assertTrue(hasCode(statik, "svd.fit.window"), statik::describe);
        Assertions.assertTrue(hasCode(compile(SOURCES, svd.replace("mode: forward", "mode: fold")), "svd.fit.mode"));
        // the plan hash covers the fit window
        Assertions.assertNotEquals(plan.getHash(), compile(SOURCES, svd.replace("window: P10D", "window: P30D")).getHash());

        // a block with no fit.mode of its own follows a top-level forward fit, geometry included
        final String blockFit = "    fit: {mode: forward, blocks: {size: P7D}, window: P10D, minHistory: P21D}\n";
        final String topFit = "fit: {mode: forward, blocks: {size: P7D}, window: P10D, minHistory: P21D}\noutput:\n  prefix: f_";
        final String inherited = svd.replace(blockFit, "").replace("output:\n  prefix: f_", topFit);
        final FeaturePlan inheritedPlan = compile(SOURCES, inherited);
        Assertions.assertFalse(inheritedPlan.getDiagnostics().hasErrors(), inheritedPlan::describe);
        final OutputColumn ic = column(inheritedPlan, "pc_0");
        Assertions.assertEquals("forward", ic.getCoordinates().get("fit"), inheritedPlan::describe);
        Assertions.assertEquals("2", ic.getCoordinates().get("windowBlocks"));
        Assertions.assertEquals("3", ic.getCoordinates().get("minBlocks"));
        Assertions.assertFalse(hasCode(inheritedPlan, "svd.fit.mode.static"), inheritedPlan::describe);
        // an explicit static opts the block out of the spec's forward walk, with an info naming the whole-input fit
        final FeaturePlan optedOut = compile(SOURCES, inherited.replace("    type: svd", "    type: svd\n    fit: {mode: static}"));
        Assertions.assertEquals("static", column(optedOut, "pc_0").getCoordinates().get("fit"));
        Assertions.assertTrue(hasCode(optedOut, "svd.fit.mode.static"), optedOut::describe);
        // the other top-level modes have no lookup-fit counterpart: the block stays static and says nothing
        final FeaturePlan expanding = compile(SOURCES, inherited.replace("mode: forward,", "mode: expanding,"));
        Assertions.assertEquals("static", column(expanding, "pc_0").getCoordinates().get("fit"));
        Assertions.assertFalse(hasCode(expanding, "svd.fit.mode.static"), expanding::describe);

        // encoding: fit.window / minHistory at the top level apply where a keySet declares no maxAge
        final String enc = SPEC.replace("output:\n  prefix: f_", "fit: {mode: forward, blocks: {size: P7D}, window: P10D, minHistory: P21D}\noutput:\n  prefix: f_");
        final FeaturePlan encPlan = compile(SOURCES, enc);
        Assertions.assertFalse(encPlan.getDiagnostics().hasErrors(), encPlan::describe);
        final OutputColumn sellerN = column(encPlan, "enc__seller_id__e2__n");
        Assertions.assertEquals("2", sellerN.getCoordinates().get("windowBlocks"), "fit.window fills in for a keySet without maxAge");
        Assertions.assertEquals("3", sellerN.getCoordinates().get("minBlocks"));
        // a keySet's own maxAge (P365D) takes precedence over fit.window
        Assertions.assertEquals("53", column(encPlan, "enc__category__365d__e2__n").getCoordinates().get("windowBlocks"), "the unshrunk stat's hidden level keeps the keySet window under forward");
        Assertions.assertEquals("53", column(encPlan, "enc__category__365d__n").getCoordinates().get("windowBlocks"));
    }

    private static final String LATTICE_ENC = """
                  - name: enc
                    scope: population
                    type: encoding
                    keySets:
                      - keys: [seller_id]
                        hierarchy: [[category], []]
                    targets:
                      - {expr: "sold >= 1", stats: [mean]}
                    shrinkage: {priorWeight: 2, scale: identity, output: [composed, deviations, effectiveN]}
            """;

    private static String withEncoding(final String encodingBlock) {
        final int start = SPEC.indexOf("  - name: enc\n");
        final int end = SPEC.indexOf("output:\n");
        return SPEC.substring(0, start) + encodingBlock.replaceAll("(?m)^    ", "") + SPEC.substring(end);
    }

    /**
     * The residual's scale is the key {@code on} — a boolean under YAML 1.1, where a YAML spec delivered it as
     * {@code "true"} and the residual fell back to identity without a word. Configs are parsed as YAML 1.2 (core
     * schema), so the key keeps its name: a bare and a quoted {@code on} both reach the column.
     */
    @Test
    public void testResidualScaleFromYaml() {
        Assertions.assertEquals("identity", column(compile(SOURCES, SPEC), "vs_market").getCoordinates().get("on"));
        Assertions.assertEquals("logit", column(compile(SOURCES, SPEC.replace("on: identity", "on: logit")), "vs_market").getCoordinates().get("on"));
        Assertions.assertEquals("log", column(compile(SOURCES, SPEC.replace("on: identity", "\"on\": log")), "vs_market").getCoordinates().get("on"));
        Assertions.assertEquals("identity", column(compile(SOURCES, SPEC.replace("    on: identity\n", "")), "vs_market").getCoordinates().get("on"), "the default");
        // the parsed document carries the key under its own name, never as the boolean's text
        final JsonObject specJson = Config.convertConfigJson(SPEC.replace("on: identity", "on: logit"), Config.Format.yaml);
        int residuals = 0;
        for (final com.google.gson.JsonElement f : specJson.getAsJsonArray("features")) {
            final JsonObject block = f.getAsJsonObject();
            Assertions.assertFalse(block.has("true"), block::toString);
            if (block.has("on")) {
                Assertions.assertEquals("logit", block.get("on").getAsString());
                residuals++;
            }
        }
        Assertions.assertEquals(1, residuals);
        // an unknown scale is rejected, and the plan hash sees the scale
        Assertions.assertTrue(hasCode(compile(SOURCES, SPEC.replace("on: identity", "on: probit")), "row.residual.on"));
        Assertions.assertNotEquals(compile(SOURCES, SPEC).getHash(), compile(SOURCES, SPEC.replace("on: identity", "on: logit")).getHash());
    }

    @Test
    public void testEngineRowIdMustBeInputFields() {
        final FeaturePlan bad = compile(SOURCES, SPEC.replace("output:\n", "engine: {rowId: [session_id, nope]}\noutput:\n"));
        Assertions.assertTrue(hasCode(bad, "engine.rowId"), bad::describe);
        final FeaturePlan ok = compile(SOURCES, SPEC.replace("output:\n", "engine: {rowId: [session_id, seller_id], parallelWaves: false}\noutput:\n"));
        Assertions.assertFalse(ok.getDiagnostics().hasErrors(), ok::describe);
        Assertions.assertEquals(List.of("session_id", "seller_id"), ok.getSpec().engine.rowId);
        Assertions.assertFalse(ok.getSpec().engine.parallelWaves);
        // engine knobs do not change the plan hash
        Assertions.assertEquals(compile(SOURCES, SPEC).getHash(), ok.getHash());
    }

    @Test
    public void testReservedInputFieldRejected() {
        // the fan-out merge rides __rowId / __partial in the row map: an input field with either name would
        // make every base row look like a partial, in the linear chain too
        final FeaturePlan bad = compile(
                SOURCES.replace("      - {name: category, type: string}\n", "      - {name: category, type: string}\n      - {name: __rowId, type: string}\n"),
                SPEC.replace("[session_id, seller_id, category", "[session_id, seller_id, category, __rowId"));
        Assertions.assertTrue(hasCode(bad, "input.reserved"), bad::describe);
    }

    @Test
    public void testDagShuffleEstimateFoldsIntoContextStage() {
        // a context block reading both keyed stages lands in a wave of its own: a single context stage whose
        // key (session_id) the base rows carry, so the wave-1 merge rides its GroupByKey and the estimate
        // counts RowId_Pin + the wave-1 branches + the folded stage — one less than the linear chain
        final String rel = "  - name: rel\n    scope: context\n    context: session\n    inputs: [recent_n5_sold_count, enc__seller_id__count]\n    ops: [zscore]\n";
        final FeaturePlan plan = compile(SOURCES, SPEC.replace("output:\n", rel + "output:\n"));
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        Assertions.assertEquals(2, plan.getWaves().size(), plan::describe);
        Assertions.assertEquals(5, plan.getShuffleCount(), plan::describe);
        Assertions.assertEquals(3, plan.getDagShuffleEstimate(), plan::describe);
        // a declared engine.rowId removes the pinning Reshuffle from the estimate too
        final FeaturePlan declared = compile(SOURCES,
                SPEC.replace("output:\n", rel + "engine: {rowId: [session_id, seller_id]}\noutput:\n"));
        Assertions.assertFalse(declared.getDiagnostics().hasErrors(), declared::describe);
        Assertions.assertEquals(2, declared.getDagShuffleEstimate(), declared::describe);
    }

    @Test
    public void testGlobalKeyStageHint() {
        // a single-key stage (a lattice's global level, a share denominator) is one worker thread and the
        // critical path of a parallel-wave run: the S4 hint points at fit.mode static / fold, one hint per
        // stage at the blocks that force the global level
        final FeaturePlan lattice = compile(SOURCES, withEncoding(LATTICE_ENC));
        Assertions.assertTrue(lattice.getDiagnostics().getMessages().stream()
                        .anyMatch(m -> m.code().equals("encoding.globalKey") && m.location().startsWith("features.")),
                lattice::describe);
        // the plain SPEC's share statistic needs a global denominator stage: the hint fires there too
        Assertions.assertTrue(hasCode(compile(SOURCES, SPEC), "encoding.globalKey"));
        // no share statistic, no hierarchy: no global stage, no hint
        final FeaturePlan noGlobal = compile(SOURCES, SPEC.replace("stats: [count, share]", "stats: [count]"));
        Assertions.assertFalse(hasCode(noGlobal, "encoding.globalKey"), noGlobal::describe);
    }

    @Test
    public void testStageDependenciesAndWaves() {
        // the levels of a shrinkage lattice are independent keyed stages: the seller level (fused with the
        // sequence block), the global level and the context stage form one wave; the category stage hosts the
        // compose rows over all three levels, so it depends on them (the row expression the levels share is
        // followed through to its input field: placing it in the seller stage is not a data dependency)
        final FeaturePlan plan = compile(SOURCES, withEncoding(LATTICE_ENC));
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        final List<FeaturePlan.Stage> stages = plan.getStages();
        Assertions.assertEquals(4, stages.size(), plan::describe);
        Assertions.assertEquals(List.of(), stages.get(0).dependsOn(), plan::describe);
        Assertions.assertEquals(List.of(), stages.get(1).dependsOn(), plan::describe);
        Assertions.assertEquals(List.of(), stages.get(2).dependsOn(), plan::describe);
        Assertions.assertEquals(List.of(0, 1, 2), stages.get(3).dependsOn(), plan::describe);
        Assertions.assertEquals(List.of(List.of(0, 1, 2), List.of(3)), plan.getWaves(), plan::describe);
        Assertions.assertEquals(1, plan.getWave(2));
        Assertions.assertEquals(2, plan.getWave(3));
        // linear chain: 4 shuffles; wave DAG: wave 1 = one shuffle for its three keyed branches + row-id merge
        // + Reshuffle pinning the ids, wave 2 = one shuffle
        Assertions.assertEquals(4, plan.getShuffleCount());
        Assertions.assertEquals(4, plan.getDagShuffleEstimate());
        Assertions.assertTrue(plan.describe().contains("waves=2 (dag shuffles~4)"), plan::describe);
        Assertions.assertTrue(plan.describe().contains("deps=[0, 1, 2] wave=2"), plan::describe);
        final com.google.gson.JsonObject json = plan.toJson();
        Assertions.assertEquals(2, json.get("waves").getAsInt());
        Assertions.assertEquals(4, json.get("dagShuffles").getAsInt());
        final com.google.gson.JsonObject last = json.getAsJsonArray("stages").get(3).getAsJsonObject();
        Assertions.assertEquals(2, last.get("wave").getAsInt());
        Assertions.assertEquals(3, last.getAsJsonArray("dependsOn").size());

        // output.groupBy: the finalize stage depends on every stage; the merge of the wave before it folds
        // into its GroupByKey (no extra shuffle)
        final FeaturePlan grouped = compile(SOURCES, SPEC.replace("prefix: f_", "prefix: f_" + (char) 10 + "  groupBy: session"));
        Assertions.assertFalse(grouped.getDiagnostics().hasErrors(), grouped::describe);
        final FeaturePlan.Stage groupBy = grouped.getStages().get(grouped.getStages().size() - 1);
        Assertions.assertEquals(FeaturePlan.StageKind.groupBy, groupBy.kind());
        Assertions.assertEquals(List.of(0, 1, 2, 3), groupBy.dependsOn(), grouped::describe);
        Assertions.assertEquals(3, grouped.getWaves().size(), grouped::describe);
        Assertions.assertEquals(5, grouped.getShuffleCount());
        Assertions.assertEquals(5, grouped.getDagShuffleEstimate());
    }

    @Test
    public void testShrinkageLatticeExpansion() {
        final FeaturePlan plan = compile(SOURCES, withEncoding(LATTICE_ENC));
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);

        // hidden sufficient statistics per level (leaf, parent, global), composed in a row column
        for (final String hidden : List.of("enc__seller_id__e1__n", "enc__seller_id__e1__sum", "enc__category__e1__n", "enc__global__e1__n", "enc__global__e1__sum")) {
            final OutputColumn c = column(plan, hidden);
            Assertions.assertTrue(c.isIntermediate(), hidden);
            Assertions.assertEquals(FeatureSpec.Scope.population, c.getScope());
            Assertions.assertEquals(OutputColumn.Status.windowShift, c.getStatus());
        }
        final OutputColumn composed = column(plan, "enc__seller_id__e1__mean");
        Assertions.assertEquals("compose", composed.getOperator());
        Assertions.assertEquals(FeatureSpec.Scope.row, composed.getScope());
        Assertions.assertFalse(composed.isIntermediate());
        Assertions.assertEquals("2.0", composed.getCoordinates().get("priorWeight"));
        Assertions.assertEquals("backoff", composed.getCoordinates().get("estimator"));
        Assertions.assertTrue(composed.getCoordinates().get("levels").contains("global"));
        Assertions.assertTrue(composed.getInputs().contains("enc__category__e1__sum"));
        Assertions.assertTrue(composed.getDerivedFrom().contains("outcome"));
        Assertions.assertEquals("0", column(plan, "enc__seller_id__e1__dev0").getCoordinates().get("level"));
        Assertions.assertEquals("category", column(plan, "enc__seller_id__e1__dev1").getCoordinates().get("levelKeys"));
        column(plan, "enc__seller_id__e1__mean__neff");

        // stages: ... → seller_id level (fused with the seller_id sequence block) → global level → category level
        // (+ fused compose rows)
        final List<FeaturePlan.Stage> stages = plan.getStages();
        final FeaturePlan.Stage last = stages.get(stages.size() - 1);
        Assertions.assertEquals(List.of("category"), last.keys());
        Assertions.assertTrue(last.columnNames().contains("enc__seller_id__e1__mean"));
        Assertions.assertEquals(List.of(), stages.get(stages.size() - 2).keys());
        final FeaturePlan.Stage sellerStage = stages.stream().filter(s -> s.keys().equals(List.of("seller_id"))).findFirst().orElseThrow();
        Assertions.assertTrue(sellerStage.blocks().containsAll(List.of("recent", "enc")), plan::describe);
        Assertions.assertTrue(sellerStage.columnNames().contains("enc__seller_id__e1__n"), plan::describe);
    }

    @Test
    public void testStageSchedulingByKeyAffinity() {
        // blocks keyed by seller_id are separated by a category block in the config: they still share one stage,
        // and the category encoding (no dependency on them) joins the earliest category stage
        final String enc = """
                  - name: enc_a
                    scope: population
                    type: encoding
                    keySets:
                      - keys: [seller_id]
                    targets:
                      - {field: sold, stats: [count]}
                  - name: enc_b
                    scope: population
                    type: encoding
                    keySets:
                      - keys: [category]
                    targets:
                      - {field: sold, stats: [count]}
                  - name: enc_c
                    scope: population
                    type: encoding
                    keySets:
                      - keys: [seller_id]
                    targets:
                      - {field: sold, stats: [mean]}
            """;
        final FeaturePlan plan = compile(SOURCES, withEncoding(enc));
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        final List<FeaturePlan.Stage> keyed = plan.getStages().stream()
                .filter(s -> s.kind() == FeaturePlan.StageKind.sequence || s.kind() == FeaturePlan.StageKind.population).toList();
        Assertions.assertEquals(2, keyed.size(), plan::describe);
        Assertions.assertEquals(List.of("seller_id"), keyed.get(0).keys());
        Assertions.assertEquals(List.of("recent", "enc_a", "enc_c"), keyed.get(0).blocks(), plan::describe);
        Assertions.assertEquals(List.of("category"), keyed.get(1).keys());
        Assertions.assertTrue(keyed.get(1).blocks().contains("enc_b"), plan::describe);
        // the relative order inside the fused stage is the expansion order
        final List<String> names = keyed.get(0).columnNames();
        Assertions.assertTrue(names.indexOf("recent_n5_sold_lag1") < names.indexOf("enc_a__seller_id__sold__count"), plan::describe);
        Assertions.assertTrue(names.indexOf("enc_a__seller_id__sold__count") < names.indexOf("enc_c__seller_id__sold__mean"), plan::describe);
        // row columns only the output reads are evaluated in the last stage (not carried through the shuffles);
        // the anonymous target expression of the sequence block sits in its consumer's stage
        final FeaturePlan.Stage last = plan.getStages().get(plan.getStages().size() - 1);
        Assertions.assertTrue(last.columnNames().containsAll(List.of("price_per_unit", "vs_market", "time_parts_month_sin")), plan::describe);
        Assertions.assertTrue(keyed.get(0).columnNames().contains("recent__e1"), plan::describe);
        Assertions.assertEquals(plan.getStages().stream().filter(st -> st.kind() != FeaturePlan.StageKind.row && st.kind() != FeaturePlan.StageKind.fit).count(),
                plan.getShuffleCount());
    }

    @Test
    public void testStaticFitBlockStaysInOneFitStage() {
        // the levels of a static block have different dependencies (one key is derived from a keyed stage's
        // output): they still share the block's single fit stage, together with the row columns over them
        final String enc = """
                  - name: bucket
                    scope: row
                    expr: "recent_n5_sold_count > 2"
                  - name: enc
                    scope: population
                    type: encoding
                    fit: {mode: static, artifact: "gs://bucket/features"}
                    keySets:
                      - keys: [category]
                      - keys: [bucket]
                    targets:
                      - {stats: [count, share]}
                      - {expr: "sold >= 1", stats: [mean]}
                    shrinkage: {priorWeight: 5, weights: varianceComponents}
            """;
        final FeaturePlan plan = compile(SOURCES, withEncoding(enc));
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        final List<FeaturePlan.Stage> fits = plan.getStages().stream().filter(s -> s.kind() == FeaturePlan.StageKind.fit).toList();
        Assertions.assertEquals(1, fits.size(), plan::describe);
        final FeaturePlan.Stage fit = fits.get(0);
        for (final String name : List.of("enc__category__n", "enc__bucket__n", "enc__global__n", "enc__bucket__e2__sum",
                "enc__category__count", "enc__bucket__share", "enc__bucket__e2__mean")) {
            Assertions.assertTrue(fit.columnNames().contains(name), () -> name + "\n" + plan.describe());
        }
        // the fit stage reads bucket from its input: bucket is evaluated in an earlier stage (the sequence stage)
        final int bucketStage = plan.getStages().stream().filter(s -> s.columnNames().contains("bucket")).findFirst().orElseThrow().index();
        Assertions.assertTrue(bucketStage < fit.index(), plan::describe);
        Assertions.assertTrue(plan.getStages().get(bucketStage).columnNames().contains("recent_n5_sold_count"), plan::describe);
    }

    @Test
    public void testUnboundedColumnFusesWithItsKey() {
        // a scan-path window without maxAge keeps its own inputs for the whole history of its key; the history
        // is trimmed per field, so it shares the key's stage without extending the other columns' retention
        final String enc = """
                  - name: pinned
                    scope: sequence
                    entity: seller
                    windows: [{filter: "start_price > 10"}]
                    ops:
                      - {type: aggregate, field: sold, funcs: [count]}
                  - name: enc
                    scope: population
                    type: encoding
                    keySets:
                      - keys: [seller_id]
                    targets:
                      - {stats: [count]}
            """;
        final FeaturePlan plan = compile(SOURCES, withEncoding(enc));
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        Assertions.assertTrue(hasCode(plan, "sequence.window.unbounded"), plan::describe);
        final List<FeaturePlan.Stage> seller = plan.getStages().stream().filter(s -> s.keys().equals(List.of("seller_id"))).toList();
        Assertions.assertEquals(1, seller.size(), plan::describe);
        Assertions.assertTrue(seller.get(0).blocks().containsAll(List.of("recent", "enc", "pinned")), plan::describe);
    }

    @Test
    public void testStageSchedulingKeepsKeyDependenciesInEarlierStages() {
        // a keyed stage whose key is derived from another keyed stage's column must come strictly after it,
        // even though a stage with the same key exists earlier
        final String enc = """
                  - name: enc_a
                    scope: population
                    type: encoding
                    keySets:
                      - keys: [category]
                    targets:
                      - {field: sold, stats: [count]}
                  - name: seller_bucket
                    scope: row
                    expr: "enc_a__category__sold__count > 5"
                  - name: enc_b
                    scope: population
                    type: encoding
                    keySets:
                      - keys: [seller_bucket]
                    targets:
                      - {field: sold, stats: [count]}
                  - name: enc_c
                    scope: population
                    type: encoding
                    keySets:
                      - keys: [category]
                    targets:
                      - {field: sold, stats: [mean]}
            """;
        final FeaturePlan plan = compile(SOURCES, withEncoding(enc));
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        final List<FeaturePlan.Stage> stages = plan.getStages();
        final int categoryStage = indexOfStage(stages, List.of("category"));
        final int bucketStage = indexOfStage(stages, List.of("seller_bucket"));
        Assertions.assertTrue(categoryStage >= 0 && bucketStage > categoryStage, plan::describe);
        // enc_c has no dependency on the bucket: it joins the first category stage
        Assertions.assertTrue(stages.get(categoryStage).blocks().containsAll(List.of("enc_a", "enc_c")), plan::describe);
        // the derived key is a row column evaluated in the category stage itself (read inside the DoFn)
        Assertions.assertTrue(stages.get(categoryStage).columnNames().contains("seller_bucket"), plan::describe);
        // the stage DAG follows the derived key through the row column to the category stage: one wave later
        Assertions.assertTrue(stages.get(bucketStage).dependsOn().contains(categoryStage), plan::describe);
        Assertions.assertEquals(plan.getWave(categoryStage) + 1, plan.getWave(bucketStage), plan::describe);
    }

    private static int indexOfStage(final List<FeaturePlan.Stage> stages, final List<String> keys) {
        for (final FeaturePlan.Stage s : stages) {
            if (s.kind() != FeaturePlan.StageKind.row && s.kind() != FeaturePlan.StageKind.fit && s.keys().equals(keys)) return s.index();
        }
        return -1;
    }

    @Test
    public void testAdditiveLatticeValidation() {
        final String cross = """
                  - name: enc
                    scope: population
                    type: encoding
                    keySets:
                      - keys: [seller_id, category]
                        structure: cross
                    targets:
                      - {expr: "sold >= 1", stats: [mean]}
                    shrinkage: {scale: logit}
            """;
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(cross)), "encoding.hierarchy.additive"));

        final String withMains = cross.replace("          - keys: [seller_id, category]", "          - {keys: [seller_id]}\n          - {keys: [category]}\n          - keys: [seller_id, category]");
        final FeaturePlan ok = compile(SOURCES, withEncoding(withMains));
        Assertions.assertFalse(ok.getDiagnostics().hasErrors(), ok::describe);
        final OutputColumn cell = column(ok, "enc__seller_id_category__e1__mean");
        Assertions.assertEquals("sequential", cell.getCoordinates().get("estimator"));
        Assertions.assertTrue(cell.getCoordinates().get("levels").contains("additive("));
        Assertions.assertEquals("logit", cell.getCoordinates().get("scale"));

        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(withMains.replace("shrinkage: {scale: logit}", "shrinkage: {priorWeight: 5}"))), "encoding.hierarchy.scale"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(withMains.replace("shrinkage: {scale: logit}", "shrinkage: {scale: logit, estimator: backoff}"))), "encoding.shrinkage.estimator"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(withMains.replace("shrinkage: {scale: logit}", "shrinkage: {scale: logit, estimator: joint}"))), "encoding.shrinkage.estimator"));
        final FeaturePlan vc = compile(SOURCES, withEncoding(withMains.replace("shrinkage: {scale: logit}", "shrinkage: {scale: logit, weights: varianceComponents}")));
        Assertions.assertFalse(vc.getDiagnostics().hasErrors(), vc::describe);
        Assertions.assertEquals("varianceComponents", column(vc, "enc__seller_id_category__e1__mean").getCoordinates().get("weights"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(withMains.replace("shrinkage: {scale: logit}", "shrinkage: {scale: logit, weights: heldOut}"))), "encoding.shrinkage.weights"));
    }

    /**
     * {@code structure: sequence}: the keys are a path declared most recent first, and the lattice is the chain of its
     * suffixes — the same levels an explicit {@code hierarchy} of the shortened key lists declares.
     */
    @Test
    public void testSequenceStructureDerivesTheSuffixChain() {
        final String path = """
                  - name: enc
                    scope: population
                    type: encoding
                    keySets:
                      - keys: [condition_grade, category, seller_id]
                        structure: sequence
                    targets:
                      - {expr: "sold >= 1", stats: [mean]}
                    shrinkage: {priorWeight: 2}
            """;
        final FeaturePlan derived = compile(SOURCES, withEncoding(path));
        Assertions.assertFalse(derived.getDiagnostics().hasErrors(), derived::describe);
        Assertions.assertTrue(hasCode(derived, "encoding.keySet.sequence"), derived::describe);
        final FeaturePlan declared = compile(SOURCES, withEncoding(path.replace("structure: sequence", "hierarchy: [[condition_grade, category], [condition_grade], []]")));
        Assertions.assertFalse(declared.getDiagnostics().hasErrors(), declared::describe);
        final OutputColumn a = column(derived, "enc__condition_grade_category_seller_id__e1__mean"), b = column(declared, "enc__condition_grade_category_seller_id__e1__mean");
        Assertions.assertEquals(b.getCoordinates().get("levels"), a.getCoordinates().get("levels"));
        // one keyed stage per level of the chain, the global one included
        for (final List<String> keys : List.of(List.of("condition_grade", "category", "seller_id"), List.of("condition_grade", "category"), List.of("condition_grade"), List.<String>of())) {
            Assertions.assertTrue(indexOfStage(derived.getStages(), keys) >= 0, () -> keys + " in\n" + derived.describe());
        }
        // a path has at least two steps (an error, not the info of the same code); an unknown structure names the accepted ones
        final FeaturePlan oneStep = compile(SOURCES, withEncoding(path.replace("[condition_grade, category, seller_id]", "[condition_grade]")));
        Assertions.assertTrue(hasCode(oneStep, "encoding.keySet.sequence"), oneStep::describe);
        Assertions.assertTrue(oneStep.getDiagnostics().hasErrors(), oneStep::describe);
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(path.replace("structure: sequence", "structure: tree"))), "encoding.keySet.structure"));
        // the derivation is not silenced by a hierarchy key that declares nothing (a bare `hierarchy:` is JSON null)
        for (final String empty : List.of("hierarchy:", "hierarchy: []")) {
            final FeaturePlan plan = compile(SOURCES, withEncoding(path.replace("structure: sequence", "structure: sequence\n" + " ".repeat(12) + empty)));
            Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
            Assertions.assertEquals(a.getCoordinates().get("levels"),
                    column(plan, "enc__condition_grade_category_seller_id__e1__mean").getCoordinates().get("levels"), plan::describe);
        }
        // without shrinkage the chain is never composed: the same code warns instead of informing
        final FeaturePlan unshrunk = compile(SOURCES, withEncoding(path.replace("\n" + " ".repeat(8) + "shrinkage: {priorWeight: 2}", "")));
        Assertions.assertFalse(unshrunk.getDiagnostics().hasErrors(), unshrunk::describe);
        Assertions.assertTrue(unshrunk.getDiagnostics().getMessages().stream()
                .anyMatch(m -> m.code().equals("encoding.keySet.sequence") && m.level() == Diagnostics.Level.warning), unshrunk::describe);
    }

    /**
     * {@code as} on a keySet and on a window names the segment the generated columns would otherwise derive: the keys
     * joined by {@code _} (a path of lag columns is a name of a hundred characters) and the window token — which a
     * filter does not have, so a filter-only window is {@code all} like the unconditional one and the two cannot stand
     * in one block without a name. Only names change: the coordinates, the hidden level statistics (shared between the
     * keySets of a block by their keys) and the stages are those of the unnamed declaration.
     */
    @Test
    public void testKeySetAndWindowAs() {
        final String path = """
                  - name: enc
                    scope: population
                    type: encoding
                    keySets:
                      - keys: [condition_grade, category, seller_id]
                        structure: sequence
                    targets:
                      - {expr: "sold >= 1", stats: [mean]}
                    shrinkage: {priorWeight: 2, output: [composed, deviations, effectiveN]}
            """;
        final FeaturePlan plain = compile(SOURCES, withEncoding(path));
        final FeaturePlan named = compile(SOURCES, withEncoding(path.replace("structure: sequence", "structure: sequence\n" + " ".repeat(12) + "as: gradePath")));
        Assertions.assertFalse(named.getDiagnostics().hasErrors(), named::describe);
        Assertions.assertNull(named.getColumn("enc__condition_grade_category_seller_id__e1__mean"), named::describe);
        for (final String stat : List.of("mean", "dev0", "dev2", "mean__neff")) {
            final OutputColumn a = column(plain, "enc__condition_grade_category_seller_id__e1__" + stat), b = column(named, "enc__gradePath__e1__" + stat);
            Assertions.assertEquals(a.getCoordinates(), b.getCoordinates(), stat);
            Assertions.assertEquals(a.getInputs(), b.getInputs(), stat);
        }
        Assertions.assertEquals("f_enc__gradePath__e1__mean", column(named, "enc__gradePath__e1__mean").getOutputName());
        Assertions.assertEquals(plain.getStages().size(), named.getStages().size());

        // the same keys twice — the raw path next to its shrunk chain — is a duplicate until one of them is named
        // (runtime indentation of the text block: keySets entries at 10, their keys at 12, the block's keys at 8)
        final String structure = " ".repeat(12) + "structure: sequence\n";
        final String twice = path.replace(" ".repeat(8) + "shrinkage: {priorWeight: 2, output: [composed, deviations, effectiveN]}\n", "")
                .replace(structure, structure + " ".repeat(12) + "shrinkage: {priorWeight: 2}\n" + " ".repeat(10) + "- keys: [condition_grade, category, seller_id]\n");
        Assertions.assertNotEquals(path, twice);
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(twice)), "column.duplicate"));
        final FeaturePlan both = compile(SOURCES, withEncoding(twice.replace(structure, structure + " ".repeat(12) + "as: gradePath\n")));
        Assertions.assertFalse(both.getDiagnostics().hasErrors(), both::describe);
        Assertions.assertEquals("compose", column(both, "enc__gradePath__e1__mean").getOperator());
        Assertions.assertEquals("encoding", column(both, "enc__condition_grade_category_seller_id__e1__mean").getOperator());

        // a window: the statistic over everything next to the one over the rows sharing the current row's grade
        final String filter = "- {filter: \"condition_grade = $self.condition_grade\"}";
        final FeaturePlan unnamed = compile(SOURCES, SPEC.replace("- {maxEvents: 5}", "- {}\n      " + filter));
        Assertions.assertTrue(hasCode(unnamed, "column.duplicate"), unnamed::describe);
        final FeaturePlan windows = compile(SOURCES, SPEC.replace("- {maxEvents: 5}", "- {}\n      " + filter.replace("}", ", as: sameGrade}")));
        Assertions.assertFalse(windows.getDiagnostics().hasErrors(), windows::describe);
        Assertions.assertNull(column(windows, "recent_all_sold_mean").getCoordinates().get("filter"));
        final OutputColumn sameGrade = column(windows, "recent_sameGrade_sold_mean");
        Assertions.assertEquals("sameGrade", sameGrade.getCoordinates().get("window"));
        // the pre-event equality filter is reduced to a partition key as it is unnamed: the name changes nothing else
        Assertions.assertEquals("seller_id,condition_grade", sameGrade.getCoordinates().get("stageKeys"));
        Assertions.assertNull(column(windows, "recent_all_sold_mean").getCoordinates().get("stageKeys"), "the unconditional window stays under the entity key");
        // a bounded window may be named too, and an encoding keySet's window takes the name into {window}
        Assertions.assertNotNull(compile(SOURCES, SPEC.replace("- {maxAge: P365D}", "- {maxAge: P365D, as: lastYear}")).getColumn("recent_lastYear_sold_mean"));
        final FeaturePlan encoding = compile(SOURCES, SPEC.replace("windows: [{maxAge: P365D}]", "windows: [{maxAge: P365D, as: lastYear}]"));
        Assertions.assertFalse(encoding.getDiagnostics().hasErrors(), encoding::describe);
        Assertions.assertNotNull(encoding.getColumn("enc__category__lastYear__e2__mean"), encoding::describe);

        // a name is a segment of a column name
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(path.replace("structure: sequence", "structure: sequence\n" + " ".repeat(12) + "as: grade-path"))), "encoding.keySet.as"));
        Assertions.assertTrue(hasCode(compile(SOURCES, SPEC.replace("- {maxEvents: 5}", "- {maxEvents: 5, as: 5events}")), "window.as"));
        Assertions.assertTrue(hasCode(compile(SOURCES, SPEC.replace("- {maxEvents: 5}", "- {maxEvents: 5, as: _hidden}")), "window.as"));

        // one name is one window: two windows of a block that select different rows must not share it — the hidden
        // level statistics of the block are shared by the name, wherever the emitted names happen not to collide
        Assertions.assertTrue(hasCode(compile(SOURCES, SPEC.replace("- {maxEvents: 5}", "- {maxEvents: 5, as: pool}\n      - {maxAge: P30D, as: pool}")), "window.as"));
        Assertions.assertFalse(hasCode(compile(SOURCES, SPEC.replace("- {maxEvents: 5}", "- {maxEvents: 5, as: pool}\n      - {maxEvents: 5, as: pool}")), "window.as"));
    }

    /**
     * A window's {@code as} is a display name, not part of the window: an {@code additive} lattice requires its
     * main-effect keySets to declare the same windows, and naming one of them declares the same window.
     */
    @Test
    public void testWindowNameIsNotPartOfTheWindow() {
        final String cross = """
                  - name: enc
                    scope: population
                    type: encoding
                    keySets:
                      - {keys: [seller_id], windows: [{maxAge: P365D}]}
                      - {keys: [category], windows: [{maxAge: P365D}]}
                      - keys: [seller_id, category]
                        structure: cross
                        windows: [{maxAge: P365D}]
                    targets:
                      - {expr: "sold >= 1", stats: [mean]}
                    shrinkage: {scale: logit}
            """;
        final FeaturePlan plain = compile(SOURCES, withEncoding(cross));
        Assertions.assertFalse(plain.getDiagnostics().hasErrors(), plain::describe);
        final String crossWindow = "structure: cross\n" + " ".repeat(12) + "windows: [{maxAge: P365D}]";
        Assertions.assertTrue(cross.contains(crossWindow), cross);
        final FeaturePlan named = compile(SOURCES, withEncoding(cross.replace(crossWindow, crossWindow.replace("P365D}", "P365D, as: lastYear}"))));
        Assertions.assertFalse(named.getDiagnostics().hasErrors(), named::describe);
        Assertions.assertNotNull(named.getColumn("enc__seller_id_category__lastYear__e1__mean"), named::describe);
        // a window that really differs is still rejected, named or not
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(cross.replace("- {keys: [category], windows: [{maxAge: P365D}]}",
                "- {keys: [category], windows: [{maxAge: P30D}]}"))), "encoding.hierarchy.additive"));
    }

    @Test
    public void testJointEstimatorCompilesToFitStageColumns() {
        final String joint = """
                  - name: enc
                    scope: population
                    type: encoding
                    fit: {mode: static, artifact: "gs://bucket/features"}
                    keySets:
                      - {keys: [seller_id]}
                      - {keys: [category]}
                      - keys: [seller_id, category]
                        structure: cross
                    targets:
                      - {expr: "sold >= 1", stats: [mean]}
                    shrinkage: {scale: logit, estimator: joint, weights: varianceComponents, output: [composed, deviations, effectiveN]}
            """;
        final FeaturePlan plan = compile(SOURCES, withEncoding(joint));
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        Assertions.assertTrue(hasCode(plan, "encoding.shrinkage.joint"), plan::describe);
        final OutputColumn cell = column(plan, "enc__seller_id_category__e1__mean");
        Assertions.assertEquals("joint", cell.getOperator());
        Assertions.assertEquals(FeatureSpec.Scope.population, cell.getScope());
        Assertions.assertTrue(cell.isFitted());
        Assertions.assertEquals("static", cell.getCoordinates().get("fit"));
        Assertions.assertEquals("joint", cell.getCoordinates().get("estimator"));
        Assertions.assertEquals("composed", cell.getCoordinates().get("kind"));
        Assertions.assertEquals("gaussian", cell.getCoordinates().get("family"));
        Assertions.assertEquals("logit", cell.getCoordinates().get("scale"));
        Assertions.assertEquals("enc__seller_id_category__e1", cell.getCoordinates().get("joint"));
        // additive → the main-effect key lists; the global level is the intercept
        Assertions.assertEquals("seller_id_category=seller_id,category;seller_id=seller_id;category=category;global=", cell.getCoordinates().get("jointLevels"));
        Assertions.assertTrue(cell.getInputs().containsAll(List.of("seller_id", "category")), cell.getInputs()::toString);
        Assertions.assertTrue(cell.getPastInputs().stream().anyMatch(p -> p.startsWith("enc__e")), cell.getPastInputs()::toString); // the desugared target expression
        Assertions.assertEquals("gs://bucket/features", cell.getCoordinates().get("artifactUri"));
        Assertions.assertEquals(OutputColumn.Status.staticSafe, cell.getStatus());
        // deviations = one per effect level, effectiveN = the leaf's posterior pseudo-count
        Assertions.assertEquals("seller_id_category", column(plan, "enc__seller_id_category__e1__dev0").getCoordinates().get("levelKeys"));
        Assertions.assertEquals("category", column(plan, "enc__seller_id_category__e1__dev2").getCoordinates().get("levelKeys"));
        Assertions.assertNull(plan.getColumn("enc__seller_id_category__e1__dev3"));
        Assertions.assertEquals("effectiveN", column(plan, "enc__seller_id_category__e1__mean__neff").getCoordinates().get("kind"));
        // the chain keySets of the block are joint too (a one-level ridge), and no hidden level statistics are registered
        Assertions.assertEquals("joint", column(plan, "enc__seller_id__e1__mean").getOperator());
        Assertions.assertEquals("seller_id=seller_id;global=", column(plan, "enc__seller_id__e1__mean").getCoordinates().get("jointLevels"));
        Assertions.assertNull(plan.getColumn("enc__seller_id__e1__n"), plan::describe);
        Assertions.assertNull(plan.getColumn("enc__global__e1__n"), plan::describe);
        // one fit stage holds every joint column and reads the target from an earlier stage
        final List<FeaturePlan.Stage> fits = plan.getStages().stream().filter(s -> s.kind() == FeaturePlan.StageKind.fit).toList();
        Assertions.assertEquals(1, fits.size(), plan::describe);
        Assertions.assertTrue(fits.get(0).columnNames().containsAll(List.of("enc__seller_id_category__e1__mean", "enc__seller_id__e1__mean", "enc__category__e1__mean")), plan::describe);
        Assertions.assertTrue(FeatureStages.engineConstraints(plan, false).isEmpty());
        Assertions.assertFalse(FeatureStages.artifactPaths(plan).isEmpty());
        Assertions.assertTrue(FeatureStages.artifactPaths(plan).get("enc__seller_id_category__e1").endsWith("enc__seller_id_category__e1.joint.avro"));

        // fold and forward are accepted; expanding is not (the joint solve needs the whole cell table)
        Assertions.assertFalse(compile(SOURCES, withEncoding(joint.replace("mode: static", "mode: fold, folds: 3, groupBy: seller"))).getDiagnostics().hasErrors());
        final FeaturePlan forward = compile(SOURCES, withEncoding(joint.replace("mode: static", "mode: forward, blocks: {bucket: month}")));
        Assertions.assertFalse(forward.getDiagnostics().hasErrors(), forward::describe);
        // a joint column declares weights: varianceComponents but estimates its pseudo-counts inside the solve: the
        // fit stage's row evaluator must not request the λ estimate (under forward, that was a scan of the series)
        final List<OutputColumn> fitColumns = forward.getStages().stream().filter(s -> s.kind() == FeaturePlan.StageKind.fit)
                .flatMap(s -> s.columnNames().stream()).map(forward::getColumn).toList();
        Assertions.assertTrue(fitColumns.stream().anyMatch(c -> "varianceComponents".equals(c.getCoordinates().get("weights"))), forward::describe);
        Assertions.assertFalse(new RowEvaluator(fitColumns).needsVarianceComponents(), forward::describe);
        Assertions.assertEquals("month", column(forward, "enc__seller_id_category__e1__mean").getCoordinates().get("blockBucket"));
        final FeaturePlan expanding = compile(SOURCES, withEncoding(joint.replace("        fit: {mode: static, artifact: \"gs://bucket/features\"}\n", "")));
        Assertions.assertTrue(hasCode(expanding, "encoding.shrinkage.estimator"), expanding::describe);
        // a distribution never reaches the joint solve: the statistic is expanding-only, one diagnostic says so
        final FeaturePlan jointDistribution = compile(SOURCES, withEncoding(joint
                .replace("- {expr: \"sold >= 1\", stats: [mean]}", "- {field: condition_grade, stats: [distribution]}")));
        Assertions.assertTrue(hasCode(jointDistribution, "encoding.stat.static"), jointDistribution::describe);
        Assertions.assertTrue(jointDistribution.getDiagnostics().getMessages().stream().filter(m -> m.level() == Diagnostics.Level.error).allMatch(m -> "encoding.stat.static".equals(m.code())),
                "no second, contradicting diagnostic (the old 'use backoff' hint led to this same error): " + jointDistribution.describe());

        // the fit a joint column reads is identified by the keys, the window and the target — a keySet's `as` renames
        // its columns, not the fit. The same keys twice share one solve: right with the same lattice and shrinkage,
        // and rejected when they differ (the second keySet's columns would be filled from the first's model)
        final String cellKeySet = " ".repeat(10) + "- keys: [seller_id, category]\n" + " ".repeat(12) + "structure: cross\n";
        Assertions.assertTrue(joint.contains(cellKeySet), joint);
        final FeaturePlan shared = compile(SOURCES, withEncoding(joint.replace(cellKeySet, cellKeySet + cellKeySet + " ".repeat(12) + "as: cell2\n")));
        Assertions.assertFalse(shared.getDiagnostics().hasErrors(), shared::describe);
        Assertions.assertEquals("enc__seller_id_category__e1", column(shared, "enc__cell2__e1__mean").getCoordinates().get("joint"));
        final FeaturePlan conflicting = compile(SOURCES, withEncoding(joint.replace(cellKeySet,
                cellKeySet + cellKeySet + " ".repeat(12) + "as: cell2\n" + " ".repeat(12) + "shrinkage: {scale: logit, estimator: joint, priorWeight: 50}\n")));
        Assertions.assertTrue(conflicting.getDiagnostics().getMessages().stream()
                .anyMatch(m -> "encoding.shrinkage.joint".equals(m.code()) && m.level() == Diagnostics.Level.error), conflicting::describe);
    }

    @Test
    public void testShrinkageFamilies() {
        final String base = """
                  - name: enc
                    scope: population
                    type: encoding
                    keySets:
                      - keys: [seller_id]
                    targets:
                      - {expr: "sold >= 1", stats: [mean, rate]}
                    shrinkage: {priorWeight: 2}
            """;
        // derived: mean → gaussian, rate → betaBinomial; identical arithmetic, recorded in the coordinates
        final FeaturePlan derived = compile(SOURCES, withEncoding(base));
        Assertions.assertFalse(derived.getDiagnostics().hasErrors(), derived::describe);
        Assertions.assertEquals("gaussian", column(derived, "enc__seller_id__e1__mean").getCoordinates().get("family"));
        Assertions.assertEquals("betaBinomial", column(derived, "enc__seller_id__e1__rate").getCoordinates().get("family"));
        // declared conjugate families need the identity scale; gaussian on logit is the approximation of rule 7
        Assertions.assertEquals("gammaPoisson", column(compile(SOURCES, withEncoding(base.replace("{priorWeight: 2}", "{priorWeight: 2, family: gammaPoisson}"))), "enc__seller_id__e1__mean").getCoordinates().get("family"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(base.replace("{priorWeight: 2}", "{priorWeight: 2, family: betaBinomial, scale: logit}"))), "encoding.shrinkage.family.scale"));
        Assertions.assertFalse(compile(SOURCES, withEncoding(base.replace("{priorWeight: 2}", "{priorWeight: 2, family: gaussian, scale: logit}"))).getDiagnostics().hasErrors());
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(base.replace("{priorWeight: 2}", "{priorWeight: 2, family: dirichletMultinomial}"))), "encoding.shrinkage.family.stat"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(base.replace("{priorWeight: 2}", "{priorWeight: 2, family: poisson}"))), "encoding.shrinkage.family"));
        // a derived family on logit / log is the Gaussian approximation (rule 7), not an error: rate + logit compiled before families existed
        final FeaturePlan derivedLogit = compile(SOURCES, withEncoding(base.replace("{priorWeight: 2}", "{priorWeight: 2, scale: logit}")));
        Assertions.assertFalse(derivedLogit.getDiagnostics().hasErrors(), derivedLogit::describe);
        Assertions.assertEquals("gaussian", column(derivedLogit, "enc__seller_id__e1__rate").getCoordinates().get("family"));
        Assertions.assertEquals("compose", column(derivedLogit, "enc__seller_id__e1__rate").getOperator());

        // distribution: Dirichlet-Multinomial shrinkage along the chain, a map-valued composed column over hidden per-level shares
        final String distribution = base.replace("- {expr: \"sold >= 1\", stats: [mean, rate]}", "- {field: condition_grade, stats: [distribution]}");
        final FeaturePlan dist = compile(SOURCES, withEncoding(distribution.replace("{priorWeight: 2}", "{priorWeight: 2, output: [composed, deviations, effectiveN]}")));
        Assertions.assertFalse(dist.getDiagnostics().hasErrors(), dist::describe);
        final OutputColumn composed = column(dist, "enc__seller_id__condition_grade__distribution");
        Assertions.assertEquals("compose", composed.getOperator());
        Assertions.assertEquals(Schema.Type.map, composed.getFieldType().getType());
        Assertions.assertEquals("dirichletMultinomial", composed.getCoordinates().get("family"));
        Assertions.assertTrue(composed.getInputs().containsAll(List.of("enc__seller_id__condition_grade__n", "enc__seller_id__condition_grade__dist", "enc__global__condition_grade__dist")), composed.getInputs()::toString);
        final OutputColumn shares = column(dist, "enc__seller_id__condition_grade__dist");
        Assertions.assertTrue(shares.isIntermediate());
        Assertions.assertEquals("distribution", shares.getCoordinates().get("stat"));
        Assertions.assertEquals(Schema.Type.map, shares.getFieldType().getType());
        Assertions.assertNull(dist.getColumn("enc__seller_id__condition_grade__sum"), dist::describe);
        Assertions.assertNull(dist.getColumn("enc__seller_id__condition_grade__dev0"), "deviations are undefined for a distribution");
        // values: the map column becomes an intermediate and one FLOAT64 share column per listed category reads it
        final FeaturePlan flat = compile(SOURCES, withEncoding(distribution.replace("stats: [distribution]}", "stats: [distribution], values: [good, fair]}")));
        Assertions.assertFalse(flat.getDiagnostics().hasErrors(), flat::describe);
        Assertions.assertTrue(column(flat, "enc__seller_id__condition_grade__distribution").isIntermediate(), flat::describe);
        final OutputColumn good = column(flat, "enc__seller_id__condition_grade__distribution_good");
        Assertions.assertEquals("mapValue", good.getOperator());
        Assertions.assertEquals(Schema.FieldType.FLOAT64.getType(), good.getFieldType().getType());
        Assertions.assertEquals("good", good.getCoordinates().get("value"));
        Assertions.assertEquals(List.of("enc__seller_id__condition_grade__distribution"), List.copyOf(good.getInputs()));
        Assertions.assertTrue(good.isFitted());
        Assertions.assertEquals(composed.getStatus(), good.getStatus());
        Assertions.assertNotNull(column(flat, "enc__seller_id__condition_grade__distribution_fair"));
        Assertions.assertTrue(flat.getEmittedColumns().stream().noneMatch(c -> c.getCanonicalName().equals("enc__seller_id__condition_grade__distribution")), flat::describe);
        // values on a target without a distribution stat is an error
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(base.replace("stats: [mean, rate]}", "stats: [mean, rate], values: [good]}"))), "encoding.target.values"));
        Assertions.assertTrue(hasCode(dist, "encoding.shrinkage.output"), dist::describe);
        column(dist, "enc__seller_id__condition_grade__distribution__neff");
        Assertions.assertTrue(FeatureStages.engineConstraints(dist, false).isEmpty(), FeatureStages.engineConstraints(dist, false)::toString);
        // no additive / cross lattice, no variance-components λ (warning), and expanding only
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(distribution.replace("          - keys: [seller_id]\n", "          - {keys: [seller_id]}\n          - {keys: [category]}\n          - {keys: [seller_id, category], structure: cross}\n").replace("{priorWeight: 2}", "{priorWeight: 2, scale: identity}"))), "encoding.shrinkage.family.lattice"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(distribution.replace("{priorWeight: 2}", "{priorWeight: 2, weights: varianceComponents}"))), "encoding.shrinkage.weights.distribution"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(distribution.replace("        keySets:", "        fit: {mode: static}\n        keySets:"))), "encoding.stat.static"));
        // without shrinkage the statistic stays the raw per-key distribution of the population stage
        final FeaturePlan raw = compile(SOURCES, withEncoding(distribution.replace("        shrinkage: {priorWeight: 2}\n", "")));
        Assertions.assertEquals("encoding", column(raw, "enc__seller_id__condition_grade__distribution").getOperator());
        // on logit / log a derived distribution has no Gaussian counterpart: emitted unshrunk with a warning, no hidden shares
        final FeaturePlan logitDist = compile(SOURCES, withEncoding(distribution.replace("{priorWeight: 2}", "{priorWeight: 2, scale: logit}")));
        Assertions.assertFalse(logitDist.getDiagnostics().hasErrors(), logitDist::describe);
        Assertions.assertTrue(hasCode(logitDist, "encoding.shrinkage.family.scale"), logitDist::describe);
        Assertions.assertEquals("encoding", column(logitDist, "enc__seller_id__condition_grade__distribution").getOperator());
        Assertions.assertNull(logitDist.getColumn("enc__global__condition_grade__dist"), logitDist::describe);
        // next to a scalar statistic of the same target the distribution keeps priorWeight: its compose column declares
        // fixed weights, so it never resolves the scalar's variance-components λ through the shared hidden n columns
        final FeaturePlan mixed = compile(SOURCES, withEncoding(base
                .replace("- {expr: \"sold >= 1\", stats: [mean, rate]}", "- {field: condition_grade, stats: [mean, distribution]}")
                .replace("{priorWeight: 2}", "{priorWeight: 2, weights: varianceComponents}")));
        Assertions.assertFalse(mixed.getDiagnostics().hasErrors(), mixed::describe);
        Assertions.assertEquals("varianceComponents", column(mixed, "enc__seller_id__condition_grade__mean").getCoordinates().get("weights"));
        Assertions.assertEquals("fixed", column(mixed, "enc__seller_id__condition_grade__distribution").getCoordinates().get("weights"));
        Assertions.assertEquals(column(mixed, "enc__seller_id__condition_grade__mean").getInputs().stream().filter(i -> i.endsWith("__n")).toList(),
                column(mixed, "enc__seller_id__condition_grade__distribution").getInputs().stream().filter(i -> i.endsWith("__n")).toList(), "the levels share their n columns");
    }

    @Test
    public void testLegacySmoothingIsShrinkageSugar() {
        final String legacy = """
                  - name: enc
                    scope: population
                    type: encoding
                    keySets:
                      - keys: [seller_id]
                    targets:
                      - {expr: "sold >= 1", stats: [mean]}
                    smoothing: {type: bayesian, priorWeight: 10}
            """;
        final FeaturePlan plan = compile(SOURCES, withEncoding(legacy));
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        final OutputColumn composed = column(plan, "enc__seller_id__e1__mean");
        Assertions.assertEquals("compose", composed.getOperator());
        Assertions.assertEquals("10.0", composed.getCoordinates().get("priorWeight"));
        Assertions.assertEquals("seller_id,enc__seller_id__e1__n,enc__seller_id__e1__sum;global,enc__global__e1__n,enc__global__e1__sum",
                composed.getCoordinates().get("levels"));
    }

    @Test
    public void testMaxFeaturesGuard() {
        final String spec = SPEC.replace("maxFeatures: 50", "maxFeatures: 2");
        Assertions.assertTrue(hasCode(compile(SOURCES, spec), "encoding.maxFeatures"));
    }

    @Test
    public void testStaticFitExpansion() {
        final String block = """
                  - name: enc
                    scope: population
                    type: encoding
                    fit: {mode: static, artifact: {uri: "gs://bucket/features", refit: true}}
                    keySets:
                      - keys: [seller_id]
                        windows: [{maxAge: P365D}]
                    targets:
                      - {stats: [count]}
                      - {expr: "sold >= 1", stats: [mean, std]}
                    shrinkage: {priorWeight: 5}
            """;
        final FeaturePlan plan = compile(SOURCES, withEncoding(block));
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        Assertions.assertTrue(hasCode(plan, "fit.mode.static"));
        Assertions.assertTrue(hasCode(plan, "fit.mode.static.windows"));

        // hidden fitted statistics (windows ignored in static mode) incl. Σy², applied by a fit stage
        for (final String hidden : List.of("enc__seller_id__e2__n", "enc__seller_id__e2__sum", "enc__seller_id__e2__sumsq", "enc__global__e2__n")) {
            final OutputColumn c = column(plan, hidden);
            Assertions.assertTrue(c.isIntermediate(), hidden);
            Assertions.assertEquals("static", c.getCoordinates().get("fit"));
            Assertions.assertEquals("gs://bucket/features", c.getCoordinates().get("artifactUri"));
            Assertions.assertEquals("true", c.getCoordinates().get("refit"));
            Assertions.assertEquals(OutputColumn.Status.staticSafe, c.getStatus());
        }
        Assertions.assertEquals("fitStat", column(plan, "enc__seller_id__count").getOperator());
        Assertions.assertEquals("fitStat", column(plan, "enc__seller_id__e2__std").getOperator());
        Assertions.assertEquals("compose", column(plan, "enc__seller_id__e2__mean").getOperator());
        Assertions.assertTrue(plan.getStages().stream().anyMatch(s -> s.kind() == FeaturePlan.StageKind.fit), plan::describe);
        Assertions.assertTrue(plan.getStages().stream().noneMatch(s -> s.kind() == FeaturePlan.StageKind.population), plan::describe);

        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(block.replace("mean, std", "distribution"))), "encoding.stat.static"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(block.replace("mean, std", "distribution").replace("mode: static", "mode: fold"))), "encoding.stat.static"));
    }

    private static final String FM_BLOCK = """
                  - name: fm
                    scope: population
                    type: factorization
                    variant: fwfm
                    fields: [seller_id, category, condition_grade]
                    latentDim: 4
                    task: {expr: "sold >= 1", offset: market}
                    fit: {artifact: "gs://bucket/features", window: "trailing(P3Y)"}
                    outputs:
                      - {pair: [seller_id, category], as: fm_seller_category}
                      - {embedding: category, as: cat_emb, dims: 2}
                      - {sum: true, as: fm_linear}
            """;

    @Test
    public void testFactorizationExpansion() {
        final FeaturePlan plan = compile(SOURCES, withEncoding(FM_BLOCK));
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        Assertions.assertTrue(hasCode(plan, "factorization.fit.window"));
        for (final String name : List.of("fm_seller_category", "cat_emb_0", "cat_emb_1", "fm_linear")) {
            final OutputColumn c = column(plan, name);
            Assertions.assertEquals("fm", c.getOperator(), name);
            Assertions.assertEquals("static", c.getCoordinates().get("fit"));
            Assertions.assertEquals("fwfm", c.getCoordinates().get("variant"));
            Assertions.assertEquals("seller_id,category,condition_grade", c.getCoordinates().get("fields"));
            Assertions.assertEquals("gs://bucket/features", c.getCoordinates().get("artifactUri"));
            Assertions.assertTrue(c.getDerivedFrom().contains("outcome"), name);
            Assertions.assertTrue(c.getDerivedFrom().contains("market"), name);
            Assertions.assertEquals(OutputColumn.Status.staticSafe, c.getStatus());
            Assertions.assertFalse(c.isIntermediate());
        }
        Assertions.assertNull(plan.getColumn("cat_emb_2"));
        Assertions.assertEquals("1", column(plan, "cat_emb_1").getCoordinates().get("dim"));
        Assertions.assertTrue(plan.getStages().stream().anyMatch(s -> s.kind() == FeaturePlan.StageKind.fit), plan::describe);

        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(FM_BLOCK.replace("variant: fwfm", "variant: bayesian"))), "factorization.variant"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(FM_BLOCK.replace("fit: {artifact", "fit: {mode: expanding, artifact"))), "factorization.fit.mode"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(FM_BLOCK.replace("pair: [seller_id, category]", "pair: [seller_id, quantity]"))), "factorization.outputs"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(FM_BLOCK.replace("fields: [seller_id, category, condition_grade]", "fields: [seller_id, start_price]"))), "factorization.fields"));
    }

    private static final String DISCRETIZE_BLOCK = """
                  - name: price_bin
                    scope: population
                    type: discretize
                    input: start_price
                    bins: 4
                    fit: {artifact: "gs://bucket/features", window: "trailing(P3Y)"}
                  - name: by_bin
                    scope: population
                    type: encoding
                    keySets:
                      - keys: [price_bin]
                    targets:
                      - {stats: [count]}
                      - {field: sold, stats: [mean]}
            """;

    @Test
    public void testDiscretizeExpansion() {
        final FeaturePlan plan = compile(SOURCES, withEncoding(DISCRETIZE_BLOCK));
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        Assertions.assertTrue(hasCode(plan, "discretize.fit.window"));
        final OutputColumn c = column(plan, "price_bin");
        Assertions.assertEquals("discretize", c.getOperator());
        Assertions.assertEquals(FeatureSpec.Scope.population, c.getScope());
        Assertions.assertEquals(Schema.FieldType.INT64.getType(), c.getFieldType().getType());
        Assertions.assertEquals("static", c.getCoordinates().get("fit"));
        Assertions.assertEquals("quantile", c.getCoordinates().get("method"));
        Assertions.assertEquals("4", c.getCoordinates().get("bins"));
        Assertions.assertEquals("start_price", c.getCoordinates().get("field"));
        Assertions.assertEquals("gs://bucket/features", c.getCoordinates().get("artifactUri"));
        Assertions.assertEquals(OutputColumn.Status.staticSafe, c.getStatus());
        Assertions.assertFalse(c.isIntermediate());
        // the fitted bins key an encoding: the fit stage runs before the keyed stage that reads the bins
        final FeaturePlan.Stage fit = plan.getStages().stream().filter(s -> s.kind() == FeaturePlan.StageKind.fit).findFirst().orElseThrow();
        Assertions.assertTrue(fit.columnNames().contains("price_bin"), plan::describe);
        final FeaturePlan.Stage keyed = plan.getStages().stream().filter(s -> s.keys().equals(List.of("price_bin"))).findFirst().orElseThrow();
        Assertions.assertTrue(fit.index() < keyed.index(), plan::describe);
        Assertions.assertNotNull(column(plan, "by_bin__price_bin__count"));

        // (inserted lines carry the text block's 8-space property indentation; withEncoding strips 4)
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(DISCRETIZE_BLOCK.replace("bins: 4", "bins: 4\n        method: tree\n        target: sold"))), "discretize.method"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(DISCRETIZE_BLOCK.replace("input: start_price", "input: condition_grade"))), "discretize.input"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(DISCRETIZE_BLOCK.replace("bins: 4", "bins: 1"))), "discretize.bins"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(DISCRETIZE_BLOCK.replace("fit: {artifact", "fit: {mode: expanding, artifact"))), "discretize.fit.mode"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(DISCRETIZE_BLOCK.replace("bins: 4", "bins: 4\n        target: sold"))), "discretize.target"));
    }

    private static final String QUANTILE_TRANSFORM_BLOCK = """
                  - name: price_q
                    scope: population
                    type: quantileTransform
                    input: start_price
                    bins: 20
                    distribution: normal
                    fit: {artifact: "gs://bucket/features", cadence: daily}
                  - name: price_q_sq
                    scope: row
                    expr: "price_q * price_q"
            """;

    @Test
    public void testQuantileTransformExpansion() {
        final FeaturePlan plan = compile(SOURCES, withEncoding(QUANTILE_TRANSFORM_BLOCK));
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        Assertions.assertTrue(hasCode(plan, "quantileTransform.fit.cadence"));
        final OutputColumn c = column(plan, "price_q");
        Assertions.assertEquals("quantileTransform", c.getOperator());
        Assertions.assertEquals(FeatureSpec.Scope.population, c.getScope());
        Assertions.assertEquals(Schema.FieldType.FLOAT64.getType(), c.getFieldType().getType());
        Assertions.assertEquals("static", c.getCoordinates().get("fit"));
        Assertions.assertEquals("start_price", c.getCoordinates().get("field"));
        Assertions.assertEquals("20", c.getCoordinates().get("bins"));
        Assertions.assertEquals("normal", c.getCoordinates().get("distribution"));
        Assertions.assertEquals("gs://bucket/features", c.getCoordinates().get("artifactUri"));
        Assertions.assertEquals(OutputColumn.Status.staticSafe, c.getStatus());
        Assertions.assertTrue(c.isFitted());
        // a row expression over the fitted column is evaluated in the fit stage
        final FeaturePlan.Stage fit = plan.getStages().stream().filter(s -> s.kind() == FeaturePlan.StageKind.fit).findFirst().orElseThrow();
        Assertions.assertTrue(fit.columnNames().containsAll(List.of("price_q", "price_q_sq")), plan::describe);
        Assertions.assertTrue(FeatureStages.artifactPaths(plan).get("price_q").endsWith("price_q.quantiles.json"));
        // defaults: 100 intervals, uniform
        final OutputColumn defaults = column(compile(SOURCES, withEncoding(QUANTILE_TRANSFORM_BLOCK.replace("        bins: 20\n        distribution: normal\n", ""))), "price_q");
        Assertions.assertEquals("100", defaults.getCoordinates().get("bins"));
        Assertions.assertEquals("uniform", defaults.getCoordinates().get("distribution"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(QUANTILE_TRANSFORM_BLOCK.replace("input: start_price", "input: condition_grade"))), "quantileTransform.input"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(QUANTILE_TRANSFORM_BLOCK.replace("bins: 20", "bins: 1"))), "quantileTransform.bins"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(QUANTILE_TRANSFORM_BLOCK.replace("distribution: normal", "distribution: logistic"))), "quantileTransform.distribution"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(QUANTILE_TRANSFORM_BLOCK.replace("fit: {artifact", "fit: {mode: expanding, artifact"))), "quantileTransform.fit.mode"));
        // clip: the probability clamp of the normal score — absent by default (the engine's 1e-6), a coordinate when declared
        Assertions.assertNull(c.getCoordinates().get("clip"));
        final FeaturePlan clipped = compile(SOURCES, withEncoding(QUANTILE_TRANSFORM_BLOCK.replace("distribution: normal", "distribution: normal\n        clip: 0.001")));
        Assertions.assertFalse(clipped.getDiagnostics().hasErrors(), clipped::describe);
        Assertions.assertEquals("0.001", column(clipped, "price_q").getCoordinates().get("clip"));
        Assertions.assertNotEquals(plan.getHash(), clipped.getHash(), "clip changes the fitted transform, so the artifact directory");
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(QUANTILE_TRANSFORM_BLOCK.replace("distribution: normal", "distribution: normal\n        clip: 0.5"))), "quantileTransform.clip"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(QUANTILE_TRANSFORM_BLOCK.replace("distribution: normal", "distribution: normal\n        clip: 0"))), "quantileTransform.clip"));
        final FeaturePlan notANumber = compile(SOURCES, withEncoding(QUANTILE_TRANSFORM_BLOCK.replace("distribution: normal", "distribution: normal\n        clip: tiny")));
        Assertions.assertTrue(hasCode(notANumber, "clip.invalid"), "not a number is a diagnostic, not a crash");
        Assertions.assertTrue(notANumber.describe().contains("tiny"), "the diagnostic names the rejected text");
        // clip with uniform is a warning and leaves no coordinate
        final FeaturePlan uniformClip = compile(SOURCES, withEncoding(QUANTILE_TRANSFORM_BLOCK.replace("distribution: normal", "distribution: uniform\n        clip: 0.001")));
        Assertions.assertFalse(uniformClip.getDiagnostics().hasErrors(), uniformClip::describe);
        Assertions.assertTrue(hasCode(uniformClip, "quantileTransform.clip"));
        Assertions.assertNull(column(uniformClip, "price_q").getCoordinates().get("clip"));
    }

    /** The sources contract accepts {@code array<element type>}; the svd array path is reachable through it. */
    @Test
    public void testSvdArrayInputFromContract() {
        // the text block strips its closing delimiter's 12 spaces: the contract fields run at 6 spaces
        final String sources = SOURCES.replace("      - {name: condition_grade, type: string}\n",
                "      - {name: condition_grade, type: string}\n      - {name: embedding, type: array<float64>, kind: attribute}\n");
        Assertions.assertTrue(sources.contains("embedding"));
        final String block = """
                  - name: emb_pc
                    scope: population
                    type: svd
                    input: embedding
                    rank: 2
                    fit: {artifact: "gs://bucket/features"}
            """;
        final String spec = withEncoding(block).replace(", condition_grade], from: listings}", ", condition_grade, embedding], from: listings}");
        final FeaturePlan plan = compile(sources, spec);
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        Assertions.assertTrue(hasCode(plan, "svd.rank"), "the rank cannot be checked against the array length at compile time");
        final OutputColumn c = column(plan, "emb_pc_0");
        Assertions.assertEquals("svd", c.getOperator());
        Assertions.assertEquals("embedding", c.getCoordinates().get("arrayField"));
        Assertions.assertNull(c.getCoordinates().get("fields"));
        Assertions.assertEquals(Schema.Type.array, plan.getInputFields().get("embedding").getType().getType());
        Assertions.assertEquals(Schema.Type.float64, plan.getInputFields().get("embedding").getType().getArrayValueType().getType());
        // an array has no named dimensions: the per-input residual needs 'inputs', the residual norm does not
        Assertions.assertTrue(spec.contains("\n    rank: 2\n"), spec);
        Assertions.assertTrue(hasCode(compile(sources, spec.replace("    rank: 2\n", "    rank: 2\n    outputs: [residual]\n")), "svd.outputs"));
        final FeaturePlan norm = compile(sources, spec.replace("    rank: 2\n", "    rank: 2\n    outputs: [scores, residualNorm]\n"));
        Assertions.assertFalse(norm.getDiagnostics().hasErrors(), norm::describe);
        Assertions.assertEquals("norm", column(norm, "emb_pc_residnorm").getCoordinates().get("residual"));
        // an array input needs rank; a non-numeric array is not a vector
        Assertions.assertTrue(hasCode(compile(sources, spec.replace("        rank: 2\n", "")), "svd.rank"));
        Assertions.assertTrue(hasCode(compile(sources.replace("array<float64>", "array<string>"), spec), "svd.input"));
        // the type syntax: a bare 'array' has no element type, nested arrays are not accepted, spacing is lenient
        Assertions.assertTrue(hasCode(compile(sources.replace("array<float64>", "array"), spec), "sources.fields.type"));
        Assertions.assertTrue(hasCode(compile(sources.replace("array<float64>", "array<array<float64>>"), spec), "sources.fields.type"));
        Assertions.assertTrue(hasCode(compile(sources.replace("array<float64>", "array<vector>"), spec), "sources.fields.type"));
        Assertions.assertFalse(compile(sources.replace("array<float64>", "\"Array< double >\""), spec).getDiagnostics().hasErrors());
        Assertions.assertTrue(hasCode(compile(sources.replace("array<float64>", "array<struct>"), spec), "sources.fields.type"), "a non-primitive element is not a vector");
        // the manifest / report spell the type as the contract does, so it round-trips
        Assertions.assertEquals("array<float64>", FeaturePlan.typeName(plan.getInputFields().get("embedding").getType()));
        Assertions.assertEquals("float64", FeaturePlan.typeName(Schema.FieldType.FLOAT64));
        // against an input schema the array-ness of the contract type is checked: a shape mismatch reads null for every row
        final JsonObject sourcesJson = Config.convertConfigJson(sources, Config.Format.yaml);
        final JsonObject specJson = Config.convertConfigJson(spec, Config.Format.yaml);
        final List<Schema.Field> scalar = new java.util.ArrayList<>(inputFields(true));
        scalar.add(Schema.Field.of("embedding", Schema.FieldType.FLOAT64));
        Assertions.assertTrue(hasCode(FeaturePlanCompiler.compile(sourcesJson, specJson, scalar), "lineage.type.mismatch"));
        final List<Schema.Field> repeated = new java.util.ArrayList<>(inputFields(true));
        repeated.add(Schema.Field.of("embedding", Schema.FieldType.array(Schema.FieldType.FLOAT64)));
        Assertions.assertFalse(hasCode(FeaturePlanCompiler.compile(sourcesJson, specJson, repeated), "lineage.type.mismatch"));
    }

    /**
     * Row {@code type: vector}: one column per readout (polyfit: one per coefficient), the steps and the readout in
     * the coordinates, the array field's availability and lineage inherited, every parameter validated.
     */
    @Test
    public void testVectorExpansion() {
        // bid_path: the bids observed up to ten minutes before the session (market information, like current_bid_t10)
        final String sources = SOURCES.replace("      - {name: snapshot_time, type: timestamp,",
                "      - {name: bid_path, type: array<float64>, availableAt: \"event_time - PT10M\", observedAtField: snapshot_time, kind: market}\n      - {name: snapshot_time, type: timestamp,");
        Assertions.assertTrue(sources.contains("bid_path"), "the contract line must match the text block's runtime indentation");
        final String block = """
                  - name: bids
                    scope: row
                    type: vector
                    input: bid_path
                    slice: {from: -4}
                    diff: 1
                    normalize: mean
                    position: unit
                    funcs: [length, mean, argmax, slope, polyfit]
                    degree: 3
                  - name: bid_momentum
                    scope: row
                    expr: "bids_slope / bids_mean"
            """;
        final String spec = withEncoding(block).replace("- {fields: [current_bid_t10], from: price_snapshots}", "- {fields: [current_bid_t10, bid_path], from: price_snapshots}");
        final FeaturePlan plan = compile(sources, spec);
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);

        final OutputColumn slope = column(plan, "bids_slope");
        Assertions.assertEquals("vector", slope.getOperator());
        Assertions.assertEquals(Schema.Type.float64, slope.getFieldType().getType());
        Assertions.assertEquals("slope", slope.getCoordinates().get("func"));
        Assertions.assertEquals("-4", slope.getCoordinates().get("sliceFrom"));
        Assertions.assertNull(slope.getCoordinates().get("sliceTo"));
        Assertions.assertEquals("1", slope.getCoordinates().get("diff"));
        Assertions.assertEquals("mean", slope.getCoordinates().get("normalize"));
        Assertions.assertEquals("unit", slope.getCoordinates().get("position"));
        Assertions.assertEquals(List.of("bid_path"), List.copyOf(slope.getInputs()));
        // the readouts inherit the array field: market information known 10 minutes (+ 1 minute ingestion) before the event
        Assertions.assertEquals(OutputColumn.Status.staticSafe, slope.getStatus());
        Assertions.assertEquals(column(plan, "relative_current_bid_t10_rank").getAvailableAt().toString(), slope.getAvailableAt().toString());
        Assertions.assertTrue(slope.getDerivedFrom().contains("market"));

        Assertions.assertEquals(Schema.Type.int64, column(plan, "bids_length").getFieldType().getType());
        Assertions.assertEquals(Schema.Type.int64, column(plan, "bids_argmax").getFieldType().getType());
        Assertions.assertNull(column(plan, "bids_mean").getCoordinates().get("position"), "position only shapes slope / polyfit");
        for (int k = 0; k <= 3; k++) {
            final OutputColumn poly = column(plan, "bids_poly" + k);
            Assertions.assertEquals("polyfit", poly.getCoordinates().get("func"));
            Assertions.assertEquals("3", poly.getCoordinates().get("degree"));
            Assertions.assertEquals(Integer.toString(k), poly.getCoordinates().get("coefficient"));
        }
        Assertions.assertNull(plan.getColumn("bids_poly4"));
        Assertions.assertNull(plan.getColumn("bids_polyfit"));
        // the readouts are ordinary columns: an expr composes them, and they are part of the plan hash
        Assertions.assertTrue(column(plan, "bid_momentum").getInputs().contains("bids_slope"));
        // (withEncoding strips 4 more spaces: the block's parameters run at 4)
        Assertions.assertTrue(spec.contains("\n    diff: 1\n"), spec);
        Assertions.assertNotEquals(plan.getHash(), compile(sources, spec.replace("    diff: 1\n", "")).getHash());
        // every readout the catalog lists is served by VectorOps
        for (final String func : OperatorCatalog.VECTOR_FUNCS) {
            if (!"polyfit".equals(func) && !"vector".equals(func)) Assertions.assertNotNull(VectorOps.read(func, new double[]{1, 3, 2}, VectorOps.positions(3, false)), func);
        }

        // polyfit coefficients can be picked (the level is often not a feature), the stepped vector can be emitted as an
        // array at a fixed length (resample / pad) and fed to an array svd, and the parameters are validated
        Assertions.assertTrue(block.contains("funcs: [length, mean, argmax, slope, polyfit]\n        degree: 3"), "the text block runs at 8 spaces");
        final String picked = block.replace("funcs: [length, mean, argmax, slope, polyfit]\n        degree: 3", "funcs: [polyfit, vector]\n        degree: 3\n        coefficients: [1, 2]\n        resample: 12\n        pad: {length: 12, mode: edge, side: start}")
                .replace("      - name: bid_momentum\n        scope: row\n        expr: \"bids_slope / bids_mean\"\n", "")
                + "      - name: path_pc\n        scope: population\n        type: svd\n        input: bids_vector\n        rank: 2\n";   // the text block runs at 6 / 8 before withEncoding strips 4
        final FeaturePlan pickedPlan = compile(sources, withEncoding(picked).replace("- {fields: [current_bid_t10], from: price_snapshots}", "- {fields: [current_bid_t10, bid_path], from: price_snapshots}"));
        Assertions.assertFalse(pickedPlan.getDiagnostics().hasErrors(), pickedPlan::describe);
        Assertions.assertNull(pickedPlan.getColumn("bids_poly0"));
        Assertions.assertNotNull(pickedPlan.getColumn("bids_poly1"));
        Assertions.assertNotNull(pickedPlan.getColumn("bids_poly2"));
        Assertions.assertNull(pickedPlan.getColumn("bids_poly3"));
        final OutputColumn vector = column(pickedPlan, "bids_vector");
        Assertions.assertEquals(Schema.Type.array, vector.getFieldType().getType());
        Assertions.assertEquals("12", vector.getCoordinates().get("resample"));
        Assertions.assertEquals("12", vector.getCoordinates().get("padLength"));
        Assertions.assertEquals("start", vector.getCoordinates().get("padSide"));
        Assertions.assertEquals("edge", vector.getCoordinates().get("padMode"));
        Assertions.assertEquals("bids_vector", column(pickedPlan, "path_pc_0").getCoordinates().get("arrayField"), pickedPlan::describe);
        final java.util.function.Function<String, FeaturePlan> withPath = extra -> compile(sources, withEncoding(block.replace("degree: 3", "degree: 3\n        " + extra))
                .replace("- {fields: [current_bid_t10], from: price_snapshots}", "- {fields: [current_bid_t10, bid_path], from: price_snapshots}"));
        Assertions.assertTrue(hasCode(withPath.apply("coefficients: [1, 4]"), "row.vector.coefficients"));
        Assertions.assertTrue(hasCode(withPath.apply("coefficients: [1, 1]"), "row.vector.coefficients"));
        Assertions.assertTrue(hasCode(withPath.apply("resample: 0"), "row.vector.resample"));
        Assertions.assertTrue(hasCode(withPath.apply("pad: {length: 4, mode: repeat}"), "row.vector.pad"));
        Assertions.assertTrue(hasCode(withPath.apply("pad: {mode: zero}"), "row.vector.pad"));
        Assertions.assertTrue(hasCode(withPath.apply("pad: 4"), "row.vector.pad"));
        Assertions.assertFalse(withPath.apply("pad: {length: 4}").getDiagnostics().hasErrors(), "edge / end are the defaults");

        // an outcome-like array cannot feed a feature before it is known
        final String outcome = SOURCES.replace("      - {name: final_price, type: double, availableAt: after(event), kind: outcome}\n",
                "      - {name: final_price, type: double, availableAt: after(event), kind: outcome}\n      - {name: bid_path, type: array<float64>, availableAt: after(event), kind: outcome}\n");
        final FeaturePlan leak = compile(outcome, withEncoding(block).replace("- {fields: [sold, final_price], from: auction_results}", "- {fields: [sold, final_price, bid_path], from: auction_results}"));
        Assertions.assertTrue(hasCode(leak, "availability.violation"), leak::describe);

        // parameter validation
        Assertions.assertTrue(hasCode(compile(sources, spec.replace("input: bid_path", "input: start_price")), "row.vector.input"));
        Assertions.assertTrue(hasCode(compile(sources.replace("array<float64>, availableAt", "array<string>, availableAt"), spec), "row.vector.input"));
        Assertions.assertTrue(hasCode(compile(sources, spec.replace("    funcs: [length, mean, argmax, slope, polyfit]\n", "")), "row.vector.funcs"));
        final FeaturePlan unknown = compile(sources, spec.replace("funcs: [length,", "funcs: [kurtosis, length,"));
        Assertions.assertTrue(hasCode(unknown, "row.vector.funcs"));
        Assertions.assertTrue(unknown.getDiagnostics().getErrorMessages().stream().anyMatch(m -> m.contains("argmax") && m.contains("polyfit")), "the message lists what is available");
        Assertions.assertTrue(hasCode(compile(sources, spec.replace("funcs: [length,", "funcs: [mean, length,")), "row.vector.funcs"), "a readout listed twice");
        Assertions.assertTrue(hasCode(compile(sources, spec.replace("slice: {from: -4}", "slice: [0, 4]")), "row.vector.slice"));
        Assertions.assertTrue(hasCode(compile(sources, spec.replace("diff: 1", "diff: -1")), "row.vector.diff"));
        Assertions.assertTrue(hasCode(compile(sources, spec.replace("normalize: mean", "normalize: softmax")), "row.vector.normalize"));
        Assertions.assertTrue(hasCode(compile(sources, spec.replace("position: unit", "position: time")), "row.vector.position"));
        Assertions.assertTrue(hasCode(compile(sources, spec.replace("degree: 3", "degree: 9")), "row.vector.degree"));
        final FeaturePlan unused = compile(sources, spec.replace("slope, polyfit]", "slope]").replace("expr: \"bids_slope / bids_mean\"", "expr: \"bids_slope\""));
        Assertions.assertFalse(unused.getDiagnostics().hasErrors(), unused::describe);
        Assertions.assertTrue(hasCode(unused, "row.vector.degree"), "degree without polyfit is a warning");
    }

    private static final String SVD_BLOCK = """
                  - name: hist
                    scope: population
                    type: svd
                    inputs: [recent_n5_start_price_lag1, recent_n5_start_price_lag2, start_price]
                    rank: 2
                    fit: {artifact: "gs://bucket/features"}
            """;

    @Test
    public void testSvdExpansion() {
        final FeaturePlan plan = compile(SOURCES, withEncoding(SVD_BLOCK));
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        for (int k = 0; k < 2; k++) {
            final OutputColumn c = column(plan, "hist_" + k);
            Assertions.assertEquals("svd", c.getOperator());
            Assertions.assertEquals(FeatureSpec.Scope.population, c.getScope());
            Assertions.assertEquals(Schema.FieldType.FLOAT64.getType(), c.getFieldType().getType());
            Assertions.assertEquals("static", c.getCoordinates().get("fit"));
            Assertions.assertEquals("recent_n5_start_price_lag1,recent_n5_start_price_lag2,start_price", c.getCoordinates().get("fields"));
            Assertions.assertEquals("2", c.getCoordinates().get("rank"));
            Assertions.assertEquals(Integer.toString(k), c.getCoordinates().get("component"));
            Assertions.assertEquals("true", c.getCoordinates().get("center"));
            Assertions.assertEquals("false", c.getCoordinates().get("standardize"));
            Assertions.assertEquals(OutputColumn.Status.staticSafe, c.getStatus());
            Assertions.assertTrue(c.getInputs().contains("recent_n5_start_price_lag1"), c.getInputs()::toString);
        }
        Assertions.assertNull(plan.getColumn("hist_2"));
        // the fit reads the lag columns from the stage input: the sequence stage runs first, the fit stage after it
        final FeaturePlan.Stage fit = plan.getStages().stream().filter(s -> s.kind() == FeaturePlan.StageKind.fit).findFirst().orElseThrow();
        Assertions.assertTrue(fit.columnNames().containsAll(List.of("hist_0", "hist_1")), plan::describe);
        final FeaturePlan.Stage seller = plan.getStages().stream().filter(s -> s.columnNames().contains("recent_n5_start_price_lag1")).findFirst().orElseThrow();
        Assertions.assertTrue(seller.index() < fit.index(), plan::describe);
        Assertions.assertTrue(FeatureStages.artifactPaths(plan).get("hist").endsWith("hist.svd.json"));
        // outputs: the scores by default; residual = one column per input (in input units), residualNorm = their length
        final FeaturePlan residual = compile(SOURCES, withEncoding(SVD_BLOCK.replace("        rank: 2\n", "        rank: 2\n        outputs: [residual, residualNorm]\n")));
        Assertions.assertFalse(residual.getDiagnostics().hasErrors(), residual::describe);
        Assertions.assertNull(residual.getColumn("hist_0"), "scores are not listed");
        Assertions.assertEquals("2", column(residual, "hist_resid_start_price").getCoordinates().get("residual"));
        Assertions.assertEquals("0", column(residual, "hist_resid_recent_n5_start_price_lag1").getCoordinates().get("residual"));
        Assertions.assertNull(column(residual, "hist_resid_start_price").getCoordinates().get("component"));
        Assertions.assertEquals("norm", column(residual, "hist_residnorm").getCoordinates().get("residual"));
        Assertions.assertEquals(OutputColumn.Status.staticSafe, column(residual, "hist_residnorm").getStatus());
        final FeaturePlan.Stage residualFit = residual.getStages().stream().filter(st -> st.kind() == FeaturePlan.StageKind.fit).findFirst().orElseThrow();
        Assertions.assertTrue(residualFit.columnNames().containsAll(List.of("hist_resid_start_price", "hist_residnorm")), residual::describe);
        Assertions.assertNotEquals(plan.getHash(), residual.getHash());
        Assertions.assertNotNull(compile(SOURCES, withEncoding(SVD_BLOCK.replace("        rank: 2\n", "        rank: 2\n        outputs: [scores, residual]\n"))).getColumn("hist_1"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(SVD_BLOCK.replace("        rank: 2\n", "        rank: 2\n        outputs: [loadings]\n"))), "svd.outputs"));
        // defaults: rank = min(d, 8)
        Assertions.assertNotNull(compile(SOURCES, withEncoding(SVD_BLOCK.replace("        rank: 2\n", ""))).getColumn("hist_2"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(SVD_BLOCK.replace("rank: 2", "rank: 4"))), "svd.rank"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(SVD_BLOCK.replace("rank: 2", "rank: 0"))), "svd.rank"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(SVD_BLOCK.replace(", start_price]", ", condition_grade]"))), "svd.input"));
        // a single scalar input is neither a vector nor an array
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(SVD_BLOCK.replace("inputs: [recent_n5_start_price_lag1, recent_n5_start_price_lag2, start_price]", "input: start_price"))), "svd.input"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(SVD_BLOCK.replace("fit: {artifact", "fit: {mode: fold, artifact"))), "svd.fit.mode"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(SVD_BLOCK.replace("rank: 2", "rank: 2\n        maxFeatures: 1"))), "svd.maxFeatures"));
    }

    private static final String SMOOTH_BLOCK = """
                  - name: price_curve
                    scope: population
                    type: smooth
                    input: start_price
                    target: sold
                    range: [0, 500]
                    segments: 6
                    outputs: [curve, residual]
                    fit: {artifact: "gs://bucket/features"}
                  - name: enc
                    scope: population
                    type: encoding
                    keySets:
                      - keys: [seller_id]
                    targets:
                      - {field: price_curve_resid, stats: [mean]}
            """;

    /**
     * smooth: the curve of a target over a numeric key is a feature (the target is read through the fit only), the
     * residual reads the row's own target — a target for other blocks or a label, never a feature — and the block is
     * one fit stage ahead of the keyed stage that consumes it.
     */
    @Test
    public void testSmoothExpansion() {
        final FeaturePlan plan = compile(SOURCES, withEncoding(SMOOTH_BLOCK));
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        Assertions.assertTrue(hasCode(plan, "fit.mode.static"), plan::describe);
        final OutputColumn curve = column(plan, "price_curve");
        Assertions.assertEquals("smooth", curve.getOperator());
        Assertions.assertEquals(FeatureSpec.Scope.population, curve.getScope());
        Assertions.assertEquals(Schema.FieldType.FLOAT64.getType(), curve.getFieldType().getType());
        Assertions.assertEquals(OutputColumn.Status.staticSafe, curve.getStatus());
        Assertions.assertFalse(curve.isIntermediate());
        final Map<String, String> k = curve.getCoordinates();
        Assertions.assertEquals("static", k.get("fit"));
        Assertions.assertEquals("spline", k.get("method"));
        Assertions.assertEquals("start_price", k.get("field"));
        Assertions.assertEquals("sold", k.get("target"));
        Assertions.assertEquals("curve", k.get("output"));
        Assertions.assertEquals("6", k.get("segments"));
        Assertions.assertEquals("3", k.get("degree"));
        Assertions.assertEquals("0.0", k.get("lo"));
        Assertions.assertEquals("500.0", k.get("hi"));
        Assertions.assertEquals("2", k.get("penaltyOrder"));
        Assertions.assertEquals("reml", k.get("lambda"));
        Assertions.assertTrue(curve.getInputs().containsAll(List.of("start_price", "sold")), curve.getInputs()::toString);
        // the residual is as available as the row's own outcome: consumed by the encoding, it is an intermediate
        final OutputColumn residual = column(plan, "price_curve_resid");
        Assertions.assertEquals("residual", residual.getCoordinates().get("output"));
        Assertions.assertTrue(residual.isIntermediate(), plan::describe);
        Assertions.assertTrue(hasCode(plan, "availability.intermediate"), plan::describe);
        final FeaturePlan.Stage fit = plan.getStages().stream().filter(s -> s.kind() == FeaturePlan.StageKind.fit).findFirst().orElseThrow();
        Assertions.assertTrue(fit.columnNames().containsAll(List.of("price_curve", "price_curve_resid")), plan::describe);
        final FeaturePlan.Stage keyed = plan.getStages().stream().filter(s -> s.columnNames().stream().anyMatch(n -> n.startsWith("enc__seller_id"))).findFirst().orElseThrow();
        Assertions.assertTrue(fit.index() < keyed.index(), plan::describe);
        Assertions.assertTrue(FeatureStages.artifactPaths(plan).get("price_curve").endsWith("price_curve.smooth.json"));

        // a residual nobody consumes is an outcome in the output — unless it is the declared label
        final String alone = SMOOTH_BLOCK.substring(0, SMOOTH_BLOCK.indexOf("      - name: enc\n"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(alone)), "availability.violation"));
        final FeaturePlan label = compile(SOURCES, withEncoding(alone).replace("output:\n", "output:\n  roles: {label: price_curve_resid}\n"));
        Assertions.assertFalse(label.getDiagnostics().hasErrors(), label::describe);
        Assertions.assertEquals(OutputColumn.Status.label, column(label, "price_curve_resid").getStatus());
        // the curve alone (the default output), a declared strength, another penalty: all in the hash
        final FeaturePlan declared = compile(SOURCES, withEncoding(alone.replace("        outputs: [curve, residual]\n", "        penalty: {order: 1, lambda: 25}\n        degree: 2\n")));
        Assertions.assertFalse(declared.getDiagnostics().hasErrors(), declared::describe);
        Assertions.assertNull(declared.getColumn("price_curve_resid"));
        Assertions.assertEquals("25.0", column(declared, "price_curve").getCoordinates().get("lambda"));
        Assertions.assertEquals("1", column(declared, "price_curve").getCoordinates().get("penaltyOrder"));
        Assertions.assertEquals("2", column(declared, "price_curve").getCoordinates().get("degree"));
        Assertions.assertNotEquals(plan.getHash(), declared.getHash());

        // every parameter error
        final String curveOnly = alone.replace("        outputs: [curve, residual]\n", "");
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(curveOnly.replace("        range: [0, 500]\n", ""))), "smooth.range"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(curveOnly.replace("range: [0, 500]", "range: [500, 0]"))), "smooth.range"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(curveOnly.replace("        target: sold\n", ""))), "smooth.target"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(curveOnly.replace("target: sold", "target: category"))), "smooth.target"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(curveOnly.replace("input: start_price", "input: condition_grade"))), "smooth.input"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(curveOnly.replace("segments: 6", "segments: 6\n        method: isotonic"))), "smooth.method"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(curveOnly.replace("segments: 6", "segments: 0"))), "smooth.segments"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(curveOnly.replace("segments: 6", "segments: 80"))), "smooth.segments"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(curveOnly.replace("segments: 6", "segments: 6\n        degree: 9"))), "smooth.degree"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(curveOnly.replace("segments: 6", "segments: 6\n        penalty: {order: 4}"))), "smooth.penalty"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(curveOnly.replace("segments: 6", "segments: 6\n        penalty: {lambda: -1}"))), "smooth.penalty"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(curveOnly.replace("segments: 6", "segments: 6\n        penalty: {lamda: 1}"))), "smooth.penalty"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(curveOnly.replace("segments: 6", "segments: 1\n        degree: 1"))), "smooth.penalty"), "two basis functions cannot carry a second difference");
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(curveOnly.replace("segments: 6", "segments: 6\n        outputs: [slope]"))), "smooth.outputs"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(curveOnly.replace("fit: {artifact", "fit: {mode: fold, artifact"))), "smooth.fit.mode"));
    }

    /**
     * Under {@code fit.mode: forward} the curve is re-solved per time block from the blocks whose TARGETS are known at
     * predictAt (the lag of the outcome), and a uniform quantileTransform input supplies the range: knots at the
     * input's quantiles, from a fit stage of its own ahead of the curve's.
     */
    @Test
    public void testSmoothForwardFitAndQuantileKnots() {
        final String blocks = """
                  - name: price_q
                    scope: population
                    type: quantileTransform
                    input: start_price
                  - name: price_curve
                    scope: population
                    type: smooth
                    input: price_q
                    target: sold
                    fit: {mode: forward, blocks: {size: P7D}, window: P28D}
            """;
        final FeaturePlan plan = compile(SOURCES, withEncoding(blocks));
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        Assertions.assertTrue(hasCode(plan, "smooth.range"), plan::describe);
        Assertions.assertTrue(hasCode(plan, "fit.mode.forward"), plan::describe);
        final OutputColumn curve = column(plan, "price_curve");
        Assertions.assertEquals("forward", curve.getCoordinates().get("fit"));
        Assertions.assertEquals("0.0", curve.getCoordinates().get("lo"));
        Assertions.assertEquals("1.0", curve.getCoordinates().get("hi"));
        Assertions.assertEquals("4", curve.getCoordinates().get("windowBlocks"));
        // sold settles PT30M after the event and is ingested P6D later: the readable blocks are delayed by that lag
        Assertions.assertTrue(Long.parseLong(curve.getCoordinates().get("forwardLagMillis")) > 6L * 86_400_000L, curve.getCoordinates()::toString);
        Assertions.assertEquals(OutputColumn.Status.staticSafe, curve.getStatus());
        final FeaturePlan.Stage knots = plan.getStages().stream().filter(s -> s.columnNames().contains("price_q")).findFirst().orElseThrow();
        final FeaturePlan.Stage fit = plan.getStages().stream().filter(s -> s.columnNames().contains("price_curve")).findFirst().orElseThrow();
        Assertions.assertTrue(knots.index() < fit.index(), plan::describe);
        // an additive fit by hand: the second curve is fitted on what the first leaves, one fit stage later — and its
        // own residual is what an encoding would take as the target net of both keys
        final String chained = """
                  - name: by_price
                    scope: population
                    type: smooth
                    input: start_price
                    target: final_price
                    range: [0, 500]
                    outputs: [residual]
                  - name: by_quantity
                    scope: population
                    type: smooth
                    input: quantity
                    target: by_price_resid
                    range: [1, 10]
                    segments: 4
            """;
        final FeaturePlan chain = compile(SOURCES, withEncoding(chained));
        Assertions.assertFalse(chain.getDiagnostics().hasErrors(), chain::describe);
        Assertions.assertNull(chain.getColumn("by_price"), "outputs: [residual] alone emits no curve column");
        Assertions.assertTrue(column(chain, "by_price_resid").isIntermediate());
        Assertions.assertEquals("by_price_resid", column(chain, "by_quantity").getCoordinates().get("target"));
        Assertions.assertEquals(OutputColumn.Status.staticSafe, column(chain, "by_quantity").getStatus());
        final FeaturePlan.Stage first = chain.getStages().stream().filter(s -> s.columnNames().contains("by_price_resid")).findFirst().orElseThrow();
        final FeaturePlan.Stage second = chain.getStages().stream().filter(s -> s.columnNames().contains("by_quantity")).findFirst().orElseThrow();
        Assertions.assertTrue(first.index() < second.index(), chain::describe);
        // a normal-score input has no bounded range to default to
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(blocks.replace("input: start_price\n", "input: start_price\n        distribution: normal\n"))), "smooth.range"));
        Assertions.assertTrue(compile(SOURCES, withEncoding(blocks.replace("input: start_price\n", "input: start_price\n        distribution: normal\n"))).getDiagnostics().hasErrors());
    }

    /**
     * {@code fit.minRows}: the fewest rows a lookup fit is solved from. A curve defaults to one row more than its
     * coefficients (fewer leave it to the penalty alone, as many are interpolated), the other types to no floor; a
     * block's own value wins over the top-level one, 0 switches the floor off, and the types that do not take it —
     * discretize / factorization, and an encoding, which shrinks a thin level instead — say that they ignore it.
     */
    @Test
    public void testFitMinRows() {
        final String blocks = """
                  - name: price_curve
                    scope: population
                    type: smooth
                    input: start_price
                    target: final_price
                    range: [0, 500]
                    segments: 6
                  - name: price_q
                    scope: population
                    type: quantileTransform
                    input: start_price
            """;
        final FeaturePlan defaults = compile(SOURCES, withEncoding(blocks));
        Assertions.assertFalse(defaults.getDiagnostics().hasErrors(), defaults::describe);
        Assertions.assertEquals("10", column(defaults, "price_curve").getCoordinates().get("minRows"), "segments 6 + degree 3, and one row to spare");
        Assertions.assertNull(column(defaults, "price_q").getCoordinates().get("minRows"));
        Assertions.assertTrue(defaults.describe().contains("fewer than 10 row(s)"), defaults::describe);

        final FeaturePlan declared = compile(SOURCES, withEncoding(blocks
                .replace("        segments: 6\n", "        segments: 6\n        fit: {minRows: 0}\n")
                .replace("        type: quantileTransform\n", "        type: quantileTransform\n        fit: {minRows: 200}\n")));
        Assertions.assertFalse(declared.getDiagnostics().hasErrors(), declared::describe);
        Assertions.assertNull(column(declared, "price_curve").getCoordinates().get("minRows"));
        Assertions.assertEquals("200", column(declared, "price_q").getCoordinates().get("minRows"));
        // the floor is part of what is fitted: a different plan, a different artifact directory
        Assertions.assertNotEquals(defaults.getHash(), declared.getHash());

        // a top-level floor reaches every lookup fit that declares none of its own
        final String topLevel = withEncoding(blocks.replace("        type: quantileTransform\n", "        type: quantileTransform\n        fit: {minRows: 200}\n"))
                .replace("features:\n", "fit: {minRows: 30}\nfeatures:\n");
        final FeaturePlan inherited = compile(SOURCES, topLevel);
        Assertions.assertFalse(inherited.getDiagnostics().hasErrors(), inherited::describe);
        Assertions.assertEquals("30", column(inherited, "price_curve").getCoordinates().get("minRows"));
        Assertions.assertEquals("200", column(inherited, "price_q").getCoordinates().get("minRows"));
        Assertions.assertTrue(hasCode(inherited, "fit.minRows"), inherited::describe);

        final FeaturePlan negative = compile(SOURCES, withEncoding(blocks.replace("        segments: 6\n", "        segments: 6\n        fit: {minRows: -1}\n")));
        Assertions.assertTrue(hasCode(negative, "fit.minRows"), negative::describe);
        Assertions.assertTrue(negative.getDiagnostics().hasErrors());

        final String binned = """
                  - name: price_bin
                    scope: population
                    type: discretize
                    input: start_price
                    bins: 3
                    fit: {minRows: 50}
            """;
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(binned)), "discretize.fit.minRows"));
        final String encoded = """
                  - name: enc
                    scope: population
                    type: encoding
                    keySets:
                      - keys: [seller_id]
                    targets:
                      - {field: sold, stats: [mean]}
                    fit: {minRows: 50}
            """;
        final FeaturePlan encoding = compile(SOURCES, withEncoding(encoded));
        Assertions.assertFalse(encoding.getDiagnostics().hasErrors(), encoding::describe);
        Assertions.assertTrue(hasCode(encoding, "encoding.fit.minRows"), encoding::describe);
    }

    /**
     * A row column may read fitted columns of several fit stages — the sum of the curves of a chained additive fit is
     * the prediction of that fit. It is evaluated in the LATEST of those fit stages (every fitted input exists there),
     * not the earliest one, which precedes some of what it reads.
     */
    @Test
    public void testRowColumnOverSeveralFitStages() {
        final String blocks = """
                  - name: by_price
                    scope: population
                    type: smooth
                    input: start_price
                    target: final_price
                    range: [0, 500]
                    outputs: [curve, residual]
                  - name: by_quantity
                    scope: population
                    type: smooth
                    input: quantity
                    target: by_price_resid
                    range: [1, 10]
                    segments: 4
                  - name: additive
                    scope: row
                    expr: "by_price + by_quantity"
            """;
        final FeaturePlan plan = compile(SOURCES, withEncoding(blocks));
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        final FeaturePlan.Stage first = plan.getStages().stream().filter(s -> s.columnNames().contains("by_price")).findFirst().orElseThrow();
        final FeaturePlan.Stage second = plan.getStages().stream().filter(s -> s.columnNames().contains("by_quantity")).findFirst().orElseThrow();
        final FeaturePlan.Stage sum = plan.getStages().stream().filter(s -> s.columnNames().contains("additive")).findFirst().orElseThrow();
        Assertions.assertTrue(first.index() < second.index(), plan::describe);
        Assertions.assertEquals(second.index(), sum.index(), plan::describe);
        // two blocks of ONE fit stage were never the problem: the column stays in that stage
        final String sameStage = blocks.replace("target: by_price_resid", "target: final_price").replace("        outputs: [curve, residual]\n", "");
        final FeaturePlan same = compile(SOURCES, withEncoding(sameStage));
        Assertions.assertFalse(same.getDiagnostics().hasErrors(), same::describe);
        final FeaturePlan.Stage fit = same.getStages().stream().filter(s -> s.columnNames().contains("by_price")).findFirst().orElseThrow();
        Assertions.assertTrue(fit.columnNames().containsAll(List.of("by_quantity", "additive")), same::describe);
    }

    /**
     * The same rule with a keyed stage instead of a second fit: a row column over a fitted column AND a sequence
     * column of a later stage goes to that later stage — the fit stage precedes half of what it reads.
     */
    @Test
    public void testRowColumnOverFitAndLaterKeyedStage() {
        final String blocks = """
                  - name: by_price
                    scope: population
                    type: smooth
                    input: start_price
                    target: final_price
                    range: [0, 500]
                  - name: bycat
                    scope: sequence
                    entity: cat
                    windows:
                      - {maxEvents: 3}
                    ops:
                      - {type: aggregate, field: start_price, funcs: [mean]}
                  - name: mix
                    scope: row
                    expr: "by_price + bycat_n3_start_price_mean"
            """;
        final FeaturePlan plan = compile(SOURCES, withEncoding(blocks));
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        final FeaturePlan.Stage fit = plan.getStages().stream().filter(s -> s.columnNames().contains("by_price")).findFirst().orElseThrow();
        final FeaturePlan.Stage keyed = plan.getStages().stream().filter(s -> s.columnNames().contains("bycat_n3_start_price_mean")).findFirst().orElseThrow();
        Assertions.assertTrue(fit.index() < keyed.index(), plan::describe);
        Assertions.assertTrue(keyed.columnNames().contains("mix"), plan::describe);
    }

    /**
     * {@code fit.align}: a forward svd / spectralEmbedding rotates every fit into the coordinates of the one before it
     * unless told otherwise; a static fit is solved once and has nothing to align to, and the fits without a gauge
     * (a curve, quantile knots, level statistics) say that they ignore it.
     */
    @Test
    public void testFitAlign() {
        final String blocks = """
                  - name: pc
                    scope: population
                    type: svd
                    inputs: [start_price, current_bid_t10]
                    rank: 2
                    fit: {mode: forward, blocks: {size: P7D}}
                  - name: grade_embed
                    scope: population
                    type: spectralEmbedding
                    sequenceOf: {entity: seller, field: condition_grade}
                    rank: 2
                    fit: {mode: forward, blocks: {size: P7D}, align: sign}
            """;
        final FeaturePlan plan = compile(SOURCES, withEncoding(blocks));
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        Assertions.assertEquals("procrustes", column(plan, "pc_0").getCoordinates().get("align"));
        Assertions.assertEquals("sign", column(plan, "grade_embed_1").getCoordinates().get("align"));
        Assertions.assertTrue(plan.describe().contains("fit.align procrustes, the default"), plan::describe);
        Assertions.assertTrue(plan.describe().contains("fit.align sign"), plan::describe);
        // declared, it is part of what is fitted
        final FeaturePlan none = compile(SOURCES, withEncoding(blocks.replace("blocks: {size: P7D}}", "blocks: {size: P7D}, align: none}")));
        Assertions.assertEquals("none", column(none, "pc_0").getCoordinates().get("align"));
        Assertions.assertNotEquals(plan.getHash(), none.getHash());

        final FeaturePlan unknown = compile(SOURCES, withEncoding(blocks.replace("align: sign", "align: rotate")));
        Assertions.assertTrue(hasCode(unknown, "fit.align"), unknown::describe);
        Assertions.assertTrue(unknown.getDiagnostics().hasErrors());

        final String ignored = """
                  - name: pc
                    scope: population
                    type: svd
                    inputs: [start_price, current_bid_t10]
                    rank: 2
                    fit: {align: sign}
                  - name: price_curve
                    scope: population
                    type: smooth
                    input: start_price
                    target: final_price
                    range: [0, 500]
                    fit: {mode: forward, blocks: {size: P7D}, align: procrustes}
            """;
        final FeaturePlan statik = compile(SOURCES, withEncoding(ignored));
        Assertions.assertFalse(statik.getDiagnostics().hasErrors(), statik::describe);
        Assertions.assertNull(column(statik, "pc_0").getCoordinates().get("align"));
        Assertions.assertTrue(hasCode(statik, "svd.fit.align"), statik::describe);
        Assertions.assertTrue(hasCode(statik, "smooth.fit.align"), statik::describe);
    }

    /**
     * An entity's {@code minInterval} is a declaration the compiler trusts: it turns the window shift of an outcome
     * (settlement + ingestion of {@code sold}, PT144H38M here) into {@code staticSafe} when it is at least as long. The
     * plan says what rests on it — the columns, the shift absorbed — with an audit query over the input, an info, and
     * the run-time counter's name; an entity whose declaration absorbs nothing appears nowhere.
     */
    @Test
    public void testMinIntervalAudit() {
        final FeaturePlan plan = compile(SOURCES, SPEC.replace("- {name: seller, keys: [seller_id]}", "- {name: seller, keys: [seller_id], minInterval: P7D}"));
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        final OutputColumn mean = column(plan, "recent_n5_sold_mean");
        Assertions.assertEquals(OutputColumn.Status.staticSafe, mean.getStatus());
        Assertions.assertEquals("PT168H", mean.getCoordinates().get("minInterval"));
        Assertions.assertEquals("seller", mean.getCoordinates().get("minIntervalEntity"));
        Assertions.assertEquals(OutputColumn.Status.staticSafe, column(plan, "recent_n5_start_price_lag1").getStatus());
        Assertions.assertNull(column(plan, "recent_n5_start_price_lag1").getCoordinates().get("minIntervalEntity"), "an attribute needs no shift: nothing rests on the declaration");
        Assertions.assertEquals(1, plan.getMinIntervalAudits().size(), plan::describe);
        final FeaturePlan.MinIntervalAudit audit = plan.getMinIntervalAudits().get(0);
        Assertions.assertEquals("seller", audit.entity());
        Assertions.assertEquals(List.of("seller_id"), audit.keys());
        Assertions.assertEquals(java.time.Duration.ofDays(7), audit.minInterval());
        Assertions.assertEquals(column(compile(SOURCES, SPEC), "recent_n5_sold_mean").getWindowShift(), audit.shift());
        Assertions.assertTrue(audit.columns().contains("recent_n5_sold_mean"), audit::describe);
        Assertions.assertFalse(audit.columns().contains("recent_n5_start_price_lag1"), audit::describe);
        Assertions.assertTrue(hasCode(plan, "entity.minInterval"), plan::describe);
        Assertions.assertEquals(1, plan.getDiagnostics().getMessages().stream().filter(m -> m.code().equals("entity.minInterval")).count(), "once per entity");
        final FeaturePlan.AuditQuery query = plan.getAuditQueries().stream().filter(q -> q.stages().contains("entity seller")).findFirst().orElseThrow(() -> new AssertionError(plan.describe()));
        Assertions.assertTrue(query.sql().contains("LAG(session_time) OVER (PARTITION BY seller_id ORDER BY session_time)"), query.sql());
        Assertions.assertTrue(query.sql().contains("gap_seconds < 604800"), query.sql());
        Assertions.assertTrue(query.note().contains("feature/minInterval_seller_below"), query.note());
        Assertions.assertTrue(plan.describe().contains("-- minInterval audit"), plan::describe);
        Assertions.assertEquals(1, plan.toJson().getAsJsonArray("minIntervalAudit").size());
        // a declaration shorter than the shift absorbs nothing: the window stays shifted and no audit is raised
        final FeaturePlan shorter = compile(SOURCES, SPEC.replace("- {name: seller, keys: [seller_id]}", "- {name: seller, keys: [seller_id], minInterval: P5D}"));
        Assertions.assertEquals(OutputColumn.Status.windowShift, column(shorter, "recent_n5_sold_mean").getStatus());
        Assertions.assertTrue(shorter.getMinIntervalAudits().isEmpty());
        Assertions.assertFalse(hasCode(shorter, "entity.minInterval"));
        Assertions.assertTrue(compile(SOURCES, SPEC).getMinIntervalAudits().isEmpty());
    }

    /**
     * The counter is kept by ONE keyed stage per entity, the one keyed by the entity itself: a window reduced by a
     * {@code $self} equality filter runs under a finer key (entity keys + the filter field) and would see the gaps of
     * that sub-key, not the entity's; and every keyed stage replays the same rows, so two stages counting would count
     * a row twice. The reduced-key stage is used only when no exact one rests on the declaration.
     */
    @Test
    public void testMinIntervalAuditIsAssignedToOneStagePerEntity() {
        final String declared = SPEC.replace("- {name: seller, keys: [seller_id]}", "- {name: seller, keys: [seller_id], minInterval: P7D}");
        final String filter = "- {filter: \"condition_grade = $self.condition_grade\", as: sameGrade}";
        final FeaturePlan both = compile(SOURCES, declared.replace("- {maxEvents: 5}", "- {}\n      " + filter));
        Assertions.assertFalse(both.getDiagnostics().hasErrors(), both::describe);
        Assertions.assertEquals("seller_id,condition_grade", column(both, "recent_sameGrade_sold_mean").getCoordinates().get("stageKeys"));
        Assertions.assertEquals("seller", column(both, "recent_sameGrade_sold_mean").getCoordinates().get("minIntervalEntity"), "the reduced window rests on it too");
        Assertions.assertEquals("seller", column(both, "recent_all_sold_mean").getCoordinates().get("minIntervalEntity"));
        final Map<String, OutputColumn> columns = new java.util.HashMap<>();
        for (final OutputColumn c : both.getColumns()) columns.put(c.getCanonicalName(), c);
        final Map<Integer, Map<String, Long>> assigned = FeatureStages.Wiring.assignMinIntervalAudits(both, columns);
        Assertions.assertEquals(1, assigned.size(), assigned::toString);
        final int stage = assigned.keySet().iterator().next();
        Assertions.assertEquals(List.of("seller_id"), both.getStages().get(stage).keys(), both::describe);
        Assertions.assertEquals(Map.of("seller", 7L * 86_400_000L), assigned.get(stage));
        // only the reduced window rests on it: that stage is the fallback
        final String reducedBlock = """
            features:
              - name: recent
                scope: sequence
                entity: seller
                windows:
                  - {filter: "condition_grade = $self.condition_grade", as: sameGrade}
                ops:
                  - {type: aggregate, field: sold, funcs: [mean]}
            """;
        final FeaturePlan reducedOnly = compile(SOURCES, declared.substring(0, declared.indexOf("features:\n")) + reducedBlock + declared.substring(declared.indexOf("output:\n")));
        Assertions.assertFalse(reducedOnly.getDiagnostics().hasErrors(), reducedOnly::describe);
        columns.clear();
        for (final OutputColumn c : reducedOnly.getColumns()) columns.put(c.getCanonicalName(), c);
        final Map<Integer, Map<String, Long>> fallback = FeatureStages.Wiring.assignMinIntervalAudits(reducedOnly, columns);
        Assertions.assertEquals(1, fallback.size(), fallback::toString);
        Assertions.assertEquals(List.of("seller_id", "condition_grade"), reducedOnly.getStages().get(fallback.keySet().iterator().next()).keys(), reducedOnly::describe);
        // nothing rests on the declaration: nothing is assigned
        Assertions.assertTrue(FeatureStages.Wiring.assignMinIntervalAudits(compile(SOURCES, SPEC), columns).isEmpty());
    }

    private static final String TRANSITION_BLOCK = """
                  - name: grade_next
                    scope: population
                    type: transitionStats
                    sequenceOf: {entity: seller, field: condition_grade}
                    emit: [{toValueProb: good}, {toValueProb: fair}]
                    blend: {perEntity: true, priorWeight: 5}
            """;

    /**
     * transitionStats is a desugaring: the previous value is a lag column of the entity, the statistic the expanding,
     * shrunk {@code distribution} of the field keyed on (entity, previous value) → (previous value) → global.
     */
    @Test
    public void testTransitionStatsExpansion() {
        final FeaturePlan plan = compile(SOURCES, withEncoding(TRANSITION_BLOCK));
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        Assertions.assertTrue(hasCode(plan, "transitionStats.expansion"), plan::describe);
        // the state: the entity's previous value, an intermediate lag column of the block
        final OutputColumn previous = column(plan, "grade_next_all_prev_lag1");
        Assertions.assertEquals("lag", previous.getOperator());
        Assertions.assertEquals("condition_grade", previous.getCoordinates().get("field"));
        Assertions.assertTrue(previous.isIntermediate());
        // one probability column per emitted value, read from the (intermediate) shrunk distribution map
        for (final String value : List.of("good", "fair")) {
            final OutputColumn c = column(plan, "grade_next_to_" + value);
            Assertions.assertEquals("mapValue", c.getOperator());
            Assertions.assertEquals(value, c.getCoordinates().get("value"));
            Assertions.assertEquals(Schema.FieldType.FLOAT64.getType(), c.getFieldType().getType());
            Assertions.assertEquals(OutputColumn.Status.staticSafe, c.getStatus());
            Assertions.assertFalse(c.isIntermediate());
        }
        final OutputColumn map = column(plan, "grade_next_to");
        Assertions.assertTrue(map.isIntermediate());
        Assertions.assertEquals("dirichletMultinomial", map.getCoordinates().get("family"));
        final List<Shrinkage.Level> levels = Shrinkage.parseLevels(map.getCoordinates().get("levels"));
        Assertions.assertEquals(3, levels.size(), map.getCoordinates()::toString);
        // the lag column is computed by the seller stage, the distribution keyed on it one keyed stage later
        final FeaturePlan.Stage lagStage = plan.getStages().stream().filter(s -> s.columnNames().contains("grade_next_all_prev_lag1")).findFirst().orElseThrow();
        final FeaturePlan.Stage leafStage = plan.getStages().stream().filter(s -> s.keys().equals(List.of("seller_id", "grade_next_all_prev_lag1"))).findFirst().orElseThrow();
        Assertions.assertTrue(lagStage.index() < leafStage.index(), plan::describe);

        // emit: distribution keeps the map; pooled (no blend) drops the entity level; order 2 adds the suffix level
        final FeaturePlan distribution = compile(SOURCES, withEncoding(TRANSITION_BLOCK.replace("emit: [{toValueProb: good}, {toValueProb: fair}]", "emit: [distribution, {toValueProb: good}]")));
        Assertions.assertFalse(distribution.getDiagnostics().hasErrors(), distribution::describe);
        Assertions.assertFalse(column(distribution, "grade_next_to").isIntermediate());
        Assertions.assertNotNull(distribution.getColumn("grade_next_to_good"));
        final FeaturePlan pooled = compile(SOURCES, withEncoding(TRANSITION_BLOCK.replace("        blend: {perEntity: true, priorWeight: 5}\n", "")));
        Assertions.assertFalse(pooled.getDiagnostics().hasErrors(), pooled::describe);
        Assertions.assertEquals(2, Shrinkage.parseLevels(column(pooled, "grade_next_to").getCoordinates().get("levels")).size());
        final FeaturePlan second = compile(SOURCES, withEncoding(TRANSITION_BLOCK.replace("        emit:", "        order: 2\n        emit:")));
        Assertions.assertFalse(second.getDiagnostics().hasErrors(), second::describe);
        Assertions.assertNotNull(second.getColumn("grade_next_all_prev_lag2"));
        Assertions.assertEquals(4, Shrinkage.parseLevels(column(second, "grade_next_to").getCoordinates().get("levels")).size());
        Assertions.assertNotEquals(plan.getHash(), second.getHash());
        // a top-level lookup fit does not reach it: a value distribution lives in the expanding replay
        final FeaturePlan underStatic = compile(SOURCES, withEncoding(TRANSITION_BLOCK).replace("output:\n", "fit: {mode: static}\noutput:\n"));
        Assertions.assertFalse(hasCode(underStatic, "encoding.stat.static"), underStatic::describe);

        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(TRANSITION_BLOCK.replace("        emit: [{toValueProb: good}, {toValueProb: fair}]\n", ""))), "transitionStats.emit"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(TRANSITION_BLOCK.replace("{toValueProb: fair}", "{fromValueProb: fair}"))), "transitionStats.parameters"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(TRANSITION_BLOCK.replace("priorWeight: 5", "priorWeight: 0"))), "transitionStats.blend"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(TRANSITION_BLOCK.replace("        emit:", "        order: 9\n        emit:"))), "transitionStats.order"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(TRANSITION_BLOCK.replace("entity: seller", "entity: buyer"))), "transitionStats.sequenceOf"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(TRANSITION_BLOCK.replace("field: condition_grade", "field: start_price"))), "transitionStats.sequenceOf"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(TRANSITION_BLOCK.replace("        emit:", "        maxFeatures: 1\n        emit:"))), "transitionStats.maxFeatures"));
        // a parameter of the sibling type would be parsed and silently dropped (the shrinkage / the vocabulary cap)
        for (final String foreign : List.of("cooccur: {window: 2}", "of: previous", "maxValues: 32", "rank: 4")) {
            Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(TRANSITION_BLOCK.replace("        emit:", "        " + foreign + "\n        emit:"))),
                    "transitionStats.parameters"), foreign);
        }
    }

    /**
     * The readouts of the distribution: {@code ownValueProb} / {@code surprisal} read the row's own value too — as
     * available as that value is — while {@code entropy} / {@code expected} read the map alone; {@code expected}
     * needs an integer code; every readout is one column, counted against {@code maxFeatures}.
     */
    @Test
    public void testTransitionStatsReadouts() {
        final FeaturePlan plan = compile(SOURCES, withEncoding(TRANSITION_BLOCK.replace("emit: [{toValueProb: good}, {toValueProb: fair}]", "emit: [ownValueProb, surprisal, entropy, ownValueProb]")));
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        for (final String readout : List.of("ownValueProb", "surprisal", "entropy")) {
            final OutputColumn c = column(plan, "grade_next_" + readout);
            Assertions.assertEquals("mapReadout", c.getOperator());
            Assertions.assertEquals(readout, c.getCoordinates().get("readout"));
            Assertions.assertEquals(OutputColumn.Status.staticSafe, c.getStatus(), readout);
            Assertions.assertTrue(c.getInputs().contains("grade_next_to"), readout);
            Assertions.assertEquals(!"entropy".equals(readout), c.getInputs().contains("condition_grade"), readout);
        }
        Assertions.assertTrue(column(plan, "grade_next_to").isIntermediate());
        Assertions.assertNull(plan.getColumn("grade_next_to_good"));
        Assertions.assertFalse(hasCode(plan, "transitionStats.emit.own"), "an attribute field: the own value is available");
        // an outcome field: the own value is the row's outcome, so the two readouts that read it are violations (a hint says so)
        final FeaturePlan outcome = compile(SOURCES, withEncoding(TRANSITION_BLOCK.replace("field: condition_grade", "field: sold").replace("emit: [{toValueProb: good}, {toValueProb: fair}]", "emit: [ownValueProb, entropy]")));
        Assertions.assertEquals(OutputColumn.Status.violation, column(outcome, "grade_next_ownValueProb").getStatus(), outcome::describe);
        Assertions.assertEquals(OutputColumn.Status.staticSafe, column(outcome, "grade_next_entropy").getStatus(), outcome::describe);
        Assertions.assertTrue(hasCode(outcome, "transitionStats.emit.own"), outcome::describe);
        // expected: the probability-weighted mean of an integer code — refused on a string field
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(TRANSITION_BLOCK.replace("emit: [{toValueProb: good}, {toValueProb: fair}]", "emit: [expected]"))), "transitionStats.emit"));
        final FeaturePlan expected = compile(SOURCES, withEncoding(TRANSITION_BLOCK.replace("field: condition_grade", "field: quantity").replace("emit: [{toValueProb: good}, {toValueProb: fair}]", "emit: [expected, {toValueProb: 1}]")));
        Assertions.assertFalse(expected.getDiagnostics().hasErrors(), expected::describe);
        Assertions.assertEquals("mapReadout", column(expected, "grade_next_expected").getOperator());
        Assertions.assertNotNull(expected.getColumn("grade_next_to_1"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(TRANSITION_BLOCK.replace("emit: [{toValueProb: good}, {toValueProb: fair}]", "emit: [ownValueProb, entropy]\n        maxFeatures: 1"))), "transitionStats.maxFeatures"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(TRANSITION_BLOCK.replace("{toValueProb: fair}", "perplexity"))), "transitionStats.parameters"));
    }

    private static final String SPECTRAL_BLOCK = """
                  - name: grade_embed
                    scope: population
                    type: spectralEmbedding
                    sequenceOf: {entity: seller, field: condition_grade}
                    cooccur: {window: 2, weighting: ppmi}
                    rank: 3
                    fit: {artifact: "gs://bucket/features"}
            """;

    /**
     * spectralEmbedding: the pairs come from the lag path (a keyed stage), the factorisation is a fit stage after it,
     * and the row's own value — or its previous one — is looked up.
     */
    @Test
    public void testSpectralEmbeddingExpansion() {
        final FeaturePlan plan = compile(SOURCES, withEncoding(SPECTRAL_BLOCK));
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        Assertions.assertTrue(hasCode(plan, "fit.mode.static"), plan::describe);
        for (int k = 0; k < 3; k++) {
            final OutputColumn c = column(plan, "grade_embed_" + k);
            Assertions.assertEquals("spectralEmbedding", c.getOperator());
            Assertions.assertEquals("static", c.getCoordinates().get("fit"));
            Assertions.assertEquals("condition_grade", c.getCoordinates().get("field"));
            Assertions.assertEquals("condition_grade", c.getCoordinates().get("applied"));
            Assertions.assertEquals("grade_embed_all_prev_lag1,grade_embed_all_prev_lag2", c.getCoordinates().get("path"));
            Assertions.assertEquals(Integer.toString(k), c.getCoordinates().get("component"));
            Assertions.assertEquals("256", c.getCoordinates().get("maxValues"));
            Assertions.assertEquals(OutputColumn.Status.staticSafe, c.getStatus());
        }
        Assertions.assertNull(plan.getColumn("grade_embed_3"));
        Assertions.assertTrue(column(plan, "grade_embed_all_prev_lag2").isIntermediate());
        final FeaturePlan.Stage fit = plan.getStages().stream().filter(s -> s.kind() == FeaturePlan.StageKind.fit).findFirst().orElseThrow();
        Assertions.assertTrue(fit.columnNames().containsAll(List.of("grade_embed_0", "grade_embed_2")), plan::describe);
        final FeaturePlan.Stage lagStage = plan.getStages().stream().filter(s -> s.columnNames().contains("grade_embed_all_prev_lag1")).findFirst().orElseThrow();
        Assertions.assertTrue(lagStage.index() < fit.index(), plan::describe);
        Assertions.assertTrue(FeatureStages.artifactPaths(plan).get("grade_embed").endsWith("grade_embed.spectral.json"));

        // of: previous embeds the state the entity comes from; forward walks the neighbourhoods
        final FeaturePlan previous = compile(SOURCES, withEncoding(SPECTRAL_BLOCK.replace("        rank: 3\n", "        rank: 3\n        of: previous\n")));
        Assertions.assertFalse(previous.getDiagnostics().hasErrors(), previous::describe);
        Assertions.assertEquals("grade_embed_all_prev_lag1", column(previous, "grade_embed_0").getCoordinates().get("applied"));
        Assertions.assertNotEquals(plan.getHash(), previous.getHash());
        // of: [current, previous] — one block, one fit, both values read from it; the previous value's columns are marked
        final FeaturePlan both = compile(SOURCES, withEncoding(SPECTRAL_BLOCK.replace("        rank: 3\n", "        rank: 3\n        of: [current, previous]\n")));
        Assertions.assertFalse(both.getDiagnostics().hasErrors(), both::describe);
        Assertions.assertEquals("condition_grade", column(both, "grade_embed_2").getCoordinates().get("applied"));
        Assertions.assertEquals("grade_embed_all_prev_lag1", column(both, "grade_embed_prev_2").getCoordinates().get("applied"));
        Assertions.assertEquals("2", column(both, "grade_embed_prev_2").getCoordinates().get("component"));
        Assertions.assertEquals(1, both.getStages().stream().filter(s -> s.kind() == FeaturePlan.StageKind.fit).count(), both::describe);
        Assertions.assertEquals(1, FeatureStages.spectralSpecs(both.getColumns()).size(), "one fit for both embedded values");
        Assertions.assertNotEquals(plan.getHash(), both.getHash());
        // the list is a set: written the other way round it is the same block — the same columns in the same order
        final FeaturePlan reversed = compile(SOURCES, withEncoding(SPECTRAL_BLOCK.replace("        rank: 3\n", "        rank: 3\n        of: [previous, current]\n")));
        Assertions.assertEquals(both.getColumns().stream().map(OutputColumn::getCanonicalName).toList(),
                reversed.getColumns().stream().map(OutputColumn::getCanonicalName).toList(), reversed::describe);
        // under forward the two values are still one fit — one chain of aligned fits, not two
        final FeaturePlan forwardBoth = compile(SOURCES, withEncoding(SPECTRAL_BLOCK
                .replace("        rank: 3\n", "        rank: 3\n        of: [current, previous]\n")
                .replace("fit: {artifact", "fit: {mode: forward, blocks: {size: P7D}, artifact")));
        Assertions.assertFalse(forwardBoth.getDiagnostics().hasErrors(), forwardBoth::describe);
        Assertions.assertEquals(1, forwardBoth.getStages().stream().filter(s -> s.kind() == FeaturePlan.StageKind.fit).count(), forwardBoth::describe);
        Assertions.assertEquals(1, FeatureStages.spectralSpecs(forwardBoth.getColumns()).size(), "one forward chain for both embedded values");
        // a single previous keeps the plain names; a repeated value and too many columns are refused
        Assertions.assertNull(previous.getColumn("grade_embed_prev_0"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(SPECTRAL_BLOCK.replace("rank: 3", "rank: 3\n        of: [current, current]"))), "spectralEmbedding.of"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(SPECTRAL_BLOCK.replace("rank: 3", "rank: 3\n        of: [current, previous]\n        maxFeatures: 4"))), "spectralEmbedding.maxFeatures"));
        // an entry in another shape, or an empty list, is reported instead of silently embedding the row's own value
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(SPECTRAL_BLOCK.replace("rank: 3", "rank: 3\n        of: [current, {a: 1}]"))), "spectralEmbedding.parameters"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(SPECTRAL_BLOCK.replace("rank: 3", "rank: 3\n        of: []"))), "spectralEmbedding.parameters"));
        final FeaturePlan forward = compile(SOURCES, withEncoding(SPECTRAL_BLOCK.replace("fit: {artifact", "fit: {mode: forward, blocks: {size: P7D}, artifact")));
        Assertions.assertFalse(forward.getDiagnostics().hasErrors(), forward::describe);
        Assertions.assertEquals("forward", column(forward, "grade_embed_0").getCoordinates().get("fit"));
        Assertions.assertEquals("0", column(forward, "grade_embed_0").getCoordinates().get("forwardLagMillis"));

        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(SPECTRAL_BLOCK.replace("window: 2", "window: 0"))), "spectralEmbedding.cooccur"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(SPECTRAL_BLOCK.replace("weighting: ppmi", "weighting: tfidf"))), "spectralEmbedding.cooccur"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(SPECTRAL_BLOCK.replace("window: 2,", "window: 2, decay: 1,"))), "spectralEmbedding.parameters"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(SPECTRAL_BLOCK.replace("rank: 3", "rank: 0"))), "spectralEmbedding.rank"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(SPECTRAL_BLOCK.replace("rank: 3", "rank: 3\n        maxValues: 5000"))), "spectralEmbedding.maxValues"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(SPECTRAL_BLOCK.replace("rank: 3", "rank: 3\n        of: next"))), "spectralEmbedding.of"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(SPECTRAL_BLOCK.replace("fit: {artifact", "fit: {mode: fold, artifact"))), "spectralEmbedding.fit.mode"));
        // an emit of readouts only is as foreign as one of values: it would otherwise be parsed and dropped in silence
        for (final String foreign : List.of("order: 2", "emit: [distribution]", "emit: [entropy]", "emit: [{toValueProb: good}]", "blend: {priorWeight: 5}")) {
            Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(SPECTRAL_BLOCK.replace("        rank: 3\n", "        rank: 3\n        " + foreign + "\n"))),
                    "spectralEmbedding.parameters"), foreign);
        }
    }

    private static final String QUANTILE_BLOCK = """
                  - name: enc
                    scope: population
                    type: encoding
                    keySets:
                      - keys: [seller_id]
                    targets:
                      - {field: final_price, stats: [quantile, q25, quantile90]}
            """;

    @Test
    public void testQuantileStat() {
        final FeaturePlan plan = compile(SOURCES, withEncoding(QUANTILE_BLOCK));
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        for (final String stat : List.of("quantile", "q25", "quantile90")) {
            final OutputColumn c = column(plan, "enc__seller_id__final_price__" + stat);
            Assertions.assertEquals("encoding", c.getOperator());
            Assertions.assertEquals(stat, c.getCoordinates().get("stat"));
            Assertions.assertEquals("expanding", c.getCoordinates().get("fit"));
            Assertions.assertEquals(Schema.FieldType.FLOAT64.getType(), c.getFieldType().getType());
        }
        Assertions.assertEquals(0.5, OperatorCatalog.quantileProbability("quantile"));
        Assertions.assertEquals(0.25, OperatorCatalog.quantileProbability("q25"));
        Assertions.assertEquals(0.9, OperatorCatalog.quantileProbability("quantile90"));
        Assertions.assertNull(OperatorCatalog.quantileProbability("q101"));
        Assertions.assertNull(OperatorCatalog.quantileProbability("mean"));
        // needs the per-key value distribution: rejected in the lookup fit modes, unknown tokens are unknown stats
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(QUANTILE_BLOCK.replace("type: encoding", "type: encoding\n        fit: {mode: static}"))), "encoding.stat.static"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(QUANTILE_BLOCK.replace("type: encoding", "type: encoding\n        fit: {mode: fold}"))), "encoding.stat.static"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(QUANTILE_BLOCK.replace("quantile90", "q101"))), "encoding.stat"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withEncoding(QUANTILE_BLOCK.replace("field: final_price, ", ""))), "encoding.stat.target"));
    }

    @Test
    public void testPredictAtLiteralEventTime() {
        // predictAt: "event_time" is the literal event time (offset 0), not the pre-event keyword
        final FeaturePlan plan = compile(SOURCES, SPEC.replace("predictAt: \"event_time - PT8M\"", "predictAt: \"event_time\""));
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        final OutputColumn lag = column(plan, "recent_n5_sold_lag1");
        Assertions.assertEquals(OutputColumn.Status.windowShift, lag.getStatus());
        Assertions.assertEquals(Duration.ofDays(6).plusMinutes(30), lag.getWindowShift());
        Assertions.assertEquals(OutputColumn.Status.staticSafe, column(plan, "recent_n5_start_price_lag1").getStatus());
        Assertions.assertEquals(Duration.ZERO, AvailableAt.parseTimeExpression("event_time").getOffset());
        Assertions.assertTrue(AvailableAt.parse("atEventTime", null).isPreEvent());
    }

    @Test
    public void testScientificNotationIsNotAReference() {
        final FeaturePlan plan = compile(SOURCES, SPEC.replace("expr: \"start_price / quantity\"", "expr: \"start_price / 1e6 + 2E3 * quantity\""));
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        Assertions.assertEquals(List.of("start_price", "quantity"), List.copyOf(column(plan, "price_per_unit").getInputs()));
    }

    @Test
    public void testFeatureMustNotShadowInputField() {
        final FeaturePlan plan = compile(SOURCES, SPEC.replace("- name: price_per_unit", "- name: quantity"));
        Assertions.assertTrue(hasCode(plan, "column.shadowsInput"), plan::describe);
    }

    @Test
    public void testDatetimeOnDateField() {
        final String sources = SOURCES.replace("- {name: condition_grade, type: string}", "- {name: condition_grade, type: string}\n      - {name: listed_on, type: date}");
        final String spec = SPEC.replace("from: listings}", "from: listings}\n  - {fields: [listed_on], from: listings}")
                .replace("input: session_time", "input: listed_on");
        final FeaturePlan plan = compile(sources, spec);
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        Assertions.assertEquals("date", column(plan, "time_parts_month_sin").getCoordinates().get("inputType"));
        Assertions.assertTrue(hasCode(compile(sources, spec.replace("derive: [month, dayOfWeek]", "derive: [hour]")), "row.datetime.derive"));
        Assertions.assertTrue(hasCode(compile(SOURCES, SPEC.replace("input: session_time", "input: start_price")), "row.datetime.input"));
    }

    @Test
    public void testArtifactSettingsDoNotChangeTheHash() {
        final String base = SPEC.replace("output:\n  prefix: f_", "fit: {mode: static, artifact: {uri: \"gs://a\", refit: false}}\noutput:\n  prefix: f_");
        final String refit = base.replace("refit: false", "refit: true").replace("gs://a", "gs://b");
        final FeaturePlan a = compile(SOURCES, base);
        final FeaturePlan b = compile(SOURCES, refit);
        Assertions.assertEquals(a.getHash(), b.getHash());
        Assertions.assertEquals(a.getHash(), a.getArtifactVersion());
        final FeaturePlan pinned = compile(SOURCES, base.replace("refit: false", "refit: false, id: v42"));
        Assertions.assertEquals("v42", pinned.getArtifactVersion());
        // engine knobs do not change the plan either: tuning the spill budget must not invalidate an artifact
        final FeaturePlan tuned = compile(SOURCES, base + "engine: {spill: {memoryMB: 8, compress: true}}\n");
        Assertions.assertEquals(a.getHash(), tuned.getHash(), tuned::describe);
        Assertions.assertEquals(8, tuned.getSpec().engine.spillMemoryMB);
        Assertions.assertTrue(hasCode(compile(SOURCES, base + "engine: {spill: {memoryMB: '64MB'}}\n"), "engine.spill.memoryMB"));
        Assertions.assertTrue(hasCode(compile(SOURCES, base + "engine: {spill: {memoryMB: 0}}\n"), "engine.spill.memoryMB"));
        Assertions.assertTrue(hasCode(compile(SOURCES, SPEC.replace("output:\n  prefix: f_", "fit: {minHistory: P30D}\noutput:\n  prefix: f_")), "fit.minHistory"));
    }

    @Test
    public void testEqualityFilterReducesToStageKey() {
        final String spec = SPEC.replace("- {maxEvents: 5}", "- {filter: \"condition_grade = $self.condition_grade\"}");
        final FeaturePlan plan = compile(SOURCES, spec);
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        Assertions.assertTrue(hasCode(plan, "sequence.filter.reduced"));
        final OutputColumn lag = column(plan, "recent_all_start_price_lag1");
        Assertions.assertEquals("seller_id,condition_grade", lag.getCoordinates().get("stageKeys"));
        Assertions.assertNull(lag.getCoordinates().get("filter"));
        Assertions.assertTrue(plan.getStages().stream().anyMatch(s -> s.keys().equals(List.of("seller_id", "condition_grade"))), plan::describe);

        // an outcome-derived filter field must NOT become a key (its value is unknown at the past row's key time)
        final String outcomeFilter = SPEC.replace("- {maxEvents: 5}", "- {filter: \"sold = $self.sold\"}");
        final FeaturePlan kept = compile(SOURCES, outcomeFilter);
        Assertions.assertEquals("sold = $self.sold", column(kept, "recent_all_start_price_lag1").getCoordinates().get("filter"));
    }

    @Test
    public void testFieldlessCountAndRowStringOps() {
        final String spec = SPEC
                .replace("- {type: lag, fields: [sold, start_price], k: 2}", "- {type: lag, fields: [sold, start_price], k: 2}\n      - {type: aggregate, funcs: [count]}")
                .replace("- name: price_per_unit\n    scope: row\n    expr: \"start_price / quantity\"",
                        "- name: price_per_unit\n    scope: row\n    expr: \"start_price / quantity\"\n  - {name: grade_is, scope: row, type: indicator, input: condition_grade, values: [good, fair]}\n  - {name: same_grade, scope: row, type: equals, inputs: [condition_grade, category]}");
        final FeaturePlan plan = compile(SOURCES, spec);
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        // COUNT(1): counts every visible row (keys as past inputs → no shift for pre-event keys)
        final OutputColumn countAll = column(plan, "recent_n5_count");
        Assertions.assertEquals(OutputColumn.Status.staticSafe, countAll.getStatus());
        Assertions.assertNull(countAll.getCoordinates().get("field"));
        Assertions.assertEquals(Schema.Type.int64, column(plan, "grade_is_good").getFieldType().getType());
        column(plan, "grade_is_fair");
        column(plan, "same_grade");
        Assertions.assertTrue(hasCode(compile(SOURCES, spec.replace("values: [good, fair]", "values: []")), "row.indicator.values"));
    }

    @Test
    public void testCascadedDiagnosticsAndDeferredViolation() {
        // paceX is not declared in any source: the lineage error is the root cause
        final String spec = SPEC
                .replace("- {fields: [sold, final_price], from: auction_results}", "- {fields: [sold, final_price, paceX], from: auction_results}")
                .replace("expr: \"start_price / quantity\"", "expr: \"paceX * 2\"");
        final FeaturePlan plan = compile(SOURCES, spec);
        Assertions.assertTrue(hasCode(plan, "lineage.field"));
        // the dependent block is reported as caused-by info, not as another error
        final long unresolvedErrors = plan.getDiagnostics().get(Diagnostics.Level.error).stream()
                .filter(m -> m.code().equals("reference.unresolved")).count();
        Assertions.assertEquals(0, unresolvedErrors, plan::describe);
        Assertions.assertTrue(plan.getDiagnostics().get(Diagnostics.Level.info).stream()
                .anyMatch(m -> m.code().equals("reference.unresolved") && m.message().contains("paceX")), plan::describe);
        // availability verdicts are deferred while blocks are unresolved (no misleading violation errors)
        Assertions.assertFalse(hasCode(plan, "availability.violation"), plan::describe);
    }

    @Test
    public void testGroupedChildName() {
        final String spec = SPEC.replace("prefix: f_", "prefix: f_\n  groupBy: session\n  childName: entries");
        final FeaturePlan plan = compile(SOURCES, spec);
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        Assertions.assertEquals("entries", plan.getSpec().output.childName);
        Assertions.assertTrue(hasCode(compile(SOURCES, spec.replace("childName: entries", "childName: quantity")), "output.childName"));
    }

    @Test
    public void testReviewRegressions() {
        // a numeric time.field is rejected (a date field would otherwise be read as microseconds)
        final String numericTime = SPEC.replace("time: {field: session_time, orderTieBreak: [session_id]}", "time: {field: start_price}");
        Assertions.assertTrue(hasCode(compile(SOURCES, numericTime), "time.field.type"));
        // date-typed time values are epoch days
        Assertions.assertEquals(20692L * 86_400_000L, FeatureValues.toEpochMillis(20692, "date"));
        Assertions.assertEquals(1_000L, FeatureValues.toEpochMillis(1_000_000L, "timestamp"));

        // composite keys are length-prefixed: values containing the separator cannot collide
        final java.util.Map<String, Object> r1 = java.util.Map.of("a", "1:x", "b", "y");
        final java.util.Map<String, Object> r2 = java.util.Map.of("a", "1", "b", "x:y");
        Assertions.assertNotEquals(FeatureValues.key(r1, List.of("a", "b")), FeatureValues.key(r2, List.of("a", "b")));

        // shareOfTotal with excludeSelf uses the others' total
        Assertions.assertEquals(0.25, (Double) ContextEvaluator.apply("shareOfTotal", List.of(2.0, 3.0, 5.0), 0, true), 1e-9);
        Assertions.assertEquals(0.2, (Double) ContextEvaluator.apply("shareOfTotal", List.of(2.0, 3.0, 5.0), 0, false), 1e-9);

        // quantile is an expanding-only statistic (see testQuantileStat); an unknown token is a compile error rather than a runtime crash
        Assertions.assertFalse(compile(SOURCES, SPEC.replace("stats: [mean]", "stats: [quantile]")).getDiagnostics().hasErrors());
        Assertions.assertTrue(hasCode(compile(SOURCES, SPEC.replace("stats: [mean]", "stats: [percentile]")), "encoding.stat"));

        // parent placement requires the grouping context: a non-groupBy context stays on the children
        final String twoContexts = SPEC
                .replace("contexts:\n  - {name: session, keys: [session_id]}", "contexts:\n  - {name: session, keys: [session_id]}\n  - {name: cat, keys: [category]}")
                .replace("prefix: f_", "prefix: f_\n  groupBy: session");
        final FeaturePlan grouped = compile(SOURCES, twoContexts.replace("context: session\n    ops:\n      - {type: countByValue", "context: cat\n    ops:\n      - {type: countByValue"));
        Assertions.assertFalse(grouped.getDiagnostics().hasErrors(), grouped::describe);
        Assertions.assertEquals(OutputColumn.Placement.child, column(grouped, "composition_condition_grade_countByValue").getPlacement());

        // the pipeline-level name must not disable step selection in validate()
        final com.google.gson.JsonObject request = new com.google.gson.JsonObject();
        request.addProperty("name", "my-pipeline");
        final com.google.gson.JsonArray transforms = new com.google.gson.JsonArray();
        final com.google.gson.JsonObject step = new com.google.gson.JsonObject();
        step.addProperty("name", "features");
        step.addProperty("module", "feature");
        final com.google.gson.JsonObject parameters = Config.convertConfigJson(SPEC, Config.Format.yaml);
        parameters.add("sources", Config.convertConfigJson(SOURCES, Config.Format.yaml));
        step.add("parameters", parameters);
        transforms.add(step);
        request.add("transforms", transforms);
        final com.google.gson.JsonObject response = FeaturePlanService.validate(request);
        Assertions.assertTrue(response.get("ok").getAsBoolean(), response::toString);
    }

    @Test
    public void testAdditiveLeaveNodeOutSubtractsLeafFromMainEffects() {
        // cell (n=2, Σ=2) inside main effect A (n=4, Σ=2) and root (n=8, Σ=2), λ=2, identity scale:
        // with leave-node-out the cell's rows leave A and the root, so the additive parent is 0 and the
        // composed estimate is 0 + 0.5 · (1 − 0) = 0.5 (without the fix the leaked parent gives ~0.708)
        final Shrinkage shrinkage = Shrinkage.of(Shrinkage.Scale.identity, 2, true);
        final List<Shrinkage.Level> main = List.of(
                new Shrinkage.Level("A", "a_n", "a_sum", null),
                new Shrinkage.Level("global", "g_n", "g_sum", null));
        final List<Shrinkage.Level> levels = List.of(
                new Shrinkage.Level("cell", "c_n", "c_sum", null),
                new Shrinkage.Level(Shrinkage.ADDITIVE, null, null, List.of(main)),
                new Shrinkage.Level("global", "g_n", "g_sum", null));
        final java.util.Map<String, Object> row = java.util.Map.of(
                "c_n", 2.0, "c_sum", 2.0, "a_n", 4.0, "a_sum", 2.0, "g_n", 8.0, "g_sum", 2.0);
        Assertions.assertEquals(0.5, shrinkage.compose(row, levels).value(), 1e-9);
    }

    @Test
    public void testLambdaFromMoments() {
        // keys A: [1,1,1,0], B: [0,0,0,1] → σ² = 0.25, τ² = 0.0625 → λ = 4
        Assertions.assertEquals(4.0, Shrinkage.lambdaFromMoments(2, 8, 4, 4, 2.5, 32), 1e-9);
        // identical key means → τ² ≤ 0 → full shrinkage
        Assertions.assertTrue(Double.isInfinite(Shrinkage.lambdaFromMoments(2, 4, 2, 2, 1.0, 8)));
        // no within-key variance → λ = 0 (no shrinkage)
        Assertions.assertEquals(0.0, Shrinkage.lambdaFromMoments(2, 6, 3, 3, 3.0, 18), 1e-9);
        // a single key cannot be estimated
        Assertions.assertNull(Shrinkage.lambdaFromMoments(1, 6, 3, 3, 1.5, 36));
    }

    @Test
    public void testHotKeyAuditQueries() {
        final FeaturePlan plan = compile(SOURCES, SPEC);
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        final List<FeaturePlan.AuditQuery> audit = plan.getAuditQueries();
        // one query per distinct key set of the keyed stages, in stage order; row / fit stages contribute nothing
        final List<List<String>> keySets = audit.stream().map(FeaturePlan.AuditQuery::keys).toList();
        Assertions.assertEquals(keySets.size(), keySets.stream().distinct().count());
        Assertions.assertTrue(keySets.contains(List.of("session_id")), keySets::toString);
        Assertions.assertTrue(keySets.contains(List.of("seller_id")), keySets::toString);
        Assertions.assertTrue(keySets.contains(List.of()), keySets::toString); // global level (share / shrinkage prior)
        final FeaturePlan.AuditQuery seller = audit.stream().filter(q -> q.keys().equals(List.of("seller_id"))).findFirst().orElseThrow();
        Assertions.assertEquals("SELECT seller_id, COUNT(1) AS row_count FROM {input} WHERE seller_id IS NOT NULL"
                + " GROUP BY seller_id ORDER BY row_count DESC LIMIT 20", seller.sql());
        Assertions.assertFalse(seller.stages().isEmpty());
        Assertions.assertFalse(seller.note().contains("intermediate"));
        final FeaturePlan.AuditQuery global = audit.stream().filter(q -> q.keys().isEmpty()).findFirst().orElseThrow();
        Assertions.assertEquals("SELECT COUNT(1) AS row_count FROM {input}", global.sql());
        Assertions.assertTrue(plan.describe().contains("-- audit"));
        Assertions.assertEquals(audit.size(), plan.toJson().getAsJsonArray("audit").size());
        // row-only plans have no keyed stage → no audit queries
        final FeaturePlan rowOnly = compile(SOURCES, """
                lineage:
                  - {fields: [session_id, seller_id, category, quantity, start_price], from: listings}
                time: {field: session_time}
                predictAt: "event_time - PT8M"
                features:
                  - {name: unit, scope: row, expr: "start_price / quantity"}
                """);
        Assertions.assertFalse(rowOnly.getDiagnostics().hasErrors(), rowOnly::describe);
        Assertions.assertTrue(rowOnly.getAuditQueries().isEmpty());
        Assertions.assertFalse(rowOnly.describe().contains("-- audit"));
    }

    @Test
    public void testReservedWordColumnsInConditionsAreQuoted() {
        final String sources = """
                sources:
                  - name: results
                    eventTime: event_time
                    settlementLag: PT30M
                    keys: [event_id, subject_id]
                    fields:
                      - {name: event_id, type: string}
                      - {name: subject_id, type: string}
                      - {name: rank, type: int32, availableAt: after(event), kind: outcome}
                      - {name: score, type: float64}
                """;
        final String spec = """
                lineage:
                  - {fields: [event_id, subject_id, rank, score], from: results}
                time: {field: event_time, orderTieBreak: [event_id]}
                predictAt: "event_time - PT1H"
                entities:
                  - {name: subject, keys: [subject_id]}
                features:
                  - name: recent
                    scope: sequence
                    entity: subject
                    windows: [{maxEvents: 5}, {maxEvents: 10, filter: "score > 1 AND rank <= 3"}]
                    ops:
                      - {type: countMatch, predicate: "rank <= 3", as: top3}
                      - {type: sinceEvent, predicate: "rank = 1", unit: [days], as: since_win}
                """;
        final FeaturePlan plan = compile(sources, spec);
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        // `rank` is a keyword of the condition grammar: the compiler quotes it and records the rewrite
        Assertions.assertTrue(hasCode(plan, "predicate.quoted"), plan::describe);
        Assertions.assertTrue(hasCode(plan, "filter.quoted"), plan::describe);
        final OutputColumn top3 = column(plan, "recent_n5_top3");            // `as` replaces the op suffix
        Assertions.assertEquals("`rank` <= 3", top3.getCoordinates().get("predicate"));
        // every known column of the condition is quoted (harmless for non-keywords)
        Assertions.assertEquals("`score` > 1 AND `rank` <= 3", column(plan, "recent_n10_top3").getCoordinates().get("filter"));
        column(plan, "recent_n5_since_win");
        Assertions.assertEquals("`rank` = 1", column(plan, "recent_n5_since_win").getCoordinates().get("predicate"));
        // the n10 window has a filter and no maxAge: its columns pin the whole history of a key (S5 hint);
        // the n5 window without a filter is bounded by maxEvents
        final List<String> unbounded = plan.getDiagnostics().getMessages().stream()
                .filter(m -> m.code().equals("sequence.window.unbounded")).map(m -> m.message()).toList();
        Assertions.assertEquals(2, unbounded.size(), plan::describe);
        Assertions.assertTrue(unbounded.stream().allMatch(m -> m.contains("recent_n10_")), unbounded::toString);
        // the evaluator parses the rewritten text
        Assertions.assertDoesNotThrow(() -> com.mercari.solution.util.pipeline.Filter.parse(top3.getCoordinates().get("predicate")));
        // a condition that cannot be parsed even when quoted is a compile error, not a worker failure
        final FeaturePlan broken = compile(sources, spec.replace("predicate: \"rank <= 3\"", "predicate: \"rank <= = 3\""));
        Assertions.assertTrue(hasCode(broken, "predicate.parse"), broken::describe);
    }

    @Test
    public void testAliasesAndPerValueContextColumns() {
        final String block = """
                  - name: enc
                    scope: population
                    type: encoding
                    keySets:
                      - keys: [seller_id]
                    targets:
                      - {expr: "sold >= 1", stats: [mean], as: win}
                      - {field: sold, stats: [count]}
            """;
        final String spec = withEncoding(block)
                .replace("- {type: countByValue, fields: [condition_grade]}", "- {type: countByValue, fields: [condition_grade], values: [good, fair]}\n      - {type: ratioByValue, fields: [condition_grade], values: [good], as: grade}")
                .replace("ops:\n      - {type: lag, fields: [sold, start_price], k: 2}", "ops:\n      - {type: ewma, expr: \"start_price * 2\", halflife: [3], as: price2}\n      - {type: lag, fields: [sold, start_price], k: 2}");
        Assertions.assertNotEquals(SPEC, spec);
        final FeaturePlan plan = compile(SOURCES, spec);
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        // target alias replaces the anonymous e{n}
        column(plan, "enc__seller_id__win__mean");
        Assertions.assertNull(plan.getColumn("enc__seller_id__e1__mean"));
        // op alias replaces the anonymous expression segment
        column(plan, "recent_n5_price2_ewma3");
        // countByValue with values: one INT64 column per value instead of a map; ratioByValue likewise (FLOAT64)
        final OutputColumn good = column(plan, "composition_condition_grade_countByValue_good");
        Assertions.assertEquals(Schema.Type.int64, good.getFieldType().getType());
        Assertions.assertEquals("good", good.getCoordinates().get("value"));
        column(plan, "composition_condition_grade_countByValue_fair");
        Assertions.assertNull(plan.getColumn("composition_condition_grade_countByValue"));
        Assertions.assertEquals(Schema.Type.float64, column(plan, "composition_grade_ratioByValue_good").getFieldType().getType());
        // output.passThrough is validated (a typo must not silently pass every input column through)
        Assertions.assertTrue(hasCode(compile(SOURCES, SPEC.replace("output:\n  prefix: f_", "output:\n  prefix: f_\n  passThrough: keysOnly")), "output.passThrough"));
        Assertions.assertFalse(hasCode(compile(SOURCES, SPEC.replace("output:\n  prefix: f_", "output:\n  prefix: f_\n  passThrough: keys")), "output.passThrough"));
        // the outcome-mean hint is reported once per block, not once per window x field x func
        final long hints = plan.getDiagnostics().getMessages().stream().filter(m -> m.code().equals("sequence.aggregate.encoding")).count();
        Assertions.assertTrue(hints <= 1, plan::describe);
    }

    @Test
    public void testAvailableAtAlgebra() {
        final AvailableAt a = AvailableAt.parse("event_time - PT10M", null);
        final AvailableAt b = AvailableAt.parse("after(event)", Duration.ofMinutes(30));
        Assertions.assertEquals(Duration.ofMinutes(30), AvailableAt.max(a, b).getOffset());
        Assertions.assertTrue(a.isStaticallyAtOrBefore(b));
        final AvailableAt dynamic = AvailableAt.parse("atRowCreation", null);
        Assertions.assertFalse(AvailableAt.max(a, dynamic).isStatic());
        Assertions.assertTrue(AvailableAt.max(b, dynamic).isProvablyAfter(a));
        Assertions.assertEquals(Duration.ofDays(730), Durations.parse("P2Y"));
        Assertions.assertEquals("365d", Durations.shortName(Durations.parse("P365D")));
        Assertions.assertEquals("10m", Durations.shortName(Durations.parse("PT10M")));
    }

    // ------------------------------------------------------------------------------------------
    // output contract: roles / include / manifest hashes, and the observedAt audit entries
    // ------------------------------------------------------------------------------------------

    private static final String OUTPUT_CONTRACT = "output:\n  prefix: f_\n"
            + "  roles: {group: session, time: session_time, entity: seller_id, label: sold, baseline: market}\n"
            + "  include: [price_per_unit, f_relative_start_price_rank, nope]\n"
            + "  manifest: target/feature-manifests/test/manifest.json\n";

    @Test
    public void testEmptyIncludeIsAnError() {
        // a screening step that passed nothing: the table would carry no feature column
        final FeaturePlan plan = compile(SOURCES, SPEC.replace("output:\n  prefix: f_\n", "output:\n  prefix: f_\n  include: []\n"));
        Assertions.assertTrue(plan.getDiagnostics().hasErrors(), plan::describe);
        Assertions.assertTrue(hasCode(plan, "output.include.empty"), plan::describe);
    }

    @Test
    public void testOutputRolesAndInclude() {
        final FeaturePlan plan = compile(SOURCES, SPEC.replace("output:\n  prefix: f_\n", OUTPUT_CONTRACT));
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        // include is the projection: only the listed columns are emitted, by canonical or output name
        Assertions.assertEquals(List.of("f_price_per_unit", "f_relative_start_price_rank"),
                plan.getEmittedColumns().stream().map(OutputColumn::getOutputName).toList());
        Assertions.assertTrue(hasCode(plan, "output.include.unknown"), plan::describe);
        // the baseline role names an intermediate column: reported, not an error (baselines[].emit is the follow-up)
        Assertions.assertTrue(hasCode(plan, "output.roles.baseline.notEmitted"), plan::describe);
        Assertions.assertEquals("session", plan.getSpec().output.roles.get("group"));
        Assertions.assertEquals("sold", plan.getRoleColumns().get("label"));
        Assertions.assertNull(plan.getRoleColumns().get("group")); // a context, not a column
        Assertions.assertTrue(plan.describe().contains("-- output contract"), plan::describe);
        final JsonObject json = plan.toJson();
        Assertions.assertEquals("sold", json.getAsJsonObject("roles").getAsJsonObject("label").get("column").getAsString());
        Assertions.assertEquals(3, json.getAsJsonObject("include").getAsJsonArray("listed").size());
    }

    @Test
    public void testIncludeKeepsRoleColumns() {
        // a pass list never names a role column (roles were never candidates): the baseline's emitted copy and a
        // label derived as a column stay emitted under the projection, and the report says which were kept
        final String spec = SPEC.replace("output:\n  prefix: f_\n", OUTPUT_CONTRACT.replace("label: sold", "label: price_per_unit").replace("include: [price_per_unit, ", "include: ["))
                .replace("baselines:\n  - {name: market, context: session, expr: \"share(1 / current_bid_t10)\"}",
                        "baselines:\n  - {name: market, context: session, expr: \"share(1 / current_bid_t10)\", emit: marketProb}");
        final FeaturePlan plan = compile(SOURCES, spec);
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        Assertions.assertEquals(Set.of("f_marketProb", "f_price_per_unit", "f_relative_start_price_rank"),
                plan.getEmittedColumns().stream().map(OutputColumn::getOutputName).collect(java.util.stream.Collectors.toSet()));
        Assertions.assertTrue(hasCode(plan, "output.include.role"), plan::describe);
        Assertions.assertFalse(hasCode(plan, "output.roles.baseline.notEmitted"), plan::describe);
        Assertions.assertEquals("f_marketProb", plan.getRoleColumns().get("baseline"));
        Assertions.assertEquals("f_price_per_unit", plan.getRoleColumns().get("label"));
        final JsonObject manifest = plan.toManifest(List.of(), Map.of());
        Assertions.assertEquals("baseline", manifest.getAsJsonArray("columns").asList().stream().map(JsonElement::getAsJsonObject)
                .filter(c -> "f_marketProb".equals(c.get("name").getAsString())).findFirst().orElseThrow().get("role").getAsString());
        // without a role the same copy column is projected away like any feature
        final FeaturePlan noRole = compile(SOURCES, spec.replace(", baseline: market}", "}"));
        Assertions.assertFalse(noRole.getEmittedColumns().stream().anyMatch(c -> c.getOutputName().equals("f_marketProb")), noRole::describe);
        // the label role still keeps its column; the kept list no longer mentions the copy
        final String kept = noRole.getDiagnostics().getMessages().stream().filter(m -> m.code().equals("output.include.role")).findFirst().orElseThrow().message();
        Assertions.assertTrue(kept.contains("f_price_per_unit (label)") && !kept.contains("marketProb"), kept);
    }

    @Test
    public void testRoleColumnsSurviveExcludeAndCarryNoIndicator() {
        final String emit = SPEC.replace("baselines:\n  - {name: market, context: session, expr: \"share(1 / current_bid_t10)\"}",
                "baselines:\n  - {name: market, context: session, expr: \"share(1 / current_bid_t10)\", emit: marketProb}");
        // exclude follows the include rule: the market baseline's emitted copy survives derivedFrom:market (the
        // features derived from the market field do not), and the report says so
        final FeaturePlan excluded = compile(SOURCES, emit.replace("output:\n  prefix: f_\n",
                "output:\n  prefix: f_\n  nullPolicy: indicator\n  roles: {baseline: market}\n  exclude: [\"derivedFrom:market\"]\n"));
        Assertions.assertFalse(excluded.getDiagnostics().hasErrors(), excluded::describe);
        Assertions.assertFalse(column(excluded, "marketProb").isIntermediate(), excluded::describe);
        Assertions.assertEquals("baseline", column(excluded, "marketProb").getRole());
        Assertions.assertTrue(column(excluded, "relative_current_bid_t10_rank").isIntermediate(), excluded::describe);
        Assertions.assertTrue(hasCode(excluded, "output.exclude.role"), excluded::describe);
        Assertions.assertFalse(hasCode(excluded, "output.roles.baseline.notEmitted"), excluded::describe);
        Assertions.assertEquals("f_marketProb", excluded.getRoleColumns().get("baseline"));
        // a column kept only as a role gets no indicator (its validFor would trigger one): the flag would be a
        // feature column the projection never admitted
        Assertions.assertNull(excluded.getColumn("marketProb_isnull"), excluded::describe);
        final FeaturePlan roleOnly = compile(SOURCES, emit.replace("output:\n  prefix: f_\n",
                "output:\n  prefix: f_\n  nullPolicy: indicator\n  roles: {baseline: market}\n  include: [price_per_unit]\n"));
        Assertions.assertNull(roleOnly.getColumn("marketProb_isnull"), roleOnly::describe);
        Assertions.assertEquals("baseline", column(roleOnly, "marketProb").toField().getOptions().get("feature.role"));
        // the same column admitted by the projection keeps its indicator like any feature
        final FeaturePlan included = compile(SOURCES, emit.replace("output:\n  prefix: f_\n",
                "output:\n  prefix: f_\n  nullPolicy: indicator\n  roles: {baseline: market}\n  include: [marketProb, price_per_unit]\n"));
        Assertions.assertNotNull(included.getColumn("marketProb_isnull"), included::describe);
        Assertions.assertFalse(hasCode(included, "output.include.role"), included::describe);
    }

    @Test
    public void testGroupRoleNamingAContextNeverResolvesToAColumn() {
        // a feature that happens to share the context's name is not the group column: the role resolves to the
        // context's keys, the column is a feature the projection drops like any other
        final FeaturePlan plan = compile(SOURCES, SPEC.replace("features:\n", "features:\n  - {name: session, scope: row, expr: \"quantity * 2\"}\n")
                .replace("output:\n  prefix: f_\n", "output:\n  prefix: f_\n  roles: {group: session}\n  include: [price_per_unit]\n"));
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        Assertions.assertNull(column(plan, "session").getRole());
        Assertions.assertTrue(column(plan, "session").isIntermediate(), plan::describe);
        Assertions.assertNull(plan.getRoleColumns().get("group"));
        final JsonObject group = plan.toManifest(List.of(), Map.of()).getAsJsonObject("roles").getAsJsonObject("group");
        Assertions.assertTrue(group.get("column").isJsonNull(), group::toString);
        Assertions.assertEquals("session_id", group.getAsJsonArray("keys").get(0).getAsString());
    }

    @Test
    public void testPassThroughOptionsReplaceUpstreamLineage() {
        final FeaturePlan plan = compile(SOURCES, SPEC.replace("output:\n  prefix: f_\n", "output:\n  prefix: f_\n  roles: {label: sold, time: session_time}\n"));
        // an input that arrives from another feature transform: its feature.* options describe that table, except
        // the derivedFrom lineage, which is carried forward; non-feature options are untouched
        final Schema.Field upstream = Schema.Field.of("sold", Schema.FieldType.INT64).withOptions(new java.util.HashMap<>(Map.of(
                "feature.scope", "row", "feature.block", "upstream", "feature.role", "weight", "feature.derivedFrom", "market", "sqlType", "INT64")));
        final Map<String, String> options = plan.passThroughOptions(upstream);
        Assertions.assertEquals("input", options.get("feature.scope"));
        Assertions.assertEquals("outcome", options.get("feature.kind"));
        Assertions.assertEquals("market,outcome", options.get("feature.derivedFrom"));
        Assertions.assertEquals("auction_results", options.get("feature.sources"));
        Assertions.assertEquals("label", options.get("feature.role"));
        Assertions.assertFalse(options.containsKey("feature.block"));
        final Schema.Field field = FeatureStages.passThroughField(plan, upstream);
        Assertions.assertEquals("label", field.getOptions().get("feature.role"));
        Assertions.assertNull(field.getOptions().get("feature.block"));
        Assertions.assertEquals("INT64", field.getOptions().get("sqlType"));
        Assertions.assertEquals("row", upstream.getOptions().get("feature.scope")); // the input schema is not mutated
        // the manifest's fields entry is built from the same map
        final JsonObject entry = plan.toManifest(List.of(upstream), Map.of()).getAsJsonArray("fields").get(0).getAsJsonObject();
        Assertions.assertEquals("input", entry.get("scope").getAsString());
        Assertions.assertEquals("label", entry.get("role").getAsString());
        Assertions.assertEquals("auction_results", entry.get("source").getAsString());
        Assertions.assertEquals(List.of("market", "outcome"), entry.getAsJsonArray("derivedFrom").asList().stream().map(JsonElement::getAsString).toList());
        // no kind, no upstream lineage, no role: no derivedFrom at all (not an empty string) and no role
        final Map<String, String> plain = plan.passThroughOptions(Schema.Field.of("category", Schema.FieldType.STRING));
        Assertions.assertEquals("input", plain.get("feature.scope"));
        Assertions.assertFalse(plain.containsKey("feature.derivedFrom"));
        Assertions.assertFalse(plain.containsKey("feature.role"));
    }

    @Test
    public void testIncludeAndManifestAreOutsideThePlanHash() {
        final FeaturePlan base = compile(SOURCES, SPEC);
        final FeaturePlan projected = compile(SOURCES, SPEC.replace("output:\n  prefix: f_\n",
                "output:\n  prefix: f_\n  include: [price_per_unit]\n  includeHash: abc\n  manifest: gs://b/m.json\n"));
        // a projection does not change what is fitted: same plan hash (artifacts stay valid), different output hash
        Assertions.assertEquals(base.getHash(), projected.getHash());
        Assertions.assertNotEquals(base.getOutputHash(), projected.getOutputHash());
        Assertions.assertEquals(List.of("f_price_per_unit"), projected.getEmittedColumns().stream().map(OutputColumn::getOutputName).toList());
        // roles are part of the output hash too
        final FeaturePlan roles = compile(SOURCES, SPEC.replace("output:\n  prefix: f_\n", "output:\n  prefix: f_\n  roles: {label: sold}\n"));
        Assertions.assertNotEquals(base.getOutputHash(), roles.getOutputHash());
        // include replaces exclude
        final FeaturePlan both = compile(SOURCES, SPEC.replace("output:\n  prefix: f_\n",
                "output:\n  prefix: f_\n  include: [price_per_unit]\n  exclude: [price_per_unit]\n"));
        Assertions.assertTrue(hasCode(both, "output.include.exclude"));
        Assertions.assertEquals(1, both.getEmittedColumns().size());
    }

    @Test
    public void testOutputRolesDiagnostics() {
        Assertions.assertTrue(hasCode(compile(SOURCES, SPEC.replace("output:\n  prefix: f_\n", "output:\n  prefix: f_\n  roles: {rank: sold}\n")), "output.roles.unknown"));
        Assertions.assertTrue(hasCode(compile(SOURCES, SPEC.replace("output:\n  prefix: f_\n", "output:\n  prefix: f_\n  roles: {label: nope}\n")), "output.roles.unresolved"));
        Assertions.assertTrue(hasCode(compile(SOURCES, SPEC.replace("output:\n  prefix: f_\n", "output:\n  prefix: f_\n  roles: {group: nope}\n")), "output.roles.unresolved"));
        Assertions.assertTrue(hasCode(compile(SOURCES, SPEC.replace("output:\n  prefix: f_\n", "output:\n  prefix: f_\n  roles: {time: quantity}\n")), "output.roles.time"));
        // a URI must be resolved by FeaturePlanService before the compiler sees it
        Assertions.assertTrue(hasCode(compile(SOURCES, SPEC.replace("output:\n  prefix: f_\n", "output:\n  prefix: f_\n  include: gs://b/passed.json\n")), "output.include.unresolved"));
        Assertions.assertTrue(hasCode(compile(SOURCES, SPEC + "audit: {observedAt: maybe}\n"), "audit.observedAt"));
    }

    @Test
    public void testIncludeListParsing() {
        Assertions.assertEquals(List.of("a", "b"), FeaturePlanService.parseIncludeList("[\"a\", \"b\"]", "x"));
        Assertions.assertEquals(List.of("a", "b"), FeaturePlanService.parseIncludeList("{\"passed\": [\"a\", {\"name\": \"b\", \"gain\": 1}]}", "x"));
        Assertions.assertEquals(List.of("a", "b"), FeaturePlanService.parseIncludeList("# comment\na\n\nb\n", "x"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> FeaturePlanService.parseIncludeList("{\"other\": 1}", "x"));
        final JsonObject parameters = new JsonObject();
        final JsonObject output = new JsonObject();
        output.addProperty("include", "data:" + java.util.Base64.getEncoder().encodeToString("a\nb".getBytes()));
        parameters.add("output", output);
        FeaturePlanService.resolveInclude(parameters, null);
        Assertions.assertEquals(2, output.getAsJsonArray("include").size());
        Assertions.assertTrue(output.get("includeSource").getAsString().startsWith("data:"));
        Assertions.assertEquals(16, output.get("includeHash").getAsString().length());
    }

    private static List<Schema.Field> inputFields(final boolean withSnapshotTime) {
        final List<Schema.Field> fields = new java.util.ArrayList<>(List.of(
                Schema.Field.of("session_id", Schema.FieldType.STRING), Schema.Field.of("seller_id", Schema.FieldType.STRING),
                Schema.Field.of("category", Schema.FieldType.STRING), Schema.Field.of("quantity", Schema.FieldType.INT32),
                Schema.Field.of("start_price", Schema.FieldType.FLOAT64), Schema.Field.of("condition_grade", Schema.FieldType.STRING),
                Schema.Field.of("current_bid_t10", Schema.FieldType.FLOAT64), Schema.Field.of("sold", Schema.FieldType.INT32),
                Schema.Field.of("final_price", Schema.FieldType.FLOAT64), Schema.Field.of("session_time", Schema.FieldType.TIMESTAMP)));
        if (withSnapshotTime) fields.add(Schema.Field.of("snapshot_time", Schema.FieldType.TIMESTAMP));
        return fields;
    }

    @Test
    public void testObservedAtAuditEntries() {
        final JsonObject sourcesJson = Config.convertConfigJson(SOURCES, Config.Format.yaml);
        final JsonObject specJson = Config.convertConfigJson(SPEC, Config.Format.yaml);
        // observation column present: one runnable entry with the declared deadline (event_time - 10 min)
        final FeaturePlan plan = FeaturePlanCompiler.compile(sourcesJson, specJson, inputFields(true));
        Assertions.assertEquals(1, plan.getObservedAtAudits().size(), plan::describe);
        final FeaturePlan.ObservedAtAudit audit = plan.getObservedAtAudits().get(0);
        Assertions.assertEquals("current_bid_t10", audit.field());
        Assertions.assertEquals("snapshot_time", audit.observedAtField());
        Assertions.assertTrue(audit.present());
        Assertions.assertEquals(-10L * 60 * 1000, audit.deadlineOffsetMillis());
        Assertions.assertEquals(-8L * 60 * 1000, audit.predictAtOffsetMillis());
        Assertions.assertEquals(1, plan.getRunnableObservedAtAudits().size());
        Assertions.assertFalse(hasCode(plan, "sources.observedAt.missingInput"), plan::describe);
        Assertions.assertTrue(plan.describe().contains("-- observedAt audit"), plan::describe);
        Assertions.assertEquals(1, plan.toJson().getAsJsonArray("observedAtAudit").size());
        // observation column absent from the input: the declaration cannot be checked -> warning, entry not runnable
        final FeaturePlan missing = FeaturePlanCompiler.compile(sourcesJson, specJson, inputFields(false));
        Assertions.assertTrue(hasCode(missing, "sources.observedAt.missingInput"), missing::describe);
        Assertions.assertEquals(1, missing.getObservedAtAudits().size());
        Assertions.assertTrue(missing.getRunnableObservedAtAudits().isEmpty());
        // audit.observedAt: off keeps the entries in the report but the engine runs none
        final JsonObject off = specJson.deepCopy();
        final JsonObject auditSpec = new JsonObject();
        auditSpec.addProperty("observedAt", "off");
        off.add("audit", auditSpec);
        Assertions.assertTrue(FeaturePlanCompiler.compile(sourcesJson, off, inputFields(true)).getRunnableObservedAtAudits().isEmpty());
    }

    @Test
    public void testManifestContent() {
        final FeaturePlan plan = compile(SOURCES, SPEC.replace("output:\n  prefix: f_\n", OUTPUT_CONTRACT));
        final JsonObject manifest = plan.toManifest(List.of(Schema.Field.of("session_id", Schema.FieldType.STRING), Schema.Field.of("sold", Schema.FieldType.INT32)),
                java.util.Map.of("enc", "gs://b/artifacts/hash/enc.avro"));
        Assertions.assertEquals(plan.getHash(), manifest.get("planHash").getAsString());
        Assertions.assertEquals(plan.getOutputHash(), manifest.get("outputHash").getAsString());
        Assertions.assertEquals(2, manifest.getAsJsonArray("columns").size());
        final JsonObject first = manifest.getAsJsonArray("columns").get(0).getAsJsonObject();
        Assertions.assertEquals("f_price_per_unit", first.get("name").getAsString());
        Assertions.assertEquals("row", first.get("scope").getAsString());
        Assertions.assertFalse(first.get("categorical").getAsBoolean());
        Assertions.assertTrue(first.getAsJsonObject("lineage").has("derivedFrom"));
        // pass-through fields carry their contract and role
        final JsonObject sold = manifest.getAsJsonArray("fields").get(1).getAsJsonObject();
        Assertions.assertEquals("label", sold.get("role").getAsString());
        Assertions.assertEquals("auction_results", sold.get("source").getAsString());
        Assertions.assertEquals("outcome", sold.get("kind").getAsString());
        Assertions.assertEquals("gs://b/artifacts/hash/enc.avro", manifest.getAsJsonObject("artifacts").get("enc").getAsString());
        Assertions.assertTrue(manifest.getAsJsonObject("plan").has("stages"));
    }

    // ------------------------------------------------------------------------------------------
    // softmax / baseline emit / placebo ops (noise, shuffle)
    // ------------------------------------------------------------------------------------------

    private static final String PROB_BLOCK = """
      - name: prob
        scope: context
        context: session
        ops:
          - {type: softmax, field: price_per_unit, offset: market, temperature: 1.3, as: pWin}
    """;

    private static String withBlocks(final String blocks) {
        return SPEC.replace("output:\n", blocks + "output:\n");
    }

    @Test
    public void testSoftmaxExpansion() {
        final FeaturePlan plan = compile(SOURCES, withBlocks(PROB_BLOCK).replace("baselines:\n  - {name: market, context: session, expr: \"share(1 / current_bid_t10)\"}",
                "baselines:\n  - {name: market, context: session, expr: \"share(1 / current_bid_t10)\", emit: marketProb}"));
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        final OutputColumn p = column(plan, "prob_pWin_softmax");
        Assertions.assertEquals(Schema.FieldType.FLOAT64.getType(), p.getFieldType().getType());
        Assertions.assertEquals("price_per_unit", p.getCoordinates().get("field"));
        Assertions.assertEquals("__baseline_market", p.getCoordinates().get("offset"));
        Assertions.assertEquals("1.3", p.getCoordinates().get("temperature"));
        Assertions.assertEquals("probability", p.getCoordinates().get("offsetScale"));
        Assertions.assertEquals("zero", p.getCoordinates().get("scoreNull"));
        // the probability inherits the perishability of the market offset (current_bid_t10 validFor PT15M through the baseline)
        Assertions.assertEquals(Duration.ofMinutes(15), p.getValidFor());
        Assertions.assertEquals(OutputColumn.Status.staticSafe, p.getStatus());
        Assertions.assertTrue(p.getDerivedFrom().contains("market"), p::describe);
        // baselines[].emit: the baseline value as an output column, in the same context stage
        final OutputColumn emitted = column(plan, "marketProb");
        Assertions.assertFalse(emitted.isIntermediate());
        Assertions.assertEquals("copy", emitted.getOperator());
        Assertions.assertEquals("f_marketProb", emitted.getOutputName());
        Assertions.assertEquals(Duration.ofMinutes(15), emitted.getValidFor());
        Assertions.assertTrue(column(plan, "__baseline_market").isIntermediate());
    }

    @Test
    public void testSoftmaxDiagnosticsAndIndicators() {
        Assertions.assertTrue(hasCode(compile(SOURCES, withBlocks(PROB_BLOCK.replace("offset: market", "offset: nope"))), "context.softmax.offset"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withBlocks(PROB_BLOCK.replace("offset: market", "offset: category"))), "context.softmax.offset"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withBlocks(PROB_BLOCK.replace("temperature: 1.3", "temperature: 0"))), "context.softmax.temperature"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withBlocks(PROB_BLOCK.replace("temperature: 1.3", "offsetScale: exp"))), "context.softmax.offsetScale"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withBlocks(PROB_BLOCK.replace("temperature: 1.3", "scoreNull: drop"))), "context.softmax.scoreNull"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withBlocks(PROB_BLOCK.replace("temperature: 1.3", "temperatureFrom: gs://b/calibration.json"))), "context.softmax.temperatureFrom.unresolved"));
        // nullPolicy indicator: the null-row flag and the score-fallback flag
        final FeaturePlan indicator = compile(SOURCES, withBlocks(PROB_BLOCK).replace("output:\n  prefix: f_\n", "output:\n  prefix: f_\n  nullPolicy: indicator\n"));
        Assertions.assertNotNull(indicator.getColumn("prob_pWin_softmax_isnull"), indicator::describe);
        final OutputColumn fallback = column(indicator, "prob_pWin_softmax_scoreNull");
        Assertions.assertEquals("price_per_unit", fallback.getCoordinates().get("indicatorOf"));
        // without an offset: plain group softmax, no validFor
        final FeaturePlan plain = compile(SOURCES, withBlocks(PROB_BLOCK.replace("offset: market, ", "")));
        Assertions.assertNull(column(plain, "prob_pWin_softmax").getCoordinates().get("offset"));
        Assertions.assertNull(column(plain, "prob_pWin_softmax").getValidFor());
    }

    @Test
    public void testTemperatureFromIsOutsideThePlanHash() {
        final JsonObject sourcesJson = Config.convertConfigJson(SOURCES, Config.Format.yaml);
        final JsonObject base = Config.convertConfigJson(withBlocks(PROB_BLOCK), Config.Format.yaml);
        final JsonObject fromUri = Config.convertConfigJson(withBlocks(PROB_BLOCK.replace("temperature: 1.3",
                "temperatureFrom: \"data:" + java.util.Base64.getEncoder().encodeToString("{\"temperature\": 1.3, \"a\": 0.1}".getBytes()) + "\"")), Config.Format.yaml);
        FeaturePlanService.resolveTemperatureFrom(fromUri, null);
        final JsonObject resolved = fromUri.getAsJsonArray("features").asList().stream().map(e -> e.getAsJsonObject())
                .filter(f -> "prob".equals(f.get("name").getAsString())).findFirst().orElseThrow()
                .getAsJsonArray("ops").get(0).getAsJsonObject().getAsJsonObject("temperatureFrom");
        Assertions.assertEquals(1.3, resolved.get("value").getAsDouble(), 1e-9);
        Assertions.assertTrue(resolved.get("source").getAsString().startsWith("data:"));
        final FeaturePlan a = FeaturePlanCompiler.compile(sourcesJson, base, null);
        final FeaturePlan b = FeaturePlanCompiler.compile(sourcesJson, fromUri, null);
        Assertions.assertFalse(b.getDiagnostics().hasErrors(), b::describe);
        Assertions.assertEquals("1.3", column(b, "prob_pWin_softmax").getCoordinates().get("temperature"));
        // the literal and the document give different plan hashes only because the literal is in the parameters;
        // the document itself is stripped: two documents with different values share the plan hash
        final JsonObject other = fromUri.deepCopy();
        other.getAsJsonArray("features").asList().stream().map(e -> e.getAsJsonObject())
                .filter(f -> "prob".equals(f.get("name").getAsString())).findFirst().orElseThrow()
                .getAsJsonArray("ops").get(0).getAsJsonObject().getAsJsonObject("temperatureFrom").addProperty("value", 2.0);
        final FeaturePlan c = FeaturePlanCompiler.compile(sourcesJson, other, null);
        Assertions.assertEquals(b.getHash(), c.getHash());
        Assertions.assertNotEquals(b.getOutputHash(), c.getOutputHash());
        Assertions.assertEquals(1, b.getSpec().resolvedExternals.size());
        Assertions.assertEquals(1, b.toManifest(List.of(), java.util.Map.of()).getAsJsonArray("externals").size());
        Assertions.assertNotEquals(a.getHash(), b.getHash());
        Assertions.assertEquals(2.0, FeaturePlanService.parseTemperature("2.0", "x"), 1e-9);
        Assertions.assertEquals(1.5, FeaturePlanService.parseTemperature("{\"T\": 1.5}", "x"), 1e-9);
        Assertions.assertThrows(IllegalArgumentException.class, () -> FeaturePlanService.parseTemperature("{\"x\": 1}", "x"));
    }

    @Test
    public void testSoftmaxEvaluation() {
        final FeaturePlan plan = compile(SOURCES, withBlocks(PROB_BLOCK.replace("temperature: 1.3", "temperature: 2")));
        final OutputColumn p = column(plan, "prob_pWin_softmax");
        final List<java.util.Map<String, Object>> rows = new java.util.ArrayList<>();
        rows.add(row("price_per_unit", 2.0, "__baseline_market", 0.5));   // w=0.5, f=1
        rows.add(row("price_per_unit", 0.0, "__baseline_market", 0.3));   // w=0.3, f=0
        rows.add(row("price_per_unit", null, "__baseline_market", 0.2));  // score null -> f=0 (scoreNull zero)
        rows.add(row("price_per_unit", 5.0, "__baseline_market", 0.0));   // offset 0 -> p=0, stays in the denominator as 0
        rows.add(row("price_per_unit", 5.0, "__baseline_market", null));  // offset null -> row null, out of the denominator
        ContextEvaluator.softmax(p, rows);
        final double e1 = 0.5 * Math.exp(1.0), e2 = 0.3, e3 = 0.2, z = e1 + e2 + e3;
        Assertions.assertEquals(e1 / z, (Double) rows.get(0).get("prob_pWin_softmax"), 1e-12);
        Assertions.assertEquals(e2 / z, (Double) rows.get(1).get("prob_pWin_softmax"), 1e-12);
        Assertions.assertEquals(e3 / z, (Double) rows.get(2).get("prob_pWin_softmax"), 1e-12);
        Assertions.assertEquals(0.0, (Double) rows.get(3).get("prob_pWin_softmax"), 1e-12);
        Assertions.assertNull(rows.get(4).get("prob_pWin_softmax"));
        // f = 0, T = 1: the probabilities are the offsets renormalised
        final FeaturePlan unit = compile(SOURCES, withBlocks(PROB_BLOCK.replace("temperature: 1.3", "temperature: 1")));
        final OutputColumn u = column(unit, "prob_pWin_softmax");
        final List<java.util.Map<String, Object>> flat = new java.util.ArrayList<>();
        flat.add(row("price_per_unit", 0.0, "__baseline_market", 0.6));
        flat.add(row("price_per_unit", 0.0, "__baseline_market", 0.4));
        ContextEvaluator.softmax(u, flat);
        Assertions.assertEquals(0.6, (Double) flat.get(0).get("prob_pWin_softmax"), 1e-12);
        // scoreNull: null and offsetScale: log
        final FeaturePlan logScale = compile(SOURCES, withBlocks(PROB_BLOCK.replace("temperature: 1.3", "temperature: 1, offsetScale: log, scoreNull: \"null\"")));
        final OutputColumn l = column(logScale, "prob_pWin_softmax");
        final List<java.util.Map<String, Object>> logRows = new java.util.ArrayList<>();
        logRows.add(row("price_per_unit", 0.0, "__baseline_market", Math.log(0.75)));
        logRows.add(row("price_per_unit", 0.0, "__baseline_market", Math.log(0.25)));
        logRows.add(row("price_per_unit", null, "__baseline_market", Math.log(0.5)));
        ContextEvaluator.softmax(l, logRows);
        Assertions.assertEquals(0.75, (Double) logRows.get(0).get("prob_pWin_softmax"), 1e-12);
        Assertions.assertNull(logRows.get(2).get("prob_pWin_softmax"));
    }

    private static java.util.Map<String, Object> row(final Object... kv) {
        final java.util.Map<String, Object> row = new java.util.HashMap<>();
        for (int i = 0; i < kv.length; i += 2) row.put((String) kv[i], kv[i + 1]);
        return row;
    }

    @Test
    public void testPlaceboOps() {
        final String blocks = """
              - {name: placeboNoise, scope: row, type: noise, distribution: uniform, seed: 20260717}
              - name: placeboShuffle
                scope: context
                context: session
                ops:
                  - {type: shuffle, fields: [condition_grade, start_price], seed: 7}
            """;
        final FeaturePlan plan = compile(SOURCES, withBlocks(blocks));
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        final OutputColumn noise = column(plan, "placeboNoise");
        Assertions.assertEquals("session_time,session_id", noise.getCoordinates().get("identity"));
        Assertions.assertEquals("20260717", noise.getCoordinates().get("seed"));
        Assertions.assertEquals(OutputColumn.Status.staticSafe, noise.getStatus());
        Assertions.assertTrue(noise.getAvailableAt().isPreEvent(), noise::describe);
        final OutputColumn grade = column(plan, "placeboShuffle_condition_grade_shuffle");
        Assertions.assertEquals(Schema.Type.string, grade.getFieldType().getType());
        Assertions.assertEquals("session_time,session_id", grade.getCoordinates().get("order"));
        Assertions.assertEquals("session_id", grade.getCoordinates().get("contextKeys"));
        Assertions.assertEquals(Schema.Type.float64, column(plan, "placeboShuffle_start_price_shuffle").getFieldType().getType());
        // a shuffled outcome keeps the outcome's availability: emitting it is a violation, exactly like the original
        final FeaturePlan outcome = compile(SOURCES, withBlocks(blocks.replace("fields: [condition_grade, start_price]", "fields: [sold]")));
        Assertions.assertTrue(hasCode(outcome, "availability.violation"), outcome::describe);
        Assertions.assertTrue(hasCode(compile(SOURCES, withBlocks(blocks.replace(", seed: 20260717", ""))), "row.noise.seed"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withBlocks(blocks.replace(", seed: 7", ""))), "context.shuffle.seed"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withBlocks(blocks.replace("uniform", "cauchy"))), "row.noise.distribution"));
        final FeaturePlan noTie = compile(SOURCES, withBlocks(blocks).replace(", orderTieBreak: [session_id]", ""));
        Assertions.assertTrue(hasCode(noTie, "row.noise.identity"));
        Assertions.assertTrue(hasCode(noTie, "context.shuffle.identity"));

        // evaluation: deterministic in the row identity / group, multiset preserved
        final java.util.Map<String, Object> r1 = row("session_time", "2025-01-01T10:00:00Z", "session_id", "A");
        final java.util.Map<String, Object> r2 = row("session_time", "2025-01-01T10:00:00Z", "session_id", "B");
        final double v1 = (Double) RowEvaluator.noise(noise, r1);
        Assertions.assertEquals(v1, (Double) RowEvaluator.noise(noise, new java.util.HashMap<>(r1)), 0d);
        Assertions.assertNotEquals(v1, (Double) RowEvaluator.noise(noise, r2));
        Assertions.assertTrue(v1 >= 0 && v1 < 1);
        final List<java.util.Map<String, Object>> group = new java.util.ArrayList<>();
        for (int i = 0; i < 6; i++) group.add(row("session_time", "2025-01-01T10:00:00Z", "session_id", "A", "condition_grade", "g" + i));
        ContextEvaluator.shuffle(grade, group);
        final java.util.Set<Object> before = new java.util.HashSet<>(), after = new java.util.HashSet<>();
        final List<Object> permuted = new java.util.ArrayList<>();
        for (final java.util.Map<String, Object> r : group) {
            before.add(r.get("condition_grade"));
            after.add(r.get("placeboShuffle_condition_grade_shuffle"));
            permuted.add(r.get("placeboShuffle_condition_grade_shuffle"));
        }
        Assertions.assertEquals(before, after);
        final List<java.util.Map<String, Object>> again = new java.util.ArrayList<>();
        for (int i = 0; i < 6; i++) again.add(row("session_time", "2025-01-01T10:00:00Z", "session_id", "A", "condition_grade", "g" + i));
        java.util.Collections.reverse(again); // GroupByKey order must not matter
        ContextEvaluator.shuffle(grade, again);
        java.util.Collections.reverse(again);
        for (int i = 0; i < 6; i++) Assertions.assertEquals(permuted.get(i), again.get(i).get("placeboShuffle_condition_grade_shuffle"));
    }

    /** A baseline expression may call the context ops that read one value per row - and only those. */
    @Test
    public void testBaselineOpCall() {
        final String baseline = "- {name: market, context: session, expr: \"share(1 / current_bid_t10)\"}";
        Assertions.assertTrue(SPEC.contains(baseline));
        // an op that takes parameters of its own has no place to read them from in a baseline
        for (final String call : List.of("residualize(current_bid_t10)", "harville(current_bid_t10)",
                "softmax(current_bid_t10)", "shuffle(current_bid_t10)")) {
            final FeaturePlan plan = compile(SOURCES, SPEC.replace("share(1 / current_bid_t10)", call));
            Assertions.assertTrue(hasCode(plan, "baselines.expr.op"), () -> call + "\n" + plan.describe());
        }
        // a group op without the group it is computed over
        final FeaturePlan rowScope = compile(SOURCES, SPEC.replace(baseline, "- {name: market, expr: \"share(1 / current_bid_t10)\"}"));
        Assertions.assertTrue(hasCode(rowScope, "baselines.expr.op"), rowScope::describe);
        // an ordinary expression, and a function that is not an op, stay expressions
        final FeaturePlan expression = compile(SOURCES, SPEC.replace("share(1 / current_bid_t10)", "ln(current_bid_t10)"));
        Assertions.assertFalse(hasCode(expression, "baselines.expr.op"), expression::describe);
    }

    @Test
    public void testBaselineEmitAndRole() {
        final String spec = SPEC.replace("- {name: market, context: session, expr: \"share(1 / current_bid_t10)\"}",
                "- {name: market, context: session, expr: \"share(1 / current_bid_t10)\", emit: marketProb}")
                .replace("output:\n  prefix: f_\n", "output:\n  prefix: f_\n  roles: {baseline: market}\n");
        final FeaturePlan plan = compile(SOURCES, spec);
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        Assertions.assertFalse(hasCode(plan, "output.roles.baseline.notEmitted"), plan::describe);
        Assertions.assertEquals("f_marketProb", plan.getRoleColumns().get("baseline"));
        Assertions.assertTrue(hasCode(compile(SOURCES, spec.replace("emit: marketProb", "emit: category")), "baselines.emit.duplicate"));
    }

    // ------------------------------------------------------------------------------------------
    // fit.mode forward
    // ------------------------------------------------------------------------------------------

    private static String withForward(final String fit) {
        return SPEC.replace("output:\n  prefix: f_\n", fit + "output:\n  prefix: f_\n");
    }

    @Test
    public void testForwardFitExpansion() {
        final FeaturePlan plan = compile(SOURCES, withForward("fit: {mode: forward, blocks: {size: P7D}, minBlocks: 2}\n"));
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        Assertions.assertTrue(hasCode(plan, "fit.mode.forward"), plan::describe);
        // the hidden level columns carry the block geometry and, per level, the target's availability lag
        final OutputColumn count = column(plan, "enc__seller_id__n");
        Assertions.assertEquals("forward", count.getCoordinates().get("fit"));
        Assertions.assertEquals(Long.toString(7L * 86_400_000L), count.getCoordinates().get("blockSizeMillis"));
        Assertions.assertEquals("2", count.getCoordinates().get("minBlocks"));
        Assertions.assertEquals("session_time", count.getCoordinates().get("blockField"));
        Assertions.assertEquals("0", count.getCoordinates().get("forwardLagMillis")); // no target: attribute-only level
        final OutputColumn mean = column(plan, "enc__seller_id__e2__n");
        Assertions.assertEquals(Long.toString(6L * 86_400_000L + 30L * 60_000L), mean.getCoordinates().get("forwardLagMillis")); // sold: settlement PT30M + ingestion P6D
        Assertions.assertTrue(column(plan, "enc__seller_id__e2__mean").isFitted());
        Assertions.assertEquals("fitStat", column(plan, "enc__seller_id__e2__mean").getOperator());
        // a fit stage, no time-ordered replay for the block
        Assertions.assertTrue(plan.getStages().stream().anyMatch(s -> s.kind() == FeaturePlan.StageKind.fit && s.blocks().contains("enc")), plan::describe);
        // the hidden level columns live in the fit stage (the target expression is a row column hosted earlier, as for static)
        Assertions.assertTrue(plan.getStages().stream().filter(s -> s.columnNames().contains("enc__seller_id__n")).allMatch(s -> s.kind() == FeaturePlan.StageKind.fit), plan::describe);
        // calendar buckets and the default size
        Assertions.assertEquals("quarter", column(compile(SOURCES, withForward("fit: {mode: forward, blocks: {bucket: quarter}}\n")), "enc__seller_id__n").getCoordinates().get("blockBucket"));
        Assertions.assertEquals(Long.toString(90L * 86_400_000L), column(compile(SOURCES, withForward("fit: {mode: forward}\n")), "enc__seller_id__n").getCoordinates().get("blockSizeMillis"));
        // the geometry is semantic: it changes the plan hash
        Assertions.assertNotEquals(compile(SOURCES, withForward("fit: {mode: forward, blocks: {size: P7D}}\n")).getHash(),
                compile(SOURCES, withForward("fit: {mode: forward, blocks: {size: P30D}}\n")).getHash());
        // batch only
        Assertions.assertTrue(FeatureStages.engineConstraints(plan, true).stream().anyMatch(m -> m.contains("forward")));
        Assertions.assertTrue(FeatureStages.engineConstraints(plan, false).isEmpty());
    }

    @Test
    public void testForwardFitDiagnostics() {
        Assertions.assertTrue(hasCode(compile(SOURCES, withForward("fit: {mode: forward, blocks: {bucket: decade}}\n")), "fit.blocks.bucket"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withForward("fit: {mode: forward, blocks: {bucket: year, size: P7D}}\n")), "fit.blocks"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withForward("fit: {mode: forward, blocks: {field: start_price, size: P7D}}\n")), "fit.blocks.field"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withForward("fit: {mode: forward, minBlocks: 0}\n")), "fit.minBlocks"));
        Assertions.assertTrue(hasCode(compile(SOURCES, withForward("fit: {mode: forward, blocks: {size: PT0S}}\n")), "fit.blocks.size"));
        // sufficient statistics only, like static / fold
        final FeaturePlan quantile = compile(SOURCES, withForward("fit: {mode: forward}\n").replace("- {expr: \"sold >= 1\", stats: [mean]}", "- {expr: \"sold >= 1\", stats: [mean, quantile50]}"));
        Assertions.assertTrue(hasCode(quantile, "encoding.stat.static"), quantile::describe);
        // windows: maxAge rounded to blocks, maxEvents / filter ignored with a warning; no static-windows warning
        final FeaturePlan window = compile(SOURCES, withForward("fit: {mode: forward, blocks: {size: P7D}}\n")
                .replace("      - keys: [seller_id]\n", "      - keys: [seller_id]\n        windows: [{maxAge: P30D}, {maxEvents: 5}]\n"));
        Assertions.assertTrue(hasCode(window, "fit.mode.forward.window"), window::describe);
        Assertions.assertTrue(hasCode(window, "fit.mode.forward.windowIgnored"), window::describe);
        Assertions.assertFalse(hasCode(window, "fit.mode.static.windows"), window::describe);
        Assertions.assertEquals("5", column(window, "enc__seller_id__30d__n").getCoordinates().get("windowBlocks"));
        // per-block override
        final FeaturePlan perBlock = compile(SOURCES, SPEC.replace("  - name: enc\n    scope: population\n    type: encoding\n",
                "  - name: enc\n    scope: population\n    type: encoding\n    fit: {mode: forward, blocks: {bucket: month}, minBlocks: 3}\n"));
        Assertions.assertFalse(perBlock.getDiagnostics().hasErrors(), perBlock::describe);
        Assertions.assertEquals("month", column(perBlock, "enc__seller_id__n").getCoordinates().get("blockBucket"));
        Assertions.assertEquals("3", column(perBlock, "enc__seller_id__n").getCoordinates().get("minBlocks"));
    }

    @Test
    public void testForwardBlocksArithmetic() {
        final ForwardBlocks weekly = ForwardBlocks.ofSize(Duration.ofDays(7));
        final long jan1 = java.time.Instant.parse("2025-01-01T10:00:00Z").toEpochMilli();
        final long jan3 = java.time.Instant.parse("2025-01-03T10:00:00Z").toEpochMilli();
        final long jan20 = java.time.Instant.parse("2025-01-20T10:00:00Z").toEpochMilli();
        final long feb1 = java.time.Instant.parse("2025-02-01T10:00:00Z").toEpochMilli();
        Assertions.assertEquals(2869, weekly.indexOf(jan1));
        Assertions.assertEquals(2870, weekly.indexOf(jan3));
        Assertions.assertEquals(2872, weekly.indexOf(jan20));
        Assertions.assertEquals(2874, weekly.indexOf(feb1));
        // usable = the last block ending at or before event + predictOffset − lag: Feb 1 with predictAt −8 min and a
        // 6-day-30-minute target lag reads up to block 2872 (Jan 16–22); Jan 3 with no lag reads block 2869
        final long predict = -8L * 60_000L, lag = 6L * 86_400_000L + 30L * 60_000L;
        Assertions.assertEquals(2872, weekly.usableBlock(feb1, predict, lag));
        Assertions.assertEquals(2869, weekly.usableBlock(jan3, predict, 0));
        Assertions.assertEquals(2868, weekly.usableBlock(jan3, predict, lag));
        // calendar buckets
        Assertions.assertEquals(2025L * 4, ForwardBlocks.ofBucket("quarter").indexOf(jan1));
        Assertions.assertEquals(2025L * 4, ForwardBlocks.ofBucket("quarter").indexOf(java.time.Instant.parse("2025-03-31T23:59:59Z").toEpochMilli()));
        Assertions.assertEquals(2025L * 4 + 1, ForwardBlocks.ofBucket("quarter").indexOf(java.time.Instant.parse("2025-04-01T00:00:00Z").toEpochMilli()));
        Assertions.assertEquals(2025L * 12 + 1, ForwardBlocks.ofBucket("month").indexOf(feb1));
        Assertions.assertEquals(2025L, ForwardBlocks.ofBucket("year").indexOf(feb1));
        Assertions.assertEquals(1, ForwardBlocks.ofBucket("year").windowBlocks(Duration.ofDays(200)));
        Assertions.assertEquals(2, ForwardBlocks.ofBucket("year").windowBlocks(Duration.ofDays(400)));
        Assertions.assertEquals(5, weekly.windowBlocks(Duration.ofDays(30)));
        Assertions.assertThrows(IllegalArgumentException.class, () -> ForwardBlocks.ofBucket("decade"));
        // series: floor / prefix differences
        final ForwardBlocks.Series s = new ForwardBlocks.Series(new long[]{10, 12, 15}, new double[]{1, 3, 6}, new double[]{1, 2, 4}, new double[]{1, 2, 4});
        Assertions.assertEquals(-1, s.floor(9));
        Assertions.assertEquals(0, s.floor(10));
        Assertions.assertEquals(1, s.floor(14));
        Assertions.assertEquals(2, s.floor(99));
        Assertions.assertEquals(6.0, s.totals().n, 0d);
        Assertions.assertEquals(5.0, s.statsBetween(0, 2).n, 0d);
        Assertions.assertNull(s.statsBetween(-1, -1));
        Assertions.assertNull(s.statsBetween(2, 2));
        // λ per block from cumulative series: one level, two keys, moments over the keys' prefix up to each block
        final java.util.Map<String, ForwardBlocks.Series> series = new java.util.HashMap<>();
        series.put(FitArtifact.entryKey("lvl__n", "a"), new ForwardBlocks.Series(new long[]{1, 2}, new double[]{4, 8}, new double[]{2, 4}, new double[]{2, 4}));
        series.put(FitArtifact.entryKey("lvl__n", "b"), new ForwardBlocks.Series(new long[]{2}, new double[]{4}, new double[]{0}, new double[]{0}));
        final java.util.Map<String, java.util.TreeMap<Long, Double>> lambdas = VarianceComponents.lambdasByBlock(series);
        Assertions.assertTrue(lambdas.containsKey("lvl__n"));
        Assertions.assertFalse(lambdas.get("lvl__n").containsKey(1L)); // one key only at block 1: no estimate
        Assertions.assertTrue(lambdas.get("lvl__n").containsKey(2L));
        Assertions.assertEquals(12.0, VarianceComponents.forwardTotals(series).get(FitArtifact.entryKey("lvl__n", "a")).n + VarianceComponents.forwardTotals(series).get(FitArtifact.entryKey("lvl__n", "b")).n, 0d);
    }

    /**
     * {@code direction: future}: label columns over the strictly-future window — status label (no role: the role
     * stays with the declared label), the availability of the horizon's last event, a stage of their own
     * (descending replay); a declared label over them is emitted, a feature reading them is a violation; the ops a
     * future window rejects.
     */
    @Test
    public void testFutureLabels() {
        final String labels = """
                  - name: horizon
                    scope: sequence
                    entity: seller
                    direction: future
                    windows: [{maxAge: P7D}]
                    ops:
                      - {type: aggregate, field: final_price, funcs: [last, mean]}
                      - {type: lag, field: start_price, k: 1}
                      - {type: barrier, field: start_price, up: 0.05, down: -0.05}
                      - {type: sinceEvent, predicate: "sold = 1", unit: [events]}
                  - name: ret
                    scope: row
                    expr: "horizon_7d_start_price_lead1 / start_price - 1"
                """;
        final String anchor = "  - name: vs_market\n";
        final String output = "output:\n  prefix: f_\n";
        Assertions.assertTrue(SPEC.contains(anchor) && SPEC.contains(output));
        final String spec = SPEC.replace(anchor, labels + anchor).replace(output, output + "  nullPolicy: indicator\n  roles: {label: ret}\n");
        final FeaturePlan plan = compile(SOURCES, spec);
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        Assertions.assertTrue(hasCode(plan, "sequence.direction.future"), plan::describe);

        // the horizon's last event is known after its own availability (final_price: settlement 30 min + ingestion 6 days)
        final OutputColumn last = column(plan, "horizon_7d_final_price_last");
        Assertions.assertEquals(OutputColumn.Status.label, last.getStatus());
        Assertions.assertNull(last.getRole());
        Assertions.assertEquals("future", last.getCoordinates().get("direction"));
        Assertions.assertEquals("last", last.getCoordinates().get("func"), "the coordinates keep the declared func (the evaluator swaps it)");
        Assertions.assertEquals(Duration.ofDays(13).plusMinutes(30), last.getAvailableAt().getOffset());
        Assertions.assertNull(last.getWindowShift());
        Assertions.assertFalse(last.isIntermediate());
        // a pre-event field: the horizon itself; lag reads the next events (lead), sinceEvent the events until
        Assertions.assertEquals(Duration.ofDays(7), column(plan, "horizon_7d_start_price_lead1").getAvailableAt().getOffset());
        // the self side counts too: a $self filter field known after the horizon delays the label
        final FeaturePlan selfSide = compile(SOURCES, spec.replace("windows: [{maxAge: P7D}]", "windows: [{maxAge: P1D, filter: \"start_price <= $self.final_price\"}]")
                .replace("horizon_7d_", "horizon_1d_"));
        Assertions.assertEquals(Duration.ofDays(6).plusMinutes(30), column(selfSide, "horizon_1d_start_price_lead1").getAvailableAt().getOffset(), selfSide::describe);
        Assertions.assertEquals(Schema.Type.int64, column(plan, "horizon_7d_start_price_barrier").getFieldType().getType());
        Assertions.assertTrue(column(plan, "horizon_7d_start_price_barrier").getInputs().contains("start_price"));
        column(plan, "horizon_7d_until_events");
        // labels get no _isnull indicator (a post-event flag would be a feature); the past blocks still do
        Assertions.assertNull(plan.getColumn("horizon_7d_final_price_last_isnull"), plan::describe);
        Assertions.assertTrue(plan.getColumns().stream().anyMatch(c -> c.getCanonicalName().startsWith("recent_") && c.getCanonicalName().endsWith("_isnull")));

        // a stage of their own: the same entity, replayed in descending time
        final FeaturePlan.Stage future = plan.getStages().stream().filter(s -> s.kind() == FeaturePlan.StageKind.future).findFirst().orElseThrow();
        Assertions.assertEquals(List.of("seller_id"), future.keys());
        Assertions.assertTrue(future.isReplay() && future.isKeyed());
        Assertions.assertTrue(future.columnNames().contains("horizon_7d_final_price_last"));
        Assertions.assertTrue(plan.getStages().stream().anyMatch(s -> s.kind() != FeaturePlan.StageKind.future && s.columnNames().contains("recent_n5_sold_lag1")));

        // the declared label over them is emitted as the label; the other future columns are status label only
        final OutputColumn ret = column(plan, "ret");
        Assertions.assertEquals(OutputColumn.Status.label, ret.getStatus());
        Assertions.assertEquals("label", ret.getRole());
        Assertions.assertFalse(ret.isIntermediate());
        Assertions.assertEquals("f_ret", plan.getRoleColumns().get("label"));
        Assertions.assertEquals(1, plan.getColumns().stream().filter(c -> "label".equals(c.getRole())).count());
        // another role may name a future column: it is resolved, not dropped
        final FeaturePlan weighted = compile(SOURCES, spec.replace("roles: {label: ret}", "roles: {label: ret, weight: horizon_7d_final_price_mean}"));
        Assertions.assertFalse(weighted.getDiagnostics().hasErrors(), weighted::describe);
        Assertions.assertEquals("f_horizon_7d_final_price_mean", weighted.getRoleColumns().get("weight"), weighted::describe);
        Assertions.assertEquals("f_ret", weighted.getRoleColumns().get("label"));

        // an encoding over past labels is fine: a past row's label counts once its horizon has passed (a window shift)
        final String target = "- {expr: \"sold >= 1\", stats: [mean]}";
        Assertions.assertTrue(spec.contains(target));
        final FeaturePlan encoded = compile(SOURCES, spec.replace(target, target + "\n      - {field: horizon_7d_final_price_mean, stats: [mean]}"));
        Assertions.assertFalse(encoded.getDiagnostics().hasErrors(), encoded::describe);
        Assertions.assertTrue(encoded.getColumns().stream().anyMatch(c -> c.getCanonicalName().contains("horizon_7d_final_price_mean") && "encoding".equals(c.getOperator())
                && c.getStatus() == OutputColumn.Status.windowShift && c.getWindowShift().compareTo(Duration.ofDays(13)) > 0), encoded::describe);

        // a feature reading a label is a leak
        final FeaturePlan leak = compile(SOURCES, spec.replace(anchor, "  - name: leak\n    scope: row\n    expr: \"horizon_7d_final_price_mean * 2\"\n" + anchor));
        Assertions.assertTrue(leak.getDiagnostics().getErrorMessages().stream().anyMatch(m -> m.contains("leak")), leak::describe);
        Assertions.assertTrue(hasCode(leak, "availability.violation"), leak::describe);

        final Map<String, String> rejected = new java.util.LinkedHashMap<>();
        rejected.put("direction: sideways", "sequence.direction");
        rejected.put("windows: [{maxEvents: 5}]", "sequence.direction.maxAge");
        rejected.put("- {type: lag, field: start_price, k: 1}", "sequence.direction.op");
        final String lag = "- {type: lag, field: start_price, k: 1}";
        for (final Map.Entry<String, String> e : rejected.entrySet()) {
            final String replaced = switch (e.getValue()) {
                case "sequence.direction" -> spec.replace("direction: future", e.getKey());
                case "sequence.direction.maxAge" -> spec.replace("windows: [{maxAge: P7D}]", e.getKey());
                default -> spec.replace(lag, "- {type: delta, field: start_price, k: 1}");
            };
            final FeaturePlan bad = compile(SOURCES, replaced);
            Assertions.assertTrue(hasCode(bad, e.getValue()), () -> e + "\n" + bad.describe());
        }
        Assertions.assertTrue(hasCode(compile(SOURCES, spec.replace(lag, "- {type: regression, field: final_price, against: start_price, lag: 1}")), "sequence.direction.op"));
        final FeaturePlan sameEvent = compile(SOURCES, spec.replace(lag, lag + "\n      - {type: regression, field: final_price, against: start_price}"));
        Assertions.assertFalse(sameEvent.getDiagnostics().hasErrors(), sameEvent::describe);
        Assertions.assertTrue(hasCode(compile(SOURCES, spec.replace("up: 0.05, down: -0.05", "up: -0.05")), "sequence.barrier.levels"));
        Assertions.assertTrue(hasCode(compile(SOURCES, SPEC.replace("- {type: aggregate, field: sold, funcs: [count, mean]}",
                "- {type: barrier, field: start_price, up: 0.05}")), "sequence.barrier.direction"));
        Assertions.assertTrue(hasCode(compile(SOURCES, SPEC.replace("    expr: \"start_price / quantity\"\n", "    expr: \"start_price / quantity\"\n    direction: future\n")), "features.direction"));
    }

    /**
     * {@code fit.fold: {by: time, purge, embargo}}: the time-fold coordinates (blocks, purge / embargo rounded up to
     * whole blocks), the purge defaulting to the horizon of the label the target reads, and the validation.
     */
    @Test
    public void testTimeFold() {
        final String label = """
                  - name: horizon
                    scope: sequence
                    entity: seller
                    direction: future
                    windows: [{maxAge: P20D}]
                    ops:
                      - {type: aggregate, field: sold, funcs: [mean]}
                  - name: enc_label
                    scope: population
                    type: encoding
                    keySets:
                      - keys: [category]
                    targets:
                      - {field: horizon_20d_sold_mean, stats: [mean]}
                    fit: {mode: fold, blocks: {size: P7D}, fold: {by: time}}
                """;
        final String anchor = "  - name: vs_market\n";
        final String spec = SPEC.replace(anchor, label + anchor)
                .replace("output:\n  prefix: f_\n", "fit: {mode: fold, blocks: {bucket: month}, fold: {by: time, embargo: P40D}}\noutput:\n  prefix: f_\n");
        final FeaturePlan plan = compile(SOURCES, spec);
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);

        // the top-level fold: month blocks, no label in the target → no purge, embargo 40 days → 2 blocks
        final OutputColumn mean = plan.getColumns().stream().filter(c -> "enc".equals(c.getBlock()) && "encoding".equals(c.getOperator()) && c.getCoordinates().containsKey("fit")).findFirst().orElseThrow();
        Assertions.assertEquals("fold", mean.getCoordinates().get("fit"));
        Assertions.assertEquals("time", mean.getCoordinates().get("foldBy"));
        Assertions.assertEquals("month", mean.getCoordinates().get("blockBucket"));
        Assertions.assertEquals("0", mean.getCoordinates().get("purgeBlocks"));
        Assertions.assertEquals("2", mean.getCoordinates().get("embargoBlocks"));
        Assertions.assertNull(mean.getCoordinates().get("foldKeys"), "time folds have no hash unit");
        // a label target: the purge defaults to its horizon (20 days of 7-day blocks → 3), the block's own fit overrides the blocks
        final OutputColumn labelMean = plan.getColumns().stream().filter(c -> "enc_label".equals(c.getBlock()) && "encoding".equals(c.getOperator()) && c.getCoordinates().containsKey("fit")).findFirst().orElseThrow();
        Assertions.assertEquals(Long.toString(java.time.Duration.ofDays(7).toMillis()), labelMean.getCoordinates().get("blockSizeMillis"));
        Assertions.assertEquals("3", labelMean.getCoordinates().get("purgeBlocks"));
        // the top-level embargo (40 days) is inherited and rounded to the block's own 7-day blocks
        Assertions.assertEquals("6", labelMean.getCoordinates().get("embargoBlocks"));
        Assertions.assertTrue(plan.getDiagnostics().getMessages().stream().anyMatch(m -> "fit.fold.purge".equals(m.code()) && m.location().contains("enc_label")), plan::describe);
        Assertions.assertTrue(hasCode(plan, "fit.mode.fold"));
        // the purge is two-sided and the embargo extends it (the info spells out the width left out)
        Assertions.assertTrue(plan.getDiagnostics().getMessages().stream().anyMatch(m -> "fit.mode.fold".equals(m.code())
                && m.message().contains("on both sides") && m.message().contains("2·purge + embargo + 1")), plan::describe);

        // a declared purge wins over the label's horizon and is inherited by the blocks (10 days of 7-day blocks → 2)
        final FeaturePlan declared = compile(SOURCES, spec.replace("fold: {by: time, embargo: P40D}", "fold: {by: time, purge: P10D, embargo: P40D}"));
        Assertions.assertEquals("2", declared.getColumns().stream().filter(c -> "enc_label".equals(c.getBlock()) && "encoding".equals(c.getOperator())
                && c.getCoordinates().containsKey("fit")).findFirst().orElseThrow().getCoordinates().get("purgeBlocks"));

        final String fold = "fold: {by: time, embargo: P40D}";
        // purge / embargo round by the shortest block: 30 days of month blocks → 2 (a 28-day February may lie between)
        final FeaturePlan monthly = compile(SOURCES, spec.replace(fold, "fold: {by: time, purge: P30D, embargo: P40D}"));
        Assertions.assertEquals("2", monthly.getColumns().stream().filter(c -> "enc".equals(c.getBlock()) && "encoding".equals(c.getOperator())
                && c.getCoordinates().containsKey("fit")).findFirst().orElseThrow().getCoordinates().get("purgeBlocks"));
        // a time fold has no hash folds to count
        Assertions.assertFalse(hasCode(compile(SOURCES, spec.replace("mode: fold, blocks: {bucket: month}", "mode: fold, folds: 1, blocks: {bucket: month}")), "fit.folds"));
        // groupBy does not silence the past-target key guard under a time fold (a time fold ignores groupBy), unlike under hash folds
        final String pastTargetKey = spec.replace("      - keys: [seller_id]\n", "      - keys: [seller_id]\n      - keys: [recent_365d_sold_mean]\n")
                .replace("fit: {mode: fold, blocks: {bucket: month}", "fit: {mode: fold, groupBy: seller, blocks: {bucket: month}");
        Assertions.assertTrue(hasCode(compile(SOURCES, pastTargetKey), "fit.groupBy.required"));
        Assertions.assertFalse(hasCode(compile(SOURCES, pastTargetKey.replace(fold, "fold: {by: row}")), "fit.groupBy.required"));

        Assertions.assertTrue(hasCode(compile(SOURCES, spec.replace(fold, "fold: {by: calendar}")), "fit.fold.by"));
        // a negative duration is its own error code, distinct from the fit.fold.purge info (reported once, where declared)
        final FeaturePlan negative = compile(SOURCES, spec.replace(fold, "fold: {by: time, purge: -P1D}"));
        Assertions.assertEquals(1, negative.getDiagnostics().getMessages().stream().filter(m -> "fit.fold.negative".equals(m.code())).count(), negative::describe);
        Assertions.assertTrue(negative.getDiagnostics().getMessages().stream().noneMatch(m -> "fit.fold.purge".equals(m.code()) && m.level() == Diagnostics.Level.error), negative::describe);
        Assertions.assertTrue(hasCode(compile(SOURCES, spec.replace(fold, "fold: {by: time, gap: P1D}")), "fit.fold"));
        Assertions.assertTrue(hasCode(compile(SOURCES, spec.replace(fold, "fold: {by: row, purge: P1D}")), "fit.fold.ignored"));
        Assertions.assertTrue(hasCode(compile(SOURCES, spec.replace("mode: fold, blocks: {bucket: month}", "mode: static, blocks: {bucket: month}")), "fit.fold.ignored"));
        // joint under a time fold: one error for the block, not one per keySet (enc has two)
        final FeaturePlan joint = compile(SOURCES, spec.replace("- {expr: \"sold >= 1\", stats: [mean]}",
                "- {expr: \"sold >= 1\", stats: [mean]}\n    shrinkage: {estimator: joint}"));
        Assertions.assertEquals(1, joint.getDiagnostics().getMessages().stream().filter(m -> "fit.fold.time.joint".equals(m.code())).count(), joint::describe);
    }

    /**
     * Calendar clocks: a window / decay / fit blocks measured in ticks of a clock declared in the sources — the
     * coordinates, the calendar attached to the columns, the plan hash covering the dates, and the validation.
     */
    @Test
    public void testCalendarClocks() {
        final String sources = SOURCES + "clocks:\n  - {name: business, type: calendar, dates: [2025-01-06, 2025-01-07, 2025-01-08, 2025-01-09, 2025-01-10]}\n";
        final String blocks = """
                  - name: days
                    scope: sequence
                    entity: seller
                    windows: [{maxAge: 3, clock: business}]
                    ops:
                      - {type: aggregate, field: start_price, funcs: [mean]}
                      - {type: ewma, field: start_price, halflife: [2], decayBy: business}
                  - name: enc_days
                    scope: population
                    type: encoding
                    keySets:
                      - keys: [category]
                        windows: [{maxAge: 7, clock: business}]
                    targets:
                      - {field: sold, stats: [mean]}
                    fit: {mode: forward, blocks: {size: 5, clock: business}}
                """;
        final String anchor = "  - name: vs_market\n";
        final String spec = SPEC.replace(anchor, blocks + anchor);
        final FeaturePlan plan = compile(sources, spec);
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);

        final OutputColumn mean = column(plan, "days_3business_start_price_mean");
        Assertions.assertEquals("3", mean.getCoordinates().get("maxAgeTicks"));
        Assertions.assertEquals("business", mean.getCoordinates().get("windowClock"));
        Assertions.assertNull(mean.getCoordinates().get("maxAge"));
        Assertions.assertEquals(5, mean.getClocks().get("business").size());
        final OutputColumn ewma = column(plan, "days_3business_start_price_ewma2");
        Assertions.assertEquals("business", ewma.getCoordinates().get("decayBy"));
        Assertions.assertNotNull(ewma.getClocks().get("business"));
        // forward blocks of 5 ticks; the keySet window of 7 ticks reads 2 blocks
        final OutputColumn level = plan.getColumns().stream().filter(c -> "enc_days".equals(c.getBlock()) && "encoding".equals(c.getOperator()) && "category".equals(c.getCoordinates().get("keys"))
                && c.getCoordinates().containsKey("fit")).findFirst().orElseThrow();
        Assertions.assertEquals("business", level.getCoordinates().get("blockClock"));
        Assertions.assertEquals("5", level.getCoordinates().get("blockTicks"));
        Assertions.assertEquals("2", level.getCoordinates().get("windowBlocks"));
        Assertions.assertNull(level.getCoordinates().get("blockSizeMillis"));
        Assertions.assertNotNull(level.getClocks().get("business"));

        // a time fold over the same calendar blocks: the purge rounds up by the clock's shortest tick spacing
        // (5 ticks of consecutive days = 5 days, so P7D covers 2 blocks)
        final FeaturePlan folded = compile(sources, spec.replace("fit: {mode: forward, blocks: {size: 5, clock: business}}",
                "fit: {mode: fold, blocks: {size: 5, clock: business}, fold: {by: time, purge: P7D}}"));
        Assertions.assertFalse(folded.getDiagnostics().hasErrors(), folded::describe);
        final OutputColumn foldLevel = folded.getColumns().stream().filter(c -> "enc_days".equals(c.getBlock()) && "encoding".equals(c.getOperator()) && "category".equals(c.getCoordinates().get("keys"))
                && c.getCoordinates().containsKey("fit")).findFirst().orElseThrow();
        Assertions.assertEquals("business", foldLevel.getCoordinates().get("blockClock"));
        Assertions.assertEquals("5", foldLevel.getCoordinates().get("blockTicks"));
        Assertions.assertEquals("time", foldLevel.getCoordinates().get("foldBy"));
        Assertions.assertEquals("2", foldLevel.getCoordinates().get("purgeBlocks"));

        // the calendar is part of the plan: another holiday, another hash
        Assertions.assertNotEquals(plan.getHash(), compile(sources.replace("2025-01-08, ", ""), spec).getHash());

        final Map<String, String> rejected = new java.util.LinkedHashMap<>();
        rejected.put(spec.replace("windows: [{maxAge: 3, clock: business}]", "windows: [{maxAge: 3, clock: exchange}]"), "clock.unknown");
        rejected.put(spec.replace("decayBy: business}", "decayBy: exchange}"), "clock.unknown");
        rejected.put(spec.replace("blocks: {size: 5, clock: business}", "blocks: {size: 5, clock: exchange}"), "clock.unknown");
        rejected.put(spec.replace("blocks: {size: 5, clock: business}", "blocks: {size: P7D}"), "clock.fit");
        rejected.put(spec.replace("blocks: {size: 5, clock: business}", "blocks: {bucket: month, clock: business}"), "fit.blocks.clock");
        rejected.put(spec.replace("windows: [{maxAge: 3, clock: business}]", "windows: [{maxAge: P3D, clock: business}]"), "window.clock");
        rejected.put(spec.replace("windows: [{maxAge: 3, clock: business}]", "windows: [{maxAge: 3, clock: events}]"), "window.clock");
        rejected.put(spec.replace("    entity: seller\n    windows: [{maxAge: 3, clock: business}]\n    ops:\n      - {type: aggregate, field: start_price, funcs: [mean]}\n      - {type: ewma, field: start_price, halflife: [2], decayBy: business}",
                "    entity: seller\n    direction: future\n    windows: [{maxAge: 3, clock: business}]\n    ops:\n      - {type: aggregate, field: start_price, funcs: [mean]}"), "clock.direction");
        for (final Map.Entry<String, String> e : rejected.entrySet()) {
            Assertions.assertNotEquals(spec, e.getKey(), e.getValue());
            final FeaturePlan bad = compile(sources, e.getKey());
            Assertions.assertTrue(hasCode(bad, e.getValue()), () -> e.getValue() + "\n" + bad.describe());
        }
    }

    /**
     * {@code family: bilinear}: one log-signature state per window over every channel, a column per Lyndon word (named by
     * channel letters); {@code compress: {svd}} fits an svd block over the component columns, which become intermediate.
     */
    @Test
    public void testLogSignatureAndCompress() {
        final String block = """
                  - name: path
                    scope: sequence
                    entity: seller
                    windows: [{maxEvents: 10}]
                    lift: {fields: [start_price, quantity], timeAugment: true}
                    summarize:
                      dynamics: {family: bilinear, type: logsignature, depth: 2, decayBy: time}
                    compress:
                      svd: {rank: 2}
                """;
        final String anchor = "  - name: vs_market\n";
        final String spec = SPEC.replace(anchor, block + anchor);
        final FeaturePlan plan = compile(SOURCES, spec);
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);

        // three letters (a = start_price, b = quantity, c = time), depth 2: a b c ab ac bc
        final List<String> components = plan.getColumns().stream().filter(c -> "path".equals(c.getBlock()) && "dynamics".equals(c.getOperator()))
                .map(OutputColumn::getCanonicalName).toList();
        Assertions.assertEquals(List.of("path_n10_logsig_a", "path_n10_logsig_b", "path_n10_logsig_c", "path_n10_logsig_ab", "path_n10_logsig_ac", "path_n10_logsig_bc"), components);
        final OutputColumn area = column(plan, "path_n10_logsig_ab");
        Assertions.assertEquals("bilinear", area.getCoordinates().get("family"));
        Assertions.assertEquals("start_price,quantity", area.getCoordinates().get("fields"));
        Assertions.assertEquals("true", area.getCoordinates().get("timeAugment"));
        Assertions.assertEquals("path_n10_logsig", area.getCoordinates().get("stateKey"));
        Assertions.assertEquals(Set.of("start_price", "quantity"), area.getPastInputs());
        Assertions.assertTrue(area.isIntermediate(), "compressed away");
        Assertions.assertTrue(hasCode(plan, "sequence.dynamics.logsignature"));
        // the svd block over the six components: two score columns, emitted
        final OutputColumn score = column(plan, "path_svd_1");
        Assertions.assertEquals("svd", score.getOperator());
        Assertions.assertEquals(String.join(",", components), score.getCoordinates().get("fields"));
        Assertions.assertFalse(score.isIntermediate());
        Assertions.assertTrue(hasCode(plan, "sequence.compress"));
        // keep: true emits the components too
        Assertions.assertFalse(column(compile(SOURCES, spec.replace("svd: {rank: 2}", "svd: {rank: 2}\n      keep: true")), "path_n10_logsig_ab").isIntermediate());

        final String dynamics = "dynamics: {family: bilinear, type: logsignature, depth: 2, decayBy: time}";
        final Map<String, String> rejected = new java.util.LinkedHashMap<>();
        rejected.put(spec.replace(dynamics, "dynamics: {family: bilinear, type: signature, depth: 2}"), "sequence.dynamics.type");
        rejected.put(spec.replace(dynamics, "dynamics: {family: bilinear, depth: 5}"), "sequence.dynamics.depth");
        rejected.put(spec.replace(dynamics, "dynamics: {family: bilinear, depth: 2, halflife: [3]}"), "sequence.dynamics.parameter");
        rejected.put(spec.replace(dynamics, "dynamics: {family: lti, measure: exponential, halflife: [3], depth: 2}"), "sequence.dynamics.parameter");
        rejected.put(spec.replace(dynamics, "dynamics: {family: probabilistic}"), "sequence.dynamics.family");
        rejected.put(spec.replace(dynamics, "dynamics: {family: bilinear, depth: 4}").replace("lift: {fields: [start_price, quantity], timeAugment: true}",
                "lift: {fields: [start_price, quantity, current_bid_t10], timeAugment: true}"), "sequence.dynamics.size");
        rejected.put(spec.replace("      svd: {rank: 2}", "      pca: {rank: 2}"), "sequence.compress");
        for (final Map.Entry<String, String> e : rejected.entrySet()) {
            Assertions.assertNotEquals(spec, e.getKey(), e.getValue());
            final FeaturePlan bad = compile(SOURCES, e.getKey());
            Assertions.assertTrue(hasCode(bad, e.getValue()), () -> e.getValue() + "\n" + bad.describe());
        }
        // one channel: its total increment only
        Assertions.assertTrue(hasCode(compile(SOURCES, spec.replace("lift: {fields: [start_price, quantity], timeAugment: true}", "lift: {fields: [start_price]}")
                .replace("    compress:\n      svd: {rank: 2}\n", "")), "sequence.dynamics.channels"));
    }
}
