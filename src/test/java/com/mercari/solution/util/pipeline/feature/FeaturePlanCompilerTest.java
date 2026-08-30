package com.mercari.solution.util.pipeline.feature;

import com.google.gson.JsonObject;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

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

    @Test
    public void testUnresolvedReferenceAndCycle() {
        final String spec = SPEC.replace("expr: \"start_price / quantity\"", "expr: \"start_price / nosuchfield\"");
        Assertions.assertTrue(hasCode(compile(SOURCES, spec), "reference.unresolved"));

        final String cyclic = SPEC.replace("expr: \"start_price / quantity\"", "expr: \"start_price / vs_market\"");
        final FeaturePlan plan = compile(SOURCES, cyclic);
        Assertions.assertTrue(hasCode(plan, "reference.cycle"), plan::describe);
        Assertions.assertTrue(plan.getDiagnostics().hasErrors());
    }

    @Test
    public void testSelfInOpExpressionIsRejected() {
        final String spec = SPEC.replace("expr: \"sold >= 1\", halflife: [5]", "expr: \"start_price - $self.start_price\", halflife: [5]");
        Assertions.assertTrue(hasCode(compile(SOURCES, spec), "sequence.self"));
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
        final String svd = SPEC.replace("type: encoding", "type: svd");
        Assertions.assertTrue(hasCode(compile(SOURCES, svd), "population.unsupported"));
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

        // quantile is not implemented: compile error rather than a runtime crash
        Assertions.assertTrue(hasCode(compile(SOURCES, SPEC.replace("stats: [mean]", "stats: [quantile]")), "encoding.stat.unsupported"));

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

}
