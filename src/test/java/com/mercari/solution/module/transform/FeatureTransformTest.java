package com.mercari.solution.module.transform;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * End-to-end test of the {@code feature} transform on a small online-auction dataset: sellers list items in
 * sessions (the co-occurrence context); {@code sold} / {@code final_price} are outcomes that become known
 * 30 minutes after the session and reach the system up to 6 days later.
 */
public class FeatureTransformTest {

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    private static final String SOURCE_CONFIG = """
            sources:
              - name: create
                module: create
                timestampAttribute: session_time
                parameters:
                  type: element
                  elements:
                    - {session_id: A, seller_id: s1, category: electronics, quantity: 2, start_price: 100.0, condition_grade: good, current_bid_t10: 120.0, sold: 1, final_price: 150.0, session_time: "2025-01-01T10:00:00Z"}
                    - {session_id: A, seller_id: s2, category: toys,        quantity: 1, start_price: 50.0,  condition_grade: fair, current_bid_t10: 55.0,  sold: 0, final_price: 0.0,   session_time: "2025-01-01T10:00:00Z"}
                    - {session_id: B, seller_id: s1, category: electronics, quantity: 1, start_price: 200.0, condition_grade: good, current_bid_t10: 210.0, sold: 0, final_price: 0.0,   session_time: "2025-01-03T10:00:00Z"}
                    - {session_id: C, seller_id: s1, category: electronics, quantity: 4, start_price: 80.0,  condition_grade: fair, current_bid_t10: 90.0,  sold: 1, final_price: 95.0,  session_time: "2025-01-20T10:00:00Z"}
                    - {session_id: C, seller_id: s2, category: toys,        quantity: 2, start_price: 60.0,  condition_grade: good, current_bid_t10: 70.0,  sold: 1, final_price: 72.0,  session_time: "2025-01-20T10:00:00Z"}
                    - {session_id: D, seller_id: s1, category: electronics, quantity: 1, start_price: 120.0, condition_grade: good, current_bid_t10: 130.0, sold: 1, final_price: 140.0, session_time: "2025-02-01T10:00:00Z"}
                schema:
                  fields:
                    - {name: session_id, type: string}
                    - {name: seller_id, type: string}
                    - {name: category, type: string}
                    - {name: quantity, type: int32}
                    - {name: start_price, type: float64}
                    - {name: condition_grade, type: string}
                    - {name: current_bid_t10, type: float64}
                    - {name: sold, type: int32}
                    - {name: final_price, type: float64}
                    - {name: session_time, type: timestamp}
            """;

    private static final String SOURCES_CONTRACT = """
                  sources:
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
                          - {name: quantity, type: int32}
                          - {name: start_price, type: float64, kind: attribute}
                          - {name: condition_grade, type: string}
                      - name: price_snapshots
                        eventTime: session_time
                        ingestionLag: PT1M
                        keys: [session_id, seller_id]
                        fields:
                          - {name: current_bid_t10, type: float64, availableAt: "event_time - PT10M", observedAtField: snapshot_time, kind: market}
                      - name: auction_results
                        eventTime: session_time
                        settlementLag: PT30M
                        ingestionLag: P6D
                        mutability: corrections
                        keys: [session_id, seller_id]
                        fields:
                          - {name: sold, type: int32, availableAt: after(event), kind: outcome}
                          - {name: final_price, type: float64, availableAt: after(event), kind: outcome}
            """;

    private static final String FEATURE_CONFIG = """
            transforms:
              - name: features
                module: feature
                inputs: [create]
                parameters:
            """ + SOURCES_CONTRACT + """
                  lineage:
                    - {fields: [session_id, seller_id, category, quantity, start_price, condition_grade], from: listings}
                    - {fields: [current_bid_t10], from: price_snapshots}
                    - {fields: [sold, final_price], from: auction_results}
                  time: {field: session_time, orderTieBreak: [session_id]}
                  predictAt: "event_time - PT8M"
                  entities:
                    - {name: seller, keys: [seller_id]}
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
                      derive: [month]
                      cyclical: true
                    - name: relative
                      scope: context
                      context: session
                      inputs: [start_price]
                      ops: [rank, shareOfTotal]
                    - name: composition
                      scope: context
                      context: session
                      ops:
                        - {type: countByValue, fields: [condition_grade]}
                        - {type: groupSize}
                    - name: recent
                      scope: sequence
                      entity: seller
                      windows:
                        - {maxEvents: 5}
                      ops:
                        - {type: lag, fields: [sold, start_price], k: 2}
                        - {type: aggregate, field: start_price, funcs: [count, mean]}
                        - {type: sinceEvent, predicate: "sold = 1", unit: [events, days]}
                    - name: vs_market
                      scope: row
                      type: residual
                      input: relative_start_price_shareOfTotal
                      baseline: market
                    - name: enc
                      scope: population
                      type: encoding
                      keySets:
                        - keys: [seller_id]
                      targets:
                        - {stats: [count]}
                        - {expr: "sold >= 1", stats: [mean]}
                  output:
                    prefix: f_
            """;

    @Test
    public void testFeatureTransform() throws java.io.IOException {
        final Config config = Config.load(SOURCE_CONFIG + FEATURE_CONFIG);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);
        final MCollection output = outputs.get("features");

        final Schema schema = output.getSchema();
        Assertions.assertNotNull(schema.getField("f_price_per_unit"));
        Assertions.assertEquals("windowShift", schema.getField("f_recent_n5_sold_lag1").getOptions().get("feature.status"));
        Assertions.assertEquals("outcome", schema.getField("f_enc__seller_id__e2__mean").getOptions().get("feature.derivedFrom"));

        PAssert.that(output.getCollection()).satisfies(rows -> {
            final Map<String, MElement> byKey = new HashMap<>();
            for (final MElement row : rows) {
                byKey.put(row.getAsString("session_id") + "/" + row.getAsString("seller_id"), row);
            }
            Assertions.assertEquals(6, byKey.size());

            // row scope
            Assertions.assertEquals(50.0, byKey.get("A/s1").getAsDouble("f_price_per_unit"), 1e-9);
            Assertions.assertEquals(Math.sin(2 * Math.PI / 12), byKey.get("A/s1").getAsDouble("f_time_parts_month_sin"), 1e-9);

            // context scope: rank / share within the session, group composition
            Assertions.assertEquals(1L, ((Number) byKey.get("A/s1").getPrimitiveValue("f_relative_start_price_rank")).longValue());
            Assertions.assertEquals(2L, ((Number) byKey.get("A/s2").getPrimitiveValue("f_relative_start_price_rank")).longValue());
            Assertions.assertEquals(100.0 / 150.0, byKey.get("A/s1").getAsDouble("f_relative_start_price_shareOfTotal"), 1e-9);
            Assertions.assertEquals(2L, ((Number) byKey.get("A/s1").getPrimitiveValue("f_composition_groupSize")).longValue());
            Assertions.assertEquals(1L, ((Number) byKey.get("D/s1").getPrimitiveValue("f_composition_groupSize")).longValue());
            final Map<?, ?> counts = (Map<?, ?>) byKey.get("A/s1").getPrimitiveValue("f_composition_condition_grade_countByValue");
            Assertions.assertEquals(2, counts.size());

            // residual against the market baseline (share of 1/bid within the session)
            final double marketA1 = (1 / 120.0) / (1 / 120.0 + 1 / 55.0);
            Assertions.assertEquals(100.0 / 150.0 - marketA1, byKey.get("A/s1").getAsDouble("f_vs_market"), 1e-9);

            // sequence scope, pre-event attribute: strictly past, no shift
            Assertions.assertNull(byKey.get("A/s1").getPrimitiveValue("f_recent_n5_start_price_lag1"));
            Assertions.assertEquals(100.0, byKey.get("B/s1").getAsDouble("f_recent_n5_start_price_lag1"), 1e-9);
            Assertions.assertEquals(200.0, byKey.get("C/s1").getAsDouble("f_recent_n5_start_price_lag1"), 1e-9);
            Assertions.assertEquals(100.0, byKey.get("C/s1").getAsDouble("f_recent_n5_start_price_lag2"), 1e-9);
            Assertions.assertEquals(2L, ((Number) byKey.get("C/s1").getPrimitiveValue("f_recent_n5_start_price_count")).longValue());
            Assertions.assertEquals(150.0, byKey.get("C/s1").getAsDouble("f_recent_n5_start_price_mean"), 1e-9);

            // sequence scope, outcome: the window near edge is shifted by settlementLag + ingestionLag + predict offset
            // (6 days 38 minutes), so the Jan 1 outcome is still unknown on Jan 3 but known on Jan 20
            Assertions.assertNull(byKey.get("B/s1").getPrimitiveValue("f_recent_n5_sold_lag1"));
            Assertions.assertEquals(0L, ((Number) byKey.get("C/s1").getPrimitiveValue("f_recent_n5_sold_lag1")).longValue());
            Assertions.assertEquals(1L, ((Number) byKey.get("C/s1").getPrimitiveValue("f_recent_n5_sold_lag2")).longValue());
            Assertions.assertEquals(1L, ((Number) byKey.get("D/s1").getPrimitiveValue("f_recent_n5_sold_lag1")).longValue());
            Assertions.assertNull(byKey.get("B/s1").getPrimitiveValue("f_recent_n5_since_events"));
            Assertions.assertEquals(2L, ((Number) byKey.get("C/s1").getPrimitiveValue("f_recent_n5_since_events")).longValue());
            Assertions.assertEquals(19.0, byKey.get("C/s1").getAsDouble("f_recent_n5_since_days"), 1e-6);

            // population scope, expanding encoding over the seller's past outcomes (same near-edge rule)
            Assertions.assertEquals(1L, ((Number) byKey.get("B/s1").getPrimitiveValue("f_enc__seller_id__count")).longValue());
            Assertions.assertNull(byKey.get("B/s1").getPrimitiveValue("f_enc__seller_id__e2__mean"));
            Assertions.assertEquals(0.5, byKey.get("C/s1").getAsDouble("f_enc__seller_id__e2__mean"), 1e-9);
            Assertions.assertEquals(2.0 / 3.0, byKey.get("D/s1").getAsDouble("f_enc__seller_id__e2__mean"), 1e-9);
            Assertions.assertEquals(3L, ((Number) byKey.get("D/s1").getPrimitiveValue("f_enc__seller_id__count")).longValue());
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testShrinkageAndShare() throws java.io.IOException {
        // seller-level mean of (sold >= 1) shrunk toward the global mean (leave-node-out), plus share = n_seller / n_global
        final String config = FEATURE_CONFIG
                .replace("- {stats: [count]}", "- {stats: [count, share]}")
                .replace("- {expr: \"sold >= 1\", stats: [mean]}",
                        "- {expr: \"sold >= 1\", stats: [mean]}\n          shrinkage: {priorWeight: 1, output: [composed, deviations]}");
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(SOURCE_CONFIG + config));
        final MCollection output = outputs.get("features");
        Assertions.assertNotNull(output.getSchema().getField("f_enc__seller_id__e2__mean"));
        Assertions.assertNotNull(output.getSchema().getField("f_enc__seller_id__e2__dev0"));
        Assertions.assertNull(output.getSchema().getField("enc__global__e2__n")); // hidden statistics are not emitted

        PAssert.that(output.getCollection()).satisfies(rows -> {
            final Map<String, MElement> byKey = new HashMap<>();
            for (final MElement row : rows) byKey.put(row.getAsString("session_id") + "/" + row.getAsString("seller_id"), row);

            // Jan 3: no outcome has reached the system yet (6 days 38 minutes) → nothing to shrink toward
            Assertions.assertNull(byKey.get("B/s1").getPrimitiveValue("f_enc__seller_id__e2__mean"));
            // Jan 20: s1 own = 1/2 (n=2), global without s1 = 0/1 → 0 + 2/3 · (1/2 − 0) = 1/3
            Assertions.assertEquals(1.0 / 3.0, byKey.get("C/s1").getAsDouble("f_enc__seller_id__e2__mean"), 1e-9);
            Assertions.assertEquals(1.0 / 3.0, byKey.get("C/s1").getAsDouble("f_enc__seller_id__e2__dev0"), 1e-9);
            // Jan 20: s2 own = 0 (n=1), global without s2 = 1/2 → 1/2 + 1/2 · (0 − 1/2) = 1/4
            Assertions.assertEquals(0.25, byKey.get("C/s2").getAsDouble("f_enc__seller_id__e2__mean"), 1e-9);
            // Feb 1: s1 own = 2/3 (n=3), global without s1 = 1/2 → 1/2 + 3/4 · (2/3 − 1/2) = 0.625
            Assertions.assertEquals(0.625, byKey.get("D/s1").getAsDouble("f_enc__seller_id__e2__mean"), 1e-9);

            // share of the seller's rows among all strictly-past rows (no outcome involved → no shift)
            Assertions.assertNull(byKey.get("A/s1").getPrimitiveValue("f_enc__seller_id__share"));
            Assertions.assertEquals(0.5, byKey.get("B/s1").getAsDouble("f_enc__seller_id__share"), 1e-9);
            Assertions.assertEquals(2.0 / 3.0, byKey.get("C/s1").getAsDouble("f_enc__seller_id__share"), 1e-9);
            Assertions.assertEquals(0.6, byKey.get("D/s1").getAsDouble("f_enc__seller_id__share"), 1e-9);
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testVarianceComponents() throws java.io.IOException {
        // seller means [3/4, 1/2] are closer than the within-seller noise: τ² truncates to 0 → λ = ∞ → full shrinkage,
        // so every row gets the leave-node-out global mean
        final String config = FEATURE_CONFIG.replace("- {expr: \"sold >= 1\", stats: [mean]}",
                "- {expr: \"sold >= 1\", stats: [mean]}\n          shrinkage: {weights: varianceComponents, priorWeight: 1}");
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(SOURCE_CONFIG + config));
        PAssert.that(outputs.get("features").getCollection()).satisfies(rows -> {
            final Map<String, MElement> byKey = new HashMap<>();
            for (final MElement row : rows) byKey.put(row.getAsString("session_id") + "/" + row.getAsString("seller_id"), row);
            Assertions.assertNull(byKey.get("B/s1").getPrimitiveValue("f_enc__seller_id__e2__mean"));
            Assertions.assertEquals(0.0, byKey.get("C/s1").getAsDouble("f_enc__seller_id__e2__mean"), 1e-9);
            Assertions.assertEquals(0.5, byKey.get("C/s2").getAsDouble("f_enc__seller_id__e2__mean"), 1e-9);
            Assertions.assertEquals(0.5, byKey.get("D/s1").getAsDouble("f_enc__seller_id__e2__mean"), 1e-9);
            return null;
        });
        pipeline.run();
    }

    private static String staticConfig(final String artifactDir) {
        return FEATURE_CONFIG
                .replace("- {stats: [count]}", "- {stats: [count]}")
                .replace("- {expr: \"sold >= 1\", stats: [mean]}",
                        "- {expr: \"sold >= 1\", stats: [mean, std]}\n          shrinkage: {priorWeight: 1}")
                .replace("      output:\n", "      fit: {mode: static, artifact: {uri: \"" + artifactDir + "\"}}\n      output:\n");
    }

    @Test
    public void testStaticFitWritesAndReusesArtifact() throws java.io.IOException {
        final String dir = "target/feature-artifacts/" + java.util.UUID.randomUUID(); // relative: Beam FileSystems treats a Windows drive letter as a scheme
        final String config = staticConfig(dir);

        // run 1: fit on the whole input, apply, and persist the statistics
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(SOURCE_CONFIG + config));
        PAssert.that(outputs.get("features").getCollection()).satisfies(rows -> {
            final Map<String, MElement> byKey = new HashMap<>();
            for (final MElement row : rows) byKey.put(row.getAsString("session_id") + "/" + row.getAsString("seller_id"), row);
            // static: every row of a seller sees the seller's full statistics (including its own outcome)
            Assertions.assertEquals(4L, ((Number) byKey.get("A/s1").getPrimitiveValue("f_enc__seller_id__count")).longValue());
            Assertions.assertEquals(2L, ((Number) byKey.get("C/s2").getPrimitiveValue("f_enc__seller_id__count")).longValue());
            // s1: own 3/4 (n=4), global without s1 = 1/2 → 1/2 + 4/5 · (3/4 − 1/2) = 0.7
            Assertions.assertEquals(0.7, byKey.get("A/s1").getAsDouble("f_enc__seller_id__e2__mean"), 1e-9);
            Assertions.assertEquals(0.7, byKey.get("D/s1").getAsDouble("f_enc__seller_id__e2__mean"), 1e-9);
            // s2: own 1/2 (n=2), global without s2 = 3/4 → 3/4 + 2/3 · (1/2 − 3/4) = 0.5833
            Assertions.assertEquals(0.75 + 2.0 / 3.0 * (0.5 - 0.75), byKey.get("A/s2").getAsDouble("f_enc__seller_id__e2__mean"), 1e-9);
            // std of [1,0,1,1] = sqrt(3/4 − 9/16)
            Assertions.assertEquals(Math.sqrt(0.75 - 0.5625), byKey.get("A/s1").getAsDouble("f_enc__seller_id__e2__std"), 1e-9);
            return null;
        });
        pipeline.run();

        final java.io.File[] files = new java.io.File(dir).listFiles();
        Assertions.assertNotNull(files, "artifact directory missing: " + dir);
        Assertions.assertEquals(1, files.length, "one plan hash directory expected");
        Assertions.assertTrue(new java.io.File(files[0], "enc.avro").exists());
        Assertions.assertTrue(new java.io.File(files[0], "enc.manifest.json").exists());

        // run 2: same plan hash, different rows → the artifact is applied instead of re-fitting
        final String subset = SOURCE_CONFIG
                .replace("        - {session_id: A, seller_id: s1, category: electronics, quantity: 2, start_price: 100.0, condition_grade: good, current_bid_t10: 120.0, sold: 1, final_price: 150.0, session_time: \"2025-01-01T10:00:00Z\"}\n", "")
                .replace("        - {session_id: C, seller_id: s1, category: electronics, quantity: 4, start_price: 80.0,  condition_grade: fair, current_bid_t10: 90.0,  sold: 1, final_price: 95.0,  session_time: \"2025-01-20T10:00:00Z\"}\n", "");
        final TestPipeline second = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        final Map<String, MCollection> reused = MPipeline.apply(second, Config.load(subset + config));
        PAssert.that(reused.get("features").getCollection()).satisfies(rows -> {
            int n = 0;
            for (final MElement row : rows) {
                n++;
                if ("s1".equals(row.getAsString("seller_id"))) {
                    Assertions.assertEquals(4L, ((Number) row.getPrimitiveValue("f_enc__seller_id__count")).longValue());
                    Assertions.assertEquals(0.7, row.getAsDouble("f_enc__seller_id__e2__mean"), 1e-9);
                }
            }
            Assertions.assertEquals(4, n);
            return null;
        });
        second.run();
    }

    /**
     * A static fit is over the whole input even when the module declares a non-global windowing strategy:
     * the statistics are computed in the global window, the artifact is still written, and the windowed rows
     * see the same fitted values as under the default strategy.
     */
    @Test
    public void testStaticFitUnderFixedWindows() throws java.io.IOException {
        final String dir = "target/feature-artifacts/" + java.util.UUID.randomUUID();
        final String config = staticConfig(dir)
                .replace("    inputs: [create]\n    parameters:\n", "    inputs: [create]\n    strategy:\n      window: {type: fixed, unit: day, size: 1, offset: 0}\n    parameters:\n");
        Assertions.assertTrue(config.contains("type: fixed"), config);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(SOURCE_CONFIG + config));
        PAssert.that(outputs.get("features").getCollection()).satisfies(rows -> {
            final Map<String, MElement> byKey = new HashMap<>();
            for (final MElement row : rows) byKey.put(row.getAsString("session_id") + "/" + row.getAsString("seller_id"), row);
            Assertions.assertEquals(6, byKey.size());
            // same values as testStaticFitWritesAndReusesArtifact: the fit ignores the daily windows
            Assertions.assertEquals(4L, ((Number) byKey.get("A/s1").getPrimitiveValue("f_enc__seller_id__count")).longValue());
            Assertions.assertEquals(0.7, byKey.get("A/s1").getAsDouble("f_enc__seller_id__e2__mean"), 1e-9);
            Assertions.assertEquals(0.7, byKey.get("D/s1").getAsDouble("f_enc__seller_id__e2__mean"), 1e-9);
            Assertions.assertEquals(0.75 + 2.0 / 3.0 * (0.5 - 0.75), byKey.get("A/s2").getAsDouble("f_enc__seller_id__e2__mean"), 1e-9);
            return null;
        });
        pipeline.run();
        final java.io.File[] files = new java.io.File(dir).listFiles();
        Assertions.assertNotNull(files, "artifact directory missing: " + dir);
        Assertions.assertTrue(new java.io.File(files[0], "enc.avro").exists());
    }

    /**
     * fit.mode fold with entity folds: a seller's rows never see the seller's own statistics (its whole
     * entity is its fold), and the global level holds only the other folds' rows. The expected values are
     * derived from the same fold assignment the engine uses.
     */
    @Test
    public void testFoldFit() throws java.io.IOException {
        final java.util.function.IntFunction<Integer> foldOf = folds -> com.mercari.solution.util.pipeline.feature.VarianceComponents.foldOf(
                com.mercari.solution.util.pipeline.feature.FeatureValues.key(Map.of("seller_id", "s1"), List.of("seller_id")), folds);
        final java.util.function.IntFunction<Integer> foldOf2 = folds -> com.mercari.solution.util.pipeline.feature.VarianceComponents.foldOf(
                com.mercari.solution.util.pipeline.feature.FeatureValues.key(Map.of("seller_id", "s2"), List.of("seller_id")), folds);
        int folds = 2;
        while (foldOf.apply(folds).equals(foldOf2.apply(folds))) folds++; // the two sellers must land in different folds
        final String dir = "target/feature-artifacts/" + java.util.UUID.randomUUID();
        final String config = staticConfig(dir).replace("mode: static", "mode: fold, folds: " + folds + ", groupBy: seller");
        Assertions.assertTrue(config.contains("mode: fold"), config);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(SOURCE_CONFIG + config));
        PAssert.that(outputs.get("features").getCollection()).satisfies(rows -> {
            final Map<String, MElement> byKey = new HashMap<>();
            for (final MElement row : rows) byKey.put(row.getAsString("session_id") + "/" + row.getAsString("seller_id"), row);
            Assertions.assertEquals(6, byKey.size());
            for (final MElement row : byKey.values()) {
                // the seller level is the row's own fold: nothing remains out of fold
                Assertions.assertEquals(0L, ((Number) row.getPrimitiveValue("f_enc__seller_id__count")).longValue(), row::toString);
                Assertions.assertNull(row.getPrimitiveValue("f_enc__seller_id__e2__std"), row::toString);
            }
            // shrinkage with an empty leaf falls back to the out-of-fold global mean: s1 sees s2's rows
            // (sold 0, 1 → 0.5) and s2 sees s1's rows (sold 1, 0, 1, 1 → 0.75)
            Assertions.assertEquals(0.5, byKey.get("A/s1").getAsDouble("f_enc__seller_id__e2__mean"), 1e-9);
            Assertions.assertEquals(0.5, byKey.get("D/s1").getAsDouble("f_enc__seller_id__e2__mean"), 1e-9);
            Assertions.assertEquals(0.75, byKey.get("A/s2").getAsDouble("f_enc__seller_id__e2__mean"), 1e-9);
            Assertions.assertEquals(0.75, byKey.get("C/s2").getAsDouble("f_enc__seller_id__e2__mean"), 1e-9);
            return null;
        });
        pipeline.run();
        // the artifact holds the whole-input statistics (a static serving run can load them)
        final java.io.File[] files = new java.io.File(dir).listFiles();
        Assertions.assertNotNull(files, "artifact directory missing: " + dir);
        Assertions.assertTrue(new java.io.File(files[0], "enc.avro").exists());
    }

    /** countByValue with values: numeric per-value columns (sink-friendly); output.passThrough: keys keeps only join keys. */
    @Test
    public void testPerValueColumnsAndPassThroughKeys() throws java.io.IOException {
        final String config = FEATURE_CONFIG
                .replace("- {type: countByValue, fields: [condition_grade]}", "- {type: countByValue, fields: [condition_grade], values: [good, fair]}")
                .replace("      output:\n        prefix: f_", "      output:\n        prefix: f_\n        passThrough: keys");
        Assertions.assertTrue(config.contains("passThrough: keys"));
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(SOURCE_CONFIG + config));
        final Schema schema = outputs.get("features").getSchema();
        // keys pass through, other inputs (including outcomes) do not
        Assertions.assertNotNull(schema.getField("session_id"));
        Assertions.assertNotNull(schema.getField("seller_id"));
        Assertions.assertNotNull(schema.getField("session_time"));
        Assertions.assertNull(schema.getField("start_price"));
        Assertions.assertNull(schema.getField("sold"));
        Assertions.assertNull(schema.getField("f_composition_condition_grade_countByValue"));
        Assertions.assertEquals(Schema.Type.int64, schema.getField("f_composition_condition_grade_countByValue_good").getFieldType().getType());
        PAssert.that(outputs.get("features").getCollection()).satisfies(rows -> {
            final Map<String, MElement> byKey = new HashMap<>();
            for (final MElement row : rows) byKey.put(row.getAsString("session_id") + "/" + row.getAsString("seller_id"), row);
            Assertions.assertEquals(6, byKey.size());
            // session A: s1 good + s2 fair
            Assertions.assertEquals(1L, ((Number) byKey.get("A/s1").getPrimitiveValue("f_composition_condition_grade_countByValue_good")).longValue());
            Assertions.assertEquals(1L, ((Number) byKey.get("A/s1").getPrimitiveValue("f_composition_condition_grade_countByValue_fair")).longValue());
            // session B: s1 good only -> fair absent = 0
            Assertions.assertEquals(1L, ((Number) byKey.get("B/s1").getPrimitiveValue("f_composition_condition_grade_countByValue_good")).longValue());
            Assertions.assertEquals(0L, ((Number) byKey.get("B/s1").getPrimitiveValue("f_composition_condition_grade_countByValue_fair")).longValue());
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testFactorization() throws java.io.IOException {
        final String dir = "target/feature-artifacts/" + java.util.UUID.randomUUID();
        final String fm = """
                    - name: fm
                      scope: population
                      type: factorization
                      variant: fwfm
                      fields: [seller_id, category, condition_grade]
                      latentDim: 2
                      task: {expr: "sold >= 1"}
                      fit: {artifact: "%s"}
                      als: {epochs: 20, reg: 0.1, seed: 1}
                      outputs:
                        - {pair: [seller_id, category], as: fm_seller_category}
                        - {embedding: condition_grade, as: grade_emb, dims: 2}
                        - {sum: true, as: fm_linear}
                """.formatted(dir);
        final String config = FEATURE_CONFIG.replace("      output:\n", fm.replaceAll("(?m)^", "    ") + "      output:\n");
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(SOURCE_CONFIG + config));
        final MCollection output = outputs.get("features");
        Assertions.assertNotNull(output.getSchema().getField("f_fm_seller_category"));
        Assertions.assertNotNull(output.getSchema().getField("f_grade_emb_1"));
        PAssert.that(output.getCollection()).satisfies(rows -> {
            final Map<String, MElement> byKey = new HashMap<>();
            for (final MElement row : rows) {
                byKey.put(row.getAsString("session_id") + "/" + row.getAsString("seller_id"), row);
                Assertions.assertNotNull(row.getPrimitiveValue("f_fm_seller_category"), row.toString());
                Assertions.assertNotNull(row.getPrimitiveValue("f_grade_emb_0"), row.toString());
                Assertions.assertNotNull(row.getPrimitiveValue("f_fm_linear"), row.toString());
            }
            Assertions.assertEquals(6, byKey.size());
            // A, B and D share the same field values (s1 / electronics / good) with targets 1, 0, 1:
            // the model is a pure function of the fields, so they get one prediction near their mean
            Assertions.assertEquals(byKey.get("A/s1").getAsDouble("f_fm_linear"), byKey.get("B/s1").getAsDouble("f_fm_linear"), 1e-9);
            Assertions.assertEquals(2.0 / 3.0, byKey.get("A/s1").getAsDouble("f_fm_linear"), 0.15);
            // C/s2 (s2 / toys / good, sold) and A/s2 (s2 / toys / fair, unsold) differ only by grade
            Assertions.assertTrue(byKey.get("C/s2").getAsDouble("f_fm_linear") > byKey.get("A/s2").getAsDouble("f_fm_linear"));
            Assertions.assertNotEquals(byKey.get("C/s2").getAsDouble("f_grade_emb_0"), byKey.get("A/s2").getAsDouble("f_grade_emb_0"));
            return null;
        });
        pipeline.run();
        final java.io.File[] dirs = new java.io.File(dir).listFiles();
        Assertions.assertNotNull(dirs);
        Assertions.assertTrue(new java.io.File(dirs[0], "fm.fm.avro").exists());
        Assertions.assertTrue(new java.io.File(dirs[0], "fm.fm.manifest.json").exists());
        Assertions.assertTrue(java.nio.file.Files.readString(new java.io.File(dirs[0], "fm.fm.manifest.json").toPath()).contains("pairWeights"));
    }

    @Test
    public void testDiscretize() throws java.io.IOException {
        final String dir = "target/feature-artifacts/" + java.util.UUID.randomUUID();
        // start_price over the 6 rows: 50, 60, 80, 100, 120, 200 -> tercile edges 73.3 / 106.7 (type-7 quantiles)
        final String blocks = """
                    - name: price_bin
                      scope: population
                      type: discretize
                      input: start_price
                      bins: 3
                      fit: {artifact: "%s"}
                    - name: by_bin
                      scope: population
                      type: encoding
                      keySets:
                        - keys: [price_bin]
                      targets:
                        - {stats: [count]}
                """.formatted(dir);
        final String config = FEATURE_CONFIG.replace("      output:\n", blocks.replaceAll("(?m)^", "    ") + "      output:\n");
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(SOURCE_CONFIG + config));
        final MCollection output = outputs.get("features");
        Assertions.assertEquals(Schema.FieldType.INT64.getType(), output.getSchema().getField("f_price_bin").getFieldType().getType());
        Assertions.assertNotNull(output.getSchema().getField("f_by_bin__price_bin__count"));
        PAssert.that(output.getCollection()).satisfies(rows -> {
            final Map<String, MElement> byKey = new HashMap<>();
            for (final MElement row : rows) byKey.put(row.getAsString("session_id") + "/" + row.getAsString("seller_id"), row);
            Assertions.assertEquals(6, byKey.size());
            final Map<String, Long> bins = Map.of("A/s1", 2L, "A/s2", 1L, "B/s1", 3L, "C/s1", 2L, "C/s2", 1L, "D/s1", 3L);
            for (final Map.Entry<String, Long> e : bins.entrySet()) {
                Assertions.assertEquals(e.getValue(), ((Number) byKey.get(e.getKey()).getPrimitiveValue("f_price_bin")).longValue(), e.getKey());
            }
            // the bins key an expanding encoding: strictly-past rows of the same bin
            final Map<String, Long> counts = Map.of("A/s1", 0L, "A/s2", 0L, "B/s1", 0L, "C/s1", 1L, "C/s2", 1L, "D/s1", 1L);
            for (final Map.Entry<String, Long> e : counts.entrySet()) {
                Assertions.assertEquals(e.getValue(), ((Number) byKey.get(e.getKey()).getPrimitiveValue("f_by_bin__price_bin__count")).longValue(), e.getKey());
            }
            return null;
        });
        pipeline.run();
        final java.io.File[] dirs = new java.io.File(dir).listFiles();
        Assertions.assertNotNull(dirs);
        final java.io.File artifact = new java.io.File(dirs[0], "price_bin.bins.json");
        Assertions.assertTrue(artifact.exists());
        final com.google.gson.JsonObject json = com.google.gson.JsonParser.parseString(java.nio.file.Files.readString(artifact.toPath())).getAsJsonObject();
        Assertions.assertEquals(3, json.get("bins").getAsInt());
        Assertions.assertEquals(50.0, json.get("min").getAsDouble(), 1e-9);
        Assertions.assertEquals(200.0, json.get("max").getAsDouble(), 1e-9);
        Assertions.assertEquals(2, json.getAsJsonArray("edges").size());
        Assertions.assertEquals(60 + 20 * 2 / 3d, json.getAsJsonArray("edges").get(0).getAsDouble(), 1e-9);
        Assertions.assertEquals(100 + 20 / 3d, json.getAsJsonArray("edges").get(1).getAsDouble(), 1e-9);
    }

    @Test
    public void testQuantileStat() throws java.io.IOException {
        // expanding median / first quartile of the seller's past start prices
        final String config = FEATURE_CONFIG.replace("            - {expr: \"sold >= 1\", stats: [mean]}\n",
                "            - {expr: \"sold >= 1\", stats: [mean]}\n            - {field: start_price, stats: [quantile, q25]}\n");
        Assertions.assertNotEquals(FEATURE_CONFIG, config);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(SOURCE_CONFIG + config));
        final MCollection output = outputs.get("features");
        Assertions.assertNotNull(output.getSchema().getField("f_enc__seller_id__start_price__quantile"));
        PAssert.that(output.getCollection()).satisfies(rows -> {
            final Map<String, MElement> byKey = new HashMap<>();
            for (final MElement row : rows) byKey.put(row.getAsString("session_id") + "/" + row.getAsString("seller_id"), row);
            Assertions.assertEquals(6, byKey.size());
            // s1: 100 (A) -> 200 (B) -> 80 (C) -> 120 (D); s2: 50 (A) -> 60 (C)
            Assertions.assertNull(byKey.get("A/s1").getPrimitiveValue("f_enc__seller_id__start_price__quantile"));
            Assertions.assertEquals(100.0, byKey.get("B/s1").getAsDouble("f_enc__seller_id__start_price__quantile"), 1e-9);
            Assertions.assertEquals(150.0, byKey.get("C/s1").getAsDouble("f_enc__seller_id__start_price__quantile"), 1e-9);
            Assertions.assertEquals(100.0, byKey.get("D/s1").getAsDouble("f_enc__seller_id__start_price__quantile"), 1e-9);
            Assertions.assertEquals(90.0, byKey.get("D/s1").getAsDouble("f_enc__seller_id__start_price__q25"), 1e-9);
            Assertions.assertNull(byKey.get("A/s2").getPrimitiveValue("f_enc__seller_id__start_price__quantile"));
            Assertions.assertEquals(50.0, byKey.get("C/s2").getAsDouble("f_enc__seller_id__start_price__quantile"), 1e-9);
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testAvroInputWithoutTimestampAttributeAndKeyedFirstStage() throws java.io.IOException {
        // Avro-typed input, no timestampAttribute (all elements share the default timestamp), and a spec whose
        // first block is keyed: rows must still be converted to the element form and ordered by time.field
        final String source = SOURCE_CONFIG.replace("        timestampAttribute: session_time\n", "        outputType: avro\n");
        final String config = FEATURE_CONFIG.replace("      output:\n        prefix: f_", "      output:\n        prefix: f_\n        nullPolicy: fillZero");
        // make a sequence block the first feature
        final String reordered = config.replace("      features:\n", "      features:\n        - name: first\n          scope: sequence\n          entity: seller\n          ops:\n            - {type: lag, fields: [start_price], k: 1}\n");
        Assertions.assertNotEquals(config, reordered);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(source + reordered));
        PAssert.that(outputs.get("features").getCollection()).satisfies(rows -> {
            final Map<String, MElement> byKey = new HashMap<>();
            for (final MElement row : rows) byKey.put(row.getAsString("session_id") + "/" + row.getAsString("seller_id"), row);
            Assertions.assertEquals(6, byKey.size());
            // ordering comes from time.field, not from the element timestamp
            Assertions.assertEquals(100.0, byKey.get("B/s1").getAsDouble("f_first_all_start_price_lag1"), 1e-9);
            Assertions.assertEquals(200.0, byKey.get("C/s1").getAsDouble("f_first_all_start_price_lag1"), 1e-9);
            Assertions.assertEquals(0L, ((Number) byKey.get("C/s1").getPrimitiveValue("f_recent_n5_sold_lag1")).longValue());
            // fillZero: missing numeric features become 0 instead of null
            Assertions.assertEquals(0.0, byKey.get("A/s1").getAsDouble("f_first_all_start_price_lag1"), 1e-9);
            Assertions.assertEquals(0L, ((Number) byKey.get("A/s1").getPrimitiveValue("f_recent_n5_sold_lag1")).longValue());
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testLeakIsRejectedAtAssembly() throws java.io.IOException {
        // a row feature that reads an outcome directly is available after predictAt → compile error
        final String leaking = FEATURE_CONFIG.replace("expr: \"start_price / quantity\"", "expr: \"final_price / quantity\"");
        final Config config = Config.load(SOURCE_CONFIG + leaking);
        final IllegalModuleException e = Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, config));
        Assertions.assertTrue(e.getMessage().contains("availability.violation"), e.getMessage());
    }

    @Test
    public void testGroupedOutput() throws java.io.IOException {
        final String grouped = FEATURE_CONFIG.replace("prefix: f_", "prefix: f_\n        groupBy: session\n        parentFields: [session_time]");
        final Config config = Config.load(SOURCE_CONFIG + grouped);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);
        final MCollection output = outputs.get("features");

        final Schema schema = output.getSchema();
        Assertions.assertNotNull(schema.getField("rows"));
        Assertions.assertNotNull(schema.getField("f_composition_groupSize"));
        Assertions.assertNull(schema.getField("f_price_per_unit"));

        PAssert.that(output.getCollection()).satisfies(rows -> {
            int sessions = 0;
            for (final MElement row : rows) {
                sessions++;
                final List<?> children = (List<?>) row.getPrimitiveValue("rows");
                final long size = ((Number) row.getPrimitiveValue("f_composition_groupSize")).longValue();
                Assertions.assertEquals(size, children.size());
            }
            Assertions.assertEquals(4, sessions);
            return null;
        });
        pipeline.run();
    }

    // ---------------------------------------------------------------------------------------------
    // parallel waves (engine doc §9.4): independent stages branch and are merged back by row id
    // ---------------------------------------------------------------------------------------------

    /**
     * Extra blocks: a row expression over an outcome shared by the seller and the category blocks (placed in the
     * seller stage, the category branch must recompute it), a category encoding (independent of the seller stage)
     * and a context block reading both keyed stages.
     */
    private static final String CAT_BLOCK = """
        - name: won
          scope: row
          expr: "sold >= 1"
        - name: cat
          scope: population
          type: encoding
          keySets:
            - keys: [category]
          targets:
            - {stats: [count]}
            - {field: won, stats: [mean]}
""";
    private static final String HIST_REL_BLOCK = """
        - name: histRel
          scope: context
          context: session
          inputs: [recent_n5_start_price_count, cat__category__count]
          ops: [zscore]
""";

    /** wave 1 = session context + seller keyed + category keyed, wave 2 = the histRel context stage (folded merge). */
    private static final String PARALLEL_CONFIG = FEATURE_CONFIG
            .replace("            - {type: aggregate, field: start_price, funcs: [count, mean]}\n",
                    "            - {type: aggregate, field: start_price, funcs: [count, mean]}\n            - {type: aggregate, field: won, funcs: [mean]}\n")
            .replace("      output:\n", CAT_BLOCK + HIST_REL_BLOCK + "      output:\n");

    /** Renders a row as a canonical string (sorted fields, sorted nested maps) so two engine modes can be compared. */
    static String canonical(final Object value) {
        if (value instanceof MElement element) return canonical(element.asPrimitiveMap());
        if (value instanceof Map<?, ?> map) {
            final TreeMap<String, String> sorted = new TreeMap<>();
            for (final Map.Entry<?, ?> e : map.entrySet()) sorted.put(String.valueOf(e.getKey()), canonical(e.getValue()));
            return sorted.toString();
        }
        if (value instanceof List<?> list) {
            final List<String> items = new ArrayList<>();
            for (final Object o : list) items.add(canonical(o));
            java.util.Collections.sort(items);
            return items.toString();
        }
        return String.valueOf(value);
    }

    static class TagDoFn extends DoFn<MElement, KV<String, String>> {
        private final String tag;
        TagDoFn(final String tag) { this.tag = tag; }
        @ProcessElement
        public void processElement(final ProcessContext c) {
            c.output(KV.of(tag, canonical(c.element())));
        }
    }

    /** Full names of the pipeline's transforms, to check which merge path the engine chose. */
    private Set<String> transformNames() {
        final Set<String> names = new HashSet<>();
        pipeline.traverseTopologically(new org.apache.beam.sdk.Pipeline.PipelineVisitor.Defaults() {
            @Override
            public CompositeBehavior enterCompositeTransform(final org.apache.beam.sdk.runners.TransformHierarchy.Node node) {
                names.add(node.getFullName());
                return CompositeBehavior.ENTER_TRANSFORM;
            }
            @Override
            public void visitPrimitiveTransform(final org.apache.beam.sdk.runners.TransformHierarchy.Node node) {
                names.add(node.getFullName());
            }
        });
        return names;
    }

    private static boolean hasTransform(final Set<String> names, final String module, final String needle) {
        return names.stream().anyMatch(n -> n.startsWith(module + "/") && n.contains(needle));
    }

    /**
     * Runs the feature config in both engine modes on one pipeline and asserts identical outputs; {@code expected}
     * / {@code forbidden} name the engine transforms the parallel graph must / must not contain.
     */
    private void assertParallelMatchesLinear(final String featureConfig, final int expectedRows,
                                             final List<String> expected, final List<String> forbidden) throws java.io.IOException {
        final String linear = featureConfig
                .replace("name: features", "name: linear")
                .replace("      lineage:", "      engine: {parallelWaves: false}\n      lineage:")
                .replace("transforms:\n", "");
        final Config config = Config.load(SOURCE_CONFIG + featureConfig + linear);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);
        final Set<String> names = transformNames();
        for (final String e : expected) Assertions.assertTrue(hasTransform(names, "features", e), () -> e + " missing in " + names);
        for (final String f : forbidden) Assertions.assertFalse(hasTransform(names, "features", f), () -> f + " present in " + names);
        Assertions.assertFalse(hasTransform(names, "linear", "Wave"), "the linear chain must not branch");
        Assertions.assertFalse(hasTransform(names, "linear", "RowId_Pin"));
        final PCollection<KV<String, String>> a = outputs.get("features").getCollection().apply("TagParallel", ParDo.of(new TagDoFn("parallel")));
        final PCollection<KV<String, String>> b = outputs.get("linear").getCollection().apply("TagLinear", ParDo.of(new TagDoFn("linear")));
        PAssert.that(PCollectionList.of(a).and(b).apply(Flatten.pCollections())).satisfies(kvs -> {
            final Set<String> parallel = new HashSet<>(), lin = new HashSet<>();
            for (final KV<String, String> kv : kvs) (kv.getKey().equals("parallel") ? parallel : lin).add(kv.getValue());
            Assertions.assertEquals(expectedRows, parallel.size(), parallel::toString);
            Assertions.assertEquals(lin, parallel);
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testParallelWavesFoldIntoContextStage() throws java.io.IOException {
        // wave 1 (session context, seller keyed, category keyed) merges inside the histRel context GroupByKey
        assertParallelMatchesLinear(PARALLEL_CONFIG, 6, List.of("RowId_Pin", "Wave1_FanIn"), List.of("Wave1_Merge"));
    }

    @Test
    public void testParallelWavesFoldWithVarianceComponents() throws java.io.IOException {
        // the category encoding is shrunk with variance-components weights: its compose column (read by the output
        // only) lands in the last stage, the histRel context stage, whose lambda estimate is then taken over the
        // wave input so the merge still rides that stage's GroupByKey
        final String vc = PARALLEL_CONFIG.replace("            - {field: won, stats: [mean]}\n",
                "            - {field: won, stats: [mean]}\n          shrinkage: {weights: varianceComponents, priorWeight: 1}\n");
        assertParallelMatchesLinear(vc, 6, List.of("RowId_Pin", "Wave1_FanIn", "_context_Vc"), List.of("Wave1_Merge"));
    }

    @Test
    public void testParallelWavesWithDeclaredRowId() throws java.io.IOException {
        // engine.rowId names the natural key: no Reshuffle, same result
        assertParallelMatchesLinear(PARALLEL_CONFIG.replace("      lineage:", "      engine: {rowId: [session_id, seller_id]}\n      lineage:"), 6,
                List.of("Wave1_FanIn"), List.of("RowId_Pin", "Wave1_Merge"));
    }

    @Test
    public void testParallelWavesRowIdMerge() throws java.io.IOException {
        // shrinkage lattice: the seller / global levels and the session context branch; the category stage
        // (a keyed stage hosting the compose rows) follows, so the merge is a row-id GroupByKey
        final String lattice = FEATURE_CONFIG
                .replace("- {expr: \"sold >= 1\", stats: [mean]}",
                        "- {expr: \"sold >= 1\", stats: [mean]}\n          shrinkage: {priorWeight: 1, output: [composed, deviations]}")
                .replace("- keys: [seller_id]", "- keys: [seller_id]\n          hierarchy: [[category], []]");
        assertParallelMatchesLinear(lattice, 6, List.of("RowId_Pin", "Wave1_Merge"), List.of("Wave1_FanIn"));
    }

    @Test
    public void testParallelWavesFoldIntoGroupedFinalize() throws java.io.IOException {
        // two independent keyed blocks and output.groupBy: the last wave merges inside the finalize GroupByKey
        // (vs_market is dropped: a row column over the context stage is placed in the last keyed stage and
        // makes it depend on the context stage, i.e. a wave of its own)
        final String grouped = FEATURE_CONFIG
                .replace("        - name: vs_market\n          scope: row\n          type: residual\n          input: relative_start_price_shareOfTotal\n          baseline: market\n", "")
                .replace("            - {type: aggregate, field: start_price, funcs: [count, mean]}\n",
                        "            - {type: aggregate, field: start_price, funcs: [count, mean]}\n            - {type: aggregate, field: won, funcs: [mean]}\n")
                .replace("      output:\n", CAT_BLOCK + "      output:\n")
                .replace("prefix: f_", "prefix: f_\n        groupBy: session\n        parentFields: [session_time]");
        final Config config = Config.load(SOURCE_CONFIG + grouped);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);
        final MCollection output = outputs.get("features");
        Assertions.assertNotNull(output.getSchema().getField("rows"));
        final Set<String> names = transformNames();
        Assertions.assertTrue(hasTransform(names, "features", "Wave1_FanIn"), names::toString);
        Assertions.assertFalse(hasTransform(names, "features", "Wave1_Merge"), names::toString);
        PAssert.that(output.getCollection()).satisfies(rows -> {
            final Map<String, MElement> bySession = new HashMap<>();
            for (final MElement row : rows) bySession.put(row.getAsString("session_id"), row);
            Assertions.assertEquals(4, bySession.size());
            final List<?> c = (List<?>) bySession.get("C").getPrimitiveValue("rows");
            Assertions.assertEquals(2, c.size());
            for (final Object child : c) {
                final Map<?, ?> m = child instanceof MElement e ? e.asPrimitiveMap() : (Map<?, ?>) child;
                if ("s1".equals(String.valueOf(m.get("seller_id")))) {
                    // seller branch (strictly past): C/s1 sees A/s1 and B/s1; category branch: electronics count = 2
                    Assertions.assertEquals(200.0, ((Number) m.get("f_recent_n5_start_price_lag1")).doubleValue(), 1e-9);
                    Assertions.assertEquals(2L, ((Number) m.get("f_recent_n5_start_price_count")).longValue());
                    Assertions.assertEquals(2L, ((Number) m.get("f_cat__category__count")).longValue());
                    // the shared row expression is recomputed on the category branch: electronics won mean over A/s1 (1), B/s1 (0)
                    Assertions.assertEquals(0.5, ((Number) m.get("f_cat__category__won__mean")).doubleValue(), 1e-9);
                } else {
                    Assertions.assertEquals(50.0, ((Number) m.get("f_recent_n5_start_price_lag1")).doubleValue(), 1e-9);
                    Assertions.assertEquals(1L, ((Number) m.get("f_cat__category__count")).longValue());
                }
            }
            return null;
        });
        pipeline.run();
    }

    // ------------------------------------------------------------------------------------------
    // output contract (roles / include / manifest) and the observedAt audit
    // ------------------------------------------------------------------------------------------

    /**
     * The source with the observation-time column of {@code current_bid_t10} (declared availability event_time - 10 min,
     * predictAt event_time - 8 min): A/s1 on time, A/s2 after predictAt, B exactly at the deadline, C/s1 late but
     * before predictAt, C/s2 without observation time, D on time.
     */
    private static final String AUDIT_SOURCE_CONFIG = SOURCE_CONFIG
            .replace("final_price: 150.0, session_time: \"2025-01-01T10:00:00Z\"}", "final_price: 150.0, session_time: \"2025-01-01T10:00:00Z\", snapshot_time: \"2025-01-01T09:49:00Z\"}")
            .replace("final_price: 0.0,   session_time: \"2025-01-01T10:00:00Z\"}", "final_price: 0.0,   session_time: \"2025-01-01T10:00:00Z\", snapshot_time: \"2025-01-01T09:55:00Z\"}")
            .replace("final_price: 0.0,   session_time: \"2025-01-03T10:00:00Z\"}", "final_price: 0.0,   session_time: \"2025-01-03T10:00:00Z\", snapshot_time: \"2025-01-03T09:50:00Z\"}")
            .replace("final_price: 95.0,  session_time: \"2025-01-20T10:00:00Z\"}", "final_price: 95.0,  session_time: \"2025-01-20T10:00:00Z\", snapshot_time: \"2025-01-20T09:51:00Z\"}")
            .replace("final_price: 140.0, session_time: \"2025-02-01T10:00:00Z\"}", "final_price: 140.0, session_time: \"2025-02-01T10:00:00Z\", snapshot_time: \"2025-02-01T09:45:00Z\"}")
            .replace("        - {name: session_time, type: timestamp}\n", "        - {name: session_time, type: timestamp}\n        - {name: snapshot_time, type: timestamp}\n");

    private static String contractConfig(final String manifest) {
        return FEATURE_CONFIG.replace("      output:\n        prefix: f_\n",
                "      output:\n        prefix: f_\n        passThrough: keys\n"
                        + "        roles: {group: session, time: session_time, entity: seller_id, label: sold}\n"
                        + "        include: [f_price_per_unit, f_relative_start_price_rank, enc__seller_id__count, f_nope]\n"
                        + "        manifest: " + manifest + "\n");
    }

    @Test
    public void testManifestIncludeAndObservedAtAudit() throws java.io.IOException {
        Assertions.assertEquals(5, AUDIT_SOURCE_CONFIG.split("snapshot_time: \"").length - 1);
        final String dir = "target/feature-manifests/" + java.util.UUID.randomUUID(); // relative: Beam FileSystems treats a Windows drive letter as a scheme
        final String manifest = dir + "/manifest.json";
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(AUDIT_SOURCE_CONFIG + contractConfig(manifest)));
        final MCollection output = outputs.get("features");
        final Schema schema = output.getSchema();
        // include projects the features; passThrough keys + the role fields (sold is a label, not a key) pass through
        Assertions.assertEquals(Set.of("session_id", "seller_id", "sold", "session_time", "f_price_per_unit", "f_relative_start_price_rank", "f_enc__seller_id__count"),
                new HashSet<>(schema.getFields().stream().map(Schema.Field::getName).toList()));
        Assertions.assertNull(schema.getField("snapshot_time"));
        Assertions.assertNull(schema.getField("f_vs_market"));
        // pass-through fields carry their source contract as lineage options (the schema twin of the manifest's
        // fields entries): a consumer's derivedFrom: / scope: selectors see them, and roles are named
        Assertions.assertEquals("input", schema.getField("sold").getOptions().get("feature.scope"));
        Assertions.assertEquals("outcome", schema.getField("sold").getOptions().get("feature.kind"));
        Assertions.assertEquals("outcome", schema.getField("sold").getOptions().get("feature.derivedFrom"));
        Assertions.assertEquals("auction_results", schema.getField("sold").getOptions().get("feature.sources"));
        Assertions.assertEquals("label", schema.getField("sold").getOptions().get("feature.role"));
        Assertions.assertEquals("time", schema.getField("session_time").getOptions().get("feature.role"));
        Assertions.assertEquals("entity", schema.getField("seller_id").getOptions().get("feature.role"));
        Assertions.assertNull(schema.getField("f_price_per_unit").getOptions().get("feature.role"));
        Assertions.assertEquals("row", schema.getField("f_price_per_unit").getOptions().get("feature.scope"));

        // the assembly-time manifest exists before the run (a dry run writes the same file)
        final com.google.gson.JsonObject assembled = com.google.gson.JsonParser.parseString(
                java.nio.file.Files.readString(java.nio.file.Path.of(manifest))).getAsJsonObject();
        Assertions.assertEquals(List.of("f_price_per_unit", "f_relative_start_price_rank", "f_enc__seller_id__count"),
                assembled.getAsJsonArray("columns").asList().stream().map(e -> e.getAsJsonObject().get("name").getAsString()).toList());
        Assertions.assertEquals("session", assembled.getAsJsonObject("roles").getAsJsonObject("group").get("name").getAsString());
        Assertions.assertEquals("sold", assembled.getAsJsonObject("roles").getAsJsonObject("label").get("column").getAsString());
        Assertions.assertEquals(16, assembled.get("outputHash").getAsString().length());
        Assertions.assertTrue(assembled.getAsJsonObject("include").getAsJsonArray("unknown").toString().contains("f_nope"));
        final List<String> fieldNames = assembled.getAsJsonArray("fields").asList().stream().map(e -> e.getAsJsonObject().get("name").getAsString()).toList();
        Assertions.assertEquals(List.of("session_id", "seller_id", "sold", "session_time"), fieldNames);
        final com.google.gson.JsonObject soldField = assembled.getAsJsonArray("fields").get(2).getAsJsonObject();
        Assertions.assertEquals("input", soldField.get("scope").getAsString());
        Assertions.assertEquals("outcome", soldField.get("kind").getAsString());
        Assertions.assertEquals("label", soldField.get("role").getAsString());
        Assertions.assertEquals(1, assembled.getAsJsonObject("plan").getAsJsonArray("observedAtAudit").size());

        PAssert.that(output.getCollection()).satisfies(rows -> {
            int n = 0;
            for (final MElement row : rows) {
                n++;
                Assertions.assertNotNull(row.getPrimitiveValue("sold"));
            }
            Assertions.assertEquals(6, n);
            return null;
        });
        pipeline.run();

        // the run manifest: row count and the observedAt audit of current_bid_t10
        final com.google.gson.JsonObject run = com.google.gson.JsonParser.parseString(
                java.nio.file.Files.readString(java.nio.file.Path.of(dir + "/manifest.run.json"))).getAsJsonObject();
        Assertions.assertEquals(assembled.get("planHash").getAsString(), run.get("planHash").getAsString());
        Assertions.assertEquals(assembled.get("outputHash").getAsString(), run.get("outputHash").getAsString());
        Assertions.assertEquals(6, run.get("rows").getAsLong());
        final com.google.gson.JsonObject audit = run.getAsJsonObject("observedAtAudit").getAsJsonObject("current_bid_t10");
        Assertions.assertEquals(6, audit.get("rows").getAsLong());
        Assertions.assertEquals(1, audit.get("missing").getAsLong());          // C/s2
        Assertions.assertEquals(2, audit.get("late").getAsLong());             // A/s2 (09:55), C/s1 (09:51) after the 09:50 deadline
        Assertions.assertEquals(1, audit.get("afterPredictAt").getAsLong());   // A/s2 after 09:52
        Assertions.assertEquals(5, audit.get("measured").getAsLong());
        final com.google.gson.JsonArray deciles = audit.getAsJsonArray("leadSecondsDeciles");
        Assertions.assertEquals(11, deciles.size());
        Assertions.assertEquals(-180.0, deciles.get(0).getAsDouble(), 1e-9);   // A/s2: predictAt 09:52 - observed 09:55
        Assertions.assertEquals(420.0, deciles.get(10).getAsDouble(), 1e-9);   // D: 09:52 - 09:45
    }

    @Test
    public void testObservedAtAuditFailRoutesLateRows() throws java.io.IOException {
        final String config = FEATURE_CONFIG
                .replace("    inputs: [create]\n", "    inputs: [create]\n    failFast: false\n")
                .replace("      output:\n        prefix: f_\n", "      audit: {observedAt: fail}\n      output:\n        prefix: f_\n");
        Assertions.assertTrue(config.contains("failFast: false"));
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(AUDIT_SOURCE_CONFIG + config));
        PAssert.that(outputs.get("features").getCollection()).satisfies(rows -> {
            final Set<String> keys = new HashSet<>();
            for (final MElement row : rows) keys.add(row.getAsString("session_id") + "/" + row.getAsString("seller_id"));
            // the two rows observed after their declared availability went to the failure output
            Assertions.assertEquals(Set.of("A/s1", "B/s1", "C/s2", "D/s1"), keys);
            return null;
        });
        pipeline.run();
    }

    // ------------------------------------------------------------------------------------------
    // softmax / baseline emit / placebo ops
    // ------------------------------------------------------------------------------------------

    private static final String PROB_BLOCKS = """
        - name: score
          scope: row
          expr: "0"
        - name: prob
          scope: context
          context: session
          ops:
            - {type: softmax, field: score, offset: market, temperature: 1, as: pWin}
        - name: placeboNoise
          scope: row
          type: noise
          distribution: normal
          seed: 20260717
        - name: placebo
          scope: context
          context: session
          ops:
            - {type: shuffle, fields: [start_price], seed: 20260717}
""";

    private static final String PROB_CONFIG = FEATURE_CONFIG
            .replace("- {name: market, context: session, expr: \"share(1 / current_bid_t10)\"}",
                    "- {name: market, context: session, expr: \"share(1 / current_bid_t10)\", emit: marketProb}")
            .replace("      output:\n", PROB_BLOCKS + "      output:\n");

    @Test
    public void testIncludeKeepsEmittedBaselineRole() throws java.io.IOException {
        // the closed loop: a screening pass list projects the features, and the baseline role's emitted copy
        // (never a candidate, so never in the list) must still reach the consumer with its role
        final String config = PROB_CONFIG.replace("      output:\n        prefix: f_\n",
                "      output:\n        prefix: f_\n        roles: {group: session, label: sold, baseline: market}\n        include: [f_prob_pWin_softmax]\n");
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(SOURCE_CONFIG + config));
        final Schema schema = outputs.get("features").getSchema();
        Assertions.assertNotNull(schema.getField("f_prob_pWin_softmax"));
        Assertions.assertNotNull(schema.getField("f_marketProb"));
        Assertions.assertEquals("baseline", schema.getField("f_marketProb").getOptions().get("feature.role"));
        Assertions.assertEquals("label", schema.getField("sold").getOptions().get("feature.role"));
        Assertions.assertNull(schema.getField("f_placeboNoise"));
        Assertions.assertNull(schema.getField("f_placebo_start_price_shuffle"));
        PAssert.that(outputs.get("features").getCollection()).satisfies(rows -> {
            int n = 0;
            for (final MElement row : rows) {
                n++;
                Assertions.assertEquals(row.getAsDouble("f_marketProb"), row.getAsDouble("f_prob_pWin_softmax"), 1e-12);
            }
            Assertions.assertEquals(6, n);
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testSoftmaxEmitAndPlacebos() throws java.io.IOException {
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(SOURCE_CONFIG + PROB_CONFIG));
        final Schema schema = outputs.get("features").getSchema();
        Assertions.assertNotNull(schema.getField("f_marketProb"));
        Assertions.assertEquals("market", schema.getField("f_prob_pWin_softmax").getOptions().get("feature.derivedFrom"));
        Assertions.assertEquals(Schema.Type.float64, schema.getField("f_placebo_start_price_shuffle").getFieldType().getType());
        PAssert.that(outputs.get("features").getCollection()).satisfies(rows -> {
            final Map<String, MElement> byKey = new HashMap<>();
            for (final MElement row : rows) byKey.put(row.getAsString("session_id") + "/" + row.getAsString("seller_id"), row);
            Assertions.assertEquals(6, byKey.size());
            // f = 0, T = 1: the softmax probability equals the emitted market baseline (share of 1 / bid within the session)
            for (final MElement row : byKey.values()) {
                Assertions.assertEquals(row.getAsDouble("f_marketProb"), row.getAsDouble("f_prob_pWin_softmax"), 1e-12);
                Assertions.assertTrue(Math.abs(row.getAsDouble("f_placeboNoise")) < 6);
            }
            final double marketA1 = (1 / 120.0) / (1 / 120.0 + 1 / 55.0);
            Assertions.assertEquals(marketA1, byKey.get("A/s1").getAsDouble("f_prob_pWin_softmax"), 1e-9);
            Assertions.assertEquals(1.0, byKey.get("B/s1").getAsDouble("f_prob_pWin_softmax"), 1e-9);
            // shuffle keeps the multiset of start_price per session; noise differs per row
            Assertions.assertEquals(Set.of(100.0, 50.0), Set.of(byKey.get("A/s1").getAsDouble("f_placebo_start_price_shuffle"), byKey.get("A/s2").getAsDouble("f_placebo_start_price_shuffle")));
            Assertions.assertEquals(200.0, byKey.get("B/s1").getAsDouble("f_placebo_start_price_shuffle"), 0d);
            // the draw is a function of (time.field, orderTieBreak): A/s1 and A/s2 share both here and get the same draw, B differs
            Assertions.assertEquals(byKey.get("A/s1").getAsDouble("f_placeboNoise"), byKey.get("A/s2").getAsDouble("f_placeboNoise"), 0d);
            Assertions.assertNotEquals(byKey.get("A/s1").getAsDouble("f_placeboNoise"), byKey.get("B/s1").getAsDouble("f_placeboNoise"));
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testPlacebosAreDeterministicAcrossEngineModes() throws java.io.IOException {
        // noise / shuffle are pure functions of the row identity / group: the parallel waves and the linear chain agree
        assertParallelMatchesLinear(PROB_CONFIG, 6, List.of(), List.of());
    }

    // ------------------------------------------------------------------------------------------
    // fit.mode forward
    // ------------------------------------------------------------------------------------------

    private static String forwardConfig(final String dir, final String extra) {
        return FEATURE_CONFIG.replace("      output:\n",
                "      fit: {mode: forward, blocks: {size: P7D}" + extra + ", artifact: {uri: \"" + dir + "\"}}\n      output:\n");
    }

    /**
     * Weekly blocks from the epoch: Jan 1 (A) = block 2869, Jan 3 (B) = 2870, Jan 20 (C) = 2872, Feb 1 (D) = 2874. The
     * row-count level has no lag; the sold level lags 6 days 30 minutes (settlement + ingestion). Every block boundary
     * falls between sessions here, so forward reproduces the expanding values of {@link #testFeatureTransform}.
     */
    @Test
    public void testForwardFit() throws java.io.IOException {
        final String dir = "target/feature-artifacts/" + java.util.UUID.randomUUID();
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(SOURCE_CONFIG + forwardConfig(dir, "")));
        Assertions.assertEquals("true", outputs.get("features").getSchema().getField("f_enc__seller_id__count").getOptions().get("feature.fit"));
        PAssert.that(outputs.get("features").getCollection()).satisfies(rows -> {
            final Map<String, MElement> byKey = new HashMap<>();
            for (final MElement row : rows) byKey.put(row.getAsString("session_id") + "/" + row.getAsString("seller_id"), row);
            Assertions.assertEquals(6, byKey.size());
            // A: nothing before its block
            Assertions.assertEquals(0L, ((Number) byKey.get("A/s1").getPrimitiveValue("f_enc__seller_id__count")).longValue());
            Assertions.assertNull(byKey.get("A/s1").getPrimitiveValue("f_enc__seller_id__e2__mean"));
            // B (Jan 3): the row count sees A's block; the outcome of A (known Jan 7) does not fit a complete known block yet
            Assertions.assertEquals(1L, ((Number) byKey.get("B/s1").getPrimitiveValue("f_enc__seller_id__count")).longValue());
            Assertions.assertNull(byKey.get("B/s1").getPrimitiveValue("f_enc__seller_id__e2__mean"));
            // C (Jan 20): blocks 2869 + 2870 are complete and their outcomes known -> A, B
            Assertions.assertEquals(2L, ((Number) byKey.get("C/s1").getPrimitiveValue("f_enc__seller_id__count")).longValue());
            Assertions.assertEquals(0.5, byKey.get("C/s1").getAsDouble("f_enc__seller_id__e2__mean"), 1e-9);
            Assertions.assertEquals(1L, ((Number) byKey.get("C/s2").getPrimitiveValue("f_enc__seller_id__count")).longValue());
            Assertions.assertEquals(0.0, byKey.get("C/s2").getAsDouble("f_enc__seller_id__e2__mean"), 1e-9);
            // D (Feb 1): up to block 2872 -> A, B, C
            Assertions.assertEquals(3L, ((Number) byKey.get("D/s1").getPrimitiveValue("f_enc__seller_id__count")).longValue());
            Assertions.assertEquals(2.0 / 3.0, byKey.get("D/s1").getAsDouble("f_enc__seller_id__e2__mean"), 1e-9);
            return null;
        });
        pipeline.run();
        // the artifact holds the whole-input totals (a static serving run can load them) and the manifest the λ per block
        final java.io.File[] files = new java.io.File(dir).listFiles();
        Assertions.assertNotNull(files, "artifact directory missing: " + dir);
        Assertions.assertTrue(new java.io.File(files[0], "enc.avro").exists());
        final String manifest = java.nio.file.Files.readString(new java.io.File(files[0], "enc.manifest.json").toPath());
        Assertions.assertTrue(manifest.contains("lambdasByBlock"), manifest);
    }

    @Test
    public void testForwardFitMinBlocks() throws java.io.IOException {
        final String dir = "target/feature-artifacts/" + java.util.UUID.randomUUID();
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(SOURCE_CONFIG + forwardConfig(dir, ", minBlocks: 2")));
        PAssert.that(outputs.get("features").getCollection()).satisfies(rows -> {
            final Map<String, MElement> byKey = new HashMap<>();
            for (final MElement row : rows) byKey.put(row.getAsString("session_id") + "/" + row.getAsString("seller_id"), row);
            // B sees one preceding block with data for s1 (< 2): nothing; C sees two (2869, 2870); D three
            Assertions.assertEquals(0L, ((Number) byKey.get("B/s1").getPrimitiveValue("f_enc__seller_id__count")).longValue());
            Assertions.assertEquals(2L, ((Number) byKey.get("C/s1").getPrimitiveValue("f_enc__seller_id__count")).longValue());
            Assertions.assertEquals(3L, ((Number) byKey.get("D/s1").getPrimitiveValue("f_enc__seller_id__count")).longValue());
            // s2 has a single preceding block at C and D: below minBlocks
            Assertions.assertEquals(0L, ((Number) byKey.get("C/s2").getPrimitiveValue("f_enc__seller_id__count")).longValue());
            Assertions.assertNull(byKey.get("C/s2").getPrimitiveValue("f_enc__seller_id__e2__mean"));
            return null;
        });
        pipeline.run();
    }

}
