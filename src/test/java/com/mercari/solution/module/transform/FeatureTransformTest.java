package com.mercari.solution.module.transform;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

}
