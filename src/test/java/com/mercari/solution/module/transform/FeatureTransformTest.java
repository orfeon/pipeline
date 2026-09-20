package com.mercari.solution.module.transform;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.feature.Rating;
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

    /**
     * History is strictly past by timestamp, not by day: a row later the same day sees the morning's rows of its
     * entity, while rows that share a timestamp never see each other. Session B is moved to 15:00 on Jan 1 and a
     * second s1 session (E) is added at A's exact time — B counts both, A and E count nothing. A date-granularity
     * time field would make every same-day row invisible (they would all share the timestamp), so a "same-day
     * earlier events" feature needs the event's actual time in {@code time.field}.
     */
    @Test
    public void testSameDayEarlierRowsAreStrictlyPast() throws java.io.IOException {
        final String source = SOURCE_CONFIG
                .replace("sold: 0, final_price: 0.0,   session_time: \"2025-01-03T10:00:00Z\"}", "sold: 0, final_price: 0.0,   session_time: \"2025-01-01T15:00:00Z\"}")
                .replace("        - {session_id: C, seller_id: s1,",
                        "        - {session_id: E, seller_id: s1, category: electronics, quantity: 1, start_price: 90.0,  condition_grade: good, current_bid_t10: 95.0,  sold: 0, final_price: 0.0,   session_time: \"2025-01-01T10:00:00Z\"}\n        - {session_id: C, seller_id: s1,");
        Assertions.assertTrue(source.contains("2025-01-01T15:00:00Z") && source.contains("session_id: E"), source);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(source + FEATURE_CONFIG));
        PAssert.that(outputs.get("features").getCollection()).satisfies(rows -> {
            final Map<String, MElement> byKey = new HashMap<>();
            for (final MElement row : rows) byKey.put(row.getAsString("session_id") + "/" + row.getAsString("seller_id"), row);
            Assertions.assertEquals(7, byKey.size());
            // 10:00: A/s1 and E/s1 share the timestamp — neither is in the other's past
            for (final String same : List.of("A/s1", "E/s1")) {
                Assertions.assertEquals(0L, ((Number) byKey.get(same).getPrimitiveValue("f_recent_n5_start_price_count")).longValue(), same);
                Assertions.assertNull(byKey.get(same).getPrimitiveValue("f_recent_n5_start_price_lag1"), same);
                Assertions.assertEquals(0L, ((Number) byKey.get(same).getPrimitiveValue("f_enc__seller_id__count")).longValue(), same);
            }
            // 15:00 the same day: both morning sessions of s1 are strictly past (pre-event attributes need no shift)
            Assertions.assertEquals(2L, ((Number) byKey.get("B/s1").getPrimitiveValue("f_recent_n5_start_price_count")).longValue());
            Assertions.assertEquals(95.0, byKey.get("B/s1").getAsDouble("f_recent_n5_start_price_mean"), 1e-9);
            Assertions.assertTrue(List.of(100.0, 90.0).contains(byKey.get("B/s1").getAsDouble("f_recent_n5_start_price_lag1")));
            Assertions.assertEquals(2L, ((Number) byKey.get("B/s1").getPrimitiveValue("f_enc__seller_id__count")).longValue());
            // the outcome of the morning sessions is still unknown at 15:00 (settlement + ingestion lag)
            Assertions.assertNull(byKey.get("B/s1").getPrimitiveValue("f_recent_n5_sold_lag1"));
            Assertions.assertNull(byKey.get("B/s1").getPrimitiveValue("f_enc__seller_id__e2__mean"));
            return null;
        });
        pipeline.run();
    }

    /**
     * {@code weightBy}: the seller's past sessions weighed by how close their start price was to this listing's,
     * w = exp(−|Δprice| / 50). {@code sold} reaches the system 6 days after a session, so B/s1 (two days after A) sees
     * nothing yet; C/s1 (price 80) sees A (100, sold) and B (200, unsold); D/s1 (120) sees A, B and C (80, sold).
     */
    @Test
    public void testSequenceWeightBy() throws java.io.IOException {
        final String blocks = """
                    - name: similar
                      scope: sequence
                      entity: seller
                      ops:
                        - {type: aggregate, field: sold, funcs: [count, mean], weightBy: "exp(-abs(start_price - $self.start_price) / 50)"}
                        - {type: aggregate, field: sold, funcs: [count, mean], as: plain}
                """;
        final String config = FEATURE_CONFIG.replace("      output:\n", blocks.replaceAll("(?m)^", "    ") + "      output:\n");
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(SOURCE_CONFIG + config));
        final MCollection output = outputs.get("features");
        Assertions.assertEquals(Schema.Type.float64, output.getSchema().getField("f_similar_all_sold_count").getFieldType().getType());
        Assertions.assertEquals(Schema.Type.int64, output.getSchema().getField("f_similar_all_plain_count").getFieldType().getType());
        Assertions.assertEquals("windowShift", output.getSchema().getField("f_similar_all_sold_mean").getOptions().get("feature.status"));
        PAssert.that(output.getCollection()).satisfies(rows -> {
            int count = 0;
            for (final MElement row : rows) {
                count++;
                final String id = row.getAsString("session_id") + "/" + row.getAsString("seller_id");
                final double a, b, c;
                switch (id) {
                    case "A/s1", "A/s2", "B/s1" -> {
                        Assertions.assertEquals(0L, row.getAsLong("f_similar_all_plain_count"), id);
                        Assertions.assertEquals(0.0, row.getAsDouble("f_similar_all_sold_count"), 1e-12, id);
                        Assertions.assertNull(row.getPrimitiveValue("f_similar_all_sold_mean"), id);
                    }
                    case "C/s1" -> {
                        a = Math.exp(-20 / 50d);
                        b = Math.exp(-120 / 50d);
                        Assertions.assertEquals(2L, row.getAsLong("f_similar_all_plain_count"));
                        Assertions.assertEquals(0.5, row.getAsDouble("f_similar_all_plain_mean"), 1e-12);
                        Assertions.assertEquals(a + b, row.getAsDouble("f_similar_all_sold_count"), 1e-12);
                        Assertions.assertEquals(a / (a + b), row.getAsDouble("f_similar_all_sold_mean"), 1e-12, "the similar listing (sold) dominates the distant one");
                    }
                    case "C/s2" -> {
                        Assertions.assertEquals(Math.exp(-10 / 50d), row.getAsDouble("f_similar_all_sold_count"), 1e-12);
                        Assertions.assertEquals(0.0, row.getAsDouble("f_similar_all_sold_mean"), 1e-12);
                    }
                    case "D/s1" -> {
                        a = Math.exp(-20 / 50d);
                        b = Math.exp(-80 / 50d);
                        c = Math.exp(-40 / 50d);
                        Assertions.assertEquals(3L, row.getAsLong("f_similar_all_plain_count"));
                        Assertions.assertEquals(a + b + c, row.getAsDouble("f_similar_all_sold_count"), 1e-12);
                        Assertions.assertEquals((a + c) / (a + b + c), row.getAsDouble("f_similar_all_sold_mean"), 1e-12);
                        Assertions.assertTrue(row.getAsDouble("f_similar_all_sold_mean") > row.getAsDouble("f_similar_all_plain_mean"));
                    }
                    default -> Assertions.fail("unexpected row " + id);
                }
            }
            Assertions.assertEquals(6, count);
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

    private static final String PATH_BLOCKS = """
                - name: grade
                  scope: sequence
                  entity: seller
                  ops:
                    - {type: lag, field: condition_grade, k: 2}
                - name: path
                  scope: population
                  type: encoding
                  keySets:
                    - keys: [grade_all_condition_grade_lag1, grade_all_condition_grade_lag2]
                      structure: sequence
                  targets:
                    - {field: sold, stats: [mean]}
                  shrinkage: {priorWeight: 1}
            """.replaceAll("(?m)^", "    ");

    /**
     * {@code structure: sequence}: the sale rate given the last two condition grades of the seller (most recent first),
     * backing off to the last grade alone and then to the global rate. The grades of s1 run good, good, fair, good;
     * those of s2 fair, good. No row has seen its own two-step path before, so every value is its longest known suffix —
     * and the derived lattice equals the explicit hierarchy of the shortened key list, row for row.
     */
    @Test
    public void testSequenceStructure() throws java.io.IOException {
        final String derived = FEATURE_CONFIG.replace("      output:\n", PATH_BLOCKS + "      output:\n");
        final String declared = derived.replace("structure: sequence", "hierarchy: [[grade_all_condition_grade_lag1], []]")
                .replace("name: features", "name: declared").replace("transforms:\n", "");
        Assertions.assertNotEquals(derived, declared);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(SOURCE_CONFIG + derived + declared));
        final String column = "f_path__grade_all_condition_grade_lag1_grade_all_condition_grade_lag2__sold__mean";
        Assertions.assertNotNull(outputs.get("features").getSchema().getField(column), () -> outputs.get("features").getSchema().getFields().toString());
        final PCollection<KV<String, String>> a = outputs.get("features").getCollection().apply("TagDerived", ParDo.of(new TagDoFn("derived")));
        final PCollection<KV<String, String>> b = outputs.get("declared").getCollection().apply("TagDeclared", ParDo.of(new TagDoFn("declared")));
        PAssert.that(PCollectionList.of(a).and(b).apply(Flatten.pCollections())).satisfies(kvs -> {
            final Set<String> first = new HashSet<>(), second = new HashSet<>();
            for (final KV<String, String> kv : kvs) (kv.getKey().equals("derived") ? first : second).add(kv.getValue());
            Assertions.assertEquals(6, first.size());
            Assertions.assertEquals(second, first);
            return null;
        });
        PAssert.that(outputs.get("features").getCollection()).satisfies(rows -> {
            final Map<String, MElement> byKey = new HashMap<>();
            for (final MElement row : rows) byKey.put(row.getAsString("session_id") + "/" + row.getAsString("seller_id"), row);
            // Jan 3: no outcome has reached the system yet
            Assertions.assertNull(byKey.get("B/s1").getPrimitiveValue(column));
            // Jan 20, s1 (good, good): the path is new; after "good" came B/s1 (not sold): 1/3 + 1/2 · (0 − 1/3) = 1/6
            Assertions.assertEquals(1.0 / 6.0, byKey.get("C/s1").getAsDouble(column), 1e-9);
            // Jan 20, s2 (fair, –): no second step and nothing seen after "fair" yet: the global rate 1/3
            Assertions.assertEquals(1.0 / 3.0, byKey.get("C/s2").getAsDouble(column), 1e-9);
            // Feb 1, s1 (fair, good): new path; after "fair" came C/s2 (sold): 3/5 + 1/2 · (1 − 3/5) = 0.8
            Assertions.assertEquals(0.8, byKey.get("D/s1").getAsDouble(column), 1e-9);
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

    /**
     * svd {@code outputs}: with one component of (start_price, current_bid_t10) kept, the residual is what the shared
     * price axis does not explain, per input and in input units — it sums to zero over the rows, its norm column is its
     * length, and it is orthogonal to the score; with both components kept nothing is left.
     */
    @Test
    public void testSvdResidual() throws java.io.IOException {
        final String blocks = """
                    - name: price_pc
                      scope: population
                      type: svd
                      inputs: [start_price, current_bid_t10]
                      rank: 1
                      outputs: [scores, residual, residualNorm]
                    - name: price_full
                      scope: population
                      type: svd
                      inputs: [start_price, current_bid_t10]
                      rank: 2
                      outputs: [residualNorm]
                """;
        final String config = FEATURE_CONFIG.replace("      output:\n", blocks.replaceAll("(?m)^", "    ") + "      output:\n");
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(SOURCE_CONFIG + config));
        final MCollection output = outputs.get("features");
        Assertions.assertNotNull(output.getSchema().getField("f_price_pc_0"));
        Assertions.assertNull(output.getSchema().getField("f_price_pc_1"));
        Assertions.assertNull(output.getSchema().getField("f_price_full_0"), "scores are not listed");
        Assertions.assertTrue(output.getSchema().getField("f_price_pc_resid_current_bid_t10").getOptions().get("feature.derivedFrom").contains("market"));
        PAssert.that(output.getCollection()).satisfies(rows -> {
            int count = 0;
            double sumPrice = 0, sumBid = 0, sumSq = 0, cross = 0;
            for (final MElement row : rows) {
                count++;
                final double rp = row.getAsDouble("f_price_pc_resid_start_price"), rb = row.getAsDouble("f_price_pc_resid_current_bid_t10");
                Assertions.assertEquals(Math.sqrt(rp * rp + rb * rb), row.getAsDouble("f_price_pc_residnorm"), 1e-9);
                Assertions.assertEquals(0.0, row.getAsDouble("f_price_full_residnorm"), 1e-6, "both components kept: nothing is left");
                sumPrice += rp;
                sumBid += rb;
                sumSq += rp * rp + rb * rb;
                // the residual is the dropped component's part, and the two components' scores are uncorrelated
                cross += row.getAsDouble("f_price_pc_0") * (rp + rb);
            }
            Assertions.assertEquals(6, count);
            Assertions.assertEquals(0.0, sumPrice, 1e-9);
            Assertions.assertEquals(0.0, sumBid, 1e-9);
            Assertions.assertEquals(0.0, cross, 1e-6);
            Assertions.assertTrue(sumSq > 1, "the bid does not track the price exactly: " + sumSq);
            return null;
        });
        pipeline.run();
    }

    /**
     * The general form end to end: a Legendre summary on the time clock (with its time channel), a Laguerre summary
     * on the events clock, and the {@code ewma} sugar reading the same state as the order-0 Laguerre component.
     */
    @Test
    public void testSequenceDynamics() throws java.io.IOException {
        final String blocks = """
                    - name: path
                      scope: sequence
                      entity: seller
                      lift: {fields: [start_price], timeAugment: true}
                      summarize:
                        dynamics: {family: lti, measure: legendre, order: 1, decayBy: time}
                    - name: laguerre
                      scope: sequence
                      entity: seller
                      lift: {fields: [start_price]}
                      summarize:
                        dynamics: {family: lti, measure: exponential, order: 1, halflife: [2]}
                    - name: smooth
                      scope: sequence
                      entity: seller
                      ops:
                        - {type: ewma, field: start_price, halflife: [2]}
                """;
        final String config = FEATURE_CONFIG.replace("      output:\n", blocks.replaceAll("(?m)^", "    ") + "      output:\n");
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(SOURCE_CONFIG + config));
        final MCollection output = outputs.get("features");
        Assertions.assertEquals("legendre", output.getSchema().getField("f_path_all_start_price_leg_1").getOptions().get("feature.coord.measure"));
        Assertions.assertEquals(Schema.Type.float64, output.getSchema().getField("f_laguerre_all_start_price_exp2_1").getFieldType().getType());
        PAssert.that(output.getCollection()).satisfies(rows -> {
            int count = 0;
            for (final MElement row : rows) {
                count++;
                final String id = row.getAsString("session_id") + "/" + row.getAsString("seller_id");
                switch (id) {
                    case "D/s1" -> {
                        // s1's past: 100 on day 0, 200 on day 2, 80 on day 19; the row is on day 31
                        final double[] x = {100, 200, 80}, day = {0, 2, 19};
                        double p1 = 0, t1 = 0;
                        for (int i = 0; i < 3; i++) {
                            p1 += x[i] * (2 * day[i] / 31 - 1);
                            t1 += 2 * day[i] / 31 - 1;
                        }
                        Assertions.assertEquals(380.0 / 3, row.getAsDouble("f_path_all_start_price_leg_0"), 1e-9);
                        Assertions.assertEquals(p1 / 3, row.getAsDouble("f_path_all_start_price_leg_1"), 1e-9);
                        Assertions.assertEquals(t1 / 3, row.getAsDouble("f_path_all_time_leg_1"), 1e-9);
                        // events clock: the newest event is 0 events old, the oldest 2 (halflife 2)
                        final double[] w = {0.5, Math.sqrt(0.5), 1}, age = {2, 1, 0};
                        double num = 0, den = 0, l1 = 0;
                        for (int i = 0; i < 3; i++) {
                            num += w[i] * x[i];
                            den += w[i];
                            l1 += w[i] * x[i] * (1 - Math.log(2) / 2 * age[i]);
                        }
                        Assertions.assertEquals(num / den, row.getAsDouble("f_smooth_all_start_price_ewma2"), 1e-9);
                        Assertions.assertEquals(num / den, row.getAsDouble("f_laguerre_all_start_price_exp2_0"), 1e-9);
                        Assertions.assertEquals(l1 / den, row.getAsDouble("f_laguerre_all_start_price_exp2_1"), 1e-9);
                    }
                    case "B/s1" -> {
                        // one past event on day 0, the row on day 2: u = 0, P1 = −1
                        Assertions.assertEquals(100.0, row.getAsDouble("f_path_all_start_price_leg_0"), 1e-9);
                        Assertions.assertEquals(-100.0, row.getAsDouble("f_path_all_start_price_leg_1"), 1e-9);
                        Assertions.assertEquals(100.0, row.getAsDouble("f_smooth_all_start_price_ewma2"), 1e-9);
                        Assertions.assertEquals(100.0, row.getAsDouble("f_laguerre_all_start_price_exp2_1"), 1e-9);
                    }
                    case "C/s1" -> Assertions.assertEquals(row.getAsDouble("f_smooth_all_start_price_ewma2"),
                            row.getAsDouble("f_laguerre_all_start_price_exp2_0"), 1e-9);
                    case "A/s1", "A/s2" -> {
                        Assertions.assertNull(row.getPrimitiveValue("f_path_all_start_price_leg_0"), id + ": no past event");
                        Assertions.assertNull(row.getPrimitiveValue("f_laguerre_all_start_price_exp2_1"), id + ": no past event");
                        Assertions.assertNull(row.getPrimitiveValue("f_smooth_all_start_price_ewma2"), id + ": no past event");
                    }
                    case "C/s2" -> Assertions.assertEquals(50.0, row.getAsDouble("f_laguerre_all_start_price_exp2_0"), 1e-9);
                    default -> Assertions.fail("unexpected row " + id);
                }
            }
            Assertions.assertEquals(6, count);
            return null;
        });
        pipeline.run();
    }

    /**
     * The two-series {@code regression} and {@code fracdiff} in the keyed stage. Seller s1's sessions A, B, C are
     * visible to D (their outcomes arrived): final_price (150, 0, 95) against start_price (100, 200, 80); the start
     * prices themselves are known at once, so D's first difference reads C − B = 80 − 200.
     */
    @Test
    public void testSequenceRegressionAndFracdiff() throws java.io.IOException {
        final String blocks = """
                    - name: pair
                      scope: sequence
                      entity: seller
                      ops:
                        - {type: regression, field: final_price, against: start_price, funcs: [beta, corr]}
                        - {type: fracdiff, field: start_price, d: 1, k: 2}
                """;
        final String config = FEATURE_CONFIG.replace("      output:\n", blocks.replaceAll("(?m)^", "    ") + "      output:\n");
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(SOURCE_CONFIG + config));
        final MCollection output = outputs.get("features");
        Assertions.assertEquals("windowShift", output.getSchema().getField("f_pair_all_final_price_vs_start_price_beta").getOptions().get("feature.status"));
        PAssert.that(output.getCollection()).satisfies(rows -> {
            int count = 0;
            for (final MElement row : rows) {
                count++;
                final String id = row.getAsString("session_id") + "/" + row.getAsString("seller_id");
                switch (id) {
                    case "D/s1" -> {
                        // x = (100, 200, 80), y = (150, 0, 95): the least-squares line of y on x, computed directly
                        final double[] x = {100, 200, 80}, y = {150, 0, 95};
                        final double mx = (x[0] + x[1] + x[2]) / 3, my = (y[0] + y[1] + y[2]) / 3;
                        double sxy = 0, sxx = 0, syy = 0;
                        for (int i = 0; i < 3; i++) {
                            sxy += (x[i] - mx) * (y[i] - my);
                            sxx += (x[i] - mx) * (x[i] - mx);
                            syy += (y[i] - my) * (y[i] - my);
                        }
                        Assertions.assertEquals(sxy / sxx, row.getAsDouble("f_pair_all_final_price_vs_start_price_beta"), 1e-9);
                        Assertions.assertEquals(sxy / Math.sqrt(sxx * syy), row.getAsDouble("f_pair_all_final_price_vs_start_price_corr"), 1e-9);
                        Assertions.assertTrue(row.getAsDouble("f_pair_all_final_price_vs_start_price_beta") < 0, "the expensive listing did not sell");
                        Assertions.assertEquals(80.0 - 200.0, row.getAsDouble("f_pair_all_start_price_fracdiff1"), 1e-9);
                    }
                    case "C/s1" -> {
                        // two pairs: a line through (100, 150) and (200, 0)
                        Assertions.assertEquals(-1.5, row.getAsDouble("f_pair_all_final_price_vs_start_price_beta"), 1e-9);
                        Assertions.assertEquals(-1.0, row.getAsDouble("f_pair_all_final_price_vs_start_price_corr"), 1e-9);
                        Assertions.assertEquals(200.0 - 100.0, row.getAsDouble("f_pair_all_start_price_fracdiff1"), 1e-9);
                    }
                    case "A/s1", "A/s2", "B/s1", "C/s2" -> {
                        Assertions.assertNull(row.getPrimitiveValue("f_pair_all_final_price_vs_start_price_beta"), id + ": fewer than two pairs");
                        Assertions.assertNull(row.getPrimitiveValue("f_pair_all_start_price_fracdiff1"), id + ": fewer than k events");
                    }
                    default -> Assertions.fail("unexpected row " + id);
                }
            }
            Assertions.assertEquals(6, count);
            return null;
        });
        pipeline.run();
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

    /**
     * The scalar summaries on seller s1's start prices, which are known at once: D sees (100, 200, 80) — one peak,
     * a positive skew, one sign change of price − 100 (0, +100, −20: the zero has no sign) — and C sees (100, 200).
     */
    @Test
    public void testAggregateShapeAndSeriesFuncs() throws java.io.IOException {
        final String blocks = """
                    - name: shape
                      scope: sequence
                      entity: seller
                      ops:
                        - {type: aggregate, field: start_price, funcs: [skew, peaks, acf1]}
                        - {type: aggregate, expr: "start_price - 100", funcs: [zeroCross], as: vs100}
                """;
        final String config = FEATURE_CONFIG.replace("      output:\n", blocks.replaceAll("(?m)^", "    ") + "      output:\n");
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(SOURCE_CONFIG + config));
        final MCollection output = outputs.get("features");
        Assertions.assertEquals(Schema.Type.int64, output.getSchema().getField("f_shape_all_start_price_peaks").getFieldType().getType());
        Assertions.assertEquals(Schema.Type.float64, output.getSchema().getField("f_shape_all_start_price_skew").getFieldType().getType());
        PAssert.that(output.getCollection()).satisfies(rows -> {
            int count = 0;
            for (final MElement row : rows) {
                count++;
                final String id = row.getAsString("session_id") + "/" + row.getAsString("seller_id");
                switch (id) {
                    case "D/s1" -> {
                        final double[] x = {100, 200, 80};
                        final double mean = (x[0] + x[1] + x[2]) / 3;
                        double m2 = 0, m3 = 0, c1 = 0;
                        for (final double v : x) {
                            m2 += (v - mean) * (v - mean) / 3;
                            m3 += (v - mean) * (v - mean) * (v - mean) / 3;
                        }
                        for (int t = 1; t < 3; t++) c1 += (x[t] - mean) * (x[t - 1] - mean);
                        Assertions.assertEquals(m3 / Math.pow(m2, 1.5), row.getAsDouble("f_shape_all_start_price_skew"), 1e-9);
                        Assertions.assertTrue(row.getAsDouble("f_shape_all_start_price_skew") > 0);
                        Assertions.assertEquals(1L, row.getAsLong("f_shape_all_start_price_peaks"));
                        Assertions.assertEquals(c1 / (3 * m2), row.getAsDouble("f_shape_all_start_price_acf1"), 1e-9);
                        Assertions.assertEquals(1L, row.getAsLong("f_shape_all_vs100_zeroCross"));
                    }
                    case "C/s1" -> {
                        Assertions.assertNull(row.getPrimitiveValue("f_shape_all_start_price_skew"), "two values have no skew");
                        Assertions.assertEquals(0L, row.getAsLong("f_shape_all_start_price_peaks"));
                        Assertions.assertEquals(-0.5, row.getAsDouble("f_shape_all_start_price_acf1"), 1e-9);
                        Assertions.assertEquals(0L, row.getAsLong("f_shape_all_vs100_zeroCross"));
                    }
                    case "A/s1", "A/s2" -> {
                        Assertions.assertNull(row.getPrimitiveValue("f_shape_all_start_price_peaks"), id + ": no history");
                        Assertions.assertNull(row.getPrimitiveValue("f_shape_all_start_price_acf1"), id);
                    }
                    case "B/s1", "C/s2" -> {
                        Assertions.assertEquals(0L, row.getAsLong("f_shape_all_start_price_peaks"), id + ": one past value");
                        Assertions.assertNull(row.getPrimitiveValue("f_shape_all_start_price_acf1"), id);
                    }
                    default -> Assertions.fail("unexpected row " + id);
                }
            }
            Assertions.assertEquals(6, count);
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

    /**
     * A residual's {@code on:} scale written in YAML (a boolean key under YAML 1.1, a plain key under the 1.2 parser) reaches the evaluator:
     * session A, seller s1 has a start-price share of 100 / 150 against a market share of (1/120) / (1/120 + 1/55).
     */
    @Test
    public void testResidualScaleFromYaml() throws java.io.IOException {
        final String config = FEATURE_CONFIG.replace("          baseline: market\n", "          baseline: market\n          on: log\n");
        Assertions.assertTrue(config.contains("on: log"), "the residual line must match the text block's runtime indentation");
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(SOURCE_CONFIG + config));
        Assertions.assertEquals("log", outputs.get("features").getSchema().getField("f_vs_market").getOptions().get("feature.coord.on"));
        PAssert.that(outputs.get("features").getCollection()).satisfies(rows -> {
            for (final MElement row : rows) {
                if (!"A".equals(row.getAsString("session_id")) || !"s1".equals(row.getAsString("seller_id"))) continue;
                final double market = (1 / 120.0) / (1 / 120.0 + 1 / 55.0);
                Assertions.assertEquals(Math.log(100.0 / 150.0) - Math.log(market), row.getAsDouble("f_vs_market"), 1e-9);
            }
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
        // merge into a declared engine block: a second `engine:` key is a duplicate-key error
        final String linearEngine = featureConfig.contains("      engine: {")
                ? featureConfig.replace("      engine: {", "      engine: {parallelWaves: false, ")
                : featureConfig.replace("      lineage:", "      engine: {parallelWaves: false}\n      lineage:");
        final String linear = linearEngine
                .replace("name: features", "name: linear")
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

    private static final String GROUP_OP_BLOCKS = """
                - name: neutral
                  scope: context
                  context: byCategory
                  ops:
                    - {type: residualize, field: current_bid_t10, against: [start_price]}
                - name: strength
                  scope: context
                  context: byCategory
                  ops:
                    - {type: harville, field: start_price, top: [1, 2]}
            """.replaceAll("(?m)^", "    ");

    /**
     * The group solvers over the rows of a category (electronics: s1's four listings, toys: s2's two).
     * {@code residualize}: the bid regressed on the start price within the group — toys has fewer than p + 2 = 3 rows and
     * reads null. {@code harville}: the start price read as a strength, normalised over the group.
     */
    @Test
    public void testContextGroupSolvers() throws java.io.IOException {
        final String config = FEATURE_CONFIG
                .replace("        - {name: session, keys: [session_id]}\n", "        - {name: session, keys: [session_id]}\n        - {name: byCategory, keys: [category]}\n")
                .replace("      output:\n", GROUP_OP_BLOCKS + "      output:\n");
        Assertions.assertTrue(config.contains("byCategory, keys") && config.contains("type: harville"));
        final MCollection output = MPipeline.apply(pipeline, Config.load(SOURCE_CONFIG + config)).get("features");
        PAssert.that(output.getCollection()).satisfies(rows -> {
            // electronics: x = start_price, y = current_bid_t10
            final double[] x = {100, 200, 80, 120}, y = {120, 210, 90, 130};
            final String[] ids = {"A/s1", "B/s1", "C/s1", "D/s1"};
            double xMean = 0, yMean = 0, sxy = 0, sxx = 0, total = 0;
            for (int i = 0; i < 4; i++) {
                xMean += x[i] / 4;
                yMean += y[i] / 4;
                total += x[i];
            }
            for (int i = 0; i < 4; i++) {
                sxy += (x[i] - xMean) * (y[i] - yMean);
                sxx += (x[i] - xMean) * (x[i] - xMean);
            }
            final Map<String, double[]> expected = new HashMap<>();
            for (int i = 0; i < 4; i++) {
                final double p = x[i] / total;
                double second = 0;
                for (int j = 0; j < 4; j++) if (j != i) second += (x[j] / total) * p / (1 - x[j] / total);
                expected.put(ids[i], new double[]{(y[i] - yMean) - sxy / sxx * (x[i] - xMean), p, p + second});
            }
            int count = 0;
            for (final MElement row : rows) {
                count++;
                final String id = row.getAsString("session_id") + "/" + row.getAsString("seller_id");
                final double[] e = expected.get(id);
                if (e == null) {
                    // toys: two rows — no residual; both are within the first two
                    Assertions.assertNull(row.getPrimitiveValue("f_neutral_current_bid_t10_residualize"), id);
                    Assertions.assertEquals(1.0, ((Number) row.getPrimitiveValue("f_strength_start_price_harville_top2")).doubleValue(), 1e-12, id);
                    Assertions.assertEquals("A/s2".equals(id) ? 50.0 / 110 : 60.0 / 110, ((Number) row.getPrimitiveValue("f_strength_start_price_harville_top1")).doubleValue(), 1e-12, id);
                    continue;
                }
                Assertions.assertEquals(e[0], ((Number) row.getPrimitiveValue("f_neutral_current_bid_t10_residualize")).doubleValue(), 1e-9, id);
                Assertions.assertEquals(e[1], ((Number) row.getPrimitiveValue("f_strength_start_price_harville_top1")).doubleValue(), 1e-12, id);
                Assertions.assertEquals(e[2], ((Number) row.getPrimitiveValue("f_strength_start_price_harville_top2")).doubleValue(), 1e-12, id);
            }
            Assertions.assertEquals(6, count);
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testContextGroupSolversParallelMatchesLinear() throws java.io.IOException {
        // one more context branch of wave 1: the solvers sum in the order of the values, not of the rows' arrival
        assertParallelMatchesLinear(PARALLEL_CONFIG
                        .replace("        - {name: session, keys: [session_id]}\n", "        - {name: session, keys: [session_id]}\n        - {name: byCategory, keys: [category]}\n")
                        .replace("      output:\n", GROUP_OP_BLOCKS + "      output:\n"), 6,
                List.of("Wave1_FanIn"), List.of());
    }

    // ------------------------------------------------------------------------------------------
    // quantileTransform / svd
    // ------------------------------------------------------------------------------------------

    /** start_price over the 6 rows is 50, 60, 80, 100, 120, 200: with bins = 5 every value is a knot at i / 5. */
    @Test
    public void testQuantileTransform() throws java.io.IOException {
        final String dir = "target/feature-artifacts/" + java.util.UUID.randomUUID();
        final String blocks = """
                    - name: price_q
                      scope: population
                      type: quantileTransform
                      input: start_price
                      bins: 5
                      fit: {artifact: "%s"}
                    - name: price_z
                      scope: population
                      type: quantileTransform
                      input: start_price
                      bins: 5
                      distribution: normal
                """.formatted(dir);
        final String config = FEATURE_CONFIG.replace("      output:\n", blocks.replaceAll("(?m)^", "    ") + "      output:\n");
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(SOURCE_CONFIG + config));
        final MCollection output = outputs.get("features");
        Assertions.assertEquals(Schema.FieldType.FLOAT64.getType(), output.getSchema().getField("f_price_q").getFieldType().getType());
        Assertions.assertEquals("quantileTransform", output.getSchema().getField("f_price_z").getOptions().get("feature.operator"));
        PAssert.that(output.getCollection()).satisfies(rows -> {
            final Map<String, MElement> byKey = new HashMap<>();
            for (final MElement row : rows) byKey.put(row.getAsString("session_id") + "/" + row.getAsString("seller_id"), row);
            Assertions.assertEquals(6, byKey.size());
            final Map<String, Double> positions = Map.of("A/s1", 0.6, "A/s2", 0.0, "B/s1", 1.0, "C/s1", 0.4, "C/s2", 0.2, "D/s1", 0.8);
            for (final Map.Entry<String, Double> e : positions.entrySet()) {
                Assertions.assertEquals(e.getValue(), byKey.get(e.getKey()).getAsDouble("f_price_q"), 1e-9, e.getKey());
            }
            // normal scores: Φ⁻¹(0.4) and Φ⁻¹(0.6) are symmetric, the extremes are finite
            Assertions.assertEquals(-0.2533471031, byKey.get("C/s1").getAsDouble("f_price_z"), 1e-6);
            Assertions.assertEquals(0.2533471031, byKey.get("A/s1").getAsDouble("f_price_z"), 1e-6);
            Assertions.assertTrue(Double.isFinite(byKey.get("B/s1").getAsDouble("f_price_z")) && byKey.get("B/s1").getAsDouble("f_price_z") > 4);
            return null;
        });
        pipeline.run();
        final java.io.File[] dirs = new java.io.File(dir).listFiles();
        Assertions.assertNotNull(dirs);
        final java.io.File artifact = new java.io.File(dirs[0], "price_q.quantiles.json");
        Assertions.assertTrue(artifact.exists());
        final com.google.gson.JsonObject json = com.google.gson.JsonParser.parseString(java.nio.file.Files.readString(artifact.toPath())).getAsJsonObject();
        Assertions.assertEquals(5, json.get("bins").getAsInt());
        Assertions.assertEquals(6, json.get("n").getAsLong());
        Assertions.assertEquals("uniform", json.get("distribution").getAsString());
        Assertions.assertEquals(6, json.getAsJsonArray("knots").size());
        Assertions.assertEquals(50.0, json.getAsJsonArray("knots").get(0).getAsDouble(), 1e-9);
        Assertions.assertEquals(200.0, json.getAsJsonArray("knots").get(5).getAsDouble(), 1e-9);
        Assertions.assertFalse(new java.io.File(dirs[0], "price_z.quantiles.json").exists(), "no artifact URI for the second block");
    }

    private static String quantileForwardConfig(final String dir, final String extra) {
        final String blocks = """
                    - name: price_q
                      scope: population
                      type: quantileTransform
                      input: start_price
                      bins: 4
                      fit: {mode: forward, blocks: {size: P7D}%s, artifact: {uri: "%s"}}
                """.formatted(extra, dir);
        return FEATURE_CONFIG.replace("      output:\n", blocks.replaceAll("(?m)^", "    ") + "      output:\n");
    }

    private static Map<String, MElement> byRow(final Iterable<MElement> rows) {
        final Map<String, MElement> byKey = new HashMap<>();
        for (final MElement row : rows) byKey.put(row.getAsString("session_id") + "/" + row.getAsString("seller_id"), row);
        return byKey;
    }

    /**
     * quantileTransform under {@code fit.mode: forward} with weekly blocks (A = 2869, B = 2870, C = 2872, D = 2874):
     * every row is placed in the distribution of the complete preceding blocks. A reads nothing; B (200) lies above
     * {50, 100} → 1; C reads {50, 100, 200}, knots (50, 75, 100, 150, 200): 80 → (1 + 5/25) / 4 = 0.3, 60 → (10/25) / 4
     * = 0.1; D reads {50, 60, 80, 100, 200}, knots = the values: 120 → (3 + 20/100) / 4 = 0.8 (the static fit, which
     * counts D itself, puts it at 0.76). The artifact holds the whole-input knots.
     */
    @Test
    public void testQuantileTransformForwardFit() throws java.io.IOException {
        final String dir = "target/feature-artifacts/" + java.util.UUID.randomUUID();
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(SOURCE_CONFIG + quantileForwardConfig(dir, "")));
        Assertions.assertEquals("forward", outputs.get("features").getSchema().getField("f_price_q").getOptions().get("feature.coord.fit"));
        PAssert.that(outputs.get("features").getCollection()).satisfies(rows -> {
            final Map<String, MElement> byKey = byRow(rows);
            Assertions.assertEquals(6, byKey.size());
            Assertions.assertNull(byKey.get("A/s1").getPrimitiveValue("f_price_q"));
            Assertions.assertNull(byKey.get("A/s2").getPrimitiveValue("f_price_q"));
            Assertions.assertEquals(1.0, byKey.get("B/s1").getAsDouble("f_price_q"), 1e-9);
            Assertions.assertEquals(0.3, byKey.get("C/s1").getAsDouble("f_price_q"), 1e-9);
            Assertions.assertEquals(0.1, byKey.get("C/s2").getAsDouble("f_price_q"), 1e-9);
            Assertions.assertEquals(0.8, byKey.get("D/s1").getAsDouble("f_price_q"), 1e-9);
            return null;
        });
        pipeline.run();
        final java.io.File[] dirs = new java.io.File(dir).listFiles();
        Assertions.assertNotNull(dirs, "artifact directory missing: " + dir);
        final java.io.File artifact = new java.io.File(dirs[0], "price_q.quantiles.json");
        Assertions.assertTrue(artifact.exists(), "the whole-input knots are persisted for a static serving run");
        Assertions.assertEquals(6, com.google.gson.JsonParser.parseString(java.nio.file.Files.readString(artifact.toPath())).getAsJsonObject().get("n").getAsLong());

        // window: P14D → two blocks. C reads (2870, 2872) → block 2870 alone ({200}: 80 lies below it → 0);
        // D reads block 2872 ({60, 80}: 120 lies above → 1)
        final TestPipeline windowed = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        final Map<String, MCollection> windowedOut = MPipeline.apply(windowed, Config.load(SOURCE_CONFIG + quantileForwardConfig("target/feature-artifacts/" + java.util.UUID.randomUUID(), ", window: P14D")));
        PAssert.that(windowedOut.get("features").getCollection()).satisfies(rows -> {
            final Map<String, MElement> byKey = byRow(rows);
            Assertions.assertEquals(1.0, byKey.get("B/s1").getAsDouble("f_price_q"), 1e-9);
            Assertions.assertEquals(0.0, byKey.get("C/s1").getAsDouble("f_price_q"), 1e-9);
            Assertions.assertEquals(1.0, byKey.get("D/s1").getAsDouble("f_price_q"), 1e-9);
            return null;
        });
        windowed.run();

        // minBlocks: 2 → B (one preceding block) reads nothing, C (two) reads the same knots as above
        final TestPipeline min = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        final Map<String, MCollection> minOut = MPipeline.apply(min, Config.load(SOURCE_CONFIG + quantileForwardConfig("target/feature-artifacts/" + java.util.UUID.randomUUID(), ", minBlocks: 2")));
        PAssert.that(minOut.get("features").getCollection()).satisfies(rows -> {
            final Map<String, MElement> byKey = byRow(rows);
            Assertions.assertNull(byKey.get("B/s1").getPrimitiveValue("f_price_q"));
            Assertions.assertEquals(0.3, byKey.get("C/s1").getAsDouble("f_price_q"), 1e-9);
            return null;
        });
        min.run();
    }

    /**
     * A fit stage with many summary-fit blocks (five quantile transforms — static, forward, one over a field without a
     * single value —, three svds, static and forward, and a smooth curve, whose state is the svds' family) shares ONE {@code Combine.perKey} per summary family and one
     * side input, instead of a chain per block; the values are what each block gives on its own (duplicates agree, the
     * forward blocks read the hand-computed values of their own tests), and the block without values still fits
     * (n = 0: null everywhere) and writes its artifact.
     */
    @Test
    public void testFitStageSharesOneCombinePerFamily() throws java.io.IOException {
        final String dir = "target/feature-artifacts/" + java.util.UUID.randomUUID();
        final String blocks = """
                    - {name: q1, scope: population, type: quantileTransform, input: start_price, bins: 5, fit: {artifact: {uri: "%1$s"}}}
                    - {name: q1_again, scope: population, type: quantileTransform, input: start_price, bins: 5}
                    - {name: q_bid, scope: population, type: quantileTransform, input: current_bid_t10, bins: 5, distribution: normal}
                    - {name: q_forward, scope: population, type: quantileTransform, input: start_price, bins: 4, fit: {mode: forward, blocks: {size: P7D}}}
                    - {name: q_empty, scope: population, type: quantileTransform, input: reserve_price, bins: 5, fit: {artifact: {uri: "%1$s"}}}
                    - {name: pc, scope: population, type: svd, inputs: [start_price, current_bid_t10], rank: 2}
                    - {name: pc_again, scope: population, type: svd, inputs: [start_price, current_bid_t10], rank: 2, fit: {artifact: {uri: "%1$s"}}}
                    - {name: pc_forward, scope: population, type: svd, inputs: [start_price, current_bid_t10], rank: 2, fit: {mode: forward, blocks: {size: P7D}}}
                    - {name: bid_curve, scope: population, type: smooth, input: start_price, target: current_bid_t10, range: [0, 250], segments: 4, penalty: {lambda: 1000000000}}
                """.formatted(dir);
        // reserve_price: declared, never present in a row
        final String source = SOURCE_CONFIG.replace("        - {name: final_price, type: float64}\n",
                "        - {name: final_price, type: float64}\n        - {name: reserve_price, type: float64}\n");
        final String config = FEATURE_CONFIG
                .replace("              - {name: start_price, type: float64, kind: attribute}\n",
                        "              - {name: start_price, type: float64, kind: attribute}\n              - {name: reserve_price, type: float64, kind: attribute}\n")
                .replace("start_price, condition_grade], from: listings}", "start_price, condition_grade, reserve_price], from: listings}")
                .replace("      output:\n", blocks.replaceAll("(?m)^", "    ") + "      output:\n");
        Assertions.assertTrue(source.contains("reserve_price") && config.contains("reserve_price, type: float64, kind: attribute") && config.contains("condition_grade, reserve_price]"),
                "the schema / contract / lineage lines must match the text blocks' runtime indentation");
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(source + config));

        // the graph: one Combine per family for the whole stage, no per-block chains
        final Set<String> names = transformNames();
        // nine blocks: the stage's top-level transforms stay a handful (a chain per block made it 40)
        final long fitTransforms = names.stream().filter(n -> n.startsWith("features/") && n.contains("_fit_") && n.indexOf('/', "features/".length()) < 0).count();
        Assertions.assertTrue(fitTransforms <= 16, "fit stage transforms: " + fitTransforms);
        Assertions.assertEquals(1, names.stream().filter(n -> n.startsWith("features/") && n.endsWith("_FitValues_Combine")).count(), names::toString);
        Assertions.assertEquals(1, names.stream().filter(n -> n.startsWith("features/") && n.endsWith("_FitMoments_Combine")).count(), names::toString);
        Assertions.assertEquals(1, names.stream().filter(n -> n.startsWith("features/") && n.endsWith("_FitModelsView")).count(), names::toString);
        Assertions.assertFalse(hasTransform(names, "features", "_Quantiles_"), names::toString);
        Assertions.assertFalse(hasTransform(names, "features", "_Svd_"), names::toString);

        PAssert.that(outputs.get("features").getCollection()).satisfies(rows -> {
            final Map<String, MElement> byKey = byRow(rows);
            Assertions.assertEquals(6, byKey.size());
            double sum0 = 0;
            for (final MElement row : byKey.values()) {
                Assertions.assertEquals(row.getAsDouble("f_q1"), row.getAsDouble("f_q1_again"), 0, row::toString);
                Assertions.assertNotNull(row.getPrimitiveValue("f_q_bid"), row::toString);
                Assertions.assertNull(row.getPrimitiveValue("f_q_empty"), "no value was ever seen: " + row);
                for (int k = 0; k < 2; k++) Assertions.assertEquals(row.getAsDouble("f_pc_" + k), row.getAsDouble("f_pc_again_" + k), 0, row::toString);
                sum0 += row.getAsDouble("f_pc_0");
            }
            Assertions.assertEquals(0.0, sum0, 1e-9, "centred scores");
            // the static transform of the cheapest / dearest listing, and the forward values of testQuantileTransformForwardFit
            Assertions.assertEquals(0.0, byKey.get("A/s2").getAsDouble("f_q1"), 1e-9);
            Assertions.assertEquals(1.0, byKey.get("B/s1").getAsDouble("f_q1"), 1e-9);
            Assertions.assertNull(byKey.get("A/s1").getPrimitiveValue("f_q_forward"));
            Assertions.assertEquals(0.3, byKey.get("C/s1").getAsDouble("f_q_forward"), 1e-9);
            Assertions.assertEquals(0.8, byKey.get("D/s1").getAsDouble("f_q_forward"), 1e-9);
            // and of testSvdForwardFit
            Assertions.assertNull(byKey.get("A/s1").getPrimitiveValue("f_pc_forward_0"));
            final double[] d = svdScores(VEC_D1, VEC_A1, VEC_A2, VEC_B1, VEC_C1, VEC_C2);
            Assertions.assertEquals(d[0], byKey.get("D/s1").getAsDouble("f_pc_forward_0"), 1e-9);
            Assertions.assertEquals(d[1], byKey.get("D/s1").getAsDouble("f_pc_forward_1"), 1e-9);
            // the smooth curve rode the svds' Combine: under a heavy penalty, the least-squares line of the bid on the price
            // (Σx = 610, Σy = 675, Σxx = 76900, Σxy = 83750 over the six rows)
            final double slope = (83750 - 610 * 675 / 6.0) / (76900 - 610 * 610 / 6.0), intercept = 675 / 6.0 - slope * 610 / 6.0;
            Assertions.assertEquals(intercept + slope * 120, byKey.get("D/s1").getAsDouble("f_bid_curve"), 1e-3);
            Assertions.assertEquals(intercept + slope * 50, byKey.get("A/s2").getAsDouble("f_bid_curve"), 1e-3);
            return null;
        });
        pipeline.run();

        final java.io.File[] dirs = new java.io.File(dir).listFiles();
        Assertions.assertNotNull(dirs, "artifact directory missing: " + dir);
        Assertions.assertTrue(new java.io.File(dirs[0], "q1.quantiles.json").exists());
        Assertions.assertTrue(new java.io.File(dirs[0], "pc_again.svd.json").exists());
        final java.io.File empty = new java.io.File(dirs[0], "q_empty.quantiles.json");
        Assertions.assertTrue(empty.exists(), "a fit without a single value is still an artifact (n = 0)");
        Assertions.assertEquals(0, com.google.gson.JsonParser.parseString(java.nio.file.Files.readString(empty.toPath())).getAsJsonObject().get("n").getAsLong());
        Assertions.assertFalse(new java.io.File(dirs[0], "q1_again.quantiles.json").exists(), "no artifact URI, no artifact");
    }

    /**
     * svd over (start_price, current_bid_t10): the bid tracks the price (+5..+10), so the first component carries
     * nearly all the variance and the scores are centred and uncorrelated; the artifact holds the fitted moments.
     */
    @Test
    public void testSvd() throws java.io.IOException {
        final String dir = "target/feature-artifacts/" + java.util.UUID.randomUUID();
        final String blocks = """
                    - name: price_pc
                      scope: population
                      type: svd
                      inputs: [start_price, current_bid_t10]
                      rank: 2
                      fit: {artifact: "%s"}
                """.formatted(dir);
        final String config = FEATURE_CONFIG.replace("      output:\n", blocks.replaceAll("(?m)^", "    ") + "      output:\n");
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(SOURCE_CONFIG + config));
        final MCollection output = outputs.get("features");
        Assertions.assertNotNull(output.getSchema().getField("f_price_pc_0"));
        Assertions.assertNotNull(output.getSchema().getField("f_price_pc_1"));
        Assertions.assertNull(output.getSchema().getField("f_price_pc_2"));
        // current_bid_t10 is market information available 10 minutes before the event: the scores inherit it
        Assertions.assertTrue(output.getSchema().getField("f_price_pc_0").getOptions().get("feature.derivedFrom").contains("market"));
        PAssert.that(output.getCollection()).satisfies(rows -> {
            final List<MElement> list = new ArrayList<>();
            for (final MElement row : rows) list.add(row);
            Assertions.assertEquals(6, list.size());
            double sum0 = 0, sum1 = 0, cross = 0, var0 = 0, var1 = 0;
            for (final MElement row : list) {
                final double s0 = row.getAsDouble("f_price_pc_0"), s1 = row.getAsDouble("f_price_pc_1");
                sum0 += s0;
                sum1 += s1;
                cross += s0 * s1;
                var0 += s0 * s0;
                var1 += s1 * s1;
            }
            Assertions.assertEquals(0.0, sum0, 1e-9);
            Assertions.assertEquals(0.0, sum1, 1e-9);
            Assertions.assertEquals(0.0, cross, 1e-6);
            Assertions.assertTrue(var0 > 100 * var1, "the first component carries the shared price variance: " + var0 + " vs " + var1);
            // the most expensive listing (B/s1: 200 / 210) has the largest positive score on the price axis
            MElement max = list.get(0);
            for (final MElement row : list) if (row.getAsDouble("f_price_pc_0") > max.getAsDouble("f_price_pc_0")) max = row;
            Assertions.assertEquals("B", max.getAsString("session_id"));
            return null;
        });
        pipeline.run();
        final java.io.File[] dirs = new java.io.File(dir).listFiles();
        Assertions.assertNotNull(dirs);
        final java.io.File artifact = new java.io.File(dirs[0], "price_pc.svd.json");
        Assertions.assertTrue(artifact.exists());
        final com.google.gson.JsonObject json = com.google.gson.JsonParser.parseString(java.nio.file.Files.readString(artifact.toPath())).getAsJsonObject();
        Assertions.assertEquals(2, json.get("dimension").getAsInt());
        Assertions.assertEquals(2, json.get("rank").getAsInt());
        Assertions.assertEquals(6, json.get("n").getAsLong());
        Assertions.assertEquals(610.0 / 6, json.getAsJsonArray("mean").get(0).getAsDouble(), 1e-9);
        Assertions.assertEquals(675.0 / 6, json.getAsJsonArray("mean").get(1).getAsDouble(), 1e-9);
        final double v0 = json.getAsJsonArray("variances").get(0).getAsDouble(), v1 = json.getAsJsonArray("variances").get(1).getAsDouble();
        Assertions.assertEquals(json.get("totalVariance").getAsDouble(), v0 + v1, 1e-6);
        Assertions.assertTrue(v0 > v1);
    }

    /**
     * The same svd over an array field declared as {@code array<float64>} in the sources contract (the input schema
     * declares it {@code mode: repeated}): the vector [start_price, current_bid_t10] per row gives the scores of
     * {@link #testSvd()} — the array path and the {@code inputs} path are the same fit.
     */
    @Test
    public void testSvdArrayInput() throws java.io.IOException {
        final String[][] rows = {
                {"current_bid_t10: 120.0,", "[100.0, 120.0]"}, {"current_bid_t10: 55.0, ", "[50.0, 55.0]"}, {"current_bid_t10: 210.0,", "[200.0, 210.0]"},
                {"current_bid_t10: 90.0, ", "[80.0, 90.0]"}, {"current_bid_t10: 70.0, ", "[60.0, 70.0]"}, {"current_bid_t10: 130.0,", "[120.0, 130.0]"}};
        // the text blocks strip their closing delimiter's 12 spaces: schema fields run at 8 spaces, contract fields at 14
        String source = SOURCE_CONFIG.replace("        - {name: current_bid_t10, type: float64}\n",
                "        - {name: current_bid_t10, type: float64}\n        - {name: price_vec, type: float64, mode: repeated}\n");
        Assertions.assertTrue(source.contains("price_vec"), "the schema field line must match the text block's runtime indentation");
        for (final String[] row : rows) {
            Assertions.assertTrue(source.contains(row[0]), row[0]);
            source = source.replace(row[0], row[0] + " price_vec: " + row[1] + ",");
        }
        final String blocks = """
                    - name: price_pc
                      scope: population
                      type: svd
                      inputs: [start_price, current_bid_t10]
                      rank: 2
                    - name: vec_pc
                      scope: population
                      type: svd
                      input: price_vec
                      rank: 2
                """;
        final String config = FEATURE_CONFIG
                .replace("              - {name: current_bid_t10, type: float64, availableAt: \"event_time - PT10M\", observedAtField: snapshot_time, kind: market}\n",
                        "              - {name: current_bid_t10, type: float64, availableAt: \"event_time - PT10M\", observedAtField: snapshot_time, kind: market}\n"
                                + "              - {name: price_vec, type: array<float64>, availableAt: \"event_time - PT10M\", observedAtField: snapshot_time, kind: market}\n")
                .replace("- {fields: [current_bid_t10], from: price_snapshots}", "- {fields: [current_bid_t10, price_vec], from: price_snapshots}")
                .replace("      output:\n", blocks.replaceAll("(?m)^", "    ") + "      output:\n");
        Assertions.assertTrue(config.contains("array<float64>") && config.contains("current_bid_t10, price_vec]"), "the contract lines must match the text block's runtime indentation");
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(source + config));
        final MCollection output = outputs.get("features");
        Assertions.assertEquals(Schema.Type.array, output.getSchema().getField("price_vec").getFieldType().getType(), "the array input passes through");
        Assertions.assertNotNull(output.getSchema().getField("f_vec_pc_1"));
        Assertions.assertTrue(output.getSchema().getField("f_vec_pc_0").getOptions().get("feature.derivedFrom").contains("market"));
        PAssert.that(output.getCollection()).satisfies(rows_ -> {
            int count = 0;
            for (final MElement row : rows_) {
                count++;
                for (int k = 0; k < 2; k++) {
                    Assertions.assertEquals(row.getAsDouble("f_price_pc_" + k), row.getAsDouble("f_vec_pc_" + k), 1e-9, row.getAsString("session_id") + "/" + k);
                }
            }
            Assertions.assertEquals(6, count);
            return null;
        });
        pipeline.run();
    }

    /**
     * Row {@code type: vector} over {@code bid_path}, the bids observed before the session: readouts of the whole
     * path, of the differences of its last three elements, and an expr composing two readouts. An empty path has
     * length 0 and no other readout — a repeated field without a value (C/s2) arrives as the empty array, never as
     * null; a single bid has no slope.
     */
    @Test
    public void testVector() throws java.io.IOException {
        final String[][] rows = {
                {"current_bid_t10: 120.0,", "[100.0, 110.0, 120.0]"}, {"current_bid_t10: 55.0, ", "[50.0, 55.0]"}, {"current_bid_t10: 210.0,", "[200.0, 205.0, 203.0, 210.0]"},
                {"current_bid_t10: 90.0, ", "[90.0]"}, {"current_bid_t10: 70.0, ", null}, {"current_bid_t10: 130.0,", "[]"}};
        String source = SOURCE_CONFIG.replace("        - {name: current_bid_t10, type: float64}\n",
                "        - {name: current_bid_t10, type: float64}\n        - {name: bid_path, type: float64, mode: repeated}\n");
        Assertions.assertTrue(source.contains("bid_path"), "the schema field line must match the text block's runtime indentation");
        for (final String[] row : rows) {
            Assertions.assertTrue(source.contains(row[0]), row[0]);
            if (row[1] != null) source = source.replace(row[0], row[0] + " bid_path: " + row[1] + ",");
        }
        final String blocks = """
                    - name: bids
                      scope: row
                      type: vector
                      input: bid_path
                      funcs: [length, mean, last, argmax, slope, polyfit]
                      degree: 1
                    - name: bid_step
                      scope: row
                      type: vector
                      input: bid_path
                      slice: {from: -3}
                      diff: 1
                      funcs: [mean]
                    - name: bid_lift
                      scope: row
                      expr: "bids_last / bids_mean"
                """;
        final String config = FEATURE_CONFIG
                .replace("              - {name: current_bid_t10, type: float64, availableAt: \"event_time - PT10M\", observedAtField: snapshot_time, kind: market}\n",
                        "              - {name: current_bid_t10, type: float64, availableAt: \"event_time - PT10M\", observedAtField: snapshot_time, kind: market}\n"
                                + "              - {name: bid_path, type: array<float64>, availableAt: \"event_time - PT10M\", observedAtField: snapshot_time, kind: market}\n")
                .replace("- {fields: [current_bid_t10], from: price_snapshots}", "- {fields: [current_bid_t10, bid_path], from: price_snapshots}")
                .replace("      output:\n", blocks.replaceAll("(?m)^", "    ") + "      output:\n");
        Assertions.assertTrue(config.contains("array<float64>") && config.contains("current_bid_t10, bid_path]"), "the contract lines must match the text block's runtime indentation");
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(source + config));
        final MCollection output = outputs.get("features");
        Assertions.assertEquals(Schema.Type.int64, output.getSchema().getField("f_bids_length").getFieldType().getType());
        Assertions.assertEquals(Schema.Type.int64, output.getSchema().getField("f_bids_argmax").getFieldType().getType());
        Assertions.assertEquals(Schema.Type.float64, output.getSchema().getField("f_bids_poly1").getFieldType().getType());
        Assertions.assertNull(output.getSchema().getField("f_bids_poly2"));
        Assertions.assertEquals("vector", output.getSchema().getField("f_bids_slope").getOptions().get("feature.operator"));
        Assertions.assertTrue(output.getSchema().getField("f_bids_slope").getOptions().get("feature.derivedFrom").contains("market"), "the readouts inherit the array field's lineage");
        PAssert.that(output.getCollection()).satisfies(rows_ -> {
            int count = 0;
            for (final MElement row : rows_) {
                count++;
                final String id = row.getAsString("session_id") + "/" + row.getAsString("seller_id");
                switch (id) {
                    case "A/s1" -> {
                        Assertions.assertEquals(3L, row.getAsLong("f_bids_length"));
                        Assertions.assertEquals(110.0, row.getAsDouble("f_bids_mean"), 1e-9);
                        Assertions.assertEquals(2L, row.getAsLong("f_bids_argmax"));
                        Assertions.assertEquals(10.0, row.getAsDouble("f_bids_slope"), 1e-9);
                        // degree 1 is the regression line 100 + 10 p
                        Assertions.assertEquals(100.0, row.getAsDouble("f_bids_poly0"), 1e-6);
                        Assertions.assertEquals(10.0, row.getAsDouble("f_bids_poly1"), 1e-6);
                        Assertions.assertEquals(10.0, row.getAsDouble("f_bid_step_mean"), 1e-9);
                        Assertions.assertEquals(120.0 / 110.0, row.getAsDouble("f_bid_lift"), 1e-9);
                    }
                    case "A/s2" -> {
                        Assertions.assertEquals(2L, row.getAsLong("f_bids_length"));
                        Assertions.assertEquals(5.0, row.getAsDouble("f_bids_slope"), 1e-9);
                        Assertions.assertEquals(5.0, row.getAsDouble("f_bid_step_mean"), 1e-9, "the slice is clamped to the two bids there are");
                    }
                    case "B/s1" -> {
                        Assertions.assertEquals(204.5, row.getAsDouble("f_bids_mean"), 1e-9);
                        Assertions.assertEquals(3L, row.getAsLong("f_bids_argmax"));
                        // Σ(p − 1.5)(x − 204.5) / Σ(p − 1.5)² = 14 / 5
                        Assertions.assertEquals(2.8, row.getAsDouble("f_bids_slope"), 1e-9);
                        // the last three bids 205, 203, 210 step by −2 and +7
                        Assertions.assertEquals(2.5, row.getAsDouble("f_bid_step_mean"), 1e-9);
                    }
                    case "C/s1" -> {
                        Assertions.assertEquals(1L, row.getAsLong("f_bids_length"));
                        Assertions.assertEquals(90.0, row.getAsDouble("f_bids_last"), 1e-9);
                        Assertions.assertEquals(0L, row.getAsLong("f_bids_argmax"));
                        Assertions.assertNull(row.getPrimitiveValue("f_bids_slope"), "one bid has no slope");
                        Assertions.assertNull(row.getPrimitiveValue("f_bids_poly1"));
                        Assertions.assertNull(row.getPrimitiveValue("f_bid_step_mean"), "one bid has no step");
                        Assertions.assertEquals(1.0, row.getAsDouble("f_bid_lift"), 1e-9);
                    }
                    case "C/s2", "D/s1" -> {
                        Assertions.assertEquals(0L, row.getAsLong("f_bids_length"), "an empty path is a vector of length 0");
                        for (final String name : List.of("f_bids_mean", "f_bids_last", "f_bids_argmax", "f_bids_slope", "f_bid_step_mean", "f_bid_lift")) {
                            Assertions.assertNull(row.getPrimitiveValue(name), name + " of an empty bid path: " + row);
                        }
                    }
                    default -> Assertions.fail("unexpected row " + id);
                }
            }
            Assertions.assertEquals(6, count);
            return null;
        });
        pipeline.run();
    }

    // ------------------------------------------------------------------------------------------
    // estimator: joint / conjugate families
    // ------------------------------------------------------------------------------------------

    private static String jointConfig(final String dir, final String mode) {
        return FEATURE_CONFIG
                .replace("            - keys: [seller_id]\n          targets:", "            - {keys: [seller_id]}\n            - {keys: [category]}\n            - {keys: [seller_id, category], structure: cross}\n          targets:")
                .replace("- {stats: [count]}\n", "")
                .replace("- {expr: \"sold >= 1\", stats: [mean]}",
                        "- {expr: \"sold >= 1\", stats: [mean]}\n          shrinkage: {estimator: joint, scale: identity, priorWeight: 1, output: [composed, deviations, effectiveN]}")
                .replace("      output:\n", "      fit: {mode: " + mode + ", artifact: {uri: \"" + dir + "\"}}\n      output:\n");
    }

    /**
     * seller and category are perfectly confounded here (s1 ↔ electronics, s2 ↔ toys), so the cell, seller and
     * category levels share the two contexts (n = 4, ȳ = 3/4) and (n = 2, ȳ = 1/2). With λ = 1 on every level the
     * ridge normal equations give, for the cross lattice, μ = 17/27 and effects ±1/27 per level (13 e₁ = 3 − 4μ,
     * 7 e₂ = 1 − 2μ, 6μ = 4 − 12 e₁ − 6 e₂), and for the seller-only lattice μ = 7/11, a₁ = 1/11
     * (5 a₁ = 3 − 4μ, 3 a₂ = 1 − 2μ, 6μ = 4 − 4 a₁ − 2 a₂).
     */
    @Test
    public void testJointEstimatorStaticFit() throws java.io.IOException {
        final String dir = "target/feature-artifacts/" + java.util.UUID.randomUUID();
        final String config = jointConfig(dir, "static");
        Assertions.assertTrue(config.contains("estimator: joint"), config);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(SOURCE_CONFIG + config));
        final Schema schema = outputs.get("features").getSchema();
        Assertions.assertEquals("joint", schema.getField("f_enc__seller_id_category__e1__mean").getOptions().get("feature.operator"));
        Assertions.assertEquals("joint", schema.getField("f_enc__seller_id_category__e1__mean").getOptions().get("feature.coord.estimator"));
        PAssert.that(outputs.get("features").getCollection()).satisfies(rows -> {
            final Map<String, MElement> byKey = new HashMap<>();
            for (final MElement row : rows) byKey.put(row.getAsString("session_id") + "/" + row.getAsString("seller_id"), row);
            Assertions.assertEquals(6, byKey.size());
            final double mu = 17.0 / 27, e = 1.0 / 27;
            for (final String s1 : List.of("A/s1", "B/s1", "C/s1", "D/s1")) {
                Assertions.assertEquals(mu + 3 * e, byKey.get(s1).getAsDouble("f_enc__seller_id_category__e1__mean"), 1e-6, s1);
                Assertions.assertEquals(e, byKey.get(s1).getAsDouble("f_enc__seller_id_category__e1__dev0"), 1e-6, s1);
                Assertions.assertEquals(e, byKey.get(s1).getAsDouble("f_enc__seller_id_category__e1__dev1"), 1e-6, s1);
                Assertions.assertEquals(5.0, byKey.get(s1).getAsDouble("f_enc__seller_id_category__e1__mean__neff"), 1e-9, s1); // n = 4 + λ
                Assertions.assertEquals(8.0 / 11, byKey.get(s1).getAsDouble("f_enc__seller_id__e1__mean"), 1e-6, s1);
            }
            for (final String s2 : List.of("A/s2", "C/s2")) {
                Assertions.assertEquals(mu - 3 * e, byKey.get(s2).getAsDouble("f_enc__seller_id_category__e1__mean"), 1e-6, s2);
                Assertions.assertEquals(-e, byKey.get(s2).getAsDouble("f_enc__seller_id_category__e1__dev2"), 1e-6, s2);
                Assertions.assertEquals(3.0, byKey.get(s2).getAsDouble("f_enc__seller_id_category__e1__mean__neff"), 1e-9, s2);
            }
            return null;
        });
        pipeline.run();

        // one artifact per joint model (keySet × target), plus its manifest
        final java.io.File[] files = new java.io.File(dir).listFiles();
        Assertions.assertNotNull(files, "artifact directory missing: " + dir);
        Assertions.assertEquals(1, files.length, "one plan hash directory expected");
        for (final String id : List.of("enc__seller_id_category__e1", "enc__seller_id__e1", "enc__category__e1")) {
            Assertions.assertTrue(new java.io.File(files[0], id + ".joint.avro").exists(), id);
            Assertions.assertTrue(new java.io.File(files[0], id + ".joint.manifest.json").exists(), id);
        }
        final String manifest = java.nio.file.Files.readString(new java.io.File(files[0], "enc__seller_id_category__e1.joint.manifest.json").toPath());
        Assertions.assertTrue(manifest.contains("\"estimator\":\"joint\""), manifest);

        // run 2: same plan hash on a subset → the solution is loaded, not re-fitted (s1 rows keep 20/27)
        final String subset = SOURCE_CONFIG
                .replace("        - {session_id: A, seller_id: s1, category: electronics, quantity: 2, start_price: 100.0, condition_grade: good, current_bid_t10: 120.0, sold: 1, final_price: 150.0, session_time: \"2025-01-01T10:00:00Z\"}\n", "")
                .replace("        - {session_id: C, seller_id: s1, category: electronics, quantity: 4, start_price: 80.0,  condition_grade: fair, current_bid_t10: 90.0,  sold: 1, final_price: 95.0,  session_time: \"2025-01-20T10:00:00Z\"}\n", "");
        final TestPipeline second = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        final Map<String, MCollection> reused = MPipeline.apply(second, Config.load(subset + config));
        PAssert.that(reused.get("features").getCollection()).satisfies(rows -> {
            int n = 0;
            for (final MElement row : rows) {
                n++;
                if ("s1".equals(row.getAsString("seller_id"))) Assertions.assertEquals(20.0 / 27, row.getAsDouble("f_enc__seller_id_category__e1__mean"), 1e-6);
            }
            Assertions.assertEquals(4, n);
            return null;
        });
        second.run();
    }

    /** fold with entity folds: a seller's rows read the solution fitted without the seller — only the other cell remains, so its intercept. */
    @Test
    public void testJointEstimatorFoldFit() throws java.io.IOException {
        final java.util.function.IntFunction<Integer> foldOf = folds -> com.mercari.solution.util.pipeline.feature.VarianceComponents.foldOf(
                com.mercari.solution.util.pipeline.feature.FeatureValues.key(Map.of("seller_id", "s1"), List.of("seller_id")), folds);
        final java.util.function.IntFunction<Integer> foldOf2 = folds -> com.mercari.solution.util.pipeline.feature.VarianceComponents.foldOf(
                com.mercari.solution.util.pipeline.feature.FeatureValues.key(Map.of("seller_id", "s2"), List.of("seller_id")), folds);
        int folds = 2;
        while (foldOf.apply(folds).equals(foldOf2.apply(folds))) folds++;
        final String dir = "target/feature-artifacts/" + java.util.UUID.randomUUID();
        final String config = jointConfig(dir, "fold, folds: " + folds + ", groupBy: seller");
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(SOURCE_CONFIG + config));
        PAssert.that(outputs.get("features").getCollection()).satisfies(rows -> {
            final Map<String, MElement> byKey = new HashMap<>();
            for (final MElement row : rows) byKey.put(row.getAsString("session_id") + "/" + row.getAsString("seller_id"), row);
            Assertions.assertEquals(6, byKey.size());
            // out of s1's fold only s2's cell (sold 0, 1) remains: μ = 1/2, every context unseen → 1/2; and the reverse 3/4
            Assertions.assertEquals(0.5, byKey.get("A/s1").getAsDouble("f_enc__seller_id_category__e1__mean"), 1e-9);
            Assertions.assertEquals(0.5, byKey.get("D/s1").getAsDouble("f_enc__seller_id_category__e1__mean"), 1e-9);
            Assertions.assertEquals(0.0, byKey.get("D/s1").getAsDouble("f_enc__seller_id_category__e1__dev0"), 1e-9);
            Assertions.assertEquals(0.75, byKey.get("A/s2").getAsDouble("f_enc__seller_id_category__e1__mean"), 1e-9);
            Assertions.assertEquals(0.75, byKey.get("C/s2").getAsDouble("f_enc__seller_id__e1__mean"), 1e-9);
            return null;
        });
        pipeline.run();
        // the artifact holds the whole-input solution
        final java.io.File[] files = new java.io.File(dir).listFiles();
        Assertions.assertNotNull(files, "artifact directory missing: " + dir);
        Assertions.assertTrue(new java.io.File(files[0], "enc__seller_id_category__e1.joint.avro").exists());
    }

    private static double logit(final double p) {
        return Math.log(p / (1 - p));
    }

    /** The market baseline of each row: share(1 / current_bid_t10) within its session. */
    private static final double MARKET_A1 = 55.0 / 175, MARKET_A2 = 120.0 / 175, MARKET_B1 = 1.0, MARKET_C1 = 70.0 / 160, MARKET_C2 = 90.0 / 160, MARKET_D1 = 1.0;

    /**
     * Expanding encoding with a baseline offset on the logit scale: the composed value is the shrunk log-odds ratio
     * of the seller's observed sale rate against its mean market baseline (spec §3 rule 5), the global term being
     * the leave-node-out counterpart; on the identity scale the same declaration is the mean residual as before.
     */
    @Test
    public void testOffsetOnLogitScale() throws java.io.IOException {
        final String logitConfig = FEATURE_CONFIG.replace("- {expr: \"sold >= 1\", stats: [mean]}",
                "- {expr: \"sold >= 1\", stats: [mean]}\n          offset: market\n          shrinkage: {priorWeight: 1, scale: logit, output: [composed, deviations]}");
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(SOURCE_CONFIG + logitConfig));
        final Schema schema = outputs.get("features").getSchema();
        Assertions.assertNotNull(schema.getField("f_enc__seller_id__e2__mean"));
        Assertions.assertNull(schema.getField("enc__seller_id__e2__sumoff"), "hidden statistics are not emitted");
        Assertions.assertTrue(schema.getField("f_enc__seller_id__e2__mean").getOptions().get("feature.derivedFrom").contains("market"), "lineage carries the baseline");
        PAssert.that(outputs.get("features").getCollection()).satisfies(rows -> {
            final Map<String, MElement> byKey = new HashMap<>();
            for (final MElement row : rows) byKey.put(row.getAsString("session_id") + "/" + row.getAsString("seller_id"), row);
            // Jan 3: no outcome has reached the system yet
            Assertions.assertNull(byKey.get("B/s1").getPrimitiveValue("f_enc__seller_id__e2__mean"));
            // Feb 1, s1: own rows A (1), B (0), C (1) → observed 2/3 vs mean baseline (bA1 + bB1 + bC1) / 3;
            // global without s1 = s2's rows A (0), C (1) → observed 1/2 vs (bA2 + bC2) / 2; w = 3 / (3 + 1)
            final double own = logit(2.0 / 3) - logit((MARKET_A1 + MARKET_B1 + MARKET_C1) / 3);
            final double root = logit(0.5) - logit((MARKET_A2 + MARKET_C2) / 2);
            Assertions.assertEquals(root + 0.75 * (own - root), byKey.get("D/s1").getAsDouble("f_enc__seller_id__e2__mean"), 1e-9);
            Assertions.assertEquals(0.75 * (own - root), byKey.get("D/s1").getAsDouble("f_enc__seller_id__e2__dev0"), 1e-9);
            return null;
        });
        pipeline.run();

        // identity: Σ(y − b) / n shrunk toward the leave-node-out global mean residual — unchanged by the offset sum
        final String identityConfig = logitConfig.replace("scale: logit, ", "");
        final TestPipeline second = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        final Map<String, MCollection> identity = MPipeline.apply(second, Config.load(SOURCE_CONFIG + identityConfig));
        PAssert.that(identity.get("features").getCollection()).satisfies(rows -> {
            for (final MElement row : rows) {
                if (!"D".equals(row.getAsString("session_id"))) continue;
                final double own = (2 - (MARKET_A1 + MARKET_B1 + MARKET_C1)) / 3;
                final double root = (1 - (MARKET_A2 + MARKET_C2)) / 2;
                Assertions.assertEquals(root + 0.75 * (own - root), row.getAsDouble("f_enc__seller_id__e2__mean"), 1e-9);
            }
            return null;
        });
        second.run();
    }

    /**
     * The same declaration under {@code fit.mode: static}: the fit stage keeps Σ baseline per key next to (n, Σy, Σy²),
     * the artifact persists it, and a second run applies the loaded statistics (the fitted log-odds ratio is reproduced
     * from the artifact alone).
     */
    @Test
    public void testOffsetOnLogitScaleStaticFitAndArtifact() throws java.io.IOException {
        final String dir = "target/feature-artifacts/" + java.util.UUID.randomUUID();
        final String config = FEATURE_CONFIG
                .replace("- {expr: \"sold >= 1\", stats: [mean]}",
                        "- {expr: \"sold >= 1\", stats: [mean]}\n          offset: market\n          shrinkage: {priorWeight: 1, scale: logit}")
                .replace("      output:\n", "      fit: {mode: static, artifact: {uri: \"" + dir + "\"}}\n      output:\n");
        // s1: rows A, B, C, D → observed 3/4 vs mean baseline; global without s1 = s2's rows → 1/2 vs its mean baseline; w = 4/5
        final double own = logit(0.75) - logit((MARKET_A1 + MARKET_B1 + MARKET_C1 + MARKET_D1) / 4);
        final double root = logit(0.5) - logit((MARKET_A2 + MARKET_C2) / 2);
        final double expected = root + 0.8 * (own - root);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(SOURCE_CONFIG + config));
        PAssert.that(outputs.get("features").getCollection()).satisfies(rows -> {
            for (final MElement row : rows) {
                if ("s1".equals(row.getAsString("seller_id"))) Assertions.assertEquals(expected, row.getAsDouble("f_enc__seller_id__e2__mean"), 1e-9);
            }
            return null;
        });
        pipeline.run();

        final java.io.File[] files = new java.io.File(dir).listFiles();
        Assertions.assertNotNull(files, "artifact directory missing: " + dir);
        Assertions.assertTrue(new java.io.File(files[0], "enc.avro").exists());

        // run 2 on a subset: the statistics (Σ baseline included) come from the artifact
        final String subset = SOURCE_CONFIG
                .replace("        - {session_id: A, seller_id: s1, category: electronics, quantity: 2, start_price: 100.0, condition_grade: good, current_bid_t10: 120.0, sold: 1, final_price: 150.0, session_time: \"2025-01-01T10:00:00Z\"}\n", "")
                .replace("        - {session_id: C, seller_id: s1, category: electronics, quantity: 4, start_price: 80.0,  condition_grade: fair, current_bid_t10: 90.0,  sold: 1, final_price: 95.0,  session_time: \"2025-01-20T10:00:00Z\"}\n", "");
        final TestPipeline second = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        final Map<String, MCollection> reused = MPipeline.apply(second, Config.load(subset + config));
        PAssert.that(reused.get("features").getCollection()).satisfies(rows -> {
            int n = 0;
            for (final MElement row : rows) {
                n++;
                if ("s1".equals(row.getAsString("seller_id"))) Assertions.assertEquals(expected, row.getAsDouble("f_enc__seller_id__e2__mean"), 1e-9);
            }
            Assertions.assertEquals(4, n);
            return null;
        });
        second.run();
    }

    /**
     * {@code estimator: joint} with an offset on the logit scale solves the ridge over each cell's log-odds ratio
     * against its mean baseline: the estimate is the additive term (finite, ordered like the cells' own terms),
     * and the artifact manifest records the offset.
     */
    @Test
    public void testJointEstimatorOffsetOnLogitScale() throws java.io.IOException {
        final String dir = "target/feature-artifacts/" + java.util.UUID.randomUUID();
        final String config = jointConfig(dir, "static")
                .replace("shrinkage: {estimator: joint, scale: identity, priorWeight: 1, output: [composed, deviations, effectiveN]}",
                        "offset: market\n          shrinkage: {estimator: joint, scale: logit, priorWeight: 1, output: [composed, deviations, effectiveN]}");
        Assertions.assertTrue(config.contains("offset: market"), config);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(SOURCE_CONFIG + config));
        PAssert.that(outputs.get("features").getCollection()).satisfies(rows -> {
            final Map<String, MElement> byKey = new HashMap<>();
            for (final MElement row : rows) byKey.put(row.getAsString("session_id") + "/" + row.getAsString("seller_id"), row);
            final double s1 = byKey.get("A/s1").getAsDouble("f_enc__seller_id__e1__mean");
            final double s2 = byKey.get("A/s2").getAsDouble("f_enc__seller_id__e1__mean");
            Assertions.assertTrue(Double.isFinite(s1) && Double.isFinite(s2));
            // s1 sells 3/4 against a mean baseline of ~0.69 (positive term), s2 1/2 against ~0.62 (negative term)
            final double z1 = logit(0.75) - logit((MARKET_A1 + MARKET_B1 + MARKET_C1 + MARKET_D1) / 4);
            final double z2 = logit(0.5) - logit((MARKET_A2 + MARKET_C2) / 2);
            Assertions.assertTrue(z1 > 0 && z2 < 0);
            Assertions.assertTrue(s1 > s2, s1 + " > " + s2);
            // the ridge pulls both terms toward the intercept, which lies between them
            Assertions.assertTrue(s1 < z1 && s2 > z2, s1 + " / " + s2);
            Assertions.assertEquals(s1, byKey.get("D/s1").getAsDouble("f_enc__seller_id__e1__mean"), 1e-12, "static: every row of the key reads the same term");
            return null;
        });
        pipeline.run();
        final java.io.File[] files = new java.io.File(dir).listFiles();
        Assertions.assertNotNull(files, "artifact directory missing: " + dir);
        final String manifest = java.nio.file.Files.readString(new java.io.File(files[0], "enc__seller_id__e1.joint.manifest.json").toPath());
        Assertions.assertTrue(manifest.contains("\"offset\":true"), manifest);
        Assertions.assertTrue(manifest.contains("\"scale\":\"logit\""), manifest);
    }

    /** The row vectors [start_price, current_bid_t10] of the auction rows. */
    private static final double[] VEC_A1 = {100, 120}, VEC_A2 = {50, 55}, VEC_B1 = {200, 210}, VEC_C1 = {80, 90}, VEC_C2 = {60, 70}, VEC_D1 = {120, 130};

    /** The svd scores of {@code x} fitted on {@code vectors} (rank 2, centred), as the engine computes them. */
    private static double[] svdScores(final double[] x, final double[]... vectors) {
        final com.mercari.solution.util.pipeline.feature.Svd.Moments m = new com.mercari.solution.util.pipeline.feature.Svd.Moments();
        for (final double[] v : vectors) m.add(v);
        return com.mercari.solution.util.pipeline.feature.Svd.fit(m, 2, true, false).transform(x);
    }

    private static String svdForwardConfig(final String dir, final String extra) {
        final String blocks = """
                    - name: pc
                      scope: population
                      type: svd
                      inputs: [start_price, current_bid_t10]
                      rank: 2
                      fit: {mode: forward, blocks: {size: P7D}%s, artifact: {uri: "%s"}}
                """.formatted(extra, dir);
        return FEATURE_CONFIG.replace("      output:\n", blocks.replaceAll("(?m)^", "    ") + "      output:\n");
    }

    private static void assertScores(final MElement row, final double[] expected) {
        if (expected == null) {
            Assertions.assertNull(row.getPrimitiveValue("f_pc_0"), row::toString);
            Assertions.assertNull(row.getPrimitiveValue("f_pc_1"), row::toString);
            return;
        }
        Assertions.assertEquals(expected[0], row.getAsDouble("f_pc_0"), 1e-9, row::toString);
        Assertions.assertEquals(expected[1], row.getAsDouble("f_pc_1"), 1e-9, row::toString);
    }

    /**
     * svd under {@code fit.mode: forward} with weekly blocks (A = 2869, B = 2870, C = 2872, D = 2874): every row is
     * projected with the components fitted on the vectors of the complete preceding blocks — A reads nothing, B the
     * two vectors of A's block, C the three of blocks 2869–2870, D the five of 2869–2872 — and the artifact holds the
     * whole-input components. A window of two blocks limits C to block 2870 (one vector: nothing fitted) and D to
     * block 2872 (C's two vectors); {@code minBlocks: 2} blanks B.
     */
    @Test
    public void testSvdForwardFit() throws java.io.IOException {
        final String dir = "target/feature-artifacts/" + java.util.UUID.randomUUID();
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(SOURCE_CONFIG + svdForwardConfig(dir, "")));
        Assertions.assertEquals("forward", outputs.get("features").getSchema().getField("f_pc_0").getOptions().get("feature.coord.fit"));
        PAssert.that(outputs.get("features").getCollection()).satisfies(rows -> {
            final Map<String, MElement> byKey = new HashMap<>();
            for (final MElement row : rows) byKey.put(row.getAsString("session_id") + "/" + row.getAsString("seller_id"), row);
            Assertions.assertEquals(6, byKey.size());
            assertScores(byKey.get("A/s1"), null);
            assertScores(byKey.get("A/s2"), null);
            assertScores(byKey.get("B/s1"), svdScores(VEC_B1, VEC_A1, VEC_A2));
            assertScores(byKey.get("C/s1"), svdScores(VEC_C1, VEC_A1, VEC_A2, VEC_B1));
            assertScores(byKey.get("C/s2"), svdScores(VEC_C2, VEC_A1, VEC_A2, VEC_B1));
            assertScores(byKey.get("D/s1"), svdScores(VEC_D1, VEC_A1, VEC_A2, VEC_B1, VEC_C1, VEC_C2));
            return null;
        });
        pipeline.run();
        final java.io.File[] dirs = new java.io.File(dir).listFiles();
        Assertions.assertNotNull(dirs, "artifact directory missing: " + dir);
        final java.io.File artifact = new java.io.File(dirs[0], "pc.svd.json");
        Assertions.assertTrue(artifact.exists(), "the whole-input components are persisted for a static serving run");
        Assertions.assertEquals(6, com.google.gson.JsonParser.parseString(java.nio.file.Files.readString(artifact.toPath())).getAsJsonObject().get("n").getAsLong());

        // window: P14D → two blocks
        final TestPipeline windowed = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        final Map<String, MCollection> windowedOut = MPipeline.apply(windowed, Config.load(SOURCE_CONFIG + svdForwardConfig("target/feature-artifacts/" + java.util.UUID.randomUUID(), ", window: P14D")));
        PAssert.that(windowedOut.get("features").getCollection()).satisfies(rows -> {
            final Map<String, MElement> byKey = new HashMap<>();
            for (final MElement row : rows) byKey.put(row.getAsString("session_id") + "/" + row.getAsString("seller_id"), row);
            assertScores(byKey.get("B/s1"), svdScores(VEC_B1, VEC_A1, VEC_A2));
            assertScores(byKey.get("C/s1"), null);                                   // (2869, 2871]: block 2870 alone has one vector
            assertScores(byKey.get("D/s1"), svdScores(VEC_D1, VEC_C1, VEC_C2));    // (2871, 2873]: block 2872
            return null;
        });
        windowed.run();

        // minBlocks: 2 → B (one preceding block) reads nothing, C (two) reads the same components as above
        final TestPipeline min = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        final Map<String, MCollection> minOut = MPipeline.apply(min, Config.load(SOURCE_CONFIG + svdForwardConfig("target/feature-artifacts/" + java.util.UUID.randomUUID(), ", minBlocks: 2")));
        PAssert.that(minOut.get("features").getCollection()).satisfies(rows -> {
            final Map<String, MElement> byKey = new HashMap<>();
            for (final MElement row : rows) byKey.put(row.getAsString("session_id") + "/" + row.getAsString("seller_id"), row);
            assertScores(byKey.get("B/s1"), null);
            assertScores(byKey.get("C/s1"), svdScores(VEC_C1, VEC_A1, VEC_A2, VEC_B1));
            return null;
        });
        min.run();
    }

    /** start_price and final_price of the six rows, in the order A/s1, A/s2, B/s1, C/s1, C/s2, D/s1. */
    private static final double[] SMOOTH_X = {100, 50, 200, 80, 60, 120}, SMOOTH_Y = {150, 0, 0, 95, 72, 140};
    private static final String[] SMOOTH_KEYS = {"A/s1", "A/s2", "B/s1", "C/s1", "C/s2", "D/s1"};

    /**
     * smooth of final_price over start_price. A heavy second-difference penalty leaves only what the penalty ignores —
     * a line — so the curve is the least-squares line of the six rows, which the test solves by hand; the residual
     * (target − curve) is an outcome, kept as the intermediate target of an encoding that averages a seller's past
     * residuals. The artifact holds the coefficients.
     */
    @Test
    public void testSmooth() throws java.io.IOException {
        final String dir = "target/feature-artifacts/" + java.util.UUID.randomUUID();
        final String blocks = """
                    - name: price_curve
                      scope: population
                      type: smooth
                      input: start_price
                      target: final_price
                      range: [0, 250]
                      segments: 5
                      penalty: {lambda: 1000000000}
                      outputs: [curve, residual]
                      fit: {artifact: "%s"}
                    - name: resid_enc
                      scope: population
                      type: encoding
                      keySets:
                        - keys: [seller_id]
                      targets:
                        - {field: price_curve_resid, stats: [mean], as: resid}
                """.formatted(dir);
        final String config = FEATURE_CONFIG.replace("      output:\n", blocks.replaceAll("(?m)^", "    ") + "      output:\n");
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(SOURCE_CONFIG + config));
        final MCollection output = outputs.get("features");
        final Schema schema = output.getSchema();
        Assertions.assertNotNull(schema.getField("f_price_curve"), schema::toString);
        Assertions.assertEquals("staticSafe", schema.getField("f_price_curve").getOptions().get("feature.status"));
        Assertions.assertTrue(schema.getField("f_price_curve").getOptions().get("feature.derivedFrom").contains("outcome"));
        Assertions.assertNull(schema.getField("f_price_curve_resid"), "the residual is an outcome: an intermediate, not an output");
        final String encoded = schema.getFields().stream().map(Schema.Field::getName).filter(n -> n.startsWith("f_resid_enc") && n.endsWith("mean")).findFirst().orElseThrow();

        double sx = 0, sy = 0, sxx = 0, sxy = 0;
        for (int i = 0; i < 6; i++) {
            sx += SMOOTH_X[i];
            sy += SMOOTH_Y[i];
            sxx += SMOOTH_X[i] * SMOOTH_X[i];
            sxy += SMOOTH_X[i] * SMOOTH_Y[i];
        }
        final double slope = (sxy - sx * sy / 6) / (sxx - sx * sx / 6), intercept = sy / 6 - slope * sx / 6;
        final double[] residual = new double[6];
        for (int i = 0; i < 6; i++) residual[i] = SMOOTH_Y[i] - intercept - slope * SMOOTH_X[i];
        PAssert.that(output.getCollection()).satisfies(rows -> {
            final Map<String, MElement> byKey = new HashMap<>();
            for (final MElement row : rows) byKey.put(row.getAsString("session_id") + "/" + row.getAsString("seller_id"), row);
            Assertions.assertEquals(6, byKey.size());
            for (int i = 0; i < 6; i++) {
                Assertions.assertEquals(intercept + slope * SMOOTH_X[i], byKey.get(SMOOTH_KEYS[i]).getAsDouble("f_price_curve"), 1e-3, SMOOTH_KEYS[i]);
            }
            // s1's past residuals, once their outcomes are known (settlement + ingestion < the gaps between C, D and the rows before)
            Assertions.assertNull(byKey.get("A/s1").getPrimitiveValue(encoded));
            Assertions.assertEquals((residual[0] + residual[2]) / 2, byKey.get("C/s1").getAsDouble(encoded), 1e-3);
            Assertions.assertEquals((residual[0] + residual[2] + residual[3]) / 3, byKey.get("D/s1").getAsDouble(encoded), 1e-3);
            Assertions.assertEquals(residual[1], byKey.get("C/s2").getAsDouble(encoded), 1e-3);
            return null;
        });
        pipeline.run();
        final java.io.File[] dirs = new java.io.File(dir).listFiles();
        Assertions.assertNotNull(dirs);
        final java.io.File artifact = new java.io.File(dirs[0], "price_curve.smooth.json");
        Assertions.assertTrue(artifact.exists());
        final com.google.gson.JsonObject json = com.google.gson.JsonParser.parseString(java.nio.file.Files.readString(artifact.toPath())).getAsJsonObject();
        Assertions.assertEquals(6, json.get("n").getAsLong());
        Assertions.assertEquals(8, json.getAsJsonArray("coefficients").size());
        Assertions.assertFalse(json.get("estimated").getAsBoolean());
        Assertions.assertEquals(2.0, json.get("edf").getAsDouble(), 1e-3);
    }

    /**
     * The same curve under {@code fit.mode: forward} with a heavy FIRST-difference penalty — what is left is a
     * constant, the mean target of the readable rows. final_price is known PT30M + P6D after its event, so B (Jan 3)
     * cannot read A's block yet, C (Jan 20) reads A and B, and D (Feb 1) reads everything before it; a REML-chosen
     * strength runs through the same wiring.
     */
    @Test
    public void testSmoothForwardFit() throws java.io.IOException {
        final String blocks = """
                    - name: price_curve
                      scope: population
                      type: smooth
                      input: start_price
                      target: final_price
                      range: [0, 250]
                      segments: 5
                      penalty: {order: 1, lambda: 1000000000}
                      fit: {mode: forward, blocks: {size: P7D}}
                    - name: price_reml
                      scope: population
                      type: smooth
                      input: start_price
                      target: final_price
                      range: [0, 250]
                      segments: 5
                      fit: {mode: forward, blocks: {size: P7D}}
                """;
        final String config = FEATURE_CONFIG.replace("      output:\n", blocks.replaceAll("(?m)^", "    ") + "      output:\n");
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(SOURCE_CONFIG + config));
        Assertions.assertEquals("forward", outputs.get("features").getSchema().getField("f_price_curve").getOptions().get("feature.coord.fit"));
        PAssert.that(outputs.get("features").getCollection()).satisfies(rows -> {
            final Map<String, MElement> byKey = new HashMap<>();
            for (final MElement row : rows) byKey.put(row.getAsString("session_id") + "/" + row.getAsString("seller_id"), row);
            Assertions.assertEquals(6, byKey.size());
            for (final String key : List.of("A/s1", "A/s2", "B/s1")) {
                Assertions.assertNull(byKey.get(key).getPrimitiveValue("f_price_curve"), key);
                Assertions.assertNull(byKey.get(key).getPrimitiveValue("f_price_reml"), key);
            }
            Assertions.assertEquals(50.0, byKey.get("C/s1").getAsDouble("f_price_curve"), 1e-3);
            Assertions.assertEquals(50.0, byKey.get("C/s2").getAsDouble("f_price_curve"), 1e-3);
            Assertions.assertEquals(63.4, byKey.get("D/s1").getAsDouble("f_price_curve"), 1e-3);
            // three rows behind C, five behind D: a curve exists and stays within the targets it was fitted on
            for (final String key : List.of("C/s1", "C/s2", "D/s1")) {
                final double v = byKey.get(key).getAsDouble("f_price_reml");
                Assertions.assertTrue(v > -100 && v < 250, key + ": " + v);
            }
            return null;
        });
        pipeline.run();
    }

    /**
     * transitionStats is a desugaring, so it must read exactly what the blocks it stands for read: a lag of the
     * entity plus an expanding, shrunk distribution keyed on it. Sellers' grades in time order — s1: good, good, fair,
     * good; s2: fair, good — give the pooled transitions good→good (B), good→fair (C/s1), fair→good (C/s2, D).
     * D (previous = fair) has seen one fair→good; its parent is every earlier row but that one (good 2, fair 2), so
     * P(good) = (1 + 2 · 0.5) / (1 + 2). B (previous = good) has seen no transition yet and reads the marginal.
     */
    @Test
    public void testTransitionStats() throws java.io.IOException {
        final String blocks = """
                    - name: grade_next
                      scope: population
                      type: transitionStats
                      sequenceOf: {entity: seller, field: condition_grade}
                      emit: [{toValueProb: good}]
                      blend: {perEntity: false, priorWeight: 2}
                    - name: grade_own
                      scope: population
                      type: transitionStats
                      sequenceOf: {entity: seller, field: condition_grade}
                      emit: [{toValueProb: good}]
                      blend: {priorWeight: 2}
                    - name: prev
                      scope: sequence
                      entity: seller
                      ops: [{type: lag, fields: [condition_grade], k: 1}]
                    - name: manual
                      scope: population
                      type: encoding
                      naming: "{block}_{target}"
                      keySets:
                        - keys: [prev_all_condition_grade_lag1]
                          hierarchy: [[]]
                      targets:
                        - {field: condition_grade, stats: [distribution], values: [good], as: to}
                      shrinkage: {priorWeight: 2}
                """;
        final String config = FEATURE_CONFIG.replace("      output:\n", blocks.replaceAll("(?m)^", "    ") + "      output:\n");
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(SOURCE_CONFIG + config));
        final Schema schema = outputs.get("features").getSchema();
        Assertions.assertNotNull(schema.getField("f_grade_next_to_good"), schema::toString);
        Assertions.assertNotNull(schema.getField("f_manual_to_good"), schema::toString);
        Assertions.assertNull(schema.getField("f_grade_next_all_prev_lag1"), "the state is an intermediate");
        PAssert.that(outputs.get("features").getCollection()).satisfies(rows -> {
            final Map<String, MElement> byKey = new HashMap<>();
            for (final MElement row : rows) byKey.put(row.getAsString("session_id") + "/" + row.getAsString("seller_id"), row);
            Assertions.assertEquals(6, byKey.size());
            for (final MElement row : byKey.values()) {
                Assertions.assertEquals(row.getPrimitiveValue("f_manual_to_good"), row.getPrimitiveValue("f_grade_next_to_good"), row::toString);
            }
            Assertions.assertNull(byKey.get("A/s1").getPrimitiveValue("f_grade_next_to_good"));
            Assertions.assertEquals(0.5, byKey.get("B/s1").getAsDouble("f_grade_next_to_good"), 1e-9);
            Assertions.assertEquals(2.0 / 3, byKey.get("D/s1").getAsDouble("f_grade_next_to_good"), 1e-9);
            // per entity: D's own (s1, fair) cell is empty, so it reads the pooled fair level — which now shrinks toward
            // the whole marginal (good 3 of 5), since leave-node-out subtracts the LEAF's rows and the leaf has none:
            // (1 + 2 · 0.6) / (1 + 2). s2's first transition (C/s2, previous = fair) finds the pooled cell empty too —
            // C/s1 shares its timestamp — and reads the marginal of the three rows before it (good, fair, good)
            Assertions.assertEquals(11.0 / 15, byKey.get("D/s1").getAsDouble("f_grade_own_to_good"), 1e-9);
            Assertions.assertEquals(2.0 / 3, byKey.get("C/s2").getAsDouble("f_grade_next_to_good"), 1e-9);
            Assertions.assertEquals(2.0 / 3, byKey.get("C/s2").getAsDouble("f_grade_own_to_good"), 1e-9);
            return null;
        });
        pipeline.run();
    }

    /**
     * spectralEmbedding with a window of one step. The pairs are (good, good) once and (good, fair) three times, so
     * C = [[2, 3], [3, 0]] over (good, fair), the only positive PMI is ln(3 · 8 / 15) = ln 1.6 off the diagonal, and
     * the eigenvalues are ± ln 1.6: every coordinate has the magnitude sqrt(ln 1.6 / 2), both positive for the value
     * with the larger mass (good), of opposite signs for fair. Under forward D reads B's and C's blocks only
     * ((good, good) once, (good, fair) twice → ln 1.5), and C's rows read a block with a single value: nothing.
     */
    @Test
    public void testSpectralEmbedding() throws java.io.IOException {
        final String dir = "target/feature-artifacts/" + java.util.UUID.randomUUID();
        final String blocks = """
                    - name: grade_embed
                      scope: population
                      type: spectralEmbedding
                      sequenceOf: {entity: seller, field: condition_grade}
                      cooccur: {window: 1}
                      rank: 2
                      fit: {artifact: "%s"}
                    - name: grade_walk
                      scope: population
                      type: spectralEmbedding
                      sequenceOf: {entity: seller, field: condition_grade}
                      cooccur: {window: 1}
                      rank: 2
                      fit: {mode: forward, blocks: {size: P7D}}
                """.formatted(dir);
        final String config = FEATURE_CONFIG.replace("      output:\n", blocks.replaceAll("(?m)^", "    ") + "      output:\n");
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(SOURCE_CONFIG + config));
        Assertions.assertNull(outputs.get("features").getSchema().getField("f_grade_embed_2"));
        final double m = Math.sqrt(Math.log(1.6) / 2), walk = Math.sqrt(Math.log(1.5) / 2);
        PAssert.that(outputs.get("features").getCollection()).satisfies(rows -> {
            int good = 0;
            for (final MElement row : rows) {
                final double e0 = row.getAsDouble("f_grade_embed_0"), e1 = row.getAsDouble("f_grade_embed_1");
                if ("good".equals(row.getAsString("condition_grade"))) {
                    good++;
                    Assertions.assertEquals(m, e0, 1e-9, row::toString);
                    Assertions.assertEquals(m, e1, 1e-9, row::toString);
                } else {
                    Assertions.assertEquals(m, Math.abs(e0), 1e-9, row::toString);
                    Assertions.assertEquals(0, e0 + e1, 1e-9, row::toString);
                }
                final String key = row.getAsString("session_id") + "/" + row.getAsString("seller_id");
                if ("D/s1".equals(key)) {
                    Assertions.assertEquals(walk, row.getAsDouble("f_grade_walk_0"), 1e-9);
                    Assertions.assertEquals(walk, row.getAsDouble("f_grade_walk_1"), 1e-9);
                } else {
                    Assertions.assertNull(row.getPrimitiveValue("f_grade_walk_0"), row::toString);
                }
            }
            Assertions.assertEquals(4, good);
            return null;
        });
        pipeline.run();
        final java.io.File[] dirs = new java.io.File(dir).listFiles();
        Assertions.assertNotNull(dirs);
        final java.io.File artifact = new java.io.File(dirs[0], "grade_embed.spectral.json");
        Assertions.assertTrue(artifact.exists());
        final com.google.gson.JsonObject json = com.google.gson.JsonParser.parseString(java.nio.file.Files.readString(artifact.toPath())).getAsJsonObject();
        Assertions.assertEquals(4, json.get("pairs").getAsLong());
        Assertions.assertEquals("good", json.getAsJsonArray("values").get(0).getAsJsonObject().get("value").getAsString());
        Assertions.assertEquals(2, json.getAsJsonArray("values").size());
    }

    /**
     * {@code fit.window} on an encoding's forward fit bounds the blocks a keySet without {@code maxAge} reads: with two
     * weekly blocks C (Jan 20) reads blocks 2870–2871 for its row count (B only) and 2869–2870 for the lagged outcome
     * (A, B); D (Feb 1) reads 2872–2873 (C) and 2871–2872 (C).
     */
    @Test
    public void testForwardFitWindow() throws java.io.IOException {
        final String dir = "target/feature-artifacts/" + java.util.UUID.randomUUID();
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(SOURCE_CONFIG + forwardConfig(dir, ", window: P14D")));
        PAssert.that(outputs.get("features").getCollection()).satisfies(rows -> {
            final Map<String, MElement> byKey = new HashMap<>();
            for (final MElement row : rows) byKey.put(row.getAsString("session_id") + "/" + row.getAsString("seller_id"), row);
            Assertions.assertEquals(1L, ((Number) byKey.get("C/s1").getPrimitiveValue("f_enc__seller_id__count")).longValue());
            Assertions.assertEquals(0.5, byKey.get("C/s1").getAsDouble("f_enc__seller_id__e2__mean"), 1e-9);
            Assertions.assertEquals(1L, ((Number) byKey.get("D/s1").getPrimitiveValue("f_enc__seller_id__count")).longValue());
            Assertions.assertEquals(1.0, byKey.get("D/s1").getAsDouble("f_enc__seller_id__e2__mean"), 1e-9);
            return null;
        });
        pipeline.run();
    }

    /** A map-valued column as String → Double (the coder round trip yields CharSequence keys). */
    private static Map<String, Double> distribution(final MElement row, final String column) {
        final Map<?, ?> value = (Map<?, ?>) row.getPrimitiveValue(column);
        Assertions.assertNotNull(value, row::toString);
        final Map<String, Double> out = new HashMap<>();
        for (final Map.Entry<?, ?> e : value.entrySet()) out.put(e.getKey().toString(), ((Number) e.getValue()).doubleValue());
        return out;
    }

    /**
     * targets[].values: the shrunk distribution is emitted as one FLOAT64 column per listed category (the values of
     * {@link #testDistributionShrinkage}'s map), the map column itself is not emitted; the unshrunk distribution
     * (no shrinkage block) expands the same way, a listed category with no mass reading 0.
     */
    @Test
    public void testDistributionValues() throws java.io.IOException {
        final String config = FEATURE_CONFIG
                .replace("- {stats: [count]}\n", "")
                .replace("- {expr: \"sold >= 1\", stats: [mean]}",
                        "- {field: condition_grade, stats: [distribution], values: [good, fair]}\n          shrinkage: {priorWeight: 1, output: [composed, effectiveN]}");
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(SOURCE_CONFIG + config));
        final Schema schema = outputs.get("features").getSchema();
        final String column = "f_enc__seller_id__condition_grade__distribution";
        Assertions.assertNull(schema.getField(column), "the map column is replaced by the per-value columns");
        Assertions.assertEquals(Schema.Type.float64, schema.getField(column + "_good").getFieldType().getType());
        Assertions.assertEquals("mapValue", schema.getField(column + "_good").getOptions().get("feature.operator"));
        Assertions.assertNotNull(schema.getField(column + "__neff"));
        PAssert.that(outputs.get("features").getCollection()).satisfies(rows -> {
            final Map<String, MElement> byKey = new HashMap<>();
            for (final MElement row : rows) byKey.put(row.getAsString("session_id") + "/" + row.getAsString("seller_id"), row);
            Assertions.assertNull(byKey.get("A/s1").getPrimitiveValue(column + "_good"));
            Assertions.assertEquals(0.5, byKey.get("B/s1").getAsDouble(column + "_good"), 1e-9);
            Assertions.assertEquals(0.5, byKey.get("B/s1").getAsDouble(column + "_fair"), 1e-9);
            Assertions.assertEquals(2.0 / 3.0, byKey.get("C/s1").getAsDouble(column + "_good"), 1e-9);
            Assertions.assertEquals(1.0 / 3.0, byKey.get("C/s1").getAsDouble(column + "_fair"), 1e-9);
            Assertions.assertEquals(2.0, byKey.get("B/s1").getAsDouble(column + "__neff"), 1e-9);
            return null;
        });
        pipeline.run();

        // unshrunk: the raw per-key shares; C/s1's own history is {good, good} → good 1, fair 0 (listed, no mass)
        final String raw = FEATURE_CONFIG
                .replace("- {stats: [count]}\n", "")
                .replace("- {expr: \"sold >= 1\", stats: [mean]}", "- {field: condition_grade, stats: [distribution], values: [good, fair]}");
        final TestPipeline second = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        final Map<String, MCollection> rawOutputs = MPipeline.apply(second, Config.load(SOURCE_CONFIG + raw));
        Assertions.assertNull(rawOutputs.get("features").getSchema().getField(column));
        PAssert.that(rawOutputs.get("features").getCollection()).satisfies(rows -> {
            final Map<String, MElement> byKey = new HashMap<>();
            for (final MElement row : rows) byKey.put(row.getAsString("session_id") + "/" + row.getAsString("seller_id"), row);
            Assertions.assertNull(byKey.get("A/s1").getPrimitiveValue(column + "_good"));
            Assertions.assertEquals(1.0, byKey.get("C/s1").getAsDouble(column + "_good"), 1e-9);
            Assertions.assertEquals(0.0, byKey.get("C/s1").getAsDouble(column + "_fair"), 1e-9);
            return null;
        });
        second.run();
    }

    /**
     * Dirichlet-Multinomial: the seller's condition_grade distribution shrunk toward the (leave-node-out) global one
     * with λ = 1. condition_grade is a pre-event attribute, so the strictly-past rows have no near-edge shift.
     */
    @Test
    public void testDistributionShrinkage() throws java.io.IOException {
        final String config = FEATURE_CONFIG
                .replace("- {stats: [count]}\n", "")
                .replace("- {expr: \"sold >= 1\", stats: [mean]}",
                        "- {field: condition_grade, stats: [distribution]}\n          shrinkage: {priorWeight: 1, output: [composed, effectiveN]}");
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(SOURCE_CONFIG + config));
        final Schema schema = outputs.get("features").getSchema();
        Assertions.assertEquals(Schema.Type.map, schema.getField("f_enc__seller_id__condition_grade__distribution").getFieldType().getType());
        Assertions.assertEquals("dirichletMultinomial", schema.getField("f_enc__seller_id__condition_grade__distribution").getOptions().get("feature.coord.family"));
        Assertions.assertNull(schema.getField("enc__seller_id__condition_grade__dist"));
        PAssert.that(outputs.get("features").getCollection()).satisfies(rows -> {
            final Map<String, MElement> byKey = new HashMap<>();
            for (final MElement row : rows) byKey.put(row.getAsString("session_id") + "/" + row.getAsString("seller_id"), row);
            final String column = "f_enc__seller_id__condition_grade__distribution";
            // Jan 1: nothing strictly past
            Assertions.assertNull(byKey.get("A/s1").getPrimitiveValue(column));
            // Jan 3, s1: own {good} (n=1); global minus own = {fair} (n=1) → w = 1/2 → {good: 1/2, fair: 1/2}, n_eff = 2
            Map<String, Double> d = distribution(byKey.get("B/s1"), column);
            Assertions.assertEquals(0.5, d.get("good"), 1e-9);
            Assertions.assertEquals(0.5, d.get("fair"), 1e-9);
            Assertions.assertEquals(2.0, byKey.get("B/s1").getAsDouble(column + "__neff"), 1e-9);
            // Jan 20, s1: own {good, good} (n=2); global minus own = {fair} → w = 2/3 → {good: 2/3, fair: 1/3}
            d = distribution(byKey.get("C/s1"), column);
            Assertions.assertEquals(2.0 / 3.0, d.get("good"), 1e-9);
            Assertions.assertEquals(1.0 / 3.0, d.get("fair"), 1e-9);
            // Jan 20, s2: own {fair} (n=1); global minus own = {good, good} → {good: 1/2, fair: 1/2}
            d = distribution(byKey.get("C/s2"), column);
            Assertions.assertEquals(0.5, d.get("good"), 1e-9);
            Assertions.assertEquals(0.5, d.get("fair"), 1e-9);
            // Feb 1, s1: own {good, good, fair} (n=3); global minus own = {fair, good} → w = 3/4 → {good: 5/8, fair: 3/8}
            d = distribution(byKey.get("D/s1"), column);
            Assertions.assertEquals(0.625, d.get("good"), 1e-9);
            Assertions.assertEquals(0.375, d.get("fair"), 1e-9);
            return null;
        });
        pipeline.run();
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

    /** Rows collected by {@link #testForwardFitVarianceComponents} (the assertion needs the manifest written by the same run). */
    private static final Map<String, Map<String, Object>> FORWARD_VC_ROWS = new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * fit.mode forward with variance-components weights: the λ of the row's usable block comes from the per-level
     * Combine ({@code lambdasByBlockView}) — the same values the artifact manifest records — so
     * {@code effectiveN = n + λ(block)} (or {@code n + priorWeight} where the block has no estimate).
     */
    @Test
    public void testForwardFitVarianceComponents() throws java.io.IOException {
        final String dir = "target/feature-artifacts/" + java.util.UUID.randomUUID();
        final String config = forwardConfig(dir, "").replace("- {expr: \"sold >= 1\", stats: [mean]}",
                "- {expr: \"sold >= 1\", stats: [mean]}\n          shrinkage: {weights: varianceComponents, priorWeight: 1, output: [composed, effectiveN]}");
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(SOURCE_CONFIG + config));
        FORWARD_VC_ROWS.clear();
        PAssert.that(outputs.get("features").getCollection()).satisfies(rows -> {
            for (final MElement row : rows) FORWARD_VC_ROWS.put(row.getAsString("session_id") + "/" + row.getAsString("seller_id"), row.asPrimitiveMap());
            return null;
        });
        pipeline.run();
        Assertions.assertEquals(6, FORWARD_VC_ROWS.size());
        final java.io.File[] files = new java.io.File(dir).listFiles();
        Assertions.assertNotNull(files, "artifact directory missing: " + dir);
        final com.google.gson.JsonObject manifest = com.google.gson.JsonParser.parseString(
                java.nio.file.Files.readString(new java.io.File(files[0], "enc.manifest.json").toPath())).getAsJsonObject();
        final com.google.gson.JsonObject byBlock = manifest.getAsJsonObject("lambdasByBlock").getAsJsonObject("enc__seller_id__e2__n");
        Assertions.assertNotNull(byBlock, manifest::toString);
        // D (Feb 1) reads the sold level up to block 2872 (A, B, C of s1: n = 3) with the λ in force at that block
        final java.util.TreeMap<Long, Double> lambdas = new java.util.TreeMap<>();
        for (final Map.Entry<String, com.google.gson.JsonElement> e : byBlock.entrySet()) lambdas.put(Long.parseLong(e.getKey()), e.getValue().getAsDouble());
        final Map.Entry<Long, Double> inForce = lambdas.floorEntry(2872L);
        Assertions.assertNotNull(inForce, lambdas::toString);
        // the seller means at that block are closer than the within-seller noise: τ² truncates to 0, λ = ∞ (full
        // shrinkage); priorWeight 1 would have given (2 + 0.5) / 4 instead of the leave-node-out parent mean
        Assertions.assertTrue(Double.isInfinite(inForce.getValue()), lambdas::toString);
        final Map<String, Object> d = FORWARD_VC_ROWS.get("D/s1");
        Assertions.assertEquals(0.5, ((Number) d.get("f_enc__seller_id__e2__mean")).doubleValue(), 1e-9, d::toString);
        // effectiveN under full shrinkage = own n (3) + the parent's effective n (s2 at A and C: 2)
        Assertions.assertEquals(5.0, ((Number) d.get("f_enc__seller_id__e2__mean__neff")).doubleValue(), 1e-9, d::toString);
        // A reads nothing (no usable block): no estimate at all
        Assertions.assertNull(FORWARD_VC_ROWS.get("A/s1").get("f_enc__seller_id__e2__mean"));
    }

    private static final Map<String, Map<String, Object>> MIXED_FIT_ROWS = new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * A forward block and a time-fold block with variance-components weights in one fit stage: both read the one
     * series side input, the forward levels keep their per-block λ (the values of
     * {@link #testForwardFitVarianceComponents}) and the time-fold levels get the whole-input λ from their totals
     * ({@code _TimeFoldVc}), without entering the per-block λ Combine (the series are split by {@code _ForwardOnly}).
     */
    @Test
    public void testForwardAndTimeFoldInOneStage() throws java.io.IOException {
        final String dir = "target/feature-artifacts/" + java.util.UUID.randomUUID();
        final String vc = "- {expr: \"sold >= 1\", stats: [mean]}\n          shrinkage: {weights: varianceComponents, priorWeight: 1, output: [composed, effectiveN]}";
        final String timeFold = """
                    - name: enc_tf
                      scope: population
                      type: encoding
                      keySets:
                        - keys: [seller_id]
                      targets:
                        - {stats: [count]}
                        - {expr: "sold >= 1", stats: [mean]}
                      shrinkage: {weights: varianceComponents, priorWeight: 1, output: [composed, effectiveN]}
                      fit: {mode: fold, blocks: {size: P7D}, fold: {by: time, purge: P7D, embargo: P7D}}
            """;
        final String config = forwardConfig(dir, "").replace("- {expr: \"sold >= 1\", stats: [mean]}", vc)
                .replace("        - name: enc\n", timeFold + "        - name: enc\n");
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(SOURCE_CONFIG + config));
        final Set<String> names = transformNames();
        Assertions.assertTrue(hasTransform(names, "features", "_TimeFoldVc"), names::toString);
        Assertions.assertTrue(hasTransform(names, "features", "_ForwardOnly"), names::toString);
        Assertions.assertTrue(hasTransform(names, "features", "_ForwardVc"), names::toString);
        MIXED_FIT_ROWS.clear();
        PAssert.that(outputs.get("features").getCollection()).satisfies(rows -> {
            for (final MElement row : rows) MIXED_FIT_ROWS.put(row.getAsString("session_id") + "/" + row.getAsString("seller_id"), row.asPrimitiveMap());
            return null;
        });
        pipeline.run();
        Assertions.assertEquals(6, MIXED_FIT_ROWS.size());
        // the forward block: unchanged by the time fold sharing its stage
        final Map<String, Object> d = MIXED_FIT_ROWS.get("D/s1");
        Assertions.assertEquals(0.5, ((Number) d.get("f_enc__seller_id__e2__mean")).doubleValue(), 1e-9, d::toString);
        Assertions.assertEquals(5.0, ((Number) d.get("f_enc__seller_id__e2__mean__neff")).doubleValue(), 1e-9, d::toString);
        // the time-fold block: the counts of testTimeFoldFit ([b − 1, b + 2] left out) and a λ-weighted estimate
        final Map<String, Long> counts = Map.of("A/s1", 2L, "B/s1", 1L, "C/s1", 2L, "D/s1", 3L, "A/s2", 1L, "C/s2", 1L);
        for (final Map.Entry<String, Long> e : counts.entrySet()) {
            final Map<String, Object> row = MIXED_FIT_ROWS.get(e.getKey());
            Assertions.assertEquals(e.getValue(), ((Number) row.get("f_enc_tf__seller_id__count")).longValue(), e.getKey());
            Assertions.assertNotNull(row.get("f_enc_tf__seller_id__e2__mean__neff"), row::toString);
        }
        // D's time fold reads A, B and C — the rows its forward read sees — and the whole input's λ (s1: 3 of 4 sold,
        // s2: 1 of 2) is ∞ as well: the same full-shrinkage values. A missing λ would fall back to priorWeight 1
        // ((2 + 0.5) / 4 = 0.625), so these pin the _TimeFoldVc side input reaching the evaluator
        Assertions.assertEquals(0.5, ((Number) d.get("f_enc_tf__seller_id__e2__mean")).doubleValue(), 1e-9, d::toString);
        Assertions.assertEquals(5.0, ((Number) d.get("f_enc_tf__seller_id__e2__mean__neff")).doubleValue(), 1e-9, d::toString);
        // the time-fold artifact (the top-level artifact uri is inherited) is written like a hash fold's: the totals and
        // their whole-input λ, no per-block λ; the forward manifest records its own levels' per-block λ only
        final java.io.File[] files = new java.io.File(dir).listFiles();
        Assertions.assertNotNull(files, "artifact directory missing: " + dir);
        final com.google.gson.JsonObject timeFoldManifest = com.google.gson.JsonParser.parseString(
                java.nio.file.Files.readString(new java.io.File(files[0], "enc_tf.manifest.json").toPath())).getAsJsonObject();
        Assertions.assertFalse(timeFoldManifest.has("lambdasByBlock"), timeFoldManifest::toString);
        Assertions.assertEquals("Infinity", timeFoldManifest.getAsJsonObject("lambdas").get("enc_tf__seller_id__e2__n").getAsString(), timeFoldManifest::toString);
        final com.google.gson.JsonObject forwardManifest = com.google.gson.JsonParser.parseString(
                java.nio.file.Files.readString(new java.io.File(files[0], "enc.manifest.json").toPath())).getAsJsonObject();
        for (final String level : forwardManifest.getAsJsonObject("lambdasByBlock").keySet()) {
            Assertions.assertTrue(level.startsWith("enc__"), forwardManifest::toString);
        }
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

    private static final String FUTURE_BLOCKS = """
                - name: next
                  scope: sequence
                  entity: seller
                  direction: future
                  windows: [{maxAge: P20D}]
                  ops:
                    - {type: aggregate, field: final_price, funcs: [count, first, last]}
                    - {type: lag, field: start_price, k: 1}
                    - {type: barrier, field: start_price, up: 0.5, down: -0.3}
                    - {type: sinceEvent, predicate: "sold = 1", unit: [days]}
                - name: ret
                  scope: row
                  expr: "next_20d_start_price_lead1 / start_price - 1"
            """.replaceAll("(?m)^", "    ");

    /**
     * {@code direction: future}: the seller's next 20 days after each listing, replayed latest first in its own
     * keyed stage; the label columns are emitted with the status {@code label}, the declared label over them with
     * the role {@code label} — which a directly-downstream consumer resolves from the schema alone.
     */
    @Test
    public void testFutureLabels() throws java.io.IOException {
        final String config = FEATURE_CONFIG.replace("      output:\n", FUTURE_BLOCKS + "      output:\n")
                .replace("        prefix: f_\n", "        prefix: f_\n        roles: {label: ret}\n");
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(SOURCE_CONFIG + config));
        final MCollection output = outputs.get("features");
        final Map<String, String> options = output.getSchema().getField("f_next_20d_final_price_last").getOptions();
        Assertions.assertNull(options.get("feature.role"));
        Assertions.assertEquals("label", options.get("feature.status"));
        Assertions.assertEquals("label", output.getSchema().getField("f_ret").getOptions().get("feature.role"));
        final com.mercari.solution.util.pipeline.feature.FeatureLineage lineage = com.mercari.solution.util.pipeline.feature.FeatureLineage.fromSchema(output.getSchema());
        Assertions.assertEquals("f_ret", lineage.roles.get("label"));
        Assertions.assertTrue(lineage.labels.containsAll(List.of("f_ret", "f_next_20d_final_price_last")));
        Assertions.assertTrue(transformNames().stream().anyMatch(n -> n.startsWith("features/") && n.contains("_future")));
        PAssert.that(output.getCollection()).satisfies(rows -> {
            int count = 0;
            for (final MElement row : rows) {
                count++;
                final String id = row.getAsString("session_id") + "/" + row.getAsString("seller_id");
                // count, first, last final_price, next start_price, barrier, days until the next sale, ret
                final Object[] expected = switch (id) {
                    case "A/s1" -> new Object[]{2L, 0.0, 95.0, 200.0, 1L, 19.0, 1.0};         // B (day 2) and C (day 19)
                    case "B/s1" -> new Object[]{1L, 95.0, 95.0, 80.0, -1L, 17.0, -0.6};       // C only: 80 / 200 − 1 ≤ −0.3
                    case "C/s1" -> new Object[]{1L, 140.0, 140.0, 120.0, 1L, 12.0, 0.5};      // D: 120 / 80 − 1 = 0.5 ≥ 0.5
                    case "A/s2" -> new Object[]{1L, 72.0, 72.0, 60.0, 0L, 19.0, 0.2};         // 60 / 50 − 1 touches neither
                    case "D/s1", "C/s2" -> new Object[]{0L, null, null, null, null, null, null}; // nothing within 20 days
                    default -> throw new AssertionError("unexpected row " + id);
                };
                final String[] columns = {"f_next_20d_final_price_count", "f_next_20d_final_price_first", "f_next_20d_final_price_last",
                        "f_next_20d_start_price_lead1", "f_next_20d_start_price_barrier", "f_next_20d_until_days", "f_ret"};
                for (int i = 0; i < columns.length; i++) {
                    final Object actual = row.getPrimitiveValue(columns[i]);
                    if (expected[i] == null) {
                        Assertions.assertNull(actual, id + " " + columns[i]);
                    } else if (expected[i] instanceof Double d) {
                        Assertions.assertEquals(d, ((Number) actual).doubleValue(), 1e-9, id + " " + columns[i]);
                    } else {
                        Assertions.assertEquals(expected[i], ((Number) actual).longValue(), id + " " + columns[i]);
                    }
                }
            }
            Assertions.assertEquals(6, count);
            return null;
        });
        pipeline.run();
    }

    /**
     * Rows sharing a timestamp are not in each other's future: the mirrored replay holds them pending until its clock
     * moves, exactly as the past replay does. E is listed at B's time: B and E each see C only, A sees all three.
     */
    @Test
    public void testFutureLabelsSameTimestamp() throws java.io.IOException {
        final String b = "{session_id: B, seller_id: s1, category: electronics, quantity: 1, start_price: 200.0, condition_grade: good, current_bid_t10: 210.0, sold: 0, final_price: 0.0,   session_time: \"2025-01-03T10:00:00Z\"}\n";
        final String e = "{session_id: E, seller_id: s1, category: electronics, quantity: 1, start_price: 160.0, condition_grade: good, current_bid_t10: 170.0, sold: 1, final_price: 110.0, session_time: \"2025-01-03T10:00:00Z\"}\n";
        Assertions.assertTrue(SOURCE_CONFIG.contains(b));
        final String indent = "        - "; // the elements list inside the text block
        final String source = SOURCE_CONFIG.replace(b, b + indent + e);
        final String config = FEATURE_CONFIG.replace("      output:\n", FUTURE_BLOCKS + "      output:\n")
                .replace("        prefix: f_\n", "        prefix: f_\n        roles: {label: ret}\n");
        final MCollection output = MPipeline.apply(pipeline, Config.load(source + config)).get("features");
        PAssert.that(output.getCollection()).satisfies(rows -> {
            final Map<String, MElement> byId = new HashMap<>();
            for (final MElement row : rows) byId.put(row.getAsString("session_id") + "/" + row.getAsString("seller_id"), row);
            Assertions.assertEquals(7, byId.size());
            // count, last final_price, next start_price
            for (final String id : List.of("B/s1", "E/s1")) {
                final MElement row = byId.get(id);
                Assertions.assertEquals(1L, ((Number) row.getPrimitiveValue("f_next_20d_final_price_count")).longValue(), id);
                Assertions.assertEquals(95.0, ((Number) row.getPrimitiveValue("f_next_20d_final_price_last")).doubleValue(), 1e-9, id);
                Assertions.assertEquals(80.0, ((Number) row.getPrimitiveValue("f_next_20d_start_price_lead1")).doubleValue(), 1e-9, id);
            }
            final MElement a = byId.get("A/s1");
            Assertions.assertEquals(3L, ((Number) a.getPrimitiveValue("f_next_20d_final_price_count")).longValue());
            Assertions.assertEquals(95.0, ((Number) a.getPrimitiveValue("f_next_20d_final_price_last")).doubleValue(), 1e-9);
            // E sold at day 2: A's next sale is E's, not C's
            Assertions.assertEquals(2.0, ((Number) a.getPrimitiveValue("f_next_20d_until_days")).doubleValue(), 1e-9);
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testFutureLabelsParallelMatchesLinear() throws java.io.IOException {
        // the future stage is one more keyed branch of wave 1 (same key as the past seller stage, its own replay)
        // (ret reads a label: undeclared, it would be a feature over the future — an availability violation)
        assertParallelMatchesLinear(PARALLEL_CONFIG.replace("      output:\n", FUTURE_BLOCKS + "      output:\n")
                        .replace("        prefix: f_\n", "        prefix: f_\n        roles: {label: ret}\n"), 6,
                List.of("Wave1_FanIn", "_future"), List.of());
    }

    /**
     * {@code fit.fold.by: time}: every 7-day block (counted from the epoch) is a fold; a row reads the whole input minus
     * its own block, one block of purge on both sides of it and one of embargo after the purge — {@code [b − 1, b + 2]}.
     * s1's sessions fall in blocks 2869 (A), 2870 (B), 2872 (C) and 2874 (D), s2's in 2869 (A) and 2872 (C). The input
     * spans 6 blocks (2869..2874): the rows whose 4 left-out blocks all lie inside it (B/s1, C/s1, C/s2) leave out more
     * than half of them and are counted by {@code feature/timeFold_<level>_excludedOverHalf} (per level).
     */
    @Test
    public void testTimeFoldFit() throws java.io.IOException {
        final String config = FEATURE_CONFIG.replace("      output:\n",
                "      fit: {mode: fold, blocks: {size: P7D}, fold: {by: time, purge: P7D, embargo: P7D}}\n      output:\n");
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(SOURCE_CONFIG + config));
        PAssert.that(outputs.get("features").getCollection()).satisfies(rows -> {
            final Map<String, MElement> byKey = new HashMap<>();
            for (final MElement row : rows) byKey.put(row.getAsString("session_id") + "/" + row.getAsString("seller_id"), row);
            Assertions.assertEquals(6, byKey.size());
            // count of the seller's rows outside [b − 1, b + 2], and the share of them that sold
            final Map<String, double[]> expected = Map.of(
                    "A/s1", new double[]{2, 1.0},        // C, D
                    "B/s1", new double[]{1, 1.0},        // D (C lies in the purge after B)
                    "C/s1", new double[]{2, 0.5},        // A, B (D lies in the purge + embargo after C)
                    "D/s1", new double[]{3, 2.0 / 3},    // A, B, C
                    "A/s2", new double[]{1, 1.0},        // C
                    "C/s2", new double[]{1, 0.0});       // A
            for (final Map.Entry<String, double[]> e : expected.entrySet()) {
                final MElement row = byKey.get(e.getKey());
                Assertions.assertEquals((long) e.getValue()[0], ((Number) row.getPrimitiveValue("f_enc__seller_id__count")).longValue(), e.getKey());
                Assertions.assertEquals(e.getValue()[1], row.getAsDouble("f_enc__seller_id__e2__mean"), 1e-9, e.getKey());
            }
            return null;
        });
        final org.apache.beam.sdk.PipelineResult result = pipeline.run();
        result.waitUntilFinish();
        final Map<String, Long> overHalf = new TreeMap<>();
        for (final org.apache.beam.sdk.metrics.MetricResult<Long> counter : result.metrics().queryMetrics(org.apache.beam.sdk.metrics.MetricsFilter.builder()
                .addNameFilter(org.apache.beam.sdk.metrics.MetricNameFilter.inNamespace("feature")).build()).getCounters()) {
            final String name = counter.getName().getName();
            if (name.startsWith("timeFold_") && name.endsWith("_excludedOverHalf")) overHalf.merge(name, counter.getAttempted(), Long::sum);
        }
        // per level (the count level and the target's): B/s1, C/s1, C/s2
        Assertions.assertEquals(Map.of("timeFold_enc__seller_id__n_excludedOverHalf", 3L, "timeFold_enc__seller_id__e2__n_excludedOverHalf", 3L), overHalf);
    }

    /**
     * Uniqueness weights of overlapping labels: a row's 20-day label window overlaps the windows of the seller's rows
     * within 20 days on either side, counted by a past and a future COUNT(1); the weight {@code 1 / (1 + overlaps)} is a
     * post-event column declared as {@code output.roles.weight}. s1 lists on days 0, 2, 19 and 31, s2 on days 0 and 19.
     */
    @Test
    public void testUniquenessWeight() throws java.io.IOException {
        final String blocks = """
                - name: before
                  scope: sequence
                  entity: seller
                  windows: [{maxAge: P20D}]
                  ops:
                    - {type: aggregate, funcs: [count]}
                - name: after
                  scope: sequence
                  entity: seller
                  direction: future
                  windows: [{maxAge: P20D}]
                  ops:
                    - {type: aggregate, funcs: [count]}
                - name: uniqueness
                  scope: row
                  expr: "1 / (1 + before_20d_count + after_20d_count)"
            """.replaceAll("(?m)^", "    ");
        final String config = FEATURE_CONFIG.replace("      output:\n", blocks + "      output:\n")
                .replace("        prefix: f_\n", "        prefix: f_\n        roles: {weight: uniqueness}\n");
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(SOURCE_CONFIG + config));
        final MCollection output = outputs.get("features");
        Assertions.assertEquals("weight", output.getSchema().getField("f_uniqueness").getOptions().get("feature.role"));
        Assertions.assertEquals("label", output.getSchema().getField("f_uniqueness").getOptions().get("feature.status"));
        PAssert.that(output.getCollection()).satisfies(rows -> {
            final Map<String, Double> expected = Map.of(
                    "A/s1", 1.0 / 3,   // after: days 2, 19
                    "B/s1", 1.0 / 3,   // before: day 0; after: day 19
                    "C/s1", 1.0 / 4,   // before: days 0, 2; after: day 31
                    "D/s1", 1.0 / 2,   // before: day 19
                    "A/s2", 1.0 / 2,   // after: day 19
                    "C/s2", 1.0 / 2);  // before: day 0
            int count = 0;
            for (final MElement row : rows) {
                count++;
                final String id = row.getAsString("session_id") + "/" + row.getAsString("seller_id");
                Assertions.assertEquals(expected.get(id), row.getAsDouble("f_uniqueness"), 1e-9, id);
            }
            Assertions.assertEquals(6, count);
            return null;
        });
        pipeline.run();
    }

    /**
     * A calendar clock whose ticks are the four session days (2025-01-01, 01-03, 01-20, 02-01): a window of one tick
     * reaches the previous session day however far back it is, and forward blocks of one tick are those days — the
     * row count level reads the blocks before the row's, the outcome level (known 6 days and 30 minutes after its
     * session) the blocks complete by then.
     */
    @Test
    public void testCalendarClock() throws java.io.IOException {
        final String blocks = """
                - name: sess
                  scope: sequence
                  entity: seller
                  windows: [{maxAge: 1, clock: sessions}]
                  ops:
                    - {type: aggregate, field: start_price, funcs: [count]}
            """.replaceAll("(?m)^", "    ");
        final String config = FEATURE_CONFIG
                .replace("      lineage:\n", "        clocks:\n          - {name: sessions, type: calendar, dates: [2025-01-01, 2025-01-03, 2025-01-20, 2025-02-01]}\n      lineage:\n")
                .replace("      output:\n", blocks + "      fit: {mode: forward, blocks: {size: 1, clock: sessions}}\n      output:\n");
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(SOURCE_CONFIG + config));
        final MCollection output = outputs.get("features");
        Assertions.assertEquals("sessions", output.getSchema().getField("f_sess_1sessions_start_price_count").getOptions().get("feature.coord.windowClock"));
        PAssert.that(output.getCollection()).satisfies(rows -> {
            final Map<String, MElement> byKey = new HashMap<>();
            for (final MElement row : rows) byKey.put(row.getAsString("session_id") + "/" + row.getAsString("seller_id"), row);
            Assertions.assertEquals(6, byKey.size());
            // one tick back: the previous session day of the seller (C/s2's previous session is two ticks back)
            final Map<String, Long> counts = Map.of("A/s1", 0L, "B/s1", 1L, "C/s1", 1L, "D/s1", 1L, "A/s2", 0L, "C/s2", 0L);
            for (final Map.Entry<String, Long> e : counts.entrySet()) {
                Assertions.assertEquals(e.getValue(), ((Number) byKey.get(e.getKey()).getPrimitiveValue("f_sess_1sessions_start_price_count")).longValue(), e.getKey());
            }
            // forward, one block per session day: the row count reads the earlier days
            Assertions.assertEquals(1L, ((Number) byKey.get("B/s1").getPrimitiveValue("f_enc__seller_id__count")).longValue());
            Assertions.assertEquals(2L, ((Number) byKey.get("C/s1").getPrimitiveValue("f_enc__seller_id__count")).longValue());
            Assertions.assertEquals(3L, ((Number) byKey.get("D/s1").getPrimitiveValue("f_enc__seller_id__count")).longValue());
            Assertions.assertEquals(1L, ((Number) byKey.get("C/s2").getPrimitiveValue("f_enc__seller_id__count")).longValue());
            // the outcome is known 6d30m after its session: D (02-01) reads the blocks complete by 01-26 → A, B (sold 1, 0);
            // C (01-20) reads by 01-14 → A only; B (01-03) reads nothing yet
            Assertions.assertEquals(0.5, byKey.get("D/s1").getAsDouble("f_enc__seller_id__e2__mean"), 1e-9);
            Assertions.assertEquals(1.0, byKey.get("C/s1").getAsDouble("f_enc__seller_id__e2__mean"), 1e-9);
            Assertions.assertNull(byKey.get("B/s1").getPrimitiveValue("f_enc__seller_id__e2__mean"));
            return null;
        });
        pipeline.run();
    }

    /**
     * The log-signature of a seller's path through (start_price, quantity) and {@code compress: {svd}} over its
     * components, against hand values (s1 before D: (100, 2) → (200, 1) → (80, 4)) and an explicit svd block over the
     * same components.
     */
    @Test
    public void testLogSignatureCompress() throws java.io.IOException {
        final String blocks = """
                - name: path
                  scope: sequence
                  entity: seller
                  lift: {fields: [start_price, quantity]}
                  summarize:
                    dynamics: {family: bilinear, type: logsignature, depth: 2}
                  compress:
                    svd: {rank: 2}
                    keep: true
                - name: manual
                  scope: population
                  type: svd
                  inputs: [path_all_logsig_a, path_all_logsig_b, path_all_logsig_ab]
                  rank: 2
            """.replaceAll("(?m)^", "    ");
        final String config = FEATURE_CONFIG.replace("      output:\n", blocks + "      output:\n");
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(SOURCE_CONFIG + config));
        final MCollection output = outputs.get("features");
        Assertions.assertEquals("bilinear", output.getSchema().getField("f_path_all_logsig_ab").getOptions().get("feature.coord.family"));
        PAssert.that(output.getCollection()).satisfies(rows -> {
            final Map<String, MElement> byKey = new HashMap<>();
            for (final MElement row : rows) byKey.put(row.getAsString("session_id") + "/" + row.getAsString("seller_id"), row);
            final MElement d = byKey.get("D/s1");
            // increments (100, −1) then (−120, 3): totals (−20, 2), Lévy area ½(100·3 − (−1)(−120)) = 90
            Assertions.assertEquals(-20.0, d.getAsDouble("f_path_all_logsig_a"), 1e-9);
            Assertions.assertEquals(2.0, d.getAsDouble("f_path_all_logsig_b"), 1e-9);
            Assertions.assertEquals(90.0, d.getAsDouble("f_path_all_logsig_ab"), 1e-9);
            // fewer than two points: no path
            Assertions.assertNull(byKey.get("B/s1").getPrimitiveValue("f_path_all_logsig_ab"));
            // compress is the svd block over the components
            for (final MElement row : byKey.values()) {
                for (int k = 0; k < 2; k++) {
                    final Object compressed = row.getPrimitiveValue("f_path_svd_" + k), manual = row.getPrimitiveValue("f_manual_" + k);
                    if (manual == null) Assertions.assertNull(compressed, row::toString);
                    else Assertions.assertEquals(((Number) manual).doubleValue(), ((Number) compressed).doubleValue(), 1e-9, row::toString);
                }
            }
            return null;
        });
        pipeline.run();
    }

    private static final String RATING_BLOCKS = """
                - name: skill
                  scope: sequence
                  entity: seller
                  ops:
                    - {type: rating, field: final_price, context: session, order: descending, tau: 0, as: pl, funcs: [mu, sigma, count]}
                    - {type: rating, field: final_price, context: session, order: descending, method: elo, as: elo, funcs: [mu, count, delta]}
                - name: field
                  scope: context
                  context: session
                  inputs: [skill_all_elo_mu]
                  ops: [gapToBest]
            """.replaceAll("(?m)^", "    ");

    /**
     * The sequence {@code rating} op: sellers are rated by the final price within a session (the higher the better),
     * all sellers in one replay under the global key. The outcome reaches the system 6 days 30 minutes after a session
     * and a row is computed 8 minutes before its own, so session A (s1 150 beats s2 0) is unknown at B (2 days later)
     * and known at C; C (s1 95 beats s2 72) is known at D. B and D are single-seller sessions: no contest. A context
     * block reads the rating like any column (the seller's rating against the best of its session).
     */
    @Test
    public void testSequenceRating() throws java.io.IOException {
        final String config = FEATURE_CONFIG.replace("      output:\n", RATING_BLOCKS + "      output:\n");
        Assertions.assertNotEquals(FEATURE_CONFIG, config);
        final MCollection output = MPipeline.apply(pipeline, Config.load(SOURCE_CONFIG + config)).get("features");
        Assertions.assertEquals(Schema.Type.int64, output.getSchema().getField("f_skill_all_elo_count").getFieldType().getType());
        Assertions.assertEquals("windowShift", output.getSchema().getField("f_skill_all_pl_mu").getOptions().get("feature.status"));
        PAssert.that(output.getCollection()).satisfies(rows -> {
            // the second contest, from the ratings the first one left (the update rules themselves: RatingTest)
            final Rating pl = Rating.of(
                    Rating.Method.plackettLuce, false, null, null, null, 0d, null, null, List.of(), List.of(), "y");
            final Rating.State state = new Rating.State();
            pl.update(state, List.of(new Rating.Entry("s1", 150), new Rating.Entry("s2", 0)));
            pl.update(state, List.of(new Rating.Entry("s1", 95), new Rating.Entry("s2", 72)));
            final double plMuD = (Double) pl.read(state, "s1", "mu"), plSigmaD = (Double) pl.read(state, "s1", "sigma");
            // elo at D: 1516 beat 1484 with the expected score 1 / (1 + 10^(-32 / 400))
            final double eloDeltaD = 32d * (1d - 1d / (1d + Math.pow(10d, -32d / 400d)));
            int count = 0;
            for (final MElement row : rows) {
                count++;
                final String id = row.getAsString("session_id") + "/" + row.getAsString("seller_id");
                // pl mu, pl sigma, pl count, elo mu, elo count, elo delta, elo gap to the session's best
                final Object[] expected = switch (id) {
                    case "A/s1", "A/s2", "B/s1" -> new Object[]{25.0, 25.0 / 3, 0L, 1500.0, 0L, null, 0.0}; // nothing known yet
                    case "C/s1" -> new Object[]{27.63523138347365, 8.065506316323548, 1L, 1516.0, 1L, 16.0, 0.0};
                    case "C/s2" -> new Object[]{22.36476861652635, 8.065506316323548, 1L, 1484.0, 1L, -16.0, -32.0};
                    case "D/s1" -> new Object[]{plMuD, plSigmaD, 2L, 1516.0 + eloDeltaD, 2L, eloDeltaD, 0.0};
                    default -> throw new AssertionError("unexpected row " + id);
                };
                final String[] columns = {"f_skill_all_pl_mu", "f_skill_all_pl_sigma", "f_skill_all_pl_count",
                        "f_skill_all_elo_mu", "f_skill_all_elo_count", "f_skill_all_elo_delta", "f_field_skill_all_elo_mu_gapToBest"};
                for (int i = 0; i < columns.length; i++) {
                    final Object actual = row.getPrimitiveValue(columns[i]);
                    if (expected[i] == null) {
                        Assertions.assertNull(actual, id + " " + columns[i]);
                    } else if (expected[i] instanceof Double d) {
                        Assertions.assertEquals(d, ((Number) actual).doubleValue(), 1e-9, id + " " + columns[i]);
                    } else {
                        Assertions.assertEquals(expected[i], ((Number) actual).longValue(), id + " " + columns[i]);
                    }
                }
            }
            Assertions.assertEquals(6, count);
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testSequenceRatingParallelMatchesLinear() throws java.io.IOException {
        // the rating replay (global key) is one more keyed branch of wave 1; the context block reading it follows
        assertParallelMatchesLinear(PARALLEL_CONFIG.replace("      output:\n", RATING_BLOCKS + "      output:\n"), 6,
                List.of("Wave1_FanIn"), List.of());
    }
}
