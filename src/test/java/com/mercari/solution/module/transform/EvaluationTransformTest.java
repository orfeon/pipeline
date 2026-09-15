package com.mercari.solution.module.transform;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

/**
 * Config-driven e2e tests of the evaluation transform on a synthetic online-auction dataset: sessions of
 * listings where one listing sells. {@code p_model} is the exact conditional probability given {@code f_known}
 * (the baseline); {@code p_true} is the true probability (it knows {@code f_extra} too), {@code p_copy} is the
 * baseline again, and {@code s_extra} is a raw score to be combined with the baseline as an offset.
 */
public class EvaluationTransformTest {

    private static TestPipeline createPipeline() {
        final DirectOptions options = PipelineOptionsFactory.as(DirectOptions.class);
        options.setEnforceImmutability(false);
        return TestPipeline.fromOptions(options).enableAbandonedNodeEnforcement(false);
    }

    private final transient TestPipeline pipeline = createPipeline();

    static final int SESSIONS = 400;
    static final int LISTINGS = 4;

    private static String sessionsConfig(final long seed) {
        final Random random = new Random(seed);
        final StringBuilder sb = new StringBuilder();
        sb.append("sources:\n  - name: listings\n    module: create\n    parameters:\n      type: element\n      elements:\n");
        final long start = Instant.parse("2024-01-01T00:00:00Z").toEpochMilli();
        final int draws = 400;
        for (int s = 0; s < SESSIONS; s++) {
            final double[] known = new double[LISTINGS];
            final double[] extra = new double[LISTINGS];
            final double[] price = new double[LISTINGS];
            final double[] truth = new double[LISTINGS];
            final double[] base = new double[LISTINGS];
            double truthSum = 0;
            for (int i = 0; i < LISTINGS; i++) {
                known[i] = random.nextGaussian();
                extra[i] = random.nextGaussian();
                price[i] = 50 + 100 * random.nextDouble();
                truth[i] = Math.exp(1.5 * known[i] + 1.0 * extra[i]);
                truthSum += truth[i];
            }
            for (int i = 0; i < LISTINGS; i++) truth[i] /= truthSum;
            final double[] draw = new double[LISTINGS];
            for (int m = 0; m < draws; m++) {
                double sum = 0;
                for (int i = 0; i < LISTINGS; i++) {
                    draw[i] = Math.exp(1.5 * known[i] + 1.0 * random.nextGaussian());
                    sum += draw[i];
                }
                for (int i = 0; i < LISTINGS; i++) base[i] += draw[i] / sum / draws;
            }
            int winner = LISTINGS - 1;
            double u = random.nextDouble();
            for (int i = 0; i < LISTINGS; i++) {
                u -= truth[i];
                if (u <= 0) {
                    winner = i;
                    break;
                }
            }
            final Instant time = Instant.ofEpochMilli(start + s * 2L * 86_400_000L);
            final String fold = time.toString().startsWith("2024") ? "valid" : time.toString().startsWith("2025") ? "test" : "later";
            for (int i = 0; i < LISTINGS; i++) {
                sb.append(String.format(Locale.ROOT,
                        "        - {session_id: S%d, listing_id: L%d_%d, f_known: %.6f, f_extra: %.6f, start_price: %.2f, p_model: %.6f, p_true: %.6f, p_copy: %.6f, s_extra: %.6f, sold: %d, payoff: %.4f, track: %s, fold: %s, session_time: \"%s\"}\n",
                        s, s, i, known[i], extra[i], price[i], base[i], truth[i], base[i], extra[i], i == winner ? 1 : 0, i == winner ? 1d / base[i] : 0d, s % 2 == 0 ? "A" : "B", fold, time));
            }
        }
        sb.append("""
                      schema:
                        fields:
                          - {name: session_id, type: string}
                          - {name: listing_id, type: string}
                          - {name: f_known, type: float64}
                          - {name: f_extra, type: float64}
                          - {name: start_price, type: float64}
                          - {name: p_model, type: float64}
                          - {name: p_true, type: float64}
                          - {name: p_copy, type: float64}
                          - {name: s_extra, type: float64}
                          - {name: sold, type: int32}
                          - {name: payoff, type: float64}
                          - {name: track, type: string}
                          - {name: fold, type: string}
                          - {name: session_time, type: timestamp}
                """);
        return sb.toString();
    }

    private static String key(final MElement r) {
        return r.getAsString("split") + "/" + r.getAsString("prediction") + "/" + r.getAsString("pair") + "/" + r.getAsString("slice") + "/" + r.getAsString("value");
    }

    @Test
    public void testGroupedMultinomialAgainstBaseline() throws Exception {
        final String config = sessionsConfig(11) + """
                transforms:
                  - name: eval
                    module: evaluation
                    inputs: [listings]
                    parameters:
                      family: groupedMultinomial
                      group: session_id
                      label: sold
                      baseline: {field: p_model, form: prob}
                      time: session_time
                      predictions:
                        - {name: truth, prob: p_true}
                        - {name: copy, prob: p_copy}
                        - {name: scored, score: s_extra, offset: p_model, offsetScale: prob}
                      splits:
                        valid: {from: "2024-01-01", to: "2024-12-31", role: selection}
                        test:  {from: "2025-01-01", to: "2025-12-31", role: report}
                      bootstrap: {samples: 200, seed: 7}
                      calibration:
                        - {type: reliability, by: prediction, bins: 5}
                        - {type: reliability, by: divergence, bins: 5}
                        - {type: reliability, by: field, field: start_price, edges: [80, 120]}
                        - {type: edge, thresholds: [1.0, 1.5]}
                      slices:
                        - {field: track}
                        - {field: session_time, bucket: quarter}
                      utility: {field: payoff}
                """;
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(config));
        Assertions.assertNotNull(outputs.get("eval"));
        Assertions.assertNotNull(outputs.get("eval.calibration"));
        Assertions.assertNotNull(outputs.get("eval.units"));
        Assertions.assertNotNull(outputs.get("eval.summary"));
        // sessions two days apart from 2024-01-01: 183 in 2024, 183 in 2025 (leap year), 34 in 2026 (unassigned)
        final long validUnits = 183, testUnits = 183;

        PAssert.that(outputs.get("eval").getCollection()).satisfies(rows -> {
            final Map<String, MElement> records = new HashMap<>();
            for (final MElement r : rows) records.put(key(r), r);
            // per split: overall cell = baseline + 3 predictions + 3 pairs = 7; track A / B = 14; two quarters
            // per split (sessions every two days cover the whole year: 4 quarters) = 28 → 49 per split
            Assertions.assertEquals(2 * (7 + 14 + 28), records.size(), records.keySet().toString());
            final MElement truth = records.get("test/truth/null/null/null");
            final MElement copy = records.get("test/copy/null/null/null");
            final MElement baseline = records.get("test/baseline/null/null/null");
            final MElement scored = records.get("test/scored/null/null/null");
            Assertions.assertEquals("report", truth.getAsString("role"));
            Assertions.assertEquals(testUnits, truth.getAsLong("n_units"));
            Assertions.assertEquals(testUnits * LISTINGS, truth.getAsLong("n_rows"));
            // the true model carries the extra information: its excess over the baseline is positive with a CI above 0
            Assertions.assertTrue(truth.getAsDouble("excessLogScore") > 0.05, "excess of truth: " + truth.getAsDouble("excessLogScore"));
            Assertions.assertTrue(truth.getAsDouble("excessLogScore_lo") > 0, "excess lo of truth: " + truth.getAsDouble("excessLogScore_lo"));
            Assertions.assertTrue(truth.getAsDouble("excessLogScore_hi") > truth.getAsDouble("excessLogScore"));
            Assertions.assertEquals(-truth.getAsDouble("logScore"), truth.getAsDouble("logloss"), 1e-12);
            Assertions.assertTrue(truth.getAsDouble("hitAt1") > 0.3);
            Assertions.assertTrue(truth.getAsDouble("brier") < baseline.getAsDouble("brier"));
            // the copy of the baseline has exactly zero excess, with a degenerate interval
            Assertions.assertEquals(0d, copy.getAsDouble("excessLogScore"), 1e-12);
            Assertions.assertEquals(0d, copy.getAsDouble("excessLogScore_lo"), 1e-12);
            Assertions.assertEquals(0d, copy.getAsDouble("excessLogScore_hi"), 1e-12);
            Assertions.assertEquals(baseline.getAsDouble("logScore"), copy.getAsDouble("logScore"), 1e-12);
            Assertions.assertEquals(0d, baseline.getAsDouble("excessLogScore"));
            Assertions.assertNull(baseline.getPrimitiveValue("excessLogScore_lo"));
            // the score set (extra on top of the baseline's log share) is informative too
            Assertions.assertTrue(scored.getAsDouble("excessLogScore") > 0, "excess of scored: " + scored.getAsDouble("excessLogScore"));
            // pair truth − copy = the excess of truth (same units, same weights)
            final MElement pair = records.get("test/truth/copy/null/null");
            Assertions.assertEquals(truth.getAsDouble("excessLogScore"), pair.getAsDouble("excessLogScore"), 1e-12);
            Assertions.assertEquals(truth.getAsDouble("excessLogScore_lo"), pair.getAsDouble("excessLogScore_lo"), 1e-12);
            Assertions.assertEquals(truth.getAsDouble("logScore") - copy.getAsDouble("logScore"), pair.getAsDouble("logScore"), 1e-12);
            // slices: the two tracks partition the units; the quarters carry the bucket labels
            final MElement a = records.get("test/truth/null/track/A");
            final MElement b = records.get("test/truth/null/track/B");
            Assertions.assertEquals(testUnits, a.getAsLong("n_units") + b.getAsLong("n_units"));
            Assertions.assertNotNull(records.get("test/truth/null/session_time/quarter/2025-Q3"));
            Assertions.assertNotNull(records.get("valid/truth/null/session_time/quarter/2024-Q1"));
            Assertions.assertEquals("selection", records.get("valid/truth/null/null/null").getAsString("role"));
            Assertions.assertEquals(validUnits, records.get("valid/truth/null/null/null").getAsLong("n_units"));
            return null;
        });
        PAssert.that(outputs.get("eval.calibration").getCollection()).satisfies(rows -> {
            final List<MElement> list = new ArrayList<>();
            rows.forEach(list::add);
            // 2 splits x 3 predictions x (5 + 5 + 3 + 2) bins
            Assertions.assertEquals(2 * 3 * 15, list.size());
            long n = 0;
            double positives = 0;
            for (final MElement r : list) {
                if (!"test".equals(r.getAsString("split")) || !"truth".equals(r.getAsString("prediction")) || r.getAsLong("table") != 0) continue;
                Assertions.assertEquals("reliability", r.getAsString("type"));
                Assertions.assertEquals("prediction", r.getAsString("by"));
                n += r.getAsLong("n");
                positives += r.getAsDouble("positives");
                Assertions.assertTrue(r.getAsDouble("rate_lo") <= r.getAsDouble("rate") && r.getAsDouble("rate") <= r.getAsDouble("rate_hi"));
                Assertions.assertNotNull(r.getPrimitiveValue("lower"));
                Assertions.assertNotNull(r.getPrimitiveValue("upper"));
                Assertions.assertNotNull(r.getPrimitiveValue("utility"));
                Assertions.assertNotNull(r.getPrimitiveValue("p_baseline"));
            }
            Assertions.assertEquals(testUnits * LISTINGS, n);
            Assertions.assertEquals(testUnits, positives, 1e-9);
            // the field table: three bands with declared bounds
            final MElement band = list.stream().filter(r -> "test".equals(r.getAsString("split")) && r.getAsLong("table") == 2 && r.getAsLong("bin") == 1).findFirst().orElseThrow();
            Assertions.assertEquals(80d, band.getAsDouble("lower"));
            Assertions.assertEquals(120d, band.getAsDouble("upper"));
            Assertions.assertEquals("start_price", band.getAsString("field"));
            // the edge table: fewer rows above 1.5 x baseline than above 1.0 x
            final MElement edge1 = list.stream().filter(r -> "test".equals(r.getAsString("split")) && "truth".equals(r.getAsString("prediction")) && r.getAsLong("table") == 3 && r.getAsLong("bin") == 0).findFirst().orElseThrow();
            final MElement edge2 = list.stream().filter(r -> "test".equals(r.getAsString("split")) && "truth".equals(r.getAsString("prediction")) && r.getAsLong("table") == 3 && r.getAsLong("bin") == 1).findFirst().orElseThrow();
            Assertions.assertEquals("edge", edge1.getAsString("type"));
            Assertions.assertEquals(1.0, edge1.getAsDouble("lower"));
            Assertions.assertTrue(edge1.getAsLong("n") > edge2.getAsLong("n"));
            // where the true model exceeds the baseline, the realised rate follows the model, not the baseline
            Assertions.assertTrue(edge1.getAsDouble("rate") > edge1.getAsDouble("p_baseline"), "edge rate " + edge1.getAsDouble("rate") + " vs baseline " + edge1.getAsDouble("p_baseline"));
            return null;
        });
        PAssert.that(outputs.get("eval.units").getCollection()).satisfies(rows -> {
            final List<MElement> list = new ArrayList<>();
            rows.forEach(list::add);
            Assertions.assertEquals((validUnits + testUnits) * 4, list.size());
            final MElement one = list.stream().filter(r -> "truth".equals(r.getAsString("prediction")) && "test".equals(r.getAsString("split"))).findFirst().orElseThrow();
            Assertions.assertEquals(4L, one.getAsLong("n_rows"));
            Assertions.assertNotNull(one.getPrimitiveValue("time"));
            Assertions.assertEquals(one.getAsDouble("logScore") - one.getAsDouble("logScoreBaseline"), one.getAsDouble("excessLogScore"), 1e-12);
            final List<?> slices = (List<?>) one.getPrimitiveValue("slices");
            Assertions.assertEquals(2, slices.size());
            return null;
        });
        PAssert.that(outputs.get("eval.summary").getCollection()).satisfies(rows -> {
            final List<MElement> list = new ArrayList<>();
            rows.forEach(list::add);
            Assertions.assertEquals(1, list.size());
            final MElement summary = list.get(0);
            Assertions.assertEquals(SESSIONS * LISTINGS, summary.getAsLong("nRows"));
            Assertions.assertEquals(34L * LISTINGS, summary.getAsLong("nRowsUnassigned"));
            Assertions.assertEquals(0L, summary.getAsLong("nRowsInvalid"));
            Assertions.assertEquals(validUnits + testUnits, summary.getAsLong("nUnits"));
            Assertions.assertEquals(0L, summary.getAsLong("nUnitsSkipped"));
            Assertions.assertEquals("session_id", summary.getAsString("group"));
            Assertions.assertEquals("prob", summary.getAsString("baselineForm"));
            Assertions.assertEquals(List.of("truth", "copy", "scored"), summary.getPrimitiveValue("predictions"));
            Assertions.assertEquals(200L, summary.getAsLong("bootstrapSamples"));
            Assertions.assertEquals(4L, summary.getAsLong("nCalibrationTables"));
            final List<?> splits = (List<?>) summary.getPrimitiveValue("splits");
            Assertions.assertEquals(2, splits.size());
            Assertions.assertEquals(List.of(), summary.getPrimitiveValue("notes"));
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testBinomialPriorModeWithColumnSplits() throws Exception {
        final String config = sessionsConfig(5) + """
                transforms:
                  - name: eval
                    module: evaluation
                    inputs: [listings]
                    parameters:
                      family: binomial
                      label: {expr: "sold > 0 ? 1 : 0"}
                      time: session_time
                      rowId: [listing_id]
                      predictions:
                        - {name: truth, prob: p_true}
                      splits: {field: fold, roles: {valid: selection, test: report}}
                      bootstrap: {samples: 100, seed: 1, unit: session_id}
                      calibration:
                        - {by: prediction, bins: 4}
                """;
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(config));
        PAssert.that(outputs.get("eval").getCollection()).satisfies(rows -> {
            final Map<String, MElement> records = new HashMap<>();
            for (final MElement r : rows) records.put(key(r), r);
            Assertions.assertEquals(2 * 2, records.size(), records.keySet().toString());
            final MElement truth = records.get("test/truth/null/null/null");
            Assertions.assertEquals(183L * LISTINGS, truth.getAsLong("n_units"));
            Assertions.assertEquals(183d, truth.getAsDouble("positives"), 1e-9);
            // against the prior (the label mean 1/4) the true probabilities score well above
            Assertions.assertTrue(truth.getAsDouble("excessLogScore") > 0.1, "excess: " + truth.getAsDouble("excessLogScore"));
            Assertions.assertTrue(truth.getAsDouble("excessLogScore_lo") > 0);
            Assertions.assertNull(truth.getPrimitiveValue("hitAt1"));
            final MElement prior = records.get("test/baseline/null/null/null");
            Assertions.assertNull(prior.getPrimitiveValue("logScore"));
            return null;
        });
        PAssert.that(outputs.get("eval.calibration").getCollection()).satisfies(rows -> {
            final List<MElement> list = new ArrayList<>();
            rows.forEach(list::add);
            Assertions.assertEquals(2 * 4, list.size());
            for (final MElement r : list) Assertions.assertNull(r.getPrimitiveValue("p_baseline"));
            return null;
        });
        PAssert.that(outputs.get("eval.summary").getCollection()).satisfies(rows -> {
            final MElement summary = rows.iterator().next();
            Assertions.assertEquals("fold", summary.getAsString("splitField"));
            Assertions.assertEquals(34L * LISTINGS, summary.getAsLong("nRowsUnassigned"));
            Assertions.assertNull(summary.getAsString("baseline"));
            Assertions.assertEquals("session_id", summary.getAsString("bootstrapUnit"));
            final List<?> notes = (List<?>) summary.getPrimitiveValue("notes");
            Assertions.assertTrue(notes.toString().contains("no baseline"), notes.toString());
            Assertions.assertFalse(notes.toString().contains("overlaps"), notes.toString());
            return null;
        });
        pipeline.run();
    }
}
