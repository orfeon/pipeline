package com.mercari.solution.module.transform;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.IllegalModuleException;
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
 * Config-driven e2e tests of the screen transform on a synthetic online-auction dataset: sessions of
 * listings where one listing sells. The baseline model already knows {@code f_known}; {@code f_extra}
 * carries extra information; {@code f_noise} carries none.
 */
public class ScreenTransformTest {

    /**
     * The DirectRunner's immutability enforcement traverses the whole pipeline graph once per bundle; the
     * unrolled conditioning passes make the graph large and the GroupByKey output is one bundle per key, so the
     * check dominates the run time (CPU-bound in ImmutabilityEnforcementFactory). It is disabled here as in the
     * Spanner / Datastore ITs; the engine never mutates its inputs.
     */
    private static TestPipeline createPipeline() {
        final DirectOptions options = PipelineOptionsFactory.as(DirectOptions.class);
        options.setEnforceImmutability(false);
        return TestPipeline.fromOptions(options).enableAbandonedNodeEnforcement(false);
    }

    private final transient TestPipeline pipeline = createPipeline();

    private static String sessionsConfig(final int sessions, final int listings, final long seed) {
        final Random random = new Random(seed);
        final StringBuilder sb = new StringBuilder();
        sb.append("sources:\n  - name: listings\n    module: create\n    timestampAttribute: session_time\n    parameters:\n      type: element\n      elements:\n");
        final long start = Instant.parse("2024-01-01T00:00:00Z").toEpochMilli();
        // true model: winner ~ softmax(1.5 known + 1.0 extra); the baseline knows f_known only, and is the exact
        // conditional probability given f_known (Monte Carlo over the unobserved extra), so f_known carries no
        // residual information while f_extra does; f_noise is independent of everything
        final int draws = 400;
        for (int s = 0; s < sessions; s++) {
            final double[] known = new double[listings];
            final double[] extra = new double[listings];
            final double[] noise = new double[listings];
            final double[] price = new double[listings];
            final double[] score = new double[listings];
            final double[] base = new double[listings];
            double scoreSum = 0;
            for (int i = 0; i < listings; i++) {
                known[i] = random.nextGaussian();
                extra[i] = random.nextGaussian();
                noise[i] = random.nextGaussian();
                price[i] = 50 + 100 * random.nextDouble();
                score[i] = Math.exp(1.5 * known[i] + 1.0 * extra[i]);
                scoreSum += score[i];
            }
            final double[] draw = new double[listings];
            for (int m = 0; m < draws; m++) {
                double sum = 0;
                for (int i = 0; i < listings; i++) {
                    draw[i] = Math.exp(1.5 * known[i] + 1.0 * random.nextGaussian());
                    sum += draw[i];
                }
                for (int i = 0; i < listings; i++) base[i] += draw[i] / sum / draws;
            }
            int winner = listings - 1;
            double u = random.nextDouble() * scoreSum;
            for (int i = 0; i < listings; i++) {
                u -= score[i];
                if (u <= 0) {
                    winner = i;
                    break;
                }
            }
            final String time = Instant.ofEpochMilli(start + s * 2L * 86_400_000L).toString();
            for (int i = 0; i < listings; i++) {
                // continuous and count labels for the gaussian / poisson families: both driven by f_extra only
                // (a separate generator keeps the main random stream, and the other tests' data, unchanged)
                final Random labels = new Random(seed * 1_000_003L + s * 131L + i);
                final double value = 2 * extra[i] + labels.nextGaussian();
                final long bids = Math.round(Math.exp(0.8 * extra[i] + 0.3 * labels.nextGaussian()));
                sb.append(String.format(Locale.ROOT,
                        "        - {session_id: S%d, listing_id: L%d_%d, f_known: %.6f, f_extra: %.6f, f_noise: %.6f, start_price: %.2f, p_model: %.6f, sold: %d, v_price: %.6f, n_bids: %d, session_time: \"%s\"}\n",
                        s, s, i, known[i], extra[i], noise[i], price[i], base[i], i == winner ? 1 : 0, value, bids, time));
            }
        }
        sb.append("""
                      schema:
                        fields:
                          - {name: session_id, type: string}
                          - {name: listing_id, type: string}
                          - {name: f_known, type: float64}
                          - {name: f_extra, type: float64}
                          - {name: f_noise, type: float64}
                          - {name: start_price, type: float64}
                          - {name: p_model, type: float64}
                          - {name: sold, type: int32}
                          - {name: v_price, type: float64}
                          - {name: n_bids, type: int64}
                          - {name: session_time, type: timestamp}
                """);
        return sb.toString();
    }

    private static Map<String, MElement> byKey(final Iterable<MElement> rows) {
        final Map<String, MElement> map = new HashMap<>();
        for (final MElement r : rows) map.put(r.getAsString("candidate") + ":" + r.getAsString("transform"), r);
        return map;
    }

    @Test
    public void testGroupedMultinomialConditionedOnBaseline() throws Exception {
        final String config = sessionsConfig(300, 4, 42) + """
                transforms:
                  - name: screen
                    module: screen
                    inputs: [listings]
                    parameters:
                      family: groupedMultinomial
                      group: session_id
                      label: sold
                      baseline: {field: p_model, form: prob}
                      time: {field: session_time}
                      candidates:
                        include: ["f_*"]
                      transforms: [raw, rank, absdev]
                      periods: {bucket: year}
                      placebo:
                        noise: 50
                        shuffle: {field: f_noise, n: 20}
                        quantile: 0.99
                        seed: 7
                      flags: {leakZ: 20}
                """;
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(config));
        Assertions.assertNotNull(outputs.get("screen"));
        Assertions.assertNotNull(outputs.get("screen.summary"));

        PAssert.that(outputs.get("screen").getCollection()).satisfies(rows -> {
            final Map<String, MElement> records = byKey(rows);
            Assertions.assertEquals((3 + 50 + 20) * 3, records.size());
            final MElement signal = records.get("f_extra:raw");
            final MElement known = records.get("f_known:raw");
            final MElement noise = records.get("f_noise:raw");
            Assertions.assertEquals(300L, signal.getAsLong("n_groups"));
            Assertions.assertEquals(1200L, signal.getAsLong("n_obs"));
            Assertions.assertEquals(1L, signal.getAsLong("df"));
            Assertions.assertEquals("scoreTest", signal.getAsString("method"));
            Assertions.assertEquals("groupedMultinomial", signal.getAsString("family"));
            // the extra information passes with a large positive z; the baseline's own feature and the noise do not
            Assertions.assertTrue(signal.getAsDouble("z") > 5, "z of f_extra: " + signal.getAsDouble("z"));
            Assertions.assertEquals(Boolean.TRUE, signal.getPrimitiveValue("passed"));
            Assertions.assertEquals(Boolean.FALSE, signal.getPrimitiveValue("leakSuspect"));
            Assertions.assertEquals(Boolean.FALSE, signal.getPrimitiveValue("placebo"));
            Assertions.assertTrue(signal.getAsDouble("est_gain") > signal.getAsDouble("threshold"));
            Assertions.assertTrue(signal.getAsDouble("pValue") < 1e-6);
            Assertions.assertTrue(signal.getAsDouble("qValue") < 1e-4);
            Assertions.assertTrue(Math.abs(known.getAsDouble("z")) < 3, "z of f_known (conditioned out): " + known.getAsDouble("z"));
            Assertions.assertEquals(Boolean.FALSE, known.getPrimitiveValue("passed"));
            Assertions.assertEquals(Boolean.FALSE, noise.getPrimitiveValue("passed"));
            // periods: two years, the signal agrees in both
            Assertions.assertEquals(2L, signal.getAsLong("n_periods"));
            Assertions.assertEquals(2L, signal.getAsLong("periods_agree"));
            final List<?> periods = (List<?>) signal.getPrimitiveValue("period_z");
            Assertions.assertEquals(2, periods.size());
            // placebo records carry the placebo flag and no q-value
            final MElement placebo = records.get("__noise_0:raw");
            Assertions.assertEquals(Boolean.TRUE, placebo.getPrimitiveValue("placebo"));
            Assertions.assertEquals(Boolean.FALSE, placebo.getPrimitiveValue("passed"));
            Assertions.assertNull(placebo.getPrimitiveValue("qValue"));
            Assertions.assertNotNull(records.get("__shuffle_19:absdev"));
            // every record shares the threshold
            final double threshold = signal.getAsDouble("threshold");
            for (final MElement r : rows) Assertions.assertEquals(threshold, r.getAsDouble("threshold"));
            return null;
        });
        PAssert.that(outputs.get("screen.summary").getCollection()).satisfies(rows -> {
            final List<MElement> list = new ArrayList<>();
            rows.forEach(list::add);
            Assertions.assertEquals(1, list.size());
            final MElement summary = list.get(0);
            Assertions.assertEquals(1200L, summary.getAsLong("nRows"));
            Assertions.assertEquals(0L, summary.getAsLong("nRowsTimeFiltered"));
            Assertions.assertEquals(300L, summary.getAsLong("nUnits"));
            Assertions.assertEquals(0L, summary.getAsLong("nUnitsSkipped"));
            Assertions.assertEquals(3L, summary.getAsLong("nCandidates"));
            Assertions.assertEquals(9L, summary.getAsLong("nScored"));
            Assertions.assertEquals(210L, summary.getAsLong("nPlacebo"));
            Assertions.assertEquals("session_id", summary.getAsString("group"));
            Assertions.assertEquals("prob", summary.getAsString("baselineForm"));
            Assertions.assertEquals("year", summary.getAsString("periodsBucket"));
            Assertions.assertTrue(summary.getAsDouble("threshold") > 0);
            Assertions.assertTrue(summary.getAsDouble("thresholdTheoretical") > 0);
            final List<?> passed = (List<?>) summary.getPrimitiveValue("passedColumns");
            // f_extra passes; f_known is exactly conditioned out by the baseline, f_noise carries nothing
            Assertions.assertTrue(passed.contains("f_extra"), "passedColumns: " + passed);
            Assertions.assertFalse(passed.contains("f_known"), "passedColumns: " + passed);
            Assertions.assertFalse(passed.contains("f_noise"), "passedColumns: " + passed);
            Assertions.assertNotNull(summary.getPrimitiveValue("maxTime"));
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testIndependentBinomialWithExpressionLabelAndTimeWindow() throws Exception {
        final String config = sessionsConfig(120, 3, 3) + """
                transforms:
                  - name: screen
                    module: screen
                    inputs: [listings]
                    parameters:
                      family: binomial
                      label: {expr: "sold > 0 ? 1 : 0"}
                      time: {field: session_time, to: "2024-06-30T23:59:59Z"}
                      candidates: {include: ["*"], exclude: ["p_model", "start_price", "v_price", "n_bids"]}
                      periods: {field: session_time, bucket: quarter}
                      placebo: {noise: 10, quantile: 0.95, seed: 1}
                """;
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(config));
        PAssert.that(outputs.get("screen").getCollection()).satisfies(rows -> {
            final Map<String, MElement> records = byKey(rows);
            // raw only for independent rows; sold / session_time are roles, p_model / start_price excluded by name
            Assertions.assertEquals(3 + 10, records.size());
            Assertions.assertTrue(records.containsKey("f_extra:raw"));
            Assertions.assertFalse(records.containsKey("p_model:raw"));
            Assertions.assertFalse(records.containsKey("f_extra:rank"));
            final MElement signal = records.get("f_extra:raw");
            // 120 sessions two days apart from 2024-01-01: 91 sessions fall before the end of June (Q1 + Q2)
            Assertions.assertEquals(91L * 3, signal.getAsLong("n_groups"));
            Assertions.assertEquals(2L, signal.getAsLong("n_periods"));
            Assertions.assertTrue(signal.getAsDouble("z") > 2, "z of f_extra: " + signal.getAsDouble("z"));
            Assertions.assertEquals("binomial", signal.getAsString("family"));
            return null;
        });
        PAssert.that(outputs.get("screen.summary").getCollection()).satisfies(rows -> {
            final MElement summary = rows.iterator().next();
            Assertions.assertEquals(360L, summary.getAsLong("nRows"));
            Assertions.assertEquals(29L * 3, summary.getAsLong("nRowsTimeFiltered"));
            Assertions.assertEquals(273L, summary.getAsLong("nUnits"));
            Assertions.assertNull(summary.getAsString("baseline"));
            Assertions.assertEquals("2024-06-30T23:59:59Z", summary.getAsString("timeTo"));
            Assertions.assertEquals(List.of("raw"), summary.getPrimitiveValue("transforms"));
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testConditioningReplacesTheBaseline() throws Exception {
        // no baseline: the prior sees f_known and f_extra; conditioning on f_known removes f_known (r2_F ≈ 1)
        // while f_extra keeps its partial gain
        // small on purpose: the DirectRunner's cost grows with (unrolled passes x keyed bundles), see createPipeline
        final String config = sessionsConfig(80, 8, 42) + """
                transforms:
                  - name: screen
                    module: screen
                    inputs: [listings]
                    parameters:
                      family: groupedMultinomial
                      group: session_id
                      label: sold
                      time: {field: session_time}
                      candidates: {include: ["f_*"]}
                      transforms: [raw, rank]
                      placebo: {noise: 30, seed: 3}
                      conditioning: {fields: [f_known], l2: 1.0e-4, maxIter: 6}
                """;
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(config));
        PAssert.that(outputs.get("screen").getCollection()).satisfies(rows -> {
            final Map<String, MElement> records = byKey(rows);
            Assertions.assertEquals((3 + 30) * 2, records.size());
            final MElement known = records.get("f_known:raw");
            final MElement extra = records.get("f_extra:raw");
            final MElement noise = records.get("f_noise:raw");
            // marginally f_known is the strongest column; conditioned on itself it vanishes
            Assertions.assertTrue(known.getAsDouble("z") > 5, "marginal z of f_known: " + known.getAsDouble("z"));
            // the L2 ridge leaves a residual of order l2 in the orthogonalisation: r2_F ≈ 1, partial gain ≈ 0
            Assertions.assertTrue(known.getAsDouble("r2_F") > 0.999, "r2_F of f_known: " + known.getAsDouble("r2_F"));
            Assertions.assertTrue(known.getAsDouble("partial_gain") < known.getAsDouble("threshold") / 100, "partial gain of f_known: " + known.getAsDouble("partial_gain"));
            Assertions.assertEquals(Boolean.FALSE, known.getPrimitiveValue("passed"));
            // f_extra survives the conditioning
            Assertions.assertTrue(extra.getAsDouble("partial_z") > 5, "partial z of f_extra: " + extra.getAsDouble("partial_z"));
            Assertions.assertTrue(extra.getAsDouble("r2_F") < 0.2, "r2_F of f_extra: " + extra.getAsDouble("r2_F"));
            Assertions.assertEquals(Boolean.TRUE, extra.getPrimitiveValue("passed"));
            Assertions.assertTrue(extra.getAsDouble("partial_gain") > extra.getAsDouble("threshold"));
            Assertions.assertNotNull(extra.getAsDouble("partial_pValue"));
            Assertions.assertNotNull(extra.getAsDouble("qValue"));
            Assertions.assertEquals(Boolean.FALSE, noise.getPrimitiveValue("passed"));
            // placebo columns carry partial statistics too
            Assertions.assertNotNull(records.get("__noise_0:rank").getAsDouble("partial_gain"));
            return null;
        });
        PAssert.that(outputs.get("screen.summary").getCollection()).satisfies(rows -> {
            final MElement summary = rows.iterator().next();
            Assertions.assertEquals(List.of("f_known"), summary.getPrimitiveValue("conditioningFields"));
            Assertions.assertEquals(1L, summary.getAsLong("conditioningK"));
            Assertions.assertEquals(Boolean.TRUE, summary.getPrimitiveValue("conditioningConverged"));
            Assertions.assertTrue(summary.getAsLong("conditioningIterations") <= 6);
            Assertions.assertTrue(summary.getAsDouble("conditioningGain") > 0);
            final List<?> passed = (List<?>) summary.getPrimitiveValue("passedColumns");
            Assertions.assertTrue(passed.contains("f_extra"), "passedColumns: " + passed);
            Assertions.assertFalse(passed.contains("f_known"), "passedColumns: " + passed);
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testConditioningRejectsWindowedInput() {
        final String config = sessionsConfig(2, 2, 1) + """
                transforms:
                  - name: screen
                    module: screen
                    inputs: [listings]
                    strategy:
                      window: {type: fixed, unit: day, size: 1, offset: 0}
                    parameters:
                      family: groupedMultinomial
                      group: session_id
                      label: sold
                      candidates: {include: ["f_*"]}
                      conditioning: {fields: [f_known]}
                """;
        final IllegalModuleException e = Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, Config.load(config)));
        Assertions.assertTrue(String.join(" ", e.errorMessages).contains("global window"), e.getMessage());
    }

    @Test
    public void testRejectsTriggeredInput() {
        // a triggered global window fires the Combines once per pane: the singleton views of the conditioning chain
        // and the single summary both assume one pane
        final String config = sessionsConfig(2, 2, 1) + """
                transforms:
                  - name: screen
                    module: screen
                    inputs: [listings]
                    strategy:
                      window: {type: global}
                      trigger: {type: repeatedly, foreverTrigger: {type: afterPane, elementCountAtLeast: 1}}
                    parameters:
                      family: groupedMultinomial
                      group: session_id
                      label: sold
                      candidates: {include: ["f_*"]}
                      conditioning: {fields: [f_known]}
                """;
        final IllegalModuleException e = Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, Config.load(config)));
        Assertions.assertTrue(String.join(" ", e.errorMessages).contains("default trigger"), e.getMessage());
    }

    @Test
    public void testSelectionFileClosesTheLoop() throws Exception {
        // relative path: Beam FileSystems treats a Windows drive letter as a URI scheme
        final String selection = "target/screen-selection/" + java.util.UUID.randomUUID() + "/passed.json";
        final String config = sessionsConfig(120, 4, 42) + """
                transforms:
                  - name: screen
                    module: screen
                    inputs: [listings]
                    parameters:
                      family: groupedMultinomial
                      group: session_id
                      label: sold
                      baseline: {field: p_model, form: prob}
                      time: {field: session_time, to: "2024-12-31T23:59:59Z"}
                      candidates: {include: ["f_*"]}
                      transforms: [raw]
                      placebo: {noise: 20, seed: 5}
                      output: {selection: SELECTION}
                """.replace("SELECTION", selection);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(config));
        PAssert.that(outputs.get("screen.summary").getCollection()).satisfies(rows -> {
            Assertions.assertEquals(1, ((List<?>) rows.iterator().next().getPrimitiveValue("passedColumns")).size());
            return null;
        });
        pipeline.run();

        final java.nio.file.Path file = java.nio.file.Paths.get(selection);
        Assertions.assertTrue(java.nio.file.Files.exists(file), "selection file written: " + selection);
        final com.google.gson.JsonObject json = com.google.gson.JsonParser.parseString(java.nio.file.Files.readString(file)).getAsJsonObject();
        Assertions.assertEquals(1, json.get("version").getAsInt());
        Assertions.assertEquals(List.of("f_extra"), json.getAsJsonArray("columns").asList().stream().map(com.google.gson.JsonElement::getAsString).toList());
        Assertions.assertEquals("marginal", json.get("test").getAsString());
        Assertions.assertEquals("2024-12-31T23:59:59Z", json.get("timeTo").getAsString());
        Assertions.assertTrue(json.get("threshold").getAsDouble() > 0);
        Assertions.assertEquals(64, json.get("screenHash").getAsString().length());
        Assertions.assertTrue(json.get("planHash").isJsonNull());
        Assertions.assertEquals("f_extra", json.getAsJsonArray("passed").get(0).getAsJsonObject().get("candidate").getAsString());
    }

    @Test
    public void testGaussianAndPoissonFamilies() throws Exception {
        // gaussian: a continuous label linear in f_extra (f_noise is the noise); poisson: a count driven by f_extra
        final String gaussian = sessionsConfig(150, 4, 11) + """
                transforms:
                  - name: screen
                    module: screen
                    inputs: [listings]
                    parameters:
                      family: gaussian
                      label: v_price
                      time: {field: session_time}
                      candidates: {include: ["f_known", "f_extra"]}
                      placebo: {noise: 20, seed: 2}
                      conditioning: {fields: [f_known], maxIter: 3}
                """;
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(gaussian));
        PAssert.that(outputs.get("screen").getCollection()).satisfies(rows -> {
            final Map<String, MElement> records = byKey(rows);
            final MElement extra = records.get("f_extra:raw");
            final MElement known = records.get("f_known:raw");
            Assertions.assertEquals("gaussian", extra.getAsString("family"));
            Assertions.assertEquals(600L, extra.getAsLong("n_groups"));
            Assertions.assertTrue(extra.getAsDouble("z") > 10, "gaussian z of f_extra: " + extra.getAsDouble("z"));
            Assertions.assertEquals(Boolean.TRUE, extra.getPrimitiveValue("passed"));
            Assertions.assertTrue(extra.getAsDouble("partial_z") > 10, "gaussian partial z of f_extra: " + extra.getAsDouble("partial_z"));
            Assertions.assertTrue(Math.abs(known.getAsDouble("z")) < 4, "gaussian z of f_known: " + known.getAsDouble("z"));
            Assertions.assertEquals(1d, known.getAsDouble("r2_F"), 1e-6);
            return null;
        });
        PAssert.that(outputs.get("screen.summary").getCollection()).satisfies(rows -> {
            final MElement summary = rows.iterator().next();
            Assertions.assertEquals(Boolean.TRUE, summary.getPrimitiveValue("conditioningConverged"));
            Assertions.assertTrue(summary.getAsLong("conditioningIterations") <= 3);   // least squares: one Newton step
            return null;
        });
        pipeline.run();

        final TestPipeline second = createPipeline();
        final String poisson = sessionsConfig(150, 4, 12) + """
                transforms:
                  - name: screen
                    module: screen
                    inputs: [listings]
                    parameters:
                      family: poisson
                      label: n_bids
                      time: {field: session_time}
                      candidates: {include: ["f_known", "f_extra"]}
                      placebo: {noise: 20, seed: 2}
                """;
        final Map<String, MCollection> counts = MPipeline.apply(second, Config.load(poisson));
        PAssert.that(counts.get("screen").getCollection()).satisfies(rows -> {
            final Map<String, MElement> records = byKey(rows);
            final MElement extra = records.get("f_extra:raw");
            final MElement known = records.get("f_known:raw");
            Assertions.assertEquals("poisson", extra.getAsString("family"));
            Assertions.assertTrue(extra.getAsDouble("z") > 5, "poisson z of f_extra: " + extra.getAsDouble("z"));
            Assertions.assertEquals(Boolean.TRUE, extra.getPrimitiveValue("passed"));
            Assertions.assertEquals(Boolean.FALSE, known.getPrimitiveValue("passed"));
            return null;
        });
        second.run();
    }

    @Test
    public void testAssemblyRejectsUnresolvableSpec() {
        final String config = sessionsConfig(2, 2, 1) + """
                transforms:
                  - name: screen
                    module: screen
                    inputs: [listings]
                    parameters:
                      family: groupedMultinomial
                      label: sold
                      candidates: {include: ["f_*"]}
                """;
        final IllegalModuleException e = Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, Config.load(config)));
        Assertions.assertTrue(String.join(" ", e.errorMessages).contains("group is required"), e.getMessage());
    }
}
