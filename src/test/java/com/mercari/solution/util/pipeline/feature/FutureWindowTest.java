package com.mercari.solution.util.pipeline.feature;

import com.google.gson.JsonObject;
import com.mercari.solution.config.Config;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;

/**
 * {@code direction: future}: the keyed stage replays a key latest first on the mirrored clock {@code −t}, and the
 * strictly-past evaluator then reads the strictly-future window {@code (t, t + maxAge]}. Every column is compared
 * with a direct computation over the real-time window (nearest event first), on both evaluation paths.
 */
public class FutureWindowTest {

    private static final String SOURCES = """
            sources:
              - name: listings
                eventTime: session_time
                keys: [session_id, seller_id]
                fields:
                  - {name: session_id, type: string}
                  - {name: seller_id, type: string}
                  - {name: start_price, type: float64}
                  - {name: sold, type: int32, availableAt: after(event), kind: outcome}
                settlementLag: PT30M
                ingestionLag: P2D
            """;

    private static final String SPEC = """
            lineage:
              - {fields: [session_id, seller_id, start_price, sold], from: listings}
            time: {field: session_time}
            predictAt: "event_time - PT10M"
            entities:
              - {name: seller, keys: [seller_id]}
            features:
              - name: next
                scope: sequence
                entity: seller
                direction: future
                windows: [{maxAge: P7D}, {maxAge: P30D, maxEvents: 3}]
                ops:
                  - {type: aggregate, field: start_price, funcs: [count, mean, min, max, first, last, std]}
                  - {type: lag, field: start_price, k: 2}
                  - {type: ewma, field: start_price, halflife: [2], decayBy: time}
                  - {type: sinceEvent, predicate: "sold = 1", unit: [events, days]}
                  - {type: countMatch, predicate: "sold = 1"}
                  - {type: barrier, field: start_price, up: 0.1, down: -0.1}
            """;

    private static final double DAY = 86_400_000d;

    @Test
    public void testMirroredReplayReadsTheFutureWindow() {
        final FeaturePlan plan = FeaturePlanCompiler.compile(Config.convertConfigJson(SOURCES, Config.Format.yaml),
                Config.convertConfigJson(SPEC, Config.Format.yaml), null);
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        final List<OutputColumn> columns = plan.getColumns().stream().filter(c -> c.getScope() == FeatureSpec.Scope.sequence).toList();
        final SequenceEvaluator evaluator = new SequenceEvaluator(columns);
        evaluator.setup();

        // one key, strictly increasing times (same-timestamp rows: FeatureTransformTest#testFutureLabelsSameTimestamp, through the real replay), steps of hours to days
        final Random random = new Random(7);
        final List<Map<String, Object>> rows = new ArrayList<>();
        final List<Long> times = new ArrayList<>();
        long millis = 1_700_000_000_000L;
        for (int i = 0; i < 300; i++) {
            millis += 3_600_000L + (long) (random.nextDouble() * 3 * DAY);
            final Map<String, Object> row = new HashMap<>();
            row.put("seller_id", "s1");
            row.put("start_price", random.nextInt(10) == 0 ? null : 80 + random.nextInt(40) * 1.0);
            row.put("sold", random.nextInt(3) == 0 ? 1 : 0);
            rows.add(row);
            times.add(millis);
        }

        // the replay of the keyed stage: latest first, clock −t
        final Map<Integer, Map<String, Object>> incremental = new HashMap<>(), scan = new HashMap<>();
        final List<SequenceEvaluator.Past> history = new ArrayList<>();
        final SequenceEvaluator.KeyState state = new SequenceEvaluator.KeyState();
        for (int i = rows.size() - 1; i >= 0; i--) {
            final long clock = -times.get(i);
            final Map<String, Object> a = new HashMap<>(rows.get(i)), b = new HashMap<>(rows.get(i));
            for (final OutputColumn c : columns) {
                a.put(c.getCanonicalName(), evaluator.evaluateColumn(c, a, clock, history, state));
                b.put(c.getCanonicalName(), evaluator.evaluateColumn(c, b, clock, history, null));
            }
            incremental.put(i, a);
            scan.put(i, b);
            history.add(new SequenceEvaluator.Past(clock, new HashMap<>(rows.get(i))));
        }

        int compared = 0;
        for (int i = 0; i < rows.size(); i++) {
            for (final String window : List.of("7d", "30d_n3")) {
                final long maxAge = "7d".equals(window) ? 7 * 86_400_000L : 30 * 86_400_000L;
                final List<Integer> future = new ArrayList<>();
                for (int j = i + 1; j < rows.size() && times.get(j) <= times.get(i) + maxAge; j++) future.add(j);
                if ("30d_n3".equals(window) && future.size() > 3) future.subList(3, future.size()).clear();
                final Map<String, Object> expected = oracle(rows, times, i, future);
                for (final Map.Entry<String, Object> e : expected.entrySet()) {
                    final String name = "next_" + window + "_" + e.getKey();
                    Assertions.assertNotNull(plan.getColumn(name), () -> "missing " + name + "\n" + plan.describe());
                    assertSame(name + "@" + i, e.getValue(), incremental.get(i).get(name));
                    assertSame(name + "@" + i + " (scan)", e.getValue(), scan.get(i).get(name));
                    compared++;
                }
            }
        }
        Assertions.assertTrue(compared > 5000, "compared " + compared);
    }

    /** The label values over a real-time future window (indices ascending = nearest first). */
    private static Map<String, Object> oracle(final List<Map<String, Object>> rows, final List<Long> times, final int i, final List<Integer> future) {
        final Map<String, Object> out = new LinkedHashMap<>();
        final List<Double> prices = new ArrayList<>();
        Object first = null, last = null;
        for (final int j : future) {
            final Object v = rows.get(j).get("start_price");
            if (v == null) continue;
            if (first == null) first = v;
            last = v;
            prices.add((Double) v);
        }
        out.put("start_price_count", (long) prices.size());
        out.put("start_price_mean", prices.isEmpty() ? null : prices.stream().mapToDouble(d -> d).average().orElseThrow());
        out.put("start_price_min", prices.isEmpty() ? null : prices.stream().mapToDouble(d -> d).min().orElseThrow());
        out.put("start_price_max", prices.isEmpty() ? null : prices.stream().mapToDouble(d -> d).max().orElseThrow());
        out.put("start_price_first", first);
        out.put("start_price_last", last);
        if (prices.size() < 2) {
            out.put("start_price_std", null);
        } else {
            final double mean = prices.stream().mapToDouble(d -> d).average().orElseThrow();
            out.put("start_price_std", Math.sqrt(prices.stream().mapToDouble(d -> (d - mean) * (d - mean)).sum() / prices.size()));
        }
        out.put("start_price_lead1", future.isEmpty() ? null : rows.get(future.get(0)).get("start_price"));
        out.put("start_price_lead2", future.size() < 2 ? null : rows.get(future.get(1)).get("start_price"));
        double num = 0, den = 0;
        for (final int j : future) {
            final Object v = rows.get(j).get("start_price");
            if (v == null) continue;
            final double w = Math.pow(0.5, (times.get(j) - times.get(i)) / DAY / 2);
            num += w * (Double) v;
            den += w;
        }
        out.put("start_price_ewma2", den == 0 ? null : num / den);
        Object untilEvents = null, untilDays = null;
        long matches = 0;
        for (int n = 0; n < future.size(); n++) {
            if (Integer.valueOf(1).equals(rows.get(future.get(n)).get("sold"))) {
                if (untilEvents == null) {
                    untilEvents = (long) (n + 1);
                    untilDays = (times.get(future.get(n)) - times.get(i)) / DAY;
                }
                matches++;
            }
        }
        out.put("until_events", untilEvents);
        out.put("until_days", untilDays);
        out.put("countmatch", matches);
        final Object entry = rows.get(i).get("start_price");
        Object barrier = null;
        if (entry != null) {
            for (final int j : future) {
                final Object v = rows.get(j).get("start_price");
                if (v == null) continue;
                if (barrier == null) barrier = 0L;
                final double move = ((Double) v - (Double) entry) / Math.abs((Double) entry);
                if (move >= 0.1) { barrier = 1L; break; }
                if (move <= -0.1) { barrier = -1L; break; }
            }
        }
        out.put("start_price_barrier", barrier);
        return out;
    }

    /** A move of exactly the declared level touches the barrier (100 → 90 at −10%), and "up" is an increase for a negative entry. */
    @Test
    public void testBarrierLevelsAreExactAndSignAware() {
        final FeaturePlan plan = FeaturePlanCompiler.compile(Config.convertConfigJson(SOURCES, Config.Format.yaml),
                Config.convertConfigJson(SPEC, Config.Format.yaml), null);
        final OutputColumn barrier = plan.getColumn("next_7d_start_price_barrier");
        final SequenceEvaluator evaluator = new SequenceEvaluator(List.of(barrier));
        evaluator.setup();
        final long t = 1_700_000_000_000L;
        for (final double[] c : new double[][]{{100, 90, -1}, {100, 110, 1}, {-10, -12, -1}, {-10, -8, 1}, {100, 95, 0}}) {
            final List<SequenceEvaluator.Past> history = new ArrayList<>(List.of(new SequenceEvaluator.Past(-(t + 3_600_000L), new HashMap<>(Map.of("seller_id", "s1", "start_price", c[1])))));
            final Map<String, Object> row = new HashMap<>(Map.of("seller_id", "s1", "start_price", c[0]));
            Assertions.assertEquals((long) c[2], evaluator.evaluateColumn(barrier, row, -t, history, null), () -> java.util.Arrays.toString(c));
        }
    }

    private static void assertSame(final String at, final Object expected, final Object actual) {
        if (expected == null || actual == null) {
            Assertions.assertEquals(expected, actual, at);
            return;
        }
        if (expected instanceof Number e && actual instanceof Number a) {
            Assertions.assertEquals(e.doubleValue(), a.doubleValue(), Math.max(1e-9, Math.abs(e.doubleValue()) * 1e-9), at);
            return;
        }
        Assertions.assertEquals(expected, actual, at);
    }
}
