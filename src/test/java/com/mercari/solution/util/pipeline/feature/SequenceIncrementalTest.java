package com.mercari.solution.util.pipeline.feature;

import com.google.gson.JsonObject;
import com.mercari.solution.config.Config;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;

/**
 * Randomized equivalence of the incremental (running sufficient statistics) and scan evaluation paths:
 * replays one key's history exactly like the keyed stage (pending rows join once the timestamp advances)
 * and compares every sequence / population column value row by row.
 */
public class SequenceIncrementalTest {

    private static final String SOURCES = """
            sources:
              - name: listings
                eventTime: session_time
                keys: [session_id, seller_id]
                fields:
                  - {name: session_id, type: string}
                  - {name: seller_id, type: string}
                  - {name: condition_grade, type: string}
                  - {name: start_price, type: float64}
                  - {name: sold, type: int32, availableAt: after(event), kind: outcome}
                settlementLag: PT30M
                ingestionLag: P2D
            """;

    private static final String SPEC = """
            lineage:
              - {fields: [session_id, seller_id, condition_grade, start_price, sold], from: listings}
            time: {field: session_time, orderTieBreak: [session_id]}
            predictAt: "event_time - PT10M"
            entities:
              - {name: seller, keys: [seller_id]}
            features:
              - name: seq
                scope: sequence
                entity: seller
                windows:
                  - {maxAge: P30D}
                  - {maxAge: P90D, filter: "condition_grade = $self.condition_grade"}
                ops:
                  - {type: aggregate, field: start_price, funcs: [count, mean, sum, std]}
                  - {type: aggregate, field: sold, funcs: [mean]}
              - name: unbounded
                scope: sequence
                entity: seller
                ops:
                  - {type: aggregate, field: start_price, funcs: [max, min, count]}
              - name: enc
                scope: population
                type: encoding
                keySets:
                  - keys: [seller_id]
                    windows: [{maxAge: P60D}]
                targets:
                  - {stats: [count]}
                  - {field: sold, stats: [mean, std]}
                  - {field: condition_grade, stats: [distribution]}
            """;

    @Test
    public void testIncrementalMatchesScan() {
        final JsonObject sources = Config.convertConfigJson(SOURCES, Config.Format.yaml);
        final JsonObject spec = Config.convertConfigJson(SPEC, Config.Format.yaml);
        final FeaturePlan plan = FeaturePlanCompiler.compile(sources, spec, null);
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);

        final List<OutputColumn> keyed = plan.getColumns().stream()
                .filter(c -> c.getScope() == FeatureSpec.Scope.sequence || c.getScope() == FeatureSpec.Scope.population)
                .toList();
        Assertions.assertTrue(keyed.size() >= 12, () -> "columns: " + keyed);
        final SequenceEvaluator sequence = new SequenceEvaluator(keyed);
        final PopulationEvaluator population = new PopulationEvaluator(keyed);
        sequence.setup();
        population.setup();

        final Random random = new Random(11);
        final long base = 1_700_000_000_000L;
        long millis = base;
        final List<SequenceEvaluator.Past> history = new ArrayList<>();
        final List<SequenceEvaluator.Past> pending = new ArrayList<>();
        long pendingMillis = Long.MIN_VALUE;
        final SequenceEvaluator.KeyState seqState = new SequenceEvaluator.KeyState();
        final SequenceEvaluator.KeyState popState = new SequenceEvaluator.KeyState();

        int compared = 0;
        for (int i = 0; i < 400; i++) {
            // occasionally rows share a timestamp; steps span minutes to weeks so windows and shifts both bite
            if (random.nextDouble() > 0.15) {
                millis += (long) (Math.pow(10, 4 + random.nextDouble() * 5));
            }
            final Map<String, Object> row = new HashMap<>();
            row.put("seller_id", "s1");
            row.put("condition_grade", "g" + random.nextInt(3));
            row.put("start_price", random.nextInt(10) == 0 ? null : Math.round(random.nextDouble() * 1000) / 10.0);
            row.put("sold", random.nextInt(12) == 0 ? null : random.nextInt(2));

            if (millis != pendingMillis) {
                history.addAll(pending);
                pending.clear();
                pendingMillis = millis;
            }
            for (final OutputColumn c : keyed) {
                final SequenceEvaluator evaluator = c.getScope() == FeatureSpec.Scope.sequence ? sequence : population;
                final SequenceEvaluator.KeyState state = c.getScope() == FeatureSpec.Scope.sequence ? seqState : popState;
                final Object incremental = evaluator.evaluateColumn(c, row, millis, history, state);
                final Object scan = evaluator.evaluateColumn(c, row, millis, history, null);
                assertSame(c.getCanonicalName() + "@" + i, scan, incremental);
                compared++;
            }
            pending.add(new SequenceEvaluator.Past(millis, new HashMap<>(row)));
        }
        Assertions.assertTrue(compared > 4000);
    }

    @SuppressWarnings("unchecked")
    private static void assertSame(final String at, final Object scan, final Object incremental) {
        if (scan == null || incremental == null) {
            Assertions.assertEquals(scan, incremental, at);
            return;
        }
        if (scan instanceof Number a && incremental instanceof Number b) {
            Assertions.assertEquals(a.doubleValue(), b.doubleValue(), Math.max(1e-9, Math.abs(a.doubleValue()) * 1e-9), at);
            return;
        }
        if (scan instanceof Map && incremental instanceof Map) {
            final Map<String, Object> a = (Map<String, Object>) scan;
            final Map<String, Object> b = (Map<String, Object>) incremental;
            Assertions.assertEquals(a.keySet(), b.keySet(), at);
            for (final String k : a.keySet()) assertSame(at + "/" + k, a.get(k), b.get(k));
            return;
        }
        Assertions.assertEquals(scan, incremental, at);
    }

}
