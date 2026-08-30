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
                  - {field: condition_grade, stats: [distribution, count]}
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

    /**
     * The keyed stage trims the history behind every column's fold / evict pointer (or maxAge far edge).
     * Replaying with trimming must give the same values as the untrimmed list, and must actually drop rows.
     */
    @Test
    public void testTrimmedHistoryMatchesUntrimmed() {
        final JsonObject sources = Config.convertConfigJson(SOURCES, Config.Format.yaml);
        // every column here is trimmable: windowed (evict pointer / maxAge far edge) or unbounded but
        // incremental (fold pointer); an unbounded scan-path column would pin the whole history
        final FeaturePlan plan = FeaturePlanCompiler.compile(sources, Config.convertConfigJson(SPEC, Config.Format.yaml), null);
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        final List<OutputColumn> keyed = plan.getColumns().stream()
                .filter(c -> c.getScope() == FeatureSpec.Scope.sequence || c.getScope() == FeatureSpec.Scope.population)
                .toList();
        Assertions.assertFalse(keyed.isEmpty());
        // like StageEvaluator: each evaluator owns its scope's columns (its trim watermark covers only those)
        final List<OutputColumn> sequences = keyed.stream().filter(c -> c.getScope() == FeatureSpec.Scope.sequence).toList();
        final List<OutputColumn> populations = keyed.stream().filter(c -> c.getScope() == FeatureSpec.Scope.population).toList();
        final SequenceEvaluator sequence = new SequenceEvaluator(sequences);
        final PopulationEvaluator population = new PopulationEvaluator(populations);
        sequence.setup();
        population.setup();
        final SequenceEvaluator trimmedSequence = new SequenceEvaluator(sequences);
        final PopulationEvaluator trimmedPopulation = new PopulationEvaluator(populations);
        trimmedSequence.setup();
        trimmedPopulation.setup();

        final Random random = new Random(7);
        long millis = 1_700_000_000_000L;
        final List<SequenceEvaluator.Past> full = new ArrayList<>();
        final SequenceEvaluator.History trimmed = new SequenceEvaluator.History();
        final List<SequenceEvaluator.Past> pending = new ArrayList<>();
        long pendingMillis = Long.MIN_VALUE;
        final SequenceEvaluator.KeyState seq = new SequenceEvaluator.KeyState(), pop = new SequenceEvaluator.KeyState();
        final SequenceEvaluator.KeyState tseq = new SequenceEvaluator.KeyState(), tpop = new SequenceEvaluator.KeyState();
        int maxRetained = 0;
        for (int i = 0; i < 5000; i++) {
            millis += 1 + (long) (random.nextDouble() * 6 * 3600_000L); // ~ 3 hours on average → 30 days ≈ 240 rows
            final Map<String, Object> row = new HashMap<>();
            row.put("seller_id", "s1");
            row.put("condition_grade", "g" + random.nextInt(3));
            row.put("start_price", Math.round(random.nextDouble() * 1000) / 10.0);
            row.put("sold", random.nextInt(2));
            if (millis != pendingMillis) {
                full.addAll(pending);
                trimmed.addAll(pending);
                pending.clear();
                pendingMillis = millis;
            }
            final Map<String, Object> trimmedRow = new HashMap<>(row);
            for (final OutputColumn c : keyed) {
                final boolean isSequence = c.getScope() == FeatureSpec.Scope.sequence;
                final Object expected = (isSequence ? sequence : population).evaluateColumn(c, row, millis, full, isSequence ? seq : pop);
                final Object actual = (isSequence ? trimmedSequence : trimmedPopulation).evaluateColumn(c, trimmedRow, millis, trimmed, isSequence ? tseq : tpop);
                assertSame(c.getCanonicalName() + "@" + i, expected, actual);
            }
            pending.add(new SequenceEvaluator.Past(millis, new HashMap<>(row)));
            trimmed.trim(SequenceEvaluator.Retention.of(trimmedSequence.retention(tseq, millis, trimmed), trimmedPopulation.retention(tpop, millis, trimmed)));
            maxRetained = Math.max(maxRetained, trimmed.retained());
        }
        Assertions.assertEquals(full.size(), trimmed.size()); // absolute indices are preserved
        Assertions.assertTrue(trimmed.base() > 0, () -> "history was never trimmed; unbounded: " + trimmedSequence.unboundedColumns() + " / " + trimmedPopulation.unboundedColumns());
        final int retainedPeak = maxRetained;
        Assertions.assertTrue(retainedPeak < full.size() / 2, () -> "retained " + retainedPeak + " of " + full.size());
        Assertions.assertThrows(IndexOutOfBoundsException.class, () -> trimmed.get(0));
    }

    /**
     * Scan-path columns without maxAge: lag / delta / trend and maxEvents-only windows read a bounded tail of
     * the history and let it be trimmed; ewma (and filtered windows) are unbounded and pin it.
     */
    @Test
    public void testBoundedTailTrimsWithoutMaxAge() {
        final JsonObject sources = Config.convertConfigJson(SOURCES, Config.Format.yaml);
        final String bounded = """
                lineage:
                  - {fields: [session_id, seller_id, condition_grade, start_price, sold], from: listings}
                time: {field: session_time}
                predictAt: "event_time - PT10M"
                entities:
                  - {name: seller, keys: [seller_id]}
                features:
                  - name: tail
                    scope: sequence
                    entity: seller
                    ops:
                      - {type: lag, fields: [start_price], k: 2}
                      - {type: delta, fields: [start_price], k: 1}
                      - {type: trend, fields: [start_price], k: 5}
                  - name: last3
                    scope: sequence
                    entity: seller
                    windows: [{maxEvents: 3}]
                    ops:
                      - {type: aggregate, fields: [start_price], funcs: [mean, max]}
                """;
        final FeaturePlan plan = FeaturePlanCompiler.compile(sources, Config.convertConfigJson(bounded, Config.Format.yaml), null);
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        final List<OutputColumn> keyed = plan.getColumns().stream().filter(c -> c.getScope() == FeatureSpec.Scope.sequence).toList();
        Assertions.assertTrue(keyed.size() >= 5, plan::describe);
        final SequenceEvaluator full = new SequenceEvaluator(keyed);
        final SequenceEvaluator trimmedEvaluator = new SequenceEvaluator(keyed);
        full.setup();
        trimmedEvaluator.setup();
        Assertions.assertTrue(trimmedEvaluator.unboundedColumns().isEmpty(), () -> trimmedEvaluator.unboundedColumns().toString());

        final Random random = new Random(3);
        long millis = 1_700_000_000_000L;
        final List<SequenceEvaluator.Past> history = new ArrayList<>();
        final SequenceEvaluator.History trimmed = new SequenceEvaluator.History();
        final List<SequenceEvaluator.Past> pending = new ArrayList<>();
        long pendingMillis = Long.MIN_VALUE;
        final SequenceEvaluator.KeyState state = new SequenceEvaluator.KeyState(), trimmedState = new SequenceEvaluator.KeyState();
        int maxRetained = 0;
        for (int i = 0; i < 3000; i++) {
            if (random.nextDouble() > 0.2) millis += 1 + (long) (random.nextDouble() * 3600_000L);
            final Map<String, Object> row = new HashMap<>();
            row.put("seller_id", "s1");
            row.put("start_price", random.nextInt(8) == 0 ? null : (double) random.nextInt(100));
            if (millis != pendingMillis) {
                history.addAll(pending);
                trimmed.addAll(pending);
                pending.clear();
                pendingMillis = millis;
            }
            final Map<String, Object> trimmedRow = new HashMap<>(row);
            for (final OutputColumn c : keyed) {
                assertSame(c.getCanonicalName() + "@" + i,
                        full.evaluateColumn(c, row, millis, history, state),
                        trimmedEvaluator.evaluateColumn(c, trimmedRow, millis, trimmed, trimmedState));
            }
            pending.add(new SequenceEvaluator.Past(millis, new HashMap<>(row)));
            trimmed.trim(trimmedEvaluator.retention(trimmedState, millis, trimmed));
            maxRetained = Math.max(maxRetained, trimmed.retained());
        }
        final int retainedPeak = maxRetained;
        Assertions.assertTrue(trimmed.base() > 0, "history was never trimmed");
        // trend k=5 is the longest tail; the trim is amortised (drops wait for a 1024-row or half-size prefix)
        Assertions.assertTrue(retainedPeak < 2100, () -> "retained " + retainedPeak);

        // ewma and a filtered lag have no bounded tail
        final String unbounded = bounded.replace("- {type: trend, fields: [start_price], k: 5}",
                "- {type: ewma, fields: [start_price], halflife: [3]}")
                .replace("windows: [{maxEvents: 3}]", "windows: [{maxEvents: 3, filter: \"start_price > 10\"}]");
        final FeaturePlan plan2 = FeaturePlanCompiler.compile(sources, Config.convertConfigJson(unbounded, Config.Format.yaml), null);
        Assertions.assertFalse(plan2.getDiagnostics().hasErrors(), plan2::describe);
        final SequenceEvaluator e2 = new SequenceEvaluator(plan2.getColumns().stream().filter(c -> c.getScope() == FeatureSpec.Scope.sequence).toList());
        e2.setup();
        final List<String> pinned = e2.unboundedColumns();
        Assertions.assertEquals(3, pinned.size(), pinned::toString); // ewma + 2 filtered aggregates
        Assertions.assertTrue(pinned.stream().anyMatch(n -> n.contains("ewma")), pinned::toString);
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


    /**
     * Per-field retention: a scan-path column without maxAge keeps the whole history of its key, but only
     * the fields it reads. The fields of the bounded columns are removed from the older entries, and every
     * column still evaluates exactly as over the untrimmed list.
     */
    @Test
    public void testFieldsAreTrimmedPerColumnWindow() {
        final JsonObject sources = Config.convertConfigJson(SOURCES, Config.Format.yaml);
        final String mixed = """
                lineage:
                  - {fields: [session_id, seller_id, condition_grade, start_price, sold], from: listings}
                time: {field: session_time}
                predictAt: "event_time - PT10M"
                entities:
                  - {name: seller, keys: [seller_id]}
                features:
                  - name: all_time
                    scope: sequence
                    entity: seller
                    windows: [{filter: "condition_grade = 'g0'"}]
                    ops:
                      - {type: aggregate, field: sold, funcs: [count]}
                  - name: recent
                    scope: sequence
                    entity: seller
                    windows: [{maxAge: P30D}]
                    ops:
                      - {type: aggregate, field: start_price, funcs: [mean, max]}
                """;
        final FeaturePlan plan = FeaturePlanCompiler.compile(sources, Config.convertConfigJson(mixed, Config.Format.yaml), null);
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        final List<OutputColumn> keyed = plan.getColumns().stream().filter(c -> c.getScope() == FeatureSpec.Scope.sequence).toList();
        final SequenceEvaluator full = new SequenceEvaluator(keyed);
        final SequenceEvaluator trimmedEvaluator = new SequenceEvaluator(keyed);
        full.setup();
        trimmedEvaluator.setup();
        Assertions.assertEquals(1, trimmedEvaluator.unboundedColumns().size(), () -> trimmedEvaluator.unboundedColumns().toString());

        final Random random = new Random(11);
        long millis = 1_700_000_000_000L;
        final List<SequenceEvaluator.Past> history = new ArrayList<>();
        final SequenceEvaluator.History trimmed = new SequenceEvaluator.History();
        final List<SequenceEvaluator.Past> pending = new ArrayList<>();
        long pendingMillis = Long.MIN_VALUE;
        final SequenceEvaluator.KeyState state = new SequenceEvaluator.KeyState(), trimmedState = new SequenceEvaluator.KeyState();
        for (int i = 0; i < 3000; i++) {
            millis += 1 + (long) (random.nextDouble() * 6 * 3600_000L);
            final Map<String, Object> row = new HashMap<>();
            row.put("seller_id", "s1");
            row.put("condition_grade", "g" + random.nextInt(3));
            row.put("start_price", random.nextInt(8) == 0 ? null : (double) random.nextInt(100));
            row.put("sold", random.nextInt(2));
            if (millis != pendingMillis) {
                history.addAll(pending);
                // the trimmed history owns its maps (the per-field trim mutates them)
                for (final SequenceEvaluator.Past p : pending) trimmed.add(new SequenceEvaluator.Past(p.millis(), new HashMap<>(p.values())));
                pending.clear();
                pendingMillis = millis;
            }
            final Map<String, Object> trimmedRow = new HashMap<>(row);
            for (final OutputColumn c : keyed) {
                assertSame(c.getCanonicalName() + "@" + i,
                        full.evaluateColumn(c, row, millis, history, state),
                        trimmedEvaluator.evaluateColumn(c, trimmedRow, millis, trimmed, trimmedState));
            }
            pending.add(new SequenceEvaluator.Past(millis, new HashMap<>(row)));
            trimmed.trim(trimmedEvaluator.retention(trimmedState, millis, trimmed));
        }
        // no entry is dropped (the filtered window reads them all) ...
        Assertions.assertEquals(0, trimmed.base());
        Assertions.assertEquals(history.size(), trimmed.retained());
        // ... but the bounded column's field is gone from the old entries while the unbounded one's stays
        final SequenceEvaluator.Past oldest = trimmed.get(0);
        Assertions.assertTrue(oldest.values().containsKey("condition_grade") && oldest.values().containsKey("sold"), oldest::toString);
        Assertions.assertFalse(oldest.values().containsKey("start_price"), oldest::toString);
        int withPrice = 0;
        for (int i = 0; i < trimmed.size(); i++) if (trimmed.get(i).values().containsKey("start_price")) withPrice++;
        Assertions.assertTrue(withPrice < trimmed.size() / 4, () -> "entries still holding start_price: " + trimmed.size());
    }
}
