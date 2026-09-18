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
            contexts:
              - {name: session, keys: [session_id]}
            baselines:
              - {name: market, context: session, expr: "share(1 / start_price)"}
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
                  - {type: regression, field: sold, against: start_price, funcs: [cov, corr, beta, intercept, r2]}
              - name: unbounded
                scope: sequence
                entity: seller
                ops:
                  - {type: aggregate, field: start_price, funcs: [max, min, count]}
              - name: similar
                scope: sequence
                entity: seller
                windows:
                  - {maxAge: P30D}
                  - {maxEvents: 20}
                ops:
                  - {type: aggregate, field: sold, funcs: [count, mean, std], weightBy: "exp(-abs(start_price - $self.start_price) / 100)"}
                  - {type: aggregate, field: start_price, funcs: [count, mean, sum, std], weightBy: "1", as: unit}
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
                  - {field: start_price, stats: [quantile, q25, quantile90]}
              - name: off
                scope: population
                type: encoding
                keySets:
                  - keys: [seller_id]
                    windows: [{maxAge: P60D}]
                offset: market
                shrinkage: {priorWeight: 1, scale: logit}
                targets:
                  - {field: sold, stats: [mean]}
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
        // the offset lattice's hidden statistics (Σ baseline and the count / sums taken over the same rows)
        Assertions.assertTrue(keyed.stream().anyMatch(c -> c.getCanonicalName().endsWith("__" + PopulationEvaluator.SUM_OFFSET)
                        && "market".equals(c.getCoordinates().get("offset"))),
                () -> "no offset statistic among " + keyed.stream().map(OutputColumn::getCanonicalName).toList());
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
            // the offset block's baseline, occasionally missing / NaN: such a row counts for no statistic of its level
            row.put("__baseline_market", switch (random.nextInt(10)) {
                case 0 -> null;
                case 1 -> Double.NaN;
                default -> 0.05 + random.nextDouble() * 0.9;
            });

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
     * {@code weightBy}: a weight of 1 reproduces the plain aggregate over the same window, a similarity kernel on the
     * current row matches a direct computation, and the weighted aggregate never runs on a running state — the
     * catalog declares it scan-only, so without a window it keeps the whole history (the unbounded hint).
     */
    @Test
    public void testWeightBy() {
        final JsonObject sources = Config.convertConfigJson(SOURCES, Config.Format.yaml);
        final FeaturePlan plan = FeaturePlanCompiler.compile(sources, Config.convertConfigJson(SPEC, Config.Format.yaml), null);
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        final List<OutputColumn> columns = plan.getColumns().stream().filter(c -> c.getScope() == FeatureSpec.Scope.sequence).toList();
        final SequenceEvaluator evaluator = new SequenceEvaluator(columns);
        evaluator.setup();
        final OutputColumn kernelMean = plan.getColumn("similar_30d_sold_mean");
        final OutputColumn kernelCount = plan.getColumn("similar_30d_sold_count");
        Assertions.assertNotNull(kernelMean, plan::describe);
        final long shift = kernelMean.getWindowShift() == null ? 0L : kernelMean.getWindowShift().toMillis();
        Assertions.assertTrue(shift > 0, "sold is an outcome: the weighted window is shifted like any other");

        final Random random = new Random(23);
        long millis = 1_700_000_000_000L;
        final List<SequenceEvaluator.Past> history = new ArrayList<>();
        final List<SequenceEvaluator.Past> pending = new ArrayList<>();
        long pendingMillis = Long.MIN_VALUE;
        final SequenceEvaluator.KeyState state = new SequenceEvaluator.KeyState();
        int weighted = 0, empty = 0;
        for (int i = 0; i < 600; i++) {
            if (random.nextDouble() > 0.15) millis += (long) (Math.pow(10, 4 + random.nextDouble() * 5));
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
            // a unit weight is the plain aggregate (the count as a double)
            for (final String func : List.of("count", "mean", "sum", "std")) {
                assertSame("unit " + func + "@" + i,
                        evaluator.evaluateColumn(plan.getColumn("seq_30d_start_price_" + func), row, millis, history, state),
                        evaluator.evaluateColumn(plan.getColumn("similar_30d_unit_" + func), row, millis, history, state));
            }
            // the kernel against a direct computation over the shifted 30-day window
            final Double self = (Double) row.get("start_price");
            double sumW = 0, sumWy = 0;
            int n = 0;
            for (final SequenceEvaluator.Past p : history) {
                if (p.millis() > millis - shift || p.millis() < millis - java.time.Duration.ofDays(30).toMillis()) continue;
                final Object y = p.values().get("sold"), x = p.values().get("start_price");
                if (y == null || x == null || self == null) continue;
                final double w = Math.exp(-Math.abs((Double) x - self) / 100);
                sumW += w;
                sumWy += w * ((Number) y).doubleValue();
                n++;
            }
            assertSame("kernel count@" + i, sumW, evaluator.evaluateColumn(kernelCount, row, millis, history, state));
            assertSame("kernel mean@" + i, n == 0 ? null : sumWy / sumW, evaluator.evaluateColumn(kernelMean, row, millis, history, state));
            if (n > 0) weighted++; else empty++;
            pending.add(new SequenceEvaluator.Past(millis, new HashMap<>(row)));
        }
        Assertions.assertTrue(weighted > 300 && empty > 30, "both the weighted and the empty case are exercised: " + weighted + " / " + empty);

        // declared scan-only: no summary family under a weight, so the same aggregate that folds incrementally
        // without a window (bounded) reads its whole history once it is weighted (unbounded)
        Assertions.assertNotNull(OperatorCatalog.summary("mean", false));
        Assertions.assertNull(OperatorCatalog.summary("mean", true));
        final String unwindowed = SPEC.replace("    windows:\n      - {maxAge: P30D}\n      - {maxEvents: 20}\n", "");
        Assertions.assertNotEquals(SPEC, unwindowed);
        final FeaturePlan open = FeaturePlanCompiler.compile(sources, Config.convertConfigJson(unwindowed, Config.Format.yaml), null);
        Assertions.assertFalse(open.getDiagnostics().hasErrors(), open::describe);
        Assertions.assertNull(SequenceEvaluator.unboundedReason(open.getColumn("unbounded_all_start_price_count")), "a plain count folds incrementally");
        final String reason = SequenceEvaluator.unboundedReason(open.getColumn("similar_all_sold_mean"));
        Assertions.assertNotNull(reason);
        Assertions.assertTrue(reason.contains("weightBy"), reason);
        Assertions.assertTrue(open.getDiagnostics().getMessages().stream().anyMatch(m -> m.code().equals("sequence.window.unbounded") && m.message().contains("similar_all_sold_mean")));
        // bounded by either window kind
        Assertions.assertNull(SequenceEvaluator.unboundedReason(plan.getColumn("similar_30d_sold_mean")));
        Assertions.assertNull(SequenceEvaluator.unboundedReason(plan.getColumn("similar_n20_sold_mean")));
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
        final Set<String> buffered = new LinkedHashSet<>(trimmedSequence.bufferedFields());
        buffered.addAll(trimmedPopulation.bufferedFields());
        final SequenceEvaluator.Watermarks watermarks = new SequenceEvaluator.Watermarks(buffered);
        trimmedSequence.register(watermarks);
        trimmedPopulation.register(watermarks);

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
                // the trimmed history owns its maps (the per-field trim mutates them; sharing would blind the oracle)
                for (final SequenceEvaluator.Past p : pending) trimmed.add(new SequenceEvaluator.Past(p.millis(), new HashMap<>(p.values())));
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
            watermarks.reset(trimmed.size());
            trimmedSequence.retainInto(tseq, millis, trimmed, watermarks);
            trimmedPopulation.retainInto(tpop, millis, trimmed, watermarks);
            trimmed.trim(watermarks);
            maxRetained = Math.max(maxRetained, trimmed.retained());
        }
        Assertions.assertEquals(full.size(), trimmed.size()); // absolute indices are preserved
        Assertions.assertTrue(trimmed.base() > 0, () -> "history was never trimmed; unbounded: " + trimmedSequence.unboundedColumns() + " / " + trimmedPopulation.unboundedColumns());
        final int retainedPeak = maxRetained;
        Assertions.assertTrue(retainedPeak < full.size() / 2, () -> "retained " + retainedPeak + " of " + full.size());
        Assertions.assertThrows(IndexOutOfBoundsException.class, () -> trimmed.get(0));
    }

    /**
     * Hand-checked values of the two-series and the fractional-difference ops on a short history (x = 1..6,
     * y = 2x + 1 with y of the third event missing): the same-event regression is the exact line, the lagged one pairs
     * y with x two events earlier, fracdiff applies (1 − B)^d truncated to k terms — d = 1 is the first difference.
     */
    @Test
    public void testRegressionAndFracdiffValues() {
        final JsonObject sources = Config.convertConfigJson(SOURCES, Config.Format.yaml);
        final String spec = """
                lineage:
                  - {fields: [session_id, seller_id, condition_grade, start_price, sold], from: listings}
                time: {field: session_time}
                predictAt: "event_time - PT10M"
                entities:
                  - {name: seller, keys: [seller_id]}
                features:
                  - name: pair
                    scope: sequence
                    entity: seller
                    ops:
                      - {type: regression, field: sold, against: start_price, funcs: [cov, corr, beta, intercept, r2]}
                      - {type: regression, field: sold, against: start_price, lag: 2, funcs: [beta, intercept]}
                      - {type: fracdiff, field: start_price, d: 1, k: 3}
                      - {type: fracdiff, field: start_price, d: 0.5, k: 4}
                """;
        final FeaturePlan plan = FeaturePlanCompiler.compile(sources, Config.convertConfigJson(spec, Config.Format.yaml), null);
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        final List<OutputColumn> columns = plan.getColumns().stream().filter(c -> c.getScope() == FeatureSpec.Scope.sequence).toList();
        final SequenceEvaluator evaluator = new SequenceEvaluator(columns);
        evaluator.setup();
        // sold's window shift does not matter here: the row is evaluated far after the history
        final List<SequenceEvaluator.Past> history = new ArrayList<>();
        final long base = 1_700_000_000_000L;
        for (int i = 1; i <= 6; i++) {
            final Map<String, Object> values = new HashMap<>();
            values.put("start_price", (double) i);
            values.put("sold", i == 3 ? null : 2.0 * i + 1);
            history.add(new SequenceEvaluator.Past(base + i * 1000L, values));
        }
        final long now = base + 100L * 86_400_000L;
        final Map<String, Object> row = new HashMap<>(Map.of("seller_id", "s1"));
        final SequenceEvaluator.KeyState state = new SequenceEvaluator.KeyState();
        final java.util.function.Function<String, Object> read = name -> evaluator.evaluateColumn(plan.getColumn(name), row, now, history, state);
        // x ∈ {1, 2, 4, 5, 6}: mean 3.6, var = 3.44; y = 2x + 1 exactly
        assertSame("cov", 2 * 3.44, read.apply("pair_all_sold_vs_start_price_cov"));
        assertSame("beta", 2.0, read.apply("pair_all_sold_vs_start_price_beta"));
        assertSame("intercept", 1.0, read.apply("pair_all_sold_vs_start_price_intercept"));
        assertSame("corr", 1.0, read.apply("pair_all_sold_vs_start_price_corr"));
        assertSame("r2", 1.0, read.apply("pair_all_sold_vs_start_price_r2"));
        // lag 2: (x_{i−2}, y_i) for i = 4, 5, 6 (i = 3 has no y) → x = 2, 3, 4 and y = 9, 11, 13 = 2x + 5
        assertSame("lag beta", 2.0, read.apply("pair_all_sold_vs_start_price_lag2_beta"));
        assertSame("lag intercept", 5.0, read.apply("pair_all_sold_vs_start_price_lag2_intercept"));
        // d = 1: x_6 − x_5 (the third weight is 0); d = 0.5: w = (1, −0.5, −0.125, −0.0625) over x = 6, 5, 4, 3
        assertSame("diff", 1.0, read.apply("pair_all_start_price_fracdiff1"));
        assertSame("fracdiff", 6 - 0.5 * 5 - 0.125 * 4 - 0.0625 * 3, read.apply("pair_all_start_price_fracdiff0p5"));
        Assertions.assertArrayEquals(new double[]{1, -0.5, -0.125, -0.0625}, SequenceEvaluator.fracdiffWeights(0.5, 4), 1e-15);
        // the scan path gives the same numbers, and a history shorter than k (or with a hole in it) has no fracdiff
        assertSame("scan beta", 2.0, evaluator.evaluateColumn(plan.getColumn("pair_all_sold_vs_start_price_beta"), row, now, history, null));
        Assertions.assertNull(evaluator.evaluateColumn(plan.getColumn("pair_all_start_price_fracdiff0p5"), row, now, history.subList(0, 3), null));
        history.get(4).values().put("start_price", null);
        Assertions.assertNull(evaluator.evaluateColumn(plan.getColumn("pair_all_start_price_fracdiff0p5"), row, now, history, null));

        // the same-event regression folds incrementally (bounded without a window); the lagged pairing scans, its tail
        // is bounded by maxEvents only; fracdiff reads its last k events
        Assertions.assertNull(SequenceEvaluator.unboundedReason(plan.getColumn("pair_all_sold_vs_start_price_beta")));
        Assertions.assertNotNull(SequenceEvaluator.unboundedReason(plan.getColumn("pair_all_sold_vs_start_price_lag2_beta")));
        Assertions.assertNull(SequenceEvaluator.unboundedReason(plan.getColumn("pair_all_start_price_fracdiff0p5")));
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
                      - {type: fracdiff, field: start_price, d: 0.4, k: 8}
                  - name: leadlag
                    scope: sequence
                    entity: seller
                    windows: [{maxEvents: 12}]
                    ops:
                      - {type: regression, field: sold, against: start_price, lag: 2, funcs: [corr, beta]}
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
        final SequenceEvaluator.Watermarks watermarks = new SequenceEvaluator.Watermarks(trimmedEvaluator.bufferedFields());
        trimmedEvaluator.register(watermarks);

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
            row.put("sold", random.nextInt(2));
            if (millis != pendingMillis) {
                history.addAll(pending);
                // the trimmed history owns its maps (the per-field trim mutates them; sharing would blind the oracle)
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
            watermarks.reset(trimmed.size());
            trimmedEvaluator.retainInto(trimmedState, millis, trimmed, watermarks);
            trimmed.trim(watermarks);
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
        final SequenceEvaluator.Watermarks watermarks = new SequenceEvaluator.Watermarks(trimmedEvaluator.bufferedFields());
        trimmedEvaluator.register(watermarks);

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
            watermarks.reset(trimmed.size());
            trimmedEvaluator.retainInto(trimmedState, millis, trimmed, watermarks);
            trimmed.trim(watermarks);
        }
        // no entry is dropped (the filtered window reads them all) ...
        Assertions.assertEquals(0, trimmed.base());
        Assertions.assertEquals(history.size(), trimmed.retained());
        // ... but the bounded column's field is gone from the old entries while the unbounded one's stays
        final SequenceEvaluator.Past oldest = trimmed.get(0);
        Assertions.assertTrue(oldest.values().containsKey("condition_grade") && oldest.values().containsKey("sold"), oldest::toString);
        Assertions.assertFalse(oldest.values().containsKey("start_price"), oldest::toString);
        // entity keys are self reads: the engine never projects them into the history
        Assertions.assertFalse(trimmedEvaluator.bufferedFields().contains("seller_id"), () -> trimmedEvaluator.bufferedFields().toString());
        int withPrice = 0;
        for (int i = 0; i < trimmed.size(); i++) if (trimmed.get(i).values().containsKey("start_price")) withPrice++;
        Assertions.assertTrue(withPrice < trimmed.size() / 4, () -> "entries still holding start_price: " + trimmed.size());
    }
}
