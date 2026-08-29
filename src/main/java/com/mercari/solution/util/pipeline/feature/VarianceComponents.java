package com.mercari.solution.util.pipeline.feature;

import com.mercari.solution.module.MElement;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

/**
 * Batch estimation of the per-level pseudo-counts for {@code weights: varianceComponents}
 * (work-feature.md §5.5 rule 3): for every lattice level that a stage composes, per-key sufficient
 * statistics of the target are combined into the one-way random-effects moments and
 * {@link Shrinkage#lambdaFromMoments} turns them into λ = σ²/τ². The result is a side input
 * {@code Map<levelNColumn, λ>} read by the composing DoFn.
 *
 * <p>The estimate uses the whole batch (not the expanding prefix): it is a hyper-parameter, which the spec
 * classifies as structural rather than value leakage (§6.3).
 */
public final class VarianceComponents {

    private static final Logger LOG = LoggerFactory.getLogger(VarianceComponents.class);
    private static final char SEPARATOR = (char) 1;

    private VarianceComponents() {}

    /** One level whose λ is estimated: keyed by the level's hidden {@code n} column name. */
    public record LevelSpec(String id, List<String> keys, String field, String offsetColumn) implements Serializable {}

    /** Levels referenced by the composed columns of a stage that declare variance-components weights. */
    public static List<LevelSpec> specsOf(final List<OutputColumn> stageColumns, final Map<String, OutputColumn> allColumns) {
        final Map<String, LevelSpec> specs = new LinkedHashMap<>();
        for (final OutputColumn c : stageColumns) {
            if (!"varianceComponents".equals(c.getCoordinates().get("weights"))) continue;
            collect(Shrinkage.parseLevels(c.getCoordinates().get("levels")), allColumns, specs);
        }
        return new ArrayList<>(specs.values());
    }

    private static void collect(final List<Shrinkage.Level> levels, final Map<String, OutputColumn> allColumns,
                                final Map<String, LevelSpec> specs) {
        for (final Shrinkage.Level level : levels) {
            if (level.mainEffects() != null) {
                for (final List<Shrinkage.Level> main : level.mainEffects()) collect(main, allColumns, specs);
                continue;
            }
            final OutputColumn hidden = allColumns.get(level.nColumn());
            if (hidden == null || specs.containsKey(level.nColumn())) continue;
            final String keys = hidden.getCoordinates().get("keys");
            if (keys == null || keys.isEmpty()) continue; // the global level is the root: no shrinkage weight
            final String field = hidden.getCoordinates().get("field");
            if (field == null) continue;
            final String offset = hidden.getCoordinates().containsKey("offset") ? "__baseline_" + hidden.getCoordinates().get("offset") : null;
            specs.put(level.nColumn(), new LevelSpec(level.nColumn(), List.of(keys.split(",")), field, offset));
        }
    }

    public static PCollectionView<Map<String, Double>> estimate(final PCollection<MElement> input, final List<LevelSpec> specs, final String label) {
        return lambdasFromKeyStats(perKeyStats(input, specs, label), label);
    }

    /** Per-key sufficient statistics of every level: {@code KV<levelId + (char)1 + key, stats>}. */
    public static PCollection<KV<String, KeyStats>> perKeyStats(final PCollection<MElement> input, final List<LevelSpec> specs, final String label) {
        return input
                .apply(label + "_Values", ParDo.of(new ExtractDoFn(specs)))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), DoubleCoder.of()))
                .apply(label + "_PerKey", Combine.perKey(new KeyStatsFn()))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(KeyStats.class)));
    }

    /** In-memory counterpart of {@link #lambdasFromKeyStats} for statistics loaded from an artifact. */
    public static Map<String, Double> lambdasInMemory(final Map<String, KeyStats> stats) {
        final Map<String, Moments> moments = new HashMap<>();
        final MomentsFn fn = new MomentsFn();
        for (final Map.Entry<String, KeyStats> e : stats.entrySet()) {
            final String level = e.getKey().substring(0, e.getKey().indexOf(SEPARATOR));
            moments.put(level, fn.addInput(moments.getOrDefault(level, new Moments()), e.getValue()));
        }
        final Map<String, Double> lambdas = new HashMap<>();
        for (final Map.Entry<String, Moments> e : moments.entrySet()) {
            final Moments m = e.getValue();
            final Double lambda = Shrinkage.lambdaFromMoments(m.keys, m.n, m.sum, m.sumSq, m.sumSqOverN, m.sumNSq);
            if (lambda != null) lambdas.put(e.getKey(), lambda);
        }
        return lambdas;
    }

    public static PCollectionView<Map<String, Double>> lambdasFromKeyStats(final PCollection<KV<String, KeyStats>> perKey, final String label) {
        return perKey
                .apply(label + "_PerLevel", ParDo.of(new DoFn<KV<String, KeyStats>, KV<String, KeyStats>>() {
                    @ProcessElement
                    public void processElement(final ProcessContext c) {
                        final String composite = c.element().getKey();
                        c.output(KV.of(composite.substring(0, composite.indexOf(SEPARATOR)), c.element().getValue()));
                    }
                }))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(KeyStats.class)))
                .apply(label + "_Moments", Combine.perKey(new MomentsFn()))
                .apply(label + "_Lambda", ParDo.of(new DoFn<KV<String, Moments>, KV<String, Double>>() {
                    @ProcessElement
                    public void processElement(final ProcessContext c) {
                        final Moments m = c.element().getValue();
                        final Double lambda = Shrinkage.lambdaFromMoments(m.keys, m.n, m.sum, m.sumSq, m.sumSqOverN, m.sumNSq);
                        if (lambda == null) {
                            LOG.info("varianceComponents {}: too few keys/rows (K={}, N={}); using priorWeight", c.element().getKey(), m.keys, m.n);
                            return;
                        }
                        if (Double.isInfinite(lambda)) {
                            LOG.info("varianceComponents {}: between-key variance truncated to 0 (no signal at this level, full shrinkage)", c.element().getKey());
                        } else {
                            LOG.info("varianceComponents {}: lambda={} (K={}, N={})", c.element().getKey(), lambda, m.keys, m.n);
                        }
                        c.output(KV.of(c.element().getKey(), lambda));
                    }
                }))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), DoubleCoder.of()))
                .apply(label + "_View", View.asMap());
    }

    static class ExtractDoFn extends DoFn<MElement, KV<String, Double>> {
        private final List<LevelSpec> specs;

        ExtractDoFn(final List<LevelSpec> specs) {
            this.specs = specs;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final MElement element = c.element();
            if (element == null) return;
            final Map<String, Object> row = element.asPrimitiveMap();
            for (final LevelSpec spec : specs) {
                // a level without a target counts rows: contribute y = 0 so n is tracked
                Double y = spec.field() == null ? 0d : FeatureValues.toDouble(row.get(spec.field()));
                if (y == null) continue;
                if (spec.offsetColumn() != null) {
                    final Double b = FeatureValues.toDouble(row.get(spec.offsetColumn()));
                    if (b == null) continue;
                    y -= b;
                }
                final String key = FeatureValues.key(row, spec.keys());
                if (key == null) continue;
                c.output(KV.of(spec.id() + SEPARATOR + key, y));
            }
        }
    }

    /** Per-key sufficient statistics (n, Σy, Σy²). */
    public static class KeyStats implements Serializable {
        double n, sum, sumSq;
    }

    static class KeyStatsFn extends Combine.CombineFn<Double, KeyStats, KeyStats> {
        @Override
        public KeyStats createAccumulator() { return new KeyStats(); }

        @Override
        public KeyStats addInput(final KeyStats acc, final Double y) {
            acc.n += 1;
            acc.sum += y;
            acc.sumSq += y * y;
            return acc;
        }

        @Override
        public KeyStats mergeAccumulators(final Iterable<KeyStats> accs) {
            final KeyStats out = new KeyStats();
            for (final KeyStats a : accs) {
                out.n += a.n;
                out.sum += a.sum;
                out.sumSq += a.sumSq;
            }
            return out;
        }

        @Override
        public KeyStats extractOutput(final KeyStats acc) { return acc; }

        @Override
        public Coder<KeyStats> getAccumulatorCoder(final CoderRegistry registry, final Coder<Double> inputCoder) {
            return SerializableCoder.of(KeyStats.class);
        }
    }

    /** Level-wide moments over keys (inputs of {@link Shrinkage#lambdaFromMoments}). */
    public static class Moments implements Serializable {
        long keys;
        double n, sum, sumSq, sumSqOverN, sumNSq;
    }

    static class MomentsFn extends Combine.CombineFn<KeyStats, Moments, Moments> {
        @Override
        public Moments createAccumulator() { return new Moments(); }

        @Override
        public Moments addInput(final Moments acc, final KeyStats k) {
            if (k.n <= 0) return acc;
            acc.keys += 1;
            acc.n += k.n;
            acc.sum += k.sum;
            acc.sumSq += k.sumSq;
            acc.sumSqOverN += k.sum * k.sum / k.n;
            acc.sumNSq += k.n * k.n;
            return acc;
        }

        @Override
        public Moments mergeAccumulators(final Iterable<Moments> accs) {
            final Moments out = new Moments();
            for (final Moments a : accs) {
                out.keys += a.keys;
                out.n += a.n;
                out.sum += a.sum;
                out.sumSq += a.sumSq;
                out.sumSqOverN += a.sumSqOverN;
                out.sumNSq += a.sumNSq;
            }
            return out;
        }

        @Override
        public Moments extractOutput(final Moments acc) { return acc; }

        @Override
        public Coder<Moments> getAccumulatorCoder(final CoderRegistry registry, final Coder<KeyStats> inputCoder) {
            return SerializableCoder.of(Moments.class);
        }
    }

}
