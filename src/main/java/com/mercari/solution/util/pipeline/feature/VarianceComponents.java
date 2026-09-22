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
 * (docs/design/feature-dsl.md §5.5 rule 3): for every lattice level that a stage composes, per-key sufficient
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
    /**
     * One lattice level whose per-key sufficient statistics are fitted; {@code foldKeys} (with {@code folds})
     * additionally tags every contribution with the row's fold so out-of-fold statistics can be derived by
     * subtraction ({@code fit.mode: fold}).
     */
    public record LevelSpec(String id, List<String> keys, String field, String offsetColumn, List<String> foldKeys, int folds,
                            String scoreScale) implements Serializable {
        public LevelSpec(final String id, final List<String> keys, final String field, final String offsetColumn) {
            this(id, keys, field, offsetColumn, null, 0, null);
        }

        public LevelSpec(final String id, final List<String> keys, final String field, final String offsetColumn, final List<String> foldKeys, final int folds) {
            this(id, keys, field, offsetColumn, foldKeys, folds, null);
        }
    }

    /**
     * The levels composed on the score scale (an offset term on logit / log — the {@code scoreScale} coordinate of their
     * hidden columns): level id → scale name. Their λ is {@link Shrinkage#lambdaFromScore} instead of the identity moments.
     */
    public static Map<String, String> scoreScalesOf(final List<LevelSpec> specs) {
        final Map<String, String> scales = new HashMap<>();
        for (final LevelSpec spec : specs) if (spec.scoreScale() != null) scales.put(spec.id(), spec.scoreScale());
        return scales;
    }

    /** The λ of one level from its moments: on the score scale for a score level, the identity moments otherwise. */
    static Double lambdaOf(final Moments m, final String scoreScale) {
        if (scoreScale == null) return Shrinkage.lambdaFromMoments(m.keys, m.n, m.sum, m.sumSq, m.sumSqOverN, m.sumNSq);
        return switch (scoreScale) {
            case "logit" -> Shrinkage.lambdaFromScore(m.keysLogit, m.sumVLogit, m.sumV2Logit, m.sumSLogit, m.sumS2OverVLogit);
            case "log" -> Shrinkage.lambdaFromScore(m.keysLog, m.sumVLog, m.sumV2Log, m.sumSLog, m.sumS2OverVLog);
            default -> Shrinkage.lambdaFromMoments(m.keys, m.n, m.sum, m.sumSq, m.sumSqOverN, m.sumNSq);
        };
    }

    /** Prefix of the per-fold entries in the per-key statistics map: {@code #<fold>} + separator + level entry. */
    static final String FOLD_PREFIX = "#";

    /**
     * One level of {@code fit.mode: forward}: per-key statistics are combined per time block ({@code blocks} of the
     * row's {@code timeField}) and turned into a cumulative {@link ForwardBlocks.Series} per (level, key).
     */
    public record ForwardSpec(String id, List<String> keys, String field, String offsetColumn, ForwardBlocks blocks,
                              String timeField, String timeFieldType) implements Serializable {}

    /**
     * fit.mode forward: {@code KV<levelId + (char)1 + key, Series>} — the cumulative sufficient statistics of every
     * (level, key) over its blocks. Two Combines (per (key, block), then the per-key prefix over at most one entry
     * per block) instead of one time-ordered replay per key.
     */
    public static PCollection<KV<String, ForwardBlocks.Series>> forwardSeries(final PCollection<MElement> input, final List<ForwardSpec> specs, final String label) {
        return input
                .apply(label + "_Values", ParDo.of(new ForwardExtractDoFn(specs)))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), valueCoder()))
                .apply(label + "_PerBlock", Combine.perKey(new KeyStatsFn()))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(KeyStats.class)))
                .apply(label + "_ByKey", ParDo.of(new DoFn<KV<String, KeyStats>, KV<String, KV<Long, KeyStats>>>() {
                    @ProcessElement
                    public void processElement(final ProcessContext c) {
                        final String composite = c.element().getKey();
                        final int at = composite.lastIndexOf(SEPARATOR);
                        c.output(KV.of(composite.substring(0, at), KV.of(Long.parseLong(composite.substring(at + 1)), c.element().getValue())));
                    }
                }))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), KvCoder.of(org.apache.beam.sdk.coders.VarLongCoder.of(), SerializableCoder.of(KeyStats.class))))
                .apply(label + "_Group", org.apache.beam.sdk.transforms.GroupByKey.create())
                .apply(label + "_Series", ParDo.of(new DoFn<KV<String, Iterable<KV<Long, KeyStats>>>, KV<String, ForwardBlocks.Series>>() {
                    @ProcessElement
                    public void processElement(final ProcessContext c) {
                        final List<KV<Long, KeyStats>> entries = new ArrayList<>();
                        for (final KV<Long, KeyStats> e : c.element().getValue()) entries.add(e);
                        entries.sort(Comparator.comparingLong(KV::getKey));
                        final long[] blocks = new long[entries.size()];
                        final double[] n = new double[entries.size()], sum = new double[entries.size()], sumSq = new double[entries.size()],
                                sumOff = new double[entries.size()], sumInfo = new double[entries.size()];
                        double cn = 0, cs = 0, cq = 0, co = 0, ci = 0;
                        for (int i = 0; i < entries.size(); i++) {
                            final KeyStats s = entries.get(i).getValue();
                            cn += s.n;
                            cs += s.sum;
                            cq += s.sumSq;
                            co += s.sumOff;
                            ci += s.sumInfo;
                            blocks[i] = entries.get(i).getKey();
                            n[i] = cn;
                            sum[i] = cs;
                            sumSq[i] = cq;
                            sumOff[i] = co;
                            sumInfo[i] = ci;
                        }
                        c.output(KV.of(c.element().getKey(), new ForwardBlocks.Series(blocks, n, sum, sumSq, sumOff, sumInfo)));
                    }
                }))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(ForwardBlocks.Series.class)));
    }

    /** The whole-input totals of every forward series (what a static artifact holds). */
    public static Map<String, KeyStats> forwardTotals(final Map<String, ForwardBlocks.Series> series) {
        final Map<String, KeyStats> totals = new HashMap<>();
        for (final Map.Entry<String, ForwardBlocks.Series> e : series.entrySet()) {
            final KeyStats t = e.getValue().totals();
            if (t != null) totals.put(e.getKey(), t);
        }
        return totals;
    }

    /**
     * fit.mode forward: λ per level and block — the moments over the keys' cumulative statistics up to each block
     * (what a row reading that block shrinks with). Levels with too few keys at a block have no entry there.
     */
    public static Map<String, TreeMap<Long, Double>> lambdasByBlock(final Map<String, ForwardBlocks.Series> series) {
        return lambdasByBlock(series, Map.of());
    }

    /** @param scoreScales the score levels ({@link #scoreScalesOf}): their λ is estimated on the score scale */
    public static Map<String, TreeMap<Long, Double>> lambdasByBlock(final Map<String, ForwardBlocks.Series> series, final Map<String, String> scoreScales) {
        final Map<String, List<ForwardBlocks.Series>> byLevel = new HashMap<>();
        final Map<String, TreeSet<Long>> blocksByLevel = new HashMap<>();
        for (final Map.Entry<String, ForwardBlocks.Series> e : series.entrySet()) {
            final String level = e.getKey().substring(0, e.getKey().indexOf(SEPARATOR));
            byLevel.computeIfAbsent(level, l -> new ArrayList<>()).add(e.getValue());
            final TreeSet<Long> blocks = blocksByLevel.computeIfAbsent(level, l -> new TreeSet<>());
            for (int i = 0; i < e.getValue().size(); i++) blocks.add(e.getValue().blockAt(i));
        }
        final MomentsFn fn = new MomentsFn();
        final Map<String, TreeMap<Long, Double>> lambdas = new HashMap<>();
        for (final Map.Entry<String, List<ForwardBlocks.Series>> e : byLevel.entrySet()) {
            final TreeMap<Long, Double> perBlock = new TreeMap<>();
            for (final long block : blocksByLevel.get(e.getKey())) {
                Moments m = new Moments();
                for (final ForwardBlocks.Series s : e.getValue()) {
                    final KeyStats stats = s.statsBetween(-1, s.floor(block));
                    if (stats != null) m = fn.addInput(m, stats);
                }
                final Double lambda = lambdaOf(m, scoreScales.get(e.getKey()));
                if (lambda != null) perBlock.put(block, lambda);
            }
            lambdas.put(e.getKey(), perBlock);
        }
        return lambdas;
    }

    /** The λ per block of one forward level, as {@link #lambdasByBlockView} computes it in the pipeline. */
    public static class LevelLambdas implements Serializable {
        final String level;
        final TreeMap<Long, Double> byBlock;

        LevelLambdas(final String level, final TreeMap<Long, Double> byBlock) {
            this.level = level;
            this.byBlock = byBlock;
        }
    }

    /** The map {@link #lambdasByBlock(Map)} builds, assembled from the pipeline's per-level results. */
    public static Map<String, TreeMap<Long, Double>> lambdasByBlock(final Iterable<LevelLambdas> levels) {
        final Map<String, TreeMap<Long, Double>> lambdas = new HashMap<>();
        for (final LevelLambdas l : levels) lambdas.put(l.level, l.byBlock);
        return lambdas;
    }

    /**
     * fit.mode forward: the λ per (level, block) of {@link #lambdasByBlock(Map)} computed inside the pipeline — one
     * Combine per level over the series ({@link BlockMomentsFn}: the level-wide moments at every block, kept as a
     * step function over the keys' blocks) — gathered into one small side input (levels × blocks). No DoFn scans
     * the series to derive λ (a scan of a map side input is one state fetch per entry on a portable runner).
     */
    public static PCollectionView<List<LevelLambdas>> lambdasByBlockView(final PCollection<KV<String, ForwardBlocks.Series>> series,
                                                                         final Map<String, String> scoreScales, final String label) {
        final Map<String, String> scales = new HashMap<>(scoreScales); // a serializable copy for the DoFn
        return series
                .apply(label + "_PerLevel", ParDo.of(new DoFn<KV<String, ForwardBlocks.Series>, KV<String, ForwardBlocks.Series>>() {
                    @ProcessElement
                    public void processElement(final ProcessContext c) {
                        c.output(KV.of(FitArtifact.levelOf(c.element().getKey()), c.element().getValue()));
                    }
                }))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(ForwardBlocks.Series.class)))
                .apply(label + "_BlockMoments", Combine.perKey(new BlockMomentsFn()))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(BlockMoments.class)))
                .apply(label + "_Lambda", ParDo.of(new DoFn<KV<String, BlockMoments>, LevelLambdas>() {
                    @ProcessElement
                    public void processElement(final ProcessContext c) {
                        final TreeMap<Long, Double> perBlock = new TreeMap<>();
                        for (final Map.Entry<Long, Moments> e : c.element().getValue().byBlock.entrySet()) {
                            final Double lambda = lambdaOf(e.getValue(), scales.get(c.element().getKey()));
                            if (lambda != null) perBlock.put(e.getKey(), lambda);
                        }
                        LOG.info("varianceComponents {} (forward): lambda estimated at {} of {} blocks",
                                c.element().getKey(), perBlock.size(), c.element().getValue().byBlock.size());
                        c.output(new LevelLambdas(c.element().getKey(), perBlock));
                    }
                }))
                .setCoder(SerializableCoder.of(LevelLambdas.class))
                .apply(label + "_View", View.asList());
    }

    /**
     * Level-wide moments over the keys' cumulative statistics, at every block the level's keys touch: a step function
     * over blocks (the value at block {@code b} is the entry at the last block ≤ {@code b}; nothing before the first).
     */
    public static class BlockMoments implements Serializable {
        TreeMap<Long, Moments> byBlock = new TreeMap<>();
    }

    /** One key's contribution: its prefix statistics at each of its own blocks (the same step a merge unions). */
    static TreeMap<Long, Moments> blockMoments(final ForwardBlocks.Series series) {
        final MomentsFn fn = new MomentsFn();
        final TreeMap<Long, Moments> step = new TreeMap<>();
        for (int i = 0; i < series.size(); i++) {
            final KeyStats stats = series.statsBetween(-1, i);
            if (stats != null) step.put(series.blockAt(i), fn.addInput(new Moments(), stats));
        }
        return step;
    }

    /** Sum of two step functions: the union of their blocks, each block adding the two values in force there. */
    static TreeMap<Long, Moments> mergeBlockMoments(final TreeMap<Long, Moments> a, final TreeMap<Long, Moments> b) {
        if (a.isEmpty()) return b;
        if (b.isEmpty()) return a;
        final TreeSet<Long> blocks = new TreeSet<>(a.keySet());
        blocks.addAll(b.keySet());
        final MomentsFn fn = new MomentsFn();
        final TreeMap<Long, Moments> out = new TreeMap<>();
        for (final long block : blocks) {
            final List<Moments> parts = new ArrayList<>(2);
            final Map.Entry<Long, Moments> fa = a.floorEntry(block), fb = b.floorEntry(block);
            if (fa != null) parts.add(fa.getValue());
            if (fb != null) parts.add(fb.getValue());
            out.put(block, fn.mergeAccumulators(parts));
        }
        return out;
    }

    static class BlockMomentsFn extends Combine.CombineFn<ForwardBlocks.Series, BlockMoments, BlockMoments> {
        @Override
        public BlockMoments createAccumulator() { return new BlockMoments(); }

        @Override
        public BlockMoments addInput(final BlockMoments acc, final ForwardBlocks.Series series) {
            acc.byBlock = mergeBlockMoments(acc.byBlock, blockMoments(series));
            return acc;
        }

        @Override
        public BlockMoments mergeAccumulators(final Iterable<BlockMoments> accs) {
            final BlockMoments out = new BlockMoments();
            for (final BlockMoments a : accs) out.byBlock = mergeBlockMoments(out.byBlock, a.byBlock);
            return out;
        }

        @Override
        public BlockMoments extractOutput(final BlockMoments acc) { return acc; }

        @Override
        public Coder<BlockMoments> getAccumulatorCoder(final CoderRegistry registry, final Coder<ForwardBlocks.Series> inputCoder) {
            return SerializableCoder.of(BlockMoments.class);
        }
    }

    static class ForwardExtractDoFn extends DoFn<MElement, KV<String, KV<Double, Double>>> {
        private final List<ForwardSpec> specs;

        ForwardExtractDoFn(final List<ForwardSpec> specs) {
            this.specs = specs;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final MElement element = c.element();
            if (element == null) return;
            final Map<String, Object> row = element.asPrimitiveMap();
            for (final ForwardSpec spec : specs) {
                final KV<Double, Double> value = FeatureValues.offsetTarget(row, spec.field(), spec.offsetColumn());
                if (value == null) continue;
                final String key = FeatureValues.key(row, spec.keys());
                if (key == null) continue;
                final Long millis = FeatureValues.toEpochMillis(row.get(spec.timeField()), spec.timeFieldType());
                if (millis == null) continue;
                c.output(KV.of(spec.id() + SEPARATOR + key + SEPARATOR + spec.blocks().indexOf(millis), value));
            }
        }
    }

    /** Deterministic fold of a fold-unit key (Java's String hash is stable across JVMs). */
    public static int foldOf(final String unitKey, final int folds) {
        return Math.floorMod(unitKey.hashCode(), folds);
    }

    static String foldEntry(final int fold, final String entry) {
        return FOLD_PREFIX + fold + SEPARATOR + entry;
    }

    static boolean isFoldEntry(final String entry) {
        return entry.startsWith(FOLD_PREFIX);
    }

    /** {@code total − part} (part = the row's own fold); null when nothing remains. */
    static KeyStats subtract(final KeyStats total, final KeyStats part) {
        if (total == null) return null;
        if (part == null) return total;
        final KeyStats out = new KeyStats();
        out.n = total.n - part.n;
        out.sum = total.sum - part.sum;
        out.sumSq = total.sumSq - part.sumSq;
        out.sumOff = total.sumOff - part.sumOff;
        out.sumInfo = total.sumInfo - part.sumInfo;
        return out.n <= 0 ? null : out;
    }

    /** Levels referenced by the composed columns of a stage that declare variance-components weights. */
    public static List<LevelSpec> specsOf(final List<OutputColumn> stageColumns, final Map<String, OutputColumn> allColumns) {
        final Map<String, LevelSpec> specs = new LinkedHashMap<>();
        for (final OutputColumn c : stageColumns) {
            // joint columns estimate their pseudo-counts inside the fit's solve (no hidden levels to combine here)
            if (!"varianceComponents".equals(c.getCoordinates().get("weights")) || !c.getCoordinates().containsKey("levels")) continue;
            collect(Shrinkage.parseLevels(c.getCoordinates().get("levels")), allColumns, specs);
        }
        return new ArrayList<>(specs.values());
    }

    private static void collect(final List<Shrinkage.Level> levels, final Map<String, OutputColumn> allColumns,
                                final Map<String, LevelSpec> specs) {
        for (final Shrinkage.Level level : Shrinkage.leaves(levels)) {
            final OutputColumn hidden = allColumns.get(level.nColumn());
            if (hidden == null || specs.containsKey(level.nColumn())) continue;
            final String keys = hidden.getCoordinates().get("keys");
            if (keys == null || keys.isEmpty()) continue; // the global level is the root: no shrinkage weight
            final String field = hidden.getCoordinates().get("field");
            if (field == null) continue;
            final String offset = hidden.getCoordinates().containsKey("offset") ? "__baseline_" + hidden.getCoordinates().get("offset") : null;
            specs.put(level.nColumn(), new LevelSpec(level.nColumn(), List.of(keys.split(",")), field, offset, null, 0, hidden.getCoordinates().get("scoreScale")));
        }
    }

    public static PCollectionView<Map<String, Double>> estimate(final PCollection<MElement> input, final List<LevelSpec> specs, final String label) {
        return lambdasFromKeyStats(perKeyStats(input, specs, label), scoreScalesOf(specs), label);
    }

    /** Per-key sufficient statistics of every level: {@code KV<levelId + (char)1 + key, stats>}. */
    public static PCollection<KV<String, KeyStats>> perKeyStats(final PCollection<MElement> input, final List<LevelSpec> specs, final String label) {
        return input
                .apply(label + "_Values", ParDo.of(new ExtractDoFn(specs)))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), valueCoder()))
                .apply(label + "_PerKey", Combine.perKey(new KeyStatsFn()))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(KeyStats.class)));
    }

    /** In-memory counterpart of {@link #lambdasFromKeyStats} for statistics loaded from an artifact. */
    public static Map<String, Double> lambdasInMemory(final Map<String, KeyStats> stats) {
        return lambdasInMemory(stats, Map.of());
    }

    /** @param scoreScales the score levels ({@link #scoreScalesOf}): their λ is estimated on the score scale */
    public static Map<String, Double> lambdasInMemory(final Map<String, KeyStats> stats, final Map<String, String> scoreScales) {
        final Map<String, Moments> moments = new HashMap<>();
        final MomentsFn fn = new MomentsFn();
        for (final Map.Entry<String, KeyStats> e : stats.entrySet()) {
            final String level = e.getKey().substring(0, e.getKey().indexOf(SEPARATOR));
            moments.put(level, fn.addInput(moments.getOrDefault(level, new Moments()), e.getValue()));
        }
        final Map<String, Double> lambdas = new HashMap<>();
        for (final Map.Entry<String, Moments> e : moments.entrySet()) {
            final Double lambda = lambdaOf(e.getValue(), scoreScales.get(e.getKey()));
            if (lambda != null) lambdas.put(e.getKey(), lambda);
        }
        return lambdas;
    }

    /** @param scoreScales the score levels ({@link #scoreScalesOf}): their λ is estimated on the score scale (1 / τ²) */
    public static PCollectionView<Map<String, Double>> lambdasFromKeyStats(final PCollection<KV<String, KeyStats>> perKey,
                                                                           final Map<String, String> scoreScales, final String label) {
        final Map<String, String> scales = new HashMap<>(scoreScales); // a serializable copy for the DoFn
        return perKey
                .apply(label + "_PerLevel", ParDo.of(new DoFn<KV<String, KeyStats>, KV<String, KeyStats>>() {
                    @ProcessElement
                    public void processElement(final ProcessContext c) {
                        final String composite = c.element().getKey();
                        if (isFoldEntry(composite)) return; // per-fold tags are not keys of the level
                        c.output(KV.of(composite.substring(0, composite.indexOf(SEPARATOR)), c.element().getValue()));
                    }
                }))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(KeyStats.class)))
                .apply(label + "_Moments", Combine.perKey(new MomentsFn()))
                .apply(label + "_Lambda", ParDo.of(new DoFn<KV<String, Moments>, KV<String, Double>>() {
                    @ProcessElement
                    public void processElement(final ProcessContext c) {
                        final Moments m = c.element().getValue();
                        final Double lambda = lambdaOf(m, scales.get(c.element().getKey()));
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

    /** Per row and level: {@code KV<levelId + key, KV<y − b, b>>} — the (offset) target value and, under an offset, the baseline it was offset by (else null). */
    static class ExtractDoFn extends DoFn<MElement, KV<String, KV<Double, Double>>> {
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
                // a level without a target counts rows (y = 0 so n is tracked); otherwise (y − b, b) or nothing
                final KV<Double, Double> value = FeatureValues.offsetTarget(row, spec.field(), spec.offsetColumn());
                if (value == null) continue;
                final String key = FeatureValues.key(row, spec.keys());
                if (key == null) continue;
                final String entry = spec.id() + SEPARATOR + key;
                c.output(KV.of(entry, value));
                if (spec.foldKeys() != null) {
                    // the row's own fold, subtracted at apply time (rows with a null fold unit are not tagged)
                    final String unit = FeatureValues.key(row, spec.foldKeys());
                    if (unit != null) c.output(KV.of(foldEntry(foldOf(unit, spec.folds()), entry), value));
                }
            }
        }
    }

    /**
     * Per-key sufficient statistics (n, Σy, Σy²) plus, under a baseline offset, Σb ({@code sumOff}) and Σ b(1 − b)
     * ({@code sumInfo}, the logit-scale information; on log the information is Σb) of the same rows, else 0.
     */
    public static class KeyStats implements Serializable {
        double n, sum, sumSq, sumOff, sumInfo;
    }

    /** Coder of the extracted values: {@code KV<y (offset), b or null>}. */
    static Coder<KV<Double, Double>> valueCoder() {
        return KvCoder.of(DoubleCoder.of(), org.apache.beam.sdk.coders.NullableCoder.of(DoubleCoder.of()));
    }

    static class KeyStatsFn extends Combine.CombineFn<KV<Double, Double>, KeyStats, KeyStats> {
        @Override
        public KeyStats createAccumulator() { return new KeyStats(); }

        @Override
        public KeyStats addInput(final KeyStats acc, final KV<Double, Double> value) {
            final double y = value.getKey();
            acc.n += 1;
            acc.sum += y;
            acc.sumSq += y * y;
            if (value.getValue() != null) {
                final double b = value.getValue();
                acc.sumOff += b;
                acc.sumInfo += Shrinkage.information(Shrinkage.Scale.logit, b);
            }
            return acc;
        }

        @Override
        public KeyStats mergeAccumulators(final Iterable<KeyStats> accs) {
            final KeyStats out = new KeyStats();
            for (final KeyStats a : accs) {
                out.n += a.n;
                out.sum += a.sum;
                out.sumSq += a.sumSq;
                out.sumOff += a.sumOff;
                out.sumInfo += a.sumInfo;
            }
            return out;
        }

        @Override
        public KeyStats extractOutput(final KeyStats acc) { return acc; }

        @Override
        public Coder<KeyStats> getAccumulatorCoder(final CoderRegistry registry, final Coder<KV<Double, Double>> inputCoder) {
            return SerializableCoder.of(KeyStats.class);
        }
    }

    /**
     * Level-wide moments over keys: the identity ones (inputs of {@link Shrinkage#lambdaFromMoments}) and, for an offset
     * term, the score ones on either transformed scale (inputs of {@link Shrinkage#lambdaFromScore}: keys with
     * information, Σ V_k, Σ V_k², Σ S_k, Σ S_k² / V_k — V = Σ b(1 − b) on logit, Σb on log). Both scales are kept
     * because the Combine does not know the level's scale; {@link #lambdaOf} reads the one the level composes on.
     */
    public static class Moments implements Serializable {
        long keys;
        double n, sum, sumSq, sumSqOverN, sumNSq;
        long keysLogit, keysLog;
        double sumVLogit, sumV2Logit, sumSLogit, sumS2OverVLogit;
        double sumVLog, sumV2Log, sumSLog, sumS2OverVLog;
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
            if (k.sumInfo > 0) {
                acc.keysLogit += 1;
                acc.sumVLogit += k.sumInfo;
                acc.sumV2Logit += k.sumInfo * k.sumInfo;
                acc.sumSLogit += k.sum;
                acc.sumS2OverVLogit += k.sum * k.sum / k.sumInfo;
            }
            if (k.sumOff > 0) {
                acc.keysLog += 1;
                acc.sumVLog += k.sumOff;
                acc.sumV2Log += k.sumOff * k.sumOff;
                acc.sumSLog += k.sum;
                acc.sumS2OverVLog += k.sum * k.sum / k.sumOff;
            }
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
                out.keysLogit += a.keysLogit;
                out.sumVLogit += a.sumVLogit;
                out.sumV2Logit += a.sumV2Logit;
                out.sumSLogit += a.sumSLogit;
                out.sumS2OverVLogit += a.sumS2OverVLogit;
                out.keysLog += a.keysLog;
                out.sumVLog += a.sumVLog;
                out.sumV2Log += a.sumV2Log;
                out.sumSLog += a.sumSLog;
                out.sumS2OverVLog += a.sumS2OverVLog;
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
