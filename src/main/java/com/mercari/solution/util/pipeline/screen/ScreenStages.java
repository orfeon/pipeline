package com.mercari.solution.util.pipeline.screen;

import com.mercari.solution.module.Logging;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Module;
import com.mercari.solution.util.ExpressionUtil;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Beam wiring of the screen transform: prepare rows → units (a GroupByKey, or one row each) → score units into
 * bundle-local accumulators → Combine per (column, transform) key → gather → one finalize step emitting the
 * scoring records and the summary. With conditioning: a moments pass, {@code maxIter} unrolled Newton passes
 * (each one Combine, the controller a tiny ParDo with the previous state as side input), and one partial pass
 * whose sums join the finalize step as a side input. Every stage is a bounded Combine; the pass count does
 * not depend on the data.
 */
public final class ScreenStages {

    private static final Logger LOG = LoggerFactory.getLogger(ScreenStages.class);

    private ScreenStages() {}

    private static final String SEP = String.valueOf((char) 1);

    public record Outputs(PCollection<MElement> records, PCollection<MElement> summary, PCollection<BadRecord> failures) {}

    /** Engine rejections that only the input can tell (called by the module before wiring). */
    public static List<String> engineConstraints(final PCollection<MElement> input, final ScreenSpec spec) {
        final List<String> errors = new ArrayList<>();
        final WindowingStrategy<?, ?> strategy = input.getWindowingStrategy();
        if (spec.hasConditioning() && !(strategy.getWindowFn() instanceof GlobalWindows)) {
            errors.add("conditioning needs the global window (the Newton passes combine over the whole input); remove the windowing strategy or the conditioning block");
        }
        if (!(strategy.getTrigger() instanceof DefaultTrigger)) {
            errors.add("screen needs the default trigger (a triggered input fires the Combines once per pane: several partial summaries, and the conditioning singleton views break); remove strategy.trigger");
        }
        return errors;
    }

    public static Outputs apply(final PCollection<MElement> input, final ScreenSpec spec, final List<Logging> loggings, final boolean failFast) {
        final TupleTag<KV<String, ScreenRow>> rowTag = new TupleTag<>() {};
        final TupleTag<KV<Integer, ScoreAccumulator>> bookTag = new TupleTag<>() {};
        final TupleTag<BadRecord> failureTag = new TupleTag<>() {};

        final PCollectionTuple prepared = input.apply("Prepare", ParDo
                .of(new PrepareDoFn(spec, loggings, failFast, rowTag, bookTag, failureTag))
                .withOutputTags(rowTag, TupleTagList.of(bookTag).and(failureTag)));
        final PCollection<KV<String, ScreenRow>> rows = prepared.get(rowTag)
                .setCoder(KvCoder.of(StringUtf8Coder.of(), ScreenRow.CODER));
        final PCollection<KV<Integer, ScoreAccumulator>> bookkeeping = prepared.get(bookTag)
                .setCoder(KvCoder.of(VarIntCoder.of(), ScoreAccumulator.CODER));

        final Coder<KV<String, Iterable<ScreenRow>>> unitCoder = KvCoder.of(StringUtf8Coder.of(), IterableCoder.of(ScreenRow.CODER));
        final PCollection<KV<String, Iterable<ScreenRow>>> units;
        if (spec.isGrouped()) {
            units = rows.apply("Group", GroupByKey.create());
        } else {
            units = rows.apply("Units", ParDo.of(new SingletonUnitDoFn())).setCoder(unitCoder);
        }

        final PCollection<KV<Integer, ScoreAccumulator>> scored = units
                .apply("ScoreUnits", ParDo.of(new ScoreUnitsDoFn(spec)))
                .setCoder(KvCoder.of(VarIntCoder.of(), ScoreAccumulator.CODER));
        final PCollection<KV<Integer, ScoreAccumulator>> combined = PCollectionList
                .of(scored)
                .and(bookkeeping)
                .apply("FlattenPartials", Flatten.pCollections())
                .apply("Combine", Combine.perKey(new ScoreAccumulator.Fn()))
                .setCoder(KvCoder.of(VarIntCoder.of(), ScoreAccumulator.CODER));

        // conditioning: moments → unrolled Newton passes → partial pass (all global-window Combines)
        PCollectionView<FitState> fitView = null;
        PCollectionView<Map<Integer, VectorAccumulator>> partialView = null;
        final List<PCollectionView<?>> finalizeSideInputs = new ArrayList<>();
        if (spec.hasConditioning()) {
            final ConditioningScorer scorer = new ConditioningScorer(spec);
            // singleton views over default-carrying global Combines: every pass yields exactly one element (an
            // empty vector when nothing was evaluated), so the state chain never has an unready or empty view
            final PCollectionView<VectorAccumulator> momentsView = rows
                    .apply("ConditioningMoments", ParDo.of(new MomentsDoFn(spec)))
                    .setCoder(VectorAccumulator.CODER)
                    .apply("ConditioningMoments_Combine", Combine.globally(new VectorAccumulator.Fn()))
                    .apply("ConditioningMoments_View", View.asSingleton());
            PCollectionView<FitState> state = input.getPipeline()
                    .apply("ConditioningInit", Create.of(FitState.initial(scorer.k)).withCoder(SerializableCoder.of(FitState.class)))
                    .apply("ConditioningInit_View", View.asSingleton());
            for (int it = 1; it <= spec.conditioningMaxIter; it++) {
                final PCollection<VectorAccumulator> evaluation = units
                        .apply("ConditioningFit" + it, ParDo.of(new FitPassDoFn(spec, momentsView, state)).withSideInputs(momentsView, state))
                        .setCoder(VectorAccumulator.CODER)
                        .apply("ConditioningFit" + it + "_Combine", Combine.globally(new VectorAccumulator.Fn()));
                state = evaluation
                        .apply("ConditioningFit" + it + "_Advance", ParDo.of(new AdvanceDoFn(spec, state)).withSideInputs(state))
                        .setCoder(SerializableCoder.of(FitState.class))
                        .apply("ConditioningFit" + it + "_View", View.asSingleton());
            }
            fitView = state;
            partialView = units
                    .apply("ConditioningPartial", ParDo.of(new PartialPassDoFn(spec, momentsView, fitView)).withSideInputs(momentsView, fitView))
                    .setCoder(KvCoder.of(VarIntCoder.of(), VectorAccumulator.CODER))
                    .apply("ConditioningPartial_Combine", Combine.perKey(new VectorAccumulator.Fn()))
                    .apply("ConditioningPartial_View", View.asMap());
            finalizeSideInputs.add(fitView);
            finalizeSideInputs.add(partialView);
        }

        final TupleTag<MElement> recordTag = new TupleTag<>() {};
        final TupleTag<MElement> summaryTag = new TupleTag<>() {};
        // in the global window the Combine emits its (empty) default on empty input, so the summary is always produced
        final Combine.Globally<KV<Integer, ScoreAccumulator>, List<KV<Integer, ScoreAccumulator>>> gather =
                Combine.globally(new GatherFn<KV<Integer, ScoreAccumulator>>(KvCoder.of(VarIntCoder.of(), ScoreAccumulator.CODER)));
        final PCollectionTuple finalized = combined
                .apply("Gather", combined.getWindowingStrategy().getWindowFn() instanceof GlobalWindows ? gather : gather.withoutDefaults())
                .apply("Finalize", ParDo.of(new FinalizeDoFn(spec, recordTag, summaryTag, fitView, partialView))
                        .withSideInputs(finalizeSideInputs)
                        .withOutputTags(recordTag, TupleTagList.of(summaryTag)));
        return new Outputs(finalized.get(recordTag), finalized.get(summaryTag), prepared.get(failureTag));
    }

    /** Reads one element into a {@link ScreenRow}, applying the time window and the validity rules. */
    static class PrepareDoFn extends DoFn<MElement, KV<String, ScreenRow>> {
        private final ScreenSpec spec;
        private final Map<String, Logging> logs;
        private final boolean failFast;
        private final TupleTag<KV<String, ScreenRow>> rowTag;
        private final TupleTag<KV<Integer, ScoreAccumulator>> bookTag;
        private final TupleTag<BadRecord> failureTag;
        private final List<String> columns;
        private transient ExpressionUtil.Expression labelExpression;
        private transient Map<String, Double> expressionValues;

        PrepareDoFn(final ScreenSpec spec, final List<Logging> loggings, final boolean failFast,
                    final TupleTag<KV<String, ScreenRow>> rowTag, final TupleTag<KV<Integer, ScoreAccumulator>> bookTag, final TupleTag<BadRecord> failureTag) {
            this.spec = spec;
            this.logs = Logging.map(loggings);
            this.failFast = failFast;
            this.rowTag = rowTag;
            this.bookTag = bookTag;
            this.failureTag = failureTag;
            this.columns = spec.rowColumns();
        }

        @Setup
        public void setup() {
            if (spec.labelExpr != null) {
                labelExpression = ExpressionUtil.createDefaultExpression(spec.labelExpr);
                expressionValues = new HashMap<>();
            }
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final MElement input = c.element();
            if (input == null) return;
            try {
                Logging.log(LOG, logs, "input", input);
                final Map<String, Object> values = input.asPrimitiveMap();
                final double[] book = new double[ScoreAccumulator.SLOTS];
                book[ScoreAccumulator.ROWS_IN] = 1;

                final long time;
                if (spec.timeField != null) {
                    final Long millis = ScreenMath.toEpochMillis(values.get(spec.timeField), spec.timeFieldType);
                    if (millis == null) throw new IllegalArgumentException("time.field '" + spec.timeField + "' is null or not a timestamp");
                    time = millis;
                } else {
                    // bounded sources without a timestampAttribute carry TIMESTAMP_MIN_VALUE: keep it out of the summary range
                    final long ts = c.timestamp().getMillis();
                    time = ts <= BoundedWindow.TIMESTAMP_MIN_VALUE.getMillis() ? ScreenRow.NO_TIME : ts;
                }
                if ((spec.timeToMillis != null && time > spec.timeToMillis) || (spec.timeFromMillis != null && time < spec.timeFromMillis)) {
                    book[ScoreAccumulator.ROWS_TIME_FILTERED] = 1;
                    c.output(bookTag, KV.of(ScoreAccumulator.BOOKKEEPING_KEY, new ScoreAccumulator().add(null, book)));
                    return;
                }

                final Double label = label(values);
                final String group = spec.group == null ? null : text(values.get(spec.group));
                final Double weight = spec.weightField == null ? 1d : ScreenMath.toDouble(values.get(spec.weightField));
                final boolean invalid = label == null || !Double.isFinite(label)
                        || (spec.group != null && group == null)
                        || weight == null || !Double.isFinite(weight) || weight < 0;
                if (invalid) {
                    book[ScoreAccumulator.ROWS_INVALID] = 1;
                    c.output(bookTag, KV.of(ScoreAccumulator.BOOKKEEPING_KEY, new ScoreAccumulator().add(null, book)));
                    Logging.log(LOG, logs, "invalid", input);
                    return;
                }
                final Double baseline = spec.hasBaseline() ? ScreenMath.toDouble(values.get(spec.baselineField)) : null;
                final double[] x = new double[columns.size()];
                for (int i = 0; i < x.length; i++) {
                    final Double v = ScreenMath.toDouble(values.get(columns.get(i)));
                    x[i] = v == null ? Double.NaN : v;
                }
                String period = null;
                if (spec.periodsBucket != null) {
                    final Long periodMillis = spec.periodsField.equals(spec.timeField)
                            ? time
                            : ScreenMath.toEpochMillis(values.get(spec.periodsField), spec.periodsFieldType);
                    if (periodMillis != null) period = ScreenMath.periodBucket(periodMillis, spec.periodsBucket);
                }
                final String identity = identity(values);
                final ScreenRow row = new ScreenRow(group, identity, time, period, label, baseline == null ? Double.NaN : baseline, weight, x);
                c.output(rowTag, KV.of(group == null ? identity : group, row));
                c.output(bookTag, KV.of(ScoreAccumulator.BOOKKEEPING_KEY, new ScoreAccumulator().add(null, book)));
            } catch (final Throwable e) {
                c.output(failureTag, Module.processError("Failed to prepare screen input", input, e, failFast));
            }
        }

        private Double label(final Map<String, Object> values) {
            if (labelExpression == null) return ScreenMath.toDouble(values.get(spec.labelField));
            expressionValues.clear();
            for (final String v : labelExpression.getVariableNames()) {
                final Double d = ScreenMath.toDouble(values.get(v));
                expressionValues.put(v, d == null ? Double.NaN : d);
            }
            return labelExpression.evaluate(expressionValues);
        }

        /** Deterministic row identity: the declared rowId fields, else every field value in name order. */
        private String identity(final Map<String, Object> values) {
            final StringBuilder sb = new StringBuilder();
            if (!spec.rowId.isEmpty()) {
                for (final String f : spec.rowId) sb.append(text(values.get(f))).append(SEP);
            } else {
                for (final Map.Entry<String, Object> e : new TreeMap<>(values).entrySet()) {
                    sb.append(e.getKey()).append('=').append(text(e.getValue())).append(SEP);
                }
            }
            return sb.toString();
        }

        private static String text(final Object value) {
            if (value == null) return null;
            if (value instanceof Double d && d == Math.rint(d) && !Double.isInfinite(d)) return String.valueOf(d.longValue());
            if (value instanceof Float f && f == Math.rint(f) && !Float.isInfinite(f)) return String.valueOf(f.longValue());
            if (value instanceof byte[] b) return Base64.getEncoder().encodeToString(b);
            if (value instanceof ByteBuffer bb) {
                final ByteBuffer d = bb.duplicate();
                final byte[] b = new byte[d.remaining()];
                d.get(b);
                return Base64.getEncoder().encodeToString(b);
            }
            return String.valueOf(value);
        }
    }

    /** Independent rows: every row is its own unit. */
    static class SingletonUnitDoFn extends DoFn<KV<String, ScreenRow>, KV<String, Iterable<ScreenRow>>> {
        @ProcessElement
        public void processElement(final ProcessContext c) {
            c.output(KV.of(c.element().getKey(), List.of(c.element().getValue())));
        }
    }

    /**
     * Scores units into bundle-local accumulator maps (one per window), flushed once per bundle: a partial
     * combine, so the shuffle carries keys x bundles elements instead of units x columns.
     */
    static class ScoreUnitsDoFn extends DoFn<KV<String, Iterable<ScreenRow>>, KV<Integer, ScoreAccumulator>> {
        private final ScreenSpec spec;
        private transient GroupScorer scorer;
        private transient Map<BoundedWindow, Map<Integer, ScoreAccumulator>> partials;

        ScoreUnitsDoFn(final ScreenSpec spec) {
            this.spec = spec;
        }

        @Setup
        public void setup() {
            scorer = new GroupScorer(spec);
        }

        @StartBundle
        public void startBundle() {
            partials = new HashMap<>();
        }

        @ProcessElement
        public void processElement(final ProcessContext c, final BoundedWindow window) {
            final List<ScreenRow> rows = new ArrayList<>();
            for (final ScreenRow r : c.element().getValue()) rows.add(r);
            if (rows.isEmpty()) return;
            scorer.score(rows, c.element().getKey(), partials.computeIfAbsent(window, w -> new HashMap<>()));
        }

        @FinishBundle
        public void finishBundle(final FinishBundleContext c) {
            for (final Map.Entry<BoundedWindow, Map<Integer, ScoreAccumulator>> w : partials.entrySet()) {
                for (final Map.Entry<Integer, ScoreAccumulator> e : w.getValue().entrySet()) {
                    c.output(KV.of(e.getKey(), e.getValue()), w.getKey().maxTimestamp(), w.getKey());
                }
            }
            partials = new HashMap<>();
        }
    }

    /** Standardisation sums of the conditioning columns, one vector per row (combined globally). */
    static class MomentsDoFn extends DoFn<KV<String, ScreenRow>, VectorAccumulator> {
        private final ScreenSpec spec;
        private transient ConditioningScorer scorer;
        private transient VectorAccumulator partial;

        MomentsDoFn(final ScreenSpec spec) {
            this.spec = spec;
        }

        @Setup
        public void setup() {
            scorer = new ConditioningScorer(spec);
        }

        @StartBundle
        public void startBundle() {
            partial = new VectorAccumulator();
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            partial.add(scorer.moments(c.element().getValue()));
        }

        @FinishBundle
        public void finishBundle(final FinishBundleContext c) {
            if (!partial.isEmpty()) c.output(partial, GlobalWindow.INSTANCE.maxTimestamp(), GlobalWindow.INSTANCE);
            partial = new VectorAccumulator();
        }
    }

    /** One Newton pass: evaluates every scored unit at the state's proposal (nothing once converged). */
    static class FitPassDoFn extends DoFn<KV<String, Iterable<ScreenRow>>, VectorAccumulator> {
        private final ScreenSpec spec;
        private final PCollectionView<VectorAccumulator> momentsView;
        private final PCollectionView<FitState> stateView;
        private transient GroupScorer groups;
        private transient ConditioningScorer scorer;
        private transient VectorAccumulator partial;

        FitPassDoFn(final ScreenSpec spec, final PCollectionView<VectorAccumulator> momentsView, final PCollectionView<FitState> stateView) {
            this.spec = spec;
            this.momentsView = momentsView;
            this.stateView = stateView;
        }

        @Setup
        public void setup() {
            groups = new GroupScorer(spec);
            scorer = new ConditioningScorer(spec);
        }

        @StartBundle
        public void startBundle() {
            partial = new VectorAccumulator();
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final FitState state = c.sideInput(stateView);
            final VectorAccumulator moments = c.sideInput(momentsView);
            if (state.converged || moments.isEmpty()) return;
            final List<ScreenRow> rows = new ArrayList<>();
            for (final ScreenRow r : c.element().getValue()) rows.add(r);
            if (rows.isEmpty()) return;
            final GroupScorer.Unit unit = groups.prepare(rows, c.element().getKey());
            if (unit.skip != GroupScorer.Skip.NONE) return;
            partial.add(scorer.evaluate(unit, state.proposal, moments.getValues()));
        }

        @FinishBundle
        public void finishBundle(final FinishBundleContext c) {
            if (!partial.isEmpty()) c.output(partial, GlobalWindow.INSTANCE.maxTimestamp(), GlobalWindow.INSTANCE);
            partial = new VectorAccumulator();
        }
    }

    /** The Newton controller: previous state + the pass evaluation → next state. */
    static class AdvanceDoFn extends DoFn<VectorAccumulator, FitState> {
        private final ScreenSpec spec;
        private final PCollectionView<FitState> stateView;

        AdvanceDoFn(final ScreenSpec spec, final PCollectionView<FitState> stateView) {
            this.spec = spec;
            this.stateView = stateView;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final FitState previous = c.sideInput(stateView);
            // side-input values must not be mutated: the controller advances a copy
            final FitState next = SerializableUtils.clone(previous).advance(c.element().getValues(), spec.conditioningL2, spec.conditioningTol);
            LOG.info("screen conditioning iteration {}: objective {} converged={} rejected={}", next.iteration,
                    next.objectiveHistory.isEmpty() ? null : next.objectiveHistory.get(next.objectiveHistory.size() - 1), next.converged, next.rejected);
            c.output(next);
        }
    }

    /** The partial-test sums per (column, transform) at the fitted model, bundle-local like the score pass. */
    static class PartialPassDoFn extends DoFn<KV<String, Iterable<ScreenRow>>, KV<Integer, VectorAccumulator>> {
        private final ScreenSpec spec;
        private final PCollectionView<VectorAccumulator> momentsView;
        private final PCollectionView<FitState> stateView;
        private transient GroupScorer groups;
        private transient ConditioningScorer scorer;
        private transient Map<Integer, double[]> partial;

        PartialPassDoFn(final ScreenSpec spec, final PCollectionView<VectorAccumulator> momentsView, final PCollectionView<FitState> stateView) {
            this.spec = spec;
            this.momentsView = momentsView;
            this.stateView = stateView;
        }

        @Setup
        public void setup() {
            groups = new GroupScorer(spec);
            scorer = new ConditioningScorer(spec);
        }

        @StartBundle
        public void startBundle() {
            partial = new HashMap<>();
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final FitState state = c.sideInput(stateView);
            final VectorAccumulator moments = c.sideInput(momentsView);
            if (!state.hasBest || moments.isEmpty()) return;
            final List<ScreenRow> rows = new ArrayList<>();
            for (final ScreenRow r : c.element().getValue()) rows.add(r);
            if (rows.isEmpty()) return;
            final GroupScorer.Unit unit = groups.prepare(rows, c.element().getKey());
            if (unit.skip != GroupScorer.Skip.NONE) return;
            scorer.partial(unit, groups.columns(unit), state.bestTheta, moments.getValues(), partial);
        }

        @FinishBundle
        public void finishBundle(final FinishBundleContext c) {
            for (final Map.Entry<Integer, double[]> e : partial.entrySet()) {
                c.output(KV.of(e.getKey(), new VectorAccumulator(e.getValue())), GlobalWindow.INSTANCE.maxTimestamp(), GlobalWindow.INSTANCE);
            }
            partial = new HashMap<>();
        }
    }

    /** Gathers the (few) combined accumulators into one list for the finalize step. */
    static class GatherFn<T> extends Combine.CombineFn<T, List<T>, List<T>> {
        private final Coder<List<T>> coder;

        GatherFn(final Coder<T> elementCoder) {
            this.coder = ListCoder.of(elementCoder);
        }

        @Override
        public List<T> createAccumulator() {
            return new ArrayList<>();
        }

        @Override
        public List<T> addInput(final List<T> acc, final T input) {
            acc.add(input);
            return acc;
        }

        @Override
        public List<T> mergeAccumulators(final Iterable<List<T>> accs) {
            final List<T> merged = new ArrayList<>();
            for (final List<T> a : accs) merged.addAll(a);
            return merged;
        }

        @Override
        public List<T> extractOutput(final List<T> acc) {
            return acc;
        }

        @Override
        public Coder<List<T>> getAccumulatorCoder(final CoderRegistry registry, final Coder<T> inputCoder) {
            return coder;
        }

        @Override
        public Coder<List<T>> getDefaultOutputCoder(final CoderRegistry registry, final Coder<T> inputCoder) {
            return coder;
        }
    }

    static class FinalizeDoFn extends DoFn<List<KV<Integer, ScoreAccumulator>>, MElement> {
        private final ScreenSpec spec;
        private final TupleTag<MElement> recordTag;
        private final TupleTag<MElement> summaryTag;
        private final PCollectionView<FitState> fitView;
        private final PCollectionView<Map<Integer, VectorAccumulator>> partialView;

        FinalizeDoFn(final ScreenSpec spec, final TupleTag<MElement> recordTag, final TupleTag<MElement> summaryTag,
                     final PCollectionView<FitState> fitView, final PCollectionView<Map<Integer, VectorAccumulator>> partialView) {
            this.spec = spec;
            this.recordTag = recordTag;
            this.summaryTag = summaryTag;
            this.fitView = fitView;
            this.partialView = partialView;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final Map<Integer, ScoreAccumulator> accumulators = new HashMap<>();
            for (final KV<Integer, ScoreAccumulator> kv : c.element()) {
                accumulators.merge(kv.getKey(), kv.getValue(), ScoreAccumulator::merge);
            }
            FitState fit = null;
            Map<Integer, double[]> partials = null;
            if (fitView != null) {
                fit = c.sideInput(fitView);
                partials = new HashMap<>();
                // Combine.perKey in the global window: exactly one vector per key
                for (final Map.Entry<Integer, VectorAccumulator> e : c.sideInput(partialView).entrySet()) {
                    partials.put(e.getKey(), e.getValue().getValues());
                }
            }
            final ScreenReport.Result result = ScreenReport.build(spec, accumulators, partials, fit);
            for (final Map<String, Object> record : result.records()) {
                c.output(recordTag, MElement.of(record, c.timestamp()));
            }
            c.output(summaryTag, MElement.of(result.summary(), c.timestamp()));
            LOG.info("screen finalized: {} records, summary {}", result.records().size(), result.summary());
        }
    }
}
