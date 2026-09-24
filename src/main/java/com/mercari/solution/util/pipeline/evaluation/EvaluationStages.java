package com.mercari.solution.util.pipeline.evaluation;

import com.google.common.hash.Hashing;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.module.Logging;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Module;
import com.mercari.solution.util.ExpressionUtil;
import com.mercari.solution.util.pipeline.feature.FeatureValues;
import com.mercari.solution.util.domain.file.ResourceUtil;
import com.mercari.solution.util.pipeline.glm.FitState;
import com.mercari.solution.util.pipeline.glm.GatherFn;
import com.mercari.solution.util.pipeline.glm.StatMath;
import com.mercari.solution.util.pipeline.glm.VectorAccumulator;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Beam wiring of the evaluation transform (design: docs/design/evaluation-engine.md): prepare rows → units
 * (a GroupByKey per split × group, or one row each) → align and score units into bundle-local metric
 * accumulators (the units output and the aligned rows come out of the same step) → Combine per key → gather
 * → one finalize step (metrics + summary). The calibration tables read the aligned rows: a sketch pass when
 * a table is quantile-binned (its edges become a side input), then one Combine of the bin sums and a
 * finalize step. Every stage is a bounded Combine; the pass count does not depend on the data.
 */
public final class EvaluationStages {

    private static final Logger LOG = LoggerFactory.getLogger(EvaluationStages.class);

    private EvaluationStages() {}

    private static final String SEP = MetricAccumulator.SEP;

    public record Outputs(PCollection<MElement> metrics, PCollection<MElement> calibration, PCollection<MElement> units,
                          PCollection<MElement> slices, PCollection<MElement> summary, PCollection<MElement> rows, PCollection<BadRecord> failures) {}

    /** Engine rejections that only the input can tell (called by the module before wiring). */
    public static List<String> engineConstraints(final PCollection<MElement> input) {
        final List<String> errors = new ArrayList<>();
        final WindowingStrategy<?, ?> strategy = input.getWindowingStrategy();
        if (!(strategy.getWindowFn() instanceof GlobalWindows)) {
            errors.add("evaluation needs the global window (the splits are the time partition; a windowed input would report per window); remove the windowing strategy");
        }
        if (!(strategy.getTrigger() instanceof DefaultTrigger)) {
            errors.add("evaluation needs the default trigger (a triggered input fires every Combine once per pane); remove strategy.trigger");
        }
        return errors;
    }

    public static Outputs apply(final PCollection<MElement> input, final EvaluationSpec spec, final List<Logging> loggings, final boolean failFast) {
        final TupleTag<KV<String, EvaluationRow>> rowTag = new TupleTag<>() {};
        final TupleTag<KV<String, MetricAccumulator>> bookTag = new TupleTag<>() {};
        final TupleTag<BadRecord> failureTag = new TupleTag<>() {};
        final Coder<KV<String, MetricAccumulator>> accumulatorCoder = KvCoder.of(StringUtf8Coder.of(), MetricAccumulator.CODER);

        final PCollectionTuple prepared = input.apply("Prepare", ParDo
                .of(new PrepareDoFn(spec, loggings, failFast, rowTag, bookTag, failureTag))
                .withOutputTags(rowTag, TupleTagList.of(bookTag).and(failureTag)));
        final PCollection<KV<String, EvaluationRow>> rows = prepared.get(rowTag).setCoder(KvCoder.of(StringUtf8Coder.of(), EvaluationRow.CODER));
        final PCollection<KV<String, MetricAccumulator>> bookkeeping = prepared.get(bookTag).setCoder(accumulatorCoder);

        final Coder<KV<String, Iterable<EvaluationRow>>> unitCoder = KvCoder.of(StringUtf8Coder.of(), IterableCoder.of(EvaluationRow.CODER));
        final PCollection<KV<String, Iterable<EvaluationRow>>> units = spec.isGrouped()
                ? rows.apply("Group", GroupByKey.create())
                : rows.apply("Units", ParDo.of(new SingletonUnitDoFn())).setCoder(unitCoder);

        // calibration fits on the selection split: a grid pass per temperature fit, unrolled Newton passes per
        // blend and base set; collected into one FitResults singleton the align and finalize steps read
        final PCollectionView<FitResults> fitView = fits(input, units, spec);
        // slice discovery: the numeric dimensions' quantile edges from the discovery split's units (one sketch pass)
        final Coder<KV<Integer, SketchAccumulator>> dimensionCoder = KvCoder.of(org.apache.beam.sdk.coders.VarIntCoder.of(), SketchAccumulator.CODER);
        final PCollectionView<Map<Integer, SketchAccumulator>> dimensionView = (spec.hasDiscovery() && spec.discovery.hasNumeric()
                ? units.apply("Dimensions", ParDo.of(new DimensionSketchDoFn(spec))).setCoder(dimensionCoder)
                        .apply("Dimensions_Combine", Combine.perKey(new SketchAccumulator.Fn())).setCoder(dimensionCoder)
                : input.getPipeline().apply("NoDimensions", Create.empty(dimensionCoder)))
                .apply("Dimensions_View", View.asMap());

        final TupleTag<KV<String, MetricAccumulator>> scoredTag = new TupleTag<>() {};
        final TupleTag<MElement> unitRecordTag = new TupleTag<>() {};
        final TupleTag<AlignedRow> alignedTag = new TupleTag<>() {};
        final TupleTag<MElement> rowRecordTag = new TupleTag<>() {};
        final PCollectionTuple aligned = units.apply("Align", ParDo
                .of(new AlignDoFn(spec, scoredTag, unitRecordTag, alignedTag, rowRecordTag, fitView, dimensionView))
                .withSideInputs(fitView, dimensionView)
                .withOutputTags(scoredTag, TupleTagList.of(unitRecordTag).and(alignedTag).and(rowRecordTag)));
        final PCollection<KV<String, MetricAccumulator>> scored = aligned.get(scoredTag).setCoder(accumulatorCoder);
        final PCollection<MElement> unitRecords = aligned.get(unitRecordTag);
        final PCollection<MElement> rowRecords = aligned.get(rowRecordTag).setCoder(ElementCoder.of(EvaluationReport.rowsSchema()));
        final PCollection<AlignedRow> alignedRows = aligned.get(alignedTag).setCoder(AlignedRow.CODER);

        final PCollection<KV<String, MetricAccumulator>> combined = PCollectionList.of(scored).and(bookkeeping)
                .apply("FlattenPartials", Flatten.pCollections())
                .apply("Combine", Combine.perKey(new MetricAccumulator.Fn(spec.bootstrapSeed, spec.bootstrapSamples)))
                .setCoder(accumulatorCoder);
        final TupleTag<MElement> metricsTag = new TupleTag<>() {};
        final TupleTag<MElement> summaryTag = new TupleTag<>() {};
        final TupleTag<MElement> slicesTag = new TupleTag<>() {};
        final PCollectionTuple finalized = combined
                .apply("PruneDiscovery", Filter.by(kv -> keepForGather(spec, kv)))
                .apply("Gather", Combine.globally(new GatherFn<>(accumulatorCoder)))
                .apply("Finalize", ParDo.of(new FinalizeDoFn(spec, metricsTag, summaryTag, slicesTag, fitView)).withSideInputs(fitView).withOutputTags(metricsTag, TupleTagList.of(summaryTag).and(slicesTag)));

        // calibration tables
        final PCollection<MElement> calibration;
        if (spec.tables.isEmpty()) {
            calibration = input.getPipeline().apply("NoCalibration", Create.empty(ElementCoder.of(EvaluationReport.calibrationSchema())));
        } else {
            final Coder<KV<String, SketchAccumulator>> sketchCoder = KvCoder.of(StringUtf8Coder.of(), SketchAccumulator.CODER);
            final PCollectionView<Map<String, SketchAccumulator>> sketchView = alignedRows
                    .apply("Sketch", ParDo.of(new SketchDoFn(spec)))
                    .setCoder(sketchCoder)
                    .apply("Sketch_Combine", Combine.perKey(new SketchAccumulator.Fn()))
                    .setCoder(sketchCoder)
                    .apply("Sketch_View", View.asMap());
            final Coder<KV<String, VectorAccumulator>> binCoder = KvCoder.of(StringUtf8Coder.of(), VectorAccumulator.CODER);
            calibration = alignedRows
                    .apply("Bins", ParDo.of(new BinsDoFn(spec, sketchView)).withSideInputs(sketchView))
                    .setCoder(binCoder)
                    .apply("Bins_Combine", Combine.perKey(new VectorAccumulator.Fn()))
                    .setCoder(binCoder)
                    .apply("Bins_Gather", Combine.globally(new GatherFn<>(binCoder)))
                    .apply("Bins_Finalize", ParDo.of(new CalibrationDoFn(spec, sketchView)).withSideInputs(sketchView));
        }
        return new Outputs(finalized.get(metricsTag), calibration, unitRecords, finalized.get(slicesTag), finalized.get(summaryTag), rowRecords, prepared.get(failureTag));
    }

    /**
     * The calibration fits (design §7.1). A temperature fit is one pass over the selection split's units (every
     * grid value's log score into one vector, {@code Combine.globally}); a blend fit is {@code maxIter} unrolled
     * Newton passes per base set (the screen transform's controller: {@link FitState} as a singleton view chained
     * pass to pass). One collect step turns every fit's result into the {@link FitResults} singleton.
     */
    static PCollectionView<FitResults> fits(final PCollection<MElement> input, final PCollection<KV<String, Iterable<EvaluationRow>>> allUnits, final EvaluationSpec spec) {
        final List<PCollectionView<?>> views = new ArrayList<>();
        final Map<String, PCollectionView<VectorAccumulator>> temperatureViews = new HashMap<>();
        final Map<String, PCollectionView<FitState>> blendViews = new HashMap<>();
        // every pass of a fit reads only its selection split's units: filtered once per split, so the unrolled
        // passes (maxIter per blend, converged or not) decode the fit's units and nothing else
        final Map<String, PCollection<KV<String, Iterable<EvaluationRow>>>> fitUnits = new HashMap<>();
        for (int i = 0; i < spec.fits.size(); i++) {
            final EvaluationSpec.Fit fit = spec.fits.get(i);
            final PCollection<KV<String, Iterable<EvaluationRow>>> units = fitUnits.computeIfAbsent(fit.fitOn, split -> {
                final String prefix = split + SEP;
                return allUnits.apply("FitUnits_" + split, Filter.by(kv -> kv.getKey().startsWith(prefix)));
            });
            if (fit.isTemperature()) {
                final PCollectionView<VectorAccumulator> view = units
                        .apply("Temperature" + i, ParDo.of(new TemperaturePassDoFn(spec, i)))
                        .setCoder(VectorAccumulator.CODER)
                        .apply("Temperature" + i + "_Combine", Combine.globally(new VectorAccumulator.Fn()))
                        .apply("Temperature" + i + "_View", View.asSingleton());
                temperatureViews.put("t" + i, view);
                views.add(view);
                continue;
            }
            for (final int set : spec.derivedOf(i)) {
                final EvaluationSpec.Derived dv = spec.derived.get(set - spec.predictions.size());
                final String tag = "Blend" + i + "_" + dv.base;
                PCollectionView<FitState> state = input.getPipeline()
                        .apply(tag + "_Init", Create.of(dv.base))
                        .apply(tag + "_State", ParDo.of(new BlendInitDoFn(spec)))
                        .setCoder(SerializableCoder.of(FitState.class))
                        .apply(tag + "_InitView", View.asSingleton());
                for (int it = 1; it <= fit.maxIter; it++) {
                    final PCollection<VectorAccumulator> evaluation = units
                            .apply(tag + "_Fit" + it, ParDo.of(new BlendPassDoFn(spec, i, dv.base, state)).withSideInputs(state))
                            .setCoder(VectorAccumulator.CODER)
                            .apply(tag + "_Fit" + it + "_Combine", Combine.globally(new VectorAccumulator.Fn()));
                    state = evaluation
                            .apply(tag + "_Fit" + it + "_Advance", ParDo.of(new BlendAdvanceDoFn(fit, state)).withSideInputs(state))
                            .setCoder(SerializableCoder.of(FitState.class))
                            .apply(tag + "_Fit" + it + "_View", View.asSingleton());
                }
                blendViews.put("b" + i + "_" + dv.base, state);
                views.add(state);
            }
        }
        return input.getPipeline()
                .apply("Fits", Create.of(0))
                .apply("Fits_Collect", ParDo.of(new CollectFitsDoFn(spec, temperatureViews, blendViews)).withSideInputs(views))
                .setCoder(SerializableCoder.of(FitResults.class))
                .apply("Fits_View", View.asSingleton());
    }

    /** One temperature fit's grid log scores over the selection split's units, bundle-local. */
    static class TemperaturePassDoFn extends DoFn<KV<String, Iterable<EvaluationRow>>, VectorAccumulator> {
        private final EvaluationSpec spec;
        private final int fit;
        private transient EvaluationScorer scorer;
        private transient VectorAccumulator partial;

        TemperaturePassDoFn(final EvaluationSpec spec, final int fit) {
            this.spec = spec;
            this.fit = fit;
        }

        @Setup
        public void setup() {
            scorer = new EvaluationScorer(spec);
        }

        @StartBundle
        public void startBundle() {
            partial = new VectorAccumulator();
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final EvaluationScorer.Unit unit = unit(scorer, c.element(), spec.fits.get(fit).fitOn);
            if (unit == null) return;
            partial.add(scorer.temperatureLogLikelihoods(unit, fit));
        }

        @FinishBundle
        public void finishBundle(final FinishBundleContext c) {
            if (!partial.isEmpty()) c.output(partial, GlobalWindow.INSTANCE.maxTimestamp(), GlobalWindow.INSTANCE);
            partial = new VectorAccumulator();
        }
    }

    /**
     * A prepared, scorable unit of the named split (null otherwise). The split is the key's prefix
     * ({@code split + SEP + group}), so a unit of another split is dropped before its rows are read.
     */
    static EvaluationScorer.Unit unit(final EvaluationScorer scorer, final KV<String, Iterable<EvaluationRow>> element, final String split) {
        if (!element.getKey().startsWith(split + SEP)) return null;
        final List<EvaluationRow> rows = new ArrayList<>();
        for (final EvaluationRow r : element.getValue()) rows.add(r);
        if (rows.isEmpty()) return null;
        final String key = element.getKey().substring(split.length() + SEP.length());
        final EvaluationScorer.Unit unit = scorer.prepare(rows, key);
        return unit.skip == EvaluationScorer.Skip.NONE ? unit : null;
    }

    /** The state before the first blend pass of a base set (the element): the set as declared, see {@link EvaluationScorer#blendStart}. */
    static class BlendInitDoFn extends DoFn<Integer, FitState> {
        private final EvaluationSpec spec;

        BlendInitDoFn(final EvaluationSpec spec) {
            this.spec = spec;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final EvaluationScorer scorer = new EvaluationScorer(spec);
            c.output(FitState.initial(scorer.blendK(), scorer.blendStart(c.element())));
        }
    }

    /** One Newton pass of a blend fit over the selection split's units at the state's proposal (nothing once converged). */
    static class BlendPassDoFn extends DoFn<KV<String, Iterable<EvaluationRow>>, VectorAccumulator> {
        private final EvaluationSpec spec;
        private final int fit;
        private final int base;
        private final PCollectionView<FitState> stateView;
        private transient EvaluationScorer scorer;
        private transient VectorAccumulator partial;

        BlendPassDoFn(final EvaluationSpec spec, final int fit, final int base, final PCollectionView<FitState> stateView) {
            this.spec = spec;
            this.fit = fit;
            this.base = base;
            this.stateView = stateView;
        }

        @Setup
        public void setup() {
            scorer = new EvaluationScorer(spec);
        }

        @StartBundle
        public void startBundle() {
            partial = new VectorAccumulator();
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final FitState state = c.sideInput(stateView);
            if (state.converged) return;
            final EvaluationScorer.Unit unit = unit(scorer, c.element(), spec.fits.get(fit).fitOn);
            if (unit == null) return;
            partial.add(scorer.blendEvaluate(unit, base, state.proposal));
        }

        @FinishBundle
        public void finishBundle(final FinishBundleContext c) {
            if (!partial.isEmpty()) c.output(partial, GlobalWindow.INSTANCE.maxTimestamp(), GlobalWindow.INSTANCE);
            partial = new VectorAccumulator();
        }
    }

    /** The Newton controller of a blend: previous state + the pass evaluation → next state. */
    static class BlendAdvanceDoFn extends DoFn<VectorAccumulator, FitState> {
        private final EvaluationSpec.Fit fit;
        private final PCollectionView<FitState> stateView;

        BlendAdvanceDoFn(final EvaluationSpec.Fit fit, final PCollectionView<FitState> stateView) {
            this.fit = fit;
            this.stateView = stateView;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final FitState next = SerializableUtils.clone(c.sideInput(stateView)).advance(c.element().getValues(), fit.l2, fit.tol);
            LOG.info("evaluation blend iteration {}: objective {} converged={} rejected={}", next.iteration,
                    next.objectiveHistory.isEmpty() ? null : next.objectiveHistory.get(next.objectiveHistory.size() - 1), next.converged, next.rejected);
            c.output(next);
        }
    }

    /** Turns every fit's view into the {@link FitResults} singleton: the derived sets' parameters and the fit records. */
    static class CollectFitsDoFn extends DoFn<Integer, FitResults> {
        private final EvaluationSpec spec;
        private final Map<String, PCollectionView<VectorAccumulator>> temperatureViews;
        private final Map<String, PCollectionView<FitState>> blendViews;

        CollectFitsDoFn(final EvaluationSpec spec, final Map<String, PCollectionView<VectorAccumulator>> temperatureViews, final Map<String, PCollectionView<FitState>> blendViews) {
            this.spec = spec;
            this.temperatureViews = temperatureViews;
            this.blendViews = blendViews;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final FitResults results = new FitResults();
            for (int i = 0; i < spec.fits.size(); i++) {
                final EvaluationSpec.Fit fit = spec.fits.get(i);
                final List<Integer> derivedSets = spec.derivedOf(i);
                if (fit.isTemperature()) {
                    final double[] v = c.sideInput(temperatureViews.get("t" + i)).getValues();
                    final double[] grid = fit.grid();
                    for (int s = 0; s < derivedSets.size(); s++) {
                        final EvaluationSpec.Derived dv = spec.derived.get(derivedSets.get(s) - spec.predictions.size());
                        final Map<String, Object> r = fitRecord(dv, fit);
                        final double mass = v.length == 0 ? 0 : v[v.length - 1];
                        if (!(mass > 0)) {
                            r.put("fitted", false);
                            r.put("note", "no scored unit in split " + fit.fitOn);
                            results.records.add(r);
                            continue;
                        }
                        int best = -1;
                        double bestLl = Double.NEGATIVE_INFINITY;
                        int identity = -1;
                        for (int g = 0; g < grid.length; g++) {
                            final double ll = v[s * fit.gridSize + g];
                            if (Double.isFinite(ll) && ll > bestLl) {
                                bestLl = ll;
                                best = g;
                            }
                            if (identity < 0 || Math.abs(grid[g] - 1d) < Math.abs(grid[identity] - 1d)) identity = g;
                        }
                        if (best < 0) {
                            r.put("fitted", false);
                            r.put("note", "no finite log score on the grid");
                            results.records.add(r);
                            continue;
                        }
                        results.parameters.put(dv.name, new double[]{grid[best]});
                        r.put("fitted", true);
                        r.put("temperature", grid[best]);
                        r.put("nUnits", mass);
                        r.put("logScore", EvaluationReport.finiteOrNull(bestLl / mass));
                        final boolean hasIdentity = Math.abs(grid[identity] - 1d) < 1e-9;
                        r.put("logScoreAtIdentity", hasIdentity ? EvaluationReport.finiteOrNull(v[s * fit.gridSize + identity] / mass) : null);
                        r.put("gainPerUnit", hasIdentity ? EvaluationReport.finiteOrNull((bestLl - v[s * fit.gridSize + identity]) / mass) : null);
                        r.put("converged", best > 0 && best < grid.length - 1);
                        r.put("note", best == 0 || best == grid.length - 1 ? "the optimum is at the grid boundary; widen grid" : null);
                        results.records.add(r);
                    }
                    continue;
                }
                for (final int set : derivedSets) {
                    final EvaluationSpec.Derived dv = spec.derived.get(set - spec.predictions.size());
                    final FitState state = c.sideInput(blendViews.get("b" + i + "_" + dv.base));
                    final Map<String, Object> r = fitRecord(dv, fit);
                    r.put("iterations", (long) state.iteration);
                    r.put("rejectedSteps", (long) state.rejected);
                    // a stall (every step from the best point rejected down to the step floor) ends the chain like a
                    // convergence but is not one: the best point may still be the start with a large gradient
                    final boolean converged = state.converged && state.hasBest && !state.stalled;
                    r.put("converged", converged);
                    if (!state.hasBest) {
                        r.put("fitted", false);
                        r.put("note", state.iteration == 0 ? "no scored unit in split " + fit.fitOn : "the log likelihood is not finite at the starting point");
                        results.records.add(r);
                        continue;
                    }
                    final double[] theta = state.bestTheta;
                    final double[] se = EvaluationScorer.standardErrors(state);
                    results.parameters.put(dv.name, theta.clone());
                    r.put("fitted", true);
                    r.put("a", EvaluationReport.finiteOrNull(theta[0]));
                    r.put("b", EvaluationReport.finiteOrNull(theta[1]));
                    r.put("intercept", theta.length > 2 ? EvaluationReport.finiteOrNull(theta[2]) : null);
                    r.put("se_a", EvaluationReport.finiteOrNull(se[0]));
                    r.put("se_b", EvaluationReport.finiteOrNull(se[1]));
                    r.put("se_intercept", theta.length > 2 ? EvaluationReport.finiteOrNull(se[2]) : null);
                    r.put("z_a", se[0] > 0 ? EvaluationReport.finiteOrNull(theta[0] / se[0]) : null);
                    r.put("nUnits", state.nUnits);
                    r.put("logScore", state.nUnits > 0 ? EvaluationReport.finiteOrNull(state.bestLl / state.nUnits) : null);
                    r.put("logScoreAtIdentity", state.nUnits > 0 && !Double.isNaN(state.ll0) ? EvaluationReport.finiteOrNull(state.ll0 / state.nUnits) : null);
                    r.put("gainPerUnit", EvaluationReport.finiteOrNull(state.gainPerUnit()));
                    r.put("note", converged ? null : state.stalled
                            ? "the fit stalled: every Newton step from the best point was rejected (" + state.rejected + " rejected steps); the estimate may be the starting point"
                            : "not converged within maxIter passes");
                    results.records.add(r);
                }
            }
            c.output(results);
        }

        /** The record template of a derived set: the identity fields set, every other field of {@link EvaluationReport#fitSchema} null. */
        private Map<String, Object> fitRecord(final EvaluationSpec.Derived dv, final EvaluationSpec.Fit fit) {
            final Map<String, Object> r = new java.util.LinkedHashMap<>();
            for (final com.mercari.solution.module.Schema.Field field : EvaluationReport.fitSchema().getFields()) r.put(field.getName(), null);
            r.put("prediction", spec.predictions.get(dv.base).name);
            r.put("derived", dv.name);
            r.put("type", fit.type);
            r.put("fitOn", fit.fitOn);
            r.put("fitted", false);
            return r;
        }
    }

    /**
     * Whether a combined accumulator reaches the gather onto one worker. Discovery cells of the discovery split
     * whose support is below {@code minSupport} can never be candidates and are dropped here, where their counts
     * are final; the split's overall cell and the confirmation split's cells (read for any passed candidate) stay.
     * Bounds the gathered map by the dimensions' cardinality above minSupport instead of their raw cardinality.
     */
    static boolean keepForGather(final EvaluationSpec spec, final KV<String, MetricAccumulator> kv) {
        if (!spec.hasDiscovery() || !kv.getKey().startsWith(EvaluationReport.DISCOVERY_PREFIX)) return true;
        final String[] parts = EvaluationScorer.parseDiscoveryKey(kv.getKey().substring(EvaluationReport.DISCOVERY_PREFIX.length()));
        if (parts == null) return false;
        if (!parts[0].equals(spec.discovery.discoverOn) || parts[2].isEmpty()) return true;
        return kv.getValue().getTotal()[0] >= spec.discovery.minSupport;
    }

    /** The KLL sketches of the numeric discovery dimensions over the discovery split's units, keyed by dimension index. */
    static class DimensionSketchDoFn extends DoFn<KV<String, Iterable<EvaluationRow>>, KV<Integer, SketchAccumulator>> {
        private final EvaluationSpec spec;
        private transient EvaluationScorer scorer;
        private transient Map<Integer, SketchAccumulator> partials;

        DimensionSketchDoFn(final EvaluationSpec spec) {
            this.spec = spec;
        }

        @Setup
        public void setup() {
            scorer = new EvaluationScorer(spec);
        }

        @StartBundle
        public void startBundle() {
            partials = new HashMap<>();
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final EvaluationScorer.Unit unit = unit(scorer, c.element(), spec.discovery.discoverOn);
            if (unit == null) return;
            final EvaluationRow first = unit.rows.get(0);
            for (int i = 0; i < spec.discovery.dimensions.size(); i++) {
                final EvaluationSpec.Dimension d = spec.discovery.dimensions.get(i);
                if (!d.isNumeric() || d.index < 0) continue;
                partials.computeIfAbsent(i, k -> new SketchAccumulator()).update(first.x[d.index]);
            }
        }

        @FinishBundle
        public void finishBundle(final FinishBundleContext c) {
            for (final Map.Entry<Integer, SketchAccumulator> e : partials.entrySet()) {
                c.output(KV.of(e.getKey(), e.getValue()), GlobalWindow.INSTANCE.maxTimestamp(), GlobalWindow.INSTANCE);
            }
            partials = new HashMap<>();
        }
    }

    /** Reads one element into an {@link EvaluationRow}: split assignment, validity, identity, slice values. */
    static class PrepareDoFn extends DoFn<MElement, KV<String, EvaluationRow>> {
        private final EvaluationSpec spec;
        private final Map<String, Logging> logs;
        private final boolean failFast;
        private final TupleTag<KV<String, EvaluationRow>> rowTag;
        private final TupleTag<KV<String, MetricAccumulator>> bookTag;
        private final TupleTag<BadRecord> failureTag;
        private final List<String> columns;
        private transient ExpressionUtil.Expression labelExpression;
        private transient Map<String, Double> expressionValues;
        private transient MetricAccumulator book;

        PrepareDoFn(final EvaluationSpec spec, final List<Logging> loggings, final boolean failFast,
                    final TupleTag<KV<String, EvaluationRow>> rowTag, final TupleTag<KV<String, MetricAccumulator>> bookTag, final TupleTag<BadRecord> failureTag) {
            this.spec = spec;
            this.logs = Logging.map(loggings);
            this.failFast = failFast;
            this.rowTag = rowTag;
            this.bookTag = bookTag;
            this.failureTag = failureTag;
            this.columns = spec.rowColumns;
        }

        @Setup
        public void setup() {
            if (spec.labelExpr != null) {
                labelExpression = ExpressionUtil.createDefaultExpression(spec.labelExpr);
                expressionValues = new HashMap<>();
            }
        }

        @StartBundle
        public void startBundle() {
            book = new MetricAccumulator();
        }

        @FinishBundle
        public void finishBundle(final FinishBundleContext c) {
            c.output(bookTag, KV.of(MetricAccumulator.ROWS_KEY, book), GlobalWindow.INSTANCE.maxTimestamp(), GlobalWindow.INSTANCE);
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final MElement input = c.element();
            if (input == null) return;
            try {
                Logging.log(LOG, logs, "input", input);
                final Map<String, Object> values = input.asPrimitiveMap();
                final double[] slots = new double[MetricAccumulator.SLOTS];
                slots[MetricAccumulator.ROWS_IN] = 1;

                long time = EvaluationRow.NO_TIME;
                if (spec.timeField != null) {
                    final Long millis = FeatureValues.toEpochMillis(values.get(spec.timeField), spec.timeFieldType);
                    if (millis == null) {
                        if (spec.splitField == null) throw new IllegalArgumentException("time.field '" + spec.timeField + "' is null or not a timestamp");
                    } else {
                        time = millis;
                    }
                } else {
                    final long ts = c.timestamp().getMillis();
                    time = ts <= BoundedWindow.TIMESTAMP_MIN_VALUE.getMillis() ? EvaluationRow.NO_TIME : ts;
                }
                final String split;
                if (spec.splitField != null) {
                    final String value = text(values.get(spec.splitField));
                    split = value != null && spec.split(value) != null ? value : null;
                } else {
                    split = spec.splitOf(time);
                }
                if (split == null) {
                    slots[MetricAccumulator.ROWS_UNASSIGNED] = 1;
                    book.add(slots);
                    return;
                }

                final Double label = label(values);
                final String group = spec.group == null ? null : text(values.get(spec.group));
                final Double weight = spec.weightField == null ? 1d : FeatureValues.toDouble(values.get(spec.weightField));
                final boolean invalid = label == null || !Double.isFinite(label)
                        || (spec.group != null && group == null)
                        || weight == null || !Double.isFinite(weight) || weight < 0;
                if (invalid) {
                    slots[MetricAccumulator.ROWS_INVALID] = 1;
                    book.add(slots);
                    Logging.log(LOG, logs, "invalid", input);
                    return;
                }
                final Double baseline = spec.hasBaseline() ? FeatureValues.toDouble(values.get(spec.baselineField)) : null;
                final double[] x = new double[columns.size()];
                for (int i = 0; i < x.length; i++) {
                    final Double v = FeatureValues.toDouble(values.get(columns.get(i)));
                    x[i] = v == null ? Double.NaN : v;
                }
                final String[] slices = new String[spec.slices.size()];
                for (int i = 0; i < slices.length; i++) {
                    final EvaluationSpec.Slice sl = spec.slices.get(i);
                    if (sl.bucket == null) {
                        slices[i] = text(values.get(sl.field));
                    } else {
                        final Long millis = sl.field.equals(spec.timeField) ? (time == EvaluationRow.NO_TIME ? null : time) : FeatureValues.toEpochMillis(values.get(sl.field), sl.fieldType);
                        slices[i] = millis == null ? null : StatMath.periodBucket(millis, sl.bucket);
                    }
                }
                final String[] dims = new String[spec.dimColumns.size()];
                for (int i = 0; i < dims.length; i++) dims[i] = text(values.get(spec.dimColumns.get(i)));
                final String bootKey = spec.bootstrapUnit == null ? null : text(values.get(spec.bootstrapUnit));
                final String identity = identity(values);
                // the rowId values travel only for a rows output (the identity hash serves everything else)
                final String[] ids = new String[spec.hasRows() ? spec.rowId.size() : 0];
                for (int i = 0; i < ids.length; i++) ids[i] = text(values.get(spec.rowId.get(i)));
                final EvaluationRow row = new EvaluationRow(split, group, identity, time, bootKey, label, baseline == null ? Double.NaN : baseline, weight, slices, dims, x, ids);
                c.output(rowTag, KV.of(split + SEP + (group == null ? identity : group), row));
                book.add(slots);
            } catch (final Throwable e) {
                c.output(failureTag, Module.processError("Failed to prepare evaluation input", input, e, failFast));
            }
        }

        private Double label(final Map<String, Object> values) {
            if (labelExpression == null) return FeatureValues.toDouble(values.get(spec.labelField));
            expressionValues.clear();
            for (final String v : labelExpression.getVariableNames()) {
                final Double d = FeatureValues.toDouble(values.get(v));
                expressionValues.put(v, d == null ? Double.NaN : d);
            }
            return labelExpression.evaluate(expressionValues);
        }

        /** Deterministic row identity: a 128-bit hash of the declared rowId fields, else of every field value in name order. */
        private String identity(final Map<String, Object> values) {
            final StringBuilder sb = new StringBuilder();
            if (!spec.rowId.isEmpty()) {
                for (final String f : spec.rowId) sb.append(text(values.get(f))).append(SEP);
            } else {
                for (final Map.Entry<String, Object> e : new TreeMap<>(values).entrySet()) {
                    sb.append(e.getKey()).append('=').append(text(e.getValue())).append(SEP);
                }
            }
            return Hashing.murmur3_128().hashString(sb, StandardCharsets.UTF_8).toString();
        }

        static String text(final Object value) {
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
    static class SingletonUnitDoFn extends DoFn<KV<String, EvaluationRow>, KV<String, Iterable<EvaluationRow>>> {
        @ProcessElement
        public void processElement(final ProcessContext c) {
            c.output(KV.of(c.element().getKey(), List.of(c.element().getValue())));
        }
    }

    /**
     * Aligns and scores units into bundle-local accumulators (flushed once per bundle: a partial combine), and
     * emits the unit records and the aligned rows (the latter only when calibration tables are declared).
     */
    static class AlignDoFn extends DoFn<KV<String, Iterable<EvaluationRow>>, KV<String, MetricAccumulator>> {
        private final EvaluationSpec spec;
        private final TupleTag<KV<String, MetricAccumulator>> scoredTag;
        private final TupleTag<MElement> unitRecordTag;
        private final TupleTag<AlignedRow> alignedTag;
        private final TupleTag<MElement> rowRecordTag;
        private final PCollectionView<FitResults> fitView;
        private final PCollectionView<Map<Integer, SketchAccumulator>> dimensionView;
        private transient EvaluationScorer scorer;
        private transient Map<String, MetricAccumulator> partials;
        private transient Map<String, double[]> discovery;
        private transient Map<Integer, double[]> edges;

        AlignDoFn(final EvaluationSpec spec, final TupleTag<KV<String, MetricAccumulator>> scoredTag, final TupleTag<MElement> unitRecordTag,
                  final TupleTag<AlignedRow> alignedTag, final TupleTag<MElement> rowRecordTag,
                  final PCollectionView<FitResults> fitView, final PCollectionView<Map<Integer, SketchAccumulator>> dimensionView) {
            this.spec = spec;
            this.scoredTag = scoredTag;
            this.unitRecordTag = unitRecordTag;
            this.alignedTag = alignedTag;
            this.rowRecordTag = rowRecordTag;
            this.fitView = fitView;
            this.dimensionView = dimensionView;
        }

        @Setup
        public void setup() {
            scorer = new EvaluationScorer(spec);
        }

        @StartBundle
        public void startBundle() {
            partials = new HashMap<>();
            discovery = new HashMap<>();
            edges = null;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final List<EvaluationRow> rows = new ArrayList<>();
            for (final EvaluationRow r : c.element().getValue()) rows.add(r);
            if (rows.isEmpty()) return;
            // the unit key is the group (or the row identity) without the split prefix
            final String key = c.element().getKey().substring(c.element().getKey().indexOf(SEP) + 1);
            final EvaluationScorer.Unit unit = scorer.prepare(rows, key);
            if (unit.skip != EvaluationScorer.Skip.NONE) {
                scorer.skipped(unit, partials);
                return;
            }
            scorer.derive(unit, c.sideInput(fitView));
            final EvaluationScorer.Metrics m = scorer.score(unit);
            // the keys the unit touches bound their pending contributions (EvaluationScorer#add)
            scorer.accumulate(unit, m, partials);
            if (spec.hasDiscovery()) {
                if (edges == null) {
                    edges = new HashMap<>();
                    for (final Map.Entry<Integer, SketchAccumulator> e : c.sideInput(dimensionView).entrySet()) {
                        if (!e.getValue().isEmpty()) edges.put(e.getKey(), e.getValue().edges(spec.discovery.dimensions.get(e.getKey()).bins));
                    }
                }
                scorer.accumulateDiscovery(unit, m, edges, discovery);
            }
            for (final Map<String, Object> record : scorer.unitRecords(unit, m)) {
                c.output(unitRecordTag, MElement.of(record, c.timestamp()));
            }
            if (!spec.tables.isEmpty()) {
                for (final AlignedRow row : scorer.aligned(unit)) c.output(alignedTag, row);
            }
            if (spec.outputsRows(unit.split)) {
                for (final Map<String, Object> record : scorer.rowRecords(unit)) c.output(rowRecordTag, MElement.of(record, c.timestamp()));
            }
        }

        @FinishBundle
        public void finishBundle(final FinishBundleContext c) {
            for (final Map.Entry<String, MetricAccumulator> e : partials.entrySet()) {
                c.output(scoredTag, KV.of(e.getKey(), e.getValue()), GlobalWindow.INSTANCE.maxTimestamp(), GlobalWindow.INSTANCE);
            }
            // the discovery cells ride the same Combine as [n, Σd, Σd²] in the first three total slots
            for (final Map.Entry<String, double[]> e : discovery.entrySet()) {
                final MetricAccumulator acc = new MetricAccumulator();
                final double[] slots = new double[MetricAccumulator.SLOTS];
                System.arraycopy(e.getValue(), 0, slots, 0, 3);
                acc.add(slots);
                c.output(scoredTag, KV.of(EvaluationReport.DISCOVERY_PREFIX + e.getKey(), acc), GlobalWindow.INSTANCE.maxTimestamp(), GlobalWindow.INSTANCE);
            }
            partials = new HashMap<>();
            discovery = new HashMap<>();
        }
    }

    static class FinalizeDoFn extends DoFn<List<KV<String, MetricAccumulator>>, MElement> {
        private final EvaluationSpec spec;
        private final TupleTag<MElement> metricsTag;
        private final TupleTag<MElement> summaryTag;
        private final TupleTag<MElement> slicesTag;
        private final PCollectionView<FitResults> fitView;
        private static final com.google.gson.Gson JSON = new com.google.gson.GsonBuilder().setPrettyPrinting().serializeNulls().create();

        FinalizeDoFn(final EvaluationSpec spec, final TupleTag<MElement> metricsTag, final TupleTag<MElement> summaryTag, final TupleTag<MElement> slicesTag, final PCollectionView<FitResults> fitView) {
            this.spec = spec;
            this.metricsTag = metricsTag;
            this.summaryTag = summaryTag;
            this.slicesTag = slicesTag;
            this.fitView = fitView;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final Map<String, MetricAccumulator> accumulators = new HashMap<>();
            for (final KV<String, MetricAccumulator> kv : c.element()) accumulators.merge(kv.getKey(), kv.getValue(), MetricAccumulator::merge);
            final FitResults fits = c.sideInput(fitView);
            final EvaluationReport.Result result = EvaluationReport.build(spec, accumulators, fits);
            for (final Map<String, Object> record : result.records()) c.output(metricsTag, MElement.of(record, c.timestamp()));
            for (final Map<String, Object> record : result.slices()) c.output(slicesTag, MElement.of(record, c.timestamp()));
            c.output(summaryTag, MElement.of(result.summary(), c.timestamp()));
            if (spec.calibrationUri != null) {
                // the fitted parameters are a deliverable: a write failure fails the step
                ResourceUtil.writeString(spec.calibrationUri, JSON.toJson(EvaluationReport.calibrationJson(spec, fits)));
                LOG.info("evaluation calibration written to {}: {} fits", spec.calibrationUri, fits.records.size());
            }
            LOG.info("evaluation finalized: {} metric records, summary {}", result.records().size(), result.summary());
        }
    }

    /** The KLL sketches of the quantile-binned tables' values, one per (split, prediction, table), bundle-local. */
    static class SketchDoFn extends DoFn<AlignedRow, KV<String, SketchAccumulator>> {
        private final EvaluationSpec spec;
        private transient Map<String, SketchAccumulator> partials;

        SketchDoFn(final EvaluationSpec spec) {
            this.spec = spec;
        }

        @StartBundle
        public void startBundle() {
            partials = new HashMap<>();
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final AlignedRow row = c.element();
            for (int j = 0; j < row.predictions.length; j++) {
                for (int t = 0; t < spec.tables.size(); t++) {
                    final EvaluationSpec.Table table = spec.tables.get(t);
                    if (!table.isQuantile()) continue;
                    final double v = EvaluationReport.tableValue(table, t, row, j);
                    if (Double.isNaN(v)) continue;
                    partials.computeIfAbsent(EvaluationReport.tableKey(row.split, 1 + j, t), k -> new SketchAccumulator(table.k)).update(v);
                }
            }
        }

        @FinishBundle
        public void finishBundle(final FinishBundleContext c) {
            for (final Map.Entry<String, SketchAccumulator> e : partials.entrySet()) {
                c.output(KV.of(e.getKey(), e.getValue()), GlobalWindow.INSTANCE.maxTimestamp(), GlobalWindow.INSTANCE);
            }
            partials = new HashMap<>();
        }
    }

    /** The bin sums of every table per (split, prediction, table, bin), bundle-local; the quantile edges from the sketch view. */
    static class BinsDoFn extends DoFn<AlignedRow, KV<String, VectorAccumulator>> {
        private final EvaluationSpec spec;
        private final PCollectionView<Map<String, SketchAccumulator>> sketchView;
        private transient Map<String, double[]> partials;
        private transient Map<String, double[]> edges;

        BinsDoFn(final EvaluationSpec spec, final PCollectionView<Map<String, SketchAccumulator>> sketchView) {
            this.spec = spec;
            this.sketchView = sketchView;
        }

        @StartBundle
        public void startBundle() {
            partials = new HashMap<>();
            edges = new HashMap<>();
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final AlignedRow row = c.element();
            for (int j = 0; j < row.predictions.length; j++) {
                final double q = row.predictions[j];
                // a derived set without parameters (its fit produced none) has no prediction to bin
                if (Double.isNaN(q)) continue;
                for (int t = 0; t < spec.tables.size(); t++) {
                    final EvaluationSpec.Table table = spec.tables.get(t);
                    if (table.isEdge()) {
                        if (Double.isNaN(row.baseline)) continue;
                        for (int b = 0; b < table.thresholds.length; b++) {
                            if (q > table.thresholds[b] * row.baseline) add(row, q, EvaluationReport.binKey(row.split, 1 + j, t, b));
                        }
                        continue;
                    }
                    final double v = EvaluationReport.tableValue(table, t, row, j);
                    if (Double.isNaN(v)) continue;
                    final double[] bounds;
                    if (table.isQuantile()) {
                        final String key = EvaluationReport.tableKey(row.split, 1 + j, t);
                        bounds = edges.computeIfAbsent(key, k -> {
                            final SketchAccumulator sketch = c.sideInput(sketchView).get(k);
                            return sketch == null || sketch.isEmpty() ? null : sketch.edges(table.bins);
                        });
                        if (bounds == null) continue;
                    } else {
                        bounds = table.edges;
                    }
                    // quantile bins are right-closed at the sketch's boundaries (an inclusive-rank quantile is a value of
                    // the stream); declared edges close on the side the table says
                    add(row, q, EvaluationReport.binKey(row.split, 1 + j, t, EvaluationReport.bin(v, bounds, !table.isQuantile() && table.closedLeft)));
                }
            }
        }

        private void add(final AlignedRow row, final double q, final String key) {
            EvaluationReport.addBin(partials.computeIfAbsent(key, k -> new double[EvaluationReport.BIN_SLOTS]), row, q);
        }

        @FinishBundle
        public void finishBundle(final FinishBundleContext c) {
            for (final Map.Entry<String, double[]> e : partials.entrySet()) {
                c.output(KV.of(e.getKey(), new VectorAccumulator(e.getValue())), GlobalWindow.INSTANCE.maxTimestamp(), GlobalWindow.INSTANCE);
            }
            partials = new HashMap<>();
        }
    }

    static class CalibrationDoFn extends DoFn<List<KV<String, VectorAccumulator>>, MElement> {
        private final EvaluationSpec spec;
        private final PCollectionView<Map<String, SketchAccumulator>> sketchView;

        CalibrationDoFn(final EvaluationSpec spec, final PCollectionView<Map<String, SketchAccumulator>> sketchView) {
            this.spec = spec;
            this.sketchView = sketchView;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final Map<String, double[]> bins = new HashMap<>();
            for (final KV<String, VectorAccumulator> kv : c.element()) bins.put(kv.getKey(), kv.getValue().getValues());
            for (final Map<String, Object> record : EvaluationReport.calibration(spec, bins, c.sideInput(sketchView))) {
                c.output(MElement.of(record, c.timestamp()));
            }
        }
    }
}
