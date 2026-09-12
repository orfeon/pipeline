package com.mercari.solution.util.pipeline.evaluation;

import com.google.common.hash.Hashing;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.module.Logging;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Module;
import com.mercari.solution.util.ExpressionUtil;
import com.mercari.solution.util.pipeline.feature.FeatureValues;
import com.mercari.solution.util.pipeline.glm.GatherFn;
import com.mercari.solution.util.pipeline.glm.StatMath;
import com.mercari.solution.util.pipeline.glm.VectorAccumulator;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
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
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
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

    private static final String SEP = String.valueOf((char) 1);

    public record Outputs(PCollection<MElement> metrics, PCollection<MElement> calibration, PCollection<MElement> units,
                          PCollection<MElement> summary, PCollection<BadRecord> failures) {}

    /** Engine rejections that only the input can tell (called by the module before wiring). */
    public static List<String> engineConstraints(final PCollection<MElement> input, final EvaluationSpec spec) {
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

        final TupleTag<KV<String, MetricAccumulator>> scoredTag = new TupleTag<>() {};
        final TupleTag<MElement> unitRecordTag = new TupleTag<>() {};
        final TupleTag<AlignedRow> alignedTag = new TupleTag<>() {};
        final PCollectionTuple aligned = units.apply("Align", ParDo
                .of(new AlignDoFn(spec, scoredTag, unitRecordTag, alignedTag))
                .withOutputTags(scoredTag, TupleTagList.of(unitRecordTag).and(alignedTag)));
        final PCollection<KV<String, MetricAccumulator>> scored = aligned.get(scoredTag).setCoder(accumulatorCoder);
        final PCollection<MElement> unitRecords = aligned.get(unitRecordTag);
        final PCollection<AlignedRow> alignedRows = aligned.get(alignedTag).setCoder(AlignedRow.CODER);

        final PCollection<KV<String, MetricAccumulator>> combined = PCollectionList.of(scored).and(bookkeeping)
                .apply("FlattenPartials", Flatten.pCollections())
                .apply("Combine", Combine.perKey(new MetricAccumulator.Fn()))
                .setCoder(accumulatorCoder);
        final TupleTag<MElement> metricsTag = new TupleTag<>() {};
        final TupleTag<MElement> summaryTag = new TupleTag<>() {};
        final PCollectionTuple finalized = combined
                .apply("Gather", Combine.globally(new GatherFn<>(accumulatorCoder)))
                .apply("Finalize", ParDo.of(new FinalizeDoFn(spec, metricsTag, summaryTag)).withOutputTags(metricsTag, TupleTagList.of(summaryTag)));

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
        return new Outputs(finalized.get(metricsTag), calibration, unitRecords, finalized.get(summaryTag), prepared.get(failureTag));
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
                    split = time == EvaluationRow.NO_TIME ? null : spec.splitOf(time);
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
                final String bootKey = spec.bootstrapUnit == null ? null : text(values.get(spec.bootstrapUnit));
                final String identity = identity(values);
                final EvaluationRow row = new EvaluationRow(split, group, identity, time, bootKey, label, baseline == null ? Double.NaN : baseline, weight, slices, x);
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
        private transient EvaluationScorer scorer;
        private transient Map<String, MetricAccumulator> partials;

        AlignDoFn(final EvaluationSpec spec, final TupleTag<KV<String, MetricAccumulator>> scoredTag, final TupleTag<MElement> unitRecordTag, final TupleTag<AlignedRow> alignedTag) {
            this.spec = spec;
            this.scoredTag = scoredTag;
            this.unitRecordTag = unitRecordTag;
            this.alignedTag = alignedTag;
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
            final EvaluationScorer.Metrics m = scorer.score(unit);
            scorer.accumulate(unit, m, partials);
            for (final Map<String, Object> record : scorer.unitRecords(unit, m)) {
                c.output(unitRecordTag, MElement.of(record, c.timestamp()));
            }
            if (!spec.tables.isEmpty()) {
                for (final AlignedRow row : scorer.aligned(unit)) c.output(alignedTag, row);
            }
        }

        @FinishBundle
        public void finishBundle(final FinishBundleContext c) {
            for (final Map.Entry<String, MetricAccumulator> e : partials.entrySet()) {
                c.output(scoredTag, KV.of(e.getKey(), e.getValue()), GlobalWindow.INSTANCE.maxTimestamp(), GlobalWindow.INSTANCE);
            }
            partials = new HashMap<>();
        }
    }

    static class FinalizeDoFn extends DoFn<List<KV<String, MetricAccumulator>>, MElement> {
        private final EvaluationSpec spec;
        private final TupleTag<MElement> metricsTag;
        private final TupleTag<MElement> summaryTag;

        FinalizeDoFn(final EvaluationSpec spec, final TupleTag<MElement> metricsTag, final TupleTag<MElement> summaryTag) {
            this.spec = spec;
            this.metricsTag = metricsTag;
            this.summaryTag = summaryTag;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final Map<String, MetricAccumulator> accumulators = new HashMap<>();
            for (final KV<String, MetricAccumulator> kv : c.element()) accumulators.merge(kv.getKey(), kv.getValue(), MetricAccumulator::merge);
            final EvaluationReport.Result result = EvaluationReport.build(spec, accumulators);
            for (final Map<String, Object> record : result.records()) c.output(metricsTag, MElement.of(record, c.timestamp()));
            c.output(summaryTag, MElement.of(result.summary(), c.timestamp()));
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
                    partials.computeIfAbsent(EvaluationReport.tableKey(row.split, 1 + j, t), k -> new SketchAccumulator()).update(v);
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
                    add(row, q, EvaluationReport.binKey(row.split, 1 + j, t, EvaluationReport.bin(v, bounds)));
                }
            }
        }

        private void add(final AlignedRow row, final double q, final String key) {
            final double[] v = partials.computeIfAbsent(key, k -> new double[EvaluationReport.BIN_SLOTS]);
            v[EvaluationReport.BIN_N] += 1;
            v[EvaluationReport.BIN_POSITIVES] += row.label;
            v[EvaluationReport.BIN_Q] += q;
            v[EvaluationReport.BIN_P] += row.baseline;
            if (!Double.isNaN(row.utility)) v[EvaluationReport.BIN_UTILITY] += row.utility * row.label;
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
