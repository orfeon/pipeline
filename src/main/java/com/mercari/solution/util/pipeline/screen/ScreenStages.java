package com.mercari.solution.util.pipeline.screen;

import com.mercari.solution.module.Logging;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Module;
import com.mercari.solution.util.ExpressionUtil;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Beam wiring of the screen transform: prepare rows → (group) → score units into bundle-local accumulators →
 * Combine per (column, transform) key → gather → one finalize step emitting the scoring records and the summary.
 * Every stage is a bounded Combine; the pass count does not depend on the data.
 */
public final class ScreenStages {

    private static final Logger LOG = LoggerFactory.getLogger(ScreenStages.class);

    private ScreenStages() {}

    private static final String SEP = String.valueOf((char) 1);

    public record Outputs(PCollection<MElement> records, PCollection<MElement> summary, PCollection<BadRecord> failures) {}

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

        final PCollection<KV<Integer, ScoreAccumulator>> scored;
        if (spec.isGrouped()) {
            scored = rows
                    .apply("Group", GroupByKey.create())
                    .apply("ScoreGroups", ParDo.of(new ScoreGroupDoFn(spec)));
        } else {
            scored = rows.apply("ScoreRows", ParDo.of(new ScoreRowDoFn(spec)));
        }
        final PCollection<KV<Integer, ScoreAccumulator>> combined = PCollectionList
                .of(scored.setCoder(KvCoder.of(VarIntCoder.of(), ScoreAccumulator.CODER)))
                .and(bookkeeping)
                .apply("FlattenPartials", Flatten.pCollections())
                .apply("Combine", Combine.perKey(new ScoreAccumulator.Fn()))
                .setCoder(KvCoder.of(VarIntCoder.of(), ScoreAccumulator.CODER));

        final TupleTag<MElement> recordTag = new TupleTag<>() {};
        final TupleTag<MElement> summaryTag = new TupleTag<>() {};
        final PCollectionTuple finalized = combined
                .apply("Gather", Combine.globally(new GatherFn()).withoutDefaults())
                .apply("Finalize", ParDo.of(new FinalizeDoFn(spec, recordTag, summaryTag))
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
            this.columns = new ArrayList<>(spec.candidates);
            if (spec.hasShuffle()) this.columns.add(spec.shuffleField);
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
                    time = c.timestamp().getMillis();
                }
                if ((spec.timeToMillis != null && time > spec.timeToMillis) || (spec.timeFromMillis != null && time < spec.timeFromMillis)) {
                    book[ScoreAccumulator.ROWS_TIME_FILTERED] = 1;
                    c.output(bookTag, KV.of(ScoreAccumulator.BOOKKEEPING_KEY, new ScoreAccumulator().add(null, book)));
                    return;
                }

                final Double label = label(values);
                final String group = spec.group == null ? null : text(values.get(spec.group));
                final Double weight = spec.weightField == null ? 1d : ScreenMath.toDouble(values.get(spec.weightField));
                final boolean invalid = label == null || Double.isNaN(label)
                        || (spec.group != null && group == null)
                        || weight == null || Double.isNaN(weight) || weight < 0;
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
                            : ScreenMath.toEpochMillis(values.get(spec.periodsField), null);
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
            return String.valueOf(value);
        }
    }

    /**
     * Scores units into bundle-local accumulator maps (one per window), flushed once per bundle: a partial
     * combine, so the shuffle carries keys x bundles elements instead of units x columns.
     */
    abstract static class ScoreDoFn<T> extends DoFn<T, KV<Integer, ScoreAccumulator>> {
        protected final ScreenSpec spec;
        protected transient GroupScorer scorer;
        private transient Map<BoundedWindow, Map<Integer, ScoreAccumulator>> partials;

        ScoreDoFn(final ScreenSpec spec) {
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

        protected Map<Integer, ScoreAccumulator> partial(final BoundedWindow window) {
            return partials.computeIfAbsent(window, w -> new HashMap<>());
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

    static class ScoreGroupDoFn extends ScoreDoFn<KV<String, Iterable<ScreenRow>>> {
        ScoreGroupDoFn(final ScreenSpec spec) {
            super(spec);
        }

        @ProcessElement
        public void processElement(final ProcessContext c, final BoundedWindow window) {
            final List<ScreenRow> rows = new ArrayList<>();
            for (final ScreenRow r : c.element().getValue()) rows.add(r);
            if (rows.isEmpty()) return;
            scorer.score(rows, c.element().getKey(), partial(window));
        }
    }

    static class ScoreRowDoFn extends ScoreDoFn<KV<String, ScreenRow>> {
        ScoreRowDoFn(final ScreenSpec spec) {
            super(spec);
        }

        @ProcessElement
        public void processElement(final ProcessContext c, final BoundedWindow window) {
            scorer.score(List.of(c.element().getValue()), c.element().getKey(), partial(window));
        }
    }

    /** Gathers the (few) combined accumulators into one list for the finalize step. */
    static class GatherFn extends Combine.CombineFn<KV<Integer, ScoreAccumulator>, List<KV<Integer, ScoreAccumulator>>, List<KV<Integer, ScoreAccumulator>>> {
        private static final Coder<List<KV<Integer, ScoreAccumulator>>> CODER = ListCoder.of(KvCoder.of(VarIntCoder.of(), ScoreAccumulator.CODER));

        @Override
        public List<KV<Integer, ScoreAccumulator>> createAccumulator() {
            return new ArrayList<>();
        }

        @Override
        public List<KV<Integer, ScoreAccumulator>> addInput(final List<KV<Integer, ScoreAccumulator>> acc, final KV<Integer, ScoreAccumulator> input) {
            acc.add(input);
            return acc;
        }

        @Override
        public List<KV<Integer, ScoreAccumulator>> mergeAccumulators(final Iterable<List<KV<Integer, ScoreAccumulator>>> accs) {
            final List<KV<Integer, ScoreAccumulator>> merged = new ArrayList<>();
            for (final List<KV<Integer, ScoreAccumulator>> a : accs) merged.addAll(a);
            return merged;
        }

        @Override
        public List<KV<Integer, ScoreAccumulator>> extractOutput(final List<KV<Integer, ScoreAccumulator>> acc) {
            return acc;
        }

        @Override
        public Coder<List<KV<Integer, ScoreAccumulator>>> getAccumulatorCoder(final CoderRegistry registry, final Coder<KV<Integer, ScoreAccumulator>> inputCoder) {
            return CODER;
        }

        @Override
        public Coder<List<KV<Integer, ScoreAccumulator>>> getDefaultOutputCoder(final CoderRegistry registry, final Coder<KV<Integer, ScoreAccumulator>> inputCoder) {
            return CODER;
        }
    }

    static class FinalizeDoFn extends DoFn<List<KV<Integer, ScoreAccumulator>>, MElement> {
        private final ScreenSpec spec;
        private final TupleTag<MElement> recordTag;
        private final TupleTag<MElement> summaryTag;

        FinalizeDoFn(final ScreenSpec spec, final TupleTag<MElement> recordTag, final TupleTag<MElement> summaryTag) {
            this.spec = spec;
            this.recordTag = recordTag;
            this.summaryTag = summaryTag;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final Map<Integer, ScoreAccumulator> accumulators = new HashMap<>();
            for (final KV<Integer, ScoreAccumulator> kv : c.element()) {
                accumulators.merge(kv.getKey(), kv.getValue(), ScoreAccumulator::merge);
            }
            final ScreenReport.Result result = ScreenReport.build(spec, accumulators);
            for (final Map<String, Object> record : result.records()) {
                c.output(recordTag, MElement.of(record, c.timestamp()));
            }
            c.output(summaryTag, MElement.of(result.summary(), c.timestamp()));
            LOG.info("screen finalized: {} records, summary {}", result.records().size(), result.summary());
        }
    }
}
