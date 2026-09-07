package com.mercari.solution.util.pipeline.process;

import com.mercari.solution.MPipeline;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.Logging;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.pipeline.Filter;
import com.mercari.solution.util.pipeline.feature.KeyedSpillSorter;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static com.mercari.solution.module.Module.processError;

/**
 * Beam wiring of the {@code process} transform: projects every input row to a compact event keyed by case,
 * groups the events of a case and replays them in time order ({@link KeyedSpillSorter} keeps a huge case
 * off the heap), then merges the per-case partials into the aggregated outputs with one merge-only Combine
 * per output. The windowing of the input decides the scope of every aggregate (the global window in batch;
 * session or fixed windows in streaming, where a window closes a case).
 */
public final class ProcessStages {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessStages.class);

    private ProcessStages() {}

    static final String KEY_SEPARATOR = "\u0000";
    static final String CASE_ID_SEPARATOR = "|";

    /** The cases output columns a case attribute may not shadow. */
    static final List<String> CASE_FIELDS = List.of("caseId", "variant", "activities", "length", "truncated", "startTime", "endTime",
            "duration", "resources", "resourceCount", "violations", "fitness");

    // event projection (the shuffled record)
    static final String EVENT_ACTIVITY = "activity";
    static final String EVENT_RESOURCE = "resource";
    static final String EVENT_SEQUENCE_INTEGER = "sequenceInteger";
    static final String EVENT_SEQUENCE_NUMBER = "sequenceNumber";
    static final String EVENT_SEQUENCE_TEXT = "sequenceText";
    static final String EVENT_CASE_ID = "caseId";
    /** The projected event columns a case attribute may not shadow. */
    static final List<String> EVENT_FIELDS = List.of(EVENT_CASE_ID, EVENT_ACTIVITY, EVENT_RESOURCE, EVENT_SEQUENCE_INTEGER,
            EVENT_SEQUENCE_NUMBER, EVENT_SEQUENCE_TEXT);

    private static final Pattern VARIANT_SPLIT = Pattern.compile(Pattern.quote(CaseReplay.VARIANT_SEPARATOR));

    public record Outputs(PCollection<MElement> edges, PCollection<MElement> nodes, PCollection<MElement> variants,
                          PCollection<MElement> cases, PCollection<MElement> handovers, PCollection<MElement> conformance,
                          PCollection<BadRecord> failures) {}

    // ---- schemas ----

    public static Schema eventSchema(final ProcessSpec spec) {
        final Schema.Builder builder = Schema.builder()
                .withField(EVENT_CASE_ID, Schema.FieldType.STRING)
                .withField(EVENT_ACTIVITY, Schema.FieldType.STRING)
                .withField(EVENT_RESOURCE, Schema.FieldType.STRING)
                .withField(EVENT_SEQUENCE_INTEGER, Schema.FieldType.INT64)
                .withField(EVENT_SEQUENCE_NUMBER, Schema.FieldType.FLOAT64)
                .withField(EVENT_SEQUENCE_TEXT, Schema.FieldType.STRING);
        for (final Schema.Field f : spec.caseAttributeFields()) builder.withField(f);
        return builder.withType(DataType.ELEMENT).build();
    }

    private static Schema.Builder withDurationStats(final Schema.Builder builder) {
        return builder
                .withField("durationMean", Schema.FieldType.FLOAT64)
                .withField("durationMin", Schema.FieldType.FLOAT64)
                .withField("durationMax", Schema.FieldType.FLOAT64)
                .withField("durationMedian", Schema.FieldType.FLOAT64)
                .withField("durationP95", Schema.FieldType.FLOAT64);
    }

    public static Schema edgesSchema() {
        return withDurationStats(Schema.builder()
                .withField("source", Schema.FieldType.STRING)
                .withField("target", Schema.FieldType.STRING)
                .withField("frequency", Schema.FieldType.INT64)
                .withField("caseCount", Schema.FieldType.INT64))
                .build();
    }

    public static Schema nodesSchema() {
        return Schema.builder()
                .withField("activity", Schema.FieldType.STRING)
                .withField("frequency", Schema.FieldType.INT64)
                .withField("caseCount", Schema.FieldType.INT64)
                .withField("startCount", Schema.FieldType.INT64)
                .withField("endCount", Schema.FieldType.INT64)
                .build();
    }

    public static Schema variantsSchema() {
        return withDurationStats(Schema.builder()
                .withField("variant", Schema.FieldType.STRING)
                .withField("activities", Schema.FieldType.array(Schema.FieldType.STRING))
                .withField("length", Schema.FieldType.INT64)
                .withField("caseCount", Schema.FieldType.INT64)
                .withField("caseShare", Schema.FieldType.FLOAT64))
                .build();
    }

    public static Schema casesSchema(final ProcessSpec spec) {
        final Schema.Builder builder = Schema.builder()
                .withField("caseId", Schema.FieldType.STRING)
                .withField("variant", Schema.FieldType.STRING)
                .withField("activities", Schema.FieldType.array(Schema.FieldType.STRING))
                .withField("length", Schema.FieldType.INT64)
                .withField("truncated", Schema.FieldType.BOOLEAN)
                .withField("startTime", Schema.FieldType.TIMESTAMP)
                .withField("endTime", Schema.FieldType.TIMESTAMP)
                .withField("duration", Schema.FieldType.FLOAT64)
                .withField("resources", Schema.FieldType.array(Schema.FieldType.STRING))
                .withField("resourceCount", Schema.FieldType.INT64);
        for (final Schema.Field f : spec.caseAttributeFields()) builder.withField(f);
        if (!spec.constraints.isEmpty()) {
            builder.withField("violations", Schema.FieldType.array(Schema.FieldType.STRING))
                    .withField("fitness", Schema.FieldType.FLOAT64);
        }
        return builder.build();
    }

    public static Schema handoversSchema() {
        return Schema.builder()
                .withField("source", Schema.FieldType.STRING)
                .withField("target", Schema.FieldType.STRING)
                .withField("frequency", Schema.FieldType.INT64)
                .withField("caseCount", Schema.FieldType.INT64)
                .build();
    }

    public static Schema conformanceSchema() {
        return Schema.builder()
                .withField("constraint", Schema.FieldType.STRING)
                .withField("type", Schema.FieldType.STRING)
                .withField("cases", Schema.FieldType.INT64)
                .withField("applicable", Schema.FieldType.INT64)
                .withField("violations", Schema.FieldType.INT64)
                .withField("violationRate", Schema.FieldType.FLOAT64)
                .build();
    }

    // ---- wiring ----

    public static Outputs apply(final PCollection<MElement> input, final Schema inputSchema, final ProcessSpec spec,
                                final List<Logging> loggings, final boolean failFast) {

        final Schema eventSchema = eventSchema(spec);
        final Coder<MElement> eventCoder = ElementCoder.of(eventSchema);
        final KvCoder<String, KV<Long, MElement>> sortKvCoder = KvCoder.of(StringUtf8Coder.of(), KvCoder.of(BigEndianLongCoder.of(), eventCoder));
        final KvCoder<String, ProcessStats> statsCoder = KvCoder.of(StringUtf8Coder.of(), ProcessStats.StatsCoder.of());
        final KeyedSpillSorter sorter = new KeyedSpillSorter(spillOptions(spec, input.getPipeline().getOptions()), eventCoder);

        final TupleTag<KV<String, KV<Long, MElement>>> eventTag = new TupleTag<>() {};
        final TupleTag<BadRecord> extractFailureTag = new TupleTag<>() {};
        final PCollectionTuple extracted = input.apply("Events", ParDo
                .of(new ExtractEventDoFn(spec, inputSchema.getFields(), loggings, failFast, extractFailureTag))
                .withOutputTags(eventTag, TupleTagList.of(extractFailureTag)));

        final TupleTag<MElement> casesTag = new TupleTag<>() {};
        final TupleTag<KV<String, ProcessStats>> edgesTag = new TupleTag<>() {};
        final TupleTag<KV<String, ProcessStats>> nodesTag = new TupleTag<>() {};
        final TupleTag<KV<String, ProcessStats>> variantsTag = new TupleTag<>() {};
        final TupleTag<KV<String, ProcessStats>> handoversTag = new TupleTag<>() {};
        final TupleTag<KV<String, ProcessStats>> conformanceTag = new TupleTag<>() {};
        final TupleTag<BadRecord> replayFailureTag = new TupleTag<>() {};
        final PCollectionTuple replayed = extracted.get(eventTag).setCoder(sortKvCoder)
                .apply("GroupByCase", GroupByKey.create())
                .apply("Replay", ParDo
                        .of(new ReplayDoFn(spec, sorter, loggings, failFast, casesTag, edgesTag, nodesTag, variantsTag, handoversTag, conformanceTag, replayFailureTag))
                        .withOutputTags(casesTag, TupleTagList.of(List.of(edgesTag, nodesTag, variantsTag, handoversTag, conformanceTag, replayFailureTag))));

        final PCollection<BadRecord> failures = org.apache.beam.sdk.values.PCollectionList
                .of(extracted.get(extractFailureTag)).and(replayed.get(replayFailureTag))
                .apply("Failures", org.apache.beam.sdk.transforms.Flatten.pCollections());

        // the per-case GroupByKey consumed a merging (session) window; the aggregates re-merge it per output key so
        // that overlapping cases land in one window instead of one row per case
        final boolean remerge = !input.getWindowingStrategy().getWindowFn().isNonMerging();
        final PCollection<MElement> edges = combine(replayed, edgesTag, statsCoder, remerge, "Edges")
                .apply("Edges", ParDo.of(new EdgesDoFn(spec)));
        final PCollection<MElement> nodes = combine(replayed, nodesTag, statsCoder, remerge, "Nodes")
                .apply("Nodes", ParDo.of(new NodesDoFn()));
        // the case share needs the window total: one group over the (already combined) variants rather than a side
        // input, which a session window (the streaming mode of this transform) does not allow
        PCollection<KV<String, KV<String, ProcessStats>>> keyedVariants = combine(replayed, variantsTag, statsCoder, remerge, "Variants")
                .apply("KeyVariants", WithKeys.of(""))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), statsCoder));
        if (remerge) keyedVariants = keyedVariants.apply("RemergeVariantTotals", Window.remerge());
        final PCollection<MElement> variants = keyedVariants
                .apply("GroupVariants", GroupByKey.create())
                .apply("Variants", ParDo.of(new VariantsDoFn()));
        final PCollection<MElement> handovers = combine(replayed, handoversTag, statsCoder, remerge, "Handovers")
                .apply("Handovers", ParDo.of(new HandoversDoFn()));
        final PCollection<MElement> conformance = combine(replayed, conformanceTag, statsCoder, remerge, "Conformance")
                .apply("Conformance", ParDo.of(new ConformanceDoFn(spec)));

        return new Outputs(edges, nodes, variants, replayed.get(casesTag), handovers, conformance, failures);
    }

    private static PCollection<KV<String, ProcessStats>> combine(final PCollectionTuple replayed, final TupleTag<KV<String, ProcessStats>> tag,
                                                                 final KvCoder<String, ProcessStats> coder, final boolean remerge, final String name) {
        PCollection<KV<String, ProcessStats>> partials = replayed.get(tag).setCoder(coder);
        if (remerge) partials = partials.apply("Remerge" + name, Window.remerge());
        return partials.apply("Combine" + name, Combine.perKey(new ProcessStats.Fn()));
    }

    /** Same resolution as the feature engine: the spec, then the {@code featureSpillMemoryMB} pipeline option, then the sorter default. */
    static KeyedSpillSorter.Options spillOptions(final ProcessSpec spec, final PipelineOptions options) {
        Integer memoryMB = spec.spillMemoryMB;
        if (memoryMB == null && options != null) {
            memoryMB = options.as(MPipeline.MPipelineOptions.class).getFeatureSpillMemoryMB();
        }
        return new KeyedSpillSorter.Options(memoryMB, spec.spillDirectory, spec.spillCompress);
    }

    // ---- event projection ----

    static class ExtractEventDoFn extends DoFn<MElement, KV<String, KV<Long, MElement>>> {

        private final ProcessSpec spec;
        private final List<Schema.Field> fields;
        private final Map<String, Logging> logs;
        private final boolean failFast;
        private final TupleTag<BadRecord> failureTag;
        private final Schema.Type timestampType;
        private transient List<Filter.ConditionNode> conditions;
        private transient Set<String> conditionVariables;

        ExtractEventDoFn(final ProcessSpec spec, final List<Schema.Field> fields, final List<Logging> loggings,
                         final boolean failFast, final TupleTag<BadRecord> failureTag) {
            this.spec = spec;
            this.fields = fields;
            this.logs = Logging.map(loggings);
            this.failFast = failFast;
            this.failureTag = failureTag;
            Schema.Type type = null;
            if (spec.timestamp != null) {
                for (final Schema.Field f : fields) if (f.getName().equals(spec.timestamp)) type = f.getFieldType().getType();
            }
            this.timestampType = type;
        }

        @Setup
        public void setup() {
            conditions = new ArrayList<>();
            conditionVariables = new HashSet<>();
            for (final ProcessSpec.ActivityRule rule : spec.activities) {
                final Filter.ConditionNode node = rule.filter == null ? null : Filter.parse(rule.filter);
                conditions.add(node);
                if (node != null) conditionVariables.addAll(node.getRequiredVariables());
            }
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final MElement input = c.element();
            if (input == null) return;
            try {
                Logging.log(LOG, logs, "input", input);
                final StringBuilder key = new StringBuilder();
                for (int i = 0; i < spec.caseId.size(); i++) {
                    final Object v = input.getPrimitiveValue(spec.caseId.get(i));
                    if (v == null) {
                        c.output(failureTag, processError("case id field '" + spec.caseId.get(i) + "' is null", input, new IllegalStateException("null case id"), failFast));
                        return;
                    }
                    if (i > 0) key.append(CASE_ID_SEPARATOR);
                    key.append(v);
                }
                final String activity = activity(input);
                if (activity == null) {
                    if (spec.unmatched == ProcessSpec.Unmatched.failure) {
                        c.output(failureTag, processError("no activity rule matched the event", input, new IllegalStateException("unmatched event"), failFast));
                    } else {
                        Logging.log(LOG, logs, "unmatched", input);
                    }
                    return;
                }
                final long millis = millis(input);
                final Map<String, Object> values = new HashMap<>();
                values.put(EVENT_CASE_ID, key.toString());
                values.put(EVENT_ACTIVITY, activity);
                if (spec.resource != null) {
                    final Object r = input.getPrimitiveValue(spec.resource);
                    values.put(EVENT_RESOURCE, r == null ? null : r.toString());
                }
                if (spec.sequence != null) {
                    // integers keep their full precision (a double loses it above 2^53)
                    final Object s = input.getPrimitiveValue(spec.sequence);
                    if (s instanceof Long || s instanceof Integer || s instanceof Short || s instanceof Byte) values.put(EVENT_SEQUENCE_INTEGER, ((Number) s).longValue());
                    else if (s instanceof Number n) values.put(EVENT_SEQUENCE_NUMBER, n.doubleValue());
                    else if (s != null) values.put(EVENT_SEQUENCE_TEXT, s.toString());
                }
                for (final Schema.Field f : spec.caseAttributeFields()) {
                    values.put(f.getName(), input.getPrimitiveValue(f.getName()));
                }
                final MElement event = MElement.of(values, millis);
                c.output(KV.of(key.toString(), KV.of(millis, event)));
            } catch (final Throwable e) {
                c.output(failureTag, processError("failed to project the event", input, e, failFast));
            }
        }

        private String activity(final MElement input) {
            if (spec.activity != null) {
                final Object v = input.getPrimitiveValue(spec.activity);
                return v == null ? null : v.toString();
            }
            // one conversion of the variables every rule needs, then the rules in order
            final Map<String, Object> values = input.asStandardMap(fields, conditionVariables);
            for (int i = 0; i < conditions.size(); i++) {
                final Filter.ConditionNode node = conditions.get(i);
                if (node == null || Filter.filter(node, values)) return spec.activities.get(i).name;
            }
            return null;
        }

        private long millis(final MElement input) {
            if (spec.timestamp == null) return input.getEpochMillis();
            final Object v = input.getPrimitiveValue(spec.timestamp);
            if (v == null) throw new IllegalStateException("timestamp field '" + spec.timestamp + "' is null");
            if (timestampType == Schema.Type.date) {
                if (v instanceof Number n) return LocalDate.ofEpochDay(n.longValue()).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
                return LocalDate.parse(v.toString()).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
            }
            final Instant instant = DateTimeUtil.toInstant(v);
            if (instant == null) throw new IllegalStateException("timestamp field '" + spec.timestamp + "' could not be read: " + v);
            return instant.toEpochMilli();
        }
    }

    // ---- case replay ----

    static class ReplayDoFn extends DoFn<KV<String, Iterable<KV<Long, MElement>>>, MElement> {

        private final ProcessSpec spec;
        private final KeyedSpillSorter sorter;
        private final Map<String, Logging> logs;
        private final boolean failFast;
        private final TupleTag<MElement> casesTag;
        private final TupleTag<KV<String, ProcessStats>> edgesTag;
        private final TupleTag<KV<String, ProcessStats>> nodesTag;
        private final TupleTag<KV<String, ProcessStats>> variantsTag;
        private final TupleTag<KV<String, ProcessStats>> handoversTag;
        private final TupleTag<KV<String, ProcessStats>> conformanceTag;
        private final TupleTag<BadRecord> failureTag;

        ReplayDoFn(final ProcessSpec spec, final KeyedSpillSorter sorter, final List<Logging> loggings, final boolean failFast,
                   final TupleTag<MElement> casesTag, final TupleTag<KV<String, ProcessStats>> edgesTag,
                   final TupleTag<KV<String, ProcessStats>> nodesTag, final TupleTag<KV<String, ProcessStats>> variantsTag,
                   final TupleTag<KV<String, ProcessStats>> handoversTag, final TupleTag<KV<String, ProcessStats>> conformanceTag,
                   final TupleTag<BadRecord> failureTag) {
            this.spec = spec;
            this.sorter = sorter;
            this.logs = Logging.map(loggings);
            this.failFast = failFast;
            this.casesTag = casesTag;
            this.edgesTag = edgesTag;
            this.nodesTag = nodesTag;
            this.variantsTag = variantsTag;
            this.handoversTag = handoversTag;
            this.conformanceTag = conformanceTag;
            this.failureTag = failureTag;
        }

        @Setup
        public void setup() {
            try {
                sorter.setup();
            } catch (final IOException e) {
                throw new UncheckedIOException("failed to prepare the spill directory of the case replay", e);
            }
        }

        @Teardown
        public void teardown() {
            sorter.teardown();
        }

        /** Case records carry their end time, which precedes the grouped element's window-end timestamp. */
        @Override
        public org.joda.time.Duration getAllowedTimestampSkew() {
            return org.joda.time.Duration.millis(Long.MAX_VALUE);
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final KV<String, Iterable<KV<Long, MElement>>> kv = c.element();
            if (kv == null) return;
            final String caseId = kv.getKey();
            final Map<String, Object> attributes = new LinkedHashMap<>();
            for (final Schema.Field f : spec.caseAttributeFields()) attributes.put(f.getName(), null);
            try (final KeyedSpillSorter.Sorted sorted = sorter.sort(kv.getValue(), "process case=" + caseId)) {
                final CaseReplay.Result result = CaseReplay.replay(() -> new EventIterator(sorted.iterator(), attributes), spec);
                if (result.length == 0) return;
                emit(c, caseId, result, attributes);
            } catch (final Throwable e) {
                final MElement marker = MElement.builder().withString(EVENT_CASE_ID, caseId).withEventTime(c.timestamp()).build();
                c.output(failureTag, processError("failed to replay case " + caseId, marker, e, failFast));
            }
        }

        private void emit(final ProcessContext c, final String caseId, final CaseReplay.Result result, final Map<String, Object> attributes) {
            // cases
            final Map<String, Object> values = new HashMap<>();
            values.put("caseId", caseId);
            values.put("variant", result.variant);
            values.put("activities", new ArrayList<>(result.activities));
            values.put("length", (long) result.length);
            values.put("truncated", result.truncated);
            values.put("startTime", result.startMillis * 1000L);
            values.put("endTime", result.endMillis * 1000L);
            values.put("duration", spec.unit.fromMillis(result.durationMillis()));
            values.put("resources", new ArrayList<>(result.resources));
            values.put("resourceCount", (long) result.resources.size());
            values.putAll(attributes);
            if (!spec.constraints.isEmpty()) {
                values.put("violations", result.violations());
                values.put("fitness", result.fitness());
            }
            final MElement caseRecord = MElement.of(values, result.endMillis);
            c.outputWithTimestamp(casesTag, caseRecord, org.joda.time.Instant.ofEpochMilli(result.endMillis));
            Logging.log(LOG, logs, "output", caseRecord);

            // edges: one partial per distinct edge of the case
            for (final Map.Entry<CaseReplay.Edge, ProcessStats> e : result.edges.entrySet()) {
                c.output(edgesTag, KV.of(e.getKey().source() + KEY_SEPARATOR + e.getKey().target(), e.getValue()));
            }

            // nodes
            for (final Map.Entry<String, CaseReplay.ActivityCount> e : result.activityCounts.entrySet()) {
                final ProcessStats s = new ProcessStats();
                s.frequency = e.getValue().frequency;
                s.cases = 1;
                s.starts = e.getValue().first ? 1 : 0;
                s.ends = e.getValue().last ? 1 : 0;
                c.output(nodesTag, KV.of(e.getKey(), s));
            }

            // variants
            final ProcessStats variant = new ProcessStats();
            variant.cases = 1;
            variant.frequency = result.length;
            variant.addDuration(spec.unit.fromMillis(result.durationMillis()));
            c.output(variantsTag, KV.of(result.variant, variant));

            // handovers
            for (final Map.Entry<String, Long> e : result.handovers.entrySet()) {
                final ProcessStats s = new ProcessStats();
                s.frequency = e.getValue();
                s.cases = 1;
                c.output(handoversTag, KV.of(e.getKey(), s));
            }

            // conformance
            for (final CaseReplay.Verdict v : result.verdicts) {
                final ProcessStats s = new ProcessStats();
                s.cases = 1;
                s.applicable = v.applicable() ? 1 : 0;
                s.violations = v.violated() ? 1 : 0;
                c.output(conformanceTag, KV.of(v.name(), s));
            }
        }
    }

    /** Streams the sorted event rows as replay events and captures the case attributes from the first non-null values. */
    static final class EventIterator implements Iterator<CaseReplay.Event> {
        private final Iterator<KV<Long, MElement>> rows;
        private final Map<String, Object> attributes;

        EventIterator(final Iterator<KV<Long, MElement>> rows, final Map<String, Object> attributes) {
            this.rows = rows;
            this.attributes = attributes;
        }

        @Override
        public boolean hasNext() {
            return rows.hasNext();
        }

        @Override
        public CaseReplay.Event next() {
            final KV<Long, MElement> row = rows.next();
            final MElement e = row.getValue();
            for (final Map.Entry<String, Object> a : attributes.entrySet()) {
                if (a.getValue() == null) {
                    final Object v = e.getPrimitiveValue(a.getKey());
                    if (v != null) a.setValue(v);
                }
            }
            final Object integer = e.getPrimitiveValue(EVENT_SEQUENCE_INTEGER);
            final Object number = e.getPrimitiveValue(EVENT_SEQUENCE_NUMBER);
            final Object text = e.getPrimitiveValue(EVENT_SEQUENCE_TEXT);
            final Comparable<?> sequence = integer instanceof Long l ? l : number instanceof Double d ? d : (text == null ? null : text.toString());
            final Object resource = e.getPrimitiveValue(EVENT_RESOURCE);
            return new CaseReplay.Event(e.getAsString(EVENT_ACTIVITY), row.getKey(), resource == null ? null : resource.toString(), sequence);
        }
    }

    // ---- output records ----

    private static void putDurations(final Map<String, Object> values, final ProcessStats stats) {
        values.put("durationMean", stats.durationMean());
        values.put("durationMin", stats.durationMinOrNull());
        values.put("durationMax", stats.durationMaxOrNull());
        final Double[] quantiles = stats.durationQuantiles(0.5, 0.95);
        values.put("durationMedian", quantiles[0]);
        values.put("durationP95", quantiles[1]);
    }

    static class EdgesDoFn extends DoFn<KV<String, ProcessStats>, MElement> {
        private final long minFrequency;

        EdgesDoFn(final ProcessSpec spec) {
            this.minFrequency = spec.dfgMinFrequency;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final KV<String, ProcessStats> kv = c.element();
            final ProcessStats stats = kv.getValue();
            if (stats.frequency < minFrequency) return;
            final String[] parts = kv.getKey().split(KEY_SEPARATOR, 2);
            final Map<String, Object> values = new HashMap<>();
            values.put("source", parts[0]);
            values.put("target", parts.length > 1 ? parts[1] : null);
            values.put("frequency", stats.frequency);
            values.put("caseCount", stats.cases);
            putDurations(values, stats);
            c.output(MElement.of(values, c.timestamp()));
        }
    }

    static class NodesDoFn extends DoFn<KV<String, ProcessStats>, MElement> {
        @ProcessElement
        public void processElement(final ProcessContext c) {
            final KV<String, ProcessStats> kv = c.element();
            final ProcessStats stats = kv.getValue();
            final Map<String, Object> values = new HashMap<>();
            values.put("activity", kv.getKey());
            values.put("frequency", stats.frequency);
            values.put("caseCount", stats.cases);
            values.put("startCount", stats.starts);
            values.put("endCount", stats.ends);
            c.output(MElement.of(values, c.timestamp()));
        }
    }

    /** All variants of a window under one key: the first pass sums the cases, the second emits each variant with its share. */
    static class VariantsDoFn extends DoFn<KV<String, Iterable<KV<String, ProcessStats>>>, MElement> {
        @ProcessElement
        public void processElement(final ProcessContext c) {
            final Iterable<KV<String, ProcessStats>> variants = c.element().getValue();
            long total = 0;
            for (final KV<String, ProcessStats> kv : variants) total += kv.getValue().cases;
            for (final KV<String, ProcessStats> kv : variants) {
                final ProcessStats stats = kv.getValue();
                final Map<String, Object> values = new HashMap<>();
                values.put("variant", kv.getKey());
                final List<String> activities = new ArrayList<>(Arrays.asList(VARIANT_SPLIT.split(kv.getKey())));
                if (!activities.isEmpty() && activities.get(activities.size() - 1).startsWith("...(+")) activities.remove(activities.size() - 1);
                values.put("activities", activities);
                values.put("length", stats.frequency / Math.max(1, stats.cases));
                values.put("caseCount", stats.cases);
                values.put("caseShare", total == 0 ? 0D : (double) stats.cases / total);
                putDurations(values, stats);
                c.output(MElement.of(values, c.timestamp()));
            }
        }
    }

    static class HandoversDoFn extends DoFn<KV<String, ProcessStats>, MElement> {
        @ProcessElement
        public void processElement(final ProcessContext c) {
            final KV<String, ProcessStats> kv = c.element();
            final ProcessStats stats = kv.getValue();
            final String[] parts = kv.getKey().split(CaseReplay.HANDOVER_SEPARATOR, 2);
            final Map<String, Object> values = new HashMap<>();
            values.put("source", parts[0]);
            values.put("target", parts.length > 1 ? parts[1] : null);
            values.put("frequency", stats.frequency);
            values.put("caseCount", stats.cases);
            c.output(MElement.of(values, c.timestamp()));
        }
    }

    static class ConformanceDoFn extends DoFn<KV<String, ProcessStats>, MElement> {
        private final Map<String, String> types = new HashMap<>();

        ConformanceDoFn(final ProcessSpec spec) {
            for (final ProcessSpec.Constraint constraint : spec.constraints) types.put(constraint.name, constraint.type.name());
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final KV<String, ProcessStats> kv = c.element();
            final ProcessStats stats = kv.getValue();
            final Map<String, Object> values = new HashMap<>();
            values.put("constraint", kv.getKey());
            values.put("type", types.get(kv.getKey()));
            values.put("cases", stats.cases);
            values.put("applicable", stats.applicable);
            values.put("violations", stats.violations);
            values.put("violationRate", stats.applicable == 0 ? 0D : (double) stats.violations / stats.applicable);
            c.output(MElement.of(values, c.timestamp()));
        }
    }
}
