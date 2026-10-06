package com.mercari.solution.util.pipeline.profile;

import com.mercari.solution.module.MElement;
import com.mercari.solution.util.FailureUtil;
import com.mercari.solution.util.cloud.google.StorageUtil;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.domain.file.ResourceUtil;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.regex.Pattern;

/**
 * The profile transform's graph (profile-engine.md §2): extract the profiled fields once, build
 * the whole-dataset profile and from it the edges (first pass), count every group's rows over
 * those edges (second pass), then finalize — records, report file, payload file, summary.
 */
public class ProfileStages {

    private static final Logger LOG = LoggerFactory.getLogger(ProfileStages.class);

    public record Outputs(
            PCollection<MElement> fields,
            PCollection<MElement> groups,
            PCollection<MElement> values,
            PCollection<MElement> bins,
            PCollection<MElement> pairs,
            PCollection<MElement> target,
            PCollection<MElement> keys,
            PCollection<MElement> summary,
            PCollection<BadRecord> failures) {}

    public static Outputs apply(
            final PCollection<MElement> input,
            final ProfileSpec spec,
            final ProfileReport.Config config,
            final int fanout,
            final boolean failFast) {

        // extract the profiled fields once per element; conversion failures go to the failure output
        final TupleTag<ProfileRow> rowTag = new TupleTag<>() {};
        final TupleTag<BadRecord> failureTag = new TupleTag<>() {};
        final PCollectionTuple extracted = input
                .apply("ExtractRows", ParDo
                        .of(new ExtractDoFn(config.moduleName, spec, failureTag, failFast))
                        .withOutputTags(rowTag, TupleTagList.of(failureTag)));
        final PCollection<ProfileRow> rows = extracted.get(rowTag)
                .setCoder(SerializableCoder.of(ProfileRow.class));

        // first pass: the whole-dataset profile
        final PCollection<ProfileAccumulator> profile = rows
                .apply("Profile", Combine.globally(new ProfileCombineFn(spec)).withFanout(fanout))
                .setCoder(SerializableCoder.of(ProfileAccumulator.class));

        // comparison groups are bounded before any shuffle of rows (top-K by rows / most recent time
        // buckets), so a high-cardinality segment field cannot fan out into millions of accumulators
        PCollectionView<Map<String, Long>> selectedGroupsView = null;
        if(!config.axes.isEmpty()) {
            selectedGroupsView = rows
                    .apply("GroupOfRow", ParDo.of(new GroupOfRowDoFn(config.axes)))
                    .setCoder(StringUtf8Coder.of())
                    .apply("CountGroupRows", Count.perElement())
                    .apply("KeyByAxisId", ParDo.of(new AxisOfGroupDoFn(config.axes)))
                    .setCoder(KvCoder.of(StringUtf8Coder.of(), KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of())))
                    .apply("GroupByAxis", GroupByKey.create())
                    .apply("SelectGroups", ParDo.of(new SelectGroupsDoFn(config.axes, config.timeGroupLimit)))
                    .setCoder(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()))
                    .apply("AsSelectedGroups", View.asMap());
        }

        // second pass: exact counts of every group over the edges of the first
        PCollectionView<ProfileEdges> edgesView = null;
        PCollectionView<Map<String, ProfileCells>> cellsView = null;
        if(config.countingPass) {
            edgesView = profile
                    .apply("Edges", ParDo.of(new EdgesDoFn(config)))
                    .setCoder(SerializableCoder.of(ProfileEdges.class))
                    .apply("AsEdges", View.asSingleton());
            final LocateDoFn locateDoFn = new LocateDoFn(spec, config, edgesView, selectedGroupsView);
            cellsView = rows
                    .apply("Locate", selectedGroupsView == null
                            ? ParDo.of(locateDoFn).withSideInputs(edgesView)
                            : ParDo.of(locateDoFn).withSideInputs(edgesView, selectedGroupsView))
                    .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(ProfileCells.Row.class)))
                    .apply("Count", Combine.<String, ProfileCells.Row, ProfileCells>perKey(new ProfileCells.Fn(spec.getFields().size()))
                            .withHotKeyFanout(fanout))
                    .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(ProfileCells.class)))
                    .apply("AsCells", View.asMap());
        }

        final TupleTag<MElement> fieldsTag = new TupleTag<>() {};
        final TupleTag<MElement> groupsTag = new TupleTag<>() {};
        final TupleTag<MElement> valuesTag = new TupleTag<>() {};
        final TupleTag<MElement> binsTag = new TupleTag<>() {};
        final TupleTag<MElement> pairsTag = new TupleTag<>() {};
        final TupleTag<MElement> targetTag = new TupleTag<>() {};
        final TupleTag<MElement> keysTag = new TupleTag<>() {};
        final TupleTag<MElement> summaryTag = new TupleTag<>() {};
        final FinalizeDoFn finalizeDoFn = new FinalizeDoFn(
                config, edgesView, cellsView, selectedGroupsView,
                groupsTag, valuesTag, binsTag, pairsTag, targetTag, keysTag, summaryTag);
        final List<PCollectionView<?>> sideInputs = new ArrayList<>();
        if(edgesView != null) {
            sideInputs.add(edgesView);
            sideInputs.add(cellsView);
        }
        if(selectedGroupsView != null) {
            sideInputs.add(selectedGroupsView);
        }
        final PCollectionTuple finalized = profile
                .apply("Finalize", ParDo.of(finalizeDoFn)
                        .withSideInputs(sideInputs)
                        .withOutputTags(fieldsTag, TupleTagList.of(groupsTag).and(valuesTag).and(binsTag)
                                .and(pairsTag).and(targetTag).and(keysTag).and(summaryTag)));

        return new Outputs(
                finalized.get(fieldsTag).setCoder(ElementCoder.of(ProfileReport.fieldsSchema())),
                finalized.get(groupsTag).setCoder(ElementCoder.of(ProfileReport.groupsSchema())),
                finalized.get(valuesTag).setCoder(ElementCoder.of(ProfileReport.valuesSchema())),
                finalized.get(binsTag).setCoder(ElementCoder.of(ProfileReport.binsSchema())),
                finalized.get(pairsTag).setCoder(ElementCoder.of(ProfileReport.pairsSchema())),
                finalized.get(targetTag).setCoder(ElementCoder.of(ProfileReport.targetSchema())),
                finalized.get(keysTag).setCoder(ElementCoder.of(ProfileReport.keysSchema())),
                finalized.get(summaryTag).setCoder(ElementCoder.of(ProfileReport.summarySchema())),
                extracted.get(failureTag));
    }

    /**
     * Reduces each element to its profiled fields. A field whose conversion throws is recorded as
     * a field error and the row is routed to the failure output (or fails the job with failFast);
     * the row still counts in the profile so the result shows what went wrong and where.
     */
    private static class ExtractDoFn extends DoFn<MElement, ProfileRow> {

        private static final int MAX_LOGGED_FAILURES = 20;

        private final String name;
        private final ProfileSpec spec;
        private final TupleTag<BadRecord> failureTag;
        private final boolean failFast;
        private final Counter failures;

        private transient int logged;

        ExtractDoFn(final String name, final ProfileSpec spec, final TupleTag<BadRecord> failureTag, final boolean failFast) {
            this.name = name;
            this.spec = spec;
            this.failureTag = failureTag;
            this.failFast = failFast;
            this.failures = Metrics.counter(name, "profile_extract_failures");
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final MElement element = c.element();
            if(element == null) {
                return;
            }
            final ProfileRow row = ProfileRow.of(spec, element);
            if(row.isFailed()) {
                final String message = String.format(
                        "profile transform `%s` failed to extract field `%s`: %s", name, row.failedField, row.failureMessage);
                failures.inc();
                if(failFast) {
                    throw new IllegalStateException(message + " for input: " + element);
                }
                if(logged < MAX_LOGGED_FAILURES) {
                    logged += 1;
                    LOG.warn("{} for input: {}", message, element);
                }
                c.output(failureTag, FailureUtil.createBadRecord(
                        element, message, new IllegalStateException(row.failureMessage)));
            }
            c.output(row);
        }
    }

    /** The group of a row on one axis, or null when it has none (a field that failed to extract is already an error). */
    private static String groupOf(final ProfileAxis axis, final ProfileRow row) {
        if(ProfileAxis.Kind.inputs.equals(axis.kind)) {
            return axis.groupOfInputIndex(row.inputIndex);
        }
        final Object value = axis.fieldIndex < 0 || axis.fieldIndex >= row.values.length
                ? null : row.values[axis.fieldIndex];
        return value == ProfileRow.Marker.ERROR ? null : axis.groupValue(value);
    }

    /** Emits the group key of each row on every comparison axis (only the keys: the group sizes are counted from them). */
    private static class GroupOfRowDoFn extends DoFn<ProfileRow, String> {

        private final List<ProfileAxis> axes;

        GroupOfRowDoFn(final List<ProfileAxis> axes) {
            this.axes = axes;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final ProfileRow row = c.element();
            if(row == null || row.values == null) {
                return;
            }
            for(final ProfileAxis axis : axes) {
                final String group = groupOf(axis, row);
                if(group != null) {
                    c.output(axis.groupKey(group));
                }
            }
        }
    }

    /** Re-keys (groupKey, rows) by the axis the group belongs to. */
    private static class AxisOfGroupDoFn extends DoFn<KV<String, Long>, KV<String, KV<String, Long>>> {

        private final List<ProfileAxis> axes;

        AxisOfGroupDoFn(final List<ProfileAxis> axes) {
            this.axes = axes;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final KV<String, Long> groupCount = c.element();
            for(final ProfileAxis axis : axes) {
                if(axis.groupOfKey(groupCount.getKey()) != null) {
                    c.output(KV.of(axis.id(), groupCount));
                    return;
                }
            }
        }
    }

    /**
     * Keeps, per axis, only the groups the result will show (segments: the largest {@code topK}
     * by rows; time: the most recent {@code timeGroupLimit} buckets; inputs: all) and emits the
     * axis's total distinct group count under the axis id, which no group key equals.
     * A bounded heap keeps memory at O(limit) however many groups an axis has.
     */
    private static class SelectGroupsDoFn extends DoFn<KV<String, Iterable<KV<String, Long>>>, KV<String, Long>> {

        private final List<ProfileAxis> axes;
        private final int timeGroupLimit;

        SelectGroupsDoFn(final List<ProfileAxis> axes, final int timeGroupLimit) {
            this.axes = axes;
            this.timeGroupLimit = timeGroupLimit;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final String axisId = c.element().getKey();
            ProfileAxis axis = null;
            for(final ProfileAxis candidate : axes) {
                if(candidate.id().equals(axisId)) {
                    axis = candidate;
                }
            }
            if(axis == null) {
                return;
            }
            final int limit;
            final Comparator<KV<String, Long>> keepOrder;   // ascending: the head is evicted first
            switch (axis.kind) {
                case inputs -> {
                    limit = Integer.MAX_VALUE;
                    keepOrder = Comparator.comparing(KV::getKey);
                }
                case time -> {
                    limit = timeGroupLimit;
                    keepOrder = Comparator.comparing(KV::getKey);   // chronological labels: oldest evicted first
                }
                default -> {
                    limit = axis.topK;
                    keepOrder = Comparator.<KV<String, Long>>comparingLong(KV::getValue).thenComparing(KV::getKey);
                }
            }
            final PriorityQueue<KV<String, Long>> kept = new PriorityQueue<>(keepOrder);
            long total = 0;
            for(final KV<String, Long> groupCount : c.element().getValue()) {
                total += 1;
                kept.add(groupCount);
                if(kept.size() > limit) {
                    kept.poll();
                }
            }
            for(final KV<String, Long> groupCount : kept) {
                c.output(groupCount);
            }
            c.output(KV.of(axisId, total));
        }
    }

    /** The edges of the counting pass, from a copy of the profile (querying a sketch mutates its lazily sorted state). */
    private static class EdgesDoFn extends DoFn<ProfileAccumulator, ProfileEdges> {

        private final ProfileReport.Config config;

        EdgesDoFn(final ProfileReport.Config config) {
            this.config = config;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            if(c.element() != null) {
                c.output(ProfileEdges.of(c.element().copy(), config.declaredEdges, config.comparePairs, config.categoryLimit));
            }
        }
    }

    /**
     * Locates each row on the edges once and emits it under every group it is counted in: the
     * whole dataset, its target class, and its group on each axis when that group was kept.
     */
    private static class LocateDoFn extends DoFn<ProfileRow, KV<String, ProfileCells.Row>> {

        private final ProfileSpec spec;
        private final List<ProfileAxis> axes;
        private final boolean hasDeclared;
        private final PCollectionView<ProfileEdges> edgesView;
        private final PCollectionView<Map<String, Long>> selectedGroupsView;

        LocateDoFn(
                final ProfileSpec spec,
                final ProfileReport.Config config,
                final PCollectionView<ProfileEdges> edgesView,
                final PCollectionView<Map<String, Long>> selectedGroupsView) {

            this.spec = spec;
            this.axes = config.axes;
            this.hasDeclared = config.hasDeclaredEdges();
            this.edgesView = edgesView;
            this.selectedGroupsView = selectedGroupsView;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final ProfileRow source = c.element();
            if(source == null || source.values == null) {
                return;
            }
            final ProfileEdges edges = c.sideInput(edgesView);
            final ProfileCells.Row row = ProfileCells.locate(spec, edges, source, hasDeclared);
            c.output(KV.of(ProfileReport.ALL_KEY, ProfileCells.withPairs(edges, row)));
            if(row.targetClass >= 0) {
                c.output(KV.of(row.targetClass == 1 ? ProfileReport.TARGET_POSITIVE_KEY : ProfileReport.TARGET_NEGATIVE_KEY, row));
            }
            if(selectedGroupsView != null) {
                final Map<String, Long> selected = c.sideInput(selectedGroupsView);
                for(final ProfileAxis axis : axes) {
                    final String group = groupOf(axis, source);
                    if(group != null && selected.containsKey(axis.groupKey(group))) {
                        c.output(KV.of(axis.groupKey(group), row));
                    }
                }
            }
        }
    }

    /**
     * Builds the result from the first pass's profile (the element) and the counting pass (side
     * inputs), emits the records, writes the report and payload files, and only then emits the
     * summary — so a step waiting on the summary runs after the files exist.
     */
    private static class FinalizeDoFn extends DoFn<ProfileAccumulator, MElement> {

        /** {@code scheme://...} with a scheme of two or more characters (a single letter is a Windows drive). */
        private static final Pattern URI_PATTERN = Pattern.compile("^[a-zA-Z][a-zA-Z0-9+.-]+://.*");

        private final ProfileReport.Config config;
        private final PCollectionView<ProfileEdges> edgesView;
        private final PCollectionView<Map<String, ProfileCells>> cellsView;
        private final PCollectionView<Map<String, Long>> selectedGroupsView;
        private final TupleTag<MElement> groupsTag;
        private final TupleTag<MElement> valuesTag;
        private final TupleTag<MElement> binsTag;
        private final TupleTag<MElement> pairsTag;
        private final TupleTag<MElement> targetTag;
        private final TupleTag<MElement> keysTag;
        private final TupleTag<MElement> summaryTag;

        FinalizeDoFn(
                final ProfileReport.Config config,
                final PCollectionView<ProfileEdges> edgesView,
                final PCollectionView<Map<String, ProfileCells>> cellsView,
                final PCollectionView<Map<String, Long>> selectedGroupsView,
                final TupleTag<MElement> groupsTag,
                final TupleTag<MElement> valuesTag,
                final TupleTag<MElement> binsTag,
                final TupleTag<MElement> pairsTag,
                final TupleTag<MElement> targetTag,
                final TupleTag<MElement> keysTag,
                final TupleTag<MElement> summaryTag) {

            this.config = config;
            this.edgesView = edgesView;
            this.cellsView = cellsView;
            this.selectedGroupsView = selectedGroupsView;
            this.groupsTag = groupsTag;
            this.valuesTag = valuesTag;
            this.binsTag = binsTag;
            this.pairsTag = pairsTag;
            this.targetTag = targetTag;
            this.keysTag = keysTag;
            this.summaryTag = summaryTag;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) throws Exception {
            if(c.element() == null) {
                return;
            }
            // reading the result queries the sketches, which mutates their lazily sorted state —
            // work on a copy so the input element stays byte-identical
            final ProfileAccumulator accumulator = c.element().copy();
            final ProfileEdges edges = edgesView == null ? null : c.sideInput(edgesView);
            final Map<String, ProfileCells> cells = cellsView == null ? null : c.sideInput(cellsView);
            Map<String, Long> groupTotals = null;
            if(selectedGroupsView != null) {
                groupTotals = new HashMap<>();
                for(final ProfileAxis axis : config.axes) {
                    final Long total = c.sideInput(selectedGroupsView).get(axis.id());
                    if(total != null) {
                        groupTotals.put(axis.id(), total);
                    }
                }
            }
            final ProfileReport.Result result = ProfileReport.build(accumulator, cells, edges, config, groupTotals);

            for(final MElement record : ProfileReport.fieldRecords(result, config)) {
                c.output(record);
            }
            for(final MElement record : ProfileReport.groupRecords(result, config)) {
                c.output(groupsTag, record);
            }
            if(config.outputValues) {
                for(final MElement record : ProfileReport.valueRecords(result, config)) {
                    c.output(valuesTag, record);
                }
            }
            if(config.outputBins) {
                for(final MElement record : ProfileReport.binRecords(result, config)) {
                    c.output(binsTag, record);
                }
            }
            for(final MElement record : ProfileReport.pairRecords(result, config)) {
                c.output(pairsTag, record);
            }
            for(final MElement record : ProfileReport.targetRecords(result, config)) {
                c.output(targetTag, record);
            }
            for(final MElement record : ProfileReport.keyRecords(result, config)) {
                c.output(keysTag, record);
            }

            Long reportBytes = null;
            List<String> degradations = List.of();
            if(config.reportOutput != null || config.payloadOutput != null) {
                final ProfileRenderer.Rendered rendered = ProfileRenderer.render(result, accumulator, config);
                degradations = rendered.degradations;
                if(config.reportOutput != null) {
                    final byte[] html = rendered.html.getBytes(StandardCharsets.UTF_8);
                    write(config.reportOutput, html, "text/html");
                    reportBytes = (long) html.length;
                    LOG.info("profile transform `{}` wrote report to: {} ({} bytes, rows: {}, error rows: {})",
                            config.moduleName, config.reportOutput, html.length, result.rows, result.errorRows);
                }
                if(config.payloadOutput != null) {
                    write(config.payloadOutput, ProfileRenderer.payloadFile(rendered).getBytes(StandardCharsets.UTF_8), "application/json");
                    LOG.info("profile transform `{}` wrote payload to: {}", config.moduleName, config.payloadOutput);
                }
            }
            if(result.target != null && result.target.warning != null) {
                LOG.warn("profile transform `{}` target: {}", config.moduleName, result.target.warning);
            }
            c.output(summaryTag, ProfileReport.summaryRecord(result, config, reportBytes, degradations));
        }

        /**
         * GCS keeps its content type (the report is meant to be opened in place), every other
         * {@code scheme://} uri goes through Beam FileSystems (s3, hdfs, ...), and anything else is a
         * local path (a Windows drive letter is not a scheme, which Beam FileSystems would take it for).
         */
        private static void write(final String path, final byte[] bytes, final String contentType) throws Exception {
            if(path.startsWith("gs://")) {
                StorageUtil.writeBytes(path, bytes, contentType, Map.of(), Map.of());
            } else if(URI_PATTERN.matcher(path).matches()) {
                ResourceUtil.writeBytes(path, bytes);
            } else {
                final Path localPath = Paths.get(path);
                if(localPath.getParent() != null) {
                    Files.createDirectories(localPath.getParent());
                }
                Files.write(localPath, bytes);
            }
        }
    }
}
