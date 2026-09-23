package com.mercari.solution.util.pipeline.feature;

import com.mercari.solution.MPipeline;
import com.mercari.solution.module.*;
import com.mercari.solution.module.Module;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.pipeline.feature.FeaturePlan.Stage;
import com.mercari.solution.util.pipeline.feature.FeaturePlan.StageKind;
import com.mercari.solution.util.pipeline.feature.FeatureSpec.Scope;
import com.mercari.solution.util.pipeline.feature.SequenceEvaluator.Past;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.ApproximateQuantiles;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.*;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.*;

/**
 * Beam wiring of a {@link FeaturePlan} (docs/design/feature-engine.md §3, §9.4): one ParDo / GroupByKey per
 * stage, each appending its columns to the element's primitive map. The stages are executed wave by wave
 * ({@link FeaturePlan#getWaves()}): the independent stages of a wave branch from the same input in
 * parallel, each emitting only its own columns keyed by a row id, and the branches are merged back into
 * full rows — inside the next stage's GroupByKey when that stage is a single context stage (or the groupBy
 * finalize), by a row-id GroupByKey otherwise. A wave of one stage is the plain linear chain. Rows travel
 * as {@code DataType.ELEMENT} maps keyed by canonical column names and are converted to the output schema
 * in the final stage.
 */
public final class FeatureStages {

    private static final Logger LOG = LoggerFactory.getLogger(FeatureStages.class);

    /**
     * Spill options of the keyed stages: {@code engine.spill} of the feature parameters, then the
     * {@code --featureSpillMemoryMB} pipeline option; the memory default (null) is derived from the worker
     * heap at setup ({@link KeyedSpillSorter#defaultMemoryMB()}).
     */
    static KeyedSpillSorter.Options spillOptions(final FeatureSpec spec, final PipelineOptions options) {
        Integer memoryMB = spec.engine.spillMemoryMB;
        if (memoryMB == null && options != null) {
            memoryMB = options.as(MPipeline.MPipelineOptions.class).getFeatureSpillMemoryMB();
        }
        return new KeyedSpillSorter.Options(memoryMB, spec.engine.spillDirectory, spec.engine.spillCompress);
    }

    /** Key used for rows whose key fields contain null: they bypass keyed evaluation (§3.2). */
    static final String NULL_KEY = "\u0000";

    static final char KEY_SEPARATOR = (char) 1;

    /** Array field holding the child rows of a {@code output.groupBy} record. */
    public static final String ROWS_FIELD = "rows";

    /** Row identity of the fan-out merge ({@code engine.rowId} or a random id); an intermediate field the finalize drops. */
    static final String ROW_ID_FIELD = "__rowId";
    /** Marks a partial row (one branch's columns + the row id + the merge key) on its way to the merge. */
    static final String PARTIAL_FIELD = "__partial";

    private FeatureStages() {}

    public record Result(PCollection<MElement> output, List<PCollection<BadRecord>> failures) {}

    public static Result apply(final PCollection<MElement> input,
                               final Schema inputSchema,
                               final FeaturePlan plan,
                               final Schema outputSchema,
                               final List<Logging> loggings,
                               final boolean failFast) {

        final FeatureSpec spec = plan.getSpec();
        final Schema elementSchema = Schema.builder(inputSchema).withType(DataType.ELEMENT).build();
        final Coder<MElement> elementCoder = ElementCoder.of(elementSchema);
        final KvCoder<String, MElement> kvCoder = KvCoder.of(StringUtf8Coder.of(), elementCoder);
        // keyed stages carry (key, (event millis, row)) so KeyedSpillSorter can order the rows of each key
        final KvCoder<String, KV<Long, MElement>> sortKvCoder = KvCoder.of(StringUtf8Coder.of(), KvCoder.of(BigEndianLongCoder.of(), elementCoder));
        final KeyedSpillSorter sorter = new KeyedSpillSorter(spillOptions(spec, input.getPipeline().getOptions()), elementCoder);
        final List<PCollection<BadRecord>> failures = new ArrayList<>();
        final Map<String, OutputColumn> columns = new HashMap<>();
        for (final OutputColumn c : plan.getColumns()) columns.put(c.getCanonicalName(), c);
        final FeatureSpec.ContextDef groupBy = groupByContext(plan);

        // execution waves (engine doc §9.4): the groupBy stage is the finalize, not a stage of the chain
        final List<List<Stage>> waves = plan.getEngineWaves();
        // streaming stays linear: the fan-out merge is a GroupByKey (the stateful merge is the streaming follow-up, §9.4.6)
        final boolean parallel = spec.engine.parallelWaves
                && !com.mercari.solution.util.pipeline.OptionUtil.isStreaming(input)
                && waves.stream().anyMatch(w -> w.size() >= 2);
        if (parallel) {
            LOG.info("feature engine: {} stages in {} waves, parallel branches per wave {}", plan.getStages().size(), waves.size(),
                    waves.stream().map(List::size).toList());
        }

        // every row becomes an ELEMENT map (whatever the source type) timestamped by time.field, which is
        // the time axis of all keyed stages (strictly-past windows, maxAge, ordering)
        final TupleTag<MElement> elementTag = new TupleTag<>() {};
        final TupleTag<BadRecord> elementFailureTag = new TupleTag<>() {};
        final TupleTag<KV<String, Double>> auditTag = new TupleTag<>() {};
        final SourceContract.FieldContract timeContract = plan.getInputFields().get(spec.timeField);
        final String timeFieldType = timeContract == null || timeContract.getType() == null ? "timestamp" : timeContract.getType().getType().name();
        // the observedAt audit (DSL spec §7) rides the same pass: counters always, the quantile samples only
        // when a run manifest will be written (batch)
        final boolean streaming = com.mercari.solution.util.pipeline.OptionUtil.isStreaming(input);
        final List<FeaturePlan.ObservedAtAudit> audits = plan.getRunnableObservedAtAudits();
        final boolean runManifest = spec.output.manifest != null && !streaming;
        final PCollectionTuple elements = input.apply("ToElement", ParDo
                .of(new ToElementDoFn(spec.timeField, timeFieldType, parallel ? spec.engine.rowId : null, failFast, elementFailureTag,
                        audits, "fail".equals(spec.audit.observedAt), runManifest ? auditTag : null))
                .withOutputTags(elementTag, TupleTagList.of(elementFailureTag).and(auditTag)));
        failures.add(elements.get(elementFailureTag));
        PCollection<MElement> current = elements.get(elementTag).setCoder(elementCoder);
        final PCollection<KV<String, Double>> auditSamples = elements.get(auditTag).setCoder(KvCoder.of(StringUtf8Coder.of(), DoubleCoder.of()));

        final Wiring wiring = new Wiring(plan, columns, elementCoder, kvCoder, sortKvCoder, sorter, loggings, failFast, failures);
        PCollection<MElement> pending = null; // base + partials of the last wave, merged inside the groupBy finalize
        int pendingBranches = 0;
        // a declared engine.rowId is a deterministic function of the row; a random id must ride a GroupByKey
        // or a Reshuffle before any fan-out reads it (a retry must not recompute what a branch already saw)
        boolean pinned = !spec.engine.rowId.isEmpty();
        if (!parallel) {
            for (final Stage stage : plan.getStages()) {
                if (stage.kind() != StageKind.groupBy) current = wiring.applyStage(current, stage);
            }
        } else {
            for (int w = 0; w < waves.size(); w++) {
                final List<Stage> wave = waves.get(w);
                if (wave.size() == 1) {
                    current = wiring.applyStage(current, wave.get(0));
                    // a keyed stage's GroupByKey materialises the row ids like the pin Reshuffle would
                    if (wave.get(0).kind() != StageKind.row && wave.get(0).kind() != StageKind.fit) pinned = true;
                    continue;
                }
                if (!pinned) {
                    // random row ids are pinned before the first fan-out: every branch must see the id a retry may
                    // recompute (a fit / row stage between the id assignment and the fan-out is no barrier)
                    current = current.apply("RowId_Pin", Reshuffle.viaRandomKey());
                    pinned = true;
                }
                // the row columns the wave's stages host are placed in their first consumer's stage by the scheduler
                // and carried on by the linear chain; the other branches read the wave input, so every row column
                // computable from it (input fields, earlier waves, such row columns) is evaluated on it first
                current = wiring.applyRows(current, "Wave" + (w + 1) + "_Rows", w);
                // fan-out: every branch reads the wave input and emits only its own columns (+ row id + merge key)
                final Stage foldInto = w + 1 < waves.size() && waves.get(w + 1).size() == 1 ? plan.getFoldTarget(waves.get(w + 1).get(0), w) : null;
                final boolean foldGroupBy = foldInto == null && w + 1 == waves.size() && groupBy != null && plan.keysAvailable(groupBy.keys(), w);
                final List<String> carry = foldInto != null ? foldInto.keys() : foldGroupBy ? groupBy.keys() : List.of();
                final List<PCollection<MElement>> pieces = new ArrayList<>();
                pieces.add(current);
                for (final Stage stage : wave) {
                    final PCollection<MElement> out = wiring.applyStage(current, stage);
                    pieces.add(out.apply(Wiring.label(stage) + "_Partial", ParDo.of(new PartialDoFn(stage.columnNames(), carry))).setCoder(elementCoder));
                }
                final String name = "Wave" + (w + 1);
                if (foldInto != null) {
                    // the merge rides the next stage's GroupByKey: the pieces are keyed by that stage's key and the
                    // rows are reassembled by row id inside each group (ContextStageDoFn)
                    // (a variance-components estimate of that stage reads the wave input, not the flattened pieces)
                    current = wiring.applyStage(PCollectionList.of(pieces).apply(name + "_FanIn", Flatten.pCollections()), foldInto, current, wave.size());
                    w++;
                } else if (foldGroupBy) {
                    pending = PCollectionList.of(pieces).apply(name + "_FanIn", Flatten.pCollections());
                    pendingBranches = wave.size();
                } else {
                    current = wiring.merge(name + "_Merge", pieces, wave.size());
                }
            }
        }

        final TupleTag<MElement> outputTag = new TupleTag<>() {};
        final TupleTag<BadRecord> failureTag = new TupleTag<>() {};
        final TupleTag<KV<String, Double>> countTag = new TupleTag<>() {};
        final PCollectionTuple finalized;
        if (groupBy == null) {
            finalized = current.apply("Finalize", ParDo
                    .of(new FinalizeDoFn(plan.getEmittedColumns(), inputSchema, outputSchema, spec.output.nullPolicy, loggings, failFast, failureTag, runManifest ? countTag : null))
                    .withOutputTags(outputTag, TupleTagList.of(failureTag).and(countTag)));
        } else {
            finalized = (pending != null ? pending : current)
                    .apply("Finalize_Key", ParDo.of(new KeyDoFn(groupBy.keys()))).setCoder(kvCoder)
                    .apply("Finalize_Group", GroupByKey.create())
                    .apply("Finalize", ParDo
                            .of(new GroupedFinalizeDoFn(plan.getEmittedColumns(), inputSchema, outputSchema, spec.output.nullPolicy,
                                    groupBy.keys(), spec.output.parentFields, spec.output.childName, pendingBranches, loggings, failFast, failureTag, runManifest ? countTag : null))
                            .withOutputTags(outputTag, TupleTagList.of(failureTag).and(countTag)));
        }
        failures.add(finalized.get(failureTag));
        if (runManifest) {
            final PCollection<KV<String, Double>> outputCounts = finalized.get(countTag).setCoder(KvCoder.of(StringUtf8Coder.of(), DoubleCoder.of()));
            writeRunManifest(plan, PCollectionList.of(auditSamples).and(outputCounts).apply("RunManifest_Samples", Flatten.pCollections()), audits);
        }
        return new Result(finalized.get(outputTag).setCoder(ElementCoder.of(outputSchema)), failures);
    }

    /**
     * The run manifest ({@code <output.manifest>.run.json}, batch only): what only execution knows — the output
     * row count and the observedAt audit (per field: rows, late = observed after the declared availability,
     * afterPredictAt, missing, and deciles of {@code predictAt − observedAt} in seconds). Everything is reduced
     * in the global window to one element and written by one worker, like a fit artifact.
     */
    private static void writeRunManifest(final FeaturePlan plan,
                                         final PCollection<KV<String, Double>> auditSamples,
                                         final List<FeaturePlan.ObservedAtAudit> audits) {
        final PCollection<KV<String, Double>> samples = auditSamples.apply("RunManifest_AuditGlobal", Window.<KV<String, Double>>into(new GlobalWindows())
                .triggering(org.apache.beam.sdk.transforms.windowing.DefaultTrigger.of())
                .withAllowedLateness(org.joda.time.Duration.ZERO)
                .discardingFiredPanes());
        final PCollection<String> counts = samples
                .apply("RunManifest_AuditCount", Count.perKey())
                .apply("RunManifest_AuditCountJson", MapElements.into(TypeDescriptors.strings())
                        .via(kv -> "{\"key\":\"" + kv.getKey() + "\",\"count\":" + kv.getValue() + "}"));
        final PCollection<String> quantiles = samples
                .apply("RunManifest_AuditSamples", Filter.by(kv -> !kv.getKey().contains("#")))
                .apply("RunManifest_AuditQuantiles", ApproximateQuantiles.perKey(11))
                .apply("RunManifest_AuditQuantilesJson", MapElements.into(TypeDescriptors.strings())
                        .via(kv -> "{\"key\":\"" + kv.getKey() + "\",\"quantiles\":" + kv.getValue() + "}"));
        final PCollectionView<List<String>> facts = PCollectionList.of(counts).and(quantiles)
                .apply("RunManifest_Facts", Flatten.pCollections())
                .apply("RunManifest_FactsView", View.asList());
        final List<String> auditFields = new ArrayList<>();
        for (final FeaturePlan.ObservedAtAudit a : audits) auditFields.add(a.field());
        auditSamples.getPipeline()
                .apply("RunManifest_Trigger", Create.of(plan.getSpec().output.manifest))
                .apply("RunManifest_Write", ParDo.of(new WriteRunManifestDoFn(plan.getHash(), plan.getOutputHash(), auditFields, facts)).withSideInputs(facts));
    }

    /** Sample key of the finalize row count ({@code #} keeps it out of the quantile keys). */
    static final String OUTPUT_COUNT_KEY = "#rows";

    /** {@code <manifest>.run.json}: the run manifest path next to the assembly-time manifest. */
    public static String runManifestPath(final String manifest) {
        return manifest.endsWith(".json") ? manifest.substring(0, manifest.length() - ".json".length()) + ".run.json" : manifest + ".run.json";
    }

    static class WriteRunManifestDoFn extends DoFn<String, Void> {
        private final String planHash;
        private final String outputHash;
        private final List<String> auditFields;
        private final PCollectionView<List<String>> facts;

        WriteRunManifestDoFn(final String planHash, final String outputHash, final List<String> auditFields, final PCollectionView<List<String>> facts) {
            this.planHash = planHash;
            this.outputHash = outputHash;
            this.auditFields = auditFields;
            this.facts = facts;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final Map<String, Long> counts = new HashMap<>();
            final Map<String, com.google.gson.JsonArray> quantiles = new HashMap<>();
            for (final String fact : c.sideInput(facts)) {
                final com.google.gson.JsonObject o = com.google.gson.JsonParser.parseString(fact).getAsJsonObject();
                final String key = o.get("key").getAsString();
                if (o.has("count")) counts.put(key, o.get("count").getAsLong());
                if (o.has("quantiles")) quantiles.put(key, o.getAsJsonArray("quantiles"));
            }
            final com.google.gson.JsonObject run = new com.google.gson.JsonObject();
            run.addProperty("version", 1);
            run.addProperty("planHash", planHash);
            run.addProperty("outputHash", outputHash);
            run.addProperty("finishedAt", java.time.Instant.now().toString());
            run.addProperty("rows", counts.getOrDefault(OUTPUT_COUNT_KEY, 0L));
            final com.google.gson.JsonObject audit = new com.google.gson.JsonObject();
            for (final String field : auditFields) {
                final com.google.gson.JsonObject o = new com.google.gson.JsonObject();
                o.addProperty("rows", counts.getOrDefault(field + "#rows", 0L));
                o.addProperty("nullValue", counts.getOrDefault(field + "#nullValue", 0L));
                o.addProperty("missing", counts.getOrDefault(field + "#missing", 0L));
                o.addProperty("late", counts.getOrDefault(field + "#late", 0L));
                o.addProperty("afterPredictAt", counts.getOrDefault(field + "#afterPredictAt", 0L));
                o.addProperty("measured", counts.getOrDefault(field, 0L));
                // [min, p10, p20, ..., p90, max] of predictAt − observedAt in seconds (positive = observed before predictAt)
                o.add("leadSecondsDeciles", quantiles.getOrDefault(field, new com.google.gson.JsonArray()));
                audit.add(field, o);
                LOG.info("feature observedAt audit {}: {}", field, o);
            }
            run.add("observedAtAudit", audit);
            final String path = runManifestPath(c.element());
            com.mercari.solution.util.domain.file.ResourceUtil.writeString(path, new com.google.gson.GsonBuilder().setPrettyPrinting().create().toJson(run));
            LOG.info("feature run manifest written to {}", path);
        }
    }

    /** Artifact paths per fitted block for the manifest (static / fold encodings and every static-fit block) — declared or not yet written. */
    public static Map<String, String> artifactPaths(final FeaturePlan plan) {
        final Map<String, String> paths = new LinkedHashMap<>();
        final String version = plan.getArtifactVersion();
        for (final FitLevel level : fitLevels(plan.getColumns())) {
            if (level.artifactUri() != null) paths.put(level.block(), FitArtifact.statsPath(level.artifactUri(), version, level.block()));
        }
        for (final StaticFitBlock<?> block : staticFitBlocks(plan.getColumns())) {
            if (block.artifactUri() != null) paths.put(block.block(), block.artifactPath(version));
        }
        // a rating's state snapshots: one directory per block (a file per rating state and pool inside it)
        for (final RatingSnapshot.Spec spec : RatingSnapshot.specsOf(plan.getColumns(), version)) {
            paths.putIfAbsent(spec.block(), RatingSnapshot.directory(spec.uri(), version, spec.block()));
        }
        return paths;
    }

    /**
     * Every static-fit block among the columns, rebuilt from their coordinates — the single registry of the
     * static-fit population types (factorization, discretize, quantileTransform, svd, smooth, spectralEmbedding)
     * and the joint estimator models of an encoding block: a new type is added here and nowhere else.
     */
    static List<StaticFitBlock<?>> staticFitBlocks(final List<OutputColumn> columns) {
        final List<StaticFitBlock<?>> blocks = new ArrayList<>();
        blocks.addAll(fmSpecs(columns));
        blocks.addAll(discretizeSpecs(columns));
        blocks.addAll(quantileTransformSpecs(columns));
        blocks.addAll(svdSpecs(columns));
        blocks.addAll(smoothSpecs(columns));
        blocks.addAll(spectralSpecs(columns));
        blocks.addAll(jointSpecs(columns));
        return blocks;
    }

    /** The shared objects of the stage wiring: one stage = one ParDo / GroupByKey, whatever wave it runs in. */
    static final class Wiring {
        private final FeaturePlan plan;
        private final Map<String, OutputColumn> columns;
        private final Coder<MElement> elementCoder;
        private final KvCoder<String, MElement> kvCoder;
        private final KvCoder<String, KV<Long, MElement>> sortKvCoder;
        private final KeyedSpillSorter sorter;
        private final List<Logging> loggings;
        private final boolean failFast;
        private final List<PCollection<BadRecord>> failures;
        /** Stage index → the entities whose {@code minInterval} that stage audits (millis): see {@link #assignMinIntervalAudits}. */
        private final Map<Integer, Map<String, Long>> minIntervalAudits;

        Wiring(final FeaturePlan plan, final Map<String, OutputColumn> columns,
               final Coder<MElement> elementCoder, final KvCoder<String, MElement> kvCoder, final KvCoder<String, KV<Long, MElement>> sortKvCoder,
               final KeyedSpillSorter sorter, final List<Logging> loggings, final boolean failFast, final List<PCollection<BadRecord>> failures) {
            this.plan = plan;
            this.columns = columns;
            this.elementCoder = elementCoder;
            this.kvCoder = kvCoder;
            this.sortKvCoder = sortKvCoder;
            this.sorter = sorter;
            this.loggings = loggings;
            this.failFast = failFast;
            this.failures = failures;
            this.minIntervalAudits = assignMinIntervalAudits(plan, columns);
        }

        static String label(final Stage stage) {
            return "Stage" + stage.index() + "_" + stage.kind();
        }

        /**
         * Which stage counts each entity's {@code minInterval} violations ({@code feature/minInterval_<entity>_below}),
         * by stage index. The keyed replay only ever sees the gaps of its own key, and every keyed stage replays the
         * same rows, so an entity is audited
         * <ul>
         *   <li>in a stage keyed exactly by the entity's keys when one of them rests on the declaration — a window
         *       reduced by a filter field is keyed finer ({@code entity.keys + reducedKey}) and would report the gaps
         *       of that sub-key, not the entity's; such a stage is used only when no exact one relies on it;</li>
         *   <li>in one stage only — two stages may share a key (a dependency forces the split) and a wave may branch
         *       several keyed stages over the same rows, and counting in each would count a row several times.</li>
         * </ul>
         * Future stages replay in descending time and are never granted a {@code minInterval}, so they are skipped.
         */
        static Map<Integer, Map<String, Long>> assignMinIntervalAudits(final FeaturePlan plan, final Map<String, OutputColumn> columns) {
            final Map<String, List<String>> entityKeys = new HashMap<>();
            for (final FeatureSpec.EntityDef e : plan.getSpec().entities) entityKeys.put(e.name(), e.keys());
            final Map<String, Long> declared = new LinkedHashMap<>();
            final Map<String, Stage> chosen = new LinkedHashMap<>();
            for (final Stage stage : plan.getStages()) {
                if (stage.kind() != StageKind.sequence && stage.kind() != StageKind.population) continue;
                for (final String name : stage.columnNames()) {
                    final OutputColumn c = columns.get(name);
                    if (c == null) continue;
                    final String entity = c.getCoordinates().get("minIntervalEntity"), interval = c.getCoordinates().get("minInterval");
                    if (entity == null || interval == null) continue;
                    declared.putIfAbsent(entity, Duration.parse(interval).toMillis());
                    final List<String> keys = entityKeys.getOrDefault(entity, List.of());
                    final Stage before = chosen.get(entity);
                    if (before == null || (!before.keys().equals(keys) && stage.keys().equals(keys))) chosen.put(entity, stage);
                }
            }
            final Map<Integer, Map<String, Long>> byStage = new HashMap<>();
            for (final Map.Entry<String, Stage> e : chosen.entrySet()) {
                byStage.computeIfAbsent(e.getValue().index(), i -> new LinkedHashMap<>()).put(e.getKey(), declared.get(e.getKey()));
            }
            return byStage;
        }

        // the anonymous tag subclasses keep their type argument for coder inference; created in static methods so
        // they do not capture the (non-serializable) wiring instance
        static TupleTag<MElement> outputTag() {
            return new TupleTag<>() {};
        }

        static TupleTag<BadRecord> failureTag() {
            return new TupleTag<>() {};
        }

        PCollection<MElement> applyStage(final PCollection<MElement> current, final Stage stage) {
            return applyStage(current, stage, current, 0);
        }

        /**
         * One stage on {@code current}; a variance-components estimate of its columns is computed over
         * {@code estimateInput}. A folded fan-out merge passes {@code fanInBranches} — the branch count of
         * the wave riding this stage's GroupByKey; 0 means the input carries plain rows (no reassembly).
         */
        PCollection<MElement> applyStage(final PCollection<MElement> current, final Stage stage, final PCollection<MElement> estimateInput, final int fanInBranches) {
            final List<OutputColumn> stageColumns = new ArrayList<>();
            for (final String name : stage.columnNames()) stageColumns.add(columns.get(name));
            final StageEvaluator evaluator = new StageEvaluator(stageColumns);
            final TupleTag<MElement> outputTag = outputTag();
            final TupleTag<BadRecord> failureTag = failureTag();
            final String label = label(stage);
            // variance-components pseudo-counts for the composed columns of this stage (batch side input)
            final List<VarianceComponents.LevelSpec> specs = stage.kind() == StageKind.fit ? List.of() : VarianceComponents.specsOf(stageColumns, columns);
            final PCollectionView<Map<String, Double>> lambdas = specs.isEmpty() ? null
                    : VarianceComponents.estimate(estimateInput, specs, label + "_Vc");
            final List<PCollectionView<?>> sideInputs = lambdas == null ? List.of() : List.of(lambdas);
            // a rating's state snapshot is one file per pool: the keyed replay runs once per window, so under any other
            // window every window of a pool would read / write that one file (a window starting from another one's state)
            final List<RatingSnapshot.Spec> snapshots = RatingSnapshot.specsOf(stageColumns, plan.getArtifactVersion());
            if (!snapshots.isEmpty() && !(current.getWindowingStrategy().getWindowFn() instanceof GlobalWindows)) {
                throw new IllegalStateException("the rating snapshot (fit.artifact) of " + snapshots.stream().map(RatingSnapshot.Spec::stateKey).toList()
                        + " requires the global window, but the input is windowed by " + current.getWindowingStrategy().getWindowFn()
                        + ": the replay of a pool runs once per window and every window would share its one snapshot file - drop the module's"
                        + " windowing strategy or the rating's fit.artifact");
            }
            final PCollectionTuple outputs = switch (stage.kind()) {
                case row -> current.apply(label, ParDo
                        .of(new RowStageDoFn(evaluator, lambdas, loggings, failFast, failureTag))
                        .withSideInputs(sideInputs)
                        .withOutputTags(outputTag, TupleTagList.of(failureTag)));
                case context -> current
                        .apply(label + "_Key", ParDo.of(new KeyDoFn(stage.keys()))).setCoder(kvCoder)
                        .apply(label + "_Group", GroupByKey.create())
                        .apply(label, ParDo
                                .of(new ContextStageDoFn(evaluator, lambdas, fanInBranches, loggings, failFast, failureTag))
                                .withSideInputs(sideInputs)
                                .withOutputTags(outputTag, TupleTagList.of(failureTag)));
                // keyed replay: group by key, then sort each key's rows by event time inside the DoFn
                // (KeyedSpillSorter: in memory up to the budget, sorted chunks on worker-local disk beyond,
                // deleted once the key is replayed) so a hot key — or the global level of a shrinkage
                // lattice, which is ONE key holding every row — is never materialised as a list; the replay
                // streams the sorted rows and trims its history
                // a future stage is the same replay in descending time on a mirrored clock (strictly-future windows)
                case sequence, population, future -> current
                        .apply(label + "_Key", ParDo.of(new SortKeyDoFn(stage.keys(), stage.kind() == StageKind.future))).setCoder(sortKvCoder)
                        .apply(label + "_Group", GroupByKey.create())
                        .apply(label, ParDo
                                .of(new KeyedHistoryDoFn(evaluator, lambdas, loggings, failFast, failureTag, sorter, label, stage.kind() == StageKind.future,
                                        minIntervalAudits.getOrDefault(stage.index(), Map.of()), snapshots))
                                .withSideInputs(sideInputs)
                                .withOutputTags(outputTag, TupleTagList.of(failureTag)));
                case fit -> applyFit(current, stageColumns, evaluator, plan.getArtifactVersion(), plan.getSpec().predictAt.getOffset().toMillis(),
                        label, loggings, failFast, outputTag, failureTag);
                default -> throw new IllegalStateException("unexpected stage kind: " + stage.kind());
            };
            failures.add(outputs.get(failureTag));
            return outputs.get(outputTag).setCoder(elementCoder);
        }

        /** Evaluates {@link FeaturePlan#getPreludeColumns} on the wave input before its fan-out (no-op when there are none). */
        PCollection<MElement> applyRows(final PCollection<MElement> current, final String name, final int w) {
            final List<OutputColumn> prelude = plan.getPreludeColumns(w);
            if (prelude.isEmpty()) return current;
            final TupleTag<MElement> outputTag = outputTag();
            final TupleTag<BadRecord> failureTag = failureTag();
            // a variance-components compose column in the prelude reads the same lambdas the linear chain
            // would (getPreludeColumns guards that the estimate's fields are on the wave input)
            final List<VarianceComponents.LevelSpec> specs = VarianceComponents.specsOf(prelude, columns);
            final PCollectionView<Map<String, Double>> lambdas = specs.isEmpty() ? null
                    : VarianceComponents.estimate(current, specs, name + "_Vc");
            final List<PCollectionView<?>> sideInputs = lambdas == null ? List.of() : List.of(lambdas);
            final PCollectionTuple outputs = current.apply(name, ParDo
                    .of(new RowStageDoFn(new StageEvaluator(prelude), lambdas, loggings, failFast, failureTag))
                    .withSideInputs(sideInputs)
                    .withOutputTags(outputTag, TupleTagList.of(failureTag)));
            failures.add(outputs.get(failureTag));
            return outputs.get(outputTag).setCoder(elementCoder);
        }

        /** Row-id merge of a wave: the base rows and every branch's partial rows, grouped by row id and reassembled. */
        PCollection<MElement> merge(final String name, final List<PCollection<MElement>> pieces, final int branches) {
            final List<PCollection<KV<String, MElement>>> keyed = new ArrayList<>();
            for (int i = 0; i < pieces.size(); i++) {
                keyed.add(pieces.get(i).apply(name + "_Key" + i, ParDo.of(new RowIdKeyDoFn())).setCoder(kvCoder));
            }
            final TupleTag<MElement> outputTag = outputTag();
            final TupleTag<BadRecord> failureTag = failureTag();
            final PCollectionTuple outputs = PCollectionList.of(keyed)
                    .apply(name + "_Flatten", Flatten.pCollections())
                    .apply(name + "_Group", GroupByKey.create())
                    .apply(name, ParDo.of(new MergeDoFn(branches, failFast, failureTag)).withOutputTags(outputTag, TupleTagList.of(failureTag)));
            failures.add(outputs.get(failureTag));
            return outputs.get(outputTag).setCoder(elementCoder);
        }
    }

    /** A merge piece the reassembly rejects, with why (turned into failure records by {@link #rejectionRecords}). */
    record Rejection(MElement piece, String message) {}

    /**
     * Reassembles the rows of a group from base rows and partial rows sharing a row id (the fan-out merge):
     * a base row takes the columns of its partials; rows without a row id (no fan-out) pass as they are.
     * Rejected, all their pieces: a row id with two base rows ({@code engine.rowId} not unique — merging its
     * partials onto an arbitrary survivor would corrupt it), a partial without a base, and a base with fewer
     * partials than {@code branches} (a branch failed the row; the linear chain would have dropped it at
     * that stage instead of emitting it with the branch's columns null).
     */
    static List<MElement> coalesce(final Iterable<MElement> pieces, final int branches, final List<Rejection> rejected) {
        final List<MElement> rows = new ArrayList<>();
        final Map<String, MElement> bases = new LinkedHashMap<>();
        final Map<String, List<MElement>> partials = new HashMap<>();
        Set<String> duplicates = null;
        for (final MElement e : pieces) {
            // probes are per-field reads: the group is large and most pieces need no full map copy
            final Object id = e.getPrimitiveValue(ROW_ID_FIELD);
            if (id == null) {
                rows.add(e);
                continue;
            }
            final String key = id.toString();
            if (e.getPrimitiveValue(PARTIAL_FIELD) != null) {
                partials.computeIfAbsent(key, k -> new ArrayList<>()).add(e);
            } else if (bases.putIfAbsent(key, e) != null) {
                if (duplicates == null) duplicates = new HashSet<>();
                duplicates.add(key);
                rejected.add(new Rejection(e, "Fan-out merge: engine.rowId is not unique (row id " + key + ")"));
            }
        }
        if (duplicates != null) {
            for (final String id : duplicates) {
                final String message = "Fan-out merge: engine.rowId is not unique (row id " + id + ")";
                rejected.add(new Rejection(bases.remove(id), message));
                final List<MElement> parts = partials.remove(id);
                if (parts != null) for (final MElement part : parts) rejected.add(new Rejection(part, message));
            }
        }
        for (final Map.Entry<String, MElement> base : bases.entrySet()) {
            final List<MElement> parts = partials.remove(base.getKey());
            final int count = parts == null ? 0 : parts.size();
            if (count != branches) {
                final String message = "Fan-out merge: " + count + " of " + branches + " branches produced row id "
                        + base.getKey() + " (a branch failed the row); dropped like the linear chain would";
                rejected.add(new Rejection(base.getValue(), message));
                if (parts != null) for (final MElement part : parts) rejected.add(new Rejection(part, message));
                continue;
            }
            final Map<String, Object> row = base.getValue().asPrimitiveMap();
            for (final MElement part : parts) {
                for (final Map.Entry<String, Object> v : part.asPrimitiveMap().entrySet()) {
                    if (!PARTIAL_FIELD.equals(v.getKey())) row.put(v.getKey(), v.getValue());
                }
            }
            rows.add(MElement.of(row, base.getValue().getTimestamp()));
        }
        for (final List<MElement> orphans : partials.values()) {
            for (final MElement orphan : orphans) {
                rejected.add(new Rejection(orphan,
                        "Fan-out merge: a partial row has no base row (row id " + orphan.getPrimitiveValue(ROW_ID_FIELD) + ")"));
            }
        }
        return rows;
    }

    /** The failure records of a merge's rejections — one construction site for all three merge-path DoFns. */
    static List<BadRecord> rejectionRecords(final List<Rejection> rejected, final boolean failFast) {
        final List<BadRecord> records = new ArrayList<>(rejected.size());
        for (final Rejection r : rejected) {
            records.add(Module.processError(r.message(), r.piece(), new IllegalStateException(r.message()), failFast));
        }
        return records;
    }

    /** Restrictions of the Beam engine that the compiler does not impose (engine doc §6, §9.2). */
    public static List<String> engineConstraints(final FeaturePlan plan, final boolean streaming) {
        final List<String> errors = new ArrayList<>();
        final boolean keyed = plan.getStages().stream().anyMatch(FeaturePlan.Stage::isReplay);
        if (streaming && keyed) {
            errors.add("sequence / population features are supported in batch only (time-sorted keyed state)");
        }
        if (streaming && plan.getColumns().stream().anyMatch(c -> "fold".equals(c.getCoordinates().get("fit")))) {
            errors.add("fit.mode fold is supported in batch only (out-of-fold statistics are fitted from the whole input); use static with an artifact for streaming");
        }
        if (streaming && plan.getColumns().stream().anyMatch(c -> "forward".equals(c.getCoordinates().get("fit")))) {
            errors.add("fit.mode forward is supported in batch only (per-block statistics are fitted from the whole input); use static with an artifact for streaming");
        }
        for (final OutputColumn c : plan.getColumns()) {
            if (c.isIntermediate()) continue;
            if (c.getStatus() == OutputColumn.Status.runtimeFilter) {
                errors.add(c.getCanonicalName() + ": per-row availability filtering (atRowCreation / event_date time) is not implemented by the Beam engine yet; declare a constant availableAt/ingestionLag");
            }
            if (c.getScope() == FeatureSpec.Scope.population && "encoding".equals(c.getOperator())
                    && !PopulationEvaluator.isSupported(c.getCoordinates().get("stat"))) {
                errors.add(c.getCanonicalName() + ": stat '" + c.getCoordinates().get("stat") + "' is not implemented yet (available: " + OperatorCatalog.AVAILABLE_STATS + ")");
            }
        }
        return errors;
    }

    // ------------------------------------------------------------------------------------------
    // fit / apply (fit.mode static)
    // ------------------------------------------------------------------------------------------

    /**
     * One fitted lattice level of a static block: hidden columns it fills ({@code n}, {@code sum}, {@code sumsq} and,
     * for an offset block on a logit / log scale, {@code sumoff} = Σ baseline — plus {@code suminfo} = Σ b(1 − b) on
     * logit), how its statistics are keyed, and the score scale it is composed on ({@code scoreScale}: logit / log for
     * an offset term, null otherwise — the λ estimate and the artifact manifest read it).
     */
    record FitLevel(String block, String id, String sumColumn, String sumSqColumn, String offSumColumn, String infoSumColumn, String scoreScale,
                    List<String> keys, String field,
                    String offsetColumn, String artifactUri, boolean refit, List<String> foldKeys, int folds,
                    Forward forward, TimeFold timeFold) implements Serializable {
        VarianceComponents.LevelSpec spec() {
            return new VarianceComponents.LevelSpec(id, keys, field, offsetColumn, foldKeys, folds, scoreScale);
        }
        /** fit.mode fold: out-of-fold statistics, always fitted in-pipeline (an artifact only holds the totals). */
        boolean isFold() {
            return foldKeys != null;
        }
        /** fit.mode forward: per-block statistics, always fitted in-pipeline (an artifact only holds the totals). */
        boolean isForward() {
            return forward != null;
        }
        /** fit.mode fold by time: the same per-block statistics, read as the totals minus a range around the row's block. */
        boolean isTimeFold() {
            return timeFold != null;
        }
        /** The per-(key, block) series of a forward or time-fold level. */
        VarianceComponents.ForwardSpec seriesSpec() {
            return isForward()
                    ? new VarianceComponents.ForwardSpec(id, keys, field, offsetColumn, forward.blocks(), forward.blockField(), forward.blockFieldType())
                    : new VarianceComponents.ForwardSpec(id, keys, field, offsetColumn, timeFold.blocks(), timeFold.blockField(), timeFold.blockFieldType());
        }
    }

    /**
     * The per-block geometry of a {@code fit.mode forward} level (from the column coordinates, see
     * FeaturePlanCompiler.forwardCoordinates): a row reads the blocks before its usable one.
     */
    record Forward(ForwardBlocks blocks, int minBlocks, long lagMillis, int windowBlocks, String blockField, String blockFieldType) implements Serializable {
        static Forward of(final OutputColumn column) {
            final Map<String, String> coordinates = column.getCoordinates();
            if (!"forward".equals(coordinates.get("fit"))) return null;
            return new Forward(ForwardBlocks.fromCoordinates(coordinates, column.getClocks()),
                    Integer.parseInt(coordinates.getOrDefault("minBlocks", "1")),
                    Long.parseLong(coordinates.getOrDefault("forwardLagMillis", "0")),
                    Integer.parseInt(coordinates.getOrDefault("windowBlocks", "0")),
                    coordinates.get("blockField"), coordinates.getOrDefault("blockFieldType", "timestamp"));
        }
    }

    /**
     * The per-block geometry of a time fold ({@code fit.mode fold} with {@code fit.fold.by: time}; from the column
     * coordinates, see FeaturePlanCompiler.timeFoldCoordinates): a row reads every block but its own, the
     * {@code purgeBlocks} on both sides of it and the {@code embargoBlocks} beyond the purge after it. It shares the
     * per-(key, block) series with forward levels, and without {@code fit.fold.until} none of their row-relative
     * geometry. Under {@code fit.fold.until} it also carries the last block of the training period
     * ({@code untilBlock}, null without) and the availability lag a later row's forward read applies
     * ({@code lagMillis}).
     */
    record TimeFold(ForwardBlocks blocks, String blockField, String blockFieldType, int purgeBlocks, int embargoBlocks,
                    Long untilBlock, long lagMillis) implements Serializable {
        static TimeFold of(final OutputColumn column) {
            final Map<String, String> coordinates = column.getCoordinates();
            if (!"fold".equals(coordinates.get("fit")) || !"time".equals(coordinates.get("foldBy"))) return null;
            final String until = coordinates.get("untilBlock");
            return new TimeFold(ForwardBlocks.fromCoordinates(coordinates, column.getClocks()),
                    coordinates.get("blockField"), coordinates.getOrDefault("blockFieldType", "timestamp"),
                    Integer.parseInt(coordinates.getOrDefault("purgeBlocks", "0")),
                    Integer.parseInt(coordinates.getOrDefault("embargoBlocks", "0")),
                    until == null ? null : Long.valueOf(until),
                    Long.parseLong(coordinates.getOrDefault("forwardLagMillis", "0")));
        }
        /** Whether a row of {@code block} lies after the training period (reads forward instead of the cross-fit). */
        boolean isEvaluation(final long block) {
            return untilBlock != null && block > untilBlock;
        }
        /** The first block a row of {@code block} leaves out: {@code block − purge}. */
        long from(final long block) {
            return block - purgeBlocks;
        }
        /** The last block a row of {@code block} leaves out: {@code block + purge + embargo}. */
        long to(final long block) {
            return block + purgeBlocks + embargoBlocks;
        }
    }

    static List<FitLevel> fitLevels(final List<OutputColumn> stageColumns) {
        final Map<String, FitLevel> levels = new LinkedHashMap<>();
        final Set<String> names = new HashSet<>();
        for (final OutputColumn c : stageColumns) names.add(c.getCanonicalName());
        for (final OutputColumn c : stageColumns) {
            final String fit = c.getCoordinates().get("fit");
            if (c.getScope() != Scope.population || !"encoding".equals(c.getOperator()) || !FeatureSpec.FitMode.isLookupToken(fit)) continue;
            final String name = c.getCanonicalName();
            final String base = name.substring(0, name.lastIndexOf("__"));
            final String id = base + "__n";
            if (levels.containsKey(id)) continue;
            final String keys = c.getCoordinates().getOrDefault("keys", "");
            final String offset = c.getCoordinates().containsKey("offset") ? "__baseline_" + c.getCoordinates().get("offset") : null;
            final String foldKeys = c.getCoordinates().get("foldKeys");
            levels.put(id, new FitLevel(c.getBlock(), id,
                    names.contains(base + "__sum") ? base + "__sum" : null,
                    names.contains(base + "__sumsq") ? base + "__sumsq" : null,
                    names.contains(base + "__" + PopulationEvaluator.SUM_OFFSET) ? base + "__" + PopulationEvaluator.SUM_OFFSET : null,
                    names.contains(base + "__" + PopulationEvaluator.SUM_INFO) ? base + "__" + PopulationEvaluator.SUM_INFO : null,
                    c.getCoordinates().get("scoreScale"),
                    keys.isEmpty() ? List.of() : List.of(keys.split(",")),
                    c.getCoordinates().get("field"), offset,
                    c.getCoordinates().get("artifactUri"), "true".equals(c.getCoordinates().get("refit")),
                    foldKeys != null ? List.of(foldKeys.split(",")) : null,
                    foldKeys != null ? Integer.parseInt(c.getCoordinates().get("folds")) : 0,
                    Forward.of(c), TimeFold.of(c)));
        }
        return new ArrayList<>(levels.values());
    }

    /** The score levels among fitted levels ({@link VarianceComponents#scoreScalesOf}): level id → scale name. */
    static Map<String, String> scoreScalesOf(final List<FitLevel> levels) {
        final Map<String, String> scales = new HashMap<>();
        for (final FitLevel level : levels) if (level.scoreScale() != null) scales.put(level.id(), level.scoreScale());
        return scales;
    }

    /**
     * Static fit: per-level sufficient statistics over the whole input (or loaded from the plan's artifact),
     * applied to every row by lookup; composition / statistics are then ordinary row columns.
     */
    private static PCollectionTuple applyFit(final PCollection<MElement> input, final List<OutputColumn> stageColumns,
                                             final StageEvaluator evaluator, final String planHash, final long predictOffsetMillis, final String label,
                                             final List<Logging> loggings, final boolean failFast,
                                             final TupleTag<MElement> outputTag, final TupleTag<BadRecord> failureTag) {
        final List<FitLevel> levels = fitLevels(stageColumns);
        final Map<String, String> loadBlocks = new LinkedHashMap<>();
        final List<FitLevel> fitted = new ArrayList<>();
        final List<FitLevel> forward = new ArrayList<>();
        final List<FitLevel> timeFolds = new ArrayList<>();
        final Map<String, String> writeBlocks = new LinkedHashMap<>();
        final Map<String, String> writeForwardBlocks = new LinkedHashMap<>();
        final Map<String, String> writeTimeFoldBlocks = new LinkedHashMap<>();
        for (final FitLevel level : levels) {
            final String uri = level.artifactUri();
            // fold / forward levels are always fitted: their per-fold / per-block parts cannot come from an artifact (which holds totals)
            final boolean exists = uri != null && !level.refit() && FitArtifact.exists(uri, planHash, level.block());
            if (exists && !level.isFold() && !level.isForward() && !level.isTimeFold()) {
                loadBlocks.put(level.block(), uri);
                continue;
            }
            (level.isForward() ? forward : level.isTimeFold() ? timeFolds : fitted).add(level);
            // fold / forward levels are re-fitted every run but respect refit: false for the (totals) artifact
            if (uri != null && !exists) (level.isForward() ? writeForwardBlocks : level.isTimeFold() ? writeTimeFoldBlocks : writeBlocks).put(level.block(), uri);
        }
        for (final Map.Entry<String, String> e : loadBlocks.entrySet()) {
            LOG.info("feature fit: block {} loads artifact {}", e.getKey(), FitArtifact.statsPath(e.getValue(), planHash, e.getKey()));
        }
        if ((!fitted.isEmpty() || !forward.isEmpty() || !timeFolds.isEmpty()) && com.mercari.solution.util.pipeline.OptionUtil.isStreaming(input)) {
            throw new IllegalStateException("fit.mode static in streaming requires an existing artifact for plan " + planHash
                    + " (fit the statistics with a batch run first; fold / forward are batch only)");
        }
        // a static fit is over the WHOLE input whatever the module's windowing strategy: the statistics
        // are computed in the global window (single pane), which also lets the global-window artifact
        // writer trigger and the windowed main input both map onto the side inputs
        final boolean globalInput = input.getWindowingStrategy().getWindowFn() instanceof org.apache.beam.sdk.transforms.windowing.GlobalWindows;
        final PCollection<MElement> fitInput = globalInput ? input : input.apply(label + "_FitGlobal", org.apache.beam.sdk.transforms.windowing.Window.<MElement>into(new org.apache.beam.sdk.transforms.windowing.GlobalWindows())
                .triggering(org.apache.beam.sdk.transforms.windowing.DefaultTrigger.of())
                .withAllowedLateness(org.joda.time.Duration.ZERO)
                .discardingFiredPanes());

        PCollectionView<Map<String, VarianceComponents.KeyStats>> statsView = null;
        PCollectionView<Map<String, Double>> lambdasView = null;
        final List<PCollectionView<?>> sideInputs = new ArrayList<>();
        // a lattice column with variance-components weights reads λ from the stage's estimate; a joint column
        // estimates its pseudo-counts inside the fit's solve and never reads them here
        final boolean needsLambdas = evaluator.row.needsVarianceComponents();
        if (!fitted.isEmpty()) {
            final List<VarianceComponents.LevelSpec> specs = new ArrayList<>();
            for (final FitLevel level : fitted) specs.add(level.spec());
            final PCollection<KV<String, VarianceComponents.KeyStats>> perKey = VarianceComponents.perKeyStats(fitInput, specs, label + "_Fit");
            statsView = perKey.apply(label + "_StatsView", View.asMap());
            sideInputs.add(statsView);
            if (needsLambdas) {
                lambdasView = VarianceComponents.lambdasFromKeyStats(perKey, scoreScalesOf(fitted), label + "_Vc");
                sideInputs.add(lambdasView);
            }
            if (!writeBlocks.isEmpty()) writeArtifacts(perKey, writeBlocks, fitted, planHash, null, label + "_WriteStatic");
        }
        // fit.mode forward: cumulative per-block statistics per (level, key) — a parallel Combine, no time-ordered replay.
        // The series travel as a list side input read once per DoFn instance (see FitApplyDoFn), λ per (level, block)
        // and the artifacts' totals are derived from the series PCollection: nothing scans or probes a map side input
        // entry by entry, which is one state fetch per entry on a portable runner (Dataflow Runner v2, prism)
        // A time fold (fold.by: time) reads the same series, but its λ is the whole input's, as for a hash fold: the
        // moments of its levels' totals, not the per-block step function a forward level reads
        PCollectionView<List<KV<String, ForwardBlocks.Series>>> seriesView = null;
        PCollectionView<List<VarianceComponents.LevelLambdas>> forwardLambdasView = null;
        PCollectionView<Map<String, Double>> timeFoldLambdasView = null;
        if (!forward.isEmpty() || !timeFolds.isEmpty()) {
            final List<VarianceComponents.ForwardSpec> specs = new ArrayList<>();
            for (final FitLevel level : forward) specs.add(level.seriesSpec());
            for (final FitLevel level : timeFolds) specs.add(level.seriesSpec());
            final PCollection<KV<String, ForwardBlocks.Series>> allSeries = VarianceComponents.forwardSeries(fitInput, specs, label + "_Forward");
            seriesView = allSeries.apply(label + "_ForwardView", View.asList());
            sideInputs.add(seriesView);
            if (!timeFolds.isEmpty() && (needsLambdas || !writeTimeFoldBlocks.isEmpty())) {
                // fit.fold.until: λ over the training period only (the artifact keeps the whole-input totals), so the
                // whole-input pass runs only when an artifact needs it or no level clips its series
                final Map<String, Long> untilBlocks = untilBlocks(timeFolds);
                final PCollection<KV<String, VarianceComponents.KeyStats>> totals =
                        !writeTimeFoldBlocks.isEmpty() || untilBlocks.isEmpty()
                                ? seriesTotals(allSeries, levelIds(timeFolds), label + "_TimeFoldTotals") : null;
                if (needsLambdas) {
                    final PCollection<KV<String, VarianceComponents.KeyStats>> training = untilBlocks.isEmpty() ? totals
                            : seriesTotals(allSeries, levelIds(timeFolds), untilBlocks, label + "_TimeFoldTrainingTotals");
                    timeFoldLambdasView = VarianceComponents.lambdasFromKeyStats(training, scoreScalesOf(timeFolds), label + "_TimeFoldVc");
                    sideInputs.add(timeFoldLambdasView);
                }
                if (!writeTimeFoldBlocks.isEmpty()) writeArtifacts(totals, writeTimeFoldBlocks, timeFolds, planHash, null, label + "_WriteTimeFold");
            }
            if (!forward.isEmpty()) {
                if (needsLambdas || !writeForwardBlocks.isEmpty()) {
                    // only the forward levels enter the per-block λ Combine (built only when something reads it)
                    final PCollection<KV<String, ForwardBlocks.Series>> series = timeFolds.isEmpty()
                            ? allSeries : seriesOf(allSeries, levelIds(forward), label + "_ForwardOnly");
                    forwardLambdasView = VarianceComponents.lambdasByBlockView(series, scoreScalesOf(forward), label + "_ForwardVc");
                }
                if (needsLambdas) sideInputs.add(forwardLambdasView);
                if (!writeForwardBlocks.isEmpty()) {
                    writeArtifacts(seriesTotals(allSeries, levelIds(forward), label + "_ForwardTotals"), writeForwardBlocks, forward, planHash,
                            forwardLambdasView, label + "_WriteForward");
                }
            }
        }
        // static-fit blocks (see staticFitBlocks): fitted on one worker over the whole input, or loaded
        // from their artifact, and applied per row through a side input
        final List<StaticFitBlock<?>> blocks = staticFitBlocks(stageColumns);
        final Map<String, PCollectionView<?>> blockViews = new LinkedHashMap<>();
        final Set<String> blockLoad = new LinkedHashSet<>();
        // blocks whose fit state is a Summary family share one Combine per family and one side input (fitSummaryBlocks)
        final List<SummaryFitBlock<?, ?, ?>> summaryBlocks = new ArrayList<>();
        for (final StaticFitBlock<?> block : blocks) {
            if (block.artifactUri() != null && !block.refit() && block.artifactExists(planHash)) {
                blockLoad.add(block.block());
                LOG.info("feature fit: block {} loads artifact {}", block.block(), block.artifactPath(planHash));
                continue;
            }
            if (com.mercari.solution.util.pipeline.OptionUtil.isStreaming(input)) {
                throw new IllegalStateException("fit.mode static block '" + block.block() + "' in streaming requires an existing artifact for plan " + planHash
                        + " (fit it with a batch run first)");
            }
            if (block instanceof SummaryFitBlock<?, ?, ?> summaryBlock) {
                summaryBlocks.add(summaryBlock);
                continue;
            }
            final PCollectionView<?> view = block.fit(fitInput, label, planHash);
            blockViews.put(block.block(), view);
            sideInputs.add(view);
        }
        PCollectionView<List<KV<String, Serializable>>> summaryModelsView = null;
        final Set<String> summaryFitted = new LinkedHashSet<>();
        if (!summaryBlocks.isEmpty()) {
            summaryModelsView = fitSummaryBlocks(fitInput, summaryBlocks, label, planHash);
            sideInputs.add(summaryModelsView);
            for (final SummaryFitBlock<?, ?, ?> block : summaryBlocks) summaryFitted.add(block.block());
        }

        // fitted statistics are extracted from the stage INPUT: a target / offset / input produced by a column
        // of this same stage would read null for every row, so reject the fusion explicitly
        final Set<String> stageProduced = new HashSet<>();
        for (final OutputColumn c : stageColumns) stageProduced.add(c.getCanonicalName());
        final List<String> sameStageDeps = new ArrayList<>();
        for (final FitLevel level : levels) {
            if (level.field() != null && stageProduced.contains(level.field())) sameStageDeps.add(level.field());
            if (level.offsetColumn() != null && stageProduced.contains(level.offsetColumn())) sameStageDeps.add(level.offsetColumn());
        }
        for (final StaticFitBlock<?> block : blocks) {
            for (final String f : block.fitInputs()) if (stageProduced.contains(f)) sameStageDeps.add(f);
        }
        if (!sameStageDeps.isEmpty()) {
            throw new IllegalStateException("fit.mode static targets/offsets/inputs " + sameStageDeps
                    + " are computed in the same fit stage and would read null; split them into a separate feature step");
        }

        return input.apply(label, ParDo
                .of(new FitApplyDoFn(evaluator, levels, statsView, lambdasView, seriesView, needsLambdas ? forwardLambdasView : null, timeFoldLambdasView,
                        predictOffsetMillis, loadBlocks, planHash,
                        blocks, blockViews, blockLoad, summaryModelsView, summaryFitted, loggings, failFast, failureTag))
                .withSideInputs(sideInputs)
                .withOutputTags(outputTag, TupleTagList.of(failureTag)));
    }

    private static Set<String> levelIds(final List<FitLevel> levels) {
        final Set<String> ids = new HashSet<>();
        for (final FitLevel level : levels) ids.add(level.id());
        return ids;
    }

    /** The series entries of the given levels. */
    private static PCollection<KV<String, ForwardBlocks.Series>> seriesOf(final PCollection<KV<String, ForwardBlocks.Series>> series,
                                                                         final Set<String> levelIds, final String label) {
        return series
                .apply(label, ParDo.of(new DoFn<KV<String, ForwardBlocks.Series>, KV<String, ForwardBlocks.Series>>() {
                    @ProcessElement
                    public void processElement(final ProcessContext c) {
                        if (levelIds.contains(FitArtifact.levelOf(c.element().getKey()))) c.output(c.element());
                    }
                }))
                .setCoder(series.getCoder());
    }

    /** The last training block of every time-fold level under {@code fit.fold.until} (level id → block); empty without. */
    private static Map<String, Long> untilBlocks(final List<FitLevel> timeFolds) {
        final Map<String, Long> until = new HashMap<>();
        for (final FitLevel level : timeFolds) if (level.timeFold().untilBlock() != null) until.put(level.id(), level.timeFold().untilBlock());
        return until;
    }

    /** The totals of the given levels' series: what an artifact holds, and what a whole-input λ is estimated from. */
    private static PCollection<KV<String, VarianceComponents.KeyStats>> seriesTotals(final PCollection<KV<String, ForwardBlocks.Series>> series,
                                                                                    final Set<String> levelIds, final String label) {
        return seriesTotals(series, levelIds, Map.of(), label);
    }

    /** @param untilBlocks per level, the last block the totals cover (a training period, that block included); a level absent reads its whole series */
    static PCollection<KV<String, VarianceComponents.KeyStats>> seriesTotals(final PCollection<KV<String, ForwardBlocks.Series>> series,
                                                                                    final Set<String> levelIds, final Map<String, Long> untilBlocks, final String label) {
        final Map<String, Long> until = new HashMap<>(untilBlocks); // a serializable copy for the DoFn
        return series
                .apply(label, ParDo.of(new DoFn<KV<String, ForwardBlocks.Series>, KV<String, VarianceComponents.KeyStats>>() {
                    @ProcessElement
                    public void processElement(final ProcessContext c) {
                        final String level = FitArtifact.levelOf(c.element().getKey());
                        if (!levelIds.contains(level)) return;
                        final ForwardBlocks.Series s = c.element().getValue();
                        final Long last = until.get(level);
                        final VarianceComponents.KeyStats t = last == null ? s.totals() : s.statsBetween(-1, s.floor(last));
                        if (t != null) c.output(KV.of(c.element().getKey(), t));
                    }
                }))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(VarianceComponents.KeyStats.class)));
    }

    /**
     * A population block fitted once on the whole input and applied by lookup ({@code fit.mode: static}
     * outside the encoding lattice): its model {@code M} is either fitted in the fit stage (one side input
     * per block) or loaded from its artifact on the worker. Implementations rebuild themselves from their
     * output columns' coordinates and are enumerated by {@link #staticFitBlocks}.
     */
    interface StaticFitBlock<M extends Serializable> extends Serializable {
        String block();
        String artifactUri();
        boolean refit();
        String artifactPath(String planHash);
        boolean artifactExists(String planHash);
        M readArtifact(String planHash);
        /** Fields the fit reads from the stage input (target / offset / input); they must come from an earlier stage. */
        List<String> fitInputs();
        PCollectionView<List<M>> fit(PCollection<MElement> fitInput, String label, String planHash);
        /** Fills the block's output columns of one row; {@code model} is null when nothing could be fitted. */
        void apply(M model, Map<String, Object> values);
    }

    /**
     * A static-fit block whose fit state is a {@link Summary} family (svd: the vector moments, quantileTransform:
     * the gathered values): what a row contributes and how the model is solved from the per-time-block states is all
     * the block declares, so every such block of a fit stage shares ONE extraction pass and ONE
     * {@code Combine.perKey} per family — keyed by (block, time block) — instead of a chain of its own
     * ({@link #fitSummaryBlocks}). A fit stage with a dozen blocks is then a handful of steps, not dozens.
     *
     * @param <T> a row's contribution to the family
     * @param <S> the family's state
     * @param <M> the solved model
     */
    interface SummaryFitBlock<T, S extends Serializable, M extends Serializable> extends StaticFitBlock<M> {
        Summary<S> family();
        /** Names the family in transform names ({@code <label>_Fit<FamilyName>_Combine}); blocks of one name share a Combine. */
        String familyName();
        Class<S> stateClass();
        Coder<T> contributionCoder();
        /** The row's contribution and the time block it belongs to (0 under a static fit), or null when the row has none. */
        KV<Long, T> contribution(Map<String, Object> row);
        /** Solves the model from the per-time-block states (and writes the artifact); {@code parts} is empty for an input without contributions. */
        M solve(Map<Long, S> parts, String planHash);
        /** Whether an input without a single contribution still yields a model (and its artifact) rather than none. */
        boolean fitsEmptyInput();

        /**
         * The block prepared for the extraction pass, once per fit stage: a block whose state is only bounded once
         * something is known about the input — the co-occurrence vocabulary of {@link SpectralSpec}, whose pair
         * counts are quadratic in the values counted — derives it from {@code fitInput} here and returns a copy
         * carrying the side input. Every other block returns itself.
         */
        default SummaryFitBlock<T, S, M> prepare(final PCollection<MElement> fitInput, final String prefix) {
            return this;
        }

        /** The side inputs {@link #contribution(Map, List)} reads, declared by the ParDo of the extraction pass. */
        default List<PCollectionView<?>> extractionViews() {
            return List.of();
        }

        /**
         * The row's contribution with this block's {@link #extractionViews} resolved, in order. An empty list is what
         * a block prepared with no side input receives — and what {@link #contribution(Map)} means: unrestricted.
         */
        default KV<Long, T> contribution(final Map<String, Object> row, final List<Object> views) {
            return contribution(row);
        }

        /**
         * The block on its own, as a list view like any other static-fit block (the {@link StaticFitBlock} contract).
         * A fit stage does not call this: it fits its summary blocks together ({@link #fitSummaryBlocks}).
         */
        @Override
        @SuppressWarnings({"unchecked", "rawtypes"})
        default PCollectionView<List<M>> fit(final PCollection<MElement> fitInput, final String label, final String planHash) {
            final String prefix = label + "_Fit" + familyName() + "_" + block();
            final PCollection<KV<String, Serializable>> models = fitFamily(fitInput, (List) List.of(this), prefix, planHash);
            return (PCollectionView) models
                    .apply(prefix + "_Model", org.apache.beam.sdk.transforms.Values.create())
                    .setCoder(SerializableCoder.of(Serializable.class))
                    .apply(prefix + "_View", View.asList());
        }
    }

    /**
     * The model of a block fitted over time blocks: the whole-input fit ({@code total}, what the artifact holds) and,
     * under {@code fit.mode: forward}, one fit per {@link BlockSeries#changePoints change point} plus the observed
     * blocks (a row reads the floor entry of its usable block, {@link BlockSeries#lookup}).
     */
    record ForwardModel<M extends Serializable>(M total, TreeMap<Long, M> byBlock, TreeSet<Long> observed) implements Serializable {}

    /**
     * A {@link SummaryFitBlock} whose model is solved from the state and persisted as its own JSON artifact — every
     * population type outside the encoding lattice (svd, quantileTransform, smooth, spectralEmbedding). They differ
     * only in what a row contributes, how a state becomes a model and which columns it fills; the fit geometry is the
     * same for all of them and lives here: the whole-input fit plus one per change point under {@code forward}, the
     * artifact written once, and the lookup a row does ({@link #modelFor}).
     *
     * @param <T> a row's contribution
     * @param <S> the summary state
     * @param <M> the solved model
     */
    interface ForwardFitBlock<T, S extends Serializable, M extends Serializable & FitArtifact.Model> extends SummaryFitBlock<T, S, ForwardModel<M>> {
        /** How this model class is named, stored and reported ({@link FitArtifact.Json}). */
        FitArtifact.Json<M> artifact();
        /** The forward geometry, or null for a static fit. */
        Forward forward();
        long predictOffsetMillis();

        /**
         * The model of one state. {@code loud} is the whole-input fit — the one that reports what it fitted; a
         * per-change-point fit is quiet, because an empty or short window at a leave point is normal.
         */
        M fit(S state, boolean loud);

        /** {@code fit.minRows}: a state fewer rows contributed to is not fitted — its rows read null (0 = no floor). */
        long minRows();

        /** The rows that contributed to a state (what {@link #minRows} counts). */
        long rowsOf(S state);

        /** Whether the {@code fit.minRows} floor empties this state: the one predicate {@link #fitAbove} and the run log share. */
        default boolean belowFloor(final S state) {
            return minRows() > 0 && rowsOf(state) < minRows();
        }

        /**
         * {@link #fit} behind the {@code fit.minRows} floor: a state below it gets the family's "nothing fitted" model
         * (the fit of an empty state), whose rows read null. The first blocks of a forward fit are where it bites — the
         * input starts mid-block, so the first window may hold a handful of rows.
         */
        default M fitAbove(final S state, final boolean loud) {
            if (belowFloor(state)) {
                if (loud) {
                    LOG.warn("{} {}: {} row(s) are fewer than fit.minRows {}; nothing is fitted, every row reads null",
                            artifact().name(), block(), rowsOf(state), minRows());
                }
                return fit(family().create(), false);
            }
            return fit(state, loud);
        }

        /**
         * {@code current} in the coordinates of {@code previous}, the (already aligned) fit of the change point before
         * it: the {@code fit.align} of a type whose solution has a gauge — the signs and rotations an eigendecomposition
         * leaves open ({@link Alignment}; svd, spectralEmbedding). The other types have none and return the fit as is.
         */
        default M alignTo(final M previous, final M current) {
            return current;
        }

        /** How a model was aligned ({@code fit.align}), null for one that stands alone — and always for a type without a gauge. */
        default String alignmentOf(final M model) {
            return null;
        }

        /**
         * The columns of an aligned model that the fit before it HAD a predecessor for and that still found none: the
         * two fits shared too little to anchor them ({@link Alignment.Map}). A component the previous fit did not have
         * (a vocabulary still smaller than the rank) is not counted — that is a fit growing, not a chain breaking.
         */
        default int unanchoredOf(final M previous, final M aligned) {
            return 0;
        }

        /** The loaded model as this block will apply it (quantileTransform takes the config's clip, not the artifact's). */
        default M adopt(final M model) {
            return model;
        }

        @Override
        default String artifactPath(final String planHash) {
            return artifact().path(artifactUri(), planHash, block());
        }

        /** A forward fit is always re-fitted: the artifact holds the whole-input model only, for a static serving run. */
        @Override
        default boolean artifactExists(final String planHash) {
            return forward() == null && artifact().exists(artifactUri(), planHash, block());
        }

        @Override
        default ForwardModel<M> readArtifact(final String planHash) {
            return new ForwardModel<>(adopt(artifact().read(artifactUri(), planHash, block())), null, null);
        }

        /**
         * Solves the per-time-block states on one worker: the whole-input model (a static fit is a single block, whose
         * state is fitted as it stands — a fit reads the state and keeps nothing) and, under forward, one model per
         * change point. The artifact is written once even under forward, which re-fits every run — but never for a
         * whole-input fit the {@code fit.minRows} floor emptied: an artifact is read back instead of fitting
         * ({@link #artifactExists}), so persisting the empty model would make "not enough rows yet" permanent.
         *
         * <p>The change points are solved independently and then chained by {@link #alignTo} in time order, each
         * into the coordinates of the last fitted one before it — never the other way: a fit aligned to a later one
         * would carry a trace of rows it may not read. The whole-input model, which a static serving run loads in
         * place of the forward fits a training run read, is aligned last, to the end of that chain, so that serving
         * continues the columns the consumer's model was trained on — and when an artifact from an earlier run is
         * kept instead of rewritten, the run warns that its coordinates are not this chain's.
         */
        @Override
        default ForwardModel<M> solve(final Map<Long, S> parts, final String planHash) {
            final BlockSeries<S> series = new BlockSeries<>(family(), parts);
            final S all = parts.size() == 1 ? parts.values().iterator().next() : series.total();
            final S whole = all == null ? family().create() : all;
            final boolean floored = belowFloor(whole);
            M total = fitAbove(whole, true);
            TreeMap<Long, M> byBlock = null;
            boolean chained = false;
            if (forward() != null) {
                final int[] emptied = {0};
                byBlock = series.models(forward().windowBlocks(), state -> {
                    // an empty window at a leave point is normal and not counted: only a window that held rows
                    if (rowsOf(state) > 0 && belowFloor(state)) emptied[0]++;
                    return fitAbove(state, false);
                });
                M previous = null;
                int loose = 0;
                for (final Map.Entry<Long, M> point : byBlock.entrySet()) {
                    if (point.getValue().isEmpty()) continue;
                    if (previous != null) {
                        point.setValue(alignTo(previous, point.getValue()));
                        if (unanchoredOf(previous, point.getValue()) > 0) loose++;
                    }
                    previous = point.getValue();
                }
                if (loose > 0) {
                    // columns that continue nothing are what fit.align exists to prevent: the run says how often it happened
                    LOG.warn("{} {}: {} change point(s) shared too little with the fit before them to anchor every column (fit.align): the columns without a"
                                    + " predecessor there are that fit's own leading components and do not continue the earlier blocks - a longer fit.window,"
                                    + " larger blocks or a smaller rank give consecutive fits more in common",
                            artifact().name(), block(), loose);
                }
                if (previous != null && !total.isEmpty()) {
                    // alignTo returns the fit itself when it aligns nothing, so identity says whether the chain moved it
                    final M end = alignTo(previous, total);
                    chained = end != total;
                    total = end;
                }
                LOG.info("{} {}: forward fit over {} block(s), {} change point(s){}", artifact().name(), block(), parts.size(), byBlock.size(),
                        emptied[0] == 0 ? "" : ", " + emptied[0] + " of them with fewer than fit.minRows " + minRows() + " row(s): not fitted, their rows read null");
            }
            if (artifactUri() != null && !floored && (refit() || !artifact().exists(artifactUri(), planHash, block()))) {
                artifact().write(artifactUri(), planHash, block(), total);
            } else if (artifactUri() != null && floored) {
                LOG.warn("{} {}: the whole-input fit is below fit.minRows {}, so no artifact is written under {}; a run with enough rows writes it",
                        artifact().name(), block(), minRows(), artifactUri());
            } else if (artifactUri() != null && chained) {
                // an artifact is kept as it is. One this chain wrote earlier is a point of the same chain (the fits of
                // the earlier blocks do not depend on what follows them), so a re-run has nothing to report. One written
                // without this alignment — before fit.align existed — holds coordinates this run's rows never saw, and a
                // serving run would load it: say so, once per run
                final String kept = alignmentOf(artifact().read(artifactUri(), planHash, block()));
                if (!java.util.Objects.equals(kept, alignmentOf(total))) {
                    LOG.warn("{} {}: the artifact under {} was written {} and is kept, while this run's forward fits were aligned to one another (fit.align {}):"
                                    + " a static serving run would load coordinates the training rows never saw; set fit.artifact.refit once to rewrite it",
                            artifact().name(), block(), artifactUri(), kept == null ? "without an alignment" : "under fit.align " + kept, alignmentOf(total));
                }
            }
            return new ForwardModel<>(total, byBlock, forward() == null ? null : series.observed());
        }

        /** The model a row reads: the whole-input fit, or under forward the fit over the blocks its usable block may read. */
        default M modelFor(final ForwardModel<M> model, final Map<String, Object> values) {
            if (model == null) return null;
            if (forward() == null) return model.total();
            final Long eventMillis = FeatureValues.toEpochMillis(values.get(forward().blockField()), forward().blockFieldType());
            if (eventMillis == null) return null;
            final long usable = forward().blocks().usableBlock(eventMillis, predictOffsetMillis(), forward().lagMillis());
            return BlockSeries.lookup(model.byBlock(), model.observed(), usable, forward().minBlocks());
        }

        /** The row's time block: 0 under a static fit, the block of its event time under forward (null = no contribution). */
        default Long timeBlock(final Map<String, Object> row) {
            if (forward() == null) return 0L;
            final Long millis = FeatureValues.toEpochMillis(row.get(forward().blockField()), forward().blockFieldType());
            return millis == null ? null : forward().blocks().indexOf(millis);
        }
    }

    /** The time-block key of the marker that keeps a block's group alive on an input without contributions. */
    private static final long EMPTY_MARKER = Long.MIN_VALUE;

    /**
     * Fits every {@link SummaryFitBlock} of a stage: per family one extraction pass over the rows, one
     * {@code Combine.perKey} over (block, time block), a regrouping by block and one solve per block (in parallel
     * across blocks); the models of all families reach the apply DoFn as ONE list side input of (block, model).
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    static PCollectionView<List<KV<String, Serializable>>> fitSummaryBlocks(final PCollection<MElement> fitInput, final List<SummaryFitBlock<?, ?, ?>> blocks,
                                                                            final String label, final String planHash) {
        final Map<String, List<SummaryFitBlock<?, ?, ?>>> byFamily = new LinkedHashMap<>();
        for (final SummaryFitBlock<?, ?, ?> block : blocks) byFamily.computeIfAbsent(block.familyName(), f -> new ArrayList<>()).add(block);
        PCollectionList<KV<String, Serializable>> models = PCollectionList.empty(fitInput.getPipeline());
        for (final Map.Entry<String, List<SummaryFitBlock<?, ?, ?>>> family : byFamily.entrySet()) {
            models = models.and(fitFamily(fitInput, (List) family.getValue(), label + "_Fit" + family.getKey(), planHash));
        }
        return models
                .apply(label + "_FitModels", Flatten.pCollections())
                .apply(label + "_FitModelsView", View.asList());
    }

    private static <T, S extends Serializable> PCollection<KV<String, Serializable>> fitFamily(final PCollection<MElement> fitInput, final List<SummaryFitBlock<T, S, ?>> blocks,
                                                                                                final String prefix, final String planHash) {
        final SummaryFitBlock<T, S, ?> first = blocks.get(0);
        // a block that needs something of the input before it may accumulate (a vocabulary cap) derives it here; the
        // solve keeps the plain blocks, so a side input travels only to the ParDo that declares it
        final List<SummaryFitBlock<T, S, ?>> extracting = new ArrayList<>();
        final List<PCollectionView<?>> views = new ArrayList<>();
        for (final SummaryFitBlock<T, S, ?> block : blocks) {
            final SummaryFitBlock<T, S, ?> prepared = block.prepare(fitInput, prefix + "_" + block.block());
            extracting.add(prepared);
            views.addAll(prepared.extractionViews());
        }
        final Coder<KV<String, KV<Long, S>>> partCoder = KvCoder.of(StringUtf8Coder.of(), KvCoder.of(org.apache.beam.sdk.coders.VarLongCoder.of(), SerializableCoder.of(first.stateClass())));
        PCollection<KV<String, KV<Long, S>>> parts = fitInput
                .apply(prefix + "_Extract", ParDo.of(new ExtractContributionsDoFn<>(extracting)).withSideInputs(views))
                .setCoder(KvCoder.of(KvCoder.of(StringUtf8Coder.of(), org.apache.beam.sdk.coders.VarLongCoder.of()), first.contributionCoder()))
                .apply(prefix + "_Combine", Combine.perKey(new SummaryFn<>(first.family(), first.stateClass())))
                .apply(prefix + "_ByBlock", ParDo.of(new ByBlockDoFn<S>()))
                .setCoder(partCoder);
        // a block that fits an empty input too gets an empty marker part, so its group (and model, and artifact) exists
        final List<KV<String, KV<Long, S>>> markers = new ArrayList<>();
        for (final SummaryFitBlock<T, S, ?> block : blocks) {
            if (block.fitsEmptyInput()) markers.add(KV.of(block.block(), KV.of(EMPTY_MARKER, first.family().create())));
        }
        if (!markers.isEmpty()) {
            final PCollection<KV<String, KV<Long, S>>> marked = fitInput.getPipeline().apply(prefix + "_Markers", Create.of(markers).withCoder(partCoder));
            parts = PCollectionList.of(parts).and(marked).apply(prefix + "_WithMarkers", Flatten.pCollections());
        }
        return parts
                .apply(prefix + "_Group", GroupByKey.create())
                .apply(prefix + "_Solve", ParDo.of(new SolveSummaryBlocksDoFn<>(blocks, planHash)))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(Serializable.class)));
    }

    /** One pass over the rows for every block of a family: ((block, time block), contribution) per row and block. */
    static class ExtractContributionsDoFn<T, S extends Serializable> extends DoFn<MElement, KV<KV<String, Long>, T>> {
        private final List<SummaryFitBlock<T, S, ?>> blocks;
        /** Each block's {@link SummaryFitBlock#extractionViews}, resolved once instead of per row. */
        private final List<List<PCollectionView<?>>> declared;

        ExtractContributionsDoFn(final List<SummaryFitBlock<T, S, ?>> blocks) {
            this.blocks = blocks;
            this.declared = new ArrayList<>(blocks.size());
            for (final SummaryFitBlock<T, S, ?> block : blocks) declared.add(block.extractionViews());
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final MElement element = c.element();
            if (element == null) return;
            final Map<String, Object> row = element.asPrimitiveMap();
            for (int b = 0; b < blocks.size(); b++) {
                final List<PCollectionView<?>> views = declared.get(b);
                List<Object> resolved = List.of();
                if (!views.isEmpty()) {
                    resolved = new ArrayList<>(views.size());
                    for (final PCollectionView<?> view : views) resolved.add(c.sideInput(view));
                }
                final SummaryFitBlock<T, S, ?> block = blocks.get(b);
                final KV<Long, T> contribution = block.contribution(row, resolved);
                if (contribution != null) c.output(KV.of(KV.of(block.block(), contribution.getKey()), contribution.getValue()));
            }
        }
    }

    /** ((block, time block), state) → (block, (time block, state)): the regrouping key of the solve. */
    static class ByBlockDoFn<S extends Serializable> extends DoFn<KV<KV<String, Long>, S>, KV<String, KV<Long, S>>> {
        @ProcessElement
        public void processElement(final ProcessContext c) {
            c.output(KV.of(c.element().getKey().getKey(), KV.of(c.element().getKey().getValue(), c.element().getValue())));
        }
    }

    /** Solves one block per group from its per-time-block states (taken as is; the inputs are never mutated). */
    static class SolveSummaryBlocksDoFn<T, S extends Serializable> extends DoFn<KV<String, Iterable<KV<Long, S>>>, KV<String, Serializable>> {
        private final List<SummaryFitBlock<T, S, ?>> blocks;
        private final String planHash;

        SolveSummaryBlocksDoFn(final List<SummaryFitBlock<T, S, ?>> blocks, final String planHash) {
            this.blocks = blocks;
            this.planHash = planHash;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final String name = c.element().getKey();
            final SummaryFitBlock<T, S, ?> block = blocks.stream().filter(b -> b.block().equals(name)).findFirst()
                    .orElseThrow(() -> new IllegalStateException("no summary-fit block named " + name));
            final Map<Long, S> parts = new HashMap<>();
            // Combine.perKey yields one part per time block, taken as is (a quantileTransform state is the whole column:
            // no copy); a repeated time block is merged into a fresh state rather than mutating the input element.
            // Nothing in a solve mutates a part (BlockSeries merges into fresh states).
            for (final KV<Long, S> part : c.element().getValue()) {
                if (part.getKey() == EMPTY_MARKER) continue;
                parts.merge(part.getKey(), part.getValue(), (a, b) -> {
                    final S merged = block.family().create();
                    block.family().merge(merged, a);
                    block.family().merge(merged, b);
                    return merged;
                });
            }
            final Serializable model = block.solve(parts, planHash);
            if (model != null) c.output(KV.of(name, model));
        }
    }

    /** Gathers a block's training examples into one list (the fit runs in memory on one worker). */
    static class GatherFn<T extends Serializable> extends Combine.CombineFn<T, ArrayList<T>, ArrayList<T>> {
        @Override
        public ArrayList<T> createAccumulator() { return new ArrayList<>(); }

        @Override
        public ArrayList<T> addInput(final ArrayList<T> acc, final T e) {
            acc.add(e);
            return acc;
        }

        @Override
        public ArrayList<T> mergeAccumulators(final Iterable<ArrayList<T>> accs) {
            final ArrayList<T> out = new ArrayList<>();
            for (final ArrayList<T> a : accs) out.addAll(a);
            return out;
        }

        @Override
        public ArrayList<T> extractOutput(final ArrayList<T> acc) { return acc; }

        @SuppressWarnings({"unchecked", "rawtypes"})
        private static <T> Coder<ArrayList<T>> listCoder() {
            return (Coder) org.apache.beam.sdk.coders.SerializableCoder.of(ArrayList.class);
        }

        @Override
        public Coder<ArrayList<T>> getAccumulatorCoder(final org.apache.beam.sdk.coders.CoderRegistry registry, final Coder<T> inputCoder) {
            return listCoder();
        }

        @Override
        public Coder<ArrayList<T>> getDefaultOutputCoder(final org.apache.beam.sdk.coders.CoderRegistry registry, final Coder<T> inputCoder) {
            return listCoder();
        }
    }

    // --- factorization ---------------------------------------------------------------------------

    /** One factorization block of a fit stage, rebuilt from its output columns' coordinates. */
    record FmSpec(String block, List<String> fields, boolean fieldWeighted, int k, String target, String offsetColumn,
                  int epochs, double reg, long seed, String artifactUri, boolean refit,
                  List<OutputColumn> columns) implements StaticFitBlock<Factorization.Model> {
        Factorization.Options options() {
            return new Factorization.Options(fields, fieldWeighted, k, epochs, reg, seed);
        }

        @Override
        public String artifactPath(final String planHash) {
            return Factorization.artifactPath(artifactUri, planHash, block);
        }

        @Override
        public boolean artifactExists(final String planHash) {
            return Factorization.exists(artifactUri, planHash, block);
        }

        @Override
        public Factorization.Model readArtifact(final String planHash) {
            return Factorization.read(artifactUri, planHash, block, fields, fieldWeighted, k);
        }

        @Override
        public List<String> fitInputs() {
            return offsetColumn == null ? List.of(target) : List.of(target, offsetColumn);
        }

        /** Gathers the training set on one worker and fits by ALS. */
        @Override
        public PCollectionView<List<Factorization.Model>> fit(final PCollection<MElement> fitInput, final String label, final String planHash) {
            return fitInput
                    .apply(label + "_Fm_" + block + "_Examples", ParDo.of(new ExtractExamplesDoFn(this)))
                    .setCoder(org.apache.beam.sdk.coders.SerializableCoder.of(Factorization.Example.class))
                    .apply(label + "_Fm_" + block + "_Gather", Combine.globally(new GatherFn<Factorization.Example>()).withoutDefaults())
                    .apply(label + "_Fm_" + block + "_Fit", ParDo.of(new FitFmDoFn(this, planHash)))
                    .setCoder(org.apache.beam.sdk.coders.SerializableCoder.of(Factorization.Model.class))
                    .apply(label + "_Fm_" + block + "_View", View.asList());
        }

        @Override
        public void apply(final Factorization.Model model, final Map<String, Object> values) {
            final String[] x = fieldValues(values, fields);
            for (final OutputColumn col : columns) {
                Object v = null;
                if (model != null) {
                    switch (col.getCoordinates().get("kind")) {
                        case "pair" -> {
                            final String[] pair = col.getCoordinates().get("pair").split(",");
                            v = model.pair(x, fields.indexOf(pair[0]), fields.indexOf(pair[1]));
                        }
                        case "embedding" -> {
                            final int f = fields.indexOf(col.getCoordinates().get("field"));
                            final int d = Integer.parseInt(col.getCoordinates().get("dim"));
                            final double[] e = model.embedding(x, f, d + 1);
                            v = e == null || e.length <= d ? null : e[d];
                        }
                        default -> v = model.predict(x);
                    }
                }
                values.put(col.getCanonicalName(), v);
            }
        }
    }

    static List<FmSpec> fmSpecs(final List<OutputColumn> stageColumns) {
        final Map<String, List<OutputColumn>> columns = new LinkedHashMap<>();
        for (final OutputColumn c : stageColumns) {
            if ("fm".equals(c.getOperator())) columns.computeIfAbsent(c.getBlock(), b -> new ArrayList<>()).add(c);
        }
        final List<FmSpec> specs = new ArrayList<>();
        for (final Map.Entry<String, List<OutputColumn>> e : columns.entrySet()) {
            final Map<String, String> k = e.getValue().get(0).getCoordinates();
            specs.add(new FmSpec(e.getKey(), List.of(k.get("fields").split(",")), "fwfm".equals(k.get("variant")),
                    Integer.parseInt(k.get("latentDim")), k.get("target"), k.get("offset"),
                    Integer.parseInt(k.get("epochs")), Double.parseDouble(k.get("reg")), Long.parseLong(k.get("seed")),
                    k.get("artifactUri"), "true".equals(k.get("refit")), e.getValue()));
        }
        return specs;
    }

    static String[] fieldValues(final Map<String, Object> row, final List<String> fields) {
        final String[] values = new String[fields.size()];
        for (int i = 0; i < fields.size(); i++) values[i] = FeatureValues.toText(row.get(fields.get(i)));
        return values;
    }

    static class ExtractExamplesDoFn extends DoFn<MElement, Factorization.Example> {
        private final FmSpec spec;

        ExtractExamplesDoFn(final FmSpec spec) {
            this.spec = spec;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final MElement element = c.element();
            if (element == null) return;
            final Map<String, Object> row = element.asPrimitiveMap();
            Double y = FeatureValues.toDouble(row.get(spec.target()));
            if (y == null) return;
            if (spec.offsetColumn() != null) {
                final Double b = FeatureValues.toDouble(row.get(spec.offsetColumn()));
                if (b == null) return;
                y -= b;
            }
            c.output(new Factorization.Example(fieldValues(row, spec.fields()), y));
        }
    }

    static class FitFmDoFn extends DoFn<ArrayList<Factorization.Example>, Factorization.Model> {
        private final FmSpec spec;
        private final String planHash;

        FitFmDoFn(final FmSpec spec, final String planHash) {
            this.spec = spec;
            this.planHash = planHash;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final ArrayList<Factorization.Example> examples = c.element();
            LOG.info("factorization {}: fitting on {} examples", spec.block(), examples.size());
            final Factorization.Model model = Factorization.fit(spec.options(), examples);
            if (spec.artifactUri() != null) Factorization.write(spec.artifactUri(), planHash, spec.block(), model);
            c.output(model);
        }
    }

    // --- discretize --------------------------------------------------------------------------------

    /** One discretize block of a fit stage, rebuilt from its output column's coordinates. */
    record DiscretizeSpec(String block, String column, String field, Integer bins, Integer minSamplesPerBin,
                          String artifactUri, boolean refit) implements StaticFitBlock<Discretization> {
        @Override
        public String artifactPath(final String planHash) {
            return Discretization.artifactPath(artifactUri, planHash, block);
        }

        @Override
        public boolean artifactExists(final String planHash) {
            return Discretization.exists(artifactUri, planHash, block);
        }

        @Override
        public Discretization readArtifact(final String planHash) {
            return Discretization.read(artifactUri, planHash, block);
        }

        @Override
        public List<String> fitInputs() {
            return List.of(field);
        }

        /**
         * Gathers the non-null input values on one worker and fits the quantile edges. The combine keeps its
         * default (an empty buffer) so an input without any value still produces a fit — n = 0, every value
         * to bin 1 — and writes the artifact instead of silently leaving the column null.
         */
        @Override
        public PCollectionView<List<Discretization>> fit(final PCollection<MElement> fitInput, final String label, final String planHash) {
            return fitInput
                    .apply(label + "_Bins_" + block + "_Values", ParDo.of(new ExtractValuesDoFn(field)))
                    .setCoder(org.apache.beam.sdk.coders.DoubleCoder.of())
                    .apply(label + "_Bins_" + block + "_Gather", Combine.globally(new GatherDoublesFn()))
                    .apply(label + "_Bins_" + block + "_Fit", ParDo.of(new FitDiscretizeDoFn(this, planHash)))
                    .setCoder(org.apache.beam.sdk.coders.SerializableCoder.of(Discretization.class))
                    .apply(label + "_Bins_" + block + "_View", View.asList());
        }

        @Override
        public void apply(final Discretization d, final Map<String, Object> values) {
            values.put(column, d == null ? null : d.bin(FeatureValues.toDouble(values.get(field))));
        }
    }

    static List<DiscretizeSpec> discretizeSpecs(final List<OutputColumn> stageColumns) {
        final List<DiscretizeSpec> specs = new ArrayList<>();
        for (final OutputColumn c : stageColumns) {
            if (!"discretize".equals(c.getOperator())) continue;
            final Map<String, String> k = c.getCoordinates();
            specs.add(new DiscretizeSpec(c.getBlock(), c.getCanonicalName(), k.get("field"),
                    k.containsKey("bins") ? Integer.parseInt(k.get("bins")) : null,
                    k.containsKey("minSamplesPerBin") ? Integer.parseInt(k.get("minSamplesPerBin")) : null,
                    k.get("artifactUri"), "true".equals(k.get("refit"))));
        }
        return specs;
    }

    // --- quantileTransform ---------------------------------------------------------------------------

    /**
     * One quantileTransform block of a fit stage, rebuilt from its column's coordinates. The fit state is the values
     * themselves ({@link QuantileTransform#VALUES}, exact), gathered per time block — one block for a static fit.
     */
    record QuantileTransformSpec(String block, String column, String field, int bins, String distribution, double clip,
                                 String artifactUri, boolean refit, Forward forward, long predictOffsetMillis, long minRows) implements ForwardFitBlock<Double, QuantileTransform.Values, QuantileTransform> {
        @Override
        public FitArtifact.Json<QuantileTransform> artifact() {
            return QuantileTransform.ARTIFACT;
        }

        /** The clamp is an apply-time parameter: a loaded artifact takes the clip of the config that applies it. */
        @Override
        public QuantileTransform adopt(final QuantileTransform q) {
            if (q.clip != clip) {
                LOG.info("quantileTransform {}: the artifact was written with clip {}; applying the config's clip {}", block, q.clip, clip);
            }
            return q.withClip(clip);
        }

        @Override
        public List<String> fitInputs() {
            return List.of(field);
        }

        @Override
        public Summary<QuantileTransform.Values> family() {
            return QuantileTransform.VALUES;
        }

        @Override
        public String familyName() {
            return "Values";
        }

        @Override
        public Class<QuantileTransform.Values> stateClass() {
            return QuantileTransform.Values.class;
        }

        @Override
        public Coder<Double> contributionCoder() {
            return DoubleCoder.of();
        }

        /** The row's non-null value. */
        @Override
        public KV<Long, Double> contribution(final Map<String, Object> row) {
            final Double v = FeatureValues.toDouble(row.get(field));
            if (v == null || v.isNaN()) return null;
            final Long timeBlock = timeBlock(row);
            return timeBlock == null ? null : KV.of(timeBlock, v);
        }

        /** An input without a single value still fits (n = 0: every value reads null) and writes its artifact. */
        @Override
        public boolean fitsEmptyInput() {
            return true;
        }

        @Override
        public long rowsOf(final QuantileTransform.Values values) {
            return values.size();
        }

        /** The values of every time block meet here (8 bytes per row, as the static fit always did). */
        @Override
        public QuantileTransform fit(final QuantileTransform.Values values, final boolean loud) {
            if (loud) LOG.info("quantileTransform {}: fitting {} quantile intervals on {} values", block, bins, values.size());
            return QuantileTransform.fit(values, bins, distribution, clip, loud);
        }

        @Override
        public void apply(final ForwardModel<QuantileTransform> model, final Map<String, Object> values) {
            final QuantileTransform q = modelFor(model, values);
            values.put(column, q == null ? null : q.transform(FeatureValues.toDouble(values.get(field))));
        }
    }

    static List<QuantileTransformSpec> quantileTransformSpecs(final List<OutputColumn> stageColumns) {
        final List<QuantileTransformSpec> specs = new ArrayList<>();
        for (final OutputColumn c : stageColumns) {
            if (!"quantileTransform".equals(c.getOperator())) continue;
            final Map<String, String> k = c.getCoordinates();
            final Forward forward = Forward.of(c);
            specs.add(new QuantileTransformSpec(c.getBlock(), c.getCanonicalName(), k.get("field"),
                    Integer.parseInt(k.getOrDefault("bins", Integer.toString(QuantileTransform.DEFAULT_BINS))),
                    k.getOrDefault("distribution", QuantileTransform.UNIFORM),
                    Double.parseDouble(k.getOrDefault("clip", Double.toString(QuantileTransform.DEFAULT_CLIP))),
                    k.get("artifactUri"), "true".equals(k.get("refit")),
                    forward, Long.parseLong(k.getOrDefault("predictOffsetMillis", "0")), Long.parseLong(k.getOrDefault("minRows", "0"))));
        }
        return specs;
    }

    // --- svd ------------------------------------------------------------------------------------------

    /**
     * One svd block of a fit stage (all its score columns), rebuilt from the columns' coordinates: the vector is
     * the listed numeric {@code fields} or one array-typed {@code arrayField}.
     */
    record SvdSpec(String block, List<String> fields, String arrayField, int rank, boolean center, boolean standardize,
                   String artifactUri, boolean refit, List<OutputColumn> columns, int[] components,
                   Forward forward, long predictOffsetMillis, long minRows, String align) implements ForwardFitBlock<double[], Svd.Moments, Svd> {
        @Override
        public FitArtifact.Json<Svd> artifact() {
            return Svd.ARTIFACT;
        }

        @Override
        public long rowsOf(final Svd.Moments m) {
            return m.n;
        }

        @Override
        public Svd alignTo(final Svd previous, final Svd current) {
            return current.alignTo(previous, align);
        }

        @Override
        public String alignmentOf(final Svd model) {
            return model.alignment;
        }

        @Override
        public int unanchoredOf(final Svd previous, final Svd aligned) {
            if (align == null || Alignment.NONE.equals(align)) return 0;
            // a fit the alignment left alone shared nothing at all with the one before it
            return Math.min(aligned.rank(), previous.rank()) - (aligned.alignment == null ? 0 : aligned.anchored);
        }

        /**
         * Solves the sufficient statistics (n, Σx, Σxxᵀ) — no vector leaves the workers. A per-change-point fit is
         * quiet: an empty / short window at a leave point is normal under forward.
         */
        @Override
        public Svd fit(final Svd.Moments m, final boolean loud) {
            final Svd fitted = Svd.fit(m, rank, center, standardize, loud);
            if (loud) {
                LOG.info("svd {}: fitted {} of {} requested component(s) from {} vectors of dimension {} ({} missing skipped, {} of another length)",
                        block, fitted.rank(), rank, m.n, m.dimension, m.skipped, m.mismatched);
            }
            return fitted;
        }

        @Override
        public List<String> fitInputs() {
            return arrayField != null ? List.of(arrayField) : fields;
        }

        /** The row's vector, or null when a component is missing (null / NaN) or the array is absent. */
        double[] vector(final Map<String, Object> values) {
            if (arrayField != null) return VectorOps.toVector(values.get(arrayField));
            final double[] x = new double[fields.size()];
            for (int i = 0; i < x.length; i++) {
                final Double d = FeatureValues.toDouble(values.get(fields.get(i)));
                if (d == null) return null;
                x[i] = d;
            }
            return x;
        }

        @Override
        public Summary<Svd.Moments> family() {
            return Svd.SUMMARY;
        }

        @Override
        public String familyName() {
            return "Moments";
        }

        @Override
        public Class<Svd.Moments> stateClass() {
            return Svd.Moments.class;
        }

        @Override
        public Coder<double[]> contributionCoder() {
            return SerializableCoder.of(double[].class);
        }

        /** The row's vector. */
        @Override
        public KV<Long, double[]> contribution(final Map<String, Object> row) {
            final double[] x = vector(row);
            if (x == null) return null;
            final Long timeBlock = timeBlock(row);
            return timeBlock == null ? null : KV.of(timeBlock, x);
        }

        /** An input without a single vector has no components: no model (every score reads null) and no artifact. */
        @Override
        public boolean fitsEmptyInput() {
            return false;
        }

        /** The {@code components} code of the residual-norm column. */
        static final int RESIDUAL_NORM = Integer.MIN_VALUE;

        /**
         * What a column carries, from its coordinates: score {@code k} as {@code k ≥ 0}, the residual of input
         * dimension {@code i} as {@code −i − 1}, the residual norm as {@link #RESIDUAL_NORM}.
         */
        static int output(final Map<String, String> coordinates) {
            final String residual = coordinates.get("residual");
            if (residual == null) return Integer.parseInt(coordinates.get("component"));
            return "norm".equals(residual) ? RESIDUAL_NORM : -Integer.parseInt(residual) - 1;
        }

        /** {@code columns.get(i)} carries output {@code components[i]} (see {@link #output}; resolved once in {@link #svdSpecs}, never parsed per row). */
        @Override
        public void apply(final ForwardModel<Svd> model, final Map<String, Object> values) {
            final Svd svd = modelFor(model, values);
            final double[] x = svd == null ? null : vector(values);
            final double[] scores = svd == null ? null : svd.transform(x);
            double[] residual = null;
            for (int i = 0; i < components.length; i++) {
                final int k = components[i];
                final Object value;
                if (k >= 0) {
                    value = scores == null || k >= scores.length ? null : scores[k];
                } else {
                    // a residual column: input dimension −k − 1, or the norm over every dimension
                    if (residual == null && scores != null) residual = svd.residual(x);
                    value = residual == null ? null : k == RESIDUAL_NORM ? (Object) com.mercari.solution.util.domain.math.MatrixOps.norm(residual) : (Object) residual[-k - 1];
                }
                values.put(columns.get(i).getCanonicalName(), value);
            }
        }
    }

    /** A {@link Summary} family as a Beam {@code CombineFn}: the per-block (or whole-input) state of a fit. */
    static class SummaryFn<T, S extends Serializable> extends Combine.CombineFn<T, S, S> {
        private final Summary<S> family;
        private final Class<S> stateClass;

        SummaryFn(final Summary<S> family, final Class<S> stateClass) {
            this.family = family;
            this.stateClass = stateClass;
        }

        @Override
        public S createAccumulator() { return family.create(); }

        @Override
        public S addInput(final S acc, final T contribution) {
            family.update(acc, contribution, 1);
            return acc;
        }

        @Override
        public S mergeAccumulators(final Iterable<S> accs) {
            final S out = family.create();
            for (final S a : accs) family.merge(out, a);
            return out;
        }

        @Override
        public S extractOutput(final S acc) { return acc; }

        @Override
        public Coder<S> getAccumulatorCoder(final org.apache.beam.sdk.coders.CoderRegistry registry, final Coder<T> inputCoder) {
            return org.apache.beam.sdk.coders.SerializableCoder.of(stateClass);
        }

        @Override
        public Coder<S> getDefaultOutputCoder(final org.apache.beam.sdk.coders.CoderRegistry registry, final Coder<T> inputCoder) {
            return org.apache.beam.sdk.coders.SerializableCoder.of(stateClass);
        }
    }

    static List<SvdSpec> svdSpecs(final List<OutputColumn> stageColumns) {
        final Map<String, List<OutputColumn>> columns = new LinkedHashMap<>();
        for (final OutputColumn c : stageColumns) {
            if ("svd".equals(c.getOperator())) columns.computeIfAbsent(c.getBlock(), b -> new ArrayList<>()).add(c);
        }
        final List<SvdSpec> specs = new ArrayList<>();
        for (final Map.Entry<String, List<OutputColumn>> e : columns.entrySet()) {
            final Map<String, String> k = e.getValue().get(0).getCoordinates();
            final Forward forward = Forward.of(e.getValue().get(0));
            final int[] components = new int[e.getValue().size()];
            for (int i = 0; i < components.length; i++) components[i] = SvdSpec.output(e.getValue().get(i).getCoordinates());
            specs.add(new SvdSpec(e.getKey(), k.containsKey("fields") ? List.of(k.get("fields").split(",")) : List.of(), k.get("arrayField"),
                    Integer.parseInt(k.get("rank")), Boolean.parseBoolean(k.getOrDefault("center", "true")),
                    Boolean.parseBoolean(k.getOrDefault("standardize", "false")), k.get("artifactUri"), "true".equals(k.get("refit")), e.getValue(), components,
                    forward, Long.parseLong(k.getOrDefault("predictOffsetMillis", "0")), Long.parseLong(k.getOrDefault("minRows", "0")), k.get("align")));
        }
        return specs;
    }

    // --- smooth ---------------------------------------------------------------------------------------

    /**
     * One smooth block of a fit stage (its curve and residual columns), rebuilt from the columns' coordinates. A row
     * contributes {@code [B(x), y]} to the vector moments — the family of the svd blocks, so the stage's one
     * {@code _FitMoments_Combine} serves both — and the penalised system is solved from them on one worker.
     */
    record SmoothSpec(String block, String field, String target, Smooth.Basis basis, int penaltyOrder, Double lambda,
                      String artifactUri, boolean refit, List<OutputColumn> columns, boolean[] residual,
                      Forward forward, long predictOffsetMillis, long minRows) implements ForwardFitBlock<double[], Svd.Moments, Smooth> {
        @Override
        public FitArtifact.Json<Smooth> artifact() {
            return Smooth.ARTIFACT;
        }

        @Override
        public List<String> fitInputs() {
            return List.of(field, target);
        }

        @Override
        public Summary<Svd.Moments> family() {
            return Svd.SUMMARY;
        }

        @Override
        public String familyName() {
            return "Moments";
        }

        @Override
        public Class<Svd.Moments> stateClass() {
            return Svd.Moments.class;
        }

        @Override
        public Coder<double[]> contributionCoder() {
            return SerializableCoder.of(double[].class);
        }

        /** The row's basis values and target. */
        @Override
        public KV<Long, double[]> contribution(final Map<String, Object> row) {
            final double[] z = Smooth.contribution(basis, FeatureValues.toDouble(row.get(field)), FeatureValues.toDouble(row.get(target)));
            if (z == null) return null;
            final Long timeBlock = timeBlock(row);
            return timeBlock == null ? null : KV.of(timeBlock, z);
        }

        /** An input without a single (key, target) pair has no curve: no model (every column reads null) and no artifact. */
        @Override
        public boolean fitsEmptyInput() {
            return false;
        }

        @Override
        public long rowsOf(final Svd.Moments m) {
            return m.n;
        }

        /** Solves the penalised system from the moments of a time block's rows. */
        @Override
        public Smooth fit(final Svd.Moments m, final boolean loud) {
            final Smooth fitted = Smooth.fit(m, basis, penaltyOrder, lambda, loud);
            if (loud) {
                LOG.info("smooth {}: fitted {} coefficients on {} rows (lambda = {}{}, edf = {})", block, fitted.coefficients.length, fitted.n,
                        fitted.lambda, !fitted.estimated ? "" : fitted.limit == null ? " by REML" : " = the end of the REML search, the " + fitted.limit + " limit", fitted.edf);
            }
            return fitted;
        }

        /** {@code columns.get(i)} is the residual when {@code residual[i]}, else the curve (resolved once in {@link #smoothSpecs}). */
        @Override
        public void apply(final ForwardModel<Smooth> model, final Map<String, Object> values) {
            final Smooth smooth = modelFor(model, values);
            final Double curve = smooth == null ? null : smooth.curve(FeatureValues.toDouble(values.get(field)));
            for (int i = 0; i < residual.length; i++) {
                Double value = curve;
                if (residual[i] && curve != null) {
                    final Double y = FeatureValues.toDouble(values.get(target));
                    value = y == null || y.isNaN() ? null : y - curve;
                }
                values.put(columns.get(i).getCanonicalName(), value);
            }
        }
    }

    static List<SmoothSpec> smoothSpecs(final List<OutputColumn> stageColumns) {
        final Map<String, List<OutputColumn>> columns = new LinkedHashMap<>();
        for (final OutputColumn c : stageColumns) {
            if ("smooth".equals(c.getOperator())) columns.computeIfAbsent(c.getBlock(), b -> new ArrayList<>()).add(c);
        }
        final List<SmoothSpec> specs = new ArrayList<>();
        for (final Map.Entry<String, List<OutputColumn>> e : columns.entrySet()) {
            final Map<String, String> k = e.getValue().get(0).getCoordinates();
            final boolean[] residual = new boolean[e.getValue().size()];
            for (int i = 0; i < residual.length; i++) residual[i] = "residual".equals(e.getValue().get(i).getCoordinates().get("output"));
            final Smooth.Basis basis = new Smooth.Basis(Double.parseDouble(k.get("lo")), Double.parseDouble(k.get("hi")),
                    Integer.parseInt(k.get("segments")), Integer.parseInt(k.get("degree")));
            specs.add(new SmoothSpec(e.getKey(), k.get("field"), k.get("target"), basis, Integer.parseInt(k.get("penaltyOrder")),
                    Smooth.REML.equals(k.get("lambda")) ? null : Double.valueOf(k.get("lambda")),
                    k.get("artifactUri"), "true".equals(k.get("refit")), e.getValue(), residual,
                    Forward.of(e.getValue().get(0)), Long.parseLong(k.getOrDefault("predictOffsetMillis", "0")), Long.parseLong(k.getOrDefault("minRows", "0"))));
        }
        return specs;
    }

    // --- spectralEmbedding ----------------------------------------------------------------------------

    /**
     * One spectralEmbedding block of a fit stage (all its coordinate columns), rebuilt from the columns' coordinates:
     * a row contributes its value of {@code field} with the values of the lag {@code path} columns — the entity's
     * previous steps, computed by an earlier keyed stage — and reads, per column, the coordinates of the value in
     * {@code applied.get(i)}: the row's own value, its previous one, or ({@code of: [current, previous]}) both.
     *
     * <p>{@code vocabulary} is the {@code maxValues} cap as a side input ({@link #vocabularyView}, null until
     * {@link #prepare} builds it): unlike every other summary block the state here is quadratic in what it counts,
     * so the values are chosen before the pairs are accumulated rather than when the eigenproblem is solved.
     */
    record SpectralSpec(String block, String field, List<String> path, List<String> applied, int rank, int maxValues,
                        String artifactUri, boolean refit, List<OutputColumn> columns, int[] components,
                        Forward forward, long predictOffsetMillis, long minRows, String align,
                        PCollectionView<Map<String, Long>> vocabulary) implements ForwardFitBlock<String[], Spectral.PairCounts, Spectral> {
        @Override
        public FitArtifact.Json<Spectral> artifact() {
            return Spectral.ARTIFACT;
        }

        @Override
        public List<String> fitInputs() {
            final List<String> inputs = new ArrayList<>(path);
            inputs.add(field);
            return inputs;
        }

        @Override
        public Summary<Spectral.PairCounts> family() {
            return Spectral.SUMMARY;
        }

        @Override
        public String familyName() {
            return "PairCounts";
        }

        @Override
        public Class<Spectral.PairCounts> stateClass() {
            return Spectral.PairCounts.class;
        }

        @Override
        public Coder<String[]> contributionCoder() {
            return SerializableCoder.of(String[].class);
        }

        /** The string form a value is counted and looked up under (an integral number without its fraction, like {@code values:}). */
        static String valueOf(final Object value) {
            return value == null ? null : ContextEvaluator.valueKey(value);
        }

        /** The row's value followed by the previous values in the window; null when the row has no value or no previous one. */
        @Override
        public KV<Long, String[]> contribution(final Map<String, Object> row) {
            final String value = valueOf(row.get(field));
            if (value == null) return null;
            final List<String> values = new ArrayList<>(path.size() + 1);
            values.add(value);
            for (final String step : path) {
                final String previous = valueOf(row.get(step));
                if (previous != null) values.add(previous);
            }
            if (values.size() == 1) return null;
            final Long timeBlock = timeBlock(row);
            return timeBlock == null ? null : KV.of(timeBlock, values.toArray(new String[0]));
        }

        /** The block with its vocabulary side input: one extra pass over the fit input per spectralEmbedding block. */
        @Override
        public SpectralSpec prepare(final PCollection<MElement> fitInput, final String prefix) {
            return new SpectralSpec(block, field, path, applied, rank, maxValues, artifactUri, refit, columns, components,
                    forward, predictOffsetMillis, minRows, align, vocabularyView(fitInput, this, prefix));
        }

        @Override
        public List<PCollectionView<?>> extractionViews() {
            return vocabulary == null ? List.of() : List.of(vocabulary);
        }

        /**
         * The row's pairs, restricted to the vocabulary: a pair with an endpoint outside it is exactly a cell
         * {@link Spectral#fit} would have skipped, so the fitted embedding is the one an unfiltered state would
         * have given — with a state bounded by the cap instead of by the field's cardinality.
         */
        @Override
        public KV<Long, String[]> contribution(final Map<String, Object> row, final List<Object> views) {
            final KV<Long, String[]> pairs = contribution(row);
            if (pairs == null || views.isEmpty()) return pairs;
            @SuppressWarnings("unchecked") final Map<String, Long> kept = (Map<String, Long>) views.get(0);
            final String[] values = pairs.getValue();
            if (!kept.containsKey(values[0])) return null;
            final List<String> within = new ArrayList<>(values.length);
            within.add(values[0]);
            for (int i = 1; i < values.length; i++) if (kept.containsKey(values[i])) within.add(values[i]);
            return within.size() == 1 ? null : KV.of(pairs.getKey(), within.toArray(new String[0]));
        }

        /** An input without a single pair has no embedding: no model (every column reads null) and no artifact. */
        @Override
        public boolean fitsEmptyInput() {
            return false;
        }

        /** The rows that contributed at least one pair (a row adds one pair per previous value in its window). */
        @Override
        public long rowsOf(final Spectral.PairCounts counts) {
            return counts.rows;
        }

        @Override
        public Spectral alignTo(final Spectral previous, final Spectral current) {
            return current.alignTo(previous, align);
        }

        @Override
        public String alignmentOf(final Spectral model) {
            return model.alignment;
        }

        @Override
        public int unanchoredOf(final Spectral previous, final Spectral aligned) {
            if (align == null || Alignment.NONE.equals(align)) return 0;
            // a fit the alignment left alone shared nothing at all with the one before it
            return Math.min(aligned.rank(), previous.rank()) - (aligned.alignment == null ? 0 : aligned.anchored);
        }

        /** Factorises the pair counts of a time block's rows on one worker. */
        @Override
        public Spectral fit(final Spectral.PairCounts counts, final boolean loud) {
            final Spectral fitted = Spectral.fit(counts, rank, maxValues, loud);
            if (loud) {
                LOG.info("spectralEmbedding {}: {} value(s) embedded in {} of {} requested coordinate(s) from {} pairs ({} counted value(s) left out: no co-occurrence row)",
                        block, fitted.vocabulary.length, fitted.rank(), rank, fitted.pairs, fitted.dropped);
            }
            return fitted;
        }

        /**
         * {@code columns.get(i)} carries coordinate {@code components[i]} of the value in the field {@code applied.get(i)}
         * (resolved once in {@link #spectralSpecs}): the row's own value, its previous one, or — {@code of: [current,
         * previous]} — a run of columns for each, read from the one model. A value's row is looked up once per run and its
         * coordinates are read one by one: the model's arrays never leave it (one instance serves every thread of a
         * worker) and no vector is copied per row.
         */
        @Override
        public void apply(final ForwardModel<Spectral> model, final Map<String, Object> values) {
            final Spectral spectral = modelFor(model, values);
            String looked = null;
            Integer row = null;
            for (int i = 0; i < components.length; i++) {
                if (!applied.get(i).equals(looked)) {
                    looked = applied.get(i);
                    row = spectral == null ? null : spectral.indexOf(valueOf(values.get(looked)));
                }
                values.put(columns.get(i).getCanonicalName(), row == null ? null : spectral.coordinate(row, components[i]));
            }
        }
    }

    static List<SpectralSpec> spectralSpecs(final List<OutputColumn> stageColumns) {
        final Map<String, List<OutputColumn>> columns = new LinkedHashMap<>();
        for (final OutputColumn c : stageColumns) {
            if ("spectralEmbedding".equals(c.getOperator())) columns.computeIfAbsent(c.getBlock(), b -> new ArrayList<>()).add(c);
        }
        final List<SpectralSpec> specs = new ArrayList<>();
        for (final Map.Entry<String, List<OutputColumn>> e : columns.entrySet()) {
            final Map<String, String> k = e.getValue().get(0).getCoordinates();
            final int[] components = new int[e.getValue().size()];
            final List<String> applied = new ArrayList<>(components.length);
            for (int i = 0; i < components.length; i++) {
                components[i] = Integer.parseInt(e.getValue().get(i).getCoordinates().get("component"));
                applied.add(e.getValue().get(i).getCoordinates().get("applied"));
            }
            specs.add(new SpectralSpec(e.getKey(), k.get("field"), List.of(k.get("path").split(",")), applied,
                    Integer.parseInt(k.get("rank")), Integer.parseInt(k.get("maxValues")),
                    k.get("artifactUri"), "true".equals(k.get("refit")), e.getValue(), components,
                    Forward.of(e.getValue().get(0)), Long.parseLong(k.getOrDefault("predictOffsetMillis", "0")), Long.parseLong(k.getOrDefault("minRows", "0")), k.get("align"), null));
        }
        return specs;
    }

    /**
     * The values a spectralEmbedding block may count: the {@code maxValues} of greatest co-occurrence mass, chosen
     * before a single pair is accumulated. A value's mass is the number of pairs it takes part in — exactly what
     * {@link Spectral#fit} ranks by, ties in string order — so the vocabulary is the one the fit would have picked,
     * but the {@code Combine} state is bounded by the cap ({@code maxValues²} cells) instead of by the field's
     * cardinality: an unfiltered state grows one cell per co-occurring pair of distinct values and dies in the
     * accumulator long before the cap is ever consulted. The cost is one extra pass over the fit input per block.
     *
     * <p>Under {@code fit.mode: forward} the vocabulary is global while the counts stay per block: a value is
     * admitted by the mass of the whole input, and a block that has not seen it simply has no cell for it (its
     * coordinates read null there, as before).
     */
    static PCollectionView<Map<String, Long>> vocabularyView(final PCollection<MElement> fitInput, final SpectralSpec spec, final String prefix) {
        return fitInput
                .apply(prefix + "_Mass", ParDo.of(new PairMassDoFn(spec)))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), org.apache.beam.sdk.coders.VarLongCoder.of()))
                .apply(prefix + "_MassSum", org.apache.beam.sdk.transforms.Sum.longsPerKey())
                .apply(prefix + "_MassTop", org.apache.beam.sdk.transforms.Top.of(spec.maxValues(), new ByMass()))
                .apply(prefix + "_Vocabulary", ParDo.of(new VocabularyDoFn(spec.block(), spec.maxValues())))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), org.apache.beam.sdk.coders.VarLongCoder.of()))
                .apply(prefix + "_VocabularyView", View.asMap());
    }

    /** The pairs a row takes part in, by value: its own value pairs with each earlier value in the window, each of those with it. */
    static class PairMassDoFn extends DoFn<MElement, KV<String, Long>> {
        private final SpectralSpec spec;

        PairMassDoFn(final SpectralSpec spec) {
            this.spec = spec;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final MElement element = c.element();
            if (element == null) return;
            final KV<Long, String[]> pairs = spec.contribution(element.asPrimitiveMap());
            if (pairs == null) return;
            final String[] values = pairs.getValue();
            c.output(KV.of(values[0], (long) values.length - 1));
            for (int i = 1; i < values.length; i++) c.output(KV.of(values[i], 1L));
        }
    }

    /** Greatest mass first, ties in string order — {@link org.apache.beam.sdk.transforms.Top} keeps the greatest. */
    static final class ByMass implements Comparator<KV<String, Long>>, Serializable {
        @Override
        public int compare(final KV<String, Long> a, final KV<String, Long> b) {
            final int byMass = Long.compare(a.getValue(), b.getValue());
            return byMass != 0 ? byMass : b.getKey().compareTo(a.getKey());
        }
    }

    /** The capped vocabulary as the entries of a map side input (and what it left out of the counts). */
    static class VocabularyDoFn extends DoFn<List<KV<String, Long>>, KV<String, Long>> {
        private final String block;
        private final int maxValues;

        VocabularyDoFn(final String block, final int maxValues) {
            this.block = block;
            this.maxValues = maxValues;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final List<KV<String, Long>> vocabulary = c.element();
            if (vocabulary == null) return;
            if (vocabulary.size() < maxValues) {
                LOG.info("spectralEmbedding {}: {} value(s) co-occur, all within maxValues {}; every pair is counted", block, vocabulary.size(), maxValues);
            } else {
                // Top kept exactly maxValues: whether anything was left out is not visible here (the distinct
                // values were never counted), so the line says what is counted, not how much it dropped
                LOG.warn("spectralEmbedding {}: the co-occurrence vocabulary is capped at maxValues {}, from {} pair(s) down;"
                        + " any value of lesser mass is not counted and reads null", block, vocabulary.size(), vocabulary.get(vocabulary.size() - 1).getValue());
            }
            for (final KV<String, Long> value : vocabulary) c.output(value);
        }
    }

    // --- joint (estimator: joint) -----------------------------------------------------------------

    /**
     * One joint-estimator model of a fit stage — a keySet × window × target of an encoding block — rebuilt
     * from its {@code joint} columns' coordinates: the lattice levels, the target (minus offset), the scale
     * and pseudo-count rule, and the fold / forward variant it is fitted under. The cells (the cross of every
     * level's key fields) are aggregated per key in parallel and solved on one worker ({@link JointFit}).
     */
    record JointSpec(String id, List<JointFit.Level> levels, String field, String offsetColumn, Shrinkage.Scale scale,
                     String weights, double priorWeight, List<String> foldKeys, int folds, Forward forward, long predictOffsetMillis,
                     String artifactUri, boolean refit, List<OutputColumn> columns) implements StaticFitBlock<JointFit> {
        @Override
        public String block() {
            return id;
        }

        @Override
        public String artifactPath(final String planHash) {
            return JointFit.artifactPath(artifactUri, planHash, id);
        }

        /** fold / forward solutions are always re-fitted: the artifact holds the whole-input solution only. */
        @Override
        public boolean artifactExists(final String planHash) {
            return foldKeys == null && forward == null && JointFit.exists(artifactUri, planHash, id);
        }

        @Override
        public JointFit readArtifact(final String planHash) {
            return JointFit.read(artifactUri, planHash, id, levels, scale, offsetColumn != null);
        }

        @Override
        public List<String> fitInputs() {
            final List<String> inputs = new ArrayList<>(JointFit.cellKeysOf(levels));
            inputs.add(field);
            if (offsetColumn != null) inputs.add(offsetColumn);
            return inputs;
        }

        /** Per-cell sufficient statistics in parallel, gathered on one worker for the solve. */
        @Override
        public PCollectionView<List<JointFit>> fit(final PCollection<MElement> fitInput, final String label, final String planHash) {
            final String prefix = label + "_Joint_" + id;
            return fitInput
                    .apply(prefix + "_Cells", ParDo.of(new ExtractCellsDoFn(this)))
                    .setCoder(KvCoder.of(StringUtf8Coder.of(), VarianceComponents.valueCoder()))
                    .apply(prefix + "_PerCell", Combine.perKey(new VarianceComponents.KeyStatsFn()))
                    .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(VarianceComponents.KeyStats.class)))
                    .apply(prefix + "_Entries", ParDo.of(new DoFn<KV<String, VarianceComponents.KeyStats>, JointFit.Cell>() {
                        @ProcessElement
                        public void processElement(final ProcessContext c) {
                            final VarianceComponents.KeyStats s = c.element().getValue();
                            c.output(new JointFit.Cell(c.element().getKey(), s.n, s.sum, s.sumSq, s.sumOff, s.sumInfo));
                        }
                    }))
                    .setCoder(org.apache.beam.sdk.coders.SerializableCoder.of(JointFit.Cell.class))
                    .apply(prefix + "_Gather", Combine.globally(new GatherFn<JointFit.Cell>()).withoutDefaults())
                    .apply(prefix + "_Fit", ParDo.of(new FitJointDoFn(this, planHash)))
                    .setCoder(org.apache.beam.sdk.coders.SerializableCoder.of(JointFit.class))
                    .apply(prefix + "_View", View.asList());
        }

        @Override
        public void apply(final JointFit model, final Map<String, Object> values) {
            JointFit.Solution solution = null;
            if (model != null) {
                Integer fold = null;
                Long usable = null;
                if (foldKeys != null) {
                    final String unit = FeatureValues.key(values, foldKeys);
                    fold = unit == null ? null : VarianceComponents.foldOf(unit, folds);
                }
                if (forward != null) {
                    final Long eventMillis = FeatureValues.toEpochMillis(values.get(forward.blockField()), forward.blockFieldType());
                    usable = eventMillis == null ? null : forward.blocks().usableBlock(eventMillis, predictOffsetMillis, forward.lagMillis());
                }
                solution = model.solutionFor(fold, usable, forward == null ? 1 : forward.minBlocks());
            }
            for (final OutputColumn col : columns) {
                Object v = null;
                if (solution != null) {
                    v = switch (col.getCoordinates().get("kind")) {
                        case "deviation" -> model.effect(solution, Integer.parseInt(col.getCoordinates().get("level")), values);
                        case "effectiveN" -> model.effectiveN(solution, values);
                        default -> model.estimate(solution, values);
                    };
                }
                values.put(col.getCanonicalName(), v);
            }
        }
    }

    static List<JointSpec> jointSpecs(final List<OutputColumn> stageColumns) {
        final Map<String, List<OutputColumn>> groups = new LinkedHashMap<>();
        for (final OutputColumn c : stageColumns) {
            if ("joint".equals(c.getOperator())) groups.computeIfAbsent(c.getCoordinates().get("joint"), j -> new ArrayList<>()).add(c);
        }
        final List<JointSpec> specs = new ArrayList<>();
        for (final Map.Entry<String, List<OutputColumn>> e : groups.entrySet()) {
            final Map<String, String> k = e.getValue().get(0).getCoordinates();
            final Forward forward = Forward.of(e.getValue().get(0));
            final String foldKeys = k.get("foldKeys");
            specs.add(new JointSpec(e.getKey(), JointFit.parseLevels(k.get("jointLevels")), k.get("field"),
                    k.containsKey("offset") ? "__baseline_" + k.get("offset") : null,
                    Shrinkage.Scale.valueOf(k.get("scale")), k.get("weights"), Double.parseDouble(k.get("priorWeight")),
                    foldKeys != null ? List.of(foldKeys.split(",")) : null,
                    foldKeys != null ? Integer.parseInt(k.get("folds")) : 0,
                    forward, Long.parseLong(k.getOrDefault("predictOffsetMillis", "0")),
                    k.get("artifactUri"), "true".equals(k.get("refit")), e.getValue()));
        }
        return specs;
    }

    /** One (cell, (y − b, b)) per row — plus the fold-tagged part under fold, or keyed by the row's block under forward. */
    static class ExtractCellsDoFn extends DoFn<MElement, KV<String, KV<Double, Double>>> {
        private final JointSpec spec;
        private final List<String> cellKeys;
        /** The leaf level's key fields: a row without them has no estimate, so it has no cell either. */
        private final List<String> leafKeys;

        ExtractCellsDoFn(final JointSpec spec) {
            this.spec = spec;
            this.cellKeys = JointFit.cellKeysOf(spec.levels());
            final List<JointFit.Level> effects = JointFit.effectLevelsOf(spec.levels());
            this.leafKeys = effects.isEmpty() ? List.of() : effects.get(0).keys();
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final MElement element = c.element();
            if (element == null) return;
            final Map<String, Object> row = element.asPrimitiveMap();
            final KV<Double, Double> value = FeatureValues.offsetTarget(row, spec.field(), spec.offsetColumn());
            if (value == null) return;
            // the leaf key must be present (as at apply time); a null coarser key leaves the row in the cells of the
            // levels it has and out of that level's contexts — the same per-level rule as the other estimators
            if (FeatureValues.key(row, leafKeys) == null) return;
            final String cell = FeatureValues.keyWithNulls(row, cellKeys);
            if (spec.forward() != null) {
                final Long millis = FeatureValues.toEpochMillis(row.get(spec.forward().blockField()), spec.forward().blockFieldType());
                if (millis == null) return;
                c.output(KV.of(JointFit.blockEntry(cell, spec.forward().blocks().indexOf(millis)), value));
                return;
            }
            c.output(KV.of(cell, value));
            if (spec.foldKeys() != null) {
                final String unit = FeatureValues.key(row, spec.foldKeys());
                if (unit != null) c.output(KV.of(JointFit.foldEntry(VarianceComponents.foldOf(unit, spec.folds()), cell), value));
            }
        }
    }

    static class FitJointDoFn extends DoFn<ArrayList<JointFit.Cell>, JointFit> {
        private final JointSpec spec;
        private final String planHash;

        FitJointDoFn(final JointSpec spec, final String planHash) {
            this.spec = spec;
            this.planHash = planHash;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final ArrayList<JointFit.Cell> entries = c.element();
            LOG.info("joint {}: solving {} on {} aggregation entries", spec.id(), JointFit.encodeLevels(spec.levels()), entries.size());
            final JointFit model = JointFit.fit(spec.levels(), spec.scale(), spec.offsetColumn() != null, spec.weights(), spec.priorWeight(), entries,
                    spec.foldKeys() == null ? 0 : spec.folds(), spec.forward() != null, spec.forward() == null ? 0 : spec.forward().windowBlocks());
            // fold / forward re-fit every run but write the whole-input solution once (refit: true overwrites)
            if (spec.artifactUri() != null && (spec.refit() || !JointFit.exists(spec.artifactUri(), planHash, spec.id()))) {
                JointFit.write(spec.artifactUri(), planHash, spec.id(), model);
            }
            c.output(model);
        }
    }

    static class ExtractValuesDoFn extends DoFn<MElement, Double> {
        private final String field;

        ExtractValuesDoFn(final String field) {
            this.field = field;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final MElement element = c.element();
            if (element == null) return;
            final Double v = FeatureValues.toDouble(element.asPrimitiveMap().get(field));
            if (v != null && !v.isNaN()) c.output(v);
        }
    }

    /** Growable double buffer: the gathered fit values of a discretize block (sorted in memory on one worker). */
    static final class Doubles implements Serializable {
        double[] values;
        int size;

        Doubles() {
            this(16);
        }

        Doubles(final int capacity) {
            values = new double[capacity];
        }

        void add(final double v) {
            if (size == values.length) values = Arrays.copyOf(values, Math.max(16, values.length * 2));
            values[size++] = v;
        }

        /** Drops the spare capacity: what crosses a worker boundary is exactly {@code size} doubles. */
        Doubles trimmed() {
            if (values.length != size) values = Arrays.copyOf(values, size);
            return this;
        }
    }

    static class GatherDoublesFn extends Combine.CombineFn<Double, Doubles, Doubles> {
        @Override
        public Doubles createAccumulator() { return new Doubles(); }

        @Override
        public Doubles addInput(final Doubles acc, final Double v) {
            acc.add(v);
            return acc;
        }

        /** Allocates the merged buffer once (the sum of the parts) instead of regrowing by doubling. */
        @Override
        public Doubles mergeAccumulators(final Iterable<Doubles> accs) {
            int total = 0;
            for (final Doubles a : accs) total += a.size;
            final Doubles out = new Doubles(Math.max(16, total));
            for (final Doubles a : accs) {
                System.arraycopy(a.values, 0, out.values, out.size, a.size);
                out.size += a.size;
            }
            return out;
        }

        @Override
        public Doubles compact(final Doubles acc) { return acc.trimmed(); }

        @Override
        public Doubles extractOutput(final Doubles acc) { return acc.trimmed(); }

        @Override
        public Coder<Doubles> getAccumulatorCoder(final org.apache.beam.sdk.coders.CoderRegistry registry, final Coder<Double> inputCoder) {
            return org.apache.beam.sdk.coders.SerializableCoder.of(Doubles.class);
        }

        @Override
        public Coder<Doubles> getDefaultOutputCoder(final org.apache.beam.sdk.coders.CoderRegistry registry, final Coder<Double> inputCoder) {
            return org.apache.beam.sdk.coders.SerializableCoder.of(Doubles.class);
        }
    }

    static class FitDiscretizeDoFn extends DoFn<Doubles, Discretization> {
        private final DiscretizeSpec spec;
        private final String planHash;

        FitDiscretizeDoFn(final DiscretizeSpec spec, final String planHash) {
            this.spec = spec;
            this.planHash = planHash;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final Doubles values = c.element();
            LOG.info("discretize {}: fitting quantile edges on {} values", spec.block(), values.size);
            final Discretization d = Discretization.fitQuantile(values.values, values.size, spec.bins(), spec.minSamplesPerBin());
            if (spec.artifactUri() != null) Discretization.write(spec.artifactUri(), planHash, spec.block(), d);
            c.output(d);
        }
    }

    // --- apply -------------------------------------------------------------------------------------

    /**
     * Writes one artifact per block from the fitted per-key statistics: the entries of a block's levels are grouped
     * under the block (plus an empty marker per block, so an empty input still writes its artifact) and written by
     * one DoFn call. The statistics arrive as a gathered PCollection, never by scanning the stage's map side input
     * (one state fetch per entry on a portable runner).
     *
     * @param lambdasView fit.mode forward: the λ per (level, block) recorded in the manifest, or null
     */
    private static void writeArtifacts(final PCollection<KV<String, VarianceComponents.KeyStats>> stats, final Map<String, String> uris,
                                       final List<FitLevel> levels, final String planHash,
                                       final PCollectionView<List<VarianceComponents.LevelLambdas>> lambdasView, final String label) {
        final Map<String, String> blockOfLevel = new HashMap<>();
        final Map<String, List<String>> levelsOfBlock = new LinkedHashMap<>();
        for (final FitLevel level : levels) {
            if (!uris.containsKey(level.block())) continue;
            blockOfLevel.put(level.id(), level.block());
            levelsOfBlock.computeIfAbsent(level.block(), b -> new ArrayList<>()).add(level.id());
        }
        final Coder<KV<String, KV<String, VarianceComponents.KeyStats>>> coder = KvCoder.of(StringUtf8Coder.of(),
                KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(VarianceComponents.KeyStats.class)));
        final PCollection<KV<String, KV<String, VarianceComponents.KeyStats>>> entries = stats
                .apply(label + "_Select", ParDo.of(new DoFn<KV<String, VarianceComponents.KeyStats>, KV<String, KV<String, VarianceComponents.KeyStats>>>() {
                    @ProcessElement
                    public void processElement(final ProcessContext c) {
                        // per-fold tags (fit.mode fold) belong to no level and stay out of the artifact
                        final String block = blockOfLevel.get(FitArtifact.levelOf(c.element().getKey()));
                        if (block != null) c.output(KV.of(block, c.element()));
                    }
                }))
                .setCoder(coder);
        final List<KV<String, KV<String, VarianceComponents.KeyStats>>> markers = new ArrayList<>();
        for (final String block : uris.keySet()) markers.add(KV.of(block, KV.of("", new VarianceComponents.KeyStats())));
        final PCollection<KV<String, KV<String, VarianceComponents.KeyStats>>> marker = stats.getPipeline()
                .apply(label + "_Marker", Create.of(markers).withCoder(coder));
        PCollectionList.of(entries).and(marker)
                .apply(label + "_Flatten", Flatten.pCollections())
                .apply(label + "_Group", GroupByKey.create())
                .apply(label, ParDo
                        .of(new WriteArtifactDoFn(uris, levelsOfBlock, planHash, lambdasView, scoreScalesOf(levels)))
                        .withSideInputs(lambdasView == null ? List.of() : List.of(lambdasView)));
    }

    /** One element per block: the entries of its levels ({@link #writeArtifacts}); the empty key is the marker. */
    static class WriteArtifactDoFn extends DoFn<KV<String, Iterable<KV<String, VarianceComponents.KeyStats>>>, Void> {
        private final Map<String, String> uris;
        private final Map<String, List<String>> levels;
        private final String planHash;
        /** fit.mode forward: the manifest records λ per block of the block's levels */
        private final PCollectionView<List<VarianceComponents.LevelLambdas>> lambdasView;
        /** the score levels (level id → scale): their manifest λ is estimated on the score scale */
        private final Map<String, String> scoreScales;

        WriteArtifactDoFn(final Map<String, String> uris, final Map<String, List<String>> levels, final String planHash,
                          final PCollectionView<List<VarianceComponents.LevelLambdas>> lambdasView, final Map<String, String> scoreScales) {
            this.uris = uris;
            this.levels = levels;
            this.planHash = planHash;
            this.lambdasView = lambdasView;
            this.scoreScales = new HashMap<>(scoreScales);
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final String block = c.element().getKey();
            final List<String> blockLevels = levels.getOrDefault(block, List.of());
            final Map<String, VarianceComponents.KeyStats> blockStats = new HashMap<>();
            for (final KV<String, VarianceComponents.KeyStats> e : c.element().getValue()) {
                if (!e.getKey().isEmpty()) blockStats.put(e.getKey(), e.getValue());
            }
            com.google.gson.JsonObject extra = null;
            if (lambdasView != null) {
                extra = new com.google.gson.JsonObject();
                final com.google.gson.JsonObject byBlock = new com.google.gson.JsonObject();
                for (final VarianceComponents.LevelLambdas l : c.sideInput(lambdasView)) {
                    if (!blockLevels.contains(l.level)) continue;
                    final com.google.gson.JsonObject perBlock = new com.google.gson.JsonObject();
                    for (final Map.Entry<Long, Double> b : l.byBlock.entrySet()) perBlock.addProperty(Long.toString(b.getKey()), b.getValue());
                    byBlock.add(l.level, perBlock);
                }
                extra.add("lambdasByBlock", byBlock);
            }
            FitArtifact.write(uris.get(block), planHash, block, blockStats, blockLevels, extra, scoreScales);
        }
    }

    static class FitApplyDoFn extends StageDoFn<MElement> {
        /** Loaded artifacts per path, for the JVM lifetime (content-addressed paths: see FitArtifact). */
        private static final Map<String, Map<String, VarianceComponents.KeyStats>> ARTIFACT_CACHE = new java.util.concurrent.ConcurrentHashMap<>();
        private static final Map<String, Object> MODEL_CACHE = new java.util.concurrent.ConcurrentHashMap<>();

        private final List<FitLevel> levels;
        private final PCollectionView<Map<String, VarianceComponents.KeyStats>> statsView;
        /** fit.mode forward and time folds: cumulative per-block statistics per (level, key), read once per instance into {@link #forwardSeries} */
        private final PCollectionView<List<KV<String, ForwardBlocks.Series>>> seriesView;
        /** fit.mode forward: λ per level per block (present when a lattice column of the stage reads variance components) */
        private final PCollectionView<List<VarianceComponents.LevelLambdas>> forwardLambdasView;
        /** fit.fold.by time: the whole-input λ per level (present when a lattice column of the stage reads variance components) */
        private final PCollectionView<Map<String, Double>> timeFoldLambdasView;
        private final long predictOffsetMillis;
        private final Map<String, String> loadBlocks;
        private final String planHash;
        private final List<StaticFitBlock<?>> blocks;
        private final Map<String, PCollectionView<?>> blockViews;
        private final Set<String> blockLoad;
        /** The models of the stage's summary-fit blocks as one (block, model) list, and the blocks fitted that way. */
        private final PCollectionView<List<KV<String, Serializable>>> summaryModelsView;
        private final Set<String> summaryFitted;
        /** Indexed once per DoFn instance (the list is immutable in a batch run; a block without contributions has no entry). */
        private transient Map<String, Serializable> summaryModels;
        private transient Map<String, VarianceComponents.KeyStats> loaded;
        private transient Map<String, Double> loadedLambdas;
        /**
         * The whole-input λ (loaded artifacts, the static fit's and the time folds'), merged once per DoFn instance: the
         * side inputs exist in a batch run only (a streaming fit requires artifacts) and are global-window values, so
         * they never change for an instance — re-reading and re-scanning the map side inputs per row would be one state
         * fetch per entry per row on a portable runner.
         */
        private transient Map<String, Double> mergedLambdas;
        private transient Map<String, Object> loadedModels;
        /**
         * fit.mode forward: the series by entry and λ per level per block, each read once per DoFn instance from its
         * list side input (immutable in a batch run). A per-row lookup into a map side input would be one state
         * fetch per (row, level) on a portable runner; the in-memory index makes it a hash lookup.
         */
        private transient Map<String, ForwardBlocks.Series> forwardSeries;
        private transient Map<String, TreeMap<Long, Double>> forwardLambdas;
        /** Time folds: the input's block span per level (from the series index), the excluded-over-half counters and the levels warned about. */
        private transient Map<String, long[]> timeFoldSpans;
        private transient Map<String, Counter> counters;
        private transient Set<String> warnedLevels;

        FitApplyDoFn(final StageEvaluator evaluator, final List<FitLevel> levels,
                     final PCollectionView<Map<String, VarianceComponents.KeyStats>> statsView,
                     final PCollectionView<Map<String, Double>> lambdas,
                     final PCollectionView<List<KV<String, ForwardBlocks.Series>>> seriesView,
                     final PCollectionView<List<VarianceComponents.LevelLambdas>> forwardLambdasView,
                     final PCollectionView<Map<String, Double>> timeFoldLambdasView, final long predictOffsetMillis,
                     final Map<String, String> loadBlocks, final String planHash,
                     final List<StaticFitBlock<?>> blocks, final Map<String, PCollectionView<?>> blockViews, final Set<String> blockLoad,
                     final PCollectionView<List<KV<String, Serializable>>> summaryModelsView, final Set<String> summaryFitted,
                     final List<Logging> loggings, final boolean failFast, final TupleTag<BadRecord> failureTag) {
            super(evaluator, lambdas, loggings, failFast, failureTag);
            this.levels = levels;
            this.statsView = statsView;
            this.seriesView = seriesView;
            this.forwardLambdasView = forwardLambdasView;
            this.timeFoldLambdasView = timeFoldLambdasView;
            this.predictOffsetMillis = predictOffsetMillis;
            this.loadBlocks = loadBlocks;
            this.planHash = planHash;
            this.blocks = blocks;
            this.blockViews = blockViews;
            this.blockLoad = blockLoad;
            this.summaryModelsView = summaryModelsView;
            this.summaryFitted = summaryFitted;
        }

        @Setup
        public void setup() {
            super.setup();
            loaded = new HashMap<>();
            // a block with a logit offset term reads Σ b(1 − b): an artifact from before that statistic is refused
            final Set<String> infoBlocks = new HashSet<>();
            for (final FitLevel level : levels) if (level.infoSumColumn() != null) infoBlocks.add(level.block());
            for (final Map.Entry<String, String> e : loadBlocks.entrySet()) {
                final String path = FitArtifact.statsPath(e.getValue(), planHash, e.getKey());
                loaded.putAll(ARTIFACT_CACHE.computeIfAbsent(path, p -> FitArtifact.read(e.getValue(), planHash, e.getKey(), infoBlocks.contains(e.getKey()))));
            }
            loadedLambdas = loaded.isEmpty() ? Map.of() : VarianceComponents.lambdasInMemory(loaded, scoreScalesOf(levels));
            counters = new HashMap<>();
            warnedLevels = new HashSet<>();
            loadedModels = new HashMap<>();
            for (final StaticFitBlock<?> block : blocks) {
                if (!blockLoad.contains(block.block())) continue;
                loadedModels.put(block.block(), MODEL_CACHE.computeIfAbsent(block.artifactPath(planHash), p -> block.readArtifact(planHash)));
            }
        }

        /** The block's model: loaded from its artifact at setup, else the first (only) element of its fit view. */
        private Object model(final ProcessContext c, final StaticFitBlock<?> block) {
            final Object loadedModel = loadedModels.get(block.block());
            if (loadedModel != null) return loadedModel;
            if (summaryFitted.contains(block.block())) {
                if (summaryModels == null) {
                    final Map<String, Serializable> index = new HashMap<>();
                    for (final KV<String, Serializable> e : c.sideInput(summaryModelsView)) index.put(e.getKey(), e.getValue());
                    summaryModels = index;
                }
                return summaryModels.get(block.block());
            }
            final PCollectionView<?> view = blockViews.get(block.block());
            if (view == null) throw new IllegalStateException("static fit block " + block.block() + " was neither fitted nor loaded");
            final List<?> models = (List<?>) c.sideInput(view);
            return models.isEmpty() ? null : models.get(0);
        }

        @SuppressWarnings("unchecked")
        private static <M extends Serializable> void apply(final StaticFitBlock<M> block, final Object model, final Map<String, Object> values) {
            block.apply((M) model, values);
        }

        @Override
        protected void prepare(final ProcessContext c) {
            if (mergedLambdas == null) {
                final Map<String, Double> merged = new HashMap<>(loadedLambdas);
                if (lambdas != null) merged.putAll(c.sideInput(lambdas));
                if (timeFoldLambdasView != null) merged.putAll(c.sideInput(timeFoldLambdasView));
                mergedLambdas = merged;
            }
            evaluator.setLambdas(mergedLambdas);
        }

        /** fit.mode forward: the row's statistics = the series up to its usable block (minus the window's older blocks). */
        private VarianceComponents.KeyStats forwardStats(final FitLevel level, final ForwardBlocks.Series s,
                                                          final long eventMillis, final Map<String, Double> rowLambdas) {
            final Forward f = level.forward();
            final long usable = f.blocks().usableBlock(eventMillis, predictOffsetMillis, f.lagMillis());
            if (rowLambdas != null && forwardLambdas != null && forwardLambdas.containsKey(level.id())) {
                final Map.Entry<Long, Double> lambda = forwardLambdas.get(level.id()).floorEntry(usable);
                if (lambda != null) rowLambdas.put(level.id(), lambda.getValue());
                else rowLambdas.remove(level.id());
            }
            if (s == null) return null;
            final int position = s.floor(usable);
            if (position < 0 || position + 1 < f.minBlocks()) return null;
            final int from = f.windowBlocks() > 0 ? s.floor(usable - f.windowBlocks()) : -1;
            return s.statsBetween(from, position);
        }

        /**
         * A time fold: the totals minus the blocks {@code [b − purge, b + purge + embargo]} around the row's block
         * {@code b} — one prefix difference of the series. The purge is two-sided (label windows overlap in both
         * directions), the embargo an extra buffer after it. λ is the whole input's, set with the other levels' in
         * {@link #prepare} (as for a hash fold). Under {@code fit.fold.until} the totals and the range are those of the
         * blocks up to the until block, and a row of a later block reads forward: the blocks before its usable one
         * (the block that ends before {@code event + predictOffset − lag}), as a {@code fit.mode forward} level
         * without a window or a floor does — its evaluation values are walk-forward, never cross-fit.
         */
        private VarianceComponents.KeyStats timeFoldStats(final FitLevel level, final ForwardBlocks.Series s, final long eventMillis) {
            if (s == null) return null;
            final TimeFold f = level.timeFold();
            final long block = f.blocks().indexOf(eventMillis);
            if (f.isEvaluation(block)) {
                final int position = s.floor(f.blocks().usableBlock(eventMillis, predictOffsetMillis, f.lagMillis()));
                return position < 0 ? null : s.statsBetween(-1, position);
            }
            final long from = f.from(block);
            final long to = f.untilBlock() == null ? f.to(block) : Math.min(f.to(block), f.untilBlock());
            auditTimeFold(level, from, to);
            final VarianceComponents.KeyStats excluded = s.statsBetween(s.floor(from - 1), s.floor(to));
            final VarianceComponents.KeyStats totals = f.untilBlock() == null ? s.totals() : s.statsBetween(-1, s.floor(f.untilBlock()));
            return VarianceComponents.subtract(totals, excluded);
        }

        /**
         * A time fold leaving out more than half of the input's blocks (the span of every key's series of the level,
         * clipped to it) reads a minority of the data: counter {@code feature/timeFold_<level>_excludedOverHalf} per
         * such row, and one warning per level and DoFn instance. Only the engine sees the input's block span.
         */
        private void auditTimeFold(final FitLevel level, final long from, final long to) {
            final long[] span = timeFoldSpans == null ? null : timeFoldSpans.get(level.id());
            if (span == null) return;
            // under fit.fold.until the training period ends at the until block
            final Long until = level.timeFold().untilBlock();
            final long last = until == null ? span[1] : Math.min(span[1], until);
            if (last < span[0]) return;
            final long total = last - span[0] + 1;
            final long excluded = Math.min(to, last) - Math.max(from, span[0]) + 1;
            if (2 * excluded <= total) return;
            counters.computeIfAbsent(level.id(), id -> Metrics.counter("feature", "timeFold_" + id + "_excludedOverHalf")).inc();
            if (warnedLevels.add(level.id())) {
                final TimeFold f = level.timeFold();
                LOG.warn("feature fit: time fold of level {} leaves out {} of the {} blocks of the {} around a row (purge {} on both sides, embargo {}): "
                        + "its out-of-fold statistics read less than half of them; use smaller blocks or a shorter purge / embargo",
                        level.id(), excluded, total, until == null ? "input" : "training period (fit.fold.until)", f.purgeBlocks(), f.embargoBlocks());
            }
        }

        /** The block span [first, last] of each time-fold level over every key's series. */
        private Map<String, long[]> timeFoldSpans(final Map<String, ForwardBlocks.Series> index) {
            final Set<String> timeFolds = new HashSet<>();
            for (final FitLevel level : levels) if (level.isTimeFold()) timeFolds.add(level.id());
            final Map<String, long[]> spans = new HashMap<>();
            if (timeFolds.isEmpty()) return spans;
            for (final Map.Entry<String, ForwardBlocks.Series> e : index.entrySet()) {
                final ForwardBlocks.Series s = e.getValue();
                final String level = FitArtifact.levelOf(e.getKey());
                if (s.size() == 0 || !timeFolds.contains(level)) continue;
                final long[] span = spans.computeIfAbsent(level, l -> new long[]{Long.MAX_VALUE, Long.MIN_VALUE});
                span[0] = Math.min(span[0], s.blockAt(0));
                span[1] = Math.max(span[1], s.blockAt(s.size() - 1));
            }
            return spans;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final MElement input = c.element();
            if (input == null) return;
            try {
                prepare(c);
                final Map<String, VarianceComponents.KeyStats> fitted = statsView == null ? Map.of() : c.sideInput(statsView);
                if (seriesView != null && forwardSeries == null) {
                    final Map<String, ForwardBlocks.Series> index = new HashMap<>();
                    for (final KV<String, ForwardBlocks.Series> e : c.sideInput(seriesView)) index.put(e.getKey(), e.getValue());
                    forwardSeries = index;
                    timeFoldSpans = timeFoldSpans(index);
                    LOG.info("feature fit: forward series indexed ({} entries)", index.size());
                }
                if (forwardLambdasView != null && forwardLambdas == null) {
                    forwardLambdas = VarianceComponents.lambdasByBlock(c.sideInput(forwardLambdasView));
                }
                final Map<String, ForwardBlocks.Series> series = forwardSeries == null ? Map.of() : forwardSeries;
                final Map<String, Object> values = input.asPrimitiveMap();
                Map<String, Double> rowLambdas = null;
                for (final FitLevel level : levels) {
                    final String key = FeatureValues.key(values, level.keys());
                    VarianceComponents.KeyStats stats = null;
                    if (key != null && level.isForward()) {
                        final Long eventMillis = FeatureValues.toEpochMillis(values.get(level.forward().blockField()), level.forward().blockFieldType());
                        if (eventMillis != null) {
                            if (forwardLambdas != null && rowLambdas == null) rowLambdas = new HashMap<>(evaluator.row.lambdas());
                            stats = forwardStats(level, series.get(FitArtifact.entryKey(level.id(), key)), eventMillis, rowLambdas);
                        }
                    } else if (key != null && level.isTimeFold()) {
                        final Long eventMillis = FeatureValues.toEpochMillis(values.get(level.timeFold().blockField()), level.timeFold().blockFieldType());
                        if (eventMillis != null) stats = timeFoldStats(level, series.get(FitArtifact.entryKey(level.id(), key)), eventMillis);
                    } else if (key != null) {
                        final String entry = FitArtifact.entryKey(level.id(), key);
                        stats = loaded.get(entry);
                        if (stats == null) stats = fitted.get(entry);
                        if (level.isFold() && stats != null) {
                            // out-of-fold: remove the row's own fold from the totals
                            final String unit = FeatureValues.key(values, level.foldKeys());
                            if (unit != null) {
                                stats = VarianceComponents.subtract(stats,
                                        fitted.get(VarianceComponents.foldEntry(VarianceComponents.foldOf(unit, level.folds()), entry)));
                            }
                        }
                    }
                    values.put(level.id(), stats == null ? 0d : stats.n);
                    if (level.sumColumn() != null) values.put(level.sumColumn(), stats == null ? 0d : stats.sum);
                    if (level.sumSqColumn() != null) values.put(level.sumSqColumn(), stats == null ? 0d : stats.sumSq);
                    if (level.offSumColumn() != null) values.put(level.offSumColumn(), stats == null ? 0d : stats.sumOff);
                    if (level.infoSumColumn() != null) values.put(level.infoSumColumn(), stats == null ? 0d : stats.sumInfo);
                }
                if (rowLambdas != null) evaluator.setLambdas(rowLambdas); // the λ of the row's usable block, per forward level
                for (final StaticFitBlock<?> block : blocks) apply(block, model(c, block), values);
                evaluator.evaluateRowColumns(values);
                c.output(MElement.of(values, c.timestamp()));
            } catch (final Throwable e) {
                c.output(failureTag, Module.processError("Failed to apply fitted features", input, e, failFast));
            }
        }
    }

    static FeatureSpec.ContextDef groupByContext(final FeaturePlan plan) {
        final String name = plan.getSpec().output.groupBy;
        if (name == null) return null;
        for (final FeatureSpec.ContextDef c : plan.getSpec().contexts) if (c.name().equals(name)) return c;
        return null;
    }

    /**
     * Output schema: input fields + emitted columns, or the grouped parent/children shape (§3.1). Every field
     * carries lineage in its options — the emitted columns theirs ({@link OutputColumn#toOptions}, the role
     * included), the pass-through input fields the contract of their source
     * ({@link FeaturePlan#passThroughOptions}: {@code feature.scope = input}, {@code feature.kind},
     * {@code feature.derivedFrom}, {@code feature.sources}, {@code feature.evidence}, {@code feature.role}) — so a
     * consumer's lineage selectors ({@code derivedFrom:market}, {@code scope:input}) and role defaults see an
     * input column the same way the manifest's {@code fields} entry describes it.
     */
    public static Schema createOutputSchema(final FeaturePlan plan, final Schema inputSchema, final DataType outputType) {
        final FeatureSpec.ContextDef groupBy = groupByContext(plan);
        final Set<String> passThrough = passThroughInputs(plan, inputSchema);
        if (groupBy == null) {
            final Schema.Builder builder = Schema.builder();
            for (final Schema.Field f : inputSchema.getFields()) if (passThrough.contains(f.getName())) builder.withField(passThroughField(plan, f));
            for (final OutputColumn c : plan.getEmittedColumns()) builder.withField(c.toField());
            return builder.withType(outputType).build();
        }
        final Set<String> parentInputs = new LinkedHashSet<>(groupBy.keys());
        parentInputs.addAll(plan.getSpec().output.parentFields);
        final Schema.Builder parent = Schema.builder();
        final Schema.Builder child = Schema.builder();
        for (final Schema.Field f : inputSchema.getFields()) {
            if (!passThrough.contains(f.getName())) continue;
            (parentInputs.contains(f.getName()) ? parent : child).withField(passThroughField(plan, f));
        }
        for (final OutputColumn c : plan.getEmittedColumns()) {
            (c.getPlacement() == OutputColumn.Placement.parent ? parent : child).withField(c.toField());
        }
        parent.withField(plan.getSpec().output.childName, Schema.FieldType.array(Schema.FieldType.element(child.build())));
        return parent.withType(outputType).build();
    }

    /**
     * A pass-through input field with this table's lineage as its options. The {@code feature.*} options the field
     * arrived with (an upstream feature transform's column: its block, operator, role ...) describe that table and
     * are replaced, except the derivedFrom lineage, which {@link FeaturePlan#passThroughOptions} carries forward.
     */
    static Schema.Field passThroughField(final FeaturePlan plan, final Schema.Field f) {
        final Map<String, String> options = plan.passThroughOptions(f);
        final Schema.Field field = f.copy();
        field.getOptions().keySet().removeIf(k -> k.startsWith("feature."));
        field.getOptions().putAll(options);
        return field;
    }

    // ------------------------------------------------------------------------------------------
    // stage evaluator: dispatches the stage's columns in evaluation order
    // ------------------------------------------------------------------------------------------

    static class StageEvaluator implements Serializable {

        private final List<OutputColumn> columns;
        final RowEvaluator row;
        private final ContextEvaluator context;
        private final SequenceEvaluator sequence;
        private final PopulationEvaluator population;
        private final Set<String> bufferedFields = new LinkedHashSet<>();
        private transient SequenceEvaluator.Watermarks watermarks;

        StageEvaluator(final List<OutputColumn> columns) {
            this.columns = columns;
            final List<OutputColumn> rows = new ArrayList<>(), contexts = new ArrayList<>(),
                    sequences = new ArrayList<>(), populations = new ArrayList<>();
            for (final OutputColumn c : columns) {
                switch (kindOf(c)) {
                    case row -> rows.add(c);
                    case context -> contexts.add(c);
                    case sequence -> sequences.add(c);
                    case population -> populations.add(c);
                }
            }
            this.row = new RowEvaluator(rows);
            this.context = new ContextEvaluator(contexts);
            this.sequence = new SequenceEvaluator(sequences);
            this.population = new PopulationEvaluator(populations);
            bufferedFields.addAll(sequence.bufferedFields());
            bufferedFields.addAll(population.bufferedFields());
        }

        static Scope kindOf(final OutputColumn c) {
            if ("isnull".equals(c.getOperator())) return Scope.row;
            if ("baseline".equals(c.getOperator())) return c.getScope() == Scope.context ? Scope.context : Scope.row;
            return c.getScope();
        }

        void setup() {
            row.setup();
            context.setup();
            sequence.setup();
            population.setup();
            watermarks = new SequenceEvaluator.Watermarks(bufferedFields);
            sequence.register(watermarks);
            population.register(watermarks);
        }

        void setLambdas(final Map<String, Double> lambdas) {
            row.setLambdas(lambdas);
        }

        /** Row-scope columns only (fitted statistics are already in the map). */
        void evaluateRowColumns(final Map<String, Object> values) {
            for (final OutputColumn c : columns) {
                if (kindOf(c) == Scope.row) values.put(c.getCanonicalName(), row.evaluateColumn(c, values));
            }
        }

        boolean hasKeyed() {
            return !sequence.getColumns().isEmpty() || !population.getColumns().isEmpty();
        }

        /** Stateless rows: only row-scope columns; keyed columns are left null. */
        void evaluateRow(final Map<String, Object> values) {
            for (final OutputColumn c : columns) {
                values.put(c.getCanonicalName(), kindOf(c) == Scope.row ? row.evaluateColumn(c, values) : null);
            }
        }

        void evaluateGroup(final List<Map<String, Object>> rows) {
            for (final OutputColumn c : columns) {
                if (kindOf(c) == Scope.context) {
                    context.evaluateColumn(c, rows);
                } else {
                    for (final Map<String, Object> r : rows) r.put(c.getCanonicalName(), kindOf(c) == Scope.row ? row.evaluateColumn(c, r) : null);
                }
            }
        }

        void evaluateKeyed(final Map<String, Object> values, final long nowMillis, final List<Past> history,
                           final SequenceEvaluator.KeyState sequenceState, final SequenceEvaluator.KeyState populationState) {
            for (final OutputColumn c : columns) {
                final Object v = switch (kindOf(c)) {
                    case row -> row.evaluateColumn(c, values);
                    case sequence -> sequence.evaluateColumn(c, values, nowMillis, history, sequenceState);
                    case population -> population.evaluateColumn(c, values, nowMillis, history, populationState);
                    default -> null;
                };
                values.put(c.getCanonicalName(), v);
            }
        }

        List<String> unboundedColumns() {
            final List<String> names = new ArrayList<>(sequence.unboundedColumns());
            names.addAll(population.unboundedColumns());
            return names;
        }

        /** Trim watermarks of the shared history: both evaluators fold their columns' retention into one reused instance. */
        SequenceEvaluator.Watermarks watermarks(final long nowMillis, final List<Past> history,
                                                final SequenceEvaluator.KeyState sequenceState, final SequenceEvaluator.KeyState populationState) {
            watermarks.reset(history.size());
            sequence.retainInto(sequenceState, nowMillis, history, watermarks);
            population.retainInto(populationState, nowMillis, history, watermarks);
            return watermarks;
        }

        Map<String, Object> project(final Map<String, Object> values) {
            final Map<String, Object> projected = new HashMap<>();
            for (final String f : bufferedFields) projected.put(f, values.get(f));
            return projected;
        }
    }

    // ------------------------------------------------------------------------------------------
    // DoFns
    // ------------------------------------------------------------------------------------------

    /** Converts any input element to the ELEMENT map form and sets its timestamp from {@code time.field}. */
    static class ToElementDoFn extends DoFn<MElement, MElement> {
        private final String timeField;
        private final String timeFieldType;
        /** engine.rowId fields (empty = random id); null = no row id (linear chain, nothing to merge) */
        private final List<String> rowIdFields;
        private final boolean failFast;
        private final TupleTag<BadRecord> failureTag;
        /** observedAt audit entries (observation column present); empty = no audit */
        private final List<FeaturePlan.ObservedAtAudit> audits;
        /** audit.observedAt: fail — a row observed after its declared availability goes to the failure output */
        private final boolean failOnLate;
        /** audit samples for the run manifest (null = counters only) */
        private final TupleTag<KV<String, Double>> auditTag;
        private transient Map<String, Counter> counters;

        ToElementDoFn(final String timeField, final String timeFieldType, final List<String> rowIdFields,
                      final boolean failFast, final TupleTag<BadRecord> failureTag) {
            this(timeField, timeFieldType, rowIdFields, failFast, failureTag, List.of(), false, null);
        }

        ToElementDoFn(final String timeField, final String timeFieldType, final List<String> rowIdFields,
                      final boolean failFast, final TupleTag<BadRecord> failureTag,
                      final List<FeaturePlan.ObservedAtAudit> audits, final boolean failOnLate, final TupleTag<KV<String, Double>> auditTag) {
            this.timeField = timeField;
            this.timeFieldType = timeFieldType;
            this.rowIdFields = rowIdFields;
            this.failFast = failFast;
            this.failureTag = failureTag;
            this.audits = audits;
            this.failOnLate = failOnLate;
            this.auditTag = auditTag;
        }

        @Setup
        public void setup() {
            counters = new HashMap<>();
        }

        private void count(final String name) {
            counters.computeIfAbsent(name, n -> Metrics.counter("feature", n)).inc();
        }

        private void sample(final ProcessContext c, final String key, final double value) {
            if (auditTag != null) c.output(auditTag, KV.of(key, value));
        }

        /**
         * The observedAt audit of one row: for every audited field, the observation time is compared with the
         * declared availability ({@code event_time + availableAt}; predictAt when the declaration is dynamic)
         * and with predictAt. Counters {@code feature/observedAt_<field>_late|afterPredictAt|missing};
         * {@code predictAt − observedAt} (seconds) is sampled for the run manifest quantiles.
         */
        private void audit(final ProcessContext c, final Map<String, Object> values, final long eventMillis) {
            for (final FeaturePlan.ObservedAtAudit a : audits) {
                final String field = a.field();
                sample(c, field + "#rows", 1d);
                if (values.get(field) == null) {
                    sample(c, field + "#nullValue", 1d);
                    continue;
                }
                final Long observed = FeatureValues.toEpochMillis(values.get(a.observedAtField()), a.observedAtType());
                if (observed == null) {
                    count("observedAt_" + field + "_missing");
                    sample(c, field + "#missing", 1d);
                    continue;
                }
                final Long deadlineOffset = a.deadlineOffsetMillis();
                final Long predictOffset = a.predictAtOffsetMillis();
                final Long deadline = deadlineOffset != null ? eventMillis + deadlineOffset : predictOffset != null ? eventMillis + predictOffset : null;
                if (deadline != null && observed > deadline) {
                    count("observedAt_" + field + "_late");
                    sample(c, field + "#late", 1d);
                    if (failOnLate) {
                        throw new IllegalStateException("observedAt audit: '" + field + "' observed at " + Instant.ofEpochMilli(observed)
                                + " after its declared availability " + Instant.ofEpochMilli(deadline) + " (" + a.availableAt().describe() + ")");
                    }
                }
                if (predictOffset != null) {
                    final long predictAt = eventMillis + predictOffset;
                    if (observed > predictAt) {
                        count("observedAt_" + field + "_afterPredictAt");
                        sample(c, field + "#afterPredictAt", 1d);
                    }
                    sample(c, field, (predictAt - observed) / 1000d);
                }
            }
        }

        @Override
        public org.joda.time.Duration getAllowedTimestampSkew() {
            return org.joda.time.Duration.millis(Long.MAX_VALUE);
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final MElement input = c.element();
            if (input == null) return;
            try {
                final Map<String, Object> values = input.asPrimitiveMap();
                final Long millis = FeatureValues.toEpochMillis(values.get(timeField), timeFieldType);
                if (millis == null) {
                    throw new IllegalArgumentException("time.field '" + timeField + "' is null or not a timestamp; rows cannot be ordered");
                }
                final Instant ts = Instant.ofEpochMilli(millis);
                if (!audits.isEmpty()) audit(c, values, millis);
                if (rowIdFields != null) {
                    // a declared engine.rowId must be deterministic across retries (null components become a
                    // token; rows genuinely colliding on it surface through the merge's uniqueness rejection);
                    // only the undeclared random id may differ per attempt, and the engine pins it (RowId_Pin)
                    values.put(ROW_ID_FIELD, rowIdFields.isEmpty()
                            ? UUID.randomUUID().toString()
                            : FeatureValues.keyWithNullTokens(values, rowIdFields));
                }
                c.outputWithTimestamp(MElement.of(values, ts), ts);
            } catch (final Throwable e) {
                c.output(failureTag, Module.processError("Failed to prepare feature input", input, e, failFast));
            }
        }
    }

    /** Keys a merge piece by its row id. */
    static class RowIdKeyDoFn extends DoFn<MElement, KV<String, MElement>> {
        @ProcessElement
        public void processElement(final ProcessContext c) {
            final MElement element = c.element();
            if (element == null) return;
            final Object id = element.getPrimitiveValue(ROW_ID_FIELD);
            c.output(KV.of(id == null ? NULL_KEY : id.toString(), element));
        }
    }

    /**
     * The partial row of a branch: the row id, the branch's columns and the fields the merge groups by (the
     * next stage's key), so the shuffle carries a branch's new columns only, not the whole row.
     */
    static class PartialDoFn extends DoFn<MElement, MElement> {
        private final List<String> columns;
        private final List<String> carry;

        PartialDoFn(final List<String> columns, final List<String> carry) {
            this.columns = columns;
            this.carry = carry;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final MElement element = c.element();
            if (element == null) return;
            // per-field reads: copying the whole (wide) row map to keep a handful of columns is the hot path
            final Map<String, Object> partial = new HashMap<>();
            partial.put(ROW_ID_FIELD, element.getPrimitiveValue(ROW_ID_FIELD));
            partial.put(PARTIAL_FIELD, true);
            for (final String name : columns) partial.put(name, element.getPrimitiveValue(name));
            for (final String name : carry) partial.put(name, element.getPrimitiveValue(name));
            c.output(MElement.of(partial, c.timestamp()));
        }
    }

    /** Reassembles one row from its base and partial rows (row-id merge of a wave). */
    static class MergeDoFn extends DoFn<KV<String, Iterable<MElement>>, MElement> {
        private final int branches;
        private final boolean failFast;
        private final TupleTag<BadRecord> failureTag;

        MergeDoFn(final int branches, final boolean failFast, final TupleTag<BadRecord> failureTag) {
            this.branches = branches;
            this.failFast = failFast;
            this.failureTag = failureTag;
        }

        // rows are re-emitted at their own event time after the GroupByKey moved them to the window end
        @Override
        public org.joda.time.Duration getAllowedTimestampSkew() {
            return org.joda.time.Duration.millis(Long.MAX_VALUE);
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final KV<String, Iterable<MElement>> kv = c.element();
            if (kv == null) return;
            final List<Rejection> rejected = new ArrayList<>();
            for (final MElement row : coalesce(kv.getValue(), branches, rejected)) c.outputWithTimestamp(row, row.getTimestamp());
            for (final BadRecord record : rejectionRecords(rejected, failFast)) c.output(failureTag, record);
        }
    }

    static class KeyDoFn extends DoFn<MElement, KV<String, MElement>> {
        private final List<String> keys;

        KeyDoFn(final List<String> keys) {
            this.keys = keys;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final MElement element = c.element();
            if (element == null) return;
            final StringBuilder sb = new StringBuilder();
            for (final String k : keys) {
                final Object v = element.getPrimitiveValue(k);
                if (v == null) {
                    c.output(KV.of(NULL_KEY, element));
                    return;
                }
                FeatureValues.appendKeyComponent(sb, v);
            }
            c.output(KV.of(sb.toString(), element));
        }
    }

    /** Keys like {@link KeyDoFn} and pairs each row with a sortable event time for {@link KeyedSpillSorter}. */
    static class SortKeyDoFn extends DoFn<MElement, KV<String, KV<Long, MElement>>> {
        private final List<String> keys;
        /** Sort the key's rows latest first (a future stage). */
        private final boolean descending;

        SortKeyDoFn(final List<String> keys, final boolean descending) {
            this.keys = keys;
            this.descending = descending;
        }

        /** The sort key: the epoch millis themselves (compared as a signed long by {@link KeyedSpillSorter}). */
        static long sortable(final long millis) {
            return millis;
        }

        /** The descending sort key: the bitwise complement reverses the signed order without overflowing. */
        static long sortableDescending(final long millis) {
            return ~millis;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final MElement element = c.element();
            if (element == null) return;
            final long millis = element.getEpochMillis();
            final KV<Long, MElement> value = KV.of(descending ? sortableDescending(millis) : sortable(millis), element);
            final StringBuilder sb = new StringBuilder();
            for (final String k : keys) {
                final Object v = element.getPrimitiveValue(k);
                if (v == null) {
                    c.output(KV.of(NULL_KEY, value));
                    return;
                }
                FeatureValues.appendKeyComponent(sb, v);
            }
            c.output(KV.of(sb.toString(), value));
        }
    }

    abstract static class StageDoFn<InputT> extends DoFn<InputT, MElement> {
        protected final StageEvaluator evaluator;
        protected final PCollectionView<Map<String, Double>> lambdas;
        protected final Map<String, Logging> logs;
        protected final boolean failFast;
        protected final TupleTag<BadRecord> failureTag;

        StageDoFn(final StageEvaluator evaluator, final PCollectionView<Map<String, Double>> lambdas,
                  final List<Logging> loggings, final boolean failFast, final TupleTag<BadRecord> failureTag) {
            this.evaluator = evaluator;
            this.lambdas = lambdas;
            this.logs = Logging.map(loggings);
            this.failFast = failFast;
            this.failureTag = failureTag;
        }

        @Setup
        public void setup() {
            evaluator.setup();
        }

        /** Reads the variance-components side input (if any) before evaluating an element. */
        protected void prepare(final ProcessContext c) {
            if (lambdas != null) evaluator.setLambdas(c.sideInput(lambdas));
        }
    }

    static class RowStageDoFn extends StageDoFn<MElement> {
        RowStageDoFn(final StageEvaluator evaluator, final PCollectionView<Map<String, Double>> lambdas,
                     final List<Logging> loggings, final boolean failFast, final TupleTag<BadRecord> failureTag) {
            super(evaluator, lambdas, loggings, failFast, failureTag);
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final MElement input = c.element();
            if (input == null) return;
            try {
                prepare(c);
                final Map<String, Object> values = input.asPrimitiveMap();
                evaluator.evaluateRow(values);
                c.output(MElement.of(values, c.timestamp()));
            } catch (final Throwable e) {
                c.output(failureTag, Module.processError("Failed to evaluate row features", input, e, failFast));
            }
        }
    }

    static class ContextStageDoFn extends StageDoFn<KV<String, Iterable<MElement>>> {
        /** Branch count of a fan-out wave folded into this stage's GroupByKey; 0 = the input carries plain rows. */
        private final int fanInBranches;

        ContextStageDoFn(final StageEvaluator evaluator, final PCollectionView<Map<String, Double>> lambdas,
                         final int fanInBranches, final List<Logging> loggings, final boolean failFast, final TupleTag<BadRecord> failureTag) {
            super(evaluator, lambdas, loggings, failFast, failureTag);
            this.fanInBranches = fanInBranches;
        }

        // rows are re-emitted at their own event time after the GroupByKey moved them to the window end
        @Override
        public org.joda.time.Duration getAllowedTimestampSkew() {
            return org.joda.time.Duration.millis(Long.MAX_VALUE);
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final KV<String, Iterable<MElement>> kv = c.element();
            if (kv == null) return;
            // a folded fan-out merge delivers base and partial rows: reassemble them by row id first
            // (a plain context stage skips the reassembly — and its per-element probes — entirely)
            final List<MElement> elements;
            if (fanInBranches > 0) {
                final List<Rejection> rejected = new ArrayList<>();
                elements = coalesce(kv.getValue(), fanInBranches, rejected);
                for (final BadRecord record : rejectionRecords(rejected, failFast)) c.output(failureTag, record);
            } else {
                elements = new ArrayList<>();
                kv.getValue().forEach(elements::add);
            }
            try {
                prepare(c);
                final List<Map<String, Object>> rows = new ArrayList<>(elements.size());
                for (final MElement e : elements) rows.add(e.asPrimitiveMap());
                if (NULL_KEY.equals(kv.getKey())) {
                    for (final Map<String, Object> r : rows) evaluator.evaluateRow(r);
                } else {
                    evaluator.evaluateGroup(rows);
                }
                for (int i = 0; i < rows.size(); i++) {
                    final Instant ts = elements.get(i).getTimestamp();
                    c.outputWithTimestamp(MElement.of(rows.get(i), ts), ts);
                }
            } catch (final Throwable e) {
                for (final MElement element : elements) {
                    c.output(failureTag, Module.processError("Failed to evaluate context features", element, e, failFast));
                }
            }
        }
    }

    /**
     * Per-key ordered history replay (batch). The key's rows are gathered by GroupByKey, sorted by event
     * time and replayed in order with an in-memory history, so every row sees strictly-past rows only
     * (rows sharing a timestamp exclude each other). Beam state + {@code @RequiresTimeSortedInput} is not
     * used because upstream GroupByKey stages re-emit rows at their event time, which that annotation
     * treats as late data and drops; a stateful variant is the streaming follow-up (engine doc §6).
     */
    static class KeyedHistoryDoFn extends StageDoFn<KV<String, Iterable<KV<Long, MElement>>>> {

        private final KeyedSpillSorter sorter;
        private final String label;
        /**
         * A future stage: the rows arrive latest first and the evaluators see the mirrored clock {@code −t}, so the
         * strictly-past machinery (windows, decay, pending same-timestamp rows, trimming) reads the strictly-future
         * window {@code (t, t + maxAge]}; the output keeps the real event time.
         */
        private final boolean mirrored;
        /**
         * The declared {@code minInterval} (millis) per entity this stage audits ({@link Wiring#assignMinIntervalAudits} —
         * one stage per entity, keyed by the entity itself where possible), and the counter
         * {@code feature/minInterval_<entity>_below} of the rows that follow the key's previous event by less: the
         * compiler trusts the declaration for a {@code staticSafe} (DSL spec §6.2 tier 2), the replay is where the
         * actual gaps are seen. A gap of zero — rows sharing a timestamp, which never see each other — is not a
         * violation, but every row at the later timestamp of a short gap is counted.
         */
        private final Map<String, Long> minIntervals;
        private transient Map<String, Counter> belowMinInterval;
        /** The rating states of this stage that are snapshotted ({@link RatingSnapshot}): loaded before a key's replay, written after it. */
        private final List<RatingSnapshot.Spec> snapshots;
        /**
         * Drawn once when the pipeline is built, so every worker and every retried attempt of this run share it: with the
         * runtime job name it marks the snapshots this run writes, which a retried key must not continue from.
         */
        private final String constructionId;
        private transient Map<String, Counter> rowsBeforeSnapshot;

        KeyedHistoryDoFn(final StageEvaluator evaluator, final PCollectionView<Map<String, Double>> lambdas,
                         final List<Logging> loggings, final boolean failFast, final TupleTag<BadRecord> failureTag,
                         final KeyedSpillSorter sorter, final String label, final boolean mirrored,
                         final Map<String, Long> minIntervals, final List<RatingSnapshot.Spec> snapshots) {
            super(evaluator, lambdas, loggings, failFast, failureTag);
            this.sorter = sorter;
            this.label = label;
            this.mirrored = mirrored;
            this.minIntervals = mirrored ? Map.of() : minIntervals;
            this.snapshots = snapshots;
            this.constructionId = java.util.UUID.randomUUID().toString();
        }

        /**
         * This run's mark on the snapshots it writes ({@link RatingSnapshot}): the build-time id — unique per launch — and
         * the runtime job name, which tells apart the executions of a pipeline built once (a classic template).
         */
        private String run(final ProcessContext c) {
            return constructionId + "/" + c.getPipelineOptions().getJobName();
        }

        /** Starts the key's rating states from their snapshots where one exists (and no refit is asked); returns the specs so loaded. */
        private Set<RatingSnapshot.Spec> loadSnapshots(final SequenceEvaluator.KeyState sequenceState, final String key, final String run) {
            final Set<RatingSnapshot.Spec> loaded = new HashSet<>();
            for (final RatingSnapshot.Spec spec : snapshots) {
                if (spec.refit()) continue;
                final Rating.State state = RatingSnapshot.read(spec, key, run);
                if (state == null) continue;
                sequenceState.column(spec.stateKey()).bySubkey.put("", state);
                loaded.add(spec);
            }
            return loaded;
        }

        /** Writes the key's rating states that were replayed from scratch; reports the rows a loaded state served too early. */
        private void writeSnapshots(final SequenceEvaluator.KeyState sequenceState, final String key, final Set<RatingSnapshot.Spec> loaded, final String run) {
            for (final RatingSnapshot.Spec spec : snapshots) {
                final SequenceEvaluator.ColumnState cs = sequenceState.columns.get(spec.stateKey());
                if (cs == null || !(cs.bySubkey.get("") instanceof Rating.State state)) continue;
                if (state.rowsBeforeSnapshot > 0) {
                    if (rowsBeforeSnapshot == null) rowsBeforeSnapshot = new HashMap<>();
                    rowsBeforeSnapshot.computeIfAbsent(spec.stateKey(), k -> Metrics.counter("feature", "ratingSnapshot_" + k + "_rowsBefore")).inc(state.rowsBeforeSnapshot);
                    LOG.warn("rating snapshot of {} ({}): {} row(s) lie before the snapshot's last contest ({}) and read a state that already holds contests"
                            + " after them - start the input after the snapshot, or refit", spec.stateKey(), spillContext(label, key), state.rowsBeforeSnapshot, state.foldedUntilMillis);
                }
                if (!loaded.contains(spec)) RatingSnapshot.write(spec, key, state, run);
            }
        }

        private void auditInterval(final long millis, final long previousMillis) {
            if (minIntervals.isEmpty() || previousMillis == Long.MIN_VALUE) return;
            final long gap = millis - previousMillis;
            for (final Map.Entry<String, Long> e : minIntervals.entrySet()) {
                if (gap >= e.getValue()) continue;
                belowMinInterval.computeIfAbsent(e.getKey(), entity -> Metrics.counter("feature", "minInterval_" + entity + "_below")).inc();
            }
        }

        /** The replay clock of an event time: itself, or {@code −t} for a future stage (ascending in replay order). */
        private long clock(final long millis) {
            return mirrored ? -millis : millis;
        }

        /** "Stage3_sequence key=a|b" (key components joined by '|', abbreviated) for the spill log. */
        static String spillContext(final String label, final String key) {
            final String trimmed = !key.isEmpty() && key.charAt(key.length() - 1) == KEY_SEPARATOR ? key.substring(0, key.length() - 1) : key;
            final String shown = trimmed.isEmpty() ? "<global>" : trimmed.replace(KEY_SEPARATOR, '|');
            return label + " key=" + (shown.length() > 80 ? shown.substring(0, 77) + "..." : shown);
        }

        @Teardown
        public void teardown() {
            sorter.teardown();
        }

        @Override
        public org.joda.time.Duration getAllowedTimestampSkew() {
            return org.joda.time.Duration.millis(Long.MAX_VALUE);
        }

        @Setup
        @Override
        public void setup() {
            super.setup();
            belowMinInterval = new HashMap<>();
            try {
                sorter.setup();
            } catch (final IOException e) {
                throw new UncheckedIOException("failed to prepare the spill directory of the keyed stage", e);
            }
            final List<String> unbounded = evaluator.unboundedColumns();
            if (!unbounded.isEmpty()) {
                LOG.info("keyed stage keeps the inputs of these columns for the whole history of each key (no maxAge on scan-path columns): {}", unbounded);
            }
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final KV<String, Iterable<KV<Long, MElement>>> kv = c.element();
            if (kv == null) return;
            prepare(c);
            if (NULL_KEY.equals(kv.getKey())) {
                // rows with a null key bypass keyed evaluation: no order needed
                for (final KV<Long, MElement> row : kv.getValue()) evaluate(c, row.getValue(), null, null, null, null, true);
                return;
            }
            // sort the rows of the key by event time (in memory, or merged from spilled chunks that are deleted on
            // close) and stream them: only the trimmable projected history and the running statistics stay in memory
            final KeyedSpillSorter.Sorted sorted;
            try {
                sorted = sorter.sort(kv.getValue(), spillContext(label, kv.getKey()));
            } catch (final IOException | RuntimeException e) {
                failKey(c, kv.getValue(), "Failed to sort keyed rows", e);
                return;
            }
            try (sorted) {
                replay(c, sorted, kv.getKey());
            } catch (final UncheckedIOException e) {
                // a chunk could not be read back mid-merge: the rows already emitted stand, the key is failed
                // row by row like every other failure path (the grouped iterable is re-iterable)
                failKey(c, kv.getValue(), "Failed to read the spilled rows of a key", e);
            }
        }

        /** Routes every row of the key to the failure output (one BadRecord per input row). */
        private void failKey(final ProcessContext c, final Iterable<KV<Long, MElement>> rows, final String message, final Exception e) {
            for (final KV<Long, MElement> row : rows) {
                c.output(failureTag, Module.processError(message, row.getValue(), e, failFast));
            }
        }

        private void replay(final ProcessContext c, final Iterable<KV<Long, MElement>> rows, final String key) {
            final SequenceEvaluator.History history = new SequenceEvaluator.History();
            final SequenceEvaluator.KeyState sequenceState = new SequenceEvaluator.KeyState();
            final SequenceEvaluator.KeyState populationState = new SequenceEvaluator.KeyState();
            final String run = snapshots.isEmpty() ? null : run(c);
            final Set<RatingSnapshot.Spec> loaded = snapshots.isEmpty() ? Set.of() : loadSnapshots(sequenceState, key, run);
            // rows sharing a timestamp are not visible to each other: their (evaluated) projections join the
            // history only once the timestamp advances
            final List<Past> pending = new ArrayList<>();
            long pendingMillis = Long.MIN_VALUE, previousMillis = Long.MIN_VALUE;
            for (final KV<Long, MElement> row : rows) {
                final MElement input = row.getValue();
                final long millis = clock(input.getTimestamp().getMillis());
                if (millis != pendingMillis) {
                    history.addAll(pending);
                    pending.clear();
                    previousMillis = pendingMillis;
                    pendingMillis = millis;
                }
                // how far this row is from the key's previous event time is what minInterval declared
                auditInterval(millis, previousMillis);
                evaluate(c, input, history, sequenceState, populationState, pending, false);
            }
            if (!snapshots.isEmpty()) writeSnapshots(sequenceState, key, loaded, run);
        }

        private void evaluate(final ProcessContext c, final MElement input, final SequenceEvaluator.History history,
                              final SequenceEvaluator.KeyState sequenceState, final SequenceEvaluator.KeyState populationState,
                              final List<Past> pending, final boolean nullKey) {
            try {
                final Map<String, Object> values = input.asPrimitiveMap();
                final Instant now = input.getTimestamp();
                if (nullKey) {
                    evaluator.evaluateRow(values);
                    c.outputWithTimestamp(MElement.of(values, now), now);
                    return;
                }
                final long clock = clock(now.getMillis());
                evaluator.evaluateKeyed(values, clock, history, sequenceState, populationState);
                pending.add(new Past(clock, evaluator.project(values)));
                // absolute indices: the fold / evict pointers stay valid across trims; fields are dropped per
                // column window, so an unbounded column keeps only its own inputs for the whole history.
                // trimmed before the output so a trim failure does not double-route an already-emitted row
                history.trim(evaluator.watermarks(clock, history, sequenceState, populationState));
                c.outputWithTimestamp(MElement.of(values, now), now);
            } catch (final Throwable e) {
                c.output(failureTag, Module.processError("Failed to evaluate keyed features", input, e, failFast));
            }
        }

    }

    /** Builds the output value map from the canonical row map (shared by both finalize DoFns). */
    /**
     * Input fields that pass through to the output ({@code output.passThrough}): {@code all} (default),
     * {@code keys} (time.field, entity / context keys, groupBy keys, parentFields - what a consumer needs to
     * join or group) or {@code none}. Outcome-like inputs are not availability-checked, so a downstream
     * {@code SELECT *} over a full pass-through can pick up post-event columns; {@code keys} makes the
     * feature table safe to consume wholesale.
     */
    public static Set<String> passThroughInputs(final FeaturePlan plan, final Schema inputSchema) {
        final FeatureSpec spec = plan.getSpec();
        final Set<String> names = new LinkedHashSet<>();
        final String mode = spec.output.passThrough == null ? "all" : spec.output.passThrough;
        switch (mode) {
            case "none" -> { }
            case "keys" -> {
                if (spec.timeField != null) names.add(spec.timeField);
                for (final FeatureSpec.EntityDef e : spec.entities) names.addAll(e.keys());
                for (final FeatureSpec.ContextDef c : spec.contexts) names.addAll(c.keys());
                names.addAll(spec.orderTieBreak);
                names.addAll(spec.output.parentFields);
            }
            default -> { for (final Schema.Field f : inputSchema.getFields()) names.add(f.getName()); }
        }
        // a role names a column the consumer reads (group / time / label ...): an input field with a role is
        // always passed through, whatever the mode
        names.addAll(spec.output.roles.values());
        final Set<String> present = new LinkedHashSet<>();
        for (final Schema.Field f : inputSchema.getFields()) if (names.contains(f.getName())) present.add(f.getName());
        return present;
    }

    static class Finalizer implements Serializable {
        private final List<OutputColumn> emitted;
        private final List<String> inputNames;
        private final boolean fillZero;

        Finalizer(final List<OutputColumn> emitted, final Schema inputSchema, final FeatureSpec.NullPolicy nullPolicy, final Set<String> passThrough) {
            this.emitted = emitted;
            this.inputNames = inputSchema.getFields().stream().map(Schema.Field::getName).filter(passThrough::contains).toList();
            this.fillZero = nullPolicy == FeatureSpec.NullPolicy.fillZero;
        }

        /** Field names of the output schema (parent + child element fields for grouped output): the pass-through set. */
        static Set<String> outputFieldNames(final Schema outputSchema, final String childName) {
            final Set<String> names = new LinkedHashSet<>();
            for (final Schema.Field f : outputSchema.getFields()) {
                names.add(f.getName());
                if (childName != null && f.getName().equals(childName) && f.getFieldType().getArrayValueType() != null
                        && f.getFieldType().getArrayValueType().getElementSchema() != null) {
                    for (final Schema.Field child : f.getFieldType().getArrayValueType().getElementSchema().getFields()) names.add(child.getName());
                }
            }
            return names;
        }

        Map<String, Object> outputValues(final Map<String, Object> values, final Collection<String> names,
                                         final OutputColumn.Placement placement) {
            final Map<String, Object> out = new HashMap<>();
            for (final String name : names) out.put(name, values.get(name));
            for (final OutputColumn c : emitted) {
                if (placement != null && c.getPlacement() != placement) continue;
                Object v = values.get(c.getCanonicalName());
                if (v == null && fillZero && c.getFieldType() != null && OperatorCatalog.isNumeric(c.getFieldType())) {
                    v = FeatureValues.cast(0d, c.getFieldType());
                }
                out.put(c.getOutputName(), v);
            }
            return out;
        }
    }

    static class FinalizeDoFn extends DoFn<MElement, MElement> {
        private final Finalizer finalizer;
        private final Schema outputSchema;
        private final Map<String, Logging> logs;
        private final boolean failFast;
        private final TupleTag<BadRecord> failureTag;
        /** one sample per output row for the run manifest (null = not counted) */
        private final TupleTag<KV<String, Double>> countTag;

        FinalizeDoFn(final List<OutputColumn> emitted, final Schema inputSchema, final Schema outputSchema, final FeatureSpec.NullPolicy nullPolicy,
                     final List<Logging> loggings, final boolean failFast, final TupleTag<BadRecord> failureTag, final TupleTag<KV<String, Double>> countTag) {
            this.finalizer = new Finalizer(emitted, inputSchema, nullPolicy, Finalizer.outputFieldNames(outputSchema, null));
            this.outputSchema = outputSchema;
            this.logs = Logging.map(loggings);
            this.failFast = failFast;
            this.failureTag = failureTag;
            this.countTag = countTag;
        }

        @Setup
        public void setup() {
            outputSchema.setup();
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final MElement input = c.element();
            if (input == null) return;
            try {
                final Map<String, Object> out = finalizer.outputValues(input.asPrimitiveMap(), finalizer.inputNames, null);
                final MElement output = MElement.of(outputSchema, out, c.timestamp()).convert(outputSchema);
                c.output(output);
                if (countTag != null) c.output(countTag, KV.of(OUTPUT_COUNT_KEY, 1d));
                Logging.log(LOG, logs, "output", output);
            } catch (final Throwable e) {
                c.output(failureTag, Module.processError("Failed to finalize features", input, e, failFast));
            }
        }
    }

    static class GroupedFinalizeDoFn extends DoFn<KV<String, Iterable<MElement>>, MElement> {
        private final Finalizer finalizer;
        private final Schema outputSchema;
        private final List<String> keys;
        private final List<String> parentFields;
        private final String childName;
        /** Branch count of a fan-out wave folded into the finalize GroupByKey; 0 = the input carries plain rows. */
        private final int fanInBranches;
        private final Map<String, Logging> logs;
        private final boolean failFast;
        private final TupleTag<BadRecord> failureTag;
        private final TupleTag<KV<String, Double>> countTag;

        GroupedFinalizeDoFn(final List<OutputColumn> emitted, final Schema inputSchema, final Schema outputSchema, final FeatureSpec.NullPolicy nullPolicy,
                            final List<String> keys, final List<String> parentFields, final String childName, final int fanInBranches,
                            final List<Logging> loggings, final boolean failFast, final TupleTag<BadRecord> failureTag, final TupleTag<KV<String, Double>> countTag) {
            this.finalizer = new Finalizer(emitted, inputSchema, nullPolicy, Finalizer.outputFieldNames(outputSchema, childName));
            this.outputSchema = outputSchema;
            this.keys = keys;
            this.parentFields = parentFields;
            this.childName = childName;
            this.fanInBranches = fanInBranches;
            this.logs = Logging.map(loggings);
            this.failFast = failFast;
            this.failureTag = failureTag;
            this.countTag = countTag;
        }

        @Setup
        public void setup() {
            outputSchema.setup();
        }

        @Override
        public org.joda.time.Duration getAllowedTimestampSkew() {
            return org.joda.time.Duration.millis(Long.MAX_VALUE);
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final KV<String, Iterable<MElement>> kv = c.element();
            if (kv == null) return;
            // a folded fan-out merge delivers base and partial rows: reassemble them by row id first
            // (a plain grouped finalize skips the reassembly — and its per-element probes — entirely)
            final List<MElement> elements;
            if (fanInBranches > 0) {
                final List<Rejection> rejected = new ArrayList<>();
                elements = coalesce(kv.getValue(), fanInBranches, rejected);
                for (final BadRecord record : rejectionRecords(rejected, failFast)) c.output(failureTag, record);
            } else {
                elements = new ArrayList<>();
                kv.getValue().forEach(elements::add);
            }
            if (elements.isEmpty()) return;
            if (NULL_KEY.equals(kv.getKey())) {
                // rows with a null groupBy key cannot be grouped: each becomes its own single-child record
                for (final MElement element : elements) {
                    emit(c, List.of(element));
                }
                return;
            }
            emit(c, elements);
        }

        private void emit(final ProcessContext c, final List<MElement> elements) {
            try {
                final Set<String> parentInputs = new LinkedHashSet<>(keys);
                parentInputs.addAll(parentFields);
                final List<String> childInputs = finalizer.inputNames.stream().filter(n -> !parentInputs.contains(n)).toList();
                final Map<String, Object> first = elements.get(0).asPrimitiveMap();
                final Map<String, Object> parent = finalizer.outputValues(first, parentInputs, OutputColumn.Placement.parent);
                final List<Map<String, Object>> children = new ArrayList<>();
                Instant ts = elements.get(0).getTimestamp();
                for (final MElement e : elements) {
                    children.add(finalizer.outputValues(e.asPrimitiveMap(), childInputs, OutputColumn.Placement.child));
                    if (e.getTimestamp().isAfter(ts)) ts = e.getTimestamp();
                }
                parent.put(childName, children);
                final MElement output = MElement.of(outputSchema, parent, ts).convert(outputSchema);
                c.outputWithTimestamp(output, ts);
                if (countTag != null) c.outputWithTimestamp(countTag, KV.of(OUTPUT_COUNT_KEY, 1d), ts);
                Logging.log(LOG, logs, "output", output);
            } catch (final Throwable e) {
                for (final MElement element : elements) {
                    c.output(failureTag, Module.processError("Failed to finalize grouped features", element, e, failFast));
                }
            }
        }
    }

}
