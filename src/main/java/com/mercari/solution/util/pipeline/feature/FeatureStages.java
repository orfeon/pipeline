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

    /** Artifact paths per fitted block for the manifest (static / fold encodings, factorization, discretize) — declared or not yet written. */
    public static Map<String, String> artifactPaths(final FeaturePlan plan) {
        final Map<String, String> paths = new LinkedHashMap<>();
        final String version = plan.getArtifactVersion();
        for (final FitLevel level : fitLevels(plan.getColumns())) {
            if (level.artifactUri() != null) paths.put(level.block(), FitArtifact.statsPath(level.artifactUri(), version, level.block()));
        }
        for (final StaticFitBlock<?> block : fmSpecs(plan.getColumns())) {
            if (block.artifactUri() != null) paths.put(block.block(), block.artifactPath(version));
        }
        for (final StaticFitBlock<?> block : discretizeSpecs(plan.getColumns())) {
            if (block.artifactUri() != null) paths.put(block.block(), block.artifactPath(version));
        }
        return paths;
    }

    /** The shared objects of the stage wiring: one stage = one ParDo / GroupByKey, whatever wave it runs in. */
    private static final class Wiring {
        private final FeaturePlan plan;
        private final Map<String, OutputColumn> columns;
        private final Coder<MElement> elementCoder;
        private final KvCoder<String, MElement> kvCoder;
        private final KvCoder<String, KV<Long, MElement>> sortKvCoder;
        private final KeyedSpillSorter sorter;
        private final List<Logging> loggings;
        private final boolean failFast;
        private final List<PCollection<BadRecord>> failures;

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
        }

        static String label(final Stage stage) {
            return "Stage" + stage.index() + "_" + stage.kind();
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
                case sequence, population -> current
                        .apply(label + "_Key", ParDo.of(new SortKeyDoFn(stage.keys()))).setCoder(sortKvCoder)
                        .apply(label + "_Group", GroupByKey.create())
                        .apply(label, ParDo
                                .of(new KeyedHistoryDoFn(evaluator, lambdas, loggings, failFast, failureTag, sorter, label))
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

    /** One fitted lattice level of a static block: hidden columns it fills and how its statistics are keyed. */
    record FitLevel(String block, String id, String sumColumn, String sumSqColumn, List<String> keys, String field,
                    String offsetColumn, String artifactUri, boolean refit, List<String> foldKeys, int folds,
                    Forward forward) implements Serializable {
        VarianceComponents.LevelSpec spec() {
            return new VarianceComponents.LevelSpec(id, keys, field, offsetColumn, foldKeys, folds);
        }
        /** fit.mode fold: out-of-fold statistics, always fitted in-pipeline (an artifact only holds the totals). */
        boolean isFold() {
            return foldKeys != null;
        }
        /** fit.mode forward: per-block statistics, always fitted in-pipeline (an artifact only holds the totals). */
        boolean isForward() {
            return forward != null;
        }
        VarianceComponents.ForwardSpec forwardSpec() {
            return new VarianceComponents.ForwardSpec(id, keys, field, offsetColumn, forward.blocks(), forward.blockField(), forward.blockFieldType());
        }
    }

    /** fit.mode forward geometry of a level (from the column coordinates, see FeaturePlanCompiler.forwardCoordinates). */
    record Forward(ForwardBlocks blocks, int minBlocks, long lagMillis, int windowBlocks, String blockField, String blockFieldType) implements Serializable {
        static Forward of(final Map<String, String> coordinates) {
            if (!"forward".equals(coordinates.get("fit"))) return null;
            return new Forward(ForwardBlocks.fromCoordinates(coordinates.get("blockBucket"), coordinates.get("blockSizeMillis")),
                    Integer.parseInt(coordinates.getOrDefault("minBlocks", "1")),
                    Long.parseLong(coordinates.getOrDefault("forwardLagMillis", "0")),
                    Integer.parseInt(coordinates.getOrDefault("windowBlocks", "0")),
                    coordinates.get("blockField"), coordinates.getOrDefault("blockFieldType", "timestamp"));
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
                    keys.isEmpty() ? List.of() : List.of(keys.split(",")),
                    c.getCoordinates().get("field"), offset,
                    c.getCoordinates().get("artifactUri"), "true".equals(c.getCoordinates().get("refit")),
                    foldKeys != null ? List.of(foldKeys.split(",")) : null,
                    foldKeys != null ? Integer.parseInt(c.getCoordinates().get("folds")) : 0,
                    Forward.of(c.getCoordinates())));
        }
        return new ArrayList<>(levels.values());
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
        final Map<String, String> writeBlocks = new LinkedHashMap<>();
        final Map<String, String> writeForwardBlocks = new LinkedHashMap<>();
        for (final FitLevel level : levels) {
            final String uri = level.artifactUri();
            // fold / forward levels are always fitted: their per-fold / per-block parts cannot come from an artifact (which holds totals)
            final boolean exists = uri != null && !level.refit() && FitArtifact.exists(uri, planHash, level.block());
            if (exists && !level.isFold() && !level.isForward()) {
                loadBlocks.put(level.block(), uri);
                continue;
            }
            (level.isForward() ? forward : fitted).add(level);
            // fold / forward levels are re-fitted every run but respect refit: false for the (totals) artifact
            if (uri != null && !exists) (level.isForward() ? writeForwardBlocks : writeBlocks).put(level.block(), uri);
        }
        for (final Map.Entry<String, String> e : loadBlocks.entrySet()) {
            LOG.info("feature fit: block {} loads artifact {}", e.getKey(), FitArtifact.statsPath(e.getValue(), planHash, e.getKey()));
        }
        if ((!fitted.isEmpty() || !forward.isEmpty()) && com.mercari.solution.util.pipeline.OptionUtil.isStreaming(input)) {
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
        if (!fitted.isEmpty()) {
            final List<VarianceComponents.LevelSpec> specs = new ArrayList<>();
            for (final FitLevel level : fitted) specs.add(level.spec());
            final PCollection<KV<String, VarianceComponents.KeyStats>> perKey = VarianceComponents.perKeyStats(fitInput, specs, label + "_Fit");
            statsView = perKey.apply(label + "_StatsView", View.asMap());
            sideInputs.add(statsView);
            if (evaluator.row.needsVarianceComponents()) {
                lambdasView = VarianceComponents.lambdasFromKeyStats(perKey, label + "_Vc");
                sideInputs.add(lambdasView);
            }
            for (final Map.Entry<String, String> e : writeBlocks.entrySet()) {
                final List<String> blockLevels = new ArrayList<>();
                for (final FitLevel level : fitted) if (level.block().equals(e.getKey())) blockLevels.add(level.id());
                input.getPipeline()
                        .apply(label + "_Write_" + e.getKey() + "_Trigger", Create.of(e.getKey()))
                        .apply(label + "_Write_" + e.getKey(), ParDo
                                .of(new WriteArtifactDoFn(e.getValue(), planHash, e.getKey(), blockLevels, statsView, null))
                                .withSideInputs(statsView));
            }
        }
        // fit.mode forward: cumulative per-block statistics per (level, key) — a parallel Combine, no time-ordered replay
        PCollectionView<Map<String, ForwardBlocks.Series>> seriesView = null;
        if (!forward.isEmpty()) {
            final List<VarianceComponents.ForwardSpec> specs = new ArrayList<>();
            for (final FitLevel level : forward) specs.add(level.forwardSpec());
            seriesView = VarianceComponents.forwardSeries(fitInput, specs, label + "_Forward").apply(label + "_ForwardView", View.asMap());
            sideInputs.add(seriesView);
            for (final Map.Entry<String, String> e : writeForwardBlocks.entrySet()) {
                final List<String> blockLevels = new ArrayList<>();
                for (final FitLevel level : forward) if (level.block().equals(e.getKey())) blockLevels.add(level.id());
                input.getPipeline()
                        .apply(label + "_Write_" + e.getKey() + "_Trigger", Create.of(e.getKey()))
                        .apply(label + "_Write_" + e.getKey(), ParDo
                                .of(new WriteArtifactDoFn(e.getValue(), planHash, e.getKey(), blockLevels, null, seriesView))
                                .withSideInputs(seriesView));
            }
        }
        // static-fit blocks (factorization / discretize): fitted on one worker over the whole input, or loaded
        // from their artifact, and applied per row through a side input
        final List<StaticFitBlock<?>> blocks = new ArrayList<>();
        blocks.addAll(fmSpecs(stageColumns));
        blocks.addAll(discretizeSpecs(stageColumns));
        final Map<String, PCollectionView<?>> blockViews = new LinkedHashMap<>();
        final Set<String> blockLoad = new LinkedHashSet<>();
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
            final PCollectionView<?> view = block.fit(fitInput, label, planHash);
            blockViews.put(block.block(), view);
            sideInputs.add(view);
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
                .of(new FitApplyDoFn(evaluator, levels, statsView, lambdasView, seriesView, predictOffsetMillis, loadBlocks, planHash,
                        blocks, blockViews, blockLoad, loggings, failFast, failureTag))
                .withSideInputs(sideInputs)
                .withOutputTags(outputTag, TupleTagList.of(failureTag)));
    }

    /**
     * A population block fitted once on the whole input and applied by lookup ({@code fit.mode: static}
     * outside the encoding lattice): its model {@code M} is either fitted in the fit stage (one side input
     * per block) or loaded from its artifact on the worker. Implementations rebuild themselves from their
     * output columns' coordinates ({@link #fmSpecs}, {@link #discretizeSpecs}).
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

    static class WriteArtifactDoFn extends DoFn<String, Void> {
        private final String uri;
        private final String planHash;
        private final String block;
        private final List<String> levels;
        private final PCollectionView<Map<String, VarianceComponents.KeyStats>> statsView;
        /** fit.mode forward: the totals come from the series, and the manifest records λ per block */
        private final PCollectionView<Map<String, ForwardBlocks.Series>> seriesView;

        WriteArtifactDoFn(final String uri, final String planHash, final String block, final List<String> levels,
                          final PCollectionView<Map<String, VarianceComponents.KeyStats>> statsView,
                          final PCollectionView<Map<String, ForwardBlocks.Series>> seriesView) {
            this.uri = uri;
            this.planHash = planHash;
            this.block = block;
            this.levels = levels;
            this.statsView = statsView;
            this.seriesView = seriesView;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final Map<String, VarianceComponents.KeyStats> blockStats = new HashMap<>();
            com.google.gson.JsonObject extra = null;
            if (seriesView != null) {
                final Map<String, ForwardBlocks.Series> all = c.sideInput(seriesView);
                final Map<String, ForwardBlocks.Series> mine = new HashMap<>();
                for (final Map.Entry<String, ForwardBlocks.Series> e : all.entrySet()) {
                    if (levels.contains(FitArtifact.levelOf(e.getKey()))) mine.put(e.getKey(), e.getValue());
                }
                blockStats.putAll(VarianceComponents.forwardTotals(mine));
                extra = new com.google.gson.JsonObject();
                final com.google.gson.JsonObject byBlock = new com.google.gson.JsonObject();
                for (final Map.Entry<String, TreeMap<Long, Double>> e : VarianceComponents.lambdasByBlock(mine).entrySet()) {
                    final com.google.gson.JsonObject perBlock = new com.google.gson.JsonObject();
                    for (final Map.Entry<Long, Double> b : e.getValue().entrySet()) perBlock.addProperty(Long.toString(b.getKey()), b.getValue());
                    byBlock.add(e.getKey(), perBlock);
                }
                extra.add("lambdasByBlock", byBlock);
            } else {
                final Map<String, VarianceComponents.KeyStats> all = c.sideInput(statsView);
                for (final Map.Entry<String, VarianceComponents.KeyStats> e : all.entrySet()) {
                    if (levels.contains(FitArtifact.levelOf(e.getKey()))) blockStats.put(e.getKey(), e.getValue());
                }
            }
            FitArtifact.write(uri, planHash, block, blockStats, levels, extra);
        }
    }

    static class FitApplyDoFn extends StageDoFn<MElement> {
        /** Loaded artifacts per path, for the JVM lifetime (content-addressed paths: see FitArtifact). */
        private static final Map<String, Map<String, VarianceComponents.KeyStats>> ARTIFACT_CACHE = new java.util.concurrent.ConcurrentHashMap<>();
        private static final Map<String, Object> MODEL_CACHE = new java.util.concurrent.ConcurrentHashMap<>();

        private final List<FitLevel> levels;
        private final PCollectionView<Map<String, VarianceComponents.KeyStats>> statsView;
        /** fit.mode forward: cumulative per-block statistics per (level, key) */
        private final PCollectionView<Map<String, ForwardBlocks.Series>> seriesView;
        private final long predictOffsetMillis;
        private final Map<String, String> loadBlocks;
        private final String planHash;
        private final List<StaticFitBlock<?>> blocks;
        private final Map<String, PCollectionView<?>> blockViews;
        private final Set<String> blockLoad;
        private transient Map<String, VarianceComponents.KeyStats> loaded;
        private transient Map<String, Double> loadedLambdas;
        private transient Map<String, Object> loadedModels;
        /** fit.mode forward: λ per level per block, derived once from the series side input (immutable in a batch run) */
        private transient Map<String, TreeMap<Long, Double>> forwardLambdas;

        FitApplyDoFn(final StageEvaluator evaluator, final List<FitLevel> levels,
                     final PCollectionView<Map<String, VarianceComponents.KeyStats>> statsView,
                     final PCollectionView<Map<String, Double>> lambdas,
                     final PCollectionView<Map<String, ForwardBlocks.Series>> seriesView, final long predictOffsetMillis,
                     final Map<String, String> loadBlocks, final String planHash,
                     final List<StaticFitBlock<?>> blocks, final Map<String, PCollectionView<?>> blockViews, final Set<String> blockLoad,
                     final List<Logging> loggings, final boolean failFast, final TupleTag<BadRecord> failureTag) {
            super(evaluator, lambdas, loggings, failFast, failureTag);
            this.levels = levels;
            this.statsView = statsView;
            this.seriesView = seriesView;
            this.predictOffsetMillis = predictOffsetMillis;
            this.loadBlocks = loadBlocks;
            this.planHash = planHash;
            this.blocks = blocks;
            this.blockViews = blockViews;
            this.blockLoad = blockLoad;
        }

        @Setup
        public void setup() {
            super.setup();
            loaded = new HashMap<>();
            for (final Map.Entry<String, String> e : loadBlocks.entrySet()) {
                final String path = FitArtifact.statsPath(e.getValue(), planHash, e.getKey());
                loaded.putAll(ARTIFACT_CACHE.computeIfAbsent(path, p -> FitArtifact.read(e.getValue(), planHash, e.getKey())));
            }
            loadedLambdas = loaded.isEmpty() ? Map.of() : VarianceComponents.lambdasInMemory(loaded);
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
            final Map<String, Double> merged = new HashMap<>(loadedLambdas);
            if (lambdas != null) merged.putAll(c.sideInput(lambdas));
            evaluator.setLambdas(merged);
        }

        /** fit.mode forward: the row's statistics = the series up to its usable block (minus the window's older blocks). */
        private VarianceComponents.KeyStats forwardStats(final FitLevel level, final Map<String, ForwardBlocks.Series> series,
                                                          final String entry, final long eventMillis, final Map<String, Double> rowLambdas) {
            final Forward f = level.forward();
            final ForwardBlocks.Series s = series.get(entry);
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

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final MElement input = c.element();
            if (input == null) return;
            try {
                prepare(c);
                final Map<String, VarianceComponents.KeyStats> fitted = statsView == null ? Map.of() : c.sideInput(statsView);
                final Map<String, ForwardBlocks.Series> series = seriesView == null ? Map.of() : c.sideInput(seriesView);
                if (seriesView != null && forwardLambdas == null && evaluator.row.needsVarianceComponents()) {
                    forwardLambdas = VarianceComponents.lambdasByBlock(series);
                }
                final Map<String, Object> values = input.asPrimitiveMap();
                Map<String, Double> rowLambdas = null;
                for (final FitLevel level : levels) {
                    final String key = FeatureValues.key(values, level.keys());
                    VarianceComponents.KeyStats stats = null;
                    if (key != null && level.isForward()) {
                        final Long eventMillis = FeatureValues.toEpochMillis(values.get(level.forward().blockField()), level.forward().blockFieldType());
                        if (eventMillis != null) {
                            if (forwardLambdas != null && rowLambdas == null) rowLambdas = new HashMap<>(evaluator.row.lambdas());
                            stats = forwardStats(level, series, FitArtifact.entryKey(level.id(), key), eventMillis, rowLambdas);
                        }
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

        SortKeyDoFn(final List<String> keys) {
            this.keys = keys;
        }

        /** The sort key: the epoch millis themselves (compared as a signed long by {@link KeyedSpillSorter}). */
        static long sortable(final long millis) {
            return millis;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final MElement element = c.element();
            if (element == null) return;
            final KV<Long, MElement> value = KV.of(sortable(element.getEpochMillis()), element);
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

        KeyedHistoryDoFn(final StageEvaluator evaluator, final PCollectionView<Map<String, Double>> lambdas,
                         final List<Logging> loggings, final boolean failFast, final TupleTag<BadRecord> failureTag,
                         final KeyedSpillSorter sorter, final String label) {
            super(evaluator, lambdas, loggings, failFast, failureTag);
            this.sorter = sorter;
            this.label = label;
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
                replay(c, sorted);
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

        private void replay(final ProcessContext c, final Iterable<KV<Long, MElement>> rows) {
            final SequenceEvaluator.History history = new SequenceEvaluator.History();
            final SequenceEvaluator.KeyState sequenceState = new SequenceEvaluator.KeyState();
            final SequenceEvaluator.KeyState populationState = new SequenceEvaluator.KeyState();
            // rows sharing a timestamp are not visible to each other: their (evaluated) projections join the
            // history only once the timestamp advances
            final List<Past> pending = new ArrayList<>();
            long pendingMillis = Long.MIN_VALUE;
            for (final KV<Long, MElement> row : rows) {
                final MElement input = row.getValue();
                final long millis = input.getTimestamp().getMillis();
                if (millis != pendingMillis) {
                    history.addAll(pending);
                    pending.clear();
                    pendingMillis = millis;
                }
                evaluate(c, input, history, sequenceState, populationState, pending, false);
            }
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
                evaluator.evaluateKeyed(values, now.getMillis(), history, sequenceState, populationState);
                pending.add(new Past(now.getMillis(), evaluator.project(values)));
                // absolute indices: the fold / evict pointers stay valid across trims; fields are dropped per
                // column window, so an unbounded column keeps only its own inputs for the whole history.
                // trimmed before the output so a trim failure does not double-route an already-emitted row
                history.trim(evaluator.watermarks(now.getMillis(), history, sequenceState, populationState));
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
