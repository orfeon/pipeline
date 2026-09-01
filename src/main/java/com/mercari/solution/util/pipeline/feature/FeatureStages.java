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
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
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
 * Beam wiring of a {@link FeaturePlan} (work-feature-engine-beam.md §3, §9.4): one ParDo / GroupByKey per
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
        final List<List<Stage>> waves = new ArrayList<>();
        for (final List<Integer> wave : plan.getWaves()) {
            final List<Stage> stages = new ArrayList<>();
            for (final int i : wave) if (plan.getStages().get(i).kind() != StageKind.groupBy) stages.add(plan.getStages().get(i));
            if (!stages.isEmpty()) waves.add(stages);
        }
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
        final SourceContract.FieldContract timeContract = plan.getInputFields().get(spec.timeField);
        final String timeFieldType = timeContract == null || timeContract.getType() == null ? "timestamp" : timeContract.getType().getType().name();
        final PCollectionTuple elements = input.apply("ToElement", ParDo
                .of(new ToElementDoFn(spec.timeField, timeFieldType, parallel ? spec.engine.rowId : null, failFast, elementFailureTag))
                .withOutputTags(elementTag, TupleTagList.of(elementFailureTag)));
        failures.add(elements.get(elementFailureTag));
        PCollection<MElement> current = elements.get(elementTag).setCoder(elementCoder);
        if (parallel && spec.engine.rowId.isEmpty() && waves.get(0).size() >= 2) {
            // random row ids are pinned before the first fan-out: every branch must see the id a retry may recompute
            current = current.apply("RowId_Pin", Reshuffle.viaRandomKey());
        }

        final Wiring wiring = new Wiring(plan, columns, inputSchema, waves, elementCoder, kvCoder, sortKvCoder, sorter, loggings, failFast, failures);
        PCollection<MElement> pending = null; // base + partials of the last wave, merged inside the groupBy finalize
        if (!parallel) {
            for (final Stage stage : plan.getStages()) {
                if (stage.kind() != StageKind.groupBy) current = wiring.applyStage(current, stage);
            }
        } else {
            for (int w = 0; w < waves.size(); w++) {
                final List<Stage> wave = waves.get(w);
                if (wave.size() == 1) {
                    current = wiring.applyStage(current, wave.get(0));
                    continue;
                }
                // the row columns the wave's stages host are placed in their first consumer's stage by the scheduler
                // and carried on by the linear chain; the other branches read the wave input, so every row column
                // computable from it (input fields, earlier waves, such row columns) is evaluated on it first
                current = wiring.applyRows(current, "Wave" + (w + 1) + "_Rows", w);
                // fan-out: every branch reads the wave input and emits only its own columns (+ row id + merge key)
                final Stage foldInto = w + 1 < waves.size() && waves.get(w + 1).size() == 1 ? wiring.foldTarget(waves.get(w + 1).get(0), w) : null;
                final boolean foldGroupBy = foldInto == null && w + 1 == waves.size() && groupBy != null && wiring.keysAvailable(groupBy.keys(), w);
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
                    current = wiring.applyStage(PCollectionList.of(pieces).apply(name + "_FanIn", Flatten.pCollections()), foldInto, current);
                    w++;
                } else if (foldGroupBy) {
                    pending = PCollectionList.of(pieces).apply(name + "_FanIn", Flatten.pCollections());
                } else {
                    current = wiring.merge(name + "_Merge", pieces);
                }
            }
        }

        final TupleTag<MElement> outputTag = new TupleTag<>() {};
        final TupleTag<BadRecord> failureTag = new TupleTag<>() {};
        final PCollectionTuple finalized;
        if (groupBy == null) {
            finalized = current.apply("Finalize", ParDo
                    .of(new FinalizeDoFn(plan.getEmittedColumns(), inputSchema, outputSchema, spec.output.nullPolicy, loggings, failFast, failureTag))
                    .withOutputTags(outputTag, TupleTagList.of(failureTag)));
        } else {
            finalized = (pending != null ? pending : current)
                    .apply("Finalize_Key", ParDo.of(new KeyDoFn(groupBy.keys()))).setCoder(kvCoder)
                    .apply("Finalize_Group", GroupByKey.create())
                    .apply("Finalize", ParDo
                            .of(new GroupedFinalizeDoFn(plan.getEmittedColumns(), inputSchema, outputSchema, spec.output.nullPolicy,
                                    groupBy.keys(), spec.output.parentFields, spec.output.childName, loggings, failFast, failureTag))
                            .withOutputTags(outputTag, TupleTagList.of(failureTag)));
        }
        failures.add(finalized.get(failureTag));
        return new Result(finalized.get(outputTag).setCoder(ElementCoder.of(outputSchema)), failures);
    }

    /** The shared objects of the stage wiring: one stage = one ParDo / GroupByKey, whatever wave it runs in. */
    private static final class Wiring {
        private final FeaturePlan plan;
        private final Map<String, OutputColumn> columns;
        private final Set<String> inputNames = new HashSet<>();
        /** wave index of every column (by the stage it is evaluated in) */
        private final Map<String, Integer> waveOfColumn = new HashMap<>();
        private final Coder<MElement> elementCoder;
        private final KvCoder<String, MElement> kvCoder;
        private final KvCoder<String, KV<Long, MElement>> sortKvCoder;
        private final KeyedSpillSorter sorter;
        private final List<Logging> loggings;
        private final boolean failFast;
        private final List<PCollection<BadRecord>> failures;

        Wiring(final FeaturePlan plan, final Map<String, OutputColumn> columns, final Schema inputSchema, final List<List<Stage>> waves,
               final Coder<MElement> elementCoder, final KvCoder<String, MElement> kvCoder, final KvCoder<String, KV<Long, MElement>> sortKvCoder,
               final KeyedSpillSorter sorter, final List<Logging> loggings, final boolean failFast, final List<PCollection<BadRecord>> failures) {
            this.plan = plan;
            this.columns = columns;
            for (final Schema.Field f : inputSchema.getFields()) inputNames.add(f.getName());
            for (int w = 0; w < waves.size(); w++) {
                for (final Stage stage : waves.get(w)) for (final String name : stage.columnNames()) waveOfColumn.put(name, w);
            }
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

        /**
         * The fields every branch of wave {@code w} reads from its input: input fields, columns of earlier waves and
         * the wave's prelude row columns ({@link #preludeColumns}).
         */
        Set<String> waveInputFields(final int w) {
            final Set<String> fields = new HashSet<>(inputNames);
            for (final Map.Entry<String, Integer> e : waveOfColumn.entrySet()) if (e.getValue() < w) fields.add(e.getKey());
            for (final OutputColumn c : preludeColumns(w)) fields.add(c.getCanonicalName());
            return fields;
        }

        /** Every key is on the wave input (input field, earlier wave, prelude row column): the base rows carry it. */
        boolean keysAvailable(final List<String> keys, final int w) {
            return waveInputFields(w).containsAll(keys);
        }

        /**
         * The next stage when the merge of wave {@code w} can ride its GroupByKey: a single context stage whose key
         * the base rows already carry. Its variance-components estimate, if any, is taken over the wave input (the
         * flattened pieces would count the partial rows too), so the fields it reads must be on the wave input.
         */
        Stage foldTarget(final Stage next, final int w) {
            if (next.kind() != StageKind.context || !keysAvailable(next.keys(), w)) return null;
            final List<OutputColumn> stageColumns = new ArrayList<>();
            for (final String name : next.columnNames()) stageColumns.add(columns.get(name));
            final Set<String> available = waveInputFields(w);
            for (final VarianceComponents.LevelSpec spec : VarianceComponents.specsOf(stageColumns, columns)) {
                if (!available.containsAll(spec.keys())) return null;
                if (spec.field() != null && !available.contains(spec.field())) return null;
                if (spec.offsetColumn() != null && !available.contains(spec.offsetColumn())) return null;
                if (spec.foldKeys() != null && !available.containsAll(spec.foldKeys())) return null;
            }
            return next;
        }

        PCollection<MElement> applyStage(final PCollection<MElement> current, final Stage stage) {
            return applyStage(current, stage, current);
        }

        /** One stage on {@code current}; a variance-components estimate of its columns is computed over {@code estimateInput}. */
        PCollection<MElement> applyStage(final PCollection<MElement> current, final Stage stage, final PCollection<MElement> estimateInput) {
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
                                .of(new ContextStageDoFn(evaluator, lambdas, loggings, failFast, failureTag))
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
                case fit -> applyFit(current, stageColumns, evaluator, plan.getArtifactVersion(), label, loggings, failFast, outputTag, failureTag);
                default -> throw new IllegalStateException("unexpected stage kind: " + stage.kind());
            };
            failures.add(outputs.get(failureTag));
            return outputs.get(outputTag).setCoder(elementCoder);
        }

        /**
         * The row columns hosted by the stages of wave {@code w} that the wave input can evaluate (their inputs are
         * input fields, columns of earlier waves or such row columns), in expansion order (dependencies first).
         */
        List<OutputColumn> preludeColumns(final int w) {
            final Set<String> available = new HashSet<>(inputNames);
            for (final Map.Entry<String, Integer> e : waveOfColumn.entrySet()) if (e.getValue() < w) available.add(e.getKey());
            final List<OutputColumn> prelude = new ArrayList<>();
            for (final OutputColumn c : plan.getColumns()) {
                final Integer at = waveOfColumn.get(c.getCanonicalName());
                if (at == null || at != w || !FeaturePlanCompiler.isRowColumn(c)) continue;
                if (available.containsAll(c.getInputs())) {
                    prelude.add(c);
                    available.add(c.getCanonicalName());
                }
            }
            return prelude;
        }

        /** Evaluates {@link #preludeColumns} on the wave input before its fan-out (no-op when there are none). */
        PCollection<MElement> applyRows(final PCollection<MElement> current, final String name, final int w) {
            final List<OutputColumn> prelude = preludeColumns(w);
            if (prelude.isEmpty()) return current;
            final TupleTag<MElement> outputTag = outputTag();
            final TupleTag<BadRecord> failureTag = failureTag();
            final PCollectionTuple outputs = current.apply(name, ParDo
                    .of(new RowStageDoFn(new StageEvaluator(prelude), null, loggings, failFast, failureTag))
                    .withOutputTags(outputTag, TupleTagList.of(failureTag)));
            failures.add(outputs.get(failureTag));
            return outputs.get(outputTag).setCoder(elementCoder);
        }

        /** Row-id merge of a wave: the base rows and every branch's partial rows, grouped by row id and reassembled. */
        PCollection<MElement> merge(final String name, final List<PCollection<MElement>> pieces) {
            final List<PCollection<KV<String, MElement>>> keyed = new ArrayList<>();
            for (int i = 0; i < pieces.size(); i++) {
                keyed.add(pieces.get(i).apply(name + "_Key" + i, ParDo.of(new RowIdKeyDoFn())).setCoder(kvCoder));
            }
            final TupleTag<MElement> outputTag = outputTag();
            final TupleTag<BadRecord> failureTag = failureTag();
            final PCollectionTuple outputs = PCollectionList.of(keyed)
                    .apply(name + "_Flatten", Flatten.pCollections())
                    .apply(name + "_Group", GroupByKey.create())
                    .apply(name, ParDo.of(new MergeDoFn(failFast, failureTag)).withOutputTags(outputTag, TupleTagList.of(failureTag)));
            failures.add(outputs.get(failureTag));
            return outputs.get(outputTag).setCoder(elementCoder);
        }
    }

    /**
     * Reassembles the rows of a group from base rows and partial rows sharing a row id (the fan-out merge):
     * a base row takes the columns of its partials; rows without a row id (no fan-out) pass as they are.
     * A row id with two base rows ({@code engine.rowId} not unique) and a partial without a base are rejected.
     */
    static List<MElement> coalesce(final Iterable<MElement> pieces, final List<MElement> rejected) {
        final List<MElement> rows = new ArrayList<>();
        final Map<String, Map<String, Object>> bases = new LinkedHashMap<>();
        final Map<String, Instant> timestamps = new HashMap<>();
        final Map<String, List<MElement>> partials = new HashMap<>();
        for (final MElement e : pieces) {
            final Map<String, Object> map = e.asPrimitiveMap();
            final Object id = map == null ? null : map.get(ROW_ID_FIELD);
            if (id == null) {
                rows.add(e);
                continue;
            }
            if (map.containsKey(PARTIAL_FIELD)) {
                partials.computeIfAbsent(id.toString(), k -> new ArrayList<>()).add(e);
                continue;
            }
            if (bases.putIfAbsent(id.toString(), map) != null) {
                rejected.add(e);
                continue;
            }
            timestamps.put(id.toString(), e.getTimestamp());
        }
        for (final Map.Entry<String, Map<String, Object>> base : bases.entrySet()) {
            final Map<String, Object> row = base.getValue();
            final List<MElement> parts = partials.remove(base.getKey());
            if (parts != null) {
                for (final MElement part : parts) {
                    for (final Map.Entry<String, Object> v : part.asPrimitiveMap().entrySet()) {
                        if (!PARTIAL_FIELD.equals(v.getKey())) row.put(v.getKey(), v.getValue());
                    }
                }
            }
            rows.add(MElement.of(row, timestamps.get(base.getKey())));
        }
        for (final List<MElement> orphans : partials.values()) rejected.addAll(orphans);
        return rows;
    }

    /** The message of a rejected merge piece. */
    static String rejectionMessage(final MElement piece) {
        final Map<String, Object> map = piece.asPrimitiveMap();
        return map != null && map.containsKey(PARTIAL_FIELD)
                ? "Fan-out merge: a partial row has no base row (row id " + map.get(ROW_ID_FIELD) + ")"
                : "Fan-out merge: engine.rowId is not unique (row id " + (map == null ? null : map.get(ROW_ID_FIELD)) + ")";
    }

    /** Restrictions of the Beam engine that the compiler does not impose (engine doc §6, §9.2). */
    public static List<String> engineConstraints(final FeaturePlan plan, final boolean streaming) {
        final List<String> errors = new ArrayList<>();
        final boolean keyed = plan.getStages().stream().anyMatch(s ->
                s.kind() == FeaturePlan.StageKind.sequence || s.kind() == FeaturePlan.StageKind.population);
        if (streaming && keyed) {
            errors.add("sequence / population features are supported in batch only (time-sorted keyed state)");
        }
        if (streaming && plan.getColumns().stream().anyMatch(c -> "fold".equals(c.getCoordinates().get("fit")))) {
            errors.add("fit.mode fold is supported in batch only (out-of-fold statistics are fitted from the whole input); use static with an artifact for streaming");
        }
        for (final OutputColumn c : plan.getColumns()) {
            if (c.isIntermediate()) continue;
            if (c.getStatus() == OutputColumn.Status.runtimeFilter) {
                errors.add(c.getCanonicalName() + ": per-row availability filtering (atRowCreation / event_date time) is not implemented by the Beam engine yet; declare a constant availableAt/ingestionLag");
            }
            if (c.getScope() == FeatureSpec.Scope.population && "encoding".equals(c.getOperator())
                    && !PopulationEvaluator.isSupported(c.getCoordinates().get("stat"))) {
                errors.add(c.getCanonicalName() + ": stat '" + c.getCoordinates().get("stat") + "' is not implemented yet (available: count | share | mean | rate | std | distribution)");
            }
        }
        return errors;
    }

    // ------------------------------------------------------------------------------------------
    // fit / apply (fit.mode static)
    // ------------------------------------------------------------------------------------------

    /** One fitted lattice level of a static block: hidden columns it fills and how its statistics are keyed. */
    record FitLevel(String block, String id, String sumColumn, String sumSqColumn, List<String> keys, String field,
                    String offsetColumn, String artifactUri, boolean refit, List<String> foldKeys, int folds) implements Serializable {
        VarianceComponents.LevelSpec spec() {
            return new VarianceComponents.LevelSpec(id, keys, field, offsetColumn, foldKeys, folds);
        }
        /** fit.mode fold: out-of-fold statistics, always fitted in-pipeline (an artifact only holds the totals). */
        boolean isFold() {
            return foldKeys != null;
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
                    foldKeys != null ? Integer.parseInt(c.getCoordinates().get("folds")) : 0));
        }
        return new ArrayList<>(levels.values());
    }

    /**
     * Static fit: per-level sufficient statistics over the whole input (or loaded from the plan's artifact),
     * applied to every row by lookup; composition / statistics are then ordinary row columns.
     */
    private static PCollectionTuple applyFit(final PCollection<MElement> input, final List<OutputColumn> stageColumns,
                                             final StageEvaluator evaluator, final String planHash, final String label,
                                             final List<Logging> loggings, final boolean failFast,
                                             final TupleTag<MElement> outputTag, final TupleTag<BadRecord> failureTag) {
        final List<FitLevel> levels = fitLevels(stageColumns);
        final Map<String, String> loadBlocks = new LinkedHashMap<>();
        final List<FitLevel> fitted = new ArrayList<>();
        final Map<String, String> writeBlocks = new LinkedHashMap<>();
        for (final FitLevel level : levels) {
            final String uri = level.artifactUri();
            // fold levels are always fitted: the per-fold tags cannot come from an artifact (which holds totals)
            final boolean exists = uri != null && !level.refit() && FitArtifact.exists(uri, planHash, level.block());
            if (exists && !level.isFold()) {
                loadBlocks.put(level.block(), uri);
                continue;
            }
            fitted.add(level);
            // fold levels are re-fitted every run but respect refit: false for the (totals) artifact
            if (uri != null && !exists) writeBlocks.put(level.block(), uri);
        }
        for (final Map.Entry<String, String> e : loadBlocks.entrySet()) {
            LOG.info("feature fit: block {} loads artifact {}", e.getKey(), FitArtifact.statsPath(e.getValue(), planHash, e.getKey()));
        }
        if (!fitted.isEmpty() && com.mercari.solution.util.pipeline.OptionUtil.isStreaming(input)) {
            throw new IllegalStateException("fit.mode static in streaming requires an existing artifact for plan " + planHash
                    + " (fit the statistics with a batch run first)");
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
                                .of(new WriteArtifactDoFn(e.getValue(), planHash, e.getKey(), blockLevels, statsView))
                                .withSideInputs(statsView));
            }
        }
        // factorization blocks: gather the training set on one worker, fit by ALS (or load the artifact)
        final List<FmSpec> fmSpecs = fmSpecs(stageColumns);
        final Map<String, PCollectionView<List<Factorization.Model>>> fmViews = new LinkedHashMap<>();
        final Map<String, FmSpec> fmLoad = new LinkedHashMap<>();
        for (final FmSpec spec : fmSpecs) {
            if (spec.artifactUri() != null && !spec.refit() && Factorization.exists(spec.artifactUri(), planHash, spec.block())) {
                fmLoad.put(spec.block(), spec);
                LOG.info("feature fit: block {} loads factorization artifact {}", spec.block(), Factorization.artifactPath(spec.artifactUri(), planHash, spec.block()));
                continue;
            }
            if (com.mercari.solution.util.pipeline.OptionUtil.isStreaming(input)) {
                throw new IllegalStateException("factorization in streaming requires an existing artifact for plan " + planHash);
            }
            final PCollectionView<List<Factorization.Model>> view = fitInput
                    .apply(label + "_Fm_" + spec.block() + "_Examples", ParDo.of(new ExtractExamplesDoFn(spec)))
                    .setCoder(org.apache.beam.sdk.coders.SerializableCoder.of(Factorization.Example.class))
                    .apply(label + "_Fm_" + spec.block() + "_Gather", Combine.globally(new GatherFn()).withoutDefaults())
                    .apply(label + "_Fm_" + spec.block() + "_Fit", ParDo.of(new FitFmDoFn(spec, planHash)))
                    .setCoder(org.apache.beam.sdk.coders.SerializableCoder.of(Factorization.Model.class))
                    .apply(label + "_Fm_" + spec.block() + "_View", View.asList());
            fmViews.put(spec.block(), view);
            sideInputs.add(view);
        }
        final List<OutputColumn> fmColumns = new ArrayList<>();
        for (final OutputColumn c : stageColumns) if ("fm".equals(c.getOperator())) fmColumns.add(c);

        // fitted statistics are extracted from the stage INPUT: a target / offset produced by a column of
        // this same stage would read null for every row, so reject the fusion explicitly
        final Set<String> stageProduced = new HashSet<>();
        for (final OutputColumn c : stageColumns) stageProduced.add(c.getCanonicalName());
        final List<String> sameStageDeps = new ArrayList<>();
        for (final FitLevel level : levels) {
            if (level.field() != null && stageProduced.contains(level.field())) sameStageDeps.add(level.field());
            if (level.offsetColumn() != null && stageProduced.contains(level.offsetColumn())) sameStageDeps.add(level.offsetColumn());
        }
        for (final FmSpec spec : fmSpecs) {
            if (stageProduced.contains(spec.target())) sameStageDeps.add(spec.target());
            if (spec.offsetColumn() != null && stageProduced.contains(spec.offsetColumn())) sameStageDeps.add(spec.offsetColumn());
        }
        if (!sameStageDeps.isEmpty()) {
            throw new IllegalStateException("fit.mode static targets/offsets " + sameStageDeps
                    + " are computed in the same fit stage and would read null; split them into a separate feature step");
        }

        return input.apply(label, ParDo
                .of(new FitApplyDoFn(evaluator, levels, statsView, lambdasView, loadBlocks, planHash,
                        fmSpecs, fmColumns, fmViews, fmLoad, loggings, failFast, failureTag))
                .withSideInputs(sideInputs)
                .withOutputTags(outputTag, TupleTagList.of(failureTag)));
    }

    /** One factorization block of a fit stage, rebuilt from its output columns' coordinates. */
    record FmSpec(String block, List<String> fields, boolean fieldWeighted, int k, String target, String offsetColumn,
                  int epochs, double reg, long seed, String artifactUri, boolean refit) implements Serializable {
        Factorization.Options options() {
            return new Factorization.Options(fields, fieldWeighted, k, epochs, reg, seed);
        }
    }

    static List<FmSpec> fmSpecs(final List<OutputColumn> stageColumns) {
        final Map<String, FmSpec> specs = new LinkedHashMap<>();
        for (final OutputColumn c : stageColumns) {
            if (!"fm".equals(c.getOperator()) || specs.containsKey(c.getBlock())) continue;
            final Map<String, String> k = c.getCoordinates();
            specs.put(c.getBlock(), new FmSpec(c.getBlock(), List.of(k.get("fields").split(",")), "fwfm".equals(k.get("variant")),
                    Integer.parseInt(k.get("latentDim")), k.get("target"), k.get("offset"),
                    Integer.parseInt(k.get("epochs")), Double.parseDouble(k.get("reg")), Long.parseLong(k.get("seed")),
                    k.get("artifactUri"), "true".equals(k.get("refit"))));
        }
        return new ArrayList<>(specs.values());
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

    /** Gathers the training examples into one list (the ALS fit runs in memory on one worker). */
    static class GatherFn extends org.apache.beam.sdk.transforms.Combine.CombineFn<Factorization.Example, ArrayList<Factorization.Example>, ArrayList<Factorization.Example>> {
        @Override
        public ArrayList<Factorization.Example> createAccumulator() { return new ArrayList<>(); }

        @Override
        public ArrayList<Factorization.Example> addInput(final ArrayList<Factorization.Example> acc, final Factorization.Example e) {
            acc.add(e);
            return acc;
        }

        @Override
        public ArrayList<Factorization.Example> mergeAccumulators(final Iterable<ArrayList<Factorization.Example>> accs) {
            final ArrayList<Factorization.Example> out = new ArrayList<>();
            for (final ArrayList<Factorization.Example> a : accs) out.addAll(a);
            return out;
        }

        @Override
        public ArrayList<Factorization.Example> extractOutput(final ArrayList<Factorization.Example> acc) { return acc; }

        @SuppressWarnings({"unchecked", "rawtypes"})
        private static Coder<ArrayList<Factorization.Example>> listCoder() {
            return (Coder) org.apache.beam.sdk.coders.SerializableCoder.of(ArrayList.class);
        }

        @Override
        public Coder<ArrayList<Factorization.Example>> getAccumulatorCoder(final org.apache.beam.sdk.coders.CoderRegistry registry, final Coder<Factorization.Example> inputCoder) {
            return listCoder();
        }

        @Override
        public Coder<ArrayList<Factorization.Example>> getDefaultOutputCoder(final org.apache.beam.sdk.coders.CoderRegistry registry, final Coder<Factorization.Example> inputCoder) {
            return listCoder();
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

    static class WriteArtifactDoFn extends DoFn<String, Void> {
        private final String uri;
        private final String planHash;
        private final String block;
        private final List<String> levels;
        private final PCollectionView<Map<String, VarianceComponents.KeyStats>> statsView;

        WriteArtifactDoFn(final String uri, final String planHash, final String block, final List<String> levels,
                          final PCollectionView<Map<String, VarianceComponents.KeyStats>> statsView) {
            this.uri = uri;
            this.planHash = planHash;
            this.block = block;
            this.levels = levels;
            this.statsView = statsView;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final Map<String, VarianceComponents.KeyStats> all = c.sideInput(statsView);
            final Map<String, VarianceComponents.KeyStats> blockStats = new HashMap<>();
            for (final Map.Entry<String, VarianceComponents.KeyStats> e : all.entrySet()) {
                if (levels.contains(FitArtifact.levelOf(e.getKey()))) blockStats.put(e.getKey(), e.getValue());
            }
            FitArtifact.write(uri, planHash, block, blockStats, levels);
        }
    }

    static class FitApplyDoFn extends StageDoFn<MElement> {
        private static final Map<String, Map<String, VarianceComponents.KeyStats>> ARTIFACT_CACHE = new java.util.concurrent.ConcurrentHashMap<>();

        private static final Map<String, Factorization.Model> FM_CACHE = new java.util.concurrent.ConcurrentHashMap<>();

        private final List<FitLevel> levels;
        private final PCollectionView<Map<String, VarianceComponents.KeyStats>> statsView;
        private final Map<String, String> loadBlocks;
        private final String planHash;
        private final List<FmSpec> fmSpecs;
        private final List<OutputColumn> fmColumns;
        private final Map<String, PCollectionView<List<Factorization.Model>>> fmViews;
        private final Map<String, FmSpec> fmLoad;
        private transient Map<String, VarianceComponents.KeyStats> loaded;
        private transient Map<String, Double> loadedLambdas;
        private transient Map<String, Factorization.Model> loadedModels;

        FitApplyDoFn(final StageEvaluator evaluator, final List<FitLevel> levels,
                     final PCollectionView<Map<String, VarianceComponents.KeyStats>> statsView,
                     final PCollectionView<Map<String, Double>> lambdas,
                     final Map<String, String> loadBlocks, final String planHash,
                     final List<FmSpec> fmSpecs, final List<OutputColumn> fmColumns,
                     final Map<String, PCollectionView<List<Factorization.Model>>> fmViews, final Map<String, FmSpec> fmLoad,
                     final List<Logging> loggings, final boolean failFast, final TupleTag<BadRecord> failureTag) {
            super(evaluator, lambdas, loggings, failFast, failureTag);
            this.levels = levels;
            this.statsView = statsView;
            this.loadBlocks = loadBlocks;
            this.planHash = planHash;
            this.fmSpecs = fmSpecs;
            this.fmColumns = fmColumns;
            this.fmViews = fmViews;
            this.fmLoad = fmLoad;
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
            for (final FmSpec spec : fmLoad.values()) {
                final String path = Factorization.artifactPath(spec.artifactUri(), planHash, spec.block());
                loadedModels.put(spec.block(), FM_CACHE.computeIfAbsent(path, p ->
                        Factorization.read(spec.artifactUri(), planHash, spec.block(), spec.fields(), spec.fieldWeighted(), spec.k())));
            }
        }

        private Factorization.Model model(final ProcessContext c, final String block) {
            final Factorization.Model loadedModel = loadedModels.get(block);
            if (loadedModel != null) return loadedModel;
            final PCollectionView<List<Factorization.Model>> view = fmViews.get(block);
            if (view == null) return null;
            final List<Factorization.Model> models = c.sideInput(view);
            return models.isEmpty() ? null : models.get(0);
        }

        private void applyFactorization(final ProcessContext c, final Map<String, Object> values) {
            for (final FmSpec spec : fmSpecs) {
                final Factorization.Model model = model(c, spec.block());
                final String[] x = fieldValues(values, spec.fields());
                for (final OutputColumn col : fmColumns) {
                    if (!col.getBlock().equals(spec.block())) continue;
                    Object v = null;
                    if (model != null) {
                        switch (col.getCoordinates().get("kind")) {
                            case "pair" -> {
                                final String[] pair = col.getCoordinates().get("pair").split(",");
                                v = model.pair(x, spec.fields().indexOf(pair[0]), spec.fields().indexOf(pair[1]));
                            }
                            case "embedding" -> {
                                final int f = spec.fields().indexOf(col.getCoordinates().get("field"));
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

        @Override
        protected void prepare(final ProcessContext c) {
            final Map<String, Double> merged = new HashMap<>(loadedLambdas);
            if (lambdas != null) merged.putAll(c.sideInput(lambdas));
            evaluator.setLambdas(merged);
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final MElement input = c.element();
            if (input == null) return;
            try {
                prepare(c);
                final Map<String, VarianceComponents.KeyStats> fitted = statsView == null ? Map.of() : c.sideInput(statsView);
                final Map<String, Object> values = input.asPrimitiveMap();
                for (final FitLevel level : levels) {
                    final String key = FeatureValues.key(values, level.keys());
                    VarianceComponents.KeyStats stats = null;
                    if (key != null) {
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
                applyFactorization(c, values);
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

    /** Output schema: input fields + emitted columns, or the grouped parent/children shape (§3.1). */
    public static Schema createOutputSchema(final FeaturePlan plan, final Schema inputSchema, final DataType outputType) {
        final FeatureSpec.ContextDef groupBy = groupByContext(plan);
        final Set<String> passThrough = passThroughInputs(plan, inputSchema);
        if (groupBy == null) {
            final Schema.Builder builder = Schema.builder();
            for (final Schema.Field f : inputSchema.getFields()) if (passThrough.contains(f.getName())) builder.withField(f.copy());
            for (final OutputColumn c : plan.getEmittedColumns()) builder.withField(c.toField());
            return builder.withType(outputType).build();
        }
        final Set<String> parentInputs = new LinkedHashSet<>(groupBy.keys());
        parentInputs.addAll(plan.getSpec().output.parentFields);
        final Schema.Builder parent = Schema.builder();
        final Schema.Builder child = Schema.builder();
        for (final Schema.Field f : inputSchema.getFields()) {
            if (!passThrough.contains(f.getName())) continue;
            (parentInputs.contains(f.getName()) ? parent : child).withField(f.copy());
        }
        for (final OutputColumn c : plan.getEmittedColumns()) {
            (c.getPlacement() == OutputColumn.Placement.parent ? parent : child).withField(c.toField());
        }
        parent.withField(plan.getSpec().output.childName, Schema.FieldType.array(Schema.FieldType.element(child.build())));
        return parent.withType(outputType).build();
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

        ToElementDoFn(final String timeField, final String timeFieldType, final List<String> rowIdFields,
                      final boolean failFast, final TupleTag<BadRecord> failureTag) {
            this.timeField = timeField;
            this.timeFieldType = timeFieldType;
            this.rowIdFields = rowIdFields;
            this.failFast = failFast;
            this.failureTag = failureTag;
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
                if (rowIdFields != null) {
                    // a row with a null rowId component falls back to a random id (it cannot be matched by its fields anyway)
                    final String id = rowIdFields.isEmpty() ? null : FeatureValues.key(values, rowIdFields);
                    values.put(ROW_ID_FIELD, id != null ? id : UUID.randomUUID().toString());
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
            final Map<String, Object> values = element.asPrimitiveMap();
            final Map<String, Object> partial = new HashMap<>();
            partial.put(ROW_ID_FIELD, values.get(ROW_ID_FIELD));
            partial.put(PARTIAL_FIELD, true);
            for (final String name : columns) partial.put(name, values.get(name));
            for (final String name : carry) partial.put(name, values.get(name));
            c.output(MElement.of(partial, c.timestamp()));
        }
    }

    /** Reassembles one row from its base and partial rows (row-id merge of a wave). */
    static class MergeDoFn extends DoFn<KV<String, Iterable<MElement>>, MElement> {
        private final boolean failFast;
        private final TupleTag<BadRecord> failureTag;

        MergeDoFn(final boolean failFast, final TupleTag<BadRecord> failureTag) {
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
            final List<MElement> rejected = new ArrayList<>();
            for (final MElement row : coalesce(kv.getValue(), rejected)) c.outputWithTimestamp(row, row.getTimestamp());
            for (final MElement piece : rejected) {
                c.output(failureTag, Module.processError(rejectionMessage(piece), piece, new IllegalStateException(rejectionMessage(piece)), failFast));
            }
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
        ContextStageDoFn(final StageEvaluator evaluator, final PCollectionView<Map<String, Double>> lambdas,
                         final List<Logging> loggings, final boolean failFast, final TupleTag<BadRecord> failureTag) {
            super(evaluator, lambdas, loggings, failFast, failureTag);
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
            final List<MElement> rejected = new ArrayList<>();
            final List<MElement> elements = coalesce(kv.getValue(), rejected);
            for (final MElement piece : rejected) {
                c.output(failureTag, Module.processError(rejectionMessage(piece), piece, new IllegalStateException(rejectionMessage(piece)), failFast));
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
    static Set<String> passThroughInputs(final FeaturePlan plan, final Schema inputSchema) {
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

        FinalizeDoFn(final List<OutputColumn> emitted, final Schema inputSchema, final Schema outputSchema, final FeatureSpec.NullPolicy nullPolicy,
                     final List<Logging> loggings, final boolean failFast, final TupleTag<BadRecord> failureTag) {
            this.finalizer = new Finalizer(emitted, inputSchema, nullPolicy, Finalizer.outputFieldNames(outputSchema, null));
            this.outputSchema = outputSchema;
            this.logs = Logging.map(loggings);
            this.failFast = failFast;
            this.failureTag = failureTag;
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
        private final Map<String, Logging> logs;
        private final boolean failFast;
        private final TupleTag<BadRecord> failureTag;

        GroupedFinalizeDoFn(final List<OutputColumn> emitted, final Schema inputSchema, final Schema outputSchema, final FeatureSpec.NullPolicy nullPolicy,
                            final List<String> keys, final List<String> parentFields, final String childName,
                            final List<Logging> loggings, final boolean failFast, final TupleTag<BadRecord> failureTag) {
            this.finalizer = new Finalizer(emitted, inputSchema, nullPolicy, Finalizer.outputFieldNames(outputSchema, childName));
            this.outputSchema = outputSchema;
            this.keys = keys;
            this.parentFields = parentFields;
            this.childName = childName;
            this.logs = Logging.map(loggings);
            this.failFast = failFast;
            this.failureTag = failureTag;
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
            final List<MElement> rejected = new ArrayList<>();
            final List<MElement> elements = coalesce(kv.getValue(), rejected);
            for (final MElement piece : rejected) {
                c.output(failureTag, Module.processError(rejectionMessage(piece), piece, new IllegalStateException(rejectionMessage(piece)), failFast));
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
                Logging.log(LOG, logs, "output", output);
            } catch (final Throwable e) {
                for (final MElement element : elements) {
                    c.output(failureTag, Module.processError("Failed to finalize grouped features", element, e, failFast));
                }
            }
        }
    }

}
