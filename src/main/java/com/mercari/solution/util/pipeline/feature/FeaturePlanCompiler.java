package com.mercari.solution.util.pipeline.feature;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.feature.FeatureSpec.*;
import com.mercari.solution.util.pipeline.feature.OperatorCatalog.InputKind;
import com.mercari.solution.util.pipeline.feature.OperatorCatalog.Operator;
import com.mercari.solution.util.pipeline.feature.OutputColumn.Placement;
import com.mercari.solution.util.pipeline.feature.OutputColumn.Status;
import com.mercari.solution.util.pipeline.feature.SourceContract.FieldContract;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Compiles a sources document + feature parameters into a {@link FeaturePlan}
 * (docs/design/feature-engine.md §1.2). Pure function: no Beam, no I/O.
 *
 * <p>Blocks are expanded with a dependency-resolution loop (the same idiom as pipeline assembly):
 * a block is expanded once every name it references resolves to an input field or an already expanded
 * column, so block order in the spec does not matter and unresolved references / cycles are reported
 * together. Availability (§6.1) and lineage (derivedFrom, evidence) are propagated during expansion.
 */
public final class FeaturePlanCompiler {

    private static final Pattern IDENTIFIER = Pattern.compile("(?<![A-Za-z0-9_.$])(?:\\$self\\.([A-Za-z_][A-Za-z0-9_]*)|([A-Za-z_][A-Za-z0-9_.]*)\\s*(\\()?)");
    private static final Pattern QUOTED = Pattern.compile("'[^']*'|\"[^\"]*\"");
    private static final Set<String> KEYWORDS = Set.of(
            "and", "or", "not", "null", "true", "false", "in", "is", "like", "between", "case", "when", "then", "else", "end");
    private static final Set<String> PARENT_CONTEXT_OPS = Set.of("countByValue", "ratioByValue", "entropy", "groupSize");
    /** Context ops whose column can be null although the field is present (a null offset, a group the solver declines). */
    private static final Set<String> NULLABLE_CONTEXT_OPS = Set.of("softmax", "ratingProb", "residualize", "harville");

    private final Diagnostics diagnostics = new Diagnostics();
    private final Map<String, SourceContract> sources;
    /** The calendar clocks declared in the sources document ({@code clocks:}). */
    private final Map<String, Clock> clocks;
    private final FeatureSpec spec;
    private final List<Schema.Field> inputSchemaFields;
    private final Map<String, FieldContract> inputFields = new LinkedHashMap<>();
    private final Map<String, OutputColumn> columnsByCanonical = new LinkedHashMap<>();
    private final List<OutputColumn> columns = new ArrayList<>();
    private final Map<String, EntityDef> entities = new LinkedHashMap<>();
    private final Map<String, ContextDef> contexts = new LinkedHashMap<>();
    private final Map<String, String> baselineColumns = new LinkedHashMap<>();
    /** baselines[].emit: baseline name → the emitted copy column. */
    private final Map<String, String> baselineEmits = new LinkedHashMap<>();
    /** Input fields whose lineage declaration failed: references to them are secondary errors. */
    private final Set<String> lineageMissing = new LinkedHashSet<>();
    /** True when at least one block could not be expanded (availability verdicts are then deferred). */
    private boolean unresolvedBlocks = false;
    /** Hint codes already reported per block (some hints are per block, not per column). */
    private final Set<String> hintedBlocks = new HashSet<>();
    private final List<FeaturePlan.ObservedAtAudit> observedAtAudits = new ArrayList<>();
    /** Per entity, the columns whose staticSafe rests on its declared minInterval, and the largest shift it absorbed. */
    private final Map<String, FeaturePlan.MinIntervalAudit> minIntervalAudits = new LinkedHashMap<>();
    private int anonymousCounter = 0;

    private FeaturePlanCompiler(final JsonElement sourcesDocument, final JsonObject parameters,
                                final List<Schema.Field> inputSchemaFields) {
        this.sources = SourceContract.parseAll(sourcesDocument, diagnostics);
        this.clocks = Clock.parseAll(sourcesDocument, diagnostics);
        this.spec = FeatureSpec.parse(parameters, diagnostics);
        this.inputSchemaFields = inputSchemaFields;
    }

    /**
     * @param sourcesDocument  parsed sources.yaml (object with {@code sources} list, or the bare list)
     * @param parameters       the transform's {@code parameters} block
     * @param inputSchemaFields fields of the input relation when known (null to skip the cross-check)
     */
    public static FeaturePlan compile(final JsonElement sourcesDocument, final JsonObject parameters,
                                      final List<Schema.Field> inputSchemaFields) {
        final FeaturePlanCompiler compiler = new FeaturePlanCompiler(sourcesDocument, parameters, inputSchemaFields);
        return compiler.run(sourcesDocument, parameters);
    }

    private FeaturePlan run(final JsonElement sourcesDocument, final JsonObject parameters) {
        if (!diagnostics.hasErrors() || (!sources.isEmpty() && !spec.features.isEmpty() && spec.timeField != null)) {
            resolveLineage();
            resolveObservedAtAudits();
            resolveDefinitions();
            expandAll();
            finalizeColumns();
            resolveRoles();
            reportMinIntervalAudits();
        }
        final List<FeaturePlan.Stage> stages = buildStages();
        hintGlobalKeyStages(stages);
        final Schema outputSchema = buildSchema();
        final String hash = hash(sourcesDocument, parameters);
        final String outputHash = outputHash(hash);
        final FeaturePlan plan = new FeaturePlan(spec, sources, inputFields, columns, stages, outputSchema, diagnostics, hash, outputHash, observedAtAudits,
                new ArrayList<>(minIntervalAudits.values()));
        hintWideRows(plan);
        return plan;
    }

    /**
     * One hint per keyed stage whose grouped rows carry a map column in the wave engine (engine doc §9.4.7): the
     * audit's row_count bounds a hot key's spill only per row, and a map over many categories is a wide row. The
     * engine drops a column once its last reader has run (a readout is evaluated as early as its inputs allow), so
     * what is listed is read by this or a later stage, or emitted.
     */
    private void hintWideRows(final FeaturePlan plan) {
        for (final FeaturePlan.Stage s : plan.getStages()) {
            if (!s.isKeyed() || s.kind() == FeaturePlan.StageKind.groupBy) continue;
            // the carry of the engine that runs: the wave engine's branches, or the linear chain (parallelWaves off, or no
            // wave to branch) - the chain hosts the deferred readouts in the last stage, so a consumed map rides every
            // keyed stage before it
            final boolean waves = plan.branchesWaves();
            final List<String> maps = plan.mapColumns(waves ? plan.getCarriedColumns(s) : plan.getCarriedColumnsLinear(s));
            if (maps.isEmpty()) continue;
            final List<String> blocks = new ArrayList<>();
            for (final String name : maps) {
                final String block = columnsByCanonical.get(name).block;
                if (!blocks.contains(block)) blocks.add(block);
            }
            diagnostics.hint("engine.rowWidth", "features." + String.join(",", blocks),
                    "stage #" + s.index() + " groups rows that carry the map column(s) " + maps + " (read by it or a later stage, or emitted"
                            + (waves ? "" : "; the linear chain evaluates the readouts in the last stage, so a consumed map rides every keyed stage before it")
                            + "): a map over many categories makes a wide row, and a hot key's spill grows with the row width times its row count"
                            + " (the audit's row_count bounds the count only); " + (waves ? "a map that only its readouts read is dropped before this stage"
                            + " - emit the readouts rather than the distribution, or place the block so that its map is consumed before the widest key"
                            : "with engine.parallelWaves the wave engine evaluates the readouts as soon as the levels are merged and drops the map"));
        }
    }

    /**
     * One info per entity whose declared {@code minInterval} absorbed a window shift (DSL spec §6.2 tier 2). Raised
     * after expansion, so it names the largest shift the declaration absorbed and every column resting on it — inside
     * {@link #classifyPast} only the first column's shift is known.
     */
    private void reportMinIntervalAudits() {
        for (final FeaturePlan.MinIntervalAudit a : minIntervalAudits.values()) {
            diagnostics.info("entity.minInterval", "entities." + a.entity(), "minInterval " + a.minInterval() + " is a declaration, not a check: it lets "
                    + a.columns().size() + " column(s) over entity " + a.entity() + " read an outcome without a shift of up to " + a.shift()
                    + " (staticSafe). An event that follows the entity's previous one sooner than that may read an outcome not yet known - the plan's"
                    + " audit query counts such events in the input, and the run counts them as feature/minInterval_" + a.entity()
                    + "_below; declare the interval the data has");
        }
    }

    /**
     * The observedAt audit (DSL spec §7): every input field whose contract names an {@code observedAtField}
     * gets an audit entry — the engine counts rows observed after the declared availability (and after
     * predictAt) and measures the {@code predictAt − observedAt} distribution. The declaration says when the
     * value is supposed to exist; the audit checks the data against it, which is where a hand-written
     * point-in-time selection upstream goes wrong (a later observation slipping into a "t-10" column).
     */
    private void resolveObservedAtAudits() {
        final Map<String, Schema.FieldType> schemaTypes = new HashMap<>();
        if (inputSchemaFields != null) {
            for (final Schema.Field f : inputSchemaFields) schemaTypes.put(f.getName(), f.getFieldType());
        }
        for (final FieldContract field : inputFields.values()) {
            final String observedAt = field.getObservedAtField();
            if (observedAt == null || field.getAvailableAt() == null) continue;
            final boolean known = inputSchemaFields != null;
            final boolean present = !known || schemaTypes.containsKey(observedAt);
            if (known && !present) {
                diagnostics.warning("sources.observedAt.missingInput", "sources." + field.getSourceName() + "." + field.getName(),
                        "observedAtField '" + observedAt + "' is not in the input relation: the observedAt audit of '" + field.getName()
                                + "' cannot run (pass the observation-time column through, or the claim stays unverified)");
            }
            final Schema.FieldType type = schemaTypes.containsKey(observedAt) ? schemaTypes.get(observedAt)
                    : inputFields.containsKey(observedAt) ? inputFields.get(observedAt).getType() : null;
            final String typeName = type == null ? "timestamp" : type.getType().name();
            observedAtAudits.add(new FeaturePlan.ObservedAtAudit(field.getName(), field.getSourceName(), observedAt, typeName,
                    present, field.getAvailableAt(), spec.predictAt));
        }
    }

    /**
     * output.roles: the data contract of the output table. Every role names something that exists (an input
     * field, a context / entity for group / entity, a baseline for baseline); the role columns are recorded in
     * the manifest so consumers can exclude them from the feature set mechanically.
     */
    private void resolveRoles() {
        for (final Map.Entry<String, String> e : spec.output.roles.entrySet()) {
            final String role = e.getKey();
            final String name = e.getValue();
            final String loc = "output.roles." + role;
            final boolean input = inputFields.containsKey(name);
            switch (role) {
                case "group" -> {
                    if (!input && !contexts.containsKey(name)) {
                        diagnostics.error("output.roles.unresolved", loc, "group role '" + name + "' is neither an input field nor a context");
                    }
                }
                case "entity" -> {
                    if (!input && !entities.containsKey(name)) {
                        diagnostics.error("output.roles.unresolved", loc, "entity role '" + name + "' is neither an input field nor an entity");
                    }
                }
                case "baseline" -> {
                    if (baselineColumns.containsKey(name)) {
                        if (!baselineEmits.containsKey(name) || !isEmittedColumn(baselineEmits.get(name))) {
                            diagnostics.warning("output.roles.baseline.notEmitted", loc,
                                    "baseline '" + name + "' is not an output column (baselines are intermediate): give it baselines[].emit so consumers reading the role from the manifest find it");
                        }
                    } else if (!input && !isEmittedColumn(name)) {
                        diagnostics.error("output.roles.unresolved", loc, "baseline role '" + name + "' is neither a baseline name, an input field nor an output column");
                    }
                }
                default -> {
                    if (!input && !isEmittedColumn(name)) {
                        diagnostics.error("output.roles.unresolved", loc, role + " role '" + name + "' is neither an input field nor an output column");
                    }
                }
            }
            if ("time".equals(role) && input && !name.equals(spec.timeField)) {
                diagnostics.warning("output.roles.time", loc, "time role '" + name + "' differs from time.field '" + spec.timeField + "'");
            }
        }
    }

    private boolean isEmittedColumn(final String name) {
        for (final OutputColumn c : columns) {
            if (!c.intermediate && (c.canonicalName.equals(name) || c.outputName.equals(name))) return true;
        }
        return false;
    }

    /**
     * S4 (engine doc §9.4.5): a key-less replay stage ({@link FeaturePlan.Stage#runsUnderSingleKey}) puts
     * every row under one key — one worker thread. The hint points at {@code fit.mode static} / {@code fold}
     * (a parallel Combine); the modeling trade-off — the values change — lives in the feature docs,
     * "Performance and sizing". One hint per stage, at the blocks that force the global level; never
     * applied automatically.
     */
    private void hintGlobalKeyStages(final List<FeaturePlan.Stage> stages) {
        for (final FeaturePlan.Stage s : stages) {
            if (!s.runsUnderSingleKey()) continue;
            // a rating's pool cannot move to a Combine (its state is not mergeable): its blocks get their own hint
            final List<String> ratingBlocks = new ArrayList<>(), keyedBlocks = new ArrayList<>();
            for (final String name : s.columnNames()) {
                final OutputColumn c = columnsByCanonical.get(name);
                if (c == null || isRowColumn(c)) continue;
                final List<String> blocks = "rating".equals(c.operator) ? ratingBlocks : keyedBlocks;
                if (!blocks.contains(c.block)) blocks.add(c.block);
            }
            if (!ratingBlocks.isEmpty()) {
                diagnostics.hint("sequence.rating.globalKey", "features." + String.join(",", ratingBlocks),
                        "stage #" + s.index() + " replays every row under one key (a single worker thread): a rating update reads the ratings the earlier"
                                + " contests left, so a pool is one replay in time order and has no parallel form; when the contests fall into independent pools,"
                                + " split them with window.filter \"<field> = $self.<field>\" on a pre-event field (it becomes the partition key)");
            }
            // without a rating the hint names the stage's blocks as before; with one, only the other keyed blocks
            final List<String> otherBlocks = ratingBlocks.isEmpty() ? s.blocks() : keyedBlocks;
            if (otherBlocks.isEmpty()) continue;
            diagnostics.hint("encoding.globalKey", "features." + String.join(",", otherBlocks),
                    "stage #" + s.index() + " evaluates every row under one key (a single worker thread"
                            + (spec.engine.parallelWaves ? " - the critical path once the waves run in parallel" : "")
                            + "); a global / very coarse encoding can move its statistics to fit.mode forward"
                            + " (complete earlier time blocks, leak-free), static (streaming-capable with an artifact) or fold (batch only) - the values change:"
                            + " see the feature docs, Performance and sizing");
        }
    }

    // ------------------------------------------------------------------------------------------
    // lineage / definitions
    // ------------------------------------------------------------------------------------------

    private void resolveLineage() {
        for (final LineageEntry entry : spec.lineage) {
            final SourceContract source = sources.get(entry.from());
            if (source == null) {
                diagnostics.error("lineage.source", "lineage", "unknown source: " + entry.from());
                lineageMissing.addAll(entry.fields());
                continue;
            }
            final String eventTime = entry.eventTime() != null ? entry.eventTime() : source.getEventTime();
            if (spec.timeField != null && !spec.timeField.equals(eventTime)) {
                diagnostics.error("time.mismatch", "lineage." + entry.from(),
                        "source eventTime '" + eventTime + "' differs from time.field '" + spec.timeField
                                + "': mixed event times need an explicit lineage.eventTime mapping");
            }
            for (final String fieldName : entry.fields()) {
                final FieldContract field = source.getField(fieldName);
                if (field == null) {
                    diagnostics.error("lineage.field", "lineage." + entry.from(), "field not declared in source: " + fieldName);
                    lineageMissing.add(fieldName);
                    continue;
                }
                if (inputFields.containsKey(fieldName)) {
                    diagnostics.error("lineage.duplicate", "lineage." + entry.from(), "field declared by multiple lineage entries: " + fieldName);
                }
                inputFields.put(fieldName, field);
            }
        }
        if (spec.timeField != null && !inputFields.containsKey(spec.timeField)) {
            inputFields.put(spec.timeField, FieldContract.synthetic(spec.timeField, Schema.FieldType.TIMESTAMP, AvailableAt.atEventTime()));
        }
        for (final String f : spec.engine.rowId) {
            if (!inputFields.containsKey(f)) diagnostics.error("engine.rowId", "engine", "engine.rowId field '" + f + "' is not an input field");
        }
        // the fan-out merge rides these keys in the row map (FeatureStages): an input field with either name
        // would make every base row look like a partial (or collide with the row id) in ANY engine mode
        for (final String f : List.of(FeatureStages.ROW_ID_FIELD, FeatureStages.PARTIAL_FIELD)) {
            if (inputFields.containsKey(f)) {
                diagnostics.error("input.reserved", "lineage", "input field '" + f + "' is reserved by the feature engine (fan-out merge); rename it upstream");
            }
        }
        if (spec.timeField != null && inputFields.get(spec.timeField).getType() != null) {
            final String timeType = inputFields.get(spec.timeField).getType().getType().name();
            if (!List.of("timestamp", "datetime", "date", "string").contains(timeType)) {
                diagnostics.error("time.field.type", "time",
                        "time.field '" + spec.timeField + "' must be a timestamp / datetime / date field (is " + timeType + ")");
            }
        }
        if (inputSchemaFields != null) {
            final Set<String> schemaNames = new LinkedHashSet<>();
            for (final Schema.Field f : inputSchemaFields) {
                schemaNames.add(f.getName());
                if (!inputFields.containsKey(f.getName())) {
                    diagnostics.warning("lineage.undeclared", "lineage",
                            "input field '" + f.getName() + "' has no lineage entry; it cannot be used by features");
                }
            }
            final Map<String, Schema.FieldType> schemaTypes = new HashMap<>();
            for (final Schema.Field f : inputSchemaFields) schemaTypes.put(f.getName(), f.getFieldType());
            for (final SourceContract.FieldContract field : inputFields.values()) {
                final String name = field.getName();
                if (!schemaNames.contains(name)) {
                    diagnostics.error("lineage.missingInput", "lineage", "lineage field '" + name + "' is not present in the input schema");
                    continue;
                }
                // the contract type selects the engine's code path (an array<...> field is read as a vector, a scalar
                // through toDouble): a shape mismatch would read null for every row, so it is an error here
                final Schema.FieldType actual = schemaTypes.get(name);
                if (field.getType() != null && actual != null
                        && (field.getType().getType() == Schema.Type.array) != (actual.getType() == Schema.Type.array)) {
                    diagnostics.error("lineage.type.mismatch", "sources." + field.getSourceName() + "." + name,
                            "field '" + name + "' is declared " + FeaturePlan.typeName(field.getType())
                                    + " in the sources contract but the input schema has " + FeaturePlan.typeName(actual));
                }
            }
        }
    }

    private void resolveDefinitions() {
        for (final EntityDef e : spec.entities) {
            if (entities.put(e.name(), e) != null) diagnostics.error("entities.duplicate", "entities." + e.name(), "duplicate entity name");
            for (final String key : e.keys()) requireInputField(key, "entities." + e.name());
        }
        for (final ContextDef c : spec.contexts) {
            if (contexts.put(c.name(), c) != null) diagnostics.error("contexts.duplicate", "contexts." + c.name(), "duplicate context name");
            for (final String key : c.keys()) requireInputField(key, "contexts." + c.name());
        }
        if (spec.fit.orderBy != null && !spec.fit.orderBy.equals(spec.timeField)) {
            diagnostics.error("fit.orderBy", "fit", "fit.orderBy must equal time.field (" + spec.timeField + ")");
        }
        if (spec.fit.minHistory != null) {
            // implemented for fit.mode forward (FitSpec.minBlocksOf rounds it up to whole blocks); the other modes have no blocks to count
            diagnostics.info("fit.minHistory", "fit", "fit.minHistory is the minimum history of a fit.mode forward block, rounded up to whole blocks"
                    + " (an explicit minBlocks wins); expanding / static / fold fits have no blocks and ignore it");
        }
        if (spec.fit.align != null) {
            diagnostics.info("fit.align", "fit", "fit.align is how the forward fits of an svd / spectralEmbedding block are brought into the coordinates of the fit before them"
                    + " (a block's own fit.align wins); the other fits have no such freedom and ignore it");
        }
        if (spec.fit.minRows != null) {
            diagnostics.info("fit.minRows", "fit", "fit.minRows is the fewest rows a smooth / svd / quantileTransform / spectralEmbedding fit is solved from"
                    + " (a block's own fit.minRows wins); encodings shrink a thin level instead and ignore it");
        }
        if (spec.fit.groupBy != null && !entities.containsKey(spec.fit.groupBy)) {
            diagnostics.error("fit.groupBy", "fit", "fit.groupBy must reference an entity: " + spec.fit.groupBy);
        }
        if (spec.output.groupBy != null && !contexts.containsKey(spec.output.groupBy)) {
            diagnostics.error("output.groupBy", "output", "output.groupBy must reference a context: " + spec.output.groupBy);
        }
        for (final String f : spec.output.parentFields) requireInputField(f, "output.parentFields");
        if (spec.output.groupBy != null && inputFields.containsKey(spec.output.childName)) {
            diagnostics.error("output.childName", "output", "output.childName collides with input field: " + spec.output.childName);
        }
        final Set<String> names = new HashSet<>();
        for (final FeatureDef def : spec.features) {
            if (!names.add(def.name)) diagnostics.error("features.duplicate", def.location(), "duplicate feature name");
            if (def.name.contains(".") || def.name.startsWith("_")) {
                diagnostics.error("features.name", def.location(), "feature names must not contain '.' or start with '_' (reserved for lint)");
            }
        }
        for (final BaselineDef b : spec.baselines) {
            if (b.context() != null && !contexts.containsKey(b.context())) {
                diagnostics.error("baselines.context", "baselines." + b.name(), "unknown context: " + b.context());
            }
        }
    }

    private void requireInputField(final String name, final String location) {
        if (!inputFields.containsKey(name)) {
            diagnostics.error("reference.unknown", location, "unknown input field: " + name);
        }
    }

    // ------------------------------------------------------------------------------------------
    // expansion loop
    // ------------------------------------------------------------------------------------------

    /** A block awaiting expansion with its syntactic references; {@code ownPrefix} is what its own columns start with. */
    private record Pending(String name, Set<String> references, String ownPrefix, Runnable expand) {
        boolean isOwn(final String reference) {
            return ownPrefix != null && reference.startsWith(ownPrefix);
        }
    }

    private void expandAll() {
        final List<Pending> pending = new ArrayList<>();
        for (final BaselineDef baseline : spec.baselines) {
            pending.add(new Pending("baselines." + baseline.name(), baselineReferences(baseline), null, () -> expandBaseline(baseline)));
        }
        for (final FeatureDef def : spec.features) {
            pending.add(new Pending(def.location(), blockReferences(def), def.name + "_", () -> expandBlock(def)));
        }
        expandReady(pending, false);
        // whatever is left may be waiting for itself: a residualize regressor naming an earlier op of the same block
        // is a reference the block satisfies while it expands (the ops run in declaration order), not a cycle. Only
        // a stalled set is retried this way, so a reference another block happens to provide is still waited for.
        expandReady(pending, true);
        // a reference into another failed block is also a secondary failure, not a fresh error
        final Set<String> failedBlocks = new LinkedHashSet<>();
        for (final Pending p : pending) {
            final int dot = p.name.indexOf('.');
            if (dot > 0) failedBlocks.add(p.name.substring(dot + 1));
        }
        for (final Pending p : pending) {
            unresolvedBlocks = true;
            final List<String> unresolved = new ArrayList<>();
            final List<String> causedByLineage = new ArrayList<>();
            for (final String r : p.references.stream().filter(r -> !resolves(r)).sorted().toList()) {
                final boolean secondary = lineageMissing.contains(r)
                        || failedBlocks.stream().anyMatch(b -> r.equals(b) || r.startsWith(b + "_") || r.startsWith(b + "."));
                (secondary ? causedByLineage : unresolved).add(r);
            }
            if (!causedByLineage.isEmpty() && unresolved.isEmpty() && !lineageMissing.isEmpty()) {
                // secondary failure: the root cause is the lineage error already reported
                diagnostics.info("reference.unresolved", p.name,
                        "not expanded (caused by earlier errors on: " + causedByLineage + ")");
                continue;
            }
            if (unresolved.isEmpty()) {
                // no root cause elsewhere: this is a dependency cycle between the failed blocks
                diagnostics.error("reference.cycle", p.name,
                        "dependency cycle with " + causedByLineage);
                continue;
            }
            diagnostics.error("reference.unresolved", p.name,
                    "unresolved references (unknown name or dependency cycle): " + unresolved
                            + (causedByLineage.isEmpty() ? "" : "; caused by: " + causedByLineage));
        }
    }

    /** Expands every block whose references resolve, until none does; {@code ignoreOwn} = a block may wait for itself. */
    private void expandReady(final List<Pending> pending, final boolean ignoreOwn) {
        boolean progress = true;
        while (!pending.isEmpty() && progress) {
            progress = false;
            final Iterator<Pending> it = pending.iterator();
            while (it.hasNext()) {
                final Pending p = it.next();
                if (p.references.stream().allMatch(r -> resolves(r) || (ignoreOwn && p.isOwn(r)))) {
                    p.expand.run();
                    it.remove();
                    progress = true;
                }
            }
        }
    }

    private boolean resolves(final String reference) {
        return resolve(reference) != null;
    }

    /** Resolved reference: either an input field or an expanded column. */
    private record Ref(String canonical, FieldContract field, OutputColumn column) {
        Schema.FieldType type() { return field != null ? field.getType() : column.fieldType; }
        AvailableAt availableAt() { return field != null ? field.getEffectiveAvailableAt() : column.availableAt; }
        /** world availability without ingestion lag: used to classify outcome-like inputs */
        AvailableAt worldAvailableAt() { return field != null ? field.getAvailableAt() : column.availableAt; }
        Set<String> derivedFrom() {
            if (field != null) return field.getKind() == null ? Set.of() : Set.of(field.getKind());
            return column.derivedFrom;
        }
        boolean declared() { return field != null ? field.isDeclared() : column.declaredEvidence; }
        Set<String> sources() { return field != null ? Set.of(field.getSourceName()) : column.sources; }
    }

    private Ref resolve(final String reference) {
        if (reference == null) return null;
        final OutputColumn column = columnsByCanonical.get(reference);
        if (column != null) return new Ref(column.canonicalName, null, column);
        final int dot = reference.indexOf('.');
        if (dot > 0) {
            // block.column form: the canonical name is the column part when it already carries the block prefix
            final String columnPart = reference.substring(dot + 1);
            final OutputColumn byPart = columnsByCanonical.get(columnPart);
            if (byPart != null && byPart.block.equals(reference.substring(0, dot))) return new Ref(byPart.canonicalName, null, byPart);
            final OutputColumn prefixed = columnsByCanonical.get(reference.substring(0, dot) + "_" + columnPart);
            if (prefixed != null) return new Ref(prefixed.canonicalName, null, prefixed);
        }
        final FieldContract field = inputFields.get(reference);
        if (field != null) return new Ref(reference, field, null);
        if (baselineColumns.containsKey(reference)) {
            final OutputColumn b = columnsByCanonical.get(baselineColumns.get(reference));
            if (b != null) return new Ref(b.canonicalName, null, b);
        }
        return null;
    }

    private Set<String> blockReferences(final FeatureDef def) {
        final Set<String> refs = new LinkedHashSet<>();
        if (def.expr != null) refs.addAll(expressionReferences(def.expr).others);
        if (def.input != null) refs.add(def.input);
        refs.addAll(def.inputs);
        if (def.baseline != null) refs.add(def.baseline);
        if (def.offset != null) refs.add(def.offset);
        // the sequence of values a spectralEmbedding / transitionStats block walks
        if (def.sequenceField != null) refs.add(def.sequenceField);
        // the target of a smooth curve is read by its fit (a discretize target is declared but unused)
        if (def.target != null && "smooth".equals(def.type)) refs.add(def.target);
        for (final Op op : def.ops) {
            refs.addAll(op.fields);
            if (op.against != null) refs.add(op.against);
            // a rating reads its contest's keys from the past rows
            if (def.scope == Scope.sequence && op.context != null && contexts.containsKey(op.context)) refs.addAll(contexts.get(op.context).keys());
            if (op.expr != null) refs.addAll(expressionReferences(op.expr).others);
            if (op.predicate != null) refs.addAll(expressionReferences(op.predicate).others);
            if (op.weightBy != null) {
                final References w = expressionReferences(op.weightBy);
                refs.addAll(w.others);
                refs.addAll(w.self);
            }
            // the explanatory fields of a context residualize (a sequence regression's single series is op.against)
            refs.addAll(op.regressors);
            // the uncertainty column of a ratingProb: it may come from a block declared after this one
            if (op.sigmaField != null) refs.add(op.sigmaField);
        }
        // the general form's channels: a typo or a forward reference must wait / be reported like an op's field
        if (def.lift != null) {
            refs.addAll(def.lift.fields);
            for (final LiftExpr expr : def.lift.exprs) refs.addAll(expressionReferences(expr.expr()).others);
        }
        for (final Window w : def.windows) {
            if (w.filter != null) {
                final References r = expressionReferences(w.filter);
                refs.addAll(r.others);
                refs.addAll(r.self);
            }
        }
        for (final KeySet ks : def.keySets) {
            refs.addAll(ks.keys);
            if (ks.parentRef != null) refs.add(ks.parentRef);
            for (final Window w : ks.windows) {
                if (w.filter != null) {
                    final References r = expressionReferences(w.filter);
                    refs.addAll(r.others);
                    refs.addAll(r.self);
                }
            }
        }
        for (final Target t : def.targets) {
            if (t.field != null) refs.add(t.field);
            if (t.expr != null) refs.addAll(expressionReferences(t.expr).others);
        }
        // factorization dependencies (block-order independence)
        refs.addAll(def.fields);
        if (def.taskTarget != null) refs.add(def.taskTarget);
        if (def.taskTargetExpr != null) refs.addAll(expressionReferences(def.taskTargetExpr).others);
        if (def.taskOffset != null) refs.add(def.taskOffset);
        return refs;
    }

    private Set<String> baselineReferences(final BaselineDef baseline) {
        final Set<String> refs = new LinkedHashSet<>(expressionReferences(baseline.expr()).others);
        // context ops used as functions (share(...), rank(...)) are not references
        refs.removeIf(r -> OperatorCatalog.get(Scope.context, r) != null);
        return refs;
    }

    private record References(Set<String> others, Set<String> self, boolean usesSelf) {}

    /** Identifiers in an expression / predicate: function calls, keywords, numbers and quoted text excluded. */
    static References expressionReferences(final String expression) {
        final Set<String> others = new LinkedHashSet<>();
        final Set<String> self = new LinkedHashSet<>();
        if (expression == null) return new References(others, self, false);
        final String stripped = QUOTED.matcher(expression).replaceAll(" ");
        final Matcher m = IDENTIFIER.matcher(stripped);
        boolean usesSelf = false;
        while (m.find()) {
            if (m.group(1) != null) {
                self.add(m.group(1));
                usesSelf = true;
                continue;
            }
            if (m.group(3) != null) continue; // function call
            final String id = m.group(2);
            if (KEYWORDS.contains(id.toLowerCase()) || id.startsWith("$")) continue;
            others.add(id);
        }
        return new References(others, self, usesSelf);
    }

    // ------------------------------------------------------------------------------------------
    // block expansion
    // ------------------------------------------------------------------------------------------

    private AvailableAt computeAtOf(final FeatureDef def) {
        if (def.computeAtExpression == null) return spec.predictAt;
        try {
            final AvailableAt computeAt = AvailableAt.parseTimeExpression(def.computeAtExpression);
            if (!computeAt.isStatic()) {
                diagnostics.error("computeAt.invalid", def.location(), "computeAt must be event_time +/- duration");
                return spec.predictAt;
            }
            if (!computeAt.isStaticallyAtOrBefore(spec.predictAt)) {
                diagnostics.error("computeAt.afterPredictAt", def.location(),
                        "computeAt " + computeAt.describe() + " is after predictAt " + spec.predictAt.describe());
            }
            return computeAt;
        } catch (final IllegalArgumentException e) {
            diagnostics.error("computeAt.invalid", def.location(), e.getMessage());
            return spec.predictAt;
        }
    }

    private OutputColumn newColumn(final String block, final Scope scope, final String operator,
                                   final String canonical, final Schema.FieldType type, final AvailableAt computeAt) {
        final OutputColumn c = new OutputColumn();
        c.block = block;
        c.scope = scope;
        c.operator = operator;
        c.canonicalName = canonical;
        c.fieldType = type;
        c.computeAt = computeAt;
        return c;
    }

    private void register(final OutputColumn c) {
        if (columnsByCanonical.containsKey(c.canonicalName)) {
            diagnostics.error("column.duplicate", "features." + c.block, "duplicate output column: " + c.canonicalName);
            return;
        }
        if (inputFields.containsKey(c.canonicalName)) {
            diagnostics.error("column.shadowsInput", "features." + c.block,
                    "output column '" + c.canonicalName + "' has the same name as an input field; choose another name (in-place overwrite is not supported)");
            return;
        }
        columnsByCanonical.put(c.canonicalName, c);
        columns.add(c);
    }

    /** The canonical name of a reference (as projected into history / row maps), or the raw text when unresolved. */
    private String canonicalOf(final String reference) {
        final Ref ref = resolve(reference);
        return ref != null ? ref.canonical() : reference;
    }

    /** Adds a row-side input (self row): availability max, lineage union. */
    private void addSelfInput(final OutputColumn c, final String reference) {
        final Ref ref = resolve(reference);
        if (ref == null) return;
        c.inputs.add(ref.canonical);
        c.availableAt = AvailableAt.max(c.availableAt, ref.availableAt());
        mergeLineage(c, ref);
    }

    private void mergeLineage(final OutputColumn c, final Ref ref) {
        c.derivedFrom.addAll(ref.derivedFrom());
        c.sources.addAll(ref.sources());
        c.sources.remove("");
        c.declaredEvidence |= ref.declared();
    }

    private void expandBaseline(final BaselineDef baseline) {
        final String loc = "baselines." + baseline.name();
        final OperatorCatalog.Call call = OperatorCatalog.parseContextCall(baseline.expr());
        if (call != null && baseline.context() == null) {
            diagnostics.error("baselines.expr.op", loc, "'" + call.operator().name() + "(...)' is a context op: the baseline must name the"
                    + " 'context' whose group it is computed over");
        } else if (call != null && !call.operator().baselineCallable()) {
            diagnostics.error("baselines.expr.op", loc, "context op '" + call.operator().name() + "' cannot be called from a baseline expression"
                    + " (it takes parameters of its own): declare it as an op of a context feature block instead");
        }
        final OutputColumn c = newColumn("baselines", baseline.context() != null ? Scope.context : Scope.row, "baseline",
                "__baseline_" + baseline.name(), Schema.FieldType.FLOAT64, spec.predictAt);
        c.intermediate = true;
        c.anonymous = true;
        c.coordinates.put("baseline", baseline.name());
        c.coordinates.put("expr", baseline.expr());
        if (baseline.context() != null) {
            c.coordinates.put("context", baseline.context());
            final ContextDef context = contexts.get(baseline.context());
            if (context != null) for (final String key : context.keys()) addSelfInput(c, key);
        }
        for (final String r : baselineReferences(baseline)) {
            final Ref ref = resolve(r);
            if (ref != null && ref.field != null && "market".equals(ref.field.getKind()) && ref.field.isDeclared() && !ref.field.isAllowDeclared()) {
                diagnostics.error("baselines.declaredMarket", loc,
                        "baseline references market field '" + r + "' with evidence: declared; a baseline must be time-consistent (measured or allowDeclared)");
            }
            addSelfInput(c, r);
            // the baseline is as perishable as its most perishable input (a market price with validFor)
            final Duration validFor = ref == null ? null : ref.field != null ? ref.field.getValidFor() : ref.column.validFor;
            if (validFor != null && (c.validFor == null || validFor.compareTo(c.validFor) < 0)) c.validFor = validFor;
        }
        if (c.availableAt == null) c.availableAt = AvailableAt.atEventTime();
        c.status = Status.staticSafe;
        register(c);
        baselineColumns.put(baseline.name(), c.canonicalName);
        if (baseline.emit() != null) {
            // emit: the baseline value as an output column (a probability from share(...), say), so consumers
            // and the softmax offset read the same number
            if (columnsByCanonical.containsKey(baseline.emit()) || inputFields.containsKey(baseline.emit())) {
                diagnostics.error("baselines.emit.duplicate", loc, "emit name collides with an existing column or input field: " + baseline.emit());
                return;
            }
            final OutputColumn e = newColumn("baselines", Scope.row, "copy", baseline.emit(), Schema.FieldType.FLOAT64, spec.predictAt);
            e.coordinates.put("baseline", baseline.name());
            addSelfInput(e, c.canonicalName);
            e.validFor = c.validFor;
            // the row's own baseline value: available when its inputs are (a baseline over an outcome is a valid
            // offset — read from past rows — but emitting it puts the outcome on the row, a violation like any other)
            if (e.availableAt == null) e.availableAt = AvailableAt.atEventTime();
            e.status = rowStatus(e.availableAt, e.computeAt);
            register(e);
            baselineEmits.put(baseline.name(), e.canonicalName);
        }
    }

    private void expandBlock(final FeatureDef def) {
        final AvailableAt computeAt = computeAtOf(def);
        switch (def.scope) {
            case row -> expandRow(def, computeAt);
            case context -> expandContext(def, computeAt);
            case sequence -> expandSequence(def, computeAt);
            case population -> expandPopulation(def, computeAt);
        }
    }

    // --- row ----------------------------------------------------------------------------------

    private void expandRow(final FeatureDef def, final AvailableAt computeAt) {
        final String loc = def.location();
        final String type = def.type != null ? def.type : (def.expr != null ? "expr" : null);
        if (type == null) {
            diagnostics.error("row.type", loc, "row feature requires 'expr' or 'type' (datetime | bin | cross | residual)");
            return;
        }
        final Operator op = OperatorCatalog.get(Scope.row, type);
        if (op == null) {
            diagnostics.error("row.type", loc, "unknown row type: " + type);
            return;
        }
        switch (type) {
            case "expr" -> {
                final References refs = expressionReferences(def.expr);
                if (refs.usesSelf) diagnostics.error("row.self", loc, "$self is only allowed in window.filter");
                final OutputColumn c = newColumn(def.name, Scope.row, "expr", def.name, Schema.FieldType.FLOAT64, computeAt);
                c.coordinates.put("expr", def.expr);
                for (final String r : refs.others) {
                    final Ref ref = resolve(r);
                    if (ref != null && !OperatorCatalog.isNumeric(ref.type()) && ref.type() != null
                            && ref.type().getType() != Schema.Type.bool) {
                        diagnostics.error("row.expr.type", loc, "expr operand '" + r + "' is not numeric (" + ref.type().getType() + "); expressions are evaluated as doubles");
                    }
                    addSelfInput(c, r);
                }
                finishRow(c, def);
            }
            case "datetime" -> {
                final String input = singleInput(def);
                if (input == null) return;
                final Ref inputRef = resolve(input);
                final String inputType = inputRef == null || inputRef.type() == null ? "timestamp" : inputRef.type().getType().name();
                if (!List.of("timestamp", "datetime", "date", "string", "int64").contains(inputType)) {
                    diagnostics.error("row.datetime.input", loc, "datetime input '" + input + "' must be a timestamp / datetime / date field (is " + inputType + ")");
                    return;
                }
                final List<String> derive = def.derive.isEmpty() ? List.of("month", "dayOfWeek") : def.derive;
                for (final String d : derive) {
                    if (!OperatorCatalog.datetimeDerivations().contains(d)) {
                        diagnostics.error("row.datetime.derive", loc, "unknown derivation: " + d + " (" + OperatorCatalog.datetimeDerivations() + ")");
                        continue;
                    }
                    if ("date".equals(inputType) && List.of("hour", "minute").contains(d)) {
                        diagnostics.error("row.datetime.derive", loc, "derivation " + d + " is not defined for a date field");
                        continue;
                    }
                    if (def.cyclical) {
                        for (final String trig : List.of("sin", "cos")) {
                            final OutputColumn c = newColumn(def.name, Scope.row, "datetime", def.name + "_" + d + "_" + trig, Schema.FieldType.FLOAT64, computeAt);
                            c.coordinates.put("derive", d);
                            c.coordinates.put("trig", trig);
                            c.coordinates.put("inputType", inputType);
                            addSelfInput(c, input);
                            finishRow(c, def);
                        }
                    } else {
                        final OutputColumn c = newColumn(def.name, Scope.row, "datetime", def.name + "_" + d, Schema.FieldType.INT64, computeAt);
                        c.coordinates.put("derive", d);
                        c.coordinates.put("inputType", inputType);
                        addSelfInput(c, input);
                        finishRow(c, def);
                    }
                }
            }
            case "bin" -> {
                final String input = singleInput(def);
                if (input == null) return;
                if (def.edges.isEmpty()) diagnostics.error("row.bin.edges", loc, "bin requires 'edges'");
                final OutputColumn c = newColumn(def.name, Scope.row, "bin", def.name, Schema.FieldType.INT64, computeAt);
                c.coordinates.put("edges", def.edges.toString());
                addSelfInput(c, input);
                finishRow(c, def);
            }
            case "cross" -> {
                if (def.inputs.size() < 2) diagnostics.error("row.cross.inputs", loc, "cross requires at least two 'inputs'");
                final OutputColumn c = newColumn(def.name, Scope.row, "cross", def.name, Schema.FieldType.STRING, computeAt);
                for (final String in : def.inputs) addSelfInput(c, in);
                finishRow(c, def);
            }
            case "indicator" -> {
                final String input = singleInput(def);
                if (input == null) return;
                if (def.values.isEmpty()) {
                    diagnostics.error("row.indicator.values", loc, "indicator requires 'values' (the categories to flag)");
                    return;
                }
                final Ref ref = resolve(input);
                for (final String value : def.values) {
                    warnNeverMatches("row.indicator.values", loc, input, ref, value);
                    final OutputColumn c = newColumn(def.name, Scope.row, "indicator", def.name + "_" + value, Schema.FieldType.INT64, computeAt);
                    c.coordinates.put("value", value);
                    addSelfInput(c, input);
                    finishRow(c, def);
                }
            }
            case "equals" -> {
                if (def.inputs.size() != 2) {
                    diagnostics.error("row.equals.inputs", loc, "equals requires exactly two 'inputs'");
                    return;
                }
                final OutputColumn c = newColumn(def.name, Scope.row, "equals", def.name, Schema.FieldType.INT64, computeAt);
                for (final String in : def.inputs) addSelfInput(c, in);
                finishRow(c, def);
            }
            case "residual" -> {
                final String input = singleInput(def);
                if (input == null) return;
                if (def.baseline == null || !baselineColumns.containsKey(def.baseline)) {
                    diagnostics.error("row.residual.baseline", loc, "residual requires 'baseline' referencing baselines[].name");
                    return;
                }
                final String on = def.on == null ? "identity" : def.on;
                if (!List.of("identity", "logit", "log").contains(on)) diagnostics.error("row.residual.on", loc, "on must be identity | logit | log");
                final OutputColumn c = newColumn(def.name, Scope.row, "residual", def.name, Schema.FieldType.FLOAT64, computeAt);
                c.coordinates.put("baseline", def.baseline);
                c.coordinates.put("on", on);
                addSelfInput(c, input);
                addSelfInput(c, def.baseline);
                finishRow(c, def);
            }
            case "noise" -> {
                // placebo column: a deterministic pseudo-random draw per row identity (the fold rule: time.field +
                // orderTieBreak), so re-runs and parallel branches agree; carries no information by construction
                final String distribution = def.distribution == null ? "normal" : def.distribution;
                if (!List.of("normal", "uniform").contains(distribution)) {
                    diagnostics.error("row.noise.distribution", loc, "distribution must be normal | uniform: " + distribution);
                    return;
                }
                if (def.seed == null) {
                    diagnostics.error("row.noise.seed", loc, "noise requires 'seed' (the draw must be reproducible)");
                    return;
                }
                if (spec.orderTieBreak.isEmpty()) {
                    diagnostics.warning("row.noise.identity", loc, "noise without time.orderTieBreak draws from time.field alone: rows sharing a timestamp get the same value; declare time.orderTieBreak for a row identity");
                }
                final OutputColumn c = newColumn(def.name, Scope.row, "noise", def.name, Schema.FieldType.FLOAT64, computeAt);
                c.coordinates.put("distribution", distribution);
                c.coordinates.put("seed", Long.toString(def.seed));
                c.coordinates.put("identity", String.join(",", rowIdentity()));
                for (final String f : rowIdentity()) addSelfInput(c, f);
                finishRow(c, def);
            }
            case "vector" -> expandVector(def, computeAt);
            default -> diagnostics.error("row.type", loc, "unsupported row type: " + type);
        }
    }

    /** The highest {@code degree} of a vector polyfit: beyond it the Vandermonde system of a short array is ill-conditioned. */
    private static final int MAX_VECTOR_DEGREE = 5;

    /**
     * Row {@code type: vector}: scalar readouts of a numeric array field. The steps run slice → diff → normalize,
     * then each of {@code funcs} reads one column {@code <name>_<func>} ({@code polyfit}: one column
     * {@code <name>_poly<k>} per coefficient). The columns inherit the array field's availability.
     */
    private void expandVector(final FeatureDef def, final AvailableAt computeAt) {
        final String loc = def.location();
        final String input = singleInput(def);
        if (input == null) return;
        final Ref ref = resolve(input);
        if (ref == null || ref.type() == null || ref.type().getType() != Schema.Type.array || !OperatorCatalog.isNumeric(ref.type().getArrayValueType())) {
            diagnostics.error("row.vector.input", loc, "vector input '" + input + "' must be an array of numbers (declare it as array<float64> in the sources contract)");
            return;
        }
        if (def.funcs.isEmpty()) {
            diagnostics.error("row.vector.funcs", loc, "vector requires 'funcs' (available: " + String.join(" | ", OperatorCatalog.VECTOR_FUNCS) + ")");
            return;
        }
        boolean valid = true;
        for (final String func : def.funcs) {
            if (OperatorCatalog.vectorOutput(func) == null) {
                diagnostics.error("row.vector.funcs", loc, "unknown vector func: " + func + " (available: " + String.join(" | ", OperatorCatalog.VECTOR_FUNCS) + ")");
                valid = false;
            }
        }
        if (new LinkedHashSet<>(def.funcs).size() != def.funcs.size()) {
            diagnostics.error("row.vector.funcs", loc, "funcs lists a readout twice: " + def.funcs);
            valid = false;
        }
        if (def.sliceMalformed) {
            diagnostics.error("row.vector.slice", loc, "slice must be an object {from, to}: elements [from, to), a negative index counts from the end");
            valid = false;
        }
        if (def.diff != null && def.diff < 0) {
            diagnostics.error("row.vector.diff", loc, "diff must be >= 0 (the differencing order): " + def.diff);
            valid = false;
        }
        if (def.normalize != null && !OperatorCatalog.VECTOR_NORMALIZATIONS.contains(def.normalize)) {
            diagnostics.error("row.vector.normalize", loc, "normalize must be " + String.join(" | ", OperatorCatalog.VECTOR_NORMALIZATIONS) + ": " + def.normalize);
            valid = false;
        }
        final String position = def.position == null ? "index" : def.position;
        if (!List.of("index", "unit").contains(position)) {
            diagnostics.error("row.vector.position", loc, "position must be index | unit: " + position);
            valid = false;
        }
        final boolean polyfit = def.funcs.contains("polyfit");
        final int degree = def.degree == null ? 2 : def.degree;
        if (def.degree != null && !polyfit) {
            diagnostics.warning("row.vector.degree", loc, "degree only applies to the polyfit readout, which funcs does not list");
        } else if (polyfit && (degree < 1 || degree > MAX_VECTOR_DEGREE)) {
            diagnostics.error("row.vector.degree", loc, "degree must be 1.." + MAX_VECTOR_DEGREE + ": " + degree);
            valid = false;
        }
        if (!def.coefficients.isEmpty() && !polyfit) {
            diagnostics.warning("row.vector.coefficients", loc, "coefficients only applies to the polyfit readout, which funcs does not list");
        } else if (!def.coefficients.isEmpty() && (def.coefficients.stream().anyMatch(k -> k < 0 || k > degree) || new LinkedHashSet<>(def.coefficients).size() != def.coefficients.size())) {
            diagnostics.error("row.vector.coefficients", loc, "coefficients must list distinct indices in 0..degree (" + degree + "): " + def.coefficients);
            valid = false;
        }
        if (def.resample != null && def.resample < 1) {
            diagnostics.error("row.vector.resample", loc, "resample must be >= 1 (the length the vector is interpolated onto): " + def.resample);
            valid = false;
        }
        if (def.padMalformed || (def.padLength != null && def.padLength < 1)
                || (def.padMode != null && !List.of("edge", "zero").contains(def.padMode)) || (def.padSide != null && !List.of("end", "start").contains(def.padSide))) {
            diagnostics.error("row.vector.pad", loc, "pad must be {length: >= 1, mode: edge | zero (default edge), side: end | start (default end)}");
            valid = false;
        } else if (def.padDeclared && def.padLength == null) {
            diagnostics.error("row.vector.pad", loc, "pad requires 'length' (the length the vector is extended to)");
            valid = false;
        }
        if (!valid) return;
        for (final String func : def.funcs) {
            final List<Integer> coefficients = !"polyfit".equals(func) ? List.of(0)
                    : !def.coefficients.isEmpty() ? def.coefficients : java.util.stream.IntStream.rangeClosed(0, degree).boxed().toList();
            for (final int k : coefficients) {
                final String name = def.name + "_" + ("polyfit".equals(func) ? "poly" + k : func);
                final OutputColumn c = newColumn(def.name, Scope.row, "vector", name, OperatorCatalog.vectorOutput(func), computeAt);
                c.coordinates.put("func", func);
                if (def.sliceFrom != null) c.coordinates.put("sliceFrom", Integer.toString(def.sliceFrom));
                if (def.sliceTo != null) c.coordinates.put("sliceTo", Integer.toString(def.sliceTo));
                if (def.diff != null && def.diff > 0) c.coordinates.put("diff", Integer.toString(def.diff));
                if (def.normalize != null) c.coordinates.put("normalize", def.normalize);
                if (def.resample != null) c.coordinates.put("resample", Integer.toString(def.resample));
                if (def.padLength != null) {
                    c.coordinates.put("padLength", Integer.toString(def.padLength));
                    c.coordinates.put("padMode", def.padMode == null ? VectorOps.DEFAULT_PAD_MODE : def.padMode);
                    c.coordinates.put("padSide", def.padSide == null ? VectorOps.DEFAULT_PAD_SIDE : def.padSide);
                }
                if ("slope".equals(func) || "polyfit".equals(func)) c.coordinates.put("position", position);
                if ("polyfit".equals(func)) {
                    c.coordinates.put("degree", Integer.toString(degree));
                    c.coordinates.put("coefficient", Integer.toString(k));
                }
                addSelfInput(c, input);
                finishRow(c, def);
            }
        }
    }

    /** The row identity of the fold rule: time.field + orderTieBreak (canonical names). */
    private List<String> rowIdentity() {
        final List<String> identity = new ArrayList<>();
        identity.add(spec.timeField);
        for (final String f : spec.orderTieBreak) if (!identity.contains(f)) identity.add(f);
        return identity;
    }

    /**
     * A declared value compared with a numeric field matches as a number ({@code value: 1} = 1.0, the type a row
     * expression always has), so only a non-numeric one can never match: warn rather than emit a column of zeros.
     */
    private void warnNeverMatches(final String code, final String loc, final String field, final Ref ref, final String value) {
        if (ref == null || !OperatorCatalog.isNumeric(ref.type()) || FeatureValues.toDouble(value) != null) return;
        // an op's inline expr is an anonymous column (<block>__e<n>): name the expression the user wrote instead
        final String label = ref.column() != null && ref.column().anonymous ? "expr \"" + ref.column().coordinates.get("expr") + "\"" : "'" + field + "'";
        diagnostics.warning(code, loc, "'" + value + "' is not a number, and " + label + " is " + ref.type().getType()
                + ": the value never matches");
    }

    private String singleInput(final FeatureDef def) {
        if (def.input != null) return def.input;
        if (def.inputs.size() == 1) return def.inputs.get(0);
        diagnostics.error("row.input", def.location(), "type: " + def.type + " requires exactly one 'input'");
        return null;
    }

    /**
     * §6.2 verdict of a row-side availability against the column's computeAt: at or before it is safe, statically
     * after it is a violation, and an availability that is not decidable statically is filtered per row.
     */
    private static Status rowStatus(final AvailableAt availableAt, final AvailableAt computeAt) {
        return availableAt.isStaticallyAtOrBefore(computeAt) ? Status.staticSafe
                : availableAt.isStatic() ? Status.violation : Status.runtimeFilter;
    }

    private void finishRow(final OutputColumn c, final FeatureDef def) {
        if (c.availableAt == null) c.availableAt = AvailableAt.atEventTime();
        c.validFor = def.validFor;
        c.status = rowStatus(c.availableAt, c.computeAt);
        register(c);
    }

    // --- context ------------------------------------------------------------------------------

    private void expandContext(final FeatureDef def, final AvailableAt computeAt) {
        final String loc = def.location();
        final ContextDef context = def.context == null ? null : contexts.get(def.context);
        if (context == null) {
            diagnostics.error("context.unknown", loc, "context feature requires 'context' referencing contexts[].name: " + def.context);
            return;
        }
        if (def.ops.isEmpty()) {
            diagnostics.error("context.ops", loc, "context feature requires 'ops'");
            return;
        }
        warnRowSetDrift(def, context);
        final List<String> blockInputs = new ArrayList<>(def.inputs);
        if (def.input != null) blockInputs.add(def.input);
        for (final Op op : def.ops) {
            final Operator operator = OperatorCatalog.get(Scope.context, op.type);
            if (operator == null) {
                diagnostics.error("context.op", loc, "unknown context op: " + op.type);
                continue;
            }
            // desugar: inputs × ops → op.fields
            final List<String> fields = !op.fields.isEmpty() ? op.fields : blockInputs;
            if (operator.input() == InputKind.none) {
                final OutputColumn c = newColumn(def.name, Scope.context, op.type, def.name + "_" + op.type, operator.output(), computeAt);
                finishContext(c, def, context, op);
                continue;
            }
            if (fields.isEmpty()) {
                diagnostics.error("context.fields", loc, "op " + op.type + " requires 'fields' (or block-level 'inputs')");
                continue;
            }
            // the op's own parameters are validated once here — an op over three fields reports a bad one once
            final ContextOpParams params = validateContextOp(op, def, context, loc);
            if (params == null) continue;
            for (final String field : fields) {
                final Ref ref = resolve(field);
                if (ref == null) continue;
                if (!accepts(operator, ref.type())) {
                    diagnostics.error("context.op.type", loc, "op " + op.type + " expects " + operator.input() + " input; '" + field + "' is " + (ref.type() == null ? "unknown" : ref.type().getType()));
                    continue;
                }
                if (params.against().contains(canonicalOf(field))) {
                    diagnostics.error("context.residualize.against", loc, "residualize 'against' names the field '" + field + "' itself");
                    continue;
                }
                for (final Variant variant : contextVariants(op, params, operator, ref)) {
                    final OutputColumn c = newColumn(def.name, Scope.context, op.type,
                            def.name + "_" + (op.as != null ? op.as : field) + variant.suffix(), variant.type(), computeAt);
                    c.coordinates.put("field", canonicalOf(field));
                    c.coordinates.putAll(params.coordinates());
                    c.coordinates.putAll(variant.coordinates());
                    addSelfInput(c, field);
                    for (final String input : params.inputs()) addSelfInput(c, input);
                    // the probability is as perishable as its offset (a market price with validFor)
                    if (def.validFor == null && params.validFor() != null) c.validFor = params.validFor();
                    finishContext(c, def, context, op);
                }
            }
        }
    }

    /** Groups with more valid rows than this read null under {@code harville} unless the op declares its own bound. */
    static final int DEFAULT_MAX_GROUP_SIZE = 64;

    /**
     * What every column of a context op needs, validated once per op rather than once per field: the coordinates the
     * op contributes to each column, the extra inputs those columns read (the regressors of a residualize, a softmax
     * offset, the row identity of a shuffle), the validFor an offset passes on, the resolved regressors and the
     * places of a harville (one column each).
     */
    private record ContextOpParams(Map<String, String> coordinates, List<String> inputs, Duration validFor,
                                   List<String> against, List<Integer> top) {}

    /**
     * Validates the parameters of one context op and resolves what its columns are built from: {@code softmax}
     * (offset / temperature / scales), {@code residualize} (the explanatory fields), {@code harville} (places /
     * discount / maxGroupSize) and {@code shuffle} (seed / ordering). Returns null when no column must be created.
     */
    private ContextOpParams validateContextOp(final Op op, final FeatureDef def, final ContextDef context, final String loc) {
        final Map<String, String> coordinates = new LinkedHashMap<>();
        final List<String> inputs = new ArrayList<>();
        final List<String> against = new ArrayList<>();
        List<Integer> top = List.of();
        Duration validFor = null;
        switch (op.type) {
            case "residualize" -> {
                if (op.regressors.isEmpty()) {
                    diagnostics.error("context.residualize.against", loc, "residualize requires 'against': the field(s) of the op are regressed on it within the group");
                    return null;
                }
                for (final String regressor : op.regressors) {
                    final Ref ref = resolve(regressor);
                    if (ref == null || !OperatorCatalog.isNumeric(ref.type())) {
                        diagnostics.error("context.residualize.against", loc, "residualize 'against' field '" + regressor + "' must be a numeric field or column"
                                + (ref == null ? "" : " (is " + (ref.type() == null ? "unknown" : ref.type().getType()) + ")"));
                        return null;
                    }
                    if (against.contains(ref.canonical())) {
                        diagnostics.error("context.residualize.against", loc, "residualize 'against' lists '" + regressor + "' twice");
                        return null;
                    }
                    against.add(ref.canonical());
                    inputs.add(ref.canonical());
                }
                coordinates.put("against", String.join(",", against));
                if (op.maxGroupSize != null) {
                    diagnostics.warning("context.op.maxGroupSize", loc,
                            "maxGroupSize is read by harville only: residualize reads every row of the group");
                }
            }
            case "harville" -> {
                top = op.top.isEmpty() ? List.of(2, 3) : op.top;
                if (top.stream().anyMatch(k -> k < 1 || k > GroupOps.MAX_TOP) || new HashSet<>(top).size() != top.size()) {
                    diagnostics.error("context.harville.top", loc, "harville top must list distinct places in [1, " + GroupOps.MAX_TOP + "]: " + top);
                    return null;
                }
                if (op.discount.size() > GroupOps.MAX_TOP - 1 || op.discount.stream().anyMatch(d -> !(d > 0) || d.isInfinite())) {
                    diagnostics.error("context.harville.discount", loc, "harville discount lists the positive exponents of the 2nd and the 3rd place (at most "
                            + (GroupOps.MAX_TOP - 1) + " values, 1 = plain Harville): " + op.discount);
                    return null;
                }
                final int maxGroupSize = op.maxGroupSize == null ? DEFAULT_MAX_GROUP_SIZE : op.maxGroupSize;
                if (maxGroupSize < 2) {
                    diagnostics.error("context.op.maxGroupSize", loc, "maxGroupSize must be >= 2: " + maxGroupSize);
                    return null;
                }
                if (def.excludeSelf) diagnostics.warning("context.harville.excludeSelf", loc, "excludeSelf has no effect on harville (the row is part of its own field)");
                if (hintedBlocks.add("context.op.groupSolver:" + def.name)) {
                    diagnostics.info("context.op.groupSolver", loc, "harville solves every group on one worker - quadratic in the group size for the 2nd place, cubic for the 3rd:"
                            + " a group with more than " + maxGroupSize + " valid rows reads null (maxGroupSize)");
                }
                if (!op.discount.isEmpty()) coordinates.put("discount", op.discount.stream().map(Object::toString).collect(java.util.stream.Collectors.joining(",")));
                coordinates.put("maxGroupSize", Integer.toString(maxGroupSize));
            }
            case "ratingProb" -> {
                // the field is the strength (a rating's mu, a team's), sigma the column of its uncertainty (optional: 0
                // without it), beta the rating's performance noise: c^2 = sum over the group of (sigma^2 + beta^2)
                if (op.sigmaField != null) {
                    // an op fans out over its fields with ONE set of coordinates: a sigma belongs to one strength, so an
                    // op over several fields would read every field's contest with the same uncertainty
                    final int fieldCount = !op.fields.isEmpty() ? op.fields.size() : def.inputs.size();
                    if (fieldCount > 1) {
                        diagnostics.error("context.ratingProb.sigma", loc, "ratingProb sigma '" + op.sigmaField + "' is the uncertainty of one strength: an op that names it takes one field"
                                + " (this op covers " + fieldCount + ") - declare one ratingProb op per field, each with its own sigma");
                        return null;
                    }
                    final Ref ref = resolve(op.sigmaField);
                    if (ref == null || !OperatorCatalog.isNumeric(ref.type())) {
                        diagnostics.error("context.ratingProb.sigma", loc, "ratingProb sigma must name a numeric column (the row's rating uncertainty): " + op.sigmaField
                                + (ref == null ? "" : " is " + (ref.type() == null ? "unknown" : ref.type().getType())));
                        return null;
                    }
                    coordinates.put("sigma", ref.canonical());
                    inputs.add(ref.canonical());
                } else if (op.sigma != null) {
                    diagnostics.error("context.ratingProb.sigma", loc, "ratingProb sigma names the column of each row's uncertainty (a rating's sigma readout), not a number: " + op.sigma);
                    return null;
                }
                if (op.beta == null || !(op.beta > 0) || op.beta.isInfinite()) {
                    diagnostics.error("context.ratingProb.beta", loc, "ratingProb requires beta > 0: the performance noise of the rating the field comes from"
                            + " (its beta parameter; sigma / 2 of its prior by default, 25 / 6 for the default prior)" + (op.beta == null ? "" : ": " + op.beta));
                    return null;
                }
                coordinates.put("beta", Double.toString(op.beta));
                if (def.excludeSelf) {
                    diagnostics.warning("context.ratingProb.excludeSelf", loc, "excludeSelf has no effect on ratingProb (the row is part of its own contest)");
                }
            }
            case "softmax" -> {
                if (op.offset != null) {
                    final String offsetColumn = baselineColumns.containsKey(op.offset) ? baselineColumns.get(op.offset) : op.offset;
                    final Ref ref = resolve(offsetColumn);
                    if (ref == null) {
                        diagnostics.error("context.softmax.offset", loc, "softmax offset must reference baselines[].name or a numeric column: " + op.offset);
                        return null;
                    }
                    if (!OperatorCatalog.isNumeric(ref.type())) {
                        diagnostics.error("context.softmax.offset", loc, "softmax offset '" + op.offset + "' is not numeric");
                        return null;
                    }
                    coordinates.put("offset", ref.canonical());
                    inputs.add(ref.canonical());
                    validFor = ref.field != null ? ref.field.getValidFor() : ref.column.validFor;
                }
                if (op.temperature != null && !(op.temperature > 0)) {
                    diagnostics.error("context.softmax.temperature", loc, "temperature must be > 0: " + op.temperature);
                    return null;
                }
                final String offsetScale = op.offsetScale == null ? "probability" : op.offsetScale;
                if (!List.of("probability", "log").contains(offsetScale)) {
                    diagnostics.error("context.softmax.offsetScale", loc, "offsetScale must be probability | log: " + offsetScale);
                    return null;
                }
                final String scoreNull = op.scoreNull == null ? "zero" : op.scoreNull;
                if (!List.of("zero", "null").contains(scoreNull)) {
                    diagnostics.error("context.softmax.scoreNull", loc, "scoreNull must be zero | null: " + scoreNull);
                    return null;
                }
                coordinates.put("temperature", Double.toString(op.temperature == null ? 1d : op.temperature));
                if (op.temperatureSource != null) coordinates.put("temperatureSource", op.temperatureSource);
                coordinates.put("offsetScale", offsetScale);
                coordinates.put("scoreNull", scoreNull);
                if (def.excludeSelf) {
                    diagnostics.warning("context.softmax.excludeSelf", loc, "excludeSelf has no effect on softmax (the row is part of its own normalisation)");
                }
            }
            case "shuffle" -> {
                if (op.seed == null) {
                    diagnostics.error("context.shuffle.seed", loc, "shuffle requires 'seed' (the permutation must be reproducible)");
                    return null;
                }
                if (spec.orderTieBreak.isEmpty()) {
                    diagnostics.warning("context.shuffle.identity", loc, "shuffle without time.orderTieBreak orders rows sharing a timestamp by their input values only; declare time.orderTieBreak for a row identity");
                }
                coordinates.put("seed", Long.toString(op.seed));
                coordinates.put("order", String.join(",", rowIdentity()));
                coordinates.put("contextKeys", String.join(",", context.keys()));
                // rows sharing the identity are told apart by their input values, so the permutation is a pure
                // function of the group whatever order the GroupByKey delivers (and identical in every engine mode)
                final List<String> tieBreak = new ArrayList<>(new TreeSet<>(inputFields.keySet()));
                tieBreak.removeAll(rowIdentity());
                coordinates.put("tieBreak", String.join(",", tieBreak));
                // the permuted values carry the availability of the source column (the op's own field); the row
                // identity is read to order the group
                inputs.addAll(rowIdentity());
            }
            default -> { }
        }
        return new ContextOpParams(coordinates, inputs, validFor, against, top);
    }

    /** One column of a context op: the ops that fan out produce several per field (a listed value, a place). */
    private record Variant(String suffix, Schema.FieldType type, Map<String, String> coordinates) {}

    /**
     * The columns one field of a context op produces: one per listed value for {@code countByValue} /
     * {@code ratioByValue} (like {@code indicator}, instead of a map column), one per place for {@code harville},
     * one otherwise.
     */
    private List<Variant> contextVariants(final Op op, final ContextOpParams params, final Operator operator, final Ref ref) {
        if (!op.values.isEmpty() && List.of("countByValue", "ratioByValue").contains(op.type)) {
            final Schema.FieldType type = "countByValue".equals(op.type) ? Schema.FieldType.INT64 : Schema.FieldType.FLOAT64;
            return op.values.stream().map(value -> new Variant("_" + op.type + "_" + value, type, Map.of("value", value))).toList();
        }
        if (!params.top().isEmpty()) {
            return params.top().stream().map(k -> new Variant("_harville_top" + k, Schema.FieldType.FLOAT64, Map.of("top", Integer.toString(k)))).toList();
        }
        return List.of(new Variant("_" + op.type, operator.outputFor(ref.type()), Map.of()));
    }

    private void finishContext(final OutputColumn c, final FeatureDef def, final ContextDef context, final Op op) {
        c.coordinates.put("context", context.name());
        c.coordinates.put("op", op.type);
        if (def.excludeSelf) c.coordinates.put("excludeSelf", "true");
        for (final String key : context.keys()) addSelfInput(c, key);
        if (c.availableAt == null) c.availableAt = AvailableAt.atEventTime();
        if (def.validFor != null || c.validFor == null) c.validFor = def.validFor; // an op may have inherited one (softmax offset)
        c.status = rowStatus(c.availableAt, c.computeAt);
        if (!def.excludeSelf && PARENT_CONTEXT_OPS.contains(op.type) && context.name().equals(spec.output.groupBy)) {
            // group-constant only within its own context: parent placement requires the grouping context
            c.placement = Placement.parent;
        }
        register(c);
    }

    /** §6.1 context: the row set of a corrections source without snapshotOf is the final set. */
    private void warnRowSetDrift(final FeatureDef def, final ContextDef context) {
        final Set<String> drifting = new LinkedHashSet<>();
        for (final String key : context.keys()) {
            final FieldContract f = inputFields.get(key);
            if (f == null) continue;
            final SourceContract s = sources.get(f.getSourceName());
            if (s != null && s.getMutability() == SourceContract.Mutability.corrections && s.getSnapshotOf() == null) {
                drifting.add(s.getName());
            }
        }
        if (!drifting.isEmpty()) {
            diagnostics.warning("context.rowSetDrift", def.location(),
                    "context '" + context.name() + "' is keyed on corrections source(s) " + drifting
                            + " without snapshotOf: the training row set is the final set (after late removals/additions), serving sees the pre-event set");
        }
    }

    private static boolean accepts(final Operator operator, final Schema.FieldType type) {
        return switch (operator.input()) {
            case none, any, predicate -> true;
            case numeric -> OperatorCatalog.isNumeric(type);
            case categorical -> OperatorCatalog.isCategorical(type);
        };
    }

    // --- sequence -----------------------------------------------------------------------------

    private void expandSequence(final FeatureDef def, final AvailableAt computeAt) {
        final String loc = def.location();
        final EntityDef entity = def.entity == null ? null : entities.get(def.entity);
        if (entity == null) {
            diagnostics.error("sequence.entity", loc, "sequence feature requires 'entity' referencing entities[].name: " + def.entity);
            return;
        }
        final boolean general = def.lift != null || def.summarize || def.compress;
        if (general && !def.ops.isEmpty()) {
            diagnostics.error("sequence.form", loc, "use either 'ops' (the sugar) or the general form 'lift' + 'summarize', not both in one block");
            return;
        }
        if (def.ops.isEmpty() && !general) {
            diagnostics.error("sequence.ops", loc, "sequence feature requires 'ops' or the general form 'lift' + 'summarize'");
            return;
        }
        if (def.fitJson != null) {
            // a sequence block fits nothing: its one fit setting is the artifact a rating op snapshots its state to
            final JsonObject fit = parseJsonObject(def.fitJson);
            for (final String key : fit.keySet()) {
                if ("artifact".equals(key)) continue;
                diagnostics.error("sequence.fit", loc, "a sequence block takes fit.artifact only (the state snapshot of a rating op): '" + key
                        + "' is a fit setting of the population blocks");
                return;
            }
            if (def.ops.stream().noneMatch(o -> "rating".equals(o.type))) {
                diagnostics.warning("sequence.fit.ignored", loc, "fit.artifact on a sequence block without a rating op has no state to snapshot and is ignored");
            }
        }
        // an unknown direction was reported by FeatureSpec (sequence.direction): the block expands nothing
        if (def.direction != null && !FeatureSpec.DIRECTIONS.contains(def.direction)) return;
        if (general && isFuture(def)) {
            diagnostics.error("sequence.direction.op", loc, "the general form 'lift' + 'summarize' reads the past window only: a future window accepts ops (available: "
                    + String.join(" | ", OperatorCatalog.FUTURE_OPS) + ")");
            return;
        }
        final GeneralForm form = general ? generalForm(def, computeAt) : null;
        if (general && form == null) return;
        // reported once per op, not per window: the anonymous name is the column's field segment
        for (final Op op : def.ops) {
            if (op.expr != null && op.as == null) {
                diagnostics.info("sequence.expr.anonymous", loc, "op " + op.type + " expr '" + op.expr + "' is named by the spec-wide anonymous counter ("
                        + def.name + "__e{n}), which renumbers when an earlier expression is added or removed: name its columns with as:");
            }
        }
        final List<Window> windows = def.windows.isEmpty() ? List.of(new Window()) : def.windows;
        for (final Window window : windows) {
            // a same-field $self equality filter is a partition of the entity: reduce it to a stage key
            // so hot entities are split across workers (rows whose field is null bypass the stage → null)
            final String reducedKey = reducibleFilterField(window, computeAt);
            final References filterRefs = window.filter == null || reducedKey != null ? null : expressionReferences(window.filter);
            if (reducedKey != null) {
                // a rating is pooled: its stage key is the reduced field alone, the entity is not part of it
                final boolean anyRating = def.ops.stream().anyMatch(o -> "rating".equals(o.type));
                diagnostics.info("sequence.filter.reduced", loc,
                        "window.filter '" + window.filter + "' is evaluated as an additional partition key (" + String.join(",", entity.keys()) + "," + reducedKey + ")"
                                + (anyRating ? "; a rating op of this block is partitioned by (" + reducedKey + ") alone - its pool is every entity sharing that value" : ""));
            }
            if (form != null) {
                expandDynamics(def, entity, window, filterRefs, reducedKey, form, computeAt);
                continue;
            }
            for (final Op op : def.ops) {
                final Operator operator = OperatorCatalog.get(Scope.sequence, op.type);
                if (operator == null) {
                    diagnostics.error("sequence.op", loc, "unknown sequence op: " + op.type);
                    continue;
                }
                if ("dynamics".equals(op.type)) {
                    // catalogued for its column operator; as an op it has no sugar (only ewma = order-0 exponential)
                    diagnostics.error("sequence.op", loc, "dynamics is not an op: use the general form 'lift' + 'summarize: {dynamics: {...}}' instead of 'ops'");
                    continue;
                }
                if (!directionAccepts(def, window, op)) continue;
                final boolean future = isFuture(def);
                final List<String> fields = new ArrayList<>(op.fields);
                if (op.expr != null) {
                    final OutputColumn anonymous = desugarExpression(def, op.expr, computeAt);
                    if (anonymous != null) fields.add(anonymous.canonicalName);
                }
                final References weightRefs = op.weightBy == null ? null : weightReferences(def, op);
                if (op.weightBy != null && weightRefs == null) continue;
                if (operator.input() == InputKind.predicate) {
                    if (op.predicate == null) {
                        diagnostics.error("sequence.predicate", loc, "op " + op.type + " requires 'predicate'");
                        continue;
                    }
                    final References refs = expressionReferences(op.predicate);
                    if (refs.usesSelf) diagnostics.error("sequence.self", loc, "op " + op.type + ": $self is not allowed in sequence ops (past rows only); use window.filter or a lag + row expr");
                    final List<String> units = "sinceEvent".equals(op.type) ? (op.unit.isEmpty() ? List.of("events") : op.unit) : List.of("");
                    for (final String unit : units) {
                        final String suffix = op.as != null ? op.as + (units.size() > 1 ? "_" + unit : "")
                                : "sinceEvent".equals(op.type) ? (future ? "until_" : "since_") + unit : op.type.toLowerCase();
                        final Schema.FieldType type = "sinceEvent".equals(op.type) && !"events".equals(unit) ? Schema.FieldType.FLOAT64 : Schema.FieldType.INT64;
                        final OutputColumn c = newColumn(def.name, Scope.sequence, op.type, def.name + "_" + window.token() + "_" + suffix, type, computeAt);
                        final String predicate = conditionText(op.predicate, loc, "predicate");
                        if (predicate != null) c.coordinates.put("predicate", predicate);
                        if (!unit.isEmpty()) c.coordinates.put("unit", unit);
                        for (final String r : refs.others) addPastInput(c, r);
                        finishSequence(c, def, entity, window, filterRefs, reducedKey, op);
                    }
                    continue;
                }
                if (fields.isEmpty()) {
                    // COUNT(1): a field-less aggregate counts every visible past row, nulls included
                    if ("aggregate".equals(op.type) && (op.funcs.isEmpty() || op.funcs.equals(List.of("count")))) {
                        // under weightBy the count is Σw over the visible rows (the effective count)
                        final OutputColumn c = newColumn(def.name, Scope.sequence, op.type,
                                def.name + "_" + window.token() + "_" + (op.as != null && weightRefs != null ? op.as + "_" : "") + "count",
                                weightRefs == null ? Schema.FieldType.INT64 : Schema.FieldType.FLOAT64, computeAt);
                        c.coordinates.put("func", "count");
                        addWeight(c, op, weightRefs);
                        // the keys are read from the self row (keying), not from past rows: no projection
                        for (final String key : entity.keys()) addSelfInput(c, key);
                        finishSequence(c, def, entity, window, filterRefs, reducedKey, op);
                        continue;
                    }
                    diagnostics.error("sequence.fields", loc, "op " + op.type + " requires 'field' / 'fields' / 'expr' (only 'aggregate' with funcs: [count] may omit them)");
                    continue;
                }
                for (final String field : fields) {
                    final Ref ref = resolve(field);
                    if (ref == null) continue;
                    if (!accepts(operator, ref.type())) {
                        diagnostics.error("sequence.op.type", loc, "op " + op.type + " expects " + operator.input() + " input; '" + field + "' is " + (ref.type() == null ? "unknown" : ref.type().getType()));
                        continue;
                    }
                    // `as` names the field segment (an inline expr would otherwise show as the anonymous __e{n})
                    final String base = def.name + "_" + window.token() + "_" + (op.as != null && fields.size() == 1 ? op.as : displayName(field)) + "_";
                    switch (op.type) {
                        case "lag" -> {
                            final int k = op.k == null ? 1 : op.k;
                            for (int i = 1; i <= k; i++) {
                                final OutputColumn c = newColumn(def.name, Scope.sequence, op.type, base + (future ? "lead" : "lag") + i, ref.type(), computeAt);
                                c.coordinates.put("k", Integer.toString(i));
                                c.coordinates.put("field", canonicalOf(field)); addPastInput(c, field);
                                finishSequence(c, def, entity, window, filterRefs, reducedKey, op);
                            }
                        }
                        case "delta" -> {
                            final int k = op.k == null ? 1 : op.k;
                            final OutputColumn c = newColumn(def.name, Scope.sequence, op.type, base + "delta" + k, Schema.FieldType.FLOAT64, computeAt);
                            c.coordinates.put("k", Integer.toString(k));
                            c.coordinates.put("field", canonicalOf(field)); addPastInput(c, field);
                            finishSequence(c, def, entity, window, filterRefs, reducedKey, op);
                        }
                        case "trend" -> {
                            final int k = op.k == null ? 5 : op.k;
                            final OutputColumn c = newColumn(def.name, Scope.sequence, op.type, base + "trend" + k, Schema.FieldType.FLOAT64, computeAt);
                            c.coordinates.put("k", Integer.toString(k));
                            c.coordinates.put("field", canonicalOf(field)); addPastInput(c, field);
                            finishSequence(c, def, entity, window, filterRefs, reducedKey, op);
                        }
                        case "ewma" -> {
                            if (op.halflife.isEmpty()) {
                                diagnostics.error("sequence.ewma.halflife", loc, "ewma requires 'halflife'");
                                continue;
                            }
                            if (op.halflife.stream().anyMatch(h -> !(h > 0) || h.isInfinite())) {
                                diagnostics.error("sequence.ewma.halflife", loc, "halflife must be a positive number (events, days under decayBy: time, or ticks under a calendar clock): " + op.halflife);
                                continue;
                            }
                            final String decayBy = op.decayBy == null ? "events" : op.decayBy;
                            final Clock decayClock = Clock.BUILT_IN.contains(decayBy) ? null : clock(decayBy, loc, "decayBy");
                            if (!Clock.BUILT_IN.contains(decayBy) && decayClock == null) continue;
                            for (final Double h : op.halflife) {
                                final OutputColumn c = newColumn(def.name, Scope.sequence, op.type, base + "ewma" + number(h), Schema.FieldType.FLOAT64, computeAt);
                                // sugar: the order-0 exponential dynamics of the general form (one running state, O(1) per row)
                                c.coordinates.put("measure", Dynamics.Measure.exponential.name());
                                c.coordinates.put("order", "0");
                                c.coordinates.put("component", "0");
                                c.coordinates.put("halflife", plainNumber(h));
                                c.coordinates.put("decayBy", decayBy);
                                if (decayClock != null) c.clocks.put(decayBy, decayClock);
                                c.coordinates.put("field", canonicalOf(field)); addPastInput(c, field);
                                finishSequence(c, def, entity, window, filterRefs, reducedKey, op);
                            }
                        }
                        case "runLength" -> {
                            if (op.value == null) diagnostics.error("sequence.runLength.value", loc, "runLength requires 'value'");
                            else warnNeverMatches("sequence.runLength.value", loc, field, ref, op.value);
                            final OutputColumn c = newColumn(def.name, Scope.sequence, op.type, base + "runlength", Schema.FieldType.INT64, computeAt);
                            c.coordinates.put("value", String.valueOf(op.value));
                            c.coordinates.put("field", canonicalOf(field)); addPastInput(c, field);
                            finishSequence(c, def, entity, window, filterRefs, reducedKey, op);
                        }
                        case "aggregate" -> {
                            final List<String> funcs = op.funcs.isEmpty() ? List.of("count", "mean") : op.funcs;
                            for (final String func : funcs) {
                                final Schema.FieldType type = weightRefs == null ? OperatorCatalog.aggregateOutput(func, ref.type()) : OperatorCatalog.weightedAggregateOutput(func);
                                if (type == null && weightRefs != null && OperatorCatalog.aggregateOutput(func, ref.type()) != null) {
                                    diagnostics.error("sequence.weightBy.func", loc, "aggregate " + func + " has no weighted form (available under weightBy: " + String.join(" | ", OperatorCatalog.WEIGHTED_FUNCS) + ")");
                                    continue;
                                }
                                if (type == null) {
                                    diagnostics.error("sequence.aggregate.func", loc, "unknown aggregate func: " + func + " (available: " + OperatorCatalog.AVAILABLE_AGGREGATES + ")");
                                    continue;
                                }
                                if (!future && List.of("mean", "avg", "rate").contains(func) && isOutcomeLike(ref) && hintedBlocks.add("sequence.aggregate.encoding:" + def.name)) {
                                    // once per block: the same hint for every window × field × func would drown the report
                                    diagnostics.hint("sequence.aggregate.encoding", loc,
                                            "aggregate " + func + " over outcome field '" + field + "' (and other outcome means in this block) has no shrinkage; consider population encoding with a windowed keySet (spec section 4.3)");
                                }
                                final OutputColumn c = newColumn(def.name, Scope.sequence, op.type, base + func, type, computeAt);
                                // the declared func (a future window's first / last swap is the evaluator's: SequenceEvaluator.func)
                                c.coordinates.put("func", func);
                                c.coordinates.put("field", canonicalOf(field)); addPastInput(c, field);
                                addWeight(c, op, weightRefs);
                                finishSequence(c, def, entity, window, filterRefs, reducedKey, op);
                            }
                        }
                        case "regression" -> {
                            // field regressed against `against` over the window's events; lag pairs field with `against` k events earlier
                            final Ref onRef = resolve(op.against);
                            if (op.against == null || onRef == null) {
                                diagnostics.error("sequence.regression.against", loc, "regression requires 'against' (the explanatory field that '" + field + "' is regressed against)");
                                continue;
                            }
                            if (!OperatorCatalog.isNumeric(onRef.type())) {
                                diagnostics.error("sequence.regression.against", loc, "regression 'against' field '" + op.against + "' must be numeric (is " + (onRef.type() == null ? "unknown" : onRef.type().getType()) + ")");
                                continue;
                            }
                            final int lag = op.lag == null ? 0 : op.lag;
                            if (lag < 0) {
                                diagnostics.error("sequence.regression.lag", loc, "lag must be >= 0 (the events by which 'against' leads 'field'); swap field and against for the other direction: " + lag);
                                continue;
                            }
                            final List<String> funcs = op.funcs.isEmpty() ? List.of("beta", "corr") : op.funcs;
                            final String pair = base + "vs_" + displayName(op.against) + "_" + (lag == 0 ? "" : "lag" + lag + "_");
                            for (final String func : funcs) {
                                if (!OperatorCatalog.REGRESSION_FUNCS.contains(func)) {
                                    diagnostics.error("sequence.regression.func", loc, "unknown regression func: " + func + " (available: " + String.join(" | ", OperatorCatalog.REGRESSION_FUNCS) + ")");
                                    continue;
                                }
                                final OutputColumn c = newColumn(def.name, Scope.sequence, op.type, (op.as != null && fields.size() == 1 ? base : pair) + func, Schema.FieldType.FLOAT64, computeAt);
                                c.coordinates.put("func", func);
                                c.coordinates.put("field", canonicalOf(field)); addPastInput(c, field);
                                c.coordinates.put("against", canonicalOf(op.against)); addPastInput(c, op.against);
                                if (lag > 0) c.coordinates.put("lag", Integer.toString(lag));
                                finishSequence(c, def, entity, window, filterRefs, reducedKey, op);
                            }
                        }
                        case "fracdiff" -> {
                            if (op.d == null || !(op.d > 0) || op.d > 2) {
                                diagnostics.error("sequence.fracdiff.d", loc, "fracdiff requires 'd', the differencing order, in (0, 2]: " + op.d);
                                continue;
                            }
                            final int k = op.k == null ? 20 : op.k;
                            if (k < 2) {
                                diagnostics.error("sequence.fracdiff.k", loc, "k (the number of events the truncated filter reads) must be >= 2: " + k);
                                continue;
                            }
                            final OutputColumn c = newColumn(def.name, Scope.sequence, op.type, base + "fracdiff" + number(op.d), Schema.FieldType.FLOAT64, computeAt);
                            c.coordinates.put("d", Double.toString(op.d));
                            c.coordinates.put("k", Integer.toString(k));
                            c.coordinates.put("field", canonicalOf(field)); addPastInput(c, field);
                            finishSequence(c, def, entity, window, filterRefs, reducedKey, op);
                        }
                        case "barrier" -> {
                            if (op.up == null && op.down == null || op.up != null && !(op.up > 0) || op.down != null && !(op.down < 0)) {
                                diagnostics.error("sequence.barrier.levels", loc, "barrier requires 'up' > 0 and / or 'down' < 0 (relative moves from the current row's value): up=" + op.up + " down=" + op.down);
                                continue;
                            }
                            final OutputColumn c = newColumn(def.name, Scope.sequence, op.type, base + "barrier", Schema.FieldType.INT64, computeAt);
                            if (op.up != null) c.coordinates.put("up", Double.toString(op.up));
                            if (op.down != null) c.coordinates.put("down", Double.toString(op.down));
                            c.coordinates.put("field", canonicalOf(field));
                            addPastInput(c, field);
                            // the entry value: the current row's own value of the path
                            addSelfInput(c, field);
                            finishSequence(c, def, entity, window, filterRefs, reducedKey, op);
                        }
                        case "rating" -> expandRating(def, entity, window, reducedKey, op, field, fields.size() == 1, computeAt);
                        default -> diagnostics.error("sequence.op", loc, "unsupported sequence op: " + op.type);
                    }
                }
            }
        }
        if (form != null && form.compress() != null) expandCompress(def, form, computeAt);
    }

    /** The largest {@code tauPer} the {@code tauPerMillis} coordinate can carry (a millisecond count). */
    private static final Duration MAX_TAU_PER = Duration.ofMillis(Long.MAX_VALUE);

    /**
     * The sequence {@code rating} op: the block's entity is the rated player, {@code field} the outcome of a contest
     * and {@code context} the contest (the rows of one group at one event time). One running state per op × window,
     * shared by its readout columns ({@code stateKey}); the columns are pooled — replayed under the global key, or
     * under the field of a reduced window filter — because a contest moves every player it involves. Only the
     * unbounded window exists: a contest cannot be taken out of the ratings again.
     */
    private void expandRating(final FeatureDef def, final EntityDef entity, final Window window, final String reducedKey,
                              final Op op, final String field, final boolean singleField, final AvailableAt computeAt) {
        final String loc = def.location();
        if (window.maxAge != null || window.maxEvents != null || window.onCalendar() || (window.filter != null && reducedKey == null)) {
            if (hintedBlocks.add("sequence.rating.window:" + def.name + ":" + window.token() + ":" + window.filter)) {
                diagnostics.error("sequence.rating.window", loc, "rating reads every earlier contest of its pool (an update cannot be taken back): window " + window.token()
                        + (window.filter == null ? "" : " with filter '" + window.filter + "'") + " is not supported - declare no maxAge / maxEvents, and only a filter"
                        + " \"<field> = $self.<field>\" on a pre-event field (it splits the contests into independent pools); tau ages an old rating instead of a window");
            }
            return;
        }
        final ContextDef contest = op.context == null ? null : contexts.get(op.context);
        if (contest == null) {
            diagnostics.error("sequence.rating.context", loc, "rating requires 'context' referencing contexts[].name - the contest whose rows are rated against each other: " + op.context);
            return;
        }
        final String methodName = op.method == null ? Rating.Method.plackettLuce.name() : op.method;
        if (!Rating.METHODS.contains(methodName)) {
            diagnostics.error("sequence.rating.method", loc, "unknown rating method: " + op.method + " (available: " + String.join(" | ", Rating.METHODS) + ")");
            return;
        }
        final Rating.Method method = Rating.Method.valueOf(methodName);
        final boolean elo = method == Rating.Method.elo;
        if (op.order != null && !Rating.ORDERS.contains(op.order)) {
            diagnostics.error("sequence.rating.order", loc, "rating order must be ascending (a smaller outcome is better: a rank) or descending (a larger one: a score): " + op.order);
            return;
        }
        boolean valid = true;
        final List<String> foreign = new ArrayList<>();
        if (elo) {
            if (op.sigma != null) foreign.add("sigma");
            if (op.beta != null) foreign.add("beta");
            if (op.tau != null) foreign.add("tau");
            if (op.tauPer != null) foreign.add("tauPer");
        } else {
            if (op.kFactor != null) foreign.add("kFactor");
            if (op.scale != null) foreign.add("scale");
        }
        if (!foreign.isEmpty()) {
            diagnostics.error("sequence.rating.parameter", loc, foreign + (elo ? " are parameters of bradleyTerry / plackettLuce / gaussian: elo takes mu, kFactor, scale"
                    : " are elo parameters: " + methodName + " takes mu, sigma, beta, tau, tauPer"));
            valid = false;
        }
        final boolean pairwise = method == Rating.Method.bradleyTerry;
        if (op.pairs != null && !pairwise) {
            // elo already shares kFactor over the opponents, and plackettLuce has no pairs: it reads the whole ranking
            diagnostics.error("sequence.rating.parameter", loc, "pairs chooses the opponents of a bradleyTerry update (" + String.join(" | ", Rating.PAIRS) + "): " + methodName + " has none" + (method == Rating.Method.gaussian ? " (gaussian conditions on the whole contest at once)" : ""));
            valid = false;
        } else if (op.pairs != null && !Rating.PAIRS.contains(op.pairs)) {
            diagnostics.error("sequence.rating.parameter", loc, "unknown pairs: " + op.pairs + " (available: " + String.join(" | ", Rating.PAIRS) + ")");
            valid = false;
        }
        // the coordinate is a millisecond count, so the DURATION being non-zero is not enough: one that rounds to 0 ms
        // (or does not fit in millis at all, where toMillis() would throw instead of reporting) would silently fall
        // back to the per-contest drift — with a tau this very check makes the spec size for a whole period
        final long tauPerMillis = op.tauPer == null || op.tauPer.compareTo(MAX_TAU_PER) > 0 ? 0L : op.tauPer.toMillis();
        if (op.tauPer != null && tauPerMillis <= 0) {
            diagnostics.error("sequence.rating.parameter", loc, "tauPer must be a positive duration of at least one millisecond,"
                    + " and no longer than a millisecond count holds (the time tau is the drift of): " + op.tauPer);
            valid = false;
        } else if (op.tauPer != null && !elo && op.tau == null) {
            // the default tau (sigma / 100) is the drift of ONE CONTEST: spread over a period it would age nothing
            diagnostics.error("sequence.rating.parameter", loc, "tauPer makes tau the drift per " + op.tauPer + " of absence (variance += tau^2 * delta_t / tauPer): declare tau with it"
                    + " - the default (sigma / 100) is sized for one contest");
            valid = false;
        }
        if (op.mu != null && !Double.isFinite(op.mu)
                || op.sigma != null && !(op.sigma > 0 && Double.isFinite(op.sigma)) || op.beta != null && !(op.beta > 0 && Double.isFinite(op.beta))
                || op.tau != null && !(op.tau >= 0 && Double.isFinite(op.tau))
                || op.kFactor != null && !(op.kFactor > 0 && Double.isFinite(op.kFactor)) || op.scale != null && !(op.scale > 0 && Double.isFinite(op.scale))) {
            diagnostics.error("sequence.rating.parameter", loc, "rating parameters must be finite, with sigma / beta / kFactor / scale > 0 and tau >= 0: mu=" + op.mu
                    + " sigma=" + op.sigma + " beta=" + op.beta + " tau=" + op.tau + " kFactor=" + op.kFactor + " scale=" + op.scale);
            valid = false;
        }
        final List<String> funcs = op.funcs.isEmpty() ? (elo ? List.of("mu") : List.of("mu", "sigma")) : op.funcs;
        for (final String func : funcs) {
            if (!Rating.FUNCS.contains(func) || (elo && "sigma".equals(func))) {
                diagnostics.error("sequence.rating.func", loc, (Rating.FUNCS.contains(func) ? "elo keeps no uncertainty: sigma is a readout of bradleyTerry / plackettLuce / gaussian"
                        : "unknown rating func: " + func) + " (available: " + String.join(" | ", Rating.FUNCS) + ")");
                valid = false;
            }
        }
        if (!validateRatingTeam(def, entity, op, elo, singleField)) valid = false;
        if (!valid) return;
        // a margin model has no scale of its own: sigma (how far strengths spread) and beta (the noise of one outcome) are
        // in the outcome's units and must be declared - the defaults derive from the prior mu (a rating's level)
        if (method == Rating.Method.gaussian && (op.sigma == null || op.beta == null)) {
            final List<String> missing = new ArrayList<>();
            if (op.sigma == null) missing.add("sigma");
            if (op.beta == null) missing.add("beta");
            diagnostics.error("sequence.rating.gaussian.units", loc, "gaussian" + (op.as == null ? "" : " '" + op.as + "'")
                    + " reads the outcome '" + field + "' as a margin, so its prior and noise are in the outcome's units and have no default:"
                    + " declare " + String.join(" and ", missing) + " (mu = a typical outcome, 0 for a standardised margin; sigma = how far strengths"
                    + " spread; beta = the noise of one outcome, e.g. mu: 0, sigma: 1, beta: 0.5)");
            return;
        }

        final double mu = op.mu != null ? op.mu : Rating.defaultMu(method);
        final double sigma = op.sigma != null ? op.sigma : Rating.defaultSigma(mu);
        if (!elo && !(sigma > 0)) {
            diagnostics.error("sequence.rating.parameter", loc, "the default sigma is |mu| / 3: declare sigma when mu is 0");
            return;
        }
        // the coordinates every readout column of this op shares: they define the one running state behind them
        final Map<String, String> shared = new LinkedHashMap<>();
        shared.put("method", methodName);
        shared.put("order", op.order == null ? "ascending" : op.order);
        shared.put("mu", Double.toString(mu));
        if (elo) {
            shared.put("kFactor", Double.toString(op.kFactor != null ? op.kFactor : Rating.DEFAULT_K_FACTOR));
            shared.put("scale", Double.toString(op.scale != null ? op.scale : Rating.DEFAULT_SCALE));
        } else {
            shared.put("sigma", Double.toString(sigma));
            shared.put("beta", Double.toString(op.beta != null ? op.beta : Rating.defaultBeta(sigma)));
            shared.put("tau", Double.toString(op.tau != null ? op.tau : Rating.defaultTau(sigma)));
            if (tauPerMillis > 0) shared.put("tauPerMillis", Long.toString(tauPerMillis));
            if (pairwise && op.pairs != null) shared.put("pairs", op.pairs);
        }
        shared.put("context", contest.name());
        // the state snapshot (RatingSnapshot): the block's own fit.artifact - the top-level one is not inherited, a
        // snapshot is an explicit choice of the block (a spec whose encodings persist artifacts would otherwise start
        // snapshotting its ratings unasked); outside the plan hash
        final FeatureSpec.FitSpec blockFit = new FeatureSpec.FitSpec();
        FeatureSpec.FitSpec.parseArtifact(parseJsonObject(def.fitJson), blockFit);
        final String artifactUri = blockFit.artifactUri;
        if (artifactUri != null) {
            shared.put("artifact", artifactUri);
            if (blockFit.refit) shared.put("artifactRefit", "true");
            if (blockFit.artifactRequired) shared.put("artifactRequired", "true");
        }
        // what a past row brings to its contest: the outcome, the player and the contest it belongs to
        shared.put("field", canonicalOf(field));
        final List<String> playerKeys = new ArrayList<>(), contestKeys = new ArrayList<>();
        for (final String key : entity.keys()) playerKeys.add(canonicalOf(key));
        for (final String key : contest.keys()) contestKeys.add(canonicalOf(key));
        shared.put("playerKeys", String.join(",", playerKeys));
        shared.put("contestKeys", String.join(",", contestKeys));
        // a team: the block's entity and the `with` entities of the same row, each with a prior and a drift of its own
        // (the op's unless declared). Without `with` neither coordinate is written — the rating, its state keys and its
        // columns are those of a rating of players
        final List<EntityDef> teamEntities = new ArrayList<>();
        if (!op.with.isEmpty()) {
            final double tau = op.tau != null ? op.tau : Rating.defaultTau(sigma);
            final List<Rating.Member> members = new ArrayList<>();
            for (final FeatureSpec.TeamMember m : op.with) {
                final EntityDef member = entities.get(m.entity);
                final List<String> keys = new ArrayList<>();
                for (final String key : member.keys()) keys.add(canonicalOf(key));
                members.add(new Rating.Member(member.name(), keys, m.mu != null ? m.mu : mu, m.sigma != null ? m.sigma : sigma, m.tau != null ? m.tau : tau));
                teamEntities.add(member);
            }
            shared.put("teamPool", entity.name());
            shared.put("teamMembers", Rating.encodeMembers(members));
            // the contests per team are one entry per distinct team for the whole replay: kept only when read
            if (op.team.contains("count")) shared.put("teamCounts", "true");
        }

        // `as` names the field segment; without it the op is part of the name (the outcome field may feed other ops)
        final String segment = op.as != null && singleField ? op.as : displayName(field) + "_rating";
        final String stateKey = def.name + "_" + window.token() + "_" + segment;
        // the readout columns of one op share the running state under `stateKey`; two ops that resolve to the same
        // segment with different parameters would silently share one replay whenever their funcs do not collide
        final String previous = ratingStates.putIfAbsent(stateKey, shared.toString());
        if (previous == null && artifactUri != null) {
            diagnostics.info("sequence.rating.artifact", loc, "rating '" + segment + "' snapshots the state of every pool to " + artifactUri + "/"
                    + (spec.fit.artifactId != null ? spec.fit.artifactId : "<planHash>") + "/" + def.name
                    + ".rating/ after the replay; a run whose input starts after a pool's last folded contest continues from its snapshot (the serving"
                    + " form: the input holds the rows to serve and every contest after that time), a run whose input reaches back to it replays"
                    + " the pool from scratch and rewrites it" + (shared.containsKey("artifactRefit") ? "; refit: true - this run replays every pool from scratch"
                    : "") + (shared.containsKey("artifactRequired") ? "; require: true - a pool without a snapshot fails instead of replaying from the prior"
                    : "; a serving run over a short input without a snapshot would replay from the prior and write that as the snapshot: set require: true"));
        }
        if (previous != null && !previous.equals(shared.toString())) {
            diagnostics.error("sequence.rating.as", loc, "two rating ops of block '" + def.name + "' resolve to the same column segment '"
                    + segment + "' with different parameters (" + previous + " vs " + shared + "): they would share one running state - name them apart with as:");
            return;
        }
        // the readouts: the rated player's (index 0, the names a rating of players has), then each member's under its
        // entity name, then the team's. Every column folds the same contests, so every one carries all the members' keys
        // — as past inputs, and of the current row too: the columns of one op share ONE fold pointer (stateKey), so
        // they must share one availability contract. A column whose self side were later than the others' would be
        // classified apart (no window shift, or a runtime filter) and advance the shared state past their near edge
        record Readout(String segment, String func, int member) {}
        final List<Readout> readouts = new ArrayList<>();
        for (final String func : funcs) readouts.add(new Readout("", func, 0));
        for (int j = 0; j < teamEntities.size(); j++) {
            for (final String func : funcs) readouts.add(new Readout(teamEntities.get(j).name() + "_", func, j + 1));
        }
        for (final String func : op.team) readouts.add(new Readout("team_", func, -1));
        for (final Readout r : readouts) {
            final OutputColumn c = newColumn(def.name, Scope.sequence, "rating", stateKey + "_" + r.segment() + r.func(),
                    "count".equals(r.func()) ? Schema.FieldType.INT64 : Schema.FieldType.FLOAT64, computeAt);
            c.coordinates.put("func", r.func());
            c.coordinates.putAll(shared);
            c.coordinates.put("stateKey", stateKey);
            if (r.member() < 0) {
                c.coordinates.put("readout", "team");
            } else if (r.member() > 0) {
                c.coordinates.put("readout", "member");
                c.coordinates.put("member", teamEntities.get(r.member() - 1).name());
                c.coordinates.put("memberIndex", Integer.toString(r.member()));
            }
            addPastInput(c, field);
            for (final String key : entity.keys()) addPastInput(c, key);
            for (final String key : contest.keys()) addPastInput(c, key);
            for (final EntityDef member : teamEntities) {
                for (final String key : member.keys()) {
                    addPastInput(c, key);
                    addSelfInput(c, key);
                }
            }
            finishSequence(c, def, entity, window, null, reducedKey, op, List.of(), true);
        }
        if (!teamEntities.isEmpty() && hintedBlocks.add("sequence.rating.with:" + stateKey)) {
            final List<String> names = new ArrayList<>(List.of(entity.name()));
            for (final EntityDef member : teamEntities) names.add(member.name());
            diagnostics.info("sequence.rating.with", loc, "rating '" + segment + "' rates a row as the team " + String.join(" + ", names)
                    + ": its strength is the sum of the members' ratings and a contest's change is shared among them by their part of the team's variance"
                    + " (a well-known member hardly moves, an uncertain one takes the update). The members' levels are identified up to a shift between the"
                    + " entities - read a member relative to its contest (a context block over the column) or to its pool (func z), or the team's sum"
                    + " (team: [mu, sigma, count, deviation])");
        }
    }

    /**
     * The separators no entity or key name of a team may hold: those of the {@code teamMembers} coordinate
     * ({@link Rating#encodeMembers}) and those of a state key ({@code U+0001} between a pool and its key,
     * {@code U+0002} between the members of a team), which {@code Rating.withTeam} rejects by throwing.
     */
    private static final Pattern TEAM_SEPARATORS = Pattern.compile("[|;,\\u0001\\u0002]");

    /**
     * {@code with} / {@code team} of a rating op (code {@code sequence.rating.with}): the members are entities of the
     * spec other than the block's, each once; a team needs the variance the Bayesian methods keep (not elo), and a
     * name of its own — its columns are the op's segment plus the member, which the default segment
     * {@code <field>_rating} makes both long and ambiguous.
     */
    private boolean validateRatingTeam(final FeatureDef def, final EntityDef entity, final Op op, final boolean elo, final boolean singleField) {
        final String loc = def.location();
        boolean valid = true;
        if (op.withInvalid != null) {
            diagnostics.error("sequence.rating.with", loc, op.withInvalid);
            valid = false;
        }
        if (op.with.isEmpty()) {
            if (!op.team.isEmpty()) {
                diagnostics.error("sequence.rating.with", loc, "team: " + op.team + " reads the rating of a team: declare its members with with: [<entity>, ...]");
                valid = false;
            }
            return valid;
        }
        if (elo) {
            diagnostics.error("sequence.rating.with", loc, "a team's change is shared among its members by their variance, which elo does not keep: rate a team with plackettLuce, bradleyTerry or gaussian");
            valid = false;
        }
        if (op.as == null || !singleField) {
            diagnostics.error("sequence.rating.with", loc, "a rating with a team needs as: (and one field): its columns are <block>_<window>_<as>_<func> for the "
                    + entity.name() + ", <block>_<window>_<as>_<member>_<func> for a member and <block>_<window>_<as>_team_<func> for the team");
            valid = false;
        }
        final Set<String> seen = new HashSet<>();
        // the rated player's pool is the block's entity name: checked once, not once per member
        if (!validTeamName(entity.name(), loc)) valid = false;
        for (final FeatureSpec.TeamMember m : op.with) {
            final EntityDef member = m.entity == null ? null : entities.get(m.entity);
            if (member == null) {
                diagnostics.error("sequence.rating.with", loc, "with names entities[].name - the other entities of the row rated with " + entity.name() + ": " + m.entity
                        + " (available: " + entities.keySet() + ")");
                valid = false;
                continue;
            }
            if (member.name().equals(entity.name())) {
                diagnostics.error("sequence.rating.with", loc, "with lists the OTHER members of the team: " + entity.name() + " is the block's entity, the rated player itself");
                valid = false;
            } else if (!seen.add(member.name())) {
                diagnostics.error("sequence.rating.with", loc, "with names " + member.name() + " twice");
                valid = false;
            }
            if ("team".equals(member.name())) {
                diagnostics.error("sequence.rating.with", loc, "an entity named 'team' cannot be a member: <as>_team_<func> are the columns of the whole team");
                valid = false;
            }
            if (!validTeamName(member.name(), loc)) valid = false;
            for (final String key : member.keys()) if (!validTeamName(key, loc)) valid = false;
            if (!m.unknown.isEmpty()) {
                diagnostics.error("sequence.rating.with", loc, "unknown key(s) " + m.unknown + " of member " + member.name()
                        + " (accepted: " + String.join(", ", FeatureSpec.TEAM_MEMBER_KEYS) + ")");
                valid = false;
            }
            if (m.mu != null && !Double.isFinite(m.mu) || m.sigma != null && !(m.sigma > 0 && Double.isFinite(m.sigma)) || m.tau != null && !(m.tau >= 0 && Double.isFinite(m.tau))) {
                diagnostics.error("sequence.rating.with", loc, "member " + member.name() + " needs a finite mu, sigma > 0 and tau >= 0: mu=" + m.mu + " sigma=" + m.sigma + " tau=" + m.tau);
                valid = false;
            }
        }
        for (final String func : op.team) {
            if (!Rating.TEAM_FUNCS.contains(func)) {
                diagnostics.error("sequence.rating.with", loc, "unknown team readout: " + func + " (available: " + String.join(" | ", Rating.TEAM_FUNCS)
                        + " - mu is the sum of the members' ratings, sigma the uncertainty of that sum, count the contests this very team ran,"
                        + " deviation the sum net of the members' priors)");
                valid = false;
            }
        }
        return valid;
    }

    /** Whether a name of a team (an entity's, or one of its key fields') is free of the separators the state keys use. */
    private boolean validTeamName(final String name, final String loc) {
        if (!TEAM_SEPARATORS.matcher(name).find()) return true;
        diagnostics.error("sequence.rating.with", loc, "a team's entity and key names cannot hold '|', ';', ','"
                + " or the state key separators U+0001 / U+0002 (two members would meet on one state key): " + name);
        return false;
    }

    /** The rating ops already expanded, by their {@code stateKey}: the coordinates behind one running state. */
    private final Map<String, String> ratingStates = new HashMap<>();

    /** The joint fits already expanded, by their id: the lattice and shrinkage behind one solve. */
    private final Map<String, String> jointFits = new HashMap<>();

    /**
     * Upper bound on the component columns one general-form block emits: {@code lti} windows × halflifes × channels ×
     * components, {@code bilinear} windows × the Lyndon words of the channels up to the depth.
     */
    static final int MAX_DYNAMICS_COLUMNS = 64;

    /** The parameters {@code compress: {svd: {...}}} forwards to a population svd block (the keys {@code type: svd} reads). */
    private static final List<String> SVD_KEYS = List.of("rank", "center", "standardize", "outputs", "fit");

    /**
     * The validated general form of a sequence block: channels (a null reference = the constant time channel), the
     * dynamics ({@code lti}: measure / order / halflifes / period; {@code bilinear}: the log-signature depth) and the
     * component columns the windows emitted, which {@code compress} reads.
     */
    private record GeneralForm(String family, List<Channel> channels, Dynamics.Measure measure, int order, List<Double> halflifes, Double period,
                               String decayBy, int depth, boolean timeAugment, JsonObject compress, List<OutputColumn> components) {
        boolean bilinear() {
            return "bilinear".equals(family);
        }
    }

    /** A lift channel: the column it reads ({@code null} = the constant time channel) and its name segment. */
    private record Channel(String reference, String name) {}

    /**
     * Validates {@code lift} / {@code summarize} / {@code compress} once per block (expressions are desugared once, not
     * per window) and returns the form, or null after reporting.
     */
    private GeneralForm generalForm(final FeatureDef def, final AvailableAt computeAt) {
        final String loc = def.location();
        boolean valid = true;
        if (!def.summarize || def.dynamics == null) {
            diagnostics.error("sequence.summarize", loc, "the general form requires summarize: {dynamics: {family: lti | bilinear, ...}}");
            return null;
        }
        final DynamicsSpec d = def.dynamics;
        if (!"lti".equals(d.family) && !"bilinear".equals(d.family)) {
            diagnostics.error("sequence.dynamics.family", loc, d.family == null
                    ? "summarize.dynamics requires 'family' (available: lti | bilinear)"
                    : "dynamics family '" + d.family + "' is " + ("probabilistic".equals(d.family) ? "not implemented yet" : "unknown") + " (available: lti | bilinear)");
            return null;
        }
        final boolean bilinear = "bilinear".equals(d.family);
        if (!d.unknown.isEmpty()) {
            diagnostics.error("sequence.dynamics.parameter", loc, "unknown " + d.family + " parameter(s) " + d.unknown + " (accepted: "
                    + (bilinear ? "type, depth, decayBy" : "measure, order, halflife, period, decayBy") + ")");
            valid = false;
        }
        Dynamics.Measure measure = null;
        int order = 0;
        int depth = 0;
        if (bilinear) {
            final List<String> foreign = new ArrayList<>();
            if (d.measure != null) foreign.add("measure");
            if (d.order != null) foreign.add("order");
            if (!d.halflife.isEmpty()) foreign.add("halflife");
            if (d.period != null) foreign.add("period");
            if (!foreign.isEmpty()) {
                diagnostics.error("sequence.dynamics.parameter", loc, foreign + " are lti parameters: a bilinear summary takes type, depth, decayBy");
                valid = false;
            }
            if (d.type != null && !"logsignature".equals(d.type)) {
                diagnostics.error("sequence.dynamics.type", loc, "bilinear type must be logsignature: " + d.type);
                valid = false;
            }
            depth = d.depth == null ? 2 : d.depth;
            if (depth < 1 || depth > Signature.MAX_DEPTH) {
                diagnostics.error("sequence.dynamics.depth", loc, "logsignature depth must be in [1, " + Signature.MAX_DEPTH + "]: " + depth);
                valid = false;
            }
        } else {
            if (d.type != null || d.depth != null) {
                diagnostics.error("sequence.dynamics.parameter", loc, "type / depth are bilinear parameters: an lti summary takes measure, order, halflife, period, decayBy");
                valid = false;
            }
            try {
                measure = Dynamics.Measure.valueOf(String.valueOf(d.measure));
            } catch (final IllegalArgumentException e) {
                diagnostics.error("sequence.dynamics.measure", loc, "lti requires 'measure': exponential | legendre | fourier (got " + d.measure + ")");
                return null;
            }
            order = d.order != null ? d.order : switch (measure) {
                case exponential -> 0;
                case fourier -> 1;
                case legendre -> 3;
            };
            final int minOrder = measure == Dynamics.Measure.fourier ? 1 : 0;
            if (order < minOrder || order > Dynamics.maxOrder(measure)) {
                diagnostics.error("sequence.dynamics.order", loc, measure + " order must be in [" + minOrder + ", " + Dynamics.maxOrder(measure) + "]: " + order);
                valid = false;
            }
            if (d.halflife.stream().anyMatch(h -> !(h > 0) || h.isInfinite())) {
                diagnostics.error("sequence.dynamics.halflife", loc, "halflife must be a positive number (clock units): " + d.halflife);
                valid = false;
            } else if (measure == Dynamics.Measure.exponential && d.halflife.isEmpty()) {
                diagnostics.error("sequence.dynamics.halflife", loc, "the exponential measure requires 'halflife' (clock units; one state per value)");
                valid = false;
            } else if (measure == Dynamics.Measure.legendre && !d.halflife.isEmpty()) {
                diagnostics.error("sequence.dynamics.halflife", loc, "legendre has no halflife: its measure is uniform over the window's span");
                valid = false;
            }
            if (measure == Dynamics.Measure.fourier && (d.period == null || !(d.period > 0) || d.period.isInfinite())) {
                diagnostics.error("sequence.dynamics.period", loc, "fourier requires a positive 'period' (clock units) for its first harmonic: " + d.period);
                valid = false;
            } else if (measure != Dynamics.Measure.fourier && d.period != null) {
                diagnostics.error("sequence.dynamics.period", loc, "period is a fourier parameter (measure " + measure + ")");
                valid = false;
            }
        }
        final String decayBy = d.decayBy == null ? "events" : d.decayBy;
        if (!Clock.BUILT_IN.contains(decayBy) && clock(decayBy, loc, "decayBy") == null) valid = false;
        if (def.lift == null || def.lift.fields.isEmpty() && def.lift.exprs.isEmpty() && !def.lift.timeAugment) {
            diagnostics.error("sequence.lift", loc, "the general form requires lift: {fields: [...]} (and / or exprs, timeAugment)");
            return null;
        }
        final List<Channel> channels = new ArrayList<>();
        for (final String field : def.lift.fields) {
            final Ref ref = resolve(field);
            if (ref == null) {
                // unreachable through the expansion loop (blockReferences waits for it), reported rather than dropped
                diagnostics.error("reference.unknown", loc, "unknown lift channel: " + field);
                valid = false;
                continue;
            }
            if (!OperatorCatalog.isNumeric(ref.type()) && (ref.type() == null || ref.type().getType() != Schema.Type.bool)) {
                diagnostics.error("sequence.lift.type", loc, "lift channel '" + field + "' must be numeric (is " + (ref.type() == null ? "unknown" : ref.type().getType()) + ")");
                valid = false;
                continue;
            }
            channels.add(new Channel(field, displayName(field)));
        }
        for (final LiftExpr expr : def.lift.exprs) {
            final OutputColumn anonymous = desugarExpression(def, expr.expr(), computeAt);
            if (anonymous == null) valid = false;
            else channels.add(new Channel(anonymous.canonicalName, expr.as() != null ? expr.as() : displayName(anonymous.canonicalName)));
        }
        if (!def.lift.exprs.isEmpty() && def.lift.exprs.stream().anyMatch(e -> e.as() == null)) {
            diagnostics.info("sequence.lift.anonymous", loc, "an unnamed lift expression is named by the spec-wide anonymous counter ("
                    + def.name + "__e{n}), which renumbers when an earlier expression is added or removed: name it with {expr: \"...\", as: name}");
        }
        final int perChannel = bilinear ? 0 : Dynamics.dimension(measure, order);
        // bilinear carries the time channel inside the one joint path (Signature), so it adds no constant channel here
        if (def.lift.timeAugment && !bilinear) {
            if (perChannel == 1) {
                diagnostics.warning("sequence.lift.timeAugment", loc, "timeAugment adds no column at order 0: the constant channel's only component is 1");
            }
            channels.add(new Channel(null, "time"));
            // the time channel reads no field: it takes the most delayed channel's availability, so its window is the
            // one the value channels see (a shifted window) rather than every event the entity had. Inside that window
            // it still describes every row: the constant 1 is present on all of them, where a value channel is an
            // event only on the rows that carry its value (Dynamics: a row without a value is no event of the channel)
            final Set<AvailableAt> availabilities = new LinkedHashSet<>();
            for (final Channel channel : channels) {
                final Ref ref = channel.reference() == null ? null : resolve(channel.reference());
                if (ref != null) availabilities.add(ref.availableAt() == null ? AvailableAt.atEventTime() : ref.availableAt());
            }
            if (availabilities.size() > 1) {
                diagnostics.info("sequence.lift.align", loc, "the lift channels are available at different times " + availabilities
                        + ": the time channel follows the latest one (its window is shifted like that channel's)");
            }
        }
        final Set<String> names = new HashSet<>();
        for (final Channel channel : channels) {
            if (!names.add(channel.name())) {
                diagnostics.error("sequence.lift.name", loc, "two lift channels are named '" + channel.name() + "': set a distinct 'as' on the expression");
                valid = false;
            }
        }
        final int columns;
        if (bilinear) {
            // one joint path over every channel (+ time): a column per Lyndon word up to the depth
            final int letters = channels.size() + (def.lift.timeAugment ? 1 : 0);
            if (letters < 2) {
                diagnostics.warning("sequence.dynamics.channels", loc, "a log-signature of one channel is its total increment only: lift two channels or add timeAugment");
            }
            if (letters > 26) {
                diagnostics.error("sequence.dynamics.size", loc, "a log-signature takes at most 26 channels (words are named by letters): " + letters);
                valid = false;
            }
            columns = Math.max(1, def.windows.size()) * Signature.lyndonWords(Math.min(letters, 26), Math.max(1, Math.min(depth, Signature.MAX_DEPTH))).size();
        } else {
            columns = Math.max(1, def.windows.size()) * Math.max(1, d.halflife.size())
                    * (perChannel * channels.size() - (def.lift.timeAugment ? 1 : 0));
        }
        if (columns > MAX_DYNAMICS_COLUMNS) {
            diagnostics.error("sequence.dynamics.size", loc, "the block would emit " + columns + " component columns (" + (bilinear
                    ? "windows x the Lyndon words of the channels up to depth " + depth : "windows x halflifes x channels x " + perChannel + " components")
                    + "), over " + MAX_DYNAMICS_COLUMNS + ": lower the " + (bilinear ? "depth" : "order") + " or split the channels over several blocks");
            valid = false;
        }
        JsonObject compress = null;
        if (def.compress) {
            compress = parseJsonObject(def.compressJson);
            if (compress == null || !compress.has("svd") || !compress.get("svd").isJsonObject()) {
                diagnostics.error("sequence.compress", loc, "compress must be {svd: {rank, center, standardize, outputs, fit}, keep: false}");
                valid = false;
            } else {
                for (final String key : compress.keySet()) {
                    if (!List.of("svd", "keep").contains(key)) {
                        diagnostics.error("sequence.compress", loc, "unknown compress key '" + key + "' (accepted: svd, keep)");
                        valid = false;
                    }
                }
                // a misspelled svd parameter would silently take its default (rank especially), as everywhere else
                for (final String key : compress.getAsJsonObject("svd").keySet()) {
                    if (!SVD_KEYS.contains(key)) {
                        diagnostics.error("sequence.compress", loc, "unknown compress.svd key '" + key + "' (accepted: " + String.join(", ", SVD_KEYS) + ")");
                        valid = false;
                    }
                }
            }
        }
        return valid ? new GeneralForm(d.family, channels, measure, order, d.halflife, d.period, decayBy, depth, def.lift.timeAugment,
                compress, new ArrayList<>()) : null;
    }

    /**
     * One window of a general-form block: per channel and halflife one running state (the {@code stateKey} its component
     * columns share) and one FLOAT64 column per component, {@code {block}_{window}_{channel}_{measure}_{component}}.
     * The constant time channel skips its component 0 (always 1). A {@code bilinear} summary is one path over every
     * channel: one state per window and a column per Lyndon word, {@code {block}_{window}_logsig_{word}}.
     */
    private void expandDynamics(final FeatureDef def, final EntityDef entity, final Window window, final References filterRefs,
                                final String reducedKey, final GeneralForm form, final AvailableAt computeAt) {
        if (form.bilinear()) {
            expandSignature(def, entity, window, filterRefs, reducedKey, form, computeAt);
            return;
        }
        final List<Double> halflifes = form.halflifes().isEmpty() ? Collections.singletonList(null) : form.halflifes();
        final List<String> valueChannels = form.channels().stream().map(Channel::reference).filter(Objects::nonNull).toList();
        for (final Channel ch : form.channels()) {
            final String channel = ch.reference();
            for (final Double h : halflifes) {
                final String measureToken = switch (form.measure()) {
                    case exponential -> "exp" + number(h);
                    case legendre -> "leg";
                    case fourier -> "fourier" + number(form.period()) + (h == null ? "" : "h" + number(h));
                };
                final String stateKey = def.name + "_" + window.token() + "_" + ch.name() + "_" + measureToken;
                for (int component = channel == null ? 1 : 0; component < Dynamics.dimension(form.measure(), form.order()); component++) {
                    final OutputColumn c = newColumn(def.name, Scope.sequence, "dynamics",
                            stateKey + "_" + Dynamics.componentName(form.measure(), component), Schema.FieldType.FLOAT64, computeAt);
                    c.coordinates.put("family", "lti");
                    c.coordinates.put("measure", form.measure().name());
                    c.coordinates.put("order", Integer.toString(form.order()));
                    c.coordinates.put("component", Integer.toString(component));
                    if (h != null) c.coordinates.put("halflife", plainNumber(h));
                    if (form.period() != null) c.coordinates.put("period", plainNumber(form.period()));
                    c.coordinates.put("decayBy", form.decayBy());
                    if (clocks.containsKey(form.decayBy())) c.clocks.put(form.decayBy(), clocks.get(form.decayBy()));
                    c.coordinates.put("stateKey", stateKey);
                    if (channel != null) {
                        c.coordinates.put("field", canonicalOf(channel));
                        addPastInput(c, channel);
                    }
                    finishSequence(c, def, entity, window, filterRefs, reducedKey, null, channel == null ? valueChannels : List.of());
                    form.components().add(c);
                }
            }
        }
    }

    /** One window of a {@code bilinear} block: the log-signature of the joint path, a column per Lyndon word (channels a, b, …). */
    private void expandSignature(final FeatureDef def, final EntityDef entity, final Window window, final References filterRefs,
                                 final String reducedKey, final GeneralForm form, final AvailableAt computeAt) {
        final List<String> fields = form.channels().stream().map(ch -> canonicalOf(ch.reference())).toList();
        final int letters = fields.size() + (form.timeAugment() ? 1 : 0);
        final List<int[]> words = Signature.lyndonWords(letters, form.depth());
        final String stateKey = def.name + "_" + window.token() + "_logsig";
        if (hintedBlocks.add(def.name + "#logsigLetters")) {
            final List<String> legend = new ArrayList<>();
            for (int i = 0; i < form.channels().size(); i++) legend.add((char) ('a' + i) + "=" + form.channels().get(i).name());
            if (form.timeAugment()) legend.add((char) ('a' + fields.size()) + "=time (" + form.decayBy() + ")");
            diagnostics.info("sequence.dynamics.logsignature", def.location(), "log-signature columns are named by Lyndon words over the channels " + legend
                    + " (e.g. _ab = the Levy area of a and b); an unbounded window folds each event in, a bounded one re-reads its events");
        }
        for (int w = 0; w < words.size(); w++) {
            final OutputColumn c = newColumn(def.name, Scope.sequence, "dynamics", stateKey + "_" + Signature.wordName(words.get(w)), Schema.FieldType.FLOAT64, computeAt);
            c.coordinates.put("family", "bilinear");
            c.coordinates.put("type", "logsignature");
            c.coordinates.put("depth", Integer.toString(form.depth()));
            c.coordinates.put("word", Integer.toString(w));
            c.coordinates.put("fields", String.join(",", fields));
            if (form.timeAugment()) c.coordinates.put("timeAugment", "true");
            c.coordinates.put("decayBy", form.decayBy());
            if (clocks.containsKey(form.decayBy())) c.clocks.put(form.decayBy(), clocks.get(form.decayBy()));
            c.coordinates.put("stateKey", stateKey);
            for (final Channel ch : form.channels()) addPastInput(c, ch.reference());
            finishSequence(c, def, entity, window, filterRefs, reducedKey, null);
            form.components().add(c);
        }
    }

    /**
     * {@code compress: {svd: {...}}}: a population svd block {@code {block}_svd} over every component column of the
     * general form (all windows), fitted like any svd block (its own {@code fit}, else the top level). The components
     * become intermediate unless {@code keep: true}.
     */
    private void expandCompress(final FeatureDef def, final GeneralForm form, final AvailableAt computeAt) {
        final JsonObject svd = form.compress().getAsJsonObject("svd");
        final FeatureDef compress = new FeatureDef();
        compress.name = def.name + "_svd";
        compress.scope = Scope.population;
        compress.type = "svd";
        compress.inputs = form.components().stream().map(c -> c.canonicalName).toList();
        compress.rank = SourceContract.Json.integer(svd, "rank");
        compress.center = svd.has("center") && !svd.get("center").isJsonNull() ? SourceContract.Json.bool(svd, "center", true) : null;
        compress.standardize = SourceContract.Json.bool(svd, "standardize", false);
        compress.outputs = SourceContract.Json.strings(svd, "outputs");
        compress.fitJson = svd.has("fit") && svd.get("fit").isJsonObject() ? svd.get("fit").toString() : null;
        compress.validFor = def.validFor;
        if (compress.inputs.size() < 2) {
            diagnostics.error("sequence.compress", def.location(), "compress needs two or more component columns (the block emits " + compress.inputs.size() + ")");
            return;
        }
        expandSvd(compress, computeAt);
        final boolean keep = SourceContract.Json.bool(form.compress(), "keep", false);
        if (!keep) {
            for (final OutputColumn c : form.components()) c.intermediate = true;
        }
        diagnostics.info("sequence.compress", def.location(), "compress fits an svd of the " + compress.inputs.size() + " component columns as block " + compress.name
                + (keep ? "; the components are emitted too (keep: true)" : "; the components are intermediate (keep: true emits them)"));
    }

    /** Validated weightBy references per (block, expression): an op is expanded once per window, reported once. */
    private final Map<String, Optional<References>> weightCache = new HashMap<>();

    /**
     * The references of an aggregate's {@code weightBy} — the event's fields by name, the current row's through
     * {@code $self} — or null (after reporting) when the weight is not usable: on another op, not a numeric
     * expression, over a non-numeric operand.
     */
    private References weightReferences(final FeatureDef def, final Op op) {
        return weightCache.computeIfAbsent(def.name + "\u0000" + op.type + "\u0000" + op.weightBy, k -> {
            final String loc = def.location();
            if (!"aggregate".equals(op.type)) {
                diagnostics.error("sequence.weightBy.op", loc, "weightBy is only defined on aggregate (op " + op.type + ")");
                return Optional.empty();
            }
            final References refs = expressionReferences(op.weightBy);
            boolean valid = true;
            final List<String> identity = new ArrayList<>();
            for (final String r : refs.others) valid &= numericWeightOperand(r, r, loc, identity);
            for (final String r : refs.self) valid &= numericWeightOperand(r, "$self." + r, loc, identity);
            if (!identity.isEmpty()) {
                // a hash answers == / != only: reading it as a magnitude (a difference, a kernel) is silently
                // nonsense — the weight would be ~1e15, so exp(-...) is 0 and every aggregate reads 0 / null
                final List<String> magnitudes = identity.stream().filter(s -> !comparedByIdentity(op.weightBy, s)).toList();
                if (magnitudes.isEmpty()) {
                    diagnostics.info("sequence.weightBy.identity", loc, "weightBy operand(s) " + identity + " are strings, compared by identity: the expression reads a hash of the text,"
                            + " so == and != (a category match: c == $self.c ? 1 : 0.25) are exact and <, > and arithmetic over them are meaningless");
                } else {
                    diagnostics.error("sequence.weightBy.identity", loc, "weightBy string operand(s) " + magnitudes + " are read outside == / !=: a string is compared by identity"
                            + " (the expression reads a hash of the text), so <, > and arithmetic over it are meaningless - write a category match (c == $self.c ? 1 : 0.25)"
                            + " or declare the operand as a number");
                    valid = false;
                }
            }
            try {
                com.mercari.solution.util.ExpressionUtil.createDefaultExpression(op.weightBy.replace("$self.", FeatureValues.SELF_PREFIX));
            } catch (final RuntimeException e) {
                diagnostics.error("sequence.weightBy.parse", loc, "cannot parse weightBy '" + op.weightBy + "' as a numeric expression: " + (e.getMessage() == null ? e.toString() : e.getMessage()));
                valid = false;
            }
            if (!valid) return Optional.empty();
            diagnostics.info("sequence.weightBy.scan", loc, "weightBy '" + op.weightBy + "' weighs every event against the current row, so no running state can serve it: "
                    + "the aggregate scans its window per row - bound the window with maxAge or maxEvents");
            return Optional.of(refs);
        }).orElse(null);
    }

    /**
     * Whether every occurrence of the operand in the weight expression is an operand of {@code ==} / {@code !=} —
     * the only comparison a hashed text answers ({@link FeatureValues#weightOperand}). Whitespace and parentheses
     * around the operand are skipped; anything else reads the hash as a magnitude.
     *
     * @param operand the operand as written: a past field's name, or {@code $self.<field>}
     */
    private static boolean comparedByIdentity(final String expression, final String operand) {
        final Matcher m = Pattern.compile("(?<![\\w.$])" + Pattern.quote(operand) + "(?!\\w)").matcher(expression);
        boolean found = false;
        while (m.find()) {
            found = true;
            int l = m.start();
            while (l > 0 && (Character.isWhitespace(expression.charAt(l - 1)) || expression.charAt(l - 1) == '(')) l--;
            int r = m.end();
            while (r < expression.length() && (Character.isWhitespace(expression.charAt(r)) || expression.charAt(r) == ')')) r++;
            final boolean equality = (l >= 2 && (expression.startsWith("==", l - 2) || expression.startsWith("!=", l - 2)))
                    || expression.startsWith("==", r) || expression.startsWith("!=", r);
            if (!equality) return false;
        }
        return found;
    }

    /** @param identity collects the string operands (compared by identity: {@link FeatureValues#weightOperand}) */
    private boolean numericWeightOperand(final String reference, final String shown, final String loc, final List<String> identity) {
        final Ref ref = resolve(reference);
        if (ref == null || ref.type() == null || OperatorCatalog.isNumeric(ref.type()) || ref.type().getType() == Schema.Type.bool) return true;
        if (ref.type().getType() == Schema.Type.string) {
            identity.add(shown);
            return true;
        }
        diagnostics.error("sequence.weightBy.type", loc, "weightBy operand '" + shown + "' is not numeric (" + ref.type().getType() + "); the weight is evaluated as a double");
        return false;
    }

    /** Records the weight on an aggregate column: the event side joins the projected history, the $self side the row inputs. */
    private void addWeight(final OutputColumn c, final Op op, final References weightRefs) {
        if (weightRefs == null) return;
        c.coordinates.put("weightBy", canonicalWeight(op.weightBy));
        for (final String r : weightRefs.others) addPastInput(c, r);
        for (final String r : weightRefs.self) addSelfInput(c, r);
    }

    /**
     * The weight expression with every reference spelled by its canonical name — the key the projected history and
     * the row map carry (a {@code block.column} or baseline reference would otherwise read null, i.e. weight NaN).
     */
    private String canonicalWeight(final String expression) {
        final Matcher m = IDENTIFIER.matcher(expression);
        final StringBuilder sb = new StringBuilder();
        while (m.find()) {
            final String replacement;
            if (m.group(1) != null) {
                replacement = "$self." + canonicalOf(m.group(1));
            } else if (m.group(3) == null && !KEYWORDS.contains(m.group(2).toLowerCase()) && !m.group(2).startsWith("$")) {
                replacement = canonicalOf(m.group(2)) + m.group().substring(m.group(2).length());
            } else {
                replacement = m.group();
            }
            m.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    /** The filter field when the window filter is a same-field {@code $self} equality over a safe field. */
    private String reducibleFilterField(final Window window, final AvailableAt computeAt) {
        if (window.filter == null) return null;
        final java.util.regex.Matcher m = java.util.regex.Pattern
                .compile("^\\s*([A-Za-z_][A-Za-z0-9_]*)\\s*=\\s*\\$self\\.([A-Za-z_][A-Za-z0-9_]*)\\s*$")
                .matcher(window.filter);
        if (!m.matches() || !m.group(1).equals(m.group(2))) return null;
        final Ref ref = resolve(m.group(1));
        // keying reads the past row's value at its own time: only safe for fields already available then
        if (ref == null || isOutcomeLike(ref) || !ref.availableAt().isStaticallyAtOrBefore(computeAt)) return null;
        return m.group(1);
    }

    private boolean isOutcomeLike(final Ref ref) {
        if (ref.derivedFrom().contains("outcome")) return true;
        return ref.worldAvailableAt().isProvablyAfter(spec.predictAt);
    }

    /**
     * A window filter / op predicate as the evaluator will parse it. Parsed here so a syntax error is a
     * compile diagnostic instead of a worker-setup failure; when the bare text fails only because a column
     * name is a reserved word of the condition grammar ({@code rank <= 3}), the referenced identifiers are
     * quoted with backticks ({@code `rank` <= 3}) and the rewritten text is what the column carries.
     * Returns null (after reporting) when the condition cannot be parsed either way.
     */
    private final Map<String, String> conditionCache = new HashMap<>();

    private String conditionText(final String text, final String loc, final String kind) {
        if (text == null) return null;
        if (conditionCache.containsKey(kind + ":" + text)) return conditionCache.get(kind + ":" + text);
        final String result = conditionTextUncached(text, loc, kind);
        conditionCache.put(kind + ":" + text, result);
        return result;
    }

    private String conditionTextUncached(final String text, final String loc, final String kind) {
        final String runtime = text.replace("$self.", FeatureValues.SELF_PREFIX);
        try {
            com.mercari.solution.util.pipeline.Filter.parse(runtime);
            return text;
        } catch (final RuntimeException bare) {
            final String quoted = quoteIdentifiers(text);
            if (!quoted.equals(text)) {
                try {
                    com.mercari.solution.util.pipeline.Filter.parse(quoted.replace("$self.", FeatureValues.SELF_PREFIX));
                    diagnostics.info(kind + ".quoted", loc, kind + " '" + text + "' uses a reserved word as a column name; evaluated as " + quoted);
                    return quoted;
                } catch (final RuntimeException ignored) {
                    // fall through to the original error
                }
            }
            diagnostics.error(kind + ".parse", loc, "cannot parse " + kind + " '" + text + "': "
                    + (bare.getMessage() == null ? bare.toString() : bare.getMessage()) + " (quote column names with backticks if they are SQL keywords)");
            return null;
        }
    }

    private static final java.util.regex.Pattern CONDITION_IDENTIFIER =
            java.util.regex.Pattern.compile("(?<![A-Za-z0-9_.`'\"$])(\\$self\\.)?([A-Za-z_][A-Za-z0-9_]*)(?![A-Za-z0-9_.`'\"(])");
    private static final Set<String> CONDITION_KEYWORDS = Set.of(
            "and", "or", "not", "is", "null", "in", "like", "between", "true", "false", "exists", "case", "when", "then", "else", "end");

    /** Backtick-quotes every bare identifier that refers to a known column (leaves keywords, literals, functions). */
    private String quoteIdentifiers(final String text) {
        final java.util.regex.Matcher m = CONDITION_IDENTIFIER.matcher(text);
        final StringBuilder sb = new StringBuilder();
        while (m.find()) {
            final String self = m.group(1) == null ? "" : m.group(1);
            final String name = m.group(2);
            final String replacement = !CONDITION_KEYWORDS.contains(name.toLowerCase()) && resolves(name)
                    ? "`" + self + name + "`" : self + name;
            m.appendReplacement(sb, java.util.regex.Matcher.quoteReplacement(replacement));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    private static String displayName(final String reference) {
        final int dot = reference.lastIndexOf('.');
        return dot < 0 ? reference : reference.substring(dot + 1);
    }

    /** A parameter as a coordinate the evaluator parses back ({@link #number} is the name token: {@code 1.5 → 1p5}). */
    private static String plainNumber(final Double d) {
        // an integral value beyond the long range keeps its own spelling (longValue() would saturate)
        return d == Math.floor(d) && Math.abs(d) < 0x1p63 ? Long.toString(d.longValue()) : d.toString();
    }

    private static String number(final Double d) {
        return plainNumber(d).replace('.', 'p');
    }

    /** Anonymous row feature {block}__e{n} for an inline expression (§4.3 脱糖規則). */
    private OutputColumn desugarExpression(final FeatureDef def, final String expr, final AvailableAt computeAt) {
        final References refs = expressionReferences(expr);
        if (refs.usesSelf) {
            diagnostics.error("sequence.self", def.location(), "$self is not allowed in op expressions (evaluated on past rows); use window.filter or a lag + row expr");
            return null;
        }
        final String canonical = def.name + "__e" + (++anonymousCounter);
        final OutputColumn c = newColumn(def.name, Scope.row, "expr", canonical, Schema.FieldType.FLOAT64, computeAt);
        c.anonymous = true;
        c.intermediate = true;
        c.coordinates.put("expr", expr);
        for (final String r : refs.others) addSelfInput(c, r);
        if (c.availableAt == null) c.availableAt = AvailableAt.atEventTime();
        c.status = Status.staticSafe;
        register(c);
        return c;
    }

    /** A contribution read from past rows: tracked separately because its availability is t'-relative. */
    private void addPastInput(final OutputColumn c, final String reference) {
        final Ref ref = resolve(reference);
        if (ref == null) return;
        c.inputs.add(ref.canonical);
        c.pastInputs.add(ref.canonical);
        mergeLineage(c, ref);
    }

    /**
     * §6.2 three-tier check for strictly-past windows: past availability δ' is constant → shift the near edge
     * by δ' − computeOffset (or prove safety via minInterval); otherwise the engine filters per row.
     */
    private void finishSequence(final OutputColumn c, final FeatureDef def, final EntityDef entity,
                                final Window window, final References filterRefs, final String reducedKey, final Op op) {
        finishSequence(c, def, entity, window, filterRefs, reducedKey, op, List.of());
    }

    /** {@code alignWith}: references whose availability the window takes without reading them (the time channel). */
    private void finishSequence(final OutputColumn c, final FeatureDef def, final EntityDef entity, final Window window,
                                final References filterRefs, final String reducedKey, final Op op, final List<String> alignWith) {
        finishSequence(c, def, entity, window, filterRefs, reducedKey, op, alignWith, false);
    }

    /**
     * {@code pooled}: the column is replayed over a pool of entities rather than per entity (a {@code rating}): its
     * stage key is the reduced filter field alone — none = the global key — and the entity's {@code minInterval}
     * proves nothing about the other entities' events, so it never absorbs the window shift.
     */
    private void finishSequence(final OutputColumn c, final FeatureDef def, final EntityDef entity, final Window window,
                                final References filterRefs, final String reducedKey, final Op op, final List<String> alignWith,
                                final boolean pooled) {
        c.coordinates.put("entity", entity.name());
        c.coordinates.put("window", window.token());
        if (window.maxAge != null) c.coordinates.put("maxAge", window.maxAge.toString());
        calendarWindow(c, window, def.location());
        if (window.maxEvents != null) c.coordinates.put("maxEvents", window.maxEvents.toString());
        if (pooled) {
            c.coordinates.put("stageKeys", reducedKey == null ? "" : reducedKey);
            if (reducedKey != null) addSelfInput(c, reducedKey);
        } else if (reducedKey != null) {
            final List<String> stageKeys = new ArrayList<>(entity.keys());
            stageKeys.add(reducedKey);
            c.coordinates.put("stageKeys", String.join(",", stageKeys));
            addSelfInput(c, reducedKey);
        } else if (window.filter != null) {
            final String filterText = conditionText(window.filter, def.location(), "filter");
            if (filterText != null) c.coordinates.put("filter", filterText);
        }
        // self-side inputs: entity keys and $self fields of the filter
        for (final String key : entity.keys()) addSelfInput(c, key);
        if (filterRefs != null) {
            for (final String s : filterRefs.self) addSelfInput(c, s);
            for (final String o : filterRefs.others) addPastInput(c, o);
        }
        if (isFuture(def)) classifyFuture(c, window);
        else classifyPast(c, pooled ? null : entity, alignWith);
        c.validFor = def.validFor;
        register(c);
    }

    private static boolean isFuture(final FeatureDef def) {
        return "future".equals(def.direction);
    }

    /** Blocks whose direction problems were reported (once per block, not per window × op). */
    private final Set<String> directionReported = new HashSet<>();

    /**
     * Whether an op of a sequence block can be expanded in the block's direction: {@code future} windows need a
     * {@code maxAge} (the label horizon) and accept the ops of {@link OperatorCatalog#FUTURE_OPS} (a lagged
     * regression reads the window in one direction and is rejected); {@code barrier} exists only there.
     */
    private boolean directionAccepts(final FeatureDef def, final Window window, final Op op) {
        final String loc = def.location();
        if (!isFuture(def)) {
            if ("barrier".equals(op.type)) {
                if (directionReported.add(def.name + ":barrier")) diagnostics.error("sequence.barrier.direction", loc, "barrier labels the path after the row: it needs direction: future");
                return false;
            }
            return true;
        }
        if (window.onCalendar()) {
            if (directionReported.add(def.name + ":clock:" + window.token())) {
                diagnostics.error("clock.direction", loc, "a future window measures its horizon on wall time (maxAge as an ISO-8601 duration): window " + window.token());
            }
            return false;
        }
        if (window.maxAge == null) {
            if (directionReported.add(def.name + ":maxAge:" + window.token())) {
                diagnostics.error("sequence.direction.maxAge", loc, "a future window needs maxAge (the label horizon): window " + window.token());
            }
            return false;
        }
        if (!OperatorCatalog.FUTURE_OPS.contains(op.type) || ("regression".equals(op.type) && op.lag != null && op.lag > 0)) {
            if (directionReported.add(def.name + ":op:" + op.type)) {
                diagnostics.error("sequence.direction.op", loc, "op " + op.type + (op.lag != null && op.lag > 0 ? " with lag" : "")
                        + " is not defined over a future window (available: " + String.join(" | ", OperatorCatalog.FUTURE_OPS) + "; regression without lag)");
            }
            return false;
        }
        if (directionReported.add(def.name + ":future")) {
            diagnostics.info("sequence.direction.future", loc, "block " + def.name + " reads the strictly-future window (t, t + maxAge] of each row: its columns are labels"
                    + " (status label, post-event by construction) - a feature referencing one is an availability violation");
        }
        return true;
    }

    /**
     * A future window's column: a label. Its value is known once the last event of the horizon is available —
     * {@code availableAt = maxAge + the past inputs' own availability} (a pre-event or earlier field adds nothing; a
     * dynamic one keeps its static lower bound, shifted by the horizon), joined with the self side read at the row
     * itself (entity keys, {@code $self} filter fields, the barrier's entry value); it is never shifted (the window
     * reads what happens, not what is known). Its status {@code label} marks it post-event — never a feature for a
     * consumer; the role {@code label} stays with the declared {@code output.roles.label} column alone.
     */
    private void classifyFuture(final OutputColumn c, final Window window) {
        AvailableAt past = null;
        for (final String p : c.pastInputs) {
            final Ref ref = resolve(p);
            if (ref != null) past = AvailableAt.max(past, ref.availableAt());
        }
        final AvailableAt horizon = AvailableAt.eventRelative(window.maxAge);
        final AvailableAt end = past == null || past.isPreEvent() ? horizon : AvailableAt.max(horizon, past.plus(window.maxAge));
        // c.availableAt holds the self side accumulated by addSelfInput
        c.availableAt = AvailableAt.max(c.availableAt, end);
        c.coordinates.put("direction", "future");
        c.status = Status.label;
    }

    private void classifyPast(final OutputColumn c, final EntityDef entity) {
        classifyPast(c, entity, List.of());
    }

    private void classifyPast(final OutputColumn c, final EntityDef entity, final List<String> alignWith) {
        final Duration minInterval = entity == null ? null : entity.minInterval();
        final Set<String> pastSide = new LinkedHashSet<>(c.pastInputs);
        pastSide.addAll(alignWith);
        AvailableAt past = null;
        for (final String p : pastSide) {
            final Ref ref = resolve(p);
            if (ref != null) past = AvailableAt.max(past, ref.availableAt());
        }
        if (past == null) past = AvailableAt.atEventTime();
        final AvailableAt selfSide = c.availableAt == null ? AvailableAt.atEventTime() : c.availableAt;
        // by construction the past contribution is available at computeAt (window shift / filter)
        c.availableAt = AvailableAt.max(selfSide, c.computeAt);
        if (!selfSide.isStaticallyAtOrBefore(c.computeAt)) {
            c.status = selfSide.isStatic() ? Status.violation : Status.runtimeFilter;
            return;
        }
        if (!past.isStatic()) {
            c.status = Status.runtimeFilter;
            return;
        }
        final Duration shift = past.getOffset().minus(c.computeAt.getOffset());
        if (shift.isNegative() || shift.isZero()) {
            c.status = Status.staticSafe;
        } else if (minInterval != null && minInterval.compareTo(shift) >= 0) {
            c.status = Status.staticSafe;
            c.coordinates.put("minInterval", minInterval.toString());
            c.coordinates.put("minIntervalEntity", entity.name());
            // the declaration is trusted here and verified nowhere: recorded for the audit query and the run-time counter
            // (the info is raised once per entity in reportMinIntervalAudits, where the largest shift absorbed is known)
            final FeaturePlan.MinIntervalAudit before = minIntervalAudits.get(entity.name());
            final List<String> relying = before == null ? new ArrayList<>() : before.columns();
            relying.add(c.canonicalName);
            minIntervalAudits.put(entity.name(), new FeaturePlan.MinIntervalAudit(entity.name(), entity.keys(), minInterval,
                    before == null || before.shift().compareTo(shift) < 0 ? shift : before.shift(), relying));
        } else {
            c.status = Status.windowShift;
            c.windowShift = shift;
        }
    }

    // --- population ---------------------------------------------------------------------------

    private void expandPopulation(final FeatureDef def, final AvailableAt computeAt) {
        final String loc = def.location();
        if (def.type == null) {
            diagnostics.error("population.type", loc, "population feature requires 'type'");
            return;
        }
        if (OperatorCatalog.get(Scope.population, def.type) == null) {
            diagnostics.error("population.type", loc, "unknown population type: " + def.type);
            return;
        }
        if (!OperatorCatalog.isImplemented(Scope.population, def.type)) {
            diagnostics.error("population.unsupported", loc, "population type '" + def.type + "' is not implemented yet (available: " + String.join(" | ", OperatorCatalog.IMPLEMENTED_POPULATION_TYPES) + ")");
            return;
        }
        if ("factorization".equals(def.type)) {
            expandFactorization(def, computeAt);
            return;
        }
        if ("discretize".equals(def.type)) {
            expandDiscretize(def, computeAt);
            return;
        }
        if ("quantileTransform".equals(def.type)) {
            expandQuantileTransform(def, computeAt);
            return;
        }
        if ("svd".equals(def.type)) {
            expandSvd(def, computeAt);
            return;
        }
        if ("smooth".equals(def.type)) {
            expandSmooth(def, computeAt);
            return;
        }
        if ("transitionStats".equals(def.type)) {
            expandTransitionStats(def, computeAt);
            return;
        }
        if ("spectralEmbedding".equals(def.type)) {
            expandSpectralEmbedding(def, computeAt);
            return;
        }
        expandEncoding(def, computeAt);
    }

    /**
     * The previous values of a block's {@code sequenceOf: {entity, field}} — a {@code lag} over the entity, expanded
     * under the block's own name as intermediate columns {@code <name>_all_prev_lag<i>}, most recent first — or null
     * after reporting. The two sequence-of-values population types are built on these columns: what an entity's
     * value was one, two ... steps ago is an ordinary keyed column, so neither type needs a keyed pass of its own.
     */
    private List<String> sequencePath(final FeatureDef def, final int steps, final AvailableAt computeAt) {
        final String loc = def.location();
        if (!def.sequenceUnknown.isEmpty()) {
            diagnostics.error(def.type + ".parameters", loc, "not understood: " + def.sequenceUnknown
                    + " (sequenceOf: {entity, field}; cooccur: {window, weighting}; of: current | previous | [current, previous];"
                    + " emit: [distribution | {toValueProb: <value>}]; blend: {perEntity, priorWeight})");
            return null;
        }
        if (def.sequenceEntity == null || def.sequenceField == null || !entities.containsKey(def.sequenceEntity)) {
            diagnostics.error(def.type + ".sequenceOf", loc, def.type + " requires sequenceOf: {entity: <entities[].name>, field: <categorical field>}"
                    + (def.sequenceEntity != null && !entities.containsKey(def.sequenceEntity) ? ": unknown entity " + def.sequenceEntity : ""));
            return null;
        }
        final Ref ref = resolve(def.sequenceField);
        if (ref == null) {
            diagnostics.error("reference.unknown", loc, "unknown field: " + def.sequenceField);
            return null;
        }
        if (!OperatorCatalog.isCategorical(ref.type())) {
            diagnostics.error(def.type + ".sequenceOf", loc, "sequenceOf.field '" + def.sequenceField + "' must be categorical (discretize a numeric field first)");
            return null;
        }
        final FeatureDef path = new FeatureDef();
        path.name = def.name;
        path.scope = Scope.sequence;
        path.entity = def.sequenceEntity;
        path.validFor = def.validFor;
        final Op lag = new Op();
        lag.type = "lag";
        lag.fields = List.of(def.sequenceField);
        lag.k = steps;
        lag.as = "prev";
        path.ops.add(lag);
        expandSequence(path, computeAt);
        // the names expandSequence gave the lag columns: <block>_<window token>_<op.as>_lag<i>, on the default window
        final String base = def.name + "_" + new Window().token() + "_" + lag.as + "_lag";
        final List<String> columns = new ArrayList<>();
        for (int i = 1; i <= steps; i++) {
            final OutputColumn c = columnsByCanonical.get(base + i);
            if (c == null) return null; // the lag failed to expand and said why
            c.intermediate = true;
            columns.add(c.canonicalName);
        }
        return columns;
    }

    /**
     * The two sequence-of-values types share {@code sequenceOf} and nothing else, and {@link FeatureSpec} parses
     * every key of either onto the same {@link FeatureDef}: a parameter of the sibling type would otherwise be
     * accepted and silently dropped (a {@code blend} that never shrinks, a {@code maxValues} that never caps).
     * Reported like the foreign parameters of a {@code rating} method.
     *
     * @return whether the block declares only its own parameters
     */
    private boolean rejectForeignSequenceParameters(final FeatureDef def, final boolean spectral) {
        final List<String> foreign = new ArrayList<>();
        if (spectral) {
            if (def.order != null) foreign.add("order");
            if (!def.emitValues.isEmpty() || def.emitDistribution || !def.emitReadouts.isEmpty()) foreign.add("emit");
            if (def.blendPerEntity != null || def.blendPriorWeight != null) foreign.add("blend");
        } else {
            if (def.cooccurWindow != null || def.cooccurWeighting != null) foreign.add("cooccur");
            if (!def.embedOf.isEmpty()) foreign.add("of");
            if (def.maxValues != null) foreign.add("maxValues");
            if (def.rank != null) foreign.add("rank");
        }
        if (foreign.isEmpty()) return true;
        diagnostics.error(def.type + ".parameters", def.location(), foreign + (spectral
                ? " are transitionStats parameters: spectralEmbedding takes sequenceOf, cooccur, rank, of, maxValues, maxFeatures, fit"
                : " are spectralEmbedding parameters: transitionStats takes sequenceOf, order, emit, blend, maxFeatures"));
        return false;
    }

    /**
     * Both sequence-of-values types build the blocks they stand for themselves — the lag path, and (transitionStats)
     * the whole encoding: its keySet, its target, its shrinkage and its always-expanding fit. An encoding parameter
     * declared on the block would be parsed onto the same {@link FeatureDef} and then overwritten in silence, so it
     * is reported for the same reason a sibling type's parameter is. {@code fit} is a spectralEmbedding parameter.
     *
     * @return whether the block declares no parameter of the blocks it expands into
     */
    private boolean rejectEncodingParameters(final FeatureDef def, final boolean spectral) {
        final List<String> ignored = new ArrayList<>();
        if (!def.keySets.isEmpty()) ignored.add("keySets");
        if (!def.targets.isEmpty()) ignored.add("targets");
        if (def.shrinkageJson != null) ignored.add("shrinkage");
        if (def.naming != null) ignored.add("naming");
        if (!def.windows.isEmpty()) ignored.add("windows");
        if (!spectral && def.fitJson != null) ignored.add("fit");
        if (ignored.isEmpty()) return true;
        diagnostics.error(def.type + ".parameters", def.location(), ignored + " are encoding parameters: " + def.type
                + " builds the blocks it stands for and would drop them" + (spectral
                ? "; spectralEmbedding takes sequenceOf, cooccur, rank, of, maxValues, maxFeatures, fit"
                : " (its fit is always expanding); transitionStats takes sequenceOf, order, emit, blend, maxFeatures"));
        return false;
    }

    /**
     * §4.4 transitionStats: the distribution of an entity's NEXT value given its previous one(s) — a desugaring, not an
     * estimator of its own. The state is the lag path of {@link #sequencePath}; the statistic is an encoding's
     * {@code distribution} of the field keyed on that state, expanding (strictly past) and shrunk along the chain
     * {@code (entity, state) → (state) → shorter states → global} with the {@code blend} pseudo-count — the
     * Dirichlet-Multinomial case of §5.5, which is what the spec's {@code blend} always was. Columns:
     * {@code <name>_to_<value>} per {@code toValueProb}, and the map {@code <name>_to} for {@code distribution}.
     */
    private void expandTransitionStats(final FeatureDef def, final AvailableAt computeAt) {
        final String loc = def.location();
        if (!rejectForeignSequenceParameters(def, false)) return;
        final int order = def.order == null ? 1 : def.order;
        if (order < 1 || order > 4) {
            diagnostics.error("transitionStats.order", loc, "order must be within 1..4 (the number of previous values that make the state): " + order);
            return;
        }
        if (def.emitValues.isEmpty() && !def.emitDistribution && def.emitReadouts.isEmpty() && def.sequenceUnknown.isEmpty()) {
            diagnostics.error("transitionStats.emit", loc, "transitionStats requires emit: [distribution | {toValueProb: <value>} | ownValueProb | surprisal | entropy | expected, ...]");
            return;
        }
        // the emitted columns: one per toValueProb and per readout, plus the distribution map (svd / spectralEmbedding cap theirs too)
        final int produced = def.emitValues.size() + def.emitReadouts.size() + (def.emitDistribution ? 1 : 0);
        if (def.maxFeatures != null && produced > def.maxFeatures) {
            diagnostics.error("transitionStats.maxFeatures", loc, "emit produces " + produced + " columns, exceeding maxFeatures " + def.maxFeatures);
            return;
        }
        final boolean perEntity = def.blendPerEntity != null && def.blendPerEntity;
        final double priorWeight = def.blendPriorWeight == null ? 20 : def.blendPriorWeight;
        if (!(priorWeight > 0) || Double.isInfinite(priorWeight)) {
            diagnostics.error("transitionStats.blend", loc, "blend.priorWeight must be a positive number: " + def.blendPriorWeight);
            return;
        }
        final List<String> path = sequencePath(def, order, computeAt);
        if (path == null) return;
        if (!rejectEncodingParameters(def, false)) return;
        // the field is resolved and categorical by now (sequencePath said so): only 'expected' constrains it further
        final Ref fieldRef = resolve(def.sequenceField);
        if (def.emitReadouts.contains("expected") && (fieldRef == null || !OperatorCatalog.isNumeric(fieldRef.type()))) {
            diagnostics.error("transitionStats.emit", loc, "emit expected is the probability-weighted mean of the next value, so the field must be an integer code"
                    + " (a bin index, an ordered band); '" + def.sequenceField + "' is " + (fieldRef == null ? "unknown" : fieldRef.type().getType()));
            return;
        }

        // the chain of coarser states under the leaf: the pooled state, then its shorter suffixes, then the marginal
        final List<String> leaf = new ArrayList<>();
        if (perEntity) leaf.addAll(entities.get(def.sequenceEntity).keys());
        leaf.addAll(path);
        // the coarser levels only: the global one is not declared here (expandEncoding appends it to any shrunk
        // lattice, and an empty hierarchy entry is dropped there — declaring it would be dead weight in the plan)
        final List<List<String>> coarser = new ArrayList<>();
        for (int length = perEntity ? order : order - 1; length >= 1; length--) coarser.add(List.copyOf(path.subList(0, length)));
        final com.google.gson.JsonArray hierarchy = new com.google.gson.JsonArray();
        for (final List<String> level : coarser) {
            final com.google.gson.JsonArray keys = new com.google.gson.JsonArray();
            for (final String step : level) keys.add(step);
            hierarchy.add(keys);
        }
        final FeatureDef encoding = new FeatureDef();
        encoding.name = def.name;
        encoding.scope = Scope.population;
        encoding.type = "encoding";
        encoding.validFor = def.validFor;
        encoding.naming = "{block}_{target}";
        final KeySet keySet = new KeySet();
        keySet.keys = leaf;
        keySet.hierarchyJson = hierarchy.toString();
        encoding.keySets.add(keySet);
        final Target target = new Target();
        target.field = def.sequenceField;
        target.as = "to";
        target.stats = List.of("distribution");
        target.values = def.emitValues;
        encoding.targets.add(target);
        final JsonObject shrinkage = new JsonObject();
        shrinkage.addProperty("priorWeight", priorWeight);
        encoding.shrinkageJson = shrinkage.toString();
        // a value distribution lives in the expanding replay only, whatever the top-level fit
        final JsonObject fit = new JsonObject();
        fit.addProperty("mode", FitMode.expanding.token());
        encoding.fitJson = fit.toString();
        expandEncoding(encoding, computeAt);
        // emit: [distribution, {toValueProb: …}] keeps the map next to its per-value columns
        final OutputColumn map = columnsByCanonical.get(def.name + "_to");
        // the map is a column of its own only when asked for: the per-value columns and the readouts read it
        // (null when the desugared encoding failed to register it — expandEncoding said why)
        if (map != null) {
            map.intermediate = !def.emitDistribution;
            for (final String readout : def.emitReadouts) {
                // a reader of the distribution map: the whole map for entropy / expected, plus the row's OWN value for
                // ownValueProb / surprisal — which is what decides their availability (an outcome field is a violation)
                final OutputColumn c = newColumn(def.name, Scope.row, "mapReadout", def.name + "_" + readout, Schema.FieldType.FLOAT64, computeAt);
                c.coordinates.put("readout", readout);
                c.fitted = map.fitted;
                addSelfInput(c, map.canonicalName);
                if ("ownValueProb".equals(readout) || "surprisal".equals(readout)) {
                    c.coordinates.put("field", fieldRef == null ? def.sequenceField : fieldRef.canonical());
                    addSelfInput(c, def.sequenceField);
                }
                finishRow(c, def);
            }
            if (fieldRef != null && isOutcomeLike(fieldRef)
                    && (def.emitReadouts.contains("ownValueProb") || def.emitReadouts.contains("surprisal")) && hintedBlocks.add("transitionStats.emit.own:" + def.name)) {
                diagnostics.hint("transitionStats.emit.own", loc, "ownValueProb / surprisal read the row's own value of '" + def.sequenceField + "', an outcome: the columns are"
                        + " availability violations (usable as a label or an intermediate target, not as a feature); entropy / expected / toValueProb read the distribution only");
            }
        }
        final List<String> chain = new ArrayList<>();
        chain.add(leaf.toString());
        for (final List<String> level : coarser) chain.add(level.toString());
        chain.add("global");
        diagnostics.info("transitionStats.expansion", loc, "transitionStats is the expanding distribution of " + def.sequenceField + " keyed on "
                + leaf + " (the previous " + (order == 1 ? "value" : order + " values") + " of entity " + def.sequenceEntity + (perEntity ? ", per entity" : ", pooled over entities")
                + "), shrunk along " + String.join(" -> ", chain) + " with pseudo-count " + priorWeight
                + " (Dirichlet-Multinomial); a row reads from the deepest level of the chain that has rows (the effective leaf), leave-node-out from that level;"
                + " an event without a previous value reads the coarser levels");
    }

    /**
     * §4.4 spectralEmbedding: coordinates of a categorical state from the company it keeps — the pairs (value, a value
     * at most {@code cooccur.window} steps earlier in the entity's sequence) are counted, turned into a PPMI matrix
     * and factorised ({@link Spectral}). The pairs come from the lag path of {@link #sequencePath}, so the fit is a
     * summary block like svd (static, or per time block under forward); the row's own value — or, with
     * {@code of: previous}, its previous one — is looked up. {@code rank} FLOAT64 columns {@code <name>_<k>}; with
     * {@code of: [current, previous]} the ONE fit is read twice and the previous value's coordinates are
     * {@code <name>_prev_<k>} (two blocks would count the same pairs and solve the same eigenproblem twice).
     */
    private void expandSpectralEmbedding(final FeatureDef def, final AvailableAt computeAt) {
        final String loc = def.location();
        if (!rejectForeignSequenceParameters(def, true)) return;
        final int window = def.cooccurWindow == null ? Spectral.DEFAULT_WINDOW : def.cooccurWindow;
        final int rank = def.rank == null ? Spectral.DEFAULT_RANK : def.rank;
        final int maxValues = def.maxValues == null ? Spectral.DEFAULT_MAX_VALUES : def.maxValues;
        final List<String> declared = def.embedOf.isEmpty() ? List.of("current") : def.embedOf;
        boolean valid = true;
        if (window < 1 || window > 8) {
            diagnostics.error("spectralEmbedding.cooccur", loc, "cooccur.window must be within 1..8 steps: " + window);
            valid = false;
        }
        if (def.cooccurWeighting != null && !Spectral.PPMI.equals(def.cooccurWeighting)) {
            diagnostics.error("spectralEmbedding.cooccur", loc, "cooccur.weighting must be ppmi: " + def.cooccurWeighting);
            valid = false;
        }
        if (rank < 1) {
            diagnostics.error("spectralEmbedding.rank", loc, "rank must be >= 1");
            valid = false;
        }
        if (maxValues < 2 || maxValues > Spectral.MAX_VALUES) {
            diagnostics.error("spectralEmbedding.maxValues", loc, "maxValues must be within 2.." + Spectral.MAX_VALUES + " (the eigenproblem is dense in the distinct values): " + maxValues);
            valid = false;
        }
        if (!List.of("current", "previous").containsAll(declared) || new HashSet<>(declared).size() != declared.size()) {
            diagnostics.error("spectralEmbedding.of", loc, "of must be current | previous, or the list of both: " + declared);
            valid = false;
        }
        final long produced = (long) rank * declared.size();
        if (def.maxFeatures != null && produced > def.maxFeatures) {
            diagnostics.error("spectralEmbedding.maxFeatures", loc, "rank " + rank + (declared.size() > 1 ? " for " + declared.size() + " embedded values" : "")
                    + " produces " + produced + " columns, exceeding maxFeatures " + def.maxFeatures);
            valid = false;
        }
        if (!valid) return;
        // `of` is a set: both values written either way round are the same block, so the columns come out in the
        // same order (the row's value first) rather than in the order the list happened to be spelled in
        final List<String> of = declared.size() < 2 ? declared : List.of("current", "previous");
        final List<String> path = sequencePath(def, window, computeAt);
        if (path == null) return;
        if (!rejectEncodingParameters(def, true)) return;
        final FeatureSpec.FitSpec fitSpec = parseLookupFit(def, "spectralEmbedding", "the embedding is fitted", "eigendecomposition of the whole input's co-occurrence counts", true);
        final int minRows = fitSpec.minRows == null ? 0 : fitSpec.minRows;
        final String minRowsPhrase = minRowsPhrase(minRows, null);
        final String align = alignOf(def, fitSpec, "spectralEmbedding");
        final boolean forward = fitSpec.mode == FitMode.forward;
        final String what = "spectralEmbedding counts the pairs of " + def.sequenceField + " within " + window + " step(s) of entity " + def.sequenceEntity
                + " and factorises their PPMI matrix into " + rank + " coordinate(s) (at most " + maxValues + " values)"
                + (of.size() > 1 ? ", read for the row's value (" + def.name + "_<k>) and for its previous one (" + def.name + "_prev_<k>) from the one fit" : "");
        if (forward) {
            final ForwardBlocks blocks = fitSpec.forwardBlocks();
            diagnostics.info("fit.mode.forward", loc, what + " per time block (" + blocks.describe() + "), re-solved for every row over the complete blocks"
                    + (fitSpec.window == null ? "" : " within " + fitSpec.window) + " whose values are known at predictAt (the row's own block excluded)"
                    + (fitSpec.minBlocksOf(blocks) <= 1 ? "" : "; rows with fewer than " + fitSpec.minBlocksOf(blocks) + " preceding blocks read null")
                    + minRowsPhrase + alignPhrase(align, "coordinates")
                    // the vocabulary cap must be decided before the counts are accumulated (the state is quadratic in
                    // the values), which is one thing a forward fit reads from the whole input rather than per block
                    + "; the " + maxValues + " values counted (maxValues, by co-occurrence mass) are chosen over the whole input, the counts themselves per block"
                    + " - when the field has more values than that, which of them are embedded depends on the whole input (the one part of the fit that is not walk-forward)"
                    + (fitSpec.artifactUri == null ? "" : "; the whole-input embedding is persisted under " + fitSpec.artifactUri + "/<planHash>/ for a static serving run"));
        } else {
            // the pairs are counted from every row's OWN value of the field, whichever value `of` looks up: an
            // outcome-like field therefore shapes the coordinates a training row reads (as svd reports for its inputs)
            final Ref fieldRef = resolve(def.sequenceField);
            final boolean outcome = fieldRef != null && isOutcomeLike(fieldRef);
            diagnostics.info("fit.mode.static", loc, what + " over the whole input" + artifactPhrase(fitSpec) + minRowsPhrase
                    + "; no target is read, but the neighbourhoods include the test period (fit.mode forward walks them)"
                    + (outcome ? "; '" + def.sequenceField + "' is outcome-like, so each training row's own outcome is one of the pairs behind"
                            + " the coordinates it reads - 'of' changes which value(s) are looked up, not what the fit counts (fit.mode forward does)" : ""));
        }
        final List<String> references = new ArrayList<>(path);
        references.add(def.sequenceField);
        // one embedded value keeps the plain names whichever it is; of the two, the previous one is marked
        for (final String embedded : of) {
            final String applied = "previous".equals(embedded) ? path.get(0) : def.sequenceField;
            final String prefix = def.name + (of.size() > 1 && "previous".equals(embedded) ? "_prev_" : "_");
            for (int k = 0; k < rank; k++) {
                final OutputColumn c = newColumn(def.name, Scope.population, "spectralEmbedding", prefix + k, Schema.FieldType.FLOAT64, computeAt);
                c.fitted = true;
                c.coordinates.put("fit", forward ? "forward" : "static");
                if (forward) {
                    forwardCoordinates(c, null, references, def, fitSpec);
                    c.coordinates.put("predictOffsetMillis", Long.toString(spec.predictAt.getOffset().toMillis()));
                }
                c.coordinates.put("field", canonicalOf(def.sequenceField));
                c.coordinates.put("path", String.join(",", path));
                c.coordinates.put("applied", canonicalOf(applied));
                c.coordinates.put("rank", Integer.toString(rank));
                c.coordinates.put("component", Integer.toString(k));
                c.coordinates.put("maxValues", Integer.toString(maxValues));
                if (fitSpec.artifactUri != null) c.coordinates.put("artifactUri", fitSpec.artifactUri);
                if (fitSpec.refit) c.coordinates.put("refit", "true");
                if (minRows > 0) c.coordinates.put("minRows", Integer.toString(minRows));
                if (align != null) c.coordinates.put("align", align);
                // only the embedded value is read from the row itself; the pairs are read through the fit
                addSelfInput(c, applied);
                for (final String reference : references) addPastInput(c, reference);
                finishStaticFitted(c, def);
                register(c);
            }
        }
    }

    /**
     * §5.6 smooth: the curve of a target over a numeric key (the linear-basis class) — a penalised B-spline
     * regression solved from the moments of {@code [B(x), y]}, so it is fitted like an svd block (static, or per
     * time block under forward) and applied by evaluating the curve at the row's key. Columns: the curve
     * {@code <name>} and, on request, the residual {@code <name>_resid} = target − curve, which reads the row's
     * own target and is therefore as available as the target is (a target for other blocks, not a feature).
     */
    private void expandSmooth(final FeatureDef def, final AvailableAt computeAt) {
        final String loc = def.location();
        final String input = singleInput(def);
        if (input == null) return;
        final Ref ref = resolve(input);
        if (ref == null) {
            diagnostics.error("reference.unknown", loc, "unknown field: " + input);
            return;
        }
        if (!OperatorCatalog.isNumeric(ref.type())) diagnostics.error("smooth.input", loc, "smooth input '" + input + "' must be numeric");
        if (def.target == null) {
            diagnostics.error("smooth.target", loc, "smooth requires 'target' (the numeric or boolean field / column whose curve over the input is fitted)");
            return;
        }
        final Ref targetRef = resolve(def.target);
        if (targetRef == null) {
            diagnostics.error("reference.unknown", loc, "unknown target: " + def.target);
            return;
        }
        if (!OperatorCatalog.isNumeric(targetRef.type()) && (targetRef.type() == null || targetRef.type().getType() != Schema.Type.bool)) {
            diagnostics.error("smooth.target", loc, "smooth target '" + def.target + "' must be numeric or boolean");
        }
        final String method = def.method == null ? Smooth.SPLINE : def.method;
        switch (method) {
            case Smooth.SPLINE -> { }
            case "isotonic", "rff" -> diagnostics.error("smooth.method", loc, "method " + method + " is not implemented yet (spline is available)");
            default -> diagnostics.error("smooth.method", loc, "method must be spline | isotonic | rff: " + method);
        }
        final int segments = def.segments == null ? Smooth.DEFAULT_SEGMENTS : def.segments;
        final int degree = def.degree == null ? Smooth.DEFAULT_DEGREE : def.degree;
        final int order = def.penaltyOrder == null ? Smooth.DEFAULT_PENALTY_ORDER : def.penaltyOrder;
        boolean valid = true;
        if (segments < 1) {
            diagnostics.error("smooth.segments", loc, "segments must be >= 1");
            valid = false;
        }
        if (degree < 0 || degree > 5) {
            diagnostics.error("smooth.degree", loc, "degree must be within 0..5 (3 = cubic B-splines): " + degree);
            valid = false;
        }
        if (order < 1 || order > 3) {
            diagnostics.error("smooth.penalty", loc, "penalty.order must be 1 | 2 | 3 (the curve shrinks towards a constant | a line | a parabola): " + order);
            valid = false;
        }
        if (!def.penaltyUnknown.isEmpty()) {
            diagnostics.error("smooth.penalty", loc, "unknown penalty key(s) " + def.penaltyUnknown + " (accepted: order, lambda)");
        }
        if (valid && segments + degree > Smooth.MAX_BASIS) {
            diagnostics.error("smooth.segments", loc, "segments + degree = " + (segments + degree) + " basis functions exceed " + Smooth.MAX_BASIS
                    + " (the fit state is (basis + 1)^2 numbers per time block; the penalty, not the knot count, sets the smoothness)");
            valid = false;
        }
        if (valid && segments + degree <= order) {
            diagnostics.error("smooth.penalty", loc, "penalty.order " + order + " needs more than " + order + " basis functions (segments + degree = " + (segments + degree) + ")");
            valid = false;
        }
        // λ: chosen by REML unless declared
        String lambda = Smooth.REML;
        if (def.penaltyLambda != null && !Smooth.REML.equals(def.penaltyLambda)) {
            try {
                final double declared = Double.parseDouble(def.penaltyLambda.trim());
                if (!(declared > 0) || Double.isInfinite(declared)) throw new NumberFormatException();
                lambda = Double.toString(declared);
            } catch (final NumberFormatException e) {
                diagnostics.error("smooth.penalty", loc, "penalty.lambda must be reml or a positive number: " + def.penaltyLambda);
                valid = false;
            }
        }
        // the knots are laid over a declared range: they must be known before the single pass over the rows
        double lo = 0, hi = 1;
        if (def.range.isEmpty() && ref.column() != null && "quantileTransform".equals(ref.column().operator)
                && QuantileTransform.UNIFORM.equals(ref.column().coordinates.getOrDefault("distribution", QuantileTransform.UNIFORM))) {
            diagnostics.info("smooth.range", loc, "range defaults to [0, 1], the range of the uniform quantileTransform column '" + input + "' (knots at the input's quantiles)");
        } else if (def.range.size() != 2 || !(def.range.get(0) < def.range.get(1)) || def.range.get(0).isInfinite() || def.range.get(1).isInfinite()) {
            diagnostics.error("smooth.range", loc, "smooth requires range: [lo, hi] with lo < hi - the knots are placed before the rows are read, and keys beyond the range are clamped to it"
                    + " (for knots at the data's quantiles, feed a uniform quantileTransform column: its range is [0, 1])" + (def.range.isEmpty() ? "" : ": " + def.range));
            valid = false;
        } else {
            lo = def.range.get(0);
            hi = def.range.get(1);
        }
        final List<String> outputs = def.outputs.isEmpty() ? List.of("curve") : def.outputs;
        for (final String output : outputs) {
            if (!List.of("curve", "residual").contains(output)) {
                diagnostics.error("smooth.outputs", loc, "unknown smooth output: " + output + " (curve | residual)");
                valid = false;
            }
        }
        final FeatureSpec.FitSpec fitSpec = parseLookupFit(def, "smooth", "the curve is fitted", "penalised regression solved from the moments of the whole input", true);
        warnAlignIgnored(def, "smooth");
        if (!valid) return;
        // the default floor of a curve is one row more than it has coefficients. With fewer rows the penalty alone decides
        // the curve; with exactly as many, the unpenalised end of the search interpolates them — edf = n, no residual
        // degree of freedom, σ² = RSS / (n − edf) undefined — and REML has nothing to weigh the penalty against
        final int minRows = fitSpec.minRows == null ? segments + degree + 1 : fitSpec.minRows;
        final String minRowsPhrase = minRowsPhrase(minRows, fitSpec.minRows == null ? "one more than the " + (segments + degree) + " coefficients" : null);
        final boolean forward = fitSpec.mode == FitMode.forward;
        final String what = "smooth fits " + (segments + degree) + " B-spline coefficients of degree " + degree + " over [" + lo + ", " + hi + "] (difference penalty of order " + order
                + ", strength " + (Smooth.REML.equals(lambda) ? "chosen by REML" : lambda) + ") from the moments of (basis, target)";
        if (forward) {
            final ForwardBlocks blocks = fitSpec.forwardBlocks();
            diagnostics.info("fit.mode.forward", loc, what + " per time block (" + blocks.describe() + ") and, for every row, re-solves them over the complete blocks"
                    + (fitSpec.window == null ? "" : " within " + fitSpec.window) + " whose targets are known at predictAt (the row's own block excluded)"
                    + (fitSpec.minBlocksOf(blocks) <= 1 ? "" : "; rows with fewer than " + fitSpec.minBlocksOf(blocks) + " preceding blocks read null")
                    + minRowsPhrase
                    + (fitSpec.artifactUri == null ? "" : "; the whole-input curve is persisted under " + fitSpec.artifactUri + "/<planHash>/ for a static serving run"));
        } else {
            diagnostics.info("fit.mode.static", loc, what + " over the whole input" + artifactPhrase(fitSpec) + minRowsPhrase
                    + "; every training row's own target shapes the curve it reads (static-fit caveat): fit.mode forward reads the earlier time blocks only");
        }

        for (final String output : new LinkedHashSet<>(outputs)) {
            final boolean residual = "residual".equals(output);
            final OutputColumn c = newColumn(def.name, Scope.population, "smooth", residual ? def.name + "_resid" : def.name, Schema.FieldType.FLOAT64, computeAt);
            c.fitted = true;
            c.coordinates.put("fit", forward ? "forward" : "static");
            if (forward) {
                forwardCoordinates(c, null, List.of(input, def.target), def, fitSpec);
                c.coordinates.put("predictOffsetMillis", Long.toString(spec.predictAt.getOffset().toMillis()));
            }
            c.coordinates.put("method", method);
            c.coordinates.put("field", canonicalOf(input));
            c.coordinates.put("target", canonicalOf(def.target));
            c.coordinates.put("output", output);
            c.coordinates.put("segments", Integer.toString(segments));
            c.coordinates.put("degree", Integer.toString(degree));
            c.coordinates.put("lo", Double.toString(lo));
            c.coordinates.put("hi", Double.toString(hi));
            c.coordinates.put("penaltyOrder", Integer.toString(order));
            c.coordinates.put("lambda", lambda);
            if (fitSpec.artifactUri != null) c.coordinates.put("artifactUri", fitSpec.artifactUri);
            if (fitSpec.refit) c.coordinates.put("refit", "true");
            if (minRows > 0) c.coordinates.put("minRows", Integer.toString(minRows));
            addSelfInput(c, input);
            addPastInput(c, input);
            // the curve reads the targets of other rows (through the fit); the residual also reads the row's own
            if (residual) addSelfInput(c, def.target);
            addPastInput(c, def.target);
            finishStaticFitted(c, def);
            register(c);
        }
    }

    /**
     * §4.4 quantileTransform: the empirical CDF position of a numeric field (or its normal score), the quantile
     * knots fitted on the whole input (static fit) and applied by interpolation — one FLOAT64 column.
     */
    private void expandQuantileTransform(final FeatureDef def, final AvailableAt computeAt) {
        final String loc = def.location();
        final String input = singleInput(def);
        if (input == null) return;
        final Ref ref = resolve(input);
        if (ref == null) {
            diagnostics.error("reference.unknown", loc, "unknown field: " + input);
            return;
        }
        if (!OperatorCatalog.isNumeric(ref.type())) diagnostics.error("quantileTransform.input", loc, "quantileTransform input '" + input + "' must be numeric");
        final int bins = def.bins == null ? QuantileTransform.DEFAULT_BINS : def.bins;
        if (bins < 2) diagnostics.error("quantileTransform.bins", loc, "bins must be >= 2");
        final String distribution = def.distribution == null ? QuantileTransform.UNIFORM : def.distribution;
        if (!List.of(QuantileTransform.UNIFORM, QuantileTransform.NORMAL).contains(distribution)) {
            diagnostics.error("quantileTransform.distribution", loc, "distribution must be uniform | normal: " + distribution);
        }
        // clip is a coordinate only when declared for normal: the plan hash of every existing config stays put
        String clipCoordinate = null;
        if (def.clip != null) {
            if (!(def.clip > 0 && def.clip < 0.5)) {
                diagnostics.error("quantileTransform.clip", loc, "clip must be a probability in (0, 0.5): " + def.clip);
            } else if (!QuantileTransform.NORMAL.equals(distribution)) {
                diagnostics.warning("quantileTransform.clip", loc, "clip only applies to distribution: normal (the uniform position is not clamped): "
                        + "it has no effect on the output but still changes the plan hash - remove it");
            } else {
                clipCoordinate = Double.toString(def.clip);
            }
        }
        final FeatureSpec.FitSpec fitSpec = parseLookupFit(def, "quantileTransform", "the quantiles are fitted", "quantile knots fitted on the whole input", true);
        warnAlignIgnored(def, "quantileTransform");
        final int minRows = fitSpec.minRows == null ? 0 : fitSpec.minRows;
        final String minRowsPhrase = minRowsPhrase(minRows, null);
        final boolean forward = fitSpec.mode == FitMode.forward;
        if (forward) {
            final ForwardBlocks blocks = fitSpec.forwardBlocks();
            diagnostics.info("fit.mode.forward", loc, "quantileTransform gathers the values per time block (" + blocks.describe() + ") and, for every row, fits " + bins
                    + " quantile intervals over the complete blocks" + (fitSpec.window == null ? "" : " within " + fitSpec.window)
                    + " whose input is known at predictAt (the row's own block excluded)"
                    + (fitSpec.minBlocksOf(blocks) <= 1 ? "" : "; rows with fewer than " + fitSpec.minBlocksOf(blocks) + " preceding blocks read null")
                    + minRowsPhrase
                    + (fitSpec.artifactUri == null ? "" : "; the whole-input knots are persisted under " + fitSpec.artifactUri + "/<planHash>/ for a static serving run"));
        } else {
            diagnostics.info("fit.mode.static", loc, "quantileTransform fits " + bins + " quantile intervals on the whole input" + artifactPhrase(fitSpec) + minRowsPhrase
                    + (isOutcomeLike(ref) ? "; the input is outcome-like, so training rows' own outcomes shape the knots (static-fit caveat)" : ""));
        }

        final OutputColumn c = newColumn(def.name, Scope.population, "quantileTransform", def.name, Schema.FieldType.FLOAT64, computeAt);
        c.fitted = true;
        c.coordinates.put("fit", forward ? "forward" : "static");
        if (forward) {
            forwardCoordinates(c, null, List.of(input), def, fitSpec);
            c.coordinates.put("predictOffsetMillis", Long.toString(spec.predictAt.getOffset().toMillis()));
        }
        c.coordinates.put("field", canonicalOf(input));
        c.coordinates.put("bins", Integer.toString(bins));
        c.coordinates.put("distribution", distribution);
        if (clipCoordinate != null) c.coordinates.put("clip", clipCoordinate);
        if (fitSpec.artifactUri != null) c.coordinates.put("artifactUri", fitSpec.artifactUri);
        if (fitSpec.refit) c.coordinates.put("refit", "true");
        if (minRows > 0) c.coordinates.put("minRows", Integer.toString(minRows));
        addSelfInput(c, input);
        addPastInput(c, input);
        finishStaticFitted(c, def);
        register(c);
    }

    /**
     * §4.4 svd: truncated SVD / PCA of a numeric vector — several numeric {@code inputs}, or one {@code input} of
     * array type — fitted from (n, Σx, Σxxᵀ) over the whole input (static fit); {@code rank} FLOAT64 score columns
     * {@code <name>_<k>} ordered by explained variance.
     */
    private void expandSvd(final FeatureDef def, final AvailableAt computeAt) {
        final String loc = def.location();
        final List<String> fields = new ArrayList<>();
        String arrayField = null;
        Integer dimension = null;
        if (def.inputs.size() >= 2) {
            if (def.input != null) diagnostics.error("svd.input", loc, "svd takes either 'inputs' (numeric fields) or one array 'input', not both");
            for (final String f : def.inputs) {
                final Ref ref = resolve(f);
                if (ref == null) {
                    diagnostics.error("reference.unknown", loc, "unknown field: " + f);
                    return;
                }
                if (!OperatorCatalog.isNumeric(ref.type())) diagnostics.error("svd.input", loc, "svd input '" + f + "' must be numeric");
                fields.add(f);
            }
            dimension = fields.size();
        } else {
            final String input = def.input != null ? def.input : def.inputs.size() == 1 ? def.inputs.get(0) : null;
            if (input == null) {
                diagnostics.error("svd.input", loc, "svd requires 'inputs' (two or more numeric fields) or one array-typed 'input'");
                return;
            }
            final Ref ref = resolve(input);
            if (ref == null) {
                diagnostics.error("reference.unknown", loc, "unknown field: " + input);
                return;
            }
            if (ref.type() == null || ref.type().getType() != Schema.Type.array || !OperatorCatalog.isNumeric(ref.type().getArrayValueType())) {
                diagnostics.error("svd.input", loc, "svd input '" + input + "' must be an array of numbers (or list two or more numeric fields under 'inputs')");
                return;
            }
            arrayField = input;
        }
        if (def.rank == null && dimension == null) {
            diagnostics.error("svd.rank", loc, "svd on an array input requires 'rank' (the vector length is not known at compile time)");
            return;
        }
        final int rank = def.rank == null ? Math.min(dimension, 8) : def.rank;
        if (rank < 1) {
            diagnostics.error("svd.rank", loc, "rank must be >= 1");
            return;
        }
        if (dimension != null && rank > dimension) diagnostics.error("svd.rank", loc, "rank " + rank + " exceeds the vector length " + dimension);
        if (dimension == null) {
            diagnostics.info("svd.rank", loc, "rank " + rank + " cannot be checked against the array length at compile time; if the vectors are shorter, the fit caps the components at their length and the surplus score columns read null (see the run-time svd warning)");
        }
        final boolean center = def.center == null || def.center;
        final FeatureSpec.FitSpec fitSpec = parseLookupFit(def, "svd", "the components are fitted", "covariance eigendecomposition on the whole input", true);
        final int minRows = fitSpec.minRows == null ? 0 : fitSpec.minRows;
        final String minRowsPhrase = minRowsPhrase(minRows, null);
        final String align = alignOf(def, fitSpec, "svd");
        final boolean forward = fitSpec.mode == FitMode.forward;
        final List<String> inputs = arrayField != null ? List.of(arrayField) : fields;
        boolean outcome = false;
        for (final String f : inputs) {
            final Ref ref = resolve(f);
            if (ref != null && isOutcomeLike(ref)) outcome = true;
        }
        final String what = "svd fits " + rank + " component(s) of " + (dimension == null ? "the vector" : dimension + " inputs")
                + " from (n, sum x, sum xx^T)" + (center ? "" : ", uncentred") + (def.standardize ? ", standardised" : "");
        if (forward) {
            final ForwardBlocks blocks = fitSpec.forwardBlocks();
            diagnostics.info("fit.mode.forward", loc, what + " per time block (" + blocks.describe() + ") and, for every row, re-solves them over the complete blocks"
                    + (fitSpec.window == null ? "" : " within " + fitSpec.window) + " whose inputs are known at predictAt (the row's own block excluded)"
                    + (fitSpec.minBlocksOf(blocks) <= 1 ? "" : "; rows with fewer than " + fitSpec.minBlocksOf(blocks) + " preceding blocks read null")
                    + minRowsPhrase + alignPhrase(align, "components")
                    + (fitSpec.artifactUri == null ? "" : "; the whole-input components are persisted under " + fitSpec.artifactUri + "/<planHash>/ for a static serving run"));
        } else {
            diagnostics.info("fit.mode.static", loc, what + " over the whole input" + artifactPhrase(fitSpec) + minRowsPhrase
                    + (outcome ? "; an input is outcome-like, so training rows' own outcomes shape the components (static-fit caveat)" : ""));
        }

        // what to emit: the scores (default), the part of every input the components do not explain, its length
        final List<String> outputs = def.outputs.isEmpty() ? List.of("scores") : def.outputs;
        for (final String output : outputs) {
            if (!List.of("scores", "residual", "residualNorm").contains(output)) {
                diagnostics.error("svd.outputs", loc, "unknown svd output: " + output + " (scores | residual | residualNorm)");
                return;
            }
        }
        if (outputs.contains("residual") && arrayField != null) {
            diagnostics.error("svd.outputs", loc, "outputs: residual emits one column per input and needs named 'inputs'; on an array input use residualNorm");
            return;
        }
        // (column name, coordinate, index): a score per component, a residual per input dimension, the residual norm
        final List<String[]> emitted = new ArrayList<>();
        if (outputs.contains("scores")) for (int k = 0; k < rank; k++) emitted.add(new String[]{def.name + "_" + k, "component", Integer.toString(k)});
        if (outputs.contains("residual")) for (int i = 0; i < fields.size(); i++) emitted.add(new String[]{def.name + "_resid_" + displayName(fields.get(i)), "residual", Integer.toString(i)});
        if (outputs.contains("residualNorm")) emitted.add(new String[]{def.name + "_residnorm", "residual", "norm"});

        int produced = 0;
        for (final String[] e : emitted) {
            final OutputColumn c = newColumn(def.name, Scope.population, "svd", e[0], Schema.FieldType.FLOAT64, computeAt);
            c.fitted = true;
            c.coordinates.put("fit", forward ? "forward" : "static");
            if (forward) {
                forwardCoordinates(c, null, inputs, def, fitSpec);
                c.coordinates.put("predictOffsetMillis", Long.toString(spec.predictAt.getOffset().toMillis()));
            }
            if (arrayField != null) c.coordinates.put("arrayField", canonicalOf(arrayField));
            else c.coordinates.put("fields", String.join(",", fields.stream().map(this::canonicalOf).toList()));
            c.coordinates.put("rank", Integer.toString(rank));
            c.coordinates.put(e[1], e[2]);
            c.coordinates.put("center", Boolean.toString(center));
            c.coordinates.put("standardize", Boolean.toString(def.standardize));
            if (fitSpec.artifactUri != null) c.coordinates.put("artifactUri", fitSpec.artifactUri);
            if (fitSpec.refit) c.coordinates.put("refit", "true");
            if (minRows > 0) c.coordinates.put("minRows", Integer.toString(minRows));
            if (align != null) c.coordinates.put("align", align);
            for (final String f : inputs) {
                addSelfInput(c, f);
                addPastInput(c, f);
            }
            finishStaticFitted(c, def);
            register(c);
            produced++;
        }
        if (def.maxFeatures != null && produced > def.maxFeatures) {
            diagnostics.error("svd.maxFeatures", loc, "rank " + rank + " with outputs " + outputs + " produces " + produced + " columns, exceeding maxFeatures " + def.maxFeatures);
        }
    }

    /**
     * §4.4 discretize: bin edges fitted on the whole input (static fit) and applied by lookup — one INT64
     * column ({@code -1} missing, {@code 0} below the fitted range, {@code 1..B} fitted bins, {@code B+1}
     * above), typically an encoding key. Only the unsupervised {@code quantile} method is implemented:
     * {@code tree} / {@code optimal} consume a target, and the spec ties their fit rule to the encoding
     * that keys on them (two-stage target consumption), which the compiler does not model yet.
     */
    private void expandDiscretize(final FeatureDef def, final AvailableAt computeAt) {
        final String loc = def.location();
        final String input = singleInput(def);
        if (input == null) return;
        final Ref ref = resolve(input);
        if (ref == null) {
            diagnostics.error("reference.unknown", loc, "unknown field: " + input);
            return;
        }
        if (!OperatorCatalog.isNumeric(ref.type())) diagnostics.error("discretize.input", loc, "discretize input '" + input + "' must be numeric");
        final String method = def.method == null ? "quantile" : def.method;
        switch (method) {
            case "quantile" -> { }
            case "tree", "optimal" -> diagnostics.error("discretize.method", loc, "method " + method + " (supervised) is not implemented yet (quantile is available)");
            default -> diagnostics.error("discretize.method", loc, "method must be quantile | tree | optimal: " + method);
        }
        if (def.target != null && "quantile".equals(method)) diagnostics.warning("discretize.target", loc, "target is only used by method tree / optimal and is ignored");
        if (def.bins != null && def.bins < 2) diagnostics.error("discretize.bins", loc, "bins must be >= 2");
        if (def.minSamplesPerBin != null && def.minSamplesPerBin < 1) diagnostics.error("discretize.minSamplesPerBin", loc, "minSamplesPerBin must be >= 1");

        final FeatureSpec.FitSpec fitSpec = parseStaticOnlyFit(def, "discretize", "the edges are fitted", "edges fitted on the whole input");
        diagnostics.info("fit.mode.static", loc, "discretize fits the bin edges on the whole input" + artifactPhrase(fitSpec)
                + (isOutcomeLike(ref) ? "; the input is outcome-like, so training rows' own outcomes shape the edges (static-fit caveat)" : ""));

        final OutputColumn c = newColumn(def.name, Scope.population, "discretize", def.name, Schema.FieldType.INT64, computeAt);
        c.fitted = true;
        c.coordinates.put("fit", "static");
        c.coordinates.put("method", method);
        c.coordinates.put("field", canonicalOf(input));
        if (def.bins != null) c.coordinates.put("bins", Integer.toString(def.bins));
        if (def.minSamplesPerBin != null) c.coordinates.put("minSamplesPerBin", Integer.toString(def.minSamplesPerBin));
        if (fitSpec.artifactUri != null) c.coordinates.put("artifactUri", fitSpec.artifactUri);
        if (fitSpec.refit) c.coordinates.put("refit", "true");
        addSelfInput(c, input);
        addPastInput(c, input);
        finishStaticFitted(c, def);
        register(c);
    }

    /**
     * The {@code fit:} block of a static-only population type (factorization / discretize): artifact settings
     * inherited from the top-level fit, {@code mode} must be static, and {@code cadence / window / warmStart}
     * are accepted but ignored until fit boundaries are implemented.
     */
    private FeatureSpec.FitSpec parseStaticOnlyFit(final FeatureDef def, final String codePrefix, final String fitted, final String why) {
        return parseLookupFit(def, codePrefix, fitted, why, false);
    }

    /**
     * {@code fit.align} of a type whose forward fits have a gauge (svd, spectralEmbedding): the mode the engine reads
     * — {@code procrustes} unless declared — or null under a static fit, which is solved once and has nothing to
     * align to (a declared mode is reported as ignored).
     */
    private String alignOf(final FeatureDef def, final FeatureSpec.FitSpec fitSpec, final String codePrefix) {
        final JsonObject defFit = parseJsonObject(def.fitJson);
        if (fitSpec.mode != FitMode.forward) {
            if (defFit != null && defFit.has("align")) {
                diagnostics.warning(codePrefix + ".fit.align", def.location(), "fit.align applies to fit.mode forward only (a static fit is solved once: there is no earlier fit to align to)");
            }
            return null;
        }
        return fitSpec.align == null ? Alignment.PROCRUSTES : fitSpec.align;
    }

    /** A block-level {@code fit.align} on a type whose fit has no gauge to align (a curve, quantile knots, level statistics). */
    private void warnAlignIgnored(final FeatureDef def, final String codePrefix) {
        warnAlignIgnored(def, parseJsonObject(def.fitJson), codePrefix);
    }

    /** {@link #warnAlignIgnored(FeatureDef, String)} where the block's {@code fit} is already parsed. */
    private void warnAlignIgnored(final FeatureDef def, final JsonObject defFit, final String codePrefix) {
        if (defFit != null && defFit.has("align")) {
            diagnostics.warning(codePrefix + ".fit.align", def.location(), "fit.align is not implemented for " + def.type + " and ignored (svd / spectralEmbedding take it)");
        }
    }

    /** The {@code fit.align} clause of a forward fit's report. */
    private static String alignPhrase(final String align, final String what) {
        return switch (align) {
            case Alignment.NONE -> "; the " + what + " of every block are oriented on their own (fit.align none): they flip and mix from block to block";
            case Alignment.SIGN -> "; the " + what + " of every block are sign-flipped towards those of the block before (fit.align sign): close eigenvalues still mix";
            case Alignment.PROCRUSTES -> "; the " + what + " of every block are rotated into those of the block before (fit.align procrustes, the default), so a column continues across"
                    + " blocks - it is a stable coordinate of the fitted subspace rather than its k-th eigenvector (an svd's columns are then no longer uncorrelated,"
                    + " and the first no longer carries the most variance)";
            // a mode added to Alignment.MODES without a clause here: name it rather than claim the default's meaning
            default -> "; the " + what + " of every block are brought into those of the block before (fit.align " + align + ")";
        };
    }

    /**
     * The {@code fit.minRows} clause of a lookup fit's report: empty without a floor.
     *
     * @param defaultReason what the floor is when the block declared none (null = declared)
     */
    private static String minRowsPhrase(final int minRows, final String defaultReason) {
        if (minRows <= 0) return "";
        return "; a fit over fewer than " + minRows + " row(s) is not solved and its rows read null (fit.minRows"
                + (defaultReason == null ? "" : ", default: " + defaultReason + " - 0 = no floor") + ")";
    }

    /**
     * The fit block of a lookup-fitted population type (svd / quantileTransform / discretize / factorization): static
     * by default, {@code forward} when the type supports per-block fits ({@code forwardAllowed}: the block's
     * {@code fit.blocks} / {@code minBlocks} / {@code window} / {@code minHistory} inherit the top-level fit and are
     * read into the returned spec, whose {@code mode} records the choice); the artifact settings as for encodings.
     * A block that declares no {@code mode} inherits a top-level {@code forward} (the other top-level modes have no
     * lookup-fit counterpart and leave the block static), so the whole spec walks forward together.
     */
    private FeatureSpec.FitSpec parseLookupFit(final FeatureDef def, final String codePrefix, final String fitted, final String why, final boolean forwardAllowed) {
        final String loc = def.location();
        final JsonObject defFit = parseJsonObject(def.fitJson);
        final FeatureSpec.FitSpec fitSpec = new FeatureSpec.FitSpec();
        fitSpec.artifactUri = spec.fit.artifactUri;
        fitSpec.refit = spec.fit.refit;
        FitMode mode = FitMode.statik;
        boolean modeDeclared = false;
        if (defFit != null) {
            if (SourceContract.Json.string(defFit, "mode") != null) {
                mode = FeatureSpec.parseFitMode(SourceContract.Json.string(defFit, "mode"), diagnostics, loc);
                modeDeclared = true;
            }
            FeatureSpec.FitSpec.parseArtifact(defFit, fitSpec);
            for (final String key : List.of("cadence", "warmStart")) {
                if (defFit.has(key)) diagnostics.warning(codePrefix + ".fit." + key, loc, "fit." + key + " is not implemented yet and ignored (" + fitted + " on the whole input)");
            }
        }
        if (forwardAllowed) {
            // a block without its own mode follows the top-level fit when the top level walks forward; the other
            // top-level modes (expanding / fold) have no lookup-fit counterpart and leave the block static
            if (!modeDeclared && spec.fit.mode == FitMode.forward) mode = FitMode.forward;
            fitSpec.blockBucket = spec.fit.blockBucket;
            fitSpec.blockSize = spec.fit.blockSize;
            fitSpec.blockClock = spec.fit.blockClock;
            fitSpec.blockTicks = spec.fit.blockTicks;
            fitSpec.minBlocks = spec.fit.minBlocks;
            fitSpec.minRows = spec.fit.minRows;
            fitSpec.align = spec.fit.align;
            fitSpec.window = spec.fit.window;
            fitSpec.minHistory = spec.fit.minHistory;
            FeatureSpec.FitSpec.parseForward(defFit, fitSpec, diagnostics, loc, spec.timeField);
            resolveBlockClock(fitSpec, loc);
            if (mode == FitMode.statik && defFit != null && defFit.has("window")) {
                diagnostics.warning(codePrefix + ".fit.window", loc, "fit.window applies to fit.mode forward only (" + fitted + " on the whole input in static)");
            }
            if (mode == FitMode.statik && spec.fit.mode == FitMode.forward) {
                // an explicit static under a forward spec: the block alone sees the whole input, including the
                // test period, while the encodings around it walk forward
                diagnostics.info(codePrefix + ".fit.mode.static", loc, def.type + " declares fit.mode static while the top-level fit is forward, so "
                        + fitted + " on the whole input; drop the block's fit.mode to walk it forward with the rest of the spec");
            }
        } else if (defFit != null) {
            warnAlignIgnored(def, defFit, codePrefix);
            if (defFit.has("window")) diagnostics.warning(codePrefix + ".fit.window", loc, "fit.window is not implemented for " + def.type + " and ignored (" + fitted + " on the whole input)");
            if (defFit.has("minRows")) diagnostics.warning(codePrefix + ".fit.minRows", loc, "fit.minRows is not implemented for " + def.type + " and ignored (smooth / svd / quantileTransform / spectralEmbedding take it)");
        }
        if (mode != FitMode.statik && !(forwardAllowed && mode == FitMode.forward)) {
            diagnostics.error(codePrefix + ".fit.mode", loc, def.type + " requires fit.mode static" + (forwardAllowed ? " | forward" : "") + " (" + why + "); "
                    + (forwardAllowed ? "expanding / fold" : "expanding / fold / forward") + " are not available");
            mode = FitMode.statik;
        }
        fitSpec.mode = mode;
        return fitSpec;
    }

    private static String artifactPhrase(final FeatureSpec.FitSpec fitSpec) {
        return fitSpec.artifactUri == null ? " (no artifact: in-pipeline only)" : " and persisted under " + fitSpec.artifactUri + "/<planHash>/";
    }

    /**
     * Availability of a column filled by lookup from a static fit: the fitted result is an artifact available
     * at computeAt by declaration (§6.1 fit boundary), so only the row-side inputs decide the status.
     */
    private void finishStaticFitted(final OutputColumn c, final FeatureDef def) {
        final AvailableAt selfSide = c.availableAt == null ? AvailableAt.atEventTime() : c.availableAt;
        c.availableAt = AvailableAt.max(selfSide, c.computeAt);
        c.status = rowStatus(selfSide, c.computeAt);
        c.validFor = def.validFor;
    }

    /** §4.4 factorization: a static fit (ALS) applied by lookup; outputs are pair scores, embeddings or the linear predictor. */
    private void expandFactorization(final FeatureDef def, final AvailableAt computeAt) {
        final String loc = def.location();
        final String variant = def.variant == null ? "fm" : def.variant;
        switch (variant) {
            case "fm", "fwfm" -> { }
            case "bayesian" -> diagnostics.error("factorization.variant", loc, "variant bayesian is v2 and not implemented yet (fm | fwfm)");
            default -> diagnostics.error("factorization.variant", loc, "variant must be fm | fwfm: " + variant);
        }
        if (def.fields.size() < 2) diagnostics.error("factorization.fields", loc, "factorization requires at least two 'fields'");
        for (final String f : def.fields) {
            final Ref ref = resolve(f);
            if (ref == null) diagnostics.error("reference.unknown", loc, "unknown field: " + f);
            else if (!OperatorCatalog.isCategorical(ref.type())) diagnostics.error("factorization.fields", loc, "field '" + f + "' must be categorical");
        }
        final int latentDim = def.latentDim == null ? 8 : def.latentDim;
        if (latentDim < 1) diagnostics.error("factorization.latentDim", loc, "latentDim must be >= 1");

        final FeatureSpec.FitSpec fitSpec = parseStaticOnlyFit(def, "factorization", "the model is fitted", "iterative ALS fit");
        diagnostics.info("fit.mode.static", loc, "factorization is fitted on the whole input" + artifactPhrase(fitSpec)
                + "; the whole training set is gathered on one worker for ALS");

        // task: target (field or expr) and optional baseline offset
        String target = def.taskTarget;
        if (def.taskTargetExpr != null) {
            final OutputColumn anonymous = desugarExpression(def, def.taskTargetExpr, computeAt);
            if (anonymous != null) target = anonymous.canonicalName;
        }
        if (target == null) {
            diagnostics.error("factorization.task", loc, "factorization requires task.target (field) or task.expr");
            return;
        }
        if (!resolves(target)) {
            diagnostics.error("reference.unknown", loc, "unknown task.target: " + target);
            return;
        }
        String offsetColumn = null;
        if (def.taskOffset != null) {
            offsetColumn = baselineColumns.get(def.taskOffset);
            if (offsetColumn == null) diagnostics.error("factorization.offset", loc, "task.offset must reference baselines[].name: " + def.taskOffset);
            else if (!computeAt.equals(spec.predictAt)) diagnostics.error("encoding.offset.computeAt", loc, "blocks referencing a baseline offset must be computed at predictAt");
        }
        if (def.fmOutputs.isEmpty()) diagnostics.error("factorization.outputs", loc, "factorization requires 'outputs'");

        int produced = 0;
        for (final FmOutput out : def.fmOutputs) {
            final List<OutputColumn> cols = new ArrayList<>();
            switch (out.kind) {
                case "pair" -> {
                    if (out.pair.size() != 2 || !def.fields.containsAll(out.pair)) {
                        diagnostics.error("factorization.outputs", loc, "pair must name two of the block's fields: " + out.pair);
                        continue;
                    }
                    final String name = out.as != null ? out.as : def.name + "_" + out.pair.get(0) + "_" + out.pair.get(1);
                    final OutputColumn c = newColumn(def.name, Scope.population, "fm", name, Schema.FieldType.FLOAT64, computeAt);
                    c.coordinates.put("kind", "pair");
                    c.coordinates.put("pair", String.join(",", out.pair));
                    cols.add(c);
                }
                case "embedding" -> {
                    if (out.embedding == null || !def.fields.contains(out.embedding)) {
                        diagnostics.error("factorization.outputs", loc, "embedding must name one of the block's fields: " + out.embedding);
                        continue;
                    }
                    final int dims = out.dims == null ? latentDim : Math.min(out.dims, latentDim);
                    final String base = out.as != null ? out.as : def.name + "_" + out.embedding + "_emb";
                    for (int d = 0; d < dims; d++) {
                        final OutputColumn c = newColumn(def.name, Scope.population, "fm", base + "_" + d, Schema.FieldType.FLOAT64, computeAt);
                        c.coordinates.put("kind", "embedding");
                        c.coordinates.put("field", out.embedding);
                        c.coordinates.put("dim", Integer.toString(d));
                        cols.add(c);
                    }
                }
                default -> {
                    final OutputColumn c = newColumn(def.name, Scope.population, "fm", out.as != null ? out.as : def.name + "_sum", Schema.FieldType.FLOAT64, computeAt);
                    c.coordinates.put("kind", "sum");
                    cols.add(c);
                }
            }
            for (final OutputColumn c : cols) {
                c.fitted = true;
                c.coordinates.put("fit", "static");
                c.coordinates.put("variant", variant);
                c.coordinates.put("fields", String.join(",", def.fields));
                c.coordinates.put("latentDim", Integer.toString(latentDim));
                c.coordinates.put("target", target);
                c.coordinates.put("epochs", Integer.toString(def.epochs == null ? 10 : def.epochs));
                c.coordinates.put("reg", Double.toString(def.reg == null ? 0.01 : def.reg));
                c.coordinates.put("seed", Long.toString(def.seed == null ? 0L : def.seed));
                if (fitSpec.artifactUri != null) c.coordinates.put("artifactUri", fitSpec.artifactUri);
                if (fitSpec.refit) c.coordinates.put("refit", "true");
                if (offsetColumn != null) {
                    // read from the training rows only (the example's target minus its baseline), like the target
                    c.coordinates.put("offset", offsetColumn);
                    addPastInput(c, offsetColumn);
                }
                for (final String f : def.fields) addSelfInput(c, f);
                addPastInput(c, target);
                finishStaticFitted(c, def);
                register(c);
                produced++;
            }
        }
        if (def.maxFeatures != null && produced > def.maxFeatures) {
            diagnostics.error("factorization.maxFeatures", loc, "outputs produce " + produced + " columns, exceeding maxFeatures " + def.maxFeatures);
        }
    }

    private void expandEncoding(final FeatureDef def, final AvailableAt computeAt) {
        final String loc = def.location();
        if (def.keySets.isEmpty()) diagnostics.error("encoding.keySets", loc, "encoding requires 'keySets'");
        if (def.targets.isEmpty()) diagnostics.error("encoding.targets", loc, "encoding requires 'targets'");

        FitMode mode = spec.fit.mode;
        String groupBy = spec.fit.groupBy;
        final JsonObject defFit = parseJsonObject(def.fitJson);
        final JsonObject defShrinkage = parseJsonObject(def.shrinkageJson);
        final JsonObject defSmoothing = parseJsonObject(def.smoothingJson);
        final FeatureSpec.FitSpec fitSpec = new FeatureSpec.FitSpec();
        fitSpec.artifactUri = spec.fit.artifactUri;
        fitSpec.refit = spec.fit.refit;
        if (defFit != null) {
            if (SourceContract.Json.string(defFit, "mode") != null) mode = FeatureSpec.parseFitMode(SourceContract.Json.string(defFit, "mode"), diagnostics, loc);
            if (SourceContract.Json.string(defFit, "groupBy") != null) {
                groupBy = SourceContract.Json.string(defFit, "groupBy");
                if (!entities.containsKey(groupBy)) {
                    // must not silently fall back to row-identity folds: the fit.groupBy.required leak guard trusts a non-null groupBy
                    diagnostics.error("fit.groupBy", loc, "fit.groupBy must reference an entity: " + groupBy);
                    groupBy = null;
                }
            }
            FeatureSpec.FitSpec.parseArtifact(defFit, fitSpec);
        }
        fitSpec.blockBucket = spec.fit.blockBucket;
        fitSpec.blockSize = spec.fit.blockSize;
        fitSpec.blockClock = spec.fit.blockClock;
        fitSpec.blockTicks = spec.fit.blockTicks;
        fitSpec.minBlocks = spec.fit.minBlocks;
        fitSpec.window = spec.fit.window;
        fitSpec.minHistory = spec.fit.minHistory;
        FeatureSpec.FitSpec.parseForward(defFit, fitSpec, diagnostics, loc, spec.timeField);
        if (defFit != null && defFit.has("align") && hintedBlocks.add("encoding.fit.align:" + def.name)) warnAlignIgnored(def, defFit, "encoding");
        // an encoding has no row floor: a level too thin to stand on its own is shrunk towards its parent instead
        if (defFit != null && defFit.has("minRows") && hintedBlocks.add("encoding.fit.minRows:" + def.name)) {
            diagnostics.warning("encoding.fit.minRows", loc, "fit.minRows is not implemented for encoding and ignored"
                    + " (a thin level is shrunk towards its parent instead; smooth / svd / quantileTransform / spectralEmbedding take it)");
        }
        resolveBlockClock(fitSpec, loc);
        // fold: {by, purge, embargo} (negative durations are rejected by parseFold); a time fold has no hash folds to count
        fitSpec.foldBy = spec.fit.foldBy;
        fitSpec.purge = spec.fit.purge;
        fitSpec.embargo = spec.fit.embargo;
        fitSpec.untilMillis = spec.fit.untilMillis;
        FeatureSpec.FitSpec.parseFold(defFit, fitSpec, diagnostics, loc);
        Integer folds = spec.fit.folds;
        if (defFit != null && SourceContract.Json.integer(defFit, "folds") != null) folds = SourceContract.Json.integer(defFit, "folds");
        if (mode == FitMode.fold && !fitSpec.isTimeFold() && folds < 2) {
            diagnostics.error("fit.folds", loc, "fit.folds must be at least 2: " + folds);
            folds = 2;
        }
        fitSpec.groupBy = groupBy;
        fitSpec.folds = folds;
        if (mode != FitMode.fold && (fitSpec.foldBy != null || fitSpec.purge != null || fitSpec.embargo != null || fitSpec.untilMillis != null) && hintedBlocks.add(def.name + "#foldIgnored")) {
            diagnostics.warning("fit.fold.ignored", loc, "fit.fold applies to fit.mode fold only (mode " + mode.token() + "): by / purge / embargo / until are ignored");
        } else if (mode == FitMode.fold && !fitSpec.isTimeFold() && (fitSpec.purge != null || fitSpec.embargo != null || fitSpec.untilMillis != null)) {
            diagnostics.warning("fit.fold.ignored", loc, "purge / embargo / until need fit.fold.by: time (hash folds have no time order): they are ignored");
        }
        // static and fold both fit sufficient statistics over the input and apply them by lookup; fold
        // subtracts the row's own fold so a row never sees its own contribution (out-of-fold statistics)
        final boolean isStatic = mode.isLookup();
        if (mode == FitMode.statik) {
            diagnostics.info("fit.mode.static", loc, "fit.mode static fits the statistics on the whole input"
                    + (fitSpec.artifactUri == null ? " (no artifact: in-pipeline only)" : " and persists them under " + fitSpec.artifactUri + "/<planHash>/")
                    + "; training rows include their own outcome, so use expanding for leak-safe backfill and static for serving / offline analysis");
        } else if (mode == FitMode.fold && fitSpec.isTimeFold()) {
            diagnostics.info("fit.mode.fold", loc, "fit.mode fold by time: every time block (" + fitSpec.forwardBlocks().describe() + ") is a fold - a row reads the"
                    + " statistics of the whole input minus its own block"
                    + (fitSpec.purge == null ? ", for a target reading a label the label's horizon on both sides of it (the default purge)" : ", the purge " + fitSpec.purge + " on both sides of it")
                    + (fitSpec.embargo == null ? "" : " and the embargo " + fitSpec.embargo + " after the purge")
                    + " (rounded up to whole blocks: 2*purge + embargo + 1 blocks are left out; the engine warns when that is more than half of the input's blocks);"
                    + (fitSpec.untilMillis == null
                            ? " the other blocks include rows AFTER it (cross-fit, not time-ordered)"
                            : " the cross-fit stays within the blocks up to " + java.time.Instant.ofEpochMilli(fitSpec.untilMillis) + " (fit.fold.until: the other blocks of the training period"
                                    + " include rows AFTER it), and a row of a later block reads the blocks before its own whose targets were known at predictAt (forward, no cross-fit) - one batch"
                                    + " yields the out-of-fold training values and the walk-forward evaluation values")
                    + (fitSpec.artifactUri == null ? "" : "; the whole-input statistics are persisted under " + fitSpec.artifactUri + "/<planHash>/ for a static serving run"));
        } else if (mode == FitMode.fold) {
            diagnostics.info("fit.mode.fold", loc, "fit.mode fold applies out-of-fold statistics (" + folds + " folds by "
                    + (groupBy == null ? "row identity (time.field + orderTieBreak)" : "entity " + groupBy) + "): a row never sees its own fold, "
                    + "but the other folds include rows AFTER it (cross-fit, not time-ordered)"
                    + (fitSpec.artifactUri == null ? "" : "; the whole-input statistics are persisted under " + fitSpec.artifactUri + "/<planHash>/ for a static serving run"));
            if (groupBy == null && spec.orderTieBreak.isEmpty()) {
                diagnostics.warning("fit.fold.identity", loc, "fit.mode fold without fit.groupBy or time.orderTieBreak assigns folds by time.field alone (rows sharing a timestamp share a fold); declare time.orderTieBreak for a row identity");
            }
        } else if (mode == FitMode.forward) {
            final ForwardBlocks blocks = fitSpec.forwardBlocks();
            diagnostics.info("fit.mode.forward", loc, "fit.mode forward reads, per row, the statistics of the complete time blocks (" + blocks.describe()
                    + ") whose targets are known at predictAt - a stepwise expanding fit computed as a parallel Combine per (key, block); the row's own block is never included"
                    + (fitSpec.minBlocksOf(blocks) <= 1 ? "" : "; rows with fewer than " + fitSpec.minBlocksOf(blocks) + " preceding blocks read null")
                    + (fitSpec.window == null ? "" : "; fit.window " + fitSpec.window + " bounds the blocks a row reads where a keySet declares no maxAge")
                    + (fitSpec.artifactUri == null ? "" : "; the whole-input statistics are persisted under " + fitSpec.artifactUri + "/<planHash>/ for a static serving run"));
        }
        if (mode != FitMode.forward && isStatic && def.keySets.stream().anyMatch(ks -> !ks.windows.isEmpty())) {
            diagnostics.warning("fit.mode.static.windows", loc, "keySet windows are ignored in fit.mode static (statistics cover the whole input)");
        }
        if (def.emitConfidence) diagnostics.warning("encoding.emitConfidence", loc, "emitConfidence is v1 and ignored");

        // baseline offset
        String offsetColumn = null;
        if (def.offset != null) {
            offsetColumn = baselineColumns.get(def.offset);
            if (offsetColumn == null) {
                diagnostics.error("encoding.offset", loc, "offset must reference baselines[].name: " + def.offset);
            } else if (!computeAt.equals(spec.predictAt)) {
                diagnostics.error("encoding.offset.computeAt", loc, "blocks referencing a baseline offset must be computed at predictAt (computeAt must not differ)");
            }
        }

        // targets: resolve / desugar
        record ResolvedTarget(String name, String reference, List<String> stats, List<String> values) {}
        final List<ResolvedTarget> resolvedTargets = new ArrayList<>();
        int targetIndex = 0;
        for (final Target t : def.targets) {
            targetIndex++;
            if (t.ref) {
                diagnostics.error("encoding.nested", loc, "nested encoding (targets[].field.ref) is v1 and not implemented yet");
                continue;
            }
            String reference = t.field;
            String name = t.as != null ? t.as : t.field == null ? "" : displayName(t.field);
            if (t.expr != null) {
                final OutputColumn anonymous = desugarExpression(def, t.expr, computeAt);
                if (anonymous == null) continue;
                reference = anonymous.canonicalName;
                if (t.as == null) name = "e" + targetIndex;
            }
            final List<String> stats = t.stats.isEmpty() ? List.of(reference == null ? "count" : "mean") : t.stats;
            for (final String stat : stats) {
                final OperatorCatalog.Stat s = OperatorCatalog.stat(stat);
                if (s == null) {
                    diagnostics.error("encoding.stat", loc, "unknown stat: " + stat + " (available: " + OperatorCatalog.AVAILABLE_STATS + ")");
                } else if (s.requiresTarget() && reference == null) {
                    diagnostics.error("encoding.stat.target", loc, "stat " + stat + " requires a target field or expr");
                }
            }
            if (reference != null) {
                final Ref ref = resolve(reference);
                if (ref != null && !isOutcomeLike(ref)) {
                    diagnostics.hint("encoding.target.preEvent", loc, "target '" + name + "' is known before the event; expanding fit is not essential for leak safety here");
                }
            }
            if (!t.values.isEmpty() && !stats.contains("distribution")) {
                // values expands the distribution map into one column per category (like countByValue); no other stat is a map
                diagnostics.error("encoding.target.values", loc, "target '" + name + "' declares values " + t.values + " but no stat distribution; values lists the categories of a distribution to emit as columns");
            }
            resolvedTargets.add(new ResolvedTarget(name, reference, stats, t.values));
        }

        // keySets: shrinkage config and generalization lattice (§5.3.1)
        record Lattice(KeySet keySet, Shrinkage shrinkage, List<List<String>> levels, int additiveAt) {}
        final Map<String, KeySet> singleKeySets = new HashMap<>();
        for (final KeySet ks : def.keySets) if (ks.keys.size() == 1) singleKeySets.putIfAbsent(ks.keys.get(0), ks);
        final List<Lattice> lattices = new ArrayList<>();
        for (final KeySet ks : def.keySets) {
            if (ks.keys.isEmpty()) {
                diagnostics.error("encoding.keySet.keys", loc, "each keySet requires 'keys'");
                continue;
            }
            final Shrinkage shrinkage = Shrinkage.parse(defShrinkage, defSmoothing, parseJsonObject(ks.shrinkageJson), diagnostics, loc);
            final List<List<String>> levels = new ArrayList<>();
            levels.add(ks.keys);
            int additiveAt = -1;
            final String structure = ks.structure == null ? "flat" : ks.structure;
            // "an explicit lattice was declared" = a non-empty hierarchy array. A null / empty / malformed one is
            // NOT a declaration: it must not silence the derivation a `structure` asks for (a bare `hierarchy:` in
            // YAML parses to JSON null, which is a non-null JsonElement)
            final JsonElement declaredHierarchy = ks.hierarchyJson == null ? null : JsonParser.parseString(ks.hierarchyJson);
            if (declaredHierarchy != null && !declaredHierarchy.isJsonNull() && !declaredHierarchy.isJsonArray()) {
                diagnostics.error("encoding.hierarchy.entry", loc, "hierarchy must be a list of key lists, 'additive' or []: " + ks.hierarchyJson);
            }
            final JsonElement hierarchy = declaredHierarchy != null && declaredHierarchy.isJsonArray()
                    && !declaredHierarchy.getAsJsonArray().isEmpty() ? declaredHierarchy : null;
            if (hierarchy != null) {
                for (final JsonElement entry : hierarchy.getAsJsonArray()) {
                    if (entry.isJsonPrimitive() && Shrinkage.ADDITIVE.equals(entry.getAsString())) {
                        if (additiveAt >= 0) diagnostics.error("encoding.hierarchy.additive", loc, "hierarchy may contain 'additive' once");
                        additiveAt = levels.size();
                        levels.add(List.of(Shrinkage.ADDITIVE));
                    } else if (entry.isJsonArray()) {
                        final List<String> keys = new ArrayList<>();
                        for (final JsonElement k : entry.getAsJsonArray()) {
                            if (!k.isJsonPrimitive()) continue;
                            if (!resolves(k.getAsString())) diagnostics.error("encoding.hierarchy.key", loc, "unknown hierarchy key: " + k.getAsString());
                            keys.add(k.getAsString());
                        }
                        if (!keys.isEmpty()) levels.add(keys);
                    } else {
                        diagnostics.error("encoding.hierarchy.entry", loc, "hierarchy entries must be key lists, 'additive' or []");
                    }
                }
            }
            switch (structure) {
                case "flat" -> { }
                case "hierarchy" -> {
                    if (ks.parentRef == null) diagnostics.error("encoding.keySet.parentRef", loc, "structure: hierarchy requires parentRef");
                    else if (hierarchy == null) levels.add(List.of(ks.parentRef));
                }
                case "cross" -> {
                    if (ks.keys.size() < 2) diagnostics.error("encoding.keySet.cross", loc, "structure: cross requires at least two keys");
                    if (hierarchy == null) {
                        additiveAt = levels.size();
                        levels.add(List.of(Shrinkage.ADDITIVE));
                    }
                }
                case "sequence" -> {
                    // the keys are a path declared most recent first: the lattice backs off by forgetting the oldest
                    // step, a chain of nested key lists like an explicit hierarchy (leave-node-out holds: the rows
                    // of a path are rows of each of its suffixes)
                    if (ks.keys.size() < 2) {
                        diagnostics.error("encoding.keySet.sequence", loc, "structure: sequence requires at least two keys (a path, most recent first): " + ks.keys);
                    } else if (hierarchy == null) {
                        for (int n = ks.keys.size() - 1; n >= 1; n--) levels.add(List.copyOf(ks.keys.subList(0, n)));
                        final String chain = String.join(" -> ", levels.stream().map(Object::toString).toList()) + " -> global";
                        if (shrinkage.enabled) {
                            diagnostics.info("encoding.keySet.sequence", loc, "structure: sequence on " + ks.keys + " (most recent first) shrinks along "
                                    + chain + ": a row whose older keys are null or unseen reads its longest known suffix");
                        } else {
                            // the lattice is only composed for a shrunk statistic: without shrinkage the block emits the
                            // raw full-path statistic and the back-off the structure asks for never happens
                            diagnostics.warning("encoding.keySet.sequence", loc, "structure: sequence on " + ks.keys + " derives " + chain
                                    + " but the block declares no shrinkage: the emitted statistic is the raw full-path value and a row whose path"
                                    + " is unseen reads null; declare shrinkage (e.g. shrinkage: {priorWeight: 20}) to back off along the suffixes");
                        }
                    }
                }
                default -> diagnostics.error("encoding.keySet.structure", loc, "structure must be flat | hierarchy | cross | sequence: " + structure);
            }
            if (additiveAt >= 0) {
                for (final String key : ks.keys) {
                    final KeySet main = singleKeySets.get(key);
                    if (main == null) {
                        diagnostics.error("encoding.hierarchy.additive", loc, "'additive' on keys " + ks.keys + " requires a single-key keySet for '" + key + "' in the same block");
                    } else if (!sameWindows(main.windows, ks.windows)) {
                        diagnostics.error("encoding.hierarchy.additive", loc, "'additive' on keys " + ks.keys + " requires the single-key keySet for '" + key + "' to declare the same windows");
                    }
                }
                final boolean scaleDeclared = (defShrinkage != null && defShrinkage.has("scale"))
                        || (parseJsonObject(ks.shrinkageJson) != null && parseJsonObject(ks.shrinkageJson).has("scale"));
                if (!scaleDeclared) diagnostics.error("encoding.hierarchy.scale", loc, "a lattice with 'additive' requires shrinkage.scale (identity | logit | log)");
                if (shrinkage.estimator == Shrinkage.Estimator.backoff) {
                    diagnostics.error("encoding.shrinkage.estimator", loc, "estimator: backoff is not valid for an overlapping lattice (additive / cross); use sequential or joint");
                }
                if (additiveAt != levels.size() - 1) {
                    diagnostics.error("encoding.hierarchy.additive", loc, "'additive' must be the last entry before the global level");
                }
            }
            // one error per block: the fold is the block's, whichever keySets ask for the joint estimator
            if (shrinkage.estimator == Shrinkage.Estimator.joint && mode == FitMode.fold && fitSpec.isTimeFold() && hintedBlocks.add(def.name + "#timeFoldJoint")) {
                diagnostics.error("fit.fold.time.joint", loc, "estimator: joint solves hash folds only: fit.fold.by: time is not implemented for the joint cell table (use backoff / sequential, or by: row)");
            }
            if (shrinkage.estimator == Shrinkage.Estimator.joint && !isStatic) {
                // the joint solve needs the whole cell table of the lattice: a fit-stage estimator, not a row-local replay
                diagnostics.error("encoding.shrinkage.estimator", loc, "estimator: joint fits every level of the lattice simultaneously over the fitted input and requires fit.mode static | fold | forward (expanding is row-local: use sequential or backoff)");
            }
            // a time fold ignores groupBy (every block is a fold): an entity's rows in the other blocks still carry keys
            // derived from this row's outcome, so groupBy must not silence the guard there
            if (mode == FitMode.fold && (groupBy == null || fitSpec.isTimeFold())) {
                for (final String key : ks.keys) {
                    final Ref ref = resolve(key);
                    if (ref != null && isOutcomeLike(ref)) {
                        diagnostics.error("fit.groupBy.required", loc, "keySet key '" + key + "' derives from a past target; " + (fitSpec.isTimeFold()
                                ? "fit.fold.by: time cannot keep the entity's rows of the other blocks (whose keys carry this row's outcome) out of its statistics: use fit.fold.by: row with fit.groupBy (entity-level folds)"
                                : "fit.mode fold requires fit.groupBy (entity-level folds)"));
                    }
                }
            }
            final boolean needsRoot = shrinkage.enabled || levels.size() > 1
                    || resolvedTargets.stream().anyMatch(t -> t.stats.contains("share"));
            if (needsRoot) levels.add(List.of());
            lattices.add(new Lattice(ks, shrinkage, levels, additiveAt));
        }
        if (lattices.stream().anyMatch(l -> offsetTerm(def, l.shrinkage))) {
            // spec §3 rule 5: the offset is an additive term on the shrinkage scale. The levels keep Σ baseline (and, on
            // logit, Σ b(1 − b)) next to Σ(y − b), each level's term is the score-type one-step estimate S / V from the
            // baseline, and the composed value is that term (a log-odds / log-rate ratio against the baseline), not a
            // probability / rate
            diagnostics.info("encoding.offset.additive", loc, "offset '" + def.offset + "' on a logit / log shrinkage scale: the composed value is the additive term on that scale"
                    + " (the key's score-type estimate sum(y - b) / information, one scoring step from the baseline, shrunk toward the parent's term by information;"
                    + " deviations on the same scale) - not a probability / rate");
        }

        // expansion: keySet × window × target × stat (product) or zip(keySet, target) × window × stat
        final String naming = def.naming != null ? def.naming : "{block}__{keys}__{window}__{target}__{stat}";
        int produced = 0;
        final List<int[]> pairs = new ArrayList<>();
        if (def.combine == Combine.zip) {
            final int n = Math.min(lattices.size(), resolvedTargets.size());
            if (lattices.size() != resolvedTargets.size()) diagnostics.warning("encoding.zip", loc, "combine: zip with unequal keySets/targets; extra entries are dropped");
            for (int i = 0; i < n; i++) pairs.add(new int[]{i, i});
        } else {
            for (int i = 0; i < lattices.size(); i++) for (int j = 0; j < resolvedTargets.size(); j++) pairs.add(new int[]{i, j});
        }
        // the global level is shared by every keySet: register it first so it is the first keyed stage (a static /
        // joint fit places its columns in the block's fit stage regardless of order)
        for (final int[] pair : pairs) {
            final Lattice lattice = lattices.get(pair[0]);
            final ResolvedTarget target = resolvedTargets.get(pair[1]);
            if (!isStatic && lattice.levels.get(lattice.levels.size() - 1).isEmpty()) {
                final boolean distribution = target.stats.contains("distribution") && lattice.shrinkage.enabled
                        && lattice.shrinkage.resolveFamily("distribution") != null;
                for (final Window window : windowsOf(lattice.keySet)) {
                    // a shrunk distribution needs the per-category shares of the level, the scalar statistics its sum
                    if (target.stats.stream().anyMatch(s -> !"distribution".equals(s)) || !distribution) {
                        levelStats(def, List.of(), lookupWindow(window, mode, def), target.name, target.reference, offsetColumn, offsetScale(def, lattice.shrinkage), computeAt, mode, fitSpec, false);
                    }
                    if (distribution) {
                        levelStats(def, List.of(), lookupWindow(window, mode, def), target.name, target.reference, offsetColumn, null, computeAt, mode, fitSpec, true);
                    }
                }
            }
        }
        for (final int[] pair : pairs) {
            final Lattice lattice = lattices.get(pair[0]);
            final KeySet ks = lattice.keySet;
            final ResolvedTarget target = resolvedTargets.get(pair[1]);
            for (final Window declaredWindow : windowsOf(ks)) {
                final Window window = lookupWindow(declaredWindow, mode, def);
                final Map<String, String> names = new HashMap<>(Map.of(
                        "block", def.name,
                        // the visible columns only: the hidden level statistics are named by their keys (levelStats),
                        // because every keySet of the block whose lattice contains the level shares them
                        "keys", ks.as != null ? ks.as : String.join("_", ks.keys),
                        "window", window == null ? "" : window.token(),
                        "target", target.name));
                for (final String stat : target.stats) {
                    final OperatorCatalog.Stat s = OperatorCatalog.stat(stat);
                    if (s == null || (s.requiresTarget() && target.reference == null)) continue;
                    names.put("stat", stat);
                    final String canonical = render(naming, names);
                    if (isStatic && "distribution".equals(stat)) {
                        // a static fit keeps (n, Σy, Σy²) per leaf: the per-key value distribution exists in the expanding replay only
                        diagnostics.error("encoding.stat.static", loc, "stat distribution is not available in fit.mode " + mode.token() + " (expanding only)");
                        continue;
                    }
                    // the family the statistic shrinks under (§5.1.1): declared or derived; null = not a shrunk statistic
                    Shrinkage.Family family = null;
                    if (lattice.shrinkage.enabled && Shrinkage.familyFor(stat) != null) {
                        family = lattice.shrinkage.resolveFamily(stat);
                        if (family == null) {
                            // a distribution on logit / log: no Gaussian approximation of the Dirichlet-Multinomial, so it is emitted unshrunk
                            if (hintedBlocks.add(def.name + "#familyScale#" + stat)) {
                                diagnostics.warning("encoding.shrinkage.family.scale", loc, "stat distribution is not shrunk on scale " + lattice.shrinkage.scale
                                        + " (dirichletMultinomial needs scale identity); the raw per-key distribution is emitted");
                            }
                        } else if (!checkFamily(lattice.shrinkage, family, stat, lattice.additiveAt >= 0, def)) {
                            continue;
                        }
                    }
                    final boolean shrunk = family != null;
                    if (isStatic && !shrunk && !"share".equals(stat)) {
                        // static: every statistic is derived from the fitted leaf sufficient statistics
                        if (!s.sufficient()) {
                            // needs the per-key value distribution, not the (n, Σy, Σy²) the fit keeps
                            diagnostics.error("encoding.stat.static", loc, "stat " + stat + " is not available in fit.mode " + mode.token() + " (expanding only)");
                            continue;
                        }
                        // unshrunk: the statistic is read straight from the leaf's (n, Σy, Σy²), never as an offset term
                        // static / fold: no window (lookupWindow is null); forward: the keySet's maxAge rounded to blocks
                        final Shrinkage.Level leaf = levelStats(def, ks.keys, window, target.name, target.reference, offsetColumn, null, computeAt, mode, fitSpec, false);
                        final OutputColumn c = newColumn(def.name, Scope.row, "fitStat", canonical, s.output(), computeAt);
                        c.fitted = true;
                        c.coordinates.put("keys", String.join(",", ks.keys));
                        c.coordinates.put("target", target.name);
                        c.coordinates.put("stat", stat);
                        c.coordinates.put("levels", Shrinkage.encodeLevels(List.of(leaf)));
                        addSelfInput(c, leaf.nColumn());
                        addSelfInput(c, leaf.sumColumn());
                        final String sumSq = leaf.nColumn().substring(0, leaf.nColumn().length() - "__n".length()) + "__sumsq";
                        if (columnsByCanonical.containsKey(sumSq)) addSelfInput(c, sumSq);
                        finishComposed(c, def);
                        produced++;
                        continue;
                    }
                    if (!shrunk && !"share".equals(stat)) {
                        // raw statistic straight from the keySet's own history (count / std / distribution / unshrunk mean)
                        final OutputColumn c = newColumn(def.name, Scope.population, "encoding", canonical, s.output(), computeAt);
                        populationColumn(c, ks, window, target.reference, stat, offsetColumn, mode, def, fitSpec);
                        register(c);
                        if ("distribution".equals(stat)) expandDistributionValues(def, target.values, c, computeAt);
                        produced++;
                        continue;
                    }
                    if (lattice.shrinkage.estimator == Shrinkage.Estimator.joint && !"share".equals(stat)) {
                        // joint: no hidden level statistics — the fit stage solves the lattice and fills the columns by lookup
                        produced += expandJoint(def, ks, lattice.levels, singleKeySets, window, target.name, target.reference, stat, family,
                                lattice.shrinkage, offsetColumn, computeAt, mode, fitSpec, naming, names);
                        continue;
                    }
                    // lattice: hidden statistics per level, composed in a row column
                    final boolean distribution = "distribution".equals(stat);
                    final Shrinkage.Scale offsetTerm = offsetScale(def, lattice.shrinkage);
                    final List<Shrinkage.Level> levels = new ArrayList<>();
                    for (final List<String> levelKeys : lattice.levels) {
                        if (levelKeys.size() == 1 && Shrinkage.ADDITIVE.equals(levelKeys.get(0))) {
                            final List<List<Shrinkage.Level>> mains = new ArrayList<>();
                            for (final String key : ks.keys) {
                                final KeySet main = singleKeySets.get(key);
                                if (main == null) continue;
                                final List<Shrinkage.Level> chain = new ArrayList<>();
                                chain.add(levelStats(def, main.keys, window, target.name, target.reference, offsetColumn, offsetTerm, computeAt, mode, fitSpec, distribution));
                                chain.add(levelStats(def, List.of(), window, target.name, target.reference, offsetColumn, offsetTerm, computeAt, mode, fitSpec, distribution));
                                mains.add(chain);
                            }
                            levels.add(new Shrinkage.Level(Shrinkage.ADDITIVE, null, null, mains));
                        } else {
                            levels.add(levelStats(def, levelKeys, window, target.name, target.reference, offsetColumn, offsetTerm, computeAt, mode, fitSpec, distribution));
                        }
                    }
                    if ("share".equals(stat)) {
                        final OutputColumn c = newColumn(def.name, Scope.row, "share", canonical, Schema.FieldType.FLOAT64, computeAt);
                        c.fitted = true;
                        c.coordinates.put("keys", String.join(",", ks.keys));
                        c.coordinates.put("target", target.name);
                        c.coordinates.put("stat", stat);
                        c.coordinates.put("levels", Shrinkage.encodeLevels(List.of(levels.get(0), levels.get(levels.size() - 1))));
                        for (final Shrinkage.Level l : List.of(levels.get(0), levels.get(levels.size() - 1))) addSelfInput(c, l.nColumn());
                        finishComposed(c, def);
                        produced++;
                        continue;
                    }
                    final Shrinkage shrinkage = lattice.shrinkage;
                    final String encoded = Shrinkage.encodeLevels(levels);
                    if (shrinkage.emits("composed")) {
                        final OutputColumn c = newColumn(def.name, Scope.row, "compose", canonical,
                                distribution ? Schema.FieldType.map(Schema.FieldType.FLOAT64) : Schema.FieldType.FLOAT64, computeAt);
                        composeCoordinates(c, ks, target, stat, encoded, shrinkage, levels);
                        c.coordinates.put("family", family.name());
                        finishComposed(c, def);
                        if (distribution) expandDistributionValues(def, target.values, c, computeAt);
                        produced++;
                    }
                    if (shrinkage.emits("deviations") && distribution) {
                        if (hintedBlocks.add(def.name + "#distributionDeviations")) {
                            diagnostics.warning("encoding.shrinkage.output", loc, "deviations are not defined for a shrunk distribution (per-category pseudo-counts); none emitted for stat distribution");
                        }
                    } else if (shrinkage.emits("deviations")) {
                        for (int i = 0; i < levels.size() - 1; i++) {
                            names.put("stat", "dev" + i);
                            final OutputColumn c = newColumn(def.name, Scope.row, "deviation", render(naming, names), Schema.FieldType.FLOAT64, computeAt);
                            composeCoordinates(c, ks, target, stat, encoded, shrinkage, levels);
                            c.coordinates.put("level", Integer.toString(i));
                            c.coordinates.put("levelKeys", levels.get(i).token());
                            finishComposed(c, def);
                            produced++;
                        }
                    }
                    if (shrinkage.emits("effectiveN")) {
                        names.put("stat", stat + "__neff");
                        final OutputColumn c = newColumn(def.name, Scope.row, "effectiveN", render(naming, names), Schema.FieldType.FLOAT64, computeAt);
                        composeCoordinates(c, ks, target, stat, encoded, shrinkage, levels);
                        c.coordinates.put("family", family.name());
                        finishComposed(c, def);
                        produced++;
                    }
                }
            }
        }
        if (def.maxFeatures != null && produced > def.maxFeatures) {
            diagnostics.error("encoding.maxFeatures", loc, "expansion produces " + produced + " columns, exceeding maxFeatures " + def.maxFeatures);
        }
        if (produced == 0 && !diagnostics.hasErrors()) {
            diagnostics.error("encoding.empty", loc, "encoding expands to no columns");
        }
    }

    /**
     * The window a lookup fit keeps: none in static / fold (whole-input statistics), the {@code maxAge} part in
     * forward (rounded to blocks by {@link #forwardCoordinates}; {@code maxEvents} / {@code filter} are dropped
     * with a warning once per block), the declared window in expanding.
     */
    private Window lookupWindow(final Window declared, final FitMode mode, final FeatureDef def) {
        if (declared == null || !mode.isLookup()) return declared;
        if (mode != FitMode.forward) return null;
        if ((declared.maxEvents != null || declared.filter != null) && hintedBlocks.add(def.name + "#forwardWindowIgnored")) {
            diagnostics.warning("fit.mode.forward.windowIgnored", def.location(), "maxEvents / filter windows are ignored in fit.mode forward (statistics are per block; only maxAge applies, rounded to blocks)");
        }
        if (declared.maxAge == null && !declared.onCalendar()) return null;
        final Window window = new Window();
        window.maxAge = declared.maxAge;
        window.as = declared.as;
        // a window on a calendar clock is counted in the blocks' ticks (forwardCoordinates)
        window.clock = declared.clock;
        window.maxAgeTicks = declared.maxAgeTicks;
        return window;
    }

    private static List<Window> windowsOf(final KeySet ks) {
        return ks.windows.isEmpty() ? Collections.singletonList(null) : ks.windows;
    }

    /** The windows of two keySets select the same rows — their names ({@code as}) are not part of the comparison. */
    private static boolean sameWindows(final List<Window> a, final List<Window> b) {
        if (a.size() != b.size()) return false;
        for (int i = 0; i < a.size(); i++) {
            if (!a.get(i).sameBounds(b.get(i))) return false;
        }
        return true;
    }

    private void composeCoordinates(final OutputColumn c, final KeySet ks, final Object target, final String stat,
                                    final String levels, final Shrinkage shrinkage, final List<Shrinkage.Level> chain) {
        c.fitted = true;
        c.coordinates.put("keys", String.join(",", ks.keys));
        c.coordinates.put("stat", stat);
        c.coordinates.put("levels", levels);
        c.coordinates.put("scale", shrinkage.scale.name());
        // a shrunk distribution has no scalar target to estimate variance components from: its levels shrink with
        // priorWeight, so the column declares fixed weights and never reads the stage's λ estimates (which its hidden
        // n columns may share with a scalar statistic of the same target)
        c.coordinates.put("weights", "distribution".equals(stat) ? "fixed" : shrinkage.weights);
        c.coordinates.put("priorWeight", Double.toString(shrinkage.priorWeight));
        c.coordinates.put("leaveNodeOut", Boolean.toString(shrinkage.leaveNodeOut));
        c.coordinates.put("estimator", chain.stream().anyMatch(Shrinkage.Level::isAdditive) ? "sequential" : "backoff");
        if (ks.structure != null) c.coordinates.put("structure", ks.structure);
        for (final Shrinkage.Level l : chain) addLevelInputs(c, l);
    }

    private void addLevelInputs(final OutputColumn c, final Shrinkage.Level level) {
        for (final Shrinkage.Level l : Shrinkage.leaves(List.of(level))) {
            addSelfInput(c, l.nColumn());
            addSelfInput(c, l.sumColumn());
            if (l.offColumn() != null) addSelfInput(c, l.offColumn());
            if (l.infoColumn() != null) addSelfInput(c, l.infoColumn());
        }
    }

    /**
     * {@code targets[].values} on stat {@code distribution}: the map column becomes an intermediate and one FLOAT64
     * column per listed category reads its share from it ({@code <map column>_<value>}, 0 when the category has no
     * mass, null when the map is null), like {@code countByValue} with {@code values} — a sink such as BigQuery
     * and a model consume flat numeric columns, not a map.
     */
    private void expandDistributionValues(final FeatureDef def, final List<String> values, final OutputColumn map, final AvailableAt computeAt) {
        if (values.isEmpty() || !columnsByCanonical.containsKey(map.canonicalName)) return;
        map.intermediate = true;
        for (final String value : values) {
            final OutputColumn c = newColumn(def.name, Scope.row, "mapValue", map.canonicalName + "_" + value, Schema.FieldType.FLOAT64, computeAt);
            c.coordinates.put("value", value);
            c.fitted = map.fitted;
            addSelfInput(c, map.canonicalName);
            finishRow(c, def);
        }
    }

    private void finishComposed(final OutputColumn c, final FeatureDef def) {
        if (c.availableAt == null) c.availableAt = AvailableAt.atEventTime();
        // the hidden statistics are available at computeAt by construction; the composed value inherits that
        c.status = rowStatus(c.availableAt, c.computeAt);
        c.validFor = def.validFor;
        register(c);
    }

    /**
     * Hidden sufficient statistics ({@code n}, {@code sum} — or the per-category {@code dist} shares of a shrunk
     * distribution) of one lattice level for a (window, target), registered once per block and shared by every
     * keySet whose lattice contains the level.
     */
    /**
     * Whether the lattice composes its levels as offset terms (spec §3 rule 5): a baseline offset shrunk on a logit /
     * log scale. Its levels then also keep Σ baseline ({@code __sumoff}); an identity or unshrunk lattice of the same
     * block does not (its statistics are the plain residual Σ(y − b) / n).
     */
    private static boolean offsetTerm(final FeatureDef def, final Shrinkage shrinkage) {
        return def.offset != null && shrinkage.enabled && shrinkage.scale != Shrinkage.Scale.identity;
    }

    /** The scale the block's offset term is composed on ({@link #offsetTerm}), null when the block has no offset term. */
    private static Shrinkage.Scale offsetScale(final FeatureDef def, final Shrinkage shrinkage) {
        return offsetTerm(def, shrinkage) ? shrinkage.scale : null;
    }

    /**
     * @param offsetScale the scale the level is composed on as an offset term ({@link #offsetScale}; null = no offset
     *                    term): register its hidden Σ baseline too and, on logit, its Σ b(1 − b) — the information the
     *                    score-type term divides by ({@link Shrinkage#ownScore}; on log the information is Σ baseline)
     */
    private Shrinkage.Level levelStats(final FeatureDef def, final List<String> levelKeys, final Window window,
                                       final String targetName, final String targetReference, final String offsetColumn,
                                       final Shrinkage.Scale offsetScale,
                                       final AvailableAt computeAt, final FitMode mode, final FeatureSpec.FitSpec fitSpec,
                                       final boolean distribution) {
        final String token = levelKeys.isEmpty() ? Shrinkage.GLOBAL : String.join("_", levelKeys);
        final String base = render("{block}__{keys}__{window}__{target}", Map.of(
                "block", def.name, "keys", token, "window", window == null ? "" : window.token(), "target", targetName));
        final String nName = base + "__n";
        final String valueName = base + (distribution ? "__dist" : "__sum");
        final String offName = base + "__" + PopulationEvaluator.SUM_OFFSET;
        final String infoName = base + "__" + PopulationEvaluator.SUM_INFO;
        final boolean isStatic = mode.isLookup();
        // an offset term also keeps Σ baseline and, on logit, Σ b(1 − b) (the level's term is S / V)
        final boolean offsetSum = offsetScale != null && !distribution && targetReference != null;
        final boolean offsetInfo = offsetSum && offsetScale == Shrinkage.Scale.logit;
        // static / fold fits also keep Σy² so std can be derived from the artifact
        final List<String> stats = new ArrayList<>(distribution ? List.of("count", "distribution") : isStatic ? List.of("count", "sum", "sumsq") : List.of("count", "sum"));
        if (offsetSum) stats.add(PopulationEvaluator.SUM_OFFSET);
        if (offsetInfo) stats.add(PopulationEvaluator.SUM_INFO);
        for (final String stat : stats) {
            final String name = switch (stat) {
                case "count" -> nName;
                case "sum", "distribution" -> valueName;
                case PopulationEvaluator.SUM_OFFSET -> offName;
                case PopulationEvaluator.SUM_INFO -> infoName;
                default -> base + "__sumsq";
            };
            if (!"count".equals(stat) && targetReference == null) continue;
            final OutputColumn existing = columnsByCanonical.get(name);
            if (existing != null) {
                // the level's hidden columns are shared: an unshrunk statistic of the same keys (fit.mode static / fold /
                // forward reads n / Σy / Σy² straight from the leaf) may have registered them before the lattice did, so
                // the score scale is stamped on whatever is already there — the engine reads it from any of them
                if (offsetSum) existing.coordinates.put("scoreScale", offsetScale.name());
                continue;
            }
            final OutputColumn c = newColumn(def.name, Scope.population, "encoding", name,
                    "distribution".equals(stat) ? Schema.FieldType.map(Schema.FieldType.FLOAT64) : Schema.FieldType.FLOAT64, computeAt);
            c.intermediate = true;
            c.anonymous = true;
            final KeySet level = new KeySet();
            level.keys = levelKeys;
            level.windows = window == null ? new ArrayList<>() : List.of(window);
            populationColumn(c, level, window, targetReference, stat, offsetColumn, mode, def, fitSpec);
            // the engine reads the score scale of a level from its hidden columns (the fit stage's λ estimate, the artifact)
            if (offsetSum) c.coordinates.put("scoreScale", offsetScale.name());
            register(c);
        }
        return new Shrinkage.Level(token, nName, targetReference == null ? nName : valueName, offsetSum ? offName : null,
                offsetSum ? (offsetInfo ? infoName : offName) : null, null);
    }

    /**
     * §5.1.1 / §5.5 family checks for one shrunk statistic: the family must shrink the statistic's sufficient
     * statistics, a <i>declared</i> conjugate closed form needs the identity scale (a derived family already
     * falls back to the Gaussian approximation of rule 7 on logit / log, see {@link Shrinkage#resolveFamily}),
     * and a distribution shrinks along chain lattices only (per-category pseudo-counts have no additive
     * decomposition). A joint fit never sees a distribution: it is a fit-stage estimator and the statistic is
     * expanding-only ({@code encoding.stat.static}).
     */
    private boolean checkFamily(final Shrinkage shrinkage, final Shrinkage.Family family, final String stat, final boolean additive, final FeatureDef def) {
        final String loc = def.location();
        if (!family.accepts(stat)) {
            if (hintedBlocks.add(def.name + "#family#" + stat)) {
                diagnostics.error("encoding.shrinkage.family.stat", loc, "family " + family + " does not shrink stat " + stat
                        + " (gaussian | betaBinomial | gammaPoisson: mean / rate; dirichletMultinomial: distribution)");
            }
            return false;
        }
        if (shrinkage.family != null && family.isConjugate() && shrinkage.scale != Shrinkage.Scale.identity) {
            if (hintedBlocks.add(def.name + "#familyScale")) {
                diagnostics.error("encoding.shrinkage.family.scale", loc, "family " + family + " (a conjugate closed form) requires scale identity; on logit / log declare family gaussian or leave it derived (Gaussian shrinkage of the transformed statistics, section 5.5 rule 7)");
            }
            return false;
        }
        if ("distribution".equals(stat)) {
            if (additive) {
                if (hintedBlocks.add(def.name + "#familyLattice")) {
                    diagnostics.error("encoding.shrinkage.family.lattice", loc, "a shrunk distribution (dirichletMultinomial) supports chain lattices only; 'additive' / cross has no per-category decomposition");
                }
                return false;
            }
            if ("varianceComponents".equals(shrinkage.weights) && hintedBlocks.add(def.name + "#familyWeights")) {
                diagnostics.warning("encoding.shrinkage.weights.distribution", loc, "weights: varianceComponents is not estimated for a distribution (no scalar target); its levels shrink with priorWeight " + shrinkage.priorWeight);
            }
        }
        return true;
    }

    /**
     * {@code estimator: joint} (§5.5 rule 1): one {@code joint} column per output kind, filled in the block's fit
     * stage from a {@link JointFit} solved over the lattice cells. The lattice's statistics-carrying levels
     * ({@code additive} → the main-effect key lists) become the effect levels, the global level the intercept;
     * every key field of every level is a self input (the fit projects the cell onto each level).
     */
    private int expandJoint(final FeatureDef def, final KeySet ks, final List<List<String>> latticeLevels, final Map<String, KeySet> singleKeySets,
                            final Window window, final String targetName, final String targetReference, final String stat,
                            final Shrinkage.Family family, final Shrinkage shrinkage, final String offsetColumn, final AvailableAt computeAt,
                            final FitMode mode, final FeatureSpec.FitSpec fitSpec, final String naming, final Map<String, String> names) {
        final String loc = def.location();
        final List<JointFit.Level> levels = new ArrayList<>();
        final Set<String> seen = new HashSet<>();
        for (final List<String> levelKeys : latticeLevels) {
            final List<List<String>> expanded = new ArrayList<>();
            if (levelKeys.size() == 1 && Shrinkage.ADDITIVE.equals(levelKeys.get(0))) {
                for (final String key : ks.keys) if (singleKeySets.containsKey(key)) expanded.add(List.of(key));
            } else {
                expanded.add(levelKeys);
            }
            for (final List<String> keys : expanded) {
                final String token = keys.isEmpty() ? Shrinkage.GLOBAL : String.join("_", keys);
                if (seen.add(token)) levels.add(new JointFit.Level(token, keys));
            }
        }
        if (levels.isEmpty() || !levels.get(levels.size() - 1).keys().isEmpty()) levels.add(new JointFit.Level(Shrinkage.GLOBAL, List.of()));
        final String id = render("{block}__{keys}__{window}__{target}", Map.of(
                "block", def.name, "keys", String.join("_", ks.keys), "window", window == null ? "" : window.token(), "target", targetName));
        // the id is key-derived (a keySet's `as` renames its columns, not the fit it reads), so a block declaring the
        // same keys twice resolves to one solve: the engine groups the columns by it and reads the parameters of the
        // first. Sharing is right when the lattice and the shrinkage are the same and wrong when they are not.
        final String signature = JointFit.encodeLevels(levels) + "|" + shrinkage.scale + "|" + shrinkage.weights + "|" + shrinkage.priorWeight;
        final String previousSignature = jointFits.putIfAbsent(id, signature);
        if (previousSignature != null && !previousSignature.equals(signature)) {
            diagnostics.error("encoding.shrinkage.joint", loc, "two keySets of block '" + def.name + "' resolve to the same joint fit '" + id
                    + "' with different lattices or shrinkage (" + previousSignature + " vs " + signature + "): the joint fit is identified by the keys,"
                    + " the window and the target - a keySet's as: renames its columns but not the fit they read, so the second would be filled from"
                    + " the first's solve; declare them in separate blocks");
            return 0;
        }
        if (hintedBlocks.add(def.name + "#joint#" + id)) {
            final List<String> tokens = new ArrayList<>();
            for (final JointFit.Level l : levels) tokens.add(l.token());
            diagnostics.info("encoding.shrinkage.joint", loc, "estimator joint fits the levels " + tokens + " of keySet " + ks.keys
                    + " simultaneously (ridge / BLUP over the lattice cells, solved on one worker; lambda per level "
                    + ("varianceComponents".equals(shrinkage.weights) ? "by the moment estimator over the level's contexts" : "= priorWeight " + shrinkage.priorWeight)
                    + "; scale " + shrinkage.scale + ")");
        }
        final List<OutputColumn> cols = new ArrayList<>();
        if (shrinkage.emits("composed")) {
            final OutputColumn c = newColumn(def.name, Scope.population, "joint", render(naming, names), Schema.FieldType.FLOAT64, computeAt);
            c.coordinates.put("kind", "composed");
            cols.add(c);
        }
        if (shrinkage.emits("deviations")) {
            for (int i = 0; i < levels.size() - 1; i++) {
                names.put("stat", "dev" + i);
                final OutputColumn c = newColumn(def.name, Scope.population, "joint", render(naming, names), Schema.FieldType.FLOAT64, computeAt);
                c.coordinates.put("kind", "deviation");
                c.coordinates.put("level", Integer.toString(i));
                c.coordinates.put("levelKeys", levels.get(i).token());
                cols.add(c);
            }
        }
        if (shrinkage.emits("effectiveN")) {
            names.put("stat", stat + "__neff");
            final OutputColumn c = newColumn(def.name, Scope.population, "joint", render(naming, names), Schema.FieldType.FLOAT64, computeAt);
            c.coordinates.put("kind", "effectiveN");
            cols.add(c);
        }
        for (final OutputColumn c : cols) {
            populationColumn(c, ks, window, targetReference, stat, offsetColumn, mode, def, fitSpec);
            c.coordinates.put("joint", id);
            c.coordinates.put("jointLevels", JointFit.encodeLevels(levels));
            c.coordinates.put("estimator", "joint");
            c.coordinates.put("family", family.name());
            c.coordinates.put("scale", shrinkage.scale.name());
            c.coordinates.put("weights", shrinkage.weights);
            c.coordinates.put("priorWeight", Double.toString(shrinkage.priorWeight));
            c.coordinates.put("predictOffsetMillis", Long.toString(spec.predictAt.getOffset().toMillis()));
            for (final JointFit.Level l : levels) for (final String key : l.keys()) addSelfInput(c, key);
            register(c);
        }
        return cols.size();
    }

    /** Common setup of a population column reading the keySet's own past contributions. */
    private void populationColumn(final OutputColumn c, final KeySet ks, final Window window, final String targetReference,
                                  final String stat, final String offsetColumn, final FitMode mode, final FeatureDef def,
                                  final FeatureSpec.FitSpec fitSpec) {
        c.fitted = true;
        final boolean lookup = mode.isLookup();
        if (lookup) {
            if (fitSpec.artifactUri != null) c.coordinates.put("artifactUri", fitSpec.artifactUri);
            if (fitSpec.refit) c.coordinates.put("refit", "true");
        }
        if (mode == FitMode.fold && fitSpec.isTimeFold()) {
            timeFoldCoordinates(c, targetReference, offsetColumn, def, fitSpec);
        } else if (mode == FitMode.fold) {
            // fold unit: the groupBy entity's keys, else the row identity (time.field + orderTieBreak; time.field
            // alone without a tie-break, so rows sharing a timestamp share a fold). Read at apply time only —
            // not a lineage input of the column (hashing must never involve outcome fields)
            final List<String> foldKeys = new ArrayList<>();
            if (fitSpec.groupBy != null && entities.containsKey(fitSpec.groupBy)) {
                foldKeys.addAll(entities.get(fitSpec.groupBy).keys());
            } else {
                foldKeys.add(spec.timeField);
                foldKeys.addAll(spec.orderTieBreak);
            }
            c.coordinates.put("foldKeys", String.join(",", foldKeys));
            c.coordinates.put("folds", String.valueOf(fitSpec.folds));
        }
        // a target-less level (row counts, share denominators) never reads the baseline: no lag from it either
        if (mode == FitMode.forward) forwardCoordinates(c, window, targetReference, targetReference == null ? null : offsetColumn, def, fitSpec);
        c.coordinates.put("keys", String.join(",", ks.keys));
        if (window != null) {
            c.coordinates.put("window", window.token());
            if (window.maxAge != null) c.coordinates.put("maxAge", window.maxAge.toString());
            calendarWindow(c, window, def.location());
            if (window.maxEvents != null) c.coordinates.put("maxEvents", window.maxEvents.toString());
            if (window.filter != null) {
                final String filterText = conditionText(window.filter, def.location(), "filter");
                if (filterText != null) c.coordinates.put("filter", filterText);
            }
        }
        if (targetReference != null) c.coordinates.put("field", canonicalOf(targetReference));
        c.coordinates.put("stat", stat);
        c.coordinates.put("fit", mode.token());
        if (ks.structure != null) c.coordinates.put("structure", ks.structure);
        for (final String key : ks.keys) addSelfInput(c, key);
        // target-less statistics (count / share denominators) count rows: the keys are self reads (keying), not projected
        if (targetReference != null) addPastInput(c, targetReference);
        // the offset is read where the target is: from the past rows (their baseline next to their outcome), never
        // from the current row — the composed value is the term δ alone. So its availability is the past side's, like
        // the target's (a baseline over an outcome shifts the window near edge, or delays a forward block), and a
        // baseline the row itself may not see yet is still a valid offset (spec §3 rule 3). A target-less level
        // (row counts, share denominators) counts every row and reads no baseline: it takes no shift from it
        if (offsetColumn != null) {
            if (targetReference != null) addPastInput(c, offsetColumn);
            c.coordinates.put("offset", def.offset);
        }
        if (window != null && window.filter != null) {
            final References fr = expressionReferences(window.filter);
            for (final String sf : fr.self) addSelfInput(c, sf);
            for (final String o : fr.others) addPastInput(c, o);
        }
        if (lookup) {
            finishStaticFitted(c, def);
        } else {
            classifyPast(c, null);
            c.validFor = def.validFor;
        }
    }

    /**
     * fit.mode forward (spec §5.6): the block geometry, the target's availability lag (the block must be complete
     * AND its targets known at predictAt: lag = the target's effective availability offset after its event,
     * settlement + ingestion; an attribute-only level has none), the window rounded to whole blocks, and the
     * time field the engine reads the row's block from.
     */
    private void forwardCoordinates(final OutputColumn c, final Window window, final String targetReference, final String offsetColumn,
                                    final FeatureDef def, final FeatureSpec.FitSpec fitSpec) {
        forwardCoordinates(c, window, fitReferences(targetReference, offsetColumn), def, fitSpec);
    }

    /**
     * @param window     the keySet window (its {@code maxAge} bounds the blocks a row reads), or null — then
     *                   {@code fit.window} does, when declared
     * @param references the fields the fit reads from a past row (target / offset / the vector inputs): the lag
     *                   is the largest post-event availability among them
     */
    private void forwardCoordinates(final OutputColumn c, final Window window, final List<String> references,
                                    final FeatureDef def, final FeatureSpec.FitSpec fitSpec) {
        final String loc = def.location();
        final ForwardBlocks blocks = fitSpec.forwardBlocks();
        blockCoordinates(c, blocks);
        c.coordinates.put("minBlocks", Integer.toString(fitSpec.minBlocksOf(blocks)));
        c.coordinates.put("forwardLagMillis", Long.toString(availabilityLag(references, def, "fit.mode forward")));
        // the blocks a row reads: the keySet's maxAge, else the block-level fit.window
        if (window != null && window.onCalendar()) {
            if (blocks.clock() == null || !blocks.clock().name().equals(window.clock)) {
                diagnostics.error("clock.fit", loc, "a keySet window on the clock '" + window.clock + "' under fit.mode forward needs fit.blocks on the same clock ({size: <ticks>, clock: "
                        + window.clock + "}); the blocks are " + blocks.describe());
            } else {
                final long k = Math.max(1, (window.maxAgeTicks + blocks.ticks() - 1) / blocks.ticks());
                c.coordinates.put("windowBlocks", Long.toString(k));
                if (hintedBlocks.add(def.name + "#forwardWindow")) {
                    diagnostics.info("fit.mode.forward.window", loc, "maxAge " + window.maxAgeTicks + " tick(s) of " + window.clock
                            + " is rounded up to " + k + " block(s) of " + blocks.describe() + " in fit.mode forward");
                }
            }
            return;
        }
        final Duration maxAge = window != null && window.maxAge != null ? window.maxAge : fitSpec.window;
        if (maxAge != null) {
            final int k = blocks.windowBlocks(maxAge);
            c.coordinates.put("windowBlocks", Integer.toString(k));
            if (hintedBlocks.add(def.name + "#forwardWindow")) {
                diagnostics.info("fit.mode.forward.window", loc, (window != null && window.maxAge != null ? "maxAge " : "fit.window ") + maxAge
                        + " is rounded up to " + k + " block(s) of " + blocks.describe() + " in fit.mode forward");
            }
        }
    }

    /**
     * A declared calendar clock by name, or null after reporting {@code clock.unknown} ({@code what} names the parameter:
     * {@code window.clock}, {@code decayBy}, {@code fit.blocks.clock}).
     */
    private Clock clock(final String name, final String loc, final String what) {
        final Clock clock = clocks.get(name);
        if (clock == null) {
            diagnostics.error("clock.unknown", loc, what + " '" + name + "' is neither a built-in clock " + Clock.BUILT_IN
                    + " nor declared in the sources' clocks" + (clocks.isEmpty() ? ""
                    : " (declared: " + String.join(", ", clocks.values().stream().map(Clock::describe).toList()) + ")"));
        }
        return clock;
    }

    /** A window on a calendar clock: its tick count and clock name in the coordinates, the calendar attached to the column. */
    private void calendarWindow(final OutputColumn c, final Window window, final String loc) {
        if (!window.onCalendar()) return;
        final Clock clock = clock(window.clock, loc, "window.clock");
        if (clock == null) return;
        c.coordinates.put("maxAgeTicks", Long.toString(window.maxAgeTicks));
        c.coordinates.put("windowClock", window.clock);
        c.clocks.put(window.clock, clock);
    }

    /** Resolves {@code fit.blocks.clock} of a block's fit to the declared calendar (reported once per block). */
    private void resolveBlockClock(final FeatureSpec.FitSpec fitSpec, final String loc) {
        if (fitSpec.blockClock == null) {
            fitSpec.blockCalendar = null;
            return;
        }
        fitSpec.blockCalendar = clocks.get(fitSpec.blockClock);
        if (fitSpec.blockCalendar == null && hintedBlocks.add(loc + "#blockClock")) clock(fitSpec.blockClock, loc, "fit.blocks.clock");
    }

    /** The time blocks of a forward fit or a time fold, and the time field (and its type) the engine reads a row's block from. */
    private void blockCoordinates(final OutputColumn c, final ForwardBlocks blocks) {
        if (blocks.clock() != null) {
            c.coordinates.put("blockClock", blocks.clock().name());
            c.coordinates.put("blockTicks", Integer.toString(blocks.ticks()));
            c.clocks.put(blocks.clock().name(), blocks.clock());
        } else if (blocks.bucket() != null) {
            c.coordinates.put("blockBucket", blocks.bucket());
        } else {
            c.coordinates.put("blockSizeMillis", Long.toString(blocks.sizeMillis()));
        }
        c.coordinates.put("blockField", spec.timeField);
        final FieldContract time = inputFields.get(spec.timeField);
        c.coordinates.put("blockFieldType", time == null || time.getType() == null ? "timestamp" : time.getType().getType().name());
    }

    /** The fields a lookup fit reads from a past row: the target and the offset baseline, in that order. */
    private static List<String> fitReferences(final String targetReference, final String offsetColumn) {
        final List<String> references = new ArrayList<>();
        if (targetReference != null) references.add(targetReference);
        if (offsetColumn != null) references.add(offsetColumn);
        return references;
    }

    /**
     * The availability lag of a forward-read block: the largest static post-event availability offset among the
     * references (target, offset, inputs) — the block must be complete AND its targets known at predictAt; a
     * pre-event / attribute-only reference has none. A dynamic availability is an error, reported once per reference.
     *
     * @param setting what asks for the forward read, named in the error ({@code fit.mode forward}, {@code fit.fold.until})
     */
    private long availabilityLag(final List<String> references, final FeatureDef def, final String setting) {
        long lag = 0;
        for (final String reference : references) {
            if (reference == null) continue;
            final Ref ref = resolve(reference);
            if (ref == null) continue;
            final AvailableAt at = ref.availableAt();
            if (at == null || at.isPreEvent()) continue;
            if (!at.isStatic()) {
                // once per block and reference: this runs per column, and a block has rank (x embedded values) of them
                if (hintedBlocks.add("fit.mode.forward.dynamic:" + def.name + ":" + reference)) diagnostics.error("fit.mode.forward.dynamic", def.location(), setting + " needs a static availability for '" + reference + "' (is " + at.describe() + "): the block boundary cannot be decided per row");
                continue;
            }
            lag = Math.max(lag, at.getOffset().toMillis());
        }
        return lag;
    }

    /**
     * {@code fit.fold.by: time}: the blocks, and the blocks left out around the row's own — the purge on both sides
     * of it (a training row whose label window overlaps the row's — before or after it — describes the same period;
     * default = the horizon of the target's label, info {@code fit.fold.purge}) and the embargo beyond the purge after
     * it, both rounded up to whole blocks. With {@code fit.fold.until} the cross-fit is confined to the blocks up to
     * the until block ({@code untilBlock}) and a row of a later block reads forward — the blocks before its own whose
     * targets were known at predictAt, the lag being the target's / offset's availability ({@code forwardLagMillis},
     * as for {@code fit.mode forward}).
     */
    private void timeFoldCoordinates(final OutputColumn c, final String targetReference, final String offsetColumn, final FeatureDef def, final FeatureSpec.FitSpec fitSpec) {
        final ForwardBlocks blocks = fitSpec.forwardBlocks();
        blockCoordinates(c, blocks);
        c.coordinates.put("foldBy", "time");
        if (fitSpec.untilMillis != null) {
            c.coordinates.put("untilBlock", Long.toString(blocks.indexOf(fitSpec.untilMillis)));
            c.coordinates.put("forwardLagMillis", Long.toString(availabilityLag(fitReferences(targetReference, offsetColumn), def,
                    "fit.fold.until (the rows after the training period read forward)")));
        }
        Duration purge = fitSpec.purge;
        if (purge == null && targetReference != null) {
            purge = labelHorizon(canonicalOf(targetReference), new HashSet<>());
            if (purge != null && hintedBlocks.add(def.name + "#purge:" + purge)) {
                diagnostics.info("fit.fold.purge", def.location(), "fit.fold.purge defaults to " + purge + ", the horizon of the label '" + targetReference
                        + "' (a training row whose label window overlaps a row's, before or after its block, is left out of it); declare fit.fold.purge to override");
            }
        }
        // covering (shortest-block) rounding, not the nominal one of windowBlocks: a leak guard must never under-cover
        c.coordinates.put("purgeBlocks", Integer.toString(purge == null || purge.isZero() ? 0 : blocks.coveringBlocks(purge)));
        final Duration embargo = fitSpec.embargo;
        c.coordinates.put("embargoBlocks", Integer.toString(embargo == null || embargo.isZero() ? 0 : blocks.coveringBlocks(embargo)));
    }

    /**
     * A calendar window ({@code maxAge: N, clock: X}) is bounded in ticks at its far edge but shifted on wall time at its
     * near edge, so a shift longer than the clock's spacing hides the newest ticks whatever N is: the info says how
     * many of the N ticks the shift covers on average (shift / mean tick spacing), so a window is not designed one or two
     * ticks wider than it can ever read. A shift that reaches the far edge (N ticks or more) leaves nothing inside the
     * window for any row - the far edge is not shifted with it ({@code SequenceEvaluator.farEdge}) - and the clause says
     * so instead. Empty for a wall-time window.
     */
    /** The shift of a calendar window in the clock's mean tick spacing, or NaN for a wall-time window / no shift. */
    private static double shiftTicks(final OutputColumn c) {
        final String clockName = c.coordinates.get("windowClock");
        final Clock clock = clockName == null ? null : c.clocks.get(clockName);
        if (clock == null || c.windowShift == null || !c.coordinates.containsKey("maxAgeTicks")) return Double.NaN;
        return (double) c.windowShift.toMillis() / clock.meanSpacingMillis();
    }

    /** Whether the shift covers every tick of a calendar window (on average): such a window reads nothing. */
    private static boolean hidesWholeWindow(final OutputColumn c) {
        final double ticks = shiftTicks(c);
        return !Double.isNaN(ticks) && ticks >= Long.parseLong(c.coordinates.get("maxAgeTicks"));
    }

    private static String shiftInTicks(final OutputColumn c) {
        final String clockName = c.coordinates.get("windowClock");
        final Clock clock = clockName == null ? null : c.clocks.get(clockName);
        final String maxAgeTicks = c.coordinates.get("maxAgeTicks");
        if (clock == null || c.windowShift == null || maxAgeTicks == null) return "";
        final long spacing = clock.meanSpacingMillis();
        final double ticks = shiftTicks(c);
        final String effect = ticks >= Long.parseLong(maxAgeTicks) ? "the shift covers the whole window, which holds no row at all"
                : ticks >= 1 ? "the newest ticks are never visible"
                : "part of the newest tick is never visible";
        // Locale.ROOT: the report is text a consumer greps, not a localized number
        return String.format(Locale.ROOT, "; on the clock '%s' that is ~%.1f of the window's %s tick(s) (mean spacing %s): %s",
                clockName, ticks, maxAgeTicks, Duration.ofMillis(spacing), effect);
    }

    /** The longest future-window horizon a column reads, directly or through the row / anonymous columns it derives from; null when none. */
    private Duration labelHorizon(final String canonical, final Set<String> visited) {
        if (canonical == null || !visited.add(canonical)) return null;
        final OutputColumn c = columnsByCanonical.get(canonical);
        if (c == null) return null;
        if ("future".equals(c.coordinates.get("direction")) && c.coordinates.containsKey("maxAge")) return Duration.parse(c.coordinates.get("maxAge"));
        Duration horizon = null;
        for (final String input : c.inputs) {
            final Duration h = labelHorizon(input, visited);
            if (h != null && (horizon == null || h.compareTo(horizon) > 0)) horizon = h;
        }
        return horizon;
    }

    private static JsonObject parseJsonObject(final String json) {
        if (json == null) return null;
        final JsonElement e = JsonParser.parseString(json);
        return e.isJsonObject() ? e.getAsJsonObject() : null;
    }

    /** Renders a naming template; empty segments collapse so {@code a____b} becomes {@code a__b}. */
    static String render(final String template, final Map<String, String> values) {
        String s = template;
        for (final Map.Entry<String, String> e : values.entrySet()) {
            s = s.replace("{" + e.getKey() + "}", e.getValue() == null ? "" : e.getValue());
        }
        final List<String> parts = new ArrayList<>();
        for (final String part : s.split("__")) if (!part.isEmpty()) parts.add(part);
        return String.join("__", parts);
    }

    // ------------------------------------------------------------------------------------------
    // final checks, naming, schema, stages
    // ------------------------------------------------------------------------------------------

    private void finalizeColumns() {
        final Set<String> consumed = new HashSet<>();
        for (final OutputColumn c : columns) consumed.addAll(c.inputs);

        resolveRoleColumns();
        final Set<String> keptByRole = applyInclude();
        final List<OutputColumn> indicators = new ArrayList<>();
        final List<String> excludedRoles = new ArrayList<>();
        final Set<String> matchedExcludes = new HashSet<>();
        for (final OutputColumn c : columns) {
            if (!c.intermediate && spec.output.include == null) {
                final List<String> matched = matchingExcludes(c);
                matchedExcludes.addAll(matched);
                if (!matched.isEmpty()) {
                    // a role column is the data contract, not a feature: no projection removes it (see applyInclude)
                    if (c.role == null) c.intermediate = true;
                    else { keptByRole.add(c.canonicalName); excludedRoles.add(outputNameOf(c) + " (" + c.role + ")"); }
                }
            }
            final String loc = "features." + c.block;
            boolean lint = false;
            if (c.scope == FeatureSpec.Scope.sequence || (c.scope == FeatureSpec.Scope.population && !FitMode.isLookupToken(c.coordinates.get("fit")))) {
                // S5: the keyed stage cannot trim the history of a key while such a column exists (worker memory)
                final String reason = SequenceEvaluator.unboundedReason(c);
                if (reason != null) {
                    diagnostics.hint("sequence.window.unbounded", loc,
                            c.canonicalName + " keeps every past row of its key on the worker (" + reason + "): the retained row count is unbounded, with only its own fields " + c.pastInputs + " kept that far back; give the window a maxAge to bound it");
                }
            }
            // a column declared as the label, the training weight or the evaluation baseline (output.roles.label / weight /
            // baseline) is post-event by declaration, like a future window's: never a feature, read by the evaluation after
            // the fact (a dynamic availability too: labels are not filtered per row, so the engine must not reject it)
            if ((c.status == Status.violation || c.status == Status.runtimeFilter) && ("label".equals(c.role) || "weight".equals(c.role) || "baseline".equals(c.role))) c.status = Status.label;
            if (c.status == Status.violation) {
                if (consumed.contains(c.canonicalName) || c.intermediate) {
                    lint = true;
                    c.intermediate = true;
                    diagnostics.info("availability.intermediate", loc,
                            c.canonicalName + " is available at " + c.availableAt.describe() + " > computeAt " + c.computeAt.describe() + "; kept as intermediate '_' column only");
                } else if (unresolvedBlocks) {
                    // a failed block may have been this column's consumer: defer the verdict to the next compile
                    lint = true;
                    c.intermediate = true;
                    diagnostics.info("availability.deferred", loc,
                            c.canonicalName + " is available after computeAt; the check is deferred because other blocks failed to expand (they may consume it as an intermediate)");
                } else {
                    diagnostics.error("availability.violation", loc,
                            "output column " + c.canonicalName + " is available at " + c.availableAt.describe() + ", after computeAt " + c.computeAt.describe());
                }
            } else if (c.status == Status.runtimeFilter && !c.intermediate) {
                diagnostics.info("availability.runtimeFilter", loc,
                        c.canonicalName + ": availability is not decidable statically (" + c.availableAt.getDynamicReasons() + "); the engine must filter contributions by effectiveAvailableAt <= computeAt");
            } else if (c.status == Status.windowShift && !c.intermediate) {
                diagnostics.info("availability.windowShift", loc,
                        c.canonicalName + ": window near edge shifted by " + c.windowShift + " (past availability + ingestionLag)" + shiftInTicks(c));
                if (hidesWholeWindow(c)) {
                    diagnostics.warning("window.clock.hidden", loc, c.canonicalName + ": the shift of " + c.windowShift + " covers the window's " + c.coordinates.get("maxAgeTicks")
                            + " tick(s) of the clock '" + c.coordinates.get("windowClock") + "' on average, so the window holds no row: widen maxAge beyond the shift in ticks, or read a wall-time window");
                }
            }
            if (!c.intermediate && c.declaredEvidence) {
                diagnostics.warning("evidence.declared", loc, c.canonicalName + " derives from a field whose pre-event availability is declared but not auditable");
            }
            if (c.validFor != null && c.availableAt.isStatic() && !c.availableAt.isPreEvent()
                    && c.availableAt.getOffset().plus(c.validFor).compareTo(spec.predictAt.getOffset()) < 0) {
                diagnostics.warning("validFor.alwaysExpired", loc, c.canonicalName + " expires before predictAt for every row (validFor " + c.validFor + ")");
            }
            c.outputName = (lint ? "_" : "") + outputNameOf(c);
            if (spec.output.groupBy == null) c.placement = Placement.child;
            // a column kept only as a role gets no indicator: the flag would be a feature column the projection
            // never admitted (the role's null-ness is the value itself)
            // nor does a label: its null-ness is post-event too, and the flag would be a feature
            final boolean indicator = !c.intermediate && spec.output.nullPolicy == NullPolicy.indicator && !keptByRole.contains(c.canonicalName)
                    && c.status != Status.label;
            if (indicator && (c.validFor != null || c.scope == Scope.sequence || c.scope == Scope.population || NULLABLE_CONTEXT_OPS.contains(c.operator))) {
                final OutputColumn flag = newColumn(c.block, c.scope, "isnull", c.canonicalName + "_isnull", Schema.FieldType.BOOLEAN, c.computeAt);
                flag.outputName = c.outputName + "_isnull";
                flag.inputs.add(c.canonicalName);
                flag.availableAt = c.availableAt;
                flag.status = c.status;
                flag.placement = c.placement;
                flag.coordinates.put("indicatorOf", c.canonicalName);
                indicators.add(flag);
            }
            if (indicator && "softmax".equals(c.operator) && "zero".equals(c.coordinates.get("scoreNull"))) {
                // the score fell back to 0 (the row took its offset's probability): the consumer must be able to tell
                final OutputColumn flag = newColumn(c.block, c.scope, "isnull", c.canonicalName + "_scoreNull", Schema.FieldType.BOOLEAN, c.computeAt);
                flag.outputName = c.outputName + "_scoreNull";
                flag.inputs.add(c.coordinates.get("field"));
                flag.availableAt = c.availableAt;
                flag.status = c.status;
                flag.placement = c.placement;
                flag.coordinates.put("indicatorOf", c.coordinates.get("field"));
                indicators.add(flag);
            }
        }
        columns.addAll(indicators);
        for (final OutputColumn i : indicators) columnsByCanonical.put(i.canonicalName, i);
        if (!excludedRoles.isEmpty()) {
            diagnostics.info("output.exclude.role", "output.exclude", "role columns are emitted although output.exclude matches them (roles are the data contract, not features): " + excludedRoles);
        }
        if (spec.output.include == null) {
            // a pattern that selects nothing is almost always a misspelling or a glob / regex the syntax does not have
            for (final String pattern : spec.output.exclude) {
                if (!matchedExcludes.contains(pattern)) {
                    diagnostics.warning("output.exclude.unmatched", "output.exclude", "exclude pattern '" + pattern
                            + "' matches no emitted column; a pattern is " + EXCLUDE_PATTERN_FORMS);
                }
            }
        }

        final Set<String> names = new HashSet<>();
        for (final OutputColumn c : columns) {
            if (!c.intermediate && !names.add(c.outputName)) {
                diagnostics.error("column.duplicate", "features." + c.block, "duplicate output name: " + c.outputName);
            }
        }
        if (spec.orderTieBreak.isEmpty() && columns.stream().anyMatch(c -> c.scope == Scope.sequence || c.scope == Scope.population)) {
            diagnostics.hint("time.orderTieBreak", "time", "sequence/encoding features present without time.orderTieBreak; rows sharing a timestamp exclude each other (strict past) but declare a tie-break for cross-engine determinism");
        }
    }

    /**
     * output.include: the output projection (a screening step pass list, a hand-written list). When declared it
     * replaces {@code exclude}: a column is emitted iff its canonical or output name is listed (an
     * {@code <name>_isnull} entry keeps its base column); names that match nothing are a warning (the list may
     * come from another plan version). Columns already intermediate (violations, hidden levels, baselines)
     * stay so. A column an {@code output.roles} entry names ({@link OutputColumn#role}: a baseline's emitted
     * copy, a label derived as a column) is part of the data contract, not of the feature set: it stays emitted
     * whether or not the list names it — a pass list never contains role columns (they were never candidates),
     * and dropping them would leave the consumer's manifest with a role that resolves to nothing.
     *
     * @return the canonical names of the columns kept only by their role (not listed); the caller adds the
     *         columns {@code output.exclude} would have dropped
     */
    private Set<String> applyInclude() {
        final Set<String> keptByRole = new HashSet<>();
        if (spec.output.include == null) return keptByRole;
        if (!spec.output.exclude.isEmpty()) {
            diagnostics.info("output.include.exclude", "output", "output.include is declared: output.exclude is ignored (include is the projection)");
        }
        final Set<String> listed = new LinkedHashSet<>(spec.output.include);
        if (listed.isEmpty()) {
            // a screening step that passed nothing, or a broken list: the table would carry no feature column
            diagnostics.error("output.include.empty", "output.include", "output.include is empty: no feature column would be emitted"
                    + (spec.output.includeSource != null ? " (from " + spec.output.includeSource + ")" : "")
                    + "; remove output.include to emit every column, or list the columns to keep");
            return keptByRole;
        }
        final Set<String> matched = new LinkedHashSet<>();
        final List<String> keptRoles = new ArrayList<>();
        for (final OutputColumn c : columns) {
            if (c.intermediate || c.fieldType == null) continue;
            final String outputName = outputNameOf(c);
            boolean included = false;
            for (final String candidate : List.of(c.canonicalName, outputName, c.canonicalName + "_isnull", outputName + "_isnull")) {
                if (listed.contains(candidate)) {
                    matched.add(candidate);
                    included = true;
                }
            }
            if (included) continue;
            if (c.role == null) c.intermediate = true;
            else { keptByRole.add(c.canonicalName); keptRoles.add(outputName + " (" + c.role + ")"); }
        }
        if (!keptRoles.isEmpty()) {
            diagnostics.info("output.include.role", "output.include", "role columns are emitted although output.include does not list them (roles are the data contract, not features): " + keptRoles);
        }
        final List<String> unknown = new ArrayList<>();
        for (final String name : listed) if (!matched.contains(name)) unknown.add(name);
        if (!unknown.isEmpty()) {
            diagnostics.warning("output.include.unknown", "output.include", "include names no column of this plan: " + unknown
                    + (spec.output.includeSource != null ? " (from " + spec.output.includeSource + ")" : ""));
        }
        return keptByRole;
    }

    /** The name a column is emitted under, before the {@code _} lint prefix of a violation is decided. */
    private String outputNameOf(final OutputColumn c) {
        return (c.anonymous ? "" : spec.output.prefix) + c.canonicalName;
    }

    /**
     * Stamps {@link OutputColumn#role} on the columns that {@code output.roles} name — the one resolution the
     * projection ({@link #applyInclude}, {@code output.exclude}), the schema options, the manifest and
     * {@link FeaturePlan#getRoleColumns} all read. An input-field role passes through outside the column set; a
     * baseline role resolves to its {@code baselines[].emit} copy; a group / entity role naming a context / entity
     * resolves to keys, never to a column that merely shares the name; anything else matches a column by its
     * canonical or output name. The first role naming a column wins.
     */
    private void resolveRoleColumns() {
        for (final Map.Entry<String, String> e : spec.output.roles.entrySet()) {
            final String role = e.getKey();
            final String name = e.getValue();
            if (inputFields.containsKey(name)) continue;
            if (("group".equals(role) && contexts.containsKey(name)) || ("entity".equals(role) && entities.containsKey(name))) continue;
            OutputColumn column = "baseline".equals(role) && baselineEmits.containsKey(name) ? columnsByCanonical.get(baselineEmits.get(name)) : null;
            if (column == null) {
                for (final OutputColumn c : columns) {
                    if (c.canonicalName.equals(name) || outputNameOf(c).equals(name)) { column = c; break; }
                }
            }
            if (column != null && column.role == null) column.role = role;
        }
    }

    /** output.exclude: name globs ({@code block.*}, {@code name}) and lineage selectors ({@code derivedFrom:market}). */
    /** What an {@code output.exclude} pattern can be — the message of {@code output.exclude.unmatched}. */
    static final String EXCLUDE_PATTERN_FORMS = "<block>.* (the whole block) | a column's canonical name | a block name"
            + " | derivedFrom:<kind> | evidence:declared | scope:<scope> | block:<name>; patterns are neither globs nor regular expressions";

    /**
     * Every {@code output.exclude} pattern that selects the column (all of them, so an unmatched pattern can be
     * reported): {@code block.*}, an exact canonical name, a block name, or a lineage selector.
     */
    private List<String> matchingExcludes(final OutputColumn c) {
        final List<String> matched = new ArrayList<>();
        for (final String pattern : spec.output.exclude) {
            final int colon = pattern.indexOf(':');
            final boolean match;
            if (colon > 0) {
                final String selector = pattern.substring(0, colon);
                final String value = pattern.substring(colon + 1);
                match = switch (selector) {
                    case "derivedFrom" -> c.derivedFrom.contains(value);
                    case "evidence" -> "declared".equals(value) && c.declaredEvidence;
                    case "scope" -> c.scope.name().equals(value);
                    case "block" -> c.block.equals(value);
                    default -> false;
                };
            } else if (pattern.endsWith(".*")) {
                match = c.block.equals(pattern.substring(0, pattern.length() - 2));
            } else {
                match = pattern.equals(c.canonicalName) || pattern.equals(c.block);
            }
            if (match) matched.add(pattern);
        }
        return matched;
    }

    private Schema buildSchema() {
        final Schema.Builder builder = Schema.builder();
        for (final OutputColumn c : columns) {
            if (c.intermediate || c.fieldType == null) continue;
            builder.withField(c.toField());
        }
        return builder.build();
    }

    private List<FeaturePlan.Stage> buildStages() {
        final StageScheduler scheduler = new StageScheduler();
        for (final OutputColumn c : columns) scheduler.add(c);
        final List<FeaturePlan.Stage> stages = scheduler.build();
        if (spec.output.groupBy != null && contexts.containsKey(spec.output.groupBy)) {
            final List<Integer> all = new ArrayList<>();
            for (final FeaturePlan.Stage s : stages) all.add(s.index());
            stages.add(new FeaturePlan.Stage(stages.size(), FeaturePlan.StageKind.groupBy, contexts.get(spec.output.groupBy).keys(), List.of("output"), List.of(), all));
        }
        return stages;
    }

    /** The comma-joined key list of a {@code keys} / {@code stageKeys} coordinate (empty = global level). */
    static List<String> keyList(final String joined) {
        return joined == null || joined.isEmpty() ? List.of() : List.of(joined.split(","));
    }

    static boolean isRowColumn(final OutputColumn c) {
        return c.scope == Scope.row || "isnull".equals(c.operator);
    }

    /**
     * Stage scheduling (engine doc §3.1 / §9.2 S2): columns are placed by key affinity, not by their position in
     * the config. A keyed column goes to the earliest stage that evaluates its kind under the same key and comes
     * after the stages its dependencies are evaluated in, so two blocks keyed by the same entity share one
     * GroupByKey even when a block with another key sits between them, and sequence and population columns of
     * one key share the same keyed replay. Row columns are placed as late as possible: in the stage of their
     * first consumer (the stage before it when the consumer reads them before its DoFn), or in the last stage
     * when only the output reads them — so a row value is not carried through shuffles that do not need it.
     * <p>Rules: a dependency read inside the evaluating DoFn (the row, the history) may live in the same stage;
     * one read before the DoFn — stage keys, fit / variance-components statistics computed over the stage input —
     * must come from an earlier stage. The hidden levels of a static-fit block, and the row columns that read
     * them, stay in the block's single fit stage (its artifact and lambdas are written / read there). A column
     * that reads the whole history of its key (no maxAge on a scan-path window) does not extend the retention
     * of the other columns' fields: the history is trimmed per field ({@link SequenceEvaluator.History#trim}).
     * Inside a stage the columns keep the expansion order, which lists dependencies first.
     */
    private final class StageScheduler {

        private final class Slot {
            final FeaturePlan.StageKind kind;
            final List<String> keys;
            final Set<String> names = new LinkedHashSet<>();
            boolean population;

            Slot(final FeaturePlan.StageKind kind, final List<String> keys) {
                this.kind = kind;
                this.keys = keys;
            }

            boolean accepts(final FeaturePlan.StageKind k, final List<String> stageKeys) {
                return kind == k && keys.equals(stageKeys);
            }

            FeaturePlan.Stage build(final int index, final List<Integer> dependsOn) {
                // inside a stage the columns keep the expansion order (dependencies first)
                final List<String> ordered = new ArrayList<>(names);
                ordered.sort(Comparator.comparingInt(order::get));
                final List<String> blocks = new ArrayList<>();
                for (final String name : ordered) {
                    final String block = columnsByCanonical.get(name).block;
                    if (!blocks.contains(block)) blocks.add(block);
                }
                // a keyed stage replays sequence and population columns together: its kind names the heavier one
                final FeaturePlan.StageKind k = kind == FeaturePlan.StageKind.sequence && population ? FeaturePlan.StageKind.population : kind;
                return new FeaturePlan.Stage(index, k, keys, blocks, ordered, dependsOn);
            }
        }

        private final List<Slot> slots = new ArrayList<>();
        /** expansion index of every column */
        private final Map<String, Integer> order = new HashMap<>();
        private final Map<String, Integer> stageOf = new HashMap<>();
        /** row columns → the earliest stage their own dependencies allow (they may move earlier down to it) */
        private final Map<String, Integer> rowEarliest = new HashMap<>();
        private final Map<String, Integer> fitStageOf = new HashMap<>();
        /** every dependency of a placed column (row / history / strict reads), mapped to stages in build */
        private final Map<String, Set<String>> depsOf = new HashMap<>();

        StageScheduler() {
            for (final OutputColumn c : columns) order.put(c.canonicalName, order.size());
        }

        void add(final OutputColumn c) {
            if (isRowColumn(c)) {
                // placed when a consumer needs it (or in the last stage): see placeRow
                rowEarliest.put(c.canonicalName, earliest(c, strictInputs(c)));
                return;
            }
            final FeaturePlan.StageKind k;
            final List<String> stageKeys;
            if (c.scope == Scope.context) {
                k = FeaturePlan.StageKind.context;
                final ContextDef context = contexts.get(c.coordinates.get("context"));
                stageKeys = context == null ? List.of() : context.keys();
            } else if (c.scope == Scope.sequence) {
                // a future window replays the key in descending time: its own stage, never fused with the past ones
                k = "future".equals(c.coordinates.get("direction")) ? FeaturePlan.StageKind.future : FeaturePlan.StageKind.sequence;
                if (c.coordinates.containsKey("stageKeys")) {
                    stageKeys = keyList(c.coordinates.get("stageKeys"));
                } else {
                    final EntityDef entity = entities.get(c.coordinates.get("entity"));
                    stageKeys = entity == null ? List.of() : entity.keys();
                }
            } else if (FitMode.isLookupToken(c.coordinates.get("fit"))) {
                k = FeaturePlan.StageKind.fit;
                stageKeys = List.of();
            } else {
                k = FeaturePlan.StageKind.sequence;
                stageKeys = keyList(c.coordinates.get("keys"));
            }
            final Set<String> strict = strictInputs(c);
            strict.addAll(stageKeys);
            final int target = k == FeaturePlan.StageKind.fit ? fitStage(c) : slotFor(k, stageKeys, earliest(c, strict));
            place(c, target, strict);
            if (c.scope == Scope.population && k == FeaturePlan.StageKind.sequence) slots.get(target).population = true;
        }

        /** Inputs read before the stage's DoFn (from the stage input), which must come from an earlier stage. */
        private Set<String> strictInputs(final OutputColumn c) {
            final Set<String> strict = new LinkedHashSet<>();
            if (FitMode.isLookupToken(c.coordinates.get("fit"))) strict.addAll(c.inputs);
            if ("varianceComponents".equals(c.coordinates.get("weights")) && c.coordinates.containsKey("levels")) {
                // the pseudo-counts are estimated over the stage input from the levels' keys / target / offset
                for (final Shrinkage.Level level : Shrinkage.leaves(Shrinkage.parseLevels(c.coordinates.get("levels")))) {
                    final OutputColumn hidden = columnsByCanonical.get(level.nColumn());
                    if (hidden != null) strict.addAll(hidden.inputs);
                }
            }
            return strict;
        }

        /** Lower bound of the stage a dependency is evaluated in (-1 for an input field); a row column can move down to it. */
        private int boundOf(final String dep) {
            final Integer row = rowEarliest.get(dep);
            if (row != null) return row;
            final Integer placed = stageOf.get(dep);
            return placed != null ? placed : -1;
        }

        private int earliest(final OutputColumn c, final Set<String> strict) {
            int earliest = 0;
            for (final String dep : c.inputs) earliest = Math.max(earliest, boundOf(dep));
            for (final String dep : c.pastInputs) earliest = Math.max(earliest, boundOf(dep));
            for (final String dep : strict) earliest = Math.max(earliest, boundOf(dep) + 1);
            return earliest;
        }

        private int slotFor(final FeaturePlan.StageKind k, final List<String> keys, final int earliest) {
            for (int i = earliest; i < slots.size(); i++) {
                if (slots.get(i).accepts(k, keys)) return i;
            }
            slots.add(new Slot(k, keys));
            return slots.size() - 1;
        }

        /**
         * The single fit stage of a static-fit block: chosen when its first fitted column arrives, after every
         * fitted column of the block (they expand together, and their inputs are all placed by then).
         */
        private int fitStage(final OutputColumn c) {
            final Integer existing = fitStageOf.get(c.block);
            if (existing != null) return existing;
            int earliest = 0;
            for (final OutputColumn x : columns) {
                if (x.block.equals(c.block) && FitMode.isLookupToken(x.coordinates.get("fit"))) earliest = Math.max(earliest, earliest(x, strictInputs(x)));
            }
            final int target = slotFor(FeaturePlan.StageKind.fit, List.of(), earliest);
            fitStageOf.put(c.block, target);
            return target;
        }

        /** Puts a column in a stage after pulling the row columns it reads into that stage (or the one before). */
        private void place(final OutputColumn c, final int target, final Set<String> strict) {
            final Set<String> deps = new LinkedHashSet<>(c.inputs);
            deps.addAll(c.pastInputs);
            deps.addAll(strict);
            depsOf.put(c.canonicalName, deps);
            for (final String dep : c.inputs) if (!strict.contains(dep)) placeRow(dep, target);
            for (final String dep : c.pastInputs) if (!strict.contains(dep)) placeRow(dep, target);
            for (final String dep : strict) placeRow(dep, target - 1);
            final Integer previous = stageOf.put(c.canonicalName, target);
            if (previous != null) slots.get(previous).names.remove(c.canonicalName);
            slots.get(target).names.add(c.canonicalName);
        }

        /**
         * A row column is evaluated in the stage of its earliest consumer: placed there when first needed and
         * moved earlier (with the row columns it reads) when a consumer in an earlier stage appears.
         */
        private void placeRow(final String name, final int at) {
            final Integer earliest = rowEarliest.get(name);
            if (earliest == null) return; // not a row column
            final Integer current = stageOf.get(name);
            if (current != null && current <= at) return;
            final OutputColumn r = columnsByCanonical.get(name);
            // a row column over fitted statistics is evaluated as early as its own inputs allow (in the fit stage
            // of a single fitted input: the artifact / lambdas live there), never earlier — with several fitted
            // inputs, or a fitted one and a keyed one, the earlier stages precede part of what the column reads
            Integer fitStage = null;
            for (final String dep : r.inputs) {
                final OutputColumn d = columnsByCanonical.get(dep);
                if (d != null && FitMode.isLookupToken(d.coordinates.get("fit")) && fitStageOf.containsKey(d.block)) {
                    fitStage = fitStageOf.get(d.block);
                    break;
                }
            }
            final int target = fitStage != null ? Math.min(at, earliest) : at;
            if (target < earliest) {
                throw new IllegalStateException("feature stage scheduling: " + name + " needs stage " + earliest + " but is required at stage " + target);
            }
            // a reader of estimated pseudo-counts must stay in its levels' fit stage: the λ of a lookup fit lives there
            // (merged with the artifact's), and any other stage would estimate its own over its input — another number,
            // silently. A reader's inputs are its block's levels, so no spec reaches this; it keeps that true.
            if (fitStage != null && target != fitStage && "varianceComponents".equals(r.coordinates.get("weights")) && r.coordinates.containsKey("levels")) {
                throw new IllegalStateException("feature stage scheduling: " + name + " reads the estimated pseudo-counts of fit stage " + fitStage
                        + " but is placed at stage " + target);
            }
            place(r, target, strictInputs(r));
        }

        List<FeaturePlan.Stage> build() {
            // row columns nobody else reads: the last stage (no shuffle either way) for the linear chain — and
            // DEFERRED for the wave engine, which evaluates them on the first wave input that carries their inputs
            // (FeaturePlan.getPreludeColumns) instead of inside the hosting stage: they are no edge of the DAG
            // (hosting them would chain the last stage after every stage they read) and no branch evaluates them.
            // A reader of a lookup fit is not deferred: its lambdas / artifact live in the fit stage it is placed in.
            if (slots.isEmpty() && !rowEarliest.isEmpty()) slots.add(new Slot(FeaturePlan.StageKind.row, List.of()));
            final Set<String> placed = new HashSet<>(stageOf.keySet());
            for (final OutputColumn c : columns) {
                if (rowEarliest.containsKey(c.canonicalName) && !stageOf.containsKey(c.canonicalName)) placeRow(c.canonicalName, slots.size() - 1);
            }
            final Set<String> deferred = new HashSet<>();
            for (final String name : stageOf.keySet()) {
                final OutputColumn c = columnsByCanonical.get(name);
                if (placed.contains(name) || readsLookupFit(c)) continue;
                c.deferred = true;
                deferred.add(name);
            }
            final List<FeaturePlan.Stage> stages = new ArrayList<>();
            for (final Slot slot : slots) stages.add(slot.build(stages.size(), dependsOn(slot, stages.size(), deferred)));
            return stages;
        }

        private boolean readsLookupFit(final OutputColumn r) {
            for (final String dep : r.inputs) {
                final OutputColumn d = columnsByCanonical.get(dep);
                if (d != null && FitMode.isLookupToken(d.coordinates.get("fit"))) return true;
            }
            return false;
        }

        /**
         * Stages whose keyed / fit columns the columns of this slot need (engine doc §9.4: the edges of the stage
         * DAG). A row column is not a node: it is followed through to its own dependencies, because the linear chain
         * places it in its first consumer's stage and carries the value forward, while a branch evaluating the same
         * columns from the stage input would simply recompute it. Input fields have no stage; a dependency inside
         * the same stage is not an edge. A deferred row column (output-only, evaluated on the wave inputs) is no
         * edge either: nothing in the stage reads it.
         */
        private List<Integer> dependsOn(final Slot slot, final int index, final Set<String> deferred) {
            final TreeSet<Integer> deps = new TreeSet<>();
            final Set<String> visited = new HashSet<>();
            for (final String name : slot.names) if (!deferred.contains(name)) collectDeps(name, index, deps, visited);
            return List.copyOf(deps);
        }

        private void collectDeps(final String name, final int index, final TreeSet<Integer> deps, final Set<String> visited) {
            if (!visited.add(name)) return;
            for (final String dep : depsOf.getOrDefault(name, Set.of())) {
                final Integer at = stageOf.get(dep);
                if (at == null) continue; // input field
                final OutputColumn d = columnsByCanonical.get(dep);
                if (d != null && isRowColumn(d)) {
                    collectDeps(dep, index, deps, visited);
                    continue;
                }
                if (at == index) continue;
                if (at > index) {
                    throw new IllegalStateException("feature stage scheduling: " + name + " (stage " + index + ") reads " + dep + " from a later stage " + at);
                }
                deps.add(at);
            }
        }
    }

    // ------------------------------------------------------------------------------------------
    // canonical hash
    // ------------------------------------------------------------------------------------------

    static String hash(final JsonElement sourcesDocument, final JsonObject parameters) {
        // fit.artifact (uri / refit / id) is excluded: re-fitting or relocating artifacts must not change
        // the identity of what was fitted
        return sha256(canonical(sourcesDocument) + "\u0000" + canonical(withoutArtifact(parameters))
                // an offset term on logit / log is the score-type estimate since PR #176 (its levels keep sum b(1 - b)):
                // an artifact of the transformed-mean estimator, addressed by the same spec, must not be found
                + (declaresOffsetTerm(parameters) ? "\u0000" + OFFSET_ESTIMATOR : ""));
    }

    /** The estimator of an offset term on a transformed scale, part of the plan hash of a spec declaring one. */
    static final String OFFSET_ESTIMATOR = "offset-estimator:score";

    /** Whether any block declares an offset with a logit / log shrinkage scale (on the block or one of its keySets). */
    static boolean declaresOffsetTerm(final JsonObject parameters) {
        if (parameters == null || !parameters.has("features") || !parameters.get("features").isJsonArray()) return false;
        for (final JsonElement f : parameters.getAsJsonArray("features")) {
            if (!f.isJsonObject() || !f.getAsJsonObject().has("offset")) continue;
            final JsonObject block = f.getAsJsonObject();
            if (transformedScale(block.get("shrinkage"))) return true;
            if (block.has("keySets") && block.get("keySets").isJsonArray()) {
                for (final JsonElement ks : block.getAsJsonArray("keySets")) {
                    if (ks.isJsonObject() && transformedScale(ks.getAsJsonObject().get("shrinkage"))) return true;
                }
            }
        }
        return false;
    }

    private static boolean transformedScale(final JsonElement shrinkage) {
        if (shrinkage == null || !shrinkage.isJsonObject()) return false;
        final String scale = SourceContract.Json.string(shrinkage.getAsJsonObject(), "scale");
        return "logit".equals(scale) || "log".equals(scale);
    }

    /**
     * The output-table identity: the plan hash plus the projection (emitted output names, roles, the include
     * list content). {@code output.include} is outside the plan hash (a projection does not change what is
     * fitted, so artifacts stay valid), which is why the output needs a hash of its own.
     */
    private String outputHash(final String planHash) {
        final StringBuilder sb = new StringBuilder(planHash);
        sb.append("\u0000");
        for (final OutputColumn c : columns) if (!c.intermediate && c.fieldType != null) sb.append(c.outputName).append(',');
        sb.append("\u0000");
        final JsonObject roles = new JsonObject();
        spec.output.roles.forEach(roles::addProperty);
        sb.append(canonical(roles));
        sb.append("\u0000").append(spec.output.includeHash == null ? "" : spec.output.includeHash);
        // values read from external documents at assembly (temperatureFrom): outside the plan hash, part of the output identity
        for (final String external : spec.resolvedExternals) sb.append("\u0000").append(external);
        return sha256(sb.toString());
    }

    /** SHA-256 of a string, first 16 hex characters (the width of the plan hash; the screen transform's screenHash shares it). */
    public static String sha256(final String text) {
        try {
            final MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(text.getBytes(StandardCharsets.UTF_8));
            final StringBuilder sb = new StringBuilder();
            for (final byte b : digest.digest()) sb.append(String.format("%02x", b));
            return sb.substring(0, 16);
        } catch (final NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * The parameters without what does not change the plan: artifact locations, the engine knobs, and the
     * output projection ({@code output.include} + its source / hash, {@code output.manifest}) — a projection
     * does not change what is fitted, so it lives in the output hash instead.
     */
    static JsonObject withoutArtifact(final JsonObject parameters) {
        final JsonObject copy = parameters.deepCopy();
        copy.remove("engine");
        if (copy.has("output") && copy.get("output").isJsonObject()) {
            final JsonObject output = copy.getAsJsonObject("output");
            for (final String key : List.of("include", "includeSource", "includeHash", "manifest")) output.remove(key);
        }
        if (copy.has("fit") && copy.get("fit").isJsonObject()) copy.getAsJsonObject("fit").remove("artifact");
        if (copy.has("features") && copy.get("features").isJsonArray()) {
            for (final JsonElement f : copy.getAsJsonArray("features")) {
                if (f.isJsonObject() && f.getAsJsonObject().has("fit") && f.getAsJsonObject().get("fit").isJsonObject()) {
                    f.getAsJsonObject().getAsJsonObject("fit").remove("artifact");
                }
                // temperatureFrom: a calibration document read at assembly (no fit depends on it; the resolved value is in the output hash)
                if (f.isJsonObject() && f.getAsJsonObject().has("ops") && f.getAsJsonObject().get("ops").isJsonArray()) {
                    for (final JsonElement op : f.getAsJsonObject().getAsJsonArray("ops")) {
                        if (op.isJsonObject()) op.getAsJsonObject().remove("temperatureFrom");
                    }
                }
            }
        }
        return copy;
    }

    /**
     * JSON with object keys sorted recursively, so formatting / key order do not change the hash. Its text is a
     * hash input (plan / output / include hashes, the screen transform's screenHash): keep it stable.
     */
    public static String canonical(final JsonElement element) {
        if (element == null || element.isJsonNull()) return "null";
        if (element.isJsonPrimitive()) return element.toString();
        if (element.isJsonArray()) {
            final StringBuilder sb = new StringBuilder("[");
            boolean first = true;
            for (final JsonElement e : element.getAsJsonArray()) {
                if (!first) sb.append(',');
                sb.append(canonical(e));
                first = false;
            }
            return sb.append(']').toString();
        }
        final JsonObject object = element.getAsJsonObject();
        final List<String> keys = new ArrayList<>(object.keySet());
        Collections.sort(keys);
        final StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (final String key : keys) {
            if (!first) sb.append(',');
            sb.append('"').append(key).append("\":").append(canonical(object.get(key)));
            first = false;
        }
        return sb.append('}').toString();
    }

}
