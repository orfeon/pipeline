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

    private final Diagnostics diagnostics = new Diagnostics();
    private final Map<String, SourceContract> sources;
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
    private int anonymousCounter = 0;

    private FeaturePlanCompiler(final JsonElement sourcesDocument, final JsonObject parameters,
                                final List<Schema.Field> inputSchemaFields) {
        this.sources = SourceContract.parseAll(sourcesDocument, diagnostics);
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
        }
        final List<FeaturePlan.Stage> stages = buildStages();
        hintGlobalKeyStages(stages);
        final Schema outputSchema = buildSchema();
        final String hash = hash(sourcesDocument, parameters);
        final String outputHash = outputHash(hash);
        return new FeaturePlan(spec, sources, inputFields, columns, stages, outputSchema, diagnostics, hash, outputHash, observedAtAudits);
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
            diagnostics.hint("encoding.globalKey", "features." + String.join(",", s.blocks()),
                    "stage #" + s.index() + " evaluates every row under one key (a single worker thread"
                            + (spec.engine.parallelWaves ? " — the critical path once the waves run in parallel" : "")
                            + "); a global / very coarse encoding can move its statistics to fit.mode forward"
                            + " (complete earlier time blocks, leak-free), static (streaming-capable with an artifact) or fold (batch only) — the values change:"
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
            for (final String name : inputFields.keySet()) {
                if (!schemaNames.contains(name)) {
                    diagnostics.error("lineage.missingInput", "lineage", "lineage field '" + name + "' is not present in the input schema");
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
            diagnostics.warning("fit.minHistory", "fit", "fit.minHistory is not implemented yet and ignored");
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

    /** A block awaiting expansion with its syntactic references. */
    private record Pending(String name, Set<String> references, Runnable expand) {}

    private void expandAll() {
        final List<Pending> pending = new ArrayList<>();
        for (final BaselineDef baseline : spec.baselines) {
            pending.add(new Pending("baselines." + baseline.name(), baselineReferences(baseline), () -> expandBaseline(baseline)));
        }
        for (final FeatureDef def : spec.features) {
            pending.add(new Pending(def.location(), blockReferences(def), () -> expandBlock(def)));
        }
        boolean progress = true;
        while (!pending.isEmpty() && progress) {
            progress = false;
            final Iterator<Pending> it = pending.iterator();
            while (it.hasNext()) {
                final Pending p = it.next();
                if (p.references.stream().allMatch(this::resolves)) {
                    p.expand.run();
                    it.remove();
                    progress = true;
                }
            }
        }
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
        for (final Op op : def.ops) {
            refs.addAll(op.fields);
            if (op.expr != null) refs.addAll(expressionReferences(op.expr).others);
            if (op.predicate != null) refs.addAll(expressionReferences(op.predicate).others);
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
                diagnostics.error("computeAt.invalid", def.location(), "computeAt must be event_time ± duration");
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
            e.status = Status.staticSafe;
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
                for (final String value : def.values) {
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
            default -> diagnostics.error("row.type", loc, "unsupported row type: " + type);
        }
    }

    /** The row identity of the fold rule: time.field + orderTieBreak (canonical names). */
    private List<String> rowIdentity() {
        final List<String> identity = new ArrayList<>();
        identity.add(spec.timeField);
        for (final String f : spec.orderTieBreak) if (!identity.contains(f)) identity.add(f);
        return identity;
    }

    private String singleInput(final FeatureDef def) {
        if (def.input != null) return def.input;
        if (def.inputs.size() == 1) return def.inputs.get(0);
        diagnostics.error("row.input", def.location(), "type: " + def.type + " requires exactly one 'input'");
        return null;
    }

    private void finishRow(final OutputColumn c, final FeatureDef def) {
        if (c.availableAt == null) c.availableAt = AvailableAt.atEventTime();
        c.validFor = def.validFor;
        c.status = c.availableAt.isStaticallyAtOrBefore(c.computeAt) ? Status.staticSafe
                : c.availableAt.isStatic() ? Status.violation : Status.runtimeFilter;
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
            for (final String field : fields) {
                final Ref ref = resolve(field);
                if (ref == null) continue;
                if (!accepts(operator, ref.type())) {
                    diagnostics.error("context.op.type", loc, "op " + op.type + " expects " + operator.input() + " input; '" + field + "' is " + (ref.type() == null ? "unknown" : ref.type().getType()));
                    continue;
                }
                if (!op.values.isEmpty() && List.of("countByValue", "ratioByValue").contains(op.type)) {
                    // one column per listed value (like indicator) instead of a map column
                    for (final String value : op.values) {
                        final Schema.FieldType type = "countByValue".equals(op.type) ? Schema.FieldType.INT64 : Schema.FieldType.FLOAT64;
                        final OutputColumn c = newColumn(def.name, Scope.context, op.type, def.name + "_" + (op.as != null ? op.as : field) + "_" + op.type + "_" + value, type, computeAt);
                        c.coordinates.put("field", canonicalOf(field));
                        c.coordinates.put("value", value);
                        addSelfInput(c, field);
                        finishContext(c, def, context, op);
                    }
                    continue;
                }
                final OutputColumn c = newColumn(def.name, Scope.context, op.type, def.name + "_" + (op.as != null ? op.as : field) + "_" + op.type, operator.outputFor(ref.type()), computeAt);
                c.coordinates.put("field", canonicalOf(field));
                addSelfInput(c, field);
                if (!configureContextOp(c, op, def, context, loc)) continue;
                finishContext(c, def, context, op);
            }
        }
    }

    /**
     * Parameters of the context ops that take more than one field: {@code softmax} (offset / temperature /
     * scales) and {@code shuffle} (seed / ordering). Returns false when the column must not be created.
     */
    private boolean configureContextOp(final OutputColumn c, final Op op, final FeatureDef def, final ContextDef context, final String loc) {
        switch (op.type) {
            case "softmax" -> {
                if (op.offset != null) {
                    final String offsetColumn = baselineColumns.containsKey(op.offset) ? baselineColumns.get(op.offset) : op.offset;
                    final Ref ref = resolve(offsetColumn);
                    if (ref == null) {
                        diagnostics.error("context.softmax.offset", loc, "softmax offset must reference baselines[].name or a numeric column: " + op.offset);
                        return false;
                    }
                    if (!OperatorCatalog.isNumeric(ref.type())) {
                        diagnostics.error("context.softmax.offset", loc, "softmax offset '" + op.offset + "' is not numeric");
                        return false;
                    }
                    c.coordinates.put("offset", ref.canonical());
                    addSelfInput(c, ref.canonical());
                    // the probability is as perishable as its offset (a market price with validFor)
                    final Duration validFor = ref.field != null ? ref.field.getValidFor() : ref.column.validFor;
                    if (def.validFor == null && validFor != null) c.validFor = validFor;
                }
                if (op.temperature != null && !(op.temperature > 0)) {
                    diagnostics.error("context.softmax.temperature", loc, "temperature must be > 0: " + op.temperature);
                    return false;
                }
                final String offsetScale = op.offsetScale == null ? "probability" : op.offsetScale;
                if (!List.of("probability", "log").contains(offsetScale)) {
                    diagnostics.error("context.softmax.offsetScale", loc, "offsetScale must be probability | log: " + offsetScale);
                    return false;
                }
                final String scoreNull = op.scoreNull == null ? "zero" : op.scoreNull;
                if (!List.of("zero", "null").contains(scoreNull)) {
                    diagnostics.error("context.softmax.scoreNull", loc, "scoreNull must be zero | null: " + scoreNull);
                    return false;
                }
                c.coordinates.put("temperature", Double.toString(op.temperature == null ? 1d : op.temperature));
                if (op.temperatureSource != null) c.coordinates.put("temperatureSource", op.temperatureSource);
                c.coordinates.put("offsetScale", offsetScale);
                c.coordinates.put("scoreNull", scoreNull);
                if (def.excludeSelf) {
                    diagnostics.warning("context.softmax.excludeSelf", loc, "excludeSelf has no effect on softmax (the row is part of its own normalisation)");
                }
            }
            case "shuffle" -> {
                if (op.seed == null) {
                    diagnostics.error("context.shuffle.seed", loc, "shuffle requires 'seed' (the permutation must be reproducible)");
                    return false;
                }
                if (spec.orderTieBreak.isEmpty()) {
                    diagnostics.warning("context.shuffle.identity", loc, "shuffle without time.orderTieBreak orders rows sharing a timestamp by their input values only; declare time.orderTieBreak for a row identity");
                }
                c.coordinates.put("seed", Long.toString(op.seed));
                c.coordinates.put("order", String.join(",", rowIdentity()));
                c.coordinates.put("contextKeys", String.join(",", context.keys()));
                // rows sharing the identity are told apart by their input values, so the permutation is a pure
                // function of the group whatever order the GroupByKey delivers (and identical in every engine mode)
                final List<String> tieBreak = new ArrayList<>(new TreeSet<>(inputFields.keySet()));
                tieBreak.removeAll(rowIdentity());
                c.coordinates.put("tieBreak", String.join(",", tieBreak));
                for (final String f : rowIdentity()) addSelfInput(c, f);
                // the permuted values carry the availability of the source column (addSelfInput above);
                // nothing else is read
            }
            default -> { }
        }
        return true;
    }

    private void finishContext(final OutputColumn c, final FeatureDef def, final ContextDef context, final Op op) {
        c.coordinates.put("context", context.name());
        c.coordinates.put("op", op.type);
        if (def.excludeSelf) c.coordinates.put("excludeSelf", "true");
        for (final String key : context.keys()) addSelfInput(c, key);
        if (c.availableAt == null) c.availableAt = AvailableAt.atEventTime();
        if (def.validFor != null || c.validFor == null) c.validFor = def.validFor; // an op may have inherited one (softmax offset)
        c.status = c.availableAt.isStaticallyAtOrBefore(c.computeAt) ? Status.staticSafe
                : c.availableAt.isStatic() ? Status.violation : Status.runtimeFilter;
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
        if (def.ops.isEmpty()) {
            diagnostics.error("sequence.ops", loc, "sequence feature requires 'ops' (general lift/summarize form is v1)");
            return;
        }
        final List<Window> windows = def.windows.isEmpty() ? List.of(new Window()) : def.windows;
        for (final Window window : windows) {
            // a same-field $self equality filter is a partition of the entity: reduce it to a stage key
            // so hot entities are split across workers (rows whose field is null bypass the stage → null)
            final String reducedKey = reducibleFilterField(window, computeAt);
            final References filterRefs = window.filter == null || reducedKey != null ? null : expressionReferences(window.filter);
            if (reducedKey != null) {
                diagnostics.info("sequence.filter.reduced", loc,
                        "window.filter '" + window.filter + "' is evaluated as an additional partition key (" + String.join(",", entity.keys()) + "," + reducedKey + ")");
            }
            for (final Op op : def.ops) {
                final Operator operator = OperatorCatalog.get(Scope.sequence, op.type);
                if (operator == null) {
                    diagnostics.error("sequence.op", loc, "unknown sequence op: " + op.type);
                    continue;
                }
                final List<String> fields = new ArrayList<>(op.fields);
                if (op.expr != null) {
                    final OutputColumn anonymous = desugarExpression(def, op.expr, computeAt);
                    if (anonymous != null) fields.add(anonymous.canonicalName);
                }
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
                                : "sinceEvent".equals(op.type) ? "since_" + unit : op.type.toLowerCase();
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
                        final OutputColumn c = newColumn(def.name, Scope.sequence, op.type,
                                def.name + "_" + window.token() + "_count", Schema.FieldType.INT64, computeAt);
                        c.coordinates.put("func", "count");
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
                                final OutputColumn c = newColumn(def.name, Scope.sequence, op.type, base + "lag" + i, ref.type(), computeAt);
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
                            final String decayBy = op.decayBy == null ? "events" : op.decayBy;
                            if (!List.of("events", "time").contains(decayBy)) diagnostics.error("sequence.ewma.decayBy", loc, "decayBy must be events | time");
                            for (final Double h : op.halflife) {
                                final OutputColumn c = newColumn(def.name, Scope.sequence, op.type, base + "ewma" + number(h), Schema.FieldType.FLOAT64, computeAt);
                                c.coordinates.put("halflife", number(h));
                                c.coordinates.put("decayBy", decayBy);
                                c.coordinates.put("field", canonicalOf(field)); addPastInput(c, field);
                                finishSequence(c, def, entity, window, filterRefs, reducedKey, op);
                            }
                        }
                        case "runLength" -> {
                            if (op.value == null) diagnostics.error("sequence.runLength.value", loc, "runLength requires 'value'");
                            final OutputColumn c = newColumn(def.name, Scope.sequence, op.type, base + "runlength", Schema.FieldType.INT64, computeAt);
                            c.coordinates.put("value", String.valueOf(op.value));
                            c.coordinates.put("field", canonicalOf(field)); addPastInput(c, field);
                            finishSequence(c, def, entity, window, filterRefs, reducedKey, op);
                        }
                        case "aggregate" -> {
                            final List<String> funcs = op.funcs.isEmpty() ? List.of("count", "mean") : op.funcs;
                            for (final String func : funcs) {
                                final Schema.FieldType type = OperatorCatalog.aggregateOutput(func, ref.type());
                                if (type == null) {
                                    diagnostics.error("sequence.aggregate.func", loc, "unknown aggregate func: " + func);
                                    continue;
                                }
                                if (List.of("mean", "avg", "rate").contains(func) && isOutcomeLike(ref) && hintedBlocks.add("sequence.aggregate.encoding:" + def.name)) {
                                    // once per block: the same hint for every window × field × func would drown the report
                                    diagnostics.hint("sequence.aggregate.encoding", loc,
                                            "aggregate " + func + " over outcome field '" + field + "' (and other outcome means in this block) has no shrinkage; consider population encoding with a windowed keySet (§4.3 役割分担)");
                                }
                                final OutputColumn c = newColumn(def.name, Scope.sequence, op.type, base + func, type, computeAt);
                                c.coordinates.put("func", func);
                                c.coordinates.put("field", canonicalOf(field)); addPastInput(c, field);
                                finishSequence(c, def, entity, window, filterRefs, reducedKey, op);
                            }
                        }
                        default -> diagnostics.error("sequence.op", loc, "unsupported sequence op: " + op.type);
                    }
                }
            }
        }
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

    private static String number(final Double d) {
        return d == Math.floor(d) && !Double.isInfinite(d) ? Long.toString(d.longValue()) : d.toString().replace('.', 'p');
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
        c.coordinates.put("entity", entity.name());
        c.coordinates.put("window", window.token());
        if (window.maxAge != null) c.coordinates.put("maxAge", window.maxAge.toString());
        if (window.maxEvents != null) c.coordinates.put("maxEvents", window.maxEvents.toString());
        if (reducedKey != null) {
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
        classifyPast(c, entity.minInterval());
        c.validFor = def.validFor;
        register(c);
    }

    private void classifyPast(final OutputColumn c, final Duration minInterval) {
        AvailableAt past = null;
        for (final String p : c.pastInputs) {
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
        expandEncoding(def, computeAt);
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
        final String loc = def.location();
        final JsonObject defFit = parseJsonObject(def.fitJson);
        final FeatureSpec.FitSpec fitSpec = new FeatureSpec.FitSpec();
        fitSpec.artifactUri = spec.fit.artifactUri;
        fitSpec.refit = spec.fit.refit;
        FitMode mode = FitMode.statik;
        if (defFit != null) {
            if (SourceContract.Json.string(defFit, "mode") != null) mode = FeatureSpec.parseFitMode(SourceContract.Json.string(defFit, "mode"), diagnostics, loc);
            FeatureSpec.FitSpec.parseArtifact(defFit, fitSpec);
            for (final String key : List.of("cadence", "window", "warmStart")) {
                if (defFit.has(key)) diagnostics.warning(codePrefix + ".fit." + key, loc, "fit." + key + " is not implemented yet and ignored (" + fitted + " on the whole input)");
            }
        }
        if (mode != FitMode.statik) {
            diagnostics.error(codePrefix + ".fit.mode", loc, def.type + " requires fit.mode static (" + why + "); expanding / fold are not available");
        }
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
        c.status = selfSide.isStaticallyAtOrBefore(c.computeAt) ? Status.staticSafe
                : selfSide.isStatic() ? Status.violation : Status.runtimeFilter;
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
                    c.coordinates.put("offset", offsetColumn);
                    addSelfInput(c, offsetColumn);
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
        fitSpec.minBlocks = spec.fit.minBlocks;
        FeatureSpec.FitSpec.parseForward(defFit, fitSpec, diagnostics, loc, spec.timeField);
        Integer folds = spec.fit.folds;
        if (defFit != null && SourceContract.Json.integer(defFit, "folds") != null) folds = SourceContract.Json.integer(defFit, "folds");
        if (mode == FitMode.fold && folds < 2) {
            diagnostics.error("fit.folds", loc, "fit.folds must be at least 2: " + folds);
            folds = 2;
        }
        fitSpec.groupBy = groupBy;
        fitSpec.folds = folds;
        // static and fold both fit sufficient statistics over the input and apply them by lookup; fold
        // subtracts the row's own fold so a row never sees its own contribution (out-of-fold statistics)
        final boolean isStatic = mode.isLookup();
        if (mode == FitMode.statik) {
            diagnostics.info("fit.mode.static", loc, "fit.mode static fits the statistics on the whole input"
                    + (fitSpec.artifactUri == null ? " (no artifact: in-pipeline only)" : " and persists them under " + fitSpec.artifactUri + "/<planHash>/")
                    + "; training rows include their own outcome, so use expanding for leak-safe backfill and static for serving / offline analysis");
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
                    + ") whose targets are known at predictAt — a stepwise expanding fit computed as a parallel Combine per (key, block); the row's own block is never included"
                    + (fitSpec.minBlocks == null || fitSpec.minBlocks <= 1 ? "" : "; rows with fewer than " + fitSpec.minBlocks + " preceding blocks read null")
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
        record ResolvedTarget(String name, String reference, List<String> stats) {}
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
            resolvedTargets.add(new ResolvedTarget(name, reference, stats));
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
            final JsonElement hierarchy = ks.hierarchyJson == null ? null : JsonParser.parseString(ks.hierarchyJson);
            if (hierarchy != null && hierarchy.isJsonArray()) {
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
                default -> diagnostics.error("encoding.keySet.structure", loc, "structure must be flat | hierarchy | cross (sequence is v1): " + structure);
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
                    diagnostics.error("encoding.shrinkage.estimator", loc, "estimator: backoff is not valid for an overlapping lattice (additive / cross); use sequential");
                }
                if (additiveAt != levels.size() - 1) {
                    diagnostics.error("encoding.hierarchy.additive", loc, "'additive' must be the last entry before the global level");
                }
            }
            if (mode == FitMode.fold && groupBy == null) {
                for (final String key : ks.keys) {
                    final Ref ref = resolve(key);
                    if (ref != null && isOutcomeLike(ref)) {
                        diagnostics.error("fit.groupBy.required", loc, "keySet key '" + key + "' derives from a past target; fit.mode fold requires fit.groupBy (entity-level folds)");
                    }
                }
            }
            final boolean needsRoot = shrinkage.enabled || levels.size() > 1
                    || resolvedTargets.stream().anyMatch(t -> t.stats.contains("share"));
            if (needsRoot) levels.add(List.of());
            lattices.add(new Lattice(ks, shrinkage, levels, additiveAt));
        }
        if (def.offset != null && lattices.stream().anyMatch(l -> l.shrinkage.scale != Shrinkage.Scale.identity)) {
            diagnostics.error("encoding.offset.scale", loc, "offset with a logit / log shrinkage scale is not implemented yet");
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
        // the global level is shared by every keySet: register it first so it forms a single stage
        for (final int[] pair : pairs) {
            final Lattice lattice = lattices.get(pair[0]);
            final ResolvedTarget target = resolvedTargets.get(pair[1]);
            if (lattice.levels.get(lattice.levels.size() - 1).isEmpty()) {
                for (final Window window : windowsOf(lattice.keySet)) {
                    levelStats(def, List.of(), lookupWindow(window, mode, def), target.name, target.reference, offsetColumn, computeAt, mode, fitSpec);
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
                        "keys", String.join("_", ks.keys),
                        "window", window == null ? "" : window.token(),
                        "target", target.name));
                for (final String stat : target.stats) {
                    final OperatorCatalog.Stat s = OperatorCatalog.stat(stat);
                    if (s == null || (s.requiresTarget() && target.reference == null)) continue;
                    names.put("stat", stat);
                    final String canonical = render(naming, names);
                    final boolean shrunk = lattice.shrinkage.enabled && List.of("mean", "rate").contains(stat);
                    if (isStatic && !shrunk && !"share".equals(stat)) {
                        // static: every statistic is derived from the fitted leaf sufficient statistics
                        if (!s.sufficient()) {
                            // needs the per-key value distribution, not the (n, Σy, Σy²) the fit keeps
                            diagnostics.error("encoding.stat.static", loc, "stat " + stat + " is not available in fit.mode " + mode.token() + " (expanding only)");
                            continue;
                        }
                        final Shrinkage.Level leaf = levelStats(def, ks.keys, null, target.name, target.reference, offsetColumn, computeAt, mode, fitSpec);
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
                        produced++;
                        continue;
                    }
                    // lattice: hidden statistics per level, composed in a row column
                    final List<Shrinkage.Level> levels = new ArrayList<>();
                    for (final List<String> levelKeys : lattice.levels) {
                        if (levelKeys.size() == 1 && Shrinkage.ADDITIVE.equals(levelKeys.get(0))) {
                            final List<List<Shrinkage.Level>> mains = new ArrayList<>();
                            for (final String key : ks.keys) {
                                final KeySet main = singleKeySets.get(key);
                                if (main == null) continue;
                                final List<Shrinkage.Level> chain = new ArrayList<>();
                                chain.add(levelStats(def, main.keys, window, target.name, target.reference, offsetColumn, computeAt, mode, fitSpec));
                                chain.add(levelStats(def, List.of(), window, target.name, target.reference, offsetColumn, computeAt, mode, fitSpec));
                                mains.add(chain);
                            }
                            levels.add(new Shrinkage.Level(Shrinkage.ADDITIVE, null, null, mains));
                        } else {
                            levels.add(levelStats(def, levelKeys, window, target.name, target.reference, offsetColumn, computeAt, mode, fitSpec));
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
                        final OutputColumn c = newColumn(def.name, Scope.row, "compose", canonical, Schema.FieldType.FLOAT64, computeAt);
                        composeCoordinates(c, ks, target, stat, encoded, shrinkage, levels);
                        finishComposed(c, def);
                        produced++;
                    }
                    if (shrinkage.emits("deviations")) {
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
        if (declared.maxAge == null) return null;
        final Window window = new Window();
        window.maxAge = declared.maxAge;
        return window;
    }

    private static List<Window> windowsOf(final KeySet ks) {
        return ks.windows.isEmpty() ? Collections.singletonList(null) : ks.windows;
    }

    private static boolean sameWindows(final List<Window> a, final List<Window> b) {
        if (a.size() != b.size()) return false;
        for (int i = 0; i < a.size(); i++) {
            if (!Objects.equals(a.get(i).token(), b.get(i).token()) || !Objects.equals(a.get(i).filter, b.get(i).filter)) return false;
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
        c.coordinates.put("weights", shrinkage.weights);
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
        }
    }

    private void finishComposed(final OutputColumn c, final FeatureDef def) {
        if (c.availableAt == null) c.availableAt = AvailableAt.atEventTime();
        // the hidden statistics are available at computeAt by construction; the composed value inherits that
        c.status = c.availableAt.isStaticallyAtOrBefore(c.computeAt) ? Status.staticSafe
                : c.availableAt.isStatic() ? Status.violation : Status.runtimeFilter;
        c.validFor = def.validFor;
        register(c);
    }

    /**
     * Hidden sufficient statistics ({@code n}, {@code sum}) of one lattice level for a (window, target),
     * registered once per block and shared by every keySet whose lattice contains the level.
     */
    private Shrinkage.Level levelStats(final FeatureDef def, final List<String> levelKeys, final Window window,
                                       final String targetName, final String targetReference, final String offsetColumn,
                                       final AvailableAt computeAt, final FitMode mode, final FeatureSpec.FitSpec fitSpec) {
        final String token = levelKeys.isEmpty() ? Shrinkage.GLOBAL : String.join("_", levelKeys);
        final String base = render("{block}__{keys}__{window}__{target}", Map.of(
                "block", def.name, "keys", token, "window", window == null ? "" : window.token(), "target", targetName));
        final String nName = base + "__n";
        final String sumName = base + "__sum";
        final boolean isStatic = mode.isLookup();
        if (!columnsByCanonical.containsKey(nName)) {
            // static / fold fits also keep Σy² so std can be derived from the artifact
            for (final String stat : isStatic ? new String[]{"count", "sum", "sumsq"} : new String[]{"count", "sum"}) {
                final String name = switch (stat) { case "count" -> nName; case "sum" -> sumName; default -> base + "__sumsq"; };
                if (!"count".equals(stat) && targetReference == null) continue;
                final OutputColumn c = newColumn(def.name, Scope.population, "encoding", name, Schema.FieldType.FLOAT64, computeAt);
                c.intermediate = true;
                c.anonymous = true;
                final KeySet level = new KeySet();
                level.keys = levelKeys;
                level.windows = window == null ? new ArrayList<>() : List.of(window);
                populationColumn(c, level, window, targetReference, stat, offsetColumn, mode, def, fitSpec);
                register(c);
            }
        }
        return new Shrinkage.Level(token, nName, targetReference == null ? nName : sumName, null);
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
        if (mode == FitMode.fold) {
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
        if (mode == FitMode.forward) forwardCoordinates(c, window, targetReference, offsetColumn, def, fitSpec);
        c.coordinates.put("keys", String.join(",", ks.keys));
        if (window != null) {
            c.coordinates.put("window", window.token());
            if (window.maxAge != null) c.coordinates.put("maxAge", window.maxAge.toString());
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
        if (offsetColumn != null) {
            addSelfInput(c, offsetColumn);
            addPastInput(c, offsetColumn);
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
        final String loc = def.location();
        final ForwardBlocks blocks = fitSpec.forwardBlocks();
        if (blocks.bucket() != null) c.coordinates.put("blockBucket", blocks.bucket());
        else c.coordinates.put("blockSizeMillis", Long.toString(blocks.sizeMillis()));
        c.coordinates.put("minBlocks", Integer.toString(fitSpec.minBlocks == null ? 1 : fitSpec.minBlocks));
        c.coordinates.put("blockField", spec.timeField);
        final FieldContract time = inputFields.get(spec.timeField);
        c.coordinates.put("blockFieldType", time == null || time.getType() == null ? "timestamp" : time.getType().getType().name());
        long lag = 0;
        for (final String reference : new String[]{targetReference, offsetColumn}) {
            if (reference == null) continue;
            final Ref ref = resolve(reference);
            if (ref == null) continue;
            final AvailableAt at = ref.availableAt();
            if (at == null || at.isPreEvent()) continue;
            if (!at.isStatic()) {
                diagnostics.error("fit.mode.forward.dynamic", loc, "fit.mode forward needs a static availability for '" + reference + "' (is " + at.describe() + "): the block boundary cannot be decided per row");
                continue;
            }
            lag = Math.max(lag, at.getOffset().toMillis());
        }
        c.coordinates.put("forwardLagMillis", Long.toString(lag));
        if (window != null && window.maxAge != null) {
            final int k = blocks.windowBlocks(window.maxAge);
            c.coordinates.put("windowBlocks", Integer.toString(k));
            if (hintedBlocks.add(def.name + "#forwardWindow")) {
                diagnostics.info("fit.mode.forward.window", loc, "maxAge " + window.maxAge + " is rounded up to " + k + " block(s) of " + blocks.describe() + " in fit.mode forward");
            }
        }
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
        for (final OutputColumn c : columns) {
            if (!c.intermediate && spec.output.include == null && isExcluded(c)) {
                // a role column is the data contract, not a feature: no projection removes it (see applyInclude)
                if (c.role == null) c.intermediate = true;
                else { keptByRole.add(c.canonicalName); excludedRoles.add(outputNameOf(c) + " (" + c.role + ")"); }
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
                        c.canonicalName + ": availability is not decidable statically (" + c.availableAt.getDynamicReasons() + "); the engine must filter contributions by effectiveAvailableAt ≤ computeAt");
            } else if (c.status == Status.windowShift && !c.intermediate) {
                diagnostics.info("availability.windowShift", loc,
                        c.canonicalName + ": window near edge shifted by " + c.windowShift + " (past availability + ingestionLag)");
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
            final boolean indicator = !c.intermediate && spec.output.nullPolicy == NullPolicy.indicator && !keptByRole.contains(c.canonicalName);
            if (indicator && (c.validFor != null || c.scope == Scope.sequence || c.scope == Scope.population || "softmax".equals(c.operator))) {
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
    private boolean isExcluded(final OutputColumn c) {
        for (final String pattern : spec.output.exclude) {
            final int colon = pattern.indexOf(':');
            if (colon > 0) {
                final String selector = pattern.substring(0, colon);
                final String value = pattern.substring(colon + 1);
                final boolean match = switch (selector) {
                    case "derivedFrom" -> c.derivedFrom.contains(value);
                    case "evidence" -> "declared".equals(value) && c.declaredEvidence;
                    case "scope" -> c.scope.name().equals(value);
                    case "block" -> c.block.equals(value);
                    default -> false;
                };
                if (match) return true;
                continue;
            }
            if (pattern.endsWith(".*")) {
                final String block = pattern.substring(0, pattern.length() - 2);
                if (c.block.equals(block)) return true;
            } else if (pattern.equals(c.canonicalName) || pattern.equals(c.block)) {
                return true;
            }
        }
        return false;
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
                k = FeaturePlan.StageKind.sequence;
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
            int target = at;
            // a row column over fitted statistics is evaluated in the block's fit stage (artifact / lambdas live there)
            for (final String dep : r.inputs) {
                final OutputColumn d = columnsByCanonical.get(dep);
                if (d != null && FitMode.isLookupToken(d.coordinates.get("fit")) && fitStageOf.containsKey(d.block)) {
                    target = Math.min(target, fitStageOf.get(d.block));
                }
            }
            if (target < earliest) {
                throw new IllegalStateException("feature stage scheduling: " + name + " needs stage " + earliest + " but is required at stage " + target);
            }
            place(r, target, strictInputs(r));
        }

        List<FeaturePlan.Stage> build() {
            // row columns nobody else reads: the last stage (no shuffle either way)
            if (slots.isEmpty() && !rowEarliest.isEmpty()) slots.add(new Slot(FeaturePlan.StageKind.row, List.of()));
            for (final OutputColumn c : columns) {
                if (rowEarliest.containsKey(c.canonicalName) && !stageOf.containsKey(c.canonicalName)) placeRow(c.canonicalName, slots.size() - 1);
            }
            final List<FeaturePlan.Stage> stages = new ArrayList<>();
            for (final Slot slot : slots) stages.add(slot.build(stages.size(), dependsOn(slot, stages.size())));
            return stages;
        }

        /**
         * Stages whose keyed / fit columns the columns of this slot need (engine doc §9.4: the edges of the stage
         * DAG). A row column is not a node: it is followed through to its own dependencies, because the linear chain
         * places it in its first consumer's stage and carries the value forward, while a branch evaluating the same
         * columns from the stage input would simply recompute it. Input fields have no stage; a dependency inside
         * the same stage is not an edge.
         */
        private List<Integer> dependsOn(final Slot slot, final int index) {
            final TreeSet<Integer> deps = new TreeSet<>();
            final Set<String> visited = new HashSet<>();
            for (final String name : slot.names) collectDeps(name, index, deps, visited);
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
        return sha256(canonical(sourcesDocument) + "\u0000" + canonical(withoutArtifact(parameters)));
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
