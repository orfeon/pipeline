package com.mercari.solution.util.pipeline.feature;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Compiled execution plan of a feature spec: expanded columns with lineage, evaluation stages
 * (key changes = shuffles), the output schema, and diagnostics. Pure data — no Beam dependency —
 * so the same object backs {@code validate --expand}, the transform, and tests.
 */
public class FeaturePlan implements Serializable {

    public enum StageKind { row, context, sequence, population, fit, groupBy }

    /**
     * One evaluation stage: the columns evaluated under one key in one pass (docs/design/feature-engine.md §3.1).
     * Columns are scheduled by key affinity, so a stage may gather blocks from anywhere in the config, and
     * two stages may share a key when a dependency forces the split.
     */
    public record Stage(int index, StageKind kind, List<String> keys, List<String> blocks, List<String> columnNames,
                        List<Integer> dependsOn) implements Serializable {
        public int columns() {
            return columnNames.size();
        }
        /** Keyed stages (context / sequence / population / groupBy) are one GroupByKey in the engine. */
        public boolean isKeyed() {
            return kind != StageKind.row && kind != StageKind.fit;
        }
        /** Sequence / population stages replay each key's rows in time order (batch only). */
        public boolean isReplay() {
            return kind == StageKind.sequence || kind == StageKind.population;
        }
        /**
         * A key-less replay stage: every row under ONE key — one worker thread (a shrinkage lattice's
         * global level, a share denominator; a key-less context / groupBy is rejected at parse time).
         */
        public boolean runsUnderSingleKey() {
            return isReplay() && keys.isEmpty();
        }
        public String describe() {
            return "#" + index + " " + kind + (keys.isEmpty() ? "" : " key=" + keys) + " blocks=" + blocks + " columns=" + columns()
                    + " deps=" + dependsOn;
        }
    }

    /**
     * A data audit query derived from the plan (docs/design/feature-dsl.md §7): hot-key row counts per keyed stage so
     * the per-key memory budget (docs "Performance and sizing") can be checked before a run. {@code {input}}
     * stands for the transform's input relation; identifiers are emitted bare (quote them for your dialect if needed).
     */
    public record AuditQuery(String id, List<String> keys, List<String> stages, String sql, String note) implements Serializable {
        public String describe() {
            return id + (keys.isEmpty() ? "" : " keys=" + keys) + " stages=" + stages
                    + "\n    " + sql + (note == null ? "" : "\n    -- " + note);
        }
    }

    private static final int AUDIT_TOP_KEYS = 20;

    private final FeatureSpec spec;
    private final Map<String, SourceContract> sources;
    private final Map<String, SourceContract.FieldContract> inputFields;
    private final List<OutputColumn> columns;
    private final List<Stage> stages;
    private final Schema outputSchema;
    private final Diagnostics diagnostics;
    private final String hash;
    private final String outputHash;
    private final List<ObservedAtAudit> observedAtAudits;

    /**
     * One observedAt audit entry (DSL spec §7): an input field whose contract names the column holding its real
     * observation time. The engine compares that column with the declared availability
     * ({@code event_time + availableAt}; predictAt when the declaration is dynamic) and with predictAt.
     * {@code present} is false when the input schema is known and lacks the observation column (the entry is
     * reported but cannot run).
     */
    public record ObservedAtAudit(String field, String source, String observedAtField, String observedAtType, boolean present,
                                  AvailableAt availableAt, AvailableAt predictAt) implements Serializable {

        /** Millis to add to event time for the declared deadline; null when the declaration is not static (predictAt is used). */
        public Long deadlineOffsetMillis() {
            return availableAt.isStatic() && !availableAt.isPreEvent() ? availableAt.getOffset().toMillis() : null;
        }

        public Long predictAtOffsetMillis() {
            return predictAt != null && predictAt.isStatic() && !predictAt.isPreEvent() ? predictAt.getOffset().toMillis() : null;
        }

        public String describe() {
            return field + " observedAt=" + observedAtField + " declared=" + availableAt.describe()
                    + (deadlineOffsetMillis() == null ? " (dynamic: checked against predictAt)" : "")
                    + (present ? "" : " (observation column missing: not audited)");
        }
    }

    FeaturePlan(final FeatureSpec spec,
                final Map<String, SourceContract> sources,
                final Map<String, SourceContract.FieldContract> inputFields,
                final List<OutputColumn> columns,
                final List<Stage> stages,
                final Schema outputSchema,
                final Diagnostics diagnostics,
                final String hash,
                final String outputHash,
                final List<ObservedAtAudit> observedAtAudits) {
        this.spec = spec;
        this.sources = sources;
        this.inputFields = inputFields;
        this.columns = columns;
        this.stages = stages;
        this.outputSchema = outputSchema;
        this.diagnostics = diagnostics;
        this.hash = hash;
        this.outputHash = outputHash;
        this.observedAtAudits = observedAtAudits;
    }

    public FeatureSpec getSpec() { return spec; }
    public Map<String, SourceContract> getSources() { return sources; }
    /** Input relation fields resolved through lineage. */
    public Map<String, SourceContract.FieldContract> getInputFields() { return inputFields; }
    /** Every expanded column, intermediates included, in evaluation order. */
    public List<OutputColumn> getColumns() { return Collections.unmodifiableList(columns); }
    /** Columns emitted by the transform (no intermediates, no excluded). */
    public List<OutputColumn> getEmittedColumns() {
        return columns.stream().filter(c -> !c.intermediate).toList();
    }
    public List<Stage> getStages() { return Collections.unmodifiableList(stages); }
    public Schema getOutputSchema() { return outputSchema; }
    public Diagnostics getDiagnostics() { return diagnostics; }
    /** Canonical-form hash of the spec + sources (content address for fit artifacts / candidate identity). */
    public String getHash() { return hash; }
    /** Version directory of fit artifacts: {@code fit.artifact.id} when pinned, else the plan hash. */
    public String getArtifactVersion() { return spec.fit.artifactId != null ? spec.fit.artifactId : hash; }
    /** Identity of the output table: plan hash + projection (emitted names, roles, include content). */
    public String getOutputHash() { return outputHash; }
    /** The observedAt audit entries (input fields with an {@code observedAtField}), runnable ones and not. */
    public List<ObservedAtAudit> getObservedAtAudits() { return Collections.unmodifiableList(observedAtAudits); }
    /** The audit entries the engine runs: observation column present and the audit not switched off. */
    public List<ObservedAtAudit> getRunnableObservedAtAudits() {
        if ("off".equals(spec.audit.observedAt)) return List.of();
        return observedAtAudits.stream().filter(ObservedAtAudit::present).toList();
    }
    /** Emitted columns that a role names (never features for the consumer). */
    public Map<String, String> getRoleColumns() {
        final Map<String, String> roles = new java.util.LinkedHashMap<>();
        for (final Map.Entry<String, String> e : spec.output.roles.entrySet()) {
            final String name = e.getValue();
            String resolved = inputFields.containsKey(name) ? name : null;
            if (resolved == null && "baseline".equals(e.getKey())) {
                // a baseline name resolves to its emitted copy (baselines[].emit)
                for (final FeatureSpec.BaselineDef b : spec.baselines) if (b.name().equals(name) && b.emit() != null) resolved = b.emit();
            }
            if (resolved != null && !inputFields.containsKey(resolved)) {
                final String emitted = resolved;
                resolved = null;
                for (final OutputColumn c : columns) {
                    if (!c.intermediate && c.canonicalName.equals(emitted)) { resolved = c.outputName; break; }
                }
            }
            if (resolved == null) {
                for (final OutputColumn c : columns) {
                    if (!c.intermediate && (c.canonicalName.equals(name) || c.outputName.equals(name))) { resolved = c.outputName; break; }
                }
            }
            if (resolved != null) roles.put(e.getKey(), resolved);
        }
        return roles;
    }

    public OutputColumn getColumn(final String canonicalName) {
        for (final OutputColumn c : columns) {
            if (c.canonicalName.equals(canonicalName)) return c;
        }
        return null;
    }

    /** Every keyed stage (context / sequence / population / groupBy) is one GroupByKey in the engine. */
    public int getShuffleCount() {
        int count = 0;
        for (final Stage s : stages) {
            if (s.isKeyed()) count++;
        }
        return count;
    }

    /**
     * Waves of the stage DAG (engine doc §9.4): stage {@code i} is in wave {@code 1 + max(wave of its
     * dependencies)}, so the stages of one wave are mutually independent and are evaluated in parallel from
     * the same input (unless {@code engine.parallelWaves} is off or the pipeline streams); the wave count is
     * the depth of the DAG's critical path — the barrier count the wave execution leaves. Conservative: the row columns
     * a stage hosts count too (the engine recomputes the evaluable ones on the wave input, {@link #getPreludeColumns}).
     */
    public List<List<Integer>> getWaves() {
        final int[] depth = new int[stages.size()];
        int max = 0;
        for (final Stage s : stages) {
            int d = 1;
            for (final int dep : s.dependsOn) d = Math.max(d, depth[dep] + 1);
            depth[s.index] = d;
            max = Math.max(max, d);
        }
        final List<List<Integer>> waves = new ArrayList<>();
        for (int w = 0; w < max; w++) waves.add(new ArrayList<>());
        for (final Stage s : stages) waves.get(depth[s.index] - 1).add(s.index);
        return waves;
    }

    /** Wave (1-based) of a stage, see {@link #getWaves()}. */
    public int getWave(final int stageIndex) {
        final List<List<Integer>> waves = getWaves();
        for (int w = 0; w < waves.size(); w++) {
            if (waves.get(w).contains(stageIndex)) return w + 1;
        }
        throw new IllegalArgumentException("no stage #" + stageIndex);
    }

    // ---- engine-wave geometry (engine doc §9.4.2) --------------------------------------------------------
    // The Beam wiring (FeatureStages) and the shuffle estimate below both read these, so what the report
    // promises and what the engine wires cannot drift apart.

    private transient List<List<Stage>> engineWaves;
    private transient Map<String, Integer> engineWaveOfColumn;
    private transient Map<String, OutputColumn> columnsByName;
    private transient Map<Integer, List<OutputColumn>> preludeByWave;

    /** Execution waves of the parallel engine: {@link #getWaves()} without the groupBy finalize stage. */
    public List<List<Stage>> getEngineWaves() {
        if (engineWaves == null) {
            final List<List<Stage>> waves = new ArrayList<>();
            for (final List<Integer> wave : getWaves()) {
                final List<Stage> stagesOfWave = new ArrayList<>();
                for (final int i : wave) if (stages.get(i).kind != StageKind.groupBy) stagesOfWave.add(stages.get(i));
                if (!stagesOfWave.isEmpty()) waves.add(stagesOfWave);
            }
            engineWaves = waves;
        }
        return engineWaves;
    }

    private Map<String, Integer> engineWaveOfColumn() {
        if (engineWaveOfColumn == null) {
            final Map<String, Integer> map = new HashMap<>();
            final List<List<Stage>> waves = getEngineWaves();
            for (int w = 0; w < waves.size(); w++) {
                for (final Stage s : waves.get(w)) for (final String name : s.columnNames) map.put(name, w);
            }
            engineWaveOfColumn = map;
        }
        return engineWaveOfColumn;
    }

    private Map<String, OutputColumn> columnsByName() {
        if (columnsByName == null) {
            final Map<String, OutputColumn> map = new HashMap<>();
            for (final OutputColumn c : columns) map.put(c.getCanonicalName(), c);
            columnsByName = map;
        }
        return columnsByName;
    }

    /** Fields the base rows of engine wave {@code w} carry before its prelude: input fields and earlier waves' columns. */
    private Set<String> availableBefore(final int w) {
        final Set<String> fields = new HashSet<>(inputFields.keySet());
        for (final Map.Entry<String, Integer> e : engineWaveOfColumn().entrySet()) if (e.getValue() < w) fields.add(e.getKey());
        return fields;
    }

    /**
     * The row columns hosted by the stages of engine wave {@code w} that the wave input can evaluate (their
     * inputs — and the fields of their variance-components estimate, if any — are input fields, columns of
     * earlier waves or such row columns), in expansion order (dependencies first). The engine evaluates them
     * on the wave input before the fan-out so every branch sees them ({@code Wave&lt;n&gt;_Rows}).
     */
    public List<OutputColumn> getPreludeColumns(final int w) {
        if (preludeByWave == null) preludeByWave = new HashMap<>();
        return preludeByWave.computeIfAbsent(w, wave -> {
            final Set<String> available = availableBefore(wave);
            final List<OutputColumn> prelude = new ArrayList<>();
            for (final OutputColumn c : columns) {
                final Integer at = engineWaveOfColumn().get(c.getCanonicalName());
                if (at == null || at != wave || !FeaturePlanCompiler.isRowColumn(c)) continue;
                if (available.containsAll(c.getInputs()) && vcFieldsAvailable(List.of(c), available)) {
                    prelude.add(c);
                    available.add(c.getCanonicalName());
                }
            }
            return prelude;
        });
    }

    /** The fields every branch of engine wave {@code w} reads from its input: base fields plus the prelude. */
    public Set<String> getWaveInputFields(final int w) {
        final Set<String> fields = availableBefore(w);
        for (final OutputColumn c : getPreludeColumns(w)) fields.add(c.getCanonicalName());
        return fields;
    }

    /** Every key is on the wave input (input field, earlier wave, prelude row column): the base rows carry it. */
    public boolean keysAvailable(final List<String> keys, final int w) {
        return getWaveInputFields(w).containsAll(keys);
    }

    /**
     * The next stage when the merge of engine wave {@code w} can ride its GroupByKey: a single context stage
     * whose key the base rows already carry. Its variance-components estimate, if any, is taken over the
     * wave input (the flattened pieces would count the partial rows too), so the fields it reads must be on
     * the wave input.
     */
    public Stage getFoldTarget(final Stage next, final int w) {
        if (next.kind != StageKind.context || !keysAvailable(next.keys, w)) return null;
        final List<OutputColumn> stageColumns = new ArrayList<>();
        for (final String name : next.columnNames) stageColumns.add(columnsByName().get(name));
        return vcFieldsAvailable(stageColumns, getWaveInputFields(w)) ? next : null;
    }

    private boolean vcFieldsAvailable(final List<OutputColumn> stageColumns, final Set<String> available) {
        for (final VarianceComponents.LevelSpec spec : VarianceComponents.specsOf(stageColumns, columnsByName())) {
            if (!available.containsAll(spec.keys())) return false;
            if (spec.field() != null && !available.contains(spec.field())) return false;
            if (spec.offsetColumn() != null && !available.contains(spec.offsetColumn())) return false;
            if (spec.foldKeys() != null && !available.containsAll(spec.foldKeys())) return false;
        }
        return true;
    }

    /**
     * Shuffle count of the parallel wave execution (engine doc §9.4.2), mirroring the engine's wave loop
     * over the shared wave geometry above: one shuffle per keyed single-stage wave; per fan-out wave one
     * shuffle for its keyed branches (they run in parallel) plus the row-id merge GroupByKey — unless the
     * merge rides the next stage's GroupByKey ({@link #getFoldTarget}) or the groupBy finalize — plus one
     * Reshuffle pinning random row ids before the first fan-out not behind a GroupByKey ({@code engine.rowId}
     * removes it). The linear chain ({@code engine.parallelWaves: false}, streaming) pays
     * {@link #getShuffleCount()} instead.
     */
    public int getDagShuffleEstimate() {
        final List<List<Stage>> waves = getEngineWaves();
        final Stage groupBy = stages.stream().filter(s -> s.kind == StageKind.groupBy).findFirst().orElse(null);
        boolean pinned = !spec.engine.rowId.isEmpty();
        int count = groupBy != null ? 1 : 0; // the finalize GroupByKey
        for (int w = 0; w < waves.size(); w++) {
            final List<Stage> wave = waves.get(w);
            if (wave.size() == 1) {
                if (wave.get(0).isKeyed()) {
                    count++;
                    pinned = true; // its GroupByKey materialises the row ids like the pin Reshuffle would
                }
                continue;
            }
            if (!pinned) {
                count++; // RowId_Pin
                pinned = true;
            }
            if (wave.stream().anyMatch(Stage::isKeyed)) count++;
            final Stage foldInto = w + 1 < waves.size() && waves.get(w + 1).size() == 1 ? getFoldTarget(waves.get(w + 1).get(0), w) : null;
            if (foldInto != null) {
                count++; // the folded stage's own GroupByKey
                w++;
                continue;
            }
            if (w + 1 == waves.size() && groupBy != null && keysAvailable(groupBy.keys, w)) continue; // rides the finalize
            count++; // the wave's row-id merge
        }
        return count;
    }

    /**
     * Hot-key audit queries: one per distinct key set of the keyed stages (context / sequence / population /
     * groupBy), plus the row count for a global (single key) level. Keys that are not input fields are
     * intermediate columns; their query must be run on the relation as it stands before that stage.
     */
    public List<AuditQuery> getAuditQueries() {
        final Map<List<String>, List<String>> byKeys = new java.util.LinkedHashMap<>();
        for (final Stage s : stages) {
            if (!s.isKeyed()) continue;
            byKeys.computeIfAbsent(s.keys, k -> new ArrayList<>()).add("#" + s.index + " " + s.kind);
        }
        final List<AuditQuery> queries = new ArrayList<>();
        int n = 0;
        for (final Map.Entry<List<String>, List<String>> e : byKeys.entrySet()) {
            final List<String> keys = e.getKey();
            final String id = "audit" + (n++);
            if (keys.isEmpty()) {
                queries.add(new AuditQuery(id, keys, e.getValue(),
                        "SELECT COUNT(1) AS row_count FROM {input}",
                        "global level: one key sorted on one worker (sorted chunks spilled to its local disk beyond the spill budget, deleted after the key); row_count bounds the spill, memory holds only the retained history"));
                continue;
            }
            final String keyList = String.join(", ", keys);
            final String notNull = keys.stream().map(k -> k + " IS NOT NULL").collect(java.util.stream.Collectors.joining(" AND "));
            final String sql = "SELECT " + keyList + ", COUNT(1) AS row_count FROM {input} WHERE " + notNull
                    + " GROUP BY " + keyList + " ORDER BY row_count DESC LIMIT " + AUDIT_TOP_KEYS;
            final List<String> derived = keys.stream().filter(k -> !inputFields.containsKey(k)).toList();
            final String note = derived.isEmpty()
                    ? "each key is sorted on one worker (sorted chunks spilled to local disk beyond the spill budget, deleted after the key): the top row_count bounds the spill; memory holds the retained history (bounded by the longest window unless a column is unbounded)"
                    : "keys " + derived + " are intermediate columns: run on the relation as it stands before this stage (or on the expression that derives them)";
            queries.add(new AuditQuery(id, keys, e.getValue(), sql, note));
        }
        return queries;
    }

    /** Human readable dry-run report ({@code validate --expand}). */
    public String describe() {
        final StringBuilder sb = new StringBuilder();
        sb.append("feature plan ").append(hash).append('\n');
        sb.append("predictAt=").append(spec.predictAt.describe())
                .append(" time.field=").append(spec.timeField)
                .append(" columns=").append(getEmittedColumns().size()).append('/').append(columns.size())
                .append(" stages=").append(stages.size())
                .append(" shuffles=").append(getShuffleCount());
        final List<List<Integer>> waves = getWaves();
        sb.append(" waves=").append(waves.size()).append(" (dag shuffles~").append(getDagShuffleEstimate()).append(")\n");
        sb.append("-- stages (linear chain; deps = stages whose keyed/fit columns this one needs, wave = depth in that DAG)\n");
        for (int w = 0; w < waves.size(); w++) {
            for (final int i : waves.get(w)) sb.append("  ").append(stages.get(i).describe()).append(" wave=").append(w + 1).append('\n');
        }
        sb.append("-- columns\n");
        for (final OutputColumn c : columns) sb.append("  ").append(c.describe()).append('\n');
        if (!spec.output.roles.isEmpty() || spec.output.include != null) {
            sb.append("-- output contract (outputHash=").append(outputHash).append(")\n");
            for (final Map.Entry<String, String> e : spec.output.roles.entrySet()) {
                sb.append("  role ").append(e.getKey()).append('=').append(e.getValue()).append('\n');
            }
            if (spec.output.include != null) {
                sb.append("  include ").append(spec.output.include.size()).append(" names")
                        .append(spec.output.includeSource != null ? " from " + spec.output.includeSource : "")
                        .append(spec.output.includeHash != null ? " (hash " + spec.output.includeHash + ")" : "")
                        .append(" -> ").append(getEmittedColumns().size()).append(" columns emitted\n");
            }
        }
        if (!observedAtAudits.isEmpty()) {
            sb.append("-- observedAt audit (").append(spec.audit.observedAt).append("; counters feature/observedAt_*, quantiles in the run manifest)\n");
            for (final ObservedAtAudit a : observedAtAudits) sb.append("  ").append(a.describe()).append('\n');
        }
        final List<AuditQuery> audit = getAuditQueries();
        if (!audit.isEmpty()) {
            sb.append("-- audit (hot keys; {input} = the transform input relation)\n");
            for (final AuditQuery q : audit) sb.append("  ").append(q.describe()).append('\n');
        }
        if (!diagnostics.getMessages().isEmpty()) {
            sb.append("-- diagnostics\n");
            for (final Diagnostics.Message m : diagnostics.getMessages()) sb.append("  ").append(m).append('\n');
        }
        return sb.toString();
    }

    public JsonObject toJson() {
        final JsonObject json = new JsonObject();
        json.addProperty("hash", hash);
        json.addProperty("outputHash", outputHash);
        json.addProperty("predictAt", spec.predictAt.describe());
        json.addProperty("shuffles", getShuffleCount());
        final List<List<Integer>> waves = getWaves();
        json.addProperty("waves", waves.size());
        json.addProperty("dagShuffles", getDagShuffleEstimate());
        final JsonArray stageArray = new JsonArray();
        for (final Stage s : stages) {
            final JsonObject o = new JsonObject();
            o.addProperty("index", s.index);
            o.addProperty("kind", s.kind.name());
            o.addProperty("wave", getWave(s.index));
            final JsonArray deps = new JsonArray();
            s.dependsOn.forEach(deps::add);
            o.add("dependsOn", deps);
            final JsonArray keys = new JsonArray();
            s.keys.forEach(keys::add);
            o.add("keys", keys);
            final JsonArray blocks = new JsonArray();
            s.blocks.forEach(blocks::add);
            o.add("blocks", blocks);
            o.addProperty("columns", s.columns());
            stageArray.add(o);
        }
        json.add("stages", stageArray);
        final JsonArray columnArray = new JsonArray();
        for (final OutputColumn c : columns) {
            final JsonObject o = new JsonObject();
            o.addProperty("name", c.outputName);
            o.addProperty("canonical", c.canonicalName);
            o.addProperty("type", c.fieldType == null ? null : c.fieldType.getType().name());
            o.addProperty("intermediate", c.intermediate);
            final JsonArray inputs = new JsonArray();
            c.inputs.forEach(inputs::add);
            o.add("inputs", inputs);
            for (final Map.Entry<String, String> e : c.toOptions().entrySet()) {
                o.addProperty(e.getKey().substring("feature.".length()), e.getValue());
            }
            columnArray.add(o);
        }
        json.add("columns", columnArray);
        final JsonArray auditArray = new JsonArray();
        for (final AuditQuery q : getAuditQueries()) {
            final JsonObject o = new JsonObject();
            o.addProperty("id", q.id());
            final JsonArray keys = new JsonArray();
            q.keys().forEach(keys::add);
            o.add("keys", keys);
            final JsonArray st = new JsonArray();
            q.stages().forEach(st::add);
            o.add("stages", st);
            o.addProperty("sql", q.sql());
            o.addProperty("note", q.note());
            auditArray.add(o);
        }
        json.add("audit", auditArray);
        json.add("roles", rolesJson());
        if (spec.output.include != null) json.add("include", includeJson());
        final JsonArray observedAtArray = new JsonArray();
        for (final ObservedAtAudit a : observedAtAudits) {
            final JsonObject o = new JsonObject();
            o.addProperty("field", a.field());
            o.addProperty("source", a.source());
            o.addProperty("observedAtField", a.observedAtField());
            o.addProperty("availableAt", a.availableAt().describe());
            o.addProperty("present", a.present());
            observedAtArray.add(o);
        }
        json.add("observedAtAudit", observedAtArray);
        final JsonArray messages = new JsonArray();
        for (final Diagnostics.Message m : diagnostics.getMessages()) {
            final JsonObject o = new JsonObject();
            o.addProperty("level", m.level().name());
            o.addProperty("code", m.code());
            o.addProperty("location", m.location());
            o.addProperty("message", m.message());
            messages.add(o);
        }
        json.add("diagnostics", messages);
        return json;
    }

    static List<OutputColumn> mutableColumns() {
        return new ArrayList<>();
    }

    private JsonObject rolesJson() {
        final JsonObject roles = new JsonObject();
        final Map<String, String> resolved = getRoleColumns();
        for (final Map.Entry<String, String> e : spec.output.roles.entrySet()) {
            final JsonObject o = new JsonObject();
            o.addProperty("name", e.getValue());
            o.addProperty("column", resolved.get(e.getKey()));
            // a group / entity role naming a context / entity: the key columns the consumer groups by
            List<String> keys = null;
            for (final FeatureSpec.ContextDef c : spec.contexts) if (c.name().equals(e.getValue())) keys = c.keys();
            for (final FeatureSpec.EntityDef d : spec.entities) if (d.name().equals(e.getValue())) keys = d.keys();
            if (keys != null && !inputFields.containsKey(e.getValue())) {
                final JsonArray array = new JsonArray();
                keys.forEach(array::add);
                o.add("keys", array);
            }
            roles.add(e.getKey(), o);
        }
        return roles;
    }

    private JsonObject includeJson() {
        final JsonObject include = new JsonObject();
        if (spec.output.includeSource != null) include.addProperty("source", spec.output.includeSource);
        if (spec.output.includeHash != null) include.addProperty("hash", spec.output.includeHash);
        final JsonArray listed = new JsonArray();
        spec.output.include.forEach(listed::add);
        include.add("listed", listed);
        final Set<String> known = new HashSet<>();
        for (final OutputColumn c : columns) {
            if (c.intermediate) continue;
            known.add(c.canonicalName);
            known.add(c.outputName);
        }
        final JsonArray unknown = new JsonArray();
        for (final String name : spec.output.include) {
            final String base = name.endsWith("_isnull") ? name.substring(0, name.length() - "_isnull".length()) : name;
            if (!known.contains(name) && !known.contains(base)) unknown.add(name);
        }
        include.add("unknown", unknown);
        return include;
    }

    /**
     * The assembly-time manifest ({@code output.manifest}): the data contract of the output table — roles,
     * every emitted column with its lineage, the pass-through input fields with their contract, the hashes and
     * the include resolution — plus the full plan report. Everything here is decided at assembly, so a dry run
     * writes the same file as a run; execution-dependent facts (row counts, the observedAt audit) go to the
     * run manifest written at finalize.
     */
    public JsonObject toManifest(final List<Schema.Field> passThroughFields, final Map<String, String> artifacts) {
        final JsonObject manifest = new JsonObject();
        manifest.addProperty("version", 1);
        manifest.addProperty("planHash", hash);
        manifest.addProperty("outputHash", outputHash);
        manifest.addProperty("artifactVersion", getArtifactVersion());
        manifest.addProperty("createdAt", java.time.Instant.now().toString());
        manifest.addProperty("predictAt", spec.predictAt.describe());
        manifest.addProperty("timeField", spec.timeField);
        manifest.add("roles", rolesJson());
        if (spec.output.include != null) manifest.add("include", includeJson());
        final JsonObject output = new JsonObject();
        output.addProperty("prefix", spec.output.prefix);
        output.addProperty("passThrough", spec.output.passThrough);
        output.addProperty("nullPolicy", spec.output.nullPolicy.name());
        if (spec.output.groupBy != null) {
            output.addProperty("groupBy", spec.output.groupBy);
            output.addProperty("childName", spec.output.childName);
        }
        manifest.add("output", output);
        final Map<String, String> roleColumns = getRoleColumns();
        final JsonArray fields = new JsonArray();
        if (passThroughFields != null) {
            for (final Schema.Field f : passThroughFields) {
                final JsonObject o = new JsonObject();
                o.addProperty("name", f.getName());
                o.addProperty("type", f.getFieldType().getType().name());
                final SourceContract.FieldContract contract = inputFields.get(f.getName());
                if (contract != null) {
                    if (contract.getSourceName() != null && !contract.getSourceName().isEmpty()) o.addProperty("source", contract.getSourceName());
                    if (contract.getKind() != null) o.addProperty("kind", contract.getKind());
                    if (contract.getAvailableAt() != null) o.addProperty("availableAt", contract.getAvailableAt().describe());
                    o.addProperty("evidence", contract.isDeclared() ? "declared" : "measured");
                }
                final String role = roleOf(roleColumns, f.getName());
                if (role != null) o.addProperty("role", role);
                fields.add(o);
            }
        }
        manifest.add("fields", fields);
        final JsonArray columnArray = new JsonArray();
        for (final OutputColumn c : getEmittedColumns()) {
            final JsonObject o = new JsonObject();
            o.addProperty("name", c.outputName);
            o.addProperty("canonical", c.canonicalName);
            o.addProperty("type", c.fieldType == null ? null : c.fieldType.getType().name());
            // categorical for the consumer (a model's categorical feature list): text / enum / flags and crosses; counts and bin ids are ordinal
            o.addProperty("categorical", c.fieldType != null && (switch (c.fieldType.getType()) {
                case string, enumeration, bool -> true;
                default -> false;
            } || "cross".equals(c.operator)));
            o.addProperty("scope", c.scope.name());
            o.addProperty("block", c.block);
            o.addProperty("operator", c.operator);
            o.addProperty("availableAt", c.availableAt == null ? null : c.availableAt.describe());
            o.addProperty("computeAt", c.computeAt == null ? null : c.computeAt.describe());
            o.addProperty("status", c.status == null ? null : c.status.name());
            o.addProperty("placement", c.placement.name());
            o.addProperty("fitted", c.fitted);
            final JsonObject lineage = new JsonObject();
            final JsonArray derivedFrom = new JsonArray();
            c.derivedFrom.forEach(derivedFrom::add);
            lineage.add("derivedFrom", derivedFrom);
            final JsonArray sourceNames = new JsonArray();
            c.sources.forEach(sourceNames::add);
            lineage.add("sources", sourceNames);
            lineage.addProperty("evidence", c.declaredEvidence ? "declared" : "measured");
            final JsonArray inputs = new JsonArray();
            c.inputs.forEach(inputs::add);
            lineage.add("inputs", inputs);
            o.add("lineage", lineage);
            if (c.validFor != null) o.addProperty("validFor", c.validFor.toString());
            final String role = roleOf(roleColumns, c.outputName);
            if (role != null) o.addProperty("role", role);
            columnArray.add(o);
        }
        manifest.add("columns", columnArray);
        final JsonObject artifactJson = new JsonObject();
        if (artifacts != null) artifacts.forEach(artifactJson::addProperty);
        manifest.add("artifacts", artifactJson);
        // values read from external documents at assembly (temperatureFrom): which calibration produced this table
        final JsonArray externals = new JsonArray();
        for (final String external : spec.resolvedExternals) {
            final JsonObject o = new JsonObject();
            final int eq = external.indexOf('=');
            o.addProperty("location", external.substring(0, eq));
            final String[] parts = external.substring(eq + 1).split(":", 3);
            o.addProperty("source", parts[0]);
            o.addProperty("hash", parts.length > 1 ? parts[1] : null);
            o.addProperty("value", parts.length > 2 ? parts[2] : null);
            externals.add(o);
        }
        manifest.add("externals", externals);
        manifest.add("plan", toJson());
        return manifest;
    }

    private static String roleOf(final Map<String, String> roleColumns, final String column) {
        for (final Map.Entry<String, String> e : roleColumns.entrySet()) {
            if (e.getValue().equals(column)) return e.getKey();
        }
        return null;
    }

}
