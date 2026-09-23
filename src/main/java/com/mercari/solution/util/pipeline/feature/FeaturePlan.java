package com.mercari.solution.util.pipeline.feature;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Compiled execution plan of a feature spec: expanded columns with lineage, evaluation stages
 * (key changes = shuffles), the output schema, and diagnostics. Pure data — no Beam dependency —
 * so the same object backs {@code validate --expand}, the transform, and tests.
 */
public class FeaturePlan implements Serializable {

    /**
     * The type of a field as the sources contract spells it: a scalar type name, or {@code array<element>} for an array
     * (so a manifest / report type round-trips into {@code sources.fields.type}); null for an unknown type.
     */
    static String typeName(final Schema.FieldType type) {
        if (type == null) return null;
        return type.getType() == Schema.Type.array ? "array<" + typeName(type.getArrayValueType()) + ">" : type.getType().name();
    }

    /** {@code future}: a sequence replay in descending time — strictly-future windows (label columns). */
    public enum StageKind { row, context, sequence, population, fit, groupBy, future }

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
        /** Sequence / population / future stages replay each key's rows in time order (batch only; future: descending). */
        public boolean isReplay() {
            return kind == StageKind.sequence || kind == StageKind.population || kind == StageKind.future;
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
    private final List<MinIntervalAudit> minIntervalAudits;

    /**
     * An entity's {@code minInterval} that a column's {@code staticSafe} rests on (DSL spec §6.2 tier 2): the
     * declaration says two events of the entity are at least {@code minInterval} apart, which is what lets a window
     * read an outcome without the shift {@code shift} (the largest the declaration absorbed). Nothing verifies the
     * declaration at compile time, so the plan carries it as an audit: a query over the input, and a run-time counter
     * {@code feature/minInterval_<entity>_below} of the rows that follow the entity's previous event by less than it.
     */
    public record MinIntervalAudit(String entity, List<String> keys, java.time.Duration minInterval, java.time.Duration shift,
                                   List<String> columns) implements Serializable {
        public String describe() {
            return entity + " keys=" + keys + " minInterval=" + minInterval + " absorbs shift " + shift + " for " + columns.size()
                    + " column(s) [" + String.join(", ", columns.size() > 4 ? columns.subList(0, 4) : columns) + (columns.size() > 4 ? ", ..." : "") + "]";
        }
    }

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
                final List<ObservedAtAudit> observedAtAudits,
                final List<MinIntervalAudit> minIntervalAudits) {
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
        this.minIntervalAudits = minIntervalAudits;
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
    public List<MinIntervalAudit> getMinIntervalAudits() { return Collections.unmodifiableList(minIntervalAudits); }
    /** The audit entries the engine runs: observation column present and the audit not switched off. */
    public List<ObservedAtAudit> getRunnableObservedAtAudits() {
        if ("off".equals(spec.audit.observedAt)) return List.of();
        return observedAtAudits.stream().filter(ObservedAtAudit::present).toList();
    }
    /**
     * Role → the output column or pass-through field it names (never features for the consumer). Columns carry
     * the resolution the compiler stamped ({@link OutputColumn#getRole}); an input-field role is the field itself.
     * A group / entity role naming a context / entity has no entry (its keys are in the manifest).
     */
    public Map<String, String> getRoleColumns() {
        final Map<String, String> roles = new java.util.LinkedHashMap<>();
        for (final Map.Entry<String, String> e : spec.output.roles.entrySet()) {
            if (inputFields.containsKey(e.getValue())) roles.put(e.getKey(), e.getValue());
        }
        for (final OutputColumn c : columns) {
            if (!c.intermediate && c.role != null) roles.putIfAbsent(c.role, c.outputName);
        }
        return roles;
    }

    /** The role an input field carries ({@code output.roles} naming the field), null for a plain pass-through. */
    public String roleOfInput(final String field) {
        if (!inputFields.containsKey(field)) return null;
        for (final Map.Entry<String, String> e : spec.output.roles.entrySet()) if (e.getValue().equals(field)) return e.getKey();
        return null;
    }

    /**
     * Lineage of a pass-through input field as {@code feature.*} options — the counterpart of
     * {@link OutputColumn#toOptions} for the columns, and the one source of the output schema's field options
     * ({@code FeatureStages.createOutputSchema}) and the manifest's {@code fields} entries ({@link #toManifest}):
     * {@code scope = input}, the source contract ({@code kind}, {@code sources}, {@code availableAt},
     * {@code evidence}), {@code derivedFrom} = the kind plus whatever lineage the field already carried (an
     * upstream feature transform's column), and the role naming the field. Any other {@code feature.*} option the
     * field arrived with describes the upstream column, not this table, and is not part of the result.
     */
    public Map<String, String> passThroughOptions(final Schema.Field field) {
        final Map<String, String> options = new java.util.LinkedHashMap<>();
        options.put("feature.scope", "input");
        final Set<String> derivedFrom = new java.util.LinkedHashSet<>();
        final String upstream = field.getOptions() == null ? null : field.getOptions().get("feature.derivedFrom");
        if (upstream != null && !upstream.isEmpty()) for (final String s : upstream.split(",")) derivedFrom.add(s.trim());
        final SourceContract.FieldContract contract = inputFields.get(field.getName());
        if (contract != null) {
            if (contract.getKind() != null) {
                options.put("feature.kind", contract.getKind());
                derivedFrom.add(contract.getKind());
            }
            if (contract.getSourceName() != null && !contract.getSourceName().isEmpty()) options.put("feature.sources", contract.getSourceName());
            if (contract.getAvailableAt() != null) options.put("feature.availableAt", contract.getAvailableAt().describe());
            options.put("feature.evidence", contract.isDeclared() ? "declared" : "measured");
        }
        if (!derivedFrom.isEmpty()) options.put("feature.derivedFrom", String.join(",", derivedFrom));
        final String role = roleOfInput(field.getName());
        if (role != null) options.put("feature.role", role);
        return options;
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
     * the depth of the DAG's critical path — the barrier count the wave execution leaves. The row columns a stage
     * hosts for a consumer count too (the engine recomputes the evaluable ones on the wave input,
     * {@link #getPreludeColumns}); the deferred ones — output-only, {@link #getDeferredColumns} — do not.
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

    /**
     * Fields the base rows of engine wave {@code w} carry before its prelude: input fields, the columns of earlier
     * waves (a deferred column only once a prelude evaluated it) and the earlier preludes.
     */
    private Set<String> availableBefore(final int w) {
        final Set<String> fields = new HashSet<>(inputFields.keySet());
        for (final Map.Entry<String, Integer> e : engineWaveOfColumn().entrySet()) {
            if (e.getValue() < w && !columnsByName().get(e.getKey()).deferred) fields.add(e.getKey());
        }
        final List<List<OutputColumn>> preludes = preludes();
        for (int i = 0; i < w && i < preludes.size(); i++) for (final OutputColumn c : preludes.get(i)) fields.add(c.getCanonicalName());
        return fields;
    }

    /**
     * The row columns nobody else reads (engine doc §9.4.7): the linear chain evaluates them in the stage hosting
     * them (the last one), the wave engine on the first wave input that carries their inputs — after the wave that
     * completes those inputs, so the branches of one wave stay independent and a consumed intermediate (a
     * distribution map read by its readouts) can leave the rows right after. A reader of a lookup fit is never
     * deferred: its lambdas and artifact live in its fit stage.
     */
    public List<OutputColumn> getDeferredColumns() {
        final List<OutputColumn> deferred = new ArrayList<>();
        for (final OutputColumn c : columns) if (c.deferred) deferred.add(c);
        return deferred;
    }

    private transient List<List<OutputColumn>> preludes;

    /**
     * The row columns evaluated on the wave inputs, per engine wave: index {@code w} = on the input of wave
     * {@code w} before its fan-out, index {@code waves} = after the last wave, before the finalize. A row column
     * hosted by a stage of wave {@code w} is evaluated on the wave input when that input carries its inputs (and the
     * fields of its variance-components estimate, if any) — a branch would only recompute it —; a deferred column
     * on the first wave input that does. Expansion order (dependencies first): a prelude column is an input of the
     * next. Computed for every wave at once, because each wave's input includes the earlier preludes.
     */
    private List<List<OutputColumn>> preludes() {
        if (preludes == null) {
            final int n = getEngineWaves().size();
            final Map<String, Integer> waveOf = engineWaveOfColumn();
            final List<List<OutputColumn>> all = new ArrayList<>();
            final Set<String> available = new HashSet<>(inputFields.keySet());
            final Set<String> evaluated = new HashSet<>();
            for (int w = 0; w <= n; w++) {
                for (final Map.Entry<String, Integer> e : waveOf.entrySet()) {
                    if (e.getValue() == w - 1 && !columnsByName().get(e.getKey()).deferred) available.add(e.getKey());
                }
                final List<OutputColumn> prelude = new ArrayList<>();
                for (final OutputColumn c : columns) {
                    if (!FeaturePlanCompiler.isRowColumn(c) || evaluated.contains(c.canonicalName)) continue;
                    final Integer at = waveOf.get(c.canonicalName);
                    if (at == null || (!c.deferred && at != w)) continue;
                    if (available.containsAll(c.inputs) && vcFieldsAvailable(List.of(c), available)) {
                        prelude.add(c);
                        available.add(c.canonicalName);
                        evaluated.add(c.canonicalName);
                    }
                }
                all.add(prelude);
            }
            preludes = all;
        }
        return preludes;
    }

    /**
     * The row columns the engine evaluates on the input of engine wave {@code w} before its fan-out
     * ({@code Wave&lt;n&gt;_Rows}) — or, for {@code w} = the wave count, after the last wave ({@code Final_Rows}):
     * the hosted row columns the wave input can evaluate and the deferred columns it completes, in expansion
     * order (dependencies first). See {@link #preludes()}.
     */
    public List<OutputColumn> getPreludeColumns(final int w) {
        final List<List<OutputColumn>> preludes = preludes();
        return w < preludes.size() ? preludes.get(w) : List.of();
    }

    /**
     * The columns a branch of engine wave {@code w} evaluates for stage {@code s}: the stage's columns without the
     * deferred ones and without those a prelude up to wave {@code w} evaluated (they are on the wave input; a
     * branch reads only what it evaluates, so their inputs need not ride its shuffle).
     */
    public List<String> getBranchColumns(final Stage s, final int w) {
        final Set<String> done = new HashSet<>();
        final List<List<OutputColumn>> preludes = preludes();
        for (int i = 0; i <= w && i < preludes.size(); i++) for (final OutputColumn c : preludes.get(i)) done.add(c.getCanonicalName());
        final List<String> names = new ArrayList<>();
        for (final String name : s.columnNames) {
            final OutputColumn c = columnsByName().get(name);
            if (c == null || c.deferred || done.contains(name)) continue;
            names.add(name);
        }
        return names;
    }

    /** The fields every branch of engine wave {@code w} reads from its input: base fields plus the prelude. */
    public Set<String> getWaveInputFields(final int w) {
        final Set<String> fields = availableBefore(w);
        for (final OutputColumn c : getPreludeColumns(w)) fields.add(c.getCanonicalName());
        return fields;
    }

    // ---- liveness (engine doc §9.4.7) ----------------------------------------------------------------------
    // What a keyed stage's GroupByKey must carry. The engine projects the rows it groups to these sets (input fields
    // always ride; only computed columns are dropped), so the report's carry counts are what the shuffles move.

    private transient Map<Integer, Set<String>> stageReads;

    /** Computed columns read from the input rows when {@code names} are evaluated under {@code keys}. */
    private Set<String> readsOf(final List<String> keys, final Collection<String> names) {
        final Set<String> reads = new LinkedHashSet<>();
        for (final String k : keys) if (columnsByName().containsKey(k)) reads.add(k);
        final List<OutputColumn> cols = new ArrayList<>();
        for (final String name : names) {
            final OutputColumn c = columnsByName().get(name);
            if (c != null) cols.add(c);
        }
        for (final OutputColumn c : cols) {
            for (final String in : c.inputs) if (columnsByName().containsKey(in)) reads.add(in);
            for (final String in : c.pastInputs) if (columnsByName().containsKey(in)) reads.add(in);
            for (final Map.Entry<String, String> e : c.coordinates.entrySet()) mentioned(e.getKey(), e.getValue(), reads);
        }
        // the variance-components estimate of the stage reads the levels' keys / target / offset / fold keys
        for (final VarianceComponents.LevelSpec spec : VarianceComponents.specsOf(cols, columnsByName())) {
            for (final String k : spec.keys()) if (columnsByName().containsKey(k)) reads.add(k);
            if (spec.field() != null && columnsByName().containsKey(spec.field())) reads.add(spec.field());
            if (spec.offsetColumn() != null && columnsByName().containsKey(spec.offsetColumn())) reads.add(spec.offsetColumn());
            if (spec.foldKeys() != null) for (final String k : spec.foldKeys()) if (columnsByName().containsKey(k)) reads.add(k);
        }
        return reads;
    }

    /**
     * A coordinate that names a column keeps it (a fold key, a block field, a regressor, an offset's baseline —
     * whatever an evaluator reads by name rather than through the column's inputs): every token of the value that
     * is a column, and {@code __baseline_<token>} for {@code offset}. Conservative by construction: a token that
     * happens to spell a column name keeps one more column.
     */
    private void mentioned(final String key, final String value, final Set<String> reads) {
        if (value == null || value.isEmpty()) return;
        for (final String token : value.split("[,;|\\s\u0000]+")) {
            if (token.isEmpty()) continue;
            if (columnsByName().containsKey(token)) reads.add(token);
            if ("offset".equals(key) && columnsByName().containsKey("__baseline_" + token)) reads.add("__baseline_" + token);
        }
    }

    /**
     * Computed columns stage {@code s} reads from its input rows as the linear chain evaluates it (every hosted
     * column): its keys, its columns' self and past inputs, the fields of its variance-components estimate and any
     * column a coordinate names.
     */
    public Set<String> getStageReads(final Stage s) {
        if (stageReads == null) stageReads = new HashMap<>();
        return stageReads.computeIfAbsent(s.index, i -> Collections.unmodifiableSet(readsOf(s.keys, s.columnNames)));
    }

    /** Computed columns the finalize reads: the emitted columns, the {@code output.groupBy} keys and the parent fields. */
    public Set<String> getOutputReads() {
        final Set<String> reads = new LinkedHashSet<>();
        for (final OutputColumn c : getEmittedColumns()) reads.add(c.canonicalName);
        for (final FeatureSpec.ContextDef ctx : spec.contexts) {
            if (!ctx.name().equals(spec.output.groupBy)) continue;
            for (final String k : ctx.keys()) if (columnsByName().containsKey(k)) reads.add(k);
        }
        for (final String f : spec.output.parentFields) if (columnsByName().containsKey(f)) reads.add(f);
        return reads;
    }

    /** Computed columns the linear chain needs on the rows entering stage {@code k}: what stages {@code k} and later read, and the output. */
    public Set<String> getLiveBefore(final int k) {
        final Set<String> live = new LinkedHashSet<>(getOutputReads());
        for (final Stage s : stages) if (s.index >= k) live.addAll(getStageReads(s));
        return live;
    }

    /** Computed columns the wave engine needs after engine wave {@code w}: what the branches and preludes of later waves read, and the output. */
    public Set<String> getLiveAfterWave(final int w) {
        final Set<String> live = new LinkedHashSet<>(getOutputReads());
        final List<List<Stage>> waves = getEngineWaves();
        for (int i = w + 1; i < waves.size(); i++) for (final Stage s : waves.get(i)) live.addAll(readsOf(s.keys, getBranchColumns(s, i)));
        final List<List<OutputColumn>> preludes = preludes();
        for (int i = w + 1; i < preludes.size(); i++) live.addAll(readsOf(List.of(), preludes.get(i).stream().map(OutputColumn::getCanonicalName).toList()));
        return live;
    }

    /**
     * Computed columns the wave engine keeps on the rows entering the GroupByKey of keyed stage {@code s}: what its
     * branch reads — plus what lives on after the wave when the stage is its wave's only one (the rows go on from
     * it: a single-stage wave, a fold target); the groupBy finalize keeps what the output reads.
     */
    public Set<String> getWaveKeep(final Stage s) {
        final List<List<Stage>> waves = getEngineWaves();
        for (int w = 0; w < waves.size(); w++) {
            if (!waves.get(w).contains(s)) continue;
            final Set<String> keep = new LinkedHashSet<>(readsOf(s.keys, isFoldTarget(s) ? getFoldColumns(s, w) : getBranchColumns(s, w)));
            if (waves.get(w).size() == 1) keep.addAll(getLiveAfterWave(w));
            return keep;
        }
        return getOutputReads();
    }

    /**
     * The computed columns that ride the GroupByKey of keyed stage {@code s} in the wave engine: what it keeps
     * ({@link #getWaveKeep}) among the columns its input rows carry (earlier waves and preludes) — the report's
     * carry count; the engine's drop set is the complement of the keep set, so both agree on every column that exists.
     */
    public List<String> getCarriedColumns(final Stage s) {
        final List<List<Stage>> waves = getEngineWaves();
        Set<String> input = null;
        for (int w = 0; w < waves.size() && input == null; w++) if (waves.get(w).contains(s)) input = getWaveInputFields(w);
        if (input == null) input = getWaveInputFields(waves.size()); // the groupBy finalize: every column is on the rows
        final List<String> carried = new ArrayList<>();
        for (final String name : getWaveKeep(s)) if (input.contains(name)) carried.add(name);
        return carried;
    }

    /** The computed columns that ride the GroupByKey of keyed stage {@code s} in the linear chain: what stages {@code s} and later read among the columns of the earlier stages. */
    public List<String> getCarriedColumnsLinear(final Stage s) {
        final Set<String> produced = new HashSet<>();
        for (final Stage earlier : stages) if (earlier.index < s.index) produced.addAll(earlier.columnNames);
        final List<String> carried = new ArrayList<>();
        for (final String name : getLiveBefore(s.index)) if (produced.contains(name)) carried.add(name);
        return carried;
    }

    /** The map-typed columns among {@code names} (a wide row: the spill of a hot key grows with the row width). */
    public List<String> mapColumns(final Collection<String> names) {
        final List<String> maps = new ArrayList<>();
        for (final String name : names) {
            final OutputColumn c = columnsByName().get(name);
            if (c != null && c.fieldType != null && c.fieldType.getType() == Schema.Type.map) maps.add(name);
        }
        return maps;
    }

    /** Whether stage {@code s} is the single stage of a wave whose predecessor's merge rides its GroupByKey ({@link #getFoldTarget}). */
    public boolean isFoldTarget(final Stage s) {
        final List<List<Stage>> waves = getEngineWaves();
        for (int w = 1; w < waves.size(); w++) {
            if (waves.get(w).size() == 1 && waves.get(w).get(0).equals(s)) return waves.get(w - 1).size() >= 2 && s.equals(getFoldTarget(s, w - 1));
        }
        return false;
    }

    /**
     * The columns a fold target of engine wave {@code w} evaluates: its branch columns plus the wave's prelude —
     * its input is the merge itself (the base rows and the partials of the wave before), so the prelude cannot run
     * on a wave input first; the deferred columns of that prelude come last (nothing in the stage reads them).
     */
    public List<String> getFoldColumns(final Stage s, final int w) {
        final List<String> names = new ArrayList<>(getBranchColumns(s, w - 1));
        final List<String> deferred = new ArrayList<>();
        for (final OutputColumn c : getPreludeColumns(w)) {
            if (names.contains(c.canonicalName)) continue;
            (c.deferred ? deferred : names).add(c.canonicalName);
        }
        names.addAll(deferred);
        return names;
    }

    /**
     * Whether the merge of the last engine wave {@code w} rides the groupBy finalize's GroupByKey: the base rows
     * carry the groupBy keys, and the final prelude (the deferred columns that wave completes) needs no
     * variance-components estimate — the grouped finalize evaluates it on the merged rows, without side inputs.
     */
    public boolean foldsIntoGroupBy(final int w) {
        final List<List<Stage>> waves = getEngineWaves();
        final Stage groupBy = stages.stream().filter(s -> s.kind == StageKind.groupBy).findFirst().orElse(null);
        return groupBy != null && w + 1 == waves.size() && keysAvailable(groupBy.keys, w)
                && VarianceComponents.specsOf(getPreludeColumns(waves.size()), columnsByName()).isEmpty();
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
            if (foldsIntoGroupBy(w)) continue; // rides the finalize
            count++; // the wave's row-id merge
        }
        return count;
    }

    /**
     * Audit queries over the transform input: the hot keys — one per distinct key set of the keyed stages
     * (context / sequence / population / groupBy), plus the row count for a global (single key) level — and one per
     * entity whose declared {@code minInterval} the plan relies on. Keys that are not input fields are intermediate
     * columns; their query must be run on the relation as it stands before that stage.
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
        // a declared minInterval is not checked at compile time: this query counts the input's events that contradict it
        for (final MinIntervalAudit a : minIntervalAudits) {
            final String keyList = String.join(", ", a.keys());
            final String notNull = a.keys().stream().map(k -> k + " IS NOT NULL").collect(java.util.stream.Collectors.joining(" AND "));
            final String time = spec.timeField;
            final String previous = "LAG(" + time + ") OVER (PARTITION BY " + keyList + " ORDER BY " + time + ")";
            // whole seconds, rounded up so no gap below the declaration escapes the comparison
            final long limit = (a.minInterval().toMillis() + 999) / 1000;
            final String sql = "SELECT COUNT(1) AS rows_below_min_interval, MIN(gap_seconds) AS min_gap_seconds FROM ("
                    + "SELECT " + gapSeconds(time, previous) + " AS gap_seconds"
                    + " FROM {input} WHERE " + notNull + ") WHERE gap_seconds IS NOT NULL AND gap_seconds > 0 AND gap_seconds < " + limit;
            final List<String> derived = a.keys().stream().filter(k -> !inputFields.containsKey(k)).toList();
            queries.add(new AuditQuery("audit" + (n++), a.keys(), List.of("entity " + a.entity()), sql,
                    "entities." + a.entity() + ".minInterval " + a.minInterval() + " is a declaration the plan relies on (it lets " + a.columns().size()
                            + " column(s) read an outcome without a window shift of up to " + a.shift() + "): every row counted here follows the entity's"
                            + " previous event by less than it and may read an outcome that was not yet known - the run counts the same events as"
                            + " feature/minInterval_" + a.entity() + "_below (BigQuery form; a gap of zero - rows sharing a timestamp, which never see"
                            + " each other - is not a violation, and where several rows share the later timestamp this query counts one of them and the"
                            + " counter counts each)"
                            + (derived.isEmpty() ? "" : "; keys " + derived + " are intermediate columns: run on the relation that derives them")));
        }
        return queries;
    }

    /**
     * The gap in seconds between two values of the time field, in the BigQuery function its declared type takes:
     * {@code TIMESTAMP_DIFF} only accepts TIMESTAMPs, and {@code time.field} may be a date or a datetime.
     */
    private String gapSeconds(final String time, final String previous) {
        final SourceContract.FieldContract contract = inputFields.get(time);
        final Schema.FieldType type = contract == null ? null : contract.getType();
        return switch (type == null ? Schema.Type.timestamp : type.getType()) {
            case date -> "DATE_DIFF(" + time + ", " + previous + ", DAY) * 86400";
            case datetime -> "DATETIME_DIFF(" + time + ", " + previous + ", SECOND)";
            default -> "TIMESTAMP_DIFF(" + time + ", " + previous + ", SECOND)";
        };
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
        final List<OutputColumn> deferred = getDeferredColumns();
        if (getShuffleCount() > 0) {
            sb.append("-- carry (computed columns on the rows a keyed stage groups: wave engine / linear chain; maps named")
                    .append(deferred.isEmpty() ? "" : "; " + deferred.size() + " deferred row column(s) evaluated on the wave inputs").append(")\n");
            for (final Stage s : stages) {
                if (!s.isKeyed()) continue;
                final List<String> carried = getCarriedColumns(s);
                sb.append("  #").append(s.index).append(' ').append(s.kind).append(s.keys.isEmpty() ? "" : " key=" + s.keys)
                        .append(": ").append(carried.size()).append(" / ").append(getCarriedColumnsLinear(s).size()).append(" columns");
                final List<String> maps = mapColumns(carried);
                if (!maps.isEmpty()) sb.append("; maps: ").append(maps);
                sb.append('\n');
            }
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
        if (!minIntervalAudits.isEmpty()) {
            sb.append("-- minInterval audit (declared, not verified; counters feature/minInterval_<entity>_below, query in -- audit)\n");
            for (final MinIntervalAudit a : minIntervalAudits) sb.append("  ").append(a.describe()).append('\n');
        }
        if (!observedAtAudits.isEmpty()) {
            sb.append("-- observedAt audit (").append(spec.audit.observedAt).append("; counters feature/observedAt_*, quantiles in the run manifest)\n");
            for (final ObservedAtAudit a : observedAtAudits) sb.append("  ").append(a.describe()).append('\n');
        }
        final List<AuditQuery> audit = getAuditQueries();
        if (!audit.isEmpty()) {
            sb.append("-- audit (hot keys and declared intervals; {input} = the transform input relation)\n");
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
            if (s.isKeyed()) {
                // what the GroupByKey carries (engine doc §9.4.7): computed columns kept on the grouped rows
                final List<String> carried = getCarriedColumns(s);
                o.addProperty("carry", carried.size());
                o.addProperty("carryLinear", getCarriedColumnsLinear(s).size());
                final List<String> maps = mapColumns(carried);
                if (!maps.isEmpty()) {
                    final JsonArray array = new JsonArray();
                    maps.forEach(array::add);
                    o.add("carryMaps", array);
                }
            }
            stageArray.add(o);
        }
        json.add("stages", stageArray);
        final JsonArray columnArray = new JsonArray();
        for (final OutputColumn c : columns) {
            final JsonObject o = new JsonObject();
            o.addProperty("name", c.outputName);
            o.addProperty("canonical", c.canonicalName);
            o.addProperty("type", typeName(c.fieldType));
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
        final JsonArray minIntervalArray = new JsonArray();
        for (final MinIntervalAudit a : minIntervalAudits) {
            final JsonObject o = new JsonObject();
            o.addProperty("entity", a.entity());
            final JsonArray keys = new JsonArray();
            a.keys().forEach(keys::add);
            o.add("keys", keys);
            o.addProperty("minInterval", a.minInterval().toString());
            o.addProperty("shift", a.shift().toString());
            final JsonArray columns = new JsonArray();
            a.columns().forEach(columns::add);
            o.add("columns", columns);
            o.addProperty("counter", "feature/minInterval_" + a.entity() + "_below");
            minIntervalArray.add(o);
        }
        json.add("minIntervalAudit", minIntervalArray);
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
        final JsonArray fields = new JsonArray();
        if (passThroughFields != null) {
            for (final Schema.Field f : passThroughFields) {
                final JsonObject o = new JsonObject();
                o.addProperty("name", f.getName());
                o.addProperty("type", typeName(f.getFieldType()));
                // the same facts as the output schema's field options (scope input: the selector vocabulary of the
                // columns, so a consumer excludes pass-through inputs by scope / kind / derivedFrom)
                final Map<String, String> options = passThroughOptions(f);
                o.addProperty("scope", options.get("feature.scope"));
                if (options.containsKey("feature.sources")) o.addProperty("source", options.get("feature.sources"));
                if (options.containsKey("feature.kind")) o.addProperty("kind", options.get("feature.kind"));
                final JsonArray derivedFrom = new JsonArray();
                if (options.containsKey("feature.derivedFrom")) for (final String d : options.get("feature.derivedFrom").split(",")) derivedFrom.add(d);
                o.add("derivedFrom", derivedFrom);
                if (options.containsKey("feature.availableAt")) o.addProperty("availableAt", options.get("feature.availableAt"));
                if (options.containsKey("feature.evidence")) o.addProperty("evidence", options.get("feature.evidence"));
                if (options.containsKey("feature.role")) o.addProperty("role", options.get("feature.role"));
                fields.add(o);
            }
        }
        manifest.add("fields", fields);
        final JsonArray columnArray = new JsonArray();
        for (final OutputColumn c : getEmittedColumns()) {
            final JsonObject o = new JsonObject();
            o.addProperty("name", c.outputName);
            o.addProperty("canonical", c.canonicalName);
            o.addProperty("type", typeName(c.fieldType));
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
            if (c.role != null) o.addProperty("role", c.role);
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
            // source:hash:value — the source is a URI (may contain ':'); hash and value never do, so split from the end
            final String rest = external.substring(eq + 1);
            final int valueSep = rest.lastIndexOf(':');
            final int hashSep = valueSep < 0 ? -1 : rest.lastIndexOf(':', valueSep - 1);
            o.addProperty("source", hashSep < 0 ? rest : rest.substring(0, hashSep));
            o.addProperty("hash", hashSep < 0 ? null : rest.substring(hashSep + 1, valueSep));
            o.addProperty("value", valueSep < 0 ? null : rest.substring(valueSep + 1));
            externals.add(o);
        }
        manifest.add("externals", externals);
        manifest.add("plan", toJson());
        return manifest;
    }

}
