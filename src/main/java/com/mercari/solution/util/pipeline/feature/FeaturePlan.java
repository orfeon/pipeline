package com.mercari.solution.util.pipeline.feature;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Compiled execution plan of a feature spec: expanded columns with lineage, evaluation stages
 * (key changes = shuffles), the output schema, and diagnostics. Pure data — no Beam dependency —
 * so the same object backs {@code validate --expand}, the transform, and tests.
 */
public class FeaturePlan implements Serializable {

    public enum StageKind { row, context, sequence, population, fit, groupBy }

    /**
     * One evaluation stage: the columns evaluated under one key in one pass (work-feature-engine-beam.md §3.1).
     * Columns are scheduled by key affinity, so a stage may gather blocks from anywhere in the config, and
     * two stages may share a key when a dependency forces the split.
     */
    public record Stage(int index, StageKind kind, List<String> keys, List<String> blocks, List<String> columnNames) implements Serializable {
        public int columns() {
            return columnNames.size();
        }
        public String describe() {
            return "#" + index + " " + kind + (keys.isEmpty() ? "" : " key=" + keys) + " blocks=" + blocks + " columns=" + columns();
        }
    }

    /**
     * A data audit query derived from the plan (work-feature.md §7): hot-key row counts per keyed stage so
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

    FeaturePlan(final FeatureSpec spec,
                final Map<String, SourceContract> sources,
                final Map<String, SourceContract.FieldContract> inputFields,
                final List<OutputColumn> columns,
                final List<Stage> stages,
                final Schema outputSchema,
                final Diagnostics diagnostics,
                final String hash) {
        this.spec = spec;
        this.sources = sources;
        this.inputFields = inputFields;
        this.columns = columns;
        this.stages = stages;
        this.outputSchema = outputSchema;
        this.diagnostics = diagnostics;
        this.hash = hash;
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
            if (s.kind != StageKind.row && s.kind != StageKind.fit) count++;
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
            if (s.kind == StageKind.row || s.kind == StageKind.fit) continue;
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
                .append(" shuffles=").append(getShuffleCount()).append('\n');
        sb.append("-- stages\n");
        for (final Stage s : stages) sb.append("  ").append(s.describe()).append('\n');
        sb.append("-- columns\n");
        for (final OutputColumn c : columns) sb.append("  ").append(c.describe()).append('\n');
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
        json.addProperty("predictAt", spec.predictAt.describe());
        json.addProperty("shuffles", getShuffleCount());
        final JsonArray stageArray = new JsonArray();
        for (final Stage s : stages) {
            final JsonObject o = new JsonObject();
            o.addProperty("index", s.index);
            o.addProperty("kind", s.kind.name());
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

}
