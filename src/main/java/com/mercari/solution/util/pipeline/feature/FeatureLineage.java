package com.mercari.solution.util.pipeline.feature;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.mercari.solution.module.Schema;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Column lineage of a feature transform output as a downstream transform (screen, evaluation) reads it: from
 * the output schema's {@code feature.*} field options when the feature transform is the direct upstream, or
 * from its manifest when the table came back through a sink. Carries the declared roles (group / label /
 * baseline / weight / time) so the data contract is declared once, on the feature side, and the lineage
 * selectors ({@code derivedFrom:} / {@code scope:} / {@code block:} / {@code evidence:} / {@code kind:}) a
 * column list may use.
 */
public final class FeatureLineage implements Serializable {

    public final Map<String, Entry> columns = new LinkedHashMap<>();
    /** role name → column (feature manifest {@code roles} / {@code feature.role} field options) */
    public final Map<String, String> roles = new LinkedHashMap<>();
    public String timeField;
    /** feature manifest identities (null when the lineage came from the schema) */
    public String planHash;
    public String outputHash;

    /** {@code kind} is the source field's origin tag (pass-through inputs only; a derived column carries its kinds in {@code derivedFrom}). */
    public record Entry(String scope, String block, Set<String> derivedFrom, String evidence, String kind) implements Serializable {
        public Entry(final String scope, final String block, final Set<String> derivedFrom, final String evidence) {
            this(scope, block, derivedFrom, evidence, null);
        }
    }

    /**
     * Lineage from the feature transform's output schema (the direct upstream): every field with
     * {@code feature.scope} — emitted columns and pass-through inputs alike — and the roles the fields carry
     * ({@code feature.role}; a {@code time} role is also the time field default).
     */
    public static FeatureLineage fromSchema(final Schema schema) {
        final FeatureLineage l = new FeatureLineage();
        if (schema == null) return l;
        for (final Schema.Field f : schema.getFields()) {
            final Map<String, String> o = f.getOptions();
            if (o == null || !o.containsKey("feature.scope")) continue;
            l.columns.put(f.getName(), new Entry(o.get("feature.scope"), o.get("feature.block"), split(o.get("feature.derivedFrom")), o.get("feature.evidence"), o.get("feature.kind")));
            final String role = o.get("feature.role");
            if (role != null) {
                l.roles.putIfAbsent(role, f.getName());
                if ("time".equals(role) && l.timeField == null) l.timeField = f.getName();
            }
        }
        return l;
    }

    private static Set<String> split(final String csv) {
        final Set<String> values = new LinkedHashSet<>();
        if (csv != null && !csv.isEmpty()) for (final String s : csv.split(",")) values.add(s.trim());
        return values;
    }

    /**
     * Reads a feature transform manifest (see {@code FeaturePlan.toManifest}).
     *
     * @throws IllegalArgumentException when the text is not a JSON object (the message names {@code parameter})
     */
    public static FeatureLineage fromManifest(final String json, final String parameter) {
        final FeatureLineage l = new FeatureLineage();
        final JsonObject m;
        try {
            m = JsonParser.parseString(json).getAsJsonObject();
        } catch (final JsonParseException | IllegalStateException e) {
            throw new IllegalArgumentException(parameter + " is not a JSON object (a local path that does not exist is read as literal content): " + e.getMessage());
        }
        l.timeField = string(m, "timeField");
        l.planHash = string(m, "planHash");
        l.outputHash = string(m, "outputHash");
        if (m.has("roles") && m.get("roles").isJsonObject()) {
            for (final Map.Entry<String, JsonElement> e : m.getAsJsonObject("roles").entrySet()) {
                if (!e.getValue().isJsonObject()) continue;
                final JsonObject r = e.getValue().getAsJsonObject();
                String column = string(r, "column");
                if (column == null && r.has("keys") && r.get("keys").isJsonArray()) {
                    final JsonArray keys = r.getAsJsonArray("keys");
                    if (keys.size() == 1) column = keys.get(0).getAsString();
                }
                if (column != null) l.roles.put(e.getKey(), column);
            }
        }
        // the pass-through input fields: scope input, derivedFrom = their kind (older manifests carry kind only)
        if (m.has("fields") && m.get("fields").isJsonArray()) {
            for (final JsonElement e : m.getAsJsonArray("fields")) {
                if (!e.isJsonObject()) continue;
                final JsonObject f = e.getAsJsonObject();
                final String name = string(f, "name");
                if (name == null) continue;
                final Set<String> derived = new LinkedHashSet<>();
                if (f.has("derivedFrom") && f.get("derivedFrom").isJsonArray()) {
                    for (final JsonElement d : f.getAsJsonArray("derivedFrom")) derived.add(d.getAsString());
                } else if (string(f, "kind") != null) {
                    derived.add(string(f, "kind"));
                }
                final String scope = string(f, "scope");
                l.columns.put(name, new Entry(scope == null ? "input" : scope, null, derived, string(f, "evidence"), string(f, "kind")));
            }
        }
        if (m.has("columns") && m.get("columns").isJsonArray()) {
            for (final JsonElement e : m.getAsJsonArray("columns")) {
                if (!e.isJsonObject()) continue;
                final JsonObject c = e.getAsJsonObject();
                final String name = string(c, "name");
                if (name == null) continue;
                final Set<String> derived = new LinkedHashSet<>();
                String evidence = null;
                if (c.has("lineage") && c.get("lineage").isJsonObject()) {
                    final JsonObject lineage = c.getAsJsonObject("lineage");
                    if (lineage.has("derivedFrom") && lineage.get("derivedFrom").isJsonArray()) {
                        for (final JsonElement d : lineage.getAsJsonArray("derivedFrom")) derived.add(d.getAsString());
                    }
                    evidence = string(lineage, "evidence");
                }
                l.columns.put(name, new Entry(string(c, "scope"), string(c, "block"), derived, evidence));
            }
        }
        return l;
    }

    /** Fills what this lineage lacks from {@code other} (schema lineage first, the manifest as a fallback). */
    public FeatureLineage merge(final FeatureLineage other) {
        if (other == null) return this;
        other.columns.forEach(columns::putIfAbsent);
        other.roles.forEach(roles::putIfAbsent);
        if (timeField == null) timeField = other.timeField;
        if (planHash == null) planHash = other.planHash;
        if (outputHash == null) outputHash = other.outputHash;
        return this;
    }

    /** Whether a column list entry is a lineage selector ({@code selector:value}) rather than a name glob. */
    public static boolean isSelector(final String pattern) {
        return pattern.indexOf(':') > 0;
    }

    /** Whether a lineage selector matches the column's entry (never, for a column without lineage). */
    public static boolean selectorMatches(final String pattern, final Entry entry) {
        if (entry == null) return false;
        final int colon = pattern.indexOf(':');
        final String selector = pattern.substring(0, colon);
        final String value = pattern.substring(colon + 1);
        return switch (selector) {
            case "derivedFrom" -> entry.derivedFrom() != null && entry.derivedFrom().contains(value);
            case "evidence" -> value.equals(entry.evidence());
            case "scope" -> value.equals(entry.scope());
            case "block" -> value.equals(entry.block());
            case "kind" -> value.equals(entry.kind());
            default -> false;
        };
    }

    /** The numeric field types the supervised transforms read as columns (booleans as 0 / 1). */
    public static boolean isNumeric(final Schema.Field f) {
        return switch (f.getFieldType().getType()) {
            case int32, int64, float32, float64, bool -> true;
            default -> false;
        };
    }

    private static String string(final JsonObject o, final String key) {
        if (o == null || !o.has(key) || o.get(key).isJsonNull()) return null;
        final JsonElement e = o.get(key);
        return e.isJsonPrimitive() ? e.getAsString() : null;
    }
}
