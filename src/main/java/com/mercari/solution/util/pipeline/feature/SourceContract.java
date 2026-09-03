package com.mercari.solution.util.pipeline.feature;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The "facts" side of the DSL (docs/developer/feature-dsl.md §2): one declared source table with its fields,
 * event time, availability (world) and ingestion lag (system), mutability and snapshot archive.
 */
public class SourceContract implements Serializable {

    public enum Mutability { appendOnly, corrections }
    public enum Evidence { measured, declared }

    public record SnapshotOf(String source, String atExpression, AvailableAt at) implements Serializable {}

    public static class FieldContract implements Serializable {
        private String name;
        private Schema.FieldType type;
        private String typeName;
        private String description;
        private String availableAtExpression;
        private AvailableAt availableAt;
        private Duration ingestionLag;
        private AvailableAt effectiveAvailableAt;
        private String observedAtField;
        private Evidence evidence;
        private boolean allowDeclared;
        private String justification;
        private Duration validFor;
        private String kind;
        private String sourceName;

        public String getName() { return name; }
        public Schema.FieldType getType() { return type; }
        public String getTypeName() { return typeName; }
        public String getDescription() { return description; }
        public AvailableAt getAvailableAt() { return availableAt; }
        public Duration getIngestionLag() { return ingestionLag; }
        /** availableAt + ingestionLag, or the snapshot time when the source declares snapshotOf (§2.6.2). */
        public AvailableAt getEffectiveAvailableAt() { return effectiveAvailableAt; }
        public String getObservedAtField() { return observedAtField; }
        public Evidence getEvidence() { return evidence; }
        public boolean isAllowDeclared() { return allowDeclared; }
        public String getJustification() { return justification; }
        public Duration getValidFor() { return validFor; }
        public String getKind() { return kind; }
        public String getSourceName() { return sourceName; }

        public boolean isDeclared() {
            return evidence == Evidence.declared;
        }

        /** A field that exists in the input relation without a source declaration (e.g. time.field). */
        static FieldContract synthetic(final String name, final Schema.FieldType type, final AvailableAt availableAt) {
            final FieldContract f = new FieldContract();
            f.name = name;
            f.type = type;
            f.typeName = type.getType().name();
            f.availableAt = availableAt;
            f.effectiveAvailableAt = availableAt;
            f.ingestionLag = Duration.ZERO;
            f.evidence = Evidence.measured;
            f.sourceName = "";
            return f;
        }
    }

    private String name;
    private String description;
    private String eventTime;
    private AvailableAt availability;
    private Duration settlementLag;
    private Duration ingestionLag;
    private Mutability mutability;
    private Duration lateness;
    private Duration minInterval;
    private SnapshotOf snapshotOf;
    private List<String> keys;
    private final Map<String, FieldContract> fields = new LinkedHashMap<>();

    public String getName() { return name; }
    public String getDescription() { return description; }
    public String getEventTime() { return eventTime; }
    public Duration getSettlementLag() { return settlementLag; }
    public Duration getIngestionLag() { return ingestionLag; }
    public Mutability getMutability() { return mutability; }
    public Duration getLateness() { return lateness; }
    public Duration getMinInterval() { return minInterval; }
    public SnapshotOf getSnapshotOf() { return snapshotOf; }
    public List<String> getKeys() { return keys; }
    public Map<String, FieldContract> getFields() { return Collections.unmodifiableMap(fields); }
    public FieldContract getField(final String fieldName) { return fields.get(fieldName); }

    /** Training-value path derived from mutability × snapshotOf (§7 経路選択). */
    public enum TrainingPath { backfill, snapshotBackfill, logAndWait }

    public TrainingPath getTrainingPath() {
        if (mutability == Mutability.appendOnly) return TrainingPath.backfill;
        return snapshotOf != null ? TrainingPath.snapshotBackfill : TrainingPath.logAndWait;
    }

    /** Parses the whole sources document ({@code {version, sources: [...]}} or a bare list). */
    public static Map<String, SourceContract> parseAll(final JsonElement document, final Diagnostics diagnostics) {
        final Map<String, SourceContract> result = new LinkedHashMap<>();
        final JsonArray array;
        if (document == null || document.isJsonNull()) {
            diagnostics.error("sources.missing", "sources", "sources definition is empty");
            return result;
        } else if (document.isJsonArray()) {
            array = document.getAsJsonArray();
        } else if (document.isJsonObject() && document.getAsJsonObject().has("sources")) {
            array = document.getAsJsonObject().getAsJsonArray("sources");
        } else {
            diagnostics.error("sources.invalid", "sources", "sources must be a list or an object with a 'sources' list");
            return result;
        }
        for (final JsonElement element : array) {
            if (!element.isJsonObject()) {
                diagnostics.error("sources.invalid", "sources", "each source must be an object");
                continue;
            }
            final SourceContract source = parse(element.getAsJsonObject(), diagnostics);
            if (source.name == null) continue;
            if (result.containsKey(source.name)) {
                diagnostics.error("sources.duplicate", "sources." + source.name, "duplicate source name");
            }
            result.put(source.name, source);
        }
        return result;
    }

    static SourceContract parse(final JsonObject json, final Diagnostics diagnostics) {
        final SourceContract source = new SourceContract();
        source.name = Json.string(json, "name");
        final String loc = "sources." + (source.name == null ? "?" : source.name);
        if (source.name == null) {
            diagnostics.error("sources.name", loc, "source.name is required");
            return source;
        }
        source.description = Json.string(json, "description");
        source.eventTime = Json.string(json, "eventTime");
        if (source.eventTime == null) {
            diagnostics.error("sources.eventTime", loc, "source.eventTime is required");
        }
        source.settlementLag = Json.duration(json, "settlementLag", Duration.ZERO, diagnostics, loc);
        source.ingestionLag = Json.duration(json, "ingestionLag", Duration.ZERO, diagnostics, loc);
        source.lateness = Json.duration(json, "lateness", Duration.ZERO, diagnostics, loc);
        source.minInterval = Json.duration(json, "minInterval", null, diagnostics, loc);
        source.keys = Json.strings(json, "keys");

        final String mutability = Json.string(json, "mutability");
        if (mutability == null) {
            source.mutability = Mutability.appendOnly;
        } else {
            try {
                source.mutability = Mutability.valueOf(mutability);
            } catch (final IllegalArgumentException e) {
                diagnostics.error("sources.mutability", loc, "mutability must be appendOnly | corrections: " + mutability);
                source.mutability = Mutability.appendOnly;
            }
        }

        try {
            source.availability = AvailableAt.parse(Json.string(json, "availability"), source.settlementLag);
        } catch (final IllegalArgumentException e) {
            diagnostics.error("sources.availability", loc, e.getMessage());
            source.availability = AvailableAt.atEventTime();
        }

        if (json.has("snapshotOf") && json.get("snapshotOf").isJsonObject()) {
            final JsonObject s = json.getAsJsonObject("snapshotOf");
            final String snapshotSource = Json.string(s, "source");
            final String atExpression = Json.string(s, "at");
            if (snapshotSource == null || atExpression == null) {
                diagnostics.error("sources.snapshotOf", loc, "snapshotOf requires 'source' and 'at'");
            } else {
                try {
                    source.snapshotOf = new SnapshotOf(snapshotSource, atExpression, AvailableAt.parseTimeExpression(atExpression));
                } catch (final IllegalArgumentException e) {
                    diagnostics.error("sources.snapshotOf", loc, e.getMessage());
                }
            }
            if (source.mutability == Mutability.appendOnly) {
                diagnostics.warning("sources.snapshotOf.appendOnly", loc,
                        "snapshotOf on an appendOnly source has no effect (initial value equals final value)");
            }
        }

        if (!json.has("fields") || !json.get("fields").isJsonArray()) {
            diagnostics.error("sources.fields", loc, "source.fields is required");
            return source;
        }
        for (final JsonElement fe : json.getAsJsonArray("fields")) {
            if (!fe.isJsonObject()) continue;
            final FieldContract field = parseField(fe.getAsJsonObject(), source, diagnostics);
            if (field == null) continue;
            if (source.fields.containsKey(field.name)) {
                diagnostics.error("sources.fields.duplicate", loc + "." + field.name, "duplicate field name");
            }
            source.fields.put(field.name, field);
        }
        return source;
    }

    private static FieldContract parseField(final JsonObject json, final SourceContract source, final Diagnostics diagnostics) {
        final FieldContract field = new FieldContract();
        field.name = Json.string(json, "name");
        final String loc = "sources." + source.name + "." + (field.name == null ? "?" : field.name);
        if (field.name == null) {
            diagnostics.error("sources.fields.name", loc, "field.name is required");
            return null;
        }
        field.sourceName = source.name;
        field.typeName = Json.string(json, "type");
        if (field.typeName == null) {
            diagnostics.error("sources.fields.type", loc, "field.type is required");
        } else {
            try {
                field.type = Schema.FieldType.type(Schema.Type.of(field.typeName));
            } catch (final IllegalArgumentException e) {
                diagnostics.error("sources.fields.type", loc, "unsupported field type: " + field.typeName);
            }
        }
        field.description = Json.string(json, "description");
        field.kind = Json.string(json, "kind");
        field.validFor = Json.duration(json, "validFor", null, diagnostics, loc);
        field.ingestionLag = Json.duration(json, "ingestionLag", source.ingestionLag, diagnostics, loc);
        field.observedAtField = Json.string(json, "observedAtField");
        field.justification = Json.string(json, "justification");
        field.allowDeclared = Json.bool(json, "allowDeclared", false);

        field.availableAtExpression = Json.string(json, "availableAt");
        if (field.availableAtExpression == null) {
            field.availableAt = source.availability;
        } else {
            try {
                field.availableAt = AvailableAt.parse(field.availableAtExpression, source.settlementLag);
            } catch (final IllegalArgumentException e) {
                diagnostics.error("sources.fields.availableAt", loc, e.getMessage());
                field.availableAt = source.availability;
            }
        }

        final String evidence = Json.string(json, "evidence");
        if (evidence != null) {
            try {
                field.evidence = Evidence.valueOf(evidence);
            } catch (final IllegalArgumentException e) {
                diagnostics.error("sources.fields.evidence", loc, "evidence must be measured | declared: " + evidence);
            }
        }
        // Pre-event relative declarations (event_time - δ, atRowCreation) claim the value was known before
        // the event; that claim must be auditable (observedAtField) or explicitly marked as unverifiable.
        final boolean preEventClaim = !field.availableAt.isStatic()
                || (field.availableAt.getOffset().isNegative() && !field.availableAt.isPreEvent());
        if (field.evidence == null) {
            field.evidence = field.observedAtField != null ? Evidence.measured : (preEventClaim ? null : Evidence.measured);
        }
        if (field.evidence == Evidence.measured && field.observedAtField == null && preEventClaim) {
            diagnostics.error("sources.fields.observedAtField", loc,
                    "availableAt '" + field.availableAtExpression + "' is a pre-event claim: observedAtField is required, or declare evidence: declared");
            field.evidence = Evidence.declared;
        } else if (field.evidence == null) {
            diagnostics.error("sources.fields.observedAtField", loc,
                    "availableAt '" + field.availableAtExpression + "' is a pre-event claim: observedAtField is required, or declare evidence: declared");
            field.evidence = Evidence.declared;
        }
        if (field.evidence == Evidence.declared) {
            if (field.allowDeclared && field.justification == null) {
                diagnostics.error("sources.fields.allowDeclared", loc, "allowDeclared: true requires a justification");
            }
            if ("market".equals(field.kind) && !field.allowDeclared) {
                diagnostics.error("sources.fields.declaredMarket", loc,
                        "kind: market with evidence: declared is an error by default; add observedAtField, or allowDeclared: true with justification");
            } else if ("market".equals(field.kind)) {
                diagnostics.warning("sources.fields.declaredMarket", loc,
                        "kind: market with evidence: declared allowed by justification: " + field.justification);
            }
        }

        if (source.snapshotOf != null && (field.availableAt.isStatic() && !field.availableAt.getOffset().isPositive())) {
            // pre-event fields of a snapshot-archived corrections source are read as of the snapshot time
            field.effectiveAvailableAt = source.snapshotOf.at();
        } else {
            field.effectiveAvailableAt = field.availableAt.plus(field.ingestionLag);
        }
        return field;
    }

    /** Minimal Gson helpers shared by the compile layer. */
    static final class Json {
        private Json() {}

        static String string(final JsonObject json, final String key) {
            if (json == null || !json.has(key) || json.get(key).isJsonNull()) return null;
            final JsonElement e = json.get(key);
            return e.isJsonPrimitive() ? e.getAsString() : e.toString();
        }

        static boolean bool(final JsonObject json, final String key, final boolean defaultValue) {
            if (json == null || !json.has(key) || !json.get(key).isJsonPrimitive()) return defaultValue;
            final JsonElement e = json.get(key);
            return e.getAsJsonPrimitive().isBoolean() ? e.getAsBoolean() : Boolean.parseBoolean(e.getAsString());
        }

        static Integer integer(final JsonObject json, final String key) {
            if (json == null || !json.has(key) || !json.get(key).isJsonPrimitive()) return null;
            return json.get(key).getAsInt();
        }

        static List<String> strings(final JsonObject json, final String key) {
            final List<String> list = new ArrayList<>();
            if (json == null || !json.has(key) || json.get(key).isJsonNull()) return list;
            final JsonElement e = json.get(key);
            if (e.isJsonArray()) {
                for (final JsonElement v : e.getAsJsonArray()) {
                    if (v.isJsonPrimitive()) list.add(v.getAsString());
                }
            } else if (e.isJsonPrimitive()) {
                list.add(e.getAsString());
            }
            return list;
        }

        static Duration duration(final JsonObject json, final String key, final Duration defaultValue,
                                 final Diagnostics diagnostics, final String location) {
            final String text = string(json, key);
            if (text == null) return defaultValue;
            try {
                return Durations.parse(text);
            } catch (final RuntimeException e) {
                diagnostics.error("duration.invalid", location, key + ": invalid ISO8601 duration '" + text + "'");
                return defaultValue;
            }
        }
    }

}
