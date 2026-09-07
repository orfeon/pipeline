package com.mercari.solution.util.pipeline.process;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.Filter;

import java.io.Serializable;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Parameters of the {@code process} transform: the event-log mapping (case id, activity, timestamp,
 * resource, case attributes), the activity derivation rules, the DFG / performance options, the Declare
 * constraints and the engine knobs. Parsed from the module's JSON parameters and validated against the input
 * schema at assembly time, so a misconfiguration fails the pipeline before any data is read.
 */
public class ProcessSpec implements Serializable {

    /** Synthetic node names of the start / end edges of the directly-follows graph. */
    public static final String START_NODE = "__start__";
    public static final String END_NODE = "__end__";

    public enum Unit {
        millis(1D), seconds(1_000D), minutes(60_000D), hours(3_600_000D), days(86_400_000D);
        private final double millisPer;
        Unit(final double millisPer) { this.millisPer = millisPer; }
        public double fromMillis(final long millis) { return millis / millisPer; }
    }

    public enum Unmatched { drop, failure }

    /** Declare constraint templates evaluated per case ({@link CaseReplay}). */
    public enum ConstraintType {
        /** activity occurs at least {@code min} times (default 1) */
        existence,
        /** activity occurs at most {@code max} times (default 0) */
        absence,
        /** activity occurs exactly {@code count} times */
        exactly,
        /** the first activity of the case is {@code activity} */
        init,
        /** the last activity of the case is {@code activity} */
        end,
        /** every occurrence of {@code activity} is eventually followed by {@code target} */
        response,
        /** every occurrence of {@code target} is preceded by some earlier {@code activity} */
        precedence,
        /** response and precedence together */
        succession,
        /** every occurrence of {@code activity} is immediately followed by {@code target} */
        chainResponse,
        /** every occurrence of {@code target} is immediately preceded by {@code activity} */
        chainPrecedence,
        /** {@code activity} and {@code target} never both occur in a case */
        notCoexistence,
        /** the case (or, with {@code activity} + {@code target}, the first activity → last target span) lasts at most {@code maxDuration} */
        duration
    }

    /** An activity derivation rule: the first rule whose filter accepts the event names its activity. */
    public static class ActivityRule implements Serializable {
        public String name;
        /** filter condition (JSON condition or SQL-like text) serialised as JSON text; null = matches every event */
        public String filter;
    }

    public static class Constraint implements Serializable {
        public String name;
        public ConstraintType type;
        public String activity;
        public String target;
        public Integer min;
        public Integer max;
        public Integer count;
        public String maxDuration;
        transient Duration maxDurationValue;

        public Duration maxDurationValue() {
            if (maxDurationValue == null && maxDuration != null) maxDurationValue = Duration.parse(maxDuration);
            return maxDurationValue;
        }
    }

    public List<String> caseId = new ArrayList<>();
    public String activity;
    public List<ActivityRule> activities = new ArrayList<>();
    public String timestamp;
    public String sequence;
    public String resource;
    public List<String> attributes = new ArrayList<>();

    public long dfgMinFrequency = 1;
    public boolean dfgStartEnd = true;
    public Unit unit = Unit.seconds;
    public List<Constraint> constraints = new ArrayList<>();

    public int maxTraceLength = 1000;
    public Integer spillMemoryMB;
    public String spillDirectory;
    public boolean spillCompress = false;
    public Unmatched unmatched = Unmatched.drop;

    /** The case attribute fields resolved against the input schema by {@link #validate} (shipped to the workers). */
    List<Schema.Field> caseAttributeFields = new ArrayList<>();

    public static ProcessSpec parse(final JsonObject json) {
        final ProcessSpec spec = new ProcessSpec();
        if (json == null) return spec;
        spec.caseId = stringList(json.get("caseId"), "caseId");
        spec.activity = string(json.get("activity"));
        if (json.has("activities") && !json.get("activities").isJsonNull()) {
            final JsonElement e = json.get("activities");
            if (!e.isJsonArray()) throw new IllegalArgumentException("activities must be an array of {name, filter}");
            for (final JsonElement r : e.getAsJsonArray()) {
                if (!r.isJsonObject()) throw new IllegalArgumentException("activities entries must be objects with name and filter: " + r);
                final JsonObject o = r.getAsJsonObject();
                final ActivityRule rule = new ActivityRule();
                rule.name = string(o.get("name"));
                final JsonElement filter = o.get("filter");
                rule.filter = filter == null || filter.isJsonNull() ? null : filter.toString();
                spec.activities.add(rule);
            }
        }
        spec.timestamp = string(json.get("timestamp"));
        spec.sequence = string(json.get("sequence"));
        spec.resource = string(json.get("resource"));
        spec.attributes = stringList(json.get("attributes"), "attributes");
        if (json.has("dfg") && json.get("dfg").isJsonObject()) {
            final JsonObject dfg = json.getAsJsonObject("dfg");
            if (dfg.has("minFrequency") && !dfg.get("minFrequency").isJsonNull()) spec.dfgMinFrequency = dfg.get("minFrequency").getAsLong();
            if (dfg.has("startEnd") && !dfg.get("startEnd").isJsonNull()) spec.dfgStartEnd = dfg.get("startEnd").getAsBoolean();
        }
        if (json.has("performance") && json.get("performance").isJsonObject()) {
            final JsonObject p = json.getAsJsonObject("performance");
            final String unit = string(p.get("unit"));
            if (unit != null) {
                try {
                    spec.unit = Unit.valueOf(unit.toLowerCase(Locale.ROOT));
                } catch (final IllegalArgumentException e) {
                    throw new IllegalArgumentException("performance.unit must be one of millis, seconds, minutes, hours, days: " + unit);
                }
            }
        }
        if (json.has("constraints") && !json.get("constraints").isJsonNull()) {
            final JsonElement e = json.get("constraints");
            if (!e.isJsonArray()) throw new IllegalArgumentException("constraints must be an array");
            for (final JsonElement c : e.getAsJsonArray()) {
                if (!c.isJsonObject()) throw new IllegalArgumentException("constraints entries must be objects: " + c);
                final JsonObject o = c.getAsJsonObject();
                final Constraint constraint = new Constraint();
                constraint.name = string(o.get("name"));
                final String type = string(o.get("type"));
                if (type == null) throw new IllegalArgumentException("constraints[].type is required: " + o);
                try {
                    constraint.type = ConstraintType.valueOf(type);
                } catch (final IllegalArgumentException ex) {
                    throw new IllegalArgumentException("constraints[].type '" + type + "' is not supported (existence, absence, exactly, init, end, response, precedence, succession, chainResponse, chainPrecedence, notCoexistence, duration)");
                }
                constraint.activity = string(o.get("activity"));
                constraint.target = string(o.get("target"));
                constraint.min = integer(o.get("min"));
                constraint.max = integer(o.get("max"));
                constraint.count = integer(o.get("count"));
                constraint.maxDuration = string(o.get("maxDuration"));
                spec.constraints.add(constraint);
            }
        }
        if (json.has("engine") && json.get("engine").isJsonObject()) {
            final JsonObject engine = json.getAsJsonObject("engine");
            if (engine.has("maxTraceLength") && !engine.get("maxTraceLength").isJsonNull()) spec.maxTraceLength = engine.get("maxTraceLength").getAsInt();
            if (engine.has("spillMemoryMB") && !engine.get("spillMemoryMB").isJsonNull()) spec.spillMemoryMB = engine.get("spillMemoryMB").getAsInt();
            spec.spillDirectory = string(engine.get("spillDirectory"));
            if (engine.has("spillCompress") && !engine.get("spillCompress").isJsonNull()) spec.spillCompress = engine.get("spillCompress").getAsBoolean();
            final String unmatched = string(engine.get("unmatched"));
            if (unmatched != null) {
                try {
                    spec.unmatched = Unmatched.valueOf(unmatched);
                } catch (final IllegalArgumentException ex) {
                    throw new IllegalArgumentException("engine.unmatched must be drop or failure: " + unmatched);
                }
            }
        }
        return spec;
    }

    /** Validates the spec against the input schema; every problem is reported at once. */
    public List<String> validate(final Schema inputSchema) {
        final List<String> errors = new ArrayList<>();
        if (caseId.isEmpty()) errors.add("caseId is required (the field, or list of fields, identifying a case)");
        for (final String f : caseId) {
            if (inputSchema != null && !inputSchema.hasField(f)) errors.add("caseId field '" + f + "' is not in the input schema");
        }
        if (activity == null && activities.isEmpty()) {
            errors.add("activity (a field) or activities (derivation rules) is required");
        }
        if (activity != null && !activities.isEmpty()) {
            errors.add("activity and activities are exclusive: name the activity field, or derive it with rules");
        }
        if (activity != null && inputSchema != null && !inputSchema.hasField(activity)) {
            errors.add("activity field '" + activity + "' is not in the input schema");
        }
        final Set<String> ruleNames = new HashSet<>();
        for (int i = 0; i < activities.size(); i++) {
            final ActivityRule rule = activities.get(i);
            if (rule.name == null || rule.name.isBlank()) errors.add("activities[" + i + "].name is required");
            else if (!ruleNames.add(rule.name)) errors.add("activities[" + i + "].name '" + rule.name + "' is declared twice");
            if (rule.name != null && (START_NODE.equals(rule.name) || END_NODE.equals(rule.name))) {
                errors.add("activities[" + i + "].name '" + rule.name + "' is reserved for the DFG start / end nodes");
            }
            if (rule.filter != null) {
                // parse the condition here so that a broken rule fails the assembly, not the workers' setup
                try {
                    final Filter.ConditionNode node = Filter.parse(rule.filter);
                    if (inputSchema != null) {
                        for (final String m : node.validate(inputSchema.getFields())) errors.add("activities[" + i + "].filter is illegal: " + m);
                    }
                } catch (final RuntimeException e) {
                    errors.add("activities[" + i + "].filter could not be parsed: " + e.getMessage());
                }
            }
        }
        for (final String f : List.of("timestamp", "sequence", "resource")) {
            final String name = switch (f) { case "timestamp" -> timestamp; case "sequence" -> sequence; default -> resource; };
            if (name != null && inputSchema != null && !inputSchema.hasField(name)) errors.add(f + " field '" + name + "' is not in the input schema");
        }
        if (timestamp != null && inputSchema != null && inputSchema.hasField(timestamp)) {
            final Schema.Type type = inputSchema.getField(timestamp).getFieldType().getType();
            switch (type) {
                case timestamp, date, string, int64 -> {}
                default -> errors.add("timestamp field '" + timestamp + "' must be a timestamp / date / string / int64 (epoch micros) field, found " + type);
            }
        }
        if (sequence != null && inputSchema != null && inputSchema.hasField(sequence)) {
            final Schema.Type type = inputSchema.getField(sequence).getFieldType().getType();
            switch (type) {
                case int8, int16, int32, int64, float32, float64, string, timestamp -> {}
                default -> errors.add("sequence field '" + sequence + "' must be a numeric / string / timestamp field, found " + type);
            }
        }
        caseAttributeFields = new ArrayList<>();
        final Set<String> seen = new LinkedHashSet<>();
        for (final String a : attributes) {
            if (!seen.add(a)) {
                errors.add("attributes lists '" + a + "' twice");
                continue;
            }
            if (inputSchema == null) continue;
            if (!inputSchema.hasField(a)) {
                errors.add("attributes field '" + a + "' is not in the input schema");
                continue;
            }
            final Schema.Field field = inputSchema.getField(a);
            switch (field.getFieldType().getType()) {
                case map, element, array, matrix, bytes, geography -> errors.add("attributes field '" + a + "' must be a primitive field, found " + field.getFieldType().getType());
                default -> caseAttributeFields.add(Schema.Field.of(a, field.getFieldType().withNullable(true)));
            }
        }
        for (final String reserved : ProcessStages.CASE_FIELDS) {
            if (seen.contains(reserved)) errors.add("attributes field '" + reserved + "' collides with a cases output column; rename it upstream");
        }
        for (final String reserved : ProcessStages.EVENT_FIELDS) {
            if (seen.contains(reserved) && !ProcessStages.CASE_FIELDS.contains(reserved)) errors.add("attributes field '" + reserved + "' collides with a projected event column; rename it upstream");
        }
        if (dfgMinFrequency < 1) errors.add("dfg.minFrequency must be at least 1");
        if (maxTraceLength < 1) errors.add("engine.maxTraceLength must be at least 1");
        if (spillMemoryMB != null && spillMemoryMB < 1) errors.add("engine.spillMemoryMB must be at least 1");
        final Set<String> constraintNames = new HashSet<>();
        for (int i = 0; i < constraints.size(); i++) {
            final Constraint c = constraints.get(i);
            final String prefix = "constraints[" + i + "]";
            if (c.name == null || c.name.isBlank()) {
                c.name = defaultConstraintName(c);
            }
            if (!constraintNames.add(c.name)) errors.add(prefix + ".name '" + c.name + "' is declared twice");
            switch (c.type) {
                case existence, absence, exactly, init, end -> {
                    if (c.activity == null) errors.add(prefix + " (" + c.type + ") requires activity");
                    if (c.type == ConstraintType.exactly && c.count == null) errors.add(prefix + " (exactly) requires count");
                    if (c.min != null && c.min < 0) errors.add(prefix + ".min must not be negative");
                    if (c.max != null && c.max < 0) errors.add(prefix + ".max must not be negative");
                }
                case response, precedence, succession, chainResponse, chainPrecedence, notCoexistence -> {
                    if (c.activity == null || c.target == null) errors.add(prefix + " (" + c.type + ") requires activity and target");
                }
                case duration -> {
                    if (c.maxDuration == null) {
                        errors.add(prefix + " (duration) requires maxDuration (ISO-8601, e.g. PT48H)");
                    } else {
                        try {
                            c.maxDurationValue();
                        } catch (final DateTimeParseException e) {
                            errors.add(prefix + ".maxDuration '" + c.maxDuration + "' is not an ISO-8601 duration");
                        }
                    }
                    if ((c.activity == null) != (c.target == null)) errors.add(prefix + " (duration) takes activity and target together, or neither (whole case)");
                }
            }
        }
        return errors;
    }

    static String defaultConstraintName(final Constraint c) {
        final StringBuilder sb = new StringBuilder(c.type.name());
        if (c.activity != null) sb.append('(').append(c.activity);
        if (c.target != null) sb.append(", ").append(c.target);
        if (c.activity != null) sb.append(')');
        if (c.type == ConstraintType.duration && c.maxDuration != null) sb.append(" <= ").append(c.maxDuration);
        return sb.toString();
    }

    public List<Schema.Field> caseAttributeFields() {
        return caseAttributeFields;
    }

    /** A one-screen summary for the assembly log. */
    public String describe() {
        final StringBuilder sb = new StringBuilder();
        sb.append("case: ").append(String.join(", ", caseId));
        sb.append(" | activity: ").append(activity != null ? activity : activities.size() + " rules");
        sb.append(" | timestamp: ").append(timestamp != null ? timestamp : "<event time>");
        if (sequence != null) sb.append(" | sequence: ").append(sequence);
        if (resource != null) sb.append(" | resource: ").append(resource);
        if (!attributes.isEmpty()) sb.append(" | attributes: ").append(String.join(", ", attributes));
        sb.append(" | dfg.minFrequency: ").append(dfgMinFrequency).append(", dfg.startEnd: ").append(dfgStartEnd);
        sb.append(" | unit: ").append(unit);
        if (!constraints.isEmpty()) {
            sb.append(" | constraints: ");
            final List<String> names = new ArrayList<>();
            for (final Constraint c : constraints) names.add(c.name);
            sb.append(String.join(", ", names));
        }
        return sb.toString();
    }

    private static String string(final JsonElement e) {
        if (e == null || e.isJsonNull()) return null;
        if (!e.isJsonPrimitive()) throw new IllegalArgumentException("expected a string, found " + e);
        return e.getAsString();
    }

    private static Integer integer(final JsonElement e) {
        if (e == null || e.isJsonNull()) return null;
        if (!e.isJsonPrimitive() || !e.getAsJsonPrimitive().isNumber()) throw new IllegalArgumentException("expected an integer, found " + e);
        return e.getAsInt();
    }

    private static List<String> stringList(final JsonElement e, final String name) {
        final List<String> list = new ArrayList<>();
        if (e == null || e.isJsonNull()) return list;
        if (e.isJsonPrimitive()) {
            list.add(e.getAsString());
        } else if (e.isJsonArray()) {
            final JsonArray array = e.getAsJsonArray();
            for (final JsonElement v : array) {
                if (!v.isJsonPrimitive()) throw new IllegalArgumentException(name + " must be a string or an array of strings");
                list.add(v.getAsString());
            }
        } else {
            throw new IllegalArgumentException(name + " must be a string or an array of strings");
        }
        return list;
    }
}
