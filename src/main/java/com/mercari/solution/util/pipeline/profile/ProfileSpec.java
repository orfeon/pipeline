package com.mercari.solution.util.pipeline.profile;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.mercari.solution.module.Schema;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Resolved profiling plan for one input schema: which fields are profiled, as which profile type,
 * and which fields were skipped (with the reason, for the report appendix).
 * Built once at pipeline construction time and serialized into the CombineFn.
 */
public class ProfileSpec implements Serializable {

    /** Max struct flatten depth (dot-joined paths). */
    public static final int MAX_DEPTH = 3;

    public enum ProfileType implements Serializable {
        NUMERIC,
        STRING,
        BOOL,
        TIMESTAMP,
        ARRAY_LENGTH
    }

    public static class FieldSpec implements Serializable {
        public String path;
        public ProfileType profileType;
        public String sourceType;
        public List<String> symbols;   // for enumeration
        public Integer scale;          // for decimal (unscaled bytes → value)
        public boolean isKey;

        FieldSpec(String path, ProfileType profileType, String sourceType, List<String> symbols) {
            this.path = path;
            this.profileType = profileType;
            this.sourceType = sourceType;
            this.symbols = symbols;
        }

        FieldSpec copy() {
            final FieldSpec copy = new FieldSpec(path, profileType, sourceType, symbols);
            copy.scale = scale;
            copy.isKey = isKey;
            return copy;
        }
    }

    /** Scale assumed for decimal fields whose schema does not declare one (BigQuery NUMERIC). */
    public static final int DEFAULT_DECIMAL_SCALE = 9;

    public static class SkippedField implements Serializable {
        public String path;
        public String sourceType;
        public String reason;

        SkippedField(String path, String sourceType, String reason) {
            this.path = path;
            this.sourceType = sourceType;
            this.reason = reason;
        }
    }

    /** Actual sketch parameters resolved from the accuracy preset. */
    public static class SketchParameters implements Serializable {
        public int kllK;
        public int cpcLgK;
        public int fiMaxMapSize;
        public int thetaLgK;
        public int sampleK;
        public int topKKeep;

        public static SketchParameters of(final String accuracy) {
            final SketchParameters p = new SketchParameters();
            switch (accuracy == null ? "default" : accuracy) {
                case "low" -> {
                    p.kllK = 100;
                    p.cpcLgK = 10;
                    p.thetaLgK = 10;
                }
                case "high" -> {
                    p.kllK = 800;
                    p.cpcLgK = 14;
                    p.thetaLgK = 14;
                }
                default -> {
                    p.kllK = 200;
                    p.cpcLgK = 12;
                    p.thetaLgK = 12;
                }
            }
            p.fiMaxMapSize = 512;
            p.sampleK = 10_000;
            p.topKKeep = 50;
            return p;
        }
    }

    /**
     * The declared binary target: a profiled field whose value equals {@link #positive} marks a
     * positive row, any other non-null value a negative row; null/error target rows are excluded
     * from the target analysis. {@link #fieldStats} enables the per-field class sketches (the
     * global profile); group sub-profiles only count the class totals.
     */
    public static class TargetSpec implements Serializable {
        public String path;
        public int fieldIndex;
        public ProfileType profileType;
        public Object positive;          // Boolean / Double / String, matching the profile type
        public boolean fieldStats = true;

        /** Display form of the positive value ({@code 1} rather than {@code 1.0} for numbers). */
        public String positiveLabel() {
            if(positive instanceof Double d && d == Math.rint(d) && Math.abs(d) < 1e15) {
                return String.valueOf(d.longValue());
            }
            return String.valueOf(positive);
        }

        /** Class of a coerced {@link ProfileRow} value: true/false, or null for null/error/unmatched types. */
        public Boolean classOf(final Object value) {
            if(value == null || value == ProfileRow.Marker.ERROR) {
                return null;
            }
            return switch (profileType) {
                case BOOL -> value instanceof Boolean b ? b.equals(positive) : null;
                case NUMERIC -> value instanceof Double d ? d.equals(positive) : null;
                case STRING -> value instanceof String s ? s.equals(positive) : null;
                default -> null;
            };
        }

        TargetSpec copy() {
            final TargetSpec copy = new TargetSpec();
            copy.path = path;
            copy.fieldIndex = fieldIndex;
            copy.profileType = profileType;
            copy.positive = positive;
            copy.fieldStats = fieldStats;
            return copy;
        }
    }

    private final List<FieldSpec> fields;
    private final List<SkippedField> skipped;
    private final SketchParameters sketchParameters;
    private final boolean sampleEnabled;
    private final boolean correlationEnabled;
    private TargetSpec target;

    private ProfileSpec(
            final List<FieldSpec> fields,
            final List<SkippedField> skipped,
            final SketchParameters sketchParameters,
            final boolean sampleEnabled,
            final boolean correlationEnabled) {

        this.fields = fields;
        this.skipped = skipped;
        this.sketchParameters = sketchParameters;
        this.sampleEnabled = sampleEnabled;
        this.correlationEnabled = correlationEnabled;
    }

    public List<FieldSpec> getFields() {
        return fields;
    }

    public List<SkippedField> getSkipped() {
        return skipped;
    }

    public SketchParameters getSketchParameters() {
        return sketchParameters;
    }

    public boolean isSampleEnabled() {
        return sampleEnabled;
    }

    public boolean isCorrelationEnabled() {
        return correlationEnabled;
    }

    /** The declared binary target, or null. */
    public TargetSpec getTarget() {
        return target;
    }

    /**
     * Declares the binary target field. {@code positive} is the value that marks a positive row:
     * optional for bool fields (default {@code true}), required for numeric ({@code 1}) and string
     * ({@code "sold"}) fields; timestamp and array fields cannot be targets.
     *
     * @throws IllegalArgumentException with a user-facing message when the declaration is invalid
     */
    public ProfileSpec withTarget(final String path, final Object positive) {
        int index = -1;
        for(int i = 0; i < fields.size(); i++) {
            if(fields.get(i).path.equals(path)) {
                index = i;
            }
        }
        if(index < 0) {
            throw new IllegalArgumentException("target field not found in the profiled fields: " + path);
        }
        final FieldSpec fieldSpec = fields.get(index);
        final TargetSpec targetSpec = new TargetSpec();
        targetSpec.path = path;
        targetSpec.fieldIndex = index;
        targetSpec.profileType = fieldSpec.profileType;
        switch (fieldSpec.profileType) {
            case BOOL -> {
                if(positive == null) {
                    targetSpec.positive = Boolean.TRUE;
                } else if(positive instanceof Boolean b) {
                    targetSpec.positive = b;
                } else if(positive instanceof String s && Set.of("true", "false").contains(s.toLowerCase())) {
                    targetSpec.positive = Boolean.parseBoolean(s);
                } else {
                    throw new IllegalArgumentException("target.positive for the bool field " + path + " must be true or false: " + positive);
                }
            }
            case NUMERIC -> {
                if(positive == null) {
                    throw new IllegalArgumentException(
                            "target.positive is required for the numeric field " + path
                                    + " (e.g. {field: " + path + ", positive: 1}); continuous targets are not supported");
                }
                final Double d = positive instanceof Boolean ? null : toDouble(positive);
                if(d == null) {
                    throw new IllegalArgumentException("target.positive for the numeric field " + path + " must be a number: " + positive);
                }
                targetSpec.positive = d;
            }
            case STRING -> {
                if(positive == null) {
                    throw new IllegalArgumentException(
                            "target.positive is required for the string field " + path + " (e.g. {field: " + path + ", positive: sold})");
                }
                targetSpec.positive = String.valueOf(positive);
            }
            default -> throw new IllegalArgumentException(
                    "target field must be a bool, numeric or string field: " + path + " is " + fieldSpec.profileType.name().toLowerCase());
        }
        this.target = targetSpec;
        return this;
    }

    /** Indices (into {@link #getFields()}) of NUMERIC fields, in order. Used by the correlation accumulator. */
    public List<Integer> getNumericFieldIndices() {
        final List<Integer> indices = new ArrayList<>();
        for(int i = 0; i < fields.size(); i++) {
            if(ProfileType.NUMERIC.equals(fields.get(i).profileType)) {
                indices.add(i);
            }
        }
        return indices;
    }

    public List<Integer> getKeyFieldIndices() {
        final List<Integer> indices = new ArrayList<>();
        for(int i = 0; i < fields.size(); i++) {
            if(fields.get(i).isKey) {
                indices.add(i);
            }
        }
        return indices;
    }

    public static ProfileSpec of(
            final Schema schema,
            final Set<String> includeFields,
            final Set<String> excludeFields,
            final Set<String> keyFields,
            final String accuracy,
            final boolean sampleEnabled,
            final boolean correlationEnabled) {

        final List<FieldSpec> fields = new ArrayList<>();
        final List<SkippedField> skipped = new ArrayList<>();
        collect(schema, "", 1, includeFields, excludeFields, fields, skipped);
        for(final FieldSpec field : fields) {
            if(keyFields != null && keyFields.contains(field.path)) {
                field.isKey = true;
            }
        }
        return new ProfileSpec(
                fields, skipped, SketchParameters.of(accuracy), sampleEnabled, correlationEnabled);
    }

    /**
     * The spec for per-group sub-profiles: the same fields in the same order (so a
     * {@link ProfileRow} extracted with this spec feeds both), without key sketches, row sample,
     * correlations and per-field target sketches (the target class totals are still counted, for
     * the per-group target rate).
     */
    public ProfileSpec groupSpec() {
        final List<FieldSpec> groupFields = new ArrayList<>();
        for(final FieldSpec field : fields) {
            final FieldSpec copy = field.copy();
            copy.isKey = false;
            groupFields.add(copy);
        }
        final ProfileSpec group = new ProfileSpec(groupFields, skipped, sketchParameters, false, false);
        if(target != null) {
            group.target = target.copy();
            group.target.fieldStats = false;
        }
        return group;
    }

    private static void collect(
            final Schema schema,
            final String prefix,
            final int depth,
            final Set<String> includeFields,
            final Set<String> excludeFields,
            final List<FieldSpec> fields,
            final List<SkippedField> skipped) {

        for(final Schema.Field field : schema.getFields()) {
            final String path = prefix.isEmpty() ? field.getName() : prefix + "." + field.getName();
            final Schema.FieldType fieldType = field.getFieldType();
            final Schema.Type type = fieldType.getType();

            if(excludeFields != null && excludeFields.contains(path)) {
                skipped.add(new SkippedField(path, type.name(), "excluded by parameters.fields.exclude"));
                continue;
            }
            // include filter applies to leaf paths; struct containers are always traversed so that
            // `include: [attributes.color]` works without listing the parent
            if(includeFields != null && !includeFields.isEmpty()
                    && !Schema.Type.element.equals(type)
                    && !matchesInclude(includeFields, path)) {
                continue;
            }

            switch (type) {
                case bool -> fields.add(new FieldSpec(path, ProfileType.BOOL, type.name(), null));
                case string, json -> fields.add(new FieldSpec(path, ProfileType.STRING, type.name(), null));
                case enumeration -> fields.add(new FieldSpec(path, ProfileType.STRING, type.name(), fieldType.getSymbols()));
                case int8, int16, int32, int64, float8, float16, float32, float64 ->
                        fields.add(new FieldSpec(path, ProfileType.NUMERIC, type.name(), null));
                case decimal -> {
                    final FieldSpec fieldSpec = new FieldSpec(path, ProfileType.NUMERIC, type.name(), null);
                    fieldSpec.scale = fieldType.getScale() == null ? DEFAULT_DECIMAL_SCALE : fieldType.getScale();
                    fields.add(fieldSpec);
                }
                case timestamp, datetime, date -> fields.add(new FieldSpec(path, ProfileType.TIMESTAMP, type.name(), null));
                case array -> fields.add(new FieldSpec(path, ProfileType.ARRAY_LENGTH, type.name(), null));
                case element -> {
                    if(depth >= MAX_DEPTH) {
                        skipped.add(new SkippedField(path, type.name(), "nested deeper than depth " + MAX_DEPTH));
                    } else {
                        collect(fieldType.getElementSchema(), path, depth + 1, includeFields, excludeFields, fields, skipped);
                    }
                }
                default -> skipped.add(new SkippedField(path, type.name(), "unsupported type"));
            }
        }
    }

    private static boolean matchesInclude(final Set<String> includeFields, final String path) {
        if(includeFields.contains(path)) {
            return true;
        }
        // a struct include (e.g. `attributes`) selects all of its leaves
        for(final String include : includeFields) {
            if(path.startsWith(include + ".")) {
                return true;
            }
        }
        return false;
    }

    // ---- value extraction (primitive map navigation + coercion) ----

    /** Navigates a dot path through nested primitive maps. Returns null when absent. */
    public static Object getValue(final Map<String, Object> primitives, final String path) {
        if(primitives == null) {
            return null;
        }
        return navigate(primitives, path.split("\\."), 0);
    }

    /**
     * Navigates {@code parts[from..]} down from {@code root}. Nested structs arrive as maps from
     * most data types, but as a JSON string from Spanner STRUCT columns and as {@link JsonObject}
     * once such a string has been parsed — all three are traversed. Leaf JSON primitives are
     * unwrapped to plain Java values.
     */
    public static Object navigate(final Object root, final String[] parts, final int from) {
        Object current = root;
        for(int p = from; p < parts.length; p++) {
            if(current == null) {
                return null;
            }
            if(current instanceof String s) {
                current = parseJsonObject(s);
            }
            if(current instanceof Map<?, ?> map) {
                current = map.get(parts[p]);
            } else if(current instanceof JsonObject json) {
                current = json.get(parts[p]);
            } else {
                return null;
            }
        }
        return unwrapJson(current);
    }

    private static JsonObject parseJsonObject(final String s) {
        final String trimmed = s.trim();
        if(!trimmed.startsWith("{")) {
            return null;
        }
        try {
            final JsonElement element = JsonParser.parseString(trimmed);
            return element.isJsonObject() ? element.getAsJsonObject() : null;
        } catch (final Exception e) {
            return null;
        }
    }

    private static Object unwrapJson(final Object value) {
        if(!(value instanceof JsonElement element)) {
            return value;
        }
        if(element.isJsonNull()) {
            return null;
        }
        if(element.isJsonPrimitive()) {
            final JsonPrimitive primitive = element.getAsJsonPrimitive();
            if(primitive.isBoolean()) {
                return primitive.getAsBoolean();
            }
            if(primitive.isNumber()) {
                return primitive.getAsBigDecimal();
            }
            return primitive.getAsString();
        }
        return element;   // JsonObject (a nested struct read as a value) or JsonArray
    }

    public static Double toDouble(final Object value) {
        return toDouble(value, null);
    }

    /** {@code scale} applies to unscaled decimal bytes (Avro decimal logical type). */
    public static Double toDouble(final Object value, final Integer scale) {
        return switch (value) {
            case null -> null;
            case Double d -> d;
            case Float f -> f.doubleValue();
            case Number n -> n.doubleValue();
            case String s -> {
                try {
                    yield Double.parseDouble(s);
                } catch (final NumberFormatException e) {
                    yield null;
                }
            }
            case ByteBuffer bb -> {
                final ByteBuffer duplicate = bb.duplicate();
                duplicate.rewind();
                final byte[] bytes = new byte[duplicate.remaining()];
                duplicate.get(bytes);
                yield decimalBytesToDouble(bytes, scale);
            }
            case byte[] bytes -> decimalBytesToDouble(bytes, scale);
            default -> null;
        };
    }

    private static Double decimalBytesToDouble(final byte[] bytes, final Integer scale) {
        if(bytes.length == 0) {
            return null;
        }
        return new BigDecimal(new BigInteger(bytes), scale == null ? DEFAULT_DECIMAL_SCALE : scale).doubleValue();
    }

    /**
     * Coerces a primitive timestamp representation to epoch millis.
     * Element primitives hold timestamps as epoch micros (Long), dates as epoch days (Integer);
     * a {@link Double} is already epoch millis (a coerced {@link ProfileRow} value); strings
     * (nested Spanner structs rendered as JSON) are ISO instants or dates.
     */
    public static Double toEpochMillis(final Object value, final String sourceType) {
        return switch (value) {
            case null -> null;
            case Double d -> d;
            case Long l -> Schema.Type.date.name().equals(sourceType)
                    ? l * 86400_000d
                    : l / 1000d;
            case Integer i -> Schema.Type.date.name().equals(sourceType)
                    ? i * 86400_000d
                    : i / 1000d;
            case BigDecimal d -> Schema.Type.date.name().equals(sourceType)
                    ? d.doubleValue() * 86400_000d
                    : d.doubleValue() / 1000d;
            case Instant i -> (double) i.toEpochMilli();
            case org.joda.time.Instant i -> (double) i.getMillis();
            case LocalDate d -> (double) d.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
            case java.util.Date d -> (double) d.getTime();
            case String s -> {
                try {
                    yield (double) Instant.parse(s).toEpochMilli();
                } catch (final Exception e) {
                    try {
                        yield (double) LocalDate.parse(s).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
                    } catch (final Exception e2) {
                        yield null;
                    }
                }
            }
            default -> null;
        };
    }

    public static String toStringValue(final Object value, final List<String> symbols) {
        if(value == null) {
            return null;
        }
        if(symbols != null && value instanceof Integer index) {
            if(index >= 0 && index < symbols.size()) {
                return symbols.get(index);
            }
            return String.valueOf(index);
        }
        if(value instanceof byte[] bytes) {
            return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        }
        if(value instanceof org.apache.beam.sdk.schemas.logicaltypes.EnumerationType.Value v) {
            return symbols != null && v.getValue() < symbols.size() ? symbols.get(v.getValue()) : String.valueOf(v.getValue());
        }
        return value.toString();
    }

    /** Element count of an array value (a coerced {@link Integer} passes through). */
    public static Integer arrayLength(final Object value) {
        return switch (value) {
            case null -> null;
            case Integer i -> i;
            case java.util.Collection<?> c -> c.size();
            case JsonArray a -> a.size();
            default -> value.getClass().isArray() ? java.lang.reflect.Array.getLength(value) : null;
        };
    }

    public static Boolean toBoolean(final Object value) {
        return switch (value) {
            case null -> null;
            case Boolean b -> b;
            case String s -> Boolean.parseBoolean(s);
            case Number n -> n.doubleValue() != 0;
            default -> null;
        };
    }
}
