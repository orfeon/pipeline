package com.mercari.solution.util.pipeline.profile;

import com.mercari.solution.module.Schema;

import java.io.Serializable;
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
        public boolean isKey;

        FieldSpec(String path, ProfileType profileType, String sourceType, List<String> symbols) {
            this.path = path;
            this.profileType = profileType;
            this.sourceType = sourceType;
            this.symbols = symbols;
        }
    }

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

    private final List<FieldSpec> fields;
    private final List<SkippedField> skipped;
    private final SketchParameters sketchParameters;
    private final boolean sampleEnabled;
    private final boolean correlationEnabled;

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
                case int8, int16, int32, int64, float8, float16, float32, float64, decimal ->
                        fields.add(new FieldSpec(path, ProfileType.NUMERIC, type.name(), null));
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
        Object current = primitives;
        for(final String part : path.split("\\.")) {
            if(!(current instanceof Map)) {
                return null;
            }
            current = ((Map<?, ?>) current).get(part);
            if(current == null) {
                return null;
            }
        }
        return current;
    }

    public static Double toDouble(final Object value) {
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
            default -> null;
        };
    }

    /**
     * Coerces a primitive timestamp representation to epoch millis.
     * Element primitives hold timestamps as epoch micros (Long), dates as epoch days (Integer).
     */
    public static Double toEpochMillis(final Object value, final String sourceType) {
        return switch (value) {
            case null -> null;
            case Long l -> Schema.Type.date.name().equals(sourceType)
                    ? l * 86400_000d
                    : l / 1000d;
            case Integer i -> Schema.Type.date.name().equals(sourceType)
                    ? i * 86400_000d
                    : i / 1000d;
            case Instant i -> (double) i.toEpochMilli();
            case org.joda.time.Instant i -> (double) i.getMillis();
            case LocalDate d -> (double) d.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
            case java.util.Date d -> (double) d.getTime();
            case String s -> {
                try {
                    yield (double) Instant.parse(s).toEpochMilli();
                } catch (final Exception e) {
                    yield null;
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
