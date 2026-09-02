package com.mercari.solution.util.pipeline.profile;

import com.google.gson.JsonObject;
import com.mercari.solution.module.MElement;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * One input row reduced to the profiled fields, extracted once per element before the combine.
 * Values are coerced to the profile type's canonical representation so the row is small,
 * {@link Serializable} regardless of the input data type, and needs no further conversion:
 * NUMERIC → {@link Double}, STRING → {@link String}, BOOL → {@link Boolean},
 * TIMESTAMP → {@link Double} epoch millis, ARRAY_LENGTH → {@link Integer}. A non-null value the
 * coercion cannot interpret becomes {@link Marker#ERROR} (counted as a field error); a value whose
 * extraction threw is also {@link Marker#ERROR} and additionally marks the row as failed so the
 * sink can route it to the failure output.
 */
public class ProfileRow implements Serializable {

    /** Enum so identity survives Java serialization. */
    public enum Marker implements Serializable {
        ERROR
    }

    public static final int MAX_SAMPLE_STRING_LENGTH = 256;

    public Object[] values;
    public String sampleJson;
    public int inputIndex;
    public String failedField;     // first field whose extraction threw (null when the row is fine)
    public String failureMessage;

    public boolean isFailed() {
        return failedField != null;
    }

    public static ProfileRow of(final ProfileSpec spec, final MElement element) {
        return of(spec, element, element == null ? -1 : element.getIndex());
    }

    public static ProfileRow of(final ProfileSpec spec, final MElement element, final int inputIndex) {
        final List<ProfileSpec.FieldSpec> fieldSpecs = spec.getFields();
        final ProfileRow row = new ProfileRow();
        row.values = new Object[fieldSpecs.size()];
        row.inputIndex = inputIndex;

        // one accessor call per top-level field: a column the profile does not use is never
        // converted, and a column that fails to convert only affects the fields under it
        final Map<String, Object> topLevel = new HashMap<>();
        for(int i = 0; i < fieldSpecs.size(); i++) {
            final ProfileSpec.FieldSpec fieldSpec = fieldSpecs.get(i);
            final String[] parts = fieldSpec.path.split("\\.");
            try {
                final Object root;
                if(topLevel.containsKey(parts[0])) {
                    root = topLevel.get(parts[0]);
                } else {
                    root = element == null ? null : element.getPrimitiveValue(parts[0]);
                    topLevel.put(parts[0], root);
                }
                final Object raw = parts.length == 1 ? root : ProfileSpec.navigate(root, parts, 1);
                row.values[i] = coerce(fieldSpec, raw);
            } catch (final Throwable e) {
                row.values[i] = Marker.ERROR;
                if(row.failedField == null) {
                    row.failedField = fieldSpec.path;
                    row.failureMessage = e.getClass().getSimpleName() + (e.getMessage() == null ? "" : ": " + e.getMessage());
                }
            }
        }
        if(spec.isSampleEnabled()) {
            row.sampleJson = toSampleJson(fieldSpecs, row.values);
        }
        return row;
    }

    /** Coerces one raw primitive to the field's canonical value, {@link Marker#ERROR} when not interpretable. */
    static Object coerce(final ProfileSpec.FieldSpec fieldSpec, final Object raw) {
        if(raw == null) {
            return null;
        }
        final Object coerced = switch (fieldSpec.profileType) {
            case NUMERIC -> ProfileSpec.toDouble(raw, fieldSpec.scale);
            case STRING -> ProfileSpec.toStringValue(raw, fieldSpec.symbols);
            case BOOL -> ProfileSpec.toBoolean(raw);
            case TIMESTAMP -> ProfileSpec.toEpochMillis(raw, fieldSpec.sourceType);
            case ARRAY_LENGTH -> ProfileSpec.arrayLength(raw);
        };
        return coerced == null ? Marker.ERROR : coerced;
    }

    /**
     * Renders the row as a compact JSON string for the VarOpt sample (profiled fields only).
     * Non-finite numbers become null so the embedded payload stays valid JSON.
     */
    private static String toSampleJson(final List<ProfileSpec.FieldSpec> fieldSpecs, final Object[] values) {
        final JsonObject json = new JsonObject();
        for(int i = 0; i < fieldSpecs.size(); i++) {
            final ProfileSpec.FieldSpec fieldSpec = fieldSpecs.get(i);
            final Object value = values[i];
            if(value == null || value == Marker.ERROR) {
                json.add(fieldSpec.path, null);
                continue;
            }
            switch (fieldSpec.profileType) {
                case NUMERIC -> {
                    final Double d = (Double) value;
                    if(d.isNaN() || d.isInfinite()) {
                        json.add(fieldSpec.path, null);
                    } else {
                        json.addProperty(fieldSpec.path, d);
                    }
                }
                case BOOL -> json.addProperty(fieldSpec.path, (Boolean) value);
                case TIMESTAMP -> {
                    final Double ms = (Double) value;
                    if(ms.isNaN() || ms.isInfinite()) {
                        json.add(fieldSpec.path, null);
                    } else {
                        json.addProperty(fieldSpec.path, Instant.ofEpochMilli(ms.longValue()).toString());
                    }
                }
                case ARRAY_LENGTH -> json.addProperty(fieldSpec.path, (Integer) value);
                default -> {
                    final String s = (String) value;
                    json.addProperty(fieldSpec.path, s.length() > MAX_SAMPLE_STRING_LENGTH ? s.substring(0, MAX_SAMPLE_STRING_LENGTH) : s);
                }
            }
        }
        return json.toString();
    }
}
