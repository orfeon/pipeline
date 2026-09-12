package com.mercari.solution.util.pipeline.glm;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Lenient readers of a parameters block shared by the supervised transforms' specs: a missing / null / wrongly
 * typed value reads as null (the caller decides whether that is an error), a list reader collects its errors.
 */
public final class SpecJson {

    private SpecJson() {}

    public static String string(final JsonObject o, final String key) {
        if (o == null || !o.has(key) || o.get(key).isJsonNull()) return null;
        final JsonElement e = o.get(key);
        return e.isJsonPrimitive() ? e.getAsString() : null;
    }

    public static Boolean bool(final JsonObject o, final String key) {
        if (o == null || !o.has(key) || o.get(key).isJsonNull() || !o.get(key).isJsonPrimitive()) return null;
        return o.get(key).getAsBoolean();
    }

    public static Integer integer(final JsonObject o, final String key) {
        if (o == null || !o.has(key) || o.get(key).isJsonNull() || !o.get(key).isJsonPrimitive()) return null;
        return o.get(key).getAsInt();
    }

    public static Long longValue(final JsonObject o, final String key) {
        if (o == null || !o.has(key) || o.get(key).isJsonNull() || !o.get(key).isJsonPrimitive()) return null;
        return o.get(key).getAsLong();
    }

    public static Double number(final JsonObject o, final String key) {
        if (o == null || !o.has(key) || o.get(key).isJsonNull() || !o.get(key).isJsonPrimitive()) return null;
        return o.get(key).getAsDouble();
    }

    /** A string or a list of strings; anything else adds an error and is skipped. */
    public static List<String> strings(final JsonObject o, final String key, final List<String> errors) {
        final List<String> out = new ArrayList<>();
        if (o == null || !o.has(key) || o.get(key).isJsonNull()) return out;
        final JsonElement e = o.get(key);
        if (e.isJsonPrimitive()) {
            out.add(e.getAsString());
        } else if (e.isJsonArray()) {
            for (final JsonElement i : e.getAsJsonArray()) {
                if (i.isJsonPrimitive()) out.add(i.getAsString());
                else errors.add(key + " must be a list of strings");
            }
        } else {
            errors.add(key + " must be a list of strings");
        }
        return out;
    }

    /** An ISO-8601 instant as epoch milliseconds; null text → null, unparsable text → an error under {@code key}. */
    public static Long parseInstant(final String text, final String key, final List<String> errors) {
        if (text == null) return null;
        try {
            return Instant.parse(text).toEpochMilli();
        } catch (final RuntimeException e) {
            errors.add(key + " must be an ISO-8601 instant such as 2025-12-31T23:59:59Z: " + text);
            return null;
        }
    }
}
