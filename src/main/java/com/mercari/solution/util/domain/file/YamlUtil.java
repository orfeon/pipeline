package com.mercari.solution.util.domain.file;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.snakeyaml.engine.v2.api.Load;
import org.snakeyaml.engine.v2.api.LoadSettings;
import org.snakeyaml.engine.v2.schema.CoreSchema;

import java.io.InputStream;
import java.util.Base64;
import java.util.Collection;
import java.util.Map;

/**
 * YAML loading on SnakeYAML Engine (YAML 1.2, core schema). YAML 1.2 is a superset of JSON, so a
 * YAML config means the same as its JSON form: {@code yes}/{@code no}/{@code on}/{@code off},
 * {@code 2024-01-01} and {@code 1:30} stay strings (SnakeYAML's YAML 1.1 resolved them to booleans,
 * {@code java.util.Date} and sexagesimal ints). The core schema adds only {@code ~}/{@code Null} as
 * null, {@code True}/{@code FALSE}, {@code 0o}/{@code 0x} ints and {@code .inf}/{@code .nan}.
 */
public class YamlUtil {

    // SnakeYAML Engine's default (3 MB) is too small for configs that inline data (e.g. create source)
    private static final int CODE_POINT_LIMIT = 64 * 1024 * 1024;

    private YamlUtil() {}

    private static Load newLoad() {
        // Load is not thread-safe: one per call
        return new Load(LoadSettings.builder()
                .setSchema(new CoreSchema())
                .setAllowDuplicateKeys(false)
                .setCodePointLimit(CODE_POINT_LIMIT)
                .build());
    }

    /** Loads one YAML document as plain Java values (Map / List / String / Number / Boolean / null). */
    public static Object load(final String text) {
        return newLoad().loadFromString(text);
    }

    public static Object load(final InputStream is) {
        return newLoad().loadFromInputStream(is);
    }

    public static JsonElement toJson(final String text) {
        return toJson(load(text));
    }

    /** Converts values loaded by {@link #load} to Gson. Non-string keys become their string form, as JSON requires. */
    public static JsonElement toJson(final Object value) {
        return switch (value) {
            case null -> JsonNull.INSTANCE;
            case Map<?, ?> map -> {
                final JsonObject object = new JsonObject();
                for (final Map.Entry<?, ?> entry : map.entrySet()) {
                    object.add(String.valueOf(entry.getKey()), toJson(entry.getValue()));
                }
                yield object;
            }
            case Collection<?> collection -> {
                final JsonArray array = new JsonArray();
                for (final Object element : collection) {
                    array.add(toJson(element));
                }
                yield array;
            }
            case String s -> new JsonPrimitive(s);
            case Number n -> new JsonPrimitive(n);
            case Boolean b -> new JsonPrimitive(b);
            // !!binary
            case byte[] bytes -> new JsonPrimitive(Base64.getEncoder().encodeToString(bytes));
            default -> new JsonPrimitive(value.toString());
        };
    }

}
