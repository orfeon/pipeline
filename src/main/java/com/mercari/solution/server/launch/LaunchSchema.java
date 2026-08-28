package com.mercari.solution.server.launch;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.Map;

/**
 * Prepares the launch JSON Schema ({@code server/api/spec/launch.json}) for the Builder UI:
 * every property carrying {@code x-launch-default: "<KEY>"} gets an {@code x-default-hint} with the
 * value {@link LaunchDefaults} resolves from the environment / metadata server. The UI shows it as
 * a placeholder only; the field is submitted empty so that config options (unknown here) still take
 * precedence over the environment at launch time.
 */
public class LaunchSchema {

    public static final String X_LAUNCH_DEFAULT = "x-launch-default";
    /** The value the server would use when the field is left empty (rendered as a placeholder, never submitted). */
    public static final String X_DEFAULT_HINT = "x-default-hint";
    public static final String X_HIDDEN = "x-hidden";

    public static JsonObject withDefaults(final JsonObject schema, final LaunchDefaults defaults) {
        final JsonObject copy = schema.deepCopy();
        if(!copy.has("oneOf")) {
            return copy;
        }
        for(final JsonElement runnerElement : copy.getAsJsonArray("oneOf")) {
            final JsonObject runnerSchema = runnerElement.getAsJsonObject();
            final String runner = idSuffix(runnerSchema);
            fill(runnerSchema, runner, defaults);
            if(runnerSchema.has("oneOf")) {
                for(final JsonElement envElement : runnerSchema.getAsJsonArray("oneOf")) {
                    fill(envElement.getAsJsonObject(), runner, defaults);
                }
            }
        }
        return copy;
    }

    /** Last path segment of {@code $id} (e.g. {@code .../launch/direct/cloudRunJob} → {@code cloudRunJob}). */
    public static String idSuffix(final JsonObject schema) {
        if(!schema.has("$id")) {
            return null;
        }
        final String id = schema.get("$id").getAsString();
        return id.substring(id.lastIndexOf('/') + 1);
    }

    private static void fill(final JsonObject schema, final String runner, final LaunchDefaults defaults) {
        if(runner == null || !schema.has("properties") || !schema.get("properties").isJsonObject()) {
            return;
        }
        for(final Map.Entry<String, JsonElement> entry : schema.getAsJsonObject("properties").entrySet()) {
            if(!entry.getValue().isJsonObject()) {
                continue;
            }
            final JsonObject property = entry.getValue().getAsJsonObject();
            if(!property.has(X_LAUNCH_DEFAULT)) {
                continue;
            }
            final String key = property.get(X_LAUNCH_DEFAULT).getAsString();
            final String value = defaults.fromEnv(runner, key);
            if(value == null) {
                continue;
            }
            // A hint, not a default: the form must submit an empty value so the server keeps its
            // resolution order (config options come before the environment).
            property.addProperty(X_DEFAULT_HINT, value);
        }
    }

    /** Runner / environment ids declared by the schema, for consistency checks against the registered launchers. */
    public static JsonArray keys(final JsonObject schema) {
        final JsonArray keys = new JsonArray();
        if(!schema.has("oneOf")) {
            return keys;
        }
        for(final JsonElement runnerElement : schema.getAsJsonArray("oneOf")) {
            final JsonObject runnerSchema = runnerElement.getAsJsonObject();
            final String runner = idSuffix(runnerSchema);
            if(runnerSchema.has("oneOf")) {
                for(final JsonElement envElement : runnerSchema.getAsJsonArray("oneOf")) {
                    keys.add(runner + "/" + idSuffix(envElement.getAsJsonObject()));
                }
            } else {
                keys.add(runner);
            }
        }
        return keys;
    }

}
