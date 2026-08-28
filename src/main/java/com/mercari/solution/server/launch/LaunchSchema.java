package com.mercari.solution.server.launch;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.Map;

/**
 * Prepares the launch JSON Schema ({@code server/api/spec/launch.json}) for the Builder UI:
 * every property carrying {@code x-launch-default: "<KEY>"} gets its {@code default} filled from
 * {@link LaunchDefaults} (environment variables / metadata server), so the modal opens with the
 * values that are in effect. Config-derived defaults ({@code options.gcp.*}) are not known here
 * and are applied server-side at launch time.
 */
public class LaunchSchema {

    public static final String X_LAUNCH_DEFAULT = "x-launch-default";
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
            final String type = property.has("type") ? property.get("type").getAsString() : "string";
            switch (type) {
                case "integer" -> {
                    try {
                        property.addProperty("default", Integer.parseInt(value.replaceAll("[^0-9-]", "")));
                    } catch (final NumberFormatException ignored) {
                        // leave the schema default
                    }
                }
                case "number" -> {
                    try {
                        property.addProperty("default", Double.parseDouble(value));
                    } catch (final NumberFormatException ignored) {
                        // leave the schema default
                    }
                }
                case "boolean" -> property.addProperty("default", Boolean.parseBoolean(value));
                default -> property.addProperty("default", value);
            }
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
