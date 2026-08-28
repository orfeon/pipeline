package com.mercari.solution.server.launch;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.config.Config;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A parsed {@code /api/launch} request: the loaded config, the launch parameters entered in the UI,
 * the template args, the calling user and the defaults resolver.
 */
public record LaunchRequest(
        Config config,
        JsonObject parameters,
        JsonObject args,
        String userEmail,
        LaunchDefaults defaults) {

    public String param(final String name) {
        if(parameters == null || !parameters.has(name) || parameters.get(name).isJsonNull()) {
            return null;
        }
        final JsonElement element = parameters.get(name);
        if(element.isJsonPrimitive()) {
            final String value = element.getAsString();
            return value.isBlank() ? null : value.trim();
        }
        return element.toString();
    }

    public Integer paramInt(final String name) {
        final String value = param(name);
        if(value == null) {
            return null;
        }
        try {
            return Integer.parseInt(value);
        } catch (final NumberFormatException e) {
            throw new IllegalArgumentException("launch parameter " + name + " must be an integer, but: " + value);
        }
    }

    public boolean paramBool(final String name) {
        final String value = param(name);
        return value != null && Boolean.parseBoolean(value);
    }

    /** Template args as {@code --args.<key>=<value>} entries (JSON values are passed as their JSON text). */
    public Map<String, String> argsMap() {
        final Map<String, String> map = new LinkedHashMap<>();
        if(args == null) {
            return map;
        }
        for(final Map.Entry<String, JsonElement> entry : args.entrySet()) {
            final JsonElement value = entry.getValue();
            if(value == null || value.isJsonNull()) {
                continue;
            }
            map.put(entry.getKey(), value.isJsonPrimitive() ? value.getAsString() : value.toString());
        }
        return map;
    }

}
