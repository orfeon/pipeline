package com.mercari.solution.util.pipeline.feature;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.TemplateUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Entry point shared by the {@code feature} transform, the REST API and the MCP / agent tools:
 * resolves the {@code sources} / {@code features} documents referenced from a parameters block (inline
 * object, URI, local path or {@code data:} text) and compiles the plan ({@code validate --expand}).
 */
public final class FeaturePlanService {

    private FeaturePlanService() {}

    /** Resolved documents of one feature step: the sources contract and the parameters with inline features. */
    public record Documents(JsonElement sources, JsonObject parameters) {}

    /**
     * Resolves document references inside {@code parameters}. The returned parameters object is a copy with
     * {@code features} inlined; {@code sources} is returned separately (null when absent).
     */
    public static Documents resolve(final JsonObject parameters, final Map<String, String> templateArgs) {
        final JsonObject copy = parameters.deepCopy();
        final JsonElement sources = loadDocument(copy, "sources", templateArgs);
        final JsonElement features = loadDocument(copy, "features", templateArgs);
        if (features != null) {
            if (!features.isJsonArray()) {
                throw new IllegalArgumentException("the referenced features document must be a list (or contain a top-level features list)");
            }
            copy.add("features", features);
        }
        resolveInclude(copy, templateArgs);
        resolveTemperatureFrom(copy, templateArgs);
        return new Documents(sources, copy);
    }

    /**
     * {@code ops[].temperatureFrom: <uri>} of a softmax op (a calibration document) is read here and replaced by
     * {@code {source, hash, value}}: a bare number, or a JSON object with {@code temperature} / {@code T}. The
     * resolved value is outside the plan hash (no fit depends on it) and inside the output hash / manifest.
     */
    static void resolveTemperatureFrom(final JsonObject parameters, final Map<String, String> templateArgs) {
        if (!parameters.has("features") || !parameters.get("features").isJsonArray()) return;
        for (final JsonElement f : parameters.getAsJsonArray("features")) {
            if (!f.isJsonObject() || !f.getAsJsonObject().has("ops") || !f.getAsJsonObject().get("ops").isJsonArray()) continue;
            for (final JsonElement e : f.getAsJsonObject().getAsJsonArray("ops")) {
                if (!e.isJsonObject()) continue;
                final JsonObject op = e.getAsJsonObject();
                if (!op.has("temperatureFrom") || !op.get("temperatureFrom").isJsonPrimitive()) continue;
                final String reference = op.get("temperatureFrom").getAsString();
                final String raw;
                try {
                    raw = Config.readContent(reference);
                } catch (final IOException ex) {
                    throw new IllegalArgumentException("failed to read temperatureFrom: " + reference, ex);
                }
                final String text = (templateArgs == null ? raw : TemplateUtil.executeStrictTemplate(raw, templateArgs)).trim();
                final Double value = parseTemperature(text, reference);
                final JsonObject resolved = new JsonObject();
                resolved.addProperty("source", reference);
                resolved.addProperty("hash", FeaturePlanCompiler.sha256(text));
                resolved.addProperty("value", value);
                op.add("temperatureFrom", resolved);
            }
        }
    }

    static Double parseTemperature(final String text, final String reference) {
        try {
            return Double.parseDouble(text);
        } catch (final NumberFormatException ignored) {
            // a document
        }
        final JsonElement parsed;
        try {
            parsed = com.google.gson.JsonParser.parseString(text);
        } catch (final RuntimeException e) {
            throw new IllegalArgumentException("temperatureFrom " + reference + " is neither a number nor JSON: " + e.getMessage(), e);
        }
        if (parsed.isJsonPrimitive() && parsed.getAsJsonPrimitive().isNumber()) return parsed.getAsDouble();
        if (parsed.isJsonObject()) {
            for (final String key : List.of("temperature", "T", "t")) {
                final JsonElement v = parsed.getAsJsonObject().get(key);
                if (v != null && v.isJsonPrimitive() && v.getAsJsonPrimitive().isNumber()) return v.getAsDouble();
            }
        }
        throw new IllegalArgumentException("temperatureFrom " + reference + " must be a number or an object with a numeric 'temperature' (or 'T')");
    }

    /**
     * {@code output.include} given as a URI / path (a screening step's pass list, a hand-written list) is read
     * here and replaced by its column list; {@code output.includeSource} keeps the reference and
     * {@code output.includeHash} the content hash, so the manifest records what was applied even when the
     * file changes later. Accepted content: a JSON array of names, a JSON object with a {@code columns} /
     * {@code fields} / {@code passed} / {@code include} array, or one name per line ({@code #} comments).
     */
    static void resolveInclude(final JsonObject parameters, final Map<String, String> templateArgs) {
        if (!parameters.has("output") || !parameters.get("output").isJsonObject()) return;
        final JsonObject output = parameters.getAsJsonObject("output");
        if (!output.has("include") || !output.get("include").isJsonPrimitive()) return;
        final String reference = output.get("include").getAsString();
        final String raw;
        try {
            raw = Config.readContent(reference);
        } catch (final IOException e) {
            throw new IllegalArgumentException("failed to read output.include: " + reference, e);
        }
        final String text = templateArgs == null ? raw : TemplateUtil.executeStrictTemplate(raw, templateArgs);
        final List<String> names = parseIncludeList(text, reference);
        final JsonArray array = new JsonArray();
        names.forEach(array::add);
        output.add("include", array);
        output.addProperty("includeSource", reference);
        output.addProperty("includeHash", FeaturePlanCompiler.sha256(FeaturePlanCompiler.canonical(array)));
    }

    static List<String> parseIncludeList(final String text, final String reference) {
        final String trimmed = text == null ? "" : text.trim();
        final List<String> names = new ArrayList<>();
        if (trimmed.startsWith("[") || trimmed.startsWith("{")) {
            final JsonElement parsed;
            try {
                parsed = com.google.gson.JsonParser.parseString(trimmed);
            } catch (final RuntimeException e) {
                throw new IllegalArgumentException("output.include " + reference + " is not valid JSON: " + e.getMessage(), e);
            }
            JsonElement list = parsed;
            if (parsed.isJsonObject()) {
                list = null;
                for (final String key : List.of("columns", "fields", "passed", "include")) {
                    if (parsed.getAsJsonObject().has(key) && parsed.getAsJsonObject().get(key).isJsonArray()) {
                        list = parsed.getAsJsonObject().get(key);
                        break;
                    }
                }
                if (list == null) {
                    throw new IllegalArgumentException("output.include " + reference + " must be a JSON array or an object with a columns / fields / passed / include array");
                }
            }
            for (final JsonElement e : list.getAsJsonArray()) {
                if (e.isJsonPrimitive()) {
                    names.add(e.getAsString());
                } else if (e.isJsonObject() && e.getAsJsonObject().has("name") && e.getAsJsonObject().get("name").isJsonPrimitive()) {
                    names.add(e.getAsJsonObject().get("name").getAsString());
                }
            }
            return names;
        }
        for (final String line : trimmed.split("\\r?\\n")) {
            final String name = line.trim();
            if (name.isEmpty() || name.startsWith("#")) continue;
            names.add(name);
        }
        return names;
    }

    /**
     * Compiles a feature step from its parameters block.
     *
     * @param inputSchema input relation schema when known (null skips the lineage ↔ schema cross-check)
     */
    public static FeaturePlan compile(final JsonObject parameters, final Map<String, String> templateArgs, final Schema inputSchema) {
        final Documents documents = resolve(parameters, templateArgs);
        return FeaturePlanCompiler.compile(documents.sources(), documents.parameters(),
                inputSchema == null ? null : inputSchema.getFields());
    }

    /**
     * Request shape of the validate / expand API:
     * {@code {parameters: {...}, inputSchema: {fields: [...]} | [fields], args: {k: v}}} or a whole
     * pipeline config with a {@code transforms[].module == feature} step selected by {@code name}.
     */
    public static JsonObject validate(final JsonObject rawRequest) {
        final JsonObject response = new JsonObject();
        // same ${args.*} substitution as Config.load (request.args / system.args), so references such as
        // sources: gs://${args.bucket}/sources.yaml validate exactly as they run
        final JsonObject request;
        try {
            request = Config.processArgs(rawRequest, null);
        } catch (final RuntimeException e) {
            response.addProperty("ok", false);
            response.addProperty("error", "failed to substitute args: " + (e.getMessage() == null ? e.toString() : e.getMessage()));
            return response;
        }
        if (request.has("system") && request.get("system").isJsonObject() && request.getAsJsonObject("system").has("imports")) {
            response.addProperty("ok", false);
            response.addProperty("error", "system.imports is not resolved by validate; pass the feature step's parameters (or a config with the step inlined)");
            return response;
        }
        JsonObject parameters = null;
        if (request.has("parameters") && request.get("parameters").isJsonObject()) {
            parameters = request.getAsJsonObject("parameters");
        } else if (request.has("transforms") && request.get("transforms").isJsonArray()) {
            // a whole config also carries the PIPELINE name at the top level: use `name` as a step selector
            // only when it actually names a feature transform, otherwise take the first feature step
            final String name = request.has("name") && request.get("name").isJsonPrimitive() ? request.get("name").getAsString() : null;
            JsonObject first = null;
            JsonObject named = null;
            for (final JsonElement e : request.getAsJsonArray("transforms")) {
                if (!e.isJsonObject()) continue;
                final JsonObject step = e.getAsJsonObject();
                final boolean isFeature = step.has("module") && "feature".equals(step.get("module").getAsString());
                if (!isFeature || !step.has("parameters")) continue;
                if (first == null) first = step.getAsJsonObject("parameters");
                if (name != null && step.has("name") && name.equals(step.get("name").getAsString())) {
                    named = step.getAsJsonObject("parameters");
                    break;
                }
            }
            parameters = named != null ? named : first;
        }
        if (parameters == null) {
            response.addProperty("ok", false);
            response.addProperty("error", "request requires 'parameters' (a feature step's parameters block) or a config with a feature transform");
            return response;
        }
        final Map<String, String> args = new java.util.HashMap<>();
        if (request.has("args") && request.get("args").isJsonObject()) {
            for (final Map.Entry<String, JsonElement> e : request.getAsJsonObject("args").entrySet()) {
                args.put(e.getKey(), e.getValue().isJsonPrimitive() ? e.getValue().getAsString() : e.getValue().toString());
            }
        }
        final Schema inputSchema = parseInputSchema(request.get("inputSchema"));
        try {
            final FeaturePlan plan = compile(parameters, args, inputSchema);
            final boolean streaming = request.has("streaming") && request.get("streaming").isJsonPrimitive() && request.get("streaming").getAsBoolean();
            final List<String> engine = FeatureStages.engineConstraints(plan, streaming);
            response.addProperty("ok", !plan.getDiagnostics().hasErrors() && engine.isEmpty());
            response.add("plan", plan.toJson());
            final JsonArray engineErrors = new JsonArray();
            engine.forEach(engineErrors::add);
            response.add("engineErrors", engineErrors);
            response.addProperty("describe", plan.describe() + (engine.isEmpty() ? "" : "-- engine\n  " + String.join("\n  ", engine) + "\n"));
        } catch (final RuntimeException e) {
            response.addProperty("ok", false);
            response.addProperty("error", e.getMessage() == null ? e.toString() : e.getMessage());
        }
        return response;
    }

    private static Schema parseInputSchema(final JsonElement element) {
        if (element == null || element.isJsonNull()) return null;
        final JsonArray fields;
        if (element.isJsonArray()) {
            fields = element.getAsJsonArray();
        } else if (element.isJsonObject() && element.getAsJsonObject().has("fields")) {
            fields = element.getAsJsonObject().getAsJsonArray("fields");
        } else {
            return null;
        }
        final List<Schema.Field> list = new ArrayList<>();
        for (final JsonElement f : fields) {
            if (!f.isJsonObject()) continue;
            final Schema.Field field = Schema.Field.parse(f.getAsJsonObject());
            if (field != null) list.add(field);
        }
        return Schema.of(list);
    }

    private static JsonElement loadDocument(final JsonObject parameters, final String key, final Map<String, String> templateArgs) {
        if (!parameters.has(key) || parameters.get(key).isJsonNull()) {
            return null;
        }
        final JsonElement element = parameters.get(key);
        if (!element.isJsonPrimitive()) {
            return element;
        }
        final String reference = element.getAsString();
        final String raw;
        try {
            raw = Config.readContent(reference);
        } catch (final IOException e) {
            throw new IllegalArgumentException("failed to read parameters." + key + ": " + reference, e);
        }
        final String text = templateArgs == null ? raw : TemplateUtil.executeStrictTemplate(raw, templateArgs);
        final JsonObject document = Config.convertConfigJson(text, Config.Format.unknown);
        if ("features".equals(key) && document.has("features")) {
            return document.get("features");
        }
        return document;
    }

}
