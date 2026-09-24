package com.mercari.solution.util.pipeline.feature;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.TemplateUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
        resolveClocks(sources, templateArgs);
        return new Documents(sources, copy);
    }

    /**
     * {@code clocks[].uri} of the sources document (a calendar's tick dates: one per line / the first CSV column, or a
     * JSON array) is read here into {@code dates}, with {@code hash} = the hash of those dates: the plan hash covers
     * the dates (and the uri they came from) — a new holiday is a new plan, while a comment or a reformatting of the
     * file that leaves the ticks alone is not (as {@code output.includeHash} hashes the parsed list, not the text).
     */
    static void resolveClocks(final JsonElement sources, final Map<String, String> templateArgs) {
        if (sources == null || !sources.isJsonObject() || !sources.getAsJsonObject().has("clocks")
                || !sources.getAsJsonObject().get("clocks").isJsonArray()) return;
        for (final JsonElement e : sources.getAsJsonObject().getAsJsonArray("clocks")) {
            if (!e.isJsonObject()) continue;
            final JsonObject clock = e.getAsJsonObject();
            if (!clock.has("uri") || clock.has("dates") || !clock.get("uri").isJsonPrimitive()) continue;
            final String reference = clock.get("uri").getAsString();
            final String raw;
            try {
                raw = Config.readContent(reference);
            } catch (final IOException ex) {
                throw new IllegalArgumentException("failed to read the dates of clock " + clock.get("name") + ": " + reference, ex);
            }
            final String text = templateArgs == null ? raw : TemplateUtil.executeStrictTemplate(raw, templateArgs);
            final List<String> parsed;
            try {
                parsed = Clock.parseDates(text);
            } catch (final RuntimeException ex) {
                throw new IllegalArgumentException("failed to parse the dates of clock " + clock.get("name") + ": " + reference
                        + " (" + ex.getMessage() + ")", ex);
            }
            final JsonArray dates = new JsonArray();
            parsed.forEach(dates::add);
            clock.add("dates", dates);
            clock.addProperty("hash", FeaturePlanCompiler.sha256(FeaturePlanCompiler.canonical(dates)));
        }
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

    /** {@code output.include.mode}: how several files combine — every name of any file (in first-appearance order), or the names in all of them (in the first file's order). */
    public static final String INCLUDE_UNION = "union";
    public static final String INCLUDE_INTERSECTION = "intersection";
    public static final List<String> INCLUDE_MODES = List.of(INCLUDE_UNION, INCLUDE_INTERSECTION);

    /**
     * {@code output.include} given as a URI / path (a screening step's pass list, a hand-written list) — or as
     * {@code {from: [uri, ...], mode: union | intersection}}, several such files combined — is read here and
     * replaced by its column list; {@code output.includeSource} keeps the reference(s) and
     * {@code output.includeHash} the hash of the resolved list, so the manifest records what was applied even
     * when a file changes later. Accepted content of each file: a JSON array of names, a JSON object with a
     * {@code columns} / {@code fields} / {@code passed} / {@code include} array, or one name per line ({@code #}
     * comments). A plain list of names is left to the compiler.
     */
    static void resolveInclude(final JsonObject parameters, final Map<String, String> templateArgs) {
        if (!parameters.has("output") || !parameters.get("output").isJsonObject()) return;
        final JsonObject output = parameters.getAsJsonObject("output");
        if (!output.has("include") || output.get("include").isJsonNull()) return;
        final JsonElement include = output.get("include");
        final List<String> references = new ArrayList<>();
        String mode = INCLUDE_UNION;
        final boolean objectForm = include.isJsonObject();
        if (include.isJsonPrimitive()) {
            references.add(include.getAsString());
        } else if (objectForm) {
            final JsonObject o = include.getAsJsonObject();
            for (final String key : o.keySet()) {
                if (!key.equals("from") && !key.equals("mode")) throw new IllegalArgumentException("output.include: unknown key '" + key + "' (an object form is {from: [uri, ...], mode: union | intersection})");
            }
            final JsonElement from = o.get("from");
            if (from == null || from.isJsonNull()) {
                throw new IllegalArgumentException("output.include.from is required in the object form ({from: [uri, ...], mode: union | intersection})");
            } else if (isString(from)) {
                references.add(from.getAsString());
            } else if (from.isJsonArray()) {
                for (final JsonElement e : from.getAsJsonArray()) {
                    if (!isString(e)) throw new IllegalArgumentException("output.include.from must list URIs / paths: " + e);
                    references.add(e.getAsString());
                }
            } else {
                throw new IllegalArgumentException("output.include.from must be a URI / path or a list of them: " + from);
            }
            if (references.isEmpty()) throw new IllegalArgumentException("output.include.from must name at least one file");
            if (o.has("mode") && !o.get("mode").isJsonNull()) {
                if (!isString(o.get("mode"))) throw new IllegalArgumentException("output.include.mode must be one of " + INCLUDE_MODES + ": " + o.get("mode"));
                mode = o.get("mode").getAsString();
                if (!INCLUDE_MODES.contains(mode)) throw new IllegalArgumentException("output.include.mode '" + mode + "' is unknown (available: " + INCLUDE_MODES + ")");
            }
        } else {
            return;
        }
        if (references.size() == 1) {
            // a single from reads exactly like the string form: same list, same source, same hash
            writeInclude(output, readInclude(references.get(0), objectForm, templateArgs), references.get(0));
            return;
        }
        // union: every name in first-appearance order; intersection: the first file's names present in all of them
        final Set<String> names = new LinkedHashSet<>(readInclude(references.get(0), true, templateArgs));
        for (final String reference : references.subList(1, references.size())) {
            final List<String> listed = readInclude(reference, true, templateArgs);
            if (INCLUDE_INTERSECTION.equals(mode)) names.retainAll(new HashSet<>(listed));
            else names.addAll(listed);
        }
        writeInclude(output, new ArrayList<>(names), mode + "(" + String.join(", ", references) + ")");
    }

    /**
     * One pass list read and parsed. {@code fileOnly} (the object form's {@code from}, documented as URIs / paths)
     * rejects a reference {@link Config#readContent} could not read as a file: it falls back to the reference text
     * itself, so a mistyped path would become a one-name list (in a union, that file's names silently missing).
     */
    private static List<String> readInclude(final String reference, final boolean fileOnly, final Map<String, String> templateArgs) {
        final String raw;
        try {
            raw = Config.readContent(reference);
        } catch (final IOException e) {
            throw new IllegalArgumentException("failed to read output.include: " + reference, e);
        }
        if (fileOnly && reference.equals(raw) && !reference.contains("\n")) {
            throw new IllegalArgumentException("output.include.from: " + reference + " is not a readable file (URI / path)");
        }
        final String text = templateArgs == null ? raw : TemplateUtil.executeStrictTemplate(raw, templateArgs);
        return parseIncludeList(text, reference);
    }

    private static boolean isString(final JsonElement element) {
        return element.isJsonPrimitive() && element.getAsJsonPrimitive().isString();
    }

    private static void writeInclude(final JsonObject output, final List<String> names, final String source) {
        final JsonArray array = new JsonArray();
        names.forEach(array::add);
        output.add("include", array);
        output.addProperty("includeSource", source);
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
