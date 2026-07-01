package com.mercari.solution.server.api;

import com.google.gson.*;
import com.mercari.solution.util.schema.JsonSchemaUtil;
import com.networknt.schema.SchemaRegistry;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.net.URL;
import java.util.Map;

/**
 * REST API service that returns all pipeline resource definitions in a single response.
 * Aggregates modules, launch, options, and system definitions.
 */
public class SpecService {

    public static void init() {
        ConfigSchema.load();
    }

    /**
     * Handle HTTP GET requests - returns all pipeline resource definitions as JSON Schema format.
     * This is the standard format that complies with JSON Schema Draft 2020-12.
     */
    public static void serve(
            final HttpServletRequest request,
            final HttpServletResponse response) throws IOException {

        response.setContentType("application/schema+json");
        response.setCharacterEncoding("UTF-8");

        try {
            JsonObject schemas = getAllSchemasAsArrays();
            response.getWriter().println(schemas.toString());
        } catch (Exception e) {
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            JsonObject error = new JsonObject();
            error.addProperty("error", e.getMessage());
            response.getWriter().println(error.toString());
        }
    }

    public static JsonArray getModuleAbstracts(String type) {
        final JsonArray array = switch (type) {
            case "source" -> ConfigSchema.getSourceJsonSchemas();
            case "transform" -> ConfigSchema.getTransformJsonSchemas();
            case "sink" -> ConfigSchema.getSinkJsonSchemas();
            default -> throw new IllegalArgumentException("Not supported module type: " + type);
        };
        return array;
    }

    /**
     * Returns only $id, title, description from each module schema (lightweight summaries).
     */
    private static JsonArray getModuleSummaries(final JsonArray fullSchemas) {
        final JsonArray summaries = new JsonArray();
        for (int i = 0; i < fullSchemas.size(); i++) {
            final JsonObject full = fullSchemas.get(i).getAsJsonObject();
            final JsonObject summary = new JsonObject();
            if (full.has("$id")) {
                summary.addProperty("$id", full.get("$id").getAsString());
            }
            if (full.has("title")) {
                summary.addProperty("title", full.get("title").getAsString());
            }
            if (full.has("description")) {
                summary.addProperty("description", full.get("description").getAsString());
            }
            summaries.add(summary);
        }
        return summaries;
    }

    public static JsonObject getAllSchemasAsArrays() {
        final JsonObject result = new JsonObject();

        final JsonObject modules = new JsonObject();
        modules.add("sources", getModuleSummaries(ConfigSchema.getSourceJsonSchemas()));
        modules.add("transforms", getModuleSummaries(ConfigSchema.getTransformJsonSchemas()));
        modules.add("sinks", getModuleSummaries(ConfigSchema.getSinkJsonSchemas()));
        result.add("modules", modules);

        return result;
    }

    /**
     * Handle HTTP GET requests for a specific module's editor schema.
     * Returns a self-contained JSON Schema for the YAML editor (parameters + common fields).
     * Path format: /api/spec/{type}/{moduleName}
     * Example: /api/spec/source/bigquery
     */
    public static void serveModuleSchema(
            final HttpServletRequest request,
            final HttpServletResponse response,
            final String moduleType,
            final String moduleName) throws IOException {

        response.setContentType("application/schema+json");
        response.setCharacterEncoding("UTF-8");

        try {
            final JsonObject schema = buildModuleEditorSchema(moduleType, moduleName);
            if (schema == null) {
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                final JsonObject error = new JsonObject();
                error.addProperty("error", "Module not found: " + moduleType + "/" + moduleName);
                response.getWriter().println(error.toString());
                return;
            }
            response.getWriter().println(schema.toString());
        } catch (Exception e) {
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            final JsonObject error = new JsonObject();
            error.addProperty("error", e.getMessage());
            response.getWriter().println(error.toString());
        }
    }

    /**
     * Handle HTTP GET requests for the system JSON Schema.
     * Path: /api/spec/system
     */
    public static void serveSystemSchema(
            final HttpServletRequest request,
            final HttpServletResponse response) throws IOException {

        response.setContentType("application/schema+json");
        response.setCharacterEncoding("UTF-8");

        try {
            final JsonObject schema = prepareEditorSchema(ConfigSchema.getSystemJsonSchema());
            response.getWriter().println(schema.toString());
        } catch (Exception e) {
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            final JsonObject error = new JsonObject();
            error.addProperty("error", e.getMessage());
            response.getWriter().println(error.toString());
        }
    }

    /**
     * Handle HTTP GET requests for the options JSON Schema.
     * Path: /api/spec/options
     */
    public static void serveOptionsSchema(
            final HttpServletRequest request,
            final HttpServletResponse response) throws IOException {

        response.setContentType("application/schema+json");
        response.setCharacterEncoding("UTF-8");

        try {
            final JsonObject schema = prepareEditorSchema(ConfigSchema.getOptionsJsonSchema());
            response.getWriter().println(schema.toString());
        } catch (Exception e) {
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            final JsonObject error = new JsonObject();
            error.addProperty("error", e.getMessage());
            response.getWriter().println(error.toString());
        }
    }

    /**
     * Handle HTTP GET requests for the launch JSON Schema.
     * Path: /api/spec/launch
     */
    public static void serveLaunchSchema(
            final HttpServletRequest request,
            final HttpServletResponse response) throws IOException {

        response.setContentType("application/schema+json");
        response.setCharacterEncoding("UTF-8");

        try {
            final JsonElement schema = ConfigSchema.getLaunchJsonSchema();
            response.getWriter().println(schema.toString());
        } catch (Exception e) {
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            final JsonObject error = new JsonObject();
            error.addProperty("error", e.getMessage());
            response.getWriter().println(error.toString());
        }
    }

    /**
     * Build a self-contained JSON Schema for a module's YAML editor.
     * Combines module-specific parameters with common module fields.
     */
    private static JsonObject buildModuleEditorSchema(final String moduleType, final String moduleName) {
        // Find the module schema
        final JsonArray moduleSchemas = switch (moduleType) {
            case "source" -> ConfigSchema.getSourceJsonSchemas();
            case "transform" -> ConfigSchema.getTransformJsonSchemas();
            case "sink" -> ConfigSchema.getSinkJsonSchemas();
            default -> new JsonArray();
        };

        JsonObject moduleSchema = null;
        for (int i = 0; i < moduleSchemas.size(); i++) {
            final JsonObject candidate = moduleSchemas.get(i).getAsJsonObject();
            final String id = candidate.has("$id") ? candidate.get("$id").getAsString() : "";
            if (id.endsWith("/" + moduleName)) {
                moduleSchema = candidate;
                break;
            }
        }

        if (moduleSchema == null) {
            return null;
        }

        // Extract parameters from $defs and flatten any $ref/$dynamicRef
        JsonObject params;
        if (moduleSchema.has("$defs")
                && moduleSchema.getAsJsonObject("$defs").has("parameters")) {
            params = moduleSchema.getAsJsonObject("$defs").getAsJsonObject("parameters").deepCopy();
            flattenSchemaFully(params);
        } else {
            params = new JsonObject();
            params.addProperty("type", "object");
        }

        // Build flattened strategy schema (inline $defs, remove $ref/$dynamicRef/$dynamicAnchor
        // which are not supported by the YAML language server used by monaco-yaml-inline)
        final JsonElement strategySchema = buildFlattenedStrategySchema();

        // Build wrapper schema
        final JsonObject result = new JsonObject();
        result.addProperty("type", "object");

        final JsonObject properties = new JsonObject();
        properties.add("parameters", params);

        final JsonObject schemaField = new JsonObject();
        schemaField.addProperty("type", "object");
        schemaField.addProperty("description", "Output schema definition (fields, avro, or protobuf)");
        properties.add("schema", schemaField);

        properties.add("strategy", strategySchema);

        final JsonObject tagsField = new JsonObject();
        tagsField.addProperty("type", "array");
        tagsField.addProperty("description", "Tags for categorizing modules");
        final JsonObject tagsItems = new JsonObject();
        tagsItems.addProperty("type", "string");
        tagsField.add("items", tagsItems);
        properties.add("tags", tagsField);

        final JsonObject logsField = new JsonObject();
        logsField.addProperty("type", "array");
        final JsonObject logsItems = new JsonObject();
        logsItems.addProperty("type", "string");
        final JsonArray logsEnum = new JsonArray();
        logsEnum.add("input");
        logsEnum.add("output");
        logsItems.add("enum", logsEnum);
        logsField.add("items", logsItems);
        properties.add("logs", logsField);

        final JsonObject tsField = new JsonObject();
        tsField.addProperty("type", "string");
        tsField.addProperty("description", "Field or attribute to use as event time");
        properties.add("timestampAttribute", tsField);

        final JsonObject failFastField = new JsonObject();
        failFastField.addProperty("type", "boolean");
        failFastField.addProperty("description", "Fail immediately on error");
        properties.add("failFast", failFastField);

        final JsonObject ignoreField = new JsonObject();
        ignoreField.addProperty("type", "boolean");
        ignoreField.addProperty("description", "Skip this module");
        properties.add("ignore", ignoreField);

        result.add("properties", properties);
        result.addProperty("additionalProperties", false);

        return result;
    }

    /**
     * Flatten a JSON Schema in-place: resolve all $ref (local and absolute URI),
     * remove $dynamicRef/$dynamicAnchor/$id/$defs.
     */
    private static void flattenSchemaFully(final JsonObject schema) {
        flattenJsonSchema(schema);
    }

    /**
     * Prepare a raw config JSON Schema for use in the Monaco YAML editor.
     * Strips $schema and $id which the YAML language server (monaco-yaml-inline)
     * does not handle correctly for JSON Schema 2020-12.
     */
    private static JsonObject prepareEditorSchema(final JsonElement raw) {
        if (raw == null || raw.isJsonNull() || !raw.isJsonObject()) {
            final JsonObject fallback = new JsonObject();
            fallback.addProperty("type", "object");
            return fallback;
        }
        final JsonObject schema = raw.getAsJsonObject().deepCopy();
        schema.remove("$schema");
        schema.remove("$id");
        return schema;
    }

    /**
     * Build a flattened strategy schema with all $ref/$defs inlined and
     * $dynamicRef/$dynamicAnchor removed.
     * The YAML language server (used by monaco-yaml-inline) does not support
     * JSON Schema 2020-12 features like $dynamicRef/$dynamicAnchor, and $ref
     * to local $defs within a sub-schema also fails. This method resolves
     * everything into plain JSON Schema that the language server can process.
     */
    private static JsonElement buildFlattenedStrategySchema() {
        final JsonElement raw = ConfigSchema.getStrategyJsonSchema();
        if (raw == null || raw.isJsonNull() || !raw.isJsonObject()) {
            return JsonParser.parseString("{\"type\":\"object\"}");
        }
        final JsonObject original = raw.getAsJsonObject();
        return flattenJsonSchema(original.deepCopy());
    }

    /**
     * Recursively flatten a JSON Schema object by:
     * 1. Resolving $ref to local #/$defs/... by inlining the referenced definition
     * 2. Removing $dynamicRef and $dynamicAnchor (replace with {type: object})
     * 3. Removing $id from sub-schemas (not needed when everything is inlined)
     * 4. Removing $defs after all references are resolved
     */
    private static JsonElement flattenJsonSchema(final JsonObject schema) {
        // Collect $defs first
        final JsonObject defs;
        if (schema.has("$defs") && schema.get("$defs").isJsonObject()) {
            defs = schema.getAsJsonObject("$defs");
        } else {
            defs = new JsonObject();
        }

        // Process the schema
        resolveRefs(schema, defs, 0);

        // Remove top-level $defs and $id (no longer needed)
        schema.remove("$defs");
        schema.remove("$id");

        return schema;
    }

    private static void resolveRefs(final JsonObject obj, final JsonObject defs, final int depth) {
        if (depth > 10) return; // prevent infinite recursion

        // Replace $ref with inlined definition
        if (obj.has("$ref")) {
            final String ref = obj.get("$ref").getAsString();
            obj.remove("$ref");
            if (ref.startsWith("#/$defs/")) {
                // Local $ref: inline from the schema's own $defs
                final String defName = ref.substring("#/$defs/".length());
                if (defs.has(defName)) {
                    final JsonObject defCopy = defs.getAsJsonObject(defName).deepCopy();
                    defCopy.remove("$dynamicAnchor");
                    for (final var entry : defCopy.entrySet()) {
                        if (!obj.has(entry.getKey())) {
                            obj.add(entry.getKey(), entry.getValue());
                        }
                    }
                }
            } else {
                // Absolute URI $ref: look up in ConfigSchema registry and inline
                final JsonElement referenced = ConfigSchema.getSchema(ref);
                if (referenced != null && !referenced.isJsonNull() && referenced.isJsonObject()) {
                    final JsonObject refCopy = referenced.getAsJsonObject().deepCopy();
                    // Flatten the referenced schema recursively
                    flattenJsonSchema(refCopy);
                    for (final var entry : refCopy.entrySet()) {
                        if (!obj.has(entry.getKey())) {
                            obj.add(entry.getKey(), entry.getValue());
                        }
                    }
                }
            }
        }

        // Replace $dynamicRef with simple {type: object} (recursive refs can't be fully inlined)
        if (obj.has("$dynamicRef")) {
            obj.remove("$dynamicRef");
            if (!obj.has("type")) {
                obj.addProperty("type", "object");
            }
        }

        // Remove $dynamicAnchor
        obj.remove("$dynamicAnchor");

        // Remove $id from sub-schemas
        obj.remove("$id");

        // Recurse into properties
        if (obj.has("properties") && obj.get("properties").isJsonObject()) {
            for (final var entry : obj.getAsJsonObject("properties").entrySet()) {
                if (entry.getValue().isJsonObject()) {
                    resolveRefs(entry.getValue().getAsJsonObject(), defs, depth + 1);
                }
            }
        }

        // Recurse into items
        if (obj.has("items") && obj.get("items").isJsonObject()) {
            resolveRefs(obj.getAsJsonObject("items"), defs, depth + 1);
        }

        // Recurse into allOf/anyOf/oneOf
        for (final String keyword : new String[]{"allOf", "anyOf", "oneOf"}) {
            if (obj.has(keyword) && obj.get(keyword).isJsonArray()) {
                for (final JsonElement el : obj.getAsJsonArray(keyword)) {
                    if (el.isJsonObject()) {
                        resolveRefs(el.getAsJsonObject(), defs, depth + 1);
                    }
                }
            }
        }

        // Recurse into if/then/else
        for (final String keyword : new String[]{"if", "then", "else"}) {
            if (obj.has(keyword) && obj.get(keyword).isJsonObject()) {
                resolveRefs(obj.getAsJsonObject(keyword), defs, depth + 1);
            }
        }

        // Recurse into $defs values too (for nested defs)
        if (obj.has("$defs") && obj.get("$defs").isJsonObject()) {
            for (final var entry : obj.getAsJsonObject("$defs").entrySet()) {
                if (entry.getValue().isJsonObject()) {
                    resolveRefs(entry.getValue().getAsJsonObject(), defs, depth + 1);
                }
            }
            // Remove $defs after processing
            obj.remove("$defs");
        }
    }

    private static class ConfigSchema {

        private static final String RESOURCES_JSON_SCHEMA_DIR = "server/schema";
        private static final String ID_PREFIX = "https://mercari.com/"; //module/transform/onnxgen

        private static SchemaRegistry schemaRegistry;
        private static Map<String, String> schemas;

        private ConfigSchema(){};

        public static void load() {
            if(schemas == null || schemaRegistry == null) {
                final URL uri = Thread.currentThread().getContextClassLoader().getResource(RESOURCES_JSON_SCHEMA_DIR);
                schemas = JsonSchemaUtil.loadJsonSchemas(uri.getPath());
                schemaRegistry = JsonSchemaUtil.createSchemaRegistry(schemas);
            }
        }

        public static JsonArray getSourceJsonSchemas() {
            return getModules("source");
        }

        public static JsonArray getTransformJsonSchemas() {
            return getModules("transform");
        }

        public static JsonArray getSinkJsonSchemas() {
            return getModules("sink");
        }

        public static JsonElement getOptionsJsonSchema() {
            return getSchema("server/api/spec/options");
        }

        public static JsonElement getSystemJsonSchema() {
            return getSchema("server/api/spec/system");
        }

        public static JsonElement getLaunchJsonSchema() {
            return getSchema("server/api/spec/launch");
        }

        public static JsonElement getSchemaJsonSchema() {
            return getSchema("pipeline/schema");
        }

        public static JsonElement getCustomSchemaJsonSchema() {
            return getSchema("pipeline/customSchema");
        }

        public static JsonElement getStrategyJsonSchema() {
            return getSchema("pipeline/strategy");
        }

        public static JsonElement getModuleJsonSchema() {
            return getSchema("pipeline/module/module");
        }

        public static JsonElement getSourceJsonSchema() {
            return getSchema("pipeline/module/source");
        }

        public static JsonElement getTransformJsonSchema() {
            return getSchema("pipeline/module/transform");
        }

        public static JsonElement getSinkJsonSchema() {
            return getSchema("pipeline/module/sink");
        }


        public static JsonElement getSelectJsonSchema() {
            return getUtil("select");
        }

        private static JsonArray getModules(String type) {
            return getSchemas("pipeline/module/" + type + "/");
        }

        private static JsonElement getUtil(String name) {
            return getSchema("pipeline/util/pipeline/" + name);
        }

        private static JsonArray getSchemas(String prefix) {
            if(schemas == null) {
                load();
            }
            if(!prefix.startsWith(ID_PREFIX)) {
                prefix = ID_PREFIX + prefix;
            }

            final JsonArray jsonArray = new JsonArray();
            for(final Map.Entry<String, String> entry : schemas.entrySet()) {
                if(entry.getKey().startsWith(prefix)) {
                    final JsonObject jsonObject = JsonParser.parseString(entry.getValue()).getAsJsonObject();
                    jsonArray.add(jsonObject);
                }
            }
            return jsonArray;
        }

        private static JsonElement getSchema(String path) {
            if(schemas == null) {
                load();
            }
            if(!path.startsWith(ID_PREFIX)) {
                path = ID_PREFIX + path;
            }
            if(schemas.containsKey(path)) {
                return JsonParser.parseString(schemas.get(path)).getAsJsonObject();
            }
            return JsonNull.INSTANCE;
        }

    }

}