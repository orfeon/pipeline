package com.mercari.solution.config;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.List;

/**
 * Rewrites old-format schema declarations in a pipeline config into the new form
 * (docs/developer/schema-redesign.md Phase 5):
 *
 * <ul>
 *   <li>a module top-level {@code schema} moves into {@code parameters.schema}</li>
 *   <li>{@code avro}/{@code protobuf}/{@code useDestinationSchema} (and the deprecated
 *       {@code avroSchema}/{@code protobufDescriptor} aliases) become
 *       {@code encoding}/{@code reference}</li>
 *   <li>the pubsub sink's {@code parameters.useDestinationSchema} becomes
 *       {@code parameters.schema.reference.destination}</li>
 * </ul>
 *
 * The input is never mutated. Declarations the rewrite cannot decide safely
 * (old+new keys mixed, both avro and protobuf declared) are left unchanged with a note.
 * {@link #scanDeprecations} runs the same analysis without producing a config, for
 * surfacing deprecation warnings in validation responses.
 */
public class SchemaConfigUpgrader {

    public record Result(JsonObject config, List<String> changes) {}

    private static final List<String> MODULE_BLOCKS = List.of("sources", "transforms", "sinks");
    private static final List<String> LEGACY_SCHEMA_KEYS =
            List.of("avro", "protobuf", "useDestinationSchema", "avroSchema", "protobufDescriptor");

    public static Result upgrade(final JsonObject config) {
        final JsonObject upgraded = config.deepCopy();
        final List<String> changes = new ArrayList<>();
        for(final String blockName : MODULE_BLOCKS) {
            if(!upgraded.has(blockName) || !upgraded.get(blockName).isJsonArray()) {
                continue;
            }
            final JsonArray modules = upgraded.getAsJsonArray(blockName);
            for(final JsonElement moduleElement : modules) {
                if(!moduleElement.isJsonObject()) {
                    continue;
                }
                upgradeModule(moduleElement.getAsJsonObject(), blockName, changes);
            }
        }
        return new Result(upgraded, changes);
    }

    /** Runs the upgrade analysis and returns the changes it would make, as deprecation warnings. */
    public static List<String> scanDeprecations(final JsonObject config) {
        return upgrade(config).changes();
    }

    private static void upgradeModule(
            final JsonObject module,
            final String blockName,
            final List<String> changes) {

        final String path = blockName + "[" + name(module) + "]";

        // pubsub sink parameter alias for the destination reference
        if("sinks".equals(blockName)
                && "pubsub".equals(asString(module, "module"))
                && module.has("parameters") && module.get("parameters").isJsonObject()) {
            final JsonObject parameters = module.getAsJsonObject("parameters");
            if(Boolean.TRUE.equals(asBoolean(parameters, "useDestinationSchema"))) {
                final JsonObject schema = parameters.has("schema") && parameters.get("schema").isJsonObject()
                        ? parameters.getAsJsonObject("schema") : new JsonObject();
                if(!schema.has("reference")) {
                    final JsonObject reference = new JsonObject();
                    reference.addProperty("destination", true);
                    schema.add("reference", reference);
                    parameters.remove("useDestinationSchema");
                    parameters.add("schema", schema);
                    changes.add(path + ": parameters.useDestinationSchema -> parameters.schema.reference.destination");
                } else {
                    changes.add(path + ": parameters.useDestinationSchema is deprecated but schema.reference already exists — left unchanged");
                }
            }
        }

        // top-level schema moves into parameters.schema
        if(module.has("schema") && module.get("schema").isJsonObject()) {
            final JsonObject parameters;
            if(module.has("parameters") && module.get("parameters").isJsonObject()) {
                parameters = module.getAsJsonObject("parameters");
            } else {
                parameters = new JsonObject();
                module.add("parameters", parameters);
            }
            if(parameters.has("schema")) {
                changes.add(path + ": schema is declared both at the module top level and in parameters.schema — resolve manually");
            } else {
                parameters.add("schema", module.get("schema"));
                module.remove("schema");
                changes.add(path + ": top-level schema -> parameters.schema");
            }
        }

        // rewrite old schema keys wherever a schema block remains
        if(module.has("parameters") && module.get("parameters").isJsonObject()) {
            final JsonObject parameters = module.getAsJsonObject("parameters");
            if(parameters.has("schema") && parameters.get("schema").isJsonObject()) {
                upgradeSchemaBlock(parameters.getAsJsonObject("schema"), path + ".parameters.schema", changes);
            }
        }
        if(module.has("schema") && module.get("schema").isJsonObject()) {
            upgradeSchemaBlock(module.getAsJsonObject("schema"), path + ".schema", changes);
        }
    }

    private static void upgradeSchemaBlock(
            final JsonObject schema,
            final String path,
            final List<String> changes) {

        final boolean hasLegacy = LEGACY_SCHEMA_KEYS.stream().anyMatch(schema::has);
        if(!hasLegacy) {
            return;
        }
        if(schema.has("encoding") || schema.has("reference")) {
            changes.add(path + ": mixes old-format and new-format keys (invalid) — resolve manually");
            return;
        }

        // normalize the deprecated string aliases into their object forms first
        if(schema.has("avroSchema")) {
            final JsonObject avro = schema.has("avro") && schema.get("avro").isJsonObject()
                    ? schema.getAsJsonObject("avro") : new JsonObject();
            if(!avro.has("file")) {
                avro.addProperty("file", schema.get("avroSchema").getAsString());
            }
            schema.add("avro", avro);
            schema.remove("avroSchema");
        }
        if(schema.has("protobufDescriptor")) {
            final JsonObject protobuf = schema.has("protobuf") && schema.get("protobuf").isJsonObject()
                    ? schema.getAsJsonObject("protobuf") : new JsonObject();
            if(!protobuf.has("descriptorFile")) {
                protobuf.addProperty("descriptorFile", schema.get("protobufDescriptor").getAsString());
            }
            schema.add("protobuf", protobuf);
            schema.remove("protobufDescriptor");
        }

        if(schema.has("avro") && schema.has("protobuf")) {
            changes.add(path + ": declares both avro and protobuf (ambiguous in the legacy format) — resolve manually");
            return;
        }

        final JsonObject encoding = new JsonObject();
        final JsonObject reference = new JsonObject();

        if(schema.has("avro") && schema.get("avro").isJsonObject()) {
            final JsonObject avro = schema.getAsJsonObject("avro");
            encoding.addProperty("format", "avro");
            if(avro.has("json")) {
                reference.addProperty("inline", avro.get("json").getAsString());
            } else if(avro.has("file")) {
                reference.addProperty("uri", avro.get("file").getAsString());
            }
            schema.remove("avro");
            changes.add(path + ": avro -> encoding/reference");
        } else if(schema.has("protobuf") && schema.get("protobuf").isJsonObject()) {
            final JsonObject protobuf = schema.getAsJsonObject("protobuf");
            encoding.addProperty("format", "protobuf");
            if(protobuf.has("messageName")) {
                encoding.addProperty("messageName", protobuf.get("messageName").getAsString());
            }
            if(protobuf.has("descriptorFile")) {
                reference.addProperty("uri", protobuf.get("descriptorFile").getAsString());
            }
            schema.remove("protobuf");
            changes.add(path + ": protobuf -> encoding/reference");
        }

        if(schema.has("useDestinationSchema")) {
            if(Boolean.TRUE.equals(asBoolean(schema, "useDestinationSchema"))) {
                reference.addProperty("destination", true);
                changes.add(path + ": useDestinationSchema -> reference.destination");
            }
            schema.remove("useDestinationSchema");
        }

        if(!encoding.entrySet().isEmpty()) {
            schema.add("encoding", encoding);
        }
        if(!reference.entrySet().isEmpty()) {
            schema.add("reference", reference);
        }
    }

    private static String name(final JsonObject module) {
        return asString(module, "name") == null ? "?" : asString(module, "name");
    }

    private static String asString(final JsonObject jsonObject, final String key) {
        if(jsonObject.has(key) && jsonObject.get(key).isJsonPrimitive()) {
            return jsonObject.get(key).getAsString();
        }
        return null;
    }

    private static Boolean asBoolean(final JsonObject jsonObject, final String key) {
        if(jsonObject.has(key) && jsonObject.get(key).isJsonPrimitive()
                && jsonObject.get(key).getAsJsonPrimitive().isBoolean()) {
            return jsonObject.get(key).getAsBoolean();
        }
        return null;
    }

}
