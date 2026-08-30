package com.mercari.solution.server.agent.tool;

import com.google.gson.JsonObject;
import com.mercari.solution.config.Config;
import dev.langchain4j.agent.tool.P;
import dev.langchain4j.agent.tool.Tool;

/** Agent tool: compile a {@code feature} transform spec — wrapper of the MCP tool {@code validate-feature}. */
public class FeatureValidator {

    @Tool(name = "validateFeature", value = """
        Compile a `feature` transform specification without running the pipeline (validate --expand).
        Returns the expanded output columns with their availability status (staticSafe / windowShift /
        runtimeFilter / violation), lineage, evaluation stages, and the compiler's errors / warnings / hints.
        Use it after writing or editing a feature transform, and fix every error it reports before running.
    """)
    public String validate(
            @P(name = "config", description = "Pipeline configuration (YAML) containing a `module: feature` transform, or just that step's parameters block") String config,
            @P(name = "name", description = "Step name of the feature transform when the config contains several", required = false) String name) {

        try {
            final JsonObject body = Config.convertConfigJson(config, Config.Format.unknown);
            final java.util.Map<String, Object> args = body.has("transforms")
                    ? McpToolBridge.args("config", body.toString(), "name", name, "format", "text")
                    : McpToolBridge.args("parameters", (body.has("parameters") ? body.get("parameters") : body).toString(), "format", "text");
            final String result = McpToolBridge.call("validate-feature", args);
            if (!result.startsWith("ERROR")) return "SUCCESS\n" + result;
            final String detail = result.replaceFirst("^ERROR:?\\s*", "");
            // a compile report (describe text) means the spec has errors; anything else is a failure to compile at all
            return detail.startsWith("feature plan ") ? "ERROR: the feature spec has errors\n" + detail : "ERROR: " + detail;
        } catch (final Exception e) {
            return "ERROR: " + e.getMessage();
        }
    }

    public static FeatureValidator create() {
        return new FeatureValidator();
    }

}
