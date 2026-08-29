package com.mercari.solution.server.agent.tool;

import com.google.gson.JsonObject;
import com.mercari.solution.config.Config;
import com.mercari.solution.util.pipeline.feature.FeaturePlanService;
import dev.langchain4j.agent.tool.P;
import dev.langchain4j.agent.tool.Tool;

/** Agent tool: compile a {@code feature} transform spec and return the expansion report. */
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
            final JsonObject request = new JsonObject();
            if (body.has("transforms")) {
                for (final var e : body.entrySet()) request.add(e.getKey(), e.getValue());
            } else if (body.has("parameters")) {
                request.add("parameters", body.get("parameters"));
            } else {
                request.add("parameters", body);
            }
            if (name != null && !name.isBlank()) request.addProperty("name", name);
            final JsonObject result = FeaturePlanService.validate(request);
            if (!result.has("describe")) {
                return "ERROR: " + (result.has("error") ? result.get("error").getAsString() : result.toString());
            }
            final boolean ok = result.get("ok").getAsBoolean();
            return (ok ? "SUCCESS\n" : "ERROR: the feature spec has errors\n") + result.get("describe").getAsString();
        } catch (final Exception e) {
            return "ERROR: " + e.getMessage();
        }
    }

    public static FeatureValidator create() {
        return new FeatureValidator();
    }

}
