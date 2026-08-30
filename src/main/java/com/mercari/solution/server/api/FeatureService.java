package com.mercari.solution.server.api;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.mercari.solution.config.Config;
import com.mercari.solution.util.FailureUtil;
import com.mercari.solution.util.pipeline.feature.FeaturePlanService;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;

/**
 * {@code POST /api/feature} — compiles a {@code feature} transform spec without running a pipeline
 * ({@code validate --expand}): expanded columns with availability status and lineage, stages, and
 * structured diagnostics.
 *
 * <p>Body (JSON or YAML): either {@code {parameters: {...}, inputSchema?: {fields: [...]}, args?: {...},
 * streaming?: bool}} or a whole pipeline config (the first {@code module: feature} transform is used,
 * or the one selected by {@code name}).
 */
public class FeatureService {

    private static final Logger LOG = LoggerFactory.getLogger(FeatureService.class);

    public static void serve(final HttpServletRequest request, final HttpServletResponse response) throws IOException {
        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");
        try {
            final JsonObject body;
            try (final Reader reader = request.getReader()) {
                body = Config.convertConfigJson(reader, Config.Format.unknown);
            }
            final JsonObject result = FeaturePlanService.validate(body);
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().println(new GsonBuilder().setPrettyPrinting().create().toJson(result));
        } catch (final Throwable e) {
            final String message = FailureUtil.convertThrowableMessage(e);
            LOG.error("feature validate failed: {}", message);
            final JsonObject error = new JsonObject();
            error.addProperty("ok", false);
            error.addProperty("error", message);
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getWriter().println(error);
        }
    }


    /**
     * Dry-run report of every {@code feature} transform in an assembled config: the validate --expand result
     * compiled against the union schema of the step's resolved inputs ({@code outputs} of {@code MPipeline.apply}).
     * Each entry: {@code {name, ok, describe, engineErrors?, error?}}.
     */
    public static com.google.gson.JsonArray describePlans(
            final com.mercari.solution.config.Config config,
            final java.util.Map<String, com.mercari.solution.module.MCollection> outputs) {
        final com.google.gson.JsonArray plans = new com.google.gson.JsonArray();
        if (config == null || config.getTransforms() == null) return plans;
        for (final com.mercari.solution.config.TransformConfig step : config.getTransforms()) {
            if (!"feature".equals(step.getModule()) || step.getParameters() == null) continue;
            final JsonObject request = new JsonObject();
            request.add("parameters", step.getParameters().deepCopy());
            final java.util.List<com.mercari.solution.module.Schema> schemas = new java.util.ArrayList<>();
            if (step.getInputs() != null) {
                for (final String input : step.getInputs()) {
                    final com.mercari.solution.module.MCollection c = outputs == null ? null : outputs.get(input);
                    if (c != null && c.getSchema() != null) schemas.add(c.getSchema());
                }
            }
            if (!schemas.isEmpty()) {
                request.add("inputSchema", com.mercari.solution.util.pipeline.Union.createUnionSchema(schemas).toJsonObject());
            }
            final JsonObject result = FeaturePlanService.validate(request);
            final JsonObject plan = new JsonObject();
            plan.addProperty("name", step.getName());
            plan.addProperty("ok", result.has("ok") && result.get("ok").getAsBoolean());
            if (result.has("describe")) plan.addProperty("describe", result.get("describe").getAsString());
            if (result.has("engineErrors")) plan.add("engineErrors", result.get("engineErrors"));
            if (result.has("error")) plan.add("error", result.get("error"));
            plans.add(plan);
        }
        return plans;
    }

}
