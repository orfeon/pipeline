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
        return describePlans(config, outputs, null);
    }

    /** The report formats of {@link #describePlans}: the text report, the structured plan, or both. */
    public static final java.util.List<String> PLAN_FORMATS = java.util.List.of("text", "json", "both");

    /**
     * {@code format}: {@code text} (default) — {@code describe}, the human-readable report; {@code json} —
     * {@code plan}, the same report as structure ({@code stages}, {@code columns} with status / availability /
     * lineage, {@code diagnostics} as {@code {level, code, location, message}}, {@code audit}, …: what
     * {@code POST /api/feature} returns), for a caller that reads it by program; {@code both}.
     */
    public static com.google.gson.JsonArray describePlans(
            final com.mercari.solution.config.Config config,
            final java.util.Map<String, com.mercari.solution.module.MCollection> outputs,
            final String format) {
        final boolean text = format == null || "text".equals(format) || "both".equals(format);
        final boolean json = "json".equals(format) || "both".equals(format);
        final com.google.gson.JsonArray plans = new com.google.gson.JsonArray();
        if (config == null || config.getTransforms() == null) return plans;
        for (final com.mercari.solution.config.TransformConfig step : config.getTransforms()) {
            if (!"feature".equals(step.getModule()) || step.getParameters() == null) continue;
            // a step the assembly left out is not part of what would run: its plan must not fail the dry run
            if (Boolean.TRUE.equals(step.getIgnore())) continue;
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
            if (text && result.has("describe")) plan.addProperty("describe", result.get("describe").getAsString());
            if (json && result.has("plan")) plan.add("plan", result.get("plan"));
            if (result.has("engineErrors")) plan.add("engineErrors", result.get("engineErrors"));
            // the error diagnostics on their own, whatever the report format: what a failed plan must show first
            if (result.has("plan")) {
                final com.google.gson.JsonArray errors = new com.google.gson.JsonArray();
                for (final com.google.gson.JsonElement m : result.getAsJsonObject("plan").getAsJsonArray("diagnostics")) {
                    final JsonObject d = m.getAsJsonObject();
                    if ("error".equals(d.get("level").getAsString())) {
                        errors.add("[" + d.get("code").getAsString() + "] "
                                + (d.get("location").isJsonNull() ? "" : d.get("location").getAsString() + ": ") + d.get("message").getAsString());
                    }
                }
                if (!errors.isEmpty()) plan.add("errors", errors);
            }
            if (result.has("error")) plan.add("error", result.get("error"));
            plans.add(plan);
        }
        return plans;
    }

    /**
     * Why the plans of a dry run that are not {@code ok} failed, one {@code <step>: <reason>} line each (the
     * compile exception, the engine constraints, the error diagnostics); empty when every plan is ok. A dry run
     * with any such line is a failed dry run: the config must not be launched.
     */
    public static java.util.List<String> planErrors(final com.google.gson.JsonArray plans) {
        final java.util.List<String> errors = new java.util.ArrayList<>();
        for (final com.google.gson.JsonElement e : plans) {
            final JsonObject plan = e.getAsJsonObject();
            if (plan.has("ok") && plan.get("ok").getAsBoolean()) continue;
            final String name = plan.has("name") && !plan.get("name").isJsonNull() ? plan.get("name").getAsString() : "feature";
            final int before = errors.size();
            if (plan.has("error")) errors.add(name + ": " + plan.get("error").getAsString());
            if (plan.has("engineErrors")) {
                plan.getAsJsonArray("engineErrors").forEach(m -> errors.add(name + ": " + m.getAsString()));
            }
            if (plan.has("errors")) {
                plan.getAsJsonArray("errors").forEach(m -> errors.add(name + ": " + m.getAsString()));
            }
            if (errors.size() == before) errors.add(name + ": the feature plan did not compile");
        }
        return errors;
    }

}
