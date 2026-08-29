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

}
