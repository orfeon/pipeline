package com.mercari.solution.server.api;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.config.Config;
import com.mercari.solution.server.launch.CloudRunJobLauncher;
import com.mercari.solution.server.launch.CloudRunWorkerPoolLauncher;
import com.mercari.solution.server.launch.ConfigStager;
import com.mercari.solution.server.launch.DataflowFlexTemplateLauncher;
import com.mercari.solution.server.launch.DataflowInProcessLauncher;
import com.mercari.solution.server.launch.DataprocServerlessLauncher;
import com.mercari.solution.server.launch.LaunchDefaults;
import com.mercari.solution.server.launch.LaunchRequest;
import com.mercari.solution.server.launch.Launcher;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.util.FailureUtil;
import com.mercari.solution.util.cloud.google.CloudRunUtil;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * {@code POST /api/launch}: submit a config to a runner / execution environment.
 * <pre>
 * { "config": "...", "args": "..." | {...},
 *   "launch": { "runner": "direct", "environment": "cloudRunJob", "parameters": {...}, "args": {...} } }
 * </pre>
 * Dispatches on {@code runner/environment} to a registered {@link Launcher}; when {@code environment}
 * is omitted the runner's default environment is used. Legacy runner names ({@code dataflowTemplate})
 * are still accepted.
 */
public class LaunchService {

    private static final Logger LOG = LoggerFactory.getLogger(LaunchService.class);

    private static final String HEADER_NAME_USER_EMAIL = "X-Goog-Authenticated-User-Email";

    private static final Map<String, String> LEGACY_RUNNERS = Map.of(
            "dataflowTemplate", "dataflow/flexTemplate");

    private static final Map<String, Launcher> LAUNCHERS = register(launchers());

    private static List<Launcher> launchers() {
        final CloudRunUtil cloudRun = new CloudRunUtil();
        final ConfigStager stager = new ConfigStager();
        return List.of(
                new DataflowFlexTemplateLauncher(),
                new DataflowInProcessLauncher(),
                new CloudRunJobLauncher(cloudRun, stager),
                new CloudRunWorkerPoolLauncher(cloudRun, stager),
                new CloudRunJobLauncher("prism", cloudRun, stager),
                new CloudRunWorkerPoolLauncher("prism", cloudRun, stager),
                new DataprocServerlessLauncher());
    }

    private static Map<String, Launcher> register(final List<Launcher> launchers) {
        final Map<String, Launcher> map = new LinkedHashMap<>();
        for(final Launcher launcher : launchers) {
            map.put(launcher.key(), launcher);
            if(launcher.isDefaultEnvironment()) {
                map.put(launcher.runner(), launcher);
            }
        }
        return map;
    }

    /** Registered {@code runner/environment} keys (for the schema consistency test). */
    public static List<String> launcherKeys() {
        return LAUNCHERS.values().stream().map(Launcher::key).distinct().toList();
    }

    public static Launcher findLauncher(final String runner, final String environment) {
        if(runner == null || runner.isBlank()) {
            throw new IllegalArgumentException("request parameter launch must have runner property");
        }
        final String legacy = LEGACY_RUNNERS.get(runner);
        final String key = legacy != null ? legacy
                : environment == null || environment.isBlank() ? runner : runner + "/" + environment;
        final Launcher launcher = LAUNCHERS.get(key);
        if(launcher == null) {
            throw new IllegalArgumentException("Not supported launch target: " + key + " (available: " + launcherKeys() + ")");
        }
        return launcher;
    }

    public static void serve(
            final HttpServletRequest request,
            final HttpServletResponse response) throws IOException {

        final String userEmail = request.getHeader(HEADER_NAME_USER_EMAIL);

        final long startMillis = Instant.now().toEpochMilli();
        try(final Reader reader = request.getReader()) {
            final JsonObject jsonObject = Config.convertConfigJson(reader, Config.Format.unknown);

            if (!jsonObject.has("config")) {
                throw new IllegalArgumentException("request parameter config is not found");
            }
            final String configText = jsonObject.get("config").getAsString();
            final String argsText;
            if (jsonObject.has("args") && !jsonObject.get("args").isJsonNull()) {
                final JsonElement args = jsonObject.get("args");
                argsText = args.isJsonPrimitive() ? args.getAsString() : args.toString();
            } else {
                argsText = null;
            }

            if (!jsonObject.has("launch") || !jsonObject.get("launch").isJsonObject()) {
                throw new IllegalArgumentException("request parameter launch is not found");
            }
            final JsonObject launch = jsonObject.getAsJsonObject("launch");

            launch(configText, argsText, launch, response, userEmail);
        } catch (final Throwable e) {
            final long endMillis = Instant.now().toEpochMilli();
            response.getWriter().println(errorResponse("server", endMillis - startMillis, e));
        }
    }

    private static void launch(
            final String configText,
            final String argsText,
            final JsonObject launch,
            final HttpServletResponse response,
            final String userEmail) throws IOException {

        final long startMillis = Instant.now().toEpochMilli();
        try {
            final JsonObject job = launchJob(configText, argsText, launch, userEmail);

            final long endMillis = Instant.now().toEpochMilli();
            final JsonObject responseJson = new JsonObject();
            responseJson.addProperty("type", "launch");
            responseJson.addProperty("status", "ok");
            responseJson.addProperty("millis", (endMillis - startMillis));
            responseJson.add("job", job);
            response.getWriter().println(responseJson);
        } catch (final Throwable e) {
            final long endMillis = Instant.now().toEpochMilli();
            response.getWriter().println(errorResponse("pipeline", endMillis - startMillis, e));
        }
    }

    /**
     * Submits a config to a launch target and returns the {@code job} object (see {@link LaunchResult#job}).
     * Shared by the REST endpoint, the MCP {@code launch-pipeline} tool and the agent's {@code launchPipeline}.
     *
     * @param launch {@code {runner, environment?, parameters?: {...}, args?: {...}}}; {@code launch.args}
     *               overrides {@code argsText}
     * @throws IllegalArgumentException for user errors (unknown target, unresolved project / region / job, ...)
     */
    public static JsonObject launchJob(
            final String configText,
            final String argsText,
            final JsonObject launch,
            final String userEmail) throws Exception {

        String configContent = null;
        try {
            // launch.args (JSON object from the modal) overrides the request-level args text.
            final JsonObject launchArgs = launch.has("args") && launch.get("args").isJsonObject()
                    ? launch.getAsJsonObject("args") : null;
            final Config config = launchArgs != null
                    ? Config.load(configText, null, Config.Format.unknown, argsMap(launchArgs))
                    : Config.load(configText, null, Config.Format.unknown, argsText);
            configContent = config.getContent();
            // a placeholder that survives substitution would reach the job as the literal text "${args.x}"
            final java.util.List<String> unresolved = Config.unresolvedArgs(configContent);
            if(!unresolved.isEmpty()) {
                throw new IllegalArgumentException("unresolved template arguments " + unresolved
                        + ": pass them in args (launch.args / --args.<name>) or define defaults under the config's args. "
                        + "Placeholders must be written as ${args.<name>}; a bare ${<name>} is not substituted");
            }

            final String runner = launch.has("runner") && !launch.get("runner").isJsonNull()
                    ? launch.get("runner").getAsString() : null;
            final String environment = launch.has("environment") && !launch.get("environment").isJsonNull()
                    ? launch.get("environment").getAsString() : null;
            final Launcher launcher = findLauncher(runner, environment);

            final JsonObject parameters = launch.has("parameters") && launch.get("parameters").isJsonObject()
                    ? launch.getAsJsonObject("parameters") : new JsonObject();
            final LaunchRequest launchRequest = new LaunchRequest(
                    config, parameters, launchArgs, userEmail, LaunchDefaults.get());

            final JsonObject job = launcher.launch(launchRequest);
            log(userEmail, launcher.key(), true, configContent, null);
            return job;
        } catch (final Throwable e) {
            log(userEmail, "Launch", false, configContent, FailureUtil.convertThrowableMessage(e));
            throw e;
        }
    }

    /** The message a caller should show for a launch failure: user errors plainly, others with their cause chain. */
    public static String launchErrorMessage(final Throwable e) {
        return isUserError(e) ? userMessage(e) : FailureUtil.convertThrowableMessage(e);
    }

    /**
     * Errors the user can act on (config validation, unresolved project/region/job, a missing Cloud Run
     * job, a 4xx from the target API) are reported as plain messages; anything unexpected keeps the
     * stack trace so it can be diagnosed.
     */
    /** JSON args as template args: string values as-is (not JSON-quoted), other values as their JSON text. */
    public static Map<String, String> argsMap(final JsonObject args) {
        return Config.templateArgs(args);
    }

    private static JsonObject errorResponse(final String module, final long millis, final Throwable e) {
        final JsonObject responseJson = new JsonObject();
        responseJson.addProperty("type", "launch");
        responseJson.addProperty("status", "error");
        responseJson.addProperty("millis", millis);
        final JsonObject error = new JsonObject();
        if(e instanceof IllegalModuleException ime) {
            error.addProperty("name", ime.name == null ? "" : ime.name);
            error.addProperty("module", ime.module == null ? module : ime.module);
            final JsonArray messages = new JsonArray();
            ime.errorMessages.forEach(messages::add);
            error.add("messages", messages);
        } else {
            error.addProperty("name", "");
            error.addProperty("module", module);
            error.addProperty("message", isUserError(e) ? userMessage(e) : FailureUtil.convertThrowableMessage(e));
        }
        responseJson.add("error", error);
        return responseJson;
    }

    private static boolean isUserError(final Throwable e) {
        return e instanceof IllegalArgumentException
                || (e instanceof CloudRunUtil.CloudRunException cre && cre.status >= 400 && cre.status < 500);
    }

    private static String userMessage(final Throwable e) {
        final StringBuilder sb = new StringBuilder(e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
        Throwable cause = e.getCause();
        while(cause != null && cause.getMessage() != null) {
            sb.append("\ncaused by: ").append(cause.getMessage());
            cause = cause.getCause();
        }
        return sb.toString();
    }

    private static void log(
            final String userEmail,
            final String type,
            final boolean succeeded,
            final String configText,
            final String errorMessage) {

        LOG.info("mercari-pipeline-server: user={}, type={}, succeeded={}, config={}, error={}",
                Optional.ofNullable(userEmail).orElse("unknown"),
                type,
                succeeded,
                configText,
                errorMessage);
    }

}
