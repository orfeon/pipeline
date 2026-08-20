package com.mercari.solution;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mercari.solution.config.Config;
import com.mercari.solution.config.Options;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.util.pipeline.OptionUtil;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

/**
 * HTTP serve mode for the direct-profile container: run the same image as a Cloud Run Service.
 *
 * Serve mode activates when the PORT environment variable is set (Cloud Run Services always set
 * it; Jobs and Worker Pools never do) or when --serve=true is passed explicitly; --serve=false
 * forces batch mode even with PORT set. Implemented on the JDK built-in HTTP server
 * (jdk.httpserver) because the direct image carries no Jetty/servlet classes at runtime.
 *
 * Endpoints:
 * - GET  /healthz — liveness/startup probe.
 * - POST /run     — assemble and run one pipeline with DirectRunner, synchronously; the HTTP
 *                   status reflects the pipeline result the same way the batch exit code does.
 *
 * Per request, "?args.xxx=" query parameters become template args, and the body is either the
 * pipeline data (fixed-config mode — fed to the request source module via the requestBody
 * pipeline option) or the config itself (no fixed config). A Pub/Sub push envelope is unwrapped
 * transparently: message.data becomes the body, message.attributes become template args.
 */
public class MPipelineHttpServer {

    private static final Logger LOG = LoggerFactory.getLogger(MPipelineHttpServer.class);

    private static final String ENV_PORT = "PORT";
    private static final String ENV_CONFIG = "MPIPELINE_CONFIG";
    private static final String ENV_CONFIG_RELOAD = "MPIPELINE_CONFIG_RELOAD";
    private static final String ENV_MAX_CONCURRENCY = "MPIPELINE_MAX_CONCURRENCY";

    private final String[] baseArgs;
    private final String fixedConfigParam;
    private final boolean reloadConfig;
    private final Config.Format format;
    private final String context;
    private final Semaphore running;

    // raw config text fetched at startup; per-request template args require re-parsing anyway,
    // so only the fetch is cached (reloadConfig re-fetches per request instead)
    private String fixedConfigContent;

    private HttpServer server;

    public MPipelineHttpServer(
            final String[] baseArgs,
            final String fixedConfigParam,
            final boolean reloadConfig,
            final Config.Format format,
            final String context,
            final int maxConcurrency) {

        this.baseArgs = baseArgs;
        this.fixedConfigParam = fixedConfigParam;
        this.reloadConfig = reloadConfig;
        this.format = format;
        this.context = context;
        this.running = new Semaphore(maxConcurrency);
    }

    public static boolean isServeMode(final MPipeline.MPipelineOptions options) {
        if(options.getServe() != null) {
            return options.getServe();
        }
        return System.getenv(ENV_PORT) != null;
    }

    public static void serve(final MPipeline.MPipelineOptions options, final String[] args) throws IOException {
        final int port = Optional
                .ofNullable(System.getenv(ENV_PORT))
                .map(Integer::parseInt)
                .orElse(8080);
        final String fixedConfigParam = Optional
                .ofNullable(options.getConfig())
                .orElse(System.getenv(ENV_CONFIG));
        final boolean reload = Boolean.parseBoolean(System.getenv(ENV_CONFIG_RELOAD));
        final int maxConcurrency = Optional
                .ofNullable(System.getenv(ENV_MAX_CONCURRENCY))
                .map(Integer::parseInt)
                .orElse(1);

        final MPipelineHttpServer server = new MPipelineHttpServer(
                args, fixedConfigParam, reload, options.getFormat(), options.getContext(), maxConcurrency);
        server.start(port);

        final CountDownLatch stopped = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.stop();
            stopped.countDown();
        }));
        try {
            stopped.await();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Binds and starts the server. Pass port 0 to pick an ephemeral port (tests). */
    public int start(final int port) throws IOException {
        if(fixedConfigParam != null) {
            // fail fast at deploy when the config resource is unreadable
            this.fixedConfigContent = Config.readContent(fixedConfigParam);
            validateFixedConfig();
        }

        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/healthz", exchange -> respond(exchange, 200, "text/plain", "ok"));
        server.createContext("/run", this::handleRun);
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();

        final int boundPort = server.getAddress().getPort();
        LOG.info("MPipeline HTTP serve mode started on port: {}, fixed config: {}",
                boundPort, fixedConfigParam == null ? "(per-request)" : fixedConfigParam);
        return boundPort;
    }

    public void stop() {
        if(server != null) {
            LOG.info("Stopping MPipeline HTTP server");
            server.stop(1);
        }
    }

    // a config may legitimately require per-request args without defaults, so startup
    // validation only warns
    private void validateFixedConfig() {
        if(fixedConfigContent == null) {
            return;
        }
        try {
            Config.parse(fixedConfigContent, context, format, baseTemplateArgs());
            LOG.info("fixed config validated at startup");
        } catch (final Throwable e) {
            LOG.warn("fixed config failed startup validation (it may require per-request args): {}", e.getMessage());
        }
    }

    private void handleRun(final HttpExchange exchange) throws IOException {
        if(!"POST".equals(exchange.getRequestMethod()) && !"GET".equals(exchange.getRequestMethod())) {
            respondJson(exchange, 405, errorJson("INVALID", "method not allowed: " + exchange.getRequestMethod()));
            return;
        }
        if(!running.tryAcquire()) {
            respondJson(exchange, 429, errorJson("BUSY", "another pipeline is running on this instance"));
            return;
        }
        try {
            final Map<String, String> queryParams = parseQuery(exchange.getRequestURI().getRawQuery());
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            if(body.isBlank()) {
                body = null;
            }

            final Map<String, String> templateArgs = baseTemplateArgs();

            // Pub/Sub push envelope: message.data becomes the body, attributes become args
            body = unwrapPubSubPush(body, templateArgs);

            for(final Map.Entry<String, String> entry : queryParams.entrySet()) {
                if(entry.getKey().startsWith("args.")) {
                    templateArgs.put(entry.getKey().substring("args.".length()), entry.getValue());
                }
            }

            // config and data resolution: with a config fixed at deploy time (or given by the
            // config query param), the body is the request source data; otherwise it is the config
            final String configContent;
            final String requestBody;
            if(fixedConfigParam != null) {
                configContent = reloadConfig ? Config.readContent(fixedConfigParam) : fixedConfigContent;
                requestBody = body;
            } else if(queryParams.containsKey("config")) {
                configContent = Config.readContent(queryParams.get("config"));
                requestBody = body;
            } else if(body != null) {
                configContent = body;
                requestBody = null;
            } else {
                respondJson(exchange, 400, errorJson("INVALID",
                        "no config: fix a config at deploy time (--config or MPIPELINE_CONFIG), pass ?config=, or POST the config as the body"));
                return;
            }
            if(configContent == null) {
                respondJson(exchange, 400, errorJson("INVALID", "config resource returned no content"));
                return;
            }

            final String runContext = queryParams.getOrDefault("context", context);
            final Config.Format runFormat = queryParams.containsKey("format")
                    ? Config.Format.valueOf(queryParams.get("format"))
                    : format;

            final Instant startedAt = Instant.now();
            final Config config = Config.parse(configContent, runContext, runFormat, templateArgs);
            if(Optional.ofNullable(config.getEmpty()).orElse(false)) {
                respondJson(exchange, 200, resultJson("EMPTY", startedAt, Instant.now()));
                return;
            }

            // fresh options per run: Options.setOptions mutates them from the config
            final MPipeline.MPipelineOptions runOptions = PipelineOptionsFactory
                    .fromArgs(OptionUtil.filterPipelineArgs(baseArgs))
                    .as(MPipeline.MPipelineOptions.class);
            runOptions.setRequestBody(requestBody);
            Options.setOptions(runOptions, config.getOptions());

            final Pipeline pipeline = Pipeline.create(runOptions);
            final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);
            for(final Map.Entry<String, MCollection> entry : outputs.entrySet()) {
                if(!entry.getKey().endsWith(".failures")) {
                    LOG.info("output: {}, schema: {}", entry.getKey(), entry.getValue().getSchema());
                }
            }
            final PipelineResult result = pipeline.run();
            final PipelineResult.State state = result.waitUntilFinish();

            final JsonObject response = resultJson(
                    Optional.ofNullable(state).map(Enum::name).orElse("UNKNOWN"), startedAt, Instant.now());
            respondJson(exchange, PipelineResult.State.DONE.equals(state) ? 200 : 500, response);

        } catch (final IllegalModuleException e) {
            LOG.error("invalid config for /run request", e);
            respondJson(exchange, 400, errorJson("INVALID", e.getMessage()));
        } catch (final Throwable e) {
            LOG.error("pipeline failed for /run request", e);
            respondJson(exchange, 500, errorJson("FAILED", e.getMessage()));
        } finally {
            running.release();
        }
    }

    private Map<String, String> baseTemplateArgs() {
        return new HashMap<>(OptionUtil
                .filterConfigArgs(baseArgs)
                .getOrDefault("args", new HashMap<>()));
    }

    /**
     * Detects a Pub/Sub push envelope ({"message": {"data": ..., "attributes": ...}, "subscription": ...})
     * and returns the decoded message data as the effective body, merging attributes into the
     * template args (message attributes are the most request-specific, so they win). Any other
     * body is returned unchanged.
     */
    static String unwrapPubSubPush(final String body, final Map<String, String> templateArgs) {
        if(body == null) {
            return null;
        }
        final JsonElement json;
        try {
            json = JsonParser.parseString(body);
        } catch (final Throwable e) {
            return body;
        }
        if(!json.isJsonObject()
                || !json.getAsJsonObject().has("subscription")
                || !json.getAsJsonObject().has("message")
                || !json.getAsJsonObject().get("message").isJsonObject()) {
            return body;
        }
        final JsonObject message = json.getAsJsonObject().getAsJsonObject("message");
        if(message.has("attributes") && message.get("attributes").isJsonObject()) {
            for(final Map.Entry<String, JsonElement> entry : message.getAsJsonObject("attributes").entrySet()) {
                if(entry.getValue().isJsonPrimitive()) {
                    templateArgs.put(entry.getKey(), entry.getValue().getAsString());
                }
            }
        }
        if(message.has("data") && message.get("data").isJsonPrimitive()) {
            return new String(Base64.getDecoder().decode(message.get("data").getAsString()), StandardCharsets.UTF_8);
        }
        return null;
    }

    private static Map<String, String> parseQuery(final String rawQuery) {
        final Map<String, String> params = new HashMap<>();
        if(rawQuery == null || rawQuery.isEmpty()) {
            return params;
        }
        for(final String pair : rawQuery.split("&")) {
            final int index = pair.indexOf('=');
            if(index <= 0) {
                continue;
            }
            final String key = URLDecoder.decode(pair.substring(0, index), StandardCharsets.UTF_8);
            final String value = URLDecoder.decode(pair.substring(index + 1), StandardCharsets.UTF_8);
            params.put(key, value);
        }
        return params;
    }

    private static JsonObject resultJson(final String state, final Instant startedAt, final Instant finishedAt) {
        final JsonObject json = new JsonObject();
        json.addProperty("state", state);
        json.addProperty("startedAt", startedAt.toString());
        json.addProperty("finishedAt", finishedAt.toString());
        json.addProperty("durationMillis", finishedAt.toEpochMilli() - startedAt.toEpochMilli());
        return json;
    }

    private static JsonObject errorJson(final String state, final String message) {
        final JsonObject json = new JsonObject();
        json.addProperty("state", state);
        json.addProperty("error", Optional.ofNullable(message).orElse(""));
        return json;
    }

    private static void respondJson(final HttpExchange exchange, final int status, final JsonObject body) throws IOException {
        respond(exchange, status, "application/json", body.toString());
    }

    private static void respond(final HttpExchange exchange, final int status, final String contentType, final String body) throws IOException {
        final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", contentType + "; charset=utf-8");
        exchange.sendResponseHeaders(status, bytes.length);
        try(final OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
        exchange.close();
    }

}
