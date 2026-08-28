package com.mercari.solution.module.action;

import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.Sleeper;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mercari.solution.config.options.DataflowOptions;
import com.mercari.solution.module.Action;
import com.mercari.solution.module.Action.Trigger;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.cloud.google.CloudBuildUtil;
import com.mercari.solution.util.cloud.google.CloudBuildUtil.CloudBuildException;
import org.apache.beam.sdk.options.PipelineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Action service for Cloud Build operations (Cloud Build REST API v1, {@code projects.locations.*}):
 * run a build (a full {@code Build} resource, or the {@code image} + {@code script} shorthand for a
 * one-step build) and wait for it, read or list builds (guards with {@code failWhen} / {@code skipWhen}),
 * wait for builds started elsewhere, cancel a build, and run a build trigger.
 *
 * <p>Idempotency: Cloud Build has neither unique build names nor a client-supplied request id, so a
 * retried bundle would start a second build. With {@code reuseExisting} (default) and deterministic
 * {@code tags}, {@code builds.create} first lists builds carrying all of those tags and adopts one that is
 * queued, working or succeeded (state {@code EXISTS} without wait; {@code payload.adopted = true}).
 * Without tags the submission is not idempotent (a WARN is logged).
 *
 * <p>The envelope payload is the {@code Build} resource JSON as returned by the API (int64 fields such as
 * {@code options.diskSizeGb} stay strings, as the REST JSON types them), plus {@code outputs[]}: the decoded {@code results.buildStepOutputs} (what a
 * step wrote to {@code $BUILDER_OUTPUT/output}; parsed when it is JSON) and {@code output}, the first of them.
 */
@Action.Service(name = "build", operations = {
        "builds.create", "builds.get", "builds.list", "builds.wait", "builds.cancel", "triggers.run"})
public class BuildAction implements ActionService {

    private static final Logger LOG = LoggerFactory.getLogger(BuildAction.class);

    public static final String ENDPOINT_MEMORY_PREFIX = ActionSupport.ENDPOINT_MEMORY_PREFIX;
    /** {@code ${root...}} - group 1 is the root identifier (after optional whitespace / a leading paren). */
    private static final java.util.regex.Pattern PATTERN_TEMPLATE_EXPRESSION =
            java.util.regex.Pattern.compile("\\$\\{\\s*\\(?\\s*([A-Za-z_][A-Za-z0-9_]*)[^}]*}");
    private static final String DEFAULT_LOCATION = "global";
    /** Statuses of a same-tagged build that a retried create adopts instead of starting another one. */
    private static final Set<String> REUSABLE_STATUSES = Set.of("PENDING", "QUEUED", "WORKING", "SUCCESS");
    /** {@code Build} resource fields accepted at the top level of {@code parameters} for builds.create. */
    static final List<String> BUILD_FIELDS = List.of(
            "steps", "source", "images", "artifacts", "substitutions", "options", "timeout", "queueTtl",
            "serviceAccount", "tags", "logsBucket", "availableSecrets", "secrets");

    /** Operations; {@code operation} is the config value (also listed in {@code @Action.Service}). */
    public enum Op {
        create("builds.create"),
        get("builds.get"),
        list("builds.list"),
        wait("builds.wait"),
        cancel("builds.cancel"),
        run("triggers.run");

        public final String operation;

        Op(final String operation) {
            this.operation = operation;
        }

        static Op of(final String operation) {
            for(final Op op : values()) {
                if(op.operation.equals(operation)) {
                    return op;
                }
            }
            throw new IllegalModuleException("Not supported operation: " + operation);
        }
    }

    public enum WaitUntil {
        terminal,
        working,
        none
    }

    public static class Parameters implements Serializable {

        public Op op;

        // common
        public String projectId;
        public String location;
        public String endpoint;

        // target build (get / wait / cancel)
        public String buildId;
        public String jobIdField;

        // builds.list / builds.wait by filter
        public String filter;
        public Integer pageSize;

        // builds.create shorthand: a single step
        public String image;
        public String script;
        public Map<String, String> env;
        public Boolean reuseExisting;
        /** The {@code Build} resource fields of the parameters as JSON text (nested; the instance is serialized into the DoFn). */
        public String build;

        // triggers.run
        public String triggerId;

        // wait
        public Boolean wait;
        public WaitUntil waitUntil;
        public Long timeoutSeconds;
        public Boolean cancelOnTimeout;

        public List<String> validate(final String name, final Trigger trigger) {
            final List<String> errorMessages = new ArrayList<>();
            final String prefix = "action module[" + name + "].parameters.";
            if(projectId == null) {
                errorMessages.add(prefix + "projectId is required (it could not be derived from the pipeline options)");
            }
            final JsonObject buildJson = build == null ? new JsonObject() : JsonParser.parseString(build).getAsJsonObject();
            switch (op) {
                case create -> {
                    final boolean hasSteps = buildJson.has("steps") && buildJson.get("steps").isJsonArray() && !buildJson.getAsJsonArray("steps").isEmpty();
                    if(script == null && !hasSteps) {
                        errorMessages.add(prefix + "steps (a Build steps list) or image + script (a one-step build) is required for builds.create");
                    }
                    if(script != null && hasSteps) {
                        errorMessages.add(prefix + "steps and script are exclusive: use one of them");
                    }
                    if(script != null && image == null) {
                        errorMessages.add(prefix + "image is required with script (the container image the script runs in)");
                    }
                    if(script == null && image != null) {
                        errorMessages.add(prefix + "image is only used with script");
                    }
                    if(buildJson.has("substitutions") && buildJson.get("substitutions").isJsonObject()) {
                        for(final String key : buildJson.getAsJsonObject("substitutions").keySet()) {
                            if(!key.startsWith("_")) {
                                errorMessages.add(prefix + "substitutions keys must start with '_' (user-defined substitutions) but: " + key);
                            }
                        }
                    }
                    if(buildJson.has("options") && buildJson.get("options").isJsonObject()
                            && buildJson.getAsJsonObject("options").has("pool")
                            && (location == null || DEFAULT_LOCATION.equals(location))) {
                        errorMessages.add(prefix + "options.pool (a private pool) requires a regional location (the pool's region), not global");
                    }
                }
                case get, cancel -> {
                    if(buildId == null) {
                        errorMessages.add(prefix + "buildId is required for " + op.operation);
                    }
                }
                case wait -> {
                    if(buildId == null && jobIdField == null && filter == null) {
                        errorMessages.add(prefix + "buildId, jobIdField or filter is required for builds.wait");
                    }
                    if(jobIdField != null && !Trigger.collect.equals(trigger)) {
                        errorMessages.add(prefix + "jobIdField requires trigger: collect");
                    }
                }
                case list -> {
                    if(pageSize != null && pageSize <= 0) {
                        errorMessages.add(prefix + "pageSize must be positive");
                    }
                }
                case run -> {
                    if(triggerId == null) {
                        errorMessages.add(prefix + "triggerId is required for triggers.run");
                    }
                    if(buildJson.has("source") && !buildJson.get("source").isJsonObject()) {
                        errorMessages.add(prefix + "source must be a RepoSource object for triggers.run");
                    }
                }
            }
            if(timeoutSeconds != null && timeoutSeconds <= 0) {
                errorMessages.add(prefix + "timeoutSeconds must be positive");
            }
            return errorMessages;
        }

        public void setDefaults() {
            if(location == null) {
                location = DEFAULT_LOCATION;
            }
            if(wait == null) {
                wait = true;
            }
            if(waitUntil == null) {
                waitUntil = WaitUntil.terminal;
            }
            if(timeoutSeconds == null) {
                timeoutSeconds = 86400L;
            }
            if(cancelOnTimeout == null) {
                // a build started elsewhere is not ours to cancel
                cancelOnTimeout = Op.create.equals(op) || Op.run.equals(op);
            }
            if(reuseExisting == null) {
                reuseExisting = true;
            }
            if(pageSize == null) {
                pageSize = 100;
            }
            if(build == null) {
                build = "{}";
            }
        }
    }

    // ---------------------------------------------------------------------------------------
    // Client abstraction (REST in production, in-memory in tests via endpoint: memory://name)
    // ---------------------------------------------------------------------------------------

    public interface BuildClient {
        /** Returns the operation ({@code metadata.build} holds the created build). */
        JsonObject createBuild(String project, String location, JsonObject build);
        JsonObject getBuild(String project, String location, String buildId);
        List<JsonObject> listBuilds(String project, String location, String filter, int limit);
        JsonObject cancelBuild(String project, String location, String buildId);
        /** Returns the operation ({@code metadata.build} holds the started build). */
        JsonObject runTrigger(String project, String location, String trigger, JsonObject source);
    }

    private static final ActionSupport.MemoryClients<BuildClient> MEMORY_CLIENTS = new ActionSupport.MemoryClients<>("build");

    public static void registerMemoryClient(final String name, final BuildClient client) {
        MEMORY_CLIENTS.register(name, client);
    }

    public static void unregisterMemoryClient(final String name) {
        MEMORY_CLIENTS.unregister(name);
    }

    static BuildClient createClient(final Parameters parameters) {
        final BuildClient memory = MEMORY_CLIENTS.resolve(parameters.endpoint);
        if(memory != null) {
            return memory;
        }
        final CloudBuildUtil util = parameters.endpoint == null
                ? new CloudBuildUtil()
                : new CloudBuildUtil(parameters.endpoint, com.mercari.solution.util.cloud.google.IAMUtil::getTokenValue);
        return new RestBuildClient(util);
    }

    static class RestBuildClient implements BuildClient {

        private final CloudBuildUtil util;

        RestBuildClient(final CloudBuildUtil util) {
            this.util = util;
        }

        @Override
        public JsonObject createBuild(final String project, final String location, final JsonObject build) {
            return util.createBuild(project, location, build);
        }

        @Override
        public JsonObject getBuild(final String project, final String location, final String buildId) {
            return util.getBuild(project, location, buildId);
        }

        @Override
        public List<JsonObject> listBuilds(final String project, final String location, final String filter, final int limit) {
            return util.listBuilds(project, location, filter, limit);
        }

        @Override
        public JsonObject cancelBuild(final String project, final String location, final String buildId) {
            return util.cancelBuild(project, location, buildId);
        }

        @Override
        public JsonObject runTrigger(final String project, final String location, final String trigger, final JsonObject source) {
            return util.runTrigger(project, location, trigger, source);
        }
    }

    // ---------------------------------------------------------------------------------------

    private String name;
    private Trigger trigger;
    private String operation;
    private Parameters parameters;

    private transient BuildClient client;

    @Override
    public void configure(final String name, final Trigger trigger, final String operation, final JsonObject parametersJson, final PipelineOptions options, final Schema inputSchema) {
        this.name = name;
        this.trigger = trigger;
        this.operation = operation;
        // the Build resource fields are nested REST objects: keep them as JSON text (the instance is serialized into the DoFn)
        final JsonObject json = parametersJson == null ? new JsonObject() : parametersJson.deepCopy();
        final JsonObject build = new JsonObject();
        for(final String field : BUILD_FIELDS) {
            if(json.has(field) && !json.get(field).isJsonNull()) {
                build.add(field, json.remove(field));
            }
        }
        this.parameters = new Gson().fromJson(json, Parameters.class);
        this.parameters.build = build.toString();
        this.parameters.op = Op.of(operation);

        if(this.parameters.projectId == null) {
            this.parameters.projectId = DataflowOptions.getProject(options);
        }

        final List<String> errorMessages = this.parameters.validate(name, trigger);
        if(!errorMessages.isEmpty()) {
            throw new IllegalModuleException(errorMessages);
        }
        this.parameters.setDefaults();
    }

    @Override
    public void setup() {
        this.client = createClient(parameters);
    }

    @Override
    public ActionResult execute(final List<MElement> elements) throws Exception {
        final Map<String, Object> data = switch (trigger) {
            case perElement -> elements.getFirst().asPrimitiveMap();
            case once, collect -> Action.createCollectTemplateData(elements);
        };
        final Parameters p = parameters;
        final String project = template(p.projectId, data);
        final String location = template(p.location, data);
        try {
            return execute(p, project, location, data, elements);
        } catch (final CloudBuildException e) {
            final NonRetryableException rejected = rejectedRequest(e);
            if(rejected != null) {
                throw rejected;
            }
            throw e;
        }
    }

    private ActionResult execute(final Parameters p, final String project, final String location, final Map<String, Object> data, final List<MElement> elements) throws Exception {
        return switch (p.op) {
            case create -> create(p, project, location, data);
            case run -> {
                final String triggerId = template(p.triggerId, data);
                final JsonObject build = templateJson(JsonParser.parseString(p.build).getAsJsonObject(), data);
                final JsonObject source = build.has("source") ? build.getAsJsonObject("source") : null;
                final JsonObject operationJson = client.runTrigger(project, location, triggerId, source);
                final JsonObject started = CloudBuildUtil.operationBuild(operationJson);
                if(started == null || CloudBuildUtil.id(started) == null) {
                    throw new IllegalStateException("action module[" + name + "] cloud build did not return a build for trigger " + triggerId + ": " + operationJson);
                }
                LOG.info("action module[{}] ran cloud build trigger {}: build {}", name, triggerId, CloudBuildUtil.id(started));
                yield afterStart(p, project, location, started, false);
            }
            case get -> {
                final JsonObject build = client.getBuild(project, location, template(p.buildId, data));
                yield result(operation, build);
            }
            case list -> {
                final String filter = p.filter == null ? null : template(p.filter, data);
                final List<JsonObject> builds = client.listBuilds(project, location, filter, p.pageSize);
                yield listResult(operation, builds, "DONE");
            }
            case wait -> {
                final List<String> buildIds = new ArrayList<>();
                final boolean collected = Trigger.collect.equals(trigger) && p.jobIdField != null;
                if(collected) {
                    buildIds.addAll(ActionSupport.collectField(elements, p.jobIdField));
                    if(buildIds.isEmpty()) {
                        LOG.info("action module[{}] found no build id in field: {}", name, p.jobIdField);
                        yield ActionResult.of(operation, null, "SKIPPED", null);
                    }
                } else if(p.buildId != null) {
                    buildIds.add(template(p.buildId, data));
                } else {
                    buildIds.add(waitForFilter(p, project, location, template(p.filter, data)));
                }
                final List<JsonObject> completed = WaitUntil.none.equals(p.waitUntil)
                        ? buildIds.stream().map(id -> client.getBuild(project, location, id)).toList()
                        : waitForAll(p, project, location, buildIds, p.waitUntil, false, false);
                if(!collected) {
                    // a single explicit build: the payload is the Build itself (same shape as builds.get)
                    yield result(operation, completed.getFirst());
                }
                yield listResult(operation, completed, "DONE");
            }
            case cancel -> {
                final String buildId = template(p.buildId, data);
                JsonObject build = client.cancelBuild(project, location, buildId);
                LOG.info("action module[{}] cancelled cloud build: {}", name, buildId);
                if(p.wait && !WaitUntil.none.equals(p.waitUntil)) {
                    build = waitForAll(p, project, location, List.of(buildId), WaitUntil.terminal, true, false).getFirst();
                }
                yield result(operation, build);
            }
        };
    }

    private ActionResult create(final Parameters p, final String project, final String location, final Map<String, Object> data) throws Exception {
        final JsonObject build = templateJson(JsonParser.parseString(p.build).getAsJsonObject(), data);
        if(p.script != null) {
            final JsonObject step = new JsonObject();
            step.addProperty("name", template(p.image, data));
            step.addProperty("script", template(p.script, data));
            if(p.env != null && !p.env.isEmpty()) {
                final JsonArray env = new JsonArray();
                p.env.forEach((k, v) -> env.add(k + "=" + template(v, data)));
                step.add("env", env);
            }
            // lets the script use $_SUBSTITUTION variables as environment variables
            step.addProperty("automapSubstitutions", true);
            final JsonArray steps = new JsonArray();
            steps.add(step);
            build.add("steps", steps);
        }

        final List<String> tags = new ArrayList<>();
        for(final JsonElement tag : CloudBuildUtil.array(build, "tags")) {
            tags.add(tag.getAsString());
        }
        if(tags.isEmpty()) {
            LOG.warn("action module[{}] tags is not set; a retried bundle may start a duplicate build (set deterministic tags to make builds.create idempotent)", name);
        } else if(p.reuseExisting) {
            final JsonObject existing = findReusableBuild(project, location, tags);
            if(existing != null) {
                LOG.info("action module[{}] cloud build with tags {} already exists, adopting: {} ({})", name, tags, CloudBuildUtil.id(existing), CloudBuildUtil.status(existing));
                return afterStart(p, project, location, existing, true);
            }
        }

        final JsonObject operationJson = client.createBuild(project, location, build);
        final JsonObject started = CloudBuildUtil.operationBuild(operationJson);
        if(started == null || CloudBuildUtil.id(started) == null) {
            throw new IllegalStateException("action module[" + name + "] cloud build did not return a build: " + operationJson);
        }
        LOG.info("action module[{}] created cloud build: {}", name, CloudBuildUtil.id(started));
        return afterStart(p, project, location, started, false);
    }

    /**
     * Wait (when configured) for a build this step started or adopted, then build the envelope: the
     * state is the observed build status, or {@code EXISTS} for an adopted build reported without waiting.
     */
    private ActionResult afterStart(final Parameters p, final String project, final String location, JsonObject build, final boolean adopted) throws Exception {
        final String state;
        if(p.wait && !WaitUntil.none.equals(p.waitUntil)) {
            if(!CloudBuildUtil.isTerminal(CloudBuildUtil.status(build))) {
                build = waitForAll(p, project, location, List.of(CloudBuildUtil.id(build)), p.waitUntil, false, !adopted).getFirst();
            }
            state = CloudBuildUtil.status(build);
        } else {
            state = adopted ? "EXISTS" : CloudBuildUtil.status(build);
        }
        final Map<String, Object> payload = toPayload(build);
        if(adopted) {
            payload.put("adopted", true);
        }
        return ActionResult.ofValues(operation, CloudBuildUtil.id(build), state, payload);
    }

    /**
     * The newest build carrying every tag, when it is queued, working or succeeded — the one a retry
     * adopts. A newest build that failed (or was cancelled / expired) means the work has to run again,
     * so an older success behind it is deliberately not considered.
     */
    private JsonObject findReusableBuild(final String project, final String location, final List<String> tags) {
        final StringBuilder filter = new StringBuilder();
        for(final String tag : tags) {
            if(!filter.isEmpty()) {
                filter.append(" AND ");
            }
            filter.append("tags=\"").append(tag.replace("\"", "\\\"")).append("\"");
        }
        final List<JsonObject> builds = client.listBuilds(project, location, filter.toString(), 1);
        if(!builds.isEmpty() && REUSABLE_STATUSES.contains(CloudBuildUtil.status(builds.getFirst()))) {
            return builds.getFirst();
        }
        return null;
    }

    /**
     * {@code builds.wait} by {@code filter}: poll the list until a matching build exists (a build
     * started elsewhere may not have been created yet), then return its id. Filters that could match
     * an older build should bound it with {@code create_time>=...}.
     */
    private String waitForFilter(final Parameters p, final String project, final String location, final String filter) throws IOException, InterruptedException {
        final ExponentialBackOff backOff = ActionSupport.createPollBackOff(p.timeoutSeconds);
        while(true) {
            List<JsonObject> builds = null;
            try {
                builds = client.listBuilds(project, location, filter, 1);
            } catch (final RuntimeException e) {
                if(!isTransient(e)) {
                    throw e;
                }
                LOG.info("action module[{}] failed to list cloud builds ({}), retrying", name, e.getMessage());
            }
            if(builds != null && !builds.isEmpty()) {
                return CloudBuildUtil.id(builds.getFirst());
            }
            final long next = backOff.nextBackOffMillis();
            if(next == ExponentialBackOff.STOP) {
                throw new NonRetryableException("action module[" + name + "] found no cloud build matching filter: " + filter
                        + " in " + project + "/" + location + " within timeoutSeconds: " + p.timeoutSeconds);
            }
            LOG.info("action module[{}] waiting for a cloud build matching filter: {}", name, filter);
            Thread.sleep(next);
        }
    }

    /**
     * Poll all builds until each reaches the target ({@code terminal}, or {@code working} which also
     * accepts a terminal state), sharing one backoff and one {@code timeoutSeconds} window.
     * A build that ended other than SUCCESS (or CANCELLED when no cancel was requested) fails the
     * firing as non-retryable with its status detail / failure info / log url attached. Transient
     * poll errors (429 / 5xx / I/O) are retried inside the loop so the module-level {@code retry}
     * does not re-run the whole firing (which could start a second build). Pending builds are
     * cancelled on timeout only when {@code cancelOnTimeout} is set and the builds are ours
     * ({@code own}: started by this step, not adopted or merely waited for).
     */
    private List<JsonObject> waitForAll(final Parameters p, final String project, final String location, final List<String> buildIds, final WaitUntil until, final boolean cancelRequested, final boolean own) throws Exception {
        return ActionSupport.waitForAll(name, "cloud builds", buildIds, p.timeoutSeconds, Sleeper.DEFAULT,
                buildId -> {
                    final JsonObject build = client.getBuild(project, location, buildId);
                    final String status = CloudBuildUtil.status(build);
                    if(CloudBuildUtil.isTerminal(status)) {
                        if("CANCELLED".equals(status) && !cancelRequested) {
                            throw new NonRetryableException("action module[" + name + "] cloud build was cancelled: " + buildId + CloudBuildUtil.describeFailure(build));
                        }
                        if(!"SUCCESS".equals(status) && !"CANCELLED".equals(status)) {
                            throw new NonRetryableException("action module[" + name + "] cloud build " + buildId + " ended " + status + ":" + CloudBuildUtil.describeFailure(build));
                        }
                        return build;
                    } else if(WaitUntil.working.equals(until) && "WORKING".equals(status)) {
                        return build;
                    }
                    return null;
                },
                e -> e instanceof RuntimeException r && isTransient(r),
                pending -> {
                    if(!(p.cancelOnTimeout && own)) {
                        return;
                    }
                    for(final String buildId : pending) {
                        try {
                            client.cancelBuild(project, location, buildId);
                            LOG.warn("action module[{}] cancelled cloud build: {} after timeoutSeconds: {}", name, buildId, p.timeoutSeconds);
                        } catch (final Exception e) {
                            LOG.warn("action module[{}] failed to cancel cloud build: {}: {}", name, buildId, e.getMessage());
                        }
                    }
                },
                pending -> new NonRetryableException("action module[" + name + "] cloud builds: " + pending
                        + " did not reach " + until.name() + " within timeoutSeconds: " + p.timeoutSeconds
                        + (p.cancelOnTimeout && own ? " (cancel requested)" : "")));
    }

    /** 429 / 5xx from the API, or a transport failure (wrapped I/O error) - worth another poll. */
    private static boolean isTransient(final RuntimeException e) {
        if(e instanceof CloudBuildException cbe) {
            return cbe.isRetryable();
        }
        return e instanceof IllegalStateException && e.getCause() instanceof IOException;
    }

    private static ActionResult result(final String operation, final JsonObject build) {
        return ActionResult.ofValues(operation, CloudBuildUtil.id(build), CloudBuildUtil.status(build), toPayload(build));
    }

    private static ActionResult listResult(final String operation, final List<JsonObject> builds, final String state) {
        final Map<String, Object> payload = new LinkedHashMap<>();
        final List<Map<String, Object>> maps = builds.stream().map(BuildAction::toPayload).toList();
        payload.put("builds", maps);
        payload.put("count", maps.size());
        if(!maps.isEmpty()) {
            payload.put("firstBuild", maps.getFirst());
        }
        final String jobId = builds.stream().map(CloudBuildUtil::id).filter(Objects::nonNull).reduce((a, b) -> a + "," + b).orElse(null);
        return ActionResult.ofValues(operation, jobId, state, payload);
    }

    /** The Build resource as payload, plus {@code outputs[]}: decoded {@code results.buildStepOutputs}. */
    static Map<String, Object> toPayload(final JsonObject build) {
        final Map<String, Object> payload = CloudBuildUtil.toPayload(build);
        final List<Object> outputs = decodeOutputs(build);
        if(!outputs.isEmpty()) {
            payload.put("outputs", outputs);
            // the first output as `output`: conditions (failWhen / skipWhen) have no array index syntax
            outputs.stream().filter(Objects::nonNull).findFirst().ifPresent(o -> payload.put("output", o));
        }
        return payload;
    }

    static List<Object> decodeOutputs(final JsonObject build) {
        final List<Object> outputs = new ArrayList<>();
        if(!build.has("results") || !build.get("results").isJsonObject()) {
            return outputs;
        }
        for(final JsonElement element : CloudBuildUtil.array(build.getAsJsonObject("results"), "buildStepOutputs")) {
            if(element.isJsonNull() || element.getAsString().isEmpty()) {
                outputs.add(null);
                continue;
            }
            String text;
            try {
                text = new String(Base64.getDecoder().decode(element.getAsString()), StandardCharsets.UTF_8);
            } catch (final IllegalArgumentException e) {
                text = element.getAsString();
            }
            final String trimmed = text.trim();
            if((trimmed.startsWith("{") && trimmed.endsWith("}")) || (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
                try {
                    outputs.add(CloudBuildUtil.toPayload(JsonParser.parseString(trimmed).getAsJsonObject()));
                    continue;
                } catch (final Exception ignored) {
                    // not an object: keep the text (arrays included)
                }
            }
            outputs.add(text);
        }
        return outputs;
    }

    /**
     * Rejected requests (bad argument, unknown build / trigger, missing permission, precondition)
     * cannot be fixed by re-execution: map them to {@link NonRetryableException} so the module-level
     * {@code retry} is spent on transient errors (429, 5xx) only.
     */
    static NonRetryableException rejectedRequest(final CloudBuildException e) {
        return switch (e.status) {
            case 400, 401, 403, 404, 409, 412 -> new NonRetryableException("cloud build request rejected (" + e.status + "): " + e.getMessage(), e);
            default -> null;
        };
    }

    /** Every string leaf of the JSON expanded as a FreeMarker template with the element data. */
    static JsonObject templateJson(final JsonObject object, final Map<String, Object> data) {
        return ActionSupport.templateJson(object, text -> template(text, data)).getAsJsonObject();
    }

    /**
     * Template expansion that leaves foreign {@code ${...}} alone: Cloud Build substitutions
     * ({@code ${PROJECT_ID}}, {@code ${_TAG}}) and shell parameter expansions ({@code ${VAR}}) share the
     * syntax with FreeMarker, so only expressions whose root identifier is a key of the template data
     * (an element field, {@code args}, {@code size}, ...) are evaluated; every other {@code ${...}} is
     * passed through to Cloud Build verbatim.
     */
    static String template(final String text, final Map<String, Object> data) {
        if(text == null || !TemplateUtil.isTemplateText(text)) {
            return text;
        }
        final java.util.regex.Matcher matcher = PATTERN_TEMPLATE_EXPRESSION.matcher(text);
        final StringBuilder sb = new StringBuilder();
        while(matcher.find()) {
            final String expression = matcher.group();
            final String root = matcher.group(1);
            final String replacement = data.containsKey(root) ? TemplateUtil.executeStrictTemplate(expression, data) : expression;
            matcher.appendReplacement(sb, java.util.regex.Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

}
