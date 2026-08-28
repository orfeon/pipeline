package com.mercari.solution.module.action;

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
import com.mercari.solution.util.cloud.google.vertexai.VertexAiUtil;
import com.mercari.solution.util.cloud.google.vertexai.VertexAiUtil.VertexAiException;
import org.apache.beam.sdk.options.PipelineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * Action service for Vertex AI generative AI operations (Vertex AI REST API v1, {@code projects.locations.*}):
 * submit a Gemini batch prediction job ({@code batchPredictionJobs.create}, GCS / BigQuery input and output)
 * and wait for it, read or list jobs (guards with {@code failWhen} / {@code skipWhen}), wait for jobs
 * submitted elsewhere, cancel a job, and run a single {@code generateContent} call from the control plane
 * ({@code models.generateContent}: summarize / classify the triggering control records, e.g. a failure list).
 *
 * <p>Idempotency: a batch prediction job has no client-supplied id, so a retried bundle would submit a
 * second job. With {@code reuseExisting} (default) and a deterministic {@code displayName},
 * {@code batchPredictionJobs.create} first lists jobs with that display name and adopts one that is
 * queued, running or succeeded (state {@code EXISTS} without wait; {@code payload.adopted = true}).
 * Without a display name the submission is not idempotent (a WARN is logged).
 *
 * <p>The envelope payload is the resource JSON as returned by the API (the {@code BatchPredictionJob}, or
 * the {@code GenerateContentResponse} plus {@code text} — the first candidate's text — and {@code json},
 * the parsed text when the response is JSON). int64 fields (e.g. {@code completionStats.successfulCount})
 * stay strings, as the REST JSON types them.
 */
@Action.Service(name = "vertexai", operations = {
        "batchPredictionJobs.create", "batchPredictionJobs.get", "batchPredictionJobs.list",
        "batchPredictionJobs.wait", "batchPredictionJobs.cancel", "models.generateContent"})
public class VertexAiAction implements ActionService {

    private static final Logger LOG = LoggerFactory.getLogger(VertexAiAction.class);

    /** States of a same-named job that a retried create adopts instead of submitting another one. */
    private static final Set<String> REUSABLE_STATES = Set.of(
            "JOB_STATE_QUEUED", "JOB_STATE_PENDING", "JOB_STATE_RUNNING", "JOB_STATE_PAUSED", "JOB_STATE_UPDATING",
            "JOB_STATE_SUCCEEDED");
    /** {@code BatchPredictionJob} resource fields accepted at the top level of {@code parameters}. */
    static final List<String> JOB_FIELDS = List.of(
            "inputConfig", "outputConfig", "instanceConfig", "modelParameters", "dedicatedResources", "serviceAccount",
            "encryptionSpec", "labels");
    /** {@code GenerateContentRequest} fields accepted at the top level of {@code parameters}. */
    static final List<String> REQUEST_FIELDS = List.of(
            "contents", "systemInstruction", "generationConfig", "tools", "toolConfig", "safetySettings", "cachedContent", "labels");

    /** Operations; {@code operation} is the config value (also listed in {@code @Action.Service}). */
    public enum Op {
        create("batchPredictionJobs.create"),
        get("batchPredictionJobs.get"),
        list("batchPredictionJobs.list"),
        wait("batchPredictionJobs.wait"),
        cancel("batchPredictionJobs.cancel"),
        generateContent("models.generateContent");

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
        running,
        none
    }

    public static class Parameters implements Serializable {

        public Op op;

        // common
        public String projectId;
        public String location;
        public String endpoint;
        public String model;

        // target job (get / wait / cancel)
        public String jobId;
        public String jobIdField;

        // batchPredictionJobs.list / batchPredictionJobs.wait by filter
        public String filter;
        public Integer pageSize;

        // batchPredictionJobs.create
        public String displayName;
        public Boolean reuseExisting;
        public Boolean failOnPartial;
        /** The {@code BatchPredictionJob} / {@code GenerateContentRequest} fields of the parameters as JSON text (nested; the instance is serialized into the DoFn). */
        public String body;

        // models.generateContent shorthand
        public String prompt;
        public String system;
        public String responseSchema;

        // wait
        public Boolean wait;
        /** Kept as text: Gson maps an unknown enum constant to null, which would silently become {@code terminal}. */
        public String waitUntil;
        public WaitUntil until;
        public Long timeoutSeconds;
        public Boolean cancelOnTimeout;

        public List<String> validate(final String name, final Trigger trigger) {
            final List<String> errorMessages = new ArrayList<>();
            final String prefix = "action module[" + name + "].parameters.";
            if(projectId == null) {
                errorMessages.add(prefix + "projectId is required (it could not be derived from the pipeline options)");
            }
            if(!Op.generateContent.equals(op)) {
                if(location == null) {
                    errorMessages.add(prefix + "location is required for " + op.operation + " (a regional location, e.g. us-central1)");
                } else if(VertexAiUtil.LOCATION_GLOBAL.equals(location)) {
                    errorMessages.add(prefix + "location must be a regional location for " + op.operation + " (batch prediction jobs do not support global)");
                }
            }
            final JsonObject bodyJson = body == null ? new JsonObject() : JsonParser.parseString(body).getAsJsonObject();
            switch (op) {
                case create -> {
                    if(model == null) {
                        errorMessages.add(prefix + "model is required for batchPredictionJobs.create");
                    } else if(model.startsWith("endpoints/") || model.contains("/endpoints/")) {
                        errorMessages.add(prefix + "model must be a model resource for batchPredictionJobs.create (publishers/google/models/... or a tuned projects/.../models/...), not an endpoint");
                    }
                    final JsonObject inputConfig = VertexAiUtil.object(bodyJson, "inputConfig");
                    if(inputConfig == null) {
                        errorMessages.add(prefix + "inputConfig is required for batchPredictionJobs.create (gcsSource.uris or bigquerySource.inputUri)");
                    } else if(VertexAiUtil.object(inputConfig, "gcsSource") == null && VertexAiUtil.object(inputConfig, "bigquerySource") == null) {
                        errorMessages.add(prefix + "inputConfig requires gcsSource or bigquerySource");
                    }
                    final JsonObject outputConfig = VertexAiUtil.object(bodyJson, "outputConfig");
                    if(outputConfig == null) {
                        errorMessages.add(prefix + "outputConfig is required for batchPredictionJobs.create (gcsDestination.outputUriPrefix or bigqueryDestination.outputUri)");
                    } else if(VertexAiUtil.object(outputConfig, "gcsDestination") == null && VertexAiUtil.object(outputConfig, "bigqueryDestination") == null) {
                        errorMessages.add(prefix + "outputConfig requires gcsDestination or bigqueryDestination");
                    }
                }
                case get, cancel -> {
                    if(jobId == null) {
                        errorMessages.add(prefix + "jobId is required for " + op.operation);
                    }
                }
                case wait -> {
                    if(jobId == null && jobIdField == null && filter == null) {
                        errorMessages.add(prefix + "jobId, jobIdField or filter is required for batchPredictionJobs.wait");
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
                case generateContent -> {
                    if(model == null) {
                        errorMessages.add(prefix + "model is required for models.generateContent");
                    }
                    final boolean hasContents = bodyJson.has("contents") && bodyJson.get("contents").isJsonArray() && !bodyJson.getAsJsonArray("contents").isEmpty();
                    if(prompt == null && !hasContents) {
                        errorMessages.add(prefix + "prompt or contents is required for models.generateContent");
                    }
                    if(prompt != null && hasContents) {
                        errorMessages.add(prefix + "prompt and contents are exclusive: use one of them");
                    }
                    if(system != null && bodyJson.has("systemInstruction")) {
                        errorMessages.add(prefix + "system and systemInstruction are exclusive: use one of them");
                    }
                    if(responseSchema != null) {
                        final JsonObject generationConfig = VertexAiUtil.object(bodyJson, "generationConfig");
                        if(generationConfig != null && (generationConfig.has("responseSchema") || generationConfig.has("responseJsonSchema"))) {
                            errorMessages.add(prefix + "responseSchema and generationConfig.responseSchema are exclusive: use one of them");
                        }
                    }
                }
            }
            if(timeoutSeconds != null && timeoutSeconds <= 0) {
                errorMessages.add(prefix + "timeoutSeconds must be positive");
            }
            if(waitUntil != null && Arrays.stream(WaitUntil.values()).noneMatch(w -> w.name().equals(waitUntil))) {
                errorMessages.add(prefix + "waitUntil must be one of " + Arrays.toString(WaitUntil.values()) + " but: " + waitUntil);
            }
            return errorMessages;
        }

        public void setDefaults() {
            if(location == null) {
                location = VertexAiUtil.LOCATION_GLOBAL;
            }
            if(wait == null) {
                wait = true;
            }
            if(waitUntil == null) {
                waitUntil = WaitUntil.terminal.name();
            }
            until = WaitUntil.valueOf(waitUntil);
            if(timeoutSeconds == null) {
                timeoutSeconds = 86400L;
            }
            if(cancelOnTimeout == null) {
                // a job submitted elsewhere is not ours to cancel
                cancelOnTimeout = Op.create.equals(op);
            }
            if(reuseExisting == null) {
                reuseExisting = true;
            }
            if(failOnPartial == null) {
                failOnPartial = false;
            }
            if(pageSize == null) {
                pageSize = 100;
            }
            if(body == null) {
                body = "{}";
            }
        }
    }

    // ---------------------------------------------------------------------------------------
    // Client abstraction (REST in production, in-memory in tests via endpoint: memory://name)
    // ---------------------------------------------------------------------------------------

    public interface VertexAiClient {
        JsonObject generateContent(String project, String location, String model, JsonObject request);
        /** Returns the created {@code BatchPredictionJob} ({@code name}, {@code state}). */
        JsonObject createBatchPredictionJob(String project, String location, JsonObject job);
        JsonObject getBatchPredictionJob(String project, String location, String jobId);
        List<JsonObject> listBatchPredictionJobs(String project, String location, String filter, int limit);
        JsonObject cancelBatchPredictionJob(String project, String location, String jobId);
    }

    private static final ActionSupport.MemoryClients<VertexAiClient> MEMORY_CLIENTS = new ActionSupport.MemoryClients<>("vertexai");

    public static void registerMemoryClient(final String name, final VertexAiClient client) {
        MEMORY_CLIENTS.register(name, client);
    }

    public static void unregisterMemoryClient(final String name) {
        MEMORY_CLIENTS.unregister(name);
    }

    static VertexAiClient createClient(final Parameters parameters) {
        final VertexAiClient memory = MEMORY_CLIENTS.resolve(parameters.endpoint);
        if(memory != null) {
            return memory;
        }
        final VertexAiUtil util = parameters.endpoint == null
                ? new VertexAiUtil()
                : new VertexAiUtil(parameters.endpoint, com.mercari.solution.util.cloud.google.IAMUtil::getTokenValue);
        return new RestVertexAiClient(util);
    }

    static class RestVertexAiClient implements VertexAiClient {

        private final VertexAiUtil util;

        RestVertexAiClient(final VertexAiUtil util) {
            this.util = util;
        }

        @Override
        public JsonObject generateContent(final String project, final String location, final String model, final JsonObject request) {
            return util.generateContent(project, location, model, request);
        }

        @Override
        public JsonObject createBatchPredictionJob(final String project, final String location, final JsonObject job) {
            return util.createBatchPredictionJob(project, location, job);
        }

        @Override
        public JsonObject getBatchPredictionJob(final String project, final String location, final String jobId) {
            return util.getBatchPredictionJob(project, location, jobId);
        }

        @Override
        public List<JsonObject> listBatchPredictionJobs(final String project, final String location, final String filter, final int limit) {
            return util.listBatchPredictionJobs(project, location, filter, limit);
        }

        @Override
        public JsonObject cancelBatchPredictionJob(final String project, final String location, final String jobId) {
            return util.cancelBatchPredictionJob(project, location, jobId);
        }
    }

    // ---------------------------------------------------------------------------------------

    private String name;
    private Trigger trigger;
    private String operation;
    private Parameters parameters;

    private transient VertexAiClient client;
    private transient Sleeper sleeper;
    /** Test hook: the poll sleeper every instance uses when set (the instance is deserialized into the DoFn, so an instance setter would be lost). */
    static volatile Sleeper testSleeper;

    @Override
    public void configure(final String name, final Trigger trigger, final String operation, final JsonObject parametersJson, final PipelineOptions options, final Schema inputSchema) {
        this.name = name;
        this.trigger = trigger;
        this.operation = operation;
        final Op op = Op.of(operation);
        // the resource / request fields are nested REST objects: keep them as JSON text (the instance is serialized into the DoFn)
        final JsonObject json = parametersJson == null ? new JsonObject() : parametersJson.deepCopy();
        final JsonObject body = new JsonObject();
        for(final String field : Op.generateContent.equals(op) ? REQUEST_FIELDS : JOB_FIELDS) {
            if(json.has(field) && !json.get(field).isJsonNull()) {
                body.add(field, json.remove(field));
            }
        }
        // responseSchema is a nested Schema object: keep it as JSON text as well
        final JsonElement responseSchema = json.remove("responseSchema");
        this.parameters = new Gson().fromJson(json, Parameters.class);
        this.parameters.body = body.toString();
        this.parameters.responseSchema = responseSchema == null || responseSchema.isJsonNull() ? null : responseSchema.toString();
        this.parameters.op = op;

        if(this.parameters.projectId == null || this.parameters.projectId.isBlank()) {
            final String project = DataflowOptions.getProject(options);
            this.parameters.projectId = project == null || project.isBlank() ? null : project;
        }

        final List<String> errorMessages = this.parameters.validate(name, trigger);
        if(!errorMessages.isEmpty()) {
            throw new IllegalModuleException(errorMessages);
        }
        this.parameters.setDefaults();

        if(Op.generateContent.equals(op) && Trigger.perElement.equals(trigger)) {
            LOG.warn("action module[{}] models.generateContent with trigger perElement runs one request per control record; for per-record inference over data records use the select transform instead", name);
        }
    }

    @Override
    public void setup() {
        this.client = createClient(parameters);
        this.sleeper = testSleeper != null ? testSleeper : Sleeper.DEFAULT;
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
        } catch (final VertexAiException e) {
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
            case generateContent -> generateContent(p, project, location, data);
            case get -> {
                final JsonObject job = client.getBatchPredictionJob(project, location, template(p.jobId, data));
                yield result(operation, job);
            }
            case list -> {
                final String filter = p.filter == null ? null : template(p.filter, data);
                final List<JsonObject> jobs = client.listBatchPredictionJobs(project, location, filter, p.pageSize);
                yield listResult(operation, jobs, "DONE");
            }
            case wait -> {
                final List<String> jobIds = new ArrayList<>();
                final boolean collected = Trigger.collect.equals(trigger) && p.jobIdField != null;
                if(collected) {
                    jobIds.addAll(ActionSupport.collectField(elements, p.jobIdField));
                    if(jobIds.isEmpty()) {
                        LOG.info("action module[{}] found no job id in field: {}", name, p.jobIdField);
                        yield ActionResult.of(operation, null, "SKIPPED", null);
                    }
                } else if(p.jobId != null) {
                    jobIds.add(template(p.jobId, data));
                } else if(WaitUntil.none.equals(p.until)) {
                    final String filter = template(p.filter, data);
                    final List<JsonObject> jobs = client.listBatchPredictionJobs(project, location, filter, 1);
                    if(jobs.isEmpty()) {
                        throw new NonRetryableException("action module[" + name + "] no vertex ai batch prediction job matches filter: " + filter);
                    }
                    yield result(operation, jobs.getFirst());
                } else {
                    // the filter lookup and the job wait share one timeoutSeconds window
                    final long started = System.currentTimeMillis();
                    jobIds.add(waitForFilter(p, project, location, template(p.filter, data)));
                    final long remaining = Math.max(1L, p.timeoutSeconds - (System.currentTimeMillis() - started) / 1000L);
                    yield result(operation, waitForAll(p, project, location, jobIds, p.until, false, false, remaining).getFirst());
                }
                final List<JsonObject> completed = WaitUntil.none.equals(p.until)
                        ? jobIds.stream().map(id -> client.getBatchPredictionJob(project, location, id)).toList()
                        : waitForAll(p, project, location, jobIds, p.until, false, false);
                if(!collected) {
                    // a single explicit job: the payload is the job itself (same shape as batchPredictionJobs.get)
                    yield result(operation, completed.getFirst());
                }
                yield listResult(operation, completed, "DONE");
            }
            case cancel -> {
                final String jobId = template(p.jobId, data);
                client.cancelBatchPredictionJob(project, location, jobId);
                LOG.info("action module[{}] cancelled vertex ai batch prediction job: {}", name, jobId);
                JsonObject job = client.getBatchPredictionJob(project, location, jobId);
                if(p.wait && !WaitUntil.none.equals(p.until) && !VertexAiUtil.isTerminal(VertexAiUtil.state(job))) {
                    job = waitForAll(p, project, location, List.of(jobId), WaitUntil.terminal, true, false).getFirst();
                }
                yield result(operation, job);
            }
        };
    }

    private ActionResult create(final Parameters p, final String project, final String location, final Map<String, Object> data) throws Exception {
        final JsonObject job = templateJson(JsonParser.parseString(p.body).getAsJsonObject(), data);
        job.addProperty("model", VertexAiUtil.modelResource(template(p.model, data)));
        // default instance / prediction formats from the chosen source / destination
        final JsonObject inputConfig = job.getAsJsonObject("inputConfig");
        if(!inputConfig.has("instancesFormat")) {
            inputConfig.addProperty("instancesFormat", inputConfig.has("bigquerySource") ? "bigquery" : "jsonl");
        }
        final JsonObject outputConfig = job.getAsJsonObject("outputConfig");
        if(!outputConfig.has("predictionsFormat")) {
            outputConfig.addProperty("predictionsFormat", outputConfig.has("bigqueryDestination") ? "bigquery" : "jsonl");
        }
        // gcsSource.uris is a list in the API: accept a single string
        final JsonObject gcsSource = VertexAiUtil.object(inputConfig, "gcsSource");
        if(gcsSource != null && gcsSource.has("uris") && gcsSource.get("uris").isJsonPrimitive()) {
            final JsonArray uris = new JsonArray();
            uris.add(gcsSource.get("uris").getAsString());
            gcsSource.add("uris", uris);
        }

        final String displayName;
        if(p.displayName == null) {
            displayName = name + "-" + new java.text.SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
            LOG.warn("action module[{}] displayName is not set; a retried bundle may submit a duplicate job (set a deterministic displayName to make batchPredictionJobs.create idempotent)", name);
        } else {
            displayName = template(p.displayName, data);
            if(p.reuseExisting) {
                final JsonObject existing = findReusableJob(project, location, displayName);
                if(existing != null) {
                    LOG.info("action module[{}] vertex ai batch prediction job named {} already exists, adopting: {} ({})", name, displayName, VertexAiUtil.id(VertexAiUtil.string(existing, "name")), VertexAiUtil.state(existing));
                    return afterStart(p, project, location, existing, true);
                }
            }
        }
        job.addProperty("displayName", displayName);

        final JsonObject submitted = client.createBatchPredictionJob(project, location, job);
        if(VertexAiUtil.string(submitted, "name") == null) {
            throw new IllegalStateException("action module[" + name + "] vertex ai did not return a batch prediction job: " + submitted);
        }
        LOG.info("action module[{}] created vertex ai batch prediction job: {}", name, VertexAiUtil.string(submitted, "name"));
        return afterStart(p, project, location, submitted, false);
    }

    /**
     * Wait (when configured) for a job this step submitted or adopted, then build the envelope: the
     * state is the observed job state, or {@code EXISTS} for an adopted job reported without waiting.
     */
    private ActionResult afterStart(final Parameters p, final String project, final String location, JsonObject job, final boolean adopted) throws Exception {
        final String jobId = VertexAiUtil.id(VertexAiUtil.string(job, "name"));
        final String state;
        if(p.wait && !WaitUntil.none.equals(p.until)) {
            if(!VertexAiUtil.isTerminal(VertexAiUtil.state(job))) {
                job = waitForAll(p, project, location, List.of(jobId), p.until, false, !adopted).getFirst();
            }
            state = VertexAiUtil.state(job);
        } else {
            state = adopted ? "EXISTS" : VertexAiUtil.state(job);
        }
        final Map<String, Object> payload = VertexAiUtil.toPayload(job);
        if(adopted) {
            payload.put("adopted", true);
        }
        return ActionResult.ofValues(operation, jobId, state, payload);
    }

    /**
     * The newest job with the display name, when it is queued, running or succeeded — the one a retry
     * adopts. A newest job that failed (or was cancelled / expired) means the work has to run again,
     * so an older success behind it is deliberately not considered.
     */
    private JsonObject findReusableJob(final String project, final String location, final String displayName) {
        final String filter = "display_name=\"" + displayName.replace("\"", "\\\"") + "\"";
        final List<JsonObject> jobs = client.listBatchPredictionJobs(project, location, filter, 1);
        if(!jobs.isEmpty() && REUSABLE_STATES.contains(VertexAiUtil.state(jobs.getFirst()))) {
            return jobs.getFirst();
        }
        return null;
    }

    private ActionResult generateContent(final Parameters p, final String project, final String location, final Map<String, Object> data) {
        final JsonObject request = templateJson(JsonParser.parseString(p.body).getAsJsonObject(), data);
        if(p.prompt != null) {
            request.add("contents", contents("user", template(p.prompt, data)));
        }
        if(p.system != null) {
            request.add("systemInstruction", content(null, template(p.system, data)));
        }
        if(p.responseSchema != null) {
            final JsonObject generationConfig = Optional.ofNullable(VertexAiUtil.object(request, "generationConfig")).orElseGet(JsonObject::new);
            generationConfig.addProperty("responseMimeType", "application/json");
            generationConfig.add("responseSchema", JsonParser.parseString(p.responseSchema));
            request.add("generationConfig", generationConfig);
        }
        final String model = template(p.model, data);
        final JsonObject response = client.generateContent(project, location, model, request);
        final String finishReason = VertexAiUtil.finishReason(response);
        final String text = VertexAiUtil.responseText(response);
        LOG.info("action module[{}] generated content with {}: finishReason: {}, usage: {}", name, model, finishReason, VertexAiUtil.object(response, "usageMetadata"));

        final Map<String, Object> payload = VertexAiUtil.toPayload(response);
        if(text != null) {
            payload.put("text", text);
            final String trimmed = text.trim();
            if((trimmed.startsWith("{") && trimmed.endsWith("}")) || (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
                try {
                    payload.put("json", com.mercari.solution.util.cloud.google.DataflowUtil.toValue(JsonParser.parseString(trimmed)));
                } catch (final Exception ignored) {
                    // not JSON: text only
                }
            }
        }
        final String state = finishReason == null ? (text == null ? "EMPTY" : "STOP") : finishReason;
        return ActionResult.ofValues(operation, VertexAiUtil.string(response, "responseId"), state, payload);
    }

    private static JsonArray contents(final String role, final String text) {
        final JsonArray contents = new JsonArray();
        contents.add(content(role, text));
        return contents;
    }

    private static JsonObject content(final String role, final String text) {
        final JsonObject part = new JsonObject();
        part.addProperty("text", text);
        final JsonArray parts = new JsonArray();
        parts.add(part);
        final JsonObject content = new JsonObject();
        if(role != null) {
            content.addProperty("role", role);
        }
        content.add("parts", parts);
        return content;
    }

    /**
     * {@code batchPredictionJobs.wait} by {@code filter}: poll the list until a matching job exists (a
     * job submitted elsewhere may not have been created yet), then return its id. Filters that could
     * match an older job should bound it with {@code create_time>"..."}.
     */
    private String waitForFilter(final Parameters p, final String project, final String location, final String filter) throws Exception {
        return ActionSupport.waitForAll(name, "vertex ai batch prediction job matching filter", List.of(filter), p.timeoutSeconds, sleeper,
                f -> {
                    final List<JsonObject> jobs = client.listBatchPredictionJobs(project, location, f, 1);
                    return jobs.isEmpty() ? null : VertexAiUtil.id(VertexAiUtil.string(jobs.getFirst(), "name"));
                },
                e -> e instanceof RuntimeException r && isTransient(r),
                "appear in " + project + "/" + location,
                null).getFirst();
    }

    /**
     * Poll all jobs until each reaches the target ({@code terminal}, or {@code running} which also
     * accepts a terminal state), sharing one backoff and one {@code timeoutSeconds} window.
     * A job that ended FAILED / EXPIRED (or CANCELLED when no cancel was requested, or
     * PARTIALLY_SUCCEEDED with {@code failOnPartial}) fails the firing as non-retryable with its error
     * and completion stats attached. Transient poll errors (429 / 5xx / I/O) are retried inside the
     * loop so the module-level {@code retry} does not re-run the whole firing (which could submit a
     * second job). Pending jobs are cancelled on timeout only when {@code cancelOnTimeout} is set and
     * the jobs are ours ({@code own}: submitted by this step, not adopted or merely waited for).
     */
    private List<JsonObject> waitForAll(final Parameters p, final String project, final String location, final List<String> jobIds, final WaitUntil until, final boolean cancelRequested, final boolean own) throws Exception {
        return waitForAll(p, project, location, jobIds, until, cancelRequested, own, p.timeoutSeconds);
    }

    private List<JsonObject> waitForAll(final Parameters p, final String project, final String location, final List<String> jobIds, final WaitUntil until, final boolean cancelRequested, final boolean own, final long timeoutSeconds) throws Exception {
        return ActionSupport.waitForAll(name, "vertex ai batch prediction jobs", jobIds, timeoutSeconds, sleeper,
                jobId -> {
                    final JsonObject job = client.getBatchPredictionJob(project, location, jobId);
                    final String state = VertexAiUtil.state(job);
                    if(VertexAiUtil.isTerminal(state)) {
                        if("JOB_STATE_CANCELLED".equals(state) && !cancelRequested) {
                            throw new NonRetryableException("action module[" + name + "] vertex ai batch prediction job was cancelled: " + jobId + VertexAiUtil.describeFailure(job));
                        }
                        if("JOB_STATE_FAILED".equals(state) || "JOB_STATE_EXPIRED".equals(state)
                                || ("JOB_STATE_PARTIALLY_SUCCEEDED".equals(state) && p.failOnPartial)) {
                            throw new NonRetryableException("action module[" + name + "] vertex ai batch prediction job " + jobId + " ended " + state + ":" + VertexAiUtil.describeFailure(job));
                        }
                        return job;
                    } else if(WaitUntil.running.equals(until) && "JOB_STATE_RUNNING".equals(state)) {
                        return job;
                    }
                    return null;
                },
                e -> e instanceof RuntimeException r && isTransient(r),
                "reach " + until.name(),
                p.cancelOnTimeout && own ? jobId -> client.cancelBatchPredictionJob(project, location, jobId) : null);
    }

    /**
     * 429 / 5xx from the API, or a transport failure (wrapped I/O error) - worth another poll.
     * A credentials failure ({@link VertexAiUtil.CredentialsException}) is not: re-polling cannot fix it.
     */
    private static boolean isTransient(final RuntimeException e) {
        if(e instanceof VertexAiException ve) {
            return ve.isRetryable();
        }
        if(e instanceof VertexAiUtil.CredentialsException) {
            return false;
        }
        return e instanceof IllegalStateException && e.getCause() instanceof IOException;
    }

    private static ActionResult result(final String operation, final JsonObject job) {
        return ActionResult.ofValues(operation, VertexAiUtil.id(VertexAiUtil.string(job, "name")), VertexAiUtil.state(job), VertexAiUtil.toPayload(job));
    }

    private static ActionResult listResult(final String operation, final List<JsonObject> jobs, final String state) {
        final Map<String, Object> payload = new LinkedHashMap<>();
        final List<Map<String, Object>> maps = jobs.stream().map(VertexAiUtil::toPayload).toList();
        payload.put("jobs", maps);
        payload.put("count", maps.size());
        if(!maps.isEmpty()) {
            payload.put("firstJob", maps.getFirst());
        }
        final String jobId = jobs.stream().map(j -> VertexAiUtil.id(VertexAiUtil.string(j, "name"))).filter(Objects::nonNull).reduce((a, b) -> a + "," + b).orElse(null);
        return ActionResult.ofValues(operation, jobId, state, payload);
    }

    /**
     * Rejected requests (bad argument, unknown job / model, missing permission, precondition)
     * cannot be fixed by re-execution: map them to {@link NonRetryableException} so the module-level
     * {@code retry} is spent on transient errors (429, 5xx) only.
     */
    static NonRetryableException rejectedRequest(final VertexAiException e) {
        return switch (e.status) {
            case 400, 401, 403, 404, 409, 412 -> new NonRetryableException("vertex ai request rejected (" + e.status + "): " + e.getMessage(), e);
            default -> null;
        };
    }

    /** Every string leaf of the JSON expanded as a FreeMarker template with the element data. */
    static JsonObject templateJson(final JsonObject object, final Map<String, Object> data) {
        return ActionSupport.templateJson(object, text -> template(text, data)).getAsJsonObject();
    }

    static String template(final String text, final Map<String, Object> data) {
        return TemplateUtil.executeStrictTemplateIfNeeded(text, data);
    }

}
