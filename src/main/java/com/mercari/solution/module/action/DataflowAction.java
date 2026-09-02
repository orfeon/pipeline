package com.mercari.solution.module.action;

import com.google.api.client.util.Sleeper;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.dataflow.v1beta3.*;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.protobuf.FieldMask;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.mercari.solution.config.options.DataflowOptions;
import com.mercari.solution.module.Action;
import com.mercari.solution.module.Action.Trigger;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.cloud.google.DataflowUtil;
import com.mercari.solution.util.cloud.google.GcpCredentialsCache;
import org.apache.beam.sdk.options.PipelineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Action service for Cloud Dataflow job operations (Dataflow REST API v1b3, {@code projects.locations.*}):
 * launch a Flex Template job and wait for it, read or list jobs (guards with {@code failWhen} /
 * {@code skipWhen}), wait for jobs launched elsewhere, cancel / drain / rescale a job
 * ({@code jobs.update}), and read job messages for diagnosis.
 *
 * <p>Idempotency: Dataflow rejects a second <em>active</em> job with the same name in a project /
 * region, so a deterministic {@code jobName} makes {@code flexTemplates.launch} safe on a retried
 * bundle — ALREADY_EXISTS adopts the running job (state {@code EXISTS}). A finished job's name can be
 * reused, so a retry after completion launches again. {@code jobs.update} is naturally idempotent.
 *
 * <p>The envelope payload is the {@code Job} resource JSON (proto JSON of the v1beta3 message, numeric
 * strings converted to numbers), so the API reference doubles as the {@code failWhen} path dictionary.
 */
@Action.Service(name = "dataflow", operations = {
        "flexTemplates.launch", "jobs.get", "jobs.list", "jobs.wait", "jobs.update", "jobs.messages.list"})
public class DataflowAction implements ActionService {

    private static final Logger LOG = LoggerFactory.getLogger(DataflowAction.class);

    private static final Pattern PATTERN_JOB_NAME = Pattern.compile("^[a-z]([-a-z0-9]{0,1022}[a-z0-9])?$");
    private static final DateTimeFormatter JOB_NAME_SUFFIX = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(ZoneOffset.UTC);
    private static final int NAME_SEARCH_LIMIT = 200;

    /** Poll errors worth another poll inside the wait window; every other ApiException fails the firing at once. */
    private static final Set<StatusCode.Code> TRANSIENT_POLL_CODES = Set.of(
            StatusCode.Code.UNAVAILABLE, StatusCode.Code.DEADLINE_EXCEEDED, StatusCode.Code.RESOURCE_EXHAUSTED);

    private static final Set<JobState> TERMINAL_STATES = EnumSet.of(
            JobState.JOB_STATE_DONE, JobState.JOB_STATE_FAILED, JobState.JOB_STATE_CANCELLED,
            JobState.JOB_STATE_DRAINED, JobState.JOB_STATE_UPDATED);

    /** Operations; {@code operation} is the config value (also listed in {@code @Action.Service}). */
    public enum Op {
        launch("flexTemplates.launch"),
        get("jobs.get"),
        list("jobs.list"),
        wait("jobs.wait"),
        update("jobs.update"),
        messages("jobs.messages.list");

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

    public static class RuntimeUpdatableParameters implements Serializable {
        public Integer maxNumWorkers;
        public Integer minNumWorkers;
        public Double workerUtilizationHint;

        boolean isEmpty() {
            return maxNumWorkers == null && minNumWorkers == null && workerUtilizationHint == null;
        }
    }

    public static class Parameters implements Serializable {

        public Op op;

        // common
        public String projectId;
        public String region;
        public String endpoint;

        // target job (get / wait / update / messages)
        public String jobId;
        public String jobName;
        public String jobIdField;

        // flexTemplates.launch (LaunchFlexTemplateParameter)
        public String containerSpecGcsPath;
        public String config;
        public Map<String, String> args;
        public Map<String, String> parameters;
        public Map<String, String> launchOptions;
        public Boolean update;
        public Map<String, String> transformNameMappings;
        /** {@code FlexTemplateRuntimeEnvironment} JSON (REST field names); kept as text because the DoFn is serialized. */
        public String environment;

        // jobs.update
        public String requestedState;
        public RuntimeUpdatableParameters runtimeUpdatableParams;

        // jobs.list
        public String filter;
        public Integer limit;

        // jobs.messages.list
        public String minimumImportance;

        // wait
        public Boolean wait;
        public WaitUntil waitUntil;
        public Long timeoutSeconds;
        public Boolean cancelOnTimeout;
        public String view;

        public List<String> validate(final String name, final Trigger trigger) {
            final List<String> errorMessages = new ArrayList<>();
            final String prefix = "action module[" + name + "].parameters.";
            if(projectId == null) {
                errorMessages.add(prefix + "projectId is required (it could not be derived from the pipeline options)");
            }
            if(region == null) {
                errorMessages.add(prefix + "region is required (it could not be derived from the pipeline options)");
            }
            switch (op) {
                case launch -> {
                    if(containerSpecGcsPath == null) {
                        errorMessages.add(prefix + "containerSpecGcsPath is required for flexTemplates.launch");
                    } else if(!TemplateUtil.isTemplateText(containerSpecGcsPath) && !containerSpecGcsPath.startsWith("gs://")) {
                        errorMessages.add(prefix + "containerSpecGcsPath must start with gs:// but: " + containerSpecGcsPath);
                    }
                    if(jobName != null && !TemplateUtil.isTemplateText(jobName) && !PATTERN_JOB_NAME.matcher(jobName).matches()) {
                        errorMessages.add(prefix + "jobName must match " + PATTERN_JOB_NAME.pattern() + " but: " + jobName);
                    }
                    if(environment != null) {
                        try {
                            parseEnvironment(environment);
                        } catch (final InvalidProtocolBufferException e) {
                            errorMessages.add(prefix + "environment is not a valid FlexTemplateRuntimeEnvironment: " + e.getMessage());
                        }
                    }
                    if(Boolean.TRUE.equals(update) && jobName == null) {
                        errorMessages.add(prefix + "jobName is required when update is true (the name of the job to replace)");
                    }
                }
                case get, update, messages -> {
                    if(jobId == null && jobName == null) {
                        errorMessages.add(prefix + "jobId or jobName is required for " + op.operation);
                    }
                }
                case wait -> {
                    if(jobId == null && jobName == null && jobIdField == null) {
                        errorMessages.add(prefix + "jobId, jobName or jobIdField is required for jobs.wait");
                    }
                    if(jobIdField != null && !Trigger.collect.equals(trigger)) {
                        errorMessages.add(prefix + "jobIdField requires trigger: collect");
                    }
                }
                case list -> {
                    if(filter != null) {
                        try {
                            ListJobsRequest.Filter.valueOf(filter);
                        } catch (final IllegalArgumentException e) {
                            errorMessages.add(prefix + "filter must be one of ALL, ACTIVE, TERMINATED but: " + filter);
                        }
                    }
                    if(limit != null && limit <= 0) {
                        errorMessages.add(prefix + "limit must be positive");
                    }
                }
            }
            if(Op.update.equals(op)) {
                if(requestedState == null && (runtimeUpdatableParams == null || runtimeUpdatableParams.isEmpty())) {
                    errorMessages.add(prefix + "jobs.update requires requestedState and/or runtimeUpdatableParams");
                }
                if(requestedState != null) {
                    final JobState state = toJobState(requestedState);
                    if(state == null || !(JobState.JOB_STATE_CANCELLED.equals(state) || JobState.JOB_STATE_DRAINED.equals(state))) {
                        errorMessages.add(prefix + "requestedState must be JOB_STATE_CANCELLED or JOB_STATE_DRAINED but: " + requestedState);
                    }
                }
            }
            if(Op.messages.equals(op) && minimumImportance != null) {
                try {
                    JobMessageImportance.valueOf(minimumImportance);
                } catch (final IllegalArgumentException e) {
                    errorMessages.add(prefix + "minimumImportance must be a JobMessageImportance name (e.g. JOB_MESSAGE_ERROR) but: " + minimumImportance);
                }
            }
            if(view != null) {
                try {
                    JobView.valueOf(view);
                } catch (final IllegalArgumentException e) {
                    errorMessages.add(prefix + "view must be a JobView name (JOB_VIEW_SUMMARY / JOB_VIEW_ALL / JOB_VIEW_DESCRIPTION) but: " + view);
                }
            }
            if(timeoutSeconds != null && timeoutSeconds <= 0) {
                errorMessages.add(prefix + "timeoutSeconds must be positive");
            }
            return errorMessages;
        }

        public void setDefaults() {
            if(wait == null) {
                wait = true;
            }
            if(timeoutSeconds == null) {
                timeoutSeconds = 86400L;
            }
            if(cancelOnTimeout == null) {
                // a job launched elsewhere is not ours to cancel
                cancelOnTimeout = Op.launch.equals(op);
            }
            if(view == null) {
                view = JobView.JOB_VIEW_SUMMARY.name();
            }
            if(filter == null) {
                filter = ListJobsRequest.Filter.ALL.name();
            }
            if(limit == null) {
                limit = 100;
            }
            if(minimumImportance == null) {
                minimumImportance = JobMessageImportance.JOB_MESSAGE_ERROR.name();
            }
            if(Op.wait.equals(op) && waitUntil == null) {
                waitUntil = WaitUntil.terminal;
            }
        }
    }

    // ---------------------------------------------------------------------------------------
    // Client abstraction (gRPC in production, in-memory in tests via endpoint: memory://name)
    // ---------------------------------------------------------------------------------------

    public interface DataflowClient extends AutoCloseable {
        Job launchFlexTemplate(String project, String region, LaunchFlexTemplateParameter parameter);
        Job getJob(String project, String region, String jobId, JobView view);
        List<Job> listJobs(String project, String region, ListJobsRequest.Filter filter, int limit);
        Job updateJob(String project, String region, String jobId, Job job, FieldMask updateMask);
        List<JobMessage> listJobMessages(String project, String region, String jobId, JobMessageImportance minimumImportance, int limit);
        @Override
        void close();
    }

    private static final ActionSupport.MemoryClients<DataflowClient> MEMORY_CLIENTS = new ActionSupport.MemoryClients<>("dataflow");

    public static void registerMemoryClient(final String name, final DataflowClient client) {
        MEMORY_CLIENTS.register(name, client);
    }

    public static void unregisterMemoryClient(final String name) {
        MEMORY_CLIENTS.unregister(name);
    }

    static DataflowClient createClient(final Parameters parameters) throws IOException {
        final DataflowClient memory = MEMORY_CLIENTS.resolve(parameters.endpoint);
        return memory != null ? memory : new GrpcDataflowClient(parameters);
    }

    static class GrpcDataflowClient implements DataflowClient {

        private final FlexTemplatesServiceClient flexTemplates;
        private final JobsV1Beta3Client jobs;
        private final MessagesV1Beta3Client messages;

        GrpcDataflowClient(final Parameters parameters) throws IOException {
            final FlexTemplatesServiceSettings.Builder flex = FlexTemplatesServiceSettings.newBuilder();
            final JobsV1Beta3Settings.Builder job = JobsV1Beta3Settings.newBuilder();
            final MessagesV1Beta3Settings.Builder message = MessagesV1Beta3Settings.newBuilder();
            if(parameters.endpoint != null) {
                final InstantiatingGrpcChannelProvider channel = InstantiatingGrpcChannelProvider.newBuilder()
                        .setEndpoint(parameters.endpoint)
                        .setChannelConfigurator(b -> b.usePlaintext())
                        .build();
                flex.setEndpoint(parameters.endpoint).setCredentialsProvider(NoCredentialsProvider.create()).setTransportChannelProvider(channel);
                job.setEndpoint(parameters.endpoint).setCredentialsProvider(NoCredentialsProvider.create()).setTransportChannelProvider(channel);
                message.setEndpoint(parameters.endpoint).setCredentialsProvider(NoCredentialsProvider.create()).setTransportChannelProvider(channel);
            } else {
                flex.setCredentialsProvider(GcpCredentialsCache::credentials);
                job.setCredentialsProvider(GcpCredentialsCache::credentials);
                message.setCredentialsProvider(GcpCredentialsCache::credentials);
            }
            this.flexTemplates = FlexTemplatesServiceClient.create(flex.build());
            this.jobs = JobsV1Beta3Client.create(job.build());
            this.messages = MessagesV1Beta3Client.create(message.build());
        }

        @Override
        public Job launchFlexTemplate(final String project, final String region, final LaunchFlexTemplateParameter parameter) {
            final LaunchFlexTemplateResponse response = flexTemplates.launchFlexTemplate(LaunchFlexTemplateRequest.newBuilder()
                    .setProjectId(project)
                    .setLocation(region)
                    .setLaunchParameter(parameter)
                    .build());
            if(!response.hasJob()) {
                throw new IllegalStateException("Dataflow did not return a job for the flex template launch: " + response);
            }
            return response.getJob();
        }

        @Override
        public Job getJob(final String project, final String region, final String jobId, final JobView view) {
            return jobs.getJob(GetJobRequest.newBuilder()
                    .setProjectId(project)
                    .setLocation(region)
                    .setJobId(jobId)
                    .setView(view)
                    .build());
        }

        @Override
        public List<Job> listJobs(final String project, final String region, final ListJobsRequest.Filter filter, final int limit) {
            final ListJobsRequest request = ListJobsRequest.newBuilder()
                    .setProjectId(project)
                    .setLocation(region)
                    .setFilter(filter)
                    .setPageSize(Math.min(limit, 100))
                    .build();
            final List<Job> result = new ArrayList<>();
            for(final Job job : jobs.listJobs(request).iterateAll()) {
                result.add(job);
                if(result.size() >= limit) {
                    break;
                }
            }
            return result;
        }

        @Override
        public Job updateJob(final String project, final String region, final String jobId, final Job job, final FieldMask updateMask) {
            final UpdateJobRequest.Builder builder = UpdateJobRequest.newBuilder()
                    .setProjectId(project)
                    .setLocation(region)
                    .setJobId(jobId)
                    .setJob(job);
            if(updateMask != null) {
                builder.setUpdateMask(updateMask);
            }
            return jobs.updateJob(builder.build());
        }

        @Override
        public List<JobMessage> listJobMessages(final String project, final String region, final String jobId, final JobMessageImportance minimumImportance, final int limit) {
            final ListJobMessagesRequest request = ListJobMessagesRequest.newBuilder()
                    .setProjectId(project)
                    .setLocation(region)
                    .setJobId(jobId)
                    .setMinimumImportance(minimumImportance)
                    .setPageSize(Math.min(limit, 100))
                    .build();
            final List<JobMessage> result = new ArrayList<>();
            for(final JobMessage message : messages.listJobMessages(request).iterateAll()) {
                result.add(message);
                if(result.size() >= limit) {
                    break;
                }
            }
            return result;
        }

        @Override
        public void close() {
            flexTemplates.close();
            jobs.close();
            messages.close();
        }
    }

    // ---------------------------------------------------------------------------------------

    private String name;
    private Trigger trigger;
    private String operation;
    private Parameters parameters;
    /** {@code FlexTemplateRuntimeEnvironment} JSON inherited from the parent pipeline's Dataflow options (null on other runners). */
    private String inheritedEnvironment;

    private transient DataflowClient client;
    private transient Sleeper sleeper;

    @Override
    public void configure(final String name, final Trigger trigger, final String operation, final JsonObject parametersJson, final PipelineOptions options, final Schema inputSchema) {
        this.name = name;
        this.trigger = trigger;
        this.operation = operation;
        // environment is a nested REST object: keep it as JSON text (the instance is serialized into the DoFn)
        final JsonObject json = parametersJson == null ? new JsonObject() : parametersJson.deepCopy();
        String environment = null;
        if(json.has("environment") && !json.get("environment").isJsonNull()) {
            final JsonElement env = json.remove("environment");
            environment = env.isJsonPrimitive() ? env.getAsString() : env.toString();
        }
        this.parameters = new Gson().fromJson(json, Parameters.class);
        this.parameters.environment = environment;
        this.parameters.op = Op.of(operation);

        if(this.parameters.projectId == null) {
            this.parameters.projectId = DataflowOptions.getProject(options);
        }
        if(this.parameters.region == null) {
            this.parameters.region = dataflowOption(options, "getRegion");
        }
        this.inheritedEnvironment = inheritedEnvironment(options);

        final List<String> errorMessages = this.parameters.validate(name, trigger);
        if(!errorMessages.isEmpty()) {
            throw new IllegalModuleException(errorMessages);
        }
        this.parameters.setDefaults();
    }

    @Override
    public void setup() {
        try {
            this.client = createClient(parameters);
        } catch (final IOException e) {
            throw new IllegalStateException("Failed to create Dataflow client", e);
        }
        if(this.sleeper == null) {
            this.sleeper = Sleeper.DEFAULT;
        }
    }

    /** Test hook: replace the poll sleeper (call before {@link #setup()}). */
    void setSleeper(final Sleeper sleeper) {
        this.sleeper = sleeper;
    }

    @Override
    public ActionResult execute(final List<MElement> elements) throws Exception {
        final Map<String, Object> data = switch (trigger) {
            case perElement -> elements.getFirst().asPrimitiveMap();
            case once, collect -> Action.createCollectTemplateData(elements);
        };
        final Parameters p = parameters;
        final String project = template(p.projectId, data);
        final String region = template(p.region, data);

        try {
            return execute(p, project, region, data, elements);
        } catch (final ApiException e) {
            final NonRetryableException rejected = rejectedRequest(e);
            if(rejected != null) {
                throw rejected;
            }
            throw e;
        }
    }

    private ActionResult execute(final Parameters p, final String project, final String region, final Map<String, Object> data, final List<MElement> elements) throws Exception {
        return switch (p.op) {
            case launch -> launch(p, project, region, data);
            case get -> {
                final Job job = resolveJob(p, project, region, data, JobView.valueOf(p.view), false);
                yield result(operation, job.getId(), job);
            }
            case list -> {
                final String jobName = p.jobName == null ? null : template(p.jobName, data);
                final List<Job> jobs = new ArrayList<>();
                for(final Job job : client.listJobs(project, region, ListJobsRequest.Filter.valueOf(p.filter), jobName == null ? p.limit : NAME_SEARCH_LIMIT)) {
                    if(jobName == null || jobName.equals(job.getName())) {
                        jobs.add(job);
                        if(jobs.size() >= p.limit) {
                            break;
                        }
                    }
                }
                final Map<String, Object> payload = new LinkedHashMap<>();
                final List<Map<String, Object>> maps = jobs.stream().map(DataflowUtil::toPayload).toList();
                payload.put("jobs", maps);
                payload.put("count", maps.size());
                if(!maps.isEmpty()) {
                    payload.put("firstJob", maps.getFirst());
                }
                yield ActionResult.ofValues(operation, jobs.isEmpty() ? null : jobs.getFirst().getId(), "DONE", payload);
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
                } else {
                    jobIds.add(resolveJob(p, project, region, data, JobView.JOB_VIEW_SUMMARY, true).getId());
                }
                final List<Job> completed = waitForAll(p, project, region, jobIds, p.waitUntil, null);
                if(!collected) {
                    // a single explicit job: the payload is the Job itself (same shape as jobs.get)
                    yield result(operation, jobIds.getFirst(), completed.getFirst());
                }
                final Map<String, Object> payload = new LinkedHashMap<>();
                final List<Map<String, Object>> maps = completed.stream().map(DataflowUtil::toPayload).toList();
                payload.put("jobs", maps);
                payload.put("count", maps.size());
                payload.put("firstJob", maps.getFirst());
                yield ActionResult.ofValues(operation, String.join(",", jobIds), "DONE", payload);
            }
            case update -> {
                final Job target = resolveJob(p, project, region, data, JobView.JOB_VIEW_SUMMARY, true);
                final Job.Builder builder = Job.newBuilder();
                final FieldMask.Builder mask = FieldMask.newBuilder();
                final JobState requested = p.requestedState == null ? null : toJobState(p.requestedState);
                if(requested != null) {
                    builder.setRequestedState(requested);
                    mask.addPaths("requested_state");
                }
                if(p.runtimeUpdatableParams != null && !p.runtimeUpdatableParams.isEmpty()) {
                    final RuntimeUpdatableParams.Builder rup = RuntimeUpdatableParams.newBuilder();
                    if(p.runtimeUpdatableParams.maxNumWorkers != null) {
                        rup.setMaxNumWorkers(p.runtimeUpdatableParams.maxNumWorkers);
                        mask.addPaths("runtime_updatable_params.max_num_workers");
                    }
                    if(p.runtimeUpdatableParams.minNumWorkers != null) {
                        rup.setMinNumWorkers(p.runtimeUpdatableParams.minNumWorkers);
                        mask.addPaths("runtime_updatable_params.min_num_workers");
                    }
                    if(p.runtimeUpdatableParams.workerUtilizationHint != null) {
                        rup.setWorkerUtilizationHint(p.runtimeUpdatableParams.workerUtilizationHint);
                        mask.addPaths("runtime_updatable_params.worker_utilization_hint");
                    }
                    builder.setRuntimeUpdatableParams(rup);
                }
                Job updated = client.updateJob(project, region, target.getId(), builder.build(), mask.build());
                if(requested != null && p.wait) {
                    updated = waitForAll(p, project, region, List.of(target.getId()), WaitUntil.terminal, requested).getFirst();
                }
                yield result(operation, target.getId(), updated);
            }
            case messages -> {
                final Job target = resolveJob(p, project, region, data, JobView.JOB_VIEW_SUMMARY, false);
                final List<JobMessage> messages = client.listJobMessages(
                        project, region, target.getId(), JobMessageImportance.valueOf(p.minimumImportance), p.limit);
                final Map<String, Object> payload = new LinkedHashMap<>();
                final List<Map<String, Object>> maps = messages.stream().map(DataflowUtil::toPayload).toList();
                payload.put("messages", maps);
                payload.put("count", maps.size());
                payload.put("currentState", target.getCurrentState().name());
                yield ActionResult.ofValues(operation, target.getId(), "DONE", payload);
            }
        };
    }

    private ActionResult launch(final Parameters p, final String project, final String region, final Map<String, Object> data) throws Exception {
        final String jobName;
        if(p.jobName == null) {
            jobName = defaultJobName(name);
            LOG.warn("action module[{}] jobName is not set; launching as {} (not idempotent on retry)", name, jobName);
        } else {
            jobName = template(p.jobName, data);
            if(!PATTERN_JOB_NAME.matcher(jobName).matches()) {
                throw new IllegalArgumentException("rendered jobName is illegal: " + jobName);
            }
        }

        final LaunchFlexTemplateParameter.Builder builder = LaunchFlexTemplateParameter.newBuilder()
                .setJobName(jobName)
                .setContainerSpecGcsPath(template(p.containerSpecGcsPath, data));
        if(p.parameters != null) {
            p.parameters.forEach((k, v) -> builder.putParameters(k, template(v, data)));
        }
        if(p.config != null) {
            builder.putParameters("config", template(p.config, data));
        }
        if(p.args != null) {
            p.args.forEach((k, v) -> builder.putParameters("args." + k, template(v, data)));
        }
        if(p.launchOptions != null) {
            p.launchOptions.forEach((k, v) -> builder.putLaunchOptions(k, template(v, data)));
        }
        if(p.update != null) {
            builder.setUpdate(p.update);
        }
        if(p.transformNameMappings != null) {
            builder.putAllTransformNameMappings(p.transformNameMappings);
        }
        // inherited parent values first, the explicit environment merged over them (proto merge skips unset scalars)
        final FlexTemplateRuntimeEnvironment.Builder environment = inheritedEnvironment == null
                ? FlexTemplateRuntimeEnvironment.newBuilder()
                : parseEnvironment(inheritedEnvironment).toBuilder();
        if(p.environment != null) {
            environment.mergeFrom(parseEnvironment(p.environment));
        }
        builder.setEnvironment(environment);

        Job job;
        boolean adopted = false;
        try {
            job = client.launchFlexTemplate(project, region, builder.build());
            LOG.info("action module[{}] launched dataflow job: {} ({})", name, job.getId(), jobName);
        } catch (final ApiException e) {
            if(!isAlreadyExists(e)) {
                throw e;
            }
            job = findActiveJobByName(project, region, jobName);
            if(job == null) {
                throw new NonRetryableException("action module[" + name + "] dataflow rejected job name " + jobName
                        + " as already existing but no active job with that name was found", e);
            }
            adopted = true;
            LOG.info("action module[{}] dataflow job already exists, adopting: {} ({})", name, job.getId(), jobName);
        }

        // Without wait an adopted job is reported as EXISTS; with wait the job's outcome is what
        // matters, so the state is the observed job state on both paths (payload.adopted marks adoption).
        String state = adopted ? "EXISTS" : job.getCurrentState().name();
        if(p.wait && !WaitUntil.none.equals(p.waitUntil)) {
            // waitUntil null: decided per poll from the job type (unset in the launch response while QUEUED)
            job = waitForAll(p, project, region, List.of(job.getId()), p.waitUntil, null).getFirst();
            state = job.getCurrentState().name();
        }
        final Map<String, Object> payload = DataflowUtil.toPayload(job);
        if(adopted) {
            payload.put("adopted", true);
        }
        return ActionResult.ofValues(operation, job.getId(), state, payload);
    }

    /**
     * Resolve the target job from {@code jobId} (exact) or {@code jobName} (the latest job with that
     * name; only among active jobs when {@code activeOnly} - wait / update only make sense for those).
     */
    private Job resolveJob(final Parameters p, final String project, final String region, final Map<String, Object> data, final JobView view, final boolean activeOnly) {
        if(p.jobId != null) {
            final String jobId = template(p.jobId, data);
            return client.getJob(project, region, jobId, view);
        }
        final String jobName = template(p.jobName, data);
        Job latest = null;
        final ListJobsRequest.Filter filter = activeOnly ? ListJobsRequest.Filter.ACTIVE : ListJobsRequest.Filter.ALL;
        for(final Job job : client.listJobs(project, region, filter, NAME_SEARCH_LIMIT)) {
            if(!jobName.equals(job.getName())) {
                continue;
            }
            if(latest == null || job.getCreateTime().getSeconds() > latest.getCreateTime().getSeconds()) {
                latest = job;
            }
        }
        if(latest == null) {
            throw new NonRetryableException("action module[" + name + "] found no " + (activeOnly ? "active " : "")
                    + "dataflow job named " + jobName + " in " + project + "/" + region);
        }
        return JobView.JOB_VIEW_SUMMARY.equals(view) ? latest : client.getJob(project, region, latest.getId(), view);
    }

    private Job findActiveJobByName(final String project, final String region, final String jobName) {
        for(final Job job : client.listJobs(project, region, ListJobsRequest.Filter.ACTIVE, NAME_SEARCH_LIMIT)) {
            if(jobName.equals(job.getName())) {
                return job;
            }
        }
        return null;
    }

    /**
     * Poll all jobs until each reaches the target ({@code terminal}, or {@code running} which also
     * accepts a terminal state), sharing one backoff and one {@code timeoutSeconds} window.
     * A job that ended FAILED (or was CANCELLED while a terminal/running state was awaited) fails the
     * firing as non-retryable with its error messages attached.
     */
    private List<Job> waitForAll(final Parameters p, final String project, final String region, final List<String> jobIds, final WaitUntil until, final JobState requested) throws Exception {
        final boolean cancelRequested = JobState.JOB_STATE_CANCELLED.equals(requested);
        return ActionSupport.waitForAll(name, "dataflow jobs", jobIds, p.timeoutSeconds, sleeper,
                jobId -> {
                    final Job job = client.getJob(project, region, jobId, JobView.JOB_VIEW_SUMMARY);
                    final JobState state = job.getCurrentState();
                    if(TERMINAL_STATES.contains(state)) {
                        if(JobState.JOB_STATE_FAILED.equals(state)) {
                            throw new NonRetryableException("action module[" + name + "] dataflow job failed: " + jobId + describeErrors(project, region, jobId));
                        }
                        if(JobState.JOB_STATE_CANCELLED.equals(state) && !cancelRequested) {
                            throw new NonRetryableException("action module[" + name + "] dataflow job was cancelled: " + jobId);
                        }
                        return job;
                    } else if(WaitUntil.running.equals(targetOf(until, job)) && JobState.JOB_STATE_RUNNING.equals(state)) {
                        return job;
                    }
                    return null;
                },
                e -> e instanceof ApiException a && TRANSIENT_POLL_CODES.contains(a.getStatusCode().getCode()),
                "reach " + (until == null ? "terminal (batch) / running (streaming)" : until.name()),
                p.cancelOnTimeout
                        ? jobId -> client.updateJob(project, region, jobId, Job.newBuilder().setRequestedState(JobState.JOB_STATE_CANCELLED).build(), null)
                        : null);
    }

    private String describeErrors(final String project, final String region, final String jobId) {
        try {
            final List<JobMessage> messages = client.listJobMessages(project, region, jobId, JobMessageImportance.JOB_MESSAGE_ERROR, 20);
            if(messages.isEmpty()) {
                return "";
            }
            return "\n" + DataflowUtil.formatJobMessages(messages);
        } catch (final Exception e) {
            LOG.warn("action module[{}] failed to read messages of dataflow job {}: {}", name, jobId, e.getMessage());
            return "";
        }
    }

    private static ActionResult result(final String operation, final String jobId, final Job job) {
        return ActionResult.ofValues(operation, jobId, job.getCurrentState().name(), DataflowUtil.toPayload(job));
    }

    /**
     * The wait target of a job: the explicit {@code until}, or by job type when none was given -
     * terminal for batch, running for streaming; null while the type is still unknown (a queued
     * launch has no type yet), in which case only a terminal state completes the wait.
     */
    private static WaitUntil targetOf(final WaitUntil until, final Job job) {
        if(until != null) {
            return until;
        }
        return switch (job.getType()) {
            case JOB_TYPE_STREAMING -> WaitUntil.running;
            case JOB_TYPE_BATCH -> WaitUntil.terminal;
            default -> null;
        };
    }

    private static boolean isAlreadyExists(final ApiException e) {
        if(e instanceof AlreadyExistsException || StatusCode.Code.ALREADY_EXISTS.equals(e.getStatusCode().getCode())) {
            return true;
        }
        final String message = e.getMessage();
        return message != null && message.toLowerCase().contains("already exists");
    }

    /**
     * Rejected requests (bad argument, unknown job, missing permission, ...) cannot be fixed by
     * re-execution: map them to {@link NonRetryableException} so the module-level {@code retry}
     * is spent on transient errors (UNAVAILABLE, DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED) only.
     */
    static NonRetryableException rejectedRequest(final ApiException e) {
        return switch (e.getStatusCode().getCode()) {
            case INVALID_ARGUMENT, NOT_FOUND, PERMISSION_DENIED, UNAUTHENTICATED, FAILED_PRECONDITION, OUT_OF_RANGE, UNIMPLEMENTED ->
                    new NonRetryableException("dataflow request rejected (" + e.getStatusCode().getCode() + "): " + e.getMessage(), e);
            default -> null;
        };
    }

    static String defaultJobName(final String stepName) {
        final String base = stepName.toLowerCase().replaceAll("[^a-z0-9-]", "-").replaceAll("^-+|-+$", "");
        return (base.isEmpty() ? "action" : base) + "-" + JOB_NAME_SUFFIX.format(Instant.now());
    }

    static JobState toJobState(final String text) {
        final String n = text.startsWith("JOB_STATE_") ? text : "JOB_STATE_" + text.toUpperCase();
        try {
            return JobState.valueOf(n);
        } catch (final IllegalArgumentException e) {
            return null;
        }
    }

    static FlexTemplateRuntimeEnvironment parseEnvironment(final String json) throws InvalidProtocolBufferException {
        final FlexTemplateRuntimeEnvironment.Builder builder = FlexTemplateRuntimeEnvironment.newBuilder();
        JsonFormat.parser().ignoringUnknownFields().merge(json, builder);
        return builder.build();
    }

    /**
     * A single {@code DataflowPipelineOptions} getter via reflection: null when the class is not on
     * the classpath (other runners) or the getter fails (Beam default factories may throw or reach
     * the network, e.g. {@code getStagingLocation} without a GCS tempLocation).
     */
    static String dataflowOption(final PipelineOptions options, final String getter) {
        try {
            @SuppressWarnings("unchecked")
            final Class<? extends PipelineOptions> clazz = (Class<? extends PipelineOptions>) Class.forName("org.apache.beam.runners.dataflow.options.DataflowPipelineOptions");
            final Object value = clazz.getMethod(getter).invoke(options.as(clazz));
            return value == null || value.toString().isEmpty() ? null : value.toString();
        } catch (final Throwable e) {
            return null;
        }
    }

    /** Environment inherited by launched jobs from the parent pipeline's Dataflow options, as JSON; null when there is none. */
    static String inheritedEnvironment(final PipelineOptions options) {
        final FlexTemplateRuntimeEnvironment.Builder builder = FlexTemplateRuntimeEnvironment.newBuilder();
        final String serviceAccount = dataflowOption(options, "getServiceAccount");
        if(serviceAccount != null) {
            builder.setServiceAccountEmail(serviceAccount);
        }
        final String subnetwork = dataflowOption(options, "getSubnetwork");
        if(subnetwork != null) {
            builder.setSubnetwork(subnetwork);
        }
        final String tempLocation = options.getTempLocation();
        if(tempLocation != null && tempLocation.startsWith("gs://")) {
            builder.setTempLocation(tempLocation);
            // the staging default factory derives from a GCS tempLocation; without one it throws or creates a bucket
            final String stagingLocation = dataflowOption(options, "getStagingLocation");
            if(stagingLocation != null) {
                builder.setStagingLocation(stagingLocation);
            }
        }
        final FlexTemplateRuntimeEnvironment environment = builder.build();
        if(environment.equals(FlexTemplateRuntimeEnvironment.getDefaultInstance())) {
            return null;
        }
        try {
            return JsonFormat.printer().omittingInsignificantWhitespace().print(environment);
        } catch (final InvalidProtocolBufferException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String template(final String text, final Map<String, Object> data) {
        return TemplateUtil.executeStrictTemplateIfNeeded(text, data);
    }

}
