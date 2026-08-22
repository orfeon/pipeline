package com.mercari.solution.module.action;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.Sleeper;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.mercari.solution.config.options.DataflowOptions;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.MElement;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.cloud.google.BigQueryUtil;
import org.apache.beam.sdk.options.PipelineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Action service that runs a BigQuery job (query or load) via the BigQuery Jobs API.
 *
 * Job submission is idempotent against Beam bundle retries: the job id is derived
 * deterministically from the pipeline job name, the step name and the effective parameters,
 * so a retried submission gets 409 ALREADY_EXISTS and falls through to polling the
 * already-running job instead of starting a duplicate one.
 *
 * Templates: with {@code trigger: perElement}, {@code ${field}} expressions in
 * {@code query}, {@code sourceUris}, {@code destinationTable} and {@code jobId} are expanded
 * with the element's values. With {@code trigger: collect}, the same parameters can use the
 * {@code elements} (list of field maps) and {@code size} template variables, and
 * {@code sourceUrisField} gathers one field's value from every element into {@code sourceUris}
 * (e.g. load every written file in a single load job).
 */
@Action.Service(name = "bigquery")
public class BigQueryAction implements Action {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryAction.class);

    public static class Parameters implements Serializable {

        public Op op;
        public String projectId;

        // for query job
        public String query;
        public Boolean useLegacySql;
        public Priority priority;

        // for load job
        public List<String> sourceUris;
        public String sourceUrisField;
        public String sourceFormat;

        // common
        public String destinationTable;
        public WriteDisposition writeDisposition;
        public CreateDisposition createDisposition;
        public String location;
        public String jobId;
        public String jobIdPrefix;
        public Boolean wait;
        public Long timeoutSeconds;
        public String quotaUser;
        public Map<String, String> labels;

        public List<String> validate(final String name, final Trigger trigger) {
            final List<String> errorMessages = new ArrayList<>();
            if(this.op == null) {
                errorMessages.add("action module[" + name + "].parameters.op must not be null");
            } else {
                switch (this.op) {
                    case query -> {
                        if(this.query == null) {
                            errorMessages.add("action module[" + name + "].parameters.query must not be null");
                        }
                    }
                    case load -> {
                        final boolean hasSourceUris = this.sourceUris != null && !this.sourceUris.isEmpty();
                        final boolean hasSourceUrisField = this.sourceUrisField != null;
                        if(!hasSourceUris && !hasSourceUrisField) {
                            errorMessages.add("action module[" + name + "].parameters requires sourceUris or sourceUrisField");
                        }
                        if(hasSourceUrisField && !Trigger.collect.equals(trigger)) {
                            errorMessages.add("action module[" + name + "].parameters.sourceUrisField requires trigger: collect");
                        }
                        if(this.destinationTable == null) {
                            errorMessages.add("action module[" + name + "].parameters.destinationTable must not be null");
                        }
                    }
                }
            }
            if(this.timeoutSeconds != null && this.timeoutSeconds <= 0) {
                errorMessages.add("action module[" + name + "].parameters.timeoutSeconds must be positive");
            }
            return errorMessages;
        }

        public void setDefaults(final String defaultProjectId) {
            if(this.projectId == null) {
                this.projectId = defaultProjectId;
            }
            if(this.wait == null) {
                this.wait = true;
            }
            if(this.timeoutSeconds == null) {
                this.timeoutSeconds = 86400L;
            }
            switch (this.op) {
                case query -> {
                    if(this.useLegacySql == null) {
                        this.useLegacySql = false;
                    }
                    if(this.priority == null) {
                        this.priority = Priority.INTERACTIVE;
                    }
                }
                case load -> {}
            }
        }

    }

    public enum Op {
        query,
        load
    }

    public enum Priority {
        INTERACTIVE,
        BATCH
    }

    public enum WriteDisposition {
        WRITE_TRUNCATE,
        WRITE_APPEND,
        WRITE_EMPTY
    }

    public enum CreateDisposition {
        CREATE_IF_NEEDED,
        CREATE_NEVER
    }

    private String name;
    private String jobName;
    private String defaultProjectId;
    private Trigger trigger;
    private Parameters parameters;

    private transient Bigquery bigquery;


    @Override
    public void configure(final String name, final JsonObject parametersJson, final PipelineOptions options) {
        this.name = name;
        this.jobName = options.getJobName();
        this.defaultProjectId = DataflowOptions.getProject(options);
        this.trigger = Trigger.of(parametersJson);
        this.parameters = new Gson().fromJson(parametersJson, Parameters.class);
        if(this.parameters == null) {
            throw new IllegalModuleException("action module[" + name + "].parameters must not be empty");
        }
        final List<String> errorMessages = this.parameters.validate(name, trigger);
        if(!errorMessages.isEmpty()) {
            throw new IllegalModuleException(errorMessages);
        }
        this.parameters.setDefaults(defaultProjectId);
    }

    @Override
    public void setup() {
        this.bigquery = BigQueryUtil.getBigquery();
    }

    @Override
    public ActionResult execute(final List<MElement> elements) throws Exception {
        final Parameters p = templateParameters(elements);
        final Job job = switch (p.op) {
            case query -> executeJob(p, createQueryJobConfiguration(p));
            case load -> executeJob(p, createLoadJobConfiguration(p));
        };
        final String state = Optional.ofNullable(job.getStatus()).map(s -> s.getState()).orElse(null);
        final String payload = Optional.ofNullable(job.getStatistics()).map(Object::toString).orElse(null);
        return ActionResult.of(p.op.name(), job.getJobReference().getJobId(), state, payload);
    }

    /**
     * Expands templates in the templatable string parameters (query, sourceUris,
     * destinationTable, jobId). perElement exposes the element's fields directly
     * (e.g. {@code ${path}}); collect exposes {@code elements} and {@code size}.
     * Values are the elements' primitive representation (e.g. timestamps as epoch micros).
     */
    private Parameters templateParameters(final List<MElement> elements) {
        if(Trigger.once.equals(trigger)) {
            return parameters;
        }
        final Map<String, Object> data = switch (trigger) {
            case perElement -> elements.getFirst().asPrimitiveMap();
            case collect -> Action.createCollectTemplateData(elements);
            default -> throw new IllegalStateException();
        };
        final Parameters p = new Gson().fromJson(new Gson().toJson(parameters), Parameters.class);
        p.query = template(p.query, data);
        p.destinationTable = template(p.destinationTable, data);
        p.jobId = template(p.jobId, data);
        if(p.sourceUris != null) {
            p.sourceUris = p.sourceUris.stream().map(uri -> template(uri, data)).toList();
        }
        if(Trigger.collect.equals(trigger) && p.sourceUrisField != null) {
            final List<String> uris = new ArrayList<>();
            for(final MElement element : elements) {
                final Object value = element.getPrimitiveValue(p.sourceUrisField);
                if(value != null) {
                    uris.add(value.toString());
                }
            }
            if(uris.isEmpty()) {
                throw new IllegalStateException(
                        "action module[" + name + "] sourceUrisField: " + p.sourceUrisField + " matched no value in collected elements");
            }
            p.sourceUris = uris;
        }
        return p;
    }

    private static String template(final String text, final Map<String, Object> data) {
        if(!TemplateUtil.isTemplateText(text)) {
            return text;
        }
        return TemplateUtil.executeStrictTemplate(text, data);
    }

    private JobConfiguration createQueryJobConfiguration(final Parameters p) {
        final JobConfigurationQuery query = new JobConfigurationQuery();
        query.setQuery(p.query);
        query.setPriority(p.priority.name());
        query.setUseLegacySql(p.useLegacySql);
        if(p.destinationTable != null) {
            query.setDestinationTable(BigQueryUtil.getTableReference(p.destinationTable, p.projectId));
        }
        if(p.createDisposition != null) {
            query.setCreateDisposition(p.createDisposition.name());
        }
        if(p.writeDisposition != null) {
            query.setWriteDisposition(p.writeDisposition.name());
        }
        return new JobConfiguration()
                .setQuery(query)
                .setJobType("QUERY");
    }

    private JobConfiguration createLoadJobConfiguration(final Parameters p) {
        final JobConfigurationLoad load = new JobConfigurationLoad();
        load.setSourceUris(p.sourceUris);
        load.setDestinationTable(BigQueryUtil.getTableReference(p.destinationTable, p.projectId));
        if(p.sourceFormat != null) {
            load.setSourceFormat(p.sourceFormat);
        }
        if(p.createDisposition != null) {
            load.setCreateDisposition(p.createDisposition.name());
        }
        if(p.writeDisposition != null) {
            load.setWriteDisposition(p.writeDisposition.name());
        }
        return new JobConfiguration()
                .setLoad(load)
                .setJobType("LOAD");
    }

    private Job executeJob(final Parameters p, final JobConfiguration configuration) throws Exception {
        if(p.labels != null && !p.labels.isEmpty()) {
            configuration.setLabels(p.labels);
        }
        final String jobId = Optional.ofNullable(p.jobId).orElseGet(() -> createDeterministicJobId(p));
        final JobReference jobReference = new JobReference()
                .setProjectId(p.projectId)
                .setJobId(jobId);
        if(p.location != null) {
            jobReference.setLocation(p.location);
        }
        final Job request = new Job()
                .setJobReference(jobReference)
                .setConfiguration(configuration);

        Job job;
        try {
            final Bigquery.Jobs.Insert insert = bigquery.jobs().insert(p.projectId, request);
            if(p.quotaUser != null) {
                insert.setQuotaUser(p.quotaUser);
            }
            job = insert.execute();
            LOG.info("action module[{}] submitted bigquery {} job: {}", name, p.op, jobId);
        } catch (final GoogleJsonResponseException e) {
            if(e.getStatusCode() == 409) {
                // Retried bundle: the job was already submitted by a previous attempt. Adopt it.
                LOG.info("action module[{}] bigquery job: {} already exists, adopting the existing job", name, jobId);
                job = bigquery.jobs().get(p.projectId, jobId).setLocation(p.location).execute();
            } else {
                throw e;
            }
        }

        if(!p.wait) {
            return job;
        }

        final ExponentialBackOff backOff = new ExponentialBackOff.Builder()
                .setInitialIntervalMillis(2000)
                .setMaxIntervalMillis(30000)
                .setMaxElapsedTimeMillis(Math.toIntExact(Math.min(p.timeoutSeconds * 1000L, Integer.MAX_VALUE)))
                .build();
        final Job completed = BigQueryUtil.pollJob(bigquery, job.getJobReference(), Sleeper.DEFAULT, backOff);
        if(completed == null) {
            throw new IllegalStateException(
                    "bigquery job: " + jobId + " did not complete within timeoutSeconds: " + p.timeoutSeconds);
        }
        if(!BigQueryUtil.isJobResultSucceeded(completed)) {
            throw new IllegalStateException(
                    "bigquery job: " + jobId + " failed with error: " + completed.getStatus().getErrorResult());
        }
        return completed;
    }

    /**
     * The id is stable across bundle retries within a pipeline run (jobName), but distinct
     * across steps, across parameter values (and thus across perElement/collect trigger
     * firings with distinct effective parameters) and across pipeline runs with distinct
     * job names.
     */
    private String createDeterministicJobId(final Parameters p) {
        final String prefix = Optional.ofNullable(p.jobIdPrefix).orElse("mp-action");
        final String seed = String.join("\n", jobName, name, new Gson().toJson(p));
        return prefix + "-" + sanitize(name) + "-" + sha256Hex(seed).substring(0, 32);
    }

    private static String sanitize(final String text) {
        return text.replaceAll("[^a-zA-Z0-9_-]", "_");
    }

    private static String sha256Hex(final String text) {
        return com.mercari.solution.util.domain.text.template.StringFunctions.sha256Hex(text);
    }

}
