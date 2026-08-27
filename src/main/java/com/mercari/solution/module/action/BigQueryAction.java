package com.mercari.solution.module.action;

import com.mercari.solution.module.Action;
import com.mercari.solution.module.Schema;
import com.mercari.solution.module.Action.Trigger;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.util.Data;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.GenericData;
import com.google.api.client.util.Sleeper;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Clustering;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.ViewDefinition;
import com.google.api.services.bigquery.model.ConnectionProperty;
import com.google.api.services.bigquery.model.ErrorProto;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.HivePartitioningOptions;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationExtract;
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobConfigurationTableCopy;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.ModelReference;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.ParquetOptions;
import com.google.api.services.bigquery.model.QueryParameter;
import com.google.api.services.bigquery.model.QueryParameterType;
import com.google.api.services.bigquery.model.QueryParameterValue;
import com.google.api.services.bigquery.model.RangePartitioning;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.mercari.solution.util.schema.converter.ElementToTableRowConverter;
import com.mercari.solution.config.options.DataflowOptions;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.MElement;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.cloud.google.BigQueryUtil;
import com.mercari.solution.util.pipeline.outbound.Durations;
import org.apache.beam.sdk.options.PipelineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Action service for the BigQuery API: runs a job (query, load, extract or copy) via the Jobs API,
 * waits for a job launched elsewhere ({@code jobs.wait}), or manages tables and datasets
 * ({@code tables.get} as a metadata guard, {@code tables.insert} / {@code tables.patch} /
 * {@code tables.delete}, {@code datasets.get} / {@code datasets.insert} / {@code datasets.delete}).
 *
 * Job submission is idempotent against Beam bundle retries: the job id is derived
 * deterministically from the pipeline job name, the step name and the effective parameters,
 * so a retried submission gets 409 ALREADY_EXISTS and adopts the already-running (or
 * succeeded) job instead of starting a duplicate one. When the adopted job has already
 * failed with a transient reason, the job is resubmitted under the same id with a
 * {@code -r<n>} suffix (so a retry can recover) — a permanent failure reason is reported as
 * {@link NonRetryableException} instead.
 *
 * The result payload is the {@code Job} resource of the Jobs API as a typed structure
 * (int64 fields as numbers), so module-level {@code failWhen} / {@code skipWhen} conditions can
 * reference e.g. {@code payload.statistics.query.numDmlAffectedRows}.
 *
 * Templates: with {@code trigger: perElement}, {@code ${field}} expressions in
 * {@code query}, {@code queryParameters} values, {@code sourceUris}, {@code destinationTable}, {@code defaultDataset},
 * {@code jobId}, {@code reservation}, {@code hivePartitioningOptions.sourceUriPrefix}, {@code sourceTable} / {@code sourceModel},
 * {@code destinationUris}, {@code sourceTables}, {@code destinationExpirationTime} and label values are expanded
 * with the element's values. {@code sourceUrisField} / {@code sourceTablesField} gather a field from every collected element. With {@code trigger: collect}, the same
 * parameters can use the {@code elements} (list of field maps) and {@code size} template variables,
 * and {@code sourceUrisField} gathers one field's value from every element into {@code sourceUris}
 * (e.g. load every written file in a single load job).
 */
@Action.Service(name = "bigquery", operations = {
        "jobs.query", "jobs.load", "jobs.extract", "jobs.copy", "jobs.wait",
        "tables.get", "tables.insert", "tables.patch", "tables.delete",
        "datasets.get", "datasets.insert", "datasets.delete"})
public class BigQueryAction implements ActionService {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryAction.class);

    private static final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();

    /** Max {@code -r<n>} resubmissions of a job whose earlier attempt failed transiently. */
    static final int MAX_RESUBMITS = 10;

    /**
     * Job error reasons that a resubmission may fix (BigQuery error table). Anything else
     * (invalidQuery, invalid, notFound, accessDenied, duplicate, resourcesExceeded, stopped, …)
     * is reported as non-retryable.
     */
    static final Set<String> RETRYABLE_REASONS = Set.of(
            "rateLimitExceeded", "quotaExceeded", "backendError", "internalError",
            "jobBackendError", "jobInternalError", "jobRateLimitExceeded");

    public static class Parameters implements Serializable {

        public Op op;
        public String projectId;

        // for query job
        public String query;
        public Boolean useLegacySql;
        public Priority priority;
        /** Named parameters ({@code {name: value}} / {@code {name: {type, value}}}) or a list of raw API QueryParameter objects. */
        public JsonElement queryParameters;
        public String queryParametersJson;
        public String defaultDataset;
        public Long maximumBytesBilled;
        public Boolean useQueryCache;
        public Map<String, String> connectionProperties;

        // for load job
        public List<String> sourceUris;
        public String sourceUrisField;
        public String sourceFormat;
        /** Destination schema in the common schema notation (see module/common/schema.md). */
        public JsonElement schema;
        public String tableSchemaJson;
        public Boolean autodetect;
        public Boolean ignoreUnknownValues;
        public Integer maxBadRecords;
        public CsvOptions csvOptions;
        public ParquetOptionsParameters parquetOptions;
        public Boolean useAvroLogicalTypes;
        public String jsonExtension;
        public HivePartitioning hivePartitioningOptions;
        public List<String> decimalTargetTypes;

        /** jobs.query with wait: fetch up to this many result rows into the payload ({@code resultRows}, {@code firstRow}). */
        public Integer resultRows;

        // for jobs.wait
        public String jobIdField;

        // for tables.* / datasets.*
        public String table;
        public String dataset;
        public Boolean ignoreNotFound;
        public Boolean ifNotExists;
        public Boolean deleteContents;
        public String description;
        public String expirationTime;
        public Long defaultTableExpirationMs;
        public String view;
        public Boolean requirePartitionFilter;
        /** Raw Table / Dataset resource JSON merged under the parameters above (explicit parameters win). */
        public JsonObject resource;
        public String resourceJson;

        // for extract job
        public String sourceTable;
        public String sourceModel;
        public List<String> destinationUris;
        public String destinationFormat;
        public String compression;
        public String fieldDelimiter;
        public Boolean printHeader;

        // for copy job
        public List<String> sourceTables;
        public String sourceTablesField;
        public OperationType operationType;
        /** RFC 3339 timestamp, or a duration ({@code 7d}, {@code PT168H}) relative to the execution time. */
        public String destinationExpirationTime;

        // common
        public String destinationTable;
        public WriteDisposition writeDisposition;
        public CreateDisposition createDisposition;
        public TimePartitioningParameters timePartitioning;
        public RangePartitioningParameters rangePartitioning;
        public List<String> clustering;
        public List<String> schemaUpdateOptions;
        public String location;
        public String jobId;
        public String jobIdPrefix;
        public Boolean wait;
        public Long timeoutSeconds;
        public Boolean cancelOnTimeout;
        public Long jobTimeoutMs;
        public String reservation;
        public Boolean dryRun;
        public String quotaUser;
        public Map<String, String> labels;
        /** Raw {@code JobConfiguration} JSON merged under the parameters above (explicit parameters win). */
        public JsonObject configuration;
        public String configurationJson;

        public List<String> validate(final String name, final Trigger trigger) {
            final List<String> errorMessages = new ArrayList<>();
            {
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
                    case extract -> {
                        if((this.sourceTable == null) == (this.sourceModel == null)) {
                            errorMessages.add("action module[" + name + "].parameters requires exactly one of sourceTable or sourceModel");
                        }
                        if(this.destinationUris == null || this.destinationUris.isEmpty()) {
                            errorMessages.add("action module[" + name + "].parameters.destinationUris must not be empty");
                        }
                    }
                    case wait -> {
                        if(this.jobId == null && this.jobIdField == null) {
                            errorMessages.add("action module[" + name + "].parameters requires jobId or jobIdField");
                        }
                        if(this.jobIdField != null && !Trigger.collect.equals(trigger)) {
                            errorMessages.add("action module[" + name + "].parameters.jobIdField requires trigger: collect");
                        }
                    }
                    case tableGet, tableDelete, tablePatch -> {
                        if(this.table == null) {
                            errorMessages.add("action module[" + name + "].parameters.table must not be null");
                        }
                    }
                    case tableInsert -> {
                        if(this.table == null) {
                            errorMessages.add("action module[" + name + "].parameters.table must not be null");
                        }
                        if(this.schema == null && this.view == null && this.resource == null) {
                            errorMessages.add("action module[" + name + "].parameters requires schema, view or resource for tables.insert");
                        }
                    }
                    case datasetGet, datasetInsert, datasetDelete -> {
                        if(this.dataset == null) {
                            errorMessages.add("action module[" + name + "].parameters.dataset must not be null");
                        }
                    }
                    case copy -> {
                        final boolean hasSourceTables = this.sourceTables != null && !this.sourceTables.isEmpty();
                        final boolean hasSourceTablesField = this.sourceTablesField != null;
                        if(!hasSourceTables && !hasSourceTablesField) {
                            errorMessages.add("action module[" + name + "].parameters requires sourceTables or sourceTablesField");
                        }
                        if(hasSourceTablesField && !Trigger.collect.equals(trigger)) {
                            errorMessages.add("action module[" + name + "].parameters.sourceTablesField requires trigger: collect");
                        }
                        if(this.destinationTable == null) {
                            errorMessages.add("action module[" + name + "].parameters.destinationTable must not be null");
                        }
                        if(this.destinationExpirationTime != null && !TemplateUtil.isTemplateText(this.destinationExpirationTime)) {
                            try {
                                resolveExpirationTime(this.destinationExpirationTime, java.time.Instant.EPOCH);
                            } catch (final RuntimeException e) {
                                errorMessages.add("action module[" + name + "].parameters.destinationExpirationTime is illegal: " + e.getMessage());
                            }
                        }
                    }
                }
            }
            if(Op.query.equals(this.op) && this.resultRows != null) {
                if(this.resultRows <= 0) {
                    errorMessages.add("action module[" + name + "].parameters.resultRows must be positive");
                }
                if(Boolean.FALSE.equals(this.wait) || Boolean.TRUE.equals(this.dryRun)) {
                    errorMessages.add("action module[" + name + "].parameters.resultRows requires wait: true and no dryRun");
                }
            }
            if(Op.query.equals(this.op) && this.queryParameters != null) {
                if(!this.queryParameters.isJsonObject() && !this.queryParameters.isJsonArray()) {
                    errorMessages.add("action module[" + name + "].parameters.queryParameters must be an object {name: value} or a list of QueryParameter objects");
                } else {
                    try {
                        createQueryParameters(this.queryParameters);
                    } catch (final RuntimeException | IOException e) {
                        errorMessages.add("action module[" + name + "].parameters.queryParameters is illegal: " + e.getMessage());
                    }
                }
            }
            if(this.expirationTime != null && !TemplateUtil.isTemplateText(this.expirationTime)) {
                try {
                    resolveExpirationTime(this.expirationTime, java.time.Instant.EPOCH);
                } catch (final RuntimeException e) {
                    errorMessages.add("action module[" + name + "].parameters.expirationTime is illegal: " + e.getMessage());
                }
            }
            if(this.resource != null) {
                try {
                    if(Op.datasetInsert.equals(this.op)) {
                        JSON_FACTORY.fromString(this.resource.toString(), Dataset.class);
                    } else {
                        JSON_FACTORY.fromString(this.resource.toString(), Table.class);
                    }
                } catch (final IOException | IllegalArgumentException e) {
                    errorMessages.add("action module[" + name + "].parameters.resource is not a valid resource: " + e.getMessage());
                }
            }
            if(Op.load.equals(this.op) || Op.tableInsert.equals(this.op) || Op.tablePatch.equals(this.op)) {
                if(this.schema != null && Boolean.TRUE.equals(this.autodetect)) {
                    errorMessages.add("action module[" + name + "].parameters.schema and autodetect are exclusive");
                }
                if(this.schema != null) {
                    try {
                        ElementToTableRowConverter.convertSchema(com.mercari.solution.module.Schema.parse(this.schema));
                    } catch (final RuntimeException e) {
                        errorMessages.add("action module[" + name + "].parameters.schema is illegal: " + e.getMessage());
                    }
                }
                if(this.maxBadRecords != null && this.maxBadRecords < 0) {
                    errorMessages.add("action module[" + name + "].parameters.maxBadRecords must not be negative");
                }
            }
            if(this.timePartitioning != null && this.rangePartitioning != null) {
                errorMessages.add("action module[" + name + "].parameters.timePartitioning and rangePartitioning are exclusive");
            }
            if(this.timePartitioning != null && this.timePartitioning.type == null) {
                errorMessages.add("action module[" + name + "].parameters.timePartitioning.type must not be null (DAY, HOUR, MONTH, YEAR)");
            }
            if(this.rangePartitioning != null) {
                errorMessages.addAll(this.rangePartitioning.validate("action module[" + name + "].parameters.rangePartitioning"));
            }
            if(this.schemaUpdateOptions != null) {
                for(final String option : this.schemaUpdateOptions) {
                    if(!"ALLOW_FIELD_ADDITION".equals(option) && !"ALLOW_FIELD_RELAXATION".equals(option)) {
                        errorMessages.add("action module[" + name + "].parameters.schemaUpdateOptions contains an unknown option: " + option);
                    }
                }
            }
            if(this.timeoutSeconds != null && this.timeoutSeconds <= 0) {
                errorMessages.add("action module[" + name + "].parameters.timeoutSeconds must be positive");
            }
            if(this.jobTimeoutMs != null && this.jobTimeoutMs <= 0) {
                errorMessages.add("action module[" + name + "].parameters.jobTimeoutMs must be positive");
            }
            if(this.configuration != null) {
                try {
                    JSON_FACTORY.fromString(this.configuration.toString(), JobConfiguration.class);
                } catch (final IOException | IllegalArgumentException e) {
                    errorMessages.add("action module[" + name + "].parameters.configuration is not a JobConfiguration: " + e.getMessage());
                }
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
            if(this.cancelOnTimeout == null) {
                this.cancelOnTimeout = true;
            }
            if(this.dryRun == null) {
                this.dryRun = false;
            }
            if(this.configuration != null) {
                // JsonObject is not Serializable: keep the text for the worker
                this.configurationJson = this.configuration.toString();
                this.configuration = null;
            }
            if(this.queryParameters != null) {
                this.queryParametersJson = this.queryParameters.toString();
                this.queryParameters = null;
            }
            if(this.resource != null) {
                this.resourceJson = this.resource.toString();
                this.resource = null;
            }
            if(this.schema != null) {
                try {
                    this.tableSchemaJson = JSON_FACTORY.toString(
                            ElementToTableRowConverter.convertSchema(com.mercari.solution.module.Schema.parse(this.schema)));
                } catch (final IOException e) {
                    throw new IllegalModuleException("failed to convert parameters.schema: " + e.getMessage());
                }
                this.schema = null;
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
                case extract -> {
                    if(this.destinationFormat == null) {
                        this.destinationFormat = "CSV";
                    }
                }
                case copy -> {
                    if(this.operationType == null) {
                        this.operationType = OperationType.COPY;
                    }
                }
                case wait -> {}
                case tableGet -> {
                    if(this.ignoreNotFound == null) {
                        this.ignoreNotFound = false;
                    }
                }
                case tableDelete, datasetDelete -> {
                    if(this.ignoreNotFound == null) {
                        this.ignoreNotFound = true;
                    }
                    if(this.deleteContents == null) {
                        this.deleteContents = false;
                    }
                }
                case datasetGet -> {
                    if(this.ignoreNotFound == null) {
                        this.ignoreNotFound = false;
                    }
                }
                case tableInsert, datasetInsert -> {
                    if(this.ifNotExists == null) {
                        this.ifNotExists = true;
                    }
                }
                case tablePatch -> {}
            }
        }

    }

    /** Operations; {@code operation} is the config value (also listed in {@code @Action.Service}). */
    public enum Op {
        query("jobs.query"),
        load("jobs.load"),
        extract("jobs.extract"),
        copy("jobs.copy"),
        wait("jobs.wait"),
        tableGet("tables.get"),
        tableInsert("tables.insert"),
        tablePatch("tables.patch"),
        tableDelete("tables.delete"),
        datasetGet("datasets.get"),
        datasetInsert("datasets.insert"),
        datasetDelete("datasets.delete");

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

    public enum OperationType {
        COPY,
        SNAPSHOT,
        RESTORE,
        CLONE
    }

    public static class TimePartitioningParameters implements Serializable {
        public String type;
        public String field;
        public Long expirationMs;
        public Boolean requirePartitionFilter;

        TimePartitioning toApi() {
            final TimePartitioning t = new TimePartitioning().setType(type);
            if(field != null) {
                t.setField(field);
            }
            if(expirationMs != null) {
                t.setExpirationMs(expirationMs);
            }
            if(requirePartitionFilter != null) {
                t.setRequirePartitionFilter(requirePartitionFilter);
            }
            return t;
        }
    }

    public static class RangePartitioningParameters implements Serializable {
        public String field;
        public Long start;
        public Long end;
        public Long interval;

        List<String> validate(final String prefix) {
            final List<String> errorMessages = new ArrayList<>();
            if(field == null) {
                errorMessages.add(prefix + ".field must not be null");
            }
            if(start == null || end == null || interval == null) {
                errorMessages.add(prefix + " requires start, end and interval");
            } else if(interval <= 0 || end <= start) {
                errorMessages.add(prefix + " requires interval > 0 and end > start");
            }
            return errorMessages;
        }

        RangePartitioning toApi() {
            return new RangePartitioning()
                    .setField(field)
                    .setRange(new RangePartitioning.Range().setStart(start).setEnd(end).setInterval(interval));
        }
    }

    public static class CsvOptions implements Serializable {
        public Integer skipLeadingRows;
        public String fieldDelimiter;
        public String quote;
        public Boolean allowQuotedNewlines;
        public Boolean allowJaggedRows;
        public String encoding;
        public String nullMarker;
        public Boolean preserveAsciiControlCharacters;

        void apply(final JobConfigurationLoad load) {
            if(skipLeadingRows != null) {
                load.setSkipLeadingRows(skipLeadingRows);
            }
            if(fieldDelimiter != null) {
                load.setFieldDelimiter(fieldDelimiter);
            }
            if(quote != null) {
                load.setQuote(quote);
            }
            if(allowQuotedNewlines != null) {
                load.setAllowQuotedNewlines(allowQuotedNewlines);
            }
            if(allowJaggedRows != null) {
                load.setAllowJaggedRows(allowJaggedRows);
            }
            if(encoding != null) {
                load.setEncoding(encoding);
            }
            if(nullMarker != null) {
                load.setNullMarker(nullMarker);
            }
            if(preserveAsciiControlCharacters != null) {
                load.setPreserveAsciiControlCharacters(preserveAsciiControlCharacters);
            }
        }
    }

    public static class ParquetOptionsParameters implements Serializable {
        public Boolean enumAsString;
        public Boolean enableListInference;

        ParquetOptions toApi() {
            final ParquetOptions o = new ParquetOptions();
            if(enumAsString != null) {
                o.setEnumAsString(enumAsString);
            }
            if(enableListInference != null) {
                o.setEnableListInference(enableListInference);
            }
            return o;
        }
    }

    public static class HivePartitioning implements Serializable {
        public String mode;
        public String sourceUriPrefix;
        public Boolean requirePartitionFilter;

        HivePartitioningOptions toApi() {
            final HivePartitioningOptions o = new HivePartitioningOptions();
            if(mode != null) {
                o.setMode(mode);
            }
            if(sourceUriPrefix != null) {
                o.setSourceUriPrefix(sourceUriPrefix);
            }
            if(requirePartitionFilter != null) {
                o.setRequirePartitionFilter(requirePartitionFilter);
            }
            return o;
        }
    }

    private String name;
    private String jobName;
    private String defaultProjectId;
    private Trigger trigger;
    private String operation;
    private Parameters parameters;

    private transient Bigquery bigquery;
    private transient Sleeper sleeper;


    @Override
    public void configure(final String name, final Trigger trigger, final String operation, final JsonObject parametersJson, final PipelineOptions options, final Schema inputSchema) {
        this.name = name;
        this.jobName = options.getJobName();
        this.defaultProjectId = DataflowOptions.getProject(options);
        this.trigger = trigger;
        this.operation = operation;
        this.parameters = new Gson().fromJson(parametersJson, Parameters.class);
        if(this.parameters == null) {
            throw new IllegalModuleException("action module[" + name + "].parameters must not be empty");
        }
        this.parameters.op = Op.of(operation);
        final List<String> errorMessages = this.parameters.validate(name, trigger);
        if(!errorMessages.isEmpty()) {
            throw new IllegalModuleException(errorMessages);
        }
        this.parameters.setDefaults(defaultProjectId);
    }

    @Override
    public void setup() {
        if(this.bigquery == null) {
            this.bigquery = BigQueryUtil.getBigquery();
        }
        if(this.sleeper == null) {
            this.sleeper = Sleeper.DEFAULT;
        }
    }

    /** Test hook: use a prepared client (e.g. over a mock transport) instead of the default one. */
    void setBigquery(final Bigquery bigquery, final Sleeper sleeper) {
        this.bigquery = bigquery;
        this.sleeper = sleeper;
    }

    Parameters getParameters() {
        return parameters;
    }

    @Override
    public ActionResult execute(final List<MElement> elements) throws Exception {
        final Parameters p = templateParameters(elements);
        if(Op.query.equals(p.op) && (p.query == null || p.query.isBlank())) {
            // a template that resolved to nothing (e.g. a cdc SCHEMA record without a generated
            // statement): nothing to run, report it instead of submitting an empty job
            LOG.info("bigquery action[{}] skips an empty query", name);
            return ActionResult.of(operation, null, "SKIPPED", null);
        }
        switch (p.op) {
            case wait -> {
                return executeWait(p, elements);
            }
            case tableGet -> {
                return executeTableGet(p);
            }
            case tableDelete -> {
                return executeTableDelete(p);
            }
            case tableInsert -> {
                return executeTableInsert(p);
            }
            case tablePatch -> {
                return executeTablePatch(p);
            }
            case datasetGet -> {
                return executeDatasetGet(p);
            }
            case datasetInsert -> {
                return executeDatasetInsert(p);
            }
            case datasetDelete -> {
                return executeDatasetDelete(p);
            }
            default -> {}
        }
        final Job job = switch (p.op) {
            case query -> executeJob(p, createQueryJobConfiguration(p));
            case load -> executeJob(p, createLoadJobConfiguration(p));
            case extract -> executeJob(p, createExtractJobConfiguration(p));
            case copy -> executeJob(p, createCopyJobConfiguration(p));
            default -> throw new IllegalStateException();
        };
        final String state = Optional.ofNullable(job.getStatus()).map(s -> s.getState()).orElse(null);
        final Map<String, Object> payload = toPayload(job);
        if(Op.query.equals(p.op) && p.resultRows != null && "DONE".equals(state)) {
            payload.putAll(fetchResultRows(p, job));
        }
        return ActionResult.ofValues(operation, job.getJobReference().getJobId(), state, payload);
    }

    /** Waits for jobs launched elsewhere: {@code jobId} (one) or, with collect, {@code jobIdField} (all gathered ids). */
    private ActionResult executeWait(final Parameters p, final List<MElement> elements) throws Exception {
        final List<String> jobIds = new ArrayList<>();
        if(Trigger.collect.equals(trigger) && p.jobIdField != null) {
            for(final MElement element : elements) {
                final Object value = element.getPrimitiveValue(p.jobIdField);
                if(value != null && !value.toString().isBlank()) {
                    jobIds.add(value.toString());
                }
            }
            if(jobIds.isEmpty()) {
                LOG.info("bigquery action[{}] found no job id in field: {}", name, p.jobIdField);
                return ActionResult.of(operation, null, "SKIPPED", null);
            }
        } else {
            if(p.jobId == null || p.jobId.isBlank()) {
                return ActionResult.of(operation, null, "SKIPPED", null);
            }
            jobIds.add(p.jobId);
        }
        final List<Map<String, Object>> jobs = new ArrayList<>();
        for(final String jobId : jobIds) {
            final Job job = bigquery.jobs().get(p.projectId, jobId).setLocation(p.location).execute();
            final Job completed = waitForCompletion(p, job, false);
            jobs.add(toPayload(completed));
        }
        if(jobs.size() == 1) {
            return ActionResult.ofValues(operation, jobIds.getFirst(), "DONE", jobs.getFirst());
        }
        final Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("jobs", jobs);
        return ActionResult.ofValues(operation, String.join(",", jobIds), "DONE", payload);
    }

    private ActionResult executeTableGet(final Parameters p) throws IOException {
        final TableReference ref = BigQueryUtil.getTableReference(p.table, p.projectId);
        final String id = tableId(ref);
        try {
            final Table table = bigquery.tables().get(ref.getProjectId(), ref.getDatasetId(), ref.getTableId()).execute();
            return ActionResult.ofValues(operation, id, "DONE", toMap(table));
        } catch (final GoogleJsonResponseException e) {
            if(e.getStatusCode() == 404 && p.ignoreNotFound) {
                return ActionResult.of(operation, id, "NOT_FOUND", null);
            }
            final NonRetryableException rejected = rejectedRequest(e);
            if(rejected != null) {
                throw rejected;
            }
            throw e;
        }
    }

    private ActionResult executeTableDelete(final Parameters p) throws IOException {
        final TableReference ref = BigQueryUtil.getTableReference(p.table, p.projectId);
        final String id = tableId(ref);
        try {
            bigquery.tables().delete(ref.getProjectId(), ref.getDatasetId(), ref.getTableId()).execute();
            LOG.info("action module[{}] deleted bigquery table: {}", name, id);
            return ActionResult.of(operation, id, "DELETED", null);
        } catch (final GoogleJsonResponseException e) {
            if(e.getStatusCode() == 404 && p.ignoreNotFound) {
                return ActionResult.of(operation, id, "NOT_FOUND", null);
            }
            final NonRetryableException rejected = rejectedRequest(e);
            if(rejected != null) {
                throw rejected;
            }
            throw e;
        }
    }

    private static String tableId(final TableReference ref) {
        return ref.getProjectId() + "." + ref.getDatasetId() + "." + ref.getTableId();
    }

    private static String datasetId(final DatasetReference ref) {
        return ref.getProjectId() + "." + ref.getDatasetId();
    }

    /** Builds the Table resource from the explicit parameters over the raw {@code resource} base. */
    Table createTable(final Parameters p, final TableReference ref) throws IOException {
        final Table built = new Table().setTableReference(ref);
        if(p.tableSchemaJson != null) {
            built.setSchema(JSON_FACTORY.fromString(p.tableSchemaJson, TableSchema.class));
        }
        if(p.description != null) {
            built.setDescription(p.description);
        }
        if(p.labels != null && !p.labels.isEmpty()) {
            built.setLabels(p.labels);
        }
        if(p.expirationTime != null) {
            built.setExpirationTime(java.time.Instant.parse(resolveExpirationTime(p.expirationTime, java.time.Instant.now())).toEpochMilli());
        }
        if(p.view != null) {
            built.setView(new ViewDefinition().setQuery(p.view).setUseLegacySql(Optional.ofNullable(p.useLegacySql).orElse(false)));
        }
        if(p.requirePartitionFilter != null) {
            built.setRequirePartitionFilter(p.requirePartitionFilter);
        }
        applyDestinationOptions(p, built::setTimePartitioning, built::setRangePartitioning, built::setClustering, options -> {});
        if(p.resourceJson == null) {
            return built;
        }
        final JsonObject base = new Gson().fromJson(p.resourceJson, JsonObject.class);
        deepMerge(base, new Gson().fromJson(JSON_FACTORY.toString(built), JsonObject.class));
        return JSON_FACTORY.fromString(base.toString(), Table.class);
    }

    private ActionResult executeTableInsert(final Parameters p) throws IOException {
        final TableReference ref = BigQueryUtil.getTableReference(p.table, p.projectId);
        final String id = tableId(ref);
        final Table table = createTable(p, ref);
        try {
            final Table created = bigquery.tables().insert(ref.getProjectId(), ref.getDatasetId(), table).execute();
            LOG.info("action module[{}] created bigquery table: {}", name, id);
            return ActionResult.ofValues(operation, id, "CREATED", toMap(created));
        } catch (final GoogleJsonResponseException e) {
            if(e.getStatusCode() == 409 && p.ifNotExists) {
                final Table existing = bigquery.tables().get(ref.getProjectId(), ref.getDatasetId(), ref.getTableId()).execute();
                return ActionResult.ofValues(operation, id, "EXISTS", toMap(existing));
            }
            final NonRetryableException rejected = rejectedRequest(e);
            if(rejected != null) {
                throw rejected;
            }
            throw e;
        }
    }

    private ActionResult executeTablePatch(final Parameters p) throws IOException {
        final TableReference ref = BigQueryUtil.getTableReference(p.table, p.projectId);
        final String id = tableId(ref);
        final Table patch = createTable(p, ref);
        patch.setTableReference(null);
        try {
            final Table patched = bigquery.tables().patch(ref.getProjectId(), ref.getDatasetId(), ref.getTableId(), patch).execute();
            LOG.info("action module[{}] patched bigquery table: {}", name, id);
            return ActionResult.ofValues(operation, id, "DONE", toMap(patched));
        } catch (final GoogleJsonResponseException e) {
            final NonRetryableException rejected = rejectedRequest(e);
            if(rejected != null) {
                throw rejected;
            }
            throw e;
        }
    }

    Dataset createDataset(final Parameters p, final DatasetReference ref) throws IOException {
        final Dataset built = new Dataset().setDatasetReference(ref);
        if(p.location != null) {
            built.setLocation(p.location);
        }
        if(p.description != null) {
            built.setDescription(p.description);
        }
        if(p.labels != null && !p.labels.isEmpty()) {
            built.setLabels(p.labels);
        }
        if(p.defaultTableExpirationMs != null) {
            built.setDefaultTableExpirationMs(p.defaultTableExpirationMs);
        }
        if(p.resourceJson == null) {
            return built;
        }
        final JsonObject base = new Gson().fromJson(p.resourceJson, JsonObject.class);
        deepMerge(base, new Gson().fromJson(JSON_FACTORY.toString(built), JsonObject.class));
        return JSON_FACTORY.fromString(base.toString(), Dataset.class);
    }

    private ActionResult executeDatasetGet(final Parameters p) throws IOException {
        final DatasetReference ref = BigQueryUtil.getDatasetReference(p.dataset, p.projectId);
        final String id = datasetId(ref);
        try {
            final Dataset dataset = bigquery.datasets().get(ref.getProjectId(), ref.getDatasetId()).execute();
            return ActionResult.ofValues(operation, id, "DONE", toMap(dataset));
        } catch (final GoogleJsonResponseException e) {
            if(e.getStatusCode() == 404 && p.ignoreNotFound) {
                return ActionResult.of(operation, id, "NOT_FOUND", null);
            }
            final NonRetryableException rejected = rejectedRequest(e);
            if(rejected != null) {
                throw rejected;
            }
            throw e;
        }
    }

    private ActionResult executeDatasetInsert(final Parameters p) throws IOException {
        final DatasetReference ref = BigQueryUtil.getDatasetReference(p.dataset, p.projectId);
        final String id = datasetId(ref);
        final Dataset dataset = createDataset(p, ref);
        try {
            final Dataset created = bigquery.datasets().insert(ref.getProjectId(), dataset).execute();
            LOG.info("action module[{}] created bigquery dataset: {}", name, id);
            return ActionResult.ofValues(operation, id, "CREATED", toMap(created));
        } catch (final GoogleJsonResponseException e) {
            if(e.getStatusCode() == 409 && p.ifNotExists) {
                final Dataset existing = bigquery.datasets().get(ref.getProjectId(), ref.getDatasetId()).execute();
                return ActionResult.ofValues(operation, id, "EXISTS", toMap(existing));
            }
            final NonRetryableException rejected = rejectedRequest(e);
            if(rejected != null) {
                throw rejected;
            }
            throw e;
        }
    }

    private ActionResult executeDatasetDelete(final Parameters p) throws IOException {
        final DatasetReference ref = BigQueryUtil.getDatasetReference(p.dataset, p.projectId);
        final String id = datasetId(ref);
        try {
            bigquery.datasets().delete(ref.getProjectId(), ref.getDatasetId()).setDeleteContents(p.deleteContents).execute();
            LOG.info("action module[{}] deleted bigquery dataset: {}", name, id);
            return ActionResult.of(operation, id, "DELETED", null);
        } catch (final GoogleJsonResponseException e) {
            if(e.getStatusCode() == 404 && p.ignoreNotFound) {
                return ActionResult.of(operation, id, "NOT_FOUND", null);
            }
            final NonRetryableException rejected = rejectedRequest(e);
            if(rejected != null) {
                throw rejected;
            }
            throw e;
        }
    }

    /** The first {@code resultRows} rows of a completed query as {@code resultRows} (list of maps), {@code firstRow} and {@code totalRows}. */
    private Map<String, Object> fetchResultRows(final Parameters p, final Job job) throws IOException {
        final GetQueryResultsResponse response = bigquery.jobs()
                .getQueryResults(p.projectId, job.getJobReference().getJobId())
                .setLocation(p.location)
                .setMaxResults((long) p.resultRows)
                .execute();
        final List<Map<String, Object>> rows = new ArrayList<>();
        if(response.getRows() != null && response.getSchema() != null) {
            for(final TableRow tableRow : response.getRows()) {
                rows.add(BigQueryUtil.parseAsPrimitiveValues(response.getSchema(), tableRow));
            }
        }
        final Map<String, Object> result = new LinkedHashMap<>();
        // keys avoid SQL reserved words (ROW / ROWS) so conditions need no quoting
        result.put("resultRows", rows);
        result.put("firstRow", rows.isEmpty() ? null : rows.getFirst());
        if(response.getTotalRows() != null) {
            result.put("totalRows", response.getTotalRows());
        }
        return result;
    }

    /**
     * Expands templates in the templatable string parameters (query, sourceUris,
     * destinationTable, jobId, reservation, label values). perElement exposes the element's
     * fields directly (e.g. {@code ${path}}); collect exposes {@code elements} and {@code size}.
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
        p.reservation = template(p.reservation, data);
        p.defaultDataset = template(p.defaultDataset, data);
        if(p.queryParametersJson != null) {
            p.queryParametersJson = templateJson(new Gson().fromJson(p.queryParametersJson, JsonElement.class), data).toString();
        }
        if(p.hivePartitioningOptions != null) {
            p.hivePartitioningOptions.sourceUriPrefix = template(p.hivePartitioningOptions.sourceUriPrefix, data);
        }
        p.sourceTable = template(p.sourceTable, data);
        p.table = template(p.table, data);
        p.dataset = template(p.dataset, data);
        p.description = template(p.description, data);
        p.expirationTime = template(p.expirationTime, data);
        p.view = template(p.view, data);
        p.sourceModel = template(p.sourceModel, data);
        p.destinationExpirationTime = template(p.destinationExpirationTime, data);
        if(p.destinationUris != null) {
            p.destinationUris = p.destinationUris.stream().map(uri -> template(uri, data)).toList();
        }
        if(p.sourceTables != null) {
            p.sourceTables = p.sourceTables.stream().map(t -> template(t, data)).toList();
        }
        if(Trigger.collect.equals(trigger) && p.sourceTablesField != null) {
            final List<String> tables = new ArrayList<>();
            for(final MElement element : elements) {
                final Object value = element.getPrimitiveValue(p.sourceTablesField);
                if(value != null) {
                    tables.add(value.toString());
                }
            }
            if(tables.isEmpty()) {
                throw new IllegalStateException(
                        "action module[" + name + "] sourceTablesField: " + p.sourceTablesField + " matched no value in collected elements");
            }
            p.sourceTables = tables;
        }
        if(p.sourceUris != null) {
            p.sourceUris = p.sourceUris.stream().map(uri -> template(uri, data)).toList();
        }
        if(p.labels != null) {
            final Map<String, String> labels = new LinkedHashMap<>();
            p.labels.forEach((k, v) -> labels.put(k, template(v, data)));
            p.labels = labels;
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

    /** Expands templates in every string primitive of a JSON tree (query parameter values). */
    static JsonElement templateJson(final JsonElement json, final Map<String, Object> data) {
        if(json == null || json.isJsonNull()) {
            return json;
        } else if(json.isJsonPrimitive()) {
            final JsonPrimitive primitive = json.getAsJsonPrimitive();
            if(primitive.isString()) {
                return new JsonPrimitive(template(primitive.getAsString(), data));
            }
            return primitive;
        } else if(json.isJsonArray()) {
            final JsonArray array = new JsonArray();
            for(final JsonElement e : json.getAsJsonArray()) {
                array.add(templateJson(e, data));
            }
            return array;
        } else {
            final JsonObject object = new JsonObject();
            for(final Map.Entry<String, JsonElement> entry : json.getAsJsonObject().entrySet()) {
                object.add(entry.getKey(), templateJson(entry.getValue(), data));
            }
            return object;
        }
    }

    /**
     * Query parameters from the config notation. Object form: {@code name -> value} where value is a
     * primitive (type inferred: BOOL / INT64 / FLOAT64 / STRING), an array of primitives (ARRAY of the
     * inferred element type), or {@code {type, value}} for an explicit scalar/array type
     * (e.g. {@code {type: DATE, value: '2026-08-27'}}). List form: raw API {@code QueryParameter} objects
     * (named or positional).
     */
    static List<QueryParameter> createQueryParameters(final JsonElement json) throws IOException {
        final List<QueryParameter> parameters = new ArrayList<>();
        if(json == null || json.isJsonNull()) {
            return parameters;
        }
        if(json.isJsonArray()) {
            for(final JsonElement e : json.getAsJsonArray()) {
                parameters.add(JSON_FACTORY.fromString(e.toString(), QueryParameter.class));
            }
            return parameters;
        }
        for(final Map.Entry<String, JsonElement> entry : json.getAsJsonObject().entrySet()) {
            final JsonElement value = entry.getValue();
            final QueryParameter parameter = new QueryParameter().setName(entry.getKey());
            if(value.isJsonObject() && value.getAsJsonObject().has("type")) {
                final JsonObject o = value.getAsJsonObject();
                final String type = o.get("type").getAsString().toUpperCase();
                final JsonElement v = o.get("value");
                if(v != null && v.isJsonArray()) {
                    parameter.setParameterType(new QueryParameterType().setType("ARRAY")
                            .setArrayType(new QueryParameterType().setType(type)));
                    parameter.setParameterValue(new QueryParameterValue().setArrayValues(
                            arrayValues(v.getAsJsonArray())));
                } else {
                    parameter.setParameterType(new QueryParameterType().setType(type));
                    parameter.setParameterValue(new QueryParameterValue()
                            .setValue(v == null || v.isJsonNull() ? Data.nullOf(String.class) : v.getAsString()));
                }
            } else if(value.isJsonArray()) {
                final JsonArray array = value.getAsJsonArray();
                final String elementType = array.isEmpty() ? "STRING" : inferType(array.get(0));
                parameter.setParameterType(new QueryParameterType().setType("ARRAY")
                        .setArrayType(new QueryParameterType().setType(elementType)));
                parameter.setParameterValue(new QueryParameterValue().setArrayValues(arrayValues(array)));
            } else if(value.isJsonPrimitive()) {
                parameter.setParameterType(new QueryParameterType().setType(inferType(value)));
                parameter.setParameterValue(new QueryParameterValue().setValue(value.getAsString()));
            } else if(value.isJsonNull()) {
                parameter.setParameterType(new QueryParameterType().setType("STRING"));
                parameter.setParameterValue(new QueryParameterValue().setValue(Data.nullOf(String.class)));
            } else {
                throw new IllegalArgumentException("queryParameters." + entry.getKey() + " must be a primitive, an array or {type, value} but: " + value);
            }
            parameters.add(parameter);
        }
        return parameters;
    }

    private static List<QueryParameterValue> arrayValues(final JsonArray array) {
        final List<QueryParameterValue> values = new ArrayList<>();
        for(final JsonElement e : array) {
            values.add(new QueryParameterValue().setValue(e.isJsonNull() ? Data.nullOf(String.class) : e.getAsString()));
        }
        return values;
    }

    private static String inferType(final JsonElement value) {
        if(!value.isJsonPrimitive()) {
            throw new IllegalArgumentException("query parameter array elements must be primitives but: " + value);
        }
        final JsonPrimitive primitive = value.getAsJsonPrimitive();
        if(primitive.isBoolean()) {
            return "BOOL";
        } else if(primitive.isNumber()) {
            final String text = primitive.getAsString();
            return text.contains(".") || text.contains("e") || text.contains("E") ? "FLOAT64" : "INT64";
        }
        return "STRING";
    }

    private static void applyDestinationOptions(
            final Parameters p,
            final java.util.function.Consumer<TimePartitioning> timePartitioning,
            final java.util.function.Consumer<RangePartitioning> rangePartitioning,
            final java.util.function.Consumer<Clustering> clustering,
            final java.util.function.Consumer<List<String>> schemaUpdateOptions) {

        if(p.timePartitioning != null) {
            timePartitioning.accept(p.timePartitioning.toApi());
        }
        if(p.rangePartitioning != null) {
            rangePartitioning.accept(p.rangePartitioning.toApi());
        }
        if(p.clustering != null && !p.clustering.isEmpty()) {
            clustering.accept(new Clustering().setFields(p.clustering));
        }
        if(p.schemaUpdateOptions != null && !p.schemaUpdateOptions.isEmpty()) {
            schemaUpdateOptions.accept(p.schemaUpdateOptions);
        }
    }

    private JobConfiguration createQueryJobConfiguration(final Parameters p) throws IOException {
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
        if(p.queryParametersJson != null) {
            final List<QueryParameter> queryParameters = createQueryParameters(new Gson().fromJson(p.queryParametersJson, JsonElement.class));
            if(!queryParameters.isEmpty()) {
                query.setQueryParameters(queryParameters);
                final boolean named = queryParameters.stream().allMatch(q -> q.getName() != null && !q.getName().isEmpty());
                query.setParameterMode(named ? "NAMED" : "POSITIONAL");
            }
        }
        if(p.defaultDataset != null) {
            query.setDefaultDataset(BigQueryUtil.getDatasetReference(p.defaultDataset, p.projectId));
        }
        if(p.maximumBytesBilled != null) {
            query.setMaximumBytesBilled(p.maximumBytesBilled);
        }
        if(p.useQueryCache != null) {
            query.setUseQueryCache(p.useQueryCache);
        }
        if(p.connectionProperties != null && !p.connectionProperties.isEmpty()) {
            final List<ConnectionProperty> properties = new ArrayList<>();
            p.connectionProperties.forEach((k, v) -> properties.add(new ConnectionProperty().setKey(k).setValue(v)));
            query.setConnectionProperties(properties);
        }
        applyDestinationOptions(p, query::setTimePartitioning, query::setRangePartitioning, query::setClustering, query::setSchemaUpdateOptions);
        return new JobConfiguration()
                .setQuery(query)
                .setJobType("QUERY");
    }

    private JobConfiguration createLoadJobConfiguration(final Parameters p) throws IOException {
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
        if(p.tableSchemaJson != null) {
            load.setSchema(JSON_FACTORY.fromString(p.tableSchemaJson, TableSchema.class));
        }
        if(p.autodetect != null) {
            load.setAutodetect(p.autodetect);
        }
        if(p.ignoreUnknownValues != null) {
            load.setIgnoreUnknownValues(p.ignoreUnknownValues);
        }
        if(p.maxBadRecords != null) {
            load.setMaxBadRecords(p.maxBadRecords);
        }
        if(p.csvOptions != null) {
            p.csvOptions.apply(load);
        }
        if(p.parquetOptions != null) {
            load.setParquetOptions(p.parquetOptions.toApi());
        }
        if(p.useAvroLogicalTypes != null) {
            load.setUseAvroLogicalTypes(p.useAvroLogicalTypes);
        }
        if(p.jsonExtension != null) {
            load.setJsonExtension(p.jsonExtension);
        }
        if(p.hivePartitioningOptions != null) {
            load.setHivePartitioningOptions(p.hivePartitioningOptions.toApi());
        }
        if(p.decimalTargetTypes != null && !p.decimalTargetTypes.isEmpty()) {
            load.setDecimalTargetTypes(p.decimalTargetTypes);
        }
        applyDestinationOptions(p, load::setTimePartitioning, load::setRangePartitioning, load::setClustering, load::setSchemaUpdateOptions);
        return new JobConfiguration()
                .setLoad(load)
                .setJobType("LOAD");
    }

    private JobConfiguration createExtractJobConfiguration(final Parameters p) {
        final JobConfigurationExtract extract = new JobConfigurationExtract();
        if(p.sourceTable != null) {
            extract.setSourceTable(BigQueryUtil.getTableReference(p.sourceTable, p.projectId));
        } else {
            final TableReference ref = BigQueryUtil.getTableReference(p.sourceModel, p.projectId);
            extract.setSourceModel(new ModelReference()
                    .setProjectId(ref.getProjectId())
                    .setDatasetId(ref.getDatasetId())
                    .setModelId(ref.getTableId()));
        }
        extract.setDestinationUris(p.destinationUris);
        extract.setDestinationFormat(p.destinationFormat);
        if(p.compression != null) {
            extract.setCompression(p.compression);
        }
        if(p.fieldDelimiter != null) {
            extract.setFieldDelimiter(p.fieldDelimiter);
        }
        if(p.printHeader != null) {
            extract.setPrintHeader(p.printHeader);
        }
        if(p.useAvroLogicalTypes != null) {
            extract.setUseAvroLogicalTypes(p.useAvroLogicalTypes);
        }
        return new JobConfiguration()
                .setExtract(extract)
                .setJobType("EXTRACT");
    }

    private JobConfiguration createCopyJobConfiguration(final Parameters p) {
        final JobConfigurationTableCopy copy = new JobConfigurationTableCopy();
        copy.setSourceTables(p.sourceTables.stream().map(t -> BigQueryUtil.getTableReference(t, p.projectId)).toList());
        copy.setDestinationTable(BigQueryUtil.getTableReference(p.destinationTable, p.projectId));
        copy.setOperationType(p.operationType.name());
        if(p.createDisposition != null) {
            copy.setCreateDisposition(p.createDisposition.name());
        }
        if(p.writeDisposition != null) {
            copy.setWriteDisposition(p.writeDisposition.name());
        }
        if(p.destinationExpirationTime != null) {
            copy.setDestinationExpirationTime(resolveExpirationTime(p.destinationExpirationTime, java.time.Instant.now()));
        }
        return new JobConfiguration()
                .setCopy(copy)
                .setJobType("COPY");
    }

    /** An RFC 3339 timestamp as given, or {@code now + duration} for a duration text ({@code 7d}, {@code PT168H}). */
    static String resolveExpirationTime(final String text, final java.time.Instant now) {
        final String t = text.trim();
        try {
            return now.plus(Durations.parse(t)).toString();
        } catch (final IllegalArgumentException e) {
            // not a duration: must be a timestamp
            return java.time.OffsetDateTime.parse(t).toInstant().toString();
        }
    }

    /**
     * Applies the common job settings and the raw {@code configuration} escape hatch: the raw
     * JSON is the base, the configuration built from explicit parameters is merged over it.
     */
    JobConfiguration finishJobConfiguration(final Parameters p, final JobConfiguration built) throws IOException {
        if(p.labels != null && !p.labels.isEmpty()) {
            built.setLabels(p.labels);
        }
        if(p.jobTimeoutMs != null) {
            built.setJobTimeoutMs(p.jobTimeoutMs);
        }
        if(p.reservation != null) {
            built.setReservation(p.reservation);
        }
        if(p.dryRun) {
            built.setDryRun(true);
        }
        if(p.configurationJson == null) {
            return built;
        }
        final JsonObject base = new Gson().fromJson(p.configurationJson, JsonObject.class);
        final JsonObject overlay = new Gson().fromJson(JSON_FACTORY.toString(built), JsonObject.class);
        deepMerge(base, overlay);
        return JSON_FACTORY.fromString(base.toString(), JobConfiguration.class);
    }

    /** Merges {@code overlay} into {@code base} in place: objects recurse, everything else in overlay wins. */
    static void deepMerge(final JsonObject base, final JsonObject overlay) {
        for(final Map.Entry<String, JsonElement> entry : overlay.entrySet()) {
            final JsonElement existing = base.get(entry.getKey());
            if(existing != null && existing.isJsonObject() && entry.getValue().isJsonObject()) {
                deepMerge(existing.getAsJsonObject(), entry.getValue().getAsJsonObject());
            } else {
                base.add(entry.getKey(), entry.getValue());
            }
        }
    }

    private Job executeJob(final Parameters p, final JobConfiguration built) throws Exception {
        final JobConfiguration configuration = finishJobConfiguration(p, built);
        final String baseJobId = Optional.ofNullable(p.jobId).orElseGet(() -> createDeterministicJobId(p));

        final Job job = submitOrAdopt(p, configuration, baseJobId);
        if(!p.wait || p.dryRun) {
            return job;
        }
        return waitForCompletion(p, job, true);
    }

    /**
     * Inserts the job under the deterministic id, adopting the existing job on 409. An adopted
     * job that already finished with a transient error is resubmitted as {@code <id>-r<n>}; a
     * permanent error is non-retryable.
     */
    private Job submitOrAdopt(final Parameters p, final JobConfiguration configuration, final String baseJobId) throws IOException {
        for(int n = 0; n <= MAX_RESUBMITS; n++) {
            final String jobId = n == 0 ? baseJobId : baseJobId + "-r" + n;
            final JobReference jobReference = new JobReference()
                    .setProjectId(p.projectId)
                    .setJobId(jobId);
            if(p.location != null) {
                jobReference.setLocation(p.location);
            }
            final Job request = new Job()
                    .setJobReference(jobReference)
                    .setConfiguration(configuration);
            try {
                final Bigquery.Jobs.Insert insert = bigquery.jobs().insert(p.projectId, request);
                if(p.quotaUser != null) {
                    insert.setQuotaUser(p.quotaUser);
                }
                final Job job = insert.execute();
                LOG.info("action module[{}] submitted bigquery {} job: {}", name, p.op, jobId);
                return job;
            } catch (final GoogleJsonResponseException e) {
                if(e.getStatusCode() != 409) {
                    final NonRetryableException rejected = rejectedRequest(e);
                    if(rejected != null) {
                        throw rejected;
                    }
                    throw e;
                }
            }
            // Retried bundle or retried firing: the job was already submitted by a previous attempt.
            final Job existing = bigquery.jobs().get(p.projectId, jobId).setLocation(p.location).execute();
            final String state = Optional.ofNullable(existing.getStatus()).map(s -> s.getState()).orElse(null);
            if(!"DONE".equals(state) || BigQueryUtil.isJobResultSucceeded(existing)) {
                LOG.info("action module[{}] bigquery job: {} already exists (state: {}), adopting the existing job", name, jobId, state);
                return existing;
            }
            final ErrorProto error = existing.getStatus().getErrorResult();
            if(!isRetryableReason(error)) {
                throw new NonRetryableException(
                        "bigquery job: " + jobId + " already exists and failed permanently with error: " + error);
            }
            LOG.warn("action module[{}] bigquery job: {} already exists and failed transiently with error: {}. resubmitting", name, jobId, error);
        }
        throw new IllegalStateException(
                "bigquery job: " + baseJobId + " failed transiently more than " + MAX_RESUBMITS + " times");
    }

    /**
     * Polls until DONE. {@code resubmittable}: a transient job error is reported as retryable (the
     * retry resubmits under the next {@code -r<n>} id); for a job this action did not submit
     * ({@code jobs.wait}) every failure is final.
     */
    private Job waitForCompletion(final Parameters p, final Job job, final boolean resubmittable) {
        final String jobId = job.getJobReference().getJobId();
        final Job completed;
        if("DONE".equals(Optional.ofNullable(job.getStatus()).map(st -> st.getState()).orElse(null))) {
            // already terminal (e.g. adopted or fetched job): no polling
            completed = job;
        } else {
            final ExponentialBackOff backOff = new ExponentialBackOff.Builder()
                    .setInitialIntervalMillis(2000)
                    .setMaxIntervalMillis(30000)
                    .setMaxElapsedTimeMillis(Math.toIntExact(Math.min(p.timeoutSeconds * 1000L, Integer.MAX_VALUE)))
                    .build();
            completed = BigQueryUtil.pollJob(bigquery, job.getJobReference(), sleeper, backOff);
        }
        if(completed == null) {
            if(p.cancelOnTimeout) {
                try {
                    bigquery.jobs().cancel(p.projectId, jobId).setLocation(p.location).execute();
                    LOG.warn("action module[{}] cancelled bigquery job: {} after timeoutSeconds: {}", name, jobId, p.timeoutSeconds);
                } catch (final IOException e) {
                    LOG.warn("action module[{}] failed to cancel bigquery job: {}: {}", name, jobId, e.getMessage());
                }
            }
            throw new NonRetryableException(
                    "bigquery job: " + jobId + " did not complete within timeoutSeconds: " + p.timeoutSeconds
                            + (p.cancelOnTimeout ? " (cancel requested)" : ""));
        }
        if(!BigQueryUtil.isJobResultSucceeded(completed)) {
            final ErrorProto error = completed.getStatus().getErrorResult();
            final String message = "bigquery job: " + jobId + " failed with error: " + error;
            if(resubmittable && isRetryableReason(error)) {
                // a retry resubmits the job under the next -r<n> id
                throw new IllegalStateException(message);
            }
            throw new NonRetryableException(message);
        }
        return completed;
    }

    static boolean isRetryableReason(final ErrorProto error) {
        if(error == null || error.getReason() == null) {
            return false;
        }
        return RETRYABLE_REASONS.contains(error.getReason());
    }

    /** HTTP errors of the insert request: 4xx (other than 408/429) are rejected requests, not transient; null otherwise. */
    static NonRetryableException rejectedRequest(final GoogleJsonResponseException e) {
        final int status = e.getStatusCode();
        if(status >= 400 && status < 500 && status != 408 && status != 429) {
            return new NonRetryableException("bigquery job submission was rejected: " + e.getMessage(), e);
        }
        return null;
    }

    /**
     * The {@code Job} resource as nested maps/lists with the API's declared types
     * (int64 fields as numbers, not the JSON wire strings), so conditions compare numerically.
     * {@code statistics.query.queryPlan} / {@code timeline} are dropped to keep the envelope small.
     */
    static Map<String, Object> toPayload(final Job job) {
        final Map<String, Object> payload = toMap(job);
        // per-stage plan and timeline can run to hundreds of KB per job and are never condition inputs
        final Object statistics = payload.get("statistics");
        if(statistics instanceof Map<?, ?> st && st.get("query") instanceof Map<?, ?> query) {
            query.remove("queryPlan");
            query.remove("timeline");
        }
        return payload;
    }

    private static Map<String, Object> toMap(final GenericData data) {
        final Map<String, Object> map = new LinkedHashMap<>();
        for(final Map.Entry<String, Object> entry : data.entrySet()) {
            final Object value = toValue(entry.getValue());
            if(value != null) {
                map.put(entry.getKey(), value);
            }
        }
        return map;
    }

    private static Object toValue(final Object value) {
        if(value == null || Data.isNull(value)) {
            return null;
        }
        return switch (value) {
            case GenericData g -> toMap(g);
            case Map<?, ?> m -> {
                final Map<String, Object> map = new LinkedHashMap<>();
                m.forEach((k, v) -> {
                    final Object converted = toValue(v);
                    if(converted != null) {
                        map.put(k.toString(), converted);
                    }
                });
                yield map;
            }
            case List<?> l -> l.stream().map(BigQueryAction::toValue).toList();
            case Number n -> n;
            case Boolean b -> b;
            case String s -> s;
            default -> value.toString();
        };
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
