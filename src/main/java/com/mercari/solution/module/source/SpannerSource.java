package com.mercari.solution.module.source;

import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.*;
import com.google.cloud.spanner.Partition;
import com.google.gson.JsonElement;
import com.mercari.solution.config.options.DataflowOptions;
import com.mercari.solution.module.*;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.coder.UnionMapCoder;
import com.mercari.solution.util.cloud.google.SpannerUtil;
import com.mercari.solution.util.domain.file.ResourceUtil;
import com.mercari.solution.util.pipeline.MicroBatch;
import com.mercari.solution.util.pipeline.OptionUtil;
import com.mercari.solution.util.schema.StructSchemaUtil;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.Transaction;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

@Source.Module(name="spanner")
public class SpannerSource extends Source {

    private static class Parameters implements Serializable {

        // common parameters
        private Source.Mode mode;
        private String projectId;
        private String instanceId;
        private String databaseId;

        private String timestampBound;
        private Boolean enableDataBoost;
        private String requestTag;
        private Boolean emulator;

        // for query parameters
        private String query;
        private Options.RpcPriority priority;

        // for table parameters
        private String table;
        private List<String> fields;
        private List<KeyRangeParameter> keyRange;

        // for all-tables batch parameters
        private JsonElement tables;

        // for change stream parameter
        private ChangeStreamParameter changeStream;

        // for microBatch parameter
        private MicroBatch.MicroBatchParameter microBatch;

        // for view parameter
        private ViewParameter view;

        private static class TablesParameter implements Serializable {

            private final List<String> includes;
            private final List<String> excludes;
            // common per-table query template (inline or gs:// path); null means the generated
            // default `SELECT * FROM <table>`
            private final String query;

            private TablesParameter(List<String> includes, List<String> excludes, String query) {
                this.includes = includes;
                this.excludes = excludes;
                this.query = query;
            }

            /**
             * Accepts either a pattern list shorthand {@code tables: ["Users", "Item*"]}
             * or the full form {@code tables: {includes: [...], excludes: [...]}}.
             * Missing includes defaults to all tables; {@code *} matches any sequence.
             */
            static TablesParameter of(final JsonElement json) {
                final List<String> includes;
                final List<String> excludes;
                final String query;
                if(json.isJsonArray()) {
                    includes = toStringList(json);
                    excludes = new ArrayList<>();
                    query = null;
                } else if(json.isJsonObject()) {
                    includes = json.getAsJsonObject().has("includes")
                            ? toStringList(json.getAsJsonObject().get("includes"))
                            : new ArrayList<>();
                    excludes = json.getAsJsonObject().has("excludes")
                            ? toStringList(json.getAsJsonObject().get("excludes"))
                            : new ArrayList<>();
                    if(json.getAsJsonObject().has("query")) {
                        final JsonElement queryElement = json.getAsJsonObject().get("query");
                        if(!queryElement.isJsonPrimitive() || !queryElement.getAsJsonPrimitive().isString()) {
                            throw new IllegalArgumentException("'tables.query' must be a string: " + json);
                        }
                        query = queryElement.getAsString();
                    } else {
                        query = null;
                    }
                } else {
                    throw new IllegalArgumentException(
                            "'tables' must be a pattern array or an object with 'includes', 'excludes' and 'query': " + json);
                }
                if(includes.isEmpty()) {
                    includes.add("*");
                }
                return new TablesParameter(includes, excludes, query);
            }

            private static List<String> toStringList(final JsonElement json) {
                if(!json.isJsonArray()) {
                    throw new IllegalArgumentException("'tables' patterns must be a string array: " + json);
                }
                final List<String> list = new ArrayList<>();
                for(final JsonElement element : json.getAsJsonArray()) {
                    if(!element.isJsonPrimitive() || !element.getAsJsonPrimitive().isString()) {
                        throw new IllegalArgumentException("'tables' patterns must be a string array: " + json);
                    }
                    list.add(element.getAsString());
                }
                return list;
            }

            boolean matches(final String table) {
                return includes.stream().anyMatch(p -> matchesGlob(p, table))
                        && excludes.stream().noneMatch(p -> matchesGlob(p, table));
            }

            private static boolean matchesGlob(final String pattern, final String value) {
                final String[] literals = pattern.split("\\*", -1);
                final StringBuilder regex = new StringBuilder();
                for(int i = 0; i < literals.length; i++) {
                    if(i > 0) {
                        regex.append(".*");
                    }
                    regex.append(java.util.regex.Pattern.quote(literals[i]));
                }
                return value.matches(regex.toString());
            }

        }

        private static class KeyRangeParameter {

            private String startType;
            private String endType;
            private JsonElement startKeys;
            private JsonElement endKeys;

            public List<String> validate() {
                final List<String> errorMessages = new ArrayList<>();
                return errorMessages;
            }

            public void setDefaults() {

            }

        }

        private static class ChangeStreamParameter implements Serializable {

            private String changeStreamName;
            private String metadataInstance;
            private String metadataDatabase;
            private String metadataTable;
            private String inclusiveStartAt;
            private String inclusiveEndAt;

            public List<String> validate(
                    String name,
                    Parameters parentParameters) {

                final List<String> errorMessages = new ArrayList<>();
                return errorMessages;
            }

            public void setDefaults(Parameters parentParameters) {

                if(this.metadataInstance == null) {
                    this.metadataInstance = parentParameters.instanceId;
                }
                if(this.metadataDatabase == null) {
                    this.metadataDatabase = parentParameters.databaseId;
                }
                if(this.inclusiveStartAt == null) {
                    this.inclusiveStartAt = Timestamp.now().toString();
                }
                if(this.inclusiveEndAt == null) {
                    this.inclusiveEndAt = Timestamp.MAX_VALUE.toString();
                }

            }

        }

        private static class ViewParameter implements Serializable {

            private String keyField;
            private Integer intervalMinute;

            public List<String> validate(String name) {
                final List<String> errorMessages = new ArrayList<>();
                if(keyField == null) {
                    errorMessages.add("spanner source module[" + name + "].view requires 'keyField' parameter");
                }
                return errorMessages;
            }

            private void setDefaults() {
                if(intervalMinute == null) {
                    intervalMinute = 60;
                }
            }

        }

        private void validate(String name) {
            final List<String> errorMessages = new ArrayList<>();
            if(projectId == null) {
                errorMessages.add("spanner source module[" + name + "] requires 'projectId' parameter");
            }
            if(instanceId == null) {
                errorMessages.add("spanner source module[" + name + "] requires 'instanceId' parameter");
            }
            if(databaseId == null) {
                errorMessages.add("spanner source module[" + name + "] requires 'databaseId' parameter");
            }

            switch (mode) {
                case microBatch -> {
                    if(microBatch == null) {
                        errorMessages.add("spanner source module[" + name + "] requires 'microBatch' parameter if mode is 'microBatch'");
                    } else {
                        errorMessages.addAll(microBatch.validate(name));
                    }
                }
                case changeDataCapture -> {
                    if(changeStream == null) {
                        errorMessages.add("spanner source module[" + name + "] requires 'changeStream' parameter if mode is 'changeStream'");
                    } else {
                        errorMessages.addAll(changeStream.validate(name, this));
                    }
                }
                case view -> {
                    if(view == null) {
                        errorMessages.add("spanner source module[" + name + "] requires 'view' parameter if mode is 'view'");
                    } else {
                        errorMessages.addAll(view.validate(name));
                    }
                }
                case null, default -> {
                    if(query == null && table == null && tables == null) {
                        errorMessages.add("spanner source module[" + name + "] requires 'query', 'table' or 'tables' parameter if mode is 'batch' or default");
                    }
                    if(tables != null) {
                        if(query != null || table != null) {
                            errorMessages.add("spanner source module[" + name + "] must not set 'tables' together with 'query' or 'table'");
                        }
                        if(fields != null || keyRange != null) {
                            errorMessages.add("spanner source module[" + name + "] does not support 'fields' or 'keyRange' with 'tables'");
                        }
                        try {
                            TablesParameter.of(tables);
                        } catch (final IllegalArgumentException e) {
                            errorMessages.add("spanner source module[" + name + "] " + e.getMessage());
                        }
                    }
                }
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        private void setDefaults() {
            if (priority == null) {
                this.priority = Options.RpcPriority.MEDIUM;
            }
            if (emulator == null) {
                this.emulator = false;
            }
            if(this.enableDataBoost == null) {
                this.enableDataBoost = false;
            }

            if(changeStream != null) {
                changeStream.setDefaults(this);
            }
            if(microBatch != null) {
                microBatch.setDefaults();
            }
            if(view != null) {
                view.setDefaults();
            }
        }
    }

    private enum Mode {
        batch,
        microBatch,
        changeStream,
        view
    }

    @Override
    public MCollectionTuple expand(
            final PBegin begin,
            final MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate(getName());
        parameters.setDefaults();

        return switch (getMode()) {
            case batch -> {
                if(parameters.tables != null) {
                    yield expandAllTables(begin, parameters);
                }
                final Type type;
                final PCollection<Struct> structs;
                if(parameters.query != null) {
                    final QuerySource source = new QuerySource(parameters, getTemplateArgs());
                    structs = begin.apply("Query", source);
                    type = source.type;
                } else {
                    final TableSource source = new TableSource(parameters);
                    structs = begin.apply("Table", source);
                    type = source.type;
                }

                final Schema outputSchema = Schema.of(type);
                final PCollection<MElement> output = structs
                        .apply("Format", ParDo.of(new WithTimestampDoFn(
                                getTimestampAttribute(), DateTimeUtil.toJodaInstant(getTimestampDefault()))));
                yield MCollectionTuple
                        .of(output, outputSchema);
            }
            case microBatch -> {
                yield null;
            }
            case changeDataCapture -> {
                final ChangeStreamSource source = new ChangeStreamSource(parameters);
                final PCollection<MMutation> mutation = begin.apply("ChangeStream", source);

                yield null;
                //yield MCollectionTuple
                //        .of(mutation, outputSchema);
            }
            case view -> {
                final TupleTag<MElement> outputTag = new TupleTag<>(){};
                final TupleTag<MElement> failureTag = new TupleTag<>(){};
                final ViewSource source = new ViewSource(getJobName(), getName(), parameters, outputTag, failureTag);
                final PCollectionTuple outputs = begin
                        .apply("Query", source);
                final PCollection<MElement> output = outputs.get(outputTag);
                // TODO
                final Type type = source.type;
                yield MCollectionTuple
                        .of(output, Schema.builder().build());
            }
            default -> throw new IllegalArgumentException();
        };
    }

    // matches ${table} (with optional spaces or FreeMarker builtins such as ${table?lower_case})
    private static final java.util.regex.Pattern TABLES_QUERY_TABLE_VARIABLE =
            java.util.regex.Pattern.compile("\\$\\{\\s*table\\b");

    private MCollectionTuple expandAllTables(final PBegin begin, final Parameters parameters) {

        final Parameters.TablesParameter tablesParameter = Parameters.TablesParameter.of(parameters.tables);
        // JsonElement is not java-serializable and the query DoFns below capture Parameters,
        // so drop the already-parsed raw JSON before building the graph
        parameters.tables = null;
        final String queryTemplate = loadTablesQueryTemplate(getName(), tablesParameter);

        final Map<String, Type> tableTypes = SpannerUtil.getBaseTableTypesFromDatabase(
                parameters.projectId, parameters.instanceId, parameters.databaseId, parameters.emulator);

        final Map<String, Type> matched = new LinkedHashMap<>();
        for(final Map.Entry<String, Type> entry : tableTypes.entrySet()) {
            if(tablesParameter.matches(entry.getKey())) {
                matched.put(entry.getKey(), entry.getValue());
            }
        }
        if(matched.isEmpty()) {
            throw new IllegalModuleException(
                    "spanner source module[" + getName() + "].tables matched no table. database tables: " + tableTypes.keySet());
        }
        LOG.info("spanner source module[{}] reads {} tables: {}", getName(), matched.size(), matched.keySet());

        // one shared batch transaction: every per-table partitioned query attaches to it,
        // so all tables are read at the same snapshot
        final PCollectionView<Transaction> transactionView = QuerySource.createTransactionView(begin, parameters);

        MCollectionTuple tuple = MCollectionTuple.empty(begin.getPipeline());
        for(final Map.Entry<String, Type> entry : matched.entrySet()) {
            final String table = entry.getKey();
            final String query = renderTableQuery(queryTemplate, getTemplateArgs(), table);
            final Type type = queryTemplate == null
                    ? entry.getValue()
                    : SpannerUtil.getTypeFromQuery(
                            parameters.projectId, parameters.instanceId, parameters.databaseId, query, parameters.emulator);

            final PCollection<MElement> output = QuerySource
                    .applyPartitionedQuery(begin, "." + table, parameters, query, transactionView)
                    .apply("Format." + table, ParDo.of(new WithTimestampDoFn(
                            getTimestampAttribute(), DateTimeUtil.toJodaInstant(getTimestampDefault()))));

            tuple = tuple.and(table, output, Schema.of(type), Map.of(
                    "table", table,
                    "projectId", parameters.projectId,
                    "instanceId", parameters.instanceId,
                    "databaseId", parameters.databaseId));
        }
        return tuple;
    }

    /**
     * Returns the resolved tables.query template, or null when not set (the generated default
     * {@code SELECT * FROM <table>} is used and the output type comes from INFORMATION_SCHEMA
     * without per-table analyzeQuery round trips).
     */
    private static String loadTablesQueryTemplate(final String name, final Parameters.TablesParameter tablesParameter) {
        if(tablesParameter.query == null) {
            return null;
        }
        final String template = ResourceUtil.isStorageUri(tablesParameter.query)
                ? ResourceUtil.readString(tablesParameter.query)
                : tablesParameter.query;
        if(!TABLES_QUERY_TABLE_VARIABLE.matcher(template).find()) {
            throw new IllegalModuleException(
                    "spanner source module[" + name + "].tables.query must reference ${table}: " + template);
        }
        if(template.contains(QuerySource.SQL_SPLITTER)) {
            throw new IllegalModuleException(
                    "spanner source module[" + name + "].tables.query does not support " + QuerySource.SQL_SPLITTER);
        }
        return template;
    }

    private static String renderTableQuery(
            final String queryTemplate,
            final Map<String, String> templateArgs,
            final String table) {

        if(queryTemplate == null) {
            return "SELECT * FROM " + quoteTableIdentifier(table);
        }
        final Map<String, Object> model = new HashMap<>();
        if(templateArgs != null) {
            model.putAll(templateArgs);
        }
        model.put("table", table);
        return TemplateUtil.executeStrictTemplate(queryTemplate, model);
    }

    // backquotes each path segment so reserved-word or named-schema tables stay valid GoogleSQL
    private static String quoteTableIdentifier(final String table) {
        return Arrays.stream(table.split("\\."))
                .map(part -> "`" + part + "`")
                .collect(Collectors.joining("."));
    }

    private static List<String> tableColumns(final Type type) {
        return type.getStructFields().stream()
                .map(Type.StructField::getName)
                .collect(Collectors.toList());
    }

    // Shared by the single-table (table) and all-tables (tables) batch paths.
    private static SpannerIO.Read createTableRead(
            final Parameters parameters,
            final String table,
            final List<String> columns,
            final KeySet keySet,
            final TimestampBound timestampBound) {

        SpannerConfig config = SpannerConfig.create()
                .withProjectId(parameters.projectId)
                .withInstanceId(parameters.instanceId)
                .withDatabaseId(parameters.databaseId)
                .withDataBoostEnabled(ValueProvider.StaticValueProvider.of(parameters.enableDataBoost))
                .withRpcPriority(parameters.priority);
        if(parameters.emulator) {
            config = config.withEmulatorHost(ValueProvider.StaticValueProvider.of(SpannerUtil.getEmulatorHost()));
        }

        return SpannerIO.read()
                .withSpannerConfig(config)
                .withTable(table)
                .withKeySet(keySet)
                .withColumns(columns)
                .withBatching(true)
                .withTimestampBound(timestampBound);
    }

    private static class QuerySource extends PTransform<PBegin, PCollection<Struct>> {

        private static final String SQL_SPLITTER = "--SPLITTER--";

        private final Parameters parameters;
        private final Map<String, String> templateArgs;

        private Type type;

        QuerySource(final Parameters parameters, final Map<String, String> templateArgs) {
            this.parameters = parameters;
            this.templateArgs = templateArgs;
        }

        @Override
        public PCollection<Struct> expand(PBegin begin) {

            final String rawQuery;
            if(ResourceUtil.isStorageUri(parameters.query)) {
                rawQuery = ResourceUtil.readString(parameters.query);
            } else {
                rawQuery = parameters.query;
            }

            final String query = TemplateUtil.executeStrictTemplate(rawQuery, templateArgs);
            this.type = SpannerUtil.getTypeFromQuery(parameters.projectId, parameters.instanceId, parameters.databaseId, query, parameters.emulator);

            final PCollectionView<Transaction> transactionView = createTransactionView(begin, parameters);
            return applyPartitionedQuery(begin, "", parameters, query, transactionView);
        }

        static PCollectionView<Transaction> createTransactionView(final PBegin begin, final Parameters parameters) {
            return begin
                    .apply(Create.of(1L))
                    .apply("CreateTransaction", ParDo.of(new CreateTransactionFn(parameters)))
                    .apply("AsView", View.asSingleton());
        }

        /**
         * Builds the partitioned-query read branch (partitionQuery via a shared
         * BatchReadOnlyTransaction, shuffled per partition, batch endpoint). Shared by the
         * single-query batch path and the all-tables (tables) path, which appends one branch
         * per table with {@code nameSuffix} and the same transaction view.
         */
        static PCollection<Struct> applyPartitionedQuery(
                final PBegin begin,
                final String nameSuffix,
                final Parameters parameters,
                final String query,
                final PCollectionView<Transaction> transactionView) {

            final TupleTag<KV<String, KV<BatchTransactionId, Partition>>> tagPartition = new TupleTag<>(){};
            final TupleTag<Struct> tagStruct = new TupleTag<>(){};

            final PCollectionTuple results = begin
                    .apply("SupplyQuery" + nameSuffix, Create.of(query))
                    .apply("SplitQuery" + nameSuffix, FlatMapElements.into(TypeDescriptors.strings()).via(s -> Arrays.asList(s.split(SQL_SPLITTER))))
                    .apply("ExecuteQuery" + nameSuffix, ParDo.of(new QueryPartitionDoFn(
                                    parameters, transactionView, tagStruct))
                            .withSideInput("transactionView", transactionView)
                            .withOutputTags(tagPartition, TupleTagList.of(tagStruct)));

            final PCollection<Struct> struct1 = results.get(tagPartition)
                    .apply("GroupByPartition" + nameSuffix, GroupByKey.create())
                    .apply("ReadStruct" + nameSuffix, ParDo.of(new ReadStructDoFn(parameters, transactionView))
                            .withSideInput("transactionView", transactionView))
                    .setCoder(SerializableCoder.of(Struct.class));
            final PCollection<Struct> struct2 = results.get(tagStruct);
            return PCollectionList.of(struct1).and(struct2)
                    .apply("FlattenStructs" + nameSuffix, Flatten.pCollections());
        }

        public static class CreateTransactionFn extends DoFn<Object, Transaction> {

            private static final Logger LOG = LoggerFactory.getLogger(CreateTransactionFn.class);

            private final Parameters parameters;
            private final TimestampBound timestampBound;

            public CreateTransactionFn(final Parameters parameters) {
                this.parameters = parameters;
                this.timestampBound = toTimestampBound(parameters.timestampBound);
                LOG.info(String.format("TimestampBound: %s", timestampBound.toString()));
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                try(final Spanner spanner = SpannerUtil
                        .connectSpanner(parameters.projectId, 1, 1, 1, true, parameters.emulator)) {

                    final BatchReadOnlyTransaction tx = spanner
                            .getBatchClient(DatabaseId.of(
                                    parameters.projectId, parameters.instanceId, parameters.databaseId))
                            .batchReadOnlyTransaction(timestampBound);
                    c.output(Transaction.create(tx.getBatchTransactionId()));
                } catch (final Throwable e) {
                    ERROR_COUNTER.inc();
                    LOG.error("Failed to create transaction cause: {}", e.getMessage());
                    throw new RuntimeException("Failed to create transaction", e);
                }
            }
        }

        public static class QueryPartitionDoFn extends DoFn<String, KV<String, KV<BatchTransactionId, Partition>>> {

            private static final Logger LOG = LoggerFactory.getLogger(QueryPartitionDoFn.class);

            private final Parameters parameters;
            private final PCollectionView<Transaction> transactionView;
            private final TupleTag<Struct> tagStruct;

            private QueryPartitionDoFn(
                    final Parameters parameters,
                    final PCollectionView<Transaction> transactionView,
                    final TupleTag<Struct> tagStruct) {

                this.parameters = parameters;
                this.transactionView = transactionView;
                this.tagStruct = tagStruct;
            }

            @Setup
            public void setup() {
                LOG.info("QueryPartitionDoFn.setup");
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final String query = c.element();
                LOG.info(String.format("Received query [%s], timestamp bound [%s]", query, parameters.timestampBound));
                final Statement statement = Statement.of(query);
                final Transaction tx = c.sideInput(transactionView);

                try(final Spanner spanner = SpannerUtil.connectSpanner(
                        parameters.projectId, 1, 1, 1, true, parameters.emulator)) {
                    final BatchReadOnlyTransaction transaction = spanner
                            .getBatchClient(DatabaseId.of(
                                    parameters.projectId, parameters.instanceId, parameters.databaseId))
                            .batchReadOnlyTransaction(tx.transactionId());

                    final PartitionOptions options = PartitionOptions.newBuilder()
                            //.setMaxPartitions(10000) // Note: this hint is currently ignored in v1.
                            //.setPartitionSizeBytes(100000000) // Note: this hint is currently ignored in v1.
                            .build();

                    final Options.ReadQueryUpdateTransactionOption tagOption = createSpannerRequestTag(c.getPipelineOptions(), parameters.requestTag);
                    try {
                        final List<com.google.cloud.spanner.Partition> partitions = transaction
                                .partitionQuery(options, statement, tagOption,
                                        Options.priority(parameters.priority),
                                        Options.dataBoostEnabled(parameters.enableDataBoost));
                        LOG.info(String.format("Query [%s] divided to [%d] partitions.", query, partitions.size()));
                        for (int i = 0; i < partitions.size(); ++i) {
                            final KV<BatchTransactionId, Partition> value = KV.of(transaction.getBatchTransactionId(), partitions.get(i));
                            final String key = String.format("%d-%s", i, query);
                            final KV<String, KV<BatchTransactionId, Partition>> kv = KV.of(key, value);
                            c.output(kv);
                        }
                    } catch (SpannerException e) {
                        if (!e.getErrorCode().equals(ErrorCode.INVALID_ARGUMENT)) {
                            throw e;
                        }
                        LOG.warn(String.format("Query [%s] could not be executed. Retrying as single query.", query));
                        try (final ResultSet resultSet = transaction.executeQuery(statement, tagOption,
                                Options.priority(parameters.priority),
                                Options.dataBoostEnabled(parameters.enableDataBoost))) {
                            int count = 0;
                            while (resultSet.next()) {
                                c.output(tagStruct, resultSet.getCurrentRowAsStruct());
                                count++;
                            }
                            LOG.info(String.format("Query read record num [%d]", count));
                        }
                    }
                }

            }

            private Options.ReadQueryUpdateTransactionOption createSpannerRequestTag(
                    final PipelineOptions options,
                    final String vtag) {

                final String project = DataflowOptions.getProject(options);
                final String jobName = options.getJobName();
                final String serviceAccount = DataflowOptions.getServiceAccount(options);

                final String tag = String.format("job=%s,sa=%s,project=%s", jobName, serviceAccount, project);
                if(vtag != null) {
                    return Options.tag(vtag + "," + tag);
                }
                return Options.tag(tag);
            }

        }

        public static class ReadStructDoFn extends DoFn<KV<String, Iterable<KV<BatchTransactionId, Partition>>>, Struct> {

            private static final Logger LOG = LoggerFactory.getLogger(ReadStructDoFn.class);

            private final Parameters parameters;
            private final PCollectionView<Transaction> transactionView;
            private transient Spanner spanner;
            private transient BatchClient batchClient;

            private ReadStructDoFn(
                    final Parameters parameters,
                    final PCollectionView<Transaction> transactionView) {

                this.parameters = parameters;
                this.transactionView = transactionView;
            }

            @Setup
            public void setup() {
                LOG.info("ReadStructDoFn.setup");
                this.spanner = SpannerUtil.connectSpanner(parameters.projectId, 1, 1, 1, true, parameters.emulator);
                this.batchClient = spanner.getBatchClient(DatabaseId.of(
                        parameters.projectId, parameters.instanceId, parameters.databaseId));
            }

            @ProcessElement
            public void processElement(final ProcessContext c) {
                final KV<String, Iterable<KV<BatchTransactionId, com.google.cloud.spanner.Partition>>> kv = c.element();
                final String partitionNumberQuery = kv.getKey();
                final KV<BatchTransactionId, com.google.cloud.spanner.Partition> value = kv.getValue().iterator().next();

                final Transaction tx = c.sideInput(transactionView);
                final BatchReadOnlyTransaction transaction = this.batchClient.batchReadOnlyTransaction(tx.transactionId()); // DO NOT CLOSE!!!
                final Partition partition = value.getValue();

                try(final ResultSet resultSet = transaction.execute(partition)) {
                    LOG.info(String.format("Started %s th partition[%s] query.", partitionNumberQuery.split("-")[0], partition));
                    int count = 0;
                    while (resultSet.next()) {
                        c.output(resultSet.getCurrentRowAsStruct());
                        count++;
                    }
                    LOG.info(String.format("%s th partition completed to read record: [%d]", partitionNumberQuery.split("-")[0], count));
                }
            }

            @Teardown
            public void teardown() {
                if(this.spanner != null) {
                    this.spanner.close();
                }
                LOG.info("ReadStructDoFn.teardown");
            }

        }

    }

    private static class TableSource extends PTransform<PBegin, PCollection<Struct>> {

        private final Parameters parameters;
        private Type type;

        TableSource(final Parameters parameters) {
            this.parameters = parameters;
        }

        @Override
        public PCollection<Struct> expand(PBegin begin) {

            this.type = SpannerUtil.getTypeFromTable(
                    parameters.projectId, parameters.instanceId, parameters.databaseId,
                    parameters.table, parameters.fields, parameters.emulator);

            // TODO check columns exists in table
            final KeySet keySet = createKeySet(parameters, type);
            final SpannerIO.Read read = createTableRead(
                    parameters, parameters.table, tableColumns(type), keySet,
                    toTimestampBound(parameters.timestampBound));

            return begin.apply("ReadSpannerTable", read);
        }

        private static KeySet createKeySet(
                final Parameters parameters,
                final Type type) {

            if(parameters.keyRange == null) {
                return KeySet.all();
            } else {
                final List<String> keyFieldNames = SpannerUtil.getPrimaryKeyFieldNames(
                        parameters.projectId, parameters.instanceId, parameters.databaseId, parameters.table, parameters.emulator);
                final List<Type.StructField> keyFields = keyFieldNames.stream()
                        .map(f -> type.getStructFields().stream()
                                .filter(s -> s.getName().equals(f))
                                .findAny()
                                .orElseThrow(() -> new IllegalArgumentException("PrimaryKey: " + f + " not found!")))
                        .collect(Collectors.toList());

                final KeySet.Builder builder = KeySet.newBuilder();
                for(final Parameters.KeyRangeParameter keyRangeParameter : parameters.keyRange) {
                    final KeyRange.Endpoint startType;
                    if(keyRangeParameter.startType == null) {
                        startType = KeyRange.Endpoint.CLOSED;
                    } else {
                        startType = "open".equalsIgnoreCase(keyRangeParameter.startType) ?
                                KeyRange.Endpoint.OPEN : KeyRange.Endpoint.CLOSED;
                    }

                    final KeyRange.Endpoint endType;
                    if(keyRangeParameter.endType == null) {
                        endType = KeyRange.Endpoint.CLOSED;
                    } else {
                        endType = "open".equalsIgnoreCase(keyRangeParameter.endType) ?
                                KeyRange.Endpoint.OPEN : KeyRange.Endpoint.CLOSED;
                    }
                    final Key start = createRangeKey(keyFields, keyRangeParameter.startKeys);
                    final Key end   = createRangeKey(keyFields, keyRangeParameter.endKeys);

                    builder.addRange(KeyRange.newBuilder()
                            .setStartType(startType)
                            .setEndType(endType)
                            .setStart(start)
                            .setEnd(end)
                            .build());
                }
                return builder.build();
            }
        }

        private static Key createRangeKey(final List<Type.StructField> keyFields, final JsonElement keyValues) {
            final Key.Builder key = Key.newBuilder();
            if(keyValues == null) {
                return key.build();
            }
            if(keyValues.isJsonPrimitive()) {
                final Type.StructField field = keyFields.get(0);
                setRangeKey(key, field, keyValues);
            } else {
                for(int i=0; i< keyValues.getAsJsonArray().size(); i++) {
                    final Type.StructField field = keyFields.get(i);
                    setRangeKey(key, field, keyValues.getAsJsonArray().get(i));
                }
            }
            return key.build();
        }

        private static void setRangeKey(final Key.Builder key, final Type.StructField field, final JsonElement element) {
            switch (field.getType().getCode()) {
                case STRING -> key.append(element.getAsString());
                case INT64 -> key.append(element.getAsLong());
                case FLOAT64 -> key.append(element.getAsDouble());
                case BOOL -> key.append(element.getAsBoolean());
                case DATE -> key.append(Date.parseDate(element.getAsString()));
                case TIMESTAMP -> key.append(Timestamp.parseTimestamp(element.getAsString()));
                default -> {
                }
            }
        }
    }

    private static class ChangeStreamSource extends PTransform<PBegin, PCollection<MMutation>> {

        private final Parameters parameters;

        ChangeStreamSource(final Parameters parameters) {
            this.parameters = parameters;
        }

        @Override
        public PCollection<MMutation> expand(PBegin begin) {
            final SpannerIO.ReadChangeStream readChangeStream = createDataChangeRecordSource(parameters);
            final PCollection<DataChangeRecord> dataChangeRecords = begin
                    .apply("ReadChangeStream", readChangeStream);

            DataChangeRecord a;
            return null;
        }

        private static SpannerIO.ReadChangeStream createDataChangeRecordSource(
                final Parameters parameters) {

            final SpannerConfig spannerConfig = SpannerConfig.create()
                    .withHost(ValueProvider.StaticValueProvider.of(SpannerUtil.SPANNER_HOST_BATCH))
                    .withProjectId(parameters.projectId)
                    .withInstanceId(parameters.instanceId)
                    .withDatabaseId(parameters.databaseId)
                    .withDataBoostEnabled(ValueProvider.StaticValueProvider.of(parameters.enableDataBoost));

            SpannerIO.ReadChangeStream readChangeStream = SpannerIO.readChangeStream()
                    .withSpannerConfig(spannerConfig)
                    .withChangeStreamName(parameters.changeStream.changeStreamName)
                    .withMetadataInstance(parameters.changeStream.metadataInstance)
                    .withMetadataDatabase(parameters.changeStream.metadataDatabase)
                    .withRpcPriority(parameters.priority);

            if(parameters.changeStream.inclusiveStartAt != null) {
                final Timestamp inclusiveStartAt = Timestamp.parseTimestamp(parameters.changeStream.inclusiveStartAt);
                readChangeStream = readChangeStream.withInclusiveStartAt(inclusiveStartAt);
            }
            if(parameters.changeStream.inclusiveEndAt != null) {
                final Timestamp inclusiveEndAt = Timestamp.parseTimestamp(parameters.changeStream.inclusiveEndAt);
                readChangeStream = readChangeStream.withInclusiveEndAt(inclusiveEndAt);
            }
            if(parameters.changeStream.metadataTable != null) {
                readChangeStream = readChangeStream.withMetadataTable(parameters.changeStream.metadataTable);
            }

            return readChangeStream;
        }

    }

    private static class ViewSource extends PTransform<PBegin, PCollectionTuple> {

        private final String jobName;
        private final String moduleName;
        private final Parameters parameters;
        private final TupleTag<MElement> outputTag;
        private final TupleTag<MElement> failuresTag;

        private Type type;

        ViewSource(
                final String jobName,
                final String moduleName,
                final Parameters parameters,
                final TupleTag<MElement> outputTag,
                final TupleTag<MElement> failuresTag) {

            this.jobName = jobName;
            this.moduleName = moduleName;
            this.parameters = parameters;
            this.outputTag = outputTag;
            this.failuresTag = failuresTag;
        }

        @Override
        public PCollectionTuple expand(PBegin begin) {

            type = SpannerUtil.getTypeFromQuery(
                    parameters.projectId, parameters.instanceId, parameters.databaseId, parameters.query, parameters.emulator);

            final PCollection<Long> sequence;
            if(OptionUtil.isStreaming(begin)) {
                sequence = begin
                        .apply("Generate", GenerateSequence
                                .from(0)
                                .withRate(1, Duration.standardMinutes(parameters.view.intervalMinute)));
            } else {
                sequence = begin
                        .apply("Create", Create.of(1L).withCoder(VarLongCoder.of()));
            }

            return sequence
                    .apply(ParDo.of(new QueryMapDoFn(jobName, moduleName, parameters, failuresTag))
                            .withOutputTags(outputTag, TupleTagList.of(failuresTag)));
        }

        private static class QueryMapDoFn extends DoFn<Long, MElement> {

            private final String jobName;
            private final String name;
            private final Parameters parameters;
            private final TupleTag<MElement> failuresTag;

            QueryMapDoFn(
                    final String jobName,
                    final String name,
                    final Parameters parameters,
                    final TupleTag<MElement> failuresTag) {

                this.jobName = jobName;
                this.name = name;
                this.parameters = parameters;
                this.failuresTag = failuresTag;
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final List<Struct> structs = new ArrayList<>();
                final Statement statement = Statement.of(parameters.query);
                try(final Spanner spanner = SpannerUtil.connectSpanner(
                        parameters.projectId, 1, 1, 1, true, parameters.emulator);
                    final BatchReadOnlyTransaction tx = spanner
                            .getBatchClient(DatabaseId.of(
                                    parameters.projectId, parameters.instanceId, parameters.databaseId))
                            .batchReadOnlyTransaction(TimestampBound.strong());
                    final ResultSet resultSet = tx.executeQuery(statement,
                            Options.priority(parameters.priority),
                            Options.dataBoostEnabled(parameters.enableDataBoost))) {

                    while (resultSet.next()) {
                        final Struct struct = resultSet.getCurrentRowAsStruct();
                        structs.add(struct);
                    }
                } catch (final Throwable e) {
                    final MElement failure = MFailure
                            .of(jobName, name, parameters.query, e, c.timestamp())
                            .toElement(c.timestamp());
                    c.output(failuresTag, failure);
                }

                final Map<String ,Object> map = new HashMap<>();
                for(final Struct struct : structs) {
                    String key = struct.getString(parameters.view.keyField);
                    Map<String, Object> values = StructSchemaUtil.asPrimitiveMap(struct);
                    map.put(key, values);
                }
                System.out.println("size: " + UnionMapCoder.serializeSize(map));
                final MElement output = MElement.of(map, c.timestamp());
                c.output(output);
            }

        }
    }

    private static class WithTimestampDoFn extends DoFn<Struct, MElement> {

        private final String timestampAttribute;
        private final Instant timestampDefault;

        private WithTimestampDoFn(
                final String timestampAttribute,
                final Instant timestampDefault) {

            this.timestampAttribute = timestampAttribute;
            this.timestampDefault = timestampDefault == null ? Instant.ofEpochSecond(0L) : timestampDefault;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final Struct struct = c.element();
            if(struct == null) {
                return;
            }
            if(timestampAttribute == null) {
                final MElement element = MElement.of(struct, c.timestamp());
                c.output(element);
            } else {
                final Instant timestamp = StructSchemaUtil.getTimestamp(struct, timestampAttribute, timestampDefault);
                final MElement element = MElement.of(struct, timestamp);
                c.outputWithTimestamp(element, timestamp);
            }
        }

    }

    private static TimestampBound toTimestampBound(final String timestampBoundString) {
        if(timestampBoundString == null) {
            return TimestampBound.strong();
        } else {
            try {
                final Instant instant = Instant.parse(timestampBoundString);
                final com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.ofTimeMicroseconds(instant.getMillis() * 1000);
                return TimestampBound.ofReadTimestamp(timestamp);
            } catch (Exception e) {
                return TimestampBound.strong();
            }
        }
    }
}
