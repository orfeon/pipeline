package com.mercari.solution.module.transform;

import com.mercari.solution.module.*;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.cloud.google.ParameterManagerUtil;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.domain.file.ResourceUtil;
import com.mercari.solution.util.pipeline.Filter;
import com.mercari.solution.util.pipeline.Query2;
import com.mercari.solution.util.pipeline.Union;
import com.mercari.solution.util.pipeline.lookup.LookupSource;
import com.mercari.solution.util.pipeline.lookup.source.BufferLookupSource;
import com.mercari.solution.util.pipeline.lookup.source.LookupSourceConfig;
import com.mercari.solution.util.pipeline.lookup.source.SideInputLookupSource;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.state.OrderedListState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Runs a Calcite SQL statement over each input element, inside a DoFn, optionally
 * joined against external lookup sources (JDBC / Spanner / Bigtable / REST / gRPC)
 * or other pipeline collections delivered as Beam side inputs ({@code sideinput}).
 *
 * <p>Unlike the {@code beamsql} module (which plans SQL over whole PCollections and may
 * shuffle), this module evaluates the SQL per element as a bounded in-memory query:
 * the element is registered as a one-row table, and any array-of-struct fields can be
 * expanded with {@code UNNEST}/{@code LATERAL} so that aggregation, ORDER BY / LIMIT and
 * set operations run over the per-element collection. The evaluation never shuffles and
 * preserves the input windowing/timestamps, so batch and streaming behave identically.
 * One input element yields zero or more output rows (fan-out or fold, decided by the SQL).
 *
 * <p>With {@code sources}, external tables are referenced as {@code sourceName.tableName}
 * and joined on their key (primary key / unique index / row key / request parameter):
 * a join whose condition constrains a contiguous prefix of the key — point equality on
 * the full key, or leading-column equality plus a bounded range on the next column —
 * becomes a batched key-driven read (never a scan of the external table). INNER and
 * LEFT joins are supported; any other use of a lookup table fails with an explanatory
 * error.
 */
@Transform.Module(name="query")
public class QueryTransform extends Transform {

    private static class Parameters implements Serializable {

        private String sql;
        private List<QueryParameters> queries;
        private Boolean exclusive;
        private String table;
        private com.google.gson.JsonElement filter;
        private List<LookupSourceConfig.SourceParameters> sources;

        private String filterJson() {
            return filter == null || filter.isJsonNull() ? null : filter.toString();
        }

        private boolean isExclusive() {
            return Boolean.TRUE.equals(exclusive);
        }

        /** The session statements: the single {@code sql} or the {@code queries} list. */
        private List<Query2.Statement> statements() {
            if(queries == null) {
                return List.of(Query2.Statement.of("", sql, true));
            }
            final List<Query2.Statement> statements = new ArrayList<>();
            for(final QueryParameters query : queries) {
                statements.add(Query2.Statement.of(
                        query.name, query.sql, !Boolean.FALSE.equals(query.output)));
            }
            return statements;
        }

        private void validate() {
            if(this.sql == null && (this.queries == null || this.queries.isEmpty())) {
                throw new IllegalModuleException("parameters requires sql or queries");
            }
            if(this.sql != null && this.queries != null) {
                throw new IllegalModuleException("parameters.sql and parameters.queries are exclusive; specify one");
            }
            if(this.queries != null) {
                final Set<String> names = new HashSet<>();
                boolean hasOutput = false;
                for(int i = 0; i < queries.size(); i++) {
                    final QueryParameters query = queries.get(i);
                    if(query.name == null || query.name.isEmpty()) {
                        throw new IllegalModuleException("parameters.queries[" + i + "].name must not be null");
                    }
                    if(query.sql == null) {
                        throw new IllegalModuleException("parameters.queries[" + i + "].sql must not be null");
                    }
                    if(!names.add(query.name.toUpperCase())) {
                        throw new IllegalModuleException(
                                "parameters.queries[" + i + "].name is duplicated: " + query.name);
                    }
                    if(!Boolean.FALSE.equals(query.output)) {
                        hasOutput = true;
                    }
                }
                if(!hasOutput) {
                    throw new IllegalModuleException(
                            "parameters.queries requires at least one query with output: true");
                }
            }
            LookupSourceConfig.validate(this.sources);
        }

        private void setDefaults(Map<String, String> templateArgs) {
            sql = loadQuery(sql, templateArgs);
            if(queries != null) {
                for(final QueryParameters query : queries) {
                    query.sql = loadQuery(query.sql, templateArgs);
                }
            }
            if(table == null) {
                table = "INPUT";
            }
            if(sources == null) {
                sources = new ArrayList<>();
            }
        }

        private static String loadQuery(String sql, Map<String, String> templateArgs) {
            if(sql == null) {
                return null;
            }
            String query;
            if(ResourceUtil.isStorageUri(sql)) {
                LOG.info("sql parameter is storage path: {}", sql);
                final String rawQuery = ResourceUtil.readString(sql);
                query = TemplateUtil.executeStrictTemplate(rawQuery, templateArgs);
            } else if(ParameterManagerUtil.isParameterVersionResource(sql)) {
                LOG.info("sql parameter is Parameter Manager resource: {}", sql);
                final ParameterManagerUtil.Version version = ParameterManagerUtil.getParameterVersion(sql);
                if(version.payload == null) {
                    throw new IllegalArgumentException("sql resource does not exists for: " + sql);
                }
                query = new String(version.payload, StandardCharsets.UTF_8);
            } else if(sql.startsWith("data:")) {
                LOG.info("sql parameter is base64 encoded");
                query = new String(Base64.getDecoder().decode(sql.substring("data:".length())), StandardCharsets.UTF_8);
            } else {
                // Query text is not a valid file path on some platforms (e.g. newlines or ':' on Windows)
                Path path;
                try {
                    path = Paths.get(sql);
                } catch (final Throwable e) {
                    path = null;
                }
                if(path != null && Files.exists(path) && !Files.isDirectory(path)) {
                    try {
                        final String rawQuery = Files.readString(path, StandardCharsets.UTF_8);
                        query = TemplateUtil.executeStrictTemplate(rawQuery, templateArgs);
                    } catch (IOException e) {
                        query = sql;
                    }
                } else {
                    query = sql;
                }
            }
            return query;
        }
    }

    /** One session statement of the {@code queries} form. */
    private static class QueryParameters implements Serializable {

        private String name;
        private String sql;
        private Boolean output;
    }

    @Override
    public MCollectionTuple expand(
            final MCollectionTuple inputs,
            final MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate();
        parameters.setDefaults(getTemplateArgs());

        final Schema inputSchema = Union.createUnionSchema(inputs);
        // A buffer source turns this transform into a stateful DoFn keyed by
        // its groupFields (state is per key), so the pipeline shape changes:
        // resolve its config first.
        final LookupSourceConfig.BufferConfig bufferConfig =
                LookupSourceConfig.bufferConfig(parameters.sources);

        // sideinput sources join against other MCollections delivered as Beam
        // side inputs: resolve the referenced collections here (their schemas
        // feed the planner; their views feed the DoFn).
        final Map<String, Schema> sideInputSchemas = new LinkedHashMap<>();
        final Map<String, PCollectionView<Iterable<MElement>>> sideInputViews = new LinkedHashMap<>();
        for(final String sideInputName : LookupSourceConfig.sideInputNames(parameters.sources)) {
            final MCollection collection = getSideInputs().stream()
                    .filter(c -> sideInputName.equals(c.getName()))
                    .findAny()
                    .orElseThrow(() -> new IllegalModuleException(
                            "query transform module[" + getName() + "] sources references side input '"
                                    + sideInputName + "' which is not listed in sideInputs: "
                                    + getSideInputs().stream().map(MCollection::getName).toList()));
            sideInputSchemas.put(sideInputName, collection.getSchema());
            sideInputViews.put(sideInputName,
                    collection.apply("AsSideInput_" + sideInputName, View.asIterable()));
        }

        // The schema of the buffered rows: with insertSql the buffer holds the
        // insert query's result rows — derive its schema first, planned WITHOUT
        // the buffer source (so an insert query referencing the buffer itself
        // fails naturally at construction). The restore query is planned the
        // same way, for the same reason.
        final String bufferInsertSql = bufferConfig == null ? null
                : Parameters.loadQuery(bufferConfig.insertSql(), getTemplateArgs());
        final String bufferRestoreSql = bufferConfig == null ? null
                : Parameters.loadQuery(bufferConfig.restoreSql(), getTemplateArgs());
        Schema bufferRowSchema = null;
        if(bufferConfig != null) {
            if(bufferInsertSql != null) {
                try {
                    bufferRowSchema = Query2.builder()
                            .withInput(parameters.table, inputSchema)
                            .withSources(LookupSourceConfig.createSources(
                                    LookupSourceConfig.nonBufferSources(parameters.sources),
                                    sideInputSchemas, null))
                            .withSql(bufferInsertSql)
                            .build()
                            .getOutputSchema();
                } catch (final Throwable e) {
                    throw new IllegalModuleException(
                            "query transform module[" + getName()
                                    + "] failed to plan buffer insertSql, cause: " + e.getMessage());
                }
            } else {
                bufferRowSchema = inputSchema;
            }
            if(bufferRestoreSql != null) {
                try {
                    Query2.builder()
                            .withInput(parameters.table, inputSchema)
                            .withSources(LookupSourceConfig.createSources(
                                    LookupSourceConfig.nonBufferSources(parameters.sources),
                                    sideInputSchemas, null))
                            .withSql(bufferRestoreSql)
                            .build();
                } catch (final Throwable e) {
                    throw new IllegalModuleException(
                            "query transform module[" + getName()
                                    + "] failed to plan buffer restoreSql, cause: " + e.getMessage());
                }
            }
        }

        final Query2 query;
        try {
            final Query2.Builder builder = Query2.builder()
                    .withInput(parameters.table, inputSchema)
                    .withSources(LookupSourceConfig.createSources(parameters.sources, sideInputSchemas,
                            bufferRowSchema))
                    .withQueries(parameters.statements())
                    .withExclusive(parameters.isExclusive());
            if(bufferInsertSql != null) {
                builder.withBufferInsert(bufferInsertSql);
            }
            if(bufferRestoreSql != null) {
                builder.withStateRestore(bufferRestoreSql);
            }
            query = builder.build();
        } catch (final Throwable e) {
            throw new IllegalModuleException(
                    "query transform module[" + getName() + "] failed to plan sql, cause: " + e.getMessage());
        }

        final Map<String, Schema> outputSchemas = query.getOutputSchemas();
        for(final Schema schema : outputSchemas.values()) {
            validateOutputSchema(schema);
        }

        // Buffer preparation: the persisted columns, the state row schema, and
        // the restore-schema/dedup validations (needed here because the
        // insertOutput output rides the state row schema).
        List<String> storedFields = null;
        Schema stateSchema = null;
        if(bufferConfig != null) {
            final BufferLookupSource bufferSource = query.getSources().stream()
                    .filter(s -> s instanceof BufferLookupSource)
                    .map(s -> (BufferLookupSource) s)
                    .findAny()
                    .orElseThrow(() -> new IllegalModuleException(
                            "query transform module[" + getName() + "] buffer source was not created"));
            final List<Schema.Field> stateFields = new ArrayList<>();
            if(bufferInsertSql != null) {
                final Schema insertSchema = query.getBufferInsertSchema();
                storedFields = insertSchema.getFields().stream().map(Schema.Field::getName).toList();
                stateFields.addAll(insertSchema.getFields());
            } else {
                storedFields = resolveStoredFields(bufferConfig, bufferSource, inputSchema);
                for(final Schema.Field field : inputSchema.getFields()) {
                    if(storedFields.contains(field.getName())) {
                        stateFields.add(field);
                    }
                }
            }
            stateFields.add(Schema.Field.of(BufferLookupSource.TIMESTAMP_FIELD, Schema.FieldType.TIMESTAMP));
            stateFields.add(Schema.Field.of(BufferLookupSource.INPUT_FIELD, Schema.FieldType.STRING.withNullable(true)));
            stateSchema = Schema.of(stateFields);
            validateBufferRecovery(bufferConfig, query, storedFields, outputSchemas.keySet());
        }

        // One tag per output statement (the legacy single-sql form uses the
        // default unnamed tag) plus the optional buffer insertOutput; the
        // first output statement is the main output.
        final Map<String, Schema> emittedSchemas = new LinkedHashMap<>(outputSchemas);
        if(bufferConfig != null && bufferConfig.insertOutput() != null) {
            emittedSchemas.put(bufferConfig.insertOutput(), stateSchema);
        }
        final Map<String, TupleTag<MElement>> outputTags = new LinkedHashMap<>();
        for(final String name : emittedSchemas.keySet()) {
            outputTags.put(name, new TupleTag<>(name.isEmpty() ? "output" : name));
        }
        final TupleTag<BadRecord> failureTag = new TupleTag<>() {};
        final List<TupleTag<?>> additionalTags = new ArrayList<>();
        final TupleTag<MElement> mainTag = outputTags.values().iterator().next();
        for(final TupleTag<MElement> tag : outputTags.values()) {
            if(tag != mainTag) {
                additionalTags.add(tag);
            }
        }
        additionalTags.add(failureTag);

        final PCollectionTuple outputs;
        if(bufferConfig == null) {
            final PCollection<MElement> input = inputs
                    .apply("Union", Union.flatten()
                            .withWaits(getWaits())
                            .withStrategy(getStrategy()));
            outputs = input
                    .apply("Query", ParDo
                            .of(new QueryDoFn(query, parameters.table, parameters.filterJson(), inputSchema,
                                    outputTags, sideInputViews, getLoggings(), getFailFast(), failureTag))
                            .withSideInputs(sideInputViews.values())
                            .withOutputTags(mainTag, TupleTagList.of(additionalTags)));
        } else {
            outputs = expandWithBuffer(inputs, inputSchema, bufferConfig, query, parameters,
                    storedFields, stateSchema, sideInputViews, outputTags, mainTag,
                    additionalTags, failureTag);
        }

        errorHandler.addError(outputs.get(failureTag));

        MCollectionTuple tuple = null;
        for(final Map.Entry<String, Schema> entry : emittedSchemas.entrySet()) {
            final PCollection<MElement> collection = outputs.get(outputTags.get(entry.getKey()));
            tuple = tuple == null
                    ? MCollectionTuple.of(entry.getKey(), collection, entry.getValue())
                    : tuple.and(entry.getKey(), collection, entry.getValue());
        }
        return tuple;
    }

    /**
     * Construction-time validation of the recovery surface: the restore
     * statement must yield exactly the buffered row shape (stored fields +
     * {@code __timestamp}, optionally {@code __input}), the dedup fields must
     * be buffered columns, and the insertOutput name must not collide.
     */
    private void validateBufferRecovery(
            final LookupSourceConfig.BufferConfig bufferConfig,
            final Query2 query,
            final List<String> storedFields,
            final Set<String> outputNames) {

        final List<String> allowed = new ArrayList<>(storedFields);
        allowed.add(BufferLookupSource.TIMESTAMP_FIELD);
        allowed.add(BufferLookupSource.INPUT_FIELD);
        if(bufferConfig.dedupFields() != null) {
            for(final String dedupField : bufferConfig.dedupFields()) {
                if(!allowed.contains(dedupField)) {
                    throw new IllegalModuleException(
                            "query transform module[" + getName() + "] buffer dedupFields '"
                                    + dedupField + "' is not a buffered column: " + allowed);
                }
            }
        }
        if(bufferConfig.restoreSql() != null) {
            final Schema restoreSchema = query.getStateRestoreSchema();
            final List<String> restoreFields = restoreSchema.getFields().stream()
                    .map(Schema.Field::getName).toList();
            if(!restoreFields.contains(BufferLookupSource.TIMESTAMP_FIELD)) {
                throw new IllegalModuleException(
                        "query transform module[" + getName() + "] buffer restoreSql must select"
                                + " the restored rows' original event time as '"
                                + BufferLookupSource.TIMESTAMP_FIELD + "' (TIMESTAMP)");
            }
            for(final String storedField : storedFields) {
                if(!restoreFields.contains(storedField)) {
                    throw new IllegalModuleException(
                            "query transform module[" + getName() + "] buffer restoreSql must"
                                    + " select every buffered column; missing: " + storedField
                                    + " (buffered columns: " + storedFields + ")");
                }
            }
            for(final String restoreField : restoreFields) {
                if(!allowed.contains(restoreField)) {
                    throw new IllegalModuleException(
                            "query transform module[" + getName() + "] buffer restoreSql selects '"
                                    + restoreField + "' which is not a buffered column"
                                    + " (buffered columns: " + storedFields + " plus "
                                    + BufferLookupSource.TIMESTAMP_FIELD + "/"
                                    + BufferLookupSource.INPUT_FIELD + ")");
                }
            }
        }
        if(bufferConfig.insertOutput() != null && outputNames.contains(bufferConfig.insertOutput())) {
            throw new IllegalModuleException(
                    "query transform module[" + getName() + "] buffer insertOutput '"
                            + bufferConfig.insertOutput() + "' collides with a query output name");
        }
    }

    /**
     * The buffer branch: key the union'd input by the buffer's groupFields and
     * run the query in a stateful DoFn that accumulates each group's history in
     * {@code OrderedListState}. The bounded variant requires time-sorted input
     * so "past elements" is well-defined in batch; the streaming variant
     * processes in arrival order (the buffer contents are still time-ordered)
     * and adds an event-time TTL timer for state GC.
     */
    private PCollectionTuple expandWithBuffer(
            final MCollectionTuple inputs,
            final Schema inputSchema,
            final LookupSourceConfig.BufferConfig bufferConfig,
            final Query2 query,
            final Parameters parameters,
            final List<String> storedFields,
            final Schema stateSchema,
            final Map<String, PCollectionView<Iterable<MElement>>> sideInputViews,
            final Map<String, TupleTag<MElement>> outputTags,
            final TupleTag<MElement> mainTag,
            final List<TupleTag<?>> additionalTags,
            final TupleTag<BadRecord> failureTag) {

        final PCollection<KV<String, MElement>> keyed = inputs
                .apply("UnionWithKey", Union.withKeys(bufferConfig.groupFields())
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));
        if(!keyed.getWindowingStrategy().getWindowFn().isNonMerging()) {
            throw new IllegalModuleException(
                    "query transform module[" + getName() + "] buffer sources require a"
                            + " non-merging windowing strategy (stateful processing does not"
                            + " support session windows)");
        }

        final Coder<MElement> stateCoder = ElementCoder.of(stateSchema);

        final BufferQueryProcessor processor = new BufferQueryProcessor(
                query, parameters.table, parameters.filterJson(), bufferConfig, storedFields,
                new ArrayList<>(inputs.getAll().keySet()), inputSchema,
                outputTags, sideInputViews, getLoggings(), getFailFast(), failureTag);

        final boolean bounded = PCollection.IsBounded.BOUNDED.equals(keyed.isBounded());
        final DoFn<KV<String, MElement>, MElement> doFn = bounded
                ? new BatchBufferQueryDoFn(processor, stateCoder)
                : new StreamingBufferQueryDoFn(processor, stateCoder);
        return keyed
                .apply("Query", ParDo.of(doFn)
                        .withSideInputs(sideInputViews.values())
                        .withOutputTags(mainTag, TupleTagList.of(additionalTags)));
    }

    /**
     * The input fields persisted in state per buffered element: the explicit
     * {@code fields} list when given (validated to cover every buffer column
     * the SQL references), otherwise the referenced columns collected from the
     * plan — in both cases forcing the groupFields and any input-column
     * dedupFields in, in input schema order.
     */
    private List<String> resolveStoredFields(
            final LookupSourceConfig.BufferConfig bufferConfig,
            final BufferLookupSource bufferSource,
            final Schema inputSchema) {

        final List<String> inputFieldNames = inputSchema.getFields().stream()
                .map(Schema.Field::getName)
                .toList();
        final Set<String> referencedInput = new LinkedHashSet<>();
        for(final String column : bufferSource.referencedColumns()) {
            if(inputFieldNames.contains(column)) {
                referencedInput.add(column);
            }
        }
        // dedup identity columns must be persisted even when the SQL never
        // reads them (the dedup happens outside the SQL)
        final Set<String> forced = new LinkedHashSet<>(bufferConfig.groupFields());
        if(bufferConfig.dedupFields() != null) {
            for(final String dedupField : bufferConfig.dedupFields()) {
                if(inputFieldNames.contains(dedupField)) {
                    forced.add(dedupField);
                }
            }
        }
        final Set<String> stored = new LinkedHashSet<>();
        if(bufferConfig.fields() != null) {
            for(final String field : bufferConfig.fields()) {
                if(!inputFieldNames.contains(field)) {
                    throw new IllegalModuleException(
                            "query transform module[" + getName() + "] buffer fields '" + field
                                    + "' is not a field of the input schema: " + inputFieldNames);
                }
            }
            stored.addAll(bufferConfig.fields());
            stored.addAll(forced);
            final List<String> missing = referencedInput.stream()
                    .filter(column -> !stored.contains(column))
                    .toList();
            if(!missing.isEmpty()) {
                throw new IllegalModuleException(
                        "query transform module[" + getName() + "] sql references buffer columns"
                                + " that are not listed in the buffer table's fields: " + missing);
            }
        } else {
            stored.addAll(referencedInput);
            stored.addAll(forced);
        }
        return inputFieldNames.stream().filter(stored::contains).toList();
    }

    // Auto-generated column names such as EXPR$0 are not legal field names for
    // downstream schema conversions (e.g. Avro). Require an explicit alias instead.
    private void validateOutputSchema(final Schema outputSchema) {
        for(final Schema.Field field : outputSchema.getFields()) {
            if(field.getName().contains("$")) {
                throw new IllegalModuleException(
                        "query transform module[" + getName() + "] output column '" + field.getName()
                                + "' has an auto-generated name. Add an explicit alias (e.g. `AS my_column`) in the select list");
            }
        }
    }

    /** Emits every output statement's rows to its tag (after the whole session succeeded). */
    private static void emitOutputs(
            final DoFn<?, MElement>.ProcessContext c,
            final Map<String, TupleTag<MElement>> outputTags,
            final Map<String, Logging> logs,
            final Query2.SessionResult result) {
        for(final Map.Entry<String, List<MElement>> entry : result.outputs().entrySet()) {
            final TupleTag<MElement> tag = outputTags.get(entry.getKey());
            for(final MElement output : entry.getValue()) {
                Logging.log(LOG, logs, "output", output);
                c.output(tag, output);
            }
        }
    }

    private static class QueryDoFn extends DoFn<MElement, MElement> {

        private final Query2 query;
        private final String table;
        private final String filterJson;
        private final Schema inputSchema;
        private final Map<String, TupleTag<MElement>> outputTags;
        private final Map<String, PCollectionView<Iterable<MElement>>> sideInputViews;
        private final Map<String, Logging> logs;
        private final boolean failFast;
        private final TupleTag<BadRecord> failuresTag;

        private transient Filter.ConditionNode filterCondition;

        QueryDoFn(
                final Query2 query,
                final String table,
                final String filterJson,
                final Schema inputSchema,
                final Map<String, TupleTag<MElement>> outputTags,
                final Map<String, PCollectionView<Iterable<MElement>>> sideInputViews,
                final List<Logging> logs,
                final boolean failFast,
                final TupleTag<BadRecord> failuresTag) {

            this.query = query;
            this.table = table;
            this.filterJson = filterJson;
            this.inputSchema = inputSchema;
            this.outputTags = outputTags;
            this.sideInputViews = sideInputViews;
            this.logs = Logging.map(logs);
            this.failFast = failFast;
            this.failuresTag = failuresTag;
        }

        @Setup
        public void setup() {
            query.setup();
            this.filterCondition = filterJson == null ? null : Filter.parse(filterJson);
        }

        @Teardown
        public void teardown() {
            query.teardown();
        }

        @ProcessElement
        public void processElement(ProcessContext c, BoundedWindow window) {
            final MElement input = c.element();
            if(input == null) {
                return;
            }
            // The cheap pre-filter: elements not matching are dropped before
            // the (far more expensive) SQL evaluation.
            if(filterCondition != null && !Filter.filter(filterCondition, inputSchema, input)) {
                return;
            }
            try {
                // Feed the current window's side input contents to the sideinput
                // sources; the window token makes indexing once-per-window.
                if(!sideInputViews.isEmpty()) {
                    for(final LookupSource source : query.getSources()) {
                        if(source instanceof SideInputLookupSource sideInputSource) {
                            for(final String inputName : sideInputSource.inputNames()) {
                                sideInputSource.setData(inputName,
                                        c.sideInput(sideInputViews.get(inputName)), window);
                            }
                        }
                    }
                }
                Logging.log(LOG, logs, "input", input);
                final Query2.SessionResult result =
                        query.executeAll(Map.of(table, List.of(input)), c.timestamp());
                emitOutputs(c, outputTags, logs, result);
            } catch (final Throwable e) {
                final BadRecord badRecord = processError("Failed to execute query", input, e, failFast);
                c.output(failuresTag, badRecord);
            }
        }

    }

    /**
     * The shared per-element logic of the buffer branch's stateful DoFns
     * (batch and streaming declare their own state/timer specs and delegate
     * here). Per element:
     * <ol>
     *   <li>evaluate {@code bufferFilter} (persist?) and {@code triggerFilter}
     *       (evaluate the SQL?);</li>
     *   <li>persist the element (its stored fields + synthetic columns) into
     *       {@code OrderedListState} and evict — retention is approximate at
     *       write (clearRange works on timestamp boundaries), exact at read;</li>
     *   <li>on trigger, feed the visible buffer (duration/count-trimmed, plus
     *       the current element when {@code includeCurrent}) to the buffer
     *       source and run the query.</li>
     * </ol>
     * The write happens before the evaluation so a failing SQL evaluation
     * still buffers the element. Elements whose group key has a null component
     * are evaluated but not persisted (SQL equality never matches null, so
     * their rows could never be looked up).
     */
    static class BufferQueryProcessor implements Serializable {

        private final Query2 query;
        private final String table;
        private final String filterJson;
        private final List<String> groupFields;
        private final List<String> storedFields;
        private final List<String> inputNames;
        private final Schema inputSchema;
        private final boolean includeCurrent;
        private final Integer maxCount;
        private final Long maxDurationMillis;
        private final Long ttlMillis;
        private final String bufferFilterJson;
        private final String triggerFilterJson;
        private final boolean insertMode;
        private final boolean restoreMode;
        private final List<String> dedupFields;
        private final String insertOutputName;
        private final Map<String, TupleTag<MElement>> outputTags;
        private final Map<String, PCollectionView<Iterable<MElement>>> sideInputViews;
        private final Map<String, Logging> logs;
        private final boolean failFast;
        private final TupleTag<BadRecord> failuresTag;

        private transient BufferLookupSource bufferSource;
        private transient Filter.ConditionNode filterCondition;
        private transient Filter.ConditionNode bufferCondition;
        private transient Filter.ConditionNode triggerCondition;

        BufferQueryProcessor(
                final Query2 query,
                final String table,
                final String filterJson,
                final LookupSourceConfig.BufferConfig bufferConfig,
                final List<String> storedFields,
                final List<String> inputNames,
                final Schema inputSchema,
                final Map<String, TupleTag<MElement>> outputTags,
                final Map<String, PCollectionView<Iterable<MElement>>> sideInputViews,
                final List<Logging> logs,
                final boolean failFast,
                final TupleTag<BadRecord> failuresTag) {

            this.query = query;
            this.table = table;
            this.filterJson = filterJson;
            this.outputTags = outputTags;
            this.groupFields = bufferConfig.groupFields();
            this.storedFields = storedFields;
            this.inputNames = inputNames;
            this.inputSchema = inputSchema;
            this.includeCurrent = bufferConfig.includeCurrent();
            this.maxCount = bufferConfig.maxCount();
            this.maxDurationMillis = bufferConfig.maxDurationSeconds() == null
                    ? null : bufferConfig.maxDurationSeconds() * 1000L;
            this.ttlMillis = bufferConfig.stateTtlSeconds() == null
                    ? null : bufferConfig.stateTtlSeconds() * 1000L;
            this.bufferFilterJson = bufferConfig.bufferFilterJson();
            this.triggerFilterJson = bufferConfig.triggerFilterJson();
            this.insertMode = bufferConfig.insertSql() != null;
            this.restoreMode = bufferConfig.restoreSql() != null;
            this.dedupFields = bufferConfig.dedupFields();
            this.insertOutputName = bufferConfig.insertOutput();
            this.sideInputViews = sideInputViews;
            this.logs = Logging.map(logs);
            this.failFast = failFast;
            this.failuresTag = failuresTag;
        }

        void setup() {
            query.setup();
            this.bufferSource = null;
            for(final LookupSource source : query.getSources()) {
                if(source instanceof BufferLookupSource buffer) {
                    this.bufferSource = buffer;
                }
            }
            if(this.bufferSource == null) {
                throw new IllegalStateException("buffer source is not registered in the query");
            }
            this.filterCondition = filterJson == null ? null : Filter.parse(filterJson);
            this.bufferCondition = bufferFilterJson == null ? null : Filter.parse(bufferFilterJson);
            this.triggerCondition = triggerFilterJson == null ? null : Filter.parse(triggerFilterJson);
        }

        void teardown() {
            query.teardown();
        }

        void process(
                final DoFn<KV<String, MElement>, MElement>.ProcessContext c,
                final BoundedWindow window,
                final OrderedListState<MElement> buffer,
                final ValueState<Long> count,
                final ValueState<Boolean> restoredState,
                final ValueState<Long> maxTsState,
                final Timer ttlTimer) {

            final MElement input = c.element() == null ? null : c.element().getValue();
            if(input == null) {
                return;
            }
            // The module-level pre-filter drops the element entirely: it is
            // neither buffered nor evaluated (unlike bufferFilter/triggerFilter,
            // which split those two concerns).
            if(filterCondition != null && !Filter.filter(filterCondition, inputSchema, input)) {
                return;
            }
            try {
                if(!sideInputViews.isEmpty()) {
                    for(final LookupSource source : query.getSources()) {
                        if(source instanceof SideInputLookupSource sideInputSource) {
                            for(final String inputName : sideInputSource.inputNames()) {
                                sideInputSource.setData(inputName,
                                        c.sideInput(sideInputViews.get(inputName)), window);
                            }
                        }
                    }
                }
                final Instant timestamp = c.timestamp();
                final boolean bufferable = bufferCondition == null
                        || Filter.filter(bufferCondition, inputSchema, input);
                final boolean trigger = triggerCondition == null
                        || Filter.filter(triggerCondition, inputSchema, input);
                boolean keyComplete = true;
                for(final String groupField : groupFields) {
                    if(input.getPrimitiveValue(groupField) == null) {
                        keyComplete = false;
                        break;
                    }
                }
                final boolean persist = bufferable && keyComplete;
                if(!trigger && !persist) {
                    return;
                }

                final String inputName = input.getIndex() < inputNames.size()
                        ? inputNames.get(input.getIndex()) : null;

                // State restore on the key's first touch: seed the buffer from
                // the restore query's rows at their original event times. The
                // pre-seed contents are read first and merged in memory (no
                // OrderedListState read-after-write); the restored flag guards
                // re-execution until a TTL clear.
                List<TimestampedValue<MElement>> buffered = null;
                if(restoreMode && keyComplete && !Boolean.TRUE.equals(restoredState.read())) {
                    buffered = readSorted(buffer);
                    buffered.addAll(restoreState(buffer, count, input, timestamp));
                    buffered.sort(java.util.Comparator.comparing(TimestampedValue::getTimestamp));
                    restoredState.write(true);
                }

                // The current element's buffer contribution: with insertSql the
                // insert query's result rows (0..N, evaluated first in the same
                // session), otherwise the element itself.
                List<MElement> currentRows = List.of();
                if(persist || (trigger && includeCurrent)) {
                    if(insertMode) {
                        final Query2.SessionResult insertResult = query.executeAll(
                                Map.of(table, List.of(input)), timestamp,
                                EnumSet.of(Query2.Statement.Kind.BUFFER_INSERT));
                        final List<MElement> insertRows = insertResult.bufferRows() == null
                                ? List.of() : insertResult.bufferRows();
                        final List<MElement> converted = new ArrayList<>(insertRows.size());
                        for(final MElement row : insertRows) {
                            validateGroupKey(row, input);
                            converted.add(BufferLookupSource
                                    .createBufferElement(row, timestamp, inputName, storedFields));
                        }
                        currentRows = converted;
                    } else {
                        currentRows = List.of(BufferLookupSource
                                .createBufferElement(input, timestamp, inputName, storedFields));
                    }
                }

                if(trigger && buffered == null) {
                    buffered = readSorted(buffer);
                }
                if(persist && !currentRows.isEmpty()) {
                    writeAndEvict(buffer, count, maxTsState, ttlTimer,
                            currentRows, timestamp, buffered, window);
                    // The live rows just persisted also feed the external copy
                    // (restored/seeded rows are never re-emitted, so no loop
                    // back into the external store).
                    if(insertOutputName != null) {
                        final TupleTag<MElement> insertTag = outputTags.get(insertOutputName);
                        for(final MElement row : currentRows) {
                            c.output(insertTag, row);
                        }
                    }
                }
                if(trigger) {
                    final List<MElement> visible = visibleRows(buffered, timestamp,
                            includeCurrent ? currentRows : List.of());
                    // The key carrier: lookups may only use the current element's
                    // own group values (built from the input's groupFields).
                    final MElement keyElement = BufferLookupSource
                            .createBufferElement(input, timestamp, inputName, groupFields);
                    bufferSource.setData(visible, keyElement);
                    Logging.log(LOG, logs, "input", input);
                    final Query2.SessionResult result = query.executeAll(
                            Map.of(table, List.of(input)), timestamp,
                            EnumSet.of(Query2.Statement.Kind.QUERY));
                    emitOutputs(c, outputTags, logs, result);
                }
            } catch (final Throwable e) {
                final BadRecord badRecord = processError("Failed to execute query", input, e, failFast);
                c.output(failuresTag, badRecord);
            }
        }

        /**
         * Seeds the buffer state from the restore query's rows at their
         * original event times, trimming to the retention window relative to
         * the current element (so a TTL-triggered re-restore can never
         * resurrect expired history, regardless of the restore SQL's own
         * filters). Returns the seeded entries for the in-memory merge.
         */
        private List<TimestampedValue<MElement>> restoreState(
                final OrderedListState<MElement> buffer,
                final ValueState<Long> count,
                final MElement input,
                final Instant timestamp) {

            final Query2.SessionResult restoreResult = query.executeAll(
                    Map.of(table, List.of(input)), timestamp,
                    EnumSet.of(Query2.Statement.Kind.STATE_RESTORE));
            final List<MElement> restoreRows = restoreResult.restoreRows() == null
                    ? List.of() : restoreResult.restoreRows();
            final Instant cutoff = maxDurationMillis == null
                    ? null : timestamp.minus(Duration.millis(maxDurationMillis));
            final List<TimestampedValue<MElement>> seeded = new ArrayList<>();
            for(final MElement row : restoreRows) {
                validateGroupKey(row, input);
                final Object micros = row.getPrimitiveValue(BufferLookupSource.TIMESTAMP_FIELD);
                if(micros == null) {
                    throw new IllegalStateException(
                            "buffer restoreSql row has a null " + BufferLookupSource.TIMESTAMP_FIELD
                                    + " (the restored rows' original event time is required)");
                }
                final Instant rowTimestamp = Instant.ofEpochMilli(((Number) micros).longValue() / 1000L);
                if(cutoff != null && rowTimestamp.isBefore(cutoff)) {
                    continue;
                }
                final Map<String, Object> values = new HashMap<>();
                for(final String storedField : storedFields) {
                    values.put(storedField, row.getPrimitiveValue(storedField));
                }
                values.put(BufferLookupSource.TIMESTAMP_FIELD, ((Number) micros).longValue());
                values.put(BufferLookupSource.INPUT_FIELD,
                        row.getPrimitiveValue(BufferLookupSource.INPUT_FIELD));
                final MElement bufferRow = MElement.of(values, rowTimestamp);
                buffer.add(TimestampedValue.of(bufferRow, rowTimestamp));
                seeded.add(TimestampedValue.of(bufferRow, rowTimestamp));
            }
            if(maxCount != null && !seeded.isEmpty()) {
                final Long read = count.read();
                count.write((read == null ? 0L : read) + seeded.size());
            }
            return seeded;
        }

        /**
         * The insert query must select the group fields unchanged: state is
         * partitioned by the input element's key, so a row whose key columns
         * differ could never be looked up (and would poison the key affinity).
         */
        private void validateGroupKey(final MElement row, final MElement input) {
            for(final String groupField : groupFields) {
                final Object rowValue = row.getPrimitiveValue(groupField);
                final Object inputValue = input.getPrimitiveValue(groupField);
                if(!Objects.equals(rowValue, inputValue)) {
                    throw new IllegalStateException(
                            "buffer insertSql row has groupField '" + groupField + "' = " + rowValue
                                    + " but the input element's key value is " + inputValue
                                    + "; the insert query must select the groupFields unchanged");
                }
            }
        }

        /**
         * Persists the element's buffer rows and applies retention. Count-based
         * eviction computes a timestamp boundary from the pre-add buffer contents
         * (entries at the boundary timestamp survive — retention may briefly
         * exceed maxCount under identical timestamps; read-time trimming is
         * exact), and never clears at or past the current element's timestamp.
         */
        private void writeAndEvict(
                final OrderedListState<MElement> buffer,
                final ValueState<Long> count,
                final ValueState<Long> maxTsState,
                final Timer ttlTimer,
                final List<MElement> bufferElements,
                final Instant timestamp,
                List<TimestampedValue<MElement>> buffered,
                final BoundedWindow window) {

            final int adding = bufferElements.size();
            Instant durationCutoff = maxDurationMillis == null
                    ? null : timestamp.minus(Duration.millis(maxDurationMillis));
            if(durationCutoff != null && !durationCutoff.isAfter(BoundedWindow.TIMESTAMP_MIN_VALUE)) {
                durationCutoff = null;
            }
            Instant boundary = durationCutoff;
            Long newCount = null;
            if(maxCount != null) {
                final Long read = count.read();
                final long knownCount = read == null ? 0L : read;
                if(knownCount + adding > maxCount) {
                    if(buffered == null) {
                        buffered = readSorted(buffer);
                    }
                    final List<TimestampedValue<MElement>> survivors = new ArrayList<>();
                    for(final TimestampedValue<MElement> entry : buffered) {
                        if(durationCutoff == null || !entry.getTimestamp().isBefore(durationCutoff)) {
                            survivors.add(entry);
                        }
                    }
                    final int keep = Math.max(0, maxCount - adding); // the new rows take their slots
                    if(survivors.size() > keep) {
                        Instant countBoundary = keep == 0
                                ? timestamp : survivors.get(survivors.size() - keep).getTimestamp();
                        if(countBoundary.isAfter(timestamp)) {
                            countBoundary = timestamp;
                        }
                        if(boundary == null || countBoundary.isAfter(boundary)) {
                            boundary = countBoundary;
                        }
                    }
                    long retained = 0;
                    for(final TimestampedValue<MElement> entry : survivors) {
                        if(boundary == null || !entry.getTimestamp().isBefore(boundary)) {
                            retained++;
                        }
                    }
                    newCount = retained + adding;
                } else {
                    newCount = knownCount + adding;
                }
            }
            if(boundary != null) {
                buffer.clearRange(BoundedWindow.TIMESTAMP_MIN_VALUE, boundary);
            }
            for(final MElement bufferElement : bufferElements) {
                buffer.add(TimestampedValue.of(bufferElement, timestamp));
            }
            if(newCount != null) {
                count.write(newCount);
            }
            if(ttlMillis != null && ttlTimer != null && maxTsState != null) {
                final Long previous = maxTsState.read();
                final long newMaxTs = previous == null
                        ? timestamp.getMillis() : Math.max(previous, timestamp.getMillis());
                maxTsState.write(newMaxTs);
                Instant fireAt = Instant.ofEpochMilli(newMaxTs).plus(Duration.millis(ttlMillis));
                if(fireAt.isAfter(window.maxTimestamp())) {
                    fireAt = window.maxTimestamp();
                }
                ttlTimer.withNoOutputTimestamp().set(fireAt);
            }
        }

        /**
         * Exact read-time visibility: duration cutoff, plus the current
         * element's buffer rows when {@code includeCurrent}, deduplicated by
         * {@code dedupFields} (first occurrence in time order wins — restored
         * rows, replayed input and at-least-once external writes may overlap),
         * trimmed to the newest {@code maxCount} — the query never sees more
         * than maxCount buffer rows per evaluation.
         */
        private List<MElement> visibleRows(
                final List<TimestampedValue<MElement>> buffered,
                final Instant timestamp,
                final List<MElement> currentRows) {
            List<MElement> visible = new ArrayList<>();
            final Instant cutoff = maxDurationMillis == null
                    ? null : timestamp.minus(Duration.millis(maxDurationMillis));
            if(buffered != null) {
                for(final TimestampedValue<MElement> entry : buffered) {
                    if(cutoff == null || !entry.getTimestamp().isBefore(cutoff)) {
                        visible.add(entry.getValue());
                    }
                }
            }
            visible.addAll(currentRows);
            if(dedupFields != null && !dedupFields.isEmpty()) {
                final Map<List<Object>, MElement> seen = new LinkedHashMap<>();
                for(final MElement row : visible) {
                    final List<Object> identity = new ArrayList<>(dedupFields.size());
                    for(final String dedupField : dedupFields) {
                        identity.add(row.getPrimitiveValue(dedupField));
                    }
                    seen.putIfAbsent(identity, row);
                }
                visible = new ArrayList<>(seen.values());
            }
            if(maxCount != null && visible.size() > maxCount) {
                return new ArrayList<>(visible.subList(visible.size() - maxCount, visible.size()));
            }
            return visible;
        }

        /**
         * Clears the group's state when the TTL elapsed with no newer element.
         * The restored flag is cleared too: the key's next element re-restores
         * from the external store within the retention window (cache
         * semantics) — with no restoreSql this is a no-op flag.
         */
        void onTtl(
                final Instant firingTimestamp,
                final OrderedListState<MElement> buffer,
                final ValueState<Long> count,
                final ValueState<Boolean> restoredState,
                final ValueState<Long> maxTsState) {
            if(ttlMillis == null) {
                return;
            }
            final Long maxTs = maxTsState.read();
            if(maxTs != null
                    && firingTimestamp.isBefore(Instant.ofEpochMilli(maxTs).plus(Duration.millis(ttlMillis)))) {
                return; // superseded by a newer element's timer
            }
            buffer.clear();
            count.clear();
            restoredState.clear();
            maxTsState.clear();
        }

        private static List<TimestampedValue<MElement>> readSorted(
                final OrderedListState<MElement> buffer) {
            final List<TimestampedValue<MElement>> entries = new ArrayList<>();
            final Iterable<TimestampedValue<MElement>> read = buffer.read();
            if(read != null) {
                for(final TimestampedValue<MElement> entry : read) {
                    entries.add(entry);
                }
            }
            return entries;
        }
    }

    /**
     * The bounded variant: {@code @RequiresTimeSortedInput} makes "the past" of
     * an element well-defined in batch (each key's elements arrive in event-time
     * order), matching streaming arrival order. No TTL timer — bounded state
     * ends with the pipeline.
     */
    static class BatchBufferQueryDoFn extends DoFn<KV<String, MElement>, MElement> {

        private final BufferQueryProcessor processor;

        @StateId("buffer")
        private final StateSpec<OrderedListState<MElement>> bufferSpec;
        @StateId("count")
        private final StateSpec<ValueState<Long>> countSpec = StateSpecs.value(VarLongCoder.of());
        @StateId("restored")
        private final StateSpec<ValueState<Boolean>> restoredSpec = StateSpecs.value(BooleanCoder.of());

        BatchBufferQueryDoFn(final BufferQueryProcessor processor, final Coder<MElement> coder) {
            this.processor = processor;
            this.bufferSpec = StateSpecs.orderedList(coder);
        }

        @Setup
        public void setup() {
            processor.setup();
        }

        @Teardown
        public void teardown() {
            processor.teardown();
        }

        @RequiresTimeSortedInput
        @ProcessElement
        public void processElement(
                final ProcessContext c,
                final BoundedWindow window,
                @StateId("buffer") final OrderedListState<MElement> buffer,
                @StateId("count") final ValueState<Long> count,
                @StateId("restored") final ValueState<Boolean> restored) {
            processor.process(c, window, buffer, count, restored, null, null);
        }
    }

    /**
     * The unbounded variant: elements are processed in arrival order (the
     * buffer contents remain event-time-ordered via {@code OrderedListState};
     * only membership at evaluation time is arrival-dependent), with an
     * event-time TTL timer clearing idle groups' state.
     */
    static class StreamingBufferQueryDoFn extends DoFn<KV<String, MElement>, MElement> {

        private final BufferQueryProcessor processor;

        @StateId("buffer")
        private final StateSpec<OrderedListState<MElement>> bufferSpec;
        @StateId("count")
        private final StateSpec<ValueState<Long>> countSpec = StateSpecs.value(VarLongCoder.of());
        @StateId("restored")
        private final StateSpec<ValueState<Boolean>> restoredSpec = StateSpecs.value(BooleanCoder.of());
        @StateId("maxTs")
        private final StateSpec<ValueState<Long>> maxTsSpec = StateSpecs.value(VarLongCoder.of());
        @TimerId("ttl")
        private final TimerSpec ttlSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

        StreamingBufferQueryDoFn(final BufferQueryProcessor processor, final Coder<MElement> coder) {
            this.processor = processor;
            this.bufferSpec = StateSpecs.orderedList(coder);
        }

        @Setup
        public void setup() {
            processor.setup();
        }

        @Teardown
        public void teardown() {
            processor.teardown();
        }

        @ProcessElement
        public void processElement(
                final ProcessContext c,
                final BoundedWindow window,
                @StateId("buffer") final OrderedListState<MElement> buffer,
                @StateId("count") final ValueState<Long> count,
                @StateId("restored") final ValueState<Boolean> restored,
                @StateId("maxTs") final ValueState<Long> maxTs,
                @TimerId("ttl") final Timer ttl) {
            processor.process(c, window, buffer, count, restored, maxTs, ttl);
        }

        @OnTimer("ttl")
        public void onTtl(
                final OnTimerContext c,
                @StateId("buffer") final OrderedListState<MElement> buffer,
                @StateId("count") final ValueState<Long> count,
                @StateId("restored") final ValueState<Boolean> restored,
                @StateId("maxTs") final ValueState<Long> maxTs) {
            processor.onTtl(c.timestamp(), buffer, count, restored, maxTs);
        }
    }

}
