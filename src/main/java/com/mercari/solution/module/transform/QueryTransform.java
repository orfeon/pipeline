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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
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
        private String table;
        private com.google.gson.JsonElement filter;
        private List<LookupSourceConfig.SourceParameters> sources;

        private String filterJson() {
            return filter == null || filter.isJsonNull() ? null : filter.toString();
        }

        private void validate() {
            if(this.sql == null) {
                throw new IllegalModuleException("parameters.sql must not be null");
            }
            LookupSourceConfig.validate(this.sources);
        }

        private void setDefaults(Map<String, String> templateArgs) {
            sql = loadQuery(sql, templateArgs);
            if(table == null) {
                table = "INPUT";
            }
            if(sources == null) {
                sources = new ArrayList<>();
            }
        }

        private String loadQuery(String sql, Map<String, String> templateArgs) {
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

        final Query2 query;
        try {
            query = Query2.builder()
                    .withInput(parameters.table, inputSchema)
                    .withSources(LookupSourceConfig.createSources(parameters.sources, sideInputSchemas,
                            bufferConfig == null ? null : inputSchema))
                    .withSql(parameters.sql)
                    .build();
        } catch (final Throwable e) {
            throw new IllegalModuleException(
                    "query transform module[" + getName() + "] failed to plan sql: " + parameters.sql + ", cause: " + e.getMessage());
        }

        final Schema outputSchema = query.getOutputSchema();
        validateOutputSchema(outputSchema);

        final TupleTag<MElement> outputTag = new TupleTag<>() {};
        final TupleTag<BadRecord> failureTag = new TupleTag<>() {};

        final PCollectionTuple outputs;
        if(bufferConfig == null) {
            final PCollection<MElement> input = inputs
                    .apply("Union", Union.flatten()
                            .withWaits(getWaits())
                            .withStrategy(getStrategy()));
            outputs = input
                    .apply("Query", ParDo
                            .of(new QueryDoFn(query, parameters.table, parameters.filterJson(), inputSchema,
                                    sideInputViews, getLoggings(), getFailFast(), failureTag))
                            .withSideInputs(sideInputViews.values())
                            .withOutputTags(outputTag, TupleTagList.of(failureTag)));
        } else {
            outputs = expandWithBuffer(inputs, inputSchema, bufferConfig, query, parameters,
                    sideInputViews, outputTag, failureTag);
        }

        errorHandler.addError(outputs.get(failureTag));

        return MCollectionTuple
                .of(outputs.get(outputTag), outputSchema);
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
            final Map<String, PCollectionView<Iterable<MElement>>> sideInputViews,
            final TupleTag<MElement> outputTag,
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

        final BufferLookupSource bufferSource = query.getSources().stream()
                .filter(s -> s instanceof BufferLookupSource)
                .map(s -> (BufferLookupSource) s)
                .findAny()
                .orElseThrow(() -> new IllegalModuleException(
                        "query transform module[" + getName() + "] buffer source was not created"));
        final List<String> storedFields = resolveStoredFields(bufferConfig, bufferSource, inputSchema);

        final List<Schema.Field> stateFields = new ArrayList<>();
        for(final Schema.Field field : inputSchema.getFields()) {
            if(storedFields.contains(field.getName())) {
                stateFields.add(field);
            }
        }
        stateFields.add(Schema.Field.of(BufferLookupSource.TIMESTAMP_FIELD, Schema.FieldType.TIMESTAMP));
        stateFields.add(Schema.Field.of(BufferLookupSource.INPUT_FIELD, Schema.FieldType.STRING.withNullable(true)));
        final Coder<MElement> stateCoder = ElementCoder.of(Schema.of(stateFields));

        final BufferQueryProcessor processor = new BufferQueryProcessor(
                query, parameters.table, parameters.filterJson(), bufferConfig, storedFields,
                new ArrayList<>(inputs.getAll().keySet()), inputSchema,
                sideInputViews, getLoggings(), getFailFast(), failureTag);

        final boolean bounded = PCollection.IsBounded.BOUNDED.equals(keyed.isBounded());
        final DoFn<KV<String, MElement>, MElement> doFn = bounded
                ? new BatchBufferQueryDoFn(processor, stateCoder)
                : new StreamingBufferQueryDoFn(processor, stateCoder);
        return keyed
                .apply("Query", ParDo.of(doFn)
                        .withSideInputs(sideInputViews.values())
                        .withOutputTags(outputTag, TupleTagList.of(failureTag)));
    }

    /**
     * The input fields persisted in state per buffered element: the explicit
     * {@code fields} list when given (validated to cover every buffer column
     * the SQL references), otherwise the referenced columns collected from the
     * plan — in both cases forcing the groupFields in, in input schema order.
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
            stored.addAll(bufferConfig.groupFields());
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
            stored.addAll(bufferConfig.groupFields());
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

    private static class QueryDoFn extends DoFn<MElement, MElement> {

        private final Query2 query;
        private final String table;
        private final String filterJson;
        private final Schema inputSchema;
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
                final Map<String, PCollectionView<Iterable<MElement>>> sideInputViews,
                final List<Logging> logs,
                final boolean failFast,
                final TupleTag<BadRecord> failuresTag) {

            this.query = query;
            this.table = table;
            this.filterJson = filterJson;
            this.inputSchema = inputSchema;
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
                final List<MElement> results = query.execute(Map.of(table, List.of(input)), c.timestamp());
                for(final MElement output : results) {
                    Logging.log(LOG, logs, "output", output);
                    c.output(output);
                }
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
                final Map<String, PCollectionView<Iterable<MElement>>> sideInputViews,
                final List<Logging> logs,
                final boolean failFast,
                final TupleTag<BadRecord> failuresTag) {

            this.query = query;
            this.table = table;
            this.filterJson = filterJson;
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
                final MElement bufferElement = BufferLookupSource
                        .createBufferElement(input, timestamp, inputName, storedFields);

                final List<TimestampedValue<MElement>> buffered =
                        trigger ? readSorted(buffer) : null;
                if(persist) {
                    writeAndEvict(buffer, count, maxTsState, ttlTimer,
                            bufferElement, timestamp, buffered, window);
                }
                if(trigger) {
                    final List<MElement> visible = visibleRows(buffered, timestamp,
                            includeCurrent ? bufferElement : null);
                    bufferSource.setData(visible, bufferElement);
                    Logging.log(LOG, logs, "input", input);
                    final List<MElement> results =
                            query.execute(Map.of(table, List.of(input)), timestamp);
                    for(final MElement output : results) {
                        Logging.log(LOG, logs, "output", output);
                        c.output(output);
                    }
                }
            } catch (final Throwable e) {
                final BadRecord badRecord = processError("Failed to execute query", input, e, failFast);
                c.output(failuresTag, badRecord);
            }
        }

        /**
         * Persists the element and applies retention. Count-based eviction
         * computes a timestamp boundary from the pre-add buffer contents
         * (entries at the boundary timestamp survive — retention may briefly
         * exceed maxCount under identical timestamps; read-time trimming is
         * exact), and never clears at or past the current element's timestamp.
         */
        private void writeAndEvict(
                final OrderedListState<MElement> buffer,
                final ValueState<Long> count,
                final ValueState<Long> maxTsState,
                final Timer ttlTimer,
                final MElement bufferElement,
                final Instant timestamp,
                List<TimestampedValue<MElement>> buffered,
                final BoundedWindow window) {

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
                if(knownCount + 1 > maxCount) {
                    if(buffered == null) {
                        buffered = readSorted(buffer);
                    }
                    final List<TimestampedValue<MElement>> survivors = new ArrayList<>();
                    for(final TimestampedValue<MElement> entry : buffered) {
                        if(durationCutoff == null || !entry.getTimestamp().isBefore(durationCutoff)) {
                            survivors.add(entry);
                        }
                    }
                    final int keep = maxCount - 1; // the current element takes one slot
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
                    newCount = retained + 1;
                } else {
                    newCount = knownCount + 1;
                }
            }
            if(boundary != null) {
                buffer.clearRange(BoundedWindow.TIMESTAMP_MIN_VALUE, boundary);
            }
            buffer.add(TimestampedValue.of(bufferElement, timestamp));
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
         * Exact read-time visibility: duration cutoff, plus the current element
         * when {@code includeCurrent}, trimmed to the newest {@code maxCount} —
         * the query never sees more than maxCount buffer rows per evaluation.
         */
        private List<MElement> visibleRows(
                final List<TimestampedValue<MElement>> buffered,
                final Instant timestamp,
                final MElement current) {
            final List<MElement> visible = new ArrayList<>();
            final Instant cutoff = maxDurationMillis == null
                    ? null : timestamp.minus(Duration.millis(maxDurationMillis));
            if(buffered != null) {
                for(final TimestampedValue<MElement> entry : buffered) {
                    if(cutoff == null || !entry.getTimestamp().isBefore(cutoff)) {
                        visible.add(entry.getValue());
                    }
                }
            }
            if(current != null) {
                visible.add(current);
            }
            if(maxCount != null && visible.size() > maxCount) {
                return new ArrayList<>(visible.subList(visible.size() - maxCount, visible.size()));
            }
            return visible;
        }

        /** Clears the group's state when the TTL elapsed with no newer element. */
        void onTtl(
                final Instant firingTimestamp,
                final OrderedListState<MElement> buffer,
                final ValueState<Long> count,
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
                @StateId("count") final ValueState<Long> count) {
            processor.process(c, window, buffer, count, null, null);
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
                @StateId("maxTs") final ValueState<Long> maxTs,
                @TimerId("ttl") final Timer ttl) {
            processor.process(c, window, buffer, count, maxTs, ttl);
        }

        @OnTimer("ttl")
        public void onTtl(
                final OnTimerContext c,
                @StateId("buffer") final OrderedListState<MElement> buffer,
                @StateId("count") final ValueState<Long> count,
                @StateId("maxTs") final ValueState<Long> maxTs) {
            processor.onTtl(c.timestamp(), buffer, count, maxTs);
        }
    }

}
