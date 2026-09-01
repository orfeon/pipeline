package com.mercari.solution.module.transform;

import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.mercari.solution.module.*;
import com.mercari.solution.util.pipeline.*;
import com.mercari.solution.util.pipeline.select.SelectFunction;
import com.mercari.solution.util.pipeline.select.stateful.StatefulFunction;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.*;
import org.joda.time.Instant;

import java.util.*;

@Transform.Module(name="select")
public class SelectTransform extends Transform {

    private static class Parameters {

        private JsonElement filter;
        private JsonArray select;
        private String flattenField;
        private JsonElement outputFilter;

        private List<String> groupFields;

        public boolean useSelect() {
            return select != null && select.isJsonArray();
        }

        public boolean useUnnest() {
            return flattenField != null;
        }

        public boolean useOutputFilter() {
            return outputFilter != null && !outputFilter.isJsonNull();
        }

        private void validate(Schema inputSchema) {
            final List<String> errorMessages = new ArrayList<>();
            if((this.filter == null || this.filter.isJsonNull())
                    && (this.select == null || !this.select.isJsonArray())
                    && !useOutputFilter()) {

                errorMessages.add("requires filter, select or outputFilter parameter.");
            } else if(this.select != null && this.select.isJsonArray()) {
                int i=0;
                for(final JsonElement e : this.select.getAsJsonArray()) {
                    if(!e.isJsonObject()) {
                        errorMessages.add("select[" + i + "] must be object but: " + e);
                    }
                    i++;
                }
            }
            if(this.groupFields != null && !this.groupFields.isEmpty()) {
                if(!this.groupFields.stream().allMatch(inputSchema::hasField)) {
                    errorMessages.add("all groupFields[" + groupFields + "] must in input schema: " + inputSchema.getFields());
                }
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        private void setDefaults() {
            if(this.groupFields == null) {
                this.groupFields = new ArrayList<>();
            }
        }
    }

    @Override
    public MCollectionTuple expand(
            final MCollectionTuple inputs,
            final MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);

        final Schema inputSchema = Union.createUnionSchema(inputs);
        parameters.validate(inputSchema);
        parameters.setDefaults();

        final Schema outputSchema = createOutputSchema(inputSchema, parameters);

        // outputFilter references the fields after select / flatten: validate against the output
        // schema (the SQL-like text form is translated to the same condition structure by parse,
        // so it is validated identically; a malformed condition is an assembly-time error)
        if(parameters.useOutputFilter()) {
            final List<String> errorMessages = new ArrayList<>();
            try {
                final Filter.ConditionNode node = Filter.parse(parameters.outputFilter);
                if(isEmptyCondition(node)) {
                    errorMessages.add("parameters.outputFilter must not be an empty condition (it would match no record)");
                } else {
                    for(final String m : node.validate(outputSchema.getFields())) {
                        errorMessages.add("parameters.outputFilter is illegal (fields are validated against the output schema): " + m);
                    }
                }
            } catch (final Throwable e) {
                errorMessages.add("parameters.outputFilter is illegal: " + e.getMessage());
            }
            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        final TupleTag<MElement> outputTag = new TupleTag<>() {};
        final TupleTag<BadRecord> failuresTag = new TupleTag<>() {};

        final List<SelectFunction> selectFunctions = SelectFunction
                .of(parameters.select, inputSchema.getFields());

        final PCollectionTuple outputs;
        if(SelectFunction.isGrouping(selectFunctions)) {
            final PCollection<KV<String,MElement>> input = inputs
                    .apply("UnionWithKey", Union.withKeys(parameters.groupFields)
                            .withWaits(getWaits())
                            .withStrategy(getStrategy()));
            if(SelectFunction.isStateful(selectFunctions)) {
                final KvCoder<String, MElement> inputCoder = (KvCoder<String, MElement>) input.getCoder();
                final StatefulSelectDoFn statefulSelectDoFn;
                if(OptionUtil.isStreaming(inputs)) {
                    statefulSelectDoFn = new StatefulStreamingSelectDoFn(
                            getJobName(), getName(),
                            inputSchema, outputSchema, parameters.filter, selectFunctions, parameters.flattenField, parameters.outputFilter,
                            getLoggings(), getFailFast(), failuresTag, inputCoder.getValueCoder());
                } else {
                    statefulSelectDoFn = new StatefulBatchSelectDoFn(
                            getJobName(), getName(),
                            inputSchema, outputSchema, parameters.filter, selectFunctions, parameters.flattenField, parameters.outputFilter,
                            getLoggings(), getFailFast(), failuresTag, inputCoder.getValueCoder());
                }
                outputs = input
                        .apply("StatefulSelect", ParDo
                                .of(statefulSelectDoFn)
                                .withOutputTags(outputTag, TupleTagList.of(failuresTag)));
            } else {
                outputs = input
                        .apply("GroupByKey", GroupByKey.create())
                        .apply("NavigationSelect", ParDo
                                .of(new NavigationSelectDoFn(
                                        getJobName(), getName(),
                                        outputSchema, parameters.filter, selectFunctions, parameters.flattenField, parameters.outputFilter,
                                        getLoggings(), getFailFast(), failuresTag))
                                .withOutputTags(outputTag, TupleTagList.of(failuresTag)));
            }
        } else {
            outputs = inputs
                    .apply("Union", Union.flatten()
                            .withWaits(getWaits())
                            .withStrategy(getStrategy()))
                    .apply("StatelessSelect", ParDo
                            .of(new StatelessSelectDoFn(
                                    getJobName(), getName(),
                                    outputSchema, parameters.filter, selectFunctions, parameters.flattenField, parameters.outputFilter,
                                    getLoggings(), getFailFast(), failuresTag))
                            .withOutputTags(outputTag, TupleTagList.of(failuresTag)));
        }

        if(errorHandler != null) {
            errorHandler.addError(outputs.get(failuresTag));
        }

        return MCollectionTuple
                .of(outputs.get(outputTag), outputSchema);
    }

    // an AND/OR node with no leaves and no child nodes (e.g. `[]` or `{and: []}`) evaluates to
    // "match nothing" at runtime — reject it at assembly instead of silently dropping every record
    private static boolean isEmptyCondition(final Filter.ConditionNode node) {
        if(node == null) {
            return true;
        }
        if(Filter.Type.TRUE.equals(node.getType()) || Filter.Type.FALSE.equals(node.getType())) {
            return false;
        }
        return (node.getLeaves() == null || node.getLeaves().isEmpty())
                && (node.getNodes() == null || node.getNodes().isEmpty());
    }

    private Schema createOutputSchema(final Schema inputSchema, final Parameters parameters) {
        Schema outputSchema;
        if(parameters.useSelect()) {
            final List<SelectFunction> selectFunctions = SelectFunction.of(parameters.select, inputSchema.getFields());
            if(selectFunctions.isEmpty()) {
                // copy: withType below must not mutate the upstream module's schema instance
                outputSchema = inputSchema.copy();
            } else {
                outputSchema = SelectFunction.createSchema(selectFunctions);
            }
        } else {
            outputSchema = inputSchema.copy();
        }

        if(parameters.useUnnest()) {
            outputSchema = Unnest.createSchema(outputSchema.getFields(), parameters.flattenField);
        }

        final DataType outputType = Optional
                .ofNullable(getOutputType())
                .orElse(DataType.AVRO);
        return outputSchema.withType(outputType);
    }



    private static class SelectDoFn<InputT> extends DoFn<InputT, MElement> {

        protected final String jobName;
        protected final String moduleName;
        protected final Schema outputSchema;

        protected final Map<String, Logging> logging;

        protected final boolean failFast;
        protected final TupleTag<BadRecord> failuresTag;

        protected final Filter filter;
        protected final Select select;
        protected final Unnest unnest;
        protected final Filter outputFilter;


        public SelectDoFn(
                final String jobName,
                final String moduleName,
                //
                final Schema outputSchema,
                final JsonElement filterJson,
                final List<SelectFunction> selectFunctions,
                final String flattenField,
                final JsonElement outputFilterJson,
                //
                final List<Logging> logging,
                final boolean failFast,
                final TupleTag<BadRecord> failuresTag) {

            this.jobName = jobName;
            this.moduleName = moduleName;
            this.outputSchema = outputSchema;

            this.logging = Logging.map(logging);

            this.failFast = failFast;
            this.failuresTag = failuresTag;

            this.filter = Filter.of(filterJson);
            this.select = Select.of(selectFunctions);
            this.unnest = Unnest.of(flattenField);
            this.outputFilter = Filter.of(outputFilterJson);
        }

        /**
         * Applies outputFilter to an output record using the typed (standard) representation of
         * the output schema fields, so timestamp / date / enum conditions behave as documented
         * for filter conditions. Returns true when the record should be emitted.
         */
        protected boolean filterOutput(final MElement output) {
            if(outputFilter.filter(outputSchema.getFields(), output)) {
                return true;
            }
            if(logging.containsKey("output_not_matched")) {
                Logging.log(LOG, logging, "output_not_matched", output);
            }
            return false;
        }

    }

    private static class StatelessSelectDoFn extends SelectDoFn<MElement> {

        public StatelessSelectDoFn(
                final String jobName,
                final String moduleName,
                //
                final Schema outputSchema,
                final JsonElement filterJson,
                final List<SelectFunction> selectFunctions,
                final String flattenField,
                final JsonElement outputFilterJson,
                //
                final List<Logging> logging,
                final boolean failFast,
                final TupleTag<BadRecord> failuresTag) {

            super(jobName, moduleName, outputSchema, filterJson, selectFunctions, flattenField, outputFilterJson, logging, failFast, failuresTag);
        }

        @Setup
        public void setup() {
            filter.setup();
            select.setup();
            unnest.setup();
            outputFilter.setup();
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final MElement input = c.element();
            if(input == null) {
                return;
            }

            try {
                Logging.log(LOG, logging, "input", input);

                if (!filter.filter(input)) {
                    Logging.log(LOG, logging, "not_matched", input);
                    return;
                }

                final List<MElement> outputs = new ArrayList<>();
                if (!select.useSelect() && !unnest.useUnnest()) {
                    outputs.add(input);
                } else if(select.useSelect()) {
                    final Map<String, Object> primitiveValues = select.select(input, c.timestamp());
                    if(unnest.useUnnest()) {
                        final List<Map<String, Object>> list = unnest.unnest(primitiveValues);
                        outputs.addAll(MElement.ofList(list, c.timestamp()));
                    } else {
                        final MElement output = MElement.of(primitiveValues, c.timestamp());
                        outputs.add(output);
                    }
                } else {
                    final List<Map<String, Object>> list = unnest.unnest(input);
                    outputs.addAll(MElement.ofList(list, c.timestamp()));
                }

                for(final MElement output : outputs) {
                    if(!filterOutput(output)) {
                        continue;
                    }
                    final MElement output_ = output.convert(outputSchema);
                    c.output(output_);
                    Logging.log(LOG, logging, "output", output_);
                }
            } catch (final Throwable e) {
                final BadRecord badRecord = processError("Failed to process stateless select", input, e, failFast);
                c.output(failuresTag, badRecord);
            }
        }
    }

    private static class StatefulSelectDoFn extends SelectDoFn<KV<String, MElement>> {

        protected static final String STATE_ID_BUFFER = "statefulSelectBuffer";
        protected static final String STATE_ID_MAX_COUNT_TIME = "statefulSelectMaxCountTime";
        protected static final String TIMER_ID = "statefulSelectBufferTimer";

        private final Schema inputSchema;
        private final StatefulFunction.RangeBound maxRange;

        private transient org.apache.avro.Schema stateAvroSchema;

        public StatefulSelectDoFn(
                final String jobName,
                final String moduleName,
                //
                final Schema inputSchema,
                final Schema outputSchema,
                final JsonElement filterJson,
                final List<SelectFunction> selectFunctions,
                final String flattenField,
                final JsonElement outputFilterJson,
                //
                final List<Logging> logging,
                final boolean failFast,
                final TupleTag<BadRecord> failuresTag) {

            super(jobName, moduleName, outputSchema, filterJson, selectFunctions, flattenField, outputFilterJson, logging, failFast, failuresTag);
            this.inputSchema = inputSchema;
            this.maxRange = StatefulFunction.calcMaxRange(selectFunctions);
        }

        public void setup() {
            filter.setup();
            select.setup();
            unnest.setup();
            outputFilter.setup();
            stateAvroSchema = Select.createStateAvroSchema(inputSchema, select.getSelectFunctions());
        }

        public List<MElement> process(
                final MElement input,
                final OrderedListState<MElement> bufferState,
                final ValueState<Instant> maxCountState,
                final Instant eventTime) {

            if (!filter.filter(input)) {
                Logging.log(LOG, logging, "not_matched", input);
                return new ArrayList<>();
            }

            final Map<String, Object> primitiveValues = processStateful(input, bufferState, maxCountState, eventTime);

            final List<MElement> outputs = new ArrayList<>();
            final List<Map<String, Object>> list;
            if(unnest.useUnnest()) {
                list = unnest.unnest(primitiveValues);
            } else {
                list = List.of(primitiveValues);
            }
            for(final Map<String, Object> values : list) {
                final MElement output = MElement.of(values, eventTime);
                if(!filterOutput(output)) {
                    continue;
                }
                final MElement output_ = output.convert(outputSchema);
                outputs.add(output_);
            }
            return outputs;
        }

        public Map<String, Object> processStateful(
                final MElement input,
                final OrderedListState<MElement> bufferState,
                final ValueState<Instant> maxCountState,
                final Instant eventTime) {

            final Instant countMinTimestamp = Optional
                    .ofNullable(maxCountState.read())
                    .orElseGet(() -> Instant.ofEpochMilli(0L));
            final Instant durationMinTimestamp = maxRange.firstTimestamp(eventTime);

            // read state
            final Instant maxMinTimestamp = durationMinTimestamp.compareTo(countMinTimestamp) > 0 ? countMinTimestamp : durationMinTimestamp;
            final List<TimestampedValue<MElement>> buffer = Lists.newArrayList(bufferState.readRange(maxMinTimestamp, eventTime));

            // process
            final Map<String, Object> output = select.select(input, buffer, eventTime);

            // update state
            if(buffer.size() >= maxRange.maxCount) {
                maxCountState.write(buffer.get(buffer.size() - maxRange.maxCount).getTimestamp());
            }

            final GenericRecord record = AvroSchemaUtil.create(stateAvroSchema, output);
            final MElement stateElement = MElement.of(record, eventTime);

            bufferState.clearRange(Instant.ofEpochMilli(0L), maxMinTimestamp);
            bufferState.add(TimestampedValue.of(stateElement, eventTime));

            return output;
        }

    }

    private static class StatefulBatchSelectDoFn extends StatefulSelectDoFn {

        @StateId(STATE_ID_BUFFER)
        private final StateSpec<OrderedListState<MElement>> bufferStateSpec;
        @StateId(STATE_ID_MAX_COUNT_TIME)
        private final StateSpec<ValueState<Instant>> maxCountTimeStateSpec;

        public StatefulBatchSelectDoFn(
                final String jobName,
                final String moduleName,
                //
                final Schema inputSchema,
                final Schema outputSchema,
                final JsonElement filterJson,
                final List<SelectFunction> selectFunctions,
                final String flattenField,
                final JsonElement outputFilterJson,
                //
                final List<Logging> logging,
                final boolean failFast,
                final TupleTag<BadRecord> failuresTag,
                final Coder<MElement> inputCoder) {

            super(jobName, moduleName, inputSchema, outputSchema, filterJson, selectFunctions, flattenField, outputFilterJson, logging, failFast, failuresTag);

            this.bufferStateSpec = StateSpecs.orderedList(inputCoder);
            this.maxCountTimeStateSpec = StateSpecs.value(InstantCoder.of());
        }

        @Setup
        public void setup() {
            super.setup();
        }

        @ProcessElement
        @RequiresTimeSortedInput
        public void processElement(
                final ProcessContext c,
                final @StateId(STATE_ID_BUFFER) OrderedListState<MElement> bufferState,
                final @StateId(STATE_ID_MAX_COUNT_TIME) ValueState<Instant> maxCountState) {

            final KV<String, MElement> kv = c.element();
            if(kv == null) {
                return;
            }
            final MElement input = kv.getValue();
            if(input == null) {
                return;
            }

            try {
                Logging.log(LOG, logging, "input", input);

                final List<MElement> outputs = process(input, bufferState, maxCountState, c.timestamp());

                for(final MElement output : outputs) {
                    c.output(output);
                    Logging.log(LOG, logging, "output", output);
                }
            } catch (final Throwable e) {
                final BadRecord badRecord = processError("Failed to process stateful batch select", input, e, failFast);
                c.output(failuresTag, badRecord);
            }
        }


        @OnWindowExpiration
        public void onWindowExpiration(
                final OutputReceiver<MElement> receiver,
                final @StateId(STATE_ID_BUFFER) OrderedListState<MElement> bufferState,
                final @StateId(STATE_ID_MAX_COUNT_TIME) ValueState<Instant> maxCountState) {

            LOG.info("onWindowExpiration");

            //flush(receiver, bufferState, bufferSizeState);
        }

    }

    private static class StatefulStreamingSelectDoFn extends StatefulSelectDoFn {

        @StateId(STATE_ID_BUFFER)
        private final StateSpec<OrderedListState<MElement>> bufferStateSpec;
        @StateId(STATE_ID_MAX_COUNT_TIME)
        private final StateSpec<ValueState<Instant>> maxCountTimeStateSpec;

        @TimerId(TIMER_ID)
        private final TimerSpec timer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

        public StatefulStreamingSelectDoFn(
                final String jobName,
                final String moduleName,
                //
                final Schema inputSchema,
                final Schema outputSchema,
                final JsonElement filterJson,
                final List<SelectFunction> selectFunctions,
                final String flattenField,
                final JsonElement outputFilterJson,
                //
                final List<Logging> logging,
                final boolean failFast,
                final TupleTag<BadRecord> failuresTag,
                final Coder<MElement> inputCoder) {

            super(jobName, moduleName, inputSchema, outputSchema, filterJson, selectFunctions, flattenField, outputFilterJson, logging, failFast, failuresTag);

            this.bufferStateSpec = StateSpecs.orderedList(inputCoder);
            this.maxCountTimeStateSpec = StateSpecs.value(InstantCoder.of());
        }

        @Setup
        public void setup() {
            super.setup();
        }

        @ProcessElement
        public void processElement(
                ProcessContext c,
                final @StateId(STATE_ID_BUFFER) OrderedListState<MElement> bufferState,
                final @StateId(STATE_ID_MAX_COUNT_TIME) ValueState<Instant> maxCountState,
                final @TimerId(TIMER_ID) Timer timer) {

            final KV<String, MElement> kv = c.element();
            if(kv == null) {
                return;
            }
            final MElement input = kv.getValue();
            if(input == null) {
                return;
            }

            try {
                Logging.log(LOG, logging, "input", input);

                final List<MElement> outputs = process(input, bufferState, maxCountState, c.timestamp());

                for(final MElement output : outputs) {
                    c.output(output);
                    Logging.log(LOG, logging, "output", output);
                }
            } catch (final Throwable e) {
                final BadRecord badRecord = processError("Failed to process stateful streaming select", input, e, failFast);
                c.output(failuresTag, badRecord);
            }
        }

        @OnWindowExpiration
        public void onWindowExpiration(
                final OutputReceiver<Row> receiver,
                final @StateId(STATE_ID_BUFFER) OrderedListState<MElement> bufferState,
                final @StateId(STATE_ID_MAX_COUNT_TIME) ValueState<Instant> maxCountState) {

            LOG.info("onWindowExpiration");

            //flush(receiver, bufferState, bufferSizeState);
        }

    }

    private static class NavigationSelectDoFn extends SelectDoFn<KV<String, Iterable<MElement>>> {

        public NavigationSelectDoFn(
                final String jobName,
                final String moduleName,
                //
                final Schema outputSchema,
                final JsonElement filterJson,
                final List<SelectFunction> selectFunctions,
                final String flattenField,
                final JsonElement outputFilterJson,
                //
                final List<Logging> logging,
                final boolean failFast,
                final TupleTag<BadRecord> failuresTag) {

            super(jobName, moduleName, outputSchema, filterJson, selectFunctions, flattenField, outputFilterJson, logging, failFast, failuresTag);
        }

        @Setup
        public void setup() {
            filter.setup();
            select.setup();
            unnest.setup();
            outputFilter.setup();
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            // TODO (apply filterOutput to emitted records when implemented)
        }
    }

}
