package com.mercari.solution.module.action;

import com.google.gson.JsonObject;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.*;
import com.mercari.solution.module.Module;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.pipeline.Union;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Shared assembly logic behind the {@code action.<service>} modules. The thin adapters in
 * module/source, module/transform and module/sink all delegate here, so an action step behaves
 * identically wherever it is placed — placement in the config only tells the reader where the
 * step sits in the flow (no upstream → sources, consumed downstream → transforms, terminal → sinks).
 *
 * Trigger topologies:
 * <ul>
 *   <li>{@code once} — an internally generated seed element gated by {@code Wait.on} over all
 *       waits and inputs (inputs are pure completion signals).</li>
 *   <li>{@code perElement} — the flattened inputs drive one execution per element.</li>
 *   <li>{@code collect} — the flattened inputs are gathered (single-key GroupByKey) and the
 *       action fires once with the full element list. In streaming this happens per window.</li>
 * </ul>
 *
 * Every execution emits one record with the common envelope schema
 * ({@code service, op, jobId, state, startedAt, finishedAt, payload}); failures are routed as
 * {@code BadRecord}s honoring {@code failFast} / {@code failureSinks}.
 */
public class Actions {

    private static final String MODULE_PREFIX = "action.";

    public static boolean isActionModule(final String module) {
        return module != null && (module.equals("action") || module.startsWith(MODULE_PREFIX));
    }

    /**
     * Registers every discovered action service as {@code action.<service>} in a module-type
     * registry, mapped to that type's adapter class. Called from the Source/Transform/Sink
     * registry initializers so the same services are available at every placement.
     */
    @SuppressWarnings("unchecked")
    public static <T> java.util.Map<String, Class<T>> registerActionModules(
            final java.util.Map<String, Class<T>> modules,
            final Class<? extends T> adapterClass) {

        for(final String service : Action.serviceNames()) {
            modules.put(MODULE_PREFIX + service, (Class<T>) adapterClass);
        }
        return modules;
    }

    public static MCollectionTuple expand(
            final Module<?> module,
            final Pipeline pipeline,
            final MCollectionTuple inputs,
            final MErrorHandler errorHandler) {

        final String service = serviceOf(module.getModule());
        final JsonObject parametersJson = Config.convertConfigJson(module.getParametersText(), Config.Format.json);
        final Action.Trigger trigger = Action.Trigger.of(parametersJson);
        if(!Action.Trigger.once.equals(trigger) && inputs.size() == 0) {
            throw new IllegalModuleException(
                    "action module[" + module.getName() + "] with trigger: " + trigger + " requires inputs");
        }

        final Action action = Action.create(module.getName(), service, parametersJson, pipeline.getOptions());

        final TupleTag<MElement> outputTag = new TupleTag<>() {};
        final TupleTag<BadRecord> failureTag = new TupleTag<>() {};

        final PCollectionTuple outputs = switch (trigger) {
            case once -> {
                final List<PCollection<?>> waits = new ArrayList<>();
                if(module.getWaits() != null) {
                    waits.addAll(module.getWaits());
                }
                waits.addAll(inputs.getAll().values());

                final Schema seedSchema = MElement.dummySchema();
                PCollection<MElement> seed = pipeline.begin()
                        .apply("Seed", Create
                                .of("")
                                .withCoder(StringUtf8Coder.of()))
                        .apply("ToElement", ParDo.of(new SeedDoFn()))
                        .setCoder(ElementCoder.of(seedSchema));
                if(!waits.isEmpty()) {
                    seed = seed
                            .apply("Wait", Wait.on(waits))
                            .setCoder(ElementCoder.of(seedSchema));
                }
                yield seed
                        .apply("Action", ParDo
                                .of(new ExecuteDoFn(service, action, false, module.getFailFast(), failureTag))
                                .withOutputTags(outputTag, TupleTagList.of(failureTag)));
            }
            case perElement -> union(module, inputs)
                    .apply("Action", ParDo
                            .of(new ExecuteDoFn(service, action, true, module.getFailFast(), failureTag))
                            .withOutputTags(outputTag, TupleTagList.of(failureTag)));
            case collect -> {
                final PCollection<MElement> unioned = union(module, inputs);
                @SuppressWarnings("unchecked")
                final Coder<MElement> elementCoder = (Coder<MElement>) unioned.getCoder();
                yield unioned
                        .apply("WithSingleKey", WithKeys.of(""))
                        .setCoder(KvCoder.of(StringUtf8Coder.of(), elementCoder))
                        .apply("Collect", GroupByKey.create())
                        .apply("Action", ParDo
                                .of(new CollectExecuteDoFn(service, action, module.getFailFast(), failureTag))
                                .withOutputTags(outputTag, TupleTagList.of(failureTag)));
            }
        };

        errorHandler.addError(outputs.get(failureTag));

        return MCollectionTuple
                .of(outputs.get(outputTag), ActionResult.createOutputSchema());
    }

    private static PCollection<MElement> union(final Module<?> module, final MCollectionTuple inputs) {
        final Strategy strategy = switch (module) {
            case Sink sink -> sink.getStrategy();
            case Transform transform -> transform.getStrategy();
            default -> Strategy.createDefaultStrategy();
        };
        return inputs
                .apply("Union", Union.flatten()
                        .withWaits(module.getWaits())
                        .withStrategy(strategy));
    }

    private static String serviceOf(final String module) {
        if(module == null || !module.startsWith(MODULE_PREFIX) || module.length() <= MODULE_PREFIX.length()) {
            throw new IllegalModuleException(
                    "action module must be specified as action.<service> (e.g. action.bigquery)."
                            + " supported services: " + Action.serviceNames());
        }
        return module.substring(MODULE_PREFIX.length());
    }

    private static MElement createEnvelope(
            final String service,
            final ActionResult result,
            final Instant startedAt,
            final org.joda.time.Instant eventTime) {

        final MElement.Builder builder = MElement.builder()
                .withString("service", service)
                .withTimestamp("startedAt", startedAt)
                .withTimestamp("finishedAt", Instant.now())
                .withEventTime(eventTime);
        if(result != null) {
            builder
                    .withString("op", result.getOp())
                    .withString("jobId", result.getJobId())
                    .withString("state", result.getState())
                    .withString("payload", result.getPayload());
        }
        return builder.build();
    }

    private static class ExecuteDoFn extends DoFn<MElement, MElement> {

        private final String service;
        private final Action action;
        private final boolean perElement;

        private final boolean failFast;
        private final TupleTag<BadRecord> failureTag;


        ExecuteDoFn(
                final String service,
                final Action action,
                final boolean perElement,
                final boolean failFast,
                final TupleTag<BadRecord> failureTag) {

            this.service = service;
            this.action = action;
            this.perElement = perElement;
            this.failFast = failFast;
            this.failureTag = failureTag;
        }

        @Setup
        public void setup() {
            this.action.setup();
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final MElement input = c.element();
            if(input == null) {
                return;
            }
            final Instant startedAt = Instant.now();
            try {
                final ActionResult result = action.execute(perElement ? List.of(input) : List.of());
                c.output(createEnvelope(service, result, startedAt, c.timestamp()));
            } catch (final Throwable e) {
                final BadRecord badRecord = Module.processError(
                        "Failed to execute action service: " + service, input, e, failFast);
                c.output(failureTag, badRecord);
            }
        }

    }

    private static class CollectExecuteDoFn extends DoFn<KV<String, Iterable<MElement>>, MElement> {

        private final String service;
        private final Action action;

        private final boolean failFast;
        private final TupleTag<BadRecord> failureTag;


        CollectExecuteDoFn(
                final String service,
                final Action action,
                final boolean failFast,
                final TupleTag<BadRecord> failureTag) {

            this.service = service;
            this.action = action;
            this.failFast = failFast;
            this.failureTag = failureTag;
        }

        @Setup
        public void setup() {
            this.action.setup();
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final KV<String, Iterable<MElement>> input = c.element();
            if(input == null || input.getValue() == null) {
                return;
            }
            final List<MElement> elements = new ArrayList<>();
            for(final MElement element : input.getValue()) {
                elements.add(element);
            }
            final Instant startedAt = Instant.now();
            try {
                final ActionResult result = action.execute(elements);
                c.output(createEnvelope(service, result, startedAt, c.timestamp()));
            } catch (final Throwable e) {
                final MElement first = elements.isEmpty() ? MElement.createDummyElement(c.timestamp()) : elements.getFirst();
                final BadRecord badRecord = Module.processError(
                        "Failed to execute action service: " + service, first, e, failFast);
                c.output(failureTag, badRecord);
            }
        }

    }

    private static class SeedDoFn extends DoFn<String, MElement> {

        @ProcessElement
        public void processElement(final ProcessContext c) {
            c.output(MElement.createDummyElement(c.timestamp()));
        }

    }

}
