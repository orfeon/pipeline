package com.mercari.solution.util.pipeline;

import com.mercari.solution.module.DataType;
import com.mercari.solution.module.Logging;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Module;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.coder.UnionMapCoder;
import com.mercari.solution.util.domain.web.HttpUtil;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

public class Http {

    private static final Logger LOG = LoggerFactory.getLogger(Http.class);

    public static Transform transform(
            final List<HttpUtil.Request> requests,
            final Integer timeoutSecond,
            final HttpUtil.RetryParameters retry,
            final HttpUtil.BackoffParameters backoff,
            final List<Logging> logging,
            final boolean failFast) {

        final List<String> requestJsons = requests.stream().map(HttpUtil.Request::toJsonString).toList();
        return new Transform(requestJsons, timeoutSecond, retry, backoff, logging, failFast);
    }

    public static class Transform extends PTransform<PCollection<MElement>, PCollectionTuple> {

        private final List<String> requestJsons;
        private final Integer timeoutSecond;
        private final HttpUtil.RetryParameters retry;
        private final HttpUtil.BackoffParameters backoff;

        private final List<Logging> logging;
        private final boolean failFast;

        public final TupleTag<MElement> outputTag;
        public final TupleTag<BadRecord> failureTag;

        Transform(
                final List<String> requestJsons,
                final Integer timeoutSecond,
                final HttpUtil.RetryParameters retry,
                final HttpUtil.BackoffParameters backoff,
                final List<Logging> logging,
                final boolean failFast) {

            this.requestJsons = requestJsons;
            this.timeoutSecond = timeoutSecond;
            this.retry = retry;
            this.backoff = backoff;

            this.logging = logging;
            this.failFast = failFast;

            this.outputTag = new TupleTag<>() {};
            this.failureTag = new TupleTag<>() {};
        }

        @Override
        public PCollectionTuple expand(PCollection<MElement> input) {

            final List<HttpUtil.Request> requests = requestJsons.stream().map(HttpUtil.Request::fromJsonString).toList();
            final Map<String, Set<String>> requestDependencies = HttpUtil.Request.resolveDependencies(requests);

            PCollectionList<MElement> responseList = PCollectionList.empty(input.getPipeline());
            PCollectionList<BadRecord> failuresList = PCollectionList.empty(input.getPipeline());

            final ElementCoder outputCoder = ElementCoder.of(Schema.of(HttpUtil.Request.schema().getAvroSchema()));

            int level = 0;
            Set<String> parents = new HashSet<>();
            parents.add("");
            final Map<String, PCollection<MElement>> inputs = new HashMap<>();
            inputs.put("", input);

            final PBegin begin = input.getPipeline().begin();
            while(!parents.isEmpty()) {
                final List<HttpUtil.Request> childRequests = getSubRequests(requests, parents);
                final PCollectionView<Iterable<String>> requestsWithKey = begin
                        .apply("Parents" + level, Create
                                .of(HttpUtil.Request.toJsonStringList(childRequests))
                                .withCoder(StringUtf8Coder.of()))
                        .apply("FeedRequests" + level, ParDo
                                .of(new FeedRequestDoFn()))
                        .setCoder(StringUtf8Coder.of())
                        .apply("ViewAsIterable" + level, View.asIterable());

                int index = 0;
                for(final String parent : parents) {
                    final Set<String> children = Optional.ofNullable(requestDependencies.get(parent)).orElseGet(HashSet::new);
                    final Map<String, TupleTag<MElement>> childOutputTags = new HashMap<>();
                    for(final String child : children) {
                        childOutputTags.put(child, new TupleTag<>() {});
                    }
                    final TupleTag<BadRecord> failureTag = new TupleTag<>() {};
                    final TupleTag<KV<String,KV<String, Map<String,Object>>>> createRequestOutputTag = new TupleTag<>() {};

                    final PCollectionTuple createRequestOutputs = inputs.get(parent)
                            .apply(String.format("CreateRequest%d_%d", level, index), ParDo
                                    .of(new CreateRequestDoFn(requestsWithKey, logging, failFast, failureTag))
                                    .withSideInputs(requestsWithKey)
                                    .withOutputTags(createRequestOutputTag, TupleTagList.of(failureTag)));
                    final PCollectionTuple outputs = createRequestOutputs.get(createRequestOutputTag)
                            .setCoder(KvCoder.of(StringUtf8Coder.of(), KvCoder.of(StringUtf8Coder.of(), UnionMapCoder.mapCoder())))
                            .apply(String.format("ExecuteRequest%d_%d", level, index), ParDo
                                    .of(new ExecuteRequestDoFn(timeoutSecond, retry, backoff, logging, failFast, childOutputTags))
                                    .withOutputTags(failureTag, TupleTagList.of(new ArrayList<>(childOutputTags.values()))));
                    for(final Map.Entry<String, TupleTag<MElement>> entry : childOutputTags.entrySet()) {
                        final PCollection<MElement> output = outputs.get(entry.getValue()).setCoder(outputCoder);
                        inputs.put(entry.getKey(), output);
                        responseList = responseList.and(output);
                    }
                    failuresList = failuresList
                            .and(createRequestOutputs.get(failureTag))
                            .and(outputs.get(failureTag));
                }

                parents = parents.stream()
                        .flatMap(p -> requestDependencies.getOrDefault(p, new HashSet<>()).stream())
                        .filter(requestDependencies::containsKey)
                        .collect(Collectors.toSet());
                level += 1;
            }

            final PCollection<MElement> result = responseList
                    .apply("Flatten", Flatten.pCollections());
            final PCollection<BadRecord> failures = failuresList
                    .apply("FlattenFailures", Flatten.pCollections());

            return PCollectionTuple
                    .of(outputTag, result)
                    .and(failureTag, failures);
        }

        private static List<HttpUtil.Request> getSubRequests(final List<HttpUtil.Request> requests, final Set<String> names) {
            final Map<String, Set<String>> requestDependencies = HttpUtil.Request.resolveDependencies(requests);
            final Set<String> childrenNames = names.stream()
                    .flatMap(n -> requestDependencies.getOrDefault(n, new HashSet<>()).stream())
                    .collect(Collectors.toSet());
            return requests.stream()
                    .filter(request -> request.getName() != null && childrenNames.contains(request.getName()))
                    .collect(Collectors.toList());
        }

        private static class FeedRequestDoFn extends DoFn<String, String> {
            @ProcessElement
            public void processElement(final ProcessContext c) {
                final String requestJson = c.element();
                c.output(requestJson);
            }
        }

        private static class CreateRequestDoFn extends DoFn<MElement, KV<String,KV<String, Map<String,Object>>>> {

            private final PCollectionView<Iterable<String>> requestsWithKey;

            private final Map<String, Logging> logs;
            private final boolean failFast;
            private final TupleTag<BadRecord> failuresTag;

            public CreateRequestDoFn(
                    final PCollectionView<Iterable<String>> requestsWithKey,
                    final List<Logging> logging,
                    final boolean failFast,
                    final TupleTag<BadRecord> failuresTag) {

                this.requestsWithKey = requestsWithKey;
                this.logs = Logging.map(logging);
                this.failFast = failFast;
                this.failuresTag = failuresTag;
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final MElement input = c.element();
                if(input == null) {
                    return;
                }
                Logging.log(LOG, logs, "input", input);

                final Iterable<String> requestJsons = c.sideInput(requestsWithKey);
                if(requestJsons == null) {
                    return;
                }

                try {
                    for(final String requestJson : requestJsons) {
                        if(requestJson == null) {
                            continue;
                        }
                        final HttpUtil.Request request = HttpUtil.Request
                                .fromJsonString(requestJson)
                                .setup();
                        for(final Map<String, Object> inputValue : request.process(input)) {
                            final String group = request.getGroup();
                            c.output(KV.of(group, KV.of(requestJson, inputValue)));
                        }
                    }
                } catch (final Throwable e) {
                    final BadRecord badRecord = Module.processError("Failed to create request", input, e, failFast);
                    c.output(failuresTag, badRecord);
                }
            }
        }

        private static class ExecuteRequestDoFn extends DoFn<KV<String,KV<String, Map<String,Object>>>, BadRecord> {

            private static final String CLIENT_NAME = "";
            private static final Map<String, HttpClient> clients = new HashMap<>();

            private final Integer timeoutSecond;
            private final HttpUtil.RetryParameters retry;
            private final HttpUtil.BackoffParameters backoff;

            private final Map<String, Logging> logs;
            private final boolean failFast;

            private final Map<String, TupleTag<MElement>> outputTags;

            private transient Schema outputSchema;

            public ExecuteRequestDoFn(
                    final Integer timeoutSecond,
                    final HttpUtil.RetryParameters retry,
                    final HttpUtil.BackoffParameters backoff,
                    final List<Logging> logging,
                    final boolean failFast,
                    final Map<String, TupleTag<MElement>> outputTags) {

                this.timeoutSecond = timeoutSecond;

                this.retry = retry;
                this.backoff = backoff;

                this.logs = Logging.map(logging);

                this.failFast = failFast;
                this.outputTags = outputTags;
            }

            @Setup
            public void setup() {
                getOrCreateClient(clients, timeoutSecond);
                this.outputSchema = HttpUtil.Request.schema().setup(DataType.AVRO);
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final KV<String, KV<String, Map<String,Object>>> input = c.element();
                if(input == null) {
                    return;
                }
                final KV<String, Map<String,Object>> requestJsonAndValues = input.getValue();
                if(requestJsonAndValues == null) {
                    return;
                }

                try {
                    final HttpClient client = Optional
                            .ofNullable(clients.get(CLIENT_NAME))
                            .orElseGet(() -> getOrCreateClient(clients, timeoutSecond));
                    final HttpUtil.Request request = HttpUtil.Request
                            .fromJsonString(requestJsonAndValues.getKey())
                            .setup();
                    for(final Map<String, Object> result : HttpUtil.execute(client, request, requestJsonAndValues.getValue())) {
                        final MElement output = MElement
                                .of(result, c.timestamp())
                                .convert(outputSchema);
                        final String name = request.getName();
                        c.output(outputTags.get(name), output);
                        Logging.log(LOG, logs, "output", output);
                    }
                } catch (final Throwable e) {
                    final BadRecord badRecord = Module.processError("Failed to create request", requestJsonAndValues.getValue(), e, failFast);
                    c.output(badRecord);
                }
            }

            public synchronized static HttpClient getOrCreateClient(
                    final Map<String, HttpClient> clients,
                    final long timeoutSecond) {

                return Optional
                        .ofNullable(clients.get(CLIENT_NAME))
                        .orElseGet(() -> {
                            clients.put(CLIENT_NAME, HttpClient.newBuilder()
                                    .connectTimeout(Duration.ofSeconds(timeoutSecond))
                                    .build());
                            return clients.get(CLIENT_NAME);
                        });
            }
        }
    }
}
