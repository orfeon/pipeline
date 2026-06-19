package com.mercari.solution.util.pipeline;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.*;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.FailureUtil;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.coder.UnionMapCoder;
import com.mercari.solution.util.domain.web.HttpUtil;
import com.mercari.solution.util.schema.converter.JsonToMapConverter;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;

public class Auth {

    private static final Logger LOG = LoggerFactory.getLogger(Auth.class);

    public static class Parameters implements Serializable {

        public Map<String, String> args;
        public Refresh refresh;
        public HttpUtil.Format format;

        public List<HttpUtil.Request> requests;
        public Integer timeoutSecond;

        public List<String> validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(requests == null || requests.isEmpty()) {
                errorMessages.add("auth parameter requires `requests` parameter");
            } else {
                for(var request : requests) {
                    errorMessages.addAll(request.validate());
                }
            }
            return errorMessages;
        }

        public void setDefaults() {
            if(args == null) {
                args = new HashMap<>();
            }
            if(timeoutSecond == null) {
                timeoutSecond = 60;
            }
            if(format == null) {
                format = HttpUtil.Format.json;
            }
            if(requests == null) {
                requests = new ArrayList<>();
            }
            for(var request : requests) {
                request.setDefaults();
            }
            if(refresh == null) {
                refresh = new Refresh();
            }

        }

        public void setup() {
            for(var request : requests) {
                request.setup();
            }
        }

        public JsonObject toJson() {
            final JsonObject jsonObject = new JsonObject();
            {
                final JsonObject argsJson = new JsonObject();
                for(final Map.Entry<String, String> entry : args.entrySet()) {
                    argsJson.addProperty(entry.getKey(), entry.getValue());
                }
                jsonObject.add("args", argsJson);
            }
            jsonObject.addProperty("format", format.name());

            final JsonArray requestArray = new JsonArray();
            for(final HttpUtil.Request request : requests) {
                final JsonObject requestJson = request.toJson();
                requestArray.add(requestJson);
            }
            jsonObject.add("requests", requestArray);
            return jsonObject;
        }

        public String toJsonString() {
            final JsonObject jsonObject = toJson();
            return jsonObject.toString();
        }

        public static Parameters fromJsonString(final String jsonString) {
            return new Gson().fromJson(jsonString, Parameters.class);
        }

    }

    public static class Refresh implements Serializable {

        public Long intervalMinute;
        public Long maxMinute;

        private List<String> validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(intervalMinute != null && intervalMinute < 1) {
                errorMessages.add("auth.refresh.intervalMinute must be over than 0");
            }
            if(maxMinute != null && maxMinute < 1) {
                errorMessages.add("auth.refresh.maxMinute must be over than 0");
            }
            return errorMessages;
        }

        public void setDefaults() {
        }

    }

    public static Transform of(
            final Parameters parameters,
            final boolean failFast,
            final List<Logging> loggings) {

        return new Transform(parameters, failFast, loggings);
    }

    public static Transform google(
            final boolean failFast,
            final List<Logging> loggings) {


        final Parameters parameters = new Parameters();

        return new Transform(parameters, failFast, loggings);
    }

    public static class Transform extends PTransform<PBegin, PCollectionTuple> {

        private final String parametersText;
        private final boolean failFast;
        private final Map<String, Logging> loggings;

        public final TupleTag<Map<String, Object>> outputTag;
        public final TupleTag<BadRecord> failureTag;

        Transform(
                final Parameters parameters,
                final boolean failFast,
                final List<Logging> loggings) {

            this.parametersText = parameters.toJsonString();
            this.failFast = failFast;
            this.loggings = Logging.map(loggings);

            this.outputTag = new TupleTag<>() {};
            this.failureTag = new TupleTag<>() {};
        }

        @Override
        public PCollectionTuple expand(PBegin begin) {

            final Parameters parameters = new Gson().fromJson(parametersText, Parameters.class);
            parameters.setDefaults();
            parameters.validate();
            parameters.setDefaults();

            final PCollection<Long> seeds;
            if(parameters.refresh.intervalMinute != null && OptionUtil.isStreaming(begin)) {
                GenerateSequence generateSequence = GenerateSequence
                        .from(0)
                        .withRate(1, DateTimeUtil
                                .getDuration(DateTimeUtil.TimeUnit.minute, parameters.refresh.intervalMinute));
                if(parameters.refresh.maxMinute != null) {
                    generateSequence = generateSequence
                            .withMaxReadTime(DateTimeUtil
                                    .getDuration(DateTimeUtil.TimeUnit.minute, parameters.refresh.maxMinute));
                }
                seeds = begin
                        .apply("GenerateSequence", generateSequence);
            } else {
                seeds = begin
                        .apply("Seed", Create
                                .of(0L)
                                .withCoder(VarLongCoder.of()))
                        .setCoder(VarLongCoder.of());
            }

            final PCollectionTuple outputs = seeds
                    .apply("HttpCall", ParDo
                            .of(new RequestDoFn(parameters, failFast, failureTag, loggings))
                            .withOutputTags(outputTag, TupleTagList.of(failureTag)));

            return PCollectionTuple
                    .of(outputTag, outputs.get(outputTag)
                            .setCoder(UnionMapCoder.mapCoder()))
                    .and(failureTag, outputs.get(failureTag));
        }

        private static class RequestDoFn extends DoFn<Long, Map<String, Object>> {

            private static final String CLIENT_NAME = "";
            private static final Map<String, HttpClient> clients = new HashMap<>();

            private final String parametersText;
            private final boolean failFast;
            private final TupleTag<BadRecord> failuresTag;
            private final Map<String, Logging> loggings;

            private transient Parameters parameters;

            RequestDoFn(
                    final Parameters parameters,
                    final boolean failFast,
                    final TupleTag<BadRecord> failuresTag,
                    final Map<String, Logging> loggings) {

                this.parametersText = parameters.toJsonString();
                this.failFast = failFast;
                this.failuresTag = failuresTag;
                this.loggings = loggings;
            }

            @Setup
            public void setup() {
                parameters = new Gson().fromJson(parametersText, Parameters.class);
                parameters.setDefaults();
                parameters.setup();
                getOrCreateClient(clients, parameters.timeoutSecond);
            }

            @ProcessElement
            public void processElement(final ProcessContext c) {
                final Map<String, Object> output = new HashMap<>();
                try {
                    final HttpResponse.BodyHandler<?> bodyHandler = HttpUtil.getBodyHandler(parameters.format);
                    final Map<String, Object> params = new HashMap<>();
                    for(final Map.Entry<String, String> entry : parameters.args.entrySet()) {
                        String value = entry.getValue();
                        if(TemplateUtil.isTemplateText(value)) {
                            value = TemplateUtil.executeStrictTemplate(value, params);
                        }
                        params.put(entry.getKey(), value);
                    }
                    final HttpClient client = Optional
                            .ofNullable(clients.get(CLIENT_NAME))
                            .orElseGet(() -> getOrCreateClient(clients, parameters.timeoutSecond));

                    for(final HttpUtil.Request request : parameters.requests) {
                        final HttpResponse<?> httpResponse = HttpUtil.sendRequest(client, request, params, bodyHandler);
                        if(httpResponse.statusCode() >= 400 && httpResponse.statusCode() < 500) {
                            final String errorMessage = "Illegal response code: " + httpResponse.statusCode() + ", for endpoint: " + request.endpoint + ", response: " + httpResponse.body();
                            LOG.error(errorMessage);
                            throw new IllegalModuleException(errorMessage);
                        } else if(httpResponse.statusCode() >= 500) {

                        }

                        final Object body = switch (parameters.format) {
                            case text, xml -> httpResponse.body();
                            case bytes -> ByteBuffer.wrap((byte[])httpResponse.body());
                            case json -> {
                                final String bodyString = (String) httpResponse.body();
                                final JsonElement responseJson = new Gson().fromJson(bodyString, JsonElement.class);
                                yield JsonToMapConverter.convert(responseJson);
                            }
                        };

                        output.put("auth", body);

                        switch (body) {
                            case Map map -> params.putAll(map);
                            default -> params.put("body", body);
                        }
                    }

                    Logging.log(LOG, loggings, "auth", output.toString());
                    c.output(output);
                } catch (Throwable e) {
                    final Map<String, Object> values = new HashMap<>();
                    c.output(output);
                    c.output(failuresTag, FailureUtil.createBadRecord(values, "Failed to auth request", e));
                }
            }

            @Teardown
            public void teardown() {

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
