package com.mercari.solution.module.transform;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.*;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.domain.web.HttpUtil;
import com.mercari.solution.util.pipeline.*;
import com.mercari.solution.util.pipeline.Filter;
import com.mercari.solution.util.pipeline.select.SelectFunction;
import com.mercari.solution.util.schema.converter.JsonToMapConverter;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;


@Transform.Module(name="http")
public class HttpTransform extends Transform {

    private static final Logger LOG = LoggerFactory.getLogger(HttpTransform.class);

    public static class Parameters {

        private Auth.Parameters auth;
        private HttpUtil.Request request;
        private List<HttpUtil.Request> requests;
        private HttpUtil.Response response;
        private HttpUtil.RetryParameters retry;
        private Integer timeoutSecond;

        private JsonArray select;
        private JsonElement filter;
        private String flattenField;

        public void validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(auth != null) {
                errorMessages.addAll(auth.validate());
            }
            if(this.request == null && (this.requests == null || this.requests.isEmpty())) {
                errorMessages.add("parameters.request must not be null.");
            } else if(this.request != null) {
                errorMessages.addAll(this.request.validate());
            } else {
                for(var req : requests) {
                    req.validate();
                }
            }

            if(this.response == null) {
                errorMessages.add("parameters.response must not be null.");
            } else {
                errorMessages.addAll(this.response.validate(""));
            }

            if(this.retry != null) {
                errorMessages.addAll(this.retry.validate());
            }

            if(this.auth != null) {
                errorMessages.addAll(auth.validate());
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        public void setDefaults() {
            if(auth != null) {
                auth.setDefaults();
            }
            if(requests == null) {
                requests = new ArrayList<>();
            }
            if(request != null) {
                requests.add(request);
            }
            for(var req : requests) {
                req.setDefaults();
            }
            this.response.setDefaults();
            if(this.retry == null) {
                this.retry = new HttpUtil.RetryParameters();
            }
            this.retry.setDefaults();
            if(this.timeoutSecond == null) {
                this.timeoutSecond = 30;
            }
        }

        public String toJsonString() {
            final JsonArray requestArray = new JsonArray();
            for(final HttpUtil.Request request : requests) {
                final JsonObject requestJson = request.toJson();
                requestArray.add(requestJson);
            }
            final JsonObject jsonObject = new JsonObject();
            jsonObject.add("requests", requestArray);
            jsonObject.add("response", response.toJson());
            if(auth != null) {
                jsonObject.add("auth", auth.toJson());
            }
            if(timeoutSecond != null) {
                jsonObject.addProperty("timeoutSecond", timeoutSecond);
            }
            return jsonObject.toString();
        }

    }

    @Override
    public MCollectionTuple expand(
            final MCollectionTuple inputs,
            final MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate();
        parameters.setDefaults();

        for(final HttpUtil.Request request : parameters.requests) {
            request.setup();
        }

        final Schema responseSchema = HttpUtil.createResponseSchema(parameters.response);
        final List<SelectFunction> selectFunctions = SelectFunction.of(parameters.select, responseSchema.getFields());
        final Schema outputSchema = createOutputSchema(responseSchema, selectFunctions, parameters.flattenField);

        final PCollection<MElement> input = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));
        final Schema inputSchema = Union.createUnionSchema(inputs);

        final Map<String, PCollectionView<?>> views = new HashMap<>();
        if(parameters.auth != null || !getSideInputs().isEmpty()) {
            for(final MCollection collection : getSideInputs()) {
                final PCollectionView<Map<String,Object>> sideInputView = collection
                        .apply("AsView" + collection.getName(), Views.singleton());
                views.put(collection.getName(), sideInputView);
            }

            if(parameters.auth != null) {
                final Auth.Transform transform = Auth.of(
                        parameters.auth, getFailFast(), getLoggings());
                final PCollectionTuple auth = inputs.getPipeline()
                        .begin()
                        .apply("Auth", transform);

                final PCollection<Map<String, Object>> authOutput = auth
                        .get(transform.outputTag)
                        .apply("WithTrigger", Window
                                .<Map<String, Object>>into(new GlobalWindows())
                                .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()))
                                .discardingFiredPanes());

                if(errorHandler != null) {
                    errorHandler.addError(auth.get(transform.failureTag));
                }

                final PCollectionView<Map<String, Object>> params = authOutput
                        .apply(View
                                .<Map<String, Object>>asSingleton()
                                .withDefaultValue(new HashMap<>()));
                views.put("auth", params);
            }
        }

        final TupleTag<MElement> outputTag = new TupleTag<>() {};
        final TupleTag<BadRecord> failureTag = new TupleTag<>() {};

        ParDo.MultiOutput<MElement,MElement> pardo = ParDo
                .of(new HttpDoFn(parameters, inputSchema, outputSchema, selectFunctions,
                        views, getLoggings(), getFailFast(), failureTag))
                .withOutputTags(outputTag, TupleTagList.of(failureTag));
        if(!views.isEmpty()) {
            pardo = pardo.withSideInputs(views);
        }
        final PCollectionTuple outputs = input.apply("HttpCall", pardo);

        if(errorHandler != null) {
            errorHandler.addError(outputs.get(failureTag));
        }

        return MCollectionTuple
                .of(outputs.get(outputTag), outputSchema);
    }

    private static Schema createOutputSchema(
            final Schema responseSchema,
            final List<SelectFunction> selectFunctions,
            final String flattenField) {

        if(selectFunctions.isEmpty()) {
            return responseSchema;
        } else {
            return SelectFunction.createSchema(selectFunctions, flattenField);
        }
    }

    private static class HttpDoFn extends DoFn<MElement, MElement> {

        private static final String CLIENT_NAME = "";
        private static final Map<String, HttpClient> clients = new HashMap<>();

        private String parametersJson;
        private HttpUtil.RetryParameters retry;
        private Integer timeoutSecond;

        private final Schema inputSchema;
        private final Schema outputSchema;

        private Select select;
        private Filter filter;
        private Unnest unnest;

        private final Map<String, PCollectionView<?>> views;

        private final Map<String, Logging> logging;
        private final boolean failFast;
        private final TupleTag<BadRecord> failuresTag;

        private transient List<HttpUtil.Request> requests;
        private transient HttpUtil.Response response;

        HttpDoFn(
                final Parameters parameters,
                final Schema inputSchema,
                final Schema outputSchema,
                final List<SelectFunction> selectFunctions,
                final Map<String, PCollectionView<?>> views,
                final List<Logging> logging,
                final boolean failFast,
                final TupleTag<BadRecord> failuresTag) {

            this.parametersJson = parameters.toJsonString();
            this.requests = parameters.requests;
            this.response = parameters.response;
            this.retry = parameters.retry;
            this.timeoutSecond = parameters.timeoutSecond;
            this.inputSchema = inputSchema;
            this.outputSchema = outputSchema;

            this.select = Select.of(selectFunctions);
            this.filter = Filter.of(parameters.filter);
            this.unnest = Unnest.of(parameters.flattenField);

            this.views = views;

            this.logging = Logging.map(logging);

            this.failFast = failFast;
            this.failuresTag = failuresTag;
        }

        @Setup
        public void setup() {
            final Parameters parameters = new Gson().fromJson(parametersJson, Parameters.class);
            parameters.setDefaults();
            this.requests = parameters.requests;
            this.response = parameters.response;

            for(final HttpUtil.Request request : requests) {
                request.setup();
            }
            this.response.setup();

            this.select.setup();
            this.filter.setup();
            this.unnest.setup();

            getOrCreateClient(clients, timeoutSecond);
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
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

                final Map<String, Object> inputValues = input.asStandardMap(inputSchema);
                final Map<String, Object> sideInputsValues = getSideInputs(c);
                inputValues.putAll(sideInputsValues);

                final Map<String, Object> outputValues = sendRequests(inputValues, response.format);

                final List<MElement> outputs = new ArrayList<>();
                if (!select.useSelect() && !unnest.useUnnest()) {
                    final MElement output = MElement.of(outputValues, c.timestamp());
                    outputs.add(output);
                } else if(select.useSelect()) {
                    final Map<String, Object> primitiveValues = select.select(outputValues, c.timestamp());
                    if(unnest.useUnnest()) {
                        final List<Map<String, Object>> list = unnest.unnest(primitiveValues);
                        outputs.addAll(MElement.ofList(list, c.timestamp()));
                    } else {
                        final MElement output = MElement.of(primitiveValues, c.timestamp());
                        outputs.add(output);
                    }
                } else {
                    final List<Map<String, Object>> list = unnest.unnest(outputValues);
                    outputs.addAll(MElement.ofList(list, c.timestamp()));
                }

                for(final MElement output : outputs) {
                    Logging.log(LOG, logging, "response", output);
                    final MElement output_ = output.convert(outputSchema);
                    c.output(output_);
                    Logging.log(LOG, logging, "output", output_);
                }
            } catch (final Throwable e) {
                final BadRecord badRecord = processError("Failed to process http module", input, e, failFast);
                c.output(failuresTag, badRecord);
            }
        }

        @Teardown
        public void teardown() {

        }

        private Map<String, Object> getSideInputs(ProcessContext c) {
            final Map<String, Object> sideInputsValues = new HashMap<>();
            for(final PCollectionView<?> view : views.values()) {
                final Map<String, Object> sideInput = (Map<String, Object>) c.sideInput(view);
                if(sideInput != null) {
                    sideInputsValues.putAll(sideInput);
                }
            }
            return sideInputsValues;
        }

        public Map<String, Object> sendRequests(
                final Map<String, Object> input,
                final HttpUtil.Format format) {

            final HttpClient client = Optional
                    .ofNullable(clients.get(CLIENT_NAME))
                    .orElseGet(() -> getOrCreateClient(clients, timeoutSecond));

            final HttpResponse.BodyHandler<?> bodyHandler = HttpUtil.getBodyHandler(format);

            final Map<String, Object> output = new HashMap<>();
            final Map<String, Object> params = new HashMap<>(input);
            params.put("_", new HashMap<String, Object>());
            for(final HttpUtil.Request request : requests) {
                HttpResponse<?> httpResponse = null;
                try {
                    httpResponse = HttpUtil.sendRequest(client, request, params, bodyHandler);
                    if (httpResponse.statusCode() >= 400 && httpResponse.statusCode() < 500) {
                        final String errorMessage = "Illegal response code: " + httpResponse.statusCode() + ", for endpoint: " + request.endpoint + ", response: " + httpResponse.body();
                        LOG.error(errorMessage);
                        throw new IllegalModuleException(errorMessage);
                    }
                } catch (final Throwable e) {
                    LOG.error("error: {}", e.getMessage());
                }

                if(httpResponse == null) {
                    throw new IllegalArgumentException("httpResponse is null");
                }

                output.put("statusCode", httpResponse.statusCode());
                output.put("headers", httpResponse.headers().map());
                output.put("timestamp", DateTimeUtil.toEpochMicroSecond(java.time.Instant.now()));

                final Object body = switch (format) {
                    case text -> httpResponse.body();
                    case bytes -> ByteBuffer.wrap((byte[])httpResponse.body());
                    case json -> {
                        final String bodyString = (String) httpResponse.body();
                        yield Optional
                                .ofNullable(request.convertJsonToMap(bodyString))
                                .orElseGet(() -> JsonToMapConverter.convert(bodyString));
                    }
                };
                output.put("body", body);

                ((Map<String,Object>)params.get("_")).put(Optional.ofNullable(request.getName()).orElse("body"), body);
            }

            return output;
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
