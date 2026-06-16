package com.mercari.solution.util.domain.web;

import com.google.gson.*;
import com.mercari.solution.module.*;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.domain.file.JsonUtil;
import com.mercari.solution.util.pipeline.*;
import com.mercari.solution.util.schema.converter.JsonToElementConverter;
import com.mercari.solution.util.schema.converter.JsonToMapConverter;
import freemarker.template.Template;
import org.apache.commons.lang3.math.NumberUtils;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class HttpUtil {

    private static final Logger LOG = LoggerFactory.getLogger(HttpUtil.class);

    public static class Request {

        private String name;
        public String endpoint;
        private String method;
        private Map<String, String> params;
        private Map<String, String> headers;
        private JsonElement body;

        public LoopParameters loop;

        public Format format;
        public JsonElement schema;
        private List<Integer> acceptableStatusCodes;

        private Input.Parameters input;
        private List<Preprocessor.Parameters> preprocessors;
        private String group;

        private transient Input input_;
        private transient List<Preprocessor> preprocessors_;

        private transient Template templateEndpoint;
        private transient Map<String, Template> templateParams;
        private transient Map<String, Template> templateHeaders;
        private transient Template templateBody;

        private transient Schema schema_;

        public String getName() {
            return name;
        }

        public String getGroup() {
            if(this.group == null) {
                final String domain = extractDomain(this.endpoint);
                return Objects.requireNonNullElse(domain, "");
            } else {
                return group;
            }
        }

        public List<String> validate() {
            final List<String> errorMessages = new ArrayList<>();

            if(this.endpoint == null) {
                errorMessages.add("requests[].endpoint must not be null.");
            }

            if(this.acceptableStatusCodes != null) {
                for(final Integer acceptableStatusCode : acceptableStatusCodes) {
                    if(acceptableStatusCode == null) {
                        errorMessages.add("requests[].acceptableStatusCodes value must not be null.");
                    } else if(acceptableStatusCode >= 600 || acceptableStatusCode < 100) {
                        errorMessages.add("requests[].acceptableStatusCodes value[" + acceptableStatusCode + "] must be between 100 and 599");
                    }
                }
            }

            return errorMessages;
        }

        public void setDefaults() {
            if(this.method == null) {
                this.method = "GET";
            }
            if(this.params == null) {
                this.params = new HashMap<>();
            }
            if(this.headers == null) {
                this.headers = new HashMap<>();
            }
            if(this.format == null) {
                this.format = Format.text;
            }
            if(this.group == null) {
                this.group = "";
            }
            if(preprocessors == null) {
                preprocessors = new ArrayList<>();
            }
        }

        public Request setup() {
            this.templateEndpoint = TemplateUtil.createStrictTemplate("TemplateEndpoint", endpoint);
            this.templateParams = new HashMap<>();
            if(!this.params.isEmpty()) {
                int count = 0;
                for(final Map.Entry<String, String> entry : params.entrySet()) {
                    final Template template = TemplateUtil.createStrictTemplate("TemplateParams" + count, entry.getValue());
                    this.templateParams.put(entry.getKey(), template);
                    count++;
                }
            }
            this.templateHeaders = new HashMap<>();
            if(!this.headers.isEmpty()) {
                int count = 0;
                for(final Map.Entry<String, String> entry : headers.entrySet()) {
                    final Template template = TemplateUtil.createStrictTemplate("TemplateHeaders" + count, entry.getValue());
                    this.templateHeaders.put(entry.getKey(), template);
                    count++;
                }
            }
            if(this.body != null) {
                this.templateBody = TemplateUtil.createStrictTemplate("TemplateBody", this.body.toString());
            }
            if(this.schema != null && this.schema.isJsonObject()) {
                this.schema_ = Schema.parse(schema.getAsJsonObject());
            }

            if(this.loop == null) {
                this.loop = new LoopParameters();
            }
            this.loop.setup();

            if(this.input != null) {
                this.input_ = input.create(schema());
                this.input_.setup(schema());
            }

            this.preprocessors_ = new ArrayList<>();
            for(final Preprocessor.Parameters parameters : preprocessors) {
                final Preprocessor preprocessor = parameters.create(Request.schema());
                this.preprocessors_.add(preprocessor.setup(Request.schema()));
            }

            return this;
        }

        public boolean isParamsBody() {
            return headers != null
                    && headers.containsKey("Content-Type")
                    && headers.containsValue("application/x-www-form-urlencoded");
        }

        public boolean hasNext(final Map<String, Object> values) {
            return loop.hasNext(values);
        }

        public Map<String, Object> vars() {
            return new HashMap<>(loop.vars_);
        }

        public Map<String, Object> feed(final Map<String, Object> values) {
            return loop.feed(values);
        }

        public List<Map<String, Object>> process(final MElement element) {
            final List<Map<String, Object>> results = new ArrayList<>();
            if(input_ != null) {
                results.addAll(input_.process(element));
            } else {
                results.add(element.asPrimitiveMap());
            }
            if(preprocessors.isEmpty()) {
                return results;
            }
            final List<Map<String, Object>> outputs = new ArrayList<>();
            for(final Map<String, Object> result : results) {
                outputs.addAll(preprocess(preprocessors_, result, element.getTimestamp()));
            }
            return outputs;
        }

        private List<Map<String, Object>> preprocess(
                List<Preprocessor> preprocessors,
                Map<String, Object> input,
                Instant timestamp) {

            if(preprocessors.size() == 1) {
                return preprocessors.getFirst().process(input, timestamp);
            }
            final List<Map<String, Object>> results = new ArrayList<>();
            for(final Map<String, Object> result : preprocessors.getFirst().process(input, timestamp)) {
                final List<Map<String, Object>> results_ = preprocess(
                        preprocessors.subList(1, preprocessors.size()), result, timestamp);
                results.addAll(results_);
            }
            return results;
        }

        public JsonObject toJson() {
            final JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty("name", name);
            jsonObject.addProperty("endpoint", endpoint);
            jsonObject.addProperty("method", method);
            if(params != null){
                final JsonObject paramsJsonObject = new JsonObject();
                for(final Map.Entry<String, String> entry : params.entrySet()) {
                    paramsJsonObject.addProperty(entry.getKey(), entry.getValue());
                }
                jsonObject.add("params", paramsJsonObject);
            }
            if(headers != null){
                final JsonObject headersJsonObject = new JsonObject();
                for(final Map.Entry<String, String> entry : headers.entrySet()) {
                    headersJsonObject.addProperty(entry.getKey(), entry.getValue());
                }
                jsonObject.add("headers", headersJsonObject);
            }
            if(body != null) {
                jsonObject.add("body", body);
            }
            if(format != null) {
                jsonObject.addProperty("format", format.name());
            }
            if(schema != null) {
                jsonObject.add("schema", schema);
            }
            if(loop != null) {
                jsonObject.add("loop", loop.toJson());
            }
            if(input != null) {
                jsonObject.add("input", input.toJson());
            }
            final JsonArray preprocessorsArray = new JsonArray();
            for(final Preprocessor.Parameters preprocessor : preprocessors) {
                preprocessorsArray.add(preprocessor.toJson());
            }
            jsonObject.add("preprocessors", preprocessorsArray);

            if(acceptableStatusCodes != null){
                final JsonArray acceptableStatusCodesArray = new JsonArray();
                for(final Integer statusCode : acceptableStatusCodes) {
                    acceptableStatusCodesArray.add(statusCode);
                }
                jsonObject.add("acceptableStatusCodes", acceptableStatusCodesArray);
            }

            return jsonObject;
        }

        public String toJsonString() {
            final JsonObject jsonObject = toJson();
            return jsonObject.toString();
        }

        public Map<String, Object> convertJsonToMap(String jsonText) {
            if(schema_ != null) {
                return JsonToElementConverter.convert(schema_.getFields(), jsonText);
            }
            return null;
        }

        public static List<String> toJsonStringList(List<Request> requests) {
            return requests.stream().map(Request::toJsonString).toList();
        }

        public static Request fromJsonString(final String jsonString) {
            return JsonUtil.fromJson(jsonString, Request.class);
        }

        public static Request copy(final Request request) {
            return fromJsonString(request.toJsonString());
        }

        public static Schema schema() {
            return Schema.builder()
                    .withField("name", Schema.FieldType.STRING)
                    .withField("endpoint", Schema.FieldType.STRING)
                    .withField("method", Schema.FieldType.STRING)
                    .withField("statusCode", Schema.FieldType.INT32)
                    .withField("headers", Schema.FieldType.map(Schema.FieldType.array(Schema.FieldType.STRING)).withNullable(true))
                    .withField("body", Schema.FieldType.STRING)
                    .withField("blob", Schema.FieldType.BYTES)
                    .withField("durationMs", Schema.FieldType.INT64)
                    .withField("timestamp", Schema.FieldType.TIMESTAMP)
                    .withType(DataType.AVRO)
                    .build();
        }

        public static Map<String, Set<String>> resolveDependencies(final List<Request> requests) {
            final Map<String, Set<String>> dependencies = new HashMap<>();
            if(requests == null) {
                return dependencies;
            }
            for(final Request request : requests) {
                if(request.name == null) {
                    continue;
                }
                if(request.input == null) {
                    dependencies.computeIfAbsent("", k -> new HashSet<>()).add(request.name);
                } else {
                    dependencies.computeIfAbsent(request.input.getName(), k -> new HashSet<>()).add(request.name);
                }
            }
            return dependencies;
        }

        @Override
        public String toString() {
            return toJsonString();
        }
    }

    public static class LoopParameters implements Serializable {

        private JsonElement condition;
        public Map<String, JsonElement> vars;
        public LinkedHashMap<String, String> feeds;

        private transient Filter condition_;
        private transient Map<String, Object> vars_;

        private transient boolean called;

        public void setup() {
            if(vars == null) {
                vars = new HashMap<>();
            }
            if(feeds == null) {
                feeds = new LinkedHashMap<>();
            }

            condition_ = Filter.of(condition);
            condition_.setup();
            vars_ = new HashMap<>();
            for(final Map.Entry<String, JsonElement> entry : vars.entrySet()) {
                if(!entry.getValue().isJsonPrimitive()) {
                    continue;
                }
                final JsonPrimitive jsonPrimitive = entry.getValue().getAsJsonPrimitive();
                if(jsonPrimitive.isNumber()) {
                    vars_.put(entry.getKey(), jsonPrimitive.getAsLong());
                } else if(jsonPrimitive.isString()) {
                    vars_.put(entry.getKey(), jsonPrimitive.getAsString());
                }
            }
            called = false;
        }

        private boolean hasNext(final Map<String, Object> values) {
            if(condition == null || condition.isJsonNull()) {
                return false;
            }
            return condition_.filter(values);
        }

        private Map<String, Object> feed(Map<String, Object> values) {
            if(!called) {
                called = true;
                return new HashMap<>(vars_);
            }
            final Map<String, Object> result = new HashMap<>();
            final Map<String, Object> values_ = new HashMap<>(values);
            values_.putAll(vars_);
            for(Map.Entry<String, String> entry : feeds.entrySet()) {
                final String value = TemplateUtil.executeStrictTemplate(entry.getValue(), values_);
                result.put(entry.getKey(), value);
                if(NumberUtils.isCreatable(value)) {
                    if(NumberUtils.isDigits(value)) {
                        vars_.put(entry.getKey(), Long.parseLong(value));
                    } else {
                        vars_.put(entry.getKey(), Double.parseDouble(value));
                    }
                } else {
                    vars_.put(entry.getKey(), value);
                }
            }
            return result;
        }

        public JsonObject toJson() {
            final JsonObject jsonObject = new JsonObject();
            jsonObject.add("condition", condition);

            final JsonObject varsJsonObject = new JsonObject();
            if(vars != null) {
                for(final Map.Entry<String, JsonElement> entry : vars.entrySet()) {
                    varsJsonObject.add(entry.getKey(), entry.getValue());
                }
            }
            jsonObject.add("vars", varsJsonObject);

            final JsonObject feedsJsonObject = new JsonObject();
            if(vars != null) {
                for(final Map.Entry<String, String> entry : feeds.entrySet()) {
                    feedsJsonObject.addProperty(entry.getKey(), entry.getValue());
                }
            }
            jsonObject.add("feeds", feedsJsonObject);
            return jsonObject;
        }
    }

    public static class RetryParameters implements Serializable {

        private BackoffParameters backoff;

        public List<String> validate() {
            return new ArrayList<>();
        }

        public void setDefaults() {
            if(backoff == null) {
                backoff = new BackoffParameters();
            }
            backoff.setDefaults();
        }

    }

    public static class BackoffParameters implements Serializable {

        private Double exponent;
        private Integer initialBackoffSecond;
        private Integer maxBackoffSecond;
        private Integer maxCumulativeBackoffSecond;
        private Integer maxRetries;

        public List<String> validate() {
            return new ArrayList<>();
        }

        public void setDefaults() {
            // reference: FluentBackoff.DEFAULT
            if(exponent == null) {
                exponent = 1.5;
            }
            if(initialBackoffSecond == null) {
                initialBackoffSecond = 1;
            }
            if(maxBackoffSecond == null) {
                maxBackoffSecond = 60 * 60 * 24 * 1000;
            }
            if(maxCumulativeBackoffSecond == null) {
                maxCumulativeBackoffSecond = 60 * 60 * 24 * 1000;
            }
            if(maxRetries == null) {
                this.maxRetries = Integer.MAX_VALUE;
            }
        }

        public JsonObject toJson() {
            final JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty("exponent", exponent);
            jsonObject.addProperty("initialBackoffSecond", initialBackoffSecond);
            jsonObject.addProperty("maxBackoffSecond", maxBackoffSecond);
            jsonObject.addProperty("maxCumulativeBackoffSecond", maxCumulativeBackoffSecond);
            jsonObject.addProperty("maxRetries", maxRetries);
            return jsonObject;
        }
    }

    public static class Preprocessor extends Processor {

        public static class Parameters extends Processor.Parameters {

            protected Request request;

            public List<String> validate() {
                final List<String> errorMessages = super.validate();
                return errorMessages;
            }

            public Parameters setDefaults() {
                super.setDefaults();
                return this;
            }

            public JsonObject toJson() {
                final JsonObject jsonObject = super.toJson();
                if(request != null) {
                    jsonObject.add("request", request.toJson());
                }
                return jsonObject;
            }

            public Preprocessor create(Schema inputSchema) {
                final Processor processor = super.create(inputSchema);
                return new Preprocessor(processor, request);
            }
        }

        protected Request request;

        Preprocessor(Processor processor, Request request) {
            super(processor);
            this.request = request;
        }

        public Preprocessor setup(Schema inputSchema) {
            super.setup(inputSchema);
            if(request != null) {
                request.setup();
            }
            return this;
        }

        public List<Map<String, Object>> process(
                final HttpClient client,
                final Map<String, Object> values,
                final Instant timestamp) throws IOException, InterruptedException {

            final List<Map<String, Object>> results;
            if(request != null) {
                results = HttpUtil.execute(client, request, values);
            } else {
                results = Collections.singletonList(values);
            }

            final List<Map<String, Object>> outputs = new ArrayList<>();
            for(final Map<String, Object> result : results) {
                outputs.addAll(super.process(result, timestamp));
            }
            return outputs;
        }

    }

    public static class RateParameters {

        public String unit;
        public Integer count;

        private RateParameters setDefaults() {
            if(unit == null) {
                unit = "1s";
            }
            if(count == null) {
                count = 0;
            }
            return this;
        }
    }

    public static class Response2 {

        public String name;

        // request fields
        public String endpoint;
        public String method;

        // response fields
        public int statusCode;
        public String body;
        public ByteBuffer blob;
        public Map<String, List<String>> headers;

        public Long durationMs;
        public Instant timestamp;

        public Map<String, Object> toMap(final Format format) {
            final Map<String, Object> values = new HashMap<>();
            values.put("name", name);

            values.put("endpoint", endpoint);
            values.put("method", method);

            values.put("statusCode", statusCode);
            values.put("headers", headers);
            if(body != null) {
                values.put("body", body);
            }
            if(blob != null) {
                values.put("blob", blob);
            }
            values.put("response", getContent(format));

            values.put("durationMs", durationMs);
            values.put("timestamp", timestamp.getMillis() * 1000L);

            return values;
        }

        public Object getContent(final Format format) {
            System.out.println(body);
            return switch (format) {
                case text -> body;
                case bytes -> blob;
                case json -> JsonToMapConverter.convert(body);
                case xml -> JsonToMapConverter.convert(body);
            };
        }

        public static Response2 create(final HttpResponse<?> httpResponse) {
            final Response2 response = new Response2();
            response.endpoint = httpResponse.request().uri().toString();
            response.method = httpResponse.request().method();
            switch (httpResponse.body()) {
                case String string -> response.body = string;
                case byte[] bytes -> response.blob = ByteBuffer.wrap(bytes);
                case ByteBuffer byteBuffer -> response.blob = byteBuffer;
                case null, default -> {}
            }
            response.statusCode = httpResponse.statusCode();
            response.headers = httpResponse.headers().map();
            response.timestamp = Instant.now();

            return response;
        }
    }

    public static class Response {

        public Format format;
        public JsonElement schema;
        public List<Integer> acceptableStatusCodes;

        private transient Schema schema_;

        public HttpResponse.BodyHandler<?> getBodyHandler() {
            return switch (format) {
                case text, json, xml -> HttpResponse.BodyHandlers.ofString();
                case bytes -> HttpResponse.BodyHandlers.ofByteArray();
            };
        }

        public List<String> validate(String name) {
            final List<String> errorMessages = new ArrayList<>();
            if(this.format == null) {
                errorMessages.add("http transform module[" + name + "].format must not be null.");
            }
            if(this.schema == null || !this.schema.isJsonObject()) {
                errorMessages.add("http transform module[" + name + "].schema must not be empty.");
            }
            if(this.acceptableStatusCodes != null) {
                for(final Integer acceptableStatusCode : acceptableStatusCodes) {
                    if(acceptableStatusCode == null) {
                        errorMessages.add("http transform module[" + name + "].acceptableStatusCodes value must not be null.");
                    } else if(acceptableStatusCode >= 600 || acceptableStatusCode < 100) {
                        errorMessages.add("http transform module[" + name + "].acceptableStatusCodes value[" + acceptableStatusCode + "] must be between 100 and 599");
                    }
                }
            }
            return errorMessages;
        }

        public void setDefaults() {
            if(this.acceptableStatusCodes == null) {
                this.acceptableStatusCodes = new ArrayList<>();
            }
        }

        public void setup() {
            if(schema != null && schema.isJsonObject()) {
                this.schema_ = Schema.parse(schema);
            }
        }

        public JsonObject toJson() {
            final JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty("format", format.name());
            if(acceptableStatusCodes != null){
                final JsonArray acceptableStatusCodesArray = new JsonArray();
                for(final Integer acceptableStatusCode : acceptableStatusCodes) {
                    acceptableStatusCodesArray.add(acceptableStatusCode);
                }
                jsonObject.add("acceptableStatusCodes", acceptableStatusCodesArray);
            }
            if(schema != null){
                jsonObject.add("schema", schema);
            }
            return jsonObject;
        }

        public String toJsonString() {
            final JsonObject jsonObject = toJson();
            return jsonObject.toString();
        }

    }

    public enum Format {
        text,
        bytes,
        json,
        xml
    }

    public enum Type {
        custom,
        oauth,
        openid
    }

    public static HttpResponse.BodyHandler<?> getBodyHandler(final Format format) {
        return switch (format) {
            case text, json, xml -> HttpResponse.BodyHandlers.ofString();
            case bytes -> HttpResponse.BodyHandlers.ofByteArray();
        };
    }

    public static Schema createResponseSchema(final Response response) {
        return createResponseSchema(response.schema, response.format);
    }

    public static Schema createResponseSchema(final JsonElement schema, final Format format) {
        final Schema.FieldType fieldType;
        if(schema == null || !schema.isJsonObject()) {
            fieldType = switch (format) {
                case bytes -> Schema.FieldType.BYTES.withNullable(true);
                case text, xml -> Schema.FieldType.STRING.withNullable(true);
                case json -> Schema.FieldType.JSON.withNullable(true);
            };
        } else {
            final Schema responseSchema = Schema.parse(schema.getAsJsonObject());
            fieldType = Schema.FieldType.element(responseSchema);
        }

        return Schema.builder()
                .withField("statusCode", Schema.FieldType.INT32)
                .withField("body", fieldType)
                .withField("headers", Schema.FieldType.map(Schema.FieldType.array(Schema.FieldType.STRING)).withNullable(true))
                .withField("timestamp", Schema.FieldType.TIMESTAMP)
                .build();
    }

    public static Response2 sendRequest(
            final HttpClient client,
            final Request request,
            final Map<String, Object> standardValues) throws IOException, InterruptedException {

        final HttpResponse.BodyHandler<?> bodyHandler = getBodyHandler(request.format);
        final HttpRequest httpRequest = createRequestBuilder(request, standardValues)
                .timeout(java.time.Duration.ofSeconds(100))
                .build();
        final Instant startTime = Instant.now();
        final HttpResponse<?> httpResponse = client.send(httpRequest, bodyHandler);
        final Instant endTime = Instant.now();
        final Response2 response = Response2.create(httpResponse);
        response.name = request.name;
        response.timestamp = endTime;
        response.durationMs = endTime.getMillis() - startTime.getMillis();

        return response;
    }

    public static List<Map<String, Object>> execute(final HttpClient client, final Request request)
            throws IOException, InterruptedException {
        return execute(client, request, new HashMap<>());
    }

    public static List<Map<String, Object>> execute(
            final HttpClient client,
            final Request request,
            final Map<String, Object> values) throws IOException, InterruptedException {

        final List<Map<String, Object>> results = new ArrayList<>();
        final Map<String, Object> loopValues = new HashMap<>(values);
        do {
            final Map<String, Object> state = request.feed(loopValues);
            loopValues.putAll(state);
            final HttpUtil.Response2 response = sendRequest(client, request, loopValues);
            final Map<String, Object> responseValues = response.toMap(request.format);
            loopValues.putAll(responseValues);
            results.add(responseValues);
        } while (request.hasNext(loopValues));

        return results;
    }

    public static <ResponseT> HttpResponse<ResponseT> sendRequest(
            final HttpClient client,
            final Request request,
            final Map<String, Object> standardValues,
            final HttpResponse.BodyHandler<ResponseT> bodyHandler) throws IOException, InterruptedException, URISyntaxException {

        final HttpRequest req = createRequestBuilder(request, standardValues)
                .timeout(java.time.Duration.ofSeconds(100))
                .build();
        return client.send(req, bodyHandler);
    }

    private static HttpRequest.Builder createRequestBuilder(
            final Request request,
            final Map<String, Object> standardValues) {

        final String url;
        final String bodyText;
        if(request.isParamsBody()) {
            url = createEndpoint(request, standardValues);
            bodyText = createUrlParams(request, standardValues);
        } else if(request.templateBody != null) {
            url = createEndpointWithParams(request, standardValues);
            bodyText = TemplateUtil.executeStrictTemplate(request.templateBody, standardValues);
        } else {
            url = createEndpointWithParams(request, standardValues);
            bodyText = "";
        }
        LOG.info("request url: {} body: {}", url, bodyText);

        final HttpRequest.BodyPublisher bodyPublisher = HttpRequest.BodyPublishers.ofString(bodyText);

        try {
            HttpRequest.Builder builder = HttpRequest.newBuilder()
                    .uri(new URI(url))
                    .method(request.method.toUpperCase(), bodyPublisher);
            for(final Map.Entry<String, Template> entry : request.templateHeaders.entrySet()) {
                final String headerValue = TemplateUtil.executeStrictTemplate(entry.getValue(), standardValues);
                builder = builder.header(entry.getKey(), headerValue);
            }
            return builder;
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static String createEndpoint(
            final Request request,
            final Map<?, ?> standardValues) {

        return TemplateUtil.executeStrictTemplate(request.templateEndpoint, standardValues);
    }

    private static String createUrlParams(final Request request, Map<?, ?> standardValues) {
        return request.templateParams.entrySet()
                .stream()
                .filter(e -> e.getValue() != null)
                .map(e -> e.getKey() + "=" + URLEncoder
                        .encode(TemplateUtil
                                .executeStrictTemplate(e.getValue(), standardValues), StandardCharsets.UTF_8))
                .collect(Collectors.joining("&"));
    }

    private static String createEndpointWithParams(
            final Request request,
            final Map<?, ?> standardValues) {

        final String params = createUrlParams(request, standardValues);
        return createEndpoint(request, standardValues) + (params.isEmpty() ? "" : ("?" + params));
    }

    public static String extractDomain(String url) {
        if (url == null || !url.contains("://")) {
            return null;
        }
        int startIndex = url.indexOf("://") + 3;
        int slashIndex = url.indexOf('/', startIndex);
        int colonIndex = url.indexOf(':', startIndex);

        int endIndex;
        if (slashIndex == -1 && colonIndex == -1) {
            endIndex = url.length();
        } else if (slashIndex != -1 && colonIndex != -1) {
            endIndex = Math.min(slashIndex, colonIndex);
        } else {
            endIndex = Math.max(slashIndex, colonIndex);
        }
        return url.substring(startIndex, endIndex);
    }

}
