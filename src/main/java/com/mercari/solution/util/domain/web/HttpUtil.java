package com.mercari.solution.util.domain.web;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.schema.converter.JsonToElementConverter;
import freemarker.template.Template;
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

        private Format format;
        private JsonElement schema;
        private List<Integer> acceptableStatusCodes;

        private transient Template templateEndpoint;
        private transient Map<String, Template> templateParams;
        private transient Map<String, Template> templateHeaders;
        private transient Template templateBody;

        private transient Schema schema_;

        public String getName() {
            return name;
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
        }

        public void setup() {
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
        }

        public boolean isParamsBody() {
            return headers != null
                    && headers.containsKey("Content-Type")
                    && headers.containsValue("application/x-www-form-urlencoded");
        }

        public JsonObject toJson() {
            final JsonObject jsonObject = new JsonObject();
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

            if(acceptableStatusCodes != null){
                final JsonArray acceptableStatusCodesArray = new JsonArray();
                for(final Integer statusCode : acceptableStatusCodes) {
                    acceptableStatusCodesArray.add(statusCode);
                }
                jsonObject.add("acceptableStatusCodes", acceptableStatusCodesArray);
            }

            return jsonObject;
        }

        public Map<String, Object> convertJsonToMap(String jsonText) {
            if(schema_ != null) {
                return JsonToElementConverter.convert(schema_.getFields(), jsonText);
            }
            return null;
        }

        public String toJsonString() {
            final JsonObject jsonObject = toJson();
            return jsonObject.toString();
        }

        public static Request fromJsonString(final String jsonString) {
            return new Gson().fromJson(jsonString, Request.class);
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

    }

    public static class Response {

        public Format format;
        public JsonElement schema;
        public List<Integer> acceptableStatusCodes;

        private transient Schema schema_;

        public HttpResponse.BodyHandler<?> getBodyHandler() {
            return switch (format) {
                case text, json -> HttpResponse.BodyHandlers.ofString();
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

        public static Response fromJsonString(final String jsonString) {
            return new Gson().fromJson(jsonString, Response.class);
        }

    }

    public enum Format {
        text,
        bytes,
        json
    }

    public enum Type {
        custom,
        oauth,
        openid
    }

    public static HttpResponse.BodyHandler<?> getBodyHandler(final Format format) {
        return switch (format) {
            case text, json -> HttpResponse.BodyHandlers.ofString();
            case bytes -> HttpResponse.BodyHandlers.ofByteArray();
        };
    }

    /*
    public static Schema createResponseSchema(final Schema inputSchema, final List<Request> requests) {
        return null;
    }

     */

    public static Schema createResponseSchema(final Response response) {
        return createResponseSchema(response.schema, response.format);
    }

    public static Schema createResponseSchema(final JsonElement schema, final Format format) {
        final Schema.FieldType fieldType;
        if(schema == null || !schema.isJsonObject()) {
            fieldType = switch (format) {
                case bytes -> Schema.FieldType.BYTES.withNullable(true);
                case text -> Schema.FieldType.STRING.withNullable(true);
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

    /*
    public static <ResponseT> Map<String, Object> send(
            final HttpClient client,
            final Request request,
            final Map<String, Object> standardValues,
            final HttpResponse.BodyHandler<ResponseT> bodyHandler) throws IOException, InterruptedException, URISyntaxException {

        final HttpResponse<ResponseT> response = sendRequest(client, request, standardValues, bodyHandler);
    }

     */

    public static <ResponseT> HttpResponse<ResponseT> sendRequest(
            final HttpClient client,
            final Request request,
            final Map<String, Object> standardValues,
            final HttpResponse.BodyHandler<ResponseT> bodyHandler) throws IOException, InterruptedException, URISyntaxException {

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

        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(new URI(url))
                .timeout(java.time.Duration.ofSeconds(100))
                .method(request.method.toUpperCase(), bodyPublisher);

        for(final Map.Entry<String, Template> entry : request.templateHeaders.entrySet()) {
            final String headerValue = TemplateUtil.executeStrictTemplate(entry.getValue(), standardValues);
            builder = builder.header(entry.getKey(), headerValue);
        }

        return client.send(builder.build(), bodyHandler);
    }

    private static String createEndpoint(
            final Request request,
            final Map<?, ?> standardValues) {

        return TemplateUtil.executeStrictTemplate(request.templateEndpoint, standardValues);
    }

    private static String createUrlParams(final Request request, Map<?, ?> standardValues) {
        return request.templateParams.entrySet()
                .stream()
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

}
