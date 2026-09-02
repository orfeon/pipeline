package com.mercari.solution.util.pipeline.select;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.outbound.*;
import com.mercari.solution.util.schema.converter.JsonToElementConverter;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Select function that calls an HTTP endpoint once per record and returns the response as the
 * field value: text (default), bytes, or a typed struct when {@code response.schema} is set.
 *
 * <p>Uses the same {@code target} / {@code body} / {@code response} blocks as the http source /
 * sink ({@link RequestSpec}, {@link ResponsePolicy}, {@link AuthProvider}) with retry via
 * {@link SyncCaller}. A 404 response yields {@code null}; other failures throw (the select
 * module routes the record to failures). Intended for light per-record enrichment — for keyed
 * joins with batching and caching prefer the query transform's rest lookup source.
 */
public class Http implements SelectFunction {

    private static final Logger LOG = LoggerFactory.getLogger(Http.class);

    public static class Parameters implements Serializable {
        public RequestSpec.Target target;
        public RequestSpec.Body body;
        public ResponsePolicy.Parameters response;
        public HttpTransport.TimeoutParameters timeout;
        public HttpTransport.Parameters http;
    }

    private final String name;
    private final Parameters parameters;
    private final List<Schema.Field> inputFields;
    private final Schema inputSchema;
    private final Schema.FieldType outputFieldType;
    private final boolean ignore;
    private final RequestRenderer renderer;
    private final ResponsePolicy policy;

    private transient HttpTransport transport;

    private Http(String name, Parameters parameters, List<Schema.Field> inputFields, boolean ignore) {
        this.name = name;
        this.parameters = parameters;
        this.inputFields = inputFields;
        this.inputSchema = Schema.of(inputFields);
        this.ignore = ignore;
        this.policy = new ResponsePolicy(parameters.response);
        this.outputFieldType = policy.schema() != null
                ? Schema.FieldType.element(policy.schema()).withNullable(true)
                : ResponsePolicy.Format.bytes.equals(parameters.response.format)
                        ? Schema.FieldType.BYTES.withNullable(true)
                        : Schema.FieldType.STRING.withNullable(true);
        this.renderer = new RequestRenderer("select." + name, parameters.target, parameters.body,
                null, false, inputSchema, inputSchema, List.of());
    }

    public static Http of(String name, JsonObject jsonObject, List<Schema.Field> inputFields, boolean ignore) {
        final Parameters parameters = new Gson().fromJson(jsonObject, Parameters.class);
        final List<String> errorMessages = new ArrayList<>();
        final String prefix = "SelectField http: " + name;
        final Schema inputSchema = Schema.of(inputFields);
        if(parameters.target == null) {
            errorMessages.add(prefix + " requires target");
        } else {
            errorMessages.addAll(parameters.target.validate(prefix + ".target", inputSchema,
                    parameters.http != null && parameters.http.allowedHosts != null));
        }
        if(parameters.body != null) {
            errorMessages.addAll(parameters.body.validate(prefix + ".body", inputSchema));
        }
        if(parameters.response != null) {
            errorMessages.addAll(parameters.response.validate(prefix + ".response"));
            if(parameters.response.partialFailure != null) {
                errorMessages.add(prefix + ".response.partialFailure is not supported");
            }
        }
        if(parameters.timeout != null) {
            errorMessages.addAll(parameters.timeout.validate(prefix + ".timeout"));
        }
        if(parameters.http != null) {
            errorMessages.addAll(parameters.http.validate(prefix + ".http"));
        }
        if(!errorMessages.isEmpty()) {
            throw new IllegalArgumentException(String.join(", ", errorMessages));
        }
        if(parameters.target.method == null) {
            parameters.target.method = "GET";
        }
        parameters.target.setDefaults();
        if(parameters.body == null) {
            parameters.body = new RequestSpec.Body();
            parameters.body.format = RequestSpec.Format.none;
        }
        parameters.body.setDefaults();
        if(parameters.target.headers.keySet().stream().anyMatch(k -> k.equalsIgnoreCase("Content-Type"))) {
            parameters.body.contentType = null;
        }
        if(parameters.response == null) {
            parameters.response = new ResponsePolicy.Parameters();
            parameters.response.format = ResponsePolicy.Format.text;
        }
        if(parameters.response.format == null) {
            parameters.response.format = parameters.response.schema != null ? ResponsePolicy.Format.json : ResponsePolicy.Format.text;
        }
        parameters.response.setDefaults();
        if(parameters.timeout == null) {
            parameters.timeout = new HttpTransport.TimeoutParameters();
        }
        parameters.timeout.setDefaults();
        if(parameters.http == null) {
            parameters.http = new HttpTransport.Parameters();
        }
        parameters.http.setDefaults();
        if(parameters.http.allowedHosts == null && !parameters.target.auth.isNone()) {
            final String origin = RequestSpec.staticOrigin(parameters.target.url);
            if(origin != null) {
                parameters.http.allowedHosts = List.of(java.net.URI.create(origin).getHost());
            }
        }
        return new Http(name, parameters, inputFields, ignore);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean ignore() {
        return ignore;
    }

    @Override
    public List<Schema.Field> getInputFields() {
        return inputFields;
    }

    @Override
    public Schema.FieldType getOutputFieldType() {
        return outputFieldType;
    }

    @Override
    public void setup() {
        renderer.setup();
        policy.setup();
        if(policy.schema() != null) {
            policy.schema().setup();
        }
        final AuthProvider auth = AuthProvider.create(parameters.target.auth, RequestSpec.staticOrigin(parameters.target.url));
        this.transport = new HttpTransport(parameters.http, parameters.timeout, auth);
    }

    @Override
    public Object apply(Map<String, Object> input, Instant timestamp) {
        final MElement element = MElement.of(input, timestamp);
        final OutboundRequest request = renderer.build(element);
        final SyncCaller.Result result;
        try {
            result = SyncCaller.call("select." + name, transport, policy, request);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("http select function " + name + " interrupted", e);
        }
        if(result.response() != null && result.response().statusCode() == 404) {
            LOG.debug("select.{}: 404 from {}", name, request.url());
            return null;
        }
        if(!result.succeeded()) {
            throw new IllegalStateException("http select function " + name + " " + request.method() + " " + request.url() + " failed: " + result.error());
        }
        final ResponsePolicy.Parsed parsed = result.parsed();
        if(policy.schema() != null) {
            final JsonElement json = parsed.values().get("json") instanceof JsonElement e ? e : null;
            if(json == null || !json.isJsonObject()) {
                return null;
            }
            return JsonToElementConverter.convert(policy.schema().getFields(), json.getAsJsonObject());
        }
        return switch (outputFieldType.getType()) {
            case bytes -> parsed.bytes() == null ? null : ByteBuffer.wrap(parsed.bytes());
            default -> parsed.text();
        };
    }
}
