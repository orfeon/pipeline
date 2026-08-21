package com.mercari.solution.module.source;

import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.mercari.solution.module.*;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.domain.file.JsonUtil;
import com.mercari.solution.util.pipeline.Filter;
import com.mercari.solution.util.pipeline.OptionUtil;
import com.mercari.solution.util.pipeline.outbound.*;
import com.mercari.solution.util.schema.converter.JsonToElementConverter;
import com.mercari.solution.util.schema.converter.JsonToMapConverter;
import freemarker.template.Template;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.*;
import org.apache.commons.lang3.math.NumberUtils;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

/**
 * Source that fetches records from HTTP APIs.
 *
 * <p>A request definition mirrors the http sink: {@code target} (url / method / params / headers /
 * auth), {@code body} and {@code response} (format / schema / success / retry) are the shared
 * {@link RequestSpec} / {@link ResponsePolicy} blocks. A source adds {@code response.itemsPath}
 * (array fan-out into records), {@code loop} (pagination), {@code input} (+ {@code foreach}) for
 * chaining one request per parent record (or per item of the parent's response), {@code rate}
 * and, in streaming mode, {@code polling}. A single request may be written directly under
 * {@code parameters}; several requests go under {@code requests}.
 *
 * <p>Outputs: the first request is the module's main output; every request is also available as
 * the tagged output {@code <module>.<request name>}.
 */
@Source.Module(name="http")
public class HttpSource extends Source {

    private static final Logger LOG = LoggerFactory.getLogger(HttpSource.class);

    public static class Parameters implements Serializable {

        private List<Request> requests;
        // single-request shorthand (same blocks as one requests[] entry)
        private RequestSpec.Target target;
        private RequestSpec.Body body;
        private Response response;
        private Loop loop;
        private Rate rate;

        private AuthProvider.Parameters auth;
        private HttpTransport.TimeoutParameters timeout;
        private HttpTransport.Parameters http;
        private Polling polling;

        private void validate(final String moduleName) {
            final List<String> errorMessages = new ArrayList<>();
            if(requests == null || requests.isEmpty()) {
                if(target == null) {
                    errorMessages.add("parameters.requests (or parameters.target for a single request) must not be empty");
                } else {
                    final Request single = new Request();
                    single.name = moduleName;
                    single.target = target;
                    single.body = body;
                    single.response = response;
                    single.loop = loop;
                    single.rate = rate;
                    requests = new ArrayList<>(List.of(single));
                }
            } else if(target != null || body != null || response != null || loop != null) {
                errorMessages.add("parameters.target/body/response/loop are the single-request form and cannot be combined with parameters.requests");
            }
            if(auth != null) {
                errorMessages.addAll(auth.validate("parameters.auth"));
            }
            if(timeout != null) {
                errorMessages.addAll(timeout.validate("parameters.timeout"));
            }
            if(http != null) {
                errorMessages.addAll(http.validate("parameters.http"));
            }
            if(polling != null) {
                errorMessages.addAll(polling.validate());
            }
            if(requests != null && !requests.isEmpty()) {
                final Map<String, Request> byName = new LinkedHashMap<>();
                for(int i = 0; i < requests.size(); i++) {
                    final Request request = requests.get(i);
                    final String prefix = "parameters.requests[" + i + "]";
                    if(request == null) {
                        errorMessages.add(prefix + " must not be null");
                        continue;
                    }
                    if(request.name == null) {
                        if(requests.size() > 1) {
                            errorMessages.add(prefix + ".name is required when several requests are defined");
                        }
                    } else if(byName.put(request.name, request) != null) {
                        errorMessages.add(prefix + ".name is duplicated: " + request.name);
                    }
                    errorMessages.addAll(request.validate(prefix, http != null && http.allowedHosts != null, auth));
                }
                for(int i = 0; i < requests.size(); i++) {
                    final Request request = requests.get(i);
                    if(request == null || request.input == null) {
                        continue;
                    }
                    final Request parent = byName.get(request.input);
                    if(parent == null) {
                        errorMessages.add("parameters.requests[" + i + "].input refers to an unknown request: " + request.input);
                    } else if(request.foreach != null && parent.response != null && parent.response.schema != null) {
                        errorMessages.add("parameters.requests[" + i + "].foreach requires an untyped parent (" + request.input + " has response.schema); use the parent's fields directly instead");
                    }
                }
                if(byName.size() == requests.size() && hasCycle(requests)) {
                    errorMessages.add("parameters.requests input chain must not be cyclic");
                }
            }
            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        private static boolean hasCycle(final List<Request> requests) {
            final Map<String, String> parent = new HashMap<>();
            for(final Request r : requests) {
                parent.put(r.name, r.input);
            }
            for(final Request r : requests) {
                final Set<String> seen = new HashSet<>();
                String current = r.name;
                while(current != null) {
                    if(!seen.add(current)) {
                        return true;
                    }
                    current = parent.get(current);
                }
            }
            return false;
        }

        private void setDefaults(final String moduleName) {
            if(auth == null) {
                auth = new AuthProvider.Parameters();
            }
            auth.setDefaults();
            if(timeout == null) {
                timeout = new HttpTransport.TimeoutParameters();
            }
            timeout.setDefaults();
            if(http == null) {
                http = new HttpTransport.Parameters();
            }
            http.setDefaults();
            for(final Request request : requests) {
                request.setDefaults(moduleName, auth);
            }
            if(http.allowedHosts == null && requests.stream().anyMatch(r -> !r.target.auth.isNone())) {
                // pin auth headers to the configured hosts
                final List<String> hosts = new ArrayList<>();
                for(final Request request : requests) {
                    final String origin = RequestSpec.staticOrigin(request.target.url);
                    if(origin != null) {
                        final String host = java.net.URI.create(origin).getHost();
                        if(host != null && !hosts.contains(host)) {
                            hosts.add(host);
                        }
                    }
                }
                http.allowedHosts = hosts;
            }
        }
    }

    public static class Request implements Serializable {

        private String name;
        private RequestSpec.Target target;
        private RequestSpec.Body body;
        private Response response;
        private Loop loop;
        private String input;
        private String foreach;
        private Rate rate;

        private List<String> validate(final String prefix, final boolean allowedHostsGiven, final AuthProvider.Parameters defaultAuth) {
            final List<String> errorMessages = new ArrayList<>();
            if(target == null) {
                errorMessages.add(prefix + ".target must not be null");
            } else {
                errorMessages.addAll(target.validate(prefix + ".target", null, allowedHostsGiven));
                if(target.auth == null && defaultAuth != null && !defaultAuth.isNone()
                        && target.url != null && RequestSpec.staticOrigin(target.url) == null && !allowedHostsGiven) {
                    errorMessages.add("parameters.http.allowedHosts is required when parameters.auth is set and the host part of " + prefix + ".target.url is a template");
                }
            }
            if(body != null) {
                errorMessages.addAll(body.validate(prefix + ".body", null));
                if(RequestSpec.Format.bytes.equals(body.format) || RequestSpec.Format.avro.equals(body.format) || RequestSpec.Format.protobuf.equals(body.format)) {
                    errorMessages.add(prefix + ".body.format " + body.format + " is not supported in the http source (json, ndjson, form, multipart, template, none)");
                }
            }
            if(response != null) {
                errorMessages.addAll(response.validate(prefix + ".response"));
            }
            if(loop != null) {
                errorMessages.addAll(loop.validate(prefix + ".loop"));
            }
            if(foreach != null) {
                if(input == null) {
                    errorMessages.add(prefix + ".foreach requires input");
                }
                if(!foreach.startsWith("/")) {
                    errorMessages.add(prefix + ".foreach must be a JSON pointer starting with / but: " + foreach);
                }
            }
            if(rate != null) {
                errorMessages.addAll(rate.validate(prefix + ".rate"));
            }
            return errorMessages;
        }

        private void setDefaults(final String moduleName, final AuthProvider.Parameters defaultAuth) {
            if(name == null) {
                name = moduleName;
            }
            if(target.method == null) {
                target.method = "GET";
            }
            if(target.auth == null) {
                target.auth = defaultAuth;
            }
            target.setDefaults();
            if(body == null) {
                body = new RequestSpec.Body();
                body.format = RequestSpec.Format.none;
            }
            body.setDefaults();
            if(target.headers.keySet().stream().anyMatch(k -> k.equalsIgnoreCase("Content-Type"))) {
                body.contentType = null; // header given explicitly
            }
            if(response == null) {
                response = new Response();
            }
            response.setDefaults();
            if(loop == null) {
                loop = new Loop();
            }
            loop.setDefaults();
        }

        public String getName() {
            return name;
        }

        public boolean isTyped() {
            return response.schema_ != null;
        }

        public Schema outputSchema() {
            return isTyped() ? response.schema_ : createRawSchema();
        }
    }

    public static class Response implements Serializable {
        private ResponsePolicy.Format format;
        private JsonElement schema;
        private String itemsPath;
        private ResponsePolicy.Success success;
        private ResponsePolicy.Retry retry;

        private ResponsePolicy.Parameters policy;   // built in setDefaults
        private Schema schema_;

        private List<String> validate(final String prefix) {
            final List<String> errorMessages = new ArrayList<>();
            if(schema != null && !schema.isJsonObject()) {
                errorMessages.add(prefix + ".schema must be an object");
            }
            if(itemsPath != null && !itemsPath.startsWith("/")) {
                errorMessages.add(prefix + ".itemsPath must be a JSON pointer starting with / but: " + itemsPath);
            }
            if((itemsPath != null || schema != null) && format != null && !ResponsePolicy.Format.json.equals(format)) {
                errorMessages.add(prefix + ".itemsPath / schema require format json");
            }
            final ResponsePolicy.Parameters p = new ResponsePolicy.Parameters();
            p.success = success;
            p.retry = retry;
            errorMessages.addAll(p.validate(prefix));
            return errorMessages;
        }

        private void setDefaults() {
            policy = new ResponsePolicy.Parameters();
            policy.format = format == null ? ResponsePolicy.Format.json : format;
            policy.success = success;
            policy.retry = retry;
            policy.setDefaults();
            if(schema != null) {
                schema_ = Schema.parse(schema.getAsJsonObject());
                schema = null;
            }
        }
    }

    /** Pagination: repeat the request until {@code until} holds, computing the next variables from each response. */
    public static class Loop implements Serializable {
        private Map<String, JsonElement> vars;
        private LinkedHashMap<String, String> next;
        private JsonElement until;
        private Integer maxIterations;

        private String untilJson;
        private Map<String, String> varsJson;

        private List<String> validate(final String prefix) {
            final List<String> errorMessages = new ArrayList<>();
            if(maxIterations != null && maxIterations < 1) {
                errorMessages.add(prefix + ".maxIterations must be >= 1");
            }
            if(until == null) {
                errorMessages.add(prefix + ".until must not be null");
            }
            return errorMessages;
        }

        private void setDefaults() {
            if(until != null) {
                untilJson = until.toString();
                until = null;
            }
            varsJson = new LinkedHashMap<>();
            if(vars != null) {
                for(final Map.Entry<String, JsonElement> entry : vars.entrySet()) {
                    varsJson.put(entry.getKey(), entry.getValue().toString());
                }
                vars = null;
            }
            if(next == null) {
                next = new LinkedHashMap<>();
            }
            if(maxIterations == null) {
                maxIterations = 10000;
            }
        }
    }

    public static class Rate implements Serializable {
        private Double count;
        private String unit;

        private List<String> validate(final String prefix) {
            final List<String> errorMessages = new ArrayList<>();
            if(count == null || count <= 0) {
                errorMessages.add(prefix + ".count must be positive");
            }
            if(unit != null && !unit.equals("second") && !unit.equals("minute")) {
                errorMessages.add(prefix + ".unit must be second or minute but: " + unit);
            }
            return errorMessages;
        }

        double permitsPerSecond() {
            return "minute".equals(unit) ? count / 60D : count;
        }
    }

    public static class Polling implements Serializable {
        private String interval;

        private List<String> validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(interval == null) {
                errorMessages.add("parameters.polling.interval must not be null");
            } else {
                try {
                    Durations.parse(interval);
                } catch (final IllegalArgumentException e) {
                    errorMessages.add("parameters.polling.interval is illegal: " + e.getMessage());
                }
            }
            return errorMessages;
        }
    }

    @Override
    public MCollectionTuple expand(
            final PBegin begin,
            final MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        if(parameters == null) {
            throw new IllegalModuleException("http source module parameters must not be empty!");
        }
        parameters.validate(getName());
        parameters.setDefaults(getName());

        // seed: one element (batch) or one element per polling tick (streaming)
        final Schema seedSchema = MElement.dummySchema();
        final PCollection<MElement> seed;
        if(parameters.polling != null && OptionUtil.isStreaming(begin)) {
            final Duration interval = Duration.millis(Durations.parse(parameters.polling.interval).toMillis());
            seed = begin
                    .apply("Polling", GenerateSequence.from(0).withRate(1, interval))
                    .apply("Seed", ParDo.of(new SeedDoFn()))
                    .setCoder(ElementCoder.of(seedSchema));
        } else {
            seed = begin
                    .apply("Seed", Create.of(0L).withCoder(VarLongCoder.of()))
                    .apply("ToElement", ParDo.of(new SeedDoFn()))
                    .setCoder(ElementCoder.of(seedSchema));
        }

        final Map<String, PCollection<MElement>> outputs = new LinkedHashMap<>();
        final Map<String, Schema> schemas = new LinkedHashMap<>();
        final List<PCollection<BadRecord>> failures = new ArrayList<>();
        // the tuple registers coders: build it as outputs are created, before children consume them
        final Request first = parameters.requests.get(0);
        MCollectionTuple result = null;

        // requests are applied level by level: parents before children
        final List<Request> pending = new ArrayList<>(parameters.requests);
        int guard = 0;
        while(!pending.isEmpty()) {
            if(guard++ > parameters.requests.size()) {
                throw new IllegalModuleException("http source requests could not be ordered (cyclic input?)");
            }
            for(final Iterator<Request> it = pending.iterator(); it.hasNext();) {
                final Request request = it.next();
                final PCollection<MElement> parent;
                final Schema parentSchema;
                if(request.input == null) {
                    parent = seed;
                    parentSchema = seedSchema;
                } else if(outputs.containsKey(request.input)) {
                    parent = outputs.get(request.input).apply("Reshuffle_" + request.name, Reshuffle.viaRandomKey());
                    parentSchema = schemas.get(request.input);
                } else {
                    continue;
                }
                it.remove();

                final Schema outputSchema = request.outputSchema();
                final TupleTag<MElement> outputTag = new TupleTag<>() {};
                final TupleTag<BadRecord> failureTag = new TupleTag<>() {};
                final PCollectionTuple tuple = parent
                        .apply("Request_" + request.name, ParDo
                                .of(new RequestDoFn(getName(), request, parameters, parentSchema, outputSchema, failureTag, getFailFast(), getLoggings()))
                                .withOutputTags(outputTag, TupleTagList.of(failureTag)));
                final PCollection<MElement> output = tuple.get(outputTag).setCoder(ElementCoder.of(outputSchema));
                outputs.put(request.name, output);
                schemas.put(request.name, outputSchema);
                failures.add(tuple.get(failureTag));
                if(request == first) {
                    result = MCollectionTuple.of(output, outputSchema);
                }
                result = result.and(request.name, output, outputSchema);
            }
        }

        errorHandler.addError(PCollectionList.of(failures).apply("FlattenFailures", Flatten.pCollections()));
        return result;
    }

    private static class SeedDoFn extends DoFn<Long, MElement> {
        @ProcessElement
        public void processElement(final ProcessContext c) {
            c.output(MElement.createDummyElement(Instant.now()));
        }
    }

    /** Raw (untyped) response record schema. */
    public static Schema createRawSchema() {
        return Schema.builder()
                .withField(Schema.Field.of("name", Schema.FieldType.STRING.withNullable(false)))
                .withField(Schema.Field.of("url", Schema.FieldType.STRING.withNullable(false)))
                .withField(Schema.Field.of("method", Schema.FieldType.STRING.withNullable(false)))
                .withField(Schema.Field.of("statusCode", Schema.FieldType.INT32.withNullable(false)))
                .withField(Schema.Field.of("headers", Schema.FieldType.map(Schema.FieldType.array(Schema.FieldType.STRING)).withNullable(true)))
                .withField(Schema.Field.of("body", Schema.FieldType.STRING.withNullable(true)))
                .withField(Schema.Field.of("blob", Schema.FieldType.BYTES.withNullable(true)))
                .withField(Schema.Field.of("payload", Schema.FieldType.JSON.withNullable(true)))
                .withField(Schema.Field.of("attempts", Schema.FieldType.INT32.withNullable(false)))
                .withField(Schema.Field.of("durationMs", Schema.FieldType.INT64.withNullable(false)))
                .withField(Schema.Field.of("timestamp", Schema.FieldType.TIMESTAMP.withNullable(false)))
                .build();
    }

    /** Executes one request definition for each input element (seed, parent record, or parent item), with pagination. */
    private static class RequestDoFn extends DoFn<MElement, MElement> {

        private final String moduleName;
        private final Request request;
        private final HttpTransport.TimeoutParameters timeout;
        private final HttpTransport.Parameters http;
        private final Schema outputSchema;
        private final TupleTag<BadRecord> failureTag;
        private final boolean failFast;
        private final Map<String, Logging> logging;
        private final RequestRenderer renderer;
        private final ResponsePolicy policy;

        private transient HttpTransport transport;
        private transient Filter.ConditionNode untilCondition;
        private transient Map<String, Template> nextTemplates;
        private transient Map<String, Object> initialVars;
        private transient RateLimiter rateLimiter;

        RequestDoFn(
                final String moduleName,
                final Request request,
                final Parameters parameters,
                final Schema inputSchema,
                final Schema outputSchema,
                final TupleTag<BadRecord> failureTag,
                final boolean failFast,
                final List<Logging> loggings) {

            this.moduleName = moduleName;
            this.request = request;
            this.timeout = parameters.timeout;
            this.http = parameters.http;
            this.outputSchema = outputSchema;
            this.failureTag = failureTag;
            this.failFast = failFast;
            this.logging = Logging.map(loggings);
            final List<String> extraTexts = new ArrayList<>(request.loop.next.values());
            final Set<String> dynamicVars = new HashSet<>(request.loop.varsJson.keySet());
            dynamicVars.addAll(request.loop.next.keySet());
            if(request.foreach != null) {
                // item fields are unknown at assembly time: render every template per request
                dynamicVars.add(RequestRenderer.DYNAMIC_ALL);
            }
            this.renderer = new RequestRenderer(moduleName + "." + request.name, request.target, request.body,
                    null, false, inputSchema, inputSchema, List.of(request.input == null ? "" : request.input), extraTexts, dynamicVars);
            this.policy = new ResponsePolicy(request.response.policy);
        }

        @Setup
        public void setup() {
            renderer.setup();
            policy.setup();
            if(outputSchema != null) {
                outputSchema.setup();
            }
            final AuthProvider auth = AuthProvider.create(request.target.auth, RequestSpec.staticOrigin(request.target.url));
            this.transport = new HttpTransport(http, timeout, auth);
            this.untilCondition = request.loop.untilJson == null ? null : Filter.parse(request.loop.untilJson);
            this.nextTemplates = new LinkedHashMap<>();
            for(final Map.Entry<String, String> entry : request.loop.next.entrySet()) {
                nextTemplates.put(entry.getKey(), TemplateUtil.createStrictTemplate(moduleName + "." + request.name + ".next." + entry.getKey(), entry.getValue()));
            }
            this.initialVars = new LinkedHashMap<>();
            for(final Map.Entry<String, String> entry : request.loop.varsJson.entrySet()) {
                initialVars.put(entry.getKey(), toVar(JsonUtil.fromJson(entry.getValue())));
            }
            this.rateLimiter = request.rate == null ? null : RateLimiter.create(request.rate.permitsPerSecond());
        }

        @Teardown
        public void teardown() {
            if(transport != null) {
                transport.close();
            }
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final MElement input = c.element();
            if(input == null) {
                return;
            }
            if(request.input != null) {
                Logging.log(LOG, logging, "input", input);
            }
            try {
                if(request.foreach == null) {
                    execute(c, input, renderer.createTemplateValues(input));
                    return;
                }
                // fan out over the parent's response items
                final Object payload = input.getPrimitiveValue("payload");
                final JsonElement json = payload == null ? null : JsonUtil.fromJson(payload.toString());
                final JsonElement items = json == null ? null : ResponsePolicy.pointer(json, request.foreach);
                if(items == null || items.isJsonNull()) {
                    return;
                }
                final List<JsonElement> list = new ArrayList<>();
                if(items.isJsonArray()) {
                    items.getAsJsonArray().forEach(list::add);
                } else {
                    list.add(items);
                }
                for(final JsonElement item : list) {
                    final Map<String, Object> values = renderer.createTemplateValues(input);
                    if(item.isJsonObject()) {
                        final Map<String, Object> itemValues = JsonToMapConverter.convert(item);
                        values.putAll(itemValues);
                        values.put("__item", itemValues);
                    } else {
                        values.put("__item", toVar(item));
                    }
                    execute(c, input, values);
                }
            } catch (final Throwable e) {
                final BadRecord badRecord = processError("Failed http request: " + request.name, input, e, failFast);
                c.output(failureTag, badRecord);
            }
        }

        private void execute(final ProcessContext c, final MElement input, final Map<String, Object> values) throws InterruptedException {
            final Map<String, Object> vars = new LinkedHashMap<>(initialVars);
            int iteration = 0;
            while(true) {
                iteration++;
                values.putAll(vars);
                if(rateLimiter != null) {
                    rateLimiter.acquire();
                }
                final OutboundRequest outbound = renderer.build(List.of(input), values);
                final SyncCaller.Result result = SyncCaller.call(moduleName + "." + request.name, transport, policy, outbound);
                if(!result.succeeded()) {
                    throw new IllegalStateException("request " + request.name + " " + outbound.method() + " " + outbound.url() + " failed: " + result.error());
                }
                emit(c, outbound, result);
                if(untilCondition == null || iteration >= request.loop.maxIterations) {
                    break;
                }
                final Map<String, Object> loopValues = new HashMap<>(values);
                loopValues.putAll(responseValues(result));
                if(Filter.filter(untilCondition, loopValues)) {
                    break;
                }
                for(final Map.Entry<String, Template> entry : nextTemplates.entrySet()) {
                    final String text = TemplateUtil.executeStrictTemplate(entry.getValue(), loopValues);
                    vars.put(entry.getKey(), toVar(text));
                }
            }
        }

        private void emit(final ProcessContext c, final OutboundRequest outbound, final SyncCaller.Result result) {
            final ResponsePolicy.Parsed parsed = result.parsed();
            final JsonElement json = parsed.values().get("json") instanceof JsonElement e ? e : null;
            final List<JsonElement> rows = new ArrayList<>();
            if(request.response.itemsPath != null) {
                final JsonElement target = json == null ? null : ResponsePolicy.pointer(json, request.response.itemsPath);
                if(target != null && target.isJsonArray()) {
                    for(final JsonElement row : target.getAsJsonArray()) {
                        rows.add(row);
                    }
                } else if(target != null && !target.isJsonNull()) {
                    rows.add(target);
                }
            } else {
                rows.add(json);
            }
            final Instant timestamp = c.timestamp();
            if(request.isTyped()) {
                for(final JsonElement row : rows) {
                    if(row == null || !row.isJsonObject()) {
                        LOG.warn("{}: skipped non-object response row: {}", request.name, row);
                        continue;
                    }
                    final Map<String, Object> map = JsonToElementConverter.convert(outputSchema.getFields(), row.getAsJsonObject());
                    final MElement output = MElement.of(map, timestamp);
                    Logging.log(LOG, logging, "output", output);
                    c.output(output);
                }
            } else {
                for(final JsonElement row : rows) {
                    final Map<String, Object> map = new HashMap<>();
                    map.put("name", request.name);
                    map.put("url", outbound.url());
                    map.put("method", outbound.method());
                    map.put("statusCode", result.response().statusCode());
                    map.put("headers", new HashMap<>(result.response().headers()));
                    map.put("body", parsed.text());
                    map.put("blob", parsed.bytes() == null ? null : java.nio.ByteBuffer.wrap(parsed.bytes()));
                    map.put("payload", row == null || row.isJsonNull() ? null : row.toString());
                    map.put("attempts", result.attempts());
                    map.put("durationMs", result.response().durationMs());
                    map.put("timestamp", timestamp.getMillis() * 1000L);
                    final MElement output = MElement.of(map, timestamp);
                    Logging.log(LOG, logging, "output", output);
                    c.output(output);
                }
            }
        }

        /** Response variables for loop until / next: statusCode, headers (first values), body, payload. */
        private static Map<String, Object> responseValues(final SyncCaller.Result result) {
            final Map<String, Object> values = new HashMap<>();
            values.put("statusCode", result.response().statusCode());
            final Map<String, String> headers = new HashMap<>();
            for(final Map.Entry<String, List<String>> entry : result.response().headers().entrySet()) {
                if(entry.getKey() != null && entry.getValue() != null && !entry.getValue().isEmpty()) {
                    headers.put(entry.getKey(), entry.getValue().get(0));
                    headers.put(entry.getKey().toLowerCase(), entry.getValue().get(0));
                }
            }
            values.put("headers", headers);
            values.put("body", result.parsed().text());
            values.put("payload", result.parsed().payload());
            return values;
        }

        private static Object toVar(final JsonElement element) {
            if(element == null || element.isJsonNull()) {
                return null;
            }
            if(element.isJsonPrimitive()) {
                final JsonPrimitive p = element.getAsJsonPrimitive();
                if(p.isBoolean()) {
                    return p.getAsBoolean();
                }
                if(p.isNumber()) {
                    final String s = p.getAsString();
                    return s.contains(".") || s.contains("e") || s.contains("E") ? (Object) p.getAsDouble() : (Object) p.getAsLong();
                }
                return p.getAsString();
            }
            return element.toString();
        }

        private static Object toVar(final String text) {
            if(text == null) {
                return null;
            }
            final String t = text.trim();
            if(NumberUtils.isCreatable(t)) {
                return NumberUtils.isDigits(t) ? (Object) Long.parseLong(t) : (Object) Double.parseDouble(t);
            }
            if("true".equals(t) || "false".equals(t)) {
                return Boolean.parseBoolean(t);
            }
            return text;
        }
    }
}
