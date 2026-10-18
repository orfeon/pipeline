package com.mercari.solution.module.source;

import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.mercari.solution.module.*;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.domain.file.JsonUtil;
import com.mercari.solution.util.pipeline.Filter;
import com.mercari.solution.util.pipeline.OptionUtil;
import com.mercari.solution.util.pipeline.outbound.*;
import com.mercari.solution.util.pipeline.select.Scrape;
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
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.*;

/**
 * Source that fetches records from HTTP APIs.
 *
 * <p>A request definition mirrors the http sink: {@code target} (url / method / params / headers /
 * auth), {@code body} and {@code response} (format / schema / success / retry) are the shared
 * {@link RequestSpec} / {@link ResponsePolicy} blocks. A source adds {@code response.itemsPath}
 * (array fan-out into records), {@code format: html} with {@code itemsSelector} / {@code fields}
 * (pages scraped into typed records with {@link Scrape} definitions), template-valued
 * {@code fields}, {@code loop} (pagination), {@code input} (+ {@code foreach}, {@code when}) for
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

        // allowedHosts derived from the credential-bearing requests' urls (not configured): pins only those requests.
        // transient: set in setDefaults, never read from the config
        private transient boolean allowedHostsDerived;

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
                    } else if(request.foreach != null && parent.response != null && (parent.response.schema != null || parent.response.isHtml())) {
                        errorMessages.add("parameters.requests[" + i + "].foreach requires an untyped parent (" + request.input + " has response.schema or format html); use the parent's fields directly instead");
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
                // pin credentials to the configured hosts of the requests that send them
                final List<String> hosts = new ArrayList<>();
                for(final Request request : requests) {
                    if(!request.sendsCredentials()) {
                        continue;
                    }
                    final String origin = RequestSpec.staticOrigin(request.target.url);
                    if(origin != null) {
                        final String host = java.net.URI.create(origin).getHost();
                        if(host != null && !hosts.contains(host)) {
                            hosts.add(host);
                        }
                    }
                }
                http.allowedHosts = hosts;
                allowedHostsDerived = true;
            }
        }
    }

    /** Headers that carry credentials: a request setting one in target.headers is pinned like a request with auth. */
    private static final Set<String> CREDENTIAL_HEADERS = Set.of("authorization", "proxy-authorization", "cookie", "x-api-key");

    public static class Request implements Serializable {

        private String name;
        private RequestSpec.Target target;
        private RequestSpec.Body body;
        private Response response;
        private Loop loop;
        private String input;
        private String foreach;
        private JsonElement when;
        private Rate rate;

        private String whenJson;

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
            if(when != null) {
                if(input == null) {
                    errorMessages.add(prefix + ".when requires input (it is evaluated on each parent record or foreach item)");
                }
                try {
                    Filter.parse(when.toString());
                } catch (final RuntimeException e) {
                    errorMessages.add(prefix + ".when is illegal: " + e.getMessage());
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
            if(target.omitEmptyParams == null) {
                target.omitEmptyParams = true;   // e.g. a pagination cursor that is empty on the first page
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
            if(when != null) {
                whenJson = when.toString();
                when = null;
            }
        }

        public String getName() {
            return name;
        }

        /** Whether the request sends credentials: auth, or a credential header given in target.headers. */
        private boolean sendsCredentials() {
            return !target.auth.isNone()
                    || target.headers.keySet().stream().anyMatch(k -> CREDENTIAL_HEADERS.contains(k.toLowerCase()));
        }

        public boolean isTyped() {
            return response.schema_ != null || response.isHtml();
        }

        public Schema outputSchema() {
            if(response.fields_.isEmpty()) {
                return isTyped() ? response.schema_ : createRawSchema();
            }
            final Schema.Builder builder = response.isHtml() ? Schema.builder()
                    : Schema.builder(response.schema_ != null ? response.schema_ : createRawSchema());
            for(final ResponseField field : response.fields_) {
                builder.withField(Schema.Field.of(field.name(), field.fieldType()));
            }
            return builder.build();
        }
    }

    /** Response formats of the source: the {@link ResponsePolicy.Format}s plus {@code html} (scraped into typed records). */
    public enum Format {
        json,
        text,
        bytes,
        none,
        html
    }

    /** Types a {@code value} (template) field can be converted to (see RequestDoFn.toFieldValue). */
    private static final Set<Schema.Type> VALUE_FIELD_TYPES = EnumSet.of(
            Schema.Type.string, Schema.Type.json, Schema.Type.uuid, Schema.Type.bool,
            Schema.Type.int32, Schema.Type.int64, Schema.Type.float32, Schema.Type.float64,
            Schema.Type.date, Schema.Type.time, Schema.Type.timestamp, Schema.Type.bytes);

    public static class Response implements Serializable {
        private Format format;
        private JsonElement schema;
        private String itemsPath;
        private String itemsSelector;
        private String baseUri;
        private JsonElement fields;
        private ResponsePolicy.Success success;
        private ResponsePolicy.Retry retry;

        private ResponsePolicy.Parameters policy;   // built in setDefaults
        private Schema schema_;
        private List<ResponseField> fields_;

        private List<String> validate(final String prefix) {
            final List<String> errorMessages = new ArrayList<>();
            if(schema != null && !schema.isJsonObject()) {
                errorMessages.add(prefix + ".schema must be an object");
            }
            if(itemsPath != null && !itemsPath.startsWith("/")) {
                errorMessages.add(prefix + ".itemsPath must be a JSON pointer starting with / but: " + itemsPath);
            }
            if((itemsPath != null || schema != null) && format != null && !Format.json.equals(format)) {
                errorMessages.add(prefix + ".itemsPath / schema require format json (format html takes itemsSelector and fields)");
            }
            if(isHtml()) {
                if(fields == null) {
                    errorMessages.add(prefix + ".fields is required with format html");
                }
            } else if(itemsSelector != null || baseUri != null) {
                errorMessages.add(prefix + ".itemsSelector / baseUri require format html");
            }
            if(itemsSelector != null) {
                try {
                    org.jsoup.select.QueryParser.parse(itemsSelector);
                } catch (final RuntimeException e) {
                    errorMessages.add(prefix + ".itemsSelector is illegal: " + e.getMessage());
                }
            }
            if(fields != null) {
                errorMessages.addAll(validateFields(prefix));
            }
            final ResponsePolicy.Parameters p = new ResponsePolicy.Parameters();
            p.success = success;
            p.retry = retry;
            errorMessages.addAll(p.validate(prefix));
            return errorMessages;
        }

        private List<String> validateFields(final String prefix) {
            final List<String> errorMessages = new ArrayList<>();
            if(!fields.isJsonArray() || fields.getAsJsonArray().isEmpty()) {
                errorMessages.add(prefix + ".fields must be a non-empty array");
                return errorMessages;
            }
            // names the fields are added to: the raw record's (untyped) or none (html: the fields make
            // up the whole record). Collisions with response.schema are checked once it is parsed (setDefaults).
            final Set<String> names = new HashSet<>();
            if(!isHtml() && schema == null) {
                createRawSchema().getFields().forEach(f -> names.add(f.getName()));
            }
            final JsonArray array = fields.getAsJsonArray();
            for(int i = 0; i < array.size(); i++) {
                final String fieldPrefix = prefix + ".fields[" + i + "]";
                if(!array.get(i).isJsonObject()) {
                    errorMessages.add(fieldPrefix + " must be an object");
                    continue;
                }
                final JsonObject field = array.get(i).getAsJsonObject();
                if(!field.has("name") || field.get("name").isJsonNull()) {
                    errorMessages.add(fieldPrefix + ".name is required");
                    continue;
                }
                final String name = field.get("name").getAsString();
                if(!names.add(name)) {
                    errorMessages.add(fieldPrefix + ".name is duplicated (or collides with an output field): " + name);
                }
                final boolean hasSelector = field.has("selector");
                final boolean hasValue = field.has("value");
                if(hasSelector == hasValue) {
                    errorMessages.add(fieldPrefix + " requires exactly one of selector (scraped from the html item) or value (template)");
                } else if(hasSelector) {
                    if(!isHtml()) {
                        errorMessages.add(fieldPrefix + ".selector requires format html");
                    }
                    try {
                        Scrape.of(name, field, null, false).validate();
                    } catch (final RuntimeException e) {
                        errorMessages.add(fieldPrefix + " is illegal: " + e.getMessage());
                    }
                } else {
                    if(!field.get("value").isJsonPrimitive()) {
                        errorMessages.add(fieldPrefix + ".value must be a string (template)");
                    }
                    try {
                        final Schema.Type type = Schema.FieldType.type(field.has("type") ? field.get("type").getAsString() : "string").getType();
                        if(!VALUE_FIELD_TYPES.contains(type)) {
                            errorMessages.add(fieldPrefix + ".type " + type + " is not supported for a value field: " + VALUE_FIELD_TYPES);
                        }
                    } catch (final RuntimeException e) {
                        errorMessages.add(fieldPrefix + ".type is illegal for a value field: " + e.getMessage());
                    }
                }
            }
            return errorMessages;
        }

        private void setDefaults() {
            policy = new ResponsePolicy.Parameters();
            policy.format = switch (format == null ? Format.json : format) {
                case json -> ResponsePolicy.Format.json;
                case text, html -> ResponsePolicy.Format.text;
                case bytes -> ResponsePolicy.Format.bytes;
                case none -> ResponsePolicy.Format.none;
            };
            policy.success = success;
            policy.retry = retry;
            policy.setDefaults();
            if(schema != null) {
                schema_ = Schema.parse(schema.getAsJsonObject());
                schema = null;
            }
            fields_ = new ArrayList<>();
            if(fields != null) {
                for(final JsonElement element : fields.getAsJsonArray()) {
                    final JsonObject field = element.getAsJsonObject();
                    final String name = field.get("name").getAsString();
                    if(field.has("selector")) {
                        final Scrape scrape = Scrape.of(name, field, null, false);
                        fields_.add(new ResponseField(name, scrape, null, scrape.getOutputFieldType()));
                    } else {
                        final String type = field.has("type") ? field.get("type").getAsString() : "string";
                        fields_.add(new ResponseField(name, null, field.get("value").getAsString(), Schema.FieldType.type(type)));
                    }
                }
                fields = null;
            }
            if(schema_ != null) {
                for(final ResponseField field : fields_) {
                    if(schema_.hasField(field.name())) {
                        throw new IllegalModuleException("response.fields name collides with a response.schema field: " + field.name());
                    }
                }
            }
        }

        private boolean isHtml() {
            return Format.html.equals(format);
        }
    }

    /**
     * One entry of {@code response.fields}: scraped from each html item ({@code selector}, a
     * {@link Scrape} definition) or rendered per record from a template ({@code value}) against the
     * request context — the parent record / foreach item, loop variables and {@code __url} — and the
     * record's own fields.
     */
    public record ResponseField(String name, Scrape scrape, String value, Schema.FieldType fieldType) implements Serializable {}

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
            // batch: records are stamped with the execution time instead of the minimum timestamp
            seed = begin
                    .apply("Seed", Create.of(0L).withCoder(VarLongCoder.of()))
                    .apply("ToElement", ParDo.of(new SeedDoFn(true)))
                    .setCoder(ElementCoder.of(seedSchema));
        }

        // timestampAttribute: the event time of every request whose output has that field
        final String timestampAttribute = getTimestampAttribute();
        if(timestampAttribute != null) {
            final List<String> errorMessages = new ArrayList<>();
            boolean found = false;
            for(final Request request : parameters.requests) {
                final Schema schema = request.outputSchema();
                if(!schema.hasField(timestampAttribute)) {
                    continue;
                }
                found = true;
                final Schema.Type type = schema.getField(timestampAttribute).getFieldType().getType();
                if(!Schema.Type.timestamp.equals(type) && !Schema.Type.date.equals(type)) {
                    errorMessages.add("timestampAttribute " + timestampAttribute + " of request " + request.name + " must be timestamp or date type but: " + type);
                }
            }
            if(!found) {
                errorMessages.add("timestampAttribute " + timestampAttribute + " is not an output field of any request");
            }
            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
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

                if(request.whenJson != null && request.foreach == null) {
                    // when sees only the parent record's fields: an unknown one is always null and would skip every request silently
                    final List<String> unknown = new ArrayList<>();
                    for(final String variable : Filter.parse(request.whenJson).getRequiredVariables()) {
                        final int dot = variable.indexOf('.');
                        if(!parentSchema.hasField(dot < 0 ? variable : variable.substring(0, dot))) {
                            unknown.add(variable);
                        }
                    }
                    if(!unknown.isEmpty()) {
                        throw new IllegalModuleException("parameters.requests " + request.name + ".when refers to fields that are not in the output of " + request.input + ": " + unknown);
                    }
                }

                final Schema outputSchema = request.outputSchema();
                final String eventTimeField = timestampAttribute != null && outputSchema.hasField(timestampAttribute) ? timestampAttribute : null;
                final TupleTag<MElement> outputTag = new TupleTag<>() {};
                final TupleTag<BadRecord> failureTag = new TupleTag<>() {};
                final PCollectionTuple tuple = parent
                        .apply("Request_" + request.name, ParDo
                                .of(new RequestDoFn(getName(), request, parameters, parentSchema, outputSchema, eventTimeField, failureTag, getFailFast(), getLoggings()))
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

        private final boolean stampNow;

        SeedDoFn() {
            this(false);
        }

        SeedDoFn(final boolean stampNow) {
            this.stampNow = stampNow;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final Instant now = Instant.now();
            if(stampNow) {
                c.outputWithTimestamp(MElement.createDummyElement(now), now);
            } else {
                c.output(MElement.createDummyElement(now));
            }
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

        private static final java.util.regex.Pattern CHARSET = java.util.regex.Pattern.compile("charset=\"?([^;\"\\s]+)",java.util.regex.Pattern.CASE_INSENSITIVE);

        private final String moduleName;
        private final Request request;
        private final HttpTransport.TimeoutParameters timeout;
        private final HttpTransport.Parameters http;
        private final Schema inputSchema;
        private final Schema outputSchema;
        private final String eventTimeField;
        private final Schema.Type eventTimeType;
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
        private transient Filter.ConditionNode whenCondition;
        private transient Map<String, Template> valueTemplates;

        RequestDoFn(
                final String moduleName,
                final Request request,
                final Parameters parameters,
                final Schema inputSchema,
                final Schema outputSchema,
                final String eventTimeField,
                final TupleTag<BadRecord> failureTag,
                final boolean failFast,
                final List<Logging> loggings) {

            this.moduleName = moduleName;
            this.request = request;
            this.timeout = parameters.timeout;
            // a request without auth or credential headers sends no credentials: the derived host pinning does not
            // apply to it (e.g. a signed download URL on another host); configured allowedHosts apply to every request
            this.http = parameters.allowedHostsDerived && !request.sendsCredentials()
                    ? parameters.http.withAllowedHosts(null) : parameters.http;
            this.inputSchema = inputSchema;
            this.outputSchema = outputSchema;
            this.eventTimeField = eventTimeField;
            this.eventTimeType = eventTimeField == null ? null : outputSchema.getField(eventTimeField).getFieldType().getType();
            this.failureTag = failureTag;
            this.failFast = failFast;
            this.logging = Logging.map(loggings);
            final List<String> extraTexts = new ArrayList<>(request.loop.next.values());
            for(final ResponseField field : request.response.fields_) {
                if(field.value() != null) {
                    extraTexts.add(field.value());
                }
            }
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
            this.whenCondition = request.whenJson == null ? null : Filter.parse(request.whenJson);
            this.valueTemplates = new LinkedHashMap<>();
            for(final ResponseField field : request.response.fields_) {
                if(field.scrape() != null) {
                    field.scrape().setup();
                } else {
                    valueTemplates.put(field.name(), TemplateUtil.createStrictTemplate(moduleName + "." + request.name + ".fields." + field.name(), field.value()));
                }
            }
        }

        @Teardown
        public void teardown() {
            if(transport != null) {
                transport.close();
            }
        }

        // timestampAttribute values (e.g. a publication time) may precede the parent's timestamp
        @Override
        public Duration getAllowedTimestampSkew() {
            return Duration.millis(Long.MAX_VALUE);
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
                    if(whenCondition != null && !Filter.filter(whenCondition, input.asStandardMap(inputSchema))) {
                        return;
                    }
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
                final Map<String, Object> parentValues = whenCondition == null ? null : input.asStandardMap(inputSchema);
                for(final JsonElement item : list) {
                    final Map<String, Object> itemValues = item.isJsonObject() ? JsonToMapConverter.convert(item) : Map.of();
                    final Object itemVar = item.isJsonObject() ? itemValues : toVar(item);
                    if(whenCondition != null) {
                        final Map<String, Object> whenValues = new HashMap<>(parentValues);
                        whenValues.putAll(itemValues);
                        whenValues.put("__item", itemVar);
                        if(!Filter.filter(whenCondition, whenValues)) {
                            continue;
                        }
                    }
                    final Map<String, Object> values = renderer.createTemplateValues(input);
                    values.putAll(itemValues);
                    values.put("__item", itemVar);
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
                emit(c, outbound, result, values);
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

        private void emit(final ProcessContext c, final OutboundRequest outbound, final SyncCaller.Result result, final Map<String, Object> values) {
            final Instant responseTime = Instant.now();
            final Map<String, Object> templateValues;
            if(valueTemplates.isEmpty()) {
                templateValues = null;
            } else {
                templateValues = new HashMap<>(values);
                templateValues.put("__url", outbound.url());
            }
            if(request.response.isHtml()) {
                final Document document = parseHtml(outbound, result);
                final List<org.jsoup.nodes.Element> items = request.response.itemsSelector == null
                        ? List.of(document) : document.select(request.response.itemsSelector);
                for(final org.jsoup.nodes.Element item : items) {
                    final Map<String, Object> map = new HashMap<>();
                    for(final ResponseField field : request.response.fields_) {
                        if(field.scrape() != null) {
                            map.put(field.name(), field.scrape().extractFrom(item));
                        }
                    }
                    output(c, map, templateValues);
                }
                return;
            }
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
            if(request.isTyped()) {
                for(final JsonElement row : rows) {
                    if(row == null || !row.isJsonObject()) {
                        LOG.warn("{}: skipped non-object response row: {}", request.name, row);
                        continue;
                    }
                    final Map<String, Object> map = JsonToElementConverter.convert(request.response.schema_.getFields(), row.getAsJsonObject());
                    output(c, map, templateValues);
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
                    map.put("timestamp", responseTime.getMillis() * 1000L);
                    output(c, map, templateValues);
                }
            }
        }

        /** Adds the value fields to one record and outputs it, stamped with eventTimeField when it is set. */
        private void output(final ProcessContext c, final Map<String, Object> map, final Map<String, Object> templateValues) {
            if(templateValues != null) {
                renderValueFields(map, templateValues);
            }
            final Instant eventTime = eventTime(map);
            final MElement output = MElement.of(map, eventTime == null ? c.timestamp() : eventTime);
            Logging.log(LOG, logging, "output", output);
            if(eventTime == null) {
                c.output(output);
            } else {
                c.outputWithTimestamp(output, eventTime);
            }
        }

        /**
         * Renders the {@code value} fields of one record, in order. A template sees the request values
         * (parent record / foreach item, loop variables, {@code __url}) overlaid with the record's own
         * fields (scraped, JSON or raw) and the value fields rendered before it, as standard values.
         */
        private void renderValueFields(final Map<String, Object> map, final Map<String, Object> templateValues) {
            final Map<String, Object> rowValues = new HashMap<>(templateValues);
            for(final Map.Entry<String, Object> entry : map.entrySet()) {
                rowValues.put(entry.getKey(), standardValue(entry.getKey(), entry.getValue()));
            }
            for(final ResponseField field : request.response.fields_) {
                final Template template = valueTemplates.get(field.name());
                if(template == null) {
                    continue;
                }
                final Object value = toFieldValue(field.fieldType(), TemplateUtil.executeStrictTemplate(template, rowValues));
                map.put(field.name(), value);
                rowValues.put(field.name(), standardValue(field.name(), value));
            }
        }

        private Object standardValue(final String name, final Object primitiveValue) {
            if(primitiveValue == null || !outputSchema.hasField(name)) {
                return primitiveValue;
            }
            return MElement.getAsStandardValue(outputSchema.getField(name).getFieldType(), primitiveValue);
        }

        /** The record's eventTimeField value as an instant (timestamp: epoch micros, date: epoch days at UTC midnight), or null. */
        private Instant eventTime(final Map<String, Object> map) {
            if(eventTimeField == null || !(map.get(eventTimeField) instanceof Number number)) {
                return null;
            }
            return switch (eventTimeType) {
                case timestamp -> Instant.ofEpochMilli(Math.floorDiv(number.longValue(), 1000L));
                case date -> Instant.ofEpochMilli(number.longValue() * 86_400_000L);
                default -> null;
            };
        }

        private static Object toFieldValue(final Schema.FieldType fieldType, final String text) {
            if(text == null) {
                return null;
            }
            if(Schema.Type.string.equals(fieldType.getType())) {
                return text;
            }
            if(text.isBlank()) {
                return null;
            }
            return JsonToMapConverter.getAsPrimitiveValue(fieldType, new JsonPrimitive(text.trim()));
        }

        /** Parses the body as html with the charset of the Content-Type header (else the document's meta / BOM, default UTF-8). */
        private Document parseHtml(final OutboundRequest outbound, final SyncCaller.Result result) {
            final String baseUri = request.response.baseUri == null ? outbound.url() : request.response.baseUri;
            final byte[] body = result.response().body();
            if(body == null) {
                return Jsoup.parse("", baseUri);
            }
            try(final InputStream in = new ByteArrayInputStream(body)) {
                return Jsoup.parse(in, charset(result.response().headers()), baseUri);
            } catch (final IOException e) {
                throw new UncheckedIOException("failed to parse html response of " + outbound.url(), e);
            }
        }

        private static String charset(final Map<String, List<String>> headers) {
            for(final Map.Entry<String, List<String>> entry : headers.entrySet()) {
                if(entry.getKey() == null || !entry.getKey().equalsIgnoreCase("Content-Type") || entry.getValue() == null) {
                    continue;
                }
                for(final String value : entry.getValue()) {
                    final java.util.regex.Matcher matcher = CHARSET.matcher(value);
                    if(matcher.find()) {
                        final String name = matcher.group(1);
                        try {
                            if(java.nio.charset.Charset.isSupported(name)) {
                                return name;
                            }
                        } catch (final IllegalArgumentException e) {
                            // illegal charset name: fall back to detection
                        }
                    }
                }
            }
            return null;
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
