package com.mercari.solution.module.action;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.pipeline.Filter;
import com.mercari.solution.util.pipeline.outbound.*;
import freemarker.template.Template;
import org.apache.beam.sdk.options.PipelineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

/**
 * Action service that performs one HTTP request per firing — the control-plane counterpart of the
 * http sink. Typical uses: notify (Slack / webhook) after upstream steps complete, trigger a
 * downstream job (Cloud Run Job, Airflow, Workflows), call maintenance endpoints around a bulk
 * load (create index, disable refresh, commit, alias swap), and start an asynchronous API job
 * then poll its status until done ({@code poll}).
 *
 * <p>Shares {@code target} / {@code body} / {@code response} / {@code timeout} / {@code http}
 * with the http sink ({@link RequestSpec}, {@link ResponsePolicy}, {@link AuthProvider}).
 * Elements by trigger: once → none (templates see only {@code utils.*} and {@code __timestamp}),
 * perElement → the element's fields, collect → {@code elements} / {@code size}.
 *
 * <p>Not idempotent by itself: a retried bundle re-sends the request. Prefer idempotent endpoints
 * (PUT, or an {@code Idempotency-Key} header built from {@code utils.string.sha256}).
 */
@Action.Service(name = "http")
public class HttpAction implements Action {

    private static final Logger LOG = LoggerFactory.getLogger(HttpAction.class);

    public static class Parameters implements Serializable {

        public RequestSpec.Target target;
        public RequestSpec.Body body;
        public ResponsePolicy.Parameters response;
        public HttpTransport.TimeoutParameters timeout;
        public HttpTransport.Parameters http;
        public Poll poll;

        List<String> validate(final String name, final Schema inputSchema) {
            final List<String> errorMessages = new ArrayList<>();
            final String prefix = "action module[" + name + "].parameters";
            if(target == null) {
                errorMessages.add(prefix + ".target must not be null");
            } else {
                errorMessages.addAll(target.validate(prefix + ".target", inputSchema, http != null && http.allowedHosts != null));
            }
            if(body != null) {
                errorMessages.addAll(body.validate(prefix + ".body", inputSchema));
            }
            if(response != null) {
                errorMessages.addAll(response.validate(prefix + ".response"));
                if(response.partialFailure != null) {
                    errorMessages.add(prefix + ".response.partialFailure is not supported by action.http (use the http sink with batch)");
                }
            }
            if(timeout != null) {
                errorMessages.addAll(timeout.validate(prefix + ".timeout"));
            }
            if(http != null) {
                errorMessages.addAll(http.validate(prefix + ".http"));
            }
            if(poll != null) {
                errorMessages.addAll(poll.validate(prefix + ".poll"));
            }
            return errorMessages;
        }

        void setDefaults() {
            target.setDefaults();
            if(body == null) {
                body = new RequestSpec.Body();
            }
            body.setDefaults();
            if(response == null) {
                response = new ResponsePolicy.Parameters();
            }
            response.setDefaults();
            if(timeout == null) {
                timeout = new HttpTransport.TimeoutParameters();
            }
            timeout.setDefaults();
            if(http == null) {
                http = new HttpTransport.Parameters();
            }
            http.setDefaults();
            if(http.allowedHosts == null && !target.auth.isNone()) {
                final String origin = RequestSpec.staticOrigin(target.url);
                if(origin != null) {
                    final List<String> hosts = new ArrayList<>();
                    hosts.add(URI.create(origin).getHost());
                    if(poll != null && poll.url != null) {
                        final String pollOrigin = RequestSpec.staticOrigin(poll.url);
                        if(pollOrigin != null && !hosts.contains(URI.create(pollOrigin).getHost())) {
                            hosts.add(URI.create(pollOrigin).getHost());
                        }
                    }
                    http.allowedHosts = hosts;
                }
            }
            if(poll != null) {
                poll.setDefaults();
            }
        }
    }

    /** Poll a status endpoint after the request until a terminal condition holds. */
    public static class Poll implements Serializable {

        public String url;
        public String method;
        public Map<String, String> headers;
        public JsonElement until;
        public JsonElement failWhen;
        public String interval;
        public String timeout;
        public String untilJson;
        public String failWhenJson;

        List<String> validate(final String prefix) {
            final List<String> errorMessages = new ArrayList<>();
            if(url == null) {
                errorMessages.add(prefix + ".url must not be null");
            }
            if(until == null && untilJson == null) {
                errorMessages.add(prefix + ".until must not be null");
            }
            for(final Map.Entry<String, String> e : Map.of("interval", interval == null ? "" : interval, "timeout", timeout == null ? "" : timeout).entrySet()) {
                if(!e.getValue().isEmpty()) {
                    try {
                        Durations.parse(e.getValue());
                    } catch (final IllegalArgumentException ex) {
                        errorMessages.add(prefix + "." + e.getKey() + " is illegal: " + ex.getMessage());
                    }
                }
            }
            return errorMessages;
        }

        void setDefaults() {
            if(method == null) {
                method = "GET";
            }
            method = method.toUpperCase();
            if(headers == null) {
                headers = new HashMap<>();
            }
            if(interval == null) {
                interval = "10s";
            }
            if(timeout == null) {
                timeout = "1h";
            }
            if(until != null) {
                untilJson = until.toString();
                until = null;
            }
            if(failWhen != null) {
                failWhenJson = failWhen.toString();
                failWhen = null;
            }
        }
    }

    private String name;
    private Trigger trigger;
    private Parameters parameters;
    private Schema inputSchema;
    private RequestRenderer renderer;
    private ResponsePolicy policy;

    private transient HttpTransport transport;
    private transient Template pollUrlTemplate;
    private transient Map<String, Template> pollHeaderTemplates;
    private transient Filter.ConditionNode untilCondition;
    private transient Filter.ConditionNode failWhenCondition;

    @Override
    public void configure(final String name, final JsonObject parametersJson, final PipelineOptions options) {
        configure(name, parametersJson, options, null);
    }

    @Override
    public void configure(final String name, final JsonObject parametersJson, final PipelineOptions options, final Schema inputSchema) {
        this.name = name;
        this.trigger = Trigger.of(parametersJson);
        this.parameters = new Gson().fromJson(parametersJson, Parameters.class);
        if(this.parameters == null) {
            throw new IllegalModuleException("action module[" + name + "].parameters must not be empty");
        }
        this.inputSchema = inputSchema;
        final List<String> errorMessages = this.parameters.validate(name, inputSchema);
        if(!errorMessages.isEmpty()) {
            throw new IllegalModuleException(errorMessages);
        }
        this.parameters.setDefaults();
        this.renderer = new RequestRenderer(name, parameters.target, parameters.body, null,
                Trigger.collect.equals(trigger), inputSchema, inputSchema, List.of());
        this.policy = new ResponsePolicy(parameters.response);
    }

    @Override
    public void setup() {
        renderer.setup();
        policy.setup();
        final AuthProvider auth = AuthProvider.create(parameters.target.auth, RequestSpec.staticOrigin(parameters.target.url));
        this.transport = new HttpTransport(parameters.http, parameters.timeout, auth);
        if(parameters.poll != null) {
            this.pollUrlTemplate = TemplateUtil.createStrictTemplate(name + ".poll.url", parameters.poll.url);
            this.pollHeaderTemplates = new HashMap<>();
            for(final Map.Entry<String, String> entry : parameters.poll.headers.entrySet()) {
                pollHeaderTemplates.put(entry.getKey(), TemplateUtil.createStrictTemplate(name + ".poll.header." + entry.getKey(), entry.getValue()));
            }
            this.untilCondition = Filter.parse(parameters.poll.untilJson);
            this.failWhenCondition = parameters.poll.failWhenJson == null ? null : Filter.parse(parameters.poll.failWhenJson);
        }
    }

    @Override
    public ActionResult execute(final List<MElement> elements) throws Exception {
        final Map<String, Object> values = switch (trigger) {
            case once -> renderer.createTemplateValues((Map<String, Object>) null);
            case perElement -> renderer.createTemplateValues(elements.getFirst());
            case collect -> renderer.createTemplateValues(elements, null);
        };
        final OutboundRequest request = renderer.build(elements, values);
        LOG.info("action[{}] {} {}", name, request.method(), request.url());
        final SyncCaller.Result result = SyncCaller.call(name, transport, policy, request);
        if(!result.succeeded()) {
            throw new IllegalStateException("action[" + name + "] request " + request.method() + " " + request.url() + " failed: " + result.error());
        }
        if(parameters.poll == null) {
            return ActionResult.of(request.method(), request.url(), "SUCCEEDED", payloadJson(result));
        }
        return poll(values, result);
    }

    private ActionResult poll(final Map<String, Object> values, final SyncCaller.Result first) throws Exception {
        final Map<String, Object> pollValues = new HashMap<>(values);
        pollValues.putAll(responseValues(first));
        final String url = TemplateUtil.executeStrictTemplate(pollUrlTemplate, pollValues);
        final Map<String, String> headers = new LinkedHashMap<>();
        for(final Map.Entry<String, Template> entry : pollHeaderTemplates.entrySet()) {
            headers.put(entry.getKey(), TemplateUtil.executeStrictTemplate(entry.getValue(), pollValues));
        }
        final OutboundRequest pollRequest = new OutboundRequest(url, parameters.poll.method, headers, null, 0);
        final Duration interval = Durations.parse(parameters.poll.interval);
        final Instant deadline = Instant.now().plus(Durations.parse(parameters.poll.timeout));
        int polls = 0;
        while(true) {
            polls++;
            final SyncCaller.Result result = SyncCaller.call(name, transport, policy, pollRequest);
            if(!result.succeeded()) {
                throw new IllegalStateException("action[" + name + "] poll " + url + " failed: " + result.error());
            }
            final Map<String, Object> conditionValues = result.parsed().values();
            if(failWhenCondition != null && Filter.filter(failWhenCondition, conditionValues)) {
                throw new IllegalStateException("action[" + name + "] poll " + url + " reported failure: " + SyncCaller.abbreviate(result.parsed().text()));
            }
            if(Filter.filter(untilCondition, conditionValues)) {
                LOG.info("action[{}] poll {} completed after {} poll(s)", name, url, polls);
                return ActionResult.of(first.request().method(), url, "SUCCEEDED", payloadJson(result));
            }
            if(Instant.now().plus(interval).isAfter(deadline)) {
                throw new IllegalStateException("action[" + name + "] poll " + url + " timed out after " + parameters.poll.timeout + " (" + polls + " polls)");
            }
            Thread.sleep(interval.toMillis());
        }
    }

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

    /** Envelope payload: the final response as JSON (status, json body or text, attempts). */
    private static String payloadJson(final SyncCaller.Result result) {
        final JsonObject json = new JsonObject();
        json.addProperty("statusCode", result.response().statusCode());
        json.addProperty("attempts", result.attempts());
        final Object raw = result.parsed().values().get("json");
        if(raw instanceof JsonElement e) {
            json.add("body", e);
        } else {
            json.addProperty("body", result.parsed().text());
        }
        return json.toString();
    }
}
