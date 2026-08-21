package com.mercari.solution.util.pipeline.outbound;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.domain.file.JsonUtil;
import com.mercari.solution.util.pipeline.Filter;
import com.mercari.solution.util.schema.converter.JsonToElementConverter;
import com.mercari.solution.util.schema.converter.JsonToMapConverter;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Declarative classification of HTTP responses: success / retry / failed, plus per-item partial
 * failure for bulk APIs. Pure (no I/O) so it is unit-testable; compile with {@link #setup()} on the
 * worker.
 */
public class ResponsePolicy implements Serializable {

    public enum Format {
        text,
        json,
        bytes,
        none
    }

    public enum Verdict {
        SUCCESS,
        RETRY,
        FAILED
    }

    public static class Parameters implements Serializable {

        public Format format;
        public JsonElement schema;
        public String schemaJson;
        public Success success;
        public Retry retry;
        public PartialFailure partialFailure;

        public List<String> validate(final String prefix) {
            final List<String> errorMessages = new ArrayList<>();
            if(schema != null && !schema.isJsonObject()) {
                errorMessages.add(prefix + ".schema must be an object");
            }
            if(success != null && success.statusCodes != null) {
                for(final Integer code : success.statusCodes) {
                    if(code == null || code < 100 || code > 599) {
                        errorMessages.add(prefix + ".success.statusCodes must be between 100 and 599 but: " + code);
                    }
                }
            }
            if(retry != null) {
                errorMessages.addAll(retry.validate(prefix + ".retry"));
            }
            if(partialFailure != null) {
                errorMessages.addAll(partialFailure.validate(prefix + ".partialFailure"));
            }
            return errorMessages;
        }

        /** Also moves Gson JsonElements into String fields so the parameters are Java-serializable. */
        public void setDefaults() {
            if(format == null) {
                format = Format.json;
            }
            if(schema != null) {
                schemaJson = schema.toString();
                schema = null;
            }
            if(success == null) {
                success = new Success();
            }
            success.setDefaults();
            if(retry == null) {
                retry = new Retry();
            }
            retry.setDefaults();
            if(partialFailure != null) {
                partialFailure.setDefaults();
            }
        }
    }

    public static class Success implements Serializable {
        public List<Integer> statusCodes;
        public JsonElement condition;
        public String conditionJson;

        void setDefaults() {
            if(condition != null) {
                conditionJson = condition.toString();
                condition = null;
            }
        }
    }

    public static class Retry implements Serializable {
        public List<Integer> statusCodes;
        public JsonElement condition;
        public String conditionJson;
        public Boolean respectRetryAfter;
        public Integer maxAttempts;
        public String initialBackoff;
        public String maxBackoff;
        public String totalTimeout;

        List<String> validate(final String prefix) {
            final List<String> errorMessages = new ArrayList<>();
            if(maxAttempts != null && maxAttempts < 1) {
                errorMessages.add(prefix + ".maxAttempts must be >= 1 but: " + maxAttempts);
            }
            for(final Map.Entry<String, String> e : Map.of(
                    "initialBackoff", initialBackoff == null ? "" : initialBackoff,
                    "maxBackoff", maxBackoff == null ? "" : maxBackoff,
                    "totalTimeout", totalTimeout == null ? "" : totalTimeout).entrySet()) {
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
            if(condition != null) {
                conditionJson = condition.toString();
                condition = null;
            }
            if(statusCodes == null) {
                statusCodes = List.of(408, 425, 429, 500, 502, 503, 504);
            }
            if(respectRetryAfter == null) {
                respectRetryAfter = true;
            }
            if(maxAttempts == null) {
                maxAttempts = 5;
            }
            if(initialBackoff == null) {
                initialBackoff = "1s";
            }
            if(maxBackoff == null) {
                maxBackoff = "30s";
            }
            if(totalTimeout == null) {
                totalTimeout = "5m";
            }
        }
    }

    public static class PartialFailure implements Serializable {
        public String itemsPath;
        public JsonElement errorCondition;
        public JsonElement retryCondition;
        public String errorConditionJson;
        public String retryConditionJson;

        List<String> validate(final String prefix) {
            final List<String> errorMessages = new ArrayList<>();
            if(itemsPath == null) {
                errorMessages.add(prefix + ".itemsPath must not be null");
            } else if(!itemsPath.startsWith("/")) {
                errorMessages.add(prefix + ".itemsPath must be a JSON pointer starting with / but: " + itemsPath);
            }
            if(errorCondition == null) {
                errorMessages.add(prefix + ".errorCondition must not be null");
            }
            return errorMessages;
        }

        void setDefaults() {
            if(errorCondition != null) {
                errorConditionJson = errorCondition.toString();
                errorCondition = null;
            }
            if(retryCondition != null) {
                retryConditionJson = retryCondition.toString();
                retryCondition = null;
            }
        }
    }

    /** Result of parsing one response body according to {@code format} / {@code schema}. */
    public record Parsed(String text, byte[] bytes, Object payload, Map<String, Object> values) {}

    /** Outcome for one bulk item. */
    public record ItemVerdict(int index, Verdict verdict, String error) {}

    private final Parameters parameters;
    private final Schema schema;

    private transient Filter.ConditionNode successCondition;
    private transient Filter.ConditionNode retryCondition;
    private transient Filter.ConditionNode itemErrorCondition;
    private transient Filter.ConditionNode itemRetryCondition;
    private transient Set<Integer> successCodes;
    private transient Set<Integer> retryCodes;
    private transient Duration initialBackoff;
    private transient Duration maxBackoff;
    private transient Duration totalTimeout;

    public ResponsePolicy(final Parameters parameters) {
        this.parameters = parameters;
        this.schema = parameters.schemaJson == null ? null : Schema.parse(parameters.schemaJson);
    }

    public Parameters parameters() {
        return parameters;
    }

    public Schema schema() {
        return schema;
    }

    public Format format() {
        return parameters.format;
    }

    public int maxAttempts() {
        return parameters.retry.maxAttempts;
    }

    public Duration totalTimeout() {
        return totalTimeout;
    }

    public boolean hasPartialFailure() {
        return parameters.partialFailure != null;
    }

    public void setup() {
        this.successCondition = parameters.success.conditionJson == null ? null : Filter.parse(parameters.success.conditionJson);
        this.retryCondition = parameters.retry.conditionJson == null ? null : Filter.parse(parameters.retry.conditionJson);
        this.successCodes = parameters.success.statusCodes == null ? null : Set.copyOf(parameters.success.statusCodes);
        this.retryCodes = Set.copyOf(parameters.retry.statusCodes);
        this.initialBackoff = Durations.parse(parameters.retry.initialBackoff);
        this.maxBackoff = Durations.parse(parameters.retry.maxBackoff);
        this.totalTimeout = Durations.parse(parameters.retry.totalTimeout);
        if(parameters.partialFailure != null) {
            this.itemErrorCondition = Filter.parse(parameters.partialFailure.errorConditionJson);
            this.itemRetryCondition = parameters.partialFailure.retryConditionJson == null ? null : Filter.parse(parameters.partialFailure.retryConditionJson);
        }
        if(schema != null) {
            schema.setup();
        }
    }

    /** Parses the body according to format (and schema). Never throws: unparsable JSON leaves payload null. */
    public Parsed parse(final OutboundRequest.Response response) {
        final byte[] body = response.body();
        final Map<String, Object> values = new HashMap<>();
        values.put("statusCode", response.statusCode());
        String text = null;
        byte[] bytes = null;
        Object payload = null;
        switch (parameters.format) {
            case none -> {}
            case bytes -> bytes = body;
            case text -> text = HttpTransport.text(body);
            case json -> {
                text = HttpTransport.text(body);
                JsonElement json = null;
                if(text != null && !text.isBlank()) {
                    try {
                        json = JsonUtil.fromJson(text, JsonElement.class);
                    } catch (final RuntimeException e) {
                        json = null;
                    }
                }
                if(json != null) {
                    if(schema != null && json.isJsonObject()) {
                        payload = JsonToElementConverter.convert(schema.getFields(), json.getAsJsonObject());
                    } else if(json.isJsonObject()) {
                        payload = JsonToMapConverter.convert(json);
                    } else if(json.isJsonArray()) {
                        final List<Object> list = new ArrayList<>();
                        for(final JsonElement e : json.getAsJsonArray()) {
                            list.add(e.isJsonObject() ? JsonToMapConverter.convert(e) : JsonToMapConverter.getAsPrimitiveValue(Schema.FieldType.STRING, e));
                        }
                        payload = list;
                    } else if(json.isJsonPrimitive()) {
                        payload = json.getAsString();
                    }
                    values.put("json", json);
                }
            }
        }
        values.put("body", text);
        values.put("payload", payload);
        return new Parsed(text, bytes, payload, values);
    }

    /** Classifies one attempt. */
    public Verdict classify(final OutboundRequest.Response response, final Parsed parsed) {
        final int code = response.statusCode();
        final boolean codeOk = successCodes != null ? successCodes.contains(code) : (code / 100 == 2);
        if(codeOk) {
            if(successCondition != null && !Filter.filter(successCondition, parsed.values())) {
                if(retryCondition != null && Filter.filter(retryCondition, parsed.values())) {
                    return Verdict.RETRY;
                }
                return Verdict.FAILED;
            }
            return Verdict.SUCCESS;
        }
        if(retryCodes.contains(code)) {
            return Verdict.RETRY;
        }
        if(retryCondition != null && Filter.filter(retryCondition, parsed.values())) {
            return Verdict.RETRY;
        }
        return Verdict.FAILED;
    }

    /** True when an exception (connect error, timeout) should be retried — always, bounded by attempts/timeout. */
    public boolean isRetryable(final Throwable e) {
        return true;
    }

    /**
     * Backoff before attempt {@code nextAttempt} (1-based count of attempts already made). Honors
     * Retry-After when present and enabled; exponential with full jitter otherwise. Returns null
     * when attempts or the total timeout are exhausted.
     */
    public Duration backoff(final int attemptsMade, final OutboundRequest.Response response, final Instant startedAt) {
        if(attemptsMade >= parameters.retry.maxAttempts) {
            return null;
        }
        Duration delay = null;
        if(response != null && parameters.retry.respectRetryAfter) {
            delay = parseRetryAfter(response.header("Retry-After"));
        }
        if(delay == null) {
            final long base = initialBackoff.toMillis() * (1L << Math.min(attemptsMade - 1, 20));
            final long capped = Math.min(base, maxBackoff.toMillis());
            delay = Duration.ofMillis(capped <= 0 ? 0 : ThreadLocalRandom.current().nextLong(capped / 2, capped + 1));
        }
        if(delay.compareTo(maxBackoff) > 0) {
            delay = maxBackoff;
        }
        if(Instant.now().plus(delay).isAfter(startedAt.plus(totalTimeout))) {
            return null;
        }
        return delay;
    }

    static Duration parseRetryAfter(final String value) {
        if(value == null || value.isBlank()) {
            return null;
        }
        final String v = value.trim();
        if(v.matches("^\\d+$")) {
            return Duration.ofSeconds(Long.parseLong(v));
        }
        try {
            final ZonedDateTime at = ZonedDateTime.parse(v, DateTimeFormatter.RFC_1123_DATE_TIME);
            final Duration d = Duration.between(Instant.now(), at.toInstant());
            return d.isNegative() ? Duration.ZERO : d;
        } catch (final RuntimeException e) {
            return null;
        }
    }

    /**
     * Evaluates partial failure for a bulk response. Returns one verdict per item in request order,
     * or null when the response does not carry an item array of the expected size (caller treats
     * the whole batch by the top-level verdict).
     */
    public List<ItemVerdict> items(final Parsed parsed, final int expectedSize) {
        if(parameters.partialFailure == null || parsed.values().get("json") == null) {
            return null;
        }
        final JsonElement items = pointer((JsonElement) parsed.values().get("json"), parameters.partialFailure.itemsPath);
        if(items == null || !items.isJsonArray()) {
            return null;
        }
        final JsonArray array = items.getAsJsonArray();
        if(array.size() != expectedSize) {
            return null;
        }
        final List<ItemVerdict> verdicts = new ArrayList<>(array.size());
        for(int i = 0; i < array.size(); i++) {
            final JsonElement item = array.get(i);
            final Map<String, Object> values = item.isJsonObject()
                    ? JsonToMapConverter.convert(item)
                    : Map.of("value", JsonToMapConverter.getAsPrimitiveValue(Schema.FieldType.STRING, item));
            if(itemRetryCondition != null && Filter.filter(itemRetryCondition, values)) {
                verdicts.add(new ItemVerdict(i, Verdict.RETRY, item.toString()));
            } else if(Filter.filter(itemErrorCondition, values)) {
                verdicts.add(new ItemVerdict(i, Verdict.FAILED, item.toString()));
            } else {
                verdicts.add(new ItemVerdict(i, Verdict.SUCCESS, null));
            }
        }
        return verdicts;
    }

    /** Minimal JSON pointer (RFC 6901) over Gson trees. */
    static JsonElement pointer(final JsonElement root, final String pointer) {
        if(pointer == null || pointer.isEmpty() || pointer.equals("/")) {
            return root;
        }
        JsonElement current = root;
        for(final String rawToken : pointer.substring(1).split("/")) {
            final String token = rawToken.replace("~1", "/").replace("~0", "~");
            if(current == null) {
                return null;
            }
            if(current.isJsonObject()) {
                final JsonObject o = current.getAsJsonObject();
                current = o.has(token) ? o.get(token) : null;
            } else if(current.isJsonArray()) {
                try {
                    final int i = Integer.parseInt(token);
                    final JsonArray a = current.getAsJsonArray();
                    current = i >= 0 && i < a.size() ? a.get(i) : null;
                } catch (final NumberFormatException e) {
                    return null;
                }
            } else {
                return null;
            }
        }
        return current;
    }
}
