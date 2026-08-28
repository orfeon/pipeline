package com.mercari.solution.module.action;

import com.google.api.client.util.BackOff;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.Sleeper;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.mercari.solution.module.MElement;
import com.mercari.solution.util.TemplateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * Helpers shared by the action services that drive long-running cloud jobs
 * (bigquery / dataflow / build / tasks): template expansion over JSON trees, id collection for
 * {@code collect}-trigger waits, the poll backoff and the multi-id wait loop, and the
 * {@code memory://} client registry used by the tests.
 */
final class ActionSupport {

    private static final Logger LOG = LoggerFactory.getLogger(ActionSupport.class);

    /** Endpoint prefix selecting a client registered with a {@link MemoryClients} registry (tests). */
    static final String ENDPOINT_MEMORY_PREFIX = "memory://";

    private static final int POLL_INITIAL_INTERVAL_MILLIS = 2000;
    private static final int POLL_MAX_INTERVAL_MILLIS = 30000;

    private ActionSupport() {}

    /** Expands every string leaf of the JSON tree as a strict FreeMarker template with the element data. */
    static JsonElement templateJson(final JsonElement json, final Map<String, Object> data) {
        return templateJson(json, text -> TemplateUtil.executeStrictTemplateIfNeeded(text, data));
    }

    /** Applies {@code template} to every string leaf of the JSON tree (objects / arrays recursed, other primitives kept). */
    static JsonElement templateJson(final JsonElement json, final UnaryOperator<String> template) {
        if(json == null || json.isJsonNull()) {
            return json;
        } else if(json.isJsonPrimitive()) {
            final JsonPrimitive primitive = json.getAsJsonPrimitive();
            if(primitive.isString()) {
                return new JsonPrimitive(template.apply(primitive.getAsString()));
            }
            return primitive;
        } else if(json.isJsonArray()) {
            final JsonArray array = new JsonArray();
            for(final JsonElement e : json.getAsJsonArray()) {
                array.add(templateJson(e, template));
            }
            return array;
        } else {
            final JsonObject object = new JsonObject();
            for(final Map.Entry<String, JsonElement> entry : json.getAsJsonObject().entrySet()) {
                object.add(entry.getKey(), templateJson(entry.getValue(), template));
            }
            return object;
        }
    }

    /** Distinct, non-blank values of {@code field} over the collected elements, in first-seen order. */
    static List<String> collectField(final List<MElement> elements, final String field) {
        final LinkedHashSet<String> values = new LinkedHashSet<>();
        for(final MElement element : elements) {
            final Object value = element.getPrimitiveValue(field);
            if(value != null && !value.toString().isBlank()) {
                values.add(value.toString());
            }
        }
        return new ArrayList<>(values);
    }

    /** Poll backoff (2s → 30s) whose elapsed-time limit is the action's {@code timeoutSeconds}. */
    static ExponentialBackOff createPollBackOff(final long timeoutSeconds) {
        return new ExponentialBackOff.Builder()
                .setInitialIntervalMillis(POLL_INITIAL_INTERVAL_MILLIS)
                .setMaxIntervalMillis(POLL_MAX_INTERVAL_MILLIS)
                .setMaxElapsedTimeMillis(Math.toIntExact(Math.min(timeoutSeconds * 1000L, Integer.MAX_VALUE)))
                .build();
    }

    /**
     * One poll of one id. Returns the resource when the id reached the wait target, {@code null}
     * while it is still pending. Throw {@link NonRetryableException} (or any other exception the
     * loop's {@code isTransient} rejects) to fail the firing; a transient exception is logged and
     * the id is polled again after the next backoff, inside the same timeout window.
     */
    @FunctionalInterface
    interface Poll<T> {
        T poll(String id) throws Exception;
    }

    /**
     * Waits for several ids at once: one poll loop over the still-pending set with a shared backoff
     * and a single {@code timeoutSeconds} window (not one full wait per id). Results are returned in
     * the order of {@code ids}. On timeout {@code onTimeout} (cancel pending jobs, ...) runs with
     * the pending ids, then the exception from {@code timedOut} is thrown.
     *
     * @param label       resource label for the log lines ("bigquery jobs", "cloud builds", ...)
     * @param isTransient classifies a poll exception as retryable within the loop
     */
    static <T> List<T> waitForAll(
            final String name,
            final String label,
            final List<String> ids,
            final long timeoutSeconds,
            final Sleeper sleeper,
            final Poll<T> poll,
            final Predicate<Exception> isTransient,
            final java.util.function.Consumer<List<String>> onTimeout,
            final Function<List<String>, NonRetryableException> timedOut) throws Exception {

        final Map<String, T> completed = new LinkedHashMap<>();
        final ExponentialBackOff backOff = createPollBackOff(timeoutSeconds);
        while(true) {
            for(final String id : ids) {
                if(completed.containsKey(id)) {
                    continue;
                }
                final T result;
                try {
                    result = poll.poll(id);
                } catch (final NonRetryableException e) {
                    throw e;
                } catch (final Exception e) {
                    if(!isTransient.test(e)) {
                        throw e;
                    }
                    // transient poll error: keep the completed set and the shared timeout window, retry after the backoff
                    LOG.info("action module[{}] failed to poll {}: {} ({}), retrying", name, label, id, e.getMessage());
                    continue;
                }
                if(result != null) {
                    completed.put(id, result);
                }
            }
            if(completed.size() == ids.size()) {
                return ids.stream().map(completed::get).toList();
            }
            final long next = backOff.nextBackOffMillis();
            if(next == BackOff.STOP) {
                final List<String> pending = ids.stream().filter(id -> !completed.containsKey(id)).toList();
                if(onTimeout != null) {
                    onTimeout.accept(pending);
                }
                throw timedOut.apply(pending);
            }
            LOG.info("action module[{}] waiting for {}: {}/{} done", name, label, completed.size(), ids.size());
            sleeper.sleep(next);
        }
    }

    /**
     * Registry of in-memory clients keyed by the {@code memory://<name>} endpoint, for tests that
     * fake the cloud API. {@link #resolve} returns the registered client for a memory endpoint and
     * {@code null} for every other endpoint (the caller then builds the real client).
     */
    static final class MemoryClients<C> {

        private final String label;
        private final Map<String, C> clients = new HashMap<>();

        MemoryClients(final String label) {
            this.label = label;
        }

        void register(final String name, final C client) {
            synchronized (clients) {
                clients.put(name, client);
            }
        }

        void unregister(final String name) {
            synchronized (clients) {
                clients.remove(name);
            }
        }

        C resolve(final String endpoint) {
            if(endpoint == null || !endpoint.startsWith(ENDPOINT_MEMORY_PREFIX)) {
                return null;
            }
            final String name = endpoint.substring(ENDPOINT_MEMORY_PREFIX.length());
            synchronized (clients) {
                final C client = clients.get(name);
                if(client == null) {
                    throw new IllegalStateException("in-memory " + label + " client is not registered: " + name);
                }
                return client;
            }
        }
    }

}
