package com.mercari.solution.server.agent;

import com.google.gson.Gson;
import dev.langchain4j.service.tool.ToolExecution;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Structured logging of Pipeline Builder agent interactions: one event per line as
 * single-line JSON on stdout. On Cloud Run each line becomes a Cloud Logging entry with
 * these fields under jsonPayload, so conversations can be exported to BigQuery and
 * reassembled with:
 *
 *   SELECT * FROM ...
 *   WHERE jsonPayload.log_type = 'pipeline_agent'
 *     AND jsonPayload.conversation_id = '...'
 *   ORDER BY timestamp
 *
 * Schema rules kept stable on purpose (BigQuery log exports infer column types from the
 * first entries and reject type flips): content-bearing fields are always strings,
 * counters/durations are always numbers, and every event carries the same envelope
 * (log_type / event / conversation_id / request_id / model / turn).
 *
 * Events: user_message, tool_execution, assistant_message, completion, error.
 */
public class AgentInteractionLogger {

    private static final Gson GSON = new Gson();
    private static final String LOG_TYPE = "pipeline_agent";
    // Cloud Logging caps an entry at 256KB; leave ample headroom for the envelope.
    private static final int MAX_FIELD_CHARS = 60_000;

    private final String conversationId;
    private final String requestId;
    private final String model;
    private final int turn;
    private final long startMillis;
    private final AtomicInteger toolExecutionCount = new AtomicInteger();
    private final AtomicInteger assistantMessageCount = new AtomicInteger();

    public AgentInteractionLogger(final String conversationId, final String model, final int turn) {
        this.conversationId = conversationId == null || conversationId.isBlank()
                ? UUID.randomUUID().toString() : conversationId;
        this.requestId = UUID.randomUUID().toString();
        this.model = model == null ? "" : model;
        this.turn = turn;
        this.startMillis = System.currentTimeMillis();
    }

    public void userMessage(final String content, final String canvasConfig) {
        final Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("content", truncate(content));
        fields.put("canvas_config", truncate(canvasConfig));
        emit("INFO", "user_message", "pipeline-agent user_message", fields);
    }

    public void toolExecution(final ToolExecution execution) {
        toolExecutionCount.incrementAndGet();
        final Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("tool_id", execution.request().id());
        fields.put("tool_name", execution.request().name());
        fields.put("tool_arguments", truncate(execution.request().arguments()));
        fields.put("tool_result", truncate(execution.result()));
        fields.put("tool_failed", execution.hasFailed());
        fields.put("duration_ms", execution.duration().toMillis());
        emit(execution.hasFailed() ? "WARNING" : "INFO", "tool_execution",
                "pipeline-agent tool " + execution.request().name(), fields);
    }

    public void assistantMessage(final String content) {
        assistantMessageCount.incrementAndGet();
        final Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("content", truncate(content));
        emit("INFO", "assistant_message", "pipeline-agent assistant_message", fields);
    }

    public void completion() {
        final long durationMillis = System.currentTimeMillis() - startMillis;
        final Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("duration_ms", durationMillis);
        fields.put("tool_execution_count", toolExecutionCount.get());
        fields.put("assistant_message_count", assistantMessageCount.get());
        emit("INFO", "completion", "pipeline-agent completion ("
                + toolExecutionCount.get() + " tools, " + durationMillis + "ms)", fields);
    }

    public void error(final Throwable e) {
        final Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("duration_ms", System.currentTimeMillis() - startMillis);
        fields.put("tool_execution_count", toolExecutionCount.get());
        fields.put("error", truncate(String.valueOf(e)));
        emit("ERROR", "error", "pipeline-agent error: " + e.getMessage(), fields);
    }

    private void emit(final String severity, final String event, final String message,
                      final Map<String, Object> fields) {
        final Map<String, Object> line = new LinkedHashMap<>();
        line.put("time", Instant.now().toString());
        line.put("severity", severity);
        line.put("message", message);
        line.put("log_type", LOG_TYPE);
        line.put("event", event);
        line.put("conversation_id", conversationId);
        line.put("request_id", requestId);
        line.put("model", model);
        line.put("turn", turn);
        line.putAll(fields);
        // Gson escapes newlines inside strings, so this is guaranteed to be a single line —
        // the format Cloud Run ingests as one structured (jsonPayload) log entry.
        System.out.println(GSON.toJson(line));
    }

    private static String truncate(final String text) {
        if (text == null) {
            return "";
        }
        if (text.length() <= MAX_FIELD_CHARS) {
            return text;
        }
        return text.substring(0, MAX_FIELD_CHARS) + "...(truncated, " + text.length() + " chars)";
    }

}
