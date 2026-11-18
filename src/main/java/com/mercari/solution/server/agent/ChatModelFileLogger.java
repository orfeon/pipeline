package com.mercari.solution.server.agent;

import com.google.gson.Gson;
import dev.langchain4j.agent.tool.ToolExecutionRequest;
import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.data.message.SystemMessage;
import dev.langchain4j.data.message.ToolExecutionResultMessage;
import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.model.ModelProvider;
import dev.langchain4j.model.chat.Capability;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.chat.ChatRequestOptions;
import dev.langchain4j.model.chat.listener.ChatModelListener;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.chat.request.ChatRequestParameters;
import dev.langchain4j.model.chat.response.ChatResponse;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Diagnostic ChatModel decorator that appends one JSON line per request/response to a file,
 * so model interactions can be grepped even when the server logs only to the console.
 * Enabled via the MERCARI_PIPELINE_AGENT_DEBUG_LOG env var (see AgentService).
 *
 * The main purpose is to verify Gemini thought-signature round-trips: AiMessage attributes
 * (where langchain4j stores "thought_signature_&lt;toolCallId&gt;") are logged as key -&gt; value
 * length for both outgoing request messages and incoming responses.
 */
public class ChatModelFileLogger implements ChatModel {

    private static final Gson GSON = new Gson();
    private static final AtomicLong SEQ = new AtomicLong();

    private final ChatModel delegate;
    private final Path logFile;

    public ChatModelFileLogger(final ChatModel delegate, final Path logFile) {
        this.delegate = delegate;
        this.logFile = logFile;
    }

    @Override
    public ChatResponse chat(final ChatRequest request) {
        final long seq = SEQ.incrementAndGet();
        writeLine(describeRequest(seq, request));
        try {
            final ChatResponse response = delegate.chat(request);
            writeLine(describeResponse(seq, response));
            return response;
        } catch (final RuntimeException e) {
            writeLine(Map.of("seq", seq, "phase", "error", "error", String.valueOf(e)));
            throw e;
        }
    }

    @Override
    public ChatResponse chat(final ChatRequest request, final ChatRequestOptions options) {
        final long seq = SEQ.incrementAndGet();
        writeLine(describeRequest(seq, request));
        try {
            final ChatResponse response = delegate.chat(request, options);
            writeLine(describeResponse(seq, response));
            return response;
        } catch (final RuntimeException e) {
            writeLine(Map.of("seq", seq, "phase", "error", "error", String.valueOf(e)));
            throw e;
        }
    }

    @Override
    public ChatRequestParameters defaultRequestParameters() {
        return delegate.defaultRequestParameters();
    }

    @Override
    public List<ChatModelListener> listeners() {
        return delegate.listeners();
    }

    @Override
    public ModelProvider provider() {
        return delegate.provider();
    }

    @Override
    public Set<Capability> supportedCapabilities() {
        return delegate.supportedCapabilities();
    }

    private Map<String, Object> describeRequest(final long seq, final ChatRequest request) {
        final Map<String, Object> line = new LinkedHashMap<>();
        line.put("ts", Instant.now().toString());
        line.put("seq", seq);
        line.put("phase", "request");
        line.put("model", request.modelName());
        final List<Map<String, Object>> messages = new ArrayList<>();
        for (final ChatMessage message : request.messages()) {
            messages.add(describeMessage(message));
        }
        line.put("messages", messages);
        if (request.parameters() != null && request.parameters().responseFormat() != null) {
            line.put("responseFormat", String.valueOf(request.parameters().responseFormat().type()));
        }
        return line;
    }

    private Map<String, Object> describeResponse(final long seq, final ChatResponse response) {
        final Map<String, Object> line = new LinkedHashMap<>();
        line.put("ts", Instant.now().toString());
        line.put("seq", seq);
        line.put("phase", "response");
        line.put("aiMessage", describeMessage(response.aiMessage()));
        if (response.finishReason() != null) {
            line.put("finishReason", response.finishReason().name());
        }
        if (response.tokenUsage() != null) {
            line.put("tokenUsage", String.valueOf(response.tokenUsage()));
        }
        return line;
    }

    private Map<String, Object> describeMessage(final ChatMessage message) {
        final Map<String, Object> map = new LinkedHashMap<>();
        switch (message) {
            case SystemMessage system -> {
                map.put("role", "system");
                map.put("textLength", system.text() == null ? 0 : system.text().length());
            }
            case UserMessage user -> {
                map.put("role", "user");
                map.put("text", head(user.singleText()));
            }
            case AiMessage ai -> {
                map.put("role", "ai");
                if (ai.text() != null) {
                    map.put("text", head(ai.text()));
                }
                if (ai.thinking() != null) {
                    map.put("thinkingLength", ai.thinking().length());
                }
                if (ai.hasToolExecutionRequests()) {
                    final List<Map<String, Object>> toolCalls = new ArrayList<>();
                    for (final ToolExecutionRequest toolRequest : ai.toolExecutionRequests()) {
                        final Map<String, Object> call = new LinkedHashMap<>();
                        call.put("id", toolRequest.id());
                        call.put("name", toolRequest.name());
                        call.put("arguments", head(toolRequest.arguments()));
                        toolCalls.add(call);
                    }
                    map.put("toolCalls", toolCalls);
                }
                if (ai.attributes() != null && !ai.attributes().isEmpty()) {
                    final Map<String, Object> attributes = new LinkedHashMap<>();
                    for (final Map.Entry<String, Object> entry : ai.attributes().entrySet()) {
                        final Object value = entry.getValue();
                        attributes.put(entry.getKey(), value instanceof String s ? s.length() : String.valueOf(value));
                    }
                    map.put("attributes", attributes);
                }
            }
            case ToolExecutionResultMessage toolResult -> {
                map.put("role", "tool");
                map.put("id", toolResult.id());
                map.put("name", toolResult.toolName());
                map.put("text", head(toolResult.text()));
            }
            default -> map.put("role", String.valueOf(message.type()));
        }
        return map;
    }

    private static String head(final String text) {
        if (text == null) {
            return null;
        }
        return text.length() <= 300 ? text : text.substring(0, 300) + "...(" + text.length() + " chars)";
    }

    private synchronized void writeLine(final Map<String, Object> line) {
        try {
            Files.writeString(logFile, GSON.toJson(line) + System.lineSeparator(),
                    StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (final IOException e) {
            // diagnostics must never break the agent request
            System.err.println("ChatModelFileLogger failed to write " + logFile + ": " + e);
        }
    }

}
