package com.mercari.solution.server.agent;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import dev.langchain4j.agent.tool.ToolExecutionRequest;
import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.data.message.ToolExecutionResultMessage;
import dev.langchain4j.data.message.UserMessage;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Converts the agent conversation between LangChain4j messages and the JSON messages the UI keeps
 * and sends back as {@code history} on the next request.
 *
 * <p>JSON messages are flat — one message per tool call:
 * <pre>
 * {"role": "user",      "content": "..."}
 * {"role": "assistant", "content": "..."}                                    // text
 * {"role": "assistant", "content": "", "toolCall": {id, name, arguments}}    // one tool call
 * {"role": "tool",      "content": "...", "toolCall": {id, name, arguments: ""}}
 * </pre>
 *
 * <p>A model turn is restored as the model produced it, which Gemini 4 requires of a turn that
 * called tools (it answers a history replayed without that with an empty MALFORMED_RESPONSE):
 * <ul>
 *   <li>Adjacent assistant messages are one model turn — its text and its (parallel) tool calls —
 *       and are put back into a single {@code AiMessage}. In the flat history two model turns are
 *       always separated by tool results or a user message.</li>
 *   <li>{@code thoughtSignature} — the opaque signature Gemini attaches to a part of a tool-calling
 *       turn (base64), stored on the JSON message of that part and handed back to the model on
 *       replay. Optional: older conversations kept by the browser do not have it.</li>
 * </ul>
 * The UI stores and returns the messages untouched.
 */
final class AgentHistory {

    static final String FIELD_SIGNATURE = "thoughtSignature";

    /**
     * AiMessage attributes under which langchain4j-google-genai keeps thought signatures: the plain
     * key for the text part, the prefix + tool call id for a function call part.
     */
    static final String ATTRIBUTE_SIGNATURE = "thought_signature";
    static final String ATTRIBUTE_SIGNATURE_PREFIX = "thought_signature_";

    private AgentHistory() {
    }

    static List<ChatMessage> toMessages(final JsonArray historyJson) {
        final List<ChatMessage> messages = new ArrayList<>();
        for (int i = 0; i < historyJson.size(); i++) {
            final JsonObject msg = historyJson.get(i).getAsJsonObject();
            final String role = msg.get("role").getAsString();
            final String content = getString(msg, "content").orElse("");

            switch (role) {
                case "user" -> messages.add(UserMessage.from(content));
                case "assistant" -> {
                    // the following assistant messages belong to the same model turn
                    int end = i;
                    while (end + 1 < historyJson.size() && isAssistant(historyJson.get(end + 1).getAsJsonObject())) {
                        end++;
                    }
                    messages.add(toAiMessage(historyJson, i, end));
                    i = end;
                }
                case "tool" -> {
                    final ToolExecutionRequest request;
                    if (hasToolCall(msg)) {
                        final JsonObject toolCall = msg.getAsJsonObject("toolCall");
                        request = ToolExecutionRequest.builder()
                                .id(getString(toolCall, "id").orElseGet(() -> UUID.randomUUID().toString()))
                                .name(getString(toolCall, "name").orElse("unknown"))
                                .arguments("")
                                .build();
                    } else {
                        // Fallback: create a tool result with a dummy request
                        request = ToolExecutionRequest.builder()
                                .id(UUID.randomUUID().toString())
                                .name("unknown")
                                .arguments("")
                                .build();
                    }
                    messages.add(ToolExecutionResultMessage.from(request, content));
                }
                default -> messages.add(null);
            }
        }
        return messages;
    }

    private static AiMessage toAiMessage(final JsonArray historyJson, final int from, final int to) {
        final StringBuilder text = new StringBuilder();
        final List<ToolExecutionRequest> requests = new ArrayList<>();
        final Map<String, Object> attributes = new LinkedHashMap<>();
        for (int i = from; i <= to; i++) {
            final JsonObject msg = historyJson.get(i).getAsJsonObject();
            final Optional<String> signature = getString(msg, FIELD_SIGNATURE).filter(s -> !s.isEmpty());
            if (hasToolCall(msg)) {
                final ToolExecutionRequest request = toToolRequest(msg.getAsJsonObject("toolCall"));
                requests.add(request);
                signature.ifPresent(s -> attributes.put(ATTRIBUTE_SIGNATURE_PREFIX + request.id(), s));
            } else {
                text.append(getString(msg, "content").orElse(""));
                signature.ifPresent(s -> attributes.put(ATTRIBUTE_SIGNATURE, s));
            }
        }
        if (requests.isEmpty() && attributes.isEmpty()) {
            return AiMessage.from(text.toString());
        }
        final AiMessage.Builder builder = AiMessage.builder();
        if (!text.isEmpty()) {
            builder.text(text.toString());
        }
        if (!requests.isEmpty()) {
            builder.toolExecutionRequests(requests);
        }
        if (!attributes.isEmpty()) {
            builder.attributes(attributes);
        }
        return builder.build();
    }

    static List<Map<String, Object>> toJson(final List<ChatMessage> messages) {
        final List<Map<String, Object>> json = new ArrayList<>();
        for (final ChatMessage message : messages) {
            switch (message) {
                case AiMessage aiMessage -> {
                    final boolean hasTools = aiMessage.hasToolExecutionRequests();
                    final String text = aiMessage.text();
                    if (text != null && !text.isBlank()) {
                        final Map<String, Object> textMessage = new LinkedHashMap<>();
                        textMessage.put("role", "assistant");
                        textMessage.put("content", text);
                        if (hasTools) {
                            // only a tool-calling turn is replayed part by part
                            putSignature(textMessage, aiMessage, ATTRIBUTE_SIGNATURE);
                        }
                        json.add(textMessage);
                    }
                    if (hasTools) {
                        for (final ToolExecutionRequest toolRequest : aiMessage.toolExecutionRequests()) {
                            final Map<String, Object> toolCallMessage = new LinkedHashMap<>();
                            toolCallMessage.put("role", "assistant");
                            toolCallMessage.put("content", "");
                            toolCallMessage.put("toolCall", Map.of(
                                    "id", Optional.ofNullable(toolRequest.id()).orElse(""),
                                    "name", toolRequest.name(),
                                    "arguments", Optional.ofNullable(toolRequest.arguments()).orElse("")
                            ));
                            if (toolRequest.id() != null) {
                                putSignature(toolCallMessage, aiMessage, ATTRIBUTE_SIGNATURE_PREFIX + toolRequest.id());
                            }
                            json.add(toolCallMessage);
                        }
                    }
                }
                case ToolExecutionResultMessage toolResultMessage -> {
                    final Map<String, Object> toolResultMessageJson = new LinkedHashMap<>();
                    toolResultMessageJson.put("role", "tool");
                    toolResultMessageJson.put("content", toolResultMessage.text());
                    toolResultMessageJson.put("toolCall", Map.of(
                            "id", Optional.ofNullable(toolResultMessage.id()).orElse(""),
                            "name", toolResultMessage.toolName(),
                            "arguments", "" // the result message does not keep the arguments
                    ));
                    json.add(toolResultMessageJson);
                }
                default -> {}
            }
        }
        return json;
    }

    private static void putSignature(final Map<String, Object> jsonMessage, final AiMessage aiMessage, final String attribute) {
        final String signature = aiMessage.attribute(attribute, String.class);
        if (signature != null && !signature.isEmpty()) {
            jsonMessage.put(FIELD_SIGNATURE, signature);
        }
    }

    private static ToolExecutionRequest toToolRequest(final JsonObject toolCall) {
        return ToolExecutionRequest.builder()
                .id(getString(toolCall, "id").filter(s -> !s.isEmpty()).orElseGet(() -> UUID.randomUUID().toString()))
                .name(toolCall.get("name").getAsString())
                .arguments(getString(toolCall, "arguments").orElse(""))
                .build();
    }

    private static boolean isAssistant(final JsonObject msg) {
        return "assistant".equals(getString(msg, "role").orElse(null));
    }

    private static boolean hasToolCall(final JsonObject msg) {
        return msg.has("toolCall") && msg.get("toolCall").isJsonObject();
    }

    private static Optional<String> getString(final JsonObject object, final String field) {
        if (!object.has(field) || !object.get(field).isJsonPrimitive()) {
            return Optional.empty();
        }
        return Optional.of(object.get(field).getAsString());
    }

}
