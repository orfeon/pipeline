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
 * <p>A model turn is restored exactly as the model produced it, which Gemini 4 requires of a turn
 * that called tools (it answers a history replayed without that with an empty MALFORMED_RESPONSE):
 * <ul>
 *   <li>{@code toolCallGroup} — set on every message that came from one model turn (its text and its
 *       tool calls), so that parallel tool calls and the accompanying text are put back into a single
 *       {@code AiMessage} instead of one model turn each;</li>
 *   <li>{@code thoughtSignature} — the opaque signature Gemini attaches to a tool-calling turn, set on
 *       the first tool call message of the group and handed back to the model on replay.</li>
 * </ul>
 * Both are optional: messages without them (older conversations kept by the browser) are restored
 * one message per turn as before. The UI stores and returns the messages untouched.
 */
final class AgentHistory {

    static final String FIELD_GROUP = "toolCallGroup";
    static final String FIELD_SIGNATURE = "thoughtSignature";

    /** The AiMessage attribute under which langchain4j-google-ai-gemini keeps the thought signature. */
    static final String ATTRIBUTE_SIGNATURE = "thinking_signature";

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
                    final String group = getString(msg, FIELD_GROUP).orElse(null);
                    if (group != null) {
                        // every following assistant message of the same group belongs to this model turn
                        int end = i;
                        while (end + 1 < historyJson.size() && isAssistantOfGroup(historyJson.get(end + 1).getAsJsonObject(), group)) {
                            end++;
                        }
                        messages.add(toGroupedAiMessage(historyJson, i, end));
                        i = end;
                    } else if (hasToolCall(msg)) {
                        messages.add(AiMessage.from(toToolRequest(msg.getAsJsonObject("toolCall"))));
                    } else {
                        messages.add(AiMessage.from(content));
                    }
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

    private static AiMessage toGroupedAiMessage(final JsonArray historyJson, final int from, final int to) {
        final StringBuilder text = new StringBuilder();
        final List<ToolExecutionRequest> requests = new ArrayList<>();
        String signature = null;
        for (int i = from; i <= to; i++) {
            final JsonObject msg = historyJson.get(i).getAsJsonObject();
            if (hasToolCall(msg)) {
                requests.add(toToolRequest(msg.getAsJsonObject("toolCall")));
                if (signature == null) {
                    signature = getString(msg, FIELD_SIGNATURE).filter(s -> !s.isEmpty()).orElse(null);
                }
            } else {
                text.append(getString(msg, "content").orElse(""));
            }
        }
        final AiMessage.Builder builder = AiMessage.builder();
        if (!text.isEmpty()) {
            builder.text(text.toString());
        }
        if (!requests.isEmpty()) {
            builder.toolExecutionRequests(requests);
        }
        if (signature != null) {
            builder.attributes(Map.of(ATTRIBUTE_SIGNATURE, signature));
        }
        return builder.build();
    }

    static List<Map<String, Object>> toJson(final List<ChatMessage> messages) {
        final List<Map<String, Object>> json = new ArrayList<>();
        for (final ChatMessage message : messages) {
            switch (message) {
                case AiMessage aiMessage -> {
                    final boolean hasTools = aiMessage.hasToolExecutionRequests();
                    final String group = hasTools ? UUID.randomUUID().toString() : null;
                    final String text = aiMessage.text();
                    if (text != null && !text.isBlank()) {
                        final Map<String, Object> textMessage = new LinkedHashMap<>();
                        textMessage.put("role", "assistant");
                        textMessage.put("content", text);
                        if (hasTools) {
                            textMessage.put(FIELD_GROUP, group);
                        }
                        json.add(textMessage);
                    }
                    if (hasTools) {
                        String signature = aiMessage.attribute(ATTRIBUTE_SIGNATURE, String.class);
                        for (final ToolExecutionRequest toolRequest : aiMessage.toolExecutionRequests()) {
                            final Map<String, Object> toolCallMessage = new LinkedHashMap<>();
                            toolCallMessage.put("role", "assistant");
                            toolCallMessage.put("content", "");
                            toolCallMessage.put("toolCall", Map.of(
                                    "id", Optional.ofNullable(toolRequest.id()).orElse(""),
                                    "name", toolRequest.name(),
                                    "arguments", Optional.ofNullable(toolRequest.arguments()).orElse("")
                            ));
                            toolCallMessage.put(FIELD_GROUP, group);
                            if (signature != null && !signature.isEmpty()) {
                                // one signature per model turn: carried by its first tool call
                                toolCallMessage.put(FIELD_SIGNATURE, signature);
                                signature = null;
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

    private static ToolExecutionRequest toToolRequest(final JsonObject toolCall) {
        return ToolExecutionRequest.builder()
                .id(getString(toolCall, "id").orElseGet(() -> UUID.randomUUID().toString()))
                .name(toolCall.get("name").getAsString())
                .arguments(getString(toolCall, "arguments").orElse(""))
                .build();
    }

    private static boolean isAssistantOfGroup(final JsonObject msg, final String group) {
        return "assistant".equals(getString(msg, "role").orElse(null))
                && group.equals(getString(msg, FIELD_GROUP).orElse(null));
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
