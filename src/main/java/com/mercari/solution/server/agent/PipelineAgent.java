package com.mercari.solution.server.agent;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.server.agent.tool.PipelineExecutor;
import dev.langchain4j.agent.tool.*;
import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.data.message.ToolExecutionResultMessage;
import dev.langchain4j.memory.ChatMemory;
import dev.langchain4j.memory.chat.MessageWindowChatMemory;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.service.AiServices;
import dev.langchain4j.service.SystemMessage;
import dev.langchain4j.service.UserMessage;
import dev.langchain4j.service.V;
import org.apache.beam.sdk.values.KV;

import java.util.*;

public interface PipelineAgent {

    @SystemMessage(fromResource = "server/agents/prompts/pipeline-builder-system.md")
    @UserMessage("""
            {{message}}
            {{config}}
        """)
    String chat(@V("message") String message, @V("config") String config);

    static String process(final ChatModel chatModel, final JsonObject body) {
        final JsonArray historyJson = body.getAsJsonArray("history");
        final List<ChatMessage> history = createHistoryMessages(historyJson);
        final dev.langchain4j.data.message.UserMessage lastUserMessage = (dev.langchain4j.data.message.UserMessage) history.removeLast();

        final ChatMemory historyMemory = MessageWindowChatMemory.withMaxMessages(history.size() + 20);
        history.forEach(historyMemory::add);

        AiServices.builder(PipelineAgent.class)
                .chatModel(chatModel)
                .chatMemory(historyMemory)
                .tools(
                        PipelineExecutor.create()
                )
                .afterToolExecution((e) -> {
                    System.out.println(e.request());
                })
                .build()
                .chat(lastUserMessage.singleText(), "");

        final List<ChatMessage> newResponseMessages = historyMemory
                .messages()
                .subList(history.size(), historyMemory.messages().size());
        final List<Map<String, Object>> newMessages = createResponseMessages(newResponseMessages);

        return new Gson().toJson(newMessages);
    }

    private static List<ChatMessage> createHistoryMessages(final JsonArray historyJson) {
        final List<ChatMessage> messages = new ArrayList<>();
        for (final JsonElement entry : historyJson) {
            final JsonObject msg = entry.getAsJsonObject();
            final String role = msg.get("role").getAsString();
            final String content = msg.has("content") ? msg.get("content").getAsString() : "";

            final ChatMessage message = switch (role) {
                case "user" -> dev.langchain4j.data.message.UserMessage.from(content);
                case "assistant" -> {
                    if (msg.has("toolCall") && msg.get("toolCall").isJsonObject()) {
                        final JsonObject tc = msg.getAsJsonObject("toolCall");
                        final ToolExecutionRequest toolRequest = ToolExecutionRequest.builder()
                                .id(tc.has("id") ? tc.get("id").getAsString() : UUID.randomUUID().toString())
                                .name(tc.get("name").getAsString())
                                .arguments(tc.has("arguments") ? tc.get("arguments").getAsString() : "")
                                .build();
                        yield AiMessage.from(toolRequest);
                    } else {
                        yield AiMessage.from(content);
                    }
                }
                case "tool" -> {
                    if (msg.has("toolCall") && msg.get("toolCall").isJsonObject()) {
                        final JsonObject tc = msg.getAsJsonObject("toolCall");
                        final String toolId = tc.has("id") ? tc.get("id").getAsString() : UUID.randomUUID().toString();
                        final String toolName = tc.has("name") ? tc.get("name").getAsString() : "unknown";
                        final ToolExecutionRequest origReq = ToolExecutionRequest.builder()
                                .id(toolId)
                                .name(toolName)
                                .arguments("")
                                .build();
                        yield ToolExecutionResultMessage.from(origReq, content);
                    } else {
                        // Fallback: create a tool result with a dummy request
                        final ToolExecutionRequest dummyReq = ToolExecutionRequest.builder()
                                .id(UUID.randomUUID().toString())
                                .name("unknown")
                                .arguments("")
                                .build();
                        yield ToolExecutionResultMessage.from(dummyReq, content);
                    }
                }
                default -> null;
            };
            messages.add(message);
        }
        return messages;
    }

    private static List<Map<String, Object>> createResponseMessages(final List<ChatMessage> newResponseMessages) {
        final List<Map<String, Object>> responseMessages = new ArrayList<>();
        for(final ChatMessage newMessage : newResponseMessages) {
            switch (newMessage) {
                case AiMessage aiMessage -> {
                    final String text = aiMessage.text();
                    if (text != null && !text.isBlank()) {
                        responseMessages.add(Map.of(
                                "role", "assistant",
                                "content", text
                        ));
                    }
                    if (aiMessage.hasToolExecutionRequests()) {
                        for (final ToolExecutionRequest toolRequest : aiMessage.toolExecutionRequests()) {
                            final Map<String, Object> toolCallMsg = new LinkedHashMap<>();
                            toolCallMsg.put("role", "assistant");
                            toolCallMsg.put("content", "");
                            toolCallMsg.put("toolCall", Map.of(
                                    "id", Optional.ofNullable(toolRequest.id()).orElse(""),
                                    "name", toolRequest.name(),
                                    "arguments", Optional.ofNullable(toolRequest.arguments()).orElse("")
                            ));
                            responseMessages.add(toolCallMsg);
                        }
                    }
                }
                case ToolExecutionResultMessage toolResultMessage -> {
                    final Map<String, Object> toolResultMap = new LinkedHashMap<>();
                    toolResultMap.put("role", "tool");
                    toolResultMap.put("content", toolResultMessage.text());
                    toolResultMap.put("toolCall", Map.of(
                            "id", Optional.ofNullable(toolResultMessage.id()).orElse(""),
                            "name", toolResultMessage.toolName(),
                            "arguments", "" // 結果メッセージ側には引数は保持されないため空
                    ));
                    responseMessages.add(toolResultMap);
                }
                default -> {}
            }
        }
        return responseMessages;
    }

    private static KV<String, String> parseUserMessage() {
        return KV.of("", "");
    }

}
