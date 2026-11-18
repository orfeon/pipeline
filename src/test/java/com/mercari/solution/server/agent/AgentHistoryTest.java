package com.mercari.solution.server.agent;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import dev.langchain4j.agent.tool.ToolExecutionRequest;
import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.data.message.ToolExecutionResultMessage;
import dev.langchain4j.data.message.UserMessage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class AgentHistoryTest {

    private static ToolExecutionRequest request(final String id, final String name, final String arguments) {
        return ToolExecutionRequest.builder().id(id).name(name).arguments(arguments).build();
    }

    private static JsonArray toJson(final List<ChatMessage> messages) {
        return JsonParser.parseString(new Gson().toJson(AgentHistory.toJson(messages))).getAsJsonArray();
    }

    @Test
    public void testToolCallingTurnRoundTrips() {
        final ToolExecutionRequest first = request("call_1", "readDocs", "{\"module\":\"source/create\"}");
        final ToolExecutionRequest second = request("call_2", "readDocs", "{\"module\":\"sink/debug\"}");
        final List<ChatMessage> produced = List.of(
                AiMessage.builder()
                        .text("Reading both docs.")
                        .toolExecutionRequests(List.of(first, second))
                        .attributes(Map.of(AgentHistory.ATTRIBUTE_SIGNATURE, "sig-1"))
                        .build(),
                ToolExecutionResultMessage.from(first, "create docs"),
                ToolExecutionResultMessage.from(second, "debug docs"),
                AiMessage.from("{\"message\": \"done\"}"));

        final JsonArray json = toJson(produced);
        // flat for the UI: text, one message per tool call, the results, the final text
        Assertions.assertEquals(6, json.size());
        final JsonObject text = json.get(0).getAsJsonObject();
        final JsonObject call1 = json.get(1).getAsJsonObject();
        final JsonObject call2 = json.get(2).getAsJsonObject();
        Assertions.assertEquals("Reading both docs.", text.get("content").getAsString());
        Assertions.assertEquals("call_1", call1.getAsJsonObject("toolCall").get("id").getAsString());
        final String group = call1.get(AgentHistory.FIELD_GROUP).getAsString();
        Assertions.assertEquals(group, text.get(AgentHistory.FIELD_GROUP).getAsString());
        Assertions.assertEquals(group, call2.get(AgentHistory.FIELD_GROUP).getAsString());
        // the signature of the turn travels on its first tool call only
        Assertions.assertEquals("sig-1", call1.get(AgentHistory.FIELD_SIGNATURE).getAsString());
        Assertions.assertFalse(call2.has(AgentHistory.FIELD_SIGNATURE));
        Assertions.assertEquals("tool", json.get(3).getAsJsonObject().get("role").getAsString());
        Assertions.assertFalse(json.get(5).getAsJsonObject().has(AgentHistory.FIELD_GROUP));

        final JsonArray history = new JsonArray();
        final JsonObject user = new JsonObject();
        user.addProperty("role", "user");
        user.addProperty("content", "build it");
        history.add(user);
        history.addAll(json);

        final List<ChatMessage> restored = AgentHistory.toMessages(history);
        Assertions.assertEquals(5, restored.size());
        Assertions.assertEquals("build it", ((UserMessage) restored.get(0)).singleText());
        final AiMessage turn = (AiMessage) restored.get(1);
        Assertions.assertEquals("Reading both docs.", turn.text());
        Assertions.assertEquals(List.of(first, second), turn.toolExecutionRequests());
        Assertions.assertEquals("sig-1", turn.attribute(AgentHistory.ATTRIBUTE_SIGNATURE, String.class));
        Assertions.assertEquals("call_1", ((ToolExecutionResultMessage) restored.get(2)).id());
        Assertions.assertEquals("debug docs", ((ToolExecutionResultMessage) restored.get(3)).text());
        final AiMessage answer = (AiMessage) restored.get(4);
        Assertions.assertEquals("{\"message\": \"done\"}", answer.text());
        Assertions.assertFalse(answer.hasToolExecutionRequests());
    }

    @Test
    public void testConsecutiveToolRoundsStaySeparateTurns() {
        final ToolExecutionRequest first = request("call_1", "readDocs", "{}");
        final ToolExecutionRequest second = request("call_2", "runPipeline", "{}");
        final JsonArray json = toJson(List.of(
                AiMessage.builder().toolExecutionRequests(List.of(first))
                        .attributes(Map.of(AgentHistory.ATTRIBUTE_SIGNATURE, "sig-1")).build(),
                ToolExecutionResultMessage.from(first, "docs"),
                AiMessage.builder().toolExecutionRequests(List.of(second))
                        .attributes(Map.of(AgentHistory.ATTRIBUTE_SIGNATURE, "sig-2")).build(),
                ToolExecutionResultMessage.from(second, "ok")));

        final List<ChatMessage> restored = AgentHistory.toMessages(json);
        Assertions.assertEquals(4, restored.size());
        Assertions.assertEquals("sig-1", ((AiMessage) restored.get(0)).attribute(AgentHistory.ATTRIBUTE_SIGNATURE, String.class));
        Assertions.assertEquals(List.of(second), ((AiMessage) restored.get(2)).toolExecutionRequests());
        Assertions.assertEquals("sig-2", ((AiMessage) restored.get(2)).attribute(AgentHistory.ATTRIBUTE_SIGNATURE, String.class));
    }

    @Test
    public void testTurnWithoutSignature() {
        final ToolExecutionRequest only = request("call_1", "readDocs", "{}");
        final JsonArray json = toJson(List.of(AiMessage.builder().toolExecutionRequests(List.of(only)).build()));
        Assertions.assertFalse(json.get(0).getAsJsonObject().has(AgentHistory.FIELD_SIGNATURE));

        final AiMessage restored = (AiMessage) AgentHistory.toMessages(json).get(0);
        Assertions.assertEquals(List.of(only), restored.toolExecutionRequests());
        Assertions.assertNull(restored.attribute(AgentHistory.ATTRIBUTE_SIGNATURE, String.class));
    }

    @Test
    public void testHistoryWithoutGroupsIsRestoredOneMessagePerTurn() {
        // a conversation the browser kept from before toolCallGroup / thoughtSignature existed
        final JsonArray history = JsonParser.parseString("""
                [
                  {"role": "user", "content": "build it"},
                  {"role": "assistant", "content": "", "toolCall": {"id": "a", "name": "readDocs", "arguments": "{}"}},
                  {"role": "assistant", "content": "", "toolCall": {"id": "b", "name": "readDocs", "arguments": "{}"}},
                  {"role": "tool", "content": "docs a", "toolCall": {"id": "a", "name": "readDocs", "arguments": ""}},
                  {"role": "tool", "content": "docs b"},
                  {"role": "assistant", "content": "done"}
                ]
                """).getAsJsonArray();
        final List<ChatMessage> restored = AgentHistory.toMessages(history);
        Assertions.assertEquals(6, restored.size());
        Assertions.assertEquals(1, ((AiMessage) restored.get(1)).toolExecutionRequests().size());
        Assertions.assertEquals("b", ((AiMessage) restored.get(2)).toolExecutionRequests().get(0).id());
        Assertions.assertEquals("a", ((ToolExecutionResultMessage) restored.get(3)).id());
        Assertions.assertEquals("unknown", ((ToolExecutionResultMessage) restored.get(4)).toolName());
        Assertions.assertEquals("done", ((AiMessage) restored.get(5)).text());
    }

}
