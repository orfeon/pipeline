package com.mercari.solution.server.agent;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class VertexAiGeminiHttpClientTest {

    @Test
    public void testParallelFunctionResponsesAreMergedIntoOneTurn() {
        final String body = """
                {
                  "contents": [
                    {"role": "user", "parts": [{"text": "count both tables"}]},
                    {"role": "model", "parts": [
                      {"functionCall": {"id": "a", "name": "rowCount", "args": {"table": "orders"}}, "thoughtSignature": "sig"},
                      {"functionCall": {"id": "b", "name": "rowCount", "args": {"table": "users"}}}
                    ]},
                    {"role": "user", "parts": [{"functionResponse": {"id": "a", "name": "rowCount", "response": {"response": "1"}}}]},
                    {"role": "user", "parts": [{"functionResponse": {"id": "b", "name": "rowCount", "response": {"response": "2"}}}]}
                  ],
                  "tools": [{"functionDeclarations": [{"name": "rowCount"}]}]
                }
                """;
        final JsonObject merged = JsonParser.parseString(VertexAiGeminiHttpClient.mergeFunctionResponses(body)).getAsJsonObject();
        final JsonArray contents = merged.getAsJsonArray("contents");
        Assertions.assertEquals(3, contents.size());
        final JsonArray responses = contents.get(2).getAsJsonObject().getAsJsonArray("parts");
        Assertions.assertEquals(2, responses.size());
        Assertions.assertEquals("a", responses.get(0).getAsJsonObject().getAsJsonObject("functionResponse").get("id").getAsString());
        Assertions.assertEquals("b", responses.get(1).getAsJsonObject().getAsJsonObject("functionResponse").get("id").getAsString());
        // the rest of the request is carried over as is
        Assertions.assertEquals("sig", contents.get(1).getAsJsonObject().getAsJsonArray("parts")
                .get(0).getAsJsonObject().get("thoughtSignature").getAsString());
        Assertions.assertTrue(merged.has("tools"));
    }

    @Test
    public void testSeparateToolRoundsAreNotMerged() {
        // two sequential rounds: each function response already follows its own function call turn
        final String body = """
                {
                  "contents": [
                    {"role": "user", "parts": [{"text": "q"}]},
                    {"role": "model", "parts": [{"functionCall": {"id": "a", "name": "f", "args": {}}}]},
                    {"role": "user", "parts": [{"functionResponse": {"id": "a", "name": "f", "response": {}}}]},
                    {"role": "model", "parts": [{"functionCall": {"id": "b", "name": "f", "args": {}}}]},
                    {"role": "user", "parts": [{"functionResponse": {"id": "b", "name": "f", "response": {}}}]}
                  ]
                }
                """;
        Assertions.assertSame(body, VertexAiGeminiHttpClient.mergeFunctionResponses(body));
    }

    @Test
    public void testUserTextAfterFunctionResponseIsNotMerged() {
        final String body = """
                {
                  "contents": [
                    {"role": "model", "parts": [{"functionCall": {"id": "a", "name": "f", "args": {}}}]},
                    {"role": "user", "parts": [{"functionResponse": {"id": "a", "name": "f", "response": {}}}]},
                    {"role": "user", "parts": [{"text": "a follow-up that mentions functionResponse"}]}
                  ]
                }
                """;
        Assertions.assertSame(body, VertexAiGeminiHttpClient.mergeFunctionResponses(body));
    }

    @Test
    public void testOtherBodiesAreReturnedAsIs() {
        Assertions.assertNull(VertexAiGeminiHttpClient.mergeFunctionResponses(null));
        final String plain = "{\"contents\":[{\"role\":\"user\",\"parts\":[{\"text\":\"hi\"}]}]}";
        Assertions.assertSame(plain, VertexAiGeminiHttpClient.mergeFunctionResponses(plain));
        final String notJson = "\"functionResponse\" but not json {";
        Assertions.assertSame(notJson, VertexAiGeminiHttpClient.mergeFunctionResponses(notJson));
        final String noContents = "{\"note\":\"functionResponse\"}";
        Assertions.assertSame(noContents, VertexAiGeminiHttpClient.mergeFunctionResponses(noContents));
    }

}
