package com.mercari.solution.util.pipeline;

import com.google.gson.JsonObject;
import com.mercari.solution.util.domain.web.HttpUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

// Only network independent parts (parameters handling) are tested here
public class AuthTest {

    @Test
    public void testParametersValidate() {
        final Auth.Parameters empty = new Auth.Parameters();
        final List<String> errorMessages = empty.validate();
        Assertions.assertEquals(1, errorMessages.size());
        Assertions.assertTrue(errorMessages.get(0).contains("requests"));

        // request without endpoint is invalid
        final Auth.Parameters invalid = Auth.Parameters.fromJsonString("""
                { "requests": [ { "method": "GET" } ] }
                """);
        Assertions.assertFalse(invalid.validate().isEmpty());

        final Auth.Parameters valid = Auth.Parameters.fromJsonString("""
                { "requests": [ { "endpoint": "https://example.com/token", "method": "POST" } ] }
                """);
        Assertions.assertTrue(valid.validate().isEmpty());
    }

    @Test
    public void testParametersSetDefaults() {
        final Auth.Parameters parameters = new Auth.Parameters();
        parameters.setDefaults();

        Assertions.assertNotNull(parameters.args);
        Assertions.assertEquals(60, parameters.timeoutSecond);
        Assertions.assertEquals(HttpUtil.Format.json, parameters.format);
        Assertions.assertNotNull(parameters.requests);
        Assertions.assertNotNull(parameters.refresh);
    }

    @Test
    public void testParametersJsonRoundTrip() {
        final Auth.Parameters parameters = Auth.Parameters.fromJsonString("""
                {
                  "args": { "clientId": "id", "clientSecret": "secret" },
                  "requests": [ { "endpoint": "https://example.com/token", "method": "POST" } ]
                }
                """);
        parameters.setDefaults();
        parameters.setup();

        final JsonObject json = parameters.toJson();
        Assertions.assertEquals("json", json.get("format").getAsString());
        Assertions.assertEquals("id", json.getAsJsonObject("args").get("clientId").getAsString());
        Assertions.assertEquals(1, json.getAsJsonArray("requests").size());

        final Auth.Parameters restored = Auth.Parameters.fromJsonString(parameters.toJsonString());
        Assertions.assertEquals(1, restored.requests.size());
        Assertions.assertEquals("https://example.com/token", restored.requests.get(0).endpoint);
        Assertions.assertTrue(restored.validate().isEmpty());
    }

    @Test
    public void testTransformCreation() {
        final Auth.Parameters parameters = Auth.Parameters.fromJsonString("""
                { "requests": [ { "endpoint": "https://example.com/token", "method": "POST" } ] }
                """);
        parameters.setDefaults();

        final Auth.Transform transform = Auth.of(parameters, false, new ArrayList<>());
        Assertions.assertNotNull(transform.outputTag);
        Assertions.assertNotNull(transform.failureTag);
    }

}
