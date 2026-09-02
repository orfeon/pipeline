package com.mercari.solution;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mercari.solution.config.Config;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class MPipelineHttpServerTest {

    private static final String[] BASE_ARGS = new String[]{"--runner=DirectRunner"};

    private static final String CREATE_CONFIG = """
            {
              "sources": [
                {
                  "name": "create",
                  "module": "create",
                  "parameters": {
                    "type": "int64",
                    "from": 1,
                    "to": 3
                  }
                }
              ]
            }
            """;

    private static final String REQUEST_CONFIG = """
            {
              "sources": [
                {
                  "name": "input",
                  "module": "request",
                  "parameters": {
                    "schema": {
                      "fields": [
                        { "name": "id", "type": "string" },
                        { "name": "value", "type": "int64" }
                      ]
                    }
                  }
                }
              ]
            }
            """;

    @Test
    public void testHealthzAndRunWithConfigBody() throws Exception {
        final MPipelineHttpServer server = new MPipelineHttpServer(
                BASE_ARGS, null, false, Config.Format.unknown, null, 1);
        final int port = server.start(0);
        try(final HttpClient client = HttpClient.newHttpClient()) {
            final HttpResponse<String> health = client.send(
                    HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/healthz")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            Assertions.assertEquals(200, health.statusCode());
            Assertions.assertEquals("ok", health.body());

            // no fixed config: the request body is the config
            final HttpResponse<String> run = client.send(
                    HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/run"))
                            .POST(HttpRequest.BodyPublishers.ofString(CREATE_CONFIG))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            Assertions.assertEquals(200, run.statusCode(), "body: " + run.body());
            final JsonObject result = JsonParser.parseString(run.body()).getAsJsonObject();
            Assertions.assertEquals("DONE", result.get("state").getAsString());
        } finally {
            server.stop();
        }
    }

    @Test
    public void testRunWithoutConfigFails() throws Exception {
        final MPipelineHttpServer server = new MPipelineHttpServer(
                BASE_ARGS, null, false, Config.Format.unknown, null, 1);
        final int port = server.start(0);
        try(final HttpClient client = HttpClient.newHttpClient()) {
            final HttpResponse<String> run = client.send(
                    HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/run"))
                            .POST(HttpRequest.BodyPublishers.noBody())
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            Assertions.assertEquals(400, run.statusCode());
        } finally {
            server.stop();
        }
    }

    @Test
    public void testFixedConfigWithRequestBodyData() throws Exception {
        // fixed config at startup: the request body becomes the request source data
        final MPipelineHttpServer server = new MPipelineHttpServer(
                BASE_ARGS, REQUEST_CONFIG, false, Config.Format.unknown, null, 1);
        final int port = server.start(0);
        try(final HttpClient client = HttpClient.newHttpClient()) {
            final HttpResponse<String> run = client.send(
                    HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/run"))
                            .POST(HttpRequest.BodyPublishers.ofString(
                                    "[{\"id\":\"a\",\"value\":1},{\"id\":\"b\",\"value\":2}]"))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            Assertions.assertEquals(200, run.statusCode(), "body: " + run.body());
            final JsonObject result = JsonParser.parseString(run.body()).getAsJsonObject();
            Assertions.assertEquals("DONE", result.get("state").getAsString());
        } finally {
            server.stop();
        }
    }

    @Test
    public void testFixedConfigWithInvalidDataFails() throws Exception {
        final MPipelineHttpServer server = new MPipelineHttpServer(
                BASE_ARGS, REQUEST_CONFIG, false, Config.Format.unknown, null, 1);
        final int port = server.start(0);
        try(final HttpClient client = HttpClient.newHttpClient()) {
            final HttpResponse<String> run = client.send(
                    HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/run"))
                            .POST(HttpRequest.BodyPublishers.ofString("not a json"))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            Assertions.assertEquals(400, run.statusCode(), "body: " + run.body());
        } finally {
            server.stop();
        }
    }

    @Test
    public void testUnwrapPubSubPush() {
        final String data = Base64.getEncoder().encodeToString(
                "[{\"id\":\"a\",\"value\":1}]".getBytes(StandardCharsets.UTF_8));
        final String envelope = """
                {
                  "message": {
                    "data": "%s",
                    "attributes": { "targetDate": "2026-08-19" },
                    "messageId": "1"
                  },
                  "subscription": "projects/p/subscriptions/s"
                }
                """.formatted(data);

        final Map<String, String> args = new HashMap<>();
        final String body = MPipelineHttpServer.unwrapPubSubPush(envelope, args);
        Assertions.assertEquals("[{\"id\":\"a\",\"value\":1}]", body);
        Assertions.assertEquals("2026-08-19", args.get("targetDate"));

        // a non-envelope body passes through unchanged
        final Map<String, String> args2 = new HashMap<>();
        Assertions.assertEquals("[{\"id\":1}]", MPipelineHttpServer.unwrapPubSubPush("[{\"id\":1}]", args2));
        Assertions.assertTrue(args2.isEmpty());
    }

}
