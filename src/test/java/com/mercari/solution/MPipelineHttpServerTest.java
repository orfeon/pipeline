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

    private static final String REQUEST_CONFIG_YAML = """
            sources:
              - name: input
                module: request
                parameters:
                  schema:
                    fields:
                      - name: id
                        type: string
                      - name: value
                        type: int64
            """;

    @Test
    public void testTemplatedRequestArgsAreRefused() throws Exception {
        final MPipelineHttpServer server = new MPipelineHttpServer(
                BASE_ARGS, REQUEST_CONFIG, false, Config.Format.unknown, null, 1);
        final int port = server.start(0);
        try(final HttpClient client = HttpClient.newHttpClient()) {
            // ?args.x=${utils.gcp.accessToken()}
            final HttpResponse<String> query = client.send(
                    HttpRequest.newBuilder(URI.create("http://localhost:" + port
                                    + "/run?args.x=%24%7Butils.gcp.accessToken()%7D"))
                            .POST(HttpRequest.BodyPublishers.ofString("[{\"id\":\"a\",\"value\":1}]"))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            Assertions.assertEquals(400, query.statusCode(), "body: " + query.body());
            Assertions.assertTrue(query.body().contains("request arg 'x'"), query.body());

            // a Pub/Sub push attribute follows the same rule
            final String data = Base64.getEncoder().encodeToString(
                    "[{\"id\":\"a\",\"value\":1}]".getBytes(StandardCharsets.UTF_8));
            final String envelope = """
                    {
                      "message": {
                        "data": "%s",
                        "attributes": { "k": "<#assign a=1>" },
                        "messageId": "1"
                      },
                      "subscription": "projects/p/subscriptions/s"
                    }
                    """.formatted(data);
            final HttpResponse<String> push = client.send(
                    HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/run"))
                            .POST(HttpRequest.BodyPublishers.ofString(envelope))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            Assertions.assertEquals(400, push.statusCode(), "body: " + push.body());

            // plain values still pass
            final HttpResponse<String> plain = client.send(
                    HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/run?args.targetDate=2026-08-19"))
                            .POST(HttpRequest.BodyPublishers.ofString("[{\"id\":\"a\",\"value\":1}]"))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            Assertions.assertEquals(200, plain.statusCode(), "body: " + plain.body());
        } finally {
            server.stop();
        }
    }

    @Test
    public void testFixedConfigIgnoresFormatParameter() throws Exception {
        // with a fixed YAML config, ?format=json must not force a parse error that echoes the config
        final MPipelineHttpServer server = new MPipelineHttpServer(
                BASE_ARGS, REQUEST_CONFIG_YAML, false, Config.Format.unknown, null, 1);
        final int port = server.start(0);
        try(final HttpClient client = HttpClient.newHttpClient()) {
            final HttpResponse<String> run = client.send(
                    HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/run?format=json"))
                            .POST(HttpRequest.BodyPublishers.ofString("[{\"id\":\"a\",\"value\":1}]"))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            Assertions.assertEquals(200, run.statusCode(), "body: " + run.body());
            Assertions.assertFalse(run.body().contains("module: request"), run.body());
        } finally {
            server.stop();
        }
    }

    @Test
    public void testTemplateSyntaxArg() {
        Assertions.assertNull(MPipelineHttpServer.templateSyntaxArg(Map.of("targetDate", "2026-08-19")));
        Assertions.assertEquals("t", MPipelineHttpServer.templateSyntaxArg(Map.of("t", "${utils.gcp.accessToken()}")));
        Assertions.assertEquals("${x}", MPipelineHttpServer.templateSyntaxArg(Map.of("${x}", "v")));
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
