package com.mercari.solution.util.cloud.google;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;

public class DataprocUtil {

    private static final String ENDPOINT_BASE = "https://dataproc.googleapis.com/v1/projects/%s/locations/%s/";

    public static JsonObject launchServerlessBatchJob(
            final String jarFileUri,
            final String version,
            final Map<String, String> args,
            final String project,
            final String region,
            final String batchId) {

        final List<String> argsList = args.entrySet().stream()
                .map(e -> String.format("\"%s=%s\"",e.getKey() , e.getValue()))
                .toList();

        final String endpoint = String.format(ENDPOINT_BASE, project, region) + "batches" + (batchId != null ? "?batchId=" + batchId : "");
        final String body = String.format("""
            {
              "sparkBatch": {
                "mainClass": "com.mercari.solution.MPipeline",
                "jarFileUris": [
                  "%s"
                ],
                "args": %s
              },
              "runtimeConfig": {
                "version": "%s"
              }
            }
            """, jarFileUri, argsList, version);

        try(final HttpClient client = HttpClient.newHttpClient()) {
            final String accessToken = IAMUtil.getAccessToken().getTokenValue();
            final HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(endpoint))
                    .header("Authorization", "Bearer " + accessToken)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if(response.statusCode() >= 400) {
                throw new RuntimeException("Failed to execute batch job. status: " + response.statusCode() + ", body: " + response.body());
            }
            return new Gson().fromJson(response.body(), JsonObject.class);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
