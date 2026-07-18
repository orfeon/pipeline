package com.mercari.solution.server.diagnosis;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import com.mercari.solution.util.cloud.google.LoggingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

/**
 * Sends a diagnosis result to the configured destinations:
 * a Slack incoming webhook, a generic JSON webhook, and a structured Cloud Logging record
 * (the queryable history used to spot recurring failures). Each destination is optional and
 * failures in one do not prevent the others.
 */
public class DiagnosisNotifier {

    private static final Logger LOG = LoggerFactory.getLogger(DiagnosisNotifier.class);

    private static final String ENV_SLACK_WEBHOOK = "MERCARI_PIPELINE_DIAGNOSIS_SLACK_WEBHOOK";
    private static final String ENV_WEBHOOK = "MERCARI_PIPELINE_DIAGNOSIS_WEBHOOK";
    private static final String ENV_LOG_NAME = "MERCARI_PIPELINE_DIAGNOSIS_LOG_NAME";

    private static final String DEFAULT_LOG_NAME = "mercari-pipeline-diagnosis";
    private static final int MAX_FACTS_LENGTH = 20000;

    public static void notify(
            final DiagnosisService.DataflowJobEvent event,
            final String facts,
            final String diagnosisText) {

        final JsonObject record = buildRecord(event, facts, diagnosisText);

        final String slackWebhook = System.getenv(ENV_SLACK_WEBHOOK);
        if (slackWebhook != null && !slackWebhook.isBlank()) {
            try {
                final JsonObject payload = new JsonObject();
                payload.addProperty("text", buildSlackText(record));
                post(slackWebhook.trim(), payload.toString());
            } catch (final Throwable e) {
                LOG.error("Failed to notify Slack for job: " + event.jobId(), e);
            }
        }

        final String webhook = System.getenv(ENV_WEBHOOK);
        if (webhook != null && !webhook.isBlank()) {
            try {
                post(webhook.trim(), record.toString());
            } catch (final Throwable e) {
                LOG.error("Failed to notify webhook for job: " + event.jobId(), e);
            }
        }

        if (event.projectId() != null) {
            try {
                final String logName = System.getenv().getOrDefault(ENV_LOG_NAME, DEFAULT_LOG_NAME);
                final Map<String, Object> payload = new Gson().fromJson(
                        record, new TypeToken<Map<String, Object>>() { }.getType());
                LoggingUtil.write(event.projectId(), logName, payload);
            } catch (final Throwable e) {
                LOG.error("Failed to write diagnosis log for job: " + event.jobId(), e);
            }
        }

        LOG.info("Diagnosis completed for job {}: {}", event.jobId(), record);
    }

    static JsonObject buildRecord(
            final DiagnosisService.DataflowJobEvent event,
            final String facts,
            final String diagnosisText) {

        final JsonObject record = new JsonObject();
        record.addProperty("jobId", event.jobId());
        if (event.projectId() != null) {
            record.addProperty("projectId", event.projectId());
        }
        if (event.region() != null) {
            record.addProperty("region", event.region());
        }
        if (event.messageText() != null) {
            record.addProperty("eventMessage", event.messageText());
        }
        record.addProperty("diagnosedAt", Instant.now().toString());

        final JsonObject diagnosis = parseDiagnosis(diagnosisText);
        if (diagnosis != null) {
            record.add("diagnosis", diagnosis);
        } else if (facts != null) {
            // No LLM verdict available: fall back to shipping the collected facts
            record.addProperty("facts", facts.length() > MAX_FACTS_LENGTH
                    ? facts.substring(0, MAX_FACTS_LENGTH) + "... (truncated)"
                    : facts);
        }
        return record;
    }

    /** Parse the agent's JSON verdict, tolerating markdown code fences; null when absent. */
    static JsonObject parseDiagnosis(final String diagnosisText) {
        if (diagnosisText == null || diagnosisText.isBlank()) {
            return null;
        }
        String text = diagnosisText.trim();
        if (text.startsWith("```")) {
            final int firstNewline = text.indexOf('\n');
            final int lastFence = text.lastIndexOf("```");
            if (firstNewline >= 0 && lastFence > firstNewline) {
                text = text.substring(firstNewline + 1, lastFence).trim();
            }
        }
        try {
            final JsonElement element = JsonParser.parseString(text);
            if (element.isJsonObject()) {
                return element.getAsJsonObject();
            }
        } catch (final Exception e) {
            // not JSON: wrap below
        }
        final JsonObject wrapped = new JsonObject();
        wrapped.addProperty("raw", diagnosisText);
        return wrapped;
    }

    static String buildSlackText(final JsonObject record) {
        final StringBuilder text = new StringBuilder();
        text.append(":rotating_light: Dataflow job failed: `").append(getString(record, "jobId")).append("`");
        final String project = getString(record, "projectId");
        final String region = getString(record, "region");
        if (project != null) {
            text.append(" (").append(project);
            if (region != null) {
                text.append(" / ").append(region);
            }
            text.append(")");
        }
        text.append("\n");

        if (record.has("diagnosis") && record.get("diagnosis").isJsonObject()) {
            final JsonObject diagnosis = record.getAsJsonObject("diagnosis");
            final String cause = getString(diagnosis, "causeCategory");
            final String confidence = getString(diagnosis, "confidence");
            if (cause != null) {
                text.append("*cause*: ").append(cause);
                if (confidence != null) {
                    text.append(" (confidence: ").append(confidence).append(")");
                }
                text.append("\n");
            }
            final String summary = getString(diagnosis, "summary");
            if (summary != null) {
                text.append("*summary*: ").append(summary).append("\n");
            }
            final String recommendation = getString(diagnosis, "recommendation");
            if (recommendation != null) {
                text.append("*recommendation*: ").append(recommendation).append("\n");
            }
            final String raw = getString(diagnosis, "raw");
            if (raw != null) {
                text.append(raw.length() > 2000 ? raw.substring(0, 2000) + "..." : raw).append("\n");
            }
        } else {
            text.append("No automated diagnosis available. See the diagnosis log for collected error facts.\n");
        }
        return text.toString();
    }

    private static void post(final String url, final String body) throws Exception {
        final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json; charset=utf-8")
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .build();
        final HttpResponse<String> response = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build()
                .send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() >= 300) {
            LOG.warn("Notification endpoint {} returned status {}: {}",
                    url, response.statusCode(), response.body());
        }
    }

    private static String getString(final JsonObject object, final String key) {
        return object.has(key) && object.get(key).isJsonPrimitive()
                ? object.get(key).getAsString()
                : null;
    }

}
