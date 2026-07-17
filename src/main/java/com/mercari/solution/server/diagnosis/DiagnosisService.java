package com.mercari.solution.server.diagnosis;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mercari.solution.server.agent.DiagnosisAgent;
import com.mercari.solution.server.api.AgentService;
import com.mercari.solution.server.dataflow.DataflowJobReader;
import dev.langchain4j.model.chat.ChatModel;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Receives Dataflow job-failure events (Cloud Logging Log Router -&gt; Pub/Sub push) at
 * /webhook/events/dataflow, diagnoses the failed job with the agent, and notifies the
 * configured endpoints. See docs/deploy/diagnosis.md for the required GCP setup.
 */
public class DiagnosisService {

    private static final Logger LOG = LoggerFactory.getLogger(DiagnosisService.class);

    private static final String ENV_WEBHOOK_TOKEN = "MERCARI_PIPELINE_WEBHOOK_TOKEN";

    /** A job failure may emit several error log entries; diagnose each job at most once per TTL. */
    private static final Duration DEDUP_TTL = Duration.ofHours(6);
    private static final Map<String, Instant> processedJobs = new ConcurrentHashMap<>();

    // Single worker: diagnosis is LLM-bound and bursts are absorbed by the queue.
    // When the queue is full the request is answered 429 so Pub/Sub redelivers later.
    private static final ExecutorService executor = new ThreadPoolExecutor(
            1, 1, 60L, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(16),
            runnable -> {
                final Thread thread = new Thread(runnable, "pipeline-diagnosis");
                thread.setDaemon(true);
                return thread;
            });

    public record DataflowJobEvent(
            String jobId,
            String projectId,
            String region,
            String messageText) {
    }

    public static void serveDataflowEvent(
            final HttpServletRequest request,
            final HttpServletResponse response) {

        try {
            if (!authorized(request)) {
                response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                return;
            }

            final String body;
            try (final Reader reader = new InputStreamReader(request.getInputStream(), StandardCharsets.UTF_8)) {
                final StringBuilder sb = new StringBuilder();
                final char[] buffer = new char[4096];
                int read;
                while ((read = reader.read(buffer)) >= 0) {
                    sb.append(buffer, 0, read);
                }
                body = sb.toString();
            }

            final DataflowJobEvent event = parseDataflowEvent(body);
            if (event == null || event.jobId() == null || event.jobId().isBlank()) {
                // Malformed or non-job events are acked so Pub/Sub does not retry them forever
                LOG.warn("Ignored dataflow event without job id: {}", truncate(body, 1000));
                response.setStatus(HttpServletResponse.SC_NO_CONTENT);
                return;
            }

            if (!markProcessed(event.jobId())) {
                LOG.info("Skipped already-diagnosed dataflow job: {}", event.jobId());
                response.setStatus(HttpServletResponse.SC_NO_CONTENT);
                return;
            }

            final ChatModel chatModel = (ChatModel) request.getServletContext()
                    .getAttribute(AgentService.CONTEXT_ATTRIBUTE_CHAT_MODEL);
            try {
                executor.execute(() -> diagnoseAndNotify(event, chatModel));
            } catch (final RejectedExecutionException e) {
                // Undo dedup so the redelivered event is processed
                processedJobs.remove(event.jobId());
                LOG.warn("Diagnosis queue is full, requesting redelivery for job: {}", event.jobId());
                response.setStatus(429);
                return;
            }
            response.setStatus(HttpServletResponse.SC_NO_CONTENT);
        } catch (final Throwable e) {
            LOG.error("Failed to handle dataflow event", e);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Parse a Pub/Sub push envelope (message.data holds a base64ed Cloud Logging LogEntry)
     * or, for manual invocation, a raw LogEntry JSON body.
     */
    static DataflowJobEvent parseDataflowEvent(final String body) {
        if (body == null || body.isBlank()) {
            return null;
        }
        try {
            final JsonElement element = JsonParser.parseString(body);
            if (!element.isJsonObject()) {
                return null;
            }
            JsonObject logEntry = element.getAsJsonObject();
            if (logEntry.has("message") && logEntry.get("message").isJsonObject()) {
                final JsonObject message = logEntry.getAsJsonObject("message");
                if (!message.has("data")) {
                    return null;
                }
                final String decoded = new String(
                        Base64.getDecoder().decode(message.get("data").getAsString()),
                        StandardCharsets.UTF_8);
                final JsonElement decodedElement = JsonParser.parseString(decoded);
                if (!decodedElement.isJsonObject()) {
                    return null;
                }
                logEntry = decodedElement.getAsJsonObject();
            }

            if (!logEntry.has("resource") || !logEntry.get("resource").isJsonObject()) {
                return null;
            }
            final JsonObject resource = logEntry.getAsJsonObject("resource");
            if (!resource.has("labels") || !resource.get("labels").isJsonObject()) {
                return null;
            }
            final JsonObject labels = resource.getAsJsonObject("labels");

            final String jobId = getString(labels, "job_id");
            final String projectId = getString(labels, "project_id");
            final String region = getString(labels, "region");

            String messageText = getString(logEntry, "textPayload");
            if (messageText == null
                    && logEntry.has("jsonPayload") && logEntry.get("jsonPayload").isJsonObject()) {
                messageText = getString(logEntry.getAsJsonObject("jsonPayload"), "message");
            }

            return new DataflowJobEvent(jobId, projectId, region, messageText);
        } catch (final Exception e) {
            LOG.warn("Failed to parse dataflow event: {}", e.getMessage());
            return null;
        }
    }

    /** Returns true when the job has not been diagnosed within the dedup TTL. */
    static boolean markProcessed(final String jobId) {
        final Instant now = Instant.now();
        processedJobs.entrySet().removeIf(entry -> entry.getValue().plus(DEDUP_TTL).isBefore(now));
        return processedJobs.putIfAbsent(jobId, now) == null;
    }

    private static void diagnoseAndNotify(final DataflowJobEvent event, final ChatModel chatModel) {
        try {
            LOG.info("Diagnosing failed dataflow job: {}", event.jobId());
            final String facts = DataflowJobReader.listJobErrors(
                    event.jobId(), event.projectId(), event.region());

            String diagnosis = null;
            if (chatModel != null) {
                try {
                    diagnosis = DiagnosisAgent.diagnose(chatModel, createFactsMessage(event, facts));
                } catch (final Throwable e) {
                    LOG.error("Diagnosis agent failed for job: " + event.jobId(), e);
                }
            } else {
                LOG.warn("ChatModel is not initialized. Notifying raw error facts without diagnosis.");
            }

            DiagnosisNotifier.notify(event, facts, diagnosis);
        } catch (final Throwable e) {
            LOG.error("Failed to diagnose dataflow job: " + event.jobId(), e);
        }
    }

    static String createFactsMessage(final DataflowJobEvent event, final String facts) {
        final StringBuilder message = new StringBuilder();
        message.append("A Dataflow job failure event was received.\n");
        message.append("- jobId: ").append(event.jobId()).append("\n");
        if (event.messageText() != null && !event.messageText().isBlank()) {
            message.append("- event message: ").append(truncate(event.messageText(), 2000)).append("\n");
        }
        message.append("\nCollected error information:\n\n").append(facts);
        return message.toString();
    }

    private static boolean authorized(final HttpServletRequest request) {
        final String token = System.getenv(ENV_WEBHOOK_TOKEN);
        if (token == null || token.isBlank()) {
            // No shared secret configured: rely on infrastructure-level auth (IAP / push OIDC)
            return true;
        }
        String provided = request.getParameter("token");
        if (provided == null) {
            provided = request.getHeader("X-Mercari-Pipeline-Token");
        }
        return provided != null && MessageDigest.isEqual(
                token.getBytes(StandardCharsets.UTF_8),
                provided.getBytes(StandardCharsets.UTF_8));
    }

    private static String getString(final JsonObject object, final String key) {
        return object.has(key) && object.get(key).isJsonPrimitive()
                ? object.get(key).getAsString()
                : null;
    }

    private static String truncate(final String text, final int maxLength) {
        return text.length() <= maxLength ? text : text.substring(0, maxLength) + "...";
    }

}
