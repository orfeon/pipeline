package com.mercari.solution.server.diagnosis;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class DiagnosisServiceTest {

    private static final String LOG_ENTRY_JSON = """
            {
              "resource": {
                "type": "dataflow_step",
                "labels": {
                  "job_id": "2026-07-17_22_25_11-1234567890123456789",
                  "project_id": "myproject",
                  "region": "asia-northeast1"
                }
              },
              "textPayload": "Workflow failed. Causes: something broke",
              "severity": "ERROR"
            }
            """;

    @Test
    public void testParsePubSubPushEnvelope() {
        final String encoded = Base64.getEncoder()
                .encodeToString(LOG_ENTRY_JSON.getBytes(StandardCharsets.UTF_8));
        final String envelope = """
                {
                  "message": {
                    "data": "%s",
                    "messageId": "1234"
                  },
                  "subscription": "projects/myproject/subscriptions/dataflow-failures"
                }
                """.formatted(encoded);

        final DiagnosisService.DataflowJobEvent event = DiagnosisService.parseDataflowEvent(envelope);
        Assertions.assertNotNull(event);
        Assertions.assertEquals("2026-07-17_22_25_11-1234567890123456789", event.jobId());
        Assertions.assertEquals("myproject", event.projectId());
        Assertions.assertEquals("asia-northeast1", event.region());
        Assertions.assertEquals("Workflow failed. Causes: something broke", event.messageText());
    }

    @Test
    public void testParseRawLogEntry() {
        final DiagnosisService.DataflowJobEvent event = DiagnosisService.parseDataflowEvent(LOG_ENTRY_JSON);
        Assertions.assertNotNull(event);
        Assertions.assertEquals("2026-07-17_22_25_11-1234567890123456789", event.jobId());
    }

    @Test
    public void testParseJsonPayloadMessage() {
        final String json = """
                {
                  "resource": {"type": "dataflow_step", "labels": {"job_id": "job-1"}},
                  "jsonPayload": {"message": "worker exception"}
                }
                """;
        final DiagnosisService.DataflowJobEvent event = DiagnosisService.parseDataflowEvent(json);
        Assertions.assertNotNull(event);
        Assertions.assertEquals("job-1", event.jobId());
        Assertions.assertEquals("worker exception", event.messageText());
    }

    @Test
    public void testParseInvalidBodies() {
        Assertions.assertNull(DiagnosisService.parseDataflowEvent(null));
        Assertions.assertNull(DiagnosisService.parseDataflowEvent(""));
        Assertions.assertNull(DiagnosisService.parseDataflowEvent("not json"));
        Assertions.assertNull(DiagnosisService.parseDataflowEvent("{}"));
        Assertions.assertNull(DiagnosisService.parseDataflowEvent("{\"message\": {\"messageId\": \"1\"}}"));
        // an envelope whose data is not a LogEntry
        final String encoded = Base64.getEncoder().encodeToString("\"just a string\"".getBytes(StandardCharsets.UTF_8));
        Assertions.assertNull(DiagnosisService.parseDataflowEvent(
                "{\"message\": {\"data\": \"" + encoded + "\"}}"));
    }

    @Test
    public void testMarkProcessedDeduplicates() {
        final String jobId = "dedup-test-" + System.nanoTime();
        Assertions.assertTrue(DiagnosisService.markProcessed(jobId));
        Assertions.assertFalse(DiagnosisService.markProcessed(jobId));
    }

    @Test
    public void testCreateFactsMessage() {
        final DiagnosisService.DataflowJobEvent event = new DiagnosisService.DataflowJobEvent(
                "job-1", "myproject", "asia-northeast1", "Workflow failed");
        final String message = DiagnosisService.createFactsMessage(event, "## Dataflow job job-1\n...");
        Assertions.assertTrue(message.contains("jobId: job-1"), message);
        Assertions.assertTrue(message.contains("Workflow failed"), message);
        Assertions.assertTrue(message.contains("## Dataflow job job-1"), message);
    }

}
