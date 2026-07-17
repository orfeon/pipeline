package com.mercari.solution.server.diagnosis;

import com.google.gson.JsonObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DiagnosisNotifierTest {

    private static final DiagnosisService.DataflowJobEvent EVENT = new DiagnosisService.DataflowJobEvent(
            "2026-07-17_22_25_11-123", "myproject", "asia-northeast1", "Workflow failed");

    @Test
    public void testParseDiagnosis() {
        final String json = "{\"causeCategory\":\"config\",\"summary\":\"bad table name\"}";
        final JsonObject plain = DiagnosisNotifier.parseDiagnosis(json);
        Assertions.assertEquals("config", plain.get("causeCategory").getAsString());

        final JsonObject fenced = DiagnosisNotifier.parseDiagnosis("```json\n" + json + "\n```");
        Assertions.assertEquals("config", fenced.get("causeCategory").getAsString());

        final JsonObject raw = DiagnosisNotifier.parseDiagnosis("free text verdict");
        Assertions.assertEquals("free text verdict", raw.get("raw").getAsString());

        Assertions.assertNull(DiagnosisNotifier.parseDiagnosis(null));
        Assertions.assertNull(DiagnosisNotifier.parseDiagnosis("  "));
    }

    @Test
    public void testBuildRecordWithDiagnosis() {
        final JsonObject record = DiagnosisNotifier.buildRecord(
                EVENT, "facts text",
                "{\"causeCategory\":\"config\",\"summary\":\"s\",\"recommendation\":\"r\"}");
        Assertions.assertEquals("2026-07-17_22_25_11-123", record.get("jobId").getAsString());
        Assertions.assertEquals("myproject", record.get("projectId").getAsString());
        Assertions.assertTrue(record.has("diagnosedAt"));
        Assertions.assertEquals("config",
                record.getAsJsonObject("diagnosis").get("causeCategory").getAsString());
        // facts are only shipped when there is no diagnosis
        Assertions.assertFalse(record.has("facts"));
    }

    @Test
    public void testBuildRecordFallsBackToFacts() {
        final JsonObject record = DiagnosisNotifier.buildRecord(EVENT, "collected facts", null);
        Assertions.assertFalse(record.has("diagnosis"));
        Assertions.assertEquals("collected facts", record.get("facts").getAsString());
    }

    @Test
    public void testBuildSlackText() {
        final JsonObject record = DiagnosisNotifier.buildRecord(
                EVENT, null,
                "{\"causeCategory\":\"config\",\"confidence\":\"high\",\"summary\":\"bad table\",\"recommendation\":\"fix the table name\"}");
        final String text = DiagnosisNotifier.buildSlackText(record);
        Assertions.assertTrue(text.contains("2026-07-17_22_25_11-123"), text);
        Assertions.assertTrue(text.contains("*cause*: config (confidence: high)"), text);
        Assertions.assertTrue(text.contains("*summary*: bad table"), text);
        Assertions.assertTrue(text.contains("*recommendation*: fix the table name"), text);

        final String noDiagnosis = DiagnosisNotifier.buildSlackText(
                DiagnosisNotifier.buildRecord(EVENT, "facts", null));
        Assertions.assertTrue(noDiagnosis.contains("No automated diagnosis available"), noDiagnosis);
    }

}
