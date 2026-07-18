package com.mercari.solution.util.cloud.google;

import com.google.cloud.logging.LogEntry;
import com.google.cloud.logging.Payload;
import com.google.cloud.logging.Severity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public class LoggingUtilTest {

    @Test
    public void testCreateDataflowErrorLogFilter() {
        final String filter = LoggingUtil.createDataflowErrorLogFilter(
                "2026-07-17_22_25_11-123", Instant.parse("2026-07-17T10:00:00Z"));
        Assertions.assertEquals(
                "resource.type=\"dataflow_step\""
                        + " AND resource.labels.job_id=\"2026-07-17_22_25_11-123\""
                        + " AND severity>=ERROR"
                        + " AND timestamp>=\"2026-07-17T10:00:00Z\"",
                filter);

        final String noTime = LoggingUtil.createDataflowErrorLogFilter("job", null);
        Assertions.assertFalse(noTime.contains("timestamp"), noTime);
    }

    @Test
    public void testExtractText() {
        final LogEntry stringEntry = LogEntry.newBuilder(Payload.StringPayload.of("plain error")).build();
        Assertions.assertEquals("plain error", LoggingUtil.extractText(stringEntry));

        final LogEntry jsonEntry = LogEntry.newBuilder(Payload.JsonPayload.of(Map.of(
                "message", "Error processing element",
                "exception", "java.lang.NullPointerException\n\tat com.mercari.solution.Foo.bar(Foo.java:12)"
        ))).build();
        final String text = LoggingUtil.extractText(jsonEntry);
        Assertions.assertTrue(text.contains("Error processing element"), text);
        Assertions.assertTrue(text.contains("NullPointerException"), text);
        Assertions.assertTrue(text.contains("Foo.java:12"), text);
    }

    @Test
    public void testSummarizeErrorLogsDeduplicates() {
        final LogEntry repeated = LogEntry.newBuilder(Payload.StringPayload.of(
                        "java.lang.IllegalStateException: broken\n\tat com.mercari.solution.Foo.bar(Foo.java:12)"))
                .setSeverity(Severity.ERROR)
                .build();
        final LogEntry other = LogEntry.newBuilder(Payload.StringPayload.of("different failure"))
                .setSeverity(Severity.ERROR)
                .build();

        final String result = LoggingUtil.summarizeErrorLogs(List.of(repeated, repeated, repeated, other));
        Assertions.assertTrue(result.contains("4 entries, 2 distinct"), result);
        Assertions.assertTrue(result.contains("(x3)"), result);
        Assertions.assertTrue(result.contains("(x1)"), result);
        Assertions.assertTrue(result.contains("IllegalStateException: broken"), result);
        Assertions.assertTrue(result.contains("different failure"), result);
    }

    @Test
    public void testSummarizeErrorLogsEmpty() {
        final String result = LoggingUtil.summarizeErrorLogs(List.of());
        Assertions.assertTrue(result.startsWith("No error log entries"), result);
    }

}
