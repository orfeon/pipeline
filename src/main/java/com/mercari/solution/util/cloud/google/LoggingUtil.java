package com.mercari.solution.util.cloud.google;

import com.google.api.gax.paging.Page;
import com.google.cloud.MonitoredResource;
import com.google.cloud.logging.LogEntry;
import com.google.cloud.logging.Logging;
import com.google.cloud.logging.LoggingOptions;
import com.google.cloud.logging.Payload;
import com.google.cloud.logging.Severity;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class LoggingUtil {

    private static final int MAX_DISTINCT_ERRORS = 10;
    private static final int MAX_ERROR_TEXT_LENGTH = 3000;
    private static final int FINGERPRINT_LENGTH = 200;

    public static List<LogEntry> listEntries(
            final String project,
            final String filter,
            final int maxEntries) throws Exception {

        final LoggingOptions options = LoggingOptions.newBuilder()
                .setProjectId(project)
                .build();
        final List<LogEntry> entries = new ArrayList<>();
        try(final Logging logging = options.getService()) {
            Page<LogEntry> page = logging.listLogEntries(
                    Logging.EntryListOption.filter(filter),
                    Logging.EntryListOption.pageSize(Math.min(maxEntries, 1000)));
            while(page != null) {
                for(final LogEntry entry : page.getValues()) {
                    entries.add(entry);
                    if(entries.size() >= maxEntries) {
                        return entries;
                    }
                }
                page = page.hasNextPage() ? page.getNextPage() : null;
            }
        }
        return entries;
    }

    /** Write a structured (jsonPayload) entry, e.g. a diagnosis record queryable later. */
    public static void write(
            final String project,
            final String logName,
            final Map<String, Object> jsonPayload) throws Exception {

        final LoggingOptions options = LoggingOptions.newBuilder()
                .setProjectId(project)
                .build();
        try(final Logging logging = options.getService()) {
            final LogEntry entry = LogEntry.newBuilder(Payload.JsonPayload.of(jsonPayload))
                    .setLogName(logName)
                    .setSeverity(Severity.INFO)
                    .setResource(MonitoredResource.newBuilder("global").build())
                    .build();
            logging.write(List.of(entry));
            logging.flush();
        }
    }

    /**
     * Filter matching Dataflow worker/launcher error logs for a job.
     * Covers worker, harness and launcher logs, which all use resource.type dataflow_step.
     */
    public static String createDataflowErrorLogFilter(final String jobId, final Instant startTime) {
        final StringBuilder filter = new StringBuilder();
        filter.append("resource.type=\"dataflow_step\"");
        filter.append(" AND resource.labels.job_id=\"").append(jobId).append("\"");
        filter.append(" AND severity>=ERROR");
        if(startTime != null) {
            filter.append(" AND timestamp>=\"").append(startTime).append("\"");
        }
        return filter.toString();
    }

    /**
     * Summarize error log entries for LLM consumption: entries with the same leading text are
     * collapsed into one representative with an occurrence count and first/last timestamps.
     */
    public static String summarizeErrorLogs(final List<LogEntry> entries) {
        if(entries.isEmpty()) {
            return "No error log entries found.\n";
        }

        final Map<String, Summary> summaries = new LinkedHashMap<>();
        for(final LogEntry entry : entries) {
            final String text = extractText(entry);
            if(text == null || text.isBlank()) {
                continue;
            }
            final String fingerprint = text.substring(0, Math.min(text.length(), FINGERPRINT_LENGTH));
            final Instant timestamp = entry.getInstantTimestamp();
            summaries.computeIfAbsent(fingerprint, k -> new Summary(text, timestamp)).add(timestamp);
        }

        final StringBuilder result = new StringBuilder();
        result.append("## Worker error logs (")
                .append(entries.size()).append(" entries, ")
                .append(summaries.size()).append(" distinct)\n");
        int distinct = 0;
        for(final Summary summary : summaries.values()) {
            if(distinct >= MAX_DISTINCT_ERRORS) {
                result.append("... (").append(summaries.size() - distinct)
                        .append(" more distinct errors omitted)\n");
                break;
            }
            distinct++;
            result.append("### (x").append(summary.count).append(")");
            if(summary.firstSeen != null) {
                result.append(" first ").append(summary.firstSeen);
            }
            if(summary.lastSeen != null && !summary.lastSeen.equals(summary.firstSeen)) {
                result.append(", last ").append(summary.lastSeen);
            }
            result.append("\n");
            if(summary.text.length() > MAX_ERROR_TEXT_LENGTH) {
                result.append(summary.text, 0, MAX_ERROR_TEXT_LENGTH).append("... (truncated)\n");
            } else {
                result.append(summary.text).append("\n");
            }
        }
        return result.toString();
    }

    /** Extract readable text from a log entry payload, including worker exception stack traces. */
    static String extractText(final LogEntry entry) {
        final Payload<?> payload = entry.getPayload();
        return switch (payload.getType()) {
            case STRING -> ((Payload.StringPayload) payload).getData();
            case JSON -> {
                final Map<String, Object> data = ((Payload.JsonPayload) payload).getDataAsMap();
                final StringBuilder text = new StringBuilder();
                final Object message = data.get("message");
                if(message != null) {
                    text.append(message);
                }
                final Object exception = data.get("exception");
                if(exception != null) {
                    if(!text.isEmpty()) {
                        text.append("\n");
                    }
                    text.append(exception);
                }
                yield text.toString();
            }
            case PROTO -> payload.getData().toString();
        };
    }

    private static class Summary {

        private final String text;
        private final Instant firstSeen;

        private Instant lastSeen;
        private int count;

        Summary(final String text, final Instant firstSeen) {
            this.text = text;
            this.firstSeen = firstSeen;
        }

        void add(final Instant timestamp) {
            this.count++;
            if(timestamp != null && (lastSeen == null || timestamp.isAfter(lastSeen))) {
                this.lastSeen = timestamp;
            }
        }

    }

}
