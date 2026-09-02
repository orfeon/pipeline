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
        return listEntries(project, filter, maxEntries, false);
    }

    /** @param latestFirst read the newest entries first (the result is then reversed to oldest-first order) */
    public static List<LogEntry> listEntries(
            final String project,
            final String filter,
            final int maxEntries,
            final boolean latestFirst) throws Exception {

        final LoggingOptions options = LoggingOptions.newBuilder()
                .setProjectId(project)
                .build();
        final List<LogEntry> entries = new ArrayList<>();
        try(final Logging logging = options.getService()) {
            Page<LogEntry> page = logging.listLogEntries(
                    Logging.EntryListOption.filter(filter),
                    Logging.EntryListOption.sortOrder(Logging.SortingField.TIMESTAMP,
                            latestFirst ? Logging.SortingOrder.DESCENDING : Logging.SortingOrder.ASCENDING),
                    Logging.EntryListOption.pageSize(Math.min(maxEntries, 1000)));
            while(page != null) {
                for(final LogEntry entry : page.getValues()) {
                    entries.add(entry);
                    if(entries.size() >= maxEntries) {
                        if(latestFirst) java.util.Collections.reverse(entries);
                        return entries;
                    }
                }
                page = page.hasNextPage() ? page.getNextPage() : null;
            }
        }
        if(latestFirst) java.util.Collections.reverse(entries);
        return entries;
    }

    /** Worker / harness / launcher logs of a Dataflow job (resource.type dataflow_step), optionally filtered by text. */
    public static String createDataflowLogFilter(final String jobId, final String minSeverity, final Instant since, final String contains) {
        final StringBuilder filter = new StringBuilder();
        filter.append("resource.type=\"dataflow_step\"");
        filter.append(" AND resource.labels.job_id=\"").append(jobId).append("\"");
        appendCommonFilter(filter, minSeverity, since, contains);
        return filter.toString();
    }

    /** Container logs of one Cloud Run Job execution (resource.type cloud_run_job), optionally filtered by text. */
    public static String createCloudRunJobLogFilter(final String job, final String execution, final String minSeverity, final Instant since, final String contains) {
        final StringBuilder filter = new StringBuilder();
        filter.append("resource.type=\"cloud_run_job\"");
        filter.append(" AND resource.labels.job_name=\"").append(job).append("\"");
        filter.append(" AND labels.\"run.googleapis.com/execution_name\"=\"").append(execution).append("\"");
        // a container's plain stdout lines are ingested with severity DEFAULT (only stderr maps to ERROR),
        // so a DEBUG / INFO threshold would exclude every ordinary log line — those thresholds drop the clause
        appendCommonFilter(filter, containerSeverityDropped(minSeverity) ? null : minSeverity, since, contains);
        return filter.toString();
    }

    /** True when {@code minSeverity} would exclude the DEFAULT-severity stdout lines of a container (DEBUG / INFO / none). */
    public static boolean containerSeverityDropped(final String minSeverity) {
        return minSeverity == null || minSeverity.isBlank()
                || List.of("DEFAULT", "DEBUG", "INFO").contains(minSeverity.trim().toUpperCase());
    }

    private static void appendCommonFilter(final StringBuilder filter, final String minSeverity, final Instant since, final String contains) {
        if(minSeverity != null && !minSeverity.isBlank() && !"DEFAULT".equalsIgnoreCase(minSeverity)) {
            filter.append(" AND severity>=").append(minSeverity.trim().toUpperCase());
        }
        if(since != null) {
            filter.append(" AND timestamp>=\"").append(since).append("\"");
        }
        if(contains != null && !contains.isBlank()) {
            // substring match over the text / json payloads
            final String quoted = contains.replace("\\", "\\\\").replace("\"", "\\\"");
            filter.append(" AND (textPayload:\"").append(quoted).append("\" OR jsonPayload.message:\"").append(quoted)
                    .append("\" OR jsonPayload.exception:\"").append(quoted).append("\")");
        }
    }

    /** One line per entry, oldest first: {@code timestamp [SEVERITY] text} (text truncated to maxText, newlines kept indented). */
    public static String formatEntries(final List<LogEntry> entries, final int maxText) {
        final StringBuilder sb = new StringBuilder();
        for(final LogEntry entry : entries) {
            String text = extractText(entry);
            if(text == null) text = "";
            if(text.length() > maxText) text = text.substring(0, maxText) + "... (truncated)";
            sb.append(entry.getInstantTimestamp()).append(" [").append(entry.getSeverity()).append("] ")
                    .append(text.replace("\n", "\n    ")).append('\n');
        }
        return sb.toString();
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
        return createDataflowLogFilter(jobId, "ERROR", startTime, null);
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
