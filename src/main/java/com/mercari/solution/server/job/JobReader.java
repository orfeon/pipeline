package com.mercari.solution.server.job;

import com.google.cloud.logging.LogEntry;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.server.dataflow.DataflowJobReader;
import com.mercari.solution.server.launch.LaunchDefaults;
import com.mercari.solution.util.cloud.google.CloudRunUtil;
import com.mercari.solution.util.cloud.google.LoggingUtil;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Runner-agnostic job observation shared by the MCP tools ({@code get-job}, {@code list-job-errors},
 * {@code get-job-logs}, {@code list-failed-jobs}) and the agent tools: Dataflow jobs (delegating to
 * {@link DataflowJobReader}) and Cloud Run Job executions (the {@code direct/cloudRunJob} launch target).
 * All methods return LLM-consumable text and never throw: failures come back as ERROR strings.
 */
public final class JobReader {

    public enum Runner { dataflow, direct }

    /** A resolved job reference: Dataflow job id / name, or a Cloud Run execution resource name (null = the job's latest executions). */
    public record Ref(Runner runner, String id, String project, String region) {}

    private static final Pattern DATAFLOW_JOB_ID = Pattern.compile("^\\d{4}-\\d{2}-\\d{2}_\\d{2}_\\d{2}_\\d{2}-\\d+$");
    private static final Pattern EXECUTION_NAME = Pattern.compile("^projects/([^/]+)/locations/([^/]+)/jobs/([^/]+)/executions/([^/]+)$");
    private static final int MAX_LOG_ENTRIES = 300;
    private static final int DEFAULT_LOG_ENTRIES = 100;
    private static final int MAX_LOG_TEXT = 600;
    private static final String KEY_JOB = "JOB";
    private static final java.util.Set<String> SEVERITIES = java.util.Set.of("DEFAULT", "DEBUG", "INFO", "NOTICE", "WARNING", "ERROR", "CRITICAL", "ALERT", "EMERGENCY");
    /** One HTTP client for every Cloud Run call (token cached by the util). */
    private static final CloudRunUtil CLOUD_RUN = new CloudRunUtil();

    private JobReader() {}

    /**
     * Decides which runner a job reference belongs to: an explicit {@code runner} wins; otherwise a Cloud Run
     * execution resource name or a Dataflow job id is recognised by shape, and anything else is a Dataflow job name.
     */
    public static Ref resolve(final String job, final String runnerArg, final String projectArg, final String regionArg) {
        final String key = job == null ? null : job.trim();
        final Matcher execution = key == null ? null : EXECUTION_NAME.matcher(key);
        Runner runner = parseRunner(runnerArg);
        if (runner == null) {
            runner = execution != null && execution.matches() ? Runner.direct : Runner.dataflow;
        }
        if (runner == Runner.dataflow) {
            if (key == null || key.isBlank()) {
                throw new IllegalArgumentException("a Dataflow job id or job name is required (or runner: direct to look at Cloud Run Job executions)");
            }
            return new Ref(runner, key, projectArg, regionArg);
        }
        // Cloud Run Job executions
        if (execution != null && execution.matches()) {
            return new Ref(runner, key, execution.group(1), execution.group(2));
        }
        final LaunchDefaults defaults = LaunchDefaults.get();
        final String project = defaults.require("direct", LaunchDefaults.KEY_PROJECT, projectArg);
        final String region = defaults.require("direct", LaunchDefaults.KEY_REGION, regionArg);
        if (key == null || key.isBlank()) {
            return new Ref(runner, null, project, region);
        }
        // a short execution id ('<job>-abc12') of the server's configured Cloud Run Job
        final String jobName = defaults.resolve("direct", KEY_JOB).orElse(null);
        if (jobName == null) {
            throw new IllegalArgumentException("Cloud Run execution '" + key + "' needs its job: pass the full execution name"
                    + " (projects/.../jobs/<job>/executions/<execution>) or set " + LaunchDefaults.envName("direct", KEY_JOB));
        }
        return new Ref(runner, CloudRunUtil.jobName(project, region, jobName) + "/executions/" + key, project, region);
    }

    static Runner parseRunner(final String runnerArg) {
        if (runnerArg == null || runnerArg.isBlank()) return null;
        return switch (runnerArg.trim().toLowerCase()) {
            case "dataflow" -> Runner.dataflow;
            case "direct", "cloudrunjob", "cloud-run", "cloudrun", "run" -> Runner.direct;
            default -> throw new IllegalArgumentException("unknown runner: " + runnerArg + " (dataflow | direct)");
        };
    }

    // ---- get-job ----

    public static String getJob(final String job, final String runnerArg, final String projectArg, final String regionArg, final Integer limit) {
        final Ref ref;
        try {
            ref = resolve(job, runnerArg, projectArg, regionArg);
        } catch (final IllegalArgumentException e) {
            return "ERROR: " + e.getMessage();
        }
        if (ref.runner() == Runner.dataflow) {
            return DataflowJobReader.getJob(ref.id(), ref.project(), ref.region());
        }
        try {
            final CloudRunUtil cloudRun = CLOUD_RUN;
            if (ref.id() != null) {
                return "## Cloud Run Job execution\n" + formatExecution(summarizeExecution(cloudRun.getExecution(ref.id()), ref.project()));
            }
            final String jobName = LaunchDefaults.get().resolve("direct", KEY_JOB)
                    .orElseThrow(() -> new IllegalArgumentException("no Cloud Run Job to list: pass an execution name or set " + LaunchDefaults.envName("direct", KEY_JOB)));
            final JsonObject list = cloudRun.listExecutions(CloudRunUtil.jobName(ref.project(), ref.region(), jobName), limit == null || limit <= 0 ? 5 : Math.min(limit, 100));
            final StringBuilder sb = new StringBuilder("## Latest executions of Cloud Run Job " + jobName + " (" + ref.project() + "/" + ref.region() + ")\n");
            if (!list.has("executions") || !list.get("executions").isJsonArray() || list.getAsJsonArray("executions").isEmpty()) {
                return sb.append("No executions.\n").toString();
            }
            for (final JsonElement e : list.getAsJsonArray("executions")) {
                sb.append(formatExecution(summarizeExecution(e.getAsJsonObject(), ref.project()))).append('\n');
            }
            return sb.toString();
        } catch (final Throwable e) {
            return "ERROR: failed to get Cloud Run Job execution: " + e.getMessage();
        }
    }

    // ---- list-job-errors ----

    public static String listJobErrors(final String job, final String runnerArg, final String projectArg, final String regionArg) {
        final Ref ref;
        try {
            ref = resolve(job, runnerArg, projectArg, regionArg);
        } catch (final IllegalArgumentException e) {
            return "ERROR: " + e.getMessage();
        }
        if (ref.runner() == Runner.dataflow) {
            return DataflowJobReader.listJobErrors(ref.id(), ref.project(), ref.region());
        }
        if (ref.id() == null) {
            return "ERROR: list-job-errors needs one execution: pass its execution name (get-job with runner: direct lists the latest ones)";
        }
        try {
            final JsonObject execution = CLOUD_RUN.getExecution(ref.id());
            final JsonObject summary = summarizeExecution(execution, ref.project());
            final StringBuilder sb = new StringBuilder("## Cloud Run Job execution\n").append(formatExecution(summary)).append('\n');
            final Matcher m = EXECUTION_NAME.matcher(ref.id());
            m.matches();
            final Instant since = execution.has("createTime") ? Instant.parse(execution.get("createTime").getAsString()) : null;
            try {
                final List<LogEntry> entries = LoggingUtil.listEntries(ref.project(),
                        LoggingUtil.createCloudRunJobLogFilter(m.group(3), m.group(4), "ERROR", since, null), MAX_LOG_ENTRIES, false);
                sb.append(LoggingUtil.summarizeErrorLogs(entries));
            } catch (final Exception e) {
                sb.append("## Worker error logs\nFailed to read Cloud Logging entries: ").append(e.getMessage()).append('\n');
            }
            sb.append("\nIf the output above contains Java stack traces with com.mercari.solution frames, "
                    + "pass the stack trace text to the resolveStackTrace tool to see the failing source code.\n");
            return sb.toString();
        } catch (final Throwable e) {
            return "ERROR: failed to list Cloud Run Job execution errors: " + e.getMessage();
        }
    }

    // ---- get-job-logs ----

    public static String getJobLogs(final String job, final String runnerArg, final String projectArg, final String regionArg,
                                    final String minSeverity, final Integer limit, final String contains) {
        final Ref ref;
        try {
            ref = resolve(job, runnerArg, projectArg, regionArg);
        } catch (final IllegalArgumentException e) {
            return "ERROR: " + e.getMessage();
        }
        final int max = limit == null || limit <= 0 ? DEFAULT_LOG_ENTRIES : Math.min(limit, MAX_LOG_ENTRIES);
        final String severity = minSeverity == null || minSeverity.isBlank() ? "INFO" : minSeverity.trim().toUpperCase();
        if (!SEVERITIES.contains(severity)) {
            return "ERROR: unknown minSeverity '" + minSeverity + "' (DEBUG | INFO | NOTICE | WARNING | ERROR | CRITICAL)";
        }
        try {
            final String project;
            final String filter;
            final String title;
            if (ref.runner() == Runner.dataflow) {
                project = DataflowJobReader.resolveProject(ref.project());
                final String region = DataflowJobReader.resolveRegion(ref.region());
                final com.google.dataflow.v1beta3.Job dataflowJob = DataflowJobReader.resolve(ref.id(), project, region);
                if (dataflowJob == null) {
                    return "ERROR: Dataflow job not found: '" + ref.id() + "' (project=" + project + ", region=" + region + ")";
                }
                filter = LoggingUtil.createDataflowLogFilter(dataflowJob.getId(), severity, null, contains);
                title = "Dataflow job " + dataflowJob.getId() + " (" + dataflowJob.getName() + ")";
            } else {
                if (ref.id() == null) {
                    return "ERROR: get-job-logs needs one execution: pass its execution name (get-job with runner: direct lists the latest ones)";
                }
                final Matcher m = EXECUTION_NAME.matcher(ref.id());
                m.matches();
                project = ref.project();
                filter = LoggingUtil.createCloudRunJobLogFilter(m.group(3), m.group(4), severity, null, contains);
                title = "Cloud Run Job execution " + m.group(4) + " (job " + m.group(3) + ")";
            }
            final List<LogEntry> entries = LoggingUtil.listEntries(project, filter, max, true);
            final StringBuilder sb = new StringBuilder("## Logs of ").append(title)
                    .append(" (severity >= ").append(severity).append(contains == null ? "" : ", containing '" + contains + "'")
                    .append(", latest ").append(entries.size()).append(" of at most ").append(max).append(", oldest first)\n");
            if (entries.isEmpty()) {
                return sb.append("No log entries.\n").toString();
            }
            sb.append(LoggingUtil.formatEntries(entries, MAX_LOG_TEXT));
            return sb.toString();
        } catch (final Throwable e) {
            return "ERROR: failed to read job logs: " + e.getMessage();
        }
    }

    // ---- list-failed-jobs ----

    public static String listFailedJobs(final Integer hours, final String projectArg, final String regionArg) {
        final String dataflow = DataflowJobReader.listRecentFailedJobs(hours, projectArg, regionArg);
        // the configured Cloud Run Job (direct launches), when there is one
        final LaunchDefaults defaults = LaunchDefaults.get();
        final Optional<String> jobName = defaults.resolve("direct", KEY_JOB);
        if (jobName.isEmpty()) {
            return dataflow;
        }
        // a Dataflow-side failure (e.g. no Dataflow project configured) must not hide the Cloud Run findings
        final StringBuilder sb = new StringBuilder(dataflow.startsWith("ERROR")
                ? "## Failed Dataflow jobs\n(not listed: " + dataflow.substring("ERROR:".length()).trim() + ")\n" : dataflow);
        try {
            final String project = defaults.require("direct", LaunchDefaults.KEY_PROJECT, projectArg);
            final String region = defaults.require("direct", LaunchDefaults.KEY_REGION, regionArg);
            final int windowHours = Optional.ofNullable(hours).filter(h -> h > 0).orElse(24);
            final Instant threshold = Instant.now().minus(windowHours, ChronoUnit.HOURS);
            final JsonObject list = CLOUD_RUN.listExecutions(CloudRunUtil.jobName(project, region, jobName.get()), 50);
            final JsonArray failed = new JsonArray();
            if (list.has("executions") && list.get("executions").isJsonArray()) {
                for (final JsonElement e : list.getAsJsonArray("executions")) {
                    final JsonObject summary = summarizeExecution(e.getAsJsonObject(), project);
                    final String created = summary.has("createTime") ? summary.get("createTime").getAsString() : null;
                    if ("FAILED".equals(summary.get("state").getAsString()) && created != null && Instant.parse(created).isAfter(threshold)) {
                        failed.add(summary);
                    }
                }
            }
            sb.append("\n\n## Failed executions of Cloud Run Job ").append(jobName.get()).append(" in the last ").append(windowHours)
                    .append(" hours (project=").append(project).append(", region=").append(region).append(")\n");
            if (failed.isEmpty()) {
                sb.append("None.\n");
            } else {
                for (final JsonElement e : failed) sb.append(formatExecution(e.getAsJsonObject())).append('\n');
                sb.append("Use list-job-errors with the execution name to see why an execution failed.\n");
            }
        } catch (final Throwable e) {
            sb.append("\n\n(Cloud Run Job executions not listed: ").append(e.getMessage()).append(")\n");
        }
        return sb.toString();
    }

    // ---- Cloud Run execution summary ----

    private static final List<String> COPIED = List.of(
            "createTime", "startTime", "completionTime", "taskCount",
            "succeededCount", "failedCount", "cancelledCount", "runningCount", "logUri");

    /** Compact view of an Execution resource: identity, derived state, timings, counts, conditions, log / console links. */
    public static JsonObject summarizeExecution(final JsonObject execution, final String project) {
        final JsonObject o = new JsonObject();
        final String name = execution.has("name") ? execution.get("name").getAsString() : null;
        o.addProperty("name", name);
        o.addProperty("id", name == null ? null : CloudRunUtil.lastSegment(name));
        o.addProperty("state", CloudRunUtil.executionState(execution));
        for (final String key : COPIED) {
            if (execution.has(key)) o.add(key, execution.get(key));
        }
        final JsonArray conditions = new JsonArray();
        if (execution.has("conditions") && execution.get("conditions").isJsonArray()) {
            for (final JsonElement c : execution.getAsJsonArray("conditions")) {
                final JsonObject condition = c.getAsJsonObject();
                final JsonObject summary = new JsonObject();
                for (final String key : List.of("type", "state", "message", "reason")) {
                    if (condition.has(key)) summary.add(key, condition.get(key));
                }
                conditions.add(summary);
            }
        }
        o.add("conditions", conditions);
        if (name != null && project != null) o.addProperty("consoleUrl", CloudRunUtil.executionConsoleUrl(name, project));
        return o;
    }

    static String formatExecution(final JsonObject s) {
        final StringBuilder sb = new StringBuilder();
        sb.append("- ").append(text(s, "id")).append(": ").append(text(s, "state"));
        sb.append(" (created ").append(text(s, "createTime"));
        if (s.has("completionTime")) sb.append(", completed ").append(text(s, "completionTime"));
        sb.append(")\n");
        sb.append("  name: ").append(text(s, "name")).append('\n');
        final StringBuilder counts = new StringBuilder();
        for (final String k : List.of("taskCount", "succeededCount", "failedCount", "cancelledCount", "runningCount")) {
            if (s.has(k)) counts.append(counts.isEmpty() ? "" : ", ").append(k).append('=').append(text(s, k));
        }
        if (!counts.isEmpty()) sb.append("  ").append(counts).append('\n');
        if (s.has("conditions")) {
            for (final JsonElement c : s.getAsJsonArray("conditions")) {
                final JsonObject condition = c.getAsJsonObject();
                if (!condition.has("message") && !condition.has("state")) continue;
                sb.append("  condition ").append(text(condition, "type")).append(": ").append(text(condition, "state"));
                if (condition.has("message")) sb.append(" - ").append(text(condition, "message"));
                sb.append('\n');
            }
        }
        if (s.has("logUri")) sb.append("  logs: ").append(text(s, "logUri")).append('\n');
        if (s.has("consoleUrl")) sb.append("  console: ").append(text(s, "consoleUrl")).append('\n');
        return sb.toString();
    }

    private static String text(final JsonObject o, final String key) {
        return o.has(key) && !o.get(key).isJsonNull() ? o.get(key).getAsString() : "?";
    }

}
