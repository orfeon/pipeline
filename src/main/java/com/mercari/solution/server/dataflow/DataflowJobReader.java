package com.mercari.solution.server.dataflow;

import com.google.cloud.ServiceOptions;
import com.google.cloud.logging.LogEntry;
import com.google.dataflow.v1beta3.Job;
import com.google.dataflow.v1beta3.JobMessage;
import com.google.dataflow.v1beta3.JobMessageImportance;
import com.google.dataflow.v1beta3.JobState;
import com.google.dataflow.v1beta3.JobView;
import com.google.dataflow.v1beta3.ListJobsRequest;
import com.mercari.solution.server.ServerVersion;
import com.mercari.solution.util.cloud.google.DataflowUtil;
import com.mercari.solution.util.cloud.google.LoggingUtil;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Read-only Dataflow job diagnostics shared by the agent tools and the MCP tools.
 * All methods return LLM-consumable text and never throw: failures come back as ERROR strings.
 */
public class DataflowJobReader {

    public static final String VERSION_LABEL = "mercari-pipeline-version";

    private static final String ENV_DATAFLOW_PROJECT = "MERCARI_PIPELINE_DATAFLOW_PROJECT";
    private static final String ENV_DATAFLOW_REGION = "MERCARI_PIPELINE_DATAFLOW_REGION";

    // Dataflow job ids look like 2026-07-17_22_25_11-1234567890123456789
    private static final Pattern JOB_ID_PATTERN = Pattern
            .compile("^\\d{4}-\\d{2}-\\d{2}_\\d{2}_\\d{2}_\\d{2}-\\d+$");

    private static final int NAME_SEARCH_LIMIT = 200;
    private static final int MAX_JOB_MESSAGES = 100;
    private static final int MAX_LOG_ENTRIES = 300;
    private static final int MAX_CONFIG_LENGTH = 8000;

    public static String getJob(final String jobIdOrName, final String projectArg, final String regionArg) {
        try {
            final String project = resolveProject(projectArg);
            final String region = resolveRegion(regionArg);
            final Job job = resolveJob(jobIdOrName, project, region);
            if (job == null) {
                return jobNotFoundMessage(jobIdOrName, project, region);
            }

            final StringBuilder result = new StringBuilder(DataflowUtil.formatJob(job));
            appendVersionSkewNote(result, job);

            final String config = DataflowUtil.getPipelineOption(job, "config");
            if (config != null && !config.isBlank()) {
                result.append("\n## Pipeline config (from job parameters)\n```yaml\n");
                if (config.length() > MAX_CONFIG_LENGTH) {
                    result.append(config, 0, MAX_CONFIG_LENGTH).append("\n... (truncated)\n");
                } else {
                    result.append(config).append("\n");
                }
                result.append("```\n");
            } else {
                result.append("\nNo 'config' parameter found on the job "
                        + "(the job may not have been launched from a mercari/pipeline flex template).\n");
            }
            return result.toString();
        } catch (final Throwable e) {
            return "ERROR: failed to get Dataflow job: " + e.getMessage();
        }
    }

    public static String listJobErrors(final String jobIdOrName, final String projectArg, final String regionArg) {
        try {
            final String project = resolveProject(projectArg);
            final String region = resolveRegion(regionArg);
            final Job job = resolveJob(jobIdOrName, project, region);
            if (job == null) {
                return jobNotFoundMessage(jobIdOrName, project, region);
            }

            final StringBuilder result = new StringBuilder(DataflowUtil.formatJob(job));
            appendVersionSkewNote(result, job);
            result.append("\n");

            final List<JobMessage> messages = DataflowUtil.listJobMessages(
                    project, region, job.getId(), JobMessageImportance.JOB_MESSAGE_ERROR, MAX_JOB_MESSAGES);
            result.append("## Job messages (severity ERROR and above)\n");
            if (messages.isEmpty()) {
                result.append("No error job messages.\n");
            } else {
                result.append(DataflowUtil.formatJobMessages(messages));
            }
            result.append("\n");

            final Instant startTime = job.hasCreateTime()
                    ? DataflowUtil.toInstant(job.getCreateTime())
                    : null;
            try {
                final List<LogEntry> entries = LoggingUtil.listEntries(
                        project,
                        LoggingUtil.createDataflowErrorLogFilter(job.getId(), startTime),
                        MAX_LOG_ENTRIES);
                result.append(LoggingUtil.summarizeErrorLogs(entries));
            } catch (final Exception e) {
                result.append("## Worker error logs\n")
                        .append("Failed to read Cloud Logging entries: ").append(e.getMessage()).append("\n");
            }

            result.append("\nIf the output above contains Java stack traces with com.mercari.solution frames, "
                    + "pass the stack trace text to the resolveStackTrace tool to see the failing source code.\n");
            return result.toString();
        } catch (final Throwable e) {
            return "ERROR: failed to list Dataflow job errors: " + e.getMessage();
        }
    }

    public static String listRecentFailedJobs(final Integer hours, final String projectArg, final String regionArg) {
        try {
            final String project = resolveProject(projectArg);
            final String region = resolveRegion(regionArg);
            final int windowHours = Optional.ofNullable(hours).filter(h -> h > 0).orElse(24);
            final Instant threshold = Instant.now().minus(windowHours, ChronoUnit.HOURS);

            final List<Job> failed = DataflowUtil
                    .listJobs(project, region, ListJobsRequest.Filter.TERMINATED, NAME_SEARCH_LIMIT)
                    .stream()
                    .filter(job -> JobState.JOB_STATE_FAILED.equals(job.getCurrentState()))
                    .filter(job -> job.hasCreateTime()
                            && DataflowUtil.toInstant(job.getCreateTime()).isAfter(threshold))
                    .toList();
            if (failed.isEmpty()) {
                return String.format("No failed Dataflow jobs in the last %d hours (project=%s, region=%s).",
                        windowHours, project, region);
            }
            return String.format("## Failed Dataflow jobs in the last %d hours (project=%s, region=%s)%n",
                    windowHours, project, region)
                    + DataflowUtil.formatJobs(failed)
                    + "\nUse listJobErrors with a job id to see why a job failed.";
        } catch (final Throwable e) {
            return "ERROR: failed to list Dataflow jobs: " + e.getMessage();
        }
    }

    private static Job resolveJob(final String jobIdOrName, final String project, final String region)
            throws Exception {

        if (jobIdOrName == null || jobIdOrName.isBlank()) {
            throw new IllegalArgumentException("job id or job name is required");
        }
        final String key = jobIdOrName.trim();
        if (JOB_ID_PATTERN.matcher(key).matches()) {
            return DataflowUtil.getJob(project, region, key, JobView.JOB_VIEW_ALL);
        }
        final Job byName = DataflowUtil.findJobByName(project, region, key, NAME_SEARCH_LIMIT);
        if (byName == null) {
            return null;
        }
        // listJobs returns summary views; re-fetch with full view for pipeline options
        return DataflowUtil.getJob(project, region, byName.getId(), JobView.JOB_VIEW_ALL);
    }

    private static void appendVersionSkewNote(final StringBuilder result, final Job job) {
        final String jobVersion = job.getLabelsMap().get(VERSION_LABEL);
        final String serverVersion = ServerVersion.get();
        if (jobVersion != null && serverVersion != null && !jobVersion.equals(serverVersion)) {
            result.append("- NOTE: this job was launched from version '").append(jobVersion)
                    .append("' but this server (and its bundled source code) is version '")
                    .append(serverVersion)
                    .append("'. Source line numbers may not match the job exactly.\n");
        }
    }

    private static String jobNotFoundMessage(final String jobIdOrName, final String project, final String region) {
        return String.format("Dataflow job not found: '%s' (project=%s, region=%s). "
                        + "Specify a job id or exact job name; use listRecentFailedJobs to discover jobs.",
                jobIdOrName, project, region);
    }

    private static String resolveProject(final String projectArg) {
        if (projectArg != null && !projectArg.isBlank()) {
            return projectArg.trim();
        }
        final String env = System.getenv(ENV_DATAFLOW_PROJECT);
        if (env != null && !env.isBlank()) {
            return env.trim();
        }
        final String defaultProject = ServiceOptions.getDefaultProjectId();
        if (defaultProject != null) {
            return defaultProject;
        }
        throw new IllegalArgumentException(
                "project could not be resolved: pass it explicitly or set " + ENV_DATAFLOW_PROJECT);
    }

    private static String resolveRegion(final String regionArg) {
        if (regionArg != null && !regionArg.isBlank()) {
            return regionArg.trim();
        }
        final String env = System.getenv(ENV_DATAFLOW_REGION);
        if (env != null && !env.isBlank()) {
            return env.trim();
        }
        throw new IllegalArgumentException(
                "region could not be resolved: pass it explicitly or set " + ENV_DATAFLOW_REGION);
    }

}
