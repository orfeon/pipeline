package com.mercari.solution.server.agent.tool;

import dev.langchain4j.agent.tool.P;
import dev.langchain4j.agent.tool.Tool;

/** Agent tools over launched jobs (Dataflow / Cloud Run Job): wrappers of the MCP tools get-job / list-job-errors / list-failed-jobs / get-job-logs. */
public class JobTools {

    @Tool(name = "getJob", value = """
        Get the status of a launched pipeline job. Dataflow: a job id (e.g. '2026-07-17_22_25_11-123...')
        or an exact job name — returns state, timing, SDK version, labels, and the pipeline config recovered
        from the job's launch parameters. Cloud Run Job (runner 'direct'): the execution name from
        launchPipeline (projects/.../jobs/.../executions/...) — returns state, timings, task counts,
        conditions, log and console links; with runner 'direct' and no job, lists the latest executions of
        the server's configured Cloud Run Job. Use this first when a user asks about a specific job.
    """)
    public String getJob(
            @P(name = "job", description = "Dataflow job id / exact job name, or a Cloud Run execution name; omit with runner 'direct' to list the latest executions.", required = false) String job,
            @P(name = "runner", description = "dataflow | direct (default: inferred from the job reference)", required = false) String runner,
            @P(name = "project", description = "GCP project id. Defaults to the server's configured project.", required = false) String project,
            @P(name = "region", description = "Region. Defaults to the server's configured region.", required = false) String region) {
        return McpToolBridge.call("get-job", McpToolBridge.args("job", job, "runner", runner, "project", project, "region", region));
    }

    @Tool(name = "listJobErrors", value = """
        Collect the error information of a launched job: Dataflow job status, error job messages from
        the Dataflow service and deduplicated worker error logs (with stack traces) from Cloud Logging, or
        a Cloud Run Job execution's status / conditions and its deduplicated error logs. Use this when
        diagnosing why a job failed. If the result contains Java stack traces, pass them to
        resolveStackTrace to see the failing source code; use getJobLogs for the surrounding context.
    """)
    public String listJobErrors(
            @P(name = "job", description = "Dataflow job id / exact job name, or a Cloud Run execution name.") String job,
            @P(name = "runner", description = "dataflow | direct (default: inferred from the job reference)", required = false) String runner,
            @P(name = "project", description = "GCP project id. Defaults to the server's configured project.", required = false) String project,
            @P(name = "region", description = "Region. Defaults to the server's configured region.", required = false) String region) {
        return McpToolBridge.call("list-job-errors", McpToolBridge.args("job", job, "runner", runner, "project", project, "region", region));
    }

    @Tool(name = "listFailedJobs", value = """
        List jobs that failed recently: Dataflow jobs and, when the server has a configured Cloud Run Job,
        its failed executions. Use this when the user mentions a failure but does not know the job id,
        or to check whether anything failed lately.
    """)
    public String listFailedJobs(
            @P(name = "hours", description = "Look-back window in hours. Defaults to 24.", required = false) Integer hours,
            @P(name = "project", description = "GCP project id. Defaults to the server's configured project.", required = false) String project,
            @P(name = "region", description = "Region. Defaults to the server's configured region.", required = false) String region) {
        return McpToolBridge.call("list-failed-jobs", McpToolBridge.args("hours", hours, "project", project, "region", region));
    }

    @Tool(name = "getJobLogs", value = """
        Read a job's Cloud Logging entries (Dataflow worker / launcher logs, or a Cloud Run Job execution's
        container logs): the latest entries at or above minSeverity (default INFO), optionally only those
        containing a text. Use it to see what happened around an error, the feature plan report a job
        logged at startup, or progress messages; listJobErrors is the deduplicated error summary.
    """)
    public String getJobLogs(
            @P(name = "job", description = "Dataflow job id / exact job name, or a Cloud Run execution name.") String job,
            @P(name = "runner", description = "dataflow | direct (default: inferred from the job reference)", required = false) String runner,
            @P(name = "minSeverity", description = "DEBUG | INFO | NOTICE | WARNING | ERROR | CRITICAL (default INFO)", required = false) String minSeverity,
            @P(name = "contains", description = "Only entries containing this text", required = false) String contains,
            @P(name = "limit", description = "Maximum entries (default 100, at most 300)", required = false) Integer limit,
            @P(name = "project", description = "GCP project id. Defaults to the server's configured project.", required = false) String project,
            @P(name = "region", description = "Region. Defaults to the server's configured region.", required = false) String region) {
        return McpToolBridge.call("get-job-logs", McpToolBridge.args("job", job, "runner", runner, "minSeverity", minSeverity,
                "contains", contains, "limit", limit, "project", project, "region", region));
    }

    public static JobTools create() {
        return new JobTools();
    }

}
