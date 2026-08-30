package com.mercari.solution.server.agent.tool;

import dev.langchain4j.agent.tool.P;
import dev.langchain4j.agent.tool.Tool;

/** Agent tools over Dataflow jobs: wrappers of the MCP tools {@code get-dataflow-job} / {@code list-job-errors} / {@code list-failed-jobs}. */
public class DataflowReader {

    @Tool("""
        Get a Dataflow job's status and the pipeline config it was launched with.
        Accepts a job id (e.g. '2026-07-17_22_25_11-123...') or an exact job name.
        Returns state, timing, SDK version, labels, and the pipeline config recovered from the
        job's launch parameters. Use this first when a user asks about a specific job.
    """)
    public String getDataflowJob(
            @P(name = "jobIdOrName", description = "Dataflow job id or exact job name.") String jobIdOrName,
            @P(name = "project", description = "GCP project id. Defaults to the server's configured project.", required = false) String project,
            @P(name = "region", description = "Dataflow region (e.g. 'asia-northeast1'). Defaults to the server's configured region.", required = false) String region) {
        return McpToolBridge.call("get-dataflow-job", McpToolBridge.args("jobIdOrName", jobIdOrName, "project", project, "region", region));
    }

    @Tool("""
        Collect the error information of a Dataflow job: job status, error job messages from the
        Dataflow service, and deduplicated worker error logs (including exception stack traces)
        from Cloud Logging. Use this when diagnosing why a job failed. If the result contains
        Java stack traces, pass them to resolveStackTrace to see the failing source code.
    """)
    public String listJobErrors(
            @P(name = "jobIdOrName", description = "Dataflow job id or exact job name.") String jobIdOrName,
            @P(name = "project", description = "GCP project id. Defaults to the server's configured project.", required = false) String project,
            @P(name = "region", description = "Dataflow region. Defaults to the server's configured region.", required = false) String region) {
        return McpToolBridge.call("list-job-errors", McpToolBridge.args("jobIdOrName", jobIdOrName, "project", project, "region", region));
    }

    @Tool("""
        List Dataflow jobs that failed recently. Use this when the user mentions a failure but
        does not know the job id, or to check whether anything failed lately.
    """)
    public String listRecentFailedJobs(
            @P(name = "hours", description = "Look-back window in hours. Defaults to 24.", required = false) Integer hours,
            @P(name = "project", description = "GCP project id. Defaults to the server's configured project.", required = false) String project,
            @P(name = "region", description = "Dataflow region. Defaults to the server's configured region.", required = false) String region) {
        return McpToolBridge.call("list-failed-jobs", McpToolBridge.args("hours", hours, "project", project, "region", region));
    }

    public static DataflowReader create() {
        return new DataflowReader();
    }

}
