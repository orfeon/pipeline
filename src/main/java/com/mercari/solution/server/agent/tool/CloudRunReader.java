package com.mercari.solution.server.agent.tool;

import dev.langchain4j.agent.tool.P;
import dev.langchain4j.agent.tool.Tool;

/** Agent tool: status of Cloud Run Job executions — wrapper of the MCP tool {@code get-cloud-run-execution}. */
public class CloudRunReader {

    @Tool(name = "getCloudRunExecution", value = """
        Get the status of a Cloud Run Job execution created by launchPipeline (runner 'direct'), or list
        the latest executions of a Cloud Run Job. Returns state (RUNNING | SUCCEEDED | FAILED | CANCELLED),
        timings, task counts, conditions with messages, the Cloud Logging URI and the console URL.
    """)
    public String get(
            @P(name = "executionName", description = "Full execution resource name from the launch result (projects/.../executions/...)", required = false) String executionName,
            @P(name = "jobName", description = "Cloud Run Job name to list executions of (default: the server's configured job)", required = false) String jobName,
            @P(name = "project", description = "GCP project (default: the server's)", required = false) String project,
            @P(name = "region", description = "Cloud Run region (default: the server's)", required = false) String region,
            @P(name = "limit", description = "Number of executions to list (default 5)", required = false) Integer limit) {
        return McpToolBridge.call("get-cloud-run-execution", McpToolBridge.args(
                "executionName", executionName, "jobName", jobName, "project", project, "region", region, "limit", limit), "SUCCESS: ");
    }

    public static CloudRunReader create() {
        return new CloudRunReader();
    }

}
