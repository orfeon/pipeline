package com.mercari.solution.server.agent.tool;

import com.mercari.solution.server.mcp.tool.GetCloudRunExecutionTool;
import dev.langchain4j.agent.tool.P;
import dev.langchain4j.agent.tool.Tool;
import io.modelcontextprotocol.spec.McpSchema;

import java.util.HashMap;
import java.util.Map;

/** Agent tool: status of Cloud Run Job executions (the {@code direct/cloudRunJob} launch target). */
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

        final Map<String, Object> arguments = new HashMap<>();
        if (executionName != null) arguments.put("executionName", executionName);
        if (jobName != null) arguments.put("jobName", jobName);
        if (project != null) arguments.put("project", project);
        if (region != null) arguments.put("region", region);
        if (limit != null) arguments.put("limit", limit);
        final McpSchema.CallToolResult result = new GetCloudRunExecutionTool().sync(null,
                new McpSchema.CallToolRequest("get-cloud-run-execution", arguments));
        final String text = ((McpSchema.TextContent) result.content().getFirst()).text();
        return (Boolean.TRUE.equals(result.isError()) ? "ERROR: " : "SUCCESS: ") + text;
    }

    public static CloudRunReader create() {
        return new CloudRunReader();
    }

}
