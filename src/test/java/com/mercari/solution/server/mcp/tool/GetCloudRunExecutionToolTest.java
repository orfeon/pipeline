package com.mercari.solution.server.mcp.tool;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.modelcontextprotocol.spec.McpSchema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

/** The Cloud Run calls need credentials; these cover the summary shape and the argument checks. */
public class GetCloudRunExecutionToolTest {

    private static String text(final McpSchema.CallToolResult result) {
        return ((McpSchema.TextContent) result.content().getFirst()).text();
    }

    @Test
    public void testSummarizeExecution() {
        final JsonObject execution = JsonParser.parseString("""
                {
                  "name": "projects/p/locations/asia-northeast1/jobs/pipeline/executions/pipeline-abc12",
                  "createTime": "2026-08-30T00:00:00Z",
                  "startTime": "2026-08-30T00:00:05Z",
                  "completionTime": "2026-08-30T00:10:00Z",
                  "taskCount": 1,
                  "failedCount": 1,
                  "logUri": "https://console.cloud.google.com/logs/...",
                  "conditions": [
                    {"type": "Completed", "state": "CONDITION_FAILED", "message": "Task pipeline-abc12-task0 failed with exit code 1", "lastTransitionTime": "..."}
                  ],
                  "template": {"containers": []}
                }
                """).getAsJsonObject();
        final JsonObject summary = GetCloudRunExecutionTool.summarize(execution, "p");
        Assertions.assertEquals("pipeline-abc12", summary.get("id").getAsString());
        Assertions.assertEquals("FAILED", summary.get("state").getAsString());
        Assertions.assertEquals(1, summary.get("failedCount").getAsInt());
        Assertions.assertTrue(summary.get("logUri").getAsString().startsWith("https://"));
        Assertions.assertFalse(summary.has("template"), "raw template must not be copied");
        final JsonObject condition = summary.getAsJsonArray("conditions").get(0).getAsJsonObject();
        Assertions.assertEquals("CONDITION_FAILED", condition.get("state").getAsString());
        Assertions.assertTrue(condition.get("message").getAsString().contains("exit code 1"));
        Assertions.assertFalse(condition.has("lastTransitionTime"));
        Assertions.assertTrue(summary.get("consoleUrl").getAsString().contains("pipeline-abc12"), summary::toString);
        Assertions.assertEquals("p", GetCloudRunExecutionTool.projectOf(execution.get("name").getAsString()));
    }

    @Test
    public void testRunningExecution() {
        final JsonObject execution = JsonParser.parseString("""
                {"name": "projects/p/locations/r/jobs/j/executions/e", "createTime": "2026-08-30T00:00:00Z", "runningCount": 1}
                """).getAsJsonObject();
        Assertions.assertEquals("RUNNING", GetCloudRunExecutionTool.summarize(execution, "p").get("state").getAsString());
    }

    @Test
    public void testMissingArgumentsWithoutServerDefaults() {
        // no executionName / jobName and no MERCARI_PIPELINE_LAUNCH_DIRECT_JOB in this test environment
        org.junit.jupiter.api.Assumptions.assumeTrue(System.getenv("MERCARI_PIPELINE_LAUNCH_DIRECT_JOB") == null && System.getenv("MERCARI_PIPELINE_LAUNCH_JOB") == null);
        final McpSchema.CallToolResult result = new GetCloudRunExecutionTool().sync(null,
                new McpSchema.CallToolRequest("get-cloud-run-execution", Map.of("project", "p", "region", "r")));
        Assertions.assertTrue(result.isError(), text(result));
        Assertions.assertTrue(text(result).contains("executionName or jobName"), text(result));
    }

}
