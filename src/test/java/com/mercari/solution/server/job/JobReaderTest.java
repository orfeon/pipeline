package com.mercari.solution.server.job;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mercari.solution.util.cloud.google.LoggingUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.time.Instant;

/** The cloud calls need credentials; these cover reference resolution, the execution summary and the log filters. */
public class JobReaderTest {

    @Test
    public void testResolveInfersTheRunner() {
        final JobReader.Ref dataflow = JobReader.resolve("2026-08-29_19_21_20-11662381342726745999", null, "p", "r");
        Assertions.assertEquals(JobReader.Runner.dataflow, dataflow.runner());
        Assertions.assertEquals("2026-08-29_19_21_20-11662381342726745999", dataflow.id());

        final JobReader.Ref byName = JobReader.resolve("feature-v039-full", null, null, null);
        Assertions.assertEquals(JobReader.Runner.dataflow, byName.runner());

        final JobReader.Ref execution = JobReader.resolve("projects/p/locations/asia-northeast1/jobs/pipeline/executions/pipeline-abc12", null, null, null);
        Assertions.assertEquals(JobReader.Runner.direct, execution.runner());
        Assertions.assertEquals("p", execution.project());
        Assertions.assertEquals("asia-northeast1", execution.region());

        // explicit runner wins; a Cloud Run listing needs no job but does need project / region
        Assertions.assertEquals(JobReader.Runner.dataflow, JobReader.resolve("feature-v039-full", "dataflow", null, null).runner());
        final JobReader.Ref listing = JobReader.resolve(null, "direct", "p", "r");
        Assertions.assertEquals(JobReader.Runner.direct, listing.runner());
        Assertions.assertNull(listing.id());
        Assertions.assertThrows(IllegalArgumentException.class, () -> JobReader.resolve(null, null, "p", "r"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> JobReader.resolve("x", "flink", "p", "r"));
        // a short execution id needs the configured Cloud Run Job
        Assumptions.assumeTrue(System.getenv("MERCARI_PIPELINE_LAUNCH_DIRECT_JOB") == null && System.getenv("MERCARI_PIPELINE_LAUNCH_JOB") == null);
        Assertions.assertThrows(IllegalArgumentException.class, () -> JobReader.resolve("pipeline-abc12", "direct", "p", "r"));
    }

    @Test
    public void testSummarizeAndFormatExecution() {
        final JsonObject execution = JsonParser.parseString("""
                {
                  "name": "projects/p/locations/asia-northeast1/jobs/pipeline/executions/pipeline-abc12",
                  "createTime": "2026-08-30T00:00:00Z",
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
        final JsonObject summary = JobReader.summarizeExecution(execution, "p");
        Assertions.assertEquals("pipeline-abc12", summary.get("id").getAsString());
        Assertions.assertEquals("FAILED", summary.get("state").getAsString());
        Assertions.assertFalse(summary.has("template"));
        Assertions.assertTrue(summary.get("consoleUrl").getAsString().contains("pipeline-abc12"));
        final String text = JobReader.formatExecution(summary);
        Assertions.assertTrue(text.startsWith("- pipeline-abc12: FAILED (created 2026-08-30T00:00:00Z, completed 2026-08-30T00:10:00Z)"), text);
        Assertions.assertTrue(text.contains("failedCount=1"), text);
        Assertions.assertTrue(text.contains("condition Completed: CONDITION_FAILED - Task pipeline-abc12-task0 failed with exit code 1"), text);
        Assertions.assertTrue(text.contains("logs: https://"), text);
        Assertions.assertEquals("RUNNING", JobReader.summarizeExecution(JsonParser.parseString(
                "{\"name\": \"projects/p/locations/r/jobs/j/executions/e\", \"runningCount\": 1}").getAsJsonObject(), "p").get("state").getAsString());
    }

    @Test
    public void testLogFilters() {
        Assertions.assertEquals("resource.type=\"dataflow_step\" AND resource.labels.job_id=\"j1\" AND severity>=WARNING",
                LoggingUtil.createDataflowLogFilter("j1", "warning", null, null));
        Assertions.assertEquals("resource.type=\"cloud_run_job\" AND resource.labels.job_name=\"pipeline\""
                        + " AND labels.\"run.googleapis.com/execution_name\"=\"pipeline-abc12\" AND severity>=ERROR"
                        + " AND timestamp>=\"2026-08-30T00:00:00Z\"",
                LoggingUtil.createCloudRunJobLogFilter("pipeline", "pipeline-abc12", "ERROR", Instant.parse("2026-08-30T00:00:00Z"), null));
        final String grep = LoggingUtil.createDataflowLogFilter("j1", "INFO", null, "feature plan for \"features\"");
        Assertions.assertTrue(grep.contains("textPayload:\"feature plan for \\\"features\\\"\""), grep);
        Assertions.assertTrue(grep.contains("jsonPayload.message:"), grep);
        // DEFAULT means no severity clause
        Assertions.assertFalse(LoggingUtil.createDataflowLogFilter("j1", "DEFAULT", null, null).contains("severity"));
    }

}
