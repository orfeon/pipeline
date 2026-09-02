package com.mercari.solution.server.job;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mercari.solution.server.launch.LaunchDefaults;
import com.mercari.solution.server.mcp.tool.GetJobLogsTool;
import com.mercari.solution.server.mcp.tool.GetJobProgressTool;
import com.mercari.solution.server.mcp.tool.GetJobTool;
import com.mercari.solution.server.mcp.tool.ListJobErrorsTool;
import com.mercari.solution.server.mcp.tool.Tool;
import com.mercari.solution.util.cloud.google.LoggingUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** The cloud calls need credentials; these cover reference resolution, the execution summary and the log filters. */
public class JobReaderTest {

    private static LaunchDefaults defaults(final String... keyValues) {
        final Map<String, String> env = new HashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) env.put(keyValues[i], keyValues[i + 1]);
        return LaunchDefaults.of(env);
    }

    @Test
    public void testResolveInfersTheRunner() {
        final LaunchDefaults none = defaults();
        final JobReader.Ref dataflow = JobReader.resolve("2026-08-29_19_21_20-11662381342726745999", null, "p", "r", none);
        Assertions.assertEquals(JobReader.Runner.dataflow, dataflow.runner());
        Assertions.assertEquals("2026-08-29_19_21_20-11662381342726745999", dataflow.id());

        final JobReader.Ref byName = JobReader.resolve("feature-v039-full", null, null, null, none);
        Assertions.assertEquals(JobReader.Runner.dataflow, byName.runner());

        final JobReader.Ref execution = JobReader.resolve("projects/p/locations/asia-northeast1/jobs/pipeline/executions/pipeline-abc12", null, null, null, none);
        Assertions.assertEquals(JobReader.Runner.direct, execution.runner());
        Assertions.assertEquals("p", execution.project());
        Assertions.assertEquals("asia-northeast1", execution.region());

        // explicit runner wins; a Cloud Run listing needs no job but does need project / region
        Assertions.assertEquals(JobReader.Runner.dataflow, JobReader.resolve("feature-v039-full", "dataflow", null, null, none).runner());
        final JobReader.Ref listing = JobReader.resolve(null, "direct", "p", "r", none);
        Assertions.assertEquals(JobReader.Runner.direct, listing.runner());
        Assertions.assertNull(listing.id());
        Assertions.assertThrows(IllegalArgumentException.class, () -> JobReader.resolve(null, null, "p", "r", none));
        Assertions.assertThrows(IllegalArgumentException.class, () -> JobReader.resolve("x", "flink", "p", "r", none));
        Assertions.assertThrows(IllegalArgumentException.class, () -> JobReader.resolve(null, "prism", null, null, none), "project / region required");

        // prism is the same Cloud Run shape under its own runner; an execution name is accepted as is
        final JobReader.Ref prism = JobReader.resolve(null, "prism", "p", "r", none);
        Assertions.assertEquals(JobReader.Runner.prism, prism.runner());
        Assertions.assertTrue(prism.runner().isCloudRun());
        Assertions.assertFalse(JobReader.Runner.dataflow.isCloudRun());
        Assertions.assertEquals(List.of(JobReader.Runner.direct, JobReader.Runner.prism), JobReader.CLOUD_RUN_RUNNERS);
        final JobReader.Ref prismExecution = JobReader.resolve("projects/p/locations/r/jobs/pipeline-prism/executions/pipeline-prism-abc12", "prism", null, null, none);
        Assertions.assertEquals(JobReader.Runner.prism, prismExecution.runner());
        Assertions.assertEquals("p", prismExecution.project());
    }

    @Test
    public void testShortExecutionIdResolvesAgainstTheRunnersJob() {
        // a short id needs the job configured for that runner: the direct job (or a common JOB) is never the prism one
        final LaunchDefaults directOnly = defaults("MERCARI_PIPELINE_LAUNCH_DIRECT_JOB", "mp-job", "MERCARI_PIPELINE_LAUNCH_JOB", "mp-job");
        final JobReader.Ref direct = JobReader.resolve("mp-job-abc12", "direct", "p", "r", directOnly);
        Assertions.assertEquals("projects/p/locations/r/jobs/mp-job/executions/mp-job-abc12", direct.id());
        final IllegalArgumentException prismShort = Assertions.assertThrows(IllegalArgumentException.class,
                () -> JobReader.resolve("mp-job-prism-abc12", "prism", "p", "r", directOnly));
        Assertions.assertTrue(prismShort.getMessage().contains("MERCARI_PIPELINE_LAUNCH_PRISM_JOB"), prismShort.getMessage());
        Assertions.assertThrows(IllegalArgumentException.class, () -> JobReader.resolve("mp-job-abc12", "direct", "p", "r", defaults()));

        final LaunchDefaults both = defaults("MERCARI_PIPELINE_LAUNCH_DIRECT_JOB", "mp-job", "MERCARI_PIPELINE_LAUNCH_PRISM_JOB", "mp-job-prism");
        Assertions.assertEquals("projects/p/locations/r/jobs/mp-job-prism/executions/mp-job-prism-abc12",
                JobReader.resolve("mp-job-prism-abc12", "prism", "p", "r", both).id());
    }

    @Test
    public void testCloudRunAliasesMeanTheConfiguredCloudRunRunner() {
        // 'cloudRunJob' / 'cloud-run' / 'run' are environment-style names: the runner is whichever Cloud Run job is configured
        Assertions.assertEquals(JobReader.Runner.direct, JobReader.parseRunner("cloudRunJob", defaults()));
        Assertions.assertEquals(JobReader.Runner.direct, JobReader.parseRunner("cloud-run", defaults("MERCARI_PIPELINE_LAUNCH_DIRECT_JOB", "mp-job")));
        Assertions.assertEquals(JobReader.Runner.direct, JobReader.parseRunner("run",
                defaults("MERCARI_PIPELINE_LAUNCH_DIRECT_JOB", "mp-job", "MERCARI_PIPELINE_LAUNCH_PRISM_JOB", "mp-job-prism")));
        Assertions.assertEquals(JobReader.Runner.prism, JobReader.parseRunner("cloudrun", defaults("MERCARI_PIPELINE_LAUNCH_PRISM_JOB", "mp-job-prism")));
        Assertions.assertEquals(JobReader.Runner.prism, JobReader.parseRunner("PRISM", defaults()));
        Assertions.assertNull(JobReader.parseRunner(" ", defaults()));
        final IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class, () -> JobReader.parseRunner("flink", defaults()));
        Assertions.assertTrue(e.getMessage().contains("dataflow | direct | prism"), e.getMessage());
    }

    @Test
    public void testConfiguredJobsAreDistinctResources() {
        // the same name in two regions is two jobs; the same resource configured for both runners is one (first runner wins)
        final StringBuilder notes = new StringBuilder();
        final List<JobReader.ConfiguredJob> twoRegions = JobReader.configuredJobs(defaults(
                "MERCARI_PIPELINE_LAUNCH_PROJECT", "p", "MERCARI_PIPELINE_LAUNCH_REGION", "asia-northeast1",
                "MERCARI_PIPELINE_LAUNCH_DIRECT_JOB", "mp-job",
                "MERCARI_PIPELINE_LAUNCH_PRISM_JOB", "mp-job", "MERCARI_PIPELINE_LAUNCH_PRISM_REGION", "us-central1"), null, null, notes);
        Assertions.assertEquals(2, twoRegions.size());
        Assertions.assertEquals("projects/p/locations/asia-northeast1/jobs/mp-job", twoRegions.get(0).resourceName());
        Assertions.assertEquals(JobReader.Runner.prism, twoRegions.get(1).runner());
        Assertions.assertEquals("projects/p/locations/us-central1/jobs/mp-job", twoRegions.get(1).resourceName());
        Assertions.assertTrue(notes.isEmpty());

        final List<JobReader.ConfiguredJob> shared = JobReader.configuredJobs(defaults(
                "MERCARI_PIPELINE_LAUNCH_PROJECT", "p", "MERCARI_PIPELINE_LAUNCH_REGION", "asia-northeast1",
                "MERCARI_PIPELINE_LAUNCH_DIRECT_JOB", "mp-job", "MERCARI_PIPELINE_LAUNCH_PRISM_JOB", "mp-job"), null, null, notes);
        Assertions.assertEquals(1, shared.size());
        Assertions.assertEquals(JobReader.Runner.direct, shared.get(0).runner());

        // a job whose project / region cannot be resolved is reported, not dropped
        final List<JobReader.ConfiguredJob> unresolved = JobReader.configuredJobs(defaults("MERCARI_PIPELINE_LAUNCH_PRISM_JOB", "mp-job-prism"), null, null, notes);
        Assertions.assertTrue(unresolved.isEmpty());
        Assertions.assertTrue(notes.toString().contains("mp-job-prism configured for prism not listed"), notes.toString());
        Assertions.assertTrue(JobReader.configuredJobs(defaults(), "p", "r", new StringBuilder()).isEmpty());
    }

    /** The tools' runner enums are hand-written schema text: they must spell exactly the runners JobReader accepts. */
    @Test
    public void testToolSchemasListEveryRunner() {
        final List<String> runners = java.util.Arrays.stream(JobReader.Runner.values()).map(Enum::name).toList();
        for (final Class<?> tool : List.of(GetJobTool.class, GetJobLogsTool.class, GetJobProgressTool.class, ListJobErrorsTool.class)) {
            final Tool.Module module = tool.getAnnotation(Tool.Module.class);
            final JsonObject schema = JsonParser.parseString(module.inputSchema()).getAsJsonObject();
            final List<String> declared = new java.util.ArrayList<>();
            schema.getAsJsonObject("properties").getAsJsonObject("runner").getAsJsonArray("enum").forEach(e -> declared.add(e.getAsString()));
            Assertions.assertEquals(runners, declared, module.name());
        }
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
    public void testLogsRejectUnknownSeverity() {
        final String result = JobReader.getJobLogs("projects/p/locations/r/jobs/j/executions/e", null, null, null, "WARN", 10, null);
        Assertions.assertTrue(result.startsWith("ERROR: unknown minSeverity 'WARN'"), result);
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
