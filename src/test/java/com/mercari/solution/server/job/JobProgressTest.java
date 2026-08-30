package com.mercari.solution.server.job;

import com.google.dataflow.v1beta3.AutoscalingEvent;
import com.google.dataflow.v1beta3.ExecutionStageState;
import com.google.dataflow.v1beta3.ExecutionStageSummary;
import com.google.dataflow.v1beta3.Job;
import com.google.dataflow.v1beta3.JobMetrics;
import com.google.dataflow.v1beta3.JobState;
import com.google.dataflow.v1beta3.MetricStructuredName;
import com.google.dataflow.v1beta3.MetricUpdate;
import com.google.dataflow.v1beta3.PipelineDescription;
import com.google.dataflow.v1beta3.StructuredMessage;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

/** The progress report over synthetic API objects shaped like the job investigated on 2026-08-30. */
public class JobProgressTest {

    private static Timestamp ts(final String iso) {
        final Instant i = Instant.parse(iso);
        return Timestamp.newBuilder().setSeconds(i.getEpochSecond()).setNanos(i.getNano()).build();
    }

    private static ExecutionStageState state(final String name, final JobState s, final String at) {
        return ExecutionStageState.newBuilder().setExecutionStageName(name).setExecutionStageState(s).setCurrentStateTime(ts(at)).build();
    }

    private static MetricUpdate count(final String outputUserName, final double value) {
        return MetricUpdate.newBuilder()
                .setName(MetricStructuredName.newBuilder().setName("ElementCount").putContext("output_user_name", outputUserName))
                .setScalar(Value.newBuilder().setNumberValue(value))
                .build();
    }

    @Test
    public void testReport() {
        final Job job = Job.newBuilder()
                .setId("2026-08-29_19_21_20-11662381342726745999")
                .setName("feature-v039-full")
                .setCurrentState(JobState.JOB_STATE_RUNNING)
                .setCreateTime(ts("2026-08-30T02:21:21Z"))
                .addStageStates(state("F540", JobState.JOB_STATE_DONE, "2026-08-30T02:26:20Z"))
                .addStageStates(state("features-Stage1_sequence_Group-shuffle-session1", JobState.JOB_STATE_DONE, "2026-08-30T02:26:52Z"))
                .addStageStates(state("F541", JobState.JOB_STATE_DONE, "2026-08-30T02:26:52Z"))
                .addStageStates(state("F563", JobState.JOB_STATE_RUNNING, "2026-08-30T02:42:59Z"))
                .addStageStates(state("features-Stage15_population_Group-open-shuffle2", JobState.JOB_STATE_PENDING, "2026-08-30T02:42:59Z"))
                .setPipelineDescription(PipelineDescription.newBuilder()
                        .addExecutionPipelineStage(ExecutionStageSummary.newBuilder().setId("F540")
                                .addComponentTransform(ExecutionStageSummary.ComponentTransform.newBuilder().setUserName("features/Stage0_context")))
                        .addExecutionPipelineStage(ExecutionStageSummary.newBuilder().setId("F541")
                                .addComponentTransform(ExecutionStageSummary.ComponentTransform.newBuilder().setUserName("features/Stage1_sequence_Group/Read"))
                                .addComponentTransform(ExecutionStageSummary.ComponentTransform.newBuilder().setUserName("features/Stage1_sequence")))
                        .addExecutionPipelineStage(ExecutionStageSummary.newBuilder().setId("F563")
                                .addComponentTransform(ExecutionStageSummary.ComponentTransform.newBuilder().setUserName("features/Stage14_population_Group/Read"))
                                .addComponentTransform(ExecutionStageSummary.ComponentTransform.newBuilder().setUserName("features/Stage14_population"))
                                .addComponentSource(ExecutionStageSummary.ComponentSource.newBuilder().setUserName("features/Stage14_population_Group/Read-out0"))
                                .addComponentSource(ExecutionStageSummary.ComponentSource.newBuilder().setUserName("features/Stage14_population-out1"))))
                .build();
        final JobMetrics metrics = JobMetrics.newBuilder()
                .addMetrics(count("features/Stage14_population_Group/Read-out0", 2135))
                .addMetrics(count("features/Stage14_population-out1", 896563))
                .addMetrics(count("features/Stage1_sequence-out1", 916403))
                .build();
        final List<AutoscalingEvent> events = List.of(
                AutoscalingEvent.newBuilder().setTime(ts("2026-08-30T02:24:32Z")).setEventType(AutoscalingEvent.AutoscalingEventType.CURRENT_NUM_WORKERS_CHANGED).setCurrentNumWorkers(2).build(),
                AutoscalingEvent.newBuilder().setTime(ts("2026-08-30T02:27:15Z")).setEventType(AutoscalingEvent.AutoscalingEventType.TARGET_NUM_WORKERS_CHANGED).setCurrentNumWorkers(2).setTargetNumWorkers(1)
                        .setDescription(StructuredMessage.newBuilder().setMessageText("Autoscaling: Reduced the number of workers to 1 based on the rate of progress in the currently running stage(s).")).build());
        final JsonArray planStages = JsonParser.parseString("""
                [{"index":0,"kind":"context","keys":["raceKey"],"blocks":["win"],"columns":3},
                 {"index":1,"kind":"sequence","keys":["kettoNum"],"blocks":["horseHist"],"columns":5},
                 {"index":14,"kind":"population","keys":["bf"],"blocks":["encVC"],"columns":9},
                 {"index":12,"kind":"population","keys":[],"blocks":["encVC"],"columns":2}]
                """).getAsJsonArray();

        final String report = JobProgress.report(job, metrics, events, planStages);
        Assertions.assertTrue(report.contains("## Job feature-v039-full (2026-08-29_19_21_20-11662381342726745999)"), report);
        Assertions.assertTrue(report.contains("state: RUNNING"), report);
        Assertions.assertTrue(report.contains("current 2, target 1"), report);
        Assertions.assertTrue(report.contains("Reduced the number of workers to 1"), report);
        Assertions.assertTrue(report.contains("## Stages: 3 done, 1 running, 1 pending"), report);
        // stage timeline labelled by feature stage, with the time since the previous stage
        Assertions.assertTrue(report.contains("Stage0_context (F540) done (+4m59s)"), report);
        Assertions.assertTrue(report.contains("Stage1_sequence") && report.contains("done (+32s)"), report);
        Assertions.assertTrue(report.contains("RUNNING Stage14_population (F563) since 2026-08-30T02:42:59Z"), report);
        Assertions.assertTrue(report.contains("transforms: features/Stage14_population_Group/Read | features/Stage14_population"), report);
        // element counts of the running stage only
        Assertions.assertTrue(report.contains("features/Stage14_population_Group/Read-out0 = 2135"), report);
        Assertions.assertTrue(report.contains("features/Stage14_population-out1 = 896563"), report);
        Assertions.assertFalse(report.contains("Stage1_sequence-out1"), report);
        // plan mapping: keys, the global level called out, fused stage ids attached
        Assertions.assertTrue(report.contains("#14 population key=[\"bf\"] blocks=[\"encVC\"] → F563"), report);
        Assertions.assertTrue(report.contains("#12 population key=[] (global: one key, one worker)"), report);
        Assertions.assertTrue(report.contains("#1 sequence key=[\"kettoNum\"] blocks=[\"horseHist\"] → F541"), report);
    }

    @Test
    public void testHelpers() {
        Assertions.assertEquals("45s", JobProgress.human(java.time.Duration.ofSeconds(45)));
        Assertions.assertEquals("4m59s", JobProgress.human(java.time.Duration.ofSeconds(299)));
        Assertions.assertEquals("1h5m", JobProgress.human(java.time.Duration.ofMinutes(65)));
        Assertions.assertEquals("2135", JobProgress.formatScalar(count("x", 2135)));
        Assertions.assertEquals("0.95", JobProgress.formatScalar(count("x", 0.95)));
    }

}
