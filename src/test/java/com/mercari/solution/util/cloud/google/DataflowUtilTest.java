package com.mercari.solution.util.cloud.google;

import com.google.dataflow.v1beta3.Environment;
import com.google.dataflow.v1beta3.Job;
import com.google.dataflow.v1beta3.JobMessage;
import com.google.dataflow.v1beta3.JobMessageImportance;
import com.google.dataflow.v1beta3.JobState;
import com.google.dataflow.v1beta3.JobType;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

public class DataflowUtilTest {

    private static Job createJob() {
        return Job.newBuilder()
                .setId("2026-07-17_22_25_11-1234567890123456789")
                .setName("myjob")
                .setProjectId("myproject")
                .setLocation("asia-northeast1")
                .setType(JobType.JOB_TYPE_BATCH)
                .setCurrentState(JobState.JOB_STATE_FAILED)
                .setCreateTime(Timestamp.newBuilder().setSeconds(1752787511).build())
                .putLabels("mercari-pipeline-version", "abc1234")
                .setEnvironment(Environment.newBuilder()
                        .setSdkPipelineOptions(Struct.newBuilder()
                                .putFields("options", Value.newBuilder()
                                        .setStructValue(Struct.newBuilder()
                                                .putFields("config", Value.newBuilder()
                                                        .setStringValue("sources: []")
                                                        .build())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
    }

    @Test
    public void testFormatJob() {
        final String result = DataflowUtil.formatJob(createJob());
        Assertions.assertTrue(result.contains("## Dataflow job 2026-07-17_22_25_11-1234567890123456789"), result);
        Assertions.assertTrue(result.contains("- name: myjob"), result);
        Assertions.assertTrue(result.contains("- state: JOB_STATE_FAILED"), result);
        Assertions.assertTrue(result.contains("mercari-pipeline-version: abc1234"), result);
    }

    @Test
    public void testFormatJobs() {
        final String result = DataflowUtil.formatJobs(List.of(createJob()));
        Assertions.assertTrue(result.contains("2026-07-17_22_25_11-1234567890123456789 | myjob | JOB_STATE_FAILED"), result);
    }

    @Test
    public void testGetPipelineOption() {
        final Job job = createJob();
        Assertions.assertEquals("sources: []", DataflowUtil.getPipelineOption(job, "config"));
        Assertions.assertNull(DataflowUtil.getPipelineOption(job, "nosuchoption"));
        Assertions.assertNull(DataflowUtil.getPipelineOption(Job.newBuilder().build(), "config"));
    }

    @Test
    public void testFormatJobMessagesCollapsesDuplicates() {
        final JobMessage error = JobMessage.newBuilder()
                .setMessageText("Workflow failed. Causes: something broke")
                .setMessageImportance(JobMessageImportance.JOB_MESSAGE_ERROR)
                .setTime(Timestamp.newBuilder().setSeconds(1752787600).build())
                .build();
        final String result = DataflowUtil.formatJobMessages(List.of(error, error, error));
        Assertions.assertTrue(result.contains("[JOB_MESSAGE_ERROR] (x3)"), result);
        Assertions.assertTrue(result.contains("Workflow failed. Causes: something broke"), result);
        // collapsed to a single entry
        Assertions.assertEquals(1, result.split("###", -1).length - 1, result);
    }

    @Test
    public void testToInstant() {
        final Instant instant = DataflowUtil.toInstant(
                Timestamp.newBuilder().setSeconds(1752787511).setNanos(500_000_000).build());
        Assertions.assertEquals(Instant.ofEpochSecond(1752787511, 500_000_000), instant);
    }

}
