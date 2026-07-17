package com.mercari.solution.util.cloud.google;

import com.google.dataflow.v1beta3.*;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DataflowUtil {

    private static final int MAX_MESSAGE_TEXT_LENGTH = 3000;

    public static LaunchFlexTemplateResponse launchFlexTemplate(
            final String project,
            final String region,
            final LaunchFlexTemplateParameter parameter,
            final Boolean validateOnly) throws IOException {

        final LaunchFlexTemplateRequest request = LaunchFlexTemplateRequest
                .newBuilder()
                .setProjectId(project)
                .setLocation(region)
                .setLaunchParameter(parameter)
                .setValidateOnly(validateOnly)
                .build();

        try(final FlexTemplatesServiceClient client = FlexTemplatesServiceClient.create()) {
            final LaunchFlexTemplateResponse response = client.launchFlexTemplate(request);
            return response;
        }
    }

    public static Job update(
            final String project,
            final String region,
            final String jobId,
            final JobState requestedState) throws IOException {

        final UpdateJobRequest request = UpdateJobRequest.newBuilder()
                .setProjectId(project)
                .setLocation(region)
                .setJobId(jobId)
                .setJob(Job.newBuilder()
                        .setRequestedState(requestedState)
                        .build())
                .build();

        //JOB_STATE_CANCELLED
        try(final JobsV1Beta3Client client = JobsV1Beta3Client.create()) {
            return client.updateJob(request);
        }
    }

    public static Job cancel(
            final String project,
            final String region,
            final String jobId) throws IOException {

        return update(project, region, jobId, JobState.JOB_STATE_CANCELLED);
    }

    public static Job getJob(
            final String project,
            final String region,
            final String jobId,
            final JobView view) throws IOException {

        final GetJobRequest request = GetJobRequest.newBuilder()
                .setProjectId(project)
                .setLocation(region)
                .setJobId(jobId)
                .setView(view == null ? JobView.JOB_VIEW_ALL : view)
                .build();

        try(final JobsV1Beta3Client client = JobsV1Beta3Client.create()) {
            return client.getJob(request);
        }
    }

    public static List<Job> listJobs(
            final String project,
            final String region,
            final ListJobsRequest.Filter filter,
            final int limit) throws IOException {

        final ListJobsRequest request = ListJobsRequest.newBuilder()
                .setProjectId(project)
                .setLocation(region)
                .setFilter(filter == null ? ListJobsRequest.Filter.ALL : filter)
                .setPageSize(Math.min(limit, 100))
                .build();

        final List<Job> jobs = new ArrayList<>();
        try(final JobsV1Beta3Client client = JobsV1Beta3Client.create()) {
            for(final Job job : client.listJobs(request).iterateAll()) {
                jobs.add(job);
                if(jobs.size() >= limit) {
                    break;
                }
            }
        }
        return jobs;
    }

    public static Job findJobByName(
            final String project,
            final String region,
            final String jobName,
            final int searchLimit) throws IOException {

        Job latest = null;
        for(final Job job : listJobs(project, region, ListJobsRequest.Filter.ALL, searchLimit)) {
            if(!job.getName().equals(jobName)) {
                continue;
            }
            if(latest == null || job.getCreateTime().getSeconds() > latest.getCreateTime().getSeconds()) {
                latest = job;
            }
        }
        return latest;
    }

    public static List<JobMessage> listJobMessages(
            final String project,
            final String region,
            final String jobId,
            final JobMessageImportance minimumImportance,
            final int limit) throws IOException {

        final ListJobMessagesRequest request = ListJobMessagesRequest.newBuilder()
                .setProjectId(project)
                .setLocation(region)
                .setJobId(jobId)
                .setMinimumImportance(minimumImportance == null ? JobMessageImportance.JOB_MESSAGE_ERROR : minimumImportance)
                .setPageSize(Math.min(limit, 100))
                .build();

        final List<JobMessage> messages = new ArrayList<>();
        try(final MessagesV1Beta3Client client = MessagesV1Beta3Client.create()) {
            for(final JobMessage message : client.listJobMessages(request).iterateAll()) {
                messages.add(message);
                if(messages.size() >= limit) {
                    break;
                }
            }
        }
        return messages;
    }

    /**
     * Extract a pipeline option value (e.g. the flex-template 'config' parameter) from a job
     * fetched with JOB_VIEW_ALL. The Java SDK records options in environment.sdkPipelineOptions
     * under the 'options' struct.
     */
    public static String getPipelineOption(final Job job, final String name) {
        if(!job.hasEnvironment() || !job.getEnvironment().hasSdkPipelineOptions()) {
            return null;
        }
        final Struct sdkPipelineOptions = job.getEnvironment().getSdkPipelineOptions();
        Struct options = sdkPipelineOptions;
        final Value optionsValue = sdkPipelineOptions.getFieldsMap().get("options");
        if(optionsValue != null && optionsValue.hasStructValue()) {
            options = optionsValue.getStructValue();
        }
        final Value value = options.getFieldsMap().get(name);
        if(value == null) {
            return null;
        }
        return switch (value.getKindCase()) {
            case STRING_VALUE -> value.getStringValue();
            case NUMBER_VALUE -> String.valueOf(value.getNumberValue());
            case BOOL_VALUE -> String.valueOf(value.getBoolValue());
            default -> null;
        };
    }

    public static String formatJob(final Job job) {
        final StringBuilder result = new StringBuilder();
        result.append("## Dataflow job ").append(job.getId()).append("\n");
        result.append("- name: ").append(job.getName()).append("\n");
        result.append("- project: ").append(job.getProjectId())
                .append(", location: ").append(job.getLocation()).append("\n");
        result.append("- type: ").append(job.getType().name()).append("\n");
        result.append("- state: ").append(job.getCurrentState().name());
        if(job.hasCurrentStateTime()) {
            result.append(" (at ").append(toInstant(job.getCurrentStateTime())).append(")");
        }
        result.append("\n");
        if(job.hasCreateTime()) {
            result.append("- createTime: ").append(toInstant(job.getCreateTime())).append("\n");
        }
        if(job.hasJobMetadata() && job.getJobMetadata().hasSdkVersion()) {
            result.append("- sdkVersion: ").append(job.getJobMetadata().getSdkVersion().getVersion()).append("\n");
        }
        if(!job.getLabelsMap().isEmpty()) {
            result.append("- labels:\n");
            for(final Map.Entry<String, String> label : job.getLabelsMap().entrySet()) {
                result.append("    ").append(label.getKey()).append(": ").append(label.getValue()).append("\n");
            }
        }
        return result.toString();
    }

    public static String formatJobs(final List<Job> jobs) {
        final StringBuilder result = new StringBuilder();
        for(final Job job : jobs) {
            result.append("- ").append(job.getId())
                    .append(" | ").append(job.getName())
                    .append(" | ").append(job.getCurrentState().name())
                    .append(" | created ").append(job.hasCreateTime() ? toInstant(job.getCreateTime()) : "-")
                    .append("\n");
        }
        return result.toString();
    }

    /** Format job messages, collapsing repeated identical texts into a single entry with a count. */
    public static String formatJobMessages(final List<JobMessage> messages) {
        final Map<String, int[]> counts = new LinkedHashMap<>();
        final Map<String, JobMessage> firsts = new LinkedHashMap<>();
        for(final JobMessage message : messages) {
            final String text = message.getMessageText();
            counts.computeIfAbsent(text, k -> new int[1])[0]++;
            firsts.putIfAbsent(text, message);
        }
        final StringBuilder result = new StringBuilder();
        for(final Map.Entry<String, JobMessage> entry : firsts.entrySet()) {
            final JobMessage first = entry.getValue();
            final int count = counts.get(entry.getKey())[0];
            result.append("### [").append(first.getMessageImportance().name()).append("]");
            if(count > 1) {
                result.append(" (x").append(count).append(")");
            }
            if(first.hasTime()) {
                result.append(" at ").append(toInstant(first.getTime()));
            }
            result.append("\n");
            final String text = entry.getKey();
            if(text.length() > MAX_MESSAGE_TEXT_LENGTH) {
                result.append(text, 0, MAX_MESSAGE_TEXT_LENGTH).append("... (truncated)\n");
            } else {
                result.append(text).append("\n");
            }
        }
        return result.toString();
    }

    public static Instant toInstant(final Timestamp timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }

    public static LaunchTemplateResponse launchTemplate(
            final String project,
            final String region,
            final String gcsPath,
            final LaunchTemplateParameters parameter,
            final Boolean validateOnly) throws IOException {

        final LaunchTemplateRequest request = LaunchTemplateRequest
                .newBuilder()
                .setProjectId(project)
                .setLocation(region)
                .setGcsPath(gcsPath)
                .setLaunchParameters(parameter)
                .setValidateOnly(validateOnly)
                .build();

        try(final TemplatesServiceClient client = TemplatesServiceClient.create()) {
            return client.launchTemplate(request);
        }
    }

}
