package com.mercari.solution.server.job;

import com.google.dataflow.v1beta3.AutoscalingEvent;
import com.google.dataflow.v1beta3.ExecutionStageState;
import com.google.dataflow.v1beta3.ExecutionStageSummary;
import com.google.dataflow.v1beta3.Job;
import com.google.dataflow.v1beta3.JobMetrics;
import com.google.dataflow.v1beta3.JobState;
import com.google.dataflow.v1beta3.MetricUpdate;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.util.cloud.google.DataflowUtil;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Progress report of a Dataflow job — the questions "why is it slow / not scaling": worker counts and the
 * autoscaler's decisions, the execution stages in completion order with their durations, what the running
 * fused stage is made of and how far it is (element counts of its inputs / outputs), and, when the job runs
 * a {@code feature} transform, the mapping from Dataflow stage names to the feature plan's stages and keys.
 * Pure formatting over already-fetched API objects so it can be unit-tested.
 */
public final class JobProgress {

    private static final Pattern FEATURE_STAGE = Pattern.compile("Stage(\\d+)_(row|context|sequence|population|fit|groupBy)");
    private static final int MAX_EVENTS = 12;
    private static final int MAX_DONE_STAGES = 60;

    private JobProgress() {}

    /**
     * @param planStages the feature plan's {@code stages} array ({@code {index, kind, keys, blocks, columns}}), or null
     */
    public static String report(final Job job, final JobMetrics metrics, final List<AutoscalingEvent> events, final JsonArray planStages) {
        final StringBuilder sb = new StringBuilder();
        final Instant now = Instant.now();
        final Instant created = job.hasCreateTime() ? DataflowUtil.toInstant(job.getCreateTime()) : null;
        sb.append("## Job ").append(job.getName()).append(" (").append(job.getId()).append(")\n");
        sb.append("state: ").append(job.getCurrentState().name().replace("JOB_STATE_", ""));
        if (created != null) sb.append(", created ").append(created).append(", elapsed ").append(human(Duration.between(created, now)));
        sb.append('\n');
        final Map<String, String> options = new LinkedHashMap<>();
        for (final String key : List.of("numWorkers", "maxNumWorkers", "workerMachineType", "autoscalingAlgorithm", "diskSizeGb", "numberOfWorkerHarnessThreads")) {
            final String v = DataflowUtil.getPipelineOption(job, key);
            if (v != null && !v.isBlank() && !"null".equals(v) && !"0".equals(v)) options.put(key, v);
        }
        if (!options.isEmpty()) sb.append("options: ").append(options).append('\n');

        // ---- workers ----
        sb.append("\n## Workers\n");
        if (events == null || events.isEmpty()) {
            sb.append("no autoscaling events (yet)\n");
        } else {
            long current = -1, target = -1;
            for (final AutoscalingEvent e : events) {
                if (e.getCurrentNumWorkers() > 0) current = e.getCurrentNumWorkers();
                if (e.getTargetNumWorkers() > 0) target = e.getTargetNumWorkers();
            }
            sb.append("current ").append(current < 0 ? "?" : current).append(", target ").append(target < 0 ? "?" : target).append('\n');
            final int from = Math.max(0, events.size() - MAX_EVENTS);
            for (final AutoscalingEvent e : events.subList(from, events.size())) {
                sb.append("- ").append(DataflowUtil.toInstant(e.getTime())).append(' ').append(e.getEventType().name())
                        .append(" current=").append(e.getCurrentNumWorkers()).append(" target=").append(e.getTargetNumWorkers());
                if (e.hasDescription() && !e.getDescription().getMessageText().isBlank()) {
                    sb.append(" — ").append(e.getDescription().getMessageText().replace('\n', ' '));
                }
                sb.append('\n');
            }
        }

        // ---- stages ----
        final List<ExecutionStageState> states = job.getStageStatesList();
        final List<ExecutionStageState> done = new ArrayList<>(), running = new ArrayList<>();
        int pending = 0;
        for (final ExecutionStageState s : states) {
            switch (s.getExecutionStageState()) {
                case JOB_STATE_DONE -> done.add(s);
                case JOB_STATE_RUNNING -> running.add(s);
                case JOB_STATE_PENDING, JOB_STATE_QUEUED -> pending++;
                default -> { }
            }
        }
        done.sort((a, b) -> Long.compare(a.getCurrentStateTime().getSeconds(), b.getCurrentStateTime().getSeconds()));
        sb.append("\n## Stages: ").append(done.size()).append(" done, ").append(running.size()).append(" running, ").append(pending).append(" pending\n");
        final Map<String, ExecutionStageSummary> summaries = new LinkedHashMap<>();
        for (final ExecutionStageSummary s : job.getPipelineDescription().getExecutionPipelineStageList()) summaries.put(s.getId(), s);
        final Map<String, String> featureStageOf = featureStages(summaries);

        // completion timeline: collapse the many shuffle / read stages of one feature stage into that stage
        final Map<String, Instant> completedAt = new TreeMap<>(JobProgress::compareStageLabels);
        Instant previous = created;
        for (final ExecutionStageState s : done) {
            final String label = label(s.getExecutionStageName(), featureStageOf);
            completedAt.put(label, DataflowUtil.toInstant(s.getCurrentStateTime()));
        }
        final List<Map.Entry<String, Instant>> timeline = new ArrayList<>(completedAt.entrySet());
        timeline.sort(Map.Entry.comparingByValue());
        int shown = 0;
        for (final Map.Entry<String, Instant> e : timeline) {
            if (shown++ >= MAX_DONE_STAGES) { sb.append("  ... (").append(timeline.size() - MAX_DONE_STAGES).append(" more)\n"); break; }
            sb.append("- ").append(e.getValue()).append(' ').append(e.getKey()).append(" done");
            if (previous != null) sb.append(" (+").append(human(Duration.between(previous, e.getValue()))).append(')');
            sb.append('\n');
            previous = e.getValue();
        }
        for (final ExecutionStageState s : running) {
            final Instant since = DataflowUtil.toInstant(s.getCurrentStateTime());
            sb.append("- RUNNING ").append(label(s.getExecutionStageName(), featureStageOf))
                    .append(" since ").append(since).append(" (").append(human(Duration.between(since, now))).append(")\n");
            final ExecutionStageSummary summary = summaries.get(s.getExecutionStageName());
            if (summary != null) {
                sb.append("  transforms: ");
                final List<String> names = new ArrayList<>();
                for (final ExecutionStageSummary.ComponentTransform t : summary.getComponentTransformList()) names.add(t.getUserName());
                sb.append(String.join(" | ", names)).append('\n');
                appendStageCounts(sb, summary, metrics);
            }
        }

        // ---- feature plan mapping ----
        if (planStages != null && !planStages.isEmpty()) {
            sb.append("\n## Feature plan stages (plan # → kind, keys; Dataflow fused stage if known)\n");
            final Map<String, String> fusedByFeature = new LinkedHashMap<>();
            for (final Map.Entry<String, String> e : featureStageOf.entrySet()) fusedByFeature.merge(e.getValue(), e.getKey(), (a, b) -> a + "," + b);
            for (final JsonElement e : planStages) {
                final JsonObject st = e.getAsJsonObject();
                final String key = "Stage" + st.get("index").getAsInt() + "_" + st.get("kind").getAsString();
                sb.append("- #").append(st.get("index").getAsInt()).append(' ').append(st.get("kind").getAsString());
                if (st.has("keys") && !st.getAsJsonArray("keys").isEmpty()) sb.append(" key=").append(st.getAsJsonArray("keys"));
                else if (!"row".equals(st.get("kind").getAsString()) && !"fit".equals(st.get("kind").getAsString())) sb.append(" key=[] (global: one key, one worker)");
                if (st.has("blocks")) sb.append(" blocks=").append(st.getAsJsonArray("blocks"));
                if (fusedByFeature.containsKey(key)) sb.append(" → ").append(fusedByFeature.get(key));
                sb.append('\n');
            }
        }
        return sb.toString();
    }

    /** ElementCount metrics of the running stage's components: inputs read vs. rows produced (a tail of few big keys shows here). */
    private static void appendStageCounts(final StringBuilder sb, final ExecutionStageSummary summary, final JobMetrics metrics) {
        if (metrics == null) return;
        final List<String> prefixes = new ArrayList<>();
        for (final ExecutionStageSummary.ComponentSource c : summary.getComponentSourceList()) prefixes.add(c.getUserName());
        for (final ExecutionStageSummary.StageSource c : summary.getInputSourceList()) prefixes.add(c.getUserName());
        final Map<String, String> counts = new TreeMap<>();
        for (final MetricUpdate m : metrics.getMetricsList()) {
            if (!"ElementCount".equals(m.getName().getName())) continue;
            final Map<String, String> ctx = m.getName().getContextMap();
            final String output = ctx.getOrDefault("output_user_name", ctx.getOrDefault("original_name", ""));
            for (final String p : prefixes) {
                if (!p.isEmpty() && (output.equals(p) || output.startsWith(p))) {
                    counts.put(output, formatScalar(m));
                    break;
                }
            }
        }
        if (!counts.isEmpty()) {
            sb.append("  element counts:\n");
            for (final Map.Entry<String, String> e : counts.entrySet()) sb.append("    ").append(e.getKey()).append(" = ").append(e.getValue()).append('\n');
        }
    }

    /** Dataflow fused-stage id → feature plan stage token (Stage14_population) when the stage runs feature transforms. */
    static Map<String, String> featureStages(final Map<String, ExecutionStageSummary> summaries) {
        final Map<String, String> result = new LinkedHashMap<>();
        for (final ExecutionStageSummary s : summaries.values()) {
            String best = null;
            for (final ExecutionStageSummary.ComponentTransform t : s.getComponentTransformList()) {
                final Matcher m = FEATURE_STAGE.matcher(t.getUserName());
                if (m.find()) { best = m.group(0); if (!t.getUserName().contains("_Group") && !t.getUserName().contains("_Vc")) break; }
            }
            if (best != null) result.put(s.getId(), best);
        }
        return result;
    }

    static String label(final String executionStageName, final Map<String, String> featureStageOf) {
        final String feature = featureStageOf.get(executionStageName);
        if (feature != null) return feature + " (" + executionStageName + ")";
        final Matcher m = FEATURE_STAGE.matcher(executionStageName);
        return m.find() ? m.group(0) : executionStageName;
    }

    private static int compareStageLabels(final String a, final String b) {
        final Matcher ma = FEATURE_STAGE.matcher(a), mb = FEATURE_STAGE.matcher(b);
        if (ma.find() && mb.find()) return Integer.compare(Integer.parseInt(ma.group(1)), Integer.parseInt(mb.group(1)));
        return a.compareTo(b);
    }

    static String formatScalar(final MetricUpdate m) {
        if (!m.hasScalar()) return "?";
        return switch (m.getScalar().getKindCase()) {
            case NUMBER_VALUE -> {
                final double d = m.getScalar().getNumberValue();
                yield d == Math.rint(d) ? Long.toString((long) d) : Double.toString(d);
            }
            case STRING_VALUE -> m.getScalar().getStringValue();
            default -> m.getScalar().toString().trim();
        };
    }

    static String human(final Duration d) {
        final long s = Math.max(0, d.getSeconds());
        if (s < 60) return s + "s";
        if (s < 3600) return (s / 60) + "m" + (s % 60) + "s";
        return (s / 3600) + "h" + ((s % 3600) / 60) + "m";
    }

    static boolean isActive(final Job job) {
        return job.getCurrentState() == JobState.JOB_STATE_RUNNING;
    }

}
