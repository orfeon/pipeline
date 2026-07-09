package com.mercari.solution.util.pipeline;

import com.google.cloud.ServiceOptions;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.mercari.solution.MPipeline;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.PValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class OptionUtil {

    private static final Logger LOG = LoggerFactory.getLogger(OptionUtil.class);

    public static MPipeline.Runner getRunner(final PipelineOptions options) {
        return switch (options.getRunner().getSimpleName()) {
            case "DirectRunner" -> MPipeline.Runner.direct;
            case "PrismRunner" -> MPipeline.Runner.prism;
            case "PortableRunner" -> MPipeline.Runner.portable;
            case "DataflowRunner" -> MPipeline.Runner.dataflow;
            case "SparkRunner", "SparkStructuredStreamingRunner" -> MPipeline.Runner.spark;
            case "FlinkRunner" -> MPipeline.Runner.flink;
            default -> throw new IllegalArgumentException("Not supported runner: " + options.getRunner().getSimpleName());
        };
    }

    public static boolean isStreaming(final PipelineOptions options) {
        return options.as(StreamingOptions.class).isStreaming();
    }

    public static boolean isStreaming(final PInput input) {
        return isStreaming(input.getPipeline().getOptions());
    }

    public static Map<String, Object> getTemplateArgs(final String[] args) {
        final Map<String, Map<String, String>> argsParameters = filterConfigArgs(args);
        final Map<String, Object> map = new HashMap<>();
        for(final Map.Entry<String, String> entry : argsParameters.getOrDefault("template", new HashMap<>()).entrySet()) {
            JsonElement jsonElement;
            try {
                jsonElement = new Gson().fromJson(entry.getValue(), JsonElement.class);
            } catch (final JsonParseException e) {
                // covers JsonSyntaxException and JsonIOException (e.g. values with
                // trailing tokens such as "hello world"): fall back to the string value
                jsonElement = new JsonPrimitive(entry.getValue());
            }
            map.put(entry.getKey(), extractTemplateParameters(jsonElement));
        }
        return map;
    }

    public static boolean isUnbounded(final PInput input) {
        if (input instanceof PCollection<?>) {
            PCollection<?> pCol = (PCollection<?>) input;
            return pCol.isBounded() == PCollection.IsBounded.UNBOUNDED;
        }
        return false;
    }

    public static boolean isUnbounded(final Pipeline pipeline) {
        final UnboundedCheckVisitor visitor = new UnboundedCheckVisitor();
        pipeline.traverseTopologically(visitor);
        return visitor.isUnbounded();
    }

    public static String[] filterPipelineArgs(final String[] args) {
        final List<String> filteredArgs = Arrays.stream(args)
                .filter(s -> !s.contains("=") || !s.split("=")[0].contains("."))
                .toList();
        return filteredArgs.toArray(new String[filteredArgs.size()]);
    }

    public static Map<String, Map<String, String>> filterConfigArgs(final String[] args) {
        return Arrays.stream(args)
                .filter(s -> s.contains("=") && s.split("=")[0].contains("."))
                .map(s -> s.startsWith("--") ? s.replaceFirst("--", "") : s)
                .collect(Collectors.groupingBy(
                        s -> s.substring(0, s.indexOf(".")),
                        Collectors.toMap(
                                s -> s.substring(s.indexOf(".") + 1).split("=")[0],
                                s -> s.substring(s.indexOf(".") + 1).split("=", 2)[1],
                                (s1, s2) -> s2)));
    }

    public static String getDefaultProject() {
        final String project = ServiceOptions.getDefaultProjectId();
        if(project == null) {
            throw new IllegalArgumentException(
                    "Could not resolve a default GCP project (not running on Google Cloud?). "
                            + "Set the module's projectId parameter or the GOOGLE_CLOUD_PROJECT environment variable.");
        }
        return project;
    }

    public static boolean isDirectRunner(final PInput input) {
        return PipelineOptions.DirectRunner.class.getSimpleName().equals(input.getPipeline().getOptions().getRunner().getSimpleName());
    }

    public static Set<String> toSet(final List<String> params) {
        return Optional.ofNullable(params)
                .map(HashSet::new)
                .orElse(new HashSet<>());
    }

    private static Object extractTemplateParameters(final JsonElement jsonElement) {
        if(jsonElement.isJsonPrimitive()) {
            if(jsonElement.getAsJsonPrimitive().isBoolean()) {
                return jsonElement.getAsBoolean();
            } else if(jsonElement.getAsJsonPrimitive().isString()) {
                return jsonElement.getAsString();
            } else if(jsonElement.getAsJsonPrimitive().isNumber()) {
                return jsonElement.getAsLong();
            }
            return jsonElement.toString();
        }
        if(jsonElement.isJsonObject()) {
            final Map<String, Object> map = new HashMap<>();
            jsonElement.getAsJsonObject().entrySet().forEach(kv -> map.put(kv.getKey(), extractTemplateParameters(kv.getValue())));
            return map;
        }
        if(jsonElement.isJsonArray()) {
            final List<Object> list = new ArrayList<>();
            jsonElement.getAsJsonArray().forEach(element -> list.add(extractTemplateParameters(element)));
            return list;
        }
        return null;
    }

    private static class UnboundedCheckVisitor extends Pipeline.PipelineVisitor.Defaults {

        private boolean hasUnboundedPCollection = false;

        @Override
        public void visitValue(PValue value, TransformHierarchy.Node producer) {
            if (value instanceof PCollection<?>) {
                PCollection<?> pCol = (PCollection<?>) value;
                if (pCol.isBounded() == PCollection.IsBounded.UNBOUNDED) {
                    hasUnboundedPCollection = true;
                }
            }
        }

        public boolean isUnbounded() {
            return hasUnboundedPCollection;
        }
    }

}
