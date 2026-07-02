package com.mercari.solution.util.pipeline;

import com.mercari.solution.MPipeline;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class OptionUtilTest {

    @Test
    public void testGetRunner() {
        final PipelineOptions direct = PipelineOptionsFactory.create();
        direct.setRunner(DirectRunner.class);
        Assertions.assertEquals(MPipeline.Runner.direct, OptionUtil.getRunner(direct));

        final PipelineOptions dataflow = PipelineOptionsFactory.create();
        dataflow.setRunner(DataflowRunner.class);
        Assertions.assertEquals(MPipeline.Runner.dataflow, OptionUtil.getRunner(dataflow));
    }

    @Test
    public void testIsStreaming() {
        final PipelineOptions options = PipelineOptionsFactory.create();
        Assertions.assertFalse(OptionUtil.isStreaming(options));

        options.as(StreamingOptions.class).setStreaming(true);
        Assertions.assertTrue(OptionUtil.isStreaming(options));

        final Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.create());
        final PCollection<String> pc = pipeline.apply(Create.of("a"));
        Assertions.assertFalse(OptionUtil.isStreaming(pc));
    }

    @Test
    public void testIsUnbounded() {
        final Pipeline bounded = Pipeline.create(PipelineOptionsFactory.create());
        final PCollection<String> boundedPc = bounded.apply(Create.of("a"));
        Assertions.assertFalse(OptionUtil.isUnbounded(boundedPc));
        Assertions.assertFalse(OptionUtil.isUnbounded(bounded));
        // non PCollection input
        Assertions.assertFalse(OptionUtil.isUnbounded(bounded.begin()));

        final Pipeline unbounded = Pipeline.create(PipelineOptionsFactory.create());
        final PCollection<Long> unboundedPc = unbounded.apply(GenerateSequence.from(0));
        Assertions.assertTrue(OptionUtil.isUnbounded(unboundedPc));
        Assertions.assertTrue(OptionUtil.isUnbounded(unbounded));
    }

    @Test
    public void testFilterPipelineArgs() {
        final String[] args = {
                "--runner=DirectRunner",
                "--streaming",
                "--template.myVar=hello",
                "--config.path=gs://bucket/config.yaml"
        };
        final String[] filtered = OptionUtil.filterPipelineArgs(args);
        Assertions.assertArrayEquals(new String[]{"--runner=DirectRunner", "--streaming"}, filtered);

        Assertions.assertArrayEquals(new String[]{}, OptionUtil.filterPipelineArgs(new String[]{}));
    }

    @Test
    public void testFilterConfigArgs() {
        final String[] args = {
                "--runner=DirectRunner",
                "--template.myVar=hello",
                "--template.other=va=lue",
                "--config.path=gs://bucket/config.yaml",
                "template.noPrefix=raw"
        };
        final Map<String, Map<String, String>> configArgs = OptionUtil.filterConfigArgs(args);
        Assertions.assertEquals(Set.of("template", "config"), configArgs.keySet());
        Assertions.assertEquals("hello", configArgs.get("template").get("myVar"));
        // value may itself contain '='
        Assertions.assertEquals("va=lue", configArgs.get("template").get("other"));
        // args without leading "--" are also accepted; duplicate keys keep the last value
        Assertions.assertEquals("raw", configArgs.get("template").get("noPrefix"));
        Assertions.assertEquals("gs://bucket/config.yaml", configArgs.get("config").get("path"));
    }

    @Test
    public void testGetTemplateArgs() {
        final String[] args = {
                "--template.stringVar=hello",
                "--template.longVar=10",
                "--template.boolVar=true",
                "--template.objVar={\"a\": 1, \"b\": [true, \"x\"]}",
                "--runner=DirectRunner"
        };
        final Map<String, Object> templateArgs = OptionUtil.getTemplateArgs(args);
        Assertions.assertEquals("hello", templateArgs.get("stringVar"));
        Assertions.assertEquals(10L, templateArgs.get("longVar"));
        Assertions.assertEquals(true, templateArgs.get("boolVar"));

        @SuppressWarnings("unchecked") final Map<String, Object> obj = (Map<String, Object>) templateArgs.get("objVar");
        Assertions.assertEquals(1L, obj.get("a"));
        Assertions.assertEquals(List.of(true, "x"), obj.get("b"));

        Assertions.assertFalse(templateArgs.containsKey("runner"));
        Assertions.assertTrue(OptionUtil.getTemplateArgs(new String[]{}).isEmpty());
    }

    @Test
    public void testToSet() {
        Assertions.assertTrue(OptionUtil.toSet(null).isEmpty());
        Assertions.assertEquals(Set.of("a", "b"), OptionUtil.toSet(Arrays.asList("a", "b", "a")));
    }

}
