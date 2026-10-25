package com.mercari.solution.config.options;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.SdkHarnessOptions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class RunnerOptionSetterTest {

    public enum Mode { FAST, SAFE }

    public interface TestOptions extends PipelineOptions {
        boolean getPrimitiveFlag();
        void setPrimitiveFlag(boolean value);
        Boolean getBoxedFlag();
        void setBoxedFlag(Boolean value);
        Long getInterval();
        void setInterval(Long value);
        int getCount();
        void setCount(int value);
        Mode getMode();
        void setMode(Mode value);
        List<String> getItems();
        void setItems(List<String> value);
    }

    /** The setter is found by name: a Boolean config value fits a boolean setter and vice versa, numbers are widened / narrowed. */
    @Test
    public void testSetConvertsToTheSetterParameterType() {
        final PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        RunnerOptionSetter.of(pipelineOptions, TestOptions.class)
                .set("primitiveFlag", Boolean.TRUE)
                .set("boxedFlag", false)
                .set("interval", 1000)
                .set("count", 7L)
                .set("mode", "SAFE")
                .set("items", List.of("a", "b"))
                .set("count", null);
        final TestOptions options = pipelineOptions.as(TestOptions.class);
        Assertions.assertTrue(options.getPrimitiveFlag());
        Assertions.assertEquals(Boolean.FALSE, options.getBoxedFlag());
        Assertions.assertEquals(1000L, options.getInterval());
        Assertions.assertEquals(7, options.getCount(), "a null value leaves the option alone");
        Assertions.assertEquals(Mode.SAFE, options.getMode());
        Assertions.assertEquals(List.of("a", "b"), options.getItems());
    }

    @Test
    public void testUnknownEnumAndUnknownPropertyFail() {
        final RunnerOptionSetter setter = RunnerOptionSetter.of(PipelineOptionsFactory.create(), TestOptions.class);
        final IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class, () -> setter.set("mode", "safe"));
        Assertions.assertTrue(e.getMessage().contains("[FAST, SAFE]"), e.getMessage());
        Assertions.assertThrows(IllegalStateException.class, () -> setter.set("noSuchOption", "x"));
    }

    /** A plain map is converted through the typed map's from(Map) factory (SdkHarnessLogLevelOverrides). */
    @Test
    public void testMapIsConvertedToTypedOverrides() {
        final PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        RunnerOptionSetter.of(pipelineOptions, SdkHarnessOptions.class)
                .set("sdkHarnessLogLevelOverrides", Map.of("org.apache.kafka", "WARN"));
        Assertions.assertEquals(SdkHarnessOptions.LogLevel.WARN,
                pipelineOptions.as(SdkHarnessOptions.class).getSdkHarnessLogLevelOverrides().get("org.apache.kafka"));

        Assertions.assertThrows(IllegalArgumentException.class, () -> RunnerOptionSetter.of(pipelineOptions, SdkHarnessOptions.class)
                .set("sdkHarnessLogLevelOverrides", Map.of("org.apache.kafka", "VERBOSE")));
    }

}
