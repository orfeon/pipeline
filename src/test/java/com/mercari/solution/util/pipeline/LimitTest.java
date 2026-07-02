package com.mercari.solution.util.pipeline;

import com.google.gson.Gson;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.coder.ElementCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LimitTest {

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    private static Schema createSchema() {
        return Schema.of(List.of(
                Schema.Field.of("id", Schema.FieldType.INT64)));
    }

    private static MElement element(long id) {
        final Map<String, Object> values = new HashMap<>();
        values.put("id", id);
        return MElement.of(values, Instant.parse("2025-01-01T00:00:00Z"));
    }

    @Test
    public void testParametersValidate() {
        final Limit.LimitParameters missing = new Gson().fromJson("{}", Limit.LimitParameters.class);
        Assertions.assertEquals(1, missing.validate().size());

        final Limit.LimitParameters invalidCount = new Gson().fromJson("{\"count\":0}", Limit.LimitParameters.class);
        Assertions.assertEquals(1, invalidCount.validate().size());

        final Limit.LimitParameters valid = new Gson().fromJson("{\"count\":2}", Limit.LimitParameters.class);
        valid.setDefaults();
        Assertions.assertTrue(valid.validate().isEmpty());
        Assertions.assertEquals(2, valid.getCount());
        Assertions.assertNull(valid.getOutputStartAt());
    }

    @Test
    public void testLimitBatch() {
        final Schema schema = createSchema();
        final Limit.LimitParameters parameters = new Gson().fromJson("{\"count\":2}", Limit.LimitParameters.class);

        final PCollection<KV<String, MElement>> input = pipeline
                .apply("Create", Create
                        .timestamped(
                                TimestampedValue.of(KV.of("k", element(1L)), Instant.parse("2025-01-01T00:00:01Z")),
                                TimestampedValue.of(KV.of("k", element(2L)), Instant.parse("2025-01-01T00:00:02Z")),
                                TimestampedValue.of(KV.of("k", element(3L)), Instant.parse("2025-01-01T00:00:03Z")),
                                TimestampedValue.of(KV.of("k", element(4L)), Instant.parse("2025-01-01T00:00:04Z")))
                        .withCoder(KvCoder.of(StringUtf8Coder.of(), ElementCoder.of(schema))));

        final PCollection<MElement> outputs = input
                .apply("Limit", Limit.of(parameters, List.of(schema), false));

        PAssert.that(outputs).satisfies(elements -> {
            final Set<Long> ids = new HashSet<>();
            for(final MElement element : elements) {
                ids.add(element.getAsLong("id"));
            }
            // time sorted input: the earliest two elements are output
            Assertions.assertEquals(Set.of(1L, 2L), ids);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testLimitBatchWithoutCount() {
        final Schema schema = createSchema();
        // outputStartAt only: elements after outputStartAt are output without count limit
        final Limit.LimitParameters parameters = new Gson().fromJson("{}", Limit.LimitParameters.class);

        final PCollection<KV<String, MElement>> input = pipeline
                .apply("Create", Create
                        .timestamped(
                                TimestampedValue.of(KV.of("k", element(1L)), Instant.parse("2025-01-01T00:00:01Z")),
                                TimestampedValue.of(KV.of("k", element(2L)), Instant.parse("2025-01-01T00:00:02Z")))
                        .withCoder(KvCoder.of(StringUtf8Coder.of(), ElementCoder.of(schema))));

        final PCollection<MElement> outputs = input
                .apply("Limit", Limit.of(parameters, List.of(schema), false));

        PAssert.that(outputs).satisfies(elements -> {
            final Set<Long> ids = new HashSet<>();
            for(final MElement element : elements) {
                ids.add(element.getAsLong("id"));
            }
            Assertions.assertEquals(Set.of(1L, 2L), ids);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testLimitStreaming() {
        final Schema schema = createSchema();
        final Limit.LimitParameters parameters = new Gson().fromJson("{\"count\":1}", Limit.LimitParameters.class);

        final TestStream<KV<String, MElement>> stream = TestStream
                .create(KvCoder.of(StringUtf8Coder.of(), ElementCoder.of(schema)))
                .addElements(
                        TimestampedValue.of(KV.of("k", element(1L)), Instant.parse("2025-01-01T00:00:01Z")),
                        TimestampedValue.of(KV.of("k", element(2L)), Instant.parse("2025-01-01T00:00:02Z")))
                .advanceWatermarkToInfinity();

        final PCollection<MElement> outputs = pipeline
                .apply("Stream", stream)
                .apply("Limit", Limit.of(parameters, List.of(schema), true));

        PAssert.that(outputs).satisfies(elements -> {
            int count = 0;
            for(final MElement ignored : elements) {
                count++;
            }
            Assertions.assertEquals(1, count);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testLimitStreamingOutputStartAt() throws Exception {
        final Schema schema = createSchema();

        final Limit.LimitParameters parameters = new Gson().fromJson("{}", Limit.LimitParameters.class);
        final java.lang.reflect.Field field = Limit.LimitParameters.class.getDeclaredField("outputStartAt");
        field.setAccessible(true);
        field.set(parameters, Instant.parse("2025-01-01T00:01:00Z"));

        final TestStream<KV<String, MElement>> stream = TestStream
                .create(KvCoder.of(StringUtf8Coder.of(), ElementCoder.of(schema)))
                .addElements(
                        TimestampedValue.of(KV.of("k", element(1L)), Instant.parse("2025-01-01T00:00:01Z")),
                        TimestampedValue.of(KV.of("k", element(2L)), Instant.parse("2025-01-01T00:00:02Z")))
                .advanceWatermarkTo(Instant.parse("2025-01-01T00:00:30Z"))
                .addElements(
                        TimestampedValue.of(KV.of("k", element(3L)), Instant.parse("2025-01-01T00:02:00Z")))
                .advanceWatermarkToInfinity();

        final PCollection<MElement> outputs = pipeline
                .apply("Stream", stream)
                .apply("Limit", Limit.of(parameters, List.of(schema), true));

        PAssert.that(outputs).satisfies(elements -> {
            final Set<Long> ids = new HashSet<>();
            for(final MElement element : elements) {
                ids.add(element.getAsLong("id"));
            }
            // element 3 is output directly (after outputStartAt),
            // element 2 is the latest buffered element and is emitted by the timer
            Assertions.assertEquals(Set.of(2L, 3L), ids);
            return null;
        });

        pipeline.run();
    }

}
