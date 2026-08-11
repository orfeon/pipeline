package com.mercari.solution.util.coder;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class StructCoderTest {

    @Test
    public void testRoundTrip() throws Exception {
        final Struct child = Struct.newBuilder()
                .set("id").to(10L)
                .build();
        final Struct struct = Struct.newBuilder()
                .set("bool").to(true)
                .set("bytes").to(ByteArray.copyFrom(new byte[]{1, 2, 3}))
                .set("name").to("test")
                .set("json").to(Value.json("{\"key\":\"value\"}"))
                .set("int64").to(20L)
                .set("float32").to(1.25F)
                .set("float64").to(2.5D)
                .set("numeric").to(new BigDecimal("123.456"))
                .set("date").to(Date.fromYearMonthDay(2026, 8, 11))
                .set("timestamp").to(Timestamp.ofTimeSecondsAndNanos(123L, 456000000))
                .set("uuid").to(UUID.fromString("123e4567-e89b-12d3-a456-426614174000"))
                .set("child").to(child)
                .set("strings").toStringArray(Arrays.asList("a", null, "c"))
                .set("children").toStructArray(child.getType(), Arrays.asList(child, null))
                .build();

        final byte[] encoded = CoderUtils.encodeToByteArray(StructCoder.of(), struct);
        final Struct decoded = CoderUtils.decodeFromByteArray(StructCoder.of(), encoded);

        Assert.assertEquals(struct, decoded);
    }

    @Test
    public void testNameBasedAccessDoesNotChangeEncoding() throws Exception {
        final Struct child = Struct.newBuilder()
                .set("id").to(10L)
                .build();
        final Struct struct = Struct.newBuilder()
                .set("name").to("test")
                .set("child").to(child)
                .set("children").toStructArray(child.getType(), List.of(child))
                .build();
        final ElementCoder coder = ElementCoder.of(Schema.of(struct.getType()));
        final MElement element = MElement.of(struct, Instant.EPOCH);
        final byte[] before = CoderUtils.encodeToByteArray(coder, element);

        Assert.assertEquals("test", struct.getString("name"));
        Assert.assertEquals(10L, struct.getStruct("child").getLong("id"));
        Assert.assertEquals(10L, struct.getStructList("children").getFirst().getLong("id"));

        final byte[] after = CoderUtils.encodeToByteArray(coder, element);
        Assert.assertArrayEquals(before, after);
    }

    @Test
    public void testDirectRunnerAllowsNameBasedAccess() {
        final Struct struct = Struct.newBuilder()
                .set("name").to("test")
                .build();
        final ElementCoder coder = ElementCoder.of(Schema.of(struct.getType()));
        final MElement element = MElement.of(struct, Instant.EPOCH);
        final Pipeline pipeline = Pipeline.create();

        PAssert.that(pipeline
                        .apply(Create.of(element).withCoder(coder))
                        .apply(MapElements.into(TypeDescriptors.strings())
                                .via(e -> ((Struct) e.getValue()).getString("name"))))
                .containsInAnyOrder("test");

        pipeline.run().waitUntilFinish();
    }
}
