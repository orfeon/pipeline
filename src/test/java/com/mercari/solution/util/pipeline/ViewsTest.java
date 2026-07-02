package com.mercari.solution.util.pipeline;

import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.coder.ElementCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ViewsTest {

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    @Test
    public void testOf() {
        Assertions.assertInstanceOf(Views.SingletonView.class, Views.of(Views.Type.singleton));
        Assertions.assertNotNull(Views.singleton());
        Assertions.assertThrows(IllegalArgumentException.class, () -> Views.of(Views.Type.map));
        Assertions.assertThrows(IllegalArgumentException.class, () -> Views.of(Views.Type.list));
    }

    @Test
    public void testSingletonView() {
        final Schema schema = Schema.of(List.of(
                Schema.Field.of("stringField", Schema.FieldType.STRING),
                Schema.Field.of("longField", Schema.FieldType.INT64)));

        final Map<String, Object> values = new HashMap<>();
        values.put("stringField", "a");
        values.put("longField", 1L);
        final MElement element = MElement.of(values, Instant.parse("2025-01-01T00:00:00Z"));

        final PCollectionView<Map<String, Object>> view = pipeline
                .apply("CreateView", Create.of(element).withCoder(ElementCoder.of(schema)))
                .apply("Singleton", Views.singleton());

        final PCollection<String> outputs = pipeline
                .apply("CreateMain", Create.of("main"))
                .apply("ReadView", ParDo
                        .of(new ReadViewDoFn(view))
                        .withSideInputs(view));

        PAssert.that(outputs).containsInAnyOrder("a:1");

        pipeline.run();
    }

    private static class ReadViewDoFn extends DoFn<String, String> {

        private final PCollectionView<Map<String, Object>> view;

        ReadViewDoFn(final PCollectionView<Map<String, Object>> view) {
            this.view = view;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final Map<String, Object> values = c.sideInput(view);
            c.output(values.get("stringField") + ":" + values.get("longField"));
        }

    }

}
