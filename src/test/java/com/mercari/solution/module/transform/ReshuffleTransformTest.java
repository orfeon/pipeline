package com.mercari.solution.module.transform;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ReshuffleTransformTest {

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    @Test
    public void testReshuffleWithoutParameters() throws Exception {
        // Omitting the parameters block entirely must behave as empty parameters, not throw NPE
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "reshuffleNoParamsCreate",
                      "module": "create",
                      "parameters": {
                        "type": "int64",
                        "elements": [1, 2]
                      }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "reshuffle",
                      "module": "reshuffle",
                      "inputs": ["reshuffleNoParamsCreate"]
                    }
                  ]
                }
                """;

        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);
        Assertions.assertNotNull(outputs.get("reshuffle"));
        pipeline.run();
    }

    @Test
    public void testReshuffleSingleInput() throws Exception {

        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "reshuffleSingleCreate",
                      "module": "create",
                      "parameters": {
                        "type": "int64",
                        "elements": [10, 20, 30, 40]
                      }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "reshuffle",
                      "module": "reshuffle",
                      "inputs": ["reshuffleSingleCreate"],
                      "parameters": {}
                    }
                  ]
                }
                """;

        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        final MCollection output = outputs.get("reshuffle");
        Assertions.assertNotNull(output);

        // schema must be kept as-is
        final Schema outputSchema = output.getSchema();
        Assertions.assertEquals(Schema.Type.int64, outputSchema.getField("value").getFieldType().getType());

        PAssert.that(output.getCollection()).satisfies(elements -> {
            final Set<Long> values = new HashSet<>();
            int count = 0;
            for(final MElement element : elements) {
                values.add((Long) element.getPrimitiveValue("value"));
                count++;
            }
            Assertions.assertEquals(4, count);
            Assertions.assertEquals(Set.of(10L, 20L, 30L, 40L), values);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testReshuffleMultiInputs() throws Exception {

        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "reshuffleMultiCreateA",
                      "module": "create",
                      "parameters": {
                        "type": "int64",
                        "elements": [1, 2]
                      }
                    },
                    {
                      "name": "reshuffleMultiCreateB",
                      "module": "create",
                      "parameters": {
                        "type": "int64",
                        "elements": [3, 4, 5]
                      }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "reshuffle",
                      "module": "reshuffle",
                      "inputs": ["reshuffleMultiCreateA", "reshuffleMultiCreateB"],
                      "parameters": {}
                    }
                  ]
                }
                """;

        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        // with multiple inputs, each input is reshuffled and output under its own tag
        final MCollection outputA = outputs.get("reshuffle.reshuffleMultiCreateA");
        Assertions.assertNotNull(outputA);
        PAssert.that(outputA.getCollection()).satisfies(elements -> {
            final Set<Long> values = new HashSet<>();
            for(final MElement element : elements) {
                values.add((Long) element.getPrimitiveValue("value"));
            }
            Assertions.assertEquals(Set.of(1L, 2L), values);
            return null;
        });

        final MCollection outputB = outputs.get("reshuffle.reshuffleMultiCreateB");
        Assertions.assertNotNull(outputB);
        PAssert.that(outputB.getCollection()).satisfies(elements -> {
            final Set<Long> values = new HashSet<>();
            for(final MElement element : elements) {
                values.add((Long) element.getPrimitiveValue("value"));
            }
            Assertions.assertEquals(Set.of(3L, 4L, 5L), values);
            return null;
        });

        pipeline.run();
    }

}
