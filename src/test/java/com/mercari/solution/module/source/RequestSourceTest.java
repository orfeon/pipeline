package com.mercari.solution.module.source;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.*;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RequestSourceTest {

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    private static final String CONFIG = """
            {
              "sources": [
                {
                  "name": "request",
                  "module": "request",
                  "parameters": {
                    "schema": {
                      "fields": [
                        { "name": "id", "type": "string" },
                        { "name": "value", "type": "int64" }
                      ]
                    }
                  }
                }
              ]
            }
            """;

    @Test
    public void testRequestBodyArray() throws Exception {
        pipeline.getOptions().as(MPipeline.MPipelineOptions.class)
                .setRequestBody("""
                        [
                          { "id": "a", "value": 1 },
                          { "id": "b", "value": 2 }
                        ]
                        """);

        final Config config = Config.load(CONFIG);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        final MCollection output = outputs.get("request");
        Assertions.assertEquals(Schema.Type.string, output.getSchema().getField("id").getFieldType().getType());
        Assertions.assertEquals(Schema.Type.int64, output.getSchema().getField("value").getFieldType().getType());

        PAssert.that(output.getCollection()).satisfies(elements -> {
            final Set<String> ids = new HashSet<>();
            long sum = 0;
            for(final MElement element : elements) {
                ids.add(element.getAsString("id"));
                sum += element.getAsLong("value");
            }
            Assertions.assertEquals(Set.of("a", "b"), ids);
            Assertions.assertEquals(3L, sum);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testRequestBodySingleObject() throws Exception {
        pipeline.getOptions().as(MPipeline.MPipelineOptions.class)
                .setRequestBody("{ \"id\": \"only\", \"value\": 10 }");

        final Config config = Config.load(CONFIG);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        PAssert.that(outputs.get("request").getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement element : elements) {
                Assertions.assertEquals("only", element.getAsString("id"));
                Assertions.assertEquals(10L, element.getAsLong("value"));
                count++;
            }
            Assertions.assertEquals(1, count);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testPathAndSampleFallback() throws Exception {
        // no requestBody option: the sample parameter supplies the body, path selects the subtree
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "request",
                      "module": "request",
                      "parameters": {
                        "path": "payload.items",
                        "sample": {
                          "payload": {
                            "items": [
                              { "id": "s1", "value": 1 },
                              { "id": "s2", "value": 2 },
                              { "id": "s3", "value": 3 }
                            ]
                          }
                        },
                        "schema": {
                          "fields": [
                            { "name": "id", "type": "string" },
                            { "name": "value", "type": "int64" }
                          ]
                        }
                      }
                    }
                  ]
                }
                """;

        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        PAssert.that(outputs.get("request").getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement element : elements) {
                count++;
            }
            Assertions.assertEquals(3, count);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testMissingBodyFails() throws Exception {
        final Config config = Config.load(CONFIG);
        final IllegalModuleException e = Assertions.assertThrows(IllegalModuleException.class,
                () -> MPipeline.apply(pipeline, config));
        Assertions.assertTrue(e.getMessage().contains("no request body"),
                "unexpected message: " + e.getMessage());
    }

    @Test
    public void testMissingSchemaFails() throws Exception {
        pipeline.getOptions().as(MPipeline.MPipelineOptions.class)
                .setRequestBody("[]");
        final String configJson = """
                {
                  "sources": [
                    { "name": "request", "module": "request" }
                  ]
                }
                """;
        final Config config = Config.load(configJson);
        final IllegalModuleException e = Assertions.assertThrows(IllegalModuleException.class,
                () -> MPipeline.apply(pipeline, config));
        Assertions.assertTrue(e.getMessage().contains("requires parameters.schema"),
                "unexpected message: " + e.getMessage());
    }

}
