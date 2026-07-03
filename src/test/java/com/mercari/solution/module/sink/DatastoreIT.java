package com.mercari.solution.module.sink;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.DatastoreEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;

/**
 * Integration test (run via maven-failsafe: {@code mvn verify -DskipITs=false -Dit.test=DatastoreIT})
 * for the datastore sink and source modules against the Cloud Datastore emulator managed by
 * Testcontainers.
 *
 * The datastore modules use Beam DatastoreIO, pointed at the emulator via
 * {@code DatastoreV1.withLocalhost}, wired from the modules' {@code emulatorHost} parameter
 * (resolution: parameter > DATASTORE_EMULATOR_HOST env var > system property, see
 * {@code DatastoreUtil.getEmulatorHost}). This test passes the container's mapped endpoint
 * as the {@code emulatorHost} parameter, so no fixed host ports or env vars are required.
 *
 * The emulator is started with {@code --consistency=1.0} so that non-ancestor (GQL) queries
 * are strongly consistent and immediately observe prior writes.
 */
@Testcontainers
public class DatastoreIT {

    private static final double DELTA = 1e-9;

    // must match the project the DatastoreEmulatorContainer starts the emulator with
    private static final String PROJECT = "test-project";

    @Container
    private static final DatastoreEmulatorContainer emulator = new DatastoreEmulatorContainer(
            DockerImageName.parse("gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators"))
            .withFlags("--consistency=1.0");

    /**
     * Creates a TestPipeline. For read pipelines the DirectRunner's byte-level immutability
     * check is disabled, following the same workaround as SpannerIT (the source outputs
     * MElements wrapping protobuf Entities encoded with SerializableCoder).
     */
    private static TestPipeline createPipeline(final boolean read) {
        final DirectOptions options = PipelineOptionsFactory.as(DirectOptions.class);
        options.as(GcpOptions.class).setProject(PROJECT);
        if(read) {
            options.setEnforceImmutability(false);
        }
        return TestPipeline.fromOptions(options).enableAbandonedNodeEnforcement(false);
    }

    @Test
    public void testRoundTrip() throws Exception {
        // pipeline 1: create source -> datastore sink (key name from the id field)
        final String sinkConfigJson = """
                {
                  "sources": [
                    {
                      "name": "create",
                      "module": "create",
                      "outputType": "AVRO",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "id": "a", "longvalue": 1, "doublevalue": 0.15, "boolvalue": true },
                          { "id": "b", "longvalue": 2, "doublevalue": 1.15, "boolvalue": false },
                          { "id": "c", "longvalue": 3, "doublevalue": 2.15, "boolvalue": true }
                        ]
                      },
                      "schema": {
                        "fields": [
                          { "name": "id", "type": "string" },
                          { "name": "longvalue", "type": "int64" },
                          { "name": "doublevalue", "type": "float64" },
                          { "name": "boolvalue", "type": "bool" }
                        ]
                      }
                    }
                  ],
                  "sinks": [
                    {
                      "name": "datastoreSink",
                      "module": "datastore",
                      "inputs": ["create"],
                      "parameters": {
                        "projectId": "%s",
                        "kind": "RoundTrip",
                        "keyFields": ["id"],
                        "emulatorHost": "%s"
                      }
                    }
                  ]
                }
                """.formatted(PROJECT, emulator.getEmulatorEndpoint());

        final TestPipeline writePipeline = createPipeline(false);
        MPipeline.apply(writePipeline, Config.load(sinkConfigJson));
        writePipeline.run().waitUntilFinish();

        // pipeline 2: datastore source (GQL query) -> assert
        final String sourceConfigJson = """
                {
                  "sources": [
                    {
                      "name": "datastoreSource",
                      "module": "datastore",
                      "parameters": {
                        "projectId": "%s",
                        "gql": "SELECT * FROM RoundTrip",
                        "numQuerySplits": 1,
                        "emulatorHost": "%s"
                      },
                      "schema": {
                        "fields": [
                          { "name": "id", "type": "string" },
                          { "name": "longvalue", "type": "int64" },
                          { "name": "doublevalue", "type": "float64" },
                          { "name": "boolvalue", "type": "bool" }
                        ]
                      }
                    }
                  ]
                }
                """.formatted(PROJECT, emulator.getEmulatorEndpoint());

        final TestPipeline readPipeline = createPipeline(true);
        final Map<String, MCollection> outputs = MPipeline.apply(readPipeline, Config.load(sourceConfigJson));
        final MCollection output = outputs.get("datastoreSource");

        PAssert.that(output.getCollection()).satisfies(entities -> {
            int count = 0;
            for(final MElement entity : entities) {
                switch (entity.getAsString("id")) {
                    case "a" -> {
                        Assertions.assertEquals(1L, entity.getAsLong("longvalue"));
                        Assertions.assertEquals(0.15D, entity.getAsDouble("doublevalue"), DELTA);
                        Assertions.assertEquals(Boolean.TRUE, entity.getPrimitiveValue("boolvalue"));
                    }
                    case "b" -> {
                        Assertions.assertEquals(2L, entity.getAsLong("longvalue"));
                        Assertions.assertEquals(1.15D, entity.getAsDouble("doublevalue"), DELTA);
                        Assertions.assertEquals(Boolean.FALSE, entity.getPrimitiveValue("boolvalue"));
                    }
                    case "c" -> {
                        Assertions.assertEquals(3L, entity.getAsLong("longvalue"));
                        Assertions.assertEquals(2.15D, entity.getAsDouble("doublevalue"), DELTA);
                        Assertions.assertEquals(Boolean.TRUE, entity.getPrimitiveValue("boolvalue"));
                    }
                    default -> Assertions.fail("unexpected id: " + entity.getAsString("id"));
                }
                count++;
            }
            Assertions.assertEquals(3, count);
            return null;
        });

        readPipeline.run().waitUntilFinish();
    }

    @Test
    public void testDelete() throws Exception {
        // pipeline 1: insert three entities
        final String insertConfigJson = """
                {
                  "sources": [
                    {
                      "name": "create",
                      "module": "create",
                      "outputType": "AVRO",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "id": "a", "longvalue": 1 },
                          { "id": "b", "longvalue": 2 },
                          { "id": "c", "longvalue": 3 }
                        ]
                      },
                      "schema": {
                        "fields": [
                          { "name": "id", "type": "string" },
                          { "name": "longvalue", "type": "int64" }
                        ]
                      }
                    }
                  ],
                  "sinks": [
                    {
                      "name": "datastoreSink",
                      "module": "datastore",
                      "inputs": ["create"],
                      "parameters": {
                        "projectId": "%s",
                        "kind": "DeleteTest",
                        "keyFields": ["id"],
                        "emulatorHost": "%s"
                      }
                    }
                  ]
                }
                """.formatted(PROJECT, emulator.getEmulatorEndpoint());

        final TestPipeline writePipeline = createPipeline(false);
        MPipeline.apply(writePipeline, Config.load(insertConfigJson));
        writePipeline.run().waitUntilFinish();

        // pipeline 2: delete the entity with key "b"
        final String deleteConfigJson = """
                {
                  "sources": [
                    {
                      "name": "create",
                      "module": "create",
                      "outputType": "AVRO",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "id": "b" }
                        ]
                      },
                      "schema": {
                        "fields": [
                          { "name": "id", "type": "string" }
                        ]
                      }
                    }
                  ],
                  "sinks": [
                    {
                      "name": "datastoreSink",
                      "module": "datastore",
                      "inputs": ["create"],
                      "parameters": {
                        "projectId": "%s",
                        "kind": "DeleteTest",
                        "keyFields": ["id"],
                        "delete": true,
                        "emulatorHost": "%s"
                      }
                    }
                  ]
                }
                """.formatted(PROJECT, emulator.getEmulatorEndpoint());

        final TestPipeline deletePipeline = createPipeline(false);
        MPipeline.apply(deletePipeline, Config.load(deleteConfigJson));
        deletePipeline.run().waitUntilFinish();

        // pipeline 3: read back and verify only "a" and "c" remain
        final String sourceConfigJson = """
                {
                  "sources": [
                    {
                      "name": "datastoreSource",
                      "module": "datastore",
                      "parameters": {
                        "projectId": "%s",
                        "gql": "SELECT * FROM DeleteTest",
                        "numQuerySplits": 1,
                        "emulatorHost": "%s"
                      },
                      "schema": {
                        "fields": [
                          { "name": "id", "type": "string" },
                          { "name": "longvalue", "type": "int64" }
                        ]
                      }
                    }
                  ]
                }
                """.formatted(PROJECT, emulator.getEmulatorEndpoint());

        final TestPipeline readPipeline = createPipeline(true);
        final Map<String, MCollection> outputs = MPipeline.apply(readPipeline, Config.load(sourceConfigJson));
        final MCollection output = outputs.get("datastoreSource");

        PAssert.that(output.getCollection()).satisfies(entities -> {
            int count = 0;
            for(final MElement entity : entities) {
                switch (entity.getAsString("id")) {
                    case "a" -> Assertions.assertEquals(1L, entity.getAsLong("longvalue"));
                    case "c" -> Assertions.assertEquals(3L, entity.getAsLong("longvalue"));
                    default -> Assertions.fail("unexpected id: " + entity.getAsString("id"));
                }
                count++;
            }
            Assertions.assertEquals(2, count);
            return null;
        });

        readPipeline.run().waitUntilFinish();
    }

}
