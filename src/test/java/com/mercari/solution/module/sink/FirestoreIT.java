package com.mercari.solution.module.sink;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.FirestoreEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;

/**
 * Integration test (run via maven-failsafe: {@code mvn verify -DskipITs=false -Dit.test=FirestoreIT})
 * for the firestore sink and source modules against the Firestore emulator managed by Testcontainers.
 *
 * The firestore modules use Beam FirestoreIO, which resolves the emulator endpoint from Beam's
 * {@code org.apache.beam.sdk.io.gcp.firestore.FirestoreOptions#getEmulatorHost()}. In production
 * this is wired from the config options block ({@code options.gcp.firestore.emulatorHost}, see
 * {@code GCPOptions.FirestoreOptions.setOptions}), but that wiring only runs in
 * {@code MPipeline.main} — {@code MPipeline.apply(pipeline, config)} does not apply the options
 * block — so this test sets FirestoreOptions on the TestPipeline's options directly.
 * With emulatorHost set, Beam's FirestoreStatefulComponentFactory switches to emulator
 * credentials, so no real GCP credentials are required.
 */
@Testcontainers
public class FirestoreIT {

    private static final double DELTA = 1e-9;

    private static final String PROJECT = "test-project";

    @Container
    private static final FirestoreEmulatorContainer emulator = new FirestoreEmulatorContainer(
            DockerImageName.parse("gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators"));

    /**
     * Creates a TestPipeline whose options point Beam FirestoreIO at the emulator.
     * For read pipelines the DirectRunner's byte-level immutability check is disabled,
     * following the same workaround as SpannerIT (the source outputs MElements wrapping
     * protobuf Documents encoded with SerializableCoder).
     */
    private static TestPipeline createPipeline(final boolean read) {
        final DirectOptions options = PipelineOptionsFactory.as(DirectOptions.class);
        options.as(GcpOptions.class).setProject(PROJECT);
        final FirestoreOptions firestoreOptions = options.as(FirestoreOptions.class);
        firestoreOptions.setEmulatorHost(emulator.getEmulatorEndpoint());
        firestoreOptions.setFirestoreProject(PROJECT);
        if(read) {
            options.setEnforceImmutability(false);
        }
        return TestPipeline.fromOptions(options).enableAbandonedNodeEnforcement(false);
    }

    @Test
    public void testRoundTrip() throws Exception {
        // pipeline 1: create source -> firestore sink (document name from the id field)
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
                      "name": "firestoreSink",
                      "module": "firestore",
                      "inputs": ["create"],
                      "parameters": {
                        "projectId": "%s",
                        "collection": "RoundTrip",
                        "nameFields": ["id"]
                      }
                    }
                  ]
                }
                """.formatted(PROJECT);

        final TestPipeline writePipeline = createPipeline(false);
        MPipeline.apply(writePipeline, Config.load(sinkConfigJson));
        writePipeline.run().waitUntilFinish();

        // pipeline 2: firestore source (list documents in the collection) -> assert
        final String sourceConfigJson = """
                {
                  "sources": [
                    {
                      "name": "firestoreSource",
                      "module": "firestore",
                      "parameters": {
                        "projectId": "%s",
                        "collection": "RoundTrip"
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
                """.formatted(PROJECT);

        final TestPipeline readPipeline = createPipeline(true);
        final Map<String, MCollection> outputs = MPipeline.apply(readPipeline, Config.load(sourceConfigJson));
        final MCollection output = outputs.get("firestoreSource");

        PAssert.that(output.getCollection()).satisfies(docs -> {
            int count = 0;
            for(final MElement doc : docs) {
                switch (doc.getAsString("id")) {
                    case "a" -> {
                        Assertions.assertEquals(1L, doc.getAsLong("longvalue"));
                        Assertions.assertEquals(0.15D, doc.getAsDouble("doublevalue"), DELTA);
                        Assertions.assertEquals(Boolean.TRUE, doc.getPrimitiveValue("boolvalue"));
                    }
                    case "b" -> {
                        Assertions.assertEquals(2L, doc.getAsLong("longvalue"));
                        Assertions.assertEquals(1.15D, doc.getAsDouble("doublevalue"), DELTA);
                        Assertions.assertEquals(Boolean.FALSE, doc.getPrimitiveValue("boolvalue"));
                    }
                    case "c" -> {
                        Assertions.assertEquals(3L, doc.getAsLong("longvalue"));
                        Assertions.assertEquals(2.15D, doc.getAsDouble("doublevalue"), DELTA);
                        Assertions.assertEquals(Boolean.TRUE, doc.getPrimitiveValue("boolvalue"));
                    }
                    default -> Assertions.fail("unexpected id: " + doc.getAsString("id"));
                }
                count++;
            }
            Assertions.assertEquals(3, count);
            return null;
        });

        readPipeline.run().waitUntilFinish();
    }

    @Test
    public void testQueryWithFilter() throws Exception {
        // pipeline 1: create source -> firestore sink into a dedicated collection
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
                      "name": "firestoreSink",
                      "module": "firestore",
                      "inputs": ["create"],
                      "parameters": {
                        "projectId": "%s",
                        "collection": "QueryTest",
                        "nameFields": ["id"]
                      }
                    }
                  ]
                }
                """.formatted(PROJECT);

        final TestPipeline writePipeline = createPipeline(false);
        MPipeline.apply(writePipeline, Config.load(sinkConfigJson));
        writePipeline.run().waitUntilFinish();

        // pipeline 2: firestore source with filter (structured query / runQuery path) -> assert
        final String sourceConfigJson = """
                {
                  "sources": [
                    {
                      "name": "firestoreSource",
                      "module": "firestore",
                      "parameters": {
                        "projectId": "%s",
                        "collection": "QueryTest",
                        "filter": "longvalue >= 2"
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
                """.formatted(PROJECT);

        final TestPipeline readPipeline = createPipeline(true);
        final Map<String, MCollection> outputs = MPipeline.apply(readPipeline, Config.load(sourceConfigJson));
        final MCollection output = outputs.get("firestoreSource");

        PAssert.that(output.getCollection()).satisfies(docs -> {
            int count = 0;
            for(final MElement doc : docs) {
                switch (doc.getAsString("id")) {
                    case "b" -> Assertions.assertEquals(2L, doc.getAsLong("longvalue"));
                    case "c" -> Assertions.assertEquals(3L, doc.getAsLong("longvalue"));
                    default -> Assertions.fail("unexpected id: " + doc.getAsString("id"));
                }
                count++;
            }
            Assertions.assertEquals(2, count);
            return null;
        });

        readPipeline.run().waitUntilFinish();
    }

}
