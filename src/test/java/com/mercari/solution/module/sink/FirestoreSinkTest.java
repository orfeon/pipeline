package com.mercari.solution.module.sink;

import com.google.firestore.v1.Document;
import com.google.firestore.v1.Write;
import com.google.firestore.v1.WriteResult;
import com.google.rpc.Code;
import com.google.rpc.Status;
import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.PCollection;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit test for the firestore sink dead-letter path ({@code failFast: false}).
 * Beam's {@code FirestoreV1.WriteFailure} cannot easily be provoked against the emulator
 * (the batch writer retries most errors), so the FailureDoFn is tested directly with a
 * constructed WriteFailure.
 */
public class FirestoreSinkTest {

    private static final String DOCUMENT_NAME = "projects/p/databases/(default)/documents/MyCollection/doc1";

    /**
     * Assembling the sink reads the Dataflow maxNumWorkers as the rpcQos hint. It must not need the Dataflow
     * runner on the classpath: the direct / prism / flink / spark builds do not ship it (run with -Pdirect).
     */
    @Test
    public void testAssemblyWithoutDataflowRunner() throws Exception {
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "create",
                      "module": "create",
                      "parameters": {
                        "type": "element",
                        "elements": [ { "id": "a" } ]
                      },
                      "schema": {
                        "fields": [ { "name": "id", "type": "string" } ]
                      }
                    }
                  ],
                  "sinks": [
                    {
                      "name": "firestoreSink",
                      "module": "firestore",
                      "inputs": ["create"],
                      "parameters": {
                        "projectId": "myproject",
                        "collection": "MyCollection",
                        "nameFields": ["id"]
                      }
                    }
                  ]
                }
                """;
        final TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        MPipeline.apply(pipeline, Config.load(configJson));
    }

    @Test
    public void testFailureDoFnEmitsBadRecord() {
        final Write write = Write.newBuilder()
                .setUpdate(Document.newBuilder()
                        .setName(DOCUMENT_NAME)
                        .build())
                .build();
        final Status status = Status.newBuilder()
                .setCode(Code.INVALID_ARGUMENT.getNumber())
                .setMessage("invalid document")
                .build();
        final FirestoreV1.WriteFailure writeFailure = new FirestoreV1.WriteFailure(
                write, WriteResult.getDefaultInstance(), status);

        final TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        final PCollection<BadRecord> badRecords = pipeline
                .apply("CreateWriteFailure", Create
                        .of(writeFailure)
                        .withCoder(SerializableCoder.of(FirestoreV1.WriteFailure.class)))
                .apply("Failure", ParDo.of(new FirestoreSink.FailureDoFn()))
                .setCoder(BadRecord.getCoder(pipeline));

        PAssert.that(badRecords).satisfies(iterable -> {
            int count = 0;
            for(final BadRecord badRecord : iterable) {
                Assertions.assertNotNull(badRecord.getFailure());
                Assertions.assertNotNull(badRecord.getRecord());
                Assertions.assertTrue(
                        badRecord.getFailure().getDescription().contains("firestore"),
                        "description should mention firestore: " + badRecord.getFailure().getDescription());
                Assertions.assertNotNull(badRecord.getRecord().getHumanReadableJsonRecord());
                Assertions.assertTrue(
                        badRecord.getRecord().getHumanReadableJsonRecord().contains(DOCUMENT_NAME),
                        "record should contain the document name: " + badRecord.getRecord().getHumanReadableJsonRecord());
                count++;
            }
            Assertions.assertEquals(1, count);
            return null;
        });

        pipeline.run().waitUntilFinish();
    }

}
