package com.mercari.solution.module;

import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MErrorHandlerTest {

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    // The Beam IO builders are immutable, so apply must return a new instance with the
    // error handler attached. Returning the untouched input silently disables failure routing.
    @Test
    public void testApplyReturnsTransformWithErrorHandler() {
        final MErrorHandler errorHandler = MErrorHandler.dummy(pipeline);

        final FileIO.Write<Void, String> fileWrite = FileIO.<String>write().via(TextIO.sink());
        Assertions.assertNotSame(fileWrite, errorHandler.apply(fileWrite));

        final PubsubIO.Write<org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage> pubsubWrite = PubsubIO.writeMessages();
        Assertions.assertNotSame(pubsubWrite, errorHandler.apply(pubsubWrite));

        final PubsubIO.Read<org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage> pubsubRead = PubsubIO.readMessages();
        Assertions.assertNotSame(pubsubRead, errorHandler.apply(pubsubRead));

        final BigtableIO.Write bigtableWrite = BigtableIO.write();
        Assertions.assertNotSame(bigtableWrite, errorHandler.apply(bigtableWrite));

        final BigQueryIO.Write<com.google.api.services.bigquery.model.TableRow> bigqueryWrite = BigQueryIO.writeTableRows();
        Assertions.assertNotSame(bigqueryWrite, errorHandler.apply(bigqueryWrite));

        final BigQueryIO.TypedRead<com.google.api.services.bigquery.model.TableRow> bigqueryRead = BigQueryIO.readTableRows();
        Assertions.assertNotSame(bigqueryRead, errorHandler.apply(bigqueryRead));
    }

    @Test
    public void testEmptyHandlerReturnsSameInstance() {
        final MErrorHandler empty = MErrorHandler.empty();

        final FileIO.Write<Void, String> fileWrite = FileIO.<String>write().via(TextIO.sink());
        Assertions.assertSame(fileWrite, empty.apply(fileWrite));

        final PubsubIO.Write<org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage> pubsubWrite = PubsubIO.writeMessages();
        Assertions.assertSame(pubsubWrite, empty.apply(pubsubWrite));
    }

}
