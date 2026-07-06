package com.mercari.solution.module.sink;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PubSubEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Integration test (run via maven-failsafe:
 * {@code mvn verify -DskipITs=false -Dit.test=PubSubIT}) for the pubsub sink and source modules
 * against the Cloud Pub/Sub emulator managed by Testcontainers.
 *
 * Beam PubsubIO is pointed at the emulator via {@code PubsubOptions.setPubsubRootUrl}
 * ({@code http://host:port}); the modules themselves need no extra wiring as long as an explicit
 * schema is passed (schema resolution from a topic/subscription via PubSubUtil is only attempted
 * when no schema is configured). A {@link TestCredential} is set so the test does not require
 * application default credentials on the host.
 *
 * The pubsub source module is streaming-only and unbounded (PubsubIO has no maxNumRecords), so
 * {@link #testSourceRead()} runs the pipeline non-blocking ({@code DirectOptions.setBlockOnRun
 * (false)}) with a debug sink writing windowed Avro files to a workDir
 * ({@code MPipelineServerOptions.setWorkDir}), publishes messages to the emulator, polls the
 * workDir for the output records and finally cancels the pipeline.
 */
@Testcontainers
public class PubSubIT {

    private static final double DELTA = 1e-9;

    private static final String PROJECT = "test-project";

    private static final TopicName SINK_TOPIC = TopicName.of(PROJECT, "sink-topic");
    private static final SubscriptionName SINK_SUBSCRIPTION = SubscriptionName.of(PROJECT, "sink-sub");
    private static final TopicName SOURCE_TOPIC = TopicName.of(PROJECT, "source-topic");
    private static final SubscriptionName SOURCE_SUBSCRIPTION = SubscriptionName.of(PROJECT, "source-sub");

    @Container
    private static final PubSubEmulatorContainer emulator = new PubSubEmulatorContainer(
            DockerImageName.parse("gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators"));

    private static ManagedChannel channel;
    private static TransportChannelProvider channelProvider;

    @BeforeAll
    static void setupTopicsAndSubscriptions() throws Exception {
        channel = ManagedChannelBuilder.forTarget(emulator.getEmulatorEndpoint()).usePlaintext().build();
        channelProvider = FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));

        try(final TopicAdminClient topicAdmin = TopicAdminClient.create(TopicAdminSettings.newBuilder()
                .setTransportChannelProvider(channelProvider)
                .setCredentialsProvider(NoCredentialsProvider.create())
                .build())) {
            topicAdmin.createTopic(SINK_TOPIC);
            topicAdmin.createTopic(SOURCE_TOPIC);
        }
        try(final SubscriptionAdminClient subscriptionAdmin = SubscriptionAdminClient.create(SubscriptionAdminSettings.newBuilder()
                .setTransportChannelProvider(channelProvider)
                .setCredentialsProvider(NoCredentialsProvider.create())
                .build())) {
            subscriptionAdmin.createSubscription(SINK_SUBSCRIPTION, SINK_TOPIC, PushConfig.getDefaultInstance(), 60);
            subscriptionAdmin.createSubscription(SOURCE_SUBSCRIPTION, SOURCE_TOPIC, PushConfig.getDefaultInstance(), 60);
        }
    }

    @AfterAll
    static void cleanup() {
        if(channel != null) {
            channel.shutdownNow();
        }
    }

    private static DirectOptions createOptions() {
        final DirectOptions options = PipelineOptionsFactory.as(DirectOptions.class);
        options.as(GcpOptions.class).setProject(PROJECT);
        options.as(GcpOptions.class).setGcpCredential(new EmulatorCredentials());
        options.as(PubsubOptions.class).setPubsubRootUrl("http://" + emulator.getEmulatorEndpoint());
        return options;
    }

    @Test
    public void testSinkPublish() throws Exception {
        // pipeline: create source -> pubsub sink (json format, static attribute)
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
                      "name": "pubsubSink",
                      "module": "pubsub",
                      "inputs": ["create"],
                      "parameters": {
                        "topic": "%s",
                        "format": "json",
                        "attributes": {
                          "origin": "pubsub-it"
                        }
                      }
                    }
                  ]
                }
                """.formatted(SINK_TOPIC.toString());

        final TestPipeline pipeline = TestPipeline.fromOptions(createOptions()).enableAbandonedNodeEnforcement(false);
        MPipeline.apply(pipeline, Config.load(sinkConfigJson));
        pipeline.run().waitUntilFinish();

        // verify by pulling from the subscription attached to the topic
        final SubscriberStubSettings settings = SubscriberStubSettings.newBuilder()
                .setTransportChannelProvider(channelProvider)
                .setCredentialsProvider(NoCredentialsProvider.create())
                .build();
        final Map<String, JsonObject> payloads = new HashMap<>();
        try(final SubscriberStub subscriber = GrpcSubscriberStub.create(settings)) {
            final long timeoutMillis = Instant.now().toEpochMilli() + 60_000L;
            while(payloads.size() < 3 && Instant.now().toEpochMilli() < timeoutMillis) {
                final PullResponse response = subscriber.pullCallable().call(PullRequest.newBuilder()
                        .setSubscription(SINK_SUBSCRIPTION.toString())
                        .setMaxMessages(10)
                        .build());
                final List<String> ackIds = new ArrayList<>();
                for(final ReceivedMessage received : response.getReceivedMessagesList()) {
                    final PubsubMessage message = received.getMessage();
                    Assertions.assertEquals("pubsub-it", message.getAttributesOrThrow("origin"));
                    final JsonObject json = new Gson().fromJson(
                            message.getData().toString(StandardCharsets.UTF_8), JsonObject.class);
                    payloads.put(json.get("id").getAsString(), json);
                    ackIds.add(received.getAckId());
                }
                if(!ackIds.isEmpty()) {
                    subscriber.acknowledgeCallable().call(AcknowledgeRequest.newBuilder()
                            .setSubscription(SINK_SUBSCRIPTION.toString())
                            .addAllAckIds(ackIds)
                            .build());
                }
            }
        }

        Assertions.assertEquals(3, payloads.size());
        Assertions.assertEquals(1L, payloads.get("a").get("longvalue").getAsLong());
        Assertions.assertEquals(0.15D, payloads.get("a").get("doublevalue").getAsDouble(), DELTA);
        Assertions.assertTrue(payloads.get("a").get("boolvalue").getAsBoolean());
        Assertions.assertEquals(2L, payloads.get("b").get("longvalue").getAsLong());
        Assertions.assertEquals(1.15D, payloads.get("b").get("doublevalue").getAsDouble(), DELTA);
        Assertions.assertFalse(payloads.get("b").get("boolvalue").getAsBoolean());
        Assertions.assertEquals(3L, payloads.get("c").get("longvalue").getAsLong());
        Assertions.assertEquals(2.15D, payloads.get("c").get("doublevalue").getAsDouble(), DELTA);
        Assertions.assertTrue(payloads.get("c").get("boolvalue").getAsBoolean());
    }

    @Test
    public void testSourceRead() throws Exception {
        // streaming pipeline: pubsub source (subscription, json format) -> debug sink writing
        // windowed avro files into workDir (see DebugSink#expandLocal / Debug.DebugTransform)
        final String sourceConfigJson = """
                {
                  "sources": [
                    {
                      "name": "pubsubSource",
                      "module": "pubsub",
                      "parameters": {
                        "subscription": "%s",
                        "format": "json"
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
                      "name": "debug",
                      "module": "debug",
                      "inputs": ["pubsubSource"]
                    }
                  ]
                }
                """.formatted(SOURCE_SUBSCRIPTION.toString());

        final Path workDir = Files.createTempDirectory(Path.of("target"), "pubsub-it-work-");

        final DirectOptions options = createOptions();
        options.as(StreamingOptions.class).setStreaming(true);
        options.setBlockOnRun(false);
        options.as(MPipeline.MPipelineServerOptions.class).setWorkDir(workDir.toAbsolutePath().toString());

        final Pipeline pipeline = Pipeline.create(options);
        MPipeline.apply(pipeline, Config.load(sourceConfigJson));

        final PipelineResult result = pipeline.run();
        try {
            // messages published to an existing subscription are retained until pulled,
            // so there is no race with the pipeline's subscriber startup
            final Publisher publisher = Publisher.newBuilder(SOURCE_TOPIC)
                    .setChannelProvider(channelProvider)
                    .setCredentialsProvider(NoCredentialsProvider.create())
                    .build();
            try {
                for(final String payload : List.of(
                        "{\"id\":\"a\",\"longvalue\":1}",
                        "{\"id\":\"b\",\"longvalue\":2}",
                        "{\"id\":\"c\",\"longvalue\":3}")) {
                    publisher.publish(PubsubMessage.newBuilder()
                            .setData(ByteString.copyFrom(payload, StandardCharsets.UTF_8))
                            .build()).get(30, TimeUnit.SECONDS);
                }
            } finally {
                publisher.shutdown();
                publisher.awaitTermination(30, TimeUnit.SECONDS);
            }

            // The debug sink windows the unbounded input (fixed 10s windows, triggering ~5s
            // after the first element) and writes avro files under workDir/outputs/debug/.
            // Poll until the three published records appear.
            final Map<String, Long> values = new HashMap<>();
            final long timeoutMillis = Instant.now().toEpochMilli() + 120_000L;
            while(values.size() < 3 && Instant.now().toEpochMilli() < timeoutMillis) {
                if(PipelineResult.State.FAILED.equals(result.getState())) {
                    result.waitUntilFinish(); // rethrows the pipeline failure cause
                    Assertions.fail("pipeline failed");
                }
                values.putAll(readDebugRecords(workDir.resolve("outputs").resolve("debug")));
                if(values.size() < 3) {
                    Thread.sleep(2000L);
                }
            }

            Assertions.assertEquals(3, values.size(), "expected 3 records, got: " + values);
            Assertions.assertEquals(1L, values.get("a"));
            Assertions.assertEquals(2L, values.get("b"));
            Assertions.assertEquals(3L, values.get("c"));
        } finally {
            result.cancel();
        }
    }

    /**
     * No-op credentials so the test does not require application default credentials on the
     * host (the emulator ignores authentication). Equivalent to Beam's test-only TestCredential.
     */
    private static class EmulatorCredentials extends com.google.auth.Credentials {
        @Override
        public String getAuthenticationType() {
            return "emulator";
        }
        @Override
        public Map<String, List<String>> getRequestMetadata(final java.net.URI uri) {
            return Map.of();
        }
        @Override
        public boolean hasRequestMetadata() {
            return false;
        }
        @Override
        public boolean hasRequestMetadataOnly() {
            return true;
        }
        @Override
        public void refresh() {
        }
    }

    /**
     * Reads the debug sink's windowed avro output files ({@code DebugRecord} records with a
     * {@code data} field, see {@code Debug.createOutputAvroSchema}), skipping the records-less
     * {@code schema.avro} marker file.
     */
    private static Map<String, Long> readDebugRecords(final Path outputDir) throws Exception {
        final Map<String, Long> values = new HashMap<>();
        if(!Files.isDirectory(outputDir)) {
            return values;
        }
        try(final var files = Files.list(outputDir)) {
            for(final Path path : files.filter(Files::isRegularFile).toList()) {
                if(path.getFileName().toString().equals("schema.avro")) {
                    continue;
                }
                final File file = path.toFile();
                try(final DataFileReader<GenericRecord> reader =
                            new DataFileReader<>(file, new GenericDatumReader<>())) {
                    while(reader.hasNext()) {
                        final GenericRecord debugRecord = reader.next();
                        final GenericRecord data = (GenericRecord) debugRecord.get("data");
                        values.put(data.get("id").toString(), (Long) data.get("longvalue"));
                    }
                } catch (final Exception e) {
                    // the file may still be being written; retry on the next poll
                }
            }
        }
        return values;
    }

}
