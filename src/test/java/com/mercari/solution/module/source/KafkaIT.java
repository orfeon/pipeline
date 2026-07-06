package com.mercari.solution.module.source;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Integration test (run via maven-failsafe: {@code mvn verify -DskipITs=false -Dit.test=KafkaIT})
 * for the kafka source module against an Apache Kafka broker (KRaft mode) managed by
 * Testcontainers.
 *
 * There is no kafka sink module in this codebase, so the test topics are seeded with a plain
 * {@link KafkaProducer} and the pipelines exercise the kafka source module only.
 *
 * The kafka source module requires streaming mode, so the pipeline options set
 * {@code streaming=true}. The {@code maxNumRecords} parameter turns the KafkaIO read into a
 * bounded read ({@code KafkaIO.Read#withMaxNumRecords}), which lets the DirectRunner pipeline
 * terminate. Beam KafkaIO defaults {@code auto.offset.reset} to {@code latest}, so the tests pass
 * {@code consumerConfig: {"auto.offset.reset": "earliest"}} to read the pre-produced records.
 */
@Testcontainers
public class KafkaIT {

    private static final double DELTA = 1e-9;

    private static final String TOPIC_SINGLE = "test-topic-single";
    private static final String TOPIC_LIST = "test-topic-list";

    // apache/kafka:3.9.x is incompatible with testcontainers 1.20.4 KafkaContainer
    // (fails at the storage-format step with "advertised.listeners cannot use the
    // nonroutable meta-address 0.0.0.0"), so pin 3.8.1
    @Container
    private static final KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("apache/kafka:3.8.1"));

    @BeforeAll
    static void setupTopics() throws Exception {
        // create the topics with a single partition so that the bounded read
        // (maxNumRecords) is deterministic, then seed them with three json records each
        try(final Admin admin = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(
                    new NewTopic(TOPIC_SINGLE, 1, (short) 1),
                    new NewTopic(TOPIC_LIST, 1, (short) 1)))
                    .all().get();
        }

        final List<String> records = List.of(
                "{\"id\":\"a\",\"longvalue\":1,\"doublevalue\":0.15,\"boolvalue\":true}",
                "{\"id\":\"b\",\"longvalue\":2,\"doublevalue\":1.15,\"boolvalue\":false}",
                "{\"id\":\"c\",\"longvalue\":3,\"doublevalue\":2.15,\"boolvalue\":true}");

        final Map<String, Object> producerConfig = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        try(final KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerConfig)) {
            for(final String topic : List.of(TOPIC_SINGLE, TOPIC_LIST)) {
                for(final String record : records) {
                    producer.send(new ProducerRecord<>(topic, record.getBytes(StandardCharsets.UTF_8))).get();
                }
            }
        }
    }

    /**
     * The kafka source module only supports streaming mode, so streaming=true is required to pass
     * the module validation. The read itself is made bounded via the maxNumRecords parameter.
     */
    private static TestPipeline createStreamingPipeline() {
        final DirectOptions options = PipelineOptionsFactory.as(DirectOptions.class);
        options.as(StreamingOptions.class).setStreaming(true);
        return TestPipeline.fromOptions(options).enableAbandonedNodeEnforcement(false);
    }

    private static String sourceConfigJson(final String topicParameter) {
        return """
                {
                  "sources": [
                    {
                      "name": "kafkaSource",
                      "module": "kafka",
                      "parameters": {
                        "bootstrapServers": "%s",
                        %s,
                        "format": "json",
                        "maxNumRecords": 3,
                        "consumerConfig": {
                          "auto.offset.reset": "earliest",
                          "group.id": "kafka-it"
                        }
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
                """.formatted(kafka.getBootstrapServers(), topicParameter);
    }

    private static void assertRecords(final MCollection output) {
        PAssert.that(output.getCollection()).satisfies(rows -> {
            int count = 0;
            for(final MElement row : rows) {
                switch (row.getAsString("id")) {
                    case "a" -> {
                        Assertions.assertEquals(1L, row.getAsLong("longvalue"));
                        Assertions.assertEquals(0.15D, row.getAsDouble("doublevalue"), DELTA);
                        Assertions.assertEquals(Boolean.TRUE, Optional.ofNullable(row.getPrimitiveValue("boolvalue")).orElse(null));
                    }
                    case "b" -> {
                        Assertions.assertEquals(2L, row.getAsLong("longvalue"));
                        Assertions.assertEquals(1.15D, row.getAsDouble("doublevalue"), DELTA);
                        Assertions.assertEquals(Boolean.FALSE, Optional.ofNullable(row.getPrimitiveValue("boolvalue")).orElse(null));
                    }
                    case "c" -> {
                        Assertions.assertEquals(3L, row.getAsLong("longvalue"));
                        Assertions.assertEquals(2.15D, row.getAsDouble("doublevalue"), DELTA);
                        Assertions.assertEquals(Boolean.TRUE, Optional.ofNullable(row.getPrimitiveValue("boolvalue")).orElse(null));
                    }
                    default -> Assertions.fail("unexpected id: " + row.getAsString("id"));
                }
                count++;
            }
            Assertions.assertEquals(3, count);
            return null;
        });
    }

    @Test
    public void testReadJsonFromTopic() throws Exception {
        final String configJson = sourceConfigJson("\"topic\": \"" + TOPIC_SINGLE + "\"");

        final TestPipeline pipeline = createStreamingPipeline();
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(configJson));
        assertRecords(outputs.get("kafkaSource"));

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void testReadJsonFromTopicsList() throws Exception {
        final String configJson = sourceConfigJson("\"topics\": [\"" + TOPIC_LIST + "\"]");

        final TestPipeline pipeline = createStreamingPipeline();
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(configJson));
        assertRecords(outputs.get("kafkaSource"));

        pipeline.run().waitUntilFinish();
    }

}
