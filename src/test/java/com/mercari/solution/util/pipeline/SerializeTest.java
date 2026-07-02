package com.mercari.solution.util.pipeline;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.ResourceUtil;
import com.mercari.solution.util.schema.ProtoSchemaUtil;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SerializeTest {

    private static List<Schema.Field> createFields() {
        return List.of(
                Schema.Field.of("stringField", Schema.FieldType.STRING),
                Schema.Field.of("longField", Schema.FieldType.INT64),
                Schema.Field.of("doubleField", Schema.FieldType.FLOAT64),
                Schema.Field.of("boolField", Schema.FieldType.BOOLEAN));
    }

    private static Map<String, Object> createValues() {
        final Map<String, Object> values = new HashMap<>();
        values.put("stringField", "text");
        values.put("longField", 10L);
        values.put("doubleField", 2.5D);
        values.put("boolField", true);
        return values;
    }

    @Test
    public void testJsonRoundTrip() throws Exception {
        final Schema schema = Schema.of(createFields());
        final Serialize serialize = Serialize.of(Serialize.Format.json, schema);
        serialize.setupSerialize();
        serialize.setupDeserialize();

        final Instant timestamp = Instant.parse("2025-01-01T00:00:00Z");
        final MElement element = MElement.of(createValues(), timestamp);

        final byte[] bytes = serialize.serialize(element);
        final JsonObject json = new Gson().fromJson(new String(bytes, java.nio.charset.StandardCharsets.UTF_8), JsonObject.class);
        Assertions.assertEquals("text", json.get("stringField").getAsString());
        Assertions.assertEquals(10L, json.get("longField").getAsLong());
        Assertions.assertEquals(2.5D, json.get("doubleField").getAsDouble());
        Assertions.assertTrue(json.get("boolField").getAsBoolean());

        final MElement deserialized = serialize.deserialize(bytes, timestamp);
        Assertions.assertEquals("text", deserialized.getAsString("stringField"));
        Assertions.assertEquals(10L, deserialized.getAsLong("longField"));
        Assertions.assertEquals(2.5D, deserialized.getAsDouble("doubleField"));
        Assertions.assertEquals(timestamp, deserialized.getTimestamp());

        // deserialize with explicit output type
        final MElement asElement = serialize.deserialize(bytes, timestamp, DataType.ELEMENT);
        Assertions.assertEquals("text", asElement.getAsString("stringField"));
        final MElement asAvro = serialize.deserialize(bytes, timestamp, DataType.AVRO);
        Assertions.assertEquals("text", asAvro.getAsString("stringField"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> serialize.deserialize(bytes, timestamp, DataType.ROW));
    }

    @Test
    public void testAvroRoundTrip() throws Exception {
        final Schema schema = Schema.of(createFields());
        final Serialize serializer = Serialize.of(Serialize.Format.avro, schema);
        serializer.setupSerialize();
        final Serialize deserializer = Serialize.of(Serialize.Format.avro, schema);
        deserializer.setupDeserialize();

        final Instant timestamp = Instant.parse("2025-01-01T00:00:00Z");
        final MElement element = MElement.of(createValues(), timestamp);

        final byte[] bytes = serializer.serialize(element);
        Assertions.assertTrue(bytes.length > 0);

        final MElement deserialized = deserializer.deserialize(bytes, timestamp);
        final GenericRecord record = (GenericRecord) deserialized.getValue();
        Assertions.assertEquals("text", record.get("stringField").toString());
        Assertions.assertEquals(10L, record.get("longField"));
        Assertions.assertEquals(2.5D, record.get("doubleField"));
        Assertions.assertEquals(true, record.get("boolField"));

        // repeated use reuses encoder/decoder instances
        final byte[] bytes2 = serializer.serialize(element);
        final MElement deserialized2 = deserializer.deserialize(bytes2, timestamp, DataType.AVRO);
        Assertions.assertEquals("text", ((GenericRecord) deserialized2.getValue()).get("stringField").toString());
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> deserializer.deserialize(bytes2, timestamp, DataType.ROW));
    }

    @Test
    public void testProtobufRoundTrip() throws Exception {
        final byte[] descBytes = ResourceUtil.getResourceFileAsBytes("schema/test.desc");
        final Map<String, Descriptors.Descriptor> descriptors = ProtoSchemaUtil.getDescriptors(descBytes);
        final Descriptors.Descriptor descriptor = descriptors.get("com.mercari.solution.entity.TestMessage");
        Assertions.assertNotNull(descriptor);

        final Schema schema = Schema.of(descriptor);

        // constructing a non-protobuf serializer registers the descriptor in the static cache,
        // allowing the protobuf serializer to resolve the message without a descriptor file
        Serialize.of(Serialize.Format.json, schema);

        final Serialize serialize = Serialize.of(Serialize.Format.protobuf, schema);
        serialize.setupSerialize();
        serialize.setupDeserialize();

        final Instant timestamp = Instant.parse("2025-01-01T00:00:00Z");
        final Map<String, Object> values = new HashMap<>();
        values.put("stringValue", "text");
        final MElement element = MElement.of(values, timestamp);

        final byte[] bytes = serialize.serialize(element);
        final DynamicMessage message = DynamicMessage.parseFrom(descriptor, bytes);
        Assertions.assertEquals("text", message.getField(descriptor.findFieldByName("stringValue")));

        final MElement deserialized = serialize.deserialize(bytes, timestamp);
        Assertions.assertInstanceOf(DynamicMessage.class, deserialized.getValue());
        final DynamicMessage deserializedMessage = (DynamicMessage) deserialized.getValue();
        Assertions.assertEquals("text", deserializedMessage.getField(descriptor.findFieldByName("stringValue")));

        final MElement deserializedTyped = serialize.deserialize(bytes, timestamp, DataType.ELEMENT);
        Assertions.assertInstanceOf(DynamicMessage.class, deserializedTyped.getValue());
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> serialize.deserialize(bytes, timestamp, DataType.ROW));
    }

    @Test
    public void testUnsupportedFormat() {
        final Schema schema = Schema.of(createFields());
        final Serialize serialize = Serialize.of(Serialize.Format.message, schema);
        serialize.setupSerialize();
        serialize.setupDeserialize();

        final Instant timestamp = Instant.parse("2025-01-01T00:00:00Z");
        final MElement element = MElement.of(createValues(), timestamp);
        Assertions.assertThrows(IllegalArgumentException.class, () -> serialize.serialize(element));
        Assertions.assertThrows(IllegalArgumentException.class, () -> serialize.deserialize(new byte[0], timestamp));
        Assertions.assertThrows(IllegalArgumentException.class, () -> serialize.deserialize(new byte[0], timestamp, DataType.ELEMENT));
    }

    @Test
    public void testGetOrLoadAvroSchema() {
        final Schema schema = Schema.of(createFields());
        final String avroJson = schema.getAvro().getJson();
        Assertions.assertNotNull(avroJson);

        final JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("json", avroJson);
        final Schema.AvroSchema avroSchema = Schema.AvroSchema.parse(jsonObject);

        final Map<String, org.apache.avro.Schema> avroSchemas = new HashMap<>();
        final Map<String, DatumWriter<GenericRecord>> writers = new HashMap<>();
        final org.apache.avro.Schema result = Serialize.getOrLoadAvroSchema(avroSchemas, writers, avroSchema);
        Assertions.assertNotNull(result);
        Assertions.assertNotNull(result.getField("stringField"));
        Assertions.assertNotNull(result.getField("longField"));
    }

    @Test
    public void testGetOrLoadDescriptor() {
        final byte[] descBytes = ResourceUtil.getResourceFileAsBytes("schema/test.desc");
        final Map<String, Descriptors.Descriptor> loaded = ProtoSchemaUtil.getDescriptors(descBytes);
        final Descriptors.Descriptor descriptor = loaded.get("com.mercari.solution.entity.TestMessage");

        final Map<String, Descriptors.Descriptor> cache = new HashMap<>();
        cache.put("com.mercari.solution.entity.TestMessage", descriptor);
        final Descriptors.Descriptor result = Serialize.getOrLoadDescriptor(
                cache, new HashMap<>(), "com.mercari.solution.entity.TestMessage", null);
        Assertions.assertSame(descriptor, result);
    }

}
