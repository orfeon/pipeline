package com.mercari.solution.util.schema.converter;

import com.google.firestore.v1.Document;
import com.google.protobuf.Timestamp;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;

public class AvroToDocumentConverterTest {

    private static Schema createTimestampSchema() {
        final Schema timestampMillisType = LogicalTypes.timestampMillis()
                .addToSchema(Schema.create(Schema.Type.LONG));
        final Schema timestampMicrosType = LogicalTypes.timestampMicros()
                .addToSchema(Schema.create(Schema.Type.LONG));
        return SchemaBuilder.record("TimestampRecord").fields()
                .name("timestampMillisField").type(timestampMillisType).noDefault()
                .name("timestampMicrosField").type(timestampMicrosType).noDefault()
                .endRecord();
    }

    @Test
    public void testTimestampMillisField() {
        final Instant instant = Instant.parse("2026-08-07T10:15:30.123Z");

        final Schema schema = createTimestampSchema();
        final GenericRecord record = new GenericData.Record(schema);
        record.put("timestampMillisField", instant.toEpochMilli());
        record.put("timestampMicrosField", 0L);

        final Document document = AvroToDocumentConverter.convertBuilder(schema, record).build();
        final Timestamp actual = document.getFieldsMap().get("timestampMillisField").getTimestampValue();

        Assert.assertEquals(instant.getEpochSecond(), actual.getSeconds());
        Assert.assertEquals(123_000_000, actual.getNanos());
    }

    @Test
    public void testTimestampMicrosField() {
        final Instant instant = Instant.parse("2026-08-07T10:15:30.123456Z");
        final long epochMicros = instant.getEpochSecond() * 1000_000L + instant.getNano() / 1000L;

        final Schema schema = createTimestampSchema();
        final GenericRecord record = new GenericData.Record(schema);
        record.put("timestampMillisField", 0L);
        record.put("timestampMicrosField", epochMicros);

        final Document document = AvroToDocumentConverter.convertBuilder(schema, record).build();
        final Timestamp actual = document.getFieldsMap().get("timestampMicrosField").getTimestampValue();

        Assert.assertEquals(instant.getEpochSecond(), actual.getSeconds());
        Assert.assertEquals(123_456_000, actual.getNanos());
    }

    @Test
    public void testTimestampMillisAndMicrosAgree() {
        final Instant instant = Instant.parse("2026-08-07T10:15:30.123Z");

        final Schema schema = createTimestampSchema();
        final GenericRecord record = new GenericData.Record(schema);
        record.put("timestampMillisField", instant.toEpochMilli());
        record.put("timestampMicrosField", instant.getEpochSecond() * 1000_000L + instant.getNano() / 1000L);

        final Document document = AvroToDocumentConverter.convertBuilder(schema, record).build();

        Assert.assertEquals(
                document.getFieldsMap().get("timestampMicrosField").getTimestampValue(),
                document.getFieldsMap().get("timestampMillisField").getTimestampValue());
    }
}
