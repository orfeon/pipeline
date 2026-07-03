package com.mercari.solution.util.schema.converter;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Value;
import com.google.protobuf.ByteString;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;

public class EntityToAvroConverterTest {

    private static final double DELTA = 1e-15;

    @Test
    public void testConvert() {
        final Key key = Key.newBuilder()
                .addPath(Key.PathElement.newBuilder().setKind("MyKind").setName("mykey").build())
                .build();
        final Schema schema = Schema.builder()
                .addField("booleanField", Schema.FieldType.BOOLEAN)
                .addField("stringField", Schema.FieldType.STRING)
                .addField("bytesField", Schema.FieldType.BYTES)
                .addField("intField", Schema.FieldType.INT32)
                .addField("longField", Schema.FieldType.INT64)
                .addField("floatField", Schema.FieldType.FLOAT)
                .addField("doubleField", Schema.FieldType.DOUBLE)
                .addField("dateField", CalciteUtils.DATE)
                .addField("timeField", CalciteUtils.TIME)
                .addField("timestampField", Schema.FieldType.DATETIME)
                .addField("decimalField", Schema.FieldType.DECIMAL)
                .build();

        final org.apache.avro.Schema avroSchema = RowToRecordConverter.convertSchema(schema);
        final org.apache.avro.Schema avroSchemaWithKey = EntityToAvroConverter.addKeyToSchema(avroSchema);

        byte[] bytes = "this is bytesField value".getBytes();

        final Entity entity = Entity.newBuilder()
                .setKey(key)
                .putProperties("booleanField", Value.newBuilder().setBooleanValue(true).build())
                .putProperties("stringField", Value.newBuilder().setStringValue("str").build())
                .putProperties("bytesField", Value.newBuilder().setBlobValue(ByteString.copyFrom(bytes)).build())
                .putProperties("intField", Value.newBuilder().setIntegerValue(1).build())
                .putProperties("longField", Value.newBuilder().setIntegerValue(1L).build())
                .putProperties("floatField", Value.newBuilder().setDoubleValue(1F).build())
                .putProperties("doubleField", Value.newBuilder().setDoubleValue(1D).build())
                .putProperties("timestampField", Value.newBuilder().setTimestampValue(DateTimeUtil.toProtoTimestamp(Instant.parse("2023-04-20T00:00:00Z"))).build())
                .putProperties("dateField", Value.newBuilder().setStringValue("2021-12-01").build())
                .putProperties("timeField", Value.newBuilder().setStringValue("18:51:32.123").build())
                .putProperties("decimalField", Value.newBuilder().setStringValue("1234.56789").build())
                .build();

        final GenericRecord convertedRecord = EntityToAvroConverter.convert(avroSchemaWithKey, entity);

        final ByteBuffer bytesFieldValue = (ByteBuffer)convertedRecord.get("bytesField");
        final byte[] bytesFieldValueBytes = new byte[bytesFieldValue.remaining()];
        bytesFieldValue.get(bytesFieldValueBytes);

        Assertions.assertEquals(true, convertedRecord.get("booleanField"));
        Assertions.assertEquals("str", convertedRecord.get("stringField"));
        Assertions.assertEquals(new String(bytes), new String(bytesFieldValueBytes));
        Assertions.assertEquals(1, convertedRecord.get("intField"));
        Assertions.assertEquals(1L, convertedRecord.get("longField"));
        Assertions.assertEquals(1F, convertedRecord.get("floatField"));
        Assertions.assertEquals(1D, convertedRecord.get("doubleField"));
        Assertions.assertEquals(Instant.parse("2023-04-20T00:00:00Z").getMillis() * 1000L, convertedRecord.get("timestampField"));
        Assertions.assertEquals(Long.valueOf(LocalDate.of(2021,12,1).toEpochDay()).intValue(), convertedRecord.get("dateField"));
        Assertions.assertEquals(LocalTime.of(18, 51,32, 123000000).toNanoOfDay() / 1000, convertedRecord.get("timeField"));

        final ByteBuffer bf = (ByteBuffer)convertedRecord.get("decimalField");
        final BigDecimal decimal = AvroSchemaUtil.getAsBigDecimal(avroSchemaWithKey.getField("decimalField").schema(), bf);
        Assertions.assertEquals(1234.56789D, decimal.doubleValue(), DELTA);

        final GenericRecord keyRecord = (GenericRecord) convertedRecord.get("__key__");
        Assertions.assertEquals("", keyRecord.get("namespace").toString());
        Assertions.assertEquals("", keyRecord.get("app").toString());
        Assertions.assertEquals("\"MyKind\", \"mykey\"", keyRecord.get("path").toString());
        Assertions.assertEquals("MyKind", keyRecord.get("kind").toString());
        Assertions.assertEquals("mykey", keyRecord.get("name").toString());
        Assertions.assertNull(keyRecord.get("id"));
    }

}
