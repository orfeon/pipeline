package com.mercari.solution.util.schema.converter;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class AvroToArrowConverterTest {

    private static final String AVRO_SCHEMA_JSON = """
            {
              "type": "record",
              "name": "root",
              "fields": [
                { "name": "str", "type": ["null", "string"] },
                { "name": "ts", "type": { "type": "long", "logicalType": "timestamp-micros" } },
                { "name": "d", "type": { "type": "int", "logicalType": "date" } },
                { "name": "dec", "type": ["null", { "type": "bytes", "logicalType": "decimal", "precision": 38, "scale": 9 }] },
                { "name": "nested", "type": ["null", {
                    "type": "record", "name": "nested_",
                    "fields": [
                      { "name": "s", "type": "string" },
                      { "name": "n", "type": "long" }
                    ] }] },
                { "name": "arr", "type": { "type": "array", "items": "long" } },
                { "name": "m", "type": ["null", { "type": "map", "values": "long" }] }
              ]
            }
            """;

    @Test
    public void testConvertSchema() {
        final Schema avroSchema = new Schema.Parser().parse(AVRO_SCHEMA_JSON);
        final org.apache.arrow.vector.types.pojo.Schema arrowSchema = AvroToArrowConverter.convertSchema(avroSchema);

        Assertions.assertEquals(ArrowType.Utf8.INSTANCE, arrowSchema.findField("str").getType());
        Assertions.assertTrue(arrowSchema.findField("str").isNullable());
        Assertions.assertEquals(
                new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MICROSECOND, "UTC"),
                arrowSchema.findField("ts").getType());
        Assertions.assertFalse(arrowSchema.findField("ts").isNullable());
        Assertions.assertEquals(
                new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY),
                arrowSchema.findField("d").getType());
        Assertions.assertEquals(new ArrowType.Decimal(38, 9, 128), arrowSchema.findField("dec").getType());
        Assertions.assertEquals(ArrowType.Struct.INSTANCE, arrowSchema.findField("nested").getType());
        Assertions.assertEquals(2, arrowSchema.findField("nested").getChildren().size());
        Assertions.assertEquals(ArrowType.List.INSTANCE, arrowSchema.findField("arr").getType());
        Assertions.assertEquals(new ArrowType.Map(false), arrowSchema.findField("m").getType());
    }

    @Test
    public void testSetRecord() {
        final Schema avroSchema = new Schema.Parser().parse(AVRO_SCHEMA_JSON);
        final Schema nestedSchema = avroSchema.getField("nested").schema().getTypes().get(1);
        final org.apache.arrow.vector.types.pojo.Schema arrowSchema = AvroToArrowConverter.convertSchema(avroSchema);

        final GenericRecord nested = new GenericRecordBuilder(nestedSchema)
                .set("s", "inner")
                .set("n", 42L)
                .build();
        final BigDecimal decimal = new BigDecimal("1234.567890000").setScale(9);
        final GenericRecord record0 = new GenericRecordBuilder(avroSchema)
                .set("str", "hello")
                .set("ts", 1700000000_000000L)
                .set("d", 20000)
                .set("dec", ByteBuffer.wrap(decimal.unscaledValue().toByteArray()))
                .set("nested", nested)
                .set("arr", List.of(1L, 2L, 3L))
                .set("m", Map.of("k", 10L))
                .build();
        final GenericRecord record1 = new GenericRecordBuilder(avroSchema)
                .set("str", null)
                .set("ts", 0L)
                .set("d", 0)
                .set("dec", null)
                .set("nested", null)
                .set("arr", List.of())
                .set("m", null)
                .build();

        try(final BufferAllocator allocator = new RootAllocator();
            final VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {

            root.allocateNew();
            AvroToArrowConverter.setRecord(root, 0, record0);
            AvroToArrowConverter.setRecord(root, 1, record1);
            root.setRowCount(2);

            final VarCharVector str = (VarCharVector) root.getVector("str");
            Assertions.assertEquals("hello", new String(str.get(0), StandardCharsets.UTF_8));
            Assertions.assertTrue(str.isNull(1));

            Assertions.assertEquals(1700000000_000000L, ((TimeStampMicroTZVector) root.getVector("ts")).get(0));
            Assertions.assertEquals(20000, ((DateDayVector) root.getVector("d")).get(0));

            final DecimalVector dec = (DecimalVector) root.getVector("dec");
            Assertions.assertEquals(decimal, dec.getObject(0));
            Assertions.assertTrue(dec.isNull(1));

            final StructVector nestedVector = (StructVector) root.getVector("nested");
            Assertions.assertEquals("inner", nestedVector.getChild("s").getObject(0).toString());
            Assertions.assertEquals(42L, ((BigIntVector) nestedVector.getChild("n")).get(0));
            Assertions.assertTrue(nestedVector.isNull(1));

            final ListVector arr = (ListVector) root.getVector("arr");
            Assertions.assertEquals(List.of(1L, 2L, 3L), arr.getObject(0));
            Assertions.assertEquals(0, ((List<?>) arr.getObject(1)).size());

            final MapVector m = (MapVector) root.getVector("m");
            final List<?> entries = (List<?>) m.getObject(0);
            Assertions.assertEquals(1, entries.size());
            final Map<?, ?> entry = (Map<?, ?>) entries.getFirst();
            Assertions.assertEquals("k", entry.get(MapVector.KEY_NAME).toString());
            Assertions.assertEquals(10L, entry.get(MapVector.VALUE_NAME));
            Assertions.assertTrue(m.isNull(1));
        }
    }

    @Test
    public void testEnumValue() {
        final Schema schema = new Schema.Parser().parse("""
                {
                  "type": "record", "name": "root",
                  "fields": [
                    { "name": "e", "type": { "type": "enum", "name": "color", "symbols": ["RED", "BLUE"] } }
                  ]
                }
                """);
        final org.apache.arrow.vector.types.pojo.Schema arrowSchema = AvroToArrowConverter.convertSchema(schema);
        Assertions.assertEquals(ArrowType.Utf8.INSTANCE, arrowSchema.findField("e").getType());

        final GenericRecord record = new GenericRecordBuilder(schema)
                .set("e", new GenericData.EnumSymbol(schema.getField("e").schema(), "BLUE"))
                .build();
        try(final BufferAllocator allocator = new RootAllocator();
            final VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
            root.allocateNew();
            AvroToArrowConverter.setRecord(root, 0, record);
            root.setRowCount(1);
            Assertions.assertEquals("BLUE",
                    new String(((VarCharVector) root.getVector("e")).get(0), StandardCharsets.UTF_8));
        }
    }

}
