package com.mercari.solution.util.schema.converter;

import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.time.Instant;

public class ElementToAvroConverterTest {

    @Test
    public void testConvert() {

        final Schema schema = Schema.builder()
                .withField("stringField", Schema.FieldType.STRING)
                .withField("timestampField", Schema.FieldType.TIMESTAMP)
                .build();

        final MElement element = MElement.builder()
                .withString("stringField", "stringValue")
                .withTimestamp("timestampField", Instant.now())
                .build();

        final GenericRecord record = ElementToAvroConverter.convert(schema, element);
        System.out.println(record.getSchema());
        System.out.println(record);

    }

    /**
     * BigQuery file loads: the writer schema comes from the destination TableSchema, where a map is a repeated
     * {key, value} record, while the element carries an Avro map. Both an AVRO element (its record passed through)
     * and an ELEMENT element must serialize against that writer schema, including a map nested in a record and in
     * an array of records.
     */
    @Test
    public void testConvertWriteRequestRemapsMapsToWriterSchema() throws java.io.IOException {
        final Schema child = Schema.builder()
                .withField("name", Schema.FieldType.STRING)
                .withField("shares", Schema.FieldType.map(Schema.FieldType.FLOAT64))
                .build();
        final Schema schema = Schema.builder()
                .withField("id", Schema.FieldType.STRING)
                .withField("dist", Schema.FieldType.map(Schema.FieldType.FLOAT64).withNullable(true))
                .withField("nested", Schema.FieldType.element(child))
                .withField("rows", Schema.FieldType.array(Schema.FieldType.element(child)))
                .withField("count", Schema.FieldType.INT64)
                .withType(com.mercari.solution.module.DataType.AVRO)
                .build();
        final java.util.Map<String, Object> values = new java.util.HashMap<>();
        values.put("id", "a");
        values.put("dist", java.util.Map.of("S", 0.75, "H", 0.25));
        values.put("nested", java.util.Map.of("name", "n1", "shares", java.util.Map.of("x", 1.0)));
        values.put("rows", java.util.List.of(java.util.Map.of("name", "r1", "shares", java.util.Map.of("y", 2.0))));
        values.put("count", 3L);

        // the writer schema the bigquery sink derives from the table
        final org.apache.avro.Schema writer = TableRowToAvroConverter.convertSchema(ElementToTableRowConverter.convertSchema(schema));
        org.junit.jupiter.api.Assertions.assertEquals(org.apache.avro.Schema.Type.ARRAY,
                com.mercari.solution.util.schema.AvroSchemaUtil.unnestUnion(writer.getField("dist").schema()).getType());

        final MElement avroElement = MElement.of(schema, values, 0L).convert(schema);
        org.junit.jupiter.api.Assertions.assertEquals(com.mercari.solution.module.DataType.AVRO, avroElement.getType());
        final MElement plainElement = MElement.of(values, 0L);
        for (final MElement element : java.util.List.of(avroElement, plainElement)) {
            final GenericRecord record = ElementToAvroConverter.convert(new org.apache.beam.sdk.io.gcp.bigquery.AvroWriteRequest<>(element, writer));
            org.junit.jupiter.api.Assertions.assertSame(writer, record.getSchema());
            // it serializes against the writer schema (the datum writer resolves unions by value class)
            final java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
            try (final org.apache.avro.file.DataFileWriter<GenericRecord> w = new org.apache.avro.file.DataFileWriter<>(new org.apache.avro.generic.GenericDatumWriter<>(writer))) {
                w.create(writer, bytes);
                w.append(record);
            }
            try (final org.apache.avro.file.DataFileReader<GenericRecord> r = new org.apache.avro.file.DataFileReader<>(
                    new org.apache.avro.file.SeekableByteArrayInput(bytes.toByteArray()), new org.apache.avro.generic.GenericDatumReader<>(writer))) {
                final GenericRecord read = r.next();
                final java.util.Map<String, Double> dist = new java.util.HashMap<>();
                for (final Object entry : (java.util.List<?>) read.get("dist")) {
                    final GenericRecord kv = (GenericRecord) entry;
                    dist.put(kv.get("key").toString(), (Double) kv.get("value"));
                }
                org.junit.jupiter.api.Assertions.assertEquals(java.util.Map.of("S", 0.75, "H", 0.25), dist, element.getType().name());
                org.junit.jupiter.api.Assertions.assertEquals("x", ((GenericRecord) ((java.util.List<?>) ((GenericRecord) read.get("nested")).get("shares")).get(0)).get("key").toString());
                org.junit.jupiter.api.Assertions.assertEquals("y", ((GenericRecord) ((java.util.List<?>) ((GenericRecord) ((java.util.List<?>) read.get("rows")).get(0)).get("shares")).get(0)).get("key").toString());
                org.junit.jupiter.api.Assertions.assertEquals(3L, read.get("count"));
                org.junit.jupiter.api.Assertions.assertFalse(r.hasNext());
            }
        }
        // a record already on the writer schema is passed through untouched
        final GenericRecord onWriter = ElementToAvroConverter.convert(writer, plainElement);
        org.junit.jupiter.api.Assertions.assertSame(onWriter, com.mercari.solution.util.schema.AvroSchemaUtil.toWriterSchema(writer, onWriter));
        // no map anywhere: an AVRO element's record is passed through even though the writer schema differs by name
        final Schema flat = Schema.builder()
                .withField("id", Schema.FieldType.STRING)
                .withField("rows", Schema.FieldType.array(Schema.FieldType.element(Schema.builder().withField("name", Schema.FieldType.STRING).build())))
                .withType(com.mercari.solution.module.DataType.AVRO)
                .build();
        final java.util.Map<String, Object> flatValues = new java.util.HashMap<>();
        flatValues.put("id", "b");
        flatValues.put("rows", java.util.List.of(java.util.Map.of("name", "r1")));
        final MElement flatElement = MElement.of(flat, flatValues, 0L).convert(flat);
        final org.apache.avro.Schema flatWriter = TableRowToAvroConverter.convertSchema(ElementToTableRowConverter.convertSchema(flat));
        org.junit.jupiter.api.Assertions.assertNotEquals(flatWriter, ((GenericRecord) flatElement.getValue()).getSchema());
        org.junit.jupiter.api.Assertions.assertSame(flatElement.getValue(),
                ElementToAvroConverter.convert(new org.apache.beam.sdk.io.gcp.bigquery.AvroWriteRequest<>(flatElement, flatWriter)));
    }

}
