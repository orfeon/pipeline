package com.mercari.solution.util.schema.converter;

import com.google.api.services.bigquery.model.TableRow;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A decimal value of a map element is a {@link BigDecimal} (the json and fwf decoders), or its text
 * once the element has passed the map coder. The converters out of a map element accept both.
 */
public class ElementDecimalConversionTest {

    private static final Schema SCHEMA = Schema.builder()
            .withField("id", Schema.FieldType.STRING)
            .withField("amount", Schema.FieldType.decimal(38, 9))
            .build();

    private static Map<String, Object> values(final Object amount) {
        final Map<String, Object> values = new HashMap<>();
        values.put("id", "a");
        values.put("amount", amount);
        return values;
    }

    @Test
    public void testToAvro() {
        final org.apache.avro.Schema avroSchema = SCHEMA.getAvroSchema();
        for(final Object amount : List.of(new BigDecimal("12.3"), "12.3", " 12.30 ", 12.3d)) {
            final GenericRecord record = ElementToAvroConverter.convert(avroSchema, values(amount));
            Assertions.assertEquals(0, new BigDecimal("12.3").compareTo(AvroSchemaUtil.getAsBigDecimal(record, "amount")), String.valueOf(amount));
            // the unscaled value at the scale of the schema (9)
            Assertions.assertEquals(new BigInteger("12300000000"), new BigInteger(((ByteBuffer) record.get("amount")).array()));
        }
        final GenericRecord negative = ElementToAvroConverter.convert(avroSchema, values(new BigDecimal("-0.5")));
        Assertions.assertEquals(0, new BigDecimal("-0.5").compareTo(AvroSchemaUtil.getAsBigDecimal(negative, "amount")));
        final GenericRecord integer = ElementToAvroConverter.convert(avroSchema, values(7L));
        Assertions.assertEquals(0, new BigDecimal("7").compareTo(AvroSchemaUtil.getAsBigDecimal(integer, "amount")));

        // bytes that are already encoded pass as they are
        final ByteBuffer encoded = ByteBuffer.wrap(new BigInteger("12300000000").toByteArray());
        final GenericRecord bytes = ElementToAvroConverter.convert(avroSchema, values(encoded));
        Assertions.assertEquals(0, new BigDecimal("12.3").compareTo(AvroSchemaUtil.getAsBigDecimal(bytes, "amount")));
        // also as a byte[]: an avro generic record holds bytes as a ByteBuffer (a byte[] can not be encoded)
        final GenericRecord array = ElementToAvroConverter.convert(avroSchema, values(new BigInteger("12300000000").toByteArray()));
        Assertions.assertInstanceOf(ByteBuffer.class, array.get("amount"));
        Assertions.assertEquals(0, new BigDecimal("12.3").compareTo(AvroSchemaUtil.getAsBigDecimal(array, "amount")));

        Assertions.assertNull(ElementToAvroConverter.convert(avroSchema, values(null)).get("amount"));
    }

    @Test
    public void testToRow() {
        final org.apache.beam.sdk.schemas.Schema.FieldType decimal = org.apache.beam.sdk.schemas.Schema.FieldType.DECIMAL;
        Assertions.assertEquals(new BigDecimal("12.3"), RowSchemaUtil.convertPrimitive(decimal, new BigDecimal("12.3")));
        Assertions.assertEquals(new BigDecimal("12.3"), RowSchemaUtil.convertPrimitive(decimal, "12.3"));
        Assertions.assertEquals(new BigDecimal("12.3"), RowSchemaUtil.convertPrimitive(decimal, 12.3d));
        Assertions.assertEquals(new BigDecimal("7"), RowSchemaUtil.convertPrimitive(decimal, 7L));
        // avro-style bytes: the unscaled value at scale 9
        Assertions.assertEquals(0, new BigDecimal("12.3").compareTo((BigDecimal) RowSchemaUtil.convertPrimitive(
                decimal, ByteBuffer.wrap(new BigInteger("12300000000").toByteArray()))));
        // only the remaining bytes of a buffer are the value (a slice of a larger array, a read-only buffer)
        final byte[] unscaled = new BigInteger("12300000000").toByteArray();
        final byte[] padded = new byte[unscaled.length + 3];
        System.arraycopy(unscaled, 0, padded, 1, unscaled.length);
        padded[0] = 0x7f;
        Assertions.assertEquals(0, new BigDecimal("12.3").compareTo((BigDecimal) RowSchemaUtil.convertPrimitive(
                decimal, ByteBuffer.wrap(padded, 1, unscaled.length))));
        Assertions.assertEquals(0, new BigDecimal("12.3").compareTo((BigDecimal) RowSchemaUtil.convertPrimitive(
                decimal, ByteBuffer.wrap(unscaled).asReadOnlyBuffer())));
        Assertions.assertNull(RowSchemaUtil.convertPrimitive(decimal, null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> RowSchemaUtil.convertPrimitive(decimal, true));
    }

    @Test
    public void testToRowArrayAndBytes() {
        // a repeated decimal (a fwf field with repeat): element by element, empty elements stay null
        final org.apache.beam.sdk.schemas.Schema.FieldType decimals = org.apache.beam.sdk.schemas.Schema.FieldType
                .array(org.apache.beam.sdk.schemas.Schema.FieldType.DECIMAL.withNullable(true));
        Assertions.assertEquals(
                Arrays.asList(new BigDecimal("12.3"), null, new BigDecimal("4.5")),
                RowSchemaUtil.convertPrimitive(decimals, Arrays.asList(new BigDecimal("12.3"), null, "4.5")));

        // the bytes of a map element are a ByteBuffer: its content, not the text of the buffer object
        final org.apache.beam.sdk.schemas.Schema.FieldType bytes = org.apache.beam.sdk.schemas.Schema.FieldType.BYTES;
        Assertions.assertArrayEquals(new byte[] {1, 2, 3},
                (byte[]) RowSchemaUtil.convertPrimitive(bytes, ByteBuffer.wrap(new byte[] {1, 2, 3})));
    }

    @Test
    public void testAvroDecimalKeepsItsPrecisionAndScale() {
        // a decimal(10,2) read from avro: its bytes are the unscaled value at scale 2, not at the default 9
        final org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse("""
                { "type": "record", "name": "root", "fields": [
                    { "name": "price", "type": { "type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2 } },
                    { "name": "total", "type": [ "null", { "type": "bytes", "logicalType": "decimal", "precision": 38, "scale": 9 } ] } ] }
                """);
        final List<Schema.Field> fields = AvroToElementConverter.convertFields(avroSchema.getFields());
        Assertions.assertEquals(Schema.Type.decimal, fields.get(0).getFieldType().getType());
        Assertions.assertEquals(2, fields.get(0).getFieldType().getScale());
        Assertions.assertEquals(10, fields.get(0).getFieldType().getPrecision());
        Assertions.assertEquals(9, fields.get(1).getFieldType().getScale());

        // and back: the schema written for the field keeps them, so passed-through bytes stay right
        final org.apache.avro.Schema written = ElementToAvroConverter.convertSchema(fields);
        final org.apache.avro.LogicalTypes.Decimal price = (org.apache.avro.LogicalTypes.Decimal)
                AvroSchemaUtil.unnestUnion(written.getField("price").schema()).getLogicalType();
        Assertions.assertEquals(2, price.getScale());
        Assertions.assertEquals(10, price.getPrecision());

        // a number written into it is encoded at that scale
        final Map<String, Object> values = new HashMap<>();
        values.put("price", new BigDecimal("12.34"));
        final GenericRecord record = ElementToAvroConverter.convert(written, values);
        Assertions.assertEquals(new BigInteger("1234"), new BigInteger(((ByteBuffer) record.get("price")).array()));
        Assertions.assertEquals(0, new BigDecimal("12.34").compareTo(AvroSchemaUtil.getAsBigDecimal(record, "price")));
    }

    @Test
    public void testToTableRow() {
        for(final Object amount : List.of(new BigDecimal("12.3"), "12.3", 12.3d)) {
            final TableRow row = ElementToTableRowConverter.convert(SCHEMA, values(amount));
            Assertions.assertEquals("12.3", row.get("amount"), String.valueOf(amount));
        }
    }

}
