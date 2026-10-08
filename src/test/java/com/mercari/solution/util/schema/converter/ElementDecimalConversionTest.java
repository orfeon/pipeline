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
        Assertions.assertNull(RowSchemaUtil.convertPrimitive(decimal, null));
    }

    @Test
    public void testToTableRow() {
        for(final Object amount : List.of(new BigDecimal("12.3"), "12.3", 12.3d)) {
            final TableRow row = ElementToTableRowConverter.convert(SCHEMA, values(amount));
            Assertions.assertEquals("12.3", row.get("amount"), String.valueOf(amount));
        }
    }

}
