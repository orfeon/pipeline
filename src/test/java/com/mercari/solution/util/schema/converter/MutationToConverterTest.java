package com.mercari.solution.util.schema.converter;

import com.google.cloud.spanner.Mutation;
import com.mercari.solution.module.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.values.Row;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertNull;

public class MutationToConverterTest {

    @Test
    public void testTypedNullValues() {
        final Schema schema = Schema.builder()
                .withField("string_field", Schema.FieldType.STRING.withNullable(true))
                .withField("bool_field", Schema.FieldType.BOOLEAN.withNullable(true))
                .withField("uuid_field", Schema.FieldType.UUID.withNullable(true))
                .build();
        final Mutation mutation = Mutation.newInsertBuilder("TypedNullTable")
                .set("string_field").to((String) null)
                .set("bool_field").to((Boolean) null)
                .set("uuid_field").to((UUID) null)
                .build();

        final GenericRecord record = MutationToAvroConverter.convert(schema.getAvroSchema(), mutation);
        assertNull(record.get("string_field"));
        assertNull(record.get("bool_field"));
        assertNull(record.get("uuid_field"));

        final Row row = MutationToRowConverter.convert(schema.getRowSchema(), mutation);
        assertNull(row.getValue("string_field"));
        assertNull(row.getValue("bool_field"));
        assertNull(row.getValue("uuid_field"));
    }

}
