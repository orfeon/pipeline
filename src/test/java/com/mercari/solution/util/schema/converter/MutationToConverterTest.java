package com.mercari.solution.util.schema.converter;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.mercari.solution.module.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

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

        final TableRow tableRow = MutationToTableRowConverter.convert(mutation);
        assertNull(tableRow.get("string_field"));
        assertNull(tableRow.get("bool_field"));
        assertNull(tableRow.get("uuid_field"));
    }

    @Test
    public void testUuidValues() {
        final UUID uuid = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        final Mutation insert = Mutation.newInsertBuilder("UuidTable")
                .set("id").to(uuid)
                .build();

        final TableRow insertRow = MutationToTableRowConverter.convert(insert);
        assertEquals(uuid.toString(), insertRow.get("id"));

        final Key key = Key.newBuilder().append(uuid).build();
        final Mutation delete = Mutation.delete("UuidTable", key);
        final TableRow deleteRow = MutationToTableRowConverter.convert(delete, List.of("id"));
        assertEquals(uuid.toString(), deleteRow.get("id"));
    }

}
