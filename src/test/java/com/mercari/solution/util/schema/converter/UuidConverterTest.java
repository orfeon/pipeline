package com.mercari.solution.util.schema.converter;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.datastore.v1.Entity;
import com.google.firestore.v1.Document;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.schema.CalciteSchemaUtil;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class UuidConverterTest {

    private static final String UUID_VALUE = "550e8400-e29b-41d4-a716-446655440000";
    private static final Schema SCHEMA = Schema.builder()
            .withField("id", Schema.FieldType.UUID)
            .build();
    private static final Map<String, Object> VALUES = Map.of("id", UUID_VALUE);

    @Test
    public void testTextInputs() {
        assertEquals(UUID_VALUE, CsvToElementConverter.convert(SCHEMA.getFields(), UUID_VALUE).get("id"));
        assertEquals(UUID_VALUE, JsonToElementConverter.convert(
                SCHEMA.getFields(), "{\"id\":\"" + UUID_VALUE + "\"}").get("id"));
    }

    @Test
    public void testRowConversion() {
        final org.apache.beam.sdk.schemas.Schema rowSchema = ElementToRowConverter.convertSchema(SCHEMA.getFields());
        assertEquals(org.apache.beam.sdk.schemas.Schema.TypeName.STRING,
                rowSchema.getField("id").getType().getTypeName());

        final Row row = ElementToRowConverter.convert(rowSchema, VALUES);
        assertEquals(UUID_VALUE, row.getString("id"));
    }

    @Test
    public void testTableRowConversion() {
        final TableSchema tableSchema = ElementToTableRowConverter.convertSchema(SCHEMA);
        assertEquals("STRING", tableSchema.getFields().getFirst().getType());

        final TableRow row = ElementToTableRowConverter.convert(SCHEMA, VALUES);
        assertEquals(UUID_VALUE, row.get("id"));
    }

    @Test
    public void testEntityAndDocumentConversion() {
        final Entity entity = ElementToEntityConverter
                .convertBuilder(SCHEMA, VALUES, List.of())
                .build();
        assertEquals(UUID_VALUE, entity.getPropertiesOrThrow("id").getStringValue());

        final Document document = ElementToDocumentConverter.convertBuilder(SCHEMA, VALUES).build();
        assertEquals(UUID_VALUE, document.getFieldsOrThrow("id").getStringValue());
    }

    @Test
    public void testCalciteConversion() {
        assertEquals(UUID_VALUE, CalciteSchemaUtil.convertSqlValue(Schema.FieldType.UUID, UUID_VALUE));
    }

}
