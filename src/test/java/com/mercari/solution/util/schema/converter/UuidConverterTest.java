package com.mercari.solution.util.schema.converter;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.Timestamp;
import com.google.datastore.v1.Entity;
import com.google.firestore.v1.Document;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.mercari.solution.module.Schema;
import com.mercari.solution.module.MElement;
import com.mercari.solution.util.cloud.google.SpannerUtil;
import com.mercari.solution.util.schema.BigtableSchemaUtil;
import com.mercari.solution.util.schema.CalciteSchemaUtil;
import com.mercari.solution.util.schema.StructSchemaUtil;
import org.apache.beam.sdk.values.Row;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ColumnType;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ModType;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.TypeCode;
import org.joda.time.Instant;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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

    @Test
    public void testSpannerNativeUuidConversion() {
        final UUID uuid = UUID.fromString(UUID_VALUE);
        final Struct struct = Struct.newBuilder()
                .set("id").to(uuid)
                .set("ids").toUuidArray(List.of(uuid))
                .build();

        final Schema elementSchema = Schema.of(struct.getType());
        assertEquals(Schema.Type.uuid, elementSchema.getField("id").getFieldType().getType());
        assertEquals(Schema.Type.uuid,
                elementSchema.getField("ids").getFieldType().getArrayValueType().getType());

        assertEquals(UUID_VALUE, StructSchemaUtil.getValue(struct, "id"));
        assertEquals(List.of(UUID_VALUE), StructSchemaUtil.getValue(struct, "ids"));
        assertEquals(UUID_VALUE, StructSchemaUtil.getCSVLineValue(struct, "id"));
        assertEquals(uuid, StructSchemaUtil.getStructValue(struct, "id").getUuid());
        assertEquals(List.of(uuid), StructSchemaUtil.getStructValue(struct, "ids").getUuidArray());

        final Struct copied = StructSchemaUtil.toBuilder(struct.getType(), struct).build();
        assertEquals(uuid, copied.getUuid("id"));
        assertEquals(List.of(uuid), copied.getUuidList("ids"));

        final Struct merged = StructSchemaUtil.merge(
                struct.getType(), struct, Map.of("id", UUID_VALUE, "ids", List.of(UUID_VALUE)));
        assertEquals(uuid, merged.getUuid("id"));
        assertEquals(List.of(uuid), merged.getUuidList("ids"));

        final Struct created = StructSchemaUtil.create(
                Type.struct(Type.StructField.of("id", Type.uuid())), Map.of("id", UUID_VALUE));
        assertEquals(uuid, created.getUuid("id"));

        final org.apache.beam.sdk.schemas.Schema rowSchema = StructToRowConverter.convertSchema(struct.getType());
        assertEquals(org.apache.beam.sdk.schemas.Schema.TypeName.STRING,
                rowSchema.getField("id").getType().getTypeName());
        assertEquals("UUID", rowSchema.getField("id").getOptions().getValue("spannerType"));
        assertEquals("UUID", rowSchema.getField("ids").getOptions().getValue("spannerType"));
        assertEquals(UUID_VALUE, StructToRowConverter.convert(rowSchema, struct).getString("id"));

        final Type roundTripType = RowToMutationConverter.convertSchema(rowSchema);
        assertEquals(Type.Code.UUID, roundTripType.getStructFields().get(0).getType().getCode());
        assertEquals(Type.Code.UUID,
                roundTripType.getStructFields().get(1).getType().getArrayElementType().getCode());

        final Row roundTripRow = StructToRowConverter.convert(rowSchema, struct);
        final Mutation roundTripMutation = RowToMutationConverter.convert(
                roundTripRow, "UuidTable", "INSERT");
        assertEquals(Type.Code.UUID, roundTripMutation.asMap().get("id").getType().getCode());
        assertEquals(Type.Code.UUID,
                roundTripMutation.asMap().get("ids").getType().getArrayElementType().getCode());
        assertEquals(List.of(uuid), roundTripMutation.asMap().get("ids").getUuidArray());

        final org.apache.avro.Schema avroSchema = StructToAvroConverter.convertSchema(struct.getType());
        assertEquals(org.apache.avro.LogicalTypes.uuid(),
                avroSchema.getField("id").schema().getTypes().get(1).getLogicalType());
        assertEquals(UUID_VALUE, StructToAvroConverter.convert(avroSchema, struct).get("id").toString());
        assertEquals(List.of(UUID_VALUE),
                StructToAvroConverter.convert(avroSchema, struct).get("ids"));
    }

    @Test
    public void testSpannerNativeUuidStructOutputs() throws Descriptors.DescriptorValidationException {
        final UUID uuid = UUID.fromString(UUID_VALUE);
        final Struct struct = Struct.newBuilder()
                .set("id").to(uuid)
                .set("ids").toUuidArray(List.of(uuid))
                .build();

        assertEquals(UUID_VALUE, StructSchemaUtil.getAsString(struct, "id"));
        assertEquals("[" + UUID_VALUE + "]", StructSchemaUtil.getAsString(struct, "ids"));
        assertEquals(ByteString.copyFromUtf8(UUID_VALUE), StructSchemaUtil.getAsByteString(struct, "id"));

        final Entity entity = StructToEntityConverter.convertBuilder(struct.getType(), struct, List.of()).build();
        assertEquals(UUID_VALUE, entity.getPropertiesOrThrow("id").getStringValue());
        assertEquals(UUID_VALUE, entity.getPropertiesOrThrow("ids")
                .getArrayValue().getValues(0).getStringValue());

        final Document document = StructToDocumentConverter.convert(struct).build();
        assertEquals(UUID_VALUE, document.getFieldsOrThrow("id").getStringValue());
        assertEquals(UUID_VALUE, document.getFieldsOrThrow("ids")
                .getArrayValue().getValues(0).getStringValue());

        final TableRow tableRow = StructToTableRowConverter.convert(struct);
        assertEquals(UUID_VALUE, tableRow.get("id"));
        assertEquals(List.of(UUID_VALUE), tableRow.get("ids"));
        assertEquals(UUID_VALUE, StructToMapConverter.convert(struct).get("id"));
        assertEquals(UUID_VALUE, StructToJsonConverter.convertObject(struct).get("id").getAsString());

        final Descriptors.Descriptor descriptor = createUuidProtoDescriptor();
        final DynamicMessage message = StructToProtoConverter.convert(descriptor, struct);
        assertEquals(UUID_VALUE, message.getField(descriptor.findFieldByName("id")));
        assertEquals(List.of(UUID_VALUE), message.getField(descriptor.findFieldByName("ids")));

        final List<com.google.bigtable.v2.Mutation> mutations = new ArrayList<>();
        StructToBigtableConverter.convert(
                struct.getType(),
                struct,
                "cf",
                BigtableSchemaUtil.Format.text,
                BigtableSchemaUtil.MutationOp.SET_CELL,
                Map.of(),
                0L).forEach(mutations::add);
        assertEquals(ByteString.copyFromUtf8(UUID_VALUE), mutations.get(0).getSetCell().getValue());
        assertEquals(ByteString.copyFromUtf8("[" + UUID_VALUE + "]"), mutations.get(1).getSetCell().getValue());
    }

    @Test
    public void testSpannerNativeUuidMutationAndDdl() {
        final Mutation mutation = ElementToSpannerMutationConverter.convert(
                SCHEMA,
                MElement.of(VALUES, Instant.EPOCH),
                "UuidTable",
                "INSERT",
                List.of("id"),
                List.of());

        assertEquals(Type.Code.UUID, mutation.asMap().get("id").getType().getCode());
        assertEquals(UUID.fromString(UUID_VALUE), mutation.asMap().get("id").getUuid());

        final org.apache.beam.sdk.schemas.Schema rowSchema = SCHEMA.getRowSchema();
        final Row row = ElementToRowConverter.convert(rowSchema, VALUES);
        final Mutation rowMutation = ElementToSpannerMutationConverter.convert(
                SCHEMA, MElement.of(row, Instant.EPOCH), "UuidTable", "INSERT", List.of("id"), List.of());
        assertEquals(Type.Code.UUID, rowMutation.asMap().get("id").getType().getCode());

        final org.apache.avro.Schema avroSchema = SCHEMA.getAvroSchema();
        final GenericRecord avro = ElementToAvroConverter.convert(avroSchema, VALUES);
        final Mutation avroMutation = ElementToSpannerMutationConverter.convert(
                SCHEMA, MElement.of(avro, Instant.EPOCH), "UuidTable", "INSERT", List.of("id"), List.of());
        assertEquals(Type.Code.UUID, avroMutation.asMap().get("id").getType().getCode());

        final Struct struct = Struct.newBuilder().set("id").to(UUID.fromString(UUID_VALUE)).build();
        final Mutation structMutation = ElementToSpannerMutationConverter.convert(
                SCHEMA, MElement.of(struct, Instant.EPOCH), "UuidTable", "INSERT", List.of("id"), List.of());
        assertEquals(Type.Code.UUID, structMutation.asMap().get("id").getType().getCode());

        assertEquals("CREATE TABLE UuidTable ( id UUID) PRIMARY KEY ( id )",
                SpannerUtil.buildCreateTableSQL(SCHEMA.getRowSchema(), "UuidTable", List.of("id"), null, false));

        final Schema arraySchema = Schema.builder()
                .withField("id", Schema.FieldType.UUID)
                .withField("ids", Schema.FieldType.array(Schema.FieldType.UUID))
                .build();
        assertEquals("CREATE TABLE UuidArrayTable ( id UUID,ids ARRAY<UUID>) PRIMARY KEY ( id )",
                SpannerUtil.buildCreateTableSQL(arraySchema.getRowSchema(), "UuidArrayTable", List.of("id"), null, false));
    }

    @Test
    public void testSpannerNativeUuidDeleteKey() {
        final UUID uuid = UUID.fromString(UUID_VALUE);
        final Struct struct = Struct.newBuilder().set("id").to(uuid).build();

        final Mutation mutation = StructToMutationConverter.convert(
                struct, "UuidTable", "DELETE", List.of("id"));

        assertEquals(List.of(uuid), mutation.getKeySet().getKeys().iterator().next().getParts());
    }

    @Test
    public void testSpannerChangeRecordUuidSchema() {
        final org.apache.avro.Schema changeRecordSchema = StructSchemaUtil.createDataChangeRecordAvroSchema();
        final org.apache.avro.Schema typeCodeSchema = changeRecordSchema
                .getField("rowType").schema().getElementType()
                .getField("Type").schema();
        assertEquals(true, typeCodeSchema.getEnumSymbols().contains("UUID"));

        final GenericRecord rowType = new GenericData.Record(
                changeRecordSchema.getField("rowType").schema().getElementType());
        rowType.put("name", "id");
        rowType.put("Type", new GenericData.EnumSymbol(typeCodeSchema, "UUID"));
        rowType.put("isPrimaryKey", true);
        rowType.put("ordinalPosition", 1L);

        final GenericRecord mod = new GenericData.Record(
                changeRecordSchema.getField("mods").schema().getElementType());
        mod.put("keysJson", "{\"id\":\"" + UUID_VALUE + "\"}");
        mod.put("oldValuesJson", null);
        mod.put("newValuesJson", "{}");

        final GenericRecord record = new GenericData.Record(changeRecordSchema);
        record.put("commitTimestamp", 0L);
        record.put("recordSequence", "1");
        record.put("tableName", "UuidTable");
        record.put("rowType", List.of(rowType));
        record.put("mods", List.of(mod));
        record.put("modType", new GenericData.EnumSymbol(changeRecordSchema.getField("modType").schema(), "INSERT"));

        final Mutation mutation = StructSchemaUtil.convertChangeRecordToMutations(MElement.of(record, Instant.EPOCH))
                .getFirst().getValue().getSpannerMutation();
        assertEquals(Type.Code.UUID, mutation.asMap().get("id").getType().getCode());
        assertEquals(UUID.fromString(UUID_VALUE), mutation.asMap().get("id").getUuid());
    }

    @Test
    public void testSpannerChangeRecordUuidDeleteKey() {
        final UUID uuid = UUID.fromString(UUID_VALUE);
        final Type tableType = Type.struct(Type.StructField.of("id", Type.uuid()));
        final DataChangeRecord record = new DataChangeRecord(
                "partition",
                Timestamp.ofTimeSecondsAndNanos(0L, 0),
                "transaction",
                true,
                "1",
                "UuidTable",
                List.of(new ColumnType("id", new TypeCode("UUID"), true, 1L)),
                List.of(new Mod("{\"id\":\"" + UUID_VALUE + "\"}", null, null)),
                ModType.DELETE,
                null,
                1L,
                1L,
                null,
                false,
                null);

        final Mutation mutation = StructSchemaUtil.convertToMutation(tableType, record).getFirst();

        assertEquals(List.of(uuid), mutation.getKeySet().getKeys().iterator().next().getParts());
    }

    @Test
    public void testAccumulateUuidChangeRecords() {
        final UUID uuid = UUID.fromString(UUID_VALUE);
        final org.apache.avro.Schema tableSchema = SCHEMA.getAvroSchema();
        final GenericRecord snapshot = ElementToAvroConverter.convert(tableSchema, VALUES);
        final GenericRecord insertRecord = createUuidChangeRecord("INSERT");

        final Mutation insert = StructSchemaUtil.accumulateChangeRecords(
                "UuidTable", tableSchema, snapshot, List.of(insertRecord));
        assertEquals(Type.Code.UUID, insert.asMap().get("id").getType().getCode());
        assertEquals(uuid, insert.asMap().get("id").getUuid());

        final Mutation delete = StructSchemaUtil.accumulateChangeRecords(
                "UuidTable", tableSchema, null, List.of(createUuidChangeRecord("DELETE")));
        assertEquals(List.of(uuid), delete.getKeySet().getKeys().iterator().next().getParts());
    }

    @Test
    public void testSpannerUuidInformationSchemaAndMutationArray() {
        final Struct informationSchema = Struct.newBuilder()
                .set("COLUMN_NAME").to("id")
                .set("SPANNER_TYPE").to("UUID")
                .set("IS_NULLABLE").to("NO")
                .build();
        final org.apache.beam.sdk.schemas.Schema schema = StructSchemaUtil.convertSchemaFromInformationSchema(
                List.of(informationSchema), null);
        assertEquals(org.apache.beam.sdk.schemas.Schema.TypeName.STRING,
                schema.getField("id").getType().getTypeName());
        assertEquals("UUID", schema.getField("id").getOptions().getValue("spannerType"));

        final Schema nullableArraySchema = Schema.builder()
                .withField("ids", Schema.FieldType.array(Schema.FieldType.UUID).withNullable(true))
                .build();
        final Row nullArrayRow = Row.withSchema(nullableArraySchema.getRowSchema())
                .withFieldValue("ids", null)
                .build();
        final Mutation nullArrayMutation = RowToMutationConverter.convert(
                nullArrayRow, "UuidArrayTable", "INSERT");
        assertEquals(Type.Code.ARRAY, nullArrayMutation.asMap().get("ids").getType().getCode());
        assertEquals(Type.Code.UUID,
                nullArrayMutation.asMap().get("ids").getType().getArrayElementType().getCode());
        assertEquals(true, nullArrayMutation.asMap().get("ids").isNull());
    }

    @Test
    public void testSpannerNullableScalarUuid() {
        final Schema nullableSchema = Schema.builder()
                .withField("optional_uuid", Schema.FieldType.UUID.withNullable(true))
                .build();
        final Map<String, Object> values = new HashMap<>();
        values.put("optional_uuid", null);

        final Mutation elementMutation = ElementToSpannerMutationConverter.convert(
                nullableSchema,
                MElement.of(values, Instant.EPOCH),
                "NullableUuidTable",
                "INSERT",
                List.of(),
                List.of());
        assertEquals(Type.Code.UUID, elementMutation.asMap().get("optional_uuid").getType().getCode());
        assertEquals(true, elementMutation.asMap().get("optional_uuid").isNull());

        final org.apache.avro.Schema avroSchema = nullableSchema.getAvroSchema();
        final GenericRecord avro = ElementToAvroConverter.convert(avroSchema, values);
        final Mutation avroMutation = ElementToSpannerMutationConverter.convert(
                nullableSchema,
                MElement.of(avro, Instant.EPOCH),
                "NullableUuidTable",
                "INSERT",
                List.of(),
                List.of());
        assertEquals(Type.Code.UUID, avroMutation.asMap().get("optional_uuid").getType().getCode());
        assertEquals(true, avroMutation.asMap().get("optional_uuid").isNull());

        final org.apache.beam.sdk.schemas.Schema rowSchema = nullableSchema.getRowSchema();
        final Row row = ElementToRowConverter.convert(rowSchema, values);
        final Mutation rowMutation = ElementToSpannerMutationConverter.convert(
                nullableSchema,
                MElement.of(row, Instant.EPOCH),
                "NullableUuidTable",
                "INSERT",
                List.of(),
                List.of());
        assertEquals(Type.Code.UUID, rowMutation.asMap().get("optional_uuid").getType().getCode());
        assertEquals(true, rowMutation.asMap().get("optional_uuid").isNull());

        final Struct struct = Struct.newBuilder().set("optional_uuid").to((UUID) null).build();
        final Mutation structMutation = ElementToSpannerMutationConverter.convert(
                nullableSchema,
                MElement.of(struct, Instant.EPOCH),
                "NullableUuidTable",
                "INSERT",
                List.of(),
                List.of());
        assertEquals(Type.Code.UUID, structMutation.asMap().get("optional_uuid").getType().getCode());
        assertEquals(true, structMutation.asMap().get("optional_uuid").isNull());

        assertEquals(null, MutationToAvroConverter.convert(avroSchema, elementMutation).get("optional_uuid"));
        assertEquals(null, MutationToRowConverter.convert(rowSchema, elementMutation).getValue("optional_uuid"));
    }

    private static GenericRecord createUuidChangeRecord(final String modType) {
        final org.apache.avro.Schema changeRecordSchema = StructSchemaUtil.createDataChangeRecordAvroSchema();
        final org.apache.avro.Schema rowTypeSchema = changeRecordSchema.getField("rowType").schema().getElementType();
        final org.apache.avro.Schema typeCodeSchema = rowTypeSchema.getField("Type").schema();

        final GenericRecord rowType = new GenericData.Record(rowTypeSchema);
        rowType.put("name", "id");
        rowType.put("Type", new GenericData.EnumSymbol(typeCodeSchema, "UUID"));
        rowType.put("isPrimaryKey", true);
        rowType.put("ordinalPosition", 1L);

        final GenericRecord mod = new GenericData.Record(changeRecordSchema.getField("mods").schema().getElementType());
        mod.put("keysJson", "{\"id\":\"" + UUID_VALUE + "\"}");
        mod.put("oldValuesJson", null);
        mod.put("newValuesJson", "{}");

        final GenericRecord record = new GenericData.Record(changeRecordSchema);
        record.put("partitionToken", "partition");
        record.put("commitTimestamp", 0L);
        record.put("serverTransactionId", "transaction");
        record.put("isLastRecordInTransactionInPartition", true);
        record.put("recordSequence", "1");
        record.put("tableName", "UuidTable");
        record.put("rowType", List.of(rowType));
        record.put("mods", List.of(mod));
        record.put("modType", new GenericData.EnumSymbol(changeRecordSchema.getField("modType").schema(), modType));
        record.put("valueCaptureType", new GenericData.EnumSymbol(
                changeRecordSchema.getField("valueCaptureType").schema(), "NEW_VALUES"));
        record.put("numberOfRecordsInTransaction", 1L);
        record.put("numberOfPartitionsInTransaction", 1L);
        record.put("metadata", null);
        return record;
    }

    private static Descriptors.Descriptor createUuidProtoDescriptor()
            throws Descriptors.DescriptorValidationException {

        final DescriptorProtos.DescriptorProto message = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("UuidMessage")
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName("id")
                        .setNumber(1)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL))
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName("ids")
                        .setNumber(2)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED))
                .build();
        final DescriptorProtos.FileDescriptorProto file = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("uuid.proto")
                .addMessageType(message)
                .build();
        return Descriptors.FileDescriptor.buildFrom(file, new Descriptors.FileDescriptor[0])
                .findMessageTypeByName("UuidMessage");
    }

}
