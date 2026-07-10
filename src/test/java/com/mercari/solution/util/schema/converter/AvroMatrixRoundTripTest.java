package com.mercari.solution.util.schema.converter;

import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.select.SelectFunction;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.LogicalType;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/**
 * Matrix fields survive the Avro round trip: {@code ElementToAvroConverter}
 * writes the marker props ({@code logicalType: matrix} + {@code shape}) on the
 * flat array schema, and {@code AvroToElementConverter} (the
 * {@code Schema.of(avroSchema)} path used by the storage source for
 * avro/parquet reads) restores the matrix FieldType from them — so a file
 * written by the storage sink, or a config-declared schema converted through
 * Avro, keeps its shape without redeclaration.
 */
public class AvroMatrixRoundTripTest {

    private static final Instant TIMESTAMP = Instant.parse("2025-05-01T00:00:00Z");
    private static final double DELTA = 1e-9;

    private static List<Schema.Field> matrixFields() {
        return List.of(
                Schema.Field.of("userId", Schema.FieldType.STRING),
                Schema.Field.of("vec", Schema.FieldType.array(Schema.FieldType.FLOAT64)),
                Schema.Field.of("mat", Schema.FieldType.matrix(
                        Schema.FieldType.FLOAT64, List.of(2, 2))));
    }

    @Test
    public void testSchemaRoundTrip() {
        final org.apache.avro.Schema avroSchema = ElementToAvroConverter.convertSchema(matrixFields());

        // The writer marks the flat array schema with the matrix props.
        final org.apache.avro.Schema matSchema = AvroSchemaUtil.unnestUnion(
                avroSchema.getField("mat").schema());
        Assertions.assertEquals(org.apache.avro.Schema.Type.ARRAY, matSchema.getType());
        Assertions.assertEquals(AvroSchemaUtil.LOGICAL_TYPE_MATRIX,
                matSchema.getProp(LogicalType.LOGICAL_TYPE_PROP));
        Assertions.assertEquals(List.of(2, 2), AvroSchemaUtil.getMatrixShape(matSchema));

        // The reader (storage source's schema derivation) restores the matrix type.
        final Schema restored = Schema.of(avroSchema);
        final Schema.FieldType matType = restored.getField("mat").getFieldType();
        Assertions.assertEquals(Schema.Type.matrix, matType.getType());
        Assertions.assertEquals(List.of(2, 2), matType.getShape());
        Assertions.assertEquals(Schema.Type.float64, matType.getMatrixValueType().getType());

        // A plain array field stays an array.
        Assertions.assertEquals(Schema.Type.array, restored.getField("vec").getFieldType().getType());
    }

    @Test
    public void testMalformedPropsDegradeToPlainArray() {
        final org.apache.avro.Schema element = org.apache.avro.Schema.create(
                org.apache.avro.Schema.Type.DOUBLE);

        final org.apache.avro.Schema noShape = org.apache.avro.Schema.createArray(element);
        noShape.addProp(LogicalType.LOGICAL_TYPE_PROP, AvroSchemaUtil.LOGICAL_TYPE_MATRIX);
        Assertions.assertNull(AvroSchemaUtil.getMatrixShape(noShape));

        final org.apache.avro.Schema badShape = org.apache.avro.Schema.createArray(element);
        badShape.addProp(LogicalType.LOGICAL_TYPE_PROP, AvroSchemaUtil.LOGICAL_TYPE_MATRIX);
        badShape.addProp(AvroSchemaUtil.PROP_MATRIX_SHAPE, "not a shape");
        Assertions.assertNull(AvroSchemaUtil.getMatrixShape(badShape));

        final org.apache.avro.Schema zeroDim = org.apache.avro.Schema.createArray(element);
        zeroDim.addProp(LogicalType.LOGICAL_TYPE_PROP, AvroSchemaUtil.LOGICAL_TYPE_MATRIX);
        zeroDim.addProp(AvroSchemaUtil.PROP_MATRIX_SHAPE, "[2, 0]");
        Assertions.assertNull(AvroSchemaUtil.getMatrixShape(zeroDim));
    }

    @Test
    public void testAvroBackedElementFeedsShapeAwareSelect() {
        // A GenericRecord read from an Avro file: the restored schema carries the
        // shape, the value stays a flat list — the select module's matrix functions
        // interpret it without any config beyond the field name.
        final org.apache.avro.Schema avroSchema = ElementToAvroConverter.convertSchema(matrixFields());
        final GenericRecord record = new GenericData.Record(avroSchema);
        record.put("userId", "u1");
        record.put("vec", List.of(5d, 10d));
        record.put("mat", List.of(2d, 1d, 1d, 3d));  // [[2, 1], [1, 3]] row-major

        final Schema restored = Schema.of(avroSchema);
        final MElement element = MElement.of(record, TIMESTAMP);

        final List<SelectFunction> selectFunctions = SelectFunction.of(
                new com.google.gson.Gson().fromJson("""
                        [{ "name": "solved", "func": "matrix_solve", "matrixField": "mat", "field": "vec" }]
                        """, com.google.gson.JsonArray.class), restored.getFields());
        selectFunctions.forEach(SelectFunction::setup);

        // 2x + y = 5, x + 3y = 10 -> [1, 3]
        final Map<String, Object> output = SelectFunction.apply(
                selectFunctions, element, TIMESTAMP);
        Assertions.assertInstanceOf(List.class, output.get("solved"));
        final List<?> solved = (List<?>) output.get("solved");
        Assertions.assertEquals(1d, ((Number) solved.get(0)).doubleValue(), DELTA);
        Assertions.assertEquals(3d, ((Number) solved.get(1)).doubleValue(), DELTA);
    }
}
