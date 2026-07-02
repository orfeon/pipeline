package com.mercari.solution.util.schema.converter;

import com.google.api.services.bigquery.model.TableRow;
import com.mercari.solution.TestDatum;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;


public class TableRowToAvroConverterTest {

    @Test
    public void convertTest() {
        final GenericRecord originalRecord = TestDatum.generateRecord();
        final TableRow tableRow = AvroToTableRowConverter.convert(originalRecord);
        final GenericRecord record = TableRowToAvroConverter.convert(originalRecord.getSchema(), tableRow);

        testFlatField(originalRecord, record);

        final GenericRecord childRecord = (GenericRecord)record.get("recordField");
        testFlatField((GenericRecord)record.get("recordField"), childRecord);
        for(GenericRecord g : (List<GenericRecord>)childRecord.get("recordArrayField")) {
            testFlatField(originalRecord, g);
        }
        final GenericRecord grandchild = (GenericRecord)childRecord.get("recordField");
        testFlatField(originalRecord, grandchild);

        for(GenericRecord c : (List<GenericRecord>)record.get("recordArrayField")) {
            testFlatField(originalRecord, c);
            final GenericRecord gc = (GenericRecord)c.get("recordField");
            testFlatField(originalRecord, gc);
            for(GenericRecord g : (List<GenericRecord>)c.get("recordArrayField")) {
                testFlatField(originalRecord, g);
            }
        }
    }

    @Test
    public void convertTestNull() {
        final GenericRecord originalRecord = TestDatum.generateRecordNull();
        final TableRow tableRow = AvroToTableRowConverter.convert(originalRecord);
        final GenericRecord record = TableRowToAvroConverter.convert(originalRecord.getSchema(), tableRow);

        testFlatField(originalRecord, record);

        final GenericRecord childRecord = (GenericRecord)record.get("recordField");
        testFlatField((GenericRecord)record.get("recordField"), childRecord);
        for(GenericRecord g : (List<GenericRecord>)childRecord.get("recordArrayField")) {
            testFlatField(originalRecord, g);
        }
        final GenericRecord grandchild = (GenericRecord)childRecord.get("recordField");
        testFlatField(originalRecord, grandchild);

        for(GenericRecord c : (List<GenericRecord>)record.get("recordArrayField")) {
            testFlatField(originalRecord, c);
            final GenericRecord gc = (GenericRecord)c.get("recordField");
            testFlatField(originalRecord, gc);
            for(GenericRecord g : (List<GenericRecord>)c.get("recordArrayField")) {
                testFlatField(originalRecord, g);
            }
        }
    }

    private void testFlatField(final GenericRecord originalRecord, final GenericRecord record) {
        Assertions.assertEquals(originalRecord.get("booleanField"), record.get("booleanField"));
        Assertions.assertEquals(originalRecord.get("stringField"), record.get("stringField"));
        Assertions.assertEquals(originalRecord.get("bytesField"), record.get("bytesField"));
        Assertions.assertEquals(originalRecord.get("intField"), record.get("intField"));
        Assertions.assertEquals(originalRecord.get("longField"), record.get("longField"));
        Assertions.assertEquals(originalRecord.get("floatField"), record.get("floatField"));
        Assertions.assertEquals(originalRecord.get("doubleField"), record.get("doubleField"));
        Assertions.assertEquals(originalRecord.get("dateField"), record.get("dateField"));
        Assertions.assertEquals(originalRecord.get("timeField"), record.get("timeField"));
        Assertions.assertEquals(originalRecord.get("timestampField"), record.get("timestampField"));
        Assertions.assertEquals(originalRecord.get("decimalField"), record.get("decimalField"));
    }

}
