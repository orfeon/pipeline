package com.mercari.solution.util.schema.converter;

import com.google.api.services.bigquery.model.TableRow;
import com.mercari.solution.TestDatum;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Base64;
import java.util.List;

public class RowToTableRowConverterTest {

    private static final double DELTA = 1e-15;

    @Test
    public void test() {
        final Row row = TestDatum.generateRow();
        final TableRow tableRow = RowToTableRowConverter.convert(row);
        testFlatField(tableRow);
        final TableRow childRow = (TableRow)tableRow.get("recordField");
        testFlatField(childRow);
        for(TableRow g : (List<TableRow>)childRow.get("recordArrayField")) {
            testFlatField(g);
        }
        final TableRow grandchildRow = (TableRow)childRow.get("recordField");
        testFlatField(grandchildRow);

        for(TableRow c : (List<TableRow>)tableRow.get("recordArrayField")) {
            testFlatField(c);
            final TableRow gc = (TableRow)c.get("recordField");
            testFlatField(gc);
            for(TableRow g : (List<TableRow>)c.get("recordArrayField")) {
                testFlatField(g);
            }
        }
    }

    private void testFlatField(final TableRow tableRow) {
        Assertions.assertEquals(TestDatum.getBooleanFieldValue(), tableRow.get("booleanField"));
        Assertions.assertEquals(TestDatum.getStringFieldValue(), tableRow.get("stringField"));
        Assertions.assertEquals(TestDatum.getBytesFieldValue(), new String(
                Base64.getDecoder().decode(tableRow.get("bytesField").toString()), StandardCharsets.UTF_8));
        Assertions.assertEquals(TestDatum.getIntFieldValue().longValue(), tableRow.get("intField"));
        Assertions.assertEquals(TestDatum.getLongFieldValue().longValue(), tableRow.get("longField"));
        Assertions.assertEquals(TestDatum.getFloatFieldValue().doubleValue(), (Double)tableRow.get("floatField"), DELTA);
        Assertions.assertEquals(TestDatum.getDoubleFieldValue(), (Double)tableRow.get("doubleField"), DELTA);
        Assertions.assertEquals(TestDatum.getDateFieldValue().toEpochDay(), LocalDate.parse(tableRow.get("dateField").toString()).toEpochDay());
        Assertions.assertEquals(TestDatum.getTimeFieldValue().toSecondOfDay(), LocalTime.parse(tableRow.get("timeField").toString()).toSecondOfDay());
        Assertions.assertEquals(TestDatum.getTimestampFieldValue().getMillis(), Instant.parse(tableRow.get("timestampField").toString() + "Z").getMillis());
        Assertions.assertEquals(TestDatum.getDecimalFieldValue().toString(), tableRow.get("decimalField"));
    }

}
