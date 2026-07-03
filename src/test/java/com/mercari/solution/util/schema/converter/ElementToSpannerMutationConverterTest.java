package com.mercari.solution.util.schema.converter;

import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.DateTimeUtil;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElementToSpannerMutationConverterTest {

    private static Schema createSchema() {
        return Schema.builder()
                .withField("id", Schema.FieldType.STRING)
                .withField("timestampField", Schema.FieldType.TIMESTAMP)
                .withField("dateField", Schema.FieldType.DATE)
                .withField("timeField", Schema.FieldType.TIME)
                .build();
    }

    @Test
    public void testConvertElementWithTimestampAndDate() {
        final Schema schema = createSchema();

        final long epochMicros = DateTimeUtil.toEpochMicroSecond("2024-10-10T01:23:45.678901Z");
        final Map<String, Object> values = new HashMap<>();
        values.put("id", "a");
        values.put("timestampField", epochMicros);
        values.put("dateField", (int) LocalDate.of(2024, 3, 1).toEpochDay());
        values.put("timeField", LocalTime.of(12, 34, 56).toNanoOfDay() / 1000L);

        final MElement element = MElement.of(values, Instant.parse("2024-10-10T00:00:00Z"));
        final Mutation mutation = ElementToSpannerMutationConverter
                .convert(schema, element, "mytable", null, null, null);

        final Map<String, Value> mutationValues = mutation.asMap();
        Assertions.assertEquals(Value.string("a"), mutationValues.get("id"));
        Assertions.assertEquals(
                Value.timestamp(Timestamp.ofTimeMicroseconds(epochMicros)),
                mutationValues.get("timestampField"));
        Assertions.assertEquals(
                Value.date(Date.fromYearMonthDay(2024, 3, 1)),
                mutationValues.get("dateField"));
        Assertions.assertEquals(Value.string("12:34:56"), mutationValues.get("timeField"));
    }

    @Test
    public void testConvertElementWithNullTimestampAndDate() {
        final Schema schema = createSchema();

        final Map<String, Object> values = new HashMap<>();
        values.put("id", "a");
        values.put("timestampField", null);
        values.put("dateField", null);
        values.put("timeField", null);

        final MElement element = MElement.of(values, Instant.parse("2024-10-10T00:00:00Z"));
        final Mutation mutation = ElementToSpannerMutationConverter
                .convert(schema, element, "mytable", null, null, null);

        final Map<String, Value> mutationValues = mutation.asMap();
        Assertions.assertTrue(mutationValues.containsKey("timestampField"));
        Assertions.assertTrue(mutationValues.get("timestampField").isNull());
        Assertions.assertTrue(mutationValues.containsKey("dateField"));
        Assertions.assertTrue(mutationValues.get("dateField").isNull());
        Assertions.assertTrue(mutationValues.containsKey("timeField"));
        Assertions.assertTrue(mutationValues.get("timeField").isNull());
    }

    @Test
    public void testConvertElementWithCommitTimestampField() {
        final Schema schema = createSchema();

        final Map<String, Object> values = new HashMap<>();
        values.put("id", "a");
        values.put("timestampField", DateTimeUtil.toEpochMicroSecond("2024-10-10T01:23:45Z"));
        values.put("dateField", (int) LocalDate.of(2024, 3, 1).toEpochDay());
        values.put("timeField", 0L);

        final MElement element = MElement.of(values, Instant.parse("2024-10-10T00:00:00Z"));
        final Mutation mutation = ElementToSpannerMutationConverter
                .convert(schema, element, "mytable", null, null, List.of("timestampField"));

        final Map<String, Value> mutationValues = mutation.asMap();
        Assertions.assertEquals(Value.timestamp(Value.COMMIT_TIMESTAMP), mutationValues.get("timestampField"));
    }

}
