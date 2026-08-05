package com.mercari.solution.util.schema.converter;

import com.mercari.solution.util.domain.db.JdbcUtil;
import com.mercari.solution.util.domain.db.stmt.PreparedStatementTemplate;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Proxy;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicReference;

public class ToStatementConverterTest {

    private static Schema timestampMicrosSchema() {
        return SchemaBuilder.builder()
                .record("root").fields()
                .requiredInt("id")
                .name("created_at").type(AvroSchemaUtil.REQUIRED_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
                .endRecord();
    }

    /**
     * Minimal PreparedStatement stub that records the Timestamp handed to setTimestamp.
     * Uses a JDK dynamic proxy so the test needs no mocking dependency.
     */
    private static PreparedStatement recordingStatement(final AtomicReference<Timestamp> captured) {
        return (PreparedStatement) Proxy.newProxyInstance(
                ToStatementConverterTest.class.getClassLoader(),
                new Class<?>[]{PreparedStatement.class},
                (proxy, method, args) -> {
                    if ("setTimestamp".equals(method.getName())
                            && args != null && args.length >= 2 && args[1] instanceof Timestamp) {
                        captured.set((Timestamp) args[1]);
                    }
                    return defaultValueFor(method.getReturnType());
                });
    }

    private static Object defaultValueFor(final Class<?> returnType) {
        if (!returnType.isPrimitive() || void.class.equals(returnType)) {
            return null;
        }
        if (boolean.class.equals(returnType)) return false;
        if (byte.class.equals(returnType)) return (byte) 0;
        if (short.class.equals(returnType)) return (short) 0;
        if (char.class.equals(returnType)) return (char) 0;
        if (int.class.equals(returnType)) return 0;
        if (long.class.equals(returnType)) return 0L;
        if (float.class.equals(returnType)) return 0f;
        return 0d;
    }

    private static Timestamp convertAndCapture(final long epochMicros) throws SQLException {
        final Schema schema = timestampMicrosSchema();
        final PreparedStatementTemplate template =
                JdbcUtil.createStatement("people", schema, JdbcUtil.OP.INSERT, JdbcUtil.DB.MYSQL, null);

        final AtomicReference<Timestamp> captured = new AtomicReference<>();
        final PreparedStatementTemplate.PlaceholderSetterProxy setter =
                template.createPlaceholderSetterProxy(recordingStatement(captured));

        final GenericRecord record = new GenericData.Record(schema);
        record.put("id", 1);
        record.put("created_at", epochMicros);

        ToStatementConverter.convertRecord(record, setter);

        final Timestamp timestamp = captured.get();
        Assert.assertNotNull("setTimestamp was never called", timestamp);
        return timestamp;
    }

    /**
     * Regression test for #118.
     *
     * timestamp-micros was converted with {@code Instant.ofEpochMilli(micros / 1000)}, whose
     * integer division discarded the final three digits. java.sql.Timestamp carries nanosecond
     * precision, so the microseconds must survive the conversion.
     */
    @Test
    public void testTimestampMicrosPreservesMicrosecondPrecision() throws SQLException {
        final long epochMicros = 1785415696123456L; // 2026-07-30T12:48:16.123456Z

        final Timestamp actual = convertAndCapture(epochMicros);

        Assert.assertEquals(Timestamp.from(Instant.EPOCH.plus(epochMicros, ChronoUnit.MICROS)), actual);
        Assert.assertEquals(123456000, actual.getNanos());
        Assert.assertEquals("2026-07-30T12:48:16.123456Z", actual.toInstant().toString());
    }

    /**
     * Two timestamps differing only below the millisecond previously collapsed onto the same
     * value, silently merging rows that should stay distinct.
     */
    @Test
    public void testTimestampMicrosKeepsSubMillisecondValuesDistinct() throws SQLException {
        final Timestamp first = convertAndCapture(1785415696123456L);
        final Timestamp second = convertAndCapture(1785415696123789L);

        Assert.assertNotEquals(first, second);
        Assert.assertEquals(123456000, first.getNanos());
        Assert.assertEquals(123789000, second.getNanos());
    }

    /**
     * Epoch micros are negative for pre-1970 timestamps. Integer division truncates toward zero,
     * so the previous conversion rounded those the wrong way.
     */
    @Test
    public void testTimestampMicrosHandlesNegativeEpochValues() throws SQLException {
        final Timestamp actual = convertAndCapture(-1L); // one microsecond before the epoch

        Assert.assertEquals("1969-12-31T23:59:59.999999Z", actual.toInstant().toString());
    }
}
