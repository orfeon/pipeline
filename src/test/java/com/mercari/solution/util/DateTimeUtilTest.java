package com.mercari.solution.util;

import com.google.cloud.Timestamp;
import org.joda.time.Duration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;


public class DateTimeUtilTest {

    @Test
    public void testIsTime() {
        Assertions.assertTrue(DateTimeUtil.isTime("01:00"));
        Assertions.assertTrue(DateTimeUtil.isTime("21:59"));
        Assertions.assertTrue(DateTimeUtil.isTime("01:12:12"));
        Assertions.assertTrue(DateTimeUtil.isTime("19:34:51"));
        Assertions.assertTrue(DateTimeUtil.isTime("23:27:49"));
        Assertions.assertTrue(DateTimeUtil.isTime("01:12:12,000"));
        Assertions.assertTrue(DateTimeUtil.isTime("23:59:59.999"));
        Assertions.assertTrue(DateTimeUtil.isTime("23:59:59.999999"));
        Assertions.assertTrue(DateTimeUtil.isTime("23:59:59.999999999"));

        Assertions.assertFalse(DateTimeUtil.isTime("24:03"));
        Assertions.assertFalse(DateTimeUtil.isTime("30:12"));
        Assertions.assertFalse(DateTimeUtil.isTime("00:60"));
        Assertions.assertFalse(DateTimeUtil.isTime("24:27:49"));
        Assertions.assertFalse(DateTimeUtil.isTime("23:64:49"));
        Assertions.assertFalse(DateTimeUtil.isTime("23:54:60"));


        Assertions.assertFalse(DateTimeUtil.isTime("0"));
        Assertions.assertFalse(DateTimeUtil.isTime("00"));
        Assertions.assertFalse(DateTimeUtil.isTime("000"));
        Assertions.assertFalse(DateTimeUtil.isTime("00000"));
        Assertions.assertFalse(DateTimeUtil.isTime("adsfadsaf"));
    }

    @Test
    public void testIsDate() {
        Assertions.assertTrue(DateTimeUtil.isDate("19000101"));
        Assertions.assertTrue(DateTimeUtil.isDate("19991231"));
        Assertions.assertTrue(DateTimeUtil.isDate("20010101"));
        Assertions.assertTrue(DateTimeUtil.isDate("20991231"));
        Assertions.assertTrue(DateTimeUtil.isDate("1999-12-31"));
        Assertions.assertTrue(DateTimeUtil.isDate("1999/12/31"));
        Assertions.assertTrue(DateTimeUtil.isDate("2020/01/01"));

        Assertions.assertFalse(DateTimeUtil.isDate("0"));
        Assertions.assertFalse(DateTimeUtil.isDate("00"));
        Assertions.assertFalse(DateTimeUtil.isDate("000"));
        Assertions.assertFalse(DateTimeUtil.isDate("0000"));
        Assertions.assertFalse(DateTimeUtil.isDate("00000"));
        Assertions.assertFalse(DateTimeUtil.isDate("000000"));
        Assertions.assertFalse(DateTimeUtil.isDate("0000000"));
        Assertions.assertFalse(DateTimeUtil.isDate("00000000"));

        Assertions.assertFalse(DateTimeUtil.isDate("10001010"));
        Assertions.assertFalse(DateTimeUtil.isDate("18991231"));
        Assertions.assertFalse(DateTimeUtil.isDate("21000101"));
        Assertions.assertFalse(DateTimeUtil.isDate("1999-00-01"));
        Assertions.assertFalse(DateTimeUtil.isDate("1999-01-00"));
        Assertions.assertFalse(DateTimeUtil.isDate("2000-13-01"));
        Assertions.assertFalse(DateTimeUtil.isDate("2000-21-01"));
        Assertions.assertFalse(DateTimeUtil.isDate("2000-01-32"));
    }

    @Test
    public void testIsTimestamp() {
        Assertions.assertTrue(DateTimeUtil.isTimestamp("1999-12-31T12:12:12"));
        Assertions.assertTrue(DateTimeUtil.isTimestamp("1999-12-31 12:12:12"));
        Assertions.assertTrue(DateTimeUtil.isTimestamp("1999/12/31 12:12:12"));
        Assertions.assertTrue(DateTimeUtil.isTimestamp("1999-12-31 12:12:12Z"));
        Assertions.assertTrue(DateTimeUtil.isTimestamp("1999-12-31T12:12:12.123456789Z"));
        Assertions.assertTrue(DateTimeUtil.isTimestamp("1999-12-31 12:12:12+0900"));
        Assertions.assertTrue(DateTimeUtil.isTimestamp("2020-01-31T23:59:59+09:00"));
        Assertions.assertTrue(DateTimeUtil.isTimestamp("2020-01-31T23:59:59-1200"));
        Assertions.assertTrue(DateTimeUtil.isTimestamp("2020-01-31T23:59:59.000123+09:00"));
        Assertions.assertTrue(DateTimeUtil.isTimestamp("1999-12-31T12:12:12.123456789Z".trim().toLowerCase()));

        Assertions.assertFalse(DateTimeUtil.isTimestamp("1999-12-31"));
        Assertions.assertFalse(DateTimeUtil.isTimestamp("23:59:59.000"));
        Assertions.assertFalse(DateTimeUtil.isTimestamp("2020-13-31T23:59:59+09:00"));
        Assertions.assertFalse(DateTimeUtil.isTimestamp("2020-13-31T24:00:00"));
        Assertions.assertFalse(DateTimeUtil.isTimestamp("1999-12-31T12:12:12.1234567890Z"));
        Assertions.assertFalse(DateTimeUtil.isTimestamp("19991231235959"));

    }

    @Test
    public void testToLocalTime() {
        Assertions.assertEquals(0, DateTimeUtil.toLocalTime("00:00").toSecondOfDay());
        Assertions.assertEquals(3600, DateTimeUtil.toLocalTime("01:00").toSecondOfDay());
        Assertions.assertEquals(3600 * 24 - 60, DateTimeUtil.toLocalTime("23:59").toSecondOfDay());
        Assertions.assertEquals(3600 * 24 - 60, DateTimeUtil.toLocalTime("23:59:00").toSecondOfDay());
        Assertions.assertEquals(3600 * 24 - 1, DateTimeUtil.toLocalTime("23:59:59").toSecondOfDay());
        Assertions.assertEquals(3600 * 10 - 1, DateTimeUtil.toLocalTime("09:59:59").toSecondOfDay());
        Assertions.assertEquals(3600 * 10 - 1, DateTimeUtil.toLocalTime("09:59:59.000").toSecondOfDay());
        Assertions.assertEquals(0, DateTimeUtil.toLocalTime("09:59:59.000").getNano());
        Assertions.assertEquals(599_000_000, DateTimeUtil.toLocalTime("12:59:59.599").getNano());
        Assertions.assertEquals(599, DateTimeUtil.toLocalTime("23:59:59.000000599").getNano());

        Assertions.assertNull(DateTimeUtil.toLocalTime("24:00", true));
        Assertions.assertNull(DateTimeUtil.toLocalTime("23:60", true));
        Assertions.assertNull(DateTimeUtil.toLocalTime("asdfasdfa", true));
    }

    @Test
    public void testToLocalDate() {
        Assertions.assertEquals(0, DateTimeUtil.toLocalDate("1970-01-01").toEpochDay());
        Assertions.assertEquals(
                LocalDate.of(1000, 1, 1).toEpochDay(),
                DateTimeUtil.toLocalDate("1000-01-01").toEpochDay());
        Assertions.assertEquals(
                LocalDate.of(2010, 12, 31).toEpochDay(),
                DateTimeUtil.toLocalDate("2010/12/31").toEpochDay());
        Assertions.assertEquals(
                LocalDate.of(2020, 2, 29).toEpochDay(),
                DateTimeUtil.toLocalDate("2020/02/29").toEpochDay());
        Assertions.assertEquals(
                LocalDate.of(2020, 2, 29).toEpochDay(),
                Long.valueOf(DateTimeUtil.toEpochDay(new Date(2020, Calendar.FEBRUARY, 29))).longValue());
        Assertions.assertEquals(
                LocalDate.of(2020, 2, 29).toEpochDay(),
                Long.valueOf(DateTimeUtil.toEpochDay(java.sql.Date.valueOf("2020-02-29"))).longValue());
    }

    @Test
    public void testToInstant() {
        Assertions.assertEquals(0, DateTimeUtil.toInstant("1970-01-01T00:00:00.000Z").toEpochMilli());
        Assertions.assertEquals(0, DateTimeUtil.toInstant("1970-01-01 00:00:00").toEpochMilli());
        Assertions.assertEquals(0, DateTimeUtil.toInstant("19700101").toEpochMilli());
        Assertions.assertEquals(0, DateTimeUtil.toInstant("1970-01-01").toEpochMilli());
        Assertions.assertEquals(0, DateTimeUtil.toInstant("1970/01/01").toEpochMilli());
        Assertions.assertEquals(0, DateTimeUtil.toInstant("1970-01-01T00:00:00.000Z".trim().toLowerCase()).toEpochMilli());

        Assertions.assertEquals(-3600_000 * 9, DateTimeUtil.toInstant("1970-01-01T00:00:00+0900").toEpochMilli());
        Assertions.assertEquals(3600_000 * 9, DateTimeUtil.toInstant("1970-01-01T00:00:00-0900").toEpochMilli());
        Assertions.assertEquals(-3600_000 * 9, DateTimeUtil.toInstant("1970-01-01T00:00:00.000000+0900").toEpochMilli());
        Assertions.assertEquals(-3600_000 * 9 + 123, DateTimeUtil.toInstant("1970-01-01T00:00:00.123000+0900").toEpochMilli());
        Assertions.assertEquals(123456789, DateTimeUtil.toInstant("1970-01-01T00:00:00.123456789+0900").getNano());
    }

    @Test
    public void testToJodaInstant() {
        Instant instant = Instant.parse("1970-01-01T00:00:00.123456Z");
        Assertions.assertEquals(123, DateTimeUtil.toJodaInstant(instant).getMillis());
    }

    @Test
    public void testToJodaInstantFromCloudTimestamp() {
        Timestamp timestamp = Timestamp.parseTimestamp("1970-01-01T00:00:00.123456Z");
        Assertions.assertEquals(123, DateTimeUtil.toJodaInstant(timestamp).getMillis());
    }

    @Test
    public void testToEpochMicroSecond() {
        Instant instant = Instant.parse("1970-01-01T00:00:00.123456Z");
        Assertions.assertEquals(Long.valueOf(123456L), DateTimeUtil.toEpochMicroSecond(instant));
    }

    @Test
    public void testToEpochMicroSecondFromCloudTimestamp() {
        Timestamp timestamp1 = Timestamp.parseTimestamp("1970-01-01T00:00:00.123456Z");
        Assertions.assertEquals(Long.valueOf(123456L), DateTimeUtil.toEpochMicroSecond(timestamp1));
        final Timestamp timestamp2 = Timestamp.parseTimestamp("2022-06-09T03:16:01.369262000Z");
        Assertions.assertEquals(Instant.ofEpochSecond(timestamp2.getSeconds(), timestamp2.getNanos()).toEpochMilli(), DateTimeUtil.toEpochMicroSecond(timestamp2)/ 1000);
    }

    @Test
    public void testToLocalDateTime() {
        Instant instant1 = Instant.parse("2022-03-25T12:01:32.123Z");
        final LocalDateTime localDateTime1 = DateTimeUtil.toLocalDateTime(instant1.toEpochMilli() * 1000L);
        Assertions.assertEquals("2022-03-25T12:01:32.123", localDateTime1.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")));

        Instant instant2 = Instant.parse("2022-12-25T22:21:32.999Z");
        final LocalDateTime localDateTime2 = DateTimeUtil.toLocalDateTime(instant2.toEpochMilli() * 1000L);
        Assertions.assertEquals("2022-12-25T22:21:32.999", localDateTime2.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")));
    }

    @Test
    public void testGetDuration() {
        final Duration durationSecond12 = DateTimeUtil.getDuration(DateTimeUtil.TimeUnit.second, 12L);
        Assertions.assertEquals(12L, durationSecond12.getStandardSeconds());
        final Duration durationMinute7 = DateTimeUtil.getDuration(DateTimeUtil.TimeUnit.minute, 7L);
        Assertions.assertEquals(7L, durationMinute7.getStandardMinutes());
        final Duration durationHour19 = DateTimeUtil.getDuration(DateTimeUtil.TimeUnit.hour, 19L);
        Assertions.assertEquals(19L, durationHour19.getStandardHours());
        final Duration durationDay45 = DateTimeUtil.getDuration(DateTimeUtil.TimeUnit.day, 45L);
        Assertions.assertEquals(45L, durationDay45.getStandardDays());
    }

}
