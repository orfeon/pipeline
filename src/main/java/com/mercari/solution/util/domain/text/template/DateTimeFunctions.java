package com.mercari.solution.util.domain.text.template;

import com.mercari.solution.util.DateTimeUtil;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

public class DateTimeFunctions {

    public String formatTimestamp(Long epocMicros) {
        return formatTimestamp(epocMicros, null, null);
    }

    public String formatTimestamp(Instant timestamp) {
        return formatTimestamp(timestamp, null, null);
    }

    public String formatTimestamp(Long epocMicros, String pattern) {
        return formatTimestamp(epocMicros, pattern,null);
    }

    public String formatTimestamp(Instant timestamp, String pattern) {
        return formatTimestamp(timestamp, pattern,null);
    }

    public String formatTimestamp(Long epocMicros, String pattern, String timezone) {
        if(epocMicros == null) {
            return "";
        }
        final Instant timestamp = DateTimeUtil.toInstant(epocMicros);
        return formatTimestamp(timestamp, pattern, timezone);
    }

    public String formatTimestamp(Instant timestamp, String pattern, String timezone) {
        if(timestamp == null) {
            return "";
        }
        final LocalDateTime dateTime = getLocalDateTime(timestamp, timezone);
        final DateTimeFormatter formatter;
        if(pattern == null) {
            formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'");
        } else {
            formatter = DateTimeFormatter.ofPattern(pattern);
        }
        return dateTime.format(formatter);
    }

    public String formatDate(Integer epocDays) {
        return formatDate(epocDays, null);
    }

    public String formatDate(LocalDate localDate) {
        return formatDate(localDate, null);
    }

    public String formatDate(String dateStr) {
        return formatDate(dateStr, null);
    }

    public String formatDate(Integer epocDays, String pattern) {
        if(epocDays == null) {
            return "";
        }
        final LocalDate localDate = LocalDate.ofEpochDay(epocDays);
        return formatDate(localDate, pattern);
    }

    public String formatDate(String dateStr, String pattern) {
        if(dateStr == null) {
            return "";
        }
        final LocalDate localDate = DateTimeUtil.toLocalDate(dateStr);
        return formatDate(localDate, pattern);
    }

    public String formatDate(LocalDate localDate, String pattern) {
        if(localDate == null) {
            return "";
        }
        final DateTimeFormatter formatter;
        if(pattern == null) {
            formatter = DateTimeFormatter.ISO_DATE;
        } else {
            formatter = DateTimeFormatter.ofPattern(pattern);
        }
        return localDate.format(formatter);
    }

    public String formatTime(Long epocMicros) {
        return formatTime(epocMicros, null);
    }

    public String formatTime(LocalTime localTime) {
        return formatTime(localTime, null);
    }

    public String formatTime(Long epocMicros, String pattern) {
        if(epocMicros == null) {
            return "";
        }
        final LocalTime localTime = LocalTime.ofNanoOfDay(epocMicros * 1000L);
        return formatTime(localTime, pattern);
    }

    public String formatTime(LocalTime localTime, String pattern) {
        if(localTime == null) {
            return "";
        }
        final DateTimeFormatter formatter;
        if(pattern == null) {
            formatter = DateTimeFormatter.ISO_TIME;
        } else {
            formatter = DateTimeFormatter.ofPattern(pattern);
        }
        return localTime.format(formatter);
    }

    public LocalDate currentDate(final String zone) {
        return currentDate(zone, null, null);
    }

    public LocalDate currentDate(final String zone, final Long plusDays) {
        return currentDate(zone, plusDays, "DAYS");
    }

    public LocalDate currentDate(final String zone, final Long plusAmount, final String unit) {
        LocalDate localDate;
        if(zone == null) {
            localDate = LocalDate.now(ZoneOffset.UTC);
        } else {
            localDate = LocalDate.now(ZoneId.of(zone));
        }
        if(plusAmount != null && unit != null) {
            final ChronoUnit chronoUnit = ChronoUnit.valueOf(unit);
            localDate = localDate.plus(plusAmount, chronoUnit);
        }
        return localDate;
    }

    public LocalTime currentTime(final String zone) {
        return currentTime(zone, null, null);
    }

    public LocalTime currentTime(final String zone, final Long plusSeconds) {
        return currentTime(zone, plusSeconds, "SECONDS");
    }

    public LocalTime currentTime(final String zone, final Long plusAmount, final String unit) {
        LocalTime localTime;
        if(zone == null) {
            localTime = LocalTime.now(ZoneOffset.UTC);
        } else {
            localTime = LocalTime.now(ZoneId.of(zone));
        }
        if(plusAmount != null && unit != null) {
            final ChronoUnit chronoUnit = ChronoUnit.valueOf(unit);
            localTime = localTime.plus(plusAmount, chronoUnit);
        }
        return localTime;
    }

    public String currentDateTime(final String zone) {
        return currentDateTime(zone, 0L);
    }

    public String currentDateTime(final String zone, final Long plusSeconds) {
        return currentDateTime(zone, plusSeconds, "yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
    }

    public String currentDateTime(final String zone, final Long plusSeconds, final String pattern) {
        final LocalDateTime localDateTime = LocalDateTime.now(ZoneId.of(zone));
        return DateTimeFormatter.ofPattern(pattern).format(localDateTime.plusSeconds(plusSeconds));
    }

    public Instant currentTimestamp() {
        return currentTimestamp(null, null, null);
    }

    public Instant currentTimestamp(final Long plusSeconds) {
        return currentTimestamp(plusSeconds, ChronoUnit.SECONDS.name(), null);
    }

    public Instant currentTimestamp(final Long plusAmount, final String unit) {
        return currentTimestamp(plusAmount, unit, null);
    }

    public Instant currentTimestamp(final String truncateUnit) {
        return currentTimestamp(null, null, truncateUnit);
    }

    public Instant currentTimestamp(final Long plusAmount, final String unit, final String truncateUnit) {
        LocalDateTime instant = LocalDateTime.now(ZoneOffset.UTC);
        if(plusAmount != null && unit != null) {
            final ChronoUnit chronoUnit = ChronoUnit.valueOf(unit);
            instant = instant.plus(plusAmount, chronoUnit);
        }
        if(truncateUnit != null) {
            final ChronoUnit chronoUnit = ChronoUnit.valueOf(truncateUnit);
            instant = instant.truncatedTo(chronoUnit);
        }
        return instant.toInstant(ZoneOffset.UTC);
    }

    public Integer currentEpochSeconds() {
        return currentEpochSeconds(null, null);
    }

    public Integer currentEpochSeconds(final Long plusSecond) {
        return currentEpochSeconds(plusSecond, ChronoUnit.SECONDS.name());
    }

    public Integer currentEpochSeconds(final Long plusAmount, final String unit) {
        Instant instant = Instant.now();
        if(plusAmount != null && unit != null) {
            final ChronoUnit chronoUnit = ChronoUnit.valueOf(unit);
            instant = instant.plus(plusAmount, chronoUnit);
        }
        return Long.valueOf(instant.getEpochSecond()).intValue();
    }

    public String year(Instant timestamp, String timezone) {
        if(timestamp == null) {
            return "";
        }
        final LocalDateTime dateTime = getLocalDateTime(timestamp, timezone);
        return Integer.valueOf(dateTime.getYear()).toString();
    }

    public String month(Instant timestamp, String timezone) {
        if(timestamp == null) {
            return "";
        }
        final LocalDateTime dateTime = getLocalDateTime(timestamp, timezone);
        return Integer.valueOf(dateTime.getMonthValue()).toString();
    }

    public String month(Instant timestamp, String timezone, Integer padding) {
        if(timestamp == null) {
            return "";
        }
        final LocalDateTime dateTime = getLocalDateTime(timestamp, timezone);
        return String.format("%0" + padding + "d", dateTime.getMonthValue());
    }

    public String day(Instant timestamp, String timezone) {
        if(timestamp == null) {
            return "";
        }
        final LocalDateTime dateTime = getLocalDateTime(timestamp, timezone);
        return Integer.valueOf(dateTime.getDayOfMonth()).toString();
    }

    public String day(Instant timestamp, String timezone, Integer padding) {
        if(timestamp == null) {
            return "";
        }
        final LocalDateTime dateTime = getLocalDateTime(timestamp, timezone);
        return String.format("%0" + padding + "d", dateTime.getDayOfMonth());
    }

    private static LocalDateTime getLocalDateTime(final Instant timestamp, final String timezone) {
        if(timezone == null) {
            return LocalDateTime.ofInstant(timestamp, ZoneOffset.UTC);
        } else {
            return LocalDateTime.ofInstant(timestamp, ZoneId.of(timezone));
        }
    }

}
