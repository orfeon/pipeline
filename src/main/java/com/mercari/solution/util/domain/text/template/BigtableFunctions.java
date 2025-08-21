package com.mercari.solution.util.domain.text.template;

import com.mercari.solution.util.DateTimeUtil;

import java.time.Instant;

public class BigtableFunctions {

    public Long reverseTimestampMicros(final Instant instant) {
        return reverseTimestampMicros(DateTimeUtil.toEpochMicroSecond(instant));
    }

    public Long reverseTimestampMicros(final Long epochMicros) {
        return Long.MAX_VALUE - epochMicros;
    }

    public Long reverseTimestampMillis(final Instant instant) {
        return reverseTimestampMillis(DateTimeUtil.reduceAccuracy(DateTimeUtil.toEpochMicroSecond(instant), 1000));
    }

    public Long reverseTimestampMillis(final Long epochMillis) {
        return (Long.MAX_VALUE / 1000) - epochMillis;
    }

}
