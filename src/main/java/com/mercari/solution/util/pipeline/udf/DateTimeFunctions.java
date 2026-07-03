package com.mercari.solution.util.pipeline.udf;

import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.linq4j.tree.Types;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.Function;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.sql.Date;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

/**
 * Built-in date/time scalar UDFs registered by {@code Query2} for every query
 * (successor of the deprecated {@code util.domain.sql.calcite.udf} versions):
 * {@code CURRENT_DATE_()} / {@code CURRENT_DATE_('Asia/Tokyo')} — the current
 * date, optionally in a time zone (the trailing underscore avoids the SQL
 * standard {@code CURRENT_DATE} niladic function).
 */
public final class DateTimeFunctions {

    private DateTimeFunctions() {
    }

    static Iterable<Map.Entry<String, Function>> functions() {
        return List.of(
                Map.entry("CURRENT_DATE_", ScalarFunctionImpl.create(
                        Types.lookupMethod(DateTimeFunctions.class, "currentDate"))),
                Map.entry("CURRENT_DATE_", ScalarFunctionImpl.create(
                        Types.lookupMethod(DateTimeFunctions.class, "currentDate", String.class))));
    }

    public static Date currentDate() {
        return Date.valueOf(LocalDate.now());
    }

    public static Date currentDate(String zoneId) {
        return Date.valueOf(LocalDate.now(ZoneId.of(zoneId)));
    }
}
