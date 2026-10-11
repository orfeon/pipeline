package com.mercari.solution.util.pipeline.udf;

import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.adapter.enumerable.CallImplementor;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.adapter.enumerable.ReflectiveCallNotNullImplementor;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.linq4j.tree.Types;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.Function;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.FunctionParameter;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.ImplementableFunction;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.ScalarFunction;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.type.SqlTypeName;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.util.format.FormatElement;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.util.format.FormatElementEnum;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.util.format.FormatModels;

import java.lang.reflect.Method;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

/**
 * {@code PARSE_DATE} / {@code PARSE_TIME} / {@code PARSE_DATETIME} /
 * {@code PARSE_TIMESTAMP} — replacements of the Calcite BigQuery-library
 * functions of the same names, which {@link UserDefinedFunctions#libraryOperatorTable()}
 * removes from the operator table so that these resolve instead.
 *
 * <p>Calcite turns the format string into a {@link SimpleDateFormat} pattern
 * with <em>one-letter</em> hour / minute / second fields ({@code %H%M} →
 * {@code "Hm"}). A one-letter numeric field directly followed by another
 * numeric field takes a single digit, so an unseparated time is misread
 * without an error: {@code PARSE_TIME("%H%M", "1935")} is hour 1 and minute
 * 935 = 16:35. Here the two-digit elements ({@code %H} {@code %I} {@code %M}
 * {@code %S} {@code %W} {@code %e}) and the three-digit {@code %j} get their
 * full width; everything else is Calcite's own format model, so separated
 * formats parse as before.
 *
 * <p>Results are Calcite-internal values: DATE = epoch day, TIME = millis of
 * day, DATETIME / TIMESTAMP = epoch millis. {@code PARSE_TIMESTAMP} reads the
 * string in the optional time-zone argument (UTC by default) and returns the
 * instant; a wall clock that a daylight-saving transition skips is shifted
 * forward by the gap, and a repeated one is the earlier instant. Literal text
 * of the format is matched as is ({@code "%Y-%m-%dT%H:%M:%S"}). NULL in → NULL out; a string the format does not match, or an
 * out-of-range field (hour 24, February 30), is an error. The parameters are
 * VARCHAR, as the library operators' are: a non-string argument (an integer
 * {@code 20261003}) is cast to a string implicitly.
 */
public final class ParseDateTimeFunctions {

    /** The library operators these functions replace. */
    static final Set<String> NAMES =
            Set.of("PARSE_DATE", "PARSE_TIME", "PARSE_DATETIME", "PARSE_TIMESTAMP");

    private static final long MILLIS_PER_DAY = 86_400_000L;
    private static final String UTC = "UTC";
    private static final int CACHE_SIZE = 64;

    // SimpleDateFormat is not thread-safe; the format is usually a literal, but it may as well
    // be a column, hence the bound.
    private static final ThreadLocal<Map<ParserKey, Parser>> PARSERS =
            ThreadLocal.withInitial(() -> new LinkedHashMap<>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(final Map.Entry<ParserKey, Parser> eldest) {
                    return size() > CACHE_SIZE;
                }
            });

    private record ParserKey(String zoneId, String format) {
    }

    /**
     * A cached parser and the zone it reads in (restored before every parse). When the format
     * has no zone element the parser reads in UTC and {@code wallClockZone} turns that wall
     * clock into the instant; otherwise the parser reads in the zone itself, for the string's
     * own zone to override it, and {@code wallClockZone} is null.
     */
    private record Parser(SimpleDateFormat format, TimeZone zone, ZoneId wallClockZone) {
    }

    private ParseDateTimeFunctions() {
    }

    public static Integer parseDate(Object format, Object value) {
        if (format == null || value == null) {
            return null;
        }
        return (int) Math.floorDiv(parse("PARSE_DATE", format, value, UTC), MILLIS_PER_DAY);
    }

    public static Integer parseTime(Object format, Object value) {
        if (format == null || value == null) {
            return null;
        }
        return (int) Math.floorMod(parse("PARSE_TIME", format, value, UTC), MILLIS_PER_DAY);
    }

    public static Long parseDatetime(Object format, Object value) {
        if (format == null || value == null) {
            return null;
        }
        return parse("PARSE_DATETIME", format, value, UTC);
    }

    public static Long parseTimestamp(Object format, Object value) {
        return parseTimestamp(format, value, UTC);
    }

    public static Long parseTimestamp(Object format, Object value, Object timeZone) {
        if (format == null || value == null || timeZone == null) {
            return null;
        }
        return parse("PARSE_TIMESTAMP", format, value, string("PARSE_TIMESTAMP", "time zone", timeZone));
    }

    /** Parses the value as a wall clock in the zone; the result is the epoch millis of that instant. */
    private static long parse(final String function, final Object format, final Object value,
            final String zoneId) {
        final String fmt = string(function, "format", format);
        final String text = string(function, "value", value);
        final Parser parser = PARSERS.get()
                .computeIfAbsent(new ParserKey(zoneId, fmt), k -> parser(function, fmt, zoneId));
        // A zone name in the string (%Z) switches the SimpleDateFormat to that zone for good, and
        // an ambiguous abbreviation ("CST") is resolved against the current zone first: without
        // this the result would depend on what the thread parsed before.
        parser.format().setTimeZone(parser.zone());
        final ParsePosition position = new ParsePosition(0);
        final Date parsed = parser.format().parse(text, position);
        // either the parse failed, or the format does not cover the whole string
        if (parsed == null || position.getErrorIndex() >= 0 || position.getIndex() != text.length()) {
            throw new IllegalArgumentException(
                    function + ": invalid format: '" + fmt + "' for string: '" + text + "'");
        }
        if (parser.wallClockZone() == null) {
            return parsed.getTime();
        }
        // java.time resolves a wall clock that does not exist in the zone (a daylight-saving gap)
        // by shifting it forward, and a repeated one to the earlier instant; the strict calendar
        // would reject the first as an out-of-range field.
        final long wallClock = parsed.getTime();
        return LocalDateTime.ofEpochSecond(
                        Math.floorDiv(wallClock, 1000L), (int) Math.floorMod(wallClock, 1000L) * 1_000_000,
                        ZoneOffset.UTC)
                .atZone(parser.wallClockZone()).toInstant().toEpochMilli();
    }

    private static Parser parser(final String function, final String fmt, final String zoneId) {
        final SimpleDateFormat parser;
        final boolean[] zoneElement = {false};
        try {
            final StringBuilder pattern = new StringBuilder();
            for (final FormatElement element : FormatModels.BIG_QUERY.parse(fmt)) {
                element.flatten(e -> {
                    zoneElement[0] |= e == FormatElementEnum.TZR;
                    appendPattern(pattern, e);
                });
            }
            // ENGLISH for the month / weekday names, as in Calcite
            parser = new SimpleDateFormat(pattern.toString(), Locale.ENGLISH);
        } catch (final RuntimeException e) {
            throw new IllegalArgumentException(
                    function + ": not supported format: '" + fmt + "': " + e.getMessage(), e);
        }
        final ZoneId zoneOfId;
        try {
            // a region id ("Asia/Tokyo") or an offset ("+09:00"); TimeZone.getTimeZone(String)
            // would silently answer GMT for an id it does not know
            zoneOfId = ZoneId.of(zoneId);
        } catch (final DateTimeException e) {
            throw new IllegalArgumentException(function + ": unknown time zone: '" + zoneId + "'", e);
        }
        final TimeZone zone = TimeZone.getTimeZone(zoneElement[0] ? zoneOfId : ZoneOffset.UTC);
        // Proleptic Gregorian, as Calcite's internal dates are: with the default cutover a date
        // before 1582-10-15 is read in the Julian calendar (1500-03-01 comes out 10 days late).
        final GregorianCalendar calendar = new GregorianCalendar(zone, Locale.ROOT);
        calendar.setGregorianChange(new Date(Long.MIN_VALUE));
        parser.setCalendar(calendar);
        // after setCalendar: leniency lives in the calendar. (Calcite sets it before replacing the
        // calendar, so its parser is lenient and accepts e.g. minute 935 or February 30.)
        parser.setLenient(false);
        return new Parser(parser, zone, zoneElement[0] ? null : zoneOfId);
    }

    private static void appendPattern(final StringBuilder pattern, final FormatElement element) {
        if (element instanceof FormatElementEnum e) {
            // Fixed-width elements: Calcite's one-letter patterns take a single digit when the
            // next field is numeric too.
            final String fixed = switch (e) {
                case HH24 -> "HH";
                case HH12 -> "hh";
                case MI -> "mm";
                case SS -> "ss";
                case WW -> "ww";
                case E -> "dd";
                case DDD -> "DDD";
                default -> null;
            };
            if (fixed != null) {
                pattern.append(fixed);
                return;
            }
            element.toPattern(pattern);
            return;
        }
        // Literal text. Calcite appends it as is, so a letter in it would be read as a pattern
        // letter ("T" is an illegal one, "h" an hour).
        final StringBuilder literal = new StringBuilder();
        element.toPattern(literal);
        boolean quoted = false;
        for (int i = 0; i < literal.length(); i++) {
            final char c = literal.charAt(i);
            // an apostrophe is written '' outside a quoted run ('''' would be two of them)
            if ((c != '\'') != quoted) {
                pattern.append('\'');
                quoted = !quoted;
            }
            pattern.append(c == '\'' ? "''" : String.valueOf(c));
        }
        if (quoted) {
            pattern.append('\'');
        }
    }

    private static String string(final String function, final String parameter, final Object value) {
        if (value instanceof CharSequence s) {
            return s.toString();
        }
        throw new IllegalArgumentException(function + " " + parameter + " must be a string, but got "
                + value.getClass().getSimpleName());
    }

    static List<Map.Entry<String, Function>> builtIns() {
        return List.of(
                Map.entry("PARSE_DATE", new ParseFunction("parseDate", 2, SqlTypeName.DATE)),
                Map.entry("PARSE_TIME", new ParseFunction("parseTime", 2, SqlTypeName.TIME)),
                Map.entry("PARSE_DATETIME", new ParseFunction("parseDatetime", 2, SqlTypeName.TIMESTAMP)),
                // the library operators' own return types: BigQuery's TIMESTAMP is Calcite's
                // TIMESTAMP WITH LOCAL TIME ZONE
                Map.entry("PARSE_TIMESTAMP", new ParseFunction("parseTimestamp", 2,
                        SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)),
                Map.entry("PARSE_TIMESTAMP", new ParseFunction("parseTimestamp", 3,
                        SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)));
    }

    /** The Calcite function object. Hand-built for the date/time return types. */
    private static final class ParseFunction implements ScalarFunction, ImplementableFunction {

        private static final String[] PARAMETERS = {"format", "string", "timeZone"};

        private final Method method;
        private final int arity;
        private final SqlTypeName returnType;

        private ParseFunction(final String methodName, final int arity, final SqlTypeName returnType) {
            final Class<?>[] parameterTypes = new Class<?>[arity];
            java.util.Arrays.fill(parameterTypes, Object.class);
            this.method = Types.lookupMethod(ParseDateTimeFunctions.class, methodName, parameterTypes);
            this.arity = arity;
            this.returnType = returnType;
        }

        @Override
        public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(returnType), true);
        }

        @Override
        public List<FunctionParameter> getParameters() {
            // VARCHAR, not ANY: the validator then casts a non-string argument to a string, as
            // it did for the library operators (STRING operands), and rejects one it cannot
            // cast at planning time instead of failing every element at runtime.
            return SequenceFunnelFunctions.parameters(
                    java.util.Arrays.copyOf(PARAMETERS, arity), ordinal -> SqlTypeName.VARCHAR);
        }

        @Override
        public CallImplementor getImplementor() {
            return RexImpTable.createImplementor(
                    new ReflectiveCallNotNullImplementor(method), NullPolicy.NONE, false);
        }
    }
}
