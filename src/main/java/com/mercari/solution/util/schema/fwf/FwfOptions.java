package com.mercari.solution.util.schema.fwf;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Conversion options of the fixed-width format, declared as {@code schema.encoding} keys next to
 * {@code format: fwf} (work_fixedwidth.md §4.1). The layout itself (positions / lengths) is the
 * definition document and lives in {@code schema.reference} ({@link FwfLayout}).
 */
public class FwfOptions implements Serializable {

    public enum Unit { byte_, char_ }
    public enum Trim { both, left, right, none }
    public enum OnLengthMismatch { fail, pad }
    public enum OnParseError { fail, nullify }

    // a List, not Set.of: the keys are printed in error messages, and the iteration order of Set.of
    // changes from one JVM run to the next
    private static final List<String> KEYS = List.of(
            "charset", "unit", "trim", "emptyAsNull", "onLengthMismatch", "onParseError");

    private String charset = StandardCharsets.UTF_8.name();
    private Unit unit = Unit.byte_;
    private Trim trim = Trim.both;
    private boolean emptyAsNull = true;
    private OnLengthMismatch onLengthMismatch = OnLengthMismatch.fail;
    private OnParseError onParseError = OnParseError.fail;

    public Charset getCharset() {
        return Charset.forName(charset);
    }

    public String getCharsetName() {
        return charset;
    }

    public Unit getUnit() {
        return unit;
    }

    public Trim getTrim() {
        return trim;
    }

    public boolean isEmptyAsNull() {
        return emptyAsNull;
    }

    public OnLengthMismatch getOnLengthMismatch() {
        return onLengthMismatch;
    }

    public OnParseError getOnParseError() {
        return onParseError;
    }

    public static FwfOptions defaults() {
        return new FwfOptions();
    }

    /**
     * Parses the encoding options (all keys of {@code schema.encoding} except {@code format}).
     * Unknown keys and invalid values are collected into one {@link IllegalArgumentException}.
     */
    public static FwfOptions of(final Map<String, String> values) {
        final FwfOptions options = new FwfOptions();
        if(values == null || values.isEmpty()) {
            return options;
        }
        final List<String> errors = new ArrayList<>();
        for(final Map.Entry<String, String> entry : values.entrySet()) {
            final String key = entry.getKey();
            final String value = entry.getValue() == null ? null : entry.getValue().trim();
            if(!KEYS.contains(key)) {
                errors.add("schema.encoding." + key + " is not supported for format fwf. supported keys: " + KEYS);
                continue;
            }
            if(value == null || value.isEmpty()) {
                errors.add("schema.encoding." + key + " must not be empty");
                continue;
            }
            switch (key) {
                case "charset" -> {
                    try {
                        options.charset = Charset.forName(value).name();
                    } catch (final IllegalArgumentException e) {
                        errors.add("schema.encoding.charset: " + value + " is not a supported charset");
                    }
                }
                case "unit" -> {
                    switch (value.toLowerCase(Locale.ROOT)) {
                        case "byte" -> options.unit = Unit.byte_;
                        case "char" -> options.unit = Unit.char_;
                        default -> errors.add("schema.encoding.unit must be byte or char. but: " + value);
                    }
                }
                case "trim" -> {
                    try {
                        options.trim = Trim.valueOf(value.toLowerCase(Locale.ROOT));
                    } catch (final IllegalArgumentException e) {
                        errors.add("schema.encoding.trim must be one of both, left, right, none. but: " + value);
                    }
                }
                case "emptyAsNull" -> {
                    switch (value.toLowerCase(Locale.ROOT)) {
                        case "true" -> options.emptyAsNull = true;
                        case "false" -> options.emptyAsNull = false;
                        default -> errors.add("schema.encoding.emptyAsNull must be boolean. but: " + value);
                    }
                }
                case "onLengthMismatch" -> {
                    try {
                        options.onLengthMismatch = OnLengthMismatch.valueOf(value.toLowerCase(Locale.ROOT));
                    } catch (final IllegalArgumentException e) {
                        errors.add("schema.encoding.onLengthMismatch must be fail or pad. but: " + value);
                    }
                }
                case "onParseError" -> {
                    switch (value.toLowerCase(Locale.ROOT)) {
                        case "fail" -> options.onParseError = OnParseError.fail;
                        case "null" -> options.onParseError = OnParseError.nullify;
                        default -> errors.add("schema.encoding.onParseError must be fail or null. but: " + value);
                    }
                }
            }
        }
        if(!errors.isEmpty()) {
            throw new IllegalArgumentException(String.join(", ", errors));
        }
        return options;
    }

    @Override
    public String toString() {
        return String.format("{ charset: %s, unit: %s, trim: %s, emptyAsNull: %s, onLengthMismatch: %s, onParseError: %s }",
                charset, unit == Unit.byte_ ? "byte" : "char", trim, emptyAsNull, onLengthMismatch,
                onParseError == OnParseError.fail ? "fail" : "null");
    }

}
