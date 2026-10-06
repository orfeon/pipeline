package com.mercari.solution.util.schema.fwf;

/**
 * A record that can not be decoded with the fwf layout: a record length mismatch, a value that can
 * not be converted to its declared type, or a null in a required field. Callers route the record to
 * the failure output (work_fixedwidth.md §4.1 onLengthMismatch / onParseError).
 */
public class FwfException extends RuntimeException {

    private final String field;
    private final String value;

    public FwfException(final String message) {
        super(message);
        this.field = null;
        this.value = null;
    }

    public FwfException(final String field, final String value, final String message, final Throwable cause) {
        super("fwf field " + field + ": " + message + (value == null ? "" : " (value: '" + value + "')"), cause);
        this.field = field;
        this.value = value;
    }

    /** Dotted path of the field (array elements as {@code name[i]}); null for record-level errors. */
    public String getField() {
        return field;
    }

    /** The raw text of the field; null for record-level errors. */
    public String getValue() {
        return value;
    }

}
