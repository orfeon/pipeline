package com.mercari.solution.util.schema.fwf;

/**
 * A record that can not be decoded with the fwf layout: a record length mismatch, a value that can
 * not be converted to its declared type, or a null in a required field. Callers route the record to
 * the failure output (work_fixedwidth.md §4.1 onLengthMismatch / onParseError).
 */
public class FwfException extends RuntimeException {

    private final String field;
    private final String value;
    // the message without the field path and the value; null for record-level errors
    private final String detail;

    public FwfException(final String message) {
        super(message);
        this.field = null;
        this.value = null;
        this.detail = null;
    }

    public FwfException(final String field, final String value, final String message, final Throwable cause) {
        super("fwf field " + field + ": " + message + (value == null ? "" : " (value: '" + value + "')"), cause);
        this.field = field;
        this.value = value;
        this.detail = message;
    }

    /** Dotted path of the field (array elements as {@code name[i]}); null for record-level errors. */
    public String getField() {
        return field;
    }

    /** The raw text of the field; null for record-level errors. */
    public String getValue() {
        return value;
    }

    /**
     * The same error with the field path prefixed by the enclosing group ({@code items[1]} +
     * {@code qty} → {@code items[1].qty}). The decoder builds the path on the way out of a failing
     * record, so that decoding a valid record never concatenates path strings.
     */
    FwfException within(final String parent) {
        if(field == null) {
            return this;
        }
        return new FwfException(parent + "." + field, value, detail, getCause());
    }

}
