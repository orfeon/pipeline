package com.mercari.solution.util.domain.file;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

/**
 * Reads records from a byte stream without decoding them (work_fixedwidth.md §5): text IO decodes
 * lines as UTF-8, which destroys data in other charsets (Shift_JIS ...), so formats that cut or
 * decode the bytes themselves read through this.
 *
 * <ul>
 *   <li>{@link #lines}: records separated by LF; a CR before the LF is removed, so CRLF and LF files
 *       (and files mixing them) read the same.</li>
 *   <li>{@link #delimited}: records separated by an arbitrary byte sequence, kept as they are.</li>
 *   <li>{@link #fixed}: records of a fixed length with no separator. The last record is shorter when
 *       the stream does not end on a record boundary.</li>
 * </ul>
 *
 * An empty record between two separators is returned (the caller decides to skip it); the empty
 * tail after the final separator is not. The record buffer is reused: copy the bytes to keep them.
 */
public class ByteRecordReader implements Closeable {

    private static final int READ_BUFFER_SIZE = 64 * 1024;
    // the largest array the JVM is sure to allocate
    private static final int MAX_RECORD_LENGTH = Integer.MAX_VALUE - 8;

    private final InputStream input;
    private final byte[] delimiter;
    private final boolean stripCarriageReturn;
    private final int fixedLength;

    private final byte[] readBuffer = new byte[READ_BUFFER_SIZE];
    private int readPosition;
    private int readLimit;
    private boolean endOfStream;

    private byte[] record;
    private int length;
    private long number;

    private ByteRecordReader(
            final InputStream input,
            final byte[] delimiter,
            final boolean stripCarriageReturn,
            final int fixedLength) {

        this.input = input;
        this.delimiter = delimiter;
        this.stripCarriageReturn = stripCarriageReturn;
        this.fixedLength = fixedLength;
        this.record = new byte[fixedLength > 0 ? fixedLength : 1024];
    }

    public static ByteRecordReader lines(final InputStream input) {
        return new ByteRecordReader(input, new byte[] { '\n' }, true, 0);
    }

    public static ByteRecordReader delimited(final InputStream input, final byte[] delimiter) {
        if(delimiter == null || delimiter.length == 0) {
            throw new IllegalArgumentException("delimiter must not be empty");
        }
        return new ByteRecordReader(input, delimiter.clone(), false, 0);
    }

    public static ByteRecordReader fixed(final InputStream input, final int length) {
        if(length <= 0) {
            throw new IllegalArgumentException("record length must be positive. but: " + length);
        }
        return new ByteRecordReader(input, null, false, length);
    }

    /** Advances to the next record; false at the end of the stream. */
    public boolean next() throws IOException {
        length = 0;
        return fixedLength > 0 ? nextFixed() : nextDelimited();
    }

    /** The buffer holding the current record in {@code [0, length())}; valid until the next call of {@link #next}. */
    public byte[] buffer() {
        return record;
    }

    public int length() {
        return length;
    }

    /** 1-based number of the current record (the physical line number for {@link #lines}). */
    public long number() {
        return number;
    }

    private boolean nextFixed() throws IOException {
        while(length < fixedLength) {
            if(readPosition == readLimit && !fill()) {
                break;
            }
            final int size = Math.min(fixedLength - length, readLimit - readPosition);
            System.arraycopy(readBuffer, readPosition, record, length, size);
            readPosition += size;
            length += size;
        }
        if(length == 0) {
            return false;
        }
        number++;
        return true;
    }

    private boolean nextDelimited() throws IOException {
        final byte last = delimiter[delimiter.length - 1];
        boolean consumed = false;
        while(true) {
            if(readPosition == readLimit && !fill()) {
                if(!consumed) {
                    return false;
                }
                // the last record has no separator
                break;
            }
            // copy up to and including the next byte that can end a separator in one go
            int end = readPosition;
            while(end < readLimit && readBuffer[end] != last) {
                end++;
            }
            final boolean candidate = end < readLimit;
            final int size = (candidate ? end + 1 : end) - readPosition;
            ensureCapacity(size);
            System.arraycopy(readBuffer, readPosition, record, length, size);
            readPosition += size;
            length += size;
            consumed = true;
            if(candidate && endsWithDelimiter()) {
                length -= delimiter.length;
                break;
            }
        }
        if(stripCarriageReturn && length > 0 && record[length - 1] == '\r') {
            length--;
        }
        number++;
        return true;
    }

    // Room for {@code size} more bytes of the current record. A stream without any separator (a
    // fixed-length file read as lines) ends up here as one record: report it instead of failing
    // on the array size.
    private void ensureCapacity(final int size) throws IOException {
        if(size > MAX_RECORD_LENGTH - length) {
            throw new IOException("record " + (number + 1) + " is longer than " + MAX_RECORD_LENGTH
                    + " bytes: the stream does not seem to have the record separator");
        }
        final int required = length + size;
        if(required > record.length) {
            record = Arrays.copyOf(record, (int) Math.min(MAX_RECORD_LENGTH, Math.max(required, record.length * 2L)));
        }
    }

    private boolean endsWithDelimiter() {
        return length >= delimiter.length
                && Arrays.equals(record, length - delimiter.length, length, delimiter, 0, delimiter.length);
    }

    private boolean fill() throws IOException {
        if(endOfStream) {
            return false;
        }
        int read;
        do {
            read = input.read(readBuffer, 0, readBuffer.length);
        } while(read == 0);
        if(read < 0) {
            endOfStream = true;
            return false;
        }
        readPosition = 0;
        readLimit = read;
        return true;
    }

    @Override
    public void close() throws IOException {
        input.close();
    }

}
