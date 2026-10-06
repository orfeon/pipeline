package com.mercari.solution.util.domain.file;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ByteRecordReaderTest {

    private static List<String> read(final ByteRecordReader reader) throws IOException {
        final List<String> records = new ArrayList<>();
        try(reader) {
            while(reader.next()) {
                records.add(reader.number() + ":" + new String(reader.buffer(), 0, reader.length(), StandardCharsets.ISO_8859_1));
            }
        }
        return records;
    }

    private static InputStream stream(final String text) {
        return new ByteArrayInputStream(text.getBytes(StandardCharsets.ISO_8859_1));
    }

    // returns one byte per read: separators and records straddle every buffer boundary
    private static InputStream trickle(final String text) {
        final InputStream in = stream(text);
        return new InputStream() {
            @Override
            public int read() throws IOException {
                return in.read();
            }

            @Override
            public int read(final byte[] b, final int off, final int len) throws IOException {
                return in.read(b, off, Math.min(1, len));
            }
        };
    }

    @Test
    public void testLines() throws IOException {
        // CRLF and LF mixed, a blank line, and no separator after the last record
        Assertions.assertEquals(List.of("1:a", "2:bb", "3:", "4:ccc"),
                read(ByteRecordReader.lines(stream("a\r\nbb\n\r\nccc"))));
        // the empty tail after the final separator is not a record
        Assertions.assertEquals(List.of("1:a", "2:b"), read(ByteRecordReader.lines(stream("a\nb\n"))));
        Assertions.assertEquals(List.of(), read(ByteRecordReader.lines(stream(""))));
        Assertions.assertEquals(List.of("1:"), read(ByteRecordReader.lines(stream("\n"))));
        // a lone CR inside a record is kept
        Assertions.assertEquals(List.of("1:a\rb"), read(ByteRecordReader.lines(stream("a\rb\n"))));
        Assertions.assertEquals(List.of("1:a", "2:bb", "3:", "4:ccc"),
                read(ByteRecordReader.lines(trickle("a\r\nbb\n\r\nccc"))));
    }

    @Test
    public void testLongLine() throws IOException {
        // longer than the initial record buffer and the read buffer
        final String longLine = "x".repeat(200_000);
        final List<String> records = read(ByteRecordReader.lines(stream(longLine + "\r\n" + "y\n")));
        Assertions.assertEquals(2, records.size());
        Assertions.assertEquals("1:" + longLine, records.get(0));
        Assertions.assertEquals("2:y", records.get(1));
    }

    @Test
    public void testDelimited() throws IOException {
        // a multi-byte separator; a partial match stays in the record; CR is not touched
        final byte[] delimiter = "||".getBytes(StandardCharsets.ISO_8859_1);
        Assertions.assertEquals(List.of("1:a|b", "2:", "3:c\r", "4:d"),
                read(ByteRecordReader.delimited(stream("a|b||||c\r||d"), delimiter)));
        Assertions.assertEquals(List.of("1:a|b", "2:", "3:c\r", "4:d"),
                read(ByteRecordReader.delimited(trickle("a|b||||c\r||d"), delimiter)));
        // the separator straddles the read buffer boundary (64 KiB)
        final String head = "x".repeat(64 * 1024 - 1);
        Assertions.assertEquals(List.of("1:" + head, "2:y"),
                read(ByteRecordReader.delimited(stream(head + "||y"), delimiter)));
        Assertions.assertThrows(IllegalArgumentException.class, () -> ByteRecordReader.delimited(stream("a"), new byte[0]));
    }

    @Test
    public void testFixed() throws IOException {
        Assertions.assertEquals(List.of("1:abc", "2:def"), read(ByteRecordReader.fixed(stream("abcdef"), 3)));
        // the stream does not end on a record boundary: the last record is short
        Assertions.assertEquals(List.of("1:abc", "2:de"), read(ByteRecordReader.fixed(trickle("abcde"), 3)));
        Assertions.assertEquals(List.of(), read(ByteRecordReader.fixed(stream(""), 3)));
        // separators are data
        Assertions.assertEquals(List.of("1:a\nb", "2:\r\nc"), read(ByteRecordReader.fixed(stream("a\nb\r\nc"), 3)));
        Assertions.assertThrows(IllegalArgumentException.class, () -> ByteRecordReader.fixed(stream("a"), 0));
    }

}
