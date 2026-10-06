package com.mercari.solution.util.schema.fwf;

import com.mercari.solution.module.Schema;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Decodes one fixed-width record into element values (work_fixedwidth.md §4.4), shared by the
 * storage source and the select op {@code fwf_decode}.
 *
 * <ol>
 *   <li>cut the range given by pos / len / repeat (with {@code unit: byte} the bytes are cut first
 *       and each range is decoded with the charset on its own, so multi-byte characters never
 *       shift the positions of the following fields)</li>
 *   <li>trim (string / json only; other types are always parsed from the text stripped on both sides)</li>
 *   <li>nullIf → null; empty → null (string / json follow emptyAsNull)</li>
 *   <li>convert to the declared type (failure follows onParseError)</li>
 *   <li>null → defaultValue</li>
 * </ol>
 *
 * Half-width spaces and the ideographic space (U+3000) are stripped. Values of repeated fields keep
 * their positions: an empty element stays null in the list instead of being dropped.
 */
public class FwfDecoder implements Serializable {

    private static final char IDEOGRAPHIC_SPACE = '　';

    private final FwfLayout layout;
    private final FwfOptions options;

    private transient Charset charset;

    private FwfDecoder(final FwfLayout layout, final FwfOptions options) {
        this.layout = layout;
        this.options = options;
    }

    public static FwfDecoder of(final FwfLayout layout, final FwfOptions options) {
        if(layout == null) {
            throw new IllegalArgumentException("fwf layout must not be null");
        }
        return new FwfDecoder(layout, options == null ? FwfOptions.defaults() : options);
    }

    public FwfLayout getLayout() {
        return layout;
    }

    public FwfOptions getOptions() {
        return options;
    }

    public Map<String, Object> decode(final byte[] record) {
        return decode(record, 0, record.length);
    }

    public Map<String, Object> decode(final byte[] buffer, final int offset, final int length) {
        final Source source = switch (options.getUnit()) {
            case byte_ -> new ByteSource(buffer, offset, length, charset());
            case char_ -> new CharSource(new String(buffer, offset, length, charset()), charset());
        };
        return decode(source);
    }

    /**
     * Decodes a record held as text. With {@code unit: byte} the text is encoded back with the
     * charset before cutting (lossless when it was decoded from the same charset).
     */
    public Map<String, Object> decode(final String record) {
        return switch (options.getUnit()) {
            case byte_ -> decode(record.getBytes(charset()));
            case char_ -> decode(new CharSource(record, charset()));
        };
    }

    private Map<String, Object> decode(final Source source) {
        final Integer recordLength = layout.getRecordLength();
        if(recordLength != null && source.length() != recordLength
                && options.getOnLengthMismatch() == FwfOptions.OnLengthMismatch.fail) {
            throw new FwfException(String.format("record length %d does not match the layout recordLength %d",
                    source.length(), recordLength));
        }
        return decodeFields(layout.getFields(), source, 0, null);
    }

    private Map<String, Object> decodeFields(
            final List<FwfLayout.Field> fields,
            final Source source,
            final int base,
            final String parentPath) {

        final Map<String, Object> values = new HashMap<>(Math.max(16, fields.size() * 2));
        for(final FwfLayout.Field field : fields) {
            final String path = parentPath == null ? field.getName() : parentPath + "." + field.getName();
            final int start = base + field.getStart();
            if(field.getRepeat() == null) {
                values.put(field.getName(), decodeElement(field, source, start, path));
            } else {
                final List<Object> list = new ArrayList<>(field.getRepeat());
                for(int i = 0; i < field.getRepeat(); i++) {
                    list.add(decodeElement(field, source, start + i * field.unit(), path + "[" + i + "]"));
                }
                values.put(field.getName(), list);
            }
        }
        return values;
    }

    private Object decodeElement(
            final FwfLayout.Field field,
            final Source source,
            final int start,
            final String path) {

        if(field.isGroup()) {
            return decodeFields(field.getChildren(), source, start, path);
        }

        Object value = null;
        String raw = null;
        // a range beyond the record end (onLengthMismatch: pad, or no recordLength) is null
        if(start + field.getLen() <= source.length()) {
            if(field.getType() == Schema.Type.bytes) {
                value = ByteBuffer.wrap(source.bytes(start, field.getLen()));
            } else {
                raw = source.text(start, field.getLen());
                value = convert(field, raw, path);
            }
        }
        if(value == null && field.getDefaultValue() != null) {
            value = field.getDefaultValue();
        }
        if(value == null && field.isRequired()) {
            throw new FwfException(path, raw, "required field is null", null);
        }
        return value;
    }

    private Object convert(final FwfLayout.Field field, final String raw, final String path) {
        final String stripped = strip(raw, FwfOptions.Trim.both);
        if(field.getNullIf() != null && field.getNullIf().contains(stripped)) {
            return null;
        }
        final boolean text = switch (field.getType()) {
            case string, json -> true;
            default -> false;
        };
        if(text) {
            final FwfOptions.Trim trim = field.getTrim() == null ? options.getTrim() : field.getTrim();
            final String value = trim == FwfOptions.Trim.both ? stripped : strip(raw, trim);
            if(stripped.isEmpty() && options.isEmptyAsNull()) {
                return null;
            }
            return value;
        }
        if(stripped.isEmpty()) {
            return null;
        }
        try {
            return field.convert(stripped);
        } catch (final Exception e) {
            if(options.getOnParseError() == FwfOptions.OnParseError.nullify) {
                return null;
            }
            throw new FwfException(path, raw, "can not convert to " + field.getType() + ": " + e.getMessage(), e);
        }
    }

    private Charset charset() {
        if(charset == null) {
            charset = options.getCharset();
        }
        return charset;
    }

    /** Strips half-width spaces and the ideographic space (U+3000) from the given side(s). */
    public static String strip(final String text, final FwfOptions.Trim trim) {
        if(text == null || trim == FwfOptions.Trim.none) {
            return text;
        }
        int begin = 0;
        int end = text.length();
        if(trim == FwfOptions.Trim.both || trim == FwfOptions.Trim.left) {
            while(begin < end && isSpace(text.charAt(begin))) {
                begin++;
            }
        }
        if(trim == FwfOptions.Trim.both || trim == FwfOptions.Trim.right) {
            while(end > begin && isSpace(text.charAt(end - 1))) {
                end--;
            }
        }
        return text.substring(begin, end);
    }

    private static boolean isSpace(final char c) {
        return c == ' ' || c == IDEOGRAPHIC_SPACE;
    }

    private interface Source {
        int length();
        String text(int start, int len);
        byte[] bytes(int start, int len);
    }

    private record ByteSource(byte[] buffer, int offset, int length, Charset charset) implements Source {

        @Override
        public String text(final int start, final int len) {
            return new String(buffer, offset + start, len, charset);
        }

        @Override
        public byte[] bytes(final int start, final int len) {
            return Arrays.copyOfRange(buffer, offset + start, offset + start + len);
        }

    }

    private static final class CharSource implements Source {

        private final int[] codePoints;
        private final Charset charset;

        CharSource(final String text, final Charset charset) {
            this.codePoints = text.codePoints().toArray();
            this.charset = charset;
        }

        @Override
        public int length() {
            return codePoints.length;
        }

        @Override
        public String text(final int start, final int len) {
            return new String(codePoints, start, len);
        }

        @Override
        public byte[] bytes(final int start, final int len) {
            return text(start, len).getBytes(charset);
        }

    }

}
