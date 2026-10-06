package com.mercari.solution.util.schema.fwf;

import com.mercari.solution.module.Schema;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
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
 *
 * <p>Bytes that the charset can not decode (malformed, or without a mapping such as a NEC special
 * character under strict Shift_JIS) are never replaced with U+FFFD: with {@code unit: byte} they are
 * a parse error of that field (onParseError), with {@code unit: char} the record fails as a whole.
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

    /**
     * The decoder of a schema declared with {@code encoding.format: fwf} — the one entry point for
     * modules, so that a projection is never forgotten: the layout is narrowed to the schema's
     * fields (a declared {@code schema.fields} projection; all layout fields when omitted) and then
     * to {@code fields} when given (a module-level projection such as storage's
     * {@code parameters.fields}). {@link #getLayout()} of the result is the projected layout, whose
     * {@link FwfLayout#toSchemaFields()} are the fields the decoder outputs.
     */
    public static FwfDecoder of(final Schema schema, final List<String> fields) {
        if(schema == null || schema.getFwfLayout() == null) {
            throw new IllegalArgumentException("schema does not declare encoding.format: fwf with a layout (schema.reference)");
        }
        FwfLayout layout = schema.getFwfLayout()
                .project(schema.getFields().stream().map(Schema.Field::getName).toList());
        if(fields != null && !fields.isEmpty()) {
            layout = layout.project(fields);
        }
        return new FwfDecoder(layout, schema.getFwfOptions());
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
            case byte_ -> new ByteSource(buffer, offset, length, strictDecoder());
            case char_ -> {
                // positions count characters, so the record has to be decoded as a whole
                try {
                    yield new CharSource(strictDecoder().decode(ByteBuffer.wrap(buffer, offset, length)).toString(), charset());
                } catch (final CharacterCodingException e) {
                    throw new FwfException("record can not be decoded with charset " + options.getCharsetName()
                            + " (malformed or unmappable bytes)");
                }
            }
        };
        return decode(source);
    }

    /**
     * Decodes a record held as text. With {@code unit: byte} the text is encoded back with the
     * charset before cutting (lossless when it was decoded from the same charset); a character the
     * charset can not encode fails the record instead of being replaced with {@code ?}.
     */
    public Map<String, Object> decode(final String record) {
        return switch (options.getUnit()) {
            case byte_ -> {
                final ByteBuffer encoded;
                try {
                    encoded = charset().newEncoder()
                            .onMalformedInput(CodingErrorAction.REPORT)
                            .onUnmappableCharacter(CodingErrorAction.REPORT)
                            .encode(CharBuffer.wrap(record));
                } catch (final CharacterCodingException e) {
                    throw new FwfException("record can not be encoded with charset " + options.getCharsetName()
                            + " (it has characters outside the charset)");
                }
                yield decode(encoded.array(), encoded.arrayOffset() + encoded.position(), encoded.remaining());
            }
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
        return decodeFields(layout.getFields(), source, 0);
    }

    private Map<String, Object> decodeFields(
            final List<FwfLayout.Field> fields,
            final Source source,
            final int base) {

        final Map<String, Object> values = new HashMap<>(Math.max(16, fields.size() * 2));
        for(final FwfLayout.Field field : fields) {
            final int start = base + field.getStart();
            if(field.getRepeat() == null) {
                values.put(field.getName(), decodeElement(field, source, start, -1));
            } else {
                final List<Object> list = new ArrayList<>(field.getRepeat());
                for(int i = 0; i < field.getRepeat(); i++) {
                    list.add(decodeElement(field, source, start + i * field.unit(), i));
                }
                values.put(field.getName(), list);
            }
        }
        return values;
    }

    /**
     * @param index position in a repeated field; -1 when the field is not repeated. Together with
     *              the field it names the failing value ({@link #path}); the path of a nested value
     *              is completed by the enclosing groups only when a record fails.
     */
    private Object decodeElement(
            final FwfLayout.Field field,
            final Source source,
            final int start,
            final int index) {

        if(field.isGroup()) {
            try {
                return decodeFields(field.getChildren(), source, start);
            } catch (final FwfException e) {
                throw e.within(path(field, index));
            }
        }

        Object value = null;
        String raw = null;
        // a range beyond the record end (onLengthMismatch: pad, or no recordLength) is null
        if(start + field.getLen() <= source.length()) {
            if(field.getType() == Schema.Type.bytes) {
                value = ByteBuffer.wrap(source.bytes(start, field.getLen()));
            } else {
                try {
                    raw = source.text(start, field.getLen());
                    value = convert(field, raw, index);
                } catch (final CharacterCodingException e) {
                    // bytes outside the charset are a parse error of this field, not a silent U+FFFD
                    if(options.getOnParseError() == FwfOptions.OnParseError.fail) {
                        throw new FwfException(path(field, index),
                                "0x" + HexFormat.of().formatHex(source.bytes(start, field.getLen())),
                                "can not decode with charset " + options.getCharsetName() + " (malformed or unmappable bytes)", e);
                    }
                }
            }
        }
        if(value == null && field.getDefaultValue() != null) {
            value = field.getDefaultValue();
        }
        if(value == null && field.isRequired()) {
            throw new FwfException(path(field, index), raw, "required field is null", null);
        }
        return value;
    }

    private static String path(final FwfLayout.Field field, final int index) {
        return index < 0 ? field.getName() : field.getName() + "[" + index + "]";
    }

    private Object convert(final FwfLayout.Field field, final String raw, final int index) {
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
            throw new FwfException(path(field, index), raw, "can not convert to " + field.getType() + ": " + e.getMessage(), e);
        }
    }

    private Charset charset() {
        if(charset == null) {
            charset = options.getCharset();
        }
        return charset;
    }

    // one decoder per record: a CharsetDecoder is not thread-safe and this decoder may be shared
    private CharsetDecoder strictDecoder() {
        return charset().newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
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
        String text(int start, int len) throws CharacterCodingException;
        byte[] bytes(int start, int len);
    }

    private record ByteSource(byte[] buffer, int offset, int length, CharsetDecoder decoder) implements Source {

        @Override
        public String text(final int start, final int len) throws CharacterCodingException {
            return decoder.decode(ByteBuffer.wrap(buffer, offset + start, len)).toString();
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
