package com.mercari.solution.util.schema.fwf;

import org.apache.beam.sdk.util.SerializableUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * work_fixedwidth.md §4.4 conversion rules on a synthetic windows-31j record (no real data:
 * the full-width text, NEC special character ㈱, ideographic spaces and half-width kana exercise
 * the byte-position slicing).
 */
public class FwfDecoderTest {

    private static final Charset MS932 = Charset.forName("windows-31j");

    // pos: content
    //   1-8   key (region / year / seq / day(hex) / branch), also read whole as keyRaw
    //   9-28  name: 6 full-width chars + 4 ideographic spaces (20 bytes)
    //  29-33  amount " 12.3" (explicit decimal point)
    //  34-36  weight "550" (implied scale 1)
    //  37-42  diff "+12", diff2 "- 4"
    //  43-45  count blank (defaultValue 0)
    //  46-49  rate "0012" (float64, implied scale 1)
    //  50-65  orderDate "20240131", emptyDate "00000000" (nullIf)
    //  66-77  at "202401311530" (Asia/Tokyo)
    //  78     flag "1"
    //  79-84  marks "01  03" (int32 x3)
    //  85-106 items: 2 x { sku 4, qty 2, price 5 }, the second one empty
    // 107-108 raw "AB" (bytes)
    // 109-111 reserved (not declared)
    // 112-115 tail: half-width kana + spaces
    private static final String RECORD_TEXT = "05243a11"
            + "㈱テスト商店　　　　"
            + " 12.3" + "550" + "+12" + "- 4" + "   " + "0012"
            + "20240131" + "00000000" + "202401311530" + "1" + "01  03"
            + "A0010200150" + "           "
            + "AB" + "   " + "ｶﾅ  ";

    private static final String LAYOUT = """
            {
              "recordLength": 115,
              "fields": [
                { "name": "key", "pos": 1, "fields": [
                    { "name": "region", "type": "string", "len": 2 },
                    { "name": "year",   "type": "string", "len": 2 },
                    { "name": "seq",    "type": "int32",  "len": 1 },
                    { "name": "day",    "type": "string", "len": 1 },
                    { "name": "branch", "type": "int32",  "len": 2 } ] },
                { "name": "keyRaw", "type": "string", "pos": 1, "len": 8 },
                { "name": "dayNumber", "type": "int32", "pos": 6, "len": 1, "radix": 16 },
                { "name": "name",   "type": "string",  "pos": 9, "len": 20 },
                { "name": "amount", "type": "decimal", "len": 5 },
                { "name": "weight", "type": "decimal", "len": 3, "scale": 1 },
                { "name": "diff",   "type": "int32", "len": 3 },
                { "name": "diff2",  "type": "int32", "len": 3 },
                { "name": "count",  "type": "int32", "len": 3, "defaultValue": 0 },
                { "name": "rate",   "type": "float64", "len": 4, "scale": 1 },
                { "name": "orderDate", "type": "date", "len": 8, "pattern": "yyyyMMdd" },
                { "name": "emptyDate", "type": "date", "len": 8, "pattern": "yyyyMMdd", "nullIf": ["00000000"] },
                { "name": "at",   "type": "timestamp", "len": 12, "pattern": "yyyyMMddHHmm", "zone": "Asia/Tokyo" },
                { "name": "flag", "type": "bool", "len": 1 },
                { "name": "marks", "type": "int32", "len": 2, "repeat": 3 },
                { "name": "items", "repeat": 2, "fields": [
                    { "name": "sku",   "type": "string", "len": 4 },
                    { "name": "qty",   "type": "int32",  "len": 2 },
                    { "name": "price", "type": "int64",  "len": 5 } ] },
                { "name": "raw",  "type": "bytes",  "len": 2 },
                { "name": "tail", "type": "string", "pos": 112, "len": 4 }
              ]
            }
            """;

    private static byte[] record() {
        final byte[] bytes = RECORD_TEXT.getBytes(MS932);
        Assertions.assertEquals(115, bytes.length, "synthetic record length");
        return bytes;
    }

    private static FwfDecoder decoder(final Map<String, String> options) {
        return FwfDecoder.of(FwfLayout.parse(LAYOUT), FwfOptions.of(options));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testDecodeAllTypes() {
        final Map<String, Object> values = decoder(Map.of("charset", "windows-31j")).decode(record());

        final Map<String, Object> key = (Map<String, Object>) values.get("key");
        Assertions.assertEquals("05", key.get("region"));
        Assertions.assertEquals("24", key.get("year"));
        Assertions.assertEquals(3, key.get("seq"));
        Assertions.assertEquals("a", key.get("day"));
        Assertions.assertEquals(11, key.get("branch"));
        Assertions.assertEquals("05243a11", values.get("keyRaw"));
        Assertions.assertEquals(10, values.get("dayNumber"));

        // ideographic spaces are trimmed; the NEC special character survives with windows-31j
        Assertions.assertEquals("㈱テスト商店", values.get("name"));
        Assertions.assertEquals(new BigDecimal("12.3"), values.get("amount"));
        Assertions.assertEquals(new BigDecimal("55.0"), values.get("weight"));
        Assertions.assertEquals(12, values.get("diff"));
        Assertions.assertEquals(-4, values.get("diff2"));
        Assertions.assertEquals(0, values.get("count"));
        Assertions.assertEquals(1.2, (Double) values.get("rate"), 1e-9);
        Assertions.assertEquals((int) LocalDate.of(2024, 1, 31).toEpochDay(), values.get("orderDate"));
        Assertions.assertNull(values.get("emptyDate"));
        final long expectedAt = OffsetDateTime.of(2024, 1, 31, 15, 30, 0, 0, ZoneOffset.ofHours(9))
                .toInstant().toEpochMilli() * 1000L;
        Assertions.assertEquals(expectedAt, values.get("at"));
        Assertions.assertEquals(true, values.get("flag"));
        // repeated values keep their positions: the blank element stays null
        Assertions.assertEquals(Arrays.asList(1, null, 3), values.get("marks"));

        final List<Map<String, Object>> items = (List<Map<String, Object>>) values.get("items");
        Assertions.assertEquals(2, items.size());
        Assertions.assertEquals("A001", items.get(0).get("sku"));
        Assertions.assertEquals(2, items.get(0).get("qty"));
        Assertions.assertEquals(150L, items.get(0).get("price"));
        Assertions.assertNull(items.get(1).get("sku"));
        Assertions.assertNull(items.get(1).get("qty"));
        Assertions.assertNull(items.get(1).get("price"));

        Assertions.assertEquals(ByteBuffer.wrap("AB".getBytes(StandardCharsets.US_ASCII)), values.get("raw"));
        Assertions.assertEquals("ｶﾅ", values.get("tail"));
    }

    @Test
    public void testStrictShiftJisLosesNecSpecialCharacter() {
        // why the docs require windows-31j: strict Shift_JIS has no mapping for ㈱ (0x878A)
        final Map<String, Object> values = decoder(Map.of("charset", "Shift_JIS")).decode(record());
        Assertions.assertNotEquals("㈱テスト商店", values.get("name"));
        // positions are bytes, so the following fields are unaffected
        Assertions.assertEquals(new BigDecimal("12.3"), values.get("amount"));
    }

    @Test
    public void testDecodeStringInByteUnit() {
        final String text = new String(record(), MS932);
        final Map<String, Object> values = decoder(Map.of("charset", "windows-31j")).decode(text);
        Assertions.assertEquals("㈱テスト商店", values.get("name"));
        Assertions.assertEquals("ｶﾅ", values.get("tail"));
    }

    @Test
    public void testLengthMismatch() {
        final byte[] shorter = Arrays.copyOf(record(), 114);
        final FwfException e = Assertions.assertThrows(FwfException.class,
                () -> decoder(Map.of("charset", "windows-31j")).decode(shorter));
        Assertions.assertTrue(e.getMessage().contains("record length 114"), e.getMessage());
        Assertions.assertNull(e.getField());

        // pad: a range beyond the record end is null, the rest is decoded
        final Map<String, Object> values = decoder(Map.of("charset", "windows-31j", "onLengthMismatch", "pad")).decode(shorter);
        Assertions.assertNull(values.get("tail"));
        Assertions.assertEquals("㈱テスト商店", values.get("name"));

        // pad: a longer record ignores the excess
        final byte[] longer = Arrays.copyOf(record(), 120);
        Arrays.fill(longer, 115, 120, (byte) 'X');
        final Map<String, Object> longerValues = decoder(Map.of("charset", "windows-31j", "onLengthMismatch", "pad")).decode(longer);
        Assertions.assertEquals("ｶﾅ", longerValues.get("tail"));
    }

    @Test
    public void testParseError() {
        final byte[] broken = record();
        broken[29] = 'x'; // amount " 12.3" -> " x2.3"
        broken[80] = 'x'; // marks[1] "  " -> " x"
        final FwfException e = Assertions.assertThrows(FwfException.class,
                () -> decoder(Map.of("charset", "windows-31j")).decode(broken));
        Assertions.assertEquals("amount", e.getField());
        Assertions.assertEquals(" x2.3", e.getValue());

        final Map<String, Object> values = decoder(Map.of("charset", "windows-31j", "onParseError", "null")).decode(broken);
        Assertions.assertNull(values.get("amount"));
        Assertions.assertEquals(Arrays.asList(1, null, 3), values.get("marks"));
        Assertions.assertEquals(new BigDecimal("55.0"), values.get("weight"));
    }

    @Test
    public void testParseErrorPathInRepeatedGroup() {
        final FwfLayout layout = FwfLayout.parse("""
                { "fields": [ { "name": "items", "repeat": 2, "fields": [
                    { "name": "sku", "type": "string", "len": 2 },
                    { "name": "qty", "type": "int32", "len": 2 } ] } ] }
                """);
        final FwfException e = Assertions.assertThrows(FwfException.class,
                () -> FwfDecoder.of(layout, null).decode("A101B1x2".getBytes(StandardCharsets.UTF_8)));
        Assertions.assertEquals("items[1].qty", e.getField());
    }

    @Test
    public void testRequiredAndTextOptions() {
        final FwfLayout layout = FwfLayout.parse("""
                { "fields": [
                    { "name": "id",    "type": "int32",  "len": 3, "mode": "required" },
                    { "name": "blank", "type": "string", "len": 3 },
                    { "name": "keep",  "type": "string", "len": 6, "trim": "none" },
                    { "name": "right", "type": "string", "len": 6, "trim": "right" } ] }
                """);
        final byte[] ok = ("  1" + "   " + "ab    " + "  ab  ").getBytes(StandardCharsets.UTF_8);
        final Map<String, Object> values = FwfDecoder.of(layout, null).decode(ok);
        Assertions.assertEquals(1, values.get("id"));
        Assertions.assertNull(values.get("blank"));
        Assertions.assertEquals("ab    ", values.get("keep"));
        Assertions.assertEquals("  ab", values.get("right"));

        final Map<String, Object> keepEmpty = FwfDecoder.of(layout, FwfOptions.of(Map.of("emptyAsNull", "false"))).decode(ok);
        Assertions.assertEquals("", keepEmpty.get("blank"));

        final byte[] missingId = ("   " + "   " + "ab    " + "  ab  ").getBytes(StandardCharsets.UTF_8);
        final FwfException e = Assertions.assertThrows(FwfException.class, () -> FwfDecoder.of(layout, null).decode(missingId));
        Assertions.assertEquals("id", e.getField());
    }

    @Test
    public void testCharUnit() {
        final FwfLayout layout = FwfLayout.parse("""
                { "recordLength": 6, "fields": [
                    { "name": "name",  "type": "string", "len": 3 },
                    { "name": "count", "type": "int32",  "len": 2 },
                    { "name": "flag",  "type": "bool",   "len": 1 } ] }
                """);
        // UTF-8 text whose layout counts characters, not bytes
        final byte[] bytes = "あいう12Y".getBytes(StandardCharsets.UTF_8);
        final FwfDecoder decoder = FwfDecoder.of(layout, FwfOptions.of(Map.of("unit", "char")));
        final Map<String, Object> values = decoder.decode(bytes);
        Assertions.assertEquals("あいう", values.get("name"));
        Assertions.assertEquals(12, values.get("count"));
        Assertions.assertEquals(true, values.get("flag"));
        Assertions.assertEquals(values, decoder.decode("あいう12Y"));
    }

    @Test
    public void testDecodeSlice() {
        final byte[] buffer = ("XX" + RECORD_TEXT + "YY").getBytes(MS932);
        final Map<String, Object> values = decoder(Map.of("charset", "windows-31j")).decode(buffer, 2, 115);
        Assertions.assertEquals("05243a11", values.get("keyRaw"));
        Assertions.assertEquals("ｶﾅ", values.get("tail"));
    }

    @Test
    public void testSerializable() {
        final FwfDecoder decoder = SerializableUtils.ensureSerializable(decoder(Map.of("charset", "windows-31j")));
        Assertions.assertEquals((int) LocalDate.of(2024, 1, 31).toEpochDay(), decoder.decode(record()).get("orderDate"));
    }

    @Test
    public void testStrip() {
        Assertions.assertEquals("a b", FwfDecoder.strip(" 　a b　 ", FwfOptions.Trim.both));
        Assertions.assertEquals("a b　 ", FwfDecoder.strip(" 　a b　 ", FwfOptions.Trim.left));
        Assertions.assertEquals(" 　a b", FwfDecoder.strip(" 　a b　 ", FwfOptions.Trim.right));
        Assertions.assertEquals(" a ", FwfDecoder.strip(" a ", FwfOptions.Trim.none));
    }

}
