package com.mercari.solution.util.pipeline.select;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.schema.fwf.FwfException;
import org.apache.beam.sdk.util.SerializableUtils;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The select function {@code fwf_decode} (work_fixedwidth.md §7): one fixed-width record in a bytes
 * or string field becomes a record value. Synthetic windows-31j data.
 */
public class FwfDecodeTest {

    private static final Charset MS932 = Charset.forName("windows-31j");

    // code (4) + name (10 bytes, full-width) + amount (5, implied scale 1) + items: 2 x { sku (2), qty (2) }
    private static final String RECORD = "A001" + "㈱東京　　" + "00123" + "S103" + "S2  ";

    private static final String SCHEMA = """
            {
              "encoding": { "format": "fwf", "charset": "windows-31j" },
              "reference": { "inline": {
                "recordLength": 27,
                "fields": [
                  { "name": "code",   "type": "string",  "len": 4 },
                  { "name": "name",   "type": "string",  "len": 10 },
                  { "name": "amount", "type": "decimal", "len": 5, "scale": 1 },
                  { "name": "items", "repeat": 2, "fields": [
                      { "name": "sku", "type": "string", "len": 2 },
                      { "name": "qty", "type": "int32",  "len": 2 } ] }
                ] } }
            }
            """;

    private static List<SelectFunction> functions(final String select, final Schema.FieldType payloadType) {
        final JsonArray array = new Gson().fromJson(select, JsonArray.class);
        final List<SelectFunction> functions = SelectFunction.of(array, List.of(Schema.Field.of("payload", payloadType)));
        functions.forEach(SelectFunction::setup);
        return functions;
    }

    private static Map<String, Object> apply(final List<SelectFunction> functions, final Object payload) {
        final Map<String, Object> input = new HashMap<>();
        input.put("payload", payload);
        return SelectFunction.apply(functions, input, Instant.EPOCH);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testDecodeBytesAndString() {
        final List<SelectFunction> functions = functions("""
                [ { "name": "order", "func": "fwf_decode", "field": "payload", "schema": %s } ]
                """.formatted(SCHEMA), Schema.FieldType.BYTES);

        // the output type is the record of the layout
        final Schema.FieldType outputType = functions.getFirst().getOutputFieldType();
        Assertions.assertEquals(Schema.Type.element, outputType.getType());
        Assertions.assertEquals(List.of("code", "name", "amount", "items"),
                outputType.getElementSchema().getFields().stream().map(Schema.Field::getName).toList());
        Assertions.assertEquals(Schema.Type.array, outputType.getElementSchema().getField("items").getFieldType().getType());

        final byte[] bytes = RECORD.getBytes(MS932);
        Assertions.assertEquals(27, bytes.length);
        final Map<String, Object> fromArray = (Map<String, Object>) apply(functions, bytes).get("order");
        Assertions.assertEquals("A001", fromArray.get("code"));
        Assertions.assertEquals("㈱東京", fromArray.get("name"));
        Assertions.assertEquals(new BigDecimal("12.3"), fromArray.get("amount"));
        final List<Map<String, Object>> items = (List<Map<String, Object>>) fromArray.get("items");
        Assertions.assertEquals(Map.of("sku", "S1", "qty", 3), items.get(0));
        Assertions.assertEquals("S2", items.get(1).get("sku"));
        Assertions.assertNull(items.get(1).get("qty"));

        // a ByteBuffer that is a slice of a larger array
        final byte[] padded = ("XX" + RECORD + "YY").getBytes(MS932);
        final ByteBuffer slice = ByteBuffer.wrap(padded, 2, 27).slice();
        Assertions.assertEquals(fromArray, apply(functions, slice).get("order"));
        Assertions.assertEquals(fromArray, apply(functions, ByteBuffer.wrap(bytes).asReadOnlyBuffer()).get("order"));

        // text is encoded back with the charset of the schema before it is cut by bytes
        Assertions.assertEquals(fromArray, apply(functions, RECORD).get("order"));

        Assertions.assertNull(apply(functions, null).get("order"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testProjectionAndDefaultField() {
        // field defaults to the name; fields narrows the record
        final List<SelectFunction> functions = functions("""
                [ { "name": "payload", "func": "fwf_decode", "fields": ["amount", "code"], "schema": %s } ]
                """.formatted(SCHEMA), Schema.FieldType.STRING);
        Assertions.assertEquals(List.of("amount", "code"),
                functions.getFirst().getOutputFieldType().getElementSchema().getFields().stream().map(Schema.Field::getName).toList());
        final Map<String, Object> order = (Map<String, Object>) apply(functions, RECORD).get("payload");
        Assertions.assertEquals(Map.of("amount", new BigDecimal("12.3"), "code", "A001"), order);
    }

    @Test
    public void testUndecodableRecordThrows() {
        final List<SelectFunction> functions = functions("""
                [ { "name": "order", "func": "fwf_decode", "field": "payload", "schema": %s } ]
                """.formatted(SCHEMA), Schema.FieldType.BYTES);
        final FwfException shortRecord = Assertions.assertThrows(FwfException.class, () -> apply(functions, "SHORT".getBytes(MS932)));
        Assertions.assertTrue(shortRecord.getMessage().contains("record length 5"), shortRecord.getMessage());

        final byte[] broken = RECORD.getBytes(MS932);
        broken[15] = 'x';
        final FwfException badAmount = Assertions.assertThrows(FwfException.class, () -> apply(functions, broken));
        Assertions.assertEquals("amount", badAmount.getField());
    }

    @Test
    public void testSerializable() {
        final List<SelectFunction> functions = functions("""
                [ { "name": "order", "func": "fwf_decode", "field": "payload", "schema": %s } ]
                """.formatted(SCHEMA), Schema.FieldType.BYTES);
        final SelectFunction function = SerializableUtils.ensureSerializable(functions.getFirst());
        Assertions.assertNotNull(function.apply(new HashMap<>(Map.of("payload", RECORD.getBytes(MS932))), Instant.EPOCH));
    }

    @Test
    public void testValidation() {
        assertInvalid("requires schema parameter", """
                [ { "name": "order", "func": "fwf_decode", "field": "payload" } ]
                """, Schema.FieldType.BYTES);
        assertInvalid("schema must declare encoding.format: fwf", """
                [ { "name": "order", "func": "fwf_decode", "field": "payload",
                    "schema": { "fields": [ { "name": "a", "type": "string" } ] } } ]
                """, Schema.FieldType.BYTES);
        assertInvalid("must be bytes or string. but: int64", """
                [ { "name": "order", "func": "fwf_decode", "field": "payload", "schema": %s } ]
                """.formatted(SCHEMA), Schema.FieldType.INT64);
        assertInvalid("Not found field: nothing", """
                [ { "name": "order", "func": "fwf_decode", "field": "nothing", "schema": %s } ]
                """.formatted(SCHEMA), Schema.FieldType.BYTES);
        assertInvalid("order.fields: ", """
                [ { "name": "order", "func": "fwf_decode", "field": "payload", "fields": ["nothing"], "schema": %s } ]
                """.formatted(SCHEMA), Schema.FieldType.BYTES);
        assertInvalid("order.schema: ", """
                [ { "name": "order", "func": "fwf_decode", "field": "payload",
                    "schema": { "encoding": { "format": "fwf" }, "reference": { "inline": { "fields": [ { "name": "a", "offset": 1, "len": 1 } ] } } } } ]
                """, Schema.FieldType.BYTES);
    }

    private static void assertInvalid(final String expected, final String select, final Schema.FieldType payloadType) {
        final IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class, () -> functions(select, payloadType));
        Assertions.assertTrue(e.getMessage().contains(expected), "expected '" + expected + "' in: " + e.getMessage());
    }

}
