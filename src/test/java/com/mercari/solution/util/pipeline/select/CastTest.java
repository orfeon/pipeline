package com.mercari.solution.util.pipeline.select;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The select function {@code cast} from a decimal. A decimal has no single representation: a
 * {@link BigDecimal} (the json / fwf decoders), its text once the element has passed the map coder,
 * or the bytes of the unscaled value when it is read from an avro record (scale 9). The result of
 * the cast must not depend on which of them arrives.
 */
public class CastTest {

    private static final Instant TIMESTAMP = Instant.parse("2024-01-01T00:00:00Z");

    private static Object cast(final String type, final Object value) {
        final JsonObject jsonObject = new Gson().fromJson(
                "{ \"name\": \"out\", \"field\": \"amount\", \"type\": \"" + type + "\" }", JsonObject.class);
        final SelectFunction function = SelectFunction.of(jsonObject, List.of(Schema.Field.of("amount", Schema.FieldType.DECIMAL)));
        function.setup();
        final Map<String, Object> input = new HashMap<>();
        input.put("amount", value);
        return function.apply(input, TIMESTAMP);
    }

    // the three representations of the same decimal
    private static List<Object> representations(final String number) {
        final BigDecimal decimal = new BigDecimal(number);
        return List.of(
                decimal,
                decimal.toString(),
                ByteBuffer.wrap(decimal.setScale(9).unscaledValue().toByteArray()));
    }

    @Test
    public void testDecimalToNumbers() {
        for(final Object value : representations("12.30")) {
            final String label = value.getClass().getSimpleName();
            Assertions.assertEquals(12.3d, cast("float64", value), label);
            Assertions.assertEquals(12.3f, cast("float32", value), label);
            // a text such as "12.30" is not parsed as an integer: the decimal is truncated like the other two
            Assertions.assertEquals(12L, cast("int64", value), label);
            Assertions.assertEquals(12, cast("int32", value), label);
            Assertions.assertEquals(true, cast("bool", value), label);
        }
        for(final Object value : representations("-0.5")) {
            Assertions.assertEquals(-0.5d, cast("float64", value), value.getClass().getSimpleName());
            Assertions.assertEquals(false, cast("bool", value), value.getClass().getSimpleName());
        }
        Assertions.assertNull(cast("float64", null));
    }

    @Test
    public void testDecimalOfAnotherScale() {
        // the bytes of a decimal(10,2) read from avro are at scale 2: 1234 is 12.34, not 0.000001234
        final Schema.FieldType decimal = Schema.FieldType.decimal(10, 2);
        final com.google.gson.JsonArray select = new com.google.gson.Gson().fromJson("""
                [ { "name": "asDouble", "field": "price", "type": "float64" },
                  { "name": "asText", "field": "price", "type": "string" } ]
                """, com.google.gson.JsonArray.class);
        final java.util.List<SelectFunction> functions = SelectFunction.of(select, java.util.List.of(Schema.Field.of("price", decimal)));
        final java.util.Map<String, Object> input = new java.util.HashMap<>();
        input.put("price", java.nio.ByteBuffer.wrap(new java.math.BigInteger("1234").toByteArray()));
        final java.util.Map<String, Object> output = SelectFunction.apply(functions, input, org.joda.time.Instant.EPOCH);
        Assertions.assertEquals(12.34, (Double) output.get("asDouble"), 1e-12);
        Assertions.assertEquals("12.34", output.get("asText"));
    }

    @Test
    public void testDecimalToString() {
        // plain notation without trailing zeros, whatever arrives
        for(final Object value : representations("12.30")) {
            Assertions.assertEquals("12.3", cast("string", value), value.getClass().getSimpleName());
        }
        for(final Object value : representations("100")) {
            Assertions.assertEquals("100", cast("string", value), value.getClass().getSimpleName());
        }
        // BigDecimal.toString gives "1E-7" and "1E+3" for these
        for(final Object value : representations("0.0000001")) {
            Assertions.assertEquals("0.0000001", cast("string", value), value.getClass().getSimpleName());
        }
        Assertions.assertEquals("1000", cast("string", new BigDecimal("1E+3")));
        Assertions.assertEquals("1000", cast("string", "1E+3"));
        for(final Object value : representations("0.000")) {
            Assertions.assertEquals("0", cast("string", value), value.getClass().getSimpleName());
        }
        Assertions.assertNull(cast("string", null));
    }

    @Test
    public void testDecimalBytes() {
        // a slice of a larger array, as a reused avro buffer is: only its remaining bytes are the value
        final byte[] unscaled = new BigInteger("12300000000").toByteArray();
        final byte[] padded = new byte[unscaled.length + 3];
        System.arraycopy(unscaled, 0, padded, 1, unscaled.length);
        padded[0] = 0x7f;
        Assertions.assertEquals(12.3d, cast("float64", ByteBuffer.wrap(padded, 1, unscaled.length)));
        Assertions.assertEquals(12.3d, cast("float64", ByteBuffer.wrap(unscaled).asReadOnlyBuffer()));
        Assertions.assertEquals(12.3d, cast("float64", unscaled));

        // a cast to bytes keeps the bytes as they are
        final ByteBuffer bytes = ByteBuffer.wrap(unscaled);
        Assertions.assertSame(bytes, cast("bytes", bytes));
    }

}
