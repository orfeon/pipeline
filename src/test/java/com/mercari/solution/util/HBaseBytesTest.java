package com.mercari.solution.util;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

/**
 * Verifies that HBaseBytes stays byte-compatible with the real
 * {@code org.apache.hadoop.hbase.util.Bytes} (test-scoped hbase-common),
 * so that data already stored in Bigtable by previous versions remains readable.
 */
public class HBaseBytesTest {

    @Test
    public void testEncodeCompatibility() {
        Assertions.assertArrayEquals(Bytes.toBytes(true), HBaseBytes.toBytes(true));
        Assertions.assertArrayEquals(Bytes.toBytes(false), HBaseBytes.toBytes(false));

        for(final String value : new String[]{"", "hello", "こんにちは", "emoji😀"}) {
            Assertions.assertArrayEquals(Bytes.toBytes(value), HBaseBytes.toBytes(value));
        }

        for(final short value : new short[]{Short.MIN_VALUE, -1, 0, 1, 42, Short.MAX_VALUE}) {
            Assertions.assertArrayEquals(Bytes.toBytes(value), HBaseBytes.toBytes(value));
        }
        for(final int value : new int[]{Integer.MIN_VALUE, -1, 0, 1, 42, Integer.MAX_VALUE}) {
            Assertions.assertArrayEquals(Bytes.toBytes(value), HBaseBytes.toBytes(value));
        }
        for(final long value : new long[]{Long.MIN_VALUE, -1L, 0L, 1L, 42L, Long.MAX_VALUE}) {
            Assertions.assertArrayEquals(Bytes.toBytes(value), HBaseBytes.toBytes(value));
        }
        for(final float value : new float[]{Float.MIN_VALUE, -1.5F, -0.0F, 0.0F, 1.5F, Float.MAX_VALUE, Float.NaN, Float.POSITIVE_INFINITY}) {
            Assertions.assertArrayEquals(Bytes.toBytes(value), HBaseBytes.toBytes(value));
        }
        for(final double value : new double[]{Double.MIN_VALUE, -2.5D, -0.0D, 0.0D, 2.5D, Double.MAX_VALUE, Double.NaN, Double.NEGATIVE_INFINITY}) {
            Assertions.assertArrayEquals(Bytes.toBytes(value), HBaseBytes.toBytes(value));
        }

        for(final BigDecimal value : new BigDecimal[]{
                BigDecimal.ZERO, BigDecimal.ONE, new BigDecimal("1.23"), new BigDecimal("-1.23"),
                new BigDecimal("123456789012345678901234567890.123456789"), new BigDecimal("1E+10"), new BigDecimal("0.00000001")}) {
            Assertions.assertArrayEquals(Bytes.toBytes(value), HBaseBytes.toBytes(value));
        }
    }

    @Test
    public void testDecodeCompatibility() {
        Assertions.assertEquals(Bytes.toBoolean(Bytes.toBytes(true)), HBaseBytes.toBoolean(Bytes.toBytes(true)));
        Assertions.assertEquals(Bytes.toBoolean(Bytes.toBytes(false)), HBaseBytes.toBoolean(Bytes.toBytes(false)));
        Assertions.assertEquals("こんにちは", HBaseBytes.toString(Bytes.toBytes("こんにちは")));
        Assertions.assertNull(HBaseBytes.toString(null));

        Assertions.assertEquals((short) -12345, HBaseBytes.toShort(Bytes.toBytes((short) -12345)));
        Assertions.assertEquals(-123456789, HBaseBytes.toInt(Bytes.toBytes(-123456789)));
        Assertions.assertEquals(-1234567890123456789L, HBaseBytes.toLong(Bytes.toBytes(-1234567890123456789L)));
        Assertions.assertEquals(-1.5F, HBaseBytes.toFloat(Bytes.toBytes(-1.5F)));
        Assertions.assertEquals(-2.5D, HBaseBytes.toDouble(Bytes.toBytes(-2.5D)));
        Assertions.assertEquals(new BigDecimal("-12.345"), HBaseBytes.toBigDecimal(Bytes.toBytes(new BigDecimal("-12.345"))));

        // same as HBase Bytes: null for null or too-short BigDecimal input
        Assertions.assertNull(HBaseBytes.toBigDecimal(null));
        Assertions.assertNull(HBaseBytes.toBigDecimal(new byte[]{1, 2, 3, 4}));

        // same as HBase Bytes: wrong length is an IllegalArgumentException
        Assertions.assertThrows(IllegalArgumentException.class, () -> HBaseBytes.toBoolean(new byte[]{1, 2}));
        Assertions.assertThrows(IllegalArgumentException.class, () -> HBaseBytes.toShort(new byte[]{1}));
        Assertions.assertThrows(IllegalArgumentException.class, () -> HBaseBytes.toInt(new byte[]{1, 2, 3}));
        Assertions.assertThrows(IllegalArgumentException.class, () -> HBaseBytes.toLong(new byte[]{1, 2, 3, 4}));
        Assertions.assertThrows(IllegalArgumentException.class, () -> HBaseBytes.toFloat(new byte[]{1}));
        Assertions.assertThrows(IllegalArgumentException.class, () -> HBaseBytes.toDouble(new byte[]{1}));
    }

}
