package com.mercari.solution.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Byte codec compatible with the wire format of {@code org.apache.hadoop.hbase.util.Bytes}
 * (big-endian primitives, UTF-8 strings, boolean true = 0xFF, BigDecimal = 4-byte scale + unscaled bytes),
 * without depending on hbase-common at runtime.
 * Compatibility with the original is verified against the real HBase Bytes in BigtableSchemaUtilTest.
 */
public class HBaseBytes {

    private HBaseBytes() {
    }

    public static byte[] toBytes(final boolean value) {
        return new byte[]{ value ? (byte) -1 : (byte) 0 };
    }

    public static byte[] toBytes(final String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] toBytes(final short value) {
        return new byte[]{
                (byte) (value >>> 8),
                (byte) value };
    }

    public static byte[] toBytes(final int value) {
        return new byte[]{
                (byte) (value >>> 24),
                (byte) (value >>> 16),
                (byte) (value >>> 8),
                (byte) value };
    }

    public static byte[] toBytes(final long value) {
        return new byte[]{
                (byte) (value >>> 56),
                (byte) (value >>> 48),
                (byte) (value >>> 40),
                (byte) (value >>> 32),
                (byte) (value >>> 24),
                (byte) (value >>> 16),
                (byte) (value >>> 8),
                (byte) value };
    }

    public static byte[] toBytes(final float value) {
        return toBytes(Float.floatToRawIntBits(value));
    }

    public static byte[] toBytes(final double value) {
        return toBytes(Double.doubleToRawLongBits(value));
    }

    public static byte[] toBytes(final BigDecimal value) {
        final byte[] unscaledBytes = value.unscaledValue().toByteArray();
        final byte[] scaleBytes = toBytes(value.scale());
        final byte[] result = new byte[scaleBytes.length + unscaledBytes.length];
        System.arraycopy(scaleBytes, 0, result, 0, scaleBytes.length);
        System.arraycopy(unscaledBytes, 0, result, scaleBytes.length, unscaledBytes.length);
        return result;
    }

    public static boolean toBoolean(final byte[] bytes) {
        checkLength(bytes, 1, "boolean");
        return bytes[0] != (byte) 0;
    }

    public static String toString(final byte[] bytes) {
        if(bytes == null) {
            return null;
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static short toShort(final byte[] bytes) {
        checkLength(bytes, 2, "short");
        return (short) (((bytes[0] & 0xFF) << 8)
                | (bytes[1] & 0xFF));
    }

    public static int toInt(final byte[] bytes) {
        checkLength(bytes, 4, "int");
        return ((bytes[0] & 0xFF) << 24)
                | ((bytes[1] & 0xFF) << 16)
                | ((bytes[2] & 0xFF) << 8)
                | (bytes[3] & 0xFF);
    }

    public static long toLong(final byte[] bytes) {
        checkLength(bytes, 8, "long");
        long value = 0L;
        for(int i = 0; i < 8; i++) {
            value = (value << 8) | (bytes[i] & 0xFFL);
        }
        return value;
    }

    public static float toFloat(final byte[] bytes) {
        checkLength(bytes, 4, "float");
        return Float.intBitsToFloat(toInt(bytes));
    }

    public static double toDouble(final byte[] bytes) {
        checkLength(bytes, 8, "double");
        return Double.longBitsToDouble(toLong(bytes));
    }

    public static BigDecimal toBigDecimal(final byte[] bytes) {
        // same as HBase Bytes.toBigDecimal: null (not an exception) for null or too-short input
        if(bytes == null || bytes.length < 5) {
            return null;
        }
        final int scale = toInt(Arrays.copyOfRange(bytes, 0, 4));
        final BigInteger unscaled = new BigInteger(Arrays.copyOfRange(bytes, 4, bytes.length));
        return new BigDecimal(unscaled, scale);
    }

    private static void checkLength(final byte[] bytes, final int expected, final String type) {
        if(bytes == null || bytes.length != expected) {
            throw new IllegalArgumentException("Failed to deserialize " + type
                    + ", array length must be " + expected + " but was: " + (bytes == null ? "null" : bytes.length));
        }
    }

}
