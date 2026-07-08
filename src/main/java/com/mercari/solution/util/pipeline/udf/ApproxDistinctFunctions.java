package com.mercari.solution.util.pipeline.udf;

import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.Function;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.impl.AggregateFunctionImpl;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * {@code APPROX_DISTINCT} — a fixed-memory approximate distinct count
 * (HyperLogLog; ClickHouse {@code uniqCombined} territory), always registered
 * as a built-in. Not named {@code APPROX_COUNT_DISTINCT}: that name exists in
 * Calcite's standard operator table, and a same-name user aggregate makes the
 * validator's overload-precedence filter assert on the ANY parameter.
 *
 * <pre>{@code
 * SELECT TIME_BUCKET(ts, 300000) AS bucket, APPROX_DISTINCT(userId) AS users
 * FROM input GROUP BY TIME_BUCKET(ts, 300000)
 * }</pre>
 *
 * <p>{@code APPROX_DISTINCT(value)} → {@code BIGINT}. Dense HLL with 2^12
 * registers: ~4&nbsp;KB per group, standard error ≈ 1.6&nbsp;%; small
 * cardinalities fall back to linear counting (exact in practice for tiny
 * sets). NULLs are ignored (SQL {@code COUNT(DISTINCT)} semantics); an empty
 * group returns 0. Values are hashed by SQL value semantics per type (strings,
 * integers, doubles, booleans; anything else hashes via {@code hashCode} — an
 * acceptable fallback since SQL guarantees one value type per call site).
 * Unlike exact {@code COUNT(DISTINCT x)}, memory does not grow with the
 * number of distinct values.
 */
public class ApproxDistinctFunctions {

    private ApproxDistinctFunctions() {
    }

    private static final int PRECISION = 12;
    private static final int REGISTERS = 1 << PRECISION; // 4096
    private static final double ALPHA = 0.7213 / (1 + 1.079 / REGISTERS);

    /** The dense HLL register file; public because generated code manipulates it. */
    public static class Acc {
        public final byte[] registers = new byte[REGISTERS];
    }

    /** The accumulator host. */
    public static class Host {
        public Acc init() {
            return new Acc();
        }

        public Acc add(Acc acc, Object value) {
            if (value == null) {
                return acc;
            }
            long hash = hash64(value);
            int index = (int) (hash >>> (64 - PRECISION));
            long remainder = hash << PRECISION;
            // Rank = leading zeros of the remaining bits + 1, saturating when
            // the remainder is all zeros.
            int rank = remainder == 0
                    ? 64 - PRECISION + 1
                    : Long.numberOfLeadingZeros(remainder) + 1;
            if (rank > acc.registers[index]) {
                acc.registers[index] = (byte) rank;
            }
            return acc;
        }

        public Acc merge(Acc a, Acc b) {
            for (int i = 0; i < REGISTERS; i++) {
                if (b.registers[i] > a.registers[i]) {
                    a.registers[i] = b.registers[i];
                }
            }
            return a;
        }

        public Long result(Acc acc) {
            double sum = 0;
            int zeros = 0;
            for (byte register : acc.registers) {
                sum += Math.pow(2, -register);
                if (register == 0) {
                    zeros++;
                }
            }
            double estimate = ALPHA * REGISTERS * REGISTERS / sum;
            if (estimate <= 2.5 * REGISTERS && zeros > 0) {
                // Small-range correction: linear counting.
                estimate = REGISTERS * Math.log((double) REGISTERS / zeros);
            }
            return Math.round(estimate);
        }
    }

    /**
     * A 64-bit hash by SQL value semantics: equal SQL values must hash equal,
     * so each type is reduced to a canonical long (or a string/bytes fold)
     * before the murmur3 finalizer spreads it.
     */
    static long hash64(Object value) {
        long raw;
        if (value instanceof String s) {
            raw = fnv64(s.getBytes(StandardCharsets.UTF_8));
        } else if (value instanceof Long || value instanceof Integer
                || value instanceof Short || value instanceof Byte) {
            raw = ((Number) value).longValue();
        } else if (value instanceof Double || value instanceof Float) {
            raw = Double.doubleToLongBits(((Number) value).doubleValue());
        } else if (value instanceof Boolean b) {
            raw = b ? 1 : 0;
        } else if (value instanceof byte[] bytes) {
            raw = fnv64(bytes);
        } else {
            raw = value.hashCode();
        }
        return fmix64(raw);
    }

    private static long fnv64(byte[] bytes) {
        long hash = 0xcbf29ce484222325L;
        for (byte b : bytes) {
            hash ^= b & 0xffL;
            hash *= 0x100000001b3L;
        }
        return hash;
    }

    /** The murmur3 64-bit finalizer (full avalanche). */
    private static long fmix64(long h) {
        h ^= h >>> 33;
        h *= 0xff51afd7ed558ccdL;
        h ^= h >>> 33;
        h *= 0xc4ceb9fe1a85ec53L;
        h ^= h >>> 33;
        return h;
    }

    /** The Calcite function object. */
    static List<Map.Entry<String, Function>> builtIns() {
        Function function = AggregateFunctionImpl.create(Host.class);
        if (function == null) {
            throw new IllegalStateException(
                    "APPROX_DISTINCT host does not match the aggregate convention");
        }
        return List.of(Map.entry("APPROX_DISTINCT", function));
    }
}
