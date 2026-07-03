package com.mercari.solution.util.domain.db;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.sql.Types;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TiDBUtilTest {

    @Test
    public void testAppendQueryParameters() {

        final Map<String, String> params = new LinkedHashMap<>();
        params.put("rewriteBatchedStatements", "true");
        params.put("cachePrepStmts", "true");

        // no existing query string -> first param uses '?'
        Assertions.assertEquals(
                "jdbc:mysql://h:4000/db?rewriteBatchedStatements=true&cachePrepStmts=true",
                TiDBUtil.appendQueryParameters("jdbc:mysql://h:4000/db", params));

        // existing query string -> appended with '&'
        Assertions.assertEquals(
                "jdbc:mysql://h:4000/db?useSSL=false&rewriteBatchedStatements=true&cachePrepStmts=true",
                TiDBUtil.appendQueryParameters("jdbc:mysql://h:4000/db?useSSL=false", params));

        // already present key is not duplicated
        Assertions.assertEquals(
                "jdbc:mysql://h:4000/db?cachePrepStmts=false&rewriteBatchedStatements=true",
                TiDBUtil.appendQueryParameters("jdbc:mysql://h:4000/db?cachePrepStmts=false", params));
    }

    @Test
    public void testRangeCondition() {

        Assertions.assertEquals("`id` >= 1 AND `id` < 100",
                TiDBUtil.Range.of("1", "100").createCondition("id"));
        Assertions.assertEquals("`id` < 100",
                TiDBUtil.Range.of(null, "100").createCondition("id"));
        Assertions.assertEquals("`id` >= 100",
                TiDBUtil.Range.of("100", null).createCondition("id"));
        Assertions.assertEquals("`name` >= 'a' AND `name` < 'm'",
                TiDBUtil.Range.of("'a'", "'m'").createCondition("name"));
        Assertions.assertNull(TiDBUtil.Range.full().createCondition("id"));
    }

    @Test
    public void testCreateQuery() {

        Assertions.assertEquals("SELECT * FROM mytable",
                TiDBUtil.createQuery("mytable", "*", null, null));
        Assertions.assertEquals("SELECT id,name FROM db.mytable WHERE `id` >= 1 AND `id` < 100 AND (name IS NOT NULL)",
                TiDBUtil.createQuery("db.mytable", "id,name", "name IS NOT NULL", "`id` >= 1 AND `id` < 100"));
    }

    @Test
    public void testRangesFromBoundaries() {

        // multiple regions -> first open below, last open above, middle bounded
        final List<TiDBUtil.Range> ranges = TiDBUtil
                .rangesFromBoundaries(Arrays.asList("0", "100", "200", "300"));
        Assertions.assertEquals(4, ranges.size());
        Assertions.assertEquals("`id` < 100", ranges.get(0).createCondition("id"));
        Assertions.assertEquals("`id` >= 100 AND `id` < 200", ranges.get(1).createCondition("id"));
        Assertions.assertEquals("`id` >= 200 AND `id` < 300", ranges.get(2).createCondition("id"));
        Assertions.assertEquals("`id` >= 300", ranges.get(3).createCondition("id"));

        // a single region -> read whole table
        final List<TiDBUtil.Range> single = TiDBUtil.rangesFromBoundaries(List.of("0"));
        Assertions.assertEquals(1, single.size());
        Assertions.assertTrue(single.getFirst().isFull());

        // no regions -> read whole table
        final List<TiDBUtil.Range> empty = TiDBUtil.rangesFromBoundaries(List.of());
        Assertions.assertEquals(1, empty.size());
        Assertions.assertTrue(empty.getFirst().isFull());
    }

    @Test
    public void testCreateNumericRanges() {

        // 1000 rows / 100 per chunk -> 10 chunks, step = (99/10)+1 = 10
        final List<TiDBUtil.Range> ranges = TiDBUtil
                .createNumericRanges(BigInteger.valueOf(1), BigInteger.valueOf(100), 1000, 100);
        Assertions.assertEquals(10, ranges.size());
        Assertions.assertEquals("`id` < 11", ranges.getFirst().createCondition("id"));
        Assertions.assertEquals("`id` >= 11 AND `id` < 21", ranges.get(1).createCondition("id"));
        Assertions.assertEquals("`id` >= 91", ranges.getLast().createCondition("id"));

        // estimatedRows fits in one split -> whole table
        final List<TiDBUtil.Range> one = TiDBUtil
                .createNumericRanges(BigInteger.ONE, BigInteger.valueOf(100), 50, 100);
        Assertions.assertEquals(1, one.size());
        Assertions.assertTrue(one.getFirst().isFull());

        // unknown row count -> whole table
        final List<TiDBUtil.Range> unknown = TiDBUtil
                .createNumericRanges(BigInteger.ONE, BigInteger.valueOf(100), -1, 100);
        Assertions.assertEquals(1, unknown.size());
        Assertions.assertTrue(unknown.getFirst().isFull());

        // empty / single-value table -> whole table
        final List<TiDBUtil.Range> empty = TiDBUtil
                .createNumericRanges(BigInteger.TEN, BigInteger.TEN, 1000, 100);
        Assertions.assertEquals(1, empty.size());
        Assertions.assertTrue(empty.getFirst().isFull());
    }

    @Test
    public void testCreateNumericRangesCoverage() {

        // every value in [min, max] must fall into exactly one range
        final List<TiDBUtil.Range> ranges = TiDBUtil
                .createNumericRanges(BigInteger.valueOf(5), BigInteger.valueOf(57), 530, 100);
        for(long v = 5; v <= 57; v++) {
            int hits = 0;
            for(final TiDBUtil.Range range : ranges) {
                final boolean aboveLower = range.lower == null || v >= Long.parseLong(range.lower);
                final boolean belowUpper = range.upper == null || v < Long.parseLong(range.upper);
                if(aboveLower && belowUpper) {
                    hits++;
                }
            }
            Assertions.assertEquals(1, hits, "value " + v + " must be covered exactly once");
        }
    }

    @Test
    public void testToLiteral() {

        Assertions.assertEquals("123", TiDBUtil.toLiteral(Types.BIGINT, "123"));
        Assertions.assertEquals("12.5", TiDBUtil.toLiteral(Types.DECIMAL, "12.5"));
        Assertions.assertEquals("'abc'", TiDBUtil.toLiteral(Types.VARCHAR, "abc"));
        Assertions.assertEquals("'a''b'", TiDBUtil.toLiteral(Types.VARCHAR, "a'b"));
        Assertions.assertEquals("'a\\\\b'", TiDBUtil.toLiteral(Types.CHAR, "a\\b"));
        Assertions.assertNull(TiDBUtil.toLiteral(Types.VARCHAR, null));
    }

    @Test
    public void testTypePredicates() {

        Assertions.assertTrue(TiDBUtil.isIntegerType(Types.BIGINT));
        Assertions.assertTrue(TiDBUtil.isIntegerType(Types.INTEGER));
        Assertions.assertFalse(TiDBUtil.isIntegerType(Types.DECIMAL));
        Assertions.assertFalse(TiDBUtil.isIntegerType(Types.VARCHAR));

        Assertions.assertTrue(TiDBUtil.isNumericType(Types.DECIMAL));
        Assertions.assertTrue(TiDBUtil.isNumericType(Types.DOUBLE));
        Assertions.assertFalse(TiDBUtil.isNumericType(Types.VARCHAR));
        Assertions.assertFalse(TiDBUtil.isNumericType(Types.TIMESTAMP));
    }

}