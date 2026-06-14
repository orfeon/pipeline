package com.mercari.solution.util.domain.db;

import org.junit.Assert;
import org.junit.Test;

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
        Assert.assertEquals(
                "jdbc:mysql://h:4000/db?rewriteBatchedStatements=true&cachePrepStmts=true",
                TiDBUtil.appendQueryParameters("jdbc:mysql://h:4000/db", params));

        // existing query string -> appended with '&'
        Assert.assertEquals(
                "jdbc:mysql://h:4000/db?useSSL=false&rewriteBatchedStatements=true&cachePrepStmts=true",
                TiDBUtil.appendQueryParameters("jdbc:mysql://h:4000/db?useSSL=false", params));

        // already present key is not duplicated
        Assert.assertEquals(
                "jdbc:mysql://h:4000/db?cachePrepStmts=false&rewriteBatchedStatements=true",
                TiDBUtil.appendQueryParameters("jdbc:mysql://h:4000/db?cachePrepStmts=false", params));
    }

    @Test
    public void testRangeCondition() {

        Assert.assertEquals("`id` >= 1 AND `id` < 100",
                TiDBUtil.Range.of("1", "100").createCondition("id"));
        Assert.assertEquals("`id` < 100",
                TiDBUtil.Range.of(null, "100").createCondition("id"));
        Assert.assertEquals("`id` >= 100",
                TiDBUtil.Range.of("100", null).createCondition("id"));
        Assert.assertEquals("`name` >= 'a' AND `name` < 'm'",
                TiDBUtil.Range.of("'a'", "'m'").createCondition("name"));
        Assert.assertNull(TiDBUtil.Range.full().createCondition("id"));
    }

    @Test
    public void testCreateQuery() {

        Assert.assertEquals("SELECT * FROM mytable",
                TiDBUtil.createQuery("mytable", "*", null, null));
        Assert.assertEquals("SELECT id,name FROM db.mytable WHERE `id` >= 1 AND `id` < 100 AND (name IS NOT NULL)",
                TiDBUtil.createQuery("db.mytable", "id,name", "name IS NOT NULL", "`id` >= 1 AND `id` < 100"));
    }

    @Test
    public void testRangesFromBoundaries() {

        // multiple regions -> first open below, last open above, middle bounded
        final List<TiDBUtil.Range> ranges = TiDBUtil
                .rangesFromBoundaries(Arrays.asList("0", "100", "200", "300"));
        Assert.assertEquals(4, ranges.size());
        Assert.assertEquals("`id` < 100", ranges.get(0).createCondition("id"));
        Assert.assertEquals("`id` >= 100 AND `id` < 200", ranges.get(1).createCondition("id"));
        Assert.assertEquals("`id` >= 200 AND `id` < 300", ranges.get(2).createCondition("id"));
        Assert.assertEquals("`id` >= 300", ranges.get(3).createCondition("id"));

        // a single region -> read whole table
        final List<TiDBUtil.Range> single = TiDBUtil.rangesFromBoundaries(List.of("0"));
        Assert.assertEquals(1, single.size());
        Assert.assertTrue(single.getFirst().isFull());

        // no regions -> read whole table
        final List<TiDBUtil.Range> empty = TiDBUtil.rangesFromBoundaries(List.of());
        Assert.assertEquals(1, empty.size());
        Assert.assertTrue(empty.getFirst().isFull());
    }

    @Test
    public void testCreateNumericRanges() {

        // 1000 rows / 100 per chunk -> 10 chunks, step = (99/10)+1 = 10
        final List<TiDBUtil.Range> ranges = TiDBUtil
                .createNumericRanges(BigInteger.valueOf(1), BigInteger.valueOf(100), 1000, 100);
        Assert.assertEquals(10, ranges.size());
        Assert.assertEquals("`id` < 11", ranges.getFirst().createCondition("id"));
        Assert.assertEquals("`id` >= 11 AND `id` < 21", ranges.get(1).createCondition("id"));
        Assert.assertEquals("`id` >= 91", ranges.getLast().createCondition("id"));

        // estimatedRows fits in one split -> whole table
        final List<TiDBUtil.Range> one = TiDBUtil
                .createNumericRanges(BigInteger.ONE, BigInteger.valueOf(100), 50, 100);
        Assert.assertEquals(1, one.size());
        Assert.assertTrue(one.getFirst().isFull());

        // unknown row count -> whole table
        final List<TiDBUtil.Range> unknown = TiDBUtil
                .createNumericRanges(BigInteger.ONE, BigInteger.valueOf(100), -1, 100);
        Assert.assertEquals(1, unknown.size());
        Assert.assertTrue(unknown.getFirst().isFull());

        // empty / single-value table -> whole table
        final List<TiDBUtil.Range> empty = TiDBUtil
                .createNumericRanges(BigInteger.TEN, BigInteger.TEN, 1000, 100);
        Assert.assertEquals(1, empty.size());
        Assert.assertTrue(empty.getFirst().isFull());
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
            Assert.assertEquals("value " + v + " must be covered exactly once", 1, hits);
        }
    }

    @Test
    public void testToLiteral() {

        Assert.assertEquals("123", TiDBUtil.toLiteral(Types.BIGINT, "123"));
        Assert.assertEquals("12.5", TiDBUtil.toLiteral(Types.DECIMAL, "12.5"));
        Assert.assertEquals("'abc'", TiDBUtil.toLiteral(Types.VARCHAR, "abc"));
        Assert.assertEquals("'a''b'", TiDBUtil.toLiteral(Types.VARCHAR, "a'b"));
        Assert.assertEquals("'a\\\\b'", TiDBUtil.toLiteral(Types.CHAR, "a\\b"));
        Assert.assertNull(TiDBUtil.toLiteral(Types.VARCHAR, null));
    }

    @Test
    public void testTypePredicates() {

        Assert.assertTrue(TiDBUtil.isIntegerType(Types.BIGINT));
        Assert.assertTrue(TiDBUtil.isIntegerType(Types.INTEGER));
        Assert.assertFalse(TiDBUtil.isIntegerType(Types.DECIMAL));
        Assert.assertFalse(TiDBUtil.isIntegerType(Types.VARCHAR));

        Assert.assertTrue(TiDBUtil.isNumericType(Types.DECIMAL));
        Assert.assertTrue(TiDBUtil.isNumericType(Types.DOUBLE));
        Assert.assertFalse(TiDBUtil.isNumericType(Types.VARCHAR));
        Assert.assertFalse(TiDBUtil.isNumericType(Types.TIMESTAMP));
    }

}
