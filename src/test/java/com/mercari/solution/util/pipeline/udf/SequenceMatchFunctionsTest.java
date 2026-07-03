package com.mercari.solution.util.pipeline.udf;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/** Unit tests for the SEQ_MATCH engine: pattern DSL, predicates, match semantics. */
public class SequenceMatchFunctionsTest {

    private static List<Object[]> series(long... amounts) {
        final List<Object[]> rows = new ArrayList<>();
        for (int i = 0; i < amounts.length; i++) {
            rows.add(new Object[]{(long) (i + 1), amounts[i]});
        }
        return rows;
    }

    @SuppressWarnings("unchecked")
    private static List<Object[]> match(List<Object[]> rows, String pattern, String define) {
        return (List<Object[]>) SequenceMatchFunctions.seqMatch(
                rows, "seq,amount", pattern, define);
    }

    private static void assertMatches(List<Object[]> actual, int[]... expected) {
        Assertions.assertEquals(expected.length, actual.size(),
                "match count; actual=" + describe(actual));
        for (int i = 0; i < expected.length; i++) {
            Assertions.assertEquals(i + 1, actual.get(i)[0], "matchNo");
            Assertions.assertEquals(expected[i][0], actual.get(i)[1], "startIdx");
            Assertions.assertEquals(expected[i][1], actual.get(i)[2], "endIdx");
        }
    }

    private static String describe(List<Object[]> matches) {
        final StringBuilder sb = new StringBuilder();
        for (final Object[] match : matches) {
            sb.append('[').append(match[1]).append("..").append(match[2]).append(']');
        }
        return sb.toString();
    }

    @Test
    public void testUnboundedQuantifierRisingRuns() {
        // rising runs of >= 2 steps: rows 1-3 (10,20,30) and 4-6 (5,8,40)
        final List<Object[]> matches = match(series(10, 20, 30, 5, 8, 40),
                "STRT UP{2,}", "UP: $amount > PREV($amount)");
        assertMatches(matches, new int[]{1, 3}, new int[]{4, 6});
    }

    @Test
    public void testLongestMatchPerStart() {
        // 4 rising steps: the {2,} match from row 1 must be maximal (1..5), not 1..3
        final List<Object[]> matches = match(series(1, 2, 3, 4, 5),
                "STRT UP{2,}", "UP: $amount > PREV($amount)");
        assertMatches(matches, new int[]{1, 5});
    }

    @Test
    public void testPlusAndBoundedRepeat() {
        final List<Object[]> plus = match(series(1, 2, 1, 2),
                "UP+", "UP: $amount > PREV($amount)");
        assertMatches(plus, new int[]{2, 2}, new int[]{4, 4});

        // {1,2}: maximal two rising rows starting at 3 (values 2,3)... rows 2..3 rise
        final List<Object[]> bounded = match(series(5, 1, 2, 3, 0),
                "UP{1,2}", "UP: $amount > PREV($amount)");
        assertMatches(bounded, new int[]{3, 4});
    }

    @Test
    public void testAlternationAndOptional() {
        // H (high) or L (low), then optionally M (mid)
        final List<Object[]> matches = match(series(100, 50, 1),
                "(H|L) M?",
                "H: $amount >= 100; L: $amount <= 10; M: $amount > 10 AND $amount < 100");
        // row1 H, then M(row2=50) → [1..2]; row3 L → [3..3]
        assertMatches(matches, new int[]{1, 2}, new int[]{3, 3});
    }

    @Test
    public void testPrevOffsetAndArithmetic() {
        // JUMP: more than double the value two rows back
        final List<Object[]> matches = match(series(10, 11, 25, 9),
                "JUMP", "JUMP: $amount > PREV($amount, 2) * 2");
        assertMatches(matches, new int[]{3, 3});
    }

    @Test
    public void testPositionalFieldReference() {
        @SuppressWarnings("unchecked")
        final List<Object[]> matches = (List<Object[]>) SequenceMatchFunctions.seqMatch(
                series(1, 5), null, "BIG", "BIG: $1 >= 5");
        assertMatches(matches, new int[]{2, 2});
    }

    @Test
    public void testScalarArrayElements() {
        // An array of scalars (not ROWs): $0 is the element itself
        final List<Object> scalars = List.of(1L, 10L, 2L);
        @SuppressWarnings("unchecked")
        final List<Object[]> matches = (List<Object[]>) SequenceMatchFunctions.seqMatch(
                scalars, null, "BIG", "BIG: $0 >= 10");
        assertMatches(matches, new int[]{2, 2});
    }

    @Test
    public void testNullValuesCompareFalse() {
        final List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[]{1L, 10L});
        rows.add(new Object[]{2L, null});
        rows.add(new Object[]{3L, 20L});
        final List<Object[]> matches = match(rows,
                "UP", "UP: $amount > PREV($amount)");
        // row2: null > 10 → false; row3: 20 > null → false
        assertMatches(matches);
    }

    @Test
    public void testEmptyInputAndNoMatch() {
        Assertions.assertTrue(SequenceMatchFunctions.seqMatch(
                List.of(), "seq,amount", "UP", "UP: $amount > PREV($amount)").isEmpty());
        Assertions.assertTrue(SequenceMatchFunctions.seqMatch(
                null, "seq,amount", "UP", "UP: $amount > PREV($amount)").isEmpty());
        assertMatches(match(series(3, 2, 1), "UP{2,}", "UP: $amount > PREV($amount)"));
    }

    @Test
    public void testStringAndBooleanPredicates() {
        final List<Object[]> rows = List.of(
                new Object[]{1L, "start"}, new Object[]{2L, "data"}, new Object[]{3L, "end"});
        @SuppressWarnings("unchecked")
        final List<Object[]> matches = (List<Object[]>) SequenceMatchFunctions.seqMatch(
                rows, "seq,kind", "S D* E",
                "S: $kind = 'start'; D: $kind = 'data'; E: $kind = 'end'");
        assertMatches(matches, new int[]{1, 3});
    }

    @Test
    public void testInvalidPatternAndUnknownField() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> match(series(1), "UP{3,1}", ""));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> match(series(1), "UP", "UP: $missing > 1"));
    }
}
