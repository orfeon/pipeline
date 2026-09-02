package com.mercari.solution.util.pipeline.feature;

import com.mercari.solution.module.MElement;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** The fan-out merge reassembly ({@link FeatureStages#coalesce}) and the row-id key derivation. */
public class FeatureStagesMergeTest {

    private static MElement base(final String id, final String field, final Object value) {
        final Map<String, Object> map = new HashMap<>();
        map.put(FeatureStages.ROW_ID_FIELD, id);
        map.put(field, value);
        return MElement.of(map, Instant.ofEpochMilli(1000));
    }

    private static MElement partial(final String id, final String column, final Object value) {
        final Map<String, Object> map = new HashMap<>();
        map.put(FeatureStages.ROW_ID_FIELD, id);
        map.put(FeatureStages.PARTIAL_FIELD, true);
        map.put(column, value);
        return MElement.of(map, Instant.ofEpochMilli(1000));
    }

    @Test
    public void testMergesPartialsOntoBase() {
        final List<FeatureStages.Rejection> rejected = new ArrayList<>();
        final List<MElement> rows = FeatureStages.coalesce(
                List.of(base("a", "x", 1), partial("a", "f1", 10), partial("a", "f2", 20)), 2, rejected);
        Assertions.assertTrue(rejected.isEmpty(), rejected::toString);
        Assertions.assertEquals(1, rows.size());
        final Map<String, Object> row = rows.get(0).asPrimitiveMap();
        Assertions.assertEquals(1, row.get("x"));
        Assertions.assertEquals(10, row.get("f1"));
        Assertions.assertEquals(20, row.get("f2"));
        Assertions.assertFalse(row.containsKey(FeatureStages.PARTIAL_FIELD));
        Assertions.assertEquals(1000, rows.get(0).getTimestamp().getMillis());
    }

    @Test
    public void testBranchFailureDropsTheRowLikeTheLinearChain() {
        // one of two branches produced no partial (its stage failed the row): the row must not be emitted
        // with that branch's columns null — the linear chain would have dropped it at the failing stage
        final List<FeatureStages.Rejection> rejected = new ArrayList<>();
        final List<MElement> rows = FeatureStages.coalesce(
                List.of(base("a", "x", 1), partial("a", "f1", 10)), 2, rejected);
        Assertions.assertTrue(rows.isEmpty(), rows::toString);
        Assertions.assertEquals(2, rejected.size());
        Assertions.assertTrue(rejected.get(0).message().contains("1 of 2 branches"), rejected.get(0).message());
    }

    @Test
    public void testDuplicateRowIdRejectsTheWholeGroup() {
        // two base rows share a row id: merging their partials onto an arbitrary survivor would corrupt it,
        // so every piece of that id is rejected; other ids merge normally
        final List<FeatureStages.Rejection> rejected = new ArrayList<>();
        final List<MElement> rows = FeatureStages.coalesce(List.of(
                base("a", "x", 1), base("a", "x", 2), partial("a", "f1", 10), partial("a", "f1", 11),
                base("b", "x", 3), partial("b", "f1", 30), partial("b", "f2", 31)), 2, rejected);
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals(3, rows.get(0).asPrimitiveMap().get("x"));
        Assertions.assertEquals(30, rows.get(0).asPrimitiveMap().get("f1"));
        Assertions.assertEquals(4, rejected.size(), rejected::toString);
        for (final FeatureStages.Rejection r : rejected) {
            Assertions.assertTrue(r.message().contains("not unique"), r.message());
        }
    }

    @Test
    public void testOrphanPartialIsRejected() {
        final List<FeatureStages.Rejection> rejected = new ArrayList<>();
        final List<MElement> rows = FeatureStages.coalesce(List.of(partial("a", "f1", 10)), 1, rejected);
        Assertions.assertTrue(rows.isEmpty());
        Assertions.assertEquals(1, rejected.size());
        Assertions.assertTrue(rejected.get(0).message().contains("no base row"), rejected.get(0).message());
    }

    @Test
    public void testRowsWithoutARowIdPassAsTheyAre() {
        final Map<String, Object> plain = new HashMap<>();
        plain.put("x", 7);
        final List<FeatureStages.Rejection> rejected = new ArrayList<>();
        final List<MElement> rows = FeatureStages.coalesce(List.of(MElement.of(plain, Instant.ofEpochMilli(0))), 2, rejected);
        Assertions.assertTrue(rejected.isEmpty());
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals(7, rows.get(0).asPrimitiveMap().get("x"));
    }

    @Test
    public void testKeyWithNullTokensIsDeterministicAndCollisionFree() {
        final Map<String, Object> nullRow = new HashMap<>();
        nullRow.put("a", "x");
        nullRow.put("b", null);
        final Map<String, Object> emptyRow = new HashMap<>();
        emptyRow.put("a", "x");
        emptyRow.put("b", "");
        final List<String> keys = List.of("a", "b");
        // deterministic (a retry recomputes the same id) and a null component differs from an empty string
        Assertions.assertEquals(FeatureValues.keyWithNullTokens(nullRow, keys), FeatureValues.keyWithNullTokens(new HashMap<>(nullRow), keys));
        Assertions.assertNotEquals(FeatureValues.keyWithNullTokens(nullRow, keys), FeatureValues.keyWithNullTokens(emptyRow, keys));
        // the non-null path matches the ordinary key
        Assertions.assertEquals(FeatureValues.key(emptyRow, keys), FeatureValues.keyWithNullTokens(emptyRow, keys));
    }

}
