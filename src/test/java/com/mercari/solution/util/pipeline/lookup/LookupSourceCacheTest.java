package com.mercari.solution.util.pipeline.lookup;

import com.google.common.base.Ticker;
import com.mercari.solution.module.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests of {@link LookupSource#lookupWithCache} against a hermetic stub
 * backend that counts calls and records the batches it receives: hit/miss
 * partitioning, negative caching, prefix-only requests, projections, the range
 * bypass, TTL expiry (fake ticker) and the DoFn-style serialization round trip.
 */
public class LookupSourceCacheTest {

    /**
     * In-memory backend with one table {@code T(ID, SEQ, NAME)}, primary key
     * {@code (ID, SEQ)}. Answers prefix-equality lookups (a superset for range
     * batches, which the operator would filter) and records every backend call.
     */
    private static class StubLookupSource extends LookupSource {

        private final List<Object[]> rows;

        private transient int lookupCalls;
        private transient List<LookupBatch> receivedBatches;

        StubLookupSource(List<Object[]> rows) {
            super("stub");
            this.rows = new ArrayList<>(rows);
        }

        @Override
        protected void setupInternal() {
            if (receivedBatches == null) {
                receivedBatches = new ArrayList<>();
            }
        }

        @Override
        protected void closeInternal() {
        }

        @Override
        public Map<String, Schema> tableSchemas() {
            final Map<String, Schema> schemas = new LinkedHashMap<>();
            schemas.put("T", Schema.of(List.of(
                    Schema.Field.of("ID", Schema.FieldType.INT64),
                    Schema.Field.of("SEQ", Schema.FieldType.INT64),
                    Schema.Field.of("NAME", Schema.FieldType.STRING))));
            return schemas;
        }

        @Override
        public List<LookupKey> keyCandidates(String table) {
            return List.of(LookupKey.primaryKey(List.of("ID", "SEQ")));
        }

        @Override
        public boolean supportsKeyPrefixLookup() {
            return true;
        }

        @Override
        public Iterable<Object[]> lookup(String table, String indexName, LookupBatch batch,
                int[] projects) {
            lookupCalls++;
            receivedBatches.add(batch);
            final List<Object[]> result = new ArrayList<>();
            for (final List<Object> prefix : batch.distinctPrefixes()) {
                for (final Object[] row : rows) {
                    boolean matches = true;
                    for (int i = 0; i < prefix.size(); i++) {
                        if (!prefix.get(i).equals(row[i])) {
                            matches = false;
                            break;
                        }
                    }
                    if (matches) {
                        result.add(project(row, projects));
                    }
                }
            }
            return result;
        }

        private static Object[] project(Object[] row, int[] projects) {
            if (projects == null) {
                return row.clone();
            }
            final Object[] out = new Object[projects.length];
            for (int i = 0; i < projects.length; i++) {
                out[i] = row[projects[i]];
            }
            return out;
        }
    }

    private static class FakeTicker extends Ticker {

        private long nanos;

        @Override
        public long read() {
            return nanos;
        }

        void advanceSeconds(long seconds) {
            nanos += seconds * 1_000_000_000L;
        }
    }

    private static StubLookupSource stub() {
        return new StubLookupSource(List.of(
                new Object[]{1L, 1L, "a"},
                new Object[]{1L, 2L, "b"},
                new Object[]{2L, 1L, "x"}));
    }

    @SafeVarargs
    private static LookupBatch pointBatch(List<Object>... keys) {
        final List<LookupRequest> requests = new ArrayList<>();
        for (final List<Object> key : keys) {
            requests.add(LookupRequest.point(key));
        }
        return LookupBatch.of(requests);
    }

    private static List<Object[]> collect(Iterable<Object[]> rows) {
        final List<Object[]> out = new ArrayList<>();
        rows.forEach(out::add);
        return out;
    }

    @Test
    public void testWithoutSpecDelegatesEveryCall() {
        final StubLookupSource source = stub();
        source.setup();
        try {
            for (int i = 0; i < 2; i++) {
                final List<Object[]> rows = collect(source.lookupWithCache(
                        "T", null, pointBatch(List.of(1L, 1L)), null));
                Assertions.assertEquals(1, rows.size());
            }
            Assertions.assertEquals(2, source.lookupCalls);
        } finally {
            source.close();
        }
    }

    @Test
    public void testHitSkipsBackend() {
        final StubLookupSource source = stub();
        source.setCacheSpec(LookupSource.CacheSpec.withDefaults());
        source.setup();
        try {
            final List<Object[]> first = collect(source.lookupWithCache(
                    "T", null, pointBatch(List.of(1L, 1L)), null));
            final List<Object[]> second = collect(source.lookupWithCache(
                    "T", null, pointBatch(List.of(1L, 1L)), null));
            Assertions.assertEquals(1, source.lookupCalls);
            Assertions.assertEquals(1, first.size());
            Assertions.assertArrayEquals(first.get(0), second.get(0));
            Assertions.assertEquals("a", second.get(0)[2]);
        } finally {
            source.close();
        }
    }

    @Test
    public void testMixedBatchFetchesOnlyMisses() {
        final StubLookupSource source = stub();
        source.setCacheSpec(LookupSource.CacheSpec.withDefaults());
        source.setup();
        try {
            collect(source.lookupWithCache("T", null, pointBatch(List.of(1L, 1L)), null));
            final List<Object[]> rows = collect(source.lookupWithCache(
                    "T", null, pointBatch(List.of(1L, 1L), List.of(2L, 1L)), null));
            Assertions.assertEquals(2, source.lookupCalls);
            // The second backend call carries only the missing request.
            final LookupBatch missBatch = source.receivedBatches.get(1);
            Assertions.assertEquals(1, missBatch.requests().size());
            Assertions.assertEquals(List.of(2L, 1L), missBatch.requests().get(0).prefix());
            // The combined result still covers both keys.
            Assertions.assertEquals(2, rows.size());
        } finally {
            source.close();
        }
    }

    @Test
    public void testEmptyResultIsCached() {
        final StubLookupSource source = stub();
        source.setCacheSpec(LookupSource.CacheSpec.withDefaults());
        source.setup();
        try {
            for (int i = 0; i < 2; i++) {
                final List<Object[]> rows = collect(source.lookupWithCache(
                        "T", null, pointBatch(List.of(99L, 1L)), null));
                Assertions.assertTrue(rows.isEmpty());
            }
            Assertions.assertEquals(1, source.lookupCalls);
        } finally {
            source.close();
        }
    }

    @Test
    public void testEmptyResultNotCachedWhenDisabled() {
        final StubLookupSource source = stub();
        source.setCacheSpec(new LookupSource.CacheSpec(100, 0, false));
        source.setup();
        try {
            for (int i = 0; i < 2; i++) {
                collect(source.lookupWithCache("T", null, pointBatch(List.of(99L, 1L)), null));
            }
            Assertions.assertEquals(2, source.lookupCalls);
        } finally {
            source.close();
        }
    }

    @Test
    public void testRangeBatchBypassesCache() {
        final StubLookupSource source = stub();
        source.setCacheSpec(LookupSource.CacheSpec.withDefaults());
        source.setup();
        try {
            final LookupBatch range = LookupBatch.of(List.of(
                    LookupRequest.range(List.of(1L), 1L, true, 2L, true)));
            for (int i = 0; i < 2; i++) {
                Assertions.assertFalse(collect(
                        source.lookupWithCache("T", null, range, null)).isEmpty());
            }
            Assertions.assertEquals(2, source.lookupCalls);
        } finally {
            source.close();
        }
    }

    @Test
    public void testPrefixOnlyLookupCached() {
        final StubLookupSource source = stub();
        source.setCacheSpec(LookupSource.CacheSpec.withDefaults());
        source.setup();
        try {
            for (int i = 0; i < 2; i++) {
                final List<Object[]> rows = collect(source.lookupWithCache(
                        "T", null, pointBatch(List.of(1L)), null));
                Assertions.assertEquals(2, rows.size()); // (1,1,'a') and (1,2,'b')
            }
            Assertions.assertEquals(1, source.lookupCalls);
        } finally {
            source.close();
        }
    }

    @Test
    public void testProjectionWithoutKeyColumnsBypassesCache() {
        final StubLookupSource source = stub();
        source.setCacheSpec(LookupSource.CacheSpec.withDefaults());
        source.setup();
        try {
            // NAME only: the key columns cannot be located in the returned rows,
            // so per-request attribution is impossible and the cache is bypassed.
            for (int i = 0; i < 2; i++) {
                final List<Object[]> rows = collect(source.lookupWithCache(
                        "T", null, pointBatch(List.of(1L, 1L)), new int[]{2}));
                Assertions.assertEquals(1, rows.size());
                Assertions.assertEquals("a", rows.get(0)[0]);
            }
            Assertions.assertEquals(2, source.lookupCalls);
        } finally {
            source.close();
        }
    }

    @Test
    public void testProjectionIncludingKeysCachedSeparately() {
        final StubLookupSource source = stub();
        source.setCacheSpec(LookupSource.CacheSpec.withDefaults());
        source.setup();
        try {
            // Reordered projection including the key columns is cacheable.
            final int[] projects = new int[]{2, 0, 1};
            for (int i = 0; i < 2; i++) {
                final List<Object[]> rows = collect(source.lookupWithCache(
                        "T", null, pointBatch(List.of(1L, 1L)), projects));
                Assertions.assertArrayEquals(new Object[]{"a", 1L, 1L}, rows.get(0));
            }
            Assertions.assertEquals(1, source.lookupCalls);
            // A different projection of the same request is a separate entry.
            collect(source.lookupWithCache("T", null, pointBatch(List.of(1L, 1L)), null));
            Assertions.assertEquals(2, source.lookupCalls);
        } finally {
            source.close();
        }
    }

    @Test
    public void testTtlExpiry() {
        final StubLookupSource source = stub();
        source.setCacheSpec(new LookupSource.CacheSpec(100, 60, true));
        final FakeTicker ticker = new FakeTicker();
        source.cacheTicker = ticker;
        source.setup();
        try {
            collect(source.lookupWithCache("T", null, pointBatch(List.of(1L, 1L)), null));
            ticker.advanceSeconds(59);
            collect(source.lookupWithCache("T", null, pointBatch(List.of(1L, 1L)), null));
            Assertions.assertEquals(1, source.lookupCalls);
            ticker.advanceSeconds(2);
            collect(source.lookupWithCache("T", null, pointBatch(List.of(1L, 1L)), null));
            Assertions.assertEquals(2, source.lookupCalls);
        } finally {
            source.close();
        }
    }

    @Test
    public void testCloseClearsCache() {
        final StubLookupSource source = stub();
        source.setCacheSpec(LookupSource.CacheSpec.withDefaults());
        source.setup();
        collect(source.lookupWithCache("T", null, pointBatch(List.of(1L, 1L)), null));
        source.close();
        source.setup();
        try {
            collect(source.lookupWithCache("T", null, pointBatch(List.of(1L, 1L)), null));
            Assertions.assertEquals(2, source.lookupCalls);
        } finally {
            source.close();
        }
    }

    @Test
    public void testSerializedRoundTripKeepsCacheSpec() throws Exception {
        final StubLookupSource source = stub();
        source.setCacheSpec(new LookupSource.CacheSpec(100, 300, true));

        final java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
        try (java.io.ObjectOutputStream out = new java.io.ObjectOutputStream(bytes)) {
            out.writeObject(source);
        }
        final StubLookupSource restored;
        try (java.io.ObjectInputStream in = new java.io.ObjectInputStream(
                new java.io.ByteArrayInputStream(bytes.toByteArray()))) {
            restored = (StubLookupSource) in.readObject();
        }

        restored.setup();
        try {
            for (int i = 0; i < 2; i++) {
                final List<Object[]> rows = collect(restored.lookupWithCache(
                        "T", null, pointBatch(List.of(1L, 1L)), null));
                Assertions.assertEquals(1, rows.size());
            }
            Assertions.assertEquals(1, restored.lookupCalls);
        } finally {
            restored.close();
        }
    }

    @Test
    public void testInvalidSpecRejected() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new LookupSource.CacheSpec(0, 0, true));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new LookupSource.CacheSpec(100, -1, true));
    }
}
