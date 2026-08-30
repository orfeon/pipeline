package com.mercari.solution.util.pipeline.feature;

import com.mercari.solution.module.DataType;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.coder.ElementCoder;
import org.apache.beam.sdk.values.KV;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Stream;

/**
 * The keyed stages' sorter: small keys sort in memory, large keys spill sorted chunks that are merged on read
 * and deleted on close; ties keep arrival order; the instance's directory disappears on teardown.
 */
public class KeyedSpillSorterTest {

    private static final Schema SCHEMA = Schema.builder()
            .withField("id", Schema.FieldType.INT64)
            .withField("note", Schema.FieldType.STRING)
            .withType(DataType.ELEMENT)
            .build();

    private static final ElementCoder CODER = ElementCoder.of(SCHEMA);

    private static KV<Long, MElement> row(final long millis, final long id) {
        final Map<String, Object> values = new HashMap<>();
        values.put("id", id);
        values.put("note", "x".repeat(200));
        return KV.of(FeatureStages.SortKeyDoFn.sortable(millis), MElement.of(values, millis));
    }

    private static List<KV<Long, MElement>> shuffled(final int n, final long seed) {
        final List<KV<Long, MElement>> rows = new ArrayList<>();
        final Random random = new Random(seed);
        for (int i = 0; i < n; i++) {
            // ~10 rows per millisecond so ties are common; negative millis exercise the sign flip
            rows.add(row(random.nextInt(n / 10 + 1) - n / 20, i));
        }
        return rows;
    }

    /** Sorted by millis; ties by arrival order (row id) - what a stable sort of the input gives. */
    private static List<Long> expectedIds(final List<KV<Long, MElement>> rows) {
        final List<KV<Long, MElement>> copy = new ArrayList<>(rows);
        copy.sort(Comparator.comparingLong(KV::getKey));
        return copy.stream().map(kv -> (Long) kv.getValue().getPrimitiveValue("id")).toList();
    }

    private static List<Long> ids(final Iterable<KV<Long, MElement>> sorted) {
        final List<Long> out = new ArrayList<>();
        for (final KV<Long, MElement> kv : sorted) out.add(((Number) kv.getValue().getPrimitiveValue("id")).longValue());
        return out;
    }

    private static long filesUnder(final Path dir) throws IOException {
        if (!Files.isDirectory(dir)) return 0;
        try (Stream<Path> s = Files.list(dir)) { return s.count(); }
    }

    @Test
    public void testSmallKeySortsInMemory(@TempDir final Path dir) throws IOException {
        final KeyedSpillSorter sorter = new KeyedSpillSorter(new KeyedSpillSorter.Options(64, dir.toString(), false), CODER);
        sorter.setup();
        final List<KV<Long, MElement>> rows = shuffled(1000, 1);
        try (KeyedSpillSorter.Sorted sorted = sorter.sort(rows)) {
            Assertions.assertFalse(sorted.spilled());
            Assertions.assertEquals(expectedIds(rows), ids(sorted));
            Assertions.assertEquals(0, filesUnder(sorter.tempDir()), "no chunk files for an in-memory sort");
        }
        try (KeyedSpillSorter.Sorted sorted = sorter.sort(List.of())) {
            Assertions.assertFalse(sorted.iterator().hasNext());
        }
        try (KeyedSpillSorter.Sorted sorted = sorter.sort(List.of(row(5, 42)))) {
            Assertions.assertEquals(List.of(42L), ids(sorted));
        }
        final Path tempDir = sorter.tempDir();
        Assertions.assertTrue(Files.isDirectory(tempDir));
        sorter.teardown();
        Assertions.assertFalse(Files.exists(tempDir), "teardown removes the instance directory");
    }

    @Test
    public void testLargeKeySpillsMergesAndDeletes(@TempDir final Path dir) throws IOException {
        for (final boolean compress : new boolean[]{false, true}) {
            // 1 MB budget: 200-byte rows x heap factor 3 -> ~1.7k rows per chunk, so 20k rows spill ~11 chunks
            final KeyedSpillSorter sorter = new KeyedSpillSorter(new KeyedSpillSorter.Options(1, dir.toString(), compress), CODER);
            sorter.setup();
            final List<KV<Long, MElement>> rows = shuffled(20_000, 7);
            final KeyedSpillSorter.Sorted sorted = sorter.sort(rows);
            Assertions.assertTrue(sorted.spilled(), "compress=" + compress);
            Assertions.assertTrue(filesUnder(sorter.tempDir()) > 1, "several chunks on disk while the key is open");
            Assertions.assertEquals(expectedIds(rows), ids(sorted), "compress=" + compress);
            sorted.close();
            Assertions.assertEquals(0, filesUnder(sorter.tempDir()), "chunks deleted on close (compress=" + compress + ")");
            sorter.teardown();
        }
    }

    @Test
    public void testExactChunkBoundaryAndTies(@TempDir final Path dir) throws IOException {
        final KeyedSpillSorter sorter = new KeyedSpillSorter(new KeyedSpillSorter.Options(1, dir.toString(), false), CODER);
        sorter.setup();
        // every row on the same millis: the merge must keep arrival order across chunk boundaries
        final List<KV<Long, MElement>> rows = new ArrayList<>();
        for (int i = 0; i < 10_000; i++) rows.add(row(100, i));
        try (KeyedSpillSorter.Sorted sorted = sorter.sort(rows)) {
            Assertions.assertTrue(sorted.spilled());
            final List<Long> ids = ids(sorted);
            for (int i = 0; i < ids.size(); i++) Assertions.assertEquals(i, ids.get(i));
        }
        sorter.teardown();
    }

    @Test
    public void testSweepRemovesDeadPidDirectories(@TempDir final Path dir) throws IOException {
        // a directory named for a pid that cannot be alive: sweep removes it; one for this pid stays
        final Path dead = Files.createDirectories(dir.resolve(KeyedSpillSorter.DIR_PREFIX + Long.MAX_VALUE + "-old"));
        Files.writeString(dead.resolve("chunk-0.bin"), "stale");
        final Path alive = Files.createDirectories(dir.resolve(KeyedSpillSorter.DIR_PREFIX + ProcessHandle.current().pid() + "-live"));
        final KeyedSpillSorter sorter = new KeyedSpillSorter(new KeyedSpillSorter.Options(8, dir.toString(), false), CODER);
        sorter.setup();
        Assertions.assertFalse(Files.exists(dead));
        Assertions.assertTrue(Files.exists(alive));
        sorter.teardown();
        Assertions.assertTrue(Files.exists(alive), "teardown only removes its own directory");
    }

    @Test
    public void testDefaultBudgetIsClamped() {
        final int mb = KeyedSpillSorter.defaultMemoryMB();
        Assertions.assertTrue(mb >= 16 && mb <= 256, "default " + mb);
    }
}
