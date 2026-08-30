package com.mercari.solution.util.pipeline.feature;

import com.mercari.solution.module.MElement;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

/**
 * Sorts one key's rows by their event time for the keyed replay of {@link FeatureStages}.
 * <p>
 * Hybrid: rows are collected as objects up to a memory budget and sorted in memory (no encoding, no disk)
 * — the path every small key takes. Beyond the budget each full buffer is sorted and written as one chunk
 * file; the last buffer stays in memory, and the chunks are k-way merged on read. The chunk files of a key
 * are deleted as soon as its replay closes the {@link Sorted} iterable (and the instance's directory on
 * {@link #teardown()}), so a worker's disk never holds more than the keys being processed.
 * <p>
 * Design borrowed from Beam's {@code BufferedExternalSorter} / {@code NativeFileSorter} (buffer-then-spill,
 * chunk sort + priority-queue merge); what it does not do: pre-encode every value to {@code byte[]}, write an
 * unsorted data file first, or keep spill files until the JVM exits.
 */
public final class KeyedSpillSorter implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(KeyedSpillSorter.class);

    /** Rows encoded to estimate the heap size of a row (per key: later stages carry more columns). */
    static final int SAMPLE_ROWS = 64;
    /** Heap bytes per encoded byte: an {@code MElement} with its value map is a few times its wire size. */
    static final int HEAP_FACTOR = 3;
    /** Merge fan-in cap: beyond it the key is larger than 1024 x budget (visible in the hot-key audit). */
    static final int MAX_CHUNKS = 1024;
    static final String DIR_PREFIX = "feature-spill-";

    /** The budget line is the same for every bundle of a JVM: INFO once, DEBUG afterwards. */
    private static final AtomicBoolean BUDGET_LOGGED = new AtomicBoolean();
    /** Bytes of chunk files currently on this worker's disk, and the highest that number has reached. */
    static final AtomicLong LIVE_BYTES = new AtomicLong();
    static final AtomicLong PEAK_BYTES = new AtomicLong();

    /**
     * @param memoryMB  in-memory buffer per key being sorted; null = derived from the worker heap
     * @param directory spill directory; null = the worker's {@code java.io.tmpdir}
     * @param compress  deflate (level 1) the chunk files
     */
    public record Options(Integer memoryMB, String directory, boolean compress) implements Serializable {
        public static Options defaults() { return new Options(null, null, false); }
    }

    private final Options options;
    private final Coder<MElement> coder;
    private transient Path tempDir;
    private transient long budgetBytes;

    public KeyedSpillSorter(final Options options, final Coder<MElement> coder) {
        this.options = options == null ? Options.defaults() : options;
        this.coder = coder;
    }

    /**
     * The default budget on this JVM: a quarter of the heap shared by the concurrent bundles (one per core on
     * Dataflow batch), clamped to [16, 256] MB.
     */
    public static int defaultMemoryMB() {
        final long maxHeap = Runtime.getRuntime().maxMemory();
        final int cores = Math.max(1, Runtime.getRuntime().availableProcessors());
        final long mb = maxHeap / (cores * 4L) / (1024L * 1024L);
        return (int) Math.max(16, Math.min(256, mb));
    }

    public long budgetBytes() { return budgetBytes; }
    Path tempDir() { return tempDir; }

    /** Creates the instance's spill directory and removes directories left by dead JVMs on this machine. */
    public void setup() throws IOException {
        final int mb = options.memoryMB() != null ? options.memoryMB() : defaultMemoryMB();
        this.budgetBytes = mb * 1024L * 1024L;
        final Path base = Paths.get(options.directory() != null ? options.directory() : System.getProperty("java.io.tmpdir"));
        Files.createDirectories(base);
        sweepStale(base);
        this.tempDir = Files.createTempDirectory(base, DIR_PREFIX + ProcessHandle.current().pid() + "-");
        if (BUDGET_LOGGED.compareAndSet(false, true)) {
            LOG.info("keyed spill sorter: budget {} MB per key, chunks under {} (compress={})", mb, base, options.compress());
        } else {
            LOG.debug("keyed spill sorter: budget {} MB per key, chunks under {} (compress={})", mb, tempDir, options.compress());
        }
    }

    /** Deletes the instance's spill directory (chunk files of an interrupted replay included). */
    public void teardown() {
        if (tempDir != null) {
            deleteRecursively(tempDir);
            tempDir = null;
        }
    }

    /** Sorts the rows by their sort key (the sign-flipped event millis), stable within equal keys. */
    public Sorted sort(final Iterable<KV<Long, MElement>> rows) throws IOException {
        return sort(rows, "");
    }

    /** @param context what is being sorted (stage and key), for the spill log */
    public Sorted sort(final Iterable<KV<Long, MElement>> rows, final String context) throws IOException {
        if (tempDir == null) setup();
        final List<KV<Long, MElement>> buffer = new ArrayList<>();
        final List<Path> chunks = new ArrayList<>();
        long rowLimit = Long.MAX_VALUE; // decided after the sample
        try {
            for (final KV<Long, MElement> row : rows) {
                buffer.add(row);
                if (buffer.size() == SAMPLE_ROWS && rowLimit == Long.MAX_VALUE) {
                    rowLimit = rowLimit(buffer);
                }
                if (buffer.size() >= rowLimit) {
                    if (chunks.size() >= MAX_CHUNKS) {
                        throw new IOException("key exceeds " + MAX_CHUNKS + " spill chunks of " + (budgetBytes >> 20) + " MB: raise the spill memory or split the key");
                    }
                    sortBuffer(buffer);
                    chunks.add(writeChunk(buffer, chunks.size()));
                    buffer.clear();
                }
            }
        } catch (final IOException | RuntimeException e) {
            for (final Path p : chunks) deleteQuietly(p);
            throw e;
        }
        sortBuffer(buffer);
        if (chunks.isEmpty()) return new Sorted(buffer, List.of(), List.of(), 0L);
        long bytes = 0;
        for (final Path p : chunks) bytes += Files.size(p);
        final long live = LIVE_BYTES.addAndGet(bytes);
        final long peak = PEAK_BYTES.accumulateAndGet(live, Math::max);
        LOG.info("keyed spill sorter {}: {} chunk(s) / {} MB on disk + {} rows in memory; live spill on this worker {} MB (peak {} MB)",
                context, chunks.size(), bytes >> 20, buffer.size(), live >> 20, peak >> 20);
        final List<Source> sources = new ArrayList<>();
        try {
            for (final Path p : chunks) sources.add(new ChunkReader(p, coder, options.compress()));
        } catch (final IOException e) {
            for (final Source s : sources) s.close();
            for (final Path p : chunks) deleteQuietly(p);
            throw e;
        }
        sources.add(new MemorySource(buffer));
        return new Sorted(null, sources, chunks, bytes);
    }

    private static void sortBuffer(final List<KV<Long, MElement>> buffer) {
        buffer.sort(Comparator.comparingLong(KV::getKey)); // List.sort is stable: equal keys keep arrival order
    }

    /** Rows that fit the budget, from the encoded size of the sample times the heap factor. */
    private long rowLimit(final List<KV<Long, MElement>> sample) throws IOException {
        long bytes = 0;
        try (CountingOutputStream counting = new CountingOutputStream()) {
            for (final KV<Long, MElement> row : sample) coder.encode(row.getValue(), counting);
            bytes = counting.count;
        }
        final long perRow = Math.max(1, bytes / sample.size()) * HEAP_FACTOR + 16; // + list slot / KV
        return Math.max(1, budgetBytes / perRow);
    }

    private Path writeChunk(final List<KV<Long, MElement>> sorted, final int index) throws IOException {
        final Path path = tempDir.resolve("chunk-" + index + ".bin");
        OutputStream raw = new BufferedOutputStream(Files.newOutputStream(path), 1 << 16);
        if (options.compress()) raw = new DeflaterOutputStream(raw, new Deflater(Deflater.BEST_SPEED), 1 << 16);
        try (DataOutputStream out = new DataOutputStream(raw)) {
            out.writeInt(sorted.size());
            for (final KV<Long, MElement> row : sorted) {
                out.writeLong(row.getKey());
                coder.encode(row.getValue(), out);
            }
        }
        return path;
    }

    /** A sorted key: iterate once, then close to delete the chunk files. */
    public static final class Sorted implements Iterable<KV<Long, MElement>>, AutoCloseable {
        private final List<KV<Long, MElement>> inMemory;
        private final List<Source> sources;
        private final List<Path> chunks;
        private final long bytes;
        private boolean closed;

        Sorted(final List<KV<Long, MElement>> inMemory, final List<Source> sources, final List<Path> chunks, final long bytes) {
            this.inMemory = inMemory;
            this.sources = sources;
            this.chunks = chunks;
            this.bytes = bytes;
        }

        public boolean spilled() { return !chunks.isEmpty(); }
        /** Encoded bytes the chunk files of this key occupied on disk. */
        public long spilledBytes() { return bytes; }

        @Override
        public Iterator<KV<Long, MElement>> iterator() {
            if (inMemory != null) return inMemory.iterator();
            return new MergeIterator(sources);
        }

        @Override
        public void close() {
            if (closed) return;
            closed = true;
            for (final Source s : sources) s.close();
            for (final Path p : chunks) deleteQuietly(p);
            if (bytes > 0) LIVE_BYTES.addAndGet(-bytes);
        }
    }

    interface Source extends Closeable {
        /** Next row or null at the end. */
        KV<Long, MElement> next() throws IOException;
        @Override
        void close();
    }

    static final class MemorySource implements Source {
        private final Iterator<KV<Long, MElement>> it;
        MemorySource(final List<KV<Long, MElement>> rows) { this.it = rows.iterator(); }
        @Override public KV<Long, MElement> next() { return it.hasNext() ? it.next() : null; }
        @Override public void close() {}
    }

    static final class ChunkReader implements Source {
        private final DataInputStream in;
        private final Coder<MElement> coder;
        private int remaining;

        ChunkReader(final Path path, final Coder<MElement> coder, final boolean compressed) throws IOException {
            InputStream raw = new BufferedInputStream(Files.newInputStream(path), 1 << 16);
            if (compressed) raw = new InflaterInputStream(raw, new java.util.zip.Inflater(), 1 << 16);
            this.in = new DataInputStream(raw);
            this.coder = coder;
            this.remaining = in.readInt();
        }

        @Override
        public KV<Long, MElement> next() throws IOException {
            if (remaining <= 0) return null;
            remaining--;
            final long key = in.readLong();
            return KV.of(key, coder.decode(in));
        }

        @Override
        public void close() {
            try { in.close(); } catch (final IOException ignored) { }
        }
    }

    /** k-way merge over the chunk readers and the in-memory tail; ties keep (chunk, sequence) order. */
    static final class MergeIterator implements Iterator<KV<Long, MElement>> {
        private record Entry(long key, int source, long seq, KV<Long, MElement> row) {}
        private static final Comparator<Entry> ORDER = Comparator.comparingLong(Entry::key)
                .thenComparingInt(Entry::source).thenComparingLong(Entry::seq);

        private final List<Source> sources;
        private final PriorityQueue<Entry> queue = new PriorityQueue<>(ORDER);
        private final long[] seqs;

        MergeIterator(final List<Source> sources) {
            this.sources = sources;
            this.seqs = new long[sources.size()];
            for (int i = 0; i < sources.size(); i++) advance(i);
        }

        private void advance(final int i) {
            try {
                final KV<Long, MElement> row = sources.get(i).next();
                if (row != null) queue.add(new Entry(row.getKey(), i, seqs[i]++, row));
            } catch (final IOException e) {
                throw new UncheckedIOException("failed to read spill chunk " + i, e);
            }
        }

        @Override public boolean hasNext() { return !queue.isEmpty(); }

        @Override
        public KV<Long, MElement> next() {
            final Entry e = queue.poll();
            if (e == null) throw new NoSuchElementException();
            advance(e.source);
            return e.row;
        }
    }

    private static final class CountingOutputStream extends OutputStream {
        long count;
        @Override public void write(final int b) { count++; }
        @Override public void write(final byte[] b, final int off, final int len) { count += len; }
    }

    /** Removes spill directories of this JVM's prefix whose owning process is gone (a crashed worker JVM). */
    static void sweepStale(final Path base) {
        try (DirectoryStream<Path> dirs = Files.newDirectoryStream(base, DIR_PREFIX + "*")) {
            for (final Path dir : dirs) {
                final String rest = dir.getFileName().toString().substring(DIR_PREFIX.length());
                final int dash = rest.indexOf('-');
                if (dash <= 0) continue;
                final long pid;
                try { pid = Long.parseLong(rest.substring(0, dash)); } catch (final NumberFormatException e) { continue; }
                if (pid == ProcessHandle.current().pid() || ProcessHandle.of(pid).isPresent()) continue;
                LOG.info("removing stale spill directory {}", dir);
                deleteRecursively(dir);
            }
        } catch (final IOException e) {
            LOG.warn("failed to scan spill directory {}: {}", base, e.toString());
        }
    }

    static void deleteRecursively(final Path dir) {
        try (DirectoryStream<Path> files = Files.newDirectoryStream(dir)) {
            for (final Path p : files) {
                if (Files.isDirectory(p)) deleteRecursively(p); else deleteQuietly(p);
            }
        } catch (final IOException ignored) { }
        deleteQuietly(dir);
    }

    static void deleteQuietly(final Path p) {
        try { Files.deleteIfExists(p); } catch (final IOException e) { LOG.warn("failed to delete {}: {}", p, e.toString()); }
    }
}
