package com.mercari.solution.util.domain.file;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.compress.archivers.zip.ZipFile;

import java.io.Closeable;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Locale;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * Reads the file entries of an archive one after another (work_fixedwidth.md §6): an archive is a
 * virtual directory and an entry a virtual file.
 *
 * <ul>
 *   <li>{@link #zip(SeekableByteChannel, Charset)}: reads the central directory at the end of the
 *       archive and then only the byte ranges of the entries that are opened — entries skipped by
 *       the filter cost nothing.</li>
 *   <li>{@link #zipStream} / {@link #tar}: read the archive front to back; skipped entries are read
 *       past (a zip entry with a data descriptor is inflated on the way).</li>
 * </ul>
 *
 * Directory entries are never returned. An archive inside an archive is an ordinary entry.
 * (commons-compress comes with beam-sdks-java-core, which uses it for its own compression support.)
 */
public final class ArchiveReader implements Closeable {

    public enum Type {
        zip,
        tar;

        /** The archive type a file name says, or null: {@code .zip}, {@code .tar}, {@code .tar.gz}, {@code .tgz}, ... */
        public static Type detect(final String fileName) {
            if(fileName == null) {
                return null;
            }
            final String name = fileName.toLowerCase(Locale.ROOT);
            if(name.endsWith(".zip")) {
                return zip;
            }
            if(name.endsWith(".tar") || name.contains(".tar.") || name.endsWith(".tgz")
                    || name.endsWith(".tbz2") || name.endsWith(".tbz") || name.endsWith(".tzst")) {
                return tar;
            }
            return null;
        }
    }

    private final Closeable archive;
    // central directory mode
    private final ZipFile zipFile;
    private final Enumeration<ZipArchiveEntry> zipEntries;
    // streaming mode
    private final ArchiveInputStream<? extends ArchiveEntry> stream;

    private String name;
    private long size;
    private InputStream entryStream;

    private ArchiveReader(
            final ZipFile zipFile,
            final ArchiveInputStream<? extends ArchiveEntry> stream) {

        this.zipFile = zipFile;
        this.zipEntries = zipFile == null ? null : zipFile.getEntriesInPhysicalOrder();
        this.stream = stream;
        this.archive = zipFile != null ? zipFile : stream;
    }

    /** A zip archive on a seekable channel: only the central directory and the opened entries are read. */
    public static ArchiveReader zip(final SeekableByteChannel channel, final Charset nameCharset) throws IOException {
        final ZipFile zipFile = ZipFile.builder()
                .setSeekableByteChannel(new BlockBufferedChannel(channel))
                .setCharset(nameCharset)
                .setUseUnicodeExtraFields(true)
                .get();
        return new ArchiveReader(zipFile, null);
    }

    /** A zip archive read front to back (no seek: a compressed or non-seekable source). */
    public static ArchiveReader zipStream(final InputStream input, final Charset nameCharset) {
        // allowStoredEntriesWithDataDescriptor: such entries are legal and written by some tools
        return new ArchiveReader(null, new ZipArchiveInputStream(input, nameCharset.name(), true, true));
    }

    public static ArchiveReader tar(final InputStream input, final Charset nameCharset) {
        return new ArchiveReader(null, new TarArchiveInputStream(input, nameCharset.name()));
    }

    /**
     * Advances to the next file entry accepted by the filter; false at the end of the archive.
     * The stream of the previous entry is no longer valid.
     */
    public boolean next(final Predicate<String> filter) throws IOException {
        closeEntryStream();
        if(zipFile != null) {
            while(zipEntries.hasMoreElements()) {
                final ZipArchiveEntry entry = zipEntries.nextElement();
                if(accept(entry, filter)) {
                    this.entryStream = zipFile.getInputStream(entry);
                    return true;
                }
            }
            return false;
        }
        ArchiveEntry entry;
        while((entry = stream.getNextEntry()) != null) {
            if(accept(entry, filter)) {
                this.entryStream = new EntryStream(stream);
                return true;
            }
        }
        return false;
    }

    private boolean accept(final ArchiveEntry entry, final Predicate<String> filter) {
        if(entry.isDirectory()) {
            return false;
        }
        final String entryName = normalize(entry.getName());
        if(entryName.isEmpty() || (filter != null && !filter.test(entryName))) {
            return false;
        }
        this.name = entryName;
        this.size = entry.getSize();
        return true;
    }

    /** The path of the current entry inside the archive, with {@code /} separators and no leading {@code ./}. */
    public String name() {
        return name;
    }

    /** The uncompressed size of the current entry in bytes; -1 when the archive does not say. */
    public long size() {
        return size;
    }

    /** The content of the current entry. Closing it does not close the archive. */
    public InputStream stream() {
        return entryStream;
    }

    private void closeEntryStream() throws IOException {
        if(entryStream != null && zipFile != null) {
            entryStream.close();
        }
        entryStream = null;
    }

    @Override
    public void close() throws IOException {
        try {
            closeEntryStream();
        } finally {
            archive.close();
        }
    }

    private static String normalize(final String entryName) {
        String normalized = entryName.replace('\\', '/');
        while(normalized.startsWith("./")) {
            normalized = normalized.substring(2);
        }
        while(normalized.startsWith("/")) {
            normalized = normalized.substring(1);
        }
        return normalized;
    }

    /**
     * A filter of entry paths from include / exclude globs: {@code *} matches within one path segment,
     * {@code **} across segments, {@code ?} one character; matching is case-sensitive and against the
     * whole path. No includes means every entry; excludes are applied after the includes.
     */
    public static Predicate<String> filter(final List<String> includes, final List<String> excludes) {
        final List<Pattern> includePatterns = compile(includes);
        final List<Pattern> excludePatterns = compile(excludes);
        return entryName -> {
            if(!includePatterns.isEmpty() && includePatterns.stream().noneMatch(p -> p.matcher(entryName).matches())) {
                return false;
            }
            return excludePatterns.stream().noneMatch(p -> p.matcher(entryName).matches());
        };
    }

    private static List<Pattern> compile(final List<String> globs) {
        final List<Pattern> patterns = new ArrayList<>();
        if(globs != null) {
            for(final String glob : globs) {
                if(glob != null && !glob.isBlank()) {
                    patterns.add(Pattern.compile(toRegex(normalize(glob.trim()))));
                }
            }
        }
        return patterns;
    }

    static String toRegex(final String glob) {
        final StringBuilder regex = new StringBuilder();
        for(int i = 0; i < glob.length(); i++) {
            final char c = glob.charAt(i);
            if(c == '*') {
                if(i + 1 < glob.length() && glob.charAt(i + 1) == '*') {
                    i++;
                    if(i + 1 < glob.length() && glob.charAt(i + 1) == '/') {
                        // "**/" also matches no directory at all: **/a.txt matches a.txt
                        i++;
                        regex.append("(?:.*/)?");
                    } else {
                        regex.append(".*");
                    }
                } else {
                    regex.append("[^/]*");
                }
            } else if(c == '?') {
                regex.append("[^/]");
            } else {
                regex.append(Pattern.quote(String.valueOf(c)));
            }
        }
        return regex.toString();
    }

    /**
     * Serves reads from one block of the underlying channel at a time. The zip reader locates the
     * central directory by stepping backwards through the tail of the file and then hops between
     * local headers with many small reads; on object storage every backward step or hop would
     * otherwise be a range request of its own. Reading on within an entry stays sequential for the
     * underlying channel (block after block), so it keeps one stream open.
     */
    private static final class BlockBufferedChannel implements SeekableByteChannel {

        private static final int BLOCK_SIZE = 64 * 1024;

        private final SeekableByteChannel channel;
        private final long size;
        private final ByteBuffer block = ByteBuffer.allocate(BLOCK_SIZE);
        private long blockStart = -1;
        private int blockLength;
        private long position;
        // where the underlying channel stands; -1: unknown
        private long channelPosition = -1;

        BlockBufferedChannel(final SeekableByteChannel channel) throws IOException {
            this.channel = channel;
            this.size = channel.size();
        }

        @Override
        public int read(final ByteBuffer dst) throws IOException {
            if(!channel.isOpen()) {
                throw new ClosedChannelException();
            }
            if(position >= size) {
                return -1;
            }
            int total = 0;
            while(dst.hasRemaining() && position < size) {
                final long start = position - position % BLOCK_SIZE;
                if(start != blockStart) {
                    load(start);
                }
                final int offset = (int) (position - blockStart);
                if(offset >= blockLength) {
                    break;
                }
                final int length = Math.min(dst.remaining(), blockLength - offset);
                dst.put(block.array(), offset, length);
                position += length;
                total += length;
            }
            return total == 0 ? -1 : total;
        }

        private void load(final long start) throws IOException {
            if(channelPosition != start) {
                channel.position(start);
            }
            block.clear();
            block.limit((int) Math.min(BLOCK_SIZE, size - start));
            while(block.hasRemaining()) {
                if(channel.read(block) < 0) {
                    break;
                }
            }
            blockStart = start;
            blockLength = block.position();
            channelPosition = start + blockLength;
        }

        @Override
        public long position() {
            return position;
        }

        @Override
        public SeekableByteChannel position(final long newPosition) {
            if(newPosition < 0) {
                throw new IllegalArgumentException("negative position: " + newPosition);
            }
            this.position = newPosition;
            return this;
        }

        @Override
        public long size() {
            return size;
        }

        @Override
        public int write(final ByteBuffer src) {
            throw new NonWritableChannelException();
        }

        @Override
        public SeekableByteChannel truncate(final long newSize) {
            throw new NonWritableChannelException();
        }

        @Override
        public boolean isOpen() {
            return channel.isOpen();
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }

    }

    // the content of the current entry of a streaming archive: closing it must not close the archive
    private static final class EntryStream extends FilterInputStream {

        EntryStream(final InputStream archive) {
            super(archive);
        }

        @Override
        public void close() {
            // the archive stream is closed by ArchiveReader.close()
        }

        @Override
        public boolean markSupported() {
            return false;
        }

    }

}
