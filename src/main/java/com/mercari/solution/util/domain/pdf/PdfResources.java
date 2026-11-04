package com.mercari.solution.util.domain.pdf;

import com.mercari.solution.util.domain.file.ResourceUtil;

import java.util.HashMap;
import java.util.Map;

/**
 * Files of the config (fonts, form templates, overlay images) shared by the threads of a worker.
 *
 * <p>Each DoFn thread would otherwise hold its own copy, which for a CJK font is tens of MB per thread.
 * The bytes are read once per owner (one module of one pipeline) and released when the last thread of that
 * owner is torn down, so that a long-running JVM (serve mode) does not keep the files of finished pipelines
 * and reads a changed file again for the next pipeline. The bytes are never modified: parsers wrap them.
 */
public final class PdfResources {

    private static final Map<String, Entry> CACHE = new HashMap<>();

    private static final class Entry {
        private final byte[] bytes;
        private int references;

        private Entry(final byte[] bytes) {
            this.bytes = bytes;
        }
    }

    private final String owner;
    private final Map<String, Boolean> acquired = new HashMap<>();

    /** @param owner id of the module instance, the same in all threads of the module (e.g. a UUID made at assembly) */
    public PdfResources(final String owner) {
        this.owner = owner;
    }

    /** Bytes of the file at {@code path} (gs:// / s3:// / local), read once for all threads of the owner. */
    public byte[] read(final String path) {
        final String key = owner + "\n" + path;
        synchronized (CACHE) {
            Entry entry = CACHE.get(key);
            if(entry == null) {
                entry = new Entry(ResourceUtil.readBytes(path));
                CACHE.put(key, entry);
            }
            if(acquired.put(path, Boolean.TRUE) == null) {
                entry.references++;
            }
            return entry.bytes;
        }
    }

    /** Releases the files read by this thread. */
    public void release() {
        synchronized (CACHE) {
            for(final String path : acquired.keySet()) {
                final String key = owner + "\n" + path;
                final Entry entry = CACHE.get(key);
                if(entry != null && --entry.references <= 0) {
                    CACHE.remove(key);
                }
            }
            acquired.clear();
        }
    }

}
