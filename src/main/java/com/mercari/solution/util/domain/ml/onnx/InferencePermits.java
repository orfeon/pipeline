package com.mercari.solution.util.domain.ml.onnx;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

/**
 * The permits that cap how many inferences one onnx transform runs at once in a worker process (JVM): its
 * {@code maxConcurrency}. A runner makes one DoFn instance per worker thread, each a deserialized copy of the one built
 * at assembly, so the copies share the transform's id and find one semaphore under it. The semaphore is fair: a thread
 * waiting for a permit is not overtaken by threads arriving later.
 *
 * <p>The copies register in their setup and unregister in their teardown; the last one to leave removes the entry, so a
 * process that assembles many pipelines (the HTTP server) does not keep one per run. A runner that skips the teardown
 * leaves the entry behind, a few bytes.
 */
public final class InferencePermits {

    private record Entry(Semaphore semaphore, int users) {}

    private static final Map<String, Entry> PERMITS = new ConcurrentHashMap<>();

    private InferencePermits() {}

    /** The transform's semaphore, created with {@code permits} by the first copy that registers in this process. */
    public static Semaphore register(final String id, final int permits) {
        return PERMITS.compute(id, (key, entry) -> entry == null
                ? new Entry(new Semaphore(permits, true), 1)
                : new Entry(entry.semaphore(), entry.users() + 1)).semaphore();
    }

    public static void unregister(final String id) {
        PERMITS.computeIfPresent(id, (key, entry) -> entry.users() <= 1 ? null : new Entry(entry.semaphore(), entry.users() - 1));
    }

    /** The copies registered under the id (0 = no entry). */
    static int users(final String id) {
        final Entry entry = PERMITS.get(id);
        return entry == null ? 0 : entry.users();
    }

}
