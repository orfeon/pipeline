package com.mercari.solution.util.pipeline.lookup;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Process-wide registry mapping a stable id to a live {@link LookupSource}.
 * Generated lookup-join code embeds the id (a constant) and resolves the source
 * here at runtime, since the live source object cannot be embedded in generated
 * code. Each {@code Query2} instance registers its own source instances in
 * {@code setup()} and deregisters them in {@code teardown()}, so concurrent
 * DoFn instances in one worker JVM never collide.
 */
public final class LookupSourceRegistry {

    private static final AtomicLong IDS = new AtomicLong();
    private static final ConcurrentHashMap<Long, LookupSource> SOURCES =
            new ConcurrentHashMap<>();

    private LookupSourceRegistry() {
    }

    public static long register(LookupSource source) {
        long id = IDS.incrementAndGet();
        SOURCES.put(id, source);
        return id;
    }

    public static void deregister(long id) {
        SOURCES.remove(id);
    }

    public static LookupSource get(long id) {
        LookupSource source = SOURCES.get(id);
        if (source == null) {
            throw new IllegalStateException("Lookup source id " + id + " is not registered "
                    + "(source closed or not set up?)");
        }
        return source;
    }
}
