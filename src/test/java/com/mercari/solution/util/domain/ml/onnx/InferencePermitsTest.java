package com.mercari.solution.util.domain.ml.onnx;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class InferencePermitsTest {

    // the copies of one transform share one semaphore, removed when the last one leaves; another transform has its own
    @Test
    public void testRegisterShared() {
        final String id = UUID.randomUUID().toString();
        final Semaphore first = InferencePermits.register(id, 2);
        final Semaphore second = InferencePermits.register(id, 2);
        Assertions.assertSame(first, second);
        Assertions.assertEquals(2, InferencePermits.users(id));
        Assertions.assertEquals(2, first.availablePermits());

        final String other = UUID.randomUUID().toString();
        Assertions.assertNotSame(first, InferencePermits.register(other, 2));
        InferencePermits.unregister(other);

        InferencePermits.unregister(id);
        Assertions.assertEquals(1, InferencePermits.users(id));
        InferencePermits.unregister(id);
        Assertions.assertEquals(0, InferencePermits.users(id));
        // registering again starts a new entry
        Assertions.assertNotSame(first, InferencePermits.register(id, 2));
        InferencePermits.unregister(id);
        Assertions.assertEquals(0, InferencePermits.users(id));
    }

    // with 2 permits, 2 holders run at once (they meet at a barrier), and 8 threads never run more than 2 at once
    @Test
    public void testCap() throws Exception {
        final String id = UUID.randomUUID().toString();
        final int cap = 2;
        final ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            final CyclicBarrier both = new CyclicBarrier(cap);
            final List<Future<?>> meet = new ArrayList<>();
            for(int t = 0; t < cap; t++) {
                meet.add(executor.submit(() -> hold(id, cap, () -> both.await(10, TimeUnit.SECONDS))));
            }
            for(final Future<?> f : meet) f.get(30, TimeUnit.SECONDS);

            final AtomicInteger active = new AtomicInteger();
            final AtomicInteger peak = new AtomicInteger();
            final List<Future<?>> runs = new ArrayList<>();
            for(int t = 0; t < 8; t++) {
                runs.add(executor.submit(() -> hold(id, cap, () -> {
                    peak.accumulateAndGet(active.incrementAndGet(), Math::max);
                    Thread.sleep(20);
                    active.decrementAndGet();
                    return null;
                })));
            }
            for(final Future<?> f : runs) f.get(30, TimeUnit.SECONDS);
            Assertions.assertTrue(peak.get() <= cap, "peak " + peak.get());
            Assertions.assertEquals(0, InferencePermits.users(id));
        } finally {
            executor.shutdownNow();
        }
    }

    // what a DoFn copy does: register in setup, hold a permit around the run, unregister in teardown
    private static Void hold(final String id, final int cap, final Callable<?> run) throws Exception {
        final Semaphore permits = InferencePermits.register(id, cap);
        try {
            permits.acquireUninterruptibly();
            try {
                run.call();
            } finally {
                permits.release();
            }
        } finally {
            InferencePermits.unregister(id);
        }
        return null;
    }

}
