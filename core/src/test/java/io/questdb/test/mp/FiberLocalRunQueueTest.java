/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.test.mp;

import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.mp.continuation.FiberWakeSink;
import io.questdb.std.Os;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;

public class FiberLocalRunQueueTest {
    private static final long AWAIT_SECONDS = 10;

    @Test
    public void testCapacityCalculationIsBoundedPowerOfTwo() {
        Assert.assertEquals(2, FiberRuntime.calculateLocalQueueCapacityForTesting(1, 1));
        Assert.assertEquals(2, FiberRuntime.calculateLocalQueueCapacityForTesting(64, 64));
        Assert.assertEquals(16, FiberRuntime.calculateLocalQueueCapacityForTesting(17, 4));
        Assert.assertEquals(64, FiberRuntime.calculateLocalQueueCapacityForTesting(64, 1));
        Assert.assertEquals(128, FiberRuntime.calculateLocalQueueCapacityForTesting(129, 4));
        Assert.assertEquals(256, FiberRuntime.calculateLocalQueueCapacityForTesting(1024, 8));
        Assert.assertEquals(256, FiberRuntime.calculateLocalQueueCapacityForTesting(Integer.MAX_VALUE, 1));
        Assert.assertThrows(
                IllegalArgumentException.class,
                () -> FiberRuntime.calculateLocalQueueCapacityForTesting(0, 1)
        );
        Assert.assertThrows(
                IllegalArgumentException.class,
                () -> FiberRuntime.calculateLocalQueueCapacityForTesting(1, 0)
        );
    }

    @Test
    public void testFifoFullAndReuse() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = newOwnerRuntime(4);
            final Fiber[] fibers = reserve(runtime, 4);
            Fiber overflow = null;
            try {
                Assert.assertEquals(4, runtime.getLocalQueueCapacityForTesting(0));
                for (Fiber fiber : fibers) {
                    Assert.assertTrue(runtime.offerLocalForTesting(0, fiber));
                }
                Assert.assertFalse(runtime.offerLocalForTesting(0, fibers[0]));
                overflow = runtime.tryReserveFiber();
                Assert.assertNull(overflow);
                Assert.assertEquals(4, runtime.getLocalQueueDepthForTesting(0));
                for (Fiber fiber : fibers) {
                    Assert.assertSame(fiber, runtime.tryDequeueLocalForTesting(0));
                }
                Assert.assertNull(runtime.tryDequeueLocalForTesting(0));
                Assert.assertEquals(0, runtime.getLocalQueueDepthForTesting(0));

                for (Fiber fiber : fibers) {
                    Assert.assertTrue(runtime.offerLocalForTesting(0, fiber));
                }
                for (Fiber fiber : fibers) {
                    Assert.assertSame(fiber, runtime.tryDequeueLocalForTesting(0));
                }
            } finally {
                if (overflow != null) {
                    release(runtime, overflow);
                }
                release(runtime, fibers);
                close(runtime);
            }
        });
    }

    @Test
    public void testSignedPositionWrap() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = newOwnerRuntime(4);
            final Fiber[] fibers = reserve(runtime, 4);
            try {
                runtime.initializeLocalPositionForTesting(0, Long.MAX_VALUE - 1);
                for (Fiber fiber : fibers) {
                    Assert.assertTrue(runtime.offerLocalForTesting(0, fiber));
                }
                for (Fiber fiber : fibers) {
                    Assert.assertSame(fiber, runtime.tryDequeueLocalForTesting(0));
                }
                Assert.assertEquals(0, runtime.getLocalQueueDepthForTesting(0));

                for (Fiber fiber : fibers) {
                    Assert.assertTrue(runtime.offerLocalForTesting(0, fiber));
                }
                for (Fiber fiber : fibers) {
                    Assert.assertSame(fiber, runtime.tryDequeueLocalForTesting(0));
                }
                Assert.assertNull(runtime.tryDequeueLocalForTesting(0));
            } finally {
                release(runtime, fibers);
                close(runtime);
            }
        });
    }

    @Test
    public void testStalledConsumerClaimPreventsSlotReuse() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = newOwnerRuntime(3);
            final Fiber[] fibers = reserve(runtime, 3);
            try {
                Assert.assertEquals(4, runtime.getLocalQueueCapacityForTesting(0));
                // Repositioning lets a capacity-four queue exercise one occupied slot through reuse
                // without needing a second producer or mutating the production capacity policy.
                runtime.initializeLocalPositionForTesting(0, 0);
                Assert.assertTrue(runtime.offerLocalForTesting(0, fibers[0]));
                Assert.assertTrue(runtime.offerLocalForTesting(0, fibers[1]));
                Assert.assertTrue(runtime.claimLocalHeadForTesting(0, 0));
                Assert.assertSame(fibers[1], runtime.tryDequeueLocalForTesting(0));
                Assert.assertTrue(runtime.offerLocalForTesting(0, fibers[2]));

                // Fill through position three. The producer then reaches the still-claimed position
                // zero and must report full until that consumer publishes the release sequence.
                Assert.assertTrue(runtime.offerLocalForTesting(0, fibers[1]));
                Assert.assertFalse(runtime.offerLocalForTesting(0, fibers[0]));
                Assert.assertSame(fibers[0], runtime.releaseLocalClaimForTesting(0, 0));
                Assert.assertTrue(runtime.offerLocalForTesting(0, fibers[0]));

                Assert.assertSame(fibers[2], runtime.tryDequeueLocalForTesting(0));
                Assert.assertSame(fibers[1], runtime.tryDequeueLocalForTesting(0));
                Assert.assertSame(fibers[0], runtime.tryDequeueLocalForTesting(0));
                Assert.assertNull(runtime.tryDequeueLocalForTesting(0));
            } finally {
                release(runtime, fibers);
                close(runtime);
            }
        });
    }

    @Test
    public void testMultipleConsumersDequeueEachEntryExactlyOnce() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int fiberCount = 64;
            final FiberRuntime runtime = newOwnerRuntime(fiberCount);
            final Fiber[] fibers = reserve(runtime, fiberCount);
            final ConcurrentHashMap<Fiber, Integer> indexes = new ConcurrentHashMap<>();
            final AtomicIntegerArray seen = new AtomicIntegerArray(fiberCount);
            final AtomicInteger consumed = new AtomicInteger();
            final AtomicReference<Throwable> error = new AtomicReference<>();
            final CountDownLatch start = new CountDownLatch(1);
            final Thread[] consumers = new Thread[4];
            try {
                for (int i = 0; i < fiberCount; i++) {
                    indexes.put(fibers[i], i);
                    Assert.assertTrue(runtime.offerLocalForTesting(0, fibers[i]));
                }
                for (int i = 0; i < consumers.length; i++) {
                    consumers[i] = newConsumer(runtime, indexes, seen, consumed, error, start, fiberCount, i);
                    consumers[i].start();
                }
                start.countDown();
                joinAll(consumers);
                rethrow(error);
                Assert.assertEquals(fiberCount, consumed.get());
                for (int i = 0; i < fiberCount; i++) {
                    Assert.assertEquals(1, seen.get(i));
                }
                Assert.assertNull(runtime.tryDequeueLocalForTesting(0));
            } finally {
                start.countDown();
                stopAll(consumers);
                release(runtime, fibers);
                close(runtime);
            }
        });
    }

    @Test
    public void testConcurrentProducerAndConsumersTransferEachEntryExactlyOnce() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int fiberCount = 256;
            // 64 owner Workers give an 8-slot ring, so the producer keeps refilling behind the
            // consumers instead of running ahead of them.
            final FiberRuntime runtime = newOwnerRuntime(fiberCount, 64);
            final Fiber[] fibers = reserve(runtime, fiberCount);
            final ConcurrentHashMap<Fiber, Integer> indexes = new ConcurrentHashMap<>();
            final AtomicIntegerArray seen = new AtomicIntegerArray(fiberCount);
            final AtomicInteger consumed = new AtomicInteger();
            final AtomicReference<Throwable> error = new AtomicReference<>();
            final CountDownLatch start = new CountDownLatch(1);
            final Thread[] consumers = new Thread[4];
            Thread producer = null;
            try {
                Assert.assertEquals(8, runtime.getLocalQueueCapacityForTesting(0));
                for (int i = 0; i < fiberCount; i++) {
                    indexes.put(fibers[i], i);
                }
                for (int i = 0; i < consumers.length; i++) {
                    consumers[i] = newConsumer(runtime, indexes, seen, consumed, error, start, fiberCount, i);
                    consumers[i].start();
                }
                producer = new Thread(() -> {
                    try {
                        Assert.assertTrue(start.await(AWAIT_SECONDS, TimeUnit.SECONDS));
                        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(AWAIT_SECONDS);
                        for (int i = 0; i < fiberCount; i++) {
                            while (!runtime.offerLocalForTesting(0, fibers[i])) {
                                if (error.get() != null) {
                                    return;
                                }
                                if (System.nanoTime() - deadline >= 0) {
                                    throw new AssertionError("timed out publishing into local queue [published="
                                            + i + ", expected=" + fiberCount + ']');
                                }
                                Os.pause();
                            }
                        }
                    } catch (Throwable th) {
                        error.compareAndSet(null, th);
                    }
                }, "fiber-local-producer");
                producer.start();

                start.countDown();
                joinAll(consumers);
                producer.join(TimeUnit.SECONDS.toMillis(AWAIT_SECONDS));
                Assert.assertFalse("producer did not stop", producer.isAlive());
                rethrow(error);
                Assert.assertEquals(fiberCount, consumed.get());
                for (int i = 0; i < fiberCount; i++) {
                    Assert.assertEquals(1, seen.get(i));
                }
                Assert.assertNull(runtime.tryDequeueLocalForTesting(0));
                Assert.assertEquals(0, runtime.getLocalQueueDepthForTesting(0));
            } finally {
                start.countDown();
                stopAll(consumers);
                stopAll(producer);
                release(runtime, fibers);
                close(runtime);
            }
        });
    }

    private static void close(FiberRuntime runtime) {
        runtime.beginQuiesce();
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(AWAIT_SECONDS);
        while (runtime.state() != FiberRuntimeState.CLOSED && System.nanoTime() < deadline) {
            runtime.drain(64);
        }
        Assert.assertTrue(runtime.awaitClosed(deadline));
        runtime.closeAfterDrained();
    }

    private static void joinAll(Thread[] threads) throws InterruptedException {
        for (Thread thread : threads) {
            thread.join(TimeUnit.SECONDS.toMillis(AWAIT_SECONDS));
            Assert.assertFalse("consumer did not stop", thread.isAlive());
        }
    }

    private static Thread newConsumer(
            FiberRuntime runtime,
            ConcurrentHashMap<Fiber, Integer> indexes,
            AtomicIntegerArray seen,
            AtomicInteger consumed,
            AtomicReference<Throwable> error,
            CountDownLatch start,
            int fiberCount,
            int consumerId
    ) {
        return new Thread(() -> {
            try {
                Assert.assertTrue(start.await(AWAIT_SECONDS, TimeUnit.SECONDS));
                final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(AWAIT_SECONDS);
                while (consumed.get() < fiberCount
                        && error.get() == null
                        && System.nanoTime() - deadline < 0) {
                    final Fiber fiber = runtime.tryDequeueLocalForTesting(0);
                    if (fiber == null) {
                        Os.pause();
                        continue;
                    }
                    final Integer index = indexes.get(fiber);
                    if (index == null || seen.getAndIncrement(index) != 0) {
                        throw new AssertionError("local queue returned an unknown or duplicate Fiber");
                    }
                    consumed.incrementAndGet();
                }
                if (consumed.get() < fiberCount && error.get() == null) {
                    throw new AssertionError("timed out draining local queue [consumed="
                            + consumed.get() + ", expected=" + fiberCount + ']');
                }
            } catch (Throwable th) {
                error.compareAndSet(null, th);
            }
        }, "fiber-local-consumer-" + consumerId);
    }

    private static FiberRuntime newOwnerRuntime(int maxLiveFiberCount) {
        return newOwnerRuntime(maxLiveFiberCount, 1);
    }

    private static FiberRuntime newOwnerRuntime(int maxLiveFiberCount, int ownerWorkerCount) {
        return new FiberRuntime(
                maxLiveFiberCount,
                maxLiveFiberCount,
                64,
                ownerWorkerCount,
                FiberWakeSink.NO_OP
        );
    }

    private static void release(FiberRuntime runtime, Fiber fiber) {
        runtime.releaseReservedFiber(fiber, fiber.getReservationEpoch());
    }

    private static void release(FiberRuntime runtime, Fiber[] fibers) {
        for (Fiber fiber : fibers) {
            release(runtime, fiber);
        }
    }

    private static Fiber[] reserve(FiberRuntime runtime, int count) {
        final Fiber[] fibers = new Fiber[count];
        for (int i = 0; i < count; i++) {
            fibers[i] = runtime.tryReserveFiber();
            Assert.assertNotNull(fibers[i]);
        }
        return fibers;
    }

    private static void rethrow(AtomicReference<Throwable> error) {
        final Throwable th = error.get();
        if (th != null) {
            throw new AssertionError(th);
        }
    }

    private static void stopAll(Thread... threads) throws InterruptedException {
        for (Thread thread : threads) {
            if (thread != null && thread.isAlive()) {
                thread.interrupt();
                thread.join(TimeUnit.SECONDS.toMillis(AWAIT_SECONDS));
            }
        }
    }
}
