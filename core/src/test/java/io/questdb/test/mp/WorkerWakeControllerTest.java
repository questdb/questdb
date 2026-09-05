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

import io.questdb.Metrics;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.std.ObjList;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class WorkerWakeControllerTest {

    @Test
    public void testConcurrentRegisterWakeAllAndSelfUnregisterReconcileState() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int registrarCount = 8;
            final int registrationCount = 10_000;
            final WorkerPool pool = createPool(registrarCount);
            final CountDownLatch start = new CountDownLatch(1);
            final CountDownLatch registrarsDone = new CountDownLatch(registrarCount);
            final AtomicBoolean isWakerStopped = new AtomicBoolean();
            final AtomicReference<Throwable> error = new AtomicReference<>();
            final ObjList<Thread> registrars = new ObjList<>(registrarCount);
            final Thread waker = new Thread(() -> {
                try {
                    Assert.assertTrue(start.await(10, TimeUnit.SECONDS));
                    int iteration = 0;
                    while (!isWakerStopped.get()
                            && (registrarsDone.getCount() != 0 || pool.getReadyWorkerCountForTesting() != 0)) {
                        if ((iteration++ & 7) == 0) {
                            pool.wakeAllForTesting();
                        } else {
                            pool.wakeOneForTesting(FiberRuntime.NO_WORKER);
                        }
                    }
                } catch (Throwable th) {
                    error.compareAndSet(null, th);
                }
            }, "fiber-ready-waker");
            waker.setDaemon(true);
            try {
                registerAllTargets(pool, registrarCount);
                for (int workerId = 0; workerId < registrarCount; workerId++) {
                    final int id = workerId;
                    final Thread registrar = new Thread(() -> {
                        try {
                            Assert.assertTrue(start.await(10, TimeUnit.SECONDS));
                            for (int i = 0; i < registrationCount; i++) {
                                if (!pool.registerReadyWorkerForTesting(id)) {
                                    throw new AssertionError("could not register an unowned ready bit");
                                }
                                if ((i & 7) == 0) {
                                    Thread.yield();
                                }
                                pool.unregisterReadyWorkerForTesting(id);
                            }
                        } catch (Throwable th) {
                            error.compareAndSet(null, th);
                        } finally {
                            registrarsDone.countDown();
                        }
                    }, "fiber-ready-registrar-" + workerId);
                    registrars.add(registrar);
                    registrar.start();
                }
                waker.start();
                start.countDown();

                for (int i = 0; i < registrarCount; i++) {
                    registrars.getQuick(i).join(10_000L);
                    Assert.assertFalse(
                            registrars.getQuick(i).getName() + " did not stop",
                            registrars.getQuick(i).isAlive()
                    );
                }
                waker.join(10_000L);
                Assert.assertFalse("ready-bit waker did not stop", waker.isAlive());
                if (error.get() != null) {
                    throw new AssertionError(error.get());
                }
                assertNoneReady(pool, registrarCount);
            } finally {
                isWakerStopped.set(true);
                start.countDown();
                for (int i = 0; i < registrarCount; i++) {
                    final Thread registrar = registrars.getQuick(i);
                    registrar.join(10_000L);
                    Assert.assertFalse(registrar.getName() + " did not stop", registrar.isAlive());
                }
                waker.join(10_000L);
                Assert.assertFalse("ready-bit waker did not stop", waker.isAlive());
                pool.halt();
            }
        });
    }

    @Test
    public void testConcurrentWakeClaimsEachReadyWorkerAtMostOnce() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int workerCount = 130;
            final int wakerCount = 8;
            final WorkerPool pool = createPool(workerCount);
            try {
                registerAllTargets(pool, workerCount);
                for (int i = 0; i < workerCount; i++) {
                    Assert.assertTrue(pool.registerReadyWorkerForTesting(i));
                }

                final CountDownLatch start = new CountDownLatch(1);
                final AtomicInteger claimCount = new AtomicInteger();
                final AtomicReference<Throwable> error = new AtomicReference<>();
                final ObjList<Thread> wakers = new ObjList<>(wakerCount);
                for (int i = 0; i < wakerCount; i++) {
                    final Thread waker = new Thread(() -> {
                        try {
                            Assert.assertTrue(start.await(10, TimeUnit.SECONDS));
                            while (pool.wakeOneForTesting(FiberRuntime.NO_WORKER)) {
                                claimCount.incrementAndGet();
                            }
                        } catch (Throwable th) {
                            error.compareAndSet(null, th);
                        }
                    }, "fiber-waker-" + i);
                    wakers.add(waker);
                    waker.start();
                }
                start.countDown();
                for (int i = 0; i < wakerCount; i++) {
                    wakers.getQuick(i).join(10_000L);
                    Assert.assertFalse(wakers.getQuick(i).getName() + " did not stop", wakers.getQuick(i).isAlive());
                }
                if (error.get() != null) {
                    throw new AssertionError(error.get());
                }
                Assert.assertEquals(workerCount, claimCount.get());
                assertNoneReady(pool, workerCount);
            } finally {
                pool.halt();
            }
        });
    }

    @Test
    public void testCursorWrapsFromPartialLastWord() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            assertCursorWrapsFromPartialLastWord(65);
            assertCursorWrapsFromPartialLastWord(129);
        });
    }

    @Test
    public void testPreferredClaimWinsOverReadyCursorSelection() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            assertPreferredClaimWinsOverReadyCursorSelection(4, 2, 0);
            assertPreferredClaimWinsOverReadyCursorSelection(130, 65, 3);
        });
    }

    @Test
    public void testPreferredMissFallsBackAcrossBitmapWords() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final WorkerPool pool = createPool(130);
            try {
                registerAllTargets(pool, 130);
                Assert.assertTrue(pool.registerReadyWorkerForTesting(64));
                Assert.assertTrue(pool.wakeOneForTesting(17));
                Assert.assertFalse(pool.isWorkerReadyForTesting(64));
                Assert.assertEquals(0, pool.getReadyWorkerCountForTesting());
            } finally {
                pool.halt();
            }
        });
    }

    @Test
    public void testWakeCursorContinuesAcrossSignedOverflow() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final WorkerPool pool = createPool(3);
            try {
                registerAllTargets(pool, 3);
                for (int i = 0; i < 3; i++) {
                    Assert.assertTrue(pool.registerReadyWorkerForTesting(i));
                }
                pool.setWakeCursorForTesting(Integer.MAX_VALUE);
                for (int i = 0; i < 3; i++) {
                    Assert.assertTrue(pool.wakeOneForTesting(FiberRuntime.NO_WORKER));
                }
                assertNoneReady(pool, 3);
            } finally {
                pool.halt();
            }
        });
    }

    @Test
    public void testWakeOneAndSelfUnregisterReconcileCount() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final WorkerPool pool = createPool(2);
            try {
                registerAllTargets(pool, 2);
                Assert.assertTrue(pool.registerReadyWorkerForTesting(0));
                Assert.assertTrue(pool.registerReadyWorkerForTesting(1));
                Assert.assertTrue(pool.wakeOneForTesting(1));
                pool.unregisterReadyWorkerForTesting(1);
                pool.unregisterReadyWorkerForTesting(0);
                assertNoneReady(pool, 2);
            } finally {
                pool.halt();
            }
        });
    }

    private static void assertCursorWrapsFromPartialLastWord(int workerCount) {
        final WorkerPool pool = createPool(workerCount);
        try {
            registerAllTargets(pool, workerCount);
            Assert.assertTrue(pool.registerReadyWorkerForTesting(0));
            pool.setWakeCursorForTesting(workerCount - 1);
            Assert.assertTrue(pool.wakeOneForTesting(FiberRuntime.NO_WORKER));
            Assert.assertFalse(pool.isWorkerReadyForTesting(0));
            Assert.assertEquals(0, pool.getReadyWorkerCountForTesting());
        } finally {
            pool.halt();
        }
    }

    private static void assertNoneReady(WorkerPool pool, int workerCount) {
        Assert.assertEquals(0, pool.getReadyWorkerCountForTesting());
        for (int i = 0; i < workerCount; i++) {
            Assert.assertFalse(pool.isWorkerReadyForTesting(i));
        }
    }

    private static void assertPreferredClaimWinsOverReadyCursorSelection(
            int workerCount,
            int preferredWorkerId,
            int cursorWorkerId
    ) {
        final WorkerPool pool = createPool(workerCount);
        try {
            registerAllTargets(pool, workerCount);
            Assert.assertTrue(pool.registerReadyWorkerForTesting(cursorWorkerId));
            Assert.assertTrue(pool.registerReadyWorkerForTesting(preferredWorkerId));
            // Cursor scans forward from here: a preference-blind claim takes cursorWorkerId.
            pool.setWakeCursorForTesting(cursorWorkerId);

            Assert.assertTrue(pool.wakeOneForTesting(preferredWorkerId));

            Assert.assertFalse(
                    "preferred Worker was not claimed [workerId=" + preferredWorkerId + ']',
                    pool.isWorkerReadyForTesting(preferredWorkerId)
            );
            Assert.assertTrue(
                    "cursor Worker was claimed instead of the preferred one [workerId=" + cursorWorkerId + ']',
                    pool.isWorkerReadyForTesting(cursorWorkerId)
            );
            Assert.assertEquals(1, pool.getReadyWorkerCountForTesting());

            pool.unregisterReadyWorkerForTesting(cursorWorkerId);
            assertNoneReady(pool, workerCount);
        } finally {
            pool.halt();
        }
    }

    private static WorkerPool createPool(int workerCount) {
        return new WorkerPool(new WorkerPoolConfiguration() {
            @Override
            public int getFiberMaxLiveCount() {
                return 64;
            }

            @Override
            public int getFiberRetainedCount() {
                return 1;
            }

            @Override
            public Metrics getMetrics() {
                return Metrics.DISABLED;
            }

            @Override
            public String getPoolName() {
                return "wake-controller-test";
            }

            @Override
            public int getWorkerCount() {
                return workerCount;
            }

            @Override
            public WorkerPoolMode getWorkerPoolMode() {
                return WorkerPoolMode.FIBER_HOST;
            }
        });
    }

    private static void registerAllTargets(WorkerPool pool, int workerCount) {
        for (int i = 0; i < workerCount; i++) {
            pool.registerWakeTargetForTesting(i, new Thread());
        }
    }
}
