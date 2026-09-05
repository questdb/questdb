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
import io.questdb.mp.CarrierIdentity;
import io.questdb.mp.Job;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.Worker;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.mp.continuation.FiberWaitCoordinator;
import io.questdb.mp.continuation.FiberWalWaitQueue;
import io.questdb.mp.continuation.FiberWalWaitRegistration;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.mp.continuation.SourceRegistrationResult;
import io.questdb.std.ObjHashSet;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class WorkerCurrentTest {
    private static final long AWAIT_SECONDS = 10;

    @Test
    public void testCurrentClearsWhenWorkerRunReturns() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final SOCountDownLatch haltLatch = new SOCountDownLatch(1);
            final ObjHashSet<Job> jobs = new ObjHashSet<>();
            final AtomicReference<Worker> workerReference = new AtomicReference<>();
            final AtomicReference<Worker> currentInLoop = new AtomicReference<>();
            final AtomicReference<Worker> currentAfterRun = new AtomicReference<>();
            final AtomicInteger carrierAfterRun = new AtomicInteger(Integer.MIN_VALUE);
            final AtomicBoolean isAfterRunSampled = new AtomicBoolean();
            final AtomicReference<Throwable> workerFailure = new AtomicReference<>();
            final AtomicReference<Throwable> afterRunFailure = new AtomicReference<>();
            jobs.add(workerContext -> {
                currentInLoop.set(Worker.current());
                workerReference.get().halt();
                return false;
            });

            final Worker worker = new Worker(
                    "worker-current-cleanup-test",
                    0,
                    Worker.NO_THREAD_AFFINITY,
                    jobs,
                    haltLatch,
                    workerFailure::set,
                    true,
                    1,
                    2,
                    3,
                    1,
                    Metrics.DISABLED,
                    null,
                    null
            ) {
                @Override
                public void run() {
                    super.run();
                    try {
                        currentAfterRun.set(Worker.current());
                        carrierAfterRun.set(CarrierIdentity.current());
                    } catch (Throwable th) {
                        afterRunFailure.set(th);
                    } finally {
                        isAfterRunSampled.set(true);
                    }
                }
            };
            workerReference.set(worker);
            worker.setDaemon(true);

            worker.start();
            try {
                Assert.assertTrue(
                        "worker did not halt",
                        haltLatch.await(TimeUnit.SECONDS.toNanos(AWAIT_SECONDS))
                );
                worker.join(TimeUnit.SECONDS.toMillis(AWAIT_SECONDS));
            } finally {
                worker.halt();
                worker.join(TimeUnit.SECONDS.toMillis(AWAIT_SECONDS));
            }

            Assert.assertFalse("worker did not return", worker.isAlive());
            assertNoFailure(workerFailure);
            assertNoFailure(afterRunFailure);
            Assert.assertSame(worker, currentInLoop.get());
            Assert.assertTrue(isAfterRunSampled.get());
            Assert.assertNull(currentAfterRun.get());
            Assert.assertEquals(CarrierIdentity.UNBOUND, carrierAfterRun.get());
        });
    }

    @Test
    public void testCurrentFollowsFiberAcrossWorkers() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CountDownLatch workerZeroSeen = new CountDownLatch(1);
            final CountDownLatch workerOneBlocked = new CountDownLatch(1);
            final CountDownLatch workerZeroBlocked = new CountDownLatch(1);
            final CountDownLatch fiberParked = new CountDownLatch(1);
            final CountDownLatch releaseWorkerZero = new CountDownLatch(1);
            final CountDownLatch releaseWorkerOne = new CountDownLatch(1);
            final AtomicBoolean isWorkerZeroGateArmed = new AtomicBoolean();
            final AtomicBoolean isWorkerZeroBlocked = new AtomicBoolean();
            final AtomicBoolean isWorkerOneBlocked = new AtomicBoolean();
            final AtomicReference<Worker> workerZero = new AtomicReference<>();
            final AtomicReference<Worker> workerOne = new AtomicReference<>();
            final AtomicReference<Throwable> asyncFailure = new AtomicReference<>();
            final TestWorkerPool pool = new TestWorkerPool(new WorkerPoolConfiguration() {
                @Override
                public int getFiberMaxLiveCount() {
                    return 1;
                }

                @Override
                public int getFiberMountBudget() {
                    return 1;
                }

                @Override
                public Metrics getMetrics() {
                    return Metrics.DISABLED;
                }

                @Override
                public String getPoolName() {
                    return "worker-current-migration-test";
                }

                @Override
                public int getWorkerCount() {
                    return 2;
                }

                @Override
                public WorkerPoolMode getWorkerPoolMode() {
                    return WorkerPoolMode.FIBER_HOST;
                }

                @Override
                public boolean haltOnError() {
                    return true;
                }

                @Override
                public boolean isDaemonPool() {
                    return true;
                }
            });
            final FiberRuntime runtime = pool.getFiberRuntime();
            runtime.setAfterProcessForTesting(() -> {
                if (runtime.getParkedFiberCount() == 1) {
                    runtime.setAfterProcessForTesting(null);
                    fiberParked.countDown();
                }
            });
            final MigratingTask task = new MigratingTask(asyncFailure);
            pool.assign(0, workerContext -> {
                workerZero.compareAndSet(null, Worker.current());
                workerZeroSeen.countDown();
                if (isWorkerZeroGateArmed.get() && isWorkerZeroBlocked.compareAndSet(false, true)) {
                    workerZeroBlocked.countDown();
                    awaitOnWorker(releaseWorkerZero, "worker 0 release", asyncFailure);
                }
                return false;
            });
            pool.assign(1, workerContext -> {
                workerOne.compareAndSet(null, Worker.current());
                if (isWorkerOneBlocked.compareAndSet(false, true)) {
                    workerOneBlocked.countDown();
                    awaitOnWorker(releaseWorkerOne, "worker 1 release", asyncFailure);
                }
                return false;
            });

            pool.start();
            try {
                await(workerZeroSeen, "worker 0 did not start");
                await(workerOneBlocked, "worker 1 did not block");
                Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
                await(fiberParked, "fiber did not park");

                isWorkerZeroGateArmed.set(true);
                await(workerZeroBlocked, "worker 0 did not block");
                task.fire();
                releaseWorkerOne.countDown();
                await(task.done, "fiber did not resume");
            } finally {
                releaseWorkerZero.countDown();
                releaseWorkerOne.countDown();
                runtime.setAfterProcessForTesting(null);
                try {
                    task.fire();
                } finally {
                    pool.halt();
                }
            }

            assertNoFailure(asyncFailure);
            Assert.assertNotNull(workerZero.get());
            Assert.assertNotNull(workerOne.get());
            Assert.assertEquals(0, workerZero.get().getWorkerId());
            Assert.assertEquals(1, workerOne.get().getWorkerId());
            Assert.assertNotSame(workerZero.get(), workerOne.get());
            Assert.assertSame(workerZero.get(), task.currentBeforePark.get());
            Assert.assertSame(workerOne.get(), task.currentAfterResume.get());
        });
    }

    private static void assertNoFailure(AtomicReference<Throwable> failure) {
        final Throwable th = failure.get();
        if (th != null) {
            throw new AssertionError(th);
        }
    }

    private static void await(CountDownLatch latch, String failureMessage) throws InterruptedException {
        Assert.assertTrue(failureMessage, latch.await(AWAIT_SECONDS, TimeUnit.SECONDS));
    }

    private static void awaitOnWorker(
            CountDownLatch latch,
            String operation,
            AtomicReference<Throwable> failure
    ) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            failure.compareAndSet(null, new AssertionError("interrupted while waiting for " + operation, e));
        }
    }

    private static final class MigratingTask extends FiberTask {
        private final AtomicReference<Throwable> asyncFailure;
        private final AtomicReference<Worker> currentAfterResume = new AtomicReference<>();
        private final AtomicReference<Worker> currentBeforePark = new AtomicReference<>();
        private final CountDownLatch done = new CountDownLatch(1);
        private final FiberWalWaitQueue waitQueue = new FiberWalWaitQueue();

        private MigratingTask(AtomicReference<Throwable> asyncFailure) {
            this.asyncFailure = asyncFailure;
        }

        private void fire() {
            waitQueue.fire(1, false);
        }

        @Override
        protected void onDone() {
            done.countDown();
        }

        @Override
        protected void onError(Throwable th) {
            asyncFailure.compareAndSet(null, th);
            done.countDown();
        }

        @Override
        protected boolean runStep() {
            currentBeforePark.set(Worker.current());
            final Fiber fiber = Objects.requireNonNull(Fiber.current());
            final FiberWaitCoordinator coordinator = fiber.getWaitCoordinator();
            final long token = fiber.beginWaitBuild(1);
            final FiberWalWaitRegistration registration = coordinator.acquireWal(token, 1);
            try {
                if (registration.register(waitQueue) != SourceRegistrationResult.ACCEPTED) {
                    throw new IllegalStateException("wait registration failed");
                }
                final int reason = fiber.suspendWait(token);
                if (reason != FiberWaitCoordinator.REASON_WAL) {
                    throw new IllegalStateException("unexpected wait reason [reason=" + reason + ']');
                }
                currentAfterResume.set(Worker.current());
                return true;
            } finally {
                registration.cancel();
                coordinator.abort(token);
                coordinator.consume(token);
            }
        }
    }
}
