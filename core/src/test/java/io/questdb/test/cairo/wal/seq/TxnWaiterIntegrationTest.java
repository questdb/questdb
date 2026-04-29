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

package io.questdb.test.cairo.wal.seq;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.ErrorTag;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.cairo.wal.seq.TxnWaiter;
import io.questdb.cairo.wal.seq.WaiterTimeoutJob;
import io.questdb.mp.ContinuationResumeJob;
import io.questdb.mp.SqlContinuation;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.datetime.millitime.MillisecondClockImpl;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Integration test that wires SqlContinuation + SeqTxnTracker + ContinuationResumeJob
 * end-to-end, exercising the same sequence a SQL-layer gateway would drive:
 *   caller runs cont.run() -> body registers TxnWaiter + suspends -> tracker advances
 *   -> fireWaiters calls cont.scheduleResume() -> pool worker dequeues and calls cont.run()
 *   -> body resumes on worker thread and completes.
 */
public class TxnWaiterIntegrationTest {

    @Test
    public void testCancelledWaiterThrowsOnResume() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SeqTxnTracker tracker = new SeqTxnTracker(configuration());
            tracker.initTxns(1, 5, false);

            ContinuationResumeJob resumeJob = new ContinuationResumeJob();
            TestWorkerPool pool = new TestWorkerPool("txn-waiter-cancel-test", 1);
            pool.assign(resumeJob);

            AtomicReference<SqlContinuation> self = new AtomicReference<>();
            AtomicReference<Boolean> observedCancelled = new AtomicReference<>();
            AtomicReference<TxnWaiter> waiterRef = new AtomicReference<>();
            CountDownLatch doneLatch = new CountDownLatch(1);

            SqlContinuation cont = new SqlContinuation(() -> {
                TxnWaiter w = new TxnWaiter(100, self.get());
                waiterRef.set(w);
                tracker.registerWaiter(w);
                SqlContinuation.suspend();
                observedCancelled.set(w.isCancelled());
                doneLatch.countDown();
            }, resumeJob);
            self.set(cont);

            try {
                cont.run();
                Assert.assertFalse(cont.isDone());
                pool.start();

                // Cancel the waiter; the canceller is responsible for scheduling resume.
                TxnWaiter w = waiterRef.get();
                Assert.assertTrue(w.tryCancel());
                cont.scheduleResume();

                Assert.assertTrue(doneLatch.await(5, TimeUnit.SECONDS));
                Assert.assertEquals(Boolean.TRUE, observedCancelled.get());
            } finally {
                pool.halt();
            }
        });
    }

    @Test
    public void testSuspendResumeDrivenBySeqTxnTrackerAdvance() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SeqTxnTracker tracker = new SeqTxnTracker(configuration());
            tracker.initTxns(1, 5, false);
            Assert.assertEquals(1, tracker.getWriterTxn());
            Assert.assertEquals(5, tracker.getSeqTxn());

            ContinuationResumeJob resumeJob = new ContinuationResumeJob();
            TestWorkerPool pool = new TestWorkerPool("txn-waiter-advance-test", 1);
            pool.assign(resumeJob);

            AtomicReference<SqlContinuation> self = new AtomicReference<>();
            AtomicReference<Thread> threadAtStart = new AtomicReference<>();
            AtomicReference<Thread> threadAfterResume = new AtomicReference<>();
            AtomicReference<Long> writerTxnAtResume = new AtomicReference<>();
            CountDownLatch doneLatch = new CountDownLatch(1);

            SqlContinuation cont = new SqlContinuation(() -> {
                threadAtStart.set(Thread.currentThread());
                TxnWaiter w = new TxnWaiter(5, self.get());
                tracker.registerWaiter(w);
                SqlContinuation.suspend();
                threadAfterResume.set(Thread.currentThread());
                writerTxnAtResume.set(tracker.getWriterTxn());
                doneLatch.countDown();
            }, resumeJob);
            self.set(cont);

            try {
                Thread runner = Thread.currentThread();
                cont.run();
                Assert.assertFalse("continuation must be parked awaiting writerTxn", cont.isDone());
                Assert.assertSame(runner, threadAtStart.get());

                pool.start();
                // Advance writerTxn past the target. updateWriterTxns fires the waiter,
                // which calls cont.scheduleResume() on the resume job.
                tracker.updateWriterTxns(5, 5);

                Assert.assertTrue("continuation did not resume", doneLatch.await(5, TimeUnit.SECONDS));
                Assert.assertTrue(cont.isDone());
                Assert.assertNotSame("body must resume on a worker thread", runner, threadAfterResume.get());
                Assert.assertTrue(
                        "resumed on a pool worker",
                        threadAfterResume.get().getName().contains("txn-waiter-advance-test")
                );
                Assert.assertEquals(Long.valueOf(5), writerTxnAtResume.get());
            } finally {
                pool.halt();
            }
        });
    }

    @Test
    public void testTxnWaiterResetAllowsRefire() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Pool contract: reset() clears state back to PENDING so the same instance can
            // serve a second wait. This is what makes WaitWalFunction's slow path
            // allocation-free after the pool is warm.
            ContinuationResumeJob resumeJob = new ContinuationResumeJob();
            SqlContinuation c1 = new SqlContinuation(() -> {
            }, resumeJob);
            SqlContinuation c2 = new SqlContinuation(() -> {
            }, resumeJob);
            TxnWaiter w = new TxnWaiter();
            w.reset(10, c1, 42L);
            Assert.assertFalse(w.isFired());
            Assert.assertFalse(w.isCancelled());
            Assert.assertTrue(w.tryFire());
            Assert.assertTrue(w.isFired());
            // tryFire is one-shot per state.
            Assert.assertFalse(w.tryFire());

            // Reset to a different task; FIRED -> PENDING.
            w.reset(20, c2, TxnWaiter.NO_DEADLINE);
            Assert.assertFalse(w.isFired());
            Assert.assertFalse(w.isCancelled());
            // Fire again -- proves state was reset.
            Assert.assertTrue(w.tryFire());

            // Reset once more and try the cancel path.
            w.reset(30, c1, TxnWaiter.NO_DEADLINE);
            Assert.assertTrue(w.tryCancel());
            Assert.assertTrue(w.isCancelled());
        });
    }

    @Test
    public void testPooledContinuationMultiTaskSameThread() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Validates the pooled-continuation pattern at the mechanism level: one
            // SqlContinuation whose body is a persistent loop that yields between tasks.
            // The same instance processes multiple tasks; the tail-yield parks it ready for
            // the next cont.run() call. Mirrors what PGConnectionContext.continuationLoop does.
            int[] taskInput = new int[1];
            int[] taskOutput = new int[1];
            boolean[] shutdown = new boolean[1];
            SqlContinuation cont = new SqlContinuation(() -> {
                while (!shutdown[0]) {
                    taskOutput[0] = taskInput[0] * 2;
                    SqlContinuation.suspend();
                }
            }, c -> {
            });

            taskInput[0] = 3;
            cont.run();
            Assert.assertFalse("loop must tail-yield, not complete", cont.isDone());
            Assert.assertEquals(6, taskOutput[0]);

            taskInput[0] = 5;
            cont.run();
            Assert.assertFalse(cont.isDone());
            Assert.assertEquals(10, taskOutput[0]);

            taskInput[0] = 7;
            cont.run();
            Assert.assertFalse(cont.isDone());
            Assert.assertEquals(14, taskOutput[0]);

            // Shutdown: the next run resumes past the tail yield, the while-check sees
            // shutdown==true, the body returns, and the continuation becomes done.
            shutdown[0] = true;
            cont.run();
            Assert.assertTrue("shutdown must terminate the loop", cont.isDone());
        });
    }

    @Test
    public void testDeadlineTriggersCancellation() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SeqTxnTracker tracker = new SeqTxnTracker(configuration());
            tracker.initTxns(1, 5, false);

            ContinuationResumeJob resumeJob = new ContinuationResumeJob();
            AtomicLong fakeNowMillis = new AtomicLong(1_000L);

            TestWorkerPool pool = new TestWorkerPool("txn-waiter-timeout-test", 1);
            pool.assign(resumeJob);
            WaiterTimeoutJob timeoutJob = new WaiterTimeoutJob(
                    fakeNowMillis::get,
                    1L, // 1ms sweep interval
                    consumer -> consumer.accept(tracker)
            );
            pool.assign(timeoutJob);

            AtomicReference<SqlContinuation> self = new AtomicReference<>();
            AtomicReference<Boolean> observedCancelled = new AtomicReference<>();
            CountDownLatch doneLatch = new CountDownLatch(1);

            long deadline = fakeNowMillis.get() + 50L; // 50ms in future

            SqlContinuation cont = new SqlContinuation(() -> {
                TxnWaiter w = new TxnWaiter(100, self.get(), deadline);
                tracker.registerWaiter(w);
                SqlContinuation.suspend();
                observedCancelled.set(w.isCancelled());
                doneLatch.countDown();
            }, resumeJob);
            self.set(cont);

            try {
                cont.run();
                Assert.assertFalse(cont.isDone());

                pool.start();
                // Advance virtual clock past the deadline; timeout job will sweep on its next tick.
                fakeNowMillis.set(deadline + 1);

                Assert.assertTrue("waiter did not time out", doneLatch.await(5, TimeUnit.SECONDS));
                Assert.assertEquals(Boolean.TRUE, observedCancelled.get());
            } finally {
                pool.halt();
            }
        });
    }

    @Test
    public void testSuspendResumeDrivenBySuspendState() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SeqTxnTracker tracker = new SeqTxnTracker(configuration());
            tracker.initTxns(1, 5, false);

            ContinuationResumeJob resumeJob = new ContinuationResumeJob();
            TestWorkerPool pool = new TestWorkerPool("txn-waiter-suspend-test", 1);
            pool.assign(resumeJob);

            AtomicReference<SqlContinuation> self = new AtomicReference<>();
            AtomicReference<Boolean> suspendedAtResume = new AtomicReference<>();
            CountDownLatch doneLatch = new CountDownLatch(1);

            SqlContinuation cont = new SqlContinuation(() -> {
                TxnWaiter w = new TxnWaiter(999, self.get());
                tracker.registerWaiter(w);
                SqlContinuation.suspend();
                suspendedAtResume.set(tracker.isSuspended());
                doneLatch.countDown();
            }, resumeJob);
            self.set(cont);

            try {
                cont.run();
                Assert.assertFalse(cont.isDone());

                pool.start();
                tracker.setSuspended(ErrorTag.NONE, "test");

                Assert.assertTrue(doneLatch.await(5, TimeUnit.SECONDS));
                Assert.assertEquals(Boolean.TRUE, suspendedAtResume.get());
            } finally {
                pool.halt();
            }
        });
    }

    private static CairoConfiguration configuration() {
        return new DefaultCairoConfiguration(null);
    }
}
