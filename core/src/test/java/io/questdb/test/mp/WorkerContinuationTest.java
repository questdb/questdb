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

import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.continuation.WorkerContinuation;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class WorkerContinuationTest {

    @Test
    public void testHaltDrainsContinuationScheduledJustBeforeHalt() throws Exception {
        // Regression: a continuation scheduled for resume just before the pool halts must
        // still be resumed so its body can unwind and release any checked-out resource,
        // instead of being stranded in the resume queue. This is the worker-level shape of
        // the sleep()-at-shutdown server socket leak: the parked query continuation was
        // scheduleResume()'d by engine close but the worker exited without draining it.
        for (int i = 0; i < 5; i++) {
            // Single worker that drops into a long backoff sleep after one idle iteration,
            // so when we schedule the resume it is asleep (past its last empty dequeue) and
            // exits its RUNNING loop on halt without a further dequeue -- isolating the
            // shutdown drain from the normal in-loop dequeue.
            TestWorkerPool pool = new TestWorkerPool(new WorkerPoolConfiguration() {
                @Override
                public String getPoolName() {
                    return "halt-drain-test";
                }

                @Override
                public long getSleepThreshold() {
                    return 0;
                }

                @Override
                public long getSleepTimeout() {
                    return 100;
                }

                @Override
                public int getWorkerCount() {
                    return 1;
                }
            });
            CountDownLatch resumed = new CountDownLatch(1);
            WorkerContinuation cont = new WorkerContinuation(() -> {
                WorkerContinuation.suspend();
                resumed.countDown();
            }, pool.getContinuationSink());

            cont.run();
            Assert.assertFalse("continuation must be parked, not done (iter " + i + ")", cont.isDone());

            pool.start();
            Thread.sleep(50); // let the worker reach its backoff sleep
            cont.scheduleResume();
            pool.halt(); // synchronous: returns only after the worker has exited (and drained)

            Assert.assertTrue(
                    "continuation scheduled before halt was stranded, not drained (iter " + i + ")",
                    resumed.await(10, TimeUnit.SECONDS)
            );
        }
    }

    @Test
    public void testIsMountedFalseOutsideRun() {
        Assert.assertFalse(WorkerContinuation.isMounted());
    }

    @Test
    public void testIsMountedTrueInsideRun() {
        AtomicReference<Boolean> mountedInsideBody = new AtomicReference<>();
        WorkerContinuation cont = new WorkerContinuation(
                () -> mountedInsideBody.set(WorkerContinuation.isMounted()),
                c -> {
                }
        );
        cont.run();
        Assert.assertTrue(cont.isDone());
        Assert.assertEquals(Boolean.TRUE, mountedInsideBody.get());
        Assert.assertFalse(WorkerContinuation.isMounted());
    }

    @Test
    public void testMultipleYieldsInOneContinuation() {
        AtomicReference<Integer> step = new AtomicReference<>(0);
        WorkerContinuation cont = new WorkerContinuation(() -> {
            step.set(1);
            WorkerContinuation.suspend();
            step.set(2);
            WorkerContinuation.suspend();
            step.set(3);
        }, c -> {
        });
        cont.run();
        Assert.assertFalse(cont.isDone());
        Assert.assertEquals(Integer.valueOf(1), step.get());

        cont.run();
        Assert.assertFalse(cont.isDone());
        Assert.assertEquals(Integer.valueOf(2), step.get());

        cont.run();
        Assert.assertTrue(cont.isDone());
        Assert.assertEquals(Integer.valueOf(3), step.get());
    }

    @Test
    public void testSuspendResumeOnDifferentThread() throws InterruptedException {
        AtomicReference<Thread> threadAtStart = new AtomicReference<>();
        AtomicReference<Thread> threadAfterResume = new AtomicReference<>();
        CountDownLatch doneLatch = new CountDownLatch(1);

        TestWorkerPool pool = new TestWorkerPool("sql-continuation-test", 1);
        // Cont's sink is the pool's resume queue; the pool's worker outer driver
        // drains the queue between mounts, so cont.scheduleResume() lands on a
        // worker of this pool.
        WorkerContinuation cont = new WorkerContinuation(() -> {
            threadAtStart.set(Thread.currentThread());
            WorkerContinuation.suspend();
            threadAfterResume.set(Thread.currentThread());
            doneLatch.countDown();
        }, pool.getContinuationSink());

        try {
            Thread runner = Thread.currentThread();
            cont.run();
            Assert.assertFalse("continuation must be parked, not done", cont.isDone());
            Assert.assertSame("body start ran on the caller thread", runner, threadAtStart.get());
            Assert.assertNull("body has not yet passed suspend()", threadAfterResume.get());

            pool.start();
            cont.scheduleResume();

            Assert.assertTrue("continuation resume timed out", doneLatch.await(5, TimeUnit.SECONDS));
            // Body counts down the latch as its last statement, but the worker may
            // still be unwinding out of cont.run() -- isDone() only flips once that returns.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (!cont.isDone() && System.nanoTime() < deadline) {
                Thread.sleep(1);
            }
            Assert.assertTrue("cont must be marked done after body completion", cont.isDone());
            Assert.assertNotNull(threadAfterResume.get());
            Assert.assertNotSame(
                    "resumed body must run on a worker thread, not the caller",
                    runner,
                    threadAfterResume.get()
            );
            Assert.assertTrue(
                    "resumed body must run on a pool worker",
                    threadAfterResume.get().getName().contains("sql-continuation-test")
            );
        } finally {
            pool.halt();
        }
    }
}
