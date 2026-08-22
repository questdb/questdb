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

import io.questdb.mp.Worker;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.continuation.WorkerContinuation;
import io.questdb.std.Os;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class WorkerContinuationTest {

    @Test
    public void testHaltDrainsContinuationScheduledJustBeforeHalt() throws Exception {
        // Regression: a continuation scheduleResume()'d just before the pool halts must still be
        // resumed -- by the worker's shutdown drain -- so its body can unwind and release any
        // checked-out resource, instead of being stranded in the resume queue. This is the
        // worker-level shape of the sleep()-at-shutdown server socket leak: the parked query
        // continuation was scheduled by engine close but the worker exited without draining it.
        //
        // A handshake job makes the worker observably RUNNING before we schedule/halt, so halt()
        // cannot win the BORN->RUNNING race and skip the whole run() body (and the drain). The
        // resume is then enqueued while the single worker is parked in its idle backoff sleep,
        // past its in-loop dequeue, so the RUNNING loop exits via the top-of-loop check without
        // dequeuing the cont and only drainShutdownContinuations() can pick it up. Each resume
        // records whether it actually came through the drain (vs the in-loop dequeue), so a
        // regression in the drain cannot pass as a false green where the in-loop dequeue covers.
        boolean drainWasExercised = false;
        for (int attempt = 0; attempt < 8; attempt++) {
            CountDownLatch workerRunning = new CountDownLatch(1);
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
            // Handshake job: returns false (no runAsap) so the worker drops into its backoff
            // sleep after one idle iteration, and counts down once the worker is observably
            // RUNNING -- i.e. past the BORN->RUNNING CAS that halt() would otherwise race.
            pool.assign(_ -> {
                workerRunning.countDown();
                return false;
            });

            CountDownLatch resumed = new CountDownLatch(1);
            AtomicBoolean resumedByDrain = new AtomicBoolean();
            WorkerContinuation cont = new WorkerContinuation(() -> {
                WorkerContinuation.suspend();
                resumedByDrain.set(isResumedByShutdownDrain());
                resumed.countDown();
            }, pool.getContinuationSink());

            cont.run();
            Assert.assertFalse("continuation must be parked, not done (attempt " + attempt + ")", cont.isDone());

            pool.start();
            Assert.assertTrue(
                    "worker did not reach RUNNING (attempt " + attempt + ")",
                    workerRunning.await(10, TimeUnit.SECONDS)
            );
            // Let the worker settle past its in-loop dequeue into the backoff sleep, so the
            // resume lands while it is parked and the RUNNING loop exits without dequeuing it.
            Os.sleep(20);

            cont.scheduleResume();
            pool.halt(); // synchronous: returns only after the worker has exited (and drained)

            Assert.assertTrue(
                    "continuation scheduled before halt was stranded, not drained (attempt " + attempt + ")",
                    resumed.await(10, TimeUnit.SECONDS)
            );
            drainWasExercised |= resumedByDrain.get();
        }
        Assert.assertTrue(
                "the shutdown drain never resumed the continuation; every resume went through the "
                        + "in-loop dequeue, so drainShutdownContinuations() was not exercised",
                drainWasExercised
        );
    }

    @Test
    public void testHaltDropsPhantomResumeInsteadOfRetrying() throws Exception {
        // Counterpart to the re-enqueue path: the drain re-enqueues a continuation transiently
        // mounted on its registering carrier, but a phantom resume -- a refused suspend (carrier
        // pinned) whose queue entry is stale while the continuation keeps running on its polling
        // carrier -- must be DROPPED (park-refused consumed), not retried, or the drain would spin
        // forever on a continuation that never unmounts. A holder thread models the pinned carrier:
        // it keeps the continuation mounted and does not release it during the drain. With
        // park-refused set, halt must still complete promptly rather than block on a remount that
        // can never succeed.
        TestWorkerPool pool = new TestWorkerPool(new WorkerPoolConfiguration() {
            @Override
            public String getPoolName() {
                return "halt-phantom-test";
            }

            @Override
            public int getWorkerCount() {
                return 1;
            }
        });
        final CountDownLatch releaseHolder = new CountDownLatch(1);
        Thread holder = null;
        Thread halter = null;
        try {
            pool.start();

            CountDownLatch probeResumed = new CountDownLatch(1);
            WorkerContinuation probe = new WorkerContinuation(() -> {
                WorkerContinuation.suspend();
                probeResumed.countDown();
            }, pool.getContinuationSink());
            probe.run();
            probe.scheduleResume();
            Assert.assertTrue("worker not RUNNING", probeResumed.await(10, TimeUnit.SECONDS));

            CountDownLatch mountedOnHolder = new CountDownLatch(1);
            WorkerContinuation cont = new WorkerContinuation(() -> {
                mountedOnHolder.countDown();
                try {
                    releaseHolder.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, pool.getContinuationSink());
            holder = new Thread(cont::run, "phantom-holder");
            holder.start();
            Assert.assertTrue("holder did not mount the cont", mountedOnHolder.await(10, TimeUnit.SECONDS));

            // Phantom: a queue entry exists but the continuation stays mounted on the holder.
            // park-refused tells the drain to drop the entry rather than retry a remount that throws
            // for as long as the holder keeps it mounted.
            cont.markParkRefused();
            cont.scheduleResume();

            halter = new Thread(pool::halt, "halter");
            halter.start();
            halter.join(TimeUnit.SECONDS.toMillis(5));
            Assert.assertFalse("halt blocked: the phantom resume was retried instead of dropped", halter.isAlive());
        } finally {
            releaseHolder.countDown();
            if (halter != null) {
                halter.join(TimeUnit.SECONDS.toMillis(35));
            }
            if (holder != null) {
                holder.join(TimeUnit.SECONDS.toMillis(10));
            }
            pool.halt();
        }
    }

    @Test
    public void testHaltReEnqueuesContinuationStillMountedOnAnotherCarrier() throws Exception {
        // Regression for the multi-carrier shutdown window: a continuation can be scheduled for
        // resume while it is STILL mounted on the carrier that scheduled it -- the sleep() shutdown
        // path enqueues the cont before it reaches suspend(). A peer worker draining at halt dequeues
        // it but cannot remount it (cont.run() throws because the cont is mounted elsewhere). The
        // worker must re-enqueue and retry once that carrier unmounts, not abandon it off-queue with
        // its resource unreleased. A holder thread plays the carrier that still has the cont mounted;
        // the pool worker plays the peer drainer.
        for (int attempt = 0; attempt < 3; attempt++) {
            TestWorkerPool pool = new TestWorkerPool(new WorkerPoolConfiguration() {
                @Override
                public String getPoolName() {
                    return "halt-reenqueue-test";
                }

                @Override
                public int getWorkerCount() {
                    return 1;
                }
            });
            final CountDownLatch releaseHolder = new CountDownLatch(1);
            Thread holder = null;
            Thread halter = null;
            try {
                pool.start();

                // Prove the worker reached RUNNING, so the halt below moves it RUNNING -> HALTED and
                // runs its drain, rather than winning the BORN -> RUNNING CAS and skipping the body.
                CountDownLatch probeResumed = new CountDownLatch(1);
                WorkerContinuation probe = new WorkerContinuation(() -> {
                    WorkerContinuation.suspend();
                    probeResumed.countDown();
                }, pool.getContinuationSink());
                probe.run();
                probe.scheduleResume();
                Assert.assertTrue("worker not RUNNING (attempt " + attempt + ")", probeResumed.await(10, TimeUnit.SECONDS));

                CountDownLatch mountedOnHolder = new CountDownLatch(1);
                CountDownLatch resumed = new CountDownLatch(1);
                AtomicBoolean resumedByDrain = new AtomicBoolean();
                WorkerContinuation cont = new WorkerContinuation(() -> {
                    mountedOnHolder.countDown();
                    try {
                        releaseHolder.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    WorkerContinuation.suspend(); // unmount the holder; only a remount can advance the body
                    resumedByDrain.set(isResumedByShutdownDrain());
                    resumed.countDown();
                }, pool.getContinuationSink());

                holder = new Thread(cont::run, "holder-" + attempt);
                holder.start();
                Assert.assertTrue("holder did not mount the cont (attempt " + attempt + ")", mountedOnHolder.await(10, TimeUnit.SECONDS));

                // Enqueue while still mounted on the holder, then halt on a separate thread. With the
                // fix the worker re-enqueues the still-mounted cont and retries, so halt() blocks until
                // the cont is released and drained; without the fix the worker bails on the failed
                // remount, strands the cont, and halt() returns promptly.
                cont.scheduleResume();
                halter = new Thread(pool::halt, "halter-" + attempt);
                halter.start();
                halter.join(500);
                Assert.assertTrue("halt abandoned a continuation still mounted on a peer carrier (attempt " + attempt + ")", halter.isAlive());

                // Release the held mount: the cont unmounts, the worker's drain retry succeeds, and
                // the body runs to completion -- releasing whatever it had checked out.
                releaseHolder.countDown();
                Assert.assertTrue("continuation stranded at shutdown, not drained (attempt " + attempt + ")", resumed.await(10, TimeUnit.SECONDS));
                Assert.assertTrue("continuation was not resumed by the shutdown drain (attempt " + attempt + ")", resumedByDrain.get());
            } finally {
                releaseHolder.countDown();
                if (halter != null) {
                    halter.join(TimeUnit.SECONDS.toMillis(35));
                }
                if (holder != null) {
                    holder.join(TimeUnit.SECONDS.toMillis(10));
                }
                pool.halt();
            }
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

    // Runs inside the resumed continuation body, on the worker carrier. A mounted continuation
    // sees the carrier's live stack below it, so when the shutdown drain remounts the cont the
    // stack includes Worker.drainShutdownContinuations; the in-loop dequeue resume path does not.
    private static boolean isResumedByShutdownDrain() {
        for (StackTraceElement frame : Thread.currentThread().getStackTrace()) {
            if (Worker.class.getName().equals(frame.getClassName())
                    && "drainShutdownContinuations".equals(frame.getMethodName())) {
                return true;
            }
        }
        return false;
    }
}
