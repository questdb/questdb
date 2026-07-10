/*******************************************************************************
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

import io.questdb.mp.continuation.BackpressureSignal;
import io.questdb.mp.continuation.ContinuationQueue;
import io.questdb.mp.continuation.QueryFiber;
import io.questdb.mp.continuation.QueryFiberPool;
import io.questdb.mp.continuation.QueryTask;
import io.questdb.mp.continuation.TxnWaiter;
import io.questdb.mp.continuation.WorkerContinuation;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Exercises the query-fiber tier in isolation, driven from plain code the way the
 * production paths drive it: {@link QueryFiberPool#launch(QueryTask)} pushes a
 * claimed task's fiber onto a {@link ContinuationQueue}, and the test's drive loop
 * stands in for a worker -- dequeue, drop phantoms, mount, reclaim on free-yield.
 * The single-threaded drive makes every park/resume interleaving deterministic.
 */
public class QueryFiberTest {

    @Test
    public void testBackpressureParkGateOrderingAndResume() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final ContinuationQueue queue = new ContinuationQueue();
            try (QueryFiberPool pool = new QueryFiberPool(4, queue)) {
                final BackpressureTask task = new BackpressureTask();

                Assert.assertTrue(pool.launch(task));
                Assert.assertEquals(1, drainFiberQueue(queue));

                // parked: gate back to IDLE, hook invoked AFTER the gate accepted
                // re-launching (the no-lost-wakeup ordering)
                Assert.assertEquals(QueryTask.STATE_IDLE, task.getScheduleState());
                Assert.assertTrue(task.hasParked);
                Assert.assertEquals(QueryTask.STATE_IDLE, task.observedStateInHook);
                Assert.assertFalse(task.isDone());

                // simulated fd WRITE-ready event
                Assert.assertTrue(pool.launch(task));
                Assert.assertEquals(1, drainFiberQueue(queue));
                Assert.assertTrue(task.isDone());
                Assert.assertEquals(2, task.runs);
                Assert.assertEquals(1, pool.getCreatedCount());
            }
        });
    }

    @Test
    public void testCancelPreventsLaunch() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final ContinuationQueue queue = new ContinuationQueue();
            try (QueryFiberPool pool = new QueryFiberPool(4, queue)) {
                final OneShotTask task = new OneShotTask();

                Assert.assertTrue(task.tryCancel());
                Assert.assertTrue(task.isCancelled());
                // a cancelled task cannot be launched without reopen()
                Assert.assertFalse(pool.launch(task));
                Assert.assertEquals(0, drainFiberQueue(queue));
                Assert.assertFalse(task.hasRun);
                Assert.assertEquals(0, pool.getCreatedCount());

                // reopen makes it launchable again
                task.reopen();
                Assert.assertTrue(pool.launch(task));
                Assert.assertEquals(1, drainFiberQueue(queue));
                Assert.assertTrue(task.isDone());
            }
        });
    }

    @Test
    public void testCloseRetiresFreeFibers() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final ContinuationQueue queue = new ContinuationQueue();
            final QueryFiberPool pool = new QueryFiberPool(4, queue);
            final OneShotTask task = new OneShotTask();
            Assert.assertTrue(pool.launch(task));
            Assert.assertEquals(1, drainFiberQueue(queue));
            Assert.assertTrue(task.isDone());

            Assert.assertEquals(1, pool.getPooledCount());
            pool.close();
            Assert.assertEquals(0, pool.getPooledCount());
            Assert.assertEquals(1, pool.getRetiredCount());
        });
    }

    @Test
    public void testCloseWithFrozenFiberRetiresOnThaw() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final ContinuationQueue queue = new ContinuationQueue();
            final QueryFiberPool pool = new QueryFiberPool(4, queue);
            final WaitingTask task = new WaitingTask();

            Assert.assertTrue(pool.launch(task));
            Assert.assertEquals(1, drainFiberQueue(queue));
            Assert.assertEquals(QueryTask.STATE_RUNNING, task.getScheduleState());

            // close while the fiber is frozen in the wait: the shutdown flag is set
            // on every fiber, but the frozen one completes only on its next thaw
            pool.close();
            Assert.assertFalse(task.parkedCont.isDone());

            // waiter fires after close; the thawed body finishes the step, observes
            // shutdown at the loop top, completes and is retired
            task.parkedCont.scheduleResume();
            Assert.assertEquals(1, drainFiberQueue(queue));
            Assert.assertTrue(task.isDone());
            Assert.assertNull(task.error);
            Assert.assertTrue(task.parkedCont.isDone());
            Assert.assertEquals(1, pool.getRetiredCount());
        });
    }

    @Test
    public void testCloseBeforeFirstMountReleasesStagedTask() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final ContinuationQueue queue = new ContinuationQueue();
            final QueryFiberPool pool = new QueryFiberPool(4, queue);
            final ReleaseTrackingTask task = new ReleaseTrackingTask();

            // launched (gate RUNNING, fiber staged on the queue) but not yet mounted
            Assert.assertTrue(pool.launch(task));
            // pool closes first: the fiber is marked for shutdown before its first mount
            pool.close();

            // the drive loop mounts the fiber; the body observes shutdown and must
            // drive the staged task's terminal hooks instead of abandoning it
            Assert.assertEquals(1, drainFiberQueue(queue));
            Assert.assertFalse(task.hasRun);
            Assert.assertTrue(task.isDone());
            Assert.assertTrue(task.hasBeenAbandoned);
            Assert.assertTrue(task.hasReleasedResources);
            Assert.assertEquals(1, pool.getRetiredCount());
        });
    }

    @Test
    public void testEmbeddedTxnWaiterReusedAcrossWaits() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final ContinuationQueue queue = new ContinuationQueue();
            try (QueryFiberPool pool = new QueryFiberPool(4, queue)) {
                final DoubleWaitTask task = new DoubleWaitTask();
                Assert.assertTrue(pool.launch(task));
                // frozen in the first wait
                Assert.assertEquals(1, drainFiberQueue(queue));
                Assert.assertNotNull(task.firstWaiter);
                Assert.assertFalse(task.isDone());

                // fire wait 1: the body cancels it (terminal), then acquires again
                // and freezes in the second wait on the SAME embedded instance
                task.firstWaiter.tryFire();
                Assert.assertEquals(1, drainFiberQueue(queue));
                Assert.assertSame(task.firstWaiter, task.secondWaiter);
                Assert.assertFalse(task.isDone());

                task.secondWaiter.tryFire();
                Assert.assertEquals(1, drainFiberQueue(queue));
                Assert.assertTrue(task.isDone());
                Assert.assertNull(task.error);
            }
        });
    }

    @Test
    public void testErrorInStepMarksTaskDoneAndFiberSurvives() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final ContinuationQueue queue = new ContinuationQueue();
            try (QueryFiberPool pool = new QueryFiberPool(4, queue)) {
                final FailingTask failing = new FailingTask();

                Assert.assertTrue(pool.launch(failing));
                Assert.assertEquals(1, drainFiberQueue(queue));
                Assert.assertTrue(failing.isDone());
                Assert.assertTrue(failing.observedError instanceof UnsupportedOperationException);

                // the fiber survived the throwing step and is reused as-is
                final OneShotTask task = new OneShotTask();
                Assert.assertTrue(pool.launch(task));
                Assert.assertEquals(1, drainFiberQueue(queue));
                Assert.assertTrue(task.isDone());
                Assert.assertEquals(1, pool.getCreatedCount());
            }
        });
    }

    @Test
    public void testFiberReuseAcrossManyTasks() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final ContinuationQueue queue = new ContinuationQueue();
            try (QueryFiberPool pool = new QueryFiberPool(4, queue)) {
                for (int i = 0; i < 10_000; i++) {
                    final OneShotTask task = new OneShotTask();
                    Assert.assertTrue(pool.launch(task));
                    Assert.assertEquals(1, drainFiberQueue(queue));
                    Assert.assertTrue(task.hasRun);
                    Assert.assertTrue(task.isDone());
                }
                // one fiber, one native stack chunk, reused for all 10k tasks
                Assert.assertEquals(1, pool.getCreatedCount());
                Assert.assertEquals(1, pool.getPooledCount());
                Assert.assertEquals(0, pool.getRetiredCount());
            }
        });
    }

    @Test
    public void testPhantomResumeDropped() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final ContinuationQueue queue = new ContinuationQueue();
            try (QueryFiberPool pool = new QueryFiberPool(4, queue)) {
                final WaitingTask task = new WaitingTask();

                Assert.assertTrue(pool.launch(task));
                Assert.assertEquals(1, drainFiberQueue(queue));
                Assert.assertEquals(QueryTask.STATE_RUNNING, task.getScheduleState());

                // simulate the phantom shape: a wakeup enqueued the cont, but the
                // park was refused and parkRefused was marked
                task.parkedCont.markParkRefused();
                queue.put(task.parkedCont);
                Assert.assertEquals(1, drainFiberQueue(queue));
                // dropped: the fiber was not thawed
                Assert.assertFalse(task.isDone());
                Assert.assertEquals(-1, task.frameSum);
                Assert.assertEquals(QueryTask.STATE_RUNNING, task.getScheduleState());

                // the real resume still works; the flag was consumed exactly once
                task.parkedCont.scheduleResume();
                Assert.assertEquals(1, drainFiberQueue(queue));
                Assert.assertTrue(task.isDone());
                Assert.assertNull(task.error);
            }
        });
    }

    @Test
    public void testSpikeAllocationPastCap() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final ContinuationQueue queue = new ContinuationQueue();
            try (QueryFiberPool pool = new QueryFiberPool(1, queue)) {
                // first task freezes in a wait, holding fiber 1
                final WaitingTask waiting = new WaitingTask();
                Assert.assertTrue(pool.launch(waiting));
                Assert.assertEquals(1, drainFiberQueue(queue));
                Assert.assertEquals(1, pool.getCreatedCount());
                Assert.assertEquals(0, pool.getPooledCount());

                // second task cannot be refused admission: fiber 2 is spike-created
                final OneShotTask oneShot = new OneShotTask();
                Assert.assertTrue(pool.launch(oneShot));
                Assert.assertEquals(1, drainFiberQueue(queue));
                Assert.assertTrue(oneShot.isDone());
                Assert.assertEquals(2, pool.getCreatedCount());
                Assert.assertEquals(1, pool.getPooledCount());

                // fiber 1 thaws and completes; the free-list is full, so it retires
                waiting.parkedCont.scheduleResume();
                Assert.assertEquals(1, drainFiberQueue(queue));
                Assert.assertTrue(waiting.isDone());
                Assert.assertNull(waiting.error);
                Assert.assertEquals(1, pool.getPooledCount());
                Assert.assertEquals(1, pool.getRetiredCount());
            }
        });
    }

    @Test
    public void testTaskReopenAllowsReuse() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final ContinuationQueue queue = new ContinuationQueue();
            try (QueryFiberPool pool = new QueryFiberPool(4, queue)) {
                final OneShotTask task = new OneShotTask();

                Assert.assertTrue(pool.launch(task));
                Assert.assertEquals(1, drainFiberQueue(queue));
                Assert.assertTrue(task.isDone());

                task.reopen();
                task.hasRun = false;
                Assert.assertEquals(QueryTask.STATE_IDLE, task.getScheduleState());
                Assert.assertTrue(pool.launch(task));
                Assert.assertEquals(1, drainFiberQueue(queue));
                Assert.assertTrue(task.hasRun);
                Assert.assertTrue(task.isDone());
                Assert.assertEquals(1, pool.getCreatedCount());
            }
        });
    }

    @Test
    public void testWaitParkAndResumeRestoresDeepFrames() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final ContinuationQueue queue = new ContinuationQueue();
            try (QueryFiberPool pool = new QueryFiberPool(4, queue)) {
                final WaitingTask task = new WaitingTask();

                Assert.assertTrue(pool.launch(task));
                Assert.assertEquals(1, drainFiberQueue(queue));

                // frozen deep inside runStep: the task stays RUNNING (wait park),
                // the fiber is out of the pool, and the gate refuses re-launching
                Assert.assertEquals(QueryTask.STATE_RUNNING, task.getScheduleState());
                Assert.assertNotNull(task.parkedCont);
                Assert.assertFalse(task.parkedCont.isDone());
                Assert.assertEquals(0, pool.getPooledCount());
                Assert.assertFalse(pool.launch(task));
                Assert.assertEquals(-1, task.frameSum);

                // the waiter fires: the frozen fiber flows back through the queue,
                // thaws in place and completes the step
                task.parkedCont.scheduleResume();
                Assert.assertEquals(1, drainFiberQueue(queue));
                Assert.assertTrue(task.isDone());
                Assert.assertNull(task.error);
                // per-frame locals survived the freeze/thaw round trip
                Assert.assertEquals(152, task.frameSum);
                Assert.assertEquals(1, pool.getCreatedCount());
                Assert.assertEquals(1, pool.getPooledCount());
            }
        });
    }

    /**
     * Stands in for a worker's drive loop: dequeue, drop phantoms exactly like
     * {@code Worker.mountForeignCont}, mount, reclaim on free-yield. Returns the
     * number of queue items consumed (including dropped phantoms).
     */
    private static int drainFiberQueue(ContinuationQueue queue) {
        final ContinuationQueue.ResumeTask scratch = new ContinuationQueue.ResumeTask();
        int count = 0;
        WorkerContinuation cont;
        while ((cont = queue.tryDequeue(scratch)) != null) {
            count++;
            if (cont.consumeParkRefused()) {
                // phantom: the cont is still mounted on its refusing carrier
                continue;
            }
            if (!cont.isDone()) {
                cont.run();
            }
            QueryFiber.reclaimIfIdle(cont);
        }
        return count;
    }

    /**
     * Mimics two consecutive wait_wal_table calls in one step: acquire, park, get
     * fired, cancel (terminal), then acquire again -- on a fiber the second acquire
     * must hand back the same embedded instance, re-armed.
     */
    private static class DoubleWaitTask extends QueryTask {
        Throwable error;
        volatile TxnWaiter firstWaiter;
        volatile TxnWaiter secondWaiter;

        @Override
        protected void onError(Throwable th) {
            error = th;
        }

        @Override
        protected boolean runStep() {
            if (firstWaiter == null) {
                final TxnWaiter waiter = TxnWaiter.acquire(null, TxnWaiter.NO_DELAY, 1);
                Assert.assertTrue(waiter.tryBindCurrent());
                firstWaiter = waiter;
                Assert.assertTrue(waiter.suspend());
                waiter.cancel();

                final TxnWaiter next = TxnWaiter.acquire(null, TxnWaiter.NO_DELAY, 2);
                Assert.assertTrue(next.tryBindCurrent());
                secondWaiter = next;
                Assert.assertTrue(next.suspend());
                next.cancel();
            }
            return true;
        }
    }

    private static class BackpressureTask extends QueryTask {
        boolean hasParked;
        int observedStateInHook = -1;
        int runs;

        @Override
        protected void onParked() {
            hasParked = true;
            observedStateInHook = getScheduleState();
        }

        @Override
        protected boolean runStep() throws BackpressureSignal {
            if (++runs == 1) {
                throw BackpressureSignal.INSTANCE;
            }
            return true;
        }
    }

    private static class FailingTask extends QueryTask {
        Throwable observedError;

        @Override
        protected void onError(Throwable th) {
            observedError = th;
        }

        @Override
        protected boolean runStep() {
            throw new UnsupportedOperationException("boom");
        }
    }

    private static class OneShotTask extends QueryTask {
        boolean hasRun;

        @Override
        protected boolean runStep() {
            hasRun = true;
            return true;
        }
    }

    private static class ReleaseTrackingTask extends QueryTask {
        boolean hasBeenAbandoned;
        boolean hasReleasedResources;
        boolean hasRun;

        @Override
        protected void onAbandoned() {
            hasBeenAbandoned = true;
        }

        @Override
        protected void onDone() {
            hasReleasedResources = true;
        }

        @Override
        protected boolean runStep() {
            hasRun = true;
            return true;
        }
    }

    /**
     * Mimics a wait function: on first entry it descends a recursive call chain,
     * binds the mounted continuation and suspends at the bottom, carrying live
     * per-frame state. On thaw it unwinds, folding the per-frame carries into
     * {@link #frameSum} to prove the frames were restored intact.
     */
    private static class WaitingTask extends QueryTask {
        Throwable error;
        long frameSum = -1;
        boolean hasResumed;
        volatile WorkerContinuation parkedCont;

        @Override
        protected void onError(Throwable th) {
            error = th;
        }

        @Override
        protected boolean runStep() {
            if (!hasResumed) {
                hasResumed = true;
                frameSum = deepPark(16, 0);
            }
            return true;
        }

        private long deepPark(int depth, long carry) {
            if (depth == 0) {
                parkedCont = WorkerContinuation.current();
                Assert.assertTrue(WorkerContinuation.suspend());
                return carry;
            }
            return deepPark(depth - 1, carry + depth) + 1;
        }
    }
}
