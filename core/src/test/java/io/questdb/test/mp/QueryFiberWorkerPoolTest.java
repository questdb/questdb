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
import io.questdb.mp.continuation.QueryFiberPool;
import io.questdb.mp.continuation.QueryTask;
import io.questdb.mp.continuation.WorkerContinuation;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Exercises the query-fiber tier end-to-end through a REAL worker pool: a launch
 * pushes the fiber onto the pool's ContinuationQueue, a worker's loopBody dequeues
 * and hands it to the outer driver, mountForeignCont mounts it, a wait park freezes
 * it, scheduleResume routes it back through the same queue (possibly to a peer
 * worker), and the outer driver's reclaim hook returns free-yielded fibers to the
 * pool. This is the transition-mode integration the fiber tier ships with.
 */
public class QueryFiberWorkerPoolTest {

    @Test
    public void testFiberHostPoolMountsFibersOnPlainLoop() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // end-state mode: plain worker loop, no worker-loop continuation, no
            // handoff -- fibers mount directly on the cont-free worker frame
            final TestWorkerPool pool = new TestWorkerPool(new WorkerPoolConfiguration() {
                @Override
                public String getPoolName() {
                    return "query-fiber-host-test";
                }

                @Override
                public int getWorkerCount() {
                    return 2;
                }

                @Override
                public boolean isFiberHost() {
                    return true;
                }
            });
            pool.assign(workerContext -> true);
            final QueryFiberPool fiberPool = new QueryFiberPool(2, pool.getContinuationSink());
            pool.start();
            try {
                final WaitingTask waiting = new WaitingTask();
                Assert.assertTrue(fiberPool.launch(waiting));
                TestUtils.assertEventually(() -> Assert.assertNotNull(waiting.parkedCont));
                Assert.assertEquals(QueryTask.STATE_RUNNING, waiting.getScheduleState());

                waiting.parkedCont.scheduleResume();
                TestUtils.assertEventually(() -> Assert.assertTrue(waiting.isDone()));
                Assert.assertNull(waiting.error);
                TestUtils.assertEventually(() -> Assert.assertEquals(1, fiberPool.getPooledCount()));
                Assert.assertEquals(1, fiberPool.getCreatedCount());

                for (int i = 0; i < 100; i++) {
                    final OneShotTask task = new OneShotTask();
                    Assert.assertTrue(fiberPool.launch(task));
                    TestUtils.assertEventually(() -> Assert.assertTrue(task.isDone()));
                    TestUtils.assertEventually(() -> Assert.assertEquals(1, fiberPool.getPooledCount()));
                }
                Assert.assertEquals(1, fiberPool.getCreatedCount());
                Assert.assertEquals(0, fiberPool.getRetiredCount());
            } finally {
                pool.halt();
                fiberPool.close();
            }
        });
    }

    @Test
    public void testLaunchParkResumeReclaimThroughWorkerPool() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final TestWorkerPool pool = new TestWorkerPool("query-fiber-pool-test", 2);
            // keep the worker loop hot so the dequeue/handoff dance runs without napping
            pool.assign(workerContext -> true);
            final QueryFiberPool fiberPool = new QueryFiberPool(2, pool.getContinuationSink());
            pool.start();
            try {
                // a task that parks in a wait deep inside its step
                final WaitingTask waiting = new WaitingTask();
                Assert.assertTrue(fiberPool.launch(waiting));
                TestUtils.assertEventually(() -> Assert.assertNotNull(waiting.parkedCont));
                Assert.assertEquals(QueryTask.STATE_RUNNING, waiting.getScheduleState());
                Assert.assertFalse(waiting.isDone());

                // the "waiter" fires: the frozen fiber flows back through the pool's
                // queue and thaws on whichever worker dequeues it
                waiting.parkedCont.scheduleResume();
                TestUtils.assertEventually(() -> Assert.assertTrue(waiting.isDone()));
                Assert.assertNull(waiting.error);

                // the outer driver's reclaim hook returned the fiber to the pool
                TestUtils.assertEventually(() -> Assert.assertEquals(1, fiberPool.getPooledCount()));
                Assert.assertEquals(1, fiberPool.getCreatedCount());

                // sequential tasks keep reusing the same fiber; await the reclaim
                // between launches so the pool is never empty at acquire time
                for (int i = 0; i < 100; i++) {
                    final OneShotTask task = new OneShotTask();
                    Assert.assertTrue(fiberPool.launch(task));
                    TestUtils.assertEventually(() -> Assert.assertTrue(task.isDone()));
                    TestUtils.assertEventually(() -> Assert.assertEquals(1, fiberPool.getPooledCount()));
                }
                Assert.assertEquals(1, fiberPool.getCreatedCount());
                Assert.assertEquals(0, fiberPool.getRetiredCount());
            } finally {
                pool.halt();
                fiberPool.close();
            }
        });
    }

    private static class OneShotTask extends QueryTask {
        volatile boolean hasRun;

        @Override
        protected boolean runStep() {
            hasRun = true;
            return true;
        }
    }

    private static class WaitingTask extends QueryTask {
        volatile Throwable error;
        volatile WorkerContinuation parkedCont;

        @Override
        protected void onError(Throwable th) {
            error = th;
        }

        @Override
        protected boolean runStep() {
            if (parkedCont == null) {
                final WorkerContinuation cont = WorkerContinuation.current();
                parkedCont = cont;
                // the resume may be scheduled before this suspend completes; the
                // dequeuing peer spins through the benign mount race until we unmount
                if (!WorkerContinuation.suspend()) {
                    error = new IllegalStateException("park refused");
                }
            }
            return true;
        }
    }
}
