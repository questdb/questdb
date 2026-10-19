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

package io.questdb.test.cairo.mv;

import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.mv.MatViewRefreshTask;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class MatViewFiberRefreshTest extends AbstractCairoTest {
    private static final String CREATE_BASE_TABLE = """
            CREATE TABLE base_price (
                sym VARCHAR,
                price DOUBLE,
                ts TIMESTAMP
            ) TIMESTAMP(ts) PARTITION BY DAY WAL
            """;
    private static final String CREATE_MAT_VIEW = """
            CREATE MATERIALIZED VIEW price_1h AS
            SELECT sym, last(price) AS price, ts
            FROM base_price
            SAMPLE BY 1h
            """;

    @Test
    public void testFiberHostReservesOnlyForRefreshWork() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_BASE_TABLE);
            execute(CREATE_MAT_VIEW);
            drainWalAndMatViewQueues();

            final WorkerPool pool = newFiberHostPool();
            final FiberRuntime runtime = pool.getFiberRuntime();
            final MatViewRefreshJob job = new MatViewRefreshJob(engine, 1, runtime);
            pool.assign(job);
            try {
                Assert.assertFalse(job.run());
                Assert.assertEquals(0, runtime.getCreatedFiberCount());

                engine.getMatViewStateStore().enqueueUpdateRefreshIntervals(engine.verifyTableName("price_1h"));
                Assert.assertFalse(job.run());
                Assert.assertEquals(0, runtime.getCreatedFiberCount());

                engine.getMatViewStateStore().enqueueInvalidate(engine.verifyTableName("price_1h"), "test");
                Assert.assertFalse(job.run());
                Assert.assertEquals(0, runtime.getCreatedFiberCount());
            } finally {
                Assert.assertTrue(pool.haltWithin(TimeUnit.SECONDS.toNanos(10)));
            }
        });
    }

    @Test
    public void testFiberModeIncrementalAndFullRefresh() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_BASE_TABLE);
            execute(CREATE_MAT_VIEW);

            final WorkerPool pool = newFiberHostPool();
            final FiberRuntime runtime = pool.getFiberRuntime();
            pool.assign(new MatViewRefreshJob(engine, 1, runtime));
            pool.start();
            try {
                awaitRefresh(runtime, 0, "sym\tprice\tts\n");

                long previousMountCount = runtime.getMountCount();
                execute(
                        "insert into base_price (sym, price, ts) values('gbpusd', 1.320, '2024-09-10T12:01')" +
                                ",('gbpusd', 1.323, '2024-09-10T12:02')"
                );
                drainWalQueue();
                awaitRefresh(
                        runtime,
                        previousMountCount,
                        "sym\tprice\tts\n" +
                                "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n"
                );

                previousMountCount = runtime.getMountCount();
                execute("insert into base_price (sym, price, ts) values('gbpusd', 1.5, '2024-09-10T13:01')");
                drainWalQueue();
                awaitRefresh(
                        runtime,
                        previousMountCount,
                        "sym\tprice\tts\n" +
                                "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                                "gbpusd\t1.5\t2024-09-10T13:00:00.000000Z\n"
                );

                previousMountCount = runtime.getMountCount();
                execute("refresh materialized view price_1h full;");
                awaitRefresh(
                        runtime,
                        previousMountCount,
                        "sym\tprice\tts\n" +
                                "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                                "gbpusd\t1.5\t2024-09-10T13:00:00.000000Z\n"
                );

                Assert.assertEquals(1, runtime.getCreatedFiberCount());
                Assert.assertEquals(0, runtime.getOutstandingTaskCount());
                Assert.assertEquals(1, runtime.getRetainedFiberCount());
            } finally {
                Assert.assertTrue(pool.haltWithin(TimeUnit.SECONDS.toNanos(10)));
            }
        });
    }

    @Test
    public void testFiberSaturationLeavesRefreshQueued() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_BASE_TABLE);
            execute(CREATE_MAT_VIEW);
            drainWalAndMatViewQueues();

            final WorkerPool pool = newFiberHostPool();
            final FiberRuntime runtime = pool.getFiberRuntime();
            final MatViewRefreshJob job = new MatViewRefreshJob(engine, 1, runtime);
            final MatViewRefreshTask queuedTask = new MatViewRefreshTask();
            Fiber heldFiber = null;
            long heldFiberEpoch = 0;
            pool.assign(job);
            engine.getMatViewStateStore().enqueueFullRefresh(engine.verifyTableName("price_1h"));
            try {
                heldFiber = runtime.tryReserveFiber();
                Assert.assertNotNull(heldFiber);
                heldFiberEpoch = heldFiber.getReservationEpoch();
                Assert.assertFalse(job.run());

                Assert.assertTrue(engine.getMatViewStateStore().tryDequeueRefreshTask(queuedTask));
                Assert.assertEquals(MatViewRefreshTask.FULL_REFRESH, queuedTask.operation);
                engine.getMatViewStateStore().reenqueueRefreshTask(queuedTask);

                runtime.releaseReservedFiber(heldFiber, heldFiberEpoch);
                heldFiber = null;
                Assert.assertTrue(job.run());
                TestUtils.assertEventually(() -> {
                    runtime.drain(8);
                    Assert.assertEquals(0, runtime.getOutstandingTaskCount());
                });
                Assert.assertEquals(1, runtime.getMountCount());
            } finally {
                if (heldFiber != null) {
                    runtime.releaseReservedFiber(heldFiber, heldFiberEpoch);
                }
                closeRuntime(runtime);
                Assert.assertTrue(pool.haltWithin(TimeUnit.SECONDS.toNanos(10)));
            }
        });
    }

    @Test
    public void testQuiesceLeavesRefreshQueued() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_BASE_TABLE);
            execute(CREATE_MAT_VIEW);
            drainWalAndMatViewQueues();

            execute("insert into base_price (sym, price, ts) values('gbpusd', 1.5, '2024-09-10T13:01')");
            drainWalQueue();

            final WorkerPool pool = newFiberHostPool();
            final FiberRuntime runtime = pool.getFiberRuntime();
            final MatViewRefreshJob job = new MatViewRefreshJob(engine, 1, runtime);
            pool.assign(job);
            runtime.beginQuiesce();
            try {
                Assert.assertFalse(job.run());
                Assert.assertEquals(0, runtime.getLaunchCount(LaunchResult.QUIESCING));

                final MatViewRefreshTask task = new MatViewRefreshTask();
                Assert.assertTrue(engine.getMatViewStateStore().tryDequeueRefreshTask(task));
                Assert.assertEquals(MatViewRefreshTask.INCREMENTAL_REFRESH, task.operation);
                Assert.assertNotNull(task.baseTableToken);
                Assert.assertNull(task.matViewToken);
            } finally {
                Assert.assertTrue(pool.haltWithin(TimeUnit.SECONDS.toNanos(10)));
            }
        });
    }

    @Test
    public void testQuiesceBeforeFirstMountRequeuesRefresh() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_BASE_TABLE);
            execute(CREATE_MAT_VIEW);
            drainWalAndMatViewQueues();

            final WorkerPool pool = newFiberHostPool();
            final FiberRuntime runtime = pool.getFiberRuntime();
            final MatViewRefreshJob job = new MatViewRefreshJob(engine, 1, runtime);
            pool.assign(job);
            engine.getMatViewStateStore().enqueueRangeRefresh(engine.verifyTableName("price_1h"), 11, 22);
            try {
                Assert.assertTrue(job.run());
                Assert.assertEquals(1, runtime.getOutstandingTaskCount());
                Assert.assertEquals(1, runtime.getQueuedCount());

                runtime.beginQuiesce();
                TestUtils.assertEventually(() -> {
                    runtime.drain(8);
                    Assert.assertEquals(FiberRuntimeState.CLOSED, runtime.state());
                });

                final MatViewRefreshTask task = new MatViewRefreshTask();
                Assert.assertTrue(engine.getMatViewStateStore().tryDequeueRefreshTask(task));
                Assert.assertEquals(MatViewRefreshTask.RANGE_REFRESH, task.operation);
                Assert.assertEquals(11, task.rangeFrom);
                Assert.assertEquals(22, task.rangeTo);
                Assert.assertFalse(engine.getMatViewStateStore().tryDequeueRefreshTask(task));
            } finally {
                Assert.assertTrue(pool.haltWithin(TimeUnit.SECONDS.toNanos(10)));
            }
        });
    }

    @Test
    public void testShutdownWakesParkedRefresh() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_BASE_TABLE);
            execute(CREATE_MAT_VIEW);
            drainWalAndMatViewQueues();

            final MatViewState viewState = engine.getMatViewStateStore().getViewState(
                    engine.verifyTableName("price_1h")
            );
            Assert.assertNotNull(viewState);
            viewState.getViewDefinition().setMatViewSqlForTesting("""
                    SELECT b.sym, last(b.price) AS price, b.ts
                    FROM base_price b
                    CROSS JOIN sleep(60.0) s
                    WHERE s.sleep IS NOT NULL
                    SAMPLE BY 1h
                    """);
            execute("insert into base_price (sym, price, ts) values('gbpusd', 1.5, '2024-09-10T13:01')");
            drainWalQueue();

            final WorkerPool pool = newFiberHostPool();
            final FiberRuntime runtime = pool.getFiberRuntime();
            pool.assign(new MatViewRefreshJob(engine, 1, runtime));
            pool.start();
            try {
                awaitParkedRefresh(runtime);
                // Budget is far below the parked timer, so only a shutdown-driven wake can drain in
                // time; letting the timer expire on its own would not satisfy this.
                Assert.assertTrue(pool.haltWithin(TimeUnit.SECONDS.toNanos(5)));
                Assert.assertEquals(FiberRuntimeState.CLOSED, runtime.state());
                Assert.assertEquals(0, runtime.getOutstandingTaskCount());
            } finally {
                pool.haltWithin(TimeUnit.SECONDS.toNanos(10));
            }
        });
    }

    private static void awaitParkedRefresh(FiberRuntime runtime) throws Exception {
        TestUtils.assertEventually(() -> {
            Assert.assertTrue(runtime.getMountCount() > 0);
            Assert.assertEquals(0, runtime.getMountedCount());
            Assert.assertEquals(1, runtime.getOutstandingTaskCount());
            Assert.assertEquals(0, runtime.getQueuedCount());
            Assert.assertEquals(1, runtime.getCreatedFiberCount());
            Assert.assertEquals(0, runtime.getRetainedFiberCount());
            Assert.assertEquals(0, runtime.getRetiredFiberCount());
        });
    }

    private void awaitRefresh(FiberRuntime runtime, long previousMountCount, String expected) throws Exception {
        TestUtils.assertEventually(() -> {
            Assert.assertTrue(runtime.getMountCount() > previousMountCount);
            Assert.assertEquals(0, runtime.getOutstandingTaskCount());
            Assert.assertEquals(0, runtime.getQueuedCount());
            Assert.assertEquals(1, runtime.getRetainedFiberCount());
            drainWalQueue();
            assertQuery("price_1h").noLeakCheck().expectSize().timestamp("ts").returns(expected);
        });
    }

    private static void closeRuntime(FiberRuntime runtime) throws Exception {
        runtime.beginQuiesce();
        TestUtils.assertEventually(() -> {
            runtime.drain(8);
            Assert.assertEquals(FiberRuntimeState.CLOSED, runtime.state());
        });
    }

    private static WorkerPool newFiberHostPool() {
        return new WorkerPool(new WorkerPoolConfiguration() {
            @Override
            public int getFiberMaxLiveCount() {
                return 1;
            }

            @Override
            public int getFiberRetainedCount() {
                return 1;
            }

            @Override
            public String getPoolName() {
                return "mat-view-fiber-refresh-test";
            }

            @Override
            public int getWorkerCount() {
                return 1;
            }

            @Override
            public WorkerPoolMode getWorkerPoolMode() {
                return WorkerPoolMode.FIBER_HOST;
            }

            @Override
            public boolean isDaemonPool() {
                return true;
            }
        });
    }
}
