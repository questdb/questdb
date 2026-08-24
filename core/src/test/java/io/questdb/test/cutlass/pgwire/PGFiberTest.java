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

package io.questdb.test.cutlass.pgwire;

import io.questdb.Metrics;
import io.questdb.cutlass.pgwire.PGConfiguration;
import io.questdb.cutlass.pgwire.PGConnectionContext;
import io.questdb.cutlass.pgwire.PGServer;
import io.questdb.mp.Job;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.network.IODispatcher;
import io.questdb.network.IORequestProcessor;
import io.questdb.std.LongList;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Exercises PGWire query execution on pooled fibers.
 */
public class PGFiberTest extends BasePGTest {

    @Test
    public void testQueriesRunOnFiberHostPool() throws Exception {
        assertMemoryLeak(() -> {
            final PGConfiguration conf = new Port0PGConfiguration(-1) {
                @Override
                public WorkerPoolMode getWorkerPoolMode() {
                    return WorkerPoolMode.FIBER_HOST;
                }

                @Override
                public boolean isFiberEnabled() {
                    return true;
                }
            };
            try (
                    PGServer server = createPGServer(conf);
                    WorkerPool workerPool = server.getWorkerPool()
            ) {
                workerPool.start(LOG);
                TestUtils.assertEventually(() -> Assert.assertTrue(server.isListening()));
                try (Connection connection = getConnection(Mode.EXTENDED, server.getPort(), true)) {
                    try (ResultSet rs = connection.createStatement().executeQuery("select 42 x")) {
                        Assert.assertTrue(rs.next());
                        Assert.assertEquals(42, rs.getInt(1));
                    }
                    final long sleepStart = System.nanoTime();
                    try (ResultSet rs = connection.createStatement().executeQuery("select * from sleep(0.3)")) {
                        Assert.assertTrue(rs.next());
                    }
                    final long sleepMillis = (System.nanoTime() - sleepStart) / 1_000_000;
                    Assert.assertTrue("sleep completed too early: " + sleepMillis + "ms", sleepMillis >= 290);
                    try (ResultSet rs = connection.createStatement().executeQuery("select 43 x")) {
                        Assert.assertTrue(rs.next());
                        Assert.assertEquals(43, rs.getInt(1));
                    }
                }
            }
        });
    }

    @Test
    public void testQueriesRunOnPooledFibers() throws Exception {
        assertMemoryLeak(() -> {
            final PGConfiguration conf = new Port0PGConfiguration(-1) {
                @Override
                public boolean isFiberEnabled() {
                    return true;
                }
            };
            try (
                    PGServer server = createPGServer(conf);
                    WorkerPool workerPool = server.getWorkerPool()
            ) {
                workerPool.start(LOG);
                TestUtils.assertEventually(() -> Assert.assertTrue(server.isListening()));
                try (Connection connection = getConnection(Mode.EXTENDED, server.getPort(), true)) {
                    // a plain query end-to-end on a fiber
                    try (ResultSet rs = connection.createStatement().executeQuery("select 42 x")) {
                        Assert.assertTrue(rs.next());
                        Assert.assertEquals(42, rs.getInt(1));
                    }
                    // a parking query: sleep() freezes the fiber on a timer wait; the
                    // timer fires and the frozen fiber resumes through the network
                    // pool's continuation queue to finish streaming the result
                    final long sleepStart = System.nanoTime();
                    try (ResultSet rs = connection.createStatement().executeQuery("select * from sleep(0.3)")) {
                        Assert.assertTrue(rs.next());
                    }
                    final long sleepMillis = (System.nanoTime() - sleepStart) / 1_000_000;
                    Assert.assertTrue("sleep completed too early: " + sleepMillis + "ms", sleepMillis >= 290);
                    // the same connection keeps reusing its task and the pooled fiber
                    try (ResultSet rs = connection.createStatement().executeQuery("select 43 x")) {
                        Assert.assertTrue(rs.next());
                        Assert.assertEquals(43, rs.getInt(1));
                    }
                }
            }
        });
    }

    @Test
    public void testRequestJobSkipsIoQueueWhenSaturated() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final Fiber heldFiber = runtime.tryReserveFiber();
            Assert.assertNotNull(heldFiber);
            final long heldFiberEpoch = heldFiber.getReservationEpoch();
            try {
                final SaturatedTestPGDispatcher dispatcher = new SaturatedTestPGDispatcher();

                Assert.assertFalse(PGServer.runFiberRequestJobForTesting(
                        dispatcher,
                        Metrics.DISABLED,
                        runtime
                ));
                Assert.assertEquals(0, dispatcher.getProcessCount());
                Assert.assertTrue(dispatcher.hasPendingIOEvents());

                runtime.releaseReservedFiber(heldFiber, heldFiberEpoch);
                Assert.assertFalse(PGServer.runFiberRequestJobForTesting(
                        dispatcher,
                        Metrics.DISABLED,
                        runtime
                ));
                Assert.assertEquals(1, dispatcher.getProcessCount());
                Assert.assertFalse(dispatcher.hasPendingIOEvents());
            } finally {
                if (runtime.getOutstandingTaskCount() > 0) {
                    runtime.releaseReservedFiber(heldFiber, heldFiberEpoch);
                }
                closeFiberRuntime(runtime);
            }
        });
    }

    @Test
    public void testSaturationKeepsIoEventQueued() throws Exception {
        assertMemoryLeak(() -> {
            final PGConfiguration conf = new Port0PGConfiguration(-1) {
                @Override
                public int getFiberMaxLiveCount() {
                    return 1;
                }

                @Override
                public int getFiberRetainedCount() {
                    return 1;
                }

                @Override
                public boolean isFiberEnabled() {
                    return true;
                }
            };
            try (
                    PGServer server = createPGServer(conf);
                    WorkerPool workerPool = server.getWorkerPool()
            ) {
                workerPool.start(LOG);
                TestUtils.assertEventually(() -> Assert.assertTrue(server.isListening()));

                final CountDownLatch completed = new CountDownLatch(2);
                final AtomicReference<Throwable> error = new AtomicReference<>();
                final Thread sleepingQuery = new Thread(() -> {
                    try (
                            Connection connection = getConnection(Mode.EXTENDED, server.getPort(), true);
                            Statement statement = connection.createStatement();
                            ResultSet resultSet = statement.executeQuery("SELECT * FROM sleep(1.0)")
                    ) {
                        Assert.assertTrue(resultSet.next());
                    } catch (Throwable th) {
                        error.compareAndSet(null, th);
                    } finally {
                        completed.countDown();
                    }
                });
                sleepingQuery.start();

                final LongList queryIds = new LongList();
                TestUtils.assertEventually(() -> {
                    engine.getQueryRegistry().getEntryIds(queryIds);
                    Assert.assertEquals(1, queryIds.size());
                });

                final Thread queuedQuery = new Thread(() -> {
                    try (
                            Connection connection = getConnection(Mode.EXTENDED, server.getPort(), true);
                            Statement statement = connection.createStatement();
                            ResultSet resultSet = statement.executeQuery("SELECT 42")
                    ) {
                        Assert.assertTrue(resultSet.next());
                        Assert.assertEquals(42, resultSet.getInt(1));
                    } catch (Throwable th) {
                        error.compareAndSet(null, th);
                    } finally {
                        completed.countDown();
                    }
                });
                queuedQuery.start();

                Assert.assertTrue(completed.await(10, TimeUnit.SECONDS));
                sleepingQuery.join();
                queuedQuery.join();
                Assert.assertNull(error.get());
            }
        });
    }

    @Test
    public void testWorkerPoolModeControlsFiberExecution() throws Exception {
        assertMemoryLeak(() -> {
            assertQueryExecutionMode(false, WorkerPoolMode.LEGACY, false);
            assertQueryExecutionMode(false, WorkerPoolMode.FIBER_HOST, false);
            assertQueryExecutionMode(true, WorkerPoolMode.LEGACY, false);
            assertQueryExecutionMode(true, WorkerPoolMode.FIBER_HOST, true);
        });
    }

    private void assertQueryExecutionMode(
            boolean isFiberEnabled,
            WorkerPoolMode workerPoolMode,
            boolean isFiberExecutionExpected
    ) throws Exception {
        final PGConfiguration configuration = new Port0PGConfiguration(-1) {
            @Override
            public int getWorkerCount() {
                return 1;
            }

            @Override
            public boolean isFiberEnabled() {
                return isFiberEnabled;
            }
        };
        final WorkerPoolConfiguration workerPoolConfiguration = new WorkerPoolConfiguration() {
            @Override
            public Metrics getMetrics() {
                return Metrics.DISABLED;
            }

            @Override
            public int getWorkerCount() {
                return 1;
            }

            @Override
            public WorkerPoolMode getWorkerPoolMode() {
                return workerPoolMode;
            }
        };
        final WorkerPool workerPool = new TestWorkerPool(workerPoolConfiguration);
        try (
                workerPool;
                PGServer server = createPGWireServer(configuration, engine, workerPool)
        ) {
            workerPool.start(LOG);
            try {
                TestUtils.assertEventually(() -> Assert.assertTrue(server.isListening()));
                try (
                        Connection connection = getConnection(Mode.EXTENDED, server.getPort(), true);
                        Statement statement = connection.createStatement();
                        ResultSet resultSet = statement.executeQuery("SELECT 42 x")
                ) {
                    Assert.assertTrue(resultSet.next());
                    Assert.assertEquals(42, resultSet.getInt(1));
                }

                Assert.assertEquals(workerPoolMode, workerPool.getWorkerPoolMode());
                Assert.assertEquals(
                        isFiberEnabled ? WorkerPoolMode.FIBER_HOST : WorkerPoolMode.LEGACY,
                        configuration.getWorkerPoolMode()
                );
                if (workerPoolMode == WorkerPoolMode.FIBER_HOST) {
                    Assert.assertEquals(
                            isFiberExecutionExpected,
                            workerPool.getFiberRuntime().getLaunchCount(LaunchResult.LAUNCHED) > 0
                    );
                    Assert.assertEquals(
                            isFiberExecutionExpected,
                            workerPool.getFiberRuntime().getCreatedFiberCount() > 0
                    );
                }
            } finally {
                workerPool.halt();
            }
        }
    }

    private void closeFiberRuntime(FiberRuntime runtime) {
        runtime.beginQuiesce();
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (runtime.state() != FiberRuntimeState.CLOSED && System.nanoTime() < deadline) {
            runtime.drain(64);
        }
        Assert.assertTrue(runtime.awaitClosed(deadline));
        runtime.closeAfterDrained();
    }

    private static class SaturatedTestPGDispatcher implements IODispatcher<PGConnectionContext> {
        private int processCount;

        @Override
        public void close() {
        }

        @Override
        public void disconnect(PGConnectionContext context, int reason) {
        }

        @Override
        public int getConnectionCount() {
            return 0;
        }

        public int getProcessCount() {
            return processCount;
        }

        @Override
        public int getPort() {
            return 0;
        }

        @Override
        public boolean hasPendingIOEvents() {
            return processCount == 0;
        }

        @Override
        public boolean isListening() {
            return false;
        }

        @Override
        public boolean processIOQueue(IORequestProcessor<PGConnectionContext> processor) {
            processCount++;
            return false;
        }

        @Override
        public void registerChannel(PGConnectionContext context, int operation) {
        }

        @Override
        public boolean run(Job.WorkerContext workerContext) {
            return false;
        }
    }
}
