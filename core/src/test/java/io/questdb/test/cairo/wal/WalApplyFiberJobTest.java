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

package io.questdb.test.cairo.wal;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.cairo.wal.WalApplyFiberJob;
import io.questdb.mp.Job;
import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.std.Os;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.cairo.TableUtils.WAL_2_TABLE_WRITE_REASON;

public class WalApplyFiberJobTest extends AbstractCairoTest {

    @Test
    public void testBusyWriterDoesNotBlockAnotherTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE wal_fiber_a (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE wal_fiber_b (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            execute("INSERT INTO wal_fiber_a VALUES (1, '2026-01-01T00:00:00.000000Z')");
            execute("INSERT INTO wal_fiber_b VALUES (2, '2026-01-01T00:00:00.000000Z')");

            final TableToken tableTokenA = engine.verifyTableName("wal_fiber_a");
            final TableToken tableTokenB = engine.verifyTableName("wal_fiber_b");
            final FiberRuntime runtime = new FiberRuntime(2);
            final WalApplyFiberJob job = new WalApplyFiberJob(engine, 0, runtime);
            try {
                try (TableWriter ignored = engine.getWriter(tableTokenA, WAL_2_TABLE_WRITE_REASON)) {
                    Assert.assertTrue(job.run(Job.RUNNING_STATUS));
                    Assert.assertEquals(1, runtime.drain(1));
                    Assert.assertEquals(0, runtime.getParkedFiberCount());
                    Assert.assertEquals(0, runtime.getOutstandingTaskCount());
                    Assert.assertEquals(job.getExecutorCount(), job.getFreeExecutorCount());

                    Assert.assertTrue(job.run(Job.RUNNING_STATUS));
                    Assert.assertEquals(1, runtime.drain(1));

                    Assert.assertTrue(writerIsBehind(tableTokenA));
                    Assert.assertFalse(writerIsBehind(tableTokenB));
                }

                final long publishedBeforeCheck = engine.getMessageBus()
                        .getWalTxnNotificationPubSequence()
                        .current();
                new CheckWalTransactionsJob(engine).run();
                Assert.assertTrue(
                        publishedBeforeCheck < engine.getMessageBus()
                                .getWalTxnNotificationPubSequence()
                                .current()
                );
                drain(job, runtime);

                Assert.assertEquals(2, job.getTaskCount());
                Assert.assertEquals(job.getExecutorCount(), job.getFreeExecutorCount());
                assertQuery("SELECT x FROM wal_fiber_a").expectSize().returns("x\n1\n");
                assertQuery("SELECT x FROM wal_fiber_b").expectSize().returns("x\n2\n");
                close(runtime);
            } finally {
                close(runtime, job);
            }
        });
    }

    @Test
    public void testDuplicateNotificationRequeuesOwnedTask() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE wal_fiber_duplicate (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            execute("INSERT INTO wal_fiber_duplicate VALUES (42, '2026-01-01T00:00:00.000000Z')");

            final TableToken tableToken = engine.verifyTableName("wal_fiber_duplicate");
            final FiberRuntime runtime = new FiberRuntime(2);
            final WalApplyFiberJob job = new WalApplyFiberJob(engine, 0, runtime);
            try {
                Assert.assertTrue(job.run(Job.RUNNING_STATUS));
                Assert.assertTrue(engine.notifyWalTxnCommitted(tableToken));
                Assert.assertTrue(job.run(Job.RUNNING_STATUS));
                Assert.assertEquals(1, job.getTaskCount());
                Assert.assertEquals(1, runtime.getOutstandingTaskCount());

                Assert.assertEquals(1, runtime.drain(1));
                Assert.assertEquals(0, runtime.getOutstandingTaskCount());
                Assert.assertTrue(job.run(Job.RUNNING_STATUS));
                Assert.assertEquals(1, runtime.drain(1));

                Assert.assertEquals(0, runtime.getOutstandingTaskCount());
                Assert.assertEquals(job.getExecutorCount(), job.getFreeExecutorCount());
                assertQuery("SELECT x FROM wal_fiber_duplicate").expectSize().returns("x\n42\n");
                close(runtime);
            } finally {
                close(runtime, job);
            }
        });
    }

    @Test
    public void testDroppedTaskCannotRebindBeforeEviction() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE wal_fiber_drop (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            execute("INSERT INTO wal_fiber_drop VALUES (42, '2026-01-01T00:00:00.000000Z')");

            final TableToken tableToken = engine.verifyTableName("wal_fiber_drop");
            final CountDownLatch beforeEvict = new CountDownLatch(1);
            final CountDownLatch continueEviction = new CountDownLatch(1);
            final AtomicReference<Throwable> error = new AtomicReference<>();
            final FiberRuntime runtime = new FiberRuntime(2);
            final WalApplyFiberJob job = new WalApplyFiberJob(engine, 0, runtime);
            Thread processor = null;
            int outstandingWhileEvictionBlocked = -1;
            try {
                job.setBeforeEvictForTesting(() -> {
                    beforeEvict.countDown();
                    try {
                        continueEviction.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(e);
                    }
                });

                Assert.assertTrue(job.run(Job.RUNNING_STATUS));
                execute("DROP TABLE wal_fiber_drop");
                Assert.assertTrue(engine.notifyWalTxnCommitted(tableToken));

                processor = new Thread(() -> {
                    try {
                        runtime.initializeCarrier();
                        runtime.drain(1);
                    } catch (Throwable th) {
                        error.set(th);
                    }
                });
                processor.start();
                Assert.assertTrue(beforeEvict.await(10, TimeUnit.SECONDS));

                Assert.assertTrue(job.run(Job.RUNNING_STATUS));
                outstandingWhileEvictionBlocked = runtime.getOutstandingTaskCount();
                continueEviction.countDown();
                processor.join(10_000);

                Assert.assertFalse(processor.isAlive());
                Assert.assertNull(error.get());
                job.setBeforeEvictForTesting(null);
                drain(job, runtime);
                Assert.assertEquals(0, job.getTaskCount());
                close(runtime);
            } finally {
                continueEviction.countDown();
                if (processor != null) {
                    processor.join(10_000);
                }
                close(runtime, job);
            }
            Assert.assertEquals(0, outstandingWhileEvictionBlocked);
        });
    }

    @Test
    public void testEmptyQueueDoesNotAllocate() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final WalApplyFiberJob job = new WalApplyFiberJob(engine, 0, runtime);
            try {
                Assert.assertFalse(job.run(Job.RUNNING_STATUS));
                Assert.assertEquals(0, runtime.getCreatedFiberCount());
                Assert.assertEquals(0, job.getExecutorCount());
                Assert.assertEquals(0, job.getTaskCount());
                close(runtime);
            } finally {
                close(runtime, job);
            }
        });
    }

    @Test
    public void testQuiesceDuringLaunchReleasesIdleLease() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE wal_fiber_quiesce (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            execute("INSERT INTO wal_fiber_quiesce VALUES (42, '2026-01-01T00:00:00.000000Z')");

            final AtomicReference<FiberRuntime> runtimeRef = new AtomicReference<>();
            final FiberRuntime runtime = new FiberRuntime(1, 1, () -> runtimeRef.get().beginQuiesce());
            runtimeRef.set(runtime);
            final WalApplyFiberJob job = new WalApplyFiberJob(engine, 0, runtime);
            try {
                Assert.assertTrue(job.run(Job.RUNNING_STATUS));
                Assert.assertEquals(FiberRuntimeState.QUIESCING, runtime.state());
                Assert.assertEquals(0, runtime.getOutstandingTaskCount());
                Assert.assertEquals(job.getExecutorCount(), job.getFreeExecutorCount());
                close(runtime);
            } finally {
                close(runtime, job);
            }
        });
    }

    @Test
    public void testSaturationLeavesNotificationQueued() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE wal_fiber_saturation (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            execute("INSERT INTO wal_fiber_saturation VALUES (42, '2026-01-01T00:00:00.000000Z')");

            final FiberRuntime runtime = new FiberRuntime(1);
            final WalApplyFiberJob job = new WalApplyFiberJob(engine, 0, runtime);
            try {
                final Fiber reservedFiber = runtime.tryReserveFiber();
                Assert.assertNotNull(reservedFiber);
                Assert.assertFalse(job.run(Job.RUNNING_STATUS));
                Assert.assertEquals(0, job.getExecutorCount());
                Assert.assertEquals(0, job.getTaskCount());

                runtime.releaseReservedFiber(reservedFiber);
                drain(job, runtime);

                Assert.assertEquals(1, job.getExecutorCount());
                Assert.assertEquals(1, job.getFreeExecutorCount());
                Assert.assertEquals(1, job.getTaskCount());
                assertQuery("SELECT x FROM wal_fiber_saturation").expectSize().returns("x\n42\n");
                close(runtime);
            } finally {
                close(runtime, job);
            }
        });
    }

    private static void close(FiberRuntime runtime) {
        runtime.beginQuiesce();
        final long deadline = System.nanoTime() + 5_000_000_000L;
        while (runtime.state() != FiberRuntimeState.CLOSED && System.nanoTime() < deadline) {
            runtime.drain(64);
            Os.pause();
        }
        Assert.assertTrue(runtime.awaitClosed(deadline));
        Assert.assertEquals(0, runtime.getInlineSuspendViolationCount());
        runtime.closeAfterDrained();
    }

    private static void close(FiberRuntime runtime, WalApplyFiberJob job) {
        try {
            if (runtime.state() != FiberRuntimeState.CLOSED) {
                close(runtime);
            }
        } finally {
            job.close();
        }
    }

    private static void drain(WalApplyFiberJob job, FiberRuntime runtime) {
        final long deadline = System.nanoTime() + 5_000_000_000L;
        while (System.nanoTime() < deadline) {
            final boolean hasQueueWork = job.run(Job.RUNNING_STATUS);
            final boolean hasFiberWork = runtime.drain(64) > 0;
            if (!hasQueueWork && !hasFiberWork && runtime.getOutstandingTaskCount() == 0) {
                return;
            }
            Os.pause();
        }
        Assert.fail(
                "WAL fiber drain timed out [outstanding=" + runtime.getOutstandingTaskCount()
                        + ", parked=" + runtime.getParkedFiberCount()
                        + ", queued=" + runtime.getQueuedCount()
                        + ']'
        );
    }

    private static boolean writerIsBehind(TableToken tableToken) {
        return engine.getTableSequencerAPI().getTxnTracker(tableToken).getWriterTxn()
                < engine.getTableSequencerAPI().getTxnTracker(tableToken).getSeqTxn();
    }
}
