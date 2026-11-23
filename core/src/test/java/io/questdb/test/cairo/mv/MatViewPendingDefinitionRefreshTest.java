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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.mv.MatViewRefreshTask;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.mv.MatViewStateStore;
import io.questdb.cairo.mv.MatViewStateStoreImpl;
import io.questdb.cairo.mv.MatViewTimerJob;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.mp.Job;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.std.FilesFacade;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Refreshes that meet the definition event of an {@code ALTER MATERIALIZED VIEW ... AS} that still waits in the view's
 * WAL. A refresh whose base snapshot can hold rows committed after the statement returned defers without writing, and
 * the WAL apply job, as it applies or skips the event, or RESUME WAL FROM TXN, as it skips it, wakes one incremental
 * refresh for the deferred work, which then runs with the definition the view's WAL keeps. A FULL refresh parks its
 * request until then, and a user {@code REFRESH ... RANGE} that meets the definition pending in the refresh job ends
 * with an error in {@code materialized_views().last_refresh_error}.
 * <p>
 * The view v sums x by the hour (SUM_SQL), and ALTER ... AS moves it to the maximum (MAX_SQL) or the minimum
 * (MIN_SQL). The buckets the tests fill before an ALTER hold one row each, so every SQL gives them the same value. The
 * 02:00 bucket gets the rows 1.0, 2.0 and 3.0 after the ALTER: 6.0 with the sum, 3.0 with the maximum and 1.0 with the
 * minimum. The tests of a FULL refresh give the 00:00 bucket the rows 1.0 and 5.0: 6.0 with the sum, 5.0 with the
 * maximum.
 */
public class MatViewPendingDefinitionRefreshTest extends AbstractCairoTest {
    // the rows of the 02:00 bucket, inserted after the ALTER ... AS returned
    private static final String BUCKET_0200_ROWS = "INSERT INTO base VALUES ('2024-01-01T02:10:00.000000Z', 1.0), ('2024-01-01T02:20:00.000000Z', 2.0), ('2024-01-01T02:30:00.000000Z', 3.0)";
    // the log line of a refresh that waits for a definition event, once per event
    private static final String DEFINITION_WAIT_LOG = "materialized view definition is not applied yet, refresh waits for it \\[view=v~";
    // An incremental refresh of new base rows commits two transactions to the view's WAL: the base table intervals it
    // caches, so that the WAL purge job knows them, and then the rows.
    private static final int INCREMENTAL_REFRESH_TXNS = 2;
    private static final String MAX_SQL = "SELECT ts, max(x) AS s FROM base SAMPLE BY 1h";
    private static final String MIN_SQL = "SELECT ts, min(x) AS s FROM base SAMPLE BY 1h";
    // the error of a user REFRESH ... RANGE that meets a pending definition in the refresh job
    private static final String RANGE_ERROR = "an earlier ALTER MATERIALIZED VIEW ... AS is not applied yet, run the REFRESH ... RANGE again";
    private static final String SUM_SQL = "SELECT ts, sum(x) AS s FROM base SAMPLE BY 1h";

    @Test
    public void testAlterAndRowsBetweenLockAndReaderDefer() throws Exception {
        // ALTER ... AS and the rows after it land once a refresh holds the view lock, which it took with no definition
        // pending, and before it opens its base reader: the check after the reader opens defers the refresh.
        assertMemoryLeak(() -> {
            createManualView();
            execute("REFRESH MATERIALIZED VIEW v INCREMENTAL");
            final AtomicLong definitionSeqTxn = new AtomicLong(-1);
            try (MatViewRefreshJob refreshJob = newRefreshJob()) {
                refreshJob.setOnHoldingLockForTesting(once(() -> {
                    alterView(MAX_SQL);
                    definitionSeqTxn.set(viewState().getPendingDefinitionSeqTxn());
                    execute(BUCKET_0200_ROWS);
                    applyBaseWal();
                }));
                drainMatViewQueue(refreshJob);
            }
            Assert.assertTrue(definitionSeqTxn.get() > -1);
            Assert.assertEquals(definitionSeqTxn.get(), viewSeqTxn());
            Assert.assertTrue(viewState().hasDeferredDefinitionRefreshForTesting());
            drainWalQueue();
            Assert.assertEquals("incremental_refresh v\n", queuedTasks());
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t1.0
                    2024-01-01T02:00:00.000000Z\t3.0
                    """);
            assertSqlCursors(MAX_SQL, "v");
        });
    }

    @Test
    public void testAlterAppliedBeforeRefreshDoesNotWait() throws Exception {
        // An ALTER ... AS whose definition event applied before the refresh: the refresh runs at once with the new SQL,
        // in one data transaction, and nothing defers.
        assertMemoryLeak(() -> {
            createImmediateView();
            alterView(MAX_SQL);
            drainWalQueue();
            final long viewSeqTxn = viewSeqTxn();
            final LogCapture logCapture = new LogCapture();
            logCapture.start();
            try {
                execute(BUCKET_0200_ROWS);
                applyBaseWal();
                drainMatViewQueue(engine);
                logCapture.drain();
                Assert.assertEquals(0, logCapture.countLoggedRE(DEFINITION_WAIT_LOG));
            } finally {
                logCapture.stop();
            }
            Assert.assertEquals(viewSeqTxn + INCREMENTAL_REFRESH_TXNS, viewSeqTxn());
            Assert.assertFalse(viewState().hasDeferredDefinitionRefreshForTesting());
            Assert.assertEquals("", queuedTasks());
            drainWalQueue();
            assertViewRows("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t1.0
                    2024-01-01T02:00:00.000000Z\t3.0
                    """);
            assertSqlCursors(MAX_SQL, "v");
        });
    }

    @Test
    public void testAppliedDefinitionSeqTxnIsNeverLowered() throws Exception {
        // The record of the applied definition events only grows: a raise below it, or to it, keeps it.
        assertMemoryLeak(() -> {
            createImmediateView();
            alterView(MAX_SQL);
            final long definitionSeqTxn = viewState().getPendingDefinitionSeqTxn();
            drainWalQueue();
            Assert.assertEquals(definitionSeqTxn, viewState().getAppliedDefinitionSeqTxnForTesting());
            viewState().recordAppliedDefinitionSeqTxn(definitionSeqTxn - 1);
            Assert.assertEquals(definitionSeqTxn, viewState().getAppliedDefinitionSeqTxnForTesting());
            viewState().recordAppliedDefinitionSeqTxn(definitionSeqTxn);
            Assert.assertEquals(definitionSeqTxn, viewState().getAppliedDefinitionSeqTxnForTesting());
            Assert.assertFalse(viewState().isDefinitionPending());
        });
    }

    @Test
    public void testClaimFailureDuringResumeWalStillRedeliversMarker() throws Exception {
        // RESUME WAL FROM TXN skips a definition event that a deferred refresh and a parked FULL wait for. The wake of
        // the deferred refresh fails to enqueue, and the same call still redelivers the parked FULL; the refresh job's
        // retry scan delivers the deferred refresh. Both run with the SQL the skip left.
        assertMemoryLeak(() -> {
            createImmediateViewWithTwoRowBucket();
            alterView(MAX_SQL);
            final long definitionSeqTxn = viewState().getPendingDefinitionSeqTxn();
            execute("ALTER MATERIALIZED VIEW v SUSPEND WAL");
            execute(BUCKET_0200_ROWS);
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW v FULL");
            drainMatViewQueue(engine);
            final Object fullRefreshOwner = viewState().getPendingFullRefreshOwnerForTesting();
            Assert.assertNotNull(fullRefreshOwner);
            Assert.assertTrue(viewState().hasDeferredDefinitionRefreshForTesting());

            final MatViewStateStoreImpl stateStore = (MatViewStateStoreImpl) engine.getMatViewStateStore();
            final AtomicInteger appends = new AtomicInteger();
            stateStore.setOnTaskQueueAppendForTesting(() -> {
                if (appends.incrementAndGet() == 1) {
                    throw new OutOfMemoryError("test enqueue failure");
                }
            });
            try {
                execute("ALTER MATERIALIZED VIEW v RESUME WAL FROM TXN " + (definitionSeqTxn + 1));
            } finally {
                stateStore.setOnTaskQueueAppendForTesting(null);
            }
            Assert.assertEquals(2, appends.get());
            Assert.assertEquals("full_refresh v\n", queuedTasks());
            Assert.assertSame(fullRefreshOwner, viewState().getPendingFullRefreshOwnerForTesting());
            Assert.assertFalse(viewState().hasDeferredDefinitionRefreshForTesting());

            drainWalAndMatViewQueues();
            Assert.assertNull(viewState().getPendingFullRefreshOwnerForTesting());
            Assert.assertEquals(SUM_SQL, viewDefinition().getMatViewSql());
            assertViewRows("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t6.0
                    2024-01-01T02:00:00.000000Z\t6.0
                    """);
            assertSqlCursors(SUM_SQL, "v");
            assertViewStatus("valid");
        });
    }

    @Test
    public void testClaimSkippedWhileWriteDeniedSuspendedKeepsRequest() throws Exception {
        // With cairo.wal.apply.suspended.write.denied on, a view whose WAL is hard-suspended takes no refresh, so the
        // wake of a deferred refresh waits for the suspension to lift, though the definition event no longer waits.
        setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, "true");
        assertMemoryLeak(() -> {
            createImmediateView();
            alterView(MAX_SQL);
            final long definitionSeqTxn = viewState().getPendingDefinitionSeqTxn();
            execute(BUCKET_0200_ROWS);
            applyBaseWal();
            drainMatViewQueue(engine);
            Assert.assertTrue(viewState().hasDeferredDefinitionRefreshForTesting());
            execute("ALTER MATERIALIZED VIEW v SUSPEND WAL");
            // RESUME WAL FROM TXN past the definition event, without the statement's wake
            engine.getTableSequencerAPI().resumeTable(viewToken(), definitionSeqTxn + 1);
            Assert.assertEquals(definitionSeqTxn, viewState().getAppliedDefinitionSeqTxnForTesting());
            engine.addWalApplySuspended(viewToken());
            engine.getMatViewStateStore().reenqueuePendingOnResume(viewToken());
            Assert.assertEquals("", queuedTasks());
            Assert.assertTrue(viewState().hasDeferredDefinitionRefreshForTesting());
            drainMatViewQueue(engine);

            engine.removeWalApplySuspended(viewToken());
            engine.getMatViewStateStore().reenqueuePendingOnResume(viewToken());
            Assert.assertEquals("incremental_refresh v\n", queuedTasks());
            Assert.assertFalse(viewState().hasDeferredDefinitionRefreshForTesting());
            drainWalAndMatViewQueues();
            // the skipped event leaves the view's SQL as it was
            Assert.assertEquals(SUM_SQL, viewDefinition().getMatViewSql());
            assertViewRows("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t1.0
                    2024-01-01T02:00:00.000000Z\t6.0
                    """);
            assertSqlCursors(SUM_SQL, "v");
        });
    }

    @Test
    public void testClaimWhileDeferringRefreshHoldsLatch() throws Exception {
        // A refresh defers at its check after the reader opened, and the definition event applies while it still
        // holds the view lock: the wake's refresh loses the lock and leaves its run to the holder's release, which
        // enqueues it, so the deferred rows are computed once, with the new SQL.
        assertMemoryLeak(() -> {
            createImmediateView();
            execute("INSERT INTO base VALUES ('2024-01-01T01:10:00.000000Z', 1.0)");
            applyBaseWal();
            final AtomicLong definitionSeqTxn = new AtomicLong(-1);
            final AtomicBoolean isUnlockSeamRun = new AtomicBoolean();
            try (MatViewRefreshJob refreshJob = newRefreshJob(); MatViewRefreshJob secondJob = newRefreshJob()) {
                refreshJob.setOnHoldingLockForTesting(once(() -> {
                    alterView(MAX_SQL);
                    definitionSeqTxn.set(viewState().getPendingDefinitionSeqTxn());
                    execute(BUCKET_0200_ROWS);
                    applyBaseWal();
                    viewState().setOnBeforeUnlockForTesting(unchecked(() -> {
                        isUnlockSeamRun.set(true);
                        Assert.assertTrue(viewState().hasDeferredDefinitionRefreshForTesting());
                        drainWalQueue();
                        Assert.assertFalse(viewState().hasDeferredDefinitionRefreshForTesting());
                        drainMatViewQueue(secondJob);
                        Assert.assertTrue(viewState().hasPendingIncrementalRefreshForTesting());
                    }));
                }));
                drainMatViewQueue(refreshJob);
            }
            Assert.assertTrue(isUnlockSeamRun.get());
            // one refresh after the definition event
            Assert.assertEquals(definitionSeqTxn.get() + INCREMENTAL_REFRESH_TXNS, viewSeqTxn());
            Assert.assertEquals("", queuedTasks());
            drainWalQueue();
            assertViewRows("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t1.0
                    2024-01-01T01:00:00.000000Z\t1.0
                    2024-01-01T02:00:00.000000Z\t3.0
                    """);
            assertSqlCursors(MAX_SQL, "v");
        });
    }

    @Test
    public void testDeferredRefreshAfterDefinitionApplyFailureResumed() throws Exception {
        assertDeferredRefreshAfterDefinitionApplyFailure(false);
    }

    @Test
    public void testDeferredRefreshAfterDefinitionApplyFailureSkipped() throws Exception {
        assertDeferredRefreshAfterDefinitionApplyFailure(true);
    }

    @Test
    public void testDeferredRefreshClaimEnqueueCairoException() throws Exception {
        // a failure other than OutOfMemoryError goes up to the WAL apply job, which logs it and goes on
        assertDeferredRefreshClaimEnqueueFailure(
                false,
                "could not redeliver materialized view work after applying its WAL, the retry scan delivers it \\[view=v~"
        );
    }

    @Test
    public void testDeferredRefreshClaimEnqueueOutOfMemory() throws Exception {
        // a failure to grow the queue stays in the store, which records the retry
        assertDeferredRefreshClaimEnqueueFailure(
                true,
                "could not enqueue deferred incremental refresh, the retry scan delivers it \\[view=v~"
        );
    }

    @Test
    public void testDeferredRefreshClaimedInsideFailingBatch() throws Exception {
        // The WAL apply batch of the view applies the definition event and fails on the next transaction, which
        // suspends the view's WAL. The batch woke the deferred refresh as it applied the event, before it failed.
        final AtomicBoolean isFailArmed = new AtomicBoolean();
        final AtomicInteger definitionOpens = new AtomicInteger();
        final FilesFacade ff = newDefinitionOpenFailingFacade(isFailArmed, definitionOpens);
        assertMemoryLeak(ff, () -> {
            createImmediateView();
            alterView(MAX_SQL);
            final long definitionSeqTxn = viewState().getPendingDefinitionSeqTxn();
            execute(BUCKET_0200_ROWS);
            applyBaseWal();
            drainMatViewQueue(engine);
            Assert.assertTrue(viewState().hasDeferredDefinitionRefreshForTesting());
            execute("ALTER MATERIALIZED VIEW v SET REFRESH LIMIT 10 DAYS");
            isFailArmed.set(true);
            try {
                drainWalQueue();
            } finally {
                isFailArmed.set(false);
            }
            Assert.assertEquals(2, definitionOpens.get());
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(viewToken()));
            Assert.assertEquals(definitionSeqTxn, writerTxn());
            Assert.assertEquals(MAX_SQL, viewDefinition().getMatViewSql());
            Assert.assertEquals("incremental_refresh v\n", queuedTasks());
            final long viewSeqTxn = viewSeqTxn();
            drainMatViewQueue(engine);
            // the refresh commits into the suspended WAL
            Assert.assertEquals(viewSeqTxn + INCREMENTAL_REFRESH_TXNS, viewSeqTxn());
            execute("ALTER MATERIALIZED VIEW v RESUME WAL");
            drainWalAndMatViewQueues();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(viewToken()));
            assertViewRows("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t1.0
                    2024-01-01T02:00:00.000000Z\t3.0
                    """);
            assertSqlCursors(MAX_SQL, "v");
            assertViewStatus("valid");
        });
    }

    @Test
    public void testDeferredRefreshRedeliveredWhenTrackerResetAfterApply() throws Exception {
        // A republish resets the view's tracker right after the WAL apply applied the definition event: the wake
        // already went out from the apply, and the woken refresh runs with no CheckWalTransactionsJob pass.
        assertMemoryLeak(() -> {
            createImmediateView();
            alterView(MAX_SQL);
            execute(BUCKET_0200_ROWS);
            applyBaseWal();
            drainMatViewQueue(engine);
            Assert.assertTrue(viewState().hasDeferredDefinitionRefreshForTesting());
            applyViewWal();
            engine.notifyWalTxnRepublisher(viewToken());
            Assert.assertEquals(-1, writerTxn());
            Assert.assertEquals("incremental_refresh v\n", queuedTasks());
            final long viewSeqTxn = viewSeqTxn();
            drainMatViewQueue(engine);
            Assert.assertEquals(viewSeqTxn + INCREMENTAL_REFRESH_TXNS, viewSeqTxn());
            applyViewWal();
            assertViewRows("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t1.0
                    2024-01-01T02:00:00.000000Z\t3.0
                    """);
            assertSqlCursors(MAX_SQL, "v");
        });
    }

    @Test
    public void testDeferredRefreshRedeliveredWithoutLaterBaseCommit() throws Exception {
        // A burst of base rows ends while the definition event waits: no later base commit triggers the view, and the
        // apply of the event alone wakes one refresh, which computes the burst with the new SQL.
        assertMemoryLeak(() -> {
            createImmediateView();
            alterView(MAX_SQL);
            final long definitionSeqTxn = viewState().getPendingDefinitionSeqTxn();
            execute(BUCKET_0200_ROWS);
            applyBaseWal();
            drainMatViewQueue(engine);
            Assert.assertEquals(definitionSeqTxn, viewSeqTxn());
            Assert.assertTrue(viewState().hasDeferredDefinitionRefreshForTesting());
            drainWalQueue();
            Assert.assertEquals("incremental_refresh v\n", queuedTasks());
            Assert.assertFalse(viewState().hasDeferredDefinitionRefreshForTesting());
            drainMatViewQueue(engine);
            Assert.assertEquals(definitionSeqTxn + INCREMENTAL_REFRESH_TXNS, viewSeqTxn());
            drainWalQueue();
            assertViewRows("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t1.0
                    2024-01-01T02:00:00.000000Z\t3.0
                    """);
            assertSqlCursors(MAX_SQL, "v");
            Assert.assertEquals("", queuedTasks());
        });
    }

    @Test
    public void testDefinitionAppliedWithoutViewState() throws Exception {
        // The WAL apply of a definition event finds no view state, as on a node whose store holds none: it applies the
        // event, and wakes nothing. The view's next state refreshes with the new SQL.
        assertMemoryLeak(() -> {
            createImmediateView();
            alterView(MAX_SQL);
            final long definitionSeqTxn = viewState().getPendingDefinitionSeqTxn();
            execute(BUCKET_0200_ROWS);
            applyBaseWal();
            engine.getMatViewStateStore().clear();
            Assert.assertNull(viewState());
            applyViewWal();
            // the apply ran through the event, and left the view's WAL running
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(viewToken()));
            Assert.assertEquals(definitionSeqTxn, writerTxn());
            Assert.assertEquals(MAX_SQL, viewDefinition().getMatViewSql());
            Assert.assertTrue(engine.getMatViewStateStore().isRefreshQueueEmpty());
            engine.hydrateMatViewStateStore();
            Assert.assertFalse(viewState().isDefinitionPending());
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t1.0
                    2024-01-01T02:00:00.000000Z\t3.0
                    """);
            assertSqlCursors(MAX_SQL, "v");
        });
    }

    @Test
    public void testDefinitionResolvedBetweenPendingReadAndRequestGoesAhead() throws Exception {
        // The definition event applies after a refresh read it pending at its check after the reader opened, and
        // before the refresh registered its request: the refresh goes ahead in the same pass with the new SQL, and
        // registers nothing that a later wake would redeliver.
        assertMemoryLeak(() -> {
            createImmediateView();
            execute("INSERT INTO base VALUES ('2024-01-01T01:10:00.000000Z', 1.0)");
            applyBaseWal();
            final AtomicLong definitionSeqTxn = new AtomicLong(-1);
            final AtomicBoolean isRequestSeamRun = new AtomicBoolean();
            try (MatViewRefreshJob refreshJob = newRefreshJob()) {
                refreshJob.setOnHoldingLockForTesting(once(() -> {
                    alterView(MAX_SQL);
                    definitionSeqTxn.set(viewState().getPendingDefinitionSeqTxn());
                    execute(BUCKET_0200_ROWS);
                    applyBaseWal();
                    viewState().setOnDeferredDefinitionRequestForTesting(() -> {
                        isRequestSeamRun.set(true);
                        drainWalQueue();
                    });
                }));
                drainMatViewQueue(refreshJob);
            }
            Assert.assertTrue(isRequestSeamRun.get());
            Assert.assertFalse(viewState().hasDeferredDefinitionRefreshForTesting());
            Assert.assertEquals(definitionSeqTxn.get() + INCREMENTAL_REFRESH_TXNS, viewSeqTxn());
            Assert.assertEquals("", queuedTasks());
            drainWalQueue();
            assertViewRows("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t1.0
                    2024-01-01T01:00:00.000000Z\t1.0
                    2024-01-01T02:00:00.000000Z\t3.0
                    """);
            assertSqlCursors(MAX_SQL, "v");
        });
    }

    @Test
    public void testDropViewWhileRefreshDeferred() throws Exception {
        // A view dropped while its refresh waits for a definition event: nothing wakes a refresh for it.
        assertMemoryLeak(() -> {
            createImmediateView();
            alterView(MAX_SQL);
            execute(BUCKET_0200_ROWS);
            applyBaseWal();
            drainMatViewQueue(engine);
            Assert.assertTrue(viewState().hasDeferredDefinitionRefreshForTesting());
            execute("DROP MATERIALIZED VIEW v");
            drainWalQueue();
            // the drop's invalidation of the views over v, and no refresh of v
            Assert.assertEquals("invalidate base:v\n", queuedTasks());
            drainWalAndMatViewQueues();
            Assert.assertNull(engine.getTableTokenIfExists("v"));
            Assert.assertEquals("", queuedTasks());
        });
    }

    @Test
    public void testFiberRefreshJobDefersAndIsWoken() throws Exception {
        // The refresh job of a fiber host defers on a fiber, and runs the woken refresh there.
        assertMemoryLeak(() -> {
            createImmediateView();
            alterView(MAX_SQL);
            final long definitionSeqTxn = viewState().getPendingDefinitionSeqTxn();
            execute(BUCKET_0200_ROWS);
            applyBaseWal();
            final WorkerPool pool = newFiberHostPool();
            final FiberRuntime runtime = pool.getFiberRuntime();
            final MatViewRefreshJob refreshJob = new MatViewRefreshJob(engine, 1, runtime);
            pool.assign(refreshJob);
            try {
                runOnFiber(refreshJob, runtime);
                Assert.assertEquals(definitionSeqTxn, viewSeqTxn());
                Assert.assertTrue(viewState().hasDeferredDefinitionRefreshForTesting());
                drainWalQueue();
                Assert.assertEquals("incremental_refresh v\n", queuedTasks());
                runOnFiber(refreshJob, runtime);
                Assert.assertEquals(definitionSeqTxn + INCREMENTAL_REFRESH_TXNS, viewSeqTxn());
            } finally {
                closeRuntime(runtime);
                Assert.assertTrue(pool.haltWithin(TimeUnit.SECONDS.toNanos(10)));
            }
            drainWalQueue();
            assertViewRows("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t1.0
                    2024-01-01T02:00:00.000000Z\t3.0
                    """);
            assertSqlCursors(MAX_SQL, "v");
        });
    }

    @Test
    public void testFullAfterTrackerResetOfAppliedDefinitionRebuilds() throws Exception {
        // A republish that resets the view's tracker after the definition event applied changes nothing for a FULL:
        // it rebuilds at once with the new SQL.
        assertMemoryLeak(() -> {
            createManualViewWithTwoRowBucket();
            alterView(MAX_SQL);
            drainWalQueue();
            execute(BUCKET_0200_ROWS);
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW v FULL");
            engine.notifyWalTxnRepublisher(viewToken());
            Assert.assertEquals(-1, writerTxn());
            drainMatViewQueue(engine);
            Assert.assertNull(viewState().getPendingFullRefreshOwnerForTesting());
            applyViewWal();
            assertViewRows("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t5.0
                    2024-01-01T02:00:00.000000Z\t3.0
                    """);
            assertSqlCursors(MAX_SQL, "v");
        });
    }

    @Test
    public void testFullLateParkDoesNotRedeliverReplacedOwner() throws Exception {
        // A FULL parked after its reader on a pending definition whose request a newer REFRESH ... FULL replaced while
        // it held the view lock: once the event applied, its check after the unlock finds its owner no longer pending,
        // and redelivers only the newer request, which rebuilds the view once.
        assertMemoryLeak(() -> {
            createImmediateViewWithTwoRowBucket();
            execute("REFRESH MATERIALIZED VIEW v FULL");
            final AtomicReference<Object> parkedOwner = new AtomicReference<>();
            final AtomicReference<Object> newerOwner = new AtomicReference<>();
            final AtomicInteger rebuilds = new AtomicInteger();
            try (MatViewRefreshJob refreshJob = newRefreshJob()) {
                // one task per pass
                refreshJob.setMaxRunDurationForTesting(0);
                refreshJob.setOnBaseReaderSnapshotForTesting(rebuilds::incrementAndGet);
                refreshJob.setOnFullRefreshDefinitionReadForTesting(once(() -> {
                    alterView(MAX_SQL);
                    execute(BUCKET_0200_ROWS);
                    applyBaseWal();
                    parkedOwner.set(viewState().getPendingFullRefreshOwnerForTesting());
                    viewState().markAsPendingFullRefreshForTesting();
                    newerOwner.set(viewState().getPendingFullRefreshOwnerForTesting());
                    viewState().setOnBeforeUnlockForTesting(AbstractCairoTest::drainWalQueue);
                }));
                Assert.assertTrue(refreshJob.run());
                Assert.assertEquals(0, rebuilds.get());
                Assert.assertNotNull(parkedOwner.get());
                Assert.assertNotSame(parkedOwner.get(), newerOwner.get());
                Assert.assertFalse(viewState().isDefinitionPending());
                // the tasks queued for the parked FULL's view carry the newer request alone
                final ObjList<Object> owners = queuedFullRefreshOwners();
                Assert.assertTrue(owners.size() > 0);
                for (int i = 0, n = owners.size(); i < n; i++) {
                    Assert.assertSame(newerOwner.get(), owners.getQuick(i));
                }
                drainMatViewQueue(refreshJob);
            }
            Assert.assertEquals(1, rebuilds.get());
            Assert.assertNull(viewState().getPendingFullRefreshOwnerForTesting());
            drainWalQueue();
            assertViewRows("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t5.0
                    2024-01-01T02:00:00.000000Z\t3.0
                    """);
            assertSqlCursors(MAX_SQL, "v");
        });
    }

    @Test
    public void testFullLateParkHandsOffToRedeliveredOwner() throws Exception {
        // ALTER ... AS and the rows after it land once a FULL passed its early check and before its base reader opens:
        // the FULL parks after the reader. The definition event applies while it still holds the view lock, and the
        // FULL it redelivers loses the lock and hands its owner to the holder's release: the view is rebuilt once,
        // with the new SQL.
        assertMemoryLeak(() -> {
            createImmediateViewWithTwoRowBucket();
            execute("REFRESH MATERIALIZED VIEW v FULL");
            final AtomicInteger rebuilds = new AtomicInteger();
            final AtomicBoolean isUnlockSeamRun = new AtomicBoolean();
            try (MatViewRefreshJob refreshJob = newRefreshJob(); MatViewRefreshJob secondJob = newRefreshJob()) {
                refreshJob.setOnBaseReaderSnapshotForTesting(rebuilds::incrementAndGet);
                secondJob.setOnBaseReaderSnapshotForTesting(rebuilds::incrementAndGet);
                refreshJob.setOnFullRefreshDefinitionReadForTesting(once(() -> {
                    alterView(MAX_SQL);
                    execute(BUCKET_0200_ROWS);
                    applyBaseWal();
                    viewState().setOnBeforeUnlockForTesting(unchecked(() -> {
                        isUnlockSeamRun.set(true);
                        Assert.assertNotNull(viewState().getPendingFullRefreshOwnerForTesting());
                        drainWalQueue();
                        drainMatViewQueue(secondJob);
                        Assert.assertTrue(viewState().hasPendingFullRefreshHandoffForTesting());
                    }));
                }));
                drainMatViewQueue(refreshJob);
            }
            Assert.assertTrue(isUnlockSeamRun.get());
            Assert.assertEquals(1, rebuilds.get());
            Assert.assertNull(viewState().getPendingFullRefreshOwnerForTesting());
            Assert.assertEquals("", queuedTasks());
            drainWalQueue();
            assertViewRows("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t5.0
                    2024-01-01T02:00:00.000000Z\t3.0
                    """);
            assertSqlCursors(MAX_SQL, "v");
            assertViewStatus("valid");
        });
    }

    @Test
    public void testFullQueuedBeforeAlterRebuildsWithNewSql() throws Exception {
        // A FULL queued before ALTER ... AS runs after the rows that followed the statement applied: it parks its
        // request without the view lock until the definition event applies, and then rebuilds every bucket with the
        // new SQL.
        assertMemoryLeak(() -> {
            createImmediateViewWithTwoRowBucket();
            execute("REFRESH MATERIALIZED VIEW v FULL");
            alterView(MAX_SQL);
            final long definitionSeqTxn = viewState().getPendingDefinitionSeqTxn();
            execute(BUCKET_0200_ROWS);
            applyBaseWal();
            final AtomicBoolean isLockTaken = new AtomicBoolean();
            viewState().setOnBeforeUnlockForTesting(() -> isLockTaken.set(true));
            try {
                drainMatViewQueue(engine);
            } finally {
                viewState().setOnBeforeUnlockForTesting(null);
            }
            Assert.assertFalse(isLockTaken.get());
            final Object fullRefreshOwner = viewState().getPendingFullRefreshOwnerForTesting();
            Assert.assertNotNull(fullRefreshOwner);
            Assert.assertEquals(definitionSeqTxn, viewSeqTxn());
            Assert.assertEquals("", queuedTasks());
            drainWalQueue();
            // the apply of the definition event woke the deferred refresh of the base commit and redelivered the
            // parked FULL, and the end of the apply batch redelivered it again
            Assert.assertEquals("incremental_refresh v\nfull_refresh v\nfull_refresh v\n", queuedTasks());
            drainWalAndMatViewQueues();
            Assert.assertNull(viewState().getPendingFullRefreshOwnerForTesting());
            assertViewRows("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t5.0
                    2024-01-01T02:00:00.000000Z\t3.0
                    """);
            assertSqlCursors(MAX_SQL, "v");
            assertViewStatus("valid");
        });
    }

    @Test
    public void testFullReReadsDefinitionAfterReaderOpens() throws Exception {
        // ALTER ... AS, its WAL apply and the rows after it land after a FULL on a valid view read the view's
        // definition and before it opens its base reader: the FULL reads the definition again once the reader opened,
        // and rebuilds every bucket with the new SQL.
        assertMemoryLeak(() -> {
            createImmediateViewWithTwoRowBucket();
            execute("REFRESH MATERIALIZED VIEW v FULL");
            final AtomicBoolean isSeamRun = new AtomicBoolean();
            try (MatViewRefreshJob refreshJob = newRefreshJob()) {
                refreshJob.setOnFullRefreshDefinitionReadForTesting(once(() -> {
                    isSeamRun.set(true);
                    alterView(MAX_SQL);
                    applyViewWal();
                    execute(BUCKET_0200_ROWS);
                    applyBaseWal();
                }));
                drainMatViewQueue(refreshJob);
            }
            Assert.assertTrue(isSeamRun.get());
            drainWalQueue();
            assertViewRows("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t5.0
                    2024-01-01T02:00:00.000000Z\t3.0
                    """);
            assertSqlCursors(MAX_SQL, "v");
        });
    }

    @Test
    public void testFullReadsDefinitionAfterViewWalCheck() throws Exception {
        // A FULL runs after ALTER ... WITH BASE wrote its definition event and before the statement records it, so no
        // definition is pending yet, and the view's WAL applies the event as the FULL takes the view's WAL writer,
        // before its check of the view's WAL. The FULL reads the definition after that check, and rebuilds the view
        // from the new base table.
        assertMemoryLeak(() -> {
            createImmediateView();
            execute("CREATE TABLE base2 (ts TIMESTAMP, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base2 VALUES ('2024-01-01T05:10:00.000000Z', 7.0)");
            drainWalQueue();
            final AtomicBoolean isWalApplied = new AtomicBoolean();
            viewState().setOnDefinitionEventWrittenForTesting(unchecked(() -> {
                engine.getMatViewStateStore().enqueueFullRefresh(viewToken());
                try (WalWriterHook ignore = new WalWriterHook(engine, viewToken(), () -> {
                    isWalApplied.set(true);
                    drainWalQueue();
                })) {
                    drainMatViewQueue(engine);
                }
            }));
            execute("ALTER MATERIALIZED VIEW v WITH BASE base2 AS (SELECT ts, sum(x) AS s FROM base2 SAMPLE BY 1h)");
            Assert.assertTrue(isWalApplied.get());
            drainWalQueue();
            Assert.assertEquals(engine.verifyTableName("base2").getTableId(), viewState().getBaseTableId());
            assertViewStatus("valid");
            assertViewRows("""
                    ts\ts
                    2024-01-01T05:00:00.000000Z\t7.0
                    """);
            execute("INSERT INTO base2 VALUES ('2024-01-01T06:10:00.000000Z', 8.0)");
            drainWalAndMatViewQueues();
            assertViewStatus("valid");
            assertViewRows("""
                    ts\ts
                    2024-01-01T05:00:00.000000Z\t7.0
                    2024-01-01T06:00:00.000000Z\t8.0
                    """);
        });
    }

    @Test
    public void testImmediateRefreshInWindowWaitsForDefinition() throws Exception {
        // The trigger: a base commit queued before ALTER ... AS, the rows after it, one WAL apply step that serves the
        // base alone, then the refresh job, before the view's WAL applies the definition event. The refresh defers
        // without the view lock, and the apply of the event wakes it.
        assertMemoryLeak(() -> {
            createImmediateView();
            execute("INSERT INTO base VALUES ('2024-01-01T01:10:00.000000Z', 1.0)");
            alterView(MAX_SQL);
            final long definitionSeqTxn = viewState().getPendingDefinitionSeqTxn();
            Assert.assertEquals(viewSeqTxn(), definitionSeqTxn);
            execute(BUCKET_0200_ROWS);
            try (ApplyWal2TableJob walApplyJob = createWalApplyJob()) {
                // the base's notification is ahead of the view's
                Assert.assertTrue(walApplyJob.run());
            }
            Assert.assertTrue(writerTxn() < definitionSeqTxn);
            final AtomicInteger lockAttempts = new AtomicInteger();
            try (MatViewRefreshJob refreshJob = newRefreshJob()) {
                refreshJob.setOnBeforeRefreshLockForTesting(lockAttempts::incrementAndGet);
                drainMatViewQueue(refreshJob);
            }
            Assert.assertEquals(0, lockAttempts.get());
            Assert.assertEquals(definitionSeqTxn, viewSeqTxn());
            Assert.assertTrue(viewState().hasDeferredDefinitionRefreshForTesting());
            Assert.assertEquals("", queuedTasks());

            drainWalQueue();
            Assert.assertEquals("incremental_refresh v\n", queuedTasks());
            Assert.assertFalse(viewState().hasDeferredDefinitionRefreshForTesting());
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t1.0
                    2024-01-01T01:00:00.000000Z\t1.0
                    2024-01-01T02:00:00.000000Z\t3.0
                    """);
            assertSqlCursors(MAX_SQL, "v");
            assertViewStatus("valid");
        });
    }

    @Test
    public void testImmediateRefreshOnSuspendedViewWalWritesNothing() throws Exception {
        // While the view's WAL is suspended short of the definition event, base commits refresh nothing into it: no
        // lock, no view WAL transaction, one log line. Once RESUME WAL lets the event apply, the view follows the new
        // SQL.
        assertMemoryLeak(() -> {
            createImmediateView();
            alterView(MAX_SQL);
            execute("ALTER MATERIALIZED VIEW v SUSPEND WAL");
            final long viewSeqTxn = viewSeqTxn();
            final AtomicInteger lockAttempts = new AtomicInteger();
            final LogCapture logCapture = new LogCapture();
            logCapture.start();
            try (MatViewRefreshJob refreshJob = newRefreshJob()) {
                refreshJob.setOnBeforeRefreshLockForTesting(lockAttempts::incrementAndGet);
                for (String row : new String[]{
                        "('2024-01-01T02:10:00.000000Z', 1.0)",
                        "('2024-01-01T02:20:00.000000Z', 2.0)",
                        "('2024-01-01T02:30:00.000000Z', 3.0)"
                }) {
                    execute("INSERT INTO base VALUES " + row);
                    drainWalAndMatViewQueues(refreshJob, engine);
                    Assert.assertEquals(viewSeqTxn, viewSeqTxn());
                }
                logCapture.drain();
                Assert.assertEquals(1, logCapture.countLoggedRE(DEFINITION_WAIT_LOG));
            } finally {
                logCapture.stop();
            }
            Assert.assertEquals(0, lockAttempts.get());
            execute("ALTER MATERIALIZED VIEW v RESUME WAL");
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t1.0
                    2024-01-01T02:00:00.000000Z\t3.0
                    """);
            assertSqlCursors(MAX_SQL, "v");
        });
    }

    @Test
    public void testImmediateRefreshReadsDefinitionAppliedBeforeLock() throws Exception {
        // ALTER ... AS, its WAL apply and the rows after it land after an incremental refresh of an IMMEDIATE view read
        // the view's definition and before it takes the view lock: the refresh reads the definition again and computes
        // with the new SQL.
        assertMemoryLeak(() -> {
            createImmediateView();
            execute("INSERT INTO base VALUES ('2024-01-01T01:10:00.000000Z', 1.0)");
            applyBaseWal();
            final AtomicBoolean isSeamRun = new AtomicBoolean();
            try (MatViewRefreshJob refreshJob = newRefreshJob()) {
                refreshJob.setOnBeforeRefreshLockForTesting(once(() -> {
                    isSeamRun.set(true);
                    applyAlterAndRows();
                }));
                drainMatViewQueue(refreshJob);
            }
            Assert.assertTrue(isSeamRun.get());
            Assert.assertFalse(viewState().hasDeferredDefinitionRefreshForTesting());
            applyViewWal();
            assertViewRows("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t1.0
                    2024-01-01T01:00:00.000000Z\t1.0
                    2024-01-01T02:00:00.000000Z\t3.0
                    """);
            assertSqlCursors(MAX_SQL, "v");
        });
    }

    @Test
    public void testManualViewWithPendingDefinitionRefreshesNothingUnasked() throws Exception {
        // The apply of a definition event wakes no refresh of a view that deferred none: a MANUAL view stays as it is.
        assertMemoryLeak(() -> {
            createManualView();
            alterView(MAX_SQL);
            final long definitionSeqTxn = viewState().getPendingDefinitionSeqTxn();
            execute(BUCKET_0200_ROWS);
            drainWalQueue();
            // the base commit's task, which moves the refresh intervals sequence of a MANUAL view
            Assert.assertEquals("incremental_refresh base:base\n", queuedTasks());
            drainWalAndMatViewQueues();
            Assert.assertEquals(definitionSeqTxn, viewSeqTxn());
            Assert.assertEquals(MAX_SQL, viewDefinition().getMatViewSql());
            assertViewRows("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t1.0
                    """);
        });
    }

    @Test
    public void testMarkerRedeliveryFailureAfterDeferredRefreshWake() throws Exception {
        // the wake of the deferred refresh succeeds, the redelivery of the parked FULL fails
        assertMarkerRedeliveryFailure(false, false);
    }

    @Test
    public void testMarkerRedeliveryFailureCarriesDeferredRefreshWakeFailure() throws Exception {
        // both fail, each with its own exception: the redelivery's carries the wake's as a suppressed one
        assertMarkerRedeliveryFailure(true, false);
    }

    @Test
    public void testMarkerRedeliveryFailureWithWakeFailureOfSameInstance() throws Exception {
        // both fail with one exception instance: it carries no suppressed exception of itself
        assertMarkerRedeliveryFailure(true, true);
    }

    @Test
    public void testNewerAlterAfterClaimDefersClaimedRefresh() throws Exception {
        // A newer ALTER ... AS lands after the apply of the first definition event woke the deferred refresh: the
        // woken refresh defers again, for the newer event, and runs with the newer SQL once that one applies.
        assertMemoryLeak(() -> {
            createImmediateView();
            alterView(MAX_SQL);
            execute(BUCKET_0200_ROWS);
            applyBaseWal();
            drainMatViewQueue(engine);
            Assert.assertTrue(viewState().hasDeferredDefinitionRefreshForTesting());
            drainWalQueue();
            Assert.assertEquals("incremental_refresh v\n", queuedTasks());
            alterView(MIN_SQL);
            final long definitionSeqTxn = viewState().getPendingDefinitionSeqTxn();
            drainMatViewQueue(engine);
            Assert.assertEquals(definitionSeqTxn, viewSeqTxn());
            Assert.assertTrue(viewState().hasDeferredDefinitionRefreshForTesting());
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t1.0
                    2024-01-01T02:00:00.000000Z\t1.0
                    """);
            assertSqlCursors(MIN_SQL, "v");
        });
    }

    @Test
    public void testNoAlterRefreshesWithoutWaiting() throws Exception {
        // Without an ALTER ... AS, a base commit refreshes the view in one data transaction, and nothing defers.
        assertMemoryLeak(() -> {
            createImmediateView();
            final long viewSeqTxn = viewSeqTxn();
            final LogCapture logCapture = new LogCapture();
            logCapture.start();
            try {
                execute(BUCKET_0200_ROWS);
                applyBaseWal();
                drainMatViewQueue(engine);
                logCapture.drain();
                Assert.assertEquals(0, logCapture.countLoggedRE(DEFINITION_WAIT_LOG));
            } finally {
                logCapture.stop();
            }
            Assert.assertEquals(viewSeqTxn + INCREMENTAL_REFRESH_TXNS, viewSeqTxn());
            Assert.assertFalse(viewState().hasDeferredDefinitionRefreshForTesting());
            Assert.assertEquals("", queuedTasks());
            drainWalQueue();
            Assert.assertEquals("", queuedTasks());
            assertViewRows("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t1.0
                    2024-01-01T02:00:00.000000Z\t6.0
                    """);
            assertSqlCursors(SUM_SQL, "v");
        });
    }

    @Test
    public void testParkedFullNotWokenByDeferringRefreshes() throws Exception {
        // A FULL parked on a pending definition stays parked while base commits keep coming: their refreshes defer
        // without the view lock, whose release would redeliver the FULL.
        assertMemoryLeak(() -> {
            createImmediateViewWithTwoRowBucket();
            alterView(MAX_SQL);
            execute("ALTER MATERIALIZED VIEW v SUSPEND WAL");
            final long viewSeqTxn = viewSeqTxn();
            execute("REFRESH MATERIALIZED VIEW v FULL");
            final AtomicInteger dequeuedTasks = new AtomicInteger();
            final AtomicInteger lockAttempts = new AtomicInteger();
            final LogCapture logCapture = new LogCapture();
            logCapture.start();
            try (MatViewRefreshJob refreshJob = newRefreshJob()) {
                refreshJob.setOnRefreshTaskDequeuedForTesting(dequeuedTasks::incrementAndGet);
                refreshJob.setOnBeforeRefreshLockForTesting(lockAttempts::incrementAndGet);
                drainMatViewQueue(refreshJob);
                Assert.assertEquals(1, dequeuedTasks.get());
                Assert.assertNotNull(viewState().getPendingFullRefreshOwnerForTesting());
                for (String row : new String[]{
                        "('2024-01-01T02:10:00.000000Z', 1.0)",
                        "('2024-01-01T02:20:00.000000Z', 2.0)",
                        "('2024-01-01T02:30:00.000000Z', 3.0)"
                }) {
                    execute("INSERT INTO base VALUES " + row);
                    drainWalQueue();
                    dequeuedTasks.set(0);
                    drainMatViewQueue(refreshJob);
                    // the base commit's refresh alone
                    Assert.assertEquals(1, dequeuedTasks.get());
                    Assert.assertEquals(viewSeqTxn, viewSeqTxn());
                    Assert.assertEquals("", queuedTasks());
                }
                logCapture.drain();
                Assert.assertEquals(1, logCapture.countLoggedRE(DEFINITION_WAIT_LOG));
            } finally {
                logCapture.stop();
            }
            Assert.assertEquals(0, lockAttempts.get());
            execute("ALTER MATERIALIZED VIEW v RESUME WAL");
            drainWalAndMatViewQueues();
            Assert.assertNull(viewState().getPendingFullRefreshOwnerForTesting());
            assertViewRows("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t5.0
                    2024-01-01T02:00:00.000000Z\t3.0
                    """);
            assertSqlCursors(MAX_SQL, "v");
            assertViewStatus("valid");
        });
    }

    @Test
    public void testParkedFullWokenByLatchHolderReparksSilently() throws Exception {
        // The release of a view lock holder, here REFRESH ... STATS, redelivers a FULL parked on a pending definition:
        // the FULL parks again before it takes the view's WAL writer, and logs nothing more.
        assertMemoryLeak(() -> {
            createImmediateViewWithTwoRowBucket();
            alterView(MAX_SQL);
            execute("ALTER MATERIALIZED VIEW v SUSPEND WAL");
            execute("REFRESH MATERIALIZED VIEW v FULL");
            final LogCapture logCapture = new LogCapture();
            logCapture.start();
            try {
                drainMatViewQueue(engine);
                Assert.assertNotNull(viewState().getPendingFullRefreshOwnerForTesting());
                execute("REFRESH MATERIALIZED VIEW v STATS");
                Assert.assertEquals("full_refresh v\n", queuedTasks());
                try (WalWriterHook walWriterHook = new WalWriterHook(engine, viewToken(), null)) {
                    drainMatViewQueue(engine);
                    Assert.assertEquals(0, walWriterHook.getCount());
                }
                Assert.assertNotNull(viewState().getPendingFullRefreshOwnerForTesting());
                Assert.assertEquals("", queuedTasks());
                logCapture.drain();
                Assert.assertEquals(1, logCapture.countLoggedRE(DEFINITION_WAIT_LOG));
            } finally {
                logCapture.stop();
            }
            execute("ALTER MATERIALIZED VIEW v RESUME WAL");
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t5.0
                    """);
            assertSqlCursors(MAX_SQL, "v");
        });
    }

    @Test
    public void testPeriodRefreshInWindowWaitsForDefinitionMicros() throws Exception {
        assertPeriodRefreshInWindowWaitsForDefinition("TIMESTAMP");
    }

    @Test
    public void testPeriodRefreshInWindowWaitsForDefinitionNanos() throws Exception {
        assertPeriodRefreshInWindowWaitsForDefinition("TIMESTAMP_NS");
    }

    @Test
    public void testPeriodRefreshLateCheckWaitsForDefinitionMicros() throws Exception {
        assertPeriodRefreshLateCheckWaitsForDefinition("TIMESTAMP");
    }

    @Test
    public void testPeriodRefreshLateCheckWaitsForDefinitionNanos() throws Exception {
        assertPeriodRefreshLateCheckWaitsForDefinition("TIMESTAMP_NS");
    }

    @Test
    public void testRangeOverEmptyRangeEndsWithErrorWhileDefinitionPending() throws Exception {
        // The refresh job checks for a pending definition before it looks for base rows in the range.
        assertMemoryLeak(() -> {
            createManualView();
            execute("REFRESH MATERIALIZED VIEW v RANGE FROM '2024-01-05T00:00:00.000000Z' TO '2024-01-05T23:59:59.999999Z'");
            alterView(MAX_SQL);
            final long definitionSeqTxn = viewState().getPendingDefinitionSeqTxn();
            drainMatViewQueue(engine);
            Assert.assertEquals(definitionSeqTxn, viewSeqTxn());
            assertRangeError(RANGE_ERROR);
        });
    }

    @Test
    public void testRangeQueuedBeforeAlterEndsWithError() throws Exception {
        // A user REFRESH ... RANGE that the refresh job runs while a later ALTER ... AS waits in the view's WAL ends
        // with an error in last_refresh_error and leaves the view valid, as it is. A rerun waits for the definition
        // event and recomputes the range with the new SQL, which clears the error.
        assertMemoryLeak(() -> {
            createManualView();
            final String rangeSql = "REFRESH MATERIALIZED VIEW v RANGE FROM '2024-01-01T00:00:00.000000Z' TO '2024-01-01T02:59:59.999999Z'";
            execute(rangeSql);
            alterView(MAX_SQL);
            final long definitionSeqTxn = viewState().getPendingDefinitionSeqTxn();
            execute(BUCKET_0200_ROWS);
            applyBaseWal();
            drainMatViewQueue(engine);
            Assert.assertEquals(definitionSeqTxn, viewSeqTxn());
            assertRangeError(RANGE_ERROR);
            Assert.assertFalse(viewState().hasDeferredDefinitionRefreshForTesting());

            final AtomicBoolean isWaitSeen = new AtomicBoolean();
            viewState().setOnDefinitionWaitForTesting(() -> {
                isWaitSeen.set(true);
                drainWalQueue();
            });
            execute(rangeSql);
            Assert.assertTrue(isWaitSeen.get());
            // the apply of the definition event woke nothing: the RANGE deferred nothing
            Assert.assertEquals("range_refresh v\n", queuedTasks());
            drainWalAndMatViewQueues();
            Assert.assertNull(viewState().getLastRefreshError());
            Assert.assertFalse(viewState().isInvalid());
            assertViewRows("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t1.0
                    2024-01-01T02:00:00.000000Z\t3.0
                    """);
            assertSqlCursors(MAX_SQL, "v");
        });
    }

    @Test
    public void testRefreshAfterTrackerResetOfAppliedDefinitionRunsAtOnce() throws Exception {
        // The unsolicited-lock arm of the WAL apply job resets the tracker of a view whose definition event applied
        // long ago: the next refresh runs at once with the new SQL, without a CheckWalTransactionsJob pass.
        assertMemoryLeak(() -> {
            createImmediateView();
            alterView(MAX_SQL);
            drainWalAndMatViewQueues();
            resetViewTrackerByUnsolicitedLock();
            Assert.assertEquals(-1, writerTxn());
            final long viewSeqTxn = viewSeqTxn();
            final LogCapture logCapture = new LogCapture();
            logCapture.start();
            try {
                execute(BUCKET_0200_ROWS);
                applyBaseWal();
                drainMatViewQueue(engine);
                logCapture.drain();
                Assert.assertEquals(0, logCapture.countLoggedRE(DEFINITION_WAIT_LOG));
            } finally {
                logCapture.stop();
            }
            Assert.assertEquals(viewSeqTxn + INCREMENTAL_REFRESH_TXNS, viewSeqTxn());
            Assert.assertFalse(viewState().hasDeferredDefinitionRefreshForTesting());
            applyViewWal();
            assertViewRows("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t1.0
                    2024-01-01T02:00:00.000000Z\t3.0
                    """);
            assertSqlCursors(MAX_SQL, "v");
        });
    }

    @Test
    public void testRefreshGoingAheadKeepsAnotherRefreshDeferredRequest() throws Exception {
        // Two refreshes meet one pending definition: R1 at its check after the reader opened, R2 at its check before
        // the lock. RESUME WAL FROM TXN skips the event between R2's request and R1's, so R1 goes ahead with the SQL
        // the skip left, and R2's request waits for the wake of RESUME WAL, which then computes R2's rows.
        assertMemoryLeak(() -> {
            createBase("TIMESTAMP");
            execute("INSERT INTO base VALUES ('2024-01-01T00:10:00.000000Z', 1.0), ('2024-01-01T01:10:00.000000Z', 2.0)");
            execute("CREATE MATERIALIZED VIEW v REFRESH MANUAL AS (" + SUM_SQL + ") PARTITION BY DAY");
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW v INCREMENTAL");
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW v SUSPEND WAL");
            execute(BUCKET_0200_ROWS);
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW v INCREMENTAL");
            final AtomicLong definitionSeqTxn = new AtomicLong(-1);
            final AtomicBoolean isRequestSeamRun = new AtomicBoolean();
            try (MatViewRefreshJob refreshJob = newRefreshJob(); MatViewRefreshJob secondJob = newRefreshJob()) {
                refreshJob.setOnHoldingLockForTesting(once(() -> {
                    alterView(MAX_SQL);
                    definitionSeqTxn.set(viewState().getPendingDefinitionSeqTxn());
                    viewState().setOnDeferredDefinitionRequestForTesting(unchecked(() -> {
                        isRequestSeamRun.set(true);
                        execute("INSERT INTO base VALUES ('2024-01-01T03:10:00.000000Z', 5.0)");
                        applyBaseWal();
                        // REFRESH does not wait on a suspended view WAL; its refresh defers before the lock
                        execute("REFRESH MATERIALIZED VIEW v INCREMENTAL");
                        drainMatViewQueue(secondJob);
                        Assert.assertTrue(viewState().hasDeferredDefinitionRefreshForTesting());
                        // RESUME WAL FROM TXN past the definition event, without the statement's wake
                        engine.getTableSequencerAPI().resumeTable(viewToken(), definitionSeqTxn.get() + 1);
                    }));
                }));
                drainMatViewQueue(refreshJob);
            }
            Assert.assertTrue(isRequestSeamRun.get());
            // R1 committed with the SQL the skip left, and R2's request stands
            Assert.assertEquals(definitionSeqTxn.get() + INCREMENTAL_REFRESH_TXNS, viewSeqTxn());
            Assert.assertTrue(viewState().hasDeferredDefinitionRefreshForTesting());
            Assert.assertEquals("", queuedTasks());
            engine.getMatViewStateStore().reenqueuePendingOnResume(viewToken());
            Assert.assertEquals("incremental_refresh v\n", queuedTasks());
            Assert.assertFalse(viewState().hasDeferredDefinitionRefreshForTesting());
            drainWalAndMatViewQueues();
            Assert.assertEquals(SUM_SQL, viewDefinition().getMatViewSql());
            assertViewRows("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t1.0
                    2024-01-01T01:00:00.000000Z\t2.0
                    2024-01-01T02:00:00.000000Z\t6.0
                    2024-01-01T03:00:00.000000Z\t5.0
                    """);
            assertSqlCursors(SUM_SQL, "v");
        });
    }

    @Test
    public void testResumeFromTxnAtDefinitionKeepsItPending() throws Exception {
        // RESUME WAL FROM TXN at the definition event skips the transactions before it and applies the event: the
        // event stays pending until the WAL apply reaches it, so a refresh in between defers.
        assertMemoryLeak(() -> {
            createImmediateView();
            execute("ALTER MATERIALIZED VIEW v SUSPEND WAL");
            // a view transaction before the definition event, which the resume skips
            execute("ALTER MATERIALIZED VIEW v SET REFRESH LIMIT 10 DAYS");
            final long limitSeqTxn = viewSeqTxn();
            alterView(MAX_SQL);
            final long definitionSeqTxn = viewState().getPendingDefinitionSeqTxn();
            Assert.assertEquals(limitSeqTxn + 1, definitionSeqTxn);
            execute(BUCKET_0200_ROWS);
            drainWalQueue();
            drainMatViewQueue(engine);
            Assert.assertTrue(viewState().hasDeferredDefinitionRefreshForTesting());
            execute("ALTER MATERIALIZED VIEW v RESUME WAL FROM TXN " + definitionSeqTxn);
            Assert.assertEquals(limitSeqTxn, viewState().getAppliedDefinitionSeqTxnForTesting());
            Assert.assertEquals("", queuedTasks());
            Assert.assertTrue(viewState().hasDeferredDefinitionRefreshForTesting());
            engine.getMatViewStateStore().enqueueIncrementalRefresh(viewToken());
            drainMatViewQueue(engine);
            Assert.assertEquals(definitionSeqTxn, viewSeqTxn());
            drainWalAndMatViewQueues();
            Assert.assertEquals(MAX_SQL, viewDefinition().getMatViewSql());
            assertViewRows("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t1.0
                    2024-01-01T02:00:00.000000Z\t3.0
                    """);
            assertSqlCursors(MAX_SQL, "v");
        });
    }

    @Test
    public void testResumeFromTxnOfBaseTableLeavesViewDefinitionPending() throws Exception {
        // RESUME WAL FROM TXN of a base table skips the base's transactions and records nothing for the views over it:
        // a definition event that waits in a view's WAL stays pending until the view's WAL applies it.
        assertMemoryLeak(() -> {
            createImmediateView();
            alterView(MAX_SQL);
            final TableToken baseToken = engine.verifyTableName("base");
            execute("ALTER TABLE base SUSPEND WAL");
            execute("INSERT INTO base VALUES ('2024-01-01T01:10:00.000000Z', 7.0)");
            final long skippedBaseSeqTxn = engine.getTableSequencerAPI().lastTxn(baseToken);
            execute(BUCKET_0200_ROWS);
            execute("ALTER TABLE base RESUME WAL FROM TXN " + (skippedBaseSeqTxn + 1));
            Assert.assertTrue(viewState().isDefinitionPending());
            Assert.assertEquals(-1, viewState().getAppliedDefinitionSeqTxnForTesting());
            drainWalAndMatViewQueues();
            Assert.assertFalse(viewState().isDefinitionPending());
            // the skipped 01:10 row never reaches the base
            assertViewRows("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t1.0
                    2024-01-01T02:00:00.000000Z\t3.0
                    """);
            assertSqlCursors(MAX_SQL, "v");
        });
    }

    @Test
    public void testResumeFromTxnPastDefinitionOfViewWithoutState() throws Exception {
        // RESUME WAL FROM TXN past a definition event of a view that has no state, as on a node whose store holds none,
        // skips the event and records nothing: the view keeps its SQL, and its next state refreshes with it.
        assertMemoryLeak(() -> {
            createImmediateView();
            execute("ALTER MATERIALIZED VIEW v SUSPEND WAL");
            alterView(MAX_SQL);
            final long definitionSeqTxn = viewState().getPendingDefinitionSeqTxn();
            execute(BUCKET_0200_ROWS);
            drainWalQueue();
            engine.getMatViewStateStore().clear();
            Assert.assertNull(viewState());
            execute("ALTER MATERIALIZED VIEW v RESUME WAL FROM TXN " + (definitionSeqTxn + 1));
            engine.hydrateMatViewStateStore();
            Assert.assertEquals(-1, viewState().getAppliedDefinitionSeqTxnForTesting());
            drainWalAndMatViewQueues();
            Assert.assertEquals(SUM_SQL, viewDefinition().getMatViewSql());
            assertViewRows("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t1.0
                    2024-01-01T02:00:00.000000Z\t6.0
                    """);
            assertSqlCursors(SUM_SQL, "v");
        });
    }

    @Test
    public void testResumeFromTxnPastDefinitionRedeliversWithoutApplyRun() throws Exception {
        // RESUME WAL FROM TXN past the definition event wakes the deferred refresh from the statement: a full
        // notification queue resets the view's tracker instead of starting an apply run, and the refresh job alone
        // runs the refresh, with the SQL the skip left.
        assertMemoryLeak(() -> {
            createImmediateView();
            execute("ALTER MATERIALIZED VIEW v SUSPEND WAL");
            alterView(MAX_SQL);
            final long definitionSeqTxn = viewState().getPendingDefinitionSeqTxn();
            execute(BUCKET_0200_ROWS);
            drainWalQueue();
            drainMatViewQueue(engine);
            Assert.assertTrue(viewState().hasDeferredDefinitionRefreshForTesting());
            Assert.assertEquals(definitionSeqTxn, viewSeqTxn());
            execute("ALTER MATERIALIZED VIEW v RESUME WAL FROM TXN " + (definitionSeqTxn + 1));
            engine.notifyWalTxnRepublisher(viewToken());
            Assert.assertEquals(-1, writerTxn());
            drainMatViewQueue(engine);
            Assert.assertEquals(definitionSeqTxn + INCREMENTAL_REFRESH_TXNS, viewSeqTxn());
            drainWalQueue();
            Assert.assertEquals(SUM_SQL, viewDefinition().getMatViewSql());
            assertViewRows("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t1.0
                    2024-01-01T02:00:00.000000Z\t6.0
                    """);
            assertSqlCursors(SUM_SQL, "v");
        });
    }

    @Test
    public void testResumeWalOnViewThatNeverDeferredEnqueuesNothing() throws Exception {
        // Neither the apply of a definition event nor RESUME WAL wakes a refresh when none deferred.
        assertMemoryLeak(() -> {
            createImmediateView();
            alterView(MAX_SQL);
            drainWalQueue();
            Assert.assertEquals("", queuedTasks());
            execute("ALTER MATERIALIZED VIEW v SUSPEND WAL");
            execute("ALTER MATERIALIZED VIEW v RESUME WAL");
            Assert.assertEquals("", queuedTasks());
            Assert.assertFalse(viewState().hasDeferredDefinitionRefreshForTesting());
            drainWalQueue();
            Assert.assertEquals("", queuedTasks());
        });
    }

    @Test
    public void testResumeWalRedeliversInvalidationOfValidView() throws Exception {
        // RESUME WAL redelivers the pending invalidation of a valid view whose task never reached the refresh job: the
        // view goes invalid with the invalidation's reason.
        assertMemoryLeak(() -> {
            createImmediateView();
            viewState().markAsPendingInvalidation("test invalidation");
            execute("ALTER MATERIALIZED VIEW v SUSPEND WAL");
            execute("ALTER MATERIALIZED VIEW v RESUME WAL");
            Assert.assertEquals("invalidate v\n", queuedTasks());
            drainWalAndMatViewQueues();
            Assert.assertTrue(viewState().isInvalid());
            assertQuery("SELECT view_status, invalidation_reason FROM materialized_views() WHERE view_name = 'v'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("view_status\tinvalidation_reason\ninvalid\ttest invalidation\n");
        });
    }

    @Test
    public void testResumeWalRedeliversNoInvalidationOfInvalidView() throws Exception {
        // RESUME WAL redelivers a pending invalidation of a valid view only: an invalid view's stays as it is.
        assertMemoryLeak(() -> {
            createImmediateView();
            engine.getMatViewStateStore().enqueueInvalidate(viewToken(), "test invalidation");
            drainWalAndMatViewQueues();
            Assert.assertTrue(viewState().isInvalid());
            viewState().markAsPendingInvalidation("another invalidation");
            execute("ALTER MATERIALIZED VIEW v SUSPEND WAL");
            execute("ALTER MATERIALIZED VIEW v RESUME WAL");
            Assert.assertEquals("", queuedTasks());
            Assert.assertEquals("another invalidation", viewState().getPendingInvalidationReason());
        });
    }

    @Test
    public void testTimerRefreshInWindowWaitsForDefinition() throws Exception {
        // A timer firing between the base apply and the apply of the definition event: its refresh defers, and still
        // finishes as a task, so the next firing enqueues the next one.
        assertMemoryLeak(() -> {
            currentMicros = parseFloorPartialTimestamp("2024-01-01T03:00:00.000000Z");
            createBase("TIMESTAMP");
            execute("INSERT INTO base VALUES ('2024-01-01T00:10:00.000000Z', 1.0)");
            execute("CREATE MATERIALIZED VIEW v REFRESH EVERY 1m AS (" + SUM_SQL + ") PARTITION BY DAY");
            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
            drainWalQueue();
            drainMatViewTimerQueue(timerJob);
            currentMicros += Micros.MINUTE_MICROS;
            drainMatViewTimerQueue(timerJob);
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t1.0
                    """);

            alterView(MAX_SQL);
            execute(BUCKET_0200_ROWS);
            applyBaseWal();
            final long lastRefreshBaseTxn = viewState().getLastRefreshBaseTxn();
            currentMicros += Micros.MINUTE_MICROS;
            drainMatViewTimerQueue(timerJob);
            final long refreshSeq = viewState().getRefreshSeq();
            drainMatViewQueue(engine);
            Assert.assertEquals(lastRefreshBaseTxn, viewState().getLastRefreshBaseTxn());
            Assert.assertTrue(viewState().hasDeferredDefinitionRefreshForTesting());
            Assert.assertEquals(refreshSeq + 1, viewState().getRefreshSeq());
            currentMicros += Micros.MINUTE_MICROS;
            drainMatViewTimerQueue(timerJob);
            Assert.assertTrue(queuedTasks().contains("incremental_refresh v\n"));
            drainMatViewQueue(engine);
            Assert.assertEquals(lastRefreshBaseTxn, viewState().getLastRefreshBaseTxn());

            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t1.0
                    2024-01-01T02:00:00.000000Z\t3.0
                    """);
            assertSqlCursors(MAX_SQL, "v");
        });
    }

    @Test
    public void testTwoClaimsOfOneRequestEnqueueOneRefresh() throws Exception {
        // A second claim takes the request between the first claim's read without the lock and its lock: the first
        // finds the request gone, and the two enqueue one refresh.
        assertMemoryLeak(() -> {
            createImmediateView();
            execute("ALTER MATERIALIZED VIEW v SUSPEND WAL");
            alterView(MAX_SQL);
            final long definitionSeqTxn = viewState().getPendingDefinitionSeqTxn();
            execute(BUCKET_0200_ROWS);
            drainWalQueue();
            drainMatViewQueue(engine);
            Assert.assertTrue(viewState().hasDeferredDefinitionRefreshForTesting());
            // RESUME WAL FROM TXN past the definition event, without the statement's wake
            engine.getTableSequencerAPI().resumeTable(viewToken(), definitionSeqTxn + 1);
            final AtomicBoolean isSeamRun = new AtomicBoolean();
            viewState().setOnDeferredDefinitionClaimForTesting(() -> {
                isSeamRun.set(true);
                engine.getMatViewStateStore().reenqueuePendingOnResume(viewToken());
            });
            engine.getMatViewStateStore().reenqueuePendingOnResume(viewToken());
            Assert.assertTrue(isSeamRun.get());
            Assert.assertEquals("incremental_refresh v\n", queuedTasks());
            Assert.assertFalse(viewState().hasDeferredDefinitionRefreshForTesting());
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t1.0
                    2024-01-01T02:00:00.000000Z\t6.0
                    """);
            assertSqlCursors(SUM_SQL, "v");
        });
    }

    @Test
    public void testTwoPendingDefinitionsDeferUntilTheLastApplies() throws Exception {
        // Two definition events wait at once, and the apply batch applies the first and fails on the second: the
        // refreshes keep deferring, and the deferred one runs once the second applies, with its SQL.
        final AtomicBoolean isFailArmed = new AtomicBoolean();
        final AtomicInteger definitionOpens = new AtomicInteger();
        final FilesFacade ff = newDefinitionOpenFailingFacade(isFailArmed, definitionOpens);
        assertMemoryLeak(ff, () -> {
            createImmediateView();
            execute("INSERT INTO base VALUES ('2024-01-01T01:10:00.000000Z', 1.0)");
            // the seam runs one ALTER ... AS between the validation and the write of another
            viewState().setOnAlterValidatedForTesting(unchecked(() -> alterView(MAX_SQL)));
            alterView(MIN_SQL);
            final long definitionSeqTxn = viewState().getPendingDefinitionSeqTxn();
            Assert.assertEquals(viewSeqTxn(), definitionSeqTxn);
            execute(BUCKET_0200_ROWS);
            try (ApplyWal2TableJob walApplyJob = createWalApplyJob()) {
                Assert.assertTrue(walApplyJob.run());
            }
            drainMatViewQueue(engine);
            Assert.assertTrue(viewState().hasDeferredDefinitionRefreshForTesting());
            isFailArmed.set(true);
            try {
                drainWalQueue();
            } finally {
                isFailArmed.set(false);
            }
            Assert.assertEquals(2, definitionOpens.get());
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(viewToken()));
            Assert.assertEquals(MAX_SQL, viewDefinition().getMatViewSql());
            Assert.assertEquals(definitionSeqTxn - 1, viewState().getAppliedDefinitionSeqTxnForTesting());
            Assert.assertEquals("", queuedTasks());
            Assert.assertTrue(viewState().hasDeferredDefinitionRefreshForTesting());
            final long viewSeqTxn = viewSeqTxn();
            execute("INSERT INTO base VALUES ('2024-01-01T03:10:00.000000Z', 5.0)");
            drainWalQueue();
            drainMatViewQueue(engine);
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());

            execute("ALTER MATERIALIZED VIEW v RESUME WAL");
            Assert.assertEquals("", queuedTasks());
            drainWalQueue();
            Assert.assertEquals("incremental_refresh v\n", queuedTasks());
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t1.0
                    2024-01-01T01:00:00.000000Z\t1.0
                    2024-01-01T02:00:00.000000Z\t1.0
                    2024-01-01T03:00:00.000000Z\t5.0
                    """);
            assertSqlCursors(MIN_SQL, "v");
        });
    }

    @Test
    public void testTwoRefreshesDeferredOnOneDefinitionShareOneRedelivery() throws Exception {
        // R1 defers at its check after the reader opened and, while it still holds the view lock, R2 defers at its
        // check before the lock: the apply of the definition event wakes one refresh, which covers the rows of both.
        assertMemoryLeak(() -> {
            createImmediateView();
            execute("INSERT INTO base VALUES ('2024-01-01T01:10:00.000000Z', 1.0)");
            applyBaseWal();
            final AtomicLong definitionSeqTxn = new AtomicLong(-1);
            final AtomicBoolean isUnlockSeamRun = new AtomicBoolean();
            try (MatViewRefreshJob refreshJob = newRefreshJob(); MatViewRefreshJob secondJob = newRefreshJob()) {
                refreshJob.setOnHoldingLockForTesting(once(() -> {
                    alterView(MAX_SQL);
                    definitionSeqTxn.set(viewState().getPendingDefinitionSeqTxn());
                    execute(BUCKET_0200_ROWS);
                    applyBaseWal();
                    viewState().setOnBeforeUnlockForTesting(unchecked(() -> {
                        isUnlockSeamRun.set(true);
                        execute("INSERT INTO base VALUES ('2024-01-01T03:10:00.000000Z', 5.0)");
                        applyBaseWal();
                        engine.getMatViewStateStore().enqueueIncrementalRefresh(viewToken());
                        drainMatViewQueue(secondJob);
                        Assert.assertFalse(viewState().hasPendingIncrementalRefreshForTesting());
                    }));
                }));
                drainMatViewQueue(refreshJob);
            }
            Assert.assertTrue(isUnlockSeamRun.get());
            Assert.assertEquals(definitionSeqTxn.get(), viewSeqTxn());
            Assert.assertTrue(viewState().hasDeferredDefinitionRefreshForTesting());
            Assert.assertEquals("", queuedTasks());
            drainWalQueue();
            Assert.assertEquals("incremental_refresh v\n", queuedTasks());
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t1.0
                    2024-01-01T01:00:00.000000Z\t1.0
                    2024-01-01T02:00:00.000000Z\t3.0
                    2024-01-01T03:00:00.000000Z\t5.0
                    """);
            assertSqlCursors(MAX_SQL, "v");
        });
    }

    @Test
    public void testTwoViewsOnOneBaseOnlyPendingViewDefers() throws Exception {
        // Of two views on one base, only the one with a pending definition defers: the other refreshes at each base
        // commit, and the base's later commits still reach both.
        assertMemoryLeak(() -> {
            createBase("TIMESTAMP");
            execute("INSERT INTO base VALUES ('2024-01-01T00:10:00.000000Z', 1.0)");
            execute("CREATE MATERIALIZED VIEW v AS (" + SUM_SQL + ") PARTITION BY DAY");
            execute("CREATE MATERIALIZED VIEW v2 AS (" + SUM_SQL + ") PARTITION BY DAY");
            drainWalAndMatViewQueues();
            alterView(MAX_SQL);
            final long viewSeqTxn = viewSeqTxn();
            final TableToken v2Token = engine.verifyTableName("v2");
            final long v2SeqTxn = engine.getTableSequencerAPI().getTxnTracker(v2Token).getSeqTxn();
            execute(BUCKET_0200_ROWS);
            applyBaseWal();
            drainMatViewQueue(engine);
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            Assert.assertEquals(v2SeqTxn + INCREMENTAL_REFRESH_TXNS, engine.getTableSequencerAPI().getTxnTracker(v2Token).getSeqTxn());
            execute("INSERT INTO base VALUES ('2024-01-01T03:10:00.000000Z', 5.0)");
            applyBaseWal();
            drainMatViewQueue(engine);
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            Assert.assertEquals(v2SeqTxn + 2 * INCREMENTAL_REFRESH_TXNS, engine.getTableSequencerAPI().getTxnTracker(v2Token).getSeqTxn());
            Assert.assertTrue(viewState().hasDeferredDefinitionRefreshForTesting());

            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t1.0
                    2024-01-01T02:00:00.000000Z\t3.0
                    2024-01-01T03:00:00.000000Z\t5.0
                    """);
            assertSqlCursors(MAX_SQL, "v");
            assertQuery("v2").timestamp("ts").noLeakCheck().expectSize().returns("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t1.0
                    2024-01-01T02:00:00.000000Z\t6.0
                    2024-01-01T03:00:00.000000Z\t5.0
                    """);
            assertSqlCursors(SUM_SQL, "v2");
        });
    }

    @Test
    public void testUpToDateViewDefersAndRedeliveryCommitsNothing() throws Exception {
        // A refresh of an up-to-date view defers too, and the refresh the apply of the definition event wakes finds the
        // view up to date: the view's WAL gets the definition event alone.
        assertMemoryLeak(() -> {
            createImmediateView();
            alterView(MAX_SQL);
            final long definitionSeqTxn = viewState().getPendingDefinitionSeqTxn();
            engine.getMatViewStateStore().enqueueIncrementalRefresh(viewToken());
            drainMatViewQueue(engine);
            Assert.assertTrue(viewState().hasDeferredDefinitionRefreshForTesting());
            Assert.assertEquals(definitionSeqTxn, viewSeqTxn());
            drainWalQueue();
            Assert.assertEquals("incremental_refresh v\n", queuedTasks());
            drainWalAndMatViewQueues();
            Assert.assertEquals(definitionSeqTxn, viewSeqTxn());
            Assert.assertEquals("", queuedTasks());
            assertViewRows("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t1.0
                    """);
        });
    }

    @Test
    public void testUserFullOnSuspendedViewWalWaitsForDefinition() throws Exception {
        // REFRESH ... FULL on a view WAL suspended short of the definition event parks; RESUME WAL redelivers it and
        // it parks again, silently, until the event applies, and then rebuilds with the new SQL.
        assertMemoryLeak(() -> {
            createManualViewWithTwoRowBucket();
            alterView(MAX_SQL);
            execute("ALTER MATERIALIZED VIEW v SUSPEND WAL");
            final long viewSeqTxn = viewSeqTxn();
            execute(BUCKET_0200_ROWS);
            drainWalQueue();
            final LogCapture logCapture = new LogCapture();
            logCapture.start();
            try {
                execute("REFRESH MATERIALIZED VIEW v FULL");
                drainMatViewQueue(engine);
                Assert.assertNotNull(viewState().getPendingFullRefreshOwnerForTesting());
                Assert.assertEquals(viewSeqTxn, viewSeqTxn());
                execute("ALTER MATERIALIZED VIEW v RESUME WAL");
                Assert.assertEquals("full_refresh v\n", queuedTasks());
                drainMatViewQueue(engine);
                Assert.assertNotNull(viewState().getPendingFullRefreshOwnerForTesting());
                Assert.assertEquals(viewSeqTxn, viewSeqTxn());
                drainWalAndMatViewQueues();
                logCapture.drain();
                Assert.assertEquals(1, logCapture.countLoggedRE(DEFINITION_WAIT_LOG));
            } finally {
                logCapture.stop();
            }
            Assert.assertNull(viewState().getPendingFullRefreshOwnerForTesting());
            assertViewRows("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t5.0
                    2024-01-01T02:00:00.000000Z\t3.0
                    """);
            assertSqlCursors(MAX_SQL, "v");
        });
    }

    @Test
    public void testUserIncrementalOnSuspendedViewWalWaitsForDefinition() throws Exception {
        // REFRESH ... INCREMENTAL on a view WAL suspended short of the definition event writes nothing into it: the
        // refresh defers, RESUME WAL wakes nothing while the event waits, and its apply wakes the refresh.
        assertMemoryLeak(() -> {
            createManualView();
            alterView(MAX_SQL);
            execute("ALTER MATERIALIZED VIEW v SUSPEND WAL");
            final long viewSeqTxn = viewSeqTxn();
            execute(BUCKET_0200_ROWS);
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW v INCREMENTAL");
            drainMatViewQueue(engine);
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            Assert.assertTrue(viewState().hasDeferredDefinitionRefreshForTesting());
            execute("ALTER MATERIALIZED VIEW v RESUME WAL");
            Assert.assertEquals("", queuedTasks());
            drainWalQueue();
            Assert.assertEquals("incremental_refresh v\n", queuedTasks());
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t1.0
                    2024-01-01T02:00:00.000000Z\t3.0
                    """);
            assertSqlCursors(MAX_SQL, "v");
        });
    }

    @Test
    public void testUserRangeAfterTrackerResetOfAppliedDefinitionSucceeds() throws Exception {
        // A republish that resets the view's tracker after the definition event applied changes nothing for a user
        // REFRESH ... RANGE: it computes with the new SQL.
        assertMemoryLeak(() -> {
            createManualView();
            alterView(MAX_SQL);
            drainWalQueue();
            execute(BUCKET_0200_ROWS);
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW v RANGE FROM '2024-01-01T02:00:00.000000Z' TO '2024-01-01T02:59:59.999999Z'");
            engine.notifyWalTxnRepublisher(viewToken());
            Assert.assertEquals(-1, writerTxn());
            final long viewSeqTxn = viewSeqTxn();
            drainMatViewQueue(engine);
            // a RANGE commits its rows alone
            Assert.assertEquals(viewSeqTxn + 1, viewSeqTxn());
            Assert.assertNull(viewState().getLastRefreshError());
            applyViewWal();
            assertViewRows("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t1.0
                    2024-01-01T02:00:00.000000Z\t3.0
                    """);
            assertSqlCursors(MAX_SQL, "v");
        });
    }

    @Test
    public void testUserRangeReadsDefinitionAppliedBeforeLock() throws Exception {
        // ALTER ... AS, its WAL apply and the rows after it land after a user REFRESH ... RANGE read the view's
        // definition and before it takes the view lock: the RANGE reads the definition again and computes with the new
        // SQL.
        assertMemoryLeak(() -> {
            createManualView();
            execute("REFRESH MATERIALIZED VIEW v RANGE FROM '2024-01-01T02:00:00.000000Z' TO '2024-01-01T02:59:59.999999Z'");
            final AtomicBoolean isSeamRun = new AtomicBoolean();
            try (MatViewRefreshJob refreshJob = newRefreshJob()) {
                refreshJob.setOnBeforeRefreshLockForTesting(once(() -> {
                    isSeamRun.set(true);
                    applyAlterAndRows();
                }));
                drainMatViewQueue(refreshJob);
            }
            Assert.assertTrue(isSeamRun.get());
            Assert.assertNull(viewState().getLastRefreshError());
            applyViewWal();
            assertViewRows("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t1.0
                    2024-01-01T02:00:00.000000Z\t3.0
                    """);
            assertSqlCursors(MAX_SQL, "v");
        });
    }

    @Test
    public void testViewScopedRefreshReadsDefinitionAppliedBeforeReader() throws Exception {
        // ALTER ... AS, its WAL apply and the rows after it land after a view-scoped incremental refresh read the view's
        // definition under the view lock and before it opens its base reader: the refresh reads the definition again
        // and computes with the new SQL.
        assertMemoryLeak(() -> {
            createManualView();
            execute("REFRESH MATERIALIZED VIEW v INCREMENTAL");
            final AtomicBoolean isSeamRun = new AtomicBoolean();
            try (MatViewRefreshJob refreshJob = newRefreshJob()) {
                refreshJob.setOnHoldingLockForTesting(once(() -> {
                    isSeamRun.set(true);
                    applyAlterAndRows();
                }));
                drainMatViewQueue(refreshJob);
            }
            Assert.assertTrue(isSeamRun.get());
            Assert.assertFalse(viewState().hasDeferredDefinitionRefreshForTesting());
            applyViewWal();
            assertViewRows("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t1.0
                    2024-01-01T02:00:00.000000Z\t3.0
                    """);
            assertSqlCursors(MAX_SQL, "v");
        });
    }

    @Test
    public void testWriteDeniedSuspensionSkipsBeforeDefinitionCheck() throws Exception {
        // With cairo.wal.apply.suspended.write.denied on, a hard-suspended view skips its refreshes before the check of
        // a pending definition, as it did before: nothing defers, and RESUME WAL and the apply of the event wake
        // nothing. The next base commit after the resume refreshes the view with the new SQL.
        setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, "true");
        assertMemoryLeak(() -> {
            createImmediateView();
            alterView(MAX_SQL);
            execute("ALTER MATERIALIZED VIEW v SUSPEND WAL");
            final long viewSeqTxn = viewSeqTxn();
            execute(BUCKET_0200_ROWS);
            drainWalAndMatViewQueues();
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            Assert.assertFalse(viewState().hasDeferredDefinitionRefreshForTesting());
            execute("ALTER MATERIALIZED VIEW v RESUME WAL");
            Assert.assertEquals("", queuedTasks());
            drainWalQueue();
            Assert.assertEquals(MAX_SQL, viewDefinition().getMatViewSql());
            Assert.assertEquals("", queuedTasks());
            execute("INSERT INTO base VALUES ('2024-01-01T03:10:00.000000Z', 5.0)");
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t1.0
                    2024-01-01T02:00:00.000000Z\t3.0
                    2024-01-01T03:00:00.000000Z\t5.0
                    """);
            assertSqlCursors(MAX_SQL, "v");
        });
    }

    private static void alterView(String sql) throws Exception {
        execute("ALTER MATERIALIZED VIEW v AS (" + sql + ")");
    }

    // ALTER ... AS to MAX_SQL with its WAL apply, and the rows of the 02:00 bucket with theirs.
    private static void applyAlterAndRows() throws Exception {
        alterView(MAX_SQL);
        applyViewWal();
        execute(BUCKET_0200_ROWS);
        applyBaseWal();
    }

    // Applies the base's WAL alone, as an apply run that serves the base before the view.
    private static void applyBaseWal() {
        try (ApplyWal2TableJob walApplyJob = createWalApplyJob()) {
            walApplyJob.applyWalDirect(engine.verifyTableName("base"), Job.RUNNING_STATUS);
        }
    }

    private static void applyViewWal() {
        try (ApplyWal2TableJob walApplyJob = createWalApplyJob()) {
            walApplyJob.applyWalDirect(viewToken(), Job.RUNNING_STATUS);
        }
    }

    private static void closeRuntime(FiberRuntime runtime) throws Exception {
        runtime.beginQuiesce();
        TestUtils.assertEventually(() -> {
            runtime.drain(8);
            Assert.assertEquals(FiberRuntimeState.CLOSED, runtime.state());
        });
    }

    private static void createBase(String timestampType) throws Exception {
        execute("CREATE TABLE base (ts " + timestampType + ", x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    // Fails, while armed, the second open of a view's definition file _mv, which a definition event or SET REFRESH
    // LIMIT writes when the WAL apply job applies it.
    private static FilesFacade newDefinitionOpenFailingFacade(AtomicBoolean isFailArmed, AtomicInteger definitionOpens) {
        return new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (isFailArmed.get()
                        && Utf8s.endsWithAscii(name, MatViewDefinition.MAT_VIEW_DEFINITION_FILE_NAME)
                        && definitionOpens.incrementAndGet() == 2) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
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
                return "mat-view-pending-definition-test";
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

    private static MatViewRefreshJob newRefreshJob() {
        return new MatViewRefreshJob(0, engine, 1);
    }

    // A seam that runs the action on its first firing only.
    private static Runnable once(SeamAction action) {
        final AtomicBoolean isRun = new AtomicBoolean();
        return () -> {
            if (isRun.compareAndSet(false, true)) {
                unchecked(action).run();
            }
        };
    }

    // The owners of the FULL refresh tasks in the queue, in queue order. The tasks go back on the queue in their order.
    private static ObjList<Object> queuedFullRefreshOwners() {
        final MatViewStateStore stateStore = engine.getMatViewStateStore();
        final ObjList<MatViewRefreshTask> tasks = new ObjList<>();
        final ObjList<Object> owners = new ObjList<>();
        while (true) {
            final MatViewRefreshTask task = new MatViewRefreshTask();
            if (!stateStore.tryDequeueRefreshTask(task)) {
                break;
            }
            tasks.add(task);
            if (task.operation == MatViewRefreshTask.FULL_REFRESH) {
                owners.add(task.fullRefreshOwner);
            }
        }
        for (int i = 0, n = tasks.size(); i < n; i++) {
            stateStore.reenqueueRefreshTask(tasks.getQuick(i));
        }
        return owners;
    }

    // The refresh tasks in the queue, one line each, in queue order: the operation and the view's name, or the base
    // table's of a task that refreshes the views of a base. The tasks go back on the queue in their order.
    private static String queuedTasks() {
        final MatViewStateStore stateStore = engine.getMatViewStateStore();
        final ObjList<MatViewRefreshTask> tasks = new ObjList<>();
        final StringSink sink = new StringSink();
        while (true) {
            final MatViewRefreshTask task = new MatViewRefreshTask();
            if (!stateStore.tryDequeueRefreshTask(task)) {
                break;
            }
            tasks.add(task);
            sink.put(MatViewRefreshTask.getRefreshOperationName(task.operation)).put(' ')
                    .put(task.matViewToken != null ? task.matViewToken.getTableName() : "base:" + task.baseTableToken.getTableName())
                    .put('\n');
        }
        for (int i = 0, n = tasks.size(); i < n; i++) {
            stateStore.reenqueueRefreshTask(tasks.getQuick(i));
        }
        return sink.toString();
    }

    // The unsolicited-lock arm of the WAL apply job resets the view's tracker: the view's writer held under a reason
    // the WAL machinery did not arrange, a notification for work that is applied, and one apply run.
    private static void resetViewTrackerByUnsolicitedLock() {
        try (TableWriter ignore = engine.getWriter(viewToken(), "test lock")) {
            engine.notifyWalTxnCommitted(viewToken());
            try (ApplyWal2TableJob walApplyJob = createWalApplyJob()) {
                walApplyJob.run();
            }
        }
    }

    // Runs one dispatch pass of the fiber host's refresh job and drains the fiber it launched.
    private static void runOnFiber(MatViewRefreshJob refreshJob, FiberRuntime runtime) throws Exception {
        Assert.assertTrue(refreshJob.run());
        TestUtils.assertEventually(() -> {
            runtime.drain(8);
            Assert.assertEquals(0, runtime.getOutstandingTaskCount());
        });
    }

    private static Runnable unchecked(SeamAction action) {
        return () -> {
            try {
                action.run();
            } catch (RuntimeException | Error e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    private static MatViewDefinition viewDefinition() {
        return engine.getDependentViewGraph().getViewDefinition(viewToken());
    }

    private static long viewSeqTxn() {
        return engine.getTableSequencerAPI().getTxnTracker(viewToken()).getSeqTxn();
    }

    private static MatViewState viewState() {
        return engine.getMatViewStateStore().getViewState(viewToken());
    }

    private static TableToken viewToken() {
        return engine.verifyTableName("v");
    }

    private static long writerTxn() {
        return engine.getTableSequencerAPI().getTxnTracker(viewToken()).getWriterTxn();
    }

    // The definition event applies or fails: here it fails, as the view's graph entry is gone, which suspends the
    // view's WAL. The refreshes defer meanwhile; RESUME WAL lets the event apply, which wakes the deferred refresh, and
    // RESUME WAL FROM TXN past the event skips it and wakes the refresh at once, before any apply run.
    private void assertDeferredRefreshAfterDefinitionApplyFailure(boolean isSkipped) throws Exception {
        assertMemoryLeak(() -> {
            createImmediateView();
            alterView(MAX_SQL);
            final long definitionSeqTxn = viewState().getPendingDefinitionSeqTxn();
            execute(BUCKET_0200_ROWS);
            final MatViewDefinition definition = viewDefinition();
            engine.getDependentViewGraph().removeView(viewToken());
            try {
                drainWalQueue();
                Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(viewToken()));
            } finally {
                engine.getDependentViewGraph().addView(definition);
            }
            Assert.assertEquals(definitionSeqTxn - 1, writerTxn());
            drainMatViewQueue(engine);
            Assert.assertEquals(definitionSeqTxn, viewSeqTxn());
            Assert.assertTrue(viewState().hasDeferredDefinitionRefreshForTesting());
            if (isSkipped) {
                execute("ALTER MATERIALIZED VIEW v RESUME WAL FROM TXN " + (definitionSeqTxn + 1));
                Assert.assertEquals(definitionSeqTxn - 1, writerTxn());
                Assert.assertEquals("incremental_refresh v\n", queuedTasks());
                drainMatViewQueue(engine);
                Assert.assertEquals(definitionSeqTxn + INCREMENTAL_REFRESH_TXNS, viewSeqTxn());
                drainWalQueue();
                Assert.assertEquals(SUM_SQL, viewDefinition().getMatViewSql());
                assertViewRows("""
                        ts\ts
                        2024-01-01T00:00:00.000000Z\t1.0
                        2024-01-01T02:00:00.000000Z\t6.0
                        """);
                assertSqlCursors(SUM_SQL, "v");
            } else {
                execute("ALTER MATERIALIZED VIEW v RESUME WAL");
                Assert.assertEquals("", queuedTasks());
                drainWalQueue();
                Assert.assertEquals("incremental_refresh v\n", queuedTasks());
                drainWalAndMatViewQueues();
                Assert.assertEquals(MAX_SQL, viewDefinition().getMatViewSql());
                assertViewRows("""
                        ts\ts
                        2024-01-01T00:00:00.000000Z\t1.0
                        2024-01-01T02:00:00.000000Z\t3.0
                        """);
                assertSqlCursors(MAX_SQL, "v");
            }
            assertViewStatus("valid");
        });
    }

    // The wake of the deferred refresh that the apply of the definition event sends fails to enqueue, once: the apply
    // batch goes on, and the refresh job's retry scan delivers the refresh.
    private void assertDeferredRefreshClaimEnqueueFailure(boolean isOutOfMemory, String loggedError) throws Exception {
        assertMemoryLeak(() -> {
            createImmediateView();
            alterView(MAX_SQL);
            execute(BUCKET_0200_ROWS);
            applyBaseWal();
            drainMatViewQueue(engine);
            Assert.assertTrue(viewState().hasDeferredDefinitionRefreshForTesting());
            final MatViewStateStoreImpl stateStore = (MatViewStateStoreImpl) engine.getMatViewStateStore();
            final AtomicInteger appends = new AtomicInteger();
            stateStore.setOnTaskQueueAppendForTesting(() -> {
                if (appends.incrementAndGet() == 1) {
                    if (isOutOfMemory) {
                        throw new OutOfMemoryError("test enqueue failure");
                    }
                    throw CairoException.nonCritical().put("test enqueue failure");
                }
            });
            final LogCapture logCapture = new LogCapture();
            logCapture.start();
            try {
                drainWalQueue();
                logCapture.drain();
                Assert.assertEquals(1, logCapture.countLoggedRE(loggedError));
            } finally {
                logCapture.stop();
                stateStore.setOnTaskQueueAppendForTesting(null);
            }
            Assert.assertEquals(1, appends.get());
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(viewToken()));
            Assert.assertEquals(MAX_SQL, viewDefinition().getMatViewSql());
            Assert.assertFalse(viewState().hasDeferredDefinitionRefreshForTesting());
            Assert.assertEquals("", queuedTasks());
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t1.0
                    2024-01-01T02:00:00.000000Z\t3.0
                    """);
            assertSqlCursors(MAX_SQL, "v");
        });
    }

    // The wake of a deferred refresh and the redelivery of a parked FULL, which RESUME WAL and the WAL apply job run
    // together once a definition event no longer waits: the redelivery fails, and the wake fails before it when
    // isWakeFailing, with the redelivery's exception instance when isSameInstance. Both run, the redelivery's failure
    // goes up and carries the wake's, unless it is the same instance, and the retry scan delivers both.
    private void assertMarkerRedeliveryFailure(boolean isWakeFailing, boolean isSameInstance) throws Exception {
        assertMemoryLeak(() -> {
            createImmediateViewWithTwoRowBucket();
            execute("ALTER MATERIALIZED VIEW v SUSPEND WAL");
            alterView(MAX_SQL);
            final long definitionSeqTxn = viewState().getPendingDefinitionSeqTxn();
            execute(BUCKET_0200_ROWS);
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW v FULL");
            drainMatViewQueue(engine);
            Assert.assertNotNull(viewState().getPendingFullRefreshOwnerForTesting());
            Assert.assertTrue(viewState().hasDeferredDefinitionRefreshForTesting());
            // RESUME WAL FROM TXN past the definition event, without the statement's wake
            engine.getTableSequencerAPI().resumeTable(viewToken(), definitionSeqTxn + 1);

            final MatViewStateStoreImpl stateStore = (MatViewStateStoreImpl) engine.getMatViewStateStore();
            final AtomicInteger appends = new AtomicInteger();
            final AtomicReference<Throwable> wakeFailure = new AtomicReference<>();
            final CairoException sharedFailure = CairoException.nonCritical().put("test failure");
            stateStore.setOnTaskQueueAppendForTesting(() -> {
                final int append = appends.incrementAndGet();
                if (append == 1 && isWakeFailing) {
                    final CairoException e = isSameInstance ? sharedFailure : CairoException.nonCritical().put("test wake failure");
                    wakeFailure.set(e);
                    throw e;
                }
                if (append == 2) {
                    if (isSameInstance) {
                        throw sharedFailure;
                    }
                    throw new IllegalStateException("test redelivery failure");
                }
            });
            Throwable failure = null;
            try {
                engine.getMatViewStateStore().reenqueuePendingOnResume(viewToken());
            } catch (Throwable th) {
                failure = th;
            } finally {
                stateStore.setOnTaskQueueAppendForTesting(null);
            }
            Assert.assertEquals(2, appends.get());
            Assert.assertNotNull(failure);
            if (isSameInstance) {
                Assert.assertSame(wakeFailure.get(), failure);
                Assert.assertEquals(0, failure.getSuppressed().length);
            } else {
                Assert.assertTrue(failure instanceof IllegalStateException);
                if (isWakeFailing) {
                    Assert.assertEquals(1, failure.getSuppressed().length);
                    Assert.assertSame(wakeFailure.get(), failure.getSuppressed()[0]);
                } else {
                    Assert.assertEquals(0, failure.getSuppressed().length);
                }
            }
            // the wake took the request either way, and the retry scan delivers whatever failed
            Assert.assertFalse(viewState().hasDeferredDefinitionRefreshForTesting());
            Assert.assertEquals(isWakeFailing ? "" : "incremental_refresh v\n", queuedTasks());
            drainWalAndMatViewQueues();
            Assert.assertNull(viewState().getPendingFullRefreshOwnerForTesting());
            Assert.assertEquals(SUM_SQL, viewDefinition().getMatViewSql());
            assertViewRows("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t6.0
                    2024-01-01T02:00:00.000000Z\t6.0
                    """);
            assertSqlCursors(SUM_SQL, "v");
        });
    }

    // An IMMEDIATE period view of one hour: the base apply and the rows after ALTER ... AS land in the open period,
    // and the clock then ends the period, whose timer refresh runs before the view's WAL applies the definition event.
    // It defers before the view lock, and the woken incremental refresh computes the complete period with the new SQL.
    private void assertPeriodRefreshInWindowWaitsForDefinition(String timestampType) throws Exception {
        assertMemoryLeak(() -> {
            final MatViewTimerJob timerJob = createPeriodView(timestampType);
            alterView(MAX_SQL);
            final long definitionSeqTxn = viewState().getPendingDefinitionSeqTxn();
            execute(BUCKET_0200_ROWS);
            applyBaseWal();
            drainMatViewQueue(engine);
            currentMicros = parseFloorPartialTimestamp("2024-01-01T03:00:00.000001Z");
            drainMatViewTimerQueue(timerJob);
            Assert.assertEquals("range_refresh v\n", queuedTasks());
            final AtomicInteger lockAttempts = new AtomicInteger();
            try (MatViewRefreshJob refreshJob = newRefreshJob()) {
                refreshJob.setOnBeforeRefreshLockForTesting(lockAttempts::incrementAndGet);
                drainMatViewQueue(refreshJob);
            }
            Assert.assertEquals(0, lockAttempts.get());
            Assert.assertEquals(definitionSeqTxn, viewSeqTxn());
            Assert.assertTrue(viewState().hasDeferredDefinitionRefreshForTesting());
            drainWalQueue();
            Assert.assertEquals("incremental_refresh v\n", queuedTasks());
            drainWalAndMatViewQueues();
            assertPeriodViewRows(timestampType);
        });
    }

    // As above, but ALTER ... AS and the rows after it land once the period refresh holds the view lock and before
    // its base reader opens: the period refresh defers at its check after the reader opened.
    private void assertPeriodRefreshLateCheckWaitsForDefinition(String timestampType) throws Exception {
        assertMemoryLeak(() -> {
            final MatViewTimerJob timerJob = createPeriodView(timestampType);
            currentMicros = parseFloorPartialTimestamp("2024-01-01T03:00:00.000001Z");
            drainMatViewTimerQueue(timerJob);
            Assert.assertEquals("range_refresh v\n", queuedTasks());
            final AtomicLong definitionSeqTxn = new AtomicLong(-1);
            try (MatViewRefreshJob refreshJob = newRefreshJob()) {
                refreshJob.setOnHoldingLockForTesting(once(() -> {
                    alterView(MAX_SQL);
                    definitionSeqTxn.set(viewState().getPendingDefinitionSeqTxn());
                    execute(BUCKET_0200_ROWS);
                    applyBaseWal();
                }));
                drainMatViewQueue(refreshJob);
            }
            Assert.assertTrue(definitionSeqTxn.get() > -1);
            Assert.assertEquals(definitionSeqTxn.get(), viewSeqTxn());
            Assert.assertTrue(viewState().hasDeferredDefinitionRefreshForTesting());
            drainWalQueue();
            Assert.assertEquals("incremental_refresh v\n", queuedTasks());
            drainWalAndMatViewQueues();
            assertPeriodViewRows(timestampType);
        });
    }

    private void assertPeriodViewRows(String timestampType) throws Exception {
        final String fraction = "TIMESTAMP_NS".equals(timestampType) ? ".000000000Z" : ".000000Z";
        assertViewRows("ts\ts\n2024-01-01T00:00:00" + fraction + "\t1.0\n2024-01-01T02:00:00" + fraction + "\t3.0\n");
        assertSqlCursors(MAX_SQL, "v");
    }

    private void assertRangeError(String error) throws Exception {
        assertQuery("SELECT view_status, last_refresh_error FROM materialized_views() WHERE view_name = 'v'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("view_status\tlast_refresh_error\nvalid\t" + error + '\n');
        Assert.assertEquals(error, viewState().getLastRefreshError());
    }

    private void assertViewRows(String expected) throws Exception {
        assertQuery("v").timestamp("ts").noLeakCheck().expectSize().returns(expected);
    }

    private void assertViewStatus(String status) throws Exception {
        assertQuery("SELECT view_status FROM materialized_views() WHERE view_name = 'v'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("view_status\n" + status + '\n');
    }

    // The IMMEDIATE view v of SUM_SQL over a base with one row at 00:10, built.
    private void createImmediateView() throws Exception {
        createBase("TIMESTAMP");
        execute("INSERT INTO base VALUES ('2024-01-01T00:10:00.000000Z', 1.0)");
        execute("CREATE MATERIALIZED VIEW v AS (" + SUM_SQL + ") PARTITION BY DAY");
        drainWalAndMatViewQueues();
        assertViewRows("""
                ts\ts
                2024-01-01T00:00:00.000000Z\t1.0
                """);
    }

    // The IMMEDIATE view v of SUM_SQL over a base with the rows 1.0 and 5.0 in the 00:00 bucket, built.
    private void createImmediateViewWithTwoRowBucket() throws Exception {
        createBase("TIMESTAMP");
        execute("INSERT INTO base VALUES ('2024-01-01T00:10:00.000000Z', 1.0), ('2024-01-01T00:20:00.000000Z', 5.0)");
        execute("CREATE MATERIALIZED VIEW v AS (" + SUM_SQL + ") PARTITION BY DAY");
        drainWalAndMatViewQueues();
        assertViewRows("""
                ts\ts
                2024-01-01T00:00:00.000000Z\t6.0
                """);
    }

    // The MANUAL view v of SUM_SQL over a base with one row at 00:10, built.
    private void createManualView() throws Exception {
        createBase("TIMESTAMP");
        execute("INSERT INTO base VALUES ('2024-01-01T00:10:00.000000Z', 1.0)");
        execute("CREATE MATERIALIZED VIEW v REFRESH MANUAL AS (" + SUM_SQL + ") PARTITION BY DAY");
        drainWalQueue();
        execute("REFRESH MATERIALIZED VIEW v INCREMENTAL");
        drainWalAndMatViewQueues();
        assertViewRows("""
                ts\ts
                2024-01-01T00:00:00.000000Z\t1.0
                """);
    }

    // The MANUAL view v of SUM_SQL over a base with the rows 1.0 and 5.0 in the 00:00 bucket, built.
    private void createManualViewWithTwoRowBucket() throws Exception {
        createBase("TIMESTAMP");
        execute("INSERT INTO base VALUES ('2024-01-01T00:10:00.000000Z', 1.0), ('2024-01-01T00:20:00.000000Z', 5.0)");
        execute("CREATE MATERIALIZED VIEW v REFRESH MANUAL AS (" + SUM_SQL + ") PARTITION BY DAY");
        drainWalQueue();
        execute("REFRESH MATERIALIZED VIEW v INCREMENTAL");
        drainWalAndMatViewQueues();
        assertViewRows("""
                ts\ts
                2024-01-01T00:00:00.000000Z\t6.0
                """);
    }

    // The IMMEDIATE period view v of one hour periods over a base of the timestamp type with one row at 00:10, built at
    // 02:40, when the periods up to 02:00 are complete; returns the timer job that fires its period timer.
    private MatViewTimerJob createPeriodView(String timestampType) throws Exception {
        currentMicros = parseFloorPartialTimestamp("2024-01-01T02:40:00.000000Z");
        createBase(timestampType);
        execute("INSERT INTO base VALUES ('2024-01-01T00:10:00.000000Z', 1.0)");
        execute("CREATE MATERIALIZED VIEW v REFRESH IMMEDIATE PERIOD (LENGTH 1h) AS (" + SUM_SQL + ") PARTITION BY DAY");
        final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
        drainWalQueue();
        drainMatViewTimerQueue(timerJob);
        drainWalAndMatViewQueues();
        final String fraction = "TIMESTAMP_NS".equals(timestampType) ? ".000000000Z" : ".000000Z";
        assertViewRows("ts\ts\n2024-01-01T00:00:00" + fraction + "\t1.0\n");
        return timerJob;
    }

    @FunctionalInterface
    private interface SeamAction {
        void run() throws Exception;
    }

    // Counts the view's WAL writers taken from the pool from its creation on, and runs the action, when given, as the
    // first one is taken.
    private static final class WalWriterHook implements PoolListener, Closeable {
        private final Runnable action;
        private final AtomicInteger count = new AtomicInteger();
        private final CairoEngine engine;
        private final PoolListener previousListener;
        private final String viewDirName;

        private WalWriterHook(CairoEngine engine, TableToken viewToken, Runnable action) {
            this.engine = engine;
            this.viewDirName = viewToken.getDirName();
            this.action = action;
            this.previousListener = engine.getPoolListener();
            engine.setPoolListener(this);
        }

        @Override
        public void close() {
            engine.setPoolListener(previousListener);
        }

        @Override
        public void onEvent(byte factoryType, long thread, TableToken tableToken, short event, short segment, short position) {
            if (previousListener != null) {
                previousListener.onEvent(factoryType, thread, tableToken, event, segment, position);
            }
            if (factoryType != PoolListener.SRC_WAL_WRITER || tableToken == null || !viewDirName.equals(tableToken.getDirName())) {
                return;
            }
            if (event == PoolListener.EV_CREATE || event == PoolListener.EV_GET) {
                if (count.incrementAndGet() == 1 && action != null) {
                    action.run();
                }
            }
        }

        int getCount() {
            return count.get();
        }
    }
}
