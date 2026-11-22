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
import io.questdb.TelemetryJob;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.mv.MatViewBacklog;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.mv.MatViewRefreshTask;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.mv.MatViewStateReader;
import io.questdb.cairo.mv.MatViewStateStore;
import io.questdb.cairo.mv.MatViewTimerJob;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.test.TestTimestampCounterFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.cairo.wal.WalUtils.EVENT_FILE_NAME;
import static io.questdb.cairo.wal.WalUtils.WAL_NAME_BASE;

/**
 * The backlog of an invalid materialized view: while a view is invalid, it keeps recording the time ranges of its base
 * table's changes in its refresh-interval cache, and {@code REFRESH MATERIALIZED VIEW v RESUME} catches up on them.
 * {@code RESUME SKIP BACKLOG} jumps over them instead, and is required once the backlog is lost.
 * <p>
 * Expected rows come from the base rows each test inserts: a bucket of the 1h view holds the last price of its
 * symbol's base rows within the hour. A view that caught up equals its SQL run on the base, which the tests check
 * with that SQL as the reference.
 */
public class MatViewBacklogTest extends AbstractCairoTest {
    // The rows of the view over the attachable base's second day, see createAttachableBaseWithView().
    private static final String ATTACHABLE_VIEW_ROWS = """
            ts\tsym\tprice
            2024-01-02T00:00:00.000000Z\tc\t3.0
            """;
    private static final String CAUGHT_UP_ROWS = """
            ts\tsym\tprice
            2024-01-01T00:00:00.000000Z\ta\t3.0
            2024-01-01T01:00:00.000000Z\tb\t2.0
            2024-01-02T00:00:00.000000Z\tc\t4.0
            """;
    // The rows of the period view of createRefreshedPeriodTimerView().
    private static final String PERIOD_VIEW_ROWS = """
            ts\tsym\tprice
            2024-01-01T00:00:00.000000Z\ta\t1.0
            2024-01-01T05:00:00.000000Z\tb\t2.0
            """;
    private static final String RESUME_SKIP_SQL = "REFRESH MATERIALIZED VIEW price_1h RESUME SKIP BACKLOG";
    private static final String RESUME_SQL = "REFRESH MATERIALIZED VIEW price_1h RESUME";
    private static final int VIEW_NAME_POSITION = "REFRESH MATERIALIZED VIEW ".length();
    private static final String VIEW_ROWS = """
            ts\tsym\tprice
            2024-01-01T00:00:00.000000Z\ta\t1.0
            2024-01-01T01:00:00.000000Z\tb\t2.0
            """;
    private static final String VIEW_SQL = "SELECT ts, sym, last(price) AS price FROM base SAMPLE BY 1h";

    @Test
    public void testAttachWhileInvalidJoinsBacklog() throws Exception {
        // The WAL scan does not see an ATTACH PARTITION, which commits as a SQL transaction; the WAL apply job queues
        // its range, and an invalid view takes it into its backlog, so the refresh after RESUME recomputes it.
        assertMemoryLeak(() -> {
            createBase();
            execute("""
                    INSERT INTO base VALUES
                    ('a', 1.0, 1.0, '2024-01-01T00:10:00.000000Z'),
                    ('c', 3.0, 1.0, '2024-01-02T00:10:00.000000Z')
                    """);
            drainWalQueue();
            execute("ALTER TABLE base DETACH PARTITION LIST '2024-01-01'");
            drainWalQueue();
            makeAttachable("2024-01-01");
            createView("IMMEDIATE");
            drainWalAndMatViewQueues();
            final String dayTwoRows = """
                    ts\tsym\tprice
                    2024-01-02T00:00:00.000000Z\tc\t3.0
                    """;
            assertViewRows(dayTwoRows);
            invalidate("test invalidation");

            // The range task brings the backlog up to the ATTACH and adds its range in one view state event.
            final long viewTxn = viewSeqTxn();
            execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-01'");
            drainWalAndMatViewQueues();
            Assert.assertEquals(viewTxn + 1, viewSeqTxn());
            assertViewStatus("invalid", "test invalidation");
            assertCache(baseSeqTxn(), "2024-01-01T00:00:00.000000Z", "2024-01-01T23:59:59.999999Z");
            assertPersistedCache(baseSeqTxn(), "2024-01-01T00:00:00.000000Z", "2024-01-01T23:59:59.999999Z");
            assertViewRows(dayTwoRows);

            assertResumeSucceeds(RESUME_SQL);
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-02T00:00:00.000000Z\tc\t3.0
                    """);
        });
    }

    @Test
    public void testBacklogRefreshHonoursPartitionClip() throws Exception {
        // The backlog holds a change to a base partition that the base drops before RESUME. The refresh after RESUME
        // clips the backlog to the base's partitions, so the view keeps its rows in the dropped range.
        assertMemoryLeak(() -> {
            createBase();
            execute("""
                    INSERT INTO base VALUES
                    ('a', 1.0, 1.0, '2024-01-01T00:10:00.000000Z'),
                    ('b', 2.0, 1.0, '2024-01-02T00:10:00.000000Z')
                    """);
            createView("IMMEDIATE");
            drainWalAndMatViewQueues();
            invalidate("test invalidation");
            insert("a", 3.0, "2024-01-01T00:20:00.000000Z");
            insert("c", 4.0, "2024-01-03T00:10:00.000000Z");
            drainWalAndMatViewQueues();
            updateBacklog();
            assertCache(
                    baseSeqTxn(),
                    "2024-01-01T00:20:00.000000Z", "2024-01-01T00:20:00.000000Z",
                    "2024-01-03T00:10:00.000000Z", "2024-01-03T00:10:00.000000Z"
            );
            execute("ALTER TABLE base DROP PARTITION LIST '2024-01-01'");
            drainWalAndMatViewQueues();

            assertResumeSucceeds(RESUME_SQL);
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-02T00:00:00.000000Z\tb\t2.0
                    2024-01-03T00:00:00.000000Z\tc\t4.0
                    """);
        });
    }

    @Test
    public void testBacklogRefreshHonoursRefreshLimit() throws Exception {
        // The refresh after RESUME is an incremental refresh, so REFRESH LIMIT keeps it away from buckets below its
        // boundary: two days before the base's newest row.
        assertMemoryLeak(() -> {
            currentMicros = parseFloorPartialTimestamp("2024-01-10T12:00:00.000000Z");
            createBase();
            execute("""
                    INSERT INTO base VALUES
                    ('a', 1.0, 1.0, '2024-01-01T00:10:00.000000Z'),
                    ('b', 2.0, 1.0, '2024-01-09T00:10:00.000000Z')
                    """);
            createView("IMMEDIATE");
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW price_1h SET REFRESH LIMIT 2 DAYS");
            drainWalAndMatViewQueues();
            invalidate("test invalidation");
            insert("a", 10.0, "2024-01-01T00:20:00.000000Z");
            insert("b", 20.0, "2024-01-09T00:20:00.000000Z");
            drainWalAndMatViewQueues();

            assertResumeSucceeds(RESUME_SQL);
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-09T00:00:00.000000Z\tb\t20.0
                    """);
        });
    }

    @Test
    public void testBacklogSurvivesCheckpointRestore() throws Exception {
        // CHECKPOINT CREATE copies an invalid view's state with its backlog; after a restore the view catches up.
        // CHECKPOINT CREATE needs sync(), which Windows does not have.
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("MANUAL");
            invalidate("test invalidation");
            insert("a", 3.0, "2024-01-01T00:20:00.000000Z");
            drainWalAndMatViewQueues();
            updateBacklog();
            insert("c", 4.0, "2024-01-02T00:10:00.000000Z");
            drainWalAndMatViewQueues();
            final int baseTableId = baseToken().getTableId();

            execute("CHECKPOINT CREATE");
            engine.clear();
            engine.closeNameRegistry();
            try (Path triggerPath = new Path()) {
                triggerPath.of(configuration.getDbRoot()).parent().concat(TableUtils.RESTORE_FROM_CHECKPOINT_TRIGGER_FILE_NAME);
                Assert.assertTrue(Files.touch(triggerPath.$()));
            }
            engine.checkpointRecover();
            engine.reloadTableNames();
            engine.getMetadataCache().onStartupAsyncHydrator();
            engine.buildViewGraphs();

            Assert.assertTrue(viewState().isInvalid());
            Assert.assertNull(viewState().getBacklogLostReason());
            Assert.assertEquals(baseTableId, viewState().getBaseTableId());
            assertCache(2, "2024-01-01T00:20:00.000000Z", "2024-01-01T00:20:00.000000Z");
            execute("CHECKPOINT RELEASE");

            assertResumeSucceeds(RESUME_SQL);
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows(CAUGHT_UP_ROWS);
        });
    }

    @Test
    public void testBacklogSurvivesRestart() throws Exception {
        // The backlog persists with the view's state: a restart keeps the cached part, and the base WAL from the cache
        // position holds the rest.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("MANUAL");
            invalidate("test invalidation");
            insert("a", 3.0, "2024-01-01T00:20:00.000000Z");
            drainWalAndMatViewQueues();
            updateBacklog();
            insert("c", 4.0, "2024-01-02T00:10:00.000000Z");
            drainWalAndMatViewQueues();

            restart();
            Assert.assertTrue(viewState().isInvalid());
            Assert.assertNull(viewState().getBacklogLostReason());
            Assert.assertEquals(baseToken().getTableId(), viewState().getBaseTableId());
            assertRefreshBaseTxn(1);
            assertCache(2, "2024-01-01T00:20:00.000000Z", "2024-01-01T00:20:00.000000Z");
            assertViewStatus("invalid", "test invalidation");

            assertResumeSucceeds(RESUME_SQL);
            assertRefreshBaseTxn(1);
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows(CAUGHT_UP_ROWS);
            assertRefreshBaseTxn(3);
            assertSqlCursors(VIEW_SQL, "price_1h");
        });
    }

    @Test
    public void testBacklogUpdateDefersWhileLatchBusy() throws Exception {
        // A backlog update that finds the view latch taken goes back on the queue, and runs once the holder lets go.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("MANUAL");
            invalidate("test invalidation");
            insertGapRows();
            final long viewSeqTxn = viewSeqTxn();
            Assert.assertTrue(viewState().tryLock());
            try (MatViewRefreshJob job = new MatViewRefreshJob(0, engine, 0)) {
                engine.getMatViewStateStore().enqueueUpdateRefreshIntervals(viewToken());
                job.run();
                assertCache(-1);
                Assert.assertEquals(viewSeqTxn, viewSeqTxn());
                viewState().unlock();
                drainMatViewQueue(job);
            }
            drainWalQueue();
            Assert.assertFalse(viewState().isLocked());
            assertCache(
                    3,
                    "2024-01-01T00:20:00.000000Z", "2024-01-01T00:20:00.000000Z",
                    "2024-01-02T00:10:00.000000Z", "2024-01-02T00:10:00.000000Z"
            );
            Assert.assertEquals(viewSeqTxn + 1, viewSeqTxn());
            assertViewStatus("invalid", "test invalidation");
        });
    }

    @Test
    public void testBacklogUpdateFailsToOpenViewWal() throws Exception {
        // A backlog update that cannot open the view's WAL writer loses the backlog and keeps the view's invalidation
        // reason; without a writer the marker lives in memory.
        final AtomicBoolean isFailArmed = new AtomicBoolean();
        assertMemoryLeak(newViewWalOpenFailingFacade(isFailArmed), () -> {
            createBaseWithRefreshedView("MANUAL");
            invalidate("test invalidation");
            insertGapRows();
            final long viewSeqTxn = viewSeqTxn();
            engine.releaseInactive();
            isFailArmed.set(true);
            updateBacklog();
            Assert.assertFalse(isFailArmed.get());
            Assert.assertFalse(viewState().isLocked());
            assertViewStatus("invalid", "test invalidation");
            Assert.assertEquals(MatViewBacklog.LOST_UPDATE_FAILED, viewState().getBacklogLostReason());
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            assertLostThenSkipBacklogJumps(MatViewBacklog.LOST_UPDATE_FAILED, VIEW_ROWS);
        });
    }

    @Test
    public void testBacklogUpdateIgnoresSuspendedViewError() throws Exception {
        // A view WAL suspended while a backlog update holds the latch fails the update with a "table suspended"
        // error: the update writes nothing and keeps the backlog, as for a view suspended before the update.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("MANUAL");
            invalidate("test invalidation");
            insertGapRows();
            final long viewSeqTxn = viewSeqTxn();
            final AtomicBoolean isArmed = new AtomicBoolean(true);
            try (MatViewRefreshJob job = new MatViewRefreshJob(0, engine, 0)) {
                job.setOnHoldingLockForTesting(() -> {
                    if (isArmed.compareAndSet(true, false)) {
                        throw CairoException.tableSuspended(viewToken());
                    }
                });
                engine.getMatViewStateStore().enqueueUpdateRefreshIntervals(viewToken());
                drainMatViewQueue(job);
            }
            drainWalQueue();
            Assert.assertFalse(isArmed.get());
            Assert.assertFalse(viewState().isLocked());
            Assert.assertNull(viewState().getBacklogLostReason());
            assertCache(-1);
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            assertViewStatus("invalid", "test invalidation");

            // the next update brings the backlog up to date
            updateBacklog();
            assertCache(
                    3,
                    "2024-01-01T00:20:00.000000Z", "2024-01-01T00:20:00.000000Z",
                    "2024-01-02T00:10:00.000000Z", "2024-01-02T00:10:00.000000Z"
            );
        });
    }

    @Test
    public void testBacklogUpdateSkipsBacklogLostBeforeLock() throws Exception {
        // A RESUME that finds the backlog lost between the backlog update's checks and its latch leaves the update
        // nothing to do: the lost backlog takes no cache update.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("MANUAL");
            invalidate("test invalidation");
            execute("DROP TABLE base");
            createBase();
            insert("y", 8.0, "2024-01-01T05:10:00.000000Z");
            drainWalAndMatViewQueues();
            final long viewSeqTxn = viewSeqTxn();
            final AtomicBoolean isArmed = new AtomicBoolean(true);
            try (MatViewRefreshJob job = new MatViewRefreshJob(0, engine, 0)) {
                job.setOnBeforeRefreshLockForTesting(() -> {
                    if (isArmed.compareAndSet(true, false)) {
                        try {
                            assertResumeRefused(MatViewBacklog.LOST_BASE_TABLE_REPLACED);
                        } catch (Exception e) {
                            throw new AssertionError(e);
                        }
                    }
                });
                engine.getMatViewStateStore().enqueueUpdateRefreshIntervals(viewToken());
                drainMatViewQueue(job);
            }
            drainWalQueue();
            Assert.assertFalse(isArmed.get());
            Assert.assertFalse(viewState().isLocked());
            Assert.assertEquals(MatViewBacklog.LOST_BASE_TABLE_REPLACED, viewState().getBacklogLostReason());
            // neither the refusal nor the update wrote a view state event
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            assertViewStatus("invalid", "test invalidation");
        });
    }

    @Test
    public void testBacklogUpdateSkipsViewDroppedBeforeLock() throws Exception {
        // A DROP MATERIALIZED VIEW that marks the view's state dropped between the backlog update's checks and its
        // latch leaves the update nothing to do: it writes no view state event and leaves the cache as it was.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("MANUAL");
            invalidate("test invalidation");
            insertGapRows();
            final TableToken viewToken = viewToken();
            final long viewSeqTxn = viewSeqTxn();
            final MatViewState state = viewState();
            final AtomicBoolean isArmed = new AtomicBoolean(true);
            try (MatViewRefreshJob job = new MatViewRefreshJob(0, engine, 0)) {
                job.setOnBeforeRefreshLockForTesting(() -> {
                    if (isArmed.compareAndSet(true, false)) {
                        state.markAsDropped();
                    }
                });
                engine.getMatViewStateStore().enqueueUpdateRefreshIntervals(viewToken);
                drainMatViewQueue(job);
            } finally {
                state.tryCloseIfDropped();
            }
            drainWalQueue();
            Assert.assertFalse(isArmed.get());
            Assert.assertFalse(state.isLocked());
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            Assert.assertEquals(-1, state.getRefreshIntervalsBaseTxn());
            Assert.assertEquals(0, state.getRefreshIntervals().size());
            Assert.assertNull(state.getBacklogLostReason());
            execute("DROP MATERIALIZED VIEW price_1h");
            drainWalAndMatViewQueues();
        });
    }

    @Test
    public void testBacklogUpdateSkipsViewResumedBeforeLock() throws Exception {
        // A RESUME that makes the view valid between the backlog update's checks and its latch leaves the update
        // nothing to do: it writes no view state event.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("MANUAL");
            invalidate("test invalidation");
            insertGapRows();
            final long viewSeqTxn = viewSeqTxn();
            final AtomicBoolean isArmed = new AtomicBoolean(true);
            try (MatViewRefreshJob job = new MatViewRefreshJob(0, engine, 0)) {
                job.setOnBeforeRefreshLockForTesting(() -> {
                    if (isArmed.compareAndSet(true, false)) {
                        try {
                            execute(RESUME_SKIP_SQL);
                        } catch (SqlException e) {
                            throw new AssertionError(e);
                        }
                    }
                });
                engine.getMatViewStateStore().enqueueUpdateRefreshIntervals(viewToken());
                drainMatViewQueue(job);
            }
            drainWalQueue();
            Assert.assertFalse(isArmed.get());
            Assert.assertFalse(viewState().isLocked());
            Assert.assertFalse(viewState().isInvalid());
            // only RESUME's view state event
            Assert.assertEquals(viewSeqTxn + 1, viewSeqTxn());
            assertCache(-1);
            assertRefreshBaseTxn(3);
            assertViewStatus("valid", "");
        });
    }

    @Test
    public void testBacklogWithFailingRowsInvalidatesAgain() throws Exception {
        // The backlog holds the row that made the refresh fail, so the refresh after RESUME fails on it again and
        // invalidates the view again, with its backlog. RESUME SKIP BACKLOG jumps over the row and recovers the view.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            execute("ALTER TABLE base ALTER COLUMN price TYPE VARCHAR");
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            execute("INSERT INTO base VALUES ('a', 'abc', 1.0, '2024-01-01T00:20:00.000000Z')");
            drainWalAndMatViewQueues();
            Assert.assertTrue(viewState().isInvalid());
            final String reason = viewState().getInvalidationReasonForReport();
            TestUtils.assertContains(reason, "inconvertible value: `abc` [VARCHAR -> DOUBLE] [from=price, to=price]");
            execute("INSERT INTO base VALUES ('c', '4.0', 1.0, '2024-01-02T00:10:00.000000Z')");
            drainWalAndMatViewQueues();

            assertResumeSucceeds(RESUME_SQL);
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", reason);
            Assert.assertNull(viewState().getBacklogLostReason());
            assertViewRows(VIEW_ROWS);

            assertResumeSucceeds(RESUME_SKIP_SQL);
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows(VIEW_ROWS);
            execute("INSERT INTO base VALUES ('d', '5.0', 1.0, '2024-01-03T00:10:00.000000Z')");
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-01T01:00:00.000000Z\tb\t2.0
                    2024-01-03T00:00:00.000000Z\td\t5.0
                    """);
        });
    }

    @Test
    public void testBaseWalPurgedUpToCachePositionImmediate() throws Exception {
        assertBaseWalPurgedUpToCachePosition("IMMEDIATE");
    }

    @Test
    public void testBaseWalPurgedUpToCachePositionManual() throws Exception {
        assertBaseWalPurgedUpToCachePosition("MANUAL");
    }

    @Test
    public void testBaseWalPurgedUpToCachePositionTimer() throws Exception {
        assertBaseWalPurgedUpToCachePosition("EVERY 1h");
    }

    @Test
    public void testCacheCapMergesLongBacklog() throws Exception {
        // The backlog keeps at most cairo.mat.view.max.refresh.intervals intervals: past the cap, the newest ones
        // merge into one, and the refresh after RESUME recomputes the merged interval.
        setProperty(PropertyKey.CAIRO_MAT_VIEW_MAX_REFRESH_INTERVALS, 2);
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("MANUAL");
            invalidate("test invalidation");
            insert("c", 3.0, "2024-01-02T00:10:00.000000Z");
            insert("d", 4.0, "2024-01-04T00:10:00.000000Z");
            insert("e", 5.0, "2024-01-06T00:10:00.000000Z");
            insert("f", 6.0, "2024-01-08T00:10:00.000000Z");
            drainWalAndMatViewQueues();
            updateBacklog();
            assertCache(
                    5,
                    "2024-01-02T00:10:00.000000Z", "2024-01-02T00:10:00.000000Z",
                    "2024-01-04T00:10:00.000000Z", "2024-01-08T00:10:00.000000Z"
            );

            assertResumeSucceeds(RESUME_SQL);
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-01T01:00:00.000000Z\tb\t2.0
                    2024-01-02T00:00:00.000000Z\tc\t3.0
                    2024-01-04T00:00:00.000000Z\td\t4.0
                    2024-01-06T00:00:00.000000Z\te\t5.0
                    2024-01-08T00:00:00.000000Z\tf\t6.0
                    """);
            assertSqlCursors(VIEW_SQL, "price_1h");
        });
    }

    @Test
    public void testCatchUpImmediateView() throws Exception {
        // An IMMEDIATE view runs the backlog refresh at once after RESUME.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            invalidate("test invalidation");
            insertGapRows();
            assertViewStatus("invalid", "test invalidation");
            assertViewRows(VIEW_ROWS);

            assertResumeSucceeds(RESUME_SQL);
            // catching up keeps the refresh position, and the refresh after RESUME moves it
            assertRefreshBaseTxn(1);
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows(CAUGHT_UP_ROWS);
            assertRefreshBaseTxn(3);
            assertSqlCursors(VIEW_SQL, "price_1h");
        });
    }

    @Test
    public void testCatchUpManualView() throws Exception {
        // A MANUAL view runs the backlog refresh at its next REFRESH ... INCREMENTAL.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("MANUAL");
            invalidate("test invalidation");
            insertGapRows();
            updateBacklog();
            assertCache(
                    3,
                    "2024-01-01T00:20:00.000000Z", "2024-01-01T00:20:00.000000Z",
                    "2024-01-02T00:10:00.000000Z", "2024-01-02T00:10:00.000000Z"
            );

            assertResumeSucceeds(RESUME_SQL);
            assertRefreshBaseTxn(1);
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows(VIEW_ROWS);
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewRows(CAUGHT_UP_ROWS);
            assertRefreshBaseTxn(3);
            assertCache(-1);
        });
    }

    @Test
    public void testCatchUpPeriodView() throws Exception {
        // RESUME keeps a period view's last refreshed period, so the refresh after it recomputes the backlog and
        // the periods that completed while the view was invalid.
        assertMemoryLeak(() -> {
            currentMicros = parseFloorPartialTimestamp("2024-01-01T00:00:00.000000Z");
            createBase();
            execute("CREATE MATERIALIZED VIEW price_1h REFRESH IMMEDIATE PERIOD (LENGTH 1d) AS (" + VIEW_SQL + ") PARTITION BY DAY");
            insertBaseRows();
            currentMicros = parseFloorPartialTimestamp("2024-01-02T01:00:00.000000Z");
            drainWalAndMatViewQueues();
            assertViewRows(VIEW_ROWS);
            Assert.assertEquals(parseFloorPartialTimestamp("2024-01-02T00:00:00.000000Z"), viewState().getLastPeriodHi());
            invalidate("test invalidation");
            insert("a", 3.0, "2024-01-01T00:20:00.000000Z");
            insert("c", 4.0, "2024-01-02T10:10:00.000000Z");
            drainWalAndMatViewQueues();

            currentMicros = parseFloorPartialTimestamp("2024-01-04T12:00:00.000000Z");
            assertResumeSucceeds(RESUME_SQL);
            Assert.assertEquals(parseFloorPartialTimestamp("2024-01-02T00:00:00.000000Z"), viewState().getLastPeriodHi());
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t3.0
                    2024-01-01T01:00:00.000000Z\tb\t2.0
                    2024-01-02T10:00:00.000000Z\tc\t4.0
                    """);
            Assert.assertEquals(parseFloorPartialTimestamp("2024-01-04T00:00:00.000000Z"), viewState().getLastPeriodHi());
        });
    }

    @Test
    public void testCatchUpTimerView() throws Exception {
        // A TIMER view runs the backlog refresh at its next timer firing. While the view is invalid, its refresh
        // intervals update timer keeps the backlog.
        setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_INTERVALS_UPDATE_PERIOD, "1m");
        assertMemoryLeak(() -> {
            currentMicros = parseFloorPartialTimestamp("2024-02-01T00:00:00.000000Z");
            createBase();
            insertBaseRows();
            createView("EVERY 1h");
            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
            drainWalQueue();
            drainMatViewTimerQueue(timerJob);
            drainWalAndMatViewQueues();
            assertViewRows(VIEW_ROWS);
            invalidate("test invalidation");
            insertGapRows();

            currentMicros += Micros.MINUTE_MICROS;
            drainMatViewTimerQueue(timerJob);
            drainWalAndMatViewQueues();
            assertCache(
                    3,
                    "2024-01-01T00:20:00.000000Z", "2024-01-01T00:20:00.000000Z",
                    "2024-01-02T00:10:00.000000Z", "2024-01-02T00:10:00.000000Z"
            );
            assertViewStatus("invalid", "test invalidation");

            assertResumeSucceeds(RESUME_SQL);
            drainWalAndMatViewQueues();
            assertViewRows(VIEW_ROWS);
            currentMicros += Micros.HOUR_MICROS;
            drainMatViewTimerQueue(timerJob);
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows(CAUGHT_UP_ROWS);
            assertRefreshBaseTxn(3);
        });
    }

    @Test
    public void testConfiguredWalApplySuspensionReleasesFloor() throws Exception {
        // A view listed in cairo.wal.apply.suspended.tables does not apply its WAL, so it cannot persist its backlog:
        // it takes no backlog update and releases the base's WAL floor. Once the list no longer holds it, RESUME finds
        // the WAL its backlog needs purged and refuses, and RESUME SKIP BACKLOG jumps.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("MANUAL");
            invalidate("test invalidation");
            final long viewSeqTxn = viewSeqTxn();
            setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_TABLES, viewToken().getDirName());
            try {
                engine.releaseInactive();
                insertGapRows();
                updateBacklog();
                assertCache(-1);
                Assert.assertNull(viewState().getBacklogLostReason());
                Assert.assertEquals(viewSeqTxn, viewSeqTxn());
                purgeBaseWal();
                // the view's cache position is base txn 1, and the base's WAL above it is purged
                Assert.assertFalse(isBaseWalSegmentPresent(2));
                Assert.assertFalse(isBaseWalSegmentPresent(3));
            } finally {
                setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_TABLES, null);
            }
            assertViewStatus("invalid", "test invalidation");
            assertLostThenSkipBacklogJumps(MatViewBacklog.LOST_BASE_TABLE_WAL_MISSING, VIEW_ROWS);
        });
    }

    @Test
    public void testDroppedViewTracksNoBacklog() throws Exception {
        // A view that DROP MATERIALIZED VIEW is removing tracks no backlog. A base commit does not move the refresh
        // intervals sequence of an invalid view whose state the drop marked dropped or removed, and a backlog update
        // queued before the drop finds nothing to do in either case. The state store hands out a dropped state once
        // and forgets it then, so each case starts from a freshly invalidated view.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            final AtomicBoolean isLockAttempted = new AtomicBoolean();
            try (MatViewRefreshJob job = new MatViewRefreshJob(0, engine, 0)) {
                job.setOnBeforeRefreshLockForTesting(() -> isLockAttempted.set(true));

                // base commits find the view's state dropped, then removed
                MatViewState state = createRefreshedInvalidView();
                final long refreshIntervalsSeq = state.getRefreshIntervalsSeq();
                long viewSeqTxn = viewSeqTxn();
                state.markAsDropped();
                try {
                    for (int i = 0; i < 2; i++) {
                        insert("a", 3.0 + i, "2024-01-01T00:20:00.000000Z");
                        drainWalQueue();
                        drainMatViewQueue(job);
                        Assert.assertNull(engine.getMatViewStateStore().getViewState(viewToken()));
                    }
                } finally {
                    state.tryCloseIfDropped();
                }
                Assert.assertEquals(refreshIntervalsSeq, state.getRefreshIntervalsSeq());
                Assert.assertEquals(viewSeqTxn, viewSeqTxn());
                dropView();

                // a backlog update finds the view's state dropped
                state = createRefreshedInvalidView();
                viewSeqTxn = viewSeqTxn();
                engine.getMatViewStateStore().enqueueUpdateRefreshIntervals(viewToken());
                state.markAsDropped();
                try {
                    drainMatViewQueue(job);
                } finally {
                    state.tryCloseIfDropped();
                }
                drainWalQueue();
                Assert.assertEquals(viewSeqTxn, viewSeqTxn());
                Assert.assertEquals(-1, state.getRefreshIntervalsBaseTxn());
                Assert.assertEquals(0, state.getRefreshIntervals().size());
                dropView();

                // a backlog update finds the view's state removed
                createRefreshedInvalidView();
                final TableToken viewToken = viewToken();
                engine.getMatViewStateStore().enqueueUpdateRefreshIntervals(viewToken);
                execute("DROP MATERIALIZED VIEW price_1h");
                Assert.assertNull(engine.getMatViewStateStore().getViewState(viewToken));
                drainMatViewQueue(job);
                drainWalAndMatViewQueues();
            }
            Assert.assertFalse(isLockAttempted.get());
        });
    }

    @Test
    public void testFullClearsLostMarker() throws Exception {
        // REFRESH ... FULL rebuilds the view and clears the backlog-lost marker: the next invalidation starts a
        // tracked backlog again, which plain RESUME catches up on.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            invalidate("test invalidation");
            insert("a", 3.0, "2024-01-01T00:20:00.000000Z");
            drainWalAndMatViewQueues();
            removeBaseWalEventFiles();
            assertResumeRefused(MatViewBacklog.LOST_BASE_TABLE_WAL_MISSING);
            Assert.assertEquals(MatViewBacklog.LOST_BASE_TABLE_WAL_MISSING, viewState().getBacklogLostReason());

            execute("REFRESH MATERIALIZED VIEW price_1h FULL");
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            Assert.assertNull(viewState().getBacklogLostReason());
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t3.0
                    2024-01-01T01:00:00.000000Z\tb\t2.0
                    """);
            restart();
            Assert.assertNull(viewState().getBacklogLostReason());

            invalidate("second invalidation");
            Assert.assertNull(viewState().getBacklogLostReason());
            insert("c", 4.0, "2024-01-02T00:10:00.000000Z");
            drainWalAndMatViewQueues();
            assertResumeSucceeds(RESUME_SQL);
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows(CAUGHT_UP_ROWS);
        });
    }

    @Test
    public void testIncrementalCheckpointMarksBacklogLostOnMissingWal() throws Exception {
        // When an incremental checkpoint cannot load the base changes an invalid view's backlog misses, it marks the
        // backlog lost in the checkpointed state; the view's live state is left to its own cache update.
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("MANUAL");
            invalidate("test invalidation");
            insert("a", 3.0, "2024-01-01T00:20:00.000000Z");
            drainWalAndMatViewQueues();
            removeBaseWalEventFiles();

            engine.checkpointCreate(sqlExecutionContext.getCircuitBreaker(), true);
            try {
                final MatViewStateReader stateReader = readCheckpointedState();
                Assert.assertTrue(stateReader.isInvalid());
                TestUtils.assertEquals("test invalidation", stateReader.getInvalidationReason());
                TestUtils.assertEquals(MatViewBacklog.LOST_BASE_TABLE_WAL_MISSING, stateReader.getBacklogLostReason());
                Assert.assertEquals(1, stateReader.getLastRefreshBaseTxn());
                Assert.assertEquals(baseToken().getTableId(), stateReader.getBaseTableId());
            } finally {
                execute("CHECKPOINT RELEASE");
            }
            Assert.assertNull(viewState().getBacklogLostReason());
        });
    }

    @Test
    public void testIncrementalCheckpointMarksBacklogLostOnReplacedBase() throws Exception {
        // An invalid view whose base table was replaced has no backlog to merge the new table's changes into: the
        // incremental checkpoint marks its backlog lost in the checkpointed state.
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("MANUAL");
            invalidate("test invalidation");
            execute("DROP TABLE base");
            createBase();
            insert("y", 8.0, "2024-01-01T00:20:00.000000Z");
            drainWalAndMatViewQueues();

            engine.checkpointCreate(sqlExecutionContext.getCircuitBreaker(), true);
            try {
                final MatViewStateReader stateReader = readCheckpointedState();
                Assert.assertTrue(stateReader.isInvalid());
                TestUtils.assertEquals(MatViewBacklog.LOST_BASE_TABLE_REPLACED, stateReader.getBacklogLostReason());
                Assert.assertEquals(0, stateReader.getRefreshIntervals().size());
            } finally {
                execute("CHECKPOINT RELEASE");
            }
        });
    }

    @Test
    public void testIncrementalCheckpointMergesBacklogOfInvalidView() throws Exception {
        // An incremental checkpoint does not copy the base's WAL, so it merges the base changes of the gap into the
        // checkpointed state of an invalid view as it does for a valid one, keeping the view's invalid flag.
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("MANUAL");
            invalidate("test invalidation");
            insertGapRows();
            final long baseSeqTxn = baseSeqTxn();

            engine.checkpointCreate(sqlExecutionContext.getCircuitBreaker(), true);
            try {
                final MatViewStateReader stateReader = readCheckpointedState();
                Assert.assertTrue(stateReader.isInvalid());
                TestUtils.assertEquals("test invalidation", stateReader.getInvalidationReason());
                Assert.assertNull(stateReader.getBacklogLostReason());
                Assert.assertEquals(1, stateReader.getLastRefreshBaseTxn());
                Assert.assertEquals(baseSeqTxn, stateReader.getRefreshIntervalsBaseTxn());
                final LongList expected = new LongList();
                expected.add(parseFloorPartialTimestamp("2024-01-01T00:20:00.000000Z"), parseFloorPartialTimestamp("2024-01-01T00:20:00.000000Z"));
                expected.add(parseFloorPartialTimestamp("2024-01-02T00:10:00.000000Z"), parseFloorPartialTimestamp("2024-01-02T00:10:00.000000Z"));
                TestUtils.assertEquals(expected, stateReader.getRefreshIntervals());
            } finally {
                execute("CHECKPOINT RELEASE");
            }
        });
    }

    @Test
    public void testIncrementalCheckpointKeepsValidViewOnMissingWal() throws Exception {
        // A valid view whose gap the incremental checkpoint cannot load keeps its checkpointed state as is: it has no
        // backlog to lose, and its first refresh after the restore recomputes the gap.
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("MANUAL");
            insertGapRows();
            removeBaseWalEventFiles();

            engine.checkpointCreate(sqlExecutionContext.getCircuitBreaker(), true);
            try {
                final MatViewStateReader stateReader = readCheckpointedState();
                Assert.assertFalse(stateReader.isInvalid());
                Assert.assertNull(stateReader.getBacklogLostReason());
                Assert.assertEquals(1, stateReader.getLastRefreshBaseTxn());
                Assert.assertEquals(-1, stateReader.getRefreshIntervalsBaseTxn());
                Assert.assertEquals(0, stateReader.getRefreshIntervals().size());
            } finally {
                execute("CHECKPOINT RELEASE");
            }
        });
    }

    @Test
    public void testIncrementalCheckpointSkipsInvalidViewWithoutBaseTableId() throws Exception {
        // An invalid view whose state carries no base table id, as for one invalidated before this version, tracks
        // no backlog: the incremental checkpoint copies its state as is.
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("MANUAL");
            try (WalWriter walWriter = engine.getWalWriter(viewToken())) {
                // the view state event of a previous version: no base table id and no backlog-lost marker
                walWriter.resetMatViewState(1, 0, true, "legacy invalidation", Numbers.LONG_NULL, null, -1);
            }
            drainWalQueue();
            restart();
            insertGapRows();

            engine.checkpointCreate(sqlExecutionContext.getCircuitBreaker(), true);
            try {
                final MatViewStateReader stateReader = readCheckpointedState();
                Assert.assertTrue(stateReader.isInvalid());
                Assert.assertEquals(MatViewState.BASE_TABLE_ID_UNKNOWN, stateReader.getBaseTableId());
                Assert.assertNull(stateReader.getBacklogLostReason());
                Assert.assertEquals(-1, stateReader.getRefreshIntervalsBaseTxn());
                Assert.assertEquals(0, stateReader.getRefreshIntervals().size());
            } finally {
                execute("CHECKPOINT RELEASE");
            }
        });
    }

    @Test
    public void testIncrementalCheckpointSkipsLostBacklog() throws Exception {
        // An invalid view whose backlog is lost tracks nothing: the incremental checkpoint copies its state as is.
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("MANUAL");
            invalidate("test invalidation");
            insert("a", 3.0, "2024-01-01T00:20:00.000000Z");
            drainWalAndMatViewQueues();
            removeBaseWalEventFiles();
            updateBacklog();
            Assert.assertEquals(MatViewBacklog.LOST_BASE_TABLE_WAL_MISSING, viewState().getBacklogLostReason());
            insert("c", 4.0, "2024-01-02T00:10:00.000000Z");
            drainWalAndMatViewQueues();

            engine.checkpointCreate(sqlExecutionContext.getCircuitBreaker(), true);
            try {
                final MatViewStateReader stateReader = readCheckpointedState();
                TestUtils.assertEquals(MatViewBacklog.LOST_BASE_TABLE_WAL_MISSING, stateReader.getBacklogLostReason());
                Assert.assertEquals(-1, stateReader.getRefreshIntervalsBaseTxn());
                Assert.assertEquals(0, stateReader.getRefreshIntervals().size());
            } finally {
                execute("CHECKPOINT RELEASE");
            }
        });
    }

    @Test
    public void testIncrementalCheckpointSkipsNeverRefreshedAndCaughtUpViews() throws Exception {
        // An incremental checkpoint copies as they are the state of an invalid view that never completed a refresh,
        // which tracks no backlog since its first refresh after the restore builds it, and the state of an invalid
        // view whose backlog already covers the base's last transaction. Neither reads the base's WAL: the second
        // checkpoint runs with its event files gone.
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("MANUAL");
            execute("CREATE MATERIALIZED VIEW price_deferred REFRESH MANUAL DEFERRED AS (" + VIEW_SQL + ") PARTITION BY DAY");
            drainWalQueue();
            final TableToken deferredToken = engine.verifyTableName("price_deferred");
            // RESUME gives the never-refreshed view a base table id, which its second invalidation keeps
            engine.getMatViewStateStore().enqueueInvalidate(deferredToken, "test invalidation");
            drainWalAndMatViewQueues();
            execute("REFRESH MATERIALIZED VIEW price_deferred RESUME");
            drainWalQueue();
            engine.getMatViewStateStore().enqueueInvalidate(deferredToken, "test invalidation");
            drainWalAndMatViewQueues();
            final MatViewState deferredLiveState = engine.getMatViewStateStore().getViewState(deferredToken);
            Assert.assertTrue(deferredLiveState.isInvalid());
            Assert.assertEquals(-1, deferredLiveState.getLastRefreshBaseTxn());
            Assert.assertEquals(baseToken().getTableId(), deferredLiveState.getBaseTableId());
            Assert.assertFalse(deferredLiveState.hasTrackedBacklog());
            invalidate("test invalidation");
            insertGapRows();
            updateBacklog();
            final LongList backlog = new LongList();
            backlog.add(parseFloorPartialTimestamp("2024-01-01T00:20:00.000000Z"), parseFloorPartialTimestamp("2024-01-01T00:20:00.000000Z"));
            backlog.add(parseFloorPartialTimestamp("2024-01-02T00:10:00.000000Z"), parseFloorPartialTimestamp("2024-01-02T00:10:00.000000Z"));
            TestUtils.assertEquals(backlog, viewState().getRefreshIntervals());
            Assert.assertEquals(3, viewState().getRefreshIntervalsBaseTxn());

            for (boolean isBaseWalRemoved : new boolean[]{false, true}) {
                if (isBaseWalRemoved) {
                    removeBaseWalEventFiles();
                }
                engine.checkpointCreate(sqlExecutionContext.getCircuitBreaker(), true);
                try {
                    final MatViewStateReader deferredState = readCheckpointedState(deferredToken);
                    Assert.assertTrue(deferredState.isInvalid());
                    Assert.assertNull(deferredState.getBacklogLostReason());
                    Assert.assertEquals(baseToken().getTableId(), deferredState.getBaseTableId());
                    Assert.assertEquals(-1, deferredState.getLastRefreshBaseTxn());
                    Assert.assertEquals(-1, deferredState.getRefreshIntervalsBaseTxn());
                    Assert.assertEquals(0, deferredState.getRefreshIntervals().size());

                    final MatViewStateReader viewState = readCheckpointedState();
                    Assert.assertTrue(viewState.isInvalid());
                    Assert.assertNull(viewState.getBacklogLostReason());
                    Assert.assertEquals(1, viewState.getLastRefreshBaseTxn());
                    Assert.assertEquals(3, viewState.getRefreshIntervalsBaseTxn());
                    TestUtils.assertEquals(backlog, viewState.getRefreshIntervals());
                } finally {
                    execute("CHECKPOINT RELEASE");
                }
            }
        });
    }

    @Test
    public void testInvalidImmediateViewWritesAtMostOneTxnPerUpdatePeriod() throws Exception {
        // Performance property: an invalid IMMEDIATE view writes at most one view WAL transaction per refresh
        // intervals update period while its base keeps committing, and none while the base is idle.
        setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_INTERVALS_UPDATE_PERIOD, "10s");
        assertMemoryLeak(() -> {
            currentMicros = parseFloorPartialTimestamp("2024-02-01T00:00:00.000000Z");
            createBaseWithRefreshedView("IMMEDIATE");
            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
            drainMatViewTimerQueue(timerJob);
            Assert.assertEquals(1, viewState().getRegisteredTimerCount());
            invalidate("test invalidation");

            long viewSeqTxn = viewSeqTxn();
            for (int period = 0; period < 3; period++) {
                for (int commit = 0; commit < 3; commit++) {
                    insert("c", period * 10 + commit, "2024-01-0" + (period + 2) + "T0" + commit + ":10:00.000000Z");
                    drainWalAndMatViewQueues();
                    // the timer is not due within the period
                    drainMatViewTimerQueue(timerJob);
                    drainWalAndMatViewQueues();
                    Assert.assertEquals(viewSeqTxn, viewSeqTxn());
                }
                currentMicros += 10 * Micros.SECOND_MICROS;
                drainMatViewTimerQueue(timerJob);
                drainWalAndMatViewQueues();
                drainMatViewTimerQueue(timerJob);
                drainWalAndMatViewQueues();
                Assert.assertEquals(viewSeqTxn + 1, viewSeqTxn());
                viewSeqTxn++;
                Assert.assertEquals(baseSeqTxn(), viewState().getRefreshIntervalsBaseTxn());
            }
            for (int period = 0; period < 3; period++) {
                currentMicros += 10 * Micros.SECOND_MICROS;
                drainMatViewTimerQueue(timerJob);
                drainWalAndMatViewQueues();
                Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            }
            assertViewStatus("invalid", "test invalidation");
        });
    }

    @Test
    public void testLostBacklogReleasesFloor() throws Exception {
        // An invalid view whose backlog is lost no longer holds back the base's WAL purging.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            invalidate("test invalidation");
            insert("a", 3.0, "2024-01-01T00:20:00.000000Z");
            drainWalAndMatViewQueues();
            try (MatViewRefreshJob job = new MatViewRefreshJob(0, engine, 0)) {
                job.setOnHoldingLockForTesting(() -> {
                    throw CairoException.nonCritical().put("injected backlog update failure");
                });
                engine.getMatViewStateStore().enqueueUpdateRefreshIntervals(viewToken());
                drainMatViewQueue(job);
            }
            drainWalQueue();
            Assert.assertEquals(MatViewBacklog.LOST_UPDATE_FAILED, viewState().getBacklogLostReason());
            assertCache(-1);
            engine.releaseInactive();
            insert("c", 4.0, "2024-01-02T00:10:00.000000Z");
            drainWalAndMatViewQueues();
            purgeBaseWal();
            // the view's cache position is base txn 1, and the base's WAL above it is purged
            Assert.assertFalse(isBaseWalSegmentPresent(2));
            Assert.assertFalse(isBaseWalSegmentPresent(3));
            assertViewStatus("invalid", "test invalidation");
        });
    }

    @Test
    public void testLostMarkerStaysInMemoryWhenItsWriteFails() throws Exception {
        // A backlog update finds the backlog lost and cannot write the marker: the marker lives in memory, and after
        // a restart the next cache update finds the backlog lost again and persists it.
        setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 1);
        final AtomicBoolean isFailArmed = new AtomicBoolean();
        final StringSink failingPart = new StringSink();
        assertMemoryLeak(newSeqPartFailingFacade(isFailArmed, failingPart), () -> {
            createBaseWithRefreshedView("MANUAL");
            insertGapRows();
            removeBaseWalEventFiles();
            // the invalidation reopens the view's sequencer, which opens the part of a later transaction only when
            // that transaction comes
            invalidate("test invalidation");
            final long viewSeqTxn = viewSeqTxn();
            armViewStateEventFailure(isFailArmed, failingPart);
            updateBacklog();
            Assert.assertFalse(isFailArmed.get());
            Assert.assertFalse(viewState().isLocked());
            Assert.assertEquals(MatViewBacklog.LOST_BASE_TABLE_WAL_MISSING, viewState().getBacklogLostReason());
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            assertViewStatus("invalid", "test invalidation");

            restart();
            Assert.assertNull(viewState().getBacklogLostReason());
            updateBacklog();
            Assert.assertEquals(MatViewBacklog.LOST_BASE_TABLE_WAL_MISSING, viewState().getBacklogLostReason());
            Assert.assertEquals(viewSeqTxn + 1, viewSeqTxn());
            restart();
            assertLostThenSkipBacklogJumps(MatViewBacklog.LOST_BASE_TABLE_WAL_MISSING, VIEW_ROWS);
        });
    }

    @Test
    public void testLostWhenBaseDroppedWhileInvalid() throws Exception {
        // A cache update that runs while the base table is gone loses the backlog; a table created under the base's
        // name later does not bring it back.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("MANUAL");
            invalidate("test invalidation");
            insert("a", 3.0, "2024-01-01T00:20:00.000000Z");
            drainWalAndMatViewQueues();
            execute("DROP TABLE base");
            drainWalAndMatViewQueues();
            updateBacklog();
            Assert.assertEquals(MatViewBacklog.LOST_BASE_TABLE_REPLACED, viewState().getBacklogLostReason());
            createBase();
            insert("y", 8.0, "2024-01-01T05:10:00.000000Z");
            drainWalAndMatViewQueues();
            assertLostThenSkipBacklogJumps(MatViewBacklog.LOST_BASE_TABLE_REPLACED, """
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-01T01:00:00.000000Z\tb\t2.0
                    """);
        });
    }

    @Test
    public void testLostWhenBaseRebased() throws Exception {
        // REBASE WAL gives the base table a new table id: the view's backlog belongs to the old one.
        setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, "true");
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            final int oldBaseTableId = baseToken().getTableId();
            execute("ALTER TABLE base SUSPEND WAL");
            execute("ALTER TABLE base REBASE WAL");
            drainWalAndMatViewQueues();
            Assert.assertNotEquals(oldBaseTableId, baseToken().getTableId());
            Assert.assertTrue(viewState().isInvalid());
            assertLostThenSkipBacklogJumps(MatViewBacklog.LOST_BASE_TABLE_REPLACED, VIEW_ROWS);
        });
    }

    @Test
    public void testLostWhenBaseRecreated() throws Exception {
        // The base table is dropped and re-created before any cache update: RESUME's own cache update finds another
        // table id under the base's name.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            execute("DROP TABLE base");
            createBase();
            insert("y", 8.0, "2024-01-01T05:10:00.000000Z");
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "base table is dropped or renamed");
            Assert.assertNull(viewState().getBacklogLostReason());
            assertLostThenSkipBacklogJumps(MatViewBacklog.LOST_BASE_TABLE_REPLACED, VIEW_ROWS);
        });
    }

    @Test
    public void testLostWhenBaseRenamed() throws Exception {
        // The base table is renamed and a new table takes its name.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            execute("RENAME TABLE base TO base_old");
            createBase();
            insert("y", 8.0, "2024-01-01T05:10:00.000000Z");
            drainWalAndMatViewQueues();
            Assert.assertTrue(viewState().isInvalid());
            assertLostThenSkipBacklogJumps(MatViewBacklog.LOST_BASE_TABLE_REPLACED, VIEW_ROWS);
        });
    }

    @Test
    public void testLostWhenBaseReplacedByLiveView() throws Exception {
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        assertLostWhenBaseReplacedByUnusableTable(
                "CREATE LIVE VIEW base FLUSH EVERY 100ms START FROM NOW AS SELECT ts, sym, price, count(*) OVER"
                        + " (PARTITION BY sym ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM other",
                "DROP LIVE VIEW base"
        );
    }

    @Test
    public void testLostWhenBaseReplacedByNonWalTable() throws Exception {
        assertLostWhenBaseReplacedByUnusableTable(
                "CREATE TABLE base (sym SYMBOL, price DOUBLE, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL",
                "DROP TABLE base"
        );
    }

    @Test
    public void testLostWhenBaseReplacedByView() throws Exception {
        assertLostWhenBaseReplacedByUnusableTable("CREATE VIEW base AS (SELECT * FROM other)", "DROP VIEW base");
    }

    @Test
    public void testLostWhenBaseReplacedUnderValidView() throws Exception {
        // The base table is replaced while the view is valid and the drop's invalidation is lost, as in a crash. The
        // refresh that finds another table id under the base's name invalidates the view with its backlog lost.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            execute("DROP TABLE base");
            dropQueuedInvalidation();
            createBase();
            insert("y", 8.0, "2024-01-01T05:10:00.000000Z");
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", MatViewRefreshJob.BASE_TABLE_CHANGED_REASON);
            Assert.assertEquals(MatViewBacklog.LOST_BASE_TABLE_REPLACED, viewState().getBacklogLostReason());
            // the invalidation persisted the marker
            restart();
            Assert.assertEquals(MatViewBacklog.LOST_BASE_TABLE_REPLACED, viewState().getBacklogLostReason());
            assertLostThenSkipBacklogJumps(MatViewBacklog.LOST_BASE_TABLE_REPLACED, VIEW_ROWS);
        });
    }

    @Test
    public void testLostWhenBaseWalMissing() throws Exception {
        // The base table's WAL from the cache position is gone, so the backlog cannot be read: RESUME refuses
        // instead of recomputing the base's whole min..max range.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            invalidate("test invalidation");
            insertGapRows();
            removeBaseWalEventFiles();
            assertLostThenSkipBacklogJumps(MatViewBacklog.LOST_BASE_TABLE_WAL_MISSING, VIEW_ROWS);
        });
    }

    @Test
    public void testLostWhenCacheUpdateFails() throws Exception {
        // A cache update that fails loses the backlog and keeps the view's invalidation reason; the marker persists.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            invalidate("test invalidation");
            insert("a", 3.0, "2024-01-01T00:20:00.000000Z");
            drainWalAndMatViewQueues();
            try (MatViewRefreshJob job = new MatViewRefreshJob(0, engine, 0)) {
                job.setOnHoldingLockForTesting(() -> {
                    throw CairoException.nonCritical().put("injected backlog update failure");
                });
                engine.getMatViewStateStore().enqueueUpdateRefreshIntervals(viewToken());
                drainMatViewQueue(job);
            }
            drainWalQueue();
            Assert.assertFalse(viewState().isLocked());
            assertViewStatus("invalid", "test invalidation");
            Assert.assertEquals(MatViewBacklog.LOST_UPDATE_FAILED, viewState().getBacklogLostReason());
            restart();
            assertViewStatus("invalid", "test invalidation");
            Assert.assertEquals(MatViewBacklog.LOST_UPDATE_FAILED, viewState().getBacklogLostReason());
            assertLostThenSkipBacklogJumps(MatViewBacklog.LOST_UPDATE_FAILED, VIEW_ROWS);
        });
    }

    @Test
    public void testLostWhenCacheUpdateOfResumeFails() throws Exception {
        // RESUME's own cache update fails to persist the backlog: the backlog is lost, RESUME refuses with the cause
        // and keeps the view's invalidation reason, and RESUME SKIP BACKLOG then jumps.
        setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 1);
        final AtomicBoolean isFailArmed = new AtomicBoolean();
        final StringSink failingPart = new StringSink();
        assertMemoryLeak(newSeqPartFailingFacade(isFailArmed, failingPart), () -> {
            createBaseWithRefreshedView("IMMEDIATE");
            invalidate("test invalidation");
            insertGapRows();
            final long viewSeqTxn = viewSeqTxn();
            armViewStateEventFailure(isFailArmed, failingPart);
            assertResumeRefused(MatViewBacklog.LOST_UPDATE_FAILED);
            Assert.assertFalse(isFailArmed.get());
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            assertViewStatus("invalid", "test invalidation");
            assertLostThenSkipBacklogJumps(MatViewBacklog.LOST_UPDATE_FAILED, VIEW_ROWS);
        });
    }

    @Test
    public void testLostWhenInvalidatedBeforeThisVersion() throws Exception {
        // The state of a view invalidated before this version carries no base table id, so the view's backlog
        // counts as lost.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            try (WalWriter walWriter = engine.getWalWriter(viewToken())) {
                // the view state event of a previous version: no base table id and no backlog-lost marker
                walWriter.resetMatViewState(1, 0, true, "legacy invalidation", Numbers.LONG_NULL, null, -1);
            }
            drainWalQueue();
            restart();
            assertViewStatus("invalid", "legacy invalidation");
            Assert.assertEquals(MatViewState.BASE_TABLE_ID_UNKNOWN, viewState().getBaseTableId());
            assertLostThenSkipBacklogJumps(MatViewBacklog.LOST_NO_BASE_TABLE_ID, VIEW_ROWS);
            Assert.assertEquals(baseToken().getTableId(), viewState().getBaseTableId());
        });
    }

    @Test
    public void testLostWhenViewAheadOfBase() throws Exception {
        // A view found ahead of its base table on load goes invalid, and its backlog belongs to transactions the base
        // does not have.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            try (WalWriter walWriter = engine.getWalWriter(viewToken())) {
                walWriter.resetMatViewState(baseSeqTxn() + 5, 0, false, null, Numbers.LONG_NULL, null, -1, baseToken().getTableId(), null);
            }
            drainWalQueue();
            restart();
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "materialized view is ahead of base table and cannot be synchronized");
            assertLostThenSkipBacklogJumps(MatViewBacklog.LOST_VIEW_AHEAD_OF_BASE_TABLE, VIEW_ROWS);
            assertRefreshBaseTxn(1);
        });
    }

    @Test
    public void testNeverRefreshedViewResumesWithoutSkipBacklog() throws Exception {
        // A view that never completed a refresh has no backlog: plain RESUME does not refuse it, and its next refresh
        // builds it.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            execute("CREATE MATERIALIZED VIEW price_1h REFRESH MANUAL DEFERRED AS (" + VIEW_SQL + ") PARTITION BY DAY");
            drainWalAndMatViewQueues();
            invalidate("test invalidation");
            // SKIP BACKLOG has nothing to skip either: the view keeps its "never refreshed" position
            assertResumeSucceeds(RESUME_SKIP_SQL);
            assertRefreshBaseTxn(-1);
            invalidate("test invalidation");
            insert("c", 4.0, "2024-01-02T00:10:00.000000Z");
            drainWalAndMatViewQueues();
            assertResumeSucceeds(RESUME_SQL);
            assertRefreshBaseTxn(-1);
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-01T01:00:00.000000Z\tb\t2.0
                    2024-01-02T00:00:00.000000Z\tc\t4.0
                    """);
        });
    }

    @Test
    public void testPendingInvalidationHoldsFloor() throws Exception {
        // A view with an invalidation pending is about to go invalid with a backlog that starts at its cache position,
        // so it keeps holding the base's WAL floor there, and RESUME then catches up on the WAL it held.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("MANUAL");
            engine.releaseInactive();
            insertGapRows();
            viewState().markAsPendingInvalidation("pending invalidation");
            Assert.assertFalse(viewState().isInvalid());
            purgeBaseWal();
            Assert.assertFalse(isBaseWalSegmentPresent(1));
            Assert.assertTrue(isBaseWalSegmentPresent(2));
            Assert.assertTrue(isBaseWalSegmentPresent(3));

            // a lock holder's release wakes the pending invalidation
            Assert.assertTrue(viewState().tryLock());
            MatViewRefreshJob.finalizeAndUnlock(engine, engine.getMatViewStateStore(), viewToken(), viewState(), false);
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "pending invalidation");
            Assert.assertNull(viewState().getBacklogLostReason());
            purgeBaseWal();
            Assert.assertTrue(isBaseWalSegmentPresent(2));

            assertResumeSucceeds(RESUME_SQL);
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows(CAUGHT_UP_ROWS);
        });
    }

    @Test
    public void testPeriodRefreshOverEmptyPeriodKeepsCachePositionAfterBasePurge() throws Exception {
        // A TIMER period view's refresh intervals update scans a base transaction that changes no rows, an UPDATE of a
        // day the base holds no partition of, which queues no range either: the cache stays empty, and its position
        // moves to that transaction, up to which the base's WAL is then purged. The period timer's refresh of
        // 2024-01-02, a day the base holds no partition of, keeps the position, so the next incremental refresh reads
        // no purged WAL.
        assertMemoryLeak(() -> {
            createRefreshedPeriodTimerView(VIEW_SQL);
            currentMicros = parseFloorPartialTimestamp("2024-01-02T23:45:00.000000Z");
            execute("UPDATE base SET price = price * 10 WHERE ts IN '2024-01-05'");
            drainWalAndMatViewQueues();
            updateBacklog();
            assertCache(2);
            assertPersistedCache(2);
            purgeBaseWal();
            Assert.assertFalse(isBaseWalSegmentPresent(2));

            runPeriodRefresh("2024-01-03T00:00:00.000000Z");
            Assert.assertEquals(parseFloorPartialTimestamp("2024-01-03T00:00:00.000000Z"), viewState().getLastPeriodHi());
            assertRefreshBaseTxn(1);
            assertCache(2);
            assertPersistedCache(2);

            runIncrementalRefresh("2024-01-03T00:30:00.000000Z");
            assertViewStatus("valid", "");
            assertViewRows(PERIOD_VIEW_ROWS);
            assertRefreshBaseTxn(2);
        });
    }

    @Test
    public void testPeriodRefreshOverEmptyPeriodKeepsCachedWalIntervalsAfterBasePurge() throws Exception {
        // A TIMER period view's refresh intervals update caches the interval of a late row into 2024-01-01, and the
        // base's WAL up to the cache position is then purged. The period timer's refresh of 2024-01-02, a day the
        // base holds no partition of, keeps the cache, so the next incremental refresh recomputes the late row's
        // bucket from the cache without the purged WAL.
        assertMemoryLeak(() -> {
            createRefreshedPeriodTimerView(VIEW_SQL);
            currentMicros = parseFloorPartialTimestamp("2024-01-02T23:45:00.000000Z");
            insert("a", 5.0, "2024-01-01T00:20:00.000000Z");
            drainWalAndMatViewQueues();
            updateBacklog();
            assertCache(2, "2024-01-01T00:20:00.000000Z", "2024-01-01T00:20:00.000000Z");
            purgeBaseWal();
            Assert.assertFalse(isBaseWalSegmentPresent(2));

            runPeriodRefresh("2024-01-03T00:00:00.000000Z");
            assertCache(2, "2024-01-01T00:20:00.000000Z", "2024-01-01T00:20:00.000000Z");
            assertPersistedCache(2, "2024-01-01T00:20:00.000000Z", "2024-01-01T00:20:00.000000Z");

            runIncrementalRefresh("2024-01-03T00:30:00.000000Z");
            assertViewStatus("valid", "");
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t5.0
                    2024-01-01T05:00:00.000000Z\tb\t2.0
                    """);
            assertSqlCursors(VIEW_SQL, "price_1h");
        });
    }

    @Test
    public void testPeriodRefreshOverEmptyPeriodKeepsResumedBacklog() throws Exception {
        // A TIMER period view goes invalid, and its backlog takes a row written while it is invalid and the range of
        // a base UPDATE of 2024-01-01. RESUME keeps the backlog in the cache for the next incremental refresh, and so
        // does the period timer's refresh of 2024-01-02, a day the base holds no partition of. The incremental
        // refresh after it catches up on both.
        assertMemoryLeak(() -> {
            createRefreshedPeriodTimerView(VIEW_SQL);
            invalidate("test invalidation");
            insert("c", 3.0, "2024-01-01T03:10:00.000000Z");
            drainWalAndMatViewQueues();
            updateBacklog();
            assertCache(2, "2024-01-01T03:10:00.000000Z", "2024-01-01T03:10:00.000000Z");
            execute("UPDATE base SET price = price * 10 WHERE ts IN '2024-01-01'");
            drainWalAndMatViewQueues();
            assertCache(3, "2024-01-01T00:00:00.000000Z", "2024-01-01T23:59:59.999999Z");

            currentMicros = parseFloorPartialTimestamp("2024-01-02T23:45:00.000000Z");
            assertResumeSucceeds(RESUME_SQL);
            drainWalAndMatViewQueues();
            assertRefreshBaseTxn(1);
            assertCache(3, "2024-01-01T00:00:00.000000Z", "2024-01-01T23:59:59.999999Z");
            assertViewRows(PERIOD_VIEW_ROWS);

            runPeriodRefresh("2024-01-03T00:00:00.000000Z");
            assertCache(3, "2024-01-01T00:00:00.000000Z", "2024-01-01T23:59:59.999999Z");
            assertPersistedCache(3, "2024-01-01T00:00:00.000000Z", "2024-01-01T23:59:59.999999Z");

            runIncrementalRefresh("2024-01-03T00:30:00.000000Z");
            assertViewStatus("valid", "");
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t10.0
                    2024-01-01T03:00:00.000000Z\tc\t30.0
                    2024-01-01T05:00:00.000000Z\tb\t20.0
                    """);
            assertSqlCursors(VIEW_SQL, "price_1h");
            assertRefreshBaseTxn(3);
            assertCache(-1);
        });
    }

    @Test
    public void testPeriodRefreshOverEmptyPeriodRecomputesOnlyCachedBucketAfterBasePurge() throws Exception {
        // Performance property, with missing WAL files not fatal, the server default: the period timer's refresh of
        // 2024-01-02, a day the base holds no partition of, keeps the cached interval of a late row whose WAL is
        // purged, so the next incremental refresh recomputes the one bucket of the late row, not every bucket of the
        // base, as a refresh that cannot read the WAL from its refresh position would. In dev mode,
        // test_timestamp_counter() counts the view rows the refresh computes, one per bucket here.
        setProperty(PropertyKey.DEBUG_MAT_VIEW_REFRESH_MISSING_WAL_FILES_FATAL, "false");
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        assertMemoryLeak(() -> {
            final String viewSql = "SELECT ts, test_timestamp_counter(ts) ts0, sym, last(price) AS price FROM base SAMPLE BY 1h";
            currentMicros = parseFloorPartialTimestamp("2024-01-01T00:00:00.000000Z");
            createBase();
            execute("CREATE MATERIALIZED VIEW price_1h REFRESH EVERY 1h PERIOD (LENGTH 1d) AS (" + viewSql + ") PARTITION BY DAY");
            // seven buckets, six of them in 2024-01-01
            execute("""
                    INSERT INTO base VALUES
                    ('z', 0.5, 1.0, '2023-12-31T10:10:00.000000Z'),
                    ('a', 1.0, 1.0, '2024-01-01T00:10:00.000000Z'),
                    ('b', 2.0, 1.0, '2024-01-01T01:10:00.000000Z'),
                    ('c', 3.0, 1.0, '2024-01-01T02:10:00.000000Z'),
                    ('d', 4.0, 1.0, '2024-01-01T03:10:00.000000Z'),
                    ('e', 5.0, 1.0, '2024-01-01T04:10:00.000000Z'),
                    ('f', 6.0, 1.0, '2024-01-01T05:10:00.000000Z')
                    """);
            drainWalQueue();
            runIncrementalRefresh("2024-01-02T12:00:00.000000Z");
            assertRefreshBaseTxn(1);

            currentMicros = parseFloorPartialTimestamp("2024-01-02T23:45:00.000000Z");
            insert("a", 5.0, "2024-01-01T00:20:00.000000Z");
            drainWalAndMatViewQueues();
            updateBacklog();
            assertCache(2, "2024-01-01T00:20:00.000000Z", "2024-01-01T00:20:00.000000Z");
            purgeBaseWal();
            Assert.assertFalse(isBaseWalSegmentPresent(2));
            runPeriodRefresh("2024-01-03T00:00:00.000000Z");

            final long counterBefore = TestTimestampCounterFactory.COUNTER.get();
            runIncrementalRefresh("2024-01-03T00:30:00.000000Z");
            Assert.assertEquals(1, TestTimestampCounterFactory.COUNTER.get() - counterBefore);
            assertViewStatus("valid", "");
            assertSqlCursors(viewSql, "price_1h");
        });
    }

    @Test
    public void testRangeTaskEventFailureAfterFailedScanLosesBacklog() throws Exception {
        // A valid view's ATTACH range task cannot read the base's WAL. With missing WAL files not fatal, as in
        // production, the scan clears the cache, the task merges the range into it at the refresh position, below the
        // ATTACH, and then the view state event fails. The view goes invalid in memory, and the backlog it starts with
        // is lost: the scan cleared whatever the view had cached before. The clear itself is a known defect of its own,
        // "a transient base WAL read failure drops the UPDATE/ATTACH ranges cached by a valid materialized view"; the
        // test asserts only that the task's failure then loses the backlog.
        setProperty(PropertyKey.DEBUG_MAT_VIEW_REFRESH_MISSING_WAL_FILES_FATAL, "false");
        setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 1);
        final AtomicBoolean isScanFailArmed = new AtomicBoolean();
        final AtomicBoolean isEventFailArmed = new AtomicBoolean();
        final StringSink failingPart = new StringSink();
        assertMemoryLeak(newBaseWalReadAndSeqPartFailingFacade(isScanFailArmed, isEventFailArmed, failingPart), () -> {
            createAttachableBaseWithView();
            insert("c", 4.0, "2024-01-02T02:20:00.000000Z");
            execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-01'");
            drainWalQueue();
            final long viewSeqTxn = viewSeqTxn();
            armViewStateEventFailure(isEventFailArmed, failingPart);
            isScanFailArmed.set(true);
            drainMatViewQueue(engine);
            drainWalQueue();
            Assert.assertFalse(isScanFailArmed.get());
            Assert.assertFalse(isEventFailArmed.get());
            Assert.assertFalse(viewState().isLocked());
            assertCache(2, "2024-01-01T00:00:00.000000Z", "2024-01-01T23:59:59.999999Z");
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            assertFailedRangeTaskLostThenRecovered("could not open read-write", """
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-02T00:00:00.000000Z\tc\t3.0
                    2024-01-02T01:00:00.000000Z\tc\t5.0
                    2024-01-02T02:00:00.000000Z\tc\t4.0
                    """);
        });
    }

    @Test
    public void testRangeTaskEventFailureAfterMergeKeepsBacklog() throws Exception {
        // The view state event of a valid view's ATTACH range task fails after the task's scan brought the cache up to
        // the ATTACH and merged the range into it. The view goes invalid in memory and keeps its backlog tracked, since
        // the cache holds the range: RESUME catches up on it, right away, or after the next base commit's backlog
        // update persisted the backlog and a restart.
        setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 1);
        final AtomicBoolean isFailArmed = new AtomicBoolean();
        final StringSink failingPart = new StringSink();
        assertMemoryLeak(newSeqPartFailingFacade(isFailArmed, failingPart), () -> {
            for (boolean isBacklogUpdated : new boolean[]{false, true}) {
                createAttachableBaseWithView();
                execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-01'");
                drainWalQueue();
                final long viewSeqTxn = viewSeqTxn();
                armViewStateEventFailure(isFailArmed, failingPart);
                drainMatViewQueue(engine);
                drainWalQueue();
                Assert.assertFalse(isFailArmed.get());
                Assert.assertFalse(viewState().isLocked());
                Assert.assertTrue(viewState().isInvalid());
                final String reason = viewState().getInvalidationReasonForReport();
                TestUtils.assertContains(reason, "could not open read-write");
                Assert.assertNull(viewState().getBacklogLostReason());
                Assert.assertTrue(viewState().hasTrackedBacklog());
                assertCache(3, "2024-01-01T00:00:00.000000Z", "2024-01-01T23:59:59.999999Z");
                Assert.assertEquals(viewSeqTxn, viewSeqTxn());

                String viewRows = """
                        ts\tsym\tprice
                        2024-01-01T00:00:00.000000Z\ta\t1.0
                        2024-01-02T00:00:00.000000Z\tc\t3.0
                        """;
                if (isBacklogUpdated) {
                    insert("c", 5.0, "2024-01-02T01:20:00.000000Z");
                    drainWalAndMatViewQueues();
                    updateBacklog();
                    Assert.assertEquals(viewSeqTxn + 1, viewSeqTxn());
                    assertViewStatus("invalid", reason);
                    final String[] backlog = {
                            "2024-01-01T00:00:00.000000Z", "2024-01-01T23:59:59.999999Z",
                            "2024-01-02T01:20:00.000000Z", "2024-01-02T01:20:00.000000Z"
                    };
                    assertPersistedCache(4, backlog);
                    restart();
                    Assert.assertTrue(viewState().isInvalid());
                    Assert.assertNull(viewState().getBacklogLostReason());
                    assertCache(4, backlog);
                    viewRows = """
                            ts\tsym\tprice
                            2024-01-01T00:00:00.000000Z\ta\t1.0
                            2024-01-02T00:00:00.000000Z\tc\t3.0
                            2024-01-02T01:00:00.000000Z\tc\t5.0
                            """;
                }

                assertResumeSucceeds(RESUME_SQL);
                execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
                drainWalAndMatViewQueues();
                assertViewStatus("valid", "");
                assertViewRows(viewRows);
                assertSqlCursors(VIEW_SQL, "price_1h");
                execute("DROP MATERIALIZED VIEW price_1h");
                execute("DROP TABLE base");
                drainWalAndMatViewQueues();
            }
        });
    }

    @Test
    public void testRangeTaskEventFailureWhileInvalidationPendingKeepsBacklog() throws Exception {
        // The view state event of a valid view's ATTACH range task fails while an invalidation is pending on the view,
        // after the task's scan brought the cache up to the ATTACH and merged the range into it. The view goes invalid
        // in memory with the error and keeps its backlog tracked, since the cache holds the range; the queued
        // invalidation meets an invalid view and writes nothing, and RESUME catches up on the attached day.
        setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 1);
        final AtomicBoolean isFailArmed = new AtomicBoolean();
        final StringSink failingPart = new StringSink();
        assertMemoryLeak(newSeqPartFailingFacade(isFailArmed, failingPart), () -> {
            createAttachableBaseWithView();
            execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-01'");
            drainWalQueue();
            publishInvalidation("pending invalidation");
            final long viewSeqTxn = viewSeqTxn();
            armViewStateEventFailure(isFailArmed, failingPart);
            drainMatViewQueue(engine);
            drainWalQueue();
            Assert.assertFalse(isFailArmed.get());
            Assert.assertFalse(viewState().isLocked());
            Assert.assertTrue(viewState().isInvalid());
            TestUtils.assertContains(viewState().getInvalidationReasonForReport(), "could not open read-write");
            Assert.assertNull(viewState().getBacklogLostReason());
            Assert.assertTrue(viewState().hasTrackedBacklog());
            assertCache(3, "2024-01-01T00:00:00.000000Z", "2024-01-01T23:59:59.999999Z");
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());

            assertResumeSucceeds(RESUME_SQL);
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-02T00:00:00.000000Z\tc\t3.0
                    """);
            assertSqlCursors(VIEW_SQL, "price_1h");
        });
    }

    @Test
    public void testRangeTaskEventFailureWithoutScanKeepsBacklog() throws Exception {
        // The view's refresh runs before the ATTACH's range task, which the refresh job takes in either order: the
        // task finds the cache up to the ATTACH and merges the range without a scan, and then the view state event
        // fails. The view goes invalid in memory and keeps its backlog tracked, since the cache holds the range, and
        // RESUME catches up on it.
        setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 1);
        final AtomicBoolean isFailArmed = new AtomicBoolean();
        final StringSink failingPart = new StringSink();
        assertMemoryLeak(newSeqPartFailingFacade(isFailArmed, failingPart), () -> {
            for (String refreshType : new String[]{"MANUAL", "IMMEDIATE"}) {
                createAttachableBaseWithView(refreshType);
                execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-01'");
                drainWalQueue();
                final MatViewRefreshTask rangeTask = takeRangeTask();
                if ("MANUAL".equals(refreshType)) {
                    execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
                }
                drainWalAndMatViewQueues();
                // the refresh moved the refresh position to the ATTACH, which its WAL scan skips
                assertRefreshBaseTxn(3);
                assertCache(-1);
                assertViewRows(ATTACHABLE_VIEW_ROWS);

                final long viewSeqTxn = viewSeqTxn();
                armViewStateEventFailure(isFailArmed, failingPart);
                engine.getMatViewStateStore().reenqueueRefreshTask(rangeTask);
                drainMatViewQueue(engine);
                drainWalQueue();
                Assert.assertFalse(isFailArmed.get());
                Assert.assertFalse(viewState().isLocked());
                Assert.assertTrue(viewState().isInvalid());
                TestUtils.assertContains(viewState().getInvalidationReasonForReport(), "could not open read-write");
                Assert.assertNull(viewState().getBacklogLostReason());
                assertCache(3, "2024-01-01T00:00:00.000000Z", "2024-01-01T23:59:59.999999Z");
                Assert.assertEquals(viewSeqTxn, viewSeqTxn());

                assertResumeSucceeds(RESUME_SQL);
                execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
                drainWalAndMatViewQueues();
                assertViewStatus("valid", "");
                assertViewRows("""
                        ts\tsym\tprice
                        2024-01-01T00:00:00.000000Z\ta\t1.0
                        2024-01-02T00:00:00.000000Z\tc\t3.0
                        """);
                assertSqlCursors(VIEW_SQL, "price_1h");
                execute("DROP MATERIALIZED VIEW price_1h");
                execute("DROP TABLE base");
                drainWalAndMatViewQueues();
            }
        });
    }

    @Test
    public void testRangeTaskFailureInvalidatesValidViewAndLosesBacklog() throws Exception {
        // A failure while a valid view takes an ATTACH range invalidates the view, with the error, through
        // refreshFailState(); the failed task has no view WAL writer to persist the invalidation with. The view's
        // cache becomes the backlog it starts with, and the cache lacks the range, which the base's WAL scan skips:
        // the backlog is lost, so RESUME refuses instead of returning the view valid without the attached day.
        assertMemoryLeak(() -> {
            createAttachableBaseWithView();
            final long viewSeqTxn = viewSeqTxn();
            failRangeTask(CairoException.nonCritical().put("injected range task failure"));
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            assertCache(-1);
            assertFailedRangeTaskLostThenRecovered("injected range task failure", """
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-02T00:00:00.000000Z\tc\t3.0
                    2024-01-02T01:00:00.000000Z\tc\t5.0
                    """);
        });
    }

    @Test
    public void testRangeTaskFailureKeepsBaseTableReplacedCause() throws Exception {
        // The base table is replaced while the view is valid and the drop's invalidation is lost, as in a crash. The
        // ATTACH range task of the new base finds another table id under the base's name, which loses the backlog,
        // and fails to write the view's invalidation. The view goes invalid in memory and keeps the precise cause.
        setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 1);
        final AtomicBoolean isFailArmed = new AtomicBoolean();
        final StringSink failingPart = new StringSink();
        assertMemoryLeak(newSeqPartFailingFacade(isFailArmed, failingPart), () -> {
            createBaseWithRefreshedView("MANUAL");
            execute("DROP TABLE base");
            dropQueuedInvalidation();
            createAttachableBase();
            final long viewSeqTxn = viewSeqTxn();
            armViewStateEventFailure(isFailArmed, failingPart);
            execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-01'");
            drainWalAndMatViewQueues();
            Assert.assertFalse(isFailArmed.get());
            Assert.assertFalse(viewState().isLocked());
            Assert.assertTrue(viewState().isInvalid());
            TestUtils.assertContains(viewState().getInvalidationReasonForReport(), "could not open read-write");
            Assert.assertEquals(MatViewBacklog.LOST_BASE_TABLE_REPLACED, viewState().getBacklogLostReason());
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());

            assertResumeRefused(MatViewBacklog.LOST_BASE_TABLE_REPLACED);
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            assertResumeSucceeds(RESUME_SKIP_SQL);
            assertRefreshBaseTxn(3);
            Assert.assertEquals(baseToken().getTableId(), viewState().getBaseTableId());
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows(VIEW_ROWS);
        });
    }

    @Test
    public void testRangeTaskFailureKeepsNoBaseTableIdCause() throws Exception {
        // A valid view whose state was last written by a previous version, without a base table id, tracks no backlog.
        // Its ATTACH range task fails to open the view's WAL writer: the view goes invalid in memory, and RESUME
        // refuses with the cause the view's state gives, not with the task's failure.
        final AtomicBoolean isFailArmed = new AtomicBoolean();
        assertMemoryLeak(newViewWalOpenFailingFacade(isFailArmed), () -> {
            createAttachableBaseWithView();
            try (WalWriter walWriter = engine.getWalWriter(viewToken())) {
                // the view state event of a previous version: no base table id and no backlog-lost marker
                walWriter.resetMatViewState(2, 0, false, null, Numbers.LONG_NULL, null, -1);
            }
            drainWalQueue();
            restart();
            Assert.assertEquals(MatViewState.BASE_TABLE_ID_UNKNOWN, viewState().getBaseTableId());
            final long viewSeqTxn = viewSeqTxn();
            engine.releaseInactive();
            isFailArmed.set(true);
            execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-01'");
            drainWalAndMatViewQueues();
            Assert.assertFalse(isFailArmed.get());
            Assert.assertFalse(viewState().isLocked());
            Assert.assertTrue(viewState().isInvalid());
            TestUtils.assertContains(viewState().getInvalidationReasonForReport(), "could not open read-write");
            Assert.assertNull(viewState().getBacklogLostReason());
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());

            assertResumeRefused(MatViewBacklog.LOST_NO_BASE_TABLE_ID);
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            assertResumeSucceeds(RESUME_SKIP_SQL);
            Assert.assertEquals(baseToken().getTableId(), viewState().getBaseTableId());
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            execute("REFRESH MATERIALIZED VIEW price_1h RANGE FROM '2024-01-01' TO '2024-01-03'");
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-02T00:00:00.000000Z\tc\t3.0
                    """);
            assertSqlCursors(VIEW_SQL, "price_1h");
        });
    }

    @Test
    public void testRangeTaskFailureOfNeverRefreshedViewTracksNoBacklog() throws Exception {
        // A view that never completed a refresh tracks no backlog, and its first build covers the range: a failure of
        // its ATTACH range task invalidates it in memory without a lost backlog, and RESUME succeeds.
        final AtomicBoolean isFailArmed = new AtomicBoolean();
        assertMemoryLeak(newViewWalOpenFailingFacade(isFailArmed), () -> {
            createAttachableBase();
            createView("MANUAL DEFERRED");
            drainWalAndMatViewQueues();
            assertRefreshBaseTxn(-1);
            final long viewSeqTxn = viewSeqTxn();
            engine.releaseInactive();
            isFailArmed.set(true);
            execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-01'");
            drainWalAndMatViewQueues();
            Assert.assertFalse(isFailArmed.get());
            Assert.assertFalse(viewState().isLocked());
            Assert.assertTrue(viewState().isInvalid());
            TestUtils.assertContains(viewState().getInvalidationReasonForReport(), "could not open read-write");
            Assert.assertNull(viewState().getBacklogLostReason());
            Assert.assertFalse(viewState().hasTrackedBacklog());
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());

            assertResumeSucceeds(RESUME_SQL);
            assertRefreshBaseTxn(-1);
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-02T00:00:00.000000Z\tc\t3.0
                    """);
            assertSqlCursors(VIEW_SQL, "price_1h");
        });
    }

    @Test
    public void testRangeTaskFailureReleasesFloorAndRestartRecomputes() throws Exception {
        // A valid view's backlog that a failed ATTACH range task lost no longer holds back the base's WAL purging. A
        // restart loads the view valid from its state file, and when the base's WAL from the view's refresh position
        // was purged by then, the view's next refresh recomputes the base's whole range, attached day included.
        setProperty(PropertyKey.DEBUG_MAT_VIEW_REFRESH_MISSING_WAL_FILES_FATAL, "false");
        final AtomicBoolean isFailArmed = new AtomicBoolean();
        assertMemoryLeak(newViewWalOpenFailingFacade(isFailArmed), () -> {
            createAttachableBaseWithView();
            engine.releaseInactive();
            isFailArmed.set(true);
            execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-01'");
            drainWalAndMatViewQueues();
            Assert.assertFalse(isFailArmed.get());
            Assert.assertTrue(viewState().isInvalid());
            Assert.assertEquals(MatViewBacklog.LOST_UPDATE_FAILED, viewState().getBacklogLostReason());
            for (int hour = 1; hour < 4; hour++) {
                // each commit goes through a WAL writer of its own, whose segment the purge can remove
                engine.releaseInactive();
                insert("c", 4.0 + hour, "2024-01-02T0" + hour + ":20:00.000000Z");
                drainWalAndMatViewQueues();
            }
            purgeBaseWal();
            // the ATTACH is base txn 3, and the inserts 4 to 6
            for (long baseTxn = 3; baseTxn < 7; baseTxn++) {
                Assert.assertFalse(isBaseWalSegmentPresent(baseTxn));
            }

            restart();
            assertViewStatus("valid", "");
            Assert.assertNull(viewState().getBacklogLostReason());
            assertRefreshBaseTxn(2);
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-02T00:00:00.000000Z\tc\t3.0
                    2024-01-02T01:00:00.000000Z\tc\t5.0
                    2024-01-02T02:00:00.000000Z\tc\t6.0
                    2024-01-02T03:00:00.000000Z\tc\t7.0
                    """);
            assertSqlCursors(VIEW_SQL, "price_1h");
        });
    }

    @Test
    public void testRangeTaskFailureLosesBacklog() throws Exception {
        // A failure while an invalid view takes an ATTACH range into its backlog loses the backlog and keeps the
        // view's invalidation reason; the marker persists.
        assertMemoryLeak(() -> {
            createAttachableBaseWithView();
            invalidate("test invalidation");
            final long viewSeqTxn = viewSeqTxn();
            failRangeTask(CairoException.nonCritical().put("injected range task failure"));
            assertViewStatus("invalid", "test invalidation");
            Assert.assertEquals(MatViewBacklog.LOST_UPDATE_FAILED, viewState().getBacklogLostReason());
            Assert.assertEquals(viewSeqTxn + 1, viewSeqTxn());
            restart();
            Assert.assertEquals(MatViewBacklog.LOST_UPDATE_FAILED, viewState().getBacklogLostReason());
            assertLostThenSkipBacklogJumps(MatViewBacklog.LOST_UPDATE_FAILED, ATTACHABLE_VIEW_ROWS);
        });
    }

    @Test
    public void testRangeTaskFailsToOpenViewWal() throws Exception {
        // An invalid view's ATTACH range task that cannot open the view's WAL writer loses the backlog and keeps the
        // view's invalidation reason; without a writer the marker lives in memory.
        final AtomicBoolean isFailArmed = new AtomicBoolean();
        assertMemoryLeak(newViewWalOpenFailingFacade(isFailArmed), () -> {
            createAttachableBaseWithView();
            invalidate("test invalidation");
            final long viewSeqTxn = viewSeqTxn();
            engine.releaseInactive();
            isFailArmed.set(true);
            execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-01'");
            drainWalAndMatViewQueues();
            Assert.assertFalse(isFailArmed.get());
            Assert.assertFalse(viewState().isLocked());
            assertViewStatus("invalid", "test invalidation");
            Assert.assertEquals(MatViewBacklog.LOST_UPDATE_FAILED, viewState().getBacklogLostReason());
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            Assert.assertEquals(0, viewState().getRefreshIntervals().size());
        });
    }

    @Test
    public void testRangeTaskFailsToOpenWalOfValidView() throws Exception {
        // A valid view's ATTACH or UPDATE range task that cannot open the view's WAL writer invalidates the view in
        // memory, and the backlog the view starts with lacks the range: the backlog is lost. An IMMEDIATE view's
        // incremental refresh, which the operation's commit queues after the range task, skips the invalid view, and
        // a later UPDATE adds nothing to the lost backlog.
        final AtomicBoolean isFailArmed = new AtomicBoolean();
        assertMemoryLeak(newViewWalOpenFailingFacade(isFailArmed), () -> {
            for (String refreshType : new String[]{"MANUAL", "IMMEDIATE"}) {
                for (boolean isAttach : new boolean[]{true, false}) {
                    createAttachableBaseWithView(refreshType);
                    final long viewSeqTxn = viewSeqTxn();
                    engine.releaseInactive();
                    isFailArmed.set(true);
                    execute(isAttach ? "ALTER TABLE base ATTACH PARTITION LIST '2024-01-01'" : "UPDATE base SET price = 30.0 WHERE sym = 'c'");
                    drainWalAndMatViewQueues();
                    Assert.assertFalse(isFailArmed.get());
                    Assert.assertFalse(viewState().isLocked());
                    Assert.assertTrue(viewState().isInvalid());
                    Assert.assertEquals(MatViewBacklog.LOST_UPDATE_FAILED, viewState().getBacklogLostReason());
                    Assert.assertEquals(viewSeqTxn, viewSeqTxn());
                    assertCache(-1);
                    assertViewRows(ATTACHABLE_VIEW_ROWS);

                    execute("UPDATE base SET price = 40.0 WHERE sym = 'c'");
                    drainWalAndMatViewQueues();
                    Assert.assertEquals(viewSeqTxn, viewSeqTxn());
                    Assert.assertEquals(MatViewBacklog.LOST_UPDATE_FAILED, viewState().getBacklogLostReason());
                    assertCache(-1);

                    assertFailedRangeTaskLostThenRecovered(
                            "could not open read-write",
                            "ts\tsym\tprice\n"
                                    + (isAttach ? "2024-01-01T00:00:00.000000Z\ta\t1.0\n" : "")
                                    + "2024-01-02T00:00:00.000000Z\tc\t40.0\n"
                                    + "2024-01-02T01:00:00.000000Z\tc\t5.0\n"
                    );
                    execute("DROP MATERIALIZED VIEW price_1h");
                    execute("DROP TABLE base");
                    drainWalAndMatViewQueues();
                }
            }
        });
    }

    @Test
    public void testRangeTaskFailsToOpenWalOfValidViewWhileInvalidationPending() throws Exception {
        // A valid view's ATTACH range task meets an invalidation pending on the view, takes the range, and cannot open
        // the view's WAL writer: the view goes invalid in memory with the error, and the backlog it starts with lacks
        // the range, which only the task carries, so the backlog is lost. The queued invalidation then meets an invalid
        // view and writes nothing, and RESUME refuses.
        final AtomicBoolean isFailArmed = new AtomicBoolean();
        assertMemoryLeak(newViewWalOpenFailingFacade(isFailArmed), () -> {
            createAttachableBaseWithView();
            execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-01'");
            drainWalQueue();
            publishInvalidation("pending invalidation");
            final long viewSeqTxn = viewSeqTxn();
            engine.releaseInactive();
            isFailArmed.set(true);
            drainWalAndMatViewQueues();
            Assert.assertFalse(isFailArmed.get());
            Assert.assertFalse(viewState().isLocked());
            Assert.assertTrue(viewState().isInvalid());
            TestUtils.assertContains(viewState().getInvalidationReasonForReport(), "could not open read-write");
            Assert.assertEquals(MatViewBacklog.LOST_UPDATE_FAILED, viewState().getBacklogLostReason());
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            assertCache(-1);
            assertViewRows(ATTACHABLE_VIEW_ROWS);

            assertFailedRangeTaskLostThenRecovered("could not open read-write", """
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-02T00:00:00.000000Z\tc\t3.0
                    2024-01-02T01:00:00.000000Z\tc\t5.0
                    """);
        });
    }

    @Test
    public void testRangeTaskFindsBaseReplacedUnderValidView() throws Exception {
        // The base table is replaced while the view is valid and the drop's invalidation is lost, as in a crash. The
        // ATTACH range task of the new base finds another table id under the base's name: it invalidates the view with
        // its backlog lost, which the invalidation persists, and drops the range.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("MANUAL");
            execute("DROP TABLE base");
            dropQueuedInvalidation();
            createAttachableBase();
            final long viewSeqTxn = viewSeqTxn();
            execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-01'");
            drainWalAndMatViewQueues();
            Assert.assertFalse(viewState().isLocked());
            assertViewStatus("invalid", MatViewRefreshJob.BASE_TABLE_CHANGED_REASON);
            Assert.assertEquals(MatViewBacklog.LOST_BASE_TABLE_REPLACED, viewState().getBacklogLostReason());
            // the invalidation is the only view state event
            Assert.assertEquals(viewSeqTxn + 1, viewSeqTxn());
            assertCache(-1);
            restart();
            Assert.assertEquals(MatViewBacklog.LOST_BASE_TABLE_REPLACED, viewState().getBacklogLostReason());
            assertLostThenSkipBacklogJumps(MatViewBacklog.LOST_BASE_TABLE_REPLACED, VIEW_ROWS);
        });
    }

    @Test
    public void testRangeTaskFindsBaseReplacedUnderValidViewWhileInvalidationPending() throws Exception {
        // As testRangeTaskFindsBaseReplacedUnderValidView(), with an invalidation pending on the valid view: the ATTACH
        // range task of the new base invalidates the view with its backlog lost in one view state event, and the
        // queued invalidation then meets an invalid view and writes nothing.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("MANUAL");
            execute("DROP TABLE base");
            dropQueuedInvalidation();
            createAttachableBase();
            execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-01'");
            drainWalQueue();
            publishInvalidation("pending invalidation");
            final long viewSeqTxn = viewSeqTxn();
            drainWalAndMatViewQueues();
            Assert.assertFalse(viewState().isLocked());
            assertViewStatus("invalid", MatViewRefreshJob.BASE_TABLE_CHANGED_REASON);
            Assert.assertEquals(MatViewBacklog.LOST_BASE_TABLE_REPLACED, viewState().getBacklogLostReason());
            // the range task's invalidation is the only view state event
            Assert.assertEquals(viewSeqTxn + 1, viewSeqTxn());
            assertCache(-1);
            assertLostThenSkipBacklogJumps(MatViewBacklog.LOST_BASE_TABLE_REPLACED, VIEW_ROWS);
        });
    }

    @Test
    public void testRangeTaskFindsBacklogLost() throws Exception {
        // An invalid view takes an ATTACH range only into a backlog that the cache update before it keeps: with the
        // base's WAL from the cache position gone, the backlog is lost and the range dropped.
        assertMemoryLeak(() -> {
            createAttachableBaseWithView();
            invalidate("test invalidation");
            insert("c", 5.0, "2024-01-02T00:20:00.000000Z");
            drainWalAndMatViewQueues();
            removeBaseWalEventFiles();
            final long viewSeqTxn = viewSeqTxn();
            execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-01'");
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "test invalidation");
            Assert.assertEquals(MatViewBacklog.LOST_BASE_TABLE_WAL_MISSING, viewState().getBacklogLostReason());
            assertCache(-1);
            // the lost marker is the only view state event
            Assert.assertEquals(viewSeqTxn + 1, viewSeqTxn());
            assertLostThenSkipBacklogJumps(MatViewBacklog.LOST_BASE_TABLE_WAL_MISSING, ATTACHABLE_VIEW_ROWS);
        });
    }

    @Test
    public void testRangeTaskKeepsRangeWhenInvalidationPendsBeforeLock() throws Exception {
        // An invalidation that becomes pending on a valid view between an ATTACH range task's checks and its latch
        // leaves the view valid under the latch, so the task takes the range into the view's cache in one view state
        // event: only the task carries the range, since the base's WAL scan skips an ATTACH. The task's release wakes
        // the invalidation, which makes the cache, the range included, the backlog the view starts with, and RESUME
        // catches up on the attached day.
        assertMemoryLeak(() -> {
            createAttachableBaseWithView();
            final MatViewState state = viewState();
            final long viewSeqTxn = viewSeqTxn();
            runRangeTaskChangingViewBeforeLock(() -> state.markAsPendingInvalidation("pending invalidation"));
            assertCache(3, "2024-01-01T00:00:00.000000Z", "2024-01-01T23:59:59.999999Z");
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "pending invalidation");
            // the range task's view state event, then the invalidation's
            Assert.assertEquals(viewSeqTxn + 2, viewSeqTxn());
            Assert.assertNull(viewState().getBacklogLostReason());
            assertCache(3, "2024-01-01T00:00:00.000000Z", "2024-01-01T23:59:59.999999Z");
            assertPersistedCache(3, "2024-01-01T00:00:00.000000Z", "2024-01-01T23:59:59.999999Z");
            assertViewRows(ATTACHABLE_VIEW_ROWS);

            assertResumeSucceeds(RESUME_SQL);
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-02T00:00:00.000000Z\tc\t3.0
                    """);
            assertSqlCursors(VIEW_SQL, "price_1h");
        });
    }

    @Test
    public void testRangeTaskKeepsRangeWhenInvalidationPublishedHoldingLatch() throws Exception {
        // An invalidation that becomes pending while a valid view's ATTACH range task holds the latch leaves the range
        // in the view's cache, as one that became pending before the task's latch does, see
        // testRangeTaskKeepsRangeWhenInvalidationPendsBeforeLock(): the task's release wakes the invalidation, which
        // makes the cache the backlog, and RESUME catches up on the attached day.
        assertMemoryLeak(() -> {
            createAttachableBaseWithView();
            final MatViewState state = viewState();
            final long viewSeqTxn = viewSeqTxn();
            execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-01'");
            drainWalQueue();
            final AtomicBoolean isArmed = new AtomicBoolean(true);
            try (MatViewRefreshJob job = new MatViewRefreshJob(0, engine, 0)) {
                job.setOnHoldingLockForTesting(() -> {
                    if (isArmed.compareAndSet(true, false)) {
                        state.markAsPendingInvalidation("pending invalidation");
                    }
                });
                drainMatViewQueue(job);
            }
            Assert.assertFalse(isArmed.get());
            Assert.assertFalse(state.isLocked());
            drainWalQueue();
            assertViewStatus("invalid", "pending invalidation");
            // the range task's view state event, then the invalidation's
            Assert.assertEquals(viewSeqTxn + 2, viewSeqTxn());
            Assert.assertNull(viewState().getBacklogLostReason());
            assertCache(3, "2024-01-01T00:00:00.000000Z", "2024-01-01T23:59:59.999999Z");
            assertPersistedCache(3, "2024-01-01T00:00:00.000000Z", "2024-01-01T23:59:59.999999Z");

            assertResumeSucceeds(RESUME_SQL);
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-02T00:00:00.000000Z\tc\t3.0
                    """);
            assertSqlCursors(VIEW_SQL, "price_1h");
        });
    }

    @Test
    public void testRangeTaskKeepsRangeWhenPendingInvalidationIsCoveredByFull() throws Exception {
        // An invalidation of a base table change that the view's last FULL refresh covers is pending, as the bare-name
        // guard publishes it, when a valid view's UPDATE range task runs. The task takes the range into the view's cache
        // in one view state event, and the invalidation then finds its marker covered and leaves the view valid. The
        // next refresh recomputes the range, right away or after a restart, which loads the cache from the view's state.
        assertMemoryLeak(() -> {
            for (boolean isRestarted : new boolean[]{false, true}) {
                createBase();
                insertBaseRows();
                createView("MANUAL");
                drainWalQueue();
                execute("REFRESH MATERIALIZED VIEW price_1h FULL");
                drainWalAndMatViewQueues();
                assertViewRows(VIEW_ROWS);
                assertRefreshBaseTxn(1);

                execute("UPDATE base SET price = 9.0 WHERE sym = 'a'");
                drainWalQueue();
                final long viewSeqTxn = viewSeqTxn();
                // a change at base txn 1, which the FULL refresh's snapshot includes
                publishBaseTableInvalidation("covered invalidation", 1);
                drainWalAndMatViewQueues();
                assertViewStatus("valid", "");
                Assert.assertFalse(viewState().hasPendingInvalidationReason());
                // the range task's view state event only
                Assert.assertEquals(viewSeqTxn + 1, viewSeqTxn());
                assertCache(2, "2024-01-01T00:00:00.000000Z", "2024-01-01T23:59:59.999999Z");
                assertPersistedCache(2, "2024-01-01T00:00:00.000000Z", "2024-01-01T23:59:59.999999Z");
                assertRefreshBaseTxn(1);
                assertViewRows(VIEW_ROWS);
                if (isRestarted) {
                    restart();
                    assertViewStatus("valid", "");
                    assertCache(2, "2024-01-01T00:00:00.000000Z", "2024-01-01T23:59:59.999999Z");
                }

                execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
                drainWalAndMatViewQueues();
                assertViewStatus("valid", "");
                assertViewRows("""
                        ts\tsym\tprice
                        2024-01-01T00:00:00.000000Z\ta\t9.0
                        2024-01-01T01:00:00.000000Z\tb\t2.0
                        """);
                assertSqlCursors(VIEW_SQL, "price_1h");
                execute("DROP MATERIALIZED VIEW price_1h");
                execute("DROP TABLE base");
                drainWalAndMatViewQueues();
            }
        });
    }

    @Test
    public void testRangeTaskMeetsViewSuspendedBeforeLatch() throws Exception {
        // With writes to a suspended WAL denied, the view's WAL is suspended after an ATTACH range task's checks and
        // before its latch, and the view's WAL refuses the task's writer. A valid view drops the range, as when a
        // restart loses the task, and stays valid. An invalid view's backlog lacks the range, which only the task
        // carries, so the backlog is lost: RESUME after RESUME WAL refuses instead of returning the view valid without
        // the attached day.
        setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, "true");
        assertMemoryLeak(() -> {
            for (boolean isInvalid : new boolean[]{false, true}) {
                createAttachableBaseWithView();
                if (isInvalid) {
                    invalidate("test invalidation");
                }
                final long viewSeqTxn = viewSeqTxn();
                runRangeTaskChangingViewBeforeLock(() -> executeInSeam("ALTER MATERIALIZED VIEW price_1h SUSPEND WAL"));
                Assert.assertEquals(viewSeqTxn, viewSeqTxn());
                assertCache(-1);
                Assert.assertEquals(isInvalid, viewState().isInvalid());
                if (isInvalid) {
                    Assert.assertEquals(MatViewBacklog.LOST_UPDATE_FAILED, viewState().getBacklogLostReason());
                    execute("ALTER MATERIALIZED VIEW price_1h RESUME WAL");
                    drainWalAndMatViewQueues();
                    assertFailedRangeTaskLostThenRecovered("test invalidation", """
                            ts\tsym\tprice
                            2024-01-01T00:00:00.000000Z\ta\t1.0
                            2024-01-02T00:00:00.000000Z\tc\t3.0
                            2024-01-02T01:00:00.000000Z\tc\t5.0
                            """);
                } else {
                    Assert.assertNull(viewState().getBacklogLostReason());
                    execute("ALTER MATERIALIZED VIEW price_1h RESUME WAL");
                    drainWalAndMatViewQueues();
                    assertViewStatus("valid", "");
                }
                execute("DROP MATERIALIZED VIEW price_1h");
                execute("DROP TABLE base");
                drainWalAndMatViewQueues();
            }
        });
    }

    @Test
    public void testRangeTaskOfConfiguredSuspendedViewLosesBacklog() throws Exception {
        // With writes to a suspended WAL denied, a view listed in cairo.wal.apply.suspended.tables takes no write, as
        // after ALTER ... SUSPEND WAL: an invalid view's ATTACH range task loses the backlog, which lacks the range,
        // and RESUME refuses once the list no longer holds the view.
        setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, "true");
        assertMemoryLeak(() -> {
            createAttachableBaseWithView();
            invalidate("test invalidation");
            final long viewSeqTxn = viewSeqTxn();
            setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_TABLES, viewToken().getDirName());
            try {
                execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-01'");
                drainWalAndMatViewQueues();
                Assert.assertFalse(viewState().isLocked());
                Assert.assertTrue(viewState().isInvalid());
                Assert.assertEquals(MatViewBacklog.LOST_UPDATE_FAILED, viewState().getBacklogLostReason());
                assertCache(-1);
                Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            } finally {
                setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_TABLES, null);
            }
            assertViewStatus("invalid", "test invalidation");
            assertFailedRangeTaskLostThenRecovered("test invalidation", """
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-02T00:00:00.000000Z\tc\t3.0
                    2024-01-02T01:00:00.000000Z\tc\t5.0
                    """);
        });
    }

    @Test
    public void testRangeTaskOfInvalidViewResumedBeforeLatchAddsRange() throws Exception {
        // With writes to a suspended WAL denied, an invalid view's ATTACH range task is queued while the view's WAL is
        // suspended, and RESUME WAL comes after the task's checks and before its latch. The view's WAL then takes the
        // task's writer, so the task adds the range to the backlog in one view state event, and RESUME catches up on
        // it.
        setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, "true");
        assertMemoryLeak(() -> {
            createAttachableBaseWithView();
            invalidate("test invalidation");
            execute("ALTER MATERIALIZED VIEW price_1h SUSPEND WAL");
            final long viewSeqTxn = viewSeqTxn();
            runRangeTaskChangingViewBeforeLock(() -> executeInSeam("ALTER MATERIALIZED VIEW price_1h RESUME WAL"));
            drainWalQueue();
            Assert.assertEquals(viewSeqTxn + 1, viewSeqTxn());
            Assert.assertNull(viewState().getBacklogLostReason());
            assertViewStatus("invalid", "test invalidation");
            assertCache(3, "2024-01-01T00:00:00.000000Z", "2024-01-01T23:59:59.999999Z");
            assertPersistedCache(3, "2024-01-01T00:00:00.000000Z", "2024-01-01T23:59:59.999999Z");

            assertResumeSucceeds(RESUME_SQL);
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-02T00:00:00.000000Z\tc\t3.0
                    """);
            assertSqlCursors(VIEW_SQL, "price_1h");
        });
    }

    @Test
    public void testRangeTaskOfNeverRefreshedViewDropsRangeWhileInvalidationPending() throws Exception {
        // A view that never completed a refresh takes no base table range, since its first build recomputes the whole
        // base, also while the invalidation of a base table change is pending on it: the ATTACH range task writes
        // nothing, the invalidation, which does not force, declines the view, and the first build brings the attached
        // day.
        assertMemoryLeak(() -> {
            createAttachableBase();
            createView("MANUAL DEFERRED");
            drainWalAndMatViewQueues();
            assertRefreshBaseTxn(-1);
            execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-01'");
            drainWalQueue();
            publishBaseTableInvalidation("base invalidation", 3);
            final long viewSeqTxn = viewSeqTxn();
            drainWalAndMatViewQueues();
            Assert.assertFalse(viewState().isLocked());
            assertViewStatus("valid", "");
            Assert.assertFalse(viewState().hasPendingInvalidationReason());
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            assertRefreshBaseTxn(-1);
            assertCache(-1);

            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-02T00:00:00.000000Z\tc\t3.0
                    """);
            assertSqlCursors(VIEW_SQL, "price_1h");
        });
    }

    @Test
    public void testRangeTaskOfReplacedBaseDropsRangeWhileInvalidationPending() throws Exception {
        // The base table is dropped and re-created while the range task of an UPDATE of the old base waits in the queue
        // and the drop's invalidation is pending on the valid view. The task drops the range of the replaced table
        // without a write, the invalidation that the refresh job runs for the old base's tasks is the only view state
        // event, and the backlog it starts is lost at its next update: RESUME refuses, and RESUME SKIP BACKLOG takes
        // the new base table.
        assertMemoryLeak(() -> {
            createAttachableBaseWithView();
            execute("UPDATE base SET price = 30.0 WHERE sym = 'c'");
            drainWalQueue();
            final int oldBaseTableId = baseToken().getTableId();
            execute("DROP TABLE base");
            createBase();
            Assert.assertNotEquals(oldBaseTableId, baseToken().getTableId());
            // the drop's invalidation published its marker, and its tasks wait behind the range task
            viewState().markAsPendingInvalidation("base table is dropped or renamed");
            final long viewSeqTxn = viewSeqTxn();
            drainWalAndMatViewQueues();
            Assert.assertFalse(viewState().isLocked());
            assertViewStatus("invalid", "base table is dropped or renamed");
            Assert.assertEquals(viewSeqTxn + 1, viewSeqTxn());
            assertCache(-1);
            assertLostThenSkipBacklogJumps(MatViewBacklog.LOST_BASE_TABLE_REPLACED, ATTACHABLE_VIEW_ROWS);
        });
    }

    @Test
    public void testRangeTaskOfSuspendedInvalidViewLosesBacklog() throws Exception {
        // With writes to a suspended WAL denied, an invalid view's ATTACH or UPDATE range task meets the view's WAL
        // suspended by ALTER ... SUSPEND WAL. The view's WAL refuses the task's writer, and the backlog, which lacks
        // the range that only the task carries, is lost in memory; the suspension does not invalidate the view again.
        // A later UPDATE adds nothing to the lost backlog. RESUME waits for RESUME WAL and then refuses with the cause,
        // instead of returning the view valid without the range.
        setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, "true");
        assertMemoryLeak(() -> {
            for (String refreshType : new String[]{"MANUAL", "IMMEDIATE"}) {
                for (boolean isAttach : new boolean[]{true, false}) {
                    createAttachableBaseWithView(refreshType);
                    invalidate("test invalidation");
                    execute("ALTER MATERIALIZED VIEW price_1h SUSPEND WAL");
                    final long viewSeqTxn = viewSeqTxn();
                    execute(isAttach ? "ALTER TABLE base ATTACH PARTITION LIST '2024-01-01'" : "UPDATE base SET price = 30.0 WHERE sym = 'c'");
                    drainWalAndMatViewQueues();
                    Assert.assertFalse(viewState().isLocked());
                    assertViewStatus("invalid", "test invalidation");
                    Assert.assertEquals(MatViewBacklog.LOST_UPDATE_FAILED, viewState().getBacklogLostReason());
                    assertCache(-1);
                    Assert.assertEquals(viewSeqTxn, viewSeqTxn());

                    execute("UPDATE base SET price = 40.0 WHERE sym = 'c'");
                    drainWalAndMatViewQueues();
                    Assert.assertEquals(viewSeqTxn, viewSeqTxn());
                    Assert.assertEquals(MatViewBacklog.LOST_UPDATE_FAILED, viewState().getBacklogLostReason());
                    assertCache(-1);

                    for (String resumeSql : new String[]{RESUME_SQL, RESUME_SKIP_SQL}) {
                        assertExceptionNoLeakCheck(
                                resumeSql,
                                VIEW_NAME_POSITION,
                                "materialized view WAL is suspended, run ALTER MATERIALIZED VIEW price_1h RESUME WAL first"
                        );
                        Assert.assertTrue(viewState().isInvalid());
                        Assert.assertFalse(viewState().isLocked());
                        Assert.assertEquals(MatViewBacklog.LOST_UPDATE_FAILED, viewState().getBacklogLostReason());
                    }
                    Assert.assertEquals(viewSeqTxn, viewSeqTxn());
                    execute("ALTER MATERIALIZED VIEW price_1h RESUME WAL");
                    drainWalAndMatViewQueues();
                    assertFailedRangeTaskLostThenRecovered(
                            "test invalidation",
                            "ts\tsym\tprice\n"
                                    + (isAttach ? "2024-01-01T00:00:00.000000Z\ta\t1.0\n" : "")
                                    + "2024-01-02T00:00:00.000000Z\tc\t40.0\n"
                                    + "2024-01-02T01:00:00.000000Z\tc\t5.0\n"
                    );
                    execute("DROP MATERIALIZED VIEW price_1h");
                    execute("DROP TABLE base");
                    drainWalAndMatViewQueues();
                }
            }
        });
    }

    @Test
    public void testRangeTaskOfSuspendedViewDefersWhileLatchBusy() throws Exception {
        // An invalid view's ATTACH range task that finds the view latch taken while the view's WAL is suspended goes
        // back on the queue, as for any view, and loses the backlog under the latch once the holder lets go.
        setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, "true");
        assertMemoryLeak(() -> {
            createAttachableBaseWithView();
            invalidate("test invalidation");
            execute("ALTER MATERIALIZED VIEW price_1h SUSPEND WAL");
            execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-01'");
            drainWalQueue();
            final long viewSeqTxn = viewSeqTxn();
            Assert.assertTrue(viewState().tryLock());
            try (MatViewRefreshJob job = new MatViewRefreshJob(0, engine, 0)) {
                job.run();
                Assert.assertNull(viewState().getBacklogLostReason());
                final MatViewRefreshTask rangeTask = takeRangeTask();
                Assert.assertEquals(viewToken(), rangeTask.matViewToken);
                Assert.assertEquals(parseFloorPartialTimestamp("2024-01-01T00:00:00.000000Z"), rangeTask.rangeFrom);
                Assert.assertEquals(parseFloorPartialTimestamp("2024-01-01T23:59:59.999999Z"), rangeTask.rangeTo);
                engine.getMatViewStateStore().reenqueueRefreshTask(rangeTask);
                viewState().unlock();
                drainMatViewQueue(job);
            }
            drainWalQueue();
            Assert.assertFalse(viewState().isLocked());
            Assert.assertEquals(MatViewBacklog.LOST_UPDATE_FAILED, viewState().getBacklogLostReason());
            assertCache(-1);
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            assertViewStatus("invalid", "test invalidation");
        });
    }

    @Test
    public void testRangeTaskOfSuspendedViewOverReplacedBaseLosesBacklog() throws Exception {
        // The base table is dropped and re-created while an invalid view's WAL is suspended and the range task of an
        // UPDATE of the old base waits in the queue. The view's WAL refuses the task's writer before the task checks
        // the base table's identity, so the backlog is lost as for any refused range, with that cause rather than
        // "base table replaced". RESUME refuses either way, and RESUME SKIP BACKLOG takes the new base table.
        setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, "true");
        assertMemoryLeak(() -> {
            createAttachableBaseWithView();
            invalidate("test invalidation");
            execute("ALTER MATERIALIZED VIEW price_1h SUSPEND WAL");
            execute("UPDATE base SET price = 30.0 WHERE sym = 'c'");
            drainWalQueue();
            final int oldBaseTableId = baseToken().getTableId();
            execute("DROP TABLE base");
            createBase();
            final long viewSeqTxn = viewSeqTxn();
            drainWalAndMatViewQueues();
            Assert.assertNotEquals(oldBaseTableId, baseToken().getTableId());
            Assert.assertFalse(viewState().isLocked());
            Assert.assertTrue(viewState().isInvalid());
            Assert.assertEquals(MatViewBacklog.LOST_UPDATE_FAILED, viewState().getBacklogLostReason());
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            execute("ALTER MATERIALIZED VIEW price_1h RESUME WAL");
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "test invalidation");
            assertLostThenSkipBacklogJumps(MatViewBacklog.LOST_UPDATE_FAILED, ATTACHABLE_VIEW_ROWS);
        });
    }

    @Test
    public void testRangeTaskOfViewSuspendedWhileHoldingLatchKeepsMergedRange() throws Exception {
        // With writes to a suspended WAL denied, the view's WAL is suspended while an invalid view's ATTACH range task
        // holds the latch and the view's WAL writer. The task's scan brings the backlog up to the ATTACH and merges the
        // range into it, and only the view state event is refused: the backlog keeps the range in memory, and RESUME
        // after RESUME WAL catches up on it, right away, or after the first backlog update after RESUME WAL persisted
        // the backlog and a restart.
        setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, "true");
        assertMemoryLeak(() -> {
            for (boolean isBacklogUpdated : new boolean[]{false, true}) {
                createAttachableBaseWithView();
                invalidate("test invalidation");
                execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-01'");
                drainWalQueue();
                final long viewSeqTxn = viewSeqTxn();
                runRangeTaskSuspendingWhileHolding();
                Assert.assertTrue(viewState().isInvalid());
                Assert.assertNull(viewState().getBacklogLostReason());
                assertCache(3, "2024-01-01T00:00:00.000000Z", "2024-01-01T23:59:59.999999Z");
                Assert.assertEquals(viewSeqTxn, viewSeqTxn());

                String viewRows = """
                        ts\tsym\tprice
                        2024-01-01T00:00:00.000000Z\ta\t1.0
                        2024-01-02T00:00:00.000000Z\tc\t3.0
                        """;
                if (isBacklogUpdated) {
                    // the suspended view takes no backlog update
                    insert("c", 5.0, "2024-01-02T01:20:00.000000Z");
                    drainWalAndMatViewQueues();
                    updateBacklog();
                    Assert.assertEquals(viewSeqTxn, viewSeqTxn());
                    assertCache(3, "2024-01-01T00:00:00.000000Z", "2024-01-01T23:59:59.999999Z");
                    execute("ALTER MATERIALIZED VIEW price_1h RESUME WAL");
                    drainWalAndMatViewQueues();
                    insert("c", 6.0, "2024-01-02T02:20:00.000000Z");
                    drainWalAndMatViewQueues();
                    updateBacklog();
                    Assert.assertEquals(viewSeqTxn + 1, viewSeqTxn());
                    assertViewStatus("invalid", "test invalidation");
                    final String[] backlog = {
                            "2024-01-01T00:00:00.000000Z", "2024-01-01T23:59:59.999999Z",
                            "2024-01-02T01:20:00.000000Z", "2024-01-02T01:20:00.000000Z",
                            "2024-01-02T02:20:00.000000Z", "2024-01-02T02:20:00.000000Z"
                    };
                    assertPersistedCache(5, backlog);
                    restart();
                    Assert.assertTrue(viewState().isInvalid());
                    Assert.assertNull(viewState().getBacklogLostReason());
                    assertCache(5, backlog);
                    viewRows = """
                            ts\tsym\tprice
                            2024-01-01T00:00:00.000000Z\ta\t1.0
                            2024-01-02T00:00:00.000000Z\tc\t3.0
                            2024-01-02T01:00:00.000000Z\tc\t5.0
                            2024-01-02T02:00:00.000000Z\tc\t6.0
                            """;
                } else {
                    execute("ALTER MATERIALIZED VIEW price_1h RESUME WAL");
                    drainWalAndMatViewQueues();
                }

                assertResumeSucceeds(RESUME_SQL);
                execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
                drainWalAndMatViewQueues();
                assertViewStatus("valid", "");
                assertViewRows(viewRows);
                assertSqlCursors(VIEW_SQL, "price_1h");
                execute("DROP MATERIALIZED VIEW price_1h");
                execute("DROP TABLE base");
                drainWalAndMatViewQueues();
            }
        });
    }

    @Test
    public void testRangeTaskOfViewSuspendedWhileHoldingLatchLosesNextRange() throws Exception {
        // An invalid view's WAL is suspended while the view's ATTACH range task holds the latch, after the task's scan
        // merged the range into the backlog, which keeps it in memory. An UPDATE while the view's WAL is still
        // suspended loses the backlog: the view's WAL refuses the UPDATE's range task, whose range only the task
        // carries.
        setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, "true");
        assertMemoryLeak(() -> {
            createAttachableBaseWithView();
            invalidate("test invalidation");
            execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-01'");
            drainWalQueue();
            final long viewSeqTxn = viewSeqTxn();
            runRangeTaskSuspendingWhileHolding();
            Assert.assertNull(viewState().getBacklogLostReason());
            assertCache(3, "2024-01-01T00:00:00.000000Z", "2024-01-01T23:59:59.999999Z");

            execute("UPDATE base SET price = 30.0 WHERE sym = 'c'");
            drainWalAndMatViewQueues();
            Assert.assertFalse(viewState().isLocked());
            Assert.assertTrue(viewState().isInvalid());
            Assert.assertEquals(MatViewBacklog.LOST_UPDATE_FAILED, viewState().getBacklogLostReason());
            assertCache(3, "2024-01-01T00:00:00.000000Z", "2024-01-01T23:59:59.999999Z");
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            execute("ALTER MATERIALIZED VIEW price_1h RESUME WAL");
            drainWalAndMatViewQueues();
            assertFailedRangeTaskLostThenRecovered("test invalidation", """
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-02T00:00:00.000000Z\tc\t30.0
                    2024-01-02T01:00:00.000000Z\tc\t5.0
                    """);
        });
    }

    @Test
    public void testRangeTaskOfViewSuspendedWhileHoldingLatchLosesTruncateDrop() throws Exception {
        // A base TRUNCATE applies after an ATTACH and before the ATTACH's range task runs, and the invalid view's WAL
        // is suspended while the task holds the latch. The task drops the range, whose rows the truncate removed, and
        // the view's WAL refuses the cache that the task scanned up to the truncate. The range did not reach the cache,
        // so the backlog is lost, as for any refused range, although the truncate made the range moot.
        setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, "true");
        assertMemoryLeak(() -> {
            createAttachableBaseWithView();
            invalidate("test invalidation");
            execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-01'");
            drainWalQueue();
            // a TRUNCATE in the ATTACH's apply batch would clear the batch's range, and no range task would run
            execute("TRUNCATE TABLE base");
            drainWalQueue();
            final long viewSeqTxn = viewSeqTxn();
            runRangeTaskSuspendingWhileHolding();
            Assert.assertTrue(viewState().isInvalid());
            Assert.assertEquals(MatViewBacklog.LOST_UPDATE_FAILED, viewState().getBacklogLostReason());
            assertCache(4);
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            execute("ALTER MATERIALIZED VIEW price_1h RESUME WAL");
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "test invalidation");

            assertResumeRefused(MatViewBacklog.LOST_UPDATE_FAILED);
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            assertResumeSucceeds(RESUME_SKIP_SQL);
            assertRefreshBaseTxn(4);
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            // a TRUNCATE keeps the view's rows
            assertViewRows(ATTACHABLE_VIEW_ROWS);
            restart();
            assertViewStatus("valid", "");
            Assert.assertNull(viewState().getBacklogLostReason());
        });
    }

    @Test
    public void testRangeTaskScanFailureLosesBacklog() throws Exception {
        // A valid view's ATTACH range task cannot read the base's WAL, and missing WAL files are fatal: the scan throws
        // before the range reaches the cache, so the backlog that the view starts with as it goes invalid is lost.
        setProperty(PropertyKey.DEBUG_MAT_VIEW_REFRESH_MISSING_WAL_FILES_FATAL, "true");
        final AtomicBoolean isScanFailArmed = new AtomicBoolean();
        assertMemoryLeak(newBaseWalReadAndSeqPartFailingFacade(isScanFailArmed, new AtomicBoolean(), new StringSink()), () -> {
            createAttachableBaseWithView();
            insert("c", 4.0, "2024-01-02T02:20:00.000000Z");
            execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-01'");
            drainWalQueue();
            final long viewSeqTxn = viewSeqTxn();
            isScanFailArmed.set(true);
            drainMatViewQueue(engine);
            drainWalQueue();
            Assert.assertFalse(isScanFailArmed.get());
            Assert.assertFalse(viewState().isLocked());
            assertCache(-1);
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            assertFailedRangeTaskLostThenRecovered("cannot read WAL event file", """
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-02T00:00:00.000000Z\tc\t3.0
                    2024-01-02T01:00:00.000000Z\tc\t5.0
                    2024-01-02T02:00:00.000000Z\tc\t4.0
                    """);
        });
    }

    @Test
    public void testRangeTaskSkipsViewDroppedBeforeLock() throws Exception {
        // A DROP MATERIALIZED VIEW that marks the view's state dropped between an ATTACH range task's checks and its
        // latch leaves the task nothing to add, for a valid view and for an invalid one whose backlog is tracked.
        assertMemoryLeak(() -> {
            for (boolean isInvalid : new boolean[]{false, true}) {
                createAttachableBaseWithView();
                if (isInvalid) {
                    invalidate("test invalidation");
                }
                final MatViewState state = viewState();
                final long viewSeqTxn = viewSeqTxn();
                try {
                    runRangeTaskChangingViewBeforeLock(state::markAsDropped);
                } finally {
                    state.tryCloseIfDropped();
                }
                drainWalQueue();
                Assert.assertEquals(viewSeqTxn, viewSeqTxn());
                Assert.assertEquals(0, state.getRefreshIntervals().size());
                Assert.assertEquals(-1, state.getRefreshIntervalsBaseTxn());
                Assert.assertEquals(isInvalid, state.isInvalid());
                execute("DROP MATERIALIZED VIEW price_1h");
                execute("DROP TABLE base");
                drainWalAndMatViewQueues();
            }
        });
    }

    @Test
    public void testRangeTaskSuspendedViewErrorLosesBacklog() throws Exception {
        // A view WAL suspended while an invalid view takes an ATTACH range fails the task with a "table suspended"
        // error before the range reaches the cache. The error does not invalidate the view again, but the backlog lacks
        // the range, which only the task carries: the backlog is lost, so RESUME refuses instead of returning the view
        // valid without the attached day.
        assertMemoryLeak(() -> {
            createAttachableBaseWithView();
            invalidate("test invalidation");
            final long viewSeqTxn = viewSeqTxn();
            failRangeTask(CairoException.tableSuspended(viewToken()));
            assertViewStatus("invalid", "test invalidation");
            Assert.assertEquals(MatViewBacklog.LOST_UPDATE_FAILED, viewState().getBacklogLostReason());
            assertCache(-1);
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            assertFailedRangeTaskLostThenRecovered("test invalidation", """
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-02T00:00:00.000000Z\tc\t3.0
                    2024-01-02T01:00:00.000000Z\tc\t5.0
                    """);
        });
    }

    @Test
    public void testRangeTaskTakesBacklogPathWhenInvalidationLandsBeforeLock() throws Exception {
        // An ATTACH range task meets an invalidation pending on a valid view at its checks, and the invalidation makes
        // the view invalid between the checks and the task's latch. Under the latch the task finds an invalid view whose
        // backlog is tracked, and takes the range into the backlog in one view state event that keeps the view invalid
        // with its reason: RESUME catches up on the attached day.
        assertMemoryLeak(() -> {
            createAttachableBaseWithView();
            viewState().markAsPendingInvalidation("pending invalidation");
            final long viewSeqTxn = viewSeqTxn();
            runRangeTaskChangingViewBeforeLock(() -> {
                engine.getMatViewStateStore().enqueueInvalidate(viewToken(), "pending invalidation");
                try (MatViewRefreshJob invalidationJob = new MatViewRefreshJob(1, engine, 0)) {
                    drainMatViewQueue(invalidationJob);
                }
                Assert.assertTrue(viewState().isInvalid());
            });
            drainWalQueue();
            assertViewStatus("invalid", "pending invalidation");
            // the invalidation's view state event, then the range task's
            Assert.assertEquals(viewSeqTxn + 2, viewSeqTxn());
            Assert.assertNull(viewState().getBacklogLostReason());
            assertCache(3, "2024-01-01T00:00:00.000000Z", "2024-01-01T23:59:59.999999Z");
            assertPersistedCache(3, "2024-01-01T00:00:00.000000Z", "2024-01-01T23:59:59.999999Z");

            assertResumeSucceeds(RESUME_SQL);
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-02T00:00:00.000000Z\tc\t3.0
                    """);
            assertSqlCursors(VIEW_SQL, "price_1h");
        });
    }

    @Test
    public void testRangeTaskTruncateDropFailureLosesBacklog() throws Exception {
        // A base TRUNCATE applies after an ATTACH and before the ATTACH's range task runs. The task drops the range,
        // whose rows the truncate removed, and fails to persist the cache it scanned up to the truncate. The range did
        // not reach the cache, so the backlog that the view starts with as it goes invalid is lost.
        setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 1);
        final AtomicBoolean isFailArmed = new AtomicBoolean();
        final StringSink failingPart = new StringSink();
        assertMemoryLeak(newSeqPartFailingFacade(isFailArmed, failingPart), () -> {
            createAttachableBaseWithView();
            execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-01'");
            drainWalQueue();
            // a TRUNCATE in the ATTACH's apply batch would clear the batch's range, and no range task would run
            execute("TRUNCATE TABLE base");
            drainWalQueue();
            final long viewSeqTxn = viewSeqTxn();
            armViewStateEventFailure(isFailArmed, failingPart);
            drainMatViewQueue(engine);
            drainWalQueue();
            Assert.assertFalse(isFailArmed.get());
            Assert.assertFalse(viewState().isLocked());
            Assert.assertTrue(viewState().isInvalid());
            TestUtils.assertContains(viewState().getInvalidationReasonForReport(), "could not open read-write");
            Assert.assertEquals(MatViewBacklog.LOST_UPDATE_FAILED, viewState().getBacklogLostReason());
            assertCache(4);
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());

            assertResumeRefused(MatViewBacklog.LOST_UPDATE_FAILED);
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            assertResumeSucceeds(RESUME_SKIP_SQL);
            assertRefreshBaseTxn(4);
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            // a TRUNCATE keeps the view's rows
            assertViewRows(ATTACHABLE_VIEW_ROWS);
            restart();
            assertViewStatus("valid", "");
            Assert.assertNull(viewState().getBacklogLostReason());
        });
    }

    @Test
    public void testRangeTaskTruncatedAfterOperationDropsRangeWhileInvalidationPending() throws Exception {
        // A base TRUNCATE applies after an ATTACH and before the ATTACH's range task runs, while an invalidation is
        // pending on the valid view. The task drops the range, whose rows the truncate removed, and persists the cache
        // it scanned up to the truncate, which the invalidation then makes the backlog: the backlog stays tracked, and
        // RESUME keeps the view's rows.
        assertMemoryLeak(() -> {
            createAttachableBaseWithView();
            execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-01'");
            drainWalQueue();
            // a TRUNCATE in the ATTACH's apply batch would clear the batch's range, and no range task would run
            execute("TRUNCATE TABLE base");
            drainWalQueue();
            publishInvalidation("pending invalidation");
            final long viewSeqTxn = viewSeqTxn();
            drainWalAndMatViewQueues();
            Assert.assertFalse(viewState().isLocked());
            assertViewStatus("invalid", "pending invalidation");
            Assert.assertNull(viewState().getBacklogLostReason());
            // the range task's view state event with the cache it scanned, then the invalidation's
            Assert.assertEquals(viewSeqTxn + 2, viewSeqTxn());
            assertCache(4);
            assertPersistedCache(4);

            assertResumeSucceeds(RESUME_SQL);
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            // a TRUNCATE keeps the view's rows
            assertViewRows(ATTACHABLE_VIEW_ROWS);
        });
    }

    @Test
    public void testRangeTaskWhileInvalidationPendingFindsBaseWalMissing() throws Exception {
        // With missing WAL files not fatal, as by default, a valid view's ATTACH range task meets an invalidation
        // pending on the view and cannot read the base's WAL from the view's refresh position: the scan clears the
        // cache, and the task takes the range into the cleared cache at the refresh position. The invalidation makes
        // that cache the backlog, whose next update finds the base's WAL missing, so RESUME refuses, as it does when the
        // backlog lacks the range. RESUME SKIP BACKLOG and a REFRESH ... RANGE then bring the view to its SQL.
        setProperty(PropertyKey.DEBUG_MAT_VIEW_REFRESH_MISSING_WAL_FILES_FATAL, "false");
        assertMemoryLeak(() -> {
            createAttachableBaseWithView();
            insert("c", 5.0, "2024-01-02T00:20:00.000000Z");
            drainWalAndMatViewQueues();
            removeBaseWalEventFiles();
            execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-01'");
            drainWalQueue();
            publishInvalidation("pending invalidation");
            final long viewSeqTxn = viewSeqTxn();
            drainWalAndMatViewQueues();
            Assert.assertFalse(viewState().isLocked());
            assertViewStatus("invalid", "pending invalidation");
            Assert.assertNull(viewState().getBacklogLostReason());
            // the range task's view state event, then the invalidation's
            Assert.assertEquals(viewSeqTxn + 2, viewSeqTxn());
            // the refresh position, base txn 2, is before the insert, whose WAL is missing
            assertCache(2, "2024-01-01T00:00:00.000000Z", "2024-01-01T23:59:59.999999Z");
            assertPersistedCache(2, "2024-01-01T00:00:00.000000Z", "2024-01-01T23:59:59.999999Z");

            assertResumeRefused(MatViewBacklog.LOST_BASE_TABLE_WAL_MISSING);
            Assert.assertEquals(viewSeqTxn + 2, viewSeqTxn());
            assertResumeSucceeds(RESUME_SKIP_SQL);
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            execute("REFRESH MATERIALIZED VIEW price_1h RANGE FROM '2024-01-01' TO '2024-01-03'");
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-02T00:00:00.000000Z\tc\t5.0
                    """);
            assertSqlCursors(VIEW_SQL, "price_1h");
        });
    }

    @Test
    public void testRangeTaskWritesSuspendedViewWalWhenWriteDenialOff() throws Exception {
        // With writes to a suspended WAL allowed, as by default, an invalid view's ATTACH range task writes its view
        // state event into the suspended view's WAL, which applies it after RESUME WAL: the backlog keeps the range,
        // and RESUME catches up on it.
        assertMemoryLeak(() -> {
            createAttachableBaseWithView();
            invalidate("test invalidation");
            execute("ALTER MATERIALIZED VIEW price_1h SUSPEND WAL");
            final long viewSeqTxn = viewSeqTxn();
            execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-01'");
            drainWalAndMatViewQueues();
            Assert.assertFalse(viewState().isLocked());
            Assert.assertTrue(viewState().isInvalid());
            Assert.assertNull(viewState().getBacklogLostReason());
            assertCache(3, "2024-01-01T00:00:00.000000Z", "2024-01-01T23:59:59.999999Z");
            Assert.assertEquals(viewSeqTxn + 1, viewSeqTxn());
            // the view state event waits in the suspended view's WAL
            assertPersistedCache(-1);
            execute("ALTER MATERIALIZED VIEW price_1h RESUME WAL");
            drainWalAndMatViewQueues();
            assertPersistedCache(3, "2024-01-01T00:00:00.000000Z", "2024-01-01T23:59:59.999999Z");
            assertViewStatus("invalid", "test invalidation");

            assertResumeSucceeds(RESUME_SQL);
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-02T00:00:00.000000Z\tc\t3.0
                    """);
            assertSqlCursors(VIEW_SQL, "price_1h");
        });
    }

    @Test
    public void testRangeTasksWhileInvalidationPendingReachBacklog() throws Exception {
        // The range tasks of an UPDATE of one day and of an ATTACH of another, applied in WAL batches around a data
        // commit, wait in the refresh queue while an invalidation is pending on the valid view. Each takes its range into
        // the view's cache, the first one with the data commit that its scan finds, and the invalidation makes the cache
        // the backlog: RESUME recomputes both ranges. The cache keeps them apart at the default cap of 100 intervals, and
        // squashes them into one at a cap of 1, which RESUME recomputes whole.
        assertMemoryLeak(() -> {
            for (int maxIntervals : new int[]{100, 1}) {
                setProperty(PropertyKey.CAIRO_MAT_VIEW_MAX_REFRESH_INTERVALS, maxIntervals);
                createBase();
                execute("""
                        INSERT INTO base VALUES
                        ('a', 1.0, 1.0, '2024-01-01T00:10:00.000000Z'),
                        ('c', 3.0, 1.0, '2024-01-03T00:10:00.000000Z'),
                        ('e', 5.0, 1.0, '2024-01-05T00:10:00.000000Z')
                        """);
                drainWalQueue();
                execute("ALTER TABLE base DETACH PARTITION LIST '2024-01-03'");
                drainWalQueue();
                makeAttachable("2024-01-03");
                createView("MANUAL");
                drainWalQueue();
                execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
                drainWalAndMatViewQueues();
                assertViewRows("""
                        ts\tsym\tprice
                        2024-01-01T00:00:00.000000Z\ta\t1.0
                        2024-01-05T00:00:00.000000Z\te\t5.0
                        """);
                assertRefreshBaseTxn(2);

                // base txns 3 to 5, in three WAL batches
                execute("UPDATE base SET price = 10.0 WHERE sym = 'a'");
                drainWalQueue();
                insert("e", 6.0, "2024-01-05T01:10:00.000000Z");
                drainWalQueue();
                execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-03'");
                drainWalQueue();
                publishInvalidation("pending invalidation");
                final long viewSeqTxn = viewSeqTxn();
                drainWalAndMatViewQueues();
                assertViewStatus("invalid", "pending invalidation");
                Assert.assertNull(viewState().getBacklogLostReason());
                // the view state events of the two range tasks, then the invalidation's
                Assert.assertEquals(viewSeqTxn + 3, viewSeqTxn());
                final String[] backlog = maxIntervals == 1
                        ? new String[]{"2024-01-01T00:00:00.000000Z", "2024-01-05T01:10:00.000000Z"}
                        : new String[]{
                        "2024-01-01T00:00:00.000000Z", "2024-01-01T23:59:59.999999Z",
                        "2024-01-03T00:00:00.000000Z", "2024-01-03T23:59:59.999999Z",
                        "2024-01-05T01:10:00.000000Z", "2024-01-05T01:10:00.000000Z"
                };
                assertCache(5, backlog);
                assertPersistedCache(5, backlog);

                assertResumeSucceeds(RESUME_SQL);
                execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
                drainWalAndMatViewQueues();
                assertViewStatus("valid", "");
                assertViewRows("""
                        ts\tsym\tprice
                        2024-01-01T00:00:00.000000Z\ta\t10.0
                        2024-01-03T00:00:00.000000Z\tc\t3.0
                        2024-01-05T00:00:00.000000Z\te\t5.0
                        2024-01-05T01:00:00.000000Z\te\t6.0
                        """);
                assertSqlCursors(VIEW_SQL, "price_1h");
                execute("DROP MATERIALIZED VIEW price_1h");
                execute("DROP TABLE base");
                drainWalAndMatViewQueues();
            }
        });
    }

    @Test
    public void testResumeAfterPurgeOfSuspendedViewFindsBacklogLost() throws Exception {
        // While the view's own WAL is suspended, the view releases the base's WAL floor, and the WAL its backlog
        // needs is purged. After RESUME WAL, RESUME finds the backlog lost and refuses, and RESUME SKIP BACKLOG jumps:
        // no refresh recomputes the base's whole min..max range.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            invalidate("test invalidation");
            execute("ALTER MATERIALIZED VIEW price_1h SUSPEND WAL");
            engine.releaseInactive();
            insertGapRows();
            purgeBaseWal();
            Assert.assertFalse(isBaseWalSegmentPresent(2));
            Assert.assertFalse(isBaseWalSegmentPresent(3));
            execute("ALTER MATERIALIZED VIEW price_1h RESUME WAL");
            drainWalAndMatViewQueues();
            assertLostThenSkipBacklogJumps(MatViewBacklog.LOST_BASE_TABLE_WAL_MISSING, VIEW_ROWS);
            insert("d", 5.0, "2024-01-03T00:10:00.000000Z");
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-01T01:00:00.000000Z\tb\t2.0
                    2024-01-03T00:00:00.000000Z\td\t5.0
                    """);
        });
    }

    @Test
    public void testResumeTelemetryReportsMode() throws Exception {
        // MAT_VIEW_RESUME holds the view's refresh position after RESUME and the mode: catch-up, skip backlog, or
        // skip backlog with the cause of a lost backlog.
        assertMemoryLeak(() -> {
            try (TelemetryJob telemetryJob = new TelemetryJob(engine)) {
                telemetryJob.runSerially();
                currentMicros = parseFloorPartialTimestamp("2099-01-01T00:00:00.000000Z");
                createBaseWithRefreshedView("IMMEDIATE");
                invalidate("test invalidation");
                insert("a", 3.0, "2024-01-01T00:20:00.000000Z");
                drainWalQueue();
                assertResumeSucceeds(RESUME_SQL);
                drainWalAndMatViewQueues();
                // the telemetry job stamps the events it writes with its clock
                telemetryJob.runSerially();

                currentMicros += Micros.SECOND_MICROS;
                invalidate("test invalidation");
                assertResumeSucceeds(RESUME_SKIP_SQL);
                telemetryJob.runSerially();

                currentMicros += Micros.SECOND_MICROS;
                invalidate("test invalidation");
                insert("c", 4.0, "2024-01-02T00:10:00.000000Z");
                drainWalAndMatViewQueues();
                removeBaseWalEventFiles();
                assertResumeRefused(MatViewBacklog.LOST_BASE_TABLE_WAL_MISSING);
                assertResumeSucceeds(RESUME_SKIP_SQL);
                telemetryJob.runSerially();
                assertQuery("SELECT base_table_txn, invalidation_reason FROM sys.telemetry_mat_view WHERE event = 205 AND view_table_id = "
                        + viewToken().getTableId() + " AND created >= '2099-01-01' ORDER BY created")
                        .noLeakCheck()
                        .returns("""
                                base_table_txn\tinvalidation_reason
                                1\tcatch-up
                                2\tskip backlog
                                3\tskip backlog (lost: base table WAL missing)
                                """);
            }
        });
    }

    @Test
    public void testResumeWaitsForBacklogUpdateHoldingLatch() throws Exception {
        // RESUME issued while a backlog cache update holds the view latch waits for it, without "retry later", and
        // catches up.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            invalidate("test invalidation");
            insertGapRows();
            engine.getMatViewStateStore().enqueueUpdateRefreshIntervals(viewToken());

            final CountDownLatch isUpdateHoldingLatch = new CountDownLatch(1);
            final CountDownLatch isResumeWaiting = new CountDownLatch(1);
            viewState().setOnResumeLatchBusyForTesting(isResumeWaiting::countDown);
            final AtomicReference<Throwable> updaterError = new AtomicReference<>();
            final Thread updater = new Thread(() -> {
                try (MatViewRefreshJob job = new MatViewRefreshJob(1, engine, 0)) {
                    job.setOnHoldingLockForTesting(() -> {
                        isUpdateHoldingLatch.countDown();
                        await(isResumeWaiting);
                    });
                    drainMatViewQueue(job);
                    // the view WAL transaction of the cache update, which RESUME waits for
                    drainWalQueue();
                } catch (Throwable th) {
                    updaterError.set(th);
                } finally {
                    isUpdateHoldingLatch.countDown();
                    Path.clearThreadLocals();
                }
            });
            updater.start();
            try {
                await(isUpdateHoldingLatch);
                execute(RESUME_SQL);
            } finally {
                isResumeWaiting.countDown();
                updater.join();
            }
            Assert.assertNull(updaterError.get());
            Assert.assertEquals(0, isResumeWaiting.getCount());
            Assert.assertFalse(viewState().isInvalid());
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows(CAUGHT_UP_ROWS);
        });
    }

    @Test
    public void testResumeWalBeforePurgeKeepsBacklog() throws Exception {
        // A suspended view WAL holds no backlog update, but when RESUME WAL comes before the purge, the base's WAL
        // from the cache position is still in place and RESUME catches up.
        setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_INTERVALS_UPDATE_PERIOD, "1m");
        assertMemoryLeak(() -> {
            currentMicros = parseFloorPartialTimestamp("2024-02-01T00:00:00.000000Z");
            createBaseWithRefreshedView("IMMEDIATE");
            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
            drainMatViewTimerQueue(timerJob);
            invalidate("test invalidation");
            execute("ALTER MATERIALIZED VIEW price_1h SUSPEND WAL");
            insertGapRows();
            // the timer fires, and the backlog update skips the suspended view
            currentMicros += Micros.MINUTE_MICROS;
            drainMatViewTimerQueue(timerJob);
            drainWalAndMatViewQueues();
            assertCache(-1);
            execute("ALTER MATERIALIZED VIEW price_1h RESUME WAL");
            drainWalAndMatViewQueues();

            assertResumeSucceeds(RESUME_SQL);
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows(CAUGHT_UP_ROWS);
        });
    }

    @Test
    public void testSkipBacklogJumpsAndClearsCache() throws Exception {
        // RESUME SKIP BACKLOG drops the backlog and jumps to the base's latest applied transaction: the rows the base
        // received while the view was invalid reach the view only through REFRESH ... RANGE.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            invalidate("test invalidation");
            insertGapRows();
            updateBacklog();
            assertCache(
                    3,
                    "2024-01-01T00:20:00.000000Z", "2024-01-01T00:20:00.000000Z",
                    "2024-01-02T00:10:00.000000Z", "2024-01-02T00:10:00.000000Z"
            );
            assertResumeSucceeds(RESUME_SKIP_SQL);
            assertRefreshBaseTxn(3);
            assertCache(-1);
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows(VIEW_ROWS);
            restart();
            assertCache(-1);
            execute("REFRESH MATERIALIZED VIEW price_1h RANGE FROM '2024-01-01' TO '2024-01-03'");
            drainWalAndMatViewQueues();
            assertViewRows(CAUGHT_UP_ROWS);
        });
    }

    @Test
    public void testSkipBacklogSyntax() throws Exception {
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            invalidate("test invalidation");
            assertExceptionNoLeakCheck("REFRESH MATERIALIZED VIEW price_1h RESUME SKIP", 46, "'backlog' expected");
            assertExceptionNoLeakCheck("REFRESH MATERIALIZED VIEW price_1h RESUME SKIP;", 46, "'backlog' expected");
            assertExceptionNoLeakCheck("REFRESH MATERIALIZED VIEW price_1h RESUME SKIP backlogs", 47, "'backlog' expected");
            assertExceptionNoLeakCheck("REFRESH MATERIALIZED VIEW price_1h RESUME BACKLOG", 42, "unexpected token [BACKLOG] while trying to refresh materialized view");
            assertExceptionNoLeakCheck("REFRESH MATERIALIZED VIEW price_1h RESUME SKIP BACKLOG now", 55, "unexpected token [now] while trying to refresh materialized view");
            assertExceptionNoLeakCheck("REFRESH MATERIALIZED VIEW price_1h FULL SKIP BACKLOG", 40, "unexpected token [SKIP] while trying to refresh materialized view");
            assertExceptionNoLeakCheck("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL SKIP BACKLOG", 47, "unexpected token [SKIP] while trying to refresh materialized view");
            // four-letter words that are not SKIP
            for (String word : new String[]{"WAIT", "SAAA", "SKAP", "SKIM"}) {
                assertExceptionNoLeakCheck("REFRESH MATERIALIZED VIEW price_1h RESUME " + word + " BACKLOG", 42, "unexpected token [" + word + "] while trying to refresh materialized view");
            }
            assertExceptionNoLeakCheck("REFRESH MATERIALIZED VIEW price_1h SKIP BACKLOG", 35, "'full', 'incremental', 'range', 'resume' or 'stats' expected");
            assertViewStatus("invalid", "test invalidation");
            // keywords are case-insensitive, and a comment and a semicolon may follow
            assertResumeSucceeds("refresh materialized view price_1h resume skip /* the gap */ backlog;");
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            invalidate("test invalidation");
            assertResumeSucceeds("REFRESH MATERIALIZED VIEW price_1h RESUME;");
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
        });
    }

    @Test
    public void testSuspendedViewRestartAfterPurgeFindsBacklogLost() throws Exception {
        // The suspended WAL of an invalid view refused the view's ATTACH range task, which lost the backlog in memory,
        // and the lost backlog no longer holds back the base's WAL purging after RESUME WAL. A restart forgets the
        // marker, which no view state event carried: a known defect of its own, "after a restart, RESUME catches up
        // without an UPDATE/ATTACH range whose failed task marked the backlog lost in memory", which this test does not
        // assert. Once the purge removed the base's WAL from the view's cache position, RESUME after the restart finds
        // the backlog lost again and refuses.
        setProperty(PropertyKey.DEBUG_MAT_VIEW_REFRESH_MISSING_WAL_FILES_FATAL, "false");
        setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, "true");
        assertMemoryLeak(() -> {
            createAttachableBaseWithView();
            invalidate("test invalidation");
            execute("ALTER MATERIALIZED VIEW price_1h SUSPEND WAL");
            execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-01'");
            drainWalAndMatViewQueues();
            Assert.assertEquals(MatViewBacklog.LOST_UPDATE_FAILED, viewState().getBacklogLostReason());
            execute("ALTER MATERIALIZED VIEW price_1h RESUME WAL");
            drainWalAndMatViewQueues();
            for (int hour = 1; hour < 4; hour++) {
                // each commit goes through a WAL writer of its own, whose segment the purge can remove
                engine.releaseInactive();
                insert("c", 4.0 + hour, "2024-01-02T0" + hour + ":20:00.000000Z");
                drainWalAndMatViewQueues();
            }
            purgeBaseWal();
            // the ATTACH is base txn 3, and the inserts 4 to 6
            for (long baseTxn = 3; baseTxn < 7; baseTxn++) {
                Assert.assertFalse(isBaseWalSegmentPresent(baseTxn));
            }

            restart();
            assertViewStatus("invalid", "test invalidation");
            assertLostThenSkipBacklogJumps(MatViewBacklog.LOST_BASE_TABLE_WAL_MISSING, ATTACHABLE_VIEW_ROWS);
        });
    }

    @Test
    public void testSuspendedViewWalReleasesFloor() throws Exception {
        // A suspended view cannot persist its backlog, so it releases the base's WAL floor: the base's WAL above the
        // view's cache position is purged.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            invalidate("test invalidation");
            engine.releaseInactive();
            insert("a", 3.0, "2024-01-01T00:20:00.000000Z");
            drainWalAndMatViewQueues();
            purgeBaseWal();
            // the backlog holds the floor at the cache position, before the insert
            Assert.assertFalse(isBaseWalSegmentPresent(1));
            Assert.assertTrue(isBaseWalSegmentPresent(2));
            execute("ALTER MATERIALIZED VIEW price_1h SUSPEND WAL");
            purgeBaseWal();
            Assert.assertFalse(isBaseWalSegmentPresent(2));
            assertCache(-1);
        });
    }

    @Test
    public void testTruncateWhileInvalidCutsBacklog() throws Exception {
        // A base TRUNCATE while the view is invalid drops the backlog collected before it: the base no longer holds
        // those rows, and recomputing their buckets after a later write recreated the partition would delete the
        // view's rows there.
        assertMemoryLeak(() -> {
            for (boolean isCachedBeforeTruncate : new boolean[]{false, true}) {
                createBaseWithRefreshedView("IMMEDIATE");
                invalidate("test invalidation");
                insert("a", 3.0, "2024-01-01T00:20:00.000000Z");
                drainWalAndMatViewQueues();
                if (isCachedBeforeTruncate) {
                    updateBacklog();
                    assertCache(2, "2024-01-01T00:20:00.000000Z", "2024-01-01T00:20:00.000000Z");
                }
                execute("TRUNCATE TABLE base");
                insert("d", 5.0, "2024-01-01T05:10:00.000000Z");
                insert("c", 4.0, "2024-01-02T00:10:00.000000Z");
                drainWalAndMatViewQueues();
                updateBacklog();
                assertCache(
                        baseSeqTxn(),
                        "2024-01-01T05:10:00.000000Z", "2024-01-01T05:10:00.000000Z",
                        "2024-01-02T00:10:00.000000Z", "2024-01-02T00:10:00.000000Z"
                );

                assertResumeSucceeds(RESUME_SQL);
                drainWalAndMatViewQueues();
                assertViewStatus("valid", "");
                assertViewRows("""
                        ts\tsym\tprice
                        2024-01-01T00:00:00.000000Z\ta\t1.0
                        2024-01-01T01:00:00.000000Z\tb\t2.0
                        2024-01-01T05:00:00.000000Z\td\t5.0
                        2024-01-02T00:00:00.000000Z\tc\t4.0
                        """);
                execute("DROP MATERIALIZED VIEW price_1h");
                execute("DROP TABLE base");
                drainWalAndMatViewQueues();
            }
        });
    }

    @Test
    public void testUpdateDroppedByLaterTruncateWhileInvalidKeepsScannedBacklog() throws Exception {
        // A TRUNCATE and a row apply after a base UPDATE and before the UPDATE's range task runs. The task brings the
        // backlog up to the row, which the TRUNCATE cut leaves as the only interval, and drops the UPDATE's range,
        // which the truncate emptied. It persists the backlog it scanned in one view state event, so a restart finds
        // the same backlog, and RESUME recomputes only the row's bucket.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            invalidate("test invalidation");
            final long viewTxn = viewSeqTxn();
            execute("UPDATE base SET price = 9.0 WHERE sym = 'a'");
            drainWalQueue();
            execute("TRUNCATE TABLE base");
            insert("c", 4.0, "2024-01-01T05:10:00.000000Z");
            drainWalQueue();
            drainWalAndMatViewQueues();
            Assert.assertEquals(viewTxn + 1, viewSeqTxn());
            assertViewStatus("invalid", "test invalidation");
            assertCache(baseSeqTxn(), "2024-01-01T05:10:00.000000Z", "2024-01-01T05:10:00.000000Z");
            assertPersistedCache(baseSeqTxn(), "2024-01-01T05:10:00.000000Z", "2024-01-01T05:10:00.000000Z");

            assertResumeSucceeds(RESUME_SQL);
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-01T01:00:00.000000Z\tb\t2.0
                    2024-01-01T05:00:00.000000Z\tc\t4.0
                    """);
        });
    }

    @Test
    public void testUpdateWhileInvalidJoinsBacklog() throws Exception {
        // The WAL scan does not see a base UPDATE, which commits as a SQL transaction; the WAL apply job queues the
        // range of the partitions it rewrote, and an invalid view takes it into its backlog.
        // Each UPDATE's range task brings the backlog up to the UPDATE and adds its range in one view state event.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            invalidate("test invalidation");
            for (String price : new String[]{"8.0", "9.0"}) {
                final long viewTxn = viewSeqTxn();
                execute("UPDATE base SET price = " + price + " WHERE sym = 'a'");
                drainWalAndMatViewQueues();
                Assert.assertEquals(viewTxn + 1, viewSeqTxn());
            }
            assertViewStatus("invalid", "test invalidation");
            assertCache(baseSeqTxn(), "2024-01-01T00:00:00.000000Z", "2024-01-01T23:59:59.999999Z");
            assertPersistedCache(baseSeqTxn(), "2024-01-01T00:00:00.000000Z", "2024-01-01T23:59:59.999999Z");

            assertResumeSucceeds(RESUME_SQL);
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t9.0
                    2024-01-01T01:00:00.000000Z\tb\t2.0
                    """);
        });
    }

    private static void await(CountDownLatch latch) {
        try {
            Assert.assertTrue("latch timeout", latch.await(30, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    // Arms the failure of the view's next WAL transaction: with one transaction per sequencer part, the transaction
    // opens a part of its own.
    private static void armViewStateEventFailure(AtomicBoolean isFailArmed, StringSink failingPart) {
        failingPart.clear();
        failingPart.put(viewToken().getDirName()).put(Files.SEPARATOR).put(WalUtils.SEQ_DIR).put(Files.SEPARATOR)
                .put(WalUtils.TXNLOG_PARTS_DIR).put(Files.SEPARATOR).put(viewSeqTxn());
        isFailArmed.set(true);
    }

    private static long baseSeqTxn() {
        return engine.getTableSequencerAPI().getTxnTracker(baseToken()).getSeqTxn();
    }

    private static TableToken baseToken() {
        return engine.verifyTableName("base");
    }

    // Whether the base's WAL segment that holds the transaction is on disk.
    private static boolean isBaseWalSegmentPresent(long seqTxn) {
        try (TransactionLogCursor cursor = engine.getTableSequencerAPI().getCursor(baseToken(), seqTxn - 1); Path path = new Path()) {
            Assert.assertTrue(cursor.hasNext());
            Assert.assertEquals(seqTxn, cursor.getTxn());
            path.of(configuration.getDbRoot()).concat(baseToken()).concat(WAL_NAME_BASE).put(cursor.getWalId()).slash().put(cursor.getSegmentId());
            return Files.exists(path.$());
        }
    }

    // Drops the invalidation that the drop of the base table queued, as a crash would.
    private static void dropQueuedInvalidation() {
        final MatViewRefreshTask task = new MatViewRefreshTask();
        Assert.assertTrue(engine.getMatViewStateStore().tryDequeueRefreshTask(task));
        Assert.assertEquals(MatViewRefreshTask.INVALIDATE, task.operation);
        Assert.assertFalse(engine.getMatViewStateStore().tryDequeueRefreshTask(task));
    }

    // Runs the statement from a refresh job's test seam, which takes no checked exception.
    private static void executeInSeam(String sql) {
        try {
            execute(sql);
        } catch (SqlException e) {
            throw new AssertionError(e);
        }
    }

    private static void makeAttachable(String partition) {
        try (Path path = new Path(); Path other = new Path()) {
            path.of(configuration.getDbRoot()).concat(baseToken()).concat(partition).put(TableUtils.DETACHED_DIR_MARKER).$();
            other.of(configuration.getDbRoot()).concat(baseToken()).concat(partition).put(configuration.getAttachPartitionSuffix()).$();
            Assert.assertTrue(Files.rename(path.$(), other.$()) > -1);
        }
    }

    // Fails, once armed each, the opening of an event file of the base's WAL, which a scan of the base's WAL reads,
    // and the opening of the part of the view's sequencer that failingPart names.
    private static TestFilesFacadeImpl newBaseWalReadAndSeqPartFailingFacade(
            AtomicBoolean isScanFailArmed,
            AtomicBoolean isEventFailArmed,
            StringSink failingPart
    ) {
        return new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (isScanFailArmed.get()
                        && Utf8s.containsAscii(name, "base~")
                        && Utf8s.endsWithAscii(name, Files.SEPARATOR + EVENT_FILE_NAME)) {
                    isScanFailArmed.set(false);
                    return -1;
                }
                return super.openRO(name);
            }

            @Override
            public long openRW(LPSZ name, int opts) {
                if (isEventFailArmed.get() && failingPart.length() > 0 && Utf8s.endsWithAscii(name, failingPart)) {
                    isEventFailArmed.set(false);
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
    }

    // Fails, once armed, the opening of the part of the view's sequencer that failingPart names.
    private static TestFilesFacadeImpl newSeqPartFailingFacade(AtomicBoolean isFailArmed, StringSink failingPart) {
        return new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (isFailArmed.get() && failingPart.length() > 0 && Utf8s.endsWithAscii(name, failingPart)) {
                    isFailArmed.set(false);
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
    }

    // Fails, once armed, the opening of the event file of a new WAL writer of the view.
    private static TestFilesFacadeImpl newViewWalOpenFailingFacade(AtomicBoolean isFailArmed) {
        return new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (isFailArmed.get()
                        && Utf8s.containsAscii(name, "price_1h~")
                        && Utf8s.endsWithAscii(name, Files.SEPARATOR + EVENT_FILE_NAME)) {
                    isFailArmed.set(false);
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
    }

    // Publishes the invalidation of a change of the base table at the base txn as the bare-name guard does: a marker that
    // names the base table and the txn and does not force, which the last FULL refresh covers when its snapshot includes
    // the txn, then the INVALIDATE task, which waits in the refresh queue behind the range tasks queued before it.
    private static void publishBaseTableInvalidation(String reason, long baseTxn) {
        viewState().markAsPendingInvalidationForTesting(reason, baseToken(), baseTxn, false);
        engine.getMatViewStateStore().enqueueInvalidate(viewToken(), reason, baseToken(), baseTxn, false);
    }

    // Publishes a forced invalidation of the view as an apply-time invalidation does: the marker first, then the
    // INVALIDATE task, which waits in the refresh queue behind the range tasks queued before it.
    private static void publishInvalidation(String reason) {
        viewState().markAsPendingInvalidation(reason);
        engine.getMatViewStateStore().enqueueInvalidate(viewToken(), reason);
    }

    // Under the ADAPTIVE commit mode the base WAL purge floor is held back to the last save point; with the test clock
    // frozen no save point is taken, so the purge takes one over the applied tail first.
    private static void purgeBaseWal() {
        engine.releaseInactive();
        if (engine.getConfiguration().getCommitMode() == CommitMode.ADAPTIVE) {
            try (TableWriter writer = getWriter(baseToken())) {
                writer.advanceDurableEpoch(engine.getConfiguration().getMicrosecondClock().getTicks() / 1000L);
            }
        }
        engine.releaseInactiveTableSequencers();
        drainPurgeJob();
    }

    // Removes the event files of the base's WAL segments, so that the backlog cannot be read from the WAL.
    private static void removeBaseWalEventFiles() {
        engine.releaseInactive();
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Path path = new Path()) {
            for (int walId = 1; walId < 8; walId++) {
                for (int segmentId = 0; segmentId < 4; segmentId++) {
                    path.of(engine.getConfiguration().getDbRoot()).concat(baseToken())
                            .concat(WAL_NAME_BASE).put(walId).slash().put(segmentId).concat(EVENT_FILE_NAME);
                    ff.removeQuiet(path.$());
                }
            }
        }
    }

    // A restart of the view states: the store reloads them from disk, as at startup.
    private static void restart() {
        engine.getMatViewStateStore().clear();
        engine.hydrateMatViewStateStore();
    }

    // Runs the view's queued range task and suspends the view's WAL once the task holds the latch and the view's WAL
    // writer.
    private static void runRangeTaskSuspendingWhileHolding() {
        final AtomicBoolean isArmed = new AtomicBoolean(true);
        try (MatViewRefreshJob job = new MatViewRefreshJob(0, engine, 0)) {
            job.setOnHoldingLockForTesting(() -> {
                if (isArmed.compareAndSet(true, false)) {
                    executeInSeam("ALTER MATERIALIZED VIEW price_1h SUSPEND WAL");
                }
            });
            drainMatViewQueue(job);
        }
        Assert.assertFalse(isArmed.get());
        Assert.assertFalse(viewState().isLocked());
    }

    // Takes the view's range task off the refresh queue and puts the other tasks back, so that they run before it.
    private static MatViewRefreshTask takeRangeTask() {
        final MatViewStateStore stateStore = engine.getMatViewStateStore();
        final ObjList<MatViewRefreshTask> otherTasks = new ObjList<>();
        MatViewRefreshTask rangeTask = null;
        while (true) {
            final MatViewRefreshTask task = new MatViewRefreshTask();
            if (!stateStore.tryDequeueRefreshTask(task)) {
                break;
            }
            if (task.operation == MatViewRefreshTask.ADD_REFRESH_RANGE) {
                Assert.assertNull("one range task expected", rangeTask);
                rangeTask = task;
            } else {
                otherTasks.add(task);
            }
        }
        Assert.assertNotNull("range task expected", rangeTask);
        for (int i = 0, n = otherTasks.size(); i < n; i++) {
            stateStore.reenqueueRefreshTask(otherTasks.getQuick(i));
        }
        return rangeTask;
    }

    // Runs a backlog cache update, as the refresh intervals update timer enqueues it.
    private static void updateBacklog() {
        engine.getMatViewStateStore().enqueueUpdateRefreshIntervals(viewToken());
        drainWalAndMatViewQueues();
    }

    private static long viewSeqTxn() {
        return engine.getTableSequencerAPI().getTxnTracker(viewToken()).getSeqTxn();
    }

    private static MatViewState viewState() {
        return engine.getMatViewStateStore().getViewState(viewToken());
    }

    private static TableToken viewToken() {
        return engine.verifyTableName("price_1h");
    }

    // The base WAL that an invalid view's backlog needs stays until a cache update absorbs it, and the cache position
    // keeps moving over several update periods while the base keeps committing.
    private void assertBaseWalPurgedUpToCachePosition(String refreshType) throws Exception {
        setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_INTERVALS_UPDATE_PERIOD, "5s");
        assertMemoryLeak(() -> {
            currentMicros = parseFloorPartialTimestamp("2024-02-01T00:00:00.000000Z");
            createBaseWithRefreshedView(refreshType);
            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
            drainMatViewTimerQueue(timerJob);
            drainWalAndMatViewQueues();
            invalidate("test invalidation");
            for (int round = 0; round < 3; round++) {
                engine.releaseInactive();
                insert("c", round, "2024-01-0" + (round + 2) + "T00:10:00.000000Z");
                drainWalAndMatViewQueues();
                final long baseSeqTxn = baseSeqTxn();
                purgeBaseWal();
                // the cache position is the previous round's base txn: the WAL up to it is purged, and the WAL of
                // the new commit, which each round writes through a new WAL writer, stays
                Assert.assertFalse(isBaseWalSegmentPresent(baseSeqTxn - 1));
                Assert.assertTrue(isBaseWalSegmentPresent(baseSeqTxn));

                final long viewSeqTxn = viewSeqTxn();
                currentMicros += 5 * Micros.SECOND_MICROS;
                drainMatViewTimerQueue(timerJob);
                drainWalAndMatViewQueues();
                Assert.assertEquals(baseSeqTxn, viewState().getRefreshIntervalsBaseTxn());
                Assert.assertEquals(viewSeqTxn + 1, viewSeqTxn());
                purgeBaseWal();
                Assert.assertFalse(isBaseWalSegmentPresent(baseSeqTxn));
            }
            assertViewStatus("invalid", "test invalidation");
            assertRefreshBaseTxn(1);

            // the backlog absorbed the purged WAL, so RESUME catches up on it
            assertResumeSucceeds(RESUME_SQL);
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-01T01:00:00.000000Z\tb\t2.0
                    2024-01-02T00:00:00.000000Z\tc\t0.0
                    2024-01-03T00:00:00.000000Z\tc\t1.0
                    2024-01-04T00:00:00.000000Z\tc\t2.0
                    """);
        });
    }

    // Asserts the view's cached refresh intervals, the backlog of an invalid view, as pairs of timestamps.
    private void assertCache(long baseTxn, String... bounds) {
        Assert.assertEquals(baseTxn, viewState().getRefreshIntervalsBaseTxn());
        final LongList expected = new LongList();
        for (String bound : bounds) {
            expected.add(parseFloorPartialTimestamp(bound));
        }
        TestUtils.assertEquals(expected, viewState().getRefreshIntervals());
    }

    // A failed range task left the view invalid with its backlog lost. The lost backlog takes no cache update,
    // REFRESH ... INCREMENTAL sends the user to RESUME, and RESUME refuses with the cause and writes no view state;
    // RESUME SKIP BACKLOG and a REFRESH ... RANGE over the gap then bring the view to its SQL on the base. The base
    // commit here adds the row 2024-01-02T01:20 c 5.0.
    private void assertFailedRangeTaskLostThenRecovered(String reasonPart, String viewRows) throws Exception {
        Assert.assertTrue(viewState().isInvalid());
        final String reason = viewState().getInvalidationReasonForReport();
        TestUtils.assertContains(reason, reasonPart);
        Assert.assertEquals(MatViewBacklog.LOST_UPDATE_FAILED, viewState().getBacklogLostReason());
        final long viewSeqTxn = viewSeqTxn();
        insert("c", 5.0, "2024-01-02T01:20:00.000000Z");
        drainWalAndMatViewQueues();
        updateBacklog();
        Assert.assertEquals(viewSeqTxn, viewSeqTxn());
        assertExceptionNoLeakCheck(
                "REFRESH MATERIALIZED VIEW price_1h INCREMENTAL",
                VIEW_NAME_POSITION,
                "materialized view is invalid, run REFRESH MATERIALIZED VIEW price_1h RESUME first [reason=" + reason + ']'
        );
        assertResumeRefused(MatViewBacklog.LOST_UPDATE_FAILED);
        Assert.assertEquals(viewSeqTxn, viewSeqTxn());

        assertResumeSucceeds(RESUME_SKIP_SQL);
        assertCache(-1);
        assertRefreshBaseTxn(engine.getTableSequencerAPI().getTxnTracker(baseToken()).getWriterTxn());
        drainWalAndMatViewQueues();
        assertViewStatus("valid", "");
        execute("REFRESH MATERIALIZED VIEW price_1h RANGE FROM '2024-01-01' TO '2024-01-03'");
        drainWalAndMatViewQueues();
        assertViewRows(viewRows);
        assertSqlCursors(VIEW_SQL, "price_1h");
        restart();
        assertViewStatus("valid", "");
        Assert.assertNull(viewState().getBacklogLostReason());
    }

    // RESUME refuses the lost backlog with its cause and writes no view state; RESUME SKIP BACKLOG then jumps to the
    // base's latest applied transaction, keeping the view's rows.
    private void assertLostThenSkipBacklogJumps(String cause, String viewRows) throws Exception {
        final String reason = viewState().getInvalidationReasonForReport();
        final long viewSeqTxn = viewSeqTxn();
        assertResumeRefused(cause);
        Assert.assertEquals(viewSeqTxn, viewSeqTxn());
        assertViewStatus("invalid", reason);
        Assert.assertEquals(cause, viewState().getBacklogLostReason());
        assertResumeRefused(cause);
        Assert.assertEquals(viewSeqTxn, viewSeqTxn());
        // a lost backlog takes no cache update
        updateBacklog();
        Assert.assertEquals(viewSeqTxn, viewSeqTxn());

        assertResumeSucceeds(RESUME_SKIP_SQL);
        Assert.assertNull(viewState().getBacklogLostReason());
        assertCache(-1);
        final long baseSeqTxn = engine.getTableSequencerAPI().getTxnTracker(baseToken()).getWriterTxn();
        assertRefreshBaseTxn(baseSeqTxn);
        Assert.assertEquals(baseToken().getTableId(), viewState().getBaseTableId());
        drainWalAndMatViewQueues();
        assertViewStatus("valid", "");
        assertViewRows(viewRows);
        restart();
        assertViewStatus("valid", "");
        Assert.assertNull(viewState().getBacklogLostReason());
    }

    // The base is dropped while the view is invalid, and a table the view cannot refresh from takes its name: the next
    // backlog update loses the backlog, and RESUME refuses that table.
    private void assertLostWhenBaseReplacedByUnusableTable(String createReplacement, String dropReplacement) throws Exception {
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("MANUAL");
            invalidate("test invalidation");
            execute("CREATE TABLE other (sym SYMBOL, price DOUBLE, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("DROP TABLE base");
            drainWalAndMatViewQueues();
            execute(createReplacement);
            drainWalAndMatViewQueues();
            final long viewSeqTxn = viewSeqTxn();
            updateBacklog();
            assertViewStatus("invalid", "test invalidation");
            Assert.assertEquals(MatViewBacklog.LOST_BASE_TABLE_REPLACED, viewState().getBacklogLostReason());
            Assert.assertEquals(viewSeqTxn + 1, viewSeqTxn());
            try {
                execute(RESUME_SQL);
                Assert.fail("RESUME must refuse a base the view cannot refresh from");
            } catch (SqlException e) {
                Assert.assertEquals(VIEW_NAME_POSITION, e.getPosition());
            }
            Assert.assertTrue(viewState().isInvalid());
            Assert.assertFalse(viewState().isLocked());
            execute(dropReplacement);
            drainWalAndMatViewQueues();
        });
    }

    // Asserts the view's cached refresh intervals and their base txn in the view's state file, as pairs of timestamps.
    private void assertPersistedCache(long baseTxn, String... bounds) {
        final LongList expected = new LongList();
        for (String bound : bounds) {
            expected.add(parseFloorPartialTimestamp(bound));
        }
        try (Path path = new Path(); BlockFileReader reader = new BlockFileReader(configuration)) {
            path.of(configuration.getDbRoot()).concat(viewToken()).concat(MatViewState.MAT_VIEW_STATE_FILE_NAME);
            reader.of(path.$());
            final MatViewStateReader stateReader = new MatViewStateReader().of(reader, viewToken());
            Assert.assertEquals(baseTxn, stateReader.getRefreshIntervalsBaseTxn());
            TestUtils.assertEquals(expected, stateReader.getRefreshIntervals());
        }
    }

    private void assertRefreshBaseTxn(long expected) {
        Assert.assertEquals(expected, viewState().getLastRefreshBaseTxn());
    }

    private void assertResumeRefused(String cause) throws Exception {
        assertExceptionNoLeakCheck(
                RESUME_SQL,
                VIEW_NAME_POSITION,
                "materialized view backlog is lost [reason=" + cause + "]; RESUME cannot catch up on base changes made while"
                        + " the view was invalid. Run REFRESH MATERIALIZED VIEW price_1h RESUME SKIP BACKLOG, then REFRESH ... RANGE over the gap"
        );
        Assert.assertTrue(viewState().isInvalid());
        Assert.assertFalse(viewState().isLocked());
    }

    // RESUME returns once the view state event is in the view's WAL; materialized_views() reports the state the
    // view's WAL applied, so the WAL applies it before the caller looks.
    private void assertResumeSucceeds(String sql) throws SqlException {
        execute(sql);
        Assert.assertFalse(viewState().isInvalid());
        Assert.assertFalse(viewState().isLocked());
        Assert.assertNull(viewState().getBacklogLostReason());
        drainWalQueue();
    }

    private void assertViewRows(String expected) throws Exception {
        assertQuery("price_1h ORDER BY ts, sym").timestamp("ts").noLeakCheck().expectSize().returns(expected);
    }

    private void assertViewStatus(String status, String reason) throws Exception {
        assertQuery("SELECT view_status, invalidation_reason FROM materialized_views() WHERE view_name = 'price_1h'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("view_status\tinvalidation_reason\n" + status + "\t" + reason + "\n");
    }

    // A base whose first day, 2024-01-01 a 1.0, is detached and attachable, and whose second day holds
    // 2024-01-02 c 3.0: base txns 1 (the rows) and 2 (the detach).
    private void createAttachableBase() throws Exception {
        createBase();
        execute("""
                INSERT INTO base VALUES
                ('a', 1.0, 1.0, '2024-01-01T00:10:00.000000Z'),
                ('c', 3.0, 1.0, '2024-01-02T00:10:00.000000Z')
                """);
        drainWalQueue();
        execute("ALTER TABLE base DETACH PARTITION LIST '2024-01-01'");
        drainWalQueue();
        makeAttachable("2024-01-01");
    }

    // An attachable base, see createAttachableBase(), and a MANUAL view over it refreshed at base txn 2.
    private void createAttachableBaseWithView() throws Exception {
        createAttachableBaseWithView("MANUAL");
    }

    // An attachable base, see createAttachableBase(), and a view of the refresh type over it refreshed at base txn 2.
    private void createAttachableBaseWithView(String refreshType) throws Exception {
        createAttachableBase();
        createView(refreshType);
        drainWalQueue();
        execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
        drainWalAndMatViewQueues();
        assertViewRows(ATTACHABLE_VIEW_ROWS);
        assertViewStatus("valid", "");
        assertRefreshBaseTxn(2);
    }

    private void createBase() throws SqlException {
        execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    // A base with two rows and a view refreshed from them at base txn 1.
    private void createBaseWithRefreshedView(String refreshType) throws Exception {
        createBase();
        insertBaseRows();
        createView(refreshType);
        if (!"IMMEDIATE".equals(refreshType)) {
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
        }
        drainWalAndMatViewQueues();
        assertViewRows(VIEW_ROWS);
        assertViewStatus("valid", "");
        assertRefreshBaseTxn(1);
        Assert.assertEquals(baseToken().getTableId(), viewState().getBaseTableId());
    }

    // A MANUAL view refreshed from the base's rows and then invalidated, whose backlog is tracked.
    private MatViewState createRefreshedInvalidView() throws Exception {
        createView("MANUAL");
        drainWalQueue();
        execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
        drainWalAndMatViewQueues();
        invalidate("test invalidation");
        Assert.assertTrue(viewState().hasTrackedBacklog());
        return viewState();
    }

    // A TIMER period view with the given SQL over two base rows of 2024-01-01, built at 2024-01-02T12:00 as an EVERY
    // firing would: it holds the periods up to 2024-01-02, at base txn 1. The base holds no partition of 2024-01-02.
    private void createRefreshedPeriodTimerView(String viewSql) throws Exception {
        currentMicros = parseFloorPartialTimestamp("2024-01-01T00:00:00.000000Z");
        createBase();
        execute("CREATE MATERIALIZED VIEW price_1h REFRESH EVERY 1h PERIOD (LENGTH 1d) AS (" + viewSql + ") PARTITION BY DAY");
        execute("""
                INSERT INTO base VALUES
                ('a', 1.0, 1.0, '2024-01-01T00:10:00.000000Z'),
                ('b', 2.0, 1.0, '2024-01-01T05:10:00.000000Z')
                """);
        drainWalQueue();
        runIncrementalRefresh("2024-01-02T12:00:00.000000Z");
        assertViewRows(PERIOD_VIEW_ROWS);
        assertViewStatus("valid", "");
        assertRefreshBaseTxn(1);
        Assert.assertEquals(parseFloorPartialTimestamp("2024-01-02T00:00:00.000000Z"), viewState().getLastPeriodHi());
    }

    private void createView(String refreshType) throws SqlException {
        execute("CREATE MATERIALIZED VIEW price_1h REFRESH " + refreshType + " AS (" + VIEW_SQL + ") PARTITION BY DAY");
    }

    private void dropView() throws SqlException {
        execute("DROP MATERIALIZED VIEW price_1h");
        drainWalAndMatViewQueues();
    }

    // Attaches the base's first day and fails the view's ATTACH range task, with the latch held, with the error.
    private void failRangeTask(CairoException error) throws SqlException {
        final AtomicBoolean isArmed = new AtomicBoolean(true);
        execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-01'");
        drainWalQueue();
        try (MatViewRefreshJob job = new MatViewRefreshJob(0, engine, 0)) {
            job.setOnHoldingLockForTesting(() -> {
                if (isArmed.compareAndSet(true, false)) {
                    throw error;
                }
            });
            drainMatViewQueue(job);
        }
        drainWalQueue();
        Assert.assertFalse(isArmed.get());
        Assert.assertFalse(viewState().isLocked());
    }

    private void insert(String sym, double price, String ts) throws SqlException {
        execute("INSERT INTO base VALUES ('" + sym + "', " + price + ", 1.0, '" + ts + "')");
    }

    private void insertBaseRows() throws SqlException {
        execute("""
                INSERT INTO base VALUES
                ('a', 1.0, 1.0, '2024-01-01T00:10:00.000000Z'),
                ('b', 2.0, 1.0, '2024-01-01T01:10:00.000000Z')
                """);
    }

    // Two base commits while the view is invalid: a change to an existing bucket and a new bucket, base txns 2 and 3.
    private void insertGapRows() throws SqlException {
        insert("a", 3.0, "2024-01-01T00:20:00.000000Z");
        insert("c", 4.0, "2024-01-02T00:10:00.000000Z");
        drainWalAndMatViewQueues();
    }

    private void invalidate(String reason) {
        engine.getMatViewStateStore().enqueueInvalidate(viewToken(), reason);
        drainWalAndMatViewQueues();
        Assert.assertTrue(viewState().isInvalid());
        Assert.assertNull(viewState().getBacklogLostReason());
    }

    private MatViewStateReader readCheckpointedState() {
        return readCheckpointedState(viewToken());
    }

    private MatViewStateReader readCheckpointedState(TableToken viewToken) {
        try (Path path = new Path(); BlockFileReader reader = new BlockFileReader(configuration)) {
            path.of(configuration.getCheckpointRoot()).concat(configuration.getDbDirectory()).concat(viewToken)
                    .concat(MatViewState.MAT_VIEW_STATE_FILE_NAME);
            reader.of(path.$());
            return new MatViewStateReader().of(reader, viewToken);
        }
    }

    // Runs the incremental refresh of the view that its EVERY timer enqueues, at the given time.
    private void runIncrementalRefresh(String clock) {
        currentMicros = parseFloorPartialTimestamp(clock);
        engine.getMatViewStateStore().enqueueIncrementalRefresh(viewToken());
        drainWalAndMatViewQueues();
    }

    // Runs the range refresh of the view that the firing of MatViewTimerJob's period timer at the end of the period
    // enqueues, at that time.
    private void runPeriodRefresh(String periodHi) {
        currentMicros = parseFloorPartialTimestamp(periodHi);
        engine.getMatViewStateStore().enqueueRangeRefresh(viewToken(), Numbers.LONG_NULL, currentMicros - 1);
        drainWalAndMatViewQueues();
    }

    // Attaches the base's first day and runs the view's ATTACH range task, which the change hits between the task's
    // checks and its latch.
    private void runRangeTaskChangingViewBeforeLock(Runnable change) throws SqlException {
        final MatViewState state = viewState();
        final AtomicBoolean isArmed = new AtomicBoolean(true);
        execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-01'");
        drainWalQueue();
        try (MatViewRefreshJob job = new MatViewRefreshJob(0, engine, 0)) {
            job.setOnBeforeRefreshLockForTesting(() -> {
                if (isArmed.compareAndSet(true, false)) {
                    change.run();
                }
            });
            drainMatViewQueue(job);
        }
        Assert.assertFalse(isArmed.get());
        Assert.assertFalse(state.isLocked());
    }
}
