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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.mv.ForwardingMatViewStateStore;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.mv.MatViewRefreshTask;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.mv.MatViewStateReader;
import io.questdb.cairo.mv.MatViewStateStore;
import io.questdb.cairo.mv.MatViewStateStoreImpl;
import io.questdb.cairo.mv.MatViewTimerJob;
import io.questdb.cairo.mv.NoOpMatViewStateStore;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.Files;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.TableUtils.DETACHED_DIR_MARKER;

/**
 * Base and parent view operations that no longer invalidate dependent materialized views: DROP, DETACH and ATTACH
 * PARTITION, TRUNCATE, UPDATE, and a parent view that goes invalid, fails to refresh, rebuilds with FULL or drops a
 * partition. The views keep their rows; ATTACH and UPDATE queue the range they changed, which the incremental
 * refresh path recomputes; a refresh drops the intervals queued before a TRUNCATE. Live views keep their
 * invalidations.
 * <p>
 * Expected rows come from the base rows each test inserts: a bucket of the 1h views holds the last price of its
 * symbol's base rows within the hour, and the views keep the rows of buckets that no refresh recomputes.
 */
public class MatViewBaseOperationsTest extends AbstractCairoTest {
    private static final String AMOUNT_VIEW_SQL = "SELECT ts, sym, last(amount) AS amount FROM base SAMPLE BY 1h";
    private static final String CHILD_ROWS = """
            ts\tsym\tprice
            2024-01-01T00:00:00.000000Z\ta\t2.0
            2024-01-02T00:00:00.000000Z\ta\t3.0
            """;
    private static final String TRUNCATE_BASE_ROWS = """
            INSERT INTO base VALUES
            ('a', 1.0, '2024-01-01T00:10:00.000000Z'),
            ('b', 2.0, '2024-01-01T10:10:00.000000Z'),
            ('c', 3.0, '2024-01-02T00:10:00.000000Z')
            """;
    private static final String UPDATE_BASE_ROWS = """
            INSERT INTO base VALUES
            ('a', 1.0, '2024-01-01T00:10:00.000000Z'),
            ('b', 2.0, '2024-01-02T00:10:00.000000Z'),
            ('x', 7.0, '2024-01-02T10:10:00.000000Z'),
            ('c', 3.0, '2024-01-03T00:10:00.000000Z')
            """;
    private static final String UPDATE_VIEW_ROWS = """
            ts\tsym\tprice
            2024-01-01T00:00:00.000000Z\ta\t1.0
            2024-01-02T00:00:00.000000Z\tb\t2.0
            2024-01-02T10:00:00.000000Z\tx\t7.0
            2024-01-03T00:00:00.000000Z\tc\t3.0
            """;
    private static final String VIEW_ROWS_AFTER_ATTACH = """
            ts\tsym\tprice
            2024-01-01T00:00:00.000000Z\ta\t1.0
            2024-01-01T10:00:00.000000Z\tb\t2.0
            2024-01-02T00:00:00.000000Z\tc\t3.0
            """;
    // The rows from before the truncate stay, the interval of 'a' at 00:20 written before it is dropped, and the
    // bucket of 'd' written after it is recomputed.
    private static final String VIEW_ROWS_AFTER_TRUNCATE = """
            ts\tsym\tprice
            2024-01-01T00:00:00.000000Z\ta\t1.0
            2024-01-01T05:00:00.000000Z\td\t4.0
            2024-01-01T10:00:00.000000Z\tb\t2.0
            2024-01-02T00:00:00.000000Z\tc\t3.0
            """;
    private static final String VIEW_SQL = "SELECT ts, sym, last(price) AS price FROM base SAMPLE BY 1h";

    @Test
    public void testAttachAndTruncateInOneBatchQueueOnlyLaterRange() throws Exception {
        // ATTACH 2024-01-01, TRUNCATE and ATTACH 2024-01-03 apply in one batch. The TRUNCATE clears the range the
        // first ATTACH collected, so the batch queues one range task, for 2024-01-03, with the second ATTACH's
        // seqTxn. The view keeps its row of 2024-01-02, which the truncate removed from the base.
        assertMemoryLeak(() -> {
            createBase();
            execute("""
                    INSERT INTO base VALUES
                    ('a', 1.0, '2024-01-01T00:10:00.000000Z'),
                    ('c', 3.0, '2024-01-02T00:10:00.000000Z'),
                    ('e', 5.0, '2024-01-03T00:10:00.000000Z'),
                    ('f', 6.0, '2024-01-03T10:10:00.000000Z'),
                    ('g', 7.0, '2024-01-04T00:10:00.000000Z')
                    """);
            drainWalQueue();
            execute("ALTER TABLE base DETACH PARTITION LIST '2024-01-01', '2024-01-03'");
            drainWalQueue();
            makeAttachable("2024-01-01");
            makeAttachable("2024-01-03");
            createView("price_1h", "IMMEDIATE");
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-02T00:00:00.000000Z\tc\t3.0
                    2024-01-04T00:00:00.000000Z\tg\t7.0
                    """);

            final TableToken baseToken = engine.verifyTableName("base");
            final long seqTxnBeforeBatch = engine.getTableSequencerAPI().getTxnTracker(baseToken).getSeqTxn();
            execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-01'");
            execute("TRUNCATE TABLE base");
            execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-03'");
            drainWalQueue();
            Assert.assertEquals(seqTxnBeforeBatch + 3, engine.getTableSequencerAPI().getTxnTracker(baseToken).getWriterTxn());

            final ObjList<MatViewRefreshTask> tasks = dequeueTasks();
            int rangeTaskCount = 0;
            for (int i = 0, n = tasks.size(); i < n; i++) {
                final MatViewRefreshTask task = tasks.getQuick(i);
                if (task.operation == MatViewRefreshTask.ADD_REFRESH_RANGE) {
                    rangeTaskCount++;
                    Assert.assertEquals("price_1h", task.matViewToken.getTableName());
                    Assert.assertEquals(baseToken, task.baseTableToken);
                    Assert.assertEquals(parseFloorPartialTimestamp("2024-01-03T00:00:00.000000Z"), task.rangeFrom);
                    Assert.assertEquals(parseFloorPartialTimestamp("2024-01-04T00:00:00.000000Z") - 1, task.rangeTo);
                    Assert.assertEquals(seqTxnBeforeBatch + 3, task.baseTableTxn);
                }
            }
            Assert.assertEquals(1, rangeTaskCount);
            reenqueueTasks(tasks);
            drainWalAndMatViewQueues();

            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-02T00:00:00.000000Z\tc\t3.0
                    2024-01-03T00:00:00.000000Z\te\t5.0
                    2024-01-03T10:00:00.000000Z\tf\t6.0
                    2024-01-04T00:00:00.000000Z\tg\t7.0
                    """);
            assertViewValidAndCaughtUp("price_1h");
        });
    }

    @Test
    public void testAttachOfSeveralPartitionsQueuesTheirUnion() throws Exception {
        // One ATTACH of three partitions, listed out of order, queues one range from the floor of the oldest to the
        // ceiling of the newest.
        assertMemoryLeak(() -> {
            createBase();
            execute("""
                    INSERT INTO base VALUES
                    ('a', 1.0, '2024-01-01T00:10:00.000000Z'),
                    ('c', 3.0, '2024-01-02T00:10:00.000000Z'),
                    ('e', 5.0, '2024-01-03T00:10:00.000000Z'),
                    ('g', 7.0, '2024-01-04T00:10:00.000000Z')
                    """);
            drainWalQueue();
            execute("ALTER TABLE base DETACH PARTITION LIST '2024-01-01', '2024-01-02', '2024-01-03'");
            drainWalQueue();
            makeAttachable("2024-01-01");
            makeAttachable("2024-01-02");
            makeAttachable("2024-01-03");
            createView("price_1h", "MANUAL");
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();

            execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-03', '2024-01-01', '2024-01-02'");
            drainWalAndMatViewQueues();
            final MatViewState state = getViewState("price_1h");
            Assert.assertEquals(2, state.getRefreshIntervals().size());
            Assert.assertEquals(parseFloorPartialTimestamp("2024-01-01T00:00:00.000000Z"), state.getRefreshIntervals().getQuick(0));
            Assert.assertEquals(parseFloorPartialTimestamp("2024-01-04T00:00:00.000000Z") - 1, state.getRefreshIntervals().getQuick(1));

            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-02T00:00:00.000000Z\tc\t3.0
                    2024-01-03T00:00:00.000000Z\te\t5.0
                    2024-01-04T00:00:00.000000Z\tg\t7.0
                    """);
            assertCacheEmpty("price_1h");
            assertViewValidAndCaughtUp("price_1h");
        });
    }

    @Test
    public void testAttachPartOfListQueuesAttachedRange() throws Exception {
        // An ATTACH whose list names a partition that cannot be attached fails with a WAL-tolerable error after it
        // attached the partitions before it. The range of those still reaches the view.
        assertMemoryLeak(() -> {
            createBaseWithAttachableDay();
            createView("price_1h", "IMMEDIATE");
            drainWalAndMatViewQueues();

            execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-01', '2024-01-05'");
            drainWalAndMatViewQueues();
            assertQuery("SELECT suspended FROM wal_tables() WHERE name = 'base'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("suspended\nfalse\n");
            assertViewRows(VIEW_ROWS_AFTER_ATTACH);
            assertViewValidAndCaughtUp("price_1h");
        });
    }

    @Test
    public void testAttachRangeDeferredWhileViewLocked() throws Exception {
        // The range task finds the view lock held, puts itself back on the queue and runs once the lock is free.
        assertMemoryLeak(() -> {
            createBaseWithAttachableDay();
            createView("price_1h", "IMMEDIATE");
            drainWalAndMatViewQueues();

            execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-01'");
            drainWalQueue();
            final TableToken viewToken = engine.verifyTableName("price_1h");
            final MatViewState state = getViewState("price_1h");
            Assert.assertTrue(state.tryLock());
            try {
                drainMatViewQueue(engine);
                Assert.assertEquals(0, state.getRefreshIntervals().size());
            } finally {
                MatViewRefreshJob.finalizeAndUnlock(engine, engine.getMatViewStateStore(), viewToken, state, false);
            }
            drainWalAndMatViewQueues();

            assertViewRows(VIEW_ROWS_AFTER_ATTACH);
            assertCacheEmpty("price_1h");
            assertViewValidAndCaughtUp("price_1h");
        });
    }

    @Test
    public void testAttachRangeDroppedWhenBaseDroppedBeforeTaskRuns() throws Exception {
        // The base is dropped after the ATTACH applies and before its range task runs. No table has the base's name,
        // so the task adds nothing to the view's cache and writes nothing to the view's WAL; the base drop then
        // invalidates the view, as it always has.
        assertMemoryLeak(() -> {
            createBaseWithAttachableDay();
            createView("price_1h", "IMMEDIATE");
            drainWalAndMatViewQueues();

            execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-01'");
            drainWalQueue();
            final ObjList<MatViewRefreshTask> tasks = dequeueTasks();
            final MatViewRefreshTask rangeTask = removeRangeTask(tasks);
            execute("DROP TABLE base");
            drainWalQueue();
            tasks.addAll(dequeueTasks());

            final long viewTxns = walTxnCount("price_1h");
            final MatViewState state = getViewState("price_1h");
            engine.getMatViewStateStore().reenqueueRefreshTask(rangeTask);
            drainWalAndMatViewQueues();
            Assert.assertEquals(viewTxns, walTxnCount("price_1h"));
            Assert.assertEquals(0, state.getRefreshIntervals().size());
            Assert.assertFalse(state.isInvalid());

            reenqueueTasks(tasks);
            drainWalAndMatViewQueues();
            Assert.assertTrue(state.isInvalid());
        });
    }

    @Test
    public void testAttachRangeDroppedWhenBaseReCreatedBeforeTaskRuns() throws Exception {
        // The base is dropped and re-created under the same name after the ATTACH applies and before its range task
        // runs. The task belongs to the dropped table, so it adds nothing to the view's cache and writes nothing to
        // the view's WAL.
        assertMemoryLeak(() -> {
            createBaseWithAttachableDay();
            createView("price_1h", "IMMEDIATE");
            drainWalAndMatViewQueues();

            execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-01'");
            drainWalQueue();
            final ObjList<MatViewRefreshTask> attachTasks = dequeueTasks();
            final MatViewRefreshTask rangeTask = removeRangeTask(attachTasks);

            execute("DROP TABLE base");
            createBase();
            execute("INSERT INTO base VALUES ('z', 9.0, '2024-01-05T00:10:00.000000Z')");
            drainWalQueue();
            final ObjList<MatViewRefreshTask> dropTasks = dequeueTasks();

            final long viewTxns = walTxnCount("price_1h");
            final MatViewState state = getViewState("price_1h");
            engine.getMatViewStateStore().reenqueueRefreshTask(rangeTask);
            drainWalAndMatViewQueues();
            Assert.assertEquals(viewTxns, walTxnCount("price_1h"));
            Assert.assertEquals(0, state.getRefreshIntervals().size());
            Assert.assertFalse(state.isInvalid());
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-02T00:00:00.000000Z\tc\t3.0
                    """);

            // The base drop then invalidates the view, as it always has.
            reenqueueTasks(attachTasks);
            reenqueueTasks(dropTasks);
            drainWalAndMatViewQueues();
            Assert.assertTrue(state.isInvalid());
        });
    }

    @Test
    public void testAttachRangeDroppedWhenViewSuspended() throws Exception {
        // The view's WAL is suspended and takes no write, so the range task cannot persist the range and drops it,
        // as a restart would.
        setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, "true");
        assertMemoryLeak(() -> {
            createBaseWithAttachableDay();
            createView("price_1h", "IMMEDIATE");
            drainWalAndMatViewQueues();

            execute("ALTER MATERIALIZED VIEW price_1h SUSPEND WAL");
            execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-01'");
            drainWalAndMatViewQueues();
            Assert.assertEquals(0, getViewState("price_1h").getRefreshIntervals().size());

            execute("ALTER MATERIALIZED VIEW price_1h RESUME WAL");
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-02T00:00:00.000000Z\tc\t3.0
                    """);
            assertViewStatus("price_1h", "valid", "");
        });
    }

    @Test
    public void testAttachRangeHonoursRefreshLimit() throws Exception {
        // The ATTACH and UPDATE ranges go through the incremental refresh path, which honours REFRESH LIMIT. The
        // limit of 2 days counts from the base's newest row, 2024-01-05T00:10, so a range in 2024-01-01 has nothing
        // left to recompute and the refresh leaves the cache empty, while a range in 2024-01-04 is recomputed.
        assertMemoryLeak(() -> {
            createBase();
            execute("""
                    INSERT INTO base VALUES
                    ('a', 1.0, '2024-01-01T00:10:00.000000Z'),
                    ('b', 2.0, '2024-01-01T10:10:00.000000Z'),
                    ('d', 4.0, '2024-01-04T00:10:00.000000Z'),
                    ('e', 5.0, '2024-01-04T10:10:00.000000Z'),
                    ('f', 6.0, '2024-01-05T00:10:00.000000Z')
                    """);
            drainWalQueue();
            execute("ALTER TABLE base DETACH PARTITION LIST '2024-01-01', '2024-01-04'");
            drainWalQueue();
            makeAttachable("2024-01-01");
            makeAttachable("2024-01-04");
            createView("price_1h", "IMMEDIATE");
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW price_1h SET REFRESH LIMIT 2 DAYS");
            drainWalAndMatViewQueues();

            execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-01'");
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-05T00:00:00.000000Z\tf\t6.0
                    """);
            assertCacheEmpty("price_1h");
            assertViewValidAndCaughtUp("price_1h");

            execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-04'");
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-04T00:00:00.000000Z\td\t4.0
                    2024-01-04T10:00:00.000000Z\te\t5.0
                    2024-01-05T00:00:00.000000Z\tf\t6.0
                    """);
            assertCacheEmpty("price_1h");
            assertViewValidAndCaughtUp("price_1h");

            // The range of an UPDATE that rewrites 2024-01-01 and 2024-01-04 is recomputed only above the limit.
            execute("UPDATE base SET price = price * 10 WHERE sym IN ('a', 'd')");
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-04T00:00:00.000000Z\td\t40.0
                    2024-01-04T10:00:00.000000Z\te\t5.0
                    2024-01-05T00:00:00.000000Z\tf\t6.0
                    """);
            assertCacheEmpty("price_1h");
            assertViewValidAndCaughtUp("price_1h");
        });
    }

    @Test
    public void testAttachRangeReachesImmediateViewWhenCommitRefreshRunsFirst() throws Exception {
        // The refresh that the ATTACH's own commit triggers runs before the range task and finds nothing to
        // recompute, since the WAL scan skips the ATTACH. The range task then queues an incremental refresh, which
        // recomputes the range although the base receives no further commits.
        assertMemoryLeak(() -> assertAttachRangeReachesImmediateView(false));
    }

    @Test
    public void testAttachRangeReachesImmediateViewWhenRangeTaskRunsFirst() throws Exception {
        // The range task runs before the refresh that the ATTACH's own commit triggers, which then recomputes the
        // range, and the incremental refresh the task queued finds the view up to date.
        assertMemoryLeak(() -> assertAttachRangeReachesImmediateView(true));
    }

    @Test
    public void testAttachRangeReachesManualViewThroughIncrementalRefresh() throws Exception {
        // A MANUAL view keeps the ATTACH range in its cache until REFRESH ... INCREMENTAL recomputes it.
        assertMemoryLeak(() -> {
            createBaseWithAttachableDay();
            createView("price_1h", "MANUAL");
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();

            execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-01'");
            drainWalAndMatViewQueues();
            final MatViewState state = getViewState("price_1h");
            Assert.assertEquals(2, state.getRefreshIntervals().size());
            Assert.assertEquals(parseFloorPartialTimestamp("2024-01-01T00:00:00.000000Z"), state.getRefreshIntervals().getQuick(0));
            Assert.assertEquals(parseFloorPartialTimestamp("2024-01-02T00:00:00.000000Z") - 1, state.getRefreshIntervals().getQuick(1));
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-02T00:00:00.000000Z\tc\t3.0
                    """);

            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewRows(VIEW_ROWS_AFTER_ATTACH);
            assertCacheEmpty("price_1h");
            assertViewValidAndCaughtUp("price_1h");
        });
    }

    @Test
    public void testAttachRangeSkipsDroppedView() throws Exception {
        // The view is dropped after the ATTACH queues its range task. The task finds no view state and does nothing.
        assertMemoryLeak(() -> {
            createBaseWithAttachableDay();
            createView("price_1h", "IMMEDIATE");
            drainWalAndMatViewQueues();

            execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-01'");
            drainWalQueue();
            execute("DROP MATERIALIZED VIEW price_1h");
            drainWalAndMatViewQueues();
            assertQuery("SELECT count() FROM materialized_views()")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n0\n");
        });
    }

    @Test
    public void testAttachRangeJoinsInvalidBacklogAndSkipsNeverRefreshedView() throws Exception {
        // An invalid view takes the range into its backlog, see MatViewBacklog, but recomputes nothing while it stays
        // invalid; a view that never completed a refresh takes no range, since its first build recomputes the whole
        // base. FULL and the first build then bring the attached rows.
        assertMemoryLeak(() -> {
            createBaseWithAttachableDay();
            createView("price_1h", "IMMEDIATE");
            execute("CREATE MATERIALIZED VIEW price_deferred REFRESH MANUAL DEFERRED AS (" + VIEW_SQL + ") PARTITION BY DAY");
            drainWalAndMatViewQueues();
            engine.getMatViewStateStore().enqueueInvalidate(engine.verifyTableName("price_1h"), "test invalidation");
            drainWalAndMatViewQueues();
            assertViewStatus("price_1h", "invalid", "test invalidation");

            execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-01'");
            drainWalAndMatViewQueues();
            assertViewStatus("price_1h", "invalid", "test invalidation");
            final LongList attachedDay = new LongList();
            attachedDay.add(parseFloorPartialTimestamp("2024-01-01T00:00:00.000000Z"));
            attachedDay.add(parseFloorPartialTimestamp("2024-01-01T23:59:59.999999Z"));
            TestUtils.assertEquals(attachedDay, getViewState("price_1h").getRefreshIntervals());
            Assert.assertEquals(0, getViewState("price_deferred").getRefreshIntervals().size());
            Assert.assertEquals(-1, getViewState("price_deferred").getLastRefreshBaseTxn());
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-02T00:00:00.000000Z\tc\t3.0
                    """);

            execute("REFRESH MATERIALIZED VIEW price_1h FULL");
            execute("REFRESH MATERIALIZED VIEW price_deferred INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewRows(VIEW_ROWS_AFTER_ATTACH);
            assertViewValidAndCaughtUp("price_1h");
            assertQuery("price_deferred ORDER BY ts, sym").timestamp("ts").noLeakCheck().expectSize().returns(VIEW_ROWS_AFTER_ATTACH);
            assertViewValidAndCaughtUp("price_deferred");
        });
    }

    @Test
    public void testAttachRangeSkipsViewDroppedWhileTaskQueued() throws Exception {
        // A DROP MATERIALIZED VIEW marks the view's state dropped while the state is still reachable: from a range
        // task that the store queued, and from a range the store is about to queue. The test marks the state
        // dropped in that window, as the drop would. Neither the store nor the task adds the range.
        assertMemoryLeak(() -> {
            createBaseWithAttachableDay();
            createView("price_1h", "IMMEDIATE");
            drainWalAndMatViewQueues();
            final TableToken baseToken = engine.verifyTableName("base");
            final TableToken viewToken = engine.verifyTableName("price_1h");
            final long viewTxns = walTxnCount("price_1h");

            execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-01'");
            drainWalQueue();
            final MatViewState state = getViewState("price_1h");
            state.markAsDropped();
            try {
                engine.getMatViewStateStore().enqueueAddRefreshRange(viewToken, baseToken, 0, 1, 1);
                // Only the batch's own range task and its incremental notification are queued.
                final ObjList<MatViewRefreshTask> tasks = dequeueTasks();
                final MatViewRefreshTask rangeTask = removeRangeTask(tasks);
                Assert.assertEquals(engine.getTableSequencerAPI().getTxnTracker(baseToken).getWriterTxn(), rangeTask.baseTableTxn);
                Assert.assertEquals(1, tasks.size());
                engine.getMatViewStateStore().reenqueueRefreshTask(rangeTask);
                reenqueueTasks(tasks);
                drainWalAndMatViewQueues();
            } finally {
                state.tryCloseIfDropped();
            }
            Assert.assertEquals(0, state.getRefreshIntervals().size());
            Assert.assertEquals(viewTxns, walTxnCount("price_1h"));
            execute("DROP MATERIALIZED VIEW price_1h");
            drainWalAndMatViewQueues();
        });
    }

    @Test
    public void testAttachRangeSkipsViewWithPendingInvalidation() throws Exception {
        // A range task that finds an invalidation pending on the view adds nothing: the view is about to go invalid.
        assertMemoryLeak(() -> {
            createBaseWithAttachableDay();
            createView("price_1h", "IMMEDIATE");
            drainWalAndMatViewQueues();
            final TableToken viewToken = engine.verifyTableName("price_1h");

            execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-01'");
            drainWalQueue();
            final MatViewState state = getViewState("price_1h");
            state.markAsPendingInvalidation("pending invalidation");
            drainMatViewQueue(engine);
            Assert.assertEquals(0, state.getRefreshIntervals().size());

            // A lock holder's release wakes the pending invalidation.
            Assert.assertTrue(state.tryLock());
            MatViewRefreshJob.finalizeAndUnlock(engine, engine.getMatViewStateStore(), viewToken, state, false);
            drainWalAndMatViewQueues();
            assertViewStatus("price_1h", "invalid", "pending invalidation");
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-02T00:00:00.000000Z\tc\t3.0
                    """);
        });
    }

    @Test
    public void testCachedRangesSquashedPastIntervalCap() throws Exception {
        // With a cache of one interval, a range task that adds a range disjoint from the cached one merges both
        // into one interval, which the refresh recomputes whole.
        setProperty(PropertyKey.CAIRO_MAT_VIEW_MAX_REFRESH_INTERVALS, 1);
        assertMemoryLeak(() -> {
            createBase();
            execute(UPDATE_BASE_ROWS);
            drainWalQueue();
            createView("price_1h", "MANUAL");
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();

            execute("UPDATE base SET price = price * 10 WHERE ts IN '2024-01-03'");
            drainWalAndMatViewQueues();
            execute("UPDATE base SET price = price * 10 WHERE ts IN '2024-01-01'");
            drainWalAndMatViewQueues();
            final MatViewState state = getViewState("price_1h");
            Assert.assertEquals(2, state.getRefreshIntervals().size());
            Assert.assertEquals(parseFloorPartialTimestamp("2024-01-01T00:00:00.000000Z"), state.getRefreshIntervals().getQuick(0));
            Assert.assertEquals(parseFloorPartialTimestamp("2024-01-04T00:00:00.000000Z") - 1, state.getRefreshIntervals().getQuick(1));

            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t10.0
                    2024-01-02T00:00:00.000000Z\tb\t2.0
                    2024-01-02T10:00:00.000000Z\tx\t7.0
                    2024-01-03T00:00:00.000000Z\tc\t30.0
                    """);
            assertCacheEmpty("price_1h");
        });
    }

    @Test
    public void testDetachPartitionKeepsRows() throws Exception {
        assertMemoryLeak(() -> assertPartitionRemovalKeepsRows("ALTER TABLE base DETACH PARTITION LIST '2024-01-02'"));
    }

    @Test
    public void testDropPartitionKeepsRows() throws Exception {
        assertMemoryLeak(() -> assertPartitionRemovalKeepsRows("ALTER TABLE base DROP PARTITION LIST '2024-01-02'"));
    }

    @Test
    public void testFailedRangeQueueingKeepsBatchApplied() throws Exception {
        // The range task of a committed batch cannot be queued. The failure goes to the log and the base table
        // keeps applying its WAL; the view misses the range, as after a restart, until a later change covers it.
        assertMemoryLeak(() -> {
            createBaseWithAttachableDay();
            createView("price_1h", "IMMEDIATE");
            drainWalAndMatViewQueues();

            final MatViewStateStoreImpl stateStore = (MatViewStateStoreImpl) engine.getMatViewStateStore();
            stateStore.setOnTaskQueueAppendForTesting(() -> {
                throw new OutOfMemoryError("test range task queue failure");
            });
            try {
                execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-01'");
                drainWalQueue();
            } finally {
                stateStore.setOnTaskQueueAppendForTesting(null);
            }
            drainWalAndMatViewQueues();
            assertQuery("SELECT suspended FROM wal_tables() WHERE name = 'base'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("suspended\nfalse\n");
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-02T00:00:00.000000Z\tc\t3.0
                    """);
            assertViewValidAndCaughtUp("price_1h");

            // A later change in the attached partition reaches the view.
            execute("UPDATE base SET price = 11.0 WHERE sym = 'a'");
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t11.0
                    2024-01-01T10:00:00.000000Z\tb\t2.0
                    2024-01-02T00:00:00.000000Z\tc\t3.0
                    """);
        });
    }

    @Test
    public void testFailingParentKeepsChildValid() throws Exception {
        // The parent's SQL reads a column that the base drops, so the parent's next refresh fails and invalidates
        // it. The child keeps its rows and stays valid.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE MATERIALIZED VIEW parent AS (" + AMOUNT_VIEW_SQL + ") PARTITION BY DAY");
            execute("CREATE MATERIALIZED VIEW child AS (SELECT ts, sym, last(amount) AS amount FROM parent SAMPLE BY 1d) PARTITION BY DAY");
            execute("""
                    INSERT INTO base VALUES
                    ('a', 1.0, 10.0, '2024-01-01T00:10:00.000000Z'),
                    ('a', 2.0, 20.0, '2024-01-01T10:10:00.000000Z')
                    """);
            drainParentAndChild();

            execute("ALTER TABLE base DROP COLUMN amount");
            execute("INSERT INTO base (sym, price, ts) VALUES ('a', 3.0, '2024-01-02T00:10:00.000000Z')");
            drainParentAndChild();

            assertViewStatus("parent", "invalid", "[" + AMOUNT_VIEW_SQL.indexOf("amount") + "]: Invalid column: amount");
            assertViewStatus("child", "valid", "");
            assertQuery("child ORDER BY ts").timestamp("ts").noLeakCheck().expectSize().returns("""
                    ts\tsym\tamount
                    2024-01-01T00:00:00.000000Z\ta\t20.0
                    """);
        });
    }

    @Test
    public void testForwardingAndNoOpStoresHandleRangesAndTruncates() throws Exception {
        // The forwarding store, which Enterprise installs, passes ranges and truncates on to its delegate, and the
        // no-op store of a replica keeps neither. The real store records a truncate only for a base that some view
        // used, keeps the newest seqTxn, and drops a range for a view that has no state.
        assertMemoryLeak(() -> {
            createBase();
            execute("CREATE TABLE other (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            createView("price_1h", "MANUAL");
            createView("dropped_1h", "MANUAL");
            drainWalQueue();
            final TableToken baseToken = engine.verifyTableName("base");
            final TableToken otherToken = engine.verifyTableName("other");
            final TableToken viewToken = engine.verifyTableName("price_1h");
            final TableToken droppedToken = engine.verifyTableName("dropped_1h");
            execute("DROP MATERIALIZED VIEW dropped_1h");
            drainWalQueue();
            drainMatViewQueue(engine);

            final MatViewStateStore stateStore = engine.getMatViewStateStore();
            final ForwardingMatViewStateStore forwardingStore = new ForwardingMatViewStateStore(stateStore);
            Assert.assertEquals(-1, forwardingStore.getLastBaseTableTruncateTxn(baseToken));
            forwardingStore.notifyBaseTableTruncate(baseToken, 7);
            forwardingStore.notifyBaseTableTruncate(baseToken, 5);
            Assert.assertEquals(7, stateStore.getLastBaseTableTruncateTxn(baseToken));
            Assert.assertEquals(7, forwardingStore.getLastBaseTableTruncateTxn(baseToken));
            stateStore.notifyBaseTableTruncate(otherToken, 3);
            Assert.assertEquals(-1, stateStore.getLastBaseTableTruncateTxn(otherToken));

            forwardingStore.enqueueAddRefreshRange(droppedToken, baseToken, 1, 2, 3);
            Assert.assertEquals(0, dequeueTasks().size());
            forwardingStore.enqueueAddRefreshRange(viewToken, baseToken, 10, 20, 8);
            final ObjList<MatViewRefreshTask> tasks = dequeueTasks();
            Assert.assertEquals(1, tasks.size());
            final MatViewRefreshTask task = tasks.getQuick(0);
            Assert.assertEquals(MatViewRefreshTask.ADD_REFRESH_RANGE, task.operation);
            Assert.assertEquals("add_refresh_range", MatViewRefreshTask.getRefreshOperationName(task.operation));
            Assert.assertEquals(viewToken, task.matViewToken);
            Assert.assertEquals(baseToken, task.baseTableToken);
            Assert.assertEquals(10, task.rangeFrom);
            Assert.assertEquals(20, task.rangeTo);
            Assert.assertEquals(8, task.baseTableTxn);
            task.clear();
            Assert.assertEquals(Numbers.LONG_NULL, task.baseTableTxn);

            final NoOpMatViewStateStore noOpStore = NoOpMatViewStateStore.INSTANCE;
            noOpStore.notifyBaseTableTruncate(baseToken, 9);
            Assert.assertEquals(-1, noOpStore.getLastBaseTableTruncateTxn(baseToken));
            noOpStore.enqueueAddRefreshRange(viewToken, baseToken, 10, 20, 8);
            Assert.assertEquals(0, dequeueTasks().size());
        });
    }

    @Test
    public void testIntervalsQueuedBeforeTruncateSkippedAfterRestart() throws Exception {
        // A restart between the TRUNCATE and the refresh loses no TRUNCATE cut: the first refresh after the restart
        // scans the same WAL range and drops the interval queued before the truncate.
        assertMemoryLeak(() -> {
            final DefaultTestCairoConfiguration conf = new DefaultTestCairoConfiguration(temp.newFolder().getAbsolutePath()) {
                @Override
                public int getCommitMode() {
                    return configuration.getCommitMode();
                }
            };
            try (CairoEngine first = new CairoEngine(conf)) {
                first.load();
                first.execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                first.execute("CREATE MATERIALIZED VIEW price_1h AS (" + VIEW_SQL + ") PARTITION BY DAY");
                try (SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(first, 1)) {
                    first.execute(TRUNCATE_BASE_ROWS, context);
                }
                drainWalAndMatViewQueues(first);
                try (SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(first, 1)) {
                    first.execute("INSERT INTO base VALUES ('a', 11.0, '2024-01-01T00:20:00.000000Z')", context);
                    first.execute("TRUNCATE TABLE base", context);
                    first.execute("INSERT INTO base VALUES ('d', 4.0, '2024-01-01T05:10:00.000000Z')", context);
                }
                // The base applies its WAL; the view's refresh does not run before the restart.
                drainWalQueue(first);
            }
            try (CairoEngine second = new CairoEngine(conf)) {
                second.load();
                second.hydrateMatViewStateStore();
                drainWalAndMatViewQueues(second);
                try (SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(second, 1)) {
                    assertQuery("price_1h ORDER BY ts, sym")
                            .withEngine(second)
                            .withContext(context)
                            .timestamp("ts")
                            .noLeakCheck()
                            .expectSize()
                            .returns(VIEW_ROWS_AFTER_TRUNCATE);
                    assertQuery("SELECT view_status, invalidation_reason, refresh_base_table_txn = base_table_txn AS caught_up FROM materialized_views() WHERE view_name = 'price_1h'")
                            .withEngine(second)
                            .withContext(context)
                            .noLeakCheck()
                            .noRandomAccess()
                            .returns("view_status\tinvalidation_reason\tcaught_up\nvalid\t\ttrue\n");
                }
            }
        });
    }

    @Test
    public void testInvalidParentKeepsChildValid() throws Exception {
        // A rename of the base invalidates the parent, which reads it. The child keeps its rows and stays valid.
        assertMemoryLeak(() -> {
            createParentAndChild();
            execute("RENAME TABLE base TO base2");
            drainParentAndChild();
            assertViewStatus("parent", "invalid", "base table is dropped or renamed");
            assertViewStatus("child", "valid", "");
            assertChildRows(CHILD_ROWS);
        });
    }

    @Test
    public void testLiveViewInvalidatedByBaseUpdate() throws Exception {
        // A base UPDATE keeps the mat view valid and recomputes its range, while a live view on the same base still
        // goes invalid with the "update operation" reason.
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        assertMemoryLeak(() -> {
            createBase();
            execute("INSERT INTO base VALUES ('a', 1.0, '2024-01-01T00:10:00.000000Z'), ('b', 2.0, '2024-01-02T00:10:00.000000Z')");
            drainWalQueue();
            createView("price_1h", "IMMEDIATE");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " +
                    "SELECT ts, sym, price, count(*) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            drainWalAndMatViewQueues();
            final LiveViewInstance liveView = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertFalse(liveView.isInvalid());

            execute("UPDATE base SET price = 10.0 WHERE sym = 'a'");
            drainWalAndMatViewQueues();

            Assert.assertTrue(liveView.isInvalid());
            Assert.assertEquals("update operation", liveView.getInvalidationReason().toString());
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t10.0
                    2024-01-02T00:00:00.000000Z\tb\t2.0
                    """);
            assertViewValidAndCaughtUp("price_1h");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testLiveViewInvalidatedByParentFullRefresh() throws Exception {
        // A FULL refresh of the parent view truncates it and rebuilds it. A live view on the parent still goes
        // invalid, while the child mat view keeps its rows and refreshes the range the parent re-inserts.
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        assertMemoryLeak(() -> {
            createParentAndChild();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " +
                    "SELECT ts, sym, price, count(*) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM parent");
            drainParentAndChild();
            final LiveViewInstance liveView = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertFalse(liveView.isInvalid());

            execute("REFRESH MATERIALIZED VIEW parent FULL");
            drainParentAndChild();

            Assert.assertTrue(liveView.isInvalid());
            Assert.assertEquals("base materialized view was rebuilt", liveView.getInvalidationReason().toString());
            assertViewStatus("parent", "valid", "");
            assertViewStatus("child", "valid", "");
            assertChildRows(CHILD_ROWS);
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testParentDropPartitionKeepsChildRows() throws Exception {
        // A view DROP PARTITION on the parent removes the parent's rows there. The child keeps its rows of the
        // dropped range and stays valid, also after a later parent commit.
        assertMemoryLeak(() -> {
            createParentAndChild();
            execute("ALTER MATERIALIZED VIEW parent DROP PARTITION LIST '2024-01-01'");
            drainParentAndChild();
            assertQuery("parent ORDER BY ts, sym").timestamp("ts").noLeakCheck().expectSize().returns("""
                    ts\tsym\tprice
                    2024-01-02T00:00:00.000000Z\ta\t3.0
                    """);
            execute("INSERT INTO base VALUES ('a', 4.0, '2024-01-02T10:10:00.000000Z')");
            drainParentAndChild();

            assertViewStatus("parent", "valid", "");
            assertViewStatus("child", "valid", "");
            assertChildRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t2.0
                    2024-01-02T00:00:00.000000Z\ta\t4.0
                    """);
        });
    }

    @Test
    public void testPeriodViewRefreshWithNothingToRecomputeCommitsOnlyItsPeriod() throws Exception {
        // A period view's refresh whose newly complete period has no base partition recomputes nothing. With no
        // cached range to clear, it commits only the advanced period, as a view state event.
        assertMemoryLeak(() -> {
            createBase();
            currentMicros = parseFloorPartialTimestamp("2024-01-01T00:00:00.000000Z");
            execute("CREATE MATERIALIZED VIEW price_1h REFRESH IMMEDIATE PERIOD (LENGTH 1d) AS (" + VIEW_SQL + ") PARTITION BY DAY");
            execute("INSERT INTO base VALUES ('a', 1.0, '2024-01-01T00:10:00.000000Z')");
            currentMicros = parseFloorPartialTimestamp("2024-01-02T00:00:00.000000Z");
            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
            drainMatViewTimerQueue(timerJob);
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    """);
            final MatViewState state = getViewState("price_1h");
            Assert.assertEquals(parseFloorPartialTimestamp("2024-01-02T00:00:00.000000Z"), state.getLastPeriodHi());

            final long viewTxns = walTxnCount("price_1h");
            currentMicros = parseFloorPartialTimestamp("2024-01-03T00:00:00.000000Z");
            engine.getMatViewStateStore().enqueueIncrementalRefresh(engine.verifyTableName("price_1h"));
            drainWalAndMatViewQueues();
            Assert.assertEquals(parseFloorPartialTimestamp("2024-01-03T00:00:00.000000Z"), state.getLastPeriodHi());
            Assert.assertEquals(viewTxns + 1, walTxnCount("price_1h"));
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    """);
            assertViewStatus("price_1h", "valid", "");
        });
    }

    @Test
    public void testRangeRefreshWithNothingToRecomputeCommitsNothing() throws Exception {
        // A user RANGE over a range the base holds no partition of recomputes nothing and writes nothing to the view's
        // WAL, and neither does an incremental refresh that finds the view up to date with an empty cache.
        assertMemoryLeak(() -> {
            createBase();
            execute(UPDATE_BASE_ROWS);
            createView("price_1h", "IMMEDIATE");
            drainWalAndMatViewQueues();
            execute("ALTER TABLE base DROP PARTITION LIST '2024-01-02'");
            drainWalAndMatViewQueues();

            final long viewTxns = walTxnCount("price_1h");
            execute("REFRESH MATERIALIZED VIEW price_1h RANGE FROM '2024-01-02T00:00:00.000000Z' TO '2024-01-02T23:00:00.000000Z'");
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            Assert.assertEquals(viewTxns, walTxnCount("price_1h"));
            assertViewRows(UPDATE_VIEW_ROWS);
            assertViewValidAndCaughtUp("price_1h");
        });
    }

    @Test
    public void testRangeRemovedByPartitionClipLeavesCacheEmpty() throws Exception {
        // A MANUAL view queues the range of a base UPDATE of 2024-01-02, then the base drops that partition. The
        // refresh has nothing left to recompute: it keeps the view's rows there and leaves the cache empty.
        assertMemoryLeak(() -> {
            createBase();
            execute(UPDATE_BASE_ROWS);
            drainWalQueue();
            createView("price_1h", "MANUAL");
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();

            execute("UPDATE base SET price = price * 10 WHERE ts IN '2024-01-02'");
            drainWalAndMatViewQueues();
            Assert.assertEquals(2, getViewState("price_1h").getRefreshIntervals().size());
            execute("ALTER TABLE base DROP PARTITION LIST '2024-01-02'");
            drainWalQueue();

            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewRows(UPDATE_VIEW_ROWS);
            assertCacheEmpty("price_1h");
            assertViewValidAndCaughtUp("price_1h");
        });
    }

    @Test
    public void testRangeRemovedByRefreshLimitLeavesCacheEmptyAtSamePosition() throws Exception {
        // The refresh that the ATTACH's own commit triggers runs first and moves the view to the base's last
        // transaction. The range task then queues a range below the REFRESH LIMIT, so the incremental refresh it
        // triggers has nothing to recompute and no transaction to advance past: it still clears the cache and
        // persists the empty cache with a view state event.
        assertMemoryLeak(() -> {
            createBaseWithAttachableDay();
            execute("INSERT INTO base VALUES ('f', 6.0, '2024-01-05T00:10:00.000000Z')");
            drainWalQueue();
            createView("price_1h", "IMMEDIATE");
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW price_1h SET REFRESH LIMIT 2 DAYS");
            drainWalAndMatViewQueues();

            execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-01'");
            drainWalQueue();
            final ObjList<MatViewRefreshTask> tasks = dequeueTasks();
            final MatViewRefreshTask rangeTask = removeRangeTask(tasks);
            reenqueueTasks(tasks);
            drainWalAndMatViewQueues();
            final MatViewState state = getViewState("price_1h");
            final long refreshTxn = state.getLastRefreshBaseTxn();
            Assert.assertEquals(engine.getTableSequencerAPI().getTxnTracker(engine.verifyTableName("base")).getWriterTxn(), refreshTxn);

            final long viewTxns = walTxnCount("price_1h");
            engine.getMatViewStateStore().reenqueueRefreshTask(rangeTask);
            drainWalAndMatViewQueues();
            // one state event that adds the range, one that clears it
            Assert.assertEquals(viewTxns + 2, walTxnCount("price_1h"));
            Assert.assertEquals(refreshTxn, state.getLastRefreshBaseTxn());
            assertCacheEmpty("price_1h");
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-02T00:00:00.000000Z\tc\t3.0
                    2024-01-05T00:00:00.000000Z\tf\t6.0
                    """);
            assertViewValidAndCaughtUp("price_1h");
        });
    }

    @Test
    public void testRebuiltParentKeepsChildRows() throws Exception {
        // The base drops 2024-01-01, and a FULL refresh of the parent rebuilds it from the base, without that day.
        // The child keeps its row of 2024-01-01 and recomputes 2024-01-02, the range the parent re-inserts.
        assertMemoryLeak(() -> {
            createParentAndChild();
            execute("INSERT INTO base VALUES ('a', 4.0, '2024-01-02T10:10:00.000000Z')");
            drainWalQueue();
            execute("ALTER TABLE base DROP PARTITION LIST '2024-01-01'");
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW parent FULL");
            drainParentAndChild();

            assertViewStatus("parent", "valid", "");
            assertViewStatus("child", "valid", "");
            assertQuery("parent ORDER BY ts, sym").timestamp("ts").noLeakCheck().expectSize().returns("""
                    ts\tsym\tprice
                    2024-01-02T00:00:00.000000Z\ta\t3.0
                    2024-01-02T10:00:00.000000Z\ta\t4.0
                    """);
            assertChildRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t2.0
                    2024-01-02T00:00:00.000000Z\ta\t4.0
                    """);
        });
    }

    @Test
    public void testTruncateKeepsRowsAndSkipsCachedIntervalsQueuedBeforeIt() throws Exception {
        // A MANUAL view caches the interval of a row written before the TRUNCATE. The refresh after the truncate drops
        // the cached interval, keeps the view's rows and recomputes only the bucket of the row written after it.
        assertMemoryLeak(() -> {
            createBase();
            execute(TRUNCATE_BASE_ROWS);
            drainWalQueue();
            createView("price_1h", "MANUAL");
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();

            execute("INSERT INTO base VALUES ('a', 11.0, '2024-01-01T00:20:00.000000Z')");
            drainWalQueue();
            engine.getMatViewStateStore().enqueueUpdateRefreshIntervals(engine.verifyTableName("price_1h"));
            drainWalAndMatViewQueues();
            Assert.assertEquals(2, getViewState("price_1h").getRefreshIntervals().size());

            execute("TRUNCATE TABLE base");
            execute("INSERT INTO base VALUES ('d', 4.0, '2024-01-01T05:10:00.000000Z')");
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();

            assertViewRows(VIEW_ROWS_AFTER_TRUNCATE);
            assertCacheEmpty("price_1h");
            assertViewValidAndCaughtUp("price_1h");
        });
    }

    @Test
    public void testTruncateKeepsRowsAndSkipsIntervalsQueuedBeforeIt() throws Exception {
        // An IMMEDIATE view: a row written before the TRUNCATE has not reached the view yet. The refresh after the
        // truncate scans both rows, drops the interval queued before the truncate, keeps the view's rows and
        // recomputes only the bucket of the row written after it.
        assertMemoryLeak(() -> {
            createBase();
            execute(TRUNCATE_BASE_ROWS);
            createView("price_1h", "IMMEDIATE");
            drainWalAndMatViewQueues();

            execute("INSERT INTO base VALUES ('a', 11.0, '2024-01-01T00:20:00.000000Z')");
            drainWalQueue();
            execute("TRUNCATE TABLE base");
            execute("INSERT INTO base VALUES ('d', 4.0, '2024-01-01T05:10:00.000000Z')");
            drainWalAndMatViewQueues();

            assertViewRows(VIEW_ROWS_AFTER_TRUNCATE);
            assertViewValidAndCaughtUp("price_1h");
        });
    }

    @Test
    public void testUpdateRangeDroppedAfterLaterTruncate() throws Exception {
        // A TRUNCATE applies after a base UPDATE and before the UPDATE's range task runs. The task drops the range,
        // so the refresh recomputes only the bucket of the row written after the truncate, and the view keeps its
        // other rows, with the values from before the UPDATE.
        assertMemoryLeak(() -> {
            createBase();
            execute(TRUNCATE_BASE_ROWS);
            createView("price_1h", "IMMEDIATE");
            drainWalAndMatViewQueues();

            execute("UPDATE base SET price = price * 10 WHERE ts IN '2024-01-01'");
            drainWalQueue();
            execute("TRUNCATE TABLE base");
            execute("INSERT INTO base VALUES ('d', 4.0, '2024-01-01T05:10:00.000000Z')");
            drainWalQueue();
            drainWalAndMatViewQueues();

            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-01T05:00:00.000000Z\td\t4.0
                    2024-01-01T10:00:00.000000Z\tb\t2.0
                    2024-01-02T00:00:00.000000Z\tc\t3.0
                    """);
            assertCacheEmpty("price_1h");
            assertViewValidAndCaughtUp("price_1h");
        });
    }

    @Test
    public void testUpdateRangeGoesThroughIncrementalRefresh() throws Exception {
        // A base UPDATE keeps the view valid and queues the range of the partition it rewrote, which the view's
        // incremental refresh recomputes. The other partitions keep their rows.
        assertMemoryLeak(() -> {
            createBase();
            execute(UPDATE_BASE_ROWS);
            createView("price_1h", "IMMEDIATE");
            drainWalAndMatViewQueues();
            assertViewRows(UPDATE_VIEW_ROWS);

            execute("UPDATE base SET price = price * 10 WHERE ts IN '2024-01-02'");
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-02T00:00:00.000000Z\tb\t20.0
                    2024-01-02T10:00:00.000000Z\tx\t70.0
                    2024-01-03T00:00:00.000000Z\tc\t3.0
                    """);
            assertCacheEmpty("price_1h");
            assertViewValidAndCaughtUp("price_1h");
        });
    }

    @Test
    public void testUpdateRangesInOneBatchQueueTheirUnion() throws Exception {
        // Two UPDATEs in one batch, one of them across two partitions, queue one range from the oldest to the
        // newest partition they rewrote. The incremental refresh recomputes the whole range, 2024-01-02 included,
        // whose rows neither UPDATE changed.
        assertMemoryLeak(() -> {
            createBase();
            execute(UPDATE_BASE_ROWS);
            createView("price_1h", "IMMEDIATE");
            drainWalAndMatViewQueues();

            execute("UPDATE base SET price = price * 10 WHERE sym IN ('a', 'c')");
            execute("UPDATE base SET price = price + 1 WHERE ts IN '2024-01-03'");
            drainWalQueue();
            final ObjList<MatViewRefreshTask> tasks = dequeueTasks();
            final MatViewRefreshTask rangeTask = removeRangeTask(tasks);
            Assert.assertEquals(parseFloorPartialTimestamp("2024-01-01T00:00:00.000000Z"), rangeTask.rangeFrom);
            Assert.assertEquals(parseFloorPartialTimestamp("2024-01-04T00:00:00.000000Z") - 1, rangeTask.rangeTo);
            engine.getMatViewStateStore().reenqueueRefreshTask(rangeTask);
            reenqueueTasks(tasks);
            drainWalAndMatViewQueues();

            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t10.0
                    2024-01-02T00:00:00.000000Z\tb\t2.0
                    2024-01-02T10:00:00.000000Z\tx\t7.0
                    2024-01-03T00:00:00.000000Z\tc\t31.0
                    """);
            assertCacheEmpty("price_1h");
            assertViewValidAndCaughtUp("price_1h");
        });
    }

    private static void drainParentAndChild() {
        // A child view refreshes after its parent's refresh commit applies, so each drain round reaches one level
        // deeper.
        drainWalAndMatViewQueues();
        drainWalAndMatViewQueues();
    }

    private static MatViewState getViewState(String viewName) {
        final MatViewState state = engine.getMatViewStateStore().getViewState(engine.verifyTableName(viewName));
        Assert.assertNotNull(state);
        return state;
    }

    private static void makeAttachable(String partition) {
        final TableToken tableToken = engine.verifyTableName("base");
        try (Path path = new Path(); Path other = new Path()) {
            path.of(configuration.getDbRoot()).concat(tableToken).concat(partition).put(DETACHED_DIR_MARKER).$();
            other.of(configuration.getDbRoot()).concat(tableToken).concat(partition).put(configuration.getAttachPartitionSuffix()).$();
            Assert.assertTrue(Files.rename(path.$(), other.$()) > -1);
        }
    }

    private static void reenqueueTasks(ObjList<MatViewRefreshTask> tasks) {
        final MatViewStateStore stateStore = engine.getMatViewStateStore();
        for (int i = 0, n = tasks.size(); i < n; i++) {
            stateStore.reenqueueRefreshTask(tasks.getQuick(i));
        }
    }

    private static MatViewRefreshTask removeRangeTask(ObjList<MatViewRefreshTask> tasks) {
        MatViewRefreshTask rangeTask = null;
        for (int i = tasks.size() - 1; i > -1; i--) {
            if (tasks.getQuick(i).operation == MatViewRefreshTask.ADD_REFRESH_RANGE) {
                Assert.assertNull("one range task expected", rangeTask);
                rangeTask = tasks.getQuick(i);
                tasks.remove(i);
            }
        }
        Assert.assertNotNull("range task expected", rangeTask);
        return rangeTask;
    }

    // Builds an IMMEDIATE view over a base whose 2024-01-01 partition is detached, attaches the partition and runs
    // the range task and the refresh the ATTACH's commit triggers in the given order.
    private void assertAttachRangeReachesImmediateView(boolean isRangeTaskFirst) throws Exception {
        createBaseWithAttachableDay();
        createView("price_1h", "IMMEDIATE");
        drainWalAndMatViewQueues();
        assertViewRows("""
                ts\tsym\tprice
                2024-01-02T00:00:00.000000Z\tc\t3.0
                """);

        execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-01'");
        drainWalQueue();
        final ObjList<MatViewRefreshTask> tasks = dequeueTasks();
        final MatViewRefreshTask rangeTask = removeRangeTask(tasks);
        Assert.assertEquals(1, tasks.size());
        Assert.assertEquals(MatViewRefreshTask.INCREMENTAL_REFRESH, tasks.getQuick(0).operation);
        Assert.assertTrue(tasks.getQuick(0).isBaseTableTask());
        if (isRangeTaskFirst) {
            engine.getMatViewStateStore().reenqueueRefreshTask(rangeTask);
            reenqueueTasks(tasks);
        } else {
            reenqueueTasks(tasks);
            engine.getMatViewStateStore().reenqueueRefreshTask(rangeTask);
        }
        drainWalAndMatViewQueues();

        assertViewRows(VIEW_ROWS_AFTER_ATTACH);
        assertCacheEmpty("price_1h");
        assertViewValidAndCaughtUp("price_1h");
    }

    private void assertCacheEmpty(String viewName) throws Exception {
        final TableToken viewToken = engine.verifyTableName(viewName);
        final MatViewState state = getViewState(viewName);
        Assert.assertEquals(0, state.getRefreshIntervals().size());
        try (Path path = new Path(); BlockFileReader reader = new BlockFileReader(configuration)) {
            path.of(configuration.getDbRoot()).concat(viewToken).concat(MatViewState.MAT_VIEW_STATE_FILE_NAME);
            reader.of(path.$());
            final MatViewStateReader stateReader = new MatViewStateReader();
            stateReader.of(reader, viewToken);
            Assert.assertEquals(0, stateReader.getRefreshIntervals().size());
            Assert.assertEquals(state.getLastRefreshBaseTxn(), stateReader.getLastRefreshBaseTxn());
        }
    }

    private void assertChildRows(String expected) throws Exception {
        assertQuery("child ORDER BY ts, sym").timestamp("ts").noLeakCheck().expectSize().returns(expected);
    }

    // Removes the given partition of the base. The view keeps its rows there and stays valid, and its later
    // refreshes follow the base elsewhere.
    private void assertPartitionRemovalKeepsRows(String removalSql) throws Exception {
        createBase();
        execute("""
                INSERT INTO base VALUES
                ('a', 1.0, '2024-01-01T00:10:00.000000Z'),
                ('b', 2.0, '2024-01-02T00:10:00.000000Z'),
                ('c', 3.0, '2024-01-03T00:10:00.000000Z')
                """);
        createView("price_1h", "IMMEDIATE");
        drainWalAndMatViewQueues();
        final String rows = """
                ts\tsym\tprice
                2024-01-01T00:00:00.000000Z\ta\t1.0
                2024-01-02T00:00:00.000000Z\tb\t2.0
                2024-01-03T00:00:00.000000Z\tc\t3.0
                """;
        assertViewRows(rows);

        execute(removalSql);
        drainWalAndMatViewQueues();
        assertQuery("SELECT name FROM table_partitions('base') WHERE NOT detached ORDER BY name")
                .noLeakCheck()
                .returns("name\n2024-01-01\n2024-01-03\n");
        assertViewRows(rows);
        assertViewValidAndCaughtUp("price_1h");

        execute("INSERT INTO base VALUES ('c', 33.0, '2024-01-03T00:20:00.000000Z'), ('d', 4.0, '2024-01-04T00:10:00.000000Z')");
        drainWalAndMatViewQueues();
        assertViewRows("""
                ts\tsym\tprice
                2024-01-01T00:00:00.000000Z\ta\t1.0
                2024-01-02T00:00:00.000000Z\tb\t2.0
                2024-01-03T00:00:00.000000Z\tc\t33.0
                2024-01-04T00:00:00.000000Z\td\t4.0
                """);
        assertViewValidAndCaughtUp("price_1h");
    }

    private void assertViewRows(String expected) throws Exception {
        assertQuery("price_1h ORDER BY ts, sym").timestamp("ts").noLeakCheck().expectSize().returns(expected);
    }

    private void assertViewStatus(String viewName, String status, String reason) throws Exception {
        assertQuery("SELECT view_status, invalidation_reason FROM materialized_views() WHERE view_name = '" + viewName + "'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("view_status\tinvalidation_reason\n" + status + "\t" + reason + "\n");
    }

    private void assertViewValidAndCaughtUp(String viewName) throws Exception {
        assertQuery("SELECT view_status, invalidation_reason, refresh_base_table_txn = base_table_txn AS caught_up FROM materialized_views() WHERE view_name = '" + viewName + "'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("view_status\tinvalidation_reason\tcaught_up\nvalid\t\ttrue\n");
    }

    private void createBase() throws Exception {
        execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    // A base with rows in 2024-01-01 and 2024-01-02, whose 2024-01-01 partition is detached and ready to attach.
    private void createBaseWithAttachableDay() throws Exception {
        createBase();
        execute("""
                INSERT INTO base VALUES
                ('a', 1.0, '2024-01-01T00:10:00.000000Z'),
                ('b', 2.0, '2024-01-01T10:10:00.000000Z'),
                ('c', 3.0, '2024-01-02T00:10:00.000000Z')
                """);
        drainWalQueue();
        execute("ALTER TABLE base DETACH PARTITION LIST '2024-01-01'");
        drainWalQueue();
        makeAttachable("2024-01-01");
    }

    // An hourly parent view over the base and a daily child view over the parent.
    private void createParentAndChild() throws Exception {
        createBase();
        execute("CREATE MATERIALIZED VIEW parent AS (" + VIEW_SQL + ") PARTITION BY DAY");
        execute("CREATE MATERIALIZED VIEW child AS (SELECT ts, sym, last(price) AS price FROM parent SAMPLE BY 1d) PARTITION BY DAY");
        execute("""
                INSERT INTO base VALUES
                ('a', 1.0, '2024-01-01T00:10:00.000000Z'),
                ('a', 2.0, '2024-01-01T10:10:00.000000Z'),
                ('a', 3.0, '2024-01-02T00:10:00.000000Z')
                """);
        drainParentAndChild();
        assertChildRows(CHILD_ROWS);
    }

    private void createView(String viewName, String refreshType) throws Exception {
        execute("CREATE MATERIALIZED VIEW " + viewName + " REFRESH " + refreshType + " AS (" + VIEW_SQL + ") PARTITION BY DAY");
    }

    private ObjList<MatViewRefreshTask> dequeueTasks() {
        final ObjList<MatViewRefreshTask> tasks = new ObjList<>();
        final MatViewStateStore stateStore = engine.getMatViewStateStore();
        while (true) {
            final MatViewRefreshTask task = new MatViewRefreshTask();
            if (!stateStore.tryDequeueRefreshTask(task)) {
                return tasks;
            }
            tasks.add(task);
        }
    }

    private long walTxnCount(String viewName) throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            try (
                    RecordCursorFactory factory = compiler.compile("SELECT count() FROM wal_transactions('" + viewName + "')", sqlExecutionContext).getRecordCursorFactory();
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                Assert.assertTrue(cursor.hasNext());
                return cursor.getRecord().getLong(0);
            }
        }
    }
}
