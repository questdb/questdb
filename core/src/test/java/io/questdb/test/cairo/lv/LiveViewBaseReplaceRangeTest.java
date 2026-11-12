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

package io.questdb.test.cairo.lv;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointRepairMarker;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreWriter;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewRefreshStats;
import io.questdb.cairo.lv.LiveViewState;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.Files;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * REPLACE_RANGE data commits on a live view's base table. A replace-range
 * commit atomically deletes every base row inside {@code [rangeLo, rangeHi)}
 * and inserts the commit's own rows in their place. Unlike DROP PARTITION or
 * TRUNCATE (non-DATA operations, deliberately frozen by the LV), a replace
 * commit is a DATA commit: the refresh worker must converge the view onto the
 * post-replace base state, exactly as it does for a dedup replacement.
 * <p>
 * The hazard: the raw-WAL forward drain reads only the commit's inserted rows.
 * The deletion side of the replace is visible only through the commit's range
 * metadata. A replace whose inserted rows all sit above the view's frontier
 * (or a row-less pure-delete replace) presents nothing below
 * {@code latestSeenTs}, so a drain that never consults the range would
 * forward-append the new rows and keep the derived rows of the deleted band
 * forever - silent ghosts a recompute does not have. The drain therefore
 * treats the commit's range low (clamped to the view's lower bound) as its
 * effective minimum timestamp, non-strictly against the frontier, and routes
 * such commits to the O3 replay, which re-reads the applied (post-replace)
 * base.
 */
public class LiveViewBaseReplaceRangeTest extends AbstractLiveViewTest {
    // The day below the 2025-10-26 Europe/Berlin fall-back, then the rows of the half hour the
    // clocks go back over, 02:30 CEST, whose anchor value no instant below it carries, and the
    // rows of 10-26's own segment that depart: see rowsOfTheFallBackEve.
    private static final String FALL_BACK_OPEN_RUN_ROWS = "('2025-10-26T00:40:00.000000Z', 'a', 1.0), ('2025-10-26T00:45:00.000000Z', 'a', 1.0), "
            + "('2025-10-26T02:00:00.000000Z', 'a', 1.0), ('2025-10-26T02:05:00.000000Z', 'a', 1.0), ('2025-10-26T02:10:00.000000Z', 'a', 1.0)";
    // The view's rows on 10-24 and from 10-26T00:20Z on, which the fall-back tests assert.
    private static final String FALL_BACK_TAIL_SQL = "SELECT ts, sym, x, s FROM lv "
            + "WHERE ts < '2025-10-25T00:00:00.000000Z' OR ts >= '2025-10-26T00:20:00.000000Z'";
    // Where commitTheFrameAcrossMidnight leaves a root inside the one-minute frame of the new
    // frontier 01-02T00:00:50: none, at its floor 01-01T23:59:50, or 40 seconds above it.
    private static final int FRAME_ROOT_ABOVE_THE_FLOOR = 2;
    private static final int FRAME_ROOT_AT_THE_FLOOR = 1;
    private static final int FRAME_ROOT_NONE = 0;

    // Pin the test clock below all test data before each test. A non-SEED view's
    // lower bound is the CREATE wall-clock moment, and the forward-append refresh path
    // drops rows below it. The test data is timestamped in the past, so without a
    // pinned clock every row would be dropped as pre-CREATE.
    @Before
    public void pinClockBelowTestData() {
        setCurrentMicros(0L);
    }

    @Test
    public void testAnEndOfTableRepairThatAddsALateRowBelowTheNewestRootAppendsARootAtTheFrontier() throws Exception {
        // A replace commit that only adds a row half a minute below the newest root, with a row the
        // cadence left unsealed above that root: the rebuild splices [00:58:30, +inf), and its
        // output runs past the root at 00:59 it re-versions, to the frontier 01:05. Nothing above
        // the frontier is left to truncate, so the seal appends a root there, which takes the
        // place of the head root at 00:59: the late row is the newest batch's lowest.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitTwoBatchesOverTheLostDay(job, 59);
                commitAndRefresh(job, "('2026-01-01T01:05:00.000000Z', 'a', 1.0)");
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                final long generationBefore = publishedCheckpointGeneration(instance);
                final long headMissRepairsBefore = countHeadMissRepairs(job);
                replaceBaseRange(ts("2026-01-01T00:58:30.000000Z"), ts("2026-01-01T00:58:30.000001Z"), 3.0);
                drainWalQueue();
                driveRefreshToQuiescence(job);
                Assert.assertEquals("the head-miss rebuild repairs the late row", headMissRepairsBefore + 1, countHeadMissRepairs(job));
                assertNoHeadMissSpliceFrontierStat(job);
                Assert.assertEquals("the splice and the seal", generationBefore + 2, publishedCheckpointGeneration(instance));
                assertCheckpointLadder(instance, false, """
                        2025-12-31T23:55:00.000000Z -> 1
                        2026-01-01T01:05:00.000000Z -> 63
                        """);
                assertNoRepairMarker(instance);
            }
            // The late row's frame holds 00:58 too, and 00:59's holds the late row.
            final String rows = headMissDepartureRows(true, 58) + """
                    2026-01-01T00:58:30.000000Z\ta\t3.0\t4.0
                    2026-01-01T00:59:00.000000Z\ta\t1.0\t5.0
                    2026-01-01T01:05:00.000000Z\ta\t1.0\t1.0
                    """;
            assertDepartedViewRows(rows);
            assertNoRefreshFaults("lv");
            for (int restart = 0; restart < 2; restart++) {
                restartLiveViewsAndRecover();
                assertDepartedViewRestored(rows);
            }
        });
    }

    @Test
    public void testAnEndOfTableRepairThatDeletesEveryRowOfTheViewRetiresItsTimeline() throws Exception {
        // Every row the view holds sits in the deleted range, so the view's table ends empty and
        // has no frontier: no root can sit there, and the repair retires the timeline. A restart
        // finds none and rebuilds the empty view, which still refreshes.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitAndRefresh(job, minuteRowsOfA(57, 59, "1.0"));
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                assertCheckpointLadder(instance, false, "2026-01-01T00:59:00.000000Z -> 3\n");
                departAndAssertTheTimelineRetired(job, instance, "2026-01-01T00:57:00.000000Z");
            }
            final String noRows = "ts\tsym\tx\ts\n";
            assertDepartedViewRows(noRows);
            restartLiveViewsAndRecover();
            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
            Assert.assertFalse(instance.isInvalid());
            assertDepartedViewRows(noRows);
            assertNoRefreshFaults("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                commitAndRefresh(job, "('2026-01-01T01:05:00.000000Z', 'a', 4.0)");
            }
            assertDepartedViewRows(noRows + "2026-01-01T01:05:00.000000Z\ta\t4.0\t4.0\n");
        });
    }

    @Test
    public void testAnEndOfTableRepairThatDeletesRowsAboveAnUnsealedFrontierRootKeepsItsTimeline() throws Exception {
        // The newest root sits at the row that becomes the frontier, and nothing above it was
        // sealed: two filtered-out rows and a row of the view follow it unsealed. Deleting that row
        // emits nothing, from a rebuild that warms its state from one minute below it and so reads
        // none of the filtered-out rows a resume from the root would read. The splice kept no root
        // above the frontier and the root there counts every row of the view, so the repair keeps
        // the timeline as the splice left it, and each restart restores from that root over the
        // day the base lost.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitRootAtTheFrontierThenRowsAboveIt(job, false);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                final String ladder = """
                        2025-12-31T23:55:00.000000Z -> 1
                        2026-01-01T00:56:00.000000Z -> 58
                        """;
                assertCheckpointLadder(instance, false, ladder);
                final long generationBefore = publishedCheckpointGeneration(instance);
                final long headMissRepairsBefore = countHeadMissRepairs(job);
                final long resetsBefore = instance.getCheckpointTimelineResets();
                replaceBaseRange(ts("2026-01-01T00:59:00.000000Z"), ts("2026-01-01T01:00:00.000000Z"), Double.NaN);
                drainWalQueue();
                driveRefreshToQuiescence(job);
                Assert.assertEquals("the head-miss rebuild repairs the departure", headMissRepairsBefore + 1, countHeadMissRepairs(job));
                Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_ZERO_ROW_KEEP_COUNT));
                Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_FRONTIER_TRUNCATE_COUNT));
                Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_ZERO_ROW_RETIRE_COUNT));
                Assert.assertEquals("the splice alone", generationBefore + 1, publishedCheckpointGeneration(instance));
                Assert.assertEquals(resetsBefore, instance.getCheckpointTimelineResets());
                assertCheckpointLadder(instance, false, ladder);
                assertNoRepairMarker(instance);
            }
            final String rows = headMissDepartureRows(true, 56);
            assertDepartedViewRows(rows);
            assertNoRefreshFaults("lv");
            for (int restart = 0; restart < 2; restart++) {
                restartLiveViewsAndRecover();
                assertDepartedViewRestored(rows);
            }
        });
    }

    @Test
    public void testAnEndOfTableRepairThatDeletesTheNewestRangeOverALostDayRestoresRetainedRows() throws Exception {
        // The pure delete of the newest three minutes emits nothing, and its rebuild warmed its
        // state from one minute below them, which is not the state at the new frontier 00:56: a row
        // at 00:56 reads the row at 00:55, one frame below it. The rebuild derives that state ahead
        // of its commit from the frame floor 00:55 - no complete root sits within the frame - and
        // after the commit truncates the root at 00:59 the splice kept and seals 00:56 on it. Every
        // restart restores from that root over the day the base lost, where it once retired the
        // timeline and the restatement guard stopped the view on every restart. A row committed at
        // exactly the frontier after the first restart folds over the restored root.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitTwoBatchesOverTheLostDay(job, 59);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                final LogCapture capture = new LogCapture();
                capture.start();
                try {
                    assertZeroRowDepartureSealsTheFrontier(
                            job,
                            () -> replaceBaseRange(ts("2026-01-01T00:57:00.000000Z"), ts("2026-01-01T01:00:00.000000Z"), Double.NaN),
                            "2026-01-01T00:56:00.000000Z",
                            FRONTIER_STATE_WARM_UP,
                            1,
                            // The replay reads 00:56; the warm-up 00:55 and 00:56.
                            3,
                            3,
                            false,
                            """
                                    2025-12-31T23:55:00.000000Z -> 1
                                    2026-01-01T00:56:00.000000Z -> 58
                                    """
                    );
                    capture.drain();
                    capture.assertLogged("live view O3 head miss derived the frontier state [view=lv, frontier=2026-01-01T00:56:00.000000Z, "
                            + "floor=2026-01-01T00:55:00.000000Z, openBelow=false, route=warm-up, rootMaxTs=2025-12-31T23:55:00.000000Z, rows=2]");
                } finally {
                    capture.stop();
                }
            }
            final String rows = headMissDepartureRows(true, 56);
            assertDepartedViewRows(rows);
            // The frame of the row at the frontier holds 00:55, 00:56 and itself.
            assertTieAtTheFrontierFoldsOntoTheRestoredRoot(
                    rows,
                    "('2026-01-01T00:56:00.000000Z', 'a', 1.0), ('2026-01-01T01:05:00.000000Z', 'a', 4.0)",
                    rows + """
                            2026-01-01T00:56:00.000000Z\ta\t1.0\t3.0
                            2026-01-01T01:05:00.000000Z\ta\t4.0\t4.0
                            """
            );
        });
    }

    @Test
    public void testAnEndOfTableRepairWhoseFrontierSitsOnALostDaySealsThePhysicalState() throws Exception {
        // The view keeps two rows of 2025-12-31, the day the base lost, under a root at 23:50, and
        // every base row of 01-01 up to 00:30 fails the filter. The departure of 00:31 to 00:33
        // leaves the frontier at 12-31T23:55, so the frame the rebuild warms from 23:54 holds no
        // base row any more: it seals the frontier on what the base still holds, the state any
        // repair of that row computes, not the lost 0.5. Every restart restores over the lost day,
        // and a row at 23:55 after the first one reads only itself.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitAndRefresh(job, "('2025-12-31T23:50:00.000000Z', 'a', 0.5)");
                commitAndRefresh(job, "('2025-12-31T23:55:00.000000Z', 'a', 0.5)");
                execute("ALTER TABLE base DROP PARTITION LIST '2025-12-31'");
                drainWalQueue();
                driveRefreshToQuiescence(job);
                commitAndRefresh(job, minuteRowsOfA(0, 30, "-1.0") + ", " + minuteRowsOfA(31, 33, "1.0"));
                final LogCapture capture = new LogCapture();
                capture.start();
                try {
                    assertZeroRowDepartureSealsTheFrontier(
                            job,
                            () -> replaceBaseRange(ts("2026-01-01T00:31:00.000000Z"), ts("2026-01-01T00:34:00.000000Z"), Double.NaN),
                            "2025-12-31T23:55:00.000000Z",
                            FRONTIER_STATE_WARM_UP,
                            0,
                            // The replay reads 00:30, which the filter drops; the warm-up reads nothing.
                            1,
                            2,
                            false,
                            """
                                    2025-12-31T23:50:00.000000Z -> 1
                                    2025-12-31T23:55:00.000000Z -> 2
                                    """
                    );
                    capture.drain();
                    capture.assertLogged("live view O3 head miss derived the frontier state [view=lv, frontier=2025-12-31T23:55:00.000000Z, "
                            + "floor=2025-12-31T23:54:00.000000Z, openBelow=false, route=warm-up, rootMaxTs=2025-12-31T23:50:00.000000Z, rows=0]");
                } finally {
                    capture.stop();
                }
            }
            final String rows = """
                    ts\tsym\tx\ts
                    2025-12-31T23:50:00.000000Z\ta\t0.5\t0.5
                    2025-12-31T23:55:00.000000Z\ta\t0.5\t0.5
                    """;
            assertTieAtTheFrontierFoldsOntoTheRestoredRoot(
                    rows,
                    "('2025-12-31T23:55:00.000000Z', 'a', 1.0)",
                    rows + "2025-12-31T23:55:00.000000Z\ta\t1.0\t1.0\n"
            );
        });
    }

    @Test
    public void testAnEndOfTableRepairThatLeavesOneRowOpensAFreshHistoryThere() throws Exception {
        // One commit of 00:57 to 00:59 seals the only root at 00:59, and the departure of 00:58 and
        // 00:59 leaves the view one row: the rebuild warms that row's frame, truncates the root
        // above it and opens a fresh history at 00:57.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitAndRefresh(job, minuteRowsOfA(57, 59, "1.0"));
                assertZeroRowDepartureSealsTheFrontier(
                        job,
                        () -> replaceBaseRange(ts("2026-01-01T00:58:00.000000Z"), ts("2026-01-01T01:00:00.000000Z"), Double.NaN),
                        "2026-01-01T00:57:00.000000Z",
                        FRONTIER_STATE_WARM_UP,
                        1,
                        // The replay reads 00:57; the warm-up reads it again.
                        2,
                        0,
                        true,
                        "2026-01-01T00:57:00.000000Z -> 1\n"
                );
            }
            final String rows = "ts\tsym\tx\ts\n2026-01-01T00:57:00.000000Z\ta\t1.0\t1.0\n";
            assertTieAtTheFrontierFoldsOntoTheRestoredRoot(
                    rows,
                    "('2026-01-01T00:57:00.000000Z', 'a', 1.0)",
                    rows + "2026-01-01T00:57:00.000000Z\ta\t1.0\t2.0\n"
            );
        });
    }

    @Test
    public void testAnEndOfTableRepairInTheFarFutureRestoresRetainedRows() throws Exception {
        // The item's departure over the day the base lost, moved to the last two days of year
        // 9999: the same derivation, from the frame floor 00:55, and the same seal at 00:56.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitAndRefresh(job, "('9999-12-30T23:55:00.000000Z', 'a', 0.5)");
                execute("ALTER TABLE base DROP PARTITION LIST '9999-12-30'");
                drainWalQueue();
                driveRefreshToQuiescence(job);
                commitAndRefresh(job, inTheFarFuture(minuteRowsOfA(0, 30, "1.0")));
                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                commitAndRefresh(job, inTheFarFuture(minuteRowsOfA(31, 59, "1.0")));
                assertZeroRowDepartureSealsTheFrontier(
                        job,
                        () -> replaceBaseRange(ts("9999-12-31T00:57:00.000000Z"), ts("9999-12-31T01:00:00.000000Z"), Double.NaN),
                        "9999-12-31T00:56:00.000000Z",
                        FRONTIER_STATE_WARM_UP,
                        1,
                        3,
                        3,
                        false,
                        """
                                9999-12-30T23:55:00.000000Z -> 1
                                9999-12-31T00:56:00.000000Z -> 58
                                """
                );
            }
            final String rows = inTheFarFuture(headMissDepartureRows(true, 56));
            assertDepartedViewRows(rows);
            assertTieAtTheFrontierFoldsOntoTheRestoredRoot(
                    rows,
                    "('9999-12-31T00:56:00.000000Z', 'a', 1.0)",
                    rows + "9999-12-31T00:56:00.000000Z\ta\t1.0\t3.0\n"
            );
        });
    }

    @Test
    public void testAnEndOfTableRepairDownToTheEpochRowOpensAFreshHistory() throws Exception {
        // The departure leaves one row, at the epoch, below the only root: the frame floor sits a
        // minute before the epoch, and the rebuild warms the frame from there, truncates the root
        // the splice kept and opens a fresh history at the epoch.
        assertTheDepartureDownToTheEpochOpensAFreshHistory("BEGINNING", "1969-12-31T23:59:00.000000Z");
    }

    @Test
    public void testAnEndOfTableRepairDownToTheEpochRowOfAViewStartingThereOpensAFreshHistory() throws Exception {
        // As above, under a view that starts at the epoch: the frame floor clamps to the view's
        // start.
        assertTheDepartureDownToTheEpochOpensAFreshHistory("'1970-01-01T00:00:00.000000Z'", "1970-01-01T00:00:00.000000Z");
    }

    @Test
    public void testAnEndOfTableRepairThatEmptiesAZonedDaySealsTheShortDayBelowIt() throws Exception {
        // ANCHOR DAILY '00:00' Europe/Berlin over the 2025-03-30 spring-forward, a day of 23 hours
        // that starts at 03-29T23:00Z. The departure empties the next day from 03-30T22:00Z, and
        // the new frontier 21:59Z is the short day's last row: the rebuild folds that day from
        // its start, 24 base rows, and a row at the frontier after the restart reads all of them.
        // The 03-29T22:30Z row reads 23:30 on 03-29 and stays on that day's run.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS SELECT ts, sym, x, sum(x) OVER w s FROM base "
                    + "WHERE x > 0 WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00' 'Europe/Berlin')");
            final StringSink shortDay = new StringSink();
            final StringSink shortDayRows = new StringSink();
            for (int hour = 0; hour <= 21; hour++) {
                final String ts = "2025-03-30T" + (hour < 10 ? "0" : "") + hour + ":00:00.000000Z";
                shortDay.put(", ('").put(ts).put("', 'a', 1.0)");
                shortDayRows.put(ts).put("\ta\t1.0\t").put(hour + 2).put(".0\n");
            }
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitAndRefresh(job, "('2025-03-28T12:00:00.000000Z', 'a', 0.25)");
                commitAndRefresh(
                        job,
                        "('2025-03-29T22:30:00.000000Z', 'a', 0.5), ('2025-03-29T23:30:00.000000Z', 'a', 1.0)" + shortDay
                                + ", ('2025-03-30T21:59:00.000000Z', 'a', 1.0), ('2025-03-30T22:00:00.000000Z', 'a', 1.0), "
                                + "('2025-03-30T22:30:00.000000Z', 'a', 1.0), ('2025-03-31T00:00:00.000000Z', 'a', 1.0), "
                                + "('2025-03-31T01:00:00.000000Z', 'a', 1.0)"
                );
                final LogCapture capture = new LogCapture();
                capture.start();
                try {
                    assertZeroRowDepartureSealsTheFrontier(
                            job,
                            () -> replaceBaseRange(ts("2025-03-30T22:00:00.000000Z"), ts("2025-03-31T02:00:00.000000Z"), Double.NaN),
                            "2025-03-30T21:59:00.000000Z",
                            FRONTIER_STATE_WARM_UP,
                            0,
                            24,
                            2,
                            false,
                            """
                                    2025-03-28T12:00:00.000000Z -> 1
                                    2025-03-30T21:59:00.000000Z -> 26
                                    """
                    );
                    capture.drain();
                    capture.assertLogged("live view O3 head miss derived the frontier state [view=lv, frontier=2025-03-30T21:59:00.000000Z, "
                            + "floor=2025-03-29T23:00:00.000000Z, openBelow=false, route=warm-up, rootMaxTs=2025-03-28T12:00:00.000000Z, rows=24]");
                } finally {
                    capture.stop();
                }
            }
            final String rows = """
                    ts\tsym\tx\ts
                    2025-03-28T12:00:00.000000Z\ta\t0.25\t0.25
                    2025-03-29T22:30:00.000000Z\ta\t0.5\t0.5
                    2025-03-29T23:30:00.000000Z\ta\t1.0\t1.0
                    """ + shortDayRows + "2025-03-30T21:59:00.000000Z\ta\t1.0\t24.0\n";
            assertTieAtTheFrontierFoldsOntoTheRestoredRoot(
                    rows,
                    "('2025-03-30T21:59:00.000000Z', 'a', 1.0)",
                    rows + "2025-03-30T21:59:00.000000Z\ta\t1.0\t25.0\n"
            );
        });
    }

    @Test
    public void testAnEndOfTableRepairThatDeletesTheNewestRangeWithNoRootAboveTheFrontierSealsItsFrontier() throws Exception {
        // One batch carries every row from 00:00 to 00:59 and the cadence seals no root over it,
        // so the newest root, 23:55, sits below the new frontier 00:56 and the splice captured
        // none. The splice moved the generation's base coordinate to the repair commit, and the
        // rebuild seals 00:56 on the state it warmed from the frame floor 00:55, with nothing to
        // truncate. Every restart restores from that root, where it once retired the timeline and
        // rebuilt the view from the base.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitAndRefresh(job, "('2025-12-31T23:55:00.000000Z', 'a', 0.5)");
                commitAndRefresh(job, minuteRowsOfA(0, 59, "1.0"));
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                assertCheckpointLadder(instance, false, "2025-12-31T23:55:00.000000Z -> 1\n");
                assertZeroRowDepartureSealsTheFrontier(
                        job,
                        () -> replaceBaseRange(ts("2026-01-01T00:57:00.000000Z"), ts("2026-01-01T01:00:00.000000Z"), Double.NaN),
                        "2026-01-01T00:56:00.000000Z",
                        FRONTIER_STATE_WARM_UP,
                        0,
                        3,
                        2,
                        false,
                        """
                                2025-12-31T23:55:00.000000Z -> 1
                                2026-01-01T00:56:00.000000Z -> 58
                                """
                );
            }
            assertRecomputedRowsThenTheTieAtTheFrontierRestore(headMissDepartureRows(true, 56));
        });
    }

    @Test
    public void testAnEndOfTableRepairThatDeletesTheNewestRangeWithNoRootAtTheFrontierSealsItsFrontier() throws Exception {
        // The item's pure delete over a base that lost nothing: the splice kept the root at 00:59
        // above the new frontier 00:56, and the newest root at or below the frontier is 23:55. The
        // rebuild truncates the root at 00:59 and seals 00:56 on the state it warmed from 00:55.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitAndRefresh(job, "('2025-12-31T23:55:00.000000Z', 'a', 0.5)");
                commitAndRefresh(job, minuteRowsOfA(0, 30, "1.0"));
                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                commitAndRefresh(job, minuteRowsOfA(31, 59, "1.0"));
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                assertCheckpointLadder(instance, false, """
                        2025-12-31T23:55:00.000000Z -> 1
                        2026-01-01T00:59:00.000000Z -> 61
                        """);
                assertZeroRowDepartureSealsTheFrontier(
                        job,
                        () -> replaceBaseRange(ts("2026-01-01T00:57:00.000000Z"), ts("2026-01-01T01:00:00.000000Z"), Double.NaN),
                        "2026-01-01T00:56:00.000000Z",
                        FRONTIER_STATE_WARM_UP,
                        1,
                        3,
                        3,
                        false,
                        """
                                2025-12-31T23:55:00.000000Z -> 1
                                2026-01-01T00:56:00.000000Z -> 58
                                """
                );
            }
            assertRecomputedRowsThenTheTieAtTheFrontierRestore(headMissDepartureRows(true, 56));
        });
    }

    @Test
    public void testATieAtTheSealedFrontierBeforeARestartRepairsOverTheSealedRoot() throws Exception {
        // The intact-base departure seals 00:56, and a row at exactly 00:56 lands before any
        // restart: the runtime has seen the departed rows, so the row is late. Its repair rebuilds
        // from the frame floor 00:55, re-emits both rows at 00:56 and re-versions the root the
        // departure sealed there, which the restart then restores.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitAndRefresh(job, "('2025-12-31T23:55:00.000000Z', 'a', 0.5)");
                commitAndRefresh(job, minuteRowsOfA(0, 30, "1.0"));
                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                commitAndRefresh(job, minuteRowsOfA(31, 59, "1.0"));
                assertZeroRowDepartureSealsTheFrontier(
                        job,
                        () -> replaceBaseRange(ts("2026-01-01T00:57:00.000000Z"), ts("2026-01-01T01:00:00.000000Z"), Double.NaN),
                        "2026-01-01T00:56:00.000000Z",
                        FRONTIER_STATE_WARM_UP,
                        1,
                        3,
                        3,
                        false,
                        """
                                2025-12-31T23:55:00.000000Z -> 1
                                2026-01-01T00:56:00.000000Z -> 58
                                """
                );
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                final long headMissRepairsBefore = countHeadMissRepairs(job);
                final long derivationsBefore = job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_FRONTIER_STATE_WARM_UP_COUNT)
                        + job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_FRONTIER_STATE_RESTORE_COUNT);
                commitAndRefresh(job, "('2026-01-01T00:56:00.000000Z', 'a', 1.0)");
                Assert.assertEquals("the late tie takes a head-miss rebuild", headMissRepairsBefore + 1, countHeadMissRepairs(job));
                Assert.assertEquals(
                        "which emits rows and derives nothing",
                        derivationsBefore,
                        job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_FRONTIER_STATE_WARM_UP_COUNT)
                                + job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_FRONTIER_STATE_RESTORE_COUNT)
                );
                assertCheckpointLadder(instance, false, """
                        2025-12-31T23:55:00.000000Z -> 1
                        2026-01-01T00:56:00.000000Z -> 59
                        """);
                assertNoRefreshFaults("lv");
            }
            // The view folds a timestamp tie row by row: the first row at 00:56 reads 00:55 and
            // itself, the tie the three of them.
            final String rows = headMissDepartureRows(true, 56) + "2026-01-01T00:56:00.000000Z\ta\t1.0\t3.0\n";
            assertDepartedViewRows(rows);
            restartLiveViewsAndRecover();
            assertRestoredFromTimeline("lv");
            assertDepartedViewRows(rows);
        });
    }

    @Test
    public void testAnEndOfTableRepairThatDeletesTheNewestRangeSealsTheFrontierOfAnInMemoryView() throws Exception {
        // The intact-base departure under a view that keeps an in-memory tier.
        assertTheNewestRangeDepartureSealsTheFrontier("IN MEMORY 1h ");
    }

    @Test
    public void testAnEndOfTableRepairThatDeletesTheNewestRangeSealsItsFrontierWithJitDisabled() throws Exception {
        // The intact-base departure with the filter the replay and the fold share compiled without JIT.
        node1.setProperty(PropertyKey.CAIRO_SQL_JIT_MODE, SqlJitMode.toString(SqlJitMode.JIT_MODE_DISABLED));
        assertTheNewestRangeDepartureSealsTheFrontier("");
    }

    @Test
    public void testAnEndOfTableRepairThatDeletesTheNewestRangeWithNoRootBelowItOpensAFreshHistoryAtItsFrontier() throws Exception {
        // The first seal took every row at once, so the only root sits at 00:59, above the new
        // frontier, and none sits at or below it. The frontier truncate keeps no root and retires
        // the timeline, and the seal of the state the rebuild warmed from 00:55 opens a fresh
        // history at 00:56.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitAndRefresh(job, minuteRowsOfA(0, 59, "1.0"));
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                assertCheckpointLadder(instance, false, "2026-01-01T00:59:00.000000Z -> 60\n");
                assertZeroRowDepartureSealsTheFrontier(
                        job,
                        () -> replaceBaseRange(ts("2026-01-01T00:57:00.000000Z"), ts("2026-01-01T01:00:00.000000Z"), Double.NaN),
                        "2026-01-01T00:56:00.000000Z",
                        FRONTIER_STATE_WARM_UP,
                        1,
                        3,
                        0,
                        true,
                        "2026-01-01T00:56:00.000000Z -> 57\n"
                );
            }
            assertRecomputedRowsThenTheTieAtTheFrontierRestore(headMissDepartureRows(false, 56));
        });
    }

    @Test
    public void testAnEndOfTableRepairThatDeletesTheNewestRowKeepsTheRootAtTheNewFrontier() throws Exception {
        // The departed row's own root sits above the root at the row that becomes the frontier.
        // The repair emits nothing, and the root at the frontier counts every row of the view, so
        // it truncates the spliced timeline there: the root above goes, the one at the frontier
        // stays. Each restart restores from it, and rows committed in order after a restart, below
        // where the departed row was, seal without the root above the frontier in their way.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitRootAtTheFrontierThenRowsAboveIt(job, true);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                assertCheckpointLadder(instance, false, """
                        2025-12-31T23:55:00.000000Z -> 1
                        2026-01-01T00:56:00.000000Z -> 58
                        2026-01-01T00:59:00.000000Z -> 59
                        """);
                final long generationBefore = publishedCheckpointGeneration(instance);
                final long headMissRepairsBefore = countHeadMissRepairs(job);
                final long resetsBefore = instance.getCheckpointTimelineResets();
                replaceBaseRange(ts("2026-01-01T00:59:00.000000Z"), ts("2026-01-01T01:00:00.000000Z"), Double.NaN);
                drainWalQueue();
                driveRefreshToQuiescence(job);
                Assert.assertEquals("the head-miss rebuild repairs the departure", headMissRepairsBefore + 1, countHeadMissRepairs(job));
                Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_FRONTIER_TRUNCATE_COUNT));
                Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_ZERO_ROW_KEEP_COUNT));
                Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_ZERO_ROW_RETIRE_COUNT));
                Assert.assertEquals("the splice and the truncate", generationBefore + 2, publishedCheckpointGeneration(instance));
                Assert.assertEquals(resetsBefore, instance.getCheckpointTimelineResets());
                assertCheckpointLadder(instance, false, """
                        2025-12-31T23:55:00.000000Z -> 1
                        2026-01-01T00:56:00.000000Z -> 58
                        """);
                assertNoRepairMarker(instance);
                // The head goes back on the root the truncate kept, at the repair's commit, with
                // no root identity a resume could reuse the rebuild's runtime under.
                Assert.assertEquals(ts("2026-01-01T00:56:00.000000Z"), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(6, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointRootId());
            }
            final String rows = headMissDepartureRows(true, 56);
            assertDepartedViewRows(rows);
            assertNoRefreshFaults("lv");
            restartLiveViewsAndRecover();
            assertDepartedViewRestored(rows);
            final String grownRows = rows + """
                    2026-01-01T00:58:10.000000Z\ta\t1.0\t1.0
                    2026-01-01T00:58:20.000000Z\ta\t1.0\t2.0
                    2026-01-01T00:58:30.000000Z\ta\t1.0\t3.0
                    """;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                for (int second = 10; second <= 30; second += 10) {
                    setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                    commitAndRefresh(job, "('2026-01-01T00:58:" + second + ".000000Z', 'a', 1.0)");
                }
                Assert.assertEquals(0, instance.getCheckpointSealFailures());
                Assert.assertEquals(0, instance.getCheckpointTimelineResets());
                assertCheckpointLadder(instance, false, """
                        2025-12-31T23:55:00.000000Z -> 1
                        2026-01-01T00:56:00.000000Z -> 58
                        2026-01-01T00:58:10.000000Z -> 59
                        2026-01-01T00:58:20.000000Z -> 60
                        2026-01-01T00:58:30.000000Z -> 61
                        """);
            }
            assertDepartedViewRows(grownRows);
            restartLiveViewsAndRecover();
            assertDepartedViewRestored(grownRows);
        });
    }

    @Test
    public void testAnEndOfTableRepairThatDeletesTheRowsAboveAGrownRootSealsItsFrontier() throws Exception {
        // The root at the row that becomes the frontier was sealed before a row of another key
        // tied it, so it counts one row fewer than the view holds there: no restore may stand on
        // it. The rebuild warms the state at 00:56 from the frame floor 00:55 instead, truncates
        // the grown root with every root at or above the frontier, and seals 00:56 on that state,
        // counting both rows there.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitAndRefresh(job, "('2025-12-31T23:55:00.000000Z', 'a', 0.5)");
                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                commitAndRefresh(job, minuteRowsOfA(0, 56, "1.0"));
                commitAndRefresh(job, "('2026-01-01T00:56:00.000000Z', 'b', 1.0)");
                commitAndRefresh(job, minuteRowsOfA(57, 59, "1.0"));
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                assertCheckpointLadder(instance, false, """
                        2025-12-31T23:55:00.000000Z -> 1
                        2026-01-01T00:56:00.000000Z -> 58
                        """);
                assertZeroRowDepartureSealsTheFrontier(
                        job,
                        () -> replaceBaseRange(ts("2026-01-01T00:57:00.000000Z"), ts("2026-01-01T01:00:00.000000Z"), Double.NaN),
                        "2026-01-01T00:56:00.000000Z",
                        FRONTIER_STATE_WARM_UP,
                        1,
                        // The replay reads both rows at 00:56; the warm-up 00:55 and both of them.
                        5,
                        3,
                        false,
                        """
                                2025-12-31T23:55:00.000000Z -> 1
                                2026-01-01T00:56:00.000000Z -> 59
                                """
                );
            }
            final String rows = headMissDepartureRows(true, 56) + "2026-01-01T00:56:00.000000Z\tb\t1.0\t1.0\n";
            assertRecomputedRowsThenTheTieAtTheFrontierRestore(rows);
        });
    }

    @Test
    public void testAnEndOfTableRepairThatKeepsOneRowOfTheNewestRangeRestoresAnAnchoredWindow() throws Exception {
        // A daily anchored sum, whose rebuild reads the kept row's day from its start. The root
        // below the correction sits on the previous day under rows the filter drops, which a
        // resume from it would read, so the rebuild from the day's start is the cheaper repair.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS SELECT ts, sym, x, sum(x) OVER w s FROM base "
                    + "WHERE x > 0 WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitAndRefresh(job, "('2025-12-31T20:00:00.000000Z', 'a', 0.5)");
                final StringSink droppedRows = new StringSink();
                for (int minute = 0; minute < 60; minute++) {
                    droppedRows.put(minute > 0 ? ", " : "").put("('2025-12-31T23:").put(minute < 10 ? "0" : "").put(minute).put(":30.000000Z', 'a', -1.0)");
                }
                commitAndRefresh(job, droppedRows.toString());
                commitAndRefresh(job, minuteRowsOfA(0, 30, "1.0"));
                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                commitAndRefresh(job, minuteRowsOfA(31, 59, "1.0"));
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                assertCheckpointLadder(instance, false, """
                        2025-12-31T20:00:00.000000Z -> 1
                        2026-01-01T00:59:00.000000Z -> 61
                        """);
                final long headMissRepairsBefore = countHeadMissRepairs(job);
                replaceBaseRange(ts("2026-01-01T00:57:00.000000Z"), ts("2026-01-01T01:00:00.000000Z"), 5.0);
                drainWalQueue();
                driveRefreshToQuiescence(job);
                Assert.assertEquals("the head-miss rebuild repairs the departure", headMissRepairsBefore + 1, countHeadMissRepairs(job));
                Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_FRONTIER_TRUNCATE_COUNT));
                assertCheckpointLadder(instance, false, """
                        2025-12-31T20:00:00.000000Z -> 1
                        2026-01-01T00:57:00.000000Z -> 59
                        """);
            }
            // The day's sum restarts at 00:00 and adds 1.0 a minute up to 00:56, 57.0, and the
            // kept row adds 5.0.
            final StringSink rows = new StringSink();
            rows.put("ts\tsym\tx\ts\n2025-12-31T20:00:00.000000Z\ta\t0.5\t0.5\n");
            for (int minute = 0; minute <= 56; minute++) {
                rows.put("2026-01-01T00:").put(minute < 10 ? "0" : "").put(minute).put(":00.000000Z\ta\t1.0\t").put(minute + 1).put(".0\n");
            }
            rows.put("2026-01-01T00:57:00.000000Z\ta\t5.0\t62.0\n");
            assertDepartedViewRows(rows.toString());
            assertNoRefreshFaults("lv");
            for (int restart = 0; restart < 2; restart++) {
                restartLiveViewsAndRecover();
                assertDepartedViewRestored(rows.toString());
            }
        });
    }

    @Test
    public void testAnEndOfTableRepairThatKeepsOneRowOfTheNewestRangeRestoresAnInMemoryView() throws Exception {
        assertNewestRangeKeepingOneRowMatchesRecompute(HEAD_MISS_DEPARTURE_VIEW_SQL, "IN MEMORY 1h ");
    }

    @Test
    public void testAnEndOfTableRepairThatKeepsOneRowOfTheNewestRangeRestoresAProjectedWindow() throws Exception {
        assertNewestRangeKeepingOneRowMatchesRecompute(
                "SELECT ts, sym, x, sum(x) OVER (PARTITION BY sym ORDER BY ts RANGE BETWEEN '1' MINUTE PRECEDING AND CURRENT ROW) * 2 s "
                        + "FROM base WHERE x > 0",
                ""
        );
    }

    @Test
    public void testAnEndOfTableRepairThatKeepsOneRowOfTheNewestRangeRestoresARangeAverage() throws Exception {
        // A ten-minute frame: the rebuild warms its state from ten minutes below the kept row.
        assertNewestRangeKeepingOneRowMatchesRecompute(
                "SELECT ts, sym, x, avg(x) OVER (PARTITION BY sym ORDER BY ts RANGE BETWEEN '10' MINUTE PRECEDING AND CURRENT ROW) s "
                        + "FROM base WHERE x > 0",
                ""
        );
    }

    @Test
    public void testAnEndOfTableRepairThatKeepsOneRowOfTheNewestRangeRestoresARangeCount() throws Exception {
        assertNewestRangeKeepingOneRowMatchesRecompute(
                "SELECT ts, sym, x, count(*) OVER (PARTITION BY sym ORDER BY ts RANGE BETWEEN '1' MINUTE PRECEDING AND CURRENT ROW) s "
                        + "FROM base WHERE x > 0",
                ""
        );
    }

    @Test
    public void testAnEndOfTableRepairThatKeepsOneRowOfTheNewestRangeRestoresARangeMaximum() throws Exception {
        assertNewestRangeKeepingOneRowMatchesRecompute(
                "SELECT ts, sym, x, max(x) OVER (PARTITION BY sym ORDER BY ts RANGE BETWEEN '1' MINUTE PRECEDING AND CURRENT ROW) s "
                        + "FROM base WHERE x > 0",
                ""
        );
    }

    @Test
    public void testAnEndOfTableRepairThatKeepsOneRowOfTheNewestRangeRestoresRetainedRows() throws Exception {
        // A mat-view style REPLACE_RANGE over the newest three minutes keeps one row with a new
        // value, below the root the cadence sealed at the newest row, over a base that lost the
        // 2025-12-31 day the view keeps. The rebuild splices [00:57, +inf) and emits the kept row,
        // so its output ends below the spliced root at 00:59: the repair truncates the timeline
        // at the new frontier 00:57 and seals a root there. Each restart restores from that root;
        // the rebuild it would fall back to is the one the restatement guard refuses.
        assertNewestRangeKeepingOneRowRestores(5.0, "6.0");
    }

    @Test
    public void testAnEndOfTableRepairThatKeepsOneRowOfTheNewestRangeSealsTheFrontierStateOfARangeAverage() throws Exception {
        // The same over a base that lost nothing, under a ten-minute average whose frame reaches
        // below the kept row: the rows committed in order after the restart give what a recompute
        // gives.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final String viewSql = "SELECT ts, sym, x, avg(x) OVER (PARTITION BY sym ORDER BY ts RANGE BETWEEN '10' MINUTE PRECEDING AND CURRENT ROW) s "
                + "FROM base WHERE x > 0";
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " + viewSql);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitAndRefresh(job, "('2025-12-31T23:55:00.000000Z', 'a', 0.5)");
                commitAndRefresh(job, minuteRowsOfA(0, 30, "1.0"));
                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                commitAndRefresh(job, minuteRowsOfA(31, 59, "1.0"));
                departTheNewestRangeKeepingOneRow(job);
            }
            restartLiveViewsAndRecover();
            assertRestoredFromTimeline("lv");
            commitRowsAboveTheFrontierRoot();
            assertMatchesRecompute(viewSql);
            assertNoRefreshFaults("lv");
            restartLiveViewsAndRecover();
            assertRestoredFromTimeline("lv");
            assertMatchesRecompute(viewSql);
            assertNoRefreshFaults("lv");
        });
    }

    @Test
    public void testAnEndOfTableRepairThatKeepsOneRowOfTheNewestRangeSealsTheFrontierStateRowsFoldOnto() throws Exception {
        // The root the seal appends at the new frontier 00:57 holds the window state there: the kept
        // row at its new value, and none of the rows the REPLACE_RANGE deleted. A restart restores
        // the runtime from that root, so rows committed in order after it fold onto that state.
        // 00:57:30's one-minute frame holds the kept row, 5.0 + 1.0, and 00:59:30's holds no row
        // but its own, where the deleted 00:59 was. The restart after them restores again, over the
        // day the base lost.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitTwoBatchesOverTheLostDay(job, 59);
                departTheNewestRangeKeepingOneRow(job);
            }
            final String rows = headMissDepartureRows(true, 56) + "2026-01-01T00:57:00.000000Z\ta\t5.0\t6.0\n";
            restartLiveViewsAndRecover();
            assertDepartedViewRestored(rows);
            commitRowsAboveTheFrontierRoot();
            final String grownRows = rows + """
                    2026-01-01T00:57:30.000000Z\ta\t1.0\t6.0
                    2026-01-01T00:59:30.000000Z\ta\t1.0\t1.0
                    """;
            assertDepartedViewRows(grownRows);
            assertNoRefreshFaults("lv");
            restartLiveViewsAndRecover();
            assertDepartedViewRestored(grownRows);
        });
    }

    @Test
    public void testAnEndOfTableRepairThatKeepsOneRowOfTheNewestRangeRestoresWithJitDisabled() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_JIT_MODE, SqlJitMode.toString(SqlJitMode.JIT_MODE_DISABLED));
        assertNewestRangeKeepingOneRowMatchesRecompute(HEAD_MISS_DEPARTURE_VIEW_SQL, "");
    }

    @Test
    public void testAnEndOfTableRepairThatKeepsOneRowOfTheNewestRangeUnchangedRestoresRetainedRows() throws Exception {
        // The kept row holds the value it had, so only the rows above it change.
        assertNewestRangeKeepingOneRowRestores(1.0, "2.0");
    }

    @Test
    public void testAnEndOfTableRepairThatParksAndKeepsOneRowOfTheNewestRangeRestores() throws Exception {
        // A replay budget of one row parks the rebuild on its first turn: the turn that finishes
        // it truncates the spliced timeline at the new frontier and seals a root there.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertNewestRangeKeepingOneRowMatchesRecompute(HEAD_MISS_DEPARTURE_VIEW_SQL, "");
            capture.drain();
            capture.assertLogged("live view O3 repair yielded on its turn budget [view=lv");
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testAnEndOfTableRepairThatKeepsItsTimelineWithoutRowsReleasesBaseWalAfterIt() throws Exception {
        // The departure of the unsealed 00:59 emits nothing, and the root at the new frontier
        // 00:56 counts every row of the view, so the repair keeps the spliced timeline and
        // re-stamps the head on that root at its own commit. It freezes nothing: its rebuild's
        // runtime warmed only from a minute below 00:59, and the head names no root identity.
        // Rejected commits then move the head's base coordinate on the cadence with no root above
        // the frontier, and a late row below 00:59 resumes from the root at 00:56 read off disk.
        configureWalPurge();
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView();
            final String ladder = """
                    2025-12-31T23:55:00.000000Z -> 1
                    2026-01-01T00:56:00.000000Z -> 58
                    """;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitRootAtTheFrontierThenRowsAboveIt(job, false);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                assertCheckpointLadder(instance, false, ladder);
                replaceBaseRange(ts("2026-01-01T00:59:00.000000Z"), ts("2026-01-01T01:00:00.000000Z"), Double.NaN);
                drainWalQueue();
                driveRefreshToQuiescence(job);
                Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_ZERO_ROW_KEEP_COUNT));
                Assert.assertEquals(ts("2026-01-01T00:56:00.000000Z"), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(6, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointRootId());
                Assert.assertEquals(ts("2026-01-01T00:59:00.000000Z"), instance.getLatestSeenTs());
                assertCheckpointLadder(instance, false, ladder);
                assertTimelineBase(6);

                // seqTxn 7 to 9, each past the cadence.
                commitRejectedRowsPastTheCadence(job, instance, 1, 3, 7);
                assertCheckpointLadder(instance, false, ladder);
                Assert.assertEquals(8, instance.getCheckpointTimelineWalPurgeFloor());
                Assert.assertEquals("the purge must keep only the segments above the floor", 9 - 8, purgeBaseWalAndCountSegments());
                Assert.assertEquals(0, instance.getCheckpointSealFailures());
                Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT));

                // The late row's one-minute frame holds 00:56 too, which a resume reusing the
                // rebuild's runtime would not hold.
                commitAndRefresh(job, "('2026-01-01T00:56:30.000000Z', 'a', 2.0)");
                assertCheckpointLadder(instance, false, ladder + "2026-01-01T00:56:30.000000Z -> 59\n");
                commitRejectedRowsPastTheCadence(job, instance, 4, 5, 11);
                assertCheckpointLadder(instance, false, ladder + "2026-01-01T00:56:30.000000Z -> 59\n");
            }
            final String rows = headMissDepartureRows(true, 56) + "2026-01-01T00:56:30.000000Z\ta\t2.0\t3.0\n";
            assertDepartedViewRows(rows);
            assertNoRefreshFaults("lv");
            assertRejectedRowsBetweenTwoRestartsKeepTheTimeline(rows, 6, 13);
        });
    }

    @Test
    public void testAnEndOfTableRepairThatTruncatesAtItsFrontierReleasesBaseWalAfterIt() throws Exception {
        // The kept-row departure truncates the spliced timeline at its new frontier 00:57 and
        // seals a root there, with the newest timestamp the view has seen still at 00:59. Rejected
        // commits then move the head's base coordinate on the cadence, with no root above 00:57.
        configureWalPurge();
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitTwoBatchesOverTheLostDay(job, 59);
                departTheNewestRangeKeepingOneRow(job);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                Assert.assertEquals(ts("2026-01-01T00:57:00.000000Z"), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(5, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(ts("2026-01-01T00:59:00.000000Z"), instance.getLatestSeenTs());
                // seqTxn 6 to 8, each past the cadence.
                commitRejectedRowsPastTheCadence(job, instance, 1, 3, 6);
                assertCheckpointLadder(instance, false, """
                        2025-12-31T23:55:00.000000Z -> 1
                        2026-01-01T00:57:00.000000Z -> 59
                        """);
                Assert.assertEquals(ts("2026-01-01T00:57:00.000000Z"), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(7, instance.getCheckpointTimelineWalPurgeFloor());
                Assert.assertEquals("the purge must keep only the segments above the floor", 8 - 7, purgeBaseWalAndCountSegments());
                Assert.assertEquals(0, instance.getCheckpointSealFailures());
                Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT));
            }
            final String rows = headMissDepartureRows(true, 56) + "2026-01-01T00:57:00.000000Z\ta\t5.0\t6.0\n";
            assertDepartedViewRows(rows);
            assertNoRefreshFaults("lv");
            assertRejectedRowsBetweenTwoRestartsKeepTheTimeline(rows, 6, 9);
        });
    }

    @Test
    public void testAnEndOfTableRepairWhoseRestampPassesTheCooldownReleasesBaseWal() throws Exception {
        // The zero-row keep of the test above, under a seal cooldown that ends a microsecond on.
        // The re-stamp writes nothing, so the cooldown lets it through: the head goes back on the
        // root at the frontier, and each cycle without rows past the cadence republishes it at its
        // own commit and moves the base WAL floor up, rather than ask for a first root at the
        // newest timestamp the view has seen, above its newest row.
        configureWalPurge();
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView();
            final String ladder = """
                    2025-12-31T23:55:00.000000Z -> 1
                    2026-01-01T00:56:00.000000Z -> 58
                    """;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitRootAtTheFrontierThenRowsAboveIt(job, false);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                hooks(job).setSimulateHeadMissHeadSealStart(() -> instance.setSealCooldownUntilUs(currentMicros + 1));
                replaceBaseRange(ts("2026-01-01T00:59:00.000000Z"), ts("2026-01-01T01:00:00.000000Z"), Double.NaN);
                drainWalQueue();
                driveRefreshToQuiescence(job);
                hooks(job).setSimulateHeadMissHeadSealStart(null);
                Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_ZERO_ROW_KEEP_COUNT));
                Assert.assertEquals(ts("2026-01-01T00:56:00.000000Z"), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals("at the repair's commit", 6, instance.getHeadCheckpointBaseSeqTxn());
                assertCheckpointLadder(instance, false, ladder);
                assertTimelineBase(6);

                // seqTxn 7 to 9, each past the cadence.
                commitRejectedRowsPastTheCadence(job, instance, 1, 3, 7);
                assertCheckpointLadder(instance, false, ladder);
                Assert.assertEquals(ts("2026-01-01T00:56:00.000000Z"), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(8, instance.getCheckpointTimelineWalPurgeFloor());
                Assert.assertEquals("the purge must keep only the segments above the floor", 9 - 8, purgeBaseWalAndCountSegments());
                Assert.assertEquals(0, instance.getCheckpointSealFailures());
                Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT));
            }
            final String rows = headMissDepartureRows(true, 56);
            assertDepartedViewRows(rows);
            assertNoRefreshFaults("lv");
            assertRejectedRowsBetweenTwoRestartsKeepTheTimeline(rows, 4, 10);
        });
    }

    @Test
    public void testAnEndOfTableRepairWhoseRestampTheViewTableReadDeclinesHoldsBaseWalUntilARestart() throws Exception {
        // The zero-row keep of the tests above, with its re-stamp declined by a read of the view's
        // table that fails: the head stays clear over a timeline whose newest root sits at the
        // frontier. Each cycle without rows then declines a first root at the newest timestamp the
        // view has seen, above its newest row, and holds the base WAL where the repair left it. The
        // restart restores from the root at the frontier, which puts the head back on it, and the
        // cycles after it move its base coordinate again.
        configureWalPurge();
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final AtomicReference<String> failedOpenDir = new AtomicReference<>();
        final AtomicInteger failedOpens = new AtomicInteger();
        final TestFilesFacadeImpl ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                final String dirName = failedOpenDir.get();
                if (dirName != null
                        && Utf8s.containsAscii(name, Files.SEPARATOR + dirName + Files.SEPARATOR)
                        && Utf8s.endsWithAscii(name, Files.SEPARATOR + TableUtils.TXN_FILE_NAME)
                        && failedOpenDir.compareAndSet(dirName, null)) {
                    failedOpens.incrementAndGet();
                    return -1;
                }
                return super.openRO(name);
            }
        };
        assertMemoryLeak(ff, () -> {
            createHeadMissDepartureView();
            final String ladder = """
                    2025-12-31T23:55:00.000000Z -> 1
                    2026-01-01T00:56:00.000000Z -> 58
                    """;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitRootAtTheFrontierThenRowsAboveIt(job, false);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                hooks(job).setSimulateHeadMissHeadSealStart(() -> {
                    // No pooled reader may serve the read, so it opens the table's files. The
                    // rebuild holds a reader of the base, so only the view's are released.
                    Assert.assertTrue(engine.lockReadersByTableToken(instance.getLiveViewToken()));
                    engine.unlockReaders(instance.getLiveViewToken());
                    failedOpenDir.set(instance.getLiveViewToken().getDirName());
                });
                replaceBaseRange(ts("2026-01-01T00:59:00.000000Z"), ts("2026-01-01T01:00:00.000000Z"), Double.NaN);
                drainWalQueue();
                driveRefreshToQuiescence(job);
                hooks(job).setSimulateHeadMissHeadSealStart(null);
                Assert.assertEquals("the re-stamp's read of the view's table fails", 1, failedOpens.get());
                Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_ZERO_ROW_KEEP_COUNT));
                Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
                assertCheckpointLadder(instance, false, ladder);
                assertTimelineBase(6);
                // The older superblock slot holds the seal of the root at 00:56, at seqTxn 4.
                Assert.assertEquals(4, instance.getCheckpointTimelineWalPurgeFloor());
                for (int minute = 1; minute <= 3; minute++) {
                    setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                    commitAndRefresh(job, "('2026-01-01T01:0" + minute + ":00.000000Z', 'a', -1.0)");
                    Assert.assertEquals(
                            "one decline per cycle",
                            minute,
                            job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT)
                    );
                }
                Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
                assertCheckpointLadder(instance, false, ladder);
                assertTimelineBase(6);
                Assert.assertEquals(4, instance.getCheckpointTimelineWalPurgeFloor());
                Assert.assertEquals(0, instance.getCheckpointSealFailures());
                // The commits above the floor are 5 to 9. The pure delete at 6 writes no row, and
                // a WAL segment rolls over only on the next row, so it shares 5's segment.
                Assert.assertEquals(
                        "the purge must keep the segments above the floor the repair left",
                        9 - 4 - 1,
                        purgeBaseWalAndCountSegments()
                );
            }
            final String rows = headMissDepartureRows(true, 56);
            assertDepartedViewRows(rows);
            assertNoRefreshFaults("lv");
            assertRejectedRowsBetweenTwoRestartsKeepTheTimeline(rows, 4, 10);
        });
    }

    @Test
    public void testAnEndOfTableZeroRowKeepInsideThePauseRestampsAndReleasesBaseWalAfterIt() throws Exception {
        // Three rejected rows past the cadence each fail the seal of the unsealed 00:59 at the
        // frontier, which pauses the seal over the intact timeline; the fault then clears. The
        // zero-row keep of the departure of 00:59 inside the pause re-stamps the head on the root at
        // the new frontier 00:56: the re-stamp writes nothing, so the pause lets it through. The
        // first rejected row after the pause republishes that root at its commit although the
        // cadence is not due, and the ones after it on the cadence, which moves the base WAL floor.
        configureWalPurge();
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView();
            final String ladder = """
                    2025-12-31T23:55:00.000000Z -> 1
                    2026-01-01T00:56:00.000000Z -> 58
                    """;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitRootAtTheFrontierThenRowsAboveIt(job, false);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                final long resetsBefore = instance.getCheckpointTimelineResets();
                pauseTheSealWithRejectedRows(job, instance);

                // seqTxn 9, inside the pause.
                replaceBaseRange(ts("2026-01-01T00:59:00.000000Z"), ts("2026-01-01T01:00:00.000000Z"), Double.NaN);
                drainWalQueue();
                driveRefreshToQuiescence(job);
                Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_ZERO_ROW_KEEP_COUNT));
                Assert.assertEquals("the re-stamp passes the pause", ts("2026-01-01T00:56:00.000000Z"), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals("at the repair's commit", 9, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals("and records no success", 3, instance.getCheckpointSealFailures());
                Assert.assertTrue("so the pause stands", instance.isSealOnCooldown(currentMicros));
                Assert.assertEquals(resetsBefore, instance.getCheckpointTimelineResets());
                assertCheckpointLadder(instance, false, ladder);
                assertNoRepairMarker(instance);

                // seqTxn 10, inside the cadence the re-stamp restarted, past the pause.
                setCurrentMicros(currentMicros + 61_000_000L);
                commitAndRefresh(job, "('2026-01-01T01:04:00.000000Z', 'a', -1.0)");
                Assert.assertEquals("the first cycle after the pause publishes", 10, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertFalse(instance.isSealOnCooldown(currentMicros));
                // seqTxn 11 and 12, each past the cadence.
                commitRejectedRowsPastTheCadence(job, instance, 5, 6, 11);
                assertCheckpointLadder(instance, false, ladder);
                Assert.assertEquals(ts("2026-01-01T00:56:00.000000Z"), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(11, instance.getCheckpointTimelineWalPurgeFloor());
                Assert.assertEquals("the purge must keep only the segments above the floor", 12 - 11, purgeBaseWalAndCountSegments());
                Assert.assertEquals(3, instance.getCheckpointSealFailures());
                Assert.assertEquals(resetsBefore, instance.getCheckpointTimelineResets());
                Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT));
            }
            final String rows = headMissDepartureRows(true, 56);
            assertDepartedViewRows(rows);
            assertNoRefreshFaults("lv");
            assertRejectedRowsBetweenTwoRestartsKeepTheTimeline(rows, 7, 13);
        });
    }

    @Test
    public void testAnEndOfTableRepairThatTruncatesInsideThePauseRetiresItsTruncatedTimeline() throws Exception {
        // Three failed publications over the root at 00:59 pause the seal, and the fault clears. The
        // departure of the newest three minutes inside the pause keeps one row, at 00:57: the
        // head-miss rebuild truncates the spliced timeline at that new frontier, where no root sits,
        // and the pause holds the seal that would have put one there, so the rebuild retires the
        // truncated timeline. The first row after the pause opens a fresh history, which each
        // restart restores over the day the base lost.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitTwoBatchesOverTheLostDay(job, 59);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                assertCheckpointLadder(instance, false, """
                        2025-12-31T23:55:00.000000Z -> 1
                        2026-01-01T00:59:00.000000Z -> 61
                        """);
                final long resetsBefore = instance.getCheckpointTimelineResets();
                pauseTheSealWithRejectedRows(job, instance);

                final long headMissRepairsBefore = countHeadMissRepairs(job);
                replaceBaseRange(ts("2026-01-01T00:57:00.000000Z"), ts("2026-01-01T01:00:00.000000Z"), 5.0);
                drainWalQueue();
                driveRefreshToQuiescence(job);
                Assert.assertEquals("the head-miss rebuild repairs the departure", headMissRepairsBefore + 1, countHeadMissRepairs(job));
                Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_FRONTIER_TRUNCATE_COUNT));
                Assert.assertEquals("the pause holds the seal", 3, instance.getCheckpointSealFailures());
                Assert.assertEquals("the rebuild retires the truncated timeline", resetsBefore + 1, instance.getCheckpointTimelineResets());
                Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(Numbers.LONG_NULL, instance.getCheckpointTimelineWalPurgeFloor());
                try (Path dir = checkpointsDir(instance); Path timelinePath = new Path()) {
                    LiveViewCheckpointLayout.timelinePath(timelinePath, dir);
                    Assert.assertFalse("the timeline is gone", configuration.getFilesFacade().exists(timelinePath.$()));
                    Assert.assertFalse("and the marker with it", LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir));
                }

                setCurrentMicros(currentMicros + 61_000_000L);
                commitAndRefresh(job, "('2026-01-01T01:05:00.000000Z', 'a', 1.0)");
                Assert.assertEquals(ts("2026-01-01T01:05:00.000000Z"), instance.getHeadCheckpointMaxTs());
                assertCheckpointLadder(instance, false, "2026-01-01T01:05:00.000000Z -> 60\n");
                assertNoRefreshFaults("lv");
            }
            final String rows = headMissDepartureRows(true, 56)
                    + "2026-01-01T00:57:00.000000Z\ta\t5.0\t6.0\n"
                    + "2026-01-01T01:05:00.000000Z\ta\t1.0\t1.0\n";
            assertDepartedViewRows(rows);
            for (int restart = 0; restart < 2; restart++) {
                restartLiveViewsAndRecover();
                assertDepartedViewRestored(rows);
            }
        });
    }

    @Test
    public void testAnEndOfTableSpliceOwingARootInsideThePauseRetiresTheTimeline() throws Exception {
        // Three rejected rows past the cadence each fail the seal of the unsealed rows above the
        // root at 00:56, which pauses the seal over the intact timeline. A REPLACE_RANGE inside the
        // pause then rewrites 00:58 and deletes 00:59: the head-miss rebuild's splice owes a root
        // at the new frontier 00:58, and the pause holds its closing seal. No cycle without rows
        // can land that root before a newer row, and the failures have spent their budget, so the
        // repair retires the timeline, marker included, as the failure verdict would.
        configureWalPurge();
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(() -> {
                createHeadMissDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    commitTwoBatchesOverTheLostDay(job, 56);
                    commitAndRefresh(job, minuteRowsOfA(57, 59, "1.0"));
                    final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                    Assert.assertNotNull(instance);
                    assertCheckpointLadder(instance, false, """
                            2025-12-31T23:55:00.000000Z -> 1
                            2026-01-01T00:56:00.000000Z -> 58
                            """);
                    final long resetsBefore = instance.getCheckpointTimelineResets();
                    pauseTheSealWithRejectedRows(job, instance);

                    final long headMissRepairsBefore = countHeadMissRepairs(job);
                    replaceBaseRange(ts("2026-01-01T00:58:00.000000Z"), ts("2026-01-01T01:00:00.000000Z"), 5.0);
                    drainWalQueue();
                    driveRefreshToQuiescence(job);
                    Assert.assertEquals("the head-miss rebuild repairs the range", headMissRepairsBefore + 1, countHeadMissRepairs(job));
                    Assert.assertEquals("the pause holds the closing seal", 3, instance.getCheckpointSealFailures());
                    Assert.assertEquals("the stranded owed root retires the timeline", resetsBefore + 1, instance.getCheckpointTimelineResets());
                    Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
                    Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointBaseSeqTxn());
                    Assert.assertEquals(Numbers.LONG_NULL, instance.getCheckpointTimelineWalPurgeFloor());
                    try (Path dir = checkpointsDir(instance); Path timelinePath = new Path()) {
                        LiveViewCheckpointLayout.timelinePath(timelinePath, dir);
                        Assert.assertFalse("the timeline is gone", configuration.getFilesFacade().exists(timelinePath.$()));
                        Assert.assertFalse("and the marker with it", LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir));
                    }
                }
                capture.drain();
                capture.assertLogged("live view O3 repair owes a root no seal can land before a newer row while its checkpoint "
                        + "seal backs off, retiring the timeline [view=lv, frontier=2026-01-01T00:58:00.000000Z, "
                        + "latestSeenTs=2026-01-01T00:59:00.000000Z]");
                Assert.assertEquals(0, capture.countOccurrences("live view O3 repair kept its repair marker"));
                assertDepartedViewRows(headMissDepartureRows(true, 57) + "2026-01-01T00:58:00.000000Z\ta\t5.0\t6.0\n");
                assertNoRefreshFaults("lv");
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testAnEndOfTableRepairThatRewritesRowsBelowARootAndDeletesTheOnesAboveRestampsThatRoot() throws Exception {
        // One REPLACE_RANGE rewrites the rows from 00:50 to 00:56, the first of them with a new
        // value, and deletes the three above them. The replay crosses the root at 00:56 and
        // re-versions it there, which is where its output ends: the truncate at the new frontier
        // drops the root at 00:59 and keeps the one at 00:56, and the seal only re-stamps it.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitTheDayTheBaseLoses(job);
                commitAndRefresh(job, minuteRowsOfA(0, 30, "1.0"));
                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                commitAndRefresh(job, minuteRowsOfA(31, 56, "1.0"));
                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                commitAndRefresh(job, minuteRowsOfA(57, 59, "1.0"));
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                assertCheckpointLadder(instance, false, """
                        2025-12-31T23:55:00.000000Z -> 1
                        2026-01-01T00:56:00.000000Z -> 58
                        2026-01-01T00:59:00.000000Z -> 61
                        """);
                final long generationBefore = publishedCheckpointGeneration(instance);
                final long headMissRepairsBefore = countHeadMissRepairs(job);
                try (WalWriter walWriter = engine.getWalWriter(engine.verifyTableName("base"))) {
                    for (int minute = 50; minute <= 56; minute++) {
                        final TableWriter.Row row = walWriter.newRow(ts("2026-01-01T00:" + minute + ":00.000000Z"));
                        row.putSym(1, "a");
                        row.putDouble(2, minute == 50 ? 3.0 : 1.0);
                        row.append();
                    }
                    walWriter.commitWithParams(
                            ts("2026-01-01T00:50:00.000000Z"),
                            ts("2026-01-01T01:00:00.000000Z"),
                            WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE
                    );
                }
                drainWalQueue();
                driveRefreshToQuiescence(job);
                Assert.assertEquals("the head-miss rebuild repairs the departure", headMissRepairsBefore + 1, countHeadMissRepairs(job));
                Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_FRONTIER_TRUNCATE_COUNT));
                Assert.assertEquals("the splice and the truncate, the seal re-stamps", generationBefore + 2, publishedCheckpointGeneration(instance));
                assertCheckpointLadder(instance, false, """
                        2025-12-31T23:55:00.000000Z -> 1
                        2026-01-01T00:56:00.000000Z -> 58
                        """);
                Assert.assertEquals(ts("2026-01-01T00:56:00.000000Z"), instance.getHeadCheckpointMaxTs());
                assertNoRepairMarker(instance);
            }
            final String rows = headMissDepartureRows(true, 56)
                    .replace("2026-01-01T00:50:00.000000Z\ta\t1.0\t2.0", "2026-01-01T00:50:00.000000Z\ta\t3.0\t4.0")
                    .replace("2026-01-01T00:51:00.000000Z\ta\t1.0\t2.0", "2026-01-01T00:51:00.000000Z\ta\t1.0\t4.0");
            assertDepartedViewRows(rows);
            assertNoRefreshFaults("lv");
            for (int restart = 0; restart < 2; restart++) {
                restartLiveViewsAndRecover();
                assertDepartedViewRestored(rows);
            }
        });
    }

    @Test
    public void testAnEndOfTableRepairWithoutASpliceThatDeletesTheNewestRangeSealsItsFrontier() throws Exception {
        // No chained boundary is allowed, so the rebuild declines its splice and truncates the
        // timeline at R before it commits, as every end-of-table repair did before the splice
        // existed: the root at 00:59 goes and the one at 23:55 stays. It emits nothing, so it seals
        // the new frontier 00:56 on the state it warmed from the frame floor 00:55 on top of what
        // the truncate kept, where it once retired the truncated timeline.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, 0);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitAndRefresh(job, "('2025-12-31T23:55:00.000000Z', 'a', 0.5)");
                commitAndRefresh(job, minuteRowsOfA(0, 30, "1.0"));
                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                commitAndRefresh(job, minuteRowsOfA(31, 59, "1.0"));
                assertZeroRowDepartureSealsTheFrontier(
                        job,
                        () -> replaceBaseRange(ts("2026-01-01T00:57:00.000000Z"), ts("2026-01-01T01:00:00.000000Z"), Double.NaN),
                        "2026-01-01T00:56:00.000000Z",
                        FRONTIER_STATE_WARM_UP,
                        0,
                        3,
                        2,
                        false,
                        """
                                2025-12-31T23:55:00.000000Z -> 1
                                2026-01-01T00:56:00.000000Z -> 58
                                """
                );
                assertNoHeadMissSpliceFrontierStat(job);
            }
            assertRecomputedRowsThenTheTieAtTheFrontierRestore(headMissDepartureRows(true, 56));
        });
    }

    @Test
    public void testAnEndOfTableRepairWithoutASpliceOverALostDayRestoresRetainedRows() throws Exception {
        // The truncating twin of the item's departure over the day the base lost: every restart
        // restores from the root the seal put at the new frontier, keeping the 2025-12-31 row.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, 0);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitTwoBatchesOverTheLostDay(job, 59);
                assertZeroRowDepartureSealsTheFrontier(
                        job,
                        () -> replaceBaseRange(ts("2026-01-01T00:57:00.000000Z"), ts("2026-01-01T01:00:00.000000Z"), Double.NaN),
                        "2026-01-01T00:56:00.000000Z",
                        FRONTIER_STATE_WARM_UP,
                        0,
                        3,
                        2,
                        false,
                        """
                                2025-12-31T23:55:00.000000Z -> 1
                                2026-01-01T00:56:00.000000Z -> 58
                                """
                );
            }
            final String rows = headMissDepartureRows(true, 56);
            assertDepartedViewRows(rows);
            assertTieAtTheFrontierFoldsOntoTheRestoredRoot(
                    rows,
                    "('2026-01-01T00:56:00.000000Z', 'a', 1.0)",
                    rows + "2026-01-01T00:56:00.000000Z\ta\t1.0\t3.0\n"
            );
        });
    }

    @Test
    public void testAnEndOfTableRepairWithoutASpliceOverARootAtTheFrontierPublishesItsHead() throws Exception {
        // No chained boundary is allowed, and a root sits at the row that becomes the frontier,
        // 00:56, counting every row the view holds there: the rows the filter drops and the row of
        // the view above it are unsealed. The rebuild that deletes that row emits nothing and
        // derives nothing - the root already is the state at the frontier - and its truncate at R
        // keeps that root, so a watermark publishes it at the repair commit, with no new root, where
        // the truncated timeline once retired. Every restart restores from it over the day the base
        // lost.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, 0);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitRootAtTheFrontierThenRowsAboveIt(job, false);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                final String ladder = """
                        2025-12-31T23:55:00.000000Z -> 1
                        2026-01-01T00:56:00.000000Z -> 58
                        """;
                assertCheckpointLadder(instance, false, ladder);
                final LiveViewRefreshStats stats = job.getStatsForTest();
                final long generationBefore = publishedCheckpointGeneration(instance);
                final long headMissRepairsBefore = countHeadMissRepairs(job);
                final long resetsBefore = instance.getCheckpointTimelineResets();
                final long scanRowsBefore = instance.getO3ReplayScanRows();
                replaceBaseRange(ts("2026-01-01T00:59:00.000000Z"), ts("2026-01-01T01:00:00.000000Z"), Double.NaN);
                drainWalQueue();
                driveRefreshToQuiescence(job);
                Assert.assertEquals("the head-miss rebuild repairs the departure", headMissRepairsBefore + 1, countHeadMissRepairs(job));
                Assert.assertEquals(1, stats.get(LiveViewRefreshStats.HEAD_MISS_TRUNCATE_ZERO_ROW_KEEP_COUNT));
                Assert.assertEquals(0, stats.get(LiveViewRefreshStats.HEAD_MISS_ZERO_ROW_FRONTIER_SEAL_COUNT));
                Assert.assertEquals(0, stats.get(LiveViewRefreshStats.HEAD_MISS_FRONTIER_STATE_RESTORE_COUNT));
                Assert.assertEquals(0, stats.get(LiveViewRefreshStats.HEAD_MISS_FRONTIER_STATE_WARM_UP_COUNT));
                assertNoHeadMissSpliceFrontierStat(job);
                Assert.assertEquals("the replay reads nothing above 00:58, and nothing is derived", 0, instance.getO3ReplayScanRows() - scanRowsBefore);
                Assert.assertEquals("the truncate and the watermark", generationBefore + 2, publishedCheckpointGeneration(instance));
                Assert.assertEquals(resetsBefore, instance.getCheckpointTimelineResets());
                assertCheckpointLadder(instance, false, ladder);
                assertNoRepairMarker(instance);
                // The head goes back on the root at the frontier, at the repair's commit, naming no
                // root a resume could reuse the rebuild's runtime under.
                Assert.assertEquals(ts("2026-01-01T00:56:00.000000Z"), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(
                        engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("base")),
                        instance.getHeadCheckpointBaseSeqTxn()
                );
                Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointRootId());
            }
            final String rows = headMissDepartureRows(true, 56);
            assertDepartedViewRows(rows);
            assertNoRefreshFaults("lv");
            assertTieAtTheFrontierFoldsOntoTheRestoredRoot(
                    rows,
                    "('2026-01-01T00:56:00.000000Z', 'a', 1.0)",
                    rows + "2026-01-01T00:56:00.000000Z\ta\t1.0\t3.0\n"
            );
        });
    }

    @Test
    public void testAnEndOfTableRepairThatDeletesTheNewestRangeSealsTheFrontierStateRowsFoldOnto() throws Exception {
        // A second row of the frontier's frame, 00:55:30, sits between the frame floor 00:55 and
        // the frontier 00:56: the warm-up folds all three, so a row at 00:56 after the restart
        // reads four and one at 00:56:20 reads 00:55:30 and above. A seal of the replay's own
        // state, warmed from 00:56, would give that row 3.0.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitTheDayTheBaseLoses(job);
                commitAndRefresh(job, minuteRowsOfA(0, 30, "1.0"));
                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                commitAndRefresh(job, minuteRowsOfA(31, 55, "1.0") + ", ('2026-01-01T00:55:30.000000Z', 'a', 1.0), " + minuteRowsOfA(56, 59, "1.0"));
                assertZeroRowDepartureSealsTheFrontier(
                        job,
                        () -> replaceBaseRange(ts("2026-01-01T00:57:00.000000Z"), ts("2026-01-01T01:00:00.000000Z"), Double.NaN),
                        "2026-01-01T00:56:00.000000Z",
                        FRONTIER_STATE_WARM_UP,
                        1,
                        4,
                        3,
                        false,
                        """
                                2025-12-31T23:55:00.000000Z -> 1
                                2026-01-01T00:56:00.000000Z -> 59
                                """
                );
            }
            final String rows = headMissDepartureRows(true, 55) + """
                    2026-01-01T00:55:30.000000Z\ta\t1.0\t2.0
                    2026-01-01T00:56:00.000000Z\ta\t1.0\t3.0
                    """;
            assertDepartedViewRows(rows);
            assertTieAtTheFrontierFoldsOntoTheRestoredRoot(
                    rows,
                    "('2026-01-01T00:56:00.000000Z', 'a', 1.0), ('2026-01-01T00:56:20.000000Z', 'a', 1.0)",
                    rows + """
                            2026-01-01T00:56:00.000000Z\ta\t1.0\t4.0
                            2026-01-01T00:56:20.000000Z\ta\t1.0\t4.0
                            """
            );
        });
    }

    @Test
    public void testAnEndOfTableRepairThatDeletesTheNewestRangeCountsTheFrameFloorOnly() throws Exception {
        // Rows one tick either side of the frame floor 00:55 of the new frontier 00:56: the one
        // above it is the frame's, the one below it, at 7.0, is not. The warm-up starts at exactly
        // the floor, so a row at 00:56 after the restart reads 00:55, 00:55:00.000001, 00:56 and
        // itself: 4.0, neither 11.0 (a fold from below the floor that kept the row there) nor 3.0
        // (a fold from one tick above it).
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitTheDayTheBaseLoses(job);
                commitAndRefresh(job, minuteRowsOfA(0, 30, "1.0"));
                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                commitAndRefresh(
                        job,
                        minuteRowsOfA(31, 54, "1.0")
                                + ", ('2026-01-01T00:54:59.999999Z', 'a', 7.0), ('2026-01-01T00:55:00.000000Z', 'a', 1.0)"
                                + ", ('2026-01-01T00:55:00.000001Z', 'a', 1.0), " + minuteRowsOfA(56, 59, "1.0")
                );
                assertZeroRowDepartureSealsTheFrontier(
                        job,
                        () -> replaceBaseRange(ts("2026-01-01T00:57:00.000000Z"), ts("2026-01-01T01:00:00.000000Z"), Double.NaN),
                        "2026-01-01T00:56:00.000000Z",
                        FRONTIER_STATE_WARM_UP,
                        1,
                        4,
                        3,
                        false,
                        """
                                2025-12-31T23:55:00.000000Z -> 1
                                2026-01-01T00:56:00.000000Z -> 60
                                """
                );
            }
            final String rows = headMissDepartureRows(true, 54) + """
                    2026-01-01T00:54:59.999999Z\ta\t7.0\t8.0
                    2026-01-01T00:55:00.000000Z\ta\t1.0\t9.0
                    2026-01-01T00:55:00.000001Z\ta\t1.0\t9.0
                    2026-01-01T00:56:00.000000Z\ta\t1.0\t3.0
                    """;
            assertDepartedViewRows(rows);
            assertTieAtTheFrontierFoldsOntoTheRestoredRoot(
                    rows,
                    "('2026-01-01T00:56:00.000000Z', 'a', 1.0)",
                    rows + "2026-01-01T00:56:00.000000Z\ta\t1.0\t4.0\n"
            );
        });
    }

    @Test
    public void testAnEndOfTableRepairOneTickBelowItsOutputFloorFoldsTheFrameBelowIt() throws Exception {
        // The new frontier 00:56:59.999999 sits one tick below the output floor R = 00:57, so the
        // replay warmed from R - 1 minute = 00:56 and never read the row at exactly the
        // frontier's frame floor 00:55:59.999999. The rebuild warms from that floor instead of
        // sealing the replay's state, so a row at the frontier after the restart reads four rows.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitTheDayTheBaseLoses(job);
                commitAndRefresh(job, minuteRowsOfA(0, 30, "1.0"));
                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                commitAndRefresh(
                        job,
                        minuteRowsOfA(31, 55, "1.0") + ", ('2026-01-01T00:55:59.999999Z', 'a', 1.0), ('2026-01-01T00:56:00.000000Z', 'a', 1.0)"
                                + ", ('2026-01-01T00:56:59.999999Z', 'a', 1.0), " + minuteRowsOfA(57, 59, "1.0")
                );
                assertZeroRowDepartureSealsTheFrontier(
                        job,
                        () -> replaceBaseRange(ts("2026-01-01T00:57:00.000000Z"), ts("2026-01-01T01:00:00.000000Z"), Double.NaN),
                        "2026-01-01T00:56:59.999999Z",
                        FRONTIER_STATE_WARM_UP,
                        1,
                        // The replay reads 00:56 and 00:56:59.999999, the warm-up those and the floor's row.
                        5,
                        3,
                        false,
                        """
                                2025-12-31T23:55:00.000000Z -> 1
                                2026-01-01T00:56:59.999999Z -> 60
                                """
                );
            }
            final String rows = headMissDepartureRows(true, 55) + """
                    2026-01-01T00:55:59.999999Z\ta\t1.0\t2.0
                    2026-01-01T00:56:00.000000Z\ta\t1.0\t3.0
                    2026-01-01T00:56:59.999999Z\ta\t1.0\t3.0
                    """;
            assertDepartedViewRows(rows);
            assertTieAtTheFrontierFoldsOntoTheRestoredRoot(
                    rows,
                    "('2026-01-01T00:56:59.999999Z', 'a', 1.0)",
                    rows + "2026-01-01T00:56:59.999999Z\ta\t1.0\t4.0\n"
            );
        });
    }

    @Test
    public void testAnEndOfTableRepairWithinAFrameOfTheViewStartSealsWithoutAFold() throws Exception {
        // The view starts at 2026-01-01T00:00, under a minute below every row, so the replay's
        // floor and the new frontier's both clamp to that start: the replay already folded every
        // row the frontier's state holds, and the rebuild seals it without reading a row more. The
        // frontier truncate drops the only root, at 00:00:50, and the seal opens a fresh history.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM '2026-01-01T00:00:00.000000Z' AS " + HEAD_MISS_DEPARTURE_VIEW_SQL);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitAndRefresh(
                        job,
                        "('2026-01-01T00:00:10.000000Z', 'a', 1.0), ('2026-01-01T00:00:20.000000Z', 'a', 1.0), "
                                + "('2026-01-01T00:00:40.000000Z', 'a', 1.0), ('2026-01-01T00:00:50.000000Z', 'a', 1.0)"
                );
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                assertCheckpointLadder(instance, false, "2026-01-01T00:00:50.000000Z -> 4\n");
                assertZeroRowDepartureSealsTheFrontier(
                        job,
                        () -> replaceBaseRange(ts("2026-01-01T00:00:40.000000Z"), ts("2026-01-01T00:01:00.000000Z"), Double.NaN),
                        "2026-01-01T00:00:20.000000Z",
                        FRONTIER_STATE_SKIP,
                        1,
                        2,
                        0,
                        true,
                        "2026-01-01T00:00:20.000000Z -> 2\n"
                );
            }
            final String rows = """
                    ts\tsym\tx\ts
                    2026-01-01T00:00:10.000000Z\ta\t1.0\t1.0
                    2026-01-01T00:00:20.000000Z\ta\t1.0\t2.0
                    """;
            assertDepartedViewRows(rows);
            assertTieAtTheFrontierFoldsOntoTheRestoredRoot(
                    rows,
                    "('2026-01-01T00:00:20.000000Z', 'a', 1.0)",
                    rows + "2026-01-01T00:00:20.000000Z\ta\t1.0\t3.0\n"
            );
        });
    }

    @Test
    public void testAnEndOfTableRepairThatDeletesTheNewestRangeRestoresARootWithinTheFrame() throws Exception {
        // The new frontier 01-02T00:00:50 has a root 40 seconds below it, inside its frame, which
        // counts every row of the view there: the rebuild restores it and folds the four base rows
        // above it, three of which the filter drops, rather than read the whole frame from its
        // floor 01-01T23:59:50.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitTheFrameAcrossMidnight(job, FRAME_ROOT_ABOVE_THE_FLOOR);
                assertZeroRowDepartureSealsTheFrontier(
                        job,
                        () -> departTheRowsAboveTheFrameAcrossMidnight(),
                        "2026-01-02T00:00:50.000000Z",
                        FRONTIER_STATE_RESTORE,
                        0,
                        4,
                        2,
                        false,
                        """
                                2025-12-31T23:55:00.000000Z -> 1
                                2026-01-02T00:00:10.000000Z -> 34
                                2026-01-02T00:00:50.000000Z -> 35
                                """
                );
            }
            assertTheFrameAcrossMidnightRestores();
        });
    }

    @Test
    public void testAnEndOfTableRepairRestoresARootAtTheFrameFloor() throws Exception {
        // The only root inside the new frontier's frame sits at exactly its floor
        // 01-01T23:59:50: it serves every row at or above it, so the rebuild restores it and folds
        // the five base rows above it.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitTheFrameAcrossMidnight(job, FRAME_ROOT_AT_THE_FLOOR);
                assertZeroRowDepartureSealsTheFrontier(
                        job,
                        () -> departTheRowsAboveTheFrameAcrossMidnight(),
                        "2026-01-02T00:00:50.000000Z",
                        FRONTIER_STATE_RESTORE,
                        0,
                        5,
                        2,
                        false,
                        """
                                2025-12-31T23:55:00.000000Z -> 1
                                2026-01-01T23:59:50.000000Z -> 33
                                2026-01-02T00:00:50.000000Z -> 35
                                """
                );
            }
            assertTheFrameAcrossMidnightRestores();
        });
    }

    @Test
    public void testAnEndOfTableRepairThatDeletesTheNewestRangeWarmsAcrossAPartitionBoundary() throws Exception {
        // No root sits inside the new frontier's frame, so the rebuild folds it whole from its
        // floor 01-01T23:59:50, across the day partitions, six base rows in all.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitTheFrameAcrossMidnight(job, FRAME_ROOT_NONE);
                assertZeroRowDepartureSealsTheFrontier(
                        job,
                        () -> departTheRowsAboveTheFrameAcrossMidnight(),
                        "2026-01-02T00:00:50.000000Z",
                        FRONTIER_STATE_WARM_UP,
                        0,
                        6,
                        2,
                        false,
                        """
                                2025-12-31T23:55:00.000000Z -> 1
                                2026-01-02T00:00:50.000000Z -> 35
                                """
                );
            }
            assertTheFrameAcrossMidnightRestores();
        });
    }

    @Test
    public void testAnEndOfTableRepairWhoseRootLookupFailsWarmsTheFrame() throws Exception {
        // The frame-restoring departure above, with the read of the timeline that looks the root up
        // failing once: the rebuild takes that for no root and folds the whole frame from its floor
        // instead, then seals the frontier on what it folded.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final TimelineOpenFault fault = new TimelineOpenFault();
        assertMemoryLeak(fault, () -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitTheFrameAcrossMidnight(job, FRAME_ROOT_ABOVE_THE_FLOOR);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                // The lookup's read of the timeline is the first one after the hook.
                hooks(job).setSimulateHeadMissFrontierStateFoldStart(() -> fault.arm(instance, 1));
                final LogCapture capture = new LogCapture();
                capture.start();
                try {
                    assertZeroRowDepartureSealsTheFrontier(
                            job,
                            () -> departTheRowsAboveTheFrameAcrossMidnight(),
                            "2026-01-02T00:00:50.000000Z",
                            FRONTIER_STATE_WARM_UP,
                            0,
                            6,
                            2,
                            false,
                            """
                                    2025-12-31T23:55:00.000000Z -> 1
                                    2026-01-02T00:00:10.000000Z -> 34
                                    2026-01-02T00:00:50.000000Z -> 35
                                    """
                    );
                    capture.drain();
                    capture.assertLogged("could not read the live view checkpoint root below the frontier, deriving its state from the base [view=lv, "
                            + "frontier=2026-01-02T00:00:50.000000Z");
                    capture.assertLogged("live view O3 head miss derived the frontier state [view=lv, frontier=2026-01-02T00:00:50.000000Z, "
                            + "floor=2026-01-01T23:59:50.000000Z, openBelow=false, route=warm-up, rootMaxTs=, rows=6]");
                } finally {
                    capture.stop();
                }
                Assert.assertTrue("the lookup's read of the timeline fails", fault.hasFired());
            }
            assertTheFrameAcrossMidnightRestores();
        });
    }

    @Test
    public void testAnEndOfTableRepairWhoseRootRestoreFailsWarmsTheFrame() throws Exception {
        // The frame-restoring departure, with the restore of the root it found failing on a file
        // that is gone - a failure no retry gets past: the rebuild folds the whole frame from its
        // floor over an identity runtime instead, and seals the frontier on that.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final TimelineOpenFault fault = new TimelineOpenFault();
        assertMemoryLeak(fault, () -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitTheFrameAcrossMidnight(job, FRAME_ROOT_ABOVE_THE_FLOOR);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                // The lookup reads the timeline first, then the restore reads it again.
                hooks(job).setSimulateHeadMissFrontierStateFoldStart(() -> fault.arm(instance, 2));
                final LogCapture capture = new LogCapture();
                capture.start();
                try {
                    assertZeroRowDepartureSealsTheFrontier(
                            job,
                            () -> departTheRowsAboveTheFrameAcrossMidnight(),
                            "2026-01-02T00:00:50.000000Z",
                            FRONTIER_STATE_WARM_UP,
                            0,
                            6,
                            2,
                            false,
                            """
                                    2025-12-31T23:55:00.000000Z -> 1
                                    2026-01-02T00:00:10.000000Z -> 34
                                    2026-01-02T00:00:50.000000Z -> 35
                                    """
                    );
                    capture.drain();
                    capture.assertLogged("could not restore live view O3 resume anchor [view=lv, anchorMaxTs=2026-01-02T00:00:10.000000Z");
                    capture.assertLogged("live view O3 head miss derived the frontier state [view=lv, frontier=2026-01-02T00:00:50.000000Z, "
                            + "floor=2026-01-01T23:59:50.000000Z, openBelow=false, route=warm-up, rootMaxTs=2026-01-02T00:00:10.000000Z, rows=6]");
                } finally {
                    capture.stop();
                }
                Assert.assertTrue("the restore's read of the timeline fails", fault.hasFired());
            }
            assertTheFrameAcrossMidnightRestores();
        });
    }

    @Test
    public void testAnEndOfTableRepairWhoseFrontierStateFoldFaultsRepeatsAndSealsTheFrontier() throws Exception {
        // The item's departure over the day the base lost, with its derivation of the frontier's
        // state faulting once, ahead of the commit: nothing durable has moved, so the failure
        // handler restores the runtime in place from the untouched timeline, the repair runs again
        // and seals the frontier, and every restart restores over the lost day.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitTwoBatchesOverTheLostDay(job, 59);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                final long generationBefore = publishedCheckpointGeneration(instance);
                hooks(job).setSimulateHeadMissFrontierStateFoldStart(() -> {
                    // Ahead of the commit: the timeline and the view's table are as the repair found them.
                    Assert.assertEquals(generationBefore, publishedCheckpointGeneration(instance));
                    throw CairoException.critical(0).put("simulated live view frontier state fold fault");
                });
                replaceBaseRange(ts("2026-01-01T00:57:00.000000Z"), ts("2026-01-01T01:00:00.000000Z"), Double.NaN);
                drainWalQueue();
                driveRefreshToQuiescence(job);
                Assert.assertEquals("the fold faults once", 1, instance.getRefreshFaultCount());
                Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_ZERO_ROW_FRONTIER_SEAL_COUNT));
                Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_FRONTIER_STATE_WARM_UP_COUNT));
                Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_ZERO_ROW_RETIRE_COUNT));
                Assert.assertEquals("the timeline stays", 0, instance.getCheckpointTimelineResets());
                assertCheckpointLadder(instance, false, """
                        2025-12-31T23:55:00.000000Z -> 1
                        2026-01-01T00:56:00.000000Z -> 58
                        """);
                Assert.assertEquals(ts("2026-01-01T00:56:00.000000Z"), instance.getHeadCheckpointMaxTs());
                assertNoRepairMarker(instance);
            }
            final String rows = headMissDepartureRows(true, 56);
            assertDepartedViewRows(rows);
            assertTieAtTheFrontierFoldsOntoTheRestoredRoot(
                    rows,
                    "('2026-01-01T00:56:00.000000Z', 'a', 1.0)",
                    rows + "2026-01-01T00:56:00.000000Z\ta\t1.0\t3.0\n"
            );
        });
    }

    @Test
    public void testAnEndOfTableRepairInsideAnAnchorSegmentSealsWithoutAFold() throws Exception {
        // A daily anchored sum whose departed rows and new frontier share 01-02's segment: the
        // replay warmed from the segment's start, which is the frontier's own dependency floor, so
        // it already holds the frontier's state and the rebuild seals it without a read.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createDailyAnchoredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitTheDayTheBaseLoses(job);
                commitAndRefresh(job, minuteRowsOfA(0, 59, "1.0"));
                commitAndRefresh(job, minuteRowsOfAOnTheSecond(0, 10));
                assertZeroRowDepartureSealsTheFrontier(
                        job,
                        () -> replaceBaseRange(ts("2026-01-02T00:08:00.000000Z"), ts("2026-01-02T00:11:00.000000Z"), Double.NaN),
                        "2026-01-02T00:07:00.000000Z",
                        FRONTIER_STATE_SKIP,
                        0,
                        8,
                        2,
                        false,
                        """
                                2025-12-31T23:55:00.000000Z -> 1
                                2026-01-02T00:07:00.000000Z -> 69
                                """
                );
            }
            final String sql = "SELECT ts, sym, x, s FROM lv WHERE ts >= '2026-01-02T00:05:00.000000Z'";
            final String rows = """
                    ts\tsym\tx\ts
                    2026-01-02T00:05:00.000000Z\ta\t1.0\t6.0
                    2026-01-02T00:06:00.000000Z\ta\t1.0\t7.0
                    2026-01-02T00:07:00.000000Z\ta\t1.0\t8.0
                    """;
            assertTieAtTheFrontierFoldsOntoTheRestoredRoot(
                    sql,
                    rows,
                    "('2026-01-02T00:07:00.000000Z', 'a', 1.0)",
                    rows + "2026-01-02T00:07:00.000000Z\ta\t1.0\t9.0\n"
            );
        });
    }

    @Test
    public void testAnEndOfTableRepairThatEmptiesItsAnchorSegmentRestoresTheRootBelowIt() throws Exception {
        assertTheEmptiedAnchorSegmentRestoresTheRootBelowIt();
    }

    @Test
    public void testAnEndOfTableRepairThatEmptiesItsAnchorSegmentRestoresTheRootBelowItOverPagesOfFourRows() throws Exception {
        // The restore and the fold read the base four rows a page frame at a time.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 4);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 4);
        assertTheEmptiedAnchorSegmentRestoresTheRootBelowIt();
    }

    @Test
    public void testAnEndOfTableRepairThatEmptiesItsAnchorSegmentRestoresTheRootBelowItWithoutWindowMapFusion() throws Exception {
        // The anchored sum keeps its state in its own partition map rather than in the window's
        // fused slots, which the root restore clears and refills.
        setProperty(PropertyKey.CAIRO_SQL_WINDOW_MAP_FUSION_ENABLED, "false");
        assertTheEmptiedAnchorSegmentRestoresTheRootBelowIt();
    }

    @Test
    public void testAnEndOfTableRepairThatEmptiesItsAnchorSegmentWarmsThePreviousSegment() throws Exception {
        assertTheEmptiedAnchorSegmentWarmsThePreviousSegment();
    }

    @Test
    public void testAnEndOfTableRepairThatEmptiesItsAnchorSegmentWarmsThePreviousSegmentOverPagesOfFourRows() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 4);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 4);
        assertTheEmptiedAnchorSegmentWarmsThePreviousSegment();
    }

    @Test
    public void testAnEndOfTableRepairThatEmptiesItsAnchorSegmentWarmsThePreviousSegmentWithoutWindowMapFusion() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_WINDOW_MAP_FUSION_ENABLED, "false");
        assertTheEmptiedAnchorSegmentWarmsThePreviousSegment();
    }

    @Test
    public void testAnEndOfTableRepairWhoseFrontierEndsItsAnchorSegmentWarmsTheWholeSegment() throws Exception {
        // The new frontier sits on the last instant of 01-01's segment, one tick below the
        // replay's: its floor is 01-01's start, not 01-02's, so the rebuild folds the whole day.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createDailyAnchoredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitTheDayTheBaseLoses(job);
                commitAndRefresh(
                        job,
                        minuteRowsOfA(0, 59, "1.0") + ", ('2026-01-01T23:59:59.999999Z', 'a', 1.0), " + minuteRowsOfAOnTheSecond(0, 10)
                );
                assertZeroRowDepartureSealsTheFrontier(
                        job,
                        () -> replaceBaseRange(ts("2026-01-02T00:00:00.000000Z"), ts("2026-01-02T00:11:00.000000Z"), Double.NaN),
                        "2026-01-01T23:59:59.999999Z",
                        FRONTIER_STATE_WARM_UP,
                        0,
                        61,
                        2,
                        false,
                        """
                                2025-12-31T23:55:00.000000Z -> 1
                                2026-01-01T23:59:59.999999Z -> 62
                                """
                );
            }
            final String sql = "SELECT ts, sym, x, s FROM lv WHERE ts >= '2026-01-01T23:00:00.000000Z'";
            final String rows = "ts\tsym\tx\ts\n2026-01-01T23:59:59.999999Z\ta\t1.0\t61.0\n";
            // A row at the frontier continues the day; one on 01-02 opens that day's segment.
            assertTieAtTheFrontierFoldsOntoTheRestoredRoot(
                    sql,
                    rows,
                    "('2026-01-01T23:59:59.999999Z', 'a', 1.0), ('2026-01-02T00:05:00.000000Z', 'a', 1.0)",
                    rows + """
                            2026-01-01T23:59:59.999999Z\ta\t1.0\t62.0
                            2026-01-02T00:05:00.000000Z\ta\t1.0\t1.0
                            """
            );
        });
    }

    @Test
    public void testAnEndOfTableRepairInsideAFallBackRunOpenBelowSealsWithoutAFold() throws Exception {
        // ANCHOR DAILY '02:30' Europe/Berlin across the 2025-10-26 fall-back. The departed rows
        // start at 00:30Z, 02:30 CEST, in the half hour the clocks go back over: its anchor value
        // 10-26T00:30Z recurs nowhere below it, while the half hour after it, 02:00 CET, floors
        // back to 10-25T00:30Z, so its segment has no start of its own and the replay read from
        // the view's start. The new frontier 00:25Z sits on 10-25's segment, whose start is a real
        // timestamp above the replay's floor: the replay already folded every row the frontier's
        // state holds, and more, and the rebuild seals it without a read. The frontier truncate
        // drops the only root, and the seal opens a fresh history at the frontier. After the
        // restart, b's row in the half hour after the transition continues its 10-25 run.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createBerlinAnchoredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitAndRefresh(job, rowsOfTheFallBackEve("('2025-10-26T00:50:00.000000Z', 'a', 1.0)"));
                final LogCapture capture = new LogCapture();
                capture.start();
                try {
                    assertZeroRowDepartureSealsTheFrontier(
                            job,
                            () -> replaceBaseRange(ts("2025-10-26T00:30:00.000000Z"), ts("2025-10-26T01:00:00.000000Z"), Double.NaN),
                            "2025-10-26T00:25:00.000000Z",
                            FRONTIER_STATE_SKIP,
                            1,
                            55,
                            0,
                            true,
                            "2025-10-26T00:25:00.000000Z -> 55\n"
                    );
                    capture.drain();
                    capture.assertLogged("live view O3 head miss derived the frontier state [view=lv, frontier=2025-10-26T00:25:00.000000Z, "
                            + "floor=2025-10-25T00:30:00.000000Z, openBelow=false, route=skip, rootMaxTs=, rows=0]");
                } finally {
                    capture.stop();
                }
            }
            final String rows = """
                    ts\tsym\tx\ts
                    2025-10-26T00:20:00.000000Z\ta\t1.0\t53.0
                    2025-10-26T00:20:00.000000Z\tb\t1.0\t1.0
                    2025-10-26T00:25:00.000000Z\ta\t1.0\t54.0
                    """;
            assertTieAtTheFrontierFoldsOntoTheRestoredRoot(
                    FALL_BACK_TAIL_SQL,
                    rows,
                    "('2025-10-26T00:25:00.000000Z', 'a', 1.0), ('2025-10-26T01:10:00.000000Z', 'b', 1.0)",
                    rows + """
                            2025-10-26T00:25:00.000000Z\ta\t1.0\t55.0
                            2025-10-26T01:10:00.000000Z\tb\t1.0\t2.0
                            """
            );
        });
    }

    @Test
    public void testAFrontierInAFallBackRunOpenBelowWarmsFromTheViewStart() throws Exception {
        // The fall-back fixture with the new frontier 00:45Z inside the half hour 02:30 CEST that
        // has no segment start of its own, and the departed rows at 02:00Z, on 10-26's own
        // segment, which the replay warmed from its start 01:30Z. The frontier's floor is the
        // view's start: b's row at 01:10Z after the restart continues the run b's 00:20Z row is
        // on, below the half hour. No root sits at or below the frontier, so the rebuild folds
        // every base row up to it - the rows a restart's rebuild without the timeline would read.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createBerlinAnchoredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitAndRefresh(job, rowsOfTheFallBackEve(FALL_BACK_OPEN_RUN_ROWS));
                final LogCapture capture = new LogCapture();
                capture.start();
                try {
                    assertZeroRowDepartureSealsTheFrontier(
                            job,
                            () -> replaceBaseRange(ts("2025-10-26T02:00:00.000000Z"), ts("2025-10-26T02:15:00.000000Z"), Double.NaN),
                            "2025-10-26T00:45:00.000000Z",
                            FRONTIER_STATE_WARM_UP,
                            1,
                            57,
                            0,
                            true,
                            "2025-10-26T00:45:00.000000Z -> 57\n"
                    );
                    capture.drain();
                    capture.assertLogged("live view O3 head miss derived the frontier state [view=lv, frontier=2025-10-26T00:45:00.000000Z, "
                            + "floor=, openBelow=true, route=warm-up, rootMaxTs=, rows=57]");
                } finally {
                    capture.stop();
                }
            }
            assertTheFallBackOpenRunRestores("");
        });
    }

    @Test
    public void testAnEndOfTableRepairInsideAFallBackRunOpenBelowOverALostDaySealsWithoutAFold() throws Exception {
        // The open-run skip over a base that has lost 10-24, whose row the view keeps: the seal of
        // the fresh history at the frontier counts it, and every restart restores with it.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createBerlinAnchoredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitAndRefresh(job, "('2025-10-24T12:00:00.000000Z', 'a', 0.5), " + rowsOfTheFallBackEve("('2025-10-26T00:50:00.000000Z', 'a', 1.0)"));
                execute("ALTER TABLE base DROP PARTITION LIST '2025-10-24'");
                drainWalQueue();
                driveRefreshToQuiescence(job);
                assertZeroRowDepartureSealsTheFrontier(
                        job,
                        () -> replaceBaseRange(ts("2025-10-26T00:30:00.000000Z"), ts("2025-10-26T01:00:00.000000Z"), Double.NaN),
                        "2025-10-26T00:25:00.000000Z",
                        FRONTIER_STATE_SKIP,
                        1,
                        55,
                        0,
                        true,
                        "2025-10-26T00:25:00.000000Z -> 56\n"
                );
            }
            final String rows = """
                    ts\tsym\tx\ts
                    2025-10-24T12:00:00.000000Z\ta\t0.5\t0.5
                    2025-10-26T00:20:00.000000Z\ta\t1.0\t53.0
                    2025-10-26T00:20:00.000000Z\tb\t1.0\t1.0
                    2025-10-26T00:25:00.000000Z\ta\t1.0\t54.0
                    """;
            assertTieAtTheFrontierFoldsOntoTheRestoredRoot(
                    FALL_BACK_TAIL_SQL,
                    rows,
                    "('2025-10-26T00:25:00.000000Z', 'a', 1.0), ('2025-10-26T01:10:00.000000Z', 'b', 1.0)",
                    rows + """
                            2025-10-26T00:25:00.000000Z\ta\t1.0\t55.0
                            2025-10-26T01:10:00.000000Z\tb\t1.0\t2.0
                            """
            );
        });
    }

    @Test
    public void testAnEndOfTableRepairInsideAFallBackRunOpenBelowInNanosSealsWithoutAFold() throws Exception {
        // The open-run skip over a base with nanosecond timestamps: the zoned anchor plan, the
        // floors and the seal work in the base's unit, and the derivation's log line prints them
        // as the base's dates.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP_NS, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS SELECT ts, sym, x, sum(x) OVER w s FROM base "
                    + "WHERE x > 0 WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '02:30' 'Europe/Berlin')");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitAndRefresh(job, rowsOfTheFallBackEve("('2025-10-26T00:50:00.000000000Z', 'a', 1.0)").replace("::timestamp", "000::timestamp_ns"));
                final LogCapture capture = new LogCapture();
                capture.start();
                try {
                    assertZeroRowDepartureSealsTheFrontier(
                            job,
                            () -> replaceBaseRange(ts("2025-10-26T00:30:00.000000Z") * 1000, ts("2025-10-26T01:00:00.000000Z") * 1000, Double.NaN),
                            "2025-10-26T00:25:00.000000000Z",
                            FRONTIER_STATE_SKIP,
                            1,
                            55,
                            0,
                            true,
                            "2025-10-26T00:25:00.000000000Z -> 55\n"
                    );
                    capture.drain();
                    capture.assertLogged("live view O3 head miss derived the frontier state [view=lv, frontier=2025-10-26T00:25:00.000000000Z, "
                            + "floor=2025-10-25T00:30:00.000000000Z, openBelow=false, route=skip, rootMaxTs=, rows=0]");
                } finally {
                    capture.stop();
                }
            }
            final String rows = """
                    ts\tsym\tx\ts
                    2025-10-26T00:20:00.000000000Z\ta\t1.0\t53.0
                    2025-10-26T00:20:00.000000000Z\tb\t1.0\t1.0
                    2025-10-26T00:25:00.000000000Z\ta\t1.0\t54.0
                    """;
            assertTieAtTheFrontierFoldsOntoTheRestoredRoot(
                    FALL_BACK_TAIL_SQL,
                    rows,
                    "('2025-10-26T00:25:00.000000000Z', 'a', 1.0), ('2025-10-26T01:10:00.000000000Z', 'b', 1.0)",
                    rows + """
                            2025-10-26T00:25:00.000000000Z\ta\t1.0\t55.0
                            2025-10-26T01:10:00.000000000Z\tb\t1.0\t2.0
                            """
            );
        });
    }

    @Test
    public void testAFrontierInAFallBackRunOpenBelowWarmsFromAViewStartBelowIt() throws Exception {
        // The open-run warm-up under a view that starts at 10-26T00:21Z: the frontier's floor is
        // that start, a finite timestamp, so the rebuild folds the three view rows from there and
        // none below it. b's 00:20Z row is below the view's start and in no run of the view, so
        // b's row at 01:10Z after the restart reads only itself.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM '2025-10-26T00:21:00.000000Z' AS SELECT ts, sym, x, sum(x) OVER w s "
                    + "FROM base WHERE x > 0 WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '02:30' 'Europe/Berlin')");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitAndRefresh(job, rowsOfTheFallBackEve(FALL_BACK_OPEN_RUN_ROWS));
                final LogCapture capture = new LogCapture();
                capture.start();
                try {
                    assertZeroRowDepartureSealsTheFrontier(
                            job,
                            () -> replaceBaseRange(ts("2025-10-26T02:00:00.000000Z"), ts("2025-10-26T02:15:00.000000Z"), Double.NaN),
                            "2025-10-26T00:45:00.000000Z",
                            FRONTIER_STATE_WARM_UP,
                            1,
                            3,
                            0,
                            true,
                            "2025-10-26T00:45:00.000000Z -> 3\n"
                    );
                    capture.drain();
                    capture.assertLogged("live view O3 head miss derived the frontier state [view=lv, frontier=2025-10-26T00:45:00.000000Z, "
                            + "floor=2025-10-26T00:21:00.000000Z, openBelow=true, route=warm-up, rootMaxTs=, rows=3]");
                } finally {
                    capture.stop();
                }
            }
            final String rows = """
                    ts\tsym\tx\ts
                    2025-10-26T00:25:00.000000Z\ta\t1.0\t1.0
                    2025-10-26T00:40:00.000000Z\ta\t1.0\t1.0
                    2025-10-26T00:45:00.000000Z\ta\t1.0\t2.0
                    """;
            assertTieAtTheFrontierFoldsOntoTheRestoredRoot(
                    rows,
                    "('2025-10-26T00:45:00.000000Z', 'a', 1.0), ('2025-10-26T01:10:00.000000Z', 'b', 1.0)",
                    rows + """
                            2025-10-26T00:45:00.000000Z\ta\t1.0\t3.0
                            2025-10-26T01:10:00.000000Z\tb\t1.0\t1.0
                            """
            );
        });
    }

    @Test
    public void testAFrontierInAFallBackRunOpenBelowRestoresTheNewestRootBelowIt() throws Exception {
        // As above, over a timeline whose only root, a day below, counts every row of the view
        // there: the frontier's floor is the view's start, so any complete root below the frontier
        // serves, and the rebuild restores it and folds the base rows above it.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createBerlinAnchoredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitAndRefresh(job, "('2025-10-24T12:00:00.000000Z', 'a', 0.5)");
                commitAndRefresh(job, rowsOfTheFallBackEve(FALL_BACK_OPEN_RUN_ROWS));
                assertZeroRowDepartureSealsTheFrontier(
                        job,
                        () -> replaceBaseRange(ts("2025-10-26T02:00:00.000000Z"), ts("2025-10-26T02:15:00.000000Z"), Double.NaN),
                        "2025-10-26T00:45:00.000000Z",
                        FRONTIER_STATE_RESTORE,
                        0,
                        57,
                        2,
                        false,
                        """
                                2025-10-24T12:00:00.000000Z -> 1
                                2025-10-26T00:45:00.000000Z -> 58
                                """
                );
            }
            assertTheFallBackOpenRunRestores("2025-10-24T12:00:00.000000Z\ta\t0.5\t0.5\n");
        });
    }

    @Test
    public void testAFrontierInAFallBackRunOpenBelowOverALostDayRestoresRetainedRows() throws Exception {
        // The restoring fall-back departure over a base that has lost the day of the root below
        // the frontier: the view keeps that day's row through both restarts.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createBerlinAnchoredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitAndRefresh(job, "('2025-10-24T12:00:00.000000Z', 'a', 0.5)");
                commitAndRefresh(job, rowsOfTheFallBackEve(FALL_BACK_OPEN_RUN_ROWS));
                execute("ALTER TABLE base DROP PARTITION LIST '2025-10-24'");
                drainWalQueue();
                driveRefreshToQuiescence(job);
                assertZeroRowDepartureSealsTheFrontier(
                        job,
                        () -> replaceBaseRange(ts("2025-10-26T02:00:00.000000Z"), ts("2025-10-26T02:15:00.000000Z"), Double.NaN),
                        "2025-10-26T00:45:00.000000Z",
                        FRONTIER_STATE_RESTORE,
                        0,
                        57,
                        2,
                        false,
                        """
                                2025-10-24T12:00:00.000000Z -> 1
                                2025-10-26T00:45:00.000000Z -> 58
                                """
                );
            }
            assertTheFallBackOpenRunRestores("2025-10-24T12:00:00.000000Z\ta\t0.5\t0.5\n");
        });
    }

    @Test
    public void testAnEndOfTableRepairInTheRepeatedHalfHourSharesItsFloorWithTheFrontier() throws Exception {
        // The fall-back eve with the departed rows at 01:10Z to 01:20Z, in the half hour after the
        // transition, which reads 02:10 to 02:20 again and carries 10-25's anchor value: the
        // replay warmed from 10-25's segment start, and the new frontier 00:25Z sits on that same
        // segment, so the two floors are one timestamp reached from two stretches, and the
        // rebuild seals the frontier without a read. b's row at 01:15Z after the restart continues
        // its 10-25 run.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createBerlinAnchoredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitAndRefresh(
                        job,
                        rowsOfTheFallBackEve("('2025-10-26T01:10:00.000000Z', 'a', 1.0), ('2025-10-26T01:15:00.000000Z', 'a', 1.0), "
                                + "('2025-10-26T01:20:00.000000Z', 'a', 1.0)")
                );
                final LogCapture capture = new LogCapture();
                capture.start();
                try {
                    assertZeroRowDepartureSealsTheFrontier(
                            job,
                            () -> replaceBaseRange(ts("2025-10-26T01:10:00.000000Z"), ts("2025-10-26T01:25:00.000000Z"), Double.NaN),
                            "2025-10-26T00:25:00.000000Z",
                            FRONTIER_STATE_SKIP,
                            1,
                            55,
                            0,
                            true,
                            "2025-10-26T00:25:00.000000Z -> 55\n"
                    );
                    capture.drain();
                    capture.assertLogged("live view O3 head miss derived the frontier state [view=lv, frontier=2025-10-26T00:25:00.000000Z, "
                            + "floor=2025-10-25T00:30:00.000000Z, openBelow=false, route=skip, rootMaxTs=, rows=0]");
                } finally {
                    capture.stop();
                }
            }
            final String rows = """
                    ts\tsym\tx\ts
                    2025-10-26T00:20:00.000000Z\ta\t1.0\t53.0
                    2025-10-26T00:20:00.000000Z\tb\t1.0\t1.0
                    2025-10-26T00:25:00.000000Z\ta\t1.0\t54.0
                    """;
            assertTieAtTheFrontierFoldsOntoTheRestoredRoot(
                    FALL_BACK_TAIL_SQL,
                    rows,
                    "('2025-10-26T00:25:00.000000Z', 'a', 1.0), ('2025-10-26T01:15:00.000000Z', 'b', 1.0)",
                    rows + """
                            2025-10-26T00:25:00.000000Z\ta\t1.0\t55.0
                            2025-10-26T01:15:00.000000Z\tb\t1.0\t2.0
                            """
            );
        });
    }

    @Test
    public void testAMixedViewWhoseRepairSitsInAFallBackRunOpenBelowSealsWithoutAFold() throws Exception {
        // The open-run skip under a view that also sums a one-minute RANGE frame: the replay's
        // floor is the view's start, the anchored arm's, and the frontier's floor is the lower of
        // 00:24Z and 10-25's segment start, still above it, so the rebuild seals without a read.
        // The rows the replay folded below the frame's floor leave the RANGE sums alone: the tie
        // at 00:25Z reads 00:25Z and itself.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS SELECT ts, sym, x, "
                    + "sum(x) OVER (PARTITION BY sym ORDER BY ts RANGE BETWEEN '1' MINUTE PRECEDING AND CURRENT ROW) s, "
                    + "sum(x) OVER w c FROM base WHERE x > 0 WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '02:30' 'Europe/Berlin')");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitAndRefresh(job, rowsOfTheFallBackEve("('2025-10-26T00:50:00.000000Z', 'a', 1.0)"));
                final LogCapture capture = new LogCapture();
                capture.start();
                try {
                    assertZeroRowDepartureSealsTheFrontier(
                            job,
                            () -> replaceBaseRange(ts("2025-10-26T00:30:00.000000Z"), ts("2025-10-26T01:00:00.000000Z"), Double.NaN),
                            "2025-10-26T00:25:00.000000Z",
                            FRONTIER_STATE_SKIP,
                            1,
                            55,
                            0,
                            true,
                            "2025-10-26T00:25:00.000000Z -> 55\n"
                    );
                    capture.drain();
                    capture.assertLogged("live view O3 head miss derived the frontier state [view=lv, frontier=2025-10-26T00:25:00.000000Z, "
                            + "floor=2025-10-25T00:30:00.000000Z, openBelow=false, route=skip, rootMaxTs=, rows=0]");
                } finally {
                    capture.stop();
                }
            }
            final String rows = """
                    ts\tsym\tx\ts\tc
                    2025-10-26T00:20:00.000000Z\ta\t1.0\t1.0\t53.0
                    2025-10-26T00:20:00.000000Z\tb\t1.0\t1.0\t1.0
                    2025-10-26T00:25:00.000000Z\ta\t1.0\t1.0\t54.0
                    """;
            assertTieAtTheFrontierFoldsOntoTheRestoredRoot(
                    "SELECT ts, sym, x, s, c FROM lv WHERE ts < '2025-10-25T00:00:00.000000Z' OR ts >= '2025-10-26T00:20:00.000000Z'",
                    rows,
                    "('2025-10-26T00:25:00.000000Z', 'a', 1.0), ('2025-10-26T01:10:00.000000Z', 'b', 1.0)",
                    rows + """
                            2025-10-26T00:25:00.000000Z\ta\t1.0\t2.0\t55.0
                            2025-10-26T01:10:00.000000Z\tb\t1.0\t1.0\t2.0
                            """
            );
        });
    }

    @Test
    public void testAFrontierInTheRepeatedHalfHourWarmsFromThePreviousDaysStart() throws Exception {
        // The new frontier 01:10Z sits in the half hour after the fall-back, which carries 10-25's
        // anchor value again, and the departed rows at 02:00Z on 10-26's own segment, which the
        // replay warmed from 01:30Z. The frontier's floor is 10-25's segment start, a day below,
        // and no root sits at or below the frontier, so the rebuild folds every base row from
        // there. a's 01:05Z row resets the run its 00:40Z row was on, b's 01:20Z row after the
        // restart continues the run of its 00:20Z row, and a warm-up from 01:00Z would miss it.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createBerlinAnchoredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitAndRefresh(
                        job,
                        rowsOfTheFallBackEve("('2025-10-26T00:40:00.000000Z', 'a', 1.0), ('2025-10-26T01:05:00.000000Z', 'a', 1.0), "
                                + "('2025-10-26T01:10:00.000000Z', 'a', 1.0), ('2025-10-26T02:00:00.000000Z', 'a', 1.0), "
                                + "('2025-10-26T02:05:00.000000Z', 'a', 1.0), ('2025-10-26T02:10:00.000000Z', 'a', 1.0)")
                );
                final LogCapture capture = new LogCapture();
                capture.start();
                try {
                    assertZeroRowDepartureSealsTheFrontier(
                            job,
                            () -> replaceBaseRange(ts("2025-10-26T02:00:00.000000Z"), ts("2025-10-26T02:15:00.000000Z"), Double.NaN),
                            "2025-10-26T01:10:00.000000Z",
                            FRONTIER_STATE_WARM_UP,
                            1,
                            58,
                            0,
                            true,
                            "2025-10-26T01:10:00.000000Z -> 58\n"
                    );
                    capture.drain();
                    capture.assertLogged("live view O3 head miss derived the frontier state [view=lv, frontier=2025-10-26T01:10:00.000000Z, "
                            + "floor=2025-10-25T00:30:00.000000Z, openBelow=false, route=warm-up, rootMaxTs=, rows=58]");
                } finally {
                    capture.stop();
                }
            }
            final String rows = """
                    ts\tsym\tx\ts
                    2025-10-26T00:20:00.000000Z\ta\t1.0\t53.0
                    2025-10-26T00:20:00.000000Z\tb\t1.0\t1.0
                    2025-10-26T00:25:00.000000Z\ta\t1.0\t54.0
                    2025-10-26T00:40:00.000000Z\ta\t1.0\t1.0
                    2025-10-26T01:05:00.000000Z\ta\t1.0\t1.0
                    2025-10-26T01:10:00.000000Z\ta\t1.0\t2.0
                    """;
            assertTieAtTheFrontierFoldsOntoTheRestoredRoot(
                    FALL_BACK_TAIL_SQL,
                    rows,
                    "('2025-10-26T01:10:00.000000Z', 'a', 1.0), ('2025-10-26T01:20:00.000000Z', 'b', 1.0)",
                    rows + """
                            2025-10-26T01:10:00.000000Z\ta\t1.0\t3.0
                            2025-10-26T01:20:00.000000Z\tb\t1.0\t2.0
                            """
            );
        });
    }

    @Test
    public void testAnEndOfTableRepairOnTheSpringForwardDaySealsWithoutAFold() throws Exception {
        // The departed rows at 06:00Z on 2025-03-30, the spring-forward day, whose segment starts
        // at 01:00Z, half an hour below its own anchor value, so it has no start of its own and
        // the replay read from the view's start. The new frontier 00:50Z sits on 03-29's segment,
        // which starts at 01:30Z that day, above the replay's floor: the replay already folded
        // every row the frontier's state holds, and the rebuild seals it without a read. The
        // 03-29T01:00Z row reads 02:00 CET, below 02:30, and stays on 03-28's run.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createBerlinAnchoredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitAndRefresh(
                        job,
                        "('2025-03-29T01:00:00.000000Z', 'a', 0.25), ('2025-03-29T12:00:00.000000Z', 'a', 1.0), "
                                + "('2025-03-29T23:00:00.000000Z', 'a', 1.0), ('2025-03-30T00:50:00.000000Z', 'a', 1.0), "
                                + "('2025-03-30T06:00:00.000000Z', 'a', 1.0), ('2025-03-30T06:05:00.000000Z', 'a', 1.0)"
                );
                final LogCapture capture = new LogCapture();
                capture.start();
                try {
                    assertZeroRowDepartureSealsTheFrontier(
                            job,
                            () -> replaceBaseRange(ts("2025-03-30T06:00:00.000000Z"), ts("2025-03-30T07:00:00.000000Z"), Double.NaN),
                            "2025-03-30T00:50:00.000000Z",
                            FRONTIER_STATE_SKIP,
                            1,
                            4,
                            0,
                            true,
                            "2025-03-30T00:50:00.000000Z -> 4\n"
                    );
                    capture.drain();
                    capture.assertLogged("live view O3 head miss derived the frontier state [view=lv, frontier=2025-03-30T00:50:00.000000Z, "
                            + "floor=2025-03-29T01:30:00.000000Z, openBelow=false, route=skip, rootMaxTs=, rows=0]");
                } finally {
                    capture.stop();
                }
            }
            final String rows = """
                    ts\tsym\tx\ts
                    2025-03-29T01:00:00.000000Z\ta\t0.25\t0.25
                    2025-03-29T12:00:00.000000Z\ta\t1.0\t1.0
                    2025-03-29T23:00:00.000000Z\ta\t1.0\t2.0
                    2025-03-30T00:50:00.000000Z\ta\t1.0\t3.0
                    """;
            assertTieAtTheFrontierFoldsOntoTheRestoredRoot(
                    rows,
                    "('2025-03-30T00:50:00.000000Z', 'a', 1.0)",
                    rows + "2025-03-30T00:50:00.000000Z\ta\t1.0\t4.0\n"
            );
        });
    }

    @Test
    public void testAFrontierOnTheSpringForwardDayWarmsFromTheViewStart() throws Exception {
        // ANCHOR DAILY '02:30' Europe/Berlin across the 2025-03-30 spring-forward: 02:30 CET never
        // happens that day, and its run starts at 03:00 CEST, 01:00Z, half an hour below its own
        // anchor value 01:30Z, so the segment has no start of its own. The new frontier 12:00Z sits
        // on it and the departed rows on 03-31, which the replay warmed from that day's start: the
        // rebuild folds every base row up to the frontier, and a's run from 01:00Z counts.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createBerlinAnchoredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitAndRefresh(
                        job,
                        "('2025-03-29T01:00:00.000000Z', 'a', 0.25), ('2025-03-29T12:00:00.000000Z', 'a', 1.0), "
                                + "('2025-03-30T00:30:00.000000Z', 'a', 1.0), ('2025-03-30T01:00:00.000000Z', 'a', 1.0), "
                                + "('2025-03-30T01:10:00.000000Z', 'b', 1.0), ('2025-03-30T06:00:00.000000Z', 'a', 1.0), "
                                + "('2025-03-30T12:00:00.000000Z', 'a', 1.0), ('2025-03-31T06:00:00.000000Z', 'a', 1.0), "
                                + "('2025-03-31T06:05:00.000000Z', 'a', 1.0)"
                );
                assertZeroRowDepartureSealsTheFrontier(
                        job,
                        () -> replaceBaseRange(ts("2025-03-31T06:00:00.000000Z"), ts("2025-03-31T07:00:00.000000Z"), Double.NaN),
                        "2025-03-30T12:00:00.000000Z",
                        FRONTIER_STATE_WARM_UP,
                        1,
                        7,
                        0,
                        true,
                        "2025-03-30T12:00:00.000000Z -> 7\n"
                );
            }
            final String rows = """
                    ts\tsym\tx\ts
                    2025-03-29T01:00:00.000000Z\ta\t0.25\t0.25
                    2025-03-29T12:00:00.000000Z\ta\t1.0\t1.0
                    2025-03-30T00:30:00.000000Z\ta\t1.0\t2.0
                    2025-03-30T01:00:00.000000Z\ta\t1.0\t1.0
                    2025-03-30T01:10:00.000000Z\tb\t1.0\t1.0
                    2025-03-30T06:00:00.000000Z\ta\t1.0\t2.0
                    2025-03-30T12:00:00.000000Z\ta\t1.0\t3.0
                    """;
            assertTieAtTheFrontierFoldsOntoTheRestoredRoot(
                    rows,
                    "('2025-03-30T12:00:00.000000Z', 'a', 1.0), ('2025-03-30T13:00:00.000000Z', 'b', 1.0)",
                    rows + """
                            2025-03-30T12:00:00.000000Z\ta\t1.0\t4.0
                            2025-03-30T13:00:00.000000Z\tb\t1.0\t2.0
                            """
            );
        });
    }

    @Test
    public void testAFrontierOnTheSpringForwardDayOverALostDayRestoresTheRootBelowIt() throws Exception {
        // The spring-forward departure over a timeline whose only root, on 03-27, counts every row
        // of the view there, over a base that has since lost that day: the frontier's floor is the
        // view's start, so that root serves however old it is, and the rebuild restores it and
        // folds the seven base rows above it. The view keeps the lost day's row through both
        // restarts.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createBerlinAnchoredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitAndRefresh(job, "('2025-03-27T12:00:00.000000Z', 'a', 0.5)");
                commitAndRefresh(
                        job,
                        "('2025-03-29T01:00:00.000000Z', 'a', 0.25), ('2025-03-29T12:00:00.000000Z', 'a', 1.0), "
                                + "('2025-03-30T00:30:00.000000Z', 'a', 1.0), ('2025-03-30T01:00:00.000000Z', 'a', 1.0), "
                                + "('2025-03-30T01:10:00.000000Z', 'b', 1.0), ('2025-03-30T06:00:00.000000Z', 'a', 1.0), "
                                + "('2025-03-30T12:00:00.000000Z', 'a', 1.0), ('2025-03-31T06:00:00.000000Z', 'a', 1.0), "
                                + "('2025-03-31T06:05:00.000000Z', 'a', 1.0)"
                );
                execute("ALTER TABLE base DROP PARTITION LIST '2025-03-27'");
                drainWalQueue();
                driveRefreshToQuiescence(job);
                assertZeroRowDepartureSealsTheFrontier(
                        job,
                        () -> replaceBaseRange(ts("2025-03-31T06:00:00.000000Z"), ts("2025-03-31T07:00:00.000000Z"), Double.NaN),
                        "2025-03-30T12:00:00.000000Z",
                        FRONTIER_STATE_RESTORE,
                        0,
                        7,
                        2,
                        false,
                        """
                                2025-03-27T12:00:00.000000Z -> 1
                                2025-03-30T12:00:00.000000Z -> 8
                                """
                );
            }
            final String rows = """
                    ts\tsym\tx\ts
                    2025-03-27T12:00:00.000000Z\ta\t0.5\t0.5
                    2025-03-29T01:00:00.000000Z\ta\t0.25\t0.25
                    2025-03-29T12:00:00.000000Z\ta\t1.0\t1.0
                    2025-03-30T00:30:00.000000Z\ta\t1.0\t2.0
                    2025-03-30T01:00:00.000000Z\ta\t1.0\t1.0
                    2025-03-30T01:10:00.000000Z\tb\t1.0\t1.0
                    2025-03-30T06:00:00.000000Z\ta\t1.0\t2.0
                    2025-03-30T12:00:00.000000Z\ta\t1.0\t3.0
                    """;
            assertTieAtTheFrontierFoldsOntoTheRestoredRoot(
                    rows,
                    "('2025-03-30T12:00:00.000000Z', 'a', 1.0), ('2025-03-30T13:00:00.000000Z', 'b', 1.0)",
                    rows + """
                            2025-03-30T12:00:00.000000Z\ta\t1.0\t4.0
                            2025-03-30T13:00:00.000000Z\tb\t1.0\t2.0
                            """
            );
        });
    }

    @Test
    public void testAFrontierInTheFirstBucketOfANonZeroOriginWarmsFromTheViewStart() throws Exception {
        // A zone-less ANCHOR DAILY '02:30' over 1970: every row below 1970-01-02T02:30 floors to
        // the alignment origin, a segment with no start of its own. The new frontier 12:00 sits
        // there and the departed rows on the next day's segment, so the rebuild folds every base
        // row up to the frontier: a row at 12:00 after the restart reads all four.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS SELECT ts, sym, x, sum(x) OVER w s FROM base "
                    + "WHERE x > 0 WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '02:30')");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitAndRefresh(
                        job,
                        "('1970-01-01T01:00:00.000000Z', 'a', 1.0), ('1970-01-01T02:00:00.000000Z', 'a', 1.0), "
                                + "('1970-01-01T03:00:00.000000Z', 'a', 1.0), ('1970-01-01T12:00:00.000000Z', 'a', 1.0), "
                                + "('1970-01-02T03:00:00.000000Z', 'a', 1.0), ('1970-01-02T03:05:00.000000Z', 'a', 1.0)"
                );
                assertZeroRowDepartureSealsTheFrontier(
                        job,
                        () -> replaceBaseRange(ts("1970-01-02T03:00:00.000000Z"), ts("1970-01-02T04:00:00.000000Z"), Double.NaN),
                        "1970-01-01T12:00:00.000000Z",
                        FRONTIER_STATE_WARM_UP,
                        1,
                        4,
                        0,
                        true,
                        "1970-01-01T12:00:00.000000Z -> 4\n"
                );
            }
            final String rows = """
                    ts\tsym\tx\ts
                    1970-01-01T01:00:00.000000Z\ta\t1.0\t1.0
                    1970-01-01T02:00:00.000000Z\ta\t1.0\t2.0
                    1970-01-01T03:00:00.000000Z\ta\t1.0\t3.0
                    1970-01-01T12:00:00.000000Z\ta\t1.0\t4.0
                    """;
            assertTieAtTheFrontierFoldsOntoTheRestoredRoot(
                    rows,
                    "('1970-01-01T12:00:00.000000Z', 'a', 1.0)",
                    rows + "1970-01-01T12:00:00.000000Z\ta\t1.0\t5.0\n"
            );
        });
    }

    @Test
    public void testAnEndOfTableRepairWithoutASpliceWithNoRootBelowItOpensAFreshHistoryAtItsFrontier() throws Exception {
        // No chained boundary is allowed and the only root sits at 00:59, so the truncate at R ahead
        // of the commit keeps no root and retires the timeline. The rebuild warmed the state at the
        // new frontier 00:56 from the frame floor 00:55 ahead of that, and its seal opens a fresh
        // history there.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, 0);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitAndRefresh(job, minuteRowsOfA(0, 59, "1.0"));
                assertZeroRowDepartureSealsTheFrontier(
                        job,
                        () -> replaceBaseRange(ts("2026-01-01T00:57:00.000000Z"), ts("2026-01-01T01:00:00.000000Z"), Double.NaN),
                        "2026-01-01T00:56:00.000000Z",
                        FRONTIER_STATE_WARM_UP,
                        0,
                        3,
                        0,
                        true,
                        "2026-01-01T00:56:00.000000Z -> 57\n"
                );
            }
            assertRecomputedRowsThenTheTieAtTheFrontierRestore(headMissDepartureRows(false, 56));
        });
    }

    @Test
    public void testAnEndOfTableRepairWithoutASpliceThatDeletesTheRowsAboveAGrownRootSealsItsFrontier() throws Exception {
        // No chained boundary is allowed, and the root at the row that becomes the frontier grew a
        // tie of b after its seal. The truncate at R ahead of the commit keeps it, as the prefix's
        // newest root; the rebuild warmed the state at 00:56 from the frame floor 00:55, both rows
        // at 00:56 included, so after the commit a second truncate drops the grown root at the
        // frontier and the seal puts the frontier's own root on that state.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, 0);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitAndRefresh(job, "('2025-12-31T23:55:00.000000Z', 'a', 0.5)");
                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                commitAndRefresh(job, minuteRowsOfA(0, 56, "1.0"));
                commitAndRefresh(job, "('2026-01-01T00:56:00.000000Z', 'b', 1.0)");
                commitAndRefresh(job, minuteRowsOfA(57, 59, "1.0"));
                final LogCapture capture = new LogCapture();
                capture.start();
                try {
                    assertZeroRowDepartureSealsTheFrontier(
                            job,
                            () -> replaceBaseRange(ts("2026-01-01T00:57:00.000000Z"), ts("2026-01-01T01:00:00.000000Z"), Double.NaN),
                            "2026-01-01T00:56:00.000000Z",
                            FRONTIER_STATE_WARM_UP,
                            0,
                            5,
                            // The truncate at R, the one at the frontier and the seal.
                            3,
                            false,
                            """
                                    2025-12-31T23:55:00.000000Z -> 1
                                    2026-01-01T00:56:00.000000Z -> 59
                                    """
                    );
                    capture.drain();
                    capture.assertLogged("live view O3 head miss emitted no row under a root at or above its frontier, truncating at the frontier [view=lv, "
                            + "frontier=2026-01-01T00:56:00.000000Z, rootMaxTs=2026-01-01T00:56:00.000000Z, truncateFloor=2026-01-01T00:56:00.000000Z]");
                } finally {
                    capture.stop();
                }
            }
            assertRecomputedRowsThenTheTieAtTheFrontierRestore(headMissDepartureRows(true, 56) + "2026-01-01T00:56:00.000000Z\tb\t1.0\t1.0\n");
        });
    }

    @Test
    public void testAnEndOfTableRepairWithoutASpliceWhoseFrontierRootLookupFailsRetiresTheTimeline() throws Exception {
        // No chained boundary is allowed, and the root at the row that becomes the frontier counts
        // every row of the view there, so the rebuild derives nothing; after its commit the read
        // of that root fails, and a root the repair cannot vet is no root to keep the truncated
        // timeline over: it retires. Over a base that lost nothing the restart rebuilds what a
        // recompute gives.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, 0);
        final AtomicBoolean isArmed = new AtomicBoolean();
        final AtomicBoolean isConsumedSeqTxnAdvanced = new AtomicBoolean();
        final AtomicInteger refusedReads = new AtomicInteger();
        // Once armed, refuses the first open of the timeline's superblock after the repair writes
        // the view's state file to advance its consumed seqTxn: the lookup of the root at the
        // frontier, which runs next.
        final TestFilesFacadeImpl ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (isArmed.get() && Utf8s.endsWithAscii(name, LiveViewState.LIVE_VIEW_STATE_FILE_NAME)) {
                    isArmed.set(false);
                    isConsumedSeqTxnAdvanced.set(true);
                } else if (isConsumedSeqTxnAdvanced.get() && Utf8s.endsWithAscii(name, LiveViewCheckpointLayout.TIMELINE_FILE_NAME)) {
                    isConsumedSeqTxnAdvanced.set(false);
                    refusedReads.incrementAndGet();
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(ff, () -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitRootAtTheFrontierThenRowsAboveItOverAnIntactBase(job);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                final long resetsBefore = instance.getCheckpointTimelineResets();
                final LogCapture capture = new LogCapture();
                capture.start();
                try {
                    isArmed.set(true);
                    replaceBaseRange(ts("2026-01-01T00:59:00.000000Z"), ts("2026-01-01T01:00:00.000000Z"), Double.NaN);
                    drainWalQueue();
                    driveRefreshToQuiescence(job);
                    capture.drain();
                    capture.assertLogged("could not read the live view checkpoint root at the frontier [view=lv");
                } finally {
                    capture.stop();
                }
                Assert.assertEquals(1, refusedReads.get());
                final LiveViewRefreshStats stats = job.getStatsForTest();
                Assert.assertEquals(0, stats.get(LiveViewRefreshStats.HEAD_MISS_TRUNCATE_ZERO_ROW_KEEP_COUNT));
                Assert.assertEquals(0, stats.get(LiveViewRefreshStats.HEAD_MISS_ZERO_ROW_FRONTIER_SEAL_COUNT));
                Assert.assertEquals(0, stats.get(LiveViewRefreshStats.HEAD_MISS_FRONTIER_STATE_RESTORE_COUNT));
                Assert.assertEquals(0, stats.get(LiveViewRefreshStats.HEAD_MISS_FRONTIER_STATE_WARM_UP_COUNT));
                assertTheDepartureRetiredTheTimeline(instance, resetsBefore);
            }
            assertRebuiltOnRestartAsARecomputeDoes(headMissDepartureRows(true, 56));
        });
    }

    @Test
    public void testAnEndOfTableRepairWithoutASpliceWhoseFrontierWatermarkFailsRetiresTheTimeline() throws Exception {
        // The no-splice departure over a root at the frontier, with the watermark that would publish
        // that root at the repair commit failing: the truncated prefix stays at a base coordinate
        // below the commit, so the repair retires it. Over a base that lost nothing the restart
        // rebuilds what a recompute gives.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, 0);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitRootAtTheFrontierThenRowsAboveItOverAnIntactBase(job);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                final long resetsBefore = instance.getCheckpointTimelineResets();
                hooks(job).setSimulateHeadMissHeadSealStart(
                        () -> job.setCheckpointTimelineTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH)
                );
                replaceBaseRange(ts("2026-01-01T00:59:00.000000Z"), ts("2026-01-01T01:00:00.000000Z"), Double.NaN);
                drainWalQueue();
                driveRefreshToQuiescence(job);
                job.setCheckpointTimelineTestFailureStage(0);
                final LiveViewRefreshStats stats = job.getStatsForTest();
                Assert.assertEquals(0, stats.get(LiveViewRefreshStats.HEAD_MISS_TRUNCATE_ZERO_ROW_KEEP_COUNT));
                Assert.assertEquals(0, stats.get(LiveViewRefreshStats.HEAD_MISS_ZERO_ROW_FRONTIER_SEAL_COUNT));
                Assert.assertEquals("the watermark fails", 1, instance.getCheckpointSealFailures());
                assertTheDepartureRetiredTheTimeline(instance, resetsBefore);
            }
            assertRebuiltOnRestartAsARecomputeDoes(headMissDepartureRows(true, 56));
        });
    }

    @Test
    public void testAnEndOfTableRepairWhoseFrontierSealFailsRetiresTheTimeline() throws Exception {
        // The departure over one batch with no root above the frontier, with the seal of the state
        // the rebuild derived at the frontier failing: no root stands at the frontier and the
        // splice moved the generation's base coordinate to the repair commit, so the repair
        // retires the timeline. Over a base that lost nothing the restart rebuilds what a
        // recompute gives.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitAndRefresh(job, "('2025-12-31T23:55:00.000000Z', 'a', 0.5)");
                commitAndRefresh(job, minuteRowsOfA(0, 59, "1.0"));
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                final long resetsBefore = instance.getCheckpointTimelineResets();
                hooks(job).setSimulateHeadMissHeadSealStart(
                        () -> job.setCheckpointTimelineTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH)
                );
                replaceBaseRange(ts("2026-01-01T00:57:00.000000Z"), ts("2026-01-01T01:00:00.000000Z"), Double.NaN);
                drainWalQueue();
                driveRefreshToQuiescence(job);
                job.setCheckpointTimelineTestFailureStage(0);
                final LiveViewRefreshStats stats = job.getStatsForTest();
                Assert.assertEquals("the rebuild derives the frontier's state", 1, stats.get(LiveViewRefreshStats.HEAD_MISS_FRONTIER_STATE_WARM_UP_COUNT));
                Assert.assertEquals("and its seal fails", 0, stats.get(LiveViewRefreshStats.HEAD_MISS_ZERO_ROW_FRONTIER_SEAL_COUNT));
                Assert.assertEquals(1, instance.getCheckpointSealFailures());
                Assert.assertEquals(0, stats.get(LiveViewRefreshStats.HEAD_MISS_SPLICE_ZERO_ROW_RETIRE_COUNT));
                assertTheDepartureRetiredTheTimeline(instance, resetsBefore);
            }
            assertRebuiltOnRestartAsARecomputeDoes(headMissDepartureRows(true, 56));
        });
    }

    @Test
    public void testAnEndOfTableRepairWhoseFrontierSitsInACompositeViewPartitionSealsItsFrontier() throws Exception {
        // A REPLACE_RANGE rewrites the rows from 00:10 to 00:14 with the values they had, and the
        // repair that follows rewrites the view's rows of [00:10, 00:15] in the middle of its day,
        // which makes the day's partition composite: the rewritten rows park at the tail of its
        // column files, the six rows they supersede stay behind as dead space, and the rows of the
        // next batch append after them. The departure of the newest three minutes then leaves the
        // frontier 00:56 on a row past the day's live file prefix, which only a walk of the day's
        // pieces reaches. The rebuild finds it there, warms the frame from 00:55 and seals it.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitAndRefresh(job, "('2025-12-31T23:55:00.000000Z', 'a', 0.5)");
                commitAndRefresh(job, minuteRowsOfA(0, 30, "1.0"));
                try (WalWriter walWriter = engine.getWalWriter(engine.verifyTableName("base"))) {
                    for (int minute = 10; minute <= 14; minute++) {
                        final TableWriter.Row row = walWriter.newRow(ts("2026-01-01T00:" + minute + ":00.000000Z"));
                        row.putSym(1, "a");
                        row.putDouble(2, 1.0);
                        row.append();
                    }
                    walWriter.commitWithParams(
                            ts("2026-01-01T00:10:00.000000Z"),
                            ts("2026-01-01T00:15:00.000000Z"),
                            WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE
                    );
                }
                drainWalQueue();
                driveRefreshToQuiescence(job);
                commitAndRefresh(job, minuteRowsOfA(31, 59, "1.0"));
                assertViewRowPastLiveFilePrefix("lv", "2026-01-01T00:56:00.000000Z");
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                // The repair of the rewritten rows kept the runtime, and sealed a root at its
                // frontier 00:30 over the splice.
                assertCheckpointLadder(instance, false, """
                        2025-12-31T23:55:00.000000Z -> 1
                        2026-01-01T00:30:00.000000Z -> 32
                        """);
                assertZeroRowDepartureSealsTheFrontier(
                        job,
                        () -> replaceBaseRange(ts("2026-01-01T00:57:00.000000Z"), ts("2026-01-01T01:00:00.000000Z"), Double.NaN),
                        "2026-01-01T00:56:00.000000Z",
                        FRONTIER_STATE_WARM_UP,
                        0,
                        // The replay reads 00:56; the warm-up 00:55 and 00:56.
                        3,
                        2,
                        false,
                        """
                                2025-12-31T23:55:00.000000Z -> 1
                                2026-01-01T00:30:00.000000Z -> 32
                                2026-01-01T00:56:00.000000Z -> 58
                                """
                );
            }
            assertRecomputedRowsThenTheTieAtTheFrontierRestore(headMissDepartureRows(true, 56));
        });
    }

    @Test
    public void testAnEndOfTableRepairWhoseFrontierTruncateFailsAboveARootAtTheFrontierRetiresTheTimeline() throws Exception {
        // The departure of the row above a root at the frontier 00:56, over a base that lost
        // nothing, with the frontier truncate failing to write its repair marker: no prefix stands
        // with its newest root at the frontier, so the repair puts no head back on that root, and
        // the failed truncate retires the timeline. The restart rebuilds what a recompute gives.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final AtomicInteger markerPublicationsToFail = new AtomicInteger();
        final AtomicBoolean hasMarkerPublicationFailed = new AtomicBoolean();
        final TestFilesFacadeImpl ff = new TestFilesFacadeImpl() {
            @Override
            public int rename(LPSZ from, LPSZ to) {
                if (Utf8s.endsWithAscii(to, Files.SEPARATOR + LiveViewCheckpointLayout.REPAIRING_MARKER_FILE_NAME)
                        && markerPublicationsToFail.get() > 0
                        && markerPublicationsToFail.decrementAndGet() == 0) {
                    hasMarkerPublicationFailed.set(true);
                    return Files.FILES_RENAME_ERR_OTHER;
                }
                return super.rename(from, to);
            }
        };
        assertMemoryLeak(ff, () -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitAndRefresh(job, "('2025-12-31T23:55:00.000000Z', 'a', 0.5)");
                commitAndRefresh(job, minuteRowsOfA(0, 30, "1.0"));
                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                commitAndRefresh(job, minuteRowsOfA(31, 56, "1.0"));
                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                commitAndRefresh(
                        job,
                        "('2026-01-01T00:57:00.000000Z', 'a', -1.0), ('2026-01-01T00:57:30.000000Z', 'a', -1.0), ('2026-01-01T00:59:00.000000Z', 'a', 1.0)"
                );
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                assertCheckpointLadder(instance, false, """
                        2025-12-31T23:55:00.000000Z -> 1
                        2026-01-01T00:56:00.000000Z -> 58
                        2026-01-01T00:59:00.000000Z -> 59
                        """);
                final long resetsBefore = instance.getCheckpointTimelineResets();
                // Ahead of the commit: the splice's marker goes down at the commit, and the frontier
                // truncate's after it.
                hooks(job).setSimulateHeadMissFrontierStateFoldStart(() -> markerPublicationsToFail.set(2));
                final LogCapture capture = new LogCapture();
                capture.start();
                try {
                    replaceBaseRange(ts("2026-01-01T00:59:00.000000Z"), ts("2026-01-01T01:00:00.000000Z"), Double.NaN);
                    drainWalQueue();
                    driveRefreshToQuiescence(job);
                    capture.drain();
                    capture.assertLogged("could not preserve live view checkpoint timeline prefix, retiring [view=lv");
                } finally {
                    capture.stop();
                }
                Assert.assertTrue("the frontier truncate's marker fails", hasMarkerPublicationFailed.get());
                final LiveViewRefreshStats stats = job.getStatsForTest();
                Assert.assertEquals("the head-miss rebuild repairs the departure", 1, countHeadMissRepairs(job));
                Assert.assertEquals(1, stats.get(LiveViewRefreshStats.HEAD_MISS_SPLICE_FRONTIER_TRUNCATE_COUNT));
                Assert.assertEquals(0, stats.get(LiveViewRefreshStats.HEAD_MISS_SPLICE_ZERO_ROW_KEEP_COUNT));
                Assert.assertEquals(0, stats.get(LiveViewRefreshStats.HEAD_MISS_SPLICE_ZERO_ROW_RETIRE_COUNT));
                Assert.assertEquals("the root at the frontier needs no derivation", 0, stats.get(LiveViewRefreshStats.HEAD_MISS_FRONTIER_STATE_WARM_UP_COUNT));
                Assert.assertEquals(0, stats.get(LiveViewRefreshStats.HEAD_MISS_FRONTIER_STATE_RESTORE_COUNT));
                Assert.assertEquals(0, stats.get(LiveViewRefreshStats.HEAD_MISS_ZERO_ROW_FRONTIER_SEAL_COUNT));
                assertTheDepartureRetiredTheTimeline(instance, resetsBefore);
                assertNoRefreshFaults("lv");
            }
            assertRebuiltOnRestartAsARecomputeDoes(headMissDepartureRows(true, 56));
        });
    }

    @Test
    public void testAnEndOfTableRepairWhoseSpliceFailsToPublishRetiresTheTimeline() throws Exception {
        // The departure over one batch with no root above the frontier, with the publication of
        // the splice the repair captured failing after the commit: the rebuild derived the state
        // at the frontier, but no generation describes the output the commit moved, so it seals
        // nothing there and retires the timeline, as every repair whose splice did not publish
        // does. Over a base that lost nothing the restart rebuilds what a recompute gives.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitAndRefresh(job, "('2025-12-31T23:55:00.000000Z', 'a', 0.5)");
                commitAndRefresh(job, minuteRowsOfA(0, 59, "1.0"));
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                final long resetsBefore = instance.getCheckpointTimelineResets();
                final long repairFailuresBefore = instance.getCheckpointRepairFailures();
                // Ahead of the commit, so the splice is the next timeline publication.
                hooks(job).setSimulateHeadMissFrontierStateFoldStart(
                        () -> job.setCheckpointTimelineTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_DATA_PUBLISH)
                );
                final LogCapture capture = new LogCapture();
                capture.start();
                try {
                    replaceBaseRange(ts("2026-01-01T00:57:00.000000Z"), ts("2026-01-01T01:00:00.000000Z"), Double.NaN);
                    drainWalQueue();
                    driveRefreshToQuiescence(job);
                    capture.drain();
                    capture.assertLogged("could not publish live view checkpoint timeline repair [view=lv");
                } finally {
                    capture.stop();
                    hooks(job).setSimulateHeadMissFrontierStateFoldStart(null);
                    job.setCheckpointTimelineTestFailureStage(0);
                }
                final LiveViewRefreshStats stats = job.getStatsForTest();
                Assert.assertEquals("the head-miss rebuild repairs the departure", 1, countHeadMissRepairs(job));
                Assert.assertEquals("the rebuild derives the frontier's state", 1, stats.get(LiveViewRefreshStats.HEAD_MISS_FRONTIER_STATE_WARM_UP_COUNT));
                Assert.assertEquals("but seals nothing over a splice that did not publish", 0, stats.get(LiveViewRefreshStats.HEAD_MISS_ZERO_ROW_FRONTIER_SEAL_COUNT));
                Assert.assertEquals(0, stats.get(LiveViewRefreshStats.HEAD_MISS_TRUNCATE_ZERO_ROW_KEEP_COUNT));
                Assert.assertEquals(0, stats.get(LiveViewRefreshStats.HEAD_MISS_SPLICE_ZERO_ROW_RETIRE_COUNT));
                Assert.assertEquals("the splice's publication fails", repairFailuresBefore + 1, instance.getCheckpointRepairFailures());
                Assert.assertEquals("no seal fails", 0, instance.getCheckpointSealFailures());
                assertTheDepartureRetiredTheTimeline(instance, resetsBefore);
                assertNoRefreshFaults("lv");
            }
            assertRebuiltOnRestartAsARecomputeDoes(headMissDepartureRows(true, 56));
        });
    }

    @Test
    public void testAnEndOfTableRepairOfAnUnfilteredViewSealsItsFrontier() throws Exception {
        // The no-root-at-the-frontier departure over a view with no filter: the warm-up counts the
        // rows its window cursor produced, every base row it read.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS SELECT ts, sym, x, sum(x) OVER (PARTITION BY sym "
                    + "ORDER BY ts RANGE BETWEEN '1' MINUTE PRECEDING AND CURRENT ROW) s FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitAndRefresh(job, "('2025-12-31T23:55:00.000000Z', 'a', 0.5)");
                commitAndRefresh(job, minuteRowsOfA(0, 30, "1.0"));
                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                commitAndRefresh(job, minuteRowsOfA(31, 59, "1.0"));
                assertZeroRowDepartureSealsTheFrontier(
                        job,
                        () -> replaceBaseRange(ts("2026-01-01T00:57:00.000000Z"), ts("2026-01-01T01:00:00.000000Z"), Double.NaN),
                        "2026-01-01T00:56:00.000000Z",
                        FRONTIER_STATE_WARM_UP,
                        1,
                        3,
                        3,
                        false,
                        """
                                2025-12-31T23:55:00.000000Z -> 1
                                2026-01-01T00:56:00.000000Z -> 58
                                """
                );
            }
            // Every row passes the filter the departure view's recompute applies.
            assertRecomputedRowsThenTheTieAtTheFrontierRestore(headMissDepartureRows(true, 56));
        });
    }

    @Test
    public void testAnEndOfTableRepairThatParksSealsItsFrontierOnTheTurnThatFinishesIt() throws Exception {
        // A replay budget of one row parks the daily anchored rebuild whose departed rows share the
        // new frontier's segment on every row it reads: the turn that finishes it holds what every
        // turn folded, which is the frontier's state, and seals it without a read.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            createDailyAnchoredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitTheDayTheBaseLoses(job);
                commitAndRefresh(job, minuteRowsOfA(0, 59, "1.0"));
                commitAndRefresh(job, minuteRowsOfAOnTheSecond(0, 10));
                final LogCapture capture = new LogCapture();
                capture.start();
                try {
                    assertZeroRowDepartureSealsTheFrontier(
                            job,
                            () -> replaceBaseRange(ts("2026-01-02T00:08:00.000000Z"), ts("2026-01-02T00:11:00.000000Z"), Double.NaN),
                            "2026-01-02T00:07:00.000000Z",
                            FRONTIER_STATE_SKIP,
                            0,
                            -1,
                            2,
                            false,
                            """
                                    2025-12-31T23:55:00.000000Z -> 1
                                    2026-01-02T00:07:00.000000Z -> 69
                                    """
                    );
                    capture.drain();
                    capture.assertLogged("live view O3 repair yielded on its turn budget [view=lv");
                } finally {
                    capture.stop();
                }
            }
            final String sql = "SELECT ts, sym, x, s FROM lv WHERE ts >= '2026-01-02T00:06:00.000000Z'";
            final String rows = """
                    ts\tsym\tx\ts
                    2026-01-02T00:06:00.000000Z\ta\t1.0\t7.0
                    2026-01-02T00:07:00.000000Z\ta\t1.0\t8.0
                    """;
            assertTieAtTheFrontierFoldsOntoTheRestoredRoot(
                    sql,
                    rows,
                    "('2026-01-02T00:07:00.000000Z', 'a', 1.0)",
                    rows + "2026-01-02T00:07:00.000000Z\ta\t1.0\t9.0\n"
            );
        });
    }

    @Test
    public void testAnEndOfTableRepairWithoutASpliceThatKeepsOneRowOfTheNewestRangeRestoresRetainedRows() throws Exception {
        // No chained boundary is allowed, so the rebuild declines its splice and truncates the
        // timeline at R, which drops the root at 00:59 and keeps the one at 23:55, and seals the
        // kept row's root on top: every restart restores from it over the day the base lost.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, 0);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitTwoBatchesOverTheLostDay(job, 59);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                final long generationBefore = publishedCheckpointGeneration(instance);
                final long headMissRepairsBefore = countHeadMissRepairs(job);
                replaceBaseRange(ts("2026-01-01T00:57:00.000000Z"), ts("2026-01-01T01:00:00.000000Z"), 5.0);
                drainWalQueue();
                driveRefreshToQuiescence(job);
                Assert.assertEquals("the head-miss rebuild repairs the departure", headMissRepairsBefore + 1, countHeadMissRepairs(job));
                assertNoHeadMissSpliceFrontierStat(job);
                Assert.assertEquals("the truncate and the seal", generationBefore + 2, publishedCheckpointGeneration(instance));
                assertCheckpointLadder(instance, false, """
                        2025-12-31T23:55:00.000000Z -> 1
                        2026-01-01T00:57:00.000000Z -> 59
                        """);
                assertNoRepairMarker(instance);
            }
            final String rows = headMissDepartureRows(true, 56) + "2026-01-01T00:57:00.000000Z\ta\t5.0\t6.0\n";
            assertDepartedViewRows(rows);
            assertNoRefreshFaults("lv");
            for (int restart = 0; restart < 2; restart++) {
                restartLiveViewsAndRecover();
                assertDepartedViewRestored(rows);
            }
        });
    }

    @Test
    public void testARepairConvergingBelowTheFrontierThatEmitsNoRowKeepsItsTimeline() throws Exception {
        // A row with no other row within a minute above it leaves, so the rebuild's influence
        // converges a minute above it, far below the frontier: it keeps the primary runtime, which
        // stands at the frontier, splices the empty interval, and emits nothing. The newest root
        // sits at the frontier, so the seal only re-stamps it, and nothing is truncated or retired.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitTheDayTheBaseLoses(job);
                commitAndRefresh(job, minuteRowsOfA(0, 30, "1.0") + ", ('2026-01-01T00:40:00.000000Z', 'a', 1.0)");
                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                commitAndRefresh(job, minuteRowsOfA(50, 59, "1.0"));
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                final String ladder = """
                        2025-12-31T23:55:00.000000Z -> 1
                        2026-01-01T00:59:00.000000Z -> 43
                        """;
                assertCheckpointLadder(instance, false, ladder);
                final long resetsBefore = instance.getCheckpointTimelineResets();
                final long headMissRepairsBefore = countHeadMissRepairs(job);
                final LogCapture capture = new LogCapture();
                capture.start();
                try {
                    replaceBaseRange(ts("2026-01-01T00:40:00.000000Z"), ts("2026-01-01T00:40:00.000001Z"), Double.NaN);
                    drainWalQueue();
                    driveRefreshToQuiescence(job);
                    capture.drain();
                    capture.assertLogged("runtimeStatePreserved=true");
                    capture.assertLogged("rowsEmitted=0]");
                } finally {
                    capture.stop();
                }
                Assert.assertEquals("the head-miss rebuild repairs the departure", headMissRepairsBefore + 1, countHeadMissRepairs(job));
                assertNoHeadMissSpliceFrontierStat(job);
                Assert.assertEquals(resetsBefore, instance.getCheckpointTimelineResets());
                assertCheckpointLadder(instance, false, """
                        2025-12-31T23:55:00.000000Z -> 1
                        2026-01-01T00:59:00.000000Z -> 42
                        """);
                Assert.assertEquals(ts("2026-01-01T00:59:00.000000Z"), instance.getHeadCheckpointMaxTs());
                assertNoRepairMarker(instance);
            }
            final StringSink rows = new StringSink();
            rows.put(headMissDepartureRows(true, 30));
            for (int minute = 50; minute <= 59; minute++) {
                rows.put("2026-01-01T00:").put(minute).put(":00.000000Z\ta\t1.0\t").put(minute == 50 ? "1.0" : "2.0").put('\n');
            }
            assertDepartedViewRows(rows.toString());
            assertNoRefreshFaults("lv");
            for (int restart = 0; restart < 2; restart++) {
                restartLiveViewsAndRecover();
                assertDepartedViewRestored(rows.toString());
            }
        });
    }

    @Test
    public void testADeletedNewestRowWithTheChainDeclinedRestoresRetainedRowsUnderARangeFrame() throws Exception {
        // 23:55 sits in the ten-minute frame of 00:01, and not in that of the forward row 00:06.
        assertADeletedNewestRowWithTheChainDeclinedRestoresRetainedRows(
                "sum(x) OVER (PARTITION BY sym ORDER BY ts RANGE BETWEEN '10' MINUTE PRECEDING AND CURRENT ROW)",
                "6.0"
        );
    }

    @Test
    public void testADeletedNewestRowWithTheChainDeclinedRestoresRetainedRowsUnderARowsFrame() throws Exception {
        assertADeletedNewestRowWithTheChainDeclinedRestoresRetainedRows(
                "sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)",
                "6.5"
        );
    }

    @Test
    public void testDeletedNewestRootsPastTheDefaultChainBudgetKeepTheTimeline() throws Exception {
        // 257 roots above the anchor: one more than the default chain budget.
        assertDeletedNewestRootsKeepTheTimeline(257, true);
    }

    @Test
    public void testDeletedNewestRootsWithinTheDefaultChainBudgetKeepTheTimeline() throws Exception {
        // 256 roots above the anchor: the default chain budget, which the splice takes.
        assertDeletedNewestRootsKeepTheTimeline(256, false);
    }

    @Test
    public void testDepartedNewestBucketRestoresRetainedRowsUnderARangeFrame() throws Exception {
        assertDepartedNewestBucketRestoresRetainedRows(
                "sum(x) OVER (PARTITION BY sym ORDER BY ts RANGE BETWEEN '10' MINUTE PRECEDING AND CURRENT ROW)",
                "9.0"
        );
    }

    @Test
    public void testDepartedNewestBucketRestoresRetainedRowsUnderARowsFrame() throws Exception {
        assertDepartedNewestBucketRestoresRetainedRows(
                "sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)",
                "8.5"
        );
    }

    @Test
    public void testPureDeleteReplaceRangeRemovesEmittedRows() throws Exception {
        // A row-less replace commit is a pure delete: it carries no data rows at
        // all (its min timestamp reads Long.MAX_VALUE), so only the range
        // metadata reveals that emitted rows are gone. The view must drop the
        // deleted band's derived rows and renumber, not keep them as ghosts.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x LONG, g SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " +
                    "SELECT ts, x, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('2026-01-01T00:00:01.000000Z', 1), " +
                        "('2026-01-01T00:00:02.000000Z', 2), " +
                        "('2026-01-01T00:00:03.000000Z', 3), " +
                        "('2026-01-01T00:00:04.000000Z', 4)");
                drainWalQueue();
                refreshCycle(job);
                assertQuery("SELECT count() FROM lv")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("count\n4\n");

                final TableToken baseToken = engine.verifyTableName("base");
                try (WalWriter walWriter = engine.getWalWriter(baseToken)) {
                    walWriter.commitWithParams(
                            ts("2026-01-01T00:00:02.000000Z"),
                            ts("2026-01-01T00:00:04.000000Z"),
                            WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE
                    );
                }
                drainWalQueue();
                driveRefreshToQuiescence(job);
                assertLiveViewValid();
            }

            assertQuery("SELECT ts, x, rn FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tx\trn\n" +
                            "2026-01-01T00:00:01.000000Z\t1\t1\n" +
                            "2026-01-01T00:00:04.000000Z\t4\t2\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testPureDeleteAboveHeadCheckpointRemovesGhostRows() throws Exception {
        // The head-hit arm of the ghost-row shape. The view carries a head
        // checkpoint at maxTs 00:03; a pure-delete replace over [00:04, 00:06)
        // erases every base row ABOVE that head, so its range low (00:04) sits
        // strictly above headMaxTs and the replay takes the head-hit branch:
        // state rolls back to the head and the base is re-read from 00:04 up,
        // which now yields nothing. The truncating REPLACE_RANGE must be emitted
        // even with zero rows to replay - skipping it (the old "commit only when
        // rows were appended" gate) strands the pre-delete output for 00:04 /
        // 00:05 on disk as ghosts while the watermark advances past the commit
        // that removed their base rows, so the view permanently over-reports.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x LONG, g SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " +
                    "SELECT ts, x, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Batch 1: the first commit always writes a head checkpoint, here at
                // maxTs 00:03.
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('2026-01-01T00:00:01.000000Z', 1), " +
                        "('2026-01-01T00:00:02.000000Z', 2), " +
                        "('2026-01-01T00:00:03.000000Z', 3)");
                drainWalQueue();
                refreshCycle(job);

                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                final long headLvSeqTxn = instance.getHeadCheckpointLvSeqTxn();
                Assert.assertNotEquals("batch 1 must write a head checkpoint", Numbers.LONG_NULL, headLvSeqTxn);
                Assert.assertEquals(ts("2026-01-01T00:00:03.000000Z"), instance.getHeadCheckpointMaxTs());

                // Batch 2: a forward append above the head. The checkpoint cadence
                // (1M rows / 5 min) does not fire, so the head stays at 00:03 while
                // the frontier moves to 00:05 - the head-hit precondition.
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('2026-01-01T00:00:04.000000Z', 4), " +
                        "('2026-01-01T00:00:05.000000Z', 5)");
                drainWalQueue();
                refreshCycle(job);
                assertQuery("SELECT count() FROM lv")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("count\n5\n");
                Assert.assertEquals(
                        "head must still point at batch 1 (cadence did not fire)",
                        headLvSeqTxn,
                        instance.getHeadCheckpointLvSeqTxn()
                );
                Assert.assertEquals(ts("2026-01-01T00:00:05.000000Z"), instance.getLatestSeenTs());

                // Pure delete of [00:04, 00:06): its range low is above headMaxTs
                // (00:03), so the trigger is head-hit eligible and the replay window
                // (00:03, +inf) recomputes to nothing.
                final TableToken baseToken = engine.verifyTableName("base");
                try (WalWriter walWriter = engine.getWalWriter(baseToken)) {
                    walWriter.commitWithParams(
                            ts("2026-01-01T00:00:04.000000Z"),
                            ts("2026-01-01T00:00:06.000000Z"),
                            WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE
                    );
                }
                drainWalQueue();
                driveRefreshToQuiescence(job);
                assertLiveViewValid();
                // The resume emitted nothing over the head root, which its splice kept at the
                // view's new frontier, so the head goes back on that root at the delete's commit.
                Assert.assertEquals(3, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(ts("2026-01-01T00:00:03.000000Z"), instance.getHeadCheckpointMaxTs());
            }

            assertQuery("SELECT ts, x, rn FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tx\trn\n" +
                            "2026-01-01T00:00:01.000000Z\t1\t1\n" +
                            "2026-01-01T00:00:02.000000Z\t2\t2\n" +
                            "2026-01-01T00:00:03.000000Z\t3\t3\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRepeatedPureDeletesAboveTheHeadMoveTheHead() throws Exception {
        // The pure delete of the case above, then the same row-less REPLACE_RANGE five more times.
        // Each finds nothing left to delete, yet its resume splices at its commit and emits nothing
        // over the head root at the frontier: the head goes back on that root at each delete's
        // commit, which releases the base WAL below it, and each re-stamp sweeps what the splice
        // before it superseded.
        configureWalPurge();
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x LONG, g SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " +
                    "SELECT ts, x, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('2026-01-01T00:00:01.000000Z', 1), " +
                        "('2026-01-01T00:00:02.000000Z', 2), " +
                        "('2026-01-01T00:00:03.000000Z', 3)");
                drainWalQueue();
                refreshCycle(job);
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('2026-01-01T00:00:04.000000Z', 4), " +
                        "('2026-01-01T00:00:05.000000Z', 5)");
                drainWalQueue();
                refreshCycle(job);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                Assert.assertEquals(ts("2026-01-01T00:00:03.000000Z"), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(1, instance.getHeadCheckpointBaseSeqTxn());

                final TableToken baseToken = engine.verifyTableName("base");
                int files = -1;
                long entries = -1;
                for (int seqTxn = 3; seqTxn <= 8; seqTxn++) {
                    try (WalWriter walWriter = engine.getWalWriter(baseToken)) {
                        walWriter.commitWithParams(
                                ts("2026-01-01T00:00:04.000000Z"),
                                ts("2026-01-01T00:00:06.000000Z"),
                                WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE
                        );
                    }
                    drainWalQueue();
                    driveRefreshToQuiescence(job);
                    Assert.assertEquals(seqTxn, instance.getHeadCheckpointBaseSeqTxn());
                    Assert.assertEquals(ts("2026-01-01T00:00:03.000000Z"), instance.getHeadCheckpointMaxTs());
                    Assert.assertEquals(seqTxn - 2, job.getStatsForTest().get(LiveViewRefreshStats.SPLICE_ZERO_ROW_KEEP_COUNT));
                    if (seqTxn == 4) {
                        files = checkpointFileCount(instance);
                        entries = catalogueSize(instance);
                    } else if (seqTxn > 4) {
                        Assert.assertEquals(files, checkpointFileCount(instance));
                        Assert.assertEquals(entries, catalogueSize(instance));
                    }
                }
                Assert.assertEquals("the purge must keep only the segments above the floor", 1, purgeBaseWalAndCountSegments());
                assertCatalogueMatchesDisk(instance);
                assertLiveViewValid();
            }

            assertQuery("SELECT ts, x, rn FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tx\trn\n" +
                            "2026-01-01T00:00:01.000000Z\t1\t1\n" +
                            "2026-01-01T00:00:02.000000Z\t2\t2\n" +
                            "2026-01-01T00:00:03.000000Z\t3\t3\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testReplaceRangeAboveFrontierAppendsForward() throws Exception {
        // A replace band strictly above every existing row deletes nothing the
        // view has seen: the commit's rows flow through the plain forward-append
        // path and the view converges without a replay.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x LONG, g SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " +
                    "SELECT ts, x, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('2026-01-01T00:00:01.000000Z', 1), " +
                        "('2026-01-01T00:00:02.000000Z', 2), " +
                        "('2026-01-01T00:00:03.000000Z', 3)");
                drainWalQueue();
                refreshCycle(job);

                final TableToken baseToken = engine.verifyTableName("base");
                try (WalWriter walWriter = engine.getWalWriter(baseToken)) {
                    appendRow(walWriter, ts("2026-01-01T00:00:05.000000Z"), 5);
                    appendRow(walWriter, ts("2026-01-01T00:00:06.000000Z"), 6);
                    walWriter.commitWithParams(
                            ts("2026-01-01T00:00:04.000000Z"),
                            ts("2026-01-01T00:00:10.000000Z"),
                            WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE
                    );
                }
                drainWalQueue();
                driveRefreshToQuiescence(job);
                assertLiveViewValid();
            }

            assertQuery("SELECT ts, x, rn FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tx\trn\n" +
                            "2026-01-01T00:00:01.000000Z\t1\t1\n" +
                            "2026-01-01T00:00:02.000000Z\t2\t2\n" +
                            "2026-01-01T00:00:03.000000Z\t3\t3\n" +
                            "2026-01-01T00:00:05.000000Z\t5\t4\n" +
                            "2026-01-01T00:00:06.000000Z\t6\t5\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testReplaceRangeCoveringPendingRowsConverges() throws Exception {
        // The view lags the base: two commits sit in the base WAL un-refreshed
        // when a replace commit covers them (the base apply may even skip them
        // entirely via calculateSkipTransactionCount, so their rows never
        // materialize in the base table). The drain first emits the pending
        // rows - raising the frontier into the replace band - and must then
        // route the replace commit to the replay, erasing the just-emitted
        // ghosts. The final view equals the recompute over the applied base.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x LONG, g SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " +
                    "SELECT ts, x, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('2026-01-01T00:00:01.000000Z', 1), " +
                        "('2026-01-01T00:00:02.000000Z', 2)");
                drainWalQueue();
                refreshCycle(job);
                assertQuery("SELECT count() FROM lv")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("count\n2\n");

                // Pending: committed to the base WAL, never refreshed into the view.
                execute("INSERT INTO base (ts, x) VALUES ('2026-01-01T00:00:03.000000Z', 3)");
                execute("INSERT INTO base (ts, x) VALUES ('2026-01-01T00:00:04.000000Z', 4)");

                final TableToken baseToken = engine.verifyTableName("base");
                try (WalWriter walWriter = engine.getWalWriter(baseToken)) {
                    appendRow(walWriter, ts("2026-01-01T00:00:05.000000Z"), 5);
                    walWriter.commitWithParams(
                            ts("2026-01-01T00:00:03.000000Z"),
                            ts("2026-01-01T00:00:06.000000Z"),
                            WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE
                    );
                }
                drainWalQueue();
                assertQuery("SELECT count() FROM base")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("count\n3\n");

                driveRefreshToQuiescence(job);
                assertLiveViewValid();
            }

            assertQuery("SELECT ts, x, rn FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tx\trn\n" +
                            "2026-01-01T00:00:01.000000Z\t1\t1\n" +
                            "2026-01-01T00:00:02.000000Z\t2\t2\n" +
                            "2026-01-01T00:00:05.000000Z\t5\t3\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testReplaceRangeDeletingEmittedRowsBelowItsNewRowsConverges() throws Exception {
        // The ghost-row shape: the view has emitted rows at 00:02 and 00:03; a
        // replace commit covers [00:02, 00:05) but its only inserted row sits at
        // 00:04, ABOVE the frontier (00:03). The commit deletes the two emitted
        // rows from the base while presenting nothing out of order, so a drain
        // that ignores the range would forward-append the 00:04 row and keep the
        // deleted rows' derived output forever. The view must instead replay onto
        // the post-replace base: rows at 00:01 and 00:04, renumbered 1..2.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x LONG, g SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " +
                    "SELECT ts, x, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('2026-01-01T00:00:01.000000Z', 1), " +
                        "('2026-01-01T00:00:02.000000Z', 2), " +
                        "('2026-01-01T00:00:03.000000Z', 3)");
                drainWalQueue();
                refreshCycle(job);
                assertQuery("SELECT count() FROM lv")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("count\n3\n");

                final TableToken baseToken = engine.verifyTableName("base");
                try (WalWriter walWriter = engine.getWalWriter(baseToken)) {
                    appendRow(walWriter, ts("2026-01-01T00:00:04.000000Z"), 4);
                    walWriter.commitWithParams(
                            ts("2026-01-01T00:00:02.000000Z"),
                            ts("2026-01-01T00:00:05.000000Z"),
                            WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE
                    );
                }
                drainWalQueue();
                assertQuery("SELECT ts, x FROM base")
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns("ts\tx\n" +
                                "2026-01-01T00:00:01.000000Z\t1\n" +
                                "2026-01-01T00:00:04.000000Z\t4\n");

                driveRefreshToQuiescence(job);
                assertLiveViewValid();
            }

            TestUtils.assertSqlCursors(
                    engine,
                    sqlExecutionContext,
                    "(SELECT ts, x, row_number() OVER () AS rn FROM base) ORDER BY 1",
                    "(lv) ORDER BY 1",
                    LOG,
                    true
            );
            assertNoRefreshFaults("lv");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testReplaceRangeOnDedupBaseConverges() throws Exception {
        // Same ghost shape over a DEDUP base, which routes refreshes through the
        // coupled applied-reader path (or its provably-clean raw-WAL fast path).
        // Whichever route the cycle takes must see the replace commit's deletion
        // through the range metadata and converge onto the post-replace base.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) " +
                    "TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " +
                    "SELECT ts, sym, x, sum(x) OVER (PARTITION BY sym ORDER BY ts " +
                    "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS s FROM base");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-01-01T00:00:01.000000Z', 'A', 1), " +
                        "('2026-01-01T00:00:02.000000Z', 'A', 2), " +
                        "('2026-01-01T00:00:03.000000Z', 'A', 3)");
                drainWalQueue();
                refreshCycle(job);
                assertQuery("SELECT count() FROM lv")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("count\n3\n");

                final TableToken baseToken = engine.verifyTableName("base");
                try (WalWriter walWriter = engine.getWalWriter(baseToken)) {
                    TableWriter.Row row = walWriter.newRow(ts("2026-01-01T00:00:04.000000Z"));
                    row.putSym(1, "A");
                    row.putLong(2, 10);
                    row.append();
                    walWriter.commitWithParams(
                            ts("2026-01-01T00:00:02.000000Z"),
                            ts("2026-01-01T00:00:05.000000Z"),
                            WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE
                    );
                }
                drainWalQueue();
                driveRefreshToQuiescence(job);
                assertLiveViewValid();
            }

            // The bounded frame restarts over the post-replace base: the deleted
            // rows at 00:02 / 00:03 no longer contribute to the 00:04 row's sum.
            assertQuery("SELECT ts, sym, x, s FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tsym\tx\ts\n" +
                            "2026-01-01T00:00:01.000000Z\tA\t1\t1.0\n" +
                            "2026-01-01T00:00:04.000000Z\tA\t10\t11.0\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testReplaceRangeStartingExactlyAtFrontierDeletesFrontierRow() throws Exception {
        // Boundary case for the non-strict frontier comparison: the replace band
        // starts EXACTLY at the frontier timestamp, so it deletes the frontier
        // row itself while its inserted row sits above. A strict range-low check
        // (rangeLo < latestSeenTs) would miss this commit and keep the deleted
        // frontier row as a ghost.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x LONG, g SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " +
                    "SELECT ts, x, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('2026-01-01T00:00:01.000000Z', 1), " +
                        "('2026-01-01T00:00:02.000000Z', 2), " +
                        "('2026-01-01T00:00:03.000000Z', 3)");
                drainWalQueue();
                refreshCycle(job);

                final TableToken baseToken = engine.verifyTableName("base");
                try (WalWriter walWriter = engine.getWalWriter(baseToken)) {
                    appendRow(walWriter, ts("2026-01-01T00:00:04.000000Z"), 4);
                    walWriter.commitWithParams(
                            ts("2026-01-01T00:00:03.000000Z"),
                            ts("2026-01-01T00:00:05.000000Z"),
                            WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE
                    );
                }
                drainWalQueue();
                driveRefreshToQuiescence(job);
                assertLiveViewValid();
            }

            assertQuery("SELECT ts, x, rn FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tx\trn\n" +
                            "2026-01-01T00:00:01.000000Z\t1\t1\n" +
                            "2026-01-01T00:00:02.000000Z\t2\t2\n" +
                            "2026-01-01T00:00:04.000000Z\t4\t3\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    private void assertNewestRangeKeepingOneRowRestores(double keptX, String keptS) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitTwoBatchesOverTheLostDay(job, 59);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                assertCheckpointLadder(instance, false, """
                        2025-12-31T23:55:00.000000Z -> 1
                        2026-01-01T00:59:00.000000Z -> 61
                        """);
                final long generationBefore = publishedCheckpointGeneration(instance);
                final long headMissRepairsBefore = countHeadMissRepairs(job);
                replaceBaseRange(ts("2026-01-01T00:57:00.000000Z"), ts("2026-01-01T01:00:00.000000Z"), keptX);
                drainWalQueue();
                driveRefreshToQuiescence(job);
                Assert.assertEquals("the head-miss rebuild repairs the departure", headMissRepairsBefore + 1, countHeadMissRepairs(job));
                Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_FRONTIER_TRUNCATE_COUNT));
                Assert.assertEquals("the splice, the truncate and the seal", generationBefore + 3, publishedCheckpointGeneration(instance));
                assertCheckpointLadder(instance, false, """
                        2025-12-31T23:55:00.000000Z -> 1
                        2026-01-01T00:57:00.000000Z -> 59
                        """);
                assertNoRepairMarker(instance);
            }
            final String rows = headMissDepartureRows(true, 56) + "2026-01-01T00:57:00.000000Z\ta\t" + keptX + "\t" + keptS + "\n";
            assertDepartedViewRows(rows);
            assertNoRefreshFaults("lv");
            for (int restart = 0; restart < 2; restart++) {
                restartLiveViewsAndRecover();
                assertDepartedViewRestored(rows);
            }
        });
    }

    /**
     * The minute fixture over a base that lost nothing, under a view running {@code viewSql},
     * whose newest three minutes a REPLACE_RANGE reduces to one row with a new value. The
     * rebuild's output ends below the root at 00:59 it spliced, so the repair truncates at the new
     * frontier 00:57 and seals a root there. The view gives what a recompute gives, and each
     * restart restores from that root.
     */
    /**
     * The intact-base departure of the newest three minutes under a view created with
     * {@code createOptions}: the splice kept the root at 00:59 above the new frontier 00:56, so the
     * rebuild warms the frame from 00:55, truncates that root and seals 00:56, and every restart
     * restores from it.
     */
    private void assertTheNewestRangeDepartureSealsTheFrontier(String createOptions) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms " + createOptions + "START FROM BEGINNING AS " + HEAD_MISS_DEPARTURE_VIEW_SQL);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitAndRefresh(job, "('2025-12-31T23:55:00.000000Z', 'a', 0.5)");
                commitAndRefresh(job, minuteRowsOfA(0, 30, "1.0"));
                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                commitAndRefresh(job, minuteRowsOfA(31, 59, "1.0"));
                assertZeroRowDepartureSealsTheFrontier(
                        job,
                        () -> replaceBaseRange(ts("2026-01-01T00:57:00.000000Z"), ts("2026-01-01T01:00:00.000000Z"), Double.NaN),
                        "2026-01-01T00:56:00.000000Z",
                        FRONTIER_STATE_WARM_UP,
                        1,
                        3,
                        3,
                        false,
                        """
                                2025-12-31T23:55:00.000000Z -> 1
                                2026-01-01T00:56:00.000000Z -> 58
                                """
                );
            }
            assertRecomputedRowsThenTheTieAtTheFrontierRestore(headMissDepartureRows(true, 56));
        });
    }

    private void assertNewestRangeKeepingOneRowMatchesRecompute(String viewSql, String createOptions) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms " + createOptions + "START FROM BEGINNING AS " + viewSql);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitAndRefresh(job, "('2025-12-31T23:55:00.000000Z', 'a', 0.5)");
                commitAndRefresh(job, minuteRowsOfA(0, 30, "1.0"));
                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                commitAndRefresh(job, minuteRowsOfA(31, 59, "1.0"));
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                final long headMissRepairsBefore = countHeadMissRepairs(job);
                replaceBaseRange(ts("2026-01-01T00:57:00.000000Z"), ts("2026-01-01T01:00:00.000000Z"), 5.0);
                drainWalQueue();
                driveRefreshToQuiescence(job);
                Assert.assertEquals("the head-miss rebuild repairs the departure", headMissRepairsBefore + 1, countHeadMissRepairs(job));
                Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_FRONTIER_TRUNCATE_COUNT));
                assertCheckpointLadder(instance, false, """
                        2025-12-31T23:55:00.000000Z -> 1
                        2026-01-01T00:57:00.000000Z -> 59
                        """);
                assertNoRepairMarker(instance);
            }
            assertMatchesRecompute(viewSql);
            assertNoRefreshFaults("lv");
            for (int restart = 0; restart < 2; restart++) {
                restartLiveViewsAndRecover();
                assertRestoredFromTimeline("lv");
                assertMatchesRecompute(viewSql);
                assertNoRefreshFaults("lv");
            }
        });
    }

    /**
     * Over a base that lost nothing, after a zero-row departure sealed the new frontier 00:56: the
     * view holds {@code rows}, which a recompute gives, every restart restores from the root at
     * the frontier, and a row of {@code a} committed at exactly the frontier after the first
     * restart folds over the restored root, whose frame holds 00:55, 00:56 and the row itself.
     */
    private void assertRecomputedRowsThenTheTieAtTheFrontierRestore(String rows) throws Exception {
        assertDepartedViewRows(rows);
        assertMatchesRecompute(HEAD_MISS_DEPARTURE_VIEW_SQL);
        assertTieAtTheFrontierFoldsOntoTheRestoredRoot(
                rows,
                "('2026-01-01T00:56:00.000000Z', 'a', 1.0)",
                rows + "2026-01-01T00:56:00.000000Z\ta\t1.0\t3.0\n"
        );
    }

    /**
     * After the departure of {@link #departTheRowsAboveTheFrameAcrossMidnight}: the view keeps
     * the 2025-12-31 row, every restart restores from the root at the new frontier
     * 01-02T00:00:50, and rows committed at exactly the frontier and above it after the first
     * restart read the frontier's whole frame from 01-01T23:59:50 on.
     */
    private void assertTheFrameAcrossMidnightRestores() throws Exception {
        final String rows = headMissDepartureRows(true, 30) + """
                2026-01-01T23:59:50.000000Z\ta\t1.0\t1.0
                2026-01-02T00:00:10.000000Z\ta\t1.0\t2.0
                2026-01-02T00:00:50.000000Z\ta\t1.0\t3.0
                """;
        assertDepartedViewRows(rows);
        assertTieAtTheFrontierFoldsOntoTheRestoredRoot(
                rows,
                "('2026-01-02T00:00:50.000000Z', 'a', 1.0), ('2026-01-02T00:01:05.000000Z', 'a', 1.0)",
                rows + """
                        2026-01-02T00:00:50.000000Z\ta\t1.0\t4.0
                        2026-01-02T00:01:05.000000Z\ta\t1.0\t4.0
                        """
        );
    }

    /**
     * After a departure of every 01-02 row from the daily anchored view: every restart restores
     * from the root at the new frontier 01-01T00:59, and a row at exactly the frontier after the
     * first restart, then one 30 seconds above it, continue 01-01's segment.
     */
    private void assertTheDailyAnchoredFrontierRestores() throws Exception {
        final String sql = "SELECT ts, sym, x, s FROM lv WHERE ts >= '2026-01-01T00:58:00.000000Z'";
        final String rows = """
                ts\tsym\tx\ts
                2026-01-01T00:58:00.000000Z\ta\t1.0\t59.0
                2026-01-01T00:59:00.000000Z\ta\t1.0\t60.0
                """;
        assertTieAtTheFrontierFoldsOntoTheRestoredRoot(
                sql,
                rows,
                "('2026-01-01T00:59:00.000000Z', 'a', 1.0), ('2026-01-01T00:59:30.000000Z', 'a', 1.0)",
                rows + """
                        2026-01-01T00:59:00.000000Z\ta\t1.0\t61.0
                        2026-01-01T00:59:30.000000Z\ta\t1.0\t62.0
                        """
        );
    }

    /**
     * One commit over a view starting at {@code startFrom} puts rows at the epoch and from 00:02
     * to 00:04 and seals the only root at 00:04, and the departure of the rows from 00:02 up
     * leaves the epoch row the frontier: the rebuild warms the frame from {@code floorTs}, the
     * frontier truncate drops the only root, and the seal opens a fresh history at the epoch.
     * A row at the epoch after the restart reads the epoch row and itself.
     */
    private void assertTheDepartureDownToTheEpochOpensAFreshHistory(String startFrom, String floorTs) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM " + startFrom + " AS " + HEAD_MISS_DEPARTURE_VIEW_SQL);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitAndRefresh(
                        job,
                        "('1970-01-01T00:00:00.000000Z', 'a', 1.0), ('1970-01-01T00:02:00.000000Z', 'a', 1.0), "
                                + "('1970-01-01T00:03:00.000000Z', 'a', 1.0), ('1970-01-01T00:04:00.000000Z', 'a', 1.0)"
                );
                final LogCapture capture = new LogCapture();
                capture.start();
                try {
                    assertZeroRowDepartureSealsTheFrontier(
                            job,
                            () -> replaceBaseRange(ts("1970-01-01T00:02:00.000000Z"), ts("1970-01-01T00:05:00.000000Z"), Double.NaN),
                            "1970-01-01T00:00:00.000000Z",
                            FRONTIER_STATE_WARM_UP,
                            1,
                            1,
                            0,
                            true,
                            "1970-01-01T00:00:00.000000Z -> 1\n"
                    );
                    capture.drain();
                    capture.assertLogged("live view O3 head miss derived the frontier state [view=lv, frontier=1970-01-01T00:00:00.000000Z, "
                            + "floor=" + floorTs + ", openBelow=false, route=warm-up, rootMaxTs=, rows=1]");
                } finally {
                    capture.stop();
                }
            }
            final String rows = "ts\tsym\tx\ts\n1970-01-01T00:00:00.000000Z\ta\t1.0\t1.0\n";
            assertTieAtTheFrontierFoldsOntoTheRestoredRoot(
                    rows,
                    "('1970-01-01T00:00:00.000000Z', 'a', 1.0)",
                    rows + "1970-01-01T00:00:00.000000Z\ta\t1.0\t2.0\n"
            );
        });
    }

    /**
     * Every row of 01-02 departs, so the new frontier 01-01T00:59 sits in the segment below the
     * replay's: the replay read nothing of 01-01, and the frontier's floor is that day's start.
     * The root at 01-01T00:30 counts every row of the view there, so the rebuild restores it and
     * folds the 29 rows above it, rather than the whole hour from the day's start.
     */
    private void assertTheEmptiedAnchorSegmentRestoresTheRootBelowIt() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createDailyAnchoredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitTheDayTheBaseLoses(job);
                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                commitAndRefresh(job, minuteRowsOfA(0, 30, "1.0"));
                commitAndRefresh(job, minuteRowsOfA(31, 59, "1.0") + ", " + minuteRowsOfAOnTheSecond(0, 10));
                assertZeroRowDepartureSealsTheFrontier(
                        job,
                        () -> replaceBaseRange(ts("2026-01-02T00:00:00.000000Z"), ts("2026-01-02T00:11:00.000000Z"), Double.NaN),
                        "2026-01-01T00:59:00.000000Z",
                        FRONTIER_STATE_RESTORE,
                        0,
                        29,
                        2,
                        false,
                        """
                                2025-12-31T23:55:00.000000Z -> 1
                                2026-01-01T00:30:00.000000Z -> 32
                                2026-01-01T00:59:00.000000Z -> 61
                                """
                );
            }
            assertTheDailyAnchoredFrontierRestores();
        });
    }

    // As assertTheEmptiedAnchorSegmentRestoresTheRootBelowIt with no root on 01-01: the rebuild
    // folds the frontier's segment from its start.
    private void assertTheEmptiedAnchorSegmentWarmsThePreviousSegment() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createDailyAnchoredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitTheDayTheBaseLoses(job);
                commitAndRefresh(job, minuteRowsOfA(0, 59, "1.0") + ", " + minuteRowsOfAOnTheSecond(0, 10));
                assertZeroRowDepartureSealsTheFrontier(
                        job,
                        () -> replaceBaseRange(ts("2026-01-02T00:00:00.000000Z"), ts("2026-01-02T00:11:00.000000Z"), Double.NaN),
                        "2026-01-01T00:59:00.000000Z",
                        FRONTIER_STATE_WARM_UP,
                        0,
                        60,
                        2,
                        false,
                        """
                                2025-12-31T23:55:00.000000Z -> 1
                                2026-01-01T00:59:00.000000Z -> 61
                                """
                );
            }
            assertTheDailyAnchoredFrontierRestores();
        });
    }

    /**
     * After a departure that left the fall-back fixture's new frontier at 00:45Z, in the half
     * hour the clocks go back over: every restart restores from the root there, with the view
     * holding {@code dayRows} below 10-25. a's run of that half hour holds 00:40Z and 00:45Z, so
     * a row at the frontier reads three; b's row at 01:10Z, in the half hour after it, carries the
     * anchor value of b's 00:20Z row again and continues its run.
     */
    private void assertTheFallBackOpenRunRestores(String dayRows) throws Exception {
        final String rows = "ts\tsym\tx\ts\n" + dayRows + """
                2025-10-26T00:20:00.000000Z\ta\t1.0\t53.0
                2025-10-26T00:20:00.000000Z\tb\t1.0\t1.0
                2025-10-26T00:25:00.000000Z\ta\t1.0\t54.0
                2025-10-26T00:40:00.000000Z\ta\t1.0\t1.0
                2025-10-26T00:45:00.000000Z\ta\t1.0\t2.0
                """;
        assertTieAtTheFrontierFoldsOntoTheRestoredRoot(
                FALL_BACK_TAIL_SQL,
                rows,
                "('2025-10-26T00:45:00.000000Z', 'a', 1.0), ('2025-10-26T01:10:00.000000Z', 'b', 1.0)",
                rows + """
                        2025-10-26T00:45:00.000000Z\ta\t1.0\t3.0
                        2025-10-26T01:10:00.000000Z\tb\t1.0\t2.0
                        """
        );
    }

    /**
     * The restart after a departure that retired the timeline over a base that lost nothing:
     * it rebuilds the view from the applied base, which gives {@code rows}, what a recompute
     * gives.
     */
    private void assertRebuiltOnRestartAsARecomputeDoes(String rows) throws Exception {
        assertDepartedViewRows(rows);
        restartLiveViewsAndRecover();
        assertRebuiltFromAppliedBase("lv");
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
        assertDepartedViewRows(rows);
        assertMatchesRecompute(HEAD_MISS_DEPARTURE_VIEW_SQL);
    }

    /**
     * The head-miss rebuild of the departure retired the view's timeline, which took its repair
     * marker with it, and left no head.
     */
    private void assertTheDepartureRetiredTheTimeline(LiveViewInstance instance, long resetsBefore) {
        Assert.assertEquals("the repair retires the timeline", resetsBefore + 1, instance.getCheckpointTimelineResets());
        Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
        try (Path dir = checkpointsDir(instance); Path timelinePath = new Path()) {
            LiveViewCheckpointLayout.timelinePath(timelinePath, dir);
            Assert.assertFalse("the retired timeline is gone", configuration.getFilesFacade().exists(timelinePath.$()));
            Assert.assertFalse("the retire takes the marker with it", LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir));
        }
    }

    private void assertMatchesRecompute(String viewSql) throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + viewSql + ") ORDER BY ts, sym",
                "(lv) ORDER BY ts, sym",
                LOG,
                true
        );
    }

    private static void assertNoHeadMissSpliceFrontierStat(LiveViewRefreshJob job) {
        Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_FRONTIER_TRUNCATE_COUNT));
        Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_ZERO_ROW_KEEP_COUNT));
        Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_ZERO_ROW_RETIRE_COUNT));
    }

    private void assertNoRepairMarker(LiveViewInstance instance) {
        try (Path dir = new Path().of(configuration.getDbRoot()).concat(instance.getLiveViewToken()).concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME)) {
            Assert.assertFalse("the repair must resolve its marker", LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir));
        }
    }

    /**
     * A restart, rows the filter rejects from {@code 01:0firstMinute} on, one past the cadence
     * each, from base commit {@code firstSeqTxn} on, and another restart. Each restart restores
     * from the root at the view's newest row and puts the head on it with no write time, so the
     * first cycle without rows publishes at once and the next ones on the cadence.
     */
    private void assertRejectedRowsBetweenTwoRestartsKeepTheTimeline(String rows, int firstMinute, int firstSeqTxn) throws Exception {
        restartLiveViewsAndRecover();
        assertDepartedViewRestored(rows);
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            commitRejectedRowsPastTheCadence(job, instance, firstMinute, firstMinute + 2, firstSeqTxn);
            Assert.assertEquals(0, instance.getCheckpointSealFailures());
            Assert.assertEquals(0, instance.getCheckpointTimelineResets());
            Assert.assertEquals(firstSeqTxn + 1, instance.getCheckpointTimelineWalPurgeFloor());
            Assert.assertEquals("the purge must keep only the segments above the floor", 1, purgeBaseWalAndCountSegments());
        }
        restartLiveViewsAndRecover();
        assertDepartedViewRestored(rows);
    }

    private static Path checkpointsDir(LiveViewInstance instance) {
        return new Path().of(configuration.getDbRoot()).concat(instance.getLiveViewToken()).concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }

    private void assertTimelineBase(long normalizedBaseSeqTxn) throws Exception {
        assertQuery("SELECT checkpoint_timeline_normalized_base_seqtxn FROM live_views() WHERE view_name = 'lv'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("checkpoint_timeline_normalized_base_seqtxn\n" + normalizedBaseSeqTxn + "\n");
    }

    private void commitAndRefresh(LiveViewRefreshJob job, String values) throws Exception {
        execute("INSERT INTO base VALUES " + values);
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    /**
     * Commits rows the filter rejects at 01:01 to 01:03, the first past the duration cadence, with
     * the writer failing every seal and publication: each cycle seals the unsealed rows above the
     * head at the frontier, or republishes a head at the newest row, and fails, and the third
     * failure in a row pauses the seal. The fault then clears.
     */
    private void pauseTheSealWithRejectedRows(LiveViewRefreshJob job, LiveViewInstance instance) throws Exception {
        final long headMaxTs = instance.getHeadCheckpointMaxTs();
        final long resetsBefore = instance.getCheckpointTimelineResets();
        job.setCheckpointTimelineTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH);
        setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
        for (int minute = 1; minute <= 3; minute++) {
            commitAndRefresh(job, "('2026-01-01T01:0" + minute + ":00.000000Z', 'a', -1.0)");
            Assert.assertEquals(minute, instance.getCheckpointSealFailures());
        }
        job.setCheckpointTimelineTestFailureStage(0);
        Assert.assertEquals("the third failure pauses", resetsBefore, instance.getCheckpointTimelineResets());
        Assert.assertEquals(headMaxTs, instance.getHeadCheckpointMaxTs());
        Assert.assertTrue(instance.isSealOnCooldown(currentMicros));
    }

    /**
     * One row the filter rejects a minute from {@code 01:0fromMinute} to {@code 01:0toMinute},
     * each committed past the duration cadence, which is base commit {@code firstSeqTxn} on: each
     * cycle publishes the head at its own commit.
     */
    private void commitRejectedRowsPastTheCadence(
            LiveViewRefreshJob job,
            LiveViewInstance instance,
            int fromMinute,
            int toMinute,
            int firstSeqTxn
    ) throws Exception {
        for (int minute = fromMinute; minute <= toMinute; minute++) {
            setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
            commitAndRefresh(job, "('2026-01-01T01:0" + minute + ":00.000000Z', 'a', -1.0)");
            Assert.assertEquals(firstSeqTxn + minute - fromMinute, instance.getHeadCheckpointBaseSeqTxn());
        }
    }

    /**
     * Under a fresh refresh job, two rows of {@code a} committed in order above the root at the
     * new frontier 00:57, each in a commit of its own: 00:57:30, inside the kept row's one-minute
     * frame, and 00:59:30, where the deleted rows were.
     */
    private void commitRowsAboveTheFrontierRoot() throws Exception {
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            commitAndRefresh(job, "('2026-01-01T00:57:30.000000Z', 'a', 1.0)");
            commitAndRefresh(job, "('2026-01-01T00:59:30.000000Z', 'a', 1.0)");
        }
    }

    /**
     * Over the day the base loses, a root at the row 00:56 that becomes the frontier, then two
     * rows the view's filter drops and the row 00:59 of the view, which a root of its own seals
     * when {@code isAboveSealed}.
     */
    private void commitRootAtTheFrontierThenRowsAboveIt(LiveViewRefreshJob job, boolean isAboveSealed) throws Exception {
        commitTwoBatchesOverTheLostDay(job, 56);
        if (isAboveSealed) {
            setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
        }
        commitAndRefresh(
                job,
                "('2026-01-01T00:57:00.000000Z', 'a', -1.0), ('2026-01-01T00:57:30.000000Z', 'a', -1.0), ('2026-01-01T00:59:00.000000Z', 'a', 1.0)"
        );
    }

    /**
     * A root at the row 00:56 that becomes the frontier, over a base that lost nothing, then two
     * rows the view's filter drops and the row 00:59 of the view, which no root seals.
     */
    private void commitRootAtTheFrontierThenRowsAboveItOverAnIntactBase(LiveViewRefreshJob job) throws Exception {
        commitAndRefresh(job, "('2025-12-31T23:55:00.000000Z', 'a', 0.5)");
        commitAndRefresh(job, minuteRowsOfA(0, 30, "1.0"));
        setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
        commitAndRefresh(job, minuteRowsOfA(31, 56, "1.0"));
        commitAndRefresh(
                job,
                "('2026-01-01T00:57:00.000000Z', 'a', -1.0), ('2026-01-01T00:57:30.000000Z', 'a', -1.0), ('2026-01-01T00:59:00.000000Z', 'a', 1.0)"
        );
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        assertCheckpointLadder(instance, false, """
                2025-12-31T23:55:00.000000Z -> 1
                2026-01-01T00:56:00.000000Z -> 58
                """);
    }

    // The one row of a day the base then drops, which the view walks past and keeps: a rebuild
    // from the applied base would lose it.
    private void commitTheDayTheBaseLoses(LiveViewRefreshJob job) throws Exception {
        commitAndRefresh(job, "('2025-12-31T23:55:00.000000Z', 'a', 0.5)");
        execute("ALTER TABLE base DROP PARTITION LIST '2025-12-31'");
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    /**
     * The day the base loses, then a row a minute from 00:00 to 00:30 that the cadence leaves
     * unsealed, and from 00:31 to 00:lastMinute, which seals a root at its newest row.
     */
    private void commitTwoBatchesOverTheLostDay(LiveViewRefreshJob job, int lastMinute) throws Exception {
        commitTheDayTheBaseLoses(job);
        commitAndRefresh(job, minuteRowsOfA(0, 30, "1.0"));
        setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
        commitAndRefresh(job, minuteRowsOfA(31, lastMinute, "1.0"));
    }

    /**
     * Over the day the base loses, a row a minute from 00:00 to 00:30 and one at 01-01T23:59:50,
     * the floor of the one-minute frame of 01-02T00:00:50, then on 01-02 rows at 00:00:10, three
     * rows the filter drops, 00:00:50, which becomes the view's frontier, and 00:02:00 to 00:02:20,
     * which depart. {@code rootShape} puts a root at 00:00:10 ({@link #FRAME_ROOT_ABOVE_THE_FLOOR}),
     * at 01-01T23:59:50 ({@link #FRAME_ROOT_AT_THE_FLOOR}) or nowhere in the frame
     * ({@link #FRAME_ROOT_NONE}).
     */
    private void commitTheFrameAcrossMidnight(LiveViewRefreshJob job, int rootShape) throws Exception {
        final String frameFloorRow = "('2026-01-01T23:59:50.000000Z', 'a', 1.0)";
        final String rootRow = "('2026-01-02T00:00:10.000000Z', 'a', 1.0)";
        final String frontierRows = "('2026-01-02T00:00:20.000000Z', 'a', -1.0), ('2026-01-02T00:00:30.000000Z', 'a', -1.0), "
                + "('2026-01-02T00:00:40.000000Z', 'a', -1.0), ('2026-01-02T00:00:50.000000Z', 'a', 1.0), "
                + "('2026-01-02T00:02:00.000000Z', 'a', 1.0), ('2026-01-02T00:02:10.000000Z', 'a', 1.0), "
                + "('2026-01-02T00:02:20.000000Z', 'a', 1.0)";
        commitTheDayTheBaseLoses(job);
        switch (rootShape) {
            case FRAME_ROOT_ABOVE_THE_FLOOR -> {
                commitAndRefresh(job, minuteRowsOfA(0, 30, "1.0") + ", " + frameFloorRow);
                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                commitAndRefresh(job, rootRow);
                commitAndRefresh(job, frontierRows);
            }
            case FRAME_ROOT_AT_THE_FLOOR -> {
                commitAndRefresh(job, minuteRowsOfA(0, 30, "1.0"));
                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                commitAndRefresh(job, frameFloorRow);
                commitAndRefresh(job, rootRow + ", " + frontierRows);
            }
            default -> {
                commitAndRefresh(job, minuteRowsOfA(0, 30, "1.0") + ", " + frameFloorRow);
                commitAndRefresh(job, rootRow + ", " + frontierRows);
            }
        }
    }

    private void createBerlinAnchoredView() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS SELECT ts, sym, x, sum(x) OVER w s FROM base "
                + "WHERE x > 0 WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '02:30' 'Europe/Berlin')");
    }

    private void createDailyAnchoredView() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS SELECT ts, sym, x, sum(x) OVER w s FROM base "
                + "WHERE x > 0 WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')");
    }

    private void createHeadMissDepartureView() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " + HEAD_MISS_DEPARTURE_VIEW_SQL);
    }

    /**
     * A REPLACE_RANGE over the newest three minutes of the minute fixture, keeping one row,
     * {@code (00:57, 'a', 5.0)}: the head-miss rebuild's output ends below the root at 00:59 it
     * spliced, so it truncates the timeline at the new frontier 00:57 and seals a root there.
     */
    private void departTheNewestRangeKeepingOneRow(LiveViewRefreshJob job) {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        assertCheckpointLadder(instance, false, """
                2025-12-31T23:55:00.000000Z -> 1
                2026-01-01T00:59:00.000000Z -> 61
                """);
        final long headMissRepairsBefore = countHeadMissRepairs(job);
        replaceBaseRange(ts("2026-01-01T00:57:00.000000Z"), ts("2026-01-01T01:00:00.000000Z"), 5.0);
        drainWalQueue();
        driveRefreshToQuiescence(job);
        Assert.assertEquals("the head-miss rebuild repairs the departure", headMissRepairsBefore + 1, countHeadMissRepairs(job));
        Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_FRONTIER_TRUNCATE_COUNT));
        assertCheckpointLadder(instance, false, """
                2025-12-31T23:55:00.000000Z -> 1
                2026-01-01T00:57:00.000000Z -> 59
                """);
        assertNoRepairMarker(instance);
    }

    // The departure of the rows 01-02T00:02:00 to 00:02:20 commitTheFrameAcrossMidnight leaves
    // above the new frontier 01-02T00:00:50.
    private static void departTheRowsAboveTheFrameAcrossMidnight() {
        replaceBaseRange(ts("2026-01-02T00:02:00.000000Z"), ts("2026-01-02T00:03:00.000000Z"), Double.NaN);
    }

    /**
     * Rows of {@code a}, one a minute from {@code 2026-01-02T00:fromMinute} to
     * {@code 00:toMinute}, each holding 1.0, as the VALUES list of one INSERT.
     */
    // The minute fixture's rows moved to the last two days of year 9999.
    private static String inTheFarFuture(String rows) {
        return rows.replace("2026-01-01", "9999-12-31").replace("2025-12-31", "9999-12-30");
    }

    private static String minuteRowsOfAOnTheSecond(int fromMinute, int toMinute) {
        return minuteRowsOfA(fromMinute, toMinute, "1.0").replace("2026-01-01T", "2026-01-02T");
    }

    /**
     * Rows of {@code a} every five minutes from 2025-10-25T20:00Z to 10-26T00:25Z, 54 in all,
     * all on 10-25's segment of ANCHOR DAILY '02:30' Europe/Berlin, which starts at 10-25T00:30Z,
     * with a row of {@code b} at 10-26T00:20Z, then {@code tail}, as the VALUES list of one INSERT.
     */
    private static String rowsOfTheFallBackEve(String tail) {
        final StringSink sink = new StringSink();
        final long fromTs = ts("2025-10-25T20:00:00.000000Z");
        for (int i = 0; i < 54; i++) {
            final long rowTs = fromTs + i * 5 * Micros.MINUTE_MICROS;
            sink.put("(").put(rowTs).put("::timestamp, 'a', 1.0), ");
            if (rowTs == ts("2025-10-26T00:20:00.000000Z")) {
                sink.put("('2025-10-26T00:20:00.000000Z', 'b', 1.0), ");
            }
        }
        return sink.put(tail).toString();
    }

    /**
     * Deletes the view's rows from {@code fromTs} to the end of the base table and asserts the
     * head-miss rebuild repaired it and, having emitted nothing with no root a restart could
     * restore at the new frontier, retired the timeline.
     */
    private void departAndAssertTheTimelineRetired(LiveViewRefreshJob job, LiveViewInstance instance, String fromTs) {
        final long headMissRepairsBefore = countHeadMissRepairs(job);
        final long resetsBefore = instance.getCheckpointTimelineResets();
        replaceBaseRange(ts(fromTs), ts("2026-01-01T01:00:00.000000Z"), Double.NaN);
        drainWalQueue();
        driveRefreshToQuiescence(job);
        Assert.assertEquals("the head-miss rebuild repairs the departure", headMissRepairsBefore + 1, countHeadMissRepairs(job));
        Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_ZERO_ROW_RETIRE_COUNT));
        Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_FRONTIER_TRUNCATE_COUNT));
        Assert.assertEquals("the repair retires the timeline", resetsBefore + 1, instance.getCheckpointTimelineResets());
        // No root stands at the frontier, so the head stays clear: a head over the retired
        // timeline would fail every publication of the cycles without rows after it.
        Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
        try (
                Path dir = new Path().of(configuration.getDbRoot()).concat(instance.getLiveViewToken()).concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
                Path timelinePath = new Path()
        ) {
            LiveViewCheckpointLayout.timelinePath(timelinePath, dir);
            Assert.assertFalse("the retired timeline is gone", configuration.getFilesFacade().exists(timelinePath.$()));
            Assert.assertFalse("the retire takes the marker with it", LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir));
        }
        assertNoRefreshFaults("lv");
    }

    /**
     * The materialized-view shape of a departure: an incremental refresh replaces the newest
     * bucket's range with nothing, which is how a bucket that stops passing the mat view's
     * filter leaves. Its row was the newest the view held, above a root the base cannot
     * rebuild: the day below it is dropped, and the view keeps its row. A late row then lands
     * between the root below the departed row and the departed row. Each restart must
     * restore from the timeline, since the rebuild it would fall back to is the one the
     * restatement guard refuses over the dropped day.
     *
     * @param window         the view's window over {@code x}
     * @param forwardS       the window's value at the forward row committed after the first restart
     */
    /**
     * The materialized-view shape with the chain declined: over the day the base loses, a root at
     * 00:01 and one at 00:02, then a REPLACE_RANGE that deletes 00:02 and writes nothing. The
     * resume truncates at its anchor 00:01 ahead of its commit and replays no row, so the anchor
     * is the view's new frontier, and a watermark publishes it at the repair commit. Each restart
     * restores the view from it, over the day the base lost, and the forward row 00:06 between the
     * two folds onto the restored state.
     */
    private void assertADeletedNewestRowWithTheChainDeclinedRestoresRetainedRows(String window, String forwardS) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, 0);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS SELECT ts, sym, x, "
                    + window + " s FROM base WHERE x > 0");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitTheDayTheBaseLoses(job);
                commitAndRefresh(job, "('2026-01-01T00:01:00.000000Z', 'a', 1.5)");
                commitAndRefresh(job, "('2026-01-01T00:02:00.000000Z', 'a', 2.5)");
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                assertCheckpointLadder(instance, false, """
                        2025-12-31T23:55:00.000000Z -> 1
                        2026-01-01T00:01:00.000000Z -> 2
                        2026-01-01T00:02:00.000000Z -> 3
                        """);
                final long resetsBefore = instance.getCheckpointTimelineResets();
                replaceBaseRange(ts("2026-01-01T00:02:00.000000Z"), ts("2026-01-01T00:03:00.000000Z"), Double.NaN);
                drainWalQueue();
                driveRefreshToQuiescence(job);
                Assert.assertEquals("the zero-row resume keeps its timeline", resetsBefore, instance.getCheckpointTimelineResets());
                Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.TRUNCATE_ZERO_ROW_KEEP_COUNT));
                Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT));
                assertCheckpointLadder(instance, false, """
                        2025-12-31T23:55:00.000000Z -> 1
                        2026-01-01T00:01:00.000000Z -> 2
                        """);
                assertNoRepairMarker(instance);
            }
            final String rows = """
                    ts\tsym\tx\ts
                    2025-12-31T23:55:00.000000Z\ta\t0.5\t0.5
                    2026-01-01T00:01:00.000000Z\ta\t1.5\t2.0
                    """;
            assertDepartedViewRows(rows);
            assertNoRefreshFaults("lv");

            restartLiveViewsAndRecover();
            assertDepartedViewRestored(rows);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                commitAndRefresh(job, "('2026-01-01T00:06:00.000000Z', 'a', 4.5)");
            }
            final String grownRows = rows + "2026-01-01T00:06:00.000000Z\ta\t4.5\t" + forwardS + "\n";
            assertDepartedViewRows(grownRows);
            restartLiveViewsAndRecover();
            assertDepartedViewRestored(grownRows);
        });
    }

    /**
     * Over the day the base loses, a root at 00:01, then {@code rootCount} one-row commits a
     * second apart above it, each sealing a root, then one REPLACE_RANGE deleting them all. The
     * resume anchors on the root at 00:01 and crosses a boundary per deleted root. Past the
     * default chain budget it declines the chain and truncates at its anchor, and its zero-row
     * replay publishes the anchor, the view's new frontier, at its commit; within the budget the
     * splice re-versions every root, and the frontier truncate drops them. Either way the timeline
     * keeps the anchor, which each restart restores from over the day the base lost.
     */
    private void assertDeletedNewestRootsKeepTheTimeline(int rootCount, boolean isChainDeclined) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS SELECT ts, sym, x, "
                    + "sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) s FROM base WHERE x > 0");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitTheDayTheBaseLoses(job);
                commitAndRefresh(job, "('2026-01-01T00:01:00.000000Z', 'a', 1.5)");
                final long firstDeletedTs = ts("2026-01-01T00:02:00.000000Z");
                for (int i = 0; i < rootCount; i++) {
                    execute("INSERT INTO base VALUES (" + (firstDeletedTs + i * Micros.SECOND_MICROS) + "::timestamp, 'a', 1.0)");
                    drainWalQueue();
                    driveRefreshToQuiescence(job);
                }
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                Assert.assertEquals("a root per commit", rootCount + 2, snapshotCheckpointLadder(instance).size() / 2);
                final long resetsBefore = instance.getCheckpointTimelineResets();
                replaceBaseRange(firstDeletedTs, firstDeletedTs + rootCount * Micros.SECOND_MICROS, Double.NaN);
                drainWalQueue();
                driveRefreshToQuiescence(job);
                Assert.assertEquals("the repair keeps its timeline", resetsBefore, instance.getCheckpointTimelineResets());
                Assert.assertEquals(isChainDeclined ? 1 : 0, job.getStatsForTest().get(LiveViewRefreshStats.TRUNCATE_ZERO_ROW_KEEP_COUNT));
                Assert.assertEquals(isChainDeclined ? 0 : 1, job.getStatsForTest().get(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT));
                assertCheckpointLadder(instance, false, """
                        2025-12-31T23:55:00.000000Z -> 1
                        2026-01-01T00:01:00.000000Z -> 2
                        """);
                assertNoRepairMarker(instance);
            }
            final String rows = """
                    ts\tsym\tx\ts
                    2025-12-31T23:55:00.000000Z\ta\t0.5\t0.5
                    2026-01-01T00:01:00.000000Z\ta\t1.5\t2.0
                    """;
            assertDepartedViewRows(rows);
            assertNoRefreshFaults("lv");
            restartLiveViewsAndRecover();
            assertDepartedViewRestored(rows);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                commitAndRefresh(job, "('2026-01-01T00:06:00.000000Z', 'a', 4.5)");
            }
            final String grownRows = rows + "2026-01-01T00:06:00.000000Z\ta\t4.5\t6.5\n";
            assertDepartedViewRows(grownRows);
            restartLiveViewsAndRecover();
            assertDepartedViewRestored(grownRows);
        });
    }

    private void assertDepartedNewestBucketRestoresRetainedRows(String window, String forwardS) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS SELECT ts, sym, x, "
                    + window + " s FROM base WHERE x > 0");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitAndRefresh(job, "('2025-12-31T23:55:00.000000Z', 'a', 0.5)");
                execute("ALTER TABLE base DROP PARTITION LIST '2025-12-31'");
                drainWalQueue();
                driveRefreshToQuiescence(job);
                commitAndRefresh(job, "('2026-01-01T00:01:00.000000Z', 'a', 1.5)");
                commitAndRefresh(job, "('2026-01-01T00:02:00.000000Z', 'a', 2.5)");
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                assertCheckpointLadder(instance, false, """
                        2025-12-31T23:55:00.000000Z -> 1
                        2026-01-01T00:01:00.000000Z -> 2
                        2026-01-01T00:02:00.000000Z -> 3
                        """);

                final TableToken baseToken = engine.verifyTableName("base");
                try (WalWriter walWriter = engine.getWalWriter(baseToken)) {
                    walWriter.commitWithParams(
                            ts("2026-01-01T00:02:00.000000Z"),
                            ts("2026-01-01T00:03:00.000000Z"),
                            WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE
                    );
                }
                drainWalQueue();
                driveRefreshToQuiescence(job);
                assertCheckpointLadder(instance, false, """
                        2025-12-31T23:55:00.000000Z -> 1
                        2026-01-01T00:01:00.000000Z -> 2
                        """);
                Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT));

                commitAndRefresh(job, "('2026-01-01T00:01:30.000000Z', 'a', 3.0)");
                assertCheckpointLadder(instance, false, """
                        2025-12-31T23:55:00.000000Z -> 1
                        2026-01-01T00:01:00.000000Z -> 2
                        2026-01-01T00:01:30.000000Z -> 3
                        """);
            }
            final String rows = """
                    ts\tsym\tx\ts
                    2025-12-31T23:55:00.000000Z\ta\t0.5\t0.5
                    2026-01-01T00:01:00.000000Z\ta\t1.5\t2.0
                    2026-01-01T00:01:30.000000Z\ta\t3.0\t5.0
                    """;
            assertDepartedViewRows(rows);
            assertNoRefreshFaults("lv");

            restartLiveViewsAndRecover();
            assertDepartedViewRestored(rows);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                commitAndRefresh(job, "('2026-01-01T00:03:00.000000Z', 'a', 4.0)");
            }
            final String grownRows = rows + "2026-01-01T00:03:00.000000Z\ta\t4.0\t" + forwardS + "\n";
            assertDepartedViewRows(grownRows);
            restartLiveViewsAndRecover();
            assertDepartedViewRestored(grownRows);
        });
    }

    private void assertDepartedViewRestored(String rows) throws Exception {
        assertRestoredFromTimeline("lv");
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
        assertQuery("SELECT view_status FROM live_views() WHERE view_name = 'lv'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("view_status\nactive\n");
        assertDepartedViewRows(rows);
        assertNoRefreshFaults("lv");
    }

    private void assertDepartedViewRows(String rows) throws Exception {
        assertQuery("SELECT ts, sym, x, s FROM lv")
                .noLeakCheck()
                .timestamp("ts")
                .expectSize()
                .returns(rows);
    }

    private void assertLiveViewValid() {
        LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        Assert.assertFalse("LV must stay valid after a base replace-range commit", instance.isInvalid());
    }

    // One refresh cycle past the FLUSH EVERY rate-limit: advances the clock so
    // the commit is not deferred, runs the job, and applies the LV WAL.
    private void refreshCycle(LiveViewRefreshJob job) {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        drainJob(job);
        drainWalQueue();
    }

    /**
     * Once armed, fails the n-th open of a view's timeline superblock, {@code _timeline} in its
     * checkpoint directory, as a missing file: the failure no retry gets past.
     */
    private static class TimelineOpenFault extends TestFilesFacadeImpl {
        private final AtomicReference<String> armedDirName = new AtomicReference<>();
        private final AtomicBoolean isErrnoPending = new AtomicBoolean();
        private final AtomicInteger opensToFail = new AtomicInteger();
        private final AtomicBoolean hasFired = new AtomicBoolean();

        @Override
        public int errno() {
            return isErrnoPending.compareAndSet(true, false) ? CairoException.ERRNO_FILE_DOES_NOT_EXIST : super.errno();
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            final String dirName = armedDirName.get();
            if (dirName != null
                    && Utf8s.containsAscii(name, Files.SEPARATOR + dirName + Files.SEPARATOR)
                    && Utf8s.endsWithAscii(name, Files.SEPARATOR + LiveViewCheckpointLayout.TIMELINE_FILE_NAME)
                    && opensToFail.decrementAndGet() == 0) {
                armedDirName.set(null);
                hasFired.set(true);
                isErrnoPending.set(true);
                return -1;
            }
            return super.openRW(name, opts);
        }

        void arm(LiveViewInstance instance, int nthOpen) {
            opensToFail.set(nthOpen);
            armedDirName.set(instance.getLiveViewToken().getDirName());
        }

        boolean hasFired() {
            return hasFired.get();
        }
    }
}
