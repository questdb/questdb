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
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointRecoveryPhase;
import io.questdb.cairo.lv.LiveViewCheckpointRepairMarker;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewRefreshStats;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.Numbers;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
    public void testAnEndOfTableRepairThatDeletesTheNewestRangeOverALostDayBlocksTheRestart() throws Exception {
        // The pure delete of the newest three minutes emits nothing, and its rebuild's runtime
        // holds only the state it warmed from one minute below them, which is not the state at
        // the new frontier 00:56. No root sits there, so no restart could restore the timeline the
        // splice published: the repair retires it, as a zero-row end-of-table repair did before the
        // splice existed. The restart then rebuilds, and the restatement guard refuses the rebuild
        // over the day the base lost: the view stops and keeps every row it holds, the
        // 2025-12-31 row among them. Known limitation: a zero-row end-of-table repair with no
        // restorable root at its frontier retires the timeline.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitTwoBatchesOverTheLostDay(job, 59);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                departAndAssertTheTimelineRetired(job, instance, "2026-01-01T00:57:00.000000Z");
            }
            final String rows = headMissDepartureRows(true, 56);
            assertDepartedViewRows(rows);
            for (int restart = 0; restart < 2; restart++) {
                final LogCapture capture = new LogCapture();
                capture.start();
                try {
                    restartLiveViewsAndRecover();
                    capture.drain();
                    capture.assertLogged("live view restart rebuilding from applied base [view=lv, cause=timeline is absent");
                    capture.assertNotLogged("does not match durable materialization");
                } finally {
                    capture.stop();
                }
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                Assert.assertEquals(
                        "rebuild_blocked",
                        LiveViewCheckpointRecoveryPhase.routeName(instance.getCheckpointRestoreRoute())
                );
                Assert.assertTrue(instance.isCheckpointRecoveryBlocked());
                assertDepartedViewRows(rows);
            }
        });
    }

    @Test
    public void testAnEndOfTableRepairThatDeletesTheNewestRangeWithNoRootAboveTheFrontierRetiresItsTimeline() throws Exception {
        // One batch carries every row from 00:00 to 00:59 and the cadence seals no root over it,
        // so the newest root, 23:55, sits below the new frontier 00:56 and the splice captured
        // none. The splice still moved the generation's base coordinate to the repair commit, and
        // the rebuild's runtime cannot seal 00:56, so the repair retires the timeline. Over a base
        // that lost nothing the restart rebuilds the rows a recompute gives, and the next seal
        // opens a timeline a restart restores from.
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
                departAndAssertTheTimelineRetired(job, instance, "2026-01-01T00:57:00.000000Z");
            }
            assertRebuiltOnRestartThenRestoredAfterTheNextRow(headMissDepartureRows(true, 56));
        });
    }

    @Test
    public void testAnEndOfTableRepairThatDeletesTheNewestRangeWithNoRootAtTheFrontierRetiresItsTimeline() throws Exception {
        // The item's pure delete over a base that lost nothing: the splice kept the root at 00:59
        // above the new frontier 00:56, and the newest root at or below the frontier is 23:55.
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
                departAndAssertTheTimelineRetired(job, instance, "2026-01-01T00:57:00.000000Z");
            }
            assertRebuiltOnRestartThenRestoredAfterTheNextRow(headMissDepartureRows(true, 56));
        });
    }

    @Test
    public void testAnEndOfTableRepairThatDeletesTheNewestRangeWithNoRootBelowItRetiresItsTimeline() throws Exception {
        // The first seal took every row at once, so the only root sits at 00:59, above the new
        // frontier, and none sits at or below it.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitAndRefresh(job, minuteRowsOfA(0, 59, "1.0"));
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                assertCheckpointLadder(instance, false, "2026-01-01T00:59:00.000000Z -> 60\n");
                departAndAssertTheTimelineRetired(job, instance, "2026-01-01T00:57:00.000000Z");
            }
            assertRebuiltOnRestartThenRestoredAfterTheNextRow(headMissDepartureRows(false, 56));
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
    public void testAnEndOfTableRepairThatDeletesTheRowsAboveAGrownRootRetiresItsTimeline() throws Exception {
        // The root at the row that becomes the frontier was sealed before a row of another key
        // tied it, so it counts one row fewer than the view holds there. A resume refuses it as an
        // anchor, and so does the rebuild that emits nothing above it: it is no root a restart
        // could restore the view's rows from, so the repair retires the timeline.
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
                departAndAssertTheTimelineRetired(job, instance, "2026-01-01T00:57:00.000000Z");
            }
            final String rows = headMissDepartureRows(true, 56) + "2026-01-01T00:56:00.000000Z\tb\t1.0\t1.0\n";
            assertRebuiltOnRestartThenRestoredAfterTheNextRow(rows);
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
    public void testAnEndOfTableRepairWithoutASpliceThatDeletesTheNewestRangeRetiresItsTimeline() throws Exception {
        // No chained boundary is allowed, so the rebuild declines its splice and truncates the
        // timeline at R before it commits, as every end-of-table repair did before the splice
        // existed. It emits nothing to seal at the new frontier, so the truncated timeline
        // retires, and over a base that lost nothing the restart rebuilds what a recompute gives.
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
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                final long headMissRepairsBefore = countHeadMissRepairs(job);
                final long resetsBefore = instance.getCheckpointTimelineResets();
                replaceBaseRange(ts("2026-01-01T00:57:00.000000Z"), ts("2026-01-01T01:00:00.000000Z"), Double.NaN);
                drainWalQueue();
                driveRefreshToQuiescence(job);
                Assert.assertEquals("the head-miss rebuild repairs the departure", headMissRepairsBefore + 1, countHeadMissRepairs(job));
                assertNoHeadMissSpliceFrontierStat(job);
                Assert.assertEquals("the truncate keeps a prefix, which retires with no seal", resetsBefore + 1, instance.getCheckpointTimelineResets());
                assertNoRepairMarker(instance);
            }
            assertRebuiltOnRestartThenRestoredAfterTheNextRow(headMissDepartureRows(true, 56));
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
     * The restart after a repair that retired the timeline over a base that lost nothing
     * rebuilds from the applied base, which gives what a recompute gives, and the next row seals
     * a root the restart after it restores from.
     */
    private void assertRebuiltOnRestartThenRestoredAfterTheNextRow(String rows) throws Exception {
        assertDepartedViewRows(rows);
        assertNoRefreshFaults("lv");
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            restartLiveViewsAndRecover();
            capture.drain();
            capture.assertLogged("live view restart rebuilding from applied base [view=lv, cause=timeline is absent");
        } finally {
            capture.stop();
        }
        assertRebuiltFromAppliedBase("lv");
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
        assertDepartedViewRows(rows);
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + HEAD_MISS_DEPARTURE_VIEW_SQL + ") ORDER BY ts, sym",
                "(lv) ORDER BY ts, sym",
                LOG,
                true
        );
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            commitAndRefresh(job, "('2026-01-01T01:05:00.000000Z', 'a', 4.0)");
        }
        final String grownRows = rows + "2026-01-01T01:05:00.000000Z\ta\t4.0\t4.0\n";
        restartLiveViewsAndRecover();
        assertDepartedViewRestored(grownRows);
    }

    private void commitAndRefresh(LiveViewRefreshJob job, String values) throws Exception {
        execute("INSERT INTO base VALUES " + values);
        drainWalQueue();
        driveRefreshToQuiescence(job);
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
}
