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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRebuildRestatementGuard;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewRefreshStats;
import io.questdb.cairo.lv.LiveViewRefreshTask;
import io.questdb.std.str.Path;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A materialized view is an accepted live-view base (WAL-backed, designated timestamp -
 * see {@code LiveViewSmokeTest#testMaterializedViewAcceptedAsBase}), which makes the
 * freeze-and-continue contract for base data removal unsound for that one base kind.
 * <p>
 * Freeze-and-continue says a live view keeps its already-emitted rows and its window
 * accumulators across a base TRUNCATE / DROP PARTITION: the refresh worker walks past the
 * removal seqTxn without rewriting LV state. That is right for a plain base, where a
 * TRUNCATE retires settled data the view already consumed and a live view is a
 * forward-computed row stream rather than a re-derivable aggregate.
 * <p>
 * A materialized view's contents are <em>derived</em>, so a TRUNCATE of one is never data
 * retirement - it is the rebuild half of a full refresh, which re-materialises the same
 * logical rows. A live view that walked past it would emit derived rows for those rows a
 * second time, with its accumulators still carrying the pre-rebuild state, and would stay
 * ACTIVE while doing so.
 * <p>
 * An incremental refresh is the other half: it commits a REPLACE_RANGE over the buckets it
 * recomputes, which the live view repairs as out-of-order data, and which can remove a bucket
 * the view derived output from. The anchored cases follow such refreshes through the repair,
 * the restart restore and a whole-view rebuild. No restatement guard checks a rebuild over a
 * materialized view: it follows the mat view and drops the rows the mat view no longer holds,
 * which the cases over a mat view with TTL follow through every route that rebuilds.
 */
public class LiveViewMatViewBaseTest extends AbstractLiveViewTest {

    private static final String ANCHORED_RECOMPUTE_SQL = """
            SELECT ts, k,
                sum(sv) OVER (PARTITION BY k, day ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s,
                count(sv) OVER (PARTITION BY k, day ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS c
            FROM (SELECT ts, k, sv, timestamp_floor('1d', ts) AS day FROM mvbase)
            """;
    private static final String ANCHORED_VIEW_SQL = "SELECT ts, k, sum(sv) OVER w AS s, count(sv) OVER w AS c FROM mvbase "
            + "WINDOW w AS (PARTITION BY k ORDER BY ts ANCHOR DAILY '00:00')";
    // Replaces the base row behind a@2026-01-01T02:00 with one the mat view's filter drops, so the
    // incremental refresh's REPLACE_RANGE leaves that bucket empty and removes a row the live view
    // derived output from.
    private static final String BUCKET_REMOVING_CORRECTION = "INSERT INTO base (ts, k, v) VALUES ('2026-01-01T02:10:00.000000Z', 'a', -1.0)";
    private static final String DAY_ONE_ROWS = """
            INSERT INTO base (ts, k, v) VALUES
                ('2026-01-01T00:10:00.000000Z', 'a', 1.0),
                ('2026-01-01T01:10:00.000000Z', 'a', 2.0),
                ('2026-01-01T01:20:00.000000Z', 'b', 8.0),
                ('2026-01-01T02:10:00.000000Z', 'a', 4.0)
            """;
    private static final String DAY_TWO_ROWS = """
            INSERT INTO base (ts, k, v) VALUES
                ('2026-01-02T00:10:00.000000Z', 'a', 16.0),
                ('2026-01-02T01:10:00.000000Z', 'b', 32.0)
            """;
    // The roots departNewestBucketsThroughAHeadMissRebuild leaves: the newest at the new frontier
    // 00:57, which counts the bucket a day below and the 58 buckets from 00:00 to 00:57.
    private static final String DEPARTED_BUCKETS_LADDER = """
            2025-12-31T23:55:00.000000Z -> 1
            2026-01-01T00:30:00.000000Z -> 32
            2026-01-01T00:57:00.000000Z -> 59
            """;
    // A one-minute RANGE frame over the mat view's one-minute buckets.
    private static final String MINUTE_RANGE_VIEW_SQL = "SELECT ts, k, sum(sv) OVER (PARTITION BY k ORDER BY ts "
            + "RANGE BETWEEN '1' MINUTE PRECEDING AND CURRENT ROW) AS s FROM mvbase";
    // The rows the view over the mat view with TTL holds once a rebuild has followed the mat view,
    // which no longer holds 2026-01-01: the running sums start again at 2026-01-02.
    private static final String TTL_REBUILT_ROWS = """
            ts\tk\tsv\ts
            2026-01-02T00:00:00.000000Z\ta\t2.0\t2.0
            2026-01-03T05:00:00.000000Z\ta\t4.0\t6.0
            """;
    private static final String TTL_VIEW_ROWS_QUERY = "SELECT ts, k, sv, s FROM lv_on_mv";
    private static final String TTL_VIEW_SQL = "SELECT ts, k, sv, sum(sv) OVER (PARTITION BY k ORDER BY ts "
            + "ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS s FROM mvbase";
    private static final String VIEW_ROWS_QUERY = "SELECT ts, k, s, c FROM lv_on_mv ORDER BY k, ts";

    @Test
    public void testAMatViewRebuildWhoseInvalidationCouldNotPersistStillInvalidatesTheViewAfterARestart() throws Exception {
        // The apply job invalidates a live view over a mat view when it applies the TRUNCATE a full
        // refresh starts with, and persists that to _lv.s best effort. The refresh worker stops
        // every drain at the TRUNCATE until the view is invalid. When the persist failed, a restart
        // loaded the view valid again, and since the apply job never applies that seqTxn again,
        // the view stayed active, stuck at the TRUNCATE, and its worker reported work on every
        // pass. The drain must re-derive the invalidation and make it durable.
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        assertMemoryLeak(fault.facade(), () -> {
            execute("CREATE TABLE base (ts TIMESTAMP, k SYMBOL, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE MATERIALIZED VIEW mvbase AS (" +
                    "SELECT ts, k, avg(v) AS av FROM base SAMPLE BY 1h) PARTITION BY DAY");
            drainWalAndMatViewQueues(engine);
            setCurrentMicros(0L);
            execute("CREATE LIVE VIEW lv_on_mv FLUSH EVERY 100ms START FROM NOW AS SELECT ts, k, avg(av) OVER (" +
                    "PARTITION BY k ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS a FROM mvbase");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("""
                        INSERT INTO base (ts, k, v) VALUES
                            ('2026-01-01T00:00:00.000000Z', 'a', 1.0),
                            ('2026-01-01T01:00:00.000000Z', 'a', 2.0),
                            ('2026-01-01T02:00:00.000000Z', 'a', 3.0)
                        """);
                drainWalAndMatViewQueues(engine);
                driveRefreshToQuiescence(job);
            }
            execute("TRUNCATE TABLE base");
            execute("INSERT INTO base (ts, k, v) VALUES ('2026-01-01T05:00:00.000000Z', 'a', 100.0)");
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW mvbase FULL");
            drainMatViewQueue(engine);
            // The apply job reaches the mat view's TRUNCATE and invalidates the view, but cannot
            // write its _lv.s.
            fault.armViewStateOpen();
            drainWalQueue();
            Assert.assertTrue("the invalidation must have tried to persist", fault.hasViewStateOpenFired());
            final String invalidated = """
                    view_name\tview_status\tinvalidation_reason
                    lv_on_mv\tinvalid\tbase materialized view was rebuilt
                    """;
            assertQuery("SELECT view_name, view_status, invalidation_reason FROM live_views()")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(invalidated);

            // The restart loads the view from _lv.s, which still says valid.
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            int busyPasses = 0;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int i = 0; i < 100; i++) {
                    setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                    drainWalQueue();
                    if (drainJob(job)) {
                        busyPasses++;
                    }
                }
            }
            assertQuery("SELECT view_name, view_status, invalidation_reason FROM live_views()")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(invalidated);
            Assert.assertTrue("an invalid view must leave its worker idle, busy passes: " + busyPasses, busyPasses < 100);

            // The re-derived invalidation is durable: another restart loads the view invalid
            // before any refresh pass runs.
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            assertQuery("SELECT view_name, view_status, invalidation_reason FROM live_views()")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(invalidated);
        });
    }

    @Test
    public void testARebuildOverAMatViewWhoseBacklogIsUnreadableRunsWithoutTheGuard() throws Exception {
        // The backlog of a materialized view may hold a REPLACE_RANGE or a TRUNCATE in any commit,
        // and once its WAL is gone nothing tells such a removal from a loss. No restatement guard
        // runs over a materialized view, so the rebuild never reads the backlog and follows the
        // mat view. Here the backlog whose WAL is gone is the refresh that dropped a bucket, which a
        // guard comparing the row count would refuse as a loss.
        assertMemoryLeak(() -> {
            seedAnchoredViewOverMatView();
            applyBucketRemovingRefreshWithoutTheView();
            shutdown();
            removeTimeline();
            removeBaseWal("mvbase");

            final LogCapture capture = new LogCapture();
            final LiveViewRebuildRestatementGuard guard;
            capture.start();
            try {
                guard = restart();
                capture.drain();
            } finally {
                capture.stop();
            }

            assertRebuiltFromAppliedBase("lv_on_mv");
            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv_on_mv");
            Assert.assertNotNull(instance);
            Assert.assertFalse(
                    "the rebuild must follow the mat view: " + instance.getCheckpointRecoveryReason(),
                    instance.isCheckpointRecoveryBlocked()
            );
            Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_MAT_VIEW_BASE, guard.getAbstention());
            capture.assertNotLogged("live view could not read the rebuild's base backlog");
            capture.assertLogged("live view rebuild from the applied base runs without the restatement guard [view=lv_on_mv, reason=materialized view base]");
            capture.assertNotLogged("live view rebuild from the applied base refused");
            assertViewRowsAfterTheBucketRemoval();
            assertViewActive();
        });
    }

    @Test
    public void testARebuildOverAMatViewWithNoTimelineFollowsItsRefresh() throws Exception {
        // Every incremental refresh of a materialized view commits a REPLACE_RANGE over the
        // buckets it recomputes, and one that leaves a bucket empty removes a row the live view
        // derived output from. A restart that finds no timeline rebuilds from the applied mat
        // view, which reproduces one row fewer than the view holds. No restatement guard runs
        // over a materialized view, so the rebuild follows the refresh rather than refuse it as
        // a loss.
        assertMemoryLeak(() -> {
            seedAnchoredViewOverMatView();
            applyBucketRemovingRefreshWithoutTheView();
            shutdown();
            removeTimeline();

            final LogCapture capture = new LogCapture();
            final LiveViewRebuildRestatementGuard guard;
            capture.start();
            try {
                guard = restart();
                capture.drain();
            } finally {
                capture.stop();
            }

            assertRebuiltFromAppliedBase("lv_on_mv");
            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv_on_mv");
            Assert.assertNotNull(instance);
            Assert.assertFalse(
                    "the rebuild must follow the mat view: " + instance.getCheckpointRecoveryReason(),
                    instance.isCheckpointRecoveryBlocked()
            );
            Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_MAT_VIEW_BASE, guard.getAbstention());
            capture.assertLogged("live view rebuild from the applied base runs without the restatement guard [view=lv_on_mv, reason=materialized view base]");
            capture.assertNotLogged("live view could not read the rebuild's base backlog");
            capture.assertNotLogged("live view rebuild from the applied base refused");
            assertViewRowsAfterTheBucketRemoval();
            assertViewActive();
        });
    }

    @Test
    public void testARestartWithNoTimelineOverAMatViewThatLostRowsFollowsTheMatView() throws Exception {
        // The mat view's TTL evicted a day the live view keeps. A restart that finds no timeline -
        // the disk a CHECKPOINT restore leaves - rebuilds from the applied mat view. No restatement
        // guard runs over a materialized view, so the rebuild drops the evicted day and starts the
        // running sums again at the mat view's earliest row, as every rebuild did before the guard,
        // rather than stop the view.
        assertMemoryLeak(() -> {
            seedViewOverTtlMatView();
            shutdown();
            removeTimeline();

            final LogCapture capture = new LogCapture();
            final LiveViewRebuildRestatementGuard guard;
            capture.start();
            try {
                guard = restart();
                capture.drain();
            } finally {
                capture.stop();
            }

            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv_on_mv");
            Assert.assertNotNull(instance);
            Assert.assertFalse(
                    "the rebuild must follow the mat view: " + instance.getCheckpointRecoveryReason(),
                    instance.isCheckpointRecoveryBlocked()
            );
            assertRebuiltFromAppliedBase("lv_on_mv");
            Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_MAT_VIEW_BASE, guard.getAbstention());
            Assert.assertEquals(LiveViewRebuildRestatementGuard.VERDICT_NONE, guard.getVerdict());
            capture.assertLogged("live view rebuild from the applied base runs without the restatement guard [view=lv_on_mv, reason=materialized view base]");
            capture.assertNotLogged("live view could not read the rebuild's base backlog");
            capture.assertNotLogged("live view rebuild from the applied base refused");
            assertTtlViewActive();
            assertTtlViewRows(TTL_REBUILT_ROWS);
            assertTtlViewMatchesRecompute();
            assertNoRefreshFaults("lv_on_mv");
        });
    }

    @Test
    public void testARunningRecoveryOverAMatViewThatLostRowsRebuildsWhenItsRestoreFails() throws Exception {
        // A refresh fails mid-drain, and the running recovery's restore from the timeline fails on
        // an error that names no cause. Over a table base that lost rows the view keeps, the guard
        // refuses the rebuild and the recovery retries the restore. No guard runs over a
        // materialized view, so the rebuild commits in the failing turn and follows the mat view,
        // which no longer holds the day its TTL evicted.
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault("ts", "sv", "k");
        assertMemoryLeak(fault.facade(), () -> {
            seedViewOverTtlMatView();
            fault.of(engine.verifyTableName("mvbase").getDirName());
            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv_on_mv");
            Assert.assertNotNull(instance);
            final String rebuiltRows = """
                    ts\tk\tsv\ts
                    2026-01-02T00:00:00.000000Z\ta\t2.0\t2.0
                    2026-01-03T05:00:00.000000Z\ta\t4.0\t6.0
                    2026-01-03T08:00:00.000000Z\ta\t16.0\t22.0
                    2026-01-03T09:00:00.000000Z\ta\t32.0\t54.0
                    2026-01-03T10:00:00.000000Z\ta\t64.0\t118.0
                    """;

            final LogCapture capture = new LogCapture();
            capture.start();
            try {
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    // The first refresh lands in the view's un-flushed lead, and the next two
                    // coalesce behind it into one pass, which fails reading the third.
                    setCurrentMicros(instance.getLastFlushTimeUs());
                    execute("INSERT INTO base (ts, k, v) VALUES ('2026-01-03T08:10:00.000000Z', 'a', 16.0)");
                    drainWalAndMatViewQueues(engine);
                    drainJob(job);
                    execute("INSERT INTO base (ts, k, v) VALUES ('2026-01-03T09:10:00.000000Z', 'a', 32.0)");
                    drainWalAndMatViewQueues(engine);
                    execute("INSERT INTO base (ts, k, v) VALUES ('2026-01-03T10:10:00.000000Z', 'a', 64.0)");
                    drainWalAndMatViewQueues(engine);
                    fault.arm(1);
                    fault.armTimelineOpen(0);
                    drainJob(job);
                    Assert.assertTrue("the drain must fail mid-pass", fault.hasFired());
                    Assert.assertFalse("the restore must fail opening the timeline", fault.isTimelineOpenArmed());
                    final LiveViewRebuildRestatementGuard guard = job.rebuildRestatementGuardForTest();
                    Assert.assertEquals("no guard runs over a mat view", LiveViewRebuildRestatementGuard.VERDICT_NONE, guard.getVerdict());
                    Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_MAT_VIEW_BASE, guard.getAbstention());
                    Assert.assertFalse(
                            "the rebuild must follow the mat view: " + instance.getCheckpointRecoveryReason(),
                            instance.isCheckpointRecoveryBlocked()
                    );
                    Assert.assertFalse("the rebuild recovered the window state in the failing turn", instance.isWindowStateDirty());
                    drainWalQueue();
                    assertTtlViewRows(rebuiltRows);

                    fault.disarmAll();
                    driveRefreshToQuiescence(job);
                }
                capture.drain();
            } finally {
                capture.stop();
            }
            capture.assertLogged("live view recomputed window state from applied base [view=lv_on_mv, cause=mid-drain refresh failure]");
            capture.assertLogged("live view rebuild from the applied base runs without the restatement guard [view=lv_on_mv, reason=materialized view base]");
            capture.assertNotLogged("retrying the restore");
            capture.assertNotLogged("live view rebuild from the applied base refused");
            assertTtlViewActive();
            assertTtlViewRows(rebuiltRows);
            assertTtlViewMatchesRecompute();

            shutdown();
            restart();
            assertRestoredFromTimeline("lv_on_mv");
            assertTtlViewActive();
            assertTtlViewRows(rebuiltRows);
        });
    }

    @Test
    public void testATurnedOffGuardOverAMatViewRecordsItAsDisabled() throws Exception {
        // With the guard switched off, a rebuild over a materialized view records the switch,
        // not the base kind, and logs no stand-down line, as over any base: the switch decides
        // ahead of the base kind.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_REBUILD_RESTATEMENT_GUARD_ENABLED, "false");
        assertMemoryLeak(() -> {
            seedViewOverTtlMatView();
            shutdown();
            removeTimeline();

            final LogCapture capture = new LogCapture();
            final LiveViewRebuildRestatementGuard guard;
            capture.start();
            try {
                guard = restart();
                capture.drain();
            } finally {
                capture.stop();
            }

            Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_DISABLED, guard.getAbstention());
            capture.assertNotLogged("runs without the restatement guard");
            capture.assertNotLogged("live view could not read the rebuild's base backlog");
            assertRebuiltFromAppliedBase("lv_on_mv");
            assertTtlViewActive();
            assertTtlViewRows(TTL_REBUILT_ROWS);
            assertTtlViewMatchesRecompute();
            assertNoRefreshFaults("lv_on_mv");
        });
    }

    @Test
    public void testAViewOverAMatViewWithNoRowsRebuildsWithNothingRetained() throws Exception {
        // A view whose filter rejects every mat-view row still consumes the mat view's commits,
        // so a restart that finds no timeline rebuilds it. The view holds no row for the rebuild
        // to drop, which the guard records ahead of the base kind, with no stand-down line.
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
            execute("CREATE TABLE base (ts TIMESTAMP, k SYMBOL, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE MATERIALIZED VIEW mvbase AS (SELECT ts, k, sum(v) AS sv FROM base SAMPLE BY 1h) PARTITION BY DAY");
            drainWalAndMatViewQueues(engine);
            setCurrentMicros(0L);
            execute("CREATE LIVE VIEW lv_on_mv FLUSH EVERY 100ms START FROM NOW AS "
                    + "SELECT ts, k, sv, sum(sv) OVER (PARTITION BY k ORDER BY ts ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS s "
                    + "FROM mvbase WHERE sv > 1000");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                refreshMatViewAndView(job, "INSERT INTO base (ts, k, v) VALUES ('2026-01-01T00:10:00.000000Z', 'a', 1.0)");
                refreshMatViewAndView(job, "INSERT INTO base (ts, k, v) VALUES ('2026-01-02T00:10:00.000000Z', 'a', 2.0)");
            }
            final LiveViewInstance seeded = engine.getLiveViewRegistry().getViewInstance("lv_on_mv");
            Assert.assertNotNull(seeded);
            Assert.assertEquals(
                    "the view must consume the mat view's commits",
                    engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("mvbase")),
                    seeded.getLastProcessedSeqTxn()
            );
            shutdown();
            Assert.assertFalse("a view that never emitted a row has sealed no timeline", timelineFile().exists());

            final LogCapture capture = new LogCapture();
            final LiveViewRebuildRestatementGuard guard;
            capture.start();
            try {
                guard = restart();
                capture.drain();
            } finally {
                capture.stop();
            }

            assertRebuiltFromAppliedBase("lv_on_mv");
            Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_NOTHING_RETAINED, guard.getAbstention());
            capture.assertNotLogged("runs without the restatement guard");
            assertTtlViewActive();
            assertTtlViewRows("ts\tk\tsv\ts\n");
        });
    }

    @Test
    public void testAViewRebuiltOverAMatViewKeepsRowsTheMatViewEvictsLater() throws Exception {
        // Once the restart's rebuild has followed the mat view, the view refreshes again, so a day
        // the mat view's TTL evicts after the rebuild stays in the view, as incremental refresh
        // keeps every row its base loses. A purge of the mat view's WAL and another restart then
        // restore the view from its timeline with that day still in it.
        configureWalPurge();
        assertMemoryLeak(() -> {
            seedViewOverTtlMatView();
            shutdown();
            removeTimeline();
            restart();
            final LiveViewInstance rebuilt = engine.getLiveViewRegistry().getViewInstance("lv_on_mv");
            Assert.assertNotNull(rebuilt);
            Assert.assertFalse(
                    "the rebuild must follow the mat view: " + rebuilt.getCheckpointRecoveryReason(),
                    rebuilt.isCheckpointRecoveryBlocked()
            );
            assertRebuiltFromAppliedBase("lv_on_mv");
            assertTtlViewRows(TTL_REBUILT_ROWS);

            // TTL reads min(newest row, wall clock): this commit's refresh evicts 2026-01-02.
            setCurrentMicros(ts("2026-01-05T00:00:00.000000Z"));
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                refreshMatViewAndView(job, "INSERT INTO base (ts, k, v) VALUES ('2026-01-04T05:10:00.000000Z', 'a', 16.0)");
            }
            assertQuery("SELECT ts, k, sv FROM mvbase")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tk\tsv
                            2026-01-03T05:00:00.000000Z\ta\t4.0
                            2026-01-04T05:00:00.000000Z\ta\t16.0
                            """);
            final String keptRows = """
                    ts\tk\tsv\ts
                    2026-01-02T00:00:00.000000Z\ta\t2.0\t2.0
                    2026-01-03T05:00:00.000000Z\ta\t4.0\t6.0
                    2026-01-04T05:00:00.000000Z\ta\t16.0\t22.0
                    """;
            assertTtlViewRows(keptRows);
            final long matViewSeqTxn = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("mvbase"));
            final long segmentsLeft = purgeBaseWalAndCountSegments("mvbase");
            Assert.assertTrue(
                    "the purge must take mat-view WAL [segmentsLeft=" + segmentsLeft + ", matViewSeqTxn=" + matViewSeqTxn + ']',
                    segmentsLeft < matViewSeqTxn
            );

            shutdown();
            restart();
            assertRestoredFromTimeline("lv_on_mv");
            assertTtlViewActive();
            assertTtlViewRows(keptRows);
            assertNoRefreshFaults("lv_on_mv");
        });
    }

    @Test
    public void testAWalLossRederiveOverAMatViewThatLostRowsFollowsTheMatView() throws Exception {
        // The mat view refreshes a bucket the live view has not drained, and then the mat view's
        // WAL is lost. The restart restores the view from its timeline, the drain meets the lost
        // segment, and once the flush-retry budget runs out the view re-derives from the applied
        // mat view. No guard runs over a materialized view, so the rebuild never walks the backlog
        // it could not read, and follows the mat view, which no longer holds the day its TTL evicted.
        assertMemoryLeak(() -> {
            seedViewOverTtlMatView();
            final LiveViewInstance seeded = engine.getLiveViewRegistry().getViewInstance("lv_on_mv");
            Assert.assertNotNull(seeded);
            final long processedBefore = seeded.getLastProcessedSeqTxn();
            execute("INSERT INTO base (ts, k, v) VALUES ('2026-01-03T08:10:00.000000Z', 'a', 16.0)");
            drainWalAndMatViewQueues(engine);
            Assert.assertEquals("the view must not drain the refresh", processedBefore, seeded.getLastProcessedSeqTxn());
            shutdown();
            removeBaseWal("mvbase");

            final LogCapture capture = new LogCapture();
            final LiveViewRebuildRestatementGuard guard;
            capture.start();
            try {
                guard = restart();
                capture.drain();
            } finally {
                capture.stop();
            }

            Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_MAT_VIEW_BASE, guard.getAbstention());
            Assert.assertEquals(LiveViewRebuildRestatementGuard.VERDICT_NONE, guard.getVerdict());
            capture.assertLogged("live view re-derived from the applied base after base WAL loss [view=lv_on_mv");
            capture.assertLogged("live view rebuild from the applied base runs without the restatement guard [view=lv_on_mv, reason=materialized view base]");
            capture.assertNotLogged("live view could not read the rebuild's base backlog");
            capture.assertNotLogged("live view rebuild from the applied base refused");
            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv_on_mv");
            Assert.assertNotNull(instance);
            Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
            Assert.assertFalse(instance.isInvalid());
            assertTtlViewActive();
            assertTtlViewRows("""
                    ts\tk\tsv\ts
                    2026-01-02T00:00:00.000000Z\ta\t2.0\t2.0
                    2026-01-03T05:00:00.000000Z\ta\t4.0\t6.0
                    2026-01-03T08:00:00.000000Z\ta\t16.0\t22.0
                    """);
            assertTtlViewMatchesRecompute();
        });
    }

    @Test
    public void testAnAnchoredViewOverAMatViewFollowsItsRefreshesAndRestoresFromItsTimeline() throws Exception {
        // Each incremental refresh of the mat view commits a REPLACE_RANGE, which the live view
        // drains as an out-of-order repair: a late base row that changes a bucket, one that adds
        // a bucket, and a correction that empties one. After each the view must equal a
        // recompute without a faulting turn, and a restart must restore it from the timeline
        // those repairs re-versioned, then carry on with the mat view's next refresh.
        assertMemoryLeak(() -> {
            seedAnchoredViewOverMatView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                refreshMatViewAndView(job, "INSERT INTO base (ts, k, v) VALUES ('2026-01-01T01:40:00.000000Z', 'a', 100.0)");
                assertViewMatchesRecompute();
                refreshMatViewAndView(job, "INSERT INTO base (ts, k, v) VALUES ('2026-01-01T03:10:00.000000Z', 'b', 64.0)");
                assertViewMatchesRecompute();
                refreshMatViewAndView(job, BUCKET_REMOVING_CORRECTION);
                assertViewMatchesRecompute();
            }
            final LiveViewInstance repaired = engine.getLiveViewRegistry().getViewInstance("lv_on_mv");
            Assert.assertNotNull(repaired);
            Assert.assertTrue(
                    "the refreshes must reach the view as repairs",
                    repaired.getO3ResumeReplayRows() + repaired.getO3BoundaryReplayRows() > 0
            );
            final String repairedRows = """
                    ts\tk\ts\tc
                    2026-01-01T00:00:00.000000Z\ta\t1.0\t1
                    2026-01-01T01:00:00.000000Z\ta\t103.0\t2
                    2026-01-02T00:00:00.000000Z\ta\t16.0\t1
                    2026-01-01T01:00:00.000000Z\tb\t8.0\t1
                    2026-01-01T03:00:00.000000Z\tb\t72.0\t2
                    2026-01-02T01:00:00.000000Z\tb\t32.0\t1
                    """;
            assertQuery(VIEW_ROWS_QUERY).noLeakCheck().expectSize().returns(repairedRows);

            restartCycle();
            assertRestoredFromTimeline("lv_on_mv");
            assertQuery(VIEW_ROWS_QUERY).noLeakCheck().expectSize().returns(repairedRows);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                refreshMatViewAndView(job, "INSERT INTO base (ts, k, v) VALUES ('2026-01-02T02:10:00.000000Z', 'a', 128.0)");
            }
            assertViewMatchesRecompute();
            assertQuery(VIEW_ROWS_QUERY).noLeakCheck().expectSize().returns("""
                    ts\tk\ts\tc
                    2026-01-01T00:00:00.000000Z\ta\t1.0\t1
                    2026-01-01T01:00:00.000000Z\ta\t103.0\t2
                    2026-01-02T00:00:00.000000Z\ta\t16.0\t1
                    2026-01-02T02:00:00.000000Z\ta\t144.0\t2
                    2026-01-01T01:00:00.000000Z\tb\t8.0\t1
                    2026-01-01T03:00:00.000000Z\tb\t72.0\t2
                    2026-01-02T01:00:00.000000Z\tb\t32.0\t1
                    """);
            assertViewActive();
        });
    }

    @Test
    public void testDepartedNewestBucketThenALateBucketRestoresFromTheTimeline() throws Exception {
        // A base correction that the mat view's filter drops empties its newest bucket, and its
        // incremental refresh removes that bucket with a REPLACE_RANGE: the newest row the view
        // derived leaves it. A late base row then fills a bucket between the root below the
        // departed one and the departed one. Each restart must restore from the timeline the
        // two repairs left.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, k SYMBOL, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, k)");
            execute("CREATE MATERIALIZED VIEW mvbase AS (SELECT ts, k, sum(v) AS sv FROM base WHERE v > 0 SAMPLE BY 1h) PARTITION BY DAY");
            drainWalAndMatViewQueues(engine);
            setCurrentMicros(0L);
            execute("CREATE LIVE VIEW lv_on_mv FLUSH EVERY 100ms START FROM NOW AS " + ANCHORED_VIEW_SQL);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                refreshMatViewAndView(job, "INSERT INTO base (ts, k, v) VALUES ('2026-01-01T00:10:00.000000Z', 'a', 1.0)");
                refreshMatViewAndView(job, "INSERT INTO base (ts, k, v) VALUES ('2026-01-01T02:10:00.000000Z', 'a', 4.0)");
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv_on_mv");
                Assert.assertNotNull(instance);
                assertCheckpointLadder(instance, false, """
                        2026-01-01T00:00:00.000000Z -> 1
                        2026-01-01T02:00:00.000000Z -> 2
                        """);
                refreshMatViewAndView(job, "INSERT INTO base (ts, k, v) VALUES ('2026-01-01T02:10:00.000000Z', 'a', -1.0)");
                assertCheckpointLadder(instance, false, """
                        2026-01-01T00:00:00.000000Z -> 1
                        """);
                Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT));
                refreshMatViewAndView(job, "INSERT INTO base (ts, k, v) VALUES ('2026-01-01T01:10:00.000000Z', 'a', 2.0)");
                assertCheckpointLadder(instance, false, """
                        2026-01-01T00:00:00.000000Z -> 1
                        2026-01-01T01:00:00.000000Z -> 2
                        """);
            }
            assertViewMatchesRecompute();
            final String rows = """
                    ts\tk\ts\tc
                    2026-01-01T00:00:00.000000Z\ta\t1.0\t1
                    2026-01-01T01:00:00.000000Z\ta\t3.0\t2
                    """;
            assertQuery(VIEW_ROWS_QUERY).noLeakCheck().expectSize().returns(rows);

            restartCycle();
            assertRestoredFromTimeline("lv_on_mv");
            assertQuery(VIEW_ROWS_QUERY).noLeakCheck().expectSize().returns(rows);
            assertViewActive();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                refreshMatViewAndView(job, "INSERT INTO base (ts, k, v) VALUES ('2026-01-01T03:10:00.000000Z', 'a', 8.0)");
            }
            assertViewMatchesRecompute();
            final String grownRows = rows + "2026-01-01T03:00:00.000000Z\ta\t11.0\t3\n";
            assertQuery(VIEW_ROWS_QUERY).noLeakCheck().expectSize().returns(grownRows);

            restartCycle();
            assertRestoredFromTimeline("lv_on_mv");
            assertQuery(VIEW_ROWS_QUERY).noLeakCheck().expectSize().returns(grownRows);
            assertViewActive();
        });
    }

    @Test
    public void testNewestBucketsLeavingTheMatViewThroughAHeadMissRebuildRestoreFromTheTimeline() throws Exception {
        // A base correction rewrites the newest three one-minute buckets of the mat view: one keeps
        // a new sum, and the two above it lose every row the mat view's filter keeps, so its
        // incremental refresh replaces their range with one bucket row. The live view seals a root
        // every 29 rows, so the root below the correction sits half an hour under it, and a rebuild
        // warming its one-minute frame from a minute below the correction is the cheaper repair. Its
        // output ends below the root at the newest bucket it spliced: the repair truncates at the
        // new frontier and seals a root there. Each restart restores from it.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 29);
        assertMemoryLeak(() -> {
            createMinuteRangeViewOverMatView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                departNewestBucketsThroughAHeadMissRebuild(job);
            }
            assertMinuteRangeViewMatchesRecompute();
            for (int restart = 0; restart < 2; restart++) {
                restartCycle();
                assertRestoredFromTimeline("lv_on_mv");
                assertMinuteRangeViewMatchesRecompute();
                assertViewActive();
            }
        });
    }

    @Test
    public void testNewestBucketsLeavingTheMatViewThenRefreshesWithoutRowsRestoreFromTheTimeline() throws Exception {
        // The departure of the case above leaves the view's newest row at 00:57, with the newest
        // timestamp the view has seen still at 00:59. Base rows the mat view's filter drops then
        // make its incremental refreshes commit buckets without a row, which the view consumes
        // with no row through its window, each past the duration cadence: the head stays on the
        // root at 00:57 and moves its base coordinate to each of them, and no root seals 00:59,
        // above the newest row the view holds. Each restart restores from the root at 00:57, and
        // the refreshes without rows between the two publish without a failure.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 29);
        assertMemoryLeak(() -> {
            createMinuteRangeViewOverMatView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = departNewestBucketsThroughAHeadMissRebuild(job);
                Assert.assertEquals(ts("2026-01-01T00:57:00.000000Z"), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(ts("2026-01-01T00:59:00.000000Z"), instance.getLatestSeenTs());
                refreshMatViewWithoutRowsPastTheCadence(job, instance, 1);
                assertCheckpointLadder(instance, false, DEPARTED_BUCKETS_LADDER);
                Assert.assertEquals(0, instance.getCheckpointSealFailures());
                Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT));
            }
            assertMinuteRangeViewMatchesRecompute();

            restartCycle();
            assertRestoredFromTimeline("lv_on_mv");
            assertMinuteRangeViewMatchesRecompute();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv_on_mv");
                Assert.assertNotNull(instance);
                Assert.assertEquals(ts("2026-01-01T00:57:00.000000Z"), instance.getLatestSeenTs());
                refreshMatViewWithoutRowsPastTheCadence(job, instance, 4);
                assertCheckpointLadder(instance, false, DEPARTED_BUCKETS_LADDER);
                Assert.assertEquals(0, instance.getCheckpointSealFailures());
                Assert.assertEquals(0, instance.getCheckpointTimelineResets());
            }
            assertMinuteRangeViewMatchesRecompute();

            restartCycle();
            assertRestoredFromTimeline("lv_on_mv");
            assertMinuteRangeViewMatchesRecompute();
            assertViewActive();
        });
    }

    @Test
    public void testMatViewRefreshesBelowTheDepartedBucketsMoveTheHead() throws Exception {
        // The departure of the cases above leaves the view's newest row at 00:57, with the newest
        // timestamp the view has seen still at 00:59. Base rows the mat view's filter drops then land
        // in the bucket at 00:58, so each incremental refresh replaces that bucket's range with no
        // row, below the newest timestamp the view has seen: a resume from the root at 00:57 that
        // emits nothing and splices at the refresh's commit. No clock step, so no cycle without
        // rows comes due. The head still names each refresh's commit, and each re-stamp sweeps what
        // the splice before it superseded.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 29);
        assertMemoryLeak(() -> {
            createMinuteRangeViewOverMatView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = departNewestBucketsThroughAHeadMissRebuild(job);
                Assert.assertEquals(ts("2026-01-01T00:59:00.000000Z"), instance.getLatestSeenTs());
                int files = -1;
                long entries = -1;
                for (int i = 0; i < 6; i++) {
                    final long consumedBefore = instance.getStateReader().getLvConsumedSeqTxn();
                    refreshMatViewAndView(job, "INSERT INTO base (ts, k, v) VALUES ('2026-01-01T00:58:1" + i + ".000000Z', 'b', -1.0)");
                    final long consumed = instance.getStateReader().getLvConsumedSeqTxn();
                    Assert.assertTrue("the view consumes the mat view's refresh", consumed > consumedBefore);
                    Assert.assertEquals(consumed, instance.getHeadCheckpointBaseSeqTxn());
                    Assert.assertEquals(ts("2026-01-01T00:57:00.000000Z"), instance.getHeadCheckpointMaxTs());
                    Assert.assertEquals(i + 1, job.getStatsForTest().get(LiveViewRefreshStats.SPLICE_ZERO_ROW_KEEP_COUNT));
                    if (i == 1) {
                        files = checkpointFileCount(instance);
                        entries = catalogueSize(instance);
                    } else if (i > 1) {
                        Assert.assertEquals(files, checkpointFileCount(instance));
                        Assert.assertEquals(entries, catalogueSize(instance));
                    }
                }
                assertCheckpointLadder(instance, false, DEPARTED_BUCKETS_LADDER);
                Assert.assertEquals(0, instance.getCheckpointSealFailures());
                assertCatalogueMatchesDisk(instance);
            }
            assertMinuteRangeViewMatchesRecompute();

            restartCycle();
            assertRestoredFromTimeline("lv_on_mv");
            assertMinuteRangeViewMatchesRecompute();
            assertViewActive();
        });
    }

    @Test
    public void testBaseDropDuringRefreshDefersExpiryPreflight() throws Exception {
        // DROP removes the base's name before it invalidates the base's live views. A refresh that starts in
        // between cannot resolve the base, so the expiry preflight defers it, and the drop invalidates the
        // view right afterwards. The worker then acknowledges the base notification as it does after any
        // refresh, which reopens event-driven refresh for that base name: a table later created under the
        // same name gets its commits queued again.
        assertMemoryLeak(() -> {
            createExpirySourceAndDependents();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base VALUES ('2026-01-01T00:00:00Z', 'a', 1.0)");
                drainWalAndMatViewQueues();
                driveRefreshToQuiescence(job);
                final LiveViewInstance dep = engine.getLiveViewRegistry().getViewInstance("dep");

                // A second commit leaves dep behind its source and queues a refresh task for source.
                execute("INSERT INTO base VALUES ('2026-01-01T00:01:00Z', 'a', 3.0)");
                drainWalAndMatViewQueues();

                // The hook fires inside DROP, after the name is gone and before the live views are
                // invalidated. It also fires at other mint points, so it runs the refresh only once, and
                // only inside that window.
                final AtomicBoolean isRefreshRun = new AtomicBoolean();
                final AtomicReference<Throwable> escaped = new AtomicReference<>();
                CairoEngine.setRoleSwitchMintObserver(() -> {
                    if (engine.getTableTokenIfExists("source") == null && isRefreshRun.compareAndSet(false, true)) {
                        try {
                            job.processNotificationsForTest();
                        } catch (Throwable th) {
                            escaped.set(th);
                        }
                    }
                });
                try {
                    execute("DROP MATERIALIZED VIEW source");
                } finally {
                    CairoEngine.setRoleSwitchMintObserver(null);
                }
                Assert.assertTrue("the refresh must run inside the drop window", isRefreshRun.get());
                Assert.assertNull("the refresh must not throw out of the job", escaped.get());
                Assert.assertTrue(dep.isInvalid());
                Assert.assertEquals(0, dep.getRefreshFaultCount());

                final LiveViewRefreshTask task = new LiveViewRefreshTask();
                while (engine.getLiveViewStateStore().tryDequeueRefreshTask(task)) {
                    task.clear();
                }
                execute("CREATE MATERIALIZED VIEW source AS (SELECT * FROM base)");
                drainWalAndMatViewQueues();
                boolean isSourceQueued = false;
                while (engine.getLiveViewStateStore().tryDequeueRefreshTask(task)) {
                    isSourceQueued |= "source".equals(task.baseTableToken.getTableName());
                }
                Assert.assertTrue("a commit to the recreated source must queue a refresh task", isSourceQueued);
            }
        });
    }

    @Test
    public void testExpiryPolicyOnMatViewBaseInvalidatesLiveViewOnNextTurn() throws Exception {
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, k SYMBOL, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE MATERIALIZED VIEW mvbase AS (SELECT * FROM base)");
            drainWalAndMatViewQueues(engine);
            execute("CREATE LIVE VIEW lv_on_mv FLUSH EVERY 100ms START FROM NOW AS "
                    + "SELECT ts, k, sum(v) OVER (PARTITION BY k ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s FROM mvbase");

            execute("ALTER MATERIALIZED VIEW mvbase SET EXPIRE ROWS WHEN v < 0");
            drainWalAndMatViewQueues(engine);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }

            assertQuery("SELECT view_status, invalidation_reason FROM live_views() WHERE view_name = 'lv_on_mv'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("view_status\tinvalidation_reason\n"
                            + "invalid\tcannot materialize view 'lv_on_mv': source materialized view 'mvbase' has an active EXPIRE ROWS policy\n");
        });
    }

    @Test
    public void testExpiryPolicySequencedBeforeApplyInvalidatesBeforeNewRows() throws Exception {
        assertMemoryLeak(() -> {
            createExpirySourceAndDependents();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base VALUES ('2026-01-01T00:00:00Z', 'a', 1.0)");
                drainWalAndMatViewQueues();
                driveRefreshToQuiescence(job);
                final LiveViewInstance dep = engine.getLiveViewRegistry().getViewInstance("dep");
                execute("ALTER MATERIALIZED VIEW source SET EXPIRE ROWS WHEN v < 2");
                Assert.assertTrue(job.processNotificationsForTest());
                Assert.assertFalse(dep.isInvalid());
                final long processedBefore = dep.getLastProcessedSeqTxn();
                drainWalQueue();
                // A caught-up idle view need not revalidate merely because apply finished.
                driveRefreshToQuiescence(job);
                Assert.assertFalse(dep.isInvalid());
                assertQuery("SELECT count() FROM source").noLeakCheck().noRandomAccess().expectSize()
                        .returns("count\n0\n");
                execute("INSERT INTO base VALUES ('2026-01-01T00:01:00Z', 'a', 3.0)");
                drainWalAndMatViewQueues();
                Assert.assertTrue(job.processNotificationsForTest());
                Assert.assertTrue(dep.isInvalid());
                assertExpiryConflict();
                Assert.assertEquals(processedBefore, dep.getLastProcessedSeqTxn());
                assertQuery("SELECT s FROM dep").noLeakCheck().expectSize().returns("s\n1.0\n");
                assertNoRefreshFaults("dep");
            }
        });
    }

    @Test
    public void testMatViewFullRefreshInvalidatesDependentLiveView() throws Exception {
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, k SYMBOL, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE MATERIALIZED VIEW mvbase AS (" +
                    "SELECT ts, k, avg(v) AS av FROM base SAMPLE BY 1h) PARTITION BY DAY");
            drainWalAndMatViewQueues(engine);

            // A bounded frame makes the stale-accumulator half of the corruption visible: the
            // ring still holds the deleted mat-view rows after the rebuild.
            final String viewSql = "SELECT ts, k, avg(av) OVER (" +
                    "PARTITION BY k ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS a FROM mvbase";
            // Create over the still-empty mat view so the live view consumes its commits
            // incrementally rather than seeding.
            setCurrentMicros(0L);
            execute("CREATE LIVE VIEW lv_on_mv FLUSH EVERY 100ms START FROM NOW AS " + viewSql);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, k, v) VALUES " +
                        "('2026-01-01T00:00:00.000000Z', 'a', 1.0), " +
                        "('2026-01-01T01:00:00.000000Z', 'a', 2.0), " +
                        "('2026-01-01T02:00:00.000000Z', 'a', 3.0)");
                drainWalAndMatViewQueues(engine);
                driveRefreshToQuiescence(job);

                // The view tracked the mat view incrementally, and did so without faulting.
                assertQuery("lv_on_mv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tk\ta\n" +
                        "2026-01-01T00:00:00.000000Z\ta\t1.0\n" +
                        "2026-01-01T01:00:00.000000Z\ta\t1.5\n" +
                        "2026-01-01T02:00:00.000000Z\ta\t2.0\n");
                assertNoRefreshFaults("lv_on_mv");

                // The operator rebuilds the mat view over a now-truncated base. The mat view
                // TRUNCATEs itself and re-materialises a single bucket.
                execute("TRUNCATE TABLE base");
                execute("INSERT INTO base (ts, k, v) VALUES ('2026-01-01T05:00:00.000000Z', 'a', 100.0)");
                drainWalQueue();
                execute("REFRESH MATERIALIZED VIEW mvbase FULL");
                drainWalAndMatViewQueues(engine);

                driveRefreshToQuiescence(job);
            }
            drainWalQueue();

            assertQuery("mvbase ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tk\tav\n" +
                    "2026-01-01T05:00:00.000000Z\ta\t100.0\n");

            // Pre-fix the view stayed ACTIVE and served four rows: the three buckets the
            // rebuild deleted, plus a 05:00 row of 26.5 - avg(1, 2, 3, 100), the bounded
            // frame's ring still holding the deleted rows. It must invalidate instead.
            assertQuery("SELECT view_name, view_status, invalidation_reason FROM live_views()")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("view_name\tview_status\tinvalidation_reason\n" +
                            "lv_on_mv\tinvalid\tbase materialized view was rebuilt\n");
        });
    }

    /**
     * Same rebuild as {@link #testMatViewFullRefreshInvalidatesDependentLiveView}, but with the
     * refresh worker reaching the mat view's WAL before the apply job does.
     * <p>
     * The two run on different worker pools and nothing serialises them: the sequencer fans the
     * refresh notification out at commit time, and {@code drainBaseWal} walks the base's raw
     * sequencer log with no apply gate (unlike the applied-base and O3 paths, which call
     * {@code ensureBaseApplied}). So the worker can consume the rebuild commits while the TRUNCATE
     * that precedes them is still un-applied. An invalidation hung off the apply job is a lagging
     * backstop, not a fence.
     */
    @Test
    public void testMatViewFullRefreshInvalidatesWhenRefreshRunsBeforeApply() throws Exception {
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, k SYMBOL, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE MATERIALIZED VIEW mvbase AS (" +
                    "SELECT ts, k, avg(v) AS av FROM base SAMPLE BY 1h) PARTITION BY DAY");
            drainWalAndMatViewQueues(engine);

            final String viewSql = "SELECT ts, k, avg(av) OVER (" +
                    "PARTITION BY k ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS a FROM mvbase";
            setCurrentMicros(0L);
            execute("CREATE LIVE VIEW lv_on_mv FLUSH EVERY 100ms START FROM NOW AS " + viewSql);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, k, v) VALUES " +
                        "('2026-01-01T00:00:00.000000Z', 'a', 1.0), " +
                        "('2026-01-01T01:00:00.000000Z', 'a', 2.0), " +
                        "('2026-01-01T02:00:00.000000Z', 'a', 3.0)");
                drainWalAndMatViewQueues(engine);
                driveRefreshToQuiescence(job);

                execute("TRUNCATE TABLE base");
                execute("INSERT INTO base (ts, k, v) VALUES ('2026-01-01T05:00:00.000000Z', 'a', 100.0)");
                drainWalQueue();
                execute("REFRESH MATERIALIZED VIEW mvbase FULL");

                // Only the mat-view refresh job runs: the TRUNCATE and the re-materialised rows
                // are now committed to mvbase's WAL but NOT applied to its table.
                drainMatViewQueue(engine);

                // Give the refresh worker its shot at the sequencer ahead of the apply job, with
                // the FLUSH EVERY cadence due so the cycle also flushes its lead to the LV's own
                // disk tier (flushLead applies the LV WAL inline). drainJob, not
                // driveRefreshToQuiescence: the latter drains the WAL queue first, which would
                // apply the TRUNCATE and hide the interleaving.
                setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                drainJob(job);

                // The worker must have stopped AT the TRUNCATE, not walked past it: the view still
                // serves exactly its pre-rebuild rows. Pre-fix it emitted a 4th row here -
                // 2026-01-01T05:00:00Z / 26.5, i.e. avg(1, 2, 3, 100), the bounded frame's ring
                // still holding the three rows the rebuild deleted - and served it while reporting
                // ACTIVE, since the apply job had not yet reached the TRUNCATE to invalidate.
                assertQuery("lv_on_mv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tk\ta\n" +
                        "2026-01-01T00:00:00.000000Z\ta\t1.0\n" +
                        "2026-01-01T01:00:00.000000Z\ta\t1.5\n" +
                        "2026-01-01T02:00:00.000000Z\ta\t2.0\n");

                // The apply job only now reaches the TRUNCATE, and invalidates.
                drainWalQueue();
            }
            drainWalQueue();

            // The view must not have emitted derived rows for the re-materialised base rows over
            // accumulators still holding pre-rebuild state.
            assertQuery("SELECT view_name, view_status, invalidation_reason FROM live_views()")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("view_name\tview_status\tinvalidation_reason\n" +
                            "lv_on_mv\tinvalid\tbase materialized view was rebuilt\n");
            assertQuery("lv_on_mv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tk\ta\n" +
                    "2026-01-01T00:00:00.000000Z\ta\t1.0\n" +
                    "2026-01-01T01:00:00.000000Z\ta\t1.5\n" +
                    "2026-01-01T02:00:00.000000Z\ta\t2.0\n");
        });
    }

    @Test
    public void testPendingDropRetriesWhileUnrelatedTrafficContinues() throws Exception {
        assertPendingPolicyRetry(true, false);
    }

    @Test
    public void testPendingPolicyRetryPacing() throws Exception {
        assertPendingPolicyRetry(true, true);
    }

    @Test
    public void testPendingSetRetriesWhileUnrelatedTrafficContinues() throws Exception {
        assertPendingPolicyRetry(false, false);
    }

    @Test
    public void testPlainBaseTruncateStillFreezesAndContinues() throws Exception {
        // The mat-view carve-out above must not disturb freeze-and-continue for a plain
        // base: a user TRUNCATE retires settled data, and the view keeps its emitted rows,
        // its accumulators and its ACTIVE status. This is the contract
        // LiveViewFuzzTest#testFuzzRemovalFreezeAndContinue fuzzes.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, k SYMBOL, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final String viewSql = "SELECT ts, k, sum(v) OVER (" +
                    "PARTITION BY k ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s FROM base";
            setCurrentMicros(0L);
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " + viewSql);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, k, v) VALUES " +
                        "('2026-01-01T00:00:00.000000Z', 'a', 1.0), " +
                        "('2026-01-01T01:00:00.000000Z', 'a', 2.0)");
                drainWalQueue();
                driveRefreshToQuiescence(job);

                execute("TRUNCATE TABLE base");
                drainWalQueue();
                driveRefreshToQuiescence(job);

                // Forward ingestion after the removal continues the accumulation as if the
                // removed rows were still there: 1 + 2 + 10 = 13.
                execute("INSERT INTO base (ts, k, v) VALUES ('2026-01-01T02:00:00.000000Z', 'a', 10.0)");
                drainWalQueue();
                driveRefreshToQuiescence(job);
            }
            drainWalQueue();

            assertQuery("SELECT view_name, view_status FROM live_views()")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("view_name\tview_status\n" +
                            "lv\tactive\n");
            assertQuery("lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tk\ts\n" +
                    "2026-01-01T00:00:00.000000Z\ta\t1.0\n" +
                    "2026-01-01T01:00:00.000000Z\ta\t3.0\n" +
                    "2026-01-01T02:00:00.000000Z\ta\t13.0\n");
            assertNoRefreshFaults("lv");
        });
    }

    /**
     * Commits {@link #BUCKET_REMOVING_CORRECTION} and refreshes the mat view over it, without the
     * live view consuming the refresh, so the refresh stays in the backlog a restart's rebuild
     * classifies.
     */
    private void applyBucketRemovingRefreshWithoutTheView() throws Exception {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv_on_mv");
        Assert.assertNotNull(instance);
        final long processedBefore = instance.getLastProcessedSeqTxn();
        execute(BUCKET_REMOVING_CORRECTION);
        drainWalAndMatViewQueues(engine);
        assertQuery("SELECT count() FROM mvbase")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("""
                        count
                        5
                        """);
        Assert.assertEquals("the view must not consume the refresh", processedBefore, instance.getLastProcessedSeqTxn());
    }

    private void assertViewActive() throws Exception {
        assertQuery("SELECT view_name, view_status FROM live_views()")
                .noLeakCheck()
                .noRandomAccess()
                .returns("""
                        view_name\tview_status
                        lv_on_mv\tactive
                        """);
    }

    private void assertMinuteRangeViewMatchesRecompute() throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + MINUTE_RANGE_VIEW_SQL + ") ORDER BY k, ts",
                "SELECT ts, k, s FROM lv_on_mv ORDER BY k, ts",
                LOG,
                true
        );
        assertNoRefreshFaults("lv_on_mv");
    }

    // Active, with no recovery phase and no reason: the view was neither stopped nor left waiting.
    private void assertTtlViewActive() throws Exception {
        assertQuery("SELECT view_status, checkpoint_recovery_phase, checkpoint_recovery_reason FROM live_views() WHERE view_name = 'lv_on_mv'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("""
                        view_status\tcheckpoint_recovery_phase\tcheckpoint_recovery_reason
                        active\t\t
                        """);
    }

    private void assertTtlViewMatchesRecompute() throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + TTL_VIEW_SQL + ") ORDER BY ts",
                TTL_VIEW_ROWS_QUERY + " ORDER BY ts",
                LOG,
                true
        );
    }

    private void assertTtlViewRows(String expected) throws Exception {
        assertQuery(TTL_VIEW_ROWS_QUERY)
                .noLeakCheck()
                .timestamp("ts")
                .expectSize()
                .returns(expected);
    }

    private void assertViewMatchesRecompute() throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + ANCHORED_RECOMPUTE_SQL + ") ORDER BY k, ts",
                VIEW_ROWS_QUERY,
                LOG,
                true
        );
        assertNoRefreshFaults("lv_on_mv");
    }

    // The two days the view holds once the rebuild has followed the refresh that emptied
    // a@2026-01-01T02:00.
    private void assertViewRowsAfterTheBucketRemoval() throws Exception {
        assertViewMatchesRecompute();
        assertQuery(VIEW_ROWS_QUERY)
                .noLeakCheck()
                .expectSize()
                .returns("""
                        ts\tk\ts\tc
                        2026-01-01T00:00:00.000000Z\ta\t1.0\t1
                        2026-01-01T01:00:00.000000Z\ta\t3.0\t2
                        2026-01-02T00:00:00.000000Z\ta\t16.0\t1
                        2026-01-01T01:00:00.000000Z\tb\t8.0\t1
                        2026-01-02T01:00:00.000000Z\tb\t32.0\t1
                        """);
    }

    private void createMinuteRangeViewOverMatView() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, k SYMBOL, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, k)");
        execute("CREATE MATERIALIZED VIEW mvbase AS (SELECT ts, k, sum(v) AS sv FROM base WHERE v > 0 SAMPLE BY 1m) PARTITION BY DAY");
        drainWalAndMatViewQueues(engine);
        setCurrentMicros(0L);
        execute("CREATE LIVE VIEW lv_on_mv FLUSH EVERY 100ms START FROM NOW AS " + MINUTE_RANGE_VIEW_SQL);
    }

    /**
     * A bucket a day below, then the buckets from 00:00 to 00:59 in two commits, each sealing a root
     * at its newest bucket under a cadence of 29 rows. A base correction then rewrites the newest
     * three buckets: 00:57 keeps a new sum, and 00:58 and 00:59 lose every row the mat view's filter
     * keeps, which its incremental refresh commits as one REPLACE_RANGE holding the bucket at 00:57.
     * The root below the correction sits half an hour under it, so the live view repairs it with a
     * rebuild warming its one-minute frame from a minute below the correction, whose output ends
     * below the root it spliced at 00:59: the repair truncates at the new frontier 00:57 and seals a
     * root there.
     */
    private LiveViewInstance departNewestBucketsThroughAHeadMissRebuild(LiveViewRefreshJob job) throws Exception {
        refreshMatViewAndView(job, "INSERT INTO base (ts, k, v) VALUES ('2025-12-31T23:55:10.000000Z', 'a', 0.5)");
        refreshMatViewAndView(job, "INSERT INTO base (ts, k, v) VALUES " + minuteRowsOfA(0, 30, "1.0"));
        refreshMatViewAndView(job, "INSERT INTO base (ts, k, v) VALUES " + minuteRowsOfA(31, 59, "1.0"));
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv_on_mv");
        Assert.assertNotNull(instance);
        assertCheckpointLadder(instance, false, """
                2025-12-31T23:55:00.000000Z -> 1
                2026-01-01T00:30:00.000000Z -> 32
                2026-01-01T00:59:00.000000Z -> 61
                """);
        final long headMissRepairsBefore = countHeadMissRepairs(job);
        refreshMatViewAndView(
                job,
                "INSERT INTO base (ts, k, v) VALUES ('2026-01-01T00:57:00.000000Z', 'a', 5.0), "
                        + "('2026-01-01T00:58:00.000000Z', 'a', -1.0), ('2026-01-01T00:59:00.000000Z', 'a', -1.0)"
        );
        Assert.assertEquals("the head-miss rebuild repairs the departure", headMissRepairsBefore + 1, countHeadMissRepairs(job));
        Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_FRONTIER_TRUNCATE_COUNT));
        assertCheckpointLadder(instance, false, DEPARTED_BUCKETS_LADDER);
        return instance;
    }

    private void refreshMatViewAndView(LiveViewRefreshJob job, String insert) throws Exception {
        execute(insert);
        drainWalAndMatViewQueues(engine);
        driveRefreshToQuiescence(job);
    }

    /**
     * Three base rows the mat view's filter drops, a minute apart from {@code 01:0fromMinute:10},
     * each committed past the duration cadence. The mat view's incremental refresh commits each one's
     * bucket with no row, which the live view consumes with no row through its window: the first
     * cycle over the refresh's commits comes due and moves the head's base coordinate to a commit of
     * that refresh, on the root at the view's newest row 00:57.
     */
    private void refreshMatViewWithoutRowsPastTheCadence(LiveViewRefreshJob job, LiveViewInstance instance, int fromMinute) throws Exception {
        final TableToken matViewToken = engine.verifyTableName("mvbase");
        for (int minute = fromMinute; minute < fromMinute + 3; minute++) {
            final long consumedBefore = instance.getStateReader().getLvConsumedSeqTxn();
            setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
            refreshMatViewAndView(job, "INSERT INTO base (ts, k, v) VALUES ('2026-01-01T01:0" + minute + ":10.000000Z', 'a', -1.0)");
            final long matViewSeqTxn = engine.getTableSequencerAPI().lastTxn(matViewToken);
            Assert.assertTrue("the mat view's refresh commits", matViewSeqTxn > consumedBefore);
            Assert.assertEquals("the view consumes the mat view's refresh", matViewSeqTxn, instance.getStateReader().getLvConsumedSeqTxn());
            Assert.assertEquals(ts("2026-01-01T00:57:00.000000Z"), instance.getHeadCheckpointMaxTs());
            final long headBaseSeqTxn = instance.getHeadCheckpointBaseSeqTxn();
            Assert.assertTrue(
                    "the head's base coordinate must move to a commit of the refresh [consumedBefore=" + consumedBefore
                            + ", headBase=" + headBaseSeqTxn + ", matViewSeqTxn=" + matViewSeqTxn + ']',
                    headBaseSeqTxn > consumedBefore && headBaseSeqTxn <= matViewSeqTxn
            );
        }
    }

    /**
     * Removes the live view's {@code _timeline}, so the restart that follows finds nothing to
     * restore from and rebuilds the view from the applied mat view.
     */
    private void removeTimeline() {
        Assert.assertTrue("the fixture must have published a timeline to remove", timelineFile().delete());
    }

    /**
     * Rebuilds the view registry from disk and drives the first refresh turns, which is where a
     * restart runs its recovery. Returns what the last whole-view rebuild's guard found.
     */
    private LiveViewRebuildRestatementGuard restart() {
        engine.buildViewGraphs();
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveRefreshToQuiescence(job);
            return job.rebuildRestatementGuardForTest();
        }
    }

    private void restartCycle() {
        shutdown();
        restart();
    }

    /**
     * Creates a deduplicated base, a mat view that sums its positive rows an hour at a time,
     * and a live view anchored daily over the mat view, then refreshes two days of base rows
     * through both, a day per commit, so the view seals a timeline of more than one boundary.
     */
    private void seedAnchoredViewOverMatView() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        execute("CREATE TABLE base (ts TIMESTAMP, k SYMBOL, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, k)");
        execute("CREATE MATERIALIZED VIEW mvbase AS (SELECT ts, k, sum(v) AS sv FROM base WHERE v > 0 SAMPLE BY 1h) PARTITION BY DAY");
        drainWalAndMatViewQueues(engine);
        setCurrentMicros(0L);
        execute("CREATE LIVE VIEW lv_on_mv FLUSH EVERY 100ms START FROM NOW AS " + ANCHORED_VIEW_SQL);
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            refreshMatViewAndView(job, DAY_ONE_ROWS);
            refreshMatViewAndView(job, DAY_TWO_ROWS);
        }
        assertViewMatchesRecompute();
    }

    /**
     * Creates a mat view with a one-day TTL over hourly buckets and a live view over it, then
     * refreshes three days of base rows through both, a day per commit. TTL reads min(newest row,
     * wall clock), so the clock moves to 2026-01-04 before the third commit, whose refresh evicts
     * the mat view's 2026-01-01 partition. The live view keeps that day's row, as incremental
     * refresh keeps every row its base loses.
     */
    private void seedViewOverTtlMatView() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        execute("CREATE TABLE base (ts TIMESTAMP, k SYMBOL, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE MATERIALIZED VIEW mvbase AS (SELECT ts, k, sum(v) AS sv FROM base SAMPLE BY 1h) PARTITION BY DAY TTL 1 DAY");
        drainWalAndMatViewQueues(engine);
        setCurrentMicros(0L);
        execute("CREATE LIVE VIEW lv_on_mv FLUSH EVERY 100ms START FROM NOW AS " + TTL_VIEW_SQL);
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            refreshMatViewAndView(job, "INSERT INTO base (ts, k, v) VALUES ('2026-01-01T00:10:00.000000Z', 'a', 1.0)");
            refreshMatViewAndView(job, "INSERT INTO base (ts, k, v) VALUES ('2026-01-02T00:10:00.000000Z', 'a', 2.0)");
            setCurrentMicros(ts("2026-01-04T00:00:00.000000Z"));
            refreshMatViewAndView(job, "INSERT INTO base (ts, k, v) VALUES ('2026-01-03T05:10:00.000000Z', 'a', 4.0)");
        }
        assertQuery("SELECT ts, k, sv FROM mvbase")
                .noLeakCheck()
                .timestamp("ts")
                .expectSize()
                .returns("""
                        ts\tk\tsv
                        2026-01-02T00:00:00.000000Z\ta\t2.0
                        2026-01-03T05:00:00.000000Z\ta\t4.0
                        """);
        assertTtlViewRows("""
                ts\tk\tsv\ts
                2026-01-01T00:00:00.000000Z\ta\t1.0\t1.0
                2026-01-02T00:00:00.000000Z\ta\t2.0\t3.0
                2026-01-03T05:00:00.000000Z\ta\t4.0\t7.0
                """);
        assertNoRefreshFaults("lv_on_mv");
    }

    /**
     * Releases everything that maps the view's and its bases' files, the way a stopped process
     * would, so the next {@link #restart()} starts from disk.
     */
    private void shutdown() {
        engine.getLiveViewRegistry().clear();
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        engine.releaseInactive();
    }

    private File timelineFile() {
        final TableToken viewToken = engine.verifyTableName("lv_on_mv");
        final File checkpointsDir = new File(
                new File(engine.getConfiguration().getDbRoot(), viewToken.getDirName()),
                LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME
        );
        return new File(checkpointsDir, LiveViewCheckpointLayout.TIMELINE_FILE_NAME);
    }

    private void assertExpiryConflict() throws Exception {
        assertQuery("SELECT invalidation_reason FROM live_views() WHERE view_name = 'dep'")
                .noLeakCheck().noRandomAccess().returns("""
                        invalidation_reason
                        cannot materialize view 'dep': source materialized view 'source' has an active EXPIRE ROWS policy
                        """);
    }

    private void assertPendingPolicyRetry(boolean isDrop, boolean isCheckPacing) throws Exception {
        assertMemoryLeak(() -> {
            createExpirySourceAndDependents();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base VALUES ('2026-01-01T00:00:00Z', 'a', 1.0)");
                drainWalAndMatViewQueues();
                driveRefreshToQuiescence(job);
                final LiveViewInstance dep = engine.getLiveViewRegistry().getViewInstance("dep");
                final TableToken source = engine.verifyTableName("source");
                final long processedBefore = dep.getLastProcessedSeqTxn();
                if (isDrop) {
                    execute("INSERT INTO base VALUES ('2026-01-01T00:01:00Z', 'a', 3.0)");
                    drainWalAndMatViewQueues();
                    execute("ALTER MATERIALIZED VIEW source SET EXPIRE ROWS WHEN v < 2");
                    drainWalQueue();
                }
                final CountDownLatch swapBarrierReached = new CountDownLatch(1);
                final CountDownLatch resumeSwap = new CountDownLatch(1);
                final AtomicReference<Throwable> applyError = new AtomicReference<>();
                execute(isDrop ? "ALTER MATERIALIZED VIEW source DROP EXPIRE"
                        : "ALTER MATERIALIZED VIEW source SET EXPIRE ROWS WHEN v < 2");
                TableWriter.setExpiryMetaSwapBarrier(() -> {
                    swapBarrierReached.countDown();
                    try {
                        Assert.assertTrue("timed out resuming policy publication", resumeSwap.await(30, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(e);
                    }
                });
                // Apply and refresh must not share WAL apply's carrier-local transaction cursor.
                final Thread applyThread = new Thread(() -> {
                    try {
                        drainWalQueue();
                    } catch (Throwable th) {
                        applyError.set(th);
                    } finally {
                        Path.clearThreadLocals();
                    }
                }, "expire-apply");
                try {
                    applyThread.start();
                    Assert.assertTrue("writer did not reach policy publication", swapBarrierReached.await(30, TimeUnit.SECONDS));
                    Assert.assertTrue(engine.getMetadataCache().isExpiryPolicyUpdatePending(source));
                    Assert.assertTrue(job.processNotificationsForTest());
                    Assert.assertEquals(processedBefore, dep.getLastProcessedSeqTxn());
                    Assert.assertFalse(dep.isInvalid());
                    assertNoRefreshFaults("dep");
                    if (isCheckPacing) {
                        assertPendingRetryPacing(job, dep, processedBefore);
                    }
                } finally {
                    resumeSwap.countDown();
                    applyThread.join(TimeUnit.SECONDS.toMillis(30));
                    TableWriter.setExpiryMetaSwapBarrier(null);
                }
                Assert.assertFalse("policy apply did not finish", applyThread.isAlive());
                Assert.assertNull(applyError.get());
                Assert.assertFalse(engine.getTableSequencerAPI().getTxnTracker(source).isSuspended());
                Assert.assertFalse(engine.getMetadataCache().isExpiryPolicyUpdatePending(source));
                Assert.assertTrue(engine.getTableSequencerAPI().getTxnTracker(source).getWriterTxn() > processedBefore);
                // Every turn has a real unrelated commit. Retry must not depend on a quiet turn.
                for (int i = 1; i <= 4; i++) {
                    setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                    insertNoiseAndRun(job);
                }
                if (isDrop) {
                    Assert.assertFalse(dep.isInvalid());
                    Assert.assertEquals(engine.getTableSequencerAPI().getTxnTracker(source).getWriterTxn(),
                            dep.getLastProcessedSeqTxn());
                    assertQuery("SELECT count() FROM source").noLeakCheck().noRandomAccess().expectSize()
                            .returns("count\n2\n");
                    assertQuery("SELECT s FROM dep ORDER BY ts").noLeakCheck().expectSize()
                            .returns("s\n1.0\n4.0\n");
                } else {
                    Assert.assertTrue("pending SET must invalidate within the busy retry bound", dep.isInvalid());
                    assertExpiryConflict();
                    Assert.assertEquals(processedBefore, dep.getLastProcessedSeqTxn());
                    assertQuery("SELECT s FROM dep").noLeakCheck().expectSize().returns("s\n1.0\n");
                }
                // SET + DROP changes the source's metadata version, but no turn reopens the source
                // with the old plan: each source refresh replaces only the range its base write
                // touched, above the view's frontier, so the view never replays. Pending turns above
                // must count no fault either.
                Assert.assertEquals(0, dep.getRefreshFaultCount());
                Assert.assertEquals(0, dep.getFlushRetryCount());
                assertNoRefreshFaults("noise_lv");
            }
        });
    }

    private void assertPendingRetryPacing(LiveViewRefreshJob job, LiveViewInstance dep, long processedBefore) throws Exception {
        final AtomicInteger scans = new AtomicInteger();
        try {
            for (int interval = 1; interval <= 3; interval++) {
                setCurrentMicros(currentMicros + 1_000_000L);
                hooks(job).setSimulateBaseCommitBetweenAheadGuardReads(scans::incrementAndGet);
                insertNoiseAndRun(job);
                Assert.assertEquals(interval, scans.get());
                // A due scan that cannot refresh must still consume its scheduling slot.
                hooks(job).setSimulateBaseCommitBetweenAheadGuardReads(scans::incrementAndGet);
                for (int turn = 0; turn < 100; turn++) {
                    insertNoiseAndRun(job);
                }
                setCurrentMicros(currentMicros + 999_999L);
                insertNoiseAndRun(job);
                Assert.assertEquals(interval, scans.get());
                Assert.assertEquals(processedBefore, dep.getLastProcessedSeqTxn());
                Assert.assertFalse(dep.isInvalid());
                assertNoRefreshFaults("dep");
            }
        } finally {
            hooks(job).setSimulateBaseCommitBetweenAheadGuardReads(null);
        }
        // With no notification work, a pending refresh reports no work rather than spinning.
        drainJob(job);
        Assert.assertFalse(job.processNotificationsForTest());
    }

    private void createExpirySourceAndDependents() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, k SYMBOL, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE MATERIALIZED VIEW source AS (SELECT * FROM base)");
        execute("CREATE TABLE noise (ts TIMESTAMP, k SYMBOL, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
        drainWalAndMatViewQueues();
        setCurrentMicros(0L);
        execute("""
                CREATE LIVE VIEW dep FLUSH EVERY 100ms START FROM NOW AS
                SELECT ts, k, sum(v) OVER (PARTITION BY k ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s
                FROM source
                """);
        execute("""
                CREATE LIVE VIEW noise_lv FLUSH EVERY 100ms START FROM NOW AS
                SELECT ts, k, sum(v) OVER (PARTITION BY k ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s
                FROM noise
                """);
    }

    private void insertNoiseAndRun(LiveViewRefreshJob job) throws Exception {
        execute("INSERT INTO noise VALUES (" + currentMicros + ", 'a', 1.0)");
        Assert.assertTrue(job.processNotificationsForTest());
    }
}
