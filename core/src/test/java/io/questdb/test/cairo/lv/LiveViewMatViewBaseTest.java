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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointRecoveryPhase;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRebuildRestatementGuard;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewRefreshStats;
import io.questdb.std.Files;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

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
 * the restart restore and the restatement guard of a rebuild.
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
    // The line a rebuild logs when a backlog REPLACE_RANGE above the view's earliest row limits the
    // restatement guard to the view's rows below it.
    private static final String BAND_NARROWING_LINE = "live view rebuild from the applied base checks the restatement "
            + "guard only below a range its backlog replaces [view=lv_on_mv, ";
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
    // What the view over the mat view with a one-day TTL holds once seedViewOverTtlMatView has
    // refreshed its three days of rows: the mat view's TTL evicted 2026-01-01, and the view keeps
    // that day's row.
    private static final String TTL_VIEW_ROWS = """
            ts\tk\tsv\ts
            2026-01-01T00:00:00.000000Z\ta\t1.0\t1.0
            2026-01-02T00:00:00.000000Z\ta\t2.0\t3.0
            2026-01-03T05:00:00.000000Z\ta\t4.0\t7.0
            """;
    // A sum over the twenty rows up to each one, over the mat view with a one-day TTL: every row's
    // sum carries the view's whole history in these cases.
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
        // The backlog of a materialized view may hold a REPLACE_RANGE or a TRUNCATE in any commit
        // the rebuild cannot read, so when its WAL is gone the restatement guard has nothing it
        // may compare (guardChecks=none) and the rebuild follows the mat view. Here the backlog it
        // cannot read is the refresh that dropped a bucket, which a guard comparing the row count
        // would refuse as a loss.
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
            Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_BACKLOG_UNREADABLE, guard.getAbstention());
            capture.assertLogged("live view could not read the rebuild's base backlog [view=lv_on_mv");
            capture.assertLogged("guardChecks=none,");
            capture.assertLogged("live view rebuild from the applied base runs without the restatement guard [view=lv_on_mv, reason=backlog unreadable]");
            capture.assertNotLogged("live view rebuild from the applied base refused");
            assertViewRowsAfterTheBucketRemoval();
            assertViewActive();
        });
    }

    @Test
    public void testARebuildOverAMatViewWithNoTimelineComparesOnlyTheRowsBelowItsRefresh() throws Exception {
        // Every incremental refresh of a materialized view commits a REPLACE_RANGE over the
        // buckets it recomputes, and one that leaves a bucket empty removes a row the live view
        // derived output from. A restart that finds no timeline rebuilds from the applied mat
        // view, which reproduces one row fewer than the view holds. The refresh is in the
        // rebuild's backlog, so the guard must not refuse it as a loss. The refresh replaces
        // [02:00, 03:00) and can change no base row below 02:00, so the guard compares the three
        // rows the view holds below it - which the mat view still holds - instead of standing
        // down: the rebuild follows the mat view as it did when the guard stood down.
        assertMemoryLeak(() -> {
            seedAnchoredViewOverMatView();
            applyBucketRemovingRefreshWithoutTheView();
            shutdown();
            removeTimeline();

            final LogCapture capture = new LogCapture();
            final LiveViewRebuildRestatementGuard guard = restartCapturing(capture);

            assertRebuiltFromAppliedBase("lv_on_mv");
            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv_on_mv");
            Assert.assertNotNull(instance);
            Assert.assertFalse(
                    "the rebuild must follow the mat view: " + instance.getCheckpointRecoveryReason(),
                    instance.isCheckpointRecoveryBlocked()
            );
            Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_NONE, guard.getAbstention());
            Assert.assertEquals(LiveViewRebuildRestatementGuard.VERDICT_NONE, guard.getVerdict());
            // a@00:00, a@01:00 and b@01:00, each reproduced by the rebuild.
            Assert.assertEquals(3, guard.getDurableRows());
            Assert.assertEquals(3, guard.getReproducedRows());
            capture.assertLogged(BAND_NARROWING_LINE + "replaceLo=2026-01-01T02:00:00.000000Z, rows=3]");
            capture.assertNotLogged("live view rebuild from the applied base runs without the restatement guard");
            capture.assertNotLogged("live view could not read the rebuild's base backlog");
            capture.assertNotLogged("live view rebuild from the applied base refused");
            assertViewRowsAfterTheBucketRemoval();
            assertViewActive();
        });
    }

    @Test
    public void testARefreshOfTheViewsEarliestBucketStillStandsTheGuardDown() throws Exception {
        // The correction empties a@00:00, the view's earliest row, so the mat view's refresh
        // replaces [00:00, 01:00) and covers it: no row the view holds sits below every band, and
        // the guard has nothing left it may compare. The rebuild follows the mat view.
        assertMemoryLeak(() -> {
            seedAnchoredViewOverMatView();
            applyBucketRemovingRefreshWithoutTheView("INSERT INTO base (ts, k, v) VALUES ('2026-01-01T00:10:00.000000Z', 'a', -1.0)");
            shutdown();
            removeTimeline();

            final LogCapture capture = new LogCapture();
            final LiveViewRebuildRestatementGuard guard = restartCapturing(capture);

            assertRebuiltFromAppliedBase("lv_on_mv");
            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv_on_mv");
            Assert.assertNotNull(instance);
            Assert.assertFalse(
                    "the rebuild must follow the mat view: " + instance.getCheckpointRecoveryReason(),
                    instance.isCheckpointRecoveryBlocked()
            );
            Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_BACKLOG_MAY_REMOVE, guard.getAbstention());
            capture.assertLogged("live view rebuild from the applied base runs without the restatement guard [view=lv_on_mv, reason=backlog may remove rows]");
            capture.assertNotLogged(BAND_NARROWING_LINE);
            capture.assertNotLogged("live view rebuild from the applied base refused");
            assertViewMatchesRecompute();
            // ANCHOR DAILY restarts each key's sums at midnight: a now starts at 01:00.
            assertQuery(VIEW_ROWS_QUERY)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            ts\tk\ts\tc
                            2026-01-01T01:00:00.000000Z\ta\t2.0\t1
                            2026-01-01T02:00:00.000000Z\ta\t6.0\t2
                            2026-01-02T00:00:00.000000Z\ta\t16.0\t1
                            2026-01-01T01:00:00.000000Z\tb\t8.0\t1
                            2026-01-02T01:00:00.000000Z\tb\t32.0\t1
                            """);
            assertViewActive();
        });
    }

    @Test
    public void testANewestBucketRefreshInTheBacklogOfARestartWithNoTimelineRefusesOverTheDayTtlEvicted() throws Exception {
        // The mat view's TTL evicted 2026-01-01 and the view keeps that day's row. A late base row
        // then makes the mat view refresh the view's newest bucket, a REPLACE_RANGE over
        // [01-03T05:00, 06:00) the view does not consume, and a restart finds no timeline. The
        // refresh can change no base row below 05:00, so the guard compares the two rows the view
        // holds below it, and the history floor refuses the rebuild that would drop 2026-01-01.
        assertMemoryLeak(() -> {
            seedViewOverTtlMatView();
            refreshMatViewWithoutTheView("INSERT INTO base (ts, k, v) VALUES ('2026-01-03T05:20:00.000000Z', 'a', 16.0)");
            shutdown();
            removeTimeline();

            final LogCapture capture = new LogCapture();
            final LiveViewRebuildRestatementGuard guard = restartCapturing(capture);

            assertTtlViewRebuildRefusedBelowTheNewestBucket(guard, capture, "2026-01-02T00:00:00.000000Z", TTL_VIEW_ROWS);
        });
    }

    @Test
    public void testANewestBucketRefreshDuringABlockKeepsTheBlockOverTheDayTtlEvicted() throws Exception {
        // The first restart with no timeline refuses on the history floor over every row the view
        // holds. During the block a late base row makes the mat view refresh the view's newest
        // bucket, and the next restart meets that refresh in its backlog. It can change no base row
        // below 05:00, so the block holds over the rows below it, with the same evidence.
        assertMemoryLeak(() -> {
            seedViewOverTtlMatView();
            shutdown();
            removeTimeline();
            final LiveViewRebuildRestatementGuard firstGuard = restart();
            assertTtlViewBlocked("2026-01-02T00:00:00.000000Z");
            Assert.assertEquals(LiveViewRebuildRestatementGuard.VERDICT_HISTORY_FLOOR, firstGuard.getVerdict());
            Assert.assertEquals(3, firstGuard.getDurableRows());

            refreshMatViewDuringTheBlock("INSERT INTO base (ts, k, v) VALUES ('2026-01-03T05:20:00.000000Z', 'a', 16.0)");
            shutdown();

            final LogCapture capture = new LogCapture();
            final LiveViewRebuildRestatementGuard guard = restartCapturing(capture);

            assertTtlViewRebuildRefusedBelowTheNewestBucket(guard, capture, "2026-01-02T00:00:00.000000Z", TTL_VIEW_ROWS);
        });
    }

    @Test
    public void testABlockThatOutlastsAnotherTtlEvictionKeepsEveryRowUntilTheGuardIsTurnedOff() throws Exception {
        // During the block a late base row makes the mat view refresh the view's newest bucket, and
        // a row a day later makes its TTL evict 2026-01-02 too. The next restart still refuses on
        // the history floor over the rows below the refresh, and keeps both days the mat view
        // evicted. Only the guard switch lets the rebuild follow the mat view.
        assertMemoryLeak(() -> {
            seedViewOverTtlMatView();
            shutdown();
            removeTimeline();
            restart();
            assertTtlViewBlocked("2026-01-02T00:00:00.000000Z");

            setCurrentMicros(ts("2026-01-05T00:00:00.000000Z"));
            refreshMatViewDuringTheBlock("INSERT INTO base (ts, k, v) VALUES ('2026-01-03T05:20:00.000000Z', 'a', 16.0)");
            refreshMatViewDuringTheBlock("INSERT INTO base (ts, k, v) VALUES ('2026-01-04T05:10:00.000000Z', 'a', 32.0)");
            assertQuery("SELECT ts, k, sv FROM mvbase")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tk\tsv
                            2026-01-03T05:00:00.000000Z\ta\t20.0
                            2026-01-04T05:00:00.000000Z\ta\t32.0
                            """);
            shutdown();

            final LogCapture capture = new LogCapture();
            final LiveViewRebuildRestatementGuard guard = restartCapturing(capture);

            assertTtlViewRebuildRefusedBelowTheNewestBucket(guard, capture, "2026-01-03T05:00:00.000000Z", TTL_VIEW_ROWS);

            setProperty(PropertyKey.CAIRO_LIVE_VIEW_REBUILD_RESTATEMENT_GUARD_ENABLED, "false");
            shutdown();
            restart();
            assertRebuiltFromAppliedBase("lv_on_mv");
            assertTtlViewRows("""
                    ts\tk\tsv\ts
                    2026-01-03T05:00:00.000000Z\ta\t20.0\t20.0
                    2026-01-04T05:00:00.000000Z\ta\t32.0\t52.0
                    """);
            TestUtils.assertSqlCursors(
                    engine,
                    sqlExecutionContext,
                    TTL_VIEW_SQL,
                    "SELECT ts, k, sv, s FROM lv_on_mv",
                    LOG,
                    true
            );
            assertViewActive();
        });
    }

    @Test
    public void testANewestBucketRefreshOverARepairedViewPartitionRefusesOverTheDayTtlEvicted() throws Exception {
        // The view consumes the mat view's refresh of its newest bucket as an out-of-order repair,
        // which rewrites the view's 2026-01-03 partition into pieces. A later refresh of the same
        // bucket stays in the backlog of a restart with no timeline, and the guard reads the rows
        // below it off those pieces.
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
            createViewOverTtlMatView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // The clock stays below the data, so the mat view's TTL evicts nothing yet.
                refreshMatViewAndView(job, "INSERT INTO base (ts, k, v) VALUES ('2026-01-01T00:10:00.000000Z', 'a', 1.0)");
                refreshMatViewAndView(job, "INSERT INTO base (ts, k, v) VALUES ('2026-01-02T00:10:00.000000Z', 'a', 2.0)");
                refreshMatViewAndView(job, "INSERT INTO base (ts, k, v) VALUES ('2026-01-03T05:10:00.000000Z', 'a', 4.0)");
                refreshMatViewAndView(job, "INSERT INTO base (ts, k, v) VALUES ('2026-01-03T05:20:00.000000Z', 'a', 16.0)");
            }
            final String repairedRows = """
                    ts\tk\tsv\ts
                    2026-01-01T00:00:00.000000Z\ta\t1.0\t1.0
                    2026-01-02T00:00:00.000000Z\ta\t2.0\t3.0
                    2026-01-03T05:00:00.000000Z\ta\t20.0\t23.0
                    """;
            assertTtlViewRows(repairedRows);
            assertNoRefreshFaults("lv_on_mv");
            assertViewRowPastLiveFilePrefix("lv_on_mv", "2026-01-03T05:00:00.000000Z");

            // TTL measures against the older of the newest row and the wall clock: past the data,
            // the next commit evicts the mat view's 2026-01-01.
            setCurrentMicros(ts("2026-01-04T00:00:00.000000Z"));
            refreshMatViewWithoutTheView("INSERT INTO base (ts, k, v) VALUES ('2026-01-03T05:30:00.000000Z', 'a', 8.0)");
            shutdown();
            removeTimeline();

            final LogCapture capture = new LogCapture();
            final LiveViewRebuildRestatementGuard guard = restartCapturing(capture);

            assertTtlViewRebuildRefusedBelowTheNewestBucket(guard, capture, "2026-01-02T00:00:00.000000Z", repairedRows);
        });
    }

    @Test
    public void testATransientFaultReadingTheRowsBelowANewestBucketRefreshRetriesAndRefuses() throws Exception {
        // The guard reads the view's rows below the refresh off the view's newest partition. A
        // fault there that a retry may get past hands the rebuild to the refresh retry, whose
        // recovery of the window state the failed turn left behind refuses the rebuild as a
        // restart without the fault does, and names itself as the cause.
        final ViewPartitionReadFault fault = new ViewPartitionReadFault();
        assertMemoryLeak(fault, () -> {
            seedViewOverTtlMatView();
            refreshMatViewWithoutTheView("INSERT INTO base (ts, k, v) VALUES ('2026-01-03T05:20:00.000000Z', 'a', 16.0)");
            shutdown();
            removeTimeline();
            // EMFILE on Linux and macOS: an OS error other than a missing file.
            fault.arm(engine.verifyTableName("lv_on_mv"), "2026-01-03", 24);

            final LogCapture capture = new LogCapture();
            final LiveViewRebuildRestatementGuard guard = restartCapturing(capture);

            Assert.assertTrue("the read of the view's newest partition must have failed", fault.hasFired());
            capture.assertLogged("live view restart rebuild hit a retryable fault, retrying [view=lv_on_mv, cause=timeline is absent, "
                    + "error=io.questdb.cairo.CairoException: [24] could not open read-only [file=");
            capture.assertNotLogged("live view restart applied-base rebuild failed");
            assertTtlViewRebuildRefusedBelowTheNewestBucket(
                    guard,
                    capture,
                    "mid-drain refresh failure",
                    "2026-01-02T00:00:00.000000Z",
                    TTL_VIEW_ROWS
            );
        });
    }

    @Test
    public void testAMissingFileReadingTheRowsBelowANewestBucketRefreshInvalidatesTheViewAndKeepsItsRows() throws Exception {
        // The same read failing on a file that does not exist is not a fault a retry gets past: the
        // rebuild fails before anything moved, and the view is invalidated with its rows.
        final ViewPartitionReadFault fault = new ViewPartitionReadFault();
        assertMemoryLeak(fault, () -> {
            seedViewOverTtlMatView();
            refreshMatViewWithoutTheView("INSERT INTO base (ts, k, v) VALUES ('2026-01-03T05:20:00.000000Z', 'a', 16.0)");
            shutdown();
            removeTimeline();
            fault.arm(engine.verifyTableName("lv_on_mv"), "2026-01-03", CairoException.ERRNO_FILE_DOES_NOT_EXIST);

            final LogCapture capture = new LogCapture();
            restartCapturing(capture);

            Assert.assertTrue("the read of the view's newest partition must have failed", fault.hasFired());
            capture.assertLogged("live view restart applied-base rebuild failed [view=lv_on_mv, "
                    + "error=io.questdb.cairo.CairoException: [" + CairoException.ERRNO_FILE_DOES_NOT_EXIST
                    + "] could not open, file does not exist: ");
            capture.assertNotLogged("live view restart rebuild hit a retryable fault");
            capture.assertNotLogged("live view rebuild from the applied base runs without the restatement guard");
            assertTtlViewRows(TTL_VIEW_ROWS);
            assertQuery("SELECT view_status, invalidation_reason FROM live_views()")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            view_status\tinvalidation_reason
                            invalid\tlive view restart timeline recovery failed
                            """);
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
        applyBucketRemovingRefreshWithoutTheView(BUCKET_REMOVING_CORRECTION);
    }

    /**
     * The same for {@code correction}, a base row the mat view's filter drops that replaces the
     * only row of one bucket.
     */
    private void applyBucketRemovingRefreshWithoutTheView(String correction) throws Exception {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv_on_mv");
        Assert.assertNotNull(instance);
        final long processedBefore = instance.getLastProcessedSeqTxn();
        execute(correction);
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

    /**
     * Asserts the view over the mat view with a one-day TTL is stopped on a refused rebuild whose
     * history floor met a base starting at {@code baseMinTs}, and holds every row it held before.
     */
    private void assertTtlViewBlocked(String baseMinTs) throws Exception {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv_on_mv");
        Assert.assertNotNull(instance);
        Assert.assertTrue("the rebuild must be refused", instance.isCheckpointRecoveryBlocked());
        Assert.assertEquals(LiveViewCheckpointRecoveryPhase.REBUILD_BLOCKED, instance.getCheckpointRecoveryPhase());
        Assert.assertFalse("a refused rebuild must not invalidate the view", instance.isInvalid());
        TestUtils.assertContains(
                instance.getCheckpointRecoveryReason(),
                "rebuilding the view from its base table would drop rows it retains [cause=timeline is absent]: "
                        + "the view holds rows from 2026-01-01T00:00:00.000000Z but the base table's earliest row is at "
                        + baseMinTs + "; refresh is stopped"
        );
        assertTtlViewRows(TTL_VIEW_ROWS);
        assertQuery("SELECT view_status, checkpoint_recovery_phase FROM live_views()")
                .noLeakCheck()
                .noRandomAccess()
                .returns("""
                        view_status\tcheckpoint_recovery_phase
                        invalid\trebuild_blocked
                        """);
    }

    /**
     * Asserts the restart's rebuild of the view over the mat view with a one-day TTL compared only
     * the two rows below the mat view's refresh of the view's newest bucket, [01-03T05:00, 06:00),
     * and refused on the history floor against a mat view starting at {@code baseMinTs}: the view is
     * stopped and holds {@code rows}.
     */
    private void assertTtlViewRebuildRefusedBelowTheNewestBucket(
            LiveViewRebuildRestatementGuard guard,
            LogCapture capture,
            String baseMinTs,
            String rows
    ) throws Exception {
        assertTtlViewRebuildRefusedBelowTheNewestBucket(guard, capture, "timeline is absent", baseMinTs, rows);
    }

    /**
     * The same, for a rebuild the recovery {@code cause} names asked for.
     */
    private void assertTtlViewRebuildRefusedBelowTheNewestBucket(
            LiveViewRebuildRestatementGuard guard,
            LogCapture capture,
            String cause,
            String baseMinTs,
            String rows
    ) throws Exception {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv_on_mv");
        Assert.assertNotNull(instance);
        Assert.assertTrue("the rebuild must be refused", instance.isCheckpointRecoveryBlocked());
        Assert.assertEquals(LiveViewCheckpointRecoveryPhase.REBUILD_BLOCKED, instance.getCheckpointRecoveryPhase());
        Assert.assertFalse("a refused rebuild must not invalidate the view", instance.isInvalid());
        TestUtils.assertContains(
                instance.getCheckpointRecoveryReason(),
                "rebuilding the view from its base table would drop rows it retains [cause=" + cause + "]: "
                        + "the view holds rows from 2026-01-01T00:00:00.000000Z but the base table's earliest row is at "
                        + baseMinTs + "; refresh is stopped"
        );
        Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_NONE, guard.getAbstention());
        Assert.assertEquals(LiveViewRebuildRestatementGuard.VERDICT_HISTORY_FLOOR, guard.getVerdict());
        // 2026-01-01 and 2026-01-02, the rows below the refreshed bucket.
        Assert.assertEquals(2, guard.getDurableRows());
        capture.assertLogged(BAND_NARROWING_LINE + "replaceLo=2026-01-03T05:00:00.000000Z, rows=2]");
        capture.assertLogged("live view rebuild from the applied base refused, it would drop rows the view retains [view=lv_on_mv");
        capture.assertNotLogged("live view rebuild from the applied base runs without the restatement guard");
        assertTtlViewRows(rows);
        assertQuery("SELECT view_status, checkpoint_recovery_phase FROM live_views()")
                .noLeakCheck()
                .noRandomAccess()
                .returns("""
                        view_status\tcheckpoint_recovery_phase
                        invalid\trebuild_blocked
                        """);
    }

    private void assertTtlViewRows(String expected) throws Exception {
        assertQuery("SELECT ts, k, sv, s FROM lv_on_mv")
                .noLeakCheck()
                .timestamp("ts")
                .expectSize()
                .returns(expected);
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

    /**
     * Creates a base, a mat view summing it an hour at a time with a one-day TTL, and a view over
     * the mat view summing the twenty rows up to each, created with the clock at the epoch.
     */
    private void createViewOverTtlMatView() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, k SYMBOL, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE MATERIALIZED VIEW mvbase AS (SELECT ts, k, sum(v) AS sv FROM base SAMPLE BY 1h) PARTITION BY DAY TTL 1 DAY");
        drainWalAndMatViewQueues(engine);
        setCurrentMicros(0L);
        execute("CREATE LIVE VIEW lv_on_mv FLUSH EVERY 100ms START FROM NOW AS " + TTL_VIEW_SQL);
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
     * Commits {@code insert} to the base and refreshes the mat view over it while the view is
     * stopped on a refused rebuild, then drives the refresh job, which leaves the view stopped
     * and consumes nothing.
     */
    private void refreshMatViewDuringTheBlock(String insert) throws Exception {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv_on_mv");
        Assert.assertNotNull(instance);
        final long processedBefore = instance.getLastProcessedSeqTxn();
        execute(insert);
        drainWalAndMatViewQueues(engine);
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveRefreshToQuiescence(job);
        }
        Assert.assertTrue("the view must stay stopped", instance.isCheckpointRecoveryBlocked());
        Assert.assertEquals("a stopped view consumes nothing", processedBefore, instance.getLastProcessedSeqTxn());
    }

    /**
     * Commits {@code insert} to the base and refreshes the mat view over it, without the live view
     * consuming the refresh, so the refresh stays in the backlog a restart's rebuild classifies.
     */
    private void refreshMatViewWithoutTheView(String insert) throws Exception {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv_on_mv");
        Assert.assertNotNull(instance);
        final long processedBefore = instance.getLastProcessedSeqTxn();
        execute(insert);
        drainWalAndMatViewQueues(engine);
        Assert.assertEquals("the view must not consume the refresh", processedBefore, instance.getLastProcessedSeqTxn());
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
        final TableToken viewToken = engine.verifyTableName("lv_on_mv");
        final File checkpointsDir = new File(
                new File(engine.getConfiguration().getDbRoot(), viewToken.getDirName()),
                LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME
        );
        Assert.assertTrue(
                "the fixture must have published a timeline to remove",
                new File(checkpointsDir, LiveViewCheckpointLayout.TIMELINE_FILE_NAME).delete()
        );
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

    /**
     * {@link #restart()} with {@code capture} recording the log lines the restart's recovery
     * writes.
     */
    private LiveViewRebuildRestatementGuard restartCapturing(LogCapture capture) {
        capture.start();
        try {
            final LiveViewRebuildRestatementGuard guard = restart();
            capture.drain();
            return guard;
        } finally {
            capture.stop();
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
     * {@link #createViewOverTtlMatView()}, then a row a day for three days refreshed through the
     * mat view and the view, one commit each. The clock moves past the data ahead of the third, so
     * the mat view's TTL evicts 2026-01-01 on it, and the view keeps that day's row.
     */
    private void seedViewOverTtlMatView() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        createViewOverTtlMatView();
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            refreshMatViewAndView(job, "INSERT INTO base (ts, k, v) VALUES ('2026-01-01T00:10:00.000000Z', 'a', 1.0)");
            refreshMatViewAndView(job, "INSERT INTO base (ts, k, v) VALUES ('2026-01-02T00:10:00.000000Z', 'a', 2.0)");
            // TTL measures against the older of the newest row and the wall clock: past the data,
            // this commit evicts the mat view's 2026-01-01.
            setCurrentMicros(ts("2026-01-04T00:00:00.000000Z"));
            refreshMatViewAndView(job, "INSERT INTO base (ts, k, v) VALUES ('2026-01-03T05:10:00.000000Z', 'a', 4.0)");
        }
        assertTtlViewRows(TTL_VIEW_ROWS);
        assertQuery("SELECT ts, k, sv FROM mvbase")
                .noLeakCheck()
                .timestamp("ts")
                .expectSize()
                .returns("""
                        ts\tk\tsv
                        2026-01-02T00:00:00.000000Z\ta\t2.0
                        2026-01-03T05:00:00.000000Z\ta\t4.0
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

    /**
     * Once armed, fails the first read-only open of a column file in one partition of a table,
     * reporting {@code errno}. The cases arm it on a restart, once every pooled reader is gone, for
     * the live view's newest partition: the restatement guard's binary search for the rows below a
     * backlog band is the first to read it.
     */
    private static final class ViewPartitionReadFault extends TestFilesFacadeImpl {
        private final AtomicBoolean hasFired = new AtomicBoolean();
        private final AtomicBoolean isArmed = new AtomicBoolean();
        private final AtomicBoolean isErrnoPending = new AtomicBoolean();
        private volatile int errno;
        private volatile String partitionPathPart;

        @Override
        public int errno() {
            return isErrnoPending.compareAndSet(true, false) ? errno : super.errno();
        }

        @Override
        public long openRO(LPSZ name) {
            if (isArmed.get()
                    && Utf8s.containsAscii(name, partitionPathPart)
                    && Utf8s.endsWithAscii(name, ".d")
                    && isArmed.compareAndSet(true, false)) {
                hasFired.set(true);
                isErrnoPending.set(true);
                return -1;
            }
            return super.openRO(name);
        }

        void arm(TableToken tableToken, String partitionName, int errno) {
            this.partitionPathPart = Files.SEPARATOR + tableToken.getDirName() + Files.SEPARATOR + partitionName;
            this.errno = errno;
            hasFired.set(false);
            isArmed.set(true);
        }

        boolean hasFired() {
            return hasFired.get();
        }
    }
}
