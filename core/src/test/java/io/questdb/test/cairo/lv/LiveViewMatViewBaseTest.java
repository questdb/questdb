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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRebuildRestatementGuard;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

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
    public void testARebuildOverAMatViewWithNoTimelineStandsTheGuardDownForItsRefresh() throws Exception {
        // Every incremental refresh of a materialized view commits a REPLACE_RANGE over the
        // buckets it recomputes, and one that leaves a bucket empty removes a row the live view
        // derived output from. A restart that finds no timeline rebuilds from the applied mat
        // view, which reproduces one row fewer than the view holds. The refresh is in the
        // rebuild's backlog, so the guard must stand down rather than refuse it as a loss.
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
            Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_BACKLOG_MAY_REMOVE, guard.getAbstention());
            capture.assertLogged("live view rebuild from the applied base runs without the restatement guard [view=lv_on_mv, reason=backlog may remove rows]");
            capture.assertNotLogged("live view could not read the rebuild's base backlog");
            capture.assertNotLogged("live view rebuild from the applied base refused");
            assertViewRowsAfterTheBucketRemoval();
            assertViewActive();
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

    private void refreshMatViewAndView(LiveViewRefreshJob job, String insert) throws Exception {
        execute(insert);
        drainWalAndMatViewQueues(engine);
        driveRefreshToQuiescence(job);
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
     * Releases everything that maps the view's and its bases' files, the way a stopped process
     * would, so the next {@link #restart()} starts from disk.
     */
    private void shutdown() {
        engine.getLiveViewRegistry().clear();
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        engine.releaseInactive();
    }
}
