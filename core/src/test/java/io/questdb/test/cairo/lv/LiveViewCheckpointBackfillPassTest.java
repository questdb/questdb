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
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointPendingRepairs;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Coverage for deferral and the backfill pass: a correction landing in a <b>closed</b>
 * anchor segment is recorded in the view's durable pending-repair set and repaired by a
 * later pass, rather than repaired inside the refresh turn that consumed it.
 * <p>
 * The per-segment repair already bounds what a deep correction rewrites; what it does not
 * do is take it off the refresh's critical path, and the reported failure is a view that
 * falls behind for everything - including its head - while it repairs its history. So the
 * claim here is not that the output is right, which a from-base recompute would say of an
 * inline repair too. It is that the turn does not repair at all: the closed segments go
 * into the pending set, the turn walks its watermark over them, and the view is
 * <em>knowingly</em> stale in those segments until a pass drains them. Every case
 * therefore asserts the intermediate state as well as the converged one - a case that only
 * compared the end state would pass with deferral wired to nothing.
 * <p>
 * The view is the same reported customer shape the per-segment cases use: an anchored
 * WINDOW carrying an unbounded cumulative sum and count per account, over a base whose
 * timestamps span several anchor days so closed segments exist at all.
 */
public class LiveViewCheckpointBackfillPassTest extends AbstractLiveViewTest {

    private static final long DAY_2_START = ts("2026-01-02T00:00:00.000000Z");
    private static final long DAY_3_START = ts("2026-01-03T00:00:00.000000Z");

    @Test
    public void testABoundedFrameBesideTheAnchorNeverDefers() throws Exception {
        // The same gate the per-segment repair reads. A bounded ROWS frame keeps sliding
        // across the segment boundary, so a row in a closed segment still changes a later
        // segment's output - the segments are not independent and nothing about them may
        // be left unrepaired.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        enableDeferral(0);
        assertMemoryLeak(() -> {
            execute("create table tx (created_at timestamp, cod_acct_no symbol nocache index capacity 4, "
                    + "amt_txn double) timestamp(created_at) partition by hour wal");
            execute("insert into tx values " + seedThreeDays());
            drainWalQueue();
            execute("create live view lv flush every 100ms start from beginning as "
                    + "select created_at, cod_acct_no, sum(amt_txn) over w as cumulative_sum, "
                    + "sum(amt_txn) over (partition by cod_acct_no order by created_at "
                    + "rows between 3 preceding and current row) as windowed_sum "
                    + "from tx window w as (partition by cod_acct_no order by created_at anchor daily '00:00')");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 3, "acct-1"), job);
                commit(row(2, 3, "acct-1"), job);

                Assert.assertEquals(
                        "a view carrying a bounded frame beside its anchor must not defer",
                        0,
                        job.deferredSegmentCountForTest()
                );
                Assert.assertEquals(0, viewInstance().getPendingRepairsSegments());
                assertNoRefreshFaults("lv");
                assertBoundedViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAClosedSegmentCorrectionIsDeferredAndDrainedByALaterPass() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        enableDeferral(Micros.HOUR_MICROS);
        assertMemoryLeak(() -> {
            createView(seedThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                // Head rows, so the runtime's own segment is the fifth day and the three
                // seeded days are all closed below it.
                commit(row(5, 1, "acct-1") + ", " + row(5, 2, "acct-2"), job);
                assertViewRowCount(8);

                // A correction reaching into the second day and nothing at the head, so the
                // change set is closed segments and nothing else - the shape whose turn
                // repairs nothing at all and still has to walk its watermark forward.
                final long baseHead = commit(row(2, 3, "acct-1"), job);

                Assert.assertEquals("the closed segment must be deferred, not repaired", 0, job.segmentRepairCountForTest());
                Assert.assertEquals(1, job.deferredSegmentCountForTest());
                Assert.assertEquals(0, job.backfillPassSegmentCountForTest());
                Assert.assertEquals(1, viewInstance().getPendingRepairsSegments());
                Assert.assertEquals(1, viewInstance().getPendingRepairsRows());
                Assert.assertEquals(DAY_2_START, viewInstance().getPendingRepairsOldestTs());
                // Knowingly stale: the corrected row has no output row in the view yet.
                assertViewRowCount(8);
                // And consumed all the same, or the next turn would re-drain it forever.
                Assert.assertEquals(
                        "the deferring turn owes the watermark advance",
                        baseHead,
                        viewInstance().getLastProcessedSeqTxn()
                );

                // Past the coalescing window, so the pass runs.
                setCurrentMicros(currentMicros + 2 * Micros.HOUR_MICROS);
                driveRefreshToQuiescence(job);

                Assert.assertEquals(1, job.backfillPassSegmentCountForTest());
                Assert.assertEquals(0, viewInstance().getPendingRepairsSegments());
                Assert.assertEquals(0, viewInstance().getPendingRepairsRows());
                Assert.assertEquals(Numbers.LONG_NULL, viewInstance().getPendingRepairsOldestTs());
                Assert.assertFalse("the pass removes the set once it drains", pendingSetExists());
                assertViewRowCount(9);
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testACorrectionInTheActiveSegmentRepairsInlineWhileAClosedOneDefers() throws Exception {
        // The two routes have to coexist in one turn rather than exclude each other: 46% of
        // the measured workload's commits carry sub-minute lateness, which is inside today's
        // segment and must repair now because it changes current output.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        enableDeferral(Micros.HOUR_MICROS);
        assertMemoryLeak(() -> {
            createView(seedThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 5, "acct-1"), job);
                final long resumeBefore = viewInstance().getO3ResumeReplayRows();

                // One row below the frontier but inside the fifth day - the active segment -
                // beside one in the closed second day.
                commit(row(5, 2, "acct-2") + ", " + row(2, 3, "acct-1"), job);

                Assert.assertEquals("the closed segment defers", 1, job.deferredSegmentCountForTest());
                Assert.assertEquals("and is not repaired inline", 0, job.segmentRepairCountForTest());
                Assert.assertTrue(
                        "the active-segment correction still repairs through the resume",
                        viewInstance().getO3ResumeReplayRows() > resumeBefore
                );
                Assert.assertEquals(1, viewInstance().getPendingRepairsSegments());
                // The active segment's correction is emitted; the closed segment's is not.
                assertViewRowCount(8);

                setCurrentMicros(currentMicros + 2 * Micros.HOUR_MICROS);
                driveRefreshToQuiescence(job);
                Assert.assertEquals(1, job.backfillPassSegmentCountForTest());
                Assert.assertEquals(0, viewInstance().getPendingRepairsSegments());
                assertViewRowCount(9);
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testACorruptPendingSetRecomputesTheViewFromTheAppliedBase() throws Exception {
        // The one failure a deferred correction cannot shrug off. The entries name closed
        // segments the view has already consumed, and the base WAL floor has moved over the
        // commits that carried them, so a set that will not validate cannot simply be
        // dropped - only the applied base still holds those rows.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        enableDeferral(Micros.HOUR_MICROS);
        assertMemoryLeak(() -> {
            createView(seedThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1"), job);
                commit(row(2, 3, "acct-1"), job);
                Assert.assertEquals(1, viewInstance().getPendingRepairsSegments());
                assertViewRowCount(7);
                corruptPendingSet();
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                Assert.assertEquals(
                        "the recompute repairs every segment the set could have named, so nothing stays pending",
                        0,
                        viewInstance().getPendingRepairsSegments()
                );
                Assert.assertFalse(pendingSetExists());
                assertViewRowCount(8);
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAPendingSegmentSurvivesARestart() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        enableDeferral(Micros.HOUR_MICROS);
        assertMemoryLeak(() -> {
            createView(seedThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1"), job);
                commit(row(2, 3, "acct-1"), job);
                Assert.assertEquals(1, viewInstance().getPendingRepairsSegments());
                assertViewRowCount(7);
                // One ordinary forward commit, whose live-view block records the base
                // seqTxn the deferring turn consumed. Without it the restart rewinds below
                // the deferred change - see the case below, which is that window.
                commit(row(5, 2, "acct-2"), job);
                assertViewRowCount(8);
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // A restart is not a pass: the set comes back, and the view is still stale.
                driveRefreshToQuiescence(job);
                Assert.assertEquals(1, viewInstance().getPendingRepairsSegments());
                Assert.assertEquals(DAY_2_START, viewInstance().getPendingRepairsOldestTs());
                assertViewRowCount(8);

                setCurrentMicros(currentMicros + 2 * Micros.HOUR_MICROS);
                driveRefreshToQuiescence(job);
                Assert.assertEquals(1, job.backfillPassSegmentCountForTest());
                Assert.assertEquals(0, viewInstance().getPendingRepairsSegments());
                assertViewRowCount(9);
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testARestartRightAfterADeferringTurnReprocessesTheChange() throws Exception {
        // The one window deferral pays for. A deferring turn commits no live-view block, so
        // the base seqTxn it consumed is nowhere in the live view's own table - and that
        // table is what a restart reconciles its floor from. So a restart landing between
        // the deferring turn and the next turn that commits rows rewinds below the deferred
        // change and processes it again, this time through the restore's own replay, which
        // recomputes the view rather than deferring.
        //
        // The window is bounded by the next commit and the outcome is convergence, not
        // loss, which is what this pins. It also pins the tidying that goes with it: the
        // recompute covers every segment the set could name, so the set goes with it rather
        // than leaving a backlog of passes over already-correct segments.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        enableDeferral(Micros.HOUR_MICROS);
        assertMemoryLeak(() -> {
            createView(seedThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1"), job);
                commit(row(2, 3, "acct-1"), job);
                Assert.assertEquals(1, viewInstance().getPendingRepairsSegments());
                assertViewRowCount(7);
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                Assert.assertEquals(
                        "the recompute covered the pending segment, so nothing is left owed",
                        0,
                        viewInstance().getPendingRepairsSegments()
                );
                Assert.assertFalse(pendingSetExists());
                assertViewRowCount(8);
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testDeferralIsOffByDefaultAndTheCorrectionRepairsInline() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1"), job);
                commit(row(2, 3, "acct-1"), job);

                Assert.assertEquals("the closed segment repairs inline", 1, job.segmentRepairCountForTest());
                Assert.assertEquals(0, job.deferredSegmentCountForTest());
                Assert.assertEquals(0, viewInstance().getPendingRepairsSegments());
                Assert.assertFalse(pendingSetExists());
                // No staleness at all: the correction is in the output on the turn that
                // consumed it.
                assertViewRowCount(8);
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testASegmentNoPassCouldRepairIsNeverDeferred() throws Exception {
        // The defect deferral shipped with, and the guard that closes it. A per-segment plan
        // declines when its output floor lands back on the view's START FROM boundary, which
        // happens when a corrected row sits exactly on that boundary: the decomposition keeps
        // rows at or above it, so the segment's own minimum is the boundary itself.
        //
        // Inline that decline costs nothing - the caller hands the segment back to the
        // whole-change-set plan, which repairs it. Deferred it was permanent: the pass has no
        // change set to widen and no union range to fall back on, so the entry stayed pending,
        // the view stayed wrong in that segment, and nothing above noticed. A fuzz seed found
        // it with twelve such segments outstanding and two hundred further job cycles leaving
        // the count exactly where it was.
        //
        // So deferChangeSetSegments probes each segment through the plan before it records
        // any of them, and a change set holding one it cannot drain takes the inline route
        // whole - the same all-or-nothing escape a full pending set already took.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        enableDeferral(Micros.HOUR_MICROS);
        assertMemoryLeak(() -> {
            createViewStartingFrom(seedThreeDays(), "2026-01-02T01:00:00.000000Z");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                // The head, so the three seeded days are closed below it.
                commit(row(5, 1, "acct-1"), job);
                assertViewRowCount(7);

                // Exactly on the boundary, in a closed segment: the shape whose plan declines.
                commit(row(2, 1, "acct-2"), job);

                Assert.assertEquals(
                        "a segment the pass could not have drained must not be deferred",
                        1,
                        job.undeferrableChangeSetCountForTest()
                );
                Assert.assertEquals(0, job.deferredSegmentCountForTest());
                Assert.assertEquals(0, viewInstance().getPendingRepairsSegments());
                Assert.assertFalse(
                        "nothing may be written to the set for a segment that cannot leave it",
                        pendingSetExists()
                );
                // Repaired inline instead, so the view is correct in that segment now rather
                // than never.
                assertViewRowCount(8);
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAPendingSegmentNoPassCanRepairIsRecoveredRatherThanRetriedForever() throws Exception {
        // The backstop under the guard above, and the reason the guard alone is not the fix.
        // The probe runs against the view's boundary and durable frontier as they stand when
        // the correction is deferred; the pass plans the entry against them as they stand
        // when it runs. An entry written under one verdict can therefore meet another, and
        // the grounds a per-segment plan declines on are properties of the view and the
        // segment rather than of the moment - so the next pass declines it again, forever.
        //
        // The pass now escalates to the recompute a set that cannot be acted on has always
        // taken: read the applied base, republish the whole view range, drop the set. That
        // repairs the segment it could not plan along with every other one the set named.
        //
        // Production cannot reach this state while the probe stands, which is why the state
        // is injected rather than constructed.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        enableDeferral(Micros.HOUR_MICROS);
        assertMemoryLeak(() -> {
            createView(seedThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1"), job);
                commit(row(2, 3, "acct-1"), job);
                Assert.assertEquals(1, viewInstance().getPendingRepairsSegments());
                Assert.assertTrue(pendingSetExists());
                // Knowingly stale in that segment, which is what deferral promises.
                assertViewRowCount(7);

                job.setSimulateBackfillPassDeclineForTest(true);
                setCurrentMicros(currentMicros + 2 * Micros.HOUR_MICROS);
                driveRefreshToQuiescence(job);

                Assert.assertEquals(
                        "the pass escalated rather than leaving an entry it cannot act on pending",
                        1,
                        job.backfillPassRecoveryCountForTest()
                );
                Assert.assertEquals(
                        "the recompute repairs every segment the set named",
                        0,
                        viewInstance().getPendingRepairsSegments()
                );
                Assert.assertFalse(pendingSetExists());
                assertViewRowCount(8);
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testTwoCorrectionsInOneSegmentAreRepairedOnce() throws Exception {
        // The coalescing the interval buys, and the only thing deferral adds on top of
        // per-segment scoping: two corrections into one closed segment cost one repair
        // instead of two.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        enableDeferral(Micros.HOUR_MICROS);
        assertMemoryLeak(() -> {
            createView(seedThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1"), job);

                commit(row(2, 3, "acct-1"), job);
                commit(row(2, 5, "acct-2"), job);
                // And one in a second closed segment, so the pass has an order to drain in.
                commit(row(3, 3, "acct-1"), job);

                Assert.assertEquals(3, job.deferredSegmentCountForTest());
                Assert.assertEquals("two corrections in one segment are one entry", 2, viewInstance().getPendingRepairsSegments());
                Assert.assertEquals(3, viewInstance().getPendingRepairsRows());
                Assert.assertEquals(DAY_2_START, viewInstance().getPendingRepairsOldestTs());
                assertViewRowCount(7);

                setCurrentMicros(currentMicros + 2 * Micros.HOUR_MICROS);
                driveRefreshToQuiescence(job);

                Assert.assertEquals("three corrections, two segment repairs", 2, job.backfillPassSegmentCountForTest());
                Assert.assertEquals(0, viewInstance().getPendingRepairsSegments());
                assertViewRowCount(10);
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testTwoPendingSegmentsDrainOldestFirst() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        // A budget of zero stops the pass after its first segment, which is what makes the
        // drain order observable at all: the oldest is the one that goes.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_BACKFILL_MAX_DURATION, 0);
        enableDeferral(Micros.HOUR_MICROS);
        assertMemoryLeak(() -> {
            createView(seedThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1"), job);
                commit(row(3, 3, "acct-1") + ", " + row(2, 3, "acct-2"), job);

                Assert.assertEquals(2, viewInstance().getPendingRepairsSegments());
                Assert.assertEquals(DAY_2_START, viewInstance().getPendingRepairsOldestTs());

                setCurrentMicros(currentMicros + 2 * Micros.HOUR_MICROS);
                driveRefreshToQuiescence(job);

                Assert.assertEquals("a spent budget leaves the rest for the next pass", 1, job.backfillPassSegmentCountForTest());
                Assert.assertEquals(1, viewInstance().getPendingRepairsSegments());
                Assert.assertEquals(
                        "the oldest segment is the one that drained",
                        DAY_3_START,
                        viewInstance().getPendingRepairsOldestTs()
                );

                setCurrentMicros(currentMicros + 2 * Micros.HOUR_MICROS);
                driveRefreshToQuiescence(job);
                Assert.assertEquals(2, job.backfillPassSegmentCountForTest());
                Assert.assertEquals(0, viewInstance().getPendingRepairsSegments());
                assertViewMatchesRecompute();
            }
        });
    }

    private void assertBoundedViewMatchesRecompute() throws Exception {
        final String bucket = "timestamp_floor('1d', created_at, '1970-01-01T00:00:00.000000Z'::timestamp)";
        final String recompute = "select created_at, cod_acct_no, "
                + "sum(amt_txn) over (partition by cod_acct_no, bucket order by created_at "
                + "rows between unbounded preceding and current row) as cumulative_sum, "
                + "sum(amt_txn) over (partition by cod_acct_no order by created_at "
                + "rows between 3 preceding and current row) as windowed_sum "
                + "from (select created_at, cod_acct_no, amt_txn, " + bucket + " as bucket from tx)";
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + recompute + ") order by 2, 1",
                "(lv) order by 2, 1",
                LOG,
                true
        );
        assertNoRefreshFaults("lv");
    }

    private void assertViewMatchesRecompute() throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + recompute() + ") order by 2, 1",
                "(lv) order by 2, 1",
                LOG,
                true
        );
        assertNoRefreshFaults("lv");
    }

    /**
     * How many output rows the view holds. A deferred correction is exactly one output row
     * the view does not hold yet, so this is the cheapest statement of "knowingly stale"
     * there is - and it fails immediately if a turn quietly repaired what it claimed to
     * defer.
     */
    private void assertViewRowCount(long expected) throws Exception {
        assertQuery("select count() from lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n" + expected + "\n");
    }

    /**
     * Inserts {@code values}, drains, and drives the view to quiescence.
     *
     * @return the base table's sequencer head after the commit
     */
    private long commit(String values, LiveViewRefreshJob job) throws Exception {
        execute("insert into tx values " + values);
        drainWalQueue();
        driveRefreshToQuiescence(job);
        return engine.getTableSequencerAPI()
                .getTxnTracker(engine.verifyTableName("tx"))
                .getWriterTxn();
    }

    /**
     * Flips one byte inside the set's first entry, which its trailing CRC covers. Stands in
     * for the disk damage a staged-and-renamed file cannot otherwise reach: a crash leaves
     * the previous set intact, so a set that does not validate has been corrupted under the
     * database rather than half-written by it.
     */
    private void corruptPendingSet() {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Path dir = new Path(); Path file = new Path()) {
            checkpointsDir(dir);
            LiveViewCheckpointPendingRepairs.pendingPath(file, dir);
            Assert.assertTrue("the pending set must exist to be corrupted", ff.exists(file.$()));
            final long length = ff.length(file.$());
            final MemoryMARW mem = Vm.getCMARWInstance();
            try {
                mem.of(ff, file.$(), length, -1, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE, -1);
                final long offset = LiveViewCheckpointPendingRepairs.HEADER_SIZE;
                mem.putByte(offset, (byte) (mem.getByte(offset) ^ 0xFF));
            } finally {
                mem.close(false);
            }
        }
    }

    private void checkpointsDir(Path dst) {
        dst.of(engine.getConfiguration().getDbRoot())
                .concat(viewInstance().getLiveViewToken())
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }

    private void createView(String seedRows) throws Exception {
        execute("create table tx (created_at timestamp, cod_acct_no symbol nocache index capacity 4, "
                + "amt_txn double) timestamp(created_at) partition by hour wal");
        execute("insert into tx values " + seedRows);
        drainWalQueue();
        execute("create live view lv flush every 100ms start from beginning as "
                + "select created_at, cod_acct_no, "
                + "sum(amt_txn) over w as cumulative_sum, "
                + "count(cod_acct_no) over w as cumulative_count "
                + "from tx window w as (partition by cod_acct_no order by created_at anchor daily '00:00')");
    }

    /**
     * The same view with an explicit {@code START FROM} boundary. A corrected row landing
     * exactly on that boundary is what makes a per-segment plan decline on its output floor,
     * because the decomposition keeps rows at or above the boundary and drops the rest - so
     * the segment's own minimum is the boundary itself.
     */
    private void createViewStartingFrom(String seedRows, String boundary) throws Exception {
        execute("create table tx (created_at timestamp, cod_acct_no symbol nocache index capacity 4, "
                + "amt_txn double) timestamp(created_at) partition by hour wal");
        execute("insert into tx values " + seedRows);
        drainWalQueue();
        execute("create live view lv flush every 100ms start from '" + boundary + "' as "
                + "select created_at, cod_acct_no, "
                + "sum(amt_txn) over w as cumulative_sum, "
                + "count(cod_acct_no) over w as cumulative_count "
                + "from tx window w as (partition by cod_acct_no order by created_at anchor daily '00:00')");
    }

    private void enableDeferral(long intervalMicros) {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_BACKFILL_DEFERRAL_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_BACKFILL_INTERVAL, intervalMicros);
    }

    private boolean pendingSetExists() {
        try (Path dir = new Path(); Path file = new Path()) {
            checkpointsDir(dir);
            LiveViewCheckpointPendingRepairs.pendingPath(file, dir);
            return engine.getConfiguration().getFilesFacade().exists(file.$());
        }
    }

    /**
     * The from-base oracle: the same accumulators partitioned by account and anchor day.
     */
    private String recompute() {
        final String bucket = "timestamp_floor('1d', created_at, '1970-01-01T00:00:00.000000Z'::timestamp)";
        return "select created_at, cod_acct_no, "
                + "sum(amt_txn) over (partition by cod_acct_no, bucket order by created_at "
                + "rows between unbounded preceding and current row) as cumulative_sum, "
                + "count(cod_acct_no) over (partition by cod_acct_no, bucket order by created_at "
                + "rows between unbounded preceding and current row) as cumulative_count "
                + "from (select created_at, cod_acct_no, amt_txn, " + bucket + " as bucket from tx)";
    }

    private void restartCycle() {
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
    }

    /**
     * One row of {@code account} at {@code hour} on 2026-01-{@code day}, as an INSERT tuple.
     * The day is what carries the case: with a daily anchor it is also the segment.
     */
    private String row(int day, int hour, String account) {
        return "('2026-01-" + String.format("%02d", day) + "T" + String.format("%02d", hour)
                + ":00:00.000000Z', '" + account + "', 1.0)";
    }

    /**
     * Two accounts on each of 2026-01-02, 2026-01-03 and 2026-01-04 - three anchor days that
     * are all closed once the head reaches the fifth.
     */
    private String seedThreeDays() {
        return row(2, 1, "acct-1") + ", " + row(2, 2, "acct-2") + ", "
                + row(3, 1, "acct-1") + ", " + row(3, 2, "acct-2") + ", "
                + row(4, 1, "acct-1") + ", " + row(4, 2, "acct-2");
    }

    private LiveViewInstance viewInstance() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull("live view 'lv' must be registered", instance);
        return instance;
    }
}
