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
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Coverage for the segment yield: one anchor segment's repair may stop on the refresh
 * turn's budget and continue on a later turn, and the loop it belongs to continues with it.
 * <p>
 * A per-segment repair bounds what a deep correction reads and rewrites, but the bound is
 * the anchor period's own base rows - a whole day of them under {@code ANCHOR DAILY},
 * however few rows the correction carried. The loop that drives one owns a single pinned
 * base snapshot across every segment it takes, which is why it could not let a replay park:
 * a parked repair takes the snapshot with it and the rest of the loop would have nothing to
 * run against. So the loop position parks too.
 * <p>
 * Every case here holds two things at once. The from-base recompute oracle says the output
 * converged, and the counters say <em>how</em>: the replay parked at least once, and the
 * loop repaired each of its segments exactly once. The second half is what an end-state
 * comparison cannot see - a loop that dropped its position would leave the change
 * unconsumed with its segments already repaired, and the next drain would re-classify the
 * range and repair every one of them again, converging on the same rows for twice the work.
 * <p>
 * The view is the reported customer shape the per-segment cases use: an
 * anchored WINDOW carrying an unbounded cumulative sum and count per account, over a base
 * whose timestamps span several anchor days so closed segments exist at all. The days are
 * seeded several rows deep on purpose - a one-row replay budget only spreads a repair
 * across turns if the segment holds more than one row to replay.
 */
public class LiveViewCheckpointSegmentYieldTest extends AbstractLiveViewTest {

    @Test
    public void testASegmentRepairYieldsOnItsTurnBudgetAndResumes() throws Exception {
        // The base case: one closed segment, nothing at the head, and a replay budget that
        // cannot carry the segment in one turn. The repair has to cross turns and still
        // publish once, over that segment alone.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                // Head rows, so the runtime's own segment is the fifth day and the three
                // seeded days are all closed below it.
                commit(row(5, 1, "acct-1") + ", " + row(5, 2, "acct-2"), job);

                final long baseHead = commit(row(2, 5, "acct-1"), job);

                Assert.assertTrue(
                        "a one-row replay budget must take the segment repair across several turns",
                        job.segmentYieldCountForTest() > 0
                );
                Assert.assertEquals(
                        "the parked segment must publish exactly once",
                        1,
                        job.segmentRepairCountForTest()
                );
                Assert.assertEquals(
                        "the loop's last segment must advance the watermark over the whole change",
                        baseHead,
                        viewInstance().getLastProcessedSeqTxn()
                );
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAYieldedLoopStillRepairsItsResidual() throws Exception {
        // A change set with both halves: a correction in a closed segment and rows at the
        // head. The segment parks, and the residual behind it is the runtime's own
        // correction - the turn that finishes the segment owes it too. Dropping it would
        // leave the change unconsumed with the segment already repaired, which the next
        // drain would notice by repairing that segment a second time.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1") + ", " + row(5, 2, "acct-2"), job);

                // The production shape of a deep commit: rows at the frontier beside rows in
                // one old segment.
                final long baseHead = commit(row(2, 5, "acct-1") + ", " + row(5, 3, "acct-2"), job);

                Assert.assertTrue(
                        "a one-row replay budget must take the segment repair across several turns",
                        job.segmentYieldCountForTest() > 0
                );
                Assert.assertEquals(
                        "the closed segment must be repaired once, not once per re-classification",
                        1,
                        job.segmentRepairCountForTest()
                );
                Assert.assertEquals(
                        "the residual must run on the turn that finished the loop, consuming the change",
                        baseHead,
                        viewInstance().getLastProcessedSeqTxn()
                );
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testDecliningTheIsolatedRuntimeUnderAParkedLoopLosesNoCorrection() throws Exception {
        // The runtime a parked repair is standing in can drift out from under it, and an
        // operator declining the isolated runtime mid-repair is the deterministic way to
        // make it. The candidate goes; the correction must not, because the loop never
        // advanced the watermark over it.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1") + ", " + row(5, 2, "acct-2"), job);

                execute("insert into tx values " + row(2, 5, "acct-1") + ", " + row(3, 5, "acct-1"));
                drainWalQueue();
                driveUntilParked(job);

                setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_ISOLATED_RUNTIME_ENABLED, "false");
                driveRefreshToQuiescence(job);

                Assert.assertNull(
                        "the drifted candidate must be discarded rather than continued",
                        viewInstance().getSuspendedRepair()
                );
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testDecliningTheSegmentYieldKeepsASegmentReplayInOneTurn() throws Exception {
        // The escape hatch, and the control column a measurement runs against: the same
        // correction under the same budget, on the route every segment repair took before
        // the yield existed.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SEGMENT_YIELD_ENABLED, "false");
        assertMemoryLeak(() -> {
            createView(seedThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1") + ", " + row(5, 2, "acct-2"), job);
                commit(row(2, 5, "acct-1"), job);

                Assert.assertEquals(
                        "a declined yield must carry the whole segment inside one turn",
                        0,
                        job.segmentYieldCountForTest()
                );
                Assert.assertEquals(1, job.segmentRepairCountForTest());
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testTheSegmentBehindAParkedOneIsRepairedByTheResumingTurn() throws Exception {
        // The loop position itself. Two closed segments and nothing at the head: the first
        // parks, and the turn that finishes it owes the second against the same pinned
        // snapshot. A repair count of two is what says the loop carried on rather than
        // being re-derived - a dropped position would leave the change unconsumed and the
        // next drain would repair the first segment again.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1") + ", " + row(5, 2, "acct-2"), job);

                final long baseHead = commit(row(2, 5, "acct-1") + ", " + row(3, 5, "acct-1"), job);

                Assert.assertTrue(
                        "a one-row replay budget must park the first segment of the loop",
                        job.segmentYieldCountForTest() > 0
                );
                Assert.assertEquals(
                        "both segments must be repaired, and each exactly once",
                        2,
                        job.segmentRepairCountForTest()
                );
                Assert.assertEquals(
                        "the loop's last segment must advance the watermark over the whole change",
                        baseHead,
                        viewInstance().getLastProcessedSeqTxn()
                );
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testTheViewIsUnchangedWhileASegmentRepairIsParked() throws Exception {
        // What the yield must not cost: a reader seeing a half-repaired segment. The
        // replacement stays uncommitted in the writer the session holds and no generation
        // names the roots it has staged, so the durable view is the pre-repair one for
        // every turn the repair takes.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1") + ", " + row(5, 2, "acct-2"), job);

                final String beforeRepair = viewContents();
                execute("insert into tx values " + row(2, 5, "acct-1"));
                drainWalQueue();
                driveUntilParked(job);

                TestUtils.assertEquals(
                        "a parked repair must leave the durable view exactly as it found it",
                        beforeRepair,
                        viewContents()
                );

                driveRefreshToQuiescence(job);
                Assert.assertNull(viewInstance().getSuspendedRepair());
                assertViewMatchesRecompute();
            }
        });
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
     * Drives one refresh pass at a time until the view has a repair parked on it, so a
     * caller can read the durable state a reader would see mid-repair. Fails if the repair
     * never parks - which would make every assertion after it vacuous.
     */
    private void driveUntilParked(LiveViewRefreshJob job) {
        for (int i = 0; i < REFRESH_QUIESCENCE_PASSES; i++) {
            setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
            drainWalQueue();
            job.processNotificationsForTest();
            drainWalQueue();
            if (viewInstance().getSuspendedRepair() != null) {
                return;
            }
        }
        Assert.fail("the segment repair never parked on its turn budget");
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

    /**
     * One row of {@code account} at {@code hour} on 2026-01-{@code day}, as an INSERT tuple.
     * The day is what carries the case: with a daily anchor it is also the segment.
     */
    private String row(int day, int hour, String account) {
        return "('2026-01-" + String.format("%02d", day) + "T" + String.format("%02d", hour)
                + ":00:00.000000Z', '" + account + "', 1.0)";
    }

    /**
     * Two accounts on each of 2026-01-02, 2026-01-03 and 2026-01-04, four rows deep for the
     * first of them. The depth is deliberate: a one-row replay budget only spreads a
     * segment repair across turns if the segment holds more than one row to replay.
     */
    private String seedThreeDays() {
        final StringBuilder rows = new StringBuilder();
        for (int day = 2; day <= 4; day++) {
            for (int hour = 1; hour <= 4; hour++) {
                if (rows.length() > 0) {
                    rows.append(", ");
                }
                rows.append(row(day, hour, "acct-1"));
            }
            rows.append(", ").append(row(day, 1, "acct-2"));
        }
        return rows.toString();
    }

    private String viewContents() throws Exception {
        final StringSink out = new StringSink();
        printSql("lv order by 2, 1", out);
        return out.toString();
    }

    private LiveViewInstance viewInstance() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull("live view 'lv' must be registered", instance);
        return instance;
    }
}
