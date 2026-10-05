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
import io.questdb.cairo.lv.LiveViewCheckpointGenerationPin;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointRowPositionDeltaReader;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineReader;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewWindow;
import io.questdb.std.LongList;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Coverage for the per-segment repair: a correction repairs and publishes over the anchor
 * segments it actually touches, rather than over one union range running from the anchor
 * below the deepest correction to the frontier.
 * <p>
 * The union range is what makes a deep correction expensive, and it is expensive twice
 * over: the replay reads every base row in the range, and the apply merges rather than
 * appends every live-view partition it covers and rewrites each of them whole. A commit
 * carrying rows at the head and rows a month back therefore rewrites a month of output for
 * the sake of a few thousand rows - while the rows themselves reach one old segment and the
 * head, and nothing in between.
 * <p>
 * Every case here holds two things at once. The from-base recompute oracle says the output
 * is right, and the replay counters say the repair did the work of the segments it touched
 * rather than of the distance it reached - which is the whole claim, and the one an
 * end-state comparison cannot see: a union repair produces exactly the same rows.
 * <p>
 * One case holds a third thing, because the counters cannot see it either: the cumulative
 * row positions the repaired segment's boundaries carry, and the ones the boundaries above
 * it inherit from the segment's point add. Nothing reads those until a restart resumes from
 * one of them, so the case reads them directly off the published timeline.
 * <p>
 * The view is the reported customer shape: an anchored WINDOW carrying an unbounded
 * cumulative sum and count per account, over a base whose timestamps span several anchor
 * days so closed segments exist at all.
 */
public class LiveViewCheckpointSegmentRepairTest extends AbstractLiveViewTest {

    private static final int ENTRY_CHECKPOINT_ID = 1;
    private static final int ENTRY_EFFECTIVE_POSITION = 5;
    private static final int ENTRY_MAX_TIMESTAMP = 0;
    private static final int ENTRY_ROOT_LENGTH = 4;
    private static final int ENTRY_ROOT_OFFSET = 3;
    private static final int ENTRY_ROOT_SEGMENT = 2;
    private static final int ENTRY_SIZE = 6;
    private static final long FLUSH_EVERY_MICROS = 3_600_000_000L;
    private static final String INDEXED_KEY = "symbol nocache index capacity 4";
    // The newest root while acct-2's tied row sits in the un-flushed lead. flushNext moves the
    // clock an hour, past the default checkpoint duration, so the flush of acct-1's row at the
    // tie seals a root there. The lead's row grows that root's timestamp group, and the
    // head-grown check alone decides the repair's route.
    private static final String NEWEST_ROOT_AT_THE_TIE = "2026-01-04T03:00:00.000000Z";
    // The newest root when a checkpoint duration far above FLUSH EVERY holds that seal off, as
    // a FLUSH EVERY shorter than the checkpoint duration does in production: the seed's own
    // maximum. The group does not grow, so the hand-off's lead ceiling and the gate's
    // discarded-lead term decide the route.
    private static final String NEWEST_ROOT_BELOW_THE_TIE = "2026-01-04T02:00:00.000000Z";
    // The view after the in-order head rows at 2026-01-04T04 (acct-2) and T05 (acct-1) and the
    // late row at 2026-01-02T03 (acct-2), each folded exactly once.
    private static final String ROWS_FOLDED_ONCE = """
            created_at\taccount_id\tcumulative_sum\tcumulative_count
            2026-01-02T01:00:00.000000Z\tacct-1\t1.0\t1
            2026-01-02T02:00:00.000000Z\tacct-2\t1.0\t1
            2026-01-02T03:00:00.000000Z\tacct-2\t2.0\t2
            2026-01-03T01:00:00.000000Z\tacct-1\t1.0\t1
            2026-01-03T02:00:00.000000Z\tacct-2\t1.0\t1
            2026-01-04T01:00:00.000000Z\tacct-1\t1.0\t1
            2026-01-04T02:00:00.000000Z\tacct-2\t1.0\t1
            2026-01-04T03:00:00.000000Z\tacct-1\t2.0\t2
            2026-01-04T04:00:00.000000Z\tacct-2\t2.0\t2
            2026-01-04T05:00:00.000000Z\tacct-1\t3.0\t3
            """;
    // The view after acct-1's flushed row and acct-2's un-flushed lead row at 2026-01-04T03 and
    // the late row at 2026-01-02T03 (acct-2), each emitted exactly once.
    private static final String TIED_LEAD_ROW_KEPT = """
            created_at\taccount_id\tcumulative_sum\tcumulative_count
            2026-01-02T01:00:00.000000Z\tacct-1\t1.0\t1
            2026-01-02T02:00:00.000000Z\tacct-2\t1.0\t1
            2026-01-02T03:00:00.000000Z\tacct-2\t2.0\t2
            2026-01-03T01:00:00.000000Z\tacct-1\t1.0\t1
            2026-01-03T02:00:00.000000Z\tacct-2\t1.0\t1
            2026-01-04T01:00:00.000000Z\tacct-1\t1.0\t1
            2026-01-04T02:00:00.000000Z\tacct-2\t1.0\t1
            2026-01-04T03:00:00.000000Z\tacct-1\t2.0\t2
            2026-01-04T03:00:00.000000Z\tacct-2\t2.0\t2
            """;
    // The two head rows a restarted view emits over the restored accumulators.
    private static final String TIED_LEAD_ROWS_AFTER_RESTART = """
            2026-01-04T08:00:00.000000Z\tacct-1\t3.0\t3
            2026-01-04T09:00:00.000000Z\tacct-2\t3.0\t3
            """;

    @Test
    public void testABoundedFrameBesideTheAnchorKeepsTheUnionRange() throws Exception {
        // A bounded ROWS frame declared beside the anchored window keeps sliding across the
        // segment boundary, so a row in a closed segment still changes a later segment's
        // output and the segments are not independent. The decomposition must decline - and
        // the repair must still be correct, on the route it always took.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            execute("create table tx (created_at timestamp, account_id symbol nocache index capacity 4, "
                    + "amount double) timestamp(created_at) partition by hour wal");
            execute("insert into tx values " + seedThreeDays());
            drainWalQueue();
            execute("create live view lv flush every 100ms start from beginning as "
                    + "select created_at, account_id, sum(amount) over w as cumulative_sum, "
                    + "sum(amount) over (partition by account_id order by created_at "
                    + "rows between 3 preceding and current row) as windowed_sum "
                    + "from tx window w as (partition by account_id order by created_at anchor daily '00:00')");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 3, "acct-1"), job);

                // One row back in the first day, which is a closed segment for the anchor -
                // and not a segment this view may repair on its own.
                commit(row(1, 3, "acct-1"), job);
                Assert.assertEquals(
                        "a view carrying a bounded frame beside its anchor must take the union range",
                        0,
                        job.segmentRepairCountForTest()
                );
                assertNoRefreshFaults("lv");
                assertBoundedViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testACorrectionInOneClosedSegmentReplacesThatSegmentAlone() throws Exception {
        // One boundary per commit, so the ladder under the correction is dense enough that a
        // resume would look cheap - which is exactly the case the decomposition has to win:
        // the anchor a dense cadence leaves just below an old correction is the anchor whose
        // resume replays every row above it, all the way to the frontier.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                // Head rows, so the runtime's own segment is the fifth day and the three
                // seeded days are all closed below it.
                commit(row(5, 1, "acct-1") + ", " + row(5, 2, "acct-2"), job);

                final long resumeBefore = viewInstance().getO3ResumeReplayRows();
                final long boundaryBefore = viewInstance().getO3BoundaryReplayRows();

                // A commit reaching back into the second day and forward at the head, which
                // is the shape of the production workload's deep commits: rows at the
                // frontier beside rows in one old segment.
                commit(row(2, 3, "acct-1") + ", " + row(5, 3, "acct-2"), job);

                Assert.assertEquals(
                        "the second day must be repaired as a segment of its own",
                        1,
                        job.segmentRepairCountForTest()
                );
                // The segment repair emits its segment's rows at or above the correction -
                // one row, here - rather than everything from the anchor below the
                // correction to the end of the base table, which is five.
                Assert.assertEquals(
                        "the segment repair must emit only the corrected segment's tail",
                        1,
                        viewInstance().getO3BoundaryReplayRows() - boundaryBefore
                );
                // The residual - the head row that arrived in the same commit - takes the
                // ordinary resume, which Fix 2 already bounds to one cadence.
                Assert.assertTrue(
                        "the residual must still repair through a resume",
                        viewInstance().getO3ResumeReplayRows() > resumeBefore
                );
                assertViewMatchesRecompute();
            }

            // The ladder the segment repair spliced is only worth keeping if it restores.
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                Assert.assertTrue(viewInstance().isCheckpointRestoreSucceeded());
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testASegmentRepairKeepsTheNextSealOnTheIncrementalPath() throws Exception {
        // A converging repair runs through the compiled factory's own window functions, so it
        // wipes them to identity before the replay and puts the pre-repair state back from the
        // scratch overlay afterwards. Both halves of that exchange go through the contract a
        // checkpoint restore reads state under, which deliberately leaves every target owing a
        // complete freeze - it clears the baseline generation, drops the dirty set and raises
        // the full-scan flag. LiveViewCheckpointSealCarryover carries the bookkeeping across
        // instead, so the seal that follows images the keys its own batch touched.
        //
        // The two assertions are one claim in two halves. The flag says the window came out of
        // the repair still holding a baseline; the freeze key count says the seal after it
        // actually imaged one key out of the four the domain holds. Neither is visible in the
        // published artifacts - an incremental root and a complete one both name the whole
        // domain, because the incremental one keeps every key it did not touch from its
        // predecessor.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedFourAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1") + ", " + row(5, 2, "acct-2"), job);
                Assert.assertFalse(
                        "an ordinary cadence seal must leave the window on the incremental path",
                        anchorWindow().isCheckpointFullScanRequired()
                );

                commit(row(2, 3, "acct-1"), job);
                Assert.assertEquals(
                        "the correction must be repaired as a segment of its own",
                        1,
                        job.segmentRepairCountForTest()
                );
                Assert.assertFalse(
                        "a segment repair must leave the window holding its baseline",
                        anchorWindow().isCheckpointFullScanRequired()
                );

                // One ordinary forward row on one account. The cadence seal it triggers stands
                // on the root the repair left in place and owes only that account's key.
                commit(row(5, 4, "acct-3"), job);
                Assert.assertEquals(
                        "the seal after a segment repair must image the keys its own batch touched",
                        1,
                        anchorWindow().getCheckpointLastFreezeKeyCount()
                );
                assertViewMatchesRecompute();
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                Assert.assertTrue(viewInstance().isCheckpointRestoreSucceeded());
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testASegmentRepairDoesNotLoseTheDirtyKeysItCarries() throws Exception {
        // The other half of the carryover's contract, and the one an incremental seal cannot
        // be made cheap without: a target that put its baseline back without also putting the
        // dirty set back would publish a root missing exactly the keys that set named. Nothing
        // detects that at the seal - the root is well formed and names the whole domain - and
        // nothing detects it at read time either, because the runtime still holds the state.
        // Only a restart does, by restoring the root and finding an account's accumulator short.
        //
        // A cadence far above what the case commits is what puts keys in the set at repair
        // time: the seed's own seal is the last one before the correction, so acct-3's head row
        // is still pending when the repair wipes the runtime. The post-repair seal is then the
        // one that has to freeze it.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000);
        assertMemoryLeak(() -> {
            createView(seedFourAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                // Above the seed's boundary and below the cadence, so it moves acct-3's
                // accumulator and leaves the key pending rather than sealed.
                commit(row(5, 1, "acct-3"), job);

                commit(row(2, 3, "acct-1"), job);
                Assert.assertEquals(
                        "the correction must be repaired as a segment of its own",
                        1,
                        job.segmentRepairCountForTest()
                );
                Assert.assertEquals(
                        "the post-repair seal must image the carried dirty keys, not the domain",
                        1,
                        anchorWindow().getCheckpointLastFreezeKeyCount()
                );
                assertViewMatchesRecompute();
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                Assert.assertTrue(viewInstance().isCheckpointRestoreSucceeded());
                driveRefreshToQuiescence(job);
                // The row that reads the restored accumulator back. A root sealed over a lost
                // dirty set holds acct-3's pre-commit image, so this row's cumulative sum comes
                // out one row short of the recompute's.
                commit(row(5, 5, "acct-3"), job);
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testThePerSegmentRepairCanBeTurnedOff() throws Exception {
        // The escape hatch, and the control column a measurement runs against: the same
        // correction on the same view, on the route every repair took before the change set
        // was decomposed.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_PER_SEGMENT_ENABLED, "false");
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1") + ", " + row(5, 2, "acct-2"), job);
                commit(row(2, 3, "acct-1") + ", " + row(5, 3, "acct-2"), job);
                Assert.assertEquals(
                        "the decomposition must be off",
                        0,
                        job.segmentRepairCountForTest()
                );
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testCorrectionsInTwoClosedSegmentsLeaveTheSegmentBetweenThemAlone() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1") + ", " + row(5, 2, "acct-2"), job);

                final long resumeBefore = viewInstance().getO3ResumeReplayRows();
                final long boundaryBefore = viewInstance().getO3BoundaryReplayRows();

                // One commit reaching two closed segments two days apart, and nothing at
                // the head. The union range would run from the anchor below the second day
                // to the end of the base table and rewrite the third, fourth and fifth days
                // on the way; the two segments hold one changed row each, and the third day
                // between them holds none.
                commit(row(2, 3, "acct-1") + ", " + row(4, 3, "acct-1"), job);

                Assert.assertEquals(
                        "both corrected days must be repaired as segments of their own",
                        2,
                        job.segmentRepairCountForTest()
                );
                Assert.assertEquals(
                        "the two segment repairs must emit one row each and nothing between them",
                        2,
                        viewInstance().getO3BoundaryReplayRows() - boundaryBefore
                );
                // Nothing was left above the closed segments, so the last of them advanced
                // the watermark and no residual repair ran at all.
                Assert.assertEquals(
                        "a change set held entirely in closed segments needs no resume",
                        resumeBefore,
                        viewInstance().getO3ResumeReplayRows()
                );
                assertViewMatchesRecompute();
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                Assert.assertTrue(viewInstance().isCheckpointRestoreSucceeded());
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testACommitApplyRacedPastTheTriggerIsClassifiedIntoItsOwnSegment() throws Exception {
        // The decomposition classifies the range the repair re-materialises, and that is not
        // the range the drain read. The drain breaks on the first out-of-order commit, while
        // ApplyWal2TableJob has already applied whatever the base committed after it - and the
        // watermark the repair advances consumes those apply-ahead commits too. A
        // decomposition stopping at the trigger would repair the two segments the trigger
        // reached, declare the ahead commit consumed, and leave the view permanently wrong in
        // the third.
        //
        // The ahead commit also corrects an account the trigger never names, which is what
        // makes its key domain its own rather than the trigger's. Here the segments read
        // whole, so the key buys the shape rather than an assertion;
        // LiveViewCheckpointKeyedReplayTest holds it on the route that reads by key.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                // Head rows, so the runtime's own segment is the fifth day and the three
                // seeded days are all closed below it.
                commit(row(5, 1, "acct-1") + ", " + row(5, 2, "acct-2"), job);

                final long resumeBefore = viewInstance().getO3ResumeReplayRows();
                final long boundaryBefore = viewInstance().getO3BoundaryReplayRows();

                // The trigger reaches the second and third days on one account; the commit
                // apply raced past it reaches the fourth on another.
                commitWithApplyAhead(
                        row(2, 3, "acct-1") + ", " + row(3, 3, "acct-1"),
                        row(4, 3, "acct-2"),
                        job
                );

                Assert.assertEquals(
                        "the day the ahead commit corrected must be repaired as a segment of its own",
                        3,
                        job.segmentRepairCountForTest()
                );
                Assert.assertEquals(
                        "each of the three repairs must emit its own segment's tail and nothing between them",
                        3,
                        viewInstance().getO3BoundaryReplayRows() - boundaryBefore
                );
                Assert.assertEquals(
                        "a change set held entirely in closed segments needs no resume",
                        resumeBefore,
                        viewInstance().getO3ResumeReplayRows()
                );
                assertViewMatchesRecompute();
            }

            // Three spliced ladders, and a restart is the only thing that reads the cumulative
            // positions they stamped.
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                Assert.assertTrue(viewInstance().isCheckpointRestoreSucceeded());
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testInOrderRowsDrainedBeforeALateCommitAreFoldedOnce() throws Exception {
        // Three commits land before the next refresh pass: two in order, then one into a
        // closed day. The drain feeds the in-order rows through the window functions before
        // it reaches the late commit, and the hand-off rewinds the frontier but not the
        // accumulators. A per-segment repair kept that primary runtime, sealed it as a root
        // at the rewound frontier, and the residual repair resumed from that root and folded
        // the in-order rows a second time.
        assertDrainedInOrderRowsFoldOnce(
                INDEXED_KEY,
                "",
                () -> {
                    execute("INSERT INTO tx VALUES " + row(4, 4, "acct-2"));
                    execute("INSERT INTO tx VALUES " + row(4, 5, "acct-1"));
                    execute("INSERT INTO tx VALUES " + row(2, 3, "acct-2"));
                },
                ROWS_FOLDED_ONCE,
                """
                        2026-01-04T08:00:00.000000Z\tacct-1\t4.0\t4
                        2026-01-04T09:00:00.000000Z\tacct-2\t3.0\t3
                        """
        );
    }

    @Test
    public void testInOrderRowsDrainedBeforeALateCommitAreFoldedOnceInAFilteredView() throws Exception {
        // A filter denies the open segment's keyed resume, so the decomposition stops at the
        // closed-segment gate rather than walking the change set first.
        assertDrainedInOrderRowsFoldOnce(
                INDEXED_KEY,
                " WHERE amount > 0",
                () -> {
                    execute("INSERT INTO tx VALUES " + row(4, 4, "acct-2"));
                    execute("INSERT INTO tx VALUES " + row(4, 5, "acct-1"));
                    execute("INSERT INTO tx VALUES " + row(2, 3, "acct-2"));
                },
                ROWS_FOLDED_ONCE,
                """
                        2026-01-04T08:00:00.000000Z\tacct-1\t4.0\t4
                        2026-01-04T09:00:00.000000Z\tacct-2\t3.0\t3
                        """
        );
    }

    @Test
    public void testInOrderRowsDrainedBeforeALateCommitAreFoldedOnceOverAnUnindexedKey() throws Exception {
        // Without a posting index no segment has a keyed read, so every segment replays whole.
        assertDrainedInOrderRowsFoldOnce(
                "symbol",
                "",
                () -> {
                    execute("INSERT INTO tx VALUES " + row(4, 4, "acct-2"));
                    execute("INSERT INTO tx VALUES " + row(4, 5, "acct-1"));
                    execute("INSERT INTO tx VALUES " + row(2, 3, "acct-2"));
                },
                ROWS_FOLDED_ONCE,
                """
                        2026-01-04T08:00:00.000000Z\tacct-1\t4.0\t4
                        2026-01-04T09:00:00.000000Z\tacct-2\t3.0\t3
                        """
        );
    }

    @Test
    public void testInOrderRowsDrainedBeforeALateCommitCarryingANewMaxAreFoldedOnce() throws Exception {
        // The late commit also raises the frontier, so its own head row joins the residual.
        assertDrainedInOrderRowsFoldOnce(
                INDEXED_KEY,
                "",
                () -> {
                    execute("INSERT INTO tx VALUES " + row(4, 4, "acct-2"));
                    execute("INSERT INTO tx VALUES " + row(4, 5, "acct-1"));
                    execute("INSERT INTO tx VALUES " + row(2, 3, "acct-2") + ", " + row(4, 6, "acct-1"));
                },
                ROWS_FOLDED_ONCE + "2026-01-04T06:00:00.000000Z\tacct-1\t4.0\t4\n",
                """
                        2026-01-04T08:00:00.000000Z\tacct-1\t5.0\t5
                        2026-01-04T09:00:00.000000Z\tacct-2\t3.0\t3
                        """
        );
    }

    @Test
    public void testSeveralInOrderCommitsDrainedBeforeALateCommitAreFoldedOnce() throws Exception {
        // More than one in-order commit shares the drain pass with the late one, so the
        // rewound frontier trails the accumulators by several rows across both accounts.
        assertDrainedInOrderRowsFoldOnce(
                INDEXED_KEY,
                "",
                () -> {
                    execute("INSERT INTO tx VALUES " + row(4, 4, "acct-2"));
                    execute("INSERT INTO tx VALUES " + row(4, 5, "acct-1"));
                    execute("INSERT INTO tx VALUES " + row(4, 6, "acct-2"));
                    execute("INSERT INTO tx VALUES " + row(4, 7, "acct-1"));
                    execute("INSERT INTO tx VALUES " + row(2, 3, "acct-2"));
                },
                ROWS_FOLDED_ONCE
                        + "2026-01-04T06:00:00.000000Z\tacct-2\t3.0\t3\n"
                        + "2026-01-04T07:00:00.000000Z\tacct-1\t4.0\t4\n",
                """
                        2026-01-04T08:00:00.000000Z\tacct-1\t5.0\t5
                        2026-01-04T09:00:00.000000Z\tacct-2\t4.0\t4
                        """
        );
    }

    @Test
    public void testATiedLeadRowSurvivesALateCommitCarryingANewMaxInAFilteredView() throws Exception {
        // The late commit also carries a head row, so the residual is not empty. The loop's
        // residual used to resume from the root the ordinary cadence sealed at the flushed
        // row's timestamp - strictly below the residual, but tied with the lead row it never
        // held - and replay from one tick above it, so the lead row was lost there too.
        assertTiedLeadRowSurvivesALateCommit(
                INDEXED_KEY,
                " WHERE amount > 0",
                false,
                NEWEST_ROOT_AT_THE_TIE,
                row(2, 3, "acct-2") + ", " + row(4, 4, "acct-1"),
                TIED_LEAD_ROW_KEPT + "2026-01-04T04:00:00.000000Z\tacct-1\t3.0\t3\n",
                """
                        2026-01-04T08:00:00.000000Z\tacct-1\t4.0\t4
                        2026-01-04T09:00:00.000000Z\tacct-2\t3.0\t3
                        """,
                0
        );
    }

    @Test
    public void testATiedLeadRowSurvivesALateCommitCarryingANewMaxInAFilteredViewWithTheNewestRootBelowTheTie() throws Exception {
        // The late commit's own head row already lifts the change ceiling above the lead, so
        // only the gate's discarded-lead term keeps the closed-segment loop from repairing the
        // late day alone.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_MAX_DURATION_MICROS, 7 * 24 * FLUSH_EVERY_MICROS);
        assertTiedLeadRowSurvivesALateCommit(
                INDEXED_KEY,
                " WHERE amount > 0",
                false,
                NEWEST_ROOT_BELOW_THE_TIE,
                row(2, 3, "acct-2") + ", " + row(4, 4, "acct-1"),
                TIED_LEAD_ROW_KEPT + "2026-01-04T04:00:00.000000Z\tacct-1\t3.0\t3\n",
                """
                        2026-01-04T08:00:00.000000Z\tacct-1\t4.0\t4
                        2026-01-04T09:00:00.000000Z\tacct-2\t3.0\t3
                        """,
                0
        );
    }

    @Test
    public void testATiedLeadRowSurvivesALateCommitInAFilteredView() throws Exception {
        // A row flushed at 2026-01-04T03, a second row at the same timestamp still in the
        // un-flushed lead, then a late row into a closed day. The frontier stands at the
        // durable maximum, so the frontier comparison reads the lead as durable. A filter
        // denies the keyed walk that would put the lead's commit in the residual, so the
        // closed-segment loop repaired the late day alone and the tied row was never
        // re-emitted.
        assertTiedLeadRowSurvivesALateCommit(
                INDEXED_KEY,
                " WHERE amount > 0",
                false,
                NEWEST_ROOT_AT_THE_TIE,
                row(2, 3, "acct-2"),
                TIED_LEAD_ROW_KEPT,
                TIED_LEAD_ROWS_AFTER_RESTART,
                0
        );
    }

    @Test
    public void testATiedLeadRowSurvivesALateCommitInAFilteredViewWithTheNewestRootBelowTheTie() throws Exception {
        // The production shape: a FLUSH EVERY shorter than the checkpoint duration flushes the
        // tie without sealing a root there, so the newest root sits below the tie and its
        // timestamp group has not grown. Only the hand-off's naming of the discarded lead keeps
        // the tied row in the repair: the lead's change ceiling, which keeps every H the union
        // range derives above the lead, and the gate's discarded-lead term, which keeps the
        // closed-segment loop from repairing the late day alone.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_MAX_DURATION_MICROS, 7 * 24 * FLUSH_EVERY_MICROS);
        assertTiedLeadRowSurvivesALateCommit(
                INDEXED_KEY,
                " WHERE amount > 0",
                false,
                NEWEST_ROOT_BELOW_THE_TIE,
                row(2, 3, "acct-2"),
                TIED_LEAD_ROW_KEPT,
                TIED_LEAD_ROWS_AFTER_RESTART,
                0
        );
    }

    @Test
    public void testATiedLeadRowSurvivesALateCommitInAFilteredViewWithTheNewestRootBelowTheTieOverAnUnindexedKey() throws Exception {
        // Without a posting index no closed segment has a keyed read, so the loop would
        // replay the late day whole and lose the tied row all the same.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_MAX_DURATION_MICROS, 7 * 24 * FLUSH_EVERY_MICROS);
        assertTiedLeadRowSurvivesALateCommit(
                "symbol",
                " WHERE amount > 0",
                false,
                NEWEST_ROOT_BELOW_THE_TIE,
                row(2, 3, "acct-2"),
                TIED_LEAD_ROW_KEPT,
                TIED_LEAD_ROWS_AFTER_RESTART,
                0
        );
    }

    @Test
    public void testATiedLeadRowSurvivesALateCommitInAFilteredViewWithTheNewestRootBelowTheTieWithoutThePerSegmentRepair() throws Exception {
        // With the decomposition off the gate is never consulted, so the hand-off's lead
        // ceiling alone keeps the union range's H above the lead.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_MAX_DURATION_MICROS, 7 * 24 * FLUSH_EVERY_MICROS);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_PER_SEGMENT_ENABLED, "false");
        assertTiedLeadRowSurvivesALateCommit(
                INDEXED_KEY,
                " WHERE amount > 0",
                false,
                NEWEST_ROOT_BELOW_THE_TIE,
                row(2, 3, "acct-2"),
                TIED_LEAD_ROW_KEPT,
                TIED_LEAD_ROWS_AFTER_RESTART,
                0
        );
    }

    @Test
    public void testATiedLeadRowSurvivesALateCommitInAFilteredViewWithoutThePerSegmentRepair() throws Exception {
        // The union range had the same blind spot: at the tie it derived a finite H at the
        // end of the late day, kept the primary runtime and replaced nothing above that day.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_PER_SEGMENT_ENABLED, "false");
        assertTiedLeadRowSurvivesALateCommit(
                INDEXED_KEY,
                " WHERE amount > 0",
                false,
                NEWEST_ROOT_AT_THE_TIE,
                row(2, 3, "acct-2"),
                TIED_LEAD_ROW_KEPT,
                TIED_LEAD_ROWS_AFTER_RESTART,
                0
        );
    }

    @Test
    public void testATiedLeadRowSurvivesALateCommitInARangeFramedView() throws Exception {
        // The union range's blind spot is not the anchor's alone. A RANGE frame derives
        // H = changeMaxTs + W + 1, which at the tie sits far below the lead row.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL, amount DOUBLE) "
                    + "TIMESTAMP(created_at) PARTITION BY HOUR WAL");
            execute("INSERT INTO tx VALUES " + seedThreeDays());
            drainWalQueue();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1h START FROM BEGINNING AS "
                    + "SELECT created_at, account_id, sum(amount) OVER (PARTITION BY account_id ORDER BY created_at "
                    + "RANGE BETWEEN 1 HOUR PRECEDING AND CURRENT ROW) AS windowed_sum "
                    + "FROM tx WHERE amount > 0");
            final String expected = """
                    created_at\taccount_id\twindowed_sum
                    2026-01-02T01:00:00.000000Z\tacct-1\t1.0
                    2026-01-02T02:00:00.000000Z\tacct-2\t1.0
                    2026-01-02T03:00:00.000000Z\tacct-2\t2.0
                    2026-01-03T01:00:00.000000Z\tacct-1\t1.0
                    2026-01-03T02:00:00.000000Z\tacct-2\t1.0
                    2026-01-04T01:00:00.000000Z\tacct-1\t1.0
                    2026-01-04T02:00:00.000000Z\tacct-2\t1.0
                    2026-01-04T03:00:00.000000Z\tacct-1\t1.0
                    2026-01-04T03:00:00.000000Z\tacct-2\t2.0
                    """;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                leaveATiedRowInTheLead(job, row(2, 3, "acct-2"));
                assertRangeViewReturns(expected);
                flushNext(job);
                assertRangeViewReturns(expected);
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                driveRefreshToQuiescence(job);
                assertRangeViewReturns(expected);
                execute("INSERT INTO tx VALUES " + row(4, 4, "acct-1") + ", " + row(4, 4, "acct-2"));
                flushNext(job);
                assertRangeViewReturns(expected + """
                        2026-01-04T04:00:00.000000Z\tacct-1\t2.0
                        2026-01-04T04:00:00.000000Z\tacct-2\t2.0
                        """);
            }
        });
    }

    @Test
    public void testATiedLeadRowSurvivesALateCommitInAnUnfilteredView() throws Exception {
        // Without a filter the walk starts at the applied point and reaches the lead's commit,
        // but the closed-segment loop would still keep the primary runtime that holds it and
        // seal it below the lead's seqTxn. So the unfiltered view takes the union range at the
        // tie too.
        assertTiedLeadRowSurvivesALateCommit(
                INDEXED_KEY,
                "",
                false,
                NEWEST_ROOT_AT_THE_TIE,
                row(2, 3, "acct-2"),
                TIED_LEAD_ROW_KEPT,
                TIED_LEAD_ROWS_AFTER_RESTART,
                0
        );
    }

    @Test
    public void testATiedLeadRowSurvivesALateCommitJustAboveARootInAFilteredView() throws Exception {
        // One root per seeded day, so a root sits just below the late row and a resume from
        // it is what the union range picks.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertTiedLeadRowSurvivesALateCommit(
                INDEXED_KEY,
                " WHERE amount > 0",
                true,
                NEWEST_ROOT_AT_THE_TIE,
                row(2, 3, "acct-2"),
                TIED_LEAD_ROW_KEPT,
                TIED_LEAD_ROWS_AFTER_RESTART,
                0
        );
    }

    @Test
    public void testATiedLeadRowSurvivesALateCommitJustAboveARootInAFilteredViewOverAnUnindexedKey() throws Exception {
        // Without a posting index the closed segment had no keyed read; the loop replayed
        // it whole and lost the tied row all the same.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertTiedLeadRowSurvivesALateCommit(
                "symbol",
                " WHERE amount > 0",
                true,
                NEWEST_ROOT_AT_THE_TIE,
                row(2, 3, "acct-2"),
                TIED_LEAD_ROW_KEPT,
                TIED_LEAD_ROWS_AFTER_RESTART,
                0
        );
    }

    @Test
    public void testATiedLeadRowSurvivesALateCommitWithoutThePerSegmentRepair() throws Exception {
        // Without the decomposition an unfiltered view meets the union range's blind spot
        // too.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_PER_SEGMENT_ENABLED, "false");
        assertTiedLeadRowSurvivesALateCommit(
                INDEXED_KEY,
                "",
                false,
                NEWEST_ROOT_AT_THE_TIE,
                row(2, 3, "acct-2"),
                TIED_LEAD_ROW_KEPT,
                TIED_LEAD_ROWS_AFTER_RESTART,
                0
        );
    }

    @Test
    public void testATiedLeadRowSurvivesARestartAfterAFaultedRepairInAnUnfilteredView() throws Exception {
        // The restart restores the newest root and replays the base above the seqTxn the root
        // carries.
        assertTiedLeadRowSurvivesARepairThatRunsNoResume("", true);
    }

    @Test
    public void testATiedLeadRowSurvivesARestartInAFilteredViewWhoseRepairRunsNoResume() throws Exception {
        // The restart restores the newest root and replays the base above the seqTxn the root
        // carries, which must return the tied row exactly once.
        assertTiedLeadRowSurvivesARepairThatRunsNoResume(" WHERE amount > 0", true);
    }

    @Test
    public void testATiedLeadRowSurvivesARestartAfterAKeyedResumeOfTheOpenSegment() throws Exception {
        // A late row in the runtime's own segment leaves the closed-segment loop nothing to
        // repair, so the gate does not decide the route: the walk reaches the lead's commit,
        // the keyed resume recomputes the keys of both, and the repair publishes once at the
        // pinned snapshot. The root it seals must restore without folding the lead again.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_MAX_DURATION_MICROS, 7 * 24 * FLUSH_EVERY_MICROS);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_REFRESH_TURN_MAX_COMMITS, 1);
        assertMemoryLeak(() -> {
            createTiedLeadView("");
            final String expected = """
                    created_at\taccount_id\tcumulative_sum\tcumulative_count
                    2026-01-02T01:00:00.000000Z\tacct-1\t1.0\t1
                    2026-01-02T02:00:00.000000Z\tacct-2\t1.0\t1
                    2026-01-03T01:00:00.000000Z\tacct-1\t1.0\t1
                    2026-01-03T02:00:00.000000Z\tacct-2\t1.0\t1
                    2026-01-04T01:00:00.000000Z\tacct-1\t1.0\t1
                    2026-01-04T02:00:00.000000Z\tacct-2\t1.0\t1
                    2026-01-04T02:30:00.000000Z\tacct-1\t2.0\t2
                    2026-01-04T03:00:00.000000Z\tacct-1\t3.0\t3
                    2026-01-04T03:00:00.000000Z\tacct-2\t2.0\t2
                    """;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                job.setForceOpenSegmentKeyedReplayForTest(true);
                driveRefreshToQuiescence(job);
                holdATiedRowInTheLead(job);
                execute("INSERT INTO tx VALUES ('2026-01-04T02:30:00.000000Z', 'acct-1', 1.0)");
                drainWalQueue();
                driveRefreshToQuiescence(job);
                assertViewReturns(expected);
                Assert.assertEquals(
                        "the open segment must take the keyed resume",
                        1,
                        job.openSegmentKeyedResumeCountForTest()
                );
                Assert.assertEquals(
                        "closed segments repaired over their own range",
                        0,
                        job.segmentRepairCountForTest()
                );
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                assertTiedLeadRowKept(job, expected, """
                        2026-01-04T08:00:00.000000Z\tacct-1\t4.0\t4
                        2026-01-04T09:00:00.000000Z\tacct-2\t3.0\t3
                        """);
            }
        });
    }

    @Test
    public void testATiedLeadRowSurvivesAnInPlaceRestoreAfterAFaultedRepairInAnUnfilteredView() throws Exception {
        // No restart: a fault that lands after a resume wiped the primary runtime has the
        // refresh's failure path restore it in place from the newest root, and the flush that
        // falls due on the turn re-draining the lead's commit makes that commit's row durable.
        assertTiedLeadRowSurvivesARepairThatRunsNoResume("", false);
    }

    @Test
    public void testATiedLeadRowSurvivesTheNextFlushInAFilteredViewWhoseRepairRunsNoResume() throws Exception {
        // No restart: the flush after the repair must leave the tied row durable exactly once.
        assertTiedLeadRowSurvivesARepairThatRunsNoResume(" WHERE amount > 0", false);
    }

    @Test
    public void testRowsBelowTheViewFloorAreDiscardedRatherThanDenyingTheRepair() throws Exception {
        // The denial the cost model attributes 75.5% of all replay to: a correction reaching
        // below the view's own START FROM boundary clamps the correction floor onto that
        // boundary, and a floor landing there is what DENIAL_VIEW_START_FLOOR refuses. Those
        // rows produce no output at all, so they belong out of the change set rather than in
        // charge of it.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            execute("create table tx (created_at timestamp, account_id symbol nocache index capacity 4, "
                    + "amount double) timestamp(created_at) partition by hour wal");
            // A day below the view's floor as well as the three the view holds, so the
            // correction below has real sub-floor history to reach into.
            execute("insert into tx values " + row(1, 1, "acct-1") + ", " + row(1, 2, "acct-2")
                    + ", " + seedThreeDays());
            drainWalQueue();
            execute("create live view lv flush every 100ms start from '2026-01-02T00:00:00.000000Z' as "
                    + "select created_at, account_id, "
                    + "sum(amount) over w as cumulative_sum, "
                    + "count(account_id) over w as cumulative_count "
                    + "from tx window w as (partition by account_id order by created_at anchor daily '00:00')");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1") + ", " + row(5, 2, "acct-2"), job);

                final long boundaryBefore = viewInstance().getO3BoundaryReplayRows();

                // One row under the view's floor, one inside a closed segment above it. The
                // sub-floor row is the deepest thing the commit carries, and the repair must
                // not plan from it.
                commit(row(1, 3, "acct-1") + ", " + row(3, 3, "acct-1"), job);

                Assert.assertEquals(
                        "the sub-floor row must leave the third day scoped as a segment of its own",
                        1,
                        job.segmentRepairCountForTest()
                );
                Assert.assertEquals(
                        "the segment repair must emit the corrected segment's tail alone",
                        1,
                        viewInstance().getO3BoundaryReplayRows() - boundaryBefore
                );
                assertViewMatchesRecompute("2026-01-02T00:00:00.000000Z");
            }
        });
    }

    @Test
    public void testEveryBoundaryInsideARepairedSegmentTakesItsOwnCumulativePosition() throws Exception {
        // A segment repair re-materialises one closed anchor segment and splices its boundaries
        // back in place, and what each of those boundaries owes is its own cumulative row
        // position - the count of live-view rows at or below it, the segment's newly inserted
        // ones included. The ladder is the only place that number lives: no reader detects a
        // wrong one, the view keeps serving correct results out of the runtime, and the first
        // thing to read it is the restart that resumes from one of those roots and credits the
        // view with the rows the root claims.
        //
        // The three corrected rows sit one on each side of the segment's three boundaries -
        // below the first, tied with the second, above the third - because the three cases
        // fail differently. A boundary counts every row at or below it, so the tied row belongs
        // to the boundary it ties with: BoundaryFreezingCursor freezes on the first row
        // STRICTLY above a boundary, which is what admits the complete timestamp group. A
        // freeze one row early takes the tie out of the boundary below it and the ladder is
        // short by exactly that row from there upwards.
        //
        // Above the segment, nothing was recomputed: those boundaries keep the payload roots
        // the cadence wrote, by page identity, and only their cumulative positions move - by
        // the segment's whole delta, through the single point add the repair publishes into
        // LiveViewCheckpointRowPositionDelta rather than through a rewrite of the suffix.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            // One row per commit at a one-row cadence, so each commit seals a boundary of its
            // own and the second day carries three of them - which is what makes this a case
            // about several boundaries inside one repaired segment rather than about one.
            createView(row(2, 1, "acct-1"));
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(2, 3, "acct-1"), job);
                commit(row(2, 5, "acct-1"), job);
                commit(row(3, 1, "acct-1"), job);
                // The head, which closes the second and third days below it.
                commit(row(5, 1, "acct-1"), job);

                final LongList before = snapshotTimeline();
                Assert.assertEquals("one boundary per commit", 5 * ENTRY_SIZE, before.size());
                assertBoundary(before, 0, "2026-01-02T01:00:00.000000Z", 1);
                assertBoundary(before, 1, "2026-01-02T03:00:00.000000Z", 2);
                assertBoundary(before, 2, "2026-01-02T05:00:00.000000Z", 3);
                assertBoundary(before, 3, "2026-01-03T01:00:00.000000Z", 4);
                assertBoundary(before, 4, "2026-01-05T01:00:00.000000Z", 5);

                // Three rows into the second day: one below its first boundary, one tied with
                // its second - a second account, so the tie is in the timestamp alone and the
                // output carries no repeated pair - and one above its third.
                commit(
                        row(2, 0, "acct-1") + ", " + row(2, 3, "acct-2") + ", " + row(2, 6, "acct-1"),
                        job
                );
                Assert.assertEquals(
                        "the second day must be repaired as a segment of its own",
                        1,
                        job.segmentRepairCountForTest()
                );
                // What the segment gained, and therefore what every boundary above it owes.
                final int segmentRowDelta = 3;

                final LongList after = snapshotTimeline();
                Assert.assertEquals(
                        "a splice re-versions the boundaries it repairs and neither drops one nor adds one",
                        before.size(),
                        after.size()
                );

                // Inside the segment: new root versions, and positions counting every row at or
                // below each boundary. 01:00 gains the row at 00:00; 03:00 gains that row and
                // the one tied with it; 05:00 gains nothing further of its own.
                for (int i = 0; i <= 2; i++) {
                    assertNewRoot(before, after, i);
                }
                assertBoundary(after, 0, "2026-01-02T01:00:00.000000Z", 2);
                assertBoundary(after, 1, "2026-01-02T03:00:00.000000Z", 4);
                assertBoundary(after, 2, "2026-01-02T05:00:00.000000Z", 5);

                // Above it: the same payload roots, by page identity, carrying the segment's
                // whole delta - the third corrected row included, which is above every boundary
                // the segment holds and therefore reaches none of their positions.
                for (int i = 3; i <= 4; i++) {
                    assertSameRoot(before, after, i);
                    Assert.assertEquals(
                            "the boundary above the repaired segment at index " + i
                                    + " must pick up the segment's whole delta",
                            before.getQuick(i * ENTRY_SIZE + ENTRY_EFFECTIVE_POSITION) + segmentRowDelta,
                            after.getQuick(i * ENTRY_SIZE + ENTRY_EFFECTIVE_POSITION)
                    );
                }
                assertViewMatchesRecompute();
            }

            // The restart is what reads those positions: it resumes from a root and credits the
            // view with the rows that root claims, so a ladder short by the tied row leaves the
            // resumed runtime disagreeing with the rows on disk.
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                Assert.assertTrue(viewInstance().isCheckpointRestoreSucceeded());
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute();
                commit(row(5, 3, "acct-1"), job);
                assertViewMatchesRecompute();
            }
        });
    }

    private static void assertBoundary(LongList timeline, int index, String maxTimestamp, long effectivePosition) {
        final int base = index * ENTRY_SIZE;
        Assert.assertEquals(
                "boundary timestamp at index " + index,
                ts(maxTimestamp),
                timeline.getQuick(base + ENTRY_MAX_TIMESTAMP)
        );
        Assert.assertEquals(
                "cumulative row position at index " + index,
                effectivePosition,
                timeline.getQuick(base + ENTRY_EFFECTIVE_POSITION)
        );
    }

    private static void assertNewRoot(LongList before, LongList after, int index) {
        final int base = index * ENTRY_SIZE;
        Assert.assertEquals(before.getQuick(base + ENTRY_MAX_TIMESTAMP), after.getQuick(base + ENTRY_MAX_TIMESTAMP));
        Assert.assertEquals(before.getQuick(base + ENTRY_CHECKPOINT_ID), after.getQuick(base + ENTRY_CHECKPOINT_ID));
        Assert.assertTrue(
                "the repaired root at index " + index + " must be a new physical version",
                before.getQuick(base + ENTRY_ROOT_SEGMENT) != after.getQuick(base + ENTRY_ROOT_SEGMENT)
                        || before.getQuick(base + ENTRY_ROOT_OFFSET) != after.getQuick(base + ENTRY_ROOT_OFFSET)
        );
    }

    private static void assertSameRoot(LongList before, LongList after, int index) {
        final int base = index * ENTRY_SIZE;
        for (int field = ENTRY_MAX_TIMESTAMP; field <= ENTRY_ROOT_LENGTH; field++) {
            Assert.assertEquals(
                    "reused root field " + field + " at index " + index,
                    before.getQuick(base + field),
                    after.getQuick(base + field)
            );
        }
    }

    private LiveViewWindow anchorWindow() {
        final LiveViewWindow window = viewInstance().getAnchorWindow();
        Assert.assertNotNull("the view must carry an anchored window", window);
        return window;
    }

    private void assertBoundedViewMatchesRecompute() throws Exception {
        final String bucket = "timestamp_floor('1d', created_at, '1970-01-01T00:00:00.000000Z'::timestamp)";
        final String recompute = "select created_at, account_id, "
                + "sum(amount) over (partition by account_id, bucket order by created_at "
                + "rows between unbounded preceding and current row) as cumulative_sum, "
                + "sum(amount) over (partition by account_id order by created_at "
                + "rows between 3 preceding and current row) as windowed_sum "
                + "from (select created_at, account_id, amount, " + bucket + " as bucket from tx)";
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

    /**
     * Seeds three anchor days, flushes one in-order row so the runtime frontier sits above the
     * newest root the default checkpoint cadence has sealed, then lets {@code trigger} land
     * in-order commits followed by a late commit into a closed day, all before the next refresh
     * pass. The view must return {@code expected}, and match the from-base recompute, after the
     * repair; again after a restart restores the root the repair left behind; and once more after
     * two head rows read the restored accumulators back.
     * <p>
     * The checkpoint cadence stays at its default on purpose. With a one-row cadence a correct
     * root already sits at the runtime frontier, the post-repair seal declines to seal over it,
     * and a double fold never surfaces.
     */
    private void assertDrainedInOrderRowsFoldOnce(
            String keyColumnType,
            String filter,
            TestUtils.LeakProneCode trigger,
            String expected,
            String expectedRowsAfterRestart
    ) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tx (created_at TIMESTAMP, account_id " + keyColumnType + ", "
                    + "amount DOUBLE) TIMESTAMP(created_at) PARTITION BY HOUR WAL");
            execute("INSERT INTO tx VALUES " + seedThreeDays());
            drainWalQueue();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                    + "SELECT created_at, account_id, "
                    + "sum(amount) OVER w AS cumulative_sum, "
                    + "count(account_id) OVER w AS cumulative_count "
                    + "FROM tx" + filter + " "
                    + "WINDOW w AS (PARTITION BY account_id ORDER BY created_at ANCHOR DAILY '00:00')");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(4, 3, "acct-1"), job);
                trigger.run();
                drainWalQueue();
                driveRefreshToQuiescence(job);
                assertViewReturns(expected);
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                driveRefreshToQuiescence(job);
                assertViewReturns(expected);
                commit(row(4, 8, "acct-1") + ", " + row(4, 9, "acct-2"), job);
                assertViewReturns(expected + expectedRowsAfterRestart);
            }
        });
    }

    private void assertRangeViewReturns(String expected) throws Exception {
        assertQuery("SELECT * FROM lv")
                .noLeakCheck()
                .timestamp("created_at")
                .expectSize()
                .returns(expected);
        assertNoRefreshFaults("lv");
    }

    /**
     * Asserts the view returns {@code expected}, which holds the tied row exactly once, then
     * lands two head rows that read the accumulators back and asserts they extend it by
     * {@code expectedHeadRows}.
     */
    private void assertTiedLeadRowKept(
            LiveViewRefreshJob job,
            String expected,
            String expectedHeadRows
    ) throws Exception {
        assertViewReturns(expected);
        execute("INSERT INTO tx VALUES " + row(4, 8, "acct-1") + ", " + row(4, 9, "acct-2"));
        flushNext(job);
        assertViewReturns(expected + expectedHeadRows);
    }

    /**
     * Leaves a row tied with the durable maximum in the un-flushed lead, then lands
     * {@code lateRows} before the next flush: a row at 2026-01-04T03 is flushed, a second row
     * at the same timestamp stays in the lead, and the late commit triggers the repair. The view
     * must return {@code expected}, and match the from-base recompute, after the repair; again
     * after the next flush; again after a restart; and once more after two head rows read the
     * restored accumulators back.
     * <p>
     * {@code FLUSH EVERY 1h} keeps the lead un-flushed for as long as the test does not move the
     * clock past it, and {@link #flushNext} is the only call that does.
     *
     * @param isSeedSealedPerDay      seeds each day in a commit and a flush of its own, so that
     *                                under a one-row cadence a root sits just below every day's
     *                                last row
     * @param newestRootMaxTs         the newest root's maxTimestamp while the tied row sits in
     *                                the lead, {@link #NEWEST_ROOT_AT_THE_TIE} or
     *                                {@link #NEWEST_ROOT_BELOW_THE_TIE}, which pins the shape
     *                                the case covers
     * @param expectedSegmentRepairs  the closed segments the repair takes over their own range,
     *                                which pins the route the tied row survived
     */
    private void assertTiedLeadRowSurvivesALateCommit(
            String keyColumnType,
            String filter,
            boolean isSeedSealedPerDay,
            String newestRootMaxTs,
            String lateRows,
            String expected,
            String expectedRowsAfterRestart,
            long expectedSegmentRepairs
    ) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tx (created_at TIMESTAMP, account_id " + keyColumnType + ", "
                    + "amount DOUBLE) TIMESTAMP(created_at) PARTITION BY HOUR WAL");
            if (!isSeedSealedPerDay) {
                execute("INSERT INTO tx VALUES " + seedThreeDays());
                drainWalQueue();
            }
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1h START FROM BEGINNING AS "
                    + "SELECT created_at, account_id, "
                    + "sum(amount) OVER w AS cumulative_sum, "
                    + "count(account_id) OVER w AS cumulative_count "
                    + "FROM tx" + filter + " "
                    + "WINDOW w AS (PARTITION BY account_id ORDER BY created_at ANCHOR DAILY '00:00')");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                if (isSeedSealedPerDay) {
                    for (int day = 2; day <= 4; day++) {
                        execute("INSERT INTO tx VALUES " + row(day, 1, "acct-1") + ", " + row(day, 2, "acct-2"));
                        flushNext(job);
                    }
                }
                Assert.assertEquals(
                        "the newest root while the tied row sits in the lead",
                        ts(newestRootMaxTs),
                        leaveATiedRowInTheLead(job, lateRows)
                );
                assertViewReturns(expected);
                Assert.assertEquals(
                        "closed segments repaired over their own range",
                        expectedSegmentRepairs,
                        job.segmentRepairCountForTest()
                );
                flushNext(job);
                assertViewReturns(expected);
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                driveRefreshToQuiescence(job);
                assertViewReturns(expected);
                execute("INSERT INTO tx VALUES " + row(4, 8, "acct-1") + ", " + row(4, 9, "acct-2"));
                flushNext(job);
                assertViewReturns(expected + expectedRowsAfterRestart);
            }
        });
    }

    /**
     * Leaves a row tied with the durable maximum in the un-flushed lead, with the newest root
     * below the tie, lands a late row in a closed day under a one-commit turn budget, and arms a
     * fault ahead of the first resume replay the refresh runs. The view must then return the
     * from-base recompute after a restart, or after the next flush in place, and once more after
     * two head rows read the accumulators back.
     * <p>
     * The armed fault is a tripwire, not the case's subject. The repair takes the union range at
     * the tie and publishes once, at the pinned snapshot, so it runs no resume and the fault
     * must stay unfired. In an unfiltered view the closed-segment loop used to repair the late
     * day on its own and keep the primary runtime, which still held the discarded lead's
     * commit. The seal after the segment froze that runtime as a root at the tie, stamped with
     * the base seqTxn below the lead, and only the residual's publication superseded it. The
     * fault ahead of the residual's resume left that root newest, so the restore put the lead's
     * commit back into the runtime and the next drain folded it a second time. A one-commit turn
     * budget lets the drain flush that row before the late commit reaches a repair of its own,
     * and that repair covers the late day alone.
     * <p>
     * In a filtered view the filter denies the keyed walk, so no residual reaches the lead's
     * commit and the routes that lose the tied row run no resume for the fault to stop: the
     * closed-segment loop repairs the late day alone, or a finite H at the end of the late day
     * keeps the primary runtime. The hand-off's lead ceiling and the gate's discarded-lead term
     * are all that keep the tied row in the repair, and the row assertions catch either route.
     * <p>
     * A checkpoint duration far above {@code FLUSH EVERY} keeps the flush at the tie from
     * sealing a root there, so the newest root sits below the tie.
     *
     * @param filter      the view's WHERE clause, with a leading space, or empty
     * @param isRestarted restarts the view after the repair rather than flushing in place
     */
    private void assertTiedLeadRowSurvivesARepairThatRunsNoResume(String filter, boolean isRestarted) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_MAX_DURATION_MICROS, 7 * 24 * FLUSH_EVERY_MICROS);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_REFRESH_TURN_MAX_COMMITS, 1);
        assertMemoryLeak(() -> {
            createTiedLeadView(filter);
            final AtomicBoolean hasResumeReplayStarted = new AtomicBoolean();
            final long segmentRepairs;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                Assert.assertEquals(
                        "the newest root while the tied row sits in the lead",
                        ts(NEWEST_ROOT_BELOW_THE_TIE),
                        holdATiedRowInTheLead(job)
                );
                job.setSimulateResumeReplayStartForTest(() -> {
                    hasResumeReplayStarted.set(true);
                    throw CairoException.critical(0).put("simulated fault ahead of a resume replay");
                });
                execute("INSERT INTO tx VALUES " + row(2, 3, "acct-2"));
                drainWalQueue();
                // One burst at a fixed clock: a faulted turn leaves the view backing off, so the
                // burst ends at the fault rather than retrying past it.
                setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                drainJob(job);
                segmentRepairs = job.segmentRepairCountForTest();
                if (!isRestarted) {
                    flushNext(job);
                    assertTiedLeadRowKept(job, TIED_LEAD_ROW_KEPT, TIED_LEAD_ROWS_AFTER_RESTART);
                }
            }
            if (isRestarted) {
                restartCycle();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveRefreshToQuiescence(job);
                    assertTiedLeadRowKept(job, TIED_LEAD_ROW_KEPT, TIED_LEAD_ROWS_AFTER_RESTART);
                }
            }
            Assert.assertEquals("closed segments repaired over their own range", 0, segmentRepairs);
            Assert.assertFalse(
                    "the repair at the tie must run no resume replay for the armed fault to stop",
                    hasResumeReplayStarted.get()
            );
        });
    }

    private void assertViewMatchesRecompute() throws Exception {
        assertViewMatchesRecompute(null);
    }

    private void assertViewMatchesRecompute(String startFrom) throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + recompute(startFrom) + ") order by 2, 1",
                "(lv) order by 2, 1",
                LOG,
                true
        );
        assertNoRefreshFaults("lv");
    }

    private void assertViewReturns(String expected) throws Exception {
        assertQuery("SELECT * FROM lv")
                .noLeakCheck()
                .timestamp("created_at")
                .expectSize()
                .returns(expected);
        assertViewMatchesRecompute();
    }

    private void commit(String values, LiveViewRefreshJob job) throws Exception {
        execute("insert into tx values " + values);
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    /**
     * Two base commits with no refresh between them: the base applies both, then the drain
     * breaks on the first and never reads the second at all. The second is the apply-ahead
     * range - what {@code ApplyWal2TableJob} raced past the O3 trigger - and the repair
     * re-materialises and consumes it whether or not the decomposition placed its rows.
     */
    private void commitWithApplyAhead(String triggerValues, String aheadValues, LiveViewRefreshJob job) throws Exception {
        execute("insert into tx values " + triggerValues);
        execute("insert into tx values " + aheadValues);
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    /**
     * The view over an indexed key that the tied-lead cases repair, waiting an hour between
     * flushes, over the three seeded days.
     *
     * @param filter the view's WHERE clause, with a leading space, or empty
     */
    private void createTiedLeadView(String filter) throws Exception {
        execute("CREATE TABLE tx (created_at TIMESTAMP, account_id " + INDEXED_KEY + ", "
                + "amount DOUBLE) TIMESTAMP(created_at) PARTITION BY HOUR WAL");
        execute("INSERT INTO tx VALUES " + seedThreeDays());
        drainWalQueue();
        execute("CREATE LIVE VIEW lv FLUSH EVERY 1h START FROM BEGINNING AS "
                + "SELECT created_at, account_id, "
                + "sum(amount) OVER w AS cumulative_sum, "
                + "count(account_id) OVER w AS cumulative_count "
                + "FROM tx" + filter + " "
                + "WINDOW w AS (PARTITION BY account_id ORDER BY created_at ANCHOR DAILY '00:00')");
    }

    private void createView(String seedRows) throws Exception {
        execute("create table tx (created_at timestamp, account_id symbol nocache index capacity 4, "
                + "amount double) timestamp(created_at) partition by hour wal");
        execute("insert into tx values " + seedRows);
        drainWalQueue();
        execute("create live view lv flush every 100ms start from beginning as "
                + "select created_at, account_id, "
                + "sum(amount) over w as cumulative_sum, "
                + "count(account_id) over w as cumulative_count "
                + "from tx window w as (partition by account_id order by created_at anchor daily '00:00')");
    }

    /**
     * Moves the clock a whole {@code FLUSH EVERY 1h} interval on, so the next pass flushes
     * whatever the lead holds, and drives the view to quiescence.
     */
    private void flushNext(LiveViewRefreshJob job) {
        drainWalQueue();
        setCurrentMicros(currentMicros + FLUSH_EVERY_MICROS);
        driveRefreshToQuiescence(job);
    }

    /**
     * Flushes acct-1's row at 2026-01-04T03 and leaves acct-2's row at the same timestamp in
     * the un-flushed lead. Every pass but the flush moves the clock by far less than the hour
     * the view waits between flushes.
     *
     * @return the newest root's maxTimestamp while the tied row sits in the lead, which tells a
     * root the flush sealed at the tie from one the cadence left below it
     */
    private long holdATiedRowInTheLead(LiveViewRefreshJob job) throws Exception {
        execute("INSERT INTO tx VALUES " + row(4, 3, "acct-1"));
        flushNext(job);
        execute("INSERT INTO tx VALUES " + row(4, 3, "acct-2"));
        drainWalQueue();
        driveRefreshToQuiescence(job);
        Assert.assertEquals("the tied row must sit in the un-flushed lead", 1, viewInstance().getLeadRowCount());
        return viewInstance().getHeadCheckpointMaxTs();
    }

    /**
     * {@link #holdATiedRowInTheLead}, then commits {@code lateRows} and drives the repair they
     * trigger.
     *
     * @return the newest root's maxTimestamp while the tied row sat in the lead, ahead of the
     * repair
     */
    private long leaveATiedRowInTheLead(LiveViewRefreshJob job, String lateRows) throws Exception {
        final long newestRootMaxTs = holdATiedRowInTheLead(job);
        execute("INSERT INTO tx VALUES " + lateRows);
        drainWalQueue();
        driveRefreshToQuiescence(job);
        return newestRootMaxTs;
    }

    /**
     * The from-base oracle: the same accumulators partitioned by account and anchor day,
     * over the rows the view's own {@code START FROM} boundary admits.
     */
    private String recompute(String startFrom) {
        final String bucket = "timestamp_floor('1d', created_at, '1970-01-01T00:00:00.000000Z'::timestamp)";
        final String source = startFrom == null
                ? "tx"
                : "(select * from tx where created_at >= '" + startFrom + "'::timestamp)";
        return "select created_at, account_id, "
                + "sum(amount) over (partition by account_id, bucket order by created_at "
                + "rows between unbounded preceding and current row) as cumulative_sum, "
                + "count(account_id) over (partition by account_id, bucket order by created_at "
                + "rows between unbounded preceding and current row) as cumulative_count "
                + "from (select created_at, account_id, amount, " + bucket + " as bucket from " + source + ")";
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
     * Four accounts on each of 2026-01-02, 2026-01-03 and 2026-01-04. The wider key domain is
     * what separates an incremental freeze from a complete one: a batch touching one account
     * images one key where a complete freeze images four.
     */
    private String seedFourAccountsOverThreeDays() {
        final StringBuilder rows = new StringBuilder();
        for (int day = 2; day <= 4; day++) {
            for (int account = 1; account <= 4; account++) {
                if (rows.length() > 0) {
                    rows.append(", ");
                }
                rows.append(row(day, account, "acct-" + account));
            }
        }
        return rows.toString();
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

    /**
     * Flattens every logical timeline entry into {@code (maxTimestamp, checkpointId, root
     * segment/offset/length, effective position)}. Root page identity is what separates a
     * reused payload root from a re-versioned one, and the effective position is the
     * cumulative live-view row count a restart selecting that root would credit the view
     * with.
     */
    private LongList snapshotTimeline() {
        final LiveViewInstance instance = viewInstance();
        final LongList rows = new LongList();
        try (
                Path checkpointsDir = new Path().of(configuration.getDbRoot())
                        .concat(instance.getLiveViewToken())
                        .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
                LiveViewCheckpointMetaStore store = new LiveViewCheckpointMetaStore(configuration);
                LiveViewCheckpointTimelineReader reader = new LiveViewCheckpointTimelineReader(configuration);
                LiveViewCheckpointRowPositionDeltaReader deltaReader =
                        new LiveViewCheckpointRowPositionDeltaReader(configuration)
        ) {
            store.of(checkpointsDir);
            reader.of(checkpointsDir);
            deltaReader.of(checkpointsDir);
            try (LiveViewCheckpointGenerationPin pin = store.pin()) {
                reader.iterateAll(pin.getTimelineRootRef(), entry -> {
                    rows.add(entry.maxTimestamp);
                    rows.add(entry.checkpointId);
                    rows.add(entry.rootRef.getSegmentId());
                    rows.add(entry.rootRef.getOffset());
                    rows.add(entry.rootRef.getLength());
                    rows.add(deltaReader.effectivePosition(pin.getRowPositionDeltaRootRef(), entry));
                });
            }
        }
        return rows;
    }

    private LiveViewInstance viewInstance() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull("live view 'lv' must be registered", instance);
        return instance;
    }
}
