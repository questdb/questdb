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

    @Test
    public void testABoundedFrameBesideTheAnchorKeepsTheUnionRange() throws Exception {
        // A bounded ROWS frame declared beside the anchored window keeps sliding across the
        // segment boundary, so a row in a closed segment still changes a later segment's
        // output and the segments are not independent. The decomposition must decline - and
        // the repair must still be correct, on the route it always took.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
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
    public void testRowsBelowTheViewFloorAreDiscardedRatherThanDenyingTheRepair() throws Exception {
        // The denial the cost model attributes 75.5% of all replay to: a correction reaching
        // below the view's own START FROM boundary clamps the correction floor onto that
        // boundary, and a floor landing there is what DENIAL_VIEW_START_FLOOR refuses. Those
        // rows produce no output at all, so they belong out of the change set rather than in
        // charge of it.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            execute("create table tx (created_at timestamp, cod_acct_no symbol nocache index capacity 4, "
                    + "amt_txn double) timestamp(created_at) partition by hour wal");
            // A day below the view's floor as well as the three the view holds, so the
            // correction below has real sub-floor history to reach into.
            execute("insert into tx values " + row(1, 1, "acct-1") + ", " + row(1, 2, "acct-2")
                    + ", " + seedThreeDays());
            drainWalQueue();
            execute("create live view lv flush every 100ms start from '2026-01-02T00:00:00.000000Z' as "
                    + "select created_at, cod_acct_no, "
                    + "sum(amt_txn) over w as cumulative_sum, "
                    + "count(cod_acct_no) over w as cumulative_count "
                    + "from tx window w as (partition by cod_acct_no order by created_at anchor daily '00:00')");
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
     * The from-base oracle: the same accumulators partitioned by account and anchor day,
     * over the rows the view's own {@code START FROM} boundary admits.
     */
    private String recompute(String startFrom) {
        final String bucket = "timestamp_floor('1d', created_at, '1970-01-01T00:00:00.000000Z'::timestamp)";
        final String source = startFrom == null
                ? "tx"
                : "(select * from tx where created_at >= '" + startFrom + "'::timestamp)";
        return "select created_at, cod_acct_no, "
                + "sum(amt_txn) over (partition by cod_acct_no, bucket order by created_at "
                + "rows between unbounded preceding and current row) as cumulative_sum, "
                + "count(cod_acct_no) over (partition by cod_acct_no, bucket order by created_at "
                + "rows between unbounded preceding and current row) as cumulative_count "
                + "from (select created_at, cod_acct_no, amt_txn, " + bucket + " as bucket from " + source + ")";
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
