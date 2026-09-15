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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.lv.LiveViewCheckpointAnchorPlan;
import io.questdb.cairo.lv.LiveViewCheckpointRepairPlan;
import io.questdb.cairo.lv.LiveViewCheckpointSegmentChangeSet;
import io.questdb.griffin.SqlException;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.microtime.Micros;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit coverage for the decomposition itself: which segments a run of rows opens, in which
 * order they come back, and what makes it give up.
 * <p>
 * The ordering is the part with a consequence beyond bookkeeping. Segments are repaired
 * oldest first because a later segment's cumulative row positions depend on how many rows
 * the earlier ones added, so a decomposition that returned them in arrival order would
 * publish a ladder whose positions are wrong in a way only a restart discovers.
 */
public class LiveViewCheckpointSegmentChangeSetTest {

    private static final long DAY = Micros.DAY_MICROS;
    // 2026-01-08T00:00:00Z, an epoch-aligned day start, so every "day N" below is a segment
    // boundary of the daily plan and the arithmetic in the cases stays readable.
    private static final long DAY_8 = 20_460L * DAY;

    @Test
    public void testAnUnalignedActiveSegmentStartDeclinesRatherThanOpeningASegmentAcrossIt() {
        // Defensive: the caller derives the active segment's start from the same plan, so it
        // is always aligned. If it ever were not, a row below it could still sit in a segment
        // whose end runs past it - a segment that is half closed and half live, which nothing
        // downstream can repair on its own.
        final LiveViewCheckpointAnchorPlan plan = dailyPlan();
        final LiveViewCheckpointSegmentChangeSet changeSet = new LiveViewCheckpointSegmentChangeSet();
        changeSet.of(DAY_8 + Micros.HOUR_MICROS * 6);
        Assert.assertFalse(changeSet.addRow(DAY_8 + Micros.HOUR_MICROS, null, plan));
        Assert.assertTrue(changeSet.isOverflowed());
    }

    @Test
    public void testASegmentOpenBelowDeclinesRatherThanFlooringOneAtMinValue() {
        // Every row under a non-zero alignment origin shares one segment that is open below
        // and ends where the first aligned bucket starts, so the plan answers Long.MIN_VALUE
        // for its start and a finite end for the same probe. Long.MIN_VALUE is a refusal, not
        // a floor: installed as one it would swallow every row below that end, however far
        // back, and nest the segments those rows belong to inside it.
        final long origin = 9 * Micros.HOUR_MICROS + 30 * Micros.MINUTE_MICROS;
        final LiveViewCheckpointAnchorPlan plan =
                LiveViewCheckpointAnchorPlan.of('d', 1, origin, ColumnType.TIMESTAMP_MICRO);
        Assert.assertNotNull("a daily anchor aligned to 09:30 must carry a fixed segment", plan);
        final long belowOrigin = 2 * Micros.HOUR_MICROS;
        Assert.assertEquals(Long.MIN_VALUE, plan.getSegmentStart(belowOrigin));
        Assert.assertEquals(origin, plan.getSegmentEndExclusive(belowOrigin));

        final LiveViewCheckpointSegmentChangeSet changeSet = new LiveViewCheckpointSegmentChangeSet();
        // Well above the segment's end, so nothing but the open-below start can decline it.
        changeSet.of(DAY_8 + origin);
        Assert.assertFalse(changeSet.addRow(belowOrigin, null, plan));
        Assert.assertTrue(changeSet.isOverflowed());
        Assert.assertEquals(0, changeSet.getClosedSegmentCount());
    }

    @Test
    public void testAZonedOpenBelowStartDeclinesRatherThanFlooringASegmentAtMinValue() {
        // The production shape of the same refusal. ANCHOR DAILY '02:30' 'Europe/Berlin'
        // straddles the hour the spring-forward skips, so on 2024-03-31 the plan refuses the
        // start and still reports a finite end - one probe, one bound each way, on two days a
        // year. The end sits well below the active segment's start, so the change set has
        // nothing to decline on but the start.
        final LiveViewCheckpointAnchorPlan plan = berlinPlan();
        final long inGapDay = ts("2024-03-31T10:00:00.000000Z");
        Assert.assertEquals(Long.MIN_VALUE, plan.getSegmentStart(inGapDay));
        Assert.assertEquals(ts("2024-04-01T00:30:00.000000Z"), plan.getSegmentEndExclusive(inGapDay));

        final LiveViewCheckpointSegmentChangeSet changeSet = new LiveViewCheckpointSegmentChangeSet();
        changeSet.of(ts("2024-06-01T00:30:00.000000Z"));
        Assert.assertFalse(changeSet.addRow(inGapDay, null, plan));
        Assert.assertTrue(changeSet.isOverflowed());
        // And so a row three months lower never joins a segment floored at Long.MIN_VALUE,
        // which is what the containment cache would have handed it.
        Assert.assertFalse(changeSet.addRow(ts("2024-01-05T08:00:00.000000Z"), null, plan));
        Assert.assertEquals(0, changeSet.getClosedSegmentCount());
    }

    @Test
    public void testAFallBackSegmentsRepairRangeCoversEveryRowTheEntryHolds() throws SqlException {
        // A zone floor is not monotone through a fall-back, and on 2024-10-27 Europe/Berlin
        // gives the 02:30 anchor of 26 October TWO intervals: everything up to 00:30Z, and
        // then 01:00Z..01:30Z again, once the clocks have gone back and local time reads
        // 02:00..02:29 CET. A probe in the lower interval and a probe in the upper one share
        // a start and disagree about the end - 24 hours against 25 - and the entry indexOf
        // keys on the start alone would keep whichever end arrived first while its maxTs
        // widened past it.
        //
        // The plan closes that from its own side: it reports no finite end for the lower
        // interval, because the end it would name has the upper interval standing above it.
        // The upper interval keeps a finite end, and the interval it names spans both - wider
        // than one run, but a wall at each end.
        final LiveViewCheckpointAnchorPlan plan = berlinPlan();
        final long segmentStart = ts("2024-10-26T00:30:00.000000Z");
        final long lowRow = ts("2024-10-26T23:00:00.000000Z");
        final long highRow = ts("2024-10-27T01:00:00.000000Z");
        Assert.assertEquals(segmentStart, plan.getSegmentStart(lowRow));
        Assert.assertEquals(segmentStart, plan.getSegmentStart(highRow));
        Assert.assertEquals(Numbers.LONG_NULL, plan.getSegmentEndExclusive(lowRow));
        Assert.assertEquals(ts("2024-10-27T01:30:00.000000Z"), plan.getSegmentEndExclusive(highRow));

        // The frontier sits a week above the transition, so both rows are below the runtime's
        // own segment and the decomposition may place them.
        final long frontierTs = ts("2024-11-05T12:00:00.000000Z");
        final long activeSegmentStart = plan.getSegmentStart(frontierTs);

        // Reached from below, the refused end declines the row outright and the caller falls
        // back to the union range rather than repairing a segment that stops mid-value.
        final LiveViewCheckpointSegmentChangeSet fromBelow = new LiveViewCheckpointSegmentChangeSet();
        fromBelow.of(activeSegmentStart);
        Assert.assertFalse(fromBelow.addRow(lowRow, null, plan));
        Assert.assertTrue(fromBelow.isOverflowed());
        Assert.assertEquals(0, fromBelow.getClosedSegmentCount());

        // Reached from above, the surviving end opens an entry that already spans both rows,
        // so the second one joins it through the containment cache without a plan call and
        // the entry's stored end covers everything it holds.
        final LiveViewCheckpointSegmentChangeSet changeSet = new LiveViewCheckpointSegmentChangeSet();
        changeSet.of(activeSegmentStart);
        Assert.assertTrue(changeSet.addRow(highRow, null, plan));
        Assert.assertTrue(changeSet.addRow(lowRow, null, plan));
        Assert.assertEquals(1, changeSet.getClosedSegmentCount());
        Assert.assertEquals(segmentStart, changeSet.getSegmentStart(0));
        Assert.assertEquals(lowRow, changeSet.getSegmentMinTs(0));
        Assert.assertEquals(highRow, changeSet.getSegmentMaxTs(0));

        final LiveViewCheckpointRepairPlan repairPlan = new LiveViewCheckpointRepairPlan();
        Assert.assertTrue(
                "a closed segment below a quiesced frontier must localize",
                repairPlan.ofSegment(
                        changeSet.getSegmentMinTs(0),
                        changeSet.getSegmentMaxTs(0),
                        ts("2024-10-20T00:30:00.000000Z"),
                        9,
                        9,
                        plan,
                        frontierTs,
                        frontierTs
                )
        );
        // The property to hold on to, whatever the entry stores: a repair may read wider than
        // the entry claims, and may never read narrower. The replay reads from the segment's
        // own start, and the replacement runs past the highest row the entry holds.
        Assert.assertEquals(segmentStart, repairPlan.getReplayLowTs());
        Assert.assertEquals(ts("2024-10-27T01:30:00.000000Z"), repairPlan.getHighTsExclusive());
        Assert.assertTrue(
                "the replacement must not stop below the entry's stored end",
                repairPlan.getHighTsExclusive() >= changeSet.getSegmentEndExclusive(0)
        );
        Assert.assertTrue(
                "every row the entry holds must sit inside the replaced range",
                repairPlan.getOutputLowTs() <= changeSet.getSegmentMinTs(0)
        );
    }

    @Test
    public void testRowsAboveTheActiveSegmentStartJoinTheResidual() {
        final LiveViewCheckpointAnchorPlan plan = dailyPlan();
        final LiveViewCheckpointSegmentChangeSet changeSet = new LiveViewCheckpointSegmentChangeSet();
        changeSet.of(DAY_8);
        Assert.assertEquals(Numbers.LONG_NULL, changeSet.getResidualMinTs());
        Assert.assertEquals(Numbers.LONG_NULL, changeSet.getResidualMaxTs());

        Assert.assertTrue(changeSet.addRow(DAY_8 + 7, null, plan));
        Assert.assertTrue(changeSet.addRow(DAY_8, null, plan));
        Assert.assertTrue(changeSet.addRow(DAY_8 + 3, null, plan));

        Assert.assertEquals(0, changeSet.getClosedSegmentCount());
        Assert.assertEquals(DAY_8, changeSet.getResidualMinTs());
        Assert.assertEquals(DAY_8 + 7, changeSet.getResidualMaxTs());
    }

    @Test
    public void testTheOpenSegmentCountsNewRowsAtEachBoundary() {
        final LiveViewCheckpointAnchorPlan plan = dailyPlan();
        final LiveViewCheckpointSegmentChangeSet changeSet = new LiveViewCheckpointSegmentChangeSet();
        changeSet.of(DAY_8, 16, true);

        // Deliberately unordered, with two rows at one timestamp: the WAL walk does not
        // promise timestamp order and a checkpoint boundary includes the whole equal-ts group.
        Assert.assertTrue(changeSet.addRow(DAY_8 + 3, "acct-1", plan));
        Assert.assertTrue(changeSet.addRow(DAY_8 + 1, "acct-2", plan));
        Assert.assertTrue(changeSet.addRow(DAY_8 + 3, "acct-3", plan));
        Assert.assertTrue(changeSet.addRow(DAY_8 + 7, "acct-4", plan));

        Assert.assertEquals(4, changeSet.getResidualRowCount());
        Assert.assertEquals(0, changeSet.getResidualRowCountAtOrBelow(DAY_8));
        Assert.assertEquals(1, changeSet.getResidualRowCountAtOrBelow(DAY_8 + 2));
        Assert.assertEquals(3, changeSet.getResidualRowCountAtOrBelow(DAY_8 + 3));
        Assert.assertEquals(4, changeSet.getResidualRowCountAtOrBelow(DAY_8 + 99));

        changeSet.of(DAY_8, 16, true);
        Assert.assertEquals(0, changeSet.getResidualRowCount());
        Assert.assertEquals(0, changeSet.getResidualRowCountAtOrBelow(DAY_8 + 99));
    }

    @Test
    public void testTheOpenSegmentCollectsItsKeysWhenTheCallerAsksForThem() {
        final LiveViewCheckpointAnchorPlan plan = dailyPlan();
        final LiveViewCheckpointSegmentChangeSet changeSet = new LiveViewCheckpointSegmentChangeSet();
        changeSet.of(DAY_8, 16, true);

        Assert.assertTrue(changeSet.addRow(DAY_8 + 1, "acct-1", plan));
        Assert.assertTrue(changeSet.addRow(DAY_8 + 2, "acct-2", plan));
        // The same key twice is one key, and a row of a CLOSED segment is not the open
        // segment's business however its key reads.
        Assert.assertTrue(changeSet.addRow(DAY_8 + 3, "acct-1", plan));
        Assert.assertTrue(changeSet.addRow(DAY_8 - DAY + 5, "acct-9", plan));

        Assert.assertTrue(changeSet.isResidualKeyDomainComplete());
        Assert.assertEquals(2, changeSet.getResidualKeys().size());
        Assert.assertTrue(changeSet.getResidualKeys().contains("acct-1"));
        Assert.assertTrue(changeSet.getResidualKeys().contains("acct-2"));
        Assert.assertFalse(changeSet.getResidualKeys().contains("acct-9"));
        Assert.assertFalse(changeSet.hasResidualNullKey());
    }

    @Test
    public void testTheOpenSegmentHoldsItsNullKeyBesideTheSetRatherThanInIt() {
        // A duplicate null in a keyed scan's key list puts two cursors over one key into the
        // heap and yields that key's rows twice, which is why the flag exists at all.
        final LiveViewCheckpointAnchorPlan plan = dailyPlan();
        final LiveViewCheckpointSegmentChangeSet changeSet = new LiveViewCheckpointSegmentChangeSet();
        changeSet.of(DAY_8, 16, true);

        Assert.assertTrue(changeSet.addRow(DAY_8 + 1, null, plan));
        Assert.assertTrue(changeSet.addRow(DAY_8 + 2, "acct-1", plan));

        Assert.assertTrue(changeSet.isResidualKeyDomainComplete());
        Assert.assertTrue(changeSet.hasResidualNullKey());
        Assert.assertEquals(1, changeSet.getResidualKeys().size());
        Assert.assertTrue(changeSet.getResidualKeys().contains("acct-1"));
    }

    @Test
    public void testTheOpenSegmentsDomainIsIncompleteWhenACommitWasFoldedWithoutItsRows() {
        // addResidual is the whole-commit shortcut: it folds a span without visiting a row,
        // so the keys that commit carried are keys this change set never saw. Reporting the
        // domain complete would hand a keyed resume a set short of the keys it must correct.
        final LiveViewCheckpointAnchorPlan plan = dailyPlan();
        final LiveViewCheckpointSegmentChangeSet changeSet = new LiveViewCheckpointSegmentChangeSet();
        changeSet.of(DAY_8, 16, true);

        Assert.assertTrue(changeSet.addRow(DAY_8 + 1, "acct-1", plan));
        Assert.assertTrue(changeSet.isResidualKeyDomainComplete());

        changeSet.addResidual(DAY_8 + 4, DAY_8 + 9);

        Assert.assertFalse(changeSet.isResidualKeyDomainComplete());
        Assert.assertEquals(DAY_8 + 1, changeSet.getResidualMinTs());
        Assert.assertEquals(DAY_8 + 9, changeSet.getResidualMaxTs());
    }

    @Test
    public void testTheOpenSegmentsDomainIsIncompleteOnceItReachesItsBudget() {
        final LiveViewCheckpointAnchorPlan plan = dailyPlan();
        final LiveViewCheckpointSegmentChangeSet changeSet = new LiveViewCheckpointSegmentChangeSet();
        changeSet.of(DAY_8, 2, true);

        Assert.assertTrue(changeSet.addRow(DAY_8 + 1, "acct-1", plan));
        Assert.assertTrue(changeSet.addRow(DAY_8 + 2, "acct-2", plan));
        Assert.assertTrue(changeSet.isResidualKeyDomainComplete());

        Assert.assertTrue(changeSet.addRow(DAY_8 + 3, "acct-3", plan));

        // The rows still land - the decomposition is not abandoned - but the domain no
        // longer describes every key, so the resume reads whole.
        Assert.assertFalse(changeSet.isResidualKeyDomainComplete());
        Assert.assertEquals(2, changeSet.getResidualKeys().size());
    }

    @Test
    public void testTheOpenSegmentCollectsNoKeysUnlessTheCallerAsksForThem() {
        // The collection costs the walk the whole-commit shortcut skips, so a caller that
        // does not need the domain must not be charged for it - and must not read the empty
        // set it gets as an empty domain.
        final LiveViewCheckpointAnchorPlan plan = dailyPlan();
        final LiveViewCheckpointSegmentChangeSet changeSet = new LiveViewCheckpointSegmentChangeSet();
        changeSet.of(DAY_8, 16);

        Assert.assertTrue(changeSet.addRow(DAY_8 + 1, "acct-1", plan));

        Assert.assertFalse(changeSet.isResidualKeyDomainComplete());
        Assert.assertEquals(0, changeSet.getResidualKeys().size());
    }

    @Test
    public void testRowsOfOneSegmentCollapseIntoOneEntry() {
        final LiveViewCheckpointAnchorPlan plan = dailyPlan();
        final LiveViewCheckpointSegmentChangeSet changeSet = new LiveViewCheckpointSegmentChangeSet();
        changeSet.of(DAY_8);
        // Deliberately not in timestamp order: the WAL segment of an out-of-order commit is
        // not sorted, and the decomposition walks it as it is written.
        Assert.assertTrue(changeSet.addRow(DAY_8 - DAY + 500, null, plan));
        Assert.assertTrue(changeSet.addRow(DAY_8 - DAY + 10, null, plan));
        Assert.assertTrue(changeSet.addRow(DAY_8 - DAY + 900, null, plan));

        Assert.assertEquals(1, changeSet.getClosedSegmentCount());
        Assert.assertEquals(DAY_8 - DAY, changeSet.getSegmentStart(0));
        Assert.assertEquals(DAY_8 - DAY + 10, changeSet.getSegmentMinTs(0));
        Assert.assertEquals(DAY_8 - DAY + 900, changeSet.getSegmentMaxTs(0));
        Assert.assertEquals(Numbers.LONG_NULL, changeSet.getResidualMinTs());
    }

    @Test
    public void testSegmentsComeBackOldestFirstWhateverOrderTheirRowsArriveIn() {
        final LiveViewCheckpointAnchorPlan plan = dailyPlan();
        final LiveViewCheckpointSegmentChangeSet changeSet = new LiveViewCheckpointSegmentChangeSet();
        changeSet.of(DAY_8);
        Assert.assertTrue(changeSet.addRow(DAY_8 - DAY + 1, null, plan));
        Assert.assertTrue(changeSet.addRow(DAY_8 - 5 * DAY + 1, null, plan));
        Assert.assertTrue(changeSet.addRow(DAY_8 + 4, null, plan));
        Assert.assertTrue(changeSet.addRow(DAY_8 - 3 * DAY + 1, null, plan));
        // The middle segment again, so the insert has to find the existing entry rather than
        // open a fourth one.
        Assert.assertTrue(changeSet.addRow(DAY_8 - 3 * DAY + 9, null, plan));

        Assert.assertEquals(3, changeSet.getClosedSegmentCount());
        Assert.assertEquals(DAY_8 - 5 * DAY, changeSet.getSegmentStart(0));
        Assert.assertEquals(DAY_8 - 3 * DAY, changeSet.getSegmentStart(1));
        Assert.assertEquals(DAY_8 - DAY, changeSet.getSegmentStart(2));
        Assert.assertEquals(DAY_8 - 3 * DAY + 1, changeSet.getSegmentMinTs(1));
        Assert.assertEquals(DAY_8 - 3 * DAY + 9, changeSet.getSegmentMaxTs(1));
        Assert.assertEquals(DAY_8 + 4, changeSet.getResidualMinTs());
    }

    @Test
    public void testTooManyDistinctSegmentsGiveUpRatherThanRepairingThemAll() {
        // Each segment costs its own replay, replacement commit and timeline splice, so a
        // change set reaching more of them than the cap is one the union range serves better.
        final LiveViewCheckpointAnchorPlan plan = dailyPlan();
        final LiveViewCheckpointSegmentChangeSet changeSet = new LiveViewCheckpointSegmentChangeSet();
        changeSet.of(DAY_8);
        for (int i = 1; i <= LiveViewCheckpointSegmentChangeSet.MAX_CLOSED_SEGMENTS; i++) {
            Assert.assertTrue("segment " + i + " must still fit", changeSet.addRow(DAY_8 - i * DAY + 1, null, plan));
        }
        Assert.assertEquals(LiveViewCheckpointSegmentChangeSet.MAX_CLOSED_SEGMENTS, changeSet.getClosedSegmentCount());
        Assert.assertFalse(changeSet.isOverflowed());

        // One more distinct segment gives up; a row of a segment already open still does not.
        final long extraDay = DAY_8 - (LiveViewCheckpointSegmentChangeSet.MAX_CLOSED_SEGMENTS + 1) * DAY;
        Assert.assertFalse(changeSet.addRow(extraDay + 1, null, plan));
        Assert.assertTrue(changeSet.isOverflowed());
        Assert.assertFalse("an overflowed change set stays overflowed", changeSet.addRow(DAY_8 - DAY + 2, null, plan));

        // of() rebinds the scratch for the next repair, overflow flag included.
        changeSet.of(DAY_8);
        Assert.assertFalse(changeSet.isOverflowed());
        Assert.assertEquals(0, changeSet.getClosedSegmentCount());
        Assert.assertTrue(changeSet.addRow(extraDay + 1, null, plan));
    }

    private static LiveViewCheckpointAnchorPlan berlinPlan() {
        final LiveViewCheckpointAnchorPlan plan = LiveViewCheckpointAnchorPlan.ofTimeZone(
                'd',
                1,
                ts("1970-01-01T02:30:00.000000Z"),
                ColumnType.TIMESTAMP_MICRO,
                "Europe/Berlin"
        );
        Assert.assertNotNull("ANCHOR DAILY '02:30' 'Europe/Berlin' must carry a segment", plan);
        return plan;
    }

    private static LiveViewCheckpointAnchorPlan dailyPlan() {
        final LiveViewCheckpointAnchorPlan plan =
                LiveViewCheckpointAnchorPlan.of('d', 1, 0, ColumnType.TIMESTAMP_MICRO);
        Assert.assertNotNull("an epoch-aligned daily anchor must carry a fixed segment", plan);
        return plan;
    }

    private static long ts(String timestamp) {
        return MicrosTimestampDriver.floor(timestamp);
    }
}
