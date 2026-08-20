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
import io.questdb.cairo.lv.LiveViewCheckpointAnchorPlan;
import io.questdb.cairo.lv.LiveViewCheckpointSegmentChangeSet;
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

    private static LiveViewCheckpointAnchorPlan dailyPlan() {
        final LiveViewCheckpointAnchorPlan plan =
                LiveViewCheckpointAnchorPlan.of('d', 1, 0, ColumnType.TIMESTAMP_MICRO);
        Assert.assertNotNull("an epoch-aligned daily anchor must carry a fixed segment", plan);
        return plan;
    }
}
