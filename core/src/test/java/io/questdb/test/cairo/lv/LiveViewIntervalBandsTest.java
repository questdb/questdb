/*+*****************************************************************************
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
import io.questdb.cairo.lv.LiveViewInMemoryBuffer;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.lv.LiveViewIntervalBands;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit coverage for {@link LiveViewIntervalBands}, which cuts a slot's row band down to the
 * rows an interval filter admits. Both live-view read paths take their answer from here, so
 * this is where the boundary semantics are pinned: the intervals are CLOSED at both ends and
 * the row bands are half-open, and every off-by-one lives at that meeting point.
 * <p>
 * Driven against a real {@link LiveViewInMemoryBuffer} rather than a query, because a query
 * cannot place a timestamp exactly on an interval edge, between two rows, or on a run of
 * ties - which is the whole of what can go wrong here. The buffer allocates native memory,
 * so every test runs under {@code assertMemoryLeak}.
 */
public class LiveViewIntervalBandsTest extends AbstractCairoTest {

    private static final int COL_TS = 0;
    private static final int COL_X = 1;
    private static final long PAGE_SIZE = 4096L;

    @Test
    public void testClosedIntervalEndsIncludeTheirOwnRows() throws Exception {
        // The single most likely off-by-one: an interval is closed, so a row sitting exactly
        // on either bound is IN. A half-open reading of either end drops a row, and both
        // ends are asserted here because they come from different searches.
        assertMemoryLeak(() -> {
            try (LiveViewInMemoryBuffer slot = ladder(0, 5)) { // ts 100, 200, 300, 400, 500
                // Both bounds land exactly on a row.
                assertBands(slot, 0, 5, intervals(200, 400), 1, 4);
                // Nudging either bound one microsecond inward drops that row and only it.
                assertBands(slot, 0, 5, intervals(201, 400), 2, 4);
                assertBands(slot, 0, 5, intervals(200, 399), 1, 3);
            }
        });
    }

    @Test
    public void testEmptyIntervalsAdmitNothingAndNullAdmitsEverything() throws Exception {
        // The two answers a caller must not conflate. Null means "no filter", which is what
        // every non-interval read passes and why both paths can walk bands unconditionally.
        // Empty means "a filter that admits nothing" - an interval list the optimiser
        // resolved to nothing at all. Reading empty as null would serve the whole lead to a
        // query that asked for none of it.
        assertMemoryLeak(() -> {
            try (LiveViewInMemoryBuffer slot = ladder(0, 5)) {
                assertBands(slot, 0, 5, null, 0, 5);
                assertBands(slot, 1, 4, null, 1, 4);
                assertBands(slot, 0, 5, new LongList());
            }
        });
    }

    @Test
    public void testIntervalBetweenTwoRowsSelectsNoBand() throws Exception {
        // An interval that falls in a gap in the ladder matches nothing, and must produce NO
        // band rather than an empty [r, r) one - a walk stepping over empty bands would be
        // right by accident, and a frame of zero rows is not a frame.
        assertMemoryLeak(() -> {
            try (LiveViewInMemoryBuffer slot = ladder(0, 5)) { // ts 100, 200, 300, 400, 500
                assertBands(slot, 0, 5, intervals(201, 299));
                // The same interval alongside two that DO match: the empty one drops out and
                // the others still land, so the gap is skipped rather than terminating.
                assertBands(slot, 0, 5, intervals(100, 100, 201, 299, 500, 500), 0, 1, 4, 5);
            }
        });
    }

    @Test
    public void testIntervalsBelowAndAboveTheBandSelectNothing() throws Exception {
        // An interval entirely outside the band's timestamp span selects nothing. The ABOVE
        // case is the one the live view actually hits: the slot's lead sits above the LV
        // table's on-disk maximum, so a read bounded below that maximum leaves the lead band
        // empty and must still serve the disk scan whole.
        assertMemoryLeak(() -> {
            try (LiveViewInMemoryBuffer slot = ladder(0, 5)) { // ts 100 .. 500
                assertBands(slot, 0, 5, intervals(1, 99));
                assertBands(slot, 0, 5, intervals(501, 900));
                // Straddling the whole ladder takes all of it.
                assertBands(slot, 0, 5, intervals(1, 900), 0, 5);
            }
        });
    }

    @Test
    public void testMultipleIntervalsProduceOneBandEach() throws Exception {
        // Several disjoint intervals over one band: each contributes its own sub-band, and
        // the results stay ascending and disjoint. This is what a page frame path tiles and
        // what a record path walks, so a merged or reordered answer would serve rows twice
        // or out of order.
        assertMemoryLeak(() -> {
            try (LiveViewInMemoryBuffer slot = ladder(0, 10)) { // ts 100 .. 1000
                assertBands(slot, 0, 10, intervals(100, 200, 500, 600, 900, 1000), 0, 2, 4, 6, 8, 10);
                // Adjacent intervals stay SEPARATE bands even when their rows abut: the
                // caller may not assume a band boundary means a timestamp gap.
                assertBands(slot, 0, 10, intervals(100, 200, 300, 400), 0, 2, 2, 4);
            }
        });
    }

    @Test
    public void testOpenLowerBoundTakesTheBandFromItsFloor() throws Exception {
        // WHERE ts < x resolves to an interval whose low bound is Long.MIN_VALUE. Probing
        // that bound minus one would wrap to Long.MAX_VALUE and select NOTHING - the band
        // would vanish rather than survive whole, which is the opposite of what the query
        // asked for. Both a band starting at 0 and one starting mid-slot are asserted, since
        // the guard must answer the BAND's floor and not a hard 0.
        assertMemoryLeak(() -> {
            try (LiveViewInMemoryBuffer slot = ladder(0, 5)) { // ts 100 .. 500
                assertBands(slot, 0, 5, intervals(Long.MIN_VALUE, 300), 0, 3);
                assertBands(slot, 2, 5, intervals(Long.MIN_VALUE, 400), 2, 4);
                assertBands(slot, 0, 5, intervals(Long.MIN_VALUE, Long.MAX_VALUE), 0, 5);
            }
        });
    }

    @Test
    public void testSubBandIsClampedToTheBandNotTheSlot() throws Exception {
        // The band is a sub-range of the slot: lead-only serves [leadStart, rowCount), not
        // the whole slot. An interval matching rows on BOTH sides of leadStart must yield
        // only the rows inside the band - a cut that searched the whole slot would hand back
        // overlap rows, which disk already served, and the read would print them twice.
        assertMemoryLeak(() -> {
            try (LiveViewInMemoryBuffer slot = ladder(0, 6)) { // ts 100 .. 600
                // Band [3, 6) = ts 400, 500, 600. The interval reaches down to ts 100.
                assertBands(slot, 3, 6, intervals(100, 500), 3, 5);
                // And a band that is empty to begin with yields nothing, filter or not.
                assertBands(slot, 4, 4, intervals(100, 600));
                assertBands(slot, 4, 4, null);
            }
        });
    }

    @Test
    public void testTiedTimestampsAreTakenWholeOrNotAtAll() throws Exception {
        // Ties are legitimate on this ladder - an additive commit whose minimum timestamp
        // equals the frontier appends at exactly the on-disk maximum - so a run of equal
        // timestamps must go in or out together. A binary search that stops at the FIRST
        // match rather than the last would take a run's first row and drop the rest.
        assertMemoryLeak(() -> {
            try (LiveViewInMemoryBuffer slot = tsOf(100, 200, 200, 200, 300)) {
                assertBands(slot, 0, 5, intervals(200, 200), 1, 4);
                // The run's timestamp as an upper bound takes the whole run...
                assertBands(slot, 0, 5, intervals(100, 200), 0, 4);
                // ...and as a lower bound likewise.
                assertBands(slot, 0, 5, intervals(200, 300), 1, 5);
            }
        });
    }

    // Asserts cut() produces exactly expectedBands, given as flat (lo, hi) row pairs.
    private static void assertBands(
            LiveViewInMemoryBuffer slot,
            long bandLo,
            long bandHi,
            LongList intervals,
            long... expectedBands
    ) {
        final LongList bands = new LongList();
        LiveViewIntervalBands.cut(slot, COL_TS, bandLo, bandHi, intervals, bands);
        final StringBuilder actual = new StringBuilder();
        for (int i = 0, n = bands.size(); i < n; i += 2) {
            actual.append('[').append(bands.getQuick(i)).append(',').append(bands.getQuick(i + 1)).append(')');
        }
        final StringBuilder expected = new StringBuilder();
        long expectedRows = 0;
        for (int i = 0; i < expectedBands.length; i += 2) {
            expected.append('[').append(expectedBands[i]).append(',').append(expectedBands[i + 1]).append(')');
            expectedRows += expectedBands[i + 1] - expectedBands[i];
        }
        Assert.assertEquals("bands for band [" + bandLo + "," + bandHi + ")", expected.toString(), actual.toString());
        // countRows feeds size() on both read paths, so pin it against the same answer
        // rather than leaving it to agree by inspection.
        Assert.assertEquals("countRows", expectedRows, LiveViewIntervalBands.countRows(bands));
    }

    private static LongList intervals(long... bounds) {
        final LongList intervals = new LongList();
        for (long bound : bounds) {
            intervals.add(bound);
        }
        return intervals;
    }

    // A slot whose timestamps ascend 100, 200, ... - one per row, no ties.
    private static LiveViewInMemoryBuffer ladder(int firstRow, int rowCount) {
        final long[] timestamps = new long[rowCount];
        for (int r = 0; r < rowCount; r++) {
            timestamps[r] = (firstRow + r + 1) * 100L;
        }
        return tsOf(timestamps);
    }

    // A slot carrying exactly these timestamps, so a test can place ties and gaps itself.
    private static LiveViewInMemoryBuffer tsOf(long... timestamps) {
        final IntList types = new IntList();
        types.add(ColumnType.TIMESTAMP);
        types.add(ColumnType.LONG);
        final LiveViewInMemoryBuffer slot = new LiveViewInMemoryBuffer(types, COL_TS, PAGE_SIZE);
        try {
            final TsRow row = new TsRow();
            for (int r = 0; r < timestamps.length; r++) {
                row.of(timestamps[r], r);
                slot.copyRowFromRecord(row, r);
            }
            slot.setRowCount(timestamps.length);
        } catch (Throwable t) {
            slot.close();
            throw t;
        }
        return slot;
    }

    private static class TsRow implements Record {
        private long ts;
        private long x;

        @Override
        public long getLong(int col) {
            return col == COL_X ? x : ts;
        }

        @Override
        public long getTimestamp(int col) {
            return ts;
        }

        void of(long ts, long x) {
            this.ts = ts;
            this.x = x;
        }
    }
}
