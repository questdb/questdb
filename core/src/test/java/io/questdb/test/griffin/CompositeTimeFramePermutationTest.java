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

package io.questdb.test.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.FullFwdPartitionFrameCursor;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.table.CompositeTimeFrameRecordCursor;
import io.questdb.griffin.engine.table.FwdTableReaderPageFrameCursor;
import io.questdb.std.IntList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Task 1 of the composite window/horizon-join-slave TimeFrameCursor work: {@link
 * CompositeTimeFrameRecordCursor} builds, in one lazy forward pass, a per-DAY designated-timestamp-
 * sorted PERMUTATION over a composite table's sibling cells, reusing (by mirroring, not sharing)
 * {@code CompositeMergePartitionRecordCursor}'s (Task 6a) per-day cross-cell k-way merge heap. This
 * suite drives the new cursor directly (no SQL layer involvement -- the class doesn't implement
 * {@code TimeFrameCursor} yet, that's a follow-up task), building the underlying page frame cursor
 * by hand exactly like {@code io.questdb.test.griffin.engine.table.PageFrameCursorReleasePartitionTest}
 * does for other low-level page-frame-cursor tests.
 * <p>
 * The oracle throughout is a plain twin table ({@code partition by day}, {@code exch} an ordinary
 * column) holding byte-for-byte identical rows: every permutation-order read is checked row-for-row
 * (timestamp AND payload) against the twin's own rows for the same day, read back via a fresh {@code
 * order by ts} query. All 20 timestamps are globally unique, so this comparison is unambiguous.
 * <p>
 * Dataset (composite {@code c}, plain twin {@code p}), inserted in scrambled -- not ts-sorted --
 * order so the WAL write path O3-sorts every cell:
 * <ul>
 *   <li>day 0 (2024-03-01): 2 interleaved cells (A, B), 7 rows;</li>
 *   <li>day 1 (2024-03-02): a SINGLE cell (A only), 4 rows -- the identity-permutation case;</li>
 *   <li>day 2 (2024-03-03): 3 interleaved cells (A, B, C), 9 rows.</li>
 * </ul>
 * Page frames are forced tiny ({@code max=2}) so a multi-row cell spans several contiguous frames,
 * exercising the permutation build's cross-FRAME advance within one cell, not just its cross-cell
 * interleave.
 */
public class CompositeTimeFramePermutationTest extends AbstractCairoTest {
    private static final String[] DAY_HI = {
            "2024-03-02T00:00:00.000000Z",
            "2024-03-03T00:00:00.000000Z",
            "2024-03-04T00:00:00.000000Z"
    };
    private static final String[] DAY_LO = {
            "2024-03-01T00:00:00.000000Z",
            "2024-03-02T00:00:00.000000Z",
            "2024-03-03T00:00:00.000000Z"
    };
    // day 1 (2024-03-02) is populated with exch='A' only -- see createAndPopulateTwins().
    private static final int SINGLE_CELL_DAY_INDEX = 1;

    /**
     * RED-before-implementation / GREEN-after core differential: builds the permutation directly
     * over the composite table's own page-frame cursor and checks (a) day count, (b)+(c) per-day
     * permutation-order reads against the plain twin (strictly ts-ascending, row-for-row equal,
     * tsLo/tsHi/rowCount matching), the day ceiling's exact boundary, and (d) a single-cell day
     * degenerates to the identity permutation.
     */
    @Test
    public void testPerDayPermutationMatchesPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateTwins();

            final TableReader reader = engine.getReader("c");
            final IntList columnIndexes = new IntList();
            final IntList columnSizeShifts = new IntList();
            for (int i = 0, n = reader.getMetadata().getColumnCount(); i < n; i++) {
                columnIndexes.add(i);
                columnSizeShifts.add(ColumnType.pow2SizeOf(reader.getMetadata().getColumnType(i)));
            }
            final int tsIndex = reader.getMetadata().getTimestampIndex();
            final int vIndex = reader.getMetadata().getColumnIndex("v");

            // Reader ownership transfers to partitionFrameCursor -> pageFrameCursor; closing the
            // latter (below) closes the whole chain, mirroring PageFrameCursorReleasePartitionTest.
            final FullFwdPartitionFrameCursor partitionFrameCursor = new FullFwdPartitionFrameCursor();
            partitionFrameCursor.of(reader);

            // Tiny page frames: every multi-row cell splits into several contiguous frames, so the
            // permutation build is exercised across frame boundaries within a cell too, not just
            // across cells (mirrors CompositeOrderedScanTest's established idiom).
            sqlExecutionContext.changePageFrameSizes(1, 2);
            final FwdTableReaderPageFrameCursor pageFrameCursor = new FwdTableReaderPageFrameCursor(
                    columnIndexes,
                    columnSizeShifts,
                    null,
                    1
            );

            try (pageFrameCursor) {
                pageFrameCursor.of(sqlExecutionContext, partitionFrameCursor);

                try (CompositeTimeFrameRecordCursor cursor =
                             new CompositeTimeFrameRecordCursor(configuration, reader.getMetadata())) {
                    cursor.of(pageFrameCursor, sqlExecutionContext);

                    // (a) day count == the twin's independently-computed distinct-day count.
                    final long twinDayCount = countDistinctDays();
                    Assert.assertEquals(3, twinDayCount);
                    Assert.assertEquals(twinDayCount, cursor.getDayCount());

                    // (b) + (c): per-day permutation order vs twin (ts-ascending, row-for-row equal,
                    // tsLo/tsHi/rowCount), plus the day ceiling's exact boundary.
                    for (int day = 0; day < 3; day++) {
                        assertDayMatchesTwin(cursor, day, DAY_LO[day], DAY_HI[day], tsIndex, vIndex);
                        Assert.assertEquals(
                                "day " + day + ": ceiling must be the next day's midnight",
                                toMicros(DAY_HI[day]),
                                cursor.getDayCeiling(day)
                        );
                    }

                    // (d) day 1 (2024-03-02) is single-cell (exch='A' only) by construction ->
                    // identity permutation (no cross-cell reordering to undo).
                    assertIdentityPermutation(cursor, SINGLE_CELL_DAY_INDEX);
                }
            }
        });
    }

    private void assertDayMatchesTwin(
            CompositeTimeFrameRecordCursor cursor,
            int dayIndex,
            String dayLoInclusive,
            String dayHiExclusive,
            int tsIndex,
            int vIndex
    ) throws SqlException {
        final long rowCount = cursor.getDayRowCount(dayIndex);
        final Record record = cursor.getRecord();

        Assert.assertEquals(
                "day " + dayIndex + ": rowCount vs twin",
                countRowsInRange(dayLoInclusive, dayHiExclusive),
                rowCount
        );

        try (
                RecordCursorFactory factory = select(
                        "select ts, v from p where ts >= '" + dayLoInclusive + "' and ts < '" + dayHiExclusive + "' order by ts"
                );
                RecordCursor twinCursor = factory.getCursor(sqlExecutionContext)
        ) {
            final Record twinRecord = twinCursor.getRecord();
            long prevTs = Long.MIN_VALUE;
            long observedLo = Long.MIN_VALUE;
            long observedHi = Long.MIN_VALUE;
            for (long ord = 0; ord < rowCount; ord++) {
                final long packed = cursor.getPermutationEntry(dayIndex, ord);
                cursor.recordAt(
                        record,
                        CompositeTimeFrameRecordCursor.unpackFrameIndex(packed),
                        CompositeTimeFrameRecordCursor.unpackRowIndex(packed)
                );
                final long ts = record.getTimestamp(tsIndex);
                final double v = record.getDouble(vIndex);

                Assert.assertTrue(
                        "day " + dayIndex + " ordinal " + ord + ": ts must be strictly ascending",
                        ord == 0 || ts > prevTs
                );
                prevTs = ts;
                if (ord == 0) {
                    observedLo = ts;
                }
                observedHi = ts;

                Assert.assertTrue("day " + dayIndex + " ordinal " + ord + ": twin exhausted early", twinCursor.hasNext());
                Assert.assertEquals(
                        "day " + dayIndex + " ordinal " + ord + ": ts mismatch vs twin",
                        twinRecord.getTimestamp(0),
                        ts
                );
                Assert.assertEquals(
                        "day " + dayIndex + " ordinal " + ord + ": v mismatch vs twin",
                        twinRecord.getDouble(1),
                        v,
                        0.0
                );
            }
            Assert.assertFalse("day " + dayIndex + ": permutation shorter than twin", twinCursor.hasNext());
            Assert.assertEquals("day " + dayIndex + ": tsLo vs twin", observedLo, cursor.getDayTsLo(dayIndex));
            Assert.assertEquals("day " + dayIndex + ": tsHi vs twin", observedHi, cursor.getDayTsHi(dayIndex));
        }
    }

    // A single-cell day must be the IDENTITY permutation: no cross-cell interleave ever entered the
    // heap, so consecutive permutation entries must visit their (only) cell's frames/rows in pure
    // sequential physical order -- row index +1 within a frame, or reset to 0 on a frame change,
    // with frame indexes never decreasing.
    private void assertIdentityPermutation(CompositeTimeFrameRecordCursor cursor, int dayIndex) {
        final long rowCount = cursor.getDayRowCount(dayIndex);
        Assert.assertTrue("expected a non-trivial single-cell day", rowCount > 1);
        int prevFrame = -1;
        long prevRow = -1;
        for (long ord = 0; ord < rowCount; ord++) {
            final long packed = cursor.getPermutationEntry(dayIndex, ord);
            final int frame = CompositeTimeFrameRecordCursor.unpackFrameIndex(packed);
            final long row = CompositeTimeFrameRecordCursor.unpackRowIndex(packed);
            if (ord > 0) {
                if (frame == prevFrame) {
                    Assert.assertEquals(
                            "single-cell day must visit rows sequentially within a frame",
                            prevRow + 1,
                            row
                    );
                } else {
                    Assert.assertTrue(
                            "frame index must be non-decreasing (no reordering) in a single-cell day",
                            frame > prevFrame
                    );
                    Assert.assertEquals("a new frame in a single-cell day must start at row 0", 0, row);
                }
            }
            prevFrame = frame;
            prevRow = row;
        }
    }

    private long countDistinctDays() throws SqlException {
        try (
                RecordCursorFactory factory = select("select count() from (select ts, count() from p sample by 1d)");
                RecordCursor twinCursor = factory.getCursor(sqlExecutionContext)
        ) {
            Assert.assertTrue(twinCursor.hasNext());
            return twinCursor.getRecord().getLong(0);
        }
    }

    private long countRowsInRange(String dayLoInclusive, String dayHiExclusive) throws SqlException {
        try (
                RecordCursorFactory factory = select(
                        "select count() from p where ts >= '" + dayLoInclusive + "' and ts < '" + dayHiExclusive + "'"
                );
                RecordCursor twinCursor = factory.getCursor(sqlExecutionContext)
        ) {
            Assert.assertTrue(twinCursor.hasNext());
            return twinCursor.getRecord().getLong(0);
        }
    }

    /**
     * Builds composite {@code c} ({@code partition by day, exch}) and plain twin {@code p} ({@code
     * partition by day}), columns {@code (ts, exch symbol, v double)}. 20 rows across 3 days (day 0:
     * cells A/B interleaved; day 1: SINGLE cell A only; day 2: cells A/B/C interleaved), all globally
     * unique timestamps, inserted in scrambled (not ts-sorted, not day/cell-grouped) order so the WAL
     * write path O3-sorts every cell.
     */
    private void createAndPopulateTwins() throws SqlException {
        execute("create table c (ts timestamp, exch symbol, v double) timestamp(ts) partition by day, exch wal");
        execute("create table p (ts timestamp, exch symbol, v double) timestamp(ts) partition by day wal");

        final String rows = " values " +
                "('2024-03-03T02:00:00.000000Z','B',22.0), " +
                "('2024-03-01T01:00:00.000000Z','A',1.0), " +
                "('2024-03-03T07:00:00.000000Z','A',27.0), " +
                "('2024-03-02T01:00:00.000000Z','A',11.0), " +
                "('2024-03-01T05:00:00.000000Z','A',5.0), " +
                "('2024-03-03T03:00:00.000000Z','C',23.0), " +
                "('2024-03-01T02:00:00.000000Z','B',2.0), " +
                "('2024-03-03T09:00:00.000000Z','C',29.0), " +
                "('2024-03-02T03:00:00.000000Z','A',13.0), " +
                "('2024-03-03T05:00:00.000000Z','B',25.0), " +
                "('2024-03-01T03:00:00.000000Z','A',3.0), " +
                "('2024-03-02T02:00:00.000000Z','A',12.0), " +
                "('2024-03-03T08:00:00.000000Z','B',28.0), " +
                "('2024-03-01T06:00:00.000000Z','B',6.0), " +
                "('2024-03-03T01:00:00.000000Z','A',21.0), " +
                "('2024-03-03T06:00:00.000000Z','C',26.0), " +
                "('2024-03-01T04:00:00.000000Z','B',4.0), " +
                "('2024-03-02T04:00:00.000000Z','A',14.0), " +
                "('2024-03-01T07:00:00.000000Z','A',7.0), " +
                "('2024-03-03T04:00:00.000000Z','A',24.0)";
        execute("insert into c" + rows);
        execute("insert into p" + rows);
        drainWalQueue();

        Assert.assertFalse(
                "c must not be suspended after setup",
                engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("c"))
        );
    }

    private long toMicros(String isoTimestamp) throws SqlException {
        try (
                RecordCursorFactory factory = select("select '" + isoTimestamp + "'::timestamp ts");
                RecordCursor c = factory.getCursor(sqlExecutionContext)
        ) {
            Assert.assertTrue(c.hasNext());
            return c.getRecord().getTimestamp(0);
        }
    }
}
