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
import io.questdb.cairo.sql.TimeFrame;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.table.CompositeTimeFrameRecordCursor;
import io.questdb.griffin.engine.table.FwdTableReaderPageFrameCursor;
import io.questdb.griffin.engine.table.TimeFrameCursorImpl;
import io.questdb.std.IntList;
import io.questdb.std.Rows;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Task 2 of the composite window/horizon-join-slave TimeFrameCursor work: {@link
 * CompositeTimeFrameRecordCursor} now implements the {@link TimeFrameCursor} method surface
 * ({@code jumpTo}/{@code next}/{@code prev}/{@code open}/{@code seekEstimate}/{@code recordAt}/{@code
 * recordAtRowIndex}) over the per-day timestamp-sorted permutation that task 1 built, so the existing
 * window/horizon helpers can consume a composite table as an ASC join slave unchanged.
 * <p>
 * The oracle is the plain twin's stock {@link TimeFrameCursorImpl} over a byte-for-byte identical twin
 * table {@code p}. Both cursors are driven with a LARGE page-frame size so the twin yields exactly one
 * page frame per day-partition (no column tops, no split partitions) -- aligning its frames 1:1 with
 * the composite's one-frame-per-day model. The composite still splits each cell into its own native
 * frame(s), so an interleaved day's consecutive ordinals cross native frames: exactly the condition
 * that makes {@code recordAtRowIndex}'s re-navigation (not a bare {@code setRowIndex}) mandatory.
 * <p>
 * Dataset (composite {@code c} = {@code partition by day, exch}; plain twin {@code p} = {@code
 * partition by day}), inserted scrambled so the WAL write path O3-sorts every cell, all timestamps and
 * all {@code v} values globally unique:
 * <ul>
 *   <li>day 0 (2024-03-01): cells A/B/C interleaved, 7 rows (the cross-cell trap);</li>
 *   <li>day 1 (2024-03-02): a SINGLE cell A, 4 rows (the degenerate identity day);</li>
 *   <li>[2024-03-03 absent -- a GAP day, so day indexes stay contiguous while partition timestamps
 *   jump];</li>
 *   <li>day 2 (2024-03-04): cells A/B interleaved, 4 rows.</li>
 * </ul>
 */
public class CompositeTimeFrameCursorTest extends AbstractCairoTest {

    /**
     * Flattens the composite cursor via {@code recordAtRowIndex} under TINY page frames -- so a
     * multi-row cell splits into several native frames and even WITHIN one cell consecutive rows land
     * in different native frames -- and checks it row-for-row against the plain twin's global {@code
     * order by ts} projection. RED if {@code recordAtRowIndex} did a bare {@code setRowIndex}.
     */
    @Test
    public void testRecordAtRowIndexAcrossFramesWithinCell() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            sqlExecutionContext.changePageFrameSizes(1, 2);
            try (FwdTableReaderPageFrameCursor cFrames = pageFrameCursorOf("c")) {
                final int tsIndex = cFrames.getTableReader().getMetadata().getTimestampIndex();
                final int vIndex = cFrames.getTableReader().getMetadata().getColumnIndex("v");
                try (CompositeTimeFrameRecordCursor c =
                             new CompositeTimeFrameRecordCursor(configuration, cFrames.getTableReader().getMetadata())) {
                    c.of(cFrames, sqlExecutionContext);
                    try (
                            RecordCursorFactory factory = select("select ts, v from p order by ts");
                            RecordCursor twin = factory.getCursor(sqlExecutionContext)
                    ) {
                        final Record twinRec = twin.getRecord();
                        final Record cRec = c.getRecord();
                        c.toTop();
                        while (c.next()) {
                            final long rowCount = c.open();
                            final int d = c.getTimeFrame().getFrameIndex();
                            final long rowLo = c.getTimeFrame().getRowLo();
                            // Establish the open day (currentOpenDay) + navigate to the first ordinal's
                            // cell; recordAtRowIndex must then re-navigate for every subsequent ordinal.
                            c.recordAt(cRec, d, rowLo);
                            for (long r = rowLo; r < rowCount; r++) {
                                c.recordAtRowIndex(cRec, r);
                                Assert.assertTrue("day " + d + " row " + r + ": twin exhausted early", twin.hasNext());
                                Assert.assertEquals("day " + d + " row " + r + ": ts", twinRec.getTimestamp(0), cRec.getTimestamp(tsIndex));
                                Assert.assertEquals("day " + d + " row " + r + ": v", twinRec.getDouble(1), cRec.getDouble(vIndex), 0.0);
                            }
                        }
                        Assert.assertFalse("composite yielded fewer rows than the twin", twin.hasNext());
                    }
                }
            }
        });
    }

    /**
     * The row-for-row-vs-twin capstone: dayCount, per-frame {@code jumpTo}+{@code open} bounds, per-row
     * {@code recordAt(rec,frame,row)} + rowId round-trip + the {@code recordAtRowIndex} cross-cell trap,
     * {@code seekEstimate} frame index, and the forward/backward walk with past-end parking -- all
     * asserted identical to {@link TimeFrameCursorImpl}.
     */
    @Test
    public void testTimeFrameSurfaceMatchesPlainTwinRowForRow() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            sqlExecutionContext.changePageFrameSizes(1000, 1000);
            try (
                    FwdTableReaderPageFrameCursor cFrames = pageFrameCursorOf("c");
                    FwdTableReaderPageFrameCursor pFrames = pageFrameCursorOf("p")
            ) {
                final int tsIndex = cFrames.getTableReader().getMetadata().getTimestampIndex();
                final int vIndex = cFrames.getTableReader().getMetadata().getColumnIndex("v");
                try (
                        CompositeTimeFrameRecordCursor c =
                                new CompositeTimeFrameRecordCursor(configuration, cFrames.getTableReader().getMetadata());
                        TimeFrameCursorImpl t =
                                new TimeFrameCursorImpl(configuration, pFrames.getTableReader().getMetadata())
                ) {
                    c.of(cFrames, sqlExecutionContext);
                    t.of(
                            pFrames,
                            sqlExecutionContext.getPageFrameMinRows(),
                            sqlExecutionContext.getPageFrameMaxRows(),
                            1,
                            sqlExecutionContext.getMemoryTracker()
                    );

                    final int dayCount = countFrames(c);
                    Assert.assertEquals("composite must expose one frame per present day", 3, dayCount);
                    Assert.assertEquals("frame count vs twin", countFrames(t), dayCount);

                    assertCellStructure(c);
                    assertPerFrameAndPerRow(c, t, dayCount, tsIndex, vIndex);
                    assertSeekEstimateMatchesTwin(c, t);
                    assertForwardBackwardWalkMatchesTwin(c, t, dayCount);
                }
            }
        });
    }

    // Documents the trap precondition and the degenerate case: the interleaved days (0, 2) must cross
    // cells (so a bare setRowIndex would misread), while the single-cell day (1) must not.
    private void assertCellStructure(CompositeTimeFrameRecordCursor c) {
        Assert.assertTrue("day 0 must interleave cells (cross-cell trap precondition)", crossesCells(c, 0));
        Assert.assertFalse("day 1 is single-cell -- an identity permutation, no cross-cell adjacency", crossesCells(c, 1));
        Assert.assertTrue("day 2 must interleave cells (cross-cell trap precondition)", crossesCells(c, 2));
    }

    private void assertForwardBackwardWalkMatchesTwin(TimeFrameCursor c, TimeFrameCursor t, int dayCount) {
        c.toTop();
        t.toTop();
        int walked = 0;
        for (; ; ) {
            final boolean tn = t.next();
            final boolean cn = c.next();
            Assert.assertEquals("next() parity at step " + walked, tn, cn);
            if (!tn) {
                break;
            }
            final TimeFrame cf = c.getTimeFrame();
            final TimeFrame tf = t.getTimeFrame();
            Assert.assertEquals("walk frameIndex", tf.getFrameIndex(), cf.getFrameIndex());
            Assert.assertEquals("walk estimateLo", tf.getTimestampEstimateLo(), cf.getTimestampEstimateLo());
            Assert.assertEquals("walk estimateHi", tf.getTimestampEstimateHi(), cf.getTimestampEstimateHi());
            Assert.assertEquals("walk open()", t.open(), c.open());
            Assert.assertEquals("walk tsLo", tf.getTimestampLo(), cf.getTimestampLo());
            Assert.assertEquals("walk tsHi", tf.getTimestampHi(), cf.getTimestampHi());
            walked++;
        }
        Assert.assertEquals("forward walk length", dayCount, walked);
        // Past-end: next() parks the frame index one past the last day, exactly as the twin does.
        Assert.assertEquals("past-end frame index", dayCount, c.getTimeFrame().getFrameIndex());

        for (; ; ) {
            final boolean tp = t.prev();
            final boolean cp = c.prev();
            Assert.assertEquals("prev() parity", tp, cp);
            if (!tp) {
                break;
            }
            Assert.assertEquals("backward frameIndex", t.getTimeFrame().getFrameIndex(), c.getTimeFrame().getFrameIndex());
        }
        // Before-start: prev() parks the frame index at -1, exactly as the twin does.
        Assert.assertEquals("before-start frame index", -1, c.getTimeFrame().getFrameIndex());
    }

    private void assertPerFrameAndPerRow(TimeFrameCursor c, TimeFrameCursor t, int dayCount, int tsIndex, int vIndex) {
        final Record cRec = c.getRecord();
        final Record tRec = t.getRecord();
        for (int d = 0; d < dayCount; d++) {
            c.jumpTo(d);
            t.jumpTo(d);
            final TimeFrame cf = c.getTimeFrame();
            final TimeFrame tf = t.getTimeFrame();
            Assert.assertEquals("day " + d + " estimateLo", tf.getTimestampEstimateLo(), cf.getTimestampEstimateLo());
            Assert.assertEquals("day " + d + " estimateHi", tf.getTimestampEstimateHi(), cf.getTimestampEstimateHi());

            final long cRows = c.open();
            final long tRows = t.open();
            Assert.assertEquals("day " + d + " open() rowCount", tRows, cRows);
            Assert.assertTrue("day " + d + " must be non-empty", cRows > 0);
            Assert.assertEquals("day " + d + " rowLo", tf.getRowLo(), cf.getRowLo());
            Assert.assertEquals("day " + d + " rowHi", tf.getRowHi(), cf.getRowHi());
            Assert.assertEquals("day " + d + " tsLo", tf.getTimestampLo(), cf.getTimestampLo());
            Assert.assertEquals("day " + d + " tsHi", tf.getTimestampHi(), cf.getTimestampHi());

            final long rowLo = cf.getRowLo();
            final long rowHi = cf.getRowHi();
            // recordAt(rec, frame, row) + the rowId round-trip, both vs the twin.
            for (long r = rowLo; r < rowHi; r++) {
                c.recordAt(cRec, d, r);
                t.recordAt(tRec, d, r);
                assertSameRow("recordAt day " + d + " row " + r, tRec, cRec, tsIndex, vIndex);

                final long rowId = Rows.toRowID(d, r);
                Assert.assertEquals("rowId partition round-trip", d, Rows.toPartitionIndex(rowId));
                Assert.assertEquals("rowId local round-trip", r, Rows.toLocalRowID(rowId));
                c.recordAt(cRec, rowId);
                assertSameRow("recordAt(rowId) day " + d + " row " + r, tRec, cRec, tsIndex, vIndex);
            }

            // THE TRAP: establish the day via recordAt(rec, d, rowLo), then walk the WHOLE day with
            // recordAtRowIndex. Consecutive ordinals of an interleaved day cross cells, so a bare
            // setRowIndex would read the wrong cell -- this loop goes RED under that bug.
            c.recordAt(cRec, d, rowLo);
            for (long r = rowLo; r < rowHi; r++) {
                c.recordAtRowIndex(cRec, r);
                t.recordAt(tRec, d, r);
                assertSameRow("recordAtRowIndex day " + d + " row " + r, tRec, cRec, tsIndex, vIndex);
            }
        }
    }

    private void assertSameRow(String msg, Record expected, Record actual, int tsIndex, int vIndex) {
        Assert.assertEquals(msg + ": ts", expected.getTimestamp(tsIndex), actual.getTimestamp(tsIndex));
        Assert.assertEquals(msg + ": v", expected.getDouble(vIndex), actual.getDouble(vIndex), 0.0);
    }

    private void assertSeekEstimateMatchesTwin(TimeFrameCursor c, TimeFrameCursor t) throws SqlException {
        final long hour = 3_600_000_000L;
        final long day0 = toMicros("2024-03-01T00:00:00.000000Z");
        final long day1 = toMicros("2024-03-02T00:00:00.000000Z");
        final long gapDay = toMicros("2024-03-03T00:00:00.000000Z"); // absent partition
        final long day2 = toMicros("2024-03-04T00:00:00.000000Z");
        final long dayAfter = toMicros("2024-03-05T00:00:00.000000Z");
        final long[] probes = {
                Long.MIN_VALUE,
                day0 - 1,
                day0,
                day0 + hour,
                day1,
                day1 + hour,
                gapDay,
                day2,
                day2 + hour,
                dayAfter,
                Long.MAX_VALUE
        };
        for (long ts : probes) {
            c.seekEstimate(ts);
            t.seekEstimate(ts);
            Assert.assertEquals("seekEstimate frameIndex ts=" + ts, t.getTimeFrame().getFrameIndex(), c.getTimeFrame().getFrameIndex());
            Assert.assertEquals("seekEstimate estimateLo ts=" + ts, t.getTimeFrame().getTimestampEstimateLo(), c.getTimeFrame().getTimestampEstimateLo());
            Assert.assertEquals("seekEstimate estimateHi ts=" + ts, t.getTimeFrame().getTimestampEstimateHi(), c.getTimeFrame().getTimestampEstimateHi());
        }
    }

    private int countFrames(TimeFrameCursor cursor) {
        cursor.toTop();
        int n = 0;
        while (cursor.next()) {
            n++;
        }
        return n;
    }

    private void createTwins() throws SqlException {
        execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY, exch WAL");
        execute("CREATE TABLE p (ts TIMESTAMP, exch SYMBOL, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");

        final String rows = " VALUES " +
                "('2024-03-04T02:00:00.000000Z','B',402.0)," +
                "('2024-03-01T03:00:00.000000Z','A',103.0)," +
                "('2024-03-02T02:00:00.000000Z','A',202.0)," +
                "('2024-03-01T07:00:00.000000Z','C',107.0)," +
                "('2024-03-04T01:00:00.000000Z','A',401.0)," +
                "('2024-03-01T01:00:00.000000Z','A',101.0)," +
                "('2024-03-02T04:00:00.000000Z','A',204.0)," +
                "('2024-03-01T05:00:00.000000Z','B',105.0)," +
                "('2024-03-04T04:00:00.000000Z','B',404.0)," +
                "('2024-03-01T02:00:00.000000Z','B',102.0)," +
                "('2024-03-02T01:00:00.000000Z','A',201.0)," +
                "('2024-03-01T06:00:00.000000Z','A',106.0)," +
                "('2024-03-04T03:00:00.000000Z','A',403.0)," +
                "('2024-03-01T04:00:00.000000Z','C',104.0)," +
                "('2024-03-02T03:00:00.000000Z','A',203.0)";
        execute("INSERT INTO c" + rows);
        execute("INSERT INTO p" + rows);
        drainWalQueue();

        Assert.assertFalse(
                "c must not be suspended after setup",
                engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("c"))
        );
        Assert.assertFalse(
                "p must not be suspended after setup",
                engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("p"))
        );
    }

    // Whether any two adjacent ordinals of the day live in different native cell frames.
    private boolean crossesCells(CompositeTimeFrameRecordCursor c, int dayIndex) {
        final long rowCount = c.getDayRowCount(dayIndex);
        int prevCellFrame = -1;
        for (long ord = 0; ord < rowCount; ord++) {
            final int cellFrame = CompositeTimeFrameRecordCursor.unpackFrameIndex(c.getPermutationEntry(dayIndex, ord));
            if (ord > 0 && cellFrame != prevCellFrame) {
                return true;
            }
            prevCellFrame = cellFrame;
        }
        return false;
    }

    // Builds a forward page-frame cursor over the table's reader, mirroring the low-level idiom in
    // PageFrameCursorReleasePartitionTest. The returned cursor OWNS the reader; closing it closes the
    // whole chain.
    private FwdTableReaderPageFrameCursor pageFrameCursorOf(String tableName) throws SqlException {
        final TableReader reader = engine.getReader(tableName);
        try {
            final IntList columnIndexes = new IntList();
            final IntList columnSizeShifts = new IntList();
            for (int i = 0, n = reader.getMetadata().getColumnCount(); i < n; i++) {
                columnIndexes.add(i);
                columnSizeShifts.add(ColumnType.pow2SizeOf(reader.getMetadata().getColumnType(i)));
            }
            final FullFwdPartitionFrameCursor partitionFrameCursor = new FullFwdPartitionFrameCursor();
            partitionFrameCursor.of(reader);
            final FwdTableReaderPageFrameCursor pageFrameCursor = new FwdTableReaderPageFrameCursor(
                    columnIndexes,
                    columnSizeShifts,
                    null,
                    1
            );
            pageFrameCursor.of(sqlExecutionContext, partitionFrameCursor);
            return pageFrameCursor;
        } catch (Throwable th) {
            reader.close();
            throw th;
        }
    }

    private long toMicros(String isoTimestamp) throws SqlException {
        try (
                RecordCursorFactory factory = select("select '" + isoTimestamp + "'::timestamp ts");
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            Assert.assertTrue(cursor.hasNext());
            return cursor.getRecord().getTimestamp(0);
        }
    }
}
