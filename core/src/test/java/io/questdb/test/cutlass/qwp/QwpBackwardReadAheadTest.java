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

package io.questdb.test.cutlass.qwp;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypeDriver;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.StringTypeDriver;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.cutlass.qwp.server.egress.QwpBackwardReadAhead;
import io.questdb.mp.CarrierIdentity;
import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.Utf8String;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongUnaryOperator;

/**
 * Pins {@link QwpBackwardReadAhead}, which keeps the rows of a backward page-frame stream
 * on their way into memory ahead of its walk down. With MADV_WILLNEED the stream asks for
 * windows of rows below the slice it appends: the first window holds the top rows of the
 * frame, the rows of the first batch; the next ones double up to a byte cap; every row of
 * every column that is not in memory is asked for once, before the stream reads it. After
 * its first window, the top rows of its first frame, the stream checks whether a window is
 * in memory, backs off checking while windows are not, and trusts small frames after a
 * streak in memory until its thread reads from storage. Without MADV_WILLNEED the stream
 * reads the same windows itself, each when the walk reaches it, checks nothing, and asks the
 * OS for nothing, so it counts none of those bytes as read ahead.
 */
public class QwpBackwardReadAheadTest {
    // frames a stream walks in a round of an allocation measurement, and before the first round
    private static final int ALLOCATION_ROUND_FRAMES = 1_000;
    private static final int ALLOCATION_ROUNDS = 5;
    private static final int ALLOCATION_WARM_UP_FRAMES = 10_000;
    // the egress walks a frame down in slices of at most this many rows
    private static final int SLICE_ROWS = 1_024;
    private static final int TRUST_STREAK = QwpBackwardReadAhead.TRUST_STREAK;

    @Test
    public void testColdChecksBackOff() throws Exception {
        // After a check finds pages missing, the stream advises the next frames without
        // checking them: 1, then 3, 7, 15, 31 and at most 63 frames between two checks. It
        // asks for every frame in full all the same.
        TestUtils.assertMemoryLeak(() -> {
            try (TestFrame frame = smallFrame()) {
                final RecordingFacade ff = new RecordingFacade(true);
                final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
                readAhead.of(frame.metadata());
                walkFirstFrame(readAhead, frame);
                final IntList checkedFrames = new IntList();
                for (int f = 0; f < 300; f++) {
                    final int checks = ff.residencyChecks;
                    Assert.assertEquals(frame.frameBytes(), walk(readAhead, frame));
                    if (ff.residencyChecks > checks) {
                        // a check asks about each of the two columns
                        Assert.assertEquals(checks + 2, ff.residencyChecks);
                        checkedFrames.add(f);
                    }
                }
                Assert.assertEquals("[0,2,6,14,30,62,126,190,254]", checkedFrames.toString());
                Assert.assertEquals(0, ff.faultReads);
            }
        });
    }

    @Test
    public void testEachStreamStartsAfresh() throws Exception {
        // of() and clear() start a new stream: its first frame goes out unchecked, it checks
        // the frame after it even when the stream before had stopped checking, and it trusts
        // after the first streak, not the longer one the stream before needed.
        TestUtils.assertMemoryLeak(() -> {
            try (TestFrame frame = smallFrame()) {
                final RecordingFacade ff = new RecordingFacade(true);
                final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
                for (int stream = 0; stream < 3; stream++) {
                    if (stream == 2) {
                        readAhead.clear();
                    }
                    readAhead.of(frame.metadata());
                    ff.isResident = true;
                    final int checks = ff.residencyChecks;
                    walkFirstFrame(readAhead, frame);
                    Assert.assertEquals(checks, ff.residencyChecks);
                    for (int f = 0; f < TRUST_STREAK; f++) {
                        Assert.assertEquals(0, walk(readAhead, frame));
                        Assert.assertEquals(checks + 2 * (f + 1), ff.residencyChecks);
                    }
                    // trusted
                    Assert.assertEquals(0, walk(readAhead, frame));
                    Assert.assertEquals(checks + 2 * TRUST_STREAK, ff.residencyChecks);
                    // A read from storage ends the trust, and the next frame is out of memory:
                    // the stream would advise the frame after it unchecked, and would need a
                    // streak twice as long to trust again.
                    ff.majorFaults++;
                    ff.isResident = false;
                    Assert.assertEquals(frame.frameBytes(), walk(readAhead, frame));
                    Assert.assertEquals(checks + 2 * TRUST_STREAK + 2, ff.residencyChecks);
                }
            }
        });
    }

    @Test
    public void testFirstBatchChecksItsLaterWindows() throws Exception {
        // A first batch whose rows take more than one window goes out as soon as one that fits a
        // window does: the stream asks for its first window unchecked and checks the later ones,
        // as it checks the windows after the batch. Over data in memory, it asks for one window.
        TestUtils.assertMemoryLeak(() -> {
            final long rows = 20_000;
            final long batchRows = QwpBackwardReadAhead.INITIAL_WINDOW_ROWS;
            try (SyntheticFrame frame = new SyntheticFrame(new int[]{ColumnType.LONG, ColumnType.VARCHAR}, rows)) {
                frame.values(1, 0, r -> 4_000);
                // 8 bytes of LONG, a 16-byte aux entry and 4,000 bytes of data a row
                final long fitRows = QwpBackwardReadAhead.MAX_WINDOW_BYTES / (8 + 16 + 4_000);
                Assert.assertEquals(4_169, fitRows);
                for (boolean isResident : new boolean[]{true, false}) {
                    final RecordingFacade ff = new RecordingFacade(true);
                    ff.isResident = isResident;
                    final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
                    readAhead.of(frame.metadata());
                    readAhead.ofFrame(frame, rows);
                    long bytes = 0;
                    for (long emitted = 0; emitted < batchRows; emitted += SLICE_ROWS) {
                        bytes += readAhead.advise(rows - emitted - SLICE_ROWS, 0);
                    }
                    Assert.assertEquals(ff.requestedBytes(), bytes);
                    if (isResident) {
                        // the first window: the aux entries of the batch, which fitting the window
                        // reads, and the LONG values and the data of the rows that fit
                        Assert.assertEquals(16 * batchRows + (8 + 4_000) * fitRows, bytes);
                        Assert.assertTrue("checks=" + ff.residencyChecks, ff.residencyChecks > 0);
                    } else {
                        frame.assertCovered("cold first batch", ff, rows - batchRows, rows);
                    }
                }
            }
        });
    }

    @Test
    public void testFirstBatchChecksTheFramesAfterTheFirst() throws Exception {
        // The first batch goes out as soon as its rows are in memory: the stream asks for its
        // first window, the top rows of the frame that holds its top row, without checking
        // whether they are in memory. It checks every window after it, those of the frames the
        // first batch reaches included, from the frame right after the first, whose top row
        // goes out at row 3,000, or at row 1 after a first frame of one row.
        TestUtils.assertMemoryLeak(() -> {
            try (
                    TestFrame small = smallFrame();
                    TestFrame oneRow = new TestFrame(new int[]{ColumnType.LONG, ColumnType.DOUBLE}, false, 0, 1)
            ) {
                final RecordingFacade ff = new RecordingFacade(true);
                ff.isResident = true;
                final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
                int checks = 0;
                for (TestFrame first : new TestFrame[]{small, oneRow}) {
                    readAhead.of(small.metadata());
                    ff.clear();
                    // the stream's first frame: asked for whole, a request a column, unchecked
                    Assert.assertEquals(first.frameBytes(), walk(readAhead, first));
                    Assert.assertEquals(2, ff.requestCount());
                    Assert.assertEquals(checks, ff.residencyChecks);
                    // the next frame, in the first batch: in memory, so not asked for
                    Assert.assertEquals(0, walk(readAhead, small));
                    Assert.assertEquals(2, ff.requestCount());
                    Assert.assertEquals(checks += 2, ff.residencyChecks);
                }
                Assert.assertEquals(0, ff.faultReads);
            }
        });
    }

    @Test
    public void testFirstBatchOverSmallFrames() throws Exception {
        // A first batch over small frames, the partitions of a table that takes a few rows an
        // hour or a day, reaches thousands of them, here 4,096 frames of 4 rows. A request or a
        // check of each of their ranges would cost more than the walk. Over data in memory, the
        // stream asks for the first frame, checks the next TRUST_STREAK, and trusts the rest.
        // Over data out of memory, it asks for every frame whole, and checks only frames 1, 3, 7,
        // 15, 31 and 63, then every 64th: 69 frames.
        TestUtils.assertMemoryLeak(() -> {
            // a LONG, a DOUBLE, a STRING, a VARCHAR, an inline VARCHAR and a column top
            try (TestFrame frame = new TestFrame(new int[]{ColumnType.LONG, ColumnType.DOUBLE}, true, 0, 4)) {
                final int frames = (int) (QwpBackwardReadAhead.INITIAL_WINDOW_ROWS / frame.rows);
                // values, aux entries and data: 7 ranges, a request or a check each
                final int ranges = frame.rangeCount();
                Assert.assertEquals(7, ranges);
                for (boolean isResident : new boolean[]{true, false}) {
                    final RecordingFacade ff = new RecordingFacade(true);
                    ff.isResident = isResident;
                    final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
                    readAhead.of(frame.metadata());
                    for (int f = 0; f < frames; f++) {
                        Assert.assertEquals("frame " + f, f == 0 || !isResident ? frame.frameBytes() : 0, walk(readAhead, frame));
                    }
                    if (isResident) {
                        Assert.assertEquals(ranges, ff.requestCount());
                        Assert.assertEquals(TRUST_STREAK * ranges, ff.residencyChecks);
                        // the last frame of the streak reads the faults the trust starts from,
                        // and each trusted frame reads them again
                        Assert.assertEquals(frames - TRUST_STREAK, ff.faultReads);
                    } else {
                        Assert.assertEquals(frames * ranges, ff.requestCount());
                        Assert.assertEquals(69 * ranges, ff.residencyChecks);
                        Assert.assertEquals(0, ff.faultReads);
                    }
                }
            }
        });
    }

    @Test
    public void testFirstWindowHoldsTheTopRowsOfEveryColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int rows = 100_000;
            final int lo = 5_000;
            final int[] fixedTypes = {
                    ColumnType.LONG, ColumnType.INT, ColumnType.SHORT, ColumnType.BYTE,
                    ColumnType.SYMBOL, ColumnType.UUID, ColumnType.LONG256
            };
            for (boolean isWillNeed : new boolean[]{true, false}) {
                try (TestFrame frame = new TestFrame(fixedTypes, true, lo, lo + rows)) {
                    final RecordingFacade ff = new RecordingFacade(isWillNeed);
                    final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
                    readAhead.of(frame.metadata());
                    readAhead.ofFrame(frame, rows);

                    final long top = rows - QwpBackwardReadAhead.INITIAL_WINDOW_ROWS;
                    Assert.assertEquals(frame.bytesOfRows(top, rows), advise(readAhead, ff, rows - SLICE_ROWS));
                    // one range a column for the window's rows, values or aux entries, and one
                    // for the data of a var-size column that stores any; none for the column top
                    int requests = 0;
                    for (int ci = 0; ci < frame.getColumnCount(); ci++) {
                        final long[] range = frame.rowRange(ci, top, rows);
                        if (range[1] > range[0]) {
                            Assert.assertTrue("column " + ci, ff.contains(range[0], range[1]));
                            requests++;
                        }
                        if (frame.isVarSize(ci)) {
                            final long[] dataRange = frame.dataRange(ci, top, rows);
                            if (dataRange[1] > dataRange[0]) {
                                Assert.assertTrue("data of column " + ci, ff.contains(dataRange[0], dataRange[1]));
                                requests++;
                            }
                        }
                    }
                    // 7 fixed-size columns, STRING and VARCHAR aux and data, the inline VARCHAR's aux
                    Assert.assertEquals(12, requests);
                    Assert.assertEquals(requests, ff.requestCount());
                }
            }
        });
    }

    @Test
    public void testFramesWithoutReadAheadAdvanceTheStream() throws Exception {
        // Parquet frames and frames of column tops ask for nothing, but their rows go out all
        // the same: a native frame after one such row, or 20,000, does not hold the stream's top
        // row, so the stream checks it. A frame without rows moves the stream nowhere: the frame
        // after it holds the top row, and goes out unchecked.
        TestUtils.assertMemoryLeak(() -> {
            try (TestFrame frame = smallFrame()) {
                final RecordingFacade ff = new RecordingFacade(true);
                ff.isResident = true;
                final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
                int checks = 0;
                for (long rowsBefore : new long[]{1, 20_000}) {
                    readAhead.of(frame.metadata());
                    frame.format = PartitionFormat.PARQUET;
                    readAhead.ofFrame(frame, rowsBefore);
                    Assert.assertEquals(0, readAhead.advise(Math.max(0, rowsBefore - SLICE_ROWS), 0));
                    frame.format = PartitionFormat.NATIVE;
                    Assert.assertEquals(0, walk(readAhead, frame));
                    Assert.assertEquals(checks += 2, ff.residencyChecks);

                    // rows of column tops
                    readAhead.of(frame.metadata());
                    final TestFrame.Saved saved = frame.clearAll();
                    readAhead.ofFrame(frame, rowsBefore);
                    Assert.assertEquals(0, readAhead.advise(Math.max(0, rowsBefore - SLICE_ROWS), 0));
                    frame.restore(saved);
                    Assert.assertEquals(0, walk(readAhead, frame));
                    Assert.assertEquals(checks += 2, ff.residencyChecks);
                }
                Assert.assertEquals(0, ff.requestCount());

                // a frame without rows moves the stream nowhere
                readAhead.of(frame.metadata());
                readAhead.ofFrame(frame, 0);
                Assert.assertEquals(0, readAhead.advise(0, 0));
                Assert.assertEquals(frame.frameBytes(), walk(readAhead, frame));
                Assert.assertEquals(checks, ff.residencyChecks);
                Assert.assertEquals(2, ff.requestCount());
            }
        });
    }

    @Test
    public void testInMemoryCheckEndsTheBackOff() throws Exception {
        // A check that finds the frame in memory ends the back-off: the stream checks every
        // frame after it again.
        TestUtils.assertMemoryLeak(() -> {
            try (TestFrame frame = smallFrame()) {
                final RecordingFacade ff = new RecordingFacade(true);
                final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
                readAhead.of(frame.metadata());
                walkFirstFrame(readAhead, frame);
                final IntList checkedFrames = new IntList();
                for (int f = 0; f < 21; f++) {
                    // the data comes into memory while the stream advises frames 7 to 13 unchecked
                    ff.isResident = f >= 7;
                    final int checks = ff.residencyChecks;
                    Assert.assertEquals(f < 14 ? frame.frameBytes() : 0, walk(readAhead, frame));
                    if (ff.residencyChecks > checks) {
                        checkedFrames.add(f);
                    }
                }
                Assert.assertEquals("[0,2,6,14,15,16,17,18,19,20]", checkedFrames.toString());
                // seven frames in memory in a row: one short of trusting
                Assert.assertEquals(0, ff.faultReads);
            }
        });
    }

    @Test
    public void testLargeFramesAreNeverTrusted() throws Exception {
        // A fault in a frame with a range past SMALL_FRAME_RANGE_BYTES reads at most 64 KiB of
        // it at a time, not the whole range, so the stream checks each window of such a frame,
        // however many before it were in memory.
        TestUtils.assertMemoryLeak(() -> {
            // 20,000 LONG rows, 160,000 bytes: windows of 16,384 and 3,616 rows
            try (TestFrame frame = new TestFrame(new int[]{ColumnType.LONG}, false, 0, 20_000)) {
                final RecordingFacade ff = new RecordingFacade(true);
                ff.isResident = true;
                final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
                readAhead.of(frame.metadata());
                // the first window goes out unchecked, the second starts at row 16,384
                Assert.assertEquals(16_384 * 8L, walk(readAhead, frame));
                Assert.assertEquals(1, ff.residencyChecks);
                for (int f = 1; f < 50; f++) {
                    Assert.assertEquals(0, walk(readAhead, frame));
                    Assert.assertEquals(1 + 2 * f, ff.residencyChecks);
                }
                Assert.assertEquals(1, ff.requestCount());
                Assert.assertEquals(0, ff.faultReads);
            }
        });
    }

    @Test
    public void testLargeWindowsInMemoryAreNotAskedFor() throws Exception {
        // The stream checks each window after its first one and asks for none that is in
        // memory: of a 200,000-row frame, whose windows hold 16,384, 32,768, 65,536 and the
        // last 85,312 rows, it asks for the first only.
        TestUtils.assertMemoryLeak(() -> {
            try (TestFrame frame = new TestFrame(new int[]{ColumnType.LONG}, false, 0, 200_000)) {
                final RecordingFacade ff = new RecordingFacade(true);
                ff.isResident = true;
                final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
                readAhead.of(frame.metadata());
                Assert.assertEquals(16_384 * 8L, walk(readAhead, frame));
                final long[] firstWindow = frame.rowRange(0, 200_000 - 16_384, 200_000);
                Assert.assertTrue(ff.contains(firstWindow[0], firstWindow[1]));
                Assert.assertEquals(1, ff.requestCount());
                Assert.assertEquals(3, ff.residencyChecks);
                Assert.assertEquals(0, ff.faultReads);
            }
        });
    }

    @Test
    public void testMissingRangesAreAskedFor() throws Exception {
        // A window that is in memory only in part asks for the ranges that are not, and the
        // window after it goes out unchecked; the one after that is in memory again.
        TestUtils.assertMemoryLeak(() -> {
            try (TestFrame frame = new TestFrame(new int[]{ColumnType.LONG, ColumnType.DOUBLE}, false, 0, 200_000)) {
                final RecordingFacade ff = new RecordingFacade(true);
                ff.isResident = true;
                // the DOUBLE values of the second window, rows [150,848, 183,616)
                final long[] missing = frame.rowRange(1, 150_848, 183_616);
                ff.miss(missing[0], missing[1]);
                final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
                readAhead.of(frame.metadata());
                // 16,384 rows of both columns, the missing 32,768 DOUBLEs, then 65,536 rows of both, unchecked
                Assert.assertEquals(16_384 * 16L + 32_768 * 8L + 65_536 * 16L, walk(readAhead, frame));
                Assert.assertEquals(5, ff.requestCount());
                Assert.assertTrue(ff.contains(missing[0], missing[1]));
                final long[] inMemory = frame.rowRange(0, 150_848, 183_616);
                Assert.assertEquals(inMemory[1], ff.lowestWithin(inMemory[0], inMemory[1]));
                final long[] third = frame.rowRange(0, 85_312, 150_848);
                Assert.assertTrue(ff.contains(third[0], third[1]));
                // the second window and the last ask about both columns
                Assert.assertEquals(4, ff.residencyChecks);
            }
        });
    }

    @Test
    public void testNoTrustWithoutThreadFaults() throws Exception {
        // Where the OS does not count the major faults of a thread, the stream could not tell
        // that a trusted frame read from storage, so it checks every frame, and asks for the
        // count only once.
        TestUtils.assertMemoryLeak(() -> {
            try (TestFrame frame = smallFrame()) {
                final RecordingFacade ff = new RecordingFacade(true);
                ff.isResident = true;
                ff.majorFaults = -1;
                final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
                readAhead.of(frame.metadata());
                walkFirstFrame(readAhead, frame);
                for (int f = 0; f < 100; f++) {
                    Assert.assertEquals(0, walk(readAhead, frame));
                    Assert.assertEquals(2 * (f + 1), ff.residencyChecks);
                }
                Assert.assertEquals(1, ff.faultReads);
            }
        });
    }

    @Test
    public void testNothingToReadAhead() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestFrame frame = new TestFrame(new int[]{ColumnType.LONG}, true, 0, 3_000)) {
                for (boolean isWillNeed : new boolean[]{true, false}) {
                    final RecordingFacade ff = new RecordingFacade(isWillNeed);
                    final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
                    // no frame yet
                    Assert.assertEquals(0, readAhead.advise(0, 0));
                    readAhead.of(frame.metadata());
                    Assert.assertEquals(0, readAhead.advise(0, 0));

                    // the stream decodes a parquet frame into memory when it navigates to it
                    frame.format = PartitionFormat.PARQUET;
                    readAhead.ofFrame(frame, 3_000);
                    Assert.assertEquals(0, readAhead.advise(3_000 - SLICE_ROWS, 0));
                    frame.format = PartitionFormat.NATIVE;

                    // A custom (Delta) frame reports NATIVE, but its addresses point at its
                    // base, not at its rows: the stream materializes it into memory when it
                    // navigates to it, as it decodes a parquet frame.
                    frame.partitionFrameState = 1;
                    readAhead.ofFrame(frame, 3_000);
                    Assert.assertEquals(0, readAhead.advise(3_000 - SLICE_ROWS, 0));
                    frame.partitionFrameState = 0;

                    // a frame without rows
                    readAhead.ofFrame(frame, 0);
                    Assert.assertEquals(0, readAhead.advise(0, 0));

                    // a frame whose every column is a column top
                    final TestFrame.Saved saved = frame.clearAll();
                    readAhead.ofFrame(frame, 3_000);
                    Assert.assertEquals(0, readAhead.advise(3_000 - SLICE_ROWS, 0));
                    frame.restore(saved);

                    // a bound frame, then cleared
                    readAhead.ofFrame(frame, 3_000);
                    readAhead.clear();
                    Assert.assertEquals(0, readAhead.advise(3_000 - SLICE_ROWS, 0));
                    // the stream asked for none of the frames, nor read any itself
                    Assert.assertEquals(0, ff.requestCount());
                }
            }
        });
    }

    @Test
    public void testOfFrameRestartsTheWindow() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            for (boolean isWillNeed : new boolean[]{true, false}) {
                try (
                        TestFrame first = new TestFrame(new int[]{ColumnType.LONG}, false, 0, 50_000);
                        TestFrame second = new TestFrame(new int[]{ColumnType.LONG}, false, 0, 40_000)
                ) {
                    final RecordingFacade ff = new RecordingFacade(isWillNeed);
                    final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
                    readAhead.of(first.metadata());
                    readAhead.ofFrame(first, 50_000);
                    Assert.assertEquals(8L * QwpBackwardReadAhead.INITIAL_WINDOW_ROWS, advise(readAhead, ff, 50_000 - SLICE_ROWS));
                    // the stream leaves the first frame part way and moves to the next
                    readAhead.ofFrame(second, 40_000);
                    ff.clear();
                    Assert.assertEquals(8L * QwpBackwardReadAhead.INITIAL_WINDOW_ROWS, advise(readAhead, ff, 40_000 - SLICE_ROWS));
                    final long[] range = second.rowRange(0, 40_000 - QwpBackwardReadAhead.INITIAL_WINDOW_ROWS, 40_000);
                    Assert.assertTrue(ff.contains(range[0], range[1]));
                    Assert.assertEquals(1, ff.requestCount());
                }
            }
        });
    }

    @Test
    public void testSmallFrameBounds() throws Exception {
        // A frame is small, and the stream trusts it after a streak in memory, when no range it
        // publishes holds more than SMALL_FRAME_RANGE_BYTES: values, aux entries and data
        // alike. A column top publishes no range, whatever size it hints at.
        TestUtils.assertMemoryLeak(() -> {
            final int limit = (int) QwpBackwardReadAhead.SMALL_FRAME_RANGE_BYTES;
            try (
                    TestFrame atLimit = new TestFrame(new int[]{ColumnType.LONG}, false, 0, limit / 8);
                    TestFrame pastLimit = new TestFrame(new int[]{ColumnType.LONG}, false, 0, limit / 8 + 1)
            ) {
                Assert.assertTrue(isTrustedAfterStreak(atLimit, atLimit.metadata()));
                Assert.assertFalse(isTrustedAfterStreak(pastLimit, pastLimit.metadata()));
            }
            // VARCHAR values that sit inline in their 16-byte aux entries leave no data
            assertVarSizeBounds(ColumnType.VARCHAR, 16, "v");
            // a STRING of 30 chars takes 64 bytes of data, 4 of them for the length, and 8 of aux
            assertVarSizeBounds(ColumnType.STRING, 64, "012345678901234567890123456789");
            // a column top next to a small LONG range
            try (MemoryCARWImpl longs = new MemoryCARWImpl(4_096, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                for (int r = 0; r < 1_000; r++) {
                    longs.putLong(r);
                }
                final AddressedFrame frame = new AddressedFrame(2, 1_000);
                frame.set(0, 0, limit + 1, 0, 0);
                frame.set(1, longs.addressOf(0), 8_000, 0, 0);
                Assert.assertTrue(isTrustedAfterStreak(frame, metadata(new int[]{ColumnType.LONG, ColumnType.LONG})));
            }
        });
    }

    @Test
    public void testSmallFrameGoesInOneWindow() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            for (boolean isWillNeed : new boolean[]{true, false}) {
                try (TestFrame frame = new TestFrame(new int[]{ColumnType.INT, ColumnType.DOUBLE}, true, 300, 1_000)) {
                    final RecordingFacade ff = new RecordingFacade(isWillNeed);
                    final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
                    readAhead.of(frame.metadata());
                    readAhead.ofFrame(frame, 700);
                    Assert.assertEquals(frame.frameBytes(), advise(readAhead, ff, 0));
                    Assert.assertEquals(0, advise(readAhead, ff, 0));
                    assertEachColumnTiled(frame, ff);
                }
            }
        });
    }

    @Test
    public void testSmallFramesInMemoryAreTrusted() throws Exception {
        // After the stream's first frame, TRUST_STREAK small frames in a row found in memory
        // make the stream trust the next ones: it no longer checks them or asks for them, and
        // before each it reads the byte its walk reads first, and the major faults of its thread.
        TestUtils.assertMemoryLeak(() -> {
            try (TestFrame frame = smallFrame()) {
                Unsafe.putByte(frame.getPageAddress(0) + frame.getPageSize(0) - 1, (byte) 3);
                final RecordingFacade ff = new RecordingFacade(true);
                ff.isResident = true;
                ff.majorFaults = 5;
                final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
                readAhead.of(frame.metadata());
                walkFirstFrame(readAhead, frame);
                Assert.assertEquals(2, ff.requestCount());
                for (int f = 0; f < TRUST_STREAK; f++) {
                    Assert.assertEquals(0, walk(readAhead, frame));
                }
                Assert.assertEquals(2 * TRUST_STREAK, ff.residencyChecks);
                // the last frame of the streak reads the faults the trust starts from
                Assert.assertEquals(1, ff.faultReads);
                final long sink = readAhead.getTouchSink();
                for (int f = 0; f < 100; f++) {
                    Assert.assertEquals(0, walk(readAhead, frame));
                }
                Assert.assertEquals(2 * TRUST_STREAK, ff.residencyChecks);
                Assert.assertEquals(101, ff.faultReads);
                Assert.assertEquals(sink + 100 * 3, readAhead.getTouchSink());
                Assert.assertEquals(2, ff.requestCount());
            }
        });
    }

    @Test
    public void testStreamAllocatesNothingPerFrame() throws Exception {
        // The read-ahead runs on the egress data path, frame after frame: once warm, it allocates
        // nothing, whether it checks windows, trusts small frames or reads its windows itself, on
        // a worker thread, which binds as a carrier, and on a thread that does not.
        TestUtils.assertMemoryLeak(() -> {
            final ExecutorService carrier = newCarrier();
            try (
                    TestUtils.ThreadMetricsScope<com.sun.management.ThreadMXBean> scope = TestUtils.threadAllocationScope();
                    TestFrame small = smallFrame();
                    // 160,000 bytes a column: a window holds part of the frame
                    TestFrame large = new TestFrame(new int[]{ColumnType.LONG, ColumnType.DOUBLE}, false, 0, 20_000)
            ) {
                final com.sun.management.ThreadMXBean threadMXBean = scope.getBean();
                for (TestFrame frame : new TestFrame[]{small, large}) {
                    // the pages of the frame are in memory
                    for (int ci = 0, n = frame.getColumnCount(); ci < n; ci++) {
                        Vect.memset(frame.getPageAddress(ci), frame.getPageSize(ci), 0);
                    }
                }
                for (boolean isWillNeedSupported : new boolean[]{true, false}) {
                    for (TestFrame frame : new TestFrame[]{small, large}) {
                        for (boolean isCarrier : new boolean[]{false, true}) {
                            final String context = (isWillNeedSupported ? "advised windows" : "windows read")
                                    + ", rows=" + frame.rows + ", carrier=" + isCarrier;
                            final ReadAheadFilesFacade ff = new ReadAheadFilesFacade(isWillNeedSupported);
                            final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
                            readAhead.of(frame.metadata());
                            final Callable<Long> stream = () -> leastAllocatedPerRound(threadMXBean, readAhead, frame);
                            Assert.assertEquals(context, 0, (long) (isCarrier ? runOn(carrier, stream) : stream.call()));
                            if (!isWillNeedSupported) {
                                Assert.assertEquals(context, 0, ff.residencyCheckCount());
                            } else if (Os.isLinux() && frame == small) {
                                // The stream trusted the frames in memory: fewer checks than frames, where
                                // a stream that checked every frame would ask about both columns of each.
                                final long frames = ALLOCATION_WARM_UP_FRAMES + (long) ALLOCATION_ROUNDS * ALLOCATION_ROUND_FRAMES;
                                Assert.assertTrue(context + ", checks=" + ff.residencyCheckCount(), ff.residencyCheckCount() < frames);
                            } else {
                                Assert.assertTrue(context, ff.residencyCheckCount() > 0);
                            }
                        }
                    }
                }
            } finally {
                stop(carrier);
            }
        });
    }

    @Test
    public void testTouchPathChecksNothing() throws Exception {
        // Without MADV_WILLNEED the stream reads every frame, in memory or not: a read of a page
        // in memory costs a load, so the stream checks nothing, trusts nothing and needs no
        // fault counts. It asks the OS for nothing, so it counts none of the bytes it reads as
        // read ahead, and it keeps the sums its reads return, which stop the JIT from dropping
        // the loads.
        TestUtils.assertMemoryLeak(() -> {
            try (TestFrame frame = smallFrame()) {
                for (boolean isResident : new boolean[]{true, false}) {
                    final RecordingFacade ff = new RecordingFacade(false);
                    ff.isResident = isResident;
                    final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
                    readAhead.of(frame.metadata());
                    for (int f = 1; f <= 50; f++) {
                        Assert.assertEquals(0, walk(readAhead, frame));
                        // each frame once, its 3,000 rows in one window: a range a column
                        Assert.assertEquals(f * frame.frameBytes(), ff.requestedBytes());
                        Assert.assertEquals(2 * f, ff.requestCount());
                    }
                    // the facade returns the length of each range it reads
                    Assert.assertEquals(ff.requestedBytes(), readAhead.getTouchSink());
                    Assert.assertEquals(0, ff.residencyChecks);
                    Assert.assertEquals(0, ff.faultReads);
                }
            }
        });
    }

    @Test
    public void testTouchPathCoversTheFrameBytesOnly() throws Exception {
        // Without MADV_WILLNEED the first slice reads the first window, which holds the whole
        // 500-row frame: exactly the frame's rows of every column, aux and data alike, and
        // nothing of a column top, as MADV_WILLNEED asks for.
        TestUtils.assertMemoryLeak(() -> {
            final int rowCount = 1_000;
            final int lo = 200;
            final int hi = 700;
            final int[] types = {
                    ColumnType.LONG, ColumnType.STRING, ColumnType.VARCHAR, ColumnType.VARCHAR,
                    ColumnType.INT, ColumnType.VARCHAR, ColumnType.DOUBLE, ColumnType.STRING
            };
            try (
                    MemoryCARWImpl fixedLong = new MemoryCARWImpl(4_096, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                    MemoryCARWImpl fixedInt = new MemoryCARWImpl(4_096, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                    MemoryCARWImpl strAux = new MemoryCARWImpl(4_096, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                    MemoryCARWImpl strData = new MemoryCARWImpl(4_096, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                    MemoryCARWImpl vcAux = new MemoryCARWImpl(4_096, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                    MemoryCARWImpl vcData = new MemoryCARWImpl(4_096, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                    MemoryCARWImpl inlineAux = new MemoryCARWImpl(4_096, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                    MemoryCARWImpl inlineData = new MemoryCARWImpl(4_096, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)
            ) {
                for (int r = 0; r < rowCount; r++) {
                    fixedLong.putLong(r);
                    fixedInt.putInt(r);
                }
                StringTypeDriver.INSTANCE.configureAuxMemO3RSS(strAux);
                for (int r = 0; r < rowCount; r++) {
                    if (r % 7 == 0) {
                        StringTypeDriver.INSTANCE.appendNull(strAux, strData);
                    } else {
                        StringTypeDriver.appendValue(strAux, strData, "string value " + r);
                    }
                    VarcharTypeDriver.appendValue(
                            vcAux,
                            vcData,
                            r % 5 == 0 ? null : new Utf8String(r % 3 == 0 ? "short" : "a varchar too long to inline " + r)
                    );
                    // short enough to sit inline in the aux vector, so the data vector stays empty
                    VarcharTypeDriver.appendValue(inlineAux, inlineData, new Utf8String("v" + (r % 10)));
                }
                Assert.assertEquals(0, inlineData.getAppendOffset());

                // rows [lo, hi) addressed the way the backward page-frame cursor publishes them
                final AddressedFrame frame = new AddressedFrame(types.length, hi - lo);
                frame.set(0, fixedLong.addressOf(8L * lo), 8L * (hi - lo), 0, 0);
                setVarSizeColumn(frame, 1, StringTypeDriver.INSTANCE, strAux, strData, lo, hi);
                setVarSizeColumn(frame, 2, VarcharTypeDriver.INSTANCE, vcAux, vcData, lo, hi);
                setVarSizeColumn(frame, 3, VarcharTypeDriver.INSTANCE, inlineAux, inlineData, lo, hi);
                frame.set(4, fixedInt.addressOf(4L * lo), 4L * (hi - lo), 0, 0);
                // column tops: no addresses, only the size hint a top frame publishes
                frame.set(5, 0, hi - lo, 0, 0);
                frame.set(6, 0, 8L * (hi - lo), 0, 0);
                frame.set(7, 0, hi - lo, 0, 0);

                final long strAuxBytes = 8L * (hi - lo);
                final long strDataBytes = StringTypeDriver.INSTANCE.getDataVectorSizeAt(strAux.addressOf(0), hi - 1)
                        - StringTypeDriver.INSTANCE.getDataVectorOffset(strAux.addressOf(0), lo);
                final long vcAuxBytes = 16L * (hi - lo);
                final long vcDataBytes = VarcharTypeDriver.INSTANCE.getDataVectorSizeAt(vcAux.addressOf(0), hi - 1)
                        - VarcharTypeDriver.INSTANCE.getDataVectorOffset(vcAux.addressOf(0), lo);
                Assert.assertTrue(strDataBytes > 0 && strDataBytes < strData.getAppendOffset());
                Assert.assertTrue(vcDataBytes > 0 && vcDataBytes < vcData.getAppendOffset());
                final long frameBytes = 8L * (hi - lo) + strAuxBytes + strDataBytes + vcAuxBytes + vcDataBytes + 16L * (hi - lo) + 4L * (hi - lo);

                final RecordingFacade ff = new RecordingFacade(false);
                final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
                readAhead.of(metadata(types));
                readAhead.ofFrame(frame, hi - lo);
                // the read asks the OS for nothing, so it counts no byte as read ahead
                Assert.assertEquals(0, readAhead.advise(hi - lo - SLICE_ROWS / 4, 0));
                Assert.assertEquals(frameBytes, ff.requestedBytes());
                ff.assertWithinExtents("read", frame);
                // once per frame
                Assert.assertEquals(0, readAhead.advise(0, 0));
                Assert.assertEquals(frameBytes, ff.requestedBytes());

                // the same frame through MADV_WILLNEED asks for the same ranges, each once
                final RecordingFacade willNeed = new RecordingFacade(true);
                final QwpBackwardReadAhead windows = new QwpBackwardReadAhead(willNeed);
                windows.of(metadata(types));
                windows.ofFrame(frame, hi - lo);
                Assert.assertEquals(frameBytes, windows.advise(hi - lo - SLICE_ROWS / 4, 0));
                Assert.assertEquals(frameBytes, willNeed.requestedBytes());
                Assert.assertEquals(willNeed.requestCount(), ff.requestCount());

                frame.format = PartitionFormat.PARQUET;
                readAhead.ofFrame(frame, hi - lo);
                Assert.assertEquals(0, readAhead.advise(0, 0));
                Assert.assertEquals(frameBytes, ff.requestedBytes());
            }
        });
    }

    @Test
    public void testTouchPathFirstBatchReadsItsOwnRowsOnly() throws Exception {
        // Without MADV_WILLNEED a read waits for storage, so the first batch reads no row below the
        // lowest one it takes, but at least a slice of rows. Rows of 1,124 bytes, a LONG, a 16-byte
        // aux entry and 1,100 bytes of data, fit 14,926 to a window: the first batch of 16,384 rows
        // reads a window and the 1,458 rows it leaves, not a second window. The batch after it
        // reads whole windows, which span batches. Rows of 1,074 bytes fit 15,621 to a window, and
        // the 763 rows the batch leaves are fewer than a slice, so it reads 1,024 rows.
        TestUtils.assertMemoryLeak(() -> {
            final long rows = 40_000;
            final long batchRows = QwpBackwardReadAhead.INITIAL_WINDOW_ROWS;
            final long batchLo = rows - batchRows;
            for (int valueBytes : new int[]{1_100, 1_050}) {
                final long fitRows = QwpBackwardReadAhead.MAX_WINDOW_BYTES / (8 + 16 + valueBytes);
                Assert.assertEquals(valueBytes == 1_100 ? 14_926 : 15_621, fitRows);
                try (SyntheticFrame frame = new SyntheticFrame(new int[]{ColumnType.LONG, ColumnType.VARCHAR}, rows)) {
                    frame.values(1, 0, r -> valueBytes);
                    final RecordingFacade ff = new RecordingFacade(false);
                    final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
                    readAhead.of(frame.metadata());
                    final long firstBatchBytes = walkStream(readAhead, ff, frame, rows, 1, batchRows);
                    final long firstBatchLo = valueBytes == 1_100 ? batchLo : rows - fitRows - SLICE_ROWS;
                    Assert.assertEquals("values of " + valueBytes, frame.bytesOfRows(firstBatchLo, rows), firstBatchBytes);
                    frame.assertTiled("values of " + valueBytes, ff);
                    ff.assertWithinExtents("values of " + valueBytes, frame);
                }
            }

            // the batch after the first reads a whole window below it, cut by nothing but the byte cap
            try (SyntheticFrame frame = new SyntheticFrame(new int[]{ColumnType.LONG, ColumnType.VARCHAR}, rows)) {
                frame.values(1, 0, r -> 1_100);
                final RecordingFacade ff = new RecordingFacade(false);
                final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
                readAhead.of(frame.metadata());
                readAhead.ofFrame(frame, rows);
                for (long emitted = 0; emitted < batchRows; emitted += SLICE_ROWS) {
                    Assert.assertEquals(0, readAhead.advise(rows - emitted - SLICE_ROWS, batchLo));
                }
                Assert.assertEquals(frame.bytesOfRows(batchLo, rows), ff.requestedBytes());
                Assert.assertEquals(0, readAhead.advise(batchLo - SLICE_ROWS, 0));
                Assert.assertEquals(frame.bytesOfRows(batchLo - 14_926, rows), ff.requestedBytes());
            }

            // MADV_WILLNEED advises a window ahead of the walk, whatever the batch takes
            try (SyntheticFrame frame = new SyntheticFrame(new int[]{ColumnType.LONG, ColumnType.VARCHAR}, rows)) {
                frame.values(1, 0, r -> 1_100);
                final RecordingFacade toBatch = new RecordingFacade(true);
                final RecordingFacade ignoringBatch = new RecordingFacade(true);
                final QwpBackwardReadAhead toBatchAhead = new QwpBackwardReadAhead(toBatch);
                final QwpBackwardReadAhead ignoringBatchAhead = new QwpBackwardReadAhead(ignoringBatch);
                toBatchAhead.of(frame.metadata());
                ignoringBatchAhead.of(frame.metadata());
                toBatchAhead.ofFrame(frame, rows);
                ignoringBatchAhead.ofFrame(frame, rows);
                for (long emitted = 0; emitted < batchRows; emitted += SLICE_ROWS) {
                    Assert.assertEquals(
                            ignoringBatchAhead.advise(rows - emitted - SLICE_ROWS, 0),
                            toBatchAhead.advise(rows - emitted - SLICE_ROWS, batchLo)
                    );
                }
                Assert.assertEquals(ignoringBatch.rangesToString(), toBatch.rangesToString());
                // the advice of the first batch reaches past it
                Assert.assertTrue(toBatch.lowestWithin(frame.getPageAddress(0), frame.getPageAddress(0) + 8 * rows) < frame.getPageAddress(0) + 8 * batchLo);
            }
        });
    }

    @Test
    public void testTouchPathFirstBatchSpanningFramesReadsItsOwnRowsOnly() throws Exception {
        // A first batch of 16,384 rows over frames of 3,000 rows takes five frames whole and the top
        // 1,384 rows of the sixth, and reads no other row of the sixth; the next batch reads the rest.
        TestUtils.assertMemoryLeak(() -> {
            try (SyntheticFrame frame = new SyntheticFrame(new int[]{ColumnType.LONG, ColumnType.VARCHAR}, 3_000)) {
                frame.values(1, 0, r -> 1_100);
                final RecordingFacade ff = new RecordingFacade(false);
                final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
                readAhead.of(frame.metadata());
                final long firstBatchBytes = walkStream(readAhead, ff, frame, 3_000, 7, QwpBackwardReadAhead.INITIAL_WINDOW_ROWS);
                Assert.assertEquals(5 * frame.bytesOfRows(0, 3_000) + frame.bytesOfRows(1_616, 3_000), firstBatchBytes);
                // the frames of the stream, the same memory seven times, each read once
                Assert.assertEquals(7 * frame.bytesOfRows(0, 3_000), ff.requestedBytes());
            }
        });
    }

    @Test
    public void testTouchPathReadsEachWindowWhenTheWalkReachesIt() throws Exception {
        // Without MADV_WILLNEED a read waits for storage, so the stream reads a window only when
        // the slice it appends reaches below the rows it has read. It reads the windows that
        // MADV_WILLNEED asks for: the first holds the first batch, the next ones double up to
        // the byte cap. Down 200,000 rows of a LONG and a DOUBLE they hold 16,384, 32,768 and
        // 65,536 rows, then the last 85,312, and down rows of 320 bytes the cap stops them at
        // 52,428 rows. A first batch of 1,000 or 7 rows reads 1,024, a slice, and the windows
        // after it double from the 16,384 rows the stream planned to read.
        TestUtils.assertMemoryLeak(() -> {
            final int[] wide = new int[40];
            Arrays.fill(wide, ColumnType.LONG);
            for (int[] types : new int[][]{{ColumnType.LONG, ColumnType.DOUBLE}, wide}) {
                for (int batchRows : new int[]{(int) QwpBackwardReadAhead.INITIAL_WINDOW_ROWS, 1_000, 7}) {
                    try (SyntheticFrame frame = new SyntheticFrame(types, 200_000)) {
                        final String context = types.length + " columns, batches of " + batchRows + " rows";
                        final LongList windows = assertWindowsHoldEveryByte(context, frame, batchRows, false);
                        final String expected;
                        if (batchRows == QwpBackwardReadAhead.INITIAL_WINDOW_ROWS) {
                            expected = types.length == 2 ? "[16384,32768,65536,85312]" : "[16384,32768,52428,52428,45992]";
                        } else {
                            expected = types.length == 2 ? "[1024,32768,65536,100672]" : "[1024,32768,52428,52428,52428,8924]";
                        }
                        Assert.assertEquals(context, expected, windows.toString());
                    }
                }
            }
        });
    }

    @Test
    public void testTouchPathReadsASliceForASmallFirstBatch() throws Exception {
        // A first batch of fewer rows than a slice reads a slice of rows, not the first window, of
        // 16,384 rows: the batch takes 1,000 rows, the stream reads 1,024. The next batch reads the
        // window after it, twice the one the first batch did not read whole.
        TestUtils.assertMemoryLeak(() -> {
            final int rows = 200_000;
            try (TestFrame frame = new TestFrame(new int[]{ColumnType.LONG, ColumnType.DOUBLE}, false, 0, rows)) {
                final RecordingFacade ff = new RecordingFacade(false);
                final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
                readAhead.of(frame.metadata());
                readAhead.ofFrame(frame, rows);
                Assert.assertEquals(0, readAhead.advise(rows - 1_000, rows - 1_000));
                Assert.assertEquals(frame.bytesOfRows(rows - SLICE_ROWS, rows), ff.requestedBytes());
                Assert.assertEquals(0, readAhead.advise(rows - 2_000, 0));
                final long secondWindowLo = rows - SLICE_ROWS - 2 * QwpBackwardReadAhead.INITIAL_WINDOW_ROWS;
                Assert.assertEquals(frame.bytesOfRows(secondWindowLo, rows), ff.requestedBytes());
                assertEachColumnRead(frame, ff, secondWindowLo);
            }
        });
    }

    @Test
    public void testTouchPathReadsNothingOutsideThePublishedExtents() throws Exception {
        // Extents that describe no bytes read nothing, so a frame source that publishes an
        // empty extent or an aux entry pointing outside the data extent cannot steer the
        // read-ahead out of bounds, on either path.
        TestUtils.assertMemoryLeak(() -> {
            final int[] types = {ColumnType.VARCHAR, ColumnType.STRING, ColumnType.STRING, ColumnType.LONG};
            try (
                    MemoryCARWImpl vcAux = new MemoryCARWImpl(4_096, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                    MemoryCARWImpl vcData = new MemoryCARWImpl(4_096, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                    MemoryCARWImpl strAux = new MemoryCARWImpl(4_096, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                    MemoryCARWImpl strData = new MemoryCARWImpl(4_096, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)
            ) {
                // 100 values too long to inline, then 100 that inline: a frame over the second
                // hundred has a data extent that ends where its first row's data would start
                for (int r = 0; r < 200; r++) {
                    VarcharTypeDriver.appendValue(vcAux, vcData, new Utf8String(r < 100 ? "a varchar too long to inline " + r : "v" + (r % 10)));
                }
                StringTypeDriver.INSTANCE.configureAuxMemO3RSS(strAux);
                for (int r = 0; r < 10; r++) {
                    StringTypeDriver.appendValue(strAux, strData, "s" + r);
                }
                // an aux entry whose data offset is negative
                Unsafe.putLong(strAux.addressOf(8L * 5), -8);

                final AddressedFrame frame = new AddressedFrame(types.length, 100);
                setVarSizeColumn(frame, 0, VarcharTypeDriver.INSTANCE, vcAux, vcData, 100, 200);
                Assert.assertEquals(
                        VarcharTypeDriver.INSTANCE.getDataVectorOffset(vcAux.addressOf(0), 100),
                        frame.getPageSize(0)
                );
                // aux entries of rows 5 to 9 only, the first one pointing below the data extent
                frame.set(1, strData.addressOf(0), strData.getAppendOffset(), strAux.addressOf(8L * 5), 8L * 5);
                // an aux address with an empty aux extent
                frame.set(2, strData.addressOf(0), strData.getAppendOffset(), strAux.addressOf(0), 0);
                // a fixed-size address with an empty extent
                frame.set(3, strData.addressOf(0), 0, 0, 0);

                final RecordingFacade ff = new RecordingFacade(false);
                final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
                readAhead.of(metadata(types));
                readAhead.ofFrame(frame, 100);
                // only the aux entries of columns 0 and 1, none of them counted as read ahead
                Assert.assertEquals(0, readAhead.advise(0, 0));
                Assert.assertEquals(16L * 100 + 8L * 5, ff.requestedBytes());
                Assert.assertEquals(2, ff.requestCount());
                Assert.assertTrue(ff.contains(vcAux.addressOf(16L * 100), vcAux.addressOf(16L * 200)));
                Assert.assertTrue(ff.contains(strAux.addressOf(8L * 5), strAux.addressOf(8L * 10)));

                // MADV_WILLNEED asks for the same aux entries, and for no data: the varchar
                // data extent is empty, and the string aux entries stop before the window's
                // bottom row
                final RecordingFacade willNeed = new RecordingFacade(true);
                final QwpBackwardReadAhead windows = new QwpBackwardReadAhead(willNeed);
                windows.of(metadata(types));
                windows.ofFrame(frame, 100);
                Assert.assertEquals(16L * 100 + 8L * 5, windows.advise(0, 0));
                Assert.assertEquals(2, willNeed.requestCount());
                Assert.assertTrue(willNeed.contains(vcAux.addressOf(16L * 100), vcAux.addressOf(16L * 200)));
                Assert.assertTrue(willNeed.contains(strAux.addressOf(8L * 5), strAux.addressOf(8L * 10)));
            }
        });
    }

    @Test
    public void testTouchPathReadsTheFirstBatchOnly() throws Exception {
        // Without MADV_WILLNEED a read waits for storage, so the first batch waits for its own
        // rows only: the slices of the first batch of a 200,000-row frame read its top 16,384 rows
        // of every column, values, aux entries and data, at the first slice, and the first slice
        // of the next batch reads the next window, of 32,768 rows.
        TestUtils.assertMemoryLeak(() -> {
            final int rows = 200_000;
            final long batchRows = QwpBackwardReadAhead.INITIAL_WINDOW_ROWS;
            try (TestFrame frame = new TestFrame(new int[]{ColumnType.LONG, ColumnType.DOUBLE}, true, 0, rows)) {
                final RecordingFacade ff = new RecordingFacade(false);
                final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
                readAhead.of(frame.metadata());
                readAhead.ofFrame(frame, rows);
                for (long emitted = 0; emitted < batchRows; emitted += SLICE_ROWS) {
                    Assert.assertEquals(0, readAhead.advise(rows - emitted - SLICE_ROWS, 0));
                    Assert.assertEquals("after " + emitted + " rows", frame.bytesOfRows(rows - batchRows, rows), ff.requestedBytes());
                }
                // a range a column for the window's rows, values or aux entries, and one for the
                // data of a var-size column that stores any; none for the column top
                int ranges = 0;
                for (int ci = 0; ci < frame.getColumnCount(); ci++) {
                    final long[] range = frame.rowRange(ci, rows - batchRows, rows);
                    if (range[1] > range[0]) {
                        Assert.assertTrue("column " + ci, ff.contains(range[0], range[1]));
                        ranges++;
                    }
                    if (frame.isVarSize(ci)) {
                        final long[] dataRange = frame.dataRange(ci, rows - batchRows, rows);
                        if (dataRange[1] > dataRange[0]) {
                            Assert.assertTrue("data of column " + ci, ff.contains(dataRange[0], dataRange[1]));
                            ranges++;
                        }
                    }
                }
                // LONG, DOUBLE, STRING and VARCHAR aux and data, the inline VARCHAR's aux
                Assert.assertEquals(7, ranges);
                Assert.assertEquals(ranges, ff.requestCount());

                Assert.assertEquals(0, readAhead.advise(rows - batchRows - SLICE_ROWS, 0));
                Assert.assertEquals(frame.bytesOfRows(rows - 3 * batchRows, rows), ff.requestedBytes());
                Assert.assertEquals(2 * ranges, ff.requestCount());
            }
        });
    }

    @Test
    public void testTrustEndsAtAMajorFault() throws Exception {
        // A major fault of the thread since the stream started trusting, in the walk of a
        // trusted frame or in the read of the next one's first byte, ends the trust: the stream
        // checks that next frame, and trusts again only after twice the streak it needed.
        TestUtils.assertMemoryLeak(() -> {
            try (TestFrame frame = smallFrame()) {
                final RecordingFacade ff = new RecordingFacade(true);
                ff.isResident = true;
                final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
                readAhead.of(frame.metadata());
                walkFirstFrame(readAhead, frame);
                int checks = 0;
                int streak = TRUST_STREAK;
                for (int round = 0; round < 3; round++) {
                    for (int f = 0; f < streak; f++) {
                        Assert.assertEquals(0, walk(readAhead, frame));
                        Assert.assertEquals(checks += 2, ff.residencyChecks);
                    }
                    // trusted
                    final int faultReads = ff.faultReads;
                    Assert.assertEquals(0, walk(readAhead, frame));
                    Assert.assertEquals(checks, ff.residencyChecks);
                    Assert.assertEquals(faultReads + 1, ff.faultReads);
                    // the walk of that frame read from storage, and the next frame is not in memory
                    ff.majorFaults++;
                    ff.isResident = false;
                    Assert.assertEquals(frame.frameBytes(), walk(readAhead, frame));
                    Assert.assertEquals(checks += 2, ff.residencyChecks);
                    Assert.assertEquals(faultReads + 2, ff.faultReads);
                    // the frame after a cold check goes out unchecked
                    ff.isResident = true;
                    Assert.assertEquals(frame.frameBytes(), walk(readAhead, frame));
                    Assert.assertEquals(checks, ff.residencyChecks);
                    streak *= 2;
                }
            }
        });
    }

    @Test
    public void testTrustEndsWhenTheFiberMovesCarrier() throws Exception {
        // A query fiber that yields between two frames can resume on another carrier, the OS
        // thread whose major faults the stream reads: the stream checks the next frame, and
        // trusts after the usual streak. A fiber that resumes on the carrier it yielded on keeps
        // the trust. Both carriers stay bound throughout, as the workers of a pool do.
        TestUtils.assertMemoryLeak(() -> {
            final ExecutorService first = newCarrier();
            final ExecutorService second = newCarrier();
            final FiberRuntime runtime = new FiberRuntime(1, 1);
            try (TestFrame frame = smallFrame()) {
                final RecordingFacade ff = new RecordingFacade(true);
                ff.isResident = true;
                final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
                readAhead.of(frame.metadata());
                // the carrier of each stretch the fiber runs between two yields
                final IntList stretchCarriers = new IntList();
                final AtomicReference<Throwable> failure = new AtomicReference<>();
                final FiberTask stream = new FiberTask() {
                    @Override
                    protected void onError(Throwable th) {
                        failure.set(th);
                    }

                    @Override
                    protected boolean runStep() {
                        stretchCarriers.add(CarrierIdentity.current());
                        walkFirstFrame(readAhead, frame);
                        for (int f = 0; f < TRUST_STREAK; f++) {
                            Assert.assertEquals(0, walk(readAhead, frame));
                        }
                        // trusted on the first carrier
                        Assert.assertEquals(0, walk(readAhead, frame));
                        Assert.assertEquals(2 * TRUST_STREAK, ff.residencyChecks);
                        Assert.assertEquals(2, ff.faultReads);

                        yieldAndRecordCarrier(stretchCarriers);
                        // back on the first carrier: still trusted
                        Assert.assertEquals(0, walk(readAhead, frame));
                        Assert.assertEquals(2 * TRUST_STREAK, ff.residencyChecks);
                        Assert.assertEquals(3, ff.faultReads);

                        yieldAndRecordCarrier(stretchCarriers);
                        // on the second carrier, whose faults say nothing about the frames before
                        for (int f = 0; f < TRUST_STREAK; f++) {
                            Assert.assertEquals(0, walk(readAhead, frame));
                            Assert.assertEquals(2 * TRUST_STREAK + 2 * (f + 1), ff.residencyChecks);
                        }
                        Assert.assertEquals(4, ff.faultReads);
                        // trusted on the second carrier, after a streak no longer than the first
                        Assert.assertEquals(0, walk(readAhead, frame));
                        Assert.assertEquals(4 * TRUST_STREAK, ff.residencyChecks);
                        Assert.assertEquals(5, ff.faultReads);

                        yieldAndRecordCarrier(stretchCarriers);
                        // on the first carrier again
                        Assert.assertEquals(0, walk(readAhead, frame));
                        Assert.assertEquals(4 * TRUST_STREAK + 2, ff.residencyChecks);
                        Assert.assertEquals(5, ff.faultReads);
                        return true;
                    }
                };
                Assert.assertEquals(LaunchResult.LAUNCHED, runOn(first, () -> runtime.launch(stream)));
                // each drain runs the fiber from where it yielded last to its next yield
                for (ExecutorService carrier : new ExecutorService[]{first, first, second, first}) {
                    final int drained = runOn(carrier, () -> runtime.drain(1));
                    if (failure.get() != null) {
                        throw new AssertionError(failure.get());
                    }
                    Assert.assertEquals(1, drained);
                }
                Assert.assertTrue(stream.isDone());
                final int firstId = runOn(first, CarrierIdentity::current);
                final int secondId = runOn(second, CarrierIdentity::current);
                Assert.assertNotEquals(firstId, secondId);
                Assert.assertEquals("[" + firstId + ',' + firstId + ',' + secondId + ',' + firstId + ']', stretchCarriers.toString());
            } finally {
                try {
                    close(runtime);
                } finally {
                    stop(first);
                    stop(second);
                }
            }
        });
    }

    @Test
    public void testTrustEndsWhenTheStreamMovesThread() throws Exception {
        // A parked stream can resume on another worker thread, whose major faults say nothing
        // about the frames before: the stream checks again, and trusts after the usual streak.
        TestUtils.assertMemoryLeak(() -> {
            try (TestFrame frame = smallFrame()) {
                final RecordingFacade ff = new RecordingFacade(true);
                ff.isResident = true;
                final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
                readAhead.of(frame.metadata());
                walkFirstFrame(readAhead, frame);
                for (int f = 0; f < TRUST_STREAK; f++) {
                    walk(readAhead, frame);
                }
                // trusted on this thread
                Assert.assertEquals(0, walk(readAhead, frame));
                Assert.assertEquals(2 * TRUST_STREAK, ff.residencyChecks);
                Assert.assertEquals(2, ff.faultReads);
                onAnotherThread(() -> {
                    for (int f = 0; f < TRUST_STREAK; f++) {
                        Assert.assertEquals(0, walk(readAhead, frame));
                        Assert.assertEquals(2 * TRUST_STREAK + 2 * (f + 1), ff.residencyChecks);
                    }
                    // trusted on the other thread, after a streak no longer than the first
                    Assert.assertEquals(0, walk(readAhead, frame));
                    Assert.assertEquals(4 * TRUST_STREAK, ff.residencyChecks);
                });
                // the faults of the thread the stream trusted are not this thread's
                Assert.assertEquals(4, ff.faultReads);
                Assert.assertEquals(0, walk(readAhead, frame));
                Assert.assertEquals(4 * TRUST_STREAK + 2, ff.residencyChecks);
                Assert.assertEquals(4, ff.faultReads);
            }
        });
    }

    @Test
    public void testTrustedFrameReadsTheByteItsWalkReadsFirst() throws Exception {
        // Before each trusted frame the stream reads the last byte of the first range the
        // frame publishes, which its walk down reads first: of a fixed-size column the top
        // value, of a var-size column the top aux entry. Column tops and empty ranges publish
        // nothing to read.
        TestUtils.assertMemoryLeak(() -> {
            try (
                    MemoryCARWImpl longs = new MemoryCARWImpl(4_096, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                    MemoryCARWImpl vcAux = new MemoryCARWImpl(4_096, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                    MemoryCARWImpl vcData = new MemoryCARWImpl(4_096, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)
            ) {
                final int rows = 1_000;
                for (int r = 0; r < rows; r++) {
                    longs.putLong(r);
                    VarcharTypeDriver.appendValue(vcAux, vcData, new Utf8String("v" + (r % 10)));
                }
                Unsafe.putByte(longs.addressOf(8L * rows - 1), (byte) 7);
                final GenericRecordMetadata longFirst = metadata(new int[]{ColumnType.LONG, ColumnType.VARCHAR});
                final GenericRecordMetadata varcharFirst = metadata(new int[]{ColumnType.VARCHAR, ColumnType.LONG});

                final AddressedFrame frame = new AddressedFrame(2, rows);
                frame.set(0, longs.addressOf(0), 8L * rows, 0, 0);
                setVarSizeColumn(frame, 1, VarcharTypeDriver.INSTANCE, vcAux, vcData, 0, rows);
                Unsafe.putByte(frame.getAuxPageAddress(1) + frame.getAuxPageSize(1) - 1, (byte) 5);
                Assert.assertEquals(7, trustedFrameTouch(frame, longFirst));
                // a column top, or a published address with no bytes, publishes no range
                frame.set(0, 0, 8L * rows, 0, 0);
                Assert.assertEquals(5, trustedFrameTouch(frame, longFirst));
                frame.set(0, longs.addressOf(0), 0, 0, 0);
                Assert.assertEquals(5, trustedFrameTouch(frame, longFirst));

                setVarSizeColumn(frame, 0, VarcharTypeDriver.INSTANCE, vcAux, vcData, 0, rows);
                frame.set(1, longs.addressOf(0), 8L * rows, 0, 0);
                Assert.assertEquals(5, trustedFrameTouch(frame, varcharFirst));
                frame.set(0, 0, 0, 0, 0);
                Assert.assertEquals(7, trustedFrameTouch(frame, varcharFirst));
                frame.set(0, 0, 0, vcAux.addressOf(0), 0);
                Assert.assertEquals(7, trustedFrameTouch(frame, varcharFirst));
            }
        });
    }

    @Test
    public void testWindowFitStaysInsideHostileExtents() throws Exception {
        // Fitting windows to the byte cap reads aux entries that a damaged or hostile frame source
        // can point anywhere. They cannot steer the windows out of the frame: the walk asks for,
        // or reads, nothing outside the published extents, and every value and aux entry once.
        TestUtils.assertMemoryLeak(() -> {
            final long rows = 30_000;
            final int array = ColumnType.encodeArrayType(ColumnType.DOUBLE, 1);
            final int[] types = {ColumnType.LONG, ColumnType.VARCHAR, ColumnType.STRING, array, ColumnType.VARCHAR, ColumnType.BINARY};
            for (boolean isWillNeed : new boolean[]{true, false}) {
                try (SyntheticFrame frame = new SyntheticFrame(types, rows)) {
                    final long dataSize = rows * 3_000;
                    // every 1,000th entry points past the data extent
                    frame.offsets(1, dataSize, r -> r % 1_000 == 999 ? dataSize + 1_000_000 : r * 3_000);
                    // offsets that fall: each row's data would end before it starts
                    frame.offsets(2, dataSize, r -> (rows - r) * 3_000);
                    // offsets that jump back and forth
                    frame.offsets(3, dataSize, r -> (r * 7_919) % rows * 3_000);
                    // data without aux entries to tell its rows apart
                    frame.dataWithoutAux(4, dataSize, false);
                    frame.dataWithoutAux(5, dataSize, true);
                    final RecordingFacade ff = new RecordingFacade(isWillNeed);
                    final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
                    readAhead.of(frame.metadata());
                    readAhead.ofFrame(frame, rows);
                    for (long emitted = 0; emitted < rows; emitted += SLICE_ROWS) {
                        advise(readAhead, ff, Math.max(0, rows - emitted - SLICE_ROWS));
                    }
                    ff.assertWithinExtents("hostile aux entries", frame);
                    // values and aux entries: the data of these columns means nothing
                    frame.assertTiled("hostile aux entries", ff);
                    // data whose rows the frame does not tell apart is not asked for
                    Assert.assertEquals(0, ff.coveredWithin(frame.getPageAddress(4), frame.getPageAddress(4) + dataSize));
                    Assert.assertEquals(0, ff.coveredWithin(frame.getPageAddress(5), frame.getPageAddress(5) + dataSize));
                }
            }
        });
    }

    @Test
    public void testWindowsAskForNothingOutsideThePublishedExtents() throws Exception {
        // A frame source that publishes fewer bytes than the frame's rows take, or aux entries
        // that point outside the data extent, cannot steer the windows out of bounds: down a
        // 40,000-row frame they ask for, or read, the published bytes the rows cover and nothing
        // else, and read no aux entry outside the published aux extent.
        TestUtils.assertMemoryLeak(() -> {
            for (boolean isWillNeed : new boolean[]{true, false}) {
                assertWindowsStayInsideShortExtents(isWillNeed);
            }
        });
    }

    @Test
    public void testWindowsCoverEveryRowOnceAheadOfTheWalk() throws Exception {
        // Walk a frame down in slices as the egress does: before each slice, every row of it
        // has been asked for, or read; the first window holds the first batch, each next one is
        // at most twice the last; and at the end every byte of every column was asked for, or
        // read, once.
        TestUtils.assertMemoryLeak(() -> {
            final int rows = 200_000;
            final int[] fixedTypes = {
                    ColumnType.LONG, ColumnType.TIMESTAMP, ColumnType.DOUBLE, ColumnType.INT, ColumnType.FLOAT,
                    ColumnType.IPv4, ColumnType.SHORT, ColumnType.CHAR, ColumnType.BYTE, ColumnType.BOOLEAN,
                    ColumnType.SYMBOL, ColumnType.UUID, ColumnType.LONG256, ColumnType.getGeoHashTypeWithBits(40),
                    ColumnType.getDecimalType(30, 6)
            };
            for (boolean isWillNeed : new boolean[]{true, false}) {
                try (TestFrame frame = new TestFrame(fixedTypes, true, 1_000, 1_000 + rows)) {
                    final RecordingFacade ff = new RecordingFacade(isWillNeed);
                    final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
                    readAhead.of(frame.metadata());
                    readAhead.ofFrame(frame, rows);

                    // the LONG column, column 0, tells how far down the advice reaches
                    final long longColumn = frame.getPageAddress(0);
                    long advisedLo = rows;
                    long previousWindow = 0;
                    int windows = 0;
                    long firstBatchBytes = 0;
                    for (long emitted = 0; emitted < rows; ) {
                        final long sliceRows = Math.min(SLICE_ROWS, rows - emitted);
                        final long lo = rows - emitted - sliceRows;
                        final long before = ff.requestedBytes();
                        final long bytes = readAhead.advise(lo, 0);
                        // only what the stream asks the OS for counts
                        Assert.assertEquals(isWillNeed ? ff.requestedBytes() - before : 0, bytes);
                        final long lowest = (ff.lowestWithin(longColumn, longColumn + 8L * rows) - longColumn) / 8;
                        if (lowest < advisedLo) {
                            final long window = advisedLo - lowest;
                            if (windows == 0) {
                                Assert.assertEquals(QwpBackwardReadAhead.INITIAL_WINDOW_ROWS, window);
                            } else if (lowest > 0) {
                                // the last window may stop at the bottom of the frame
                                Assert.assertTrue("window " + windows + ": " + window, window <= 2 * previousWindow);
                            }
                            previousWindow = window;
                            advisedLo = lowest;
                            windows++;
                        }
                        // the slice about to be appended is advised
                        Assert.assertTrue("lo=" + lo + " advisedLo=" + advisedLo, advisedLo <= lo);
                        emitted += sliceRows;
                        if (emitted == QwpBackwardReadAhead.INITIAL_WINDOW_ROWS) {
                            firstBatchBytes = ff.requestedBytes();
                        }
                    }
                    Assert.assertEquals(0, advisedLo);
                    // 16,384 rows, then doubling up to the 16 MiB cap: a handful of windows
                    Assert.assertTrue("windows=" + windows, windows <= 10);
                    // By the end of the first batch the stream has asked for its own rows and, from
                    // the middle of them on, for the next window of twice as many: the top 49,152
                    // rows, not the frame. A stream that reads its windows itself waits for each
                    // read, so it has read the rows of the batch only.
                    final long firstBatchRows = (isWillNeed ? 3 : 1) * QwpBackwardReadAhead.INITIAL_WINDOW_ROWS;
                    Assert.assertEquals(frame.bytesOfRows(rows - firstBatchRows, rows), firstBatchBytes);
                    assertEachColumnTiled(frame, ff);
                    Assert.assertEquals(frame.frameBytes(), ff.requestedBytes());
                }
            }
        });
    }

    @Test
    public void testWindowsHoldEveryByteOfLongValues() throws Exception {
        // Down frames whose rows take more than the byte cap, in batches of the egress's size and
        // of 7 rows, the stream asks for every byte of every column once, or reads it itself,
        // before the walk reads its row. A window takes at most the cap, unless it holds one slice
        // of rows, and at most twice the rows of the window before it.
        TestUtils.assertMemoryLeak(() -> {
            final int array = ColumnType.encodeArrayType(ColumnType.DOUBLE, 1);
            for (boolean isWillNeed : new boolean[]{true, false}) {
                for (int batchRows : new int[]{(int) QwpBackwardReadAhead.INITIAL_WINDOW_ROWS, 7}) {
                    final String batch = ", batches of " + batchRows + " rows" + (isWillNeed ? ", advised" : ", read");
                    // the first batch holds 74 MB of 4,505-byte VARCHARs, from 1,000 bytes into the data
                    try (SyntheticFrame frame = new SyntheticFrame(new int[]{ColumnType.LONG, ColumnType.VARCHAR}, 20_000)) {
                        frame.values(1, 1_000, r -> 4_505);
                        assertWindowsHoldEveryByte("4,505-byte VARCHARs" + batch, frame, batchRows, isWillNeed);
                    }
                    // one slice of 20,000-byte values takes more than the cap: windows of one slice
                    try (SyntheticFrame frame = new SyntheticFrame(new int[]{ColumnType.LONG, ColumnType.BINARY}, 5_000)) {
                        frame.values(1, 0, r -> 20_000);
                        assertWindowsHoldEveryByte("20,000-byte BINARYs" + batch, frame, batchRows, isWillNeed);
                    }
                    // short values with a burst of long ones: the windows the burst falls in fit it
                    try (SyntheticFrame frame = new SyntheticFrame(new int[]{ColumnType.LONG, ColumnType.STRING}, 100_000)) {
                        frame.values(1, 0, r -> r >= 40_000 && r < 43_000 ? 40_000 : 100);
                        assertWindowsHoldEveryByte("a burst of 40,000-byte STRINGs" + batch, frame, batchRows, isWillNeed);
                    }
                    // var-size columns that each fit the cap, but not all of them together
                    try (SyntheticFrame frame = new SyntheticFrame(new int[]{ColumnType.LONG, ColumnType.STRING, ColumnType.BINARY, ColumnType.VARCHAR, array}, 30_000)) {
                        for (int ci = 1; ci < 5; ci++) {
                            frame.values(ci, 0, r -> 900);
                        }
                        assertWindowsHoldEveryByte("four columns of 900-byte values" + batch, frame, batchRows, isWillNeed);
                    }
                    // a column top and a VARCHAR column stored inline next to long VARCHARs
                    try (SyntheticFrame frame = new SyntheticFrame(new int[]{ColumnType.LONG, ColumnType.VARCHAR, ColumnType.VARCHAR, ColumnType.VARCHAR}, 20_000)) {
                        frame.values(2, 0, r -> r % 10);
                        frame.values(3, 0, r -> 4_000);
                        assertWindowsHoldEveryByte("a column top and inline VARCHARs" + batch, frame, batchRows, isWillNeed);
                    }
                    // a frame of fewer rows than a slice whose values take more than the cap
                    try (SyntheticFrame frame = new SyntheticFrame(new int[]{ColumnType.LONG, array}, 500)) {
                        frame.values(1, 0, r -> 40_000);
                        assertWindowsHoldEveryByte("a 500-row frame of 40,000-byte arrays" + batch, frame, batchRows, isWillNeed);
                    }
                }
            }
        });
    }

    @Test
    public void testWindowsStayWithinTheByteCap() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            for (boolean isWillNeed : new boolean[]{true, false}) {
                // 40 LONG columns: 320 bytes a row caps a window at 16 MiB / 320 rows
                final int[] wide = new int[40];
                Arrays.fill(wide, ColumnType.LONG);
                final int rows = 200_000;
                try (TestFrame frame = new TestFrame(wide, false, 0, rows)) {
                    final RecordingFacade ff = new RecordingFacade(isWillNeed);
                    final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
                    readAhead.of(frame.metadata());
                    readAhead.ofFrame(frame, rows);
                    final long capRows = QwpBackwardReadAhead.MAX_WINDOW_BYTES / 320;
                    for (long emitted = 0; emitted < rows; emitted += SLICE_ROWS) {
                        final long bytes = advise(readAhead, ff, Math.max(0, rows - emitted - SLICE_ROWS));
                        Assert.assertTrue("bytes=" + bytes, bytes <= capRows * 320);
                        Assert.assertEquals(0, bytes % 320);
                    }
                    assertEachColumnTiled(frame, ff);
                }

                // Long values: 16,384 rows of 2 KiB VARCHARs hold 32 MiB. The first window asks for the
                // aux entries of those rows, which tell where their data starts, and holds the top rows
                // that fit the cap: 2,064 bytes a row with its aux entry, so 8,128 rows. The windows
                // after it hold the rest, every byte once.
                final int longRows = 20_000;
                try (
                        MemoryCARWImpl aux = new MemoryCARWImpl(1 << 20, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                        MemoryCARWImpl data = new MemoryCARWImpl(1 << 20, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)
                ) {
                    final Utf8String value = new Utf8String("x".repeat(2_048));
                    for (int r = 0; r < longRows; r++) {
                        VarcharTypeDriver.appendValue(aux, data, value);
                    }
                    final AddressedFrame frame = new AddressedFrame(1, longRows);
                    setVarSizeColumn(frame, 0, VarcharTypeDriver.INSTANCE, aux, data, 0, longRows);
                    final RecordingFacade ff = new RecordingFacade(isWillNeed);
                    final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
                    readAhead.of(metadata(new int[]{ColumnType.VARCHAR}));
                    readAhead.ofFrame(frame, longRows);

                    final long first = advise(readAhead, ff, longRows - SLICE_ROWS);
                    final long dataEnd = frame.getPageAddress(0) + frame.getPageSize(0);
                    final long fitRows = QwpBackwardReadAhead.MAX_WINDOW_BYTES / (16 + 2_048);
                    Assert.assertEquals(8_128, fitRows);
                    Assert.assertEquals(16L * QwpBackwardReadAhead.INITIAL_WINDOW_ROWS + fitRows * 2_048, first);
                    Assert.assertTrue(ff.contains(dataEnd - fitRows * 2_048, dataEnd));
                    for (long emitted = SLICE_ROWS; emitted < longRows; emitted += SLICE_ROWS) {
                        final long bytes = advise(readAhead, ff, Math.max(0, longRows - emitted - SLICE_ROWS));
                        Assert.assertTrue("bytes=" + bytes, bytes <= QwpBackwardReadAhead.MAX_WINDOW_BYTES);
                    }
                    // every byte of the data and of the aux entries was asked for, or read, once
                    ff.assertTiles("data", frame.getPageAddress(0), dataEnd);
                    ff.assertTiles("aux", frame.getAuxPageAddress(0), frame.getAuxPageAddress(0) + frame.getAuxPageSize(0));
                }
            }
        });
    }

    // Advises the slice of the current frame that starts at row lo, as the egress does, and
    // returns the bytes the stream asked for or, where it reads its windows itself, read. Only
    // what the stream asks the OS for counts as read ahead.
    private static long advise(QwpBackwardReadAhead readAhead, RecordingFacade ff, long lo) {
        final long before = ff.requestedBytes();
        final long counted = readAhead.advise(lo, 0);
        final long bytes = ff.requestedBytes() - before;
        Assert.assertEquals(ff.isWillNeedSupported ? bytes : 0, counted);
        return bytes;
    }

    // a column of the type takes bytesPerRow in its widest range: one frame of it at the limit
    // is small, one row more is not
    private static void assertVarSizeBounds(int columnType, int bytesPerRow, String value) {
        final int rowsAtLimit = (int) (QwpBackwardReadAhead.SMALL_FRAME_RANGE_BYTES / bytesPerRow);
        for (int rows : new int[]{rowsAtLimit, rowsAtLimit + 1}) {
            try (
                    MemoryCARWImpl aux = new MemoryCARWImpl(4_096, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                    MemoryCARWImpl data = new MemoryCARWImpl(4_096, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)
            ) {
                if (columnType == ColumnType.STRING) {
                    StringTypeDriver.INSTANCE.configureAuxMemO3RSS(aux);
                }
                for (int r = 0; r < rows; r++) {
                    if (columnType == ColumnType.STRING) {
                        StringTypeDriver.appendValue(aux, data, value);
                    } else {
                        VarcharTypeDriver.appendValue(aux, data, new Utf8String(value));
                    }
                }
                final AddressedFrame frame = new AddressedFrame(1, rows);
                setVarSizeColumn(frame, 0, ColumnType.getDriver(columnType), aux, data, 0, rows);
                final String context = ColumnType.nameOf(columnType) + " rows=" + rows;
                Assert.assertEquals(context, (long) rows * bytesPerRow, Math.max(frame.getPageSize(0), frame.getAuxPageSize(0)));
                Assert.assertEquals(context, rows == rowsAtLimit, isTrustedAfterStreak(frame, metadata(new int[]{columnType})));
            }
        }
    }

    // the rows [rowLo, frame.rows) of every column, and nothing else, have been read, each byte once
    private static void assertEachColumnRead(TestFrame frame, RecordingFacade ff, long rowLo) {
        long bytes = 0;
        for (int ci = 0; ci < frame.getColumnCount(); ci++) {
            final long[] range = frame.rowRange(ci, rowLo, frame.rows);
            if (range[1] > range[0]) {
                ff.assertTiles("column " + ci, range[0], range[1]);
                bytes += range[1] - range[0];
            }
        }
        Assert.assertEquals(bytes, ff.requestedBytes());
    }

    private static void assertEachColumnTiled(TestFrame frame, RecordingFacade ff) {
        for (int ci = 0; ci < frame.getColumnCount(); ci++) {
            if (frame.getPageAddress(ci) == 0 && frame.getAuxPageAddress(ci) == 0) {
                continue;
            }
            final long[] range = frame.rowRange(ci, 0, frame.rows);
            ff.assertTiles("column " + ci, range[0], range[1]);
            if (frame.isVarSize(ci)) {
                final long[] dataRange = frame.dataRange(ci, 0, frame.rows);
                ff.assertTiles("data of column " + ci, dataRange[0], dataRange[1]);
            }
        }
    }

    // Walks the frame down as the egress appends it, in batches of batchRows rows and slices of
    // at most SLICE_ROWS, over data out of memory, and returns the rows of each window, top window
    // first. Before each slice, every byte of its rows has been asked for, or read; each window
    // takes at most the byte cap, unless it holds one slice of rows, and at most twice the rows of
    // the window before it; at the end, every byte of every column has been asked for, or read,
    // once, and nothing outside the published extents. As the egress does, the walk tells the
    // read-ahead the lowest row the first batch takes. A stream that reads its windows itself
    // reads one only when the slice reaches below the rows it has read, reads no row below the
    // first batch but to hold a slice of rows, doubles the window after one the first batch cut
    // from the window it planned, and counts none of it as read ahead. Column 0 is a LONG, whose
    // ranges tell where each window starts.
    private static LongList assertWindowsHoldEveryByte(String context, SyntheticFrame frame, int batchRows, boolean isWillNeed) {
        final RecordingFacade ff = new RecordingFacade(isWillNeed);
        final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
        readAhead.of(frame.metadata());
        readAhead.ofFrame(frame, frame.rows);
        final long longs = frame.getPageAddress(0);
        final LongList windows = new LongList();
        long advisedLo = frame.rows;
        long previousWindowRows = 0;
        // the first batch cut the window before, so the window after it doubles what was planned
        boolean isPreviousCut = false;
        long batchLeft = batchRows;
        for (long emitted = 0; emitted < frame.rows; ) {
            if (batchLeft == 0) {
                batchLeft = batchRows;
            }
            final long slice = Math.min(Math.min(batchLeft, frame.rows - emitted), SLICE_ROWS);
            final long lo = frame.rows - emitted - slice;
            final long batchLo = emitted < batchRows ? Math.max(0, lo + slice - batchLeft) : 0;
            final long requested = ff.requestedBytes();
            final long bytes = readAhead.advise(lo, batchLo);
            Assert.assertEquals(context, isWillNeed ? ff.requestedBytes() - requested : 0, bytes);
            if (!isWillNeed) {
                Assert.assertEquals(
                        context + ": slice [" + lo + ", " + (lo + slice) + ") below rows read from " + advisedLo,
                        lo < advisedLo,
                        ff.requestedBytes() > requested
                );
            }
            final long windowLo = (ff.lowestWithin(longs, longs + 8 * frame.rows) - longs) / 8;
            if (windowLo < advisedLo) {
                final long windowRows = advisedLo - windowLo;
                final long windowBytes = frame.bytesOfRows(windowLo, advisedLo);
                Assert.assertTrue(
                        context + ": window [" + windowLo + ", " + advisedLo + ") takes " + windowBytes + " bytes",
                        windowBytes <= QwpBackwardReadAhead.MAX_WINDOW_BYTES || windowRows <= Math.max(SLICE_ROWS, advisedLo - lo)
                );
                // a read reaches no row below the first batch, but to hold a slice of rows
                final long cutLo = Math.min(batchLo, advisedLo - SLICE_ROWS);
                if (!isWillNeed && batchLo > 0) {
                    Assert.assertTrue(context + ": window [" + windowLo + ", " + advisedLo + ") below the first batch from " + batchLo, windowLo >= cutLo);
                }
                if (previousWindowRows > 0 && windowLo > 0 && !isPreviousCut) {
                    Assert.assertTrue(context + ": window of " + windowRows + " rows after " + previousWindowRows, windowRows <= 2 * previousWindowRows);
                }
                isPreviousCut = !isWillNeed && batchLo > 0 && windowLo == cutLo;
                windows.add(windowRows);
                previousWindowRows = windowRows;
                advisedLo = windowLo;
            }
            frame.assertCovered(context + ": slice [" + lo + ", " + (lo + slice) + ")", ff, lo, lo + slice);
            emitted += slice;
            batchLeft -= slice;
        }
        frame.assertTiled(context, ff);
        ff.assertWithinExtents(context, frame);
        return windows;
    }

    // Down a 40,000-row frame whose columns publish fewer bytes than its rows take, or aux
    // entries that point outside the data extent, the stream asks for, or reads, the published
    // bytes the rows cover and nothing else.
    private static void assertWindowsStayInsideShortExtents(boolean isWillNeed) {
        final int rows = 40_000;
        final int[] types = {ColumnType.STRING, ColumnType.LONG, ColumnType.VARCHAR, ColumnType.STRING};
        try (
                MemoryCARWImpl shortAux = new MemoryCARWImpl(4_096, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                MemoryCARWImpl shortData = new MemoryCARWImpl(4_096, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                MemoryCARWImpl longs = new MemoryCARWImpl(4_096, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                MemoryCARWImpl vcAux = new MemoryCARWImpl(1 << 20, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                MemoryCARWImpl vcData = new MemoryCARWImpl(1 << 20, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                MemoryCARWImpl strAux = new MemoryCARWImpl(1 << 20, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                MemoryCARWImpl strData = new MemoryCARWImpl(1 << 20, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)
        ) {
            StringTypeDriver.INSTANCE.configureAuxMemO3RSS(shortAux);
            for (int r = 0; r < 10; r++) {
                StringTypeDriver.appendValue(shortAux, shortData, "s" + r);
            }
            longs.jumpTo(8 * 100);
            StringTypeDriver.INSTANCE.configureAuxMemO3RSS(strAux);
            for (int r = 0; r < rows; r++) {
                VarcharTypeDriver.appendValue(vcAux, vcData, new Utf8String("a varchar too long to inline " + r));
                StringTypeDriver.appendValue(strAux, strData, "string value " + r);
            }
            final AddressedFrame frame = new AddressedFrame(types.length, rows);
            // aux entries of the first 10 rows only
            frame.set(0, shortData.addressOf(0), shortData.getAppendOffset(), shortAux.addressOf(0), 8L * 10);
            // values of the first 100 rows only
            frame.set(1, longs.addressOf(0), 8L * 100, 0, 0);
            // a data address with an empty data extent
            frame.set(2, vcData.addressOf(0), 0, vcAux.addressOf(0), 16L * rows);
            // aux entries that point past a 10-byte data extent
            frame.set(3, strData.addressOf(0), 10, strAux.addressOf(0), 8L * rows);

            final RecordingFacade ff = new RecordingFacade(isWillNeed);
            final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
            readAhead.of(metadata(types));
            readAhead.ofFrame(frame, rows);
            long bytes = 0;
            for (long emitted = 0; emitted < rows; emitted += SLICE_ROWS) {
                bytes += advise(readAhead, ff, Math.max(0, rows - emitted - SLICE_ROWS));
            }
            ff.assertWithinExtents("hostile frame", frame);
            // the short aux extent and the short LONG extent, at the bottom of the frame,
            // and every aux entry of the two columns that publish them all; no data
            Assert.assertEquals(8L * 10 + 8L * 100 + 16L * rows + 8L * rows, bytes);
            Assert.assertEquals(bytes, ff.requestedBytes());
            ff.assertTiles("varchar aux", vcAux.addressOf(0), vcAux.addressOf(16L * rows));
            ff.assertTiles("string aux", strAux.addressOf(0), strAux.addressOf(8L * rows));
            Assert.assertTrue(ff.contains(shortAux.addressOf(0), shortAux.addressOf(8L * 10)));
            Assert.assertTrue(ff.contains(longs.addressOf(0), longs.addressOf(8L * 100)));
        }

        // a frame whose only column publishes its first 100 rows: windows above them have
        // nothing to ask for, and keep the window sizes they had
        try (MemoryCARWImpl longs = new MemoryCARWImpl(4_096, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            longs.jumpTo(8 * 100);
            final AddressedFrame frame = new AddressedFrame(1, rows);
            frame.set(0, longs.addressOf(0), 8L * 100, 0, 0);
            final RecordingFacade ff = new RecordingFacade(isWillNeed);
            final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
            readAhead.of(metadata(new int[]{ColumnType.LONG}));
            readAhead.ofFrame(frame, rows);
            long bytes = 0;
            for (long emitted = 0; emitted < rows; emitted += SLICE_ROWS) {
                bytes += advise(readAhead, ff, Math.max(0, rows - emitted - SLICE_ROWS));
            }
            Assert.assertEquals(8L * 100, bytes);
            Assert.assertEquals(1, ff.requestCount());
        }
    }

    private static void close(FiberRuntime runtime) {
        runtime.beginQuiesce();
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (runtime.state() != FiberRuntimeState.CLOSED && System.nanoTime() < deadline) {
            runtime.drain(64);
        }
        Assert.assertTrue(runtime.awaitClosed(deadline));
        runtime.closeAfterDrained();
    }

    // whether, after its first frame and TRUST_STREAK frames in memory, a stream of the frame trusts the next one
    private static boolean isTrustedAfterStreak(PageFrame frame, GenericRecordMetadata metadata) {
        final RecordingFacade ff = new RecordingFacade(true);
        ff.isResident = true;
        final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
        readAhead.of(metadata);
        final long rows = frame.getPartitionHi() - frame.getPartitionLo();
        // the first frame goes out unchecked
        walk(readAhead, frame, rows);
        Assert.assertEquals(0, ff.residencyChecks);
        for (int f = 0; f < TRUST_STREAK; f++) {
            walk(readAhead, frame, rows);
        }
        final int checks = ff.residencyChecks;
        Assert.assertEquals(0, walk(readAhead, frame, rows));
        return ff.residencyChecks == checks;
    }

    // The fewest bytes the calling thread allocates in a round of frames of the stream, after
    // a warm-up: a round that allocates nothing shows that the compiled code allocates nothing
    // per frame.
    private static long leastAllocatedPerRound(com.sun.management.ThreadMXBean threadMXBean, QwpBackwardReadAhead readAhead, TestFrame frame) {
        for (int f = 0; f < ALLOCATION_WARM_UP_FRAMES; f++) {
            walk(readAhead, frame);
        }
        long leastAllocated = Long.MAX_VALUE;
        for (int round = 0; round < ALLOCATION_ROUNDS; round++) {
            final long allocatedBefore = threadMXBean.getCurrentThreadAllocatedBytes();
            for (int f = 0; f < ALLOCATION_ROUND_FRAMES; f++) {
                walk(readAhead, frame);
            }
            leastAllocated = Math.min(leastAllocated, threadMXBean.getCurrentThreadAllocatedBytes() - allocatedBefore);
        }
        return leastAllocated;
    }

    private static GenericRecordMetadata metadata(int[] types) {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        for (int i = 0; i < types.length; i++) {
            // the short constructor refuses SYMBOL columns, which need symbol table parameters
            metadata.add(new TableColumnMetadata("c" + i, types[i], IndexType.NONE, 0, false, null));
        }
        return metadata;
    }

    // a carrier thread, bound for its whole life as the worker of a pool is, that runs what the test hands it
    private static ExecutorService newCarrier() {
        return Executors.newSingleThreadExecutor(worker -> {
            final Thread thread = new Thread(() -> {
                CarrierIdentity.bind();
                try {
                    worker.run();
                } finally {
                    CarrierIdentity.unbind();
                }
            });
            thread.setDaemon(true);
            return thread;
        });
    }

    private static void onAnotherThread(Runnable task) throws InterruptedException {
        final Throwable[] failure = {null};
        final Thread thread = new Thread(() -> {
            try {
                task.run();
            } catch (Throwable th) {
                failure[0] = th;
            }
        });
        thread.start();
        thread.join();
        if (failure[0] != null) {
            throw new AssertionError(failure[0]);
        }
    }

    private static <T> T runOn(ExecutorService carrier, Callable<T> task) throws Exception {
        try {
            return carrier.submit(task).get(30, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            throw new AssertionError(e.getCause());
        }
    }

    private static void setVarSizeColumn(
            AddressedFrame frame,
            int column,
            ColumnTypeDriver driver,
            MemoryCARWImpl aux,
            MemoryCARWImpl data,
            int lo,
            int hi
    ) {
        final long auxBase = aux.addressOf(0);
        frame.set(
                column,
                data.getAppendOffset() > 0 ? data.addressOf(0) : 0,
                driver.getDataVectorSizeAt(auxBase, hi - 1),
                auxBase + driver.getAuxVectorOffset(lo),
                driver.getAuxVectorOffset(hi) - driver.getAuxVectorOffset(lo)
        );
    }

    // two columns, LONG and DOUBLE, of 3,000 rows: 48,000 bytes in two 24,000-byte ranges
    private static TestFrame smallFrame() {
        return new TestFrame(new int[]{ColumnType.LONG, ColumnType.DOUBLE}, false, 0, 3_000);
    }

    private static void stop(ExecutorService carrier) throws InterruptedException {
        carrier.shutdown();
        Assert.assertTrue(carrier.awaitTermination(30, TimeUnit.SECONDS));
    }

    // how much a trusted frame adds to the touch sink: the byte its walk reads first
    private static long trustedFrameTouch(PageFrame frame, GenericRecordMetadata metadata) {
        Assert.assertTrue(isTrustedAfterStreak(frame, metadata));
        final RecordingFacade ff = new RecordingFacade(true);
        ff.isResident = true;
        final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
        readAhead.of(metadata);
        final long rows = frame.getPartitionHi() - frame.getPartitionLo();
        // the first frame goes out unchecked, the next TRUST_STREAK earn the trust
        for (int f = 0; f <= TRUST_STREAK; f++) {
            walk(readAhead, frame, rows);
        }
        final long sink = readAhead.getTouchSink();
        walk(readAhead, frame, rows);
        return readAhead.getTouchSink() - sink;
    }

    // navigates to the frame and advises it slice by slice, top slice first, as the egress does
    private static long walk(QwpBackwardReadAhead readAhead, PageFrame frame, long rows) {
        readAhead.ofFrame(frame, rows);
        long bytes = 0;
        for (long emitted = 0; emitted < rows; ) {
            final long slice = Math.min(SLICE_ROWS, rows - emitted);
            bytes += readAhead.advise(rows - emitted - slice, 0);
            emitted += slice;
        }
        return bytes;
    }

    private static long walk(QwpBackwardReadAhead readAhead, TestFrame frame) {
        return walk(readAhead, frame, frame.rows);
    }

    // Walks frames of a stream down as the egress appends them, in batches of batchRows rows and
    // slices of at most SLICE_ROWS, and tells the read-ahead the lowest row of each frame the first
    // batch takes; returns the bytes the stream asked for, or read, by the end of the first batch.
    private static long walkStream(QwpBackwardReadAhead readAhead, RecordingFacade ff, PageFrame frame, long frameRows, int frames, long batchRows) {
        long firstBatchBytes = -1;
        long streamRows = 0;
        long batchLeft = batchRows;
        for (int f = 0; f < frames; f++) {
            readAhead.ofFrame(frame, frameRows);
            for (long emitted = 0; emitted < frameRows; ) {
                if (batchLeft == 0) {
                    batchLeft = batchRows;
                }
                final long slice = Math.min(Math.min(batchLeft, frameRows - emitted), SLICE_ROWS);
                final long lo = frameRows - emitted - slice;
                final boolean isFirstBatch = streamRows < batchRows;
                readAhead.advise(lo, isFirstBatch ? Math.max(0, lo + slice - batchLeft) : 0);
                emitted += slice;
                streamRows += slice;
                batchLeft -= slice;
                if (streamRows == batchRows) {
                    firstBatchBytes = ff.requestedBytes();
                }
            }
        }
        return firstBatchBytes;
    }

    // the stream's first frame, which it asks for whole without checking whether it is in memory
    private static void walkFirstFrame(QwpBackwardReadAhead readAhead, TestFrame frame) {
        Assert.assertEquals(frame.frameBytes(), walk(readAhead, frame));
    }

    // yields the fiber that runs the code, and records the carrier it resumes on
    private static void yieldAndRecordCarrier(IntList carriers) {
        Assert.assertTrue(Fiber.yieldCooperatively());
        carriers.add(CarrierIdentity.current());
    }

    /**
     * Page frame whose column addresses and sizes the test sets directly.
     */
    static class AddressedFrame implements PageFrame {
        private final long[] auxAddresses;
        private final long[] auxSizes;
        private final long[] pageAddresses;
        private final long[] pageSizes;
        private final long rowCount;
        byte format = PartitionFormat.NATIVE;
        long partitionFrameState;

        AddressedFrame(int columnCount, long rowCount) {
            this.rowCount = rowCount;
            pageAddresses = new long[columnCount];
            pageSizes = new long[columnCount];
            auxAddresses = new long[columnCount];
            auxSizes = new long[columnCount];
        }

        @Override
        public long getAuxPageAddress(int columnIndex) {
            return auxAddresses[columnIndex];
        }

        @Override
        public long getAuxPageSize(int columnIndex) {
            return auxSizes[columnIndex];
        }

        @Override
        public int getColumnCount() {
            return pageAddresses.length;
        }

        @Override
        public byte getFormat() {
            return format;
        }

        @Override
        public IndexReader getIndexReader(int columnIndex, int direction) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getPageAddress(int columnIndex) {
            return pageAddresses[columnIndex];
        }

        @Override
        public long getPageSize(int columnIndex) {
            return pageSizes[columnIndex];
        }

        @Override
        public int getParquetRowGroup() {
            return -1;
        }

        @Override
        public int getParquetRowGroupHi() {
            return -1;
        }

        @Override
        public int getParquetRowGroupLo() {
            return -1;
        }

        @Override
        public long getPartitionFrameState() {
            return partitionFrameState;
        }

        @Override
        public long getPartitionHi() {
            return rowCount;
        }

        @Override
        public int getPartitionIndex() {
            return 0;
        }

        @Override
        public long getPartitionLo() {
            return 0;
        }

        void set(int column, long pageAddress, long pageSize, long auxAddress, long auxSize) {
            pageAddresses[column] = pageAddress;
            pageSizes[column] = pageSize;
            auxAddresses[column] = auxAddress;
            auxSizes[column] = auxSize;
        }
    }

    /**
     * Records the ranges the read-ahead asks the OS for, without asking it, or, where the OS
     * does not read advised pages, the ranges it reads itself, without reading them. It
     * reports every range out of memory, unless a test says otherwise, and the major faults a
     * test sets.
     */
    static final class RecordingFacade extends TestFilesFacadeImpl {
        private final boolean isWillNeedSupported;
        // [lo, hi) address pairs a test reports out of memory, whatever isResident says
        private final LongList missingRanges = new LongList();
        // [lo, hi) address pairs the stream advised, or read itself, in request order
        private final LongList ranges = new LongList();
        int faultReads;
        boolean isResident;
        long majorFaults;
        int residencyChecks;

        RecordingFacade(boolean isWillNeedSupported) {
            this.isWillNeedSupported = isWillNeedSupported;
        }

        @Override
        public long getThreadMajorFaults() {
            // a stream that reads its windows itself trusts none, so it needs no faults
            Assert.assertTrue("read the faults of a stream that reads its windows itself", isWillNeedSupported);
            faultReads++;
            return majorFaults;
        }

        @Override
        public boolean isMadviseWillNeedSupported() {
            return isWillNeedSupported;
        }

        @Override
        public boolean isPageCacheResident(long address, long len) {
            Assert.assertTrue("len=" + len, len > 0);
            // a stream that reads its windows itself checks none: a page in memory costs a load
            Assert.assertTrue("checked a window it reads itself", isWillNeedSupported);
            residencyChecks++;
            for (int i = 0, n = missingRanges.size(); i < n; i += 2) {
                if (address < missingRanges.getQuick(i + 1) && address + len > missingRanges.getQuick(i)) {
                    return false;
                }
            }
            return isResident;
        }

        @Override
        public int madviseWillNeed(long address, long len) {
            Assert.assertTrue("len=" + len, len > 0);
            Assert.assertTrue("advised where the OS does not read advised pages", isWillNeedSupported);
            ranges.add(address, address + len);
            return 1;
        }

        // Records the range without reading it, so a frame can stand for data at an address that
        // maps nothing, and returns its length, which the stream adds to its touch sink.
        @Override
        public long touchPages(long address, long len) {
            Assert.assertTrue("len=" + len, len > 0);
            Assert.assertFalse("read a window the OS reads ahead", isWillNeedSupported);
            ranges.add(address, address + len);
            return len;
        }

        // every request lies inside one of the frame's published page or aux extents
        void assertWithinExtents(String context, PageFrame frame) {
            for (int i = 0, n = ranges.size(); i < n; i += 2) {
                final long lo = ranges.getQuick(i);
                final long hi = ranges.getQuick(i + 1);
                boolean isInside = false;
                for (int ci = 0, cn = frame.getColumnCount(); ci < cn && !isInside; ci++) {
                    isInside = isInside(lo, hi, frame.getPageAddress(ci), frame.getPageSize(ci))
                            || isInside(lo, hi, frame.getAuxPageAddress(ci), frame.getAuxPageSize(ci));
                }
                Assert.assertTrue(context + ": request [" + lo + ", " + hi + ") outside the frame", isInside);
            }
        }

        void assertTiles(String context, long lo, long hi) {
            final LongList within = new LongList();
            for (int i = 0, n = ranges.size(); i < n; i += 2) {
                if (ranges.getQuick(i) >= lo && ranges.getQuick(i + 1) <= hi) {
                    within.add(ranges.getQuick(i), ranges.getQuick(i + 1));
                }
            }
            // sort the pairs by their start; requests come top down, so reverse insertion
            final int pairs = within.size() / 2;
            final long[][] sorted = new long[pairs][];
            for (int p = 0; p < pairs; p++) {
                sorted[p] = new long[]{within.getQuick(2 * p), within.getQuick(2 * p + 1)};
            }
            Arrays.sort(sorted, Comparator.comparingLong(a -> a[0]));
            long next = lo;
            for (long[] r : sorted) {
                Assert.assertEquals(context + ": gap or overlap at " + (r[0] - lo), next, r[0]);
                next = r[1];
            }
            Assert.assertEquals(context + ": requests end short", hi, next);
        }

        void clear() {
            ranges.clear();
        }

        // reports [lo, hi) out of memory
        void miss(long lo, long hi) {
            missingRanges.add(lo, hi);
        }

        // reports nothing out of memory any more, beyond what isResident says
        void clearMisses() {
            missingRanges.clear();
        }

        boolean contains(long lo, long hi) {
            for (int i = 0, n = ranges.size(); i < n; i += 2) {
                if (ranges.getQuick(i) == lo && ranges.getQuick(i + 1) == hi) {
                    return true;
                }
            }
            return false;
        }

        // bytes of [lo, hi) the requests cover
        long coveredWithin(long lo, long hi) {
            final int n = ranges.size() / 2;
            final long[][] clipped = new long[n][];
            int k = 0;
            for (int i = 0; i < n; i++) {
                final long a = Math.max(lo, ranges.getQuick(2 * i));
                final long b = Math.min(hi, ranges.getQuick(2 * i + 1));
                if (b > a) {
                    clipped[k++] = new long[]{a, b};
                }
            }
            Arrays.sort(clipped, 0, k, Comparator.comparingLong(r -> r[0]));
            long covered = 0;
            long end = lo;
            for (int i = 0; i < k; i++) {
                final long a = Math.max(end, clipped[i][0]);
                if (clipped[i][1] > a) {
                    covered += clipped[i][1] - a;
                    end = clipped[i][1];
                }
            }
            return covered;
        }

        long lowestWithin(long lo, long hi) {
            long lowest = hi;
            for (int i = 0, n = ranges.size(); i < n; i += 2) {
                if (ranges.getQuick(i) >= lo && ranges.getQuick(i + 1) <= hi) {
                    lowest = Math.min(lowest, ranges.getQuick(i));
                }
            }
            return lowest;
        }

        String rangesToString() {
            return ranges.toString();
        }

        int requestCount() {
            return ranges.size() / 2;
        }

        long requestedBytes() {
            long bytes = 0;
            for (int i = 0, n = ranges.size(); i < n; i += 2) {
                bytes += ranges.getQuick(i + 1) - ranges.getQuick(i);
            }
            return bytes;
        }

        private static boolean isInside(long lo, long hi, long address, long size) {
            return address != 0 && lo >= address && hi <= address + size;
        }
    }

    /**
     * Rows of columns of the given types, published as a backward page frame publishes them, with
     * value sizes a test picks. Fixed-size columns hold values in memory. A var-size column holds
     * aux entries in memory, laid out as its type's driver reads them, over data at an address
     * that maps nothing: the advise path passes data addresses to the facade and never reads them,
     * so a frame can stand for gigabytes of long values. A var-size column starts as a column top.
     */
    static final class SyntheticFrame extends AddressedFrame implements AutoCloseable {
        private static final long FAKE_DATA_ADDRESS = 1L << 44;
        final long rows;
        // columns whose aux entries point anywhere, so their data ranges mean nothing
        private final IntList hostileColumns = new IntList();
        private final ObjList<MemoryCARWImpl> memories = new ObjList<>();
        private final int[] types;

        SyntheticFrame(int[] types, long rows) {
            super(types.length, rows);
            this.types = types;
            this.rows = rows;
            for (int ci = 0; ci < types.length; ci++) {
                if (!ColumnType.isVarSize(types[ci])) {
                    final long size = ColumnType.sizeOf(types[ci]) * rows;
                    final MemoryCARWImpl values = memory();
                    values.jumpTo(size);
                    set(ci, values.addressOf(0), size, 0, 0);
                }
            }
        }

        @Override
        public void close() {
            Misc.freeObjList(memories);
        }

        // every byte the rows [rowLo, rowHi) take, of every column, has been asked for
        void assertCovered(String context, RecordingFacade ff, long rowLo, long rowHi) {
            final LongList ranges = rangesOfRows(rowLo, rowHi);
            for (int i = 0, n = ranges.size(); i < n; i += 2) {
                final long lo = ranges.getQuick(i);
                final long hi = ranges.getQuick(i + 1);
                Assert.assertEquals(context + ": range of " + (hi - lo) + " bytes", hi - lo, ff.coveredWithin(lo, hi));
            }
        }

        // every byte of every column has been asked for once
        void assertTiled(String context, RecordingFacade ff) {
            final LongList ranges = rangesOfRows(0, rows);
            for (int i = 0, n = ranges.size(); i < n; i += 2) {
                ff.assertTiles(context, ranges.getQuick(i), ranges.getQuick(i + 1));
            }
        }

        // bytes the rows [rowLo, rowHi) take over all columns: values, aux entries and data
        long bytesOfRows(long rowLo, long rowHi) {
            final LongList ranges = rangesOfRows(rowLo, rowHi);
            long bytes = 0;
            for (int i = 0, n = ranges.size(); i < n; i += 2) {
                bytes += ranges.getQuick(i + 1) - ranges.getQuick(i);
            }
            return bytes;
        }

        // Sets var-size column ci to data that the frame publishes without aux entries to tell
        // its rows apart: with no aux address at all, or with one and no bytes behind it.
        SyntheticFrame dataWithoutAux(int ci, long dataSize, boolean hasAuxAddress) {
            long auxAddress = 0;
            if (hasAuxAddress) {
                // memory has an address once something is written to it
                final MemoryCARWImpl aux = memory();
                aux.putLong(0);
                auxAddress = aux.addressOf(0);
            }
            set(ci, dataAddress(ci), dataSize, auxAddress, 0);
            hostileColumns.add(ci);
            return this;
        }

        GenericRecordMetadata metadata() {
            return QwpBackwardReadAheadTest.metadata(types);
        }

        // Sets var-size column ci to aux entries that point at offsetOfRow(row), inside a data
        // extent of dataSize bytes or not, as a damaged or hostile frame source may publish them.
        SyntheticFrame offsets(int ci, long dataSize, LongUnaryOperator offsetOfRow) {
            final MemoryCARWImpl aux = memory();
            for (long r = 0; r < rows; r++) {
                putAuxEntry(aux, types[ci], offsetOfRow.applyAsLong(r), 1_000);
            }
            set(ci, dataAddress(ci), dataSize, aux.addressOf(0), aux.getAppendOffset());
            hostileColumns.add(ci);
            return this;
        }

        // The address ranges the rows [rowLo, rowHi) take in every column, as [lo, hi) pairs:
        // values, aux entries, and data, but for that of columns whose aux entries point anywhere.
        LongList rangesOfRows(long rowLo, long rowHi) {
            final LongList ranges = new LongList();
            for (int ci = 0; ci < types.length; ci++) {
                final int type = types[ci];
                if (!ColumnType.isVarSize(type)) {
                    final int size = ColumnType.sizeOf(type);
                    addRange(ranges, getPageAddress(ci) + rowLo * size, getPageAddress(ci) + rowHi * size);
                    continue;
                }
                final long auxAddress = getAuxPageAddress(ci);
                // a column top publishes no aux entries, and without them no row has data
                if (auxAddress == 0 || getAuxPageSize(ci) <= 0) {
                    continue;
                }
                final ColumnTypeDriver driver = ColumnType.getDriver(type);
                addRange(ranges, auxAddress + driver.getAuxVectorOffset(rowLo), auxAddress + driver.getAuxVectorOffset(rowHi));
                if (getPageAddress(ci) != 0 && !hostileColumns.contains(ci)) {
                    final long dataHi = rowHi < rows ? driver.getDataVectorOffset(auxAddress, rowHi) : getPageSize(ci);
                    addRange(ranges, getPageAddress(ci) + driver.getDataVectorOffset(auxAddress, rowLo), getPageAddress(ci) + dataHi);
                }
            }
            return ranges;
        }

        // Sets var-size column ci to rows whose values take valueBytes(row) bytes each, their data
        // from dataLo bytes into the column's data, where the frame of the rows before them ends.
        // VARCHARs of up to 9 bytes sit inline in the aux vector, and one of no bytes is NULL.
        SyntheticFrame values(int ci, long dataLo, LongUnaryOperator valueBytes) {
            final int type = types[ci];
            final MemoryCARWImpl aux = memory();
            long offset = dataLo;
            for (long r = 0; r < rows; r++) {
                final long size = valueBytes.applyAsLong(r);
                putAuxEntry(aux, type, offset, size);
                if (!ColumnType.isVarchar(type) || size > VarcharTypeDriver.VARCHAR_MAX_BYTES_FULLY_INLINED) {
                    offset += size;
                }
            }
            // as a backward page frame does, publish data up to where the frame's last row ends,
            // and no data address when no row of the column up to there takes data
            set(ci, offset > 0 ? dataAddress(ci) : 0, offset, aux.addressOf(0), aux.getAppendOffset());
            return this;
        }

        private static void addRange(LongList ranges, long lo, long hi) {
            if (hi > lo) {
                ranges.add(lo, hi);
            }
        }

        private static long dataAddress(int ci) {
            return FAKE_DATA_ADDRESS + ((long) ci << 40);
        }

        private static void putAuxEntry(MemoryCARWImpl aux, int type, long offset, long size) {
            if (ColumnType.isVarchar(type)) {
                // a header of the size and the flags, NULL 4 or inlined 1 above 4 flag bits; the
                // offset in the top 48 bits of the second long
                final long header = size == 0
                        ? VarcharTypeDriver.VARCHAR_HEADER_FLAG_NULL
                        : (size << 4) | (size <= VarcharTypeDriver.VARCHAR_MAX_BYTES_FULLY_INLINED ? 1 : 0);
                aux.putLong(header);
                aux.putLong(offset << 16);
            } else if (ColumnType.isArray(type)) {
                aux.putLong(offset);
                aux.putLong(size);
            } else {
                // STRING and BINARY: the offset alone
                aux.putLong(offset);
            }
        }

        private MemoryCARWImpl memory() {
            final MemoryCARWImpl memory = new MemoryCARWImpl(1 << 20, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
            memories.add(memory);
            return memory;
        }
    }

    /**
     * Rows {@code [lo, hi)} of fixed-size columns of the given types, followed, when asked,
     * by a STRING, a VARCHAR, a VARCHAR whose values all sit inline in its aux vector, and a
     * LONG column top; addressed the way the backward page-frame cursor publishes them.
     */
    private static final class TestFrame extends AddressedFrame implements AutoCloseable {
        final long rows;
        private final MemoryCARWImpl[] memories;
        private final int[] types;

        TestFrame(int[] fixedTypes, boolean withVarSize, int lo, int hi) {
            super(fixedTypes.length + (withVarSize ? 4 : 0), hi - lo);
            this.rows = hi - lo;
            final int columnCount = fixedTypes.length + (withVarSize ? 4 : 0);
            types = new int[columnCount];
            System.arraycopy(fixedTypes, 0, types, 0, fixedTypes.length);
            memories = new MemoryCARWImpl[fixedTypes.length + (withVarSize ? 6 : 0)];
            for (int ci = 0; ci < fixedTypes.length; ci++) {
                final int size = ColumnType.sizeOf(fixedTypes[ci]);
                final MemoryCARWImpl mem = memories[ci] = new MemoryCARWImpl(1 << 20, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                mem.jumpTo((long) size * hi);
                set(ci, mem.addressOf((long) size * lo), (long) size * (hi - lo), 0, 0);
            }
            if (withVarSize) {
                final int base = fixedTypes.length;
                types[base] = ColumnType.STRING;
                types[base + 1] = ColumnType.VARCHAR;
                types[base + 2] = ColumnType.VARCHAR;
                types[base + 3] = ColumnType.LONG;
                for (int m = base; m < memories.length; m++) {
                    memories[m] = new MemoryCARWImpl(1 << 20, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                }
                final MemoryCARWImpl strAux = memories[base];
                final MemoryCARWImpl strData = memories[base + 1];
                final MemoryCARWImpl vcAux = memories[base + 2];
                final MemoryCARWImpl vcData = memories[base + 3];
                final MemoryCARWImpl inlineAux = memories[base + 4];
                final MemoryCARWImpl inlineData = memories[base + 5];
                StringTypeDriver.INSTANCE.configureAuxMemO3RSS(strAux);
                for (int r = 0; r < hi; r++) {
                    if (r % 7 == 0) {
                        StringTypeDriver.INSTANCE.appendNull(strAux, strData);
                    } else {
                        StringTypeDriver.appendValue(strAux, strData, "string value " + r);
                    }
                    VarcharTypeDriver.appendValue(
                            vcAux,
                            vcData,
                            r % 5 == 0 ? null : new Utf8String(r % 3 == 0 ? "short" : "a varchar too long to inline " + r)
                    );
                    VarcharTypeDriver.appendValue(inlineAux, inlineData, new Utf8String("v" + (r % 10)));
                }
                setVarSizeColumn(this, base, StringTypeDriver.INSTANCE, strAux, strData, lo, hi);
                setVarSizeColumn(this, base + 1, VarcharTypeDriver.INSTANCE, vcAux, vcData, lo, hi);
                setVarSizeColumn(this, base + 2, VarcharTypeDriver.INSTANCE, inlineAux, inlineData, lo, hi);
                // a column top: no addresses, only the size hint a top frame publishes
                set(base + 3, 0, 8L * (hi - lo), 0, 0);
            }
        }

        @Override
        public void close() {
            for (MemoryCARWImpl mem : memories) {
                if (mem != null) {
                    mem.close();
                }
            }
        }

        // bytes of rows [rowLo, rowHi) over all columns: values, aux entries and data
        long bytesOfRows(long rowLo, long rowHi) {
            long bytes = 0;
            for (int ci = 0; ci < types.length; ci++) {
                final long[] range = rowRange(ci, rowLo, rowHi);
                bytes += range[1] - range[0];
                if (isVarSize(ci)) {
                    final long[] dataRange = dataRange(ci, rowLo, rowHi);
                    bytes += dataRange[1] - dataRange[0];
                }
            }
            return bytes;
        }

        // the data of rows [rowLo, rowHi) of a var-size column
        long[] dataRange(int ci, long rowLo, long rowHi) {
            final ColumnTypeDriver driver = ColumnType.getDriver(types[ci]);
            final long auxAddress = getAuxPageAddress(ci);
            final long dataLo = driver.getDataVectorOffset(auxAddress, rowLo);
            final long dataHi = rowHi < rows ? driver.getDataVectorOffset(auxAddress, rowHi) : getPageSize(ci);
            final long base = getPageAddress(ci);
            return base == 0 ? new long[]{0, 0} : new long[]{base + dataLo, base + dataHi};
        }

        // bytes of the frame's own rows over all columns: what reading the frame covers
        long frameBytes() {
            return bytesOfRows(0, rows);
        }

        // the ranges of the frame's rows that hold bytes, of values, aux entries and data
        int rangeCount() {
            int ranges = 0;
            for (int ci = 0; ci < types.length; ci++) {
                final long[] range = rowRange(ci, 0, rows);
                if (range[1] > range[0]) {
                    ranges++;
                }
                if (isVarSize(ci)) {
                    final long[] dataRange = dataRange(ci, 0, rows);
                    if (dataRange[1] > dataRange[0]) {
                        ranges++;
                    }
                }
            }
            return ranges;
        }

        boolean isVarSize(int ci) {
            return ColumnType.isVarSize(types[ci]);
        }

        GenericRecordMetadata metadata() {
            return QwpBackwardReadAheadTest.metadata(types);
        }

        // bytes of rows [rowLo, rowHi) of a fixed-size column, or of a var-size column's aux entries
        long[] rowRange(int ci, long rowLo, long rowHi) {
            if (isVarSize(ci)) {
                final ColumnTypeDriver driver = ColumnType.getDriver(types[ci]);
                final long auxAddress = getAuxPageAddress(ci);
                final long auxHi = rowHi < rows ? driver.getAuxVectorOffset(rowHi) : getAuxPageSize(ci);
                return new long[]{auxAddress + driver.getAuxVectorOffset(rowLo), auxAddress + auxHi};
            }
            final long address = getPageAddress(ci);
            if (address == 0) {
                return new long[]{0, 0};
            }
            final int size = ColumnType.sizeOf(types[ci]);
            return new long[]{address + rowLo * size, address + rowHi * size};
        }

        Saved clearAll() {
            final Saved saved = new Saved(getColumnCount());
            for (int ci = 0; ci < getColumnCount(); ci++) {
                saved.values[ci] = new long[]{getPageAddress(ci), getPageSize(ci), getAuxPageAddress(ci), getAuxPageSize(ci)};
                set(ci, 0, getPageSize(ci), 0, 0);
            }
            return saved;
        }

        void restore(Saved saved) {
            for (int ci = 0; ci < getColumnCount(); ci++) {
                final long[] v = saved.values[ci];
                set(ci, v[0], v[1], v[2], v[3]);
            }
        }

        static final class Saved {
            final long[][] values;

            Saved(int columnCount) {
                values = new long[columnCount][];
            }
        }
    }
}
