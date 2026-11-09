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
import io.questdb.std.Files;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
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

/**
 * Pins {@link QwpBackwardReadAhead}, which keeps the rows of a backward page-frame stream
 * on their way into memory ahead of its walk down. With MADV_WILLNEED the stream asks for
 * windows of rows below the slice it appends: the first window holds the top rows of the
 * frame, the rows of the first batch; the next ones double up to a byte cap; every row of
 * every column that is not in memory is asked for once, before the stream reads it. After
 * the first batch the stream checks whether a window is in memory, backs off checking while
 * windows are not, and trusts small frames after a streak in memory until its thread reads
 * from storage. Without MADV_WILLNEED the stream reads the whole frame once, before the
 * first slice.
 */
public class QwpBackwardReadAheadTest {
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
                walkFirstBatch(readAhead, frame);
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
        // of() and clear() start a new stream: its first batch goes out unchecked, it checks
        // the first frame after the batch even when the stream before had stopped checking, and
        // it trusts after the first streak, not the longer one the stream before needed.
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
                    walkFirstBatch(readAhead, frame);
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
    public void testFirstBatchGoesOutUnchecked() throws Exception {
        // The rows of the first batch go out as soon as they are read: the stream advises
        // every window whose top row goes out before row INITIAL_WINDOW_ROWS without checking
        // whether it is in memory, and checks the windows after them.
        TestUtils.assertMemoryLeak(() -> {
            final int batchRows = (int) QwpBackwardReadAhead.INITIAL_WINDOW_ROWS;
            try (
                    TestFrame small = smallFrame();
                    TestFrame longs = new TestFrame(new int[]{ColumnType.LONG}, false, 0, 3_000);
                    TestFrame shortOfBatch = new TestFrame(new int[]{ColumnType.LONG}, false, 0, batchRows - 1);
                    TestFrame wholeBatch = new TestFrame(new int[]{ColumnType.LONG}, false, 0, batchRows)
            ) {
                final RecordingFacade ff = new RecordingFacade(true);
                ff.isResident = true;
                final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
                readAhead.of(small.metadata());
                // frames whose top rows go out at rows 0, 3,000 ... 15,000
                for (int f = 0; f < 6; f++) {
                    Assert.assertEquals(small.frameBytes(), walk(readAhead, small));
                }
                Assert.assertEquals(12, ff.requestCount());
                Assert.assertEquals(0, ff.residencyChecks);
                // the next one's top row goes out at row 18,000: in memory, so not asked for
                Assert.assertEquals(0, walk(readAhead, small));
                Assert.assertEquals(12, ff.requestCount());
                Assert.assertEquals(2, ff.residencyChecks);

                // a frame whose top row goes out at row 16,383 still holds a row of the batch
                readAhead.of(longs.metadata());
                Assert.assertEquals(shortOfBatch.frameBytes(), walk(readAhead, shortOfBatch));
                Assert.assertEquals(longs.frameBytes(), walk(readAhead, longs));
                Assert.assertEquals(2, ff.residencyChecks);
                Assert.assertEquals(0, walk(readAhead, longs));
                Assert.assertEquals(3, ff.residencyChecks);

                // one whose top row goes out at row 16,384 does not
                readAhead.of(longs.metadata());
                Assert.assertEquals(wholeBatch.frameBytes(), walk(readAhead, wholeBatch));
                Assert.assertEquals(3, ff.residencyChecks);
                Assert.assertEquals(0, walk(readAhead, longs));
                Assert.assertEquals(4, ff.residencyChecks);
                Assert.assertEquals(0, ff.faultReads);
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
            try (TestFrame frame = new TestFrame(fixedTypes, true, lo, lo + rows)) {
                final RecordingFacade ff = new RecordingFacade(true);
                final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
                readAhead.of(frame.metadata());
                readAhead.ofFrame(frame, rows);

                final long top = rows - QwpBackwardReadAhead.INITIAL_WINDOW_ROWS;
                Assert.assertEquals(frame.bytesOfRows(top, rows), readAhead.advise(rows - SLICE_ROWS));
                // one request a column for the window's rows, values or aux entries, and one
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
        });
    }

    @Test
    public void testFramesWithoutReadAheadAdvanceTheStream() throws Exception {
        // Parquet frames, empty frames and frames of column tops ask for nothing, but their
        // rows go out all the same: a native frame after 20,000 such rows starts after the
        // first batch, so the stream checks it.
        TestUtils.assertMemoryLeak(() -> {
            try (TestFrame frame = smallFrame()) {
                final RecordingFacade ff = new RecordingFacade(true);
                ff.isResident = true;
                final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
                readAhead.of(frame.metadata());
                frame.format = PartitionFormat.PARQUET;
                readAhead.ofFrame(frame, 20_000);
                Assert.assertEquals(0, readAhead.advise(20_000 - SLICE_ROWS));
                frame.format = PartitionFormat.NATIVE;
                Assert.assertEquals(0, walk(readAhead, frame));
                Assert.assertEquals(2, ff.residencyChecks);

                // a frame without rows moves the stream nowhere
                readAhead.of(frame.metadata());
                readAhead.ofFrame(frame, 0);
                Assert.assertEquals(0, readAhead.advise(0));
                Assert.assertEquals(frame.frameBytes(), walk(readAhead, frame));
                Assert.assertEquals(2, ff.residencyChecks);

                // 20,000 rows of column tops
                readAhead.of(frame.metadata());
                final TestFrame.Saved saved = frame.clearAll();
                readAhead.ofFrame(frame, 20_000);
                Assert.assertEquals(0, readAhead.advise(20_000 - SLICE_ROWS));
                frame.restore(saved);
                Assert.assertEquals(0, walk(readAhead, frame));
                Assert.assertEquals(4, ff.residencyChecks);
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
                walkFirstBatch(readAhead, frame);
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
        // A fault in a frame with a range past SMALL_FRAME_RANGE_BYTES reads 64 KiB of it at a
        // time, not the whole range, so the stream checks each window of such a frame, however
        // many before it were in memory.
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
        // The stream checks each window after the first batch and asks for none that is in
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
                walkFirstBatch(readAhead, frame);
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
                    Assert.assertEquals(0, readAhead.advise(0));
                    readAhead.of(frame.metadata());
                    Assert.assertEquals(0, readAhead.advise(0));

                    // the stream decodes a parquet frame into memory when it navigates to it
                    frame.format = PartitionFormat.PARQUET;
                    readAhead.ofFrame(frame, 3_000);
                    Assert.assertEquals(0, readAhead.advise(3_000 - SLICE_ROWS));
                    frame.format = PartitionFormat.NATIVE;

                    // a frame without rows
                    readAhead.ofFrame(frame, 0);
                    Assert.assertEquals(0, readAhead.advise(0));

                    // a frame whose every column is a column top
                    final TestFrame.Saved saved = frame.clearAll();
                    readAhead.ofFrame(frame, 3_000);
                    Assert.assertEquals(0, readAhead.advise(3_000 - SLICE_ROWS));
                    frame.restore(saved);

                    // a bound frame, then cleared
                    readAhead.ofFrame(frame, 3_000);
                    readAhead.clear();
                    Assert.assertEquals(0, readAhead.advise(3_000 - SLICE_ROWS));
                    Assert.assertEquals(0, ff.requestCount());
                }
            }
        });
    }

    @Test
    public void testOfFrameRestartsTheWindow() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (
                    TestFrame first = new TestFrame(new int[]{ColumnType.LONG}, false, 0, 50_000);
                    TestFrame second = new TestFrame(new int[]{ColumnType.LONG}, false, 0, 40_000)
            ) {
                final RecordingFacade ff = new RecordingFacade(true);
                final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
                readAhead.of(first.metadata());
                readAhead.ofFrame(first, 50_000);
                Assert.assertTrue(readAhead.advise(50_000 - SLICE_ROWS) > 0);
                // the stream leaves the first frame part way and moves to the next
                readAhead.ofFrame(second, 40_000);
                ff.clear();
                Assert.assertEquals(8L * QwpBackwardReadAhead.INITIAL_WINDOW_ROWS, readAhead.advise(40_000 - SLICE_ROWS));
                final long[] range = second.rowRange(0, 40_000 - QwpBackwardReadAhead.INITIAL_WINDOW_ROWS, 40_000);
                Assert.assertTrue(ff.contains(range[0], range[1]));
                Assert.assertEquals(1, ff.requestCount());
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
            try (TestFrame frame = new TestFrame(new int[]{ColumnType.INT, ColumnType.DOUBLE}, true, 300, 1_000)) {
                final RecordingFacade ff = new RecordingFacade(true);
                final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
                readAhead.of(frame.metadata());
                readAhead.ofFrame(frame, 700);
                Assert.assertEquals(frame.frameBytes(), readAhead.advise(0));
                Assert.assertEquals(0, readAhead.advise(0));
                assertEachColumnTiled(frame, ff);
            }
        });
    }

    @Test
    public void testSmallFramesInMemoryAreTrusted() throws Exception {
        // After the first batch, TRUST_STREAK small frames in a row found in memory make the
        // stream trust the next ones: it no longer checks them or asks for them, and before
        // each it reads the byte its walk reads first, and the major faults of its thread.
        TestUtils.assertMemoryLeak(() -> {
            try (TestFrame frame = smallFrame()) {
                Unsafe.putByte(frame.getPageAddress(0) + frame.getPageSize(0) - 1, (byte) 3);
                final RecordingFacade ff = new RecordingFacade(true);
                ff.isResident = true;
                ff.majorFaults = 5;
                final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
                readAhead.of(frame.metadata());
                walkFirstBatch(readAhead, frame);
                Assert.assertEquals(12, ff.requestCount());
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
                Assert.assertEquals(12, ff.requestCount());
            }
        });
    }

    @Test
    public void testTouchPathCoversTheFrameBytesOnly() throws Exception {
        // Without MADV_WILLNEED the first slice reads the whole frame: exactly the frame's
        // rows of every column, aux and data alike, and nothing of a column top.
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
                Assert.assertEquals(frameBytes, readAhead.advise(hi - lo - SLICE_ROWS / 4));
                // once per frame, and without asking the OS
                Assert.assertEquals(0, readAhead.advise(0));
                Assert.assertEquals(0, ff.requestCount());

                // the same frame through MADV_WILLNEED asks for the same bytes, each once
                final RecordingFacade willNeed = new RecordingFacade(true);
                final QwpBackwardReadAhead windows = new QwpBackwardReadAhead(willNeed);
                windows.of(metadata(types));
                windows.ofFrame(frame, hi - lo);
                Assert.assertEquals(frameBytes, windows.advise(hi - lo - SLICE_ROWS / 4));
                Assert.assertEquals(frameBytes, willNeed.requestedBytes());

                frame.format = PartitionFormat.PARQUET;
                readAhead.ofFrame(frame, hi - lo);
                Assert.assertEquals(0, readAhead.advise(0));
            }
        });
    }

    @Test
    public void testTouchPathLoadsEveryOsPage() throws Exception {
        // Without MADV_WILLNEED the read-ahead loads one byte of every OS page of the frame.
        // Files.PAGE_SIZE is the OS page on POSIX, but on Windows it is the 64 KiB allocation
        // granularity, sixteen 4 KiB pages: a read that steps by it loads one page in sixteen.
        TestUtils.assertMemoryLeak(() -> {
            final long osPageSize = Os.isWindows() ? 4_096 : Files.PAGE_SIZE;
            final int pageCount = 20;
            // pageCount + 1 pages from a page boundary, and the slack to find the boundary
            final long allocSize = (pageCount + 2) * osPageSize;
            final long alloc = Unsafe.malloc(allocSize, MemoryTag.NATIVE_DEFAULT);
            try {
                final long base = (alloc + osPageSize - 1) & -osPageSize;
                // every byte of page p holds p + 1, so the sum of the loads names the pages they hit
                for (int p = 0; p <= pageCount; p++) {
                    Vect.memset(base + p * osPageSize, osPageSize, p + 1);
                }
                final long size = pageCount * osPageSize;
                final long rows = size / Long.BYTES;
                final AddressedFrame frame = new AddressedFrame(2, rows);
                // column 0 starts on a page boundary and covers pages [0, pageCount)
                frame.set(0, base, size, 0, 0);
                // column 1 starts mid-page and covers pages [0, pageCount]
                frame.set(1, base + osPageSize / 2, size, 0, 0);
                long expectedSink = 0;
                for (int p = 0; p < pageCount; p++) {
                    // the stride loads page p once in each column
                    expectedSink += 2L * (p + 1);
                }
                // the read of the last byte loads page pageCount - 1 again in column 0, and
                // page pageCount, which no stride reaches, in column 1
                expectedSink += pageCount + (pageCount + 1);

                final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(new RecordingFacade(false));
                readAhead.of(metadata(new int[]{ColumnType.LONG, ColumnType.LONG}));
                readAhead.ofFrame(frame, rows);
                Assert.assertEquals(2 * size, readAhead.advise(rows - SLICE_ROWS));
                Assert.assertEquals(expectedSink, readAhead.getTouchSink());
            } finally {
                Unsafe.free(alloc, allocSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testTouchPathNeverChecks() throws Exception {
        // Without MADV_WILLNEED the stream reads each frame at once, in memory or not, and
        // has nothing to check or trust.
        TestUtils.assertMemoryLeak(() -> {
            try (TestFrame frame = smallFrame()) {
                final RecordingFacade ff = new RecordingFacade(false);
                ff.isResident = true;
                final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
                readAhead.of(frame.metadata());
                for (int f = 0; f < 50; f++) {
                    Assert.assertEquals(frame.frameBytes(), walk(readAhead, frame));
                }
                Assert.assertEquals(0, ff.residencyChecks);
                Assert.assertEquals(0, ff.faultReads);
                Assert.assertEquals(0, ff.requestCount());
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
                // only the aux entries of columns 0 and 1
                Assert.assertEquals(16L * 100 + 8L * 5, readAhead.advise(0));

                // MADV_WILLNEED asks for the same aux entries, and for no data: the varchar
                // data extent is empty, and the string aux entries stop before the window's
                // bottom row
                final RecordingFacade willNeed = new RecordingFacade(true);
                final QwpBackwardReadAhead windows = new QwpBackwardReadAhead(willNeed);
                windows.of(metadata(types));
                windows.ofFrame(frame, 100);
                Assert.assertEquals(16L * 100 + 8L * 5, windows.advise(0));
                Assert.assertEquals(2, willNeed.requestCount());
                Assert.assertTrue(willNeed.contains(vcAux.addressOf(16L * 100), vcAux.addressOf(16L * 200)));
                Assert.assertTrue(willNeed.contains(strAux.addressOf(8L * 5), strAux.addressOf(8L * 10)));
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
                walkFirstBatch(readAhead, frame);
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
    public void testTrustEndsWhenTheStreamMovesThread() throws Exception {
        // A parked stream can resume on another worker thread, whose major faults say nothing
        // about the frames before: the stream checks again, and trusts after the usual streak.
        TestUtils.assertMemoryLeak(() -> {
            try (TestFrame frame = smallFrame()) {
                final RecordingFacade ff = new RecordingFacade(true);
                ff.isResident = true;
                final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
                readAhead.of(frame.metadata());
                walkFirstBatch(readAhead, frame);
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
    public void testWindowsAskForNothingOutsideThePublishedExtents() throws Exception {
        // A frame source that publishes fewer bytes than the frame's rows take, or aux entries
        // that point outside the data extent, cannot steer the windows out of bounds: down a
        // 40,000-row frame they ask for the published bytes the rows cover and nothing else,
        // and read no aux entry outside the published aux extent.
        TestUtils.assertMemoryLeak(() -> {
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

                final RecordingFacade ff = new RecordingFacade(true);
                final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
                readAhead.of(metadata(types));
                readAhead.ofFrame(frame, rows);
                long bytes = 0;
                for (long emitted = 0; emitted < rows; emitted += SLICE_ROWS) {
                    bytes += readAhead.advise(Math.max(0, rows - emitted - SLICE_ROWS));
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
                final RecordingFacade ff = new RecordingFacade(true);
                final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
                readAhead.of(metadata(new int[]{ColumnType.LONG}));
                readAhead.ofFrame(frame, rows);
                long bytes = 0;
                for (long emitted = 0; emitted < rows; emitted += SLICE_ROWS) {
                    bytes += readAhead.advise(Math.max(0, rows - emitted - SLICE_ROWS));
                }
                Assert.assertEquals(8L * 100, bytes);
                Assert.assertEquals(1, ff.requestCount());
            }
        });
    }

    @Test
    public void testWindowsCoverEveryRowOnceAheadOfTheWalk() throws Exception {
        // Walk a frame down in slices as the egress does: before each slice, every row of it
        // has been asked for; the first window holds the first batch, each next one is at
        // most twice the last; and at the end every byte of every column was asked for once.
        TestUtils.assertMemoryLeak(() -> {
            final int rows = 200_000;
            final int[] fixedTypes = {
                    ColumnType.LONG, ColumnType.TIMESTAMP, ColumnType.DOUBLE, ColumnType.INT, ColumnType.FLOAT,
                    ColumnType.IPv4, ColumnType.SHORT, ColumnType.CHAR, ColumnType.BYTE, ColumnType.BOOLEAN,
                    ColumnType.SYMBOL, ColumnType.UUID, ColumnType.LONG256, ColumnType.getGeoHashTypeWithBits(40),
                    ColumnType.getDecimalType(30, 6)
            };
            try (TestFrame frame = new TestFrame(fixedTypes, true, 1_000, 1_000 + rows)) {
                final RecordingFacade ff = new RecordingFacade(true);
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
                    final long bytes = readAhead.advise(lo);
                    Assert.assertEquals(ff.requestedBytes() - before, bytes);
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
                // rows, not the frame.
                Assert.assertEquals(frame.bytesOfRows(rows - 3 * QwpBackwardReadAhead.INITIAL_WINDOW_ROWS, rows), firstBatchBytes);
                assertEachColumnTiled(frame, ff);
                Assert.assertEquals(frame.frameBytes(), ff.requestedBytes());
            }
        });
    }

    @Test
    public void testWindowsStayWithinTheByteCap() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // 40 LONG columns: 320 bytes a row caps a window at 16 MiB / 320 rows
            final int[] wide = new int[40];
            Arrays.fill(wide, ColumnType.LONG);
            final int rows = 200_000;
            try (TestFrame frame = new TestFrame(wide, false, 0, rows)) {
                final RecordingFacade ff = new RecordingFacade(true);
                final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
                readAhead.of(frame.metadata());
                readAhead.ofFrame(frame, rows);
                final long capRows = QwpBackwardReadAhead.MAX_WINDOW_BYTES / 320;
                for (long emitted = 0; emitted < rows; emitted += SLICE_ROWS) {
                    final long bytes = readAhead.advise(Math.max(0, rows - emitted - SLICE_ROWS));
                    Assert.assertTrue("bytes=" + bytes, bytes <= capRows * 320);
                    Assert.assertEquals(0, bytes % 320);
                }
                assertEachColumnTiled(frame, ff);
            }

            // long values: 16,384 rows of 2 KiB VARCHARs hold 32 MiB, so the first window asks
            // for the top 16 MiB of their data, and sizes the windows after it by what a row takes
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
                final RecordingFacade ff = new RecordingFacade(true);
                final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
                readAhead.of(metadata(new int[]{ColumnType.VARCHAR}));
                readAhead.ofFrame(frame, longRows);

                final long first = readAhead.advise(longRows - SLICE_ROWS);
                final long dataEnd = frame.getPageAddress(0) + frame.getPageSize(0);
                // the aux entries of the first window, and the top 16 MiB of its data
                Assert.assertEquals(16L * QwpBackwardReadAhead.INITIAL_WINDOW_ROWS + QwpBackwardReadAhead.MAX_WINDOW_BYTES, first);
                Assert.assertTrue(ff.contains(dataEnd - QwpBackwardReadAhead.MAX_WINDOW_BYTES, dataEnd));
                for (long emitted = SLICE_ROWS; emitted < longRows; emitted += SLICE_ROWS) {
                    final long bytes = readAhead.advise(Math.max(0, longRows - emitted - SLICE_ROWS));
                    // a row takes 2,064 bytes with its aux entry: the next windows stay under the cap
                    Assert.assertTrue("bytes=" + bytes, bytes <= QwpBackwardReadAhead.MAX_WINDOW_BYTES);
                }
            }
        });
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

    // whether, after its first batch and TRUST_STREAK frames in memory, a stream of the frame trusts the next one
    private static boolean isTrustedAfterStreak(PageFrame frame, GenericRecordMetadata metadata) {
        final RecordingFacade ff = new RecordingFacade(true);
        ff.isResident = true;
        final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
        readAhead.of(metadata);
        final long rows = frame.getPartitionHi() - frame.getPartitionLo();
        for (long streamRows = 0; streamRows < QwpBackwardReadAhead.INITIAL_WINDOW_ROWS; streamRows += rows) {
            walk(readAhead, frame, rows);
        }
        for (int f = 0; f < TRUST_STREAK; f++) {
            walk(readAhead, frame, rows);
        }
        final int checks = ff.residencyChecks;
        Assert.assertEquals(0, walk(readAhead, frame, rows));
        return ff.residencyChecks == checks;
    }

    private static GenericRecordMetadata metadata(int[] types) {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        for (int i = 0; i < types.length; i++) {
            // the short constructor refuses SYMBOL columns, which need symbol table parameters
            metadata.add(new TableColumnMetadata("c" + i, types[i], IndexType.NONE, 0, false, null));
        }
        return metadata;
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

    // how much a trusted frame adds to the touch sink: the byte its walk reads first
    private static long trustedFrameTouch(PageFrame frame, GenericRecordMetadata metadata) {
        Assert.assertTrue(isTrustedAfterStreak(frame, metadata));
        final RecordingFacade ff = new RecordingFacade(true);
        ff.isResident = true;
        final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
        readAhead.of(metadata);
        final long rows = frame.getPartitionHi() - frame.getPartitionLo();
        for (long streamRows = 0; streamRows < QwpBackwardReadAhead.INITIAL_WINDOW_ROWS; streamRows += rows) {
            walk(readAhead, frame, rows);
        }
        for (int f = 0; f < TRUST_STREAK; f++) {
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
            bytes += readAhead.advise(rows - emitted - slice);
            emitted += slice;
        }
        return bytes;
    }

    private static long walk(QwpBackwardReadAhead readAhead, TestFrame frame) {
        return walk(readAhead, frame, frame.rows);
    }

    // the frames of the first batch: those whose top row goes out before row INITIAL_WINDOW_ROWS
    private static void walkFirstBatch(QwpBackwardReadAhead readAhead, TestFrame frame) {
        for (long streamRows = 0; streamRows < QwpBackwardReadAhead.INITIAL_WINDOW_ROWS; streamRows += frame.rows) {
            Assert.assertEquals(frame.frameBytes(), walk(readAhead, frame));
        }
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
     * Records the ranges the read-ahead asks the OS for, without asking it. It reports every
     * range out of memory, unless a test says otherwise, and the major faults a test sets.
     */
    static final class RecordingFacade extends TestFilesFacadeImpl {
        private final boolean isWillNeedSupported;
        // [lo, hi) address pairs a test reports out of memory, whatever isResident says
        private final LongList missingRanges = new LongList();
        // [lo, hi) address pairs, in request order
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
            ranges.add(address, address + len);
            return 1;
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

        boolean contains(long lo, long hi) {
            for (int i = 0, n = ranges.size(); i < n; i += 2) {
                if (ranges.getQuick(i) == lo && ranges.getQuick(i + 1) == hi) {
                    return true;
                }
            }
            return false;
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
