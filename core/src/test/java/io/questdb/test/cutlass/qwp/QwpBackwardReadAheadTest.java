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
 * every column is asked for once, before the stream reads it. Without it the stream reads
 * the whole frame once, before the first slice.
 */
public class QwpBackwardReadAheadTest {
    // the egress walks a frame down in slices of at most this many rows
    private static final int SLICE_ROWS = 1_024;

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

    private static GenericRecordMetadata metadata(int[] types) {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        for (int i = 0; i < types.length; i++) {
            // the short constructor refuses SYMBOL columns, which need symbol table parameters
            metadata.add(new TableColumnMetadata("c" + i, types[i], IndexType.NONE, 0, false, null));
        }
        return metadata;
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
     * Records the ranges the read-ahead asks the OS for, without asking it.
     */
    static final class RecordingFacade extends TestFilesFacadeImpl {
        private final boolean isWillNeedSupported;
        // [lo, hi) address pairs, in request order
        private final LongList ranges = new LongList();

        RecordingFacade(boolean isWillNeedSupported) {
            this.isWillNeedSupported = isWillNeedSupported;
        }

        @Override
        public boolean isMadviseWillNeedSupported() {
            return isWillNeedSupported;
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
