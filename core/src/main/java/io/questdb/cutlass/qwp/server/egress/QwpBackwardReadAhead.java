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

package io.questdb.cutlass.qwp.server.egress;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypeDriver;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.TestOnly;

/**
 * Reads the frames of a backward page-frame stream ahead of the stream. The stream emits
 * each frame from its last row down, and kernel readahead only follows forward access:
 * walking down over pages that are not cached faults them in one at a time, each fault a
 * synchronous read.
 * <p>
 * Where the OS reads advised pages in the background, see
 * {@link FilesFacade#isMadviseWillNeedSupported()}, the stream keeps a window of the rows
 * below the slice it appends advised with {@code MADV_WILLNEED}, as kernel readahead keeps
 * a window ahead of a forward reader. The first window holds the top
 * {@link #INITIAL_WINDOW_ROWS} rows of the frame, the rows of its first batch. When the
 * stream moves into the last window it advised, or past the middle of the first one, it
 * advises the next window, twice as large, up to {@link #MAX_WINDOW_BYTES} over all its
 * columns. Every row is advised once, the work between two batches stays within one
 * window of requests, and a stream that its client abandons has asked for at most two
 * windows it did not use.
 * <p>
 * Elsewhere it reads the whole frame forward once, before the first slice: the OS reads
 * it in large requests, and the walk down finds it in page cache.
 */
public class QwpBackwardReadAhead implements Mutable {
    // The rows of the first batch: the egress fills a batch with at most this many rows.
    public static final long INITIAL_WINDOW_ROWS = QwpEgressUpgradeProcessor.MAX_ROWS_PER_BATCH;
    public static final long MAX_WINDOW_BYTES = 16L * 1024 * 1024;
    // The egress appends a frame in slices of at most this many rows; a window holds at least one.
    private static final long MIN_WINDOW_ROWS = 1_024;
    // The OS page, the unit the OS reads a mapping in. Files.PAGE_SIZE aligns where a mapping
    // starts: on Windows it is the 64 KiB allocation granularity, sixteen 4 KiB pages.
    private static final long OS_PAGE_SIZE = Unsafe.getUnsafe().pageSize();
    private final IntList columnTypes = new IntList();
    private final FilesFacade ff;
    // the frame's rows [advisedLo, frameRows) have been advised, or read
    private long advisedLo;
    private PageFrame frame;
    private long frameRows;
    private boolean isWillNeedSupported;
    // advise the next window once the stream appends a row below this one
    private long marker;
    private long maxWindowRows;
    // Sum of the bytes the whole-frame read loads. Storing it keeps the JIT from dropping
    // loads whose values nothing else reads; only tests read it back.
    private long touchSink;
    private long windowRows;

    public QwpBackwardReadAhead(FilesFacade ff) {
        this.ff = ff;
    }

    /**
     * Makes sure the rows the stream is about to read are on their way into memory: the
     * slice of the current frame that starts at row {@code lo} and every row above it. Call
     * it before each slice, top slice first. Returns how many bytes it asked the OS to read
     * ahead, or read itself.
     */
    public long advise(long lo) {
        // nothing left to read: no frame, a parquet or empty frame, or every row is advised
        if (advisedLo == 0) {
            return 0;
        }
        if (!isWillNeedSupported) {
            advisedLo = 0;
            return touchFrame();
        }
        // the marker is never below advisedLo, so the slice is advised when it holds no row below the marker
        if (lo >= marker) {
            return 0;
        }
        final boolean isFirstWindow = advisedLo == frameRows;
        if (!isFirstWindow) {
            windowRows = Math.min(maxWindowRows, windowRows * 2);
        }
        // the window always holds the slice itself
        final long windowLo = Math.max(0, Math.min(lo, advisedLo - windowRows));
        marker = isFirstWindow ? advisedLo - windowRows / 2 : advisedLo;
        final long bytes = adviseRows(windowLo, advisedLo);
        advisedLo = windowLo;
        return bytes;
    }

    @Override
    public void clear() {
        columnTypes.clear();
        clearFrame();
    }

    /**
     * Returns the sum of the bytes the whole-frame reads loaded, see {@link #advise(long)}.
     */
    @TestOnly
    public long getTouchSink() {
        return touchSink;
    }

    /**
     * Binds the projected columns of a backward stream, in the order its page frames
     * publish them.
     */
    public void of(RecordMetadata metadata) {
        columnTypes.clear();
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            columnTypes.add(metadata.getColumnType(i));
        }
        clearFrame();
    }

    /**
     * Starts the next frame the stream navigated to, which holds {@code frameRows} rows.
     */
    public void ofFrame(PageFrame frame, long frameRows) {
        this.frame = frame;
        this.frameRows = frameRows;
        // the stream decodes a parquet frame into memory when it navigates to it
        final long rowBytes = frame.getFormat() == PartitionFormat.NATIVE ? fixedRowBytes(frame) : 0;
        if (frameRows <= 0 || rowBytes == 0) {
            advisedLo = 0;
            return;
        }
        isWillNeedSupported = ff.isMadviseWillNeedSupported();
        advisedLo = frameRows;
        marker = frameRows;
        maxWindowRows = windowCap(MAX_WINDOW_BYTES / rowBytes);
        windowRows = Math.min(INITIAL_WINDOW_ROWS, maxWindowRows);
    }

    // where the data of a row starts, from its aux entry; -1 when the entry is outside the
    // published aux extent or points outside the data extent
    private static long dataOffset(ColumnTypeDriver driver, long auxAddress, long auxSize, long pageSize, long row) {
        if (driver.getAuxVectorOffset(row + 1) > auxSize) {
            return -1;
        }
        final long offset = driver.getDataVectorOffset(auxAddress, row);
        return offset >= 0 && offset <= pageSize ? offset : -1;
    }

    /**
     * Loads one byte of every OS page {@code [address, address + size)} touches, in address
     * order, and returns their sum. {@code size} must be positive.
     */
    private static long readPagesForward(long address, long size) {
        long sink = 0;
        for (long p = address, lim = address + size; p < lim; p += OS_PAGE_SIZE) {
            sink += Unsafe.getByte(p);
        }
        // the last byte can sit on one page past the last stride when address is not page-aligned
        return sink + Unsafe.getByte(address + size - 1);
    }

    // advises the frame's rows [rowLo, rowHi) of every column; returns the bytes advised
    private long adviseRows(long rowLo, long rowHi) {
        long advised = 0;
        // the bytes the rows take, for sizing the next windows; a long value counts in full
        long extent = 0;
        for (int ci = 0, n = columnTypes.size(); ci < n; ci++) {
            final int columnType = columnTypes.getQuick(ci);
            final long pageAddress = frame.getPageAddress(ci);
            final long pageSize = frame.getPageSize(ci);
            if (ColumnType.isVarSize(columnType)) {
                final long auxAddress = frame.getAuxPageAddress(ci);
                final long auxSize = frame.getAuxPageSize(ci);
                // a column top publishes no aux entries: the frame stores nothing for it
                if (auxAddress == 0 || auxSize <= 0) {
                    continue;
                }
                final ColumnTypeDriver driver = ColumnType.getDriver(columnType);
                final long auxLo = driver.getAuxVectorOffset(rowLo);
                final long auxHi = rowHi < frameRows ? Math.min(auxSize, driver.getAuxVectorOffset(rowHi)) : auxSize;
                if (auxHi > auxLo) {
                    advised += willNeed(auxAddress + auxLo, auxHi - auxLo);
                    extent += auxHi - auxLo;
                }
                // values that all sit inline in the aux vector leave no data
                if (pageAddress == 0 || pageSize <= 0) {
                    continue;
                }
                final long dataLo = dataOffset(driver, auxAddress, auxSize, pageSize, rowLo);
                final long dataHi = rowHi < frameRows ? dataOffset(driver, auxAddress, auxSize, pageSize, rowHi) : pageSize;
                if (dataLo >= 0 && dataHi > dataLo) {
                    // the walk down reads the top of the range first; a window of very long
                    // values asks for at most the window cap of each column
                    advised += willNeed(pageAddress + Math.max(dataLo, dataHi - MAX_WINDOW_BYTES), Math.min(dataHi - dataLo, MAX_WINDOW_BYTES));
                    extent += dataHi - dataLo;
                }
            } else if (pageAddress != 0 && pageSize > 0) {
                // a type without a size, or rows past the published extent, leave nothing to ask for
                final int size = ColumnType.sizeOf(columnType);
                final long lo = rowLo * size;
                final long hi = Math.min(pageSize, rowHi * size);
                if (hi > lo) {
                    advised += willNeed(pageAddress + lo, hi - lo);
                    extent += hi - lo;
                }
            }
        }
        // size the next windows by what these rows take, long values included
        if (extent > 0) {
            maxWindowRows = windowCap(MAX_WINDOW_BYTES * (rowHi - rowLo) / extent);
        }
        return advised;
    }

    private void clearFrame() {
        frame = null;
        frameRows = 0;
        advisedLo = 0;
    }

    // bytes a row takes in the columns whose size the type tells: fixed-size values and aux
    // entries; zero when the frame stores no column
    private long fixedRowBytes(PageFrame frame) {
        long bytes = 0;
        for (int ci = 0, n = columnTypes.size(); ci < n; ci++) {
            final int columnType = columnTypes.getQuick(ci);
            if (ColumnType.isVarSize(columnType)) {
                if (frame.getAuxPageAddress(ci) != 0 && frame.getAuxPageSize(ci) > 0) {
                    final ColumnTypeDriver driver = ColumnType.getDriver(columnType);
                    bytes += driver.getAuxVectorOffset(1) - driver.getAuxVectorOffset(0);
                }
            } else if (frame.getPageAddress(ci) != 0 && frame.getPageSize(ci) > 0) {
                bytes += Math.max(0, ColumnType.sizeOf(columnType));
            }
        }
        return bytes;
    }

    /**
     * Reads the column pages of the frame once, first byte to last, and returns how many
     * bytes that covered. Each column covers exactly the frame's own bytes: the page extent
     * of a fixed-size column; for a var-size column its aux entries, and its data from where
     * the frame's first row points to the end of the data extent. The page extents of the
     * frame bound every read.
     */
    private long touchFrame() {
        long bytes = 0;
        long sink = 0;
        for (int ci = 0, n = columnTypes.size(); ci < n; ci++) {
            final long pageAddress = frame.getPageAddress(ci);
            final long pageSize = frame.getPageSize(ci);
            final int columnType = columnTypes.getQuick(ci);
            if (ColumnType.isVarSize(columnType)) {
                final long auxAddress = frame.getAuxPageAddress(ci);
                final long auxSize = frame.getAuxPageSize(ci);
                // a column top publishes no aux entries: the frame stores nothing for it
                if (auxAddress != 0 && auxSize > 0) {
                    sink += readPagesForward(auxAddress, auxSize);
                    bytes += auxSize;
                    // The data extent starts at the column's first stored row and ends at the
                    // frame's last row; the frame's own data starts where its first aux entry
                    // points. Values that all sit inline in the aux vector leave no data.
                    if (pageAddress != 0) {
                        final long dataLo = ColumnType.getDriver(columnType).getDataVectorOffset(auxAddress, 0);
                        if (dataLo >= 0 && dataLo < pageSize) {
                            sink += readPagesForward(pageAddress + dataLo, pageSize - dataLo);
                            bytes += pageSize - dataLo;
                        }
                    }
                }
            } else if (pageAddress != 0 && pageSize > 0) {
                sink += readPagesForward(pageAddress, pageSize);
                bytes += pageSize;
            }
        }
        touchSink += sink;
        return bytes;
    }

    private long willNeed(long address, long len) {
        ff.madviseWillNeed(address, len);
        return len;
    }

    private long windowCap(long rows) {
        return Math.max(MIN_WINDOW_ROWS, Math.min(frameRows, rows));
    }
}
