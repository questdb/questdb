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
 * A request costs about a microsecond even when its pages are in memory, a cost that
 * dominates on small frames, so the stream asks only for what is missing. It sends the
 * rows of its first batch without delay; after them it asks mincore(2) whether a window
 * is in memory, see {@link FilesFacade#isPageCacheResident(long, long)}, and advises only
 * the ranges that are not. After a window with missing pages it advises the next 1, 3,
 * 7, up to 63 windows without asking, so a cold stream pays for almost no checks. A check
 * costs about as much as a request, so on small frames, whose every range one fault reads
 * in a single IO, see {@link #SMALL_FRAME_RANGE_BYTES}, the stream stops checking after
 * {@link #TRUST_STREAK} frames in a row turn out to be in memory, and trusts the next
 * ones to be. Before each trusted frame it reads the byte its walk reads first, and goes
 * back to checking as soon as its thread has taken a major page fault, a read from
 * storage, see {@link FilesFacade#getThreadMajorFaults()}, or runs on another thread. A
 * trusted frame that is not in memory after all costs a synchronous read of each range
 * the stream did not ask for, one IO each, and the frames the stream must find in memory
 * before it trusts again double each time.
 * <p>
 * Elsewhere it reads the whole frame forward once, before the first slice: the OS reads
 * it in large requests, and the walk down finds it in page cache.
 */
public class QwpBackwardReadAhead implements Mutable {
    // The rows of the first batch: the egress fills a batch with at most this many rows.
    public static final long INITIAL_WINDOW_ROWS = QwpEgressUpgradeProcessor.MAX_ROWS_PER_BATCH;
    public static final long MAX_WINDOW_BYTES = 16L * 1024 * 1024;
    // A frame is small when no range it publishes, of values, aux entries or data, holds more
    // than this many bytes. A walk that faults on a small range reads all of it in one IO:
    // Linux reads half of its default 128 KiB fault read-around below the faulting page.
    public static final long SMALL_FRAME_RANGE_BYTES = 64 * 1024;
    // small frames in a row that must turn out to be in memory before the stream trusts the next ones
    public static final int TRUST_STREAK = 8;
    // after a window with missing pages, the stream advises up to 2^MAX_COLD_RUN - 1 windows without checking
    private static final int MAX_COLD_RUN = 6;
    private static final int MAX_TRUST_STREAK = 1 << 20;
    // The egress appends a frame in slices of at most this many rows; a window holds at least one.
    private static final long MIN_WINDOW_ROWS = 1_024;
    // The OS page, the unit the OS reads a mapping in. Files.PAGE_SIZE aligns where a mapping
    // starts: on Windows it is the 64 KiB allocation granularity, sixteen 4 KiB pages.
    private static final long OS_PAGE_SIZE = Unsafe.getUnsafe().pageSize();
    private final IntList columnTypes = new IntList();
    private final FilesFacade ff;
    // the frame's rows [advisedLo, frameRows) have been advised, or read
    private long advisedLo;
    // windows with missing pages in a row, which sets how many windows go out unchecked
    private int coldRun;
    private PageFrame frame;
    private long frameRows;
    // where the frame's top row goes out in the stream: the rows of the frames before it
    private long frameStreamLo;
    // the last check found pages that are not in memory
    private boolean hasMissingPages;
    private boolean isSmallFrame;
    private boolean isTrusted;
    private boolean isWillNeedSupported;
    // advise the next window once the stream appends a row below this one
    private long marker;
    private long maxWindowRows;
    // the byte the walk of the current small frame reads first
    private long probeAddress;
    // rows of the frames the stream has navigated to
    private long streamRows;
    // Sum of the bytes the whole-frame read loads. Storing it keeps the JIT from dropping
    // loads whose values nothing else reads; only tests read it back.
    private long touchSink;
    // the major faults of the trusted thread when the stream started trusting
    private long trustedMajorFaults;
    private Thread trustedThread;
    // small frames in a row found in memory, and how many the stream needs to trust the next ones
    private int trustStreak;
    private int trustStreakNeeded = TRUST_STREAK;
    // windows left to advise without checking
    private int uncheckedWindows;
    private long windowRows;

    public QwpBackwardReadAhead(FilesFacade ff) {
        this.ff = ff;
    }

    /**
     * Makes sure the rows the stream is about to read are on their way into memory: the
     * slice of the current frame that starts at row {@code lo} and every row above it. Call
     * it before each slice, top slice first. Returns how many bytes it asked the OS to read
     * ahead, or read itself; bytes already in memory do not count.
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
        final long bytes = adviseWindow(windowLo, advisedLo);
        advisedLo = windowLo;
        return bytes;
    }

    @Override
    public void clear() {
        columnTypes.clear();
        clearFrame();
        clearStream();
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
        clearStream();
    }

    /**
     * Starts the next frame the stream navigated to, which holds {@code frameRows} rows.
     */
    public void ofFrame(PageFrame frame, long frameRows) {
        this.frame = frame;
        this.frameRows = frameRows;
        frameStreamLo = streamRows;
        streamRows += Math.max(0, frameRows);
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
        isSmallFrame = isWillNeedSupported && isSmall(frame);
        if (isSmallFrame) {
            // the stream trusts or checks a small frame as a whole, in one window
            windowRows = frameRows;
        }
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

    // Advises the frame's rows [rowLo, rowHi) of every column; returns the bytes advised. When
    // it checks, it skips the ranges that are in memory, and tells whether any was not.
    private long adviseRows(long rowLo, long rowHi, boolean isChecking) {
        hasMissingPages = false;
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
                    advised += willNeed(auxAddress + auxLo, auxHi - auxLo, isChecking);
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
                    advised += willNeed(pageAddress + Math.max(dataLo, dataHi - MAX_WINDOW_BYTES), Math.min(dataHi - dataLo, MAX_WINDOW_BYTES), isChecking);
                    extent += dataHi - dataLo;
                }
            } else if (pageAddress != 0 && pageSize > 0) {
                // a type without a size, or rows past the published extent, leave nothing to ask for
                final int size = ColumnType.sizeOf(columnType);
                final long lo = rowLo * size;
                final long hi = Math.min(pageSize, rowHi * size);
                if (hi > lo) {
                    advised += willNeed(pageAddress + lo, hi - lo, isChecking);
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

    // Advises a small frame, unless the stream trusts it to be in memory, and decides whether
    // to trust the next ones.
    private long adviseSmallFrame() {
        if (isTrusted) {
            // QWP workers are platform threads: the faults of the thread are those of its OS thread
            if (Thread.currentThread() == trustedThread) {
                // Read the byte the walk reads first: when the frame is not in memory, this
                // faults, and one IO reads the range around the byte.
                touchSink += Unsafe.getByte(probeAddress);
                if (ff.getThreadMajorFaults() == trustedMajorFaults) {
                    return 0;
                }
                // this frame, or the walk of an earlier one, read from storage
                trustStreakNeeded = (int) Math.min(MAX_TRUST_STREAK, 2L * trustStreakNeeded);
            }
            // a stream that moved to another thread cannot tell whether it read from storage
            isTrusted = false;
            trustStreak = 0;
        }
        final boolean isChecked = uncheckedWindows == 0;
        final long bytes = adviseUnlessResident(0, frameRows);
        if (!isChecked || hasMissingPages) {
            trustStreak = 0;
        } else if (++trustStreak >= trustStreakNeeded) {
            final long majorFaults = ff.getThreadMajorFaults();
            if (majorFaults >= 0) {
                isTrusted = true;
                trustedThread = Thread.currentThread();
                trustedMajorFaults = majorFaults;
            } else {
                // without the thread's faults the stream cannot tell when to stop trusting
                trustStreakNeeded = MAX_TRUST_STREAK;
            }
        }
        return bytes;
    }

    // Advises the ranges of the frame's rows [rowLo, rowHi) that are not in memory. After a
    // window with missing pages, advises the next windows without checking, twice as many
    // after each such window in a row.
    private long adviseUnlessResident(long rowLo, long rowHi) {
        if (uncheckedWindows > 0) {
            uncheckedWindows--;
            return adviseRows(rowLo, rowHi, false);
        }
        final long bytes = adviseRows(rowLo, rowHi, true);
        if (hasMissingPages) {
            coldRun = Math.min(MAX_COLD_RUN, coldRun + 1);
            uncheckedWindows = (1 << coldRun) - 1;
        } else {
            coldRun = 0;
        }
        return bytes;
    }

    // advises the window [rowLo, rowHi) of the current frame
    private long adviseWindow(long rowLo, long rowHi) {
        // the first batch goes out as soon as its rows are in memory: ask for them unchecked
        if (frameStreamLo + frameRows - rowHi < INITIAL_WINDOW_ROWS) {
            return adviseRows(rowLo, rowHi, false);
        }
        return isSmallFrame ? adviseSmallFrame() : adviseUnlessResident(rowLo, rowHi);
    }

    private void clearFrame() {
        frame = null;
        frameRows = 0;
        advisedLo = 0;
        isSmallFrame = false;
    }

    private void clearStream() {
        streamRows = 0;
        isTrusted = false;
        trustedThread = null;
        trustStreak = 0;
        trustStreakNeeded = TRUST_STREAK;
        coldRun = 0;
        uncheckedWindows = 0;
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

    // Whether no range the frame publishes, of values, aux entries or data, holds more than
    // SMALL_FRAME_RANGE_BYTES; a column top publishes none. Points probeAddress at the last
    // byte of the first range of values or aux entries, which the walk down reads first. A
    // frame with row bytes publishes one.
    private boolean isSmall(PageFrame frame) {
        probeAddress = 0;
        for (int ci = 0, n = columnTypes.size(); ci < n; ci++) {
            final long pageAddress = frame.getPageAddress(ci);
            final long pageSize = frame.getPageSize(ci);
            if (pageAddress != 0 && pageSize > SMALL_FRAME_RANGE_BYTES) {
                return false;
            }
            if (ColumnType.isVarSize(columnTypes.getQuick(ci))) {
                final long auxAddress = frame.getAuxPageAddress(ci);
                final long auxSize = frame.getAuxPageSize(ci);
                if (auxAddress != 0 && auxSize > SMALL_FRAME_RANGE_BYTES) {
                    return false;
                }
                if (probeAddress == 0 && auxAddress != 0 && auxSize > 0) {
                    probeAddress = auxAddress + auxSize - 1;
                }
            } else if (probeAddress == 0 && pageAddress != 0 && pageSize > 0) {
                probeAddress = pageAddress + pageSize - 1;
            }
        }
        return true;
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

    // advises [address, address + len); when checking, only if any of its pages is not in memory
    private long willNeed(long address, long len, boolean isChecking) {
        if (isChecking) {
            if (ff.isPageCacheResident(address, len)) {
                return 0;
            }
            hasMissingPages = true;
        }
        ff.madviseWillNeed(address, len);
        return len;
    }

    private long windowCap(long rows) {
        return Math.max(MIN_WINDOW_ROWS, Math.min(frameRows, rows));
    }
}
