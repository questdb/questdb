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
import io.questdb.std.CarrierLocal;
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
 * columns, long values included: when the rows of a window take more, the window starts
 * higher, at the row a binary search over the aux entries of its var-size columns finds,
 * though it always holds the slice and at least one slice of rows. Every byte of every row
 * is advised once, the work between two batches stays within one window of requests, and a
 * stream that its client abandons has asked for at most two windows it did not use.
 * <p>
 * A request costs about a microsecond even when its pages are in memory, a cost that
 * dominates on small frames, so the stream asks only for what is missing. It sends the
 * rows of its first batch without delay; after them it asks mincore(2) whether a window
 * is in memory, see {@link FilesFacade#isPageCacheResident(long, long)}, and advises only
 * the ranges that are not. After a window with missing pages it advises the next 1, 3,
 * 7, up to 63 windows without asking, so a cold stream pays for almost no checks. A check
 * costs about as much as a request, so on small frames, whose short ranges keep a wrong
 * guess cheap, see {@link #SMALL_FRAME_RANGE_BYTES}, the stream stops checking after
 * {@link #TRUST_STREAK} frames in a row turn out to be in memory, and trusts the next
 * ones to be. Before each trusted frame it reads the byte its walk reads first, and goes
 * back to checking as soon as its thread has taken a major page fault, a read from
 * storage, see {@link FilesFacade#getThreadMajorFaults()}, or the stream runs on another
 * thread, as a query fiber may after a yield. A trusted frame that is not in memory
 * after all costs synchronous reads of each range the stream did not ask for, see
 * {@link #SMALL_FRAME_RANGE_BYTES}, and the frames the stream must find in memory before
 * it trusts again double each time.
 * <p>
 * Elsewhere the stream reads the same windows itself, each forward, see
 * {@link FilesFacade#touchPages(long, long)}: the OS reads them in large requests, and the
 * walk down finds them in page cache. A read waits for storage, and running further ahead
 * would gain nothing, so the stream reads a window only when the slice it appends reaches
 * below the rows it has read. The first batch waits for the windows that hold its rows, not
 * for the frame, the reads between two batches stay within the rows of the batch and one
 * window, and a stream that its client abandons has read at most one window it did not use.
 * Reading a page that is in memory costs a load, so the stream checks nothing. It asks the
 * OS for nothing either, so it counts none of these bytes as read ahead.
 */
public class QwpBackwardReadAhead implements Mutable {
    // The rows of the first batch: the egress fills a batch with at most this many rows.
    public static final long INITIAL_WINDOW_ROWS = QwpEgressUpgradeProcessor.MAX_ROWS_PER_BATCH;
    public static final long MAX_WINDOW_BYTES = 16L * 1024 * 1024;
    // A frame is small when no range it publishes, of values, aux entries or data, holds more
    // than this many bytes. Under the default mapping, a walk that faults on a small range
    // reads all of it in one IO: Linux reads half of its default 128 KiB fault read-around
    // below the faulting page. Under MADV_SEQUENTIAL, as QWP egress maps the partitions it
    // opens, Linux reads from the faulting page up, so the walk down reads a page per IO.
    public static final long SMALL_FRAME_RANGE_BYTES = 64 * 1024;
    // small frames in a row that must turn out to be in memory before the stream trusts the next ones
    public static final int TRUST_STREAK = 8;
    // An object per carrier, the OS thread that runs the stream, see CarrierLocal. A query fiber
    // can resume on another carrier after a yield, and compiled code can then still see the
    // Thread of the carrier it yielded on, see CARRIER_LOCAL.md; this object names the carrier
    // that runs it now. A thread that does not bind as a carrier has an object of its own.
    private static final CarrierLocal<Object> CARRIER = new CarrierLocal<>(Object::new);
    // after a window with missing pages, the stream advises up to 2^MAX_COLD_RUN - 1 windows without checking
    private static final int MAX_COLD_RUN = 6;
    private static final int MAX_TRUST_STREAK = 1 << 20;
    // The egress appends a frame in slices of at most this many rows; a window holds at least one.
    private static final long MIN_WINDOW_ROWS = 1_024;
    private final IntList columnTypes = new IntList();
    private final FilesFacade ff;
    // indexes of the var-size columns among the projected ones
    private final IntList varSizeColumns = new IntList();
    // the frame's rows [advisedLo, frameRows) have been advised, or read
    private long advisedLo;
    // The aux entries of the frame's rows [auxAdvisedLo, frameRows) have been advised. A window
    // asks for the aux entries of all the rows it may hold before it fits itself to the byte
    // cap, which reads them; the next window does not ask for those again.
    private long auxAdvisedLo;
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
    // bytes a row of the frame takes in the columns whose size the type tells, see fixedRowBytes()
    private long fixedRowBytes;
    // advise the next window once the stream appends a row below this one
    private long marker;
    private long maxWindowRows;
    // the byte the walk of the current small frame reads first
    private long probeAddress;
    // rows of the frames the stream has navigated to
    private long streamRows;
    // Sum of the bytes the stream loads itself. Storing it keeps the JIT from dropping loads
    // whose values nothing else reads; only tests read it back.
    private long touchSink;
    // the carrier the stream trusts, see CARRIER
    private Object trustedCarrier;
    // the major faults of the trusted carrier when the stream started trusting
    private long trustedMajorFaults;
    // small frames in a row found in memory, and how many the stream needs to trust the next ones
    private int trustStreak;
    private int trustStreakNeeded = TRUST_STREAK;
    // windows left to advise without checking
    private int uncheckedWindows;
    // where the window that adviseRows() advised last starts
    private long windowLo;
    private long windowRows;

    public QwpBackwardReadAhead(FilesFacade ff) {
        this.ff = ff;
    }

    /**
     * Makes sure the rows the stream is about to read are on their way into memory: the
     * slice of the current frame that starts at row {@code lo} and every row above it. Call
     * it before each slice, top slice first. Returns how many bytes it asked the OS to read
     * ahead; ranges it finds in memory, which it does not ask for, do not count. Where the OS
     * does not read advised pages, the stream reads the next window itself when the slice
     * reaches below the rows it has read, which waits for storage, asks the OS for nothing, and
     * returns 0.
     */
    public long advise(long lo) {
        // nothing left to read: no frame, a parquet, custom or empty frame, or every row is advised
        if (advisedLo == 0) {
            return 0;
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
        final long rowLo = Math.max(0, Math.min(lo, advisedLo - windowRows));
        // Long values can take more than the byte cap: the window then starts higher, never
        // above the slice or less than a slice below advisedLo. A small frame goes in one window.
        final long maxWindowLo = isSmallFrame || varSizeColumns.size() == 0
                ? rowLo
                : Math.max(rowLo, Math.min(lo, advisedLo - MIN_WINDOW_ROWS));
        // a window the stream trusts to be in memory advises nothing, and starts at rowLo
        windowLo = rowLo;
        final long bytes = adviseWindow(rowLo, advisedLo, maxWindowLo);
        if (windowLo > rowLo) {
            // the next windows double from the rows this one holds
            windowRows = advisedLo - windowLo;
        }
        // Advice goes out a window ahead, so the OS has the rows read by the time the walk
        // gets to them. A read waits for storage, so the stream reads the next window only
        // when the walk needs its rows.
        if (isWillNeedSupported) {
            marker = isFirstWindow ? advisedLo - windowRows / 2 : advisedLo;
        } else {
            marker = windowLo;
        }
        advisedLo = windowLo;
        return bytes;
    }

    @Override
    public void clear() {
        columnTypes.clear();
        varSizeColumns.clear();
        clearFrame();
        clearStream();
    }

    /**
     * Returns the sum of the bytes the stream loaded itself: of the windows it read, where
     * the OS does not read advised pages, and of the bytes it read before trusted frames.
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
        varSizeColumns.clear();
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            final int columnType = metadata.getColumnType(i);
            columnTypes.add(columnType);
            if (ColumnType.isVarSize(columnType)) {
                varSizeColumns.add(i);
            }
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
        // The stream decodes a parquet frame into memory when it navigates to it, and
        // materializes a custom (Delta) frame, whose addresses point at its base, not at its rows.
        final long rowBytes = frame.getFormat() == PartitionFormat.NATIVE && frame.getPartitionFrameState() == 0
                ? fixedRowBytes(frame)
                : 0;
        if (frameRows <= 0 || rowBytes == 0) {
            advisedLo = 0;
            return;
        }
        isWillNeedSupported = ff.isMadviseWillNeedSupported();
        advisedLo = frameRows;
        auxAdvisedLo = frameRows;
        marker = frameRows;
        fixedRowBytes = rowBytes;
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

    // Advises the aux entries of the frame's rows [rowLo, auxAdvisedLo) of every var-size column,
    // those of the rows from rowLo up that no window has asked for; returns the bytes advised.
    private long adviseAux(long rowLo, boolean isChecking) {
        if (rowLo >= auxAdvisedLo) {
            return 0;
        }
        long advised = 0;
        for (int i = 0, n = varSizeColumns.size(); i < n; i++) {
            final int ci = varSizeColumns.getQuick(i);
            final long auxAddress = frame.getAuxPageAddress(ci);
            final long auxSize = frame.getAuxPageSize(ci);
            // a column top publishes no aux entries: the frame stores nothing for it
            if (auxAddress != 0 && auxSize > 0) {
                final ColumnTypeDriver driver = ColumnType.getDriver(columnTypes.getQuick(ci));
                final long auxLo = driver.getAuxVectorOffset(rowLo);
                final long auxHi = auxAdvisedLo < frameRows ? Math.min(auxSize, driver.getAuxVectorOffset(auxAdvisedLo)) : auxSize;
                if (auxHi > auxLo) {
                    advised += adviseRange(auxAddress + auxLo, auxHi - auxLo, isChecking);
                }
            }
        }
        auxAdvisedLo = rowLo;
        return advised;
    }

    // Advises [address, address + len); when checking, only if any of its pages is not in memory.
    // Where the OS does not read advised pages, reads the range instead, and returns 0: the
    // stream asked the OS for nothing.
    private long adviseRange(long address, long len, boolean isChecking) {
        if (!isWillNeedSupported) {
            touchSink += ff.touchPages(address, len);
            return 0;
        }
        if (isChecking) {
            if (ff.isPageCacheResident(address, len)) {
                return 0;
            }
            hasMissingPages = true;
        }
        ff.madviseWillNeed(address, len);
        return len;
    }

    // Advises a window of the frame that ends at row rowHi, every byte of every column, and sets
    // windowLo to where it starts: at rowLo, or at the lowest row up to maxWindowLo from which
    // the rows fit MAX_WINDOW_BYTES, or at maxWindowLo when none does. Returns the bytes advised.
    // When it checks, it skips the ranges that are in memory, and tells whether any was not.
    private long adviseRows(long rowLo, long rowHi, long maxWindowLo, boolean isChecking) {
        hasMissingPages = false;
        // the aux entries go first: they tell where the data of the rows starts, which fitting
        // the window reads
        long advised = adviseAux(rowLo, isChecking);
        windowLo = maxWindowLo > rowLo ? fitWindow(rowLo, rowHi, maxWindowLo) : rowLo;
        // the bytes the rows take, for sizing the next windows
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
                final long auxLo = driver.getAuxVectorOffset(windowLo);
                final long auxHi = rowHi < frameRows ? Math.min(auxSize, driver.getAuxVectorOffset(rowHi)) : auxSize;
                if (auxHi > auxLo) {
                    extent += auxHi - auxLo;
                }
                // values that all sit inline in the aux vector leave no data
                if (pageAddress == 0 || pageSize <= 0) {
                    continue;
                }
                final long dataLo = dataOffset(driver, auxAddress, auxSize, pageSize, windowLo);
                final long dataHi = rowHi < frameRows ? dataOffset(driver, auxAddress, auxSize, pageSize, rowHi) : pageSize;
                if (dataLo >= 0 && dataHi > dataLo) {
                    advised += adviseRange(pageAddress + dataLo, dataHi - dataLo, isChecking);
                    extent += dataHi - dataLo;
                }
            } else if (pageAddress != 0 && pageSize > 0) {
                // a type without a size, or rows past the published extent, leave nothing to ask for
                final int size = ColumnType.sizeOf(columnType);
                final long lo = windowLo * size;
                final long hi = Math.min(pageSize, rowHi * size);
                if (hi > lo) {
                    advised += adviseRange(pageAddress + lo, hi - lo, isChecking);
                    extent += hi - lo;
                }
            }
        }
        // size the next windows by what these rows take, long values included
        if (extent > 0) {
            maxWindowRows = windowCap(MAX_WINDOW_BYTES * (rowHi - windowLo) / extent);
        }
        return advised;
    }

    // Advises a small frame, unless the stream trusts it to be in memory, and decides whether
    // to trust the next ones.
    private long adviseSmallFrame() {
        if (isTrusted) {
            // The OS counts the faults of each OS thread, and a stream can move to another one: a
            // parked stream resumes on any worker, and a query fiber on any carrier after a yield.
            if (CARRIER.get() == trustedCarrier) {
                // Read the byte the walk reads first: when the frame is not in memory, this
                // faults, a read from storage that the major faults of the thread count.
                touchSink += Unsafe.getByte(probeAddress);
                if (ff.getThreadMajorFaults() == trustedMajorFaults) {
                    return 0;
                }
                // this frame, or the walk of an earlier one, read from storage
                trustStreakNeeded = (int) Math.min(MAX_TRUST_STREAK, 2L * trustStreakNeeded);
            }
            // a stream that moved to another carrier cannot tell whether it read from storage
            isTrusted = false;
            trustStreak = 0;
        }
        final boolean isChecked = uncheckedWindows == 0;
        final long bytes = adviseUnlessResident(0, frameRows, 0);
        if (!isChecked || hasMissingPages) {
            trustStreak = 0;
        } else if (++trustStreak >= trustStreakNeeded) {
            final long majorFaults = ff.getThreadMajorFaults();
            if (majorFaults >= 0) {
                isTrusted = true;
                trustedCarrier = CARRIER.get();
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
    private long adviseUnlessResident(long rowLo, long rowHi, long maxWindowLo) {
        if (uncheckedWindows > 0) {
            uncheckedWindows--;
            return adviseRows(rowLo, rowHi, maxWindowLo, false);
        }
        final long bytes = adviseRows(rowLo, rowHi, maxWindowLo, true);
        if (hasMissingPages) {
            coldRun = Math.min(MAX_COLD_RUN, coldRun + 1);
            uncheckedWindows = (1 << coldRun) - 1;
        } else {
            coldRun = 0;
        }
        return bytes;
    }

    // advises the window of the current frame that ends at rowHi and starts in [rowLo, maxWindowLo]
    private long adviseWindow(long rowLo, long rowHi, long maxWindowLo) {
        // The first batch goes out as soon as its rows are in memory: the first window of a frame
        // whose top row goes out in it asks for them unchecked. A batch whose rows take more than
        // a window, of long values or wide rows, checks its later windows as any window after it.
        // A stream that reads its windows itself checks none: a page in memory costs it a load.
        if (!isWillNeedSupported || (rowHi == frameRows && frameStreamLo < INITIAL_WINDOW_ROWS)) {
            return adviseRows(rowLo, rowHi, maxWindowLo, false);
        }
        return isSmallFrame ? adviseSmallFrame() : adviseUnlessResident(rowLo, rowHi, maxWindowLo);
    }

    private void clearFrame() {
        frame = null;
        frameRows = 0;
        advisedLo = 0;
        auxAdvisedLo = 0;
        isSmallFrame = false;
    }

    private void clearStream() {
        streamRows = 0;
        isTrusted = false;
        trustedCarrier = null;
        trustStreak = 0;
        trustStreakNeeded = TRUST_STREAK;
        coldRun = 0;
        uncheckedWindows = 0;
    }

    // The lowest row r in [rowLo, maxWindowLo] from which the frame's rows [r, rowHi) take at most
    // MAX_WINDOW_BYTES, or maxWindowLo when none does. The bytes fall as r rises, so a binary
    // search finds r; the aux entries it reads have been advised.
    private long fitWindow(long rowLo, long rowHi, long maxWindowLo) {
        if (windowBytes(rowLo, rowHi) <= MAX_WINDOW_BYTES) {
            return rowLo;
        }
        // the rows from lo up take more than the cap; those from hi up fit, unless hi is maxWindowLo
        long lo = rowLo;
        long hi = maxWindowLo;
        while (hi - lo > 1) {
            final long mid = (lo + hi) >>> 1;
            if (windowBytes(mid, rowHi) <= MAX_WINDOW_BYTES) {
                hi = mid;
            } else {
                lo = mid;
            }
        }
        return hi;
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

    // Bytes the frame's rows [rowLo, rowHi) take over all columns, as adviseRows() asks for them:
    // values and aux entries, and the data of var-size columns. Data whose offset an aux entry
    // does not tell counts as none.
    private long windowBytes(long rowLo, long rowHi) {
        long bytes = fixedRowBytes * (rowHi - rowLo);
        for (int i = 0, n = varSizeColumns.size(); i < n; i++) {
            final int ci = varSizeColumns.getQuick(i);
            final long pageAddress = frame.getPageAddress(ci);
            final long pageSize = frame.getPageSize(ci);
            final long auxAddress = frame.getAuxPageAddress(ci);
            final long auxSize = frame.getAuxPageSize(ci);
            if (pageAddress != 0 && pageSize > 0 && auxAddress != 0 && auxSize > 0) {
                final ColumnTypeDriver driver = ColumnType.getDriver(columnTypes.getQuick(ci));
                final long dataLo = dataOffset(driver, auxAddress, auxSize, pageSize, rowLo);
                final long dataHi = rowHi < frameRows ? dataOffset(driver, auxAddress, auxSize, pageSize, rowHi) : pageSize;
                if (dataLo >= 0 && dataHi > dataLo) {
                    bytes += dataHi - dataLo;
                }
            }
        }
        return bytes;
    }

    private long windowCap(long rows) {
        return Math.max(MIN_WINDOW_ROWS, Math.min(frameRows, rows));
    }
}
