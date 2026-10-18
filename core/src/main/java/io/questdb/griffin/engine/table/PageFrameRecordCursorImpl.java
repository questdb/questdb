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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.sql.RowCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Transient;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicLong;

public class PageFrameRecordCursorImpl extends AbstractPageFrameRecordCursor {
    // @TestOnly observability for the rows the cursor walks one at a time when it sizes itself or skips rows without
    // page frame arithmetic, which no row it returns reflects. A plain static boolean guards it, as in
    // BitmapIndexFwdReader.
    @TestOnly
    public static final AtomicLong testWalkedRows = new AtomicLong();
    @TestOnly
    public static boolean isWalkCounterEnabled = false;
    private final boolean entityCursor;
    private final Function filter;
    private final @Nullable KeyOrderedRowCursorFactory keyOrderedFactory;
    private final LongList passFrameRows = new LongList();
    private final RowCursorFactory rowCursorFactory;
    private boolean areCursorsPrepared;
    private SqlExecutionCircuitBreaker circuitBreaker;
    private boolean isExhausted;
    private int keyCount;
    private int keyHi;
    private int keyLo;
    private long maxRowsAfterSkip = RecordCursor.UNBOUNDED_ROW_COUNT;
    private RowCursor rowCursor;
    private long rowsProducedSinceSkip;

    public PageFrameRecordCursorImpl(
            CairoConfiguration configuration,
            @Transient RecordMetadata metadata,
            RowCursorFactory rowCursorFactory,
            boolean entityCursor,
            // this cursor owns "toTop()" lifecycle of filter
            @Nullable Function filter
    ) {
        super(configuration, metadata);
        this.rowCursorFactory = rowCursorFactory;
        this.keyOrderedFactory = rowCursorFactory instanceof KeyOrderedRowCursorFactory keyOrdered ? keyOrdered : null;
        this.entityCursor = entityCursor;
        this.filter = filter;
    }

    @Override
    public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, RecordCursor.Counter counter) {
        prepareRowCursorFactory();

        // Mirrors the slow-path gate in skipRows(): pushdown pruning drops whole non-matching
        // parquet row groups, so the metadata-only accounting below would count physical rows
        // the cursor never yields and over-report the size. The row-by-row walk counts exactly
        // the rows hasNext() yields, matching the pruned scan.
        if (!frameCursor.supportsSizeCalculation()
                || filter != null
                || rowCursorFactory.isUsingIndex()
                || keyCount > 1 && (keyLo > 0 || frameCount > 0)
                || frameCursor.hasActivePushdownFilter()) {
            long walkedRows = 0;
            while (hasNext()) {
                counter.inc();
                walkedRows++;
            }
            countWalkedRows(walkedRows);
            return;
        }

        if (rowCursor != null) {
            while (rowCursor.hasNext()) {
                rowCursor.next();
                counter.inc();
            }
            rowCursor = Misc.free(rowCursor);
        }

        counter.add(frameCursor.getRemainingRowsInInterval());

        frameCursor.calculateSize(counter);
        isExhausted = true;
    }

    @Override
    public void close() {
        rowCursor = Misc.free(rowCursor);
        super.close();
    }

    public RowCursorFactory getRowCursorFactory() {
        return rowCursorFactory;
    }

    @Override
    public boolean hasNext() {
        if (isExhausted) {
            return false;
        }
        prepareRowCursorFactory();
        try {
            // frames are only decoded up to the cap; rows past it are undecoded memory
            if (rowsProducedSinceSkip >= maxRowsAfterSkip) {
                isExhausted = true;
                return false;
            }
            if (rowCursor != null && rowCursor.hasNext()) {
                final int frameIndex = frameCount - 1;
                final long rowIndex = rowCursor.next();
                frameMemoryPool.navigateTo(frameIndex, recordA);
                recordA.setRowIndex(rowIndex);
                rowsProducedSinceSkip++;
                return true;
            }

            PageFrame frame;
            while ((frame = nextFrame()) != null) {
                // Consult the breaker once per page frame, so a long multi-frame scan stays cancellable.
                // Use the time-throttled variant rather than the count-throttled statefulThrowExceptionIfTripped()
                // (whose 2M-consultation window would skip ~2M frames between real checks, disabling mid-scan
                // cancellation for any realistic table) and rather than the un-throttled variant (which would
                // perform a recv() connection probe on every frame). A nested-loop/cross join re-scans this
                // cursor once per master row, so an un-throttled per-frame probe becomes ~one syscall per
                // master row. The time-throttled variant still checks cancellation/timeout every frame (cheap)
                // while bounding the connection probe to once per wall-clock window for the whole query.
                circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
                frameAddressCache.add(frameCount, frame);
                final long remaining = maxRowsAfterSkip - rowsProducedSinceSkip;
                final long frameSize = frame.getPartitionHi() - frame.getPartitionLo();
                final int inFrameHi = (int) Math.min(Math.min(frameSize, remaining), Integer.MAX_VALUE);
                final PageFrameMemory frameMemory = frameMemoryPool.navigateTo(frameCount++, inFrameHi);
                rowCursor = Misc.free(rowCursor);
                rowCursor = keyCount > 1 ? keyOrderedFactory.getCursor(frame, frameMemory, keyLo, keyHi)
                        : rowCursorFactory.getCursor(frame, frameMemory);
                if (rowCursor.hasNext()) {
                    recordA.init(frameMemory);
                    recordA.setRowIndex(rowCursor.next());
                    rowsProducedSinceSkip++;
                    return true;
                }
            }
        } catch (NoMoreFramesException ignore) {
            isExhausted = true;
            return false;
        }

        isExhausted = true;
        return false;
    }

    @Override
    public boolean isUsingIndex() {
        return rowCursorFactory.isUsingIndex();
    }

    @Override
    public void of(PageFrameCursor frameCursor, SqlExecutionContext sqlExecutionContext) throws SqlException {
        if (this.frameCursor != frameCursor) {
            close();
            this.frameCursor = frameCursor;
        }
        recordA.of(frameCursor);
        recordB.of(frameCursor);
        rowCursorFactory.init(frameCursor, sqlExecutionContext);
        circuitBreaker = sqlExecutionContext.getCircuitBreaker();
        // Consult the breaker at open (time-throttled), so a scan over an empty table (zero frames, so the
        // per-frame check in hasNext never runs) still observes cancellation/timeout even when this cursor is
        // not the query's first breaker consultation. The time-throttled variant checks cancellation/timeout
        // unconditionally (so the count-throttle window can't skip it, unlike statefulThrowExceptionIfTripped())
        // while bounding the connection probe to once per wall-clock window, matching the per-frame check above.
        circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
        areCursorsPrepared = false;
        isExhausted = false;
        rowCursor = Misc.free(rowCursor);
        maxRowsAfterSkip = RecordCursor.UNBOUNDED_ROW_COUNT;
        rowsProducedSinceSkip = 0;
        // prepare for page frame iteration
        super.init(sqlExecutionContext.getMemoryTracker());
    }

    @Override
    public long preComputedStateSize() {
        return RecordCursor.fromBool(areCursorsPrepared);
    }

    @Override
    public long size() {
        // Same gate as calculateSize() and skipRows(): pushdown pruning drops whole
        // non-matching parquet row groups, so frameCursor.size() reports physical rows
        // the cursor never yields. Report unknown size instead of that over-count.
        if (frameCursor.hasActivePushdownFilter()) {
            return -1;
        }
        return entityCursor ? frameCursor.size() : -1;
    }

    @Override
    public void skipRows(Counter rowCount, long requestedMaxRowsAfterSkip) {
        prepareRowCursorFactory();

        // The clamp decodes only the leading [0, n) rows of a parquet frame, so it is
        // sound only for a scan that yields the frame's rows 1:1 in ascending order.
        // isEntity() is that guarantee; a scattered/index row cursor reports false.
        // Pushdown pruning does not forfeit the clamp: a page frame never spans more than
        // one row group, so pruning drops whole frames rather than rows inside a frame,
        // and every frame the scan does yield stays 1:1.
        final boolean canClamp = filter == null && rowCursorFactory.isEntity() && rowCursorFactory.isForwardScan();
        final long postSkipMaxRows = canClamp ? requestedMaxRowsAfterSkip : RecordCursor.UNBOUNDED_ROW_COUNT;
        rowsProducedSinceSkip = 0;

        if (filter == null && keyCount > 1 && !frameCursor.hasActivePushdownFilter()) {
            maxRowsAfterSkip = postSkipMaxRows;
            skipKeyOrderedRows(rowCount);
            return;
        }

        // Use slow path when:
        // - filter is present (need to evaluate each row)
        // - using index (row order may not be sequential)
        // - pushdown pruning is active: the cursor drops whole non-matching parquet row groups,
        //   so the metadata-only frame-size accounting below would count physical rows the cursor
        //   never yields and land the skip short (re-reading already-consumed rows). The row-by-row
        //   walk skips exactly the rows hasNext() yields, matching the pruned scan.
        if (filter != null || rowCursorFactory.isUsingIndex() || frameCursor.hasActivePushdownFilter()) {
            // hasNext() charges every row it yields against the clamp, but the rows this loop
            // walks are the skip itself, not reads after it. So walk unclamped and arm the
            // clamp only once the skip lands, mirroring ReadParquetRecordCursor.isInSkipRows.
            // Clamping the walk lets the first hasNext() find the budget already spent and
            // report exhaustion, which skips nothing whenever postSkipMaxRows is 0 -- the
            // value LimitRecordCursor.calculateSize() passes to size a cursor it will not read.
            maxRowsAfterSkip = RecordCursor.UNBOUNDED_ROW_COUNT;
            final long skipCount = rowCount.get();
            while (rowCount.get() > 0 && hasNext()) {
                rowCount.dec();
            }
            countWalkedRows(skipCount - rowCount.get());
            maxRowsAfterSkip = postSkipMaxRows;
            rowsProducedSinceSkip = 0;
            return;
        }
        maxRowsAfterSkip = postSkipMaxRows;

        // If we're mid-frame after hasNext() calls, exhaust current rowCursor first,
        // then fall through to the fast path for remaining frames
        if (rowCursor != null) {
            while (rowCount.get() > 0 && rowCursor.hasNext()) {
                rowCursor.next();
                rowCount.dec();
            }
            if (rowCount.get() == 0) {
                return;
            }
            rowCursor = Misc.free(rowCursor);
        }

        long skipTarget = rowCount.get();
        PageFrame pageFrame;
        while ((pageFrame = frameCursor.next(skipTarget)) != null) {
            frameAddressCache.add(frameCount++, pageFrame);

            long frameSize = pageFrame.getPartitionHi() - pageFrame.getPartitionLo();
            if (frameSize > skipTarget) {
                rowCount.dec(skipTarget);
                break;
            }
            rowCount.dec(frameSize);
            skipTarget -= frameSize;
        }

        final int frameIndex = frameCount - 1;
        // page frame is null when table has no partitions so there's nothing to skip
        if (pageFrame != null) {
            final long frameSize = pageFrame.getPartitionHi() - pageFrame.getPartitionLo();
            final long roomInFrame = frameSize - skipTarget;
            final long takeFromFrame = Math.min(roomInFrame, maxRowsAfterSkip);
            // The skipped frame prefix is never read, so the decode window starts at
            // the skip landing row; the pool rebases published addresses to keep
            // frame-relative row indexes valid. Only a forward 1:1 scan may do this.
            final int inFrameLo = canClamp ? (int) Math.min(skipTarget, Integer.MAX_VALUE) : 0;
            final int inFrameHi = (int) Math.min(skipTarget + takeFromFrame, Integer.MAX_VALUE);
            final PageFrameMemory frameMemory = frameMemoryPool.navigateTo(frameIndex, inFrameLo, inFrameHi);
            // move to frame, rowlo doesn't matter
            recordA.init(frameMemory);
            recordA.setRowIndex(0);
            rowCursor = Misc.free(rowCursor);
            rowCursor = rowCursorFactory.getCursor(pageFrame, frameMemory);
            rowCursor.jumpTo(skipTarget);
        } else {
            isExhausted = true;
        }
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Page frame scan");
    }

    @Override
    public void toTop() {
        if (filter != null) {
            filter.toTop();
        }
        rowCursor = Misc.free(rowCursor);
        isExhausted = false;
        maxRowsAfterSkip = RecordCursor.UNBOUNDED_ROW_COUNT;
        rowsProducedSinceSkip = 0;
        keyLo = 0;
        keyHi = Math.min(keyCount, 1);
        super.toTop();
    }

    private static void countWalkedRows(long rows) {
        if (isWalkCounterEnabled) {
            testWalkedRows.addAndGet(rows);
        }
    }

    /**
     * The rows of the pass before its page frame at {@code frameIndex}. When the keys of the pass hold every row of
     * its run, those are the rows of the run before the page frame. Otherwise a count reads the posting lists from
     * one of their ends to the page frame, so this counts the rows on the side of the page frame nearer its end of
     * the run.
     */
    private long countRowsBefore(PageFrame frame, int frameIndex, boolean isBackward, long passRowLo, long passRowHi, long passRowCount) {
        final long row = passFrameRows.getQuick(isBackward ? 2 * frameIndex + 1 : 2 * frameIndex);
        final long rowsBelow;
        if (passRowCount == passRowHi - passRowLo) {
            rowsBelow = row - passRowLo;
        } else if (row - passRowLo < passRowHi - row) {
            rowsBelow = passRowCount - keyOrderedFactory.getRowCount(frame, keyLo, keyHi, row, passRowHi - 1);
        } else {
            rowsBelow = keyOrderedFactory.getRowCount(frame, keyLo, keyHi, passRowLo, row - 1);
        }
        return isBackward ? passRowCount - rowsBelow : rowsBelow;
    }

    /**
     * The next page frame. A read ordered by keys passes over the page frames once for each key, or, when there is a
     * single page frame, once for the first key and once for all the others.
     */
    private @Nullable PageFrame nextFrame() {
        PageFrame frame = frameCursor.next();
        while (frame == null && nextKeys()) {
            frame = frameCursor.next();
        }
        return frame;
    }

    /**
     * Moves a read ordered by keys on to the keys after the pass over the page frames that ended: the next key when
     * the pass read more than one page frame, all the remaining keys otherwise.
     */
    private boolean nextKeys() {
        if (keyHi >= keyCount || frameCount == 0) {
            return false;
        }
        keyLo = keyHi;
        keyHi = frameCount > 1 ? keyHi + 1 : keyCount;
        super.toTop();
        return true;
    }

    private void prepareRowCursorFactory() {
        if (!areCursorsPrepared) {
            rowCursorFactory.prepareCursor(frameCursor);
            keyCount = keyOrderedFactory != null ? keyOrderedFactory.getKeyCount() : 0;
            keyLo = 0;
            keyHi = Math.min(keyCount, 1);
            areCursorsPrepared = true;
        }
    }

    /**
     * Moves the frame cursor on to the page frame at {@code frameIndex} of the pass, which the pass has read before,
     * caching the addresses of the page frames up to it.
     */
    private PageFrame seekFrame(int frameIndex) {
        super.toTop();
        PageFrame frame;
        do {
            frame = frameCursor.next();
            frameAddressCache.add(frameCount, frame);
        } while (++frameCount <= frameIndex);
        return frame;
    }

    /**
     * Skips the rows of a read ordered by keys pass by pass, a whole pass at a time when the factory counts the rows of
     * its keys and the pass reads a single run of rows of one partition.
     */
    private void skipKeyOrderedRows(Counter rowCount) {
        if (rowCount.get() == 0) {
            return;
        }
        if (rowCursor != null) {
            skipRowCursorRows(rowCount);
            if (rowCount.get() == 0) {
                return;
            }
            rowCursor = Misc.free(rowCursor);
        }
        do {
            if (frameCount > 0) {
                skipPassFrames(rowCount);
            } else {
                skipPass(rowCount);
            }
        } while (rowCount.get() > 0 && nextKeys());
    }

    /**
     * Skips the rows of the pass that starts at the next page frame, landing in the page frame that holds the row after
     * the skip. A skip shorter than the first page frame walks that page frame before it reads the page frames of the
     * whole pass, since the read after the skip opens that page frame anyway. Otherwise the search for the landing page
     * frame gallops from the end of the pass nearer the landing, so its counts cost no more than the rows up to it.
     */
    private void skipPass(Counter rowCount) {
        PageFrame frame = frameCursor.next();
        if (frame == null) {
            return;
        }
        final long skipCount = rowCount.get();
        if (skipCount < frame.getPartitionHi() - frame.getPartitionLo()) {
            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
            frameAddressCache.add(frameCount, frame);
            rowCursor = keyOrderedFactory.getCursor(frame, frameMemoryPool.navigateTo(frameCount++), keyLo, keyHi);
            skipRowCursorRows(rowCount);
            if (rowCount.get() == 0) {
                return;
            }
            rowCount.add(skipCount - rowCount.get());
            rowCursor = Misc.free(rowCursor);
        }
        super.toTop();
        passFrameRows.clear();
        int partitionIndex = -1;
        boolean isSingleRun = true;
        while ((frame = frameCursor.next()) != null) {
            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
            frameCount++;
            final long rowLo = frame.getPartitionLo();
            final long rowHi = frame.getPartitionHi();
            if (passFrameRows.size() > 0) {
                isSingleRun &= frame.getPartitionIndex() == partitionIndex
                        && (rowLo == passFrameRows.getLast() || rowHi == passFrameRows.getQuick(passFrameRows.size() - 2));
            }
            partitionIndex = frame.getPartitionIndex();
            passFrameRows.add(rowLo, rowHi);
        }
        final int passFrameCount = frameCount;
        frame = seekFrame(0);
        final boolean isBackward = passFrameCount > 1 && passFrameRows.getQuick(2) < passFrameRows.getQuick(0);
        final long passRowLo = passFrameRows.getQuick(isBackward ? 2 * passFrameCount - 2 : 0);
        final long passRowHi = passFrameRows.getQuick(isBackward ? 1 : 2 * passFrameCount - 1);
        final long passRowCount = isSingleRun ? keyOrderedFactory.getRowCount(frame, keyLo, keyHi, passRowLo, passRowHi - 1) : -1;
        if (passRowCount < 0) {
            super.toTop();
            skipPassFrames(rowCount);
            return;
        }
        if (passRowCount <= skipCount) {
            rowCount.dec(passRowCount);
            seekFrame(passFrameCount - 1);
            return;
        }
        int frameIndex = 0;
        long rowsBefore = 0;
        int frameIndexHi = passFrameCount;
        if (skipCount < passRowCount - skipCount) {
            for (int step = 1; frameIndex + step < frameIndexHi; step <<= 1) {
                final long rows = countRowsBefore(frame, frameIndex + step, isBackward, passRowLo, passRowHi, passRowCount);
                if (rows > skipCount) {
                    frameIndexHi = frameIndex + step;
                    break;
                }
                frameIndex += step;
                rowsBefore = rows;
            }
        } else {
            for (int step = 1; frameIndexHi - step > frameIndex; step <<= 1) {
                final long rows = countRowsBefore(frame, frameIndexHi - step, isBackward, passRowLo, passRowHi, passRowCount);
                if (rows <= skipCount) {
                    frameIndex = frameIndexHi - step;
                    rowsBefore = rows;
                    break;
                }
                frameIndexHi -= step;
            }
        }
        while (frameIndexHi - frameIndex > 1) {
            final int mid = (frameIndex + frameIndexHi) >>> 1;
            final long rows = countRowsBefore(frame, mid, isBackward, passRowLo, passRowHi, passRowCount);
            if (rows > skipCount) {
                frameIndexHi = mid;
            } else {
                frameIndex = mid;
                rowsBefore = rows;
            }
        }
        rowCount.dec(rowsBefore);
        frame = seekFrame(frameIndex);
        rowCursor = keyOrderedFactory.getCursor(frame, frameMemoryPool.navigateTo(frameIndex), keyLo, keyHi);
        skipRowCursorRows(rowCount);
    }

    /**
     * Skips the rows of the pass in progress page frame by page frame, a whole page frame at a time when the factory
     * counts its rows, landing in the page frame that holds the row after the skip.
     */
    private void skipPassFrames(Counter rowCount) {
        PageFrame frame;
        while (rowCount.get() > 0 && (frame = frameCursor.next()) != null) {
            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
            frameAddressCache.add(frameCount, frame);
            final long frameRowCount = keyOrderedFactory.getRowCount(frame, keyLo, keyHi, frame.getPartitionLo(), frame.getPartitionHi() - 1);
            if (frameRowCount >= 0 && frameRowCount <= rowCount.get()) {
                frameCount++;
                rowCount.dec(frameRowCount);
                continue;
            }
            rowCursor = keyOrderedFactory.getCursor(frame, frameMemoryPool.navigateTo(frameCount++), keyLo, keyHi);
            skipRowCursorRows(rowCount);
            if (rowCount.get() > 0) {
                rowCursor = Misc.free(rowCursor);
            }
        }
    }

    private void skipRowCursorRows(Counter rowCount) {
        final long skipCount = rowCount.get();
        while (rowCount.get() > 0 && rowCursor.hasNext()) {
            rowCursor.next();
            rowCount.dec();
        }
        countWalkedRows(skipCount - rowCount.get());
    }
}
