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
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.sql.RowCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import io.questdb.std.Rows;
import io.questdb.std.Transient;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicLong;

public class PageFrameRecordCursorImpl extends AbstractPageFrameRecordCursor {
    // @TestOnly observability for the page frames the cursor opens a row cursor over, which shows whether a read ordered
    // by keys walks the page frames of a key that has no row there.
    @TestOnly
    public static final AtomicLong testReadFrames = new AtomicLong();
    // @TestOnly observability for the rows the cursor walks one at a time when it sizes itself or skips rows without
    // page frame arithmetic, which no row it returns reflects. A plain static boolean guards both counters, as in
    // BitmapIndexFwdReader.
    @TestOnly
    public static final AtomicLong testWalkedRows = new AtomicLong();
    @TestOnly
    public static boolean isWalkCounterEnabled = false;
    private final boolean entityCursor;
    private final Function filter;
    private final @Nullable KeyOrderedRowCursorFactory keyOrderedFactory;
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
    // The number of page frames of the scan, known once a pass of a read ordered by keys reads them all, -1 before.
    private int scanFrameCount = -1;

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

            PageFrame frame = nextFrame();
            while (frame != null) {
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
                PageFrameMemory frameMemory = frame.getFormat() == PartitionFormat.PARQUET && isKeyOrderedByIndex()
                        ? null : frameMemoryPool.navigateTo(frameCount, inFrameHi);
                frameCount++;
                rowCursor = Misc.free(rowCursor);
                rowCursor = keyCount > 1 ? keyOrderedFactory.getCursor(frame, frameMemory, keyLo, keyHi)
                        : rowCursorFactory.getCursor(frame, frameMemory);
                countReadFrame();
                if (rowCursor.hasNext()) {
                    if (frameMemory == null) {
                        frameMemory = frameMemoryPool.navigateTo(frameCount - 1, inFrameHi);
                    }
                    recordA.init(frameMemory);
                    recordA.setRowIndex(rowCursor.next());
                    rowsProducedSinceSkip++;
                    return true;
                }
                frame = (frameCount == 1 || frame.getFormat() == PartitionFormat.PARQUET)
                        && keyHi - keyLo == 1 && isKeyOrderedByIndex() ? nextKeyFrame(frame) : nextFrame();
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
        scanFrameCount = -1;
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

        if (isKeyOrderedByIndex()) {
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
        scanFrameCount = -1;
        super.toTop();
    }

    private static void countReadFrame() {
        if (isWalkCounterEnabled) {
            testReadFrames.incrementAndGet();
        }
    }

    private static void countWalkedRows(long rows) {
        if (isWalkCounterEnabled) {
            testWalkedRows.addAndGet(rows);
        }
    }

    private int findCachedFrame(int partitionIndex, long row) {
        final long rowId = Rows.toRowID(partitionIndex, row);
        final boolean isBackward = isBackward();
        int lo = frameCount;
        int hi = frameAddressCache.getFrameCount() - 1;
        while (lo <= hi) {
            final int mid = (lo + hi) >>> 1;
            final long frameRowId = frameAddressCache.getRowIdOffset(mid);
            if (isBackward ? frameRowId + frameAddressCache.getFrameSize(mid) - 1 >= rowId : frameRowId <= rowId) {
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return hi;
    }

    /**
     * The row after a skip of {@code rowIndex} rows of the key among the rows {@code rowLo} to {@code rowHi}, where it
     * has {@code count} rows, more than the skip: the index finds it, unless the key holds every row there, which places
     * it by arithmetic.
     */
    private long getLandingRow(PageFrame frame, int key, long rowLo, long rowHi, long count, long rowIndex) {
        if (count == rowHi - rowLo + 1) {
            return isBackward() ? rowHi - rowIndex : rowLo + rowIndex;
        }
        return keyOrderedFactory.getRow(frame, key, rowLo, rowHi, rowIndex);
    }

    private boolean isBackward() {
        return keyOrderedFactory.getIndexDirection() == IndexReader.DIR_BACKWARD;
    }

    // The rows of a key-ordered read are the rows the index holds for its keys only when no filter drops some of them
    // and no pushdown filter prunes the row groups that hold them.
    private boolean isKeyOrderedByIndex() {
        return keyCount > 1 && filter == null && !frameCursor.hasActivePushdownFilter();
    }

    /**
     * The next page frame. A read ordered by keys passes over the page frames once for each key, or, when the scan
     * reads a single page frame, once for the first key and once for all the others.
     */
    private @Nullable PageFrame nextFrame() {
        PageFrame frame = frameCursor.next();
        while (frame == null) {
            scanFrameCount = frameCount;
            if (!nextKeys()) {
                return null;
            }
            frame = frameCursor.next();
        }
        return frame;
    }

    /**
     * The next page frame after an empty frame of a single key pass. Within the current interval the index finds the
     * next row of the key, or the last frame when no key rows remain. Only the first frame can rewind to probe later
     * intervals as whole partition frames, so an absent key reads one page frame and probes once per interval.
     */
    private @Nullable PageFrame nextKeyFrame(PageFrame frame) {
        final long restRowCount = frameCursor.getRemainingRowsInInterval();
        if (restRowCount > 0) {
            final long restLo = isBackward() ? frame.getPartitionLo() - restRowCount : frame.getPartitionHi();
            final long restHi = restLo + restRowCount - 1;
            final long count = keyOrderedFactory.getRowCount(frame, keyLo, keyHi, restLo, restHi, 0);
            if (count < 0) {
                return nextFrame();
            }
            if (count > 0) {
                return seekFrame(frame.getPartitionIndex(), keyOrderedFactory.getRow(frame, keyLo, restLo, restHi, 0));
            }
            if (frameCount > 1) {
                return seekFrame(frame.getPartitionIndex(), isBackward() ? restLo : restHi);
            }
        }
        if (frameCount > 1) {
            return nextFrame();
        }
        super.toTop();
        frameCursor.next(Long.MAX_VALUE);
        PageFrame partitionFrame = frameCursor.next(Long.MAX_VALUE);
        if (partitionFrame == null && restRowCount == 0) {
            scanFrameCount = 1;
        }
        while (partitionFrame != null) {
            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
            final long rowLo = partitionFrame.getPartitionLo();
            final long rowHi = partitionFrame.getPartitionHi() - 1;
            final long count = keyOrderedFactory.getRowCount(partitionFrame, keyLo, keyHi, rowLo, rowHi, 0);
            if (count != 0) {
                final int partitionIndex = partitionFrame.getPartitionIndex();
                final long row = count > 0 ? keyOrderedFactory.getRow(partitionFrame, keyLo, rowLo, rowHi, 0) : isBackward() ? rowHi : rowLo;
                super.toTop();
                return seekFrame(partitionIndex, row);
            }
            partitionFrame = frameCursor.next(Long.MAX_VALUE);
        }
        return nextKeys() ? nextFrame() : null;
    }

    /**
     * Moves a read ordered by keys on to the keys after the pass that ended: all the remaining keys when the scan reads
     * a single page frame, the next key otherwise or while the number of page frames is unknown.
     */
    private boolean nextKeys() {
        if (keyHi >= keyCount || scanFrameCount == 0) {
            return false;
        }
        keyLo = keyHi;
        keyHi = scanFrameCount == 1 ? keyCount : keyHi + 1;
        super.toTop();
        return true;
    }

    private void prepareRowCursorFactory() {
        if (!areCursorsPrepared) {
            rowCursorFactory.prepareCursor(frameCursor);
            keyCount = keyOrderedFactory != null ? keyOrderedFactory.getKeyCount() : 0;
            if (keyCount > 1) {
                // a consumer that revisits rows in any order keeps its SCATTERED hint
                if (frameMemoryPool.getDecodeHint() == ParquetDecodeHint.MONOTONIC) {
                    frameMemoryPool.setParquetDecodeHint(ParquetDecodeHint.CYCLIC);
                }
                // a read ordered by keys opens an index cursor per key and page frame, and lands its skips by index
                // counts, so a page frame per partition frame costs it least
                frameCursor.setPageFrameMaxRows(Integer.MAX_VALUE);
            }
            keyLo = 0;
            keyHi = Math.min(keyCount, 1);
            areCursorsPrepared = true;
        }
    }

    /**
     * Opens the cursor of the keys of the pass over the page frame and skips its rows up to the row count.
     */
    private void readFrameRows(PageFrame frame, Counter rowCount) {
        circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
        frameAddressCache.add(frameCount, frame);
        rowCursor = keyOrderedFactory.getCursor(frame, frameMemoryPool.navigateTo(frameCount++), keyLo, keyHi);
        countReadFrame();
        skipRowCursorRows(rowCount);
    }

    /**
     * Opens the cursor of the keys of the pass from {@code key} on over the page frame, starting the rows of that key at
     * {@code firstRow}.
     */
    private void readFrameRowsFrom(PageFrame frame, int key, long firstRow) {
        circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
        frameAddressCache.add(frameCount, frame);
        rowCursor = keyOrderedFactory.getCursor(frame, frameMemoryPool.navigateTo(frameCount++), key, keyHi, firstRow);
        countReadFrame();
    }

    /**
     * Moves the frame cursor on to the page frame of the partition that holds the row, caching the addresses of the
     * page frames it passes, and returns that page frame unread.
     */
    private @Nullable PageFrame seekFrame(int partitionIndex, long row) {
        final int cachedFrameIndex = findCachedFrame(partitionIndex, row);
        PageFrame frame;
        while ((frame = frameCursor.next()) != null
                && (frame.getPartitionIndex() != partitionIndex || row < frame.getPartitionLo() || row >= frame.getPartitionHi())) {
            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
            frameAddressCache.add(frameCount++, frame);
            if (cachedFrameIndex > frameCount
                    && frameCursor instanceof TablePageFrameCursor tableCursor
                    && tableCursor.skipToCachedFrame(frameAddressCache, cachedFrameIndex)) {
                frameCount = cachedFrameIndex;
            }
        }
        return frame;
    }

    /**
     * Skips the rows of a read ordered by keys pass by pass.
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
        boolean isInPass = frameCount > 0 ? skipPassFrames(frameCursor.next(), rowCount) : skipPass(rowCount);
        while (!isInPass) {
            if (!nextKeys()) {
                isExhausted = true;
                return;
            }
            isInPass = skipPass(rowCount);
        }
    }

    /**
     * Skips the rows of the pass that starts at the next page frame, and reports whether the skip ends inside the pass.
     * A skip that ends in the first page frame lands there, since the read after the skip opens it anyway. Otherwise
     * the skip counts the rows of the pass partition frame by partition frame, and goes on page frame by page frame only
     * in the partition frame where it ends. There the index finds the row after the skip, unless the pass has a single
     * key that holds every row of the partition frame, which places that row by arithmetic.
     */
    private boolean skipPass(Counter rowCount) {
        PageFrame frame = frameCursor.next();
        if (frame == null) {
            scanFrameCount = 0;
            return false;
        }
        final long restRowCount = frameCursor.getRemainingRowsInInterval();
        final long skipCount = rowCount.get();
        final long firstFrameRowCount = skipCount < frame.getPartitionHi() - frame.getPartitionLo()
                ? keyOrderedFactory.getRowCount(frame, keyLo, keyHi, frame.getPartitionLo(), frame.getPartitionHi() - 1, skipCount)
                : 0;
        if (firstFrameRowCount > skipCount) {
            skipFrameKeys(frame, rowCount);
            return true;
        }
        if (firstFrameRowCount < 0) {
            readFrameRows(frame, rowCount);
            if (rowCount.get() == 0) {
                return true;
            }
            rowCount.add(skipCount - rowCount.get());
            rowCursor = Misc.free(rowCursor);
        }
        super.toTop();
        int partitionFrameCount = 0;
        while ((frame = frameCursor.next(Long.MAX_VALUE)) != null) {
            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
            partitionFrameCount++;
            final long rowLo = frame.getPartitionLo();
            final long rowHi = frame.getPartitionHi() - 1;
            final long count = keyOrderedFactory.getRowCount(frame, keyLo, keyHi, rowLo, rowHi, rowCount.get());
            if (count >= 0 && count <= rowCount.get()) {
                rowCount.dec(count);
                continue;
            }
            final int partitionIndex = frame.getPartitionIndex();
            if (count < 0 || keyHi - keyLo > 1) {
                super.toTop();
                return skipPassFrames(seekFrame(partitionIndex, isBackward() ? rowHi : rowLo), rowCount);
            }
            final long row = getLandingRow(frame, keyLo, rowLo, rowHi, count, rowCount.get());
            super.toTop();
            readFrameRowsFrom(seekFrame(partitionIndex, row), keyLo, row);
            rowCount.dec(rowCount.get());
            return true;
        }
        if (partitionFrameCount == 1 && restRowCount == 0) {
            scanFrameCount = 1;
        }
        return false;
    }

    /**
     * Skips the rows of the pass page frame by page frame from the given page frame on, a whole page frame at a time
     * when the factory counts its rows, and reports whether the skip ends inside the pass.
     */
    private boolean skipPassFrames(@Nullable PageFrame frame, Counter rowCount) {
        while (frame != null) {
            final long count = keyOrderedFactory.getRowCount(frame, keyLo, keyHi, frame.getPartitionLo(), frame.getPartitionHi() - 1, rowCount.get());
            if (count >= 0 && count <= rowCount.get()) {
                circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
                frameAddressCache.add(frameCount++, frame);
                rowCount.dec(count);
                if (rowCount.get() == 0) {
                    return true;
                }
            } else if (count >= 0) {
                skipFrameKeys(frame, rowCount);
                return true;
            } else {
                readFrameRows(frame, rowCount);
                if (rowCount.get() == 0) {
                    return true;
                }
                rowCursor = Misc.free(rowCursor);
            }
            frame = frameCursor.next();
        }
        scanFrameCount = frameCount;
        return false;
    }

    /**
     * Skips rows of the pass inside the page frame, which holds more rows of the pass than the skip: the rows of whole
     * keys by their counts, then those of the key where the skip ends, whose cursor starts at the row the index finds.
     */
    private void skipFrameKeys(PageFrame frame, Counter rowCount) {
        final long rowLo = frame.getPartitionLo();
        final long rowHi = frame.getPartitionHi() - 1;
        int key = keyLo;
        long count;
        while ((count = keyOrderedFactory.getRowCount(frame, key, key + 1, rowLo, rowHi, rowCount.get())) <= rowCount.get()) {
            rowCount.dec(count);
            key++;
        }
        readFrameRowsFrom(frame, key, getLandingRow(frame, key, rowLo, rowHi, count, rowCount.get()));
        rowCount.dec(rowCount.get());
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
