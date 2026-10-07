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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordBlock;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.sql.RowCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.jetbrains.annotations.Nullable;

// Final: peekRecordBlock() exposes the frames' column memory as the rows hasNext() returns. A
// subclass that changed the rows in hasNext() or getRecord() would offer blocks that bypass it.
//
// Blocks: a plain forward scan offers the rest of a native frame as consecutive rows. A scan whose
// row cursor walks an index offers the frame's next rows as a list of row indexes: peekRecordBlock()
// reads them ahead from the row cursor into pendingRows, and hasNext() returns the rows read ahead
// before it asks the row cursor for more. What the row cursor throws while read ahead is kept and
// thrown by hasNext() once it has returned the rows before it: the row-by-row read meets it there.
public final class PageFrameRecordCursorImpl extends AbstractPageFrameRecordCursor {
    private final boolean entityCursor;
    private final Function filter;
    private final RowCursorFactory rowCursorFactory;
    private boolean areCursorsPrepared;
    private FrameBlock block;
    // the frame peekRecordBlock() last found not to be NATIVE, so that the rows of a Parquet
    // frame do not each pay for a frame navigation; -1 for none
    private int blockRefusedFrameIndex = -1;
    private SqlExecutionCircuitBreaker circuitBreaker;
    private boolean isExhausted;
    private long maxRowsAfterSkip = RecordCursor.UNBOUNDED_ROW_COUNT;
    // index scans: what the row cursor threw while peekRecordBlock() read ahead, after the rows in
    // pendingRows; hasNext() throws it once it has returned them, or null for none
    private RuntimeException pendingError;
    // index scans: the current frame's rows read ahead of hasNext() by peekRecordBlock(), the next
    // one to return at pendingRowsPos
    private DirectLongList pendingRows;
    private long pendingRowsPos;
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
                || frameCursor.hasActivePushdownFilter()) {
            while (hasNext()) {
                counter.inc();
            }
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
        clearPendingRows();
        pendingRows = Misc.free(pendingRows);
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
            if (pendingRows != null && pendingRowsPos < pendingRows.size()) {
                // a row peekRecordBlock() read ahead from the row cursor
                frameMemoryPool.navigateTo(frameCount - 1, recordA);
                recordA.setRowIndex(pendingRows.get(pendingRowsPos++));
                rowsProducedSinceSkip++;
                return true;
            }
            if (pendingError != null) {
                // where the row cursor threw while read ahead: NoMoreFramesException ends the scan
                // in the catch below, as it does when the row cursor throws it here
                final RuntimeException e = pendingError;
                pendingError = null;
                throw e;
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
            while ((frame = frameCursor.next()) != null) {
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
                rowCursor = rowCursorFactory.getCursor(frame, frameMemory);
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

    /**
     * A plain forward scan offers the rest of a native frame: its rows are consecutive row indexes
     * of the frame's column pages, read at the same addresses the record's getters read. An index
     * scan offers the frame's next rows, read ahead from its row cursor, as their row indexes.
     */
    @Override
    public RecordBlock peekRecordBlock(int maxRows) {
        if (isExhausted || rowCursor == null) {
            return null;
        }
        final long budget = maxRowsAfterSkip - rowsProducedSinceSkip;
        final int frameIndex = frameCount - 1;
        if (budget < 1 || frameIndex == blockRefusedFrameIndex) {
            return null;
        }
        if (rowCursor instanceof PageFrameFwdRowCursor fwdCursor) {
            final long rows = Math.min(fwdCursor.remaining(), budget);
            if (rows < 1 || !isNativeFrame(frameIndex)) {
                return null;
            }
            if (block == null) {
                block = new FrameBlock();
            }
            block.of(fwdCursor.peekNext(), 0, (int) Math.min(rows, maxRows));
            return block;
        }
        if (!rowCursorFactory.isUsingIndex() || !isNativeFrame(frameIndex)) {
            return null;
        }
        // read the frame's next rows ahead, up to the most the consumer may take
        final long wanted = Math.min(budget, maxRows);
        if (pendingRows == null) {
            pendingRows = new DirectLongList(Math.max(wanted, 16), MemoryTag.NATIVE_DEFAULT);
        }
        if (pendingRowsPos > 0) {
            // the rows already returned go, the rest move to the list's start
            final long left = pendingRows.size() - pendingRowsPos;
            if (left > 0) {
                Vect.memmove(pendingRows.getAddress(), pendingRows.getAddress() + pendingRowsPos * Long.BYTES, left * Long.BYTES);
            }
            pendingRows.setPos(left);
            pendingRowsPos = 0;
        }
        if (pendingError == null) {
            try {
                while (pendingRows.size() < wanted && rowCursor.hasNext()) {
                    pendingRows.add(rowCursor.next());
                }
            } catch (RuntimeException e) {
                // NoMoreFramesException (a LATEST ON value's row cursor ends the scan with it), or
                // the error of a filter the row cursor evaluates: either comes after the rows read
                // so far, so hasNext() throws it once it has returned them, and not before
                pendingError = e;
            }
        }
        final long rows = Math.min(pendingRows.size(), wanted);
        if (rows < 1) {
            return null;
        }
        if (block == null) {
            block = new FrameBlock();
        }
        block.of(0, pendingRows.getAddress(), (int) rows);
        return block;
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
        clearPendingRows();
        maxRowsAfterSkip = RecordCursor.UNBOUNDED_ROW_COUNT;
        rowsProducedSinceSkip = 0;
        blockRefusedFrameIndex = -1;
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
    public void skipRecordBlock(int rowCount) {
        if (rowCursor instanceof PageFrameFwdRowCursor fwdCursor) {
            fwdCursor.skip(rowCount);
        } else {
            assert pendingRows != null && pendingRowsPos + rowCount <= pendingRows.size();
            pendingRowsPos += rowCount;
        }
        rowsProducedSinceSkip += rowCount;
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
            while (rowCount.get() > 0 && hasNext()) {
                rowCount.dec();
            }
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

    /**
     * The scans {@link #peekRecordBlock(int)} serves: a plain forward scan, whose row cursors are
     * {@link PageFrameFwdRowCursor}s, with no filter; and a scan whose row cursors walk an index,
     * which apply any filter themselves, so that the rows they return are the ones to return.
     */
    @Override
    public boolean supportsRecordBlocks() {
        return (filter == null && rowCursorFactory instanceof PageFrameRowCursorFactory f && f.isForwardScan())
                || rowCursorFactory.isUsingIndex();
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
        clearPendingRows();
        isExhausted = false;
        maxRowsAfterSkip = RecordCursor.UNBOUNDED_ROW_COUNT;
        rowsProducedSinceSkip = 0;
        blockRefusedFrameIndex = -1;
        super.toTop();
    }

    private void clearPendingRows() {
        if (pendingRows != null) {
            pendingRows.clear();
        }
        pendingRowsPos = 0;
        pendingError = null;
    }

    /**
     * Whether a frame is NATIVE, its rows in column pages a block can expose. Positions the record
     * at the frame, as hasNext() leaves it; a frame found not to be is remembered, so that its rows
     * do not each pay for a frame navigation.
     */
    private boolean isNativeFrame(int frameIndex) {
        frameMemoryPool.navigateTo(frameIndex, recordA);
        if (recordA.getFrameFormat() != PartitionFormat.NATIVE) {
            blockRefusedFrameIndex = frameIndex;
            return false;
        }
        return true;
    }

    private void prepareRowCursorFactory() {
        if (!areCursorsPrepared) {
            rowCursorFactory.prepareCursor(frameCursor);
            areCursorsPrepared = true;
        }
    }

    private class FrameBlock implements RecordBlock {
        // the first row's index, for consecutive rows
        private long firstRow;
        private int rowCount;
        // the rows' indexes, or 0 for consecutive rows from firstRow
        private long rowIndexes;

        @Override
        public long getColumnAddress(int columnIndex) {
            // getPageAddress() gives 0 for a column top over the whole frame and for a column
            // read with a type cast; both are then read through getRecordAt()
            final int columnType = metadata.getColumnType(columnIndex);
            if (ColumnType.isVarSize(columnType)) {
                return 0;
            }
            final long address = recordA.getPageAddress(columnIndex);
            return address != 0 ? address + firstRow * ColumnType.sizeOf(columnType) : 0;
        }

        @Override
        public long getColumnStride(int columnIndex) {
            return ColumnType.sizeOf(metadata.getColumnType(columnIndex));
        }

        @Override
        public Record getRecordAt(int row) {
            recordA.setRowIndex(rowIndexes == 0 ? firstRow + row : Unsafe.getLong(rowIndexes + (long) row * Long.BYTES));
            return recordA;
        }

        @Override
        public int getRowCount() {
            return rowCount;
        }

        @Override
        public long getRowIndexesAddress() {
            return rowIndexes;
        }

        void of(long firstRow, long rowIndexes, int rowCount) {
            this.firstRow = firstRow;
            this.rowIndexes = rowIndexes;
            this.rowCount = rowCount;
        }
    }
}
