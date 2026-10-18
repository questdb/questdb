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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.DirectLongList;
import io.questdb.std.Misc;
import io.questdb.std.NumericException;
import io.questdb.std.Os;
import io.questdb.std.Rows;
import org.jetbrains.annotations.NotNull;

/**
 * Used to handle the LIMIT -N clause with the descending timestamp order case. To do so, this cursor
 * accumulates the row ids in a buffer and only then starts the iteration. That's necessary to preserve
 * the timestamp-based order in the result set. The buffer is filled in from bottom to top.
 * <p>
 * Here is an illustration of the described problem:
 * <pre>
 * row iteration order    frames                      frame iteration order
 *         |            [ row 0   <- frame 0 start           /\
 *         |              row 1                              |
 *         |              row 2 ] <- frame 0 end             |
 *         |            [ row 3   <- frame 1 start           |
 *        \/              row 4 ] <- frame 1 end             |
 * </pre>
 * A negative LIMIT over a join whose master the cursor reads goes on to older rows through {@link #readOlderRows()},
 * and returns to the rows it keeps through {@link #readRows(DirectLongList)}. The frame sequence reads the frames last
 * to first in scan order, so a frame with a higher index holds older rows.
 */
class AsyncFilteredNegativeLimitRecordCursor implements AsyncFilteredRecordCursorFactory.RecordFreer, RecordCursor {
    private static final Log LOG = LogFactory.getLog(AsyncFilteredNegativeLimitRecordCursor.class);
    // Each read of older rows covers this many times the rows read before it, so the join over the
    // master rows rewinds a logarithmic number of times.
    private static final long ROW_COUNT_GROWTH = 64;
    private final int dispatchLimit;
    // Used for random access: we may have to deserialize Parquet page frame.
    private final PageFrameMemoryPool frameMemoryPool;
    private final boolean hasDescendingOrder;
    private final PageFrameMemoryRecord record;
    private int frameIndex;
    private int frameLimit;
    private PageFrameSequence<?> frameSequence;
    private boolean hasOlderRows;
    private boolean isOpen;
    // The range of older frames the cursor returns from its next toTop(), from the oldest frame on; none when the
    // count is 0. The rows of the range's last frame end at olderLastFrameRowLimit.
    private int olderFirstFrameIndex;
    // The task of the older frame whose rows the cursor returns.
    private long olderFrameCursor = -1;
    private int olderFrameCount;
    private int olderFrameIndex;
    private long olderFrameRowHi;
    private long olderFrameRowIndex;
    private long olderFrameRowLo;
    private DirectLongList olderFrameRows;
    private int olderFramesLeft;
    private long olderLastFrameRowLimit;
    private PageFrameMemoryRecord recordB;
    private long rowCount;
    // The buffered rows the cursor returns end at rowHi.
    private long rowHi;
    // The row hasNext() returned last.
    private long rowId;
    private long rowIndex;
    // Artificial limit on remaining rows to be returned from this cursor.
    // It is typically copied from LIMIT clause on SQL statement.
    private long rowLimit;
    private long rowLo;
    // Buffer used to accumulate all filtered row ids.
    private DirectLongList rows;
    // The rows the cursor has not returned are those of the frames past unreadFrameIndex, and those of
    // frame unreadFrameIndex before local row unreadRowLimit in scan order.
    private int unreadFrameIndex;
    private long unreadRowLimit;

    public AsyncFilteredNegativeLimitRecordCursor(@NotNull CairoConfiguration configuration, int scanDirection) {
        // close() only frees these once isOpen (set in of()), so a ctor failure must free the
        // already-allocated natives here; build them into locals so the catch can release them.
        PageFrameMemoryRecord record = null;
        PageFrameMemoryPool frameMemoryPool = null;
        try {
            record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
            frameMemoryPool = new PageFrameMemoryPool(configuration);
        } catch (Throwable th) {
            Misc.free(record);
            Misc.free(frameMemoryPool);
            throw th;
        }
        this.record = record;
        this.hasDescendingOrder = scanDirection == RecordCursorFactory.SCAN_DIRECTION_BACKWARD;
        this.frameMemoryPool = frameMemoryPool;
        this.dispatchLimit = configuration.getSqlParallelFilterDispatchLimit();
    }

    @Override
    public void close() {
        if (isOpen) {
            try {
                if (frameSequence != null) {
                    LOG.debug()
                            .$("closing [shard=").$(frameSequence.getShard())
                            .$(", frameCount=").$(frameLimit)
                            .I$();

                    collectOlderFrame(true);
                    if (frameLimit > -1) {
                        frameSequence.await();
                    }
                    frameSequence.reset();
                }
            } finally {
                Misc.free(frameMemoryPool);
                isOpen = false;
            }
        }
    }

    @Override
    public void freeRecords() {
        Misc.free(record);
        Misc.free(recordB);
        Misc.free(frameMemoryPool);
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public Record getRecordB() {
        if (recordB != null) {
            return recordB;
        }
        recordB = new PageFrameMemoryRecord(record, PageFrameMemoryRecord.RECORD_B_LETTER);
        return recordB;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return frameSequence.getSymbolTableSource().getSymbolTable(columnIndex);
    }

    @Override
    public boolean hasNext() {
        // check for the first hasNext call
        if (frameIndex == -1) {
            fetchAllFrames();
        }
        if (rowIndex < rowHi) {
            rowId = rows.get(rowIndex);
            final PageFrameMemory frameMemory = frameMemoryPool.navigateTo(Rows.toPartitionIndex(rowId));
            record.init(frameMemory);
            record.setRowIndex(Rows.toLocalRowID(rowId));
            rowIndex++;
            return true;
        }
        return nextOlderFrameRow();
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return frameSequence.getSymbolTableSource().newSymbolTable(columnIndex);
    }

    @Override
    public long preComputedStateSize() {
        return frameIndex;
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        final PageFrameMemoryRecord frameMemoryRecord = (PageFrameMemoryRecord) record;
        frameMemoryPool.navigateTo(Rows.toPartitionIndex(atRowId), frameMemoryRecord);
        frameMemoryRecord.setRowIndex(Rows.toLocalRowID(atRowId));
    }

    @Override
    public long remainingMasterRowCount() {
        return 0;
    }

    @Override
    public void setParquetDecodeHint(ParquetDecodeHint hint) {
        frameMemoryPool.setParquetDecodeHint(hint);
    }

    @Override
    public long size() {
        if (frameIndex == -1 || olderFrameCount > 0) {
            return -1;
        }
        return rowHi - rowLo;
    }

    @Override
    public void toTop() {
        rowIndex = rowLo;
        if (olderFrameCount > 0) {
            collectOlderFrame(false);
            ((AsyncFilterAtom) frameSequence.getAtom()).getFilter(-1).toTop();
            frameSequence.toTop(olderFirstFrameIndex, olderFirstFrameIndex - olderFrameCount + 1);
            olderFramesLeft = olderFrameCount;
        }
    }

    /**
     * The index of the first of the ascending local rows that is not below {@code localRow}.
     */
    private static long lowerBound(DirectLongList localRows, long localRow) {
        long lo = 0;
        long hi = localRows.size();
        while (lo < hi) {
            final long mid = (lo + hi) >>> 1;
            if (localRows.get(mid) < localRow) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    private CairoException buildInterruptionException() {
        return frameSequence.buildInterruptionException();
    }

    private void collectOlderFrame(boolean forceCollect) {
        if (olderFrameCursor > -1) {
            frameSequence.collect(olderFrameCursor, forceCollect);
            olderFrameCursor = -1;
            olderFrameRowIndex = olderFrameRowHi - olderFrameRowLo;
            // The record reads the page frame memory of the task, which is no longer ours.
            record.clear();
        }
    }

    private void fetchAllFrames() {
        if (frameLimit == -1) {
            frameSequence.prepareForDispatch();
            frameLimit = frameSequence.getFrameCount() - 1;
        }

        boolean allFramesActive = true;
        try {
            do {
                final long cursor = frameSequence.next(dispatchLimit, false);
                if (cursor > -1) {
                    PageFrameReduceTask task = frameSequence.getTask(cursor);
                    LOG.debug()
                            .$("collected [shard=").$(frameSequence.getShard())
                            .$(", frameIndex=").$(task.getFrameIndex())
                            .$(", frameCount=").$(frameSequence.getFrameCount())
                            .$(", active=").$(frameSequence.isActive())
                            .$(", cursor=").$(cursor)
                            .I$();

                    if (task.hasError()) {
                        throw task.buildError();
                    }

                    // Consider frame sequence status only if we haven't accumulated enough rows.
                    allFramesActive &= frameSequence.isActive() || rowCount >= rowLimit;
                    final DirectLongList frameRows = task.getFilteredRows();
                    final long frameRowCount = frameRows.size();
                    frameIndex = task.getFrameIndex();

                    if (frameRowCount > 0 && rowCount < rowLimit + 1 && frameSequence.isActive()) {
                        final long frameStartRowCount = rowCount;
                        // Copy rows into the buffer.
                        if (hasDescendingOrder) {
                            for (long i = 0; i < frameRowCount && rowCount < rowLimit; i++, rowCount++) {
                                rows.set(--rowIndex, Rows.toRowID(frameIndex, frameRows.get(i)));
                            }
                        } else {
                            for (long i = frameRowCount - 1; i > -1 && rowCount < rowLimit; i--, rowCount++) {
                                rows.set(--rowIndex, Rows.toRowID(frameIndex, frameRows.get(i)));
                            }
                        }

                        if (rowCount >= rowLimit) {
                            hasOlderRows = rowCount - frameStartRowCount < frameRowCount || frameIndex < frameLimit;
                            frameSequence.cancel(SqlExecutionCircuitBreaker.STATE_OK);
                        }
                    }

                    frameSequence.collect(cursor, false);
                } else if (cursor == -2) {
                    break; // No frames to filter.
                } else {
                    Os.pause();
                }
            } while (frameIndex < frameLimit);
        } catch (Throwable e) {
            throw filterError(e);
        }

        if (!allFramesActive) {
            throw buildInterruptionException();
        }
        rowLo = rowIndex;
        rowHi = rows.getCapacity();
    }

    private RuntimeException filterError(Throwable e) {
        LOG.error().$("negative limit filter error [ex=").$(e).I$();
        if (e instanceof CairoException ce) {
            return ce.isInterruption() ? buildInterruptionException() : ce;
        }
        // Preserve typed user-facing errors (ImplicitCastException / NumericException)
        // raised via task.buildError() so the caller can recognise them.
        if (e instanceof ImplicitCastException || e instanceof NumericException) {
            return (RuntimeException) e;
        }
        return CairoException.nonCritical().put(e.getMessage());
    }

    private long frameRowCount(int frame) {
        return frameSequence.getFrameRowCount(frame);
    }

    /**
     * True when the row lies before the rows the cursor returned in scan order.
     */
    private boolean isUnread(long rowId) {
        final int rowFrameIndex = Rows.toPartitionIndex(rowId);
        if (rowFrameIndex != unreadFrameIndex) {
            return rowFrameIndex > unreadFrameIndex;
        }
        final long localRow = Rows.toLocalRowID(rowId);
        return hasDescendingOrder ? localRow > unreadRowLimit : localRow < unreadRowLimit;
    }

    /**
     * Takes the task of the next frame of the range of older frames; of the range's last frame, it keeps the rows
     * before the ones the cursor returned. Returns false at the end of the range.
     */
    private boolean nextOlderFrame() {
        collectOlderFrame(false);
        boolean isStopped = false;
        try {
            while (olderFramesLeft > 0) {
                final long cursor = frameSequence.next(dispatchLimit, false);
                if (cursor > -1) {
                    olderFrameCursor = cursor;
                    olderFramesLeft--;
                    final PageFrameReduceTask task = frameSequence.getTask(cursor);
                    if (task.hasError()) {
                        throw task.buildError();
                    }
                    if (frameSequence.isActive()) {
                        olderFrameIndex = task.getFrameIndex();
                        olderFrameRows = task.getFilteredRows();
                        olderFrameRowLo = 0;
                        olderFrameRowHi = olderFrameRows.size();
                        if (olderFramesLeft == 0) {
                            if (hasDescendingOrder) {
                                olderFrameRowLo = lowerBound(olderFrameRows, olderLastFrameRowLimit + 1);
                            } else {
                                olderFrameRowHi = lowerBound(olderFrameRows, olderLastFrameRowLimit);
                            }
                        }
                        olderFrameRowIndex = 0;
                        record.init(task.getFrameMemory());
                        return true;
                    }
                    // A stopped sequence filters no rows; a later task may carry the error that stopped it.
                    collectOlderFrame(false);
                    isStopped = true;
                } else if (cursor == -2) {
                    isStopped = true;
                    break;
                } else {
                    Os.pause();
                }
            }
        } catch (Throwable e) {
            throw filterError(e);
        }
        if (isStopped) {
            throw buildInterruptionException();
        }
        return false;
    }

    private boolean nextOlderFrameRow() {
        while (olderFrameRowIndex == olderFrameRowHi - olderFrameRowLo) {
            if (!nextOlderFrame()) {
                return false;
            }
        }
        final long localRow = olderFrameRows.get(hasDescendingOrder ? olderFrameRowHi - 1 - olderFrameRowIndex : olderFrameRowLo + olderFrameRowIndex);
        olderFrameRowIndex++;
        record.setRowIndex(localRow);
        rowId = Rows.toRowID(olderFrameIndex, localRow);
        return true;
    }

    private void readLastRows(long rowLimit) {
        ((AsyncFilterAtom) frameSequence.getAtom()).getFilter(-1).toTop();
        frameSequence.toTop();
        reset(rowLimit);
        fetchAllFrames();
    }

    private void reset(long rowLimit) {
        this.rowLimit = rowLimit;
        frameIndex = -1;
        rowIndex = rows.getCapacity();
        rowLo = rowIndex;
        rowHi = rowIndex;
        rowCount = 0;
        hasOlderRows = false;
    }

    private long unreadRowCount(int frame, long rowLimit) {
        return hasDescendingOrder ? frameRowCount(frame) - 1 - rowLimit : rowLimit;
    }

    private long untouchedRowLimit(int frame) {
        return hasDescendingOrder ? -1 : frameRowCount(frame);
    }

    /**
     * The row {@link #hasNext()} returned last.
     */
    long getRowId() {
        return rowId;
    }

    /**
     * True while the cursor serves an execution of the filter, which reads its last rows.
     */
    boolean isOpen() {
        return isOpen;
    }

    void of(PageFrameSequence<?> frameSequence, long rowLimit, DirectLongList negativeLimitRows) {
        this.isOpen = true;
        this.frameSequence = frameSequence;
        this.frameLimit = -1;
        this.rows = negativeLimitRows;
        olderFrameCursor = -1;
        olderFrameCount = 0;
        olderFramesLeft = 0;
        olderFrameRowLo = 0;
        olderFrameRowHi = 0;
        olderFrameRowIndex = 0;
        reset(rowLimit);
        frameMemoryPool.setMemoryTracker(frameSequence.getMemoryTracker());
        ((AsyncFilterAtom) frameSequence.getAtom()).setParentUsedColumns(null);
        frameMemoryPool.of(frameSequence.getPageFrameAddressCache());
        record.of(frameSequence.getSymbolTableSource());
        if (recordB != null) {
            recordB.of(frameSequence.getSymbolTableSource());
        }
    }

    /**
     * Sets the rows the cursor returns from its next {@link #toTop()} to the rows that pass the filter before the ones
     * it returned: while the buffer holds them, {@link #ROW_COUNT_GROWTH} times as many of the last rows, which the
     * filter reads again, and then a range of older frames with {@link #ROW_COUNT_GROWTH} - 1 times as many rows as the
     * frames read before it, which the frame sequence reads forward. Returns false, changing nothing, when no older row
     * may pass the filter.
     */
    boolean readOlderRows() {
        if (frameIndex == -1) {
            // nothing read the last rows yet, so they are the rows to return
            fetchAllFrames();
            return rowLo < rowHi;
        }
        if (!hasOlderRows) {
            return false;
        }
        if (olderFrameCount == 0) {
            if (rowLo < rowHi) {
                unreadFrameIndex = Rows.toPartitionIndex(rows.get(rowLo));
                unreadRowLimit = Rows.toLocalRowID(rows.get(rowLo));
            }
            final long capacity = rows.getCapacity();
            if (rowLimit < capacity) {
                readLastRows(Math.min(rowLimit * ROW_COUNT_GROWTH, capacity));
                rowHi = rowLo;
                while (rowHi < capacity && isUnread(rows.get(rowHi))) {
                    rowHi++;
                }
                return true;
            }
            rowLo = capacity;
            rowHi = capacity;
        }
        int lastFrameIndex = unreadFrameIndex;
        long lastFrameRowLimit = unreadRowLimit;
        if (unreadRowCount(lastFrameIndex, lastFrameRowLimit) == 0) {
            if (lastFrameIndex == frameLimit) {
                hasOlderRows = false;
                return false;
            }
            lastFrameIndex++;
            lastFrameRowLimit = untouchedRowLimit(lastFrameIndex);
        }
        long rangeRowCount = unreadRowCount(lastFrameIndex, lastFrameRowLimit);
        long readRowCount = frameRowCount(lastFrameIndex) - rangeRowCount;
        for (int i = 0; i < lastFrameIndex; i++) {
            readRowCount += frameRowCount(i);
        }
        final long targetRowCount = (ROW_COUNT_GROWTH - 1) * Math.max(readRowCount, 1);
        int firstFrameIndex = lastFrameIndex;
        while (rangeRowCount < targetRowCount && firstFrameIndex < frameLimit) {
            rangeRowCount += frameRowCount(++firstFrameIndex);
        }
        olderFirstFrameIndex = firstFrameIndex;
        olderFrameCount = firstFrameIndex - lastFrameIndex + 1;
        olderLastFrameRowLimit = lastFrameRowLimit;
        hasOlderRows = firstFrameIndex < frameLimit;
        if (hasOlderRows) {
            unreadFrameIndex = firstFrameIndex + 1;
            unreadRowLimit = untouchedRowLimit(unreadFrameIndex);
        }
        return true;
    }

    /**
     * Sets the rows the cursor returns from its next {@link #toTop()} to the listed rows, which the list holds newest
     * first, and stops reading older rows.
     */
    void readRows(DirectLongList rowIds) {
        final long capacity = rows.getCapacity();
        final long rowIdCount = rowIds.size();
        assert rowIdCount <= capacity;
        collectOlderFrame(false);
        olderFrameCount = 0;
        olderFramesLeft = 0;
        hasOlderRows = false;
        for (long i = 0; i < rowIdCount; i++) {
            rows.set(capacity - 1 - i, rowIds.get(i));
        }
        rowLo = capacity - rowIdCount;
        rowHi = capacity;
        rowIndex = rowLo;
    }
}
