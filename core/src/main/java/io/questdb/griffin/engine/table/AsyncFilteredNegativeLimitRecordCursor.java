/*******************************************************************************
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
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
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
 */
class AsyncFilteredNegativeLimitRecordCursor implements RecordCursor {
    private static final Log LOG = LogFactory.getLog(AsyncFilteredNegativeLimitRecordCursor.class);
    private final int dispatchLimit;
    // Used for random access: we may have to deserialize Parquet page frame.
    private final PageFrameMemoryPool frameMemoryPool;
    private final boolean hasDescendingOrder;
    private final PageFrameMemoryRecord record;
    private int frameIndex;
    private int frameLimit;
    private PageFrameSequence<?> frameSequence;
    private boolean isOpen;
    private PageFrameMemoryRecord recordB;
    private long rowCount;
    private long rowIndex;
    // Artificial limit on remaining rows to be returned from this cursor.
    // It is typically copied from LIMIT clause on SQL statement.
    private long rowLimit;
    // Buffer used to accumulate all filtered row ids.
    private DirectLongList rows;

    public AsyncFilteredNegativeLimitRecordCursor(@NotNull CairoConfiguration configuration, int scanDirection) {
        this.record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
        this.hasDescendingOrder = scanDirection == RecordCursorFactory.SCAN_DIRECTION_BACKWARD;
        this.frameMemoryPool = new PageFrameMemoryPool(configuration.getSqlParquetFrameCacheCapacity());
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
        if (rowIndex < rows.getCapacity()) {
            long rowId = rows.get(rowIndex);
            final PageFrameMemory frameMemory = frameMemoryPool.navigateTo(Rows.toPartitionIndex(rowId));
            record.init(frameMemory);
            record.setRowIndex(Rows.toLocalRowID(rowId));
            rowIndex++;
            return true;
        }
        return false;
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
    public long size() {
        if (frameIndex == -1) {
            return -1;
        }
        return rowCount;
    }

    @Override
    public void toTop() {
        rowIndex = rows.getCapacity() - rowCount;
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
                        throw CairoException.nonCritical()
                                .position(task.getErrorMessagePosition())
                                .put(task.getErrorMsg())
                                .setCancellation(task.isCancelled())
                                .setInterruption(task.isCancelled())
                                .setOutOfMemory(task.isOutOfMemory());
                    }

                    // Consider frame sequence status only if we haven't accumulated enough rows.
                    allFramesActive &= frameSequence.isActive() || rowCount >= rowLimit;
                    final DirectLongList frameRows = task.getFilteredRows();
                    final long frameRowCount = frameRows.size();
                    frameIndex = task.getFrameIndex();

                    if (frameRowCount > 0 && rowCount < rowLimit + 1 && frameSequence.isActive()) {
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
            LOG.error().$("negative limit filter error [ex=").$(e).I$();
            if (e instanceof CairoException ce) {
                if (ce.isInterruption()) {
                    throwTimeoutException();
                } else {
                    throw ce;
                }
            }
            throw CairoException.nonCritical().put(e.getMessage());
        }

        if (!allFramesActive) {
            throwTimeoutException();
        }
    }

    private void throwTimeoutException() {
        if (frameSequence.getCancelReason() == SqlExecutionCircuitBreaker.STATE_CANCELLED) {
            throw CairoException.queryCancelled();
        } else {
            throw CairoException.queryTimedOut();
        }
    }

    void of(PageFrameSequence<?> frameSequence, long rowLimit, DirectLongList negativeLimitRows) {
        this.isOpen = true;
        this.frameSequence = frameSequence;
        this.frameIndex = -1;
        this.frameLimit = -1;
        this.rowLimit = rowLimit;
        this.rows = negativeLimitRows;
        this.rowIndex = negativeLimitRows.getCapacity();
        this.rowCount = 0;
        frameMemoryPool.of(frameSequence.getPageFrameAddressCache());
        record.of(frameSequence.getSymbolTableSource());
        if (recordB != null) {
            recordB.of(frameSequence.getSymbolTableSource());
        }
    }
}
